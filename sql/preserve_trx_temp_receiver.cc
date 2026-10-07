/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_temp_receiver.h"
#include "sql/preserve_trx_temp_delta.h"
#include "sql/preserve_trx_temp_metrics.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <new>
#include <mutex>
#include <vector>
#include <openssl/evp.h>
#include <sys/stat.h>
#include <unistd.h>

#include "my_dbug.h"
#include "my_rnd.h"
#include "my_sys.h"
#include "scope_guard.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_file.h"
#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_result_manifest.h"
#include "sql/preserve_trx_temp_import.h"
#include "sql/preserve_trx_temp_gc.h"
#include "sql/preserve_trx_temp_restore.h"
#include "sql/preserve_trx_temp_transfer.h"
#include "sql/preserve_trx_temp_table_carrier.h"
#include "sql/preserve_trx_xid.h"
#include "sql/sql_class.h"
#include "storage/innobase/include/trx0temp_preserve.h"
#include "storage/innobase/include/trx0temp_preserve_import.h"
#include "storage/innobase/include/trx0temp_preserve_native.h"
#include "storage/innobase/include/trx0temp_preserve_input.h"

namespace {
// Raw intrusive owners deliberately survive terminal executor shutdown. Never
// run native destructors from static destruction after InnoDB has stopped.
std::mutex retired_mutex;
Preserve_trx_temp_receiver_work *retired_head{nullptr}, *retired_tail{nullptr};
std::atomic<uint64_t> retired_count{0};
#ifndef NDEBUG
std::atomic<uint64_t> retired_outstanding{0};
#endif

uint64_t monotonic_us() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}

dberr_t carrier_error(Preserved_trx_carrier_status status) {
  return status == Preserved_trx_carrier_status::OK ? DB_SUCCESS : DB_IO_ERROR;
}

bool valid_token(const std::string &token) {
  return !token.empty() && token.size() <= PRESERVE_TRX_TOKEN_MAX_LENGTH &&
         std::all_of(token.begin(), token.end(), [](unsigned char c) {
           return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                  (c >= '0' && c <= '9') || c == '_' || c == '-';
         });
}

bool sync_directory(const std::string &dir) {
  DBUG_EXECUTE_IF("preserve_temp_receiver_cleanup_sync_failure", {
    DBUG_PRINT("preserve_temp_import",
               ("temporary receiver cleanup directory sync retry fault"));
    return true;
  });
  File file = my_open(dir.c_str(), O_RDONLY | O_NOFOLLOW, MYF(0));
  if (file < 0) return true;
  const bool error = my_sync(file, MYF(0)) != 0;
  return my_close(file, MYF(0)) != 0 || error;
}
}  // namespace

struct Preserve_trx_temp_receiver_work::Impl {
  struct Image {
    Preserved_temp_table_image_descriptor descriptor;
    std::string installation_path;
    bool owned{false};
    std::unique_ptr<Preserved_temp_table_image_writer> writer;
    uint64_t previous_bytes{0};
    std::shared_ptr<const Preserve_trx_sealed_file> previous_source;
  };
  Preserve_memory_lease memory;
  Preserve_file_resource_lease files;
  Preserve_file_resource_lease growth;
  Owner previous;
  // Input outlives the plan and every SQL metadata borrower.
  std::unique_ptr<Preserve_trx_temp_transfer_input> input;
  std::unique_ptr<Preserve_trx_temp_import_work> source_import;
  std::unique_ptr<Preserve_trx_temp_delta_reader> early_delta;
  std::unique_ptr<trx_preserve_temp_undo_input> early_undo;
  std::unique_ptr<trx_preserve_temp_import_plan> plan;
  std::unique_ptr<Preserve_trx_temp_sql_ready> sql_ready;
  std::shared_ptr<trx_preserve_temp_native_directory> native_directory;
  Preserve_trx_temp_id_contract contract;
  std::string token;
  std::string dir;
  std::string installation_dir;
  std::string parent_dir;
  std::array<unsigned char, 32> manifest_digest{};
  uint64_t owner_trx_id{0};
  std::vector<Image> images;
  std::vector<unsigned char> page;
  std::vector<unsigned char> comparison;
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> digest{
      nullptr, EVP_MD_CTX_free};
  size_t next_space{0};
  uint32_t next_page{0};
  size_t cleanup_space{0};
  uint64_t written{0};
  uint64_t scanned{0};
  uint64_t allocated_bytes{0};
  size_t seal_space{0};
  size_t publish_space{0}, publish_table{0};
  bool file_attached{false};
  bool pipeline{false}, target_started{false}, native_ready{false}, ready{false};
  bool native_committed{false}, sql_bound{false};
  bool stats_ready{false};
  bool growth_prepared{false};
  dberr_t failure{DB_SUCCESS};
  bool owns_directory{false};
  bool owns_installation_directory{false};
  bool cleanup_sync_pending{false};
  bool undo_complete{false};
  bool cancelling{false};
  bool cancelled{false};

  dberr_t start_image() {
    const auto *target = plan->target_space(next_space);
    const auto *tables = plan->target_bindings(next_space);
    if (target == nullptr || tables == nullptr || tables->empty()) return DB_ERROR;
    auto &image = images[next_space].descriptor;
    const auto &table = tables->front();
    image.source_space_id = target->source_space_id;
    image.image_space_id = target->source_space_id;
    image.image_table_id = table.image_table_id;
    image.image_format_version = 1;
    image.size = target->image_bytes;
    image.page_size = target->page_size;
    image.space_flags = target->space_flags;
    image.table_flags = table.table_flags;
    image.clustered_root_page_no = table.clustered_root_page_no;
    image.blob_name = token + ".tempts." +
                      std::to_string(target->source_space_id) + ".image";
    images[next_space].installation_path = installation_dir + "/" + image.blob_name;
    image.indexes.reserve(table.indexes.size());
    image.indexes.clear();
    for (const auto &index : table.indexes) {
      image.indexes.push_back({index.image_index_id, index.root_page_no,
                               target->space_flags, index.name});
    }
    digest.reset(EVP_MD_CTX_new());
    if (digest == nullptr || EVP_DigestInit_ex(digest.get(), EVP_sha256(), nullptr) != 1)
      return DB_OUT_OF_MEMORY;
    if (!images[next_space].writer) {
      Local_file_preserved_temp_table_image_carrier carrier(installation_dir);
      const auto status = carrier.create_private_image_writer(
          token, target->source_space_id, &images[next_space].writer);
      if (status != Preserved_trx_carrier_status::OK) return carrier_error(status);
    }
    return DB_SUCCESS;
  }

  dberr_t checkpoint_image() {
    auto &image = images[next_space];
    unsigned digest_bytes = 0;
    if (EVP_DigestFinal_ex(digest.get(), image.descriptor.sha256.data(),
                         &digest_bytes) != 1 ||
        digest_bytes != image.descriptor.sha256.size()) return DB_ERROR;
    auto status = image.writer->truncate(image.descriptor.size);
    if (status != Preserved_trx_carrier_status::OK) return carrier_error(status);
    status = image.writer->checkpoint_result(image.descriptor.size, image.descriptor.sha256);
    if (status != Preserved_trx_carrier_status::OK) return carrier_error(status);
    auto err = plan->mark_target_image_checkpoint(
        next_space, image.descriptor.size, image.descriptor.sha256.data());
    if (err != DB_SUCCESS) return err;
    image.previous_source.reset();
    if (!pipeline) {
      err = seal_image(next_space);
      if (err != DB_SUCCESS) return err;
    }
    digest.reset();
    ++next_space;
    next_page = 0;
    return DB_SUCCESS;
  }

  dberr_t seal_image(size_t n) {
    auto &image = images[n];
    // Background completion includes the actual flush and close, before fil
    // opens the file. RESUME must not inherit a still-open writer descriptor.
    Preserved_temp_table_image_writer_result result;
    auto status = image.writer->close();
    if (status == Preserved_trx_carrier_status::OK)
      status = image.writer->result(&result);
    if (status != Preserved_trx_carrier_status::OK) return carrier_error(status);
    if (result.size != image.descriptor.size ||
        result.sha256 != image.descriptor.sha256) return DB_CORRUPTION;
    image.owned = true;
    image.writer.reset();
    DBUG_EXECUTE_IF("preserve_temp_receiver_seal_oom", {
      DBUG_PRINT("preserve_temp_import", ("temporary receiver sealed image OOM fault"));
      throw std::bad_alloc();
    });
    const auto err = plan->mark_target_image_sealed(
        n, image.descriptor.size, image.descriptor.sha256.data());
    return err;
  }
};

Preserve_trx_temp_receiver_work::Preserve_trx_temp_receiver_work()
    : m_impl(std::make_unique<Impl>()) {}

dberr_t Preserve_trx_temp_receiver_work::begin_import(
    const std::string &parent_dir,
    std::unique_ptr<Preserve_trx_temp_transfer_input> *input, Owner *output,
    Owner *previous) {
  if (!output || *output || !input || !*input || parent_dir.empty() ||
      !preserved_trx_expired_reaper_running()) return DB_ERROR;
  try {
    auto memory = preserve_trx_acquire_memory_lease(
        (*input)->token(), Preserve_trx_memory_kind::TEMP_METADATA_IMPORT,
        sizeof(Impl) + sizeof(Preserve_trx_temp_receiver_work) +
            2 * parent_dir.size() + 4096);
    if (!memory.acquired()) return DB_OUT_OF_MEMORY;
    Owner work(new Preserve_trx_temp_receiver_work());
    auto &s = *work->m_impl;
    s.memory = std::move(memory);
    s.parent_dir = parent_dir;
    const auto &manifest = (*input)->manifest_payload();
    s.manifest_digest = preserve_trx_digest(manifest.data(), manifest.size());
    const auto err = Preserve_trx_temp_import_work::begin(input, &s.source_import);
    if (err != DB_SUCCESS) return err;
    s.pipeline = true;
    if (previous) s.previous = std::move(*previous);
    *output = std::move(work);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

const Preserve_trx_temp_transfer_input *Preserve_trx_temp_receiver_work::input() const {
  return m_impl->source_import ? m_impl->source_import->input() : m_impl->input.get();
}

bool Preserve_trx_temp_receiver_work::preprepared() const {
  return m_impl->target_started && !m_impl->cancelling &&
         !m_impl->previous &&
         m_impl->failure == DB_SUCCESS && images_complete() &&
         m_impl->stats_ready && m_impl->sql_ready && m_impl->sql_ready->complete();
}
bool Preserve_trx_temp_receiver_work::authorize_final(
    const Preserve_snapshot_metadata &metadata,
    const Preserve_trx_transfer_receiver_record &record) {
  auto *source = const_cast<Preserve_trx_temp_transfer_input *>(input());
  return source && source->authorize_final(metadata, record);
}

bool Preserve_trx_temp_receiver_work::ready() const {
  return m_impl->ready && !m_impl->sql_bound && !m_impl->native_committed &&
      !m_impl->cancelling && m_impl->failure == DB_SUCCESS;
}

bool Preserve_trx_temp_receiver_work::promotion_safe() const {
  const auto &s = *m_impl;
  return s.pipeline && s.target_started && !s.source_import && !s.previous &&
      !s.cancelling && !s.sql_bound && !s.native_committed &&
      s.failure == DB_SUCCESS && s.input && s.input->final_authorized() &&
      s.plan && s.plan->target_dictionary_prepared() && s.undo_complete;
}

std::shared_ptr<Preserve_trx_temp_completion>
Preserve_trx_temp_completion::create(const Preserve_trx_temp_receiver_work &work) {
  if (!work.promotion_safe()) return {};
  auto memory = preserve_trx_acquire_memory_lease(
      work.input()->token(), Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER,
      sizeof(Preserve_trx_temp_completion) + work.input()->token().capacity() + 1);
  if (!memory.acquired()) return {};
  auto result = std::make_shared<Preserve_trx_temp_completion>();
  result->m_memory = std::make_unique<Preserve_memory_lease>(std::move(memory));
  result->m_token = work.input()->token();
  const auto &payload = work.input()->manifest_payload();
  result->m_digest = preserve_trx_digest(payload.data(), payload.size());
  result->m_contract = work.input()->contract();
  return result;
}

Preserve_trx_temp_completion::~Preserve_trx_temp_completion() = default;

bool Preserve_trx_temp_completion::matches(const std::string &token,
    const std::array<unsigned char, 32> &digest,
    const Preserve_trx_temp_id_contract &contract) const {
  return token == m_token && digest == m_digest && contract == m_contract;
}

bool Preserve_trx_temp_completion::pending() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_state == State::PENDING;
}

bool Preserve_trx_temp_completion::complete() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_state == State::COMPLETE && !m_cancelled;
}

bool Preserve_trx_temp_completion::cancelled() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_cancelled;
}

void Preserve_trx_temp_completion::cancel() {
  std::lock_guard<std::mutex> guard(m_mutex);
  m_cancelled = true;
  // Cancellation does not mean the last pwrite has returned. Keep PENDING
  // until the job relinquishes its owner; staging must remain pinned meanwhile.
  m_cv.notify_all();
}

void Preserve_trx_temp_completion::finish(
    Preserve_trx_temp_receiver_work::Owner *owner, dberr_t error) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_state != State::PENDING) return;
  if (m_cancelled) m_state = State::CANCELLED;
  else if (error != DB_SUCCESS || !owner || !*owner ||
           !(*owner)->matches(m_token, m_digest, m_contract))
    m_state = State::FAILED;
  else {
    m_owner = std::move(*owner);
    m_state = State::COMPLETE;
  }
  m_cv.notify_all();
}

bool Preserve_trx_temp_completion::wait(THD *thd, uint64_t deadline_us) {
  if (!thd || !deadline_us) return true;
  static PSI_stage_info waiting{0, "Waiting for preserved temporary I/O", 0};
  PSI_stage_info previous;
  thd->enter_stage(&waiting, &previous, __func__, __FILE__, __LINE__);
  const auto restore_stage = create_scope_guard([&] {
    thd->enter_stage(&previous, nullptr, __func__, __FILE__, __LINE__);
  });
  std::unique_lock<std::mutex> lock(m_mutex);
  while (m_state == State::PENDING && !m_cancelled) {
    const auto now = monotonic_us();
    if (thd->killed || now >= deadline_us) return true;
    m_cv.wait_for(lock, std::chrono::microseconds(
        std::min<uint64_t>(deadline_us - now, 10000)));
  }
  return thd->killed || monotonic_us() >= deadline_us ||
         m_cancelled || m_state != State::COMPLETE;
}

bool Preserve_trx_temp_completion::take(
    Preserve_trx_temp_receiver_work::Owner *output) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (!output || *output || m_state != State::COMPLETE || m_cancelled)
    return true;
  *output = std::move(m_owner);
  m_state = State::TAKEN;
  return false;
}

bool Preserve_trx_temp_receiver_work::matches(
    const std::string &token, const std::array<unsigned char, 32> &digest,
    const Preserve_trx_temp_id_contract &contract) const {
  return ready() && identity_matches(token, digest, contract);
}

bool Preserve_trx_temp_receiver_work::identity_matches(
    const std::string &token, const std::array<unsigned char, 32> &digest,
    const Preserve_trx_temp_id_contract &contract) const {
  return m_impl->token == token && m_impl->manifest_digest == digest &&
      m_impl->contract == contract;
}

void Preserve_trx_temp_receiver_work::set_sql_bound(bool bound) {
  m_impl->sql_bound = bound;
}

uint64_t Preserve_trx_temp_receiver_work::scanned_bytes() const {
  return m_impl->scanned;
}

dberr_t Preserve_trx_temp_receiver_work::prepare_step(
    THD *worker, size_t metadata_budget, size_t byte_budget, size_t page_budget) {
  auto &s = *m_impl;
  s.scanned = 0;
  if (!s.pipeline || s.cancelling || metadata_budget == 0 ||
      byte_budget == 0 || page_budget == 0) return DB_ERROR;
  if (s.failure != DB_SUCCESS || ready()) return s.failure;
  using Stage = Preserve_trx_temp_stage;
  const auto stage = [&] {
    if (s.source_import) return Stage::RECEIVER_SOURCE;
    if (!s.target_started || !s.plan->target_dictionary_prepared())
      return Stage::RECEIVER_DICTIONARY;
    if (!s.undo_complete) return Stage::RECEIVER_UNDO;
    if (!images_complete()) return s.next_space == s.images.size()
                                      ? Stage::RECEIVER_LOB : Stage::RECEIVER_IMAGE;
    if (!s.stats_ready) return Stage::RECEIVER_STATS;
    if (!s.sql_ready || !s.sql_ready->complete()) return Stage::RECEIVER_DD;
    return Stage::RECEIVER_NATIVE;
  }();
  Preserve_trx_temp_stage_timer timer(stage);
  const auto written_before = s.written;
  const auto note_io = create_scope_guard([&] {
    // Target setup replaces Impl; do not retain a reference to the old owner.
    timer.read(m_impl->scanned);
    timer.write(m_impl->written - written_before);
  });
  if (s.source_import) {
    if (!s.source_import->complete()) {
      const trx_preserve_temp_import_plan *previous = nullptr;
      if (s.previous && s.previous->preprepared() &&
          s.source_import->input()->same_lineage(*s.previous->input()) &&
          !s.previous->input()->final_authorized())
        previous = s.previous->plan();
      s.failure = s.source_import->step(metadata_budget, byte_budget,
                                        page_budget, previous);
      s.scanned = s.source_import->scanned_bytes();
      s.written += s.source_import->written_bytes();
      return s.failure;
    }
    s.failure = s.source_import->take(&s.input, &s.plan);
    if (s.failure == DB_SUCCESS) s.source_import.reset();
    return s.failure;
  }
  if (!s.target_started) {
    if (!s.input || !s.input->manifest()) return s.failure = DB_ERROR;
    if (s.previous) {
      auto &p = *s.previous->m_impl;
      if (!s.input->same_lineage(*p.input) || p.input->final_authorized() ||
          s.plan->space_count() != p.images.size()) {
        s.previous.reset();
        return DB_SUCCESS;
      }
      if (!s.growth_prepared) {
        uint64_t growth = 0;
        for (size_t i = 0; i < p.images.size(); ++i) {
          const auto bytes = s.plan->source_space(i)->image_bytes;
          if (bytes > p.images[i].descriptor.size) {
            const auto delta = bytes - p.images[i].descriptor.size;
            if (delta > UINT64_MAX - growth)
              return s.failure = DB_OUT_OF_FILE_SPACE;
            growth += delta;
          }
        }
        if (growth != 0) {
          s.growth = preserve_trx_acquire_file_resource_lease(
              p.dir, 1, growth);
          if (!s.growth.acquired()) return s.failure = DB_OUT_OF_FILE_SPACE;
        }
        if (p.comparison.empty()) {
          size_t bytes = std::max<size_t>(p.page.size(), 65536);
          if (!p.memory.grow_to(p.memory.bytes() + bytes)) {
            bytes = p.page.size();
            if (!p.memory.grow_to(p.memory.bytes() + bytes))
              return s.failure = DB_OUT_OF_MEMORY;
          }
          p.comparison.resize(bytes);
        }
        s.growth_prepared = true;
      }
      bool done = false, reused = false;
      s.failure = s.plan->reuse_private_batch(p.plan.get(), metadata_budget,
          byte_budget, &done, &reused,
          s.input->manifest()->owner_trx_id != p.owner_trx_id);
      if (s.failure != DB_SUCCESS || !done) return s.failure;
      if (!reused) {
        s.previous.reset();
        s.growth.release();
        return DB_SUCCESS;
      }
      // Keep the target file owner and its lease. Retire the previous source
      // graph/dictionaries/SQL borrowers in the small source owner's shell.
      // All fallible work preceded the first native ownership transfer.
      Owner donor = std::move(s.previous);
      for (size_t n = 0; n < p.images.size(); ++n) {
        p.images[n].previous_bytes = p.images[n].descriptor.size;
        p.images[n].previous_source = p.plan->source_file(n);
      }
      p.plan.swap(s.plan);
      p.input.swap(s.input);
      p.owner_trx_id = p.input->manifest()->owner_trx_id;
      p.sql_ready.swap(s.sql_ready);
      p.manifest_digest.swap(s.manifest_digest);
      p.growth = std::move(s.growth);
      p.next_space = p.next_page = p.seal_space = 0;
      p.written = s.written;
      p.allocated_bytes = p.scanned = 0;
      p.stats_ready = p.undo_complete = false;
      m_impl.swap(donor->m_impl);
      return DB_SUCCESS;
    }
    // Target factory consumes the plan only after all fallible setup. Until
    // then this outer owner retains the complete source work for cancellation.
    std::unique_ptr<Preserve_trx_temp_receiver_work> target;
    s.failure = begin(s.parent_dir, s.input->token(),
                     s.input->manifest()->owner_trx_id, s.input->contract(),
                     &s.plan, &target);
    if (s.failure != DB_SUCCESS) return s.failure;
    target->m_impl->input = std::move(s.input);
    target->m_impl->manifest_digest = s.manifest_digest;
    target->m_impl->written = s.written;
    target->m_impl->pipeline = target->m_impl->target_started = true;
    m_impl.swap(target->m_impl);
    return DB_SUCCESS;
  }
  if (!images_complete()) {
    const auto before = s.plan->source_read_bytes();
    const auto err = step(metadata_budget, metadata_budget, byte_budget, page_budget);
    s.scanned += s.plan->source_read_bytes() - before;
    return err;
  }
  if (!s.stats_ready) {
    DBUG_EXECUTE_IF("preserve_temp_stats_one_page", { page_budget = 1; });
    DBUG_EXECUTE_IF("preserve_temp_stats_two_pages", { page_budget = 2; });
    DBUG_EXECUTE_IF("preserve_temp_stats_three_pages", { page_budget = 3; });
    s.failure = s.plan->prepare_target_stats_batch(
        s.installation_dir, page_budget, &s.stats_ready, &s.scanned,
        s.plan->target_stats_space() < s.images.size()
            ? s.images[s.plan->target_stats_space()].writer.get()
            : nullptr);
    DBUG_PRINT("preserve_temp_import",
               ("temporary receiver stats batch budget=%zu scanned=%llu",
                page_budget, static_cast<unsigned long long>(s.scanned)));
    return s.failure;
  }
  if (!s.sql_ready) {
    s.failure = Preserve_trx_temp_sql_ready::begin(*s.input, &s.sql_ready);
    if (s.failure != DB_SUCCESS) return s.failure;
  }
  if (!s.sql_ready->complete())
    return s.failure = s.sql_ready->step(worker, *s.input, s.plan.get(), metadata_budget);
  // Only final authentication can publish native resources. Sampling and SDI
  // decoding above operate on this owner's private objects and immutable file.
  if (!s.input->final_authorized()) return DB_SUCCESS;
  if (s.seal_space < s.images.size()) {
    s.failure = s.seal_image(s.seal_space);
    if (s.failure == DB_SUCCESS) ++s.seal_space;
    return s.failure;
  }
  if (s.publish_space < s.plan->space_count()) {
    if (!s.file_attached) {
      s.failure = attach_file(s.publish_space);
      if (s.failure == DB_SUCCESS) s.file_attached = true;
      return s.failure;
    }
    const auto *bindings = s.plan->target_bindings(s.publish_space);
    while (metadata_budget-- != 0 && s.publish_table < bindings->size()) {
      s.failure = publish_table(s.publish_space, s.publish_table);
      if (s.failure != DB_SUCCESS) return s.failure;
      s.failure = s.plan->prepare_target_table_open(s.publish_space, s.publish_table);
      if (s.failure != DB_SUCCESS) return s.failure;
      ++s.publish_table;
    }
    if (s.publish_table == bindings->size()) {
      ++s.publish_space;
      s.publish_table = 0;
      s.file_attached = false;
    }
    return DB_SUCCESS;
  }
  if (!s.native_directory) {
    s.failure = trx_preserve_temp_native_directory::create(
        s.token, s.dir, s.installation_dir, &s.native_directory);
    if (s.failure != DB_SUCCESS) return s.failure;
    s.owns_directory = s.owns_installation_directory = false;
  }
  if (!s.native_ready) {
    s.failure = s.plan->prepare_native_handoff_batch(
        s.native_directory, metadata_budget, &s.native_ready);
    return s.failure;
  }
  s.ready = true;
  return s.failure;
}

#ifndef NDEBUG
size_t Preserve_trx_temp_receiver_work::sql_table_count() const {
  return ready() && m_impl->sql_ready ? m_impl->sql_ready->table_count() : 0;
}
#endif

dberr_t Preserve_trx_temp_receiver_work::begin_undo(
    const std::string &token, const std::string &object_id,
    std::shared_ptr<const Preserve_trx_sealed_file> file, Owner *output,
    std::shared_ptr<const Preserve_trx_sealed_file> base) {
  if (!output || *output) return DB_ERROR;
  try {
    auto memory = preserve_trx_acquire_memory_lease(token,
        Preserve_trx_memory_kind::TEMP_UNDO_IMPORT,
        sizeof(Impl) + sizeof(Preserve_trx_temp_receiver_work) + 128);
    if (!memory.acquired()) return DB_OUT_OF_MEMORY;
    Owner work(new Preserve_trx_temp_receiver_work());
    work->m_impl->memory = std::move(memory);
    if (base) {
      work->m_impl->token = token;
      work->m_impl->early_delta.reset(new Preserve_trx_temp_delta_reader);
      if (!work->m_impl->early_delta->begin(token, object_id, std::move(base),
                                           std::move(file))) return DB_CORRUPTION;
      work->m_impl->scanned = work->m_impl->early_delta->read_bytes();
      *output = std::move(work);
      return DB_SUCCESS;
    }
    const auto err = trx_preserve_temp_undo_input::begin_independent(
        token, object_id, std::move(file), &work->m_impl->early_undo);
    if (err != DB_SUCCESS) return err;
    work->m_impl->scanned = work->m_impl->early_undo->read_bytes();
    *output = std::move(work);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

dberr_t Preserve_trx_temp_receiver_work::step_undo(size_t pages, bool *complete) {
  auto &s = *m_impl;
  if (s.cancelling || !pages || !complete) return DB_ERROR;
  if (s.early_delta && !s.early_undo) {
    *complete = false;
    bool merged = false;
    const auto before = s.early_delta->read_bytes();
    const auto writes = s.early_delta->written_bytes();
    const bool ok = s.early_delta->step(std::min<size_t>(pages, 1024) * 16384, &merged);
    s.scanned = s.early_delta->read_bytes() - before;
    s.written += s.early_delta->written_bytes() - writes;
    if (!ok) return DB_CORRUPTION;
    if (merged) {
      const auto err = trx_preserve_temp_undo_input::begin_independent(
          s.token, s.early_delta->logical_name(), s.early_delta->file(), &s.early_undo);
      if (err != DB_SUCCESS) return err;
      s.scanned += s.early_undo->read_bytes();
    }
    return DB_SUCCESS;
  }
  if (!s.early_undo) return DB_ERROR;
  const auto before = s.early_undo->read_bytes();
  const auto err = s.early_undo->step(pages, complete);
  s.scanned = s.early_undo->read_bytes() - before;
  return err;
}

dberr_t Preserve_trx_temp_receiver_work::take_undo(
    const Preserved_temp_table_undo_descriptor &d,
    std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
    Preserve_memory_lease *memory) {
  auto &s = *m_impl;
  if (s.cancelling || !s.early_undo) return DB_ERROR;
  if (s.early_delta && !s.early_delta->matches(d)) return DB_CORRUPTION;
  const auto *decoded = s.early_undo->source();
  if (!decoded || !decoded->undo_only ||
      d.source_space_id != d.no_redo_undo_rseg_space_id ||
      decoded->source_space_id != d.source_space_id ||
      decoded->page_size != d.page_size ||
      decoded->no_redo_undo_rseg_space_id != d.no_redo_undo_rseg_space_id ||
      decoded->no_redo_undo_rseg_page_no != d.no_redo_undo_rseg_page_no ||
      decoded->no_redo_undo_rseg_slot != d.no_redo_undo_rseg_slot)
    return DB_CORRUPTION;
  return s.early_undo->take(source, memory);
}

void Preserve_trx_temp_receiver_work::Retire::operator()(
    Preserve_trx_temp_receiver_work *work) const noexcept {
  if (!work) return;
  {
    std::lock_guard<std::mutex> guard(retired_mutex);
    if (retired_tail) retired_tail->m_retired_next = work;
    else retired_head = work;
    retired_tail = work;
    retired_count.fetch_add(1, std::memory_order_release);
#ifndef NDEBUG
    retired_outstanding.fetch_add(1, std::memory_order_release);
#endif
  }
  preserved_trx_request_expired_reaper_scan();
}

bool Preserve_trx_temp_receiver_work::reap_once() {
  if (retired_count.load(std::memory_order_acquire) == 0) return false;
  Preserve_trx_temp_receiver_work *work;
  {
    std::lock_guard<std::mutex> guard(retired_mutex);
    work = retired_head;
    if (!work || work->m_retry_after_us > monotonic_us()) return false;
    retired_head = work->m_retired_next;
    if (!retired_head) retired_tail = nullptr;
    work->m_retired_next = nullptr;
    retired_count.fetch_sub(1, std::memory_order_release);
  }
  bool complete = false;
  const auto err = work->cancel_step(&complete);
  if (complete) {
    delete work;
#ifndef NDEBUG
    retired_outstanding.fetch_sub(1, std::memory_order_release);
#endif
  } else {
    work->m_retry_after_us = err == DB_SUCCESS ? 0 : monotonic_us() + 1000000;
    // Do not self-wake on persistent IO/Busy failures.
    std::lock_guard<std::mutex> guard(retired_mutex);
    if (retired_tail) retired_tail->m_retired_next = work;
    else retired_head = work;
    retired_tail = work;
    retired_count.fetch_add(1, std::memory_order_release);
  }
  if (err == DB_SUCCESS && retired_count.load(std::memory_order_acquire) != 0)
    preserved_trx_request_expired_reaper_scan();
  return true;
}

#ifndef NDEBUG
uint64_t Preserve_trx_temp_receiver_work::retired_owners() {
  return retired_outstanding.load(std::memory_order_acquire);
}
#endif

void Preserve_trx_temp_receiver_work::shutdown_retired() {
#ifndef DBUG_OFF
  bool cleanup_fault = false;
  DBUG_EXECUTE_IF("preserve_temp_receiver_shutdown_cleanup_oom", {
    DBUG_PUSH("+d,preserve_temp_image_cleanup_oom");
    cleanup_fault = true;
  });
  const auto restore_debug = create_scope_guard([&] {
    if (cleanup_fault) DBUG_POP();
  });
#endif
  Preserve_trx_temp_receiver_work *head;
  {
    std::lock_guard<std::mutex> guard(retired_mutex);
    // The caller has joined both cleanup executors and stopped producers.
    DBUG_ASSERT(retired_count.load() == retired_outstanding.load());
    head = retired_head;
    retired_head = retired_tail = nullptr;
    retired_count.store(0);
  }
  while (head != nullptr) {
    auto *work = head;
    head = head->m_retired_next;
    work->m_retired_next = nullptr;
    auto &s = *work->m_impl;
    if (s.previous) {
      // Executors are stopped: a nested donor must join this local drain,
      // rather than enqueue behind the already detached shutdown list.
      auto *donor = s.previous.release();
      donor->m_retired_next = head;
      head = donor;
#ifndef NDEBUG
      retired_outstanding.fetch_add(1, std::memory_order_release);
#endif
    }
    s.cancelling = true;
    // Scratch undo is on InnoDB's native rseg lists, outside the transaction
    // shutdown list. A failed unlink/fsync must not strand it there. Undo
    // already handed to a resumed transaction is not owned by this plan.
    if (s.plan != nullptr) {
      // The native discard also revokes an uncommitted attach journal.
      while (!s.plan->discard_target_undo_step()) {}
    }
    bool complete = false;
    while (!complete && work->cancel_step(&complete) == DB_SUCCESS) {}
    if (complete) {
      delete work;
#ifndef NDEBUG
      retired_outstanding.fetch_sub(1);
#endif
    } else {
      // Private native indexes are charged to dict_sys even when unpublished;
      // dict_close cannot find them. Retire them before leaving file debt.
      if (s.plan) s.plan->discard_native_for_process_shutdown();
      // Do not spin on persistent IO failures or block the other owners.
      // Raw debt is never destructed after InnoDB stops; boot GC owns files.
      if (retired_tail) retired_tail->m_retired_next = work;
      else retired_head = work;
      retired_tail = work;
      retired_count.fetch_add(1);
    }
  }
}

dberr_t Preserve_trx_temp_receiver_work::begin(
    const std::string &parent_dir, const std::string &token,
    uint64_t owner_trx_id, const Preserve_trx_temp_id_contract &contract,
    std::unique_ptr<trx_preserve_temp_import_plan> *plan,
    std::unique_ptr<Preserve_trx_temp_receiver_work> *output) {
  Preserve_trx_temp_id_contract local;
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable ||
      !contract.supported() || !preserve_trx_temp_id_local_contract(&local) ||
      contract != local) return DB_UNSUPPORTED;
  if (output == nullptr || *output != nullptr || plan == nullptr || *plan == nullptr ||
      (*plan)->target_ids_allocated() || ((*plan)->space_count() == 0 && !(*plan)->undo_only()) ||
      !valid_token(token) || parent_dir.empty() ||
      ((owner_trx_id == 0) != (*plan)->resource_only())) return DB_ERROR;
  try {
    const auto page_size = (*plan)->undo_only() ? (*plan)->source_undo_page_size()
                                               : (*plan)->source_space(0)->page_size;
    if (page_size == 0) return DB_ERROR;
    // Charge retained descriptors/names and the fixed page/hash workspace.
    uint64_t bytes = sizeof(Impl) + sizeof(Preserve_trx_temp_receiver_work) +
                     page_size + 4096;
    const auto add = [&](uint64_t n) {
      if (n > UINT64_MAX - bytes) return false;
      bytes += n;
      return true;
    };
    if (parent_dir.size() > UINT64_MAX / 16 - token.size() - 128)
      return DB_OUT_OF_MEMORY;
    const uint64_t path_bytes = parent_dir.size() + token.size() + 128;
    // Directory names, writer paths and transient carrier paths.
    // Per-image installation paths are retained separately.
    if (!add(16 * path_bytes)) return DB_OUT_OF_MEMORY;
    uint64_t image_bytes = 0;
    for (size_t n = 0; n < (*plan)->space_count(); ++n) {
      const auto *tables = (*plan)->source_bindings(n);
      const auto *source = (*plan)->source_space(n);
      if (tables == nullptr || tables->empty() || source == nullptr ||
          source->page_size == 0 || source->page_size != page_size ||
          source->image_bytes > preserve_trx_max_temp_sidecar_bytes) return DB_ERROR;
      if (source->image_bytes > UINT64_MAX - image_bytes) return DB_OUT_OF_FILE_SPACE;
      image_bytes += source->image_bytes;
      if (!add(sizeof(Impl::Image) + 2 * (token.size() + 64)) ||
          !add(16 * path_bytes + 4096)) return DB_OUT_OF_MEMORY;
      for (const auto &index : tables->front().indexes) {
        if (!add(sizeof(Preserved_temp_table_image_descriptor::Index_descriptor) +
                 2 * index.name.size() + 64)) return DB_OUT_OF_MEMORY;
      }
    }
    auto memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER, bytes);
    if (!memory.acquired()) return DB_OUT_OF_MEMORY;
    auto work = std::unique_ptr<Preserve_trx_temp_receiver_work>(
        new Preserve_trx_temp_receiver_work());
    auto &state = *work->m_impl;
    state.memory = std::move(memory);
    state.contract = contract;
    state.token = token;
    state.owner_trx_id = owner_trx_id;
    state.images.resize((*plan)->space_count());
    state.page.resize(page_size);
    std::array<unsigned char, 16> random;
    if (my_rand_buffer(random.data(), random.size())) return DB_ERROR;
    static constexpr char hex[] = "0123456789abcdef";
    if (!preserve_trx_temp_receiver_process_dir(parent_dir, &state.dir))
      return DB_IO_ERROR;
    state.dir += "/temp-import-";
    for (auto b : random) {
      state.dir.push_back(hex[b >> 4]);
      state.dir.push_back(hex[b & 15]);
    }
    // Never reuse or recursively clear a prior candidate's directory.
    if (my_mkdir(state.dir.c_str(), 0700, MYF(0))) return DB_IO_ERROR;
    state.owns_directory = true;
    state.installation_dir = state.dir + "/install";
    if (my_mkdir(state.installation_dir.c_str(), 0700, MYF(0))) return DB_IO_ERROR;
    state.owns_installation_directory = true;
    // This file lease survives writer close until native ownership takes over.
    state.files = preserve_trx_acquire_file_resource_lease(
        state.dir, 1, image_bytes);
    if (!state.files.acquired()) return DB_OUT_OF_FILE_SPACE;
    state.plan = std::move(*plan);
    *output = std::move(work);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return DB_OUT_OF_MEMORY;
  }
}

Preserve_trx_temp_receiver_work::~Preserve_trx_temp_receiver_work() {
  // Fallback only. Production worker retirement must drive cancel_step before
  // destruction; persistent file failures remain garbage, never live imports.
  bool complete = false;
  while (!complete && cancel_step(&complete) == DB_SUCCESS) {}
}

dberr_t Preserve_trx_temp_receiver_work::step(
    size_t metadata_work_budget, size_t undo_record_budget,
    size_t undo_byte_budget, size_t page_budget) {
  auto &s = *m_impl;
  if (s.cancelling || s.plan == nullptr || metadata_work_budget == 0 || undo_record_budget == 0 ||
      undo_byte_budget == 0 || page_budget == 0) return DB_ERROR;
  if (s.failure != DB_SUCCESS || images_complete()) return s.failure;
  try {
    if (!s.plan->target_ids_allocated()) {
      Preserve_trx_temp_id_contract local;
      if (!preserve_trx_temp_id_local_contract(&local) || local != s.contract)
        return s.failure = DB_UNSUPPORTED;
      s.failure = s.plan->allocate_target_ids();
      if (s.failure != DB_SUCCESS) return s.failure;
    }
    if (!s.plan->target_dictionary_prepared()) {
      bool complete = false;
      s.failure = s.plan->prepare_target_dictionary_batch(
          s.token, metadata_work_budget, &complete);
      return s.failure;
    }
    if (!s.undo_complete) {
      s.failure = s.plan->prepare_target_undo_batch(
          s.token, undo_record_budget, undo_byte_budget, &s.undo_complete);
      return s.failure;
    }
    if (s.next_space == s.images.size()) {
      bool complete = false;
      s.failure = s.plan->prepare_lob_batch(metadata_work_budget, undo_byte_budget, &complete);
      return s.failure;
    }
    if (!s.digest) {
      s.failure = s.start_image();
      if (s.failure != DB_SUCCESS) return s.failure;
    }
    const auto *source = s.plan->source_space(s.next_space);
    uint64_t comparison_offset = 0, comparison_end = 0;
    for (size_t n = 0; n < page_budget &&
         s.next_page < source->image_bytes / source->page_size; ++n) {
      s.failure = s.plan->read_source_page(s.next_space, s.next_page,
                                           s.page.data(), s.page.size());
      if (s.failure != DB_SUCCESS) return s.failure;
      bool allocated = false;
      s.failure = s.plan->rewrite_data_page(s.token, s.next_space, s.next_page,
          s.owner_trx_id, s.page.data(), s.page.size(), &allocated);
      if (s.failure != DB_SUCCESS) return s.failure;
      s.failure = s.plan->finish_target_page(s.next_space, s.page.data(), s.page.size());
      if (s.failure != DB_SUCCESS) return s.failure;
      DBUG_EXECUTE_IF("preserve_temp_receiver_write_failure", {
        return s.failure = DB_IO_ERROR;
      });
      auto &image = s.images[s.next_space];
      const auto offset = uint64_t{s.next_page} * source->page_size;
      bool changed = true;
      if (offset < image.previous_bytes && image.previous_source &&
          image.previous_source->known_zero_range(offset, s.page.size()) &&
          std::all_of(s.page.begin(), s.page.end(),
                      [](unsigned char byte) { return byte == 0; })) {
        // A fully prepared private donor leaves uninitialized zero pages zero.
        // This proof survives owner/undo remapping; nonzero pages still compare
        // after conversion. Keep the full target digest below.
        changed = false;
      } else if (offset < image.previous_bytes) {
        if (offset >= comparison_end) {
          // Read this batch's unmodified donor pages once. Never reuse a
          // written page or retain the range beyond this step.
          const auto pages = std::min<uint64_t>({
              s.comparison.size() / source->page_size, page_budget - n,
              (image.previous_bytes - offset) / source->page_size,
              (source->image_bytes - offset) / source->page_size});
          if (!pages) return s.failure = DB_ERROR;
          const size_t bytes = pages * source->page_size;
          DBUG_PRINT("preserve_temp_reuse_ids", ("temporary comparison read bytes=%zu page_size=%zu",
              bytes, s.page.size()));
          s.failure = carrier_error(image.writer->read_at(
              offset, s.comparison.data(), bytes));
          if (s.failure != DB_SUCCESS) return s.failure;
          s.scanned += bytes;
          comparison_offset = offset;
          comparison_end = offset + bytes;
        }
        changed = memcmp(s.comparison.data() + (offset - comparison_offset),
                         s.page.data(), s.page.size()) != 0;
      } else if (image.previous_bytes == 0 &&
                 offset + s.page.size() < source->image_bytes &&
                 std::all_of(s.page.begin(), s.page.end(),
                             [](unsigned char byte) { return byte == 0; })) {
        // A fresh private file has zero-filled holes. Keep the last page write
        // to establish the exact size without a later zero-filling truncate.
        // Donor files must still overwrite old nonzero contents normally.
        changed = false;
      }
      if (changed) {
        DBUG_EXECUTE_IF("preserve_temp_receiver_installation_write_failure", {
          DBUG_PRINT("preserve_temp_async_fault",
                     ("temporary async write failure token=%s", s.token.c_str()));
          return s.failure = DB_IO_ERROR;
        });
        s.failure = carrier_error(image.writer->write_at(
            offset, s.page.data(), s.page.size()));
        if (s.failure != DB_SUCCESS) return s.failure;
        s.written += s.page.size();
        if (offset >= image.previous_bytes) s.allocated_bytes += s.page.size();
      }
      if (EVP_DigestUpdate(s.digest.get(), s.page.data(), s.page.size()) != 1)
        return s.failure = DB_ERROR;
      ++s.next_page;
    }
    // Settle once per worker batch, not once per page. Failed partial writes
    // retain unsettled credit until cancellation and never advance next_page.
    s.files.settle_writes(s.allocated_bytes);
    if (s.growth.acquired()) s.growth.settle_writes(s.allocated_bytes);
    if (s.next_page == source->image_bytes / source->page_size)
      s.failure = s.checkpoint_image();
    return s.failure;
  } catch (const std::bad_alloc &) {
    return s.failure = DB_OUT_OF_MEMORY;
  }
}

bool Preserve_trx_temp_receiver_work::images_complete() const {
  const auto &s = *m_impl;
  return !s.cancelling && s.plan != nullptr && s.failure == DB_SUCCESS &&
         s.plan->target_dictionary_prepared() && s.undo_complete &&
         s.next_space == s.images.size() && s.plan->lob_complete();
}
uint64_t Preserve_trx_temp_receiver_work::written_bytes() const {
  return m_impl->written;
}
const trx_preserve_temp_import_plan *Preserve_trx_temp_receiver_work::plan() const {
  return m_impl->plan.get();
}
#ifndef NDEBUG
const Preserved_temp_table_image_descriptor *
Preserve_trx_temp_receiver_work::image(size_t n) const {
  return images_complete() && n < m_impl->images.size()
             ? &m_impl->images[n].descriptor : nullptr;
}
const std::string &Preserve_trx_temp_receiver_work::directory() const {
  return m_impl->dir;
}
#endif

const std::string *Preserve_trx_temp_receiver_work::installation_path(size_t n) const {
  return images_complete() && n < m_impl->images.size()
             ? &m_impl->images[n].installation_path : nullptr;
}

dberr_t Preserve_trx_temp_receiver_work::attach_file(size_t n) {
  const auto *path = installation_path(n);
  return path != nullptr ? m_impl->plan->attach_target_fil_space(n, *path) : DB_ERROR;
}


dberr_t Preserve_trx_temp_receiver_work::publish_table(size_t n, size_t t) {
  return images_complete() ? m_impl->plan->publish_target_dictionary(n, t) : DB_ERROR;
}

#ifndef NDEBUG
dberr_t Preserve_trx_temp_receiver_work::prepare_native_handoff() {
  if (!images_complete()) return DB_ERROR;
  if (!preserved_trx_expired_reaper_running()) return DB_ERROR;
  auto &s = *m_impl;
  if (!s.native_directory) {
    const auto err = trx_preserve_temp_native_directory::create(
        s.token, s.dir, s.installation_dir, &s.native_directory);
    if (err != DB_SUCCESS) return err;
    s.owns_directory = s.owns_installation_directory = false;
  }
  return s.plan->prepare_native_handoff(s.native_directory);
}
#endif

dberr_t Preserve_trx_temp_receiver_work::commit_native_handoff(trx_t *trx) {
  if (!images_complete() || !m_impl->native_directory) return DB_ERROR;
  const auto err = m_impl->plan->commit_native_handoff(trx);
  if (err != DB_SUCCESS) return err;
  for (auto &image : m_impl->images) image.owned = false;
  m_impl->native_committed = true;
  return DB_SUCCESS;
}

Preserve_trx_temp_sql_ready *Preserve_trx_temp_receiver_work::sql_ready() {
  return m_impl->sql_ready.get();
}

dberr_t Preserve_trx_temp_receiver_work::attach_undo(
    trx_t *trx, uint64_t savepoint_floor) {
  return m_impl->sql_bound && !native_committed() && !m_impl->cancelling
      ? m_impl->plan->attach_target_undo(trx, savepoint_floor) : DB_ERROR;
}

dberr_t Preserve_trx_temp_receiver_work::rollback_undo(trx_t *trx) {
  return !native_committed() && m_impl->plan
      ? m_impl->plan->rollback_target_undo_attach(trx) : DB_ERROR;
}

bool Preserve_trx_temp_receiver_work::native_committed() const {
  return m_impl->native_committed;
}

dberr_t Preserve_trx_temp_receiver_work::cancel_step(bool *complete) {
  if (complete == nullptr) return DB_ERROR;
  auto &s = *m_impl;
  *complete = s.cancelled;
  s.cancelling = true;
  if (s.cancelled) return DB_SUCCESS;
  try {
    if (s.previous) {
      s.previous.reset();
      return DB_SUCCESS;
    }
    if (s.early_undo) {
      if (s.early_undo->cancel_step(128)) s.early_undo.reset();
      return DB_SUCCESS;
    }
    if (s.early_delta) {
      s.early_delta.reset();
      return DB_SUCCESS;
    }
    if (s.sql_ready) {
      if (s.sql_ready->cancel_step(128)) s.sql_ready.reset();
      return DB_SUCCESS;
    }
    if (s.source_import) {
      if (s.source_import->cancel_step(128)) s.source_import.reset();
      return DB_SUCCESS;
    }
    if (s.plan != nullptr) {
      const auto err = s.plan->cancel_native_handoff();
      if (err != DB_SUCCESS) return err;
    }
    if (s.cleanup_space < s.images.size()) {
      const auto *target = s.plan == nullptr ? nullptr : s.plan->target_space(s.cleanup_space);
      if (target != nullptr && !target->bound_dict_tables.empty())
        return s.plan->rollback_target_dictionary_publish(s.cleanup_space);
      if (target != nullptr && target->fil_space_adopted) {
        // Keep the file and this cleanup cursor if native retirement fails.
        // A later dictionary/TABLE journal must revoke those references first.
        return s.plan->rollback_target_fil_space(s.cleanup_space);
      }
      auto &image = s.images[s.cleanup_space];
      auto &writer = image.writer;
      if (writer) {
        const auto status = writer->abort();
        if (status != Preserved_trx_carrier_status::OK) return carrier_error(status);
        writer.reset();
        s.digest.reset();
        return DB_SUCCESS;
      }
      auto &owned = image.owned;
      const auto &dir = s.installation_dir;
      if (owned) {
        Local_file_preserved_temp_table_image_carrier carrier(dir);
        const auto status = carrier.remove_sealed_image(s.token, image.descriptor.source_space_id);
        if (status != Preserved_trx_carrier_status::OK) {
          s.cleanup_sync_pending = true;
          return carrier_error(status);
        }
        if (s.cleanup_sync_pending && sync_directory(dir)) return DB_IO_ERROR;
        s.cleanup_sync_pending = false;
        owned = false;
      }
      ++s.cleanup_space;
      return DB_SUCCESS;
    }
    if (s.plan != nullptr && !s.plan->discard_target_undo_step()) return DB_SUCCESS;
    if (s.plan != nullptr && !s.plan->discard_source_metadata_step(128)) return DB_SUCCESS;
    if (s.owns_installation_directory) {
      if (rmdir(s.installation_dir.c_str()) != 0 && errno != ENOENT) return DB_IO_ERROR;
      s.owns_installation_directory = false;
    }
    if (s.owns_directory) {
      if (rmdir(s.dir.c_str()) != 0 && errno != ENOENT) return DB_IO_ERROR;
      s.owns_directory = false;
    }
    s.plan.reset();
    if (s.input && !s.input->cancel_step(128)) return DB_SUCCESS;
    s.input.reset();
    s.native_directory.reset();
    s.files.release();
    s.growth.release();
    s.digest.reset();
    const auto retained_memory = sizeof(Impl) + sizeof(Preserve_trx_temp_receiver_work) +
                                 2 * s.token.size() + 64;
    std::vector<Impl::Image>().swap(s.images);
    std::vector<unsigned char>().swap(s.page);
    std::vector<unsigned char>().swap(s.comparison);
    std::string().swap(s.dir);
    std::string().swap(s.installation_dir);
    std::string().swap(s.token);
    if (s.memory.acquired())
      (void)s.memory.shrink_to(retained_memory);
    *complete = s.cancelled = true;
    DBUG_PRINT("preserve_temp_import", ("temporary receiver cleanup complete page_import_bytes=%llu",
        static_cast<unsigned long long>(preserve_trx_resource_kind_current_bytes(
            Preserve_trx_memory_kind::TEMP_PAGE_IMPORT))));
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) {
    return DB_OUT_OF_MEMORY;
  }
}
