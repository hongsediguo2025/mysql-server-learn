/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_receiver_candidates.h"

#include <atomic>
#include <map>
#include <mutex>
#include <new>
#include "scope_guard.h"
#include "sql/preserve_trx_result_manifest.h"
#include "sql/preserve_trx_temp_transfer.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_temp_receiver.h"
#include "sql/preserve_trx_temp_delta.h"
#include "sql/preserve_trx_temp_metrics.h"
#include "sql/preserve_trx_temp_table_carrier.h"
#include "sql/preserve_trx_transfer.h"
#include "sql/preserve_trx_transfer_index.h"
#include "sql/sql_class.h"

namespace {
std::atomic<uint64_t> early_ready{0}, early_rows{0}, early_reused{0}, early_abandoned{0};
std::atomic<uint64_t> undo_ready{0}, undo_reused{0}, undo_read_bytes{0}, undo_abandoned{0};
std::atomic<uint64_t> undo_superseded{0};
std::atomic<uint64_t> native_ready{0}, native_reused{0}, native_failed{0};
using Status = Preserve_trx_file_status;
bool same_file(const std::shared_ptr<const Preserve_trx_sealed_file> &a,
               const std::shared_ptr<const Preserve_trx_sealed_file> &b) {
  return a && b && a->matches(b->size(), b->digest());
}
std::string name(const Preserve_trx_cursor_descriptor &d) {
  return preserve_trx_result_object_name(
      {d.statement_id, d.generation, d.size, d.digest});
}
bool matches(const Preserve_trx_cursor_descriptor &a,
             const Preserve_trx_cursor_descriptor &b) {
  return a.statement_id == b.statement_id && a.generation == b.generation &&
         a.size == b.size && a.digest == b.digest && a.rows == b.rows &&
         a.rows_offset == b.rows_offset && a.index_offset == b.index_offset &&
         a.schema_digest == b.schema_digest;
}
int show(uint64_t value, SHOW_VAR *var, char *buffer) {
  var->type = SHOW_LONGLONG;
  var->value = buffer;
  *reinterpret_cast<long long *>(buffer) = static_cast<long long>(value);
  return 0;
}
}  // namespace

struct Preserve_trx_receiver_candidates::Impl {
  struct Result {
    Preserve_memory_lease memory;
    std::string id;
    uint64_t generation{0};
    std::unique_ptr<Preserve_trx_cursor_decoder> decoder;
    bool working{false}, ready{false}, failed{false}, final_claimed{false};
  };
  struct Undo {
    Preserve_memory_lease memory;
    std::string id;
    std::shared_ptr<const Preserve_trx_sealed_file> file;
    std::shared_ptr<const Preserve_trx_sealed_file> base;
    Preserve_trx_temp_receiver_work::Owner work;
    bool working{false}, ready{false}, failed{false}, final_claimed{false};
  };
  struct Temp {
    Preserve_memory_lease memory;
    std::string id, payload;
    std::shared_ptr<const Preserve_trx_sealed_file> file;
    Preserve_trx_temp_receiver_work::Owner work;
    Preserve_trx_temp_receiver_work::Owner previous;
    bool working{false}, ready{false}, failed{false}, final_claimed{false};
  };
  Preserve_memory_lease memory;
  std::shared_ptr<Temp> temp;
  std::string token;
  std::mutex mutex;
  std::atomic<bool> cancelled{false};
  std::map<uint32_t, std::shared_ptr<Result>> results;
  std::shared_ptr<Undo> undo;
  // Caller owns mutex. Each slot keeps its charge across worker/final handoff.
  std::shared_ptr<Result> slot(const std::string &id,
                              const Preserve_trx_result_manifest::Result &identity,
                              std::shared_ptr<Result> *retired) {
    auto it = results.find(identity.statement_id);
    if (it != results.end() && it->second->id == id) return it->second;
    auto memory = preserve_trx_acquire_memory_lease(token,
        Preserve_trx_memory_kind::SNAPSHOT_CODEC_BUFFER,
        sizeof(Result) + id.size() * 2 + 256);
    if (!memory.acquired()) return {};
    auto result = std::make_shared<Result>();
    result->memory = std::move(memory);
    result->id = id;
    result->generation = identity.generation;
    if (it == results.end())
      results.emplace(identity.statement_id, result);
    else {
      *retired = std::move(it->second);
      it->second = result;
    }
    return result;
  }
};

Preserve_trx_receiver_candidates::Preserve_trx_receiver_candidates(
    std::unique_ptr<Impl> impl) : m_impl(std::move(impl)) {}
Preserve_trx_receiver_candidates::~Preserve_trx_receiver_candidates() = default;

std::shared_ptr<Preserve_trx_receiver_candidates>
Preserve_trx_receiver_candidates::create(const std::string &token) {
  try {
    auto memory = preserve_trx_acquire_memory_lease(token,
        Preserve_trx_memory_kind::SNAPSHOT_CODEC_BUFFER,
        sizeof(Impl) + sizeof(Preserve_trx_receiver_candidates) +
            token.size() * 2 + 128);
    if (!memory.acquired()) return {};
    auto impl = std::make_unique<Impl>();
    impl->memory = std::move(memory);
    impl->token = token;
    return std::shared_ptr<Preserve_trx_receiver_candidates>(
        new Preserve_trx_receiver_candidates(std::move(impl)));
  } catch (const std::bad_alloc &) { return {}; }
}

bool Preserve_trx_receiver_candidates::step_result(
    const std::string &id, std::shared_ptr<const Preserve_trx_sealed_file> file,
    THD *worker, uint64_t rows, uint64_t bytes, bool *complete,
    uint64_t *scanned_bytes) {
  if (!complete || !scanned_bytes) return true;
  *complete = true;
  *scanned_bytes = 0;
  Preserve_trx_temp_stage_timer timer(Preserve_trx_temp_stage::RECEIVER_RESULT);
  const auto note_io = create_scope_guard([&] { timer.read(*scanned_bytes); });
  auto &s = *m_impl;
  if (!worker || worker != current_thd || worker->killed ||
      !rows || !bytes || !file || s.cancelled.load()) return true;
  Preserve_trx_result_manifest::Result identity;
  if (!preserve_trx_result_object_identity(id, &identity)) return true;
  std::shared_ptr<Impl::Result> result;
  std::shared_ptr<Impl::Result> retired;
  try {
    {
      std::lock_guard<std::mutex> lock(s.mutex);
      const auto current = s.results.find(identity.statement_id);
      if (current != s.results.end()) {
        if (current->second->final_claimed ||
            current->second->generation > identity.generation) return false;
        if (current->second->generation == identity.generation &&
            current->second->id != id) return true;
      }
      result = s.slot(id, identity, &retired);
      if (!result || result->failed || result->working) return true;
      if (result->ready || result->final_claimed) return false;
      result->working = true;
    }
    // A decoder can close files and release memory. Never retire it under the
    // candidates mutex; a worker already using it retains its own shared pin.
    retired.reset();
    bool failed = true;
    const auto finish = create_scope_guard([&] {
      std::lock_guard<std::mutex> lock(s.mutex);
      result->working = false;
      const auto current = s.results.find(identity.statement_id);
      result->failed = failed || s.cancelled.load() ||
                       current == s.results.end() || current->second != result;
      if (!result->failed && result->decoder->values_validated()) {
        result->ready = true;
        ++early_ready;
        early_rows += result->decoder->descriptor().rows;
      }
      *complete = result->failed || result->ready;
    });
    worker->reset_for_next_command();
    worker->get_stmt_da()->reset_condition_info(worker);
    if (!result->decoder) {
      Preserve_trx_cursor_descriptor descriptor;
      std::unique_ptr<Preserve_trx_cursor_file> input;
      if (Preserve_trx_cursor_file::describe(*file, &descriptor) != Status::OK ||
          name(descriptor) != id ||
          Preserve_trx_cursor_file::open(s.token, file, descriptor, &input) != Status::OK ||
          Preserve_trx_cursor_decoder::create(s.token, worker, std::move(input),
                                              &result->decoder) != Status::OK)
        return true;
      result->decoder->bind(nullptr);
    }
    if (!file->matches(result->decoder->descriptor().size,
                       result->decoder->descriptor().digest) ||
        result->decoder->preflight_next(rows, bytes, scanned_bytes) != Status::OK ||
        worker->killed || s.cancelled.load()) return true;
    failed = false;
    return false;
  } catch (const std::bad_alloc &) { return true; }
}

Preserve_trx_receiver_candidates::Take
Preserve_trx_receiver_candidates::take_result(
    const Preserve_trx_cursor_descriptor &descriptor,
    std::unique_ptr<Preserve_trx_cursor_decoder> *output) {
  if (!output || *output) return Take::FAILED;
  auto &s = *m_impl;
  try {
    std::unique_ptr<Preserve_trx_cursor_decoder> retired;
    std::shared_ptr<Impl::Result> obsolete;
    const auto id = name(descriptor);
    const Preserve_trx_result_manifest::Result identity{
        descriptor.statement_id, descriptor.generation, descriptor.size,
        descriptor.digest};
    std::lock_guard<std::mutex> lock(s.mutex);
    if (s.cancelled.load()) return Take::FAILED;
    const auto current = s.results.find(identity.statement_id);
    if (current != s.results.end() &&
        current->second->generation == identity.generation &&
        current->second->id != id) return Take::FAILED;
    // Final can select an earlier exact generation. Retire another optional
    // candidate and let the existing READY preparation validate the right file.
    auto result = s.slot(id, identity, &obsolete);
    if (!result) return Take::FAILED;
    if (result->failed) {
      // Optional preflight may have been cancelled or exhausted its budget.
      // Final revalidates the original verified file; no partial decoder moves.
      retired = std::move(result->decoder);
      result->final_claimed = true;
      return Take::ABSENT;
    }
    if (result->working || (result->decoder && !result->ready)) return Take::WAIT;
    if (result->ready) {
      if (!matches(result->decoder->descriptor(), descriptor)) return Take::FAILED;
      *output = std::move(result->decoder);
      result->ready = false;
      result->final_claimed = true;
      ++early_reused;
      return Take::READY;
    }
    result->final_claimed = true;
    return Take::ABSENT;
  } catch (const std::bad_alloc &) { return Take::FAILED; }
}

void Preserve_trx_receiver_candidates::abandon_result(const std::string &id) {
  Preserve_trx_result_manifest::Result identity;
  if (!preserve_trx_result_object_identity(id, &identity)) return;
  std::lock_guard<std::mutex> lock(m_impl->mutex);
  const auto found = m_impl->results.find(identity.statement_id);
  if (found == m_impl->results.end() || found->second->id != id) return;
  auto &result = *found->second;
  if (!result.ready && !result.working && !result.final_claimed &&
      !result.failed) {
    result.failed = true;
    ++early_abandoned;
  }
}

void Preserve_trx_receiver_candidates::register_temp(
    const std::string &id, std::shared_ptr<const Preserve_trx_sealed_file> file) {
  std::shared_ptr<Impl::Temp> retired;
  std::lock_guard<std::mutex> guard(m_impl->mutex);
  auto &s = *m_impl;
  if (s.cancelled || !file || (s.temp && s.temp->id == id &&
      same_file(s.temp->file, file))) return;
  try {
    const auto overhead = sizeof(Impl::Temp) + id.size() * 2 + 512;
    if (file->size() > SIZE_MAX - overhead || file->size() > (UINT64_MAX - overhead) / 2) return;
    auto memory = preserve_trx_acquire_memory_lease(s.token,
        Preserve_trx_memory_kind::TEMP_METADATA_IMPORT,
        overhead + 2 * file->size());
    if (!memory.acquired()) return;
    auto slot = std::make_shared<Impl::Temp>();
    slot->memory = std::move(memory);
    slot->id = id;
    slot->file = std::move(file);
    retired = std::move(s.temp);
    if (retired && !retired->working && retired->ready && !retired->failed &&
        !retired->final_claimed && retired->work && retired->work->preprepared())
      slot->previous = std::move(retired->work);
    s.temp = std::move(slot);
  } catch (const std::bad_alloc &) {}
}

bool Preserve_trx_receiver_candidates::step_temp(
    const std::string &root, const std::string &id,
    const Preserve_trx_transfer_receiver_record &record, THD *worker,
    size_t bytes, uint64_t max_bytes, bool *complete, uint64_t *scanned) {
  if (!complete || !scanned) return true;
  *complete = true;
  *scanned = 0;
  auto &s = *m_impl;
  std::shared_ptr<Impl::Temp> slot;
  {
    std::lock_guard<std::mutex> guard(s.mutex);
    slot = s.temp;
    if (s.cancelled || !slot || slot->id != id || slot->failed || slot->working)
      return true;
    if (slot->final_claimed || slot->ready) return false;
    slot->working = true;
  }
  bool failed = true;
  const auto finish = create_scope_guard([&] {
    std::lock_guard<std::mutex> guard(s.mutex);
    slot->working = false;
    slot->failed = failed || s.cancelled || s.temp != slot;
    if (slot->failed) ++native_failed;
    else if (slot->work && slot->work->preprepared()) {
      slot->ready = true;
      ++native_ready;
    }
    *complete = slot->failed || slot->ready;
  });
  try {
    if (!slot->work) {
      if (slot->payload.empty()) slot->payload.reserve(slot->file->size());
      const size_t offset = slot->payload.size();
      const size_t count = std::min<uint64_t>(bytes, slot->file->size() - offset);
      slot->payload.resize(offset + count);
      if (!slot->file->read_at(offset,
              reinterpret_cast<unsigned char *>(&slot->payload[offset]), count)) return true;
      *scanned = count;
      if (slot->payload.size() != slot->file->size()) { failed = false; return false; }
      std::unique_ptr<Preserve_trx_temp_transfer_input> input;
      std::vector<Preserve_trx_transfer_object_descriptor> dependencies;
      if (preserve_trx_temp_transfer_descriptors(s.token, slot->payload, &dependencies) !=
          Preserve_trx_transfer_status::OK) return true;
      uint64_t total = 0;
      for (const auto &dependency : dependencies) {
        if (dependency.total_size > max_bytes - total) return true;
        total += dependency.total_size;
      }
      if (Preserve_trx_temp_transfer_input::load(s.token, slot->payload, record,
            &input, nullptr, true) != Preserve_trx_transfer_status::OK ||
          Preserve_trx_temp_receiver_work::begin_import(
              root, &input, &slot->work, &slot->previous) != DB_SUCCESS)
        return true;
    }
    const auto written = slot->work->written_bytes();
    if (!slot->work->input()->matches_candidate(record)) return true;
    const auto err = slot->work->prepare_step(worker, 128, bytes, 128);
    *scanned += slot->work->scanned_bytes() + slot->work->written_bytes() - written;
    if (err != DB_SUCCESS) return true;
    failed = false;
    return false;
  } catch (const std::bad_alloc &) { return true; }
}

Preserve_trx_receiver_candidates::Take Preserve_trx_receiver_candidates::take_temp(
    const Preserve_snapshot_metadata &metadata,
    const Preserve_trx_transfer_receiver_record &record,
    Preserve_trx_temp_receiver_work::Owner *output) {
  if (!output || *output) return Take::FAILED;
  auto &s = *m_impl;
  std::shared_ptr<Impl::Temp> slot;
  {
    std::lock_guard<std::mutex> guard(s.mutex);
    if (s.cancelled) return Take::FAILED;
    slot = s.temp;
    if (!slot || slot->final_claimed) return Take::ABSENT;
    if (slot->working) return Take::WAIT;
    slot->final_claimed = true;
    s.temp.reset();
  }
  if (slot->failed || !slot->work ||
      slot->payload != metadata.temp_table_manifest_payload) return Take::ABSENT;
  if (!slot->work->authorize_final(metadata, record)) return Take::FAILED;
  *output = std::move(slot->work);
  ++native_reused;
  return Take::READY;
}

void Preserve_trx_receiver_candidates::register_undo(
    const std::string &id, std::shared_ptr<const Preserve_trx_sealed_file> file,
    std::shared_ptr<const Preserve_trx_sealed_file> base) {
  auto &s = *m_impl;
  std::shared_ptr<Impl::Undo> retired;
  std::lock_guard<std::mutex> lock(s.mutex);
  if (s.cancelled.load() || !file || (s.undo && s.undo->id == id &&
      same_file(s.undo->file, file) &&
      ((!base && !s.undo->base) || same_file(base, s.undo->base)))) return;
  // A generation has one owner. An old active batch retains its shared slot;
  // its eventual destruction only queues bounded cleanup on the existing reaper.
  retired = std::move(s.undo);
  if (retired) ++undo_superseded;
  try {
    auto memory = preserve_trx_acquire_memory_lease(s.token,
        Preserve_trx_memory_kind::TEMP_UNDO_IMPORT,
        sizeof(Impl::Undo) + 2 * id.size() + 128);
    if (!memory.acquired()) return;
    auto undo = std::make_shared<Impl::Undo>();
    undo->memory = std::move(memory);
    undo->id = id;
    undo->file = std::move(file);
    undo->base = std::move(base);
    s.undo = std::move(undo);
  } catch (const std::bad_alloc &) { /* Optional; final reads the sealed file. */ }
}

void Preserve_trx_receiver_candidates::retain_selected_undo(
    const Preserve_trx_transfer_receiver_record &record) {
  std::shared_ptr<Impl::Undo> retired;
  std::lock_guard<std::mutex> lock(m_impl->mutex);
  if (!m_impl->undo) return;
  const auto *selected = record.object_index
      ? record.object_index->find(record.objects, m_impl->undo->id) : nullptr;
  if (!selected || selected->kind != Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR ||
      !m_impl->undo->file->matches(selected->total_size, selected->digest)) {
    retired = std::move(m_impl->undo);
    return;
  }
  try {
    if (m_impl->undo->base) {
      std::string id;
      const auto *base = preserve_trx_temp_undo_delta_id(m_impl->undo->id, &id)
          ? record.object_index->find(record.objects, id) : nullptr;
      if (!base || base->kind != Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR ||
          !m_impl->undo->base->matches(base->total_size, base->digest))
        retired = std::move(m_impl->undo);
    }
  } catch (const std::bad_alloc &) {
    // Optional predecode may be discarded; final still owns its sealed input.
    retired = std::move(m_impl->undo);
  }
}

bool Preserve_trx_receiver_candidates::step_undo(
    const std::string &id, std::shared_ptr<const Preserve_trx_sealed_file> file,
    size_t pages, bool *complete, uint64_t *scanned) {
  if (!complete || !scanned) return true;
  *complete = true;
  *scanned = 0;
  auto &s = *m_impl;
  std::shared_ptr<Impl::Undo> undo;
  {
    std::lock_guard<std::mutex> lock(s.mutex);
    undo = s.undo;
    if (s.cancelled.load() || !undo || undo->id != id || !same_file(undo->file, file) ||
        undo->failed || undo->working || !pages) return true;
    if (undo->ready || undo->final_claimed) return false;
    undo->working = true;
  }
  bool failed = true, done = false;
  uint64_t reads = 0;
  Preserve_trx_temp_stage_timer timer(Preserve_trx_temp_stage::RECEIVER_UNDO);
  const auto finish = create_scope_guard([&] {
    std::lock_guard<std::mutex> lock(s.mutex);
    undo->working = false;
    undo->failed = failed || s.cancelled.load() || s.undo != undo;
    if (!undo->failed && done) {
      undo->ready = true;
      ++undo_ready;
    }
    undo_read_bytes += reads;
    timer.read(reads);
    timer.write(*scanned - reads);
    *complete = undo->failed || undo->ready;
  });
  try {
    DBUG_EXECUTE_IF("preserve_temp_undo_early_abandon_after_batch", { pages = 1; });
    if (!undo->work) {
      if (Preserve_trx_temp_receiver_work::begin_undo(s.token, id, file,
            &undo->work, undo->base) != DB_SUCCESS) return true;
      *scanned = undo->work->scanned_bytes();
      reads = *scanned;
    }
    const auto written = undo->work->written_bytes();
    const auto err = undo->work->step_undo(pages, &done);
    reads += undo->work->scanned_bytes();
    *scanned += undo->work->scanned_bytes() + undo->work->written_bytes() - written;
    if (err != DB_SUCCESS) return true;
    failed = false;
    return false;
  } catch (const std::bad_alloc &) { return true; }
}

Preserve_trx_receiver_candidates::Take
Preserve_trx_receiver_candidates::take_undo(
    const Preserved_temp_table_undo_descriptor &d,
    std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
    Preserve_memory_lease *memory) {
  auto &s = *m_impl;
  std::unique_lock<std::mutex> lock(s.mutex);
  auto undo = s.undo;
  if (s.cancelled.load()) return Take::FAILED;
  if (!undo) return Take::ABSENT;
  if (d.delta.name.empty()) {
    if (undo->base || undo->id != d.blob_name ||
        !undo->file->matches(d.size, d.sha256)) return Take::ABSENT;
  } else if (!undo->base || undo->id != d.delta.name ||
      !undo->base->matches(d.base.size, d.base.digest) ||
      !undo->file->matches(d.delta.size, d.delta.digest)) return Take::ABSENT;
  if (undo->working) return Take::WAIT;
  if (undo->failed && undo->work) {
    // Reclaim a failed optional reader before reserving the final reader's
    // pages. Yield between batches; never wait or free all pages under mutex.
    undo->working = true;
    lock.unlock();
    bool complete = false;
    undo->work->cancel_step(&complete);
    lock.lock();
    undo->working = false;
    if (!complete) return Take::WAIT;
    undo->work.reset();
  }
  if (undo->failed || !undo->work || undo->final_claimed) {
    undo->final_claimed = true;
    return Take::ABSENT;
  }
  if (!undo->ready) return Take::WAIT;
  if (undo->work->take_undo(d, source, memory) != DB_SUCCESS) return Take::FAILED;
  undo->work.reset();
  undo->ready = false;
  undo->final_claimed = true;
  ++undo_reused;
  return Take::READY;
}

void Preserve_trx_receiver_candidates::abandon_undo(
    const std::string &id,
    const std::shared_ptr<const Preserve_trx_sealed_file> &file) {
  std::lock_guard<std::mutex> lock(m_impl->mutex);
  auto &undo = m_impl->undo;
  if (undo && undo->id == id && same_file(undo->file, file) && !undo->working &&
      !undo->ready && !undo->failed && !undo->final_claimed) {
    undo->failed = true;
    ++undo_abandoned;
  }
}

void Preserve_trx_receiver_candidates::cancel() {
  std::shared_ptr<Impl::Temp> temp;
  std::shared_ptr<Impl::Undo> retired;
  std::lock_guard<std::mutex> lock(m_impl->mutex);
  m_impl->cancelled.store(true);
  retired = std::move(m_impl->undo);
  temp = std::move(m_impl->temp);
}
int show_preserve_trx_temp_native_early_ready(THD *, SHOW_VAR *v, char *b) {
  return show(native_ready.load(), v, b);
}
int show_preserve_trx_temp_native_early_reused(THD *, SHOW_VAR *v, char *b) {
  return show(native_reused.load(), v, b);
}
int show_preserve_trx_temp_native_early_failed(THD *, SHOW_VAR *v, char *b) {
  return show(native_failed.load(), v, b);
}
int show_preserve_trx_result_early_ready(THD *, SHOW_VAR *v, char *b) {
  return show(early_ready.load(), v, b);
}
int show_preserve_trx_result_early_rows(THD *, SHOW_VAR *v, char *b) {
  return show(early_rows.load(), v, b);
}
int show_preserve_trx_result_early_reused(THD *, SHOW_VAR *v, char *b) {
  return show(early_reused.load(), v, b);
}

int show_preserve_trx_result_early_abandoned(THD *, SHOW_VAR *v, char *b) {
  return show(early_abandoned.load(), v, b);
}
int show_preserve_trx_temp_undo_early_ready(THD *, SHOW_VAR *v, char *b) {
  return show(undo_ready.load(), v, b);
}
int show_preserve_trx_temp_undo_early_reused(THD *, SHOW_VAR *v, char *b) {
  return show(undo_reused.load(), v, b);
}
int show_preserve_trx_temp_undo_early_read_bytes(THD *, SHOW_VAR *v, char *b) {
  return show(undo_read_bytes.load(), v, b);
}
int show_preserve_trx_temp_undo_early_abandoned(THD *, SHOW_VAR *v, char *b) {
  return show(undo_abandoned.load(), v, b);
}
int show_preserve_trx_temp_undo_early_superseded(THD *, SHOW_VAR *v, char *b) {
  return show(undo_superseded.load(), v, b);
}
