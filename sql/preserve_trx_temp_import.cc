/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_temp_import.h"

#include <algorithm>
#include <map>
#include <new>
#include <set>
#include <utility>
#include "my_dbug.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_file.h"
#include "sql/preserve_trx_receiver_candidates.h"
#include "sql/preserve_trx_temp_table.h"
#include "sql/preserve_trx_temp_transfer.h"
#include "sql/preserve_trx_temp_delta.h"
#include "storage/innobase/include/trx0temp_preserve_import.h"
#include "storage/innobase/include/trx0temp_preserve_input.h"

namespace {
bool undo_descriptor_has_manifest_rseg_identity(
    const Preserved_temp_table_undo_descriptor &undo) {
  return undo.no_redo_undo_rseg_space_id != 0 ||
         undo.no_redo_undo_rseg_page_no != 0 ||
         undo.no_redo_undo_rseg_slot != 0;
}
}  // namespace

bool preserve_trx_temp_table_apply_manifest_undo_identity_for_resume(
    const Preserved_temp_table_undo_descriptor &undo,
    trx_preserve_temp_space_image_descriptor *descriptor) {
  if (descriptor == nullptr) return false;
  if (!undo_descriptor_has_manifest_rseg_identity(undo)) return true;
  if (undo.no_redo_undo_rseg_space_id == 0 ||
      undo.no_redo_undo_rseg_page_no == 0) {
    return false;
  }
  if (descriptor->no_redo_undo_rseg_identity_present &&
      (descriptor->no_redo_undo_rseg_space_id !=
           undo.no_redo_undo_rseg_space_id ||
       descriptor->no_redo_undo_rseg_page_no !=
           undo.no_redo_undo_rseg_page_no ||
       descriptor->no_redo_undo_rseg_slot != undo.no_redo_undo_rseg_slot)) {
    return false;
  }
  descriptor->no_redo_undo_rseg_identity_present = true;
  descriptor->no_redo_undo_rseg_space_id = undo.no_redo_undo_rseg_space_id;
  descriptor->no_redo_undo_rseg_page_no = undo.no_redo_undo_rseg_page_no;
  descriptor->no_redo_undo_rseg_slot = undo.no_redo_undo_rseg_slot;
  return true;
}

struct Preserve_trx_temp_import_work::Impl {
  enum class Phase {
    GROUPS, SPACE_BEGIN, SPACE_BASE, SPACE_MERGE, SPACE_BIND, SPACE, DICTIONARY, UNDO_BEGIN, UNDO_MERGE, UNDO_READ, GRAPH,
    RETIRE_GROUPS, DONE, TAKEN, CANCELLED
  };
  struct Group {
    const Preserved_temp_table_manifest_entry *first{nullptr};
    std::vector<const trx_preserve_temp_dict_table_binding *> bindings;
    size_t space{0};
  };
  // The plan's pending graph borrows source and its lease. Its pending Space
  // borrows input bindings. This declaration order also protects fallback
  // destruction, but workers must drive cancel_step instead.
  Preserve_memory_lease memory;
  std::unique_ptr<Preserve_trx_temp_transfer_input> input;
  Preserve_memory_lease source_memory;
  std::unique_ptr<trx_preserve_temp_space_image_descriptor> source;
  std::unique_ptr<Preserve_trx_temp_delta_reader> delta;
  std::shared_ptr<const Preserve_trx_sealed_file> image_file;
  std::unique_ptr<trx_preserve_temp_undo_input> reader;
  std::unique_ptr<trx_preserve_temp_import_plan> plan;
  std::map<uint32_t, Group> groups;
  decltype(groups)::iterator next;
  size_t table{0};
  uint64_t scanned{0}, written{0};
#ifndef NDEBUG
  uint64_t batches{0};
#endif
  Phase phase{Phase::GROUPS};
  dberr_t error{DB_SUCCESS};
  bool cancelling{false};
};

Preserve_trx_temp_import_work::Preserve_trx_temp_import_work(std::unique_ptr<Impl> impl)
    : m_impl(std::move(impl)) {}

Preserve_trx_temp_import_work::~Preserve_trx_temp_import_work() {
  while (!cancel_step(128)) {}
}

dberr_t Preserve_trx_temp_import_work::begin(
    std::unique_ptr<Preserve_trx_temp_transfer_input> *input,
    std::unique_ptr<Preserve_trx_temp_import_work> *output) {
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (output == nullptr || *output != nullptr || input == nullptr || *input == nullptr ||
      (*input)->manifest() == nullptr) return DB_ERROR;
  const auto &manifest = *(*input)->manifest();
  if (((manifest.owner_trx_id == 0) != (*input)->resource_only()) ||
      ((*input)->resource_only() &&
       (!manifest.undo_images.empty() || !manifest.ownership_claims.empty())) ||
      (manifest.tables.empty() && !preserve_trx_temp_history_only(manifest)) || manifest.tables.size() > 1024 ||
      manifest.undo_images.size() > 1 ||
      (!manifest.undo_images.empty() && !manifest.native_adoption_capable)) return DB_CORRUPTION;
  try {
    uint64_t bytes = 4096 + sizeof(Impl) + sizeof(Preserve_trx_temp_import_work) +
        sizeof(trx_preserve_temp_import_plan) + manifest.tables.size() * 512 +
        manifest.retired_tables.size() * sizeof(uint64_t);
    DBUG_EXECUTE_IF("preserve_temp_import_work_budget_failure", { bytes = UINT64_MAX; });
    auto memory = preserve_trx_acquire_memory_lease(
        (*input)->token(), Preserve_trx_memory_kind::TEMP_METADATA_IMPORT,
        bytes);
    if (!memory.acquired()) return DB_OUT_OF_MEMORY;
    auto state = std::make_unique<Impl>();
    state->memory = std::move(memory);
    state->plan = std::make_unique<trx_preserve_temp_import_plan>();
    std::vector<uint64_t> retired;
    retired.reserve(manifest.retired_tables.size());
    for (const auto &table : manifest.retired_tables) retired.push_back(table.table_id);
    const auto history_error = state->plan->set_retired_table_ids((*input)->token(), retired);
    if (history_error != DB_SUCCESS) return history_error;
    if ((*input)->resource_only() && !state->plan->set_resource_only()) return DB_ERROR;
    if (manifest.tables.empty() && !state->plan->set_undo_only()) return DB_ERROR;
    auto work = std::unique_ptr<Preserve_trx_temp_import_work>(
        new Preserve_trx_temp_import_work(std::move(state)));
    DBUG_EXECUTE_IF("preserve_temp_import_work_oom", { throw std::bad_alloc(); });
    work->m_impl->input = std::move(*input);
    *output = std::move(work);
    return DB_SUCCESS;
  } catch (const std::bad_alloc &) { return DB_OUT_OF_MEMORY; }
}

dberr_t Preserve_trx_temp_import_work::step(
    size_t work_budget, size_t byte_budget, size_t page_budget,
    const trx_preserve_temp_import_plan *previous) {
  auto &s = *m_impl;
  s.scanned = 0;
  s.written = 0;
  if (s.cancelling || s.phase == Impl::Phase::TAKEN ||
      work_budget == 0 || byte_budget == 0 || page_budget == 0) return DB_ERROR;
  if (!preserve_trx_enable || !preserve_trx_temp_table_enable) return DB_UNSUPPORTED;
  if (s.error != DB_SUCCESS || complete()) return s.error;
#ifndef NDEBUG
  ++s.batches;
#endif
  const auto &manifest = *s.input->manifest();
  const auto &token = s.input->token();
  bool done = false;
  using Phase = Impl::Phase;
  try {
    switch (s.phase) {
      case Phase::GROUPS:
        while (work_budget-- != 0 && s.table < manifest.tables.size()) {
          const auto &table = manifest.tables[s.table];
          auto &group = s.groups[table.image.source_space_id];
          if (group.first == nullptr) group.first = &table;
          group.bindings.push_back(&table.dict_binding);
          ++s.table;
        }
        if (s.table == manifest.tables.size()) {
          s.next = s.groups.begin();
          s.phase = s.groups.empty() ? Phase::DICTIONARY : Phase::SPACE_BEGIN;
        }
        break;
      case Phase::SPACE_BEGIN: {
        const auto &image = s.next->second.first->image;
        if (preserve_trx_temp_image_sparse_id(image.base.name, nullptr)) {
          const auto base = s.input->files().find(image.base.name);
          if (base == s.input->files().end()) return s.error = DB_CORRUPTION;
          // Borrow only during this call. A matching immutable view retains
          // its own file and budget leases after the donor is retired.
          std::shared_ptr<const Preserve_trx_sealed_file> previous_file;
          const auto n = s.plan->space_count();
          const auto *old = previous ? previous->source_space(n) : nullptr;
          if (old && old->source_space_id == image.source_space_id)
            previous_file = previous->source_file(n);
          s.delta.reset(new Preserve_trx_temp_delta_reader);
          if (!s.delta->begin(token, image.base.name, nullptr, base->second,
                              nullptr, std::move(previous_file)) ||
              (image.delta.name.empty() && !s.delta->matches(image)))
            return s.error = DB_CORRUPTION;
          s.scanned = s.delta->read_bytes();
          s.phase = Phase::SPACE_BASE;
        } else if (!image.delta.name.empty()) {
          const auto base = s.input->files().find(image.base.name);
          const auto patch = s.input->files().find(image.delta.name);
          if (base == s.input->files().end() || patch == s.input->files().end())
            return s.error = DB_CORRUPTION;
          s.delta.reset(new Preserve_trx_temp_delta_reader);
          if (!s.delta->begin(token, image.delta.name, base->second, patch->second) ||
              !s.delta->matches(image)) return s.error = DB_CORRUPTION;
          s.scanned = s.delta->read_bytes();
          s.phase = Phase::SPACE_MERGE;
        } else {
          const auto file = s.input->files().find(image.blob_name);
          if (file == s.input->files().end() || !file->second)
            return s.error = DB_CORRUPTION;
          s.image_file = file->second;
          s.phase = Phase::SPACE_BIND;
        }
        break;
      }
      case Phase::SPACE_BASE:
      case Phase::SPACE_MERGE: {
        const auto before = s.delta->read_bytes();
        const auto writes = s.delta->written_bytes();
        const bool ok = s.delta->step(byte_budget, &done);
        s.scanned = s.delta->read_bytes() - before;
        s.written = s.delta->written_bytes() - writes;
        if (!ok) return s.error = DB_CORRUPTION;
        if (done) {
          s.image_file = s.delta->file();
          s.delta.reset();
          const auto &image = s.next->second.first->image;
          if (s.phase == Phase::SPACE_BASE && !image.delta.name.empty()) {
            const auto base = s.input->files().find(image.base.name);
            const auto patch = s.input->files().find(image.delta.name);
            if (base == s.input->files().end() || patch == s.input->files().end())
              return s.error = DB_CORRUPTION;
            s.delta.reset(new Preserve_trx_temp_delta_reader);
            if (!s.delta->begin(token, image.delta.name, s.image_file,
                                patch->second, base->second) ||
                !s.delta->matches(image)) return s.error = DB_CORRUPTION;
            s.scanned += s.delta->read_bytes();
            s.image_file.reset();
            s.phase = Phase::SPACE_MERGE;
            break;
          }
          s.phase = Phase::SPACE_BIND;
        }
        break;
      }
      case Phase::SPACE_BIND: {
        const auto &image = s.next->second.first->image;
        trx_preserve_temp_space_image_descriptor descriptor;
        descriptor.source_space_id = image.source_space_id;
        descriptor.page_size = image.page_size;
        descriptor.space_flags = image.space_flags;
        descriptor.image_bytes = image.size;
        std::copy(image.sha256.begin(), image.sha256.end(), descriptor.image_digest);
        descriptor.sealed = true;
        s.next->second.space = s.plan->space_count();
        s.error = s.plan->begin_source_space(token, descriptor, s.next->second.bindings,
                                             s.image_file);
        s.image_file.reset();
        if (s.error == DB_SUCCESS) s.phase = Phase::SPACE;
        break;
      }
      case Phase::SPACE: {
        const auto before = s.plan->source_read_bytes();
        s.error = s.plan->prepare_source_space_batch(work_budget, &done);
        s.scanned = s.plan->source_read_bytes() - before;
        if (s.error == DB_SUCCESS && done)
          s.phase = ++s.next == s.groups.end() ? Phase::DICTIONARY : Phase::SPACE_BEGIN;
        break;
      }
      case Phase::DICTIONARY:
        s.error = s.plan->prepare_source_dictionary_batch(token, work_budget, &done);
        if (s.error == DB_SUCCESS && done) s.phase = Phase::UNDO_BEGIN;
        break;
      case Phase::UNDO_BEGIN: {
        if (manifest.undo_images.empty()) {
          s.phase = Phase::RETIRE_GROUPS;
          break;
        }
        const auto &undo = manifest.undo_images.front();
        const auto group = s.groups.find(undo.source_space_id);
        if (preserve_trx_temp_undo_is_independent(undo)) {
          if (auto candidates = s.input->candidates()) {
            using Take = Preserve_trx_receiver_candidates::Take;
            const auto taken = candidates->take_undo(
                undo, &s.source, &s.source_memory);
            if (taken == Take::WAIT) break;
            if (taken == Take::FAILED) return s.error = DB_CORRUPTION;
            if (taken == Take::READY) {
              s.phase = Phase::GRAPH;
              break;
            }
          }
          s.source = std::make_unique<trx_preserve_temp_space_image_descriptor>();
          s.source->undo_only = true;
          s.source->source_space_id = undo.source_space_id;
          s.source->page_size = undo.page_size;
        } else {
          if (group == s.groups.end()) return s.error = DB_CORRUPTION;
          s.source = std::make_unique<trx_preserve_temp_space_image_descriptor>(
              *s.plan->source_space(group->second.space));
        }
        if (!preserve_trx_temp_table_apply_manifest_undo_identity_for_resume(undo, s.source.get()))
          return s.error = DB_CORRUPTION;
        if (!undo.delta.name.empty()) {
          const auto base = s.input->files().find(undo.base.name);
          const auto patch = s.input->files().find(undo.delta.name);
          if (base == s.input->files().end() || patch == s.input->files().end())
            return s.error = DB_CORRUPTION;
          s.delta.reset(new Preserve_trx_temp_delta_reader);
          if (!s.delta->begin(token, undo.delta.name, base->second, patch->second) ||
              !s.delta->matches(undo)) return s.error = DB_CORRUPTION;
          s.scanned = s.delta->read_bytes();
          s.phase = Phase::UNDO_MERGE;
          break;
        }
        const auto file = s.input->files().find(undo.blob_name);
        if (file == s.input->files().end() || !file->second)
          return s.error = DB_CORRUPTION;
        s.error = trx_preserve_temp_undo_input::begin(token, &s.source, file->second, &s.reader);
        if (s.error == DB_SUCCESS) {
          s.scanned = s.reader->read_bytes();
          s.phase = Phase::UNDO_READ;
        }
        break;
      }
      case Phase::UNDO_MERGE: {
        const auto before = s.delta->read_bytes();
        const auto writes = s.delta->written_bytes();
        const bool ok = s.delta->step(byte_budget, &done);
        s.scanned = s.delta->read_bytes() - before;
        s.written = s.delta->written_bytes() - writes;
        if (!ok) return s.error = DB_CORRUPTION;
        if (done) {
          s.error = trx_preserve_temp_undo_input::begin(
              token, &s.source, s.delta->file(), &s.reader);
          if (s.error == DB_SUCCESS) {
            s.scanned += s.reader->read_bytes();
            s.phase = Phase::UNDO_READ;
          }
        }
        break;
      }
      case Phase::UNDO_READ: {
        const auto before = s.reader->read_bytes();
        s.error = s.reader->step(page_budget, &done);
        s.scanned = s.reader->read_bytes() - before;
        if (s.error == DB_SUCCESS && done) {
          s.error = s.reader->take(&s.source, &s.source_memory);
          if (s.error == DB_SUCCESS) {
            s.reader.reset();
            s.delta.reset();
            s.phase = Phase::GRAPH;
          }
        }
        break;
      }
      case Phase::GRAPH:
        s.error = s.plan->prepare_source_undo_batch(token, &s.source,
            manifest.owner_trx_id, &s.source_memory, work_budget, byte_budget, &done);
        if (s.error == DB_SUCCESS && done) s.phase = Phase::RETIRE_GROUPS;
        break;
      case Phase::RETIRE_GROUPS:
        while (work_budget-- != 0 && !s.groups.empty()) s.groups.erase(s.groups.begin());
        if (s.groups.empty()) s.phase = Phase::DONE;
        break;
      default: return DB_ERROR;
    }
  } catch (const std::bad_alloc &) { s.error = DB_OUT_OF_MEMORY; }
  return s.error;
}

bool Preserve_trx_temp_import_work::complete() const {
  return !m_impl->cancelling && m_impl->phase == Impl::Phase::DONE;
}

const Preserve_trx_temp_transfer_input *Preserve_trx_temp_import_work::input() const {
  return m_impl->cancelling ? nullptr : m_impl->input.get();
}

uint64_t Preserve_trx_temp_import_work::scanned_bytes() const { return m_impl->scanned; }
uint64_t Preserve_trx_temp_import_work::written_bytes() const { return m_impl->written; }
#ifndef NDEBUG
uint64_t Preserve_trx_temp_import_work::batches() const { return m_impl->batches; }
#endif

dberr_t Preserve_trx_temp_import_work::take(
    std::unique_ptr<Preserve_trx_temp_transfer_input> *input,
    std::unique_ptr<trx_preserve_temp_import_plan> *plan) {
  if (!complete() || input == nullptr || *input != nullptr || plan == nullptr || *plan != nullptr)
    return DB_ERROR;
  *input = std::move(m_impl->input);
  *plan = std::move(m_impl->plan);
  m_impl->phase = Impl::Phase::TAKEN;
  return DB_SUCCESS;
}

bool Preserve_trx_temp_import_work::cancel_step(size_t work_budget) {
  auto &s = *m_impl;
  if (work_budget == 0) return false;
  s.cancelling = true;
  if (s.plan != nullptr) {
    if (!s.plan->discard_source_metadata_step(work_budget)) return false;
    s.plan.reset();
    return false;
  }
  if (s.reader != nullptr) {
    if (!s.reader->cancel_step(work_budget)) return false;
    s.reader.reset();
    return false;
  }
  if (s.delta) {
    s.delta.reset();
    return false;
  }
  if (s.image_file) {
    s.image_file.reset();
    return false;
  }
  if (s.source != nullptr) {
    while (work_budget != 0 && !s.source->no_redo_undo_pages.empty()) {
      s.source->no_redo_undo_pages.pop_back();
      --work_budget;
    }
    if (!s.source->no_redo_undo_pages.empty()) return false;
    s.source.reset();
    s.source_memory.release();
    return false;
  }
  if (!s.groups.empty()) {
    while (work_budget-- != 0 && !s.groups.empty()) s.groups.erase(s.groups.begin());
    return false;
  }
  if (s.input != nullptr) {
    if (!s.input->cancel_step(work_budget)) return false;
    s.input.reset();
  }
  s.phase = Impl::Phase::CANCELLED;
  return true;
}

#ifndef NDEBUG
dberr_t preserve_trx_temp_import_probe(
    std::unique_ptr<Preserve_trx_temp_transfer_input> *input,
    std::unique_ptr<trx_preserve_temp_import_plan> *plan) {
  if (input == nullptr || *input == nullptr || plan == nullptr || *plan != nullptr)
    return DB_ERROR;
  using Phase = Preserve_trx_temp_import_work::Impl::Phase;
  using Status = Preserve_trx_transfer_status;
  const auto *identity = input->get();
  const auto token = identity->token();
  const auto payload = identity->manifest_payload();
  const bool has_undo = !identity->manifest()->undo_images.empty();
  Preserve_trx_transfer_receiver_record record;
  record.protocol_version = kPreserveTrxTransferProtocolVersion;
  record.token = std::stoull(token);
  record.epoch_id = "temp-import-work-probe";
  record.state = Preserve_trx_transfer_receiver_state::SAVED_ONLINE;
  record.temp_id_contract = identity->contract();
  record.sealed_files = identity->files();
  if (preserve_trx_temp_transfer_descriptors(token, payload, &record.objects) != Status::OK)
    return DB_ERROR;
  for (const auto &object : record.objects) record.sealed_objects.insert(object.object_id);
  const auto baseline = preserve_trx_memory_current_bytes_status();
  const auto make = [&](std::unique_ptr<Preserve_trx_temp_import_work> *work) {
    std::unique_ptr<Preserve_trx_temp_transfer_input> copy;
    if (Preserve_trx_temp_transfer_input::load(token, payload, record, &copy) != Status::OK)
      return DB_ERROR;
    return Preserve_trx_temp_import_work::begin(&copy, work);
  };
  const auto retire = [&](std::unique_ptr<Preserve_trx_temp_import_work> *work) {
    size_t steps = 0;
    while (!(*work)->cancel_step(1)) {
      if (++steps > 1000000 || (*work)->step(1, 1, 1) != DB_ERROR ||
          (*work)->input() != nullptr || (*work)->complete()) return false;
    }
    if ((*work)->step(1, 1, 1) != DB_ERROR) return false;
    work->reset();
    return preserve_trx_memory_current_bytes_status() == baseline;
  };
  {
    std::unique_ptr<Preserve_trx_temp_transfer_input> copy;
    if (Preserve_trx_temp_transfer_input::load(token, payload, record, &copy) != Status::OK)
      return DB_ERROR;
    const auto retained = preserve_trx_memory_current_bytes_status();
    size_t steps = 0;
    bool done = false;
    while (!done) {
      done = copy->cancel_step(1);
      if (++steps > 1000000 || copy->manifest() != nullptr || copy->matches(payload, record) ||
          preserve_trx_memory_current_bytes_status() != retained) return DB_ERROR;
    }
    if (!copy->files().empty() || !copy->manifest_payload().empty()) return DB_ERROR;
    copy.reset();
    if (preserve_trx_memory_current_bytes_status() != baseline) return DB_ERROR;
    DBUG_PRINT("preserve_temp_import",
               ("temporary import input retired steps=%zu quota=1 revoked=1", steps));
  }
  size_t admission_failures = 0;
  for (const char *fault : {"+d,preserve_temp_import_work_budget_failure",
                            "+d,preserve_temp_import_work_oom"}) {
    std::unique_ptr<Preserve_trx_temp_transfer_input> copy;
    if (Preserve_trx_temp_transfer_input::load(token, payload, record, &copy) != Status::OK)
      return DB_ERROR;
    const auto *original = copy.get();
    const auto retained = preserve_trx_memory_current_bytes_status();
    std::unique_ptr<Preserve_trx_temp_import_work> work;
    DBUG_PUSH(fault);
    const auto err = Preserve_trx_temp_import_work::begin(&copy, &work);
    DBUG_POP();
    if (err != DB_OUT_OF_MEMORY || work != nullptr || copy.get() != original ||
        preserve_trx_memory_current_bytes_status() != retained ||
        Preserve_trx_temp_import_work::begin(&copy, &work) != DB_SUCCESS ||
        copy != nullptr || !retire(&work)) return DB_ERROR;
    ++admission_failures;
  }
  size_t cancelled = 0;
  for (Phase cutoff : {Phase::GROUPS, Phase::SPACE_BEGIN, Phase::SPACE,
                        Phase::DICTIONARY, Phase::UNDO_BEGIN, Phase::UNDO_READ,
                        Phase::GRAPH, Phase::RETIRE_GROUPS, Phase::DONE}) {
    if (!has_undo && (cutoff == Phase::UNDO_READ || cutoff == Phase::GRAPH)) continue;
    std::unique_ptr<Preserve_trx_temp_import_work> work;
    if (make(&work) != DB_SUCCESS) return DB_ERROR;
    size_t steps = 0;
    while (work->m_impl->phase != cutoff) {
      if (work->complete() || ++steps > 1000000 || work->step(1, 1, 1) != DB_SUCCESS)
        return DB_ERROR;
    }
    // Also exercise a pending dictionary/reader/graph, not only its entry.
    if (cutoff == Phase::DICTIONARY || cutoff == Phase::UNDO_READ || cutoff == Phase::GRAPH)
      if (work->step(1, 1, 1) != DB_SUCCESS) return DB_ERROR;
    if (!retire(&work)) return DB_ERROR;
    ++cancelled;
  }
  size_t failures = 0;
  for (const char *fault : {"+d,preserve_temp_metadata_budget_failure",
                            "+d,preserve_temp_undo_input_bad_digest",
                            "+d,preserve_temp_import_source_undo_oom"}) {
    if (!has_undo && failures != 0) break;
    std::unique_ptr<Preserve_trx_temp_import_work> work;
    if (make(&work) != DB_SUCCESS) return DB_ERROR;
    dberr_t err = DB_SUCCESS;
    size_t steps = 0;
    DBUG_PUSH(fault);
    while (err == DB_SUCCESS && !work->complete() && ++steps < 1000000)
      err = work->step(1, 1, 1);
    DBUG_POP();
    std::unique_ptr<Preserve_trx_temp_transfer_input> rejected_input;
    std::unique_ptr<trx_preserve_temp_import_plan> rejected_plan;
    if (err == DB_SUCCESS || work->complete() || work->step(1, 1, 1) != err ||
        work->take(&rejected_input, &rejected_plan) != DB_ERROR ||
        rejected_input != nullptr || rejected_plan != nullptr || !retire(&work)) return DB_ERROR;
    ++failures;
  }
  DBUG_PRINT("preserve_temp_import",
             ("temporary import work cancellation checked phases=%zu failures=%zu quota=1 admission=%zu",
              cancelled, failures, admission_failures));
  std::unique_ptr<Preserve_trx_temp_import_work> work;
  auto err = Preserve_trx_temp_import_work::begin(input, &work);
  if (err != DB_SUCCESS || *input != nullptr || work->input() != identity) return DB_ERROR;
  uint64_t read_bytes = 0;
  while (!work->complete()) {
    if (work->batches() > 1000000) return DB_ERROR;
    err = work->step(1, 1, 1);
    if (err != DB_SUCCESS) return err;
    read_bytes += work->scanned_bytes();
    auto moved = std::move(work);
    work = std::move(moved);
    if (!work->complete()) {
      if (work->take(input, plan) != DB_ERROR || *input != nullptr || *plan != nullptr)
        return DB_ERROR;
    }
  }
  uint64_t expected_reads = 0;
  std::set<uint32_t> spaces;
  for (const auto &table : work->input()->manifest()->tables) {
    if (spaces.insert(table.image.source_space_id).second) expected_reads += table.image.page_size;
    expected_reads += uint64_t{table.image.page_size} * table.dict_binding.indexes.size();
  }
  for (const auto &undo : work->input()->manifest()->undo_images) expected_reads += undo.size;
  auto occupied_plan = std::make_unique<trx_preserve_temp_import_plan>();
  if (read_bytes != expected_reads || work->take(input, &occupied_plan) != DB_ERROR ||
      !work->complete() || work->input() != identity) return DB_ERROR;
  const auto batches = work->batches();
  err = work->take(input, plan);
  if (err != DB_SUCCESS || input->get() != identity || work->complete() ||
      work->step(1, 1, 1) != DB_ERROR || (*plan)->target_ids_allocated() ||
      (*input)->manifest_payload() != payload) return DB_ERROR;
  DBUG_PRINT("preserve_temp_import",
             ("temporary import work prepared batches=%llu read_bytes=%llu single_read=1 owners=1",
              (unsigned long long)batches, (unsigned long long)read_bytes));
  return DB_SUCCESS;
}
#endif
