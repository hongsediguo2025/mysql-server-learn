/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_temp_prebuild.h"
#include "sql/preserve_trx_temp_metrics.h"
#include "sql/preserve_trx_temp_undo_prebuild.h"
#include "sql/preserve_trx_temp_pretransfer.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <map>
#include <mutex>
#include <new>
#include "scope_guard.h"
#include "mysqld_error.h"
#include "mysql/components/services/log_builtins.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_result_pretransfer.h"
#include "sql/preserve_trx_result_manifest.h"
#include "sql/preserve_trx_temp_transfer.h"
#include "sql/dd/types/table.h"
#include "sql/dd/impl/sdi.h"
#include "sql/sql_class.h"
#include "storage/innobase/include/trx0preserve.h"
#include "storage/innobase/include/trx0temp_preserve_capture.h"
#include "storage/innobase/include/trx0temp_preserve_undo_scan.h"
#include "storage/innobase/include/trx0temp_preserve_output.h"

namespace {
std::atomic<ulonglong> temp_prebuild_steps{0};
std::atomic<ulonglong> temp_prebuild_installed{0};
std::atomic<ulonglong> temp_prebuild_active{0};
std::atomic<ulonglong> temp_prebuild_stale{0};
std::atomic<ulonglong> temp_prebuild_baselines{0};
std::atomic<ulonglong> temp_prebuild_buffer_pages{0};
std::atomic<ulonglong> temp_prebuild_file_pages{0};
std::atomic<ulonglong> temp_prebuild_rounds{0};
std::atomic<ulonglong> temp_prebuild_round_pages{0};
std::atomic<ulonglong> temp_prebuild_undo_pages{0};
std::atomic<ulonglong> temp_prebuild_undo_reused_pages{0};
std::atomic<ulonglong> temp_prebuild_undo_scans{0};
std::atomic<ulonglong> temp_prebuild_undo_stale{0};
std::atomic<ulonglong> temp_prebuild_undo_write_steps{0};
std::atomic<ulonglong> temp_prebuild_undo_write_bytes{0};
std::atomic<ulonglong> temp_prebuild_undo_claim_pages{0};
std::atomic<ulonglong> temp_prebuild_undo_claim_reused{0};
std::atomic<ulonglong> temp_prebuild_final_reused{0};
std::atomic<ulonglong> temp_prebuild_final_fallback{0};

}  // namespace
ulonglong preserve_trx_temp_prebuild_steps_status() {
  return temp_prebuild_steps.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_installed_status() {
  return temp_prebuild_installed.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_active_status() {
  return temp_prebuild_active.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_stale_status() {
  return temp_prebuild_stale.load(std::memory_order_relaxed);
}

ulonglong preserve_trx_temp_prebuild_baselines_status() {
  return temp_prebuild_baselines.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_buffer_pages_status() {
  return temp_prebuild_buffer_pages.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_file_pages_status() {
  return temp_prebuild_file_pages.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_rounds_status() {
  return temp_prebuild_rounds.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_round_pages_status() {
  return temp_prebuild_round_pages.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_undo_pages_status() {
  return temp_prebuild_undo_pages.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_undo_reused_pages_status() {
  return temp_prebuild_undo_reused_pages.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_undo_scans_status() {
  return temp_prebuild_undo_scans.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_undo_stale_status() {
  return temp_prebuild_undo_stale.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_undo_write_steps_status() {
  return temp_prebuild_undo_write_steps.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_undo_write_bytes_status() {
  return temp_prebuild_undo_write_bytes.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_final_reused_status() {
  return temp_prebuild_final_reused.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_undo_claim_pages_status() {
  return temp_prebuild_undo_claim_pages.load(std::memory_order_relaxed);
}
ulonglong preserve_trx_temp_prebuild_undo_claim_reused_status() {
  return temp_prebuild_undo_claim_reused.load(std::memory_order_relaxed);
}
void preserve_trx_temp_prebuild_note_undo_claim_reused() {
  ++temp_prebuild_undo_claim_reused;
}
ulonglong preserve_trx_temp_prebuild_final_fallback_status() {
  return temp_prebuild_final_fallback.load(std::memory_order_relaxed);
}
void preserve_trx_temp_prebuild_note_final(bool reused) {
  if (reused) ++temp_prebuild_final_reused;
  else ++temp_prebuild_final_fallback;
}

struct Preserve_trx_temp_prebuild_job::Impl {
  enum class Stage { NEXT, OPEN, COPY, UNDO_SCAN, UNDO,
                     UNDO_WRITE, UNDO_CLOSE, UNDO_CLAIMS, UNDO_TRANSFER,
                     ROUND_START, ROUND, CHECKPOINT_EXTEND, IMAGE_COPY,
                     IMAGE_TRANSFER };
  Preserve_trx_temp_prebuild_identity identity;
  std::shared_ptr<Temp_table_warmcopy_participant> participant;
  // A descriptor's participant pointer and native backing outlive scans.
  std::vector<Preserve_trx_temp_capture_input> captures;
  std::unique_ptr<Preserve_trx_temp_manifest_capture> candidate;
  size_t manifest_table{0}, manifest_claim{0};
  const Temp_table_warmcopy_participant::Prebuilt_sidecar *manifest_undo{nullptr};
  std::string manifest_payload;
  Preserve_trx_transfer_object_descriptor manifest_object;
  Preserve_memory_lease memory;
  std::unique_ptr<trx_preserve_temp_capture_scan> scan;
  std::unique_ptr<trx_preserve_temp_capture_round> round;
  std::unique_ptr<trx_preserve_temp_undo_scan> undo_scan;
  std::unique_ptr<trx_preserve_temp_undo_output> undo_output;
  std::unique_ptr<Preserve_trx_temp_undo_claim_builder> undo_claims;
  std::unique_ptr<Preserved_temp_table_image_writer> undo_writer;
  std::shared_ptr<Preserve_trx_temp_pretransfer_file> undo_transfer;
  std::shared_ptr<Preserve_trx_temp_pretransfer_file> image_transfer;
  size_t next{0};
  Stage stage{Stage::NEXT};
  Progress progress{Progress::MORE};
  bool installed{false};
  std::string reason;

  Impl(const Preserve_trx_temp_prebuild_identity &source,
       std::shared_ptr<Temp_table_warmcopy_participant> p,
       std::vector<Preserve_trx_temp_capture_input> c,
       std::unique_ptr<Preserve_trx_temp_manifest_capture> m)
      : identity(source), participant(std::move(p)), captures(std::move(c)), candidate(std::move(m)) {}
  Progress finish(size_t bytes, Preserve_trx_transfer_source_epoch_session *session,
                  uint64_t token);
  ~Impl() {
    scan.reset();
    round.reset();
    undo_scan.reset();
    undo_output.reset();
    undo_claims.reset();
    undo_writer.reset();
    image_transfer.reset();  // End descriptor/writer borrowing before discard.
    Preserve_trx_temp_prebuild_job::discard(&captures);
  }
  Progress fail(const char *message) {
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message);
    progress = Progress::FAILED;
    try {
      reason = message;
    } catch (const std::bad_alloc &) {
    }
    return progress;
  }
  Progress stale() {
    ++temp_prebuild_stale;
    return progress = Progress::STALE;
  }
};

Preserve_trx_temp_prebuild_job::Preserve_trx_temp_prebuild_job(
    const Preserve_trx_temp_prebuild_identity &identity,
    std::shared_ptr<Temp_table_warmcopy_participant> participant,
    std::vector<Preserve_trx_temp_capture_input> captures,
    std::unique_ptr<Preserve_trx_temp_manifest_capture> manifest)
    : m_impl(new Impl(identity, std::move(participant), std::move(captures), std::move(manifest))) {
  ++temp_prebuild_active;
}
Preserve_trx_temp_prebuild_job::~Preserve_trx_temp_prebuild_job() {
  m_impl.reset();
  --temp_prebuild_active;
}

Preserve_trx_temp_manifest_capture::Preserve_trx_temp_manifest_capture() = default;
Preserve_trx_temp_manifest_capture::~Preserve_trx_temp_manifest_capture() = default;

Preserve_trx_temp_prebuild_job::Progress Preserve_trx_temp_prebuild_job::Impl::finish(
    size_t bytes, Preserve_trx_transfer_source_epoch_session *session, uint64_t token) {
  if (!candidate || !session || !token) return progress = Progress::DONE;
  auto &manifest = candidate->manifest;
  if (manifest_table < manifest.tables.size()) {
    auto &entry = manifest.tables[manifest_table];
    auto checkpoint = candidate->images[entry.image.source_space_id];
    for (const auto &capture : captures)
      if (capture.sidecar && capture.sidecar->source_space_id == entry.image.source_space_id)
        checkpoint = capture.sidecar->image_checkpoint;
    if (!checkpoint) return progress = Progress::DONE; // Optional incomplete round.
    const auto &file = checkpoint->logical();
    entry.image.blob_name = file.name;
    entry.image.size = file.size;
    entry.image.sha256 = file.digest;
    checkpoint->select(&entry.image);
    const auto sdi = dd::serialize(current_thd, *candidate->definitions[manifest_table],
                                   dd::String_type(entry.schema_name.c_str()));
    if (sdi.empty()) return progress = Progress::DONE;
    entry.serialized_dd_table.assign(sdi.data(), sdi.size());
    ++manifest_table;
    return Progress::MORE;
  }
  if (candidate->needs_undo) {
    if (!manifest_undo) {
      for (const auto &capture : captures)
        if (capture.sidecar && capture.sidecar->descriptor.undo_only &&
            capture.sidecar->has_undo && capture.sidecar->undo_claims_ready)
          manifest_undo = capture.sidecar.get();
      if (!manifest_undo || manifest_undo->undo_snapshot.trx_id != manifest.owner_trx_id)
        return progress = Progress::DONE;
      const auto charge = candidate->memory.bytes() + manifest_undo->undo_claims.size() * 512;
      if (!candidate->memory.grow_to(charge)) return progress = Progress::DONE;
      auto undo = manifest_undo->undo;
      undo.blob_name = std::to_string(token) + ".tempts." +
                       std::to_string(undo.source_space_id) + ".undo";
      manifest.undo_images.push_back(std::move(undo));
      manifest.ownership_claims.reserve(manifest_undo->undo_claims.size());
    }
    size_t count = 0;
    while (manifest_claim < manifest_undo->undo_claims.size() && count++ < 128) {
      auto claim = manifest_undo->undo_claims[manifest_claim++];
      claim.token = std::to_string(token);
      manifest.ownership_claims.push_back(std::move(claim));
    }
    if (manifest_claim != manifest_undo->undo_claims.size()) return Progress::MORE;
  }
  if (manifest_payload.empty()) {
    if (!preserve_trx_encode_temp_table_manifest(manifest, &manifest_payload))
      return progress = Progress::DONE;
    std::vector<Preserve_trx_transfer_object_descriptor> dependencies;
    if (preserve_trx_temp_transfer_descriptors(std::to_string(token), manifest_payload,
          &dependencies) != Preserve_trx_transfer_status::OK) return progress = Progress::DONE;
    for (const auto &object : dependencies)
      if (!session->object_presealed_for_token(token, object)) return progress = Progress::DONE;
    if (!candidate->memory.grow_to(candidate->memory.bytes() + manifest_payload.capacity()))
      return progress = Progress::DONE;
    manifest_object.kind = Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR;
    manifest_object.total_size = manifest_payload.size();
    manifest_object.digest = preserve_trx_digest(manifest_payload.data(), manifest_payload.size());
    manifest_object.object_id = preserve_trx_temp_candidate_name(token, manifest_object.digest);
  }
  using Status = Preserve_trx_transfer_status;
  if (session->declare_object(token, manifest_object) != Status::OK)
    return fail("temporary manifest declare");
  bool declared = false, sealed = false;
  uint64_t offset = 0;
  if (session->result_object_progress(token, manifest_object, &declared, &sealed, &offset) != Status::OK ||
      !declared) return fail("temporary manifest progress");
  if (sealed) return progress = Progress::DONE;
  if (offset == manifest_payload.size()) {
    if (session->seal_object(token, manifest_object.object_id) != Status::OK)
      return fail("temporary manifest seal");
    return progress = Progress::DONE;
  }
  const auto count = std::min<uint64_t>({bytes, session->chunk_bytes(), manifest_payload.size() - offset});
  if (!count || session->write_object_chunk(token, manifest_object.object_id, offset,
          manifest_payload.substr(offset, count)) != Status::OK) return fail("temporary manifest send");
  return Progress::MORE;
}

Preserve_trx_temp_capture_input::Preserve_trx_temp_capture_input() = default;
Preserve_trx_temp_capture_input::Preserve_trx_temp_capture_input(
    Preserve_trx_temp_capture_input &&) noexcept = default;

Preserve_trx_temp_capture_input::~Preserve_trx_temp_capture_input() {
  frozen_round.reset();
  const auto retire = [](std::unique_ptr<
      Temp_table_warmcopy_participant::Prebuilt_sidecar> &sidecar) {
    if (!sidecar || sidecar->warmcopy_id.empty()) return;
    if (sidecar->continuous)
      trx_preserve_temp_capture_discard_candidate(&sidecar->descriptor);
    // The independent immutable undo scan never registered a live stream.
    else if (sidecar->descriptor.dirty_page_stream_armed)
      trx_preserve_temp_space_image_reset_dirty_page_stream(&sidecar->descriptor);
    if (sidecar->image_writer &&
        sidecar->image_writer->abort() != Preserved_trx_carrier_status::OK)
      preserve_trx_resource_note_spill_failure();
    try {
      Local_file_preserved_temp_table_image_carrier carrier(sidecar->preserve_dir);
      if (carrier.remove_warm_sidecars(sidecar->warmcopy_id,
                                       sidecar->source_space_id) !=
          Preserved_trx_carrier_status::OK)
        preserve_trx_resource_note_spill_failure();
    } catch (const std::bad_alloc &) {
      preserve_trx_resource_note_spill_failure();
    }
    sidecar.reset();
  };
  retire(sidecar);
  retire(retired_sidecar);
}

void Preserve_trx_temp_prebuild_job::discard(
    std::vector<Preserve_trx_temp_capture_input> *captures) {
  captures->clear();
}

Preserve_trx_temp_prebuild_job::Progress Preserve_trx_temp_prebuild_job::step(
    size_t byte_budget, Preserve_trx_transfer_source_epoch_session *session,
    uint64_t transfer_token) {
  auto &s = *m_impl;
  if (s.progress != Progress::MORE) return s.progress;
  if (!byte_budget) return s.fail("temporary prebuild has zero step budget");
  if (s.next == s.captures.size()) {
    try { return s.finish(byte_budget, session, transfer_token); }
    catch (const std::bad_alloc &) { return s.stale(); }
  }
  auto &capture = s.captures[s.next];
  if (!capture.sidecar) {
    Preserve_trx_temp_capture_input retired(std::move(capture));
    ++s.next;
    return s.progress;
  }
  if (capture.reuse_only) { ++s.next; return Progress::MORE; }
  auto &sidecar = *capture.sidecar;
  auto &descriptor = sidecar.descriptor;
  ++temp_prebuild_steps;
  if ((!descriptor.undo_only &&
       s.participant->data_generation() != sidecar.data_generation) ||
      trx_preserve_temp_capture_resource_exhausted(&descriptor)) {
    ++temp_prebuild_stale;
    return s.progress = Progress::STALE;
  }
  const auto pages = std::max<size_t>(1, byte_budget / descriptor.page_size);
  bool done = false;
  using Status = Preserved_trx_carrier_status;
  struct Sink {
    Temp_table_warmcopy_participant::Prebuilt_sidecar *sidecar;
    uint64_t written{0};
  } output{&sidecar};
  const auto sink = [](void *arg, uint32_t page_no, const unsigned char *page,
                       size_t bytes) {
    auto &out = *static_cast<Sink *>(arg);
    if (bytes != out.sidecar->descriptor.page_size ||
        out.sidecar->image_writer->write_at(uint64_t(page_no) * bytes,
                                           page, bytes) != Status::OK)
      return DB_IO_ERROR;
    out.written += bytes;
    return DB_SUCCESS;
  };
  try {
    if (!s.memory.acquired()) {
      uint64_t charge = sizeof(Impl) + byte_budget;
      for (auto &c : s.captures) {
        if (!c.sidecar) continue;
        const auto &d = c.sidecar->descriptor;
        const uint64_t bytes = sizeof(c) + sizeof(*c.sidecar) +
                               c.undo_payload.capacity() +
                               c.metadata.source_path.capacity();
        const uint64_t resident = (d.no_redo_undo_pages.size() +
                                   d.no_redo_undo_pending_pages.size()) *
                                  (uint64_t(d.page_size) + 256);
        if (resident != 0 && !c.sidecar->resident_memory.acquired()) {
          c.sidecar->resident_memory = preserve_trx_acquire_memory_lease(
              c.sidecar->warmcopy_id,
              Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER, resident);
          if (!c.sidecar->resident_memory.acquired())
            return s.progress = Progress::STALE;
        }
        if (bytes > UINT64_MAX - charge)
          return s.fail("temporary prebuild size overflow");
        charge += bytes;
      }
      s.memory = preserve_trx_acquire_memory_lease(
          sidecar.warmcopy_id,
          Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER, charge);
      if (!s.memory.acquired())
        return s.progress = Progress::STALE;
    }
    switch (s.stage) {
      case Impl::Stage::NEXT:
        if (descriptor.undo_only) {
          s.stage = Impl::Stage::UNDO_SCAN;
          break;
        }
        if (sidecar.image_writer) {
          s.stage = Impl::Stage::ROUND_START;
          break;
        }
        s.stage = Impl::Stage::OPEN;
        break;
      case Impl::Stage::OPEN: {
        Local_file_preserved_temp_table_image_carrier carrier(
            sidecar.preserve_dir);
        if (carrier.create_warm_image_writer(
                sidecar.warmcopy_id, sidecar.source_space_id,
                &sidecar.image_writer) != Status::OK)
          return s.fail("temporary prebuild writer open failed");
        s.scan.reset(new trx_preserve_temp_capture_scan);
        const auto error = s.scan->start(
            trx_preserve_temp_capture_scan::Mode::LIVE_BASELINE, &descriptor,
            capture.metadata.source_path.c_str());
        if (error == DB_OUT_OF_MEMORY) return s.stale();
        if (error != DB_SUCCESS)
          return s.fail("temporary prebuild copy open failed");
        ++temp_prebuild_baselines;
        s.stage = Impl::Stage::COPY;
        break;
      }
      case Impl::Stage::COPY: {
        Preserve_trx_temp_stage_timer timer(Preserve_trx_temp_stage::SOURCE_COPY);
        const auto buffer_before = s.scan->buffer_pages_read();
        const auto file_before = s.scan->file_pages_read();
        const auto error = s.scan->step(pages, &output, sink, &done);
        timer.write(output.written);
        temp_prebuild_buffer_pages += s.scan->buffer_pages_read() - buffer_before;
        temp_prebuild_file_pages += s.scan->file_pages_read() - file_before;
        timer.read((s.scan->buffer_pages_read() - buffer_before +
                    s.scan->file_pages_read() - file_before) * descriptor.page_size);
        if (error == DB_INTERRUPTED || error == DB_OUT_OF_MEMORY ||
            (error == DB_ERROR && trx_preserve_temp_capture_resource_exhausted(&descriptor))) {
          ++temp_prebuild_stale;
          return s.progress = Progress::STALE;
        }
        if (error != DB_SUCCESS) {
          DBUG_PRINT("preserve_temp_import", ("temporary DATA copy failed error=%u space=%u",
              static_cast<unsigned>(error), descriptor.source_space_id));
          return s.fail("temporary prebuild page step failed");
        }
        if (done) {
          s.scan.reset();
          if (trx_preserve_temp_space_image_mark_dirty_queue_durable(
                  &descriptor) != DB_SUCCESS) {
            if (trx_preserve_temp_capture_resource_exhausted(&descriptor))
              return s.stale();
            return s.fail("temporary prebuild dirty queue failed");
          }
          s.stage = Impl::Stage::UNDO_SCAN;
        }
        break;
      }
      case Impl::Stage::UNDO_SCAN: {
        Preserve_trx_temp_stage_timer timer(Preserve_trx_temp_stage::SOURCE_UNDO_SCAN);
        if (!sidecar.undo_scan_pending) {
          s.stage = Impl::Stage::UNDO;
          break;
        }
        if (!s.undo_scan) {
          auto *previous = s.captures[s.next].retired_sidecar.get();
          s.undo_scan.reset(new (std::nothrow) trx_preserve_temp_undo_scan);
          if (!s.undo_scan ||
              s.undo_scan->start(sidecar.undo_snapshot,
                  previous ? &previous->descriptor : nullptr,
                  previous ? &previous->undo_page_cache : nullptr,
                  capture.undo_batch.get()) != DB_SUCCESS) {
            s.undo_scan.reset();
            sidecar.undo_scan_pending = false;
            sidecar.resident_memory.release();
            s.stage = Impl::Stage::UNDO;
            break;
          }
        }
        const auto before = s.undo_scan->pages_read();
        const auto reused_before = s.undo_scan->pages_reused();
        const auto result = s.undo_scan->step(pages);
        temp_prebuild_undo_pages += s.undo_scan->pages_read() - before;
        temp_prebuild_undo_reused_pages += s.undo_scan->pages_reused() - reused_before;
        timer.read((s.undo_scan->pages_read() - before) * descriptor.page_size);
        using Result = trx_preserve_temp_undo_scan::Result;
        if (result == Result::MORE) break;
        if (result == Result::DONE &&
            s.undo_scan->take(&descriptor, &sidecar.undo_page_cache) == DB_SUCCESS) {
          sidecar.has_undo = true;
          ++temp_prebuild_undo_scans;
        } else {
          // The source may append, roll back, commit or reuse a page while
          // this optional private candidate is read. Keep the DATA baseline;
          // final capture will rebuild undo at the frozen command boundary.
          ++temp_prebuild_undo_stale;
        }
        s.undo_scan.reset();
        capture.undo_batch.reset();
        sidecar.undo_scan_pending = false;
        const auto resident = descriptor.no_redo_undo_pages.capacity() *
                                (uint64_t(descriptor.page_size) + 512);
        (void)sidecar.resident_memory.shrink_to(resident);
        s.stage = Impl::Stage::UNDO;
        break;
      }
      case Impl::Stage::UNDO: {
        Preserve_trx_temp_stage_timer timer(Preserve_trx_temp_stage::SOURCE_UNDO_WRITE);
        if (!sidecar.has_undo) {
          s.stage = Impl::Stage::ROUND_START;
          break;
        }
        Local_file_preserved_temp_table_image_carrier carrier(sidecar.preserve_dir);
        s.undo_output.reset(new trx_preserve_temp_undo_output);
        const auto error = s.undo_output->start(descriptor);
        if (error == DB_OUT_OF_MEMORY) return s.stale();
        if (error != DB_SUCCESS)
          return s.fail("temporary prebuild undo encoder open failed");
        const auto opened = carrier.create_warm_undo_writer(
            sidecar.warmcopy_id, sidecar.source_space_id,
            s.undo_output->size(), &s.undo_writer);
        if (opened != Status::OK)
          return s.fail("temporary prebuild undo writer open failed");
        s.stage = Impl::Stage::UNDO_WRITE;
        break;
      }
      case Impl::Stage::UNDO_WRITE: {
        Preserve_trx_temp_stage_timer timer(Preserve_trx_temp_stage::SOURCE_UNDO_WRITE);
        const auto before = s.undo_output->bytes_written();
        const auto error = s.undo_output->step(byte_budget, s.undo_writer.get(),
            [](void *context, uint64_t offset, const unsigned char *bytes, size_t size) {
              auto *writer = static_cast<Preserved_temp_table_image_writer *>(context);
              return writer->write_at(offset, bytes, size) == Status::OK
                         ? DB_SUCCESS : DB_IO_ERROR;
            }, &done);
        ++temp_prebuild_undo_write_steps;
        temp_prebuild_undo_write_bytes += s.undo_output->bytes_written() - before;
        timer.write(s.undo_output->bytes_written() - before);
        if (error != DB_SUCCESS) return s.fail("temporary prebuild undo output failed");
        if (done) s.stage = Impl::Stage::UNDO_CLOSE;
        break;
      }
      case Impl::Stage::UNDO_CLOSE: {
        Preserve_trx_temp_stage_timer timer(Preserve_trx_temp_stage::SOURCE_UNDO_WRITE);
        Preserved_temp_table_image_writer_result result;
        // Sequential writer has already hashed each emitted byte. Only native
        // fsync/close and fixed file-identity checks remain here.
        if (s.undo_writer->close() != Status::OK ||
            s.undo_writer->result(&result) != Status::OK ||
            result.size != s.undo_output->bytes_written())
          return s.fail("temporary prebuild undo close failed");
        sidecar.undo = preserve_trx_temp_undo_descriptor(
            sidecar.warmcopy_id, descriptor, result);
        s.undo_output.reset();
        sidecar.undo_writer = std::move(s.undo_writer);
        DBUG_EXECUTE_IF("preserve_temp_phase1_after_undo_write", {
          DBUG_PRINT("preserve_temp_import",
                     ("temporary phase1 failed after warm undo write"));
          return s.fail("debug failure after phase1 warm undo write");
        });
        s.undo_claims.reset(new Preserve_trx_temp_undo_claim_builder);
        if (!s.undo_claims->begin(sidecar.warmcopy_id, sidecar.undo, descriptor))
          return s.fail("temporary prebuild ownership proof open failed");
        s.stage = Impl::Stage::UNDO_CLAIMS;
        break;
      }
      case Impl::Stage::UNDO_CLAIMS: {
        const auto before = s.undo_claims->pages_hashed();
        if (!s.undo_claims->step(pages, &done))
          return s.fail("temporary prebuild ownership proof failed");
        temp_prebuild_undo_claim_pages += s.undo_claims->pages_hashed() - before;
        if (done) {
          if (!s.undo_claims->take(&sidecar.undo_claims, &sidecar.undo_claim_memory))
            return s.fail("temporary prebuild ownership proof take failed");
          sidecar.undo_claims_ready = true;
          s.undo_claims.reset();
          s.stage = Impl::Stage::UNDO_TRANSFER;
        }
        break;
      }
      case Impl::Stage::UNDO_TRANSFER: {
        if (!session || !transfer_token) {
          s.stage = Impl::Stage::ROUND_START;
          break;
        }
        if (!s.undo_transfer) {
          const auto pinned = Preserve_trx_temp_pretransfer_file::pin(
              sidecar.undo_writer.get(), sidecar.undo, sidecar.warmcopy_id,
              sidecar.preserve_dir, transfer_token, &s.undo_transfer);
          if (pinned == Preserve_trx_temp_pretransfer_file::Pin_result::STALE) {
            ++temp_prebuild_stale;
            return s.progress = Progress::STALE;
          }
          if (pinned == Preserve_trx_temp_pretransfer_file::Pin_result::SKIPPED) {
            s.stage = Impl::Stage::ROUND_START;
            break;
          }
        }
        bool prepared = false;
        if (!s.undo_transfer->prepare(sidecar.undo_base, transfer_token,
              sidecar.source_space_id, byte_budget, &prepared))
          return s.fail("temporary prebuild undo delta failed");
        if (!prepared) break;
        bool sent = false;
        if (s.undo_transfer->step(session, transfer_token, byte_budget,
                                  &sent) != Preserve_trx_transfer_status::OK)
          return s.fail("temporary prebuild undo transfer failed");
        if (sent) {
          s.undo_transfer->select(&sidecar.undo);
          if (!s.undo_transfer->is_delta()) sidecar.undo_base = s.undo_transfer;
          s.undo_transfer.reset();
          s.stage = Impl::Stage::ROUND_START;
        }
        break;
      }
      case Impl::Stage::ROUND_START:
        if (descriptor.undo_only) {
          ++s.next;
          s.stage = Impl::Stage::NEXT;

          break;
        }
        if (capture.frozen_round) {
          s.round = std::move(capture.frozen_round);
        } else {
          // First baseline has no checkpoint yet. Its next idle capture will
          // freeze DATA and undo together; this round only catches up the copy.
          s.round.reset(new trx_preserve_temp_capture_round);
          const auto error = s.round->start(&descriptor);
          if (error == DB_OUT_OF_MEMORY ||
              (error == DB_ERROR &&
               trx_preserve_temp_capture_resource_exhausted(&descriptor)))
            return s.stale();
          if (error != DB_SUCCESS)
            return s.fail("temporary prebuild round open failed");
        }
        s.stage = Impl::Stage::ROUND;
        break;
      case Impl::Stage::ROUND: {
        Preserve_trx_temp_stage_timer timer(Preserve_trx_temp_stage::SOURCE_ROUND);
        const auto before = s.round->pages_written();
        const auto error = s.round->step(pages, &output, sink, &done);
        temp_prebuild_round_pages += s.round->pages_written() - before;
        timer.write(output.written);
        if (error == DB_OUT_OF_MEMORY ||
            (error == DB_ERROR &&
             trx_preserve_temp_capture_resource_exhausted(&descriptor)))
          return s.stale();
        if (error != DB_SUCCESS)
          return s.fail("temporary prebuild round step failed");
        if (done) {
          ++temp_prebuild_rounds;
          s.round.reset();
          if (capture.checkpoint_image_bytes) {
            s.stage = Impl::Stage::CHECKPOINT_EXTEND;
            break;
          }
          ++s.next;
          s.stage = Impl::Stage::NEXT;

        }
        break;
      }
      case Impl::Stage::CHECKPOINT_EXTEND: {
        Preserve_trx_temp_stage_timer timer(Preserve_trx_temp_stage::SOURCE_ROUND);
        if (sidecar.image_base)
          sidecar.image_base_clean_prefix_bytes = std::min(
              sidecar.image_base_clean_prefix_bytes, capture.checkpoint_image_bytes);
        uint64_t written = 0;
        const auto error = sidecar.image_writer->resize_step(
            capture.checkpoint_image_bytes, byte_budget, &done, &written);
        timer.write(written);
        if (error != Status::OK) return s.fail("temporary checkpoint length failed");
        if (done) {
          if (session && transfer_token) {
            s.stage = Impl::Stage::IMAGE_COPY;
            break;
          }
          ++s.next;
          s.stage = Impl::Stage::NEXT;

        }
        break;
      }
      case Impl::Stage::IMAGE_COPY: {
        if (!s.image_transfer && !Preserve_trx_temp_pretransfer_file::begin_image(
              transfer_token, sidecar.source_space_id, capture.checkpoint_image_bytes,
              sidecar.image_writer.get(), sidecar.image_base, &s.image_transfer,
              &descriptor, sidecar.image_base_capture_floor,
              sidecar.image_base_clean_prefix_bytes)) {
          s.candidate.reset();
          sidecar.checkpoint_attempted = true;
          ++s.next;
          s.stage = Impl::Stage::NEXT;
          break;
        }
        using Copy = Preserve_trx_temp_pretransfer_file::Copy_result;
        const auto copied = s.image_transfer->copy_image(
            sidecar.image_writer.get(), byte_budget);
        if (copied == Copy::SKIPPED) {
          s.image_transfer.reset();
          s.candidate.reset();
          sidecar.checkpoint_attempted = true;
          ++s.next;
          s.stage = Impl::Stage::NEXT;
          break;
        }
        if (copied == Copy::ERROR)
          return s.fail("temporary image checkpoint copy");
        if (copied == Copy::READY) s.stage = Impl::Stage::IMAGE_TRANSFER;
        break;
      }
      case Impl::Stage::IMAGE_TRANSFER:
        if (!s.image_transfer->prepare(sidecar.image_base, transfer_token,
              sidecar.source_space_id, byte_budget, &done))
          return s.fail("temporary image delta encode");
        if (!done) break;
        if (s.image_transfer->step(session, transfer_token, byte_budget, &done) !=
            Preserve_trx_transfer_status::OK) return s.fail("temporary image transfer");
        if (done) {
          if (!s.image_transfer->is_delta()) {
            sidecar.image_base = s.image_transfer;
            sidecar.image_base_capture_floor = descriptor.dirty_page_capture_floor;
            sidecar.image_base_clean_prefix_bytes = capture.checkpoint_image_bytes;
          }
          sidecar.image_checkpoint = std::move(s.image_transfer);
          sidecar.checkpoint_attempted = true;
          ++s.next;
          s.stage = Impl::Stage::NEXT;

        }
        break;
    }
    DBUG_PRINT("preserve_temp_import",
               ("temporary phase1 worker step stage=%u space=%u complete=%u",
                static_cast<unsigned>(s.stage), sidecar.source_space_id,
                s.progress == Progress::DONE));
    return s.progress;
  } catch (const std::bad_alloc &) {
    return s.stale();
  }
}

Preserve_trx_temp_prebuild_job::Install Preserve_trx_temp_prebuild_job::install(THD *target) {
  try {
    if (!target) return Install::STALE;
    auto &s = *m_impl;
    if (s.installed) return Install::INSTALLED;
    mysql_mutex_lock(&target->LOCK_thd_data);
    const auto unlock =
        create_scope_guard([&] { mysql_mutex_unlock(&target->LOCK_thd_data); });
    if (target->release_resources_done() ||
        reinterpret_cast<uintptr_t>(target) != s.identity.owner_cookie ||
        target->preserve_trx_temp_table_participant != s.participant)
      return Install::STALE;
    if (!target->m_server_idle) return Install::BUSY;
    if (s.identity.resource_only) {
      if (!trx_preserve_idle_thd_has_no_engine(target)) return Install::STALE;
    } else {
      trx_preserve_phase1_identity current;
      if (!trx_preserve_phase1_owner_identity_snapshot(target, &current) ||
          current.raw_cookie != s.identity.trx_cookie ||
          current.trx_version != s.identity.trx_version)
        return Install::STALE;
    }
    for (const auto &capture : s.captures)
      if (capture.sidecar && !capture.sidecar->descriptor.undo_only &&
          capture.sidecar->data_generation !=
                                 s.participant->data_generation())
        return Install::STALE;
    if (s.progress == Progress::FAILED) {
      s.participant->mark_degraded(s.reason);
      return Install::FAILED;
    }
    if (s.progress != Progress::DONE) return Install::STALE;
    for (const auto &capture : s.captures) {
      if (capture.sidecar && s.participant->find_prebuilt_sidecar(
                                 capture.sidecar->source_space_id))
        return Install::FAILED;
    }
    if (!s.participant->reserve_prebuilt_sidecars(s.captures.size()))
      return Install::STALE;
    for (auto &capture : s.captures) {
      if (!capture.sidecar) continue;
      if (!s.participant->remember_prebuilt_sidecar(
              std::move(capture.sidecar))) {
        s.participant->mark_degraded(
            "temporary prebuild duplicate installation");
        return Install::FAILED;
      }
    }
    s.installed = true;
    ++temp_prebuild_installed;
    return Install::INSTALLED;
  } catch (const std::bad_alloc &) {
    return Install::FAILED;
  }
}

const std::string &Preserve_trx_temp_prebuild_job::reason() const {
  return m_impl->reason;
}

bool Preserve_trx_temp_prebuild_job::initial_baseline_complete() const {
  if (m_impl->progress != Progress::DONE) return false;
  for (const auto &capture : m_impl->captures)
    if (capture.sidecar && !capture.sidecar->descriptor.undo_only &&
        capture.sidecar->continuous && !capture.sidecar->checkpoint_attempted)
      return false;
  return true;
}

struct Preserve_trx_temp_prebuild_owner::Impl {
  struct Entry {
    std::shared_ptr<Preserve_trx_temp_prebuild_job> job;
    uint64_t owner_cookie{0}, generation{0};
    bool inflight{false};
    bool result_pending{false}, prefer_result{true}, result_failed{false};
    bool initial_temp_done{false}, initial_result_done{false};
  };
  mutable std::mutex mutex;
  std::map<uint64_t, Entry> entries;
  Preserve_trx_phase1_pipeline_config config;
  Preserve_trx_phase1_pipeline *pipeline{nullptr};
  Preserve_trx_transfer_source_epoch_session *session{nullptr};
  std::shared_ptr<Preserve_trx_result_pretransfer> results;
  // Only the drain owner changes these predicates. Workers mutate job contents,
  // prefer_result and result_failed, none of which changes the statistics.
  static bool active(const Entry &e) { return e.job || e.result_pending; }
  static bool waiting(const Entry &e) {
    return (!e.initial_temp_done || !e.initial_result_done) &&
           !e.job && !e.inflight && !e.result_pending;
  }
  void rebuild_statistics() {
    active_count = waiting_count = 0;
    for (const auto &item : entries) {
      active_count += active(item.second);
      waiting_count += waiting(item.second);
    }
    statistics_valid = true;
  }
  size_t active_count{0}, waiting_count{0};
  bool statistics_valid{false};
  uint64_t next_submit_token{0};
  uint64_t next_generation{0};
  uint64_t step_bytes{0};
  bool finishing{false};
};

Preserve_trx_temp_prebuild_owner::Preserve_trx_temp_prebuild_owner()
    : m_impl(new Impl) {}
Preserve_trx_temp_prebuild_owner::~Preserve_trx_temp_prebuild_owner() = default;

void Preserve_trx_temp_prebuild_owner::attach(
    const Preserve_trx_phase1_pipeline_config &config,
    Preserve_trx_phase1_pipeline *pipeline,
    Preserve_trx_transfer_source_epoch_session *session) {
  m_impl->config = config;
  m_impl->pipeline = pipeline;
  m_impl->session = session;
  if (session) {
    m_impl->results = session->result_source();
  }
  // InnoDB pages can be up to 64 KiB; a step always admits at least one page.
  m_impl->step_bytes = std::max<uint64_t>(config.copy_chunk_bytes, 64 * 1024);
  // Do not compete forever when the configured shared pool cannot admit even
  // one TEMP step. Keep the existing final-capture fallback for this policy.
  m_impl->finishing = config.record_reserve_bytes > config.credit_bytes ||
                      config.binlog_reserve_bytes >
                          config.credit_bytes - config.record_reserve_bytes ||
                      m_impl->step_bytes > config.credit_bytes -
                                               config.record_reserve_bytes -
                                               config.binlog_reserve_bytes;
}

std::map<uint64_t, uint64_t>
Preserve_trx_temp_prebuild_owner::deferred_capture_targets() const {
  std::map<uint64_t, uint64_t> deferred;
  std::lock_guard<std::mutex> lock(m_impl->mutex);
  bool pending = false;
  for (const auto &entry : m_impl->entries) {
    const auto &state = entry.second;
    if ((!state.initial_temp_done || !state.initial_result_done) &&
        !state.job && !state.inflight && !state.result_pending) {
      pending = true;
      break;
    }
  }
  if (pending)
    for (const auto &entry : m_impl->entries)
      if (entry.second.initial_temp_done && entry.second.initial_result_done)
        deferred.emplace(entry.first, entry.second.owner_cookie);
  return deferred;
}

bool Preserve_trx_temp_prebuild_owner::capture(THD *target,
                                               const std::string &directory) {
  try {
    auto &s = *m_impl;
    if (!target || !s.pipeline || s.finishing) return true;
    const auto id = target->thread_id();
    const auto cookie = reinterpret_cast<uintptr_t>(target);
    bool accounted = false, prior_active = false, prior_waiting = false;
    const auto update_statistics = create_scope_guard([&] {
      if (!accounted) return;
      std::lock_guard<std::mutex> lock(s.mutex);
      if (!s.statistics_valid) return;
      const auto &entry = s.entries.at(id);
      s.active_count = s.active_count - prior_active + Impl::active(entry);
      s.waiting_count = s.waiting_count - prior_waiting + Impl::waiting(entry);
    });
    // Request the owner boundary independently of scarce worker admission.
    if (s.results) s.results->request_capture(target);
    {
      std::lock_guard<std::mutex> lock(s.mutex);
      // Register the whole observed cohort before applying worker capacity.
      // Otherwise the first few hot owners can hide or starve later owners.
      if (!s.statistics_valid) s.rebuild_statistics();
      const auto found = s.entries.find(id);
      const auto inserted = found == s.entries.end()
          ? s.entries.emplace(id, Impl::Entry{}) : std::make_pair(found, false);
      auto &current = inserted.first->second;
      if (!inserted.second) {
        prior_active = Impl::active(current);
        prior_waiting = Impl::waiting(current);
      }
      accounted = true;
      if (!current.owner_cookie) current.owner_cookie = cookie;
      const size_t others = s.active_count - prior_active;
      const bool initial_pending = s.waiting_count > size_t(prior_waiting);
      if (initial_pending && current.initial_temp_done &&
          current.initial_result_done) return true;
      if (others >= std::min(s.config.worker_count, s.config.result_slots))
        return true;
      // A result batch accompanies a capture round, not each worker step.
      // Keep sending the pinned batch while DATA/undo makes bounded progress.
      if (current.owner_cookie == cookie &&
          (current.job || current.inflight)) return true;
    }
    if (s.results) {
      using Result_state = Preserve_trx_result_pretransfer::State;
      const auto state = s.results->capture(target);
      if (state == Result_state::FAILED) return false;
      std::lock_guard<std::mutex> lock(s.mutex);
      auto &entry = s.entries[id];
      // An incomplete sample needs another source safe point. It must not
      // consume a worker slot, credit or another owner's capture opportunity.
      entry.result_pending = state == Result_state::RUNNABLE;
      entry.initial_result_done |= state == Result_state::COMPLETE;
      if (entry.result_pending) {
        if (!entry.generation) entry.generation = ++s.next_generation;
        entry.owner_cookie = cookie;
      }
    } else {
      std::lock_guard<std::mutex> lock(s.mutex);
      s.entries[id].initial_result_done = true;
    }
    // No native pointers are placed in the shared work queue. Lifetime pinning
    // ends on return; the prepared job holds independent space leases instead.
    std::shared_ptr<Preserve_trx_temp_prebuild_job> job;
    const auto generation = ++s.next_generation;
    const auto name = "tempwarm_" + std::to_string(s.config.drain_generation) +
                      "_" + std::to_string(id) + "_" +
                      std::to_string(generation);
    bool initial_settled = false;
    if (!preserve_trx_temp_table_prepare_phase1_job(target, directory, name,
                                                    &job, &initial_settled)) {
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             "PRESERVE: temporary ordinary capture boundary failed");
      return false;
    }
    std::lock_guard<std::mutex> lock(s.mutex);
    auto &entry = s.entries[id];
    if (!job) {
      entry.initial_temp_done |= initial_settled;
      return true;
    }
    entry.owner_cookie = cookie;
    entry.generation = generation;
    entry.job = std::move(job);
    return true;
  } catch (const std::bad_alloc &) {
    return false;
  }
}

bool Preserve_trx_temp_prebuild_owner::submit() {
  auto &s = *m_impl;
  if (!s.pipeline || s.finishing) return true;
  std::unique_lock<std::mutex> lock(s.mutex);
  s.active_count = s.waiting_count = 0;
  for (auto &item : s.entries) {
    auto &entry = item.second;
    if (s.results && !entry.inflight &&
        (!entry.initial_result_done || !entry.initial_temp_done)) {
      bool initial_temp_absent = false;
      const auto state = s.results->state(item.first, &initial_temp_absent);
      // The command owner observed this at a complete boundary. It settles
      // only the initial attempt; later TEMP changes still reach final capture.
      entry.initial_temp_done |= initial_temp_absent;
      entry.result_pending = state == Preserve_trx_result_pretransfer::State::RUNNABLE;
      entry.initial_result_done = state == Preserve_trx_result_pretransfer::State::COMPLETE;
      if (entry.result_pending && !entry.generation) entry.generation = ++s.next_generation;
    }
    s.active_count += Impl::active(entry);
    s.waiting_count += Impl::waiting(entry);
  }
  s.statistics_valid = true;
  // Finish the compact sweep even when capacity is full. Resume admission at
  // the first unadmitted token, so a large prefix cannot monopolize the pool.
  auto next = s.entries.lower_bound(s.next_submit_token);
  for (size_t remaining = s.entries.size(); remaining; --remaining) {
    if (next == s.entries.end()) next = s.entries.begin();
    auto &item = *next++;
    auto &entry = item.second;
    s.next_submit_token = next == s.entries.end() ? 0 : next->first;
    if ((!entry.job && !entry.result_pending) || entry.inflight) continue;
    Preserve_trx_phase1_work_descriptor work;
    work.attempt_id = s.config.attempt_id;
    work.drain_generation = s.config.drain_generation;
    work.target_thread_id = item.first;
    work.target_incarnation = entry.owner_cookie;
    work.family_version = entry.generation;
    work.family = Preserve_trx_phase1_pipeline_family::TEMP_TABLE;
    work.capture_byte_limit = s.step_bytes;
    work.estimated_credit_bytes = s.step_bytes;
    const auto status = s.pipeline->try_submit(work);
    using Status = Preserve_trx_phase1_pipeline_submit_status;
    if (status == Status::ADMITTED)
      entry.inflight = true;
    else if (status == Status::NO_SLOT || status == Status::NO_CREDIT) {
      s.next_submit_token = item.first;
      break;
    }
    else if (status == Status::DEADLINE || status == Status::NOT_RUNNING) {
      s.finishing = true;
      s.pipeline->clear_temp_demand();
      break;
    } else
      return false;
  }
  lock.unlock();
  if (s.finishing) finish_submissions();
  return true;
}

Preserve_trx_phase1_pipeline_result_status
Preserve_trx_temp_prebuild_owner::step(
    const Preserve_trx_phase1_work_descriptor &work, size_t bytes,
    std::string *reason) {
  using Status = Preserve_trx_phase1_pipeline_result_status;
  std::shared_ptr<Preserve_trx_temp_prebuild_job> job;
  bool send_result = false;
  {
    std::lock_guard<std::mutex> lock(m_impl->mutex);
    const auto it = m_impl->entries.find(work.target_thread_id);
    if (it == m_impl->entries.end() || !it->second.inflight ||
        it->second.owner_cookie != work.target_incarnation ||
        it->second.generation != work.family_version)
      return Status::IDENTITY_STALE;
    job = it->second.job;
    send_result = it->second.result_pending && (it->second.prefer_result || !job);
    it->second.prefer_result = !send_result;
  }
  if (send_result) {
    bool complete = false;
    const auto status = m_impl->results->step(m_impl->session,
        work.target_thread_id, bytes, &complete);
    if (status != Preserve_trx_transfer_status::OK) {
      std::lock_guard<std::mutex> lock(m_impl->mutex);
      m_impl->entries.at(work.target_thread_id).result_failed = true;
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: result pretransfer failed status=" +
              std::to_string(static_cast<unsigned>(status))).c_str());
      *reason = "result_pretransfer_failed";
      return Status::CONSISTENCY_CONFLICT;
    }
    return Status::NO_PROGRESS;
  }
  if (!job) return Status::IDENTITY_STALE;
  using Progress = Preserve_trx_temp_prebuild_job::Progress;
  switch (job->step(bytes, m_impl->session, work.target_thread_id)) {
    case Progress::MORE:
      return Status::NO_PROGRESS;
    case Progress::DONE:
      return Status::PREPARED;
    case Progress::STALE:
      return Status::IDENTITY_STALE;
    case Progress::FAILED:
      try {
        *reason = job->reason();
      } catch (const std::bad_alloc &) {
      }
      return Status::UNSUPPORTED;
  }
  return Status::UNSUPPORTED;
}

bool Preserve_trx_temp_prebuild_owner::consume(
    const Preserve_trx_phase1_prepared_result &result) {
  auto &s = *m_impl;
  using Status = Preserve_trx_phase1_pipeline_result_status;
  if (!s.pipeline ||
      result.family != Preserve_trx_phase1_pipeline_family::TEMP_TABLE ||
      result.attempt_id != s.config.attempt_id ||
      result.drain_generation != s.config.drain_generation)
    return false;
  std::shared_ptr<Preserve_trx_temp_prebuild_job> job;
  {
    std::lock_guard<std::mutex> lock(s.mutex);
    const auto it = s.entries.find(result.target_thread_id);
    if (it == s.entries.end() || !it->second.inflight ||
        it->second.owner_cookie != result.target_incarnation ||
        it->second.generation != result.family_version)
      return false;
    s.statistics_valid = false;
    it->second.inflight = false;
    if (it->second.result_failed) return false;
    using Result_state = Preserve_trx_result_pretransfer::State;
    const auto state = s.results ? s.results->state(result.target_thread_id)
                                : Result_state::COMPLETE;
    it->second.result_pending = !s.finishing && state == Result_state::RUNNABLE;
    if (state == Result_state::COMPLETE) it->second.initial_result_done = true;
    if (result.status == Status::IDENTITY_STALE)
      it->second.initial_temp_done = true;  // Optional attempt deferred to final.
    if (result.status != Status::NO_PROGRESS || s.finishing)
      job = std::move(it->second.job);
  }
  if (!s.pipeline->settle_result(
          result.admission_id,
          Preserve_trx_phase1_pipeline_result_disposition::DROP))
    return false;
  if (!job || s.finishing ||
      (result.status != Status::PREPARED &&
       result.status != Status::UNSUPPORTED))
    return true;
  Find_thd_with_id finder(static_cast<my_thread_id>(result.target_thread_id));
  THD *target = Global_THD_manager::get_instance()->find_thd(&finder);
  if (!target) {
    std::lock_guard<std::mutex> lock(s.mutex);
    s.entries[result.target_thread_id].initial_temp_done = true;
    return true;
  }
  auto pin = preserve_trx_acquire_external_thd_pin_locked(target);
  mysql_mutex_unlock(&target->LOCK_thd_data);
  if (!pin) {
    std::lock_guard<std::mutex> lock(s.mutex);
    s.entries[result.target_thread_id].initial_temp_done = true;
    return true;
  }
  const bool initial_done = job->initial_baseline_complete();
  const auto installed = job->install(pin.thd());
  if (installed == Preserve_trx_temp_prebuild_job::Install::BUSY) {
    std::lock_guard<std::mutex> lock(s.mutex);
    s.entries[result.target_thread_id].job = std::move(job);
  } else if (installed == Preserve_trx_temp_prebuild_job::Install::STALE ||
             (installed == Preserve_trx_temp_prebuild_job::Install::INSTALLED &&
              initial_done)) {
    std::lock_guard<std::mutex> lock(s.mutex);
    s.entries[result.target_thread_id].initial_temp_done = true;
  }
  return installed != Preserve_trx_temp_prebuild_job::Install::FAILED;
}

void Preserve_trx_temp_prebuild_owner::finish_submissions() {
  auto &s = *m_impl;
  if (s.results) s.results->close_capture();
  s.finishing = true;
  // Only the drain thread changes the map. Workers look up entries under the
  // mutex; slow job cleanup never holds that mutex or a pipeline mutex.
  for (auto &entry : s.entries) {
    // A partial result sample cannot be sent. Final may complete and validate it
    // under exclusive THD ownership after join; stop ordinary work here.
    std::shared_ptr<Preserve_trx_temp_prebuild_job> discard;
    {
      std::lock_guard<std::mutex> lock(s.mutex);
      s.statistics_valid = false;
      if (!entry.second.inflight) discard = std::move(entry.second.job);
      entry.second.result_pending = false;
    }
  }
  if (s.pipeline) s.pipeline->clear_temp_demand();
}

bool Preserve_trx_temp_prebuild_owner::complete() const {
  std::lock_guard<std::mutex> lock(m_impl->mutex);
  for (const auto &entry : m_impl->entries)
    if (entry.second.job || entry.second.inflight || entry.second.result_pending) return false;
  return true;
}

bool Preserve_trx_temp_prebuild_owner::initial_baselines_complete(
    bool (*eligible_locked)(THD *)) {
  if (m_impl->finishing) return true;
  // Only the drain thread changes entries or calls this method. Do not keep
  // the worker mutex while looking up an owner that left before admission.
  bool complete = true;
  for (auto &entry : m_impl->entries) {
    {
      std::lock_guard<std::mutex> lock(m_impl->mutex);
      if (entry.second.initial_temp_done && entry.second.initial_result_done) continue;
      if (entry.second.job || entry.second.inflight) {
        complete = false;
        continue;
      }
    }
    Find_thd_with_id finder(static_cast<my_thread_id>(entry.first));
    THD *target = Global_THD_manager::get_instance()->find_thd(&finder);
    // An idle owner can close its last resource or leave the supported set
    // without disconnecting. It must not block other owners or the cutoff.
    // Busy commands and already declared work retain their normal boundary.
    const bool waiting = target &&
                         (!target->m_server_idle || eligible_locked(target));
    if (target) mysql_mutex_unlock(&target->LOCK_thd_data);
    if (waiting) {
      complete = false;
    } else {
      // If this owner leaves the cohort, no later idle sweep can capture its
      // results. Finish sending any files already pinned by the worker.
      if (m_impl->results) m_impl->results->abandon_capture(entry.first);
      std::lock_guard<std::mutex> lock(m_impl->mutex);
      m_impl->statistics_valid = false;
      entry.second.result_pending = m_impl->results &&
          m_impl->results->state(entry.first) == Preserve_trx_result_pretransfer::State::RUNNABLE;
      if (entry.second.result_pending) { complete = false; continue; }
      entry.second.initial_temp_done = entry.second.initial_result_done = true;
    }
  }
  return complete;
}

void Preserve_trx_temp_prebuild_owner::discard_after_join(bool discard_results) {
  m_impl->statistics_valid = false;
  for (auto &entry : m_impl->entries) {
    entry.second.job.reset();
    entry.second.inflight = entry.second.result_pending = false;
  }
  // Cancelling the pipeline can still lead to final preserve. Keep only the
  // token identities until terminal cleanup retires any unused result sample.
  if (discard_results) m_impl->entries.clear();
  m_impl->finishing = true;
}

bool Temp_table_warmcopy_participant::reserve_prebuilt_sidecars(size_t count) {
  try {
    if (count > m_prebuilt_sidecars.max_size() - m_prebuilt_sidecars.size()) return false;
    m_prebuilt_sidecars.reserve(m_prebuilt_sidecars.size() + count);
    return true;
  } catch (const std::bad_alloc &) { return false; }
}

std::unique_ptr<Temp_table_warmcopy_participant::Prebuilt_sidecar>
Temp_table_warmcopy_participant::take_prebuilt_sidecar(uint32_t space) {
  for (auto it = m_prebuilt_sidecars.begin(); it != m_prebuilt_sidecars.end(); ++it) {
    if (*it && (*it)->source_space_id == space) {
      auto result = std::move(*it);
      m_prebuilt_sidecars.erase(it);
      return result;
    }
  }
  return nullptr;
}

bool Temp_table_warmcopy_participant::remember_prebuilt_sidecar(
    std::unique_ptr<Prebuilt_sidecar> sidecar) {
  if (sidecar == nullptr || sidecar->source_space_id == 0 ||
      sidecar->warmcopy_id.empty())
    return false;
  if (find_prebuilt_sidecar(sidecar->source_space_id) != nullptr)
    return false;
  m_prebuilt_sidecars.push_back(std::move(sidecar));
  return true;
}

Temp_table_warmcopy_participant::Prebuilt_sidecar *
Temp_table_warmcopy_participant::find_prebuilt_sidecar(
    uint32_t source_space_id) {
  auto it = std::find_if(
      m_prebuilt_sidecars.begin(), m_prebuilt_sidecars.end(),
      [source_space_id](const std::unique_ptr<Prebuilt_sidecar> &sidecar) {
        return sidecar != nullptr && sidecar->source_space_id == source_space_id;
      });
  return it == m_prebuilt_sidecars.end() ? nullptr : it->get();
}

const Temp_table_warmcopy_participant::Prebuilt_sidecar *
Temp_table_warmcopy_participant::find_prebuilt_sidecar(
    uint32_t source_space_id) const {
  auto it = std::find_if(
      m_prebuilt_sidecars.begin(), m_prebuilt_sidecars.end(),
      [source_space_id](const std::unique_ptr<Prebuilt_sidecar> &sidecar) {
        return sidecar != nullptr && sidecar->source_space_id == source_space_id;
      });
  return it == m_prebuilt_sidecars.end() ? nullptr : it->get();
}
