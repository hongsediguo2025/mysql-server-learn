/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation. */

#include "sql/preserve_trx_phase1_binlog_adapter.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <utility>
#include <vector>

#include <openssl/evp.h>

#include "my_dbug.h"
#include "my_loglevel.h"
#include "mysql/components/services/log_builtins.h"
#include "sql/binlog_warmcopy.h"
#include "sql/current_thd.h"
#include "sql/debug_sync.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_bundle.h"
#include "sql/preserve_trx_carrier.h"
#include "sql/sql_class.h"
#include "storage/innobase/include/trx0preserve.h"

struct Preserve_trx_phase1_binlog_prepared_payload {
  Preserve_trx_phase1_work_descriptor descriptor;
  PrebuiltBinlogCacheBlob blob;
  std::string live_warmcopy_id;
  std::string inline_payload;
  bool append_prefix{false};
  mutable std::mutex cleanup_mutex;
  mutable std::unique_ptr<Preserved_trx_warm_external_blob_carrier>
      cleanup_carrier;
  mutable bool cleanup_armed{false};

  ~Preserve_trx_phase1_binlog_prepared_payload() {
    if (!discard()) {
      LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
             "PRESERVE: Phase1 binlog artifact RAII cleanup failed");
    }
  }

  bool discard() const {
    std::lock_guard<std::mutex> guard(cleanup_mutex);
    if (!cleanup_armed) return true;
    if (cleanup_carrier == nullptr || blob.warmcopy_id.empty() ||
        blob.name.empty()) {
      return false;
    }
    const Preserved_trx_carrier_status status =
        cleanup_carrier->remove_warm_external_blob(blob.warmcopy_id,
                                                    blob.name);
    if (status != Preserved_trx_carrier_status::OK &&
        status != Preserved_trx_carrier_status::NOT_FOUND) {
      return false;
    }
    cleanup_armed = false;
    cleanup_carrier.reset();
    return true;
  }

  void transfer() const {
    std::lock_guard<std::mutex> guard(cleanup_mutex);
    cleanup_armed = false;
    cleanup_carrier.reset();
  }
};

namespace {

uint64_t monotonic_us() {
  using clock = std::chrono::steady_clock;
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          clock::now().time_since_epoch())
          .count());
}

bool cancelled(const Preserve_trx_phase1_binlog_adapter_control &control) {
  return control.cancel_probe != nullptr &&
         control.cancel_probe(control.cancel_context);
}

bool deadline_reached(
    const Preserve_trx_phase1_binlog_adapter_control &control) {
  return control.deadline_us != 0 && monotonic_us() >= control.deadline_us;
}

bool descriptors_equal(const Preserve_trx_phase1_work_descriptor &left,
                       const Preserve_trx_phase1_work_descriptor &right) {
  return left.attempt_id == right.attempt_id &&
         left.drain_generation == right.drain_generation &&
         left.target_thread_id == right.target_thread_id &&
         left.target_incarnation == right.target_incarnation &&
         left.family_version == right.family_version &&
         left.source_owner_cookie == right.source_owner_cookie &&
         left.source_object_cookie == right.source_object_cookie &&
         left.warmcopy_epoch == right.warmcopy_epoch &&
         left.expected_immutable_trx_id == right.expected_immutable_trx_id &&
         left.expected_trx_version == right.expected_trx_version &&
         left.capture_generation == right.capture_generation &&
         left.estimated_credit_bytes == right.estimated_credit_bytes &&
         left.expected_store_baseline_generation ==
             right.expected_store_baseline_generation &&
         left.expected_lock_coordinate_generation ==
             right.expected_lock_coordinate_generation &&
         left.binlog_prefix_progress == right.binlog_prefix_progress &&
         left.binlog_prefix_size == right.binlog_prefix_size &&
         left.binlog_prefix_truncate_generation ==
             right.binlog_prefix_truncate_generation &&
         left.binlog_prefix_digest == right.binlog_prefix_digest &&
         left.binlog_minimum_delta_bytes == right.binlog_minimum_delta_bytes &&
         left.binlog_wire_chunk_bytes == right.binlog_wire_chunk_bytes &&
         left.family == right.family &&
         left.final_generation == right.final_generation &&
         left.use_record_store_snapshot == right.use_record_store_snapshot;
}

enum class Exact_pin_status : uint8_t { PINNED, INVALID, STALE };

Exact_pin_status pin_exact_target(
    const Preserve_trx_phase1_work_descriptor &descriptor,
    Preserve_trx_external_thd_pin_handle *pin) {
  if (pin == nullptr || descriptor.target_thread_id == 0 ||
      descriptor.target_thread_id >
          std::numeric_limits<my_thread_id>::max() ||
      descriptor.target_incarnation == 0 ||
      descriptor.source_owner_cookie == 0 ||
      descriptor.source_object_cookie == 0) {
    return Exact_pin_status::INVALID;
  }
  *pin = {};
  Find_thd_with_id finder(
      static_cast<my_thread_id>(descriptor.target_thread_id));
  THD *target = Global_THD_manager::get_instance()->find_thd(&finder);
  if (target == nullptr) return Exact_pin_status::STALE;
  const uint64_t owner_cookie =
      static_cast<uint64_t>(reinterpret_cast<uintptr_t>(target));
  const uint64_t object_cookie =
      trx_preserve_phase1_peek_raw_cookie(target);
  if (owner_cookie != descriptor.source_owner_cookie ||
      object_cookie != descriptor.source_object_cookie ||
      target->release_resources_done()) {
    mysql_mutex_unlock(&target->LOCK_thd_data);
    return Exact_pin_status::STALE;
  }
  *pin = preserve_trx_acquire_external_thd_pin_locked(target);
  mysql_mutex_unlock(&target->LOCK_thd_data);
  return *pin && pin->thd() == target ? Exact_pin_status::PINNED
                                     : Exact_pin_status::STALE;
}

bool exact_transaction_identity(
    THD *target, const Preserve_trx_phase1_work_descriptor &descriptor) {
  trx_preserve_phase1_identity identity;
  return target != nullptr &&
         trx_preserve_phase1_owner_identity_snapshot(target, &identity) &&
         identity.owner_thd_cookie == descriptor.source_owner_cookie &&
         identity.raw_cookie == descriptor.source_object_cookie &&
         identity.immutable_trx_id == descriptor.expected_immutable_trx_id &&
         identity.trx_version == descriptor.expected_trx_version;
}

bool prefix_still_current(
    THD *target, Preserve_trx_phase1_binlog_provider_port *provider,
    const PrebuiltBinlogCacheBlob &prefix, const std::string &live_id) {
  PrebuiltBinlogCacheBlob current;
  return provider != nullptr && provider->sample_prefix(target, &current) &&
         current.warmcopy_id == live_id && current.name == prefix.name &&
         current.warmcopy_epoch == prefix.warmcopy_epoch &&
         current.phase1_truncate_generation ==
             prefix.phase1_truncate_generation &&
         current.size >= prefix.size &&
         (current.size != prefix.size || current.digest == prefix.digest);
}

bool reserve_credit(
    const Preserve_trx_phase1_binlog_adapter_control &control,
    uint64_t required,
    Preserve_trx_phase1_binlog_adapter_outcome *outcome) {
  if (control.reserve_credit == nullptr) return true;
  const Preserve_trx_phase1_pipeline_credit_status status =
      control.reserve_credit(control.credit_context, required);
  if (status == Preserve_trx_phase1_pipeline_credit_status::GRANTED)
    return true;
  outcome->status =
      status == Preserve_trx_phase1_pipeline_credit_status::DEADLINE
          ? Preserve_trx_phase1_pipeline_result_status::DEADLINE
          : status == Preserve_trx_phase1_pipeline_credit_status::CANCELLED
                ? Preserve_trx_phase1_pipeline_result_status::CANCELLED
                : Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
  outcome->reason = "binlog_prefix_credit_unavailable";
  return false;
}

void set_copy_cutoff(
    const Preserve_trx_phase1_binlog_adapter_control &control,
    Preserve_trx_phase1_binlog_adapter_outcome *outcome,
    const char *reason) {
  const bool was_cancelled = cancelled(control);
  outcome->status =
      was_cancelled
          ? Preserve_trx_phase1_pipeline_result_status::CANCELLED
          : Preserve_trx_phase1_pipeline_result_status::DEADLINE;
  outcome->reason = reason == nullptr ? "binlog_snapshot_cutoff" : reason;
}

bool cleanup_unpublished_snapshot(
    Preserved_trx_warm_external_blob_carrier *carrier,
    Preserved_trx_external_blob_writer *writer,
    const std::string &warmcopy_id, const std::string &object_name,
    Preserve_trx_phase1_binlog_adapter_outcome *outcome) {
  if (writer != nullptr) (void)writer->abort();
  const Preserved_trx_carrier_status status =
      carrier == nullptr
          ? Preserved_trx_carrier_status::IO_ERROR
          : carrier->remove_warm_external_blob(warmcopy_id, object_name);
  if (status == Preserved_trx_carrier_status::OK ||
      status == Preserved_trx_carrier_status::NOT_FOUND) {
    return true;
  }
  if (outcome != nullptr) {
    outcome->status = Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
    outcome->reason = "binlog_snapshot_cleanup_failed";
  }
  return false;
}

bool snapshot_prefix_in_chunks(
    Preserved_trx_warm_external_blob_carrier *carrier,
    const PrebuiltBinlogCacheBlob &source,
    const std::string &destination_warmcopy_id,
    const Preserve_trx_phase1_binlog_adapter_control &control,
    Preserve_trx_phase1_binlog_adapter_outcome *outcome) {
  if (carrier == nullptr || outcome == nullptr || source.size == 0 ||
      source.warmcopy_id.empty() || source.name.empty() ||
      destination_warmcopy_id.empty() || control.copy_chunk_bytes == 0) {
    if (outcome != nullptr) {
      outcome->status =
          Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
      outcome->reason = "binlog_snapshot_chunk_config_invalid";
    }
    return false;
  }

  std::unique_ptr<Preserved_trx_external_blob_writer> writer;
  if (carrier->create_warm_external_blob_writer(
          destination_warmcopy_id, source.name, source.warmcopy_epoch,
          &writer) != Preserved_trx_carrier_status::OK ||
      writer == nullptr) {
    outcome->status = Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
    outcome->reason = "binlog_snapshot_writer_open_failed";
    return false;
  }

  EVP_MD_CTX *digest_ctx = EVP_MD_CTX_new();
  if (digest_ctx == nullptr ||
      EVP_DigestInit_ex(digest_ctx, EVP_sha256(), nullptr) != 1) {
    if (digest_ctx != nullptr) EVP_MD_CTX_free(digest_ctx);
    outcome->status =
        Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
    outcome->reason = "binlog_snapshot_digest_init_failed";
    (void)cleanup_unpublished_snapshot(
        carrier, writer.get(), destination_warmcopy_id, source.name, outcome);
    return false;
  }

  bool failed = false;
  uint64_t offset = 0;
  while (offset < source.size) {
    if (cancelled(control) || deadline_reached(control)) {
      set_copy_cutoff(control, outcome, "binlog_snapshot_chunk_cutoff");
      failed = true;
      break;
    }
    const uint64_t length = std::min<uint64_t>(
        source.size - offset, control.copy_chunk_bytes);
    std::string payload;
    if (carrier->read_active_warm_external_blob_range(
            source.warmcopy_id, source.name, source.warmcopy_epoch, offset,
            length, control.copy_chunk_bytes, &payload) !=
            Preserved_trx_carrier_status::OK ||
        payload.size() != length ||
        EVP_DigestUpdate(digest_ctx, payload.data(), payload.size()) != 1 ||
        writer->write_at(
            offset, reinterpret_cast<const unsigned char *>(payload.data()),
            payload.size()) != Preserved_trx_carrier_status::OK) {
      outcome->status =
          Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
      outcome->reason = "binlog_snapshot_chunk_copy_failed";
      failed = true;
      break;
    }
    offset += length;
    if (cancelled(control) || deadline_reached(control)) {
      set_copy_cutoff(control, outcome, "binlog_snapshot_chunk_cutoff");
      failed = true;
      break;
    }
  }

  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  unsigned int digest_length = 0;
  if (!failed &&
      (EVP_DigestFinal_ex(digest_ctx, digest.data(), &digest_length) != 1 ||
       digest_length != digest.size() || digest != source.digest)) {
    outcome->status = Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
    outcome->reason = "binlog_snapshot_digest_mismatch";
    failed = true;
  }
  EVP_MD_CTX_free(digest_ctx);

  Preserved_trx_external_blob_descriptor descriptor;
  descriptor.name = source.name;
  descriptor.size = source.size;
  descriptor.digest = source.digest;
  if (!failed &&
      (writer->close() != Preserved_trx_carrier_status::OK ||
       writer->seal_descriptor(descriptor) !=
           Preserved_trx_carrier_status::OK)) {
    outcome->status = Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
    outcome->reason = "binlog_snapshot_seal_failed";
    failed = true;
  }
  if (failed) {
    (void)cleanup_unpublished_snapshot(
        carrier, writer.get(), destination_warmcopy_id, source.name, outcome);
    return false;
  }
  return true;
}

struct Membership {
  uint64_t thread_id{0};
  uint64_t owner_cookie{0};
};

class Membership_collector final : public Do_THD_Impl {
 public:
  explicit Membership_collector(THD *owner) : m_owner(owner) {}

  void operator()(THD *candidate) override {
    if (candidate == nullptr || candidate == m_owner) return;
    m_memberships.push_back(
        {static_cast<uint64_t>(candidate->thread_id()),
         static_cast<uint64_t>(reinterpret_cast<uintptr_t>(candidate))});
  }

  std::vector<Membership> take() { return std::move(m_memberships); }

 private:
  THD *m_owner;
  std::vector<Membership> m_memberships;
};

struct Binlog_binding {
  uint64_t thread_id{0};
  uint64_t owner_cookie{0};
  uint64_t raw_trx_cookie{0};
  uint64_t immutable_trx_id{0};
  uint64_t trx_version{0};
};

bool same_binding(const Binlog_binding &left, const Binlog_binding &right) {
  return left.thread_id == right.thread_id &&
         left.owner_cookie == right.owner_cookie &&
         left.raw_trx_cookie == right.raw_trx_cookie &&
         left.immutable_trx_id == right.immutable_trx_id &&
         left.trx_version == right.trx_version;
}

bool resolve_binding(const Membership &membership, Binlog_binding *binding) {
  if (binding == nullptr || membership.thread_id == 0 ||
      membership.thread_id > std::numeric_limits<my_thread_id>::max() ||
      membership.owner_cookie == 0) {
    return false;
  }
  Find_thd_with_id finder(static_cast<my_thread_id>(membership.thread_id));
  THD *target = Global_THD_manager::get_instance()->find_thd(&finder);
  if (target == nullptr) return false;
  const bool eligible =
      static_cast<uint64_t>(reinterpret_cast<uintptr_t>(target)) ==
          membership.owner_cookie &&
      !target->release_resources_done() && !target->is_system_thread() &&
      target->killed == THD::NOT_KILLED &&
      target->preserve_trx_batch_state == Preserve_trx_batch_thd_state::NONE &&
      preserve_trx_phase1_record_candidate_eligible_locked(target) &&
      !preserve_trx_phase1_target_unsupported_locked(target);
  Preserve_trx_external_thd_pin_handle pin;
  if (eligible) pin = preserve_trx_acquire_external_thd_pin_locked(target);
  mysql_mutex_unlock(&target->LOCK_thd_data);
  if (!pin || pin.thd() != target) return false;

  trx_preserve_phase1_identity identity;
  if (!trx_preserve_phase1_owner_identity_snapshot(pin.thd(), &identity) ||
      identity.owner_thd_cookie != membership.owner_cookie ||
      identity.raw_cookie == 0 || identity.immutable_trx_id == 0 ||
      identity.trx_version == 0) {
    return false;
  }
  *binding = {membership.thread_id, membership.owner_cookie,
              identity.raw_cookie, identity.immutable_trx_id,
              identity.trx_version};
  return true;
}

}  // namespace

void preserve_trx_phase1_binlog_adapter_prepare(
    THD *worker_thd, const Preserve_trx_phase1_work_descriptor &descriptor,
    Preserve_trx_phase1_binlog_provider_port *provider,
    const Preserve_trx_phase1_binlog_adapter_control &control,
    Preserve_trx_phase1_binlog_adapter_outcome *outcome) {
  if (outcome == nullptr) return;
  *outcome = {};
  if (worker_thd == nullptr || worker_thd != current_thd ||
      provider == nullptr || descriptor.family !=
                                 Preserve_trx_phase1_pipeline_family::
                                     BINLOG_CACHE ||
      descriptor.warmcopy_epoch == 0 ||
      descriptor.expected_immutable_trx_id == 0 ||
      descriptor.expected_trx_version == 0 ||
      descriptor.capture_generation == 0 ||
      descriptor.estimated_credit_bytes == 0 || descriptor.final_generation) {
    outcome->status = Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
    outcome->reason = "binlog_descriptor_invalid";
    return;
  }
  if (cancelled(control) || deadline_reached(control)) {
    outcome->status = cancelled(control)
                          ? Preserve_trx_phase1_pipeline_result_status::CANCELLED
                          : Preserve_trx_phase1_pipeline_result_status::DEADLINE;
    outcome->reason = "binlog_prepare_cutoff";
    return;
  }

  Preserve_trx_external_thd_pin_handle pin;
  const Exact_pin_status pin_status = pin_exact_target(descriptor, &pin);
  if (pin_status != Exact_pin_status::PINNED) {
    outcome->status = pin_status == Exact_pin_status::INVALID
                          ? Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED
                          : Preserve_trx_phase1_pipeline_result_status::
                                IDENTITY_STALE;
    outcome->reason = "binlog_prepare_target_stale";
    return;
  }
  if (!exact_transaction_identity(pin.thd(), descriptor)) {
    outcome->status =
        Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE;
    outcome->reason = "binlog_prepare_transaction_changed";
    return;
  }

  PrebuiltBinlogCacheBlob live_prefix;
  if (descriptor.binlog_prefix_progress) {
    if (!provider->sample_prefix(pin.thd(), &live_prefix) ||
        live_prefix.warmcopy_epoch != descriptor.warmcopy_epoch) {
      outcome->status = Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
      outcome->reason = "binlog_progress_sample_unavailable";
      return;
    }
    const bool append = live_prefix.phase1_truncate_generation ==
                        descriptor.binlog_prefix_truncate_generation;
    if (append && (live_prefix.size < descriptor.binlog_prefix_size ||
        (live_prefix.size == descriptor.binlog_prefix_size &&
         live_prefix.digest != descriptor.binlog_prefix_digest))) {
      outcome->status = Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
      outcome->reason = "binlog_progress_prefix_stale";
      return;
    }
    const uint64_t start = append ? descriptor.binlog_prefix_size : 0;
    const uint64_t bytes = live_prefix.size - start;
    if (bytes == 0 || (append && bytes < descriptor.binlog_minimum_delta_bytes)) {
      outcome->status = Preserve_trx_phase1_pipeline_result_status::NO_PROGRESS;
      return;
    }
    /* Cover inline, codec and chunk copies before allocating the range. */
    if (control.copy_chunk_bytes == 0 ||
        descriptor.binlog_wire_chunk_bytes == 0 ||
        bytes > std::numeric_limits<size_t>::max()) {
      outcome->status = Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
      outcome->reason = "binlog_progress_size_invalid";
      return;
    }
    const uint64_t frames = bytes / descriptor.binlog_wire_chunk_bytes +
        (bytes % descriptor.binlog_wire_chunk_bytes != 0 ? 1 : 0);
    if (frames > UINT64_MAX / 4096 - 3 ||
        bytes > (UINT64_MAX - 4096 * (frames + 3)) / 12) {
      outcome->status = Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
      outcome->reason = "binlog_progress_credit_overflow";
      return;
    }
    const uint64_t credit = 12 * bytes + 4096 * (frames + 3);
    if (!reserve_credit(control, credit, outcome)) return;
    try {
      auto prepared =
          std::make_shared<Preserve_trx_phase1_binlog_prepared_payload>();
      prepared->inline_payload.reserve(static_cast<size_t>(bytes));
      auto carrier = create_preserved_trx_process_local_warm_external_blob_carrier(
          provider->artifact_dir());
      for (uint64_t offset = start; offset < live_prefix.size;) {
        if (cancelled(control) || deadline_reached(control)) {
          set_copy_cutoff(control, outcome, "binlog_progress_copy_cutoff");
          return;
        }
        const uint64_t length = std::min<uint64_t>(
            live_prefix.size - offset, control.copy_chunk_bytes);
        std::string chunk;
        if (carrier == nullptr ||
            carrier->read_active_warm_external_blob_range(
                live_prefix.warmcopy_id, live_prefix.name,
                live_prefix.warmcopy_epoch, offset, length,
                control.copy_chunk_bytes, &chunk) !=
                Preserved_trx_carrier_status::OK || chunk.size() != length) {
          outcome->status = Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
          outcome->reason = "binlog_progress_copy_failed";
          return;
        }
        prepared->inline_payload.append(chunk);
        offset += length;
      }
      if (!exact_transaction_identity(pin.thd(), descriptor)) {
        outcome->status = Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE;
        outcome->reason = "binlog_progress_transaction_changed";
        return;
      }
      if (!prefix_still_current(pin.thd(), provider, live_prefix,
                                live_prefix.warmcopy_id)) {
        outcome->status = Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
        outcome->reason = "binlog_progress_final_fence_failed";
        return;
      }
      prepared->descriptor = descriptor;
      prepared->live_warmcopy_id = live_prefix.warmcopy_id;
      prepared->blob = std::move(live_prefix);
      prepared->append_prefix = append;
      outcome->logical_bytes = bytes;
      outcome->required_credit_bytes = credit;
      outcome->prepared_payload = std::move(prepared);
      outcome->status = Preserve_trx_phase1_pipeline_result_status::PREPARED;
    } catch (...) {
      outcome->status = Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
      outcome->reason = "binlog_progress_allocation_failed";
    }
    return;
  }
  const Preserve_trx_phase1_binlog_provider_status provider_status =
      provider->prepare_prefix(pin.thd(), descriptor, control.copy_chunk_bytes,
                               &live_prefix);
  if (provider_status !=
      Preserve_trx_phase1_binlog_provider_status::PREPARED) {
    switch (provider_status) {
      case Preserve_trx_phase1_binlog_provider_status::ABSENT:
        outcome->status = Preserve_trx_phase1_pipeline_result_status::ABSENT;
        break;
      case Preserve_trx_phase1_binlog_provider_status::IDENTITY_STALE:
        outcome->status =
            Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE;
        break;
      case Preserve_trx_phase1_binlog_provider_status::RETRYABLE:
        outcome->status =
            Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
        break;
      case Preserve_trx_phase1_binlog_provider_status::FAILED:
        outcome->status =
            Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
        break;
      case Preserve_trx_phase1_binlog_provider_status::PREPARED:
        break;
    }
    outcome->reason = "binlog_provider_prepare_failed";
    return;
  }
  if (!exact_transaction_identity(pin.thd(), descriptor) ||
      live_prefix.size == 0 || live_prefix.warmcopy_id.empty()) {
    outcome->status =
        Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE;
    outcome->reason = "binlog_provider_identity_changed";
    return;
  }
  pin = {};

  if (cancelled(control) || deadline_reached(control)) {
    outcome->status = cancelled(control)
                          ? Preserve_trx_phase1_pipeline_result_status::CANCELLED
                          : Preserve_trx_phase1_pipeline_result_status::DEADLINE;
    outcome->reason = "binlog_snapshot_cutoff";
    return;
  }
  if (!reserve_credit(control, live_prefix.size, outcome)) return;

  auto carrier = create_preserved_trx_process_local_warm_external_blob_carrier(
      provider->artifact_dir());
  if (carrier == nullptr) {
    outcome->status =
        Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
    outcome->reason = "binlog_snapshot_carrier_unavailable";
    return;
  }
  const std::string transfer_warmcopy_id =
      "transfer_binlog_" + std::to_string(descriptor.drain_generation) +
      "_" + std::to_string(descriptor.target_thread_id) + "_" +
      std::to_string(descriptor.target_incarnation) + "_" +
      std::to_string(descriptor.capture_generation);
  if (!snapshot_prefix_in_chunks(carrier.get(), live_prefix,
                                 transfer_warmcopy_id, control, outcome)) {
    return;
  }

  PrebuiltBinlogCacheBlob immutable = live_prefix;
  immutable.warmcopy_id = transfer_warmcopy_id;
  pin = {};
  const Exact_pin_status final_pin_status = pin_exact_target(descriptor, &pin);
  const bool identity_current =
      final_pin_status == Exact_pin_status::PINNED &&
      exact_transaction_identity(pin.thd(), descriptor);
  const bool current = identity_current &&
      prefix_still_current(pin.thd(), provider, immutable,
                            live_prefix.warmcopy_id);
  pin = {};
  if (!current || cancelled(control) || deadline_reached(control)) {
    outcome->status =
        !identity_current
            ? Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE
            : !current
                  ? Preserve_trx_phase1_pipeline_result_status::RETRYABLE
            : cancelled(control)
                  ? Preserve_trx_phase1_pipeline_result_status::CANCELLED
                  : Preserve_trx_phase1_pipeline_result_status::DEADLINE;
    outcome->reason = "binlog_snapshot_final_fence_failed";
    (void)cleanup_unpublished_snapshot(
        carrier.get(), nullptr, immutable.warmcopy_id, immutable.name, outcome);
    return;
  }

  try {
    auto prepared =
        std::make_shared<Preserve_trx_phase1_binlog_prepared_payload>();
    prepared->descriptor = descriptor;
    prepared->live_warmcopy_id = live_prefix.warmcopy_id;
    prepared->blob = std::move(immutable);
    prepared->cleanup_carrier = std::move(carrier);
    prepared->cleanup_armed = true;
    outcome->logical_bytes = prepared->blob.size;
    outcome->required_credit_bytes = prepared->blob.size;
    outcome->prepared_payload = std::move(prepared);
    outcome->status = Preserve_trx_phase1_pipeline_result_status::PREPARED;
  } catch (...) {
    outcome->status =
        Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
    outcome->reason = "binlog_result_allocation_failed";
    (void)cleanup_unpublished_snapshot(carrier.get(), nullptr,
                                       immutable.warmcopy_id, immutable.name,
                                       outcome);
  }
}

Preserve_trx_phase1_pipeline_result_status
preserve_trx_phase1_binlog_adapter_owner_revalidate(
    const Preserve_trx_phase1_work_descriptor &descriptor,
    const Preserve_trx_phase1_binlog_prepared_handle &payload,
    Preserve_trx_phase1_binlog_provider_port *provider,
    Preserve_trx_transfer_phase1_blob_request *request,
    std::string *reason) {
  if (request != nullptr) *request = {};
  if (reason != nullptr) reason->clear();
  if (payload == nullptr || request == nullptr ||
      !descriptors_equal(descriptor, payload->descriptor)) {
    if (reason != nullptr) *reason = "binlog_owner_payload_mismatch";
    return Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
  }
  Preserve_trx_external_thd_pin_handle pin;
  if (pin_exact_target(descriptor, &pin) != Exact_pin_status::PINNED ||
      !exact_transaction_identity(pin.thd(), descriptor)) {
    if (reason != nullptr) *reason = "binlog_owner_target_stale";
    return Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE;
  }
  if (!prefix_still_current(pin.thd(), provider, payload->blob,
                             payload->live_warmcopy_id)) {
    if (reason != nullptr) *reason = "binlog_owner_prefix_stale";
    return Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
  }
  request->transfer_token = descriptor.target_thread_id;
  request->object_id = payload->blob.name;
  request->warmcopy_id = payload->blob.warmcopy_id;
  request->warmcopy_epoch = payload->blob.warmcopy_epoch;
  request->size = payload->blob.size;
  request->digest = payload->blob.digest;
  try {
    request->inline_payload = payload->inline_payload;
  } catch (...) {
    if (reason != nullptr) *reason = "binlog_owner_payload_allocation_failed";
    return Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
  }
  if (payload->append_prefix) {
    request->preserved_prefix_size = descriptor.binlog_prefix_size;
    request->preserved_prefix_digest = descriptor.binlog_prefix_digest;
  }
  return Preserve_trx_phase1_pipeline_result_status::PREPARED;
}

const PrebuiltBinlogCacheBlob *preserve_trx_phase1_binlog_adapter_blob(
    const Preserve_trx_phase1_binlog_prepared_handle &payload) {
  return payload == nullptr ? nullptr : &payload->blob;
}

bool preserve_trx_phase1_binlog_adapter_discard(
    const Preserve_trx_phase1_binlog_prepared_handle &payload) {
  return payload == nullptr || payload->discard();
}

void preserve_trx_phase1_binlog_adapter_transfer(
    const Preserve_trx_phase1_binlog_prepared_handle &payload) {
  if (payload != nullptr) payload->transfer();
}

class Preserve_trx_phase1_binlog_owner::Impl {
 public:
  Impl(const Preserve_trx_phase1_binlog_owner_config &config,
       Preserve_trx_phase1_pipeline *pipeline,
       Preserve_trx_phase1_binlog_provider_port *provider,
       Preserve_trx_phase1_binlog_publisher_port *publisher,
      Preserve_trx_phase1_publication_registry *publication_registry)
      : m_config(config),
        m_pipeline(pipeline),
        m_provider(provider),
        m_publisher(publisher),
        m_publication_registry(publication_registry) {
    if (m_config.attempt_id == 0 || m_config.drain_generation == 0 ||
        m_config.initial_credit_bytes == 0 || m_pipeline == nullptr ||
        provider == nullptr || m_publisher == nullptr ||
        m_publication_registry == nullptr) {
      fail("binlog_owner_config_invalid");
    }
  }

  ~Impl() {
    for (const auto &item : m_entries) discard_payload(item.second.payload);
  }

  bool reconcile_targets(THD *drain_owner,
                         const std::vector<uint64_t> &declared_target_ids) {
    if (m_failed || drain_owner == nullptr ||
        Global_THD_manager::get_instance() == nullptr) {
      return false;
    }
    Membership_collector collector(drain_owner);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&collector);
    const std::vector<Membership> memberships = collector.take();
    const std::set<uint64_t> declared_targets(declared_target_ids.begin(),
                                               declared_target_ids.end());
    ++m_membership_round;
    for (const Membership &membership : memberships) {
      if (declared_targets.count(membership.thread_id) == 0) continue;
      Binlog_binding binding;
      if (!resolve_binding(membership, &binding)) continue;
      auto found = m_entries.find(binding.thread_id);
      if (found != m_entries.end() &&
          found->second.state == Entry_state::DEFERRED_TO_FINAL) {
        found->second.seen_round = m_membership_round;
        found->second.replacement_pending = false;
        found->second.retire_after_busy = false;
        continue;
      }
      if (found != m_entries.end() &&
          same_binding(found->second.binding, binding)) {
        found->second.seen_round = m_membership_round;
        found->second.replacement_pending = false;
        found->second.retire_after_busy = false;
        if (found->second.state == Entry_state::ABSENT) {
          found->second.state = Entry_state::READY;
          found->second.baseline_resolved = false;
          found->second.next_retry_us = 0;
        }
        continue;
      }
      if (found != m_entries.end() && busy(found->second)) {
        found->second.seen_round = m_membership_round;
        found->second.replacement_pending = true;
        found->second.retire_after_busy = false;
        continue;
      }
      if (found != m_entries.end()) {
        defer_to_final(found, true);
        found->second.seen_round = m_membership_round;
        continue;
      }
      if (!install_binding(binding)) return false;
    }
    for (auto it = m_entries.begin(); it != m_entries.end();) {
      if (it->second.seen_round == m_membership_round || busy(it->second)) {
        if (it->second.seen_round != m_membership_round) {
          it->second.replacement_pending = false;
          it->second.retire_after_busy = true;
        }
        ++it;
      } else {
        ++m_retired;
        it = m_entries.erase(it);
      }
    }
    return true;
  }

  bool consume_result(const Preserve_trx_phase1_prepared_result &result) {
    if (m_failed ||
        result.family != Preserve_trx_phase1_pipeline_family::BINLOG_CACHE ||
        result.attempt_id != m_config.attempt_id ||
        result.drain_generation != m_config.drain_generation ||
        result.record_payload != nullptr) {
      fail("binlog_owner_result_family_invalid");
      return false;
    }
    ++m_results;
    auto found = m_entries.find(result.target_thread_id);
    const bool current =
        found != m_entries.end() &&
        found->second.descriptor.target_incarnation ==
            result.target_incarnation &&
        found->second.descriptor.family_version == result.family_version &&
        found->second.state == Entry_state::INFLIGHT;
    if (!current) {
      ++m_stale_results;
      discard_payload(result.binlog_payload);
      return m_pipeline->settle_result(
          result.admission_id,
          Preserve_trx_phase1_pipeline_result_disposition::DROP);
    }

    Entry &entry = found->second;
    Preserve_trx_phase1_pipeline_result_status result_status = result.status;
    DBUG_EXECUTE_IF("preserve_trx_phase1_binlog_owner_inject_identity_stale", {
      if (!m_debug_identity_stale_injected &&
          (result_status ==
               Preserve_trx_phase1_pipeline_result_status::PREPARED ||
           result_status ==
               Preserve_trx_phase1_pipeline_result_status::ABSENT)) {
        result_status =
            Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE;
        m_debug_identity_stale_injected = true;
      }
    });
    if (result_status == Preserve_trx_phase1_pipeline_result_status::NO_PROGRESS &&
        entry.descriptor.binlog_prefix_progress) {
      if (!m_pipeline->settle_result(
              result.admission_id,
              Preserve_trx_phase1_pipeline_result_disposition::DROP)) {
        fail("binlog_progress_settle_failed");
        return false;
      }
      entry.state = Entry_state::PUBLISHED;
      entry.next_retry_us = monotonic_us() + 50000ULL;
      return finish_busy(found);
    }
    if (result_status ==
        Preserve_trx_phase1_pipeline_result_status::ABSENT) {
      if (!m_pipeline->settle_result(
              result.admission_id,
              Preserve_trx_phase1_pipeline_result_disposition::ABSENT)) {
        fail("binlog_absent_settle_failed");
        return false;
      }
      entry.state = Entry_state::ABSENT;
      entry.baseline_resolved = true;
      ++m_absent;
      return finish_busy(found);
    }
    if (result_status ==
        Preserve_trx_phase1_pipeline_result_status::PREPARED) {
      Preserve_trx_transfer_phase1_blob_request request;
      std::string reason;
      const Preserve_trx_phase1_pipeline_result_status validated =
          preserve_trx_phase1_binlog_adapter_owner_revalidate(
              entry.descriptor, result.binlog_payload, m_provider, &request,
              &reason);
      if (validated ==
          Preserve_trx_phase1_pipeline_result_status::PREPARED) {
        Preserve_trx_phase1_publication_handle publication_handle;
        const Preserve_trx_phase1_publication_reserve_status reserve_status =
            m_publication_registry->reserve(
                result.admission_id, result.target_incarnation, result.family,
                result.family_version, request, &publication_handle);
        if (reserve_status !=
            Preserve_trx_phase1_publication_reserve_status::RESERVED) {
          discard_payload(result.binlog_payload);
          (void)m_pipeline->settle_result(
              result.admission_id,
              Preserve_trx_phase1_pipeline_result_disposition::DROP);
          fail("binlog_publication_slot_unavailable");
          return false;
        }
        if (!m_pipeline->begin_publication(result.admission_id)) {
          (void)m_publication_registry->resolve_enqueue(
              publication_handle,
              Preserve_trx_transfer_status::INVALID_ARGUMENT);
          discard_payload(result.binlog_payload);
          (void)m_pipeline->settle_result(
              result.admission_id,
              Preserve_trx_phase1_pipeline_result_disposition::DROP);
          fail("binlog_publication_begin_failed");
          return false;
        }
        const Preserve_trx_transfer_status enqueue_status =
            m_publisher->enqueue(request);
        const Preserve_trx_phase1_publication_resolve_status resolve_status =
            m_publication_registry->resolve_enqueue(publication_handle,
                                                     enqueue_status);
        if (enqueue_status != Preserve_trx_transfer_status::OK &&
            resolve_status == Preserve_trx_phase1_publication_resolve_status::
                                  REJECTED_NO_CALLBACK) {
          discard_payload(result.binlog_payload);
          (void)m_pipeline->settle_publication(
              result.admission_id,
              Preserve_trx_phase1_pipeline_publication_status::FAILED);
          fail("binlog_sender_enqueue_failed");
          return false;
        }
        if (resolve_status !=
                Preserve_trx_phase1_publication_resolve_status::ACCEPTED &&
            resolve_status != Preserve_trx_phase1_publication_resolve_status::
                                  COMPLETED_BEFORE_RESOLVE) {
          if (enqueue_status == Preserve_trx_transfer_status::OK) {
            /*
              The existing sender may still be reading the artifact.  Retain
              ownership until its abort/reset/join barrier has completed.
            */
            entry.state = Entry_state::PUBLICATION_PENDING;
            entry.admission_id = result.admission_id;
            entry.payload = result.binlog_payload;
            m_pending_admissions.emplace(result.admission_id,
                                         result.target_thread_id);
          } else {
            discard_payload(result.binlog_payload);
            (void)m_pipeline->settle_publication(
                result.admission_id,
                Preserve_trx_phase1_pipeline_publication_status::FAILED);
          }
          fail("binlog_publication_resolve_failed");
          return false;
        }
        entry.state = Entry_state::PUBLICATION_PENDING;
        entry.admission_id = result.admission_id;
        entry.payload = result.binlog_payload;
        m_pending_admissions.emplace(result.admission_id,
                                     result.target_thread_id);
        return true;
      }
      discard_payload(result.binlog_payload);
      if (!m_pipeline->settle_result(
              result.admission_id,
              Preserve_trx_phase1_pipeline_result_disposition::DROP)) {
        fail("binlog_stale_result_settle_failed");
        return false;
      }
      if (validated !=
              Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE &&
          validated != Preserve_trx_phase1_pipeline_result_status::RETRYABLE) {
        fail(reason.empty() ? "binlog_owner_revalidate_failed" : reason);
        return false;
      }
      return retry_or_refresh_binding(
          found, validated ==
                     Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE);
    }

    const bool retryable =
        result_status ==
            Preserve_trx_phase1_pipeline_result_status::RETRYABLE ||
        result_status ==
            Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE;
    if (!m_pipeline->settle_result(
            result.admission_id,
            Preserve_trx_phase1_pipeline_result_disposition::DROP)) {
      fail("binlog_result_settle_failed");
      return false;
    }
    if (!retryable) {
      if (result_status == Preserve_trx_phase1_pipeline_result_status::DEADLINE) {
        defer_to_final(found, false);
        return !m_failed;
      }
      fail(result.reason.empty() ? "binlog_result_terminal" : result.reason);
      return false;
    }
    return retry_or_refresh_binding(
        found, result_status ==
                   Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE);
  }

  Preserve_trx_phase1_binlog_owner_pump_status pump_completions(
      uint32_t budget) {
    if (m_failed) return Preserve_trx_phase1_binlog_owner_pump_status::FAILED;
    bool progressed = false;
    for (uint32_t count = 0; count < budget; ++count) {
      Preserve_trx_phase1_publication_completion completion;
      if (!m_publication_registry->try_pop_completion(&completion)) break;
      progressed = true;
      auto pending = m_pending_admissions.find(completion.admission_id);
      if (pending == m_pending_admissions.end()) {
        (void)m_pipeline->settle_publication(
            completion.admission_id,
            Preserve_trx_phase1_pipeline_publication_status::FAILED);
        fail("binlog_completion_not_pending");
        return Preserve_trx_phase1_binlog_owner_pump_status::FAILED;
      }
      auto found = m_entries.find(pending->second);
      if (found == m_entries.end() ||
          found->second.state != Entry_state::PUBLICATION_PENDING ||
          found->second.admission_id != completion.admission_id ||
          found->second.descriptor.target_incarnation !=
              completion.target_incarnation ||
          found->second.descriptor.family_version !=
              completion.family_version ||
          completion.family !=
              Preserve_trx_phase1_pipeline_family::BINLOG_CACHE) {
        if (found != m_entries.end()) {
          discard_payload(found->second.payload);
          found->second.payload.reset();
        }
        (void)m_pipeline->settle_publication(
            completion.admission_id,
            Preserve_trx_phase1_pipeline_publication_status::FAILED);
        fail("binlog_completion_identity_mismatch");
        return Preserve_trx_phase1_binlog_owner_pump_status::FAILED;
      }
      Entry &entry = found->second;
      const PrebuiltBinlogCacheBlob *blob =
          preserve_trx_phase1_binlog_adapter_blob(entry.payload);
      if (completion.status != Preserve_trx_transfer_status::OK ||
          blob == nullptr) {
        discard_payload(entry.payload);
        (void)m_pipeline->settle_publication(
            completion.admission_id,
            completion.status == Preserve_trx_transfer_status::ACK_UNCERTAIN
                ? Preserve_trx_phase1_pipeline_publication_status::ACK_UNCERTAIN
                : Preserve_trx_phase1_pipeline_publication_status::FAILED);
        fail(completion.status == Preserve_trx_transfer_status::ACK_UNCERTAIN
                 ? "binlog_publication_ack_uncertain"
                 : "binlog_publication_failed");
        return Preserve_trx_phase1_binlog_owner_pump_status::FAILED;
      }
      if (!m_publisher->remember_acked(entry.descriptor.target_thread_id,
                                       *blob,
                                       !entry.descriptor.binlog_prefix_progress)) {
        discard_payload(entry.payload);
        (void)m_pipeline->settle_publication(
            completion.admission_id,
            Preserve_trx_phase1_pipeline_publication_status::FAILED);
        fail("binlog_publication_provider_handoff_failed");
        return Preserve_trx_phase1_binlog_owner_pump_status::FAILED;
      }
      preserve_trx_phase1_binlog_adapter_transfer(entry.payload);
      if (!m_pipeline->settle_publication(
              completion.admission_id,
              Preserve_trx_phase1_pipeline_publication_status::OK)) {
        fail("binlog_publication_settle_failed");
        return Preserve_trx_phase1_binlog_owner_pump_status::FAILED;
      }
      entry.descriptor.binlog_prefix_size = blob->size;
      entry.descriptor.binlog_prefix_digest = blob->digest;
      entry.descriptor.binlog_prefix_truncate_generation =
          blob->phase1_truncate_generation;
      entry.payload.reset();
      entry.admission_id = 0;
      entry.state = Entry_state::PUBLISHED;
      entry.baseline_resolved = true;
      entry.next_retry_us = monotonic_us() + 50000ULL;
      m_pending_admissions.erase(pending);
      if (entry.descriptor.binlog_prefix_progress)
        ++m_prefix_published;
      else
        ++m_published;
      DEBUG_SYNC(current_thd, "preserve_trx_phase1_binlog_baseline_acked");
      if (!finish_busy(found))
        return Preserve_trx_phase1_binlog_owner_pump_status::FAILED;
    }
    if (baselines_complete())
      return Preserve_trx_phase1_binlog_owner_pump_status::COMPLETE;
    return progressed ? Preserve_trx_phase1_binlog_owner_pump_status::PROGRESS
                      : Preserve_trx_phase1_binlog_owner_pump_status::IDLE;
  }

  Preserve_trx_phase1_binlog_owner_pump_status submit(uint32_t budget) {
    if (m_failed) return Preserve_trx_phase1_binlog_owner_pump_status::FAILED;
    if (m_ordinary_submissions_finished)
      return baselines_complete()
                 ? Preserve_trx_phase1_binlog_owner_pump_status::COMPLETE
                 : Preserve_trx_phase1_binlog_owner_pump_status::IDLE;
    bool progressed = false;
    for (uint32_t count = 0; count < budget; ++count) {
      Entry *entry = next_ready_entry();
      if (entry == nullptr) break;
      const Preserve_trx_phase1_pipeline_submit_status status =
          m_pipeline->try_submit(entry->descriptor);
      if (status == Preserve_trx_phase1_pipeline_submit_status::ADMITTED) {
        entry->state = Entry_state::INFLIGHT;
        ++m_submitted;
        progressed = true;
        continue;
      }
      if (status == Preserve_trx_phase1_pipeline_submit_status::NO_SLOT ||
          status == Preserve_trx_phase1_pipeline_submit_status::NO_CREDIT) {
        break;
      }
      if (status == Preserve_trx_phase1_pipeline_submit_status::DEADLINE) {
        finish_ordinary_submissions();
        break;
      }
      fail("binlog_submit_rejected");
      return Preserve_trx_phase1_binlog_owner_pump_status::FAILED;
    }
    if (baselines_complete())
      return Preserve_trx_phase1_binlog_owner_pump_status::COMPLETE;
    return progressed ? Preserve_trx_phase1_binlog_owner_pump_status::PROGRESS
                      : Preserve_trx_phase1_binlog_owner_pump_status::IDLE;
  }

  void finish_ordinary_submissions() {
    m_ordinary_submissions_finished = true;
    for (auto it = m_entries.begin(); it != m_entries.end(); ++it) {
      if (it->second.state == Entry_state::READY ||
          it->second.state == Entry_state::RETRY_WAIT) {
        defer_to_final(it, false);
      }
    }
  }

  bool captures_complete() const {
    if (m_failed) return false;
    for (const auto &item : m_entries) {
      if (item.second.state == Entry_state::READY ||
          item.second.state == Entry_state::RETRY_WAIT ||
          item.second.state == Entry_state::INFLIGHT) {
        return false;
      }
    }
    return true;
  }

  bool baselines_complete() const {
    if (m_failed || !m_pending_admissions.empty()) return false;
    for (const auto &item : m_entries) {
      if (item.second.state != Entry_state::PUBLISHED &&
          item.second.state != Entry_state::ABSENT &&
          item.second.state != Entry_state::DEFERRED_TO_FINAL) {
        return false;
      }
    }
    return true;
  }

  bool initial_baselines_complete() const {
    if (m_failed) return false;
    for (const auto &item : m_entries)
      if (!item.second.baseline_resolved) return false;
    return true;
  }

  bool flush_publications() {
    if (m_failed) return false;
    const Preserve_trx_transfer_status status = m_publisher->flush();
    if (status != Preserve_trx_transfer_status::OK) {
      fail(status == Preserve_trx_transfer_status::ACK_UNCERTAIN
               ? "binlog_flush_ack_uncertain"
               : "binlog_flush_failed");
      return false;
    }
    return true;
  }

  bool close_publication_tracking() {
    return !m_failed && baselines_complete() && flush_publications() &&
           m_publication_registry->close_tracking_after_flush();
  }

  bool abort_after_sender_join() {
    (void)m_publication_registry->sweep_after_sender_join();
    Preserve_trx_phase1_publication_completion completion;
    while (m_publication_registry->try_pop_completion(&completion)) {
      auto pending = m_pending_admissions.find(completion.admission_id);
      if (pending != m_pending_admissions.end()) {
        auto found = m_entries.find(pending->second);
        if (found != m_entries.end()) {
          discard_payload(found->second.payload);
          found->second.payload.reset();
          found->second.admission_id = 0;
        }
        m_pending_admissions.erase(pending);
      }
      (void)m_pipeline->settle_publication(
          completion.admission_id,
          !completion.callback_completed ||
                  completion.status == Preserve_trx_transfer_status::OK
              ? Preserve_trx_phase1_pipeline_publication_status::ABORTED
              : completion.status ==
                        Preserve_trx_transfer_status::ACK_UNCERTAIN
                    ? Preserve_trx_phase1_pipeline_publication_status::
                          ACK_UNCERTAIN
                    : Preserve_trx_phase1_pipeline_publication_status::FAILED);
    }
    for (auto &item : m_entries) {
      discard_payload(item.second.payload);
      item.second.payload.reset();
      item.second.admission_id = 0;
    }
    return !m_failed;
  }

  void log_event(const char *event) const {
    std::ostringstream message;
    message << "PRESERVE_PHASE1_BINLOG_ADAPTER_V1 event="
            << (event == nullptr ? "UNKNOWN" : event)
            << " targets=" << m_entries.size()
            << " submitted=" << m_submitted << " results=" << m_results
            << " prepared=" << m_published << " absent=" << m_absent
            << " deferred_to_final=" << m_deferred_to_final
            << " identity_churn_deferred=" << m_identity_churn_deferred
            << " retries=" << m_retries
            << " retired=" << m_retired
            << " stale_results=" << m_stale_results
            << " prefix_published=" << m_prefix_published
            << " inflight=" << m_pending_admissions.size()
            << " failed=" << (m_failed ? 1 : 0)
            << " reason=" << (m_failure_reason.empty() ? "NONE"
                                                         : m_failure_reason);
    LogErr(m_failed ? ERROR_LEVEL : INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           message.str().c_str());
  }

 private:
  enum class Entry_state : uint8_t {
    READY,
    INFLIGHT,
    PUBLICATION_PENDING,
    PUBLISHED,
    ABSENT,
    RETRY_WAIT,
    DEFERRED_TO_FINAL
  };

  struct Entry {
    Binlog_binding binding;
    Preserve_trx_phase1_work_descriptor descriptor;
    Preserve_trx_phase1_binlog_prepared_handle payload;
    Entry_state state{Entry_state::READY};
    uint64_t seen_round{0};
    uint64_t next_retry_us{0};
    uint64_t admission_id{0};
    bool replacement_pending{false};
    bool retire_after_busy{false};
    bool baseline_resolved{false};
  };

  static bool busy(const Entry &entry) {
    return entry.state == Entry_state::INFLIGHT ||
           entry.state == Entry_state::PUBLICATION_PENDING;
  }

  bool install_binding(const Binlog_binding &binding) {
    if (m_next_incarnation == 0 || m_next_incarnation == UINT64_MAX) {
      fail("binlog_incarnation_overflow");
      return false;
    }
    Entry entry;
    entry.binding = binding;
    entry.seen_round = m_membership_round;
    entry.descriptor.attempt_id = m_config.attempt_id;
    entry.descriptor.drain_generation = m_config.drain_generation;
    entry.descriptor.target_thread_id = binding.thread_id;
    entry.descriptor.target_incarnation = m_next_incarnation++;
    entry.descriptor.family_version = 1;
    entry.descriptor.source_owner_cookie = binding.owner_cookie;
    entry.descriptor.source_object_cookie = binding.raw_trx_cookie;
    entry.descriptor.warmcopy_epoch = m_config.drain_generation;
    entry.descriptor.expected_immutable_trx_id = binding.immutable_trx_id;
    entry.descriptor.expected_trx_version = binding.trx_version;
    entry.descriptor.capture_generation = 1;
    entry.descriptor.estimated_credit_bytes = m_config.initial_credit_bytes;
    entry.descriptor.binlog_minimum_delta_bytes = m_config.minimum_delta_bytes;
    entry.descriptor.binlog_wire_chunk_bytes = m_config.wire_chunk_bytes;
    entry.descriptor.family =
        Preserve_trx_phase1_pipeline_family::BINLOG_CACHE;
    return m_entries.emplace(binding.thread_id, std::move(entry)).second;
  }

  Entry *next_ready_entry() {
    const uint64_t now_us = monotonic_us();
    /* Alternate baseline and prefix work, with a fair cursor within each. */
    for (unsigned pass = 0; pass != 2; ++pass) {
      const bool progress = pass == 0 ? m_prefer_progress : !m_prefer_progress;
      auto it = m_entries.upper_bound(m_submit_cursor[progress ? 1 : 0]);
      for (size_t visited = 0; visited != m_entries.size(); ++visited, ++it) {
        if (it == m_entries.end()) it = m_entries.begin();
        Entry &entry = it->second;
        if (entry.baseline_resolved != progress ||
            entry.next_retry_us > now_us ||
            (entry.state != Entry_state::READY &&
             entry.state != Entry_state::RETRY_WAIT &&
             entry.state != Entry_state::PUBLISHED)) continue;
        if (entry.state == Entry_state::PUBLISHED) {
          if (entry.descriptor.capture_generation == UINT64_MAX) {
            fail("binlog_capture_generation_overflow");
            return nullptr;
          }
          ++entry.descriptor.capture_generation;
          entry.descriptor.binlog_prefix_progress = true;
          entry.state = Entry_state::RETRY_WAIT;
        }
        m_submit_cursor[progress ? 1 : 0] = it->first;
        m_prefer_progress = !progress;
        return &entry;
      }
    }
    return nullptr;
  }

  void schedule_retry(Entry *entry) {
    if (entry == nullptr ||
        entry->descriptor.capture_generation == UINT64_MAX) {
      fail("binlog_capture_generation_overflow");
      return;
    }
    ++entry->descriptor.capture_generation;
    entry->state = Entry_state::RETRY_WAIT;
    entry->next_retry_us = monotonic_us() +
        (entry->descriptor.binlog_prefix_progress ? 50000ULL : 1000ULL);
    ++m_retries;
  }

  bool finish_busy(std::map<uint64_t, Entry>::iterator found) {
    if (found == m_entries.end() || busy(found->second)) return !m_failed;
    if (found->second.replacement_pending) {
      defer_to_final(found, true);
      return true;
    }
    if (found->second.retire_after_busy) {
      ++m_retired;
      m_entries.erase(found);
    }
    return !m_failed;
  }

  bool retry_or_refresh_binding(std::map<uint64_t, Entry>::iterator found,
                                bool refresh) {
    if (found == m_entries.end()) return true;
    if (found->second.replacement_pending) {
      defer_to_final(found, true);
      return true;
    }
    if (found->second.retire_after_busy) {
      ++m_retired;
      m_entries.erase(found);
      return true;
    }
    if (m_ordinary_submissions_finished) {
      defer_to_final(found, false);
      return !m_failed;
    }
    if (!refresh) {
      schedule_retry(&found->second);
      return !m_failed;
    }
    defer_to_final(found, true);
    return true;
  }

  void defer_to_final(std::map<uint64_t, Entry>::iterator found,
                      bool identity_churn) {
    if (found == m_entries.end() ||
        found->second.state == Entry_state::DEFERRED_TO_FINAL) {
      return;
    }
    discard_payload(found->second.payload);
    found->second.payload.reset();
    found->second.state = Entry_state::DEFERRED_TO_FINAL;
    found->second.baseline_resolved = true;
    found->second.next_retry_us = 0;
    found->second.admission_id = 0;
    found->second.replacement_pending = false;
    found->second.retire_after_busy = false;
    ++m_deferred_to_final;
    if (identity_churn) ++m_identity_churn_deferred;
  }

  void discard_payload(
      const Preserve_trx_phase1_binlog_prepared_handle &payload) {
    if (!preserve_trx_phase1_binlog_adapter_discard(payload)) {
      fail("binlog_artifact_cleanup_failed");
    }
  }

  void fail(const std::string &reason) {
    if (m_failed) return;
    m_failed = true;
    m_failure_reason = reason;
  }

  Preserve_trx_phase1_binlog_owner_config m_config;
  Preserve_trx_phase1_pipeline *m_pipeline{nullptr};
  Preserve_trx_phase1_binlog_provider_port *m_provider{nullptr};
  Preserve_trx_phase1_binlog_publisher_port *m_publisher{nullptr};
  Preserve_trx_phase1_publication_registry *m_publication_registry{nullptr};
  std::map<uint64_t, Entry> m_entries;
  std::map<uint64_t, uint64_t> m_pending_admissions;
  uint64_t m_membership_round{0};
  uint64_t m_next_incarnation{1};
  uint64_t m_submitted{0};
  uint64_t m_results{0};
  uint64_t m_published{0};
  uint64_t m_prefix_published{0};
  std::array<uint64_t, 2> m_submit_cursor{};
  bool m_prefer_progress{false};
  uint64_t m_absent{0};
  uint64_t m_retries{0};
  uint64_t m_deferred_to_final{0};
  uint64_t m_identity_churn_deferred{0};
  uint64_t m_retired{0};
  uint64_t m_stale_results{0};
#ifndef DBUG_OFF
  bool m_debug_identity_stale_injected{false};
#endif
  bool m_ordinary_submissions_finished{false};
  bool m_failed{false};
  std::string m_failure_reason;
};

Preserve_trx_phase1_binlog_owner::Preserve_trx_phase1_binlog_owner(
    const Preserve_trx_phase1_binlog_owner_config &config,
    Preserve_trx_phase1_pipeline *pipeline,
    Preserve_trx_phase1_binlog_provider_port *provider,
    Preserve_trx_phase1_binlog_publisher_port *publisher,
    Preserve_trx_phase1_publication_registry *publication_registry)
    : m_impl(new Impl(config, pipeline, provider, publisher,
                      publication_registry)) {}

Preserve_trx_phase1_binlog_owner::~Preserve_trx_phase1_binlog_owner() =
    default;

bool Preserve_trx_phase1_binlog_owner::reconcile_targets(
    THD *drain_owner, const std::vector<uint64_t> &declared_target_ids) {
  return m_impl->reconcile_targets(drain_owner, declared_target_ids);
}

bool Preserve_trx_phase1_binlog_owner::consume_result(
    const Preserve_trx_phase1_prepared_result &result) {
  return m_impl->consume_result(result);
}

Preserve_trx_phase1_binlog_owner_pump_status
Preserve_trx_phase1_binlog_owner::pump_completions(uint32_t budget) {
  return m_impl->pump_completions(budget);
}

Preserve_trx_phase1_binlog_owner_pump_status
Preserve_trx_phase1_binlog_owner::submit(uint32_t budget) {
  return m_impl->submit(budget);
}

bool Preserve_trx_phase1_binlog_owner::initial_baselines_complete() const {
  return m_impl->initial_baselines_complete();
}

bool Preserve_trx_phase1_binlog_owner::captures_complete() const {
  return m_impl->captures_complete();
}

void Preserve_trx_phase1_binlog_owner::finish_ordinary_submissions() {
  m_impl->finish_ordinary_submissions();
}

bool Preserve_trx_phase1_binlog_owner::baselines_complete() const {
  return m_impl->baselines_complete();
}

bool Preserve_trx_phase1_binlog_owner::flush_publications() {
  return m_impl->flush_publications();
}

bool Preserve_trx_phase1_binlog_owner::close_publication_tracking() {
  return m_impl->close_publication_tracking();
}

bool Preserve_trx_phase1_binlog_owner::abort_after_sender_join() {
  return m_impl->abort_after_sender_join();
}

void Preserve_trx_phase1_binlog_owner::log_event(const char *event) const {
  m_impl->log_event(event);
}
