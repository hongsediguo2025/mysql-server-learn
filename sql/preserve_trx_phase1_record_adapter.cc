/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation. */

#include "sql/preserve_trx_phase1_record_adapter.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <memory>
#include <sstream>
#include <utility>

#include "my_loglevel.h"
#include "mysql/components/services/log_builtins.h"
#include "sql/current_thd.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_lock_warmcopy.h"
#include "sql/sql_class.h"
#include "storage/innobase/include/lock0preserve_capture.h"
#include "storage/innobase/include/trx0preserve.h"

struct Preserve_trx_phase1_record_capture_payload {
  Preserve_trx_phase1_work_descriptor descriptor;
  lock_preserve_phase1_record_snapshot snapshot;
  lock_warmcopy_record_store_compare_token_t store_s1;
  std::string store_payload;
  bool from_record_store{false};
};

struct Preserve_trx_phase1_record_prepared_payload {
  Preserve_trx_phase1_work_descriptor descriptor;
  lock_preserve_phase1_record_snapshot native_fence;
  lock_warmcopy_record_store_compare_token_t store_s1;
  std::shared_ptr<lock_warmcopy_record_store_candidate_t> candidate;
  std::string serialized_payload;
  uint32_t record_lock_count{0};
  uint64_t resolve_credit_bytes{0};
  uint64_t candidate_credit_bytes{0};
};

namespace {

uint64_t monotonic_us() {
  using clock = std::chrono::steady_clock;
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          clock::now().time_since_epoch())
          .count());
}

bool cancelled(const Preserve_trx_phase1_record_adapter_control &control) {
  return control.cancel_probe != nullptr &&
         control.cancel_probe(control.cancel_context);
}

bool deadline_reached(
    const Preserve_trx_phase1_record_adapter_control &control) {
  return control.deadline_us != 0 && monotonic_us() >= control.deadline_us;
}

enum class Exact_pin_status : uint8_t { PINNED, INVALID, STALE };

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
         left.item_limit == right.item_limit &&
         left.capture_byte_limit == right.capture_byte_limit &&
         left.estimated_credit_bytes == right.estimated_credit_bytes &&
         left.expected_store_baseline_generation ==
             right.expected_store_baseline_generation &&
         left.expected_lock_coordinate_generation ==
             right.expected_lock_coordinate_generation &&
         left.family == right.family &&
         left.final_generation == right.final_generation &&
         left.use_record_store_snapshot == right.use_record_store_snapshot;
}

Exact_pin_status pin_exact_target(
    const Preserve_trx_phase1_work_descriptor &descriptor,
    Preserve_trx_external_thd_pin_handle *pin) {
  if (pin == nullptr) return Exact_pin_status::INVALID;
  *pin = {};
  if (descriptor.target_thread_id == 0 ||
      descriptor.target_thread_id >
          std::numeric_limits<my_thread_id>::max() ||
      descriptor.target_incarnation == 0 ||
      descriptor.source_owner_cookie == 0 ||
      descriptor.source_object_cookie == 0) {
    return Exact_pin_status::INVALID;
  }

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
  if (!*pin || pin->thd() != target) return Exact_pin_status::STALE;
  return Exact_pin_status::PINNED;
}

Preserve_trx_phase1_pipeline_result_status map_capture_status(
    lock_preserve_phase1_record_capture_status status) {
  switch (status) {
    case lock_preserve_phase1_record_capture_status::OK:
      return Preserve_trx_phase1_pipeline_result_status::PREPARED;
    case lock_preserve_phase1_record_capture_status::RETRYABLE_LOCK_SYS_BUSY:
    case lock_preserve_phase1_record_capture_status::
        RETRYABLE_LOCK_SET_CHANGED:
      return Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
    case lock_preserve_phase1_record_capture_status::
        RETRYABLE_STALE_IDENTITY:
      return Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE;
    case lock_preserve_phase1_record_capture_status::WAITING_REQUEST:
      return Preserve_trx_phase1_pipeline_result_status::DEFERRED_TO_FINAL;
    case lock_preserve_phase1_record_capture_status::RESOURCE_EXHAUSTED:
      return Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
    case lock_preserve_phase1_record_capture_status::UNSUPPORTED:
    case lock_preserve_phase1_record_capture_status::INVALID_ARGUMENT:
      return Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
  }
  return Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
}

Preserve_trx_phase1_pipeline_result_status map_resolve_status(
    lock_preserve_phase1_record_resolve_status status) {
  switch (status) {
    case lock_preserve_phase1_record_resolve_status::OK:
      return Preserve_trx_phase1_pipeline_result_status::PREPARED;
    case lock_preserve_phase1_record_resolve_status::CANCELLED:
      return Preserve_trx_phase1_pipeline_result_status::CANCELLED;
    case lock_preserve_phase1_record_resolve_status::DEADLINE:
      return Preserve_trx_phase1_pipeline_result_status::DEADLINE;
    case lock_preserve_phase1_record_resolve_status::RESOURCE_EXHAUSTED:
      return Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
    case lock_preserve_phase1_record_resolve_status::PAGE_RETRYABLE:
    case lock_preserve_phase1_record_resolve_status::MDL_RETRYABLE:
    case lock_preserve_phase1_record_resolve_status::IDENTITY_RETRYABLE:
    case lock_preserve_phase1_record_resolve_status::NOT_FOUND:
      return Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
    case lock_preserve_phase1_record_resolve_status::INVALID_ARGUMENT:
    case lock_preserve_phase1_record_resolve_status::UNSUPPORTED:
      return Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
  }
  return Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
}

const char *last_record_reason(const char *fallback) {
  const char *reason = trx_preserve_last_record_lock_export_error();
  return reason == nullptr ? fallback : reason;
}

bool checked_add_credit(uint64_t bytes, uint64_t *total) {
  if (total == nullptr || bytes > UINT64_MAX - *total) return false;
  *total += bytes;
  return true;
}

bool reserve_credit(
    const Preserve_trx_phase1_record_adapter_control &control,
    uint64_t required, const char *reason,
    Preserve_trx_phase1_record_adapter_outcome *outcome) {
  if (control.reserve_credit == nullptr) return true;
  const Preserve_trx_phase1_pipeline_credit_status status =
      control.reserve_credit(control.credit_context, required);
  if (status == Preserve_trx_phase1_pipeline_credit_status::GRANTED) {
    return true;
  }
  outcome->status =
      status == Preserve_trx_phase1_pipeline_credit_status::DEADLINE
          ? Preserve_trx_phase1_pipeline_result_status::DEADLINE
          : status == Preserve_trx_phase1_pipeline_credit_status::CANCELLED
                ? Preserve_trx_phase1_pipeline_result_status::CANCELLED
                : Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
  outcome->reason = reason;
  return false;
}

void copy_native_fence(
    const lock_preserve_phase1_record_snapshot &source,
    lock_preserve_phase1_record_snapshot *destination) {
  *destination = {};
  destination->immutable_trx_id = source.immutable_trx_id;
  destination->trx_version = source.trx_version;
  destination->raw_trx_cookie = source.raw_trx_cookie;
  destination->owner_thd_cookie = source.owner_thd_cookie;
  destination->trx_locks_version = source.trx_locks_version;
  destination->native_record_lock_count = source.native_record_lock_count;
  destination->coordinate_generation = source.coordinate_generation;
  destination->freeze_generation = source.freeze_generation;
  destination->conversion_attempt_after_freeze =
      source.conversion_attempt_after_freeze;
  destination->conversion_unhandled_after_freeze =
      source.conversion_unhandled_after_freeze;
}

lock_warmcopy_trx_lock_fence_t live_fence_from_snapshot(
    const lock_preserve_phase1_record_snapshot &snapshot) {
  lock_warmcopy_trx_lock_fence_t fence;
  fence.trx_locks_version = snapshot.trx_locks_version;
  fence.n_rec_locks = snapshot.native_record_lock_count;
  fence.coordinate_generation = snapshot.coordinate_generation;
  fence.freeze_generation = snapshot.freeze_generation;
  fence.conversion_attempt_after_freeze =
      snapshot.conversion_attempt_after_freeze;
  fence.conversion_unhandled_after_freeze =
      snapshot.conversion_unhandled_after_freeze;
  return fence;
}

void initialize_store_snapshot_fence(
    const Preserve_trx_phase1_work_descriptor &descriptor,
    const lock_warmcopy_trx_lock_fence_t &fence, uint64_t captured_bytes,
    uint32_t record_lock_count,
    lock_preserve_phase1_record_snapshot *snapshot) {
  *snapshot = {};
  snapshot->immutable_trx_id = descriptor.expected_immutable_trx_id;
  snapshot->trx_version = descriptor.expected_trx_version;
  snapshot->raw_trx_cookie = descriptor.source_object_cookie;
  snapshot->owner_thd_cookie = descriptor.source_owner_cookie;
  snapshot->trx_locks_version = fence.trx_locks_version;
  snapshot->native_record_lock_count = fence.n_rec_locks;
  snapshot->coordinate_generation = fence.coordinate_generation;
  snapshot->freeze_generation = fence.freeze_generation;
  snapshot->conversion_attempt_after_freeze =
      fence.conversion_attempt_after_freeze;
  snapshot->conversion_unhandled_after_freeze =
      fence.conversion_unhandled_after_freeze;
  snapshot->captured_bytes = captured_bytes;
  snapshot->exported_record_bits = record_lock_count;
}

Preserve_trx_phase1_pipeline_credit_status debug_reserve_credit(
    void *, uint64_t) {
  return Preserve_trx_phase1_pipeline_credit_status::GRANTED;
}

}  // namespace

void preserve_trx_phase1_record_adapter_capture(
    const Preserve_trx_phase1_work_descriptor &descriptor,
    const Preserve_trx_phase1_record_adapter_control &control,
    Preserve_trx_phase1_record_adapter_outcome *outcome) {
  if (outcome == nullptr) return;
  *outcome = {};
  if (descriptor.family != Preserve_trx_phase1_pipeline_family::RECORD_LOCK ||
      descriptor.warmcopy_epoch == 0 ||
      descriptor.expected_immutable_trx_id == 0 ||
      descriptor.expected_trx_version == 0 ||
      descriptor.capture_generation == 0 ||
      descriptor.item_limit == 0 || descriptor.item_limit > UINT32_MAX ||
      descriptor.capture_byte_limit == 0 ||
      descriptor.estimated_credit_bytes == 0 ||
      descriptor.capture_byte_limit > descriptor.estimated_credit_bytes ||
      (descriptor.use_record_store_snapshot &&
       (!descriptor.final_generation ||
        descriptor.expected_store_baseline_generation == 0))) {
    outcome->status = Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
    outcome->reason = "record_descriptor_invalid";
    return;
  }
  if (cancelled(control)) {
    outcome->status = Preserve_trx_phase1_pipeline_result_status::CANCELLED;
    outcome->reason = "record_capture_cancelled";
    return;
  }
  if (deadline_reached(control)) {
    outcome->status = Preserve_trx_phase1_pipeline_result_status::DEADLINE;
    outcome->reason = "record_capture_deadline";
    return;
  }

  Preserve_trx_external_thd_pin_handle pin;
  const Exact_pin_status pin_status = pin_exact_target(descriptor, &pin);
  if (pin_status != Exact_pin_status::PINNED) {
    outcome->status = pin_status == Exact_pin_status::INVALID
                          ? Preserve_trx_phase1_pipeline_result_status::
                                UNSUPPORTED
                          : Preserve_trx_phase1_pipeline_result_status::
                                IDENTITY_STALE;
    outcome->reason = "record_capture_target_stale";
    return;
  }

  if (descriptor.use_record_store_snapshot) {
    trx_preserve_phase1_identity identity_before;
    lock_warmcopy_trx_lock_fence_t live_fence_before;
    if (!trx_preserve_phase1_owner_identity_snapshot(pin.thd(),
                                                     &identity_before) ||
        identity_before.owner_thd_cookie != descriptor.source_owner_cookie ||
        identity_before.raw_cookie != descriptor.source_object_cookie ||
        identity_before.immutable_trx_id !=
            descriptor.expected_immutable_trx_id ||
        identity_before.trx_version != descriptor.expected_trx_version) {
      outcome->status =
          Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE;
      outcome->reason = "record_store_snapshot_identity_changed";
      return;
    }
    if (!trx_preserve_sample_lock_warmcopy_fence(pin.thd(),
                                                 &live_fence_before)) {
      outcome->status =
          Preserve_trx_phase1_pipeline_result_status::STORE_FALLBACK;
      outcome->reason = "record_store_snapshot_live_fence_unavailable";
      return;
    }
    if (live_fence_before.coordinate_generation !=
            descriptor.expected_lock_coordinate_generation ||
        live_fence_before.conversion_attempt_after_freeze ||
        live_fence_before.conversion_unhandled_after_freeze) {
      outcome->status =
          Preserve_trx_phase1_pipeline_result_status::STORE_FALLBACK;
      outcome->reason = "record_store_snapshot_coordinate_changed";
      return;
    }

    std::string store_payload;
    uint32_t record_lock_count = 0;
    lock_warmcopy_record_store_compare_token_t store_s1;
    const lock_warmcopy_record_store_snapshot_status_t snapshot_status =
        lock_warmcopy_record_store_snapshot_candidate_for_target(
            descriptor.target_thread_id, descriptor.warmcopy_epoch,
            descriptor.expected_store_baseline_generation,
            static_cast<uint32_t>(descriptor.item_limit),
            descriptor.capture_byte_limit, &store_payload, &record_lock_count,
            &store_s1);
    if (snapshot_status !=
        lock_warmcopy_record_store_snapshot_status_t::CAPTURED) {
      outcome->status =
          snapshot_status == lock_warmcopy_record_store_snapshot_status_t::
                                 RESOURCE_EXHAUSTED
              ? Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED
              : Preserve_trx_phase1_pipeline_result_status::STORE_FALLBACK;
      outcome->reason = "record_store_snapshot_unavailable";
      return;
    }

    trx_preserve_phase1_identity identity_after;
    lock_warmcopy_trx_lock_fence_t live_fence_after;
    if (!trx_preserve_sample_lock_warmcopy_fence(pin.thd(),
                                                 &live_fence_after) ||
        !trx_preserve_phase1_owner_identity_snapshot(pin.thd(),
                                                     &identity_after) ||
        identity_after.owner_thd_cookie != identity_before.owner_thd_cookie ||
        identity_after.raw_cookie != identity_before.raw_cookie ||
        identity_after.immutable_trx_id != identity_before.immutable_trx_id ||
        identity_after.trx_version != identity_before.trx_version) {
      outcome->status =
          Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE;
      outcome->reason = "record_store_snapshot_identity_changed";
      return;
    }
    if (!lock_warmcopy_trx_lock_fence_equal(live_fence_before,
                                            live_fence_after)) {
      outcome->status =
          Preserve_trx_phase1_pipeline_result_status::STORE_FALLBACK;
      outcome->reason = "record_store_snapshot_live_fence_changed";
      return;
    }
    if ((live_fence_after.n_rec_locks == 0) != (record_lock_count == 0)) {
      outcome->status =
          Preserve_trx_phase1_pipeline_result_status::STORE_FALLBACK;
      outcome->reason = "record_store_snapshot_presence_mismatch";
      return;
    }
    try {
      auto captured =
          std::make_shared<Preserve_trx_phase1_record_capture_payload>();
      captured->descriptor = descriptor;
      initialize_store_snapshot_fence(
          descriptor, live_fence_after,
          static_cast<uint64_t>(store_payload.size()), record_lock_count,
          &captured->snapshot);
      captured->store_s1 = store_s1;
      captured->store_payload = std::move(store_payload);
      captured->from_record_store = true;
      outcome->logical_bytes = captured->snapshot.captured_bytes;
      outcome->required_credit_bytes = captured->snapshot.captured_bytes;
      outcome->capture_payload = std::move(captured);
      outcome->status = Preserve_trx_phase1_pipeline_result_status::PREPARED;
    } catch (...) {
      outcome->status =
          Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
      outcome->reason = "record_store_snapshot_result_allocation_failed";
    }
    return;
  }

  lock_warmcopy_record_store_compare_token_t store_s0;
  if (!lock_warmcopy_record_store_compare_token_for_target(
          descriptor.target_thread_id, &store_s0) ||
      store_s0.epoch != descriptor.warmcopy_epoch) {
    outcome->status = Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
    outcome->reason = "record_capture_epoch_closed";
    return;
  }

  lock_preserve_phase1_record_snapshot snapshot;
  const lock_preserve_phase1_record_capture_status capture_status =
      trx_preserve_phase1_capture_record_lock_values(
          pin.thd(), static_cast<uint32_t>(descriptor.item_limit),
          descriptor.capture_byte_limit, &snapshot);
  outcome->global_latch_wait_us = snapshot.global_latch_wait_us;
  outcome->global_latch_hold_us = snapshot.global_latch_hold_us;
  outcome->global_latch_envelope_us = snapshot.global_latch_envelope_us;
  if (capture_status != lock_preserve_phase1_record_capture_status::OK) {
    outcome->status = map_capture_status(capture_status);
    outcome->reason = last_record_reason("record_capture_failed");
    return;
  }
  if (snapshot.owner_thd_cookie != descriptor.source_owner_cookie ||
      snapshot.raw_trx_cookie != descriptor.source_object_cookie ||
      snapshot.immutable_trx_id != descriptor.expected_immutable_trx_id ||
      snapshot.trx_version != descriptor.expected_trx_version) {
    outcome->status =
        Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE;
    outcome->reason = "record_capture_identity_changed";
    return;
  }
  if ((snapshot.native_record_lock_count == 0) !=
      (snapshot.exported_record_bits == 0)) {
    outcome->status = Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
    outcome->reason = "record_capture_presence_mismatch";
    return;
  }

  lock_warmcopy_record_store_compare_token_t store_s1;
  if (!lock_warmcopy_record_store_compare_token_for_target(
          descriptor.target_thread_id, &store_s1) ||
      store_s1.epoch != descriptor.warmcopy_epoch ||
      !lock_warmcopy_record_store_compare_token_equal(store_s0, store_s1)) {
    outcome->status = Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
    outcome->reason = "record_store_changed_during_capture";
    return;
  }

  try {
    auto captured =
        std::make_shared<Preserve_trx_phase1_record_capture_payload>();
    captured->descriptor = descriptor;
    captured->snapshot = std::move(snapshot);
    captured->store_s1 = store_s1;
    outcome->logical_bytes = captured->snapshot.captured_bytes;
    outcome->required_credit_bytes = captured->snapshot.captured_bytes;
    outcome->capture_payload = std::move(captured);
    outcome->status = Preserve_trx_phase1_pipeline_result_status::PREPARED;
  } catch (...) {
    outcome->status =
        Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
    outcome->reason = "record_capture_result_allocation_failed";
  }
}

void preserve_trx_phase1_record_adapter_prepare(
    THD *worker_thd, const Preserve_trx_phase1_work_descriptor &descriptor,
    const Preserve_trx_phase1_record_capture_handle &capture_payload,
    const Preserve_trx_phase1_record_adapter_control &control,
    Preserve_trx_phase1_record_adapter_outcome *outcome) {
  if (outcome == nullptr) return;
  *outcome = {};
  const auto &capture = capture_payload;
  if (worker_thd == nullptr || worker_thd != current_thd ||
      capture == nullptr ||
      !descriptors_equal(capture->descriptor, descriptor) ||
      capture->from_record_store != descriptor.use_record_store_snapshot) {
    outcome->status = Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
    outcome->reason = "record_prepare_descriptor_mismatch";
    return;
  }

  std::string serialized_payload;
  uint64_t resolved_snapshot_logical_bytes = 0;
  uint64_t resolve_required = 0;
  if (capture->from_record_store) {
    serialized_payload = std::move(capture->store_payload);
    resolve_required = static_cast<uint64_t>(serialized_payload.size());
  } else {
    lock_preserve_phase1_record_resolve_control resolve_control;
    resolve_control.deadline_us = control.deadline_us;
    resolve_control.cancel_probe = control.cancel_probe;
    resolve_control.cancel_context = control.cancel_context;
    resolve_control.stable_page_only = true;

    lock_preserve_phase1_record_resolve_plan resolve_plan;
    const lock_preserve_phase1_record_resolve_status resolve_plan_status =
        lock_preserve_phase1_plan_record_values(
            capture->snapshot, resolve_control.stable_page_only, &resolve_plan);
    if (resolve_plan_status != lock_preserve_phase1_record_resolve_status::OK) {
      outcome->status = map_resolve_status(resolve_plan_status);
      outcome->reason = "record_resolve_plan_failed";
      return;
    }
    resolved_snapshot_logical_bytes =
        resolve_plan.resolved_snapshot_logical_bytes;
    resolve_required = resolved_snapshot_logical_bytes;
    if (!checked_add_credit(resolve_plan.serialized_payload_bytes,
                            &resolve_required) ||
        !checked_add_credit(resolve_plan.scratch_logical_bytes,
                            &resolve_required)) {
      outcome->status =
          Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
      outcome->reason = "record_resolve_credit_overflow";
      return;
    }
    if (!reserve_credit(control, resolve_required,
                        "record_resolve_credit_unavailable", outcome)) {
      return;
    }

    lock_preserve_phase1_record_resolve_metrics metrics;
    const lock_preserve_phase1_record_resolve_status resolve_status =
        trx_preserve_phase1_resolve_record_lock_values(
            worker_thd, &capture->snapshot, resolve_control,
            &serialized_payload, &metrics);
    if (resolve_status != lock_preserve_phase1_record_resolve_status::OK) {
      outcome->status = map_resolve_status(resolve_status);
      outcome->reason = last_record_reason("record_resolve_failed");
      return;
    }
    if (serialized_payload.size() !=
        resolve_plan.serialized_payload_bytes) {
      outcome->status =
          Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
      outcome->reason = "record_resolve_plan_mismatch";
      return;
    }
  }

  lock_warmcopy_record_candidate_plan_t candidate_plan;
  const lock_warmcopy_record_candidate_status_t candidate_plan_status =
      lock_warmcopy_record_store_plan_candidate(serialized_payload,
                                                 &candidate_plan);
  if (candidate_plan_status !=
      lock_warmcopy_record_candidate_status_t::PREPARED) {
    outcome->status =
        candidate_plan_status ==
                lock_warmcopy_record_candidate_status_t::RESOURCE_EXHAUSTED
            ? Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED
            : Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
    outcome->reason = "record_candidate_plan_failed";
    return;
  }
  uint64_t candidate_required = resolved_snapshot_logical_bytes;
  if (!checked_add_credit(candidate_plan.payload_bytes,
                          &candidate_required) ||
      !checked_add_credit(candidate_plan.retained_logical_bytes,
                          &candidate_required) ||
      !checked_add_credit(candidate_plan.scratch_logical_bytes,
                          &candidate_required)) {
    outcome->status =
        Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
    outcome->reason = "record_candidate_credit_overflow";
    return;
  }
  if (!reserve_credit(control, candidate_required,
                      "record_candidate_credit_unavailable", outcome)) {
    return;
  }

  std::shared_ptr<lock_warmcopy_record_store_candidate_t> candidate;
  uint32_t record_lock_count = 0;
  const lock_warmcopy_record_candidate_status_t candidate_status =
      lock_warmcopy_record_store_prepare_candidate(
          serialized_payload, &candidate, &record_lock_count);
  if (candidate_status !=
      lock_warmcopy_record_candidate_status_t::PREPARED) {
    outcome->status =
        candidate_status ==
                lock_warmcopy_record_candidate_status_t::RESOURCE_EXHAUSTED
            ? Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED
            : Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
    outcome->reason = "record_candidate_prepare_failed";
    return;
  }
  if (record_lock_count != candidate_plan.record_lock_count) {
    outcome->status =
        Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
    outcome->reason = "record_candidate_plan_mismatch";
    return;
  }
  if (capture->from_record_store &&
      record_lock_count != capture->snapshot.exported_record_bits) {
    outcome->status =
        Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
    outcome->reason = "record_store_snapshot_count_mismatch";
    return;
  }

  try {
    auto prepared =
        std::make_shared<Preserve_trx_phase1_record_prepared_payload>();
    prepared->descriptor = descriptor;
    copy_native_fence(capture->snapshot, &prepared->native_fence);
    prepared->store_s1 = capture->store_s1;
    prepared->candidate = std::move(candidate);
    prepared->serialized_payload = std::move(serialized_payload);
    prepared->record_lock_count = record_lock_count;
    prepared->resolve_credit_bytes = resolve_required;
    prepared->candidate_credit_bytes = candidate_required;
    outcome->logical_bytes = prepared->serialized_payload.size();
    outcome->required_credit_bytes = candidate_required;
    outcome->prepared_payload = std::move(prepared);
    outcome->status = record_lock_count == 0
                          ? Preserve_trx_phase1_pipeline_result_status::ABSENT
                          : Preserve_trx_phase1_pipeline_result_status::PREPARED;
  } catch (...) {
    outcome->status =
        Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
    outcome->reason = "record_prepare_result_allocation_failed";
  }
}

void preserve_trx_phase1_record_adapter_owner_install(
    const Preserve_trx_phase1_work_descriptor &descriptor,
    const Preserve_trx_phase1_record_prepared_handle &prepared_payload,
    Preserve_trx_phase1_record_adapter_install_result *result) {
  if (result == nullptr) return;
  *result = {};
  const auto &prepared = prepared_payload;
  if (prepared == nullptr ||
      !descriptors_equal(prepared->descriptor, descriptor)) {
    result->status = Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
    result->reason = "record_install_descriptor_mismatch";
    return;
  }

  Preserve_trx_external_thd_pin_handle pin;
  const Exact_pin_status pin_status = pin_exact_target(descriptor, &pin);
  if (pin_status != Exact_pin_status::PINNED) {
    result->status = pin_status == Exact_pin_status::INVALID
                         ? Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED
                         : Preserve_trx_phase1_pipeline_result_status::
                               IDENTITY_STALE;
    result->reason = "record_install_target_stale";
    return;
  }
  const lock_preserve_phase1_record_identity_status identity_status =
      trx_preserve_phase1_validate_record_lock_snapshot(
          pin.thd(), prepared->native_fence);
  if (identity_status != lock_preserve_phase1_record_identity_status::MATCH) {
    result->status =
        descriptor.use_record_store_snapshot &&
                identity_status == lock_preserve_phase1_record_identity_status::
                                       LOCK_FENCE_CHANGED
            ? Preserve_trx_phase1_pipeline_result_status::STORE_FALLBACK
            : Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE;
    result->reason = "record_install_native_fence_changed";
    return;
  }
  pin = {};

  lock_warmcopy_record_install_result_t installed;
  if (!lock_warmcopy_record_store_compare_and_install_candidate(
          descriptor.target_thread_id, prepared->store_s1,
          prepared->candidate, &installed)) {
    result->status =
        Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
    result->reason = "record_install_failed";
    return;
  }
  if (installed.status != lock_warmcopy_record_install_status_t::INSTALLED) {
    result->status =
        installed.status ==
                lock_warmcopy_record_install_status_t::COMPARE_MISMATCH
            ? Preserve_trx_phase1_pipeline_result_status::RETRYABLE
            : Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
    result->reason = "record_install_compare_rejected";
    return;
  }
  if (installed.publication_token == 0 ||
      installed.record_lock_count != prepared->record_lock_count ||
      installed.installed_token.epoch != descriptor.warmcopy_epoch ||
      installed.installed_token.target_id != descriptor.target_thread_id ||
      installed.installed_token.baseline_generation !=
          installed.publication_token ||
      !installed.installed_token.store_present ||
      installed.installed_token.target_invalid ||
      installed.installed_token.journal_sequence_present ||
      installed.installed_token.expected_delta_sequence_present ||
      installed.installed_token.journal_bytes_present) {
    result->status =
        Preserve_trx_phase1_pipeline_result_status::UNSUPPORTED;
    result->reason = "record_install_result_invariant";
    return;
  }

  result->status = installed.record_lock_count == 0
                       ? Preserve_trx_phase1_pipeline_result_status::ABSENT
                       : Preserve_trx_phase1_pipeline_result_status::PREPARED;
  result->publication_token = installed.publication_token;
  result->record_lock_count = installed.record_lock_count;
  result->installed_token = installed.installed_token;
  result->captured_live_fence =
      live_fence_from_snapshot(prepared->native_fence);
}

void preserve_trx_phase1_record_adapter_owner_publish(
    const Preserve_trx_phase1_work_descriptor &descriptor,
    const Preserve_trx_phase1_record_prepared_handle &prepared_payload,
    Preserve_trx_lock_warmcopy_drain_participant *participant,
    bool active_scan,
    Preserve_trx_phase1_record_adapter_install_result *result) {
  preserve_trx_phase1_record_adapter_owner_install(descriptor,
                                                    prepared_payload, result);
  if (result == nullptr ||
      (result->status != Preserve_trx_phase1_pipeline_result_status::PREPARED &&
       result->status != Preserve_trx_phase1_pipeline_result_status::ABSENT)) {
    return;
  }
  const auto &prepared = prepared_payload;
  PrebuiltRecordLocksBlob blob;
  if (participant == nullptr || prepared == nullptr ||
      !participant->adopt_installed_phase1_record_candidate(
          descriptor.target_thread_id, descriptor.target_incarnation,
          descriptor.capture_generation, result->publication_token,
          result->installed_token, result->captured_live_fence,
          prepared->serialized_payload, result->record_lock_count, active_scan,
          &blob)) {
    *result = {};
    result->status = Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
    result->reason = "record_publish_participant_adopt_failed";
  }
}

#ifndef DBUG_OFF
bool preserve_trx_phase1_record_adapter_debug_exercise(THD *worker_thd,
                                                        THD *target_thd,
                                                        bool expect_empty,
    Preserve_trx_lock_warmcopy_drain_participant *participant) {
  if (worker_thd == nullptr || worker_thd != current_thd ||
      target_thd == nullptr) {
    return false;
  }

  Preserve_trx_phase1_work_descriptor descriptor;
  Preserve_trx_external_thd_pin_handle target_pin;
  mysql_mutex_lock(&target_thd->LOCK_thd_data);
  if (target_thd->release_resources_done()) {
    mysql_mutex_unlock(&target_thd->LOCK_thd_data);
    return false;
  }
  descriptor.target_thread_id = target_thd->thread_id();
  descriptor.source_owner_cookie =
      static_cast<uint64_t>(reinterpret_cast<uintptr_t>(target_thd));
  descriptor.source_object_cookie =
      trx_preserve_phase1_peek_raw_cookie(target_thd);
  target_pin = preserve_trx_acquire_external_thd_pin_locked(target_thd);
  mysql_mutex_unlock(&target_thd->LOCK_thd_data);

  trx_preserve_phase1_identity identity;
  if (!target_pin || target_pin.thd() != target_thd ||
      !trx_preserve_phase1_owner_identity_snapshot(target_pin.thd(),
                                                   &identity) ||
      identity.owner_thd_cookie != descriptor.source_owner_cookie ||
      identity.raw_cookie != descriptor.source_object_cookie) {
    return false;
  }
  descriptor.attempt_id = 1;
  descriptor.drain_generation = 1;
  descriptor.target_incarnation = descriptor.source_owner_cookie;
  descriptor.family_version = 1;
  descriptor.expected_immutable_trx_id = identity.immutable_trx_id;
  descriptor.expected_trx_version = identity.trx_version;
  descriptor.capture_generation = 1;
  descriptor.item_limit = UINT32_MAX;
  descriptor.capture_byte_limit = UINT64_MAX / 2;
  descriptor.estimated_credit_bytes = UINT64_MAX / 2;
  descriptor.family = Preserve_trx_phase1_pipeline_family::RECORD_LOCK;
  if (descriptor.target_thread_id == 0 ||
      descriptor.source_object_cookie == 0) {
    return false;
  }

  struct Clear_store {
    explicit Clear_store(uint64_t id) : target_id(id) {}
    ~Clear_store() {
      lock_warmcopy_record_store_clear_for_target(target_id);
    }
    uint64_t target_id;
  } clear_store(descriptor.target_thread_id);
  lock_warmcopy_record_store_clear_for_target(descriptor.target_thread_id);

  lock_warmcopy_record_store_compare_token_t empty_store;
  if (!lock_warmcopy_record_store_compare_token_for_target(
          descriptor.target_thread_id, &empty_store) ||
      empty_store.epoch == 0) {
    return false;
  }
  descriptor.warmcopy_epoch = empty_store.epoch;
  target_pin = {};

  Preserve_trx_phase1_record_adapter_control control;
  control.deadline_us = monotonic_us() + 5000000ULL;
  Preserve_trx_phase1_record_adapter_outcome captured;
  preserve_trx_phase1_record_adapter_capture(descriptor, control, &captured);
  if (captured.status != Preserve_trx_phase1_pipeline_result_status::PREPARED ||
      captured.capture_payload == nullptr) {
    return false;
  }

  control.reserve_credit = debug_reserve_credit;
  Preserve_trx_phase1_record_adapter_outcome prepared;
  preserve_trx_phase1_record_adapter_prepare(
      worker_thd, descriptor, captured.capture_payload, control, &prepared);
  const Preserve_trx_phase1_pipeline_result_status expected_status =
      expect_empty ? Preserve_trx_phase1_pipeline_result_status::ABSENT
                   : Preserve_trx_phase1_pipeline_result_status::PREPARED;
  if (prepared.status != expected_status ||
      prepared.prepared_payload == nullptr) {
    return false;
  }
  LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
         (std::string("PRESERVE_PHASE1_RECORD_ADAPTER_V1 ") +
          "event=CREDIT_PLAN_EXERCISE_PASSED resolve_credit=" +
          std::to_string(prepared.prepared_payload->resolve_credit_bytes) +
          " candidate_credit=" +
          std::to_string(prepared.prepared_payload->candidate_credit_bytes))
             .c_str());

  Preserve_trx_phase1_record_adapter_install_result installed;
  if (!expect_empty) {
    const auto &prepared_record = prepared.prepared_payload;
    std::shared_ptr<lock_warmcopy_record_store_candidate_t> intervening;
    lock_warmcopy_record_install_result_t intervening_install;
    if (prepared_record == nullptr ||
        lock_warmcopy_record_store_prepare_candidate(
            prepared_record->serialized_payload, &intervening, nullptr) !=
            lock_warmcopy_record_candidate_status_t::PREPARED ||
        !lock_warmcopy_record_store_compare_and_install_candidate(
            descriptor.target_thread_id, prepared_record->store_s1,
            intervening, &intervening_install) ||
        intervening_install.status !=
            lock_warmcopy_record_install_status_t::INSTALLED) {
      return false;
    }
    preserve_trx_phase1_record_adapter_owner_install(
        descriptor, prepared.prepared_payload, &installed);
    if (installed.status !=
        Preserve_trx_phase1_pipeline_result_status::RETRYABLE) {
      return false;
    }
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           "PRESERVE_PHASE1_RECORD_ADAPTER_V1 "
           "event=STALE_STORE_REJECTED");

    ++descriptor.capture_generation;
    control.reserve_credit = nullptr;
    preserve_trx_phase1_record_adapter_capture(descriptor, control,
                                                &captured);
    if (captured.status !=
            Preserve_trx_phase1_pipeline_result_status::PREPARED ||
        captured.capture_payload == nullptr) {
      return false;
    }
    control.reserve_credit = debug_reserve_credit;
    preserve_trx_phase1_record_adapter_prepare(
        worker_thd, descriptor, captured.capture_payload, control, &prepared);
    if (prepared.status != expected_status ||
        prepared.prepared_payload == nullptr) {
      return false;
    }
  }

  installed = {};
  preserve_trx_phase1_record_adapter_owner_publish(
      descriptor, prepared.prepared_payload, participant, false, &installed);
  if (installed.status != expected_status || installed.publication_token == 0 ||
      !installed.installed_token.store_present ||
      (expect_empty ? installed.record_lock_count != 0
                    : installed.record_lock_count == 0)) {
    return false;
  }
  if (!expect_empty) {
    PrebuiltRecordLocksBlob blob;
    if (participant == nullptr ||
        !participant->phase1_record_prebuilt_blob_for_thread(
            descriptor.target_thread_id, &blob) ||
        blob.warmcopy_epoch != descriptor.warmcopy_epoch || blob.size == 0 ||
        blob.warmcopy_id.find("_i" +
                              std::to_string(descriptor.target_incarnation) +
                              "_g" +
                              std::to_string(descriptor.capture_generation)) ==
            std::string::npos) {
      return false;
    }
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           (std::string("PRESERVE_PHASE1_RECORD_ADAPTER_V1 ") +
            "event=PARTICIPANT_ADOPT_EXERCISE_PASSED record_locks=" +
            std::to_string(installed.record_lock_count))
               .c_str());
  }

  std::ostringstream message;
  message << "PRESERVE_PHASE1_RECORD_ADAPTER_V1"
          << " event="
          << (expect_empty ? "EMPTY_EXERCISE_PASSED" : "EXERCISE_PASSED")
          << " record_locks=" << installed.record_lock_count
          << " publication_token=" << installed.publication_token;
  LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.str().c_str());
  return true;
}
#endif  // DBUG_OFF
