/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. */

#ifndef lock0preserve_capture_h
#define lock0preserve_capture_h

#include <cstdint>
#include <string>
#include <vector>

struct trx_t;
class THD;

enum class lock_preserve_phase1_record_capture_status : uint8_t {
  OK = 0,
  INVALID_ARGUMENT,
  RETRYABLE_LOCK_SYS_BUSY,
  RETRYABLE_LOCK_SET_CHANGED,
  RETRYABLE_STALE_IDENTITY,
  WAITING_REQUEST,
  UNSUPPORTED,
  RESOURCE_EXHAUSTED
};

enum class lock_preserve_phase1_record_resolve_status : uint8_t {
  OK = 0,
  INVALID_ARGUMENT,
  PAGE_RETRYABLE,
  MDL_RETRYABLE,
  IDENTITY_RETRYABLE,
  NOT_FOUND,
  CANCELLED,
  DEADLINE,
  UNSUPPORTED,
  RESOURCE_EXHAUSTED
};

enum class lock_preserve_phase1_record_identity_status : uint8_t {
  MATCH = 0,
  INVALID_ARGUMENT,
  TRANSACTION_STALE,
  LOCK_FENCE_CHANGED
};

using lock_preserve_phase1_cancel_probe = bool (*)(void *context);

struct lock_preserve_phase1_record_resolve_control {
  uint64_t deadline_us{0};
  lock_preserve_phase1_cancel_probe cancel_probe{nullptr};
  void *cancel_context{nullptr};
  bool stable_page_only{false};
};

struct lock_preserve_phase1_record_resolve_metrics {
  uint32_t page_groups{0};
  uint32_t page_ready{0};
  uint32_t page_retryable{0};
  uint32_t table_open_attempts{0};
};

/*
  Allocation-free logical workset plan for the compact stable-page resolver.
  Container/control-block overhead is bounded by the attempt's fixed slot and
  worker counts; these fields account for target-dependent bytes.
*/
struct lock_preserve_phase1_record_resolve_plan {
  uint64_t resolved_snapshot_logical_bytes{0};
  uint64_t serialized_payload_bytes{0};
  uint64_t scratch_logical_bytes{0};
  uint32_t entry_count{0};
  uint32_t set_bit_count{0};
};

/*
  Pointer-free record-lock facts copied while the target page shards and
  trx->mutex are held. Page identity fields remain empty until the lock-out
  resolve phase.
*/
struct lock_preserve_phase1_record_entry {
  uint64_t table_id{0};
  uint64_t index_id{0};
  uint32_t space_id{0};
  uint32_t page_no{0};
  uint32_t type_mode{0};
  uint32_t n_bits{0};
  uint64_t page_lsn{0};
  uint32_t page_n_heap{0};
  std::string heap_offsets;
  std::string record_images;
  std::string bitmap;
  uint32_t set_bits{0};
  uint32_t first_set_heap_no{UINT32_MAX};
  uint32_t max_set_heap_no{UINT32_MAX};
};

struct lock_preserve_phase1_record_snapshot {
  uint64_t immutable_trx_id{0};
  uint64_t trx_version{0};
  uint64_t raw_trx_cookie{0};
  uint64_t owner_thd_cookie{0};
  uint64_t trx_locks_version{0};
  uint64_t native_record_lock_count{0};
  uint64_t coordinate_generation{0};
  uint64_t freeze_generation{0};
  uint64_t captured_bytes{0};
  uint64_t global_latch_wait_us{0};
  uint64_t global_latch_hold_us{0};
  uint64_t global_latch_envelope_us{0};
  uint32_t exported_record_bits{0};
  bool conversion_attempt_after_freeze{false};
  bool conversion_unhandled_after_freeze{false};
  std::vector<lock_preserve_phase1_record_entry> entries;
};

/**
  Compute the target-dependent logical peak before the resolver allocates.
  Predicate and non-stable identity reconstruction deliberately remain on the
  existing fallback path because their page-derived size is not in snapshot.
*/
lock_preserve_phase1_record_resolve_status
lock_preserve_phase1_plan_record_values(
    const lock_preserve_phase1_record_snapshot &snapshot,
    bool stable_page_only,
    lock_preserve_phase1_record_resolve_plan *plan);

/**
  Capture a target-granular, pointer-free record-lock snapshot. The caller
  keeps the owning THD externally pinned for the duration of this call. This
  function holds lock_sys in shared mode and try-locks only the target's page
  shards; a busy shard yields a retryable result.
*/
lock_preserve_phase1_record_capture_status
lock_preserve_phase1_capture_record_values(
    trx_t *trx, uint32_t max_lock_count, uint64_t max_snapshot_bytes,
    lock_preserve_phase1_record_snapshot *snapshot);

/**
  Resolve page identity and serialize a pointer-free snapshot without taking
  lock_sys or dereferencing its source transaction. Table metadata is opened
  once per page group under a short worker-owned MDL, and page acquisition is
  cache-only and latch-nowait.
*/
lock_preserve_phase1_record_resolve_status
lock_preserve_phase1_resolve_record_values(
    THD *worker_thd, lock_preserve_phase1_record_snapshot *snapshot,
    const lock_preserve_phase1_record_resolve_control &control,
    std::string *payload,
    lock_preserve_phase1_record_resolve_metrics *metrics);

#endif /* lock0preserve_capture_h */
