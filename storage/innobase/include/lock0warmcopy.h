/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef lock0warmcopy_h
#define lock0warmcopy_h

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "db0err.h"

struct ib_lock_t;
struct trx_lock_t;
struct trx_t;

extern std::atomic<uint64_t> lock_warmcopy_epoch;

static constexpr uint32_t LOCK_WARMCOPY_RECORD_SHARD_DIRTY = 1U << 0;
static constexpr uint32_t LOCK_WARMCOPY_RECORD_SHARD_INVALID = 1U << 1;
static constexpr uint32_t LOCK_WARMCOPY_RECORD_SHARD_TOMBSTONE = 1U << 2;
static constexpr size_t LOCK_WARMCOPY_SHA256_DIGEST_LENGTH = 32;

struct lock_warmcopy_debug_stats_t {
  uint64_t observed_hook_events{0};
};

struct lock_warmcopy_record_shard_key_t {
  uint64_t table_id{0};
  uint64_t index_id{0};
  uint32_t space_id{0};
  uint32_t page_no{0};
  uint32_t lock_type_mode{0};
  uint32_t n_bits{0};
};

struct lock_warmcopy_record_image_digest_t {
  unsigned char bytes[32]{};
};

struct lock_warmcopy_record_image_entry_t {
  uint32_t heap_no{0};
  uint32_t heap_offset{0};
  lock_warmcopy_record_image_digest_t digest;
  std::string encoded_record_image;
};

struct lock_warmcopy_record_shard_snapshot_t {
  lock_warmcopy_record_shard_key_t key;
  std::vector<unsigned char> normalized_bitmap;
  std::vector<lock_warmcopy_record_image_entry_t> record_images;
  uint32_t set_bit_count{0};
  uint32_t shard_state_flags{0};
  uint64_t mutation_generation{0};
  uint64_t implicit_exclusion_generation{0};
  uint64_t journal_cursor{0};
  uint64_t last_applied_journal_seq{0};
  std::string last_diagnostic_reason;
};

struct lock_warmcopy_record_store_fence_t {
  uint32_t shard_count{0};
  uint64_t total_mutation_generation{0};
  uint64_t dirty_generation{0};
  unsigned char canonical_fingerprint[LOCK_WARMCOPY_SHA256_DIGEST_LENGTH]{};
};

enum class lock_warmcopy_record_seal_status_t {
  NOT_ATTEMPTED,
  SEALED_VALID,
  EMPTY,
  SEAL_FENCE_CHANGED,
  TARGET_INVALID,
  RESOURCE_LIMIT_EXCEEDED
};

struct lock_warmcopy_record_seal_result_t {
  lock_warmcopy_record_seal_status_t status{
      lock_warmcopy_record_seal_status_t::NOT_ATTEMPTED};
  bool sealed{false};
  std::string record_locks_payload;
  uint32_t record_lock_count{0};
  uint64_t journal_bytes{0};
  uint64_t materialized_payload_bytes{0};
  uint32_t scanned_shard_count{0};
  uint32_t dirty_shard_count{0};
  lock_warmcopy_record_store_fence_t seal_fence;
  std::string diagnostic_reason;
};

struct lock_warmcopy_trx_lock_fence_t {
  uint64_t trx_locks_version{0};
  uint64_t n_rec_locks{0};
  uint64_t freeze_generation{0};
  bool conversion_attempt_after_freeze{false};
  bool conversion_unhandled_after_freeze{false};
};

struct lock_warmcopy_prepare_guard_t;

enum class lock_warmcopy_hook_family_t {
  RECORD,
  TABLE,
  AUTOINC
};

enum class lock_warmcopy_hook_action_t {
  UNCLASSIFIED,
  JOURNAL_DELTA,
  DIRTY_SHARD,
  INVALID_TARGET,
  READ_ONLY,
  UNREACHABLE
};

struct lock_warmcopy_hook_coverage_site_t {
  const char *symbol;
  const char *source_path;
  lock_warmcopy_hook_family_t family;
  lock_warmcopy_hook_action_t action;
};

inline bool lock_warmcopy_hooks_enabled() {
  return lock_warmcopy_epoch.load(std::memory_order_acquire) != 0;
}

void lock_warmcopy_open_epoch(uint64_t epoch);
void lock_warmcopy_close_epoch();
void lock_warmcopy_record_hook_event();
bool lock_warmcopy_record_bitmap_set(
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no);
bool lock_warmcopy_record_bitmap_reset(
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no);
bool lock_warmcopy_record_bitmap_set_for_trx(
    const trx_t *trx, const lock_warmcopy_record_shard_key_t &key,
    uint32_t heap_no);
bool lock_warmcopy_record_bitmap_reset_for_trx(
    const trx_t *trx, const lock_warmcopy_record_shard_key_t &key,
    uint32_t heap_no);
bool lock_warmcopy_record_bitmap_set_for_lock(const ib_lock_t *lock,
                                              uint32_t heap_no);
bool lock_warmcopy_record_bitmap_reset_for_lock(const ib_lock_t *lock,
                                                uint32_t heap_no);
bool lock_warmcopy_record_bitmap_set_with_image_for_trx(
    const trx_t *trx, const lock_warmcopy_record_shard_key_t &key,
    uint32_t heap_no, const lock_warmcopy_record_image_digest_t &digest,
    uint32_t heap_offset, const std::string &encoded_record_image);
bool lock_warmcopy_record_bitmap_set_with_image_for_lock(
    const ib_lock_t *lock, uint32_t heap_no, uint32_t heap_offset,
    const std::string &encoded_record_image);
lock_warmcopy_debug_stats_t lock_warmcopy_debug_stats_for_unit_test();
void lock_warmcopy_reset_for_unit_test();
bool lock_warmcopy_record_store_export_record_payload(std::string *payload,
                                                      uint32_t *lock_count);
bool lock_warmcopy_record_store_export_record_payload_for_target(
    uint64_t target_id, std::string *payload, uint32_t *lock_count);
void lock_warmcopy_record_store_target_ids(std::vector<uint64_t> *target_ids);
bool lock_warmcopy_record_store_seed_payload_for_target(
    uint64_t target_id, const std::string &payload, uint32_t *lock_count);
void lock_warmcopy_record_store_clear_for_target(uint64_t target_id);
bool lock_warmcopy_record_store_fence_for_target(
    uint64_t target_id, lock_warmcopy_record_store_fence_t *fence);
bool lock_warmcopy_record_store_fence_equal(
    const lock_warmcopy_record_store_fence_t &lhs,
    const lock_warmcopy_record_store_fence_t &rhs);
bool lock_warmcopy_record_store_seal_for_target(
    uint64_t target_id, const lock_warmcopy_record_store_fence_t &phase1_fence,
    uint32_t max_lock_count, uint64_t max_journal_bytes,
    uint32_t max_dirty_shards, lock_warmcopy_record_seal_result_t *result);
bool lock_warmcopy_record_store_seal_metadata_for_target(
    uint64_t target_id, const lock_warmcopy_record_store_fence_t &phase1_fence,
    uint32_t expected_record_lock_count, uint32_t max_lock_count,
    uint64_t max_journal_bytes, uint32_t max_dirty_shards,
    lock_warmcopy_record_seal_result_t *result);
bool lock_warmcopy_trx_lock_fence_sample(
    const trx_lock_t *trx_lock, lock_warmcopy_trx_lock_fence_t *fence);
bool lock_warmcopy_trx_lock_fence_equal(
    const lock_warmcopy_trx_lock_fence_t &lhs,
    const lock_warmcopy_trx_lock_fence_t &rhs);
lock_warmcopy_prepare_guard_t *lock_warmcopy_prepare_guard_create();
void lock_warmcopy_prepare_guard_destroy(lock_warmcopy_prepare_guard_t *guard);
void lock_warmcopy_trx_conversion_freeze(trx_lock_t *trx_lock,
                                         uint64_t wait_epoch);
void lock_warmcopy_trx_conversion_thaw(trx_lock_t *trx_lock);
bool lock_warmcopy_trx_conversion_is_frozen(const trx_lock_t *trx_lock);
bool lock_warmcopy_trx_conversion_note_attempt(trx_lock_t *trx_lock);
bool lock_warmcopy_trx_conversion_note_handled(trx_lock_t *trx_lock);
dberr_t lock_warmcopy_frozen_conversion_result(int sel_mode);
dberr_t lock_warmcopy_wait_for_conversion_thaw(trx_t *trx);
dberr_t lock_warmcopy_wait_for_conversion_thaw_for_unit_test(
    const trx_t *trx, uint timeout_ms);
void lock_warmcopy_conversion_freeze_wait_note();
uint64_t lock_warmcopy_conversion_freeze_wait_count();

bool lock_warmcopy_record_bitmap_set_for_unit_test(
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no,
    const lock_warmcopy_record_image_digest_t &digest);
bool lock_warmcopy_record_bitmap_set_for_target_for_unit_test(
    uint64_t target_id, const lock_warmcopy_record_shard_key_t &key,
    uint32_t heap_no, const lock_warmcopy_record_image_digest_t &digest);
bool lock_warmcopy_record_bitmap_set_with_image_for_unit_test(
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no,
    const lock_warmcopy_record_image_digest_t &digest, uint32_t heap_offset,
    const std::string &encoded_record_image);
bool lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
    uint64_t target_id, const lock_warmcopy_record_shard_key_t &key,
    uint32_t heap_no, const lock_warmcopy_record_image_digest_t &digest,
    uint32_t heap_offset, const std::string &encoded_record_image);
bool lock_warmcopy_record_bitmap_reset_for_unit_test(
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no);
bool lock_warmcopy_record_journal_upsert_for_target_for_unit_test(
    uint64_t target_id, uint64_t journal_sequence,
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no,
    const lock_warmcopy_record_image_digest_t &digest, uint32_t heap_offset,
    const std::string &encoded_record_image);
bool lock_warmcopy_record_journal_patch_for_target_for_unit_test(
    uint64_t target_id, uint64_t journal_sequence,
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no,
    const lock_warmcopy_record_image_digest_t &digest, uint32_t heap_offset,
    const std::string &encoded_record_image);
bool lock_warmcopy_record_journal_delete_for_target_for_unit_test(
    uint64_t target_id, uint64_t journal_sequence,
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no);
bool lock_warmcopy_record_mark_unsupported_mutation_for_target_for_unit_test(
    uint64_t target_id, uint64_t journal_sequence,
    const lock_warmcopy_record_shard_key_t &key, const std::string &reason);
bool lock_warmcopy_record_shard_snapshot_for_unit_test(
    const lock_warmcopy_record_shard_key_t &key,
    lock_warmcopy_record_shard_snapshot_t *snapshot);
bool lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
    uint64_t target_id, const lock_warmcopy_record_shard_key_t &key,
    lock_warmcopy_record_shard_snapshot_t *snapshot);
std::string lock_warmcopy_record_shard_canonical_bytes_for_unit_test(
    const lock_warmcopy_record_shard_key_t &key);
bool lock_warmcopy_record_store_export_record_payload_for_unit_test(
    std::string *payload);
bool lock_warmcopy_record_store_export_record_payload_for_target_for_unit_test(
    uint64_t target_id, std::string *payload);
void lock_warmcopy_record_store_reset_for_unit_test();
void lock_warmcopy_trx_conversion_freeze_for_unit_test(
    trx_lock_t *trx_lock, uint64_t wait_epoch);
void lock_warmcopy_trx_conversion_thaw_for_unit_test(trx_lock_t *trx_lock);
bool lock_warmcopy_trx_conversion_is_frozen_for_unit_test(
    const trx_lock_t *trx_lock);
bool lock_warmcopy_trx_conversion_note_attempt_for_unit_test(
    trx_lock_t *trx_lock);
bool lock_warmcopy_trx_conversion_note_handled_for_unit_test(
    trx_lock_t *trx_lock);
bool lock_warmcopy_trx_lock_fence_sample_for_unit_test(
    const trx_lock_t *trx_lock, lock_warmcopy_trx_lock_fence_t *fence);
bool lock_warmcopy_trx_lock_fence_equal_for_unit_test(
    const lock_warmcopy_trx_lock_fence_t &lhs,
    const lock_warmcopy_trx_lock_fence_t &rhs);
lock_warmcopy_prepare_guard_t *
lock_warmcopy_prepare_guard_create_for_unit_test();
void lock_warmcopy_prepare_guard_destroy_for_unit_test(
    lock_warmcopy_prepare_guard_t *guard);
const lock_warmcopy_hook_coverage_site_t *
lock_warmcopy_hook_coverage_sites_for_unit_test(size_t *count);

#endif /* lock0warmcopy_h */
