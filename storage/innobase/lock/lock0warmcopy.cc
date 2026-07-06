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

#define LOCK_MODULE_IMPLEMENTATION

#include "storage/innobase/include/lock0warmcopy.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <limits>
#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "sql_thd_internal_api.h"
#include "buf0buf.h"
#include "debug_sync.h"
#include "dict0mem.h"
#include "mysql/plugin.h"
#include "storage/innobase/include/lock0lock.h"
#include "storage/innobase/include/lock0priv.h"
#include "sha2.h"
#include "storage/innobase/include/lock0guards.h"
#include "storage/innobase/include/lock0types.h"
#include "storage/innobase/include/os0thread.h"
#include "storage/innobase/include/srv0start.h"
#include "storage/innobase/include/trx0trx.h"

std::atomic<uint64_t> lock_warmcopy_epoch{0};

extern uint preserve_trx_lock_warmcopy_conversion_wait_timeout_ms;

namespace {
std::atomic<uint64_t> lock_warmcopy_observed_hook_events{0};
std::atomic<uint64_t> lock_warmcopy_conversion_freeze_waits{0};
constexpr uint64_t k_lock_warmcopy_default_target_id = 0;

template <typename IsFrozen, typename ShouldAbort>
dberr_t lock_warmcopy_wait_for_conversion_thaw_impl(IsFrozen is_frozen,
                                                    ShouldAbort should_abort,
                                                    uint timeout_ms) {
  using Clock = std::chrono::steady_clock;
  const auto started_at = Clock::now();
  bool sync_registered = false;

  for (;;) {
    if (should_abort()) {
      return DB_INTERRUPTED;
    }

    if (!is_frozen()) {
      return DB_SUCCESS;
    }

    if (timeout_ms == 0) {
      return DB_LOCK_WAIT_TIMEOUT;
    }

    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - started_at);
    if (elapsed.count() >= static_cast<int64_t>(timeout_ms)) {
      return DB_LOCK_WAIT_TIMEOUT;
    }

    if (!sync_registered) {
      DEBUG_SYNC_C("lock_warmcopy_conversion_waiter_registered");
      sync_registered = true;
    }
    os_thread_sleep(1000);
  }
}

struct lock_warmcopy_record_shard_key_less {
  bool operator()(const lock_warmcopy_record_shard_key_t &lhs,
                  const lock_warmcopy_record_shard_key_t &rhs) const {
    if (lhs.table_id != rhs.table_id) return lhs.table_id < rhs.table_id;
    if (lhs.index_id != rhs.index_id) return lhs.index_id < rhs.index_id;
    if (lhs.space_id != rhs.space_id) return lhs.space_id < rhs.space_id;
    if (lhs.page_no != rhs.page_no) return lhs.page_no < rhs.page_no;
    if (lhs.lock_type_mode != rhs.lock_type_mode) {
      return lhs.lock_type_mode < rhs.lock_type_mode;
    }
    return lhs.n_bits < rhs.n_bits;
  }
};

struct lock_warmcopy_record_shard_state_t {
  lock_warmcopy_record_shard_key_t key;
  /* Current explicit record-lock bitmap for this target/table/index/page. */
  std::vector<unsigned char> normalized_bitmap;
  /*
    Per-heap record identity captured by gated image hooks or by payload seed
    parsing. Bitmap set/reset hooks remain thin and do not build these images.
  */
  std::map<uint32_t, lock_warmcopy_record_image_entry_t> record_images;
  uint64_t page_lsn{0};
  uint32_t page_n_heap{0};
  uint32_t set_bit_count{0};
  uint32_t shard_state_flags{0};
  /* Nonzero means at least one set bit could not be tied to a record image. */
  uint32_t missing_record_image_count{0};
  /* Sticky per-shard invalid marker; seal must reject or route to fallback. */
  bool forced_invalid{false};
  /* Changes whenever bitmap/object state mutates, even if final bits match. */
  uint64_t mutation_generation{0};
  uint64_t implicit_exclusion_generation{0};
  uint64_t journal_cursor{0};
  uint64_t last_applied_journal_seq{0};
  uint64_t journal_bytes{0};
  /* Rolling payload fingerprint used for cheap phase-boundary comparisons. */
  uint64_t rolling_fingerprint[4]{};
  std::string last_diagnostic_reason;
};

using lock_warmcopy_record_shard_map_t =
    std::map<lock_warmcopy_record_shard_key_t,
             lock_warmcopy_record_shard_state_t,
             lock_warmcopy_record_shard_key_less>;
struct lock_warmcopy_record_store_partition_t {
  std::mutex mutex;
  std::map<uint64_t, lock_warmcopy_record_shard_map_t> store_by_target;
  std::map<uint64_t, uint64_t> journal_sequence_by_target;
  /*
    Expected sequence tracks whether every hook delta for a target was observed
    in order. A gap makes the target invalid even if the reconstructed bitmap is
    otherwise parseable.
  */
  std::map<uint64_t, uint64_t> expected_delta_sequence_by_target;
  /*
    Target-level invalidation diagnostic. Once present, SQL must route the target
    through live export or reject it; the string is best-effort reporting detail,
    not a correctness decision.
  */
  std::map<uint64_t, std::string> target_invalid_reason_by_target;
};
/*
  The record mirror is partitioned by target SQL thread id. Native lock code
  calls thin hooks; all heavy ordering, payload and diagnostic state stays in
  this preserve-only store. SQL rebuilds and clears the final target set for
  each drain epoch so thread-id reuse cannot carry old state into a later drain.
*/
constexpr size_t k_lock_warmcopy_record_store_partition_count = 64;
std::array<lock_warmcopy_record_store_partition_t,
           k_lock_warmcopy_record_store_partition_count>
    lock_warmcopy_record_store_partitions;

enum class record_journal_delta_kind_t {
  UPSERT,
  PATCH,
  DELETE
};

constexpr lock_warmcopy_hook_coverage_site_t k_lock_warmcopy_hook_sites[] = {
    /*
      Hook coverage is an executable contract for source-shape tests. Sites
      marked JOURNAL_DELTA must describe object-level set/reset/add/remove
      changes; DIRTY_SHARD sites invalidate fast reuse when the native lock code
      moves or inherits locks without a direct one-record delta.
    */
    {"lock_rec_add_to_queue", "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::RECORD,
     lock_warmcopy_hook_action_t::JOURNAL_DELTA},
    {"lock_rec_set_nth_bit", "storage/innobase/include/lock0priv.ic",
     lock_warmcopy_hook_family_t::RECORD,
     lock_warmcopy_hook_action_t::JOURNAL_DELTA},
    {"lock_rec_reset_nth_bit", "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::RECORD,
     lock_warmcopy_hook_action_t::JOURNAL_DELTA},
    {"lock_rec_discard", "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::RECORD,
     lock_warmcopy_hook_action_t::DIRTY_SHARD},
    {"lock_rec_reset_and_release_wait_low",
     "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::RECORD,
     lock_warmcopy_hook_action_t::DIRTY_SHARD},
    {"lock_rec_move_low", "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::RECORD,
     lock_warmcopy_hook_action_t::DIRTY_SHARD},
    {"lock_rec_move", "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::RECORD,
     lock_warmcopy_hook_action_t::DIRTY_SHARD},
    {"lock_update_discard", "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::RECORD,
     lock_warmcopy_hook_action_t::DIRTY_SHARD},
    {"lock_rec_reset_and_inherit_gap_locks",
     "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::RECORD,
     lock_warmcopy_hook_action_t::DIRTY_SHARD},
    {"lock_rec_inherit_to_gap", "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::RECORD,
     lock_warmcopy_hook_action_t::DIRTY_SHARD},
    {"lock_rec_inherit_to_gap_if_gap_lock",
     "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::RECORD,
     lock_warmcopy_hook_action_t::DIRTY_SHARD},
    {"lock_rec_convert_impl_to_expl_for_trx",
     "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::RECORD,
     lock_warmcopy_hook_action_t::JOURNAL_DELTA},
    {"lock_rec_convert_active_impl_to_expl",
     "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::RECORD,
     lock_warmcopy_hook_action_t::JOURNAL_DELTA},
    {"lock_table_create", "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::TABLE,
     lock_warmcopy_hook_action_t::JOURNAL_DELTA},
    {"lock_table_remove_low", "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::TABLE,
     lock_warmcopy_hook_action_t::DIRTY_SHARD},
    {"lock_table_dequeue", "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::TABLE,
     lock_warmcopy_hook_action_t::DIRTY_SHARD},
    {"lock_unlock_table_autoinc", "storage/innobase/lock/lock0lock.cc",
     lock_warmcopy_hook_family_t::AUTOINC,
     lock_warmcopy_hook_action_t::DIRTY_SHARD},
};

const uint32_t k_record_bitmap_max_bits = 1024U * 1024U;

size_t record_bitmap_len(uint32_t n_bits) {
  if (n_bits == 0 || n_bits > k_record_bitmap_max_bits ||
      n_bits > UINT32_MAX - 7U) {
    return 0;
  }
  return (n_bits + 7U) / 8U;
}

bool record_heap_no_is_valid(const lock_warmcopy_record_shard_key_t &key,
                             uint32_t heap_no) {
  return record_bitmap_len(key.n_bits) != 0 && heap_no < key.n_bits;
}

void normalize_record_bitmap(std::vector<unsigned char> *bitmap,
                             uint32_t n_bits) {
  if (bitmap->empty()) return;

  const uint32_t trailing_bits = n_bits % 8U;
  if (trailing_bits == 0) return;

  const unsigned char mask =
      static_cast<unsigned char>((1U << trailing_bits) - 1U);
  bitmap->back() = static_cast<unsigned char>(bitmap->back() & mask);
}

uint32_t record_bitmap_set_bit_count(const std::vector<unsigned char> &bitmap) {
  uint32_t count = 0;
  for (const unsigned char bitmap_byte : bitmap) {
    unsigned char value = bitmap_byte;
    while (value != 0) {
      count += static_cast<uint32_t>(value & 1U);
      value = static_cast<unsigned char>(value >> 1U);
    }
  }
  return count;
}

void append_u32_le(std::string *out, uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

void append_u64_le(std::string *out, uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

void append_record_shard_key_le(std::string *out,
                                const lock_warmcopy_record_shard_key_t &key) {
  append_u64_le(out, key.table_id);
  append_u64_le(out, key.index_id);
  append_u32_le(out, key.space_id);
  append_u32_le(out, key.page_no);
  append_u32_le(out, key.lock_type_mode);
  append_u32_le(out, key.n_bits);
}

uint64_t mix_u64(uint64_t value) {
  value ^= value >> 30;
  value *= 0xbf58476d1ce4e5b9ULL;
  value ^= value >> 27;
  value *= 0x94d049bb133111ebULL;
  value ^= value >> 31;
  return value;
}

uint64_t digest_word_le(const lock_warmcopy_record_image_digest_t *digest,
                        size_t word_index) {
  if (digest == nullptr || word_index >= 4) return 0;
  uint64_t value = 0;
  const size_t offset = word_index * sizeof(value);
  for (size_t i = 0; i < sizeof(value); ++i) {
    value |= static_cast<uint64_t>(digest->bytes[offset + i]) << (i * 8);
  }
  return value;
}

void update_record_shard_rolling_fingerprint_locked(
    lock_warmcopy_record_shard_state_t *shard, uint32_t op_code,
    uint32_t heap_no, const lock_warmcopy_record_image_digest_t *digest,
    uint32_t heap_offset) {
  /*
    The rolling fingerprint is a seal-time consistency fence and diagnostic
    value. It is updated while the record-store partition lock is held together
    with the bitmap, image digest and generation fields it summarizes.
  */
  uint64_t base = mix_u64(shard->key.table_id) ^
                  mix_u64(shard->key.index_id + 0x9e3779b97f4a7c15ULL) ^
                  mix_u64((static_cast<uint64_t>(shard->key.space_id) << 32) |
                          shard->key.page_no) ^
                  mix_u64((static_cast<uint64_t>(shard->key.lock_type_mode)
                           << 32) |
                          shard->key.n_bits) ^
                  mix_u64((static_cast<uint64_t>(op_code) << 32) | heap_no) ^
                  mix_u64(heap_offset) ^
                  mix_u64(shard->mutation_generation) ^
                  mix_u64(shard->implicit_exclusion_generation) ^
                  mix_u64(shard->journal_cursor) ^
                  mix_u64(shard->last_applied_journal_seq) ^
                  mix_u64((static_cast<uint64_t>(shard->set_bit_count) << 32) |
                          shard->missing_record_image_count) ^
                  mix_u64(shard->shard_state_flags);
  for (size_t i = 0; i < 4; ++i) {
    const uint64_t digest_part = digest_word_le(digest, i);
    const uint64_t mixed =
        mix_u64(base ^ digest_part ^
                (0x9e3779b97f4a7c15ULL * static_cast<uint64_t>(i + 1)));
    shard->rolling_fingerprint[i] =
        mix_u64(shard->rolling_fingerprint[i] ^ mixed ^
                (base + static_cast<uint64_t>(i)));
  }
}

bool read_u32_le_from_payload(const std::string &payload, size_t *offset,
                              uint32_t *value) {
  if (offset == nullptr || value == nullptr || *offset > payload.size() ||
      payload.size() - *offset < 4) {
    return false;
  }

  const unsigned char *bytes =
      reinterpret_cast<const unsigned char *>(payload.data() + *offset);
  *value = (static_cast<uint32_t>(bytes[0])) |
           (static_cast<uint32_t>(bytes[1]) << 8) |
           (static_cast<uint32_t>(bytes[2]) << 16) |
           (static_cast<uint32_t>(bytes[3]) << 24);
  *offset += 4;
  return true;
}

bool read_u64_le_from_payload(const std::string &payload, size_t *offset,
                              uint64_t *value) {
  if (offset == nullptr || value == nullptr || *offset > payload.size() ||
      payload.size() - *offset < 8) {
    return false;
  }

  const unsigned char *bytes =
      reinterpret_cast<const unsigned char *>(payload.data() + *offset);
  uint64_t parsed = 0;
  for (int i = 7; i >= 0; --i) {
    parsed = (parsed << 8) | bytes[i];
  }
  *value = parsed;
  *offset += 8;
  return true;
}

bool read_payload_slice(const std::string &payload, size_t *offset, size_t len,
                        std::string *out) {
  if (offset == nullptr || out == nullptr || *offset > payload.size() ||
      payload.size() - *offset < len) {
    return false;
  }

  out->assign(payload.data() + *offset, len);
  *offset += len;
  return true;
}

bool read_encoded_record_image_slot(const std::string &record_images,
                                    size_t *offset, std::string *slot) {
  if (offset == nullptr || slot == nullptr || *offset > record_images.size() ||
      record_images.size() - *offset < 4) {
    return false;
  }

  const size_t slot_start = *offset;
  uint32_t image_len = 0;
  if (!read_u32_le_from_payload(record_images, offset, &image_len) ||
      image_len == 0 || *offset > record_images.size() ||
      record_images.size() - *offset < image_len) {
    return false;
  }

  *offset += image_len;
  slot->assign(record_images.data() + slot_start, *offset - slot_start);
  return true;
}

size_t record_store_partition_index(uint64_t target_id) {
  return static_cast<size_t>(mix_u64(target_id) %
                             k_lock_warmcopy_record_store_partition_count);
}

lock_warmcopy_record_store_partition_t &record_store_partition_for_target(
    uint64_t target_id) {
  return lock_warmcopy_record_store_partitions[
      record_store_partition_index(target_id)];
}

lock_warmcopy_record_shard_map_t &record_store_for_target_locked(
    uint64_t target_id) {
  return record_store_partition_for_target(target_id)
      .store_by_target[target_id];
}

const lock_warmcopy_record_shard_map_t *record_store_for_target_if_exists_locked(
    uint64_t target_id) {
  const auto &partition = record_store_partition_for_target(target_id);
  const auto it = partition.store_by_target.find(target_id);
  if (it == partition.store_by_target.end()) return nullptr;
  return &it->second;
}

lock_warmcopy_record_shard_map_t *mutable_record_store_for_target_if_exists_locked(
    uint64_t target_id) {
  auto &partition = record_store_partition_for_target(target_id);
  const auto it = partition.store_by_target.find(target_id);
  if (it == partition.store_by_target.end()) return nullptr;
  return &it->second;
}

uint64_t next_record_journal_sequence_for_target_locked(uint64_t target_id) {
  return ++record_store_partition_for_target(target_id)
               .journal_sequence_by_target[target_id];
}

uint64_t &expected_record_delta_sequence_for_target_locked(uint64_t target_id) {
  uint64_t &expected =
      record_store_partition_for_target(target_id)
          .expected_delta_sequence_by_target[target_id];
  if (expected == 0) expected = 1;
  return expected;
}

uint64_t saturating_add_u64(uint64_t lhs, uint64_t rhs) {
  if (std::numeric_limits<uint64_t>::max() - lhs < rhs) {
    return std::numeric_limits<uint64_t>::max();
  }
  return lhs + rhs;
}

uint64_t record_journal_delta_bytes(
    const std::string *encoded_record_image) {
  constexpr uint64_t k_fixed_delta_bytes =
      8 +   // journal sequence
      4 +   // delta kind
      32 +  // record shard key fields
      4 +   // heap no
      4 +   // heap offset
      LOCK_WARMCOPY_SHA256_DIGEST_LENGTH +
      4;  // encoded record image length
  return saturating_add_u64(
      k_fixed_delta_bytes,
      encoded_record_image == nullptr ? 0 : encoded_record_image->size());
}

void add_record_shard_journal_bytes_locked(
    lock_warmcopy_record_shard_state_t *shard, uint64_t bytes) {
  shard->journal_bytes = saturating_add_u64(shard->journal_bytes, bytes);
}

uint64_t record_target_id_for_trx(const trx_t *trx) {
  /*
    Normal SQL-owned transactions are partitioned by SQL thread id. A missing
    mysql_thd can only be reported in the default bucket, which is useful for
    diagnostics but cannot form a resumable per-session warm artifact.
  */
  if (trx == nullptr || trx->mysql_thd == nullptr) {
    return k_lock_warmcopy_default_target_id;
  }
  return static_cast<uint64_t>(thd_thread_id(trx->mysql_thd));
}

lock_warmcopy_record_shard_state_t &find_or_create_record_shard(
    uint64_t target_id, const lock_warmcopy_record_shard_key_t &key) {
  lock_warmcopy_record_shard_map_t &store =
      record_store_for_target_locked(target_id);
  auto result = store.emplace(
      key, lock_warmcopy_record_shard_state_t{});
  lock_warmcopy_record_shard_state_t &shard = result.first->second;
  if (result.second) {
    shard.key = key;
    shard.normalized_bitmap.assign(record_bitmap_len(key.n_bits), 0);
  }
  return shard;
}

lock_warmcopy_record_shard_state_t &find_or_create_record_shard_in_store(
    lock_warmcopy_record_shard_map_t *store,
    const lock_warmcopy_record_shard_key_t &key) {
  auto result = store->emplace(key, lock_warmcopy_record_shard_state_t{});
  lock_warmcopy_record_shard_state_t &shard = result.first->second;
  if (result.second) {
    shard.key = key;
    shard.normalized_bitmap.assign(record_bitmap_len(key.n_bits), 0);
  }
  return shard;
}

bool record_bitmap_bit_is_set(const lock_warmcopy_record_shard_state_t &shard,
                              uint32_t heap_no) {
  const size_t byte_pos = heap_no / 8U;
  const unsigned char bit_mask =
      static_cast<unsigned char>(1U << (heap_no % 8U));
  return (shard.normalized_bitmap[byte_pos] & bit_mask) != 0;
}

void refresh_missing_record_image_flag(
    lock_warmcopy_record_shard_state_t *shard) {
  if (shard->missing_record_image_count == 0 && !shard->forced_invalid) {
    shard->shard_state_flags &= ~LOCK_WARMCOPY_RECORD_SHARD_INVALID;
    shard->last_diagnostic_reason.clear();
  } else {
    shard->shard_state_flags |= LOCK_WARMCOPY_RECORD_SHARD_INVALID;
    if (shard->last_diagnostic_reason.empty()) {
      shard->last_diagnostic_reason = "record_image_missing";
    }
  }
}

void mark_record_shard_invalid_locked(lock_warmcopy_record_shard_state_t *shard,
                                      uint64_t journal_cursor,
                                      const char *reason) {
  shard->forced_invalid = true;
  shard->shard_state_flags |= LOCK_WARMCOPY_RECORD_SHARD_DIRTY |
                              LOCK_WARMCOPY_RECORD_SHARD_INVALID;
  shard->last_diagnostic_reason = reason == nullptr ? "" : reason;
  ++shard->mutation_generation;
  shard->journal_cursor = journal_cursor;
  update_record_shard_rolling_fingerprint_locked(shard, 1, 0, nullptr, 0);
}

void mark_record_target_invalid_locked(uint64_t target_id,
                                       uint64_t journal_cursor,
                                       const char *reason) {
  record_store_partition_for_target(target_id)
      .target_invalid_reason_by_target[target_id] =
      reason == nullptr ? "" : reason;
  lock_warmcopy_record_shard_map_t *store =
      mutable_record_store_for_target_if_exists_locked(target_id);
  if (store == nullptr) return;

  for (auto &entry : *store) {
    mark_record_shard_invalid_locked(&entry.second, journal_cursor, reason);
  }
}

void mark_record_store_dirty_after_base_seed_locked(
    uint64_t target_id, lock_warmcopy_record_shard_map_t *store) {
  if (store == nullptr || store->empty()) {
    mark_record_target_invalid_locked(target_id, 0,
                                      "record_base_seed_after_delta_empty");
    return;
  }

  for (auto &entry : *store) {
    entry.second.shard_state_flags |= LOCK_WARMCOPY_RECORD_SHARD_DIRTY;
    ++entry.second.mutation_generation;
    entry.second.last_diagnostic_reason = "record_base_seed_after_delta";
    update_record_shard_rolling_fingerprint_locked(&entry.second, 2, 0,
                                                   nullptr, 0);
  }
}

bool record_target_is_invalid_locked(uint64_t target_id) {
  const auto &invalid_map =
      record_store_partition_for_target(target_id)
          .target_invalid_reason_by_target;
  return invalid_map.find(target_id) != invalid_map.end();
}

void update_record_shard_page_identity_locked(
    lock_warmcopy_record_shard_state_t *shard, uint64_t page_lsn,
    uint32_t page_n_heap) {
  if (shard == nullptr || page_n_heap == 0) return;
  shard->page_lsn = page_lsn;
  shard->page_n_heap = page_n_heap;
}

bool record_bitmap_set_locked(
    uint64_t target_id, const lock_warmcopy_record_shard_key_t &key,
    uint32_t heap_no, const lock_warmcopy_record_image_digest_t *digest,
    uint32_t heap_offset, const std::string *encoded_record_image,
    uint64_t page_lsn = 0, uint32_t page_n_heap = 0) {
  /*
    Set/reset operations keep the normalized bitmap, optional record image,
    journal accounting and generation in one store critical section. Seal can
    then reject a target on any sequence gap or fence change instead of trying
    to reconstruct missing lock mutations.
  */
  const uint64_t journal_cursor =
      next_record_journal_sequence_for_target_locked(target_id);
  lock_warmcopy_record_shard_state_t &shard =
      find_or_create_record_shard(target_id, key);
  update_record_shard_page_identity_locked(&shard, page_lsn, page_n_heap);
  const bool was_set = record_bitmap_bit_is_set(shard, heap_no);
  const bool had_digest = shard.record_images.find(heap_no) !=
                          shard.record_images.end();
  const size_t byte_pos = heap_no / 8U;
  const unsigned char bit_mask =
      static_cast<unsigned char>(1U << (heap_no % 8U));

  shard.normalized_bitmap[byte_pos] =
      static_cast<unsigned char>(shard.normalized_bitmap[byte_pos] | bit_mask);
  if (!was_set) ++shard.set_bit_count;
  if (digest == nullptr) {
    if (!was_set || had_digest) ++shard.missing_record_image_count;
    shard.record_images.erase(heap_no);
  } else {
    if (was_set && !had_digest && shard.missing_record_image_count > 0) {
      --shard.missing_record_image_count;
    }
    lock_warmcopy_record_image_entry_t entry;
    entry.heap_no = heap_no;
    entry.heap_offset = heap_offset;
    entry.digest = *digest;
    if (encoded_record_image != nullptr) {
      entry.encoded_record_image = *encoded_record_image;
    }
    shard.record_images[heap_no] = entry;
  }
  shard.shard_state_flags |= LOCK_WARMCOPY_RECORD_SHARD_DIRTY;
  shard.shard_state_flags &= ~LOCK_WARMCOPY_RECORD_SHARD_TOMBSTONE;
  refresh_missing_record_image_flag(&shard);
  ++shard.mutation_generation;
  add_record_shard_journal_bytes_locked(
      &shard, record_journal_delta_bytes(encoded_record_image));
  shard.journal_cursor = journal_cursor;
  shard.last_applied_journal_seq = journal_cursor;
  normalize_record_bitmap(&shard.normalized_bitmap, shard.key.n_bits);
  update_record_shard_rolling_fingerprint_locked(&shard, 3, heap_no, digest,
                                                 heap_offset);
  return true;
}

bool record_bitmap_reset_locked(uint64_t target_id,
                                const lock_warmcopy_record_shard_key_t &key,
                                uint32_t heap_no) {
  const uint64_t journal_cursor =
      next_record_journal_sequence_for_target_locked(target_id);
  lock_warmcopy_record_shard_state_t &shard =
      find_or_create_record_shard(target_id, key);
  const bool was_set = record_bitmap_bit_is_set(shard, heap_no);
  const bool had_digest = shard.record_images.find(heap_no) !=
                          shard.record_images.end();
  const size_t byte_pos = heap_no / 8U;
  const unsigned char bit_mask =
      static_cast<unsigned char>(1U << (heap_no % 8U));

  shard.normalized_bitmap[byte_pos] =
      static_cast<unsigned char>(shard.normalized_bitmap[byte_pos] &
                                 static_cast<unsigned char>(~bit_mask));
  if (was_set && shard.set_bit_count > 0) --shard.set_bit_count;
  if (was_set && !had_digest && shard.missing_record_image_count > 0) {
    --shard.missing_record_image_count;
  }
  shard.record_images.erase(heap_no);
  shard.shard_state_flags |= LOCK_WARMCOPY_RECORD_SHARD_DIRTY;
  refresh_missing_record_image_flag(&shard);
  ++shard.mutation_generation;
  add_record_shard_journal_bytes_locked(
      &shard, record_journal_delta_bytes(nullptr));
  shard.journal_cursor = journal_cursor;
  shard.last_applied_journal_seq = journal_cursor;
  normalize_record_bitmap(&shard.normalized_bitmap, shard.key.n_bits);
  update_record_shard_rolling_fingerprint_locked(&shard, 4, heap_no, nullptr,
                                                 0);
  return true;
}

bool record_lock_discard_locked(uint64_t target_id,
                                const lock_warmcopy_record_shard_key_t &key) {
  if (record_bitmap_len(key.n_bits) == 0) return false;

  const uint64_t journal_cursor =
      next_record_journal_sequence_for_target_locked(target_id);
  lock_warmcopy_record_shard_state_t &shard =
      find_or_create_record_shard(target_id, key);

  std::fill(shard.normalized_bitmap.begin(), shard.normalized_bitmap.end(), 0);
  shard.record_images.clear();
  shard.set_bit_count = 0;
  shard.missing_record_image_count = 0;
  shard.forced_invalid = false;
  shard.shard_state_flags = LOCK_WARMCOPY_RECORD_SHARD_DIRTY |
                            LOCK_WARMCOPY_RECORD_SHARD_TOMBSTONE;
  shard.last_diagnostic_reason = "record_lock_discard";
  ++shard.mutation_generation;
  add_record_shard_journal_bytes_locked(&shard,
                                        record_journal_delta_bytes(nullptr));
  shard.journal_cursor = journal_cursor;
  shard.last_applied_journal_seq = journal_cursor;
  update_record_shard_rolling_fingerprint_locked(&shard, 9, 0, nullptr, 0);
  return true;
}

bool apply_record_journal_delta_locked(
    uint64_t target_id, uint64_t journal_sequence,
    record_journal_delta_kind_t delta_kind,
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no,
    const lock_warmcopy_record_image_digest_t *digest, uint32_t heap_offset,
    const std::string *encoded_record_image) {
  /*
    Journal deltas are applied in strict sequence. A gap means the mirror no
    longer proves the exact lock set for this target, so the target is marked
    invalid and must use fallback or reject during drain.
  */
  uint64_t &expected_sequence =
      expected_record_delta_sequence_for_target_locked(target_id);
  if (journal_sequence < expected_sequence) return false;
  if (journal_sequence > expected_sequence) {
    mark_record_target_invalid_locked(target_id, journal_sequence,
                                      "journal_seq_gap");
    expected_sequence = journal_sequence + 1;
    return false;
  }

  lock_warmcopy_record_shard_state_t &shard =
      find_or_create_record_shard(target_id, key);
  const bool bit_was_set = record_bitmap_bit_is_set(shard, heap_no);
  const bool had_digest = shard.record_images.find(heap_no) !=
                          shard.record_images.end();
  const size_t byte_pos = heap_no / 8U;
  const unsigned char bit_mask =
      static_cast<unsigned char>(1U << (heap_no % 8U));

  switch (delta_kind) {
    case record_journal_delta_kind_t::UPSERT: {
      shard.normalized_bitmap[byte_pos] = static_cast<unsigned char>(
          shard.normalized_bitmap[byte_pos] | bit_mask);
      if (!bit_was_set) ++shard.set_bit_count;
      lock_warmcopy_record_image_entry_t entry;
      entry.heap_no = heap_no;
      entry.heap_offset = heap_offset;
      entry.digest = *digest;
      entry.encoded_record_image = *encoded_record_image;
      if (bit_was_set && !had_digest && shard.missing_record_image_count > 0) {
        --shard.missing_record_image_count;
      }
      shard.record_images[heap_no] = entry;
      shard.shard_state_flags |= LOCK_WARMCOPY_RECORD_SHARD_DIRTY;
      shard.shard_state_flags &= ~LOCK_WARMCOPY_RECORD_SHARD_TOMBSTONE;
      refresh_missing_record_image_flag(&shard);
      break;
    }
    case record_journal_delta_kind_t::PATCH: {
      if (!bit_was_set ||
          (shard.shard_state_flags & LOCK_WARMCOPY_RECORD_SHARD_TOMBSTONE) !=
              0 ||
          shard.record_images.find(heap_no) == shard.record_images.end()) {
        mark_record_shard_invalid_locked(&shard, journal_sequence,
                                         "journal_patch_missing_record");
        record_store_partition_for_target(target_id)
            .target_invalid_reason_by_target[target_id] =
            "journal_patch_missing_record";
        expected_sequence = journal_sequence + 1;
        return false;
      }
      lock_warmcopy_record_image_entry_t &entry = shard.record_images[heap_no];
      entry.heap_no = heap_no;
      entry.heap_offset = heap_offset;
      entry.digest = *digest;
      entry.encoded_record_image = *encoded_record_image;
      shard.shard_state_flags |= LOCK_WARMCOPY_RECORD_SHARD_DIRTY;
      refresh_missing_record_image_flag(&shard);
      break;
    }
    case record_journal_delta_kind_t::DELETE: {
      if (bit_was_set &&
          shard.record_images.find(heap_no) == shard.record_images.end() &&
          shard.missing_record_image_count > 0) {
        --shard.missing_record_image_count;
      }
      if (bit_was_set && shard.set_bit_count > 0) --shard.set_bit_count;
      shard.normalized_bitmap[byte_pos] = static_cast<unsigned char>(
          shard.normalized_bitmap[byte_pos] &
          static_cast<unsigned char>(~bit_mask));
      shard.record_images.erase(heap_no);
      shard.shard_state_flags |= LOCK_WARMCOPY_RECORD_SHARD_DIRTY |
                                 LOCK_WARMCOPY_RECORD_SHARD_TOMBSTONE;
      refresh_missing_record_image_flag(&shard);
      break;
    }
  }

  ++shard.mutation_generation;
  add_record_shard_journal_bytes_locked(
      &shard, record_journal_delta_bytes(encoded_record_image));
  shard.journal_cursor = journal_sequence;
  shard.last_applied_journal_seq = journal_sequence;
  normalize_record_bitmap(&shard.normalized_bitmap, shard.key.n_bits);
  update_record_shard_rolling_fingerprint_locked(
      &shard,
      delta_kind == record_journal_delta_kind_t::UPSERT
          ? 5
          : (delta_kind == record_journal_delta_kind_t::PATCH ? 6 : 7),
      heap_no, digest, heap_offset);
  expected_sequence = journal_sequence + 1;
  return true;
}

void copy_record_shard_snapshot(
    const lock_warmcopy_record_shard_state_t &shard,
    lock_warmcopy_record_shard_snapshot_t *snapshot) {
  snapshot->key = shard.key;
  snapshot->normalized_bitmap = shard.normalized_bitmap;
  normalize_record_bitmap(&snapshot->normalized_bitmap, shard.key.n_bits);
  snapshot->record_images.clear();
  snapshot->record_images.reserve(shard.record_images.size());
  for (const auto &entry : shard.record_images) {
    snapshot->record_images.push_back(entry.second);
  }
  snapshot->page_lsn = shard.page_lsn;
  snapshot->page_n_heap = shard.page_n_heap;
  snapshot->set_bit_count = shard.set_bit_count;
  snapshot->shard_state_flags = shard.shard_state_flags;
  snapshot->mutation_generation = shard.mutation_generation;
  snapshot->implicit_exclusion_generation =
      shard.implicit_exclusion_generation;
  snapshot->journal_cursor = shard.journal_cursor;
  snapshot->last_applied_journal_seq = shard.last_applied_journal_seq;
  snapshot->last_diagnostic_reason = shard.last_diagnostic_reason;
}

std::string record_shard_canonical_bytes_locked(
    const lock_warmcopy_record_shard_state_t &shard) {
  lock_warmcopy_record_shard_snapshot_t snapshot;
  copy_record_shard_snapshot(shard, &snapshot);

  std::string out;
  append_u32_le(&out, 1);  // canonical_shard_semantic_bytes_v1
  append_u64_le(&out, snapshot.key.table_id);
  append_u64_le(&out, snapshot.key.index_id);
  append_u32_le(&out, snapshot.key.space_id);
  append_u32_le(&out, snapshot.key.page_no);
  append_u32_le(&out, snapshot.key.lock_type_mode);
  append_u32_le(&out, snapshot.key.n_bits);
  append_u64_le(&out, snapshot.page_lsn);
  append_u32_le(&out, snapshot.page_n_heap);
  append_u32_le(&out,
                static_cast<uint32_t>(snapshot.normalized_bitmap.size()));
  for (const unsigned char bitmap_byte : snapshot.normalized_bitmap) {
    out.push_back(static_cast<char>(bitmap_byte));
  }
  append_u32_le(&out, snapshot.set_bit_count);
  for (const lock_warmcopy_record_image_entry_t &entry :
       snapshot.record_images) {
    append_u32_le(&out, entry.heap_no);
    out.append(reinterpret_cast<const char *>(entry.digest.bytes),
               sizeof(entry.digest.bytes));
  }
  append_u32_le(&out, snapshot.shard_state_flags);
  append_u64_le(&out, snapshot.mutation_generation);
  append_u64_le(&out, snapshot.implicit_exclusion_generation);
  return out;
}

bool append_record_payload_entry_locked(
    const lock_warmcopy_record_shard_state_t &shard, std::string *payload,
    uint32_t *lock_count) {
  std::vector<unsigned char> bitmap = shard.normalized_bitmap;
  normalize_record_bitmap(&bitmap, shard.key.n_bits);
  const uint32_t set_bits = record_bitmap_set_bit_count(bitmap);
  if (set_bits == 0) return true;

  if ((shard.shard_state_flags & LOCK_WARMCOPY_RECORD_SHARD_INVALID) != 0 ||
      shard.missing_record_image_count != 0) {
    return false;
  }

  std::string heap_offsets;
  std::string record_images;
  for (uint32_t heap_no = 0; heap_no < shard.key.n_bits; ++heap_no) {
    const size_t byte_pos = heap_no / 8U;
    const unsigned char bit_mask =
        static_cast<unsigned char>(1U << (heap_no % 8U));
    if ((bitmap[byte_pos] & bit_mask) == 0) continue;

    const auto image_it = shard.record_images.find(heap_no);
    if (image_it == shard.record_images.end() ||
        image_it->second.encoded_record_image.empty()) {
      return false;
    }
    append_u32_le(&heap_offsets, image_it->second.heap_offset);
    record_images.append(image_it->second.encoded_record_image);
  }

  if (heap_offsets.size() != static_cast<size_t>(set_bits) * 4) return false;

  append_u64_le(payload, shard.key.table_id);
  append_u64_le(payload, shard.key.index_id);
  append_u32_le(payload, shard.key.space_id);
  append_u32_le(payload, shard.key.page_no);
  append_u32_le(payload, shard.key.lock_type_mode);
  append_u32_le(payload, shard.key.n_bits);
  append_u64_le(payload, shard.page_lsn);
  append_u32_le(payload, shard.page_n_heap == 0 ? shard.key.n_bits
                                                : shard.page_n_heap);
  append_u32_le(payload, static_cast<uint32_t>(heap_offsets.size()));
  append_u32_le(payload, static_cast<uint32_t>(record_images.size()));
  append_u32_le(payload, static_cast<uint32_t>(bitmap.size()));
  payload->append(heap_offsets);
  payload->append(record_images);
  for (const unsigned char bitmap_byte : bitmap) {
    payload->push_back(bitmap_byte);
  }
  if (lock_count != nullptr) *lock_count += set_bits;
  return true;
}

bool export_record_payload_from_store_locked(
    uint64_t target_id, const lock_warmcopy_record_shard_map_t *store,
    std::string *payload, uint32_t *lock_count,
    uint32_t *scanned_shard_count) {
  if (payload == nullptr) return false;
  if (scanned_shard_count != nullptr) *scanned_shard_count = 0;
  if (record_target_is_invalid_locked(target_id)) {
    payload->clear();
    if (lock_count != nullptr) *lock_count = 0;
    return false;
  }

  std::string entries_payload;
  uint32_t entry_count = 0;
  uint32_t exported_lock_count = 0;
  if (store != nullptr) {
    if (scanned_shard_count != nullptr) {
      const size_t bounded_scan_count =
          std::min(store->size(),
                   static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
      *scanned_shard_count = static_cast<uint32_t>(bounded_scan_count);
    }
    for (const auto &entry : *store) {
      const size_t before = entries_payload.size();
      if (!append_record_payload_entry_locked(entry.second, &entries_payload,
                                              &exported_lock_count)) {
        payload->clear();
        if (lock_count != nullptr) *lock_count = 0;
        return false;
      }
      if (entries_payload.size() != before) ++entry_count;
    }
  }

  payload->clear();
  if (lock_count != nullptr) *lock_count = exported_lock_count;
  if (entry_count == 0) return true;
  append_u32_le(payload, entry_count);
  payload->append(entries_payload);
  return true;
}

void append_record_fence_shard_locked(
    const lock_warmcopy_record_shard_state_t &shard, std::string *fence_bytes,
    lock_warmcopy_record_store_fence_t *fence) {
  append_record_shard_key_le(fence_bytes, shard.key);
  append_u32_le(fence_bytes, shard.set_bit_count);
  append_u32_le(fence_bytes, shard.shard_state_flags);
  append_u32_le(fence_bytes, shard.missing_record_image_count);
  append_u64_le(fence_bytes, shard.mutation_generation);
  append_u64_le(fence_bytes, shard.implicit_exclusion_generation);
  append_u64_le(fence_bytes, shard.journal_cursor);
  append_u64_le(fence_bytes, shard.last_applied_journal_seq);
  for (const uint64_t word : shard.rolling_fingerprint) {
    append_u64_le(fence_bytes, word);
  }
  ++fence->shard_count;
  fence->total_mutation_generation += shard.mutation_generation;
  if ((shard.shard_state_flags & LOCK_WARMCOPY_RECORD_SHARD_DIRTY) != 0) {
    fence->dirty_generation += shard.mutation_generation;
  }
}

bool record_store_metadata_for_target_locked(
    const lock_warmcopy_record_shard_map_t *store,
    lock_warmcopy_record_store_fence_t *fence,
    uint64_t *journal_bytes,
    uint32_t *dirty_shard_count,
    uint32_t *lock_count,
    bool *lock_count_overflow) {
  /*
    Metadata seal samples only the store shape: shard keys, generations,
    invalid/dirty flags, journal cursors, and aggregate lock counts. It does not
    export record images or payload bytes. Payload export later rechecks target
    invalid markers and per-shard state before returning bytes, so callers must
    keep both the metadata fence and payload status in the route decision.
  */
  if (fence == nullptr) return false;
  *fence = lock_warmcopy_record_store_fence_t{};
  if (journal_bytes != nullptr) *journal_bytes = 0;
  if (dirty_shard_count != nullptr) *dirty_shard_count = 0;
  if (lock_count != nullptr) *lock_count = 0;
  if (lock_count_overflow != nullptr) *lock_count_overflow = false;

  std::string fence_bytes;
  append_u32_le(&fence_bytes, 1);  // record_store_fence_v1
  append_u32_le(&fence_bytes, store == nullptr ? 0U
                                               : static_cast<uint32_t>(
                                                     store->size()));
  uint64_t local_lock_count = 0;
  if (store != nullptr) {
    for (const auto &entry : *store) {
      append_record_fence_shard_locked(entry.second, &fence_bytes, fence);
      if (journal_bytes != nullptr) {
        *journal_bytes =
            saturating_add_u64(*journal_bytes, entry.second.journal_bytes);
      }
      if (dirty_shard_count != nullptr &&
          (entry.second.shard_state_flags & LOCK_WARMCOPY_RECORD_SHARD_DIRTY) !=
              0) {
        ++(*dirty_shard_count);
      }
      if (lock_count != nullptr) {
        local_lock_count += entry.second.set_bit_count;
        if (local_lock_count > std::numeric_limits<uint32_t>::max()) {
          if (lock_count_overflow != nullptr) *lock_count_overflow = true;
          local_lock_count = std::numeric_limits<uint32_t>::max();
        }
      }
    }
  }
  if (lock_count != nullptr)
    *lock_count = static_cast<uint32_t>(local_lock_count);

  SHA_EVP256(reinterpret_cast<const unsigned char *>(fence_bytes.data()),
             fence_bytes.size(), fence->canonical_fingerprint);
  return true;
}

bool record_store_fence_for_target_locked(
    const lock_warmcopy_record_shard_map_t *store,
    lock_warmcopy_record_store_fence_t *fence) {
  return record_store_metadata_for_target_locked(store, fence, nullptr, nullptr,
                                                nullptr, nullptr);
}

bool record_shard_key_from_lock(const lock_t *lock,
                                lock_warmcopy_record_shard_key_t *key) {
  if (lock == nullptr || key == nullptr ||
      (lock->type_mode & LOCK_TYPE_MASK) != LOCK_REC || lock->trx == nullptr ||
      lock->index == nullptr || lock->index->table == nullptr) {
    return false;
  }

  key->table_id = lock->index->table->id;
  key->index_id = lock->index->id;
  key->space_id = lock->rec_lock.page_id.space();
  key->page_no = lock->rec_lock.page_id.page_no();
  key->lock_type_mode = lock->type_mode;
  key->n_bits = lock->rec_lock.n_bits;
  return true;
}
}

struct lock_warmcopy_prepare_guard_t {
  locksys::Global_exclusive_latch_guard guard;
};

void lock_warmcopy_open_epoch(uint64_t epoch) {
  /*
    Epoch zero means hooks are disabled. A non-zero epoch admits lightweight
    record-hook bookkeeping; target selection and final seal still decide
    whether any observed state is usable.
  */
  lock_warmcopy_epoch.store(epoch == 0 ? 1 : epoch, std::memory_order_release);
}

void lock_warmcopy_close_epoch() {
  lock_warmcopy_epoch.store(0, std::memory_order_release);
}

void lock_warmcopy_record_hook_event() {
  if (!lock_warmcopy_hooks_enabled()) return;
  lock_warmcopy_observed_hook_events.fetch_add(1, std::memory_order_relaxed);
}

bool lock_warmcopy_record_bitmap_set(
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no) {
  if (!lock_warmcopy_hooks_enabled() || !record_heap_no_is_valid(key, heap_no)) {
    return false;
  }

  lock_warmcopy_observed_hook_events.fetch_add(1, std::memory_order_relaxed);
  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(k_lock_warmcopy_default_target_id)
          .mutex);
  return record_bitmap_set_locked(k_lock_warmcopy_default_target_id, key,
                                  heap_no, nullptr, 0, nullptr);
}

bool lock_warmcopy_record_bitmap_reset(
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no) {
  if (!lock_warmcopy_hooks_enabled() || !record_heap_no_is_valid(key, heap_no)) {
    return false;
  }

  lock_warmcopy_observed_hook_events.fetch_add(1, std::memory_order_relaxed);
  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(k_lock_warmcopy_default_target_id)
          .mutex);
  return record_bitmap_reset_locked(k_lock_warmcopy_default_target_id, key,
                                    heap_no);
}

bool lock_warmcopy_record_bitmap_set_for_trx(
    const trx_t *trx, const lock_warmcopy_record_shard_key_t &key,
    uint32_t heap_no) {
  if (!lock_warmcopy_hooks_enabled() || !record_heap_no_is_valid(key, heap_no)) {
    return false;
  }

  /*
    Hot hooks do not parse record images or serialize payloads. They identify
    the target and record bit, then update the mirror under the target
    partition lock.
  */
  lock_warmcopy_observed_hook_events.fetch_add(1, std::memory_order_relaxed);
  const uint64_t target_id = record_target_id_for_trx(trx);
  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  return record_bitmap_set_locked(target_id, key, heap_no, nullptr, 0,
                                  nullptr);
}

bool lock_warmcopy_record_bitmap_reset_for_trx(
    const trx_t *trx, const lock_warmcopy_record_shard_key_t &key,
    uint32_t heap_no) {
  if (!lock_warmcopy_hooks_enabled() || !record_heap_no_is_valid(key, heap_no)) {
    return false;
  }

  lock_warmcopy_observed_hook_events.fetch_add(1, std::memory_order_relaxed);
  const uint64_t target_id = record_target_id_for_trx(trx);
  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  return record_bitmap_reset_locked(target_id, key, heap_no);
}

bool lock_warmcopy_record_bitmap_set_for_lock(const lock_t *lock,
                                              uint32_t heap_no) {
  lock_warmcopy_record_shard_key_t key;
  if (!record_shard_key_from_lock(lock, &key)) {
    lock_warmcopy_record_hook_event();
    return false;
  }

  return lock_warmcopy_record_bitmap_set_for_trx(lock->trx, key, heap_no);
}

bool lock_warmcopy_record_bitmap_reset_for_lock(const lock_t *lock,
                                                uint32_t heap_no) {
  lock_warmcopy_record_shard_key_t key;
  if (!record_shard_key_from_lock(lock, &key)) {
    lock_warmcopy_record_hook_event();
    return false;
  }

  return lock_warmcopy_record_bitmap_reset_for_trx(lock->trx, key, heap_no);
}

bool lock_warmcopy_record_mark_discard_for_lock(const lock_t *lock) {
  lock_warmcopy_record_shard_key_t key;
  if (!record_shard_key_from_lock(lock, &key)) {
    lock_warmcopy_record_hook_event();
    return false;
  }
  if (!lock_warmcopy_hooks_enabled()) return false;

  lock_warmcopy_observed_hook_events.fetch_add(1, std::memory_order_relaxed);
  const uint64_t target_id = record_target_id_for_trx(lock->trx);
  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  return record_lock_discard_locked(target_id, key);
}

bool lock_warmcopy_record_bitmap_set_with_image_for_trx(
    const trx_t *trx, const lock_warmcopy_record_shard_key_t &key,
    uint32_t heap_no, const lock_warmcopy_record_image_digest_t &digest,
    uint32_t heap_offset, const std::string &encoded_record_image) {
  if (!lock_warmcopy_hooks_enabled() || !record_heap_no_is_valid(key, heap_no) ||
      encoded_record_image.empty()) {
    return false;
  }

  lock_warmcopy_observed_hook_events.fetch_add(1, std::memory_order_relaxed);
  const uint64_t target_id = record_target_id_for_trx(trx);
  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  return record_bitmap_set_locked(target_id, key, heap_no, &digest, heap_offset,
                                  &encoded_record_image);
}

bool lock_warmcopy_record_bitmap_set_with_image_for_lock(
    const lock_t *lock, const buf_block_t *block, uint32_t heap_no,
    uint32_t heap_offset, const std::string &encoded_record_image) {
  lock_warmcopy_record_shard_key_t key;
  if (!record_shard_key_from_lock(lock, &key) || block == nullptr) {
    lock_warmcopy_record_hook_event();
    return false;
  }

  /*
    Image-bearing updates are used by phase-1 builders and validation paths, not
    by the minimal hot hook. The digest becomes the durable record identity for
    the exact heap slot represented in the mirror.
  */
  lock_warmcopy_record_image_digest_t digest;
  SHA_EVP256(reinterpret_cast<const unsigned char *>(
                 encoded_record_image.data()),
             encoded_record_image.size(), digest.bytes);

  const uint64_t page_lsn = mach_read_from_8(block->frame + FIL_PAGE_LSN);
  const uint32_t page_n_heap = page_dir_get_n_heap(block->frame);
  if (!lock_warmcopy_hooks_enabled() ||
      !record_heap_no_is_valid(key, heap_no) || encoded_record_image.empty()) {
    return false;
  }

  lock_warmcopy_observed_hook_events.fetch_add(1, std::memory_order_relaxed);
  const uint64_t target_id = record_target_id_for_trx(lock->trx);
  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  return record_bitmap_set_locked(target_id, key, heap_no, &digest, heap_offset,
                                  &encoded_record_image, page_lsn,
                                  page_n_heap);
}

bool lock_warmcopy_record_store_refresh_record_image_for_trx(
    const trx_t *trx, const dict_index_t *index, const buf_block_t *block,
    uint32_t heap_no, uint32_t heap_offset,
    const std::string &encoded_record_image) {
  if (!lock_warmcopy_hooks_enabled() || trx == nullptr || index == nullptr ||
      index->table == nullptr || block == nullptr ||
      encoded_record_image.empty()) {
    return false;
  }

  lock_warmcopy_record_image_digest_t digest;
  SHA_EVP256(reinterpret_cast<const unsigned char *>(
                 encoded_record_image.data()),
             encoded_record_image.size(), digest.bytes);

  const uint64_t target_id = record_target_id_for_trx(trx);
  auto &partition = record_store_partition_for_target(target_id);
  std::lock_guard<std::mutex> guard(partition.mutex);
  lock_warmcopy_record_shard_map_t *store =
      mutable_record_store_for_target_if_exists_locked(target_id);
  if (store == nullptr) return true;

  std::vector<lock_warmcopy_record_shard_key_t> matching_keys;
  const page_id_t page_id = block->get_page_id();
  for (const auto &entry : *store) {
    const lock_warmcopy_record_shard_key_t &key = entry.first;
    const lock_warmcopy_record_shard_state_t &shard = entry.second;
    if (key.table_id == index->table->id && key.index_id == index->id &&
        key.space_id == page_id.space() && key.page_no == page_id.page_no() &&
        record_heap_no_is_valid(key, heap_no) &&
        record_bitmap_bit_is_set(shard, heap_no)) {
      matching_keys.push_back(key);
    }
  }

  for (const lock_warmcopy_record_shard_key_t &key : matching_keys) {
    const uint64_t page_lsn = mach_read_from_8(block->frame + FIL_PAGE_LSN);
    const uint32_t page_n_heap = page_dir_get_n_heap(block->frame);
    record_bitmap_set_locked(target_id, key, heap_no, &digest, heap_offset,
                             &encoded_record_image, page_lsn, page_n_heap);
  }

  return true;
}

lock_warmcopy_debug_stats_t lock_warmcopy_debug_stats_for_unit_test() {
  lock_warmcopy_debug_stats_t stats;
  stats.observed_hook_events =
      lock_warmcopy_observed_hook_events.load(std::memory_order_relaxed);
  return stats;
}

void lock_warmcopy_reset_for_unit_test() {
  lock_warmcopy_close_epoch();
  lock_warmcopy_observed_hook_events.store(0, std::memory_order_relaxed);
  lock_warmcopy_conversion_freeze_waits.store(0, std::memory_order_relaxed);
  lock_warmcopy_record_store_reset_for_unit_test();
}

bool lock_warmcopy_record_bitmap_set_for_unit_test(
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no,
    const lock_warmcopy_record_image_digest_t &digest) {
  if (!record_heap_no_is_valid(key, heap_no)) return false;

  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(k_lock_warmcopy_default_target_id)
          .mutex);
  return record_bitmap_set_locked(k_lock_warmcopy_default_target_id, key,
                                  heap_no, &digest, 0, nullptr);
}

bool lock_warmcopy_record_bitmap_set_for_target_for_unit_test(
    uint64_t target_id, const lock_warmcopy_record_shard_key_t &key,
    uint32_t heap_no, const lock_warmcopy_record_image_digest_t &digest) {
  if (!record_heap_no_is_valid(key, heap_no)) return false;

  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  return record_bitmap_set_locked(target_id, key, heap_no, &digest, 0,
                                  nullptr);
}

bool lock_warmcopy_record_bitmap_set_with_image_for_unit_test(
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no,
    const lock_warmcopy_record_image_digest_t &digest, uint32_t heap_offset,
    const std::string &encoded_record_image) {
  if (!record_heap_no_is_valid(key, heap_no) || encoded_record_image.empty()) {
    return false;
  }

  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(k_lock_warmcopy_default_target_id)
          .mutex);
  return record_bitmap_set_locked(k_lock_warmcopy_default_target_id, key,
                                  heap_no, &digest, heap_offset,
                                  &encoded_record_image);
}

bool lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
    uint64_t target_id, const lock_warmcopy_record_shard_key_t &key,
    uint32_t heap_no, const lock_warmcopy_record_image_digest_t &digest,
    uint32_t heap_offset, const std::string &encoded_record_image) {
  if (!record_heap_no_is_valid(key, heap_no) || encoded_record_image.empty()) {
    return false;
  }

  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  return record_bitmap_set_locked(target_id, key, heap_no, &digest,
                                  heap_offset, &encoded_record_image);
}

bool lock_warmcopy_record_bitmap_reset_for_unit_test(
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no) {
  if (!record_heap_no_is_valid(key, heap_no)) return false;

  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(k_lock_warmcopy_default_target_id)
          .mutex);
  return record_bitmap_reset_locked(k_lock_warmcopy_default_target_id, key,
                                    heap_no);
}

bool lock_warmcopy_record_journal_upsert_for_target_for_unit_test(
    uint64_t target_id, uint64_t journal_sequence,
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no,
    const lock_warmcopy_record_image_digest_t &digest, uint32_t heap_offset,
    const std::string &encoded_record_image) {
  if (!record_heap_no_is_valid(key, heap_no) || encoded_record_image.empty()) {
    return false;
  }

  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  return apply_record_journal_delta_locked(
      target_id, journal_sequence, record_journal_delta_kind_t::UPSERT, key,
      heap_no, &digest, heap_offset, &encoded_record_image);
}

bool lock_warmcopy_record_journal_patch_for_target_for_unit_test(
    uint64_t target_id, uint64_t journal_sequence,
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no,
    const lock_warmcopy_record_image_digest_t &digest, uint32_t heap_offset,
    const std::string &encoded_record_image) {
  if (!record_heap_no_is_valid(key, heap_no) || encoded_record_image.empty()) {
    return false;
  }

  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  return apply_record_journal_delta_locked(
      target_id, journal_sequence, record_journal_delta_kind_t::PATCH, key,
      heap_no, &digest, heap_offset, &encoded_record_image);
}

bool lock_warmcopy_record_journal_delete_for_target_for_unit_test(
    uint64_t target_id, uint64_t journal_sequence,
    const lock_warmcopy_record_shard_key_t &key, uint32_t heap_no) {
  if (!record_heap_no_is_valid(key, heap_no)) return false;

  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  return apply_record_journal_delta_locked(
      target_id, journal_sequence, record_journal_delta_kind_t::DELETE, key,
      heap_no, nullptr, 0, nullptr);
}

bool lock_warmcopy_record_mark_unsupported_mutation_for_target_for_unit_test(
    uint64_t target_id, uint64_t journal_sequence,
    const lock_warmcopy_record_shard_key_t &key, const std::string &reason) {
  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  lock_warmcopy_record_shard_state_t &shard =
      find_or_create_record_shard(target_id, key);
  mark_record_shard_invalid_locked(
      &shard, journal_sequence,
      reason.empty() ? "unsupported_mutation" : reason.c_str());
  record_store_partition_for_target(target_id)
      .target_invalid_reason_by_target[target_id] =
      shard.last_diagnostic_reason;
  return true;
}

bool lock_warmcopy_record_shard_snapshot_for_unit_test(
    const lock_warmcopy_record_shard_key_t &key,
    lock_warmcopy_record_shard_snapshot_t *snapshot) {
  return lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      k_lock_warmcopy_default_target_id, key, snapshot);
}

bool lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
    uint64_t target_id, const lock_warmcopy_record_shard_key_t &key,
    lock_warmcopy_record_shard_snapshot_t *snapshot) {
  if (snapshot == nullptr) return false;

  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  const lock_warmcopy_record_shard_map_t *store =
      record_store_for_target_if_exists_locked(target_id);
  if (store == nullptr) return false;
  const auto it = store->find(key);
  if (it == store->end()) return false;

  copy_record_shard_snapshot(it->second, snapshot);
  return true;
}

std::string lock_warmcopy_record_shard_canonical_bytes_for_unit_test(
    const lock_warmcopy_record_shard_key_t &key) {
  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(k_lock_warmcopy_default_target_id)
          .mutex);
  const lock_warmcopy_record_shard_map_t *store =
      record_store_for_target_if_exists_locked(k_lock_warmcopy_default_target_id);
  if (store == nullptr) return std::string();
  const auto it = store->find(key);
  if (it == store->end()) return std::string();

  return record_shard_canonical_bytes_locked(it->second);
}

bool lock_warmcopy_record_store_export_record_payload(std::string *payload,
                                                      uint32_t *lock_count) {
  return lock_warmcopy_record_store_export_record_payload_for_target(
      k_lock_warmcopy_default_target_id, payload, lock_count);
}

bool lock_warmcopy_record_store_export_record_payload_for_target(
    uint64_t target_id, std::string *payload, uint32_t *lock_count) {
  if (payload == nullptr) return false;

  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  const lock_warmcopy_record_shard_map_t *store =
      record_store_for_target_if_exists_locked(target_id);
  return export_record_payload_from_store_locked(target_id, store, payload,
                                                 lock_count, nullptr);
}

void lock_warmcopy_record_store_target_ids(std::vector<uint64_t> *target_ids) {
  if (target_ids == nullptr) return;

  target_ids->clear();
  for (auto &partition : lock_warmcopy_record_store_partitions) {
    std::lock_guard<std::mutex> guard(partition.mutex);
    for (const auto &entry : partition.store_by_target) {
      if (!entry.second.empty()) target_ids->push_back(entry.first);
    }
  }
  std::sort(target_ids->begin(), target_ids->end());
  target_ids->erase(std::unique(target_ids->begin(), target_ids->end()),
                    target_ids->end());
}

bool seed_record_payload_into_store(const std::string &payload,
                                    lock_warmcopy_record_shard_map_t *store,
                                    uint32_t *lock_count);

bool lock_warmcopy_record_store_seed_payload_for_target(
    uint64_t target_id, const std::string &payload, uint32_t *lock_count) {
  lock_warmcopy_record_shard_map_t seeded_store;
  uint32_t seeded_lock_count = 0;
  if (!seed_record_payload_into_store(payload, &seeded_store,
                                      &seeded_lock_count)) {
    if (lock_count != nullptr) *lock_count = 0;
    return false;
  }

  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  auto &partition = record_store_partition_for_target(target_id);
  const bool had_preexisting_state =
      partition.target_invalid_reason_by_target.find(target_id) !=
          partition.target_invalid_reason_by_target.end() ||
      partition.journal_sequence_by_target.find(target_id) !=
          partition.journal_sequence_by_target.end() ||
      partition.expected_delta_sequence_by_target.find(target_id) !=
          partition.expected_delta_sequence_by_target.end() ||
      (record_store_for_target_if_exists_locked(target_id) != nullptr &&
       !record_store_for_target_if_exists_locked(target_id)->empty());
  partition.store_by_target[target_id] = std::move(seeded_store);
  partition.journal_sequence_by_target.erase(target_id);
  partition.expected_delta_sequence_by_target.erase(target_id);
  partition.target_invalid_reason_by_target.erase(target_id);
  if (had_preexisting_state) {
    /*
      A base payload that lands after hook state already exists is still useful,
      but it is not clean. Mark every shard dirty so seal must prove the final
      fence or reject the target.
    */
    mark_record_store_dirty_after_base_seed_locked(
        target_id, &partition.store_by_target[target_id]);
  }
  if (lock_count != nullptr) *lock_count = seeded_lock_count;
  return true;
}

void lock_warmcopy_record_store_clear_for_target(uint64_t target_id) {
  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  auto &partition = record_store_partition_for_target(target_id);
  partition.store_by_target.erase(target_id);
  partition.journal_sequence_by_target.erase(target_id);
  partition.expected_delta_sequence_by_target.erase(target_id);
  partition.target_invalid_reason_by_target.erase(target_id);
}

bool lock_warmcopy_record_store_fence_for_target(
    uint64_t target_id, lock_warmcopy_record_store_fence_t *fence) {
  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  const lock_warmcopy_record_shard_map_t *store =
      record_store_for_target_if_exists_locked(target_id);
  return record_store_fence_for_target_locked(store, fence);
}

bool seed_record_payload_entry_into_store(
    const std::string &payload, size_t *offset,
    lock_warmcopy_record_shard_map_t *store, uint32_t *lock_count) {
  /*
    Seeding parses the same payload shape that resume imports later. Phase-1
    warmcopy may omit record images when the payload carries a stable page LSN;
    that compact form is usable only as a prebuilt artifact guarded by the final
    fence. If later seal has to rebuild bytes from the store, the missing images
    make that path fail closed and fall back to live export.
  */
  lock_warmcopy_record_shard_key_t key;
  uint64_t page_lsn = 0;
  uint32_t page_n_heap = 0;
  uint32_t heap_offsets_len = 0;
  uint32_t record_images_len = 0;
  uint32_t bitmap_len = 0;

  if (offset == nullptr || store == nullptr ||
      !read_u64_le_from_payload(payload, offset, &key.table_id) ||
      !read_u64_le_from_payload(payload, offset, &key.index_id) ||
      !read_u32_le_from_payload(payload, offset, &key.space_id) ||
      !read_u32_le_from_payload(payload, offset, &key.page_no) ||
      !read_u32_le_from_payload(payload, offset, &key.lock_type_mode) ||
      !read_u32_le_from_payload(payload, offset, &key.n_bits) ||
      !read_u64_le_from_payload(payload, offset, &page_lsn) ||
      !read_u32_le_from_payload(payload, offset, &page_n_heap) ||
      !read_u32_le_from_payload(payload, offset, &heap_offsets_len) ||
      !read_u32_le_from_payload(payload, offset, &record_images_len) ||
      !read_u32_le_from_payload(payload, offset, &bitmap_len)) {
    return false;
  }

  if (key.n_bits == 0 || page_n_heap == 0 || bitmap_len == 0 ||
      bitmap_len != record_bitmap_len(key.n_bits)) {
    return false;
  }

  std::string heap_offsets;
  std::string record_images;
  std::string bitmap;
  if (!read_payload_slice(payload, offset, heap_offsets_len, &heap_offsets) ||
      !read_payload_slice(payload, offset, record_images_len, &record_images) ||
      !read_payload_slice(payload, offset, bitmap_len, &bitmap)) {
    return false;
  }

  std::vector<unsigned char> normalized_bitmap(bitmap.begin(), bitmap.end());
  normalize_record_bitmap(&normalized_bitmap, key.n_bits);
  if (normalized_bitmap.size() != bitmap_len) return false;
  for (size_t i = 0; i < normalized_bitmap.size(); ++i) {
    if (normalized_bitmap[i] != static_cast<unsigned char>(bitmap[i])) {
      return false;
    }
  }
  if (store->find(key) != store->end()) {
    return false;
  }

  const uint32_t set_bits = record_bitmap_set_bit_count(normalized_bitmap);
  if (set_bits == 0 || heap_offsets_len != set_bits * 4U) return false;
  const bool compact_stable_page_payload = record_images.empty();

  lock_warmcopy_record_shard_state_t &shard =
      find_or_create_record_shard_in_store(store, key);
  shard.key = key;
  shard.normalized_bitmap = normalized_bitmap;
  shard.record_images.clear();
  shard.page_lsn = page_lsn;
  shard.page_n_heap = page_n_heap;
  shard.set_bit_count = set_bits;
  shard.shard_state_flags = 0;
  shard.missing_record_image_count = 0;
  shard.forced_invalid = false;
  shard.mutation_generation = 1;
  shard.implicit_exclusion_generation = 0;
  shard.journal_cursor = 0;
  shard.last_applied_journal_seq = 0;
  shard.journal_bytes = 0;
  for (uint64_t &word : shard.rolling_fingerprint) word = 0;
  shard.last_diagnostic_reason.clear();

  size_t heap_offsets_offset = 0;
  size_t record_images_offset = 0;
  for (uint32_t heap_no = 0; heap_no < key.n_bits; ++heap_no) {
    const size_t byte_pos = heap_no / 8U;
    const unsigned char bit_mask =
        static_cast<unsigned char>(1U << (heap_no % 8U));
    if ((normalized_bitmap[byte_pos] & bit_mask) == 0) continue;

    uint32_t heap_offset = 0;
    if (!read_u32_le_from_payload(heap_offsets, &heap_offsets_offset,
                                  &heap_offset)) {
      return false;
    }

    if (compact_stable_page_payload) {
      continue;
    }

    std::string encoded_record_image;
    if (!read_encoded_record_image_slot(record_images, &record_images_offset,
                                        &encoded_record_image)) {
      return false;
    }

    lock_warmcopy_record_image_entry_t entry;
    entry.heap_no = heap_no;
    entry.heap_offset = heap_offset;
    entry.encoded_record_image = encoded_record_image;
    SHA_EVP256(reinterpret_cast<const unsigned char *>(
                   encoded_record_image.data()),
               encoded_record_image.size(), entry.digest.bytes);
    shard.record_images[heap_no] = entry;
  }

  if (heap_offsets_offset != heap_offsets.size() ||
      record_images_offset != record_images.size() ||
      (!compact_stable_page_payload && shard.record_images.size() != set_bits)) {
    return false;
  }

  for (const auto &entry : shard.record_images) {
    update_record_shard_rolling_fingerprint_locked(
        &shard, 8, entry.second.heap_no, &entry.second.digest,
        entry.second.heap_offset);
  }

  if (lock_count != nullptr) *lock_count += set_bits;
  return true;
}

bool seed_record_payload_into_store(const std::string &payload,
                                    lock_warmcopy_record_shard_map_t *store,
                                    uint32_t *lock_count) {
  if (store == nullptr) return false;
  store->clear();
  if (lock_count != nullptr) *lock_count = 0;
  if (payload.empty()) return true;

  size_t offset = 0;
  uint32_t entry_count = 0;
  if (!read_u32_le_from_payload(payload, &offset, &entry_count) ||
      entry_count == 0) {
    return false;
  }

  for (uint32_t i = 0; i < entry_count; ++i) {
    if (!seed_record_payload_entry_into_store(payload, &offset, store,
                                              lock_count)) {
      return false;
    }
  }
  return offset == payload.size();
}

bool lock_warmcopy_record_store_fence_equal(
    const lock_warmcopy_record_store_fence_t &lhs,
    const lock_warmcopy_record_store_fence_t &rhs) {
  return lhs.shard_count == rhs.shard_count &&
         lhs.total_mutation_generation == rhs.total_mutation_generation &&
         lhs.dirty_generation == rhs.dirty_generation &&
         std::memcmp(lhs.canonical_fingerprint, rhs.canonical_fingerprint,
                     sizeof(lhs.canonical_fingerprint)) == 0;
}

bool lock_warmcopy_record_store_seal_for_target(
    uint64_t target_id, const lock_warmcopy_record_store_fence_t &phase1_fence,
    uint32_t max_lock_count, uint64_t max_journal_bytes,
    uint32_t max_dirty_shards, lock_warmcopy_record_seal_result_t *result) {
  if (result == nullptr) return false;

  /*
    Seal trusts a phase-1 record mirror only if its fence is unchanged at the
    quiesced boundary. Resource limits are checked before the payload is
    accepted so an oversized mirror does not become a partial artifact.
  */
  *result = lock_warmcopy_record_seal_result_t{};
  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  const lock_warmcopy_record_shard_map_t *store =
      record_store_for_target_if_exists_locked(target_id);
  if (!record_store_metadata_for_target_locked(
          store, &result->seal_fence, &result->journal_bytes,
          &result->dirty_shard_count, nullptr, nullptr)) {
    result->status = lock_warmcopy_record_seal_status_t::TARGET_INVALID;
    result->diagnostic_reason = "record_seal_fence_unavailable";
    return true;
  }

  if (!lock_warmcopy_record_store_fence_equal(phase1_fence,
                                              result->seal_fence)) {
    result->status = lock_warmcopy_record_seal_status_t::SEAL_FENCE_CHANGED;
    result->diagnostic_reason = "record_seal_fence_changed";
    return true;
  }

  if (result->journal_bytes > max_journal_bytes) {
    result->status =
        lock_warmcopy_record_seal_status_t::RESOURCE_LIMIT_EXCEEDED;
    result->diagnostic_reason = "record_journal_budget_exceeded";
    return true;
  }

  if (result->dirty_shard_count > max_dirty_shards) {
    result->status =
        lock_warmcopy_record_seal_status_t::RESOURCE_LIMIT_EXCEEDED;
    result->diagnostic_reason = "record_dirty_shard_limit_exceeded";
    return true;
  }

  if (!export_record_payload_from_store_locked(
          target_id, store, &result->record_locks_payload,
          &result->record_lock_count, &result->scanned_shard_count)) {
    result->record_locks_payload.clear();
    result->record_lock_count = 0;
    result->materialized_payload_bytes = 0;
    result->status = lock_warmcopy_record_seal_status_t::TARGET_INVALID;
    result->diagnostic_reason = "record_payload_invalid";
    return true;
  }
  result->materialized_payload_bytes = result->record_locks_payload.size();

  if (result->record_lock_count > max_lock_count) {
    result->record_locks_payload.clear();
    result->materialized_payload_bytes = 0;
    result->status =
        lock_warmcopy_record_seal_status_t::RESOURCE_LIMIT_EXCEEDED;
    result->diagnostic_reason = "record_lock_count_limit_exceeded";
    return true;
  }

  result->sealed = true;
  result->status = result->record_locks_payload.empty()
                       ? lock_warmcopy_record_seal_status_t::EMPTY
                       : lock_warmcopy_record_seal_status_t::SEALED_VALID;
  return true;
}

bool lock_warmcopy_record_store_seal_metadata_for_target(
    uint64_t target_id, const lock_warmcopy_record_store_fence_t &phase1_fence,
    uint32_t expected_record_lock_count, uint32_t max_lock_count,
    uint64_t max_journal_bytes, uint32_t max_dirty_shards,
    lock_warmcopy_record_seal_result_t *result) {
  if (result == nullptr) return false;

  /*
    Metadata-only seal is used when phase 1 already wrote a durable record-lock
    blob. It rechecks the fence and counts without rebuilding the payload, which
    keeps phase 2 away from O(lock count) materialization.
  */
  *result = lock_warmcopy_record_seal_result_t{};
  std::lock_guard<std::mutex> guard(
      record_store_partition_for_target(target_id).mutex);
  const lock_warmcopy_record_shard_map_t *store =
      record_store_for_target_if_exists_locked(target_id);
  bool lock_count_overflow = false;
  if (!record_store_metadata_for_target_locked(
          store, &result->seal_fence, &result->journal_bytes,
          &result->dirty_shard_count, &result->record_lock_count,
          &lock_count_overflow)) {
    result->status = lock_warmcopy_record_seal_status_t::TARGET_INVALID;
    result->diagnostic_reason = "record_seal_fence_unavailable";
    return true;
  }

  if (!lock_warmcopy_record_store_fence_equal(phase1_fence,
                                              result->seal_fence)) {
    result->status = lock_warmcopy_record_seal_status_t::SEAL_FENCE_CHANGED;
    result->diagnostic_reason = "record_seal_fence_changed";
    return true;
  }

  if (result->journal_bytes > max_journal_bytes) {
    result->status =
        lock_warmcopy_record_seal_status_t::RESOURCE_LIMIT_EXCEEDED;
    result->diagnostic_reason = "record_journal_budget_exceeded";
    return true;
  }

  if (result->dirty_shard_count > max_dirty_shards) {
    result->status =
        lock_warmcopy_record_seal_status_t::RESOURCE_LIMIT_EXCEEDED;
    result->diagnostic_reason = "record_dirty_shard_limit_exceeded";
    return true;
  }

  if (lock_count_overflow ||
      result->record_lock_count != expected_record_lock_count) {
    result->status = lock_warmcopy_record_seal_status_t::SEAL_FENCE_CHANGED;
    result->diagnostic_reason = "record_seal_lock_count_changed";
    return true;
  }

  result->record_lock_count = expected_record_lock_count;
  if (result->record_lock_count > max_lock_count) {
    result->status =
        lock_warmcopy_record_seal_status_t::RESOURCE_LIMIT_EXCEEDED;
    result->diagnostic_reason = "record_lock_count_limit_exceeded";
    return true;
  }

  result->sealed = true;
  result->status = result->record_lock_count == 0
                       ? lock_warmcopy_record_seal_status_t::EMPTY
                       : lock_warmcopy_record_seal_status_t::SEALED_VALID;
  return true;
}

bool lock_warmcopy_trx_lock_fence_sample(
    const trx_lock_t *trx_lock, lock_warmcopy_trx_lock_fence_t *fence) {
  if (trx_lock == nullptr || fence == nullptr) return false;

  /*
    This fence is sampled by callers that already own the required transaction
    or quiesce protection. It cross-checks native lock-list changes and
    implicit-to-explicit conversion attempts observed after a freeze.
  */
  fence->trx_locks_version = trx_lock->trx_locks_version;
  fence->n_rec_locks = trx_lock->n_rec_locks.load(std::memory_order_relaxed);
  fence->freeze_generation = trx_lock->lock_warmcopy_freeze_generation;
  fence->conversion_attempt_after_freeze =
      trx_lock->lock_warmcopy_conversion_attempt_after_freeze;
  fence->conversion_unhandled_after_freeze =
      trx_lock->lock_warmcopy_conversion_unhandled_after_freeze;
  return true;
}

bool lock_warmcopy_trx_lock_fence_equal(
    const lock_warmcopy_trx_lock_fence_t &lhs,
    const lock_warmcopy_trx_lock_fence_t &rhs) {
  return lhs.trx_locks_version == rhs.trx_locks_version &&
         lhs.n_rec_locks == rhs.n_rec_locks &&
         lhs.freeze_generation == rhs.freeze_generation &&
         lhs.conversion_attempt_after_freeze ==
             rhs.conversion_attempt_after_freeze &&
         lhs.conversion_unhandled_after_freeze ==
             rhs.conversion_unhandled_after_freeze;
}

lock_warmcopy_prepare_guard_t *lock_warmcopy_prepare_guard_create() {
  if (lock_sys == nullptr) return nullptr;
  return new lock_warmcopy_prepare_guard_t{};
}

void lock_warmcopy_prepare_guard_destroy(
    lock_warmcopy_prepare_guard_t *guard) {
  delete guard;
}

void lock_warmcopy_trx_conversion_freeze(trx_lock_t *trx_lock,
                                         uint64_t wait_epoch) {
  if (trx_lock == nullptr) return;

  /*
    Freezing closes the implicit-to-explicit conversion window for a target. SQL
    performs a final fence check before prepare, but the freeze itself remains
    active until the preserve path explicitly thaws it after prepare/snapshot
    cleanup has passed the ownership boundary. Other sessions that collide with
    the target must wait, retry, or return their native NOWAIT/SKIP LOCKED
    result, and every conversion attempt marks the fence as changed.
  */
  DEBUG_SYNC_C("lock_warmcopy_conversion_freeze_before_broadcast");
  trx_lock->lock_warmcopy_conversion_frozen = true;
  ++trx_lock->lock_warmcopy_freeze_generation;
  trx_lock->lock_warmcopy_conversion_freeze_wait_epoch = wait_epoch;
  trx_lock->lock_warmcopy_conversion_attempt_after_freeze = false;
  trx_lock->lock_warmcopy_conversion_unhandled_after_freeze = false;
}

void lock_warmcopy_trx_conversion_thaw(trx_lock_t *trx_lock) {
  if (trx_lock == nullptr) return;

  trx_lock->lock_warmcopy_conversion_frozen = false;
  trx_lock->lock_warmcopy_conversion_freeze_wait_epoch = 0;
}

bool lock_warmcopy_trx_conversion_is_frozen(const trx_lock_t *trx_lock) {
  return trx_lock != nullptr && trx_lock->lock_warmcopy_conversion_frozen;
}

bool lock_warmcopy_trx_conversion_note_attempt(trx_lock_t *trx_lock) {
  if (trx_lock == nullptr || !trx_lock->lock_warmcopy_conversion_frozen) {
    return false;
  }

  trx_lock->lock_warmcopy_conversion_attempt_after_freeze = true;
  trx_lock->lock_warmcopy_conversion_unhandled_after_freeze = true;
  return true;
}

bool lock_warmcopy_trx_conversion_note_handled(trx_lock_t *trx_lock) {
  if (trx_lock == nullptr ||
      !trx_lock->lock_warmcopy_conversion_attempt_after_freeze) {
    return false;
  }

  trx_lock->lock_warmcopy_conversion_unhandled_after_freeze = false;
  return true;
}

dberr_t lock_warmcopy_frozen_conversion_result(int sel_mode) {
  switch (sel_mode) {
    case SELECT_SKIP_LOCKED:
      return DB_SKIP_LOCKED;
    case SELECT_NOWAIT:
      return DB_LOCK_NOWAIT;
    case SELECT_ORDINARY:
      return DB_SUCCESS;
  }

  return DB_ERROR;
}

dberr_t lock_warmcopy_wait_for_conversion_thaw(trx_t *trx, THD *wait_thd) {
  lock_warmcopy_conversion_freeze_wait_note();
  return lock_warmcopy_wait_for_conversion_thaw_impl(
      [trx]() {
        trx_mutex_enter(trx);
        const bool frozen =
            lock_warmcopy_trx_conversion_is_frozen(&trx->lock);
        trx_mutex_exit(trx);
        return frozen;
      },
      [trx, wait_thd]() {
        if (srv_shutdown_state.load() != SRV_SHUTDOWN_NONE) return true;
        if (wait_thd != nullptr && thd_killed(wait_thd) != 0) return true;
        return trx != nullptr && trx->mysql_thd != nullptr &&
               thd_killed(trx->mysql_thd) != 0;
      },
      preserve_trx_lock_warmcopy_conversion_wait_timeout_ms);
}

dberr_t lock_warmcopy_wait_for_conversion_thaw_for_unit_test(
    const trx_t *trx, uint timeout_ms) {
  lock_warmcopy_conversion_freeze_wait_note();
  return lock_warmcopy_wait_for_conversion_thaw_impl(
      [trx]() {
        return trx != nullptr &&
               lock_warmcopy_trx_conversion_is_frozen(&trx->lock);
      },
      []() { return false; },
      timeout_ms);
}

dberr_t lock_warmcopy_wait_for_conversion_thaw_abort_for_unit_test(
    const trx_t *trx, uint timeout_ms, bool abort_now) {
  lock_warmcopy_conversion_freeze_wait_note();
  return lock_warmcopy_wait_for_conversion_thaw_impl(
      [trx]() {
        return trx != nullptr &&
               lock_warmcopy_trx_conversion_is_frozen(&trx->lock);
      },
      [abort_now]() { return abort_now; }, timeout_ms);
}

void lock_warmcopy_conversion_freeze_wait_note() {
  lock_warmcopy_conversion_freeze_waits.fetch_add(1,
                                                  std::memory_order_relaxed);
}

uint64_t lock_warmcopy_conversion_freeze_wait_count() {
  return lock_warmcopy_conversion_freeze_waits.load(std::memory_order_relaxed);
}

bool lock_warmcopy_record_store_export_record_payload_for_unit_test(
    std::string *payload) {
  return lock_warmcopy_record_store_export_record_payload(payload, nullptr);
}

bool lock_warmcopy_record_store_export_record_payload_for_target_for_unit_test(
    uint64_t target_id, std::string *payload) {
  return lock_warmcopy_record_store_export_record_payload_for_target(
      target_id, payload, nullptr);
}

void lock_warmcopy_record_store_reset_for_unit_test() {
  for (auto &partition : lock_warmcopy_record_store_partitions) {
    std::lock_guard<std::mutex> guard(partition.mutex);
    partition.store_by_target.clear();
    partition.journal_sequence_by_target.clear();
    partition.expected_delta_sequence_by_target.clear();
    partition.target_invalid_reason_by_target.clear();
  }
}

void lock_warmcopy_trx_conversion_freeze_for_unit_test(
    trx_lock_t *trx_lock, uint64_t wait_epoch) {
  lock_warmcopy_trx_conversion_freeze(trx_lock, wait_epoch);
}

void lock_warmcopy_trx_conversion_thaw_for_unit_test(trx_lock_t *trx_lock) {
  lock_warmcopy_trx_conversion_thaw(trx_lock);
}

bool lock_warmcopy_trx_conversion_is_frozen_for_unit_test(
    const trx_lock_t *trx_lock) {
  return lock_warmcopy_trx_conversion_is_frozen(trx_lock);
}

bool lock_warmcopy_trx_conversion_note_attempt_for_unit_test(
    trx_lock_t *trx_lock) {
  return lock_warmcopy_trx_conversion_note_attempt(trx_lock);
}

bool lock_warmcopy_trx_conversion_note_handled_for_unit_test(
    trx_lock_t *trx_lock) {
  return lock_warmcopy_trx_conversion_note_handled(trx_lock);
}

bool lock_warmcopy_trx_lock_fence_sample_for_unit_test(
    const trx_lock_t *trx_lock, lock_warmcopy_trx_lock_fence_t *fence) {
  return lock_warmcopy_trx_lock_fence_sample(trx_lock, fence);
}

bool lock_warmcopy_trx_lock_fence_equal_for_unit_test(
    const lock_warmcopy_trx_lock_fence_t &lhs,
    const lock_warmcopy_trx_lock_fence_t &rhs) {
  return lock_warmcopy_trx_lock_fence_equal(lhs, rhs);
}

lock_warmcopy_prepare_guard_t *
lock_warmcopy_prepare_guard_create_for_unit_test() {
  return lock_warmcopy_prepare_guard_create();
}

void lock_warmcopy_prepare_guard_destroy_for_unit_test(
    lock_warmcopy_prepare_guard_t *guard) {
  lock_warmcopy_prepare_guard_destroy(guard);
}

const lock_warmcopy_hook_coverage_site_t *
lock_warmcopy_hook_coverage_sites_for_unit_test(size_t *count) {
  if (count != nullptr) {
    *count = sizeof(k_lock_warmcopy_hook_sites) /
             sizeof(k_lock_warmcopy_hook_sites[0]);
  }
  return k_lock_warmcopy_hook_sites;
}
