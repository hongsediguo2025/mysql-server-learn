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

#include "my_config.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <fstream>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "sql/sql_class.h"
#include "sha2.h"
#include "storage/innobase/include/lock0lock.h"
#include "storage/innobase/include/lock0warmcopy.h"
#include "storage/innobase/include/trx0trx.h"
#include "unittest/gunit/benchmark.h"

namespace innodb_lock0warmcopy_unittest {
namespace {

lock_warmcopy_record_shard_key_t make_record_shard_key(uint32_t n_bits) {
  lock_warmcopy_record_shard_key_t key;
  key.table_id = 0x0102030405060708ULL;
  key.index_id = 0x1112131415161718ULL;
  key.space_id = 0x01020304U;
  key.page_no = 0x05060708U;
  key.lock_type_mode = 0x090a0b0cU;
  key.n_bits = n_bits;
  return key;
}

lock_warmcopy_record_image_digest_t make_digest(unsigned char first) {
  lock_warmcopy_record_image_digest_t digest;
  for (size_t i = 0; i < sizeof(digest.bytes); ++i) {
    digest.bytes[i] = static_cast<unsigned char>(first + i);
  }
  return digest;
}

void append_u32(std::string *out, uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

void append_u64(std::string *out, uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

void append_digest(std::string *out,
                   const lock_warmcopy_record_image_digest_t &digest) {
  out->append(reinterpret_cast<const char *>(digest.bytes),
              sizeof(digest.bytes));
}

std::string metadata_payload_sha256_hex(const std::string &payload) {
  unsigned char digest[SHA256_DIGEST_LENGTH]{};
  SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
             payload.size(), digest);
  static constexpr char kHex[] = "0123456789abcdef";
  std::string encoded(SHA256_DIGEST_LENGTH * 2, '0');
  for (size_t i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
    encoded[i * 2] = kHex[digest[i] >> 4];
    encoded[i * 2 + 1] = kHex[digest[i] & 0x0f];
  }
  return encoded;
}

std::string make_encoded_record_image(const std::string &raw_image) {
  std::string image;
  append_u32(&image, static_cast<uint32_t>(raw_image.size()));
  image.append(raw_image);
  return image;
}

std::string make_metadata_record_lock_payload(uint32_t type_mode,
                                              uint32_t page_n_heap,
                                              const std::string &bitmap,
                                              const std::string &record_images =
                                                  std::string()) {
  std::string payload;
  append_u32(&payload, 1);
  append_u64(&payload, 101);
  append_u64(&payload, 202);
  append_u32(&payload, 303);
  append_u32(&payload, 404);
  append_u32(&payload, type_mode);
  append_u32(&payload, static_cast<uint32_t>(bitmap.size() * 8));
  append_u64(&payload, 505);
  append_u32(&payload, page_n_heap);
  uint32_t set_bits = 0;
  for (const unsigned char bitmap_byte : bitmap) {
    for (uint32_t bit = 0; bit < 8; ++bit) {
      if (bitmap_byte & (1U << bit)) ++set_bits;
    }
  }
  append_u32(&payload, set_bits * 4);
  append_u32(&payload, static_cast<uint32_t>(record_images.size()));
  append_u32(&payload, static_cast<uint32_t>(bitmap.size()));
  for (uint32_t i = 0; i < set_bits; ++i) append_u32(&payload, i + 1);
  payload.append(record_images);
  payload.append(bitmap);
  return payload;
}

bool metadata_dict_lease_acquire(
    void *, table_id_t, space_index_t, space_id_t, const std::string &,
    void **opaque_lease, dict_index_t **index) {
  if (opaque_lease == nullptr || index == nullptr) return false;
  *opaque_lease = new uint64_t(1);
  *index = reinterpret_cast<dict_index_t *>(static_cast<uintptr_t>(1));
  return true;
}

std::atomic<bool> metadata_dict_lease_valid{true};
std::atomic<uint64_t> metadata_dict_lease_release_count{0};

bool metadata_dict_lease_revalidate(void *opaque_lease, const std::string &,
                                    dict_index_t **index) {
  if (opaque_lease == nullptr || index == nullptr ||
      !metadata_dict_lease_valid.load(std::memory_order_relaxed)) {
    return false;
  }
  *index = reinterpret_cast<dict_index_t *>(static_cast<uintptr_t>(1));
  return true;
}

void metadata_dict_lease_release(void *opaque_lease) {
  metadata_dict_lease_release_count.fetch_add(1, std::memory_order_relaxed);
  delete static_cast<uint64_t *>(opaque_lease);
}

bool metadata_dict_lease_partial_acquire_failure(
    void *, table_id_t, space_index_t, space_id_t, const std::string &,
    void **opaque_lease, dict_index_t **index) {
  if (opaque_lease == nullptr || index == nullptr) return false;
  *opaque_lease = new uint64_t(1);
  *index = nullptr;
  return false;
}

lock_preserve_metadata_plan_validation_t make_metadata_plan_validation(
    const std::string &payload) {
  lock_preserve_metadata_plan_validation_t validation;
  validation.object_generation = 7;
  validation.expected_object_generation = 7;
  validation.physical_fence_lsn = 505;
  validation.artifact_protocol_version = 1;
  validation.source_server_version = 80022;
  validation.object_digest = metadata_payload_sha256_hex(payload);
  validation.final_lock_generation_digest.assign(64, 'a');
  validation.page_layout_digest.assign(64, 'b');
  validation.dictionary_generation_digest.assign(64, 'c');
  validation.implicit_locks_materialized = true;
  validation.is_final_quiesced = true;
  return validation;
}

lock_preserve_metadata_dict_lease_ops_t make_metadata_dict_lease_ops() {
  lock_preserve_metadata_dict_lease_ops_t ops;
  ops.acquire = metadata_dict_lease_acquire;
  ops.revalidate = metadata_dict_lease_revalidate;
  ops.release = metadata_dict_lease_release;
  return ops;
}

std::string read_source_file(const char *path) {
  std::string root = __FILE__;
  const std::string suffix = "unittest/gunit/innodb/lock0warmcopy-t.cc";
  const size_t suffix_pos = root.rfind(suffix);
  if (suffix_pos != std::string::npos) {
    root.resize(suffix_pos);
  } else {
    root.clear();
  }

  std::ifstream input(root + path);
  if (!input) return std::string();
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

struct BenchmarkRecordHotPathState {
  std::atomic<uint64_t> n_rec_locks{0};
  unsigned char bitmap[8]{};
};

volatile uint64_t lock_warmcopy_benchmark_sink = 0;

inline void benchmark_record_bitmap_set(BenchmarkRecordHotPathState *state,
                                        size_t i) {
  const size_t bit = i & 63U;
  state->bitmap[bit / 8U] =
      static_cast<unsigned char>(state->bitmap[bit / 8U] |
                                 static_cast<unsigned char>(1U << (bit % 8U)));
  state->n_rec_locks.fetch_add(1, std::memory_order_relaxed);
}

inline void benchmark_record_bitmap_set_with_disabled_hook(
    BenchmarkRecordHotPathState *state, size_t i) {
  benchmark_record_bitmap_set(state, i);
  if (lock_warmcopy_hooks_enabled()) {
    lock_warmcopy_record_hook_event();
  }
}

}  // namespace

TEST(LockWarmcopyHooks, DisabledFastPathDoesNotRecordEvents) {
  lock_warmcopy_reset_for_unit_test();

  for (int i = 0; i < 1000; ++i) {
    if (lock_warmcopy_hooks_enabled()) {
      lock_warmcopy_record_hook_event();
    }
  }

  const lock_warmcopy_debug_stats_t stats =
      lock_warmcopy_debug_stats_for_unit_test();
  EXPECT_FALSE(lock_warmcopy_hooks_enabled());
  EXPECT_EQ(0ULL, stats.observed_hook_events);
}

TEST(LockWarmcopyHooks, OpenEpochEnablesSlowPathUntilClosed) {
  lock_warmcopy_reset_for_unit_test();

  lock_warmcopy_open_epoch(7);
  ASSERT_TRUE(lock_warmcopy_hooks_enabled());
  lock_warmcopy_record_hook_event();
  lock_warmcopy_record_hook_event();
  EXPECT_EQ(2ULL,
            lock_warmcopy_debug_stats_for_unit_test().observed_hook_events);

  lock_warmcopy_close_epoch();
  EXPECT_FALSE(lock_warmcopy_hooks_enabled());
  lock_warmcopy_record_hook_event();
  EXPECT_EQ(2ULL,
            lock_warmcopy_debug_stats_for_unit_test().observed_hook_events);
}

TEST(LockWarmcopyRecordImport, MultiBitBitmapBalancesNativeAccounting) {
  lock_warmcopy_reset_for_unit_test();

  constexpr size_t kBitmapLen = 2;
  const byte imported_bitmap[kBitmapLen] = {0x0a, 0x02};  // heaps 1, 3, 9.
  uint64_t count_after_publish = 0;
  uint64_t count_after_reset = 0;
  ASSERT_TRUE(lock_preserve_record_bitmap_accounting_for_unit_test(
      imported_bitmap, kBitmapLen, &count_after_publish, &count_after_reset));
  EXPECT_EQ(3ULL, count_after_publish);
  EXPECT_EQ(0ULL, count_after_reset);
}

TEST(LockWarmcopyMetadataPlan, AcceptsFinalStablePagePayloadWithoutPageIo) {
  constexpr uint32_t kRecordBitmapMargin = 64;
  const uint32_t page_n_heap = 16;
  const size_t bitmap_bytes =
      1 + ((page_n_heap + kRecordBitmapMargin) / 8);
  std::string bitmap(bitmap_bytes, '\0');
  bitmap[0] = static_cast<char>(0x0a);
  const std::string payload = make_metadata_record_lock_payload(
      LOCK_REC | LOCK_X, page_n_heap, bitmap);
  lock_preserve_metadata_plan_t plan;
  EXPECT_EQ(lock_preserve_metadata_plan_status::OK,
            lock_preserve_build_record_lock_metadata_plan(
                payload, make_metadata_plan_validation(payload),
                make_metadata_dict_lease_ops(), &plan));
  EXPECT_TRUE(plan.ready());
  EXPECT_EQ(1U, plan.entry_count());
  EXPECT_EQ(2U, plan.bitmap_bits());
  EXPECT_GT(plan.capacity_bytes(), bitmap.size());
}

TEST(LockWarmcopyMetadataPlan,
     FinalFactsSeparateLockBitsFromPageAndDictionaryIdentity) {
  const std::string first_payload = make_metadata_record_lock_payload(
      LOCK_REC | LOCK_X, 16, std::string(2, '\x02'));
  lock_preserve_record_lock_metadata_facts_t first;
  ASSERT_EQ(lock_preserve_metadata_plan_status::OK,
            lock_preserve_build_record_lock_metadata_facts(first_payload,
                                                           &first));
  EXPECT_EQ(1U, first.bitmap_entries);
  EXPECT_EQ(2U, first.bitmap_bits);
  EXPECT_EQ(1U, first.unique_pages);
  EXPECT_EQ(64U, first.final_lock_generation_digest.size());
  EXPECT_EQ(64U, first.page_layout_digest.size());
  EXPECT_EQ(64U, first.dictionary_generation_digest.size());
  EXPECT_FALSE(first.predicate_lock_present);
  EXPECT_FALSE(first.wait_lock_present);
  EXPECT_FALSE(first.record_image_present);

  const std::string changed_bitmap = make_metadata_record_lock_payload(
      LOCK_REC | LOCK_X, 16, std::string(2, '\x04'));
  lock_preserve_record_lock_metadata_facts_t bitmap_facts;
  ASSERT_EQ(lock_preserve_metadata_plan_status::OK,
            lock_preserve_build_record_lock_metadata_facts(changed_bitmap,
                                                           &bitmap_facts));
  EXPECT_NE(first.final_lock_generation_digest,
            bitmap_facts.final_lock_generation_digest);
  EXPECT_EQ(first.page_layout_digest, bitmap_facts.page_layout_digest);
  EXPECT_EQ(first.dictionary_generation_digest,
            bitmap_facts.dictionary_generation_digest);

  const std::string changed_layout = make_metadata_record_lock_payload(
      LOCK_REC | LOCK_X, 24, std::string(3, '\x02'));
  lock_preserve_record_lock_metadata_facts_t layout_facts;
  ASSERT_EQ(lock_preserve_metadata_plan_status::OK,
            lock_preserve_build_record_lock_metadata_facts(changed_layout,
                                                           &layout_facts));
  EXPECT_NE(first.page_layout_digest, layout_facts.page_layout_digest);
  EXPECT_EQ(first.dictionary_generation_digest,
            layout_facts.dictionary_generation_digest);
}

TEST(LockWarmcopyMetadataPlan, RejectsNonFinalAndColdIdentityPayloads) {
  constexpr uint32_t kRecordBitmapMargin = 64;
  const uint32_t page_n_heap = 16;
  const size_t bitmap_bytes =
      1 + ((page_n_heap + kRecordBitmapMargin) / 8);
  std::string bitmap(bitmap_bytes, '\0');
  bitmap[0] = static_cast<char>(0x02);
  const std::string payload = make_metadata_record_lock_payload(
      LOCK_REC | LOCK_X, page_n_heap, bitmap);
  auto validation = make_metadata_plan_validation(payload);
  validation.is_final_quiesced = false;
  lock_preserve_metadata_plan_t plan;
  EXPECT_EQ(lock_preserve_metadata_plan_status::NOT_FINAL,
            lock_preserve_build_record_lock_metadata_plan(
                payload, validation, make_metadata_dict_lease_ops(), &plan));

  validation = make_metadata_plan_validation(payload);
  validation.implicit_locks_materialized = false;
  EXPECT_EQ(lock_preserve_metadata_plan_status::NOT_FINAL,
            lock_preserve_build_record_lock_metadata_plan(
                payload, validation, make_metadata_dict_lease_ops(), &plan));

  validation = make_metadata_plan_validation(payload);
  validation.expected_object_generation = validation.object_generation + 1;
  EXPECT_EQ(lock_preserve_metadata_plan_status::STALE_GENERATION,
            lock_preserve_build_record_lock_metadata_plan(
                payload, validation, make_metadata_dict_lease_ops(), &plan));

  const std::string image_payload = make_metadata_record_lock_payload(
      LOCK_REC | LOCK_X, page_n_heap, bitmap,
      make_encoded_record_image("cold-record-image"));
  EXPECT_EQ(lock_preserve_metadata_plan_status::CORRUPT_METADATA,
            lock_preserve_build_record_lock_metadata_plan(
                image_payload, make_metadata_plan_validation(image_payload),
                make_metadata_dict_lease_ops(), &plan));
}

TEST(LockWarmcopyMetadataPlan,
     AcceptsPhysicalFenceImplicitContinuityWithoutMaterialization) {
  constexpr uint32_t kRecordBitmapMargin = 64;
  constexpr uint32_t kPageNHeap = 16;
  std::string bitmap(1 + ((kPageNHeap + kRecordBitmapMargin) / 8), '\0');
  bitmap[0] = static_cast<char>(0x02);
  const std::string payload = make_metadata_record_lock_payload(
      LOCK_REC | LOCK_X, kPageNHeap, bitmap);
  auto validation = make_metadata_plan_validation(payload);
  validation.implicit_locks_materialized = false;
  validation.implicit_native_continuity_proven = true;
  lock_preserve_metadata_plan_t plan;
  EXPECT_EQ(lock_preserve_metadata_plan_status::OK,
            lock_preserve_build_record_lock_metadata_plan(
                payload, validation, make_metadata_dict_lease_ops(), &plan));

  validation.implicit_native_continuity_proven = false;
  EXPECT_EQ(lock_preserve_metadata_plan_status::NOT_FINAL,
            lock_preserve_build_record_lock_metadata_plan(
                payload, validation, make_metadata_dict_lease_ops(), &plan));
}

TEST(LockWarmcopyMetadataPlan, RejectsWaitPredicateAndDigestMismatch) {
  constexpr uint32_t kRecordBitmapMargin = 64;
  const uint32_t page_n_heap = 16;
  const size_t bitmap_bytes =
      1 + ((page_n_heap + kRecordBitmapMargin) / 8);
  std::string bitmap(bitmap_bytes, '\0');
  bitmap[0] = static_cast<char>(0x02);
  lock_preserve_metadata_plan_t plan;

  std::string payload = make_metadata_record_lock_payload(
      LOCK_REC | LOCK_X | LOCK_WAIT, page_n_heap, bitmap);
  EXPECT_EQ(lock_preserve_metadata_plan_status::UNSUPPORTED_MODE,
            lock_preserve_build_record_lock_metadata_plan(
                payload, make_metadata_plan_validation(payload),
                make_metadata_dict_lease_ops(), &plan));

  payload = make_metadata_record_lock_payload(
      LOCK_REC | LOCK_X | LOCK_PREDICATE, page_n_heap, bitmap);
  EXPECT_EQ(lock_preserve_metadata_plan_status::UNSUPPORTED_MODE,
            lock_preserve_build_record_lock_metadata_plan(
                payload, make_metadata_plan_validation(payload),
                make_metadata_dict_lease_ops(), &plan));

  payload = make_metadata_record_lock_payload(LOCK_REC | LOCK_X, page_n_heap,
                                              bitmap);
  auto validation = make_metadata_plan_validation(payload);
  validation.object_digest.assign(64, 'f');
  EXPECT_EQ(lock_preserve_metadata_plan_status::DIGEST_MISMATCH,
            lock_preserve_build_record_lock_metadata_plan(
                payload, validation, make_metadata_dict_lease_ops(), &plan));
}

TEST(LockWarmcopyMetadataPlan, DeadlineAndDictLeaseFailBeforePageAccess) {
  constexpr uint32_t kRecordBitmapMargin = 64;
  const uint32_t page_n_heap = 16;
  const size_t bitmap_bytes =
      1 + ((page_n_heap + kRecordBitmapMargin) / 8);
  std::string bitmap(bitmap_bytes, '\0');
  bitmap[0] = static_cast<char>(0x02);
  const std::string payload = make_metadata_record_lock_payload(
      LOCK_REC | LOCK_X, page_n_heap, bitmap);
  lock_preserve_metadata_plan_t plan;
  ASSERT_EQ(lock_preserve_metadata_plan_status::OK,
            lock_preserve_build_record_lock_metadata_plan(
                payload, make_metadata_plan_validation(payload),
                make_metadata_dict_lease_ops(), &plan));

  trx_t trx{};
  EXPECT_EQ(lock_preserve_metadata_conflict_result::DEADLINE_EXCEEDED,
            lock_preserve_check_record_bitmap_conflicts_from_metadata(
                &trx, plan, 1));

  metadata_dict_lease_valid.store(false, std::memory_order_relaxed);
  EXPECT_EQ(lock_preserve_metadata_conflict_result::DICT_LEASE_INVALID,
            lock_preserve_check_record_bitmap_conflicts_from_metadata(
                &trx, plan, 0));
  metadata_dict_lease_valid.store(true, std::memory_order_relaxed);
}

TEST(LockWarmcopyMetadataPlan, PartialDictLeaseAcquireIsReleased) {
  constexpr uint32_t kRecordBitmapMargin = 64;
  const uint32_t page_n_heap = 16;
  const size_t bitmap_bytes =
      1 + ((page_n_heap + kRecordBitmapMargin) / 8);
  std::string bitmap(bitmap_bytes, '\0');
  bitmap[0] = static_cast<char>(0x02);
  const std::string payload = make_metadata_record_lock_payload(
      LOCK_REC | LOCK_X, page_n_heap, bitmap);

  auto ops = make_metadata_dict_lease_ops();
  ops.acquire = metadata_dict_lease_partial_acquire_failure;
  metadata_dict_lease_release_count.store(0, std::memory_order_relaxed);
  lock_preserve_metadata_plan_t plan;
  EXPECT_EQ(lock_preserve_metadata_plan_status::DICT_LEASE_FAILED,
            lock_preserve_build_record_lock_metadata_plan(
                payload, make_metadata_plan_validation(payload), ops, &plan));
  EXPECT_EQ(1U, metadata_dict_lease_release_count.load(
                    std::memory_order_relaxed));
}

TEST(LockWarmcopyRecordShard, SetResetMutationsUpdateGenerationAndBitmap) {
  lock_warmcopy_reset_for_unit_test();

  const lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  const lock_warmcopy_record_image_digest_t digest = make_digest(0x20);

  ASSERT_TRUE(lock_warmcopy_record_bitmap_set_for_unit_test(key, 4, digest));
  ASSERT_TRUE(lock_warmcopy_record_bitmap_reset_for_unit_test(key, 4));

  lock_warmcopy_record_shard_snapshot_t snapshot;
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_unit_test(key, &snapshot));
  ASSERT_EQ(1U, snapshot.normalized_bitmap.size());
  EXPECT_EQ(0U, static_cast<unsigned char>(snapshot.normalized_bitmap[0]));
  EXPECT_EQ(0U, snapshot.set_bit_count);
  EXPECT_EQ(2ULL, snapshot.mutation_generation);
  EXPECT_EQ(0ULL, snapshot.implicit_exclusion_generation);
  EXPECT_TRUE(snapshot.record_images.empty());

  const std::string first_empty_bitmap =
      lock_warmcopy_record_shard_canonical_bytes_for_unit_test(key);

  ASSERT_TRUE(lock_warmcopy_record_bitmap_set_for_unit_test(key, 4, digest));
  ASSERT_TRUE(lock_warmcopy_record_bitmap_reset_for_unit_test(key, 4));
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_unit_test(key, &snapshot));
  EXPECT_EQ(4ULL, snapshot.mutation_generation);

  const std::string second_empty_bitmap =
      lock_warmcopy_record_shard_canonical_bytes_for_unit_test(key);
  EXPECT_NE(first_empty_bitmap, second_empty_bitmap)
      << "generation must change even when the final bitmap is identical";
}

TEST(LockWarmcopyRecordShard, ProductionBitmapHookMarksMissingDigestInvalid) {
  lock_warmcopy_reset_for_unit_test();

  const lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  lock_warmcopy_open_epoch(11);
  ASSERT_TRUE(lock_warmcopy_record_bitmap_set(key, 3));

  lock_warmcopy_record_shard_snapshot_t snapshot;
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_unit_test(key, &snapshot));
  EXPECT_EQ(1U, snapshot.set_bit_count);
  EXPECT_EQ(1ULL, snapshot.mutation_generation);
  EXPECT_TRUE(snapshot.record_images.empty());
  EXPECT_NE(0U,
            snapshot.shard_state_flags & LOCK_WARMCOPY_RECORD_SHARD_INVALID);

  ASSERT_TRUE(lock_warmcopy_record_bitmap_reset(key, 3));
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_unit_test(key, &snapshot));
  EXPECT_EQ(0U, snapshot.set_bit_count);
  EXPECT_EQ(2ULL, snapshot.mutation_generation);
  EXPECT_TRUE(snapshot.record_images.empty());
  EXPECT_EQ(0U,
            snapshot.shard_state_flags & LOCK_WARMCOPY_RECORD_SHARD_INVALID);
}

TEST(LockWarmcopyRecordShard, RejectsOverflowSizedBitmapBeforeStoreMutation) {
  lock_warmcopy_reset_for_unit_test();

  lock_warmcopy_record_shard_key_t key = make_record_shard_key(UINT32_MAX);
  const lock_warmcopy_record_image_digest_t digest = make_digest(0x20);

  EXPECT_FALSE(lock_warmcopy_record_bitmap_set_for_unit_test(key, 0, digest));
  lock_warmcopy_record_shard_snapshot_t snapshot;
  EXPECT_FALSE(lock_warmcopy_record_shard_snapshot_for_unit_test(key, &snapshot));
}

TEST(LockWarmcopyRecordShard,
     ProductionImageHookStoresPayloadUnderOwningThreadId) {
  lock_warmcopy_reset_for_unit_test();

  THD thd(false);
  thd.set_new_thread_id();
  trx_t trx{};
  trx.mysql_thd = &thd;

  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest = make_digest(0x20);
  const std::string image = make_encoded_record_image("record-image");

  lock_warmcopy_open_epoch(12);
  ASSERT_TRUE(lock_warmcopy_record_bitmap_set_with_image_for_trx(
      &trx, key, 5, digest, 5, image));

  lock_warmcopy_record_shard_snapshot_t owner_snapshot;
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      thd.thread_id(), key, &owner_snapshot));
  EXPECT_EQ(1U, owner_snapshot.set_bit_count);
  ASSERT_EQ(1U, owner_snapshot.record_images.size());
  EXPECT_EQ(5U, owner_snapshot.record_images[0].heap_no);
  EXPECT_EQ(5U, owner_snapshot.record_images[0].heap_offset);
  EXPECT_EQ(image, owner_snapshot.record_images[0].encoded_record_image);
  EXPECT_EQ(0U, owner_snapshot.shard_state_flags &
                    LOCK_WARMCOPY_RECORD_SHARD_INVALID);

  lock_warmcopy_record_shard_snapshot_t default_snapshot;
  EXPECT_FALSE(lock_warmcopy_record_shard_snapshot_for_unit_test(
      key, &default_snapshot));

  std::string payload;
  EXPECT_TRUE(
      lock_warmcopy_record_store_export_record_payload_for_target_for_unit_test(
          thd.thread_id(), &payload));
  EXPECT_FALSE(payload.empty());
}

TEST(LockWarmcopyRecordShard, CanonicalBytesAreDeterministicAndLittleEndian) {
  lock_warmcopy_reset_for_unit_test();

  const lock_warmcopy_record_shard_key_t key = make_record_shard_key(10);
  const lock_warmcopy_record_image_digest_t digest1 = make_digest(0x10);
  const lock_warmcopy_record_image_digest_t digest9 = make_digest(0x90);

  ASSERT_TRUE(lock_warmcopy_record_bitmap_set_for_unit_test(key, 9, digest9));
  ASSERT_TRUE(lock_warmcopy_record_bitmap_set_for_unit_test(key, 1, digest1));

  lock_warmcopy_record_shard_snapshot_t snapshot;
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_unit_test(key, &snapshot));
  ASSERT_EQ(2U, snapshot.normalized_bitmap.size());
  EXPECT_EQ(0x02U, static_cast<unsigned char>(snapshot.normalized_bitmap[0]));
  EXPECT_EQ(0x02U, static_cast<unsigned char>(snapshot.normalized_bitmap[1]));
  EXPECT_EQ(2U, snapshot.set_bit_count);
  EXPECT_EQ(2ULL, snapshot.mutation_generation);
  ASSERT_EQ(2U, snapshot.record_images.size());
  EXPECT_EQ(1U, snapshot.record_images[0].heap_no);
  EXPECT_EQ(9U, snapshot.record_images[1].heap_no);

  std::string expected;
  append_u32(&expected, 1);  // canonical_shard_semantic_bytes_v1
  append_u64(&expected, key.table_id);
  append_u64(&expected, key.index_id);
  append_u32(&expected, key.space_id);
  append_u32(&expected, key.page_no);
  append_u32(&expected, key.lock_type_mode);
  append_u32(&expected, key.n_bits);
  append_u64(&expected, 0);  // page_lsn
  append_u32(&expected, 0);  // page_n_heap
  append_u32(&expected, 2);  // bitmap_len
  expected.push_back(static_cast<char>(0x02));
  expected.push_back(static_cast<char>(0x02));
  append_u32(&expected, 2);  // set_bit_count
  append_u32(&expected, 1);
  append_digest(&expected, digest1);
  append_u32(&expected, 9);
  append_digest(&expected, digest9);
  append_u32(&expected, LOCK_WARMCOPY_RECORD_SHARD_DIRTY);
  append_u64(&expected, 2);  // mutation_generation
  append_u64(&expected, 0);  // implicit_exclusion_generation

  EXPECT_EQ(expected,
            lock_warmcopy_record_shard_canonical_bytes_for_unit_test(key));
  EXPECT_EQ(lock_warmcopy_record_shard_canonical_bytes_for_unit_test(key),
            lock_warmcopy_record_shard_canonical_bytes_for_unit_test(key));
}

TEST(LockWarmcopyRecordShard, ExportsImportableRecordPayloadWireFormat) {
  lock_warmcopy_reset_for_unit_test();

  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest2 = make_digest(0x20);
  const lock_warmcopy_record_image_digest_t digest4 = make_digest(0x40);
  const std::string image2 = make_encoded_record_image("rec2");
  const std::string image4 = make_encoded_record_image("rec4");

  ASSERT_TRUE(lock_warmcopy_record_bitmap_set_with_image_for_unit_test(
      key, 4, digest4, 40, image4));
  ASSERT_TRUE(lock_warmcopy_record_bitmap_set_with_image_for_unit_test(
      key, 2, digest2, 20, image2));

  std::string payload;
  ASSERT_TRUE(
      lock_warmcopy_record_store_export_record_payload_for_unit_test(&payload));

  std::string expected;
  append_u32(&expected, 1);  // entry count
  append_u64(&expected, key.table_id);
  append_u64(&expected, key.index_id);
  append_u32(&expected, key.space_id);
  append_u32(&expected, key.page_no);
  append_u32(&expected, key.lock_type_mode);
  append_u32(&expected, key.n_bits);
  append_u64(&expected, 0);           // page_lsn unavailable in unit store
  append_u32(&expected, key.n_bits);  // conservative page_n_heap placeholder
  append_u32(&expected, 8);           // heap_offsets length
  append_u32(&expected,
             static_cast<uint32_t>(image2.size() + image4.size()));
  append_u32(&expected, 1);  // bitmap length
  append_u32(&expected, 20);
  append_u32(&expected, 40);
  expected.append(image2);
  expected.append(image4);
  expected.push_back(static_cast<char>(0x14));  // heap bits 2 and 4

  EXPECT_EQ(expected, payload);
}

TEST(LockWarmcopyRecordShard, BaseSeedPayloadExportsAsSealedStorePayload) {
  lock_warmcopy_reset_for_unit_test();

  const uint64_t source_target_id = 3131;
  const uint64_t seeded_target_id = 3132;
  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest2 = make_digest(0x20);
  const lock_warmcopy_record_image_digest_t digest4 = make_digest(0x40);

  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          source_target_id, key, 2, digest2, 20,
          make_encoded_record_image("seed-rec2")));
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          source_target_id, key, 4, digest4, 40,
          make_encoded_record_image("seed-rec4")));

  std::string source_payload;
  uint32_t source_lock_count = 0;
  ASSERT_TRUE(lock_warmcopy_record_store_export_record_payload_for_target(
      source_target_id, &source_payload, &source_lock_count));
  ASSERT_EQ(2U, source_lock_count);

  uint32_t seeded_lock_count = 0;
  ASSERT_TRUE(lock_warmcopy_record_store_seed_payload_for_target(
      seeded_target_id, source_payload, &seeded_lock_count));
  EXPECT_EQ(2U, seeded_lock_count);

  std::string seeded_payload;
  uint32_t exported_seeded_lock_count = 0;
  ASSERT_TRUE(lock_warmcopy_record_store_export_record_payload_for_target(
      seeded_target_id, &seeded_payload, &exported_seeded_lock_count));
  EXPECT_EQ(2U, exported_seeded_lock_count);
  EXPECT_EQ(source_payload, seeded_payload);

  lock_warmcopy_record_store_fence_t phase1_fence;
  ASSERT_TRUE(lock_warmcopy_record_store_fence_for_target(seeded_target_id,
                                                          &phase1_fence));

  lock_warmcopy_record_seal_result_t result;
  ASSERT_TRUE(lock_warmcopy_record_store_seal_for_target(
      seeded_target_id, phase1_fence, UINT32_MAX, UINT64_MAX, UINT32_MAX,
      &result));
  EXPECT_EQ(lock_warmcopy_record_seal_status_t::SEALED_VALID, result.status);
  EXPECT_TRUE(result.sealed);
  EXPECT_EQ(2U, result.record_lock_count);
  EXPECT_EQ(source_payload, result.record_locks_payload);
  EXPECT_EQ(1U, result.scanned_shard_count);
  EXPECT_EQ(source_payload.size(), result.materialized_payload_bytes);
}

TEST(LockWarmcopyRecordShard, BaseSeedPayloadCanSealMetadataWithoutPayload) {
  lock_warmcopy_reset_for_unit_test();

  const uint64_t source_target_id = 3134;
  const uint64_t seeded_target_id = 3135;
  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest2 = make_digest(0x20);
  const lock_warmcopy_record_image_digest_t digest4 = make_digest(0x40);

  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          source_target_id, key, 2, digest2, 20,
          make_encoded_record_image("seed-rec2")));
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          source_target_id, key, 4, digest4, 40,
          make_encoded_record_image("seed-rec4")));

  std::string source_payload;
  uint32_t source_lock_count = 0;
  ASSERT_TRUE(lock_warmcopy_record_store_export_record_payload_for_target(
      source_target_id, &source_payload, &source_lock_count));
  ASSERT_EQ(2U, source_lock_count);
  ASSERT_FALSE(source_payload.empty());

  uint32_t seeded_lock_count = 0;
  ASSERT_TRUE(lock_warmcopy_record_store_seed_payload_for_target(
      seeded_target_id, source_payload, &seeded_lock_count));
  ASSERT_EQ(2U, seeded_lock_count);

  lock_warmcopy_record_store_fence_t phase1_fence;
  ASSERT_TRUE(lock_warmcopy_record_store_fence_for_target(seeded_target_id,
                                                          &phase1_fence));

  lock_warmcopy_record_seal_result_t result;
  ASSERT_TRUE(lock_warmcopy_record_store_seal_metadata_for_target(
      seeded_target_id, phase1_fence, seeded_lock_count, UINT32_MAX,
      UINT64_MAX, UINT32_MAX, &result));
  EXPECT_EQ(lock_warmcopy_record_seal_status_t::SEALED_VALID, result.status);
  EXPECT_TRUE(result.sealed);
  EXPECT_EQ(2U, result.record_lock_count);
  EXPECT_TRUE(result.record_locks_payload.empty());
  EXPECT_EQ(0U, result.scanned_shard_count);
  EXPECT_EQ(0ULL, result.materialized_payload_bytes);
}

TEST(LockWarmcopyRecordShard, BaseSeedRejectsMalformedPayloadWithoutStore) {
  lock_warmcopy_reset_for_unit_test();

  const uint64_t target_id = 3133;
  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest = make_digest(0x20);
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          target_id, key, 2, digest, 20, make_encoded_record_image("rec2")));

  std::string payload;
  ASSERT_TRUE(lock_warmcopy_record_store_export_record_payload_for_target(
      target_id, &payload, nullptr));
  ASSERT_FALSE(payload.empty());
  payload.pop_back();

  lock_warmcopy_record_store_clear_for_target(target_id);
  uint32_t seeded_lock_count = 999;
  EXPECT_FALSE(lock_warmcopy_record_store_seed_payload_for_target(
      target_id, payload, &seeded_lock_count));
  EXPECT_EQ(0U, seeded_lock_count);

  std::string after_payload;
  uint32_t after_lock_count = 999;
  ASSERT_TRUE(lock_warmcopy_record_store_export_record_payload_for_target(
      target_id, &after_payload, &after_lock_count));
  EXPECT_TRUE(after_payload.empty());
  EXPECT_EQ(0U, after_lock_count);
}

TEST(LockWarmcopyRecordShard, BaseSeedAfterDeltaMarksDirtyShard) {
  lock_warmcopy_reset_for_unit_test();

  const uint64_t source_target_id = 3134;
  const uint64_t dirty_target_id = 3135;
  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest = make_digest(0x20);

  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          source_target_id, key, 2, digest, 20,
          make_encoded_record_image("source-rec")));
  std::string source_payload;
  ASSERT_TRUE(lock_warmcopy_record_store_export_record_payload_for_target(
      source_target_id, &source_payload, nullptr));
  ASSERT_FALSE(source_payload.empty());

  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          dirty_target_id, key, 4, digest, 40,
          make_encoded_record_image("phase1-delta")));

  uint32_t seeded_lock_count = 0;
  ASSERT_TRUE(lock_warmcopy_record_store_seed_payload_for_target(
      dirty_target_id, source_payload, &seeded_lock_count));
  EXPECT_EQ(1U, seeded_lock_count);

  lock_warmcopy_record_store_fence_t phase1_fence;
  ASSERT_TRUE(lock_warmcopy_record_store_fence_for_target(dirty_target_id,
                                                          &phase1_fence));

  lock_warmcopy_record_seal_result_t result;
  ASSERT_TRUE(lock_warmcopy_record_store_seal_for_target(
      dirty_target_id, phase1_fence, UINT32_MAX, UINT64_MAX, 0,
      &result));
  EXPECT_EQ(lock_warmcopy_record_seal_status_t::RESOURCE_LIMIT_EXCEEDED,
            result.status);
  EXPECT_FALSE(result.sealed);
  EXPECT_TRUE(result.record_locks_payload.empty());
  EXPECT_EQ("record_dirty_shard_limit_exceeded", result.diagnostic_reason);

  ASSERT_TRUE(lock_warmcopy_record_store_seal_for_target(
      dirty_target_id, phase1_fence, UINT32_MAX, UINT64_MAX, UINT32_MAX,
      &result));
  EXPECT_EQ(lock_warmcopy_record_seal_status_t::SEALED_VALID, result.status);
  EXPECT_TRUE(result.sealed);
  EXPECT_EQ(1U, result.record_lock_count);
  EXPECT_EQ(source_payload, result.record_locks_payload);
}

TEST(LockWarmcopyRecordShard, PerTargetStoresAreIsolated) {
  lock_warmcopy_reset_for_unit_test();

  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest2 = make_digest(0x20);
  const lock_warmcopy_record_image_digest_t digest4 = make_digest(0x40);

  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          101, key, 2, digest2, 20, make_encoded_record_image("target101")));
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          202, key, 4, digest4, 40, make_encoded_record_image("target202")));

  lock_warmcopy_record_shard_snapshot_t target101_snapshot;
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      101, key, &target101_snapshot));
  EXPECT_EQ(1U, target101_snapshot.set_bit_count);
  EXPECT_EQ(0x04U,
            static_cast<unsigned char>(
                target101_snapshot.normalized_bitmap[0]));
  ASSERT_EQ(1U, target101_snapshot.record_images.size());
  EXPECT_EQ(2U, target101_snapshot.record_images[0].heap_no);

  lock_warmcopy_record_shard_snapshot_t target202_snapshot;
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      202, key, &target202_snapshot));
  EXPECT_EQ(1U, target202_snapshot.set_bit_count);
  EXPECT_EQ(0x10U,
            static_cast<unsigned char>(
                target202_snapshot.normalized_bitmap[0]));
  ASSERT_EQ(1U, target202_snapshot.record_images.size());
  EXPECT_EQ(4U, target202_snapshot.record_images[0].heap_no);

  std::string target101_payload;
  std::string target202_payload;
  ASSERT_TRUE(
      lock_warmcopy_record_store_export_record_payload_for_target_for_unit_test(
          101, &target101_payload));
  ASSERT_TRUE(
      lock_warmcopy_record_store_export_record_payload_for_target_for_unit_test(
          202, &target202_payload));
  EXPECT_NE(target101_payload, target202_payload);

  std::string default_target_payload;
  ASSERT_TRUE(lock_warmcopy_record_store_export_record_payload_for_unit_test(
      &default_target_payload));
  EXPECT_TRUE(default_target_payload.empty());
}

TEST(LockWarmcopyRecordShard, ConcurrentTargetStoresRemainIsolated) {
  lock_warmcopy_reset_for_unit_test();

  constexpr size_t kThreadCount = 8;
  constexpr uint32_t kLocksPerTarget = 6;
  std::atomic<bool> ok{true};
  std::vector<std::thread> threads;
  threads.reserve(kThreadCount);

  for (size_t i = 0; i < kThreadCount; ++i) {
    threads.emplace_back([i, &ok]() {
      const uint64_t target_id = 1000 + i;
      lock_warmcopy_record_shard_key_t key = make_record_shard_key(32);
      key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
      key.page_no += static_cast<uint32_t>(i);
      for (uint32_t heap_no = 1; heap_no <= kLocksPerTarget; ++heap_no) {
        const lock_warmcopy_record_image_digest_t digest =
            make_digest(static_cast<unsigned char>(0x20 + i + heap_no));
        if (!lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
                target_id, key, heap_no, digest, heap_no * 10,
                make_encoded_record_image("concurrent-target"))) {
          ok.store(false, std::memory_order_relaxed);
          return;
        }
      }
    });
  }

  for (std::thread &thread : threads) {
    thread.join();
  }
  ASSERT_TRUE(ok.load(std::memory_order_relaxed));

  for (size_t i = 0; i < kThreadCount; ++i) {
    const uint64_t target_id = 1000 + i;
    std::string payload;
    uint32_t lock_count = 0;
    ASSERT_TRUE(lock_warmcopy_record_store_export_record_payload_for_target(
        target_id, &payload, &lock_count));
    EXPECT_EQ(kLocksPerTarget, lock_count);
    EXPECT_FALSE(payload.empty());
  }
}

TEST(LockWarmcopyRecordShard,
     TargetFenceDetectsGenerationChangeWhenBitmapReturnsToSameState) {
  lock_warmcopy_reset_for_unit_test();

  const uint64_t target_id = 101;
  const lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  const lock_warmcopy_record_image_digest_t digest = make_digest(0x20);

  ASSERT_TRUE(lock_warmcopy_record_bitmap_set_for_target_for_unit_test(
      target_id, key, 4, digest));

  lock_warmcopy_record_store_fence_t first_fence;
  ASSERT_TRUE(lock_warmcopy_record_store_fence_for_target(target_id,
                                                          &first_fence));
  EXPECT_EQ(1U, first_fence.shard_count);
  EXPECT_EQ(1ULL, first_fence.total_mutation_generation);

  ASSERT_TRUE(lock_warmcopy_record_bitmap_set_for_target_for_unit_test(
      target_id, key, 4, digest));

  lock_warmcopy_record_store_fence_t second_fence;
  ASSERT_TRUE(lock_warmcopy_record_store_fence_for_target(target_id,
                                                          &second_fence));
  EXPECT_EQ(1U, second_fence.shard_count);
  EXPECT_EQ(2ULL, second_fence.total_mutation_generation);
  EXPECT_FALSE(
      lock_warmcopy_record_store_fence_equal(first_fence, second_fence));
  EXPECT_NE(std::string(reinterpret_cast<const char *>(
                            first_fence.canonical_fingerprint),
                        sizeof(first_fence.canonical_fingerprint)),
            std::string(reinterpret_cast<const char *>(
                            second_fence.canonical_fingerprint),
                        sizeof(second_fence.canonical_fingerprint)));
}

TEST(LockWarmcopyRecordShard, JournalCursorIsPerTargetAndAdvancesWithMutations) {
  lock_warmcopy_reset_for_unit_test();

  const uint64_t target101 = 101;
  const uint64_t target202 = 202;
  const lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  const lock_warmcopy_record_image_digest_t digest = make_digest(0x20);

  ASSERT_TRUE(lock_warmcopy_record_bitmap_set_for_target_for_unit_test(
      target101, key, 1, digest));
  ASSERT_TRUE(lock_warmcopy_record_bitmap_set_for_target_for_unit_test(
      target202, key, 1, digest));

  lock_warmcopy_record_shard_snapshot_t target101_snapshot;
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      target101, key, &target101_snapshot));
  EXPECT_EQ(1ULL, target101_snapshot.journal_cursor);

  lock_warmcopy_record_shard_snapshot_t target202_snapshot;
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      target202, key, &target202_snapshot));
  EXPECT_EQ(1ULL, target202_snapshot.journal_cursor);

  ASSERT_TRUE(lock_warmcopy_record_bitmap_set_for_target_for_unit_test(
      target101, key, 2, digest));
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      target101, key, &target101_snapshot));
  EXPECT_EQ(2ULL, target101_snapshot.journal_cursor);

  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      target202, key, &target202_snapshot));
  EXPECT_EQ(1ULL, target202_snapshot.journal_cursor);
}

TEST(LockWarmcopyRecordSeal, StableFenceExportsPayload) {
  lock_warmcopy_reset_for_unit_test();

  const uint64_t target_id = 808;
  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest = make_digest(0x20);
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          target_id, key, 2, digest, 20, make_encoded_record_image("rec2")));

  lock_warmcopy_record_store_fence_t phase1_fence;
  ASSERT_TRUE(lock_warmcopy_record_store_fence_for_target(target_id,
                                                          &phase1_fence));

  lock_warmcopy_record_seal_result_t result;
  ASSERT_TRUE(lock_warmcopy_record_store_seal_for_target(
      target_id, phase1_fence, UINT32_MAX, UINT64_MAX, UINT32_MAX, &result));
  EXPECT_EQ(lock_warmcopy_record_seal_status_t::SEALED_VALID, result.status);
  EXPECT_TRUE(result.sealed);
  EXPECT_EQ(1U, result.record_lock_count);
  EXPECT_FALSE(result.record_locks_payload.empty());
  EXPECT_TRUE(lock_warmcopy_record_store_fence_equal(phase1_fence,
                                                     result.seal_fence));
  EXPECT_TRUE(result.diagnostic_reason.empty());
}

TEST(LockWarmcopyRecordSeal, FenceChangeFailsClosedBeforePayloadExport) {
  lock_warmcopy_reset_for_unit_test();

  const uint64_t target_id = 909;
  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest = make_digest(0x20);
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          target_id, key, 2, digest, 20, make_encoded_record_image("rec2")));

  lock_warmcopy_record_store_fence_t phase1_fence;
  ASSERT_TRUE(lock_warmcopy_record_store_fence_for_target(target_id,
                                                          &phase1_fence));
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          target_id, key, 3, digest, 30, make_encoded_record_image("rec3")));

  lock_warmcopy_record_seal_result_t result;
  ASSERT_TRUE(lock_warmcopy_record_store_seal_for_target(
      target_id, phase1_fence, UINT32_MAX, UINT64_MAX, UINT32_MAX, &result));
  EXPECT_EQ(lock_warmcopy_record_seal_status_t::SEAL_FENCE_CHANGED,
            result.status);
  EXPECT_FALSE(result.sealed);
  EXPECT_TRUE(result.record_locks_payload.empty());
  EXPECT_EQ(0U, result.record_lock_count);
  EXPECT_EQ("record_seal_fence_changed", result.diagnostic_reason);
}

TEST(LockWarmcopyRecordSeal, MissingRecordImageInvalidatesTarget) {
  lock_warmcopy_reset_for_unit_test();

  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  lock_warmcopy_open_epoch(1001);
  ASSERT_TRUE(lock_warmcopy_record_bitmap_set(key, 2));

  lock_warmcopy_record_store_fence_t phase1_fence;
  ASSERT_TRUE(lock_warmcopy_record_store_fence_for_target(0, &phase1_fence));

  lock_warmcopy_record_seal_result_t result;
  ASSERT_TRUE(lock_warmcopy_record_store_seal_for_target(
      0, phase1_fence, UINT32_MAX, UINT64_MAX, UINT32_MAX, &result));
  EXPECT_EQ(lock_warmcopy_record_seal_status_t::TARGET_INVALID,
            result.status);
  EXPECT_FALSE(result.sealed);
  EXPECT_TRUE(result.record_locks_payload.empty());
  EXPECT_EQ("record_payload_invalid", result.diagnostic_reason);
}

TEST(LockWarmcopyRecordSeal, LockCountLimitRejectsOversizedPayload) {
  lock_warmcopy_reset_for_unit_test();

  const uint64_t target_id = 1112;
  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest2 = make_digest(0x20);
  const lock_warmcopy_record_image_digest_t digest4 = make_digest(0x40);
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          target_id, key, 2, digest2, 20, make_encoded_record_image("rec2")));
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          target_id, key, 4, digest4, 40, make_encoded_record_image("rec4")));

  lock_warmcopy_record_store_fence_t phase1_fence;
  ASSERT_TRUE(lock_warmcopy_record_store_fence_for_target(target_id,
                                                          &phase1_fence));

  lock_warmcopy_record_seal_result_t result;
  ASSERT_TRUE(lock_warmcopy_record_store_seal_for_target(target_id,
                                                         phase1_fence, 1,
                                                         UINT64_MAX,
                                                         UINT32_MAX,
                                                         &result));
  EXPECT_EQ(lock_warmcopy_record_seal_status_t::RESOURCE_LIMIT_EXCEEDED,
            result.status);
  EXPECT_FALSE(result.sealed);
  EXPECT_TRUE(result.record_locks_payload.empty());
  EXPECT_EQ(2U, result.record_lock_count);
  EXPECT_EQ("record_lock_count_limit_exceeded", result.diagnostic_reason);
}

TEST(LockWarmcopyRecordSeal, JournalBudgetRejectsOversizedJournal) {
  lock_warmcopy_reset_for_unit_test();

  const uint64_t target_id = 1113;
  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest = make_digest(0x30);
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          target_id, key, 2, digest, 20, make_encoded_record_image("rec2")));

  lock_warmcopy_record_store_fence_t phase1_fence;
  ASSERT_TRUE(lock_warmcopy_record_store_fence_for_target(target_id,
                                                          &phase1_fence));

  lock_warmcopy_record_seal_result_t result;
  ASSERT_TRUE(lock_warmcopy_record_store_seal_for_target(
      target_id, phase1_fence, UINT32_MAX, 1, UINT32_MAX, &result));
  EXPECT_EQ(lock_warmcopy_record_seal_status_t::RESOURCE_LIMIT_EXCEEDED,
            result.status);
  EXPECT_FALSE(result.sealed);
  EXPECT_TRUE(result.record_locks_payload.empty());
  EXPECT_EQ(0U, result.record_lock_count);
  EXPECT_EQ("record_journal_budget_exceeded", result.diagnostic_reason);
}

TEST(LockWarmcopyRecordSeal, DirtyShardBudgetRejectsOversizedDirtySet) {
  lock_warmcopy_reset_for_unit_test();

  const uint64_t target_id = 1114;
  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest = make_digest(0x40);
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          target_id, key, 2, digest, 20, make_encoded_record_image("rec2")));

  lock_warmcopy_record_store_fence_t phase1_fence;
  ASSERT_TRUE(lock_warmcopy_record_store_fence_for_target(target_id,
                                                          &phase1_fence));

  lock_warmcopy_record_seal_result_t result;
  ASSERT_TRUE(lock_warmcopy_record_store_seal_for_target(
      target_id, phase1_fence, UINT32_MAX, UINT64_MAX, 0, &result));
  EXPECT_EQ(lock_warmcopy_record_seal_status_t::RESOURCE_LIMIT_EXCEEDED,
            result.status);
  EXPECT_FALSE(result.sealed);
  EXPECT_TRUE(result.record_locks_payload.empty());
  EXPECT_EQ(0U, result.record_lock_count);
  EXPECT_EQ("record_dirty_shard_limit_exceeded", result.diagnostic_reason);
}

TEST(LockWarmcopyRecordJournalDelta,
     UpsertPatchDeleteAndResurrectUpdatesObjectState) {
  lock_warmcopy_reset_for_unit_test();

  const uint64_t target_id = 404;
  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest20 = make_digest(0x20);
  const lock_warmcopy_record_image_digest_t digest40 = make_digest(0x40);
  const lock_warmcopy_record_image_digest_t digest60 = make_digest(0x60);

  ASSERT_TRUE(lock_warmcopy_record_journal_upsert_for_target_for_unit_test(
      target_id, 1, key, 4, digest20, 20,
      make_encoded_record_image("insert-v1")));

  lock_warmcopy_record_shard_snapshot_t snapshot;
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      target_id, key, &snapshot));
  EXPECT_EQ(1U, snapshot.set_bit_count);
  ASSERT_EQ(1U, snapshot.record_images.size());
  EXPECT_EQ(4U, snapshot.record_images[0].heap_no);
  EXPECT_EQ(20U, snapshot.record_images[0].heap_offset);
  EXPECT_EQ(1ULL, snapshot.mutation_generation);
  EXPECT_EQ(1ULL, snapshot.journal_cursor);
  EXPECT_EQ(1ULL, snapshot.last_applied_journal_seq);
  EXPECT_EQ(0U,
            snapshot.shard_state_flags & LOCK_WARMCOPY_RECORD_SHARD_TOMBSTONE);
  EXPECT_TRUE(snapshot.last_diagnostic_reason.empty());

  ASSERT_TRUE(lock_warmcopy_record_journal_patch_for_target_for_unit_test(
      target_id, 2, key, 4, digest40, 40,
      make_encoded_record_image("patch-v2")));
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      target_id, key, &snapshot));
  EXPECT_EQ(1U, snapshot.set_bit_count);
  ASSERT_EQ(1U, snapshot.record_images.size());
  EXPECT_EQ(4U, snapshot.record_images[0].heap_no);
  EXPECT_EQ(40U, snapshot.record_images[0].heap_offset);
  EXPECT_EQ(0, std::memcmp(snapshot.record_images[0].digest.bytes,
                           digest40.bytes, sizeof(digest40.bytes)));
  EXPECT_EQ(2ULL, snapshot.mutation_generation);
  EXPECT_EQ(2ULL, snapshot.journal_cursor);
  EXPECT_EQ(2ULL, snapshot.last_applied_journal_seq);

  ASSERT_TRUE(lock_warmcopy_record_journal_delete_for_target_for_unit_test(
      target_id, 3, key, 4));
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      target_id, key, &snapshot));
  EXPECT_EQ(0U, snapshot.set_bit_count);
  EXPECT_TRUE(snapshot.record_images.empty());
  EXPECT_NE(0U,
            snapshot.shard_state_flags & LOCK_WARMCOPY_RECORD_SHARD_TOMBSTONE);
  EXPECT_EQ(3ULL, snapshot.mutation_generation);
  EXPECT_EQ(3ULL, snapshot.journal_cursor);
  EXPECT_EQ(3ULL, snapshot.last_applied_journal_seq);

  ASSERT_TRUE(lock_warmcopy_record_journal_upsert_for_target_for_unit_test(
      target_id, 4, key, 4, digest60, 60,
      make_encoded_record_image("insert-v3")));
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      target_id, key, &snapshot));
  EXPECT_EQ(1U, snapshot.set_bit_count);
  ASSERT_EQ(1U, snapshot.record_images.size());
  EXPECT_EQ(60U, snapshot.record_images[0].heap_offset);
  EXPECT_EQ(0U,
            snapshot.shard_state_flags & LOCK_WARMCOPY_RECORD_SHARD_TOMBSTONE);
  EXPECT_EQ(4ULL, snapshot.mutation_generation);
  EXPECT_EQ(4ULL, snapshot.journal_cursor);
  EXPECT_EQ(4ULL, snapshot.last_applied_journal_seq);
}

TEST(LockWarmcopyRecordJournalDelta,
     RejectsStalePatchAndDuplicateDeleteWithoutMutation) {
  lock_warmcopy_reset_for_unit_test();

  const uint64_t target_id = 505;
  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest20 = make_digest(0x20);
  const lock_warmcopy_record_image_digest_t digest40 = make_digest(0x40);

  ASSERT_TRUE(lock_warmcopy_record_journal_upsert_for_target_for_unit_test(
      target_id, 1, key, 2, digest20, 20,
      make_encoded_record_image("insert")));

  lock_warmcopy_record_shard_snapshot_t before_stale;
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      target_id, key, &before_stale));

  EXPECT_FALSE(lock_warmcopy_record_journal_patch_for_target_for_unit_test(
      target_id, 1, key, 2, digest40, 40,
      make_encoded_record_image("stale-patch")));

  lock_warmcopy_record_shard_snapshot_t after_stale;
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      target_id, key, &after_stale));
  EXPECT_EQ(before_stale.mutation_generation, after_stale.mutation_generation);
  EXPECT_EQ(before_stale.journal_cursor, after_stale.journal_cursor);
  ASSERT_EQ(1U, after_stale.record_images.size());
  EXPECT_EQ(20U, after_stale.record_images[0].heap_offset);

  ASSERT_TRUE(lock_warmcopy_record_journal_delete_for_target_for_unit_test(
      target_id, 2, key, 2));
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      target_id, key, &before_stale));
  EXPECT_NE(0U,
            before_stale.shard_state_flags &
                LOCK_WARMCOPY_RECORD_SHARD_TOMBSTONE);

  EXPECT_FALSE(lock_warmcopy_record_journal_delete_for_target_for_unit_test(
      target_id, 2, key, 2));
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      target_id, key, &after_stale));
  EXPECT_EQ(before_stale.mutation_generation, after_stale.mutation_generation);
  EXPECT_EQ(before_stale.journal_cursor, after_stale.journal_cursor);
  EXPECT_NE(0U,
            after_stale.shard_state_flags &
                LOCK_WARMCOPY_RECORD_SHARD_TOMBSTONE);
}

TEST(LockWarmcopyRecordJournalDelta, SeqGapInvalidatesRecordFamily) {
  lock_warmcopy_reset_for_unit_test();

  const uint64_t target_id = 606;
  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest20 = make_digest(0x20);
  const lock_warmcopy_record_image_digest_t digest40 = make_digest(0x40);

  ASSERT_TRUE(lock_warmcopy_record_journal_upsert_for_target_for_unit_test(
      target_id, 1, key, 1, digest20, 10,
      make_encoded_record_image("insert")));
  EXPECT_FALSE(lock_warmcopy_record_journal_patch_for_target_for_unit_test(
      target_id, 3, key, 1, digest40, 40,
      make_encoded_record_image("gap")));

  lock_warmcopy_record_shard_snapshot_t snapshot;
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      target_id, key, &snapshot));
  EXPECT_NE(0U,
            snapshot.shard_state_flags & LOCK_WARMCOPY_RECORD_SHARD_INVALID);
  EXPECT_EQ(2ULL, snapshot.mutation_generation);
  EXPECT_EQ(3ULL, snapshot.journal_cursor);
  EXPECT_EQ(1ULL, snapshot.last_applied_journal_seq);
  EXPECT_EQ("journal_seq_gap", snapshot.last_diagnostic_reason);

  std::string payload;
  EXPECT_FALSE(
      lock_warmcopy_record_store_export_record_payload_for_target_for_unit_test(
          target_id, &payload));
  EXPECT_TRUE(payload.empty());
}

TEST(LockWarmcopyRecordJournalDelta, UnsupportedMutationInvalidatesFamily) {
  lock_warmcopy_reset_for_unit_test();

  const uint64_t target_id = 707;
  lock_warmcopy_record_shard_key_t key = make_record_shard_key(8);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest20 = make_digest(0x20);

  ASSERT_TRUE(lock_warmcopy_record_journal_upsert_for_target_for_unit_test(
      target_id, 1, key, 6, digest20, 60,
      make_encoded_record_image("insert")));
  ASSERT_TRUE(
      lock_warmcopy_record_mark_unsupported_mutation_for_target_for_unit_test(
          target_id, 2, key, "unsupported_record_move"));

  lock_warmcopy_record_shard_snapshot_t snapshot;
  ASSERT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      target_id, key, &snapshot));
  EXPECT_NE(0U,
            snapshot.shard_state_flags & LOCK_WARMCOPY_RECORD_SHARD_INVALID);
  EXPECT_EQ(2ULL, snapshot.mutation_generation);
  EXPECT_EQ(2ULL, snapshot.journal_cursor);
  EXPECT_EQ(1ULL, snapshot.last_applied_journal_seq);
  EXPECT_EQ("unsupported_record_move", snapshot.last_diagnostic_reason);

  std::string payload;
  EXPECT_FALSE(
      lock_warmcopy_record_store_export_record_payload_for_target_for_unit_test(
          target_id, &payload));
  EXPECT_TRUE(payload.empty());
}

TEST(LockWarmcopyConversionFreeze,
     FreezeNoteAttemptAndThawAreTrackedInTrxLockState) {
  trx_t trx{};

  lock_warmcopy_trx_conversion_freeze_for_unit_test(&trx.lock, 7001);

  EXPECT_TRUE(
      lock_warmcopy_trx_conversion_is_frozen_for_unit_test(&trx.lock));
  EXPECT_EQ(1ULL, trx.lock.lock_warmcopy_freeze_generation);
  EXPECT_EQ(7001ULL, trx.lock.lock_warmcopy_conversion_freeze_wait_epoch);
  EXPECT_FALSE(trx.lock.lock_warmcopy_conversion_attempt_after_freeze);
  EXPECT_FALSE(trx.lock.lock_warmcopy_conversion_unhandled_after_freeze);

  EXPECT_TRUE(lock_warmcopy_trx_conversion_note_attempt_for_unit_test(
      &trx.lock));
  EXPECT_TRUE(trx.lock.lock_warmcopy_conversion_attempt_after_freeze);
  EXPECT_TRUE(trx.lock.lock_warmcopy_conversion_unhandled_after_freeze);

  EXPECT_TRUE(lock_warmcopy_trx_conversion_note_handled_for_unit_test(
      &trx.lock));
  EXPECT_TRUE(trx.lock.lock_warmcopy_conversion_attempt_after_freeze);
  EXPECT_FALSE(trx.lock.lock_warmcopy_conversion_unhandled_after_freeze);

  lock_warmcopy_trx_conversion_thaw_for_unit_test(&trx.lock);
  EXPECT_FALSE(
      lock_warmcopy_trx_conversion_is_frozen_for_unit_test(&trx.lock));
  EXPECT_EQ(1ULL, trx.lock.lock_warmcopy_freeze_generation);
  EXPECT_EQ(0ULL, trx.lock.lock_warmcopy_conversion_freeze_wait_epoch);
  EXPECT_TRUE(trx.lock.lock_warmcopy_conversion_attempt_after_freeze);
  EXPECT_FALSE(trx.lock.lock_warmcopy_conversion_unhandled_after_freeze);
}

TEST(LockWarmcopyConversionFreeze, SamplesAndComparesTrxLockFence) {
  trx_t trx{};
  trx.lock.trx_locks_version = 10;
  trx.lock.n_rec_locks.store(3);
  trx.lock.lock_warmcopy_freeze_generation = 2;

  lock_warmcopy_trx_lock_fence_t fence;
  ASSERT_TRUE(lock_warmcopy_trx_lock_fence_sample_for_unit_test(&trx.lock,
                                                                &fence));
  EXPECT_EQ(10ULL, fence.trx_locks_version);
  EXPECT_EQ(3ULL, fence.n_rec_locks);
  EXPECT_EQ(2ULL, fence.freeze_generation);
  EXPECT_FALSE(fence.conversion_attempt_after_freeze);
  EXPECT_FALSE(fence.conversion_unhandled_after_freeze);

  lock_warmcopy_trx_lock_fence_t same;
  ASSERT_TRUE(lock_warmcopy_trx_lock_fence_sample_for_unit_test(&trx.lock,
                                                                &same));
  EXPECT_TRUE(lock_warmcopy_trx_lock_fence_equal_for_unit_test(fence, same));

  trx.lock.n_rec_locks.store(4);
  lock_warmcopy_trx_lock_fence_t changed_count;
  ASSERT_TRUE(lock_warmcopy_trx_lock_fence_sample_for_unit_test(
      &trx.lock, &changed_count));
  EXPECT_FALSE(lock_warmcopy_trx_lock_fence_equal_for_unit_test(
      fence, changed_count));

  trx.lock.n_rec_locks.store(3);
  trx.lock.trx_locks_version = 11;
  lock_warmcopy_trx_lock_fence_t changed_version;
  ASSERT_TRUE(lock_warmcopy_trx_lock_fence_sample_for_unit_test(
      &trx.lock, &changed_version));
  EXPECT_FALSE(lock_warmcopy_trx_lock_fence_equal_for_unit_test(
      fence, changed_version));
}

TEST(LockWarmcopyConversionFreeze, WaitForThawReturnsSuccessAfterThaw) {
  lock_warmcopy_reset_for_unit_test();

  trx_t trx{};
  lock_warmcopy_trx_conversion_freeze_for_unit_test(&trx.lock, 9001);
  const uint64_t wait_count_before =
      lock_warmcopy_conversion_freeze_wait_count();

  std::thread thawer([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    lock_warmcopy_trx_conversion_thaw_for_unit_test(&trx.lock);
  });

  EXPECT_EQ(DB_SUCCESS,
            lock_warmcopy_wait_for_conversion_thaw_for_unit_test(&trx, 1000));
  thawer.join();
  EXPECT_EQ(wait_count_before + 1,
            lock_warmcopy_conversion_freeze_wait_count());
}

TEST(LockWarmcopyConversionFreeze, WaitForThawTimesOutWhenStillFrozen) {
  lock_warmcopy_reset_for_unit_test();

  trx_t trx{};
  lock_warmcopy_trx_conversion_freeze_for_unit_test(&trx.lock, 9002);
  const uint64_t wait_count_before =
      lock_warmcopy_conversion_freeze_wait_count();

  EXPECT_EQ(DB_LOCK_WAIT_TIMEOUT,
            lock_warmcopy_wait_for_conversion_thaw_for_unit_test(&trx, 0));
  EXPECT_EQ(wait_count_before + 1,
            lock_warmcopy_conversion_freeze_wait_count());

  lock_warmcopy_trx_conversion_thaw_for_unit_test(&trx.lock);
}

TEST(LockWarmcopyConversionFreeze, WaitForThawReturnsInterruptedOnAbort) {
  lock_warmcopy_reset_for_unit_test();

  trx_t trx{};
  lock_warmcopy_trx_conversion_freeze_for_unit_test(&trx.lock, 9003);
  const uint64_t wait_count_before =
      lock_warmcopy_conversion_freeze_wait_count();

  EXPECT_EQ(DB_INTERRUPTED,
            lock_warmcopy_wait_for_conversion_thaw_abort_for_unit_test(
                &trx, 1000, true));
  EXPECT_EQ(wait_count_before + 1,
            lock_warmcopy_conversion_freeze_wait_count());

  lock_warmcopy_trx_conversion_thaw_for_unit_test(&trx.lock);
}

TEST(LockWarmcopyConversionFreeze,
     FrozenConversionRespectsNowaitAndSkipLockedModes) {
  EXPECT_EQ(DB_SUCCESS,
            lock_warmcopy_frozen_conversion_result(SELECT_ORDINARY));
  EXPECT_EQ(DB_SKIP_LOCKED,
            lock_warmcopy_frozen_conversion_result(SELECT_SKIP_LOCKED));
  EXPECT_EQ(DB_LOCK_NOWAIT,
            lock_warmcopy_frozen_conversion_result(SELECT_NOWAIT));
}

TEST(LockWarmcopyPrepareGuard, MissingLockSysFailsClosedForUnitTest) {
  lock_warmcopy_prepare_guard_t *guard =
      lock_warmcopy_prepare_guard_create_for_unit_test();
  EXPECT_EQ(nullptr, guard);
  lock_warmcopy_prepare_guard_destroy_for_unit_test(guard);
}

TEST(LockWarmcopyHookCoverage, RequiredMutationSitesAreClassifiedAndPresent) {
  size_t site_count = 0;
  const lock_warmcopy_hook_coverage_site_t *sites =
      lock_warmcopy_hook_coverage_sites_for_unit_test(&site_count);
  ASSERT_NE(nullptr, sites);
  ASSERT_GE(site_count, 17U);

  const std::map<std::string, lock_warmcopy_hook_action_t> expected = {
      {"lock_rec_add_to_queue", lock_warmcopy_hook_action_t::JOURNAL_DELTA},
      {"lock_rec_set_nth_bit", lock_warmcopy_hook_action_t::JOURNAL_DELTA},
      {"lock_rec_reset_nth_bit", lock_warmcopy_hook_action_t::JOURNAL_DELTA},
      {"lock_rec_discard", lock_warmcopy_hook_action_t::DIRTY_SHARD},
      {"lock_rec_reset_and_release_wait_low",
       lock_warmcopy_hook_action_t::DIRTY_SHARD},
      {"lock_rec_move_low", lock_warmcopy_hook_action_t::DIRTY_SHARD},
      {"lock_rec_move", lock_warmcopy_hook_action_t::DIRTY_SHARD},
      {"lock_update_discard", lock_warmcopy_hook_action_t::DIRTY_SHARD},
      {"lock_rec_reset_and_inherit_gap_locks",
       lock_warmcopy_hook_action_t::DIRTY_SHARD},
      {"lock_rec_inherit_to_gap", lock_warmcopy_hook_action_t::DIRTY_SHARD},
      {"lock_rec_inherit_to_gap_if_gap_lock",
       lock_warmcopy_hook_action_t::DIRTY_SHARD},
      {"lock_rec_convert_impl_to_expl_for_trx",
       lock_warmcopy_hook_action_t::JOURNAL_DELTA},
      {"lock_rec_convert_active_impl_to_expl",
       lock_warmcopy_hook_action_t::JOURNAL_DELTA},
      {"lock_table_create", lock_warmcopy_hook_action_t::JOURNAL_DELTA},
      {"lock_table_remove_low", lock_warmcopy_hook_action_t::DIRTY_SHARD},
      {"lock_table_dequeue", lock_warmcopy_hook_action_t::DIRTY_SHARD},
      {"lock_unlock_table_autoinc", lock_warmcopy_hook_action_t::DIRTY_SHARD},
  };

  std::set<std::string> observed;
  for (size_t i = 0; i < site_count; ++i) {
    ASSERT_NE(nullptr, sites[i].symbol);
    ASSERT_NE(nullptr, sites[i].source_path);
    const auto expected_it = expected.find(sites[i].symbol);
    ASSERT_NE(expected.end(), expected_it) << sites[i].symbol;
    EXPECT_EQ(expected_it->second, sites[i].action) << sites[i].symbol;
    EXPECT_NE(lock_warmcopy_hook_action_t::UNCLASSIFIED, sites[i].action)
        << sites[i].symbol;
    observed.insert(sites[i].symbol);

    const std::string source = read_source_file(sites[i].source_path);
    ASSERT_FALSE(source.empty()) << sites[i].source_path;
    EXPECT_NE(std::string::npos, source.find(sites[i].symbol))
        << sites[i].source_path << " missing " << sites[i].symbol;
  }

  for (const auto &entry : expected) {
    EXPECT_EQ(1U, observed.count(entry.first)) << entry.first;
  }
}

static void BM_InnoDBLockWarmcopyRecordHotPathBaseline(size_t iterations) {
  lock_warmcopy_reset_for_unit_test();
  BenchmarkRecordHotPathState state;

  for (size_t i = 0; i < iterations; ++i) {
    benchmark_record_bitmap_set(&state, i);
  }

  lock_warmcopy_benchmark_sink =
      state.n_rec_locks.load(std::memory_order_relaxed) + state.bitmap[0];
}
BENCHMARK(BM_InnoDBLockWarmcopyRecordHotPathBaseline)

static void BM_InnoDBLockWarmcopyDisabledRecordHotPath(size_t iterations) {
  lock_warmcopy_reset_for_unit_test();
  BenchmarkRecordHotPathState state;

  for (size_t i = 0; i < iterations; ++i) {
    benchmark_record_bitmap_set_with_disabled_hook(&state, i);
  }

  lock_warmcopy_benchmark_sink =
      state.n_rec_locks.load(std::memory_order_relaxed) + state.bitmap[0];
}
BENCHMARK(BM_InnoDBLockWarmcopyDisabledRecordHotPath)

static void BM_InnoDBLockWarmcopyEnabledRecordHotPath(size_t iterations) {
  lock_warmcopy_reset_for_unit_test();
  THD thd(false);
  thd.set_new_thread_id();
  trx_t trx{};
  trx.mysql_thd = &thd;
  lock_warmcopy_record_shard_key_t key = make_record_shard_key(64);
  key.lock_type_mode = 3;  // LOCK_REC | LOCK_X
  const lock_warmcopy_record_image_digest_t digest = make_digest(0x60);
  const std::string image = make_encoded_record_image("bm-record");

  lock_warmcopy_open_epoch(4242);
  for (size_t i = 0; i < iterations; ++i) {
    const uint32_t heap_no = static_cast<uint32_t>(i & 63U);
    if (!lock_warmcopy_record_bitmap_set_with_image_for_trx(
            &trx, key, heap_no, digest, heap_no * 4U, image)) {
      ++lock_warmcopy_benchmark_sink;
    }
  }
  lock_warmcopy_close_epoch();

  lock_warmcopy_record_store_fence_t fence;
  if (lock_warmcopy_record_store_fence_for_target(thd.thread_id(), &fence)) {
    lock_warmcopy_benchmark_sink += fence.total_mutation_generation;
  }
  lock_warmcopy_record_store_reset_for_unit_test();
}
BENCHMARK(BM_InnoDBLockWarmcopyEnabledRecordHotPath)

}  // namespace innodb_lock0warmcopy_unittest
