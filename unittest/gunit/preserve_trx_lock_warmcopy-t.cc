/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with the
   program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "my_config.h"

#include <gtest/gtest.h>

#include <fcntl.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "my_sys.h"
#include "sql/mysqld.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_drain.h"
#include "sql/preserve_trx_lock_warmcopy.h"
#include "sql/mdl.h"
#include "storage/innobase/include/lock0preserve_plan.h"
#include "storage/innobase/include/lock0warmcopy.h"

namespace preserve_trx_lock_warmcopy_unittest {
namespace {

void append_u32(std::string *out, uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

void append_u16(std::string *out, uint16_t value) {
  out->push_back(static_cast<char>(value & 0xffU));
  out->push_back(static_cast<char>((value >> 8) & 0xffU));
}

void append_u64(std::string *out, uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

std::string make_record_image(char marker) {
  std::string image;
  append_u32(&image, 3);
  image.push_back(marker);
  image.push_back(static_cast<char>(marker + 1));
  image.push_back(static_cast<char>(marker + 2));
  return image;
}

std::string parent_path_for_test(const std::string &path) {
  const std::string::size_type slash = path.find_last_of(FN_LIBCHAR);
  if (slash == std::string::npos) return "";
  if (slash == 0) return path.substr(0, 1);
  return path.substr(0, slash);
}

std::string join_path_for_test(const std::string &dir,
                               const std::string &name) {
  if (dir.empty() || dir.back() == FN_LIBCHAR) return dir + name;
  return dir + FN_LIBCHAR + name;
}

void ensure_dir_for_test(const std::string &dir) {
  if (my_mkdir(dir.c_str(), 0700, MYF(0)) != 0) {
    ASSERT_EQ(EEXIST, my_errno()) << dir;
  }
}

void create_file_for_test(const std::string &path) {
  File file = my_create(path.c_str(), 0600, O_WRONLY | O_TRUNC, MYF(0));
  ASSERT_GE(file, 0) << path;
  const char payload[] = "orphan";
  ASSERT_EQ(sizeof(payload), my_write(file,
                                      reinterpret_cast<const uchar *>(payload),
                                      sizeof(payload), MYF(0)))
      << path;
  ASSERT_EQ(0, my_close(file, MYF(0))) << path;
}

void create_file_with_payload_for_test(const std::string &path,
                                       const std::string &payload) {
  File file = my_create(path.c_str(), 0600, O_WRONLY | O_TRUNC, MYF(0));
  ASSERT_GE(file, 0) << path;
  ASSERT_EQ(payload.size(),
            my_write(file, reinterpret_cast<const uchar *>(payload.data()),
                     payload.size(), MYF(0)))
      << path;
  ASSERT_EQ(0, my_close(file, MYF(0))) << path;
}

std::string unique_dir_for_test(const std::string &prefix) {
  static uint64_t counter = 0;
  return join_path_for_test(mysql_tmpdir,
                            prefix + "-" + std::to_string(getpid()) + "-" +
                                std::to_string(++counter));
}

std::string make_heap_offsets(uint32_t heap_no) {
  std::string offsets;
  append_u32(&offsets, heap_no * 10);
  return offsets;
}

std::string make_record_entry(uint64_t table_id, uint64_t index_id,
                              uint32_t page_no, uint32_t heap_no,
                              char image_marker,
                              uint64_t page_lsn =
                                  0x0102030405060708ULL,
                              uint32_t page_n_heap = 6,
                              bool include_record_image = true,
                              uint32_t type_mode = 3) {
  std::string bitmap(1, '\0');
  bitmap[0] = static_cast<char>(1U << heap_no);
  const std::string heap_offsets = make_heap_offsets(heap_no);
  const std::string record_images =
      include_record_image ? make_record_image(image_marker) : std::string();

  std::string entry;
  append_u64(&entry, table_id);
  append_u64(&entry, index_id);
  append_u32(&entry, 7);  // space_id
  append_u32(&entry, page_no);
  append_u32(&entry, type_mode);
  append_u32(&entry, 8);  // n_bits
  append_u64(&entry, page_lsn);
  append_u32(&entry, page_n_heap);
  append_u32(&entry, static_cast<uint32_t>(heap_offsets.size()));
  append_u32(&entry, static_cast<uint32_t>(record_images.size()));
  append_u32(&entry, static_cast<uint32_t>(bitmap.size()));
  entry.append(heap_offsets);
  entry.append(record_images);
  entry.append(bitmap);
  return entry;
}

std::string make_record_payload(const std::vector<std::string> &entries) {
  std::string payload;
  append_u32(&payload, static_cast<uint32_t>(entries.size()));
  for (const std::string &entry : entries) payload.append(entry);
  return payload;
}

std::string make_table_entry(uint64_t table_id, uint32_t lock_mode) {
  std::string entry;
  append_u64(&entry, table_id);
  append_u32(&entry, lock_mode);
  append_u32(&entry, 16);  // LOCK_TABLE type_mode bit in the payload contract
  append_u32(&entry, 0);
  return entry;
}

std::string make_table_payload(const std::vector<std::string> &entries) {
  std::string payload;
  append_u32(&payload, static_cast<uint32_t>(entries.size()));
  for (const std::string &entry : entries) payload.append(entry);
  return payload;
}

std::string make_mdl_entry(MDL_key::enum_mdl_namespace mdl_namespace,
                           enum_mdl_type type, const std::string &db,
                           const std::string &name, uint32_t ordinal,
                           enum_mdl_duration duration = MDL_TRANSACTION) {
  std::string part_key;
  part_key.append(db);
  part_key.push_back('\0');
  part_key.append(name);
  part_key.push_back('\0');

  std::string entry;
  entry.push_back(static_cast<char>(mdl_namespace));
  entry.push_back(static_cast<char>(type));
  entry.push_back(static_cast<char>(duration));
  entry.push_back(0);
  append_u32(&entry, ordinal);
  append_u16(&entry, static_cast<uint16_t>(db.length()));
  append_u16(&entry, static_cast<uint16_t>(part_key.length()));
  entry.append(part_key);
  return entry;
}

std::string make_mdl_payload(const std::vector<std::string> &entries) {
  std::string payload;
  append_u32(&payload, static_cast<uint32_t>(entries.size()));
  for (const std::string &entry : entries) payload.append(entry);
  return payload;
}

}  // namespace

class ScopedLockWarmcopyEnable {
 public:
  explicit ScopedLockWarmcopyEnable(bool enabled)
      : m_saved(preserve_trx_lock_warmcopy_enable) {
    preserve_trx_lock_warmcopy_enable = enabled;
  }

  ~ScopedLockWarmcopyEnable() { preserve_trx_lock_warmcopy_enable = m_saved; }

 private:
  bool m_saved;
};

TEST(PreserveTrxLockWarmcopyConfig, DefaultsMatchDocumentedGateAContract) {
  const Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();

  EXPECT_TRUE(options.enabled);
  EXPECT_TRUE(options.fallback_to_live_export);
  EXPECT_EQ(268435456ULL, options.max_memory_bytes);
  EXPECT_EQ(1073741824ULL, options.max_journal_bytes);
  EXPECT_EQ(100000U, options.max_dirty_shards);
  EXPECT_EQ(100000U, options.max_mdl_descriptors);
  EXPECT_EQ(0U, options.seal_threads);
  EXPECT_EQ(30000U, options.conversion_wait_timeout_ms);
}

TEST(PreserveTrxLockWarmcopyConfig,
     LockWarmcopyCanRequireTwoPhaseWithoutBinlogWarmcopy) {
  {
    ScopedLockWarmcopyEnable lock_warmcopy_on(true);
    EXPECT_TRUE(preserve_trx_lock_warmcopy_requires_two_phase(false));
    EXPECT_TRUE(preserve_trx_lock_warmcopy_requires_two_phase(true));
  }

  {
    ScopedLockWarmcopyEnable lock_warmcopy_off(false);
    EXPECT_FALSE(preserve_trx_lock_warmcopy_requires_two_phase(false));
    EXPECT_TRUE(preserve_trx_lock_warmcopy_requires_two_phase(true));
  }
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     NoopParticipantHasStableLifecycleAndNoArtifact) {
  lock_warmcopy_reset_for_unit_test();
  Preserve_trx_lock_warmcopy_drain_participant participant(
      preserve_trx_lock_warmcopy_current_options());

  Preserve_trx_drain_participant_observation observation =
      participant.observation();
  EXPECT_EQ(Preserve_trx_drain_participant_state::NOT_STARTED,
            observation.state);
  EXPECT_FALSE(observation.owns_artifact);
  EXPECT_EQ(268435456ULL, observation.bytes_budget);
  EXPECT_EQ(0ULL, observation.bytes_used);
  EXPECT_TRUE(observation.phase2_slo_guaranteed);
  EXPECT_EQ(0ULL, observation.phase2_slo_not_guaranteed_target_count);
  EXPECT_EQ(0U, observation.phase1_progress);
  EXPECT_TRUE(observation.failure_reason.empty());
  EXPECT_TRUE(observation.phase2_slo_reason.empty());

  EXPECT_TRUE(participant.open_phase1());
  EXPECT_TRUE(lock_warmcopy_hooks_enabled());
  observation = participant.observation();
  EXPECT_EQ(Preserve_trx_drain_participant_state::OPEN, observation.state);
  EXPECT_FALSE(observation.owns_artifact);

  EXPECT_TRUE(participant.close_phase1());
  EXPECT_FALSE(lock_warmcopy_hooks_enabled());
  EXPECT_TRUE(participant.phase1_ready());
  observation = participant.observation();
  EXPECT_EQ(Preserve_trx_drain_participant_state::READY, observation.state);
  EXPECT_FALSE(observation.owns_artifact);
  EXPECT_EQ(100U, observation.phase1_progress);

  EXPECT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));
  EXPECT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::SINGLE_PHASE));

  participant.finalize_phase();
  EXPECT_EQ(Preserve_trx_drain_participant_state::FINALIZED,
            participant.observation().state);
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase2PreflightSealsSingleTargetRecordStoreArtifact) {
  lock_warmcopy_reset_for_unit_test();
  lock_warmcopy_record_shard_key_t key;
  key.table_id = 100;
  key.index_id = 200;
  key.space_id = 7;
  key.page_no = 11;
  key.lock_type_mode = 3;
  key.n_bits = 8;
  lock_warmcopy_record_image_digest_t digest;
  digest.bytes[0] = 0xa0;
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key, 2, digest, 20, make_record_image('a')));

  Preserve_trx_lock_warmcopy_drain_participant participant(
      preserve_trx_lock_warmcopy_current_options());
  EXPECT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  EXPECT_TRUE(participant.close_phase1());
  EXPECT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  EXPECT_TRUE(artifact->valid);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::OK, artifact->reason);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_artifact_source::WARM_COPY,
            artifact->source);
  EXPECT_FALSE(artifact->record_locks_payload.empty());
  EXPECT_TRUE(artifact->predicate_locks_payload.empty());
  EXPECT_TRUE(artifact->table_locks_payload.empty());
  EXPECT_FALSE(artifact->mdl_descriptors_payload.empty());
  EXPECT_EQ(1U, artifact->record_lock_count);
  EXPECT_EQ(0U, artifact->table_lock_count);
  EXPECT_EQ(1U, artifact->record_predicate_table_lock_count);
  EXPECT_EQ(0U, artifact->mdl_descriptor_count);
  EXPECT_EQ(nullptr, participant.artifact_for_thread(43));

  const Preserve_trx_drain_participant_observation observation =
      participant.observation();
  EXPECT_TRUE(observation.owns_artifact);
  EXPECT_GT(observation.bytes_used, 0ULL);
  EXPECT_EQ(1ULL, observation.phase2_record_lock_count);
  EXPECT_EQ(0ULL, observation.phase2_table_lock_count);
  EXPECT_EQ(0ULL, observation.phase2_mdl_descriptor_count);
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase2PreflightUsesSealedRecordStorePayloadNotLiveCandidate) {
  lock_warmcopy_reset_for_unit_test();
  lock_warmcopy_record_shard_key_t key_a;
  key_a.table_id = 100;
  key_a.index_id = 200;
  key_a.space_id = 7;
  key_a.page_no = 11;
  key_a.lock_type_mode = 3;
  key_a.n_bits = 8;
  lock_warmcopy_record_shard_key_t key_b = key_a;
  key_b.table_id = 101;
  key_b.index_id = 201;
  key_b.page_no = 12;

  lock_warmcopy_record_image_digest_t digest_a;
  digest_a.bytes[0] = 0xa0;
  lock_warmcopy_record_image_digest_t digest_b;
  digest_b.bytes[0] = 0xb0;
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key_a, 2, digest_a, 20, make_record_image('a')));

  Preserve_trx_lock_warmcopy_drain_participant participant(
      preserve_trx_lock_warmcopy_current_options());
  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));

  std::string live_candidate_payload;
  ASSERT_TRUE(lock_warmcopy_record_store_export_record_payload_for_target(
      42, &live_candidate_payload, nullptr));

  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key_b, 4, digest_b, 40, make_record_image('b')));
  std::string sealed_store_payload;
  uint32_t sealed_store_count = 0;
  ASSERT_TRUE(lock_warmcopy_record_store_export_record_payload_for_target(
      42, &sealed_store_payload, &sealed_store_count));
  ASSERT_NE(live_candidate_payload, sealed_store_payload);
  ASSERT_EQ(2U, sealed_store_count);

  ASSERT_TRUE(participant.close_phase1());
  ASSERT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  EXPECT_TRUE(artifact->valid);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::OK, artifact->reason);
  EXPECT_EQ(sealed_store_payload, artifact->record_locks_payload);
  EXPECT_EQ(2U, artifact->record_lock_count);
  EXPECT_EQ(2U, artifact->record_predicate_table_lock_count);
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase1RecordSeedSurvivesQuiescedPrepareAndFeedsSeal) {
  lock_warmcopy_reset_for_unit_test();
  const std::string phase1_payload = make_record_payload(
      {make_record_entry(100, 200, 11, 2, 'p')});

  Preserve_trx_lock_warmcopy_drain_participant participant(
      preserve_trx_lock_warmcopy_current_options());
  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_phase1_record_payload_for_thread_for_unit_test(
      42, phase1_payload));

  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  ASSERT_TRUE(participant.close_phase1());
  ASSERT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  EXPECT_TRUE(artifact->valid);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::OK, artifact->reason);
  const Preserve_trx_lock_warmcopy_canonical_compare_result compare =
      preserve_trx_lock_warmcopy_compare_record_payloads_canonical(
          phase1_payload, artifact->record_locks_payload);
  EXPECT_TRUE(compare.equivalent) << compare.difference;
  EXPECT_EQ(1U, artifact->record_lock_count);

  const Preserve_trx_drain_participant_observation observation =
      participant.observation();
  EXPECT_EQ(0ULL, observation.phase1_record_prebuilt_target_count);
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase1RecordSeedCanSealAsPrebuiltBlobWithoutPhase2Payload) {
  lock_warmcopy_reset_for_unit_test();
  const std::string phase1_payload = make_record_payload(
      {make_record_entry(100, 200, 11, 2, 'p')});

  Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();
  options.preserve_dir =
      unique_dir_for_test("preserve-lock-warmcopy-prebuilt-record");
  Preserve_trx_lock_warmcopy_drain_participant participant(options);

  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_phase1_record_payload_for_thread_for_unit_test(
      42, phase1_payload));

  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  ASSERT_TRUE(participant.close_phase1());
  ASSERT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  EXPECT_TRUE(artifact->valid);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::OK, artifact->reason);
  EXPECT_TRUE(artifact->record_locks_payload.empty());
  EXPECT_TRUE(artifact->has_prebuilt_record_locks_blob);
  EXPECT_EQ(kPreservedTrxBlobRecordLocks,
            artifact->prebuilt_record_locks_blob.name);
  EXPECT_EQ(phase1_payload.size(), artifact->prebuilt_record_locks_blob.size);
  EXPECT_FALSE(artifact->prebuilt_record_locks_blob.warmcopy_id.empty());
  EXPECT_EQ(1U, artifact->record_lock_count);

  const Preserve_trx_drain_participant_observation observation =
      participant.observation();
  EXPECT_EQ(1ULL, observation.phase1_record_prebuilt_target_count);
  EXPECT_EQ(0ULL, observation.materialized_lock_payload_bytes_in_phase2);
  EXPECT_EQ(1ULL, observation.phase2_record_prebuilt_target_count);
  EXPECT_EQ(0ULL, observation.phase2_record_materialized_target_count);
  EXPECT_EQ(0ULL, observation.phase2_table_live_export_target_count);
  EXPECT_EQ(0ULL, observation.phase2_mdl_live_export_target_count);
  EXPECT_TRUE(observation.phase2_slo_guaranteed);
  EXPECT_EQ(0ULL, observation.phase2_slo_not_guaranteed_target_count);
  EXPECT_TRUE(observation.phase2_slo_reason.empty());
  participant.finalize_phase();
  (void)rmdir(options.preserve_dir.c_str());
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase2SloBlockerObservationCountsNonRecordLiveExportTargets) {
  lock_warmcopy_reset_for_unit_test();
  const std::string phase1_payload = make_record_payload(
      {make_record_entry(100, 200, 11, 2, 'p')});

  Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();
  options.preserve_dir =
      unique_dir_for_test("preserve-lock-warmcopy-non-record-blockers");
  Preserve_trx_lock_warmcopy_drain_participant participant(options);

  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_phase1_record_payload_for_thread_for_unit_test(
      42, phase1_payload));
  ASSERT_TRUE(participant.prepare_phase1_record_payload_for_thread_for_unit_test(
      43, phase1_payload));
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42, 43}));
  participant.set_table_locks_for_thread_for_unit_test(
      42, make_table_payload({make_table_entry(301, 16)}), 1, false);
  participant.set_mdl_descriptors_for_thread_for_unit_test(
      43,
      make_mdl_payload({make_mdl_entry(MDL_key::TABLE, MDL_SHARED_WRITE, "db",
                                       "t1", 1)}),
      1);

  ASSERT_TRUE(participant.close_phase1());
  ASSERT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_drain_participant_observation observation =
      participant.observation();
  EXPECT_EQ(1ULL, observation.phase2_table_live_export_target_count);
  EXPECT_EQ(1ULL, observation.phase2_mdl_live_export_target_count);
  EXPECT_FALSE(observation.phase2_slo_guaranteed);
  EXPECT_EQ(2ULL, observation.phase2_slo_not_guaranteed_target_count);
  EXPECT_STREQ("table_mdl_live_export",
               observation.phase2_slo_reason.c_str());

  participant.finalize_phase();
  (void)rmdir(options.preserve_dir.c_str());
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase1NonRecordFingerprintsAvoidLiveExportSloBlocker) {
  lock_warmcopy_reset_for_unit_test();
  const std::string phase1_payload = make_record_payload(
      {make_record_entry(100, 200, 11, 2, 'p')});
  const std::string table_payload =
      make_table_payload({make_table_entry(301, 1)});
  const std::string mdl_payload =
      make_mdl_payload({make_mdl_entry(MDL_key::TABLE, MDL_SHARED_WRITE, "db",
                                       "t1", 1)});

  Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();
  options.preserve_dir =
      unique_dir_for_test("preserve-lock-warmcopy-non-record-fingerprint");
  Preserve_trx_lock_warmcopy_drain_participant participant(options);

  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_phase1_record_payload_for_thread_for_unit_test(
      42, phase1_payload));
  participant.prepare_phase1_non_record_payloads_for_thread_for_unit_test(
      42, table_payload, 1, false, mdl_payload, 1);
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  participant.set_table_locks_for_thread_for_unit_test(42, table_payload, 1,
                                                       false);
  participant.set_mdl_descriptors_for_thread_for_unit_test(42, mdl_payload, 1);

  ASSERT_TRUE(participant.close_phase1());
  ASSERT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_drain_participant_observation observation =
      participant.observation();
  EXPECT_EQ(1ULL, observation.phase2_table_lock_count);
  EXPECT_EQ(1ULL, observation.phase2_mdl_descriptor_count);
  EXPECT_EQ(0ULL, observation.phase2_table_live_export_target_count);
  EXPECT_EQ(0ULL, observation.phase2_mdl_live_export_target_count);
  EXPECT_TRUE(observation.phase2_slo_guaranteed);
  EXPECT_EQ(0ULL, observation.phase2_slo_not_guaranteed_target_count);
  EXPECT_TRUE(observation.phase2_slo_reason.empty());

  participant.finalize_phase();
  (void)rmdir(options.preserve_dir.c_str());
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase1HookBuiltRecordStoreCanBePrebuiltBeforeQuiesce) {
  lock_warmcopy_reset_for_unit_test();
  lock_warmcopy_record_shard_key_t key;
  key.table_id = 100;
  key.index_id = 200;
  key.space_id = 7;
  key.page_no = 11;
  key.lock_type_mode = 3;
  key.n_bits = 8;
  lock_warmcopy_record_image_digest_t digest;
  digest.bytes[0] = 0xa7;

  Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();
  options.preserve_dir =
      unique_dir_for_test("preserve-lock-warmcopy-hook-prebuilt-record");
  Preserve_trx_lock_warmcopy_drain_participant participant(options);

  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key, 2, digest, 20, make_record_image('h')));
  ASSERT_TRUE(participant.prepare_phase1_record_store_targets());
  {
    const Preserve_trx_drain_participant_observation observation =
        participant.observation();
    EXPECT_EQ(1ULL, observation.phase1_record_prebuilt_target_count);
  }

  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  ASSERT_TRUE(participant.close_phase1());
  ASSERT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  EXPECT_TRUE(artifact->valid);
  EXPECT_TRUE(artifact->record_locks_payload.empty());
  EXPECT_TRUE(artifact->has_prebuilt_record_locks_blob);
  EXPECT_FALSE(
      artifact->prebuilt_record_locks_blob.strict_metadata_only_compatible);
  EXPECT_EQ(1U, artifact->record_lock_count);

  const Preserve_trx_drain_participant_observation observation =
      participant.observation();
  EXPECT_EQ(1ULL, observation.phase1_record_prebuilt_target_count);
  EXPECT_EQ(0ULL, observation.materialized_lock_payload_bytes_in_phase2);
  EXPECT_EQ(1ULL, observation.phase2_record_prebuilt_target_count);
  EXPECT_EQ(0ULL, observation.phase2_record_materialized_target_count);
  participant.finalize_phase();
  (void)rmdir(options.preserve_dir.c_str());
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase1RecordStoreInvokesBlobReadyCallbackPerTarget) {
  lock_warmcopy_reset_for_unit_test();
  lock_warmcopy_record_shard_key_t key;
  key.table_id = 100;
  key.index_id = 200;
  key.space_id = 7;
  key.page_no = 11;
  key.lock_type_mode = 3;
  key.n_bits = 8;
  lock_warmcopy_record_image_digest_t digest;
  digest.bytes[0] = 0xa8;

  Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();
  options.preserve_dir =
      unique_dir_for_test("preserve-lock-warmcopy-record-ready-callback");
  Preserve_trx_lock_warmcopy_drain_participant participant(options);
  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key, 2, digest, 20, make_record_image('i')));

  std::vector<uint64_t> callback_tokens;
  ASSERT_TRUE(participant.prepare_phase1_record_store_targets(
      [&](uint64_t token, const PrebuiltRecordLocksBlob &blob) {
        callback_tokens.push_back(token);
        return blob.size != 0 && !blob.warmcopy_id.empty() &&
               !blob.strict_metadata_only_compatible;
      }));
  ASSERT_EQ(1U, callback_tokens.size());
  EXPECT_EQ(42U, callback_tokens[0]);

  participant.finalize_phase();
  (void)rmdir(options.preserve_dir.c_str());
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase1MetadataOnlyRecordBlobIsStrictCompatible) {
  lock_warmcopy_reset_for_unit_test();
  const std::string payload = make_record_payload(
      {make_record_entry(100, 200, 11, 2, 'm', 0x0102030405060708ULL, 6,
                         false, 35)});
  lock_preserve_record_lock_metadata_facts_t facts;
  ASSERT_EQ(lock_preserve_metadata_plan_status::OK,
            lock_preserve_build_record_lock_metadata_facts(payload, &facts));
  EXPECT_FALSE(facts.record_image_present);

  Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();
  options.preserve_dir =
      unique_dir_for_test("preserve-lock-warmcopy-metadata-only");
  Preserve_trx_lock_warmcopy_drain_participant participant(options);

  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_phase1_record_payload_for_thread_for_unit_test(
      42, payload));
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  ASSERT_TRUE(participant.close_phase1());
  ASSERT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  ASSERT_TRUE(artifact->has_prebuilt_record_locks_blob);
  EXPECT_TRUE(
      artifact->prebuilt_record_locks_blob.strict_metadata_only_compatible);

  participant.finalize_phase();
  (void)rmdir(options.preserve_dir.c_str());
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase1PrebuiltRecordBlobIsNotUsedAfterStoreDelta) {
  lock_warmcopy_reset_for_unit_test();
  const std::string phase1_payload = make_record_payload(
      {make_record_entry(100, 200, 11, 2, 'p')});
  const std::string final_payload = make_record_payload(
      {make_record_entry(100, 200, 11, 4, 'q')});

  Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();
  options.preserve_dir =
      unique_dir_for_test("preserve-lock-warmcopy-stale-prebuilt-record");
  Preserve_trx_lock_warmcopy_drain_participant participant(options);

  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_phase1_record_payload_for_thread_for_unit_test(
      42, phase1_payload));

  lock_warmcopy_record_shard_key_t key;
  key.table_id = 100;
  key.index_id = 200;
  key.space_id = 7;
  key.page_no = 11;
  key.lock_type_mode = 3;
  key.n_bits = 8;
  lock_warmcopy_record_image_digest_t digest;
  digest.bytes[0] = 0xd4;
  ASSERT_TRUE(lock_warmcopy_record_journal_upsert_for_target_for_unit_test(
      42, 1, key, 4, digest, 40, make_record_image('q')));
  ASSERT_TRUE(lock_warmcopy_record_journal_delete_for_target_for_unit_test(
      42, 2, key, 2));

  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  ASSERT_TRUE(participant.close_phase1());
  ASSERT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  EXPECT_TRUE(artifact->valid);
  EXPECT_FALSE(artifact->has_prebuilt_record_locks_blob);
  EXPECT_FALSE(artifact->record_locks_payload.empty());
  const Preserve_trx_lock_warmcopy_canonical_compare_result compare =
      preserve_trx_lock_warmcopy_compare_record_payloads_canonical(
          final_payload, artifact->record_locks_payload);
  EXPECT_TRUE(compare.equivalent) << compare.difference;
  EXPECT_EQ(1U, artifact->record_lock_count);

  const Preserve_trx_drain_participant_observation observation =
      participant.observation();
  EXPECT_GT(observation.materialized_lock_payload_bytes_in_phase2, 0ULL);
  EXPECT_EQ(0ULL, observation.phase2_record_prebuilt_target_count);
  EXPECT_EQ(1ULL, observation.phase2_record_materialized_target_count);
  EXPECT_FALSE(observation.phase2_slo_guaranteed);
  participant.finalize_phase();
  (void)rmdir(options.preserve_dir.c_str());
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase1RecordSeedForNonFinalTargetIsCleared) {
  lock_warmcopy_reset_for_unit_test();
  const std::string phase1_payload = make_record_payload(
      {make_record_entry(100, 200, 11, 2, 'p')});

  Preserve_trx_lock_warmcopy_drain_participant participant(
      preserve_trx_lock_warmcopy_current_options());
  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_phase1_record_payload_for_thread_for_unit_test(
      42, phase1_payload));

  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({43}));
  ASSERT_TRUE(participant.close_phase1());
  ASSERT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  EXPECT_EQ(nullptr, participant.artifact_for_thread(42));
  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(43);
  ASSERT_NE(nullptr, artifact);
  EXPECT_TRUE(artifact->valid);
  EXPECT_TRUE(artifact->record_locks_payload.empty());

  std::string stale_payload;
  uint32_t stale_count = 999;
  ASSERT_TRUE(lock_warmcopy_record_store_export_record_payload_for_target(
      42, &stale_payload, &stale_count));
  EXPECT_TRUE(stale_payload.empty());
  EXPECT_EQ(0U, stale_count);
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase2PreflightPublishesJournalDirtyShardAndPauseStatus) {
  lock_warmcopy_reset_for_unit_test();
  const ulonglong journal_before =
      preserve_trx_lock_warmcopy_journal_bytes_status();
  const ulonglong dirty_before =
      preserve_trx_lock_warmcopy_dirty_shards_status();
  const ulonglong pause_before =
      preserve_trx_lock_warmcopy_phase2_pause_us_status();

  lock_warmcopy_record_shard_key_t key;
  key.table_id = 100;
  key.index_id = 200;
  key.space_id = 7;
  key.page_no = 11;
  key.lock_type_mode = 3;
  key.n_bits = 8;
  lock_warmcopy_record_image_digest_t digest;
  digest.bytes[0] = 0x41;
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key, 2, digest, 20, make_record_image('s')));

  Preserve_trx_lock_warmcopy_drain_participant participant(
      preserve_trx_lock_warmcopy_current_options());
  EXPECT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  EXPECT_TRUE(participant.close_phase1());
  EXPECT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  EXPECT_GT(preserve_trx_lock_warmcopy_journal_bytes_status(),
            journal_before);
  EXPECT_GE(preserve_trx_lock_warmcopy_dirty_shards_status(), dirty_before);
  EXPECT_GE(preserve_trx_lock_warmcopy_dirty_shards_status(), 1ULL);
  EXPECT_GE(preserve_trx_lock_warmcopy_phase2_pause_us_status(), pause_before);
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase2PreflightCreatesValidEmptyLockArtifactForEmptyTarget) {
  lock_warmcopy_reset_for_unit_test();

  Preserve_trx_lock_warmcopy_drain_participant participant(
      preserve_trx_lock_warmcopy_current_options());
  EXPECT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  EXPECT_TRUE(participant.close_phase1());
  EXPECT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  EXPECT_TRUE(artifact->valid);
  EXPECT_TRUE(artifact->record_locks_payload.empty());
  EXPECT_TRUE(artifact->table_locks_payload.empty());
  EXPECT_FALSE(artifact->mdl_descriptors_payload.empty());
  EXPECT_FALSE(artifact->autoinc_lock_owned);
  EXPECT_EQ(0U, artifact->record_lock_count);
  EXPECT_EQ(0U, artifact->table_lock_count);
  EXPECT_EQ(0U, artifact->record_predicate_table_lock_count);
  EXPECT_EQ(0U, artifact->mdl_descriptor_count);
  EXPECT_TRUE(artifact->record_live_seal_fence_valid);
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase2PreflightRejectsMdlDescriptorLimitOverflow) {
  lock_warmcopy_reset_for_unit_test();

  Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();
  options.fallback_to_live_export = false;
  options.max_mdl_descriptors = 0;
  Preserve_trx_lock_warmcopy_drain_participant participant(options);

  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  participant.set_mdl_descriptors_for_thread_for_unit_test(
      42, std::string("non-empty-mdl-payload"), 1);
  ASSERT_TRUE(participant.close_phase1());
  EXPECT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  EXPECT_FALSE(artifact->valid);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED,
            artifact->reason);

  Preserve_trx_lock_warmcopy_target_observation target;
  ASSERT_TRUE(participant.target_observation_for_thread_for_unit_test(
      42, &target));
  EXPECT_EQ(Preserve_trx_lock_warmcopy_target_state::SEALED_INVALID,
            target.state);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED,
            target.reason);
  EXPECT_TRUE(target.has_artifact);
  EXPECT_FALSE(target.artifact_valid);
  EXPECT_EQ(0U, target.mdl_descriptor_count);
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase2PreflightSealsIndependentArtifactsForMultipleTargets) {
  lock_warmcopy_reset_for_unit_test();
  lock_warmcopy_record_shard_key_t key_a;
  key_a.table_id = 100;
  key_a.index_id = 200;
  key_a.space_id = 7;
  key_a.page_no = 11;
  key_a.lock_type_mode = 3;
  key_a.n_bits = 8;
  lock_warmcopy_record_shard_key_t key_b = key_a;
  key_b.table_id = 101;
  key_b.index_id = 201;
  key_b.page_no = 12;

  lock_warmcopy_record_image_digest_t digest_a;
  digest_a.bytes[0] = 0xa0;
  lock_warmcopy_record_image_digest_t digest_b;
  digest_b.bytes[0] = 0xb0;
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key_a, 2, digest_a, 20, make_record_image('a')));
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          43, key_b, 4, digest_b, 40, make_record_image('b')));

  Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();
  options.seal_threads = 2;
  Preserve_trx_lock_warmcopy_drain_participant participant(options);
  EXPECT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42, 43}));
  EXPECT_TRUE(participant.close_phase1());
  EXPECT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_lock_warmcopy_artifact *artifact_a =
      participant.artifact_for_thread(42);
  const Preserve_trx_lock_warmcopy_artifact *artifact_b =
      participant.artifact_for_thread(43);
  ASSERT_NE(nullptr, artifact_a);
  ASSERT_NE(nullptr, artifact_b);
  EXPECT_TRUE(artifact_a->valid);
  EXPECT_TRUE(artifact_b->valid);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::OK, artifact_a->reason);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::OK, artifact_b->reason);
  EXPECT_NE(artifact_a->record_locks_payload,
            artifact_b->record_locks_payload);
  EXPECT_EQ(1U, artifact_a->record_lock_count);
  EXPECT_EQ(1U, artifact_b->record_lock_count);
  EXPECT_EQ(1U, artifact_a->record_predicate_table_lock_count);
  EXPECT_EQ(1U, artifact_b->record_predicate_table_lock_count);
  EXPECT_EQ(nullptr, participant.artifact_for_thread(44));

  for (const uint64_t thread_id : {42ULL, 43ULL}) {
    Preserve_trx_lock_warmcopy_target_observation target;
    ASSERT_TRUE(participant.target_observation_for_thread_for_unit_test(
        thread_id, &target));
    EXPECT_EQ(Preserve_trx_lock_warmcopy_target_state::SEALED_VALID,
              target.state);
    EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::OK, target.reason);
    EXPECT_TRUE(target.has_artifact);
    EXPECT_TRUE(target.artifact_valid);
    EXPECT_EQ(1U, target.record_predicate_table_lock_count);
    EXPECT_GT(target.bytes_used, 0ULL);
  }

  const Preserve_trx_drain_participant_observation observation =
      participant.observation();
  EXPECT_TRUE(observation.owns_artifact);
  EXPECT_GT(observation.bytes_used, 0ULL);
  EXPECT_EQ(2U, observation.phase2_seal_worker_count);
  EXPECT_TRUE(observation.failure_reason.empty());
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase2PreflightRejectsRecordStoreAboveLockCountLimit) {
  lock_warmcopy_reset_for_unit_test();
  lock_warmcopy_record_shard_key_t key;
  key.table_id = 100;
  key.index_id = 200;
  key.space_id = 7;
  key.page_no = 11;
  key.lock_type_mode = 3;
  key.n_bits = 8;
  lock_warmcopy_record_image_digest_t digest_a;
  digest_a.bytes[0] = 0xa0;
  lock_warmcopy_record_image_digest_t digest_b;
  digest_b.bytes[0] = 0xb0;
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key, 2, digest_a, 20, make_record_image('a')));
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key, 4, digest_b, 40, make_record_image('b')));

  Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();
  options.max_lock_count = 1;
  Preserve_trx_lock_warmcopy_drain_participant participant(options);
  EXPECT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  EXPECT_TRUE(participant.close_phase1());
  EXPECT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  EXPECT_FALSE(artifact->valid);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED,
            artifact->reason);
  EXPECT_TRUE(artifact->record_locks_payload.empty());
  EXPECT_EQ(0U, artifact->record_predicate_table_lock_count);

  Preserve_trx_lock_warmcopy_target_observation target;
  ASSERT_TRUE(participant.target_observation_for_thread_for_unit_test(
      42, &target));
  EXPECT_EQ(Preserve_trx_lock_warmcopy_target_state::SEALED_INVALID,
            target.state);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED,
            target.reason);
  EXPECT_TRUE(target.has_artifact);
  EXPECT_FALSE(target.artifact_valid);
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase2PreflightSpillsArtifactAboveMemoryBudget) {
  lock_warmcopy_reset_for_unit_test();
  lock_warmcopy_record_shard_key_t key;
  key.table_id = 100;
  key.index_id = 200;
  key.space_id = 7;
  key.page_no = 11;
  key.lock_type_mode = 3;
  key.n_bits = 8;
  lock_warmcopy_record_image_digest_t digest;
  digest.bytes[0] = 0xa0;
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key, 2, digest, 20, make_record_image('a')));

  Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();
  options.max_memory_bytes = 1;
  Preserve_trx_lock_warmcopy_drain_participant participant(options);
  EXPECT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  EXPECT_TRUE(participant.close_phase1());
  EXPECT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  EXPECT_TRUE(artifact->valid);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::OK, artifact->reason);
  EXPECT_TRUE(artifact->spilled_to_file);
  EXPECT_TRUE(artifact->spill_materialized);
  EXPECT_GT(artifact->spill_payload_bytes, 0ULL);
  EXPECT_FALSE(artifact->record_locks_payload.empty());
  const std::string spill_path = artifact->spill_path;
  const std::string target_dir = parent_path_for_test(spill_path);
  const std::string batch_dir = parent_path_for_test(target_dir);
  const std::string manifest_path = join_path_for_test(target_dir, "manifest");
  EXPECT_NE(std::string::npos, spill_path.find("segment-000001.dat"));
  EXPECT_EQ(0, my_access(spill_path.c_str(), F_OK));
  EXPECT_EQ(0, my_access(manifest_path.c_str(), F_OK));

  Preserve_trx_lock_warmcopy_target_observation target;
  ASSERT_TRUE(participant.target_observation_for_thread_for_unit_test(
      42, &target));
  EXPECT_EQ(Preserve_trx_lock_warmcopy_target_state::SEALED_VALID,
            target.state);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::OK, target.reason);
  EXPECT_TRUE(target.has_artifact);
  EXPECT_TRUE(target.artifact_valid);
  EXPECT_GT(target.bytes_used, 0ULL);

  participant.finalize_phase();
  EXPECT_NE(0, my_access(spill_path.c_str(), F_OK));
  EXPECT_NE(0, my_access(manifest_path.c_str(), F_OK));
  EXPECT_NE(0, my_access(target_dir.c_str(), F_OK));
  EXPECT_NE(0, my_access(batch_dir.c_str(), F_OK));
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     OrphanSpillCleanupRemovesKnownBatchTargetArtifacts) {
  const std::string root =
      preserve_trx_lock_warmcopy_spill_root_dir_for_unit_test();
  const std::string batch_dir = join_path_for_test(root, "batch-987654");
  const std::string target_dir = join_path_for_test(batch_dir, "target-42");
  const std::string ignored_dir = join_path_for_test(root, "not-a-batch");
  const std::string ignored_file =
      join_path_for_test(ignored_dir, "segment-000001.dat");
  const std::string owner_path = join_path_for_test(root, "owner");

  ensure_dir_for_test(parent_path_for_test(root));
  ensure_dir_for_test(root);
  ensure_dir_for_test(batch_dir);
  ensure_dir_for_test(target_dir);
  ensure_dir_for_test(ignored_dir);
  ASSERT_TRUE(preserve_trx_lock_warmcopy_write_spill_owner_marker_for_unit_test());
  create_file_for_test(join_path_for_test(target_dir, "manifest"));
  create_file_for_test(join_path_for_test(target_dir, "manifest.tmp"));
  create_file_for_test(join_path_for_test(target_dir, "segment-000001.dat"));
  create_file_for_test(
      join_path_for_test(target_dir, "segment-000001.dat.tmp"));
  create_file_for_test(join_path_for_test(target_dir, "artifact.dat"));
  create_file_for_test(join_path_for_test(target_dir, "artifact.dat.tmp"));
  create_file_for_test(ignored_file);

  ASSERT_TRUE(preserve_trx_lock_warmcopy_cleanup_orphan_spill_files());

  EXPECT_NE(0, my_access(target_dir.c_str(), F_OK));
  EXPECT_NE(0, my_access(batch_dir.c_str(), F_OK));
  EXPECT_EQ(0, my_access(ignored_file.c_str(), F_OK));

  (void)my_delete(ignored_file.c_str(), MYF(0));
  (void)my_delete(owner_path.c_str(), MYF(0));
  (void)rmdir(ignored_dir.c_str());
  (void)rmdir(root.c_str());
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     OrphanSpillCleanupPreservesForeignOwnerArtifacts) {
  const std::string root =
      join_path_for_test(mysql_tmpdir, "preserve-lock-warmcopy");
  const std::string batch_dir = join_path_for_test(root, "batch-777001");
  const std::string target_dir = join_path_for_test(batch_dir, "target-42");
  const std::string owner_path = join_path_for_test(root, "owner");
  const std::string manifest_path = join_path_for_test(target_dir, "manifest");
  const std::string segment_path =
      join_path_for_test(target_dir, "segment-000001.dat");

  ensure_dir_for_test(root);
  ensure_dir_for_test(batch_dir);
  ensure_dir_for_test(target_dir);
  create_file_for_test(owner_path);
  create_file_for_test(manifest_path);
  create_file_for_test(segment_path);

  ASSERT_TRUE(preserve_trx_lock_warmcopy_cleanup_orphan_spill_files());

  EXPECT_EQ(0, my_access(owner_path.c_str(), F_OK));
  EXPECT_EQ(0, my_access(manifest_path.c_str(), F_OK));
  EXPECT_EQ(0, my_access(segment_path.c_str(), F_OK));

  (void)my_delete(manifest_path.c_str(), MYF(0));
  (void)my_delete(segment_path.c_str(), MYF(0));
  (void)my_delete(owner_path.c_str(), MYF(0));
  (void)rmdir(target_dir.c_str());
  (void)rmdir(batch_dir.c_str());
  (void)rmdir(root.c_str());
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     OrphanSpillCleanupPreservesMismatchedOwnerRoot) {
  const std::string root =
      preserve_trx_lock_warmcopy_spill_root_dir_for_unit_test();
  const std::string batch_dir = join_path_for_test(root, "batch-777002");
  const std::string target_dir = join_path_for_test(batch_dir, "target-42");
  const std::string owner_path = join_path_for_test(root, "owner");
  const std::string manifest_path = join_path_for_test(target_dir, "manifest");
  const std::string segment_path =
      join_path_for_test(target_dir, "segment-000001.dat");

  ensure_dir_for_test(parent_path_for_test(root));
  ensure_dir_for_test(root);
  ensure_dir_for_test(batch_dir);
  ensure_dir_for_test(target_dir);
  create_file_with_payload_for_test(owner_path, "foreign-owner\n");
  create_file_for_test(manifest_path);
  create_file_for_test(segment_path);

  ASSERT_TRUE(preserve_trx_lock_warmcopy_cleanup_orphan_spill_files());

  EXPECT_EQ(0, my_access(owner_path.c_str(), F_OK));
  EXPECT_EQ(0, my_access(manifest_path.c_str(), F_OK));
  EXPECT_EQ(0, my_access(segment_path.c_str(), F_OK));

  (void)my_delete(manifest_path.c_str(), MYF(0));
  (void)my_delete(segment_path.c_str(), MYF(0));
  (void)my_delete(owner_path.c_str(), MYF(0));
  (void)rmdir(target_dir.c_str());
  (void)rmdir(batch_dir.c_str());
  (void)rmdir(root.c_str());
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     SpilledArtifactChecksumFailureInvalidatesTarget) {
  lock_warmcopy_reset_for_unit_test();
  lock_warmcopy_record_shard_key_t key;
  key.table_id = 100;
  key.index_id = 200;
  key.space_id = 7;
  key.page_no = 11;
  key.lock_type_mode = 3;
  key.n_bits = 8;
  lock_warmcopy_record_image_digest_t digest;
  digest.bytes[0] = 0xa1;
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key, 2, digest, 20, make_record_image('b')));

  Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();
  options.max_memory_bytes = 1;
  Preserve_trx_lock_warmcopy_drain_participant participant(options);
  EXPECT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  EXPECT_TRUE(participant.close_phase1());
  EXPECT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));
  ASSERT_TRUE(participant.corrupt_spilled_artifact_for_thread_for_unit_test(42));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  EXPECT_FALSE(artifact->valid);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID,
            artifact->reason);

  Preserve_trx_lock_warmcopy_target_observation target;
  ASSERT_TRUE(participant.target_observation_for_thread_for_unit_test(
      42, &target));
  EXPECT_EQ(Preserve_trx_lock_warmcopy_target_state::SEALED_INVALID,
            target.state);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID,
            target.reason);
  EXPECT_TRUE(target.has_artifact);
  EXPECT_FALSE(target.artifact_valid);
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     MissingSpillManifestInvalidatesTarget) {
  lock_warmcopy_reset_for_unit_test();
  lock_warmcopy_record_shard_key_t key;
  key.table_id = 100;
  key.index_id = 200;
  key.space_id = 7;
  key.page_no = 11;
  key.lock_type_mode = 3;
  key.n_bits = 8;
  lock_warmcopy_record_image_digest_t digest;
  digest.bytes[0] = 0xa2;
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key, 2, digest, 20, make_record_image('c')));

  Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();
  options.max_memory_bytes = 1;
  Preserve_trx_lock_warmcopy_drain_participant participant(options);
  EXPECT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  EXPECT_TRUE(participant.close_phase1());
  EXPECT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));
  ASSERT_TRUE(participant.corrupt_spill_manifest_for_thread_for_unit_test(42));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  EXPECT_FALSE(artifact->valid);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID,
            artifact->reason);

  Preserve_trx_lock_warmcopy_target_observation target;
  ASSERT_TRUE(participant.target_observation_for_thread_for_unit_test(
      42, &target));
  EXPECT_EQ(Preserve_trx_lock_warmcopy_target_state::SEALED_INVALID,
            target.state);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID,
            target.reason);
  EXPECT_TRUE(target.has_artifact);
  EXPECT_FALSE(target.artifact_valid);
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase2PreflightRejectsRecordStoreAboveJournalBudget) {
  lock_warmcopy_reset_for_unit_test();
  lock_warmcopy_record_shard_key_t key;
  key.table_id = 100;
  key.index_id = 200;
  key.space_id = 7;
  key.page_no = 11;
  key.lock_type_mode = 3;
  key.n_bits = 8;
  lock_warmcopy_record_image_digest_t digest;
  digest.bytes[0] = 0xb0;
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key, 2, digest, 20, make_record_image('a')));

  Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();
  options.max_journal_bytes = 1;
  Preserve_trx_lock_warmcopy_drain_participant participant(options);
  EXPECT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  EXPECT_TRUE(participant.close_phase1());
  EXPECT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  EXPECT_FALSE(artifact->valid);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED,
            artifact->reason);
  EXPECT_TRUE(artifact->record_locks_payload.empty());
  EXPECT_EQ("record_journal_budget_exceeded",
            participant.observation().failure_reason);

  Preserve_trx_lock_warmcopy_target_observation target;
  ASSERT_TRUE(participant.target_observation_for_thread_for_unit_test(
      42, &target));
  EXPECT_EQ(Preserve_trx_lock_warmcopy_target_state::SEALED_INVALID,
            target.state);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED,
            target.reason);
  EXPECT_TRUE(target.has_artifact);
  EXPECT_FALSE(target.artifact_valid);
  EXPECT_EQ(0ULL, target.bytes_used);
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase2PreflightRejectsRecordStoreAboveDirtyShardLimit) {
  lock_warmcopy_reset_for_unit_test();
  lock_warmcopy_record_shard_key_t key;
  key.table_id = 100;
  key.index_id = 200;
  key.space_id = 7;
  key.page_no = 11;
  key.lock_type_mode = 3;
  key.n_bits = 8;
  lock_warmcopy_record_image_digest_t digest;
  digest.bytes[0] = 0xc0;
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key, 2, digest, 20, make_record_image('a')));

  Preserve_trx_lock_warmcopy_options options =
      preserve_trx_lock_warmcopy_current_options();
  options.max_dirty_shards = 0;
  Preserve_trx_lock_warmcopy_drain_participant participant(options);
  EXPECT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  EXPECT_TRUE(participant.close_phase1());
  EXPECT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  EXPECT_FALSE(artifact->valid);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED,
            artifact->reason);
  EXPECT_TRUE(artifact->record_locks_payload.empty());
  EXPECT_EQ("record_dirty_shard_limit_exceeded",
            participant.observation().failure_reason);

  Preserve_trx_lock_warmcopy_target_observation target;
  ASSERT_TRUE(participant.target_observation_for_thread_for_unit_test(
      42, &target));
  EXPECT_EQ(Preserve_trx_lock_warmcopy_target_state::SEALED_INVALID,
            target.state);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED,
            target.reason);
  EXPECT_TRUE(target.has_artifact);
  EXPECT_FALSE(target.artifact_valid);
  EXPECT_EQ(0ULL, target.bytes_used);
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     PrepareQuiescedTargetsCreatesSessionsAndClearsStaleArtifacts) {
  Preserve_trx_lock_warmcopy_drain_participant participant(
      preserve_trx_lock_warmcopy_current_options());
  Preserve_trx_lock_warmcopy_artifact stale_artifact;
  stale_artifact.valid = true;
  stale_artifact.source = Preserve_trx_lock_warmcopy_artifact_source::WARM_COPY;
  stale_artifact.record_locks_payload = "stale";
  participant.set_artifact_for_thread_for_unit_test(7, stale_artifact);

  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42, 43}));

  EXPECT_EQ(nullptr, participant.artifact_for_thread(7));

  Preserve_trx_lock_warmcopy_target_observation target_42;
  ASSERT_TRUE(participant.target_observation_for_thread_for_unit_test(
      42, &target_42));
  EXPECT_EQ(42ULL, target_42.thread_id);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_target_state::JOURNAL_OPEN,
            target_42.state);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::NOT_ATTEMPTED,
            target_42.reason);
  EXPECT_FALSE(target_42.has_artifact);

  Preserve_trx_lock_warmcopy_target_observation target_43;
  ASSERT_TRUE(participant.target_observation_for_thread_for_unit_test(
      43, &target_43));
  EXPECT_EQ(Preserve_trx_lock_warmcopy_target_state::JOURNAL_OPEN,
            target_43.state);
  EXPECT_FALSE(participant.target_observation_for_thread_for_unit_test(
      7, &target_43));
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     CompleteEarlyPreparedTargetsPrunesPhase1OnlyTargets) {
  Preserve_trx_lock_warmcopy_drain_participant participant(
      preserve_trx_lock_warmcopy_current_options());
  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42, 43}));

  ASSERT_TRUE(participant.complete_early_prepared_targets({42}));

  Preserve_trx_lock_warmcopy_target_observation target;
  EXPECT_TRUE(
      participant.target_observation_for_thread_for_unit_test(42, &target));
  EXPECT_FALSE(
      participant.target_observation_for_thread_for_unit_test(43, &target));
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     Phase2PreflightCreatesValidEmptyArtifactsForMultipleTargets) {
  lock_warmcopy_reset_for_unit_test();

  Preserve_trx_lock_warmcopy_drain_participant participant(
      preserve_trx_lock_warmcopy_current_options());
  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42, 43}));
  ASSERT_TRUE(participant.close_phase1());
  EXPECT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_drain_participant_observation observation =
      participant.observation();
  EXPECT_EQ(Preserve_trx_drain_participant_state::READY,
            observation.state);
  EXPECT_TRUE(observation.owns_artifact);
  EXPECT_GT(observation.bytes_used, 0ULL);
  EXPECT_TRUE(observation.failure_reason.empty());

  for (const uint64_t thread_id : {42ULL, 43ULL}) {
    const Preserve_trx_lock_warmcopy_artifact *artifact =
        participant.artifact_for_thread(thread_id);
    ASSERT_NE(nullptr, artifact);
    EXPECT_TRUE(artifact->valid);
    EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::OK,
              artifact->reason);
    EXPECT_TRUE(artifact->record_locks_payload.empty());
    EXPECT_TRUE(artifact->table_locks_payload.empty());
    EXPECT_FALSE(artifact->mdl_descriptors_payload.empty());
    EXPECT_EQ(0U, artifact->record_predicate_table_lock_count);
    EXPECT_EQ(0U, artifact->mdl_descriptor_count);

    Preserve_trx_lock_warmcopy_target_observation target;
    ASSERT_TRUE(participant.target_observation_for_thread_for_unit_test(
        thread_id, &target));
    EXPECT_EQ(Preserve_trx_lock_warmcopy_target_state::SEALED_VALID,
              target.state);
    EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::OK,
              target.reason);
    EXPECT_TRUE(target.has_artifact);
    EXPECT_TRUE(target.artifact_valid);
    EXPECT_EQ(0U, target.record_predicate_table_lock_count);
    EXPECT_EQ(0U, target.mdl_descriptor_count);
    EXPECT_GT(target.bytes_used, 0ULL);
  }
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     ExportFailureCreatesSingleTargetInvalidArtifactWithoutGlobalFailure) {
  lock_warmcopy_reset_for_unit_test();
  lock_warmcopy_record_shard_key_t key;
  key.table_id = 100;
  key.index_id = 200;
  key.space_id = 7;
  key.page_no = 11;
  key.lock_type_mode = 3;
  key.n_bits = 8;
  lock_warmcopy_record_image_digest_t digest;
  digest.bytes[0] = 0xa0;
  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_for_target_for_unit_test(
          42, key, 2, digest));

  Preserve_trx_lock_warmcopy_drain_participant participant(
      preserve_trx_lock_warmcopy_current_options());
  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  ASSERT_TRUE(participant.close_phase1());
  EXPECT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  const Preserve_trx_lock_warmcopy_artifact *artifact =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, artifact);
  EXPECT_FALSE(artifact->valid);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID,
            artifact->reason);

  Preserve_trx_lock_warmcopy_target_observation target;
  ASSERT_TRUE(participant.target_observation_for_thread_for_unit_test(
      42, &target));
  EXPECT_EQ(Preserve_trx_lock_warmcopy_target_state::SEALED_INVALID,
            target.state);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID,
            target.reason);
  EXPECT_TRUE(target.has_artifact);
  EXPECT_FALSE(target.artifact_valid);
  EXPECT_EQ(0ULL, target.bytes_used);
}

TEST(PreserveTrxLockWarmcopyDrainParticipant, AbortMarksArtifactAbandoned) {
  Preserve_trx_lock_warmcopy_drain_participant participant(
      preserve_trx_lock_warmcopy_current_options());

  EXPECT_TRUE(participant.open_phase1());
  participant.abort_phase();

  const Preserve_trx_drain_participant_observation observation =
      participant.observation();
  EXPECT_EQ(Preserve_trx_drain_participant_state::ABANDONED,
            observation.state);
  EXPECT_FALSE(observation.owns_artifact);
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     FinalizeAndAbortClearRecordStoreForPreparedTargets) {
  lock_warmcopy_reset_for_unit_test();
  lock_warmcopy_record_shard_key_t key;
  key.table_id = 100;
  key.index_id = 200;
  key.space_id = 7;
  key.page_no = 11;
  key.lock_type_mode = 3;
  key.n_bits = 8;
  lock_warmcopy_record_image_digest_t digest;
  digest.bytes[0] = 0xa0;

  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key, 2, digest, 20, make_record_image('a')));
  {
    Preserve_trx_lock_warmcopy_drain_participant participant(
        preserve_trx_lock_warmcopy_current_options());
    ASSERT_TRUE(participant.open_phase1());
    ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
    ASSERT_TRUE(participant.close_phase1());
    ASSERT_TRUE(participant.phase2_preflight(
        Preserve_trx_drain_phase_mode::TWO_PHASE));
    participant.finalize_phase();
  }
  lock_warmcopy_record_shard_snapshot_t snapshot;
  EXPECT_FALSE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      42, key, &snapshot));

  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          43, key, 3, digest, 30, make_record_image('b')));
  {
    Preserve_trx_lock_warmcopy_drain_participant participant(
        preserve_trx_lock_warmcopy_current_options());
    ASSERT_TRUE(participant.open_phase1());
    ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({43}));
    participant.abort_phase();
  }
  EXPECT_FALSE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      43, key, &snapshot));
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     ShutdownFinalizeDefersRecordStoreClearUntilShutdownFailure) {
  lock_warmcopy_reset_for_unit_test();
  lock_warmcopy_record_shard_key_t key;
  key.table_id = 100;
  key.index_id = 200;
  key.space_id = 7;
  key.page_no = 11;
  key.lock_type_mode = 3;
  key.n_bits = 8;
  lock_warmcopy_record_image_digest_t digest;
  digest.bytes[0] = 0xa0;

  ASSERT_TRUE(
      lock_warmcopy_record_bitmap_set_with_image_for_target_for_unit_test(
          42, key, 2, digest, 20, make_record_image('a')));

  Preserve_trx_lock_warmcopy_drain_participant participant(
      preserve_trx_lock_warmcopy_current_options());
  ASSERT_TRUE(participant.open_phase1());
  ASSERT_TRUE(participant.prepare_quiesced_targets_for_unit_test({42}));
  ASSERT_TRUE(participant.close_phase1());
  ASSERT_TRUE(participant.phase2_preflight(
      Preserve_trx_drain_phase_mode::TWO_PHASE));

  participant.finalize_phase_for_shutdown();

  lock_warmcopy_record_shard_snapshot_t snapshot;
  EXPECT_TRUE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      42, key, &snapshot));
  ASSERT_EQ(1U, snapshot.normalized_bitmap.size());
  EXPECT_NE(0U,
            static_cast<unsigned char>(snapshot.normalized_bitmap[0]) &
                (1U << 2));

  participant.cleanup_after_failed_shutdown();
  EXPECT_FALSE(lock_warmcopy_record_shard_snapshot_for_target_for_unit_test(
      42, key, &snapshot));
}

TEST(PreserveTrxLockWarmcopyDrainParticipant,
     ArtifactsAreAddressedByTargetThreadId) {
  Preserve_trx_lock_warmcopy_drain_participant participant(
      preserve_trx_lock_warmcopy_current_options());
  Preserve_trx_lock_warmcopy_artifact artifact;
  artifact.valid = true;
  artifact.source = Preserve_trx_lock_warmcopy_artifact_source::WARM_COPY;
  artifact.record_locks_payload = "record";

  EXPECT_EQ(nullptr, participant.artifact_for_thread(42));
  participant.set_artifact_for_thread_for_unit_test(42, artifact);

  const Preserve_trx_lock_warmcopy_artifact *stored =
      participant.artifact_for_thread(42);
  ASSERT_NE(nullptr, stored);
  EXPECT_TRUE(stored->valid);
  EXPECT_EQ("record", stored->record_locks_payload);
  EXPECT_EQ(nullptr, participant.artifact_for_thread(43));
}

TEST(PreserveTrxLockWarmcopyArtifact, ReasonNamesAreStable) {
  EXPECT_STREQ("ok", preserve_trx_lock_warmcopy_reason_name(
                          Preserve_trx_lock_warmcopy_reason::OK));
  EXPECT_STREQ("not_attempted",
               preserve_trx_lock_warmcopy_reason_name(
                   Preserve_trx_lock_warmcopy_reason::NOT_ATTEMPTED));
  EXPECT_STREQ("artifact_invalid",
               preserve_trx_lock_warmcopy_reason_name(
                   Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID));
  EXPECT_STREQ("unsupported_family",
               preserve_trx_lock_warmcopy_reason_name(
                   Preserve_trx_lock_warmcopy_reason::UNSUPPORTED_FAMILY));
  EXPECT_STREQ("eligibility_reject",
               preserve_trx_lock_warmcopy_reason_name(
                   Preserve_trx_lock_warmcopy_reason::ELIGIBILITY_REJECT));
  EXPECT_STREQ("resource_limit_exceeded",
               preserve_trx_lock_warmcopy_reason_name(
                   Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED));
  EXPECT_STREQ(
      "canonical_equivalence_failed",
      preserve_trx_lock_warmcopy_reason_name(
          Preserve_trx_lock_warmcopy_reason::CANONICAL_EQUIVALENCE_FAILED));
  EXPECT_STREQ(
      "table_lock_warmcopy_post_prepare_drift",
      preserve_trx_lock_warmcopy_reason_name(
          Preserve_trx_lock_warmcopy_reason::TABLE_POST_PREPARE_DRIFT));
  EXPECT_STREQ("seal_fence_changed",
               preserve_trx_lock_warmcopy_reason_name(
                   Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED));
  EXPECT_STREQ("unknown", preserve_trx_lock_warmcopy_reason_name(
                              Preserve_trx_lock_warmcopy_reason::UNKNOWN));
}

TEST(PreserveTrxLockWarmcopyArtifact, ValidArtifactRoutesToWarmcopy) {
  Preserve_trx_lock_warmcopy_artifact artifact;
  artifact.valid = true;
  artifact.source = Preserve_trx_lock_warmcopy_artifact_source::WARM_COPY;
  artifact.record_locks_payload = "record";
  artifact.table_locks_payload = "table";
  artifact.mdl_descriptors_payload = "mdl";
  artifact.record_predicate_table_lock_count = 2;
  artifact.mdl_descriptor_count = 1;

  Preserve_trx_lock_warmcopy_options options;
  options.fallback_to_live_export = true;
  const Preserve_trx_lock_warmcopy_route route =
      preserve_trx_lock_warmcopy_route_artifact(&artifact, options);

  EXPECT_EQ(Preserve_trx_lock_warmcopy_route_action::USE_WARM_COPY,
            route.action);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::OK, route.reason);
}

TEST(PreserveTrxLockWarmcopyArtifact,
     InvalidArtifactFallsBackOrRejectsByPolicy) {
  Preserve_trx_lock_warmcopy_artifact artifact;
  artifact.valid = false;
  artifact.reason = Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED;

  Preserve_trx_lock_warmcopy_options options;
  options.fallback_to_live_export = true;
  Preserve_trx_lock_warmcopy_route route =
      preserve_trx_lock_warmcopy_route_artifact(&artifact, options);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_route_action::FALLBACK_TO_LIVE_EXPORT,
            route.action);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED,
            route.reason);

  options.fallback_to_live_export = false;
  route = preserve_trx_lock_warmcopy_route_artifact(&artifact, options);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_route_action::REJECT, route.action);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED,
            route.reason);
}

TEST(PreserveTrxLockWarmcopyArtifact,
     PredicatePayloadFallsBackOrRejectsEvenWhenArtifactClaimsValid) {
  Preserve_trx_lock_warmcopy_artifact artifact;
  artifact.valid = true;
  artifact.source = Preserve_trx_lock_warmcopy_artifact_source::WARM_COPY;
  artifact.predicate_locks_payload = "spatial-predicate-locks";

  Preserve_trx_lock_warmcopy_options options;
  options.fallback_to_live_export = true;
  Preserve_trx_lock_warmcopy_route route =
      preserve_trx_lock_warmcopy_route_artifact(&artifact, options);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_route_action::FALLBACK_TO_LIVE_EXPORT,
            route.action);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::UNSUPPORTED_FAMILY,
            route.reason);

  options.fallback_to_live_export = false;
  route = preserve_trx_lock_warmcopy_route_artifact(&artifact, options);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_route_action::REJECT, route.action);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::UNSUPPORTED_FAMILY,
            route.reason);
}

TEST(PreserveTrxLockWarmcopyArtifact,
     EligibilityRejectReasonAlwaysRejectsWithoutLiveFallback) {
  Preserve_trx_lock_warmcopy_artifact artifact;
  artifact.valid = false;
  artifact.reason = Preserve_trx_lock_warmcopy_reason::ELIGIBILITY_REJECT;

  Preserve_trx_lock_warmcopy_options options;
  options.enabled = true;
  options.fallback_to_live_export = true;
  Preserve_trx_lock_warmcopy_route route =
      preserve_trx_lock_warmcopy_route_artifact(&artifact, options);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_route_action::REJECT, route.action);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::ELIGIBILITY_REJECT,
            route.reason);

  options.fallback_to_live_export = false;
  route = preserve_trx_lock_warmcopy_route_artifact(&artifact, options);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_route_action::REJECT, route.action);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::ELIGIBILITY_REJECT,
            route.reason);
}

TEST(PreserveTrxLockWarmcopyArtifact,
     MissingArtifactFallsBackOrRejectsWhenWarmcopyEnabled) {
  Preserve_trx_lock_warmcopy_options options;
  options.enabled = true;
  options.fallback_to_live_export = true;
  Preserve_trx_lock_warmcopy_route route =
      preserve_trx_lock_warmcopy_route_artifact(nullptr, options);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_route_action::FALLBACK_TO_LIVE_EXPORT,
            route.action);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::NOT_ATTEMPTED, route.reason);

  options.fallback_to_live_export = false;
  route =
      preserve_trx_lock_warmcopy_route_artifact(nullptr, options);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_route_action::REJECT, route.action);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::NOT_ATTEMPTED, route.reason);
}

TEST(PreserveTrxLockWarmcopyArtifact,
     MissingArtifactUsesLiveExportWhenWarmcopyDisabled) {
  Preserve_trx_lock_warmcopy_options options;
  options.enabled = false;
  options.fallback_to_live_export = false;
  const Preserve_trx_lock_warmcopy_route route =
      preserve_trx_lock_warmcopy_route_artifact(nullptr, options);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_route_action::FALLBACK_TO_LIVE_EXPORT,
            route.action);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::NOT_ATTEMPTED, route.reason);
}

TEST(PreserveTrxLockWarmcopyArtifact,
     FinalRecordFenceRejectsMissingChangedOrUnhandledConversion) {
  Preserve_trx_lock_warmcopy_artifact artifact;
  artifact.valid = true;
  artifact.source = Preserve_trx_lock_warmcopy_artifact_source::WARM_COPY;
  artifact.record_locks_payload = make_record_payload(
      {make_record_entry(10, 20, 30, 2, 'a')});

  lock_warmcopy_trx_lock_fence_t current;
  current.trx_locks_version = 9;
  current.n_rec_locks = 1;

  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID,
            preserve_trx_lock_warmcopy_verify_record_final_fence(
                artifact, current));

  artifact.record_live_seal_fence_valid = true;
  artifact.record_live_seal_fence = current;
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::OK,
            preserve_trx_lock_warmcopy_verify_record_final_fence(
                artifact, current));

  lock_warmcopy_trx_lock_fence_t changed_count = current;
  ++changed_count.n_rec_locks;
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED,
            preserve_trx_lock_warmcopy_verify_record_final_fence(
                artifact, changed_count));

  lock_warmcopy_trx_lock_fence_t changed_version = current;
  ++changed_version.trx_locks_version;
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED,
            preserve_trx_lock_warmcopy_verify_record_final_fence(
                artifact, changed_version));

  lock_warmcopy_trx_lock_fence_t conversion_attempt = current;
  conversion_attempt.conversion_unhandled_after_freeze = true;
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED,
            preserve_trx_lock_warmcopy_verify_record_final_fence(
                artifact, conversion_attempt));
}

TEST(PreserveTrxLockWarmcopyArtifact,
     FinalFenceRouteFallsBackOrRejectsByPolicy) {
  Preserve_trx_lock_warmcopy_options options;
  options.fallback_to_live_export = true;

  Preserve_trx_lock_warmcopy_route route =
      preserve_trx_lock_warmcopy_route_final_fence(
          Preserve_trx_lock_warmcopy_reason::OK, options);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_route_action::USE_WARM_COPY,
            route.action);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::OK, route.reason);

  route = preserve_trx_lock_warmcopy_route_final_fence(
      Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED, options);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_route_action::FALLBACK_TO_LIVE_EXPORT,
            route.action);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED,
            route.reason);

  options.fallback_to_live_export = false;
  route = preserve_trx_lock_warmcopy_route_final_fence(
      Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED, options);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_route_action::REJECT, route.action);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED,
            route.reason);

  options.fallback_to_live_export = true;
  route = preserve_trx_lock_warmcopy_route_final_fence(
      Preserve_trx_lock_warmcopy_reason::ELIGIBILITY_REJECT, options);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_route_action::REJECT, route.action);
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::ELIGIBILITY_REJECT,
            route.reason);
}

TEST(PreserveTrxLockWarmcopyCanonicalPayload,
     RecordPayloadComparatorIgnoresEntryOrder) {
  const std::string entry_a = make_record_entry(100, 200, 11, 2, 'a');
  const std::string entry_b = make_record_entry(101, 201, 12, 3, 'b');
  const std::string live_payload = make_record_payload({entry_a, entry_b});
  const std::string warmcopy_payload = make_record_payload({entry_b, entry_a});

  Preserve_trx_lock_warmcopy_canonical_compare_result result =
      preserve_trx_lock_warmcopy_compare_record_payloads_canonical(
          live_payload, warmcopy_payload);

  EXPECT_TRUE(result.equivalent);
  EXPECT_TRUE(result.difference.empty());
}

TEST(PreserveTrxLockWarmcopyCanonicalPayload,
     RecordPayloadComparatorIgnoresLivePageContextFields) {
  const std::string live_payload =
      make_record_payload({make_record_entry(
          100, 200, 11, 2, 'a', 0x0102030405060708ULL, 42)});
  const std::string warmcopy_payload =
      make_record_payload({make_record_entry(100, 200, 11, 2, 'a', 0, 8)});

  Preserve_trx_lock_warmcopy_canonical_compare_result result =
      preserve_trx_lock_warmcopy_compare_record_payloads_canonical(
          live_payload, warmcopy_payload);

  EXPECT_TRUE(result.equivalent) << result.difference;
  EXPECT_TRUE(result.difference.empty());
}

TEST(PreserveTrxLockWarmcopyCanonicalPayload,
     RecordPayloadComparatorDetectsSemanticDifference) {
  const std::string live_payload =
      make_record_payload({make_record_entry(100, 200, 11, 2, 'a')});
  const std::string warmcopy_payload =
      make_record_payload({make_record_entry(100, 200, 11, 3, 'a')});

  Preserve_trx_lock_warmcopy_canonical_compare_result result =
      preserve_trx_lock_warmcopy_compare_record_payloads_canonical(
          live_payload, warmcopy_payload);

  EXPECT_FALSE(result.equivalent);
  EXPECT_NE(std::string::npos, result.difference.find("entry_mismatch"));
}

TEST(PreserveTrxLockWarmcopyCanonicalPayload,
     RecordPayloadComparatorRejectsInvalidPayload) {
  std::string truncated_payload =
      make_record_payload({make_record_entry(100, 200, 11, 2, 'a')});
  truncated_payload.resize(truncated_payload.size() - 1);

  Preserve_trx_lock_warmcopy_canonical_compare_result result =
      preserve_trx_lock_warmcopy_compare_record_payloads_canonical(
          truncated_payload, truncated_payload);

  EXPECT_FALSE(result.equivalent);
  EXPECT_NE(std::string::npos, result.difference.find("parse_failed"));
}

TEST(PreserveTrxLockWarmcopyCanonicalPayload,
     RecordStoreExportComparesCanonicallyAgainstLivePayload) {
  lock_warmcopy_reset_for_unit_test();

  lock_warmcopy_record_shard_key_t key_a;
  key_a.table_id = 100;
  key_a.index_id = 200;
  key_a.space_id = 7;
  key_a.page_no = 11;
  key_a.lock_type_mode = 3;
  key_a.n_bits = 8;
  lock_warmcopy_record_shard_key_t key_b = key_a;
  key_b.table_id = 101;
  key_b.index_id = 201;
  key_b.page_no = 12;

  lock_warmcopy_record_image_digest_t digest_a;
  digest_a.bytes[0] = 0xa0;
  lock_warmcopy_record_image_digest_t digest_b;
  digest_b.bytes[0] = 0xb0;

  ASSERT_TRUE(lock_warmcopy_record_bitmap_set_with_image_for_unit_test(
      key_b, 3, digest_b, 30, make_record_image('b')));
  ASSERT_TRUE(lock_warmcopy_record_bitmap_set_with_image_for_unit_test(
      key_a, 2, digest_a, 20, make_record_image('a')));

  std::string warmcopy_payload;
  ASSERT_TRUE(
      lock_warmcopy_record_store_export_record_payload_for_unit_test(
          &warmcopy_payload));

  const std::string live_payload = make_record_payload(
      {make_record_entry(101, 201, 12, 3, 'b', 0x1112131415161718ULL, 24),
       make_record_entry(100, 200, 11, 2, 'a', 0x0102030405060708ULL, 32)});

  Preserve_trx_lock_warmcopy_canonical_compare_result result =
      preserve_trx_lock_warmcopy_compare_record_payloads_canonical(
          live_payload, warmcopy_payload);

  EXPECT_TRUE(result.equivalent) << result.difference;
}

TEST(PreserveTrxLockWarmcopyCanonicalPayload,
     TablePayloadComparatorIgnoresEntryOrder) {
  const std::string entry_a = make_table_entry(100, 1);
  const std::string entry_b = make_table_entry(101, 4);
  const std::string live_payload = make_table_payload({entry_a, entry_b});
  const std::string warmcopy_payload = make_table_payload({entry_b, entry_a});

  Preserve_trx_lock_warmcopy_canonical_compare_result result =
      preserve_trx_lock_warmcopy_compare_table_payloads_canonical(
          live_payload, warmcopy_payload);

  EXPECT_TRUE(result.equivalent);
  EXPECT_TRUE(result.difference.empty());
}

TEST(PreserveTrxLockWarmcopyCanonicalPayload,
     TablePayloadComparatorDetectsSemanticDifference) {
  const std::string live_payload =
      make_table_payload({make_table_entry(100, 1)});
  const std::string warmcopy_payload =
      make_table_payload({make_table_entry(100, 2)});

  Preserve_trx_lock_warmcopy_canonical_compare_result result =
      preserve_trx_lock_warmcopy_compare_table_payloads_canonical(
          live_payload, warmcopy_payload);

  EXPECT_FALSE(result.equivalent);
  EXPECT_NE(std::string::npos, result.difference.find("entry_mismatch"));
}

TEST(PreserveTrxLockWarmcopyCanonicalPayload,
     TablePayloadComparatorRejectsInvalidPayload) {
  std::string invalid_payload = make_table_payload({make_table_entry(100, 1)});
  invalid_payload[16] = static_cast<char>(17);

  Preserve_trx_lock_warmcopy_canonical_compare_result result =
      preserve_trx_lock_warmcopy_compare_table_payloads_canonical(
          invalid_payload, invalid_payload);

  EXPECT_FALSE(result.equivalent);
  EXPECT_NE(std::string::npos, result.difference.find("parse_failed"));
}

TEST(PreserveTrxLockWarmcopyCanonicalPayload,
     MdlPayloadComparatorPreservesTransactionListOrdinal) {
  const std::string table_first =
      make_mdl_entry(MDL_key::TABLE, MDL_SHARED_WRITE, "db", "t1", 1);
  const std::string schema_second =
      make_mdl_entry(MDL_key::SCHEMA, MDL_INTENTION_EXCLUSIVE, "db", "", 2);
  const std::string schema_first =
      make_mdl_entry(MDL_key::SCHEMA, MDL_INTENTION_EXCLUSIVE, "db", "", 1);
  const std::string table_second =
      make_mdl_entry(MDL_key::TABLE, MDL_SHARED_WRITE, "db", "t1", 2);

  const std::string live_payload =
      make_mdl_payload({table_first, schema_second});
  const std::string warmcopy_payload =
      make_mdl_payload({schema_first, table_second});

  Preserve_trx_lock_warmcopy_canonical_compare_result result =
      preserve_trx_lock_warmcopy_compare_mdl_payloads_canonical(
          live_payload, warmcopy_payload);

  EXPECT_FALSE(result.equivalent);
  EXPECT_NE(std::string::npos, result.difference.find("entry_mismatch"));
}

TEST(PreserveTrxLockWarmcopyCanonicalPayload,
     MdlPayloadComparatorKeepsDuplicateDescriptors) {
  const std::string duplicate_first =
      make_mdl_entry(MDL_key::TABLE, MDL_SHARED_READ, "db", "t1", 1);
  const std::string duplicate_second =
      make_mdl_entry(MDL_key::TABLE, MDL_SHARED_READ, "db", "t1", 2);
  const std::string full_payload =
      make_mdl_payload({duplicate_first, duplicate_second});
  const std::string collapsed_payload = make_mdl_payload({duplicate_first});

  Preserve_trx_lock_warmcopy_canonical_compare_result result =
      preserve_trx_lock_warmcopy_compare_mdl_payloads_canonical(
          full_payload, full_payload);
  EXPECT_TRUE(result.equivalent) << result.difference;

  result = preserve_trx_lock_warmcopy_compare_mdl_payloads_canonical(
      full_payload, collapsed_payload);
  EXPECT_FALSE(result.equivalent);
  EXPECT_NE(std::string::npos, result.difference.find("entry_mismatch"));
}

TEST(PreserveTrxLockWarmcopyCanonicalPayload,
     MdlPayloadComparatorAcceptsZeroCountPayload) {
  const std::string zero_count_payload(4, '\0');

  Preserve_trx_lock_warmcopy_canonical_compare_result result =
      preserve_trx_lock_warmcopy_compare_mdl_payloads_canonical(
          zero_count_payload, zero_count_payload);

  EXPECT_TRUE(result.equivalent) << result.difference;
}

TEST(PreserveTrxLockWarmcopyCanonicalPayload,
     MdlPayloadComparatorRejectsInvalidPayload) {
  const std::string invalid_duration_payload = make_mdl_payload(
      {make_mdl_entry(MDL_key::TABLE, MDL_SHARED_READ, "db", "t1", 1,
                      MDL_STATEMENT)});

  Preserve_trx_lock_warmcopy_canonical_compare_result result =
      preserve_trx_lock_warmcopy_compare_mdl_payloads_canonical(
          invalid_duration_payload, invalid_duration_payload);

  EXPECT_FALSE(result.equivalent);
  EXPECT_NE(std::string::npos, result.difference.find("parse_failed"));
}

TEST(PreserveTrxLockWarmcopyFinalFence,
     FrozenFenceIgnoresFreezeGenerationButRejectsLockDrift) {
  Preserve_trx_lock_warmcopy_artifact artifact;
  artifact.valid = true;
  artifact.record_live_seal_fence_valid = true;
  artifact.record_live_seal_fence.trx_locks_version = 10;
  artifact.record_live_seal_fence.n_rec_locks = 3;
  artifact.record_live_seal_fence.freeze_generation = 0;

  lock_warmcopy_trx_lock_fence_t frozen_fence;
  frozen_fence.trx_locks_version = 10;
  frozen_fence.n_rec_locks = 3;
  frozen_fence.freeze_generation = 1;

  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::OK,
            preserve_trx_lock_warmcopy_verify_record_final_fence(
                artifact, frozen_fence));

  frozen_fence.n_rec_locks = 4;
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED,
            preserve_trx_lock_warmcopy_verify_record_final_fence(
                artifact, frozen_fence));

  frozen_fence.n_rec_locks = 3;
  frozen_fence.trx_locks_version = 11;
  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED,
            preserve_trx_lock_warmcopy_verify_record_final_fence(
                artifact, frozen_fence));
}

TEST(PreserveTrxLockWarmcopyFinalFence,
     FrozenFenceRejectsConversionDuringFreeze) {
  Preserve_trx_lock_warmcopy_artifact artifact;
  artifact.valid = true;
  artifact.record_live_seal_fence_valid = true;
  artifact.record_live_seal_fence.trx_locks_version = 10;
  artifact.record_live_seal_fence.n_rec_locks = 3;

  lock_warmcopy_trx_lock_fence_t frozen_fence;
  frozen_fence.trx_locks_version = 10;
  frozen_fence.n_rec_locks = 3;
  frozen_fence.freeze_generation = 1;
  frozen_fence.conversion_attempt_after_freeze = true;

  EXPECT_EQ(Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED,
            preserve_trx_lock_warmcopy_verify_record_final_fence(
                artifact, frozen_fence));
}

}  // namespace preserve_trx_lock_warmcopy_unittest
