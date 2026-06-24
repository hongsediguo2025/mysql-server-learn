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

#ifndef SQL_PRESERVE_TRX_LOCK_WARMCOPY_INCLUDED
#define SQL_PRESERVE_TRX_LOCK_WARMCOPY_INCLUDED

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "sql/preserve_trx_bundle.h"
#include "sql/preserve_trx_drain.h"
#include "storage/innobase/include/lock0warmcopy.h"

class THD;
class MDL_context;

struct Preserve_trx_lock_warmcopy_options {
  /*
    These options are copied at drain start. Per-target routing later uses this
    immutable snapshot so sysvar changes during a drain cannot make one target
    strict and another target fallback-capable.
  */
  bool enabled{true};
  bool fallback_to_live_export{true};
  bool validate_canonical_equivalence{false};
  uint64_t max_memory_bytes{268435456ULL};
  uint64_t max_journal_bytes{1073741824ULL};
  uint32_t max_dirty_shards{100000};
  uint32_t max_mdl_descriptors{100000};
  uint32_t max_lock_count{2000};
  uint32_t seal_threads{0};
  uint32_t conversion_wait_timeout_ms{30000};
  std::string preserve_dir;
};

enum class Preserve_trx_lock_warmcopy_reason {
  OK,
  NOT_ATTEMPTED,
  ARTIFACT_INVALID,
  UNSUPPORTED_FAMILY,
  ELIGIBILITY_REJECT,
  RESOURCE_LIMIT_EXCEEDED,
  CANONICAL_EQUIVALENCE_FAILED,
  TABLE_POST_PREPARE_DRIFT,
  SEAL_FENCE_CHANGED,
  UNKNOWN
};

enum class Preserve_trx_lock_warmcopy_artifact_source {
  NONE,
  WARM_COPY,
  LIVE_EXPORT
};

enum class Preserve_trx_lock_warmcopy_route_action {
  USE_WARM_COPY,
  FALLBACK_TO_LIVE_EXPORT,
  REJECT
};

enum class Preserve_trx_lock_warmcopy_target_state {
  NEW,
  JOURNAL_OPEN,
  BASE_SCAN_RUNNING,
  OPEN_VALID,
  OPEN_DIRTY,
  OPEN_UNSUPPORTED,
  SEALING,
  SEALED_VALID,
  SEALED_INVALID,
  SEALED_UNSUPPORTED,
  CONSUMED,
  ABORTED
};

struct Preserve_trx_lock_warmcopy_artifact {
  /*
    A valid artifact represents one consistent lock-family snapshot for a
    target. If any required family becomes invalid, callers must discard the
    whole artifact and route the target through live export or reject it.
  */
  std::string record_locks_payload;
  std::string predicate_locks_payload;
  std::string table_locks_payload;
  std::string mdl_descriptors_payload;
  std::string spill_path;
  PrebuiltRecordLocksBlob prebuilt_record_locks_blob;
  uint64_t spill_payload_bytes{0};
  uint64_t spill_checksum{0};
  bool autoinc_lock_owned{false};
  bool implicit_native_validated{false};
  bool record_live_seal_fence_valid{false};
  bool has_prebuilt_record_locks_blob{false};
  bool spilled_to_file{false};
  bool spill_materialized{false};
  lock_warmcopy_trx_lock_fence_t record_live_seal_fence;
  uint32_t record_lock_count{0};
  uint32_t table_lock_count{0};
  uint32_t record_predicate_table_lock_count{0};
  uint32_t mdl_descriptor_count{0};
  bool valid{false};
  Preserve_trx_lock_warmcopy_reason reason{
      Preserve_trx_lock_warmcopy_reason::NOT_ATTEMPTED};
  Preserve_trx_lock_warmcopy_artifact_source source{
      Preserve_trx_lock_warmcopy_artifact_source::NONE};
};

struct Preserve_trx_lock_warmcopy_route {
  Preserve_trx_lock_warmcopy_route_action action{
      Preserve_trx_lock_warmcopy_route_action::FALLBACK_TO_LIVE_EXPORT};
  Preserve_trx_lock_warmcopy_reason reason{
      Preserve_trx_lock_warmcopy_reason::NOT_ATTEMPTED};
};

struct Preserve_trx_lock_warmcopy_canonical_compare_result {
  bool equivalent{false};
  std::string difference;
};

struct Preserve_trx_lock_warmcopy_target_observation {
  /*
    Observations describe one target inside the current drain epoch. bytes_used
    covers warmcopy-owned artifact bytes, not the complete server heap usage of
    the transaction.
  */
  uint64_t thread_id{0};
  Preserve_trx_lock_warmcopy_target_state state{
      Preserve_trx_lock_warmcopy_target_state::NEW};
  Preserve_trx_lock_warmcopy_reason reason{
      Preserve_trx_lock_warmcopy_reason::NOT_ATTEMPTED};
  uint32_t record_predicate_table_lock_count{0};
  uint32_t mdl_descriptor_count{0};
  uint64_t bytes_used{0};
  bool has_artifact{false};
  bool artifact_valid{false};
};

Preserve_trx_lock_warmcopy_options
preserve_trx_lock_warmcopy_current_options();

bool preserve_trx_lock_warmcopy_effective();
bool preserve_trx_lock_warmcopy_requires_two_phase(
    bool binlog_warmcopy_effective);
bool preserve_trx_lock_warmcopy_cleanup_orphan_spill_files();
std::string preserve_trx_lock_warmcopy_spill_root_dir_for_unit_test();
bool preserve_trx_lock_warmcopy_write_spill_owner_marker_for_unit_test();
const char *preserve_trx_lock_warmcopy_reason_name(
    Preserve_trx_lock_warmcopy_reason reason);
Preserve_trx_lock_warmcopy_route preserve_trx_lock_warmcopy_route_artifact(
    const Preserve_trx_lock_warmcopy_artifact *artifact,
    const Preserve_trx_lock_warmcopy_options &options);
Preserve_trx_lock_warmcopy_route preserve_trx_lock_warmcopy_route_final_fence(
    Preserve_trx_lock_warmcopy_reason reason,
    const Preserve_trx_lock_warmcopy_options &options);
Preserve_trx_lock_warmcopy_reason
preserve_trx_lock_warmcopy_verify_record_final_fence(
    const Preserve_trx_lock_warmcopy_artifact &artifact,
    const lock_warmcopy_trx_lock_fence_t &current_fence);
void preserve_trx_lock_warmcopy_note_target_attempt();
void preserve_trx_lock_warmcopy_note_target_sealed_valid(uint64_t bytes);
void preserve_trx_lock_warmcopy_note_target_sealed_invalid(
    Preserve_trx_lock_warmcopy_reason reason);
void preserve_trx_lock_warmcopy_note_route_fallback(
    Preserve_trx_lock_warmcopy_reason reason);
void preserve_trx_lock_warmcopy_note_route_reject(
    Preserve_trx_lock_warmcopy_reason reason);
void preserve_trx_lock_warmcopy_note_canonical_mismatch(const char *family);
void preserve_trx_lock_warmcopy_note_final_fence_mismatch();
Preserve_trx_lock_warmcopy_canonical_compare_result
preserve_trx_lock_warmcopy_compare_record_payloads_canonical(
    const std::string &live_export_payload,
    const std::string &warmcopy_payload);
Preserve_trx_lock_warmcopy_canonical_compare_result
preserve_trx_lock_warmcopy_compare_table_payloads_canonical(
    const std::string &live_export_payload,
    const std::string &warmcopy_payload);
Preserve_trx_lock_warmcopy_canonical_compare_result
preserve_trx_lock_warmcopy_compare_mdl_payloads_canonical(
    const std::string &live_export_payload,
    const std::string &warmcopy_payload);
bool preserve_trx_lock_warmcopy_export_mdl_descriptors(
    const MDL_context &mdl_context, std::string *payload, size_t *lock_count);
bool preserve_trx_lock_warmcopy_mdl_namespace_supported(
    unsigned int raw_namespace);

class Preserve_trx_lock_warmcopy_drain_participant final
    : public Preserve_trx_drain_participant {
 public:
  explicit Preserve_trx_lock_warmcopy_drain_participant(
      const Preserve_trx_lock_warmcopy_options &options);

  bool open_phase1() override;
  bool close_phase1() override;
  bool phase1_ready() const override;
  bool phase2_preflight(Preserve_trx_drain_phase_mode mode) override;
  void abort_phase() override;
  void finalize_phase() override;
  void finalize_phase_for_shutdown() override;
  void cleanup_after_failed_shutdown() override;
  Preserve_trx_drain_participant_observation observation() const override;

  const Preserve_trx_lock_warmcopy_artifact *artifact_for_thread(
      uint64_t thread_id);
  bool prepare_phase1_idle_target(THD *target);
  bool prepare_phase1_record_scan_target(THD *target,
                                         bool active_scan = false);
  bool prepare_phase1_record_store_targets();
  bool prepare_quiesced_targets(const std::vector<uint64_t> &thread_ids);
  bool prepare_quiesced_targets_for_unit_test(
      const std::vector<uint64_t> &thread_ids);
  bool prepare_phase1_record_payload_for_thread_for_unit_test(
      uint64_t thread_id, const std::string &payload);
  void prepare_phase1_non_record_payloads_for_thread_for_unit_test(
      uint64_t thread_id, const std::string &table_payload,
      uint32_t table_lock_count, bool autoinc_lock_owned,
      const std::string &mdl_payload, uint32_t mdl_descriptor_count);
  bool target_observation_for_thread(
      uint64_t thread_id,
      Preserve_trx_lock_warmcopy_target_observation *observation) const;
  bool target_observation_for_thread_for_unit_test(
      uint64_t thread_id,
      Preserve_trx_lock_warmcopy_target_observation *observation) const;
  void set_table_locks_for_thread_for_unit_test(
      uint64_t thread_id, const std::string &payload, uint32_t lock_count,
      bool autoinc_lock_owned);
  void set_mdl_descriptors_for_thread_for_unit_test(
      uint64_t thread_id, const std::string &payload,
      uint32_t descriptor_count);
  void set_artifact_for_thread_for_unit_test(
      uint64_t thread_id, const Preserve_trx_lock_warmcopy_artifact &artifact);
  bool corrupt_spilled_artifact_for_thread_for_unit_test(uint64_t thread_id);
  bool corrupt_spill_manifest_for_thread_for_unit_test(uint64_t thread_id);

 private:
  struct Target_session {
    /*
      Target_session is the participant's phase-1/phase-2 handoff object. The
      phase1_* fields are candidates; only artifact_for_thread() exposes the
      sealed route chosen after final fence checks.
    */
    Preserve_trx_lock_warmcopy_target_observation observation;
    bool phase1_record_fence_valid{false};
    lock_warmcopy_record_store_fence_t phase1_record_fence;
    bool record_live_seal_fence_valid{false};
    lock_warmcopy_trx_lock_fence_t record_live_seal_fence;
    bool record_locks_candidate_valid{false};
    bool record_locks_seeded_in_phase1{false};
    bool has_phase1_record_prebuilt_blob{false};
    PrebuiltRecordLocksBlob phase1_record_prebuilt_blob;
    bool phase1_record_prebuilt_fence_valid{false};
    lock_warmcopy_record_store_fence_t phase1_record_prebuilt_fence;
    std::string record_locks_payload;
    uint32_t record_lock_count{0};
    bool table_locks_candidate_valid{false};
    std::string table_locks_payload;
    uint32_t table_lock_count{0};
    bool autoinc_lock_owned{false};
    bool phase1_table_locks_candidate_valid{false};
    std::string phase1_table_locks_payload;
    uint32_t phase1_table_lock_count{0};
    bool phase1_autoinc_lock_owned{false};
    bool table_locks_phase1_fingerprint_valid{false};
    bool mdl_candidate_valid{false};
    std::string mdl_descriptors_payload;
    uint32_t mdl_descriptor_count{0};
    bool phase1_mdl_candidate_valid{false};
    std::string phase1_mdl_descriptors_payload;
    uint32_t phase1_mdl_descriptor_count{0};
    bool mdl_phase1_fingerprint_valid{false};
  };

  Preserve_trx_lock_warmcopy_options m_options;
  Preserve_trx_drain_participant_observation m_observation;
  std::map<uint64_t, Preserve_trx_lock_warmcopy_artifact> m_artifacts;
  std::map<uint64_t, Target_session> m_targets;
  std::vector<std::string> m_spill_paths;
  std::vector<uint64_t> m_target_thread_ids;
  std::set<uint64_t> m_phase1_record_active_scan_targets;
  uint64_t m_epoch{0};
  bool m_record_store_cleanup_deferred_for_shutdown{false};

  Target_session *ensure_target_session(uint64_t thread_id);
  void refresh_phase1_record_prebuilt_observation();
  bool seed_phase1_record_payload_for_thread(uint64_t thread_id,
                                             const std::string &payload);
  void seed_phase1_non_record_payloads_for_thread(
      uint64_t thread_id, const std::string &table_payload,
      uint32_t table_lock_count, bool autoinc_lock_owned,
      const std::string &mdl_payload, uint32_t mdl_descriptor_count);
  void refresh_phase1_non_record_fingerprints(Target_session *target);
  bool build_phase1_record_blob_for_target(uint64_t thread_id,
                                           Target_session *target,
                                           const std::string &payload);
  void discard_phase1_record_blob(Target_session *target);
  void clear_record_stores_for_targets();
};

#endif  // SQL_PRESERVE_TRX_LOCK_WARMCOPY_INCLUDED
