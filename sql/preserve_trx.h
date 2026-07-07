/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef SQL_PRESERVE_TRX_INCLUDED
#define SQL_PRESERVE_TRX_INCLUDED

#include <stddef.h>

#include <string>
#include <vector>

#include "lex_string.h"
#include "my_command.h"
#include "my_inttypes.h"
#include "my_thread_local.h"
#include "my_sqlcommand.h"
#include "sql/preserve_trx_bundle.h"
#include "sql/preserve_trx_xid.h"
#include "sql/sql_cmd.h"

class THD;
struct TABLE;
struct LEX;
struct Preserve_trx_lock_warmcopy_artifact;
class Preserve_trx_transfer_source_epoch_session;

extern bool preserve_trx_enable;
extern bool preserve_trx_temp_table_enable;
extern uint preserve_trx_max_total;
extern uint preserve_trx_max_pending_per_user;
extern uint preserve_trx_batch_max_transactions;
extern uint preserve_trx_recovery_max_count;
extern uint preserve_trx_recovery_grace_seconds;
extern ulonglong preserve_trx_max_snapshot_bytes;
extern ulonglong preserve_trx_max_binlog_cache_bytes;
extern ulonglong preserve_trx_max_temp_sidecar_bytes;
extern ulonglong preserve_trx_memory_budget_bytes;
extern ulonglong preserve_trx_memory_per_token_bytes;
extern uint preserve_trx_spill_chunk_bytes;
extern ulonglong preserve_trx_single_phase_max_binlog_cache_bytes;
extern uint preserve_trx_max_lock_count;
extern uint preserve_trx_max_modified_tables;
extern uint preserve_trx_max_scan_pages;
extern uint preserve_trx_materialize_timeout_ms;
enum Preserve_trx_drain_mode {
  PRESERVE_TRX_DRAIN_MODE_SOFT = 0,
  PRESERVE_TRX_DRAIN_MODE_HARD = 1
};
enum Preserve_trx_off_artifact_policy {
  PRESERVE_TRX_OFF_ARTIFACT_POLICY_FAIL_IF_PRESENT = 0,
  PRESERVE_TRX_OFF_ARTIFACT_POLICY_IGNORE = 1,
  PRESERVE_TRX_OFF_ARTIFACT_POLICY_RECOVER = 2,
  PRESERVE_TRX_OFF_ARTIFACT_POLICY_ABANDON = 3
};
enum class Preserved_trx_recover_load_profile {
  SNAPSHOT_ONLY,
  WITH_SEMANTIC_EXTERNAL_BLOBS
};
struct Preserved_trx_operation_deadline {
  uint64_t deadline_us{0};
};

class Preserved_trx_peer_thd_handle {
 public:
  Preserved_trx_peer_thd_handle() = default;
  ~Preserved_trx_peer_thd_handle() { release(); }

  THD *get() const { return m_thd; }
  bool valid() const { return m_thd != nullptr; }
  bool thd_data_locked() const { return m_thd_data_locked; }
  void reset_for_internal_use(THD *thd, bool thd_data_locked) {
    release();
    m_thd = thd;
    m_thd_data_locked = thd_data_locked;
  }
  void unlock_thd_data_after_pin() { m_thd_data_locked = false; }
  void release() {
    m_thd = nullptr;
    m_thd_data_locked = false;
  }

 private:
  THD *m_thd{nullptr};
  bool m_thd_data_locked{false};
};

using Preserved_trx_peer_thd_resolver = bool (*)(
    uint64_t source_connection_id, Preserved_trx_operation_deadline deadline,
    Preserved_trx_peer_thd_handle *target_handle);
extern ulong preserve_trx_off_artifact_policy;
extern ulong preserve_trx_drain_mode;
extern uint preserve_trx_drain_grace_ms;
extern uint preserve_trx_drain_hard_timeout_ms;
extern bool preserve_trx_warmcopy_enable;
extern uint preserve_trx_warmcopy_close_timeout_ms;
extern uint preserve_trx_warmcopy_min_open_ms;
extern uint preserve_trx_warmcopy_chunk_bytes;
extern uint preserve_trx_warmcopy_tail_budget_bytes;
extern ulonglong preserve_trx_warmcopy_max_total_bytes;
extern uint preserve_trx_warmcopy_pending_range_limit;
extern ulonglong preserve_trx_warmcopy_pending_bytes_limit;
extern bool preserve_trx_lock_warmcopy_enable;
extern bool preserve_trx_lock_warmcopy_fallback_to_live_export;
extern ulonglong preserve_trx_lock_warmcopy_max_memory_bytes;
extern ulonglong preserve_trx_lock_warmcopy_max_journal_bytes;
extern uint preserve_trx_lock_warmcopy_max_dirty_shards;
extern uint preserve_trx_lock_warmcopy_max_mdl_descriptors;
extern uint preserve_trx_lock_warmcopy_seal_threads;
extern uint preserve_trx_lock_warmcopy_conversion_wait_timeout_ms;
extern uint preserve_trx_parallel_preserve_threads;
extern uint preserve_trx_startup_recovery_threads;
extern bool preserve_trx_recover_lock_page_prefetch;
extern uint preserve_trx_recover_lock_page_prefetch_io_bytes_per_sec;

uint preserve_trx_auto_parallel_preserve_threads(uint hardware_threads);
bool preserve_trx_is_enabled();
void preserve_trx_set_enable_value(bool enabled);
bool preserve_trx_magic_xid_has_snapshot(const XID &xid);
bool preserve_trx_magic_xid_should_be_protected(const XID &xid);
bool preserve_trx_off_artifact_policy_ignore();
bool preserve_trx_off_artifact_policy_recover();
bool preserve_trx_off_artifact_policy_abandon();
bool preserve_trx_off_artifact_policy_fail_if_present();
bool preserve_trx_execute_command(THD *thd);
bool preserved_trx_resume_adopted_for_promotion_on_thd(
    Preserved_trx_peer_thd_handle *target_handle, const std::string &token);

ulonglong preserve_trx_warmcopy_prefix_bytes_status();
ulonglong preserve_trx_warmcopy_digest_bytes_status();
ulonglong preserve_trx_warmcopy_durable_bytes_status();
ulonglong preserve_trx_warmcopy_provider_full_copy_to_count_status();
ulonglong preserve_trx_warmcopy_phase2_pause_us_status();
ulonglong preserve_trx_phase2_total_us_status();
ulonglong preserve_trx_phase2_target_wait_us_status();
ulonglong preserve_trx_phase2_participant_prepare_us_status();
ulonglong preserve_trx_phase2_participant_close_us_status();
ulonglong preserve_trx_phase2_participant_preflight_us_status();
ulonglong preserve_trx_phase2_lock_seal_us_status();
ulonglong preserve_trx_phase2_target_preserve_us_status();
ulonglong preserve_trx_phase2_lock_preflight_us_status();
ulonglong preserve_trx_phase2_prepare_us_status();
ulonglong preserve_trx_phase2_detach_claim_us_status();
ulonglong preserve_trx_phase2_snapshot_write_us_status();
ulonglong preserve_trx_phase2_register_us_status();
ulonglong preserve_trx_phase2_slo_miss_count_status();
ulonglong preserve_trx_resume_total_us_status();
ulonglong preserve_trx_startup_recovery_elapsed_us_status();
ulonglong preserve_trx_startup_recovery_error_status();
ulonglong preserve_trx_startup_recovery_snapshot_tokens_status();
ulonglong preserve_trx_startup_recovery_local_snapshot_tokens_status();
ulonglong preserve_trx_startup_recovery_binlog_cache_tokens_status();
ulonglong preserve_trx_startup_recovery_tainted_tokens_status();
ulonglong preserve_trx_startup_recovery_standby_pending_tokens_status();
ulonglong preserve_trx_startup_recovery_promotion_intent_tokens_status();
ulonglong preserve_trx_startup_recovery_orphan_rollback_count_status();
ulonglong preserve_trx_startup_recovery_phase_snapshot_load_us_status();
ulonglong preserve_trx_startup_recovery_phase_snapshot_validate_us_status();
ulonglong preserve_trx_startup_recovery_phase_snapshot_kernel_us_status();
ulonglong preserve_trx_startup_recovery_phase_snapshot_claim_us_status();
ulonglong preserve_trx_startup_recovery_phase_snapshot_read_view_us_status();
ulonglong preserve_trx_startup_recovery_phase_snapshot_table_locks_us_status();
ulonglong preserve_trx_startup_recovery_phase_snapshot_record_locks_us_status();
ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_entries_status();
ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_stable_page_hits_status();
ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_image_resolves_status();
ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_pages_status();
ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_bits_status();
ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_us_status();
ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_count_status();
ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_table_open_us_status();
ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_pages_status();
ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_bytes_status();
ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_residency_pages_status();
ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_resident_pages_status();
ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_io_pending_pages_status();
ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_missing_pages_status();
ulonglong
preserve_trx_startup_recovery_phase_snapshot_predicate_locks_us_status();
ulonglong preserve_trx_startup_recovery_phase_snapshot_mdl_us_status();
ulonglong preserve_trx_startup_recovery_phase_snapshot_register_us_status();
ulonglong preserve_trx_lock_warmcopy_attempts_status();
ulonglong preserve_trx_lock_warmcopy_sealed_valid_status();
ulonglong preserve_trx_lock_warmcopy_sealed_invalid_status();
ulonglong preserve_trx_lock_warmcopy_live_fallback_status();
ulonglong preserve_trx_lock_warmcopy_strict_reject_status();
ulonglong preserve_trx_lock_warmcopy_canonical_mismatch_status();
ulonglong preserve_trx_lock_warmcopy_resource_limit_status();
ulonglong preserve_trx_lock_warmcopy_unsupported_family_status();
ulonglong preserve_trx_lock_warmcopy_final_fence_mismatch_status();
ulonglong preserve_trx_lock_warmcopy_artifact_bytes_status();
ulonglong preserve_trx_lock_warmcopy_spill_bytes_status();
ulonglong preserve_trx_lock_warmcopy_spill_failures_status();
ulonglong preserve_trx_lock_warmcopy_journal_bytes_status();
ulonglong preserve_trx_lock_warmcopy_dirty_shards_status();
ulonglong preserve_trx_lock_warmcopy_phase2_pause_us_status();
ulonglong preserve_trx_lock_warmcopy_conversion_freeze_waits_status();
void preserve_trx_warmcopy_note_prefix_bytes(uint64_t bytes);
void preserve_trx_warmcopy_note_digest_bytes(uint64_t bytes);
void preserve_trx_warmcopy_note_durable_bytes(uint64_t bytes);
void preserve_trx_warmcopy_admit_current_thd_binlog_write(THD *thd);

bool preserve_trx_temp_table_session_needs_eligibility_check(const THD *thd);
bool preserve_trx_temp_table_session_supported(THD *thd);
bool preserve_trx_temp_table_capture_enabled(THD *thd, const TABLE *table);
bool preserve_trx_temp_table_resume_supported(
    bool snapshot_has_temp_table_manifest);

struct Preserved_trx_column_metadata {
  const char *name;
  uint length;
};

enum class Preserve_trx_user_vars_mode { DEFAULT, INCLUDE, EXCLUDE };

enum class Preserve_trx_manager_state {
  /*
    Public manager state is intentionally coarse. Detailed participant progress
    is reported through drain observations so command admission can depend on a
    small stable state machine.

    Active states:
      IDLE                      no preserve/drain owner
      DISABLING                 preserve feature is being disabled
      SOFT_DRAINING             single preserve is draining other active trx
      WARMCOPY_DRAINING         phase-1 participants may capture live work
      WARMCOPY_CLOSING          command admission is closing for phase 2
      BATCH_DRAINING            batch admission/target-quiesce/preserve window
      EXPIRED_ROLLBACK          expired token reaper owns manager work
      DRAIN_CLEANUP_FAILED      cleanup left observable state for operators
      SHUTDOWN_REQUESTED        shutdown/token-delivery handoff is in progress

    HARD_DRAINING and SNAPSHOTTING are retained for state compatibility and
    diagnostics; current batch preserve does not use them as manager-level
    transition points.
  */
  IDLE,
  DISABLING,
  SOFT_DRAINING,
  HARD_DRAINING,
  WARMCOPY_DRAINING,
  WARMCOPY_CLOSING,
  BATCH_DRAINING,
  SNAPSHOTTING,
  EXPIRED_ROLLBACK,
  DRAIN_CLEANUP_FAILED,
  SHUTDOWN_REQUESTED
};

enum class Preserve_trx_command_block_result {
  ALLOW,
  BLOCK_DRAINING,
  BLOCK_SESSION_DRAINED
};

using Preserved_trx_manager_state_publication_probe = void (*)(void *);

struct Preserve_trx_options {
  bool has_timeout{false};
  ulonglong timeout_seconds{0};
  Preserve_trx_user_vars_mode user_vars_mode{
      Preserve_trx_user_vars_mode::DEFAULT};
};

enum class Preserve_trx_delivery_mode {
  CLIENT_TOKEN_DELIVERY,
  BATCH_MANAGER_DELIVERY
};

/*
  High-level stage of a single target preserve.

  The stage is stored in Preserve_trx_preserve_result so failure reporting can
  say where ownership stopped: before engine prepare, after detach, during
  snapshot write, or after the token became visible. Cleanup rules depend on
  that durable boundary.
*/
enum class Preserve_trx_preserve_stage {
  VALIDATION,
  BINLOG_PREFLIGHT,
  LOCK_PREFLIGHT,
  UNDO_PREPARE,
  DETACH,
  SNAPSHOT_WRITE,
  RECORD_REGISTER,
  TOKEN_DELIVERY,
  COMPLETE
};

struct Preserve_trx_preserve_result {
  /*
    Result and ownership boundary for one target. token is allocated before
    engine prepare so later stages can name the snapshot, but it is not
    necessarily visible or resumable. failure_reason/stage and the cleanup flags
    describe where preserve stopped; durable_point_crossed means the engine or
    temp-only prepare boundary was crossed, not that a snapshot has been
    published.
  */
  std::string token;
  const char *failure_reason{nullptr};
  Preserve_trx_preserve_stage stage{
      Preserve_trx_preserve_stage::VALIDATION};
  /*
    Timings below are per-target measurements. The drain path aggregates them to
    explain where the blocked window was spent; single-transaction preserve uses
    the same fields for diagnostics.
  */
  uint64_t binlog_preflight_us{0};
  uint64_t lock_preflight_us{0};
  uint64_t lock_preflight_read_view_us{0};
  uint64_t lock_preflight_mdl_us{0};
  uint64_t lock_preflight_modified_tables_us{0};
  uint64_t lock_preflight_savepoints_us{0};
  uint64_t lock_preflight_predicate_us{0};
  uint64_t lock_preflight_table_us{0};
  uint64_t prepare_us{0};
  uint64_t detach_claim_us{0};
  uint64_t snapshot_write_us{0};
  uint64_t snapshot_write_prebuilt_binlog_us{0};
  uint64_t snapshot_write_temp_manifest_us{0};
  uint64_t snapshot_write_bundle_build_us{0};
  uint64_t snapshot_write_store_us{0};
  uint64_t snapshot_write_store_token_state_us{0};
  uint64_t snapshot_write_store_adopt_warm_blob_us{0};
  uint64_t snapshot_write_store_write_new_blobs_us{0};
  uint64_t snapshot_write_store_encode_us{0};
  uint64_t snapshot_write_store_write_snapshot_us{0};
  uint64_t record_register_us{0};
  /* Live-export fallback counters used to flag non-warmcopy phase-2 work. */
  uint64_t phase2_savepoint_live_export_target_count{0};
  /*
    Ownership flags drive cleanup after partial failure. durable_point_crossed
    means engine/temp prepare crossed a boundary that may require explicit
    rollback/reactivation; detached_from_original_thd means the original session
    no longer owns the engine trx.
  */
  bool durable_point_crossed{false};
  bool detached_from_original_thd{false};
  bool reattached_to_original_thd{false};
  /* Cleanup outcome flags keep ambiguous prepared-trx ownership observable. */
  bool cleanup_completed_after_detach_failure{false};
  bool cleanup_failed_after_reattach{false};
  bool left_preserved_after_cleanup_failure{false};
  const char *detach_failure_reason{nullptr};
  const char *reactivate_failure_reason{nullptr};
  /* True when binlog cache semantics were logged into the snapshot. */
  bool logged_binlog_cache{false};
};

/*
  Row shape used by SHOW PRESERVED TRANSACTIONS and P_S population.

  The row is intentionally denormalized: it combines snapshot metadata,
  recovered in-memory state, lock/binlog/temp summaries, and the last error so a
  DBA can diagnose a token without reading carrier files directly.
*/
struct Preserved_trx_view_row {
  /* Token and account fields after redaction/visibility filtering. */
  std::string token;
  std::string user;
  std::string host;
  std::string owner_user;
  std::string owner_host;
  /* Lifecycle and deadline fields shown to operators. */
  std::string state;
  std::string created_at;
  std::string expires_at;
  ulonglong recovered_count{0};
  ulonglong age_seconds{0};
  /* Transaction semantic summary restored or validated by resume. */
  std::string schema_name;
  std::string isolation;
  ulonglong mod_tables_count{0};
  ulonglong locks_count{0};
  bool locks_count_valid{true};
  bool has_read_view{false};
  ulonglong rv_low_limit_no{0};
  ulonglong savepoint_count{0};
  /* Binlog and warmcopy state for logged transactions. */
  std::string binlog_state;
  bool wrote_to_cache{false};
  ulonglong binlog_cache_size{0};
  std::string binlog_warmcopy_state;
  bool session_sql_log_bin{false};
  bool global_log_bin{false};
  std::string gtid_next;
  bool autoinc_lock_owned{false};
  /* Temporary table sidecar summary and last error context. */
  std::string temp_table_state;
  ulonglong temp_image_bytes{0};
  ulonglong temp_undo_bytes{0};
  bool temp_sidecars_complete{true};
  std::string last_error;
  std::string last_error_at;
};

using Preserved_trx_view_rows = std::vector<Preserved_trx_view_row>;

const Preserved_trx_column_metadata *preserved_trx_columns(size_t *count);
const char *preserved_trx_dir_value();
bool preserved_trx_ensure_snapshot_support();
bool preserved_trx_validate_snapshot_support(bool allow_create_missing);
bool preserved_trx_preflight_recoverability();
Preserve_trx_manager_state preserved_trx_manager_state();
bool preserved_trx_can_disable_feature();
bool preserved_trx_try_disable_feature_for_update();
void preserved_trx_set_manager_state_publication_probe_for_unit_test(
    Preserved_trx_manager_state_publication_probe probe, void *arg);
void preserved_trx_set_manager_state_for_unit_test(
    Preserve_trx_manager_state state, my_thread_id owner_thread_id);
void preserved_trx_set_recovery_complete_for_unit_test(bool complete);
bool preserved_trx_probe_manager_state_guard_for_unit_test(
    Preserve_trx_manager_state to, my_thread_id owner_thread_id);
void preserved_trx_add_record_for_unit_test(const std::string &token,
                                            bool observable_only);
void preserved_trx_remove_record_for_unit_test(const std::string &token);
bool preserved_trx_add_deadline_record_for_unit_test(
    const std::string &token, uint64_t created_wall_us,
    uint64_t expires_wall_us, uint64_t anchor_wall_us,
    uint64_t anchor_monotonic_us);
bool preserved_trx_record_expired_for_unit_test(const std::string &token,
                                                uint64_t now_monotonic_us);
uint64_t preserved_trx_monotonic_deadline_after_ms_for_unit_test(
    uint64_t now_monotonic_us, uint64_t timeout_ms);
bool preserved_trx_monotonic_deadline_expired_for_unit_test(
    uint64_t deadline_monotonic_us, uint64_t now_monotonic_us);
unsigned long preserved_trx_monotonic_timeout_ms_until_deadline_for_unit_test(
    uint64_t deadline_monotonic_us, unsigned long fallback_timeout_ms,
    uint64_t now_monotonic_us);
bool preserved_trx_recovery_deadline_expired_for_unit_test(
    const Preserve_snapshot_metadata &metadata, uint64_t anchor_wall_us,
    uint64_t anchor_monotonic_us, uint64_t now_monotonic_us);
bool preserved_trx_startup_record_lock_pages_prewarmed_for_unit_test(
    uint64_t record_lock_page_count, uint64_t prefetch_submitted_pages,
    uint64_t resident_pages);
void preserved_trx_add_failed_observable_record_for_unit_test(
    const std::string &token, uint64_t anchor_monotonic_us);
size_t preserved_trx_gc_failed_observable_records_for_unit_test(
    uint64_t now_monotonic_us);
bool preserved_trx_observable_record_exists_for_unit_test(
    const std::string &token);
bool preserved_trx_recovery_read_failure_requires_startup_abort_for_unit_test(
    Preserve_snapshot_status status);
bool preserved_trx_preflight_read_failure_requires_startup_abort_for_unit_test(
    Preserve_snapshot_status status);
Preserve_snapshot_status preserved_trx_load_bundle_for_recover_or_prewarm(
    const std::string &dir, const std::string &token,
    Preserved_trx_recover_load_profile profile, Preserved_trx_bundle *bundle);
Preserve_snapshot_status preserved_trx_dry_validate_loaded_bundle(
    const std::string &dir, const std::string &token,
    const Preserved_trx_bundle &bundle, std::string *reason);
bool preserved_trx_expired_reaper_claim_releases_manager_state_for_unit_test(
    const std::string &token);
bool preserved_trx_expired_reaper_empty_claim_keeps_manager_idle_for_unit_test(
    const std::string &token);
/*
  Command-boundary tracking for batch drain.

  begin_command_read marks the protocol command-read window before get_command()
  returns a concrete packet. mark_inflight_command_packet records that packet
  after it is read, and consume_inflight_command_packet consumes the marker
  before statement dispatch. Risky/unknown query markers are statement-scope
  guards cleared by the RAII/end helpers. Drain uses these marks to wait for
  already admitted work and to reject later work deterministically.
*/
bool preserved_trx_begin_command_read(THD *thd);
bool preserved_trx_command_read_is_idle(THD *thd);
bool preserved_trx_end_idle_for_command_packet(THD *thd);
bool preserved_trx_end_command_read(THD *thd);
bool preserved_trx_wait_if_batch_session_quiesced(THD *thd);
bool preserved_trx_reject_if_batch_session_drained(THD *thd);
Preserve_trx_command_block_result preserved_trx_command_block_result(
    THD *thd, enum_sql_command sql_command);
Preserve_trx_command_block_result preserved_trx_protocol_command_block_result(
    THD *thd, enum enum_server_command command);
bool preserved_trx_mark_inflight_risky_statement(THD *thd,
                                                 enum_sql_command sql_command);
bool preserved_trx_mark_inflight_risky_statement(THD *thd, LEX *lex,
                                                 enum_sql_command sql_command);
bool preserved_trx_mark_inflight_command_packet(
    THD *thd, enum enum_server_command command);
bool preserved_trx_consume_inflight_command_packet(
    THD *thd, enum enum_server_command command);
bool preserved_trx_mark_inflight_unknown_query(THD *thd);
void preserved_trx_clear_inflight_risky_statement(THD *thd);
void preserved_trx_clear_inflight_unknown_query(THD *thd);
Preserved_trx_view_rows preserved_trx_snapshot(THD *thd);
size_t preserved_trx_record_count();
bool preserved_trx_row_visible(THD *thd, const Preserved_trx_view_row &row);
bool preserved_trx_row_visible_for_account(bool has_process_acl,
                                           LEX_CSTRING priv_user,
                                           LEX_CSTRING priv_host,
                                           const Preserved_trx_view_row &row);
bool preserved_trx_resume_allowed_for_account(bool owns_token,
                                              bool has_resume_any_privilege);
bool preserved_trx_metadata_locks_count(
    const Preserve_snapshot_metadata &metadata, uint32_t *lock_count);
bool preserved_trx_populate_row_locks_count(
    const Preserve_snapshot_metadata &metadata, Preserved_trx_view_row *row);
std::string preserved_trx_redacted_token(const std::string &token);
bool preserved_trx_resume_deadline_expired(
    const Preserve_snapshot_metadata &metadata);
bool preserved_trx_resolve_timeout_seconds(const Preserve_trx_options &options,
                                           ulonglong default_timeout,
                                           ulonglong min_timeout,
                                           ulonglong max_timeout,
                                           ulonglong *timeout_seconds);
bool preserved_trx_preflight_recoverability();
bool preserved_temp_images_bootstrap_preamble();
bool preserved_trx_recover_all();
bool preserved_trx_recovery_complete();
bool preserved_trx_local_record_exists(const std::string &token);
void preserved_trx_mark_recovery_complete();

struct Preserved_trx_promotion_ready_adopt_result {
  bool claimed{false};
  bool rolled_back{false};
  std::string reason;
};

bool preserved_trx_adopt_ready_bundle_for_promotion(
    const std::string &dir, Preserved_trx_bundle bundle,
    Preserved_trx_promotion_ready_adopt_result *result,
    uint64_t deadline_us = 0);
void preserved_trx_start_expired_reaper();
void preserved_trx_start_expired_reaper_if_ready();
void preserved_trx_stop_expired_reaper();
bool preserved_trx_shutdown_requested();
bool preserved_trx_defer_shutdown_signal();
void preserved_trx_note_statement_response(THD *thd);
void preserved_trx_finalize_statement_response(THD *thd);
void preserved_trx_release_resources(THD *thd);
void preserved_trx_wait_for_external_thd_use(THD *thd);
bool preserved_trx_thd_has_external_use(THD *thd);
bool preserve_trx_preserve_attached_transaction(
    THD *target_thd, const Preserve_trx_options &options,
    ulonglong timeout_seconds, Preserve_trx_delivery_mode delivery_mode,
    Preserve_trx_preserve_result *result,
    PreserveBinlogBlobProvider *binlog_blob_provider = nullptr,
    const Preserve_trx_lock_warmcopy_artifact *lock_warmcopy_artifact = nullptr,
    bool debug_fail_ha_prepare_low = false,
    bool debug_fail_temp_only_prepare = false,
    bool defer_snapshot_directory_fsync = false,
    Preserve_trx_transfer_source_epoch_session *transfer_source_epoch_session =
        nullptr,
    const std::string &transfer_preserve_dir = std::string());

/*
  SQL command for preserving the current transaction before shutdown.

  The command stores parser options only. Validation, timeout resolution,
  binlog/lock/temp-table export, engine prepare, and token delivery all happen
  in the preserve service so batch drain and single-transaction preserve share
  one correctness path.
*/
class Sql_cmd_prepare_shutdown_preserve_transaction final : public Sql_cmd {
 public:
  explicit Sql_cmd_prepare_shutdown_preserve_transaction(
      const Preserve_trx_options &options)
      : m_options(options) {}

  enum_sql_command sql_command_code() const override {
    return SQLCOM_PREPARE_SHUTDOWN_PRESERVE;
  }

  bool execute(THD *thd) override;

 private:
  Preserve_trx_options m_options;
};

/*
  SQL command for batch drain preserve.

  The command starts the drain service, which enumerates eligible sessions,
  opens optional warmcopy participants, quiesces targets, preserves each target,
  records token outcomes through BATCH_MANAGER_DELIVERY for post-restart visibility,
  and requests shutdown after auditing the preserved records. Tokens are not
  returned to each target client connection by this command.
*/
class Sql_cmd_drain_transactions_preserve final : public Sql_cmd {
 public:
  explicit Sql_cmd_drain_transactions_preserve(
      const Preserve_trx_options &options)
      : m_options(options) {}

  enum_sql_command sql_command_code() const override {
    return SQLCOM_DRAIN_TRANSACTIONS_PRESERVE;
  }

  bool execute(THD *thd) override;

 private:
  Preserve_trx_options m_options;
};

/*
  SQL command for resuming one durable token.

  The token is an opaque durable resume handle, but possession alone is not
  authorization. execute() rechecks token ownership, RESUME_ANY privilege, object
  privileges, binlog mode, and session eligibility before importing SQL/InnoDB
  state and attaching the preserved transaction to the current THD.
*/
class Sql_cmd_resume_preserved_transaction final : public Sql_cmd {
 public:
  explicit Sql_cmd_resume_preserved_transaction(LEX_CSTRING token)
      : m_token(token) {}
  Sql_cmd_resume_preserved_transaction(const char *token, size_t token_length)
      : m_token{token, token_length} {}

  enum_sql_command sql_command_code() const override {
    return SQLCOM_RESUME_PRESERVED_TRX;
  }

  LEX_CSTRING token() const { return m_token; }

  bool execute(THD *thd) override;

 private:
  LEX_CSTRING m_token;
};

/*
  SQL command for operational visibility.

  It reads the in-memory preserved-record view rather than parsing snapshot files
  directly, so recovered, failed, expired, and cleanup-pending tokens are shown
  through the same visibility and redaction rules.
*/
class Sql_cmd_show_preserved_transactions final : public Sql_cmd {
 public:
  enum_sql_command sql_command_code() const override {
    return SQLCOM_SHOW_PRESERVED_TRX;
  }

  bool execute(THD *thd) override;
};

#endif /* SQL_PRESERVE_TRX_INCLUDED */
