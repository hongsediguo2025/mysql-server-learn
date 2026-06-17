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
#include "sql/sql_cmd.h"

class THD;
struct TABLE;
struct LEX;

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

bool preserve_trx_is_enabled();
void preserve_trx_set_enable_value(bool enabled);
bool preserve_trx_execute_command(THD *thd);

ulonglong preserve_trx_warmcopy_prefix_bytes_status();
ulonglong preserve_trx_warmcopy_digest_bytes_status();
ulonglong preserve_trx_warmcopy_durable_bytes_status();
ulonglong preserve_trx_warmcopy_provider_full_copy_to_count_status();
ulonglong preserve_trx_warmcopy_phase2_pause_us_status();
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
  std::string token;
  const char *failure_reason{nullptr};
  Preserve_trx_preserve_stage stage{
      Preserve_trx_preserve_stage::VALIDATION};
  bool durable_point_crossed{false};
  bool detached_from_original_thd{false};
  bool reattached_to_original_thd{false};
  bool cleanup_completed_after_detach_failure{false};
  bool cleanup_failed_after_reattach{false};
  bool left_preserved_after_cleanup_failure{false};
  bool logged_binlog_cache{false};
};

struct Preserved_trx_view_row {
  std::string token;
  std::string user;
  std::string host;
  std::string owner_user;
  std::string owner_host;
  std::string state;
  std::string created_at;
  std::string expires_at;
  ulonglong recovered_count{0};
  ulonglong age_seconds{0};
  std::string schema_name;
  std::string isolation;
  ulonglong mod_tables_count{0};
  ulonglong locks_count{0};
  bool locks_count_valid{true};
  bool has_read_view{false};
  ulonglong rv_low_limit_no{0};
  ulonglong savepoint_count{0};
  std::string binlog_state;
  bool wrote_to_cache{false};
  ulonglong binlog_cache_size{0};
  std::string binlog_warmcopy_state;
  bool session_sql_log_bin{false};
  bool global_log_bin{false};
  std::string gtid_next;
  bool autoinc_lock_owned{false};
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
bool preserved_trx_expired_reaper_claim_releases_manager_state_for_unit_test(
    const std::string &token);
bool preserved_trx_expired_reaper_empty_claim_keeps_manager_idle_for_unit_test(
    const std::string &token);
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
void preserved_trx_mark_recovery_complete();
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
    bool debug_fail_ha_prepare_low = false,
    bool debug_fail_temp_only_prepare = false);

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

class Sql_cmd_show_preserved_transactions final : public Sql_cmd {
 public:
  enum_sql_command sql_command_code() const override {
    return SQLCOM_SHOW_PRESERVED_TRX;
  }

  bool execute(THD *thd) override;
};

#endif /* SQL_PRESERVE_TRX_INCLUDED */
