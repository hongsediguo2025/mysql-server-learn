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

#include "sql/preserve_trx.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cctype>
#include <cstdint>
#include <condition_variable>
#include <cstring>
#include <ctime>
#include <cstdlib>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <set>
#include <sstream>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <openssl/crypto.h>
#include <openssl/sha.h>

#include "mem_root_deque.h"
#include "m_string.h"
#include "my_dbug.h"
#include "my_rnd.h"
#include "mysql_com.h"
#include "mysql/psi/mysql_transaction.h"
#include "my_sys.h"
#include "my_systime.h"
#include "sql/current_thd.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql_version.h"
#include "mysqld_error.h"
#include "sql/auth/auth_acls.h"
#include "sql/auth/auth_common.h"
#include "sql/auth/sql_auth_cache.h"
#include "sql/auth/sql_security_ctx.h"
#include "sql/auto_thd.h"
#include "sql/binlog.h"
#include "sql/binlog_preserve_prepared.h"
#include "sql/binlog_warmcopy.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/dd_schema.h"
#include "sql/dd/string_type.h"
#include "sql/dd/types/schema.h"
#include "sql/debug_sync.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/mdl_context_backup.h"
#include "sql/my_decimal.h"
#include "sql/mysqld.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/nested_join.h"
#include "sql/protocol.h"
#include "sql/query_options.h"
#include "sql/rpl_group_replication.h"
#include "sql/rpl_gtid.h"
#include "sql/rpl_mi.h"
#include "sql/rpl_msr.h"
#include "sql/session_tracker.h"
#include "sql/set_var.h"
#include "sql/handler.h"
#include "sql/preserve_trx_carrier.h"
#include "sql/preserve_trx_drain.h"
#include "sql/preserve_trx_kernel.h"
#include "sql/preserve_trx_lock_warmcopy.h"
#include "sql/preserve_trx_promotion.h"
#include "sql/preserve_trx_promotion_prepared.h"
#include "sql/preserve_trx_resurrection_index.h"
#include "sql/preserve_trx_temp_table.h"
#include "sql/preserve_trx_transfer.h"
#include "sql/preserve_trx_warmcopy.h"
#include "sql/preserve_trx_xid.h"
#include "sql/sql_const.h"
#include "sql/sql_class.h"
#include "sql/sql_error.h"
#include "sql/sql_lex.h"
#include "scope_guard.h"
#include "sql/sql_list.h"
#include "sql/sql_audit.h"
#include "sql/sql_backup_lock.h"
#include "sql/sql_parse.h"
#include "sql/sql_thd_internal_api.h"
#include "sql/table.h"
#include "sql/transaction.h"
#include "sql/transaction_info.h"
#include "sql/tztime.h"
#include "sql/xa.h"
#include "storage/innobase/include/trx0preserve.h"
#include "storage/innobase/include/trx0temp_preserve.h"

using Access_bitmask = ulong;
using Table_ref = TABLE_LIST;

bool preserve_trx_enable = true;
bool preserve_trx_temp_table_enable = true;
const uint preserve_trx_recovery_max_count = 3;
const uint preserve_trx_recovery_grace_seconds = 120;
const ulonglong preserve_trx_max_snapshot_bytes = 16777216;
const ulonglong preserve_trx_max_binlog_cache_bytes = 1073741824;
const ulonglong preserve_trx_max_temp_sidecar_bytes = 1073741824;
const ulonglong preserve_trx_single_phase_max_binlog_cache_bytes = ULLONG_MAX;
const uint preserve_trx_max_lock_count = 1000000;
const uint preserve_trx_max_modified_tables = 512;
uint preserve_trx_drain_phase1_timeout_ms = 600000;
uint preserve_trx_drain_phase2_timeout_ms = 30000;
const ulonglong preserve_trx_warmcopy_max_total_bytes = 10737418240ULL;
const uint preserve_trx_warmcopy_pending_range_limit = 1024;
const ulonglong preserve_trx_warmcopy_pending_bytes_limit = 67108864ULL;
const ulonglong preserve_trx_lock_warmcopy_max_memory_bytes = 268435456ULL;
const ulonglong preserve_trx_lock_warmcopy_max_journal_bytes = 1073741824ULL;
const uint preserve_trx_lock_warmcopy_max_dirty_shards = 100000;
const uint preserve_trx_lock_warmcopy_max_mdl_descriptors = 100000;
const uint preserve_trx_lock_warmcopy_seal_threads = 0;
uint preserve_trx_lock_warmcopy_conversion_wait_timeout_ms = 30000;
const uint preserve_trx_parallel_preserve_threads = 0;
const uint preserve_trx_startup_recovery_threads = 0;
const bool preserve_trx_recover_lock_page_prefetch = true;
extern ulong srv_force_recovery;
extern ulong srv_rollback_segments;
extern bool recv_needed_recovery;
extern bool srv_read_only_mode;
static std::atomic<bool> g_preserve_trx_enable_cached{true};
static std::atomic<bool> g_preserve_trx_enable_cache_initialized{false};
static std::atomic<bool> g_preserved_trx_server_startup_active{false};

bool preserve_trx_is_enabled() {
  if (!g_preserve_trx_enable_cache_initialized.load(std::memory_order_acquire)) {
    return preserve_trx_enable;
  }
  return g_preserve_trx_enable_cached.load(std::memory_order_acquire);
}

void preserve_trx_set_enable_value(bool enabled) {
  preserve_trx_enable = enabled;
  g_preserve_trx_enable_cached.store(enabled, std::memory_order_release);
  g_preserve_trx_enable_cache_initialized.store(true, std::memory_order_release);
}

bool preserved_trx_server_startup_active() {
  return g_preserved_trx_server_startup_active.load(std::memory_order_acquire);
}

bool preserved_trx_skip_local_startup_recovery() {
  return preserved_trx_server_startup_active() && preserve_trx_is_enabled() &&
         preserve_trx_transfer_artifact_mode ==
             PRESERVE_TRX_TRANSFER_ARTIFACT_STANDBY_TRANSFER_SAVE;
}

void preserved_trx_enter_server_startup() {
  if (!preserve_trx_is_enabled()) return;
  bool expected = false;
  const bool entered =
      g_preserved_trx_server_startup_active.compare_exchange_strong(
          expected, true, std::memory_order_acq_rel);
  DBUG_ASSERT(entered);
}

void preserved_trx_leave_server_startup() {
  const bool deferred_transfer_startup =
      preserved_trx_skip_local_startup_recovery();
  const bool was_active =
      g_preserved_trx_server_startup_active.exchange(false,
                                                     std::memory_order_acq_rel);
  if (!was_active || !deferred_transfer_startup) return;

  preserved_trx_start_expired_reaper_if_ready();
  if (!srv_read_only_mode &&
      !preserved_trx_promotion_start_gate_workers()) {
    LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
           "PRESERVE: failed to prestart promotion gate worker pool");
  }
}

uint preserve_trx_auto_parallel_preserve_threads(uint hardware_threads) {
  const uint requested =
      hardware_threads == 0 ? 8 : std::max<uint>(4, hardware_threads);
  return std::min<uint>(requested, 10);
}

static uint preserve_trx_effective_startup_recovery_threads(
    size_t snapshot_count) {
  if (snapshot_count <= 1 || preserve_trx_startup_recovery_threads == 1) {
    return 1;
  }

  uint requested = preserve_trx_startup_recovery_threads;
  if (requested == 0) {
    requested = preserve_trx_auto_parallel_preserve_threads(
        std::thread::hardware_concurrency());
  }
  requested = std::max<uint>(1, requested);
  return std::min<uint>(
      requested,
      snapshot_count > std::numeric_limits<uint>::max()
          ? std::numeric_limits<uint>::max()
          : static_cast<uint>(snapshot_count));
}

std::string preserved_trx_redacted_token(const std::string &token) {
  if (token.empty()) return "****????";
  std::string redacted("****");
  const size_t suffix_length = std::min<size_t>(4, token.length());
  redacted.append(token, token.length() - suffix_length, suffix_length);
  return redacted;
}

Preserve_trx_drain_terminal Preserve_trx_drain_ownership_state::state() const {
  return m_state.load(std::memory_order_acquire);
}

Preserve_trx_drain_reset_request
Preserve_trx_drain_ownership_state::request_reset() {
  for (;;) {
    Preserve_trx_drain_terminal current = state();
    Preserve_trx_drain_terminal desired;
    Preserve_trx_drain_reset_request result;
    switch (current) {
      case Preserve_trx_drain_terminal::RUNNING:
      case Preserve_trx_drain_terminal::FINAL_METADATA_ACCEPTED_LOCAL:
        desired = Preserve_trx_drain_terminal::RESET_REQUESTED;
        result = Preserve_trx_drain_reset_request::WON;
        break;
      case Preserve_trx_drain_terminal::HANDOFF_PENDING:
      case Preserve_trx_drain_terminal::COMMIT_UNKNOWN:
      case Preserve_trx_drain_terminal::COMMITTED_HANDOFF:
        return Preserve_trx_drain_reset_request::TOO_LATE;
      case Preserve_trx_drain_terminal::SHUTDOWN_HANDOFF:
        desired = Preserve_trx_drain_terminal::SOURCE_RESTORE_PENDING;
        result = Preserve_trx_drain_reset_request::WON;
        break;
      case Preserve_trx_drain_terminal::RESET_REQUESTED:
      case Preserve_trx_drain_terminal::SOURCE_RESTORE_PENDING:
        return Preserve_trx_drain_reset_request::JOINED;
      case Preserve_trx_drain_terminal::SOURCE_RESTORED:
        return Preserve_trx_drain_reset_request::ALREADY_RESTORED;
      default:
        return Preserve_trx_drain_reset_request::INVALID;
    }
    if (m_state.compare_exchange_strong(current, desired)) return result;
  }
}

bool Preserve_trx_drain_ownership_state::begin_commit_send() {
  Preserve_trx_drain_terminal expected =
      Preserve_trx_drain_terminal::RUNNING;
  if (m_state.compare_exchange_strong(
          expected,
          Preserve_trx_drain_terminal::FINAL_METADATA_ACCEPTED_LOCAL)) {
    DEBUG_SYNC(current_thd,
               "preserve_trx_after_final_metadata_accepted_local");
    expected = Preserve_trx_drain_terminal::FINAL_METADATA_ACCEPTED_LOCAL;
  } else if (expected == Preserve_trx_drain_terminal::HANDOFF_PENDING) {
    return true;
  } else if (expected !=
             Preserve_trx_drain_terminal::FINAL_METADATA_ACCEPTED_LOCAL) {
    return false;
  }
  return m_state.compare_exchange_strong(
             expected, Preserve_trx_drain_terminal::HANDOFF_PENDING) ||
         expected == Preserve_trx_drain_terminal::HANDOFF_PENDING;
}

bool Preserve_trx_drain_ownership_state::mark_commit_unknown() {
  Preserve_trx_drain_terminal expected =
      Preserve_trx_drain_terminal::HANDOFF_PENDING;
  return m_state.compare_exchange_strong(
             expected, Preserve_trx_drain_terminal::COMMIT_UNKNOWN) ||
         expected == Preserve_trx_drain_terminal::COMMIT_UNKNOWN;
}

bool Preserve_trx_drain_ownership_state::resolve_not_committed_clean() {
  Preserve_trx_drain_terminal expected =
      Preserve_trx_drain_terminal::HANDOFF_PENDING;
  if (m_state.compare_exchange_strong(
          expected, Preserve_trx_drain_terminal::SOURCE_RESTORE_PENDING)) {
    return true;
  }
  if (expected == Preserve_trx_drain_terminal::COMMIT_UNKNOWN) {
    return m_state.compare_exchange_strong(
               expected,
               Preserve_trx_drain_terminal::SOURCE_RESTORE_PENDING) ||
           expected ==
               Preserve_trx_drain_terminal::SOURCE_RESTORE_PENDING;
  }
  return expected == Preserve_trx_drain_terminal::SOURCE_RESTORE_PENDING;
}

bool Preserve_trx_drain_ownership_state::begin_source_restore() {
  if (resolve_not_committed_clean()) return true;
  Preserve_trx_drain_terminal expected =
      Preserve_trx_drain_terminal::RESET_REQUESTED;
  return m_state.compare_exchange_strong(
             expected, Preserve_trx_drain_terminal::SOURCE_RESTORE_PENDING) ||
         expected == Preserve_trx_drain_terminal::SOURCE_RESTORE_PENDING;
}

bool Preserve_trx_drain_ownership_state::complete_source_restore() {
  Preserve_trx_drain_terminal expected =
      Preserve_trx_drain_terminal::SOURCE_RESTORE_PENDING;
  return m_state.compare_exchange_strong(
             expected, Preserve_trx_drain_terminal::SOURCE_RESTORED) ||
         expected == Preserve_trx_drain_terminal::SOURCE_RESTORED;
}

bool Preserve_trx_drain_ownership_state::acknowledge_commit() {
  Preserve_trx_drain_terminal expected =
      Preserve_trx_drain_terminal::HANDOFF_PENDING;
  if (m_state.compare_exchange_strong(
          expected, Preserve_trx_drain_terminal::COMMITTED_HANDOFF)) {
    return true;
  }
  if (expected == Preserve_trx_drain_terminal::COMMIT_UNKNOWN) {
    return m_state.compare_exchange_strong(
               expected, Preserve_trx_drain_terminal::COMMITTED_HANDOFF) ||
           expected == Preserve_trx_drain_terminal::COMMITTED_HANDOFF;
  }
  return expected == Preserve_trx_drain_terminal::COMMITTED_HANDOFF;
}

bool Preserve_trx_drain_ownership_state::shutdown_without_commit() {
  Preserve_trx_drain_terminal expected =
      Preserve_trx_drain_terminal::RUNNING;
  return m_state.compare_exchange_strong(
             expected, Preserve_trx_drain_terminal::SHUTDOWN_HANDOFF) ||
         expected == Preserve_trx_drain_terminal::SHUTDOWN_HANDOFF;
}

bool Preserve_trx_drain_ownership_state::restore_allowed() const {
  switch (state()) {
    case Preserve_trx_drain_terminal::RUNNING:
    case Preserve_trx_drain_terminal::FINAL_METADATA_ACCEPTED_LOCAL:
    case Preserve_trx_drain_terminal::SOURCE_RESTORE_PENDING:
    case Preserve_trx_drain_terminal::RESET_REQUESTED:
      return true;
    case Preserve_trx_drain_terminal::HANDOFF_PENDING:
    case Preserve_trx_drain_terminal::COMMIT_UNKNOWN:
    case Preserve_trx_drain_terminal::SOURCE_RESTORED:
    case Preserve_trx_drain_terminal::SHUTDOWN_HANDOFF:
    case Preserve_trx_drain_terminal::COMMITTED_HANDOFF:
      return false;
  }
  return false;
}

namespace {

constexpr uint64_t kMicrosecondsPerSecond = 1000000ULL;
constexpr uint64_t kExpiredReaperRollbackFailureRetryBackoffUs =
    60 * kMicrosecondsPerSecond;
constexpr uint64_t kFailedObservableRecordRetentionUs =
    600 * kMicrosecondsPerSecond;
constexpr char kExpiredReaperRollbackFailure[] =
    "expired-token reaper rollback failure";
constexpr uint32_t kBinlogCacheFlagWithSbr = 1U << 2;
constexpr uint32_t kBinlogCacheFlagWithRbr = 1U << 3;
constexpr uint32_t kBinlogCacheFlagWithStart = 1U << 4;
constexpr uint32_t kBinlogCacheFlagWithEnd = 1U << 5;
constexpr uint32_t kBinlogCacheFlagWithContent = 1U << 6;
constexpr uint32_t kBinlogSavepointCheckpointKnownFlags =
    kBinlogCacheFlagWithSbr | kBinlogCacheFlagWithRbr |
    kBinlogCacheFlagWithStart | kBinlogCacheFlagWithEnd |
    kBinlogCacheFlagWithContent;
constexpr size_t kBinlogSavepointCheckpointLength =
    sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint64_t);
constexpr uint32_t kTempSidecarBootstrapIoRetryAttempts = 3;

constexpr uint16_t kSavepointHandlerNone = 0;
constexpr uint16_t kSavepointHandlerInnodb = 1;
constexpr uint16_t kSavepointHandlerBinlog = 2;
constexpr uint16_t kSavepointHandlerSupportedMask =
    kSavepointHandlerInnodb | kSavepointHandlerBinlog;

constexpr uint16_t kMinReadableUserVariablesVersion = 1;
constexpr uint16_t kUserVariablesVersion = 2;
constexpr size_t kUserVariablesHeaderLength = 6;
constexpr size_t kUserVariableEntryFixedLength = 12;

std::string normalize_dir(std::string dir);
std::string preserve_trx_default_dir();
bool preserve_trx_token_to_xid(const std::string &token, XID *xid);

bool preserved_trx_startup_needed_crash_recovery() {
  DBUG_EXECUTE_IF("preserve_trx_force_crash_recovery_abandon_guard",
                  return true;);
  return recv_needed_recovery;
}

bool preserved_trx_listing_has_crash_abandon_artifacts(
    const Preserved_trx_carrier_listing &listing) {
  return !listing.snapshot_tokens.empty() ||
         !listing.external_blob_tokens.empty() ||
         !listing.temp_sidecar_tokens.empty() ||
         !listing.tainted_tokens.empty() ||
         !listing.consume_state_tokens.empty() ||
         !listing.warm_external_blob_artifacts.empty();
}

bool preserved_trx_crash_recovery_artifacts_forbidden(
    const Preserved_trx_carrier_listing &listing, const char *phase) {
  if (!preserved_trx_startup_needed_crash_recovery() ||
      !preserved_trx_listing_has_crash_abandon_artifacts(listing)) {
    return false;
  }

  std::string message =
      "PRESERVE: crash recovery requires external cleanup of preserved "
      "transaction artifacts before mysqld starts";
  if (phase != nullptr && phase[0] != '\0') {
    message.append(" during ");
    message.append(phase);
  }
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
  return true;
}

std::atomic<ulonglong> g_warmcopy_prefix_bytes{0};
std::atomic<ulonglong> g_warmcopy_digest_bytes{0};
std::atomic<ulonglong> g_warmcopy_durable_bytes{0};
std::atomic<ulonglong> g_warmcopy_provider_full_copy_to_count{0};
std::atomic<ulonglong> g_warmcopy_phase2_pause_us{0};
std::mutex g_warmcopy_status_mutex;

/*
  Latest two-phase batch-drain timing snapshot.

  These fields describe one drain attempt's blocked-window breakdown and are
  published only when the attempt recorded a phase-2 start time. They are not
  process-lifetime counters; cumulative SLO misses are tracked separately.
*/
struct Preserve_trx_phase2_metrics {
  uint64_t total_us{0};
  uint64_t target_wait_us{0};
  uint64_t closing_started_us{0};
  uint64_t closing_command_effective_budget_us{0};
  uint64_t closing_command_wait_us{0};
  uint64_t closing_command_timed_out_count{0};
  uint64_t closing_command_deadline_clamped{0};
  uint64_t closing_inflight_commands{0};
  uint64_t closing_completed_before_deadline{0};
  uint64_t closing_excluded_tokens{0};
  uint64_t closing_last_excluded_token{0};
  uint64_t phase2_transfer_tail_us{0};
  uint64_t closing_to_final_ack_us{0};
  uint64_t participant_prepare_us{0};
  uint64_t participant_close_us{0};
  uint64_t participant_preflight_us{0};
  uint64_t lock_seal_us{0};
  uint64_t target_preserve_us{0};
  uint64_t target_pin_us{0};
  uint64_t target_worker_wall_us{0};
  uint64_t target_result_collect_us{0};
  uint64_t target_deferred_dir_fsync_us{0};
  uint64_t transfer_commit_epoch_us{0};
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
  uint64_t temp_manifest_build_target_count{0};
  uint64_t snapshot_write_bundle_build_us{0};
  uint64_t snapshot_write_store_us{0};
  uint64_t snapshot_write_store_token_state_us{0};
  uint64_t snapshot_write_store_adopt_warm_blob_us{0};
  uint64_t snapshot_write_store_write_new_blobs_us{0};
  uint64_t snapshot_write_store_encode_us{0};
  uint64_t snapshot_write_store_write_snapshot_us{0};
  uint64_t register_us{0};
  uint64_t target_count{0};
  uint64_t savepoint_live_export_target_count{0};
  uint64_t early_staged_tokens{0};
  uint64_t command_boundary_to_enqueue_us_max{0};
  uint64_t final_fast_scan_us{0};
  uint64_t final_dirty_tokens{0};
  uint64_t final_replacement_tokens{0};
  uint64_t final_validation_rejects{0};
};

std::atomic<ulonglong> g_phase2_total_us{0};
std::atomic<ulonglong> g_phase2_target_wait_us{0};
std::atomic<ulonglong> g_closing_started_monotonic_us{0};
std::atomic<ulonglong> g_closing_command_effective_budget_us{0};
std::atomic<ulonglong> g_closing_command_wait_us{0};
std::atomic<ulonglong> g_closing_command_timed_out_count{0};
std::atomic<ulonglong> g_closing_command_deadline_clamped{0};
std::atomic<ulonglong> g_closing_inflight_commands{0};
std::atomic<ulonglong> g_closing_completed_before_deadline{0};
std::atomic<ulonglong> g_closing_excluded_tokens{0};
std::atomic<ulonglong> g_closing_last_excluded_token{0};
std::atomic<ulonglong> g_phase2_transfer_tail_us{0};
std::atomic<ulonglong> g_closing_to_final_ack_us{0};
std::atomic<ulonglong> g_phase2_participant_prepare_us{0};
std::atomic<ulonglong> g_phase2_participant_close_us{0};
std::atomic<ulonglong> g_phase2_participant_preflight_us{0};
std::atomic<ulonglong> g_phase2_lock_seal_us{0};
std::atomic<ulonglong> g_phase2_target_preserve_us{0};
std::atomic<ulonglong> g_phase2_lock_preflight_us{0};
std::atomic<ulonglong> g_phase2_prepare_us{0};
std::atomic<ulonglong> g_phase2_detach_claim_us{0};
std::atomic<ulonglong> g_phase2_snapshot_write_us{0};
std::atomic<ulonglong> g_phase2_register_us{0};
std::atomic<ulonglong> g_early_staged_tokens{0};
std::atomic<ulonglong> g_command_boundary_to_enqueue_us_max{0};
std::atomic<ulonglong> g_final_fast_scan_us{0};
std::atomic<ulonglong> g_final_dirty_tokens{0};
std::atomic<ulonglong> g_final_replacement_tokens{0};
std::atomic<ulonglong> g_final_validation_rejects{0};
std::atomic<ulonglong> g_phase1_readiness_samples{0};
std::atomic<ulonglong> g_phase1_readiness_inflight_commands{0};
std::atomic<ulonglong> g_phase1_readiness_oldest_command_age_us{0};
std::atomic<ulonglong> g_phase1_readiness_offender_count{0};
std::atomic<ulonglong> g_phase1_readiness_wait_us{0};
std::atomic<ulonglong> g_phase2_slo_miss_count{0};
std::atomic<ulonglong> g_resume_total_us{0};
std::atomic<ulonglong> g_startup_recovery_elapsed_us{0};
std::atomic<ulonglong> g_startup_recovery_error{0};
std::atomic<ulonglong> g_startup_recovery_snapshot_tokens{0};
std::atomic<ulonglong> g_startup_recovery_local_snapshot_tokens{0};
std::atomic<ulonglong> g_startup_recovery_binlog_cache_tokens{0};
std::atomic<ulonglong> g_startup_recovery_tainted_tokens{0};
std::atomic<ulonglong> g_startup_recovery_standby_pending_tokens{0};
std::atomic<ulonglong> g_startup_recovery_promotion_intent_tokens{0};
std::atomic<ulonglong> g_startup_recovery_orphan_rollback_count{0};
std::atomic<ulonglong> g_startup_recovery_phase_snapshot_load_us{0};
std::atomic<ulonglong> g_startup_recovery_phase_snapshot_validate_us{0};
std::atomic<ulonglong> g_startup_recovery_phase_snapshot_kernel_us{0};
std::atomic<ulonglong> g_startup_recovery_phase_snapshot_claim_us{0};
std::atomic<ulonglong> g_startup_recovery_phase_snapshot_read_view_us{0};
std::atomic<ulonglong> g_startup_recovery_phase_snapshot_table_locks_us{0};
std::atomic<ulonglong> g_startup_recovery_phase_snapshot_record_locks_us{0};
std::atomic<ulonglong> g_startup_recovery_phase_snapshot_record_lock_entries{0};
std::atomic<ulonglong>
    g_startup_recovery_phase_snapshot_record_lock_stable_page_hits{0};
std::atomic<ulonglong>
    g_startup_recovery_phase_snapshot_record_lock_image_resolves{0};
std::atomic<ulonglong>
    g_startup_recovery_phase_snapshot_record_lock_bitmap_pages{0};
std::atomic<ulonglong>
    g_startup_recovery_phase_snapshot_record_lock_bitmap_bits{0};
std::atomic<ulonglong>
    g_startup_recovery_phase_snapshot_record_lock_page_get_us{0};
std::atomic<ulonglong>
    g_startup_recovery_phase_snapshot_record_lock_page_get_count{0};
std::atomic<ulonglong>
    g_startup_recovery_phase_snapshot_record_lock_table_open_us{0};
std::atomic<ulonglong>
    g_startup_recovery_phase_snapshot_record_lock_prefetch_pages{0};
std::atomic<ulonglong>
    g_startup_recovery_phase_snapshot_record_lock_prefetch_bytes{0};
std::atomic<ulonglong>
    g_startup_recovery_phase_snapshot_record_lock_prefetch_residency_pages{0};
std::atomic<ulonglong>
    g_startup_recovery_phase_snapshot_record_lock_prefetch_resident_pages{0};
std::atomic<ulonglong>
    g_startup_recovery_phase_snapshot_record_lock_prefetch_io_pending_pages{0};
std::atomic<ulonglong>
    g_startup_recovery_phase_snapshot_record_lock_prefetch_missing_pages{0};
std::atomic<ulonglong> g_startup_recovery_phase_snapshot_predicate_locks_us{0};
std::atomic<ulonglong> g_startup_recovery_phase_snapshot_mdl_us{0};
std::atomic<ulonglong> g_startup_recovery_phase_snapshot_register_us{0};

constexpr char kPreservedTrxRecoveryAttemptLedgerFilename[] =
    ".recovery_attempt_ledger";
constexpr char kPreservedTrxRecoveryAttemptLedgerMagic[] =
    "PTRX_RECOVERY_ATTEMPT_LEDGER_V1";

void preserve_trx_warmcopy_reset_status() {
  g_warmcopy_prefix_bytes.store(0);
  g_warmcopy_digest_bytes.store(0);
  g_warmcopy_durable_bytes.store(0);
  g_warmcopy_provider_full_copy_to_count.store(0);
  g_warmcopy_phase2_pause_us.store(0);
}

void preserve_trx_phase2_reset_latest_metrics() {
  g_phase2_total_us.store(0);
  g_phase2_target_wait_us.store(0);
  g_closing_started_monotonic_us.store(0);
  g_closing_command_effective_budget_us.store(0);
  g_closing_command_wait_us.store(0);
  g_closing_command_timed_out_count.store(0);
  g_closing_command_deadline_clamped.store(0);
  g_closing_inflight_commands.store(0);
  g_closing_completed_before_deadline.store(0);
  g_closing_excluded_tokens.store(0);
  g_closing_last_excluded_token.store(0);
  g_phase2_transfer_tail_us.store(0);
  g_closing_to_final_ack_us.store(0);
  g_phase2_participant_prepare_us.store(0);
  g_phase2_participant_close_us.store(0);
  g_phase2_participant_preflight_us.store(0);
  g_phase2_lock_seal_us.store(0);
  g_phase2_target_preserve_us.store(0);
  g_phase2_lock_preflight_us.store(0);
  g_phase2_prepare_us.store(0);
  g_phase2_detach_claim_us.store(0);
  g_phase2_snapshot_write_us.store(0);
  g_phase2_register_us.store(0);
  g_early_staged_tokens.store(0);
  g_command_boundary_to_enqueue_us_max.store(0);
  g_final_fast_scan_us.store(0);
  g_final_dirty_tokens.store(0);
  g_final_replacement_tokens.store(0);
  g_final_validation_rejects.store(0);
}

static void preserve_trx_phase1_readiness_reset_latest_metrics() {
  g_phase1_readiness_samples.store(0);
  g_phase1_readiness_inflight_commands.store(0);
  g_phase1_readiness_oldest_command_age_us.store(0);
  g_phase1_readiness_offender_count.store(0);
  g_phase1_readiness_wait_us.store(0);
}

static void preserve_trx_phase1_readiness_note_latest_metrics(
    uint64_t samples, uint64_t inflight_commands,
    uint64_t oldest_command_age_us, uint64_t offender_count,
    uint64_t wait_us) {
  g_phase1_readiness_samples.store(static_cast<ulonglong>(samples));
  g_phase1_readiness_inflight_commands.store(
      static_cast<ulonglong>(inflight_commands));
  g_phase1_readiness_oldest_command_age_us.store(
      static_cast<ulonglong>(oldest_command_age_us));
  g_phase1_readiness_offender_count.store(
      static_cast<ulonglong>(offender_count));
  g_phase1_readiness_wait_us.store(static_cast<ulonglong>(wait_us));
}

void preserve_trx_phase2_note_latest_metrics(
    const Preserve_trx_phase2_metrics &metrics) {
  g_phase2_total_us.store(static_cast<ulonglong>(metrics.total_us));
  g_phase2_target_wait_us.store(static_cast<ulonglong>(metrics.target_wait_us));
  g_closing_started_monotonic_us.store(
      static_cast<ulonglong>(metrics.closing_started_us));
  g_closing_command_effective_budget_us.store(
      static_cast<ulonglong>(metrics.closing_command_effective_budget_us));
  g_closing_command_wait_us.store(
      static_cast<ulonglong>(metrics.closing_command_wait_us));
  g_closing_command_timed_out_count.store(
      static_cast<ulonglong>(metrics.closing_command_timed_out_count));
  g_closing_command_deadline_clamped.store(
      static_cast<ulonglong>(metrics.closing_command_deadline_clamped));
  g_closing_inflight_commands.store(
      static_cast<ulonglong>(metrics.closing_inflight_commands));
  g_closing_completed_before_deadline.store(
      static_cast<ulonglong>(metrics.closing_completed_before_deadline));
  g_closing_excluded_tokens.store(
      static_cast<ulonglong>(metrics.closing_excluded_tokens));
  g_closing_last_excluded_token.store(
      static_cast<ulonglong>(metrics.closing_last_excluded_token));
  g_phase2_transfer_tail_us.store(
      static_cast<ulonglong>(metrics.phase2_transfer_tail_us));
  g_closing_to_final_ack_us.store(
      static_cast<ulonglong>(metrics.closing_to_final_ack_us));
  g_phase2_participant_prepare_us.store(
      static_cast<ulonglong>(metrics.participant_prepare_us));
  g_phase2_participant_close_us.store(
      static_cast<ulonglong>(metrics.participant_close_us));
  g_phase2_participant_preflight_us.store(
      static_cast<ulonglong>(metrics.participant_preflight_us));
  g_phase2_lock_seal_us.store(static_cast<ulonglong>(metrics.lock_seal_us));
  g_phase2_target_preserve_us.store(
      static_cast<ulonglong>(metrics.target_preserve_us));
  g_phase2_lock_preflight_us.store(
      static_cast<ulonglong>(metrics.lock_preflight_us));
  g_phase2_prepare_us.store(static_cast<ulonglong>(metrics.prepare_us));
  g_phase2_detach_claim_us.store(
      static_cast<ulonglong>(metrics.detach_claim_us));
  g_phase2_snapshot_write_us.store(
      static_cast<ulonglong>(metrics.snapshot_write_us));
  g_phase2_register_us.store(static_cast<ulonglong>(metrics.register_us));
  g_early_staged_tokens.store(
      static_cast<ulonglong>(metrics.early_staged_tokens));
  g_command_boundary_to_enqueue_us_max.store(
      static_cast<ulonglong>(metrics.command_boundary_to_enqueue_us_max));
  g_final_fast_scan_us.store(
      static_cast<ulonglong>(metrics.final_fast_scan_us));
  g_final_dirty_tokens.store(
      static_cast<ulonglong>(metrics.final_dirty_tokens));
  g_final_replacement_tokens.store(
      static_cast<ulonglong>(metrics.final_replacement_tokens));
  g_final_validation_rejects.store(
      static_cast<ulonglong>(metrics.final_validation_rejects));
  if (metrics.total_us > kMicrosecondsPerSecond) {
    g_phase2_slo_miss_count.fetch_add(1);
  }
}

void preserve_trx_warmcopy_note_provider_full_copy_to() {
  g_warmcopy_provider_full_copy_to_count.fetch_add(1);
}

void preserve_trx_warmcopy_note_phase2_pause_us(uint64_t phase2_pause_us) {
  g_warmcopy_phase2_pause_us.fetch_add(phase2_pause_us);
}

constexpr Preserved_trx_column_metadata kPreservedTrxColumns[] = {
    {"TOKEN", PRESERVE_TRX_TOKEN_MAX_LENGTH},
    {"USER", 32},
    {"HOST", 255},
    {"STATE", 32},
    {"CREATED_AT", 26},
    {"EXPIRES_AT", 26},
    {"RECOVERED_COUNT", 20},
    {"AGE_SECONDS", 20},
    {"SCHEMA_NAME", 64},
    {"ISOLATION", 32},
    {"MOD_TABLES_COUNT", 20},
    {"LOCKS_COUNT", 20},
    {"HAS_READ_VIEW", 3},
    {"RV_LOW_LIMIT_NO", 20},
    {"SAVEPOINT_COUNT", 20},
    {"BINLOG_STATE", 32},
    {"WROTE_TO_CACHE", 3},
    {"BINLOG_CACHE_SIZE", 20},
    {"BINLOG_WARMCOPY_STATE", 32},
    {"SESSION_SQL_LOG_BIN", 3},
    {"GLOBAL_LOG_BIN", 3},
    {"GTID_NEXT", 1024},
    {"AUTOINC_LOCK_OWNED", 3},
    {"TEMP_TABLE_STATE", 32},
    {"TEMP_IMAGE_BYTES", 20},
    {"TEMP_UNDO_BYTES", 20},
    {"TEMP_SIDECARS_COMPLETE", 3},
    {"LAST_ERROR", 1024},
    {"LAST_ERROR_AT", 26},
};

struct Preserve_trx_manager_state_owner {
  Preserve_trx_manager_state state{Preserve_trx_manager_state::IDLE};
  my_thread_id owner_thread_id{0};
};

uint64_t preserve_trx_pack_manager_state_owner(
    Preserve_trx_manager_state state, my_thread_id owner_thread_id) {
  return (static_cast<uint64_t>(owner_thread_id) << 32) |
         static_cast<uint64_t>(state);
}

Preserve_trx_manager_state_owner preserve_trx_unpack_manager_state_owner(
    uint64_t packed) {
  Preserve_trx_manager_state_owner snapshot;
  snapshot.state = static_cast<Preserve_trx_manager_state>(
      packed & static_cast<uint64_t>(UINT32_MAX));
  snapshot.owner_thread_id = static_cast<my_thread_id>(packed >> 32);
  return snapshot;
}

std::atomic<uint64_t> g_manager_state_owner{
    preserve_trx_pack_manager_state_owner(Preserve_trx_manager_state::IDLE, 0)};
std::atomic<ulonglong> g_batch_generation{0};
std::atomic<ulonglong> g_reset_drain_wins{0};
std::atomic<ulonglong> g_reset_drain_too_late{0};
std::atomic<ulonglong> g_closing_control_connection_commands{0};
bool g_startup_resurrection_preamble_failed{false};
std::mutex g_closing_target_classification_mutex;
Preserved_trx_manager_state_publication_probe
    g_manager_state_publication_probe{nullptr};
void *g_manager_state_publication_probe_arg{nullptr};

enum class Preserve_trx_reset_disposition : uint8_t {
  RESTORED_RUNNABLE,
  CONNECTION_TEARDOWN_PENDING
};

struct Preserve_trx_batch_item {
  my_thread_id original_thread_id{0};
  std::string token;
  bool logged_binlog_cache{false};
  bool local_authority_staged{false};
  std::unique_ptr<Preserve_trx_source_rollback_image> source_rollback_image;
  Preserve_trx_reset_disposition reset_disposition{
      Preserve_trx_reset_disposition::RESTORED_RUNNABLE};
};

struct Preserve_trx_batch_reset_cleanup {
  std::string token;
};

struct Preserve_trx_drain_attempt {
  Preserve_trx_drain_attempt(ulonglong attempt_generation,
                             my_thread_id attempt_owner_thread_id)
      : generation(attempt_generation),
        owner_thread_id(attempt_owner_thread_id) {}

  ulonglong generation{0};
  my_thread_id owner_thread_id{0};
  Preserve_trx_drain_ownership_state ownership;
  std::atomic<bool> closing_command_gate_published{false};
  std::atomic<bool> reset_release_barrier_complete{false};
  std::atomic<bool> source_restore_context_ready{false};
  std::atomic<bool> drain_scope_released{false};
  std::mutex sink_mutex;
  Preserve_trx_transfer_encoded_frame_sink *sink{nullptr};
  std::mutex quarantine_mutex;
  std::vector<Preserve_trx_batch_item> quarantined_items;
  std::set<std::string> quarantined_source_warmcopy_ids;
  uint64_t quarantine_started_monotonic_us{0};
  std::atomic<bool> handoff_resolution_ready{false};
  Preserve_trx_handoff_resolution_state handoff_resolution;
};

std::mutex g_active_drain_attempt_mutex;
std::shared_ptr<Preserve_trx_drain_attempt> g_active_drain_attempt;
std::shared_ptr<Preserve_trx_drain_attempt> g_last_resolved_drain_attempt;

[[noreturn]] void preserve_trx_reset_invariant_failure(
    const char *reason,
    const std::shared_ptr<Preserve_trx_drain_attempt> &attempt = nullptr) {
  const Preserve_trx_manager_state_owner manager =
      preserve_trx_unpack_manager_state_owner(g_manager_state_owner.load());
  char message[512];
  snprintf(message, sizeof(message),
           "PRESERVE: RESET DRAIN invariant failure reason=%s generation=%llu "
           "manager=%d manager_owner=%llu ownership=%d",
           reason == nullptr ? "unknown" : reason,
           attempt == nullptr ? 0 : attempt->generation,
           static_cast<int>(manager.state),
           static_cast<unsigned long long>(manager.owner_thread_id),
           attempt == nullptr ? -1
                              : static_cast<int>(attempt->ownership.state()));
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message);
  std::abort();
}

static bool preserve_trx_try_restore_quarantined_reset(
    const std::shared_ptr<Preserve_trx_drain_attempt> &attempt);
static void preserved_trx_reset_attempt_reaper_scan_once();

void preserve_trx_notify_manager_state_published_for_unit_test() {
  Preserved_trx_manager_state_publication_probe probe =
      g_manager_state_publication_probe;
  if (probe != nullptr) probe(g_manager_state_publication_probe_arg);
}

Preserve_trx_manager_state_owner preserve_trx_manager_state_owner_snapshot() {
  return preserve_trx_unpack_manager_state_owner(g_manager_state_owner.load());
}

bool preserve_trx_compare_exchange_manager_state_owner(
    Preserve_trx_manager_state expected_state, my_thread_id expected_owner,
    Preserve_trx_manager_state desired_state, my_thread_id desired_owner) {
  uint64_t expected =
      preserve_trx_pack_manager_state_owner(expected_state, expected_owner);
  const uint64_t desired =
      preserve_trx_pack_manager_state_owner(desired_state, desired_owner);
  return g_manager_state_owner.compare_exchange_strong(expected, desired);
}

bool preserve_trx_compare_exchange_manager_state_owner(
    Preserve_trx_manager_state from, Preserve_trx_manager_state to,
    my_thread_id owner_thread_id) {
  return preserve_trx_compare_exchange_manager_state_owner(
      from, 0, to, owner_thread_id);
}

void preserve_trx_store_manager_state_owner(Preserve_trx_manager_state state,
                                            my_thread_id owner_thread_id) {
  g_manager_state_owner.store(
      preserve_trx_pack_manager_state_owner(state, owner_thread_id));
}

std::shared_ptr<Preserve_trx_drain_attempt>
preserve_trx_active_drain_attempt_snapshot() {
  std::lock_guard<std::mutex> lock(g_active_drain_attempt_mutex);
  return g_active_drain_attempt;
}

void preserve_trx_clear_active_drain_attempt(
    const std::shared_ptr<Preserve_trx_drain_attempt> &attempt) {
  if (attempt == nullptr) return;
  std::lock_guard<std::mutex> lock(g_active_drain_attempt_mutex);
  if (g_active_drain_attempt == attempt) g_active_drain_attempt.reset();
}

bool preserve_trx_publish_reset_cleanup(
    const Preserve_trx_drain_attempt &attempt) {
  for (;;) {
    const Preserve_trx_manager_state_owner snapshot =
        preserve_trx_manager_state_owner_snapshot();
    if (snapshot.state == Preserve_trx_manager_state::RESET_CLEANUP &&
        (snapshot.owner_thread_id == attempt.owner_thread_id ||
         snapshot.owner_thread_id == 0)) {
      return true;
    }
    Preserve_trx_manager_state desired_from = snapshot.state;
    my_thread_id desired_owner = attempt.owner_thread_id;
    switch (snapshot.state) {
      case Preserve_trx_manager_state::WARMCOPY_DRAINING:
      case Preserve_trx_manager_state::WARMCOPY_CLOSING:
      case Preserve_trx_manager_state::BATCH_DRAINING:
      case Preserve_trx_manager_state::SHUTDOWN_REQUESTED:
        if (snapshot.owner_thread_id != attempt.owner_thread_id) return false;
        break;
      case Preserve_trx_manager_state::TRANSFER_HANDOFF_COMPLETE:
        if (snapshot.owner_thread_id != 0) return false;
        desired_owner = 0;
        break;
      default:
        return false;
    }
    if (preserve_trx_compare_exchange_manager_state_owner(
            desired_from, snapshot.owner_thread_id,
            Preserve_trx_manager_state::RESET_CLEANUP, desired_owner)) {
      return true;
    }
  }
}

void preserve_trx_cancel_active_drain_sink(
    const std::shared_ptr<Preserve_trx_drain_attempt> &attempt) {
  if (attempt == nullptr) return;
  std::lock_guard<std::mutex> lock(attempt->sink_mutex);
  if (attempt->sink != nullptr) attempt->sink->request_cancel();
}

bool preserve_trx_register_active_drain_sink(
    const std::shared_ptr<Preserve_trx_drain_attempt> &attempt,
    Preserve_trx_transfer_encoded_frame_sink *sink) {
  if (attempt == nullptr || sink == nullptr) return false;
  std::lock_guard<std::mutex> lock(attempt->sink_mutex);
  if (attempt->ownership.state() != Preserve_trx_drain_terminal::RUNNING) {
    sink->request_cancel();
    return false;
  }
  attempt->sink = sink;
  return true;
}

void preserve_trx_unregister_active_drain_sink(
    const std::shared_ptr<Preserve_trx_drain_attempt> &attempt,
    Preserve_trx_transfer_encoded_frame_sink *sink) {
  if (attempt == nullptr || sink == nullptr) return;
  std::lock_guard<std::mutex> lock(attempt->sink_mutex);
  if (attempt->sink == sink) attempt->sink = nullptr;
}

bool preserve_trx_active_drain_reset_requested(
    const std::shared_ptr<Preserve_trx_drain_attempt> &attempt) {
  if (attempt == nullptr) return false;
  const Preserve_trx_drain_terminal state = attempt->ownership.state();
  return state == Preserve_trx_drain_terminal::RESET_REQUESTED ||
         state == Preserve_trx_drain_terminal::SOURCE_RESTORE_PENDING;
}

void preserve_trx_publish_active_drain_reset_barrier(
    const std::shared_ptr<Preserve_trx_drain_attempt> &attempt) {
  if (attempt != nullptr)
    attempt->reset_release_barrier_complete.store(true,
                                                   std::memory_order_release);
}

Preserve_trx_reset_drain_result preserve_trx_request_active_drain_reset_impl(
    bool wait_for_runnable) {
  std::unique_lock<std::mutex> active_guard(g_active_drain_attempt_mutex);
  const std::shared_ptr<Preserve_trx_drain_attempt> attempt =
      g_active_drain_attempt;
  if (attempt == nullptr) {
    const Preserve_trx_manager_state_owner manager =
        preserve_trx_manager_state_owner_snapshot();
    switch (manager.state) {
      case Preserve_trx_manager_state::IDLE:
        return Preserve_trx_reset_drain_result::NO_ACTIVE;
      case Preserve_trx_manager_state::WARMCOPY_DRAINING:
      case Preserve_trx_manager_state::WARMCOPY_CLOSING:
      case Preserve_trx_manager_state::BATCH_DRAINING:
      case Preserve_trx_manager_state::SHUTDOWN_REQUESTED:
        return Preserve_trx_reset_drain_result::UNSUPPORTED;
      case Preserve_trx_manager_state::TRANSFER_HANDOFF_COMPLETE:
      case Preserve_trx_manager_state::RESET_CLEANUP:
        preserve_trx_reset_invariant_failure(
            "terminal_manager_without_transfer_attempt");
      default:
        return Preserve_trx_reset_drain_result::UNSUPPORTED;
    }
  }

  const Preserve_trx_drain_reset_request request =
      attempt->ownership.request_reset();
  Preserve_trx_reset_drain_result result =
      Preserve_trx_reset_drain_result::RESET_JOINED;
  switch (request) {
    case Preserve_trx_drain_reset_request::WON:
      result = Preserve_trx_reset_drain_result::RESET_WON;
      if (!preserve_trx_publish_reset_cleanup(*attempt)) {
        preserve_trx_reset_invariant_failure(
            "reset_manager_publication_failed", attempt);
      }
      g_reset_drain_wins.fetch_add(1);
      break;
    case Preserve_trx_drain_reset_request::JOINED:
    case Preserve_trx_drain_reset_request::ALREADY_RESTORED:
      break;
    case Preserve_trx_drain_reset_request::TOO_LATE:
      g_reset_drain_too_late.fetch_add(1);
      return Preserve_trx_reset_drain_result::TOO_LATE;
    case Preserve_trx_drain_reset_request::INVALID:
      preserve_trx_reset_invariant_failure("invalid_reset_ownership", attempt);
  }

  preserve_trx_cancel_active_drain_sink(attempt);
  active_guard.unlock();
  if (request != Preserve_trx_drain_reset_request::ALREADY_RESTORED) {
    (void)preserve_trx_try_restore_quarantined_reset(attempt);
  }
  while (wait_for_runnable &&
         !attempt->reset_release_barrier_complete.load(
             std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return result;
}

bool preserve_trx_try_active_drain_shutdown_handoff(
    Preserve_trx_drain_attempt *attempt) {
  if (attempt == nullptr) return false;
  return attempt->ownership.shutdown_without_commit();
}

bool preserve_trx_active_drain_before_commit_send(void *context) {
  auto *attempt = static_cast<Preserve_trx_drain_attempt *>(context);
  if (attempt == nullptr) return false;
  const Preserve_trx_drain_terminal before = attempt->ownership.state();
  if (!attempt->ownership.begin_commit_send()) return false;
  if (before != Preserve_trx_drain_terminal::HANDOFF_PENDING) {
    preserve_trx_transfer_note_source_handoff_pending();
  }
  return true;
}

bool preserve_trx_active_drain_final_ack_arbiter(void *context) {
  auto *attempt = static_cast<Preserve_trx_drain_attempt *>(context);
  if (attempt == nullptr || !attempt->ownership.acknowledge_commit())
    return false;
  preserve_trx_transfer_note_source_handoff_committed();
  return true;
}

/*
  Internal token lifecycle.

  The lifecycle state is the operator-visible phase of a preserved record; the
  resumable flag is the separate claim gate. Batch delivery can publish a
  DRAINING record with resumable=true after the durable token exists, while the
  drain manager and command gates still prevent ordinary concurrent resume until
  shutdown/recovery hands ownership to the next server instance. PRESERVED is
  the normal steady state. ADOPTED_FOR_PROMOTION is a promotion-owned record:
  ordinary SQL RESUME must not consume it until the promotion resume core hands
  it to a pinned target session. RESUMING and ROLLING_BACK claim the record so
  only one owner can attach or clean up the preserved trx. EXPIRED_* states are
  owned by the reaper and remain observable if cleanup cannot finish. FAILED
  records are diagnostic-only and are removed by the observable-record GC
  deadline.
*/
enum class Preserved_trx_lifecycle_state {
  DRAINING,
  SNAPSHOTTING,
  PRESERVED,
  ADOPTED_FOR_PROMOTION,
  RESUMING,
  ROLLING_BACK,
  EXPIRED_ROLLBACK,
  EXPIRED_CLEANUP_FAILED,
  FAILED
};

struct Preserved_trx_record {
  Preserve_snapshot_metadata metadata;
  trx_t *trx{nullptr};
  bool resumable{true};
  Preserved_trx_lifecycle_state state{
      Preserved_trx_lifecycle_state::PRESERVED};
  bool observable_only{false};
  std::string last_error;
  uint64_t last_error_at_us{0};
  uint64_t expires_at_monotonic_us{0};
  uint64_t last_error_monotonic_us{0};
  uint64_t observable_gc_at_monotonic_us{0};
  std::vector<Preserved_trx_external_blob_descriptor> blob_descriptors;
  bool has_promotion_key{false};
  Preserve_trx_prepared_token_key promotion_key;
};

static bool preserved_trx_promotion_keys_match(
    const Preserve_trx_prepared_token_key &lhs,
    const Preserve_trx_prepared_token_key &rhs) {
  return lhs.preserve_dir == rhs.preserve_dir &&
         lhs.epoch_scope == rhs.epoch_scope && lhs.epoch_id == rhs.epoch_id &&
         lhs.token == rhs.token &&
         lhs.target_boot_incarnation == rhs.target_boot_incarnation &&
         lhs.generation == rhs.generation;
}

struct Preserve_user_var_snapshot_entry {
  std::string name;
  std::string value;
  Item_result type{STRING_RESULT};
  uint16_t charset_number{0};
  Derivation derivation{DERIVATION_IMPLICIT};
  bool unsigned_flag{false};
  bool is_null{false};
};

std::mutex g_preserved_trx_mutex;
std::condition_variable g_preserved_trx_recovery_cond;
bool g_preserved_trx_recovery_done = false;
bool g_preserved_trx_recovery_deferred = false;
std::vector<Preserved_trx_record> g_preserved_trx_records;
std::mutex g_preserved_trx_reaper_mutex;
std::condition_variable g_preserved_trx_reaper_cond;
std::thread g_preserved_trx_reaper_thread;
bool g_preserved_trx_reaper_started = false;
bool g_preserved_trx_reaper_starting = false;
bool g_preserved_trx_reaper_stopping = false;
bool g_preserved_trx_reaper_stop = false;
bool g_preserved_trx_reaper_scan_requested = false;
bool g_preserved_trx_reaper_init_reported = false;
bool g_preserved_trx_reaper_init_failed = false;
bool g_preserved_trx_reaper_pause_init_report_for_unit_test = false;
std::atomic<bool> g_preserved_trx_reaper_fail_init_for_unit_test{false};
struct Deferred_source_warm_blob_cleanup {
  std::string dir;
  std::set<std::string> warmcopy_ids;
};
std::vector<Deferred_source_warm_blob_cleanup>
    g_deferred_source_warm_blob_cleanup;
std::mutex g_preserved_trx_thd_pin_mutex;
std::condition_variable g_preserved_trx_thd_pin_cond;
std::unordered_map<THD *, uint> g_preserved_trx_thd_pin_counts;
std::unordered_set<THD *> g_preserved_trx_thd_teardown;

static uint64_t preserve_trx_monotonic_us() {
  using clock = std::chrono::steady_clock;
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          clock::now().time_since_epoch())
          .count());
}

static uint64_t preserve_trx_monotonic_deadline_after_us(uint64_t now_us,
                                                         uint64_t delay_us) {
  if (now_us > std::numeric_limits<uint64_t>::max() - delay_us)
    return std::numeric_limits<uint64_t>::max();
  return now_us + delay_us;
}

static uint64_t preserve_trx_monotonic_deadline_after_ms(uint64_t now_us,
                                                         uint64_t timeout_ms) {
  if (timeout_ms == 0) return 0;
  if (timeout_ms > std::numeric_limits<uint64_t>::max() / 1000)
    return std::numeric_limits<uint64_t>::max();
  return preserve_trx_monotonic_deadline_after_us(now_us, timeout_ms * 1000);
}

static bool preserve_trx_monotonic_deadline_expired_at(uint64_t deadline_us,
                                                       uint64_t now_us) {
  return deadline_us != 0 && now_us >= deadline_us;
}

static unsigned long preserve_trx_monotonic_timeout_ms_until_deadline_at(
    uint64_t deadline_us, unsigned long fallback_timeout_ms, uint64_t now_us) {
  if (deadline_us == 0) return fallback_timeout_ms;
  if (now_us >= deadline_us) return 1;
  const uint64_t remaining_us = deadline_us - now_us;
  const uint64_t remaining_ms = (remaining_us + 999) / 1000;
  return remaining_ms > std::numeric_limits<unsigned long>::max()
             ? std::numeric_limits<unsigned long>::max()
             : static_cast<unsigned long>(
                   std::max<uint64_t>(1, remaining_ms));
}

static uint64_t preserve_trx_wall_deadline_to_monotonic(
    uint64_t deadline_wall_us, uint64_t anchor_wall_us,
    uint64_t anchor_monotonic_us) {
  if (deadline_wall_us == 0) return 0;
  if (deadline_wall_us <= anchor_wall_us) return anchor_monotonic_us;
  const uint64_t delta_us = deadline_wall_us - anchor_wall_us;
  if (delta_us > std::numeric_limits<uint64_t>::max() - anchor_monotonic_us)
    return std::numeric_limits<uint64_t>::max();
  return anchor_monotonic_us + delta_us;
}

static uint64_t preserve_trx_wall_deadline_for_record(
    const Preserve_snapshot_metadata &metadata) {
  if (metadata.expires_at_us == 0) return 1;

  uint64_t deadline_us = metadata.expires_at_us;
  if (metadata.recovered_count == 1) {
    const uint64_t started_at_us =
        server_start_time <= 0
            ? my_micro_time()
            : static_cast<uint64_t>(server_start_time) *
                  kMicrosecondsPerSecond;
    const uint64_t grace_deadline_us =
        started_at_us +
        static_cast<uint64_t>(preserve_trx_recovery_grace_seconds) *
            kMicrosecondsPerSecond;
    deadline_us = std::max(deadline_us, grace_deadline_us);
  }
  return deadline_us;
}

static uint64_t preserve_trx_recovery_wall_deadline_for_snapshot(
    const Preserve_snapshot_metadata &metadata, uint64_t anchor_wall_us) {
  if (metadata.expires_at_us == 0) return 1;

  uint64_t deadline_us = metadata.expires_at_us;
  if (metadata.recovered_count == 0) {
    const uint64_t started_at_us =
        server_start_time <= 0
            ? anchor_wall_us
            : static_cast<uint64_t>(server_start_time) *
                  kMicrosecondsPerSecond;
    const uint64_t grace_deadline_us =
        started_at_us +
        static_cast<uint64_t>(preserve_trx_recovery_grace_seconds) *
            kMicrosecondsPerSecond;
    deadline_us = std::max(deadline_us, grace_deadline_us);
  }
  return deadline_us;
}

static void preserved_trx_initialize_record_deadlines(
    Preserved_trx_record *record, uint64_t anchor_wall_us = my_micro_time(),
    uint64_t anchor_monotonic_us = preserve_trx_monotonic_us()) {
  if (record == nullptr) return;
  const uint64_t deadline_wall_us =
      preserve_trx_wall_deadline_for_record(record->metadata);
  record->expires_at_monotonic_us = preserve_trx_wall_deadline_to_monotonic(
      deadline_wall_us, anchor_wall_us, anchor_monotonic_us);
}

static void preserved_trx_initialize_observable_gc_deadline(
    Preserved_trx_record *record,
    uint64_t anchor_monotonic_us = preserve_trx_monotonic_us()) {
  if (record == nullptr || !record->observable_only) return;
  if (record->state != Preserved_trx_lifecycle_state::FAILED &&
      record->state != Preserved_trx_lifecycle_state::EXPIRED_CLEANUP_FAILED) {
    record->observable_gc_at_monotonic_us = 0;
    return;
  }
  if (kFailedObservableRecordRetentionUs >
      std::numeric_limits<uint64_t>::max() - anchor_monotonic_us) {
    record->observable_gc_at_monotonic_us =
        std::numeric_limits<uint64_t>::max();
  } else {
    record->observable_gc_at_monotonic_us =
        anchor_monotonic_us + kFailedObservableRecordRetentionUs;
  }
}

static bool preserved_trx_record_resume_deadline_expired(
    const Preserved_trx_record &record,
    uint64_t now_monotonic_us = preserve_trx_monotonic_us()) {
  const uint64_t deadline_us = record.expires_at_monotonic_us;
  return deadline_us != 0 && now_monotonic_us >= deadline_us;
}

static size_t preserved_trx_gc_failed_observable_records(
    uint64_t now_monotonic_us = preserve_trx_monotonic_us()) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  size_t removed = 0;
  for (auto it = g_preserved_trx_records.begin();
       it != g_preserved_trx_records.end();) {
    const bool gc_eligible =
        it->observable_only &&
        (it->state == Preserved_trx_lifecycle_state::FAILED ||
         it->state == Preserved_trx_lifecycle_state::EXPIRED_CLEANUP_FAILED) &&
        it->observable_gc_at_monotonic_us != 0 &&
        now_monotonic_us >= it->observable_gc_at_monotonic_us;
    if (gc_eligible) {
      it = g_preserved_trx_records.erase(it);
      ++removed;
    } else {
      ++it;
    }
  }
  return removed;
}

class Preserve_trx_external_thd_pin {
 public:
  static std::unique_ptr<Preserve_trx_external_thd_pin> acquire_locked(
      THD *thd) {
    std::unique_ptr<Preserve_trx_external_thd_pin> pin(
        new Preserve_trx_external_thd_pin(thd));
    {
      std::lock_guard<std::mutex> lock(g_preserved_trx_thd_pin_mutex);
      if (thd == nullptr || thd->release_resources_done() ||
          g_preserved_trx_thd_teardown.count(thd) != 0) {
        return nullptr;
      }
      ++g_preserved_trx_thd_pin_counts[thd];
    }
    DEBUG_SYNC(current_thd, "preserve_trx_external_thd_pin_acquired");
    return pin;
  }

  Preserve_trx_external_thd_pin(const Preserve_trx_external_thd_pin &) =
      delete;
  Preserve_trx_external_thd_pin &operator=(
      const Preserve_trx_external_thd_pin &) = delete;

  ~Preserve_trx_external_thd_pin() { release(); }

  THD *thd() const { return m_thd; }

 private:
  explicit Preserve_trx_external_thd_pin(THD *thd) : m_thd(thd) {}

  void release() {
    if (m_thd == nullptr) return;
    {
      std::lock_guard<std::mutex> lock(g_preserved_trx_thd_pin_mutex);
      auto it = g_preserved_trx_thd_pin_counts.find(m_thd);
      if (it != g_preserved_trx_thd_pin_counts.end()) {
        assert(it->second > 0);
        --it->second;
        if (it->second == 0) g_preserved_trx_thd_pin_counts.erase(it);
      }
    }
    m_thd = nullptr;
    g_preserved_trx_thd_pin_cond.notify_all();
  }

  THD *m_thd{nullptr};
};

struct Preserve_trx_pinned_thd {
  THD *thd{nullptr};
  std::unique_ptr<Preserve_trx_external_thd_pin> pin;
};
std::string lex_cstring_to_string(LEX_CSTRING value) {
  return value.str == nullptr ? std::string()
                              : std::string(value.str, value.length);
}

const char *binlog_state_name(Preserve_snapshot_binlog_state state) {
  switch (state) {
    case Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE:
      return "GLOBAL_OFF_NO_CACHE";
    case Preserve_snapshot_binlog_state::SESSION_OFF_NO_CACHE:
      return "SESSION_OFF_NO_CACHE";
    case Preserve_snapshot_binlog_state::LOGGED_EMPTY:
      return "LOGGED_EMPTY";
    case Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE:
      return "LOGGED_WITH_CACHE";
  }
  return "UNKNOWN";
}

const char *preserved_trx_lifecycle_state_name(
    Preserved_trx_lifecycle_state state) {
  switch (state) {
    case Preserved_trx_lifecycle_state::DRAINING:
      return "DRAINING";
    case Preserved_trx_lifecycle_state::SNAPSHOTTING:
      return "SNAPSHOTTING";
    case Preserved_trx_lifecycle_state::PRESERVED:
      return "PRESERVED";
    case Preserved_trx_lifecycle_state::ADOPTED_FOR_PROMOTION:
      return "ADOPTED_FOR_PROMOTION";
    case Preserved_trx_lifecycle_state::RESUMING:
      return "RESUMING";
    case Preserved_trx_lifecycle_state::ROLLING_BACK:
      return "ROLLING_BACK";
    case Preserved_trx_lifecycle_state::EXPIRED_ROLLBACK:
      return "EXPIRED_ROLLBACK";
    case Preserved_trx_lifecycle_state::EXPIRED_CLEANUP_FAILED:
      return "EXPIRED_CLEANUP_FAILED";
    case Preserved_trx_lifecycle_state::FAILED:
      return "FAILED";
  }
  return "FAILED";
}

uint64_t read_le64(const std::string &bytes, size_t offset);
uint32_t read_le32(const std::string &bytes, size_t offset);
uint16_t read_le16(const std::string &bytes, size_t offset);

std::string format_timestamp_us(uint64_t timestamp_us) {
  if (timestamp_us == 0) return {};

  const time_t sec = static_cast<time_t>(timestamp_us / kMicrosecondsPerSecond);
  const uint64_t subsecond = timestamp_us % kMicrosecondsPerSecond;
  tm local_tm;
#ifdef _WIN32
  if (localtime_s(&local_tm, &sec) != 0) return {};
#else
  if (localtime_r(&sec, &local_tm) == nullptr) return {};
#endif

  char buf[64];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%06llu",
                local_tm.tm_year + 1900, local_tm.tm_mon + 1,
                local_tm.tm_mday, local_tm.tm_hour, local_tm.tm_min,
                local_tm.tm_sec, static_cast<unsigned long long>(subsecond));
  return buf;
}

const char *binlog_warmcopy_state_name(
    const Preserve_snapshot_metadata &metadata) {
  if (metadata.binlog_state != Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE ||
      metadata.binlog_cache_size == 0) {
    return "NONE";
  }
  return metadata.binlog_cache_warmcopy ? "READY" : "NOT_USED";
}

void populate_temp_table_observability(
    const std::string &token, const std::string &manifest_payload,
    Preserved_trx_view_row *row) {
  if (row == nullptr) return;
  row->temp_table_state = "NONE";
  row->temp_image_bytes = 0;
  row->temp_undo_bytes = 0;
  row->temp_sidecars_complete = true;

  if (manifest_payload.empty()) return;

  Preserved_temp_table_manifest manifest;
  if (!preserve_trx_decode_temp_table_manifest(manifest_payload, &manifest)) {
    row->temp_table_state = "CORRUPT";
    row->temp_sidecars_complete = false;
    return;
  }

  std::set<std::string> image_blobs;
  std::set<std::string> undo_blobs;
  for (const Preserved_temp_table_manifest_entry &entry : manifest.tables) {
    if (image_blobs.insert(entry.image.blob_name).second) {
      row->temp_image_bytes += entry.image.size;
    }
  }
  for (const Preserved_temp_table_undo_descriptor &undo :
       manifest.undo_images) {
    if (undo_blobs.insert(undo.blob_name).second) {
      row->temp_undo_bytes += undo.size;
    }
  }

  std::string reason;
  Preserve_snapshot_metadata sidecar_metadata;
  sidecar_metadata.token = token;
  sidecar_metadata.temp_table_manifest_payload = manifest_payload;
  const Preserve_snapshot_status status =
      preserve_trx_temp_table_check_sidecars_present(
          preserved_trx_dir_value(), token, sidecar_metadata, &reason);
  if (status == Preserve_snapshot_status::OK) {
    row->temp_table_state = "READY";
    row->temp_sidecars_complete = true;
  } else {
    row->temp_table_state = "INCOMPLETE";
    row->temp_sidecars_complete = false;
  }
}

Preserved_trx_view_row record_to_row(const Preserved_trx_record &record,
                                     std::string *temp_manifest_payload) {
  Preserved_trx_view_row row;
  const Preserve_snapshot_metadata &metadata = record.metadata;

  row.token = metadata.token;
  row.user = metadata.owner_user;
  row.host = metadata.owner_host;
  row.owner_user = metadata.owner_user;
  row.owner_host = metadata.owner_host;
  row.state = preserved_trx_lifecycle_state_name(record.state);
  row.created_at = format_timestamp_us(metadata.created_at_us);
  row.expires_at = format_timestamp_us(metadata.expires_at_us);
  row.recovered_count = metadata.recovered_count;
  if (metadata.created_at_us != 0) {
    const uint64_t now_us = my_micro_time();
    row.age_seconds =
        now_us >= metadata.created_at_us
            ? (now_us - metadata.created_at_us) / kMicrosecondsPerSecond
            : 0;
  }
  row.schema_name = metadata.schema_name;
  row.isolation = metadata.tx_isolation <= ISO_SERIALIZABLE
                      ? tx_isolation_names[metadata.tx_isolation]
                      : "UNKNOWN";
  row.mod_tables_count = metadata.mod_tables_count;
  row.has_read_view = metadata.has_read_view;
  row.rv_low_limit_no = metadata.rv_low_limit_no;
  row.savepoint_count = metadata.savepoint_count;
  (void)preserved_trx_populate_row_locks_count(metadata, &row);
  row.binlog_state = binlog_state_name(metadata.binlog_state);
  row.wrote_to_cache = metadata.wrote_to_cache;
  row.binlog_cache_size = metadata.binlog_cache_size;
  row.binlog_warmcopy_state = binlog_warmcopy_state_name(metadata);
  row.session_sql_log_bin = metadata.session_sql_log_bin;
  row.global_log_bin = metadata.global_log_bin;
  row.gtid_next = metadata.binlog_gtid_next;
  row.autoinc_lock_owned = metadata.autoinc_lock_owned;
  if (temp_manifest_payload != nullptr) {
    *temp_manifest_payload = metadata.temp_table_manifest_payload;
  }
  row.last_error = record.last_error;
  row.last_error_at = format_timestamp_us(record.last_error_at_us);

  return row;
}

bool preserved_trx_record_exists_locked(const std::string &token) {
  for (const Preserved_trx_record &record : g_preserved_trx_records) {
    if (!record.observable_only && record.metadata.token == token) return true;
  }
  return false;
}

bool preserved_trx_find_record(const std::string &token,
                               Preserved_trx_record *record) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (const Preserved_trx_record &candidate : g_preserved_trx_records) {
    if (!candidate.observable_only && candidate.metadata.token == token) {
      if (record != nullptr) *record = candidate;
      return true;
    }
  }
  return false;
}

bool preserved_trx_take_record(const std::string &token,
                               Preserved_trx_record *record) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (auto it = g_preserved_trx_records.begin();
       it != g_preserved_trx_records.end(); ++it) {
    if (!it->observable_only && it->metadata.token == token) {
      if (record != nullptr) *record = *it;
      g_preserved_trx_records.erase(it);
      return true;
    }
  }
  return false;
}

bool preserved_trx_take_resumable_record(const std::string &token,
                                         Preserved_trx_record *record) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (auto it = g_preserved_trx_records.begin();
       it != g_preserved_trx_records.end(); ++it) {
    if (!it->observable_only && it->metadata.token == token &&
        it->resumable) {
      if (record != nullptr) *record = *it;
      g_preserved_trx_records.erase(it);
      return true;
    }
  }
  return false;
}

bool preserved_trx_take_promotion_adopted_record(
    const Preserve_trx_prepared_token_key &key, Preserved_trx_record *record) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (auto it = g_preserved_trx_records.begin();
       it != g_preserved_trx_records.end(); ++it) {
    if (!it->observable_only && it->metadata.token == key.token &&
        it->state == Preserved_trx_lifecycle_state::ADOPTED_FOR_PROMOTION &&
        !it->resumable && it->has_promotion_key &&
        preserved_trx_promotion_keys_match(it->promotion_key, key)) {
      if (record != nullptr) *record = *it;
      g_preserved_trx_records.erase(it);
      return true;
    }
  }
  return false;
}

static bool preserved_trx_has_non_observable_records() {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (const Preserved_trx_record &record : g_preserved_trx_records) {
    if (!record.observable_only) return true;
  }
  return false;
}

static bool preserved_trx_has_any_records() {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  return !g_preserved_trx_records.empty();
}

static void preserved_trx_clear_cleanup_failed_if_no_records() {
  if (preserved_trx_has_any_records()) return;
  (void)preserve_trx_compare_exchange_manager_state_owner(
      Preserve_trx_manager_state::DRAIN_CLEANUP_FAILED,
      Preserve_trx_manager_state::IDLE, 0);
}

static void preserve_trx_set_record_error(Preserved_trx_record *record,
                                         const std::string &error) {
  if (record == nullptr) return;
  record->last_error = error;
  record->last_error_at_us = my_micro_time();
  record->last_error_monotonic_us = preserve_trx_monotonic_us();
  record->state = Preserved_trx_lifecycle_state::FAILED;
  record->resumable = false;
  preserved_trx_initialize_observable_gc_deadline(
      record, record->last_error_monotonic_us);
}

static void preserve_trx_set_record_last_error(Preserved_trx_record *record,
                                               const std::string &error) {
  if (record == nullptr) return;
  record->last_error = error;
  record->last_error_at_us = my_micro_time();
  record->last_error_monotonic_us = preserve_trx_monotonic_us();
}

static bool preserved_trx_update_record_error(const std::string &token,
                                            const std::string &error) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (Preserved_trx_record &record : g_preserved_trx_records) {
    if (!record.observable_only && record.metadata.token == token) {
      preserve_trx_set_record_error(&record, error);
      return true;
    }
  }
  return false;
}

static bool preserved_trx_update_record_last_error(const std::string &token,
                                                   const std::string &error) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (Preserved_trx_record &record : g_preserved_trx_records) {
    if (!record.observable_only && record.metadata.token == token) {
      preserve_trx_set_record_last_error(&record, error);
      return true;
    }
  }
  return false;
}

static bool preserved_trx_mark_preserved_with_last_error(
    const std::string &token, const std::string &error) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (Preserved_trx_record &record : g_preserved_trx_records) {
    if (!record.observable_only && record.metadata.token == token) {
      record.state = Preserved_trx_lifecycle_state::PRESERVED;
      record.resumable = true;
      preserve_trx_set_record_last_error(&record, error);
      return true;
    }
  }
  return false;
}

bool preserved_trx_add_record(const Preserve_snapshot_metadata &metadata,
                              trx_t *trx, bool resumable = true,
                              Preserved_trx_lifecycle_state state =
                                  Preserved_trx_lifecycle_state::PRESERVED,
                              std::vector<Preserved_trx_external_blob_descriptor>
                                  blob_descriptors = {},
                              const Preserve_trx_prepared_token_key
                                  *promotion_key = nullptr) {
  DBUG_EXECUTE_IF("preserve_trx_fail_add_record", return true;);

  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  if (preserved_trx_record_exists_locked(metadata.token)) return true;
  Preserved_trx_record record;
  record.metadata = metadata;
  record.trx = trx;
  record.resumable = resumable;
  record.state = state;
  record.blob_descriptors = std::move(blob_descriptors);
  if (promotion_key != nullptr) {
    if (promotion_key->token != metadata.token ||
        promotion_key->preserve_dir.empty() ||
        promotion_key->epoch_scope.empty() || promotion_key->epoch_id.empty() ||
        promotion_key->target_boot_incarnation.empty() ||
        promotion_key->generation == 0) {
      return true;
    }
    record.has_promotion_key = true;
    record.promotion_key = *promotion_key;
  }
  preserved_trx_initialize_record_deadlines(&record);
  preserved_trx_initialize_observable_gc_deadline(&record);
  g_preserved_trx_records.push_back(std::move(record));
  return false;
}

bool preserved_trx_add_record_with_error(Preserved_trx_record record,
                                         const std::string &error) {
  preserve_trx_set_record_error(&record, error);

  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  if (preserved_trx_record_exists_locked(record.metadata.token)) return true;
  preserved_trx_initialize_record_deadlines(&record);
  preserved_trx_initialize_observable_gc_deadline(&record);
  g_preserved_trx_records.push_back(record);
  return false;
}

bool preserved_trx_add_record_with_last_error(Preserved_trx_record record,
                                              const std::string &error) {
  preserve_trx_set_record_last_error(&record, error);

  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (Preserved_trx_record &candidate : g_preserved_trx_records) {
    if (!candidate.observable_only &&
        candidate.metadata.token == record.metadata.token) {
      preserve_trx_set_record_last_error(&candidate, error);
      return false;
    }
  }
  preserved_trx_initialize_record_deadlines(&record);
  preserved_trx_initialize_observable_gc_deadline(&record);
  g_preserved_trx_records.push_back(std::move(record));
  return false;
}

void preserved_trx_add_observable_record(
    const Preserve_snapshot_metadata &metadata, trx_t *trx,
    Preserved_trx_lifecycle_state state) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  Preserved_trx_record record;
  record.metadata = metadata;
  record.trx = trx;
  record.resumable = false;
  record.state = state;
  record.observable_only = true;
  preserved_trx_initialize_record_deadlines(&record);
  preserved_trx_initialize_observable_gc_deadline(&record);
  g_preserved_trx_records.push_back(std::move(record));
}

static void preserved_trx_add_failed_observable_record(
    const Preserve_snapshot_metadata &metadata, const std::string &error) {
  Preserved_trx_record record;
  record.metadata = metadata;
  record.trx = nullptr;
  record.resumable = false;
  record.observable_only = true;
  preserved_trx_initialize_record_deadlines(&record);
  preserve_trx_set_record_error(&record, error);

  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (const Preserved_trx_record &candidate : g_preserved_trx_records) {
    if (candidate.metadata.token == metadata.token &&
        candidate.observable_only &&
        candidate.state == Preserved_trx_lifecycle_state::FAILED) {
      return;
    }
  }
  g_preserved_trx_records.push_back(record);
}

static void preserved_trx_add_resume_detach_failure_observable_record(
    const Preserved_trx_record &source, const std::string &error) {
  Preserved_trx_record record = source;
  record.trx = nullptr;
  record.resumable = false;
  record.observable_only = true;
  record.state = Preserved_trx_lifecycle_state::FAILED;
  preserved_trx_initialize_record_deadlines(&record);
  preserve_trx_set_record_error(&record, error);

  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (const Preserved_trx_record &candidate : g_preserved_trx_records) {
    if (candidate.metadata.token == source.metadata.token &&
        candidate.observable_only &&
        candidate.state == Preserved_trx_lifecycle_state::FAILED) {
      return;
    }
  }
  g_preserved_trx_records.push_back(std::move(record));
}

static void preserved_trx_add_observable_error_record(
    const Preserve_snapshot_metadata &metadata,
    Preserved_trx_lifecycle_state state, const std::string &error) {
  Preserved_trx_record record;
  record.metadata = metadata;
  record.trx = nullptr;
  record.resumable = false;
  record.observable_only = true;
  preserved_trx_initialize_record_deadlines(&record);
  preserve_trx_set_record_error(&record, error);
  record.state = state;
  preserved_trx_initialize_observable_gc_deadline(&record);

  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (const Preserved_trx_record &candidate : g_preserved_trx_records) {
    if (candidate.metadata.token == metadata.token &&
        candidate.observable_only && candidate.state == state) {
      return;
    }
  }
  g_preserved_trx_records.push_back(std::move(record));
}

void preserved_trx_remove_observable_record(
    const std::string &token, Preserved_trx_lifecycle_state state) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (auto it = g_preserved_trx_records.begin();
       it != g_preserved_trx_records.end();) {
    if (it->observable_only && it->metadata.token == token &&
        it->state == state) {
      it = g_preserved_trx_records.erase(it);
    } else {
      ++it;
    }
  }
}

void mark_preserved_trx_recovery_complete() {
  {
    std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
    g_preserved_trx_recovery_deferred = false;
    g_preserved_trx_recovery_done = true;
  }
  g_preserved_trx_recovery_cond.notify_all();
  if (!preserved_trx_skip_local_startup_recovery())
    preserved_trx_start_expired_reaper_if_ready();
}

void mark_preserved_trx_recovery_deferred() {
  {
    std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
    g_preserved_trx_recovery_deferred = true;
    g_preserved_trx_recovery_done = true;
  }
  g_preserved_trx_recovery_cond.notify_all();
}

void preserved_trx_wait_recovery_complete() {
  std::unique_lock<std::mutex> lock(g_preserved_trx_mutex);
  g_preserved_trx_recovery_cond.wait(
      lock, [] { return g_preserved_trx_recovery_done; });
}

std::string normalize_dir(std::string dir) {
  if (dir.empty()) return dir;
  if (dir.back() != FN_LIBCHAR
#ifdef _WIN32
      && dir.back() != FN_LIBCHAR2
#endif
  ) {
    dir.push_back(FN_LIBCHAR);
  }
  return dir;
}

std::string preserve_trx_default_dir() {
  const char *datadir =
      mysql_real_data_home_ptr != nullptr ? mysql_real_data_home_ptr
                                          : mysql_real_data_home;
  return normalize_dir(normalize_dir(std::string(datadir)) + "preserve");
}

bool token_is_filename_safe(const std::string &token) {
  if (token.empty() || token.length() > PRESERVE_TRX_TOKEN_MAX_LENGTH)
    return false;
  return std::all_of(token.begin(), token.end(), [](unsigned char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= 'a' && ch <= 'z') || ch == '_' || ch == '-';
  });
}

using Preserved_trx_recovery_attempt_ledger = std::map<std::string, uint32_t>;

static std::string preserved_trx_recovery_attempt_ledger_path(
    const std::string &dir) {
  return normalize_dir(dir) + kPreservedTrxRecoveryAttemptLedgerFilename;
}

static bool parse_uint32_decimal(const std::string &text, uint32_t *value) {
  if (value == nullptr || text.empty()) return true;
  errno = 0;
  char *end = nullptr;
  const unsigned long long parsed =
      std::strtoull(text.c_str(), &end, 10);  // NOLINT(runtime/int)
  if (errno != 0 || end == nullptr || *end != '\0' ||
      parsed > std::numeric_limits<uint32_t>::max()) {
    return true;
  }
  *value = static_cast<uint32_t>(parsed);
  return false;
}

static bool preserved_trx_read_recovery_attempt_ledger(
    const std::string &dir, Preserved_trx_recovery_attempt_ledger *ledger) {
  if (ledger == nullptr) return true;
  ledger->clear();

  const std::string path = preserved_trx_recovery_attempt_ledger_path(dir);
  File file = my_open(path.c_str(), O_RDONLY | O_NOFOLLOW, MYF(0));
  if (file < 0) return my_errno() == ENOENT ? false : true;

  bool error = false;
  const my_off_t file_bytes = my_seek(file, 0, MY_SEEK_END, MYF(0));
  if (file_bytes == MY_FILEPOS_ERROR ||
      file_bytes > static_cast<my_off_t>(16 * 1024 * 1024) ||
      my_seek(file, 0, MY_SEEK_SET, MYF(0)) == MY_FILEPOS_ERROR) {
    error = true;
  }

  std::string payload;
  if (!error && file_bytes > 0) {
    payload.resize(static_cast<size_t>(file_bytes));
    const size_t read_len =
        my_read(file, reinterpret_cast<unsigned char *>(&payload[0]),
                payload.size(), MYF(0));
    error = read_len != payload.size();
  }
  if (my_close(file, MYF(0))) error = true;
  if (error) return true;
  if (payload.empty()) return true;

  std::istringstream input(payload);
  std::string line;
  if (!std::getline(input, line) ||
      line != kPreservedTrxRecoveryAttemptLedgerMagic) {
    return true;
  }
  while (std::getline(input, line)) {
    if (line.empty()) continue;
    const size_t sep = line.find(' ');
    if (sep == std::string::npos || sep == 0 || sep + 1 >= line.length())
      return true;
    const std::string token = line.substr(0, sep);
    if (!token_is_filename_safe(token)) return true;
    uint32_t count = 0;
    if (parse_uint32_decimal(line.substr(sep + 1), &count)) return true;
    (*ledger)[token] = count;
  }
  return false;
}

static bool preserved_trx_write_all(File file, const std::string &payload) {
  return payload.empty() ||
         my_write(file, reinterpret_cast<const unsigned char *>(payload.data()),
                  payload.size(), MYF(0)) == payload.size();
}

static bool preserved_trx_write_recovery_attempt_ledger(
    const std::string &dir,
    const Preserved_trx_recovery_attempt_ledger &ledger) {
  DBUG_EXECUTE_IF("preserve_trx_fail_recovery_attempt_ledger_write",
                  return true;);

  const std::string path = preserved_trx_recovery_attempt_ledger_path(dir);
  const std::string tmp_path = path + ".tmp";
  (void)my_delete(tmp_path.c_str(), MYF(0));

  File file = my_create(tmp_path.c_str(), 0600,
                        O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, MYF(0));
  if (file < 0) return true;

  std::ostringstream payload;
  payload << kPreservedTrxRecoveryAttemptLedgerMagic << '\n';
  for (const auto &entry : ledger) {
    if (!token_is_filename_safe(entry.first)) {
      (void)my_close(file, MYF(0));
      (void)my_delete(tmp_path.c_str(), MYF(0));
      return true;
    }
    payload << entry.first << ' ' << entry.second << '\n';
  }

  bool error = !preserved_trx_write_all(file, payload.str());
  if (!error && my_sync(file, MYF(0))) error = true;
  if (my_close(file, MYF(0))) error = true;
  if (!error && my_rename(tmp_path.c_str(), path.c_str(), MYF(0))) {
    error = true;
  }
  if (!error &&
      preserve_trx_fsync_default_store_directory(dir) !=
          Preserve_snapshot_status::OK) {
    error = true;
  }
  if (error) (void)my_delete(tmp_path.c_str(), MYF(0));
  return error;
}

static bool preserve_trx_build_resurrection_index_entry(
    const std::string &authority_token,
    const trx_preserve_resurrection_facts &facts,
    Preserve_trx_resurrection_index_entry *entry) {
  if (entry == nullptr || authority_token.empty() || facts.trx_id == 0 ||
      facts.freeze_lsn == 0 || facts.authority_token.empty()) {
    return true;
  }
  Preserve_trx_resurrection_index_entry built;
  built.authority_token = authority_token;
  built.trx_id = facts.trx_id;
  built.freeze_lsn = facts.freeze_lsn;
  built.modified_table_ids = facts.modified_table_ids;
  built.undo_anchors.reserve(facts.undo_anchors.size());
  for (const trx_preserve_resurrection_undo_anchor &anchor :
       facts.undo_anchors) {
    built.undo_anchors.push_back(
        {anchor.kind == trx_preserve_resurrection_undo_kind::INSERT
             ? Preserve_trx_resurrection_undo_kind::INSERT
             : Preserve_trx_resurrection_undo_kind::UPDATE,
         anchor.rseg_space_id, anchor.undo_slot, anchor.hdr_page_no,
         anchor.hdr_offset, anchor.top_page_no, anchor.top_offset,
         anchor.top_undo_no, anchor.empty});
  }
  *entry = std::move(built);
  return false;
}

static bool preserve_trx_resurrection_entry_to_engine_facts(
    const Preserve_trx_resurrection_index_entry &entry,
    trx_preserve_resurrection_facts *facts) {
  if (facts == nullptr || entry.authority_token.empty() || entry.trx_id == 0 ||
      entry.freeze_lsn == 0) {
    return true;
  }
  trx_preserve_resurrection_facts converted;
  converted.authority_token = entry.authority_token;
  converted.trx_id = entry.trx_id;
  converted.freeze_lsn = entry.freeze_lsn;
  converted.modified_table_ids = entry.modified_table_ids;
  converted.undo_anchors.reserve(entry.undo_anchors.size());
  for (const Preserve_trx_resurrection_undo_anchor &anchor :
       entry.undo_anchors) {
    converted.undo_anchors.push_back(
        {anchor.kind == Preserve_trx_resurrection_undo_kind::INSERT
             ? trx_preserve_resurrection_undo_kind::INSERT
             : trx_preserve_resurrection_undo_kind::UPDATE,
         anchor.rseg_space_id, anchor.undo_slot, anchor.hdr_page_no,
         anchor.hdr_offset, anchor.top_page_no, anchor.top_offset,
         anchor.top_undo_no, anchor.empty});
  }
  *facts = std::move(converted);
  return false;
}

static bool preserve_trx_register_resurrection_candidate(
    const Preserve_trx_resurrection_index_entry &entry,
    const std::string &expected_token,
    trx_preserve_resurrection_facts *registered_facts = nullptr) {
  trx_preserve_resurrection_facts facts;
  if (preserve_trx_resurrection_entry_to_engine_facts(entry, &facts) ||
      facts.authority_token != expected_token ||
      trx_preserve_startup_register_resurrection_candidate(facts) !=
          DB_SUCCESS) {
    return true;
  }
  if (registered_facts != nullptr) *registered_facts = std::move(facts);
  return false;
}

static bool preserve_trx_resurrection_metadata_is_strict(
    const Preserve_snapshot_metadata &metadata, uint64_t expected_trx_id) {
  const bool read_view_is_valid =
      metadata.has_read_view
          ? !metadata.read_view_payload.empty() &&
                trx_preserve_read_view_payload_semantics_are_valid(
                    metadata.read_view_payload, metadata.rv_low_limit_no,
                    expected_trx_id)
          : metadata.read_view_payload.empty() &&
                metadata.rv_low_limit_no == 0;
  return metadata.engine_shape ==
             Preserve_snapshot_engine_shape::PERSISTENT_ONLY &&
         metadata.has_persistent_engine_state &&
         !metadata.has_temp_engine_state &&
         metadata.temp_table_manifest_payload.empty() &&
         read_view_is_valid &&
         metadata.predicate_locks_payload.empty() &&
         preserve_snapshot_gtid_state_is_strict_transfer_safe(metadata);
}

static bool preserve_trx_resurrection_metadata_supports_local_startup_index(
    const Preserve_snapshot_metadata &metadata) {
  if (!metadata.has_persistent_engine_state) return false;
  if (metadata.engine_shape ==
      Preserve_snapshot_engine_shape::PERSISTENT_ONLY) {
    return !metadata.has_temp_engine_state &&
           metadata.temp_table_manifest_payload.empty();
  }
  return metadata.engine_shape == Preserve_snapshot_engine_shape::MIXED &&
         metadata.has_temp_engine_state &&
         !metadata.temp_table_manifest_payload.empty();
}

static bool preserve_trx_write_local_resurrection_index(
    const std::string &token,
    const std::array<unsigned char, kPreservedTrxSha256Length>
        &snapshot_payload_digest,
    Preserve_trx_resurrection_index_entry entry, Preserved_trx_store *store) {
  Preserved_trx_codec_context context;
  Preserve_trx_resurrection_index index;
  index.epoch_id = "local-" + token;
  const bool input_error =
      store == nullptr ||
      store->codec_context(
          &context, Preserved_trx_codec_context_purpose::READ_EXISTING) !=
          Preserve_snapshot_status::OK ||
      context.server_uuid.empty();
  Preserve_trx_resurrection_index_status encode_status =
      Preserve_trx_resurrection_index_status::INVALID_ARGUMENT;
  std::string encoded;
  if (!input_error) {
    entry.snapshot_digest = snapshot_payload_digest;
    index.local_instance_identity = context.server_uuid;
    index.entries.push_back(std::move(entry));
    encode_status =
        preserve_trx_encode_resurrection_index(index, context, &encoded);
  }
  Preserve_snapshot_status status = Preserve_snapshot_status::INVALID_ARGUMENT;
  if (encode_status == Preserve_trx_resurrection_index_status::OK) {
    const std::vector<unsigned char> bytes(encoded.begin(), encoded.end());
    status = store->write_resurrection_index_new(token, bytes);
  }
  if (status != Preserve_snapshot_status::OK) {
    const std::string message =
        "PRESERVE: local startup Resurrection Index unavailable; native Undo "
        "scan remains authoritative token=" +
        token;
    LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return true;
  }
  return false;
}

static bool preserved_trx_publish_recovery_attempt_ledger(
    const std::string &dir, const std::set<std::string> &snapshot_tokens,
    Preserved_trx_recovery_attempt_ledger *ledger) {
  if (ledger == nullptr) return true;

  Preserved_trx_recovery_attempt_ledger existing;
  if (preserved_trx_read_recovery_attempt_ledger(dir, &existing)) return true;

  ledger->clear();
  for (const std::string &token : snapshot_tokens) {
    if (!token_is_filename_safe(token)) return true;
    const uint32_t previous =
        existing.find(token) == existing.end() ? 0 : existing[token];
    if (previous == std::numeric_limits<uint32_t>::max()) return true;
    (*ledger)[token] = previous + 1;
  }
  return preserved_trx_write_recovery_attempt_ledger(dir, *ledger);
}

std::string preserved_trx_tainted_reason(const std::string &dir,
                                         const std::string &token) {
  constexpr size_t kMaxTaintedReasonBytes = 256;
  const std::string fallback("preserved transaction snapshot tainted");
  if (!token_is_filename_safe(token)) return fallback;

  const std::string path = normalize_dir(dir) + token + ".tainted";
  File file = my_open(path.c_str(), O_RDONLY | O_NOFOLLOW, MYF(0));
  if (file < 0) return fallback;

  unsigned char buffer[kMaxTaintedReasonBytes + 1]{};
  const size_t read_len =
      my_read(file, buffer, kMaxTaintedReasonBytes, MYF(0));
  const bool close_failed = my_close(file, MYF(0)) != 0;
  if (read_len == MY_FILE_ERROR || close_failed) return fallback;

  std::string reason(reinterpret_cast<char *>(buffer), read_len);
  while (!reason.empty() &&
         (reason.back() == '\n' || reason.back() == '\r' ||
          reason.back() == '\0')) {
    reason.pop_back();
  }
  for (char &ch : reason) {
    const unsigned char value = static_cast<unsigned char>(ch);
    if (value < 0x20 || value > 0x7e) ch = '_';
  }
  if (reason.empty()) return fallback;
  return fallback + ": " + reason;
}

bool preserve_trx_token_to_xid(const std::string &token, XID *xid) {
  if (xid == nullptr || !token_is_filename_safe(token) ||
      token.length() >
          XIDDATASIZE - static_cast<size_t>(PRESERVE_TRX_XID_GTRID_LENGTH)) {
    return true;
  }

  xid->set(PRESERVE_TRX_XID_FORMAT_ID, PRESERVE_TRX_XID_GTRID,
           PRESERVE_TRX_XID_GTRID_LENGTH, token.c_str(),
           static_cast<long>(token.length()));
  return false;
}

bool preserved_trx_file_exists_for_token(const std::string &token) {
  return preserved_trx_default_carrier_generated_token_exists(
      preserve_trx_default_dir(), token);
}

static bool preserve_trx_token_collides(const std::string &token) {
  {
    std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
    if (preserved_trx_record_exists_locked(token)) return true;
  }
  return preserved_trx_file_exists_for_token(token);
}

bool generate_preserve_trx_token(std::string *token) {
  static constexpr size_t kRandomTokenBytes = 16;
  static constexpr char kHexDigits[] = "0123456789abcdef";

  if (token == nullptr) return true;

  std::array<unsigned char, kRandomTokenBytes> random_bytes;
  for (int attempt = 0; attempt < 32; ++attempt) {
    if (my_rand_buffer(random_bytes.data(), random_bytes.size())) return true;

    std::string candidate;
    candidate.resize(kRandomTokenBytes * 2);
    for (size_t i = 0; i < random_bytes.size(); ++i) {
      candidate[i * 2] = kHexDigits[random_bytes[i] >> 4];
      candidate[i * 2 + 1] = kHexDigits[random_bytes[i] & 0x0f];
    }

    if (preserve_trx_token_collides(candidate)) continue;

    *token = std::move(candidate);
    return false;
  }

  return true;
}

struct Preserve_trx_token_selection {
  std::string preserve_token_string;
  uint64_t transfer_token{0};
  bool is_transfer_token{false};
  const char *failure_reason{nullptr};
};

static bool preserve_trx_select_token_for_request(
    THD *target_thd, Preserve_trx_transfer_artifact_decision artifact_decision,
    const std::string &preselected_token,
    Preserve_trx_token_selection *selection) {
  if (selection == nullptr) return true;
  *selection = Preserve_trx_token_selection{};

  if (!preselected_token.empty()) {
    if (!token_is_filename_safe(preselected_token)) {
      selection->failure_reason = "batch_preselected_token_not_reserved";
      return true;
    }
    if (artifact_decision ==
        Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE) {
      if (target_thd == nullptr || target_thd->thread_id() == 0 ||
          preselected_token != std::to_string(target_thd->thread_id())) {
        selection->failure_reason = "batch_preselected_transfer_token_mismatch";
        return true;
      }
      selection->transfer_token =
          static_cast<uint64_t>(target_thd->thread_id());
      selection->is_transfer_token = true;
    }
    selection->preserve_token_string = preselected_token;
    return false;
  }

  if (artifact_decision !=
      Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE) {
    if (generate_preserve_trx_token(&selection->preserve_token_string)) {
      selection->failure_reason = "token_generation_failed";
      return true;
    }
    return false;
  }

  if (target_thd == nullptr) {
    selection->failure_reason = "standby_transfer_missing_target_thd";
    return true;
  }

  const uint64_t transfer_token =
      static_cast<uint64_t>(target_thd->thread_id());
  if (transfer_token == 0) {
    selection->failure_reason = "standby_transfer_invalid_token";
    return true;
  }

  const std::string token_string = std::to_string(transfer_token);
  if (!token_is_filename_safe(token_string)) {
    selection->failure_reason = "standby_transfer_invalid_token";
    return true;
  }

  if (preserve_trx_token_collides(token_string)) {
    selection->failure_reason = "standby_transfer_token_collision";
    return true;
  }

  selection->preserve_token_string = token_string;
  selection->transfer_token = transfer_token;
  selection->is_transfer_token = true;
  return false;
}

static bool preserve_trx_select_batch_tokens(
    const std::vector<my_thread_id> &target_thread_ids,
    Preserve_trx_transfer_artifact_decision artifact_decision,
    std::map<my_thread_id, std::string> *tokens_by_thread_id,
    std::vector<std::string> *tokens) {
  if (tokens_by_thread_id == nullptr || tokens == nullptr) return true;
  tokens_by_thread_id->clear();
  tokens->clear();
  tokens->reserve(target_thread_ids.size());
  for (const my_thread_id target_thread_id : target_thread_ids) {
    std::string token;
    if (artifact_decision ==
        Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE) {
      if (target_thread_id == 0) return true;
      token = std::to_string(target_thread_id);
      if (!token_is_filename_safe(token) || preserve_trx_token_collides(token))
        return true;
    } else if (generate_preserve_trx_token(&token)) {
      return true;
    }
    if (!tokens_by_thread_id->emplace(target_thread_id, token).second)
      return true;
    tokens->push_back(std::move(token));
  }
  return false;
}

void append_le64(std::string *bytes, uint64_t value) {
  for (size_t i = 0; i < 8; ++i) {
    bytes->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

void append_le32(std::string *bytes, uint32_t value) {
  for (size_t i = 0; i < 4; ++i) {
    bytes->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

void append_le16(std::string *bytes, uint16_t value) {
  bytes->push_back(static_cast<char>(value & 0xff));
  bytes->push_back(static_cast<char>((value >> 8) & 0xff));
}

bool preserve_user_var_type_supported(Item_result type) {
  switch (type) {
    case STRING_RESULT:
    case REAL_RESULT:
    case INT_RESULT:
    case DECIMAL_RESULT:
      return true;
    case INVALID_RESULT:
    case ROW_RESULT:
      return false;
  }
  return false;
}

bool preserve_user_var_derivation_supported(uint8_t derivation) {
  return derivation <= DERIVATION_IGNORABLE;
}

bool decimal_user_var_value_is_valid(const std::string &value) {
  if (value.empty() || value.length() > DECIMAL_MAX_STR_LENGTH) return false;

  my_decimal decimal;
  return str2my_decimal(0, value.data(), value.length(), &my_charset_bin,
                        &decimal) == E_DEC_OK;
}

bool parse_user_vars_payload(
    const std::string &payload,
    std::vector<Preserve_user_var_snapshot_entry> *entries) {
  if (entries == nullptr || payload.length() < kUserVariablesHeaderLength)
    return true;

  const uint16_t version = read_le16(payload, 0);
  if (version < kMinReadableUserVariablesVersion ||
      version > kUserVariablesVersion)
    return true;

  const uint32_t count = read_le32(payload, 2);
  size_t offset = kUserVariablesHeaderLength;
  const size_t max_entry_count =
      (payload.length() - kUserVariablesHeaderLength) /
      kUserVariableEntryFixedLength;
  if (count > max_entry_count) return true;
  entries->clear();
  entries->reserve(count);
  std::set<std::string> names;

  for (uint32_t i = 0; i < count; ++i) {
    if (payload.length() - offset < kUserVariableEntryFixedLength) return true;

    const uint16_t name_length = read_le16(payload, offset);
    offset += 2;
    const Item_result type =
        static_cast<Item_result>(static_cast<int8_t>(payload[offset++]));
    const uint16_t charset_number = read_le16(payload, offset);
    offset += 2;
    const uint8_t derivation = static_cast<uint8_t>(payload[offset++]);
    const uint8_t unsigned_flag = static_cast<uint8_t>(payload[offset++]);
    const uint8_t is_null = static_cast<uint8_t>(payload[offset++]);
    const uint32_t value_length = read_le32(payload, offset);
    offset += 4;

    if (!preserve_user_var_type_supported(type) ||
        !preserve_user_var_derivation_supported(derivation) ||
        unsigned_flag > 1 || is_null > 1)
      return true;
    if (payload.length() - offset < name_length ||
        payload.length() - offset - name_length < value_length)
      return true;

    Preserve_user_var_snapshot_entry entry;
    entry.name.assign(payload.data() + offset, name_length);
    offset += name_length;
    entry.type = type;
    entry.charset_number = charset_number;
    entry.derivation = static_cast<Derivation>(derivation);
    entry.unsigned_flag = unsigned_flag != 0;
    entry.is_null = is_null != 0;
    entry.value.assign(payload.data() + offset, value_length);
    offset += value_length;

    if (entry.name.empty() || entry.name.length() > NAME_CHAR_LEN ||
        get_charset(entry.charset_number, MYF(0)) == nullptr)
      return true;
    if (!names.insert(entry.name).second) return true;
    if (entry.is_null && !entry.value.empty()) return true;
    if (!entry.is_null) {
      if ((entry.type == REAL_RESULT && entry.value.length() != sizeof(double)) ||
          (entry.type == INT_RESULT &&
           entry.value.length() != sizeof(longlong)))
        return true;
      if (entry.type == DECIMAL_RESULT &&
          (version < 2 || !decimal_user_var_value_is_valid(entry.value)))
        return true;
    }

    entries->push_back(std::move(entry));
  }

  return offset != payload.length();
}

bool serialize_user_var_value(user_var_entry *entry, bool is_null,
                              std::string *value) {
  if (entry == nullptr || value == nullptr) return true;

  value->clear();
  if (is_null) return false;

  if (entry->type() == DECIMAL_RESULT) {
    const my_decimal *decimal = static_cast<const my_decimal *>(
        static_cast<const void *>(entry->ptr()));
    decimal->sanity_check();
    char buffer[DECIMAL_MAX_STR_LENGTH + 1];
    String decimal_string(buffer, sizeof(buffer), &my_charset_bin);
    if (my_decimal2string(E_DEC_FATAL_ERROR, decimal, &decimal_string))
      return true;
    value->assign(decimal_string.ptr(), decimal_string.length());
    return false;
  }

  value->assign(entry->ptr(), entry->length());
  return false;
}

bool export_user_vars_payload(THD *thd, std::string *payload) {
  if (thd == nullptr || payload == nullptr) return true;

  payload->clear();
  mysql_mutex_lock(&thd->LOCK_thd_data);

  bool error = false;
  std::string out;
  append_le16(&out, kUserVariablesVersion);
  if (thd->user_vars.size() > UINT32_MAX) {
    error = true;
  } else {
    append_le32(&out, static_cast<uint32_t>(thd->user_vars.size()));
  }

  if (!error) {
    for (const auto &key_and_value : thd->user_vars) {
      user_var_entry *entry = key_and_value.second.get();
      std::string value;
      const bool is_null = entry == nullptr || entry->ptr() == nullptr;
      if (entry == nullptr || entry->collation.collation == nullptr ||
          entry->entry_name.length() > UINT16_MAX ||
          entry->collation.collation->number > UINT16_MAX ||
          entry->collation.derivation > DERIVATION_IGNORABLE ||
          !preserve_user_var_type_supported(entry->type()) ||
          serialize_user_var_value(entry, is_null, &value) ||
          value.length() > UINT32_MAX) {
        error = true;
        break;
      }

      append_le16(&out, static_cast<uint16_t>(entry->entry_name.length()));
      out.push_back(static_cast<char>(entry->type()));
      append_le16(&out,
                  static_cast<uint16_t>(entry->collation.collation->number));
      out.push_back(static_cast<char>(entry->collation.derivation));
      out.push_back(static_cast<char>(entry->unsigned_flag ? 1 : 0));
      out.push_back(static_cast<char>(is_null ? 1 : 0));
      append_le32(&out, static_cast<uint32_t>(value.length()));
      out.append(entry->entry_name.ptr(), entry->entry_name.length());
      out.append(value);
    }
  }

  mysql_mutex_unlock(&thd->LOCK_thd_data);
  if (error) return true;

  *payload = std::move(out);
  return false;
}

void preserve_trx_free_user_var(user_var_entry *entry) {
  entry->destroy();
}

bool apply_user_vars_payload(THD *thd, const std::string &payload,
                             bool clear_existing) {
  if (thd == nullptr) return true;

  std::vector<Preserve_user_var_snapshot_entry> entries;
  if (!payload.empty() && parse_user_vars_payload(payload, &entries))
    return true;

  mysql_mutex_lock(&thd->LOCK_thd_data);
  if (clear_existing) thd->user_vars.clear();

  bool error = false;
  for (const Preserve_user_var_snapshot_entry &entry : entries) {
    CHARSET_INFO *charset = get_charset(entry.charset_number, MYF(0));
    if (charset == nullptr) {
      error = true;
      break;
    }

    user_var_entry *user_var = find_or_nullptr(thd->user_vars, entry.name);
    if (user_var == nullptr) {
      const Name_string name(entry.name.c_str(), entry.name.length());
      user_var = user_var_entry::create(thd, name, charset);
      if (user_var == nullptr) {
        error = true;
        break;
      }
      thd->user_vars.emplace(
          entry.name, unique_ptr_with_deleter<user_var_entry>(
                          user_var, &preserve_trx_free_user_var));
    }

    if (entry.is_null) {
      user_var->set_null_value(entry.type);
      user_var->collation.set(charset, entry.derivation);
      user_var->unsigned_flag = entry.unsigned_flag;
    } else if (entry.type == DECIMAL_RESULT) {
      my_decimal decimal;
      if (str2my_decimal(0, entry.value.data(), entry.value.length(),
                         &my_charset_bin, &decimal) != E_DEC_OK ||
          user_var->store(&decimal, sizeof(decimal), DECIMAL_RESULT, charset,
                          entry.derivation, entry.unsigned_flag)) {
        error = true;
        break;
      }
    } else if (user_var->store(entry.value.data(), entry.value.length(),
                               entry.type, charset, entry.derivation,
                               entry.unsigned_flag)) {
      error = true;
      break;
    }
  }
  mysql_mutex_unlock(&thd->LOCK_thd_data);

  return error;
}

bool import_user_vars_payload(THD *thd, const std::string &payload) {
  if (payload.empty()) return false;
  return apply_user_vars_payload(thd, payload, true);
}

bool restore_user_vars_payload(THD *thd, const std::string &payload) {
  return apply_user_vars_payload(thd, payload, true);
}

uint16_t charset_number_or_zero(const CHARSET_INFO *charset) {
  if (charset == nullptr) return 0;
  assert(charset->number <= UINT16_MAX);
  return static_cast<uint16_t>(charset->number);
}

bool session_state_charset_numbers_are_valid(
    uint16_t character_set_client_number,
    uint16_t character_set_results_number,
    uint16_t collation_connection_number) {
  if (character_set_client_number == 0 || collation_connection_number == 0)
    return false;

  if (get_charset(character_set_client_number, MYF(0)) == nullptr ||
      get_charset(collation_connection_number, MYF(0)) == nullptr)
    return false;

  return character_set_results_number == 0 ||
         get_charset(character_set_results_number, MYF(0)) != nullptr;
}

bool no_cache_binlog_state(Preserve_snapshot_binlog_state state) {
  switch (state) {
    case Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE:
    case Preserve_snapshot_binlog_state::SESSION_OFF_NO_CACHE:
    case Preserve_snapshot_binlog_state::LOGGED_EMPTY:
      return true;
    case Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE:
      return false;
  }
  return false;
}

bool recoverable_binlog_state(Preserve_snapshot_binlog_state state) {
  return no_cache_binlog_state(state) ||
         state == Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE;
}

uint32_t binlog_savepoint_checkpoint_flags(
    const Mysql_binlog_preserve_cache_state &state) {
  uint32_t flags = 0;
  if (state.with_sbr) flags |= kBinlogCacheFlagWithSbr;
  if (state.with_rbr) flags |= kBinlogCacheFlagWithRbr;
  if (state.with_start) flags |= kBinlogCacheFlagWithStart;
  if (state.with_end) flags |= kBinlogCacheFlagWithEnd;
  if (state.with_content) flags |= kBinlogCacheFlagWithContent;
  return flags;
}

bool apply_binlog_savepoint_checkpoint_flags(
    uint32_t flags, Mysql_binlog_preserve_cache_state *state) {
  if (state == nullptr || (flags & ~kBinlogSavepointCheckpointKnownFlags) != 0)
    return true;
  state->with_sbr = (flags & kBinlogCacheFlagWithSbr) != 0;
  state->with_rbr = (flags & kBinlogCacheFlagWithRbr) != 0;
  state->with_start = (flags & kBinlogCacheFlagWithStart) != 0;
  state->with_end = (flags & kBinlogCacheFlagWithEnd) != 0;
  state->with_content = (flags & kBinlogCacheFlagWithContent) != 0;
  return false;
}

Mysql_binlog_preserve_snapshot metadata_to_binlog_cache_snapshot(
    const Preserve_snapshot_metadata &metadata) {
  Mysql_binlog_preserve_snapshot snapshot;
  snapshot.cache_payload = metadata.binlog_cache_payload;
  snapshot.event_counter = metadata.binlog_cache_event_counter;
  snapshot.immediate = metadata.binlog_cache_immediate;
  snapshot.with_xid = metadata.binlog_cache_with_xid;
  snapshot.with_sbr = metadata.binlog_cache_with_sbr;
  snapshot.with_rbr = metadata.binlog_cache_with_rbr;
  snapshot.with_start = metadata.binlog_cache_with_start;
  snapshot.with_end = metadata.binlog_cache_with_end;
  snapshot.with_content = metadata.binlog_cache_with_content;
  snapshot.has_prev_position = metadata.binlog_cache_has_prev_position;
  snapshot.prev_position = metadata.binlog_cache_prev_position;
  snapshot.gtid_next = metadata.binlog_gtid_next;
  snapshot.owned_gtid = metadata.binlog_owned_gtid;
  snapshot.has_compression_session_state =
      metadata.binlog_cache_has_compression_session_state;
  snapshot.binlog_trx_compression = metadata.binlog_cache_compression;
  snapshot.binlog_trx_compression_type =
      metadata.binlog_cache_compression_type;
  snapshot.binlog_trx_compression_level_zstd =
      metadata.binlog_cache_compression_level_zstd;
  return snapshot;
}

bool preserved_trx_blob_descriptors_equal(
    const std::vector<Preserved_trx_external_blob_descriptor> &lhs,
    const std::vector<Preserved_trx_external_blob_descriptor> &rhs) {
  if (lhs.size() != rhs.size()) return false;
  for (const Preserved_trx_external_blob_descriptor &left : lhs) {
    const auto right = std::find_if(
        rhs.begin(), rhs.end(),
        [&left](const Preserved_trx_external_blob_descriptor &candidate) {
          return candidate.name == left.name;
        });
    if (right == rhs.end() || right->size != left.size ||
        right->digest != left.digest) {
      return false;
    }
  }
  return true;
}

bool binlog_payload_memory_peak(uint64_t payload_bytes, uint64_t *peak_bytes) {
  constexpr uint64_t kSimultaneousPayloadCopies = 3;
  if (peak_bytes == nullptr || payload_bytes == 0 ||
      payload_bytes > UINT64_MAX / kSimultaneousPayloadCopies) {
    return false;
  }
  *peak_bytes = payload_bytes * kSimultaneousPayloadCopies;
  return true;
}

bool hydrate_logged_binlog_cache_payload_if_needed(Preserved_trx_record *record,
                                                   const std::string &token,
                                                   const Preserve_trx_source_rollback_image
                                                       *source_rollback_image =
                                                           nullptr,
                                                   Preserve_memory_lease
                                                       *payload_lease = nullptr,
                                                   bool allow_staged_external_blob =
                                                       false) {
  if (record == nullptr ||
      record->metadata.binlog_state !=
          Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE ||
      !record->metadata.binlog_cache_payload.empty()) {
    return false;
  }

  uint64_t payload_bytes = 0;
  if (source_rollback_image != nullptr) {
    payload_bytes =
        !source_rollback_image->binlog_snapshot.cache_payload.empty()
            ? source_rollback_image->binlog_snapshot.cache_payload.size()
            : source_rollback_image->prebuilt_binlog_blob.size;
  } else {
    const auto descriptor = std::find_if(
        record->blob_descriptors.begin(), record->blob_descriptors.end(),
        [](const Preserved_trx_external_blob_descriptor &candidate) {
          return candidate.name == kPreservedTrxBlobBinlogCache;
        });
    if (descriptor != record->blob_descriptors.end()) {
      payload_bytes = descriptor->size;
    }
  }
  uint64_t payload_peak_bytes = 0;
  if (payload_lease == nullptr ||
      !binlog_payload_memory_peak(payload_bytes, &payload_peak_bytes)) {
    return true;
  }
  Preserve_memory_lease acquired = preserve_trx_acquire_memory_lease(
      token, Preserve_trx_memory_kind::BINLOG_WARMCOPY_BUFFER,
      payload_peak_bytes);
  if (!acquired.acquired()) return true;
  *payload_lease = std::move(acquired);

  if (source_rollback_image != nullptr) {
    const Mysql_binlog_preserve_snapshot &source_snapshot =
        source_rollback_image->binlog_snapshot;
    std::string payload;
    if (!source_snapshot.cache_payload.empty()) {
      payload = source_snapshot.cache_payload;
    } else if (source_rollback_image->has_prebuilt_binlog_blob) {
      const PrebuiltBinlogCacheBlob &prebuilt =
          source_rollback_image->prebuilt_binlog_blob;
      Preserved_trx_external_blob_descriptor descriptor;
      descriptor.name = prebuilt.name;
      descriptor.size = prebuilt.size;
      descriptor.digest = prebuilt.digest;
      const std::string &rollback_dir = source_rollback_image->preserve_dir;
      auto carrier = create_preserved_trx_default_warm_external_blob_carrier(
          rollback_dir.empty() ? preserve_trx_default_dir() : rollback_dir);
      Preserved_trx_external_blob blob;
      if (carrier == nullptr ||
          carrier->read_warm_external_blob(
              prebuilt.warmcopy_id, prebuilt.name, prebuilt.warmcopy_epoch,
              descriptor, preserve_trx_max_binlog_cache_bytes, &blob) !=
              Preserved_trx_carrier_status::OK) {
        return true;
      }
      payload = std::move(blob.payload);
    } else {
      return true;
    }
    if (payload.empty() || payload.size() != record->metadata.binlog_cache_size ||
        (source_snapshot.has_cache_length &&
         source_snapshot.cache_length != payload.size()) ||
        source_snapshot.event_counter !=
            record->metadata.binlog_cache_event_counter) {
      return true;
    }
    record->metadata.binlog_cache_payload = std::move(payload);
    return false;
  }

  auto store = create_preserved_trx_default_store(preserve_trx_default_dir());
  Preserved_trx_bundle bundle;
  if (store->read(token, true,
                  Preserved_trx_carrier::Payload_read_mode::
                      WITH_EXTERNAL_BLOBS,
                  &bundle) != Preserve_snapshot_status::OK) {
    if (!allow_staged_external_blob || token != record->metadata.token) {
      return true;
    }
    const auto descriptor = std::find_if(
        record->blob_descriptors.begin(), record->blob_descriptors.end(),
        [](const Preserved_trx_external_blob_descriptor &candidate) {
          return candidate.name == kPreservedTrxBlobBinlogCache;
        });
    Preserved_trx_external_blob blob;
    if (descriptor == record->blob_descriptors.end() ||
        store->read_external_blob(token, *descriptor, &blob) !=
            Preserve_snapshot_status::OK ||
        blob.descriptor.name != descriptor->name ||
        blob.descriptor.size != descriptor->size ||
        blob.descriptor.digest != descriptor->digest || blob.payload.empty() ||
        blob.payload.size() != record->metadata.binlog_cache_size) {
      return true;
    }
    record->metadata.binlog_cache_payload = std::move(blob.payload);
    return false;
  }
  if (bundle.metadata.token != record->metadata.token ||
      !preserved_trx_blob_descriptors_equal(record->blob_descriptors,
                                            bundle.blob_descriptors)) {
    return true;
  }
  if (bundle.metadata.binlog_state !=
          Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE ||
      bundle.metadata.binlog_cache_size != record->metadata.binlog_cache_size ||
      bundle.metadata.binlog_cache_event_counter !=
          record->metadata.binlog_cache_event_counter ||
      bundle.metadata.binlog_cache_payload.empty()) {
    return true;
  }

  record->metadata.binlog_cache_payload =
      std::move(bundle.metadata.binlog_cache_payload);
  return false;
}

bool hydrate_source_rollback_image_for_unit_test_impl(
    const std::string &token, Preserve_snapshot_metadata *metadata,
    const Preserve_trx_source_rollback_image &source_rollback_image) {
  if (metadata == nullptr) return true;
  if (source_rollback_image.native_binlog_cache_retained) return false;
  Preserved_trx_record record;
  record.metadata = *metadata;
  Preserve_memory_lease payload_lease;
  if (hydrate_logged_binlog_cache_payload_if_needed(
          &record, token, &source_rollback_image, &payload_lease)) {
    return true;
  }
  *metadata = std::move(record.metadata);
  return false;
}

bool no_cache_gtid_state_is_clean(THD *thd) {
  if (thd == nullptr || thd->variables.gtid_next.type != AUTOMATIC_GTID) {
    return false;
  }
#ifdef HAVE_GTID_NEXT_LIST
  if (thd->get_gtid_next_list() != nullptr) {
    return false;
  }
#endif
  if (thd->owned_gtid.sidno == THD::OWNED_SIDNO_ANONYMOUS ||
      thd->owned_gtid.sidno == THD::OWNED_SIDNO_GTID_SET ||
      !thd->owned_gtid_is_empty()) {
    return false;
  }
  return true;
}

bool preserve_snapshot_allows_gtid_restore(
    const Preserve_snapshot_metadata &metadata) {
  return metadata.binlog_state ==
             Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE &&
         !metadata.binlog_gtid_next.empty();
}

bool restore_logged_cache_gtid_next(THD *thd,
                                    const Preserve_snapshot_metadata &metadata) {
  if (thd == nullptr || metadata.binlog_gtid_next.empty()) return true;

  Gtid_specification spec;
  global_sid_lock->rdlock();
  if (spec.parse(global_sid_map, metadata.binlog_gtid_next.c_str()) !=
      RETURN_STATUS_OK) {
    global_sid_lock->unlock();
    return true;
  }

  if (set_gtid_next(thd, spec)) return true;

  if (spec.type == ANONYMOUS_GTID) {
    gtid_state->update_on_rollback(thd);
    thd->variables.gtid_next.set_automatic();
    return true;
  }

  if (spec.type == ASSIGNED_GTID &&
      (thd->owned_gtid_is_empty() || !thd->owned_gtid.equals(spec.gtid))) {
    gtid_state->update_on_rollback(thd);
    thd->variables.gtid_next.set_automatic();
    return true;
  }

  return false;
}

void rollback_restored_logged_cache_gtid_next(THD *thd, bool *gtid_restored) {
  if (thd == nullptr || gtid_restored == nullptr || !*gtid_restored) return;

  if (!thd->owned_gtid_is_empty() ||
      thd->variables.gtid_next.type == ASSIGNED_GTID) {
    gtid_state->update_on_rollback(thd);
  }
  thd->variables.gtid_next.set_automatic();
  *gtid_restored = false;
}

bool determine_no_cache_binlog_state(
    THD *thd, Preserve_snapshot_binlog_state *binlog_state) {
  if (thd == nullptr || binlog_state == nullptr) return true;
  if (!no_cache_gtid_state_is_clean(thd)) return true;

  const bool binlog_open = mysql_bin_log.is_open();
  const bool session_sql_log_bin =
      (thd->variables.option_bits & OPTION_BIN_LOG) != 0;
  if (!binlog_open) {
    if (opt_bin_log) return true;
    *binlog_state = Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE;
    return false;
  }

  if (!mysql_binlog_preserve_no_cache_boundary_is_clean(thd)) return true;

  if (!session_sql_log_bin) {
    *binlog_state = Preserve_snapshot_binlog_state::SESSION_OFF_NO_CACHE;
    return false;
  }

  *binlog_state = Preserve_snapshot_binlog_state::LOGGED_EMPTY;
  return false;
}

bool binlog_state_matches_global_log_bin_mode(
    const Preserve_snapshot_metadata &metadata, bool current_global_log_bin) {
  if (metadata.global_log_bin != current_global_log_bin) return false;

  switch (metadata.binlog_state) {
    case Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE:
      return !current_global_log_bin;
    case Preserve_snapshot_binlog_state::SESSION_OFF_NO_CACHE:
      return current_global_log_bin && !metadata.option_bin_log &&
             !metadata.session_sql_log_bin;
    case Preserve_snapshot_binlog_state::LOGGED_EMPTY:
      return current_global_log_bin && metadata.option_bin_log &&
             metadata.session_sql_log_bin;
    case Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE:
      return current_global_log_bin && metadata.option_bin_log &&
             metadata.session_sql_log_bin;
  }
  return false;
}

bool binlog_gtid_mode_matches(const Preserve_snapshot_metadata &metadata,
                              Gtid_mode::value_type gtid_mode) {
  return !metadata.has_binlog_gtid_mode ||
         metadata.binlog_gtid_mode ==
             static_cast<uint8_t>(gtid_mode);
}

bool binlog_state_matches_global_mode(
    const Preserve_snapshot_metadata &metadata, bool current_global_log_bin,
    Gtid_mode::value_type gtid_mode) {
  return binlog_state_matches_global_log_bin_mode(metadata,
                                                 current_global_log_bin) &&
         binlog_gtid_mode_matches(metadata, gtid_mode);
}

bool binlog_state_matches_current_mode(
    const Preserve_snapshot_metadata &metadata) {
  return binlog_state_matches_global_mode(metadata, mysql_bin_log.is_open(),
                                         global_gtid_mode.get());
}

bool binlog_state_matches_configured_mode(
    const Preserve_snapshot_metadata &metadata) {
  return binlog_state_matches_global_mode(
      metadata, opt_bin_log,
      static_cast<Gtid_mode::value_type>(Gtid_mode::sysvar_mode));
}

void capture_forced_insert_id_state(THD *thd,
                                    Preserve_snapshot_metadata *metadata);

void restore_forced_insert_id_state(THD *thd, bool has_forced_insert_id,
                                    uint64_t forced_insert_id);

void restore_forced_insert_id_state(
    THD *thd, const Preserve_snapshot_metadata &metadata);

Preserve_snapshot_metadata make_no_cache_metadata(
    THD *thd, const std::string &token,
    Preserve_snapshot_binlog_state binlog_state) {
  Preserve_snapshot_metadata metadata;
  Security_context *sctx = thd->security_context();

  metadata.token = token;
  metadata.owner_user = lex_cstring_to_string(sctx->priv_user());
  metadata.owner_host = lex_cstring_to_string(sctx->priv_host());
  metadata.schema_name = lex_cstring_to_string(thd->db());
  metadata.binlog_state = binlog_state;
  metadata.wrote_to_cache = false;
  metadata.session_sql_log_bin =
      (thd->variables.option_bits & OPTION_BIN_LOG) != 0;
  metadata.option_bin_log = metadata.session_sql_log_bin;
  metadata.global_log_bin = mysql_bin_log.is_open();
  metadata.binlog_cache_size = 0;
  metadata.has_binlog_gtid_mode = true;
  metadata.binlog_gtid_mode = static_cast<uint8_t>(global_gtid_mode.get());
  metadata.tx_isolation = static_cast<uint8_t>(thd->tx_isolation);
  metadata.session_tx_isolation =
      static_cast<uint8_t>(thd->variables.transaction_isolation);
  metadata.tx_read_only = thd->tx_read_only;
  metadata.session_tx_read_only = thd->variables.transaction_read_only;
  metadata.binlog_format = static_cast<Preserve_snapshot_binlog_format>(
      thd->variables.binlog_format);
  metadata.foreign_key_checks =
      (thd->variables.option_bits & OPTION_NO_FOREIGN_KEY_CHECKS) == 0;
  metadata.unique_checks =
      (thd->variables.option_bits & OPTION_RELAXED_UNIQUE_CHECKS) == 0;
  metadata.autocommit =
      (thd->variables.option_bits & OPTION_NOT_AUTOCOMMIT) == 0;
  metadata.auto_increment_increment =
      thd->variables.auto_increment_increment;
  metadata.auto_increment_offset = thd->variables.auto_increment_offset;
  metadata.has_extended_session_state = true;
  metadata.sql_mode = thd->variables.sql_mode;
  if (thd->variables.time_zone != nullptr &&
      thd->variables.time_zone->get_name() != nullptr) {
    const String *time_zone_name = thd->variables.time_zone->get_name();
    metadata.time_zone_name.assign(time_zone_name->ptr(),
                                   time_zone_name->length());
  }
  metadata.character_set_client_number =
      charset_number_or_zero(thd->variables.character_set_client);
  metadata.character_set_results_number =
      charset_number_or_zero(thd->variables.character_set_results);
  metadata.collation_connection_number =
      charset_number_or_zero(thd->variables.collation_connection);
  metadata.first_successful_insert_id_in_prev_stmt =
      thd->first_successful_insert_id_in_prev_stmt;
  metadata.first_successful_insert_id_in_prev_stmt_for_binlog =
      thd->first_successful_insert_id_in_prev_stmt_for_binlog;
  metadata.first_successful_insert_id_in_cur_stmt =
      thd->first_successful_insert_id_in_cur_stmt;
  metadata.arg_of_last_insert_id_function =
      thd->arg_of_last_insert_id_function;
  metadata.stmt_depends_on_first_successful_insert_id_in_prev_stmt =
      thd->stmt_depends_on_first_successful_insert_id_in_prev_stmt;
  capture_forced_insert_id_state(thd, &metadata);
  metadata.binlog_gtid_next.clear();
  metadata.binlog_owned_gtid.clear();

  return metadata;
}

void reset_thd_after_preserve_detach(THD *thd) {
  Transaction_ctx *trn_ctx = thd->get_transaction();

  trn_ctx->xid_state()->reset();
  thd->variables.option_bits &= ~OPTION_BEGIN;
  thd->server_status &=
      ~(SERVER_STATUS_IN_TRANS | SERVER_STATUS_IN_TRANS_READONLY);

  trn_ctx->reset_unsafe_rollback_flags(Transaction_ctx::SESSION);
  trn_ctx->reset_unsafe_rollback_flags(Transaction_ctx::STMT);
  trn_ctx->reset_scope(Transaction_ctx::SESSION);
  trn_ctx->reset_scope(Transaction_ctx::STMT);

  thd->mdl_context.release_transactional_locks();
  trans_reset_one_shot_chistics(thd);
  trans_track_end_trx(thd);
  trn_ctx->cleanup();

#ifdef HAVE_PSI_TRANSACTION_INTERFACE
  if (thd->m_transaction_psi != nullptr) {
    MYSQL_ROLLBACK_TRANSACTION(thd->m_transaction_psi);
    thd->m_transaction_psi = nullptr;
  }
#endif
}

void reset_preserve_xid_to_active_transaction_xid(THD *thd) {
  Transaction_ctx *trn_ctx = thd->get_transaction();
  trn_ctx->xid_state()->reset();
  trn_ctx->xid_state()->set_query_id(thd->query_id);
  thd->variables.option_bits |= OPTION_BEGIN;
  thd->server_status |= SERVER_STATUS_IN_TRANS;
  if (thd->tx_read_only) thd->server_status |= SERVER_STATUS_IN_TRANS_READONLY;
}

void restore_preserved_transaction_access_mode(
    THD *thd, const Preserve_snapshot_metadata &metadata) {
  thd->tx_read_only = metadata.tx_read_only;
  thd->variables.transaction_read_only = metadata.session_tx_read_only;
  if (metadata.tx_read_only)
    thd->server_status |= SERVER_STATUS_IN_TRANS_READONLY;
  else
    thd->server_status &= ~SERVER_STATUS_IN_TRANS_READONLY;
}

bool restore_preserved_dml_policy(
    THD *thd, trx_t *trx, const Preserve_snapshot_metadata &metadata) {
  if (thd == nullptr || trx == nullptr ||
      set_session_autocommit_internal(thd, metadata.autocommit)) {
    return true;
  }

  if (metadata.foreign_key_checks)
    thd->variables.option_bits &= ~OPTION_NO_FOREIGN_KEY_CHECKS;
  else
    thd->variables.option_bits |= OPTION_NO_FOREIGN_KEY_CHECKS;
  if (metadata.unique_checks)
    thd->variables.option_bits &= ~OPTION_RELAXED_UNIQUE_CHECKS;
  else
    thd->variables.option_bits |= OPTION_RELAXED_UNIQUE_CHECKS;
  thd->variables.auto_increment_increment =
      metadata.auto_increment_increment;
  thd->variables.auto_increment_offset = metadata.auto_increment_offset;

  trx_preserve_restore_dml_policy(trx, metadata.foreign_key_checks,
                                  metadata.unique_checks);
  return false;
}

void restore_preserved_transaction_tracker(
    THD *thd, const Preserve_snapshot_metadata &metadata) {
  TX_TRACKER_GET(tracker);
  if (tracker == nullptr) return;
  tracker->set_read_flags(
      thd, metadata.tx_read_only ? TX_READ_ONLY : TX_READ_WRITE);
  tracker->add_trx_state(thd, TX_EXPLICIT);
}

void mark_preserved_transaction_attached(
    THD *thd, const Preserve_snapshot_metadata &metadata) {
  thd->variables.option_bits |= OPTION_BEGIN;
  thd->server_status |= SERVER_STATUS_IN_TRANS;
  if (metadata.tx_read_only)
    thd->server_status |= SERVER_STATUS_IN_TRANS_READONLY;
  else
    thd->server_status &= ~SERVER_STATUS_IN_TRANS_READONLY;
  restore_preserved_transaction_tracker(thd, metadata);
}

bool preserved_trx_resume_binlog_format_is_supported(
    THD *thd, const Preserve_snapshot_metadata &metadata) {
  return thd != nullptr &&
         metadata.binlog_format == Preserve_snapshot_binlog_format::ROW &&
         thd->variables.binlog_format == BINLOG_FORMAT_ROW;
}

void reset_preserve_statement_transaction_scope(THD *thd) {
  if (thd == nullptr) return;

  trx_preserve_reset_thd_statement_registration(thd);
  thd->get_transaction()->reset_unsafe_rollback_flags(Transaction_ctx::STMT);
  thd->get_transaction()->reset_scope(Transaction_ctx::STMT);
}

void dbug_assert_original_binlog_cache_clean_after_detach(THD *thd) {
  DBUG_EXECUTE_IF("preserve_trx_assert_original_binlog_cache_clean_after_detach",
                  assert(mysql_binlog_preserve_no_cache_boundary_is_clean(thd)););
  (void)thd;
}

void cleanup_original_binlog_cache_after_detach(THD *thd,
                                                bool has_logged_binlog_cache) {
  if (has_logged_binlog_cache) mysql_binlog_preserve_discard(thd);
  dbug_assert_original_binlog_cache_clean_after_detach(thd);
}

void discard_binlog_preserve_cache_and_reset_scopes(THD *thd) {
  if (thd == nullptr) return;

  mysql_binlog_preserve_discard(thd);
  thd->get_transaction()->reset_scope(Transaction_ctx::SESSION);
  thd->get_transaction()->reset_scope(Transaction_ctx::STMT);
}

uint64_t read_le64(const std::string &bytes, size_t offset) {
  uint64_t value = 0;
  for (size_t i = 0; i < 8; ++i)
    value |= static_cast<uint64_t>(
                 static_cast<unsigned char>(bytes[offset + i]))
             << (i * 8);
  return value;
}

uint32_t read_le32(const std::string &bytes, size_t offset) {
  uint32_t value = 0;
  for (size_t i = 0; i < 4; ++i)
    value |= static_cast<uint32_t>(
                 static_cast<unsigned char>(bytes[offset + i]))
             << (i * 8);
  return value;
}

uint16_t read_le16(const std::string &bytes, size_t offset) {
  uint16_t value = 0;
  for (size_t i = 0; i < 2; ++i)
    value |= static_cast<uint16_t>(
                 static_cast<unsigned char>(bytes[offset + i]))
             << (i * 8);
  return value;
}

bool split_record_and_predicate_locks_payload(
    const std::string &payload, std::string *record_locks_payload,
    std::string *predicate_locks_payload) {
  return trx_preserve_split_record_and_predicate_locks(
      payload, record_locks_payload, predicate_locks_payload);
}

bool mdl_preserve_scoped_namespace(MDL_key::enum_mdl_namespace mdl_namespace) {
  switch (mdl_namespace) {
    case MDL_key::GLOBAL:
    case MDL_key::TABLESPACE:
    case MDL_key::SCHEMA:
    case MDL_key::COMMIT:
    case MDL_key::FOREIGN_KEY:
    case MDL_key::CHECK_CONSTRAINT:
      return true;
    default:
      return false;
  }
}

bool mdl_preserve_normalized_namespace(
    MDL_key::enum_mdl_namespace mdl_namespace) {
  switch (mdl_namespace) {
    case MDL_key::FUNCTION:
    case MDL_key::PROCEDURE:
    case MDL_key::TRIGGER:
      return true;
    default:
      return false;
  }
}

bool mdl_preserve_type_supported(MDL_key::enum_mdl_namespace mdl_namespace,
                                 enum_mdl_type type) {
  if (mdl_preserve_scoped_namespace(mdl_namespace)) {
    return type == MDL_INTENTION_EXCLUSIVE || type == MDL_SHARED ||
           type == MDL_EXCLUSIVE;
  }

  return type != MDL_INTENTION_EXCLUSIVE;
}

bool mdl_normalized_part_key_components(const char *part_key,
                                        uint16_t db_length,
                                        uint16_t part_key_length,
                                        const char **normalized_name,
                                        size_t *normalized_name_length,
                                        const char **object_name) {
  if (normalized_name == nullptr || normalized_name_length == nullptr ||
      object_name == nullptr) {
    return false;
  }
  *normalized_name = nullptr;
  *normalized_name_length = 0;
  *object_name = nullptr;

  if (part_key_length > MAX_MDLKEY_LENGTH - 1 || db_length > NAME_LEN ||
      part_key_length < db_length + 3) {
    return false;
  }
  if (part_key[db_length] != '\0' || part_key[part_key_length - 1] != '\0') {
    return false;
  }
  if (std::memchr(part_key, '\0', db_length) != nullptr) return false;

  const size_t normalized_begin = db_length + 1;
  const size_t trailing_null = part_key_length - 1;
  size_t actual_separator = trailing_null;
  while (actual_separator > normalized_begin &&
         part_key[actual_separator - 1] != '\0') {
    --actual_separator;
  }
  if (actual_separator <= normalized_begin) return false;
  --actual_separator;

  const size_t normalized_length = actual_separator - normalized_begin;
  const size_t actual_begin = actual_separator + 1;
  const size_t actual_length = trailing_null - actual_begin;
  if (normalized_length == 0 || normalized_length > NAME_CHAR_LEN * 2 ||
      actual_length > NAME_LEN) {
    return false;
  }
  if (std::memchr(part_key + actual_begin, '\0', actual_length) != nullptr)
    return false;

  *normalized_name = part_key + normalized_begin;
  *normalized_name_length = normalized_length;
  *object_name = part_key + actual_begin;
  return true;
}

bool mdl_part_key_is_valid(const char *part_key, uint16_t db_length,
                           uint16_t part_key_length) {
  if (part_key_length > MAX_MDLKEY_LENGTH - 1 || db_length > NAME_LEN ||
      part_key_length < 2 || db_length > part_key_length - 2) {
    return false;
  }

  const size_t object_length = part_key_length - db_length - 2;
  if (object_length > NAME_LEN) return false;

  if (part_key[db_length] != '\0' ||
      part_key[part_key_length - 1] != '\0') {
    return false;
  }

  return std::memchr(part_key, '\0', db_length) == nullptr &&
         std::memchr(part_key + db_length + 1, '\0', object_length) ==
             nullptr;
}

bool mdl_preserve_part_key_is_valid(
    MDL_key::enum_mdl_namespace mdl_namespace, const char *part_key,
    uint16_t db_length, uint16_t part_key_length) {
  if (mdl_preserve_normalized_namespace(mdl_namespace)) {
    const char *normalized_name = nullptr;
    size_t normalized_name_length = 0;
    const char *object_name = nullptr;
    return mdl_normalized_part_key_components(
        part_key, db_length, part_key_length, &normalized_name,
        &normalized_name_length, &object_name);
  }
  return mdl_part_key_is_valid(part_key, db_length, part_key_length);
}

bool mdl_descriptors_payload_is_valid(const std::string &payload,
                                      uint32_t *descriptor_count) {
  if (payload.length() < 4) return false;

  const uint32_t count = read_le32(payload, 0);
  size_t offset = 4;
  for (uint32_t ordinal = 1; ordinal <= count; ++ordinal) {
    if (payload.length() - offset < 12) return false;

    const auto raw_namespace =
        static_cast<unsigned char>(payload[offset]);
    const auto raw_type = static_cast<unsigned char>(payload[offset + 1]);
    const auto raw_duration =
        static_cast<unsigned char>(payload[offset + 2]);
    const auto reserved = static_cast<unsigned char>(payload[offset + 3]);
    const uint32_t stored_ordinal = read_le32(payload, offset + 4);
    const uint16_t db_length = read_le16(payload, offset + 8);
    const uint16_t part_key_length = read_le16(payload, offset + 10);
    offset += 12;

    if (reserved != 0 || raw_namespace >= MDL_key::NAMESPACE_END ||
        raw_type >= MDL_TYPE_END || raw_duration != MDL_TRANSACTION ||
        stored_ordinal != ordinal ||
        payload.length() - offset < part_key_length) {
      return false;
    }

    const auto mdl_namespace =
        static_cast<MDL_key::enum_mdl_namespace>(raw_namespace);
    const auto type = static_cast<enum_mdl_type>(raw_type);
    const char *part_key = payload.data() + offset;
    if (!preserve_trx_lock_warmcopy_mdl_namespace_supported(raw_namespace) ||
        !mdl_preserve_type_supported(mdl_namespace, type) ||
        !mdl_preserve_part_key_is_valid(mdl_namespace, part_key, db_length,
                                        part_key_length)) {
      return false;
    }
    offset += part_key_length;
  }

  if (offset != payload.length()) return false;
  if (descriptor_count != nullptr) *descriptor_count = count;
  return true;
}

bool preserve_trx_mdl_table_type_requires_write_privilege(
    enum_mdl_type type) {
  return type >= MDL_SHARED_WRITE && type != MDL_SHARED_READ_ONLY;
}

bool preserve_trx_recheck_table_privileges(
    THD *thd, const char *part_key, uint16_t db_length,
    uint16_t part_key_length, enum_mdl_type type,
    bool write_locks_require_write_privilege) {
  const char *table_name = part_key + db_length + 1;
  const size_t table_name_length = part_key_length - db_length - 2;
  const Access_bitmask access =
      write_locks_require_write_privilege &&
              preserve_trx_mdl_table_type_requires_write_privilege(type)
          ? INSERT_ACL | UPDATE_ACL | DELETE_ACL
          : TABLE_OP_ACLS;

  const std::string schema_name(part_key, db_length);
  const std::string normalized_table_name(table_name, table_name_length);
  Security_context *sctx = thd->security_context();
  Access_bitmask granted_access = sctx->master_access(schema_name);

  if (sctx->get_active_roles()->size() > 0) {
    const LEX_CSTRING db{schema_name.c_str(), schema_name.length()};
    const LEX_CSTRING table{normalized_table_name.c_str(),
                            normalized_table_name.length()};
    granted_access |= sctx->db_acl(db, true) | sctx->table_acl(db, table);
  } else {
    granted_access |=
        acl_get(thd, sctx->host().str, sctx->ip().str, sctx->priv_user().str,
                schema_name.c_str(), false);

    Acl_cache_lock_guard acl_cache_lock(thd, Acl_cache_lock_mode::READ_MODE);
    if (!acl_cache_lock.lock(false)) return true;

    GRANT_TABLE *grant_table =
        table_hash_search(sctx->host().str, sctx->ip().str,
                          schema_name.c_str(), sctx->priv_user().str,
                          normalized_table_name.c_str(), false);
    granted_access |= grant_table == nullptr ? 0 : grant_table->privs;
  }

  if ((granted_access & access) != 0) return false;
  return true;
}

bool preserve_trx_recheck_schema_privileges(THD *thd, const char *part_key,
                                            uint16_t db_length) {
  const std::string schema_name(part_key, db_length);
  for (Access_bitmask access = 1; access < DB_OP_ACLS; access <<= 1) {
    if ((access & DB_OP_ACLS) != 0 &&
        !check_access(thd, access, schema_name.c_str(), nullptr, nullptr,
                      false, true)) {
      return false;
    }
  }

  Security_context *sctx = thd->security_context();
  my_error(ER_DBACCESS_DENIED_ERROR, MYF(0), sctx->priv_user().str,
           sctx->priv_host().str, schema_name.c_str());
  return true;
}

bool preserve_trx_find_trigger_subject_table(THD *thd,
                                             const std::string &schema_name,
                                             const char *trigger_name,
                                             std::string *table_name) {
  if (table_name == nullptr) return true;

  dd::Schema_MDL_locker mdl_locker(thd);
  dd::cache::Dictionary_client *dd_client = thd->dd_client();
  dd::cache::Dictionary_client::Auto_releaser releaser(dd_client);

  const dd::Schema *schema = nullptr;
  if (mdl_locker.ensure_locked(schema_name.c_str()) ||
      dd_client->acquire(schema_name.c_str(), &schema) || schema == nullptr) {
    return true;
  }

  dd::String_type table_name_from_dd;
  if (dd_client->get_table_name_by_trigger_name(*schema, trigger_name,
                                                &table_name_from_dd) ||
      table_name_from_dd.empty()) {
    return true;
  }

  *table_name = table_name_from_dd.c_str();
  if (lower_case_table_names == 2) {
    char lc_table_name[NAME_LEN + 1];
    my_stpncpy(lc_table_name, table_name->c_str(), NAME_LEN);
    my_casedn_str(files_charset_info, lc_table_name);
    lc_table_name[NAME_LEN] = '\0';
    *table_name = lc_table_name;
  }
  return false;
}

bool preserve_trx_recheck_trigger_privileges(THD *thd, const char *part_key,
                                             uint16_t db_length,
                                             uint16_t part_key_length) {
  const char *normalized_name = nullptr;
  size_t normalized_name_length = 0;
  const char *trigger_name = nullptr;
  if (!mdl_normalized_part_key_components(part_key, db_length, part_key_length,
                                          &normalized_name,
                                          &normalized_name_length,
                                          &trigger_name)) {
    return true;
  }

  const std::string schema_name(part_key, db_length);
  std::string table_name;
  if (preserve_trx_find_trigger_subject_table(thd, schema_name, trigger_name,
                                              &table_name)) {
    return true;
  }

  Table_ref table(schema_name.c_str(), schema_name.length(),
                  table_name.c_str(), table_name.length(), table_name.c_str(),
                  TL_READ);
  if (check_table_access(thd, TRIGGER_ACL, &table, false, 1, true)) {
    my_error(ER_SPECIFIC_ACCESS_DENIED_ERROR, MYF(0), "TRIGGER");
    return true;
  }
  return false;
}

bool preserve_trx_recheck_routine_privileges(
    THD *thd, MDL_key::enum_mdl_namespace mdl_namespace, const char *part_key,
    uint16_t db_length, uint16_t part_key_length) {
  const char *normalized_name = nullptr;
  size_t normalized_name_length = 0;
  const char *object_name = nullptr;
  if (!mdl_normalized_part_key_components(part_key, db_length,
                                          part_key_length, &normalized_name,
                                          &normalized_name_length,
                                          &object_name)) {
    return true;
  }

  std::string routine_name(object_name);
  const bool is_procedure = mdl_namespace == MDL_key::PROCEDURE;
  return check_routine_access(thd, EXECUTE_ACL, part_key,
                              routine_name.empty() ? const_cast<char *>("")
                                                   : &routine_name[0],
                              is_procedure, true);
}

bool preserve_trx_recheck_mdl_object_privileges(
    THD *thd, const std::string &payload,
    bool table_write_locks_require_write_privilege = false) {
  if (thd == nullptr) return true;

  uint32_t descriptor_count = 0;
  if (!mdl_descriptors_payload_is_valid(payload, &descriptor_count)) return true;

  size_t offset = 4;
  for (uint32_t i = 0; i < descriptor_count; ++i) {
    const auto mdl_namespace = static_cast<MDL_key::enum_mdl_namespace>(
        static_cast<unsigned char>(payload[offset]));
    const auto mdl_type = static_cast<enum_mdl_type>(
        static_cast<unsigned char>(payload[offset + 1]));
    const uint16_t db_length = read_le16(payload, offset + 8);
    const uint16_t part_key_length = read_le16(payload, offset + 10);
    offset += 12;

    const char *part_key = payload.data() + offset;
    switch (mdl_namespace) {
      case MDL_key::GLOBAL:
      case MDL_key::COMMIT:
      case MDL_key::FOREIGN_KEY:
      case MDL_key::CHECK_CONSTRAINT:
        break;
      case MDL_key::TABLESPACE:
        if (check_global_access(thd, CREATE_TABLESPACE_ACL)) return true;
        break;
      case MDL_key::SCHEMA:
        if (preserve_trx_recheck_schema_privileges(thd, part_key, db_length))
          return true;
        break;
      case MDL_key::TABLE:
        if (preserve_trx_recheck_table_privileges(
                thd, part_key, db_length, part_key_length, mdl_type,
                table_write_locks_require_write_privilege))
          return true;
        break;
      case MDL_key::FUNCTION:
      case MDL_key::PROCEDURE:
        if (preserve_trx_recheck_routine_privileges(
                thd, mdl_namespace, part_key, db_length, part_key_length))
          return true;
        break;
      case MDL_key::TRIGGER:
        if (preserve_trx_recheck_trigger_privileges(
                thd, part_key, db_length, part_key_length))
          return true;
        break;
      default:
        return true;
    }

    offset += part_key_length;
  }

  return offset != payload.length();
}

constexpr Access_bitmask kModifiedTableWriteAcls =
    INSERT_ACL | UPDATE_ACL | DELETE_ACL;

bool preserve_trx_modified_table_write_access_mask(
    THD *thd, const std::string &schema_name, const std::string &table_name,
    Access_bitmask *write_access) {
  if (thd == nullptr || write_access == nullptr) return true;

  Security_context *sctx = thd->security_context();
  Access_bitmask access = sctx->master_access(schema_name);

  if (sctx->get_active_roles()->size() > 0) {
    const LEX_CSTRING db{schema_name.c_str(), schema_name.length()};
    const LEX_CSTRING table{table_name.c_str(), table_name.length()};
    access |= sctx->db_acl(db, true) | sctx->table_acl(db, table);
  } else {
    const Access_bitmask db_access =
        acl_get(thd, sctx->host().str, sctx->ip().str,
                sctx->priv_user().str, schema_name.c_str(), false);
    access |= db_access;

    Acl_cache_lock_guard acl_cache_lock(thd, Acl_cache_lock_mode::READ_MODE);
    if (!acl_cache_lock.lock(false)) return true;

    GRANT_TABLE *grant_table =
        table_hash_search(sctx->host().str, sctx->ip().str,
                          schema_name.c_str(), sctx->priv_user().str,
                          table_name.c_str(), false);
    const Access_bitmask table_access =
        grant_table == nullptr ? 0 : grant_table->privs;
    access |= table_access;
  }

  *write_access = access & kModifiedTableWriteAcls;
  return false;
}

bool preserve_trx_populate_modified_table_write_masks(
    THD *thd, std::vector<Preserve_modified_table_name> *tables) {
  if (thd == nullptr || tables == nullptr) return true;

  for (Preserve_modified_table_name &name : *tables) {
    Access_bitmask write_access = 0;
    if (preserve_trx_modified_table_write_access_mask(
            thd, name.schema_name, name.table_name, &write_access) ||
        write_access == 0) {
      return true;
    }
    name.required_write_acls = static_cast<uint32_t>(write_access);
  }

  return false;
}

bool preserve_trx_recheck_modified_table_privileges(
    THD *thd, const std::vector<Preserve_modified_table_name> &tables,
    bool require_all_write_acls = false) {
  if (thd == nullptr) return true;

  for (const Preserve_modified_table_name &name : tables) {
    Access_bitmask write_access = 0;
    if (preserve_trx_modified_table_write_access_mask(
            thd, name.schema_name, name.table_name, &write_access)) {
      return true;
    }
    if (require_all_write_acls
            ? write_access != kModifiedTableWriteAcls
            : write_access == 0) {
      return true;
    }
  }

  return false;
}

bool preserve_trx_recheck_modified_table_privileges(
    THD *thd,
    const std::vector<Preserve_snapshot_modified_table_name> &tables,
    bool require_all_write_acls = false) {
  if (thd == nullptr) return true;

  for (const Preserve_snapshot_modified_table_name &name : tables) {
    Access_bitmask write_access = 0;
    if (preserve_trx_modified_table_write_access_mask(
            thd, name.schema_name, name.table_name, &write_access)) {
      return true;
    }
    const Access_bitmask required =
        name.required_write_acls & kModifiedTableWriteAcls;
    if (required != 0) {
      if ((write_access & required) != required) return true;
    } else if (require_all_write_acls
                   ? write_access != kModifiedTableWriteAcls
                   : write_access == 0) {
      return true;
    }
  }
  return false;
}

bool preserve_trx_recheck_resume_object_privileges(
    THD *thd, const Preserve_snapshot_metadata &metadata,
    bool require_all_modified_write_acls) {
  if (preserve_trx_recheck_mdl_object_privileges(
          thd, metadata.mdl_descriptors_payload,
          true /* table_write_locks_require_write_privilege */)) {
    if (!thd->is_error())
      my_error(ER_PRESERVE_TRX_ACCESS_DENIED, MYF(0));
    return true;
  }
  if (metadata.mod_tables_count != metadata.modified_table_names.size() ||
      preserve_trx_recheck_modified_table_privileges(
          thd, metadata.modified_table_names, require_all_modified_write_acls)) {
    if (!thd->is_error())
      my_error(ER_PRESERVE_TRX_ACCESS_DENIED, MYF(0));
    return true;
  }

  return false;
}

bool sql_savepoints_payload_is_valid(const std::string &payload,
                                     uint32_t *savepoint_count,
                                     uint32_t mdl_descriptor_count,
                                     uint32_t *innodb_savepoint_count =
                                         nullptr,
                                     uint32_t *binlog_savepoint_count =
                                         nullptr,
                                     uint64_t max_binlog_position =
                                         UINT64_MAX,
                                     uint64_t max_binlog_event_counter =
                                         UINT64_MAX) {
  if (savepoint_count != nullptr) *savepoint_count = 0;
  if (innodb_savepoint_count != nullptr) *innodb_savepoint_count = 0;
  if (binlog_savepoint_count != nullptr) *binlog_savepoint_count = 0;
  if (payload.empty()) return true;
  if (payload.length() < 4) return false;

  const uint32_t count = read_le32(payload, 0);
  if (count == 0) return false;

  std::set<std::string> names;
  size_t offset = 4;
  for (uint32_t i = 0; i < count; ++i) {
    if (payload.length() - offset < 12) return false;

    const uint16_t name_length = read_le16(payload, offset);
    const uint16_t handler_flags = read_le16(payload, offset + 2);
    const uint32_t mdl_stmt_ordinal = read_le32(payload, offset + 4);
    const uint32_t mdl_trans_ordinal = read_le32(payload, offset + 8);
    offset += 12;

    if (name_length == 0 || name_length > NAME_LEN ||
        (handler_flags & ~kSavepointHandlerSupportedMask) != 0 ||
        mdl_stmt_ordinal != 0 || mdl_trans_ordinal > mdl_descriptor_count ||
        payload.length() - offset < name_length) {
      return false;
    }
    if ((handler_flags & kSavepointHandlerBinlog) != 0) {
      if (payload.length() - offset < kBinlogSavepointCheckpointLength)
        return false;
      const uint64_t binlog_position = read_le64(payload, offset);
      const uint32_t checkpoint_flags = read_le32(payload, offset + 8);
      const uint64_t checkpoint_event_counter =
          read_le64(payload, offset + 12);
      if (binlog_position == 0 || binlog_position == UINT64_MAX ||
          binlog_position > max_binlog_position ||
          (checkpoint_flags & ~kBinlogSavepointCheckpointKnownFlags) != 0 ||
          checkpoint_event_counter == 0 ||
          checkpoint_event_counter > max_binlog_event_counter) {
        return false;
      }
      offset += kBinlogSavepointCheckpointLength;
      if (payload.length() - offset < name_length) return false;
    }

    const std::string name(payload.data() + offset, name_length);
    if (name.find('\0') != std::string::npos ||
        !names.insert(name).second) {
      return false;
    }
    offset += name_length;
    if ((handler_flags & kSavepointHandlerInnodb) != 0 &&
        innodb_savepoint_count != nullptr) {
      ++*innodb_savepoint_count;
    }
    if ((handler_flags & kSavepointHandlerBinlog) != 0 &&
        binlog_savepoint_count != nullptr) {
      ++*binlog_savepoint_count;
    }
  }

  if (offset != payload.length()) return false;
  if (savepoint_count != nullptr) *savepoint_count = count;
  return true;
}

/*
  RAII owner for transitions of the global preserve manager state.

  Construction performs the compare-exchange from the expected state and records
  the owner thread id when requested. transition_to() publishes an intermediate
  or final state while the guard remains active; it does not cancel rollback.
  Destruction restores the original state unless dismiss() has explicitly handed
  ownership to a later path such as shutdown handoff or cleanup-failed recovery.
*/
class Preserve_trx_manager_state_guard {
 public:
  Preserve_trx_manager_state_guard(Preserve_trx_manager_state from,
                                   Preserve_trx_manager_state to,
                                   my_thread_id owner_thread_id = 0)
      : m_from(from),
        m_current_state(to),
        m_active(false),
        m_current_owner_thread_id(owner_thread_id) {
    m_active = preserve_trx_compare_exchange_manager_state_owner(
        from, to, owner_thread_id);
    if (m_active) preserve_trx_notify_manager_state_published_for_unit_test();
  }

  Preserve_trx_manager_state_guard(const Preserve_trx_manager_state_guard &) =
      delete;
  Preserve_trx_manager_state_guard &operator=(
      const Preserve_trx_manager_state_guard &) = delete;

  ~Preserve_trx_manager_state_guard() {
    if (m_active)
      (void)preserve_trx_compare_exchange_manager_state_owner(
          m_current_state, m_current_owner_thread_id, m_from, 0);
  }

  bool active() const { return m_active; }
  bool transition_to(Preserve_trx_manager_state to) {
    return transition_to(to, m_current_owner_thread_id);
  }
  bool transition_to(Preserve_trx_manager_state to,
                     my_thread_id owner_thread_id) {
    if (!m_active) return false;
    if (!preserve_trx_compare_exchange_manager_state_owner(
            m_current_state, m_current_owner_thread_id, to, owner_thread_id)) {
      m_active = false;
      return false;
    }
    m_current_state = to;
    m_current_owner_thread_id = owner_thread_id;
    return true;
  }
  void dismiss() { m_active = false; }

 private:
  Preserve_trx_manager_state m_from;
  Preserve_trx_manager_state m_current_state;
  bool m_active;
  my_thread_id m_current_owner_thread_id;
};

class Preserved_trx_observable_state_guard {
 public:
  Preserved_trx_observable_state_guard(
      const Preserve_snapshot_metadata &metadata, trx_t *trx,
      Preserved_trx_lifecycle_state state)
      : m_token(metadata.token), m_state(state), m_active(true) {
    preserved_trx_add_observable_record(metadata, trx, state);
  }

  Preserved_trx_observable_state_guard(
      const Preserved_trx_observable_state_guard &) = delete;
  Preserved_trx_observable_state_guard &operator=(
      const Preserved_trx_observable_state_guard &) = delete;

  ~Preserved_trx_observable_state_guard() { remove(); }

  void remove() {
    if (!m_active) return;
    preserved_trx_remove_observable_record(m_token, m_state);
    m_active = false;
  }

 private:
  std::string m_token;
  Preserved_trx_lifecycle_state m_state;
  bool m_active;
};

/*
  Temporarily execute preserve work on a quiesced batch target THD.

  The drain owner or a worker enters only after the target belongs to the same
  generation, is QUIESCED, idle in the server loop, and not killed. While active,
  the guard marks ATTACHING, installs the target THD globals/thread stack, and
  restores the target THD on destruction; when an owner THD is present, owner
  globals are restored as well. It is an ownership boundary for batch preserve,
  not a generic THD context switch helper.
*/
class Preserve_thd_context_switch {
 public:
  Preserve_thd_context_switch(THD *current_thd_arg, THD *target_thd,
                              ulonglong generation,
                              const char *worker_thread_stack = nullptr)
      : m_current_thd(current_thd_arg),
        m_target_thd(target_thd),
        m_generation(generation),
        m_worker_thread_stack(worker_thread_stack),
        m_target_old_real_id(target_thd->real_id),
        m_target_old_thread_stack(target_thd->thread_stack) {
    assert(m_target_thd != nullptr);
    assert(m_current_thd != nullptr || m_worker_thread_stack != nullptr);
    mysql_mutex_lock(&m_target_thd->LOCK_thd_data);
    m_active = m_target_thd->preserve_trx_batch_generation == generation &&
               m_target_thd->preserve_trx_batch_state ==
                   Preserve_trx_batch_thd_state::QUIESCED &&
               m_target_thd->killed == THD::NOT_KILLED &&
               m_target_thd->m_server_idle &&
               !m_target_thd->release_resources_done();
    if (m_active) {
      m_target_thd->preserve_trx_batch_state =
          Preserve_trx_batch_thd_state::ATTACHING;
    }
    mysql_mutex_unlock(&m_target_thd->LOCK_thd_data);
    if (!m_active) return;

    if (m_current_thd != nullptr) m_current_thd->restore_globals();
    m_target_thd->thread_stack =
        m_current_thd != nullptr ? m_current_thd->thread_stack
                                 : m_worker_thread_stack;
    m_target_thd->store_globals();
  }

  Preserve_thd_context_switch(const Preserve_thd_context_switch &) = delete;
  Preserve_thd_context_switch &operator=(const Preserve_thd_context_switch &) =
      delete;

  ~Preserve_thd_context_switch() {
    if (!m_active) return;
    m_target_thd->restore_globals();
    m_target_thd->real_id = m_target_old_real_id;
    m_target_thd->thread_stack = m_target_old_thread_stack;
    if (m_current_thd != nullptr) m_current_thd->store_globals();
    mysql_mutex_lock(&m_target_thd->LOCK_thd_data);
    if (m_target_thd->preserve_trx_batch_generation == m_generation &&
        m_target_thd->preserve_trx_batch_state ==
            Preserve_trx_batch_thd_state::ATTACHING) {
      m_target_thd->preserve_trx_batch_state =
          Preserve_trx_batch_thd_state::QUIESCED;
    }
    mysql_mutex_unlock(&m_target_thd->LOCK_thd_data);
  }

  bool active() const { return m_active; }

 private:
  THD *m_current_thd;
  THD *m_target_thd;
  ulonglong m_generation;
  const char *m_worker_thread_stack;
  my_thread_t m_target_old_real_id;
  const char *m_target_old_thread_stack;
  bool m_active{false};
};

/*
  Rollback guard for SQL/session state restored during RESUME.

  RESUME restores session-visible state before the preserved InnoDB transaction
  is fully attached. If a later import, MDL, temp-table materialization, or
  attach step fails, this guard returns the caller's THD variables and status to
  the pre-RESUME values. dismiss() is called only after the resumed transaction
  has taken ownership of those restored values.
*/
class Resume_thd_state_guard {
 public:
  explicit Resume_thd_state_guard(THD *thd)
      : m_thd(thd),
        m_option_bits(thd->variables.option_bits),
        m_sql_log_bin(thd->variables.sql_log_bin),
        m_server_status(thd->server_status),
        m_tx_isolation(thd->tx_isolation),
        m_session_tx_isolation(static_cast<enum_tx_isolation>(
            thd->variables.transaction_isolation)),
        m_binlog_trx_compression(thd->variables.binlog_trx_compression),
        m_binlog_trx_compression_type(
            thd->variables.binlog_trx_compression_type),
        m_binlog_trx_compression_level_zstd(
            thd->variables.binlog_trx_compression_level_zstd),
        m_tx_read_only(thd->tx_read_only),
        m_session_tx_read_only(thd->variables.transaction_read_only),
        m_auto_increment_increment(thd->variables.auto_increment_increment),
        m_auto_increment_offset(thd->variables.auto_increment_offset),
        m_sql_mode(thd->variables.sql_mode),
        m_time_zone(thd->variables.time_zone),
        m_character_set_client(thd->variables.character_set_client),
        m_character_set_results(thd->variables.character_set_results),
        m_collation_connection(thd->variables.collation_connection),
        m_first_successful_insert_id_in_prev_stmt(
            thd->first_successful_insert_id_in_prev_stmt),
        m_first_successful_insert_id_in_prev_stmt_for_binlog(
            thd->first_successful_insert_id_in_prev_stmt_for_binlog),
        m_first_successful_insert_id_in_cur_stmt(
            thd->first_successful_insert_id_in_cur_stmt),
        m_arg_of_last_insert_id_function(thd->arg_of_last_insert_id_function),
        m_stmt_depends_on_first_successful_insert_id_in_prev_stmt(
            thd->stmt_depends_on_first_successful_insert_id_in_prev_stmt),
        m_has_forced_insert_id(
            thd->auto_inc_intervals_forced.nb_elements() > 0),
        m_forced_insert_id(m_has_forced_insert_id
                               ? thd->auto_inc_intervals_forced.minimum()
                               : 0),
        m_user_vars_snapshot_valid(
            !export_user_vars_payload(thd, &m_user_vars_payload)) {}

  Resume_thd_state_guard(const Resume_thd_state_guard &) = delete;
  Resume_thd_state_guard &operator=(const Resume_thd_state_guard &) = delete;

  ~Resume_thd_state_guard() {
    if (m_active) restore();
  }

  void dismiss() { m_active = false; }

 private:
  void restore() {
    const bool saved_autocommit =
        (m_option_bits & OPTION_AUTOCOMMIT) != 0;
    if (set_session_autocommit_internal(m_thd, saved_autocommit)) {
      LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
             "PRESERVE: failed to restore autocommit after RESUME failure");
    }
    const auto autocommit_mask = OPTION_AUTOCOMMIT | OPTION_NOT_AUTOCOMMIT;
    const auto restored_autocommit_bits =
        m_thd->variables.option_bits & autocommit_mask;
    m_thd->variables.option_bits =
        (m_option_bits & ~autocommit_mask) | restored_autocommit_bits;
    m_thd->variables.sql_log_bin = m_sql_log_bin;
    m_thd->server_status = m_server_status;
    trans_reset_one_shot_chistics(m_thd);
    m_thd->tx_isolation = m_tx_isolation;
    m_thd->variables.transaction_isolation = m_session_tx_isolation;
    m_thd->variables.binlog_trx_compression = m_binlog_trx_compression;
    m_thd->variables.binlog_trx_compression_type =
        m_binlog_trx_compression_type;
    m_thd->variables.binlog_trx_compression_level_zstd =
        m_binlog_trx_compression_level_zstd;
    m_thd->tx_read_only = m_tx_read_only;
    m_thd->variables.transaction_read_only = m_session_tx_read_only;
    m_thd->variables.auto_increment_increment = m_auto_increment_increment;
    m_thd->variables.auto_increment_offset = m_auto_increment_offset;
    m_thd->variables.sql_mode = m_sql_mode;
    m_thd->variables.time_zone = m_time_zone;
    m_thd->variables.character_set_client = m_character_set_client;
    m_thd->variables.character_set_results = m_character_set_results;
    m_thd->variables.collation_connection = m_collation_connection;
    m_thd->update_charset();
    m_thd->first_successful_insert_id_in_prev_stmt =
        m_first_successful_insert_id_in_prev_stmt;
    m_thd->first_successful_insert_id_in_prev_stmt_for_binlog =
        m_first_successful_insert_id_in_prev_stmt_for_binlog;
    m_thd->first_successful_insert_id_in_cur_stmt =
        m_first_successful_insert_id_in_cur_stmt;
    m_thd->arg_of_last_insert_id_function =
        m_arg_of_last_insert_id_function;
    m_thd->stmt_depends_on_first_successful_insert_id_in_prev_stmt =
        m_stmt_depends_on_first_successful_insert_id_in_prev_stmt;
    restore_forced_insert_id_state(m_thd, m_has_forced_insert_id,
                                   m_forced_insert_id);
    if (m_user_vars_snapshot_valid &&
        restore_user_vars_payload(m_thd, m_user_vars_payload)) {
      LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
             "PRESERVE: failed to restore session user variables after "
             "RESUME failure");
    }
  }

  THD *m_thd;
  decltype(THD::variables.option_bits) m_option_bits;
  decltype(THD::variables.sql_log_bin) m_sql_log_bin;
  decltype(THD::server_status) m_server_status;
  enum_tx_isolation m_tx_isolation;
  enum_tx_isolation m_session_tx_isolation;
  decltype(THD::variables.binlog_trx_compression) m_binlog_trx_compression;
  decltype(THD::variables.binlog_trx_compression_type)
      m_binlog_trx_compression_type;
  decltype(THD::variables.binlog_trx_compression_level_zstd)
      m_binlog_trx_compression_level_zstd;
  bool m_tx_read_only;
  bool m_session_tx_read_only;
  decltype(THD::variables.auto_increment_increment) m_auto_increment_increment;
  decltype(THD::variables.auto_increment_offset) m_auto_increment_offset;
  sql_mode_t m_sql_mode;
  Time_zone *m_time_zone;
  const CHARSET_INFO *m_character_set_client;
  const CHARSET_INFO *m_character_set_results;
  const CHARSET_INFO *m_collation_connection;
  ulonglong m_first_successful_insert_id_in_prev_stmt;
  ulonglong m_first_successful_insert_id_in_prev_stmt_for_binlog;
  ulonglong m_first_successful_insert_id_in_cur_stmt;
  bool m_arg_of_last_insert_id_function;
  bool m_stmt_depends_on_first_successful_insert_id_in_prev_stmt;
  bool m_has_forced_insert_id;
  uint64_t m_forced_insert_id;
  std::string m_user_vars_payload;
  bool m_user_vars_snapshot_valid;
  bool m_active{true};
};

void restore_last_insert_id_state(
    THD *thd, const Preserve_snapshot_metadata &metadata) {
  thd->first_successful_insert_id_in_prev_stmt =
      metadata.first_successful_insert_id_in_prev_stmt;
  thd->first_successful_insert_id_in_prev_stmt_for_binlog =
      metadata.first_successful_insert_id_in_prev_stmt_for_binlog;
  thd->first_successful_insert_id_in_cur_stmt =
      metadata.first_successful_insert_id_in_cur_stmt;
  thd->arg_of_last_insert_id_function =
      metadata.arg_of_last_insert_id_function;
  thd->stmt_depends_on_first_successful_insert_id_in_prev_stmt =
      metadata.stmt_depends_on_first_successful_insert_id_in_prev_stmt;
}

void capture_forced_insert_id_state(THD *thd,
                                    Preserve_snapshot_metadata *metadata) {
  metadata->has_forced_insert_id =
      thd->auto_inc_intervals_forced.nb_elements() > 0;
  metadata->forced_insert_id =
      metadata->has_forced_insert_id
          ? thd->auto_inc_intervals_forced.minimum()
          : 0;
}

void restore_forced_insert_id_state(THD *thd, bool has_forced_insert_id,
                                    uint64_t forced_insert_id) {
  thd->auto_inc_intervals_forced.clear();
  if (has_forced_insert_id) thd->force_one_auto_inc_interval(forced_insert_id);
}

void restore_forced_insert_id_state(
    THD *thd, const Preserve_snapshot_metadata &metadata) {
  restore_forced_insert_id_state(thd, metadata.has_forced_insert_id,
                                 metadata.forced_insert_id);
}

bool restore_preserved_session_variables(
    THD *thd, const Preserve_snapshot_metadata &metadata) {
  if (!metadata.has_extended_session_state) return false;

  thd->variables.transaction_isolation =
      static_cast<enum_tx_isolation>(metadata.session_tx_isolation);
  thd->variables.sql_mode = static_cast<sql_mode_t>(metadata.sql_mode);

  if (!metadata.time_zone_name.empty()) {
    String time_zone_name(metadata.time_zone_name.data(),
                          metadata.time_zone_name.length(),
                          &my_charset_latin1);
    Time_zone *time_zone = my_tz_find(thd, &time_zone_name);
    if (time_zone == nullptr) return true;
    thd->variables.time_zone = time_zone;
  }

  if (metadata.character_set_client_number != 0 ||
      metadata.character_set_results_number != 0 ||
      metadata.collation_connection_number != 0) {
    if (!session_state_charset_numbers_are_valid(
            metadata.character_set_client_number,
            metadata.character_set_results_number,
            metadata.collation_connection_number)) {
      return true;
    }

    CHARSET_INFO *character_set_client =
        get_charset(metadata.character_set_client_number, MYF(0));
    CHARSET_INFO *collation_connection =
        get_charset(metadata.collation_connection_number, MYF(0));
    CHARSET_INFO *character_set_results =
        metadata.character_set_results_number == 0
            ? nullptr
            : get_charset(metadata.character_set_results_number, MYF(0));
    if (character_set_client == nullptr || collation_connection == nullptr ||
        (metadata.character_set_results_number != 0 &&
         character_set_results == nullptr)) {
      return true;
    }

    thd->variables.character_set_client = character_set_client;
    thd->variables.character_set_results = character_set_results;
    thd->variables.collation_connection = collation_connection;
    thd->update_charset();
  }

  return false;
}

struct Preserve_sql_savepoint_entry {
  std::string name;
  uint16_t handler_flags{kSavepointHandlerNone};
  uint32_t mdl_stmt_ordinal{0};
  uint32_t mdl_trans_ordinal{0};
  Mysql_binlog_preserve_cache_state binlog_cache_state;

  bool has_innodb_handler() const {
    return (handler_flags & kSavepointHandlerInnodb) != 0;
  }

  bool has_binlog_handler() const {
    return (handler_flags & kSavepointHandlerBinlog) != 0;
  }
};

bool savepoint_handler_state(SAVEPOINT *savepoint, uint16_t *handler_flags,
                             Mysql_binlog_preserve_cache_state
                                 *binlog_cache_state,
                             THD *thd) {
  if (handler_flags == nullptr || binlog_cache_state == nullptr) return true;
  *handler_flags = kSavepointHandlerNone;
  *binlog_cache_state = Mysql_binlog_preserve_cache_state{};
  for (Ha_trx_info *ha_info = savepoint->ha_list; ha_info != nullptr;
       ha_info = ha_info->next()) {
    handlerton *hton = ha_info->ht();
    if (ha_legacy_type(hton) == DB_TYPE_INNODB) {
      *handler_flags |= kSavepointHandlerInnodb;
    } else if (ha_legacy_type(hton) == DB_TYPE_BINLOG) {
      *handler_flags |= kSavepointHandlerBinlog;
      const my_off_t pos =
          *reinterpret_cast<const my_off_t *>(pointer_cast<const uchar *>(
              savepoint + 1) + hton->savepoint_offset);
      if (pos == ~static_cast<my_off_t>(0)) return true;
      if (mysql_binlog_preserve_get_cache_state(
              thd, static_cast<uint64_t>(pos), binlog_cache_state)) {
        return true;
      }
    } else {
      return true;
    }
  }
  return false;
}

bool export_sql_savepoints(THD *thd, Preserve_snapshot_binlog_state binlog_state,
                           std::string *payload,
                           uint32_t *savepoint_count,
                           uint32_t *innodb_savepoint_count,
                           std::vector<Preserve_savepoint_participant>
                               *session_participant_order,
                           std::vector<uint16_t> *savepoint_suffix_ordinals) {
  if (thd == nullptr || payload == nullptr || savepoint_count == nullptr ||
      innodb_savepoint_count == nullptr ||
      session_participant_order == nullptr ||
      savepoint_suffix_ordinals == nullptr)
    return true;

  payload->clear();
  *savepoint_count = 0;
  *innodb_savepoint_count = 0;
  session_participant_order->clear();
  savepoint_suffix_ordinals->clear();

  std::vector<Ha_trx_info *> participant_nodes;
  std::set<uint8_t> participants;
  for (Ha_trx_info *ha_info =
           thd->get_transaction()->ha_trx_info(Transaction_ctx::SESSION);
       ha_info != nullptr; ha_info = ha_info->next()) {
    Preserve_savepoint_participant participant;
    if (ha_legacy_type(ha_info->ht()) == DB_TYPE_INNODB) {
      participant = Preserve_savepoint_participant::INNODB;
    } else if (ha_legacy_type(ha_info->ht()) == DB_TYPE_BINLOG) {
      participant = Preserve_savepoint_participant::BINLOG;
    } else {
      return true;
    }
    const uint8_t raw = static_cast<uint8_t>(participant);
    if (!participants.insert(raw).second) return true;
    participant_nodes.push_back(ha_info);
    session_participant_order->push_back(participant);
  }

  std::vector<SAVEPOINT *> savepoints;
  for (SAVEPOINT *savepoint = thd->get_transaction()->m_savepoints;
       savepoint != nullptr; savepoint = savepoint->prev) {
    savepoints.push_back(savepoint);
  }
  if (savepoints.empty()) return false;
  if (savepoints.size() > UINT32_MAX) return true;

  append_le32(payload, static_cast<uint32_t>(savepoints.size()));
  for (auto it = savepoints.rbegin(); it != savepoints.rend(); ++it) {
    SAVEPOINT *savepoint = *it;
    uint16_t handler_flags = kSavepointHandlerNone;
    Mysql_binlog_preserve_cache_state binlog_cache_state;
    if (savepoint->name == nullptr || savepoint->length == 0 ||
        savepoint->length > NAME_LEN ||
        savepoint_handler_state(savepoint, &handler_flags,
                                &binlog_cache_state, thd)) {
      return true;
    }
    if ((handler_flags & kSavepointHandlerBinlog) != 0 &&
        binlog_state != Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE) {
      return true;
    }
    if ((handler_flags & kSavepointHandlerInnodb) != 0)
      ++*innodb_savepoint_count;

    uint16_t suffix_ordinal =
        static_cast<uint16_t>(participant_nodes.size());
    if (savepoint->ha_list != nullptr) {
      const auto participant_it =
          std::find(participant_nodes.begin(), participant_nodes.end(),
                    savepoint->ha_list);
      if (participant_it == participant_nodes.end()) return true;
      suffix_ordinal = static_cast<uint16_t>(
          std::distance(participant_nodes.begin(), participant_it));
    }
    uint16_t expected_handler_flags = 0;
    for (size_t participant_index = suffix_ordinal;
         participant_index < session_participant_order->size();
         ++participant_index) {
      expected_handler_flags |=
          (*session_participant_order)[participant_index] ==
                  Preserve_savepoint_participant::INNODB
              ? kSavepointHandlerInnodb
              : kSavepointHandlerBinlog;
    }
    if (expected_handler_flags != handler_flags) return true;
    savepoint_suffix_ordinals->push_back(suffix_ordinal);

    uint32_t mdl_stmt_ordinal = 0;
    uint32_t mdl_trans_ordinal = 0;
    if (thd->mdl_context.export_savepoint_ordinals(savepoint->mdl_savepoint,
                                                   &mdl_stmt_ordinal,
                                                   &mdl_trans_ordinal)) {
      return true;
    }

    append_le16(payload, static_cast<uint16_t>(savepoint->length));
    append_le16(payload, handler_flags);
    append_le32(payload, mdl_stmt_ordinal);
    append_le32(payload, mdl_trans_ordinal);
    if ((handler_flags & kSavepointHandlerBinlog) != 0) {
      append_le64(payload, binlog_cache_state.position);
      append_le32(payload,
                  binlog_savepoint_checkpoint_flags(binlog_cache_state));
      append_le64(payload, binlog_cache_state.event_counter);
    }
    payload->append(savepoint->name, savepoint->length);
  }

  *savepoint_count = static_cast<uint32_t>(savepoints.size());
  return false;
}

bool parse_sql_savepoint_entries(const std::string &payload,
                                 uint32_t mdl_descriptor_count,
                                 std::vector<Preserve_sql_savepoint_entry>
                                     *entries) {
  if (entries == nullptr) return true;
  entries->clear();
  uint32_t savepoint_count = 0;
  if (!sql_savepoints_payload_is_valid(payload, &savepoint_count,
                                       mdl_descriptor_count)) {
    return true;
  }
  if (savepoint_count == 0) return false;

  entries->reserve(savepoint_count);
  size_t offset = 4;
  for (uint32_t i = 0; i < savepoint_count; ++i) {
    Preserve_sql_savepoint_entry entry;
    const uint16_t name_length = read_le16(payload, offset);
    entry.handler_flags = read_le16(payload, offset + 2);
    entry.mdl_stmt_ordinal = read_le32(payload, offset + 4);
    entry.mdl_trans_ordinal = read_le32(payload, offset + 8);
    offset += 12;
    if (entry.has_binlog_handler()) {
      entry.binlog_cache_state.position = read_le64(payload, offset);
      if (apply_binlog_savepoint_checkpoint_flags(
              read_le32(payload, offset + 8), &entry.binlog_cache_state)) {
        return true;
      }
      entry.binlog_cache_state.event_counter = read_le64(payload, offset + 12);
      offset += kBinlogSavepointCheckpointLength;
    }
    entry.name.assign(payload.data() + offset, name_length);
    offset += name_length;
    entries->push_back(std::move(entry));
  }

  return offset != payload.length();
}

Ha_trx_info *find_innodb_ha_info(THD *thd) {
  for (Ha_trx_info *ha_info =
           thd->get_transaction()->ha_trx_info(Transaction_ctx::SESSION);
       ha_info != nullptr; ha_info = ha_info->next()) {
    if (ha_legacy_type(ha_info->ht()) == DB_TYPE_INNODB) return ha_info;
  }
  return nullptr;
}

Ha_trx_info *find_binlog_ha_info(THD *thd) {
  for (Ha_trx_info *ha_info =
           thd->get_transaction()->ha_trx_info(Transaction_ctx::SESSION);
       ha_info != nullptr; ha_info = ha_info->next()) {
    if (ha_legacy_type(ha_info->ht()) == DB_TYPE_BINLOG) return ha_info;
  }
  return nullptr;
}

bool restore_session_participant_topology(
    THD *thd, const Preserve_snapshot_metadata &metadata) {
  if (thd == nullptr) return true;

  struct Participant_state {
    Preserve_savepoint_participant participant;
    Ha_trx_info *ha_info;
    handlerton *hton;
    bool read_write;
  };
  std::vector<Participant_state> current;
  std::set<uint8_t> seen;
  for (Ha_trx_info *ha_info =
           thd->get_transaction()->ha_trx_info(Transaction_ctx::SESSION);
       ha_info != nullptr; ha_info = ha_info->next()) {
    Preserve_savepoint_participant participant;
    const legacy_db_type legacy_type = ha_legacy_type(ha_info->ht());
    if (legacy_type == DB_TYPE_INNODB) {
      participant = Preserve_savepoint_participant::INNODB;
    } else if (legacy_type == DB_TYPE_BINLOG) {
      participant = Preserve_savepoint_participant::BINLOG;
    } else {
      return true;
    }
    if (!seen.insert(static_cast<uint8_t>(participant)).second) return true;
    current.push_back(
        {participant, ha_info, ha_info->ht(), ha_info->is_trx_read_write()});
  }
  if (current.size() != metadata.session_participant_order.size()) return true;

  std::vector<Participant_state *> ordered;
  ordered.reserve(metadata.session_participant_order.size());
  for (Preserve_savepoint_participant participant :
       metadata.session_participant_order) {
    const auto it = std::find_if(
        current.begin(), current.end(), [&](const Participant_state &state) {
          return state.participant == participant;
        });
    if (it == current.end()) return true;
    ordered.push_back(&*it);
  }

  Transaction_ctx *trn_ctx = thd->get_transaction();
  for (Participant_state &state : current) state.ha_info->reset();
  trn_ctx->reset_scope(Transaction_ctx::SESSION);

  int read_write_count = 0;
  for (auto it = ordered.rbegin(); it != ordered.rend(); ++it) {
    trans_register_ha(thd, true, (*it)->hton, nullptr);
    Ha_trx_info *restored =
        thd->get_ha_data((*it)->hton->slot)
            ->ha_info + Transaction_ctx::SESSION;
    if ((*it)->read_write) {
      restored->set_trx_read_write();
      ++read_write_count;
    }
  }
  trn_ctx->set_rw_ha_count(Transaction_ctx::SESSION, read_write_count);
  return false;
}

Ha_trx_info *savepoint_ha_list_for_suffix(
    THD *thd, const Preserve_snapshot_metadata &metadata,
    const Preserve_sql_savepoint_entry &entry, uint16_t suffix_ordinal) {
  if (thd == nullptr ||
      suffix_ordinal > metadata.session_participant_order.size()) {
    return nullptr;
  }
  uint16_t expected_flags = kSavepointHandlerNone;
  for (size_t index = suffix_ordinal;
       index < metadata.session_participant_order.size(); ++index) {
    expected_flags |=
        metadata.session_participant_order[index] ==
                Preserve_savepoint_participant::INNODB
            ? kSavepointHandlerInnodb
            : kSavepointHandlerBinlog;
  }
  if (expected_flags != entry.handler_flags) return nullptr;

  Ha_trx_info *suffix =
      thd->get_transaction()->ha_trx_info(Transaction_ctx::SESSION);
  for (uint16_t index = 0; index < suffix_ordinal; ++index) {
    if (suffix == nullptr) return nullptr;
    suffix = suffix->next();
  }
  return suffix;
}

std::string innodb_savepoint_internal_name(SAVEPOINT *savepoint,
                                           Ha_trx_info *innodb_info) {
  char name[64];
  uchar *engine_savepoint =
      pointer_cast<uchar *>(savepoint + 1) + innodb_info->ht()->savepoint_offset;
  longlong2str(static_cast<int64_t>(reinterpret_cast<uintptr_t>(
                   engine_savepoint)),
               name, 36);
  return std::string(name);
}

bool restore_savepoints_to_thd(THD *thd, trx_t *trx,
                               const Preserve_snapshot_metadata &metadata) {
  if (restore_session_participant_topology(thd, metadata)) return true;
  if (metadata.savepoint_count == 0) {
    return trx_preserve_import_savepoints(
               trx, metadata.innodb_savepoints_payload,
               std::vector<std::string>()) != DB_SUCCESS;
  }

  uint32_t mdl_descriptor_count = 0;
  if (!mdl_descriptors_payload_is_valid(metadata.mdl_descriptors_payload,
                                        &mdl_descriptor_count)) {
    return true;
  }

  std::vector<Preserve_sql_savepoint_entry> entries;
  if (parse_sql_savepoint_entries(metadata.sql_savepoints_payload,
                                  mdl_descriptor_count, &entries) ||
      entries.size() != metadata.savepoint_count ||
      metadata.savepoint_suffix_ordinals.size() != entries.size()) {
    return true;
  }

  const bool needs_innodb_info =
      std::any_of(entries.begin(), entries.end(),
                  [](const Preserve_sql_savepoint_entry &entry) {
                    return entry.has_innodb_handler();
                  });
  const bool needs_binlog_info =
      std::any_of(entries.begin(), entries.end(),
                  [](const Preserve_sql_savepoint_entry &entry) {
                    return entry.has_binlog_handler();
                  });
  Ha_trx_info *innodb_info = needs_innodb_info ? find_innodb_ha_info(thd)
                                               : nullptr;
  Ha_trx_info *binlog_info = needs_binlog_info ? find_binlog_ha_info(thd)
                                               : nullptr;
  if (needs_innodb_info && innodb_info == nullptr) return true;
  if (needs_binlog_info && binlog_info == nullptr) return true;

  std::vector<std::string> innodb_savepoint_names;
  innodb_savepoint_names.reserve(entries.size());

  Transaction_ctx *trn_ctx = thd->get_transaction();
  trn_ctx->m_savepoints = nullptr;
  SAVEPOINT *restored_savepoints = nullptr;
  for (size_t entry_index = 0; entry_index < entries.size(); ++entry_index) {
    const Preserve_sql_savepoint_entry &entry = entries[entry_index];
    SAVEPOINT *savepoint = static_cast<SAVEPOINT *>(
        trn_ctx->allocate_memory(savepoint_alloc_size));
    if (savepoint == nullptr) return true;

    savepoint->name = trn_ctx->strmake(entry.name.data(), entry.name.length());
    if (savepoint->name == nullptr) return true;
    savepoint->length = entry.name.length();
    savepoint->ha_list = savepoint_ha_list_for_suffix(
        thd, metadata, entry,
        metadata.savepoint_suffix_ordinals[entry_index]);
    if ((entry.handler_flags != kSavepointHandlerNone) &&
        savepoint->ha_list == nullptr) {
      return true;
    }
    if (entry.has_binlog_handler()) {
      my_off_t *binlog_position = reinterpret_cast<my_off_t *>(
          pointer_cast<uchar *>(savepoint + 1) +
          binlog_info->ht()->savepoint_offset);
      *binlog_position =
          static_cast<my_off_t>(entry.binlog_cache_state.position);
      if (mysql_binlog_preserve_import_cache_state(
              thd, entry.binlog_cache_state)) {
        return true;
      }
    }
    if (thd->mdl_context.savepoint_from_ordinals(
            entry.mdl_stmt_ordinal, entry.mdl_trans_ordinal,
            &savepoint->mdl_savepoint)) {
      return true;
    }

    if (entry.has_innodb_handler()) {
      innodb_savepoint_names.push_back(
          innodb_savepoint_internal_name(savepoint, innodb_info));
    }
    savepoint->prev = restored_savepoints;
    restored_savepoints = savepoint;
  }

  DBUG_EXECUTE_IF("preserve_trx_fail_restore_savepoints_after_sql",
                  return true;);

  if (trx_preserve_import_savepoints(trx, metadata.innodb_savepoints_payload,
                                     innodb_savepoint_names) != DB_SUCCESS) {
    return true;
  }

  trn_ctx->m_savepoints = restored_savepoints;
  return false;
}

bool store_preserved_trx_row(Protocol *protocol,
                             const Preserved_trx_view_row &row) {
  protocol->start_row();

  protocol->store_string(row.token.c_str(), row.token.length(),
                         system_charset_info);
  protocol->store_string(row.user.c_str(), row.user.length(),
                         system_charset_info);
  protocol->store_string(row.host.c_str(), row.host.length(),
                         system_charset_info);
  protocol->store_string(row.state.c_str(), row.state.length(),
                         system_charset_info);
  if (row.created_at.empty())
    protocol->store_null();
  else
    protocol->store_string(row.created_at.c_str(), row.created_at.length(),
                           system_charset_info);
  if (row.expires_at.empty())
    protocol->store_null();
  else
    protocol->store_string(row.expires_at.c_str(), row.expires_at.length(),
                           system_charset_info);
  protocol->store(row.recovered_count);
  protocol->store(row.age_seconds);
  if (row.schema_name.empty())
    protocol->store_null();
  else
    protocol->store_string(row.schema_name.c_str(), row.schema_name.length(),
                           system_charset_info);
  protocol->store_string(row.isolation.c_str(), row.isolation.length(),
                         system_charset_info);
  protocol->store(row.mod_tables_count);
  if (row.locks_count_valid)
    protocol->store(row.locks_count);
  else
    protocol->store_null();
  protocol->store(row.has_read_view ? "YES" : "NO", system_charset_info);
  protocol->store(row.rv_low_limit_no);
  protocol->store(row.savepoint_count);
  protocol->store_string(row.binlog_state.c_str(), row.binlog_state.length(),
                         system_charset_info);
  protocol->store(row.wrote_to_cache ? "YES" : "NO", system_charset_info);
  protocol->store(row.binlog_cache_size);
  protocol->store_string(row.binlog_warmcopy_state.c_str(),
                         row.binlog_warmcopy_state.length(),
                         system_charset_info);
  protocol->store(row.session_sql_log_bin ? "ON" : "OFF",
                  system_charset_info);
  protocol->store(row.global_log_bin ? "ON" : "OFF", system_charset_info);
  if (row.gtid_next.empty())
    protocol->store_null();
  else
    protocol->store_string(row.gtid_next.c_str(), row.gtid_next.length(),
                           system_charset_info);
  protocol->store(row.autoinc_lock_owned ? "YES" : "NO", system_charset_info);
  protocol->store_string(row.temp_table_state.c_str(),
                         row.temp_table_state.length(), system_charset_info);
  protocol->store(row.temp_image_bytes);
  protocol->store(row.temp_undo_bytes);
  protocol->store(row.temp_sidecars_complete ? "YES" : "NO",
                  system_charset_info);
  if (row.last_error.empty())
    protocol->store_null();
  else
    protocol->store_string(row.last_error.c_str(), row.last_error.length(),
                           system_charset_info);
  if (row.last_error_at.empty())
    protocol->store_null();
  else
    protocol->store_string(row.last_error_at.c_str(),
                           row.last_error_at.length(), system_charset_info);

  return protocol->end_row();
}

bool preserved_trx_show_column_is_unsigned_integer(const char *name) {
  return strcmp(name, "RECOVERED_COUNT") == 0 ||
         strcmp(name, "AGE_SECONDS") == 0 ||
         strcmp(name, "MOD_TABLES_COUNT") == 0 ||
         strcmp(name, "LOCKS_COUNT") == 0 ||
         strcmp(name, "RV_LOW_LIMIT_NO") == 0 ||
         strcmp(name, "SAVEPOINT_COUNT") == 0 ||
         strcmp(name, "BINLOG_CACHE_SIZE") == 0 ||
         strcmp(name, "TEMP_IMAGE_BYTES") == 0 ||
         strcmp(name, "TEMP_UNDO_BYTES") == 0;
}

bool string_eq_lex_cstring(const std::string &lhs, LEX_CSTRING rhs) {
  return rhs.str != nullptr && lhs.length() == rhs.length &&
         memcmp(lhs.c_str(), rhs.str, rhs.length) == 0;
}

static std::string redacted_preserved_trx_log_subject(
    const std::string &token) {
  return "Preserved transaction '" + preserved_trx_redacted_token(token) + "'";
}

bool preserve_trx_reject_unsupported() {
  my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
  return true;
}

static bool preserve_trx_reject_batch_cleanup_failed() {
  my_error(ER_PRESERVE_TRX_BATCH_CLEANUP_FAILED, MYF(0));
  return true;
}

static bool preserve_trx_reject_drain_reset() {
  my_error(ER_PRESERVE_TRX_DRAIN_RESET, MYF(0));
  return true;
}

static bool preserve_trx_reject_drain_timeout() {
  my_error(ER_PRESERVE_TRX_DRAIN_TIMEOUT, MYF(0));
  return true;
}

void log_redacted_resume_failure(const std::string &token, const char *reason) {
  std::string message = "RESUME PRESERVED TRANSACTION '" +
                        preserved_trx_redacted_token(token) + "' failed";
  if (reason != nullptr && reason[0] != '\0') {
    message.append(": ");
    message.append(reason);
  }
  LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
}

void audit_preserved_trx_event(THD *thd, const std::string &token,
                               const char *action, const char *result,
                               const char *reason = nullptr) {
  const std::string redacted_token = preserved_trx_redacted_token(token);
  mysql_event_message_key_value_t key_values[4]{};
  size_t key_value_count = 0;

  auto add_string_key_value = [&](size_t index, const char *key,
                                  const char *value, size_t value_length) {
    key_values[index].key = {key, strlen(key)};
    key_values[index].value_type = MYSQL_AUDIT_MESSAGE_VALUE_TYPE_STR;
    key_values[index].value.str = {value, value_length};
  };

  add_string_key_value(key_value_count++, "action", action, strlen(action));
  add_string_key_value(key_value_count++, "token", redacted_token.c_str(),
                       redacted_token.length());
  add_string_key_value(key_value_count++, "result", result, strlen(result));
  if (reason != nullptr && reason[0] != '\0') {
    add_string_key_value(key_value_count++, "reason", reason, strlen(reason));
  }

  (void)mysql_audit_notify(
      thd, AUDIT_EVENT(MYSQL_AUDIT_MESSAGE_INTERNAL), STRING_WITH_LEN("mysqld"),
      STRING_WITH_LEN("preserve_trx"),
      STRING_WITH_LEN("preserved_transaction"), key_values, key_value_count);
}

void audit_preserved_trx_control_event(THD *thd, const char *action,
                                       const char *result,
                                       longlong target_count,
                                       longlong token_count) {
  mysql_event_message_key_value_t key_values[4]{};
  size_t key_value_count = 0;

  auto add_string_key_value = [&](size_t index, const char *key,
                                  const char *value, size_t value_length) {
    key_values[index].key = {key, strlen(key)};
    key_values[index].value_type = MYSQL_AUDIT_MESSAGE_VALUE_TYPE_STR;
    key_values[index].value.str = {value, value_length};
  };
  auto add_numeric_key_value = [&](size_t index, const char *key,
                                   longlong value) {
    key_values[index].key = {key, strlen(key)};
    key_values[index].value_type = MYSQL_AUDIT_MESSAGE_VALUE_TYPE_NUM;
    key_values[index].value.num = value;
  };

  add_string_key_value(key_value_count++, "action", action, strlen(action));
  add_string_key_value(key_value_count++, "result", result, strlen(result));
  add_numeric_key_value(key_value_count++, "target_count", target_count);
  add_numeric_key_value(key_value_count++, "token_count", token_count);

  (void)mysql_audit_notify(
      thd, AUDIT_EVENT(MYSQL_AUDIT_MESSAGE_INTERNAL), STRING_WITH_LEN("mysqld"),
      STRING_WITH_LEN("preserve_trx"),
      STRING_WITH_LEN("preserved_transaction_control"), key_values,
      key_value_count);
}

bool preserve_trx_participant_type_is_supported(
    legacy_db_type legacy_type, Preserve_snapshot_binlog_state binlog_state) {
  if (legacy_type == DB_TYPE_INNODB) return true;
  return legacy_type == DB_TYPE_BINLOG &&
         (binlog_state == Preserve_snapshot_binlog_state::LOGGED_EMPTY ||
          binlog_state == Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE);
}

bool preserve_trx_has_only_supported_transaction_engines(
    THD *thd, Preserve_snapshot_binlog_state binlog_state) {
  if (thd == nullptr) return false;
  for (Transaction_ctx::enum_trx_scope scope :
       {Transaction_ctx::SESSION, Transaction_ctx::STMT}) {
    for (Ha_trx_info *ha_info =
             thd->get_transaction()->ha_trx_info(scope);
         ha_info != nullptr; ha_info = ha_info->next()) {
      /*
        Read-only participants still own callback state and can affect prepare,
        rollback-to-savepoint, or cleanup. Never omit them merely because the
        TRX_READ_WRITE bit is clear.
      */
      if (!preserve_trx_participant_type_is_supported(
              ha_legacy_type(ha_info->ht()), binlog_state)) {
        return false;
      }
    }
  }
  return true;
}

bool preserve_trx_has_active_multi_stmt_transaction(THD *thd) {
  return thd != nullptr && thd->in_active_multi_stmt_transaction() &&
         thd->in_multi_stmt_transaction_mode();
}

bool preserve_trx_has_rw_transaction_participant(THD *thd) {
  if (thd == nullptr) return false;
  for (Ha_trx_info *ha_info =
           thd->get_transaction()->ha_trx_info(Transaction_ctx::SESSION);
       ha_info != nullptr; ha_info = ha_info->next()) {
    if (ha_info->is_trx_read_write()) return true;
  }
  return false;
}

static bool preserve_trx_has_lock_warmcopy_phase1_candidate_transaction(
    THD *thd) {
  if (thd == nullptr) return false;
  if (preserve_trx_has_active_multi_stmt_transaction(thd)) return true;

  /*
    Phase-1 lock warmcopy is a best-effort prebuild pass that runs before the
    final batch target set is frozen. At command-read and debug-sync packet
    boundaries SQL transaction flags can be narrower than the engine state that
    will later be preserved, while InnoDB still has record locks attached to
    the session transaction. Use engine-side evidence for the prebuild
    candidate only; the actual drain target selection below remains unchanged.
  */
  return preserve_trx_has_rw_transaction_participant(thd) ||
         trx_preserve_current_thd_has_record_locks(thd);
}

static bool preserve_trx_has_configured_replica_channel() {
  channel_map.rdlock();
  bool has_channel = false;
  for (mi_map::iterator it = channel_map.begin(); it != channel_map.end();
       ++it) {
    if (Master_info::is_configured(it->second)) {
      has_channel = true;
      break;
    }
  }
  channel_map.unlock();
  return has_channel;
}

bool preserve_trx_is_unsupported_common_context(
    THD *thd, bool allow_inflight_command_context = false) {
  DBUG_EXECUTE_IF("preserve_trx_simulate_replication_context", return true;);
  DBUG_EXECUTE_IF("preserve_trx_simulate_group_replication_context",
                  return true;);
  DBUG_EXECUTE_IF("preserve_trx_simulate_server_upgrade_context",
                  return true;);
  if (!thd->is_classic_protocol()) {
    return true;
  }
  if (opt_initialize || thd->is_bootstrap_system_thread() ||
      thd->is_server_upgrade_thread())
    return true;
  if (srv_force_recovery > 0) {
    return true;
  }
  if (thd->slave_thread || thd->rli_slave != nullptr ||
      thd->is_binlog_applier() || is_group_replication_running() ||
      preserve_trx_has_configured_replica_channel()) {
    return true;
  }
  if (!thd->get_transaction()->xid_state()->has_state(XID_STATE::XA_NOTR)) {
    return true;
  }
  if (thd_is_dd_update_stmt(thd)) {
    return true;
  }
  if (thd->temporary_tables != nullptr &&
      !preserve_trx_temp_table_session_supported(thd)) {
    return true;
  }
  if (thd->global_read_lock.is_acquired()) {
    return true;
  }
  if (is_instance_backup_locked(thd) !=
      Is_instance_backup_locked_result::NOT_LOCKED) {
    return true;
  }
  if (thd->locked_tables_mode != LTM_NONE) {
    return true;
  }
  if (!thd->ull_hash.empty()) {
    return true;
  }
  if (!thd->handler_tables_hash.empty()) {
    return true;
  }
  if (thd->stmt_map.has_open_server_side_cursor()) {
    return true;
  }
  if (!allow_inflight_command_context &&
      (thd->in_sub_stmt != 0 || thd->sp_runtime_ctx != nullptr)) {
    return true;
  }

  return false;
}

bool preserve_trx_has_unsupported_transaction_contents(
    THD *thd, Preserve_snapshot_binlog_state binlog_state) {
  if (thd->get_transaction()->has_modified_non_trans_table(
          Transaction_ctx::SESSION)) {
    return true;
  }
  if (thd->get_transaction()->has_modified_non_trans_table(
          Transaction_ctx::STMT)) {
    return true;
  }
  if (!preserve_trx_has_only_supported_transaction_engines(thd,
                                                            binlog_state)) {
    return true;
  }

  return false;
}

bool preserve_trx_has_resume_any_privilege(THD *thd) {
  return thd->security_context()
      ->has_global_grant(STRING_WITH_LEN("RESUME_ANY_PRESERVED_TRANSACTION"))
      .first;
}

bool preserve_trx_table_ref_uses_locking_read(Table_ref *table) {
  for (; table != nullptr; table = table->next_local) {
    const thr_lock_type lock_type = table->lock_descriptor().type;
    if (lock_type != TL_READ_DEFAULT && lock_type != TL_UNLOCK) return true;
    if (table->nested_join != nullptr) {
      for (Table_ref *nested_table : table->nested_join->join_list) {
        if (preserve_trx_table_ref_uses_locking_read(nested_table))
          return true;
      }
    }
  }
  return false;
}

bool preserve_trx_select_uses_locking_read(LEX *lex) {
  if (lex == nullptr) return false;
  for (SELECT_LEX *query_block = lex->all_selects_list;
       query_block != nullptr; query_block = query_block->next_select_in_list()) {
    if (preserve_trx_table_ref_uses_locking_read(
            query_block->get_table_list())) {
      return true;
    }
  }
  return false;
}

bool preserve_trx_transaction_sysvar_name(const char *name) {
  return name != nullptr &&
         (strcmp(name, "autocommit") == 0 ||
          strcmp(name, "transaction_isolation") == 0 ||
          strcmp(name, "transaction_read_only") == 0 ||
          strcmp(name, "tx_isolation") == 0 ||
          strcmp(name, "tx_read_only") == 0);
}

bool preserve_trx_set_option_changes_transaction_semantics(LEX *lex) {
  if (lex == nullptr) return false;
  List_iterator_fast<set_var_base> it(lex->var_list);
  set_var_base *var;
  while ((var = it++)) {
    const set_var *sysvar = dynamic_cast<const set_var *>(var);
    if (sysvar != nullptr && sysvar->var != nullptr &&
        preserve_trx_transaction_sysvar_name(sysvar->var->name.str)) {
      return true;
    }
  }
  return false;
}

bool preserve_trx_sql_command_may_create_trx_or_lock(
    LEX *lex, enum_sql_command sql_command) {
  if (sql_command == SQLCOM_PREPARE) return false;

  if (preserve_trx_select_uses_locking_read(lex))
    return true;

  switch (sql_command) {
    case SQLCOM_FLUSH:
      return lex != nullptr &&
             (lex->type & (REFRESH_READ_LOCK | REFRESH_FOR_EXPORT |
                           REFRESH_BINARY_LOG | REFRESH_LOG));
    case SQLCOM_RESET:
      return lex != nullptr && (lex->type & REFRESH_MASTER);
    case SQLCOM_SET_OPTION:
      return preserve_trx_set_option_changes_transaction_semantics(lex);
    case SQLCOM_BEGIN:
    case SQLCOM_SAVEPOINT:
    case SQLCOM_RELEASE_SAVEPOINT:
    case SQLCOM_ROLLBACK_TO_SAVEPOINT:
    case SQLCOM_XA_START:
    case SQLCOM_XA_PREPARE:
    case SQLCOM_XA_COMMIT:
    case SQLCOM_XA_ROLLBACK:
    case SQLCOM_EXECUTE:
    case SQLCOM_CALL:
    case SQLCOM_LOCK_TABLES:
    case SQLCOM_LOCK_INSTANCE:
    case SQLCOM_RESUME_PRESERVED_TRX:
    case SQLCOM_DRAIN_TRANSACTIONS_PRESERVE:
      return true;
    default:
      break;
  }

  return (sql_command_flags[sql_command] & CF_CHANGES_DATA) != 0;
}

bool preserve_trx_sql_command_may_create_trx_or_lock(
    THD *thd, enum_sql_command sql_command) {
  return preserve_trx_sql_command_may_create_trx_or_lock(
      thd != nullptr ? thd->lex : nullptr, sql_command);
}

bool preserve_trx_sql_command_is_preserve_control_or_shutdown(
    enum_sql_command sql_command) {
  switch (sql_command) {
    case SQLCOM_DRAIN_TRANSACTIONS_PRESERVE:
    case SQLCOM_RESET_DRAIN:
    case SQLCOM_RESUME_PRESERVED_TRX:
    case SQLCOM_SHUTDOWN:
      return true;
    default:
      return false;
  }
}

bool preserve_trx_protocol_command_may_create_trx_or_lock(
    enum enum_server_command command) {
  switch (command) {
    case COM_FIELD_LIST:
    case COM_REFRESH:
      return true;
    default:
      return false;
  }
}

static bool preserve_trx_is_ha_admin_account(THD *thd) {
  static constexpr char kHa_admin_user[] = "preserve_trx_ha_admin";
  if (thd == nullptr || thd->security_context() == nullptr) return false;

  const LEX_CSTRING user = thd->security_context()->priv_user();
  return user.length == sizeof(kHa_admin_user) - 1 &&
         memcmp(user.str, kHa_admin_user, sizeof(kHa_admin_user) - 1) == 0;
}

static bool preserve_trx_is_ha_control_connection(THD *thd) {
  return preserve_trx_is_ha_admin_account(thd) &&
         (thd->security_context()->master_access() & SHUTDOWN_ACL) != 0;
}

static bool preserve_trx_closing_command_gate_active(
    Preserve_trx_manager_state state) {
  if (state == Preserve_trx_manager_state::WARMCOPY_CLOSING ||
      state == Preserve_trx_manager_state::TRANSFER_HANDOFF_COMPLETE) {
    return true;
  }
  if (state != Preserve_trx_manager_state::BATCH_DRAINING) return false;

  const std::shared_ptr<Preserve_trx_drain_attempt> attempt =
      preserve_trx_active_drain_attempt_snapshot();
  return attempt != nullptr &&
         attempt->closing_command_gate_published.load(
             std::memory_order_acquire);
}

static bool preserve_trx_protocol_command_is_no_response_cleanup(
    enum enum_server_command command) {
  return command == COM_QUIT || command == COM_STMT_CLOSE;
}

struct Preserve_trx_phase1_readiness_metrics {
  uint64_t samples{0};
  uint64_t inflight_commands{0};
  uint64_t oldest_command_age_us{0};
  uint64_t offender_count{0};
  uint64_t wait_us{0};
};

struct Preserve_trx_phase1_command_identity {
  THD *thd{nullptr};
  my_thread_id thread_id{0};
  uint64_t sequence{0};
  std::unique_ptr<Preserve_trx_external_thd_pin> pin;
};

class Preserve_trx_phase1_readiness_collector final : public Do_THD_Impl {
 public:
  Preserve_trx_phase1_readiness_collector(THD *owner, ulonglong sampled_us,
                                          ulonglong long_command_age_us)
      : m_owner(owner),
        m_sampled_us(sampled_us),
        m_long_command_age_us(long_command_age_us) {}

  void operator()(THD *candidate) override {
    if (candidate == nullptr || candidate == m_owner) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    const ulonglong started_us =
        candidate->preserve_trx_command_started_monotonic_us;
    const bool active = !candidate->release_resources_done() &&
                        candidate->killed != THD::KILL_CONNECTION &&
                        !candidate->is_system_thread() &&
                        !preserve_trx_is_ha_control_connection(candidate) &&
                        started_us != 0;
    if (!active) {
      mysql_mutex_unlock(&candidate->LOCK_thd_data);
      return;
    }

    ++m_metrics.inflight_commands;
    const ulonglong age_us =
        m_sampled_us >= started_us ? m_sampled_us - started_us : 0;
    m_metrics.oldest_command_age_us =
        std::max(m_metrics.oldest_command_age_us,
                 static_cast<uint64_t>(age_us));
    if (age_us < m_long_command_age_us) {
      mysql_mutex_unlock(&candidate->LOCK_thd_data);
      return;
    }

    std::unique_ptr<Preserve_trx_external_thd_pin> pin =
        Preserve_trx_external_thd_pin::acquire_locked(candidate);
    if (pin != nullptr) {
      m_identities.push_back(
          {candidate, candidate->thread_id(),
           candidate->preserve_trx_command_sequence, std::move(pin)});
      ++m_metrics.offender_count;
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

  Preserve_trx_phase1_readiness_metrics &metrics() { return m_metrics; }
  std::vector<Preserve_trx_phase1_command_identity> &identities() {
    return m_identities;
  }

 private:
  THD *m_owner;
  ulonglong m_sampled_us;
  ulonglong m_long_command_age_us;
  Preserve_trx_phase1_readiness_metrics m_metrics;
  std::vector<Preserve_trx_phase1_command_identity> m_identities;
};

enum class Preserve_trx_phase1_readiness_result {
  READY,
  DEADLINE,
  RESET_REQUESTED,
  OWNER_KILLED,
  PROGRESS_FAILED
};

static bool preserve_trx_phase1_command_still_active(
    const Preserve_trx_phase1_command_identity &identity) {
  THD *candidate = identity.thd;
  if (candidate == nullptr) return false;

  mysql_mutex_lock(&candidate->LOCK_thd_data);
  const bool active = candidate->thread_id() == identity.thread_id &&
                      !candidate->release_resources_done() &&
                      candidate->killed != THD::KILL_CONNECTION &&
                      candidate->preserve_trx_command_sequence ==
                          identity.sequence &&
                      candidate->preserve_trx_command_started_monotonic_us != 0;
  mysql_mutex_unlock(&candidate->LOCK_thd_data);
  return active;
}

static Preserve_trx_phase1_readiness_result
preserve_trx_wait_for_phase1_readiness(
    THD *owner, ulonglong phase1_deadline_us, ulonglong long_command_age_us,
    const std::shared_ptr<Preserve_trx_drain_attempt> &active_drain_attempt,
    Preserve_trx_phase1_readiness_metrics *metrics,
    const std::function<bool()> &progress = {}) {
  assert(owner != nullptr);
  assert(metrics != nullptr);

  DEBUG_SYNC(owner, "preserve_trx_phase1_readiness_before_wait");
  if (preserve_trx_active_drain_reset_requested(active_drain_attempt))
    return Preserve_trx_phase1_readiness_result::RESET_REQUESTED;
  if (owner->killed) return Preserve_trx_phase1_readiness_result::OWNER_KILLED;

  const ulonglong sampled_us = preserve_trx_monotonic_us();
  if (preserve_trx_monotonic_deadline_expired_at(phase1_deadline_us,
                                                 sampled_us)) {
    return Preserve_trx_phase1_readiness_result::DEADLINE;
  }
  Preserve_trx_phase1_readiness_collector collector(
      owner, sampled_us, long_command_age_us);
  Global_THD_manager::get_instance()->do_for_all_thd_copy(&collector);
  *metrics = collector.metrics();
  ++metrics->samples;
  DEBUG_SYNC(owner, "preserve_trx_phase1_readiness_after_sample");

  std::vector<Preserve_trx_phase1_command_identity> &identities =
      collector.identities();
  if (identities.empty()) return Preserve_trx_phase1_readiness_result::READY;

  const ulonglong wait_started_us = preserve_trx_monotonic_us();
  auto note_wait = create_scope_guard([&] {
    const ulonglong now_us = preserve_trx_monotonic_us();
    metrics->wait_us = now_us >= wait_started_us ? now_us - wait_started_us : 0;
  });
  for (;;) {
    if (preserve_trx_active_drain_reset_requested(active_drain_attempt))
      return Preserve_trx_phase1_readiness_result::RESET_REQUESTED;
    if (owner->killed) return Preserve_trx_phase1_readiness_result::OWNER_KILLED;
    const ulonglong now_us = preserve_trx_monotonic_us();
    if (preserve_trx_monotonic_deadline_expired_at(phase1_deadline_us,
                                                   now_us)) {
      return Preserve_trx_phase1_readiness_result::DEADLINE;
    }
    if (progress && progress()) {
      return Preserve_trx_phase1_readiness_result::PROGRESS_FAILED;
    }

    bool offender_active = false;
    for (const Preserve_trx_phase1_command_identity &identity : identities) {
      if (preserve_trx_phase1_command_still_active(identity)) {
        offender_active = true;
        break;
      }
    }
    ++metrics->samples;
    if (!offender_active) return Preserve_trx_phase1_readiness_result::READY;

    DEBUG_SYNC(owner, "preserve_trx_phase1_readiness_before_poll_sleep");
    my_sleep(10000);
  }
}

bool preserve_trx_thd_has_batch_inflight_statement(THD *candidate) {
  return candidate != nullptr &&
         (candidate->preserve_trx_inflight_risky_statement_depth > 0 ||
          candidate->preserve_trx_inflight_unknown_query_depth > 0);
}

Preserve_trx_batch_thd_state preserve_trx_batch_state(THD *thd) {
  if (thd == nullptr) return Preserve_trx_batch_thd_state::NONE;
  mysql_mutex_lock(&thd->LOCK_thd_data);
  const Preserve_trx_batch_thd_state state = thd->preserve_trx_batch_state;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
  return state;
}

bool preserve_trx_batch_state_blocks_target_owner(
    Preserve_trx_batch_thd_state state) {
  return state == Preserve_trx_batch_thd_state::QUIESCED ||
         state == Preserve_trx_batch_thd_state::ATTACHING;
}

void preserve_trx_set_batch_state(THD *thd, ulonglong generation,
                                  Preserve_trx_batch_thd_state state) {
  if (thd == nullptr) return;
  mysql_mutex_lock(&thd->LOCK_thd_data);
  thd->preserve_trx_batch_generation = generation;
  thd->preserve_trx_batch_state = state;
  if (generation == 0 || state == Preserve_trx_batch_thd_state::NONE) {
    thd->preserve_trx_quiesce_boundary_monotonic_us = 0;
    thd->preserve_trx_temp_table_batch_capture_epoch.store(
        false, std::memory_order_release);
    preserve_trx_temp_table_clear_batch_unsupported_boundary(thd);
  } else {
    thd->preserve_trx_temp_table_batch_capture_epoch.store(
        true, std::memory_order_release);
  }
  mysql_mutex_unlock(&thd->LOCK_thd_data);
}

bool preserve_trx_batch_candidate_is_idle_target(THD *owner, THD *candidate) {
  if (candidate == nullptr || candidate == owner ||
      candidate->release_resources_done())
    return false;
  if (candidate->preserve_trx_batch_state !=
      Preserve_trx_batch_thd_state::NONE)
    return false;
  if (preserve_trx_is_unsupported_common_context(candidate)) return false;
  if (candidate->killed != THD::NOT_KILLED) return false;
  if (!candidate->m_server_idle) return false;
  if (!preserve_trx_has_active_multi_stmt_transaction(candidate)) return false;
  return true;
}

class Preserve_batch_target_counter final : public Do_THD_Impl {
 public:
  Preserve_batch_target_counter(THD *owner, ulonglong generation,
                                ulonglong closing_started_us)
      : m_owner(owner),
        m_generation(generation),
        m_closing_started_us(closing_started_us) {}

  void operator()(THD *candidate) override {
    if (candidate == nullptr) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    /*
      KILL_CONNECTION sessions are already on the connection teardown path and
      cannot receive a preserved token. Treat them like disposing THDs: their
      own cleanup/rollback owns the transaction outcome while the batch drains
      still-live sessions.
    */
    const bool ignored = candidate == m_owner ||
                         candidate->release_resources_done() ||
                         candidate->killed == THD::KILL_CONNECTION;
    if (!ignored) {
      if (candidate->is_system_thread()) {
        if (preserve_trx_has_rw_transaction_participant(candidate) ||
            preserve_trx_thd_has_batch_inflight_statement(candidate)) {
          m_has_unsupported_transaction = true;
        }
        mysql_mutex_unlock(&candidate->LOCK_thd_data);
        return;
      }

      const bool active_multi_stmt_transaction =
          preserve_trx_has_active_multi_stmt_transaction(candidate);
      const bool command_packet_before_closing =
          candidate->preserve_trx_command_packet_before_closing.load(
              std::memory_order_acquire);
      const bool batch_inflight_statement =
          candidate->preserve_trx_inflight_risky_statement_depth > 0 ||
          (candidate->preserve_trx_inflight_unknown_query_depth > 0 &&
           command_packet_before_closing);
      if (preserve_trx_is_ha_control_connection(candidate)) {
        if (active_multi_stmt_transaction ||
            preserve_trx_has_rw_transaction_participant(candidate)) {
          m_has_unsupported_transaction = true;
        }
        mysql_mutex_unlock(&candidate->LOCK_thd_data);
        return;
      }
      const bool nonidle_multi_stmt_transaction =
          active_multi_stmt_transaction && !candidate->m_server_idle;
      const bool nonidle_unclassified_command_packet =
          command_packet_before_closing && candidate->is_classic_protocol();
      if (!active_multi_stmt_transaction && !batch_inflight_statement &&
          !nonidle_unclassified_command_packet) {
        mysql_mutex_unlock(&candidate->LOCK_thd_data);
        return;
      }

      /*
        Batch drain classifies sessions before it starts waiting. Idle active
        multi-statement transactions can publish QUIESCED immediately; sessions
        that are inside a command or have an unclassified classic packet are
        admitted as pending targets and must publish their final state at the
        command boundary.
        Unsupported or unstable sessions fail the whole batch instead of being
        silently skipped.
      */
      if (active_multi_stmt_transaction) ++m_transaction_count;
      const bool unstable_unsupported =
          candidate->preserve_trx_batch_state !=
              Preserve_trx_batch_thd_state::NONE ||
          candidate->killed != THD::NOT_KILLED;
      const bool idle_unsupported =
          active_multi_stmt_transaction && !batch_inflight_statement &&
          preserve_trx_is_unsupported_common_context(candidate);
      const bool unsupported = unstable_unsupported || idle_unsupported;
      const bool idle_target =
          !unsupported && !command_packet_before_closing &&
          preserve_trx_batch_candidate_is_idle_target(m_owner, candidate);
      const bool pending_target =
          !unsupported && !idle_target &&
          (batch_inflight_statement || nonidle_multi_stmt_transaction ||
           nonidle_unclassified_command_packet);

      if (unsupported) {
        m_has_unsupported_transaction = true;
      } else if (idle_target) {
        ++m_target_count;
        m_target_thread_ids.push_back(candidate->thread_id());
        m_transaction_target_thread_ids.push_back(candidate->thread_id());
        candidate->preserve_trx_batch_generation = m_generation;
        candidate->preserve_trx_batch_state =
            Preserve_trx_batch_thd_state::QUIESCED;
        candidate->preserve_trx_quiesce_boundary_monotonic_us =
            m_closing_started_us;
        candidate->preserve_trx_temp_table_batch_capture_epoch.store(
            true, std::memory_order_release);
      } else if (pending_target) {
        ++m_target_count;
        ++m_pending_target_count;
        m_target_thread_ids.push_back(candidate->thread_id());
        if (active_multi_stmt_transaction) {
          m_transaction_target_thread_ids.push_back(candidate->thread_id());
        }
        candidate->preserve_trx_batch_generation = m_generation;
        candidate->preserve_trx_batch_state =
            Preserve_trx_batch_thd_state::PENDING_QUIESCE;
        candidate->preserve_trx_quiesce_boundary_monotonic_us = 0;
        candidate->preserve_trx_temp_table_batch_capture_epoch.store(
            true, std::memory_order_release);
      } else {
        ++m_nonidle_transaction_count;
      }
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

  uint transaction_count() const { return m_transaction_count; }
  uint target_count() const { return m_target_count; }
  uint pending_target_count() const { return m_pending_target_count; }
  uint nonidle_transaction_count() const { return m_nonidle_transaction_count; }
  bool has_unsupported_transaction() const {
    return m_has_unsupported_transaction;
  }
  const std::vector<my_thread_id> &target_thread_ids() const {
    return m_target_thread_ids;
  }
  const std::vector<my_thread_id> &transaction_target_thread_ids() const {
    return m_transaction_target_thread_ids;
  }
 private:
  THD *m_owner;
  ulonglong m_generation;
  ulonglong m_closing_started_us;
  uint m_transaction_count{0};
  uint m_target_count{0};
  uint m_pending_target_count{0};
  uint m_nonidle_transaction_count{0};
  bool m_has_unsupported_transaction{false};
  std::vector<my_thread_id> m_target_thread_ids;
  std::vector<my_thread_id> m_transaction_target_thread_ids;
};

class Preserve_batch_phase1_transfer_target_scanner final : public Do_THD_Impl {
 public:
  explicit Preserve_batch_phase1_transfer_target_scanner(THD *owner)
      : m_owner(owner) {}

  void operator()(THD *candidate) override {
    if (candidate == nullptr) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    const bool ignored = candidate == m_owner ||
                         candidate->release_resources_done() ||
                         candidate->killed == THD::KILL_CONNECTION ||
                         candidate->is_system_thread();
    if (!ignored) {
      const bool active_multi_stmt_transaction =
          preserve_trx_has_active_multi_stmt_transaction(candidate);
      const bool batch_inflight_statement =
          preserve_trx_thd_has_batch_inflight_statement(candidate);
      const bool nonidle_unclassified_command_packet =
          !candidate->m_server_idle && candidate->is_classic_protocol();
      /*
        Standby streaming transfer publishes token identity during phase 1 so
        the receiver can create current-process saved state before the blocking
        phase starts. This scan intentionally does not mutate the THD batch
        state; the later target counter remains the authoritative
        quiesce/admission decision and any token not in the final target set is
        explicitly aborted.
      */
      if (active_multi_stmt_transaction || batch_inflight_statement ||
          nonidle_unclassified_command_packet) {
        m_target_thread_ids.push_back(candidate->thread_id());
      }
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

  const std::vector<my_thread_id> &target_thread_ids() const {
    return m_target_thread_ids;
  }

 private:
  THD *m_owner;
  std::vector<my_thread_id> m_target_thread_ids;
};

class Preserve_batch_phase1_declared_target_pin_collector final
    : public Do_THD_Impl {
 public:
  struct Target {
    THD *thd{nullptr};
    std::unique_ptr<Preserve_trx_external_thd_pin> pin;
    bool idle{false};
  };

  Preserve_batch_phase1_declared_target_pin_collector(
      THD *owner, const std::set<my_thread_id> &declared_tokens)
      : m_owner(owner), m_declared_tokens(declared_tokens) {}

  void operator()(THD *candidate) override {
    if (candidate == nullptr || candidate == m_owner) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    const bool declared =
        m_declared_tokens.count(candidate->thread_id()) != 0;
    /*
      The counter already admitted this THD for the current drain generation.
      A pending old command may still own transient stored-program or statement
      context here; eligibility is checked again after it reaches QUIESCED.
      This collector only pins the admitted THD's lifetime.
    */
    const bool target = declared && !candidate->release_resources_done() &&
                        !candidate->is_system_thread() &&
                        candidate->killed == THD::NOT_KILLED;
    const bool idle = target && candidate->m_server_idle;
    std::unique_ptr<Preserve_trx_external_thd_pin> pin;
    if (target) pin = Preserve_trx_external_thd_pin::acquire_locked(candidate);
    mysql_mutex_unlock(&candidate->LOCK_thd_data);

    if (!target) return;
    if (pin == nullptr) {
      m_error = true;
      return;
    }
    m_targets.push_back({candidate, std::move(pin), idle});
  }

  bool error() const { return m_error; }
  std::vector<Target> &targets() { return m_targets; }

 private:
  THD *m_owner;
  const std::set<my_thread_id> &m_declared_tokens;
  std::vector<Target> m_targets;
  bool m_error{false};
};

static constexpr uint kPreserveTrxBatchQuiescedWaitWarningLoops = 1000;

static bool preserve_trx_publish_pending_quiesce_at_idle_boundary(THD *thd) {
  if (thd == nullptr) return false;

  if (thd->preserve_trx_batch_state !=
          Preserve_trx_batch_thd_state::PENDING_QUIESCE ||
      !thd->m_server_idle)
    return false;

  /*
    A pending target is allowed to finish the command that was already in
    flight when the drain selected it. Once the command reaches an idle
    boundary, the target either becomes a quiesced transaction or is removed
    from the batch if the command ended without an active multi-statement
    transaction.
  */
  if (preserve_trx_has_active_multi_stmt_transaction(thd)) {
    thd->preserve_trx_batch_state = Preserve_trx_batch_thd_state::QUIESCED;
  } else {
    thd->preserve_trx_batch_state =
        Preserve_trx_batch_thd_state::DRAINED_NO_TRANSACTION;
  }
  thd->preserve_trx_quiesce_boundary_monotonic_us =
      preserve_trx_monotonic_us();
  return true;
}

static void preserve_trx_note_quiesced_wait(uint *loops) {
  if (loops == nullptr) return;

  ++*loops;
  if (*loops % kPreserveTrxBatchQuiescedWaitWarningLoops == 0) {
    LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
           "PRESERVE: session remains QUIESCED while waiting for batch drain "
           "to release it");
  }
}

class Preserve_batch_target_state_reader final : public Do_THD_Impl {
 public:
  Preserve_batch_target_state_reader(ulonglong generation,
                                     my_thread_id target_thread_id)
      : m_generation(generation), m_target_thread_id(target_thread_id) {}

  void operator()(THD *candidate) override {
    if (m_seen || candidate == nullptr) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    if (candidate->thread_id() == m_target_thread_id &&
        candidate->preserve_trx_batch_generation == m_generation) {
      (void)preserve_trx_publish_pending_quiesce_at_idle_boundary(candidate);
      m_seen = true;
      m_state = candidate->preserve_trx_batch_state;
      if (m_state == Preserve_trx_batch_thd_state::QUIESCED &&
          !candidate->m_server_idle) {
        m_state = Preserve_trx_batch_thd_state::PENDING_QUIESCE;
      }
      m_temp_unsupported_boundary_seen =
          preserve_trx_temp_table_has_untracked_change(candidate) ||
          preserve_trx_temp_table_has_batch_unsupported_boundary(candidate);
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

  bool seen() const { return m_seen; }
  Preserve_trx_batch_thd_state state() const { return m_state; }
  bool temp_unsupported_boundary_seen() const {
    return m_temp_unsupported_boundary_seen;
  }

 private:
  ulonglong m_generation;
  my_thread_id m_target_thread_id;
  bool m_seen{false};
  Preserve_trx_batch_thd_state m_state{Preserve_trx_batch_thd_state::NONE};
  bool m_temp_unsupported_boundary_seen{false};
};

bool preserve_trx_quiesced_batch_target_is_eligible_locked(
    THD *candidate, ulonglong generation);

struct Preserve_batch_ready_target {
  my_thread_id thread_id{0};
  Preserve_trx_batch_thd_state state{Preserve_trx_batch_thd_state::NONE};
  bool temp_unsupported{false};
  ulonglong boundary_observed_us{0};
};

class Preserve_batch_pending_target_observer final : public Do_THD_Impl {
 public:
  Preserve_batch_pending_target_observer(
      ulonglong generation,
      const std::set<my_thread_id> &pending_thread_ids,
      bool closing_gate_active,
      std::vector<Preserve_batch_ready_target> *ready_targets)
      : m_generation(generation),
        m_pending_thread_ids(pending_thread_ids),
        m_closing_gate_active(closing_gate_active),
        m_ready_targets(ready_targets) {}

  void operator()(THD *candidate) override {
    if (candidate == nullptr ||
        m_pending_thread_ids.count(candidate->thread_id()) == 0) {
      return;
    }

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    const my_thread_id thread_id = candidate->thread_id();
    ++m_seen_count;
    if (candidate->preserve_trx_batch_generation != m_generation) {
      m_invalid = true;
      mysql_mutex_unlock(&candidate->LOCK_thd_data);
      return;
    }

    (void)preserve_trx_publish_pending_quiesce_at_idle_boundary(candidate);
    const Preserve_trx_batch_thd_state state =
        candidate->preserve_trx_batch_state;
    const bool temp_unsupported_boundary_seen =
        preserve_trx_temp_table_has_untracked_change(candidate) ||
        preserve_trx_temp_table_has_batch_unsupported_boundary(candidate);
    if (state == Preserve_trx_batch_thd_state::PENDING_QUIESCE) {
      mysql_mutex_unlock(&candidate->LOCK_thd_data);
      return;
    }

    if (state == Preserve_trx_batch_thd_state::QUIESCED) {
      const bool valid_member =
          preserve_trx_quiesced_batch_target_is_eligible_locked(candidate,
                                                                 m_generation);
      const bool rejection_inflight = !candidate->m_server_idle;
      if (!valid_member || (rejection_inflight && !m_closing_gate_active)) {
        m_invalid = true;
        mysql_mutex_unlock(&candidate->LOCK_thd_data);
        return;
      }
    } else if (state !=
               Preserve_trx_batch_thd_state::DRAINED_NO_TRANSACTION) {
      m_invalid = true;
      mysql_mutex_unlock(&candidate->LOCK_thd_data);
      return;
    }

    if (m_ready_targets != nullptr) {
      m_ready_targets->push_back(
          {thread_id, state, temp_unsupported_boundary_seen,
           candidate->preserve_trx_quiesce_boundary_monotonic_us});
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

  bool invalid() const { return m_invalid; }
  size_t seen_count() const { return m_seen_count; }

 private:
  ulonglong m_generation;
  const std::set<my_thread_id> &m_pending_thread_ids;
  bool m_closing_gate_active{false};
  bool m_invalid{false};
  size_t m_seen_count{0};
  std::vector<Preserve_batch_ready_target> *m_ready_targets;
};

bool preserve_trx_batch_thread_id_in_targets(
    my_thread_id thread_id, const std::vector<my_thread_id> &target_thread_ids) {
  for (const my_thread_id target_thread_id : target_thread_ids) {
    if (thread_id == target_thread_id) return true;
  }
  return false;
}

bool preserve_trx_quiesced_batch_target_is_stably_owned_locked(
    THD *candidate, ulonglong generation) {
  return candidate != nullptr &&
         candidate->preserve_trx_batch_generation == generation &&
         candidate->preserve_trx_batch_state ==
             Preserve_trx_batch_thd_state::QUIESCED &&
         !candidate->release_resources_done() && !candidate->is_system_thread() &&
         candidate->killed == THD::NOT_KILLED &&
         preserve_trx_has_active_multi_stmt_transaction(candidate) &&
         !preserve_trx_temp_table_has_batch_unsupported_boundary(candidate);
}

bool preserve_trx_quiesced_batch_target_is_eligible_locked(
    THD *candidate, ulonglong generation) {
  return preserve_trx_quiesced_batch_target_is_stably_owned_locked(
             candidate, generation) &&
         !preserve_trx_is_unsupported_common_context(candidate);
}

bool preserve_trx_quiesced_batch_target_is_member_locked(
    THD *candidate, ulonglong generation,
    const std::vector<my_thread_id> &target_thread_ids) {
  return preserve_trx_quiesced_batch_target_is_eligible_locked(candidate,
                                                                generation) &&
         preserve_trx_batch_thread_id_in_targets(candidate->thread_id(),
                                                 target_thread_ids);
}

bool preserve_trx_quiesced_batch_target_is_valid_locked(
    THD *candidate, ulonglong generation,
    const std::vector<my_thread_id> &target_thread_ids) {
  return preserve_trx_quiesced_batch_target_is_member_locked(
             candidate, generation, target_thread_ids) &&
         candidate->m_server_idle;
}

bool preserve_trx_quiesced_batch_target_is_member(
    THD *candidate, ulonglong generation,
    const std::vector<my_thread_id> &target_thread_ids) {
  if (candidate == nullptr) return false;
  mysql_mutex_lock(&candidate->LOCK_thd_data);
  /*
    A new command can be returning 4020 after the target published QUIESCED.
    That transient response context is not a membership loss. The context-switch
    loop waits for m_server_idle and performs the full eligibility check then.
  */
  const bool member =
      preserve_trx_quiesced_batch_target_is_stably_owned_locked(candidate,
                                                               generation) &&
      preserve_trx_batch_thread_id_in_targets(candidate->thread_id(),
                                              target_thread_ids);
  mysql_mutex_unlock(&candidate->LOCK_thd_data);
  return member;
}

bool preserve_trx_quiesced_batch_target_is_valid(
    THD *candidate, ulonglong generation,
    const std::vector<my_thread_id> &target_thread_ids) {
  if (candidate == nullptr) return false;
  mysql_mutex_lock(&candidate->LOCK_thd_data);
  const bool valid = preserve_trx_quiesced_batch_target_is_valid_locked(
      candidate, generation, target_thread_ids);
  mysql_mutex_unlock(&candidate->LOCK_thd_data);
  return valid;
}

bool preserve_trx_attached_batch_target_is_valid(
    THD *candidate, ulonglong generation,
    const std::vector<my_thread_id> &target_thread_ids) {
  if (candidate == nullptr || current_thd != candidate) return false;
  mysql_mutex_lock(&candidate->LOCK_thd_data);
  const bool valid =
      candidate->preserve_trx_batch_generation == generation &&
      candidate->preserve_trx_batch_state ==
          Preserve_trx_batch_thd_state::ATTACHING &&
      !candidate->release_resources_done() && !candidate->is_system_thread() &&
      candidate->killed == THD::NOT_KILLED && candidate->m_server_idle &&
      preserve_trx_has_active_multi_stmt_transaction(candidate) &&
      !preserve_trx_temp_table_has_batch_unsupported_boundary(candidate) &&
      !preserve_trx_is_unsupported_common_context(candidate) &&
      preserve_trx_batch_thread_id_in_targets(candidate->thread_id(),
                                              target_thread_ids);
  mysql_mutex_unlock(&candidate->LOCK_thd_data);
  return valid;
}

class Preserve_batch_quiesced_target_pin_collector final : public Do_THD_Impl {
 public:
  Preserve_batch_quiesced_target_pin_collector(
      THD *owner, ulonglong generation,
      const std::vector<my_thread_id> &target_thread_ids,
      bool allow_closing_rejection_inflight = false)
      : m_owner(owner),
        m_generation(generation),
        m_target_thread_ids(target_thread_ids),
        m_allow_closing_rejection_inflight(
            allow_closing_rejection_inflight) {}

  void operator()(THD *candidate) override {
    if (candidate == nullptr || candidate == m_owner) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    const bool member = preserve_trx_quiesced_batch_target_is_member_locked(
        candidate, m_generation, m_target_thread_ids);
    const bool closing_rejection_inflight = member && !candidate->m_server_idle;
    const bool target =
        member &&
        (candidate->m_server_idle || m_allow_closing_rejection_inflight);
    std::unique_ptr<Preserve_trx_external_thd_pin> pin;
    if (target) pin = Preserve_trx_external_thd_pin::acquire_locked(candidate);
    mysql_mutex_unlock(&candidate->LOCK_thd_data);

    if (closing_rejection_inflight) {
      DEBUG_SYNC(m_owner,
                 "preserve_trx_batch_final_attach_observed_rejection_inflight");
    }

    if (!target) return;
    if (pin == nullptr) {
      m_error = true;
      return;
    }
    m_targets.push_back({candidate, std::move(pin)});
  }

  bool error() const { return m_error; }
  std::vector<Preserve_trx_pinned_thd> &targets() { return m_targets; }

 private:
  THD *m_owner;
  ulonglong m_generation;
  const std::vector<my_thread_id> &m_target_thread_ids;
  bool m_allow_closing_rejection_inflight{false};
  std::vector<Preserve_trx_pinned_thd> m_targets;
  bool m_error{false};
};

class Preserve_batch_single_quiesced_target_pin final : public Do_THD_Impl {
 public:
  Preserve_batch_single_quiesced_target_pin(THD *owner, ulonglong generation,
                                            my_thread_id target_thread_id)
      : m_owner(owner),
        m_generation(generation),
        m_target_thread_id(target_thread_id),
        m_target_thread_ids{target_thread_id} {}

  void operator()(THD *candidate) override {
    if (m_target.thd != nullptr || candidate == nullptr || candidate == m_owner)
      return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    const bool member = preserve_trx_quiesced_batch_target_is_member_locked(
        candidate, m_generation, m_target_thread_ids);
    const bool closing_rejection_inflight = member && !candidate->m_server_idle;
    const bool target = member;
    std::unique_ptr<Preserve_trx_external_thd_pin> pin;
    if (target) pin = Preserve_trx_external_thd_pin::acquire_locked(candidate);
    mysql_mutex_unlock(&candidate->LOCK_thd_data);

    if (closing_rejection_inflight) {
      DEBUG_SYNC(m_owner,
                 "preserve_trx_batch_final_attach_observed_rejection_inflight");
    }

    if (!target) return;
    if (pin == nullptr) {
      m_error = true;
      return;
    }
    m_target.thd = candidate;
    m_target.pin = std::move(pin);
  }

  bool error() const { return m_error; }
  bool found() const { return m_target.thd != nullptr; }
  Preserve_trx_pinned_thd &target() { return m_target; }

 private:
  THD *m_owner;
  ulonglong m_generation;
  my_thread_id m_target_thread_id;
  std::vector<my_thread_id> m_target_thread_ids;
  Preserve_trx_pinned_thd m_target;
  bool m_error{false};
};

class Preserve_batch_quiesced_target_counter final : public Do_THD_Impl {
 public:
  Preserve_batch_quiesced_target_counter(
      THD *owner, ulonglong generation,
      const std::vector<my_thread_id> &target_thread_ids)
      : m_owner(owner),
        m_generation(generation),
        m_target_thread_ids(target_thread_ids) {}

  void operator()(THD *candidate) override {
    if (candidate == nullptr) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    const bool target =
        candidate != m_owner &&
        candidate->preserve_trx_batch_generation == m_generation &&
        preserve_trx_batch_thread_id_in_targets(candidate->thread_id(),
                                                m_target_thread_ids);
    if (target) {
      const bool unsupported =
          candidate->release_resources_done() || candidate->is_system_thread() ||
          candidate->killed != THD::NOT_KILLED ||
          candidate->preserve_trx_batch_state !=
              Preserve_trx_batch_thd_state::QUIESCED ||
          !candidate->m_server_idle ||
          !preserve_trx_has_active_multi_stmt_transaction(candidate) ||
          !preserved_trx_binlog_format_is_supported(
              candidate->variables.binlog_format) ||
          preserve_trx_is_unsupported_common_context(candidate);
      if (unsupported) {
        m_has_unsupported_transaction = true;
      } else {
        ++m_target_count;
      }
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

  uint target_count() const { return m_target_count; }
  bool has_unsupported_transaction() const {
    return m_has_unsupported_transaction;
  }
 private:
  THD *m_owner;
  ulonglong m_generation;
  const std::vector<my_thread_id> &m_target_thread_ids;
  uint m_target_count{0};
  bool m_has_unsupported_transaction{false};
};

class Preserve_batch_clear_target_generation final : public Do_THD_Impl {
 public:
  Preserve_batch_clear_target_generation(ulonglong generation,
                                         my_thread_id target_thread_id)
      : m_generation(generation), m_target_thread_id(target_thread_id) {}

  void operator()(THD *candidate) override {
    if (m_cleared || candidate == nullptr) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    if (candidate->thread_id() == m_target_thread_id &&
        candidate->preserve_trx_batch_generation == m_generation &&
        candidate->preserve_trx_batch_state !=
            Preserve_trx_batch_thd_state::PRESERVED_DRAINED) {
      candidate->preserve_trx_batch_generation = 0;
      candidate->preserve_trx_batch_state = Preserve_trx_batch_thd_state::NONE;
      candidate->preserve_trx_temp_table_batch_capture_epoch.store(
          false, std::memory_order_release);
      preserve_trx_temp_table_clear_batch_unsupported_boundary(candidate);
      m_cleared = true;
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

 private:
  ulonglong m_generation;
  my_thread_id m_target_thread_id;
  bool m_cleared{false};
};

class Preserve_batch_timeout_target_decision final : public Do_THD_Impl {
 public:
  Preserve_batch_timeout_target_decision(ulonglong generation,
                                         my_thread_id target_thread_id)
      : m_generation(generation), m_target_thread_id(target_thread_id) {}

  void operator()(THD *candidate) override {
    if (m_seen || candidate == nullptr) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    if (candidate->thread_id() == m_target_thread_id &&
        candidate->preserve_trx_batch_generation == m_generation) {
      (void)preserve_trx_publish_pending_quiesce_at_idle_boundary(candidate);
      m_seen = true;
      m_state = candidate->preserve_trx_batch_state;
      m_temp_unsupported_boundary_seen =
          preserve_trx_temp_table_has_untracked_change(candidate) ||
          preserve_trx_temp_table_has_batch_unsupported_boundary(candidate);
      m_boundary_observed_us =
          candidate->preserve_trx_quiesce_boundary_monotonic_us;
      if (m_state == Preserve_trx_batch_thd_state::PENDING_QUIESCE) {
        candidate->preserve_trx_batch_generation = 0;
        candidate->preserve_trx_batch_state =
            Preserve_trx_batch_thd_state::NONE;
        candidate->preserve_trx_temp_table_batch_capture_epoch.store(
            false, std::memory_order_release);
        preserve_trx_temp_table_clear_batch_unsupported_boundary(candidate);
        m_timed_out = true;
      }
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

  bool seen() const { return m_seen; }
  bool timed_out() const { return m_timed_out; }
  Preserve_trx_batch_thd_state state() const { return m_state; }
  bool temp_unsupported_boundary_seen() const {
    return m_temp_unsupported_boundary_seen;
  }
  ulonglong boundary_observed_us() const { return m_boundary_observed_us; }

 private:
  ulonglong m_generation;
  my_thread_id m_target_thread_id;
  bool m_seen{false};
  bool m_timed_out{false};
  Preserve_trx_batch_thd_state m_state{Preserve_trx_batch_thd_state::NONE};
  bool m_temp_unsupported_boundary_seen{false};
  ulonglong m_boundary_observed_us{0};
};

enum class Preserve_trx_batch_wait_result {
  READY,
  DEADLINE,
  RESET_REQUESTED,
  OWNER_KILLED,
  TARGET_NOT_FOUND
};

Preserve_trx_batch_wait_result preserve_trx_batch_wait_target_ready(
    THD *owner, ulonglong generation, my_thread_id target_thread_id,
    Preserve_trx_batch_thd_state *state, bool *temp_unsupported_boundary_seen,
    ulonglong close_deadline_us,
    const std::shared_ptr<Preserve_trx_drain_attempt> &drain_attempt,
    const std::function<bool()> &background_progress) {
  for (;;) {
    if (preserve_trx_active_drain_reset_requested(drain_attempt))
      return Preserve_trx_batch_wait_result::RESET_REQUESTED;
    if (owner != nullptr && owner->killed)
      return Preserve_trx_batch_wait_result::OWNER_KILLED;
    if (preserve_trx_monotonic_deadline_expired_at(
            close_deadline_us, preserve_trx_monotonic_us()))
      return Preserve_trx_batch_wait_result::DEADLINE;
    if (background_progress && background_progress())
      return Preserve_trx_batch_wait_result::TARGET_NOT_FOUND;

    Preserve_batch_target_state_reader reader(generation, target_thread_id);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&reader);
    if (!reader.seen())
      return Preserve_trx_batch_wait_result::TARGET_NOT_FOUND;

    const Preserve_trx_batch_thd_state current_state = reader.state();
    if (current_state != Preserve_trx_batch_thd_state::PENDING_QUIESCE) {
      *state = current_state;
      if (temp_unsupported_boundary_seen != nullptr) {
        *temp_unsupported_boundary_seen =
            reader.temp_unsupported_boundary_seen();
      }
      return Preserve_trx_batch_wait_result::READY;
    }
    my_sleep(10000);
  }
}

Preserve_trx_batch_wait_result preserve_trx_batch_observe_targets_ready_joint(
    THD *owner, ulonglong generation,
    std::set<my_thread_id> *pending,
    std::vector<Preserve_batch_ready_target> *ready_targets,
    ulonglong close_deadline_us,
    const std::shared_ptr<Preserve_trx_drain_attempt> &drain_attempt) {
  if (pending == nullptr || ready_targets == nullptr) {
    return Preserve_trx_batch_wait_result::TARGET_NOT_FOUND;
  }
  ready_targets->clear();
  if (pending->empty()) return Preserve_trx_batch_wait_result::READY;
  if (preserve_trx_active_drain_reset_requested(drain_attempt)) {
    return Preserve_trx_batch_wait_result::RESET_REQUESTED;
  }
  if (owner != nullptr && owner->killed) {
    return Preserve_trx_batch_wait_result::OWNER_KILLED;
  }
  if (preserve_trx_monotonic_deadline_expired_at(
          close_deadline_us, preserve_trx_monotonic_us())) {
    return Preserve_trx_batch_wait_result::DEADLINE;
  }

  const bool closing_gate_active = preserve_trx_closing_command_gate_active(
      preserve_trx_manager_state_owner_snapshot().state);
  Preserve_batch_pending_target_observer observer(
      generation, *pending, closing_gate_active, ready_targets);
  Global_THD_manager::get_instance()->do_for_all_thd_copy(&observer);
  if (observer.invalid()) {
    return Preserve_trx_batch_wait_result::TARGET_NOT_FOUND;
  }
  if (observer.seen_count() != pending->size())
    return Preserve_trx_batch_wait_result::TARGET_NOT_FOUND;

  for (const Preserve_batch_ready_target &ready : *ready_targets) {
    pending->erase(ready.thread_id);
  }
  return Preserve_trx_batch_wait_result::READY;
}

class Preserve_batch_clear_generation final : public Do_THD_Impl {
 public:
  explicit Preserve_batch_clear_generation(ulonglong generation)
      : m_generation(generation) {}

  void operator()(THD *candidate) override {
    if (candidate == nullptr) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    if (candidate->preserve_trx_batch_generation == m_generation &&
        candidate->preserve_trx_batch_state !=
            Preserve_trx_batch_thd_state::PRESERVED_DRAINED) {
      candidate->preserve_trx_batch_generation = 0;
      candidate->preserve_trx_batch_state = Preserve_trx_batch_thd_state::NONE;
      candidate->preserve_trx_temp_table_batch_capture_epoch.store(
          false, std::memory_order_release);
      preserve_trx_temp_table_clear_batch_unsupported_boundary(candidate);
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

 private:
  ulonglong m_generation;
};

class Preserve_batch_clear_temp_table_unsupported_boundaries final
    : public Do_THD_Impl {
 public:
  void operator()(THD *candidate) override {
    if (candidate == nullptr) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    preserve_trx_temp_table_clear_batch_unsupported_boundary(candidate);
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }
};

class Phase1_transfer_binlog_blob_provider final
    : public PreserveBinlogBlobProvider {
 public:
  explicit Phase1_transfer_binlog_blob_provider(
      PreserveBinlogBlobProvider *fallback_provider, ulonglong generation)
      : m_fallback_provider(fallback_provider), m_generation(generation) {}

  ~Phase1_transfer_binlog_blob_provider() override { cleanup_phase1_blobs(); }

  void remember_phase1_blob(my_thread_id thread_id,
                            const PrebuiltBinlogCacheBlob &blob,
                            bool owns_cleanup = true,
                            bool remote_presealed = false) {
    if (thread_id == 0 || blob.name != kPreservedTrxBlobBinlogCache ||
        blob.size == 0) {
      return;
    }
    std::lock_guard<std::mutex> guard(m_mutex);
    m_phase1_blobs[thread_id] = blob;
    m_locally_queued_thread_ids.erase(thread_id);
    if (owns_cleanup && !blob.warmcopy_id.empty()) {
      m_phase1_warmcopy_ids.insert(blob.warmcopy_id);
    }
    if (remote_presealed && !blob.warmcopy_id.empty()) {
      m_remote_presealed_warmcopy_ids.insert(blob.warmcopy_id);
    }
  }

  void remember_locally_queued_blob(my_thread_id thread_id,
                                    const PrebuiltBinlogCacheBlob &blob) {
    if (thread_id == 0 || blob.name != kPreservedTrxBlobBinlogCache ||
        blob.size == 0) {
      return;
    }
    std::lock_guard<std::mutex> guard(m_mutex);
    m_phase1_blobs[thread_id] = blob;
    m_locally_queued_thread_ids.insert(thread_id);
  }

  bool locally_queued_blob_matches_current_thd(
      THD *thd, PrebuiltBinlogCacheBlob *blob) const {
    if (thd == nullptr || blob == nullptr) return false;
    PrebuiltBinlogCacheBlob queued_blob;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      if (m_locally_queued_thread_ids.count(thd->thread_id()) == 0) {
        return false;
      }
      const auto it = m_phase1_blobs.find(thd->thread_id());
      if (it == m_phase1_blobs.end()) return false;
      queued_blob = it->second;
    }
    if (!phase1_blob_is_current(thd, queued_blob)) return false;
    *blob = std::move(queued_blob);
    return true;
  }

  void adopt_cleanup_ownership(const PrebuiltBinlogCacheBlob &blob) {
    if (blob.name != kPreservedTrxBlobBinlogCache ||
        blob.warmcopy_id.empty()) {
      return;
    }
    std::lock_guard<std::mutex> guard(m_mutex);
    m_phase1_warmcopy_ids.insert(blob.warmcopy_id);
  }

  bool phase1_blob_matches_current_thd(THD *thd) const {
    if (thd == nullptr) return false;
    PrebuiltBinlogCacheBlob phase1_blob;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      auto it = m_phase1_blobs.find(thd->thread_id());
      if (it == m_phase1_blobs.end()) return false;
      phase1_blob = it->second;
    }

    return phase1_blob_is_current(thd, phase1_blob);
  }

  bool phase1_blob_for_thd(const THD *thd,
                           PrebuiltBinlogCacheBlob *blob) const {
    if (thd == nullptr || blob == nullptr) return false;
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto it = m_phase1_blobs.find(thd->thread_id());
    if (it == m_phase1_blobs.end()) return false;
    *blob = it->second;
    return true;
  }

  bool has_blob_for_thd(const THD *thd) const override {
    if (thd == nullptr) return false;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      if (m_phase1_blobs.count(thd->thread_id()) != 0) return true;
    }
    if (m_fallback_provider != nullptr &&
        m_fallback_provider->has_blob_for_thd(thd)) {
      return true;
    }

    /*
      Transfer early workers call this only after the target is quiesced. A
      bounded current cache can therefore be materialized by
      finalize_for_preserve() even when no opportunistic phase-1 copy survived.
    */
    uint64_t cache_length = 0;
    bool has_binlog_cache = false;
    return !mysql_binlog_preserve_warmcopy_cache_length(
               const_cast<THD *>(thd), &cache_length, &has_binlog_cache) &&
           has_binlog_cache && cache_length != 0 &&
           cache_length <= preserve_trx_max_binlog_cache_bytes;
  }

  Preserve_snapshot_status finalize_for_preserve(
      THD *thd, const std::string &token,
      PrebuiltBinlogCacheBlob *blob,
      const PreserveBinlogBlobFinalizeContext &) override {
    if (thd == nullptr || blob == nullptr) {
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    }
    PrebuiltBinlogCacheBlob phase1_blob;
    bool has_phase1_blob = false;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      auto it = m_phase1_blobs.find(thd->thread_id());
      if (it != m_phase1_blobs.end()) {
        phase1_blob = it->second;
        has_phase1_blob = true;
      }
    }
    if (has_phase1_blob) {
      const bool current_matches = phase1_blob_is_current(thd, phase1_blob);
      if (current_matches) {
        std::lock_guard<std::mutex> guard(m_mutex);
        auto it = m_phase1_blobs.find(thd->thread_id());
        if (it != m_phase1_blobs.end() &&
            it->second.warmcopy_id == phase1_blob.warmcopy_id) {
          *blob = it->second;
          m_phase1_blobs.erase(it);
          m_locally_queued_thread_ids.erase(thd->thread_id());
          return Preserve_snapshot_status::OK;
        }
      }
    }
    PreserveBinlogBlobFinalizeContext fallback_context;
    if (has_phase1_blob &&
        phase1_blob_is_prefix_of_current_thd(thd, phase1_blob)) {
      fallback_context.receiver_prefix_published = true;
      fallback_context.receiver_prefix_bytes = phase1_blob.size;
    }
    if (m_fallback_provider != nullptr &&
        m_fallback_provider->finalize_for_preserve(
            thd, token, blob, fallback_context) ==
            Preserve_snapshot_status::OK) {
      return Preserve_snapshot_status::OK;
    }

    /*
      Savepoint rollback can intentionally invalidate the live mirror. The
      target is already quiesced here, so rebuild only that final cache instead
      of accepting a stale prefix or failing the whole batch.
    */
    uint64_t cache_length = 0;
    bool has_binlog_cache = false;
    if (mysql_binlog_preserve_warmcopy_cache_length(
            thd, &cache_length, &has_binlog_cache) ||
        !has_binlog_cache || cache_length == 0 ||
        cache_length > preserve_trx_max_binlog_cache_bytes) {
      return Preserve_snapshot_status::IO_ERROR;
    }
    auto carrier = create_preserved_trx_process_local_warm_external_blob_carrier(
        preserve_trx_default_dir());
    if (carrier == nullptr) return Preserve_snapshot_status::IO_ERROR;
    bool has_rebuilt_blob = false;
    const std::string warmcopy_id =
        "transfer_binlog_fallback_" + std::to_string(m_generation) + "_" +
        std::to_string(static_cast<unsigned long long>(thd->thread_id()));
    if (mysql_binlog_preserve_warmcopy_build_blob(
            thd, warmcopy_id, m_generation, carrier.get(),
            preserve_trx_max_binlog_cache_bytes, blob, &has_rebuilt_blob) ||
        !has_rebuilt_blob) {
      return Preserve_snapshot_status::IO_ERROR;
    }
    adopt_cleanup_ownership(*blob);
    return Preserve_snapshot_status::OK;
  }

  void discard_for_preserve(THD *thd, const std::string &token,
                            const PrebuiltBinlogCacheBlob &blob) override {
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      if (m_phase1_warmcopy_ids.count(blob.warmcopy_id) != 0 ||
          m_remote_presealed_warmcopy_ids.count(blob.warmcopy_id) != 0) {
        return;
      }
    }
    if (m_fallback_provider != nullptr) {
      m_fallback_provider->discard_for_preserve(thd, token, blob);
    }
  }

  void cleanup_phase1_blobs() {
    std::set<std::string> warmcopy_ids;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      warmcopy_ids.swap(m_phase1_warmcopy_ids);
      m_remote_presealed_warmcopy_ids.clear();
      m_locally_queued_thread_ids.clear();
      m_phase1_blobs.clear();
    }
    if (warmcopy_ids.empty()) return;
    auto carrier = create_preserved_trx_process_local_warm_external_blob_carrier(
        preserve_trx_default_dir());
    if (carrier == nullptr) return;
    if (carrier->remove_warm_external_blobs(
            warmcopy_ids, kPreservedTrxBlobBinlogCache) !=
        Preserved_trx_carrier_status::OK) {
      LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
             "PRESERVE: failed to remove retained source rollback binlog "
             "artifacts");
    }
  }

  std::set<std::string> release_phase1_blobs_for_deferred_cleanup() {
    std::set<std::string> warmcopy_ids;
    std::lock_guard<std::mutex> guard(m_mutex);
    warmcopy_ids.swap(m_phase1_warmcopy_ids);
    m_remote_presealed_warmcopy_ids.clear();
    m_locally_queued_thread_ids.clear();
    m_phase1_blobs.clear();
    return warmcopy_ids;
  }

 private:
  static bool phase1_blob_is_current(
      THD *thd, const PrebuiltBinlogCacheBlob &phase1_blob) {
    uint64_t current_length = 0;
    uint64_t current_generation = 0;
    bool current_has_blob = false;
    Mysql_binlog_preserve_snapshot current_metadata;
    return !mysql_binlog_preserve_warmcopy_cache_length(
               thd, &current_length, &current_has_blob) &&
           current_has_blob && current_length == phase1_blob.size &&
           !mysql_binlog_preserve_export_metadata_only(thd,
                                                       &current_metadata) &&
           binlog_metadata_matches(current_metadata, phase1_blob.metadata) &&
           !mysql_binlog_warmcopy_source_truncate_generation(
               thd, &current_generation) &&
           current_generation == phase1_blob.phase1_truncate_generation;
  }

  static bool phase1_blob_is_prefix_of_current_thd(
      THD *thd, const PrebuiltBinlogCacheBlob &phase1_blob) {
    uint64_t current_length = 0;
    uint64_t current_generation = 0;
    bool current_has_blob = false;
    return phase1_blob.size != 0 &&
           !mysql_binlog_preserve_warmcopy_cache_length(
               thd, &current_length, &current_has_blob) &&
           current_has_blob && current_length >= phase1_blob.size &&
           !mysql_binlog_warmcopy_source_truncate_generation(
               thd, &current_generation) &&
           current_generation == phase1_blob.phase1_truncate_generation;
  }

  static bool binlog_metadata_matches(
      const Mysql_binlog_preserve_snapshot &lhs,
      const Mysql_binlog_preserve_snapshot &rhs) {
    return lhs.gtid_next == rhs.gtid_next && lhs.owned_gtid == rhs.owned_gtid &&
           lhs.event_counter == rhs.event_counter &&
           lhs.immediate == rhs.immediate && lhs.with_xid == rhs.with_xid &&
           lhs.with_sbr == rhs.with_sbr && lhs.with_rbr == rhs.with_rbr &&
           lhs.with_start == rhs.with_start && lhs.with_end == rhs.with_end &&
           lhs.with_content == rhs.with_content &&
           lhs.has_prev_position == rhs.has_prev_position &&
           lhs.prev_position == rhs.prev_position &&
           lhs.has_cache_length == rhs.has_cache_length &&
           lhs.cache_length == rhs.cache_length &&
           lhs.has_compression_session_state ==
               rhs.has_compression_session_state &&
           lhs.binlog_trx_compression == rhs.binlog_trx_compression &&
           lhs.binlog_trx_compression_type == rhs.binlog_trx_compression_type &&
           lhs.binlog_trx_compression_level_zstd ==
               rhs.binlog_trx_compression_level_zstd;
  }

  PreserveBinlogBlobProvider *m_fallback_provider{nullptr};
  ulonglong m_generation{0};
  mutable std::mutex m_mutex;
  std::map<my_thread_id, PrebuiltBinlogCacheBlob> m_phase1_blobs;
  std::set<my_thread_id> m_locally_queued_thread_ids;
  std::set<std::string> m_phase1_warmcopy_ids;
  std::set<std::string> m_remote_presealed_warmcopy_ids;
};

static void preserve_trx_defer_source_warm_blob_cleanup(
    const std::string &dir, std::set<std::string> warmcopy_ids) {
  if (warmcopy_ids.empty()) return;
  try {
    std::lock_guard<std::mutex> guard(g_preserved_trx_reaper_mutex);
    g_deferred_source_warm_blob_cleanup.push_back(
        {dir, std::move(warmcopy_ids)});
  } catch (...) {
    /*
      FINAL_ACK already transferred ownership. Startup orphan cleanup can
      discover these process-local warm artifacts if this best-effort queue
      cannot retain their identifiers.
    */
    LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
           "PRESERVE: failed to defer source warm artifact cleanup");
  }
}

static void preserved_trx_cleanup_deferred_source_warm_blobs_once() {
  std::vector<Deferred_source_warm_blob_cleanup> cleanup;
  {
    std::lock_guard<std::mutex> guard(g_preserved_trx_reaper_mutex);
    cleanup.swap(g_deferred_source_warm_blob_cleanup);
  }
  for (Deferred_source_warm_blob_cleanup &entry : cleanup) {
    const ulonglong started_us = preserve_trx_monotonic_us();
    auto carrier =
        create_preserved_trx_process_local_warm_external_blob_carrier(
            entry.dir);
    const Preserved_trx_carrier_status status =
        carrier == nullptr
            ? Preserved_trx_carrier_status::IO_ERROR
            : carrier->remove_warm_external_blobs(
                  entry.warmcopy_ids, kPreservedTrxBlobBinlogCache);
    const ulonglong finished_us = preserve_trx_monotonic_us();
    LogErr(status == Preserved_trx_carrier_status::OK ? INFORMATION_LEVEL
                                                      : WARNING_LEVEL,
           ER_LOG_PRINTF_MSG,
           ("PRESERVE: deferred source warm artifact cleanup status=" +
            std::to_string(static_cast<int>(status)) +
            " warmcopy_ids=" + std::to_string(entry.warmcopy_ids.size()) +
            " elapsed_us=" +
            std::to_string(finished_us >= started_us
                               ? finished_us - started_us
                               : 0))
               .c_str());
  }
}

bool warmcopy_close_deadline_expired(ulonglong close_deadline_us);

unsigned long warmcopy_close_timeout_ms_until_deadline(
    ulonglong close_deadline_us, unsigned long fallback_timeout_ms);

class Warmcopy_batch_blob_provider final : public PreserveBinlogBlobProvider {
  struct Entry {
    Mysql_binlog_warmcopy_session *session{nullptr};
    std::string warmcopy_id;
    bool preparing{false};
    bool rebuilding{false};
    bool finalizing{false};
    bool rebuild_pending{false};
    uint64_t rebuild_incarnation{0};
    uint64_t reserved_size{0};
  };

 public:
  Warmcopy_batch_blob_provider(
      std::string dir, bool allow_quiesced_rebuild_fallback,
      const Preserve_trx_transfer_runtime_policy &runtime_policy,
      uint64_t max_inflight_bytes)
      : m_dir(std::move(dir)),
        m_carrier(allow_quiesced_rebuild_fallback
                      ? create_preserved_trx_process_local_warm_external_blob_carrier(
                            m_dir)
                      : create_preserved_trx_default_warm_external_blob_carrier(
                            m_dir)),
        m_max_total_bytes(preserve_trx_warmcopy_max_total_bytes),
        m_max_entry_bytes(preserve_trx_max_binlog_cache_bytes),
        m_reservation_chunk_bytes(
            std::max<uint64_t>(1, runtime_policy.warmcopy_chunk_bytes)),
        m_copy_chunk_bytes(
            std::max<uint64_t>(1, runtime_policy.warmcopy_chunk_bytes)),
        m_tail_budget_bytes(runtime_policy.warmcopy_tail_budget_bytes),
        m_max_inflight_bytes(max_inflight_bytes),
        m_allow_quiesced_rebuild_fallback(
            allow_quiesced_rebuild_fallback) {
    if (m_carrier == nullptr) m_error = true;
  }

  ~Warmcopy_batch_blob_provider() override { cleanup_warm_artifacts(); }

  bool prepare_blob_for_thd(THD *thd, uint64_t epoch) {
    return prepare_thd(thd, epoch, false);
  }

  bool prepare_blob_for_thd_if_present(THD *thd, uint64_t epoch) {
    uint64_t cache_length = 0;
    bool has_current_blob = false;
    if (mysql_binlog_preserve_warmcopy_cache_length(
            thd, &cache_length, &has_current_blob)) {
      std::lock_guard<std::mutex> guard(m_mutex);
      note_token_prepare_failure_locked();
      return false;
    }
    if (!has_current_blob || cache_length == 0) return true;
    return prepare_thd(thd, epoch, false);
  }

  bool prepare_active_blob_for_thd(THD *thd, uint64_t epoch) {
    if (!m_allow_quiesced_rebuild_fallback) return true;
    if (!rebuild_stale_target(thd, epoch) ||
        !prepare_thd(thd, epoch, true)) {
      return false;
    }
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto it = m_entries.find(thd->thread_id());
    return it != m_entries.end() && !it->second.preparing &&
           !it->second.rebuilding && !it->second.finalizing &&
           it->second.session != nullptr;
  }

  bool prepare_quiesced_blob_for_thd(THD *thd, uint64_t epoch) {
    if (stale_rebuildable_for_thd(thd)) {
      return m_allow_quiesced_rebuild_fallback;
    }
    return rebuild_stale_target(thd, epoch) &&
           prepare_blob_for_thd_if_present(thd, epoch);
  }

  bool has_blob_for_thd(const THD *thd) const override {
    if (thd == nullptr) return false;
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto it = m_entries.find(thd->thread_id());
    if (it == m_entries.end() || it->second.preparing ||
        it->second.rebuilding || it->second.finalizing ||
        it->second.session == nullptr) {
      return false;
    }
    uint64_t current_length = 0;
    bool current_has_blob = false;
    if (mysql_binlog_preserve_warmcopy_cache_length(
            const_cast<THD *>(thd), &current_length, &current_has_blob)) {
      return false;
    }
    return current_has_blob && current_length != 0;
  }

  bool phase1_prefix_blob_for_thd(THD *thd,
                                  PrebuiltBinlogCacheBlob *blob) const {
    if (thd == nullptr || blob == nullptr) return false;
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto it = m_entries.find(thd->thread_id());
    if (it == m_entries.end() || it->second.preparing ||
        it->second.rebuilding || it->second.finalizing ||
        it->second.session == nullptr) {
      return false;
    }
    bool has_blob = false;
    return !mysql_binlog_preserve_warmcopy_prefix_blob(
               thd, it->second.session, blob, &has_blob) &&
           has_blob;
  }

  bool phase1_prefix_delta_for_thd(
      THD *thd, const PrebuiltBinlogCacheBlob *preserved_prefix,
      uint64_t minimum_delta_bytes, PrebuiltBinlogCacheBlob *next_prefix,
      std::string *delta_payload, bool *replace_preserved_prefix) const {
    if (replace_preserved_prefix != nullptr) {
      *replace_preserved_prefix = false;
    }
    if (thd == nullptr || next_prefix == nullptr || delta_payload == nullptr) {
      return false;
    }
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto it = m_entries.find(thd->thread_id());
    if (it == m_entries.end() || it->second.preparing ||
        it->second.rebuilding || it->second.finalizing ||
        it->second.session == nullptr || m_carrier == nullptr) {
      return false;
    }

    PrebuiltBinlogCacheBlob sampled;
    bool has_sampled = false;
    if (mysql_binlog_preserve_warmcopy_prefix_blob(
            thd, it->second.session, &sampled, &has_sampled) ||
        !has_sampled || sampled.size == 0) {
      return false;
    }

    uint64_t payload_offset = 0;
    if (preserved_prefix != nullptr) {
      if (preserved_prefix->phase1_truncate_generation ==
          sampled.phase1_truncate_generation) {
        if (preserved_prefix->size > sampled.size ||
            (preserved_prefix->size == sampled.size &&
             preserved_prefix->digest != sampled.digest)) {
          return false;
        }
        payload_offset = preserved_prefix->size;
      } else if (replace_preserved_prefix != nullptr) {
        *replace_preserved_prefix = true;
      }
    }
    const uint64_t delta_bytes = sampled.size - payload_offset;
    if (delta_bytes < minimum_delta_bytes || delta_bytes == 0 ||
        delta_bytes > m_max_inflight_bytes) {
      return false;
    }

    std::string payload;
    if (m_carrier->read_active_warm_external_blob_range(
            sampled.warmcopy_id, sampled.name, sampled.warmcopy_epoch,
            payload_offset, delta_bytes,
            m_max_inflight_bytes,
            &payload) != Preserved_trx_carrier_status::OK) {
      return false;
    }

    /*
      Appends may advance the mirror while the range is copied. They preserve
      the sampled prefix. A truncate/reset changes the generation or degrades
      the session, so the copied range is discarded before network publication.
    */
    PrebuiltBinlogCacheBlob verified;
    bool has_verified = false;
    if (mysql_binlog_preserve_warmcopy_prefix_blob(
            thd, it->second.session, &verified, &has_verified) ||
        !has_verified ||
        verified.warmcopy_id != sampled.warmcopy_id ||
        verified.warmcopy_epoch != sampled.warmcopy_epoch ||
        verified.phase1_truncate_generation !=
            sampled.phase1_truncate_generation ||
        verified.size < sampled.size ||
        (verified.size == sampled.size &&
         verified.digest != sampled.digest)) {
      return false;
    }

    *next_prefix = sampled;
    *delta_payload = std::move(payload);
    return true;
  }

  Preserve_snapshot_status finalize_for_preserve(
      THD *thd, const std::string &, PrebuiltBinlogCacheBlob *blob,
      const PreserveBinlogBlobFinalizeContext &context) override {
    /*
	      This metric is scoped to warmcopy's binlog-cache phase-2 work
      (tail validation/refresh/adoption). It intentionally excludes the rest
      of batch preserve, which is dominated by lock/read-view/MDL/undo work and
      is not the large binlog-cache copy regression this gate measures.
    */
    const ulonglong phase2_started_us = preserve_trx_monotonic_us();
    auto finish = [&](Preserve_snapshot_status status) {
      const ulonglong now_us = preserve_trx_monotonic_us();
      const ulonglong elapsed_us =
          now_us >= phase2_started_us ? now_us - phase2_started_us : 0;
      preserve_trx_warmcopy_note_phase2_pause_us(
          elapsed_us == 0 ? 1 : elapsed_us);
      return status;
    };
    ulonglong observed_close_deadline_us = 0;
    auto log_provider_failure = [&](const char *reason, uint64_t reserved_size,
                                    uint64_t finalized_size) {
      std::ostringstream message;
      message << "PRESERVE: warm-copy provider finalize failed"
              << " reason=" << reason
              << " thread_id=" << (thd == nullptr ? 0 : thd->thread_id())
              << " reserved_size=" << reserved_size
              << " finalized_size=" << finalized_size
              << " total_bytes="
              << m_total_bytes.load(std::memory_order_relaxed)
              << " max_total_bytes=" << m_max_total_bytes
              << " tail_budget=" << m_tail_budget_bytes
              << " close_deadline_us=" << observed_close_deadline_us;
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.str().c_str());
    };

    if (thd == nullptr || blob == nullptr) {
      log_provider_failure("invalid argument", 0, 0);
      return finish(Preserve_snapshot_status::INVALID_ARGUMENT);
    }
    DEBUG_SYNC(thd, "preserve_trx_warmcopy_before_finalize_admission");
    Mysql_binlog_warmcopy_session *session = nullptr;
    uint64_t admitted_reserved_size = 0;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      observed_close_deadline_us = m_close_deadline_us;
      auto it = m_entries.find(thd->thread_id());
      if (it == m_entries.end() || it->second.preparing ||
          it->second.rebuilding || it->second.finalizing) {
        log_provider_failure("entry missing or busy", 0, 0);
        return finish(Preserve_snapshot_status::INVALID_ARGUMENT);
      }
      Entry &entry = it->second;
      session = entry.session;
      admitted_reserved_size = entry.reserved_size;
      if (session == nullptr) {
        log_provider_failure("session missing", admitted_reserved_size, 0);
        return finish(Preserve_snapshot_status::IO_ERROR);
      }
      entry.finalizing = true;
      ++m_active_finalizers;
    }
    bool finalizer_work_completed = false;
    auto finalizer_cleanup = create_scope_guard([&] {
      std::lock_guard<std::mutex> guard(m_mutex);
      auto it = m_entries.find(thd->thread_id());
      if (it != m_entries.end() && it->second.finalizing) {
        it->second.finalizing = false;
      } else {
        m_error = true;
      }
      if (!finalizer_work_completed) m_error = true;
      assert(m_active_finalizers != 0);
      --m_active_finalizers;
      m_condition.notify_all();
    });

    DEBUG_SYNC(thd, "preserve_trx_warmcopy_after_finalize_admission");

    bool has_final_blob = false;
    PrebuiltBinlogCacheBlob finalized;
    uint64_t retained_reservation_bytes = 0;
    const bool finalize_failed = mysql_binlog_preserve_warmcopy_finalize_session(
        thd, session, m_tail_budget_bytes,
        context.receiver_prefix_published, context.receiver_prefix_bytes,
        &finalized, &has_final_blob, &retained_reservation_bytes);
    const bool finalized_blob_ready = !finalize_failed && has_final_blob;
    /*
      m_close_deadline_us bounds the admission-closing and quiesced-target
      preparation window.  Once phase-2 target preserve has started, rejecting a
      ready warmcopy artifact solely because that earlier deadline elapsed can
      turn a large but otherwise consistent batch into a cleanup failure.  The
      session finalize path itself is bounded by its tail budget and by the
      surrounding batch drain lifecycle.
    */
    bool ownership_valid = false;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      auto it = m_entries.find(thd->thread_id());
      if (it != m_entries.end() && it->second.finalizing &&
          it->second.session == session) {
        ownership_valid = true;
        if (finalized_blob_ready) {
          mysql_binlog_preserve_warmcopy_abort_session(session);
          it->second.reserved_size = retained_reservation_bytes;
          it->second.session = nullptr;
          *blob = finalized;
        }
      } else {
        m_error = true;
      }
    }
    finalizer_work_completed = true;
    finalizer_cleanup.rollback();
    if (!ownership_valid) {
      log_provider_failure("entry ownership changed during finalize",
                           admitted_reserved_size, finalized.size);
      return finish(Preserve_snapshot_status::IO_ERROR);
    }
    if (!finalized_blob_ready) {
      log_provider_failure(finalize_failed ? "session finalize failed"
                                           : "session produced no final blob",
                           admitted_reserved_size, finalized.size);
      return finish(Preserve_snapshot_status::IO_ERROR);
    }
    return finish(Preserve_snapshot_status::OK);
  }

  bool release_finalized_artifact(
      my_thread_id thread_id, const PrebuiltBinlogCacheBlob &blob) {
    if (thread_id == 0 || blob.name != kPreservedTrxBlobBinlogCache ||
        blob.warmcopy_id.empty()) {
      return false;
    }
    std::lock_guard<std::mutex> guard(m_mutex);
    auto it = m_entries.find(thread_id);
    if (it == m_entries.end() || it->second.session != nullptr ||
        it->second.warmcopy_id != blob.warmcopy_id) {
      return false;
    }
    it->second.warmcopy_id.clear();
    return true;
  }

  void discard_for_preserve(THD *, const std::string &,
                            const PrebuiltBinlogCacheBlob &blob) override {
    if (!blob.warmcopy_id.empty()) {
      if (m_carrier != nullptr) {
        (void)m_carrier->remove_warm_external_blob(blob.warmcopy_id, blob.name);
      }
    }
  }

  bool error() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    return m_error;
  }

  uint64_t total_bytes() const {
    return m_total_bytes.load(std::memory_order_relaxed);
  }

  size_t prepared_count() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    return m_entries.size();
  }

  void stop_mirroring_for_reset() {
    std::unique_lock<std::mutex> guard(m_mutex);
    m_condition.wait(guard, [&]() { return m_active_finalizers == 0; });
    for (auto &entry : m_entries) {
      mysql_binlog_preserve_warmcopy_stop_session_mirroring(
          entry.second.session);
    }
  }

  void set_close_deadline_us(ulonglong close_deadline_us) {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_close_deadline_us = close_deadline_us;
  }

  bool tail_budget_exceeded(THD *thd, bool *exceeded) const {
    if (exceeded != nullptr) *exceeded = false;
    if (thd == nullptr) return false;
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto it = m_entries.find(thd->thread_id());
    if (it == m_entries.end() || it->second.preparing ||
        it->second.rebuilding || it->second.finalizing) {
      return false;
    }
    Mysql_binlog_warmcopy_session *const session = it->second.session;
    if (session == nullptr) return true;
    return mysql_binlog_preserve_warmcopy_tail_budget_exceeded(
        thd, session, m_tail_budget_bytes, exceeded);
  }

  bool stale_rebuildable_for_thd(const THD *thd) const {
    if (thd == nullptr) return false;
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto it = m_entries.find(thd->thread_id());
    return it != m_entries.end() && !it->second.preparing &&
           !it->second.rebuilding && !it->second.finalizing &&
           it->second.session != nullptr &&
           (it->second.rebuild_pending ||
            mysql_binlog_preserve_warmcopy_session_stale_rebuildable(
                it->second.session));
  }

  bool build_quiesced_stale_inline_blob(
      THD *thd, uint64_t epoch, uint64_t max_inline_bytes,
      PrebuiltBinlogCacheBlob *blob, std::string *payload) const {
    if (thd == nullptr || epoch == 0 || max_inline_bytes == 0 ||
        blob == nullptr || payload == nullptr) {
      return false;
    }
    if (!stale_rebuildable_for_thd(thd)) return false;

    uint64_t cache_length = 0;
    bool has_binlog_cache = false;
    if (mysql_binlog_preserve_warmcopy_cache_length(
        thd, &cache_length, &has_binlog_cache) ||
        !has_binlog_cache || cache_length == 0 ||
        cache_length > max_inline_bytes) {
      return false;
    }

    Mysql_binlog_preserve_snapshot snapshot;
    const bool export_failed = mysql_binlog_preserve_export(thd, &snapshot);
    if (export_failed ||
        !snapshot.has_cache_length ||
        snapshot.cache_length != cache_length ||
        snapshot.cache_payload.size() != cache_length) {
      return false;
    }
    uint64_t truncate_generation = 0;
    if (mysql_binlog_warmcopy_source_truncate_generation(
            thd, &truncate_generation)) {
      return false;
    }

    PrebuiltBinlogCacheBlob built;
    built.warmcopy_id =
        "transfer_binlog_inline_" + std::to_string(epoch) + "_" +
        std::to_string(
            static_cast<unsigned long long>(thd->thread_id()));
    built.warmcopy_epoch = epoch;
    built.size = cache_length;
    if (SHA256(reinterpret_cast<const unsigned char *>(
                   snapshot.cache_payload.data()),
               snapshot.cache_payload.size(), built.digest.data()) == nullptr) {
      return false;
    }
    built.phase1_truncate_generation = truncate_generation;
    *payload = std::move(snapshot.cache_payload);
    snapshot.cache_payload.clear();
    built.metadata = std::move(snapshot);
    *blob = std::move(built);
    return true;
  }

  bool retire_stale_artifact(my_thread_id thread_id,
                             PrebuiltBinlogCacheBlob *cleanup_blob) {
    if (thread_id == 0 || cleanup_blob == nullptr) return false;

    Mysql_binlog_warmcopy_session *session = nullptr;
    std::string warmcopy_id;
    uint64_t reserved_size = 0;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      const auto it = m_entries.find(thread_id);
      if (it == m_entries.end() || it->second.preparing ||
          it->second.rebuilding || it->second.finalizing ||
          it->second.session == nullptr ||
          (!it->second.rebuild_pending &&
           !mysql_binlog_preserve_warmcopy_session_stale_rebuildable(
               it->second.session))) {
        return false;
      }
      session = it->second.session;
      warmcopy_id = std::move(it->second.warmcopy_id);
      reserved_size = it->second.reserved_size;
      m_entries.erase(it);
      m_condition.notify_all();
    }

    mysql_binlog_preserve_warmcopy_abort_session(session);
    if (reserved_size != 0) {
      (void)warmcopy_retain_entry_reservation(&m_total_bytes, 0,
                                              &reserved_size);
    }
    cleanup_blob->warmcopy_id = std::move(warmcopy_id);
    cleanup_blob->name = kPreservedTrxBlobBinlogCache;
    return true;
  }

  bool rebuild_stale_target(THD *thd, uint64_t epoch) {
    if (thd == nullptr || epoch == 0 || m_carrier == nullptr) return false;

    const my_thread_id thread_id = thd->thread_id();
    Mysql_binlog_warmcopy_session *old_session = nullptr;
    std::string old_warmcopy_id;
    uint64_t old_reserved_size = 0;
    uint64_t rebuild_incarnation = 0;
    std::string next_warmcopy_id;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      auto it = m_entries.find(thread_id);
      if (it == m_entries.end() || it->second.preparing ||
          it->second.rebuilding || it->second.finalizing) {
        return true;
      }
      Entry &entry = it->second;
      const bool stale_session =
          entry.session != nullptr &&
          mysql_binlog_preserve_warmcopy_session_stale_rebuildable(
              entry.session);
      if (!stale_session && !entry.rebuild_pending) return true;

      entry.rebuilding = true;
      entry.rebuild_pending = false;
      old_session = entry.session;
      entry.session = nullptr;
      old_warmcopy_id = std::move(entry.warmcopy_id);
      old_reserved_size = entry.reserved_size;
      entry.reserved_size = 0;
      rebuild_incarnation = ++entry.rebuild_incarnation;
      next_warmcopy_id =
          "warmcopy_" + std::to_string(epoch) + "_" +
          std::to_string(static_cast<unsigned long long>(thread_id)) + "_r" +
          std::to_string(rebuild_incarnation);
      entry.warmcopy_id = next_warmcopy_id;
    }

    if (old_session != nullptr) {
      mysql_binlog_preserve_warmcopy_abort_session(old_session);
    }
    if (old_reserved_size != 0) {
      (void)warmcopy_retain_entry_reservation(
          &m_total_bytes, 0, &old_reserved_size);
    }
    if (!old_warmcopy_id.empty()) {
      (void)m_carrier->remove_warm_external_blob(
          old_warmcopy_id, kPreservedTrxBlobBinlogCache);
    }

    Mysql_binlog_warmcopy_session *new_session = nullptr;
    const bool begin_failed = mysql_binlog_preserve_warmcopy_begin_session(
        thd, next_warmcopy_id, epoch, m_carrier.get(), m_max_entry_bytes,
        &m_total_bytes, m_max_total_bytes, m_reservation_chunk_bytes,
        m_copy_chunk_bytes,
        &new_session, nullptr, nullptr, true);

    bool published = false;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      auto it = m_entries.find(thread_id);
      if (it != m_entries.end() && it->second.rebuilding &&
          it->second.rebuild_incarnation == rebuild_incarnation) {
        Entry &entry = it->second;
        entry.rebuilding = false;
        if (!begin_failed && new_session != nullptr) {
          entry.session = new_session;
          new_session = nullptr;
          published = true;
        } else {
          entry.warmcopy_id.clear();
          entry.rebuild_pending = true;
        }
        m_condition.notify_all();
      }
    }
    if (new_session != nullptr) {
      mysql_binlog_preserve_warmcopy_abort_session(new_session);
      (void)m_carrier->remove_warm_external_blob(
          next_warmcopy_id, kPreservedTrxBlobBinlogCache);
    }

    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: drain-owned active binlog mirror rebuild target=" +
            std::to_string(static_cast<unsigned long long>(thread_id)) +
            " prepared=" + std::to_string(published ? 1 : 0))
               .c_str());
    return published;
  }

  void cleanup_warm_artifacts() {
    std::unique_lock<std::mutex> guard(m_mutex);
    m_condition.wait(guard, [&]() { return m_active_finalizers == 0; });
    for (auto &entry : m_entries) {
      if (entry.second.session != nullptr) {
        mysql_binlog_preserve_warmcopy_abort_session(entry.second.session);
        entry.second.session = nullptr;
      }
      if (entry.second.reserved_size != 0) {
        (void)warmcopy_retain_entry_reservation(
            &m_total_bytes, 0, &entry.second.reserved_size);
      }
      if (!entry.second.warmcopy_id.empty()) {
        if (m_carrier != nullptr) {
          (void)m_carrier->remove_warm_external_blob(
              entry.second.warmcopy_id, kPreservedTrxBlobBinlogCache);
        }
      }
    }
  }

 private:
  std::string m_dir;
  std::unique_ptr<Preserved_trx_warm_external_blob_carrier> m_carrier;
  const uint64_t m_max_total_bytes;
  const uint64_t m_max_entry_bytes;
  const uint64_t m_reservation_chunk_bytes;
  const uint64_t m_copy_chunk_bytes;
  const uint64_t m_tail_budget_bytes;
  const uint64_t m_max_inflight_bytes;
  mutable std::mutex m_mutex;
  std::condition_variable m_condition;
  std::unordered_map<my_thread_id, Entry> m_entries;
  std::atomic<uint64_t> m_total_bytes{0};
  size_t m_active_finalizers{0};
  ulonglong m_close_deadline_us{0};
  bool m_error{false};
  bool m_allow_quiesced_rebuild_fallback{false};

  bool prepare_thd(THD *thd, uint64_t epoch,
                   bool allow_inflight_statement) {
    if (thd == nullptr) return false;
    const my_thread_id thread_id = thd->thread_id();
    const std::string warmcopy_id =
        "warmcopy_" + std::to_string(epoch) + "_" + std::to_string(thread_id);

    for (;;) {
      bool existing_entry_ready = false;
      if (!wait_for_existing_entry(thread_id, &existing_entry_ready))
        return false;
      if (existing_entry_ready) return true;
      break;
    }

    uint64_t cache_length = 0;
    if (!allow_inflight_statement) {
      bool has_current_blob = false;
      const bool cache_length_error =
          mysql_binlog_preserve_warmcopy_cache_length(
              thd, &cache_length, &has_current_blob);
      if (cache_length_error) {
        std::lock_guard<std::mutex> guard(m_mutex);
        note_token_prepare_failure_locked();
        return false;
      }
      if (!has_current_blob) cache_length = 0;
    }

    {
      std::unique_lock<std::mutex> guard(m_mutex);
      auto existing_it = m_entries.find(thread_id);
      if (existing_it != m_entries.end()) {
        while (existing_it != m_entries.end() && existing_it->second.preparing) {
          if (!wait_for_prepare_state_change_locked(thread_id, guard))
            return false;
          existing_it = m_entries.find(thread_id);
        }
        if (existing_it != m_entries.end()) return !m_error;
      }
      if (cache_length > m_max_entry_bytes) {
        note_token_prepare_failure_locked();
        return false;
      }
      Entry entry;
      entry.preparing = true;
      entry.warmcopy_id = warmcopy_id;
      m_entries.emplace(thread_id, entry);
    }

    Mysql_binlog_warmcopy_session *session = nullptr;
    if (m_carrier == nullptr) {
      std::lock_guard<std::mutex> guard(m_mutex);
      release_entry_locked(thread_id);
      m_error = true;
      return false;
    }
    if (mysql_binlog_preserve_warmcopy_begin_session(
            thd, warmcopy_id, epoch, m_carrier.get(), m_max_entry_bytes,
            &m_total_bytes, m_max_total_bytes, m_reservation_chunk_bytes,
            m_copy_chunk_bytes,
            &session, nullptr, nullptr,
            allow_inflight_statement)) {
      std::lock_guard<std::mutex> guard(m_mutex);
      release_entry_locked(thread_id);
      note_token_prepare_failure_locked();
      return false;
    }
    if (session == nullptr) {
      std::lock_guard<std::mutex> guard(m_mutex);
      release_entry_locked(thread_id);
      return false;
    }

    {
      std::lock_guard<std::mutex> guard(m_mutex);
      auto it = m_entries.find(thread_id);
      if (it != m_entries.end() && it->second.preparing &&
          it->second.session == nullptr) {
        it->second.preparing = false;
        it->second.session = session;
        session = nullptr;
        m_condition.notify_all();
      }
    }

    if (session != nullptr) mysql_binlog_preserve_warmcopy_abort_session(session);
    return session == nullptr;
  }

  bool wait_for_existing_entry(my_thread_id thread_id,
                               bool *existing_entry_ready) {
    if (existing_entry_ready != nullptr) *existing_entry_ready = false;
    std::unique_lock<std::mutex> guard(m_mutex);
    auto it = m_entries.find(thread_id);
    while (it != m_entries.end() && it->second.preparing) {
      if (!wait_for_prepare_state_change_locked(thread_id, guard)) return false;
      it = m_entries.find(thread_id);
    }
    if (it == m_entries.end()) return !m_error;
    if (existing_entry_ready != nullptr) *existing_entry_ready = true;
    return !m_error;
  }

  bool wait_for_prepare_state_change_locked(
      my_thread_id thread_id, std::unique_lock<std::mutex> &guard) {
    if (warmcopy_close_deadline_expired(m_close_deadline_us)) {
      note_token_prepare_failure_locked();
      return false;
    }
    const auto wakeup_predicate = [&]() {
      const auto it = m_entries.find(thread_id);
      return it == m_entries.end() || !it->second.preparing || m_error ||
             warmcopy_close_deadline_expired(m_close_deadline_us);
    };
    const unsigned long wait_ms = warmcopy_close_timeout_ms_until_deadline(
        m_close_deadline_us, 10);
    if (m_close_deadline_us != 0) {
      m_condition.wait_for(guard, std::chrono::milliseconds(wait_ms),
                           wakeup_predicate);
    } else {
      m_condition.wait_for(guard, std::chrono::milliseconds(10),
                           wakeup_predicate);
    }
    if (warmcopy_close_deadline_expired(m_close_deadline_us)) {
      note_token_prepare_failure_locked();
      return false;
    }
    return !m_error;
  }

  void note_token_prepare_failure_locked() {
    if (!m_allow_quiesced_rebuild_fallback) m_error = true;
  }

  void release_entry_locked(my_thread_id thread_id) {
    auto it = m_entries.find(thread_id);
    if (it == m_entries.end()) return;
    if (it->second.session != nullptr) {
      mysql_binlog_preserve_warmcopy_abort_session(it->second.session);
      it->second.session = nullptr;
    }
    if (it->second.reserved_size != 0) {
      (void)warmcopy_retain_entry_reservation(
          &m_total_bytes, 0, &it->second.reserved_size);
    }
    if (!it->second.warmcopy_id.empty()) {
      if (m_carrier != nullptr) {
        (void)m_carrier->remove_warm_external_blob(
            it->second.warmcopy_id, kPreservedTrxBlobBinlogCache);
      }
    }
    m_entries.erase(it);
    m_condition.notify_all();
  }
};

bool warmcopy_close_deadline_expired(ulonglong close_deadline_us);

class Warmcopy_tail_budget_validator final {
 public:
  Warmcopy_tail_budget_validator(
      ulonglong generation, const std::vector<my_thread_id> &target_thread_ids,
      const Warmcopy_batch_blob_provider *provider, ulonglong close_deadline_us)
      : m_generation(generation),
        m_target_thread_ids(target_thread_ids),
        m_provider(provider),
        m_close_deadline_us(close_deadline_us) {}

  void validate(THD *candidate) {
    if (m_error || m_exceeded || candidate == nullptr || m_provider == nullptr)
      return;
    if (warmcopy_close_deadline_expired(m_close_deadline_us)) {
      m_error = true;
      return;
    }

    if (!preserve_trx_quiesced_batch_target_is_valid(
            candidate, m_generation, m_target_thread_ids)) {
      m_error = true;
      return;
    }

    bool exceeded = false;
    if (candidate->binlog_flush_pending_rows_event(true) ||
        m_provider->tail_budget_exceeded(candidate, &exceeded)) {
      m_error = true;
      return;
    }
    if (exceeded) m_exceeded = true;
  }

  bool error() const { return m_error; }
  bool exceeded() const { return m_exceeded; }

 private:
  ulonglong m_generation;
  const std::vector<my_thread_id> &m_target_thread_ids;
  const Warmcopy_batch_blob_provider *m_provider;
  ulonglong m_close_deadline_us;
  bool m_error{false};
  bool m_exceeded{false};
};

class Warmcopy_prepare_quiesced_targets final {
 public:
  Warmcopy_prepare_quiesced_targets(
      ulonglong generation, const std::vector<my_thread_id> &target_thread_ids,
      Warmcopy_batch_blob_provider *provider, uint64_t epoch,
      ulonglong close_deadline_us, bool allow_quiesced_rebuild_fallback,
      bool target_attached = false)
      : m_generation(generation),
        m_target_thread_ids(target_thread_ids),
        m_provider(provider),
        m_epoch(epoch),
        m_close_deadline_us(close_deadline_us),
        m_allow_quiesced_rebuild_fallback(
            allow_quiesced_rebuild_fallback),
        m_target_attached(target_attached) {}

  void prepare(THD *candidate) {
    if (m_error || candidate == nullptr || m_provider == nullptr) return;
    if (warmcopy_close_deadline_expired(m_close_deadline_us)) {
      m_error = true;
      return;
    }

    const bool target_valid =
        m_target_attached
            ? preserve_trx_attached_batch_target_is_valid(
                  candidate, m_generation, m_target_thread_ids)
            : preserve_trx_quiesced_batch_target_is_valid(
                  candidate, m_generation, m_target_thread_ids);
    if (!target_valid) {
      m_error = true;
      return;
    }

    if (candidate->binlog_flush_pending_rows_event(true)) {
      m_error = true;
      return;
    }
    if (!m_provider->prepare_quiesced_blob_for_thd(candidate, m_epoch) &&
        !m_allow_quiesced_rebuild_fallback) {
      m_error = true;
    }
  }

  bool error() const { return m_error; }

 private:
  ulonglong m_generation;
  const std::vector<my_thread_id> &m_target_thread_ids;
  Warmcopy_batch_blob_provider *m_provider;
  uint64_t m_epoch;
  ulonglong m_close_deadline_us;
  bool m_allow_quiesced_rebuild_fallback;
  bool m_target_attached;
  bool m_error{false};
};

class Warmcopy_prepare_idle_participants final : public Do_THD_Impl {
 public:
  explicit Warmcopy_prepare_idle_participants(THD *owner) : m_owner(owner) {}

  void operator()(THD *candidate) override {
    if (candidate == nullptr || candidate == m_owner) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    const bool candidate_ready = !candidate->release_resources_done() &&
                                 !candidate->is_system_thread() &&
                                 candidate->killed == THD::NOT_KILLED &&
                                 candidate->m_server_idle &&
                                 candidate->preserve_trx_batch_state ==
                                     Preserve_trx_batch_thd_state::NONE &&
                                 preserve_trx_has_lock_warmcopy_phase1_candidate_transaction(
                                     candidate);
    std::unique_ptr<Preserve_trx_external_thd_pin> pin;
    if (candidate_ready && !preserve_trx_is_unsupported_common_context(candidate))
      pin = Preserve_trx_external_thd_pin::acquire_locked(candidate);
    mysql_mutex_unlock(&candidate->LOCK_thd_data);

    if (pin != nullptr) m_targets.push_back({candidate, std::move(pin)});
  }

  std::vector<Preserve_trx_pinned_thd> &targets() { return m_targets; }

 private:
  THD *m_owner;
  std::vector<Preserve_trx_pinned_thd> m_targets;
};

class Warmcopy_prepare_active_participants final : public Do_THD_Impl {
 public:
  explicit Warmcopy_prepare_active_participants(THD *owner)
      : m_owner(owner) {}

  void operator()(THD *candidate) override {
    if (candidate == nullptr || candidate == m_owner) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    const bool candidate_ready =
        !candidate->release_resources_done() && !candidate->is_system_thread() &&
        candidate->killed == THD::NOT_KILLED && !candidate->m_server_idle &&
        candidate->preserve_trx_batch_state ==
            Preserve_trx_batch_thd_state::NONE &&
        preserve_trx_has_lock_warmcopy_phase1_candidate_transaction(candidate) &&
        !preserve_trx_is_unsupported_common_context(candidate);
    std::unique_ptr<Preserve_trx_external_thd_pin> pin;
    if (candidate_ready)
      pin = Preserve_trx_external_thd_pin::acquire_locked(candidate);
    mysql_mutex_unlock(&candidate->LOCK_thd_data);

    if (pin != nullptr) m_targets.push_back({candidate, std::move(pin)});
  }

  std::vector<Preserve_trx_pinned_thd> &targets() { return m_targets; }

 private:
  THD *m_owner;
  std::vector<Preserve_trx_pinned_thd> m_targets;
};

static bool prepare_lock_warmcopy_idle_targets(
    THD *owner, Preserve_trx_lock_warmcopy_drain_participant *participant) {
  if (participant == nullptr) return false;

  Warmcopy_prepare_idle_participants prepare_lock_warmcopy(owner);
  Global_THD_manager::get_instance()->do_for_all_thd_copy(
      &prepare_lock_warmcopy);
  for (const Preserve_trx_pinned_thd &target :
       prepare_lock_warmcopy.targets()) {
    if (target.thd == nullptr ||
        !preserve_trx_has_lock_warmcopy_phase1_candidate_transaction(
            target.thd) ||
        preserve_trx_is_unsupported_common_context(target.thd)) {
      continue;
    }
    if (!participant->prepare_phase1_idle_target(target.thd)) return true;
  }
  return false;
}

static bool prepare_lock_warmcopy_active_record_targets(
    THD *owner, Preserve_trx_lock_warmcopy_drain_participant *participant) {
  if (participant == nullptr) return false;

  Warmcopy_prepare_active_participants prepare_lock_warmcopy(owner);
  Global_THD_manager::get_instance()->do_for_all_thd_copy(
      &prepare_lock_warmcopy);
  for (const Preserve_trx_pinned_thd &target :
       prepare_lock_warmcopy.targets()) {
    if (target.thd == nullptr ||
        !preserve_trx_has_lock_warmcopy_phase1_candidate_transaction(
            target.thd) ||
        preserve_trx_is_unsupported_common_context(target.thd)) {
      continue;
    }
    if (!participant->prepare_phase1_record_scan_target(target.thd, true)) {
      return true;
    }
  }
  DEBUG_SYNC(owner, "preserve_trx_lock_warmcopy_after_active_record_scan");
  return false;
}

static bool stream_phase1_transfer_binlog_cache_blobs(
    THD *owner, ulonglong generation,
    const std::set<my_thread_id> &declared_tokens,
    Preserve_trx_transfer_source_epoch_session *source_session,
    Phase1_transfer_binlog_blob_provider *phase1_binlog_provider,
    Warmcopy_batch_blob_provider *live_binlog_provider,
    Preserve_trx_transfer_phase1_batch_sender *phase1_batch_sender) {
  if (source_session == nullptr || declared_tokens.empty()) return false;

  std::unique_ptr<Preserved_trx_warm_external_blob_carrier> warm_carrier =
      create_preserved_trx_process_local_warm_external_blob_carrier(
          preserve_trx_default_dir());
  if (warm_carrier == nullptr) return true;

  Preserve_batch_phase1_declared_target_pin_collector targets(owner,
                                                              declared_tokens);
  Global_THD_manager::get_instance()->do_for_all_thd_copy(&targets);
  if (targets.error()) return true;
  DEBUG_SYNC(owner, "preserve_trx_transfer_after_phase1_target_collection");

  std::vector<std::pair<my_thread_id, PrebuiltBinlogCacheBlob>> built_blobs;
  bool enqueue_failed = false;
  size_t deferred_build_count = 0;
  for (const Preserve_batch_phase1_declared_target_pin_collector::Target
           &target : targets.targets()) {
    if (target.thd == nullptr) continue;
    const my_thread_id target_thread_id = target.thd->thread_id();
    /*
      Phase 1 does not own the target's command boundary, even if the target was
      observed idle while collecting its lifetime pin. Always capture through
      the live mirror: it snapshots the serialized cache prefix without flushing
      a pending Rows_log_event owned by a command that started after the sample.
      A later truncate invalidates only this token and falls back to the
      quiesced rebuild path.
    */
    PrebuiltBinlogCacheBlob prebuilt_binlog;
    const std::string warmcopy_id =
        "transfer_binlog_" + std::to_string(generation) + "_" +
        std::to_string(static_cast<unsigned long long>(target_thread_id));
    PrebuiltBinlogCacheBlob live_prefix;
    if (live_binlog_provider == nullptr ||
        !live_binlog_provider->prepare_active_blob_for_thd(target.thd,
                                                           generation) ||
        !live_binlog_provider->phase1_prefix_blob_for_thd(target.thd,
                                                          &live_prefix) ||
        live_prefix.size > preserve_trx_max_binlog_cache_bytes) {
      ++deferred_build_count;
      continue;
    }
    Preserved_trx_external_blob_descriptor prefix_descriptor;
    prefix_descriptor.name = live_prefix.name;
    prefix_descriptor.size = live_prefix.size;
    prefix_descriptor.digest = live_prefix.digest;
    const Preserved_trx_carrier_status snapshot_status =
        warm_carrier->snapshot_active_warm_external_blob_prefix(
            live_prefix.warmcopy_id, warmcopy_id, live_prefix.name,
            live_prefix.warmcopy_epoch, prefix_descriptor);
    if (snapshot_status != Preserved_trx_carrier_status::OK) {
      ++deferred_build_count;
      continue;
    }
    prebuilt_binlog = live_prefix;
    prebuilt_binlog.warmcopy_id = warmcopy_id;

    Preserve_trx_transfer_phase1_blob_request request;
    request.transfer_token = static_cast<uint64_t>(target_thread_id);
    request.object_id = prebuilt_binlog.name;
    request.warmcopy_id = prebuilt_binlog.warmcopy_id;
    request.warmcopy_epoch = prebuilt_binlog.warmcopy_epoch;
    request.size = prebuilt_binlog.size;
    request.digest = prebuilt_binlog.digest;
    const Preserve_trx_transfer_status enqueue_status =
        phase1_batch_sender == nullptr
            ? preserve_trx_transfer_stream_prebuilt_binlog_cache_blob(
                  source_session, static_cast<uint64_t>(target_thread_id),
                  preserve_trx_default_dir(), prebuilt_binlog)
            : phase1_batch_sender->enqueue(request);
    built_blobs.emplace_back(target_thread_id, prebuilt_binlog);
    if (enqueue_status != Preserve_trx_transfer_status::OK) {
      enqueue_failed = true;
      break;
    }
  }
  if (deferred_build_count != 0) {
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: standby transfer deferred phase1 binlog cache rebuild "
            "until quiescence token_count=" +
            std::to_string(deferred_build_count))
               .c_str());
  }
  const Preserve_trx_transfer_status flush_status =
      phase1_batch_sender == nullptr ? Preserve_trx_transfer_status::OK
                                     : phase1_batch_sender->flush();
  bool cleanup_failed = false;
  for (const auto &entry : built_blobs) {
    if (!enqueue_failed && flush_status == Preserve_trx_transfer_status::OK &&
        phase1_binlog_provider != nullptr) {
      phase1_binlog_provider->remember_phase1_blob(entry.first, entry.second);
      continue;
    }
    if (warm_carrier->remove_warm_external_blob(entry.second.warmcopy_id,
                                                entry.second.name) !=
        Preserved_trx_carrier_status::OK) {
      cleanup_failed = true;
    }
  }
  return enqueue_failed || flush_status != Preserve_trx_transfer_status::OK ||
         cleanup_failed;
}

enum class Preserve_trx_active_binlog_progress_result {
  NO_PROGRESS,
  QUEUED,
  STREAMED,
  FAILED
};

struct Preserve_trx_pending_binlog_publication {
  my_thread_id thread_id{0};
  PrebuiltBinlogCacheBlob blob;
  bool owns_cleanup{false};
  bool release_live_artifact{false};
  bool report_active_progress{false};
  uint64_t published_bytes{0};
  bool replacement{false};
  bool final_hwm{false};
  bool remote_presealed{false};
  bool retire_stale_live_artifact{false};
};

#ifndef NDEBUG
static void log_acked_active_binlog_progress(
    my_thread_id thread_id, const PrebuiltBinlogCacheBlob &blob,
    uint64_t published_bytes, bool replacement, bool final_hwm, bool queued) {
  LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
         ("PRESERVE: ACKed active binlog mirror progress target=" +
          std::to_string(static_cast<unsigned long long>(thread_id)) +
          " high_watermark=" + std::to_string(blob.size) +
          " delta_bytes=" + std::to_string(published_bytes) +
          " replacement=" + std::to_string(replacement ? 1 : 0) +
          " final=" + std::to_string(final_hwm ? 1 : 0) +
          " queued=" + std::to_string(queued ? 1 : 0))
             .c_str());
}
#endif

static void publish_acked_transfer_binlog_cache_progress(
    Phase1_transfer_binlog_blob_provider *phase1_provider,
    Warmcopy_batch_blob_provider *live_provider,
    const Preserve_trx_pending_binlog_publication &pending) {
  if (phase1_provider == nullptr) return;
  if (pending.retire_stale_live_artifact && live_provider != nullptr) {
    PrebuiltBinlogCacheBlob cleanup_blob;
    if (live_provider->retire_stale_artifact(pending.thread_id,
                                             &cleanup_blob) &&
        !cleanup_blob.warmcopy_id.empty()) {
      phase1_provider->adopt_cleanup_ownership(cleanup_blob);
    }
  }
  if (pending.release_live_artifact && live_provider != nullptr) {
    (void)live_provider->release_finalized_artifact(pending.thread_id,
                                                    pending.blob);
  }
  phase1_provider->remember_phase1_blob(pending.thread_id, pending.blob,
                                       pending.owns_cleanup,
                                       pending.remote_presealed);
#ifndef NDEBUG
  if (pending.report_active_progress) {
    log_acked_active_binlog_progress(
        pending.thread_id, pending.blob, pending.published_bytes,
        pending.replacement, pending.final_hwm, true);
  }
#endif
}

static void publish_acked_transfer_binlog_cache_progress(
    Phase1_transfer_binlog_blob_provider *phase1_provider,
    Warmcopy_batch_blob_provider *live_provider,
    const std::vector<Preserve_trx_pending_binlog_publication>
        &pending_publications) {
  for (const Preserve_trx_pending_binlog_publication &pending :
       pending_publications) {
    publish_acked_transfer_binlog_cache_progress(phase1_provider,
                                                 live_provider, pending);
  }
}

static Preserve_trx_active_binlog_progress_result
stream_one_active_transfer_binlog_cache_progress(
    THD *target, uint64_t generation, uint64_t minimum_delta_bytes,
    bool final_hwm, bool allow_stale_rebuild,
    Preserve_trx_transfer_source_epoch_session *source_session,
    Phase1_transfer_binlog_blob_provider *phase1_provider,
    Warmcopy_batch_blob_provider *live_provider,
    Preserve_trx_transfer_phase1_batch_sender *batch_sender,
    std::vector<Preserve_trx_pending_binlog_publication>
        *pending_publications,
    uint64_t *streamed_bytes = nullptr) {
  if (streamed_bytes != nullptr) *streamed_bytes = 0;
  if (target == nullptr || generation == 0 || source_session == nullptr ||
      phase1_provider == nullptr || live_provider == nullptr) {
    return Preserve_trx_active_binlog_progress_result::NO_PROGRESS;
  }

  PrebuiltBinlogCacheBlob preserved_prefix;
  const bool has_preserved_prefix =
      phase1_provider->phase1_blob_for_thd(target, &preserved_prefix);
  PrebuiltBinlogCacheBlob next_prefix;
  std::string delta_payload;
  bool replace_preserved_prefix = false;
  bool remote_presealed = false;
  if (!final_hwm && !allow_stale_rebuild &&
      live_provider->stale_rebuildable_for_thd(target)) {
    return Preserve_trx_active_binlog_progress_result::NO_PROGRESS;
  }
  if (final_hwm) {
    const uint64_t inline_limit = std::min<uint64_t>(
        source_session->max_inflight_bytes(),
        std::max<uint64_t>(source_session->chunk_bytes(),
                           source_session->phase1_batch_bytes()));
    remote_presealed = live_provider->build_quiesced_stale_inline_blob(
        target, generation, inline_limit, &next_prefix, &delta_payload);
    replace_preserved_prefix = remote_presealed;
  }
  if (!remote_presealed &&
      (!live_provider->prepare_active_blob_for_thd(target, generation) ||
       !live_provider->phase1_prefix_delta_for_thd(
           target, has_preserved_prefix ? &preserved_prefix : nullptr,
           minimum_delta_bytes, &next_prefix, &delta_payload,
           &replace_preserved_prefix))) {
    return Preserve_trx_active_binlog_progress_result::NO_PROGRESS;
  }
  if (final_hwm) {
    const bool metadata_export_failed =
        mysql_binlog_preserve_export_metadata_only(target,
                                                   &next_prefix.metadata);
    if (metadata_export_failed || !next_prefix.metadata.has_cache_length ||
        next_prefix.metadata.cache_length != next_prefix.size) {
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: active binlog mirror final metadata rejected target=" +
              std::to_string(
                  static_cast<unsigned long long>(target->thread_id())) +
              " export_failed=" +
              std::to_string(metadata_export_failed ? 1 : 0) +
              " has_cache_length=" +
              std::to_string(next_prefix.metadata.has_cache_length ? 1 : 0) +
              " metadata_cache_length=" +
              std::to_string(next_prefix.metadata.cache_length) +
              " mirror_size=" + std::to_string(next_prefix.size))
                 .c_str());
      return Preserve_trx_active_binlog_progress_result::FAILED;
    }
  }

  Preserve_trx_transfer_phase1_blob_request request;
  request.transfer_token = static_cast<uint64_t>(target->thread_id());
  request.object_id = next_prefix.name;
  request.warmcopy_id = next_prefix.warmcopy_id;
  request.warmcopy_epoch = next_prefix.warmcopy_epoch;
  request.size = next_prefix.size;
  request.digest = next_prefix.digest;
  request.inline_payload = std::move(delta_payload);
  if (has_preserved_prefix && !replace_preserved_prefix) {
    request.preserved_prefix_size = preserved_prefix.size;
    request.preserved_prefix_digest = preserved_prefix.digest;
  }
  const uint64_t published_bytes = request.inline_payload.size();

  const bool queued = batch_sender != nullptr;
  const Preserve_trx_transfer_status status =
      queued ? batch_sender->enqueue(request)
             : source_session->stream_prebuilt_blobs_batch(
                   preserve_trx_default_dir(), {request});
  if (status != Preserve_trx_transfer_status::OK) {
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: active binlog mirror progress failed target=" +
            std::to_string(
                static_cast<unsigned long long>(target->thread_id())) +
            " status=" + std::to_string(static_cast<int>(status)) +
            " final=" + std::to_string(final_hwm ? 1 : 0))
               .c_str());
    return Preserve_trx_active_binlog_progress_result::FAILED;
  }

  if (queued) {
    if (pending_publications == nullptr) {
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: active binlog mirror publication missing target=" +
              std::to_string(
                  static_cast<unsigned long long>(target->thread_id())))
                 .c_str());
      return Preserve_trx_active_binlog_progress_result::FAILED;
    }
    try {
      Preserve_trx_pending_binlog_publication pending;
      pending.thread_id = target->thread_id();
      pending.blob = next_prefix;
      pending.report_active_progress = true;
      pending.published_bytes = published_bytes;
      pending.replacement = replace_preserved_prefix;
      pending.final_hwm = final_hwm;
      pending.remote_presealed = remote_presealed;
      pending.retire_stale_live_artifact = remote_presealed;
      pending_publications->push_back(std::move(pending));
    } catch (...) {
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: active binlog mirror publication allocation failed "
              "target=" +
              std::to_string(
                  static_cast<unsigned long long>(target->thread_id())))
                 .c_str());
      return Preserve_trx_active_binlog_progress_result::FAILED;
    }
  } else {
    if (remote_presealed) {
      PrebuiltBinlogCacheBlob cleanup_blob;
      if (live_provider->retire_stale_artifact(target->thread_id(),
                                               &cleanup_blob) &&
          !cleanup_blob.warmcopy_id.empty()) {
        phase1_provider->adopt_cleanup_ownership(cleanup_blob);
      }
    }
    phase1_provider->remember_phase1_blob(target->thread_id(), next_prefix,
                                         false, remote_presealed);
#ifndef NDEBUG
    log_acked_active_binlog_progress(
        target->thread_id(), next_prefix, published_bytes,
        replace_preserved_prefix, final_hwm, false);
#endif
  }
  if (streamed_bytes != nullptr) *streamed_bytes = published_bytes;
  return queued ? Preserve_trx_active_binlog_progress_result::QUEUED
                : Preserve_trx_active_binlog_progress_result::STREAMED;
}

/*
  Advance receiver binlog objects from active live mirrors without waiting for
  the owning command to finish. Truncate/reset staleness is rebuilt per token by
  this drain-owned progress loop; ordinary binlog writes never perform rebuild
  I/O.
*/
static bool stream_active_transfer_binlog_cache_progress(
    THD *owner, uint64_t generation,
    const std::set<my_thread_id> &target_thread_ids,
    Preserve_trx_transfer_source_epoch_session *source_session,
    Phase1_transfer_binlog_blob_provider *phase1_provider,
    Warmcopy_batch_blob_provider *live_provider,
    Preserve_trx_transfer_phase1_batch_sender *batch_sender,
    bool finalize_idle_targets, bool allow_stale_rebuild,
    bool defer_ack_until_barrier) {
  if (owner == nullptr || generation == 0 || target_thread_ids.empty() ||
      source_session == nullptr || phase1_provider == nullptr ||
      live_provider == nullptr) {
    return false;
  }

  Preserve_batch_phase1_declared_target_pin_collector targets(
      owner, target_thread_ids);
  Global_THD_manager::get_instance()->do_for_all_thd_copy(&targets);
  if (targets.error()) {
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           "PRESERVE: active binlog mirror target pin collection failed");
    return true;
  }

  uint64_t active_target_count = 0;
  for (const auto &target : targets.targets()) {
    if (target.thd != nullptr && !target.idle) ++active_target_count;
  }
  active_target_count = std::max<uint64_t>(1, active_target_count);
  const uint64_t progress_batch_target_bytes =
      source_session->phase1_batch_bytes() == 0
          ? source_session->chunk_bytes()
          : source_session->phase1_batch_bytes();
  const uint64_t minimum_delta_bytes = std::max<uint64_t>(
      1, progress_batch_target_bytes / active_target_count +
             (progress_batch_target_bytes % active_target_count != 0 ? 1 : 0));
  uint64_t queued_count = 0;
  uint64_t streamed_count = 0;
  uint64_t streamed_bytes = 0;
  uint64_t final_idle_count = 0;
  std::vector<Preserve_trx_pending_binlog_publication> pending_publications;
  pending_publications.reserve(targets.targets().size());
  for (const auto &target : targets.targets()) {
    if (target.thd == nullptr || (target.idle && !finalize_idle_targets)) {
      continue;
    }
    const bool final_hwm = target.idle;
    uint64_t target_streamed_bytes = 0;
    const Preserve_trx_active_binlog_progress_result result =
        stream_one_active_transfer_binlog_cache_progress(
            target.thd, generation, final_hwm ? 1 : minimum_delta_bytes,
            final_hwm, allow_stale_rebuild, source_session, phase1_provider,
            live_provider, batch_sender, &pending_publications,
            &target_streamed_bytes);
    if (result == Preserve_trx_active_binlog_progress_result::FAILED) {
      return true;
    }
    if (result == Preserve_trx_active_binlog_progress_result::QUEUED) {
      ++queued_count;
      if (final_hwm) ++final_idle_count;
    } else if (result ==
               Preserve_trx_active_binlog_progress_result::STREAMED) {
      ++streamed_count;
      if (final_hwm) ++final_idle_count;
    }
    streamed_bytes += target_streamed_bytes;
  }
  if (queued_count != 0) {
    if (defer_ack_until_barrier) {
      for (const Preserve_trx_pending_binlog_publication &pending :
           pending_publications) {
        if (pending.final_hwm || pending.owns_cleanup ||
            pending.release_live_artifact || pending.remote_presealed ||
            pending.retire_stale_live_artifact) {
          LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                 "PRESERVE: active binlog mirror deferred publication has "
                 "unexpected side effects");
          return true;
        }
        phase1_provider->remember_locally_queued_blob(pending.thread_id,
                                                      pending.blob);
      }
    } else {
      if (batch_sender->flush() != Preserve_trx_transfer_status::OK) {
        LogErr(
            INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
            ("PRESERVE: active binlog mirror batch flush failed "
             "queued_tokens=" +
             std::to_string(queued_count))
                .c_str());
        return true;
      }
      publish_acked_transfer_binlog_cache_progress(
          phase1_provider, live_provider, pending_publications);
    }
  }
  if (queued_count != 0 || streamed_count != 0) {
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: active binlog mirror progress batch queued_tokens=" +
            std::to_string(queued_count) +
            " directly_streamed_tokens=" + std::to_string(streamed_count) +
            " final_idle_tokens=" + std::to_string(final_idle_count) +
            " bytes=" + std::to_string(streamed_bytes) +
            " ack_deferred=" +
            std::to_string(defer_ack_until_barrier ? 1 : 0))
               .c_str());
  }
  return false;
}

enum class Preserve_trx_binlog_catchup_result {
  CURRENT,
  NOT_REQUIRED,
  QUEUED,
  STREAMED,
  FAILED
};

/* Refresh one stale phase-1 binlog object after its target becomes quiesced. */
static Preserve_trx_binlog_catchup_result
stream_quiesced_transfer_binlog_cache_catchup(
    THD *target, uint64_t generation,
    Preserve_trx_transfer_source_epoch_session *source_session,
    Phase1_transfer_binlog_blob_provider *phase1_provider,
    Warmcopy_batch_blob_provider *live_provider,
    Preserve_trx_transfer_phase1_batch_sender *batch_sender,
    std::vector<Preserve_trx_pending_binlog_publication>
        *pending_publications,
    uint64_t *streamed_bytes = nullptr,
    const char **failure_stage = nullptr) {
  if (streamed_bytes != nullptr) *streamed_bytes = 0;
  if (failure_stage != nullptr) *failure_stage = nullptr;
  auto failed = [&](const char *stage) {
    if (failure_stage != nullptr) *failure_stage = stage;
    return Preserve_trx_binlog_catchup_result::FAILED;
  };
  if (target == nullptr || generation == 0 || source_session == nullptr ||
      phase1_provider == nullptr) {
    return failed("invalid_argument");
  }

  uint64_t final_hwm_streamed_bytes = 0;
  const Preserve_trx_active_binlog_progress_result final_hwm_status =
      stream_one_active_transfer_binlog_cache_progress(
          target, generation, 1, true, false, source_session, phase1_provider,
          live_provider, batch_sender, pending_publications,
          &final_hwm_streamed_bytes);
  if (final_hwm_status == Preserve_trx_active_binlog_progress_result::FAILED) {
    return failed("final_hwm_stream_failed");
  }
  if (final_hwm_status ==
      Preserve_trx_active_binlog_progress_result::QUEUED) {
    if (streamed_bytes != nullptr) {
      *streamed_bytes = final_hwm_streamed_bytes;
    }
    return Preserve_trx_binlog_catchup_result::QUEUED;
  }
  if (phase1_provider->phase1_blob_matches_current_thd(target)) {
    return final_hwm_status ==
                   Preserve_trx_active_binlog_progress_result::STREAMED
               ? Preserve_trx_binlog_catchup_result::STREAMED
               : Preserve_trx_binlog_catchup_result::CURRENT;
  }
  PrebuiltBinlogCacheBlob preserved_prefix;
  const bool has_preserved_prefix =
      phase1_provider->phase1_blob_for_thd(target, &preserved_prefix);

  uint64_t cache_length = 0;
  bool has_binlog_cache = false;
  if (mysql_binlog_preserve_warmcopy_cache_length(
          target, &cache_length, &has_binlog_cache)) {
    return failed("length_failed");
  }
  if (!has_binlog_cache || cache_length == 0) {
    return Preserve_trx_binlog_catchup_result::NOT_REQUIRED;
  }
  if (cache_length > preserve_trx_max_binlog_cache_bytes) {
    return failed("budget_rejected");
  }

  PrebuiltBinlogCacheBlob prebuilt_binlog;
  if (phase1_provider->finalize_for_preserve(
          target, std::to_string(static_cast<unsigned long long>(
                      target->thread_id())),
          &prebuilt_binlog, PreserveBinlogBlobFinalizeContext{}) !=
      Preserve_snapshot_status::OK) {
    return failed("finalize_failed");
  }

  const bool append_preserved_prefix =
      has_preserved_prefix &&
      preserved_prefix.name == kPreservedTrxBlobBinlogCache &&
      preserved_prefix.size != 0 &&
      preserved_prefix.size < prebuilt_binlog.size &&
      preserved_prefix.phase1_truncate_generation ==
          prebuilt_binlog.phase1_truncate_generation;
  Preserve_trx_transfer_phase1_blob_request request;
  request.transfer_token = static_cast<uint64_t>(target->thread_id());
  request.object_id = prebuilt_binlog.name;
  request.warmcopy_id = prebuilt_binlog.warmcopy_id;
  request.warmcopy_epoch = prebuilt_binlog.warmcopy_epoch;
  request.size = prebuilt_binlog.size;
  request.digest = prebuilt_binlog.digest;
  if (append_preserved_prefix) {
    request.preserved_prefix_size = preserved_prefix.size;
    request.preserved_prefix_digest = preserved_prefix.digest;
  }

  Preserve_trx_transfer_object_descriptor final_descriptor;
  final_descriptor.object_id = prebuilt_binlog.name;
  final_descriptor.kind =
      Preserve_trx_transfer_object_kind::EXTERNAL_BLOB;
  final_descriptor.total_size = prebuilt_binlog.size;
  final_descriptor.digest = prebuilt_binlog.digest;
  const bool already_presealed =
      source_session->object_presealed_for_token(
          static_cast<uint64_t>(target->thread_id()), final_descriptor);

  if (!already_presealed) {
    Preserve_trx_transfer_status stream_status =
        Preserve_trx_transfer_status::OK;
    if (batch_sender == nullptr) {
      stream_status = preserve_trx_transfer_stream_prebuilt_blobs_batch(
          source_session, preserve_trx_default_dir(), {request}, 0);
    } else {
      stream_status = batch_sender->enqueue(request);
    }
    if (stream_status != Preserve_trx_transfer_status::OK) {
      const std::string message =
          "PRESERVE: standby transfer binlog catchup stream failed status=" +
          std::to_string(static_cast<int>(stream_status)) + " target=" +
          std::to_string(
              static_cast<unsigned long long>(target->thread_id()));
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
      return failed("stream_failed");
    }
  }
  if (batch_sender != nullptr && !already_presealed) {
    if (pending_publications == nullptr) {
      return failed("pending_publication_missing");
    }
    try {
      pending_publications->push_back(
          {target->thread_id(), prebuilt_binlog, true, true});
    } catch (...) {
      return failed("pending_publication_failed");
    }
  } else {
    if (live_provider != nullptr) {
      (void)live_provider->release_finalized_artifact(target->thread_id(),
                                                      prebuilt_binlog);
    }
    phase1_provider->remember_phase1_blob(target->thread_id(), prebuilt_binlog);
  }
  if (streamed_bytes != nullptr) {
    *streamed_bytes =
        final_hwm_streamed_bytes +
        (already_presealed
             ? 0
             : prebuilt_binlog.size -
                   (append_preserved_prefix ? preserved_prefix.size : 0));
  }
  if (already_presealed) {
    return final_hwm_status ==
                   Preserve_trx_active_binlog_progress_result::STREAMED
               ? Preserve_trx_binlog_catchup_result::STREAMED
               : Preserve_trx_binlog_catchup_result::CURRENT;
  }
  return batch_sender == nullptr ? Preserve_trx_binlog_catchup_result::STREAMED
                                 : Preserve_trx_binlog_catchup_result::QUEUED;
}

/*
  Preserve one already-quiesced batch target.

  The wrapper revalidates the target's generation/state, temporarily runs the
  preserve kernel under the target THD context, maps the result to
  PRESERVED_DRAINED on success, or clears the batch state back to NONE on
  failure. The recorded flags let the outer all-or-nothing batch path decide
  whether it must roll back a claimed ACTIVE-Undo trx, delete a token, or only
  release in-memory quiesce ownership.
*/
using Preserve_batch_attached_target_preparer =
    std::function<const char *(THD *)>;

class Preserve_batch_quiesced_idle_target final {
 public:
  Preserve_batch_quiesced_idle_target(THD *owner,
                                      const Preserve_trx_options &options,
                                      ulonglong timeout_seconds,
                                      ulonglong generation,
                                      my_thread_id target_thread_id,
                                      PreserveBinlogBlobProvider
                                          *binlog_blob_provider = nullptr,
                                      const Preserve_trx_lock_warmcopy_artifact
                                          *lock_warmcopy_artifact = nullptr,
                                      bool debug_fail_ha_prepare_low = false,
                                      bool debug_fail_temp_only_prepare = false,
                                      bool defer_snapshot_directory_fsync = false,
                                      Preserve_trx_transfer_source_epoch_session
                                          *transfer_source_epoch_session =
                                              nullptr,
                                      std::string transfer_preserve_dir =
                                          std::string(),
                                      std::string preselected_token =
                                          std::string(),
                                      const char *worker_thread_stack = nullptr,
                                      ulonglong attach_deadline_us = 0,
                                      std::shared_ptr<Preserve_trx_drain_attempt>
                                          drain_attempt = nullptr,
                                      Preserve_batch_attached_target_preparer
                                          attached_target_preparer = {},
                                      Preserve_trx_deferred_transfer_candidate
                                          *deferred_transfer_candidate =
                                              nullptr)
      : m_owner(owner),
        m_options(options),
        m_timeout_seconds(timeout_seconds),
        m_generation(generation),
        m_target_thread_id(target_thread_id),
        m_binlog_blob_provider(binlog_blob_provider),
        m_lock_warmcopy_artifact(lock_warmcopy_artifact),
        m_debug_fail_ha_prepare_low(debug_fail_ha_prepare_low),
        m_debug_fail_temp_only_prepare(debug_fail_temp_only_prepare),
        m_defer_snapshot_directory_fsync(defer_snapshot_directory_fsync),
        m_transfer_source_epoch_session(transfer_source_epoch_session),
        m_transfer_preserve_dir(std::move(transfer_preserve_dir)),
        m_preselected_token(std::move(preselected_token)),
        m_worker_thread_stack(worker_thread_stack),
        m_attach_deadline_us(attach_deadline_us),
        m_drain_attempt(std::move(drain_attempt)),
        m_attached_target_preparer(std::move(attached_target_preparer)),
        m_deferred_transfer_candidate(deferred_transfer_candidate) {}

  void run(THD *candidate) {
    if (m_visited_target || candidate == nullptr) return;

    const std::vector<my_thread_id> single_target{m_target_thread_id};
    if (!preserve_trx_quiesced_batch_target_is_member(
            candidate, m_generation, single_target)) {
      m_result.failure_reason = "batch_target_membership_lost";
      m_error = true;
      m_visited_target = true;
      clear_target_after_error(candidate);
      return;
    }

    bool early_target_error = false;
    // A CLOSING packet may still be returning 4020. Keep the lifetime pin, but
    // wait outside server locks until the existing context switch owns the THD.
    for (;;) {
      if (preserve_trx_active_drain_reset_requested(m_drain_attempt)) {
        m_result.failure_reason = "batch_target_attach_reset";
        m_error = true;
        m_visited_target = true;
        early_target_error = true;
        break;
      }
      if (!preserve_trx_quiesced_batch_target_is_member(
              candidate, m_generation, single_target)) {
        m_result.failure_reason = "batch_target_attach_membership_lost";
        m_error = true;
        m_visited_target = true;
        early_target_error = true;
        break;
      }
      Preserve_thd_context_switch switch_thd(m_owner, candidate, m_generation,
                                             m_worker_thread_stack);
      if (!switch_thd.active()) {
        if (m_attach_deadline_us != 0 &&
            preserve_trx_monotonic_deadline_expired_at(
                m_attach_deadline_us, preserve_trx_monotonic_us())) {
          m_result.failure_reason = "batch_target_attach_deadline";
          m_error = true;
          m_visited_target = true;
          early_target_error = true;
          break;
        }
        my_sleep(1000);
        continue;
      } else if (preserve_trx_is_unsupported_common_context(candidate)) {
        m_result.failure_reason = "batch_target_attach_unsupported";
        m_error = true;
        m_visited_target = true;
        early_target_error = true;
      } else {
        const char *prepare_failure =
            m_attached_target_preparer == nullptr
                ? nullptr
                : m_attached_target_preparer(candidate);
        if (prepare_failure != nullptr) {
          m_result.failure_reason = prepare_failure;
          m_error = true;
          m_visited_target = true;
          early_target_error = true;
        } else {
          m_error = preserve_trx_preserve_attached_transaction(
              candidate, m_options, m_timeout_seconds, &m_result,
              m_binlog_blob_provider, m_lock_warmcopy_artifact,
              m_debug_fail_ha_prepare_low, m_debug_fail_temp_only_prepare,
              m_defer_snapshot_directory_fsync,
              m_transfer_source_epoch_session, m_transfer_preserve_dir,
              m_preselected_token, true,
              m_deferred_transfer_candidate,
              m_drain_attempt == nullptr ? nullptr
                                         : &m_drain_attempt->ownership);
        }
      }
      break;
    }
    if (early_target_error) {
      clear_target_after_error(candidate);
      return;
    }

    if (m_error && !m_result.cleanup_failed_after_reattach &&
        !m_result.reattached_to_original_thd &&
        !m_result.cleanup_completed_after_detach_failure &&
        (m_result.durable_point_crossed ||
         m_result.detached_from_original_thd ||
         m_result.stage >= Preserve_trx_preserve_stage::DETACH)) {
      m_result.cleanup_failed_after_reattach = true;
    }

    const bool restored_active_to_original_thd =
        m_result.reattached_to_original_thd &&
        !m_result.left_preserved_after_cleanup_failure;
    const bool unresolved_detached_state =
        !restored_active_to_original_thd &&
        !m_result.cleanup_completed_after_detach_failure &&
        (m_result.durable_point_crossed ||
         m_result.detached_from_original_thd ||
         m_result.stage >= Preserve_trx_preserve_stage::DETACH);
    const bool must_keep_target_drained = !m_error || unresolved_detached_state;
    preserve_trx_set_batch_state(
        candidate, must_keep_target_drained ? m_generation : 0,
        must_keep_target_drained ? Preserve_trx_batch_thd_state::PRESERVED_DRAINED
                                 : Preserve_trx_batch_thd_state::NONE);
    m_visited_target = true;
  }

  bool visited_target() const { return m_visited_target; }
  bool error() const { return m_error; }
  Preserve_trx_preserve_result take_result() { return std::move(m_result); }

 private:
  void clear_target_after_error(THD *candidate) const {
    if (candidate == nullptr) return;
    mysql_mutex_lock(&candidate->LOCK_thd_data);
    if (candidate->preserve_trx_batch_generation == m_generation &&
        candidate->preserve_trx_batch_state !=
            Preserve_trx_batch_thd_state::PRESERVED_DRAINED) {
      candidate->preserve_trx_batch_generation = 0;
      candidate->preserve_trx_batch_state = Preserve_trx_batch_thd_state::NONE;
      candidate->preserve_trx_temp_table_batch_capture_epoch.store(
          false, std::memory_order_release);
      preserve_trx_temp_table_clear_batch_unsupported_boundary(candidate);
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

  THD *m_owner;
  const Preserve_trx_options &m_options;
  ulonglong m_timeout_seconds;
  ulonglong m_generation;
  my_thread_id m_target_thread_id;
  PreserveBinlogBlobProvider *m_binlog_blob_provider;
  const Preserve_trx_lock_warmcopy_artifact *m_lock_warmcopy_artifact{nullptr};
  bool m_debug_fail_ha_prepare_low{false};
  bool m_debug_fail_temp_only_prepare{false};
  bool m_defer_snapshot_directory_fsync{false};
  Preserve_trx_transfer_source_epoch_session *m_transfer_source_epoch_session{
      nullptr};
  std::string m_transfer_preserve_dir;
  std::string m_preselected_token;
  const char *m_worker_thread_stack{nullptr};
  ulonglong m_attach_deadline_us{0};
  std::shared_ptr<Preserve_trx_drain_attempt> m_drain_attempt;
  Preserve_batch_attached_target_preparer m_attached_target_preparer;
  Preserve_trx_deferred_transfer_candidate *m_deferred_transfer_candidate{
      nullptr};
  bool m_visited_target{false};
  bool m_error{false};
  Preserve_trx_preserve_result m_result;
};

uint preserve_trx_effective_parallel_preserve_threads(size_t target_count,
                                                      bool lock_warmcopy_batch) {
  if (!lock_warmcopy_batch || target_count <= 1) return 1;
  if (preserve_trx_parallel_preserve_threads == 1) return 1;
  uint requested = preserve_trx_parallel_preserve_threads;
  if (requested == 0) {
    requested = preserve_trx_auto_parallel_preserve_threads(
        std::thread::hardware_concurrency());
  }
  requested = std::max<uint>(1, requested);
  return std::min<uint>(requested, static_cast<uint>(target_count));
}

bool warmcopy_close_deadline_expired(ulonglong close_deadline_us) {
  return preserve_trx_monotonic_deadline_expired_at(
      close_deadline_us, preserve_trx_monotonic_us());
}

unsigned long warmcopy_close_timeout_ms_until_deadline(
    ulonglong close_deadline_us, unsigned long fallback_timeout_ms) {
  return preserve_trx_monotonic_timeout_ms_until_deadline_at(
      close_deadline_us, fallback_timeout_ms, preserve_trx_monotonic_us());
}

class Warmcopy_batch_drain_participant final
    : public Preserve_trx_drain_participant {
 public:
  Warmcopy_batch_drain_participant(THD *owner, ulonglong generation,
                                   unsigned long close_timeout_ms,
                                   bool allow_quiesced_rebuild_fallback,
                                   Preserve_trx_transfer_runtime_policy
                                       runtime_policy,
                                   uint64_t max_inflight_bytes)
      : m_owner(owner),
        m_generation(generation),
        m_close_timeout_ms(close_timeout_ms),
        m_allow_quiesced_rebuild_fallback(
            allow_quiesced_rebuild_fallback),
        m_runtime_policy(std::move(runtime_policy)),
        m_max_inflight_bytes(max_inflight_bytes) {}

  bool open_phase1() override {
    preserve_trx_warmcopy_reset_status();
    m_observation = {};
    m_observation.state = Preserve_trx_drain_participant_state::OPEN;
    m_observation.bytes_budget = preserve_trx_warmcopy_max_total_bytes;
    m_observation.phase1_progress = 1;
    m_provider = std::make_shared<Warmcopy_batch_blob_provider>(
        preserve_trx_default_dir(), m_allow_quiesced_rebuild_fallback,
        m_runtime_policy, m_max_inflight_bytes);
    if (m_provider == nullptr || m_provider->error()) {
      mark_degraded("warm-copy provider open failed");
      return false;
    }
    m_provider->set_close_deadline_us(m_closing_deadline_us);

    DEBUG_SYNC(m_owner, "preserve_trx_warmcopy_open");
    if (m_runtime_policy.warmcopy_min_open_ms != 0) {
      my_sleep(static_cast<ulong>(m_runtime_policy.warmcopy_min_open_ms) *
               1000);
    }

    Warmcopy_prepare_idle_participants prepare_warmcopy(m_owner);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&prepare_warmcopy);
    for (const Preserve_trx_pinned_thd &target : prepare_warmcopy.targets()) {
      if (target.thd == nullptr ||
          !preserve_trx_has_active_multi_stmt_transaction(target.thd) ||
          preserve_trx_is_unsupported_common_context(target.thd)) {
        continue;
      }
      (void)m_provider->prepare_blob_for_thd_if_present(target.thd,
                                                        m_generation);
    }
    if (!prepare_active_targets()) {
      mark_degraded("warm-copy active target preparation failed");
      return false;
    }
    DEBUG_SYNC(m_owner, "preserve_trx_warmcopy_after_descriptor_hwm");
    refresh_observation_from_provider(50);
    if (m_provider->error()) {
      mark_degraded("warm-copy phase1 preparation failed");
      return false;
    }
    return true;
  }

  bool close_phase1() override {
    if (m_provider == nullptr || m_provider->error()) {
      mark_degraded("warm-copy provider unavailable before close");
      return false;
    }

    if (warmcopy_close_deadline_expired(m_closing_deadline_us)) {
      mark_degraded("warm-copy close deadline expired before participant close");
      return false;
    }

    DEBUG_SYNC(m_owner, "preserve_trx_warmcopy_after_admission_close_before_batch");
    if (m_provider->error()) {
      mark_degraded("warm-copy provider failed before batch close");
      return false;
    }

    if (m_closing_deadline_us == 0 && m_close_timeout_ms != 0) {
      m_closing_deadline_us = preserve_trx_monotonic_deadline_after_ms(
          preserve_trx_monotonic_us(), m_close_timeout_ms);
      m_provider->set_close_deadline_us(m_closing_deadline_us);
    }
    m_closed = true;
    m_observation.state = Preserve_trx_drain_participant_state::READY;
    refresh_observation_from_provider(100);

    DEBUG_SYNC(m_owner, "preserve_trx_warmcopy_before_closing");
    DEBUG_SYNC(m_owner, "preserve_trx_warmcopy_after_close");
    return true;
  }

  bool phase1_ready() const override {
    return m_closed && m_provider != nullptr && !m_provider->error();
  }

  bool phase2_preflight(Preserve_trx_drain_phase_mode mode) override {
    if (mode != Preserve_trx_drain_phase_mode::TWO_PHASE) {
      mark_degraded("warm-copy requires two-phase drain");
      return false;
    }
    if (!phase1_ready()) {
      mark_degraded("warm-copy phase1 not ready");
      return false;
    }
    refresh_observation_from_provider(100);
    return true;
  }

  void abort_phase() override {
    m_provider.reset();
    m_closed = false;
    m_closing_deadline_us = 0;
    m_observation.state = Preserve_trx_drain_participant_state::ABANDONED;
    m_observation.owns_artifact = false;
    if (m_observation.failure_reason.empty())
      m_observation.failure_reason = "aborted";
  }

  void finalize_phase() override {
    m_observation.state = Preserve_trx_drain_participant_state::FINALIZED;
    m_observation.owns_artifact = false;
  }

  Preserve_trx_drain_participant_observation observation() const override {
    return m_observation;
  }

  Warmcopy_batch_blob_provider *provider() const {
    return m_provider.get();
  }

  bool release_finalized_artifact(
      my_thread_id thread_id, const PrebuiltBinlogCacheBlob &blob) {
    return m_provider != nullptr &&
           m_provider->release_finalized_artifact(thread_id, blob);
  }

  void stop_mirroring_for_reset() {
    if (m_provider != nullptr) m_provider->stop_mirroring_for_reset();
  }

  ulonglong closing_deadline_us() const { return m_closing_deadline_us; }

  void set_closing_deadline_us(ulonglong close_deadline_us) {
    m_closing_deadline_us = close_deadline_us;
    if (m_provider != nullptr)
      m_provider->set_close_deadline_us(close_deadline_us);
  }

  bool prepare_quiesced_target(THD *target,
                               ulonglong close_deadline_us) {
    if (!prepare_quiesced_target_impl(target, close_deadline_us)) {
      mark_degraded("warm-copy quiesced target preparation failed");
      return false;
    }
    refresh_observation_from_provider(m_observation.phase1_progress);
    return true;
  }

  bool prepare_attached_target(THD *target, ulonglong close_deadline_us) {
    const bool prepared =
        prepare_quiesced_target_impl(target, close_deadline_us, true);
    std::lock_guard<std::mutex> guard(m_observation_mutex);
    if (!prepared) {
      mark_degraded("warm-copy attached target preparation failed");
      return false;
    }
    refresh_observation_from_provider(m_observation.phase1_progress);
    return true;
  }

  bool prepare_quiesced_targets(
      const std::vector<my_thread_id> &target_thread_ids,
      ulonglong close_deadline_us) {
    if (m_provider == nullptr) {
      mark_degraded("warm-copy provider unavailable for quiesced targets");
      return false;
    }
    if (warmcopy_close_deadline_expired(close_deadline_us)) {
      mark_degraded("warm-copy close deadline expired before target preparation");
      return false;
    }
    Preserve_batch_quiesced_target_pin_collector targets(
        m_owner, m_generation, target_thread_ids);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&targets);
    if (targets.error() || targets.targets().size() != target_thread_ids.size()) {
      mark_degraded("warm-copy quiesced target pin failed");
      return false;
    }
    bool prepare_failed = false;
    for (const Preserve_trx_pinned_thd &target : targets.targets()) {
      if (!prepare_quiesced_target_impl(target.thd, close_deadline_us)) {
        prepare_failed = true;
        break;
      }
    }
    refresh_observation_from_provider(m_observation.phase1_progress);
    if (prepare_failed || m_provider->error()) {
      mark_degraded("warm-copy quiesced target preparation failed");
      return false;
    }
    return true;
  }

  bool tail_budget_within_limits(
      const std::vector<my_thread_id> &target_thread_ids,
      ulonglong close_deadline_us) {
    if (m_provider == nullptr) {
      mark_degraded("warm-copy provider unavailable for tail budget check");
      return false;
    }
    if (warmcopy_close_deadline_expired(close_deadline_us)) {
      mark_degraded("warm-copy close deadline expired before tail budget check");
      return false;
    }
    Warmcopy_tail_budget_validator tail_validator(m_generation, target_thread_ids,
                                                  m_provider.get(),
                                                  close_deadline_us);
    Preserve_batch_quiesced_target_pin_collector targets(
        m_owner, m_generation, target_thread_ids);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&targets);
    if (targets.error() || targets.targets().size() != target_thread_ids.size()) {
      mark_degraded("warm-copy tail target pin failed");
      return false;
    }
    for (const Preserve_trx_pinned_thd &target : targets.targets()) {
      tail_validator.validate(target.thd);
      if (tail_validator.error() || tail_validator.exceeded()) break;
    }
    refresh_observation_from_provider(m_observation.phase1_progress);
    if (tail_validator.error()) {
      mark_degraded("warm-copy tail budget check failed");
      return false;
    }
    if (tail_validator.exceeded()) {
      mark_degraded("warm-copy tail budget exceeded");
      return false;
    }
    return true;
  }

 private:
  bool prepare_active_targets() {
    if (!m_allow_quiesced_rebuild_fallback) return true;

    Preserve_batch_phase1_transfer_target_scanner scanner(m_owner);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&scanner);
    const std::set<my_thread_id> target_ids(scanner.target_thread_ids().begin(),
                                            scanner.target_thread_ids().end());
    Preserve_batch_phase1_declared_target_pin_collector targets(m_owner,
                                                                 target_ids);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&targets);
    if (targets.error()) return false;

    for (const auto &target : targets.targets()) {
      const bool active_multi_stmt_transaction =
          target.thd != nullptr &&
          preserve_trx_has_active_multi_stmt_transaction(target.thd);
      const bool unsupported =
          target.thd != nullptr &&
          preserve_trx_is_unsupported_common_context(target.thd, true);
      if (target.thd == nullptr || target.idle ||
          !active_multi_stmt_transaction ||
          unsupported) {
        continue;
      }
      if (!m_provider->prepare_active_blob_for_thd(target.thd, m_generation) &&
          m_provider->error()) {
        return false;
      }
    }
    return !m_provider->error();
  }

  bool prepare_quiesced_target_impl(THD *target, ulonglong close_deadline_us,
                                    bool target_attached = false) {
    if (target == nullptr || m_provider == nullptr ||
        warmcopy_close_deadline_expired(close_deadline_us)) {
      return false;
    }
    const std::vector<my_thread_id> single_target{target->thread_id()};
    Warmcopy_prepare_quiesced_targets prepare_target(
        m_generation, single_target, m_provider.get(), m_generation,
        close_deadline_us, m_allow_quiesced_rebuild_fallback, target_attached);
    prepare_target.prepare(target);
    return !prepare_target.error() && !m_provider->error();
  }

  void refresh_observation_from_provider(uint32_t progress) {
    m_observation.phase1_progress = progress;
    m_observation.bytes_budget = preserve_trx_warmcopy_max_total_bytes;
    if (m_provider == nullptr) {
      m_observation.owns_artifact = false;
      m_observation.bytes_used = 0;
      return;
    }
    m_observation.bytes_used = m_provider->total_bytes();
    m_observation.owns_artifact = m_provider->prepared_count() != 0;
  }

  void mark_degraded(const char *reason) {
    m_observation.state = Preserve_trx_drain_participant_state::DEGRADED;
    if (reason != nullptr) m_observation.failure_reason = reason;
    refresh_observation_from_provider(m_observation.phase1_progress);
  }

  THD *m_owner;
  ulonglong m_generation;
  unsigned long m_close_timeout_ms;
  bool m_allow_quiesced_rebuild_fallback;
  Preserve_trx_transfer_runtime_policy m_runtime_policy;
  uint64_t m_max_inflight_bytes;
  std::shared_ptr<Warmcopy_batch_blob_provider> m_provider;
  ulonglong m_closing_deadline_us{0};
  bool m_closed{false};
  std::mutex m_observation_mutex;
  Preserve_trx_drain_participant_observation m_observation;
};

struct Preserve_batch_target_execution {
  enum class Failure_reason : uint8_t {
    NONE,
    LOCK_PLAN_REPLACEMENT_FAILED,
    FINAL_METADATA_BUILD_FAILED
  };

  my_thread_id target_thread_id{0};
  Failure_reason failure_reason{Failure_reason::NONE};
  bool pin_error{false};
  bool visited_target{false};
  bool error{false};
  bool no_token_target{false};
  bool early_objects_staged{false};
  bool batch_item_collected{false};
  bool lock_artifact_prepared{false};
  bool initial_lock_fence_valid{false};
  lock_warmcopy_trx_lock_fence_t initial_lock_fence;
  uint64_t final_record_lock_count{0};
  uint64_t final_table_lock_count{0};
  uint64_t final_mdl_descriptor_count{0};
  uint64_t phase2_record_materialized_bytes{0};
  uint64_t latest_record_lock_publication_generation{0};
  bool final_record_prebuilt{false};
  bool final_record_materialized{false};
  Preserve_trx_lock_warmcopy_artifact lock_artifact;
  bool has_pending_final_binlog_descriptor{false};
  bool has_pending_final_binlog_publication{false};
  Preserve_trx_pending_binlog_publication pending_final_binlog_publication;
  Preserve_trx_transfer_object_descriptor pending_final_binlog_descriptor;
  Preserve_trx_deferred_transfer_candidate deferred_candidate;
  Preserve_trx_preserve_result result;
};

static Preserve_trx_transfer_object_descriptor
preserve_trx_pending_binlog_descriptor(
    const Preserve_trx_pending_binlog_publication &pending) {
  Preserve_trx_transfer_object_descriptor descriptor;
  descriptor.object_id = pending.blob.name;
  descriptor.kind = Preserve_trx_transfer_object_kind::EXTERNAL_BLOB;
  descriptor.total_size = pending.blob.size;
  descriptor.digest = pending.blob.digest;
  return descriptor;
}

const char *preserve_trx_source_failure_reason_name(
    Preserve_batch_target_execution::Failure_reason reason) {
  switch (reason) {
    case Preserve_batch_target_execution::Failure_reason::NONE:
      return "NONE";
    case Preserve_batch_target_execution::Failure_reason::
        LOCK_PLAN_REPLACEMENT_FAILED:
      return "LOCK_PLAN_REPLACEMENT_FAILED";
    case Preserve_batch_target_execution::Failure_reason::
        FINAL_METADATA_BUILD_FAILED:
      return "FINAL_METADATA_BUILD_FAILED";
  }
  return "UNKNOWN";
}

bool preserve_trx_early_lock_fence_matches(
    const lock_warmcopy_trx_lock_fence_t &initial,
    const lock_warmcopy_trx_lock_fence_t &current) {
  return !current.conversion_attempt_after_freeze &&
         !current.conversion_unhandled_after_freeze &&
         initial.trx_locks_version == current.trx_locks_version &&
         initial.n_rec_locks == current.n_rec_locks &&
         (current.n_rec_locks == 0 ||
          initial.coordinate_generation == current.coordinate_generation);
}

Preserved_trx_external_blob *preserve_trx_deferred_record_lock_blob(
    Preserve_trx_deferred_transfer_candidate *candidate) {
  if (candidate == nullptr) return nullptr;
  const auto found = std::find_if(
      candidate->bundle.external_blobs.begin(),
      candidate->bundle.external_blobs.end(),
      [](const Preserved_trx_external_blob &blob) {
        return blob.name == kPreservedTrxBlobRecordLocks;
      });
  return found == candidate->bundle.external_blobs.end() ? nullptr : &*found;
}

bool preserve_trx_export_early_record_lock_blob(
    trx_t *trx, uint64_t expected_lock_count,
    Preserved_trx_external_blob *blob) {
  if (trx == nullptr || blob == nullptr || expected_lock_count == 0)
    return false;

  std::string combined_payload;
  if (trx_preserve_export_record_locks_stable_page_only(
          trx, &combined_payload, preserve_trx_max_lock_count) != DB_SUCCESS) {
    return false;
  }

  Preserved_trx_external_blob stable_blob;
  stable_blob.name = kPreservedTrxBlobRecordLocks;
  std::string predicate_payload;
  uint32_t exported_lock_count = 0;
  if (!split_record_and_predicate_locks_payload(
          combined_payload, &stable_blob.payload, &predicate_payload) ||
      stable_blob.payload.empty() || !predicate_payload.empty() ||
      !trx_preserve_record_locks_payload_lock_count(stable_blob.payload,
                                                    &exported_lock_count) ||
      exported_lock_count != expected_lock_count) {
    return false;
  }

  *blob = std::move(stable_blob);
  return true;
}

bool preserve_trx_bind_early_record_lock_blob(
    Preserve_trx_lock_warmcopy_drain_participant *participant,
    Preserve_batch_target_execution *execution) {
  if (participant == nullptr || execution == nullptr ||
      !execution->initial_lock_fence_valid) {
    return false;
  }
  Preserved_trx_external_blob *blob = preserve_trx_deferred_record_lock_blob(
      &execution->deferred_candidate);
  uint32_t table_lock_count = 0;
  uint32_t mdl_descriptor_count = 0;
  const Preserve_snapshot_metadata &metadata =
      execution->deferred_candidate.bundle.metadata;
  if (!trx_preserve_table_locks_payload_lock_count(
          metadata.table_locks_payload, &table_lock_count) ||
      !mdl_descriptors_payload_is_valid(metadata.mdl_descriptors_payload,
                                        &mdl_descriptor_count)) {
    return false;
  }
  execution->final_record_lock_count =
      execution->initial_lock_fence.n_rec_locks;
  execution->final_table_lock_count = table_lock_count;
  execution->final_mdl_descriptor_count = mdl_descriptor_count;
  if (execution->initial_lock_fence.n_rec_locks == 0) return blob == nullptr;
  if (blob == nullptr) return false;
  if (blob->prebuilt) {
    const Preserve_trx_lock_warmcopy_artifact &artifact =
        execution->lock_artifact;
    const bool valid =
        execution->lock_artifact_prepared && artifact.valid &&
        preserve_trx_lock_warmcopy_verify_record_final_fence(
            artifact, execution->initial_lock_fence) ==
            Preserve_trx_lock_warmcopy_reason::OK &&
        artifact.has_prebuilt_record_locks_blob &&
        blob->name == kPreservedTrxBlobRecordLocks &&
        blob->descriptor.size == artifact.prebuilt_record_locks_blob.size &&
        blob->descriptor.digest ==
            artifact.prebuilt_record_locks_blob.digest &&
        blob->lock_plan_contract_version ==
            artifact.prebuilt_record_locks_blob.lock_plan_contract_version &&
        blob->source_live_lock_generation ==
            artifact.prebuilt_record_locks_blob.source_live_lock_generation &&
        blob->source_live_lock_digest ==
            artifact.prebuilt_record_locks_blob.source_live_lock_digest &&
        blob->record_store_fingerprint ==
            artifact.prebuilt_record_locks_blob.record_store_fingerprint;
    if (!valid) return false;
    if (artifact.prebuilt_record_locks_blob
            .strict_metadata_only_compatible) {
      execution->final_record_prebuilt = true;
      execution->latest_record_lock_publication_generation =
          blob->source_live_lock_generation;
      return true;
    }
  }
  uint64_t minimum_publication_generation =
      execution->latest_record_lock_publication_generation;
  if (execution->lock_artifact.has_prebuilt_record_locks_blob) {
    minimum_publication_generation = std::max(
        minimum_publication_generation,
        execution->lock_artifact.prebuilt_record_locks_blob
            .source_live_lock_generation);
  }
  if (!preserve_trx_export_early_record_lock_blob(
          execution->result.preserved_trx,
          execution->initial_lock_fence.n_rec_locks, blob)) {
    return false;
  }
  const bool bound = participant->bind_early_live_record_blob(
      execution->target_thread_id, execution->initial_lock_fence,
      minimum_publication_generation, blob);
  if (bound) {
    execution->latest_record_lock_publication_generation =
        blob->source_live_lock_generation;
  }
  execution->final_record_materialized = bound;
  execution->phase2_record_materialized_bytes =
      bound ? blob->payload.size() : 0;
  return bound;
}

bool preserve_trx_replace_early_record_lock_blob(
    Preserve_trx_lock_warmcopy_drain_participant *participant,
    Preserve_trx_transfer_source_epoch_session *session,
    Preserve_batch_target_execution *execution,
    const lock_warmcopy_trx_lock_fence_t &current_fence) {
  if (participant == nullptr || session == nullptr || execution == nullptr ||
      execution->result.preserved_trx == nullptr ||
      current_fence.n_rec_locks == 0) {
    return false;
  }

  Preserved_trx_external_blob replacement;
  if (!preserve_trx_export_early_record_lock_blob(
          execution->result.preserved_trx, current_fence.n_rec_locks,
          &replacement)) {
    return false;
  }
  if (!participant->bind_early_live_record_blob(
          execution->target_thread_id, current_fence,
          execution->latest_record_lock_publication_generation,
          &replacement)) {
    return false;
  }
  const uint64_t replacement_generation =
      replacement.source_live_lock_generation;
  if (preserve_trx_transfer_replace_deferred_candidate_record_locks(
          &execution->deferred_candidate, std::move(replacement)) !=
      Preserve_trx_transfer_status::OK) {
    return false;
  }
  execution->latest_record_lock_publication_generation =
      replacement_generation;
  DBUG_EXECUTE_IF("preserve_trx_early_fail_replacement_stage", {
    return false;
  });
  if (preserve_trx_transfer_stage_deferred_candidate_external_objects(
          session, preserve_trx_default_dir(),
          &execution->deferred_candidate) !=
      Preserve_trx_transfer_status::OK) {
    return false;
  }
  execution->initial_lock_fence = current_fence;
  execution->initial_lock_fence_valid = true;
  return true;
}

class Temp_table_phase1_drain_participant final
    : public Preserve_trx_drain_participant {
 public:
  Temp_table_phase1_drain_participant(THD *owner, ulonglong generation)
      : m_owner(owner), m_generation(generation) {}

  bool open_phase1() override {
    m_observation = {};
    m_observation.state = Preserve_trx_drain_participant_state::OPEN;
    m_observation.phase1_progress = 1;

    /*
      Temporary-table DML support needs the capture epoch to exist before the
      user-visible quiesce. Both idle transactions and transactions currently
      inside a statement are scanned here; later command gates and preflight
      still fail closed if a target has unsupported DDL/savepoint/rollback
      history or incomplete no-redo undo capture.
    */
    Warmcopy_prepare_idle_participants idle_targets(m_owner);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&idle_targets);
    if (!begin_capture_for_targets(idle_targets.targets(), true)) return false;

    Warmcopy_prepare_active_participants active_targets(m_owner);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&active_targets);
    if (!begin_capture_for_targets(active_targets.targets(), false)) return false;

    m_closed = true;
    m_observation.state = Preserve_trx_drain_participant_state::READY;
    m_observation.phase1_progress = 100;
    return true;
  }

  bool close_phase1() override {
    m_closed = true;
    m_observation.state = Preserve_trx_drain_participant_state::READY;
    m_observation.phase1_progress = 100;
    return true;
  }

  bool phase1_ready() const override { return m_closed; }

  bool phase2_preflight(Preserve_trx_drain_phase_mode mode) override {
    if (mode != Preserve_trx_drain_phase_mode::TWO_PHASE) {
      mark_degraded("temp-table phase1 capture requires two-phase drain");
      return false;
    }
    if (!phase1_ready()) {
      mark_degraded("temp-table phase1 capture not ready");
      return false;
    }
    return true;
  }

  bool prepare_late_phase1_idle_targets() {
    /*
      Active statements selected during the first phase-1 sweep may reach an
      idle transaction boundary before WARMCOPY_CLOSING blocks new work. Re-sweep
      idle targets here so their temp-table physical sidecars can be streamed
      while the drain is still non-blocking. Targets that are still active remain
      on the conservative phase-2 fallback path.
    */
    Warmcopy_prepare_idle_participants idle_targets(m_owner);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&idle_targets);
    return begin_capture_for_targets(idle_targets.targets(), true);
  }

  void abort_phase() override {
    clear_capture_epochs();
    m_observation.state = Preserve_trx_drain_participant_state::ABANDONED;
    if (m_observation.failure_reason.empty())
      m_observation.failure_reason = "aborted";
  }

  void finalize_phase() override {
    clear_capture_epochs();
    m_observation.state = Preserve_trx_drain_participant_state::FINALIZED;
  }

  Preserve_trx_drain_participant_observation observation() const override {
    return m_observation;
  }

 private:
  bool capture_target_recorded(my_thread_id thread_id) const {
    for (my_thread_id recorded : m_capture_target_thread_ids) {
      if (recorded == thread_id) return true;
    }
    return false;
  }

  void mark_capture_epoch_target(THD *target) {
    if (target == nullptr) return;
    mysql_mutex_lock(&target->LOCK_thd_data);
    target->preserve_trx_temp_table_batch_capture_epoch.store(
        true, std::memory_order_release);
    const my_thread_id thread_id = target->thread_id();
    if (!capture_target_recorded(thread_id))
      m_capture_target_thread_ids.push_back(thread_id);
    mysql_mutex_unlock(&target->LOCK_thd_data);
  }

  class Clear_capture_epoch_targets final : public Do_THD_Impl {
   public:
    explicit Clear_capture_epoch_targets(
        const std::vector<my_thread_id> &target_thread_ids)
        : m_target_thread_ids(target_thread_ids) {}

    void operator()(THD *candidate) override {
      if (candidate == nullptr) return;
      bool target = false;
      for (my_thread_id thread_id : m_target_thread_ids) {
        if (candidate->thread_id() == thread_id) {
          target = true;
          break;
        }
      }
      if (!target) return;

      mysql_mutex_lock(&candidate->LOCK_thd_data);
      candidate->preserve_trx_temp_table_batch_capture_epoch.store(
          false, std::memory_order_release);
      preserve_trx_temp_table_clear_batch_unsupported_boundary(candidate);
      mysql_mutex_unlock(&candidate->LOCK_thd_data);
      preserve_trx_temp_table_discard_phase1_sidecars(
          candidate, preserve_trx_default_dir());
    }

   private:
    const std::vector<my_thread_id> &m_target_thread_ids;
  };

  void clear_capture_epochs() {
    if (m_capture_target_thread_ids.empty()) return;
    Clear_capture_epoch_targets clear(m_capture_target_thread_ids);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
    m_capture_target_thread_ids.clear();
  }

  bool begin_capture_for_targets(std::vector<Preserve_trx_pinned_thd> &targets,
                                 bool prebuild_sidecars) {
    for (const Preserve_trx_pinned_thd &target : targets) {
      if (target.thd == nullptr) continue;
      mark_capture_epoch_target(target.thd);
      if (!preserve_trx_temp_table_begin_capture_epoch(target.thd)) {
        mark_degraded("temp-table phase1 capture epoch open failed");
        return false;
      }
      if (prebuild_sidecars) {
        const std::string warmcopy_id =
            "tempwarm_" + std::to_string(m_generation) + "_" +
            std::to_string(
                static_cast<unsigned long long>(target.thd->thread_id()));
        if (!preserve_trx_temp_table_prebuild_phase1_sidecars(
                target.thd, trx_preserve_current_thd_trx(target.thd),
                preserve_trx_default_dir(), warmcopy_id)) {
          const std::string prebuild_reason =
              preserve_trx_temp_table_degraded_reason(target.thd);
          mark_degraded(prebuild_reason.empty()
                            ? "temp-table phase1 sidecar prebuild failed"
                            : prebuild_reason.c_str());
          return false;
        }
      }
    }
    return true;
  }

  void mark_degraded(const char *reason) {
    m_observation.state = Preserve_trx_drain_participant_state::DEGRADED;
    if (reason != nullptr) m_observation.failure_reason = reason;
  }

  THD *m_owner;
  ulonglong m_generation;
  bool m_closed{false};
  std::vector<my_thread_id> m_capture_target_thread_ids;
  Preserve_trx_drain_participant_observation m_observation;
};

}  // namespace

bool preserved_trx_resurrection_entry_to_engine_facts(
    const Preserve_trx_resurrection_index_entry &entry,
    trx_preserve_resurrection_facts *facts) {
  return preserve_trx_resurrection_entry_to_engine_facts(entry, facts);
}

Preserve_trx_reset_drain_result preserve_trx_request_active_drain_reset(
    bool wait_for_runnable) {
  return preserve_trx_request_active_drain_reset_impl(wait_for_runnable);
}

bool preserved_trx_register_physical_resurrection_candidates(
    const std::vector<Preserve_trx_resurrection_index_entry> &entries) {
  trx_preserve_startup_resurrection_reset();
  if (entries.empty()) return true;
  for (const auto &entry : entries) {
    if (preserve_trx_register_resurrection_candidate(
            entry, entry.authority_token)) {
      trx_preserve_startup_resurrection_reset();
      return true;
    }
  }
  return false;
}

bool preserved_trx_hydrate_source_rollback_image_for_unit_test(
    const std::string &token, Preserve_snapshot_metadata *metadata,
    const Preserve_trx_source_rollback_image &source_rollback_image) {
  return hydrate_source_rollback_image_for_unit_test_impl(
      token, metadata, source_rollback_image);
}

bool preserved_trx_binlog_payload_memory_peak_for_unit_test(
    uint64_t payload_bytes, uint64_t *peak_bytes) {
  return binlog_payload_memory_peak(payload_bytes, peak_bytes);
}

bool preserved_trx_build_native_binlog_cache_facts(
    const Preserve_snapshot_metadata &metadata,
    const Mysql_binlog_preserve_token_identity &identity,
    const Preserved_trx_external_blob_descriptor &descriptor,
    uint64_t binlog_incarnation, uint64_t key_generation,
    Mysql_binlog_preserve_cache_facts *facts) {
  if (facts == nullptr ||
      metadata.binlog_state !=
          Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE ||
      !metadata.global_log_bin || !metadata.option_bin_log ||
      !metadata.session_sql_log_bin ||
      descriptor.name != kPreservedTrxBlobBinlogCache ||
      descriptor.size == 0 || descriptor.size != metadata.binlog_cache_size ||
      binlog_incarnation == 0 || key_generation == 0) {
    return false;
  }

  uint32_t mdl_descriptor_count = 0;
  if (!mdl_descriptors_payload_is_valid(metadata.mdl_descriptors_payload,
                                        &mdl_descriptor_count)) {
    return false;
  }
  std::vector<Preserve_sql_savepoint_entry> savepoints;
  if (parse_sql_savepoint_entries(metadata.sql_savepoints_payload,
                                  mdl_descriptor_count, &savepoints) ||
      savepoints.size() != metadata.savepoint_count) {
    return false;
  }

  Mysql_binlog_preserve_cache_facts built;
  built.identity = identity;
  built.snapshot = metadata_to_binlog_cache_snapshot(metadata);
  built.snapshot.cache_payload.clear();
  built.snapshot.has_cache_length = true;
  built.snapshot.cache_length = descriptor.size;
  for (const Preserve_sql_savepoint_entry &savepoint : savepoints) {
    if (savepoint.has_binlog_handler()) {
      built.cache_states.push_back(savepoint.binlog_cache_state);
    }
  }
  built.payload_sha256 = descriptor.digest;
  built.cache_length = descriptor.size;
  built.binlog_incarnation = binlog_incarnation;
  built.key_generation = key_generation;
  built.option_bin_log = metadata.option_bin_log;
  built.session_sql_log_bin = metadata.session_sql_log_bin;
  if (!mysql_binlog_preserve_finalize_cache_facts(&built)) return false;
  *facts = std::move(built);
  return true;
}

bool preserve_trx_magic_xid_should_be_protected(const XID &xid) {
  if (!xid_is_preserve_magic(xid)) return false;
  if (!preserve_trx_is_enabled()) return false;

  const char *token =
      xid.get_data() + static_cast<size_t>(PRESERVE_TRX_XID_GTRID_LENGTH);
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  return preserved_trx_record_exists_locked(std::string(
      token, static_cast<size_t>(xid.get_bqual_length())));
}

bool preserve_trx_temp_table_session_needs_eligibility_check(const THD *thd) {
  return preserve_trx_temp_table_enable && thd != nullptr &&
         thd->temporary_tables != nullptr;
}

bool preserve_trx_temp_table_session_supported(THD *thd) {
  if (thd == nullptr) return false;
  if (thd->temporary_tables == nullptr) return true;
  if (!preserve_trx_temp_table_enable) return false;

  return true;
}

bool preserve_trx_temp_table_capture_enabled(THD *thd, const TABLE *table) {
  if (!preserve_trx_temp_table_enable) return false;
  if (thd == nullptr || table == nullptr) return false;
  if (table->s == nullptr || table->s->tmp_table != TRANSACTIONAL_TMP_TABLE)
    return false;

  /*
    This is the no-allocation predicate for hook sites that require an
    already admitted participant. First-touch row/create/truncate admission is
    handled by the temp-table participant helpers after their feature gate.
  */
  return preserve_trx_temp_table_get_participant(thd) != nullptr;
}

bool preserve_trx_temp_table_resume_supported(
    bool snapshot_has_temp_table_manifest) {
  Preserve_snapshot_metadata metadata;
  if (snapshot_has_temp_table_manifest)
    metadata.temp_table_manifest_payload = "present";
  return preserve_trx_temp_table_resume_policy(metadata).supported;
}

void preserve_trx_warmcopy_note_prefix_bytes(uint64_t bytes) {
  g_warmcopy_prefix_bytes.fetch_add(bytes);
}

void preserve_trx_warmcopy_note_digest_bytes(uint64_t bytes) {
  g_warmcopy_digest_bytes.fetch_add(bytes);
}

void preserve_trx_warmcopy_note_durable_bytes(uint64_t bytes) {
  g_warmcopy_durable_bytes.fetch_add(bytes);
}

ulonglong preserve_trx_warmcopy_prefix_bytes_status() {
  return g_warmcopy_prefix_bytes.load();
}

ulonglong preserve_trx_warmcopy_digest_bytes_status() {
  return g_warmcopy_digest_bytes.load();
}

ulonglong preserve_trx_warmcopy_durable_bytes_status() {
  return g_warmcopy_durable_bytes.load();
}

ulonglong preserve_trx_warmcopy_provider_full_copy_to_count_status() {
  return g_warmcopy_provider_full_copy_to_count.load();
}

ulonglong preserve_trx_warmcopy_phase2_pause_us_status() {
  return g_warmcopy_phase2_pause_us.load();
}

ulonglong preserve_trx_phase2_total_us_status() {
  return g_phase2_total_us.load();
}

ulonglong preserve_trx_phase2_target_wait_us_status() {
  return g_phase2_target_wait_us.load();
}

ulonglong preserve_trx_phase1_readiness_samples_status() {
  return g_phase1_readiness_samples.load();
}

ulonglong preserve_trx_phase1_readiness_inflight_commands_status() {
  return g_phase1_readiness_inflight_commands.load();
}

ulonglong preserve_trx_phase1_readiness_oldest_command_age_us_status() {
  return g_phase1_readiness_oldest_command_age_us.load();
}

ulonglong preserve_trx_phase1_readiness_offender_count_status() {
  return g_phase1_readiness_offender_count.load();
}

ulonglong preserve_trx_phase1_readiness_wait_us_status() {
  return g_phase1_readiness_wait_us.load();
}

ulonglong preserve_trx_closing_started_monotonic_us_status() {
  return g_closing_started_monotonic_us.load();
}

ulonglong preserve_trx_closing_command_effective_budget_us_status() {
  return g_closing_command_effective_budget_us.load();
}

ulonglong preserve_trx_closing_command_wait_us_status() {
  return g_closing_command_wait_us.load();
}

ulonglong preserve_trx_closing_command_timed_out_count_status() {
  return g_closing_command_timed_out_count.load();
}

ulonglong preserve_trx_closing_command_deadline_clamped_status() {
  return g_closing_command_deadline_clamped.load();
}

ulonglong preserve_trx_closing_inflight_commands_status() {
  return g_closing_inflight_commands.load();
}

ulonglong preserve_trx_closing_completed_before_deadline_status() {
  return g_closing_completed_before_deadline.load();
}

ulonglong preserve_trx_closing_excluded_tokens_status() {
  return g_closing_excluded_tokens.load();
}

ulonglong preserve_trx_closing_last_excluded_token_status() {
  return g_closing_last_excluded_token.load();
}

ulonglong preserve_trx_phase2_transfer_tail_us_status() {
  return g_phase2_transfer_tail_us.load();
}

ulonglong preserve_trx_closing_to_final_ack_us_status() {
  return g_closing_to_final_ack_us.load();
}

ulonglong preserve_trx_phase2_participant_prepare_us_status() {
  return g_phase2_participant_prepare_us.load();
}

ulonglong preserve_trx_phase2_participant_close_us_status() {
  return g_phase2_participant_close_us.load();
}

ulonglong preserve_trx_phase2_participant_preflight_us_status() {
  return g_phase2_participant_preflight_us.load();
}

ulonglong preserve_trx_phase2_lock_seal_us_status() {
  return g_phase2_lock_seal_us.load();
}

ulonglong preserve_trx_phase2_target_preserve_us_status() {
  return g_phase2_target_preserve_us.load();
}

ulonglong preserve_trx_phase2_lock_preflight_us_status() {
  return g_phase2_lock_preflight_us.load();
}

ulonglong preserve_trx_phase2_prepare_us_status() {
  return g_phase2_prepare_us.load();
}

ulonglong preserve_trx_phase2_detach_claim_us_status() {
  return g_phase2_detach_claim_us.load();
}

ulonglong preserve_trx_phase2_snapshot_write_us_status() {
  return g_phase2_snapshot_write_us.load();
}

ulonglong preserve_trx_phase2_register_us_status() {
  return g_phase2_register_us.load();
}

ulonglong preserve_trx_early_staged_tokens_status() {
  return g_early_staged_tokens.load();
}

ulonglong preserve_trx_command_boundary_to_enqueue_us_max_status() {
  return g_command_boundary_to_enqueue_us_max.load();
}

ulonglong preserve_trx_final_fast_scan_us_status() {
  return g_final_fast_scan_us.load();
}

ulonglong preserve_trx_final_dirty_tokens_status() {
  return g_final_dirty_tokens.load();
}

ulonglong preserve_trx_final_replacement_tokens_status() {
  return g_final_replacement_tokens.load();
}

ulonglong preserve_trx_final_validation_rejects_status() {
  return g_final_validation_rejects.load();
}

ulonglong preserve_trx_phase2_slo_miss_count_status() {
  return g_phase2_slo_miss_count.load();
}

ulonglong preserve_trx_resume_total_us_status() {
  return g_resume_total_us.load();
}

ulonglong preserve_trx_startup_recovery_elapsed_us_status() {
  return g_startup_recovery_elapsed_us.load();
}

ulonglong preserve_trx_startup_recovery_error_status() {
  return g_startup_recovery_error.load();
}

ulonglong preserve_trx_startup_recovery_snapshot_tokens_status() {
  return g_startup_recovery_snapshot_tokens.load();
}

ulonglong preserve_trx_startup_recovery_local_snapshot_tokens_status() {
  return g_startup_recovery_local_snapshot_tokens.load();
}

ulonglong preserve_trx_startup_recovery_binlog_cache_tokens_status() {
  return g_startup_recovery_binlog_cache_tokens.load();
}

ulonglong preserve_trx_startup_recovery_tainted_tokens_status() {
  return g_startup_recovery_tainted_tokens.load();
}

ulonglong preserve_trx_startup_recovery_standby_pending_tokens_status() {
  return g_startup_recovery_standby_pending_tokens.load();
}

ulonglong preserve_trx_startup_recovery_promotion_intent_tokens_status() {
  return g_startup_recovery_promotion_intent_tokens.load();
}

ulonglong preserve_trx_startup_recovery_orphan_rollback_count_status() {
  return g_startup_recovery_orphan_rollback_count.load();
}

ulonglong preserve_trx_startup_resurrection_index_candidates_status() {
  return trx_preserve_startup_resurrection_metrics_snapshot().candidates;
}

ulonglong preserve_trx_startup_resurrection_index_hits_status() {
  return trx_preserve_startup_resurrection_metrics_snapshot().index_hits;
}

ulonglong preserve_trx_startup_resurrection_index_fallbacks_status() {
  return trx_preserve_startup_resurrection_metrics_snapshot().fallbacks;
}

ulonglong preserve_trx_startup_resurrection_undo_anchor_checks_status() {
  return trx_preserve_startup_resurrection_metrics_snapshot()
      .undo_anchor_checks;
}

ulonglong preserve_trx_startup_resurrection_undo_body_pages_status() {
  return trx_preserve_startup_resurrection_metrics_snapshot().undo_body_pages;
}

ulonglong preserve_trx_startup_resurrection_undo_body_records_status() {
  return trx_preserve_startup_resurrection_metrics_snapshot()
      .undo_body_records;
}

ulonglong preserve_trx_startup_recovery_phase_snapshot_load_us_status() {
  return g_startup_recovery_phase_snapshot_load_us.load();
}

ulonglong preserve_trx_startup_recovery_phase_snapshot_validate_us_status() {
  return g_startup_recovery_phase_snapshot_validate_us.load();
}

ulonglong preserve_trx_startup_recovery_phase_snapshot_kernel_us_status() {
  return g_startup_recovery_phase_snapshot_kernel_us.load();
}

ulonglong preserve_trx_startup_recovery_phase_snapshot_claim_us_status() {
  return g_startup_recovery_phase_snapshot_claim_us.load();
}

ulonglong preserve_trx_startup_recovery_phase_snapshot_read_view_us_status() {
  return g_startup_recovery_phase_snapshot_read_view_us.load();
}

ulonglong preserve_trx_startup_recovery_phase_snapshot_table_locks_us_status() {
  return g_startup_recovery_phase_snapshot_table_locks_us.load();
}

ulonglong preserve_trx_startup_recovery_phase_snapshot_record_locks_us_status() {
  return g_startup_recovery_phase_snapshot_record_locks_us.load();
}

ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_entries_status() {
  return g_startup_recovery_phase_snapshot_record_lock_entries.load();
}

ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_stable_page_hits_status() {
  return g_startup_recovery_phase_snapshot_record_lock_stable_page_hits.load();
}

ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_image_resolves_status() {
  return g_startup_recovery_phase_snapshot_record_lock_image_resolves.load();
}

ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_pages_status() {
  return g_startup_recovery_phase_snapshot_record_lock_bitmap_pages.load();
}

ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_bits_status() {
  return g_startup_recovery_phase_snapshot_record_lock_bitmap_bits.load();
}

ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_us_status() {
  return g_startup_recovery_phase_snapshot_record_lock_page_get_us.load();
}

ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_count_status() {
  return g_startup_recovery_phase_snapshot_record_lock_page_get_count.load();
}

ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_table_open_us_status() {
  return g_startup_recovery_phase_snapshot_record_lock_table_open_us.load();
}

ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_pages_status() {
  return g_startup_recovery_phase_snapshot_record_lock_prefetch_pages.load();
}

ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_bytes_status() {
  return g_startup_recovery_phase_snapshot_record_lock_prefetch_bytes.load();
}

ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_residency_pages_status() {
  return g_startup_recovery_phase_snapshot_record_lock_prefetch_residency_pages
      .load();
}

ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_resident_pages_status() {
  return g_startup_recovery_phase_snapshot_record_lock_prefetch_resident_pages
      .load();
}

ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_io_pending_pages_status() {
  return g_startup_recovery_phase_snapshot_record_lock_prefetch_io_pending_pages
      .load();
}

ulonglong
preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_missing_pages_status() {
  return g_startup_recovery_phase_snapshot_record_lock_prefetch_missing_pages
      .load();
}

ulonglong
preserve_trx_startup_recovery_phase_snapshot_predicate_locks_us_status() {
  return g_startup_recovery_phase_snapshot_predicate_locks_us.load();
}

ulonglong preserve_trx_startup_recovery_phase_snapshot_mdl_us_status() {
  return g_startup_recovery_phase_snapshot_mdl_us.load();
}

ulonglong preserve_trx_startup_recovery_phase_snapshot_register_us_status() {
  return g_startup_recovery_phase_snapshot_register_us.load();
}

const Preserved_trx_column_metadata *preserved_trx_columns(size_t *count) {
  *count = sizeof(kPreservedTrxColumns) / sizeof(kPreservedTrxColumns[0]);
  return kPreservedTrxColumns;
}

const char *preserved_trx_dir_value() {
  static const std::string dir = preserve_trx_default_dir();
  return dir.c_str();
}

void preserved_trx_mark_recovery_complete() {
  mark_preserved_trx_recovery_complete();
  if (preserved_trx_skip_local_startup_recovery()) return;
  if (preserve_trx_is_enabled() &&
      !preserved_trx_promotion_start_gate_workers()) {
    LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
           "PRESERVE: failed to prestart promotion gate worker pool");
  }
}

bool preserved_trx_recovery_complete() {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  return g_preserved_trx_recovery_done && !g_preserved_trx_recovery_deferred;
}

bool preserved_trx_mark_innodb_read_only_recovery_state() {
  if (preserved_trx_skip_local_startup_recovery()) {
    trx_preserve_startup_resurrection_reset();
    mark_preserved_trx_recovery_complete();
    return false;
  }
  if (!preserve_trx_is_enabled()) {
    mark_preserved_trx_recovery_complete();
    return false;
  }

  Preserved_trx_carrier_listing listing;
  auto store = create_preserved_trx_default_store(preserve_trx_default_dir());
  const Preserve_snapshot_status status = store->list_tokens(&listing);
  const bool has_artifacts =
      status != Preserve_snapshot_status::OK ||
      !listing.snapshot_tokens.empty() ||
      !listing.external_blob_tokens.empty() ||
      !listing.temp_sidecar_tokens.empty() ||
      !listing.stale_tmp_tokens.empty() || !listing.tainted_tokens.empty() ||
      !listing.consume_state_tokens.empty() ||
      !listing.standby_pending_tokens.empty() ||
      !listing.promotion_adopted_tokens.empty() ||
      !listing.promotion_intent_tokens.empty() ||
      !listing.warm_external_blob_artifacts.empty();
  if (!has_artifacts) {
    mark_preserved_trx_recovery_complete();
    return false;
  }

  g_startup_recovery_error.store(1);
  g_startup_recovery_snapshot_tokens.store(listing.snapshot_tokens.size());
  g_startup_recovery_local_snapshot_tokens.store(
      preserved_trx_local_recoverable_snapshot_tokens(listing).size());
  g_startup_recovery_binlog_cache_tokens.store(
      listing.external_blob_tokens.size());
  LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
         "PRESERVE: startup recovery deferred because innodb_read_only is "
         "enabled and preserved artifacts are present; files were retained "
         "without import, rollback, or cleanup");
  mark_preserved_trx_recovery_deferred();
  return true;
}

bool preserved_trx_local_record_exists(const std::string &token) {
  return preserved_trx_find_record(token, nullptr);
}

bool preserved_trx_validate_snapshot_support(bool allow_create_missing) {
  if (preserved_trx_skip_local_startup_recovery()) return false;
  const std::string dir = preserve_trx_default_dir();
  return preserved_trx_default_carrier_support_has_error(dir,
                                                         allow_create_missing);
}

bool preserved_trx_ensure_snapshot_support() {
  return preserved_trx_validate_snapshot_support(true);
}

Preserve_trx_manager_state preserved_trx_manager_state() {
  return preserve_trx_manager_state_owner_snapshot().state;
}

ulonglong preserve_trx_reset_drain_wins_status() {
  return g_reset_drain_wins.load();
}

ulonglong preserve_trx_reset_drain_too_late_status() {
  return g_reset_drain_too_late.load();
}

ulonglong preserve_trx_closing_control_connection_commands_status() {
  return g_closing_control_connection_commands.load();
}

bool preserved_trx_can_disable_feature() {
  const Preserve_trx_manager_state state =
      preserve_trx_manager_state_owner_snapshot().state;
  if (state == Preserve_trx_manager_state::IDLE)
    return !preserved_trx_has_any_records();
  if (state == Preserve_trx_manager_state::DRAIN_CLEANUP_FAILED &&
      !preserved_trx_has_any_records()) {
    preserved_trx_clear_cleanup_failed_if_no_records();
    return preserve_trx_manager_state_owner_snapshot().state ==
           Preserve_trx_manager_state::IDLE;
  }
  return false;
}

bool preserved_trx_try_disable_feature_for_update() {
  if (!preserved_trx_can_disable_feature()) return false;

  Preserve_trx_manager_state_guard disabling(
      Preserve_trx_manager_state::IDLE, Preserve_trx_manager_state::DISABLING,
      0);
  if (!disabling.active()) return false;

  preserve_trx_notify_manager_state_published_for_unit_test();

  if (preserved_trx_has_any_records()) return false;

  preserve_trx_set_enable_value(false);
  return true;
}

void preserved_trx_set_manager_state_publication_probe_for_unit_test(
    Preserved_trx_manager_state_publication_probe probe, void *arg) {
  g_manager_state_publication_probe = probe;
  g_manager_state_publication_probe_arg = arg;
}

void preserved_trx_set_manager_state_for_unit_test(
    Preserve_trx_manager_state state, my_thread_id owner_thread_id) {
  preserve_trx_store_manager_state_owner(state, owner_thread_id);
}

void preserved_trx_set_recovery_complete_for_unit_test(bool complete) {
  {
    std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
    g_preserved_trx_recovery_done = complete;
    g_preserved_trx_recovery_deferred = false;
  }
  g_preserved_trx_recovery_cond.notify_all();
}

bool preserve_trx_participant_type_is_supported_for_unit_test(
    int legacy_type, Preserve_snapshot_binlog_state binlog_state) {
  return preserve_trx_participant_type_is_supported(
      static_cast<legacy_db_type>(legacy_type), binlog_state);
}

void preserved_trx_add_record_for_unit_test(const std::string &token,
                                            bool observable_only) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  auto remove_token = [&token](const Preserved_trx_record &record) {
    return record.metadata.token == token;
  };
  g_preserved_trx_records.erase(
      std::remove_if(g_preserved_trx_records.begin(),
                     g_preserved_trx_records.end(), remove_token),
      g_preserved_trx_records.end());

  Preserved_trx_record record;
  record.metadata.token = token;
  record.metadata.created_at_us = my_micro_time();
  record.metadata.expires_at_us =
      record.metadata.created_at_us + kMicrosecondsPerSecond;
  record.observable_only = observable_only;
  record.resumable = !observable_only;
  record.state = observable_only ? Preserved_trx_lifecycle_state::FAILED
                                 : Preserved_trx_lifecycle_state::PRESERVED;
  preserved_trx_initialize_record_deadlines(&record);
  preserved_trx_initialize_observable_gc_deadline(&record);
  g_preserved_trx_records.push_back(std::move(record));
}

void preserved_trx_remove_record_for_unit_test(const std::string &token) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  auto remove_token = [&token](const Preserved_trx_record &record) {
    return record.metadata.token == token;
  };
  g_preserved_trx_records.erase(
      std::remove_if(g_preserved_trx_records.begin(),
                     g_preserved_trx_records.end(), remove_token),
      g_preserved_trx_records.end());
}

bool preserved_trx_add_deadline_record_for_unit_test(
    const std::string &token, uint64_t created_wall_us,
    uint64_t expires_wall_us, uint64_t anchor_wall_us,
    uint64_t anchor_monotonic_us) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  auto remove_token = [&token](const Preserved_trx_record &record) {
    return record.metadata.token == token;
  };
  g_preserved_trx_records.erase(
      std::remove_if(g_preserved_trx_records.begin(),
                     g_preserved_trx_records.end(), remove_token),
      g_preserved_trx_records.end());

  Preserved_trx_record record;
  record.metadata.token = token;
  record.metadata.created_at_us = created_wall_us;
  record.metadata.expires_at_us = expires_wall_us;
  record.resumable = true;
  record.state = Preserved_trx_lifecycle_state::PRESERVED;
  preserved_trx_initialize_record_deadlines(&record, anchor_wall_us,
                                            anchor_monotonic_us);
  g_preserved_trx_records.push_back(std::move(record));
  return true;
}

bool preserved_trx_record_expired_for_unit_test(const std::string &token,
                                                uint64_t now_monotonic_us) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (const Preserved_trx_record &record : g_preserved_trx_records) {
    if (record.metadata.token == token) {
      return preserved_trx_record_resume_deadline_expired(record,
                                                          now_monotonic_us);
    }
  }
  return false;
}

uint64_t preserved_trx_monotonic_deadline_after_ms_for_unit_test(
    uint64_t now_monotonic_us, uint64_t timeout_ms) {
  return preserve_trx_monotonic_deadline_after_ms(now_monotonic_us, timeout_ms);
}

bool preserved_trx_monotonic_deadline_expired_for_unit_test(
    uint64_t deadline_monotonic_us, uint64_t now_monotonic_us) {
  return preserve_trx_monotonic_deadline_expired_at(deadline_monotonic_us,
                                                   now_monotonic_us);
}

unsigned long preserved_trx_monotonic_timeout_ms_until_deadline_for_unit_test(
    uint64_t deadline_monotonic_us, unsigned long fallback_timeout_ms,
    uint64_t now_monotonic_us) {
  return preserve_trx_monotonic_timeout_ms_until_deadline_at(
      deadline_monotonic_us, fallback_timeout_ms, now_monotonic_us);
}

bool preserved_trx_recovery_deadline_expired_for_unit_test(
    const Preserve_snapshot_metadata &metadata, uint64_t anchor_wall_us,
    uint64_t anchor_monotonic_us, uint64_t now_monotonic_us) {
  const uint64_t deadline_wall_us =
      preserve_trx_recovery_wall_deadline_for_snapshot(metadata,
                                                       anchor_wall_us);
  const uint64_t deadline_monotonic_us =
      preserve_trx_wall_deadline_to_monotonic(
          deadline_wall_us, anchor_wall_us, anchor_monotonic_us);
  return preserve_trx_monotonic_deadline_expired_at(deadline_monotonic_us,
                                                   now_monotonic_us);
}

void preserved_trx_add_failed_observable_record_for_unit_test(
    const std::string &token, uint64_t anchor_monotonic_us) {
  Preserve_snapshot_metadata metadata;
  metadata.token = token;
  metadata.created_at_us = my_micro_time();
  metadata.expires_at_us = metadata.created_at_us + kMicrosecondsPerSecond;
  Preserved_trx_record record;
  record.metadata = metadata;
  record.trx = nullptr;
  record.resumable = false;
  record.observable_only = true;
  record.state = Preserved_trx_lifecycle_state::FAILED;
  record.last_error = "unit test failure";
  record.last_error_at_us = my_micro_time();
  record.last_error_monotonic_us = anchor_monotonic_us;
  preserved_trx_initialize_record_deadlines(&record);
  preserved_trx_initialize_observable_gc_deadline(&record,
                                                  anchor_monotonic_us);

  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  auto remove_token = [&token](const Preserved_trx_record &candidate) {
    return candidate.metadata.token == token;
  };
  g_preserved_trx_records.erase(
      std::remove_if(g_preserved_trx_records.begin(),
                     g_preserved_trx_records.end(), remove_token),
      g_preserved_trx_records.end());
  g_preserved_trx_records.push_back(std::move(record));
}

size_t preserved_trx_gc_failed_observable_records_for_unit_test(
    uint64_t now_monotonic_us) {
  return preserved_trx_gc_failed_observable_records(now_monotonic_us);
}

bool preserved_trx_observable_record_exists_for_unit_test(
    const std::string &token) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (const Preserved_trx_record &record : g_preserved_trx_records) {
    if (record.metadata.token == token && record.observable_only) return true;
  }
  return false;
}

bool preserved_trx_probe_manager_state_guard_for_unit_test(
    Preserve_trx_manager_state to, my_thread_id owner_thread_id) {
  Preserve_trx_manager_state_guard guard(Preserve_trx_manager_state::IDLE, to,
                                         owner_thread_id);
  return guard.active();
}

bool preserved_trx_shutdown_requested() {
  return preserve_trx_manager_state_owner_snapshot().state ==
         Preserve_trx_manager_state::SHUTDOWN_REQUESTED;
}

static void preserve_trx_wait_for_drain_owner(uint *quiesced_wait_loops) {
  preserve_trx_note_quiesced_wait(quiesced_wait_loops);
  my_sleep(10000);
}

static bool preserve_trx_closing_gate_allows_quiesced_command_read() {
  return preserve_trx_closing_command_gate_active(
      preserve_trx_manager_state_owner_snapshot().state);
}

bool preserved_trx_begin_command_read(THD *thd) {
  if (thd == nullptr) return false;
  if (!preserve_trx_is_enabled()) {
    thd->m_server_idle = true;
    return true;
  }

  uint quiesced_wait_loops = 0;
  for (;;) {
    const bool allow_quiesced_read =
        preserve_trx_closing_gate_allows_quiesced_command_read();
    mysql_mutex_lock(&thd->LOCK_thd_data);
    thd->preserve_trx_inflight_risky_statement_depth = 0;
    thd->preserve_trx_inflight_unknown_query_depth = 0;
    thd->preserve_trx_command_started_monotonic_us = 0;
    thd->preserve_trx_command_packet_before_closing.store(
        false, std::memory_order_release);
    assert(thd->preserve_trx_inflight_risky_statement_depth == 0);
    assert(thd->preserve_trx_inflight_unknown_query_depth == 0);
    if (thd->preserve_trx_batch_state ==
        Preserve_trx_batch_thd_state::PENDING_QUIESCE) {
      thd->m_server_idle = true;
      (void)preserve_trx_publish_pending_quiesce_at_idle_boundary(thd);
    }

    if (allow_quiesced_read &&
        thd->preserve_trx_batch_state ==
            Preserve_trx_batch_thd_state::QUIESCED) {
      thd->m_server_idle = true;
      mysql_mutex_unlock(&thd->LOCK_thd_data);
      return true;
    }

    if (!preserve_trx_batch_state_blocks_target_owner(
            thd->preserve_trx_batch_state) &&
        thd->preserve_trx_batch_state !=
            Preserve_trx_batch_thd_state::DRAINED_NO_TRANSACTION) {
      thd->m_server_idle = true;
      mysql_mutex_unlock(&thd->LOCK_thd_data);
      return true;
    }
    mysql_mutex_unlock(&thd->LOCK_thd_data);
    preserve_trx_wait_for_drain_owner(&quiesced_wait_loops);
  }
}

bool preserved_trx_command_read_is_idle(THD *thd) {
  if (thd == nullptr) return false;
  if (!preserve_trx_is_enabled()) return thd->m_server_idle;

  uint quiesced_wait_loops = 0;
  for (;;) {
    const bool allow_quiesced_read =
        preserve_trx_closing_gate_allows_quiesced_command_read();
    mysql_mutex_lock(&thd->LOCK_thd_data);
    if (allow_quiesced_read &&
        thd->preserve_trx_batch_state ==
            Preserve_trx_batch_thd_state::QUIESCED) {
      const bool was_idle = thd->m_server_idle;
      mysql_mutex_unlock(&thd->LOCK_thd_data);
      return was_idle;
    }
    if (!preserve_trx_batch_state_blocks_target_owner(
            thd->preserve_trx_batch_state)) {
      const bool was_idle = thd->m_server_idle;
      mysql_mutex_unlock(&thd->LOCK_thd_data);
      return was_idle;
    }
    mysql_mutex_unlock(&thd->LOCK_thd_data);
    preserve_trx_wait_for_drain_owner(&quiesced_wait_loops);
  }
}

bool preserved_trx_end_idle_for_command_packet(THD *thd) {
  if (thd == nullptr) return false;
  if (!preserve_trx_is_enabled()) {
    const bool was_idle = thd->m_server_idle;
    if (was_idle) thd->m_server_idle = false;
    return was_idle;
  }

  thd->preserve_trx_command_packet_before_closing.store(
      !preserve_trx_closing_gate_allows_quiesced_command_read(),
      std::memory_order_release);

  uint quiesced_wait_loops = 0;
  for (;;) {
    const bool allow_quiesced_read =
        preserve_trx_closing_gate_allows_quiesced_command_read();
    mysql_mutex_lock(&thd->LOCK_thd_data);
    if (allow_quiesced_read &&
        thd->preserve_trx_batch_state ==
            Preserve_trx_batch_thd_state::QUIESCED) {
      const bool was_idle = thd->m_server_idle;
      if (was_idle) thd->m_server_idle = false;
      mysql_mutex_unlock(&thd->LOCK_thd_data);
      return was_idle;
    }
    if (!preserve_trx_batch_state_blocks_target_owner(
            thd->preserve_trx_batch_state)) {
      const bool was_idle = thd->m_server_idle;
      if (was_idle) thd->m_server_idle = false;
      mysql_mutex_unlock(&thd->LOCK_thd_data);
      return was_idle;
    }
    mysql_mutex_unlock(&thd->LOCK_thd_data);
    preserve_trx_wait_for_drain_owner(&quiesced_wait_loops);
  }
}

bool preserved_trx_end_command_read(THD *thd) {
  if (thd == nullptr) return false;
  if (!preserve_trx_is_enabled()) {
    thd->m_server_idle = false;
    return true;
  }

  uint quiesced_wait_loops = 0;
  for (;;) {
    const bool allow_quiesced_read =
        preserve_trx_closing_gate_allows_quiesced_command_read();
    mysql_mutex_lock(&thd->LOCK_thd_data);
    if (allow_quiesced_read &&
        thd->preserve_trx_batch_state ==
            Preserve_trx_batch_thd_state::QUIESCED) {
      thd->m_server_idle = false;
      mysql_mutex_unlock(&thd->LOCK_thd_data);
      return true;
    }
    if (!preserve_trx_batch_state_blocks_target_owner(
            thd->preserve_trx_batch_state)) {
      thd->m_server_idle = false;
      mysql_mutex_unlock(&thd->LOCK_thd_data);
      return true;
    }
    mysql_mutex_unlock(&thd->LOCK_thd_data);
    preserve_trx_wait_for_drain_owner(&quiesced_wait_loops);
  }
}

bool preserved_trx_wait_if_batch_session_quiesced(THD *thd) {
  if (thd == nullptr) return false;
  if (!preserve_trx_is_enabled()) return false;

  uint quiesced_wait_loops = 0;
  for (;;) {
    const Preserve_trx_batch_thd_state state = preserve_trx_batch_state(thd);
    if (!preserve_trx_batch_state_blocks_target_owner(state)) return false;
    if (state == Preserve_trx_batch_thd_state::QUIESCED &&
        preserve_trx_closing_gate_allows_quiesced_command_read()) {
      return false;
    }
    preserve_trx_wait_for_drain_owner(&quiesced_wait_loops);
  }
}

bool preserved_trx_reject_if_batch_session_drained(THD *thd) {
  if (thd == nullptr) return false;
  if (!preserve_trx_is_enabled()) return false;

  const Preserve_trx_batch_thd_state initial_state =
      preserve_trx_batch_state(thd);
  if (initial_state == Preserve_trx_batch_thd_state::PRESERVED_DRAINED) {
    my_error(ER_PRESERVE_TRX_SESSION_DRAINED, MYF(0));
    return true;
  }

  if (preserved_trx_wait_if_batch_session_quiesced(thd)) {
    return true;
  }

  if (preserve_trx_batch_state(thd) !=
      Preserve_trx_batch_thd_state::PRESERVED_DRAINED)
    return false;

  my_error(ER_PRESERVE_TRX_SESSION_DRAINED, MYF(0));
  return true;
}

Preserve_trx_command_block_result preserved_trx_command_block_result(
    THD *thd, enum_sql_command sql_command) {
  if (thd == nullptr) return Preserve_trx_command_block_result::ALLOW;
  if (!preserve_trx_is_enabled())
    return Preserve_trx_command_block_result::ALLOW;
  if (preserve_trx_batch_state(thd) ==
      Preserve_trx_batch_thd_state::PRESERVED_DRAINED)
    return Preserve_trx_command_block_result::BLOCK_SESSION_DRAINED;

  const Preserve_trx_manager_state_owner manager_snapshot =
      preserve_trx_manager_state_owner_snapshot();
  const Preserve_trx_manager_state state = manager_snapshot.state;
  if (state == Preserve_trx_manager_state::IDLE)
    return Preserve_trx_command_block_result::ALLOW;
  if (state == Preserve_trx_manager_state::DRAIN_CLEANUP_FAILED &&
      !preserved_trx_has_non_observable_records()) {
    preserved_trx_clear_cleanup_failed_if_no_records();
    return Preserve_trx_command_block_result::ALLOW;
  }

  if (preserve_trx_is_ha_control_connection(thd))
    return Preserve_trx_command_block_result::ALLOW;

  if (preserve_trx_closing_command_gate_active(state) &&
      preserve_trx_batch_state(thd) == Preserve_trx_batch_thd_state::NONE &&
      preserve_trx_thd_has_batch_inflight_statement(thd) &&
      thd->preserve_trx_command_packet_before_closing.load(
          std::memory_order_acquire)) {
    /* Wait until the initial target scan classifies this accepted command. */
    std::lock_guard<std::mutex> lock(g_closing_target_classification_mutex);
  }

  if (preserve_trx_batch_state(thd) ==
          Preserve_trx_batch_thd_state::PENDING_QUIESCE &&
      preserve_trx_thd_has_batch_inflight_statement(thd))
    return Preserve_trx_command_block_result::ALLOW;

  const my_thread_id owner_thread_id = manager_snapshot.owner_thread_id;
  if (owner_thread_id != 0 && thd->thread_id() == owner_thread_id)
    return Preserve_trx_command_block_result::ALLOW;

  if (preserve_trx_closing_command_gate_active(state)) {
    if (!thd->is_system_thread())
      return Preserve_trx_command_block_result::BLOCK_CLOSING_DRAINED;
  }

  if (state == Preserve_trx_manager_state::RESET_CLEANUP) {
    if (preserve_trx_sql_command_is_preserve_control_or_shutdown(sql_command))
      return Preserve_trx_command_block_result::BLOCK_DRAINING;
    return Preserve_trx_command_block_result::ALLOW;
  }

  if (state == Preserve_trx_manager_state::DRAIN_CLEANUP_FAILED) {
    if (!preserved_trx_has_non_observable_records()) {
      preserved_trx_clear_cleanup_failed_if_no_records();
      return Preserve_trx_command_block_result::ALLOW;
    }
    if (sql_command == SQLCOM_SHUTDOWN || sql_command == SQLCOM_ROLLBACK ||
        sql_command == SQLCOM_RESUME_PRESERVED_TRX)
      return Preserve_trx_command_block_result::ALLOW;
    if ((sql_command_flags[sql_command] & CF_STATUS_COMMAND) != 0)
      return Preserve_trx_command_block_result::ALLOW;
    if (preserve_trx_sql_command_is_preserve_control_or_shutdown(sql_command))
      return Preserve_trx_command_block_result::BLOCK_DRAINING;
    if (preserve_trx_sql_command_may_create_trx_or_lock(thd, sql_command)) {
      return Preserve_trx_command_block_result::BLOCK_DRAINING;
    }
    return Preserve_trx_command_block_result::ALLOW;
  }

  if (preserve_trx_sql_command_is_preserve_control_or_shutdown(sql_command))
    return Preserve_trx_command_block_result::BLOCK_DRAINING;

  if (state == Preserve_trx_manager_state::WARMCOPY_DRAINING) {
    return Preserve_trx_command_block_result::ALLOW;
  }

  if (preserve_trx_sql_command_may_create_trx_or_lock(thd, sql_command)) {
    return Preserve_trx_command_block_result::BLOCK_DRAINING;
  }

  return Preserve_trx_command_block_result::ALLOW;
}

Preserve_trx_command_block_result preserved_trx_protocol_command_block_result(
    THD *thd, enum enum_server_command command) {
  if (thd == nullptr) return Preserve_trx_command_block_result::ALLOW;
  if (!preserve_trx_is_enabled())
    return Preserve_trx_command_block_result::ALLOW;
  bool waited_for_initial_target_classification = false;
  for (;;) {
    mysql_mutex_lock(&thd->LOCK_thd_data);
    const Preserve_trx_batch_thd_state batch_state =
        thd->preserve_trx_batch_state;
    const bool batch_inflight_statement =
        preserve_trx_thd_has_batch_inflight_statement(thd);
    const bool command_packet_before_closing =
        thd->preserve_trx_command_packet_before_closing.load(
            std::memory_order_acquire);
    mysql_mutex_unlock(&thd->LOCK_thd_data);

    if (batch_state == Preserve_trx_batch_thd_state::PRESERVED_DRAINED)
      return Preserve_trx_command_block_result::BLOCK_SESSION_DRAINED;

    const Preserve_trx_manager_state_owner manager_snapshot =
        preserve_trx_manager_state_owner_snapshot();
    const Preserve_trx_manager_state state = manager_snapshot.state;
    if (state == Preserve_trx_manager_state::IDLE)
      return Preserve_trx_command_block_result::ALLOW;
    if (state == Preserve_trx_manager_state::DRAIN_CLEANUP_FAILED &&
        !preserved_trx_has_non_observable_records()) {
      preserved_trx_clear_cleanup_failed_if_no_records();
      return Preserve_trx_command_block_result::ALLOW;
    }

    const bool closing_command_gate_active =
        preserve_trx_closing_command_gate_active(state);
    if (batch_state == Preserve_trx_batch_thd_state::PENDING_QUIESCE &&
        batch_inflight_statement &&
        (!closing_command_gate_active || command_packet_before_closing)) {
      return Preserve_trx_command_block_result::ALLOW;
    }

    const my_thread_id owner_thread_id = manager_snapshot.owner_thread_id;
    if (owner_thread_id != 0 && thd->thread_id() == owner_thread_id)
      return Preserve_trx_command_block_result::ALLOW;

    if (preserve_trx_is_ha_control_connection(thd)) {
      if (closing_command_gate_active) {
        g_closing_control_connection_commands.fetch_add(
            1, std::memory_order_relaxed);
      }
      return Preserve_trx_command_block_result::ALLOW;
    }

    if (closing_command_gate_active) {
      if (command_packet_before_closing && batch_inflight_statement &&
          batch_state == Preserve_trx_batch_thd_state::NONE &&
          !waited_for_initial_target_classification) {
        DEBUG_SYNC(
            thd,
            "preserve_trx_closing_packet_waiting_for_initial_targets");
        std::lock_guard<std::mutex> lock(
            g_closing_target_classification_mutex);
        waited_for_initial_target_classification = true;
        continue;
      }
      if (preserve_trx_protocol_command_is_no_response_cleanup(command))
        return Preserve_trx_command_block_result::ALLOW;
      return Preserve_trx_command_block_result::BLOCK_CLOSING_DRAINED;
    }

    if (state == Preserve_trx_manager_state::RESET_CLEANUP)
      return Preserve_trx_command_block_result::ALLOW;

    if (state == Preserve_trx_manager_state::WARMCOPY_DRAINING) {
      return Preserve_trx_command_block_result::ALLOW;
    }

    if (preserve_trx_protocol_command_may_create_trx_or_lock(command)) {
      return Preserve_trx_command_block_result::BLOCK_DRAINING;
    }

    return Preserve_trx_command_block_result::ALLOW;
  }
}

bool preserved_trx_mark_inflight_risky_statement(THD *thd,
                                                 enum_sql_command sql_command) {
  return preserved_trx_mark_inflight_risky_statement(
      thd, thd != nullptr ? thd->lex : nullptr, sql_command);
}

bool preserved_trx_mark_inflight_risky_statement(THD *thd, LEX *lex,
                                                 enum_sql_command sql_command) {
  if (thd == nullptr) return false;
  if (!preserve_trx_is_enabled()) return false;
  if (!preserve_trx_has_active_multi_stmt_transaction(thd) &&
      !preserve_trx_sql_command_may_create_trx_or_lock(lex, sql_command))
    return false;

  mysql_mutex_lock(&thd->LOCK_thd_data);
  ++thd->preserve_trx_inflight_risky_statement_depth;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
  return true;
}

bool preserved_trx_mark_inflight_command_packet(
    THD *thd, enum enum_server_command command) {
  if (thd == nullptr || !preserve_trx_is_enabled()) return false;

  switch (command) {
    case COM_QUERY:
    case COM_STMT_EXECUTE:
    case COM_STMT_FETCH:
    case COM_STMT_PREPARE:
      break;
    default:
      return false;
  }

  const ulonglong started_us = preserve_trx_monotonic_us();
  mysql_mutex_lock(&thd->LOCK_thd_data);
  ++thd->preserve_trx_command_sequence;
  thd->preserve_trx_command_started_monotonic_us = started_us;
  ++thd->preserve_trx_inflight_unknown_query_depth;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
  return true;
}

bool preserved_trx_consume_inflight_command_packet(
    THD *thd, enum enum_server_command command) {
  if (thd == nullptr || !preserve_trx_is_enabled()) return false;

  switch (command) {
    case COM_QUERY:
    case COM_STMT_EXECUTE:
    case COM_STMT_FETCH:
    case COM_STMT_PREPARE:
      break;
    default:
      return false;
  }

  mysql_mutex_lock(&thd->LOCK_thd_data);
  const bool consumed = thd->preserve_trx_inflight_unknown_query_depth > 0;
  if (consumed) --thd->preserve_trx_inflight_unknown_query_depth;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
  return consumed;
}

bool preserved_trx_mark_inflight_unknown_query(THD *thd) {
  if (thd == nullptr || !preserve_trx_is_enabled()) return false;

  mysql_mutex_lock(&thd->LOCK_thd_data);
  ++thd->preserve_trx_inflight_unknown_query_depth;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
  return true;
}

void preserved_trx_clear_inflight_risky_statement(THD *thd) {
  mysql_mutex_lock(&thd->LOCK_thd_data);
  assert(thd->preserve_trx_inflight_risky_statement_depth > 0);
  --thd->preserve_trx_inflight_risky_statement_depth;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
}

void preserved_trx_clear_inflight_unknown_query(THD *thd) {
  mysql_mutex_lock(&thd->LOCK_thd_data);
  assert(thd->preserve_trx_inflight_unknown_query_depth > 0);
  --thd->preserve_trx_inflight_unknown_query_depth;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
}

static bool preserved_trx_row_owned_by_account(
    LEX_CSTRING priv_user, LEX_CSTRING priv_host,
    const Preserved_trx_view_row &row) {
  if (row.owner_user.empty() || row.owner_host.empty()) return false;

  return string_eq_lex_cstring(row.owner_user, priv_user) &&
         string_eq_lex_cstring(row.owner_host, priv_host);
}

static bool preserved_trx_row_visible_for_account_internal(
    bool has_process_acl, bool has_resume_any_privilege,
    LEX_CSTRING priv_user, LEX_CSTRING priv_host,
    const Preserved_trx_view_row &row) {
  if (has_process_acl || has_resume_any_privilege) return true;

  return preserved_trx_row_owned_by_account(priv_user, priv_host, row);
}

Preserved_trx_view_rows preserved_trx_snapshot(THD *thd) {
  if (!preserve_trx_is_enabled()) return {};
  preserved_trx_wait_recovery_complete();
  Security_context *sctx = thd != nullptr ? thd->security_context() : nullptr;
  const bool has_process_acl =
      sctx != nullptr && sctx->check_access(PROCESS_ACL);
  const bool has_resume_any_privilege =
      thd != nullptr && preserve_trx_has_resume_any_privilege(thd);
  const LEX_CSTRING priv_user =
      sctx != nullptr ? sctx->priv_user() : LEX_CSTRING{nullptr, 0};
  const LEX_CSTRING priv_host =
      sctx != nullptr ? sctx->priv_host() : LEX_CSTRING{nullptr, 0};

  struct Visible_row_candidate {
    Preserved_trx_view_row row;
    std::string sidecar_token;
    std::string temp_manifest_payload;
  };

  std::vector<Visible_row_candidate> visible_candidates;
  {
    std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
    visible_candidates.reserve(g_preserved_trx_records.size());
    for (const Preserved_trx_record &record : g_preserved_trx_records) {
      Visible_row_candidate candidate;
      candidate.row = record_to_row(record, nullptr);
      candidate.sidecar_token = candidate.row.token;
      if (preserved_trx_row_visible_for_account_internal(
              has_process_acl, has_resume_any_privilege, priv_user, priv_host,
              candidate.row)) {
        candidate.temp_manifest_payload =
            record.metadata.temp_table_manifest_payload;
        if (!has_process_acl) {
          candidate.row.token =
              preserved_trx_redacted_token(candidate.row.token);
        }
        visible_candidates.push_back(std::move(candidate));
      }
    }
  }

  Preserved_trx_view_rows visible_rows;
  visible_rows.reserve(visible_candidates.size());
  for (Visible_row_candidate &candidate : visible_candidates) {
    populate_temp_table_observability(candidate.sidecar_token,
                                      candidate.temp_manifest_payload,
                                      &candidate.row);
    visible_rows.push_back(std::move(candidate.row));
  }

  return visible_rows;
}

size_t preserved_trx_record_count() {
  if (!preserve_trx_is_enabled()) return 0;
  preserved_trx_wait_recovery_complete();

  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  return g_preserved_trx_records.size();
}

bool preserved_trx_row_visible(THD *thd, const Preserved_trx_view_row &row) {
  if (thd == nullptr) return false;
  Security_context *sctx = thd->security_context();
  return preserved_trx_row_visible_for_account_internal(
      sctx->check_access(PROCESS_ACL), preserve_trx_has_resume_any_privilege(thd),
      sctx->priv_user(), sctx->priv_host(), row);
}

bool preserved_trx_row_visible_for_account(bool has_process_acl,
                                           LEX_CSTRING priv_user,
                                           LEX_CSTRING priv_host,
                                           const Preserved_trx_view_row &row) {
  return preserved_trx_row_visible_for_account_internal(
      has_process_acl, false, priv_user, priv_host, row);
}

bool preserved_trx_resume_allowed_for_account(bool owns_token,
                                              bool has_resume_any_privilege) {
  return owns_token || has_resume_any_privilege;
}

bool preserved_trx_metadata_locks_count(
    const Preserve_snapshot_metadata &metadata, uint32_t *lock_count) {
  if (lock_count == nullptr) return false;

  uint32_t total = 0;
  const auto add_count = [&total](uint32_t payload_count) {
    if (payload_count > std::numeric_limits<uint32_t>::max() - total)
      return false;
    total += payload_count;
    return true;
  };
  const auto add_record_payload = [&add_count](const std::string &payload) {
    if (payload.empty()) return true;

    uint32_t payload_count = 0;
    if (!trx_preserve_record_locks_payload_lock_count(payload, &payload_count))
      return false;
    return add_count(payload_count);
  };
  const auto add_table_payload = [&add_count](const std::string &payload) {
    if (payload.empty()) return true;

    uint32_t payload_count = 0;
    if (!trx_preserve_table_locks_payload_lock_count(payload, &payload_count))
      return false;
    return add_count(payload_count);
  };

  if (!add_record_payload(metadata.record_locks_payload) ||
      !add_record_payload(metadata.predicate_locks_payload) ||
      !add_table_payload(metadata.table_locks_payload)) {
    return false;
  }

  *lock_count = total;
  return true;
}

bool preserved_trx_populate_row_locks_count(
    const Preserve_snapshot_metadata &metadata, Preserved_trx_view_row *row) {
  if (row == nullptr) return false;

  uint32_t lock_count = 0;
  if (!preserved_trx_metadata_locks_count(metadata, &lock_count)) {
    row->locks_count = 0;
    row->locks_count_valid = false;
    return false;
  }

  row->locks_count = lock_count;
  row->locks_count_valid = true;
  return true;
}

static Preserve_snapshot_delete_status delete_snapshot_files_with_status(
    const std::string &dir_arg, const std::string &token,
    Preserve_snapshot_remove_options options = {}) {
  auto store = create_preserved_trx_default_store(dir_arg);
  return store->remove_with_status(token, options);
}

static void log_preserved_trx_rejected_binlog_mode(
    const std::string &token, const Preserve_snapshot_metadata &metadata) {
  std::string required_mode;
  if (binlog_state_matches_global_log_bin_mode(metadata, opt_bin_log) &&
      !binlog_gtid_mode_matches(
          metadata,
          static_cast<Gtid_mode::value_type>(Gtid_mode::sysvar_mode))) {
    const auto required_gtid_mode =
        static_cast<Gtid_mode::value_type>(metadata.binlog_gtid_mode);
    required_mode =
        std::string("gtid_mode=") + Gtid_mode::to_string(required_gtid_mode);
  } else {
    switch (metadata.binlog_state) {
    case Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE:
      required_mode = "log_bin=OFF";
      break;
    case Preserve_snapshot_binlog_state::SESSION_OFF_NO_CACHE:
    case Preserve_snapshot_binlog_state::LOGGED_EMPTY:
    case Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE:
      required_mode = "log_bin=ON";
      break;
    }
  }

  const std::string message =
      redacted_preserved_trx_log_subject(token) +
      " rejected because snapshot binlog_state=" +
      binlog_state_name(metadata.binlog_state) +
      " requires " + required_mode;
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
}

static bool log_preserved_trx_cleanup_failure(const std::string &token,
                                              const std::string &reason) {
  const std::string message =
      redacted_preserved_trx_log_subject(token) +
      " recovery cleanup failed: " + reason;
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
  return true;
}

static bool log_preserved_trx_recovery_failure(const std::string &token,
                                               const std::string &reason) {
  const std::string message =
      redacted_preserved_trx_log_subject(token) +
      " recovery failed: " + reason;
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
  return true;
}

static void log_preserved_trx_recovery_warning(const std::string &token,
                                               const std::string &reason) {
  const std::string message =
      redacted_preserved_trx_log_subject(token) +
      " recovery deferred cleanup: " + reason;
  LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
}

static void log_temp_sidecar_bootstrap_state(const std::string &token,
                                             const char *state,
                                             const std::string &reason,
                                             uint32_t attempt = 0) {
  std::string message = std::string("PRESERVE: ") + state + " token='" +
                        preserved_trx_redacted_token(token) + "'";
  if (attempt != 0) {
    message.append(" attempt=");
    message.append(std::to_string(attempt));
  }
  if (!reason.empty()) {
    message.append(" reason=");
    message.append(reason);
  }
  LogErr(state != nullptr && strstr(state, "ABORT") != nullptr ? ERROR_LEVEL
                                                               : INFORMATION_LEVEL,
         ER_LOG_PRINTF_MSG, message.c_str());
}

static const char *preserved_trx_snapshot_read_failure_reason(
    Preserve_snapshot_status status) {
  switch (status) {
    case Preserve_snapshot_status::CORRUPT:
      return "corrupt durable transaction snapshot";
    case Preserve_snapshot_status::IO_ERROR:
      return "I/O error reading durable transaction snapshot";
    case Preserve_snapshot_status::UNSUPPORTED:
      return "unsupported durable transaction snapshot";
    default:
      return "failed to read durable transaction snapshot";
  }
}

Preserve_snapshot_status preserved_trx_load_bundle_for_recover_or_prewarm(
    const std::string &dir, const std::string &token,
    Preserved_trx_recover_load_profile profile, Preserved_trx_bundle *bundle) {
  if (dir.empty() || token.empty() || bundle == nullptr) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }

  Preserved_trx_carrier::Payload_read_mode read_mode =
      Preserved_trx_carrier::Payload_read_mode::SNAPSHOT_ONLY;
  if (profile ==
      Preserved_trx_recover_load_profile::WITH_SEMANTIC_EXTERNAL_BLOBS) {
    read_mode = Preserved_trx_carrier::Payload_read_mode::
        WITH_SEMANTIC_EXTERNAL_BLOBS;
  }

  auto store = create_preserved_trx_default_store(dir);
  return store->read(token, true, read_mode, bundle);
}

Preserve_snapshot_status preserved_trx_dry_validate_loaded_bundle(
    const std::string &dir, const std::string &token,
    const Preserved_trx_bundle &bundle, std::string *reason) {
  if (reason != nullptr) reason->clear();
  const Preserve_snapshot_metadata &metadata = bundle.metadata;
  if (dir.empty() || token.empty() || metadata.token.empty()) {
    if (reason != nullptr) *reason = "durable transaction token is missing";
    return Preserve_snapshot_status::CORRUPT;
  }

  if (!metadata.temp_table_manifest_payload.empty()) {
    Preserve_snapshot_status temp_status =
        preserve_trx_temp_table_validate_sidecars(dir, token, metadata, reason);
    if (temp_status != Preserve_snapshot_status::OK) return temp_status;
  }

  if (!recoverable_binlog_state(metadata.binlog_state)) {
    if (reason != nullptr) {
      *reason = "unsupported durable transaction binlog state";
    }
    return Preserve_snapshot_status::UNSUPPORTED;
  }

  return Preserve_snapshot_status::OK;
}

static const char *preserve_trx_record_lock_export_failure_reason(
    const char *fallback) {
  const char *reason = trx_preserve_last_record_lock_export_error();
  return reason == nullptr || reason[0] == '\0' ? fallback : reason;
}

static void log_preserve_reject_reason(THD *thd, const char *reason) {
  std::string message = "PRESERVE rejected";
  if (thd != nullptr) {
    message.append(" for thread_id=");
    message.append(std::to_string(thd->thread_id()));
  }
  if (reason != nullptr && reason[0] != '\0') {
    message.append(": ");
    message.append(reason);
  }
  LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
}

static bool preserved_trx_recovery_read_failure_requires_startup_abort(
    Preserve_snapshot_status status) {
  return status == Preserve_snapshot_status::IO_ERROR;
}

static bool preserved_trx_preflight_read_failure_requires_startup_abort(
    Preserve_snapshot_status status) {
  return status == Preserve_snapshot_status::IO_ERROR;
}

static bool temp_manifest_image_page_bounds_are_valid(
    const Preserved_temp_table_image_descriptor &image) {
  return image.source_space_id != 0 && image.size != 0 &&
         image.page_size != 0 && image.size % image.page_size == 0 &&
         image.size / image.page_size <=
             std::numeric_limits<uint32_t>::max();
}

static bool build_temp_bootstrap_descriptors(
    const Preserve_snapshot_metadata &metadata,
    std::vector<trx_preserve_temp_space_image_descriptor> *descriptors,
    std::string *reason) {
  if (descriptors == nullptr) return false;
  descriptors->clear();

  Preserved_temp_table_manifest manifest;
  if (!preserve_trx_decode_temp_table_manifest(
          metadata.temp_table_manifest_payload, &manifest)) {
    if (reason != nullptr) *reason = "corrupt temporary table manifest";
    return false;
  }

  std::unordered_map<uint32_t, uint32_t> page_sizes;
  for (const Preserved_temp_table_manifest_entry &entry : manifest.tables) {
    if (!temp_manifest_image_page_bounds_are_valid(entry.image)) {
      if (reason != nullptr) {
        *reason =
            "temporary table manifest has invalid image descriptor during "
            "bootstrap";
      }
      return false;
    }

    const uint32_t source_space_id = entry.image.source_space_id;
    const uint32_t page_size = entry.image.page_size;
    const std::pair<std::unordered_map<uint32_t, uint32_t>::iterator, bool>
        insert_result = page_sizes.emplace(source_space_id, page_size);
    const std::unordered_map<uint32_t, uint32_t>::iterator it =
        insert_result.first;
    const bool inserted = insert_result.second;
    if (!inserted) {
      if (it->second != page_size) {
        if (reason != nullptr) {
          *reason =
              "temporary table manifest has inconsistent page size during "
              "bootstrap";
        }
        return false;
      }
      continue;
    }

    trx_preserve_temp_space_image_descriptor descriptor;
    descriptor.source_space_id = source_space_id;
    descriptor.page_size = page_size;
    descriptor.space_flags = entry.image.space_flags;
    descriptor.image_bytes = entry.image.size;
    std::copy(entry.image.sha256.begin(), entry.image.sha256.end(),
              descriptor.image_digest);
    descriptor.sealed = true;
    descriptors->push_back(descriptor);
  }

  if (descriptors->empty()) {
    if (reason != nullptr) {
      *reason = "temporary table manifest has no image descriptors";
    }
    return false;
  }

  if (reason != nullptr) reason->clear();
  return true;
}

static void release_temp_sidecar_space_id_reservations(
    const Preserve_snapshot_metadata &metadata) {
  for (uint32_t source_space_id :
       preserve_trx_temp_table_sidecar_source_space_ids(metadata)) {
    (void)trx_preserve_temp_space_image_release_reserved_space_id(
        source_space_id);
  }
}

static Preserve_snapshot_status validate_temp_sidecars_for_bootstrap(
    const std::string &dir, const std::string &token,
    const Preserve_snapshot_metadata &metadata, std::string *reason) {
  Preserve_snapshot_status status = Preserve_snapshot_status::OK;
  for (uint32_t attempt = 1; attempt <= kTempSidecarBootstrapIoRetryAttempts;
       ++attempt) {
    status =
        preserve_trx_temp_table_validate_sidecars(dir, token, metadata, reason);
    DBUG_EXECUTE_IF("preserve_trx_simulate_bootstrap_temp_sidecar_io_once", {
      static std::atomic<uint32_t> injected_count{0};
      if (injected_count.fetch_add(1, std::memory_order_relaxed) == 0) {
        status = Preserve_snapshot_status::IO_ERROR;
        if (reason != nullptr)
          *reason = "preserved temporary table sidecar read failed";
      }
    });
    DBUG_EXECUTE_IF("preserve_trx_simulate_bootstrap_temp_sidecar_io_always", {
      status = Preserve_snapshot_status::IO_ERROR;
      if (reason != nullptr)
        *reason = "preserved temporary table sidecar read failed";
    });

    if (status != Preserve_snapshot_status::IO_ERROR) return status;

    const std::string retry_reason =
        reason == nullptr ? "preserved temporary table sidecar read failed"
                          : *reason;
    if (attempt < kTempSidecarBootstrapIoRetryAttempts) {
      log_temp_sidecar_bootstrap_state(token, "TEMP_SIDECAR_IO_RETRY",
                                       retry_reason, attempt);
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    log_temp_sidecar_bootstrap_state(token, "TEMP_SIDECAR_IO_ABORT",
                                     retry_reason, attempt);
  }
  return status;
}

static Preserve_snapshot_status reserve_temp_sidecars_for_resume_retry(
    const std::string &dir, const std::string &token,
    const Preserve_snapshot_metadata &metadata, std::string *reason) {
  if (metadata.temp_table_manifest_payload.empty()) {
    if (reason != nullptr) reason->clear();
    return Preserve_snapshot_status::OK;
  }

  std::vector<trx_preserve_temp_space_image_descriptor> descriptors;
  if (!build_temp_bootstrap_descriptors(metadata, &descriptors, reason)) {
    return Preserve_snapshot_status::CORRUPT;
  }

  /*
    Startup normally reserves preserved source space ids before InnoDB starts
    handing out session temporary tablespaces. If the temp-table subfeature was
    explicitly disabled at startup, RESUME may still become valid after the DBA
    enables it again. In that retry path, reserve any still-free source ids
    before claiming the preserved record. A conflict remains fail-closed and
    leaves the token untouched for operator recovery.
  */
  std::vector<uint32_t> created_space_ids;
  for (const trx_preserve_temp_space_image_descriptor &descriptor :
       descriptors) {
    bool created = false;
    if (!trx_preserve_temp_space_image_reserve_or_keep_space_id(descriptor,
                                                                &created)) {
      for (uint32_t source_space_id : created_space_ids) {
        (void)trx_preserve_temp_space_image_release_reserved_space_id(
            source_space_id);
      }
      if (reason != nullptr) {
        *reason =
            "failed to reserve preserved temporary tablespace id during resume";
      }
      return Preserve_snapshot_status::UNSUPPORTED;
    }
    if (created) created_space_ids.push_back(descriptor.source_space_id);
  }

  const Preserve_snapshot_status sidecar_status =
      validate_temp_sidecars_for_bootstrap(dir, token, metadata, reason);
  if (sidecar_status != Preserve_snapshot_status::OK) {
    for (uint32_t source_space_id : created_space_ids) {
      (void)trx_preserve_temp_space_image_release_reserved_space_id(
          source_space_id);
    }
  }
  return sidecar_status;
}

bool preserved_trx_recovery_read_failure_requires_startup_abort_for_unit_test(
    Preserve_snapshot_status status) {
  return preserved_trx_recovery_read_failure_requires_startup_abort(status);
}

bool preserved_trx_preflight_read_failure_requires_startup_abort_for_unit_test(
    Preserve_snapshot_status status) {
  return preserved_trx_preflight_read_failure_requires_startup_abort(status);
}

static bool delete_orphan_binlog_cache_or_log(const std::string &dir,
                                              const std::string &token) {
  auto store = create_preserved_trx_default_store(dir);
  if (store->remove_with_status(token) != Preserve_snapshot_delete_status::OK) {
    return log_preserved_trx_cleanup_failure(
        token, "failed to delete orphan binlog cache");
  }

  return false;
}

static bool delete_orphan_warm_external_blob_artifact_or_log(
    Preserved_trx_store &store, const std::string &artifact_filename) {
  const Preserve_snapshot_status status =
      store.remove_warm_external_blob_artifact(artifact_filename);
  if (status == Preserve_snapshot_status::OK) return false;

  const std::string message =
      "PRESERVE: failed to delete orphan warm-copy binlog cache artifact '" +
      artifact_filename + "' during recovery, status " +
      std::to_string(static_cast<int>(status));
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
  return true;
}

static bool delete_stale_tmp_files_or_log(Preserved_trx_store &store,
                                          const std::string &token,
                                          bool heavy_cleanup) {
  const Preserve_snapshot_status status =
      store.remove_stale_tmp_files(token, heavy_cleanup);
  if (status == Preserve_snapshot_status::OK) return false;

  return log_preserved_trx_cleanup_failure(
      token, "failed to delete stale temporary snapshot artifacts");
}

static bool delete_preserved_temp_table_sidecars_or_log(
    const std::string &dir, const std::string &token,
    const Preserve_snapshot_metadata &metadata) {
  const Preserve_snapshot_status status =
      preserve_trx_temp_table_remove_token_sidecars(dir, token, metadata);
  if (status == Preserve_snapshot_status::OK) return false;

  return log_preserved_trx_cleanup_failure(
      token, "failed to delete preserved temporary table sidecars");
}

enum class Temp_sidecar_cleanup_mode {
  METADATA_AWARE,
  RAW_UNLINK
};

static bool terminalize_local_snapshot_authority_if_present(
    const std::string &token, const std::string &reason) {
  auto store =
      create_preserved_trx_default_store(preserve_trx_default_dir());
  Preserved_trx_carrier_listing listing;
  if (store->list_tokens(&listing) != Preserve_snapshot_status::OK) {
    return log_preserved_trx_cleanup_failure(
        token, "failed to inspect local authority before source restore");
  }
  if (listing.snapshot_tokens.count(token) == 0 ||
      listing.tainted_tokens.count(token) != 0) {
    return false;
  }
  if (store->mark_tainted(token, reason) != Preserve_snapshot_status::OK) {
    return log_preserved_trx_cleanup_failure(
        token, "failed to terminalize local authority before source restore");
  }
  return false;
}

static bool terminalize_local_snapshot_authorities_if_present(
    const std::vector<Preserve_trx_batch_item> &items,
    const std::string &reason) {
  if (items.empty()) return false;

  auto store =
      create_preserved_trx_default_store(preserve_trx_default_dir());
  Preserved_trx_carrier_listing listing;
  if (store->list_tokens(&listing) != Preserve_snapshot_status::OK) {
    return log_preserved_trx_cleanup_failure(
        items.front().token,
        "failed to inspect local authorities before source restore");
  }
  for (const Preserve_trx_batch_item &item : items) {
    if (listing.snapshot_tokens.count(item.token) == 0 ||
        listing.tainted_tokens.count(item.token) != 0) {
      continue;
    }
    if (store->mark_tainted(item.token, reason) !=
        Preserve_snapshot_status::OK) {
      return log_preserved_trx_cleanup_failure(
          item.token,
          "failed to terminalize local authority before source restore");
    }
  }
  return false;
}

static bool delete_preserved_snapshot_files_and_sidecars_or_log(
    const std::string &dir, const std::string &token,
    const Preserve_snapshot_metadata *metadata,
    Temp_sidecar_cleanup_mode temp_sidecar_cleanup_mode =
        Temp_sidecar_cleanup_mode::METADATA_AWARE) {
  Preserve_snapshot_remove_options remove_options;
  if (metadata != nullptr && !metadata->temp_table_manifest_payload.empty() &&
      temp_sidecar_cleanup_mode == Temp_sidecar_cleanup_mode::METADATA_AWARE) {
    /*
      Temp-table sidecars may contain no-redo undo reservation evidence.  Keep
      them through the generic snapshot removal so the temp cleanup path can
      read the undo sidecar, release allocator reservations, and then remove the
      token-owned sidecars in one metadata-aware step.
    */
    remove_options.preserve_committed_temp_sidecar_source_space_ids =
        preserve_trx_temp_table_sidecar_source_space_ids(*metadata);
  }
  if (delete_snapshot_files_with_status(dir, token, remove_options) !=
      Preserve_snapshot_delete_status::OK) {
    return log_preserved_trx_cleanup_failure(
        token, "failed to delete snapshot files");
  }

  if (metadata != nullptr) {
    release_temp_sidecar_space_id_reservations(*metadata);
  }
  if (metadata != nullptr && !metadata->temp_table_manifest_payload.empty() &&
      temp_sidecar_cleanup_mode == Temp_sidecar_cleanup_mode::RAW_UNLINK) {
    /*
      Recovery reaches RAW_UNLINK only after the sidecar has already been
      classified as corrupt/unsupported, or the token has been tainted. Do not
      parse the sidecar again during cleanup; remove token-owned files by name
      and release any manifest-declared ownership reservations that do not
      require sidecar bytes.
    */
    preserve_trx_temp_table_release_ownership_reservations(*metadata);
  } else if (metadata != nullptr &&
             !metadata->temp_table_manifest_payload.empty() &&
             delete_preserved_temp_table_sidecars_or_log(dir, token,
                                                         *metadata)) {
    return true;
  }

  return false;
}

static bool delete_orphan_temp_table_sidecars_or_log(
    Preserved_trx_store &store,
    const Preserved_trx_carrier_listing &listing) {
  bool failed = false;
  for (const std::string &token : listing.temp_sidecar_tokens) {
    if (listing.snapshot_tokens.find(token) != listing.snapshot_tokens.end())
      continue;

    const Preserve_snapshot_delete_status status =
        store.remove_with_status(token);
    if (status == Preserve_snapshot_delete_status::OK) continue;

    std::string message =
        "PRESERVE: failed to delete orphan preserved temporary table sidecars "
        "for token '" +
        preserved_trx_redacted_token(token) + "' during recovery, status " +
        std::to_string(static_cast<int>(status));
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    failed = true;
  }
  for (const std::string &token : listing.stale_tmp_tokens) {
    if (listing.snapshot_tokens.find(token) != listing.snapshot_tokens.end() ||
        listing.temp_sidecar_tokens.find(token) !=
            listing.temp_sidecar_tokens.end()) {
      continue;
    }
    if (delete_stale_tmp_files_or_log(store, token, true)) failed = true;
  }
  return failed;
}

static void delete_detached_mdl_context(const std::string &token);
static bool restore_detached_mdl_context(THD *thd, const std::string &token);
static bool restore_record_after_resume_failure(Preserved_trx_record &record,
                                                const std::string &reason);

static bool rollback_preserved_snapshot_or_log(
    const std::string &dir, const std::string &token,
    const std::string &reason,
    const Preserve_snapshot_metadata *metadata = nullptr,
    Temp_sidecar_cleanup_mode temp_sidecar_cleanup_mode =
        Temp_sidecar_cleanup_mode::METADATA_AWARE) {
  const std::string message =
      redacted_preserved_trx_log_subject(token) +
      " recovery forced rollback: " + reason;
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());

  auto store = create_preserved_trx_default_store(dir);
  if (store->mark_tainted(token, reason) != Preserve_snapshot_status::OK) {
    return log_preserved_trx_cleanup_failure(
        token, "failed to terminalize ACTIVE Undo authority before rollback");
  }

  XID xid;
  if (preserve_trx_token_to_xid(token, &xid)) {
    return log_preserved_trx_cleanup_failure(
        token, "failed to map durable transaction token to XID");
  }

  trx_t *trx = trx_preserve_startup_resurrection_find_verified(token);
  if (trx != nullptr && !trx_preserve_validate_reserved_exact(trx, xid)) {
    return log_preserved_trx_cleanup_failure(
        token, "ACTIVE Undo exact reservation changed before rollback");
  }
  if (trx != nullptr && trx_preserve_rollback_claimed(trx) != DB_SUCCESS) {
    return log_preserved_trx_cleanup_failure(
        token, "failed to rollback durable transaction");
  }

  audit_preserved_trx_event(
      current_thd, token, "rollback",
      reason.find("timeout") != std::string::npos ? "timeout" : "failure",
      reason.c_str());
  delete_detached_mdl_context(token);
  return delete_preserved_snapshot_files_and_sidecars_or_log(
      dir, token, metadata, temp_sidecar_cleanup_mode);
}

static bool rollback_claimed_preserved_snapshot_or_log(
    const std::string &dir, const std::string &token, trx_t *trx,
    const std::string &reason,
    const Preserve_snapshot_metadata *metadata = nullptr,
    Temp_sidecar_cleanup_mode temp_sidecar_cleanup_mode =
        Temp_sidecar_cleanup_mode::METADATA_AWARE) {
  const std::string message =
      redacted_preserved_trx_log_subject(token) +
      " recovery forced rollback: " + reason;
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());

  auto store = create_preserved_trx_default_store(dir);
  if (store->mark_tainted(token, reason) != Preserve_snapshot_status::OK) {
    return log_preserved_trx_cleanup_failure(
        token, "failed to terminalize ACTIVE Undo authority before rollback");
  }

  if (trx_preserve_rollback_claimed(trx) != DB_SUCCESS) {
    return log_preserved_trx_cleanup_failure(
        token, "failed to rollback durable transaction");
  }

  audit_preserved_trx_event(
      current_thd, token, "rollback",
      reason.find("timeout") != std::string::npos ? "timeout" : "failure",
      reason.c_str());
  delete_detached_mdl_context(token);
  return delete_preserved_snapshot_files_and_sidecars_or_log(
      dir, token, metadata, temp_sidecar_cleanup_mode);
}

static bool preserve_trx_recovery_deadline_expired(
    const Preserve_snapshot_metadata &metadata, uint64_t anchor_wall_us,
    uint64_t anchor_monotonic_us) {
  const uint64_t deadline_wall_us =
      preserve_trx_recovery_wall_deadline_for_snapshot(metadata,
                                                       anchor_wall_us);
  const uint64_t deadline_monotonic_us =
      preserve_trx_wall_deadline_to_monotonic(
          deadline_wall_us, anchor_wall_us, anchor_monotonic_us);
  return preserve_trx_monotonic_deadline_expired_at(
      deadline_monotonic_us, preserve_trx_monotonic_us());
}

bool preserved_trx_resume_deadline_expired(
    const Preserve_snapshot_metadata &metadata) {
  const uint64_t anchor_wall_us = my_micro_time();
  const uint64_t anchor_monotonic_us = preserve_trx_monotonic_us();
  const uint64_t deadline_wall_us =
      preserve_trx_wall_deadline_for_record(metadata);
  const uint64_t deadline_monotonic_us =
      preserve_trx_wall_deadline_to_monotonic(
          deadline_wall_us, anchor_wall_us, anchor_monotonic_us);
  return preserve_trx_monotonic_deadline_expired_at(deadline_monotonic_us,
                                                   anchor_monotonic_us);
}

static bool restore_record_after_resume_failure(
    Preserved_trx_record &record, const std::string &reason) {
  /*
    Resume failure before activation must put the record back with the reason
    attached. The preserved transaction remains observable and eligible for
    later cleanup instead of disappearing from SHOW/PFS state.
  */
  if (!preserved_trx_add_record_with_last_error(record, reason)) {
    preserve_trx_set_record_last_error(&record, reason);
    DEBUG_SYNC(current_thd,
               "preserve_trx_after_resume_failure_record_readd_with_error");
    return false;
  }

  return log_preserved_trx_cleanup_failure(
      record.metadata.token,
      "failed to restore preserved transaction record after " + reason);
}

static bool rollback_resumable_record_after_resume_timeout(
    Preserved_trx_record &record) {
  const std::string message =
      redacted_preserved_trx_log_subject(record.metadata.token) +
      " resume forced rollback: recovery timeout expired";
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());

  bool inject_rollback_failure = false;
  DBUG_EXECUTE_IF("preserve_trx_fail_resume_timeout_rollback",
                  inject_rollback_failure = true;);
  const dberr_t rollback_status =
      inject_rollback_failure ? DB_ERROR
                              : trx_preserve_rollback_claimed(record.trx);
  if (rollback_status != DB_SUCCESS) {
    const std::string reason =
        record.last_error.empty()
            ? "resume timeout rollback failure"
            : record.last_error + "; resume timeout rollback failure";
    (void)restore_record_after_resume_failure(
        record, reason);
    return true;
  }

  audit_preserved_trx_event(current_thd, record.metadata.token, "rollback",
                            "timeout", "recovery timeout expired");
  delete_detached_mdl_context(record.metadata.token);
  return delete_preserved_snapshot_files_and_sidecars_or_log(
      preserve_trx_default_dir(), record.metadata.token, &record.metadata);
}

static bool rollback_resumable_record_after_corrupt_snapshot(
    Preserved_trx_record &record) {
  const char reason[] = "corrupt durable transaction snapshot";
  const std::string message =
      redacted_preserved_trx_log_subject(record.metadata.token) +
      " resume forced rollback: " + reason;
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());

  if (trx_preserve_rollback_claimed(record.trx) != DB_SUCCESS) {
    const std::string restore_reason =
        record.last_error.empty()
            ? "corrupt durable transaction snapshot rollback failure"
            : record.last_error +
                  "; corrupt durable transaction snapshot rollback failure";
    (void)restore_record_after_resume_failure(record, restore_reason);
    return true;
  }

  audit_preserved_trx_event(current_thd, record.metadata.token, "rollback",
                            "failure", reason);
  delete_detached_mdl_context(record.metadata.token);
  return delete_preserved_snapshot_files_and_sidecars_or_log(
      preserve_trx_default_dir(), record.metadata.token, &record.metadata);
}

static bool rollback_expired_resumable_record_by_reaper(
    Preserved_trx_record &record) {
  const std::string token = record.metadata.token;
  const std::string message =
      redacted_preserved_trx_log_subject(token) +
      " expired-token reaper forced rollback: recovery timeout expired";
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());

  bool rollback_failed = false;
  DBUG_EXECUTE_IF("preserve_trx_fail_expired_reaper_rollback",
                  rollback_failed = true;);
  if (!rollback_failed &&
      trx_preserve_rollback_claimed(record.trx) != DB_SUCCESS) {
    rollback_failed = true;
  }

  if (rollback_failed) {
    preserved_trx_remove_observable_record(
        token, Preserved_trx_lifecycle_state::EXPIRED_ROLLBACK);
    (void)preserved_trx_add_record_with_last_error(
        record, kExpiredReaperRollbackFailure);
    return true;
  }

  audit_preserved_trx_event(nullptr, token, "rollback", "expired",
                            "recovery timeout expired");
  delete_detached_mdl_context(token);

  bool cleanup_failed = false;
  DBUG_EXECUTE_IF("preserve_trx_fail_expired_reaper_cleanup",
                  cleanup_failed = true;);
  if (!cleanup_failed &&
      delete_preserved_snapshot_files_and_sidecars_or_log(
          preserve_trx_default_dir(), token, &record.metadata)) {
    cleanup_failed = true;
  }

  preserved_trx_remove_observable_record(
      token, Preserved_trx_lifecycle_state::EXPIRED_ROLLBACK);
  if (cleanup_failed) {
    auto store =
        create_preserved_trx_default_store(preserve_trx_default_dir());
    const bool taint_failed =
        store->mark_tainted(token, "expired-token reaper cleanup failure") !=
        Preserve_snapshot_status::OK;
    if (taint_failed) {
      log_preserved_trx_cleanup_failure(
          token,
          "failed to mark snapshot tainted after expired-token reaper cleanup "
          "failure");
    }
    preserved_trx_add_observable_error_record(
        record.metadata, Preserved_trx_lifecycle_state::EXPIRED_CLEANUP_FAILED,
        taint_failed ? "expired-token reaper cleanup failure; failed to mark "
                       "snapshot tainted"
                     : "expired-token reaper cleanup failure");
    return true;
  }

  return false;
}

static bool preserved_trx_claim_expired_resumable_record_for_reaper(
    Preserved_trx_record *record) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  const uint64_t now_us = preserve_trx_monotonic_us();
  auto expired = g_preserved_trx_records.end();
  for (auto it = g_preserved_trx_records.begin();
       it != g_preserved_trx_records.end(); ++it) {
    if (it->observable_only || !it->resumable ||
        !preserved_trx_record_resume_deadline_expired(*it, now_us)) {
      continue;
    }
    if (it->last_error == kExpiredReaperRollbackFailure &&
        it->last_error_monotonic_us != 0) {
      const bool within_retry_backoff =
          now_us < it->last_error_monotonic_us ||
          now_us - it->last_error_monotonic_us <
              kExpiredReaperRollbackFailureRetryBackoffUs;
      if (within_retry_backoff) continue;
    }
    expired = it;
    break;
  }
  if (expired == g_preserved_trx_records.end()) return false;

  if (!preserve_trx_compare_exchange_manager_state_owner(
          Preserve_trx_manager_state::IDLE,
          Preserve_trx_manager_state::EXPIRED_ROLLBACK, 0)) {
    return false;
  }

  Preserved_trx_record claimed = *expired;
  g_preserved_trx_records.erase(expired);
  Preserved_trx_record observable;
  observable.metadata = claimed.metadata;
  observable.trx = claimed.trx;
  observable.resumable = false;
  observable.state = Preserved_trx_lifecycle_state::EXPIRED_ROLLBACK;
  observable.observable_only = true;
  preserved_trx_initialize_record_deadlines(&observable);
  preserved_trx_initialize_observable_gc_deadline(&observable);
  g_preserved_trx_records.push_back(std::move(observable));
  if (record != nullptr) *record = claimed;

  preserve_trx_store_manager_state_owner(Preserve_trx_manager_state::IDLE, 0);
  return true;
}

bool preserved_trx_expired_reaper_claim_releases_manager_state_for_unit_test(
    const std::string &token) {
  preserved_trx_remove_record_for_unit_test(token);
  preserved_trx_remove_observable_record(
      token, Preserved_trx_lifecycle_state::EXPIRED_ROLLBACK);

  Preserved_trx_record record;
  const uint64_t now_us = my_micro_time();
  record.metadata.token = token;
  record.metadata.created_at_us = now_us - 2 * kMicrosecondsPerSecond;
  record.metadata.expires_at_us = now_us - kMicrosecondsPerSecond;
  record.metadata.recovered_count = 2;
  record.resumable = true;
  record.state = Preserved_trx_lifecycle_state::PRESERVED;
  preserved_trx_initialize_record_deadlines(&record);
  {
    std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
    g_preserved_trx_records.push_back(record);
  }

  Preserved_trx_record claimed;
  const bool claimed_record =
      preserved_trx_claim_expired_resumable_record_for_reaper(&claimed);
  const bool released_manager =
      preserved_trx_manager_state() == Preserve_trx_manager_state::IDLE;

  preserved_trx_remove_observable_record(
      token, Preserved_trx_lifecycle_state::EXPIRED_ROLLBACK);
  preserved_trx_remove_record_for_unit_test(token);

  return claimed_record && released_manager &&
         claimed.metadata.token == token;
}

bool preserved_trx_expired_reaper_empty_claim_keeps_manager_idle_for_unit_test(
    const std::string &token) {
  preserved_trx_remove_record_for_unit_test(token);
  preserved_trx_remove_observable_record(
      token, Preserved_trx_lifecycle_state::EXPIRED_ROLLBACK);

  Preserved_trx_record claimed;
  const bool claimed_record =
      preserved_trx_claim_expired_resumable_record_for_reaper(&claimed);
  return !claimed_record &&
         preserved_trx_manager_state() == Preserve_trx_manager_state::IDLE &&
         !preserved_trx_observable_record_exists_for_unit_test(token);
}

static void preserved_trx_expired_reaper_scan_once() {
  if (!preserve_trx_is_enabled()) return;
  DBUG_EXECUTE_IF("preserve_trx_expired_reaper_skip", return;);

  preserved_trx_cleanup_deferred_source_warm_blobs_once();
  preserved_trx_reset_attempt_reaper_scan_once();
  (void)preserved_trx_gc_failed_observable_records();
  try {
    const uint64_t now_us = preserve_trx_monotonic_us();
    preserve_trx_transfer_receiver_reaper_scan_once(now_us);
    (void)preserved_trx_strict_prepared_token_registry().expire_once(now_us);
  } catch (...) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           "PRESERVE: strict receiver resource reaper pass failed");
  }

  constexpr uint kMaxExpiredRecordsPerPass = 16;
  for (uint i = 0; i < kMaxExpiredRecordsPerPass; ++i) {
    Preserved_trx_record record;
    if (!preserved_trx_claim_expired_resumable_record_for_reaper(&record)) break;
    (void)rollback_expired_resumable_record_by_reaper(record);
  }
}

static void preserved_trx_expired_reaper_thread() {
  const bool init_failed =
      g_preserved_trx_reaper_fail_init_for_unit_test.load() ||
      my_thread_init();

  {
    std::unique_lock<std::mutex> lock(g_preserved_trx_reaper_mutex);
    g_preserved_trx_reaper_cond.wait(lock, [] {
      return !g_preserved_trx_reaper_pause_init_report_for_unit_test;
    });
    g_preserved_trx_reaper_init_reported = true;
    g_preserved_trx_reaper_init_failed = init_failed;
  }
  g_preserved_trx_reaper_cond.notify_all();
  if (init_failed) return;
  auto thread_end_guard = create_scope_guard([] { my_thread_end(); });

  std::unique_lock<std::mutex> lock(g_preserved_trx_reaper_mutex);
  while (!g_preserved_trx_reaper_stop) {
    g_preserved_trx_reaper_cond.wait_for(lock, std::chrono::seconds(1), [] {
      return g_preserved_trx_reaper_stop ||
             g_preserved_trx_reaper_scan_requested;
    });
    if (g_preserved_trx_reaper_stop) break;
    g_preserved_trx_reaper_scan_requested = false;
    lock.unlock();
    preserved_trx_expired_reaper_scan_once();
    lock.lock();
  }
}

void preserved_trx_request_expired_reaper_scan() {
  {
    std::lock_guard<std::mutex> lock(g_preserved_trx_reaper_mutex);
    g_preserved_trx_reaper_scan_requested = true;
  }
  g_preserved_trx_reaper_cond.notify_one();
}

bool preserved_trx_start_expired_reaper() {
  DBUG_EXECUTE_IF("preserve_trx_expired_reaper_skip", return true;);
  std::unique_lock<std::mutex> lock(g_preserved_trx_reaper_mutex);
  g_preserved_trx_reaper_cond.wait(lock, [] {
    return !g_preserved_trx_reaper_starting &&
           !g_preserved_trx_reaper_stopping;
  });
  if (g_preserved_trx_reaper_started) return true;
  g_preserved_trx_reaper_starting = true;
  g_preserved_trx_reaper_stop = false;
  g_preserved_trx_reaper_init_reported = false;
  g_preserved_trx_reaper_init_failed = false;
  try {
    g_preserved_trx_reaper_thread =
        std::thread(preserved_trx_expired_reaper_thread);
  } catch (...) {
    g_preserved_trx_reaper_starting = false;
    g_preserved_trx_reaper_cond.notify_all();
    return false;
  }
  g_preserved_trx_reaper_cond.wait(
      lock, [] { return g_preserved_trx_reaper_init_reported; });
  if (g_preserved_trx_reaper_init_failed) {
    std::thread failed_reaper = std::move(g_preserved_trx_reaper_thread);
    lock.unlock();
    if (failed_reaper.joinable()) failed_reaper.join();
    lock.lock();
    g_preserved_trx_reaper_init_reported = false;
    g_preserved_trx_reaper_init_failed = false;
    g_preserved_trx_reaper_starting = false;
    g_preserved_trx_reaper_cond.notify_all();
    return false;
  }
  g_preserved_trx_reaper_started = true;
  g_preserved_trx_reaper_starting = false;
  g_preserved_trx_reaper_cond.notify_all();
  return true;
}

void preserved_trx_start_expired_reaper_if_ready() {
  if (!preserve_trx_is_enabled()) return;
  {
    std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
    if (!g_preserved_trx_recovery_done) return;
  }
  if (!preserved_trx_start_expired_reaper()) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           "PRESERVE: failed to initialize expired-token reaper");
  }
}

void preserved_trx_stop_expired_reaper() {
  std::thread reaper;
  {
    std::unique_lock<std::mutex> lock(g_preserved_trx_reaper_mutex);
    g_preserved_trx_reaper_cond.wait(lock, [] {
      return !g_preserved_trx_reaper_starting &&
             !g_preserved_trx_reaper_stopping;
    });
    if (!g_preserved_trx_reaper_started) {
      g_preserved_trx_reaper_scan_requested = false;
      return;
    }
    g_preserved_trx_reaper_stopping = true;
    g_preserved_trx_reaper_stop = true;
    reaper = std::move(g_preserved_trx_reaper_thread);
    g_preserved_trx_reaper_started = false;
  }
  g_preserved_trx_reaper_cond.notify_all();
  if (reaper.joinable()) reaper.join();
  {
    std::lock_guard<std::mutex> lock(g_preserved_trx_reaper_mutex);
    g_preserved_trx_reaper_stop = false;
    g_preserved_trx_reaper_scan_requested = false;
    g_preserved_trx_reaper_stopping = false;
  }
  g_preserved_trx_reaper_cond.notify_all();
}

bool preserved_trx_start_expired_reaper_for_unit_test(bool fail_thread_init) {
  g_preserved_trx_reaper_fail_init_for_unit_test.store(fail_thread_init);
  const bool started = preserved_trx_start_expired_reaper();
  g_preserved_trx_reaper_fail_init_for_unit_test.store(false);
  return started;
}

bool preserved_trx_expired_reaper_started_for_unit_test() {
  std::lock_guard<std::mutex> lock(g_preserved_trx_reaper_mutex);
  return g_preserved_trx_reaper_started;
}

void preserved_trx_set_expired_reaper_init_pause_for_unit_test(bool pause) {
  {
    std::lock_guard<std::mutex> lock(g_preserved_trx_reaper_mutex);
    g_preserved_trx_reaper_pause_init_report_for_unit_test = pause;
  }
  g_preserved_trx_reaper_cond.notify_all();
}

bool preserved_trx_expired_reaper_starting_for_unit_test() {
  std::lock_guard<std::mutex> lock(g_preserved_trx_reaper_mutex);
  return g_preserved_trx_reaper_starting;
}

class Preserve_detached_mdl_owner final : public MDL_context_owner {
 public:
  void enter_cond(mysql_cond_t *, mysql_mutex_t *, const PSI_stage_info *,
                  PSI_stage_info *, const char *, const char *, int) override {}
  void exit_cond(const PSI_stage_info *, const char *, const char *,
                 int) override {}
  int is_killed() const override { return false; }
  THD *get_thd() override { return nullptr; }
  void notify_shared_lock(MDL_context_owner *, bool) override {}
  bool notify_hton_pre_acquire_exclusive(const MDL_key *, bool *) override {
    return false;
  }
  void notify_hton_post_release_exclusive(const MDL_key *) override {}
  uint get_rand_seed() const override { return 1; }
  bool is_connected() override { return true; }
};

static bool mdl_payload_to_requests(const std::string &payload,
                                    std::vector<MDL_request> *requests) {
  uint32_t descriptor_count = 0;
  if (!mdl_descriptors_payload_is_valid(payload, &descriptor_count))
    return true;

  if (requests == nullptr) return true;
  requests->resize(descriptor_count);

  size_t offset = 4;
  for (uint32_t i = 0; i < descriptor_count; ++i) {
    const auto mdl_namespace = static_cast<MDL_key::enum_mdl_namespace>(
        static_cast<unsigned char>(payload[offset]));
    const auto mdl_type = static_cast<enum_mdl_type>(
        static_cast<unsigned char>(payload[offset + 1]));
    const uint16_t db_length = read_le16(payload, offset + 8);
    const uint16_t part_key_length = read_le16(payload, offset + 10);
    offset += 12;

    const char *part_key = payload.data() + offset;
    if (mdl_preserve_normalized_namespace(mdl_namespace)) {
      const char *normalized_name = nullptr;
      size_t normalized_name_length = 0;
      const char *object_name = nullptr;
      if (!mdl_normalized_part_key_components(
              part_key, db_length, part_key_length, &normalized_name,
              &normalized_name_length, &object_name)) {
        return true;
      }
      MDL_key key;
      key.mdl_key_init(mdl_namespace, part_key, normalized_name,
                       normalized_name_length, object_name);
      MDL_REQUEST_INIT_BY_KEY(&(*requests)[i], &key, mdl_type,
                              MDL_TRANSACTION);
    } else {
      MDL_REQUEST_INIT_BY_PART_KEY(&(*requests)[i], mdl_namespace, part_key,
                                   part_key_length, db_length, mdl_type,
                                   MDL_TRANSACTION);
    }
    offset += part_key_length;
  }

  return offset != payload.length();
}

static const uchar *preserve_mdl_key_data(const std::string &token) {
  return pointer_cast<const uchar *>(token.data());
}

static bool create_detached_mdl_context(
    const Preserve_snapshot_metadata &metadata) {
  DBUG_EXECUTE_IF("preserve_trx_fail_recover_mdl", return true;);

  std::vector<MDL_request> requests;
  if (mdl_payload_to_requests(metadata.mdl_descriptors_payload, &requests)) {
    return true;
  }

  Preserve_detached_mdl_owner owner;
  MDL_context context;
  context.init(&owner);

  bool error = false;
  /*
    Snapshot descriptors are persisted in the original MDL ticket order
    (newest to oldest). The detached context is cloned into the backup manager
    and later cloned back into a RESUME THD; both clone operations insert at
    the destination front. Acquire oldest to newest here so the two clone
    reversals restore the original order used by savepoint MDL ordinals.
  */
  for (auto it = requests.rbegin(); it != requests.rend(); ++it) {
    if (context.acquire_lock(&*it, LONG_TIMEOUT)) {
      error = true;
      break;
    }
  }

  if (!error) {
    error = MDL_context_backup_manager::instance().create_backup(
        &context, preserve_mdl_key_data(metadata.token), metadata.token.length(),
        MDL_context_backup_manager::MDL_context_backup_policy::PRESERVE_STRICT);
  }

  context.release_transactional_locks();
  context.destroy();
  return error;
}

static bool create_detached_mdl_context(THD *thd, const std::string &token) {
  return MDL_context_backup_manager::instance().create_backup(
      &thd->mdl_context, preserve_mdl_key_data(token), token.length(),
      MDL_context_backup_manager::MDL_context_backup_policy::PRESERVE_STRICT);
}

static bool restore_detached_mdl_context(THD *thd, const std::string &token) {
  DBUG_EXECUTE_IF("preserve_trx_fail_resume_transfer_mdl", return true;);
  return MDL_context_backup_manager::instance().restore_backup(
      &thd->mdl_context, preserve_mdl_key_data(token), token.length(),
      MDL_context_backup_manager::MDL_context_backup_policy::PRESERVE_STRICT);
}

static void delete_detached_mdl_context(const std::string &token) {
  if (!token_is_filename_safe(token)) return;
  MDL_context_backup_manager::instance().delete_backup(
      preserve_mdl_key_data(token), token.length());
}

static bool cleanup_reset_batch_record_artifacts(
    const Preserve_trx_batch_reset_cleanup &cleanup) {
  if (cleanup.token.empty()) return false;

  Preserve_snapshot_delete_status delete_status =
      delete_snapshot_files_with_status(preserve_trx_default_dir(),
                                        cleanup.token);
  if (delete_status ==
      Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE) {
    log_preserved_trx_cleanup_failure(cleanup.token,
                                      "failed to delete snapshot files");
    return true;
  }

  delete_detached_mdl_context(cleanup.token);
  if (delete_status ==
      Preserve_snapshot_delete_status::ERROR_AFTER_SNAPSHOT_DELETE) {
    log_preserved_trx_cleanup_failure(
        cleanup.token,
        "failed to fsync preserved transaction directory after unlink");
  }

  return delete_status != Preserve_snapshot_delete_status::OK;
}

static void preserve_trx_kill_connection(THD *thd) {
  if (thd == nullptr) return;
  mysql_mutex_lock(&thd->LOCK_thd_data);
  if (thd->killed != THD::KILL_CONNECTION) thd->awake(THD::KILL_CONNECTION);
  mysql_mutex_unlock(&thd->LOCK_thd_data);
}

static bool restore_batch_record_semantics_to_original_thd(
    THD *thd, const Preserve_trx_batch_item &item,
    Preserve_trx_batch_reset_cleanup *deferred_cleanup) {
  if (thd == nullptr || item.token.empty()) return true;

  Preserved_trx_record record;
  if (!preserved_trx_take_record(item.token, &record)) return true;

  Resume_thd_state_guard thd_state_guard(thd);
  bool binlog_imported = false;
  bool gtid_restored = false;
  bool attached = false;
  bool mdl_transferred = false;

  auto restore_record_after_failure = [&](const std::string &reason) {
    if (attached) {
      if (trx_preserve_detach_resumed_from_thd(record.trx, thd) == DB_SUCCESS) {
        attached = false;
        reset_thd_after_preserve_detach(thd);
      } else {
        const std::string retry_message =
            "Preserved transaction batch cleanup failed to detach "
            "reattached transaction after " +
            reason + "; retrying cleanup detach";
        LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, retry_message.c_str());
        if (trx_preserve_detach_resumed_from_thd_for_cleanup(record.trx, thd) ==
            DB_SUCCESS) {
          attached = false;
          reset_thd_after_preserve_detach(thd);
        } else {
          const std::string kill_message =
              "Preserved transaction batch cleanup failed to detach "
              "reattached transaction after " +
              reason + "; killing session";
          LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, kill_message.c_str());
          preserved_trx_add_resume_detach_failure_observable_record(record,
                                                                    reason);
          thd->killed = THD::KILL_CONNECTION;
          thd_state_guard.dismiss();
          return true;
        }
      }
    }
    rollback_restored_logged_cache_gtid_next(thd, &gtid_restored);
    if (binlog_imported) {
      discard_binlog_preserve_cache_and_reset_scopes(thd);
      binlog_imported = false;
    }
    if (mdl_transferred) thd->mdl_context.release_transactional_locks();
    delete_detached_mdl_context(item.token);
    const dberr_t rollback_status = trx_preserve_rollback_claimed(record.trx);
    bool cleanup_failed = rollback_status != DB_SUCCESS;
    if (rollback_status == DB_SUCCESS) {
      cleanup_failed = delete_preserved_snapshot_files_and_sidecars_or_log(
          preserve_trx_default_dir(), item.token, &record.metadata);
    } else {
      record.state = Preserved_trx_lifecycle_state::FAILED;
      record.resumable = false;
      (void)preserved_trx_add_record_with_last_error(record, reason);
    }
    preserve_trx_set_batch_state(thd, 0, Preserve_trx_batch_thd_state::NONE);
    preserve_trx_kill_connection(thd);
    thd_state_guard.dismiss();
    return cleanup_failed;
  };

  if (record.metadata.tx_isolation > ISO_SERIALIZABLE ||
      set_tx_isolation(thd,
                       static_cast<enum_tx_isolation>(
                           record.metadata.tx_isolation),
                       true)) {
    return restore_record_after_failure("batch cleanup isolation restore failure");
  }
  if (restore_preserved_session_variables(thd, record.metadata)) {
    return restore_record_after_failure(
        "batch cleanup session state restore failure");
  }
  if (!preserved_trx_resume_binlog_format_is_supported(thd,
                                                        record.metadata)) {
    return restore_record_after_failure(
        "batch cleanup binlog format restore failure");
  }
  if (restore_preserved_dml_policy(thd, record.trx, record.metadata)) {
    return restore_record_after_failure(
        "batch cleanup DML policy restore failure");
  }

  thd->variables.sql_log_bin = record.metadata.session_sql_log_bin;
  if (record.metadata.option_bin_log)
    thd->variables.option_bits |= OPTION_BIN_LOG;
  else
    thd->variables.option_bits &= ~OPTION_BIN_LOG;
  restore_preserved_transaction_access_mode(thd, record.metadata);
  restore_last_insert_id_state(thd, record.metadata);
  restore_forced_insert_id_state(thd, record.metadata);
  DBUG_EXECUTE_IF("preserve_trx_batch_clear_user_vars_before_reattach_restore", {
    mysql_mutex_lock(&thd->LOCK_thd_data);
    thd->user_vars.clear();
    mysql_mutex_unlock(&thd->LOCK_thd_data);
  });
  if (import_user_vars_payload(thd, record.metadata.user_vars_payload)) {
    return restore_record_after_failure(
        "batch cleanup user variables restore failure");
  }

  if (record.metadata.binlog_state ==
      Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE) {
    if (item.source_rollback_image != nullptr &&
        item.source_rollback_image->native_binlog_cache_retained) {
      if (mysql_binlog_preserve_reactivate_after_detach_failure(
              thd, item.source_rollback_image->binlog_snapshot)) {
        return restore_record_after_failure(
            "batch cleanup retained binlog cache reactivation failure");
      }
    } else {
      Preserve_memory_lease binlog_payload_lease;
      if (hydrate_logged_binlog_cache_payload_if_needed(
              &record, item.token, item.source_rollback_image.get(),
              &binlog_payload_lease, item.local_authority_staged))
        return restore_record_after_failure(
            "batch cleanup binlog cache read failure");
      Mysql_binlog_preserve_snapshot binlog_snapshot =
          metadata_to_binlog_cache_snapshot(record.metadata);
      discard_binlog_preserve_cache_and_reset_scopes(thd);
      if (mysql_binlog_preserve_import(thd, binlog_snapshot)) {
        return restore_record_after_failure(
            "batch cleanup binlog cache import failure");
      }
    }
    binlog_imported = true;
  }

  if (restore_detached_mdl_context(thd, item.token)) {
    return restore_record_after_failure("batch cleanup MDL transfer failure");
  }
  mdl_transferred = true;

  if (preserve_snapshot_allows_gtid_restore(record.metadata)) {
    if (restore_logged_cache_gtid_next(thd, record.metadata)) {
      return restore_record_after_failure(
          "batch cleanup binlog GTID ownership restore failure");
    }
    gtid_restored = true;
  }

  if (trx_preserve_reattach_preserved_to_original_thd(record.trx, thd) !=
      DB_SUCCESS) {
    return restore_record_after_failure("batch cleanup reattach failure");
  }
  attached = true;
  mark_preserved_transaction_attached(thd, record.metadata);

  if (restore_savepoints_to_thd(thd, record.trx, record.metadata)) {
    return restore_record_after_failure("batch cleanup savepoint restore failure");
  }

  reset_preserve_statement_transaction_scope(thd);

  if (trx_preserve_activate_reattached_in_original_thd(record.trx, thd) !=
          DB_SUCCESS ||
      trx_preserve_finish_resumed_activation(record.trx, thd) != DB_SUCCESS) {
    return restore_record_after_failure("batch cleanup undo activation failure");
  }

  if (deferred_cleanup != nullptr) {
    /*
      Strict standby transfer rejects temp-table state. RESET can therefore
      publish the original session as runnable before deleting token-keyed
      process-local artifacts, without retaining a large metadata copy.
    */
    if (!record.metadata.temp_table_manifest_payload.empty()) {
      return restore_record_after_failure(
          "batch reset cleanup encountered temp-table state");
    }
    deferred_cleanup->token = item.token;
    preserve_trx_set_batch_state(thd, 0, Preserve_trx_batch_thd_state::NONE);
    thd_state_guard.dismiss();
    return false;
  }

  const Preserve_snapshot_delete_status delete_status =
      delete_snapshot_files_with_status(preserve_trx_default_dir(), item.token);
  if (delete_status ==
      Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE) {
    log_preserved_trx_cleanup_failure(item.token,
                                      "failed to delete snapshot files");
    auto store = create_preserved_trx_default_store(preserve_trx_default_dir());
    if (store->mark_tainted(item.token, "batch stale snapshot delete failure") !=
        Preserve_snapshot_status::OK) {
      return restore_record_after_failure("failed to mark stale snapshot tainted");
    }
    delete_detached_mdl_context(item.token);
    preserve_trx_set_batch_state(thd, 0, Preserve_trx_batch_thd_state::NONE);
    thd_state_guard.dismiss();
    return true;
  }
  bool cleanup_failed_after_unlink =
      delete_status ==
      Preserve_snapshot_delete_status::ERROR_AFTER_SNAPSHOT_DELETE;

  bool temp_sidecar_cleanup_failed = false;
  if (delete_status !=
      Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE) {
    release_temp_sidecar_space_id_reservations(record.metadata);
    if (!record.metadata.temp_table_manifest_payload.empty()) {
      temp_sidecar_cleanup_failed =
          delete_preserved_temp_table_sidecars_or_log(
              preserve_trx_default_dir(), item.token, record.metadata);
    }
  }

  delete_detached_mdl_context(item.token);

  if (delete_status ==
      Preserve_snapshot_delete_status::ERROR_AFTER_SNAPSHOT_DELETE) {
    log_preserved_trx_cleanup_failure(
        item.token,
        "failed to fsync preserved transaction directory after unlink");
  }

  preserve_trx_set_batch_state(thd, 0, Preserve_trx_batch_thd_state::NONE);
  thd_state_guard.dismiss();
  return cleanup_failed_after_unlink || temp_sidecar_cleanup_failed;
}

class Preserve_batch_restore_target_collector final : public Do_THD_Impl {
 public:
  Preserve_batch_restore_target_collector(
      ulonglong generation, const std::vector<my_thread_id> &thread_ids)
      : m_generation(generation), m_targets(thread_ids.size()) {
    for (size_t i = 0; i < thread_ids.size(); ++i) {
      if (!m_target_indexes.emplace(thread_ids[i], i).second) m_error = true;
    }
  }

  void operator()(THD *candidate) override {
    if (candidate == nullptr) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    const auto target_it = m_target_indexes.find(candidate->thread_id());
    const bool same_generation =
        candidate->preserve_trx_batch_generation == m_generation;
    if (target_it != m_target_indexes.end() && !same_generation) {
      m_error = true;
    }
    const bool target =
        target_it != m_target_indexes.end() && same_generation &&
        candidate->preserve_trx_batch_state ==
            Preserve_trx_batch_thd_state::PRESERVED_DRAINED;
    std::unique_ptr<Preserve_trx_external_thd_pin> pin;
    if (target) {
      pin = Preserve_trx_external_thd_pin::acquire_locked(candidate);
    } else if (same_generation &&
               candidate->preserve_trx_batch_state !=
                   Preserve_trx_batch_thd_state::PRESERVED_DRAINED) {
      candidate->preserve_trx_batch_generation = 0;
      candidate->preserve_trx_batch_state = Preserve_trx_batch_thd_state::NONE;
      candidate->preserve_trx_temp_table_batch_capture_epoch.store(
          false, std::memory_order_release);
      preserve_trx_temp_table_clear_batch_unsupported_boundary(candidate);
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);

    if (!target) return;
    if (pin == nullptr) {
      m_error = true;
      return;
    }
    Preserve_trx_pinned_thd &slot = m_targets[target_it->second];
    if (slot.thd != nullptr) {
      m_error = true;
      return;
    }
    slot.thd = candidate;
    slot.pin = std::move(pin);
  }

  bool error() const { return m_error; }
  bool complete() const {
    if (m_error) return false;
    return std::all_of(
        m_targets.begin(), m_targets.end(),
        [](const Preserve_trx_pinned_thd &target) {
          return target.thd != nullptr && target.pin != nullptr;
        });
  }
  std::vector<Preserve_trx_pinned_thd> &targets() { return m_targets; }

 private:
  ulonglong m_generation;
  std::unordered_map<my_thread_id, size_t> m_target_indexes;
  std::vector<Preserve_trx_pinned_thd> m_targets;
  bool m_error{false};
};

static bool restore_preserved_batch_items_to_original_thds(
    ulonglong generation, const std::vector<Preserve_trx_batch_item> &items,
    const std::shared_ptr<Preserve_trx_drain_attempt> &active_drain_attempt,
    std::vector<Preserve_trx_batch_reset_cleanup> *deferred_cleanup = nullptr) {
  if (active_drain_attempt != nullptr &&
      !active_drain_attempt->ownership.restore_allowed()) {
    preserve_trx_transfer_note_source_restore_guard_reject();
    return true;
  }

  if (terminalize_local_snapshot_authorities_if_present(
          items, "source batch restore revoked local authority")) {
    return true;
  }

  /*
    Roll back a partially successful batch in reverse preserve order. Later
    targets may depend on earlier batch state being held drained until their
    own token has been restored or cleaned up.
  */
  std::vector<my_thread_id> thread_ids;
  thread_ids.reserve(items.size());
  for (const Preserve_trx_batch_item &item : items)
    thread_ids.push_back(item.original_thread_id);

  Preserve_batch_restore_target_collector collector(generation, thread_ids);
  Global_THD_manager::get_instance()->do_for_all_thd_copy(&collector);
  if (!collector.complete()) return true;

  std::vector<Preserve_trx_pinned_thd> &targets = collector.targets();
  for (size_t i = items.size(); i != 0; --i) {
    Preserve_trx_batch_reset_cleanup cleanup;
    if (restore_batch_record_semantics_to_original_thd(
            targets[i - 1].thd, items[i - 1],
            deferred_cleanup == nullptr ? nullptr : &cleanup)) {
      return true;
    }
    if (deferred_cleanup != nullptr)
      deferred_cleanup->push_back(std::move(cleanup));
  }
  return false;
}

namespace {

static void restore_preserved_batch_items_for_reset(
    ulonglong generation, std::vector<Preserve_trx_batch_item> items,
    const std::shared_ptr<Preserve_trx_drain_attempt> &attempt) {
  if (attempt == nullptr || !attempt->ownership.restore_allowed()) {
    preserve_trx_transfer_note_source_restore_guard_reject();
    preserve_trx_reset_invariant_failure("reset_restore_not_allowed", attempt);
  }

  if (terminalize_local_snapshot_authorities_if_present(
          items, "RESET DRAIN revoked local authority")) {
    preserve_trx_reset_invariant_failure(
        "reset_local_authority_terminalize_failed", attempt);
  }

  if (items.empty()) {
    Preserve_batch_clear_generation clear(generation);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
  }

  std::vector<my_thread_id> thread_ids;
  thread_ids.reserve(items.size());
  std::unordered_set<std::string> unique_tokens;
  for (const Preserve_trx_batch_item &item : items) {
    if (item.original_thread_id == 0 || item.token.empty() ||
        !unique_tokens.insert(item.token).second) {
      preserve_trx_reset_invariant_failure("duplicate_reset_batch_identity",
                                           attempt);
    }
    thread_ids.push_back(item.original_thread_id);
  }

  Preserve_batch_restore_target_collector collector(generation, thread_ids);
  Global_THD_manager::get_instance()->do_for_all_thd_copy(&collector);
  if (collector.error()) {
    preserve_trx_reset_invariant_failure("reset_target_collection_failed",
                                         attempt);
  }

  std::vector<Preserve_trx_pinned_thd> &targets = collector.targets();
  for (size_t i = items.size(); i != 0; --i) {
    Preserve_trx_batch_item &item = items[i - 1];
    Preserve_trx_pinned_thd &target = targets[i - 1];

    if (target.thd == nullptr) {
      Preserved_trx_record record;
      if (preserved_trx_find_record(item.token, &record) &&
          (record.trx == nullptr || record.metadata.token != item.token ||
           !record.metadata.temp_table_manifest_payload.empty())) {
        preserve_trx_reset_invariant_failure(
            "invalid_detached_record_for_reset", attempt);
      }
      item.reset_disposition =
          Preserve_trx_reset_disposition::CONNECTION_TEARDOWN_PENDING;
      continue;
    }

    Preserve_trx_batch_reset_cleanup cleanup;
    if (!restore_batch_record_semantics_to_original_thd(target.thd, item,
                                                        &cleanup)) {
      continue;
    }

    preserve_trx_kill_connection(target.thd);
    Preserved_trx_record record;
    if (preserved_trx_find_record(item.token, &record) &&
        (record.trx == nullptr || record.metadata.token != item.token ||
         !record.metadata.temp_table_manifest_payload.empty())) {
      preserve_trx_reset_invariant_failure(
          "invalid_record_after_reset_restore_failure", attempt);
    }
    item.reset_disposition =
        Preserve_trx_reset_disposition::CONNECTION_TEARDOWN_PENDING;
  }

  std::lock_guard<std::mutex> lock(attempt->quarantine_mutex);
  if (!attempt->quarantined_items.empty()) {
    preserve_trx_reset_invariant_failure("duplicate_reset_restore_executor",
                                         attempt);
  }
  attempt->quarantined_items = std::move(items);
}

static bool preserve_trx_try_restore_quarantined_reset(
    const std::shared_ptr<Preserve_trx_drain_attempt> &attempt) {
  if (attempt == nullptr) return false;
  bool expected = true;
  if (!attempt->source_restore_context_ready.compare_exchange_strong(
          expected, false, std::memory_order_acq_rel)) {
    return false;
  }
  auto reset_restore_guard = create_scope_guard([&] {
    preserve_trx_reset_invariant_failure(
        "quarantined_reset_exited_before_release_barrier", attempt);
  });
  if (!attempt->ownership.begin_source_restore()) {
    preserve_trx_reset_invariant_failure(
        "quarantined_reset_source_restore_transition_failed", attempt);
  }

  std::vector<Preserve_trx_batch_item> items;
  {
    std::lock_guard<std::mutex> lock(attempt->quarantine_mutex);
    items = std::move(attempt->quarantined_items);
  }
  restore_preserved_batch_items_for_reset(attempt->generation,
                                          std::move(items), attempt);
  preserve_trx_publish_active_drain_reset_barrier(attempt);
  reset_restore_guard.commit();
  preserved_trx_request_expired_reaper_scan();
  return true;
}

class Preserve_reset_live_thread_ids final : public Do_THD_Impl {
 public:
  void operator()(THD *candidate) override {
    if (candidate != nullptr) m_thread_ids.insert(candidate->thread_id());
  }

  bool contains(my_thread_id thread_id) const {
    return m_thread_ids.count(thread_id) != 0;
  }

 private:
  std::unordered_set<my_thread_id> m_thread_ids;
};

static void preserved_trx_reset_attempt_reaper_scan_once() {
  const std::shared_ptr<Preserve_trx_drain_attempt> attempt =
      preserve_trx_active_drain_attempt_snapshot();
  if (attempt == nullptr ||
      !attempt->reset_release_barrier_complete.load(std::memory_order_acquire) ||
      preserve_trx_manager_state_owner_snapshot().state !=
          Preserve_trx_manager_state::RESET_CLEANUP) {
    return;
  }

  Preserve_reset_live_thread_ids live_threads;
  Global_THD_manager::get_instance()->do_for_all_thd_copy(&live_threads);
  {
    std::lock_guard<std::mutex> lock(attempt->quarantine_mutex);
    constexpr size_t kMaxResetTokensPerPass = 16;
    size_t processed = 0;
    for (auto it = attempt->quarantined_items.begin();
         it != attempt->quarantined_items.end() &&
         processed < kMaxResetTokensPerPass;) {
      bool ready_for_artifact_cleanup = false;
      switch (it->reset_disposition) {
        case Preserve_trx_reset_disposition::RESTORED_RUNNABLE:
          ready_for_artifact_cleanup = true;
          break;
        case Preserve_trx_reset_disposition::CONNECTION_TEARDOWN_PENDING:
          if (live_threads.contains(it->original_thread_id)) {
            ++it;
            ++processed;
            continue;
          }
          {
            Preserved_trx_record record;
            if (preserved_trx_take_record(it->token, &record)) {
              if (record.trx == nullptr || record.metadata.token != it->token ||
                  !record.metadata.temp_table_manifest_payload.empty()) {
                preserve_trx_reset_invariant_failure(
                    "reset_reaper_invalid_teardown_record", attempt);
              }
              if (trx_preserve_rollback_claimed(record.trx) != DB_SUCCESS) {
                if (preserved_trx_add_record_with_error(
                        record, "RESET DRAIN deferred rollback failed")) {
                  preserve_trx_reset_invariant_failure(
                      "reset_reaper_could_not_restore_failed_record", attempt);
                }
                ++it;
                ++processed;
                continue;
              }
            }
          }
          ready_for_artifact_cleanup = true;
          break;
      }

      if (ready_for_artifact_cleanup &&
          !cleanup_reset_batch_record_artifacts({it->token})) {
        it = attempt->quarantined_items.erase(it);
      } else {
        ++it;
      }
      ++processed;
    }

    if (!attempt->quarantined_source_warmcopy_ids.empty()) {
      auto carrier =
          create_preserved_trx_process_local_warm_external_blob_carrier(
              preserve_trx_default_dir());
      if (carrier != nullptr &&
          carrier->remove_warm_external_blobs(
              attempt->quarantined_source_warmcopy_ids,
              kPreservedTrxBlobBinlogCache) ==
              Preserved_trx_carrier_status::OK) {
        attempt->quarantined_source_warmcopy_ids.clear();
      }
    }

    if (!attempt->quarantined_items.empty() ||
        !attempt->quarantined_source_warmcopy_ids.empty() ||
        !attempt->drain_scope_released.load(std::memory_order_acquire)) {
      return;
    }
  }

  if (!attempt->ownership.complete_source_restore()) {
    preserve_trx_reset_invariant_failure(
        "reset_reaper_source_restore_completion_failed", attempt);
  }
  const Preserve_trx_manager_state_owner manager =
      preserve_trx_manager_state_owner_snapshot();
  if (manager.state != Preserve_trx_manager_state::RESET_CLEANUP ||
      (manager.owner_thread_id != 0 &&
       manager.owner_thread_id != attempt->owner_thread_id) ||
      !preserve_trx_compare_exchange_manager_state_owner(
          manager.state, manager.owner_thread_id,
          Preserve_trx_manager_state::IDLE, 0)) {
    preserve_trx_reset_invariant_failure("reset_reaper_manager_retire_failed",
                                         attempt);
  }
  preserve_trx_clear_active_drain_attempt(attempt);
}

}  // namespace

static bool preserve_trx_transition_manager_after_handoff_resolution(
    const Preserve_trx_drain_attempt &attempt,
    Preserve_trx_manager_state desired_state, my_thread_id desired_owner) {
  for (;;) {
    const Preserve_trx_manager_state_owner current =
        preserve_trx_manager_state_owner_snapshot();
    if (current.state == desired_state &&
        current.owner_thread_id == desired_owner) {
      return true;
    }
    if (current.owner_thread_id != attempt.owner_thread_id) return false;
    switch (current.state) {
      case Preserve_trx_manager_state::WARMCOPY_DRAINING:
      case Preserve_trx_manager_state::WARMCOPY_CLOSING:
      case Preserve_trx_manager_state::BATCH_DRAINING:
        break;
      default:
        return false;
    }
    if (preserve_trx_compare_exchange_manager_state_owner(
            current.state, current.owner_thread_id, desired_state,
            desired_owner)) {
      return true;
    }
  }
}

Preserve_trx_transfer_status preserved_trx_resolve_handoff_unknown(
    const Preserve_trx_ha_control_capability &capability,
    const Preserve_trx_handoff_resolution_proof &proof) {
  std::unique_lock<std::mutex> active_guard(g_active_drain_attempt_mutex);
  std::shared_ptr<Preserve_trx_drain_attempt> attempt;
  if (g_active_drain_attempt != nullptr &&
      g_active_drain_attempt->handoff_resolution.matches_context(proof)) {
    attempt = g_active_drain_attempt;
  } else if (g_last_resolved_drain_attempt != nullptr &&
             g_last_resolved_drain_attempt->handoff_resolution.matches_context(
                 proof)) {
    attempt = g_last_resolved_drain_attempt;
  } else if (g_active_drain_attempt != nullptr) {
    attempt = g_active_drain_attempt;
  } else {
    attempt = g_last_resolved_drain_attempt;
  }
  if (attempt == nullptr) return Preserve_trx_transfer_status::UNSUPPORTED;
  if (!attempt->handoff_resolution_ready.load(std::memory_order_acquire)) {
    return Preserve_trx_transfer_status::ACK_UNCERTAIN;
  }

  bool first_decision = false;
  const Preserve_trx_transfer_status begin_status =
      attempt->handoff_resolution.begin(capability, proof, &first_decision);
  if (begin_status != Preserve_trx_transfer_status::OK || !first_decision) {
    return begin_status;
  }
  if (g_active_drain_attempt != attempt ||
      attempt->ownership.state() !=
          Preserve_trx_drain_terminal::COMMIT_UNKNOWN) {
    (void)attempt->handoff_resolution.complete(
        proof, Preserve_trx_transfer_status::CORRUPT);
    return Preserve_trx_transfer_status::CORRUPT;
  }

  const bool source_owns =
      proof.resolution ==
      Preserve_trx_handoff_resolution::RECEIVER_FENCED_SOURCE_OWNS;
  Preserve_trx_transfer_status result = Preserve_trx_transfer_status::CORRUPT;
  if (source_owns) {
    if (!attempt->ownership.begin_source_restore()) {
      (void)attempt->handoff_resolution.complete(proof, result);
      return result;
    }
    auto source_restore_guard = create_scope_guard([&] {
      preserve_trx_reset_invariant_failure(
          "handoff_resolution_source_restore_incomplete", attempt);
    });
    {
      std::lock_guard<std::mutex> quarantine_guard(attempt->quarantine_mutex);
      if (restore_preserved_batch_items_to_original_thds(
              attempt->generation, attempt->quarantined_items, attempt)) {
        result = Preserve_trx_transfer_status::IO_ERROR;
        (void)attempt->handoff_resolution.complete(proof, result);
        return result;
      }
    }
    if (!preserve_trx_transition_manager_after_handoff_resolution(
            *attempt, Preserve_trx_manager_state::IDLE, 0)) {
      (void)attempt->handoff_resolution.complete(proof, result);
      return result;
    }
    if (!attempt->ownership.complete_source_restore()) {
      (void)attempt->handoff_resolution.complete(proof, result);
      return result;
    }
    result = Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN;
    preserve_trx_publish_active_drain_reset_barrier(attempt);
    source_restore_guard.commit();
  } else {
    if (!attempt->ownership.acknowledge_commit() ||
        !preserve_trx_transition_manager_after_handoff_resolution(
            *attempt, Preserve_trx_manager_state::TRANSFER_HANDOFF_COMPLETE,
            0)) {
      (void)attempt->handoff_resolution.complete(proof, result);
      return result;
    }
    result = Preserve_trx_transfer_status::COMMITTED_NOT_READY;
  }

  std::set<std::string> deferred_source_warm_cleanup;
  {
    std::lock_guard<std::mutex> quarantine_guard(attempt->quarantine_mutex);
    attempt->quarantined_items.clear();
    deferred_source_warm_cleanup =
        std::move(attempt->quarantined_source_warmcopy_ids);
    attempt->quarantine_started_monotonic_us = 0;
  }
  preserve_trx_defer_source_warm_blob_cleanup(
      preserve_trx_default_dir(), std::move(deferred_source_warm_cleanup));
  preserve_trx_transfer_note_source_handoff_committed();
  if (!attempt->handoff_resolution.complete(proof, result)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (source_owns) {
    g_active_drain_attempt.reset();
    g_last_resolved_drain_attempt = std::move(attempt);
  }
  return result;
}

static bool reattach_current_batch_preserve_failure_to_original_thd(
    THD *thd, trx_t *trx, const std::string &token, bool detached_mdl_context,
    bool snapshot_files_may_exist, bool resumable_authority_may_exist,
    bool *cleanup_failed, bool *left_preserved,
    const Preserve_snapshot_metadata *metadata,
    bool has_logged_binlog_cache = false,
    const Mysql_binlog_preserve_snapshot *binlog_snapshot = nullptr,
    const std::vector<Preserved_trx_external_blob_descriptor>
        *blob_descriptors = nullptr) {
  if (thd == nullptr || trx == nullptr || token.empty()) return true;
  if (cleanup_failed != nullptr) *cleanup_failed = false;
  if (left_preserved != nullptr) *left_preserved = false;

  auto preserve_detached_snapshot_after_cleanup_failure =
      [&](const std::string &reason) {
        /*
          If cleanup fails after a snapshot may exist, keep an in-memory record
          pointing at the claimed transaction. That gives recovery and operators
          a durable token to resolve instead of losing ownership of a prepared
          trx.
        */
        if (!resumable_authority_may_exist || metadata == nullptr) return true;

        std::vector<Preserved_trx_external_blob_descriptor> descriptors;
        if (blob_descriptors != nullptr) descriptors = *blob_descriptors;

        Preserved_trx_record record;
        record.metadata = *metadata;
        record.trx = trx;
        record.resumable = true;
        record.state = Preserved_trx_lifecycle_state::PRESERVED;
        record.blob_descriptors = std::move(descriptors);
        (void)preserved_trx_add_record_with_last_error(record, reason);
        if (cleanup_failed != nullptr) *cleanup_failed = true;
        if (left_preserved != nullptr) *left_preserved = true;
        return false;
      };

  auto delete_snapshot_after_activation = [&]() {
    /*
      Once the transaction is active again in the original THD, the snapshot is
      stale. A pre-unlink failure leaves the token tainted; a post-unlink fsync
      failure is logged but the active transaction remains with the session.
    */
    Preserve_snapshot_delete_status delete_status =
        Preserve_snapshot_delete_status::OK;
    if (snapshot_files_may_exist) {
      delete_status =
          delete_snapshot_files_with_status(preserve_trx_default_dir(), token);
    }
    if (delete_status ==
        Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE) {
      if (cleanup_failed != nullptr) *cleanup_failed = true;
      log_preserved_trx_cleanup_failure(token,
                                        "failed to delete snapshot files");
    }
    if (delete_status ==
        Preserve_snapshot_delete_status::ERROR_AFTER_SNAPSHOT_DELETE) {
      if (cleanup_failed != nullptr) *cleanup_failed = true;
    }
    if (snapshot_files_may_exist &&
        delete_status !=
            Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE &&
        metadata != nullptr) {
      release_temp_sidecar_space_id_reservations(*metadata);
      if (!metadata->temp_table_manifest_payload.empty() &&
          delete_preserved_temp_table_sidecars_or_log(
              preserve_trx_default_dir(), token, *metadata)) {
        if (cleanup_failed != nullptr) *cleanup_failed = true;
      }
    }
    return delete_status;
  };

  bool mdl_restored = false;
  if (detached_mdl_context) {
    if (restore_detached_mdl_context(thd, token)) {
      return preserve_detached_snapshot_after_cleanup_failure(
          "batch cleanup MDL transfer failure");
    }
    mdl_restored = true;
  }

  bool binlog_reactivated = false;
  if (has_logged_binlog_cache) {
    if (binlog_snapshot == nullptr ||
        mysql_binlog_preserve_reactivate_after_detach_failure(
            thd, *binlog_snapshot)) {
      if (mdl_restored) thd->mdl_context.release_transactional_locks();
      return preserve_detached_snapshot_after_cleanup_failure(
          "batch cleanup binlog cache restore failure");
    }
    binlog_reactivated = true;
  }

  if (trx_preserve_reattach_preserved_to_original_thd(trx, thd) != DB_SUCCESS) {
    if (binlog_reactivated) discard_binlog_preserve_cache_and_reset_scopes(thd);
    if (mdl_restored) thd->mdl_context.release_transactional_locks();
    return preserve_detached_snapshot_after_cleanup_failure(
        "batch cleanup reattach failure");
  }

  if (trx_preserve_activate_reattached_in_original_thd(trx, thd) != DB_SUCCESS ||
      trx_preserve_finish_resumed_activation(trx, thd) != DB_SUCCESS) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           "Preserved transaction batch cleanup failed to activate reattached "
           "transaction");
    if (trx_preserve_detach_resumed_from_thd_for_cleanup(trx, thd) !=
        DB_SUCCESS) {
      LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
             "Preserved transaction batch cleanup failed to detach transaction "
             "after activation failure; killing session");
      if (cleanup_failed != nullptr) *cleanup_failed = true;
      thd->killed = THD::KILL_CONNECTION;
      if (binlog_reactivated) discard_binlog_preserve_cache_and_reset_scopes(thd);
      if (mdl_restored) thd->mdl_context.release_transactional_locks();
      return true;
    }
    if (binlog_reactivated) discard_binlog_preserve_cache_and_reset_scopes(thd);
    if (mdl_restored) thd->mdl_context.release_transactional_locks();
    return preserve_detached_snapshot_after_cleanup_failure(
        "batch cleanup undo activation failure");
  }

  const Preserve_snapshot_delete_status delete_status =
      delete_snapshot_after_activation();
  if (delete_status ==
      Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE) {
    auto store = create_preserved_trx_default_store(preserve_trx_default_dir());
    if (store->mark_tainted(token, "batch cleanup snapshot delete failure") !=
        Preserve_snapshot_status::OK) {
      LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
             "Preserved transaction batch cleanup failed to mark stale "
             "snapshot tainted after delete failure");
      if (cleanup_failed != nullptr) *cleanup_failed = true;
      return true;
    }
    if (detached_mdl_context) delete_detached_mdl_context(token);
    reset_preserve_xid_to_active_transaction_xid(thd);
    reset_preserve_statement_transaction_scope(thd);
    preserve_trx_set_batch_state(thd, 0, Preserve_trx_batch_thd_state::NONE);
    return false;
  }

  reset_preserve_xid_to_active_transaction_xid(thd);
  reset_preserve_statement_transaction_scope(thd);

  if (delete_status ==
      Preserve_snapshot_delete_status::ERROR_AFTER_SNAPSHOT_DELETE) {
    log_preserved_trx_cleanup_failure(
        token,
        "failed to fsync preserved transaction directory after unlink");
  }
  if (detached_mdl_context) delete_detached_mdl_context(token);
  return false;
}

static bool reactivate_current_batch_prepared_failure_to_original_thd(
    THD *thd,
    trx_preserve_thd_transition_failure *reactivate_failure = nullptr) {
  if (reactivate_failure != nullptr) {
    *reactivate_failure = trx_preserve_thd_transition_failure::NONE;
  }
  if (trx_preserve_reactivate_prepare_failure_in_original_thd(
          thd, reactivate_failure) != DB_SUCCESS)
    return true;

  reset_preserve_xid_to_active_transaction_xid(thd);
  reset_preserve_statement_transaction_scope(thd);
  return false;
}

void preserved_trx_begin_external_thd_teardown(THD *thd) {
  if (!preserve_trx_is_enabled() || thd == nullptr) return;
  std::unique_lock<std::mutex> lock(g_preserved_trx_thd_pin_mutex);
  g_preserved_trx_thd_teardown.insert(thd);
  g_preserved_trx_thd_pin_cond.wait(lock, [thd] {
    return g_preserved_trx_thd_pin_counts.find(thd) ==
           g_preserved_trx_thd_pin_counts.end();
  });
}

void preserved_trx_end_external_thd_teardown(THD *thd) {
  if (!preserve_trx_is_enabled() || thd == nullptr) return;
  {
    std::lock_guard<std::mutex> lock(g_preserved_trx_thd_pin_mutex);
    g_preserved_trx_thd_teardown.erase(thd);
  }
  g_preserved_trx_thd_pin_cond.notify_all();
}

void preserved_trx_wait_for_external_thd_use(THD *thd) {
  preserved_trx_begin_external_thd_teardown(thd);
  preserved_trx_end_external_thd_teardown(thd);
}

bool preserved_trx_thd_has_external_use(THD *thd) {
  if (thd == nullptr) return false;
  std::lock_guard<std::mutex> lock(g_preserved_trx_thd_pin_mutex);
  return g_preserved_trx_thd_pin_counts.find(thd) !=
         g_preserved_trx_thd_pin_counts.end();
}

enum class Preserved_trx_recover_or_adopt_policy {
  LOCAL_STARTUP_RECOVERY,
  STANDBY_PROMOTION_ADOPT,
  STANDBY_PROMOTION_PHYSICAL_FENCE
};

struct Preserved_trx_recover_or_adopt_phase_metrics {
  uint64_t claim_us{0};
  uint64_t read_view_us{0};
  uint64_t table_locks_us{0};
  uint64_t record_locks_us{0};
  trx_preserve_record_lock_import_metrics_t record_lock_import;
  uint64_t predicate_locks_us{0};
  uint64_t mdl_us{0};
  uint64_t register_us{0};
};

struct Preserved_trx_recover_or_adopt_result {
  bool claimed{false};
  bool rolled_back{false};
  bool cleanup_error{false};
  bool exact_trx_missing{false};
  bool provider_contract_violation{false};
  bool record_lock_conflict{false};
  Preserved_trx_recover_or_adopt_phase_metrics phase_metrics;
  std::string reason;
};

struct Preserved_trx_simulated_publication_context {
  const Preserve_trx_targeted_publication_capability *capability{nullptr};
  const Preserve_trx_prepared_token_key *key{nullptr};
  trx_preserve_targeted_publication_journal *journal{nullptr};
};

struct Preserved_trx_recover_or_adopt_options {
  Preserved_trx_recover_or_adopt_policy policy{
      Preserved_trx_recover_or_adopt_policy::LOCAL_STARTUP_RECOVERY};
  uint64_t deadline_us{0};
  uint64_t deadline_monotonic_us{0};
  bool record_lock_pages_prewarmed{false};
  const lock_preserve_metadata_plan_t *record_lock_plan{nullptr};
  Preserve_trx_physical_fence_lease *physical_fence_lease{nullptr};
  trx_t *exact_trx{nullptr};
  const Preserve_trx_prepared_token_key *promotion_key{nullptr};
  const Preserved_trx_simulated_publication_context *simulated_publication{
      nullptr};
};

bool preserved_trx_snapshot_allows_synthetic_temp_claim(
    const Preserve_snapshot_metadata &metadata) {
  return !metadata.temp_table_manifest_payload.empty() &&
         metadata.engine_shape == Preserve_snapshot_engine_shape::TEMP_ONLY &&
         !metadata.has_persistent_engine_state &&
         metadata.has_temp_engine_state &&
         !metadata.has_logged_persistent_work;
}

bool preserved_trx_binlog_format_is_supported(ulong binlog_format) {
  return binlog_format == BINLOG_FORMAT_ROW;
}

static bool recover_or_adopt_deadline_expired(
    const Preserved_trx_recover_or_adopt_options &options) {
  return options.deadline_us != 0 &&
         my_micro_time() >= options.deadline_us;
}

static bool recover_or_adopt_deadline_expired_callback(void *context) {
  if (context == nullptr) return false;
  return recover_or_adopt_deadline_expired(
      *static_cast<const Preserved_trx_recover_or_adopt_options *>(context));
}

using Preserved_trx_after_claim_hook = bool (*)(
    const std::string &token, trx_t *trx, Preserve_snapshot_metadata *metadata,
    void *context, std::string *reason);

static bool preserved_trx_recover_or_adopt_bundle_shared(
    const std::string &dir, Preserved_trx_bundle *bundle,
    const Preserved_trx_recover_or_adopt_options &options,
    Preserved_trx_after_claim_hook after_claim_hook, void *after_claim_context,
    Preserved_trx_recover_or_adopt_result *result) {
  if (result == nullptr || bundle == nullptr) return false;
  *result = {};
  Preserve_snapshot_metadata &metadata = bundle->metadata;
  const std::string token = metadata.token;
  const bool local_startup =
      options.policy ==
      Preserved_trx_recover_or_adopt_policy::LOCAL_STARTUP_RECOVERY;
  const bool strict_physical =
      options.policy == Preserved_trx_recover_or_adopt_policy::
                            STANDBY_PROMOTION_PHYSICAL_FENCE;
  const bool simulated_publication = options.simulated_publication != nullptr;

  auto fail_before_claim = [&](const std::string &reason) {
    result->reason = reason;
    return false;
  };
  if (dir.empty() || token.empty()) {
    return fail_before_claim(local_startup ? "invalid durable transaction "
                                             "snapshot"
                                           : "invalid promotion ready record");
  }
  if (simulated_publication &&
      (!strict_physical || options.exact_trx == nullptr ||
       options.simulated_publication->capability == nullptr ||
       options.simulated_publication->key == nullptr ||
       options.simulated_publication->journal == nullptr ||
       !options.simulated_publication->journal->active ||
       options.simulated_publication->journal->trx != options.exact_trx ||
       !options.simulated_publication->capability->valid_for(
           *options.simulated_publication->key))) {
    return fail_before_claim("invalid simulated targeted publication inputs");
  }
  if (strict_physical &&
      (options.exact_trx == nullptr ||
       options.promotion_key == nullptr ||
       options.promotion_key->token != token ||
       (options.physical_fence_lease != nullptr &&
        !options.physical_fence_lease->acquired()) ||
       !metadata.predicate_locks_payload.empty() ||
       (!metadata.record_locks_payload.empty() &&
        (options.record_lock_plan == nullptr ||
         !options.record_lock_plan->ready())))) {
    return fail_before_claim(
        "invalid metadata-only physical promotion inputs");
  }
  if (!recoverable_binlog_state(metadata.binlog_state)) {
    return fail_before_claim("unsupported durable transaction binlog state");
  }
  if (!binlog_state_matches_configured_mode(metadata)) {
    log_preserved_trx_rejected_binlog_mode(token, metadata);
    return fail_before_claim("binlog mode mismatch");
  }
  if (recover_or_adopt_deadline_expired(options)) {
    return fail_before_claim(local_startup ? "durable transaction recovery "
                                             "deadline expired"
                                           : "promotion gate deadline "
                                             "expired before claim");
  }

  auto revalidate_physical_fence = [&]() {
    if (!strict_physical || options.physical_fence_lease == nullptr) {
      return Preserve_trx_physical_fence_status::OK;
    }
    return options.physical_fence_lease->revalidate();
  };
  Preserve_trx_physical_fence_status fence_status =
      revalidate_physical_fence();
  if (fence_status != Preserve_trx_physical_fence_status::OK) {
    result->provider_contract_violation =
        fence_status == Preserve_trx_physical_fence_status::
                            PROVIDER_CONTRACT_VIOLATION;
    return fail_before_claim(result->provider_contract_violation
                                 ? "physical-fence provider contract violated"
                                 : "physical-fence revalidation failed");
  }

  XID xid;
  if (preserve_trx_token_to_xid(token, &xid)) {
    return fail_before_claim(local_startup
                                 ? "failed to map durable transaction token "
                                   "to XID"
                                 : "failed to map promotion standby token "
                                   "to XID");
  }

  auto add_kernel_elapsed_us = [](uint64_t started_us, uint64_t *out) {
    if (out == nullptr) return;
    const uint64_t now_us = preserve_trx_monotonic_us();
    *out += now_us >= started_us ? now_us - started_us : 0;
  };

  uint64_t phase_started_us = preserve_trx_monotonic_us();
  trx_t *trx = nullptr;
  if (options.exact_trx != nullptr) {
    if (trx_preserve_validate_reserved_exact(options.exact_trx, xid) ||
        trx_preserve_claim_detached_active_undo_exact(options.exact_trx, xid) ==
            DB_SUCCESS) {
      trx = options.exact_trx;
    }
  }
  if (trx == nullptr && options.exact_trx == nullptr &&
      preserved_trx_snapshot_allows_synthetic_temp_claim(metadata)) {
    const uint64_t temp_owner_trx_id =
        preserve_trx_temp_table_owner_trx_id(metadata);
    if (temp_owner_trx_id != 0) {
      trx = trx_preserve_create_temp_only_claimed(xid, temp_owner_trx_id);
    }
  }
  add_kernel_elapsed_us(phase_started_us, &result->phase_metrics.claim_us);
  if (trx == nullptr) {
    result->exact_trx_missing = true;
    return fail_before_claim(local_startup
                                 ? "exact reserved trx not found"
                                 : "exact reserved trx not found for promotion "
                                   "adopt");
  }
  result->claimed = true;
  if (simulated_publication) {
    options.simulated_publication->journal->claimed = true;
  }

  lock_preserve_import_journal_t strict_lock_journal;

  auto rollback_after_claim = [&](const std::string &reason) {
    result->reason = reason;
    delete_detached_mdl_context(token);
    if (strict_physical && strict_lock_journal.size() != 0 &&
        lock_preserve_unwind_record_lock_metadata_import(
            trx, &strict_lock_journal) != DB_SUCCESS) {
      result->cleanup_error = true;
      result->reason += "; metadata lock unwind failed";
    }
    if (simulated_publication) {
      const auto unclaim_status = trx_preserve_unclaim_simulated_active_undo(
          *options.simulated_publication->capability,
          *options.simulated_publication->key,
          options.simulated_publication->journal);
      const auto unpublish_status =
          unclaim_status == trx_preserve_targeted_publication_status::OK
              ? trx_preserve_unpublish_simulated_active_undo(
                    *options.simulated_publication->capability,
                    *options.simulated_publication->key,
                    options.simulated_publication->journal)
              : trx_preserve_targeted_publication_status::INVALID_STATE;
      if (unclaim_status == trx_preserve_targeted_publication_status::OK &&
          unpublish_status == trx_preserve_targeted_publication_status::OK) {
        result->rolled_back = true;
      } else {
        result->rolled_back = false;
        result->cleanup_error = true;
        result->reason += "; simulated publication reversal failed";
      }
    } else if (local_startup) {
      const bool cleanup_error = rollback_claimed_preserved_snapshot_or_log(
          dir, token, trx, reason, &metadata);
      result->cleanup_error = cleanup_error;
      result->rolled_back = !cleanup_error;
    } else if (trx_preserve_rollback_claimed(trx) == DB_SUCCESS) {
      result->rolled_back = true;
    } else {
      result->rolled_back = false;
      result->cleanup_error = true;
      result->reason = reason + "; rollback failed";
    }
    return false;
  };

  auto taint_after_claim = [&](const std::string &reason) {
    result->reason = reason;
    result->cleanup_error = true;
    result->provider_contract_violation = true;
    return false;
  };

  auto revalidate_after_claim = [&](const char *stage) {
    fence_status = revalidate_physical_fence();
    if (fence_status == Preserve_trx_physical_fence_status::OK) return true;
    const std::string reason =
        std::string("physical-fence revalidation failed ") + stage;
    if (fence_status == Preserve_trx_physical_fence_status::
                            PROVIDER_CONTRACT_VIOLATION &&
        !simulated_publication) {
      taint_after_claim(reason + "; provider contract violated");
    } else {
      rollback_after_claim(reason);
    }
    return false;
  };

  if (!revalidate_after_claim("after claim")) return false;

  if (recover_or_adopt_deadline_expired(options)) {
    return rollback_after_claim(local_startup ? "durable transaction recovery "
                                               "deadline expired"
                                             : "promotion gate deadline "
                                               "expired after claim");
  }

  if (after_claim_hook != nullptr) {
    std::string after_claim_reason;
    if (after_claim_hook(token, trx, &metadata, after_claim_context,
                         &after_claim_reason)) {
      return rollback_after_claim(
          after_claim_reason.empty()
              ? "failed to update durable transaction recovery count"
              : after_claim_reason);
    }
  }
  if (recover_or_adopt_deadline_expired(options)) {
    return rollback_after_claim(local_startup ? "durable transaction recovery "
                                               "deadline expired"
                                             : "promotion gate deadline "
                                               "expired after claim hook");
  }

  const auto rollback_semantics_failure = [&](const char *component) {
    std::string reason =
        std::string(local_startup
                        ? "failed to restore durable transaction semantics: "
                        : "failed to restore promotion transaction semantics: ") +
        component;
    if (strcmp(component, "record locks") == 0) {
      const char *detail = trx_preserve_last_record_lock_export_error();
      if (detail != nullptr && detail[0] != '\0') {
        if (!local_startup &&
            strcmp(detail, "record_lock_import_deadline_expired") == 0) {
          return rollback_after_claim(
              "promotion gate deadline expired during record-lock import");
        }
        reason.append(": ");
        reason.append(detail);
      }
    }
    return rollback_after_claim(reason);
  };

  if (trx_preserve_set_isolation(trx, metadata.tx_isolation) != DB_SUCCESS) {
    return rollback_semantics_failure("isolation level");
  }
  if (recover_or_adopt_deadline_expired(options)) {
    return rollback_after_claim(local_startup ? "durable transaction recovery "
                                               "deadline expired"
                                             : "promotion gate deadline "
                                               "expired during semantic import");
  }
  phase_started_us = preserve_trx_monotonic_us();
  dberr_t semantic_err =
      trx_preserve_import_read_view(trx, metadata.read_view_payload);
  add_kernel_elapsed_us(phase_started_us,
                        &result->phase_metrics.read_view_us);
  if (semantic_err != DB_SUCCESS) {
    return rollback_semantics_failure("read view");
  }
  if (recover_or_adopt_deadline_expired(options)) {
    return rollback_after_claim(local_startup ? "durable transaction recovery "
                                               "deadline expired"
                                             : "promotion gate deadline "
                                               "expired during semantic import");
  }
  phase_started_us = preserve_trx_monotonic_us();
  semantic_err =
      trx_preserve_import_table_locks(trx, metadata.table_locks_payload);
  add_kernel_elapsed_us(phase_started_us,
                        &result->phase_metrics.table_locks_us);
  if (semantic_err != DB_SUCCESS) {
    return rollback_semantics_failure("table locks");
  }
  if (recover_or_adopt_deadline_expired(options)) {
    return rollback_after_claim(local_startup ? "durable transaction recovery "
                                               "deadline expired"
                                             : "promotion gate deadline "
                                               "expired during semantic import");
  }
  phase_started_us = preserve_trx_monotonic_us();
  if (local_startup && preserve_trx_recover_lock_page_prefetch &&
      !options.record_lock_pages_prewarmed) {
    semantic_err = trx_preserve_prefetch_record_lock_pages(
        metadata.record_locks_payload, &result->phase_metrics.record_lock_import);
    if (semantic_err != DB_SUCCESS) {
      add_kernel_elapsed_us(phase_started_us,
                            &result->phase_metrics.record_locks_us);
      return rollback_semantics_failure("record locks");
    }
  }
  if (strict_physical) {
    if (options.record_lock_plan != nullptr) {
      const auto conflict =
          lock_preserve_check_record_bitmap_conflicts_from_metadata(
              trx, *options.record_lock_plan,
              options.deadline_monotonic_us);
      if (conflict != lock_preserve_metadata_conflict_result::OK) {
        result->record_lock_conflict =
            conflict == lock_preserve_metadata_conflict_result::CONFLICT;
        add_kernel_elapsed_us(phase_started_us,
                              &result->phase_metrics.record_locks_us);
        return rollback_after_claim(
            result->record_lock_conflict
                ? "metadata-only record-lock conflict"
                : "metadata-only record-lock preflight failed");
      }
      semantic_err = lock_preserve_apply_record_lock_metadata_plan(
          trx, *options.record_lock_plan, options.deadline_monotonic_us,
          &strict_lock_journal);
      result->phase_metrics.record_lock_import.record_entries +=
          options.record_lock_plan->entry_count();
      result->phase_metrics.record_lock_import.bitmap_bits +=
          options.record_lock_plan->bitmap_bits();
    } else {
      semantic_err = metadata.record_locks_payload.empty() ? DB_SUCCESS
                                                           : DB_ERROR;
    }
  } else {
    semantic_err = trx_preserve_import_record_locks(
        trx, metadata.record_locks_payload,
        &result->phase_metrics.record_lock_import,
        local_startup ? nullptr : recover_or_adopt_deadline_expired_callback,
        local_startup ? nullptr
                      : const_cast<Preserved_trx_recover_or_adopt_options *>(
                            &options));
  }
  add_kernel_elapsed_us(phase_started_us,
                        &result->phase_metrics.record_locks_us);
  if (semantic_err != DB_SUCCESS) {
    return rollback_semantics_failure("record locks");
  }
  if (!revalidate_after_claim("after record-lock import")) return false;
  if (recover_or_adopt_deadline_expired(options)) {
    return rollback_after_claim(local_startup ? "durable transaction recovery "
                                               "deadline expired"
                                             : "promotion gate deadline "
                                               "expired during semantic import");
  }
  if (!strict_physical) {
    phase_started_us = preserve_trx_monotonic_us();
    semantic_err = trx_preserve_import_record_locks(
        trx, metadata.predicate_locks_payload);
    add_kernel_elapsed_us(phase_started_us,
                          &result->phase_metrics.predicate_locks_us);
    if (semantic_err != DB_SUCCESS) {
      return rollback_semantics_failure("predicate locks");
    }
  }
  if (recover_or_adopt_deadline_expired(options)) {
    return rollback_after_claim(local_startup ? "durable transaction recovery "
                                               "deadline expired"
                                             : "promotion gate deadline "
                                               "expired before MDL restore");
  }
  phase_started_us = preserve_trx_monotonic_us();
  const bool mdl_err = create_detached_mdl_context(metadata);
  add_kernel_elapsed_us(phase_started_us, &result->phase_metrics.mdl_us);
  if (mdl_err) {
    return rollback_after_claim(
        local_startup ? "failed to restore durable transaction MDL context"
                      : "failed to restore promotion transaction MDL context");
  }
  if (!revalidate_after_claim("before record registration")) return false;
  if (recover_or_adopt_deadline_expired(options)) {
    return rollback_after_claim(local_startup ? "durable transaction recovery "
                                               "deadline expired"
                                             : "promotion gate deadline "
                                               "expired before record "
                                               "registration");
  }
  phase_started_us = preserve_trx_monotonic_us();
  const bool add_record_err = preserved_trx_add_record(
          metadata, trx, local_startup,
          local_startup ? Preserved_trx_lifecycle_state::PRESERVED
                        : Preserved_trx_lifecycle_state::ADOPTED_FOR_PROMOTION,
          std::move(bundle->blob_descriptors), options.promotion_key);
  add_kernel_elapsed_us(phase_started_us, &result->phase_metrics.register_us);
  if (add_record_err) {
    return rollback_after_claim(
        local_startup ? "failed to register recovered durable transaction"
                      : "failed to register promotion adopted transaction");
  }

  audit_preserved_trx_event(
      current_thd, token, local_startup ? "recover" : "promotion-adopt",
      "success");
  return true;
}

struct Startup_snapshot_recover_phase_metrics {
  uint64_t load_us{0};
  uint64_t validate_us{0};
  uint64_t kernel_us{0};
  Preserved_trx_recover_or_adopt_phase_metrics kernel_breakdown;
};

struct Startup_snapshot_recover_task {
  std::string token;
  uint32_t recovery_attempt_count{0};
  bool record_lock_pages_prewarmed{false};
  bool processed{false};
  bool error{false};
  Startup_snapshot_recover_phase_metrics phase_metrics;
};

static void add_elapsed_us(uint64_t started_us, uint64_t *out) {
  if (out == nullptr) return;
  const uint64_t now_us = preserve_trx_monotonic_us();
  *out += now_us >= started_us ? now_us - started_us : 0;
}

static void add_recover_or_adopt_phase_metrics(
    const Preserved_trx_recover_or_adopt_phase_metrics &source,
    Preserved_trx_recover_or_adopt_phase_metrics *target) {
  if (target == nullptr) return;
  target->claim_us += source.claim_us;
  target->read_view_us += source.read_view_us;
  target->table_locks_us += source.table_locks_us;
  target->record_locks_us += source.record_locks_us;
  target->record_lock_import.add(source.record_lock_import);
  target->predicate_locks_us += source.predicate_locks_us;
  target->mdl_us += source.mdl_us;
  target->register_us += source.register_us;
}

static bool recover_preserved_snapshot(const std::string &dir,
                                       const std::string &token,
                                       uint32_t recovery_attempt_count,
                                       bool record_lock_pages_prewarmed,
                                       uint64_t recovery_anchor_wall_us,
                                       uint64_t recovery_anchor_monotonic_us,
                                       Startup_snapshot_recover_phase_metrics
                                           *phase_metrics) {
  Preserved_trx_bundle bundle;
  /*
    Recovery reads only the semantic external blobs required to reconstruct
    transaction state. Large non-semantic bodies can remain descriptor-only
    until resume needs them.
  */
  uint64_t subphase_started_us = preserve_trx_monotonic_us();
  Preserve_snapshot_status status =
      preserved_trx_load_bundle_for_recover_or_prewarm(
          dir, token,
          Preserved_trx_recover_load_profile::WITH_SEMANTIC_EXTERNAL_BLOBS,
          &bundle);
  add_elapsed_us(subphase_started_us,
                 phase_metrics == nullptr ? nullptr : &phase_metrics->load_us);
  DBUG_EXECUTE_IF("preserve_trx_simulate_recover_snapshot_read_corrupt",
                  status = Preserve_snapshot_status::CORRUPT;);
  DBUG_EXECUTE_IF("preserve_trx_simulate_recover_snapshot_read_unsupported",
                  status = Preserve_snapshot_status::UNSUPPORTED;);

  if (status != Preserve_snapshot_status::OK) {
    const char *reason = preserved_trx_snapshot_read_failure_reason(status);
    if (preserved_trx_recovery_read_failure_requires_startup_abort(status)) {
      return log_preserved_trx_recovery_failure(token, reason);
    }
    return rollback_preserved_snapshot_or_log(dir, token, reason);
  }

  subphase_started_us = preserve_trx_monotonic_us();
  std::string dry_validate_reason;
  status = preserved_trx_dry_validate_loaded_bundle(dir, token, bundle,
                                                    &dry_validate_reason);
  if (status != Preserve_snapshot_status::OK) {
    add_elapsed_us(subphase_started_us,
                   phase_metrics == nullptr ? nullptr
                                            : &phase_metrics->validate_us);
    Preserve_snapshot_metadata cleanup_metadata = bundle.metadata;
    const std::string failure_reason =
        dry_validate_reason.empty()
            ? preserved_trx_snapshot_read_failure_reason(status)
            : dry_validate_reason;
    return rollback_preserved_snapshot_or_log(dir, token,
                                              failure_reason, &cleanup_metadata,
                                              Temp_sidecar_cleanup_mode::
                                                  RAW_UNLINK);
  }

  if (!binlog_state_matches_configured_mode(bundle.metadata)) {
    add_elapsed_us(subphase_started_us,
                   phase_metrics == nullptr ? nullptr
                                            : &phase_metrics->validate_us);
    Preserve_snapshot_metadata cleanup_metadata = bundle.metadata;
    log_preserved_trx_rejected_binlog_mode(token, cleanup_metadata);
    return rollback_preserved_snapshot_or_log(
        dir, token, "binlog mode mismatch", &cleanup_metadata,
        Temp_sidecar_cleanup_mode::RAW_UNLINK);
  }

  Preserve_snapshot_metadata metadata = std::move(bundle.metadata);

  if (recovery_attempt_count == 0) {
    add_elapsed_us(subphase_started_us,
                   phase_metrics == nullptr ? nullptr
                                            : &phase_metrics->validate_us);
    return log_preserved_trx_recovery_failure(
        token, "missing durable transaction recovery attempt ledger entry");
  }
  if (metadata.recovered_count >
      std::numeric_limits<uint32_t>::max() - recovery_attempt_count) {
    add_elapsed_us(subphase_started_us,
                   phase_metrics == nullptr ? nullptr
                                            : &phase_metrics->validate_us);
    return rollback_preserved_snapshot_or_log(
        dir, token, "durable transaction recovery count overflow", &metadata,
        Temp_sidecar_cleanup_mode::RAW_UNLINK);
  }
  /*
    Evaluate the timeout before bumping recovered_count. The first recovery
    pass is the only one that receives preserve_trx_recovery_grace_seconds;
    later passes must use the original wall-clock expiry exactly as stored.
  */
  metadata.recovered_count += recovery_attempt_count - 1;
  const bool recovery_deadline_expired =
      preserve_trx_recovery_deadline_expired(metadata, recovery_anchor_wall_us,
                                             recovery_anchor_monotonic_us);
  ++metadata.recovered_count;
  if (metadata.recovered_count >= preserve_trx_recovery_max_count) {
    add_elapsed_us(subphase_started_us,
                   phase_metrics == nullptr ? nullptr
                                            : &phase_metrics->validate_us);
    preserved_trx_add_failed_observable_record(
        metadata, "recovery max count exceeded");
    return rollback_preserved_snapshot_or_log(dir, token,
                                              "recovery max count exceeded",
                                              &metadata,
                                              Temp_sidecar_cleanup_mode::
                                                  RAW_UNLINK);
  }
  if (recovery_deadline_expired) {
    add_elapsed_us(subphase_started_us,
                   phase_metrics == nullptr ? nullptr
                                            : &phase_metrics->validate_us);
    preserved_trx_add_failed_observable_record(metadata,
                                               "recovery timeout expired");
    return rollback_preserved_snapshot_or_log(dir, token,
                                              "recovery timeout expired",
                                              &metadata,
                                              Temp_sidecar_cleanup_mode::
                                                  RAW_UNLINK);
  }

  auto store = create_preserved_trx_default_store(dir);
  [[maybe_unused]] auto fail_closed_without_claim = [&](const char *reason) {
    if (store->mark_tainted(token, reason) != Preserve_snapshot_status::OK) {
      return log_preserved_trx_recovery_failure(
          token, "failed to mark snapshot tainted after recovery failure");
    }
    preserved_trx_add_failed_observable_record(metadata, reason);
    (void)log_preserved_trx_recovery_failure(token, reason);
    return false;
  };

  DBUG_EXECUTE_IF("preserve_trx_simulate_encrypted_tablespace_key_unavailable",
                  add_elapsed_us(subphase_started_us,
                                 phase_metrics == nullptr
                                     ? nullptr
                                     : &phase_metrics->validate_us);
                  return fail_closed_without_claim(
                      "encrypted tablespace key unavailable"););

  DBUG_EXECUTE_IF(
      "preserve_trx_crash_after_recover_import_before_register", {
        LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
               "Preserved transaction recovery reached post-import "
               "pre-register crash point");
        DBUG_SUICIDE();
      });

  Preserve_snapshot_metadata cleanup_metadata = metadata;
  bundle.metadata = std::move(metadata);
  Preserved_trx_recover_or_adopt_result kernel_result;
  Preserved_trx_recover_or_adopt_options recover_options;
  recover_options.policy =
      Preserved_trx_recover_or_adopt_policy::LOCAL_STARTUP_RECOVERY;
  recover_options.record_lock_pages_prewarmed = record_lock_pages_prewarmed;
  recover_options.exact_trx =
      trx_preserve_startup_resurrection_find_verified(token);
  add_elapsed_us(subphase_started_us,
                 phase_metrics == nullptr ? nullptr
                                          : &phase_metrics->validate_us);
  subphase_started_us = preserve_trx_monotonic_us();
  const bool recovered = preserved_trx_recover_or_adopt_bundle_shared(
      dir, &bundle, recover_options, nullptr, nullptr,
      &kernel_result);
  add_elapsed_us(subphase_started_us,
                 phase_metrics == nullptr ? nullptr : &phase_metrics->kernel_us);
  add_recover_or_adopt_phase_metrics(
      kernel_result.phase_metrics,
      phase_metrics == nullptr ? nullptr : &phase_metrics->kernel_breakdown);
  if (recovered) {
    return false;
  }
  if (kernel_result.exact_trx_missing) {
    return delete_preserved_snapshot_files_and_sidecars_or_log(
        dir, token, &cleanup_metadata, Temp_sidecar_cleanup_mode::RAW_UNLINK);
  }
  if (kernel_result.claimed) return kernel_result.cleanup_error;
  return log_preserved_trx_recovery_failure(token, kernel_result.reason);
}

static bool startup_record_lock_pages_prewarmed(uint64_t record_lock_page_count,
                                                uint64_t prefetch_submitted_pages,
                                                uint64_t resident_pages) {
  if (record_lock_page_count == 0) return true;
  return prefetch_submitted_pages == record_lock_page_count &&
         resident_pages == record_lock_page_count;
}

bool preserved_trx_startup_record_lock_pages_prewarmed_for_unit_test(
    uint64_t record_lock_page_count, uint64_t prefetch_submitted_pages,
    uint64_t resident_pages) {
  return startup_record_lock_pages_prewarmed(
      record_lock_page_count, prefetch_submitted_pages, resident_pages);
}

static void prewarm_record_lock_pages_for_startup_recovery(
    const std::string &dir, std::vector<Startup_snapshot_recover_task> *tasks,
    trx_preserve_record_lock_import_metrics_t *aggregate_metrics) {
  if (tasks == nullptr || tasks->empty()) return;

  for (Startup_snapshot_recover_task &task : *tasks) {
    Preserved_trx_bundle bundle;
    Preserve_snapshot_status status =
        preserved_trx_load_bundle_for_recover_or_prewarm(
            dir, task.token,
            Preserved_trx_recover_load_profile::WITH_SEMANTIC_EXTERNAL_BLOBS,
            &bundle);
    if (status != Preserve_snapshot_status::OK) continue;

    std::string dry_validate_reason;
    status = preserved_trx_dry_validate_loaded_bundle(dir, task.token, bundle,
                                                      &dry_validate_reason);
    if (status != Preserve_snapshot_status::OK) continue;

    if (bundle.metadata.record_locks_payload.empty()) {
      task.record_lock_pages_prewarmed = true;
      continue;
    }

    trx_preserve_record_lock_import_metrics_t prewarm_metrics;
    if (trx_preserve_prefetch_record_lock_pages(
            bundle.metadata.record_locks_payload, &prewarm_metrics) !=
        DB_SUCCESS) {
      continue;
    }
    trx_preserve_record_lock_residency_t residency;
    if (trx_preserve_record_lock_payload_residency(
            bundle.metadata.record_locks_payload, &residency)) {
      prewarm_metrics.prefetch_residency_pages += residency.page_count;
      prewarm_metrics.prefetch_resident_pages += residency.resident_pages;
      prewarm_metrics.prefetch_io_pending_pages += residency.io_pending_pages;
      prewarm_metrics.prefetch_missing_pages += residency.missing_pages;
      task.record_lock_pages_prewarmed = startup_record_lock_pages_prewarmed(
          residency.page_count, prewarm_metrics.prefetch_pages,
          residency.resident_pages);
    }
    if (aggregate_metrics != nullptr) {
      aggregate_metrics->add(prewarm_metrics);
    }
  }
}

bool preserved_trx_import_reserved_bundle_for_promotion(
    const std::string &dir, Preserved_trx_bundle bundle,
    Preserved_trx_promotion_ready_adopt_result *result,
    uint64_t deadline_us) {
  if (result == nullptr) return false;
  *result = {};

  Preserved_trx_recover_or_adopt_result kernel_result;
  Preserved_trx_recover_or_adopt_options adopt_options;
  adopt_options.policy =
      Preserved_trx_recover_or_adopt_policy::STANDBY_PROMOTION_ADOPT;
  adopt_options.deadline_us = deadline_us;
  const bool adopted = preserved_trx_recover_or_adopt_bundle_shared(
      dir, &bundle, adopt_options, nullptr, nullptr, &kernel_result);
  result->claimed = kernel_result.claimed;
  result->rolled_back = kernel_result.rolled_back;
  result->reason = kernel_result.reason;
  return adopted;
}

Preserved_trx_physical_adopt_status
preserved_trx_import_reserved_for_physical_promotion(
    const std::string &dir, Preserve_trx_gate_adopt_lease *adopt_lease,
    trx_t *exact_trx,
    Preserve_trx_physical_fence_lease *physical_lease,
    uint64_t operation_deadline_us,
    Preserved_trx_physical_adopt_result *result) {
  if (result == nullptr) {
    return Preserved_trx_physical_adopt_status::INVALID_ARGUMENT;
  }
  *result = {};
  if (dir.empty() || adopt_lease == nullptr || !adopt_lease->active() ||
      (physical_lease != nullptr && !physical_lease->acquired()) ||
      operation_deadline_us <= my_micro_time()) {
    result->reason = "strict promotion requires an active registry lease";
    return result->status;
  }

  const lock_preserve_metadata_plan_t *record_lock_plan =
      adopt_lease->record_lock_plan();
  const bool simulated_fence =
      physical_lease != nullptr &&
      physical_lease->proof().consistency_mode ==
      Preserve_trx_physical_consistency_mode::
          TEST_ONLY_PHYSICAL_FENCE_SIMULATOR;
  if (physical_lease != nullptr && !simulated_fence) {
    result->reason =
        "production promotion does not consume a Preserve fence provider";
    return result->status;
  }
  if (!simulated_fence && exact_trx == nullptr) {
    result->reason =
        "production promotion requires an exact verified ACTIVE-Undo trx";
    return result->status;
  }
  if (simulated_fence) {
    const Preserve_trx_physical_fence_status fence_status =
        physical_lease->revalidate();
    if (fence_status != Preserve_trx_physical_fence_status::OK) {
      result->provider_contract_violation =
          fence_status == Preserve_trx_physical_fence_status::
                              PROVIDER_CONTRACT_VIOLATION;
      result->status = result->provider_contract_violation
                           ? Preserved_trx_physical_adopt_status::
                                 PHYSICAL_FENCE_PROVIDER_VIOLATION
                           : Preserved_trx_physical_adopt_status::
                                 PHYSICAL_FENCE_REVALIDATE_FAILED;
      result->reason =
          "simulated physical fence revalidation failed before targeted "
          "publication";
      return result->status;
    }
  }
  Preserve_trx_prepared_token_key publication_key;
  Preserve_trx_final_token_facts publication_facts;
  if (adopt_lease->copy_publication(&publication_key, &publication_facts) !=
          Preserve_trx_prepared_status::OK ||
      publication_key.preserve_dir != dir) {
    result->reason = "strict promotion publication facts are unavailable";
    return result->status;
  }
  const Preserve_trx_resurrection_index_entry *resurrection_entry = nullptr;
  if (simulated_fence) {
    resurrection_entry = adopt_lease->resurrection_entry();
    if (resurrection_entry == nullptr) {
      result->reason =
          "simulated promotion requires authenticated publication facts";
      return result->status;
    }
  }
  std::unique_ptr<Preserved_trx_bundle> bundle;
  if (adopt_lease->take_semantic_bundle(&bundle) !=
          Preserve_trx_prepared_status::OK ||
      bundle == nullptr) {
    result->reason = "strict promotion semantic bundle is unavailable";
    return result->status;
  }

  Preserve_trx_targeted_publication_capability publication_capability;
  trx_preserve_targeted_publication_journal *publication_journal = nullptr;
  if (simulated_fence) {
    trx_preserve_resurrection_facts engine_facts;
    XID expected_xid;
    auto journal =
        std::make_unique<trx_preserve_targeted_publication_journal>();
    if (resurrection_entry->authority_token != publication_key.token ||
        bundle->metadata.token != publication_key.token ||
        preserve_trx_resurrection_entry_to_engine_facts(*resurrection_entry,
                                                        &engine_facts) ||
        preserve_trx_token_to_xid(publication_key.token, &expected_xid) ||
        engine_facts.authority_token != publication_key.token ||
        publication_facts.source_safe_next_trx_id_floor == 0 ||
        !physical_lease->make_targeted_publication_capability(
            publication_key, &publication_capability)) {
      result->reason = "simulated targeted publication facts are invalid";
      if (adopt_lease->restore_semantic_bundle(&bundle) !=
          Preserve_trx_prepared_status::OK) {
        result->status = Preserved_trx_physical_adopt_status::CLEANUP_TAINTED;
        result->reason += "; semantic bundle ownership restore failed";
      }
      return result->status;
    }
    trx_preserve_targeted_publication_candidate candidate;
    candidate.xid = expected_xid;
    candidate.trx_id = engine_facts.trx_id;
    candidate.freeze_lsn = engine_facts.freeze_lsn;
    candidate.safe_next_trx_id_floor =
        publication_facts.source_safe_next_trx_id_floor;
    const auto publish_status = trx_preserve_publish_simulated_active_undo(
        publication_capability, publication_key, candidate, journal.get());
    if (publish_status != trx_preserve_targeted_publication_status::OK ||
        journal->origin !=
            trx_preserve_targeted_publication_origin::NEWLY_PUBLISHED) {
      result->status =
          publish_status == trx_preserve_targeted_publication_status::OK
              ? Preserved_trx_physical_adopt_status::CLEANUP_TAINTED
              : Preserved_trx_physical_adopt_status::EXACT_TRX_NOT_FOUND;
      result->reason =
          publish_status == trx_preserve_targeted_publication_status::OK
              ? "simulated targeted publication already exists without the "
                "owning journal"
              : "simulated targeted publication failed";
      if (adopt_lease->restore_semantic_bundle(&bundle) !=
          Preserve_trx_prepared_status::OK) {
        result->status = Preserved_trx_physical_adopt_status::CLEANUP_TAINTED;
        result->reason += "; semantic bundle ownership restore failed";
      }
      return result->status;
    }
    publication_journal = journal.get();
    if (adopt_lease->install_targeted_publication_journal(
            std::move(journal)) != Preserve_trx_prepared_status::OK) {
      const auto reverse_status = trx_preserve_unpublish_simulated_active_undo(
          publication_capability, publication_key, publication_journal);
      result->status =
          reverse_status == trx_preserve_targeted_publication_status::OK
              ? Preserved_trx_physical_adopt_status::INVALID_ARGUMENT
              : Preserved_trx_physical_adopt_status::CLEANUP_TAINTED;
      result->reason = "failed to install simulated publication journal";
      if (adopt_lease->restore_semantic_bundle(&bundle) !=
          Preserve_trx_prepared_status::OK) {
        result->status = Preserved_trx_physical_adopt_status::CLEANUP_TAINTED;
        result->reason += "; semantic bundle ownership restore failed";
      }
      return result->status;
    }
  }

  Preserved_trx_recover_or_adopt_options options;
  options.policy = Preserved_trx_recover_or_adopt_policy::
      STANDBY_PROMOTION_PHYSICAL_FENCE;
  options.deadline_us = operation_deadline_us;
  const uint64_t deadline_anchor_wall_us = my_micro_time();
  const uint64_t deadline_anchor_monotonic_us = preserve_trx_monotonic_us();
  options.deadline_monotonic_us = preserve_trx_wall_deadline_to_monotonic(
      operation_deadline_us, deadline_anchor_wall_us,
      deadline_anchor_monotonic_us);
  options.record_lock_plan = record_lock_plan;
  options.physical_fence_lease = physical_lease;
  options.exact_trx = exact_trx;
  options.promotion_key = &publication_key;
  Preserved_trx_simulated_publication_context simulated_publication;
  if (simulated_fence) {
    simulated_publication.capability = &publication_capability;
    simulated_publication.key = &publication_key;
    simulated_publication.journal = publication_journal;
    options.exact_trx = publication_journal->trx;
    options.simulated_publication = &simulated_publication;
  }
  Preserved_trx_recover_or_adopt_result kernel_result;
  const bool adopted = preserved_trx_recover_or_adopt_bundle_shared(
      dir, bundle.get(), options, nullptr, nullptr, &kernel_result);

  if (!adopted && simulated_fence && publication_journal->active &&
      !publication_journal->claimed) {
    const auto reverse_status = trx_preserve_unpublish_simulated_active_undo(
        publication_capability, publication_key, publication_journal);
    if (reverse_status != trx_preserve_targeted_publication_status::OK) {
      kernel_result.cleanup_error = true;
      kernel_result.reason += "; unclaimed simulated publication reversal "
                              "failed";
    }
  }

  result->claimed = kernel_result.claimed;
  result->rolled_back = kernel_result.rolled_back;
  result->provider_contract_violation =
      kernel_result.provider_contract_violation;
  result->record_lock_apply_us =
      kernel_result.phase_metrics.record_locks_us;
  result->record_lock_entries =
      kernel_result.phase_metrics.record_lock_import.record_entries;
  result->record_lock_bits =
      kernel_result.phase_metrics.record_lock_import.bitmap_bits;
  result->reason = kernel_result.reason;

  if ((!kernel_result.claimed || kernel_result.provider_contract_violation ||
       kernel_result.cleanup_error) &&
      bundle != nullptr &&
      adopt_lease->restore_semantic_bundle(&bundle) !=
          Preserve_trx_prepared_status::OK) {
    result->status = Preserved_trx_physical_adopt_status::CLEANUP_TAINTED;
    result->reason += "; failed to restore strict semantic bundle ownership";
    return result->status;
  }

  preserved_trx_promotion_prepared_note_lock_metrics(
      0, 0, 0, result->record_lock_apply_us, result->record_lock_bits);
  if (adopted) {
    result->status = Preserved_trx_physical_adopt_status::OK;
  } else if (kernel_result.provider_contract_violation) {
    result->status = Preserved_trx_physical_adopt_status::
        PHYSICAL_FENCE_PROVIDER_VIOLATION;
  } else if (kernel_result.exact_trx_missing) {
    result->status =
        Preserved_trx_physical_adopt_status::EXACT_TRX_NOT_FOUND;
  } else if (kernel_result.record_lock_conflict) {
    result->status = Preserved_trx_physical_adopt_status::LOCK_CONFLICT;
  } else if (kernel_result.cleanup_error) {
    result->status = Preserved_trx_physical_adopt_status::CLEANUP_TAINTED;
  } else if (kernel_result.rolled_back) {
    result->status = Preserved_trx_physical_adopt_status::ROLLED_BACK;
  } else if (kernel_result.reason.find("physical-fence revalidation failed") !=
             std::string::npos) {
    result->status = Preserved_trx_physical_adopt_status::
        PHYSICAL_FENCE_REVALIDATE_FAILED;
  } else {
    result->status =
        Preserved_trx_physical_adopt_status::SEMANTIC_IMPORT_FAILED;
  }
  return result->status;
}

bool preserved_trx_reverse_simulated_promotion_adopt(
    const Preserve_trx_prepared_token_key &key,
    trx_t *, Preserve_trx_cleanup_lease *cleanup_lease,
    Preserve_trx_physical_fence_lease *physical_lease, std::string *reason) {
  auto fail = [&](const std::string &message) {
    if (reason != nullptr) *reason = message;
    return false;
  };
  if (key.token.empty() || cleanup_lease == nullptr ||
      !cleanup_lease->active() || physical_lease == nullptr ||
      !physical_lease->acquired() ||
      physical_lease->proof().consistency_mode !=
          Preserve_trx_physical_consistency_mode::
              TEST_ONLY_PHYSICAL_FENCE_SIMULATOR) {
    return fail("simulated adopt reversal requires active cleanup and fence "
                "leases");
  }

  Preserve_trx_targeted_publication_capability capability;
  trx_preserve_targeted_publication_journal *journal =
      cleanup_lease->targeted_publication_journal();
  if (journal == nullptr || !journal->active || !journal->claimed ||
      journal->origin !=
          trx_preserve_targeted_publication_origin::NEWLY_PUBLISHED ||
      journal->trx == nullptr ||
      !physical_lease->make_targeted_publication_capability(key,
                                                            &capability)) {
    return fail("simulated adopt reversal journal is unavailable");
  }

  Preserved_trx_record record;
  if (!preserved_trx_take_promotion_adopted_record(key, &record)) {
    return fail("simulated adopt reversal record is unavailable");
  }
  if (record.trx != journal->trx || record.metadata.token != key.token) {
    (void)restore_record_after_resume_failure(
        record, "simulated adopt reversal identity conflict");
    return fail("simulated adopt reversal identity conflict");
  }

  const auto unclaim_status = trx_preserve_unclaim_simulated_active_undo(
      capability, key, journal);
  if (unclaim_status != trx_preserve_targeted_publication_status::OK) {
    (void)restore_record_after_resume_failure(
        record, "simulated adopt reversal unclaim failed");
    return fail("simulated adopt reversal unclaim failed");
  }
  const auto unpublish_status = trx_preserve_unpublish_simulated_active_undo(
      capability, key, journal);
  if (unpublish_status != trx_preserve_targeted_publication_status::OK) {
    if (trx_preserve_claim_detached_active_undo_exact(record.trx, journal->xid) ==
        DB_SUCCESS) {
      journal->claimed = true;
      (void)restore_record_after_resume_failure(
          record, "simulated adopt reversal unpublish failed");
    }
    return fail("simulated adopt reversal unpublish failed");
  }

  delete_detached_mdl_context(key.token);
  audit_preserved_trx_event(current_thd, key.token,
                            "promotion-simulator-reverse", "success");
  if (reason != nullptr) reason->clear();
  return true;
}

bool preserved_trx_rollback_physical_promotion_adopt(
    const Preserve_trx_prepared_token_key &key, trx_t *exact_trx,
    Preserve_trx_cleanup_lease *cleanup_lease,
    Preserve_trx_physical_fence_lease *physical_lease, std::string *reason) {
  auto fail = [&](const std::string &message) {
    if (reason != nullptr) *reason = message;
    return false;
  };
  if (key.token.empty() || exact_trx == nullptr || cleanup_lease == nullptr ||
      !cleanup_lease->active() || physical_lease != nullptr) {
    return fail("production adopt rollback requires exact transaction and "
                "cleanup lease");
  }

  Preserved_trx_record record;
  if (!preserved_trx_take_promotion_adopted_record(key, &record)) {
    return fail("production adopt rollback record is unavailable");
  }
  if (record.trx != exact_trx || record.metadata.token != key.token) {
    (void)restore_record_after_resume_failure(
        record, "production adopt rollback identity conflict");
    return fail("production adopt rollback identity conflict");
  }
  if (trx_preserve_rollback_claimed(record.trx) != DB_SUCCESS) {
    (void)restore_record_after_resume_failure(
        record, "production adopt rollback failed");
    return fail("production adopt rollback failed");
  }

  delete_detached_mdl_context(key.token);
  audit_preserved_trx_event(current_thd, key.token,
                            "promotion-production-rollback", "success");
  if (reason != nullptr) reason->clear();
  return true;
}

bool preserved_trx_preflight_recoverability() {
  if (preserved_trx_skip_local_startup_recovery()) return false;
  if (!preserve_trx_is_enabled()) return false;
  if (srv_force_recovery > 0) return false;

  /*
    Startup preflight avoids entering crash recovery with snapshots that this
    binary cannot parse or whose binlog mode is incompatible. It uses
    SNAPSHOT_ONLY reads because no transaction is claimed during this scan.
  */
  const std::string dir = normalize_dir(preserve_trx_default_dir());
  auto store = create_preserved_trx_default_store(dir);
  Preserved_trx_carrier_listing listing;
  if (store->list_tokens(&listing) != Preserve_snapshot_status::OK) {
    const std::string message =
        "Failed to scan preserved transaction directory '" + dir +
        "' during recovery";
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return true;
  }

  const std::set<std::string> snapshot_tokens =
      preserved_trx_local_recoverable_snapshot_tokens(listing);
  for (const std::string &token : snapshot_tokens) {
    Preserved_trx_bundle bundle;
    Preserve_snapshot_status status =
        store->read(token, false,
                    Preserved_trx_carrier::Payload_read_mode::SNAPSHOT_ONLY,
                    &bundle);
    DBUG_EXECUTE_IF("preserve_trx_simulate_preflight_snapshot_read_corrupt",
                    status = Preserve_snapshot_status::CORRUPT;);
    DBUG_EXECUTE_IF("preserve_trx_simulate_preflight_snapshot_read_unsupported",
                    status = Preserve_snapshot_status::UNSUPPORTED;);
    if (status != Preserve_snapshot_status::OK) {
      const char *reason = preserved_trx_snapshot_read_failure_reason(status);
      if (preserved_trx_preflight_read_failure_requires_startup_abort(status)) {
        return log_preserved_trx_recovery_failure(token, reason);
      }
      log_preserved_trx_recovery_warning(token, reason);
      continue;
    }

    const Preserve_snapshot_metadata &metadata = bundle.metadata;
    if (!recoverable_binlog_state(metadata.binlog_state)) {
      return log_preserved_trx_recovery_failure(
          token, "unsupported durable transaction binlog state");
    }

    if (!binlog_state_matches_configured_mode(metadata)) {
      log_preserved_trx_rejected_binlog_mode(token, metadata);
    }
  }

  return false;
}

void preserved_trx_resurrection_index_bootstrap_preamble() {
  trx_preserve_startup_resurrection_reset();
  g_startup_resurrection_preamble_failed = false;
  if (preserved_trx_skip_local_startup_recovery()) return;
  if (!preserve_trx_is_enabled()) return;

  const std::string dir = normalize_dir(preserve_trx_default_dir());
  auto store = create_preserved_trx_default_store(dir);
  Preserved_trx_carrier_listing listing;
  if (store->list_tokens(&listing) != Preserve_snapshot_status::OK) {
    LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
           "PRESERVE: startup Resurrection Index scan unavailable; native "
           "Undo scan remains authoritative");
    g_startup_resurrection_preamble_failed = true;
    return;
  }

  auto terminalize = [&](const std::string &token, const char *reason) {
    log_preserved_trx_recovery_warning(token, reason);
    if (store->mark_tainted(token, reason) != Preserve_snapshot_status::OK) {
      LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: failed to mark preserved transaction snapshot "
              "tainted during startup Resurrection Index preamble; token=" +
              preserved_trx_redacted_token(token) + " reason=" + reason)
                 .c_str());
      g_startup_resurrection_preamble_failed = true;
    }
  };

  const std::set<std::string> snapshot_tokens =
      preserved_trx_local_recoverable_snapshot_tokens(listing);
  if (snapshot_tokens.empty()) return;

  if (srv_force_recovery > 0) {
    for (const std::string &token : snapshot_tokens) {
      terminalize(token, "force recovery disables ACTIVE Undo authority");
    }
    return;
  }

  Preserved_trx_codec_context context;
  bool context_initialized = false;
  constexpr uint64_t kMaxLocalResurrectionIndexBytes = 64ULL * 1024ULL * 1024ULL;
  for (const std::string &token : snapshot_tokens) {
    if (listing.tainted_tokens.count(token) != 0) continue;

    Preserved_trx_bundle bundle;
    if (store->read(token, true,
                    Preserved_trx_carrier::Payload_read_mode::SNAPSHOT_ONLY,
                    &bundle) != Preserve_snapshot_status::OK ||
        bundle.metadata.token != token) {
      terminalize(token, "startup snapshot is unreadable or mismatched");
      continue;
    }
    if (preserved_trx_snapshot_allows_synthetic_temp_claim(bundle.metadata)) {
      continue;
    }
    if (!preserve_trx_resurrection_metadata_supports_local_startup_index(
            bundle.metadata)) {
      terminalize(
          token,
          "startup Resurrection Index snapshot has unsupported engine shape");
      continue;
    }
    if (!context_initialized) {
      if (store->codec_context(
              &context, Preserved_trx_codec_context_purpose::READ_EXISTING) !=
              Preserve_snapshot_status::OK ||
          context.server_uuid.empty()) {
        LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
               "PRESERVE: startup Resurrection Index identity context "
               "unavailable; native Undo scan remains authoritative");
        g_startup_resurrection_preamble_failed = true;
        return;
      }
      context_initialized = true;
    }
    trx_preserve_startup_note_active_undo_v1_authority();

    std::vector<unsigned char> index_bytes;
    const Preserve_snapshot_status sidecar_status =
        store->read_resurrection_index(
            token, kMaxLocalResurrectionIndexBytes, &index_bytes);
    if (sidecar_status == Preserve_snapshot_status::NOT_FOUND) {
      terminalize(token, "startup Resurrection Index sidecar is missing");
      continue;
    }
    if (sidecar_status != Preserve_snapshot_status::OK) {
      terminalize(token, "startup Resurrection Index sidecar is unreadable");
      continue;
    }

    Preserve_trx_resurrection_index index;
    const std::string encoded(index_bytes.begin(), index_bytes.end());
    if (preserve_trx_decode_resurrection_index(encoded, context, &index) !=
            Preserve_trx_resurrection_index_status::OK ||
        index.local_instance_identity != context.server_uuid ||
        index.epoch_id != "local-" + token || index.entries.size() != 1) {
      terminalize(token,
                  "startup Resurrection Index digest or identity mismatch");
      continue;
    }

    std::array<unsigned char, kPreservedTrxSha256Length> expected_digest{};
    if (store->read_snapshot_payload_digest(token, &expected_digest) !=
            Preserve_snapshot_status::OK ||
        index.entries.front().snapshot_digest != expected_digest) {
      terminalize(token,
                  "startup Resurrection Index snapshot digest mismatch");
      continue;
    }

    Preserve_trx_resurrection_index_entry registration_entry =
        index.entries.front();
    DBUG_EXECUTE_IF(
        "preserve_trx_startup_force_resurrection_anchor_mismatch", {
          if (!registration_entry.undo_anchors.empty()) {
            ++registration_entry.undo_anchors.front().top_offset;
          }
        });
    if (preserve_trx_register_resurrection_candidate(registration_entry,
                                                     token)) {
      terminalize(token,
                  "startup Resurrection Index transaction facts mismatch");
      continue;
    }
  }
}

bool preserved_trx_resurrection_index_bootstrap_postamble() {
  if (preserved_trx_skip_local_startup_recovery()) {
    trx_preserve_startup_resurrection_reset();
    g_startup_resurrection_preamble_failed = false;
    return false;
  }
  if (g_startup_resurrection_preamble_failed) return true;
  if (srv_force_recovery > 0 || srv_read_only_mode) {
    trx_preserve_startup_resurrection_reset();
    return false;
  }

  trx_preserve_startup_reservation_result reservation;
  if (trx_preserve_startup_reserve_verified(&reservation) != DB_SUCCESS) {
    return true;
  }
  if (reservation.rejected_authorities.empty()) return false;

  auto store =
      create_preserved_trx_default_store(preserve_trx_default_dir());
  for (const std::string &token : reservation.rejected_authorities) {
    if (store->mark_tainted(token,
                            "ACTIVE Undo exact reservation failed") !=
        Preserve_snapshot_status::OK) {
      return true;
    }
  }
  trx_preserve_startup_forget_authorities(
      reservation.rejected_authorities);
  return false;
}

bool preserved_trx_resurrection_locks_postamble() {
  if (preserved_trx_skip_local_startup_recovery()) {
    trx_preserve_startup_resurrection_reset();
    return false;
  }
  std::vector<std::string> authorities;
  std::vector<trx_t *> transactions;
  if (trx_preserve_startup_collect_lock_resurrection_failures(
          &authorities, &transactions) != DB_SUCCESS) {
    return true;
  }
  if (authorities.empty()) return false;

  auto store =
      create_preserved_trx_default_store(preserve_trx_default_dir());
  for (const std::string &authority : authorities) {
    if (store->mark_tainted(
            authority, "ACTIVE Undo table-lock resurrection failed") !=
        Preserve_snapshot_status::OK) {
      return true;
    }
  }
  if (trx_preserve_abandon_active_undo_reservations(transactions) !=
      DB_SUCCESS) {
    return true;
  }
  trx_preserve_startup_forget_authorities(authorities);
  return false;
}

bool preserved_temp_images_bootstrap_preamble() {
  if (preserved_trx_skip_local_startup_recovery()) return false;
  if (!preserve_trx_is_enabled()) return false;
  if (srv_force_recovery > 0) return false;

  /*
    Temporary tablespace bootstrap must reserve source space ids before InnoDB
    starts assigning them to new temp spaces. Corrupt sidecars are tainted here,
    but rollback/deletion stays with normal preserve recovery.
  */
  const std::string dir = normalize_dir(preserve_trx_default_dir());
  auto store = create_preserved_trx_default_store(dir);
  Preserved_trx_carrier_listing listing;
  if (store->list_tokens(&listing) != Preserve_snapshot_status::OK) {
    const std::string message =
        "Failed to scan preserved transaction directory '" + dir +
        "' during temporary tablespace bootstrap";
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return true;
  }

  const Preserved_trx_carrier_listing local_crash_listing =
      preserved_trx_local_crash_abandon_listing(listing);
  if (preserved_trx_crash_recovery_artifacts_forbidden(
          local_crash_listing, "temporary tablespace bootstrap")) {
    return true;
  }

  if (delete_orphan_temp_table_sidecars_or_log(store.store(), listing))
    return true;

  auto taint_bootstrap_token_or_warn = [&](const std::string &token,
                                           const std::string &reason) {
    log_preserved_trx_recovery_warning(token, reason);
    if (store->mark_tainted(token, reason) != Preserve_snapshot_status::OK) {
      (void)log_preserved_trx_cleanup_failure(
          token, "failed to mark snapshot tainted during temporary tablespace "
                 "bootstrap");
    }
  };

  const std::set<std::string> snapshot_tokens =
      preserved_trx_local_recoverable_snapshot_tokens(listing);
  for (const std::string &token : snapshot_tokens) {
    Preserved_trx_bundle bundle;
    Preserve_snapshot_status status =
        store->read(token, true,
                    Preserved_trx_carrier::Payload_read_mode::SNAPSHOT_ONLY,
                    &bundle);
    DBUG_EXECUTE_IF("preserve_trx_simulate_bootstrap_snapshot_read_corrupt",
                    status = Preserve_snapshot_status::CORRUPT;);
    DBUG_EXECUTE_IF("preserve_trx_simulate_bootstrap_snapshot_read_unsupported",
                    status = Preserve_snapshot_status::UNSUPPORTED;);
    if (status != Preserve_snapshot_status::OK) {
      const std::string reason =
          std::string(preserved_trx_snapshot_read_failure_reason(status)) +
          " during temporary tablespace bootstrap";
      if (preserved_trx_preflight_read_failure_requires_startup_abort(status)) {
        return log_preserved_trx_recovery_failure(token, reason);
      }
      log_preserved_trx_recovery_warning(token, reason);
      continue;
    }
    if (bundle.metadata.temp_table_manifest_payload.empty()) continue;

    std::vector<trx_preserve_temp_space_image_descriptor> descriptors;
    std::string sidecar_reason;
    if (!build_temp_bootstrap_descriptors(bundle.metadata, &descriptors,
                                          &sidecar_reason)) {
      log_temp_sidecar_bootstrap_state(token, "TEMP_SIDECAR_CORRUPT_TAINTED",
                                       sidecar_reason);
      taint_bootstrap_token_or_warn(token, sidecar_reason);
      continue;
    }

    std::vector<uint32_t> reserved_space_ids;
    bool reserve_failed = false;
    for (const trx_preserve_temp_space_image_descriptor &descriptor :
         descriptors) {
      bool created = false;
      if (!trx_preserve_temp_space_image_reserve_or_keep_space_id(descriptor,
                                                                  &created)) {
        reserve_failed = true;
        break;
      }
      if (created) reserved_space_ids.push_back(descriptor.source_space_id);
    }
    if (reserve_failed) {
      for (uint32_t reserved_space_id : reserved_space_ids) {
        (void)trx_preserve_temp_space_image_release_reserved_space_id(
            reserved_space_id);
      }
      taint_bootstrap_token_or_warn(
          token,
          "failed to reserve preserved temporary tablespace id during "
          "bootstrap");
      continue;
    }

    DBUG_EXECUTE_IF("preserve_trx_trace_temp_sidecar_reserved_before_validate",
                    log_temp_sidecar_bootstrap_state(
                        token, "TEMP_SIDECAR_RESERVED_BEFORE_VALIDATION",
                        "reserved source space ids before sidecar digest "
                        "validation"););

    const Preserve_snapshot_status sidecar_status =
        validate_temp_sidecars_for_bootstrap(dir, token, bundle.metadata,
                                             &sidecar_reason);
    if (sidecar_status != Preserve_snapshot_status::OK) {
      if (sidecar_status == Preserve_snapshot_status::IO_ERROR) {
        return log_preserved_trx_recovery_failure(token, sidecar_reason);
      }
      release_temp_sidecar_space_id_reservations(bundle.metadata);
      if (!sidecar_reason.empty()) {
        log_temp_sidecar_bootstrap_state(token, "TEMP_SIDECAR_CORRUPT_DEFERRED",
                                         sidecar_reason);
        log_preserved_trx_recovery_failure(token, sidecar_reason);
      }
      /*
        Recovery proper still owns claiming or rolling back the prepared
        transaction; deleting the snapshot here would risk leaving a magic-XID
        transaction without its durable metadata.
      */
      continue;
    }

    /*
      Even with the temp-table subfeature disabled, existing local sidecars are
      still checked and their source space ids are reserved so a new temporary
      table cannot reuse a preserved image identity. No-redo undo bootstrap,
      page ownership adoption, and native adoption stay inert.
    */
    if (!preserve_trx_temp_table_enable) continue;

    Preserved_temp_table_manifest manifest;
    if (!preserve_trx_decode_temp_table_manifest(
            bundle.metadata.temp_table_manifest_payload, &manifest)) {
      release_temp_sidecar_space_id_reservations(bundle.metadata);
      taint_bootstrap_token_or_warn(
          token,
          "corrupt temporary table manifest during rollback segment "
          "bootstrap");
      continue;
    }

    std::vector<trx_preserve_temp_ownership_page_claim> claims;
    claims.reserve(manifest.ownership_claims.size());
    for (const Preserved_temp_table_ownership_claim &claim :
         manifest.ownership_claims) {
      if (claim.rseg_slot >= srv_rollback_segments) {
        release_temp_sidecar_space_id_reservations(bundle.metadata);
        std::ostringstream reason;
        reason << "preserved temporary rollback segment slot "
               << claim.rseg_slot
               << " is outside configured innodb_rollback_segments "
               << srv_rollback_segments << " during bootstrap";
        return log_preserved_trx_recovery_failure(token, reason.str());
      }
      trx_preserve_temp_ownership_page_claim innodb_claim;
      innodb_claim.token = claim.token;
      innodb_claim.source_space_id = claim.source_space_id;
      innodb_claim.rseg_space_id = claim.rseg_space_id;
      innodb_claim.rseg_page_no = claim.rseg_page_no;
      innodb_claim.rseg_id = claim.rseg_slot;
      innodb_claim.undo_slot = claim.undo_slot;
      innodb_claim.page_no = claim.page_no;
      innodb_claim.page_role = claim.page_role;
      claims.push_back(std::move(innodb_claim));
    }
    if (manifest.native_adoption_capable && !manifest.undo_images.empty()) {
      bool temp_undo_bootstrap_failed = false;
      std::map<uint32_t, trx_preserve_temp_space_image_descriptor>
          descriptors_by_space;
      for (const Preserved_temp_table_manifest_entry &entry :
           manifest.tables) {
        trx_preserve_temp_space_image_descriptor descriptor;
        descriptor.source_space_id = entry.image.source_space_id;
        descriptor.page_size = entry.image.page_size;
        descriptor.space_flags = entry.image.space_flags;
        descriptor.image_bytes = entry.image.size;
        std::copy(entry.image.sha256.begin(), entry.image.sha256.end(),
                  descriptor.image_digest);
        descriptor.sealed = true;
        descriptors_by_space.emplace(entry.image.source_space_id,
                                     std::move(descriptor));
      }

      Local_file_preserved_temp_table_image_carrier carrier(dir);
      for (const Preserved_temp_table_undo_descriptor &undo :
           manifest.undo_images) {
        auto descriptor_it = descriptors_by_space.find(undo.source_space_id);
        if (descriptor_it == descriptors_by_space.end()) {
          release_temp_sidecar_space_id_reservations(bundle.metadata);
          taint_bootstrap_token_or_warn(
              token,
              "temporary no-redo undo manifest references missing image "
              "during bootstrap");
          temp_undo_bootstrap_failed = true;
          break;
        }

        std::string undo_payload;
        const Preserved_trx_carrier_status read_status =
            carrier.read_sealed_undo(token, undo, &undo_payload);
        if (read_status != Preserved_trx_carrier_status::OK) {
          release_temp_sidecar_space_id_reservations(bundle.metadata);
          if (read_status == Preserved_trx_carrier_status::IO_ERROR ||
              read_status == Preserved_trx_carrier_status::
                                 IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST) {
            return log_preserved_trx_recovery_failure(
                token, "preserved temporary no-redo undo sidecar read failed "
                       "during bootstrap");
          }
          taint_bootstrap_token_or_warn(
              token,
              "corrupt temporary no-redo undo sidecar during bootstrap");
          temp_undo_bootstrap_failed = true;
          break;
        }
        if (!preserve_trx_temp_table_apply_manifest_undo_identity_for_resume(
                undo, &descriptor_it->second) ||
            trx_preserve_temp_space_image_load_no_redo_undo_sidecar(
                &descriptor_it->second,
                reinterpret_cast<const unsigned char *>(undo_payload.data()),
                undo_payload.length()) != DB_SUCCESS ||
            !trx_rseg_preserve_bootstrap_tmp_rseg_required_from_descriptor(
                descriptor_it->second)) {
          release_temp_sidecar_space_id_reservations(bundle.metadata);
          taint_bootstrap_token_or_warn(
              token,
              "invalid temporary no-redo undo bootstrap descriptor");
          temp_undo_bootstrap_failed = true;
          break;
        }
      }
      if (temp_undo_bootstrap_failed) continue;
    }

    if (!claims.empty()) {
      if (!trx_preserve_temp_space_image_register_page_reservations_from_claims(
              claims)) {
        release_temp_sidecar_space_id_reservations(bundle.metadata);
        taint_bootstrap_token_or_warn(
            token,
            "conflicting preserved temporary page ownership during bootstrap");
        continue;
      }
      if (!trx_rseg_preserve_bootstrap_tmp_rsegs_required_from_claims(
              claims)) {
        release_temp_sidecar_space_id_reservations(bundle.metadata);
        taint_bootstrap_token_or_warn(
            token,
            "conflicting preserved temporary rollback segment identity during "
            "bootstrap");
        continue;
      }
    }
  }

  return false;
}

bool preserved_trx_recover_all() {
  struct Startup_verified_resurrection_guard {
    ~Startup_verified_resurrection_guard() {
      trx_preserve_startup_resurrection_clear_verified();
    }
  } startup_verified_resurrection_guard;
  const uint64_t recovery_started_us = preserve_trx_monotonic_us();
  struct Startup_recovery_phase_metrics {
    uint64_t list_us{0};
    uint64_t orphan_rollback_us{0};
    uint64_t attempt_ledger_us{0};
    uint64_t warm_artifact_cleanup_us{0};
    uint64_t stale_tmp_cleanup_us{0};
    uint64_t tainted_cleanup_us{0};
    uint64_t snapshot_recover_us{0};
    uint64_t snapshot_load_us{0};
    uint64_t snapshot_validate_us{0};
    uint64_t snapshot_kernel_us{0};
    Preserved_trx_recover_or_adopt_phase_metrics snapshot_kernel_breakdown;
    uint64_t orphan_binlog_cleanup_us{0};
    uint64_t orphan_taint_cleanup_us{0};
  } phase_metrics;
  auto elapsed_since_us = [](uint64_t started_us) {
    const uint64_t now_us = preserve_trx_monotonic_us();
    return now_us >= started_us ? now_us - started_us : 0;
  };
  uint64_t listing_snapshot_token_count = 0;
  uint64_t listing_standby_pending_token_count = 0;
  uint64_t listing_promotion_intent_token_count = 0;
  uint64_t local_snapshot_token_count = 0;
  uint64_t binlog_cache_token_count = 0;
  uint64_t tainted_token_count = 0;
  uint32_t orphan_rollback_count = 0;

  {
    const std::string message =
        "PRESERVE: preserved transaction recovery begin preserve_enabled=" +
        std::to_string(preserve_trx_is_enabled() ? 1 : 0);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
  }

  auto finish_recovery = [&](bool error, const char *outcome) {
    const uint64_t elapsed_us = preserve_trx_monotonic_us() - recovery_started_us;
    g_startup_recovery_elapsed_us.store(static_cast<ulonglong>(elapsed_us));
    g_startup_recovery_error.store(error ? 1 : 0);
    g_startup_recovery_snapshot_tokens.store(
        static_cast<ulonglong>(listing_snapshot_token_count));
    g_startup_recovery_local_snapshot_tokens.store(
        static_cast<ulonglong>(local_snapshot_token_count));
    g_startup_recovery_binlog_cache_tokens.store(
        static_cast<ulonglong>(binlog_cache_token_count));
    g_startup_recovery_tainted_tokens.store(
        static_cast<ulonglong>(tainted_token_count));
    g_startup_recovery_standby_pending_tokens.store(
        static_cast<ulonglong>(listing_standby_pending_token_count));
    g_startup_recovery_promotion_intent_tokens.store(
        static_cast<ulonglong>(listing_promotion_intent_token_count));
    g_startup_recovery_orphan_rollback_count.store(
        static_cast<ulonglong>(orphan_rollback_count));
    g_startup_recovery_phase_snapshot_load_us.store(
        static_cast<ulonglong>(phase_metrics.snapshot_load_us));
    g_startup_recovery_phase_snapshot_validate_us.store(
        static_cast<ulonglong>(phase_metrics.snapshot_validate_us));
    g_startup_recovery_phase_snapshot_kernel_us.store(
        static_cast<ulonglong>(phase_metrics.snapshot_kernel_us));
    g_startup_recovery_phase_snapshot_claim_us.store(static_cast<ulonglong>(
        phase_metrics.snapshot_kernel_breakdown.claim_us));
    g_startup_recovery_phase_snapshot_read_view_us.store(static_cast<ulonglong>(
        phase_metrics.snapshot_kernel_breakdown.read_view_us));
    g_startup_recovery_phase_snapshot_table_locks_us.store(
        static_cast<ulonglong>(
            phase_metrics.snapshot_kernel_breakdown.table_locks_us));
    g_startup_recovery_phase_snapshot_record_locks_us.store(
        static_cast<ulonglong>(
            phase_metrics.snapshot_kernel_breakdown.record_locks_us));
    g_startup_recovery_phase_snapshot_record_lock_entries.store(
        static_cast<ulonglong>(phase_metrics.snapshot_kernel_breakdown
                                   .record_lock_import.record_entries));
    g_startup_recovery_phase_snapshot_record_lock_stable_page_hits.store(
        static_cast<ulonglong>(phase_metrics.snapshot_kernel_breakdown
                                   .record_lock_import.stable_page_hits));
    g_startup_recovery_phase_snapshot_record_lock_image_resolves.store(
        static_cast<ulonglong>(phase_metrics.snapshot_kernel_breakdown
                                   .record_lock_import.image_resolves));
    g_startup_recovery_phase_snapshot_record_lock_bitmap_pages.store(
        static_cast<ulonglong>(phase_metrics.snapshot_kernel_breakdown
                                   .record_lock_import.bitmap_pages));
    g_startup_recovery_phase_snapshot_record_lock_bitmap_bits.store(
        static_cast<ulonglong>(phase_metrics.snapshot_kernel_breakdown
                                   .record_lock_import.bitmap_bits));
    g_startup_recovery_phase_snapshot_record_lock_page_get_us.store(
        static_cast<ulonglong>(phase_metrics.snapshot_kernel_breakdown
                                   .record_lock_import.page_get_us));
    g_startup_recovery_phase_snapshot_record_lock_page_get_count.store(
        static_cast<ulonglong>(phase_metrics.snapshot_kernel_breakdown
                                   .record_lock_import.page_get_count));
    g_startup_recovery_phase_snapshot_record_lock_table_open_us.store(
        static_cast<ulonglong>(phase_metrics.snapshot_kernel_breakdown
                                   .record_lock_import.table_open_us));
    g_startup_recovery_phase_snapshot_record_lock_prefetch_pages.store(
        static_cast<ulonglong>(phase_metrics.snapshot_kernel_breakdown
                                   .record_lock_import.prefetch_pages));
    g_startup_recovery_phase_snapshot_record_lock_prefetch_bytes.store(
        static_cast<ulonglong>(phase_metrics.snapshot_kernel_breakdown
                                   .record_lock_import.prefetch_bytes));
    g_startup_recovery_phase_snapshot_record_lock_prefetch_residency_pages.store(
        static_cast<ulonglong>(phase_metrics.snapshot_kernel_breakdown
                                   .record_lock_import.prefetch_residency_pages));
    g_startup_recovery_phase_snapshot_record_lock_prefetch_resident_pages.store(
        static_cast<ulonglong>(phase_metrics.snapshot_kernel_breakdown
                                   .record_lock_import.prefetch_resident_pages));
    g_startup_recovery_phase_snapshot_record_lock_prefetch_io_pending_pages.store(
        static_cast<ulonglong>(
            phase_metrics.snapshot_kernel_breakdown.record_lock_import
                .prefetch_io_pending_pages));
    g_startup_recovery_phase_snapshot_record_lock_prefetch_missing_pages.store(
        static_cast<ulonglong>(phase_metrics.snapshot_kernel_breakdown
                                   .record_lock_import.prefetch_missing_pages));
    g_startup_recovery_phase_snapshot_predicate_locks_us.store(
        static_cast<ulonglong>(
            phase_metrics.snapshot_kernel_breakdown.predicate_locks_us));
    g_startup_recovery_phase_snapshot_mdl_us.store(
        static_cast<ulonglong>(phase_metrics.snapshot_kernel_breakdown.mdl_us));
    g_startup_recovery_phase_snapshot_register_us.store(static_cast<ulonglong>(
        phase_metrics.snapshot_kernel_breakdown.register_us));
    const std::string message =
        "PRESERVE: preserved transaction recovery end elapsed_us=" +
        std::to_string(elapsed_us) + " error=" +
        std::to_string(error ? 1 : 0) + " outcome=" +
        (outcome == nullptr ? "unknown" : outcome) +
        " snapshot_tokens=" + std::to_string(listing_snapshot_token_count) +
        " local_snapshot_tokens=" + std::to_string(local_snapshot_token_count) +
        " binlog_cache_tokens=" + std::to_string(binlog_cache_token_count) +
        " tainted_tokens=" + std::to_string(tainted_token_count) +
        " standby_pending_tokens=" +
        std::to_string(listing_standby_pending_token_count) +
        " promotion_intent_tokens=" +
        std::to_string(listing_promotion_intent_token_count) +
        " orphan_rollback_count=" + std::to_string(orphan_rollback_count) +
        " phase_list_us=" + std::to_string(phase_metrics.list_us) +
        " phase_orphan_rollback_us=" +
        std::to_string(phase_metrics.orphan_rollback_us) +
        " phase_attempt_ledger_us=" +
        std::to_string(phase_metrics.attempt_ledger_us) +
        " phase_warm_artifact_cleanup_us=" +
        std::to_string(phase_metrics.warm_artifact_cleanup_us) +
        " phase_stale_tmp_cleanup_us=" +
        std::to_string(phase_metrics.stale_tmp_cleanup_us) +
        " phase_tainted_cleanup_us=" +
        std::to_string(phase_metrics.tainted_cleanup_us) +
        " phase_snapshot_recover_us=" +
        std::to_string(phase_metrics.snapshot_recover_us) +
        " phase_snapshot_load_us=" +
        std::to_string(phase_metrics.snapshot_load_us) +
        " phase_snapshot_validate_us=" +
        std::to_string(phase_metrics.snapshot_validate_us) +
        " phase_snapshot_kernel_us=" +
        std::to_string(phase_metrics.snapshot_kernel_us) +
        " phase_snapshot_claim_us=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.claim_us) +
        " phase_snapshot_read_view_us=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.read_view_us) +
        " phase_snapshot_table_locks_us=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.table_locks_us) +
        " phase_snapshot_record_locks_us=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.record_locks_us) +
        " phase_snapshot_record_lock_entries=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.record_lock_import
                           .record_entries) +
        " phase_snapshot_record_lock_stable_page_hits=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.record_lock_import
                           .stable_page_hits) +
        " phase_snapshot_record_lock_image_resolves=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.record_lock_import
                           .image_resolves) +
        " phase_snapshot_record_lock_bitmap_pages=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.record_lock_import
                           .bitmap_pages) +
        " phase_snapshot_record_lock_bitmap_bits=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.record_lock_import
                           .bitmap_bits) +
        " phase_snapshot_record_lock_page_get_us=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.record_lock_import
                           .page_get_us) +
        " phase_snapshot_record_lock_page_get_count=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.record_lock_import
                           .page_get_count) +
        " phase_snapshot_record_lock_table_open_us=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.record_lock_import
                           .table_open_us) +
        " phase_snapshot_record_lock_prefetch_pages=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.record_lock_import
                           .prefetch_pages) +
        " phase_snapshot_record_lock_prefetch_bytes=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.record_lock_import
                           .prefetch_bytes) +
        " phase_snapshot_record_lock_prefetch_residency_pages=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.record_lock_import
                           .prefetch_residency_pages) +
        " phase_snapshot_record_lock_prefetch_resident_pages=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.record_lock_import
                           .prefetch_resident_pages) +
        " phase_snapshot_record_lock_prefetch_io_pending_pages=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.record_lock_import
                           .prefetch_io_pending_pages) +
        " phase_snapshot_record_lock_prefetch_missing_pages=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.record_lock_import
                           .prefetch_missing_pages) +
        " phase_snapshot_predicate_locks_us=" +
        std::to_string(
            phase_metrics.snapshot_kernel_breakdown.predicate_locks_us) +
        " phase_snapshot_mdl_us=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.mdl_us) +
        " phase_snapshot_register_us=" +
        std::to_string(phase_metrics.snapshot_kernel_breakdown.register_us) +
        " phase_orphan_binlog_cleanup_us=" +
        std::to_string(phase_metrics.orphan_binlog_cleanup_us) +
        " phase_orphan_taint_cleanup_us=" +
        std::to_string(phase_metrics.orphan_taint_cleanup_us);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    preserved_trx_mark_recovery_complete();
    return error;
  };

  if (preserved_trx_skip_local_startup_recovery()) {
    trx_preserve_startup_resurrection_reset();
    return finish_recovery(false, "transfer_startup_local_recovery_skipped");
  }

  if (!preserve_trx_is_enabled()) {
    return finish_recovery(false, "disabled_no_artifact_work");
  }

  /*
    Recovery owns every durable token left by the previous server: valid
    snapshots are imported, tainted snapshots are rolled back, orphan prepared
    transactions without snapshots are rolled back, and staging artifacts that
    were never published are removed.
  */
  const std::string dir = normalize_dir(preserve_trx_default_dir());
  auto store = create_preserved_trx_default_store(dir);
  const uint64_t recovery_anchor_wall_us = my_micro_time();
  const uint64_t recovery_anchor_monotonic_us = preserve_trx_monotonic_us();
  DBUG_EXECUTE_IF("preserve_trx_fail_recover_scan", {
    const std::string message =
        "Failed to scan preserved transaction directory '" + dir +
        "' during recovery, error " + std::to_string(EIO);
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return finish_recovery(true, "debug_scan_failure");
  });

  Preserved_trx_carrier_listing listing;
  uint64_t phase_started_us = preserve_trx_monotonic_us();
  if (store->list_tokens(&listing) != Preserve_snapshot_status::OK) {
    phase_metrics.list_us += elapsed_since_us(phase_started_us);
    const std::string message =
        "Failed to scan preserved transaction directory '" + dir +
        "' during recovery";
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return finish_recovery(true, "list_tokens_failed");
  }
  phase_metrics.list_us += elapsed_since_us(phase_started_us);
  listing_snapshot_token_count = listing.snapshot_tokens.size();
  listing_standby_pending_token_count = listing.standby_pending_tokens.size();
  listing_promotion_intent_token_count = listing.promotion_intent_tokens.size();

  /*
    Standby-transfer artifacts are published as durable files but are not local
    resume tokens yet. Promotion-side code must explicitly adopt them; ordinary
    startup recovery must not import or roll them back as if this server created
    the preserved transaction.
  */
  std::set<std::string> snapshot_tokens =
      preserved_trx_local_recoverable_snapshot_tokens(listing);
  std::set<std::string> binlog_cache_tokens =
      preserved_trx_filter_standby_pending_tokens_for_local_recovery(
          listing.external_blob_tokens, listing);
  std::set<std::string> tainted_tokens =
      preserved_trx_filter_standby_pending_tokens_for_local_recovery(
          listing.tainted_tokens, listing);
  std::set<std::string> consume_state_tokens =
      preserved_trx_filter_standby_pending_tokens_for_local_recovery(
          listing.consume_state_tokens, listing);
  std::set<std::string> warm_external_blob_artifacts =
      listing.warm_external_blob_artifacts;
  local_snapshot_token_count = snapshot_tokens.size();
  binlog_cache_token_count = binlog_cache_tokens.size();
  tainted_token_count = tainted_tokens.size();

  const Preserved_trx_carrier_listing local_crash_listing =
      preserved_trx_local_crash_abandon_listing(listing);
  if (preserved_trx_crash_recovery_artifacts_forbidden(
          local_crash_listing, "preserved transaction recovery")) {
    return finish_recovery(true, "crash_artifacts_forbidden");
  }

  if (srv_force_recovery > 0) {
    if (!snapshot_tokens.empty() || !binlog_cache_tokens.empty()) {
      std::set<std::string> preserved_tokens(snapshot_tokens);
      preserved_tokens.insert(binlog_cache_tokens.begin(),
                              binlog_cache_tokens.end());
      bool taint_error = false;
      for (const std::string &token : preserved_tokens) {
        if (store->mark_tainted(token, "innodb_force_recovery") !=
            Preserve_snapshot_status::OK) {
          taint_error = true;
          LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                 "PRESERVE: failed to mark preserved transaction snapshot "
                 "tainted while innodb_force_recovery is enabled");
        }
      }
      if (taint_error) {
        return finish_recovery(true, "force_recovery_taint_failed");
      }
      LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
             "PRESERVE: preserved transaction recovery is not supported while "
             "innodb_force_recovery is enabled; snapshot and binlog-cache "
             "files are retained and marked tainted");
    }
    return finish_recovery(false, "force_recovery_deferred");
  }

  phase_started_us = preserve_trx_monotonic_us();
  for (const std::string &token : consume_state_tokens) {
    if (listing.snapshot_tokens.count(token) == 0) continue;
    Preserved_trx_bundle cleanup_bundle;
    Preserve_snapshot_metadata *cleanup_metadata = nullptr;
    if (store->read(token, true,
                    Preserved_trx_carrier::Payload_read_mode::METADATA_ONLY,
                    &cleanup_bundle) == Preserve_snapshot_status::OK) {
      cleanup_metadata = &cleanup_bundle.metadata;
    }
    if (rollback_preserved_snapshot_or_log(
            dir, token, "durable consume-state cleanup", cleanup_metadata,
            cleanup_metadata == nullptr
                ? Temp_sidecar_cleanup_mode::RAW_UNLINK
                : Temp_sidecar_cleanup_mode::METADATA_AWARE)) {
      return finish_recovery(true, "consume_state_cleanup_failed");
    }
  }
  phase_metrics.tainted_cleanup_us += elapsed_since_us(phase_started_us);

  Preserved_trx_recovery_attempt_ledger recovery_attempt_ledger;
  phase_started_us = preserve_trx_monotonic_us();
  if (!snapshot_tokens.empty() &&
      preserved_trx_publish_recovery_attempt_ledger(
          dir, snapshot_tokens, &recovery_attempt_ledger)) {
    phase_metrics.attempt_ledger_us += elapsed_since_us(phase_started_us);
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           "PRESERVE: failed to publish preserved transaction recovery "
           "attempt ledger during startup recovery");
    return finish_recovery(true, "recovery_attempt_ledger_failed");
  }
  phase_metrics.attempt_ledger_us += elapsed_since_us(phase_started_us);

  bool error = false;
  std::vector<Startup_snapshot_recover_task> startup_recovery_tasks;
  startup_recovery_tasks.reserve(snapshot_tokens.size());
  auto add_snapshot_recover_metrics =
      [&](const Startup_snapshot_recover_phase_metrics &snapshot_phase_metrics) {
        phase_metrics.snapshot_load_us += snapshot_phase_metrics.load_us;
        phase_metrics.snapshot_validate_us += snapshot_phase_metrics.validate_us;
        phase_metrics.snapshot_kernel_us += snapshot_phase_metrics.kernel_us;
        add_recover_or_adopt_phase_metrics(
            snapshot_phase_metrics.kernel_breakdown,
            &phase_metrics.snapshot_kernel_breakdown);
      };
  phase_started_us = preserve_trx_monotonic_us();
  for (const std::string &artifact_filename : warm_external_blob_artifacts) {
    if (delete_orphan_warm_external_blob_artifact_or_log(store.store(),
                                                         artifact_filename)) {
      error = true;
    }
  }
  phase_metrics.warm_artifact_cleanup_us += elapsed_since_us(phase_started_us);
  for (const std::string &token : snapshot_tokens) {
    phase_started_us = preserve_trx_monotonic_us();
    const bool heavy_cleanup =
        listing.stale_tmp_tokens.find(token) != listing.stale_tmp_tokens.end() ||
        listing.temp_sidecar_tokens.find(token) !=
            listing.temp_sidecar_tokens.end();
    if (delete_stale_tmp_files_or_log(store.store(), token, heavy_cleanup)) {
      phase_metrics.stale_tmp_cleanup_us += elapsed_since_us(phase_started_us);
      error = true;
      continue;
    }
    phase_metrics.stale_tmp_cleanup_us += elapsed_since_us(phase_started_us);
    if (tainted_tokens.find(token) != tainted_tokens.end()) {
      phase_started_us = preserve_trx_monotonic_us();
      Preserved_trx_bundle cleanup_bundle;
      Preserve_snapshot_metadata *cleanup_metadata = nullptr;
      if (store->read(token, true,
                      Preserved_trx_carrier::Payload_read_mode::METADATA_ONLY,
                      &cleanup_bundle) == Preserve_snapshot_status::OK) {
        cleanup_metadata = &cleanup_bundle.metadata;
      }
      if (rollback_preserved_snapshot_or_log(
              dir, token, preserved_trx_tainted_reason(dir, token),
              cleanup_metadata, Temp_sidecar_cleanup_mode::RAW_UNLINK))
        error = true;
      phase_metrics.tainted_cleanup_us += elapsed_since_us(phase_started_us);
      continue;
    }
    const auto attempt = recovery_attempt_ledger.find(token);
    if (attempt == recovery_attempt_ledger.end()) {
      LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
             "PRESERVE: missing preserved transaction recovery attempt "
             "ledger entry during startup recovery");
      error = true;
      continue;
    }
    Startup_snapshot_recover_task task;
    task.token = token;
    task.recovery_attempt_count = attempt->second;
    startup_recovery_tasks.push_back(std::move(task));
  }

  if (!startup_recovery_tasks.empty()) {
    if (preserve_trx_recover_lock_page_prefetch) {
      prewarm_record_lock_pages_for_startup_recovery(
          dir, &startup_recovery_tasks,
          &phase_metrics.snapshot_kernel_breakdown.record_lock_import);
    }

    auto recover_startup_task = [&](Startup_snapshot_recover_task *task) {
      if (task == nullptr) return;
      task->error = recover_preserved_snapshot(
          dir, task->token, task->recovery_attempt_count,
          task->record_lock_pages_prewarmed,
          recovery_anchor_wall_us, recovery_anchor_monotonic_us,
          &task->phase_metrics);
      task->processed = true;
    };

    phase_started_us = preserve_trx_monotonic_us();
    const uint startup_recovery_worker_count =
        preserve_trx_effective_startup_recovery_threads(
            startup_recovery_tasks.size());
    if (startup_recovery_worker_count <= 1) {
      std::unique_ptr<Auto_THD> startup_recovery_thd;
      if (current_thd == nullptr) {
        startup_recovery_thd = std::make_unique<Auto_THD>();
      }
      for (Startup_snapshot_recover_task &task : startup_recovery_tasks) {
        recover_startup_task(&task);
      }
    } else {
      std::atomic<size_t> next_startup_recovery_index{0};
      std::atomic<bool> worker_init_failed{false};
      std::atomic<bool> worker_exception_failed{false};
      std::atomic<size_t> worker_init_reports{0};
      std::atomic<bool> workers_released{false};
      std::atomic<bool> worker_abort{false};
      std::vector<std::thread> startup_recovery_workers;
      auto join_startup_workers = create_scope_guard([&] {
        for (std::thread &worker : startup_recovery_workers) {
          if (worker.joinable()) worker.join();
        }
      });
      try {
        startup_recovery_workers.reserve(startup_recovery_worker_count);
        for (uint worker_index = 0;
             worker_index < startup_recovery_worker_count; ++worker_index) {
          startup_recovery_workers.emplace_back([&]() {
            if (my_thread_init()) {
              worker_init_failed.store(true, std::memory_order_relaxed);
              worker_abort.store(true, std::memory_order_release);
              worker_init_reports.fetch_add(1, std::memory_order_release);
              return;
            }
            auto thread_end_guard = create_scope_guard([] { my_thread_end(); });
            worker_init_reports.fetch_add(1, std::memory_order_release);
            while (!workers_released.load(std::memory_order_acquire)) {
              std::this_thread::yield();
            }
            if (worker_abort.load(std::memory_order_acquire)) return;
            try {
              std::unique_ptr<Auto_THD> startup_recovery_thd;
              if (current_thd == nullptr) {
                startup_recovery_thd = std::make_unique<Auto_THD>();
              }
              for (;;) {
                if (worker_abort.load(std::memory_order_acquire)) break;
                const size_t task_index =
                    next_startup_recovery_index.fetch_add(
                        1, std::memory_order_relaxed);
                if (task_index >= startup_recovery_tasks.size()) break;
                recover_startup_task(&startup_recovery_tasks[task_index]);
              }
            } catch (...) {
              worker_exception_failed.store(true, std::memory_order_relaxed);
              worker_abort.store(true, std::memory_order_release);
            }
          });
        }
      } catch (...) {
        worker_exception_failed.store(true, std::memory_order_relaxed);
        worker_abort.store(true, std::memory_order_release);
      }
      if (!worker_abort.load(std::memory_order_acquire)) {
        while (worker_init_reports.load(std::memory_order_acquire) <
               startup_recovery_workers.size()) {
          std::this_thread::yield();
        }
        if (worker_init_failed.load(std::memory_order_relaxed)) {
          worker_abort.store(true, std::memory_order_release);
        }
      }
      workers_released.store(true, std::memory_order_release);
      join_startup_workers.rollback();
      if (worker_init_failed.load(std::memory_order_relaxed) ||
          worker_exception_failed.load(std::memory_order_relaxed)) {
        error = true;
      }
    }
    phase_metrics.snapshot_recover_us += elapsed_since_us(phase_started_us);

    for (const Startup_snapshot_recover_task &task : startup_recovery_tasks) {
      add_snapshot_recover_metrics(task.phase_metrics);
      if (!task.processed || task.error) error = true;
    }
  }

  phase_started_us = preserve_trx_monotonic_us();
  for (const std::string &token : binlog_cache_tokens) {
    if (snapshot_tokens.find(token) != snapshot_tokens.end()) continue;
    if (delete_orphan_binlog_cache_or_log(dir, token)) error = true;
  }
  phase_metrics.orphan_binlog_cleanup_us += elapsed_since_us(phase_started_us);
  phase_started_us = preserve_trx_monotonic_us();
  for (const std::string &token : tainted_tokens) {
    if (snapshot_tokens.find(token) != snapshot_tokens.end()) continue;
    if (store->remove_taint(token) != Preserve_snapshot_status::OK) {
      error = true;
    }
  }
  for (const std::string &token : consume_state_tokens) {
    if (listing.snapshot_tokens.count(token) != 0) continue;
    if (store->remove_consume_state(token) != Preserve_snapshot_status::OK) {
      error = true;
    }
  }
  phase_metrics.orphan_taint_cleanup_us += elapsed_since_us(phase_started_us);

  return finish_recovery(error, error ? "completed_with_errors" : "completed");
}

bool preserve_trx_kernel_preserve_attached_transaction(
    const Preserve_trx_kernel_request &request) {
  DBUG_TRACE;

  /*
    Kernel preserve owns the correctness path for one batch drain target:
      1. validate SQL/session/engine eligibility and preflight participants;
      2. freeze undo/temp/lock/binlog state at the Preserve boundary;
      3. detach and claim the ACTIVE-Undo trx from the original THD;
      4. write the authenticated snapshot and external blobs;
      5. register the token or return ownership to cleanup/fallback paths.
  */

  THD *target_thd = request.target_thd;
  const Preserve_trx_options &options = request.options;
  const ulonglong timeout_seconds = request.timeout_seconds;
  Preserve_trx_preserve_result *result = request.result;
  PreserveBinlogBlobProvider *binlog_blob_provider =
      request.binlog_blob_provider;

  if (result != nullptr) *result = Preserve_trx_preserve_result{};
  struct Preserve_stage_timing_scope {
    Preserve_trx_preserve_result *result{nullptr};
    Preserve_trx_preserve_stage current_stage{
        Preserve_trx_preserve_stage::VALIDATION};
    ulonglong stage_started_us{preserve_trx_monotonic_us()};

    static void add_elapsed(Preserve_trx_preserve_result *target,
                            Preserve_trx_preserve_stage stage,
                            ulonglong elapsed_us) {
      if (target == nullptr || elapsed_us == 0) return;
      switch (stage) {
        case Preserve_trx_preserve_stage::BINLOG_PREFLIGHT:
          target->binlog_preflight_us += elapsed_us;
          break;
        case Preserve_trx_preserve_stage::LOCK_PREFLIGHT:
          target->lock_preflight_us += elapsed_us;
          break;
        case Preserve_trx_preserve_stage::UNDO_PREPARE:
          target->prepare_us += elapsed_us;
          break;
        case Preserve_trx_preserve_stage::DETACH:
          target->detach_claim_us += elapsed_us;
          break;
        case Preserve_trx_preserve_stage::SNAPSHOT_WRITE:
          target->snapshot_write_us += elapsed_us;
          break;
        case Preserve_trx_preserve_stage::RECORD_REGISTER:
          target->record_register_us += elapsed_us;
          break;
        case Preserve_trx_preserve_stage::VALIDATION:
        case Preserve_trx_preserve_stage::COMPLETE:
          break;
      }
    }

    void set_stage(Preserve_trx_preserve_stage stage) {
      const ulonglong now_us = preserve_trx_monotonic_us();
      add_elapsed(result, current_stage,
                  now_us >= stage_started_us ? now_us - stage_started_us : 0);
      current_stage = stage;
      stage_started_us = now_us;
      if (result != nullptr) result->stage = stage;
    }

    ~Preserve_stage_timing_scope() {
      const ulonglong now_us = preserve_trx_monotonic_us();
      add_elapsed(result, current_stage,
                  now_us >= stage_started_us ? now_us - stage_started_us : 0);
    }
  } stage_timing{result};
  auto set_stage = [&stage_timing](Preserve_trx_preserve_stage stage) {
    stage_timing.set_stage(stage);
  };
  auto set_failure_reason = [result](const char *reason) {
    if (result != nullptr && result->failure_reason == nullptr)
      result->failure_reason = reason;
  };
  auto add_result_elapsed_us =
      [result](uint64_t Preserve_trx_preserve_result::*field,
               ulonglong started_us) {
        if (result != nullptr) {
          const ulonglong now_us = preserve_trx_monotonic_us();
          result->*field +=
              now_us >= started_us ? now_us - started_us : 0;
        }
      };

  THD *thd = target_thd;
  if (thd == nullptr) {
    set_failure_reason("null_target_thd");
    return preserve_trx_reject_unsupported();
  }
  const std::string preserve_resource_lease_key =
      "preserve-thd-" + std::to_string(thd->thread_id());

  auto reset_requested = [&]() {
    return request.drain_ownership != nullptr &&
           request.drain_ownership->state() ==
               Preserve_trx_drain_terminal::RESET_REQUESTED;
  };
  auto reject_unsupported_for_delivery = [&](const char *reason = "unsupported") {
    set_failure_reason(reason);
    if (thd->is_error()) thd->clear_error();
    return true;
  };

  if (reset_requested())
    return reject_unsupported_for_delivery("batch_target_preserve_reset");

  if (timeout_seconds == 0)
    return reject_unsupported_for_delivery("timeout_zero");

  const Preserve_trx_transfer_artifact_decision artifact_decision =
      preserve_trx_transfer_artifact_decision();
  if (artifact_decision ==
      Preserve_trx_transfer_artifact_decision::UNSUPPORTED) {
    return reject_unsupported_for_delivery(
        "standby_transfer_publish_unsupported");
  }

  if (!preserved_trx_binlog_format_is_supported(
          thd->variables.binlog_format)) {
    return reject_unsupported_for_delivery("binlog_format_not_row");
  }

  set_stage(Preserve_trx_preserve_stage::BINLOG_PREFLIGHT);
  Preserve_snapshot_binlog_state binlog_state =
      Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE;
  Mysql_binlog_preserve_snapshot binlog_snapshot;
  PrebuiltBinlogCacheBlob prebuilt_binlog_blob;
  Preserve_memory_lease single_phase_binlog_payload_lease;
  bool has_logged_binlog_cache = false;
  bool use_prebuilt_binlog_cache = false;
  if (determine_no_cache_binlog_state(thd, &binlog_state)) {
    if (binlog_blob_provider != nullptr) {
      if (!binlog_blob_provider->has_blob_for_thd(thd))
        return reject_unsupported_for_delivery("warmcopy_blob_missing");
      use_prebuilt_binlog_cache = true;
      if (mysql_binlog_preserve_export_metadata_only(thd, &binlog_snapshot))
        return reject_unsupported_for_delivery("binlog_metadata_export_failed");
    } else {
      if (thd->binlog_flush_pending_rows_event(true))
        return reject_unsupported_for_delivery("binlog_pending_rows_flush_failed");
      uint64_t binlog_cache_length = 0;
      bool has_binlog_cache = false;
      if (mysql_binlog_preserve_warmcopy_cache_length(thd, &binlog_cache_length,
                                                      &has_binlog_cache) ||
          (has_binlog_cache &&
           binlog_cache_length >
               preserve_trx_single_phase_max_binlog_cache_bytes)) {
        return reject_unsupported_for_delivery("binlog_cache_too_large");
      }
      uint64_t binlog_payload_peak_bytes = 0;
      if (!has_binlog_cache ||
          !binlog_payload_memory_peak(binlog_cache_length,
                                      &binlog_payload_peak_bytes)) {
        return reject_unsupported_for_delivery("binlog_cache_size_invalid");
      }
      single_phase_binlog_payload_lease = preserve_trx_acquire_memory_lease(
          preserve_resource_lease_key,
          Preserve_trx_memory_kind::BINLOG_WARMCOPY_BUFFER,
          binlog_payload_peak_bytes);
      if (!single_phase_binlog_payload_lease.acquired()) {
        return reject_unsupported_for_delivery(
            "binlog_cache_resource_exhausted");
      }
      preserve_trx_warmcopy_note_provider_full_copy_to();
      if (mysql_binlog_preserve_export(thd, &binlog_snapshot))
        return reject_unsupported_for_delivery("binlog_cache_export_failed");
    }
    binlog_state = Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE;
    has_logged_binlog_cache = true;
    if (result != nullptr) result->logged_binlog_cache = true;
  }

  auto reject_after_binlog_export = [&](const char *reason) {
    set_failure_reason(reason);
    if (has_logged_binlog_cache) {
      if (mysql_binlog_preserve_reactivate_after_prepare_failure(
              thd, binlog_snapshot)) {
        LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
               "PRESERVE: failed to restore binlog cache after rejecting "
               "pre-prepare transaction preserve; killing session");
        if (result != nullptr) {
          result->cleanup_failed_after_reattach = true;
        }
        thd->killed = THD::KILL_CONNECTION;
      }
    }
    return reject_unsupported_for_delivery(reason);
  };

  const bool unsupported_transaction_contents =
      preserve_trx_has_unsupported_transaction_contents(thd, binlog_state);
  if (unsupported_transaction_contents) {
    return reject_after_binlog_export("unsupported_transaction_contents");
  }
  const Preserve_snapshot_status temp_preflight_status =
      preserve_trx_temp_table_preflight_preserve(thd);
  if (temp_preflight_status != Preserve_snapshot_status::OK) {
    const std::string temp_reason =
        preserve_trx_temp_table_degraded_reason(thd);
    if (!temp_reason.empty()) {
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: temp-table preflight rejected preserve: " +
              temp_reason)
                 .c_str());
    }
    return reject_after_binlog_export(
        temp_reason.empty() ? "temp_table_no_redo_undo_unsupported"
                            : temp_reason.c_str());
  }

  DBUG_EXECUTE_IF("preserve_trx_fail_after_binlog_mode_validation",
                  return reject_after_binlog_export(
                      "debug_after_binlog_mode_validation"););
  if (reset_requested())
    return reject_after_binlog_export("batch_target_preserve_reset");

  set_stage(Preserve_trx_preserve_stage::LOCK_PREFLIGHT);
  const Preserve_trx_lock_warmcopy_options lock_warmcopy_options =
      preserve_trx_lock_warmcopy_current_options();
  const Preserve_trx_lock_warmcopy_route lock_warmcopy_route =
      preserve_trx_lock_warmcopy_route_artifact(request.lock_warmcopy_artifact,
                                                lock_warmcopy_options);
  /*
    Lock warmcopy is routed per target before any destructive preserve step.
    Unsupported transaction shapes reject immediately; stale or over-budget
    artifacts may fall back to live export only when the policy allows it.
  */
  if (lock_warmcopy_route.action ==
      Preserve_trx_lock_warmcopy_route_action::REJECT) {
    if (lock_warmcopy_options.enabled) {
      preserve_trx_lock_warmcopy_note_route_reject(
          lock_warmcopy_route.reason);
    }
    return reject_after_binlog_export(
        preserve_trx_lock_warmcopy_reason_name(lock_warmcopy_route.reason));
  }
  if (lock_warmcopy_options.enabled &&
      lock_warmcopy_route.action ==
          Preserve_trx_lock_warmcopy_route_action::FALLBACK_TO_LIVE_EXPORT) {
    preserve_trx_lock_warmcopy_note_route_fallback(lock_warmcopy_route.reason);
  }
  bool use_lock_warmcopy_artifact =
      lock_warmcopy_route.action ==
      Preserve_trx_lock_warmcopy_route_action::USE_WARM_COPY;
  const Preserve_trx_lock_warmcopy_artifact *lock_warmcopy_artifact =
      request.lock_warmcopy_artifact;

  std::string read_view_payload;
  uint64_t rv_low_limit_no = 0;
  ulonglong substep_started_us = preserve_trx_monotonic_us();
  if (trx_preserve_export_read_view(thd, &read_view_payload,
                                    &rv_low_limit_no) != DB_SUCCESS) {
    return reject_after_binlog_export("read_view_export_failed");
  }
  add_result_elapsed_us(&Preserve_trx_preserve_result::
                            lock_preflight_read_view_us,
                        substep_started_us);

  /*
    Batch drain preserves the native redo-backed transaction identity. Local
    startup rebuilds the same trx_id in rw_trx_set from Undo, while strict
    promotion must prove the equivalent physical continuity before adopt.
    Record DB_TRX_ID plus that active trx_t already represents implicit lock
    ownership, so batch artifacts serialize only explicit lock_sys state.
  */

  DEBUG_SYNC(thd, "preserve_trx_after_lock_materialization");
  DBUG_EXECUTE_IF("preserve_trx_fail_after_lock_materialization",
                  return reject_after_binlog_export(
                      "debug_after_lock_materialization"););
  if (thd->killed) {
    thd->send_kill_message();
    return reject_after_binlog_export("killed_after_lock_materialization");
  }

  std::string mdl_descriptors_payload;
  size_t mdl_descriptors_count = 0;
  auto export_live_mdl_descriptors = [&]() -> const char * {
    mdl_descriptors_payload.clear();
    mdl_descriptors_count = 0;
    if (preserve_trx_lock_warmcopy_export_mdl_descriptors(
            thd->mdl_context, &mdl_descriptors_payload,
            &mdl_descriptors_count)) {
      return "mdl_export_failed";
    }
    if (preserve_trx_recheck_mdl_object_privileges(thd,
                                                   mdl_descriptors_payload)) {
      return "mdl_privilege_recheck_failed";
    }
    return nullptr;
  };
  /*
    MDL warmcopy reuses the existing transaction-duration descriptor payload
    and validates it against the live exporter before it can replace the live
    payload.
  */
  substep_started_us = preserve_trx_monotonic_us();
  const char *mdl_live_failure = export_live_mdl_descriptors();
  if (mdl_live_failure != nullptr) {
    return reject_after_binlog_export(mdl_live_failure);
  }
  if (use_lock_warmcopy_artifact) {
    const Preserve_trx_lock_warmcopy_canonical_compare_result mdl_compare =
        preserve_trx_lock_warmcopy_compare_mdl_payloads_canonical(
            mdl_descriptors_payload,
            lock_warmcopy_artifact->mdl_descriptors_payload);
    if (!mdl_compare.equivalent ||
        mdl_descriptors_count !=
            lock_warmcopy_artifact->mdl_descriptor_count) {
      /*
        MDL is currently verified against a live descriptor snapshot. A
        mismatch invalidates the whole lock artifact; keeping record warmcopy
        while replacing only MDL would mix lock families from different points.
      */
      preserve_trx_lock_warmcopy_note_canonical_mismatch("mdl");
      if (!lock_warmcopy_options.fallback_to_live_export) {
        preserve_trx_lock_warmcopy_note_route_reject(
            Preserve_trx_lock_warmcopy_reason::
                CANONICAL_EQUIVALENCE_FAILED);
        return reject_after_binlog_export(
            preserve_trx_lock_warmcopy_reason_name(
                Preserve_trx_lock_warmcopy_reason::
                    CANONICAL_EQUIVALENCE_FAILED));
      }
      preserve_trx_lock_warmcopy_note_route_fallback(
          Preserve_trx_lock_warmcopy_reason::CANONICAL_EQUIVALENCE_FAILED);
      use_lock_warmcopy_artifact = false;
      lock_warmcopy_artifact = nullptr;
    } else {
      mdl_descriptors_payload =
          lock_warmcopy_artifact->mdl_descriptors_payload;
      mdl_descriptors_count = lock_warmcopy_artifact->mdl_descriptor_count;
    }
  }
  add_result_elapsed_us(&Preserve_trx_preserve_result::lock_preflight_mdl_us,
                        substep_started_us);
  std::vector<Preserve_modified_table_name> modified_tables;
  substep_started_us = preserve_trx_monotonic_us();
  if (trx_preserve_export_modified_table_names(
          thd, &modified_tables, preserve_trx_max_modified_tables) !=
          DB_SUCCESS ||
      preserve_trx_populate_modified_table_write_masks(thd,
                                                       &modified_tables)) {
    return reject_after_binlog_export("modified_table_export_or_privilege_failed");
  }
  add_result_elapsed_us(&Preserve_trx_preserve_result::
                            lock_preflight_modified_tables_us,
                        substep_started_us);

  std::string sql_savepoints_payload;
  uint32_t savepoint_count = 0;
  uint32_t sql_innodb_savepoint_count = 0;
  std::vector<Preserve_savepoint_participant> session_participant_order;
  std::vector<uint16_t> savepoint_suffix_ordinals;
  substep_started_us = preserve_trx_monotonic_us();
  if (export_sql_savepoints(thd, binlog_state, &sql_savepoints_payload,
                            &savepoint_count,
                            &sql_innodb_savepoint_count,
                            &session_participant_order,
                            &savepoint_suffix_ordinals)) {
    return reject_after_binlog_export("sql_savepoint_export_failed");
  }
  add_result_elapsed_us(&Preserve_trx_preserve_result::
                            lock_preflight_savepoints_us,
                        substep_started_us);
  if (result != nullptr && use_lock_warmcopy_artifact && savepoint_count != 0) {
    result->phase2_savepoint_live_export_target_count = 1;
  }
  if (reset_requested())
    return reject_after_binlog_export("batch_target_preserve_reset");

  /*
    Record-lock image export may reject row shapes that are not yet part of the
    durable identity contract. Run this preflight while the transaction is still
    attached so an unsupported PRESERVE leaves the user's transaction alive and
    committable.
  */
  std::string record_locks_preflight_payload;
  std::string table_locks_preflight_payload;
  uint32_t record_locks_preflight_count = 0;
  bool live_lock_preflight_payloads_exported = false;
  auto export_live_record_locks_preflight = [&]() -> const char * {
    record_locks_preflight_payload.clear();
    record_locks_preflight_count = 0;
    if (trx_preserve_export_record_locks(thd, &record_locks_preflight_payload,
                                         preserve_trx_max_lock_count) !=
        DB_SUCCESS) {
      const char *reason = preserve_trx_record_lock_export_failure_reason(
          "record_lock_preflight_failed");
      log_preserve_reject_reason(thd, reason);
      return reason;
    }
    if (!record_locks_preflight_payload.empty() &&
        !trx_preserve_record_locks_payload_lock_count(
            record_locks_preflight_payload, &record_locks_preflight_count)) {
      return "record_lock_count_decode_failed";
    }
    return nullptr;
  };

  /*
    Table-lock preflight. Validates that every explicit IX/IS/S/X/AUTO_INC
    table lock can be serialized and that the combined record-lock plus
    table-lock count stays within preserve_trx_max_lock_count. Done before any
    destructive step so the transaction stays committable on failure.
  */
  auto export_live_table_locks_preflight = [&]() -> const char * {
    const ulonglong table_export_started_us = preserve_trx_monotonic_us();
    auto note_table_export_elapsed = create_scope_guard([&] {
      add_result_elapsed_us(
          &Preserve_trx_preserve_result::lock_preflight_table_us,
          table_export_started_us);
    });
    table_locks_preflight_payload.clear();
    if (trx_preserve_export_table_locks(thd, &table_locks_preflight_payload,
                                        preserve_trx_max_lock_count,
                                        record_locks_preflight_count) !=
        DB_SUCCESS) {
      return "table_lock_preflight_failed";
    }
    return nullptr;
  };
  auto export_live_lock_preflight_payloads = [&]() -> const char * {
    live_lock_preflight_payloads_exported = false;
    const char *record_failure = export_live_record_locks_preflight();
    if (record_failure != nullptr) return record_failure;
    const char *table_failure = export_live_table_locks_preflight();
    if (table_failure != nullptr) return table_failure;
    live_lock_preflight_payloads_exported = true;
    return nullptr;
  };
  auto switch_lock_warmcopy_to_live_export_for_preflight =
      [&]() -> const char * {
    /*
      Fallback is all-or-live for the target. Rebuild every lock-family payload
      from the live transaction so the preserved artifact has one consistent
      source instead of mixing warmcopy record state with live table or MDL
      state.
    */
    const char *mdl_failure = export_live_mdl_descriptors();
    if (mdl_failure != nullptr) return mdl_failure;

    const char *lock_preflight_failure = export_live_lock_preflight_payloads();
    if (lock_preflight_failure != nullptr) return lock_preflight_failure;

    use_lock_warmcopy_artifact = false;
    lock_warmcopy_artifact = nullptr;
    return nullptr;
  };

  if (use_lock_warmcopy_artifact) {
    substep_started_us = preserve_trx_monotonic_us();
    bool has_predicate_locks = false;
    if (!trx_preserve_has_predicate_locks(thd, &has_predicate_locks) ||
        has_predicate_locks) {
      if (!lock_warmcopy_options.fallback_to_live_export) {
        preserve_trx_lock_warmcopy_note_route_reject(
            Preserve_trx_lock_warmcopy_reason::UNSUPPORTED_FAMILY);
        return reject_after_binlog_export(
            preserve_trx_lock_warmcopy_reason_name(
                Preserve_trx_lock_warmcopy_reason::UNSUPPORTED_FAMILY));
      }
      preserve_trx_lock_warmcopy_note_route_fallback(
          Preserve_trx_lock_warmcopy_reason::UNSUPPORTED_FAMILY);
      const char *fallback_failure =
          switch_lock_warmcopy_to_live_export_for_preflight();
      if (fallback_failure != nullptr) {
        return reject_after_binlog_export(fallback_failure);
      }
    }
    add_result_elapsed_us(&Preserve_trx_preserve_result::
                              lock_preflight_predicate_us,
                          substep_started_us);
  }

  if (use_lock_warmcopy_artifact) {
    /*
      The production warmcopy path trusts the sealed artifact plus final fences.
      Re-exporting every explicit record lock here would put the phase-2 pause
      back on the same O(lock count) path as live export. Debug/test builds may
      opt into the canonical comparator to prove equivalence without making it
      part of the default runtime contract.
    */
    record_locks_preflight_count = lock_warmcopy_artifact->record_lock_count;
    if (lock_warmcopy_options.validate_canonical_equivalence) {
      const char *record_failure = export_live_record_locks_preflight();
      if (record_failure != nullptr) {
        return reject_after_binlog_export(record_failure);
      }
      const Preserve_trx_lock_warmcopy_canonical_compare_result record_compare =
          preserve_trx_lock_warmcopy_compare_record_payloads_canonical(
              record_locks_preflight_payload,
              lock_warmcopy_artifact->record_locks_payload);
      if (!record_compare.equivalent) {
        DBUG_EXECUTE_IF(
            "preserve_trx_lock_warmcopy_log_record_compare",
            {
              const std::string message =
                  "PRESERVE: lock warmcopy record canonical mismatch"
                  " difference=" +
                  record_compare.difference +
                  " live_bytes=" +
                  std::to_string(record_locks_preflight_payload.size()) +
                  " warmcopy_bytes=" +
                  std::to_string(
                      lock_warmcopy_artifact->record_locks_payload.size()) +
                  " live_count=" +
                  std::to_string(record_locks_preflight_count) +
                  " warmcopy_count=" +
                  std::to_string(lock_warmcopy_artifact->record_lock_count);
              LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
            });
        preserve_trx_lock_warmcopy_note_canonical_mismatch("record");
        if (!lock_warmcopy_options.fallback_to_live_export) {
          preserve_trx_lock_warmcopy_note_route_reject(
              Preserve_trx_lock_warmcopy_reason::
                  CANONICAL_EQUIVALENCE_FAILED);
          return reject_after_binlog_export(
              preserve_trx_lock_warmcopy_reason_name(
                  Preserve_trx_lock_warmcopy_reason::
                      CANONICAL_EQUIVALENCE_FAILED));
        }
        preserve_trx_lock_warmcopy_note_route_fallback(
            Preserve_trx_lock_warmcopy_reason::CANONICAL_EQUIVALENCE_FAILED);
        const char *fallback_failure =
            switch_lock_warmcopy_to_live_export_for_preflight();
        if (fallback_failure != nullptr) {
          return reject_after_binlog_export(fallback_failure);
        }
      }
    }
  }

  if (use_lock_warmcopy_artifact) {
    if (lock_warmcopy_artifact->record_predicate_table_lock_count >
        preserve_trx_max_lock_count) {
      preserve_trx_lock_warmcopy_note_route_reject(
          Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED);
      return reject_after_binlog_export(
          preserve_trx_lock_warmcopy_reason_name(
              Preserve_trx_lock_warmcopy_reason::RESOURCE_LIMIT_EXCEEDED));
    }
    record_locks_preflight_count = lock_warmcopy_artifact->record_lock_count;
    const char *table_failure = export_live_table_locks_preflight();
    if (table_failure != nullptr) {
      return reject_after_binlog_export(table_failure);
    }
    const Preserve_trx_lock_warmcopy_canonical_compare_result table_compare =
        preserve_trx_lock_warmcopy_compare_table_payloads_canonical(
            table_locks_preflight_payload,
            lock_warmcopy_artifact->table_locks_payload);
    if (!table_compare.equivalent ||
        trx_preserve_table_locks_payload_has_autoinc(
            lock_warmcopy_artifact->table_locks_payload) !=
            lock_warmcopy_artifact->autoinc_lock_owned) {
      preserve_trx_lock_warmcopy_note_canonical_mismatch("table");
      if (!lock_warmcopy_options.fallback_to_live_export) {
        preserve_trx_lock_warmcopy_note_route_reject(
            Preserve_trx_lock_warmcopy_reason::
                CANONICAL_EQUIVALENCE_FAILED);
        return reject_after_binlog_export(
            preserve_trx_lock_warmcopy_reason_name(
                Preserve_trx_lock_warmcopy_reason::
                    CANONICAL_EQUIVALENCE_FAILED));
      }
      preserve_trx_lock_warmcopy_note_route_fallback(
          Preserve_trx_lock_warmcopy_reason::CANONICAL_EQUIVALENCE_FAILED);
      const char *fallback_failure =
          switch_lock_warmcopy_to_live_export_for_preflight();
      if (fallback_failure != nullptr) {
        return reject_after_binlog_export(fallback_failure);
      }
    }
  } else {
    if (!live_lock_preflight_payloads_exported) {
      const char *lock_preflight_failure = export_live_lock_preflight_payloads();
      if (lock_preflight_failure != nullptr) {
        return reject_after_binlog_export(lock_preflight_failure);
      }
    }
  }

  Preserve_trx_token_selection token_selection;
  if (preserve_trx_select_token_for_request(thd, artifact_decision,
                                            request.preselected_token,
                                            &token_selection)) {
    return reject_after_binlog_export(
        token_selection.failure_reason == nullptr
            ? "token_generation_failed"
            : token_selection.failure_reason);
  }
  std::string token = token_selection.preserve_token_string;
  if (result != nullptr) result->token = token;

  XID xid;
  if (preserve_trx_token_to_xid(token, &xid))
    return reject_after_binlog_export("token_to_xid_failed");

  auto restore_batch_prepare_failure_or_rollback = [&]() {
    if (reactivate_current_batch_prepared_failure_to_original_thd(thd)) {
      if (result != nullptr) result->cleanup_failed_after_reattach = true;
      return reject_unsupported_for_delivery();
    }
    if (has_logged_binlog_cache &&
        mysql_binlog_preserve_reactivate_after_prepare_failure(
            thd, binlog_snapshot)) {
      if (result != nullptr) {
        result->reattached_to_original_thd = true;
        result->cleanup_failed_after_reattach = true;
      }
      return reject_unsupported_for_delivery();
    }
    if (result != nullptr) result->reattached_to_original_thd = true;
    return reject_unsupported_for_delivery();
  };
  auto restore_unprepared_batch_prepare_failure_or_rollback = [&]() {
    reset_preserve_xid_to_active_transaction_xid(thd);
    thd->get_transaction()->reset_unsafe_rollback_flags(
        Transaction_ctx::STMT);
    thd->get_transaction()->reset_scope(Transaction_ctx::STMT);
    if (has_logged_binlog_cache &&
        mysql_binlog_preserve_reactivate_after_prepare_failure(
            thd, binlog_snapshot)) {
      if (result != nullptr) {
        result->reattached_to_original_thd = true;
        result->cleanup_failed_after_reattach = true;
      }
      return reject_unsupported_for_delivery();
    }
    if (result != nullptr) result->reattached_to_original_thd = true;
    return reject_unsupported_for_delivery();
  };

  set_stage(Preserve_trx_preserve_stage::UNDO_PREPARE);
  DEBUG_SYNC(thd, "preserve_trx_before_undo_prepare");
  if (thd->killed) {
    thd->send_kill_message();
    return restore_unprepared_batch_prepare_failure_or_rollback();
  }
  if (reset_requested()) {
    DEBUG_SYNC(thd, "preserve_trx_batch_cancel_before_prepare");
    set_failure_reason("batch_target_preserve_reset");
    return restore_unprepared_batch_prepare_failure_or_rollback();
  }

  *thd->get_transaction()->xid_state()->get_xid() = xid;
  bool prepare_failed =
      request.debug_fail_ha_prepare_low;
  DBUG_EXECUTE_IF("pfx_prepare_low", { prepare_failed = true; });
  if (prepare_failed) {
    return restore_unprepared_batch_prepare_failure_or_rollback();
  }

  trx_t *lock_warmcopy_frozen_trx = nullptr;
  lock_warmcopy_trx_lock_fence_t lock_warmcopy_frozen_fence;
  bool lock_warmcopy_frozen_fence_valid = false;
  auto thaw_lock_warmcopy_conversion = [&]() {
    if (lock_warmcopy_frozen_trx != nullptr) {
      trx_preserve_lock_warmcopy_conversion_thaw(lock_warmcopy_frozen_trx);
      lock_warmcopy_frozen_trx = nullptr;
      lock_warmcopy_frozen_fence_valid = false;
    }
  };
  auto switch_lock_warmcopy_to_live_export_before_prepare =
      [&]() -> const char * {
    /*
      Final-fence fallback still happens before engine freeze. It must
      release the conversion freeze and rebuild all lock payloads before
      prepare observes the final state.
    */
    const char *mdl_failure = export_live_mdl_descriptors();
    if (mdl_failure != nullptr) return mdl_failure;

    const char *lock_preflight_failure = export_live_lock_preflight_payloads();
    if (lock_preflight_failure != nullptr) return lock_preflight_failure;

    use_lock_warmcopy_artifact = false;
    lock_warmcopy_artifact = nullptr;
    return nullptr;
  };
  auto verify_lock_warmcopy_frozen_fence = [&]() -> bool {
    if (!use_lock_warmcopy_artifact) return true;

    /*
      The frozen fence is a narrow race detector for engine-side
      implicit-to-explicit conversion after the artifact was sealed. Any change
      before freeze means the warmcopy payload is no longer authoritative.
    */
    lock_warmcopy_trx_lock_fence_t current_frozen_fence;
    return lock_warmcopy_frozen_fence_valid &&
           trx_preserve_sample_lock_warmcopy_fence(lock_warmcopy_frozen_trx,
                                                   &current_frozen_fence) &&
           lock_warmcopy_trx_lock_fence_equal(lock_warmcopy_frozen_fence,
                                              current_frozen_fence);
  };
  auto inject_lock_warmcopy_conversion_after_freeze = [&]() {
    (void)trx_preserve_lock_warmcopy_note_conversion_attempt_after_freeze(
        lock_warmcopy_frozen_trx);
  };
  (void)inject_lock_warmcopy_conversion_after_freeze;
  auto handle_lock_warmcopy_final_fence_failure =
      [&](Preserve_trx_lock_warmcopy_reason reason,
          const char *strict_failure_reason) -> bool {
    /*
      Final-fence failure is the last point where live fallback is still safe.
      After engine freeze, preserve must either continue with the chosen
      payload or run the prepared cleanup path; it cannot rebuild lock state.
    */
    preserve_trx_lock_warmcopy_note_final_fence_mismatch();
    const Preserve_trx_lock_warmcopy_route route =
        preserve_trx_lock_warmcopy_route_final_fence(reason,
                                                     lock_warmcopy_options);
    if (route.action ==
        Preserve_trx_lock_warmcopy_route_action::FALLBACK_TO_LIVE_EXPORT) {
      preserve_trx_lock_warmcopy_note_route_fallback(reason);
      thaw_lock_warmcopy_conversion();
      const char *fallback_failure =
          switch_lock_warmcopy_to_live_export_before_prepare();
      if (fallback_failure != nullptr) {
        set_failure_reason(fallback_failure);
        return false;
      }
      return true;
    }

    preserve_trx_lock_warmcopy_note_route_reject(reason);
    thaw_lock_warmcopy_conversion();
    set_failure_reason(strict_failure_reason != nullptr
                           ? strict_failure_reason
                           : preserve_trx_lock_warmcopy_reason_name(reason));
    return false;
  };
  if (use_lock_warmcopy_artifact) {
    if (!trx_preserve_lock_warmcopy_conversion_freeze(
            thd, &lock_warmcopy_frozen_fence, &lock_warmcopy_frozen_trx)) {
      if (!handle_lock_warmcopy_final_fence_failure(
              Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID,
              "lock_warmcopy_conversion_freeze_failed")) {
        return restore_unprepared_batch_prepare_failure_or_rollback();
      }
    } else {
      lock_warmcopy_frozen_fence_valid = true;
    }
    if (use_lock_warmcopy_artifact) {
      /*
        The warmcopy artifact is now tied to this trx-level conversion freeze.
        Any other session that tries to materialize one of this transaction's
        implicit locks before metadata is populated must update the frozen fence
        and force fail-closed handling below.
      */
      DEBUG_SYNC(thd, "preserve_trx_lock_warmcopy_after_conversion_freeze");
      DBUG_EXECUTE_IF(
          "preserve_trx_lock_warmcopy_simulate_conversion_after_freeze", {
            (void)trx_preserve_lock_warmcopy_note_conversion_attempt_after_freeze(
                lock_warmcopy_frozen_trx);
      });
    }
    if (use_lock_warmcopy_artifact) {
      DEBUG_SYNC(thd, "preserve_trx_lock_warmcopy_after_final_fence");
      if (!verify_lock_warmcopy_frozen_fence() &&
          !handle_lock_warmcopy_final_fence_failure(
              Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED,
              preserve_trx_lock_warmcopy_reason_name(
                  Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED))) {
        return restore_unprepared_batch_prepare_failure_or_rollback();
      }
    }
    if (use_lock_warmcopy_artifact) {
      const Preserve_trx_lock_warmcopy_reason final_fence_reason =
          preserve_trx_lock_warmcopy_verify_record_final_fence(
              *lock_warmcopy_artifact, lock_warmcopy_frozen_fence);
      if (final_fence_reason != Preserve_trx_lock_warmcopy_reason::OK &&
          !handle_lock_warmcopy_final_fence_failure(
              final_fence_reason,
              preserve_trx_lock_warmcopy_reason_name(final_fence_reason))) {
        return restore_unprepared_batch_prepare_failure_or_rollback();
      }
    }
  }
  if (use_lock_warmcopy_artifact) {
    std::string current_sql_savepoints_payload;
    uint32_t current_savepoint_count = 0;
    uint32_t current_sql_innodb_savepoint_count = 0;
    std::vector<Preserve_savepoint_participant>
        current_session_participant_order;
    std::vector<uint16_t> current_savepoint_suffix_ordinals;
    if (export_sql_savepoints(thd, binlog_state,
                              &current_sql_savepoints_payload,
                              &current_savepoint_count,
                              &current_sql_innodb_savepoint_count,
                              &current_session_participant_order,
                              &current_savepoint_suffix_ordinals)) {
      thaw_lock_warmcopy_conversion();
      set_failure_reason("savepoint_final_fence_export_failed");
      return restore_unprepared_batch_prepare_failure_or_rollback();
    }
    DBUG_EXECUTE_IF(
        "preserve_trx_lock_warmcopy_force_savepoint_final_fence_changed",
        { current_sql_savepoints_payload.push_back('\1'); });
    if (current_savepoint_count != savepoint_count ||
        current_sql_innodb_savepoint_count != sql_innodb_savepoint_count ||
        current_sql_savepoints_payload != sql_savepoints_payload ||
        current_session_participant_order != session_participant_order ||
        current_savepoint_suffix_ordinals != savepoint_suffix_ordinals) {
      /*
        Savepoint changes can alter rollback-to-savepoint lock ownership. Until
        savepoint warmcopy has its own generation hook, a changed final payload
        must fail before freeze instead of preserving stale lock semantics.
      */
      thaw_lock_warmcopy_conversion();
      set_failure_reason("savepoint_final_fence_changed");
      return restore_unprepared_batch_prepare_failure_or_rollback();
    }
  }
  if (trx_preserve_freeze_current(thd, xid) != DB_SUCCESS) {
    thaw_lock_warmcopy_conversion();
    return restore_batch_prepare_failure_or_rollback();
  }
  /*
    Keep the per-trx conversion freeze beyond engine freeze. The lock
    payload contract is not fully frozen until the transaction is detached,
    claimed, and the preserved lock metadata below has been populated.
  */
  bool temp_prepare_failed = request.debug_fail_temp_only_prepare;
  DBUG_EXECUTE_IF("pfx_temp_prepare", { temp_prepare_failed = true; });
  if (temp_prepare_failed) {
    thaw_lock_warmcopy_conversion();
    return restore_batch_prepare_failure_or_rollback();
  }
  trx_preserve_resurrection_facts resurrection_facts;
  bool has_resurrection_facts = false;
  if (result != nullptr) result->durable_point_crossed = true;
  auto restore_prepared_batch_or_rollback = [&]() {
    trx_preserve_thd_transition_failure reactivate_failure =
        trx_preserve_thd_transition_failure::NONE;
    if (reactivate_current_batch_prepared_failure_to_original_thd(
            thd, &reactivate_failure)) {
      if (result != nullptr) {
        result->cleanup_failed_after_reattach = true;
        result->reactivate_failure_reason =
            trx_preserve_thd_transition_failure_name(reactivate_failure);
      }
      return reject_unsupported_for_delivery();
    }
    if (has_logged_binlog_cache &&
        mysql_binlog_preserve_reactivate_after_prepare_failure(
            thd, binlog_snapshot)) {
      if (result != nullptr) {
        result->reattached_to_original_thd = true;
        result->cleanup_failed_after_reattach = true;
      }
      return reject_unsupported_for_delivery();
    }
    if (result != nullptr) result->reattached_to_original_thd = true;
    return reject_unsupported_for_delivery();
  };
  DEBUG_SYNC(thd, "preserve_trx_after_undo_prepared_before_kill_check");
  if (thd->killed == THD::KILL_QUERY) {
    thd->killed = THD::NOT_KILLED;
  }
  DEBUG_SYNC(thd, "preserve_trx_after_undo_prepared");
  DBUG_EXECUTE_IF("preserve_trx_crash_after_undo_prepared_before_snapshot",
                  DBUG_SUICIDE(););
  DBUG_EXECUTE_IF("preserve_trx_fail_after_undo_prepared", {
    thaw_lock_warmcopy_conversion();
    return restore_prepared_batch_or_rollback();
  });
  if (thd->killed == THD::KILL_CONNECTION) {
    thaw_lock_warmcopy_conversion();
    return restore_prepared_batch_or_rollback();
  }
  if (thd->killed == THD::KILL_QUERY) thd->killed = THD::NOT_KILLED;
  if (reset_requested()) {
    DEBUG_SYNC(thd, "preserve_trx_batch_cancel_after_prepare");
    set_failure_reason("batch_target_preserve_reset");
    thaw_lock_warmcopy_conversion();
    return restore_prepared_batch_or_rollback();
  }

  set_stage(Preserve_trx_preserve_stage::DETACH);
  trx_preserve_thd_transition_failure detach_failure =
      trx_preserve_thd_transition_failure::NONE;
  trx_t *trx = trx_preserve_detach_current_thd(thd, &detach_failure);
  if (trx == nullptr) {
    if (result != nullptr) {
      result->detach_failure_reason =
          trx_preserve_thd_transition_failure_name(detach_failure);
    }
    thaw_lock_warmcopy_conversion();
    return restore_prepared_batch_or_rollback();
  }
  if (result != nullptr) result->detached_from_original_thd = true;

  has_resurrection_facts =
      trx_preserve_export_resurrection_facts(
          trx, token, preserve_trx_max_modified_tables, &resurrection_facts) ==
      DB_SUCCESS;
  if (artifact_decision ==
          Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE &&
      !has_resurrection_facts) {
    thaw_lock_warmcopy_conversion();
    set_failure_reason("standby_transfer_resurrection_facts_unsupported");
    return restore_prepared_batch_or_rollback();
  }

  /*
    Detach moves the frozen transaction out of the session. Until it is
    claimed and the snapshot is durable, every later failure must either
    reattach it to the original THD or roll it back under the preserve token.
  */
  DBUG_EXECUTE_IF(
      "preserve_trx_assert_read_view_pinned_after_detach",
      if (trx_preserve_trx_has_read_view(trx)) {
        LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
               "PRESERVE: read view pinned after detach");
      } else {
        LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
               "PRESERVE: read view missing after detach");
      });

  auto rollback_claimed_after_detach_failure = [&]() {
    const dberr_t rollback_status = trx_preserve_rollback_claimed(trx);
    reset_thd_after_preserve_detach(thd);
    cleanup_original_binlog_cache_after_detach(thd, has_logged_binlog_cache);
    if (result != nullptr) {
      result->cleanup_completed_after_detach_failure =
          rollback_status == DB_SUCCESS;
      result->cleanup_failed_after_reattach =
          rollback_status != DB_SUCCESS;
    }
    return reject_unsupported_for_delivery();
  };

  auto batch_reattached_after_detach_failure = [&]() {
    bool cleanup_failed = false;
    if (reattach_current_batch_preserve_failure_to_original_thd(
            thd, trx, token, false, false, false, &cleanup_failed, nullptr,
            nullptr, has_logged_binlog_cache, &binlog_snapshot))
      return false;

    if (result != nullptr) {
      result->reattached_to_original_thd = true;
      result->cleanup_failed_after_reattach = cleanup_failed;
    }
    return true;
  };

  auto reject_after_detach_failure_or_rollback = [&]() {
    if (batch_reattached_after_detach_failure())
      return reject_unsupported_for_delivery();
    return rollback_claimed_after_detach_failure();
  };

  DEBUG_SYNC(thd, "preserve_trx_after_detach_before_claim");
  if (reset_requested()) {
    DEBUG_SYNC(thd, "preserve_trx_batch_cancel_after_detach");
    set_failure_reason("batch_target_preserve_reset");
    thaw_lock_warmcopy_conversion();
    return reject_after_detach_failure_or_rollback();
  }

  if (trx_preserve_claim_detached_active_undo(trx) != DB_SUCCESS) {
    thaw_lock_warmcopy_conversion();
    if (batch_reattached_after_detach_failure())
      return reject_unsupported_for_delivery();
    const dberr_t rollback_status =
        trx_preserve_rollback_by_token(token.c_str());
    reset_thd_after_preserve_detach(thd);
    cleanup_original_binlog_cache_after_detach(thd, has_logged_binlog_cache);
    if (result != nullptr) {
      result->cleanup_completed_after_detach_failure =
          rollback_status == DB_SUCCESS;
      result->cleanup_failed_after_reattach =
          rollback_status != DB_SUCCESS;
    }
    return reject_unsupported_for_delivery();
  }

  DBUG_EXECUTE_IF("preserve_trx_fail_after_detach_for_batch_reattach", {
    thaw_lock_warmcopy_conversion();
    return reject_after_detach_failure_or_rollback();
  });

  Preserve_snapshot_metadata metadata =
      make_no_cache_metadata(thd, token, binlog_state);
  /*
    Metadata is assembled only after the ACTIVE-Undo transaction is claimed.
    Values exported before freeze are copied in deliberately below when their
    semantics must reflect the live user transaction.
  */
  metadata.mod_tables_count = trx_preserve_modified_table_count(trx);
  metadata.modified_table_names.reserve(modified_tables.size());
  for (const Preserve_modified_table_name &name : modified_tables) {
    metadata.modified_table_names.push_back(
        {name.schema_name, name.table_name, name.required_write_acls});
  }
  metadata.autoinc_lock_owned =
      use_lock_warmcopy_artifact
          ? lock_warmcopy_artifact->autoinc_lock_owned
          : trx_preserve_trx_has_autoinc_locks(trx);
  std::string innodb_savepoints_payload;
  if (trx_preserve_export_savepoints(trx, &innodb_savepoints_payload) !=
      DB_SUCCESS) {
    thaw_lock_warmcopy_conversion();
    return reject_after_detach_failure_or_rollback();
  }
  uint32_t innodb_savepoint_count = 0;
  if (!trx_preserve_savepoints_payload_is_valid_for_import(
          innodb_savepoints_payload, &innodb_savepoint_count) ||
      innodb_savepoint_count != sql_innodb_savepoint_count) {
    thaw_lock_warmcopy_conversion();
    return reject_after_detach_failure_or_rollback();
  }
  /*
    Use the pre-prepare record-lock snapshot as the durable lock contract.
    InnoDB XA prepare may release gap locks for RC-style transactions; PRESERVE
    must keep the user-visible transaction semantics from before the durable
    prepare step, including next-key/gap locks that protect future inserts.
  */
  uint32_t record_locks_count = record_locks_preflight_count;
  if (use_lock_warmcopy_artifact) {
    if (lock_warmcopy_artifact->has_prebuilt_record_locks_blob) {
      metadata.record_locks_payload.clear();
    } else {
      metadata.record_locks_payload =
          lock_warmcopy_artifact->record_locks_payload;
    }
    metadata.predicate_locks_payload =
        lock_warmcopy_artifact->predicate_locks_payload;
  } else {
    const std::string &exported_record_locks_payload =
        record_locks_preflight_payload;
    if (!split_record_and_predicate_locks_payload(
            exported_record_locks_payload, &metadata.record_locks_payload,
            &metadata.predicate_locks_payload)) {
      thaw_lock_warmcopy_conversion();
      return reject_after_detach_failure_or_rollback();
    }
  }
  if (use_lock_warmcopy_artifact) {
    std::string prepared_table_locks_payload;
    if (trx_preserve_export_table_locks(
            trx, &prepared_table_locks_payload, preserve_trx_max_lock_count,
            record_locks_count) != DB_SUCCESS) {
      thaw_lock_warmcopy_conversion();
      return reject_after_detach_failure_or_rollback();
    }
    std::string warmcopy_table_locks_payload_for_compare =
        lock_warmcopy_artifact->table_locks_payload;
    DBUG_EXECUTE_IF(
        "preserve_trx_lock_warmcopy_simulate_table_post_prepare_drift",
        { warmcopy_table_locks_payload_for_compare.push_back('\1'); });
    const Preserve_trx_lock_warmcopy_canonical_compare_result table_compare =
        preserve_trx_lock_warmcopy_compare_table_payloads_canonical(
            prepared_table_locks_payload,
            warmcopy_table_locks_payload_for_compare);
    if (!table_compare.equivalent ||
        trx_preserve_table_locks_payload_has_autoinc(
            warmcopy_table_locks_payload_for_compare) !=
            lock_warmcopy_artifact->autoinc_lock_owned) {
      /*
        Post-prepare table drift cannot be repaired by replacing only the table
        family with live export. Reject or fall back before prepare when
        possible; after prepare this path must roll back or reattach as a whole.
      */
      preserve_trx_lock_warmcopy_note_canonical_mismatch("table_post_prepare");
      preserve_trx_lock_warmcopy_note_route_reject(
          Preserve_trx_lock_warmcopy_reason::TABLE_POST_PREPARE_DRIFT);
      thaw_lock_warmcopy_conversion();
      set_failure_reason(
          preserve_trx_lock_warmcopy_reason_name(
              Preserve_trx_lock_warmcopy_reason::TABLE_POST_PREPARE_DRIFT));
      return reject_after_detach_failure_or_rollback();
    } else {
      metadata.table_locks_payload = lock_warmcopy_artifact->table_locks_payload;
    }
  } else {
    if (trx_preserve_export_table_locks(
            trx, &metadata.table_locks_payload, preserve_trx_max_lock_count,
            record_locks_count) != DB_SUCCESS) {
      thaw_lock_warmcopy_conversion();
      return reject_after_detach_failure_or_rollback();
    }
  }
  /* Cross-check: the table-locks payload and the derived autoinc_lock_owned
  flag must agree. metadata.autoinc_lock_owned was captured up-front; any
  divergence here means the trx mutated its autoinc-lock state between the
  capture and the table-lock export, which preserve cannot tolerate. */
  if (!metadata.table_locks_payload.empty() &&
      trx_preserve_table_locks_payload_has_autoinc(
          metadata.table_locks_payload) != metadata.autoinc_lock_owned) {
    thaw_lock_warmcopy_conversion();
    return reject_after_detach_failure_or_rollback();
  }
  if (use_lock_warmcopy_artifact) {
    DEBUG_SYNC(thd, "preserve_trx_lock_warmcopy_after_lock_metadata_guarded");
  }
  if (use_lock_warmcopy_artifact && !verify_lock_warmcopy_frozen_fence()) {
    preserve_trx_lock_warmcopy_note_final_fence_mismatch();
    preserve_trx_lock_warmcopy_note_route_reject(
        Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED);
    thaw_lock_warmcopy_conversion();
    set_failure_reason("lock_warmcopy_conversion_freeze_changed");
    return reject_after_detach_failure_or_rollback();
  }
  metadata.has_read_view = !read_view_payload.empty();
  metadata.rv_low_limit_no = rv_low_limit_no;
  metadata.read_view_payload = std::move(read_view_payload);
  metadata.mdl_descriptors_payload = std::move(mdl_descriptors_payload);
  if (options.user_vars_mode == Preserve_trx_user_vars_mode::INCLUDE &&
      export_user_vars_payload(thd, &metadata.user_vars_payload)) {
    thaw_lock_warmcopy_conversion();
    return reject_after_detach_failure_or_rollback();
  }
  metadata.savepoint_count = savepoint_count;
  metadata.sql_savepoints_payload = std::move(sql_savepoints_payload);
  metadata.innodb_savepoints_payload = std::move(innodb_savepoints_payload);
  metadata.session_participant_order = std::move(session_participant_order);
  metadata.savepoint_suffix_ordinals =
      std::move(savepoint_suffix_ordinals);

  if (create_detached_mdl_context(thd, token)) {
    thaw_lock_warmcopy_conversion();
    return reject_after_detach_failure_or_rollback();
  }

  std::vector<Preserved_trx_external_blob_descriptor> blob_descriptors;
  std::unique_ptr<Preserve_trx_source_rollback_image> source_rollback_image;

  auto reject_after_snapshot_failure =
      [&](bool snapshot_files_may_exist,
          Preserve_snapshot_delete_status write_failure_delete_status =
              Preserve_snapshot_delete_status::OK) {
    /*
      Snapshot-write failure happens after detach. Batch delivery first tries to
      reattach the transaction to its original session; if the snapshot may be
      durable, cleanup must either leave a registered observable record or mark
      the token tainted for recovery.
    */
    thaw_lock_warmcopy_conversion();
    const bool effective_snapshot_files_may_exist =
        snapshot_files_may_exist ||
        write_failure_delete_status ==
            Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
    if (effective_snapshot_files_may_exist &&
        terminalize_local_snapshot_authority_if_present(
            token, "source preserve failure revoked local authority")) {
      if (result != nullptr) {
        result->left_preserved_after_cleanup_failure = true;
        result->cleanup_failed_after_reattach = true;
      }
      return reject_unsupported_for_delivery();
    }
    bool cleanup_failed = false;
    bool left_preserved = false;
    const bool store_cleanup_failed =
        write_failure_delete_status != Preserve_snapshot_delete_status::OK;
    if (write_failure_delete_status ==
        Preserve_snapshot_delete_status::ERROR_AFTER_SNAPSHOT_DELETE) {
      log_preserved_trx_cleanup_failure(
          token,
          "failed to fsync preserved transaction directory after unlink");
    }
    Mysql_binlog_preserve_snapshot rollback_binlog_snapshot = binlog_snapshot;
    Preserve_memory_lease rollback_binlog_payload_lease;
    if (has_logged_binlog_cache && source_rollback_image != nullptr) {
      Preserve_snapshot_metadata rollback_metadata;
      rollback_metadata.token = token;
      rollback_metadata.binlog_state =
          Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE;
      rollback_metadata.binlog_cache_size =
          source_rollback_image->prebuilt_binlog_blob.size;
      rollback_metadata.binlog_cache_event_counter =
          source_rollback_image->binlog_snapshot.event_counter;
      Preserved_trx_record rollback_record;
      rollback_record.metadata = std::move(rollback_metadata);
      if (hydrate_logged_binlog_cache_payload_if_needed(
              &rollback_record, token, source_rollback_image.get(),
              &rollback_binlog_payload_lease)) {
        set_failure_reason("source_rollback_binlog_hydrate_failed");
      } else {
        rollback_binlog_snapshot.cache_payload =
            std::move(rollback_record.metadata.binlog_cache_payload);
      }
    }
    if (!reattach_current_batch_preserve_failure_to_original_thd(
            thd, trx, token, true, effective_snapshot_files_may_exist, false,
            &cleanup_failed, &left_preserved, &metadata,
            has_logged_binlog_cache, &rollback_binlog_snapshot,
            &blob_descriptors)) {
      if (left_preserved) {
        reset_thd_after_preserve_detach(thd);
        cleanup_original_binlog_cache_after_detach(thd, has_logged_binlog_cache);
      }
      if (result != nullptr) {
        result->reattached_to_original_thd = !left_preserved;
        result->left_preserved_after_cleanup_failure = left_preserved;
        result->cleanup_failed_after_reattach =
            cleanup_failed || store_cleanup_failed;
      }
      return reject_unsupported_for_delivery();
    }
    if (cleanup_failed) {
      preserved_trx_add_failed_observable_record(
          metadata, "batch cleanup reattach failure");
      if (result != nullptr) result->cleanup_failed_after_reattach = true;
    }
    delete_detached_mdl_context(token);
    const dberr_t rollback_status = trx_preserve_rollback_claimed(trx);
    if (rollback_status != DB_SUCCESS &&
        trx_preserve_is_active_attached_to_thd(trx, thd)) {
      if (result != nullptr) {
        result->reattached_to_original_thd = true;
        result->cleanup_failed_after_reattach = true;
      }
      preserve_trx_set_batch_state(thd, 0, Preserve_trx_batch_thd_state::NONE);
      return reject_unsupported_for_delivery();
    }
    if (result != nullptr) {
      result->cleanup_completed_after_detach_failure = true;
      result->cleanup_failed_after_reattach =
          result->cleanup_failed_after_reattach ||
          rollback_status != DB_SUCCESS;
    }
    preserved_trx_add_failed_observable_record(
        metadata, "batch cleanup detached transaction fallback");
    const bool cleanup_artifacts_may_exist =
        effective_snapshot_files_may_exist ||
        !metadata.temp_table_manifest_payload.empty();
    if (cleanup_artifacts_may_exist) {
      const bool delete_failed =
          delete_preserved_snapshot_files_and_sidecars_or_log(
              preserve_trx_default_dir(), token, &metadata);
      if (delete_failed) {
        auto store =
            create_preserved_trx_default_store(preserve_trx_default_dir());
        if (store->mark_tainted(token, "snapshot write cleanup failure") !=
            Preserve_snapshot_status::OK) {
          log_preserved_trx_cleanup_failure(
              token,
              "failed to taint snapshot after snapshot write cleanup failure");
        }
        if (result != nullptr) result->cleanup_failed_after_reattach = true;
      }
    }
    reset_thd_after_preserve_detach(thd);
    cleanup_original_binlog_cache_after_detach(thd, has_logged_binlog_cache);
    preserve_trx_kill_connection(thd);
    return reject_unsupported_for_delivery();
  };

  if (reset_requested()) {
    set_failure_reason("batch_target_preserve_reset");
    return reject_after_snapshot_failure(false);
  }

  set_stage(Preserve_trx_preserve_stage::SNAPSHOT_WRITE);
  Preserved_trx_observable_state_guard snapshotting_state(
      metadata, trx, Preserved_trx_lifecycle_state::SNAPSHOTTING);

  bool prebuilt_binlog_blob_finalized = false;
  auto discard_prebuilt_binlog_blob_if_needed = [&]() {
    /*
      A prebuilt binlog blob is still a warm artifact until store->write adopts
      it under the final token. If snapshot build fails before adoption, remove
      the warm artifact through its provider.
    */
    if (use_prebuilt_binlog_cache && prebuilt_binlog_blob_finalized) {
      binlog_blob_provider->discard_for_preserve(thd, token,
                                                 prebuilt_binlog_blob);
      prebuilt_binlog_blob_finalized = false;
    }
  };

  if (use_prebuilt_binlog_cache) {
    substep_started_us = preserve_trx_monotonic_us();
    prebuilt_binlog_blob.metadata = binlog_snapshot;
    const Preserve_snapshot_status provider_status =
        binlog_blob_provider->finalize_for_preserve(thd, token,
                                                    &prebuilt_binlog_blob);
    add_result_elapsed_us(&Preserve_trx_preserve_result::
                              snapshot_write_prebuilt_binlog_us,
                          substep_started_us);
    if (provider_status != Preserve_snapshot_status::OK) {
      set_failure_reason("warmcopy_blob_finalize_failed");
      return reject_after_snapshot_failure(false);
    }
    prebuilt_binlog_blob_finalized = true;
  }

  if (has_logged_binlog_cache &&
      artifact_decision ==
          Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE &&
      use_prebuilt_binlog_cache && prebuilt_binlog_blob_finalized) {
    source_rollback_image.reset(
        new (std::nothrow) Preserve_trx_source_rollback_image());
    if (source_rollback_image == nullptr) {
      discard_prebuilt_binlog_blob_if_needed();
      return reject_after_snapshot_failure(false);
    }
    source_rollback_image->binlog_snapshot = binlog_snapshot;
    source_rollback_image->preserve_dir = preserve_trx_default_dir();
    if (use_prebuilt_binlog_cache && prebuilt_binlog_blob_finalized) {
      source_rollback_image->prebuilt_binlog_blob = prebuilt_binlog_blob;
      source_rollback_image->has_prebuilt_binlog_blob = true;
    }
  }

  substep_started_us = preserve_trx_monotonic_us();
  const Preserve_snapshot_status temp_manifest_status =
      preserve_trx_temp_table_build_preserve_manifest(
          thd, trx, preserve_trx_default_dir(), token, &metadata);
  add_result_elapsed_us(&Preserve_trx_preserve_result::
                            snapshot_write_temp_manifest_us,
                        substep_started_us);
  if (temp_manifest_status != Preserve_snapshot_status::OK) {
    const std::string temp_reason =
        preserve_trx_temp_table_degraded_reason(thd);
    if (!temp_reason.empty()) {
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: temp-table manifest build rejected preserve: " +
              temp_reason)
                 .c_str());
    }
    set_failure_reason("temp_table_manifest_failed");
    discard_prebuilt_binlog_blob_if_needed();
    return reject_after_snapshot_failure(false);
  }
  if (result != nullptr) {
    result->snapshot_write_temp_manifest_built =
        !metadata.temp_table_manifest_payload.empty();
  }

  bool has_persistent_engine_update = false;
  bool has_temp_engine_update = false;
  if (!trx_preserve_engine_state_facts(trx, &has_persistent_engine_update,
                                       &has_temp_engine_update)) {
    set_failure_reason("engine_state_fact_capture_failed");
    discard_prebuilt_binlog_blob_if_needed();
    return reject_after_snapshot_failure(false);
  }
  metadata.has_temp_engine_state =
      has_temp_engine_update ||
      !metadata.temp_table_manifest_payload.empty();
  metadata.has_logged_persistent_work =
      has_logged_binlog_cache && binlog_snapshot.with_content;
  metadata.has_persistent_engine_state =
      has_persistent_engine_update || metadata.has_logged_persistent_work ||
      !metadata.has_temp_engine_state;
  metadata.engine_shape =
      metadata.has_temp_engine_state
          ? (metadata.has_persistent_engine_state
                 ? Preserve_snapshot_engine_shape::MIXED
                 : Preserve_snapshot_engine_shape::TEMP_ONLY)
          : Preserve_snapshot_engine_shape::PERSISTENT_ONLY;

  uint64_t snapshot_codec_peak_bytes = 0;
  const Mysql_binlog_preserve_snapshot *codec_binlog_snapshot =
      has_logged_binlog_cache && !use_prebuilt_binlog_cache
          ? &binlog_snapshot
          : nullptr;
  if (preserve_trx_snapshot_codec_peak_bytes(
          metadata, codec_binlog_snapshot, &snapshot_codec_peak_bytes) !=
      Preserve_snapshot_status::OK) {
    set_failure_reason("snapshot_codec_size_overflow");
    discard_prebuilt_binlog_blob_if_needed();
    return reject_after_snapshot_failure(false);
  }
  Preserve_memory_lease snapshot_codec_lease =
      preserve_trx_acquire_memory_lease(
          preserve_resource_lease_key,
          Preserve_trx_memory_kind::SNAPSHOT_CODEC_BUFFER,
          snapshot_codec_peak_bytes);
  if (!snapshot_codec_lease.acquired()) {
    set_failure_reason("snapshot_codec_resource_exhausted");
    discard_prebuilt_binlog_blob_if_needed();
    return reject_after_snapshot_failure(false);
  }

  Preserved_trx_bundle_build_input bundle_input;
  bundle_input.metadata = metadata;
  if (has_logged_binlog_cache) {
    if (use_prebuilt_binlog_cache)
      bundle_input.prebuilt_binlog_cache_blob = &prebuilt_binlog_blob;
    else
      bundle_input.logged_binlog_snapshot = &binlog_snapshot;
  }
  bundle_input.options.max_snapshot_bytes = preserve_trx_max_snapshot_bytes;
  bundle_input.options.max_external_blob_bytes =
      preserve_trx_max_binlog_cache_bytes;
  bundle_input.options.max_record_locks_external_blob_bytes =
      preserve_trx_lock_warmcopy_max_journal_bytes;
  if (use_lock_warmcopy_artifact &&
      lock_warmcopy_artifact->has_prebuilt_record_locks_blob) {
    bundle_input.prebuilt_record_locks_blob =
        &lock_warmcopy_artifact->prebuilt_record_locks_blob;
  }
  bundle_input.externalize_record_locks_payload =
      (use_lock_warmcopy_artifact ||
       request.deferred_transfer_candidate != nullptr) &&
      bundle_input.prebuilt_record_locks_blob == nullptr &&
      !metadata.record_locks_payload.empty();
  bundle_input.emit_no_cache_binlog_mode_metadata =
      artifact_decision ==
      Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE;
  /*
    Lock warmcopy snapshots should not carry large record payloads inline when
    a prebuilt descriptor is unavailable. The bundle builder moves that payload
    into an external blob and leaves metadata as the semantic view.
  */
  Preserved_trx_bundle bundle;
  substep_started_us = preserve_trx_monotonic_us();
  const Preserve_snapshot_status bundle_status =
      build_preserved_trx_bundle(bundle_input, &bundle);
  add_result_elapsed_us(&Preserve_trx_preserve_result::
                            snapshot_write_bundle_build_us,
                        substep_started_us);
  if (bundle_status != Preserve_snapshot_status::OK) {
    discard_prebuilt_binlog_blob_if_needed();
    return reject_after_snapshot_failure(false);
  }
  blob_descriptors = bundle.blob_descriptors;

  Preserve_trx_resurrection_index_entry transfer_resurrection_entry;
  const Preserve_trx_resurrection_index_entry *transfer_resurrection_entry_ptr =
      nullptr;
  bool local_startup_resurrection_index_eligible = false;
  Preserve_trx_resurrection_index_entry local_resurrection_entry;
  if (artifact_decision ==
      Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE) {
    const Preserve_snapshot_metadata &transfer_metadata = bundle.metadata;
    const bool strict_semantics =
        has_resurrection_facts &&
        preserve_trx_resurrection_metadata_is_strict(
            transfer_metadata, resurrection_facts.trx_id) &&
        !preserve_trx_build_resurrection_index_entry(
            std::to_string(token_selection.transfer_token), resurrection_facts,
            &transfer_resurrection_entry);
    if (!strict_semantics) {
      set_failure_reason("standby_transfer_strict_semantics_unsupported");
      discard_prebuilt_binlog_blob_if_needed();
      return reject_after_snapshot_failure(false);
    }
    transfer_resurrection_entry_ptr = &transfer_resurrection_entry;
  } else if (artifact_decision ==
                 Preserve_trx_transfer_artifact_decision::LOCAL_CARRIER &&
             has_resurrection_facts &&
             preserve_trx_resurrection_metadata_supports_local_startup_index(
                 bundle.metadata)) {
    local_startup_resurrection_index_eligible =
        !preserve_trx_build_resurrection_index_entry(
            token, resurrection_facts, &local_resurrection_entry);
  }

  DEBUG_SYNC(thd, "preserve_trx_before_artifact_publish");
  if (reset_requested()) {
    DEBUG_SYNC(thd, "preserve_trx_batch_cancel_before_publish");
    set_failure_reason("batch_target_preserve_reset");
    discard_prebuilt_binlog_blob_if_needed();
    return reject_after_snapshot_failure(false);
  }

  Preserve_snapshot_write_options snapshot_write_options;
  snapshot_write_options.defer_file_fsync = false;
  snapshot_write_options.defer_directory_fsync =
      request.defer_snapshot_directory_fsync;
  snapshot_write_options.fast_new_token_state =
      request.defer_snapshot_directory_fsync;
  snapshot_write_options.fast_prebuilt_blob_adopt =
      request.defer_snapshot_directory_fsync;
  snapshot_write_options.shard_snapshot_files =
      request.defer_snapshot_directory_fsync;
  snapshot_write_options.shard_generic_external_blobs =
      request.defer_snapshot_directory_fsync;
  auto store = request.defer_snapshot_directory_fsync
                   ? create_preserved_trx_default_store(
                         preserve_trx_default_dir(), snapshot_write_options)
                   : create_preserved_trx_default_store(
                         preserve_trx_default_dir());
  bool durable_snapshot_may_exist = false;
  Preserve_snapshot_delete_status write_failure_delete_status =
      Preserve_snapshot_delete_status::OK;
  Preserved_trx_store_write_stats store_write_stats;
  std::unique_ptr<Preserve_trx_transfer_encoded_frame_sink> transfer_frame_sink;
  if (artifact_decision ==
          Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE &&
      request.transfer_source_epoch_session == nullptr) {
    const Preserve_trx_transfer_status frame_sink_status =
        preserve_trx_transfer_make_configured_frame_sink(&transfer_frame_sink);
    if (frame_sink_status != Preserve_trx_transfer_status::OK ||
        transfer_frame_sink == nullptr) {
      set_failure_reason("standby_transfer_publish_unsupported");
      discard_prebuilt_binlog_blob_if_needed();
      return reject_after_snapshot_failure(false);
    }
  }
  std::unique_ptr<Preserve_trx_artifact_sink> artifact_sink;
  std::array<unsigned char, kPreservedTrxSha256Length>
      local_snapshot_payload_digest{};
  bool local_authority_staged = false;
  bool local_authority_committed = false;
  substep_started_us = preserve_trx_monotonic_us();
  Preserve_snapshot_status status = Preserve_snapshot_status::OK;
  if (request.deferred_transfer_candidate != nullptr) {
    if (request.transfer_source_epoch_session == nullptr) {
      status = Preserve_snapshot_status::INVALID_ARGUMENT;
    } else {
      status = preserve_trx_transfer_capture_deferred_candidate(
          request.transfer_source_epoch_session->epoch_id(),
          token_selection.transfer_token, std::move(bundle), timeout_seconds,
          transfer_resurrection_entry_ptr,
          request.deferred_transfer_candidate, &metadata);
    }
  } else if (local_startup_resurrection_index_eligible) {
    status = store->stage_local_authority(
        std::move(bundle), timeout_seconds, &metadata,
        &local_snapshot_payload_digest, &write_failure_delete_status,
        &store_write_stats);
    local_authority_staged = status == Preserve_snapshot_status::OK;
  } else {
    const Preserve_snapshot_status artifact_sink_status =
        preserve_trx_make_artifact_sink_for_decision(
            artifact_decision, &store.store(), metadata.token,
            token_selection.transfer_token,
            request.transfer_source_epoch_session == nullptr
                ? Preserve_trx_transfer_runtime_policy{}.transfer_chunk_bytes
                : request.transfer_source_epoch_session->chunk_bytes(),
            transfer_frame_sink.get(), &artifact_sink,
            request.transfer_source_epoch_session,
            request.transfer_preserve_dir, transfer_resurrection_entry_ptr);
    if (artifact_sink_status != Preserve_snapshot_status::OK ||
        artifact_sink == nullptr) {
      set_failure_reason("standby_transfer_publish_unsupported");
      discard_prebuilt_binlog_blob_if_needed();
      return reject_after_snapshot_failure(false);
    }
    status = artifact_sink->publish_bundle(
        std::move(bundle), timeout_seconds, &metadata,
        &durable_snapshot_may_exist, &write_failure_delete_status,
        &store_write_stats);
  }
  add_result_elapsed_us(&Preserve_trx_preserve_result::snapshot_write_store_us,
                        substep_started_us);
  if (result != nullptr) {
    result->snapshot_write_store_token_state_us +=
        store_write_stats.token_state_us;
    result->snapshot_write_store_adopt_warm_blob_us +=
        store_write_stats.adopt_warm_blob_us;
    result->snapshot_write_store_write_new_blobs_us +=
        store_write_stats.write_new_blobs_us;
    result->snapshot_write_store_encode_us += store_write_stats.encode_us;
    result->snapshot_write_store_write_snapshot_us +=
        store_write_stats.write_snapshot_us;
  }
  if (status != Preserve_snapshot_status::OK) {
    discard_prebuilt_binlog_blob_if_needed();
    return reject_after_snapshot_failure(
        durable_snapshot_may_exist ||
            local_startup_resurrection_index_eligible,
                                         write_failure_delete_status);
  }
  if (local_authority_staged &&
      preserve_trx_write_local_resurrection_index(
          token, local_snapshot_payload_digest,
          std::move(local_resurrection_entry), &store.store())) {
    set_failure_reason("local_resurrection_index_publish_failed");
    return reject_after_snapshot_failure(true);
  }

  if (use_lock_warmcopy_artifact) {
    DEBUG_SYNC(thd,
               "preserve_trx_lock_warmcopy_after_snapshot_write_before_final_fence");
    DBUG_EXECUTE_IF(
        "preserve_trx_lock_warmcopy_simulate_conversion_after_snapshot_write",
        { inject_lock_warmcopy_conversion_after_freeze(); });
    if (!verify_lock_warmcopy_frozen_fence()) {
      /*
        The snapshot is already durable, so this final check cannot fall back to
        rebuilding lock payloads. The failure path below treats the snapshot as
        possibly visible and cleans up through the post-detach rules.
      */
      preserve_trx_lock_warmcopy_note_final_fence_mismatch();
      preserve_trx_lock_warmcopy_note_route_reject(
          Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED);
      set_failure_reason("lock_warmcopy_conversion_freeze_changed");
      return reject_after_snapshot_failure(durable_snapshot_may_exist);
    }
  }

  if (local_authority_staged) {
    if (result != nullptr) {
      result->freeze_lsn = resurrection_facts.freeze_lsn;
      result->local_authority_staged = true;
    }
    if (!request.defer_local_authority_commit) {
      if (trx_preserve_flush_redo_up_to(resurrection_facts.freeze_lsn) !=
              DB_SUCCESS ||
          store->commit_local_authority(token) != Preserve_snapshot_status::OK) {
        set_failure_reason("local_authority_commit_failed");
        return reject_after_snapshot_failure(true);
      }
      durable_snapshot_may_exist = true;
      local_authority_committed = true;
      if (result != nullptr) result->local_authority_staged = false;
    }
  }

  set_stage(Preserve_trx_preserve_stage::RECORD_REGISTER);
  /*
    Registration is the in-memory handoff from a durable snapshot to runtime
    ownership. Batch preserve keeps the record in DRAINING until every target
    succeeds.
  */
  if (preserved_trx_add_record(metadata, trx, true,
                               Preserved_trx_lifecycle_state::DRAINING,
                               blob_descriptors)) {
    thaw_lock_warmcopy_conversion();
    discard_prebuilt_binlog_blob_if_needed();
    bool cleanup_failed = false;
    bool left_preserved = false;
    if (!reattach_current_batch_preserve_failure_to_original_thd(
            thd, trx, token, true, true,
            durable_snapshot_may_exist || local_authority_committed,
            &cleanup_failed, &left_preserved, &metadata,
            has_logged_binlog_cache, &binlog_snapshot, &blob_descriptors)) {
      if (left_preserved) {
        reset_thd_after_preserve_detach(thd);
        cleanup_original_binlog_cache_after_detach(thd, has_logged_binlog_cache);
      }
      if (result != nullptr) {
        result->reattached_to_original_thd = !left_preserved;
        result->left_preserved_after_cleanup_failure = left_preserved;
        result->cleanup_failed_after_reattach = cleanup_failed;
      }
      return reject_unsupported_for_delivery();
    }
    delete_detached_mdl_context(token);
    const dberr_t rollback_status = trx_preserve_rollback_claimed(trx);
    if (result != nullptr && rollback_status != DB_SUCCESS)
      result->cleanup_failed_after_reattach = true;
    (void)delete_preserved_snapshot_files_and_sidecars_or_log(
        preserve_trx_default_dir(), token, &metadata);
    reset_thd_after_preserve_detach(thd);
    cleanup_original_binlog_cache_after_detach(thd, has_logged_binlog_cache);
    preserve_trx_kill_connection(thd);
    return reject_unsupported_for_delivery();
  }
  thaw_lock_warmcopy_conversion();
  snapshotting_state.remove();

  const bool retain_native_binlog_cache =
      artifact_decision ==
          Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE &&
      has_logged_binlog_cache && source_rollback_image != nullptr;
  if (retain_native_binlog_cache)
    source_rollback_image->native_binlog_cache_retained = true;
  if (result != nullptr) {
    result->preserved_trx = trx;
    result->source_rollback_image = std::move(source_rollback_image);
  }
  reset_thd_after_preserve_detach(thd);
  if (!retain_native_binlog_cache)
    cleanup_original_binlog_cache_after_detach(thd, has_logged_binlog_cache);
  audit_preserved_trx_event(thd, token, "preserve", "success");
  set_stage(Preserve_trx_preserve_stage::COMPLETE);
  return false;
}

bool preserve_trx_preserve_attached_transaction(
    THD *target_thd, const Preserve_trx_options &options,
    ulonglong timeout_seconds, Preserve_trx_preserve_result *result,
    PreserveBinlogBlobProvider *binlog_blob_provider,
    const Preserve_trx_lock_warmcopy_artifact *lock_warmcopy_artifact,
    bool debug_fail_ha_prepare_low_override,
    bool debug_fail_temp_only_prepare_override,
    bool defer_snapshot_directory_fsync,
    Preserve_trx_transfer_source_epoch_session *transfer_source_epoch_session,
    const std::string &transfer_preserve_dir,
    const std::string &preselected_token,
    bool defer_local_authority_commit,
    Preserve_trx_deferred_transfer_candidate *deferred_transfer_candidate,
    const Preserve_trx_drain_ownership_state *drain_ownership) {
  if (target_thd == nullptr) {
    return preserve_trx_reject_unsupported();
  }
  if (!preserved_trx_binlog_format_is_supported(
          target_thd->variables.binlog_format)) {
    return preserve_trx_reject_unsupported();
  }

  Preserve_trx_preserve_result local_result;
  Preserve_trx_preserve_result *effective_result =
      result == nullptr ? &local_result : result;
  bool debug_fail_ha_prepare_low = debug_fail_ha_prepare_low_override;
  bool debug_fail_temp_only_prepare = debug_fail_temp_only_prepare_override;
  DBUG_EXECUTE_IF("pfx_prepare_low", { debug_fail_ha_prepare_low = true; });
  DBUG_EXECUTE_IF("pfx_temp_prepare",
                  { debug_fail_temp_only_prepare = true; });
  Preserve_trx_kernel_request request{target_thd, options, timeout_seconds,
                                      effective_result, binlog_blob_provider,
                                      debug_fail_ha_prepare_low,
                                      debug_fail_temp_only_prepare,
                                      lock_warmcopy_artifact,
                                      defer_snapshot_directory_fsync,
                                      transfer_source_epoch_session,
                                      transfer_preserve_dir, preselected_token,
                                      defer_local_authority_commit,
                                      deferred_transfer_candidate,
                                      drain_ownership};
  return preserve_trx_kernel_preserve_attached_transaction(request);
}

static const char *preserve_trx_preserve_stage_name(
    Preserve_trx_preserve_stage stage) {
  switch (stage) {
    case Preserve_trx_preserve_stage::VALIDATION:
      return "VALIDATION";
    case Preserve_trx_preserve_stage::BINLOG_PREFLIGHT:
      return "BINLOG_PREFLIGHT";
    case Preserve_trx_preserve_stage::LOCK_PREFLIGHT:
      return "LOCK_PREFLIGHT";
    case Preserve_trx_preserve_stage::UNDO_PREPARE:
      return "UNDO_PREPARE";
    case Preserve_trx_preserve_stage::DETACH:
      return "DETACH";
    case Preserve_trx_preserve_stage::SNAPSHOT_WRITE:
      return "SNAPSHOT_WRITE";
    case Preserve_trx_preserve_stage::RECORD_REGISTER:
      return "RECORD_REGISTER";
    case Preserve_trx_preserve_stage::COMPLETE:
      return "COMPLETE";
  }
  return "UNKNOWN";
}

static const char *preserve_trx_drain_participant_state_name(
    Preserve_trx_drain_participant_state state) {
  switch (state) {
    case Preserve_trx_drain_participant_state::NOT_STARTED:
      return "NOT_STARTED";
    case Preserve_trx_drain_participant_state::OPEN:
      return "OPEN";
    case Preserve_trx_drain_participant_state::READY:
      return "READY";
    case Preserve_trx_drain_participant_state::DEGRADED:
      return "DEGRADED";
    case Preserve_trx_drain_participant_state::ABANDONED:
      return "ABANDONED";
    case Preserve_trx_drain_participant_state::FINALIZED:
      return "FINALIZED";
  }
  assert(false);
  return "UNKNOWN";
}

static void log_preserve_trx_drain_participant_observations(
    const Preserve_trx_drain_orchestrator &orchestrator, const char *stage,
    enum loglevel level) {
  const std::vector<Preserve_trx_drain_participant_observation> observations =
      orchestrator.observations();
  for (size_t index = 0; index < observations.size(); ++index) {
    const Preserve_trx_drain_participant_observation &observation =
        observations[index];
    std::string message =
        "PRESERVE: drain participant observation stage=" +
        std::string(stage == nullptr ? "unknown" : stage) +
        " index=" + std::to_string(index) +
        " state=" +
        preserve_trx_drain_participant_state_name(observation.state) +
        " owns_artifact=" +
        std::string(observation.owns_artifact ? "YES" : "NO") +
        " bytes_used=" + std::to_string(observation.bytes_used) +
        " bytes_budget=" + std::to_string(observation.bytes_budget) +
        " phase1_record_prebuilt_target_count=" +
        std::to_string(observation.phase1_record_prebuilt_target_count) +
        " phase1_record_active_scan_target_count=" +
        std::to_string(observation.phase1_record_active_scan_target_count) +
        " phase2_lock_seal_us=" +
        std::to_string(observation.phase2_lock_seal_us) +
        " phase2_record_lock_count=" +
        std::to_string(observation.phase2_record_lock_count) +
        " phase2_table_lock_count=" +
        std::to_string(observation.phase2_table_lock_count) +
        " phase2_mdl_descriptor_count=" +
        std::to_string(observation.phase2_mdl_descriptor_count) +
        " phase2_table_live_export_target_count=" +
        std::to_string(observation.phase2_table_live_export_target_count) +
        " phase2_mdl_live_export_target_count=" +
        std::to_string(observation.phase2_mdl_live_export_target_count) +
        " phase2_record_prebuilt_target_count=" +
        std::to_string(observation.phase2_record_prebuilt_target_count) +
        " phase2_record_materialized_target_count=" +
        std::to_string(observation.phase2_record_materialized_target_count) +
        " phase2_seal_worker_count=" +
        std::to_string(observation.phase2_seal_worker_count) +
        " phase2_slo_guaranteed=" +
        std::to_string(observation.phase2_slo_guaranteed ? 1 : 0) +
        " phase2_slo_not_guaranteed_target_count=" +
        std::to_string(observation.phase2_slo_not_guaranteed_target_count) +
        " phase1_progress=" +
        std::to_string(observation.phase1_progress);
    if (!observation.phase2_slo_reason.empty()) {
      message += " phase2_slo_reason=" + observation.phase2_slo_reason;
    }
    if (!observation.failure_reason.empty()) {
      message += " failure_reason=" + observation.failure_reason;
    }
    LogErr(level, ER_LOG_PRINTF_MSG, message.c_str());
  }
}

static bool send_preserve_trx_transfer_drain_result(
    THD *thd, ulonglong generation,
    const std::vector<my_thread_id> &survivor_thread_ids,
    const std::vector<my_thread_id> &excluded_timeout_thread_ids,
    const std::vector<std::pair<my_thread_id,
                               Preserve_batch_target_execution::Failure_reason>>
        &failed_tokens,
    ulonglong closing_started_us, ulonglong closing_deadline_us) {
  if (thd == nullptr) return true;

  mem_root_deque<Item *> fields(thd->mem_root);
  fields.push_back(
      new Item_return_int("generation", 20, MYSQL_TYPE_LONGLONG));
  fields.push_back(new Item_empty_string("outcome", 32));
  fields.push_back(
      new Item_return_int("source_connection_id", 20, MYSQL_TYPE_LONGLONG));
  fields.push_back(new Item_empty_string("token_role", 16));
  fields.push_back(new Item_empty_string("reason", 40));
  fields.push_back(
      new Item_return_int("closing_started_us", 20, MYSQL_TYPE_LONGLONG));
  fields.push_back(
      new Item_return_int("closing_deadline_us", 20, MYSQL_TYPE_LONGLONG));
  if (thd->send_result_metadata(fields,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF)) {
    return true;
  }

  const char *outcome =
      survivor_thread_ids.empty()
          ? "NO_PRESERVABLE_TOKENS"
          : (excluded_timeout_thread_ids.empty() && failed_tokens.empty()
                 ? "SUCCESS"
                 : "SUCCESS_WITH_EXCLUSIONS");
  Protocol *protocol = thd->get_protocol();
  auto send_row = [&](bool has_token, my_thread_id source_connection_id,
                      const char *token_role, const char *reason) {
    protocol->start_row();
    protocol->store(generation);
    protocol->store(outcome, system_charset_info);
    if (has_token) {
      protocol->store(static_cast<ulonglong>(source_connection_id));
    } else {
      protocol->store_null();
    }
    protocol->store(token_role, system_charset_info);
    protocol->store(reason, system_charset_info);
    protocol->store(closing_started_us);
    protocol->store(closing_deadline_us);
    return protocol->end_row();
  };

  if (survivor_thread_ids.empty() && excluded_timeout_thread_ids.empty() &&
      failed_tokens.empty()) {
    if (send_row(false, 0, "SUMMARY", "NONE")) return true;
  } else {
    for (my_thread_id thread_id : survivor_thread_ids) {
      if (send_row(true, thread_id, "SURVIVOR", "NONE")) return true;
    }
    for (my_thread_id thread_id : excluded_timeout_thread_ids) {
      if (send_row(true, thread_id, "EXCLUDED",
                   "CLOSING_COMMAND_TIMEOUT")) {
        return true;
      }
    }
    for (const auto &failed : failed_tokens) {
      if (send_row(true, failed.first, "EXCLUDED",
                   preserve_trx_source_failure_reason_name(failed.second))) {
        return true;
      }
    }
  }

  my_eof(thd);
  return thd->is_error();
}

bool Preserve_trx_drain_service::execute(
    THD *thd, const Preserve_trx_drain_request &request) {
  DBUG_TRACE;
  /*
    Batch drain is the multi-target command path. In two-phase mode it opens
    warmcopy participants while target sessions may still run, then publishes
    WARMCOPY_CLOSING to block every new ordinary client command. After target
    enumeration and quiesce, participant preflight/seal and per-target preserve
    run inside the blocked window. The command succeeds only after every
    survivor is preserved and audited. The Phase 2 deadline closes token
    convergence: transfer targets still executing an old command are excluded,
    while already quiesced targets continue through the existing token-local
    and epoch-global failure rules.
  */
  const Preserve_trx_options &options = request.options;

  if (!preserve_trx_is_enabled()) {
    my_error(ER_PRESERVE_TRX_DISABLED, MYF(0));
    return true;
  }

  if (check_global_access(thd, SHUTDOWN_ACL)) return true;

  if (preserve_trx_is_unsupported_common_context(thd))
    return preserve_trx_reject_unsupported();

  if (preserve_trx_has_active_multi_stmt_transaction(thd)) {
    my_error(ER_PRESERVE_TRX_INVALID_STATE, MYF(0));
    return true;
  }

  /* One drain snapshots all timeout policy before publishing any state. */
  const uint phase1_timeout_ms = preserve_trx_drain_phase1_timeout_ms;
  const uint phase2_timeout_ms = preserve_trx_drain_phase2_timeout_ms;
  const uint token_retention_timeout_ms =
      preserve_trx_token_retention_timeout_ms;
  const Preserve_trx_transfer_runtime_policy transfer_runtime_policy =
      preserve_trx_transfer_current_runtime_policy();
  const uint64_t transfer_max_inflight_bytes =
      preserve_trx_transfer_max_inflight_bytes;
  const ulonglong phase2_long_command_age_us =
      static_cast<ulonglong>(phase2_timeout_ms) * 1000ULL;
  const ulonglong timeout_seconds =
      (static_cast<ulonglong>(token_retention_timeout_ms) + 999ULL) / 1000ULL;

  const ulonglong generation = g_batch_generation.fetch_add(1) + 1;
  const bool binlog_warmcopy_enabled =
      opt_bin_log && mysql_bin_log.is_open();
  const bool lock_warmcopy_enabled = preserve_trx_lock_warmcopy_effective();
  const Preserve_trx_transfer_artifact_decision batch_artifact_decision =
      preserve_trx_transfer_artifact_decision();
  if (batch_artifact_decision ==
      Preserve_trx_transfer_artifact_decision::UNSUPPORTED) {
    return preserve_trx_reject_unsupported();
  }
  const bool standby_transfer_streaming_enabled =
      batch_artifact_decision ==
      Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE;
  const bool local_binlog_warmcopy_enabled = binlog_warmcopy_enabled;
  const bool temp_table_phase1_enabled = preserve_trx_temp_table_enable;
  const bool two_phase_enabled =
      standby_transfer_streaming_enabled || temp_table_phase1_enabled ||
      preserve_trx_lock_warmcopy_requires_two_phase(binlog_warmcopy_enabled);
  Preserve_trx_drain_orchestrator drain_orchestrator(
      two_phase_enabled ? Preserve_trx_drain_phase_mode::TWO_PHASE
                        : Preserve_trx_drain_phase_mode::SINGLE_PHASE);
  std::unique_ptr<Temp_table_phase1_drain_participant> temp_table_participant;
  if (temp_table_phase1_enabled) {
    temp_table_participant =
        std::make_unique<Temp_table_phase1_drain_participant>(thd,
                                                              generation);
    drain_orchestrator.add_participant(temp_table_participant.get());
  }
  std::unique_ptr<Warmcopy_batch_drain_participant> warmcopy_participant;
  if (local_binlog_warmcopy_enabled) {
    warmcopy_participant = std::make_unique<Warmcopy_batch_drain_participant>(
        thd, generation, phase2_timeout_ms,
        standby_transfer_streaming_enabled, transfer_runtime_policy,
        transfer_max_inflight_bytes);
    drain_orchestrator.add_participant(warmcopy_participant.get());
  }
  std::unique_ptr<Preserve_trx_lock_warmcopy_drain_participant>
      lock_warmcopy_participant;
  if (lock_warmcopy_enabled) {
    Preserve_trx_lock_warmcopy_options lock_warmcopy_options =
        preserve_trx_lock_warmcopy_current_options();
    lock_warmcopy_options.preserve_dir = preserve_trx_default_dir();
    lock_warmcopy_participant =
        std::make_unique<Preserve_trx_lock_warmcopy_drain_participant>(
            lock_warmcopy_options);
    drain_orchestrator.add_participant(lock_warmcopy_participant.get());
  }
  const bool active_binlog_progress_policy_enabled =
      standby_transfer_streaming_enabled && two_phase_enabled &&
      warmcopy_participant != nullptr;
  const bool early_pipeline_policy_enabled =
      standby_transfer_streaming_enabled && two_phase_enabled &&
      lock_warmcopy_participant != nullptr &&
      preserve_trx_lock_warmcopy_current_options().fallback_to_live_export;
  PreserveBinlogBlobProvider *warmcopy_provider = nullptr;
  std::shared_ptr<Preserve_trx_drain_attempt> active_drain_attempt;
  std::unique_ptr<Preserve_trx_manager_state_guard> draining;
  const Preserve_trx_manager_state initial_manager_state =
      two_phase_enabled ? Preserve_trx_manager_state::WARMCOPY_DRAINING
                        : Preserve_trx_manager_state::BATCH_DRAINING;
  if (standby_transfer_streaming_enabled) {
    const std::shared_ptr<Preserve_trx_drain_attempt> candidate =
        std::make_shared<Preserve_trx_drain_attempt>(generation,
                                                     thd->thread_id());
    std::lock_guard<std::mutex> lock(g_active_drain_attempt_mutex);
    if (g_active_drain_attempt != nullptr)
      return preserve_trx_reject_unsupported();
    draining = std::make_unique<Preserve_trx_manager_state_guard>(
        Preserve_trx_manager_state::IDLE, initial_manager_state,
        thd->thread_id());
    if (!draining->active()) return preserve_trx_reject_unsupported();
    g_active_drain_attempt = candidate;
    active_drain_attempt = candidate;
  } else {
    draining = std::make_unique<Preserve_trx_manager_state_guard>(
        Preserve_trx_manager_state::IDLE, initial_manager_state,
        thd->thread_id());
    if (!draining->active()) return preserve_trx_reject_unsupported();
  }
  preserve_trx_transfer_reset_source_phase1_metrics();
  preserve_trx_phase1_readiness_reset_latest_metrics();
  const ulonglong phase1_readiness_started_us = preserve_trx_monotonic_us();
  const ulonglong phase1_readiness_deadline_us =
      preserve_trx_monotonic_deadline_after_ms(
          phase1_readiness_started_us,
          phase1_timeout_ms);
  auto active_drain_attempt_cleanup = create_scope_guard([&] {
    if (preserve_trx_active_drain_reset_requested(active_drain_attempt) &&
        !active_drain_attempt->reset_release_barrier_complete.load(
            std::memory_order_acquire)) {
      preserve_trx_reset_invariant_failure(
          "drain_scope_exited_before_reset_release_barrier",
          active_drain_attempt);
    }
    preserve_trx_clear_active_drain_attempt(active_drain_attempt);
  });
  Preserve_batch_clear_temp_table_unsupported_boundaries clear_temp_boundaries;
  Global_THD_manager::get_instance()->do_for_all_thd_copy(
      &clear_temp_boundaries);
  std::unique_lock<std::mutex> warmcopy_status_guard;

  auto abort_drain_participants = [&](const char *stage) {
    drain_orchestrator.abort_participants();
    log_preserve_trx_drain_participant_observations(drain_orchestrator, stage,
                                                    INFORMATION_LEVEL);
  };

  auto stop_reset_participant_admission = [&]() {
    if (warmcopy_participant != nullptr)
      warmcopy_participant->stop_mirroring_for_reset();
    if (lock_warmcopy_participant != nullptr) lock_warmcopy_close_epoch();
  };

  auto finalize_drain_participants_for_terminal_handoff =
      [&](const char *stage) {
    drain_orchestrator.finalize_participants_for_terminal_handoff();
    log_preserve_trx_drain_participant_observations(
        drain_orchestrator, stage, INFORMATION_LEVEL);
  };

  if (local_binlog_warmcopy_enabled) {
    warmcopy_status_guard = std::unique_lock<std::mutex>(g_warmcopy_status_mutex);
  }
  Preserve_trx_phase2_metrics phase2_metrics;
  ulonglong phase2_total_started_us = 0;
  bool phase2_metrics_published = false;
  auto elapsed_since = [](ulonglong started_us) -> uint64_t {
    const ulonglong now_us = preserve_trx_monotonic_us();
    return now_us >= started_us ? now_us - started_us : 0;
  };
  auto publish_phase2_metrics = [&]() {
    if (phase2_total_started_us == 0) return;
    phase2_metrics.total_us = elapsed_since(phase2_total_started_us);
    phase2_metrics.lock_seal_us = 0;
    for (const Preserve_trx_drain_participant_observation &observation :
         drain_orchestrator.observations()) {
      phase2_metrics.lock_seal_us += observation.phase2_lock_seal_us;
    }
    preserve_trx_phase2_note_latest_metrics(phase2_metrics);
    phase2_metrics_published = true;
  };
  auto phase2_metrics_cleanup = create_scope_guard([&] {
    if (!phase2_metrics_published) publish_phase2_metrics();
  });
  std::unique_ptr<Preserve_trx_transfer_encoded_frame_sink>
      batch_transfer_frame_sink;
  std::unique_ptr<Preserve_trx_transfer_source_epoch_session>
      batch_transfer_source_session;
  Preserve_trx_transfer_phase1_batch_options batch_transfer_phase1_options;
  struct Phase1_batch_flush_context {
    Preserve_trx_transfer_source_epoch_session *session{nullptr};
    std::string preserve_dir;
    uint64_t max_batch_bytes{0};
  } batch_transfer_phase1_flush_context;
  std::unique_ptr<Preserve_trx_transfer_phase1_batch_sender>
      batch_transfer_phase1_sender;
  std::unique_ptr<Phase1_transfer_binlog_blob_provider>
      batch_transfer_binlog_blob_provider;
  std::set<my_thread_id> batch_transfer_phase1_declared_tokens;
  auto release_batch_transfer_frame_sink = [&]() {
    preserve_trx_unregister_active_drain_sink(
        active_drain_attempt, batch_transfer_frame_sink.get());
    batch_transfer_frame_sink.reset();
  };
  auto batch_transfer_frame_sink_cleanup = create_scope_guard([&] {
    preserve_trx_unregister_active_drain_sink(
        active_drain_attempt, batch_transfer_frame_sink.get());
  });
  auto abort_batch_transfer_epoch = [&](const char *reason) {
    if (batch_transfer_source_session == nullptr) return;
    if (batch_transfer_phase1_sender != nullptr) {
      batch_transfer_phase1_sender->abort();
      batch_transfer_phase1_sender.reset();
    }
    if (preserve_trx_active_drain_reset_requested(active_drain_attempt)) {
      return;
    }
    const Preserve_trx_transfer_status abort_status =
        batch_transfer_source_session->abort_epoch(reason == nullptr
                                                       ? "batch_abort"
                                                       : reason);
    if (abort_status != Preserve_trx_transfer_status::OK &&
        abort_status != Preserve_trx_transfer_status::UNSUPPORTED) {
      LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: standby transfer source epoch abort failed status=" +
              std::to_string(static_cast<int>(abort_status)))
                 .c_str());
    }
  };
  auto cleanup_cancelled_batch_transfer_epoch = [&](const char *reason) {
    if (batch_transfer_source_session == nullptr) return;
    if (batch_transfer_phase1_sender != nullptr) {
      batch_transfer_phase1_sender->abort();
      batch_transfer_phase1_sender.reset();
    }
    const Preserve_trx_transfer_status cleanup_status =
        batch_transfer_source_session->cleanup_cancelled_epoch(
            reason == nullptr ? "reset_cleanup" : reason);
    if (cleanup_status != Preserve_trx_transfer_status::OK &&
        cleanup_status != Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN &&
        cleanup_status != Preserve_trx_transfer_status::UNSUPPORTED &&
        cleanup_status != Preserve_trx_transfer_status::COMMITTED_READY &&
        cleanup_status != Preserve_trx_transfer_status::COMMITTED_NOT_READY) {
      LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: RESET receiver epoch cleanup deferred status=" +
              std::to_string(static_cast<int>(cleanup_status)))
                 .c_str());
    }
  };
  auto reset_requested = [&]() {
    return preserve_trx_active_drain_reset_requested(active_drain_attempt);
  };
  auto retain_reset_source_warmcopy_ids = [&]() {
    if (batch_transfer_binlog_blob_provider == nullptr ||
        active_drain_attempt == nullptr) {
      return;
    }
    std::set<std::string> warmcopy_ids =
        batch_transfer_binlog_blob_provider
            ->release_phase1_blobs_for_deferred_cleanup();
    std::lock_guard<std::mutex> lock(
        active_drain_attempt->quarantine_mutex);
    active_drain_attempt->quarantined_source_warmcopy_ids.insert(
        warmcopy_ids.begin(), warmcopy_ids.end());
  };
  auto release_reset_source_resources = [&]() {
    if (batch_transfer_phase1_sender != nullptr) {
      batch_transfer_phase1_sender->abort();
      batch_transfer_phase1_sender.reset();
    }
    batch_transfer_phase1_flush_context.session = nullptr;
    batch_transfer_source_session.reset();
    release_batch_transfer_frame_sink();
    retain_reset_source_warmcopy_ids();
    batch_transfer_binlog_blob_provider.reset();
    batch_transfer_phase1_declared_tokens.clear();
    if (warmcopy_status_guard.owns_lock()) warmcopy_status_guard.unlock();
  };
  auto finish_reset_manager = [&]() {
    if (active_drain_attempt == nullptr ||
        !active_drain_attempt->reset_release_barrier_complete.load(
            std::memory_order_acquire)) {
      preserve_trx_reset_invariant_failure(
          "drain_owner_finished_reset_without_release_barrier",
          active_drain_attempt);
    }
    draining->dismiss();
    active_drain_attempt_cleanup.commit();
    active_drain_attempt->drain_scope_released.store(true,
                                                     std::memory_order_release);
    preserved_trx_request_expired_reaper_scan();
    /*
      Cancelling the source-to-receiver client can leave its network error in
      the DRAIN owner's diagnostics. RESET has already restored the source
      transactions, so replace that cleanup error with the DRAIN result.
    */
    if (thd->is_error()) thd->clear_error();
    return preserve_trx_reject_drain_reset();
  };
  auto finish_phase1_reset = [&](const char *stage) {
    if (batch_transfer_phase1_sender != nullptr)
      batch_transfer_phase1_sender->abort();
    stop_reset_participant_admission();
    if (active_drain_attempt == nullptr ||
        !active_drain_attempt->ownership.begin_source_restore()) {
      preserve_trx_reset_invariant_failure(
          "phase1_reset_source_restore_transition_failed",
          active_drain_attempt);
    }
    Preserve_batch_clear_generation clear(generation);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
    preserve_trx_publish_active_drain_reset_barrier(active_drain_attempt);
    abort_drain_participants(stage);
    cleanup_cancelled_batch_transfer_epoch(stage);
    release_reset_source_resources();
    return finish_reset_manager();
  };
  auto reject_or_finish_phase1_reset = [&](const char *stage) {
    return reset_requested() ? finish_phase1_reset(stage)
                             : preserve_trx_reject_unsupported();
  };
  auto open_batch_transfer_source_epoch = [&]() {
    if (batch_artifact_decision !=
        Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE) {
      return false;
    }
    if (batch_transfer_source_session != nullptr) return false;

    const Preserve_trx_transfer_status frame_sink_status =
        preserve_trx_transfer_make_configured_frame_sink(
            &batch_transfer_frame_sink, &transfer_runtime_policy);
    if (frame_sink_status != Preserve_trx_transfer_status::OK ||
        batch_transfer_frame_sink == nullptr) {
      abort_drain_participants("standby_transfer_frame_sink_failed");
      return true;
    }
    if (active_drain_attempt != nullptr &&
        !preserve_trx_register_active_drain_sink(
            active_drain_attempt, batch_transfer_frame_sink.get())) {
      release_batch_transfer_frame_sink();
      return true;
    }

    const std::string batch_transfer_epoch_id =
        preserve_trx_transfer_make_epoch_id(
            "batch-" + std::to_string(generation) + "-" +
            std::to_string(preserve_trx_monotonic_us()));
    if (batch_transfer_epoch_id.empty()) {
      abort_drain_participants("standby_transfer_epoch_id_generation_failed");
      return true;
    }
    batch_transfer_phase1_options.max_batch_bytes =
        transfer_runtime_policy.phase1_batch_bytes;
    batch_transfer_phase1_options.linger_ms =
        transfer_runtime_policy.phase1_batch_linger_ms;
    batch_transfer_phase1_options.max_inflight_bytes =
        transfer_max_inflight_bytes;
    if (batch_transfer_phase1_options.max_batch_bytes >
        batch_transfer_phase1_options.max_inflight_bytes) {
      LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
             "PRESERVE: standby transfer phase1 batch bytes exceeds max "
             "inflight bytes");
      batch_transfer_source_session.reset();
      release_batch_transfer_frame_sink();
      abort_drain_participants("standby_transfer_phase1_batch_config_invalid");
      return true;
    }
    Preserve_trx_transfer_source_epoch_options source_epoch_options;
    source_epoch_options.runtime_policy = transfer_runtime_policy;
    source_epoch_options.chunk_bytes =
        transfer_runtime_policy.transfer_chunk_bytes;
    source_epoch_options.max_inflight_bytes =
        batch_transfer_phase1_options.max_inflight_bytes;
    source_epoch_options.phase1_batch_bytes =
        batch_transfer_phase1_options.max_batch_bytes;
    source_epoch_options.before_commit_send =
        preserve_trx_active_drain_before_commit_send;
    source_epoch_options.before_commit_send_context =
        active_drain_attempt.get();
    source_epoch_options.final_ack_arbiter =
        preserve_trx_active_drain_final_ack_arbiter;
    source_epoch_options.final_ack_arbiter_context =
        active_drain_attempt.get();
    batch_transfer_source_session.reset(
        new Preserve_trx_transfer_source_epoch_session(
            batch_transfer_epoch_id, source_epoch_options,
            batch_transfer_frame_sink.get()));
    uint64_t epoch_transport_deadline_us = phase1_readiness_deadline_us;
    epoch_transport_deadline_us = preserve_trx_monotonic_deadline_after_ms(
        epoch_transport_deadline_us, phase2_timeout_ms);
    epoch_transport_deadline_us = preserve_trx_monotonic_deadline_after_ms(
        epoch_transport_deadline_us,
        kPreserveTrxTransferOperationTimeoutMs);
    const uint64_t requested_terminal_retention_us =
        static_cast<uint64_t>(kPreserveTrxTransferOperationTimeoutMs) *
        2000ULL;
    const Preserve_trx_transfer_status open_status =
        batch_transfer_source_session->open_epoch(
            requested_terminal_retention_us, epoch_transport_deadline_us);
    if (open_status != Preserve_trx_transfer_status::OK) {
      LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: standby transfer OPEN_EPOCH failed status=" +
              std::to_string(static_cast<int>(open_status)))
                 .c_str());
      batch_transfer_source_session.reset();
      release_batch_transfer_frame_sink();
      abort_drain_participants("standby_transfer_open_epoch_failed");
      return true;
    }
    DEBUG_SYNC(
        thd,
        "preserve_trx_transfer_after_open_epoch_ack_before_first_frame");
    batch_transfer_source_session->set_phase1_metrics_enabled(true);
    batch_transfer_phase1_flush_context.session =
        batch_transfer_source_session.get();
    batch_transfer_phase1_flush_context.preserve_dir =
        preserve_trx_default_dir();
    batch_transfer_phase1_flush_context.max_batch_bytes =
        batch_transfer_phase1_options.max_batch_bytes;
    batch_transfer_phase1_sender.reset(
        new Preserve_trx_transfer_phase1_batch_sender(
            batch_transfer_phase1_options,
            [](const std::vector<Preserve_trx_transfer_phase1_blob_request>
                   &batch,
               void *context) {
              auto *flush_context =
                  static_cast<Phase1_batch_flush_context *>(context);
              if (flush_context == nullptr || flush_context->session == nullptr) {
                return Preserve_trx_transfer_status::INVALID_ARGUMENT;
              }
              DBUG_EXECUTE_IF("preserve_trx_delay_binlog_batch_callback", {
                if (std::any_of(batch.begin(), batch.end(), [](const auto &item) {
                      return item.object_id == kPreservedTrxBlobBinlogCache;
                    }))
                  my_sleep(2000000);
              });
              return preserve_trx_transfer_stream_prebuilt_blobs_batch(
                  flush_context->session, flush_context->preserve_dir, batch,
                  flush_context->max_batch_bytes);
            },
            &batch_transfer_phase1_flush_context));
    batch_transfer_binlog_blob_provider.reset(
        new Phase1_transfer_binlog_blob_provider(warmcopy_provider,
                                                 generation));
    return false;
  };
  auto declare_transfer_targets =
      [&](const std::vector<my_thread_id> &target_thread_ids,
          const char *failure_reason) {
    if (batch_transfer_source_session == nullptr) return false;
    std::vector<uint64_t> newly_declared_tokens;
    for (const my_thread_id target_thread_id : target_thread_ids) {
      if (batch_transfer_phase1_declared_tokens.count(target_thread_id) != 0)
        continue;
      newly_declared_tokens.push_back(static_cast<uint64_t>(target_thread_id));
    }
    if (newly_declared_tokens.empty()) return false;
    Preserve_trx_transfer_status declare_status =
        Preserve_trx_transfer_status::OK;
    if (batch_transfer_phase1_options.max_batch_bytes == 0 ||
        batch_transfer_phase1_options.linger_ms == 0) {
      for (uint64_t token : newly_declared_tokens) {
        declare_status = batch_transfer_source_session->declare_token(token);
        if (declare_status != Preserve_trx_transfer_status::OK) break;
      }
    } else {
      declare_status = batch_transfer_source_session->declare_tokens_batch(
          newly_declared_tokens);
    }
    if (declare_status != Preserve_trx_transfer_status::OK) {
      abort_batch_transfer_epoch(failure_reason);
      abort_drain_participants(failure_reason);
      return true;
    }
    for (uint64_t token : newly_declared_tokens) {
      const my_thread_id target_thread_id = static_cast<my_thread_id>(token);
      batch_transfer_phase1_declared_tokens.insert(target_thread_id);
    }
    return false;
  };
  auto declare_phase1_transfer_targets = [&]() {
    if (batch_transfer_source_session == nullptr) return false;
    Preserve_batch_phase1_transfer_target_scanner scanner(thd);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&scanner);
    return declare_transfer_targets(
        scanner.target_thread_ids(),
        "standby_transfer_phase1_declare_failed");
  };
  auto abort_phase1_transfer_targets_not_quiesced =
      [&](const std::vector<my_thread_id> &quiesced_target_thread_ids) {
        if (batch_transfer_source_session == nullptr ||
            batch_transfer_phase1_declared_tokens.empty()) {
          return false;
        }
        const std::set<my_thread_id> quiesced_targets(
            quiesced_target_thread_ids.begin(), quiesced_target_thread_ids.end());
        for (const my_thread_id target_thread_id :
             batch_transfer_phase1_declared_tokens) {
          if (quiesced_targets.count(target_thread_id) != 0) continue;
          const Preserve_trx_transfer_status abort_status =
              batch_transfer_source_session->abort_token(
                  static_cast<uint64_t>(target_thread_id),
                  "source_phase1_target_removed");
          if (abort_status != Preserve_trx_transfer_status::OK &&
              abort_status != Preserve_trx_transfer_status::UNSUPPORTED) {
            abort_batch_transfer_epoch("standby_transfer_target_abort_failed");
            abort_drain_participants("standby_transfer_target_abort_failed");
            return true;
          }
        }
        return false;
      };
  auto stream_phase1_transfer_record_lock_blobs = [&]() {
    if (batch_transfer_source_session == nullptr ||
        lock_warmcopy_participant == nullptr ||
        batch_transfer_phase1_declared_tokens.empty()) {
      return false;
    }
    const bool batch_record_blobs =
        batch_transfer_phase1_sender != nullptr &&
        batch_transfer_phase1_options.max_batch_bytes != 0 &&
        batch_transfer_phase1_options.linger_ms != 0;
    for (const my_thread_id target_thread_id :
         batch_transfer_phase1_declared_tokens) {
      PrebuiltRecordLocksBlob record_blob;
      if (!lock_warmcopy_participant->phase1_record_prebuilt_blob_for_thread(
              static_cast<uint64_t>(target_thread_id), &record_blob)) {
        continue;
      }
      if (batch_record_blobs) {
        Preserve_trx_transfer_object_descriptor descriptor;
        descriptor.object_id = record_blob.name;
        descriptor.kind = Preserve_trx_transfer_object_kind::EXTERNAL_BLOB;
        descriptor.total_size = record_blob.size;
        descriptor.digest = record_blob.digest;
        descriptor.lock_plan.version = record_blob.lock_plan_contract_version;
        descriptor.lock_plan.source_live_generation =
            record_blob.source_live_lock_generation;
        descriptor.lock_plan.source_live_digest =
            record_blob.source_live_lock_digest;
        descriptor.lock_plan.record_store_fingerprint =
            record_blob.record_store_fingerprint;
        if (batch_transfer_source_session->object_presealed_for_token(
                static_cast<uint64_t>(target_thread_id), descriptor)) {
          continue;
        }
        Preserve_trx_transfer_phase1_blob_request request;
        request.transfer_token = static_cast<uint64_t>(target_thread_id);
        request.object_id = record_blob.name;
        request.warmcopy_id = record_blob.warmcopy_id;
        request.warmcopy_epoch = record_blob.warmcopy_epoch;
        request.size = record_blob.size;
        request.digest = record_blob.digest;
        request.lock_plan_contract_version =
            record_blob.lock_plan_contract_version;
        request.source_live_lock_generation =
            record_blob.source_live_lock_generation;
        request.source_live_lock_digest =
            record_blob.source_live_lock_digest;
        request.record_store_fingerprint =
            record_blob.record_store_fingerprint;
        if (batch_transfer_phase1_sender->enqueue(request) !=
            Preserve_trx_transfer_status::OK) {
          abort_batch_transfer_epoch(
              "standby_transfer_phase1_record_blob_stream_failed");
          abort_drain_participants(
              "standby_transfer_phase1_record_blob_stream_failed");
          return true;
        }
        continue;
      }
      const Preserve_trx_transfer_status stream_status =
          preserve_trx_transfer_stream_prebuilt_record_locks_blob(
              batch_transfer_source_session.get(),
              static_cast<uint64_t>(target_thread_id),
              preserve_trx_default_dir(), record_blob);
      if (stream_status != Preserve_trx_transfer_status::OK) {
        abort_batch_transfer_epoch(
            "standby_transfer_phase1_record_blob_stream_failed");
        abort_drain_participants(
            "standby_transfer_phase1_record_blob_stream_failed");
        return true;
      }
    }
    if (batch_record_blobs &&
        batch_transfer_phase1_sender->flush() !=
            Preserve_trx_transfer_status::OK) {
      abort_batch_transfer_epoch(
          "standby_transfer_phase1_record_blob_stream_failed");
      abort_drain_participants(
          "standby_transfer_phase1_record_blob_stream_failed");
      return true;
    }
    return false;
  };
  auto begin_phase1_transfer_prewarm_manifests = [&]() {
    if (batch_transfer_source_session == nullptr ||
        batch_transfer_phase1_declared_tokens.empty()) {
      return false;
    }
    std::vector<uint64_t> transfer_tokens;
    transfer_tokens.reserve(batch_transfer_phase1_declared_tokens.size());
    for (const my_thread_id target_thread_id :
         batch_transfer_phase1_declared_tokens) {
      transfer_tokens.push_back(static_cast<uint64_t>(target_thread_id));
    }
    if (batch_transfer_phase1_options.max_batch_bytes != 0 &&
        batch_transfer_phase1_options.linger_ms != 0) {
      const Preserve_trx_transfer_status begin_status =
          batch_transfer_source_session->begin_token_prewarm_manifests_batch(
              transfer_tokens);
      if (begin_status == Preserve_trx_transfer_status::OK ||
          begin_status == Preserve_trx_transfer_status::UNSUPPORTED) {
        return false;
      }
      abort_batch_transfer_epoch(
          "standby_transfer_phase1_prewarm_manifest_failed");
      abort_drain_participants(
          "standby_transfer_phase1_prewarm_manifest_failed");
      return true;
    }
    for (uint64_t transfer_token : transfer_tokens) {
      const Preserve_trx_transfer_status begin_status =
          batch_transfer_source_session->begin_token_prewarm_manifest(
              transfer_token);
      if (begin_status == Preserve_trx_transfer_status::OK ||
          begin_status == Preserve_trx_transfer_status::UNSUPPORTED) {
        continue;
      }
      abort_batch_transfer_epoch(
          "standby_transfer_phase1_prewarm_manifest_failed");
      abort_drain_participants(
          "standby_transfer_phase1_prewarm_manifest_failed");
      return true;
    }
    return false;
  };
  if (two_phase_enabled) {
    if (drain_orchestrator.open_phase1_participants() !=
        Preserve_trx_drain_status::OK) {
      abort_drain_participants("open_phase1_failed");
      return reject_or_finish_phase1_reset("reset_after_phase1_open_failure");
    }
    if (warmcopy_participant != nullptr) {
      warmcopy_provider = warmcopy_participant->provider();
    }
    if (open_batch_transfer_source_epoch()) {
      return reject_or_finish_phase1_reset("reset_during_source_epoch_open");
    }
    if (declare_phase1_transfer_targets()) {
      return reject_or_finish_phase1_reset(
          "reset_during_phase1_target_declare");
    }
    DEBUG_SYNC(thd, "preserve_trx_warmcopy_after_phase1_open");
    if (reset_requested())
      return finish_phase1_reset("reset_after_phase1_open");
    if (prepare_lock_warmcopy_active_record_targets(
            thd, lock_warmcopy_participant.get())) {
      abort_batch_transfer_epoch(
          "lock_warmcopy_phase1_active_record_scan_rejected");
      abort_drain_participants(
          "lock_warmcopy_phase1_active_record_scan_rejected");
      return reject_or_finish_phase1_reset(
          "reset_during_phase1_active_record_prepare");
    }
    if (prepare_lock_warmcopy_idle_targets(thd,
                                           lock_warmcopy_participant.get())) {
      abort_batch_transfer_epoch("lock_warmcopy_phase1_prepare_rejected");
      abort_drain_participants("lock_warmcopy_phase1_prepare_rejected");
      return reject_or_finish_phase1_reset(
          "reset_during_phase1_idle_prepare");
    }
  }

  ulonglong overall_close_deadline_us = 0;
  ulonglong closing_command_deadline_us = 0;
  if (two_phase_enabled) {
    /*
      This is the last non-blocking point before WARMCOPY_CLOSING becomes
      visible to all new ordinary client-command gates. Re-sweep idle targets
      here
      so sessions that became idle during phase 1 can use a prebuilt record
      artifact instead of first exporting record locks in the blocked window.
      Non-idle active targets are also scanned here while business commands are
      still allowed; later hooks/fences decide whether that phase-1 candidate
      remains usable or must fall back to quiesced live export.
    */
    if (prepare_lock_warmcopy_active_record_targets(
            thd, lock_warmcopy_participant.get())) {
      abort_batch_transfer_epoch(
          "lock_warmcopy_late_phase1_active_record_scan_rejected");
      abort_drain_participants(
          "lock_warmcopy_late_phase1_active_record_scan_rejected");
      return reject_or_finish_phase1_reset(
          "reset_during_late_phase1_active_record_prepare");
    }
    if (prepare_lock_warmcopy_idle_targets(thd,
                                           lock_warmcopy_participant.get())) {
      abort_batch_transfer_epoch("lock_warmcopy_late_phase1_prepare_rejected");
      abort_drain_participants("lock_warmcopy_late_phase1_prepare_rejected");
      return reject_or_finish_phase1_reset(
          "reset_during_late_phase1_idle_prepare");
    }
    if (declare_phase1_transfer_targets()) {
      return reject_or_finish_phase1_reset(
          "reset_during_late_phase1_target_declare");
    }
    if (lock_warmcopy_participant != nullptr &&
        !lock_warmcopy_participant->prepare_phase1_record_store_targets(
            [&](uint64_t target_thread_id,
                const PrebuiltRecordLocksBlob &record_blob) {
              if (batch_transfer_phase1_sender == nullptr) return true;
              if (batch_transfer_phase1_declared_tokens.count(
                      static_cast<my_thread_id>(target_thread_id)) == 0) {
                return true;
              }
              Preserve_trx_transfer_phase1_blob_request request;
              request.transfer_token = target_thread_id;
              request.object_id = record_blob.name;
              request.warmcopy_id = record_blob.warmcopy_id;
              request.warmcopy_epoch = record_blob.warmcopy_epoch;
              request.size = record_blob.size;
              request.digest = record_blob.digest;
              request.lock_plan_contract_version =
                  record_blob.lock_plan_contract_version;
              request.source_live_lock_generation =
                  record_blob.source_live_lock_generation;
              request.source_live_lock_digest =
                  record_blob.source_live_lock_digest;
              request.record_store_fingerprint =
                  record_blob.record_store_fingerprint;
              return batch_transfer_phase1_sender->enqueue(request) ==
                     Preserve_trx_transfer_status::OK;
            })) {
      abort_batch_transfer_epoch("lock_warmcopy_phase1_store_prepare_rejected");
      abort_drain_participants("lock_warmcopy_phase1_store_prepare_rejected");
      return reject_or_finish_phase1_reset(
          "reset_during_phase1_store_prepare");
    }
    if (batch_transfer_phase1_sender != nullptr &&
        batch_transfer_phase1_sender->flush() !=
            Preserve_trx_transfer_status::OK) {
      abort_batch_transfer_epoch("standby_transfer_phase1_record_batch_failed");
      abort_drain_participants("standby_transfer_phase1_record_batch_failed");
      return reject_or_finish_phase1_reset(
          "reset_during_phase1_record_batch");
    }
    if (stream_phase1_transfer_record_lock_blobs()) {
      return reject_or_finish_phase1_reset(
          "reset_during_phase1_record_stream");
    }
    if (stream_phase1_transfer_binlog_cache_blobs(
            thd, generation, batch_transfer_phase1_declared_tokens,
            batch_transfer_source_session.get(),
            batch_transfer_binlog_blob_provider.get(),
            warmcopy_participant == nullptr
                ? nullptr
                : warmcopy_participant->provider(),
            batch_transfer_phase1_sender.get())) {
      abort_batch_transfer_epoch(
          "standby_transfer_phase1_binlog_blob_stream_failed");
      abort_drain_participants(
          "standby_transfer_phase1_binlog_blob_stream_failed");
      return reject_or_finish_phase1_reset(
          "reset_during_phase1_binlog_stream");
    }
    DEBUG_SYNC(thd, "preserve_trx_transfer_after_phase1_binlog_stream");
    if (begin_phase1_transfer_prewarm_manifests()) {
      return reject_or_finish_phase1_reset("reset_during_phase1_manifest");
    }
    if (batch_transfer_phase1_sender != nullptr) {
      if (batch_transfer_phase1_sender->flush() !=
          Preserve_trx_transfer_status::OK) {
        abort_batch_transfer_epoch("standby_transfer_phase1_batch_flush_failed");
        abort_drain_participants("standby_transfer_phase1_batch_flush_failed");
        return reject_or_finish_phase1_reset(
            "reset_during_phase1_final_flush");
      }
    }
    if (temp_table_participant != nullptr &&
        !temp_table_participant->prepare_late_phase1_idle_targets()) {
      abort_batch_transfer_epoch("temp_table_late_phase1_prepare_rejected");
      abort_drain_participants("temp_table_late_phase1_prepare_rejected");
      return reject_or_finish_phase1_reset(
          "reset_during_phase1_temp_prepare");
    }
  }
  ulonglong last_active_binlog_progress_us = 0;
  std::function<bool()> active_binlog_progress;
  if (active_binlog_progress_policy_enabled &&
      batch_transfer_source_session != nullptr &&
      batch_transfer_binlog_blob_provider != nullptr) {
    active_binlog_progress = [&]() {
      const ulonglong now_us = preserve_trx_monotonic_us();
      if (last_active_binlog_progress_us != 0 &&
          now_us - last_active_binlog_progress_us < 50000) {
        return false;
      }
      last_active_binlog_progress_us = now_us;
      return stream_active_transfer_binlog_cache_progress(
          thd, generation, batch_transfer_phase1_declared_tokens,
          batch_transfer_source_session.get(),
          batch_transfer_binlog_blob_provider.get(),
          warmcopy_participant->provider(),
          batch_transfer_phase1_sender.get(), false, false, false);
    };
  }
  if (two_phase_enabled) {
    Preserve_trx_phase1_readiness_metrics readiness_metrics;
    const Preserve_trx_phase1_readiness_result readiness_result =
        preserve_trx_wait_for_phase1_readiness(
            thd, phase1_readiness_deadline_us, phase2_long_command_age_us,
            active_drain_attempt, &readiness_metrics, active_binlog_progress);
    preserve_trx_phase1_readiness_note_latest_metrics(
        readiness_metrics.samples, readiness_metrics.inflight_commands,
        readiness_metrics.oldest_command_age_us,
        readiness_metrics.offender_count, readiness_metrics.wait_us);
    if (readiness_result ==
        Preserve_trx_phase1_readiness_result::RESET_REQUESTED) {
      return finish_phase1_reset("reset_during_phase1_readiness");
    }
    if (readiness_result ==
        Preserve_trx_phase1_readiness_result::OWNER_KILLED) {
      abort_batch_transfer_epoch("owner_killed_during_phase1_readiness");
      abort_drain_participants("owner_killed_during_phase1_readiness");
      return true;
    }
    if (readiness_result ==
        Preserve_trx_phase1_readiness_result::PROGRESS_FAILED) {
      abort_batch_transfer_epoch("phase1_binlog_progress_failed");
      abort_drain_participants("phase1_binlog_progress_failed");
      return preserve_trx_reject_unsupported();
    }
  }
  if (reset_requested())
    return finish_phase1_reset("reset_before_phase1_close");
  std::unique_lock<std::mutex> closing_target_classification_guard;
  if (two_phase_enabled) {
    /*
      Entering WARMCOPY_CLOSING publishes the manager state first, so command
      gates immediately block every new ordinary client command before
      close_phase1_participants() closes the participant preparation window.
    */
    preserve_trx_phase2_reset_latest_metrics();
    preserve_trx_transfer_reset_source_phase2_metrics();
    phase2_total_started_us = preserve_trx_monotonic_us();
    if (batch_transfer_source_session != nullptr) {
      batch_transfer_source_session->set_phase1_metrics_enabled(false);
    }
    closing_target_classification_guard =
        std::unique_lock<std::mutex>(g_closing_target_classification_mutex);
    if (!draining->transition_to(
            Preserve_trx_manager_state::WARMCOPY_CLOSING)) {
      if (reset_requested())
        return finish_phase1_reset("reset_before_warmcopy_closing");
      return preserve_trx_reject_unsupported();
    }
    if (active_drain_attempt != nullptr) {
      active_drain_attempt->closing_command_gate_published.store(
          true, std::memory_order_release);
    }
    const ulonglong closing_started_us = preserve_trx_monotonic_us();
    phase2_metrics.closing_started_us = closing_started_us;
    closing_command_deadline_us = preserve_trx_monotonic_deadline_after_ms(
        closing_started_us, phase2_timeout_ms);
    overall_close_deadline_us =
        standby_transfer_streaming_enabled
            ? preserve_trx_monotonic_deadline_after_ms(
                  closing_command_deadline_us,
                  kPreserveTrxTransferOperationTimeoutMs)
            : closing_command_deadline_us;
    phase2_metrics.closing_command_effective_budget_us =
        closing_command_deadline_us >= closing_started_us
            ? closing_command_deadline_us - closing_started_us
            : 0;
    phase2_metrics.closing_command_deadline_clamped = 0;
    if (warmcopy_participant != nullptr) {
      warmcopy_participant->set_closing_deadline_us(
          overall_close_deadline_us);
    }
    DEBUG_SYNC(thd, "preserve_trx_warmcopy_after_closing_state_before_targets");
  }
  if (!two_phase_enabled) {
    phase2_total_started_us = preserve_trx_monotonic_us();
    overall_close_deadline_us = preserve_trx_monotonic_deadline_after_ms(
        phase2_total_started_us, phase2_timeout_ms);
    closing_command_deadline_us = overall_close_deadline_us;
  }
  const ulonglong target_wait_deadline_us = closing_command_deadline_us;

  Preserve_batch_target_counter counter(thd, generation,
                                        phase2_metrics.closing_started_us);
  Global_THD_manager::get_instance()->do_for_all_thd_copy(&counter);
  if (closing_target_classification_guard.owns_lock())
    closing_target_classification_guard.unlock();
  DEBUG_SYNC(thd, "preserve_trx_warmcopy_after_targets_classified");
  /*
    Phase-1 readiness is observational: a connection can start a transaction
    after the final phase-1 scan but before CLOSING is published. CLOSING now
    freezes ordinary command admission, so the counter's transaction set is
    the first authoritative set and must be declared before any catch-up
    object is streamed.
  */
  if (declare_transfer_targets(
          counter.transaction_target_thread_ids(),
          "standby_transfer_closing_target_declare_failed")) {
    return reject_or_finish_phase1_reset(
        "reset_during_closing_target_declare");
  }
  size_t preserved_token_count = 0;
  std::vector<my_thread_id> quiesced_target_thread_ids;
  quiesced_target_thread_ids.reserve(counter.target_thread_ids().size());
  std::vector<my_thread_id> excluded_timeout_thread_ids;
  std::vector<std::pair<my_thread_id,
                        Preserve_batch_target_execution::Failure_reason>>
      source_failed_tokens;
  ulonglong phase2_transfer_tail_started_us = 0;
  ulonglong latest_command_boundary_us = phase2_metrics.closing_started_us;
  bool command_boundary_sample_missing = false;
  std::map<my_thread_id, std::string> batch_tokens_by_thread_id;
  std::vector<std::string> batch_tokens;
  bool batch_tokens_selected = false;
  std::vector<Preserve_batch_target_execution> target_results;
  std::vector<Preserve_trx_batch_item> preserved_batch_items;
  std::vector<Preserve_trx_batch_item> source_failed_batch_items;
  bool transfer_final_ack_accepted = false;
  bool debug_fail_ha_prepare_low = false;
  bool debug_fail_temp_only_prepare = false;
  bool debug_fail_after_one_target = false;
  bool debug_fail_after_detach_for_batch_reattach = false;
  bool debug_force_one_early_coordinate_drift = false;
  bool debug_force_early_final_fence_change = false;
  bool debug_fail_early_candidate_finalize = false;
  DBUG_EXECUTE_IF("pfx_prepare_low", { debug_fail_ha_prepare_low = true; });
  DBUG_EXECUTE_IF("pfx_temp_prepare",
                  { debug_fail_temp_only_prepare = true; });
  DBUG_EXECUTE_IF("preserve_trx_batch_fail_after_one_target",
                  { debug_fail_after_one_target = true; });
  DBUG_EXECUTE_IF("preserve_trx_fail_after_detach_for_batch_reattach", {
    debug_fail_after_detach_for_batch_reattach = true;
  });
  DBUG_EXECUTE_IF("preserve_trx_early_force_one_coordinate_drift", {
    debug_force_one_early_coordinate_drift = true;
  });
  DBUG_EXECUTE_IF("preserve_trx_early_force_final_fence_change", {
    debug_force_early_final_fence_change = true;
  });
  DBUG_EXECUTE_IF("preserve_trx_early_fail_candidate_finalize", {
    debug_fail_early_candidate_finalize = true;
  });

  auto retain_source_failed_items_in_source_context = [&]() {
    if (source_failed_batch_items.empty()) return;
    preserved_batch_items.reserve(preserved_batch_items.size() +
                                   source_failed_batch_items.size());
    for (Preserve_trx_batch_item &item : source_failed_batch_items) {
      preserved_batch_items.push_back(std::move(item));
    }
    source_failed_batch_items.clear();
  };

  auto collect_completed_target_items = [&]() {
    for (Preserve_batch_target_execution &execution : target_results) {
      if (execution.batch_item_collected ||
          execution.result.stage != Preserve_trx_preserve_stage::COMPLETE ||
          execution.result.token.empty()) {
        continue;
      }
      Preserve_trx_batch_item item;
      item.original_thread_id = execution.target_thread_id;
      item.token = execution.result.token;
      item.logged_binlog_cache = execution.result.logged_binlog_cache;
      item.local_authority_staged = execution.result.local_authority_staged;
      item.source_rollback_image =
          std::move(execution.result.source_rollback_image);
      preserved_batch_items.push_back(std::move(item));
      execution.batch_item_collected = true;
    }
    preserved_token_count = preserved_batch_items.size();
  };

  auto finish_phase2_reset =
      [&](std::vector<Preserve_trx_batch_item> *items,
          const char *stage) {
        if (items == &preserved_batch_items) {
          retain_source_failed_items_in_source_context();
        }
        if (batch_transfer_phase1_sender != nullptr)
          batch_transfer_phase1_sender->abort();
        stop_reset_participant_admission();

        if (active_drain_attempt == nullptr) {
          preserve_trx_reset_invariant_failure(
              "phase2_reset_without_transfer_attempt");
        }
        if (!active_drain_attempt->reset_release_barrier_complete.load(
                std::memory_order_acquire)) {
          if (items != nullptr) {
            if (!active_drain_attempt->ownership.begin_source_restore()) {
              preserve_trx_reset_invariant_failure(
                  "phase2_reset_source_restore_transition_failed",
                  active_drain_attempt);
            }
            restore_preserved_batch_items_for_reset(
                generation, std::move(*items), active_drain_attempt);
            preserve_trx_publish_active_drain_reset_barrier(
                active_drain_attempt);
          } else {
            (void)preserve_trx_try_restore_quarantined_reset(
                active_drain_attempt);
            while (!active_drain_attempt->reset_release_barrier_complete.load(
                std::memory_order_acquire)) {
              std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
          }
        }
        abort_drain_participants(stage);
        cleanup_cancelled_batch_transfer_epoch(stage);
        release_reset_source_resources();
        return finish_reset_manager();
      };

  auto finish_reset_from_current_items = [&](const char *stage) {
    collect_completed_target_items();
    return finish_phase2_reset(&preserved_batch_items, stage);
  };

  auto restore_current_items_to_original_thds = [&]() {
    retain_source_failed_items_in_source_context();
    return restore_preserved_batch_items_to_original_thds(
        generation, preserved_batch_items, active_drain_attempt);
  };

  auto finalize_closing_outcome_metrics = [&]() {
    if (standby_transfer_streaming_enabled) {
      std::sort(quiesced_target_thread_ids.begin(),
                quiesced_target_thread_ids.end());
      std::sort(excluded_timeout_thread_ids.begin(),
                excluded_timeout_thread_ids.end());
    }
    phase2_metrics.closing_inflight_commands = counter.pending_target_count();
    phase2_metrics.closing_excluded_tokens =
        excluded_timeout_thread_ids.size();
    phase2_metrics.closing_completed_before_deadline =
        phase2_metrics.closing_inflight_commands >=
                phase2_metrics.closing_excluded_tokens
            ? phase2_metrics.closing_inflight_commands -
                  phase2_metrics.closing_excluded_tokens
            : 0;
    phase2_metrics.closing_last_excluded_token =
        excluded_timeout_thread_ids.empty()
            ? 0
            : static_cast<uint64_t>(excluded_timeout_thread_ids.back());
  };

  auto finish_with_shutdown = [&]() {
    bool source_restore_context_published = false;
    auto release_retained_drain_scope = create_scope_guard([&] {
      if (!source_restore_context_published) return;
      active_drain_attempt->drain_scope_released.store(
          true, std::memory_order_release);
      preserved_trx_request_expired_reaper_scan();
    });
    if (active_drain_attempt != nullptr) {
      if (reset_requested())
        return finish_reset_from_current_items("reset_before_shutdown");
      if (active_drain_attempt->ownership.state() ==
              Preserve_trx_drain_terminal::RUNNING &&
          !preserve_trx_try_active_drain_shutdown_handoff(
              active_drain_attempt.get())) {
        if (reset_requested())
          return finish_reset_from_current_items(
              "reset_before_shutdown_handoff");
        return preserve_trx_reject_unsupported();
      }
    }
    finalize_closing_outcome_metrics();
    publish_phase2_metrics();
#if defined(ENABLED_DEBUG_SYNC)
    if (active_drain_attempt != nullptr &&
        (active_drain_attempt->ownership.state() ==
             Preserve_trx_drain_terminal::SHUTDOWN_HANDOFF ||
         active_drain_attempt->ownership.state() ==
             Preserve_trx_drain_terminal::COMMITTED_HANDOFF)) {
      DEBUG_SYNC(thd, "preserve_trx_drain_after_shutdown_handoff");
    }
#endif
    if (reset_requested())
      return finish_reset_from_current_items("reset_after_shutdown_handoff");
    if (standby_transfer_streaming_enabled) {
      if (active_drain_attempt == nullptr) {
        preserve_trx_reset_invariant_failure(
            "transfer_handoff_without_active_attempt");
      }
      {
        std::lock_guard<std::mutex> lock(
            active_drain_attempt->quarantine_mutex);
        if (!active_drain_attempt->quarantined_items.empty()) {
          preserve_trx_reset_invariant_failure(
              "final_ack_duplicate_rollback_context", active_drain_attempt);
        }
        active_drain_attempt->quarantine_started_monotonic_us =
            preserve_trx_monotonic_us();
        active_drain_attempt->quarantined_items =
            std::move(preserved_batch_items);
      }
      active_drain_attempt_cleanup.commit();
      active_drain_attempt->source_restore_context_ready.store(
          true, std::memory_order_release);
      source_restore_context_published = true;
      if (reset_requested()) {
        (void)preserve_trx_try_restore_quarantined_reset(active_drain_attempt);
        return finish_phase2_reset(nullptr,
                                   "reset_after_source_context_publication");
      }
    }
    if (transfer_final_ack_accepted) {
      finalize_drain_participants_for_terminal_handoff("finish");
      if (!draining->transition_to(
              Preserve_trx_manager_state::TRANSFER_HANDOFF_COMPLETE, 0)) {
        if (reset_requested()) {
          (void)preserve_trx_try_restore_quarantined_reset(
              active_drain_attempt);
          return finish_phase2_reset(
              nullptr, "reset_during_terminal_manager_publication");
        }
        preserve_trx_reset_invariant_failure(
            "terminal_manager_publication_failed", active_drain_attempt);
      }
      draining->dismiss();
    }
    bool drain_result_attempted = false;
    if (standby_transfer_streaming_enabled) {
      drain_result_attempted = true;
      if (send_preserve_trx_transfer_drain_result(
              thd, generation, quiesced_target_thread_ids,
              excluded_timeout_thread_ids, source_failed_tokens,
              phase2_metrics.closing_started_us, closing_command_deadline_us)) {
        LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
               "PRESERVE: transfer DRAIN result delivery failed after terminal "
               "handoff; source remains fenced");
      }
    }
    publish_phase2_metrics();
    audit_preserved_trx_control_event(
        thd, "drain", "success", static_cast<longlong>(counter.target_count()),
        static_cast<longlong>(preserved_token_count));
    DBUG_EXECUTE_IF("preserve_trx_drain_skip_shutdown_after_audit_no_targets", {
      if (counter.target_count() == 0) {
        finalize_drain_participants_for_terminal_handoff("finish_no_targets");
        if (!drain_result_attempted) my_ok(thd);
        return false;
      }
    });
    if (transfer_final_ack_accepted) {
      return false;
    }
    finalize_drain_participants_for_terminal_handoff("finish");
    if (!draining->transition_to(
            Preserve_trx_manager_state::SHUTDOWN_REQUESTED)) {
      if (reset_requested()) {
        if (source_restore_context_published) {
          (void)preserve_trx_try_restore_quarantined_reset(
              active_drain_attempt);
          return finish_phase2_reset(nullptr,
                                     "reset_during_shutdown_transition");
        }
        return finish_reset_from_current_items(
            "reset_during_shutdown_transition");
      }
      return preserve_trx_reject_unsupported();
    }
    if (standby_transfer_streaming_enabled) {
      draining->dismiss();
      return false;
    }

    const bool shutdown_success =
        shutdown(thd, SHUTDOWN_DEFAULT, !drain_result_attempted);
    if (shutdown_success) {
      draining->dismiss();
      return false;
    }

    drain_orchestrator.cleanup_after_failed_shutdown();
    return true;
  };

  auto finish_cleanup_failure_without_shutdown = [&](const char *stage) {
    Preserve_batch_clear_generation clear(generation);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
    abort_drain_participants(stage);
    draining->transition_to(Preserve_trx_manager_state::DRAIN_CLEANUP_FAILED,
                            0);
    draining->dismiss();
    return preserve_trx_reject_batch_cleanup_failed();
  };

  auto quarantine_retained_bytes = [&]() {
    uint64_t retained_bytes = 0;
    for (const Preserve_trx_batch_item &item : preserved_batch_items) {
      if (item.source_rollback_image == nullptr) continue;
      const uint64_t item_bytes =
          !item.source_rollback_image->binlog_snapshot.cache_payload.empty()
              ? item.source_rollback_image->binlog_snapshot.cache_payload.size()
              : item.source_rollback_image->has_prebuilt_binlog_blob
                    ? item.source_rollback_image->prebuilt_binlog_blob.size
                    : 0;
      retained_bytes =
          item_bytes > std::numeric_limits<uint64_t>::max() - retained_bytes
              ? std::numeric_limits<uint64_t>::max()
              : retained_bytes + item_bytes;
    }
    return retained_bytes;
  };
  auto enter_uncertain = [&](Preserve_trx_transfer_status status,
                             bool commit_may_have_reached_receiver) {
    if (active_drain_attempt == nullptr) {
      LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
             "PRESERVE: standby transfer has no active attempt for "
             "uncertain handoff");
      return finish_cleanup_failure_without_shutdown(
          "standby_transfer_commit_unknown_state_failed");
    }

    const uint64_t retained_bytes = quarantine_retained_bytes();
    const uint64_t retained_token_count = preserved_batch_items.size();
    {
      std::lock_guard<std::mutex> lock(
          active_drain_attempt->quarantine_mutex);
      active_drain_attempt->quarantine_started_monotonic_us =
          preserve_trx_monotonic_us();
      active_drain_attempt->quarantined_items =
          std::move(preserved_batch_items);
    }
    auto uncertain_restore_context_guard = create_scope_guard([&] {
      preserve_trx_reset_invariant_failure(
          "uncertain_handoff_exited_before_source_restore_context",
          active_drain_attempt);
    });
    active_drain_attempt_cleanup.commit();
    draining->dismiss();
    auto release_drain_scope = create_scope_guard([&] {
      active_drain_attempt->drain_scope_released.store(
          true, std::memory_order_release);
    });
    if (batch_transfer_binlog_blob_provider != nullptr) {
      std::set<std::string> retained_warmcopy_ids =
          batch_transfer_binlog_blob_provider
              ->release_phase1_blobs_for_deferred_cleanup();
      std::lock_guard<std::mutex> lock(
          active_drain_attempt->quarantine_mutex);
      active_drain_attempt->quarantined_source_warmcopy_ids =
          std::move(retained_warmcopy_ids);
    }
    bool resolution_armed = false;
    bool commit_unknown_published = false;
    if (commit_may_have_reached_receiver) {
      const Preserve_trx_handoff_resolution_context handoff_context =
          batch_transfer_source_session->handoff_resolution_context();
      resolution_armed =
          active_drain_attempt->handoff_resolution.arm(handoff_context);
      commit_unknown_published =
          active_drain_attempt->ownership.mark_commit_unknown();
      if (!resolution_armed || !commit_unknown_published) {
        LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
               "PRESERVE: standby transfer could not publish COMMIT_UNKNOWN");
      }
      preserve_trx_transfer_note_source_commit_unknown(retained_token_count,
                                                       retained_bytes);
    }

    release_reset_source_resources();
    finalize_drain_participants_for_terminal_handoff(
        commit_may_have_reached_receiver
            ? "standby_transfer_commit_unknown"
            : "standby_transfer_precommit_ack_uncertain");
    drain_orchestrator.cleanup_after_failed_shutdown();

    active_drain_attempt->source_restore_context_ready.store(
        true, std::memory_order_release);
    uncertain_restore_context_guard.commit();
    if (reset_requested()) {
      (void)preserve_trx_try_restore_quarantined_reset(active_drain_attempt);
      return finish_phase2_reset(nullptr,
                                 "reset_during_commit_unknown_publication");
    }
    active_drain_attempt->handoff_resolution_ready.store(
        commit_may_have_reached_receiver && resolution_armed &&
            commit_unknown_published,
        std::memory_order_release);
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: standby transfer source entered " +
            std::string(commit_may_have_reached_receiver
                            ? "COMMIT_UNKNOWN "
                            : "PRECOMMIT_ACK_UNCERTAIN ") +
            "status=" + std::to_string(static_cast<int>(status)) +
            " token_count=" + std::to_string(retained_token_count) +
            " retained_bytes=" + std::to_string(retained_bytes))
               .c_str());
    return preserve_trx_reject_unsupported();
  };

  auto close_warmcopy_participants_for_shutdown = [&](const char *stage) {
    if (!two_phase_enabled) return false;
    ulonglong timed_started_us = preserve_trx_monotonic_us();
    const Preserve_trx_drain_status close_status =
        drain_orchestrator.close_phase1_participants();
    phase2_metrics.participant_close_us += elapsed_since(timed_started_us);

    timed_started_us = preserve_trx_monotonic_us();
    const Preserve_trx_drain_status ready_status =
        drain_orchestrator.ensure_phase1_ready();
    const Preserve_trx_drain_status preflight_status =
        ready_status == Preserve_trx_drain_status::OK
            ? drain_orchestrator.phase2_preflight_participants()
            : ready_status;
    phase2_metrics.participant_preflight_us += elapsed_since(timed_started_us);
    if (close_status != Preserve_trx_drain_status::OK ||
        ready_status != Preserve_trx_drain_status::OK ||
        preflight_status != Preserve_trx_drain_status::OK) {
      abort_drain_participants(stage);
      return true;
    }
    return false;
  };

  auto select_batch_tokens =
      [&](const std::vector<my_thread_id> &target_thread_ids) {
        if (batch_tokens_selected) return false;
        const bool failed = preserve_trx_select_batch_tokens(
            target_thread_ids, batch_artifact_decision,
            &batch_tokens_by_thread_id, &batch_tokens);
        batch_tokens_selected = !failed;
        return failed;
      };

  const bool lock_warmcopy_batch = lock_warmcopy_participant != nullptr;
  const bool defer_batch_snapshot_directory_fsync = lock_warmcopy_batch;
  const ulonglong target_attach_deadline_us =
      two_phase_enabled ? overall_close_deadline_us : target_wait_deadline_us;
  auto preserve_one_target =
      [&](my_thread_id target_thread_id, THD *target_thd, THD *owner_thd,
          const char *worker_thread_stack,
          Preserve_batch_target_execution *execution, bool deferred_transfer,
          const Preserve_batch_attached_target_preparer
              &attached_target_preparer) {
        if (execution == nullptr) return;
        execution->target_thread_id = target_thread_id;
        const Preserve_trx_lock_warmcopy_artifact *lock_warmcopy_artifact =
            lock_warmcopy_participant == nullptr
                ? nullptr
                : deferred_transfer
                      ? &execution->lock_artifact
                      : lock_warmcopy_participant->artifact_for_thread(
                            target_thread_id);
        PreserveBinlogBlobProvider *binlog_blob_provider =
            batch_transfer_binlog_blob_provider != nullptr
                ? batch_transfer_binlog_blob_provider.get()
                : warmcopy_provider;
        const auto token_it = batch_tokens_by_thread_id.find(target_thread_id);
        if (token_it == batch_tokens_by_thread_id.end()) {
          execution->pin_error = true;
          return;
        }
        Preserve_batch_quiesced_idle_target batch(
            owner_thd, options, timeout_seconds, generation, target_thread_id,
            binlog_blob_provider, lock_warmcopy_artifact,
            debug_fail_ha_prepare_low, debug_fail_temp_only_prepare,
            deferred_transfer ? false : defer_batch_snapshot_directory_fsync,
            batch_transfer_source_session.get(), preserve_trx_default_dir(),
            token_it->second, worker_thread_stack, target_attach_deadline_us,
            active_drain_attempt, attached_target_preparer,
            deferred_transfer ? &execution->deferred_candidate : nullptr);
        if (target_thd != nullptr) batch.run(target_thd);
        execution->visited_target = batch.visited_target();
        execution->error = batch.error();
        execution->result = batch.take_result();
      };

  if (counter.nonidle_transaction_count() != 0 ||
      counter.has_unsupported_transaction()) {
    if (reset_requested())
      return finish_reset_from_current_items("reset_during_target_validation");
    sql_print_information(
        "PRESERVE: batch target counter rejected "
        "stage=target_counter_rejected transaction_count=%u target_count=%u "
        "nonidle_transaction_count=%u "
        "has_unsupported_transaction=%u",
        counter.transaction_count(), counter.target_count(),
        counter.nonidle_transaction_count(),
        counter.has_unsupported_transaction() ? 1 : 0);
    Preserve_batch_clear_generation clear(generation);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
    abort_batch_transfer_epoch("target_counter_rejected");
    abort_drain_participants("target_counter_rejected");
    return preserve_trx_reject_unsupported();
  }

  if (counter.target_count() == 0) {
    if (close_warmcopy_participants_for_shutdown("no_targets_close_failed")) {
      if (reset_requested())
        return finish_reset_from_current_items(
            "reset_during_no_target_phase1_close");
      return preserve_trx_reject_unsupported();
    }
    abort_batch_transfer_epoch("no_targets");
    return finish_with_shutdown();
  }

  const bool early_pipeline_enabled =
      early_pipeline_policy_enabled &&
      batch_transfer_source_session != nullptr &&
      counter.transaction_count() != 0;
  const bool timeout_exclusion_enabled =
      standby_transfer_streaming_enabled;
  if (early_pipeline_enabled) {
    const ulonglong token_selection_started_us = preserve_trx_monotonic_us();
    if (select_batch_tokens(counter.transaction_target_thread_ids())) {
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      abort_batch_transfer_epoch("early_token_selection_failed");
      abort_drain_participants("early_token_selection_failed");
      return preserve_trx_reject_unsupported();
    }
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: standby transfer source early token selection "
            "token_count=" +
            std::to_string(batch_tokens.size()) + " elapsed_us=" +
            std::to_string(elapsed_since(token_selection_started_us)))
               .c_str());

    target_results.resize(counter.transaction_target_thread_ids().size());
    std::map<my_thread_id, size_t> execution_by_thread_id;
    for (size_t i = 0; i < counter.transaction_target_thread_ids().size();
         ++i) {
      target_results[i].target_thread_id =
          counter.transaction_target_thread_ids()[i];
      if (batch_transfer_source_session != nullptr) {
        target_results[i].latest_record_lock_publication_generation =
            batch_transfer_source_session
                ->presealed_object_source_live_generation(
                    static_cast<uint64_t>(
                        counter.transaction_target_thread_ids()[i]),
                    kPreservedTrxBlobRecordLocks);
      }
      execution_by_thread_id.emplace(
          counter.transaction_target_thread_ids()[i], i);
    }

    const std::set<my_thread_id> target_ids(
        counter.transaction_target_thread_ids().begin(),
        counter.transaction_target_thread_ids().end());
    const ulonglong pin_started_us = preserve_trx_monotonic_us();
    Preserve_batch_phase1_declared_target_pin_collector target_pin_collector(
        thd, target_ids);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(
        &target_pin_collector);
    phase2_metrics.target_pin_us += elapsed_since(pin_started_us);
    std::vector<Preserve_batch_phase1_declared_target_pin_collector::Target>
        pinned_targets = std::move(target_pin_collector.targets());
    std::map<my_thread_id, THD *> pinned_by_thread_id;
    for (const auto &target : pinned_targets) {
      if (target.thd != nullptr)
        pinned_by_thread_id[target.thd->thread_id()] = target.thd;
    }
    if (target_pin_collector.error() ||
        pinned_by_thread_id.size() != target_ids.size()) {
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      abort_batch_transfer_epoch("early_target_pin_failed");
      abort_drain_participants("early_target_pin_failed");
      return preserve_trx_reject_unsupported();
    }

    std::mutex queue_mutex;
    std::condition_variable queue_condition;
    std::vector<size_t> ready_queue;
    size_t next_ready_index = 0;
    size_t completed_workers = 0;
    size_t early_staged_workers = 0;
    bool discovery_done = false;
    std::atomic<bool> worker_abort{false};
    std::atomic<bool> worker_init_failed{false};
    std::atomic<bool> worker_exception_failed{false};
    std::atomic<uint64_t> final_hwm_async_tokens{0};
    std::atomic<uint64_t> final_hwm_sync_fallback_tokens{0};
    std::atomic<uint64_t> final_hwm_pending_rejects{0};
    uint64_t final_hwm_flush_wait_us = 0;
    bool early_transport_cancelled = false, cancel_workers_before_join = true;
    const bool final_hwm_async_capable =
        batch_transfer_phase1_sender != nullptr &&
        batch_transfer_phase1_options.max_batch_bytes != 0 &&
        batch_transfer_phase1_options.linger_ms != 0;
    auto final_hwm_metrics = create_scope_guard([&] {
      sql_print_information(
          "PRESERVE: phase2 final HWM overlap async_tokens=%llu "
          "sync_fallback_tokens=%llu coordinator_flush_wait_us=%llu "
          "pending_rejects=%llu",
          static_cast<unsigned long long>(final_hwm_async_tokens.load()),
          static_cast<unsigned long long>(final_hwm_sync_fallback_tokens.load()),
          static_cast<unsigned long long>(final_hwm_flush_wait_us),
          static_cast<unsigned long long>(final_hwm_pending_rejects.load()));
    });
    const uint preserve_worker_count =
        preserve_trx_effective_parallel_preserve_threads(target_ids.size(),
                                                         true);
    std::vector<std::thread> workers;
    auto join_workers = create_scope_guard([&] {
      if (cancel_workers_before_join || reset_requested()) {
        worker_abort.store(true, std::memory_order_release);
        if (batch_transfer_phase1_sender != nullptr) {
          preserve_trx_cancel_active_drain_sink(active_drain_attempt);
          batch_transfer_phase1_sender->abort();
          early_transport_cancelled = true;
        }
      }
      {
        std::lock_guard<std::mutex> guard(queue_mutex);
        discovery_done = true;
      }
      queue_condition.notify_all();
      for (std::thread &worker : workers) {
        if (worker.joinable()) worker.join();
      }
    });
    const ulonglong worker_started_us = preserve_trx_monotonic_us();
    try {
      workers.reserve(preserve_worker_count);
      for (uint worker_index = 0; worker_index < preserve_worker_count;
           ++worker_index) {
        workers.emplace_back([&]() {
          if (my_thread_init()) {
            worker_init_failed.store(true, std::memory_order_relaxed);
            worker_abort.store(true, std::memory_order_release);
            queue_condition.notify_all();
            return;
          }
          auto thread_end_guard = create_scope_guard([] { my_thread_end(); });
          try {
            char worker_thread_stack_anchor = 0;
            for (;;) {
              size_t execution_index = 0;
              {
                std::unique_lock<std::mutex> lock(queue_mutex);
                queue_condition.wait(lock, [&] {
                  return worker_abort.load(std::memory_order_acquire) ||
                         next_ready_index < ready_queue.size() ||
                         discovery_done;
                });
                if (worker_abort.load(std::memory_order_acquire)) break;
                if (next_ready_index >= ready_queue.size()) {
                  if (discovery_done) break;
                  continue;
                }
                execution_index = ready_queue[next_ready_index++];
              }

              Preserve_batch_target_execution &execution =
                  target_results[execution_index];
              auto stop_for_reset = [&]() {
                if (!reset_requested()) return false;
                worker_abort.store(true, std::memory_order_release);
                queue_condition.notify_all();
                return true;
              };
              if (stop_for_reset()) break;
              const ulonglong target_work_started_us =
                  preserve_trx_monotonic_us();
              uint64_t external_stage_us = 0;
              const auto target_it =
                  pinned_by_thread_id.find(execution.target_thread_id);
              THD *target = target_it == pinned_by_thread_id.end()
                                ? nullptr
                                : target_it->second;
              if (target != nullptr) {
                bool has_persistent_engine_state = false;
                bool has_temp_engine_state = false;
                trx_t *target_trx = trx_preserve_current_thd_trx(target);
                if (target_trx == nullptr) {
                  execution.no_token_target = true;
                  execution.visited_target = true;
                  execution.result.failure_reason =
                      "strict_no_preservable_engine_state";
                } else if (!trx_preserve_engine_state_facts(
                               target_trx, &has_persistent_engine_state,
                               &has_temp_engine_state)) {
                  execution.error = true;
                  execution.visited_target = true;
                  execution.result.failure_reason =
                      "early_engine_state_facts_unavailable";
                } else if (!has_persistent_engine_state &&
                           !has_temp_engine_state) {
                  execution.no_token_target = true;
                  execution.visited_target = true;
                  execution.result.failure_reason =
                      "strict_no_preservable_engine_state";
                }
              }
              if (stop_for_reset()) break;
              if (!execution.error && !execution.no_token_target) {
                Preserve_batch_attached_target_preparer
                    attached_target_preparer =
                        [&](THD *attached_target) -> const char * {
                  if (stop_for_reset()) return "batch_target_attach_reset";
                  if (warmcopy_participant != nullptr) {
                    if (stop_for_reset()) return "batch_target_attach_reset";
                    if (!warmcopy_participant->prepare_attached_target(
                            attached_target, overall_close_deadline_us)) {
                      return "early_binlog_target_prepare_failed";
                    }
                    uint64_t streamed_bytes = 0;
                    std::vector<Preserve_trx_pending_binlog_publication>
                        pending_publications;
                    const Preserve_trx_binlog_catchup_result catchup_result =
                        stream_quiesced_transfer_binlog_cache_catchup(
                            attached_target, generation,
                            batch_transfer_source_session.get(),
                            batch_transfer_binlog_blob_provider.get(),
                            warmcopy_participant->provider(),
                            batch_transfer_phase1_sender.get(),
                            &pending_publications, &streamed_bytes);
                    if (catchup_result ==
                        Preserve_trx_binlog_catchup_result::FAILED) {
                      return "early_binlog_catchup_failed";
                    }
                    if (catchup_result ==
                        Preserve_trx_binlog_catchup_result::QUEUED) {
                      if (pending_publications.size() != 1) {
                        final_hwm_pending_rejects.fetch_add(1);
                        return "early_binlog_pending_publication_invalid";
                      }
                      Preserve_trx_pending_binlog_publication &pending =
                          pending_publications.front();
                      const bool async_pending =
                          final_hwm_async_capable && pending.final_hwm &&
                          !pending.owns_cleanup &&
                          !pending.release_live_artifact;
                      if (async_pending) {
                        if (pending.thread_id != execution.target_thread_id) {
                          final_hwm_pending_rejects.fetch_add(1);
                          return "early_binlog_pending_token_mismatch";
                        }
                        batch_transfer_binlog_blob_provider
                            ->remember_locally_queued_blob(pending.thread_id,
                                                          pending.blob);
                        execution.pending_final_binlog_descriptor =
                            preserve_trx_pending_binlog_descriptor(pending);
                        execution.has_pending_final_binlog_descriptor = true;
                        execution.pending_final_binlog_publication =
                            std::move(pending);
                        execution.has_pending_final_binlog_publication = true;
                        final_hwm_async_tokens.fetch_add(1);
                        DEBUG_SYNC(
                            attached_target,
                            "preserve_trx_phase2_final_hwm_enqueued_before_local_preserve");
                      } else {
                        final_hwm_sync_fallback_tokens.fetch_add(1);
                        if (batch_transfer_phase1_sender == nullptr ||
                            batch_transfer_phase1_sender->flush() !=
                                Preserve_trx_transfer_status::OK) {
                          return "early_binlog_sync_fallback_flush_failed";
                        }
                        publish_acked_transfer_binlog_cache_progress(
                            batch_transfer_binlog_blob_provider.get(),
                            warmcopy_participant->provider(),
                            pending_publications);
                      }
                    }
                    PrebuiltBinlogCacheBlob locally_queued_blob;
                    if (batch_transfer_binlog_blob_provider
                            ->locally_queued_blob_matches_current_thd(
                                attached_target, &locally_queued_blob)) {
                      Preserve_trx_pending_binlog_publication queued;
                      queued.thread_id = execution.target_thread_id;
                      queued.blob = std::move(locally_queued_blob);
                      const Preserve_trx_transfer_object_descriptor descriptor =
                          preserve_trx_pending_binlog_descriptor(queued);
                      if (execution.has_pending_final_binlog_descriptor &&
                          (execution.pending_final_binlog_descriptor.object_id !=
                               descriptor.object_id ||
                           execution.pending_final_binlog_descriptor.kind !=
                               descriptor.kind ||
                           execution.pending_final_binlog_descriptor.total_size !=
                               descriptor.total_size ||
                           execution.pending_final_binlog_descriptor.digest !=
                               descriptor.digest)) {
                        final_hwm_pending_rejects.fetch_add(1);
                        return "early_binlog_queued_descriptor_mismatch";
                      }
                      if (!execution.has_pending_final_binlog_descriptor) {
                        execution.pending_final_binlog_descriptor = descriptor;
                        execution.has_pending_final_binlog_descriptor = true;
                        final_hwm_async_tokens.fetch_add(1);
                      }
                    }
                    if (catchup_result ==
                            Preserve_trx_binlog_catchup_result::QUEUED ||
                        catchup_result ==
                            Preserve_trx_binlog_catchup_result::STREAMED) {
                      LogErr(
                          INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                          ("PRESERVE: standby transfer early binlog catchup "
                           "target=" +
                           std::to_string(static_cast<unsigned long long>(
                               execution.target_thread_id)) +
                           " bytes=" + std::to_string(streamed_bytes))
                              .c_str());
                    }
                  }
                  if (stop_for_reset()) return "batch_target_attach_reset";
                  if (lock_warmcopy_participant != nullptr) {
                    execution.lock_artifact_prepared =
                        lock_warmcopy_participant->prepare_quiesced_target(
                            attached_target, &execution.lock_artifact);
                    if (!execution.lock_artifact_prepared) {
                      return "early_lock_target_prepare_failed";
                    }
                  }
                  return nullptr;
                };
                preserve_one_target(execution.target_thread_id, target, nullptr,
                                    &worker_thread_stack_anchor, &execution,
                                    true, attached_target_preparer);
              }
              if (stop_for_reset()) break;
              if (!execution.error && execution.visited_target &&
                  execution.result.stage ==
                      Preserve_trx_preserve_stage::COMPLETE) {
                const ulonglong external_stage_started_us =
                    preserve_trx_monotonic_us();
                execution.initial_lock_fence_valid =
                    trx_preserve_sample_lock_warmcopy_fence(
                        execution.result.preserved_trx,
                        &execution.initial_lock_fence);
                const Preserve_trx_transfer_object_descriptor
                    *pending_final_binlog_descriptor =
                        execution.has_pending_final_binlog_descriptor
                            ? &execution.pending_final_binlog_descriptor
                            : nullptr;
                const Preserve_trx_transfer_status stage_status =
                    preserve_trx_bind_early_record_lock_blob(
                        lock_warmcopy_participant.get(), &execution)
                        ? preserve_trx_transfer_stage_deferred_candidate_external_objects(
                              batch_transfer_source_session.get(),
                              preserve_trx_default_dir(),
                              &execution.deferred_candidate,
                              batch_transfer_phase1_sender.get(),
                              pending_final_binlog_descriptor)
                        : Preserve_trx_transfer_status::UNSUPPORTED;
                execution.early_objects_staged =
                    stage_status == Preserve_trx_transfer_status::OK;
                if (!execution.early_objects_staged) {
                  execution.error = true;
                  execution.result.failure_reason =
                      "early_transfer_object_stage_failed";
                }
                external_stage_us =
                    elapsed_since(external_stage_started_us);
              }
              const uint64_t target_work_us =
                  elapsed_since(target_work_started_us);
              if (target_work_us >= 100000) {
                LogErr(
                    INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                    ("PRESERVE: early target slow path target=" +
                     std::to_string(static_cast<unsigned long long>(
                         execution.target_thread_id)) +
                     " wall_us=" + std::to_string(target_work_us) +
                     " external_stage_us=" +
                     std::to_string(external_stage_us) +
                     " lock_preflight_us=" +
                     std::to_string(execution.result.lock_preflight_us) +
                     " prepare_us=" +
                     std::to_string(execution.result.prepare_us) +
                     " snapshot_write_us=" +
                     std::to_string(execution.result.snapshot_write_us) +
                     " register_us=" +
                     std::to_string(execution.result.record_register_us))
                        .c_str());
              }
              {
                std::lock_guard<std::mutex> guard(queue_mutex);
                ++completed_workers;
                if (execution.early_objects_staged) ++early_staged_workers;
                if (execution.error || !execution.visited_target) {
                  worker_abort.store(true, std::memory_order_release);
                }
              }
              queue_condition.notify_all();
            }
          } catch (...) {
            worker_exception_failed.store(true, std::memory_order_relaxed);
            worker_abort.store(true, std::memory_order_release);
            queue_condition.notify_all();
          }
        });
      }
    } catch (...) {
      worker_exception_failed.store(true, std::memory_order_relaxed);
      worker_abort.store(true, std::memory_order_release);
    }

    std::set<my_thread_id> pending(counter.target_thread_ids().begin(),
                                   counter.target_thread_ids().end());
    Preserve_trx_batch_wait_result early_wait_result =
        Preserve_trx_batch_wait_result::READY;
    size_t last_reported_completed = 0;
    bool first_preserve_sync_fired = false;
    std::vector<Preserve_batch_ready_target> ready_targets;
    ready_targets.reserve(pending.size());
    std::vector<my_thread_id> no_transaction_targets;
    ulonglong last_active_binlog_progress_us = 0;
    while (!pending.empty() &&
           !worker_abort.load(std::memory_order_acquire)) {
      early_wait_result = preserve_trx_batch_observe_targets_ready_joint(
          thd, generation, &pending, &ready_targets, target_wait_deadline_us,
          active_drain_attempt);
      if (early_wait_result == Preserve_trx_batch_wait_result::DEADLINE &&
          timeout_exclusion_enabled) {
        DEBUG_SYNC(thd, "preserve_trx_early_timeout_before_recheck");
        if (reset_requested()) {
          early_wait_result =
              Preserve_trx_batch_wait_result::RESET_REQUESTED;
          worker_abort.store(true, std::memory_order_release);
          queue_condition.notify_all();
          break;
        }
        const std::vector<my_thread_id> deadline_targets(pending.begin(),
                                                         pending.end());
        ready_targets.clear();
        bool exclusion_failed = false;
        bool reset_during_exclusion = false;
        for (const my_thread_id target_thread_id : deadline_targets) {
          if (reset_requested()) {
            reset_during_exclusion = true;
            break;
          }
          Preserve_batch_timeout_target_decision decision(generation,
                                                          target_thread_id);
          Global_THD_manager::get_instance()->do_for_all_thd_copy(&decision);
          if (!decision.seen()) {
            exclusion_failed = true;
            break;
          }

          if (!decision.timed_out()) {
            ready_targets.push_back(
                {target_thread_id, decision.state(),
                 decision.temp_unsupported_boundary_seen(),
                 decision.boundary_observed_us()});
            pending.erase(target_thread_id);
            continue;
          }

          ++phase2_metrics.closing_command_timed_out_count;
          const bool declared =
              batch_transfer_phase1_declared_tokens.count(target_thread_id) !=
              0;
          const auto execution_it =
              execution_by_thread_id.find(target_thread_id);
          const bool has_execution =
              execution_it != execution_by_thread_id.end();
          if (has_execution && !declared) {
            exclusion_failed = true;
            break;
          }
          if (declared) {
            if (reset_requested()) {
              reset_during_exclusion = true;
              break;
            }
            if (batch_transfer_source_session->abort_token(
                    static_cast<uint64_t>(target_thread_id),
                    "closing_command_timeout") !=
                Preserve_trx_transfer_status::OK) {
              exclusion_failed = true;
              break;
            }
            batch_transfer_phase1_declared_tokens.erase(target_thread_id);
          }
          if (has_execution) {
            std::lock_guard<std::mutex> guard(queue_mutex);
            Preserve_batch_target_execution &execution =
                target_results[execution_it->second];
            execution.no_token_target = true;
            execution.visited_target = true;
            execution.result.failure_reason = "closing_command_timeout";
            ++completed_workers;
          }
          excluded_timeout_thread_ids.push_back(target_thread_id);
          pending.erase(target_thread_id);
        }
        if (reset_during_exclusion) {
          early_wait_result =
              Preserve_trx_batch_wait_result::RESET_REQUESTED;
          worker_abort.store(true, std::memory_order_release);
          queue_condition.notify_all();
          break;
        }
        if (exclusion_failed) {
          early_wait_result =
              Preserve_trx_batch_wait_result::TARGET_NOT_FOUND;
          worker_abort.store(true, std::memory_order_release);
          queue_condition.notify_all();
          break;
        }
        DEBUG_SYNC(thd, "preserve_trx_early_timeout_after_exclusion");
        if (reset_requested()) {
          early_wait_result =
              Preserve_trx_batch_wait_result::RESET_REQUESTED;
          worker_abort.store(true, std::memory_order_release);
          queue_condition.notify_all();
          break;
        }
        early_wait_result = Preserve_trx_batch_wait_result::READY;
      }
      if (early_wait_result != Preserve_trx_batch_wait_result::READY) break;

      bool enqueue_failed = false;
      no_transaction_targets.clear();
      {
        std::lock_guard<std::mutex> guard(queue_mutex);
        for (const Preserve_batch_ready_target &ready : ready_targets) {
          const auto execution_it =
              execution_by_thread_id.find(ready.thread_id);
          if (ready.temp_unsupported ||
              (ready.state !=
                   Preserve_trx_batch_thd_state::DRAINED_NO_TRANSACTION &&
               ready.state != Preserve_trx_batch_thd_state::QUIESCED)) {
            enqueue_failed = true;
            break;
          }
          if (ready.state ==
              Preserve_trx_batch_thd_state::DRAINED_NO_TRANSACTION) {
            if (ready.boundary_observed_us == 0) {
              command_boundary_sample_missing = true;
            } else {
              latest_command_boundary_us =
                  std::max(latest_command_boundary_us,
                           ready.boundary_observed_us);
            }
            if (execution_it != execution_by_thread_id.end()) {
              Preserve_batch_target_execution &execution =
                  target_results[execution_it->second];
              execution.no_token_target = true;
              execution.visited_target = true;
              execution.result.failure_reason =
                  "command_completed_without_transaction";
              ++completed_workers;
            }
            no_transaction_targets.push_back(ready.thread_id);
            continue;
          }
          if (execution_it == execution_by_thread_id.end()) {
            enqueue_failed = true;
            break;
          }
          quiesced_target_thread_ids.push_back(ready.thread_id);
          ready_queue.push_back(execution_it->second);
          if (ready.boundary_observed_us == 0) {
            command_boundary_sample_missing = true;
          } else {
            latest_command_boundary_us =
                std::max(latest_command_boundary_us,
                         ready.boundary_observed_us);
          }
          const ulonglong enqueued_us = preserve_trx_monotonic_us();
          if (ready.boundary_observed_us != 0 &&
              enqueued_us >= ready.boundary_observed_us) {
            phase2_metrics.command_boundary_to_enqueue_us_max = std::max(
                phase2_metrics.command_boundary_to_enqueue_us_max,
                static_cast<uint64_t>(enqueued_us -
                                      ready.boundary_observed_us));
          }
        }
      }
      for (const my_thread_id target_thread_id : no_transaction_targets) {
        Preserve_batch_clear_target_generation clear_target(
            generation, target_thread_id);
        Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear_target);
      }
      if (enqueue_failed) {
        worker_abort.store(true, std::memory_order_release);
      }
      queue_condition.notify_all();

      const ulonglong now_us = preserve_trx_monotonic_us();
      if (!enqueue_failed && !pending.empty() &&
          (last_active_binlog_progress_us == 0 ||
           now_us - last_active_binlog_progress_us >= 50000)) {
        if (stream_active_transfer_binlog_cache_progress(
                thd, generation, pending, batch_transfer_source_session.get(),
                batch_transfer_binlog_blob_provider.get(),
                warmcopy_participant == nullptr
                    ? nullptr
                    : warmcopy_participant->provider(),
                batch_transfer_phase1_sender.get(), false, true,
                final_hwm_async_capable)) {
          early_wait_result =
              Preserve_trx_batch_wait_result::TARGET_NOT_FOUND;
          worker_abort.store(true, std::memory_order_release);
          queue_condition.notify_all();
          break;
        }
        last_active_binlog_progress_us = preserve_trx_monotonic_us();
      }

      size_t completed_snapshot = 0;
      bool has_early_staged_token = false;
      {
        std::lock_guard<std::mutex> guard(queue_mutex);
        completed_snapshot = completed_workers;
        has_early_staged_token = early_staged_workers != 0;
      }
      if (!first_preserve_sync_fired && has_early_staged_token) {
        first_preserve_sync_fired = true;
        DEBUG_SYNC(thd, "preserve_trx_batch_after_one_target_preserved");
      }
      const bool made_progress = !ready_targets.empty() ||
                                 completed_snapshot != last_reported_completed;
      last_reported_completed = completed_snapshot;
      if (!made_progress) my_sleep(10000);
    }
    {
      std::unique_lock<std::mutex> guard(queue_mutex);
      discovery_done = true;
      queue_condition.notify_all();
      while (early_wait_result == Preserve_trx_batch_wait_result::READY &&
             completed_workers != target_results.size() &&
             !worker_abort.load(std::memory_order_acquire) &&
             !reset_requested()) {
        queue_condition.wait_for(guard, std::chrono::milliseconds(1));
      }
    }
    const bool prejoin_failure =
        reset_requested() ||
        early_wait_result != Preserve_trx_batch_wait_result::READY ||
        worker_abort.load(std::memory_order_acquire) ||
        worker_init_failed.load(std::memory_order_relaxed) ||
        worker_exception_failed.load(std::memory_order_relaxed);
    cancel_workers_before_join = prejoin_failure;
    join_workers.rollback();
    phase2_metrics.target_worker_wall_us += elapsed_since(worker_started_us);
    if (early_transport_cancelled &&
        batch_transfer_phase1_sender != nullptr) {
      batch_transfer_phase1_sender.reset();
    }

    if (reset_requested()) {
      collect_completed_target_items();
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      return finish_phase2_reset(
          &preserved_batch_items, "reset_during_early_target_pipeline");
    }

    const size_t expected_completed_workers = target_results.size();
    for (Preserve_batch_target_execution &execution : target_results) {
      if (!execution.no_token_target) continue;

      const auto token_it =
          batch_tokens_by_thread_id.find(execution.target_thread_id);
      if (token_it == batch_tokens_by_thread_id.end()) {
        execution.error = true;
        execution.result.failure_reason = "early_no_token_identity_missing";
        continue;
      }
      const std::string token = token_it->second;
      batch_tokens_by_thread_id.erase(token_it);
      batch_tokens.erase(
          std::remove(batch_tokens.begin(),
                      batch_tokens.end(), token),
          batch_tokens.end());
      Preserve_batch_clear_target_generation clear_target(
          generation, execution.target_thread_id);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear_target);
    }
    quiesced_target_thread_ids.erase(
        std::remove_if(
            quiesced_target_thread_ids.begin(),
            quiesced_target_thread_ids.end(),
            [&](my_thread_id target_thread_id) {
              const auto execution_it =
                  execution_by_thread_id.find(target_thread_id);
              return execution_it != execution_by_thread_id.end() &&
                     target_results[execution_it->second].no_token_target;
            }),
        quiesced_target_thread_ids.end());
    target_results.erase(
        std::remove_if(target_results.begin(), target_results.end(),
                       [](const Preserve_batch_target_execution &execution) {
                         return execution.no_token_target;
                       }),
        target_results.end());
    phase2_metrics.early_staged_tokens = static_cast<uint64_t>(std::count_if(
        target_results.begin(), target_results.end(),
        [](const Preserve_batch_target_execution &execution) {
          return execution.early_objects_staged;
        }));

    std::vector<uint64_t> early_lock_target_ids;
    early_lock_target_ids.reserve(quiesced_target_thread_ids.size());
    for (const my_thread_id target_thread_id : quiesced_target_thread_ids) {
      early_lock_target_ids.push_back(static_cast<uint64_t>(target_thread_id));
    }
    const ulonglong lock_completion_started_us = preserve_trx_monotonic_us();
    const bool lock_completion_failed =
        lock_warmcopy_participant != nullptr &&
        !lock_warmcopy_participant->complete_early_prepared_targets(
            early_lock_target_ids);
    if (lock_warmcopy_participant != nullptr) {
      LogErr(
          INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
          ("PRESERVE: lock warmcopy final survivor prune retained_targets=" +
           std::to_string(early_lock_target_ids.size()) + " elapsed_us=" +
           std::to_string(elapsed_since(lock_completion_started_us)))
              .c_str());
    }
    bool early_failed =
        early_wait_result != Preserve_trx_batch_wait_result::READY ||
        pending.size() != 0 ||
        worker_abort.load(std::memory_order_acquire) ||
        worker_init_failed.load(std::memory_order_relaxed) ||
        worker_exception_failed.load(std::memory_order_relaxed) ||
        completed_workers != expected_completed_workers ||
        lock_completion_failed;
    bool binlog_batch_flush_failed = false;
    if (!early_failed && batch_transfer_phase1_sender != nullptr) {
      const ulonglong flush_started_us = preserve_trx_monotonic_us();
      binlog_batch_flush_failed =
          batch_transfer_phase1_sender->flush() !=
          Preserve_trx_transfer_status::OK;
      final_hwm_flush_wait_us = elapsed_since(flush_started_us);
    }
    if (!early_failed && !binlog_batch_flush_failed &&
        batch_transfer_source_session != nullptr) {
      for (const Preserve_batch_target_execution &execution : target_results) {
        if (!execution.has_pending_final_binlog_descriptor) continue;
        if (execution.target_thread_id !=
                execution.deferred_candidate.transfer_token ||
            !batch_transfer_source_session->object_presealed_for_token(
                execution.deferred_candidate.transfer_token,
                execution.pending_final_binlog_descriptor)) {
          final_hwm_pending_rejects.fetch_add(1);
          binlog_batch_flush_failed = true;
          break;
        }
      }
    }
    if (!early_failed && !binlog_batch_flush_failed) {
      for (Preserve_batch_target_execution &execution : target_results) {
        if (!execution.has_pending_final_binlog_publication) continue;
        const Preserve_trx_pending_binlog_publication &publication =
            execution.pending_final_binlog_publication;
        if (publication.remote_presealed) {
          publish_acked_transfer_binlog_cache_progress(
              batch_transfer_binlog_blob_provider.get(),
              warmcopy_participant == nullptr
                  ? nullptr
                  : warmcopy_participant->provider(),
              publication);
        } else {
#ifndef NDEBUG
          log_acked_active_binlog_progress(
              publication.thread_id, publication.blob,
              publication.published_bytes, publication.replacement,
              publication.final_hwm, true);
#endif
        }
        execution.has_pending_final_binlog_publication = false;
        execution.has_pending_final_binlog_descriptor = false;
      }
    }
    if (!early_failed && !binlog_batch_flush_failed &&
        batch_transfer_source_session != nullptr) {
      for (Preserve_batch_target_execution &execution : target_results) {
        Preserve_trx_deferred_transfer_candidate &candidate =
            execution.deferred_candidate;
        if (!candidate.binlog_prewarm_seed_batch_pending) continue;
        if (!batch_transfer_source_session->token_prewarm_lsn_fact(
                candidate.transfer_token, &candidate.source_freeze_lsn,
                &candidate.source_epoch_commit_lsn)) {
          binlog_batch_flush_failed = true;
          break;
        }
        candidate.binlog_prewarm_seed_batch_pending = false;
        candidate.binlog_prewarm_seed_staged = true;
        candidate.external_objects_staged = true;
      }
    }
    early_failed = early_failed || binlog_batch_flush_failed;
    if (early_failed && batch_transfer_phase1_sender != nullptr) {
      preserve_trx_cancel_active_drain_sink(active_drain_attempt);
      batch_transfer_phase1_sender->abort();
      early_transport_cancelled = true;
    }
    batch_transfer_phase1_sender.reset();
    if (reset_requested() || early_failed) {
      if (early_failed && !reset_requested()) {
        LogErr(
            INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
            ("PRESERVE: early pipeline terminal failure wait_result=" +
             std::to_string(static_cast<uint>(early_wait_result)) +
             " pending=" + std::to_string(pending.size()) +
             " worker_abort=" +
             std::to_string(
                 worker_abort.load(std::memory_order_acquire) ? 1 : 0) +
             " worker_init_failed=" +
             std::to_string(
                 worker_init_failed.load(std::memory_order_relaxed) ? 1 : 0) +
             " worker_exception_failed=" +
             std::to_string(worker_exception_failed.load(
                                std::memory_order_relaxed)
                                ? 1
                                : 0) +
             " completed=" + std::to_string(completed_workers) +
             " expected=" + std::to_string(expected_completed_workers) +
             " lock_completion_failed=" +
             std::to_string(lock_completion_failed ? 1 : 0) +
             " binlog_batch_flush_failed=" +
             std::to_string(binlog_batch_flush_failed ? 1 : 0))
                .c_str());
      }
      auto first_failed = std::find_if(
          target_results.begin(), target_results.end(),
          [](const Preserve_batch_target_execution &execution) {
            return execution.error;
          });
      if (first_failed == target_results.end()) {
        first_failed = std::find_if(
            target_results.begin(), target_results.end(),
            [](const Preserve_batch_target_execution &execution) {
              return !execution.visited_target;
            });
      }
      if (early_failed && !reset_requested() &&
          first_failed != target_results.end()) {
        LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
               ("PRESERVE: early target failed target_thread_id=" +
                std::to_string(first_failed->target_thread_id) + " reason=" +
                (first_failed->result.failure_reason == nullptr
                     ? "unknown"
                     : first_failed->result.failure_reason))
                   .c_str());
      }
      collect_completed_target_items();
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      if (reset_requested()) {
        return finish_phase2_reset(
            &preserved_batch_items, "reset_during_early_target_pipeline");
      }
      const bool cleanup_error = restore_current_items_to_original_thds();
      if (early_transport_cancelled) {
        cleanup_cancelled_batch_transfer_epoch(
            "early_target_pipeline_failed");
      } else {
        abort_batch_transfer_epoch("early_target_pipeline_failed");
      }
      abort_drain_participants("early_target_pipeline_failed");
      if (cleanup_error) {
        return finish_cleanup_failure_without_shutdown(
            "early_target_pipeline_cleanup_failed");
      }
      if (early_transport_cancelled && thd->is_error()) thd->clear_error();
      return preserve_trx_reject_unsupported();
    }
  }

  ulonglong timed_started_us = preserve_trx_monotonic_us();
  if (!early_pipeline_enabled) {
    const ulonglong target_wait_started_us = timed_started_us;
    auto target_wait_metrics = create_scope_guard([&] {
      const uint64_t target_wait_us = elapsed_since(target_wait_started_us);
      phase2_metrics.target_wait_us += target_wait_us;
      if (two_phase_enabled)
        phase2_metrics.closing_command_wait_us = target_wait_us;
    });
    for (const my_thread_id target_thread_id : counter.target_thread_ids()) {
      Preserve_trx_batch_thd_state target_state{
          Preserve_trx_batch_thd_state::NONE};
      bool temp_unsupported_boundary_seen = false;
      Preserve_trx_batch_wait_result target_wait_result =
          preserve_trx_batch_wait_target_ready(
              thd, generation, target_thread_id, &target_state,
              &temp_unsupported_boundary_seen, target_wait_deadline_us,
              active_drain_attempt, active_binlog_progress);
      if (reset_requested())
        return finish_reset_from_current_items("reset_during_target_wait");
      if (target_wait_result == Preserve_trx_batch_wait_result::DEADLINE &&
          timeout_exclusion_enabled) {
        Preserve_batch_timeout_target_decision decision(generation,
                                                        target_thread_id);
        Global_THD_manager::get_instance()->do_for_all_thd_copy(&decision);
        if (!decision.seen()) {
          target_wait_result =
              Preserve_trx_batch_wait_result::TARGET_NOT_FOUND;
        } else if (decision.timed_out()) {
          ++phase2_metrics.closing_command_timed_out_count;
          const bool declared =
              batch_transfer_phase1_declared_tokens.count(target_thread_id) !=
              0;
          if (declared) {
            DEBUG_SYNC(thd, "preserve_trx_legacy_timeout_before_abort");
            if (reset_requested())
              return finish_reset_from_current_items(
                  "reset_before_legacy_timeout_abort");
            const Preserve_trx_transfer_status abort_status =
                batch_transfer_source_session->abort_token(
                    static_cast<uint64_t>(target_thread_id),
                    "closing_command_timeout");
            if (abort_status != Preserve_trx_transfer_status::OK) {
              if (reset_requested())
                return finish_reset_from_current_items(
                    "reset_during_legacy_timeout_abort");
              Preserve_batch_clear_generation clear(generation);
              Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
              abort_batch_transfer_epoch("closing_timeout_abort_failed");
              abort_drain_participants("closing_timeout_abort_failed");
              return preserve_trx_reject_unsupported();
            }
            batch_transfer_phase1_declared_tokens.erase(target_thread_id);
          }
          excluded_timeout_thread_ids.push_back(target_thread_id);
          if (reset_requested())
            return finish_reset_from_current_items(
                "reset_after_legacy_timeout_exclusion");
          continue;
        } else {
          target_wait_result = Preserve_trx_batch_wait_result::READY;
          target_state = decision.state();
          temp_unsupported_boundary_seen =
              decision.temp_unsupported_boundary_seen();
        }
      }
      if (target_wait_result != Preserve_trx_batch_wait_result::READY ||
          target_state == Preserve_trx_batch_thd_state::PENDING_QUIESCE ||
          target_state == Preserve_trx_batch_thd_state::NONE) {
        const bool deadline_expired =
            target_wait_result == Preserve_trx_batch_wait_result::DEADLINE;
        if (two_phase_enabled &&
            deadline_expired) {
          ++phase2_metrics.closing_command_timed_out_count;
        }
        Preserve_batch_clear_generation clear(generation);
        Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
        abort_batch_transfer_epoch("target_wait_failed");
        abort_drain_participants("target_wait_failed");
        return deadline_expired ? preserve_trx_reject_drain_timeout()
                                : preserve_trx_reject_unsupported();
      }

      if (temp_unsupported_boundary_seen) {
        if (reset_requested())
          return finish_reset_from_current_items(
              "reset_during_temp_boundary_validation");
        Preserve_batch_clear_generation clear(generation);
        Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
        abort_batch_transfer_epoch("temp_unsupported_boundary");
        abort_drain_participants("temp_unsupported_boundary");
        return preserve_trx_reject_unsupported();
      }

      if (target_state ==
          Preserve_trx_batch_thd_state::DRAINED_NO_TRANSACTION) {
        Preserve_batch_clear_target_generation clear_target(generation,
                                                            target_thread_id);
        Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear_target);
        continue;
      }

      if (target_state != Preserve_trx_batch_thd_state::QUIESCED) {
        if (reset_requested())
          return finish_reset_from_current_items(
              "reset_during_target_state_validation");
        Preserve_batch_clear_generation clear(generation);
        Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
        abort_batch_transfer_epoch("target_state_rejected");
        abort_drain_participants("target_state_rejected");
        return preserve_trx_reject_unsupported();
      }

      quiesced_target_thread_ids.push_back(target_thread_id);
    }
  }
  phase2_transfer_tail_started_us =
      early_pipeline_enabled && !command_boundary_sample_missing
          ? latest_command_boundary_us
          : phase2_metrics.closing_started_us;

  if (quiesced_target_thread_ids.empty()) {
    if (close_warmcopy_participants_for_shutdown("no_quiesced_close_failed")) {
      if (reset_requested())
        return finish_reset_from_current_items(
            "reset_during_no_quiesced_phase1_close");
      return preserve_trx_reject_unsupported();
    }
    abort_batch_transfer_epoch("no_quiesced_targets");
    return finish_with_shutdown();
  }

  if (!early_pipeline_enabled) {
    Preserve_batch_quiesced_target_counter ready_counter(
        thd, generation, quiesced_target_thread_ids);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&ready_counter);
    /* Recheck the exact quiesced set after every pending target converged. */
    if (static_cast<size_t>(ready_counter.target_count()) !=
            quiesced_target_thread_ids.size() ||
        ready_counter.has_unsupported_transaction()) {
      if (reset_requested())
        return finish_reset_from_current_items(
            "reset_during_quiesced_target_validation");
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      abort_batch_transfer_epoch("quiesced_counter_rejected");
      abort_drain_participants("quiesced_counter_rejected");
      return preserve_trx_reject_unsupported();
    }

    if (warmcopy_participant != nullptr) {
      /*
        Phase-2 participant prepare consumes only the final quiesced target
        list. Any participant that cannot seal its artifacts must fail before
        the transaction prepare path observes a mixed preserve artifact.
      */
      timed_started_us = preserve_trx_monotonic_us();
      if (!warmcopy_participant->prepare_quiesced_targets(
              quiesced_target_thread_ids, overall_close_deadline_us)) {
        phase2_metrics.participant_prepare_us += elapsed_since(timed_started_us);
        if (reset_requested())
          return finish_reset_from_current_items(
              "reset_during_warmcopy_participant_prepare");
        Preserve_batch_clear_generation clear(generation);
        Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
        abort_batch_transfer_epoch("participant_phase2_prepare_rejected");
        abort_drain_participants("participant_phase2_prepare_rejected");
        return preserve_trx_reject_unsupported();
      }
      phase2_metrics.participant_prepare_us += elapsed_since(timed_started_us);
    }
    if (lock_warmcopy_participant != nullptr) {
      std::vector<uint64_t> lock_warmcopy_target_thread_ids;
      lock_warmcopy_target_thread_ids.reserve(
          quiesced_target_thread_ids.size());
      for (const my_thread_id target_thread_id : quiesced_target_thread_ids) {
        lock_warmcopy_target_thread_ids.push_back(
            static_cast<uint64_t>(target_thread_id));
      }
      /*
        Lock warmcopy follows the same fail-closed boundary. A rejected or stale
        lock artifact is handled before target prepare, either by per-target
        live fallback or by rejecting the batch according to the configured
        policy.
      */
      timed_started_us = preserve_trx_monotonic_us();
      if (!lock_warmcopy_participant->prepare_quiesced_targets(
              lock_warmcopy_target_thread_ids)) {
        phase2_metrics.participant_prepare_us += elapsed_since(timed_started_us);
        if (reset_requested())
          return finish_reset_from_current_items(
              "reset_during_lock_participant_prepare");
        Preserve_batch_clear_generation clear(generation);
        Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
        abort_batch_transfer_epoch("lock_warmcopy_phase2_prepare_rejected");
        abort_drain_participants("lock_warmcopy_phase2_prepare_rejected");
        return preserve_trx_reject_unsupported();
      }
      phase2_metrics.participant_prepare_us += elapsed_since(timed_started_us);
    }
  }
  if (two_phase_enabled) {
    timed_started_us = preserve_trx_monotonic_us();
    if (drain_orchestrator.close_phase1_participants() !=
        Preserve_trx_drain_status::OK) {
      phase2_metrics.participant_close_us += elapsed_since(timed_started_us);
      if (reset_requested())
        return finish_reset_from_current_items(
            "reset_during_phase1_participant_close");
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      abort_batch_transfer_epoch("close_phase1_failed");
      abort_drain_participants("close_phase1_failed");
      return preserve_trx_reject_unsupported();
    }
    phase2_metrics.participant_close_us += elapsed_since(timed_started_us);

    timed_started_us = preserve_trx_monotonic_us();
    const Preserve_trx_drain_status ready_status =
        drain_orchestrator.ensure_phase1_ready();
    const Preserve_trx_drain_status preflight_status =
        ready_status == Preserve_trx_drain_status::OK
            ? drain_orchestrator.phase2_preflight_participants()
            : ready_status;
    phase2_metrics.participant_preflight_us += elapsed_since(timed_started_us);
    if (ready_status != Preserve_trx_drain_status::OK ||
        preflight_status != Preserve_trx_drain_status::OK) {
      if (reset_requested())
        return finish_reset_from_current_items(
            "reset_during_phase1_participant_preflight");
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      abort_batch_transfer_epoch("phase1_not_ready");
      abort_drain_participants("phase1_not_ready");
      return preserve_trx_reject_unsupported();
    }
  }
  if (!early_pipeline_enabled && warmcopy_participant != nullptr &&
      batch_transfer_binlog_blob_provider == nullptr) {
    if (!warmcopy_participant->tail_budget_within_limits(
            quiesced_target_thread_ids, overall_close_deadline_us)) {
      if (reset_requested())
        return finish_reset_from_current_items(
            "reset_during_phase2_tail_budget");
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      abort_batch_transfer_epoch("participant_phase2_budget_rejected");
      abort_drain_participants("participant_phase2_budget_rejected");
      return preserve_trx_reject_unsupported();
    }
  }
  if (two_phase_enabled) {
    if (!draining->transition_to(Preserve_trx_manager_state::BATCH_DRAINING)) {
      if (reset_requested())
        return finish_reset_from_current_items(
            "reset_before_batch_draining_transition");
      return preserve_trx_reject_unsupported();
    }
  }
  if (reset_requested())
    return finish_reset_from_current_items("reset_before_phase2_catchup");

  DEBUG_SYNC(thd, "preserve_trx_batch_after_targets_quiesced_before_attach");

  if (abort_phase1_transfer_targets_not_quiesced(quiesced_target_thread_ids)) {
    if (reset_requested())
      return finish_reset_from_current_items(
          "reset_during_phase1_target_abort");
    Preserve_batch_clear_generation clear(generation);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
    return preserve_trx_reject_unsupported();
  }

  auto missing_target_sample = [](const std::vector<my_thread_id> &sample)
      -> std::string {
    std::ostringstream out;
    for (size_t i = 0; i < sample.size(); ++i) {
      if (i != 0) out << ",";
      out << static_cast<unsigned long long>(sample[i]);
    }
    return out.str();
  };
  auto append_missing_target_sample = [](std::vector<my_thread_id> *sample,
                                         my_thread_id thread_id) {
    if (sample != nullptr && sample->size() < 8) sample->push_back(thread_id);
  };
  struct Batch_transfer_final_target_coverage {
    bool pin_error{false};
    size_t phase1_declared_count{0};
    size_t record_blob_count{0};
    size_t record_fallback_count{0};
    size_t binlog_blob_count{0};
    std::map<my_thread_id, THD *> pinned_by_thread_id;
    std::vector<my_thread_id> missing_declared_sample;
    std::vector<my_thread_id> missing_record_sample;
    std::vector<my_thread_id> missing_binlog_sample;

    bool complete(size_t target_count) const {
      return !pin_error && phase1_declared_count == target_count &&
             record_blob_count == target_count && binlog_blob_count == target_count;
    }

    size_t missing_record_count(size_t target_count) const {
      return target_count >= record_blob_count ? target_count - record_blob_count
                                               : 0;
    }

    size_t missing_binlog_count(size_t target_count) const {
      return target_count >= binlog_blob_count ? target_count - binlog_blob_count
                                               : 0;
    }
  };
  auto collect_batch_transfer_final_target_coverage = [&]() {
        Batch_transfer_final_target_coverage coverage;
        if (batch_transfer_source_session == nullptr) return coverage;

        Preserve_batch_quiesced_target_pin_collector pin_collector(
            thd, generation, quiesced_target_thread_ids);
        Global_THD_manager::get_instance()->do_for_all_thd_copy(
            &pin_collector);

        for (const Preserve_trx_pinned_thd &pinned :
             pin_collector.targets()) {
          if (pinned.thd == nullptr) continue;
          coverage.pinned_by_thread_id[pinned.thd->thread_id()] = pinned.thd;
        }

        for (const my_thread_id target_thread_id :
             quiesced_target_thread_ids) {
          if (batch_transfer_phase1_declared_tokens.count(target_thread_id) !=
              0) {
            ++coverage.phase1_declared_count;
          } else {
            append_missing_target_sample(&coverage.missing_declared_sample,
                                         target_thread_id);
          }

          const auto pinned_it =
              coverage.pinned_by_thread_id.find(target_thread_id);

          bool record_covered = false;
          if (lock_warmcopy_participant == nullptr) {
            record_covered = true;
          } else {
            PrebuiltRecordLocksBlob record_blob;
            if (lock_warmcopy_participant
                    ->phase1_record_prebuilt_blob_for_thread(
                        static_cast<uint64_t>(target_thread_id),
                        &record_blob)) {
              record_covered = true;
            } else {
              const Preserve_trx_lock_warmcopy_artifact *artifact =
                  lock_warmcopy_participant->artifact_for_thread(
                      static_cast<uint64_t>(target_thread_id));
              record_covered =
                  artifact != nullptr && artifact->valid &&
                  artifact->reason == Preserve_trx_lock_warmcopy_reason::OK &&
                  artifact->record_lock_count == 0 &&
                  !artifact->has_prebuilt_record_locks_blob &&
                  artifact->record_locks_payload.empty();
              if (!record_covered) {
                const Preserve_trx_lock_warmcopy_route route =
                    preserve_trx_lock_warmcopy_route_artifact(
                        artifact, preserve_trx_lock_warmcopy_current_options());
                if (route.action ==
                    Preserve_trx_lock_warmcopy_route_action::
                        FALLBACK_TO_LIVE_EXPORT) {
                  ++coverage.record_fallback_count;
                  record_covered = true;
                }
              }
            }
          }
          if (record_covered) {
            ++coverage.record_blob_count;
          } else {
            append_missing_target_sample(&coverage.missing_record_sample,
                                         target_thread_id);
          }

          bool binlog_cache_present = false;
          bool binlog_check_error = false;
          if (pinned_it != coverage.pinned_by_thread_id.end()) {
            uint64_t cache_length = 0;
            if (mysql_binlog_preserve_warmcopy_cache_length(
                    pinned_it->second, &cache_length, &binlog_cache_present)) {
              binlog_check_error = true;
            }
          } else {
            binlog_check_error = true;
          }
          const bool binlog_covered =
              (!binlog_check_error && !binlog_cache_present) ||
              (pinned_it != coverage.pinned_by_thread_id.end() &&
               batch_transfer_binlog_blob_provider != nullptr &&
               batch_transfer_binlog_blob_provider
                   ->phase1_blob_matches_current_thd(pinned_it->second));
          if (binlog_covered) {
            ++coverage.binlog_blob_count;
          } else {
            append_missing_target_sample(&coverage.missing_binlog_sample,
                                         target_thread_id);
          }
        }
        coverage.pin_error = pin_collector.error();
        return coverage;
      };

  auto log_batch_transfer_final_target_coverage =
      [&](const Batch_transfer_final_target_coverage &coverage,
          const char *event, int level) {
    std::ostringstream message;
    message << "PRESERVE: standby transfer final target coverage"
            << " event=" << (event == nullptr ? "check" : event)
            << " target_count=" << quiesced_target_thread_ids.size()
            << " phase1_declared=" << coverage.phase1_declared_count
            << " missing_declared="
            << (quiesced_target_thread_ids.size() -
                coverage.phase1_declared_count)
            << " record_blob=" << coverage.record_blob_count
            << " record_fallback=" << coverage.record_fallback_count
            << " missing_record="
            << coverage.missing_record_count(quiesced_target_thread_ids.size())
            << " binlog_blob=" << coverage.binlog_blob_count
            << " missing_binlog="
            << coverage.missing_binlog_count(quiesced_target_thread_ids.size())
            << " pinned=" << coverage.pinned_by_thread_id.size()
            << " pin_error=" << (coverage.pin_error ? 1 : 0)
            << " missing_declared_sample="
            << missing_target_sample(coverage.missing_declared_sample)
            << " missing_record_sample="
            << missing_target_sample(coverage.missing_record_sample)
            << " missing_binlog_sample="
            << missing_target_sample(coverage.missing_binlog_sample);
    LogErr(level, ER_LOG_PRINTF_MSG, message.str().c_str());
  };

  auto stream_phase2_transfer_record_lock_catchup_blobs = [&]() {
    if (batch_transfer_source_session == nullptr ||
        lock_warmcopy_participant == nullptr) {
      return false;
    }

    const ulonglong catchup_started_us = preserve_trx_monotonic_us();
    uint64_t catchup_tokens = 0;
    uint64_t catchup_bytes = 0;
    uint64_t not_required_tokens = 0;
    uint64_t fallback_tokens = 0;
    for (const my_thread_id target_thread_id : quiesced_target_thread_ids) {
      PrebuiltRecordLocksBlob record_blob;
      const Preserve_trx_lock_warmcopy_record_blob_status blob_status =
          lock_warmcopy_participant
              ->ensure_quiesced_record_prebuilt_blob_for_thread(
                  static_cast<uint64_t>(target_thread_id), &record_blob);
      if (blob_status ==
          Preserve_trx_lock_warmcopy_record_blob_status::NOT_REQUIRED) {
        ++not_required_tokens;
        continue;
      }
      if (blob_status != Preserve_trx_lock_warmcopy_record_blob_status::OK) {
        const Preserve_trx_lock_warmcopy_artifact *artifact =
            lock_warmcopy_participant->artifact_for_thread(
                static_cast<uint64_t>(target_thread_id));
        const Preserve_trx_lock_warmcopy_route route =
            preserve_trx_lock_warmcopy_route_artifact(
                artifact, preserve_trx_lock_warmcopy_current_options());
        if (route.action ==
            Preserve_trx_lock_warmcopy_route_action::FALLBACK_TO_LIVE_EXPORT) {
          ++fallback_tokens;
          continue;
        }
        const std::string message =
            "PRESERVE: standby transfer phase2 record catchup rejected "
            "target=" +
            std::to_string(static_cast<unsigned long long>(target_thread_id));
        LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
        abort_batch_transfer_epoch(
            "standby_transfer_phase2_record_catchup_rejected");
        abort_drain_participants(
            "standby_transfer_phase2_record_catchup_rejected");
        return true;
      }

      Preserve_trx_transfer_object_descriptor descriptor;
      descriptor.object_id = record_blob.name;
      descriptor.kind = Preserve_trx_transfer_object_kind::EXTERNAL_BLOB;
      descriptor.total_size = record_blob.size;
      descriptor.digest = record_blob.digest;
      const bool already_presealed =
          batch_transfer_source_session->object_presealed_for_token(
              static_cast<uint64_t>(target_thread_id), descriptor);

      const Preserve_trx_transfer_status stream_status =
          preserve_trx_transfer_stream_prebuilt_record_locks_blob(
              batch_transfer_source_session.get(),
              static_cast<uint64_t>(target_thread_id),
              preserve_trx_default_dir(), record_blob);
      if (stream_status != Preserve_trx_transfer_status::OK) {
        const std::string message =
            "PRESERVE: standby transfer phase2 record catchup stream failed "
            "status=" +
            std::to_string(static_cast<int>(stream_status)) + " target=" +
            std::to_string(static_cast<unsigned long long>(target_thread_id));
        LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
        abort_batch_transfer_epoch(
            "standby_transfer_phase2_record_catchup_stream_failed");
        abort_drain_participants(
            "standby_transfer_phase2_record_catchup_stream_failed");
        return true;
      }
      if (!already_presealed) {
        ++catchup_tokens;
        catchup_bytes += record_blob.size;
      }
    }

    const ulonglong catchup_elapsed_us =
        preserve_trx_monotonic_us() - catchup_started_us;
    const std::string message =
        "PRESERVE: standby transfer phase2 record catchup"
        " target_count=" +
        std::to_string(quiesced_target_thread_ids.size()) +
        " streamed_tokens=" + std::to_string(catchup_tokens) +
        " not_required_tokens=" + std::to_string(not_required_tokens) +
        " fallback_tokens=" + std::to_string(fallback_tokens) +
        " bytes=" + std::to_string(catchup_bytes) +
        " elapsed_us=" + std::to_string(catchup_elapsed_us);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return false;
  };

  auto stream_phase2_transfer_binlog_cache_catchup_blobs = [&]() {
    if (batch_transfer_source_session == nullptr ||
        batch_transfer_binlog_blob_provider == nullptr) {
      return false;
    }

    Preserve_batch_quiesced_target_pin_collector pin_collector(
        thd, generation, quiesced_target_thread_ids);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&pin_collector);
    if (pin_collector.error()) {
      abort_batch_transfer_epoch(
          "standby_transfer_phase2_binlog_catchup_pin_failed");
      abort_drain_participants(
          "standby_transfer_phase2_binlog_catchup_pin_failed");
      return true;
    }

    const ulonglong catchup_started_us = preserve_trx_monotonic_us();
    uint64_t catchup_tokens = 0;
    uint64_t catchup_bytes = 0;
    uint64_t already_current_tokens = 0;
    uint64_t not_required_tokens = 0;
    std::vector<Preserve_trx_pending_binlog_publication>
        pending_publications;
    pending_publications.reserve(pin_collector.targets().size());
    for (const Preserve_trx_pinned_thd &target : pin_collector.targets()) {
      if (target.thd == nullptr) continue;
      uint64_t streamed_bytes = 0;
      const char *failure_stage = nullptr;
      const Preserve_trx_binlog_catchup_result result =
          stream_quiesced_transfer_binlog_cache_catchup(
              target.thd, generation, batch_transfer_source_session.get(),
              batch_transfer_binlog_blob_provider.get(),
              warmcopy_participant == nullptr
                  ? nullptr
                  : warmcopy_participant->provider(),
              batch_transfer_phase1_sender.get(), &pending_publications,
              &streamed_bytes,
              &failure_stage);
      switch (result) {
        case Preserve_trx_binlog_catchup_result::CURRENT:
          ++already_current_tokens;
          break;
        case Preserve_trx_binlog_catchup_result::NOT_REQUIRED:
          ++not_required_tokens;
          break;
        case Preserve_trx_binlog_catchup_result::QUEUED:
        case Preserve_trx_binlog_catchup_result::STREAMED:
          ++catchup_tokens;
          catchup_bytes += streamed_bytes;
          break;
        case Preserve_trx_binlog_catchup_result::FAILED: {
          const std::string reason =
              "standby_transfer_phase2_binlog_catchup_" +
              std::string(failure_stage == nullptr ? "failed" : failure_stage);
          const std::string message =
              "PRESERVE: standby transfer phase2 binlog catchup failed target=" +
              std::to_string(static_cast<unsigned long long>(
                  target.thd->thread_id())) +
              " reason=" + reason;
          LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
          abort_batch_transfer_epoch(reason.c_str());
          abort_drain_participants(reason.c_str());
          return true;
        }
      }
    }
    if (batch_transfer_phase1_sender != nullptr) {
      const Preserve_trx_transfer_status flush_status =
          batch_transfer_phase1_sender->flush();
      batch_transfer_phase1_sender.reset();
      if (flush_status != Preserve_trx_transfer_status::OK) {
        abort_batch_transfer_epoch(
            "standby_transfer_phase2_binlog_catchup_flush_failed");
        abort_drain_participants(
            "standby_transfer_phase2_binlog_catchup_flush_failed");
        return true;
      }
    }
    publish_acked_transfer_binlog_cache_progress(
        batch_transfer_binlog_blob_provider.get(),
        warmcopy_participant == nullptr ? nullptr
                                        : warmcopy_participant->provider(),
        pending_publications);

    const ulonglong catchup_elapsed_us =
        preserve_trx_monotonic_us() - catchup_started_us;
    const std::string message =
        "PRESERVE: standby transfer phase2 binlog catchup"
        " target_count=" +
        std::to_string(quiesced_target_thread_ids.size()) +
        " streamed_tokens=" + std::to_string(catchup_tokens) +
        " already_current_tokens=" + std::to_string(already_current_tokens) +
        " not_required_tokens=" + std::to_string(not_required_tokens) +
        " bytes=" + std::to_string(catchup_bytes) +
        " elapsed_us=" + std::to_string(catchup_elapsed_us);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return false;
  };

  auto validate_batch_transfer_final_target_coverage = [&]() {
    if (batch_transfer_source_session == nullptr) return true;

    Batch_transfer_final_target_coverage coverage =
        collect_batch_transfer_final_target_coverage();
    log_batch_transfer_final_target_coverage(coverage, "check",
                                             INFORMATION_LEVEL);
    if (coverage.complete(quiesced_target_thread_ids.size())) return true;
    return false;
  };
  if (!early_pipeline_enabled) {
    if (stream_phase2_transfer_record_lock_catchup_blobs()) {
      if (reset_requested())
        return finish_reset_from_current_items(
            "reset_during_phase2_record_catchup");
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      return preserve_trx_reject_unsupported();
    }
    if (stream_phase2_transfer_binlog_cache_catchup_blobs()) {
      if (reset_requested())
        return finish_reset_from_current_items(
            "reset_during_phase2_binlog_catchup");
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      return preserve_trx_reject_unsupported();
    }
    if (!validate_batch_transfer_final_target_coverage()) {
      if (reset_requested())
        return finish_reset_from_current_items(
            "reset_during_final_target_coverage");
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      abort_batch_transfer_epoch(
          "standby_transfer_final_target_coverage_rejected");
      abort_drain_participants(
          "standby_transfer_final_target_coverage_rejected");
      return preserve_trx_reject_unsupported();
    }
  }
  DEBUG_SYNC(thd, "preserve_trx_batch_after_final_transfer_catchup");
  if (reset_requested())
    return finish_reset_from_current_items("reset_before_token_selection");

  bool token_selection_failed = false;
  if (!batch_tokens_selected) {
    const ulonglong token_selection_started_us = preserve_trx_monotonic_us();
    token_selection_failed = select_batch_tokens(quiesced_target_thread_ids);
    if (batch_transfer_source_session != nullptr) {
      const std::string message =
          "PRESERVE: standby transfer source token selection token_count=" +
          std::to_string(batch_tokens.size()) + " elapsed_us=" +
          std::to_string(elapsed_since(token_selection_started_us));
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    }
  }
  if (token_selection_failed) {
    if (reset_requested())
      return finish_reset_from_current_items("reset_during_token_selection");
    Preserve_batch_clear_generation clear(generation);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
    abort_batch_transfer_epoch("batch_token_selection_failed");
    abort_drain_participants("batch_token_selection_failed");
    return preserve_trx_reject_unsupported();
  }

  if (early_pipeline_enabled) {
    const ulonglong final_candidate_pass_started_us =
        preserve_trx_monotonic_us();
    uint64_t final_candidate_finalize_us = 0;
    uint64_t final_candidate_finalize_max_us = 0;
    my_thread_id final_candidate_finalize_max_target = 0;
    bool dirty_injected = false;
    for (Preserve_batch_target_execution &execution : target_results) {
      const ulonglong stamp_started_us = preserve_trx_monotonic_us();
      lock_warmcopy_trx_lock_fence_t current_fence;
      const bool stamp_valid = execution.initial_lock_fence_valid &&
                               !execution.error &&
                               trx_preserve_sample_lock_warmcopy_fence(
                                   execution.result.preserved_trx,
                                   &current_fence);
      if (!stamp_valid) {
        phase2_metrics.final_fast_scan_us += elapsed_since(stamp_started_us);
        ++phase2_metrics.final_validation_rejects;
        execution.error = true;
        execution.result.failure_reason = "early_lock_fence_unavailable";
        continue;
      }
      bool dirty = !preserve_trx_early_lock_fence_matches(
          execution.initial_lock_fence, current_fence);
      phase2_metrics.final_fast_scan_us += elapsed_since(stamp_started_us);
      if (debug_force_one_early_coordinate_drift && !dirty_injected) {
        if (current_fence.coordinate_generation ==
            std::numeric_limits<uint64_t>::max()) {
          ++phase2_metrics.final_validation_rejects;
          execution.error = true;
          execution.result.failure_reason =
              "early_lock_generation_exhausted";
          continue;
        }
        ++current_fence.coordinate_generation;
        dirty = true;
        dirty_injected = true;
      }
      if (dirty) {
        ++phase2_metrics.final_dirty_tokens;
        if (!preserve_trx_replace_early_record_lock_blob(
                lock_warmcopy_participant.get(),
                batch_transfer_source_session.get(), &execution,
                current_fence)) {
          ++phase2_metrics.final_validation_rejects;
          execution.error = true;
          execution.failure_reason = Preserve_batch_target_execution::
              Failure_reason::LOCK_PLAN_REPLACEMENT_FAILED;
          execution.result.failure_reason = "early_lock_replacement_failed";
          continue;
        }
        ++phase2_metrics.final_replacement_tokens;
        DEBUG_SYNC(thd, "preserve_trx_early_after_dirty_replacement");
      }
      const ulonglong finalize_started_us = preserve_trx_monotonic_us();
      const bool inject_finalize_failure =
          debug_fail_early_candidate_finalize;
      debug_fail_early_candidate_finalize = false;
      const Preserve_trx_transfer_status finalize_status =
          inject_finalize_failure
              ? Preserve_trx_transfer_status::UNSUPPORTED
              : preserve_trx_transfer_finalize_deferred_candidate(
                    batch_transfer_source_session.get(),
                    &execution.deferred_candidate);
      const uint64_t finalize_us = elapsed_since(finalize_started_us);
      final_candidate_finalize_us += finalize_us;
      if (finalize_us > final_candidate_finalize_max_us) {
        final_candidate_finalize_max_us = finalize_us;
        final_candidate_finalize_max_target = execution.target_thread_id;
      }
      if (finalize_status != Preserve_trx_transfer_status::OK) {
        execution.error = true;
        execution.failure_reason = Preserve_batch_target_execution::
            Failure_reason::FINAL_METADATA_BUILD_FAILED;
        execution.result.failure_reason = "early_candidate_finalize_failed";
      }
    }
    LogErr(
        INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
        ("PRESERVE: early final candidate pass target_count=" +
         std::to_string(target_results.size()) +
         " wall_us=" +
         std::to_string(elapsed_since(final_candidate_pass_started_us)) +
         " finalize_us=" + std::to_string(final_candidate_finalize_us) +
         " finalize_max_us=" +
         std::to_string(final_candidate_finalize_max_us) +
         " finalize_max_target=" +
         std::to_string(static_cast<unsigned long long>(
             final_candidate_finalize_max_target)))
            .c_str());

    const size_t token_local_failure_count = static_cast<size_t>(std::count_if(
        target_results.begin(), target_results.end(),
        [](const Preserve_batch_target_execution &execution) {
          return execution.failure_reason !=
                 Preserve_batch_target_execution::Failure_reason::NONE;
        }));
    std::set<my_thread_id> retained_failed_targets;
    if (token_local_failure_count != 0) {
      for (Preserve_batch_target_execution &execution : target_results) {
        if (execution.failure_reason ==
            Preserve_batch_target_execution::Failure_reason::NONE) {
          continue;
        }
        const bool exact_local_failure =
            execution.result.stage == Preserve_trx_preserve_stage::COMPLETE &&
            !execution.result.token.empty() &&
            !execution.result.cleanup_failed_after_reattach &&
            !execution.result.left_preserved_after_cleanup_failure &&
            batch_transfer_phase1_declared_tokens.count(
                execution.target_thread_id) != 0;
        if (!exact_local_failure ||
            batch_transfer_source_session->abort_token(
                static_cast<uint64_t>(execution.target_thread_id),
                preserve_trx_source_failure_reason_name(
                    execution.failure_reason)) !=
                Preserve_trx_transfer_status::OK) {
          break;
        }
        batch_transfer_phase1_declared_tokens.erase(
            execution.target_thread_id);

        Preserve_trx_batch_item failed_item;
        failed_item.original_thread_id = execution.target_thread_id;
        failed_item.token = execution.result.token;
        failed_item.logged_binlog_cache = execution.result.logged_binlog_cache;
        failed_item.local_authority_staged =
            execution.result.local_authority_staged;
        failed_item.source_rollback_image =
            std::move(execution.result.source_rollback_image);
        source_failed_batch_items.push_back(std::move(failed_item));
        execution.batch_item_collected = true;

        const auto token_it = batch_tokens_by_thread_id.find(
            execution.target_thread_id);
        if (token_it == batch_tokens_by_thread_id.end()) {
          abort_batch_transfer_epoch("source_token_identity_missing");
          return finish_cleanup_failure_without_shutdown(
              "source_token_identity_missing");
        }
        const std::string failed_token = token_it->second;
        batch_tokens_by_thread_id.erase(token_it);
        batch_tokens.erase(
            std::remove(batch_tokens.begin(),
                        batch_tokens.end(), failed_token),
            batch_tokens.end());
        retained_failed_targets.insert(execution.target_thread_id);
        source_failed_tokens.push_back(
            {execution.target_thread_id, execution.failure_reason});
        LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
               ("PRESERVE: source token excluded target_thread_id=" +
                std::to_string(static_cast<unsigned long long>(
                    execution.target_thread_id)) +
                " reason=" +
                preserve_trx_source_failure_reason_name(
                    execution.failure_reason))
                   .c_str());
      }
    }
    if (batch_transfer_source_session->precommit_ack_uncertain()) {
      collect_completed_target_items();
      retain_source_failed_items_in_source_context();
      return enter_uncertain(Preserve_trx_transfer_status::ACK_UNCERTAIN,
                             false);
    }
    if (!retained_failed_targets.empty()) {
      quiesced_target_thread_ids.erase(
          std::remove_if(quiesced_target_thread_ids.begin(),
                         quiesced_target_thread_ids.end(),
                         [&](my_thread_id target_thread_id) {
                           return retained_failed_targets.count(
                                      target_thread_id) != 0;
                         }),
          quiesced_target_thread_ids.end());
      target_results.erase(
          std::remove_if(target_results.begin(), target_results.end(),
                         [&](const Preserve_batch_target_execution &execution) {
                           return retained_failed_targets.count(
                                      execution.target_thread_id) != 0;
                         }),
          target_results.end());
      std::sort(source_failed_tokens.begin(), source_failed_tokens.end(),
                [](const auto &left, const auto &right) {
                  return left.first < right.first;
                });
    }
    if (quiesced_target_thread_ids.empty() &&
        !source_failed_batch_items.empty()) {
      if (close_warmcopy_participants_for_shutdown(
              "source_all_token_local_failed_close_failed")) {
        if (reset_requested())
          return finish_reset_from_current_items(
              "reset_during_source_all_token_local_failed_close");
        return preserve_trx_reject_unsupported();
      }
      abort_batch_transfer_epoch("source_all_token_local_failed");
      retain_source_failed_items_in_source_context();
      return finish_with_shutdown();
    }
  }

  preserved_batch_items.reserve(quiesced_target_thread_ids.size());
  phase2_metrics.target_count = quiesced_target_thread_ids.size();
  timed_started_us = preserve_trx_monotonic_us();
  const size_t target_execution_count =
      debug_fail_after_one_target && !quiesced_target_thread_ids.empty()
          ? 1
          : quiesced_target_thread_ids.size();
  if (!early_pipeline_enabled) target_results.resize(target_execution_count);
  auto apply_target_metrics = [&](const Preserve_trx_preserve_result &result) {
    phase2_metrics.binlog_preflight_us += result.binlog_preflight_us;
    phase2_metrics.lock_preflight_us += result.lock_preflight_us;
    phase2_metrics.lock_preflight_read_view_us +=
        result.lock_preflight_read_view_us;
    phase2_metrics.lock_preflight_mdl_us += result.lock_preflight_mdl_us;
    phase2_metrics.lock_preflight_modified_tables_us +=
        result.lock_preflight_modified_tables_us;
    phase2_metrics.lock_preflight_savepoints_us +=
        result.lock_preflight_savepoints_us;
    phase2_metrics.lock_preflight_predicate_us +=
        result.lock_preflight_predicate_us;
    phase2_metrics.lock_preflight_table_us += result.lock_preflight_table_us;
    phase2_metrics.prepare_us += result.prepare_us;
    phase2_metrics.detach_claim_us += result.detach_claim_us;
    phase2_metrics.snapshot_write_us += result.snapshot_write_us;
    phase2_metrics.snapshot_write_prebuilt_binlog_us +=
        result.snapshot_write_prebuilt_binlog_us;
    phase2_metrics.snapshot_write_temp_manifest_us +=
        result.snapshot_write_temp_manifest_us;
    phase2_metrics.temp_manifest_build_target_count +=
        result.snapshot_write_temp_manifest_built ? 1 : 0;
    phase2_metrics.snapshot_write_bundle_build_us +=
        result.snapshot_write_bundle_build_us;
    phase2_metrics.snapshot_write_store_us += result.snapshot_write_store_us;
    phase2_metrics.snapshot_write_store_token_state_us +=
        result.snapshot_write_store_token_state_us;
    phase2_metrics.snapshot_write_store_adopt_warm_blob_us +=
        result.snapshot_write_store_adopt_warm_blob_us;
    phase2_metrics.snapshot_write_store_write_new_blobs_us +=
        result.snapshot_write_store_write_new_blobs_us;
    phase2_metrics.snapshot_write_store_encode_us +=
        result.snapshot_write_store_encode_us;
    phase2_metrics.snapshot_write_store_write_snapshot_us +=
        result.snapshot_write_store_write_snapshot_us;
    phase2_metrics.register_us += result.record_register_us;
    phase2_metrics.savepoint_live_export_target_count +=
        result.phase2_savepoint_live_export_target_count;
  };
  uint preserve_worker_count =
      preserve_trx_effective_parallel_preserve_threads(
          quiesced_target_thread_ids.size(), lock_warmcopy_batch);
  DEBUG_SYNC(thd, "preserve_trx_batch_before_target_preserve_pins");
  if (debug_fail_after_one_target || debug_fail_after_detach_for_batch_reattach)
    preserve_worker_count = 1;

  if (!early_pipeline_enabled && preserve_worker_count <= 1) {
    for (size_t target_index = 0; target_index < target_results.size();
         ++target_index) {
      if (reset_requested()) break;
      const my_thread_id target_thread_id =
          quiesced_target_thread_ids[target_index];
      ulonglong target_step_started_us = preserve_trx_monotonic_us();
      Preserve_batch_single_quiesced_target_pin target_pin(thd, generation,
                                                           target_thread_id);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&target_pin);
      phase2_metrics.target_pin_us += elapsed_since(target_step_started_us);
      target_results[target_index].pin_error = target_pin.error();
      target_step_started_us = preserve_trx_monotonic_us();
      preserve_one_target(target_thread_id,
                          target_pin.found() ? target_pin.target().thd
                                             : nullptr,
                          thd, nullptr, &target_results[target_index], false,
                          {});
      phase2_metrics.target_worker_wall_us += elapsed_since(target_step_started_us);
      if (reset_requested()) break;
    }
  } else if (!early_pipeline_enabled) {
    ulonglong target_step_started_us = preserve_trx_monotonic_us();
    Preserve_batch_quiesced_target_pin_collector target_pin_collector(
        thd, generation, quiesced_target_thread_ids, true);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(
        &target_pin_collector);
    phase2_metrics.target_pin_us += elapsed_since(target_step_started_us);
    std::vector<Preserve_trx_pinned_thd> pinned_targets =
        std::move(target_pin_collector.targets());
    std::map<my_thread_id, THD *> pinned_by_thread_id;
    for (const Preserve_trx_pinned_thd &pinned : pinned_targets) {
      if (pinned.thd != nullptr) pinned_by_thread_id[pinned.thd->thread_id()] =
                                   pinned.thd;
    }
    if (target_pin_collector.error() ||
        pinned_by_thread_id.size() != quiesced_target_thread_ids.size()) {
      for (size_t target_index = 0;
           target_index < quiesced_target_thread_ids.size(); ++target_index) {
        target_results[target_index].target_thread_id =
            quiesced_target_thread_ids[target_index];
        target_results[target_index].pin_error = true;
      }
    } else {
      std::atomic<size_t> next_target_index{0};
      std::atomic<bool> worker_init_failed{false};
      std::atomic<bool> worker_exception_failed{false};
      std::atomic<size_t> worker_init_reports{0};
      std::atomic<bool> workers_released{false};
      std::atomic<bool> worker_abort{false};
      std::vector<std::thread> workers;
      auto join_workers = create_scope_guard([&] {
        for (std::thread &worker : workers) {
          if (worker.joinable()) worker.join();
        }
      });
      target_step_started_us = preserve_trx_monotonic_us();
      try {
        workers.reserve(preserve_worker_count);
        for (uint worker_index = 0; worker_index < preserve_worker_count;
             ++worker_index) {
          workers.emplace_back([&]() {
            if (my_thread_init()) {
              worker_init_failed.store(true, std::memory_order_relaxed);
              worker_abort.store(true, std::memory_order_release);
              worker_init_reports.fetch_add(1, std::memory_order_release);
              return;
            }
            auto thread_end_guard = create_scope_guard([] { my_thread_end(); });
            worker_init_reports.fetch_add(1, std::memory_order_release);
            while (!workers_released.load(std::memory_order_acquire)) {
              std::this_thread::yield();
            }
            if (worker_abort.load(std::memory_order_acquire)) return;
            try {
              char worker_thread_stack_anchor = 0;
              for (;;) {
                if (worker_abort.load(std::memory_order_acquire) ||
                    reset_requested())
                  break;
                const size_t target_index =
                    next_target_index.fetch_add(1, std::memory_order_relaxed);
                if (target_index >= quiesced_target_thread_ids.size()) break;
                const my_thread_id target_thread_id =
                    quiesced_target_thread_ids[target_index];
                auto target_it = pinned_by_thread_id.find(target_thread_id);
                preserve_one_target(
                    target_thread_id,
                    target_it == pinned_by_thread_id.end() ? nullptr
                                                           : target_it->second,
                    nullptr, &worker_thread_stack_anchor,
                    &target_results[target_index], false, {});
                if (reset_requested()) break;
              }
            } catch (...) {
              worker_exception_failed.store(true, std::memory_order_relaxed);
              worker_abort.store(true, std::memory_order_release);
            }
          });
        }
      } catch (...) {
        worker_exception_failed.store(true, std::memory_order_relaxed);
        worker_abort.store(true, std::memory_order_release);
      }
      if (!worker_abort.load(std::memory_order_acquire)) {
        while (worker_init_reports.load(std::memory_order_acquire) <
               workers.size()) {
          std::this_thread::yield();
        }
        if (worker_init_failed.load(std::memory_order_relaxed)) {
          worker_abort.store(true, std::memory_order_release);
        }
      }
      workers_released.store(true, std::memory_order_release);
      join_workers.rollback();
      phase2_metrics.target_worker_wall_us +=
          elapsed_since(target_step_started_us);
      if (worker_init_failed.load(std::memory_order_relaxed) ||
          worker_exception_failed.load(std::memory_order_relaxed)) {
        for (Preserve_batch_target_execution &execution : target_results) {
          execution.pin_error = true;
        }
      }
    }
  }

  const Preserve_batch_target_execution *failed_execution = nullptr;
  const ulonglong result_collect_started_us = preserve_trx_monotonic_us();
  collect_completed_target_items();
  if (batch_transfer_source_session != nullptr &&
      batch_transfer_source_session->precommit_ack_uncertain()) {
    retain_source_failed_items_in_source_context();
    return enter_uncertain(Preserve_trx_transfer_status::ACK_UNCERTAIN,
                           false);
  }
  for (Preserve_batch_target_execution &execution : target_results) {
    apply_target_metrics(execution.result);

    if (execution.pin_error || !execution.visited_target || execution.error ||
        execution.result.stage != Preserve_trx_preserve_stage::COMPLETE) {
      if (failed_execution == nullptr) failed_execution = &execution;
      continue;
    }
    DEBUG_SYNC(thd, "preserve_trx_batch_after_one_target_preserved");
    if (debug_fail_after_one_target) {
      if (preserved_batch_items.size() == 1 &&
          quiesced_target_thread_ids.size() > 1) {
        if (restore_current_items_to_original_thds()) {
          abort_batch_transfer_epoch("debug_after_one_target_cleanup_failed");
          LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                 "Preserved transaction batch cleanup failed after debug "
                 "injected target error");
          return finish_cleanup_failure_without_shutdown(
              "debug_after_one_target_cleanup_failed");
        }
        Preserve_batch_clear_generation clear(generation);
        Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
        abort_batch_transfer_epoch("debug_after_one_target_failed");
        abort_drain_participants("debug_after_one_target_failed");
        return preserve_trx_reject_unsupported();
      }
    }
  }
  phase2_metrics.target_result_collect_us +=
      elapsed_since(result_collect_started_us);
  if (reset_requested())
    return finish_phase2_reset(&preserved_batch_items,
                                        "reset_after_phase2_workers");
  if (failed_execution != nullptr) {
    const Preserve_trx_preserve_result &batch_result = failed_execution->result;
    /*
      Batch preserve is all-or-nothing. If any target fails, targets that were
      already detached and registered by this batch are restored to their
      original THDs unless the cleanup itself crosses a durable failure point.
    */
    const std::string message =
        "PRESERVE: batch target preserve failed visited=" +
        std::to_string(failed_execution->visited_target ? 1 : 0) +
        " target_thread_id=" +
        std::to_string(
            static_cast<unsigned long long>(failed_execution->target_thread_id)) +
        " error=" + std::to_string(failed_execution->error ? 1 : 0) +
        " stage=" + preserve_trx_preserve_stage_name(batch_result.stage) +
        " reason=" +
        (batch_result.failure_reason == nullptr ? "unknown"
                                                : batch_result.failure_reason) +
        " token_present=" + std::to_string(batch_result.token.empty() ? 0 : 1) +
        " durable_point_crossed=" +
        std::to_string(batch_result.durable_point_crossed ? 1 : 0) +
        " detached_from_original_thd=" +
        std::to_string(batch_result.detached_from_original_thd ? 1 : 0) +
        " reattached_to_original_thd=" +
        std::to_string(batch_result.reattached_to_original_thd ? 1 : 0) +
        " cleanup_completed_after_detach_failure=" +
        std::to_string(batch_result.cleanup_completed_after_detach_failure ? 1
                                                                           : 0) +
        " cleanup_failed_after_reattach=" +
        std::to_string(batch_result.cleanup_failed_after_reattach ? 1 : 0) +
        " left_preserved_after_cleanup_failure=" +
        std::to_string(batch_result.left_preserved_after_cleanup_failure ? 1
                                                                         : 0) +
        " detach_reason=" +
        (batch_result.detach_failure_reason == nullptr
             ? "none"
             : batch_result.detach_failure_reason) +
        " reactivate_reason=" +
        (batch_result.reactivate_failure_reason == nullptr
             ? "none"
             : batch_result.reactivate_failure_reason) +
        " logged_binlog_cache=" +
        std::to_string(batch_result.logged_binlog_cache ? 1 : 0);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    const bool prior_cleanup_error = restore_current_items_to_original_thds();
    const bool cleanup_error =
        batch_result.cleanup_failed_after_reattach || prior_cleanup_error;
    if (cleanup_error) {
      abort_batch_transfer_epoch("target_cleanup_failed");
      if (!batch_result.token.empty()) {
        if (batch_result.left_preserved_after_cleanup_failure) {
          (void)preserved_trx_mark_preserved_with_last_error(
              batch_result.token,
              "batch cleanup failure after target preserve error");
        } else {
          (void)preserved_trx_update_record_error(
              batch_result.token,
              "batch cleanup failure after target preserve error");
        }
      }
      LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
             "Preserved transaction batch cleanup failed after target preserve "
             "error");
      return finish_cleanup_failure_without_shutdown("target_cleanup_failed");
    }
    abort_batch_transfer_epoch("target_preserve_failed");
    abort_drain_participants("target_preserve_failed");
    return preserve_trx_reject_unsupported();
  }

  uint64_t local_authority_max_freeze_lsn = 0;
  bool has_staged_local_authority = false;
  for (const Preserve_batch_target_execution &execution : target_results) {
    if (!execution.result.local_authority_staged) continue;
    has_staged_local_authority = true;
    local_authority_max_freeze_lsn =
        std::max(local_authority_max_freeze_lsn, execution.result.freeze_lsn);
  }
  if (has_staged_local_authority) {
    Preserve_snapshot_write_options commit_options;
    commit_options.defer_directory_fsync =
        defer_batch_snapshot_directory_fsync;
    commit_options.shard_snapshot_files =
        defer_batch_snapshot_directory_fsync;
    auto local_store = create_preserved_trx_default_store(
        preserve_trx_default_dir(), commit_options);
    bool commit_failed =
        trx_preserve_flush_redo_up_to(local_authority_max_freeze_lsn) !=
        DB_SUCCESS;
    for (Preserve_batch_target_execution &execution : target_results) {
      if (commit_failed || !execution.result.local_authority_staged) continue;
      if (local_store->commit_local_authority(execution.result.token) !=
          Preserve_snapshot_status::OK) {
        commit_failed = true;
        break;
      }
      execution.result.local_authority_staged = false;
    }
    if (!commit_failed) {
      for (Preserve_trx_batch_item &item : preserved_batch_items) {
        item.local_authority_staged = false;
      }
    }
    if (commit_failed) {
      const bool cleanup_error = restore_current_items_to_original_thds();
      abort_batch_transfer_epoch("local_authority_commit_failed");
      abort_drain_participants("local_authority_commit_failed");
      if (cleanup_error) {
        return finish_cleanup_failure_without_shutdown(
            "local_authority_commit_cleanup_failed");
      }
      return preserve_trx_reject_unsupported();
    }
  }
  if (!early_pipeline_enabled && defer_batch_snapshot_directory_fsync &&
      !preserved_batch_items.empty()) {
    const ulonglong fsync_started_us = preserve_trx_monotonic_us();
    const Preserve_snapshot_status fsync_status =
        preserve_trx_fsync_default_store_directory(preserve_trx_default_dir());
    const uint64_t dir_fsync_us = elapsed_since(fsync_started_us);
    phase2_metrics.snapshot_write_us += dir_fsync_us;
    phase2_metrics.target_deferred_dir_fsync_us += dir_fsync_us;
    if (fsync_status != Preserve_snapshot_status::OK) {
      if (reset_requested())
        return finish_phase2_reset(
            &preserved_batch_items, "reset_during_deferred_directory_fsync");
      LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
             "Preserved transaction batch directory fsync failed after "
             "deferred snapshot writes");
      const bool cleanup_error = restore_current_items_to_original_thds();
      if (cleanup_error) {
        abort_batch_transfer_epoch(
            "deferred_snapshot_directory_fsync_cleanup_failed");
        LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
               "Preserved transaction batch cleanup failed after deferred "
               "snapshot directory fsync error");
        return finish_cleanup_failure_without_shutdown(
            "deferred_snapshot_directory_fsync_cleanup_failed");
      }
      abort_batch_transfer_epoch("deferred_snapshot_directory_fsync_failed");
      abort_drain_participants("deferred_snapshot_directory_fsync_failed");
      return preserve_trx_reject_unsupported();
    }
  }
  if (early_pipeline_enabled) {
    const ulonglong final_scan_started_us = preserve_trx_monotonic_us();
    bool final_fence_changed = false;
    for (const Preserve_batch_target_execution &execution : target_results) {
      lock_warmcopy_trx_lock_fence_t current_fence;
      if (!trx_preserve_sample_lock_warmcopy_fence(
              execution.result.preserved_trx, &current_fence) ||
          !preserve_trx_early_lock_fence_matches(execution.initial_lock_fence,
                                                 current_fence)) {
        final_fence_changed = true;
        break;
      }
    }
    phase2_metrics.final_fast_scan_us +=
        elapsed_since(final_scan_started_us);
    if (debug_force_early_final_fence_change) final_fence_changed = true;
    if (final_fence_changed) {
      ++phase2_metrics.final_validation_rejects;
      const bool cleanup_error = restore_current_items_to_original_thds();
      abort_batch_transfer_epoch("early_final_lock_fence_changed");
      abort_drain_participants("early_final_lock_fence_changed");
      if (cleanup_error) {
        return finish_cleanup_failure_without_shutdown(
            "early_final_lock_fence_cleanup_failed");
      }
      return preserve_trx_reject_unsupported();
    }
  }
  if (reset_requested())
    return finish_phase2_reset(&preserved_batch_items,
                                        "reset_before_transfer_commit");
  retain_source_failed_items_in_source_context();
  if (batch_transfer_source_session != nullptr) {
    const ulonglong commit_epoch_started_us = preserve_trx_monotonic_us();
    const Preserve_trx_transfer_status commit_status =
        batch_transfer_source_session->commit_epoch();
    phase2_metrics.transfer_commit_epoch_us +=
        elapsed_since(commit_epoch_started_us);
    if (commit_status == Preserve_trx_transfer_status::ACK_UNCERTAIN &&
        batch_transfer_source_session->precommit_ack_uncertain() &&
        active_drain_attempt != nullptr &&
        active_drain_attempt->ownership.state() ==
            Preserve_trx_drain_terminal::RUNNING) {
      return enter_uncertain(commit_status, false);
    }
    const bool receiver_committed =
        commit_status == Preserve_trx_transfer_status::COMMITTED_READY ||
        commit_status == Preserve_trx_transfer_status::COMMITTED_NOT_READY;
    const bool receiver_commit_succeeded =
        commit_status == Preserve_trx_transfer_status::OK ||
        commit_status == Preserve_trx_transfer_status::COMMITTED_READY ||
        commit_status == Preserve_trx_transfer_status::COMMITTED_NOT_READY;
    if (!receiver_commit_succeeded) {
      if (reset_requested())
        return finish_phase2_reset(
            &preserved_batch_items, "reset_during_transfer_commit");
      if (commit_status ==
          Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN) {
        if (active_drain_attempt == nullptr ||
            !active_drain_attempt->ownership.resolve_not_committed_clean()) {
          return enter_uncertain(commit_status, true);
        }
        preserve_trx_transfer_note_source_handoff_committed();
        const bool cleanup_error = restore_current_items_to_original_thds();
        if (cleanup_error) {
          return finish_cleanup_failure_without_shutdown(
              "standby_transfer_not_committed_clean_restore_failed");
        }
        if (!active_drain_attempt->ownership.complete_source_restore()) {
          return finish_cleanup_failure_without_shutdown(
              "standby_transfer_not_committed_clean_state_failed");
        }
        preserve_trx_publish_active_drain_reset_barrier(
            active_drain_attempt);
        abort_drain_participants(
            "standby_transfer_receiver_not_committed_clean");
        return preserve_trx_reject_unsupported();
      }
      if (active_drain_attempt != nullptr &&
          (active_drain_attempt->ownership.state() ==
               Preserve_trx_drain_terminal::HANDOFF_PENDING ||
           active_drain_attempt->ownership.state() ==
               Preserve_trx_drain_terminal::COMMIT_UNKNOWN)) {
        return enter_uncertain(commit_status, true);
      }
      LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: standby transfer source epoch commit failed status=" +
              std::to_string(static_cast<int>(commit_status)))
                 .c_str());
      if (!receiver_committed) {
        abort_batch_transfer_epoch("standby_transfer_commit_failed");
      }
      const bool cleanup_error = restore_current_items_to_original_thds();
      if (cleanup_error) {
        LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
               "Preserved transaction batch cleanup failed after standby "
               "transfer source epoch commit error");
        return finish_cleanup_failure_without_shutdown(
            "standby_transfer_commit_cleanup_failed");
      }
      abort_drain_participants("standby_transfer_commit_failed");
      return preserve_trx_reject_unsupported();
    }

    transfer_final_ack_accepted =
        active_drain_attempt != nullptr &&
        active_drain_attempt->ownership.state() ==
            Preserve_trx_drain_terminal::COMMITTED_HANDOFF;
    if (!transfer_final_ack_accepted) {
      LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
             "PRESERVE: receiver accepted transfer without terminal source "
             "ownership; source shutdown will continue");
    }

    const ulonglong final_ack_us = preserve_trx_monotonic_us();
    phase2_metrics.phase2_transfer_tail_us =
        phase2_transfer_tail_started_us != 0 &&
                final_ack_us >= phase2_transfer_tail_started_us
            ? final_ack_us - phase2_transfer_tail_started_us
            : 0;
    phase2_metrics.closing_to_final_ack_us =
        phase2_metrics.closing_started_us != 0 &&
                final_ack_us >= phase2_metrics.closing_started_us
            ? final_ack_us - phase2_metrics.closing_started_us
            : 0;

    // FINAL_ACK is the transfer ownership boundary. From this point onward,
    // target prewarm, promotion, or abandon are target-local HA concerns.
    batch_transfer_phase1_flush_context.session = nullptr;

    ulonglong teardown_started_us = preserve_trx_monotonic_us();
    batch_transfer_source_session.reset();
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: standby transfer source post-ack teardown component="
            "source_epoch_session elapsed_us=" +
            std::to_string(elapsed_since(teardown_started_us)))
               .c_str());

    teardown_started_us = preserve_trx_monotonic_us();
    release_batch_transfer_frame_sink();
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: standby transfer source post-ack teardown component="
            "frame_sink elapsed_us=" +
            std::to_string(elapsed_since(teardown_started_us)))
               .c_str());

    teardown_started_us = preserve_trx_monotonic_us();
    retain_reset_source_warmcopy_ids();
    batch_transfer_binlog_blob_provider.reset();
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: standby transfer source post-ack teardown component="
            "binlog_blob_provider elapsed_us=" +
            std::to_string(elapsed_since(teardown_started_us)))
               .c_str());

    teardown_started_us = preserve_trx_monotonic_us();
    batch_transfer_phase1_declared_tokens.clear();
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: standby transfer source post-ack teardown component="
            "declared_tokens elapsed_us=" +
            std::to_string(elapsed_since(teardown_started_us)))
               .c_str());
  }
  phase2_metrics.target_preserve_us += elapsed_since(timed_started_us);

  publish_phase2_metrics();
  if (two_phase_enabled) {
    const ulonglong phase2_pause_us =
        preserve_trx_warmcopy_phase2_pause_us_status();
    uint64_t phase2_lock_seal_us = 0;
    uint64_t phase1_record_prebuilt_target_count = 0;
    uint64_t phase1_record_active_scan_target_count = 0;
    uint64_t phase2_full_lock_scan_count = 0;
    uint64_t materialized_lock_payload_bytes_in_phase2 = 0;
    uint64_t phase2_record_lock_count = 0;
    uint64_t phase2_table_lock_count = 0;
    uint64_t phase2_mdl_descriptor_count = 0;
    uint64_t phase2_table_live_export_target_count = 0;
    uint64_t phase2_mdl_live_export_target_count = 0;
    uint64_t phase2_record_prebuilt_target_count = 0;
    uint64_t phase2_record_materialized_target_count = 0;
    uint64_t phase2_seal_worker_count = 0;
    uint64_t phase2_slo_guaranteed = 1;
    uint64_t phase2_slo_not_guaranteed_count = 0;
    std::string phase2_slo_reason;
    for (const Preserve_trx_drain_participant_observation &observation :
         drain_orchestrator.observations()) {
      phase2_lock_seal_us += observation.phase2_lock_seal_us;
      phase1_record_prebuilt_target_count +=
          observation.phase1_record_prebuilt_target_count;
      phase1_record_active_scan_target_count +=
          observation.phase1_record_active_scan_target_count;
      phase2_full_lock_scan_count += observation.phase2_full_lock_scan_count;
      materialized_lock_payload_bytes_in_phase2 +=
          observation.materialized_lock_payload_bytes_in_phase2;
      phase2_record_lock_count += observation.phase2_record_lock_count;
      phase2_table_lock_count += observation.phase2_table_lock_count;
      phase2_mdl_descriptor_count += observation.phase2_mdl_descriptor_count;
      phase2_table_live_export_target_count +=
          observation.phase2_table_live_export_target_count;
      phase2_mdl_live_export_target_count +=
          observation.phase2_mdl_live_export_target_count;
      phase2_record_prebuilt_target_count +=
          observation.phase2_record_prebuilt_target_count;
      phase2_record_materialized_target_count +=
          observation.phase2_record_materialized_target_count;
      phase2_seal_worker_count =
          std::max<uint64_t>(phase2_seal_worker_count,
                             observation.phase2_seal_worker_count);
      if (!observation.phase2_slo_guaranteed) {
        phase2_slo_guaranteed = 0;
        phase2_slo_not_guaranteed_count +=
            observation.phase2_slo_not_guaranteed_target_count != 0
                ? observation.phase2_slo_not_guaranteed_target_count
                : 1;
        if (phase2_slo_reason.empty()) {
          phase2_slo_reason = observation.phase2_slo_reason;
        }
      }
    }
    if (early_pipeline_enabled) {
      uint64_t early_slo_not_guaranteed_count = 0;
      bool early_record_materialized = false;
      bool early_table_live_export = false;
      bool early_mdl_live_export = false;
      for (const Preserve_batch_target_execution &execution : target_results) {
        phase2_record_lock_count += execution.final_record_lock_count;
        phase2_table_lock_count += execution.final_table_lock_count;
        phase2_mdl_descriptor_count +=
            execution.final_mdl_descriptor_count;
        materialized_lock_payload_bytes_in_phase2 +=
            execution.phase2_record_materialized_bytes;
        if (execution.final_record_prebuilt) {
          ++phase2_record_prebuilt_target_count;
        }
        if (execution.final_record_materialized) {
          ++phase2_record_materialized_target_count;
          early_record_materialized = true;
        }
        if (execution.final_table_lock_count != 0) {
          ++phase2_table_live_export_target_count;
          early_table_live_export = true;
        }
        if (execution.final_mdl_descriptor_count != 0) {
          ++phase2_mdl_live_export_target_count;
          early_mdl_live_export = true;
        }
        if (execution.final_record_materialized ||
            execution.final_table_lock_count != 0 ||
            execution.final_mdl_descriptor_count != 0) {
          ++early_slo_not_guaranteed_count;
        }
      }
      phase2_seal_worker_count =
          std::max<uint64_t>(phase2_seal_worker_count,
                             preserve_worker_count);
      if (early_slo_not_guaranteed_count != 0) {
        phase2_slo_guaranteed = 0;
        phase2_slo_not_guaranteed_count +=
            early_slo_not_guaranteed_count;
        if (phase2_slo_reason.empty()) {
          if (early_record_materialized) {
            phase2_slo_reason = "record_payload_materialized_in_phase2";
          } else if (early_table_live_export && early_mdl_live_export) {
            phase2_slo_reason = "table_mdl_live_export";
          } else if (early_table_live_export) {
            phase2_slo_reason = "table_live_export";
          } else {
            phase2_slo_reason = "mdl_live_export";
          }
        }
      }
    }
    if (phase2_metrics.savepoint_live_export_target_count != 0) {
      phase2_slo_guaranteed = 0;
      if (phase2_slo_reason.empty() ||
          phase2_slo_reason == "non_record_lock_family_live_export") {
        phase2_slo_reason = "savepoint_live_export";
      }
      if (phase2_slo_not_guaranteed_count <
          phase2_metrics.savepoint_live_export_target_count) {
        phase2_slo_not_guaranteed_count =
            phase2_metrics.savepoint_live_export_target_count;
      }
    }
    if (phase2_metrics.temp_manifest_build_target_count != 0) {
      phase2_slo_guaranteed = 0;
      if (phase2_slo_reason.empty() ||
          phase2_slo_reason == "non_record_lock_family_live_export" ||
          phase2_slo_reason == "savepoint_live_export" ||
          phase2_slo_reason == "record_payload_materialized_in_phase2") {
        phase2_slo_reason = "temp_table_manifest_phase2_build";
      }
      if (phase2_slo_not_guaranteed_count <
          phase2_metrics.temp_manifest_build_target_count) {
        phase2_slo_not_guaranteed_count =
            phase2_metrics.temp_manifest_build_target_count;
      }
    }
    std::string message =
        "PRESERVE: warm-copy drain metrics prefix_bytes=" +
        std::to_string(preserve_trx_warmcopy_prefix_bytes_status()) +
        " digest_bytes=" +
        std::to_string(preserve_trx_warmcopy_digest_bytes_status()) +
        " durable_bytes=" +
        std::to_string(preserve_trx_warmcopy_durable_bytes_status()) +
        " phase2_pause_us=" +
        std::to_string(phase2_pause_us) +
        " full_copy_to_count=" +
        std::to_string(
            preserve_trx_warmcopy_provider_full_copy_to_count_status()) +
        " lock_warmcopy_phase2_pause_us=" +
        std::to_string(preserve_trx_lock_warmcopy_phase2_pause_us_status()) +
        " phase2_total_us=" + std::to_string(phase2_metrics.total_us) +
        " phase2_transfer_tail_us=" +
        std::to_string(phase2_metrics.phase2_transfer_tail_us) +
        " phase2_end_monotonic_us=" +
        std::to_string(preserve_trx_monotonic_us()) +
        " phase2_target_wait_us=" +
        std::to_string(phase2_metrics.target_wait_us) +
        " phase2_participant_prepare_us=" +
        std::to_string(phase2_metrics.participant_prepare_us) +
        " phase2_participant_close_us=" +
        std::to_string(phase2_metrics.participant_close_us) +
        " phase2_participant_preflight_us=" +
        std::to_string(phase2_metrics.participant_preflight_us) +
        " phase2_lock_seal_us=" + std::to_string(phase2_lock_seal_us) +
        " phase2_target_preserve_us=" +
        std::to_string(phase2_metrics.target_preserve_us) +
        " phase2_target_pin_us=" +
        std::to_string(phase2_metrics.target_pin_us) +
        " phase2_target_worker_wall_us=" +
        std::to_string(phase2_metrics.target_worker_wall_us) +
        " early_staged_tokens=" +
        std::to_string(phase2_metrics.early_staged_tokens) +
        " command_boundary_to_enqueue_us_max=" +
        std::to_string(phase2_metrics.command_boundary_to_enqueue_us_max) +
        " final_fast_scan_us=" +
        std::to_string(phase2_metrics.final_fast_scan_us) +
        " final_dirty_tokens=" +
        std::to_string(phase2_metrics.final_dirty_tokens) +
        " final_replacement_tokens=" +
        std::to_string(phase2_metrics.final_replacement_tokens) +
        " final_validation_rejects=" +
        std::to_string(phase2_metrics.final_validation_rejects) +
        " phase2_target_result_collect_us=" +
        std::to_string(phase2_metrics.target_result_collect_us) +
        " phase2_target_deferred_dir_fsync_us=" +
        std::to_string(phase2_metrics.target_deferred_dir_fsync_us) +
        " phase2_transfer_commit_epoch_us=" +
        std::to_string(phase2_metrics.transfer_commit_epoch_us) +
        " phase2_binlog_preflight_us=" +
        std::to_string(phase2_metrics.binlog_preflight_us) +
        " phase2_lock_preflight_us=" +
        std::to_string(phase2_metrics.lock_preflight_us) +
        " phase2_lock_preflight_read_view_us=" +
        std::to_string(phase2_metrics.lock_preflight_read_view_us) +
        " phase2_lock_preflight_mdl_us=" +
        std::to_string(phase2_metrics.lock_preflight_mdl_us) +
        " phase2_lock_preflight_modified_tables_us=" +
        std::to_string(phase2_metrics.lock_preflight_modified_tables_us) +
        " phase2_lock_preflight_savepoints_us=" +
        std::to_string(phase2_metrics.lock_preflight_savepoints_us) +
        " phase2_lock_preflight_predicate_us=" +
        std::to_string(phase2_metrics.lock_preflight_predicate_us) +
        " phase2_lock_preflight_table_us=" +
        std::to_string(phase2_metrics.lock_preflight_table_us) +
        " phase2_preserve_worker_count=" +
        std::to_string(preserve_worker_count) +
        " phase2_prepare_us=" + std::to_string(phase2_metrics.prepare_us) +
        " phase2_detach_claim_us=" +
        std::to_string(phase2_metrics.detach_claim_us) +
        " phase2_snapshot_write_us=" +
        std::to_string(phase2_metrics.snapshot_write_us) +
        " phase2_snapshot_write_prebuilt_binlog_us=" +
        std::to_string(phase2_metrics.snapshot_write_prebuilt_binlog_us) +
        " phase2_snapshot_write_temp_manifest_us=" +
        std::to_string(phase2_metrics.snapshot_write_temp_manifest_us) +
        " phase2_temp_manifest_build_target_count=" +
        std::to_string(phase2_metrics.temp_manifest_build_target_count) +
        " phase2_snapshot_write_bundle_build_us=" +
        std::to_string(phase2_metrics.snapshot_write_bundle_build_us) +
        " phase2_snapshot_write_store_us=" +
        std::to_string(phase2_metrics.snapshot_write_store_us) +
        " phase2_snapshot_write_store_token_state_us=" +
        std::to_string(
            phase2_metrics.snapshot_write_store_token_state_us) +
        " phase2_snapshot_write_store_adopt_warm_blob_us=" +
        std::to_string(
            phase2_metrics.snapshot_write_store_adopt_warm_blob_us) +
        " phase2_snapshot_write_store_write_new_blobs_us=" +
        std::to_string(
            phase2_metrics.snapshot_write_store_write_new_blobs_us) +
        " phase2_snapshot_write_store_encode_us=" +
        std::to_string(phase2_metrics.snapshot_write_store_encode_us) +
        " phase2_snapshot_write_store_write_snapshot_us=" +
        std::to_string(
            phase2_metrics.snapshot_write_store_write_snapshot_us) +
        " phase2_register_us=" +
        std::to_string(phase2_metrics.register_us) +
        " phase2_target_count=" + std::to_string(phase2_metrics.target_count) +
        " phase1_record_prebuilt_target_count=" +
        std::to_string(phase1_record_prebuilt_target_count) +
        " phase1_record_active_scan_target_count=" +
        std::to_string(phase1_record_active_scan_target_count) +
        " phase2_full_lock_scan_count=" +
        std::to_string(phase2_full_lock_scan_count) +
        " materialized_lock_payload_bytes_in_phase2=" +
        std::to_string(materialized_lock_payload_bytes_in_phase2) +
        " phase2_record_lock_count=" +
        std::to_string(phase2_record_lock_count) +
        " phase2_table_lock_count=" +
        std::to_string(phase2_table_lock_count) +
        " phase2_mdl_descriptor_count=" +
        std::to_string(phase2_mdl_descriptor_count) +
        " phase2_table_live_export_target_count=" +
        std::to_string(phase2_table_live_export_target_count) +
        " phase2_mdl_live_export_target_count=" +
        std::to_string(phase2_mdl_live_export_target_count) +
        " phase2_savepoint_live_export_target_count=" +
        std::to_string(phase2_metrics.savepoint_live_export_target_count) +
        " phase2_record_prebuilt_target_count=" +
        std::to_string(phase2_record_prebuilt_target_count) +
        " phase2_record_materialized_target_count=" +
        std::to_string(phase2_record_materialized_target_count) +
        " phase2_seal_worker_count=" +
        std::to_string(phase2_seal_worker_count) +
        " phase2_slo_guaranteed=" +
        std::to_string(phase2_slo_guaranteed) +
        " phase2_slo_not_guaranteed_count=" +
        std::to_string(phase2_slo_not_guaranteed_count) +
        " source_phase2_transfer_bulk_bytes=" +
        std::to_string(preserve_trx_transfer_phase2_bulk_bytes_status()) +
        " source_phase2_transfer_snapshot_bundle_bytes=" +
        std::to_string(
            preserve_trx_transfer_phase2_snapshot_bundle_bytes_status()) +
        " source_phase2_transfer_snapshot_bundle_count=" +
        std::to_string(
            preserve_trx_transfer_phase2_snapshot_bundle_count_status()) +
        " source_phase2_transfer_final_metadata_frame_count=" +
        std::to_string(
            preserve_trx_transfer_phase2_final_metadata_frame_count_status()) +
        " source_phase2_transfer_final_metadata_bytes=" +
        std::to_string(
            preserve_trx_transfer_phase2_final_metadata_encoded_bytes_status()) +
        " source_phase2_transfer_final_metadata_ack_us=" +
        std::to_string(
            preserve_trx_transfer_phase2_final_metadata_ack_us_status()) +
        " source_phase1_transfer_frame_count=" +
        std::to_string(preserve_trx_transfer_phase1_frame_count_status()) +
        " source_phase1_transfer_network_send_count=" +
        std::to_string(
            preserve_trx_transfer_phase1_network_send_count_status()) +
        " source_phase1_transfer_batch_count=" +
        std::to_string(preserve_trx_transfer_phase1_batch_count_status()) +
        " source_phase1_transfer_batch_bytes_p50=" +
        std::to_string(
            preserve_trx_transfer_phase1_batch_bytes_p50_status()) +
        " source_phase1_transfer_batch_bytes_p95=" +
        std::to_string(
            preserve_trx_transfer_phase1_batch_bytes_p95_status()) +
        " source_phase1_transfer_batch_bytes_max=" +
        std::to_string(
            preserve_trx_transfer_phase1_batch_bytes_max_status()) +
        " source_phase1_transfer_batch_tokens_p50=" +
        std::to_string(
            preserve_trx_transfer_phase1_batch_tokens_p50_status()) +
        " source_phase1_transfer_batch_tokens_p95=" +
        std::to_string(
            preserve_trx_transfer_phase1_batch_tokens_p95_status()) +
        " source_phase1_transfer_batch_tokens_max=" +
        std::to_string(
            preserve_trx_transfer_phase1_batch_tokens_max_status()) +
        " source_phase1_record_batch_tokens_avg=" +
        std::to_string(
            preserve_trx_transfer_phase1_record_batch_tokens_avg_status()) +
        " source_phase1_transfer_batch_linger_us_p95=" +
        std::to_string(
            preserve_trx_transfer_phase1_batch_linger_us_p95_status()) +
        " source_phase1_transfer_batch_linger_us_max=" +
        std::to_string(
            preserve_trx_transfer_phase1_batch_linger_us_max_status()) +
        " source_phase1_transfer_oversize_token_count=" +
        std::to_string(
            preserve_trx_transfer_phase1_oversize_token_count_status()) +
        " source_phase1_record_first_batch_send_us=" +
        std::to_string(
            preserve_trx_transfer_phase1_record_first_batch_send_us_status()) +
        " source_phase1_record_last_batch_send_us=" +
        std::to_string(
            preserve_trx_transfer_phase1_record_last_batch_send_us_status());
    if (!phase2_slo_reason.empty()) {
      message += " phase2_slo_reason=" + phase2_slo_reason;
    }
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
  }
  DEBUG_SYNC(thd, "preserve_trx_warmcopy_after_batch_preserve");
  return finish_with_shutdown();
}

Preserve_trx_drain_request::Preserve_trx_drain_request(
    const Preserve_trx_options &options_arg)
    : options(options_arg) {}

bool Sql_cmd_drain_transactions_preserve::execute(THD *thd) {
  Preserve_trx_drain_request request{m_options};
  Preserve_trx_drain_service service;
  return service.execute(thd, request);
}

static Preserve_trx_options preserve_trx_options_from_lex(const LEX *lex) {
  Preserve_trx_options options;
  if (lex == nullptr) return options;
  /*
    The parser stores Preserve_trx_user_vars_mode as a small integer because LEX
    cannot include this runtime enum. Keep this mapping aligned with the grammar
    actions that assign INCLUDE=1 and EXCLUDE=2.
  */
  switch (lex->preserve_trx_user_vars_mode) {
    case 1:
      options.user_vars_mode = Preserve_trx_user_vars_mode::INCLUDE;
      break;
    case 2:
      options.user_vars_mode = Preserve_trx_user_vars_mode::EXCLUDE;
      break;
    default:
      options.user_vars_mode = Preserve_trx_user_vars_mode::DEFAULT;
      break;
  }
  return options;
}

static bool preserve_trx_execute_reset_drain(THD *thd) {
  if (!preserve_trx_is_ha_admin_account(thd)) {
    my_error(ER_SPECIFIC_ACCESS_DENIED_ERROR, MYF(0),
             "PRESERVE_TRX_HA_ADMIN account");
    return true;
  }
  if (check_global_access(thd, SHUTDOWN_ACL)) return true;
  switch (preserve_trx_request_active_drain_reset(true)) {
    case Preserve_trx_reset_drain_result::NO_ACTIVE:
    case Preserve_trx_reset_drain_result::RESET_WON:
    case Preserve_trx_reset_drain_result::RESET_JOINED:
      my_ok(thd);
      return false;
    case Preserve_trx_reset_drain_result::TOO_LATE:
      my_error(ER_PRESERVE_TRX_RESET_TOO_LATE, MYF(0));
      return true;
    case Preserve_trx_reset_drain_result::UNSUPPORTED:
      return preserve_trx_reject_unsupported();
  }
  return preserve_trx_reject_unsupported();
}

bool preserve_trx_execute_command(THD *thd) {
  if (thd == nullptr || thd->lex == nullptr) {
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  }

  Preserve_trx_options options = preserve_trx_options_from_lex(thd->lex);
  switch (thd->lex->sql_command) {
    case SQLCOM_DRAIN_TRANSACTIONS_PRESERVE: {
      Preserve_trx_drain_request request{options};
      Preserve_trx_drain_service service;
      return service.execute(thd, request);
    }
    case SQLCOM_RESET_DRAIN:
      return preserve_trx_execute_reset_drain(thd);
    default:
      my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
      return true;
  }
}

enum class Preserved_trx_promotion_resume_status : uint8_t {
  OK = 0,
  INVALID_ARGUMENT,
  FEATURE_DISABLED,
  DEADLINE_EXPIRED,
  REGISTRY_NOT_ADOPTED,
  TARGET_NOT_PRISTINE,
  ATTACH_INTENT_IO_ERROR,
  PROMOTION_RECORD_NOT_FOUND,
  STAGING_FAILED,
  ACTIVATION_FAILED_ROLLED_BACK,
  ATTACH_TAINTED
};

enum class Preserved_trx_resume_source {
  LOCAL_DURABLE,
  STRICT_PROMOTION
};

enum class Preserved_trx_resume_prepare_stage {
  NONE,
  SESSION_STATE,
  BINLOG_CACHE,
  MDL,
  GTID_OWNERSHIP,
  GTID_ROLLBACK_UNDO,
  TEMP_TABLE,
  TRX_ATTACH,
  SAVEPOINTS,
  TEMP_TABLE_RESEED,
  DEADLINE
};

struct Preserved_trx_resume_prepare_options {
  Preserved_trx_resume_source source{
      Preserved_trx_resume_source::LOCAL_DURABLE};
  const std::string *dir{nullptr};
  const std::string *token{nullptr};
  Preserve_trx_attach_lease *strict_attach_lease{nullptr};
  const Preserve_trx_final_token_facts *strict_facts{nullptr};
  uint64_t deadline_monotonic_us{0};
};

struct Preserved_trx_resume_runtime {
  bool local_binlog_imported{false};
  bool strict_binlog_attached{false};
  bool mdl_transferred{false};
  bool gtid_restored{false};
  bool temp_tables_materialized{false};
  bool trx_attached{false};
  Preserve_memory_lease local_binlog_payload_lease;
  std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle>
      strict_binlog_handle;
  Mysql_binlog_preserve_attach_journal strict_binlog_journal;
};

struct Preserved_trx_resume_prepare_result {
  bool ok{false};
  bool temp_cleanup_incomplete{false};
  Preserved_trx_resume_prepare_stage stage{
      Preserved_trx_resume_prepare_stage::NONE};
  std::string reason;
};

static Preserved_trx_resume_prepare_result
prepare_resume_on_current_thd_shared(
    THD *thd, Preserved_trx_record *record,
    const Preserved_trx_resume_prepare_options &options,
    Preserved_trx_resume_runtime *runtime) {
  Preserved_trx_resume_prepare_result result;
  if (thd == nullptr || record == nullptr || runtime == nullptr ||
      options.dir == nullptr || options.token == nullptr ||
      options.dir->empty() || options.token->empty()) {
    result.reason = "invalid resume preparation inputs";
    return result;
  }
  const bool strict =
      options.source == Preserved_trx_resume_source::STRICT_PROMOTION;
  auto fail = [&](Preserved_trx_resume_prepare_stage stage,
                  const std::string &reason) {
    result.stage = stage;
    result.reason = reason;
    return result;
  };

  if (strict &&
      (options.strict_attach_lease == nullptr ||
       !options.strict_attach_lease->active() ||
       options.strict_facts == nullptr ||
       !record->metadata.temp_table_manifest_payload.empty())) {
    return fail(Preserved_trx_resume_prepare_stage::SESSION_STATE,
                "invalid strict promotion resume inputs");
  }

  bool debug_isolation_failure = false;
  if (!strict) {
    DBUG_EXECUTE_IF("preserve_trx_fail_resume_set_isolation",
                    debug_isolation_failure = true;);
  }
  if (debug_isolation_failure ||
      record->metadata.tx_isolation > ISO_SERIALIZABLE ||
      set_tx_isolation(
          thd,
          static_cast<enum_tx_isolation>(record->metadata.tx_isolation),
          true)) {
    return fail(Preserved_trx_resume_prepare_stage::SESSION_STATE,
                debug_isolation_failure
                    ? "debug injected isolation restore failure"
                    : "isolation restore failure");
  }
  if (restore_preserved_session_variables(thd, record->metadata)) {
    return fail(Preserved_trx_resume_prepare_stage::SESSION_STATE,
                "session state restore failure");
  }
  if (restore_preserved_dml_policy(thd, record->trx, record->metadata)) {
    return fail(Preserved_trx_resume_prepare_stage::SESSION_STATE,
                "DML policy restore failure");
  }

  thd->variables.sql_log_bin = record->metadata.session_sql_log_bin;
  if (record->metadata.option_bin_log)
    thd->variables.option_bits |= OPTION_BIN_LOG;
  else
    thd->variables.option_bits &= ~OPTION_BIN_LOG;
  restore_preserved_transaction_access_mode(thd, record->metadata);
  restore_last_insert_id_state(thd, record->metadata);
  restore_forced_insert_id_state(thd, record->metadata);
  if (import_user_vars_payload(thd, record->metadata.user_vars_payload)) {
    return fail(Preserved_trx_resume_prepare_stage::SESSION_STATE,
                "user variables restore failure");
  }

  const bool metadata_has_binlog_cache =
      record->metadata.binlog_state ==
      Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE;
  if (strict) {
    if (metadata_has_binlog_cache != options.strict_facts->binlog_cache_present) {
      return fail(Preserved_trx_resume_prepare_stage::BINLOG_CACHE,
                  "native binlog facts do not match promoted metadata");
    }
    if (metadata_has_binlog_cache) {
      Preserve_trx_internal_operation_capability capability;
      if (options.strict_attach_lease->make_native_binlog_attach_capability(
              &capability) != Preserve_trx_prepared_status::OK ||
          options.strict_attach_lease->take_native_binlog_handle(
              &runtime->strict_binlog_handle) !=
              Preserve_trx_prepared_status::OK ||
          mysql_binlog_preserve_attach_detached_cache(
              capability, thd, &runtime->strict_binlog_handle,
              &runtime->strict_binlog_journal) !=
              Mysql_binlog_preserve_cache_status::OK) {
        return fail(Preserved_trx_resume_prepare_stage::BINLOG_CACHE,
                    "native binlog cache attach failed");
      }
      runtime->strict_binlog_attached = true;
    }
  } else if (metadata_has_binlog_cache) {
    DBUG_EXECUTE_IF("preserve_trx_resume_clear_binlog_cache_payload",
                    record->metadata.binlog_cache_payload.clear(););
    if (hydrate_logged_binlog_cache_payload_if_needed(
            record, *options.token, nullptr,
            &runtime->local_binlog_payload_lease)) {
      return fail(Preserved_trx_resume_prepare_stage::BINLOG_CACHE,
                  "binlog cache read failure");
    }
    Mysql_binlog_preserve_snapshot binlog_snapshot =
        metadata_to_binlog_cache_snapshot(record->metadata);
    if (mysql_binlog_preserve_import(thd, binlog_snapshot)) {
      return fail(Preserved_trx_resume_prepare_stage::BINLOG_CACHE,
                  "binlog cache import failure");
    }
    runtime->local_binlog_imported = true;
  }

  if (restore_detached_mdl_context(thd, *options.token)) {
    return fail(Preserved_trx_resume_prepare_stage::MDL,
                "MDL transfer failure");
  }
  runtime->mdl_transferred = true;

  bool debug_before_attach_failure = false;
  if (!strict) {
    DBUG_EXECUTE_IF("preserve_trx_fail_resume_before_attach",
                    debug_before_attach_failure = true;);
  }
  if (debug_before_attach_failure) {
    return fail(Preserved_trx_resume_prepare_stage::MDL,
                "debug injected failure");
  }

  if (preserve_snapshot_allows_gtid_restore(record->metadata)) {
    if (restore_logged_cache_gtid_next(thd, record->metadata)) {
      return fail(Preserved_trx_resume_prepare_stage::GTID_OWNERSHIP,
                  "binlog GTID ownership restore failure");
    }
    runtime->gtid_restored = true;
    if (trx_preserve_prepare_resumed_rollback_gtid(record->trx) != DB_SUCCESS) {
      return fail(Preserved_trx_resume_prepare_stage::GTID_ROLLBACK_UNDO,
                  "binlog GTID rollback undo preparation failure");
    }
  }

  std::string temp_reason;
  Preserve_snapshot_status temp_status;
  if (strict) {
    temp_status = preserve_trx_temp_table_materialize_for_resume(
        thd, record->trx, *options.dir, *options.token, record->metadata,
        &temp_reason);
  } else {
    Preserve_trx_temp_table_cleanup_result temp_cleanup;
    temp_status = preserve_trx_temp_table_materialize_for_resume(
        thd, record->trx, *options.dir, *options.token, record->metadata,
        &temp_reason, &temp_cleanup);
    result.temp_cleanup_incomplete = !temp_cleanup.complete();
  }
  if (temp_status != Preserve_snapshot_status::OK) {
    return fail(Preserved_trx_resume_prepare_stage::TEMP_TABLE,
                temp_reason.empty() ? "temporary table materialization failure"
                                    : "temporary table materialization failure: " +
                                          temp_reason);
  }
  runtime->temp_tables_materialized =
      !record->metadata.temp_table_manifest_payload.empty();

  if (trx_preserve_attach_to_thd(record->trx, thd) != DB_SUCCESS) {
    return fail(Preserved_trx_resume_prepare_stage::TRX_ATTACH,
                "attach failure");
  }
  runtime->trx_attached = true;
  mark_preserved_transaction_attached(thd, record->metadata);
  if (restore_savepoints_to_thd(thd, record->trx, record->metadata)) {
    return fail(Preserved_trx_resume_prepare_stage::SAVEPOINTS,
                "savepoint restore failure");
  }
  if (!strict && runtime->temp_tables_materialized &&
      !preserve_trx_temp_table_reseed_after_resume(thd)) {
    return fail(Preserved_trx_resume_prepare_stage::TEMP_TABLE_RESEED,
                "temporary table baseline reseed failure");
  }
  if (options.deadline_monotonic_us != 0 &&
      preserve_trx_monotonic_us() >= options.deadline_monotonic_us) {
    return fail(Preserved_trx_resume_prepare_stage::DEADLINE,
                "deadline expired before activation");
  }

  result.ok = true;
  return result;
}

static dberr_t activate_resumed_trx_shared(Preserved_trx_record *record) {
  return record == nullptr ? DB_ERROR
                           : trx_preserve_activate_resumed(record->trx);
}

static Preserved_trx_promotion_resume_status
preserved_trx_resume_adopted_for_promotion_on_current_thd(
    THD *target, const Preserve_trx_prepared_token_key &key,
    uint64_t requested_deadline_us);

static bool preserved_trx_resume_record_on_current_thd(
    THD *thd, const LEX_CSTRING &resume_token) {
  DBUG_TRACE;
  struct Resume_total_us_guard {
    uint64_t started_us{preserve_trx_monotonic_us()};
    ~Resume_total_us_guard() {
      g_resume_total_us.store(
          static_cast<ulonglong>(preserve_trx_monotonic_us() - started_us));
    }
  } resume_total_us_guard;

  /*
    Resume is intentionally staged so a failure cannot consume a token without a
    recoverable record:
      1. reject unsupported server/session modes and check token ownership;
      2. reread durable metadata before claiming the in-memory record;
      3. restore SQL session state under Resume_thd_state_guard;
      4. import binlog, MDL, GTID and temp-table state that lives outside the
         claimed InnoDB trx;
      5. attach the preserved trx, rebuild SQL savepoints, then remove the
         snapshot.
    Failures before attach restore the record through
    restore_preserved_record_after_failure(); failures after attach either detach
    again or mark the record so reaper/operator cleanup remains possible.
  */

  if (!preserve_trx_is_enabled()) {
    my_error(ER_PRESERVE_TRX_DISABLED, MYF(0));
    return true;
  }

  if (opt_readonly || opt_super_readonly) return preserve_trx_reject_unsupported();

  if (srv_force_recovery > 0) return preserve_trx_reject_unsupported();

  if (preserve_trx_is_unsupported_common_context(thd)) {
    return preserve_trx_reject_unsupported();
  }

  DEBUG_SYNC(thd, "preserve_trx_resume_start");

  const std::string token(resume_token.str, resume_token.length);
  const bool has_resume_any_privilege =
      preserve_trx_has_resume_any_privilege(thd);
  preserved_trx_wait_recovery_complete();
  Preserved_trx_record record;
  if (!preserved_trx_find_record(token, &record)) {
    if (!has_resume_any_privilege) {
      my_error(ER_PRESERVE_TRX_ACCESS_DENIED, MYF(0));
      return true;
    }

    auto store = create_preserved_trx_default_store(preserve_trx_default_dir());
    Preserved_trx_bundle bundle;
    const Preserve_snapshot_status status =
        token_is_filename_safe(token)
            ? store->read(token, true,
                          Preserved_trx_carrier::Payload_read_mode::
                              METADATA_ONLY,
                          &bundle)
            : Preserve_snapshot_status::NOT_FOUND;
    if (status == Preserve_snapshot_status::CORRUPT) {
      (void)delete_preserved_snapshot_files_and_sidecars_or_log(
          preserve_trx_default_dir(), token, nullptr,
          Temp_sidecar_cleanup_mode::RAW_UNLINK);
      my_error(ER_PRESERVE_TRX_CORRUPT_SNAPSHOT, MYF(0));
    } else if (status == Preserve_snapshot_status::NOT_FOUND) {
      log_redacted_resume_failure(token, "token not found");
      my_error(ER_PRESERVE_TRX_NOT_FOUND, MYF(0));
    } else {
      my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    }
    return true;
  }
  Security_context *sctx = thd->security_context();
  const bool owns_token =
      record.metadata.owner_user == lex_cstring_to_string(sctx->priv_user()) &&
      record.metadata.owner_host == lex_cstring_to_string(sctx->priv_host());
  if (!preserved_trx_resume_allowed_for_account(owns_token,
                                                has_resume_any_privilege)) {
    my_error(ER_PRESERVE_TRX_ACCESS_DENIED, MYF(0));
    return true;
  }
  if (!owns_token && !record.metadata.session_sql_log_bin &&
      !has_session_variable_admin_privilege(thd)) {
    my_error(ER_SPECIFIC_ACCESS_DENIED_ERROR, MYF(0),
             "SUPER, SYSTEM_VARIABLES_ADMIN or SESSION_VARIABLES_ADMIN");
    return true;
  }

  if (!record.resumable &&
      record.state == Preserved_trx_lifecycle_state::ADOPTED_FOR_PROMOTION) {
    if (!record.has_promotion_key ||
        record.promotion_key.token != token) {
      (void)preserved_trx_update_record_last_error(
          token, "promotion-owned token has no strict registry identity");
      return preserve_trx_reject_unsupported();
    }
    if (preserve_trx_recheck_resume_object_privileges(
            thd, record.metadata,
            !owns_token /* require_all_modified_write_acls */)) {
      (void)preserved_trx_update_record_last_error(
          token, "resume user lacks object privileges");
      return true;
    }
    const auto status =
        preserved_trx_resume_adopted_for_promotion_on_current_thd(
            thd, record.promotion_key, std::numeric_limits<uint64_t>::max());
    if (status == Preserved_trx_promotion_resume_status::OK) {
      my_ok(thd);
      return false;
    }
    if (status ==
            Preserved_trx_promotion_resume_status::PROMOTION_RECORD_NOT_FOUND ||
        status == Preserved_trx_promotion_resume_status::REGISTRY_NOT_ADOPTED) {
      my_error(ER_PRESERVE_TRX_NOT_FOUND, MYF(0));
      return true;
    }
    return preserve_trx_reject_unsupported();
  }

  if (preserved_trx_record_resume_deadline_expired(record)) {
    if (!preserved_trx_take_record(token, &record)) {
      my_error(ER_PRESERVE_TRX_NOT_FOUND, MYF(0));
      return true;
    }
    preserve_trx_set_record_error(&record, "recovery timeout expired");
    (void)rollback_resumable_record_after_resume_timeout(record);
    return preserve_trx_reject_unsupported();
  }

  if (!record.resumable) {
    (void)preserved_trx_update_record_last_error(
        record.metadata.token,
        "token is not resumable at current recovery mode");
    log_redacted_resume_failure(record.metadata.token,
                                "token is pending and cannot be resumed");
    return preserve_trx_reject_unsupported();
  }

  const Preserve_trx_temp_table_preclaim_decision temp_table_preclaim =
      preserve_trx_temp_table_preclaim_decision(record.metadata);
  if (temp_table_preclaim.retryable_unsupported) {
    (void)preserved_trx_update_record_last_error(
        record.metadata.token,
        "temporary table resume is unsupported until enabled and fully "
        "materialized");
    return preserve_trx_reject_unsupported();
  }
  std::string temp_namespace_reason;
  const Preserve_snapshot_status temp_namespace_status =
      preserve_trx_temp_table_check_target_namespace(
          thd, record.metadata, &temp_namespace_reason);
  if (temp_namespace_status != Preserve_snapshot_status::OK) {
    (void)preserved_trx_update_record_last_error(
        record.metadata.token,
        temp_namespace_reason.empty()
            ? "target temporary table namespace conflicts with preserved token"
            : temp_namespace_reason);
    return preserve_trx_reject_unsupported();
  }
  std::string temp_bootstrap_reason;
  const Preserve_snapshot_status temp_bootstrap_status =
      reserve_temp_sidecars_for_resume_retry(preserve_trx_default_dir(), token,
                                             record.metadata,
                                             &temp_bootstrap_reason);
  if (temp_bootstrap_status != Preserve_snapshot_status::OK) {
    (void)preserved_trx_update_record_last_error(
        record.metadata.token,
        temp_bootstrap_reason.empty()
            ? "temporary table resume bootstrap failed"
            : "temporary table resume bootstrap failed: " +
                  temp_bootstrap_reason);
    return preserve_trx_reject_unsupported();
  }

  if (thd->in_active_multi_stmt_transaction()) {
    (void)preserved_trx_update_record_last_error(
        record.metadata.token,
        "cannot resume preserved transaction while in multi-statement transaction");
    my_error(ER_PRESERVE_TRX_INVALID_STATE, MYF(0));
    return true;
  }

  DBUG_EXECUTE_IF("preserve_trx_resume_force_binlog_mode_mismatch", {
    const bool required_log_bin = !mysql_bin_log.is_open();
    record.metadata.global_log_bin = required_log_bin;
    record.metadata.option_bin_log = required_log_bin;
    record.metadata.session_sql_log_bin = required_log_bin;
    record.metadata.binlog_state =
        required_log_bin ? Preserve_snapshot_binlog_state::LOGGED_EMPTY
                         : Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE;
  });

  if (!recoverable_binlog_state(record.metadata.binlog_state)) {
    (void)preserved_trx_update_record_error(
        record.metadata.token, "token has unsupported binlog state");
    return preserve_trx_reject_unsupported();
  }

  if (!binlog_state_matches_current_mode(record.metadata)) {
    log_preserved_trx_rejected_binlog_mode(record.metadata.token,
                                           record.metadata);
    (void)preserved_trx_update_record_last_error(record.metadata.token,
                                                 "binlog mode mismatch");
    my_error(ER_PRESERVE_TRX_BINLOG_MODE_MISMATCH, MYF(0));
    return true;
  }

  if (!preserved_trx_resume_binlog_format_is_supported(thd,
                                                        record.metadata)) {
    (void)preserved_trx_update_record_last_error(
        record.metadata.token, "binlog format is not ROW");
    return preserve_trx_reject_unsupported();
  }

  if (!trx_preserve_thd_can_accept_preserved_trx(thd)) {
    (void)preserved_trx_update_record_last_error(
        record.metadata.token,
        "session cannot accept preserved transaction");
    return preserve_trx_reject_unsupported();
  }

  if (preserve_trx_recheck_resume_object_privileges(
          thd, record.metadata,
          !owns_token /* require_all_modified_write_acls */)) {
    (void)preserved_trx_update_record_last_error(
        record.metadata.token, "resume user lacks object privileges");
    return true;
  }

  {
    auto store = create_preserved_trx_default_store(preserve_trx_default_dir());
    Preserved_trx_bundle durable_bundle;
    const Preserve_snapshot_status durable_status =
        store->read(token, true,
                    Preserved_trx_carrier::Payload_read_mode::METADATA_ONLY,
                    &durable_bundle);
    if (durable_status == Preserve_snapshot_status::CORRUPT) {
      if (!preserved_trx_take_resumable_record(token, &record)) {
        my_error(ER_PRESERVE_TRX_NOT_FOUND, MYF(0));
        return true;
      }
      (void)rollback_resumable_record_after_corrupt_snapshot(record);
      my_error(ER_PRESERVE_TRX_CORRUPT_SNAPSHOT, MYF(0));
      return true;
    }
    if (durable_status != Preserve_snapshot_status::OK) {
      (void)preserved_trx_update_record_last_error(
          record.metadata.token, "durable transaction snapshot read failure");
      return preserve_trx_reject_unsupported();
    }
  }

  if (!preserved_trx_take_resumable_record(token, &record)) {
    my_error(ER_PRESERVE_TRX_NOT_FOUND, MYF(0));
    return true;
  }
  if (preserved_trx_record_resume_deadline_expired(record)) {
    preserve_trx_set_record_error(&record, "recovery timeout expired");
    (void)rollback_resumable_record_after_resume_timeout(record);
    return preserve_trx_reject_unsupported();
  }
  Preserved_trx_observable_state_guard resuming_state(
      record.metadata, record.trx, Preserved_trx_lifecycle_state::RESUMING);
  DEBUG_SYNC(thd, "preserve_trx_resume_after_record_take");

  Resume_thd_state_guard thd_state_guard(thd);
  Preserved_trx_resume_runtime resume_runtime;
  bool &binlog_imported = resume_runtime.local_binlog_imported;
  bool &mdl_transferred = resume_runtime.mdl_transferred;
  bool &gtid_restored = resume_runtime.gtid_restored;
  bool &temp_tables_materialized = resume_runtime.temp_tables_materialized;
  bool thd_detached_after_attach = false;
  auto taint_record_after_temp_cleanup_failure =
      [&](const std::string &reason) {
        const std::string cleanup_reason =
            reason + "; temporary table cleanup incomplete";
        auto store =
            create_preserved_trx_default_store(preserve_trx_default_dir());
        if (store->mark_consume_state(
                token, Preserve_snapshot_consume_state::CLEANUP_TAINTED,
                cleanup_reason) != Preserve_snapshot_status::OK) {
          log_preserved_trx_cleanup_failure(
              token, "failed to publish temporary table cleanup taint");
        }
        record.resumable = false;
        record.state = Preserved_trx_lifecycle_state::FAILED;
        record.observable_only = false;
        return preserved_trx_add_record_with_error(record, cleanup_reason);
      };
  auto restore_preserved_record_after_failure =
      [&](const std::string &reason,
          bool temp_cleanup_incomplete = false) {
        if (temp_tables_materialized) {
          Preserve_trx_temp_table_cleanup_result cleanup_result;
          const Preserve_snapshot_status rollback_status =
              preserve_trx_temp_table_rollback_materialized_for_resume(
                  thd, record.metadata, &cleanup_result);
          temp_cleanup_incomplete =
              temp_cleanup_incomplete ||
              rollback_status != Preserve_snapshot_status::OK ||
              !cleanup_result.complete();
          temp_tables_materialized = false;
        }
        const bool must_reset_thd_transaction_state =
            gtid_restored || thd_detached_after_attach;
        rollback_restored_logged_cache_gtid_next(thd, &gtid_restored);
        if (binlog_imported) {
          discard_binlog_preserve_cache_and_reset_scopes(thd);
          binlog_imported = false;
        }
        if (must_reset_thd_transaction_state) {
          reset_thd_after_preserve_detach(thd);
          thd_detached_after_attach = false;
        } else if (mdl_transferred) {
          thd->mdl_context.release_transactional_locks();
        }
        if (temp_cleanup_incomplete) {
          return taint_record_after_temp_cleanup_failure(reason);
        }
        return restore_record_after_resume_failure(record, reason);
      };

  auto detach_resumed_after_failure = [&](const char *reason) {
    if (trx_preserve_detach_resumed_from_thd(record.trx, thd) == DB_SUCCESS) {
      resume_runtime.trx_attached = false;
      thd_detached_after_attach = true;
      return false;
    }

    const std::string retry_message =
        "Preserved transaction resume failed to detach transaction after " +
        std::string(reason) + "; retrying cleanup detach";
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, retry_message.c_str());
    if (trx_preserve_detach_resumed_from_thd_for_cleanup(record.trx, thd) ==
        DB_SUCCESS) {
      resume_runtime.trx_attached = false;
      thd_detached_after_attach = true;
      return false;
    }

    const std::string kill_message =
        "Preserved transaction resume failed to detach transaction after " +
        std::string(reason) + "; killing session";
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, kill_message.c_str());
    preserved_trx_add_resume_detach_failure_observable_record(record, reason);
    thd->killed = THD::KILL_CONNECTION;
    thd_state_guard.dismiss();
    return true;
  };
  const std::string local_dir = preserve_trx_default_dir();
  Preserved_trx_resume_prepare_options prepare_options;
  prepare_options.source = Preserved_trx_resume_source::LOCAL_DURABLE;
  prepare_options.dir = &local_dir;
  prepare_options.token = &token;
  const Preserved_trx_resume_prepare_result prepare_result =
      prepare_resume_on_current_thd_shared(thd, &record, prepare_options,
                                           &resume_runtime);
  if (!prepare_result.ok) {
    if (resume_runtime.trx_attached &&
        detach_resumed_after_failure(prepare_result.reason.c_str())) {
      return preserve_trx_reject_unsupported();
    }
    if (prepare_result.stage ==
        Preserved_trx_resume_prepare_stage::GTID_OWNERSHIP) {
      if (binlog_imported) {
        discard_binlog_preserve_cache_and_reset_scopes(thd);
        binlog_imported = false;
      }
      reset_thd_after_preserve_detach(thd);
      if (trx_preserve_rollback_claimed(record.trx) != DB_SUCCESS) {
        (void)restore_preserved_record_after_failure(
            "binlog GTID ownership restore failure cleanup failure");
        return preserve_trx_reject_unsupported();
      }
      delete_detached_mdl_context(token);
      (void)delete_preserved_snapshot_files_and_sidecars_or_log(
          local_dir, token, &record.metadata);
      return preserve_trx_reject_unsupported();
    }
    (void)restore_preserved_record_after_failure(
        prepare_result.reason, prepare_result.temp_cleanup_incomplete);
    return preserve_trx_reject_unsupported();
  }

  auto consume_store =
      create_preserved_trx_default_store(preserve_trx_default_dir());
  if (consume_store->mark_consume_state(
          token, Preserve_snapshot_consume_state::CONSUME_PENDING,
          "resume activation pending") != Preserve_snapshot_status::OK) {
    if (!detach_resumed_after_failure("consume-state publish failure")) {
      (void)restore_preserved_record_after_failure(
          "consume-state publish failure");
    }
    return preserve_trx_reject_unsupported();
  }

  if (activate_resumed_trx_shared(&record) != DB_SUCCESS) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           "Preserved transaction resume failed to validate ACTIVE Undo state");
    if (!detach_resumed_after_failure("undo activation failure")) {
      if (consume_store->remove_consume_state(token) ==
          Preserve_snapshot_status::OK) {
        (void)restore_preserved_record_after_failure("undo activation failure");
      } else {
        bool temp_cleanup_incomplete = false;
        if (temp_tables_materialized) {
          Preserve_trx_temp_table_cleanup_result cleanup_result;
          const Preserve_snapshot_status cleanup_status =
              preserve_trx_temp_table_rollback_materialized_for_resume(
                  thd, record.metadata, &cleanup_result);
          temp_cleanup_incomplete =
              cleanup_status != Preserve_snapshot_status::OK ||
              !cleanup_result.complete();
          temp_tables_materialized = false;
        }
        rollback_restored_logged_cache_gtid_next(thd, &gtid_restored);
        if (binlog_imported) {
          discard_binlog_preserve_cache_and_reset_scopes(thd);
          binlog_imported = false;
        }
        reset_thd_after_preserve_detach(thd);
        thd_detached_after_attach = false;
        const dberr_t rollback_status =
            trx_preserve_rollback_claimed(record.trx);
        delete_detached_mdl_context(token);
        (void)consume_store->mark_consume_state(
            token, Preserve_snapshot_consume_state::CLEANUP_TAINTED,
            rollback_status == DB_SUCCESS && !temp_cleanup_incomplete
                ? "activation failed and consume marker cleanup failed"
                : "activation, rollback, or temp cleanup failed");
        if (rollback_status != DB_SUCCESS || temp_cleanup_incomplete) {
          preserved_trx_add_failed_observable_record(
              record.metadata,
              temp_cleanup_incomplete
                  ? "activation failed with temporary table cleanup failure"
                  : "activation failed with consume marker cleanup failure");
        }
      }
    }
    return preserve_trx_reject_unsupported();
  }

  if (consume_store->mark_consume_state(
          token, Preserve_snapshot_consume_state::ACTIVE_CONSUMED,
          "resume activation completed") != Preserve_snapshot_status::OK) {
    const std::string marker_message =
        redacted_preserved_trx_log_subject(token) +
        " failed to publish active-consumed state; pending state retained";
    LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG, marker_message.c_str());
  }

  if (trx_preserve_finish_resumed_activation(record.trx, thd) != DB_SUCCESS) {
    const std::string message =
        redacted_preserved_trx_log_subject(token) +
        " failed to clear consumed ACTIVE Undo runtime identity; killing session";
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    thd->killed = THD::KILL_CONNECTION;
    thd_state_guard.dismiss();
    return preserve_trx_reject_unsupported();
  }

  delete_detached_mdl_context(token);

  DBUG_EXECUTE_IF(
      "preserve_trx_crash_after_resume_activate_before_snapshot_delete",
      DBUG_SUICIDE(););

  Preserve_snapshot_remove_options remove_options;
  if (temp_tables_materialized) {
    remove_options.preserve_committed_temp_sidecar_source_space_ids =
        preserve_trx_temp_table_sidecar_source_space_ids(record.metadata);
  }
  Preserve_snapshot_delete_status delete_status =
      delete_snapshot_files_with_status(preserve_trx_default_dir(), token,
                                        remove_options);
  if (delete_status != Preserve_snapshot_delete_status::OK) {
    if (delete_status ==
        Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE) {
      /*
        The transaction is already attached to this THD, so the in-memory
        preserved record has intentionally been consumed. If the snapshot file
        may still exist, mark it tainted before returning success so a later
        startup treats the file as cleanup-only evidence rather than a normal
        resumable token.
      */
      auto store =
          create_preserved_trx_default_store(preserve_trx_default_dir());
      if (store->mark_tainted(token, "resume_cleanup_failure") !=
          Preserve_snapshot_status::OK) {
        const std::string taint_message =
            redacted_preserved_trx_log_subject(token) +
            " failed to mark snapshot tainted after resume cleanup failure";
        LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG, taint_message.c_str());
      }
    }
    const std::string message =
        redacted_preserved_trx_log_subject(token) +
        " snapshot cleanup failed after resume; transaction remains attached";
    LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
  }
  audit_preserved_trx_event(thd, token, "resume", "success");
  preserved_trx_clear_cleanup_failed_if_no_records();
  thd_state_guard.dismiss();
  my_ok(thd);
  return false;
}

bool Sql_cmd_resume_preserved_transaction::execute(THD *thd) {
  DBUG_TRACE;
  return preserved_trx_resume_record_on_current_thd(thd, m_token);
}

namespace {

struct Preserve_strict_attach_intent_write_context {
  Preserve_trx_strict_attach_intent_state state{
      Preserve_trx_strict_attach_intent_state::ATTACHING};
  uint64_t target_connection_id{0};
};

bool write_strict_attach_intent(
    const Preserve_trx_prepared_token_key &key, void *context) {
  auto *write_context =
      static_cast<Preserve_strict_attach_intent_write_context *>(context);
  if (write_context == nullptr || write_context->target_connection_id == 0) {
    return false;
  }
  DBUG_EXECUTE_IF("preserve_trx_fail_write_strict_attach_intent",
                  return false;);
  Preserve_trx_strict_attach_intent intent;
  intent.key = key;
  intent.state = write_context->state;
  intent.target_connection_id = write_context->target_connection_id;
  intent.generated_at_us = my_micro_time();
  std::string encoded;
  const std::string journal_id =
      preserved_trx_strict_attach_intent_journal_id(key);
  if (journal_id.empty() ||
      !preserved_trx_encode_strict_attach_intent_v1(intent, &encoded)) {
    return false;
  }
  auto store = create_preserved_trx_process_local_store(key.preserve_dir);
  return store->write_promotion_intent_epoch(journal_id, encoded) ==
         Preserve_snapshot_status::OK;
}

void remove_strict_attach_intent(
    const Preserve_trx_prepared_token_key &key) {
  const std::string journal_id =
      preserved_trx_strict_attach_intent_journal_id(key);
  if (journal_id.empty()) return;
  auto store = create_preserved_trx_process_local_store(key.preserve_dir);
  (void)store->remove_promotion_intent_epoch(journal_id);
}

}  // namespace

static Preserved_trx_promotion_resume_status
preserved_trx_resume_adopted_for_promotion_on_current_thd(
    THD *target,
    const Preserve_trx_prepared_token_key &key,
    uint64_t requested_deadline_us) {
  const uint64_t started_us = preserve_trx_monotonic_us();
  auto finish = [&](Preserved_trx_promotion_resume_status status) {
    const uint64_t elapsed_us = preserve_trx_monotonic_us() - started_us;
    preserved_trx_promotion_resume_core_note(
        elapsed_us, status == Preserved_trx_promotion_resume_status::OK);
    return status;
  };
  if (target == nullptr || key.preserve_dir.empty() ||
      key.epoch_scope.empty() || key.epoch_id.empty() || key.token.empty() ||
      key.target_boot_incarnation.empty() || key.generation == 0) {
    return finish(Preserved_trx_promotion_resume_status::INVALID_ARGUMENT);
  }
  if (!preserve_trx_is_enabled()) {
    return finish(Preserved_trx_promotion_resume_status::FEATURE_DISABLED);
  }
  Preserve_trx_prepared_token_snapshot snapshot;
  auto &registry = preserved_trx_strict_prepared_token_registry();
  if (registry.snapshot(key, &snapshot) != Preserve_trx_prepared_status::OK ||
      snapshot.state != Preserve_trx_prepared_token_state::ADOPTED_LOCKED) {
    return finish(Preserved_trx_promotion_resume_status::REGISTRY_NOT_ADOPTED);
  }
  const uint64_t effective_deadline_us =
      requested_deadline_us == 0
          ? snapshot.facts.client_resume_deadline_us
          : std::min(requested_deadline_us,
                     snapshot.facts.client_resume_deadline_us);
  if (effective_deadline_us == 0 ||
      preserve_trx_monotonic_us() >= effective_deadline_us) {
    return finish(Preserved_trx_promotion_resume_status::DEADLINE_EXPIRED);
  }
  mysql_mutex_lock(&target->LOCK_thd_data);
  const bool target_pristine = target->killed == THD::NOT_KILLED &&
                               !target->release_resources_done() &&
                               current_thd == target &&
                               target->preserve_trx_batch_state ==
                                   Preserve_trx_batch_thd_state::NONE;
  mysql_mutex_unlock(&target->LOCK_thd_data);
  if (!target_pristine || target->in_active_multi_stmt_transaction()) {
    return finish(Preserved_trx_promotion_resume_status::TARGET_NOT_PRISTINE);
  }

  Preserved_trx_record preview;
  if (!preserved_trx_find_record(key.token, &preview) ||
      preview.state !=
          Preserved_trx_lifecycle_state::ADOPTED_FOR_PROMOTION ||
      preview.resumable || preview.trx == nullptr ||
      preview.metadata.token != key.token || !preview.has_promotion_key ||
      !preserved_trx_promotion_keys_match(preview.promotion_key, key)) {
    return finish(
        Preserved_trx_promotion_resume_status::PROMOTION_RECORD_NOT_FOUND);
  }
  if (preserve_trx_is_unsupported_common_context(target) ||
      !trx_preserve_thd_can_accept_preserved_trx(target) ||
      !recoverable_binlog_state(preview.metadata.binlog_state) ||
      !binlog_state_matches_current_mode(preview.metadata) ||
      !preserved_trx_resume_binlog_format_is_supported(target,
                                                        preview.metadata)) {
    return finish(Preserved_trx_promotion_resume_status::TARGET_NOT_PRISTINE);
  }
  const bool metadata_has_binlog_cache =
      preview.metadata.binlog_state ==
      Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE;
  if (metadata_has_binlog_cache != snapshot.facts.binlog_cache_present) {
    return finish(Preserved_trx_promotion_resume_status::STAGING_FAILED);
  }

  Preserve_strict_attach_intent_write_context intent_context;
  intent_context.state = Preserve_trx_strict_attach_intent_state::ATTACHING;
  intent_context.target_connection_id = target->thread_id();
  if (!write_strict_attach_intent(key, &intent_context)) {
    return finish(
        Preserved_trx_promotion_resume_status::ATTACH_INTENT_IO_ERROR);
  }

  Preserve_trx_attach_lease attach_lease;
  if (registry.begin_attach(key, key.generation, &attach_lease) !=
      Preserve_trx_prepared_status::OK) {
    intent_context.state =
        Preserve_trx_strict_attach_intent_state::ATTACH_ROLLED_BACK;
    if (write_strict_attach_intent(key, &intent_context)) {
      remove_strict_attach_intent(key);
    }
    return finish(Preserved_trx_promotion_resume_status::REGISTRY_NOT_ADOPTED);
  }

  Preserved_trx_record record;
  if (!preserved_trx_take_promotion_adopted_record(key, &record)) {
    intent_context.state =
        Preserve_trx_strict_attach_intent_state::ATTACH_ROLLED_BACK;
    const bool terminal_written =
        write_strict_attach_intent(key, &intent_context);
    if (terminal_written &&
        registry.abort_attach_after_full_unwind(&attach_lease) ==
            Preserve_trx_prepared_status::OK) {
      remove_strict_attach_intent(key);
      return finish(
          Preserved_trx_promotion_resume_status::PROMOTION_RECORD_NOT_FOUND);
    }
    if (attach_lease.active()) (void)registry.taint_attach(&attach_lease);
    return finish(Preserved_trx_promotion_resume_status::ATTACH_TAINTED);
  }

  Resume_thd_state_guard thd_state_guard(target);
  Preserved_trx_resume_runtime resume_runtime;
  bool &binlog_attached = resume_runtime.strict_binlog_attached;
  bool &mdl_cloned = resume_runtime.mdl_transferred;
  bool &gtid_restored = resume_runtime.gtid_restored;
  bool &temp_materialized = resume_runtime.temp_tables_materialized;
  bool &trx_attached = resume_runtime.trx_attached;

  auto pre_boundary_failure = [&](const std::string &reason) {
    bool unwind_ok = true;
    if (trx_attached) {
      if (trx_preserve_detach_resumed_from_thd(record.trx, target) !=
              DB_SUCCESS &&
          trx_preserve_detach_resumed_from_thd_for_cleanup(record.trx,
                                                            target) !=
              DB_SUCCESS) {
        unwind_ok = false;
      }
      trx_attached = false;
    }
    if (temp_materialized &&
        preserve_trx_temp_table_rollback_materialized_for_resume(
            target, record.metadata) != Preserve_snapshot_status::OK) {
      unwind_ok = false;
    }
    temp_materialized = false;
    rollback_restored_logged_cache_gtid_next(target, &gtid_restored);
    if (mdl_cloned) target->mdl_context.release_transactional_locks();
    mdl_cloned = false;
    if (binlog_attached) {
      if (mysql_binlog_preserve_abort_detached_cache_attach(
              &resume_runtime.strict_binlog_journal,
              &resume_runtime.strict_binlog_handle) !=
          Mysql_binlog_preserve_cache_status::OK) {
        unwind_ok = false;
      }
      binlog_attached = false;
    }
    if (resume_runtime.strict_binlog_handle != nullptr &&
        attach_lease.restore_native_binlog_handle(
            &resume_runtime.strict_binlog_handle) !=
            Preserve_trx_prepared_status::OK) {
      unwind_ok = false;
    }
    reset_thd_after_preserve_detach(target);
    if (restore_record_after_resume_failure(record, reason)) unwind_ok = false;
    intent_context.state = unwind_ok
                               ? Preserve_trx_strict_attach_intent_state::
                                     ATTACH_ROLLED_BACK
                               : Preserve_trx_strict_attach_intent_state::
                                     ATTACH_TAINTED;
    if (!write_strict_attach_intent(key, &intent_context)) unwind_ok = false;
    if (unwind_ok &&
        registry.abort_attach_after_full_unwind(&attach_lease) ==
            Preserve_trx_prepared_status::OK) {
      remove_strict_attach_intent(key);
      return finish(Preserved_trx_promotion_resume_status::STAGING_FAILED);
    }
    if (attach_lease.active()) (void)registry.taint_attach(&attach_lease);
    return finish(Preserved_trx_promotion_resume_status::ATTACH_TAINTED);
  };
  Preserved_trx_resume_prepare_options prepare_options;
  prepare_options.source = Preserved_trx_resume_source::STRICT_PROMOTION;
  prepare_options.dir = &key.preserve_dir;
  prepare_options.token = &key.token;
  prepare_options.strict_attach_lease = &attach_lease;
  prepare_options.strict_facts = &snapshot.facts;
  prepare_options.deadline_monotonic_us = effective_deadline_us;
  const Preserved_trx_resume_prepare_result prepare_result =
      prepare_resume_on_current_thd_shared(target, &record, prepare_options,
                                           &resume_runtime);
  if (!prepare_result.ok) {
    return pre_boundary_failure(prepare_result.reason);
  }

  intent_context.state = Preserve_trx_strict_attach_intent_state::ACTIVATING;
  if (registry.begin_activation(&attach_lease, write_strict_attach_intent,
                                &intent_context) !=
      Preserve_trx_prepared_status::OK) {
    return pre_boundary_failure("durable ACTIVATING intent failed");
  }
  auto post_boundary_failure = [&](bool ownership_tainted) {
    if (!ownership_tainted && !trans_rollback(target)) {
      intent_context.state =
          Preserve_trx_strict_attach_intent_state::ATTACH_ROLLED_BACK;
      if (registry.rollback_attach_after_activation(
              &attach_lease, write_strict_attach_intent, &intent_context) ==
          Preserve_trx_prepared_status::OK) {
        delete_detached_mdl_context(key.token);
        (void)delete_preserved_snapshot_files_and_sidecars_or_log(
            key.preserve_dir, key.token, &record.metadata);
        return finish(Preserved_trx_promotion_resume_status::
                          ACTIVATION_FAILED_ROLLED_BACK);
      }
    }
    intent_context.state =
        Preserve_trx_strict_attach_intent_state::ATTACH_TAINTED;
    (void)write_strict_attach_intent(key, &intent_context);
    if (attach_lease.active()) (void)registry.taint_attach(&attach_lease);
    thd_state_guard.dismiss();
    return finish(Preserved_trx_promotion_resume_status::ATTACH_TAINTED);
  };

  if (binlog_attached) {
    const auto binlog_commit_status =
        mysql_binlog_preserve_commit_detached_cache_attach(
            &resume_runtime.strict_binlog_journal);
    if (binlog_commit_status != Mysql_binlog_preserve_cache_status::OK) {
      bool ownership_tainted =
          binlog_commit_status ==
          Mysql_binlog_preserve_cache_status::OWNERSHIP_TAINTED;
      if (!ownership_tainted) {
        const auto abort_status =
            mysql_binlog_preserve_abort_detached_cache_attach(
                &resume_runtime.strict_binlog_journal,
                &resume_runtime.strict_binlog_handle);
        if (abort_status == Mysql_binlog_preserve_cache_status::OK) {
          binlog_attached = false;
          resume_runtime.strict_binlog_handle.reset();
        } else {
          ownership_tainted = true;
        }
      }
      return post_boundary_failure(ownership_tainted);
    }
  }
  binlog_attached = false;
  if (activate_resumed_trx_shared(&record) != DB_SUCCESS) {
    return post_boundary_failure(false);
  }
  intent_context.state = Preserve_trx_strict_attach_intent_state::ACTIVE;
  if (registry.commit_attach(&attach_lease, write_strict_attach_intent,
                             &intent_context) !=
      Preserve_trx_prepared_status::OK) {
    return post_boundary_failure(false);
  }
  if (trx_preserve_finish_resumed_activation(record.trx, target) !=
      DB_SUCCESS) {
    const std::string message =
        redacted_preserved_trx_log_subject(key.token) +
        " failed to clear promoted ACTIVE Undo runtime identity; killing session";
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    target->killed = THD::KILL_CONNECTION;
    thd_state_guard.dismiss();
    return finish(Preserved_trx_promotion_resume_status::ATTACH_TAINTED);
  }

  delete_detached_mdl_context(key.token);
  Preserve_snapshot_remove_options remove_options;
  if (temp_materialized) {
    remove_options.preserve_committed_temp_sidecar_source_space_ids =
        preserve_trx_temp_table_sidecar_source_space_ids(record.metadata);
  }
  Preserve_snapshot_delete_status delete_status =
      delete_snapshot_files_with_status(key.preserve_dir, key.token,
                                        remove_options);
  if (delete_status == Preserve_snapshot_delete_status::OK) {
    remove_strict_attach_intent(key);
  } else {
    const std::string message =
        redacted_preserved_trx_log_subject(key.token) +
        " strict attach artifact cleanup remains pending";
    LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
  }
  if (metadata_has_binlog_cache) {
    preserved_trx_promotion_prepared_note_resume_binlog_io(0, 0, 0);
  }
  thd_state_guard.dismiss();
  audit_preserved_trx_event(target, key.token, "promotion-resume", "success");
  return finish(Preserved_trx_promotion_resume_status::OK);
}

bool Sql_cmd_show_preserved_transactions::execute(THD *thd) {
  DBUG_TRACE;

  mem_root_deque<Item *> fields(thd->mem_root);
  size_t column_count = 0;
  const Preserved_trx_column_metadata *columns =
      preserved_trx_columns(&column_count);

  for (size_t i = 0; i < column_count; ++i) {
    if (preserved_trx_show_column_is_unsigned_integer(columns[i].name)) {
      fields.push_back(new Item_return_int(columns[i].name, columns[i].length,
                                           MYSQL_TYPE_LONGLONG));
    } else {
      fields.push_back(
          new Item_empty_string(columns[i].name, columns[i].length));
    }
  }

  if (thd->send_result_metadata(fields,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    return true;

  Protocol *protocol = thd->get_protocol();
  for (const Preserved_trx_view_row &row : preserved_trx_snapshot(thd)) {
    if (store_preserved_trx_row(protocol, row)) return true;
  }

  my_eof(thd);
  return false;
}
