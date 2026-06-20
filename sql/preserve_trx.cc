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
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <utility>

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
#include "sql/binlog.h"
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
#include "sql/set_var.h"
#include "sql/handler.h"
#include "sql/preserve_trx_carrier.h"
#include "sql/preserve_trx_drain.h"
#include "sql/preserve_trx_kernel.h"
#include "sql/preserve_trx_lock_warmcopy.h"
#include "sql/preserve_trx_temp_table.h"
#include "sql/preserve_trx_warmcopy.h"
#include "sql/preserve_trx_xid.h"
#include "sql/sql_const.h"
#include "sql/sql_class.h"
#include "sql/sql_error.h"
#include "sql/sql_lex.h"
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
uint preserve_trx_max_total = 256;
uint preserve_trx_max_pending_per_user = 256;
uint preserve_trx_batch_max_transactions = 256;
uint preserve_trx_recovery_max_count = 3;
uint preserve_trx_recovery_grace_seconds = 120;
ulonglong preserve_trx_max_snapshot_bytes = 16777216;
ulonglong preserve_trx_max_binlog_cache_bytes = 1073741824;
ulonglong preserve_trx_max_temp_sidecar_bytes = 1073741824;
ulonglong preserve_trx_single_phase_max_binlog_cache_bytes = ULLONG_MAX;
uint preserve_trx_max_lock_count = 2000;
uint preserve_trx_max_modified_tables = 64;
uint preserve_trx_max_scan_pages = 20000;
uint preserve_trx_materialize_timeout_ms = 5000;
ulong preserve_trx_drain_mode = PRESERVE_TRX_DRAIN_MODE_SOFT;
uint preserve_trx_drain_grace_ms = 30000;
uint preserve_trx_drain_hard_timeout_ms = 30000;
bool preserve_trx_warmcopy_enable = true;
uint preserve_trx_warmcopy_close_timeout_ms = 30000;
uint preserve_trx_warmcopy_min_open_ms = 1000;
uint preserve_trx_warmcopy_chunk_bytes = 1048576;
uint preserve_trx_warmcopy_tail_budget_bytes = 1048576;
ulonglong preserve_trx_warmcopy_max_total_bytes = 10737418240ULL;
uint preserve_trx_warmcopy_pending_range_limit = 1024;
ulonglong preserve_trx_warmcopy_pending_bytes_limit = 67108864ULL;
bool preserve_trx_lock_warmcopy_enable = true;
bool preserve_trx_lock_warmcopy_fallback_to_live_export = true;
ulonglong preserve_trx_lock_warmcopy_max_memory_bytes = 268435456ULL;
ulonglong preserve_trx_lock_warmcopy_max_journal_bytes = 1073741824ULL;
uint preserve_trx_lock_warmcopy_max_dirty_shards = 100000;
uint preserve_trx_lock_warmcopy_max_mdl_descriptors = 100000;
uint preserve_trx_lock_warmcopy_seal_threads = 0;
uint preserve_trx_lock_warmcopy_conversion_wait_timeout_ms = 30000;
extern ulong srv_force_recovery;

static std::atomic<bool> g_preserve_trx_enable_cached{false};

bool preserve_trx_is_enabled() {
  return g_preserve_trx_enable_cached.load(std::memory_order_acquire);
}

void preserve_trx_set_enable_value(bool enabled) {
  preserve_trx_enable = enabled;
  g_preserve_trx_enable_cached.store(enabled, std::memory_order_release);
}

std::string preserved_trx_redacted_token(const std::string &token) {
  if (token.empty()) return "****????";
  std::string redacted("****");
  const size_t suffix_length = std::min<size_t>(4, token.length());
  redacted.append(token, token.length() - suffix_length, suffix_length);
  return redacted;
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

std::atomic<ulonglong> g_warmcopy_prefix_bytes{0};
std::atomic<ulonglong> g_warmcopy_digest_bytes{0};
std::atomic<ulonglong> g_warmcopy_durable_bytes{0};
std::atomic<ulonglong> g_warmcopy_provider_full_copy_to_count{0};
std::atomic<ulonglong> g_warmcopy_phase2_pause_us{0};
std::mutex g_warmcopy_status_mutex;

void preserve_trx_warmcopy_reset_status() {
  g_warmcopy_prefix_bytes.store(0);
  g_warmcopy_digest_bytes.store(0);
  g_warmcopy_durable_bytes.store(0);
  g_warmcopy_provider_full_copy_to_count.store(0);
  g_warmcopy_phase2_pause_us.store(0);
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
Preserved_trx_manager_state_publication_probe
    g_manager_state_publication_probe{nullptr};
void *g_manager_state_publication_probe_arg{nullptr};

void preserve_trx_notify_manager_state_published_for_unit_test() {
  Preserved_trx_manager_state_publication_probe probe =
      g_manager_state_publication_probe;
  if (probe != nullptr) probe(g_manager_state_publication_probe_arg);
}

Preserve_trx_manager_state_owner preserve_trx_manager_state_owner_snapshot() {
  return preserve_trx_unpack_manager_state_owner(g_manager_state_owner.load());
}

bool preserve_trx_compare_exchange_manager_state_owner(
    Preserve_trx_manager_state from, Preserve_trx_manager_state to,
    my_thread_id owner_thread_id) {
  uint64_t expected = preserve_trx_pack_manager_state_owner(from, 0);
  const uint64_t desired =
      preserve_trx_pack_manager_state_owner(to, owner_thread_id);
  return g_manager_state_owner.compare_exchange_strong(expected, desired);
}

void preserve_trx_store_manager_state_owner(Preserve_trx_manager_state state,
                                            my_thread_id owner_thread_id) {
  g_manager_state_owner.store(
      preserve_trx_pack_manager_state_owner(state, owner_thread_id));
}

enum class Preserved_trx_lifecycle_state {
  DRAINING,
  SNAPSHOTTING,
  PRESERVED,
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
};

struct Preserve_batch_account_count {
  std::string user;
  std::string host;
  uint count{0};
};

struct Preserve_trx_batch_item {
  my_thread_id original_thread_id{0};
  std::string token;
  bool logged_binlog_cache{false};
};

struct Pending_token_delivery {
  std::string token;
  bool response_observed{false};
  bool ok_delivered{false};
  bool finalizing{false};
};

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
std::vector<Preserved_trx_record> g_preserved_trx_records;
std::mutex g_preserved_trx_reaper_mutex;
std::condition_variable g_preserved_trx_reaper_cond;
std::thread g_preserved_trx_reaper_thread;
bool g_preserved_trx_reaper_started = false;
bool g_preserved_trx_reaper_stop = false;
std::mutex g_preserved_trx_thd_pin_mutex;
std::condition_variable g_preserved_trx_thd_pin_cond;
std::unordered_map<THD *, uint> g_preserved_trx_thd_pin_counts;

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
    if (thd == nullptr || thd->release_resources_done()) return nullptr;
    std::unique_ptr<Preserve_trx_external_thd_pin> pin(
        new Preserve_trx_external_thd_pin(thd));
    {
      std::lock_guard<std::mutex> lock(g_preserved_trx_thd_pin_mutex);
      ++g_preserved_trx_thd_pin_counts[thd];
    }
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
std::mutex g_token_delivery_mutex;
std::unordered_map<THD *, Pending_token_delivery> g_pending_token_delivery;
std::atomic<bool> g_deferred_shutdown_signal_requested{false};

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
                                  blob_descriptors = {}) {
  DBUG_EXECUTE_IF("preserve_trx_fail_add_record", return true;);

  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  if (preserved_trx_record_exists_locked(metadata.token)) return true;
  Preserved_trx_record record;
  record.metadata = metadata;
  record.trx = trx;
  record.resumable = resumable;
  record.state = state;
  record.blob_descriptors = std::move(blob_descriptors);
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

bool preserved_trx_batch_has_capacity(
    const std::vector<Preserve_batch_account_count> &batch_accounts) {
  size_t batch_count = 0;
  for (const Preserve_batch_account_count &account : batch_accounts)
    batch_count += account.count;

  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  size_t persistent_count = 0;
  for (const Preserved_trx_record &record : g_preserved_trx_records) {
    if (!record.observable_only) ++persistent_count;
  }
  if (persistent_count + batch_count > preserve_trx_max_total)
    return false;

  for (const Preserve_batch_account_count &account : batch_accounts) {
    size_t existing_count = 0;
    for (const Preserved_trx_record &record : g_preserved_trx_records) {
      if (!record.observable_only && record.metadata.owner_user == account.user &&
          record.metadata.owner_host == account.host) {
        ++existing_count;
      }
    }
    if (existing_count + account.count > preserve_trx_max_pending_per_user)
      return false;
  }

  return true;
}

bool preserved_trx_mark_resumable(const std::string &token) {
  std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
  for (Preserved_trx_record &record : g_preserved_trx_records) {
    if (!record.observable_only && record.metadata.token == token) {
      record.resumable = true;
      record.state = Preserved_trx_lifecycle_state::PRESERVED;
      return false;
    }
  }
  return true;
}

void preserved_trx_register_pending_token_delivery(THD *thd,
                                                   const std::string &token) {
  std::lock_guard<std::mutex> lock(g_token_delivery_mutex);
  g_pending_token_delivery[thd] = {token, false, false, false};
}

bool preserved_trx_has_pending_token_delivery(THD *thd) {
  std::lock_guard<std::mutex> lock(g_token_delivery_mutex);
  return g_pending_token_delivery.find(thd) != g_pending_token_delivery.end();
}

void note_pending_token_delivery_statement_response(THD *thd) {
  if (thd == nullptr) return;

  std::lock_guard<std::mutex> lock(g_token_delivery_mutex);
  auto it = g_pending_token_delivery.find(thd);
  if (it == g_pending_token_delivery.end()) return;

  Diagnostics_area *da = thd->get_stmt_da();
  it->second.response_observed = true;
  it->second.ok_delivered = da->is_sent() && da->is_ok();
}

bool preserved_trx_begin_pending_token_delivery_finalization(THD *thd,
                                                             std::string *token,
                                                             bool *ok_delivered) {
  std::lock_guard<std::mutex> lock(g_token_delivery_mutex);
  auto it = g_pending_token_delivery.find(thd);
  if (it == g_pending_token_delivery.end() || it->second.finalizing)
    return false;

  if (!it->second.response_observed && thd != nullptr) {
    Diagnostics_area *da = thd->get_stmt_da();
    it->second.response_observed = true;
    it->second.ok_delivered = da->is_sent() && da->is_ok();
  }

  it->second.finalizing = true;
  if (token != nullptr) *token = it->second.token;
  if (ok_delivered != nullptr) *ok_delivered = it->second.ok_delivered;
  return true;
}

void preserved_trx_erase_pending_token_delivery(THD *thd) {
  std::lock_guard<std::mutex> lock(g_token_delivery_mutex);
  g_pending_token_delivery.erase(thd);
}

void mark_preserved_trx_recovery_complete() {
  {
    std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
    g_preserved_trx_recovery_done = true;
  }
  g_preserved_trx_recovery_cond.notify_all();
  preserved_trx_start_expired_reaper_if_ready();
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
  return preserved_trx_default_carrier_token_exists(preserve_trx_default_dir(),
                                                    token);
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

    {
      std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
      if (preserved_trx_record_exists_locked(candidate)) continue;
    }
    if (preserved_trx_file_exists_for_token(candidate)) continue;

    *token = std::move(candidate);
    return false;
  }

  return true;
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

bool hydrate_logged_binlog_cache_payload_if_needed(Preserved_trx_record *record,
                                                   const std::string &token) {
  if (record == nullptr ||
      record->metadata.binlog_state !=
          Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE ||
      !record->metadata.binlog_cache_payload.empty()) {
    return false;
  }

  auto store = create_preserved_trx_default_store(preserve_trx_default_dir());
  Preserved_trx_bundle bundle;
  if (store->read(token, true,
                  Preserved_trx_carrier::Payload_read_mode::
                      WITH_EXTERNAL_BLOBS,
                  &bundle) != Preserve_snapshot_status::OK) {
    return true;
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
    if (!mdl_preserve_namespace_supported(mdl_namespace) ||
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

bool preserve_trx_recheck_modified_table_privileges(
    THD *thd, const std::vector<Preserve_modified_table_name> &tables,
    bool require_all_write_acls = false) {
  if (thd == nullptr) return true;

  constexpr Access_bitmask kModifiedTableWriteAcls =
      INSERT_ACL | UPDATE_ACL | DELETE_ACL;
  Security_context *sctx = thd->security_context();
  const auto recheck_one = [&](const std::string &schema_name,
                               const std::string &table_name) {
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

    return require_all_write_acls
               ? (access & kModifiedTableWriteAcls) != kModifiedTableWriteAcls
               : (access & kModifiedTableWriteAcls) == 0;
  };

  for (const Preserve_modified_table_name &name : tables) {
    if (recheck_one(name.schema_name, name.table_name)) return true;
  }

  return false;
}

bool preserve_trx_recheck_modified_table_privileges(
    THD *thd,
    const std::vector<Preserve_snapshot_modified_table_name> &tables,
    bool require_all_write_acls = false) {
  if (thd == nullptr) return true;

  std::vector<Preserve_modified_table_name> converted;
  converted.reserve(tables.size());
  for (const Preserve_snapshot_modified_table_name &name : tables) {
    converted.push_back({name.schema_name, name.table_name});
  }
  return preserve_trx_recheck_modified_table_privileges(
      thd, converted, require_all_write_acls);
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

class Preserve_trx_manager_state_guard {
 public:
  Preserve_trx_manager_state_guard(Preserve_trx_manager_state from,
                                   Preserve_trx_manager_state to,
                                   my_thread_id owner_thread_id = 0)
      : m_from(from), m_active(false), m_owner_thread_id(owner_thread_id) {
    m_active = preserve_trx_compare_exchange_manager_state_owner(
        from, to, owner_thread_id);
    if (m_active) preserve_trx_notify_manager_state_published_for_unit_test();
  }

  Preserve_trx_manager_state_guard(const Preserve_trx_manager_state_guard &) =
      delete;
  Preserve_trx_manager_state_guard &operator=(
      const Preserve_trx_manager_state_guard &) = delete;

  ~Preserve_trx_manager_state_guard() {
    if (m_active) {
      preserve_trx_store_manager_state_owner(m_from, 0);
    }
  }

  bool active() const { return m_active; }
  void transition_to(Preserve_trx_manager_state to) {
    if (m_active)
      preserve_trx_store_manager_state_owner(to, m_owner_thread_id);
  }
  void transition_to(Preserve_trx_manager_state to,
                     my_thread_id owner_thread_id) {
    if (m_active) preserve_trx_store_manager_state_owner(to, owner_thread_id);
  }
  void dismiss() { m_active = false; }

 private:
  Preserve_trx_manager_state m_from;
  bool m_active;
  my_thread_id m_owner_thread_id;
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

class Preserve_thd_context_switch {
 public:
  Preserve_thd_context_switch(THD *current_thd_arg, THD *target_thd,
                              ulonglong generation)
      : m_current_thd(current_thd_arg),
        m_target_thd(target_thd),
        m_generation(generation),
        m_target_old_real_id(target_thd->real_id),
        m_target_old_thread_stack(target_thd->thread_stack) {
    assert(m_current_thd != nullptr && m_target_thd != nullptr);
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

    m_current_thd->restore_globals();
    m_target_thd->thread_stack = m_current_thd->thread_stack;
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
    m_current_thd->store_globals();
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
  my_thread_t m_target_old_real_id;
  const char *m_target_old_thread_stack;
  bool m_active{false};
};

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
    m_thd->variables.option_bits = m_option_bits;
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
                           uint32_t *innodb_savepoint_count) {
  if (thd == nullptr || payload == nullptr || savepoint_count == nullptr ||
      innodb_savepoint_count == nullptr)
    return true;

  payload->clear();
  *savepoint_count = 0;
  *innodb_savepoint_count = 0;

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

Ha_trx_info *savepoint_ha_list_for_entry(
    THD *thd, const Preserve_sql_savepoint_entry &entry,
    Ha_trx_info *innodb_info, Ha_trx_info *binlog_info) {
  if (entry.has_innodb_handler() && innodb_info == nullptr) return nullptr;
  if (entry.has_binlog_handler() && binlog_info == nullptr) return nullptr;
  if (entry.has_innodb_handler() && entry.has_binlog_handler()) {
    Ha_trx_info *ha_list =
        thd->get_transaction()->ha_trx_info(Transaction_ctx::SESSION);
    for (Ha_trx_info *ha_info = ha_list; ha_info != nullptr;
         ha_info = ha_info->next()) {
      const auto legacy_type = ha_legacy_type(ha_info->ht());
      if (legacy_type != DB_TYPE_INNODB && legacy_type != DB_TYPE_BINLOG)
        return nullptr;
    }
    return ha_list;
  }
  if (entry.has_binlog_handler()) return binlog_info;
  if (entry.has_innodb_handler()) return innodb_info;
  return nullptr;
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
      entries.size() != metadata.savepoint_count) {
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
  for (const Preserve_sql_savepoint_entry &entry : entries) {
    SAVEPOINT *savepoint = static_cast<SAVEPOINT *>(
        trn_ctx->allocate_memory(savepoint_alloc_size));
    if (savepoint == nullptr) return true;

    savepoint->name = trn_ctx->strmake(entry.name.data(), entry.name.length());
    if (savepoint->name == nullptr) return true;
    savepoint->length = entry.name.length();
    savepoint->ha_list =
        savepoint_ha_list_for_entry(thd, entry, innodb_info, binlog_info);
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

void log_redacted_resume_failure(const std::string &token, const char *reason) {
  std::string message = "RESUME PRESERVED TRANSACTION '" +
                        preserved_trx_redacted_token(token) + "' failed";
  if (reason != nullptr && reason[0] != '\0') {
    message.append(": ");
    message.append(reason);
  }
  LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
}

void log_redacted_token_delivery_cleanup_failure(const std::string &token,
                                                 const char *reason) {
  std::string message = redacted_preserved_trx_log_subject(token) +
                        " token delivery cleanup failed";
  if (reason != nullptr && reason[0] != '\0') {
    message.append(": ");
    message.append(reason);
  }
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
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

bool preserve_trx_has_only_supported_rw_engines(
    THD *thd, Preserve_snapshot_binlog_state binlog_state) {
  for (Ha_trx_info *ha_info =
           thd->get_transaction()->ha_trx_info(Transaction_ctx::SESSION);
       ha_info != nullptr; ha_info = ha_info->next()) {
    if (!ha_info->is_trx_read_write()) continue;
    const legacy_db_type legacy_type = ha_legacy_type(ha_info->ht());
    if (legacy_type == DB_TYPE_INNODB) continue;
    if (legacy_type == DB_TYPE_BINLOG &&
        (binlog_state == Preserve_snapshot_binlog_state::LOGGED_EMPTY ||
         binlog_state == Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE))
      continue;
    return false;
  }
  return true;
}

bool preserve_trx_has_explicit_active_transaction(THD *thd) {
  return thd->in_active_multi_stmt_transaction() &&
         (thd->variables.option_bits & OPTION_BEGIN);
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

bool preserve_trx_is_unsupported_common_context(THD *thd) {
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
  if (thd->in_sub_stmt != 0 || thd->sp_runtime_ctx != nullptr) {
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
  if (!preserve_trx_has_only_supported_rw_engines(thd, binlog_state)) {
    return true;
  }

  return false;
}

bool preserve_trx_has_resume_any_privilege(THD *thd) {
  return thd->security_context()
      ->has_global_grant(STRING_WITH_LEN("RESUME_ANY_PRESERVED_TRANSACTION"))
      .first;
}

bool preserve_trx_thd_has_batch_inflight_statement(THD *candidate);

class Preserve_drain_active_transactions final : public Do_THD_Impl {
 public:
  Preserve_drain_active_transactions(THD *owner, bool kill_active)
      : m_owner(owner), m_kill_active(kill_active) {}

  void operator()(THD *candidate) override {
    if (candidate == nullptr || candidate == m_owner) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    const bool active_transaction =
        candidate->in_active_multi_stmt_transaction() ||
        candidate->get_transaction()->is_active(Transaction_ctx::SESSION) ||
        candidate->get_transaction()->is_active(Transaction_ctx::STMT);
    const bool inflight_statement =
        preserve_trx_thd_has_batch_inflight_statement(candidate);
    if (candidate->is_system_thread() &&
        (active_transaction || inflight_statement)) {
      m_has_unsupported_thread = true;
      mysql_mutex_unlock(&candidate->LOCK_thd_data);
      return;
    }
    const bool active_user_statement =
        active_transaction || inflight_statement;

    if (active_user_statement) {
      ++m_active_count;
      if (m_kill_active) {
        /*
          Do not wait at the debug sync point while holding a candidate THD's
          LOCK_thd_data. Test controller sessions may need their own THD lock to
          signal the sync point back to the drain owner.
        */
        mysql_mutex_unlock(&candidate->LOCK_thd_data);
        DEBUG_SYNC(m_owner, "preserve_trx_drain_before_kill_active");
        mysql_mutex_lock(&candidate->LOCK_thd_data);
        const bool still_active_transaction =
            candidate->in_active_multi_stmt_transaction() ||
            candidate->get_transaction()->is_active(Transaction_ctx::SESSION) ||
            candidate->get_transaction()->is_active(Transaction_ctx::STMT);
        const bool still_inflight_statement =
            preserve_trx_thd_has_batch_inflight_statement(candidate);
        if (candidate->is_system_thread() &&
            (still_active_transaction || still_inflight_statement)) {
          m_has_unsupported_thread = true;
          mysql_mutex_unlock(&candidate->LOCK_thd_data);
          return;
        }
        if (!still_active_transaction && !still_inflight_statement) {
          mysql_mutex_unlock(&candidate->LOCK_thd_data);
          return;
        }
        bool debug_skip_kill_active = false;
        DBUG_EXECUTE_IF("preserve_trx_drain_skip_kill_active",
                        debug_skip_kill_active = true;);
        if (debug_skip_kill_active) {
          mysql_mutex_unlock(&candidate->LOCK_thd_data);
          return;
        }
        if (m_owner->killed) {
          m_owner_killed = true;
        } else if (!candidate->is_killable) {
          m_has_unsupported_thread = true;
        } else if (candidate->killed != THD::KILL_CONNECTION) {
          candidate->awake(THD::KILL_CONNECTION);
        }
      }
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

  uint active_count() const { return m_active_count; }
  bool has_unsupported_thread() const { return m_has_unsupported_thread; }
  bool owner_killed() const { return m_owner_killed; }

 private:
  THD *m_owner;
  bool m_kill_active;
  uint m_active_count{0};
  bool m_has_unsupported_thread{false};
  bool m_owner_killed{false};
};

bool preserve_trx_drain_other_active_transactions(THD *thd) {
  const bool hard_configured =
      preserve_trx_drain_mode == PRESERVE_TRX_DRAIN_MODE_HARD;
  const uint64_t now_us = preserve_trx_monotonic_us();
  const uint64_t soft_deadline_us = preserve_trx_monotonic_deadline_after_us(
      now_us, static_cast<uint64_t>(preserve_trx_drain_grace_ms) * 1000ULL);
  bool hard_drain = hard_configured || preserve_trx_drain_grace_ms == 0;
  uint64_t hard_deadline_us = 0;

  auto arm_hard_deadline = [&]() {
    if (hard_deadline_us == 0) {
      hard_deadline_us = preserve_trx_monotonic_deadline_after_us(
          preserve_trx_monotonic_us(),
          static_cast<uint64_t>(preserve_trx_drain_hard_timeout_ms) * 1000ULL);
    }
  };
  if (hard_drain) arm_hard_deadline();

  for (;;) {
    if (thd->killed) return true;
    if (!hard_drain && preserve_trx_monotonic_deadline_expired_at(
                           soft_deadline_us, preserve_trx_monotonic_us())) {
      DEBUG_SYNC(thd, "preserve_trx_drain_before_hard");
      hard_drain = true;
      arm_hard_deadline();
    }
    Preserve_drain_active_transactions drain(thd, hard_drain);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&drain);
    if (drain.owner_killed() || thd->killed) return true;
    if (drain.has_unsupported_thread()) {
      return true;
    }
    if (drain.active_count() == 0) return false;
    if (hard_drain && hard_deadline_us != 0 &&
        preserve_trx_monotonic_deadline_expired_at(
            hard_deadline_us, preserve_trx_monotonic_us())) {
      my_error(ER_PRESERVE_TRX_DRAIN_TIMEOUT, MYF(0));
      return true;
    }
    my_sleep(10000);
  }
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
    case SQLCOM_PREPARE_SHUTDOWN_PRESERVE:
    case SQLCOM_DRAIN_TRANSACTIONS_PRESERVE:
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
  if (!preserve_trx_has_explicit_active_transaction(candidate)) return false;
  return true;
}

void preserve_trx_batch_add_account(
    THD *candidate, std::vector<Preserve_batch_account_count> *account_counts) {
  Security_context *sctx = candidate->security_context();
  const std::string user = lex_cstring_to_string(sctx->priv_user());
  const std::string host = lex_cstring_to_string(sctx->priv_host());
  for (Preserve_batch_account_count &account : *account_counts) {
    if (account.user == user && account.host == host) {
      ++account.count;
      return;
    }
  }
  account_counts->push_back({user, host, 1});
}

class Preserve_batch_target_counter final : public Do_THD_Impl {
 public:
  Preserve_batch_target_counter(THD *owner, ulonglong generation)
      : m_owner(owner), m_generation(generation) {}

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

      const bool active_explicit_transaction =
          preserve_trx_has_explicit_active_transaction(candidate);
      const bool batch_inflight_statement =
          preserve_trx_thd_has_batch_inflight_statement(candidate);
      const bool nonidle_explicit_transaction =
          active_explicit_transaction && !candidate->m_server_idle;
      const bool nonidle_unclassified_command_packet =
          !candidate->m_server_idle && candidate->is_classic_protocol();
      if (!active_explicit_transaction && !batch_inflight_statement &&
          !nonidle_unclassified_command_packet) {
        mysql_mutex_unlock(&candidate->LOCK_thd_data);
        return;
      }

      if (active_explicit_transaction) ++m_transaction_count;
      const bool unstable_unsupported =
          candidate->preserve_trx_batch_state !=
              Preserve_trx_batch_thd_state::NONE ||
          candidate->killed != THD::NOT_KILLED;
      const bool idle_unsupported =
          active_explicit_transaction && !batch_inflight_statement &&
          preserve_trx_is_unsupported_common_context(candidate);
      const bool unsupported = unstable_unsupported || idle_unsupported;
      const bool idle_target =
          !unsupported &&
          preserve_trx_batch_candidate_is_idle_target(m_owner, candidate);
      const bool pending_target =
          !unsupported && !idle_target &&
          (batch_inflight_statement || nonidle_explicit_transaction ||
           nonidle_unclassified_command_packet);

      if (unsupported) {
        m_has_unsupported_transaction = true;
      } else if (idle_target) {
        ++m_target_count;
        m_target_thread_ids.push_back(candidate->thread_id());
        preserve_trx_batch_add_account(candidate, &m_account_counts);
        candidate->preserve_trx_batch_generation = m_generation;
        candidate->preserve_trx_batch_state =
            Preserve_trx_batch_thd_state::QUIESCED;
      } else if (pending_target) {
        ++m_target_count;
        m_target_thread_ids.push_back(candidate->thread_id());
        if (active_explicit_transaction)
          preserve_trx_batch_add_account(candidate, &m_account_counts);
        candidate->preserve_trx_batch_generation = m_generation;
        candidate->preserve_trx_batch_state =
            Preserve_trx_batch_thd_state::PENDING_QUIESCE;
      } else {
        ++m_nonidle_transaction_count;
      }
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

  uint transaction_count() const { return m_transaction_count; }
  uint target_count() const { return m_target_count; }
  uint nonidle_transaction_count() const { return m_nonidle_transaction_count; }
  bool has_unsupported_transaction() const {
    return m_has_unsupported_transaction;
  }
  const std::vector<my_thread_id> &target_thread_ids() const {
    return m_target_thread_ids;
  }
  const std::vector<Preserve_batch_account_count> &account_counts() const {
    return m_account_counts;
  }

 private:
  THD *m_owner;
  ulonglong m_generation;
  uint m_transaction_count{0};
  uint m_target_count{0};
  uint m_nonidle_transaction_count{0};
  bool m_has_unsupported_transaction{false};
  std::vector<my_thread_id> m_target_thread_ids;
  std::vector<Preserve_batch_account_count> m_account_counts;
};

static constexpr uint kPreserveTrxBatchQuiescedWaitWarningLoops = 1000;

static bool preserve_trx_publish_pending_quiesce_at_idle_boundary(THD *thd) {
  if (thd == nullptr) return false;

  if (thd->preserve_trx_batch_state !=
          Preserve_trx_batch_thd_state::PENDING_QUIESCE ||
      !thd->m_server_idle)
    return false;

  if (preserve_trx_has_explicit_active_transaction(thd)) {
    thd->preserve_trx_batch_state = Preserve_trx_batch_thd_state::QUIESCED;
  } else {
    thd->preserve_trx_batch_state =
        Preserve_trx_batch_thd_state::DRAINED_NO_TRANSACTION;
  }
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
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

  bool seen() const { return m_seen; }
  Preserve_trx_batch_thd_state state() const { return m_state; }

 private:
  ulonglong m_generation;
  my_thread_id m_target_thread_id;
  bool m_seen{false};
  Preserve_trx_batch_thd_state m_state{Preserve_trx_batch_thd_state::NONE};
};

bool preserve_trx_batch_thread_id_in_targets(
    my_thread_id thread_id, const std::vector<my_thread_id> &target_thread_ids) {
  for (const my_thread_id target_thread_id : target_thread_ids) {
    if (thread_id == target_thread_id) return true;
  }
  return false;
}

bool preserve_trx_quiesced_batch_target_is_valid_locked(
    THD *candidate, ulonglong generation,
    const std::vector<my_thread_id> &target_thread_ids) {
  return candidate != nullptr &&
         candidate->preserve_trx_batch_generation == generation &&
         candidate->preserve_trx_batch_state ==
             Preserve_trx_batch_thd_state::QUIESCED &&
         preserve_trx_batch_thread_id_in_targets(candidate->thread_id(),
                                                 target_thread_ids) &&
         !candidate->release_resources_done() && !candidate->is_system_thread() &&
         candidate->killed == THD::NOT_KILLED && candidate->m_server_idle &&
         preserve_trx_has_explicit_active_transaction(candidate) &&
         !preserve_trx_is_unsupported_common_context(candidate);
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

class Preserve_batch_quiesced_target_pin_collector final : public Do_THD_Impl {
 public:
  Preserve_batch_quiesced_target_pin_collector(
      THD *owner, ulonglong generation,
      const std::vector<my_thread_id> &target_thread_ids)
      : m_owner(owner),
        m_generation(generation),
        m_target_thread_ids(target_thread_ids) {}

  void operator()(THD *candidate) override {
    if (candidate == nullptr || candidate == m_owner) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    const bool target = preserve_trx_quiesced_batch_target_is_valid_locked(
        candidate, m_generation, m_target_thread_ids);
    std::unique_ptr<Preserve_trx_external_thd_pin> pin;
    if (target) pin = Preserve_trx_external_thd_pin::acquire_locked(candidate);
    mysql_mutex_unlock(&candidate->LOCK_thd_data);

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
    const bool target = preserve_trx_quiesced_batch_target_is_valid_locked(
        candidate, m_generation, m_target_thread_ids);
    std::unique_ptr<Preserve_trx_external_thd_pin> pin;
    if (target) pin = Preserve_trx_external_thd_pin::acquire_locked(candidate);
    mysql_mutex_unlock(&candidate->LOCK_thd_data);

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
          !preserve_trx_has_explicit_active_transaction(candidate) ||
          preserve_trx_is_unsupported_common_context(candidate);
      if (unsupported) {
        m_has_unsupported_transaction = true;
      } else {
        ++m_target_count;
        preserve_trx_batch_add_account(candidate, &m_account_counts);
      }
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

  uint target_count() const { return m_target_count; }
  bool has_unsupported_transaction() const {
    return m_has_unsupported_transaction;
  }
  const std::vector<Preserve_batch_account_count> &account_counts() const {
    return m_account_counts;
  }

 private:
  THD *m_owner;
  ulonglong m_generation;
  const std::vector<my_thread_id> &m_target_thread_ids;
  uint m_target_count{0};
  bool m_has_unsupported_transaction{false};
  std::vector<Preserve_batch_account_count> m_account_counts;
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
      m_cleared = true;
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

 private:
  ulonglong m_generation;
  my_thread_id m_target_thread_id;
  bool m_cleared{false};
};

bool preserve_trx_batch_wait_target_ready(
    THD *owner, ulonglong generation, my_thread_id target_thread_id,
    Preserve_trx_batch_thd_state *state, ulonglong close_deadline_us) {
  for (;;) {
    if (owner != nullptr && owner->killed) return true;
    if (preserve_trx_monotonic_deadline_expired_at(
            close_deadline_us, preserve_trx_monotonic_us()))
      return true;

    Preserve_batch_target_state_reader reader(generation, target_thread_id);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&reader);
    if (!reader.seen()) return true;

    const Preserve_trx_batch_thd_state current_state = reader.state();
    if (current_state != Preserve_trx_batch_thd_state::PENDING_QUIESCE) {
      *state = current_state;
      return false;
    }
    my_sleep(10000);
  }
}

ulonglong preserve_trx_batch_hard_deadline_us() {
  return preserve_trx_monotonic_deadline_after_ms(
      preserve_trx_monotonic_us(), preserve_trx_drain_hard_timeout_ms);
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
    }
    mysql_mutex_unlock(&candidate->LOCK_thd_data);
  }

 private:
  ulonglong m_generation;
};

bool warmcopy_close_deadline_expired(ulonglong close_deadline_us);

unsigned long warmcopy_close_timeout_ms_until_deadline(
    ulonglong close_deadline_us, unsigned long fallback_timeout_ms);

class Warmcopy_batch_blob_provider final : public PreserveBinlogBlobProvider {
  struct Entry {
    Mysql_binlog_warmcopy_session *session{nullptr};
    std::string warmcopy_id;
    bool preparing{false};
    uint64_t reserved_size{0};
    uint64_t session_blob_limit{0};
  };

 public:
  explicit Warmcopy_batch_blob_provider(std::string dir)
      : m_dir(std::move(dir)),
        m_carrier(create_preserved_trx_default_warm_external_blob_carrier(
            m_dir)) {
    if (m_carrier == nullptr) m_error = true;
  }

  ~Warmcopy_batch_blob_provider() override { cleanup_warm_artifacts(); }

  bool prepare_blob_for_thd(THD *thd, uint64_t epoch) {
    return prepare_thd(thd, epoch);
  }

  bool prepare_blob_for_thd_if_present(THD *thd, uint64_t epoch) {
    uint64_t cache_length = 0;
    bool has_current_blob = false;
    if (mysql_binlog_preserve_warmcopy_cache_length(thd, &cache_length,
                                                    &has_current_blob)) {
      std::lock_guard<std::mutex> guard(m_mutex);
      m_error = true;
      return false;
    }
    if (!has_current_blob || cache_length == 0) return true;
    return prepare_thd(thd, epoch);
  }

  bool prepare_current_thd_if_needed(THD *thd, uint64_t epoch) {
    return prepare_thd(thd, epoch);
  }

  bool has_blob_for_thd(const THD *thd) const override {
    if (thd == nullptr) return false;
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto it = m_entries.find(thd->thread_id());
    if (it == m_entries.end() || it->second.preparing ||
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

  Preserve_snapshot_status finalize_for_preserve(
      THD *thd, const std::string &, PrebuiltBinlogCacheBlob *blob) override {
    /*
      This metric is scoped to warm-copy's binlog-cache phase-2 work
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
    auto log_provider_failure = [&](const char *reason, uint64_t reserved_size,
                                    uint64_t finalized_size) {
      std::ostringstream message;
      message << "PRESERVE: warm-copy provider finalize failed"
              << " reason=" << reason
              << " thread_id=" << (thd == nullptr ? 0 : thd->thread_id())
              << " reserved_size=" << reserved_size
              << " finalized_size=" << finalized_size
              << " total_bytes=" << m_total_bytes
              << " max_total_bytes=" << preserve_trx_warmcopy_max_total_bytes
              << " tail_budget=" << preserve_trx_warmcopy_tail_budget_bytes
              << " close_deadline_us=" << m_close_deadline_us;
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.str().c_str());
    };

    if (thd == nullptr || blob == nullptr) {
      log_provider_failure("invalid argument", 0, 0);
      return finish(Preserve_snapshot_status::INVALID_ARGUMENT);
    }
    std::lock_guard<std::mutex> guard(m_mutex);
    auto it = m_entries.find(thd->thread_id());
    if (it == m_entries.end() || it->second.preparing) {
      log_provider_failure("entry missing or still preparing", 0, 0);
      return finish(Preserve_snapshot_status::INVALID_ARGUMENT);
    }
    Entry &entry = it->second;
    Mysql_binlog_warmcopy_session *session = entry.session;
    if (session == nullptr) {
      log_provider_failure("session missing", entry.reserved_size, 0);
      return finish(Preserve_snapshot_status::IO_ERROR);
    }

    bool has_final_blob = false;
    PrebuiltBinlogCacheBlob finalized;
    const bool finalize_failed = mysql_binlog_preserve_warmcopy_finalize_session(
        thd, session, preserve_trx_warmcopy_tail_budget_bytes, &finalized,
        &has_final_blob);
    if (finalize_failed || !has_final_blob) {
      log_provider_failure(finalize_failed ? "session finalize failed"
                                           : "session produced no final blob",
                           entry.reserved_size, finalized.size);
      return finish(Preserve_snapshot_status::IO_ERROR);
    }
    /*
      m_close_deadline_us bounds the admission-closing and quiesced-target
      preparation window.  Once phase-2 target preserve has started, rejecting a
      ready warm-copy artifact solely because that earlier deadline elapsed can
      turn a large but otherwise consistent batch into a cleanup failure.  The
      session finalize path itself is bounded by its tail budget and by the
      surrounding batch drain lifecycle.
    */
    if (!can_account_replacement(entry.reserved_size, finalized.size)) {
      log_provider_failure("replacement accounting exceeded budget",
                           entry.reserved_size, finalized.size);
      if (m_carrier != nullptr) {
        (void)m_carrier->remove_warm_external_blob(finalized.warmcopy_id,
                                                   finalized.name);
      }
      return finish(Preserve_snapshot_status::IO_ERROR);
    }
    mysql_binlog_preserve_warmcopy_abort_session(session);
    m_total_bytes = m_total_bytes - entry.reserved_size + finalized.size;
    entry.reserved_size = finalized.size;
    entry.session = nullptr;
    *blob = finalized;
    return finish(Preserve_snapshot_status::OK);
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
    std::lock_guard<std::mutex> guard(m_mutex);
    return m_total_bytes;
  }

  size_t prepared_count() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    return m_entries.size();
  }

  void set_close_deadline_us(ulonglong close_deadline_us) {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_close_deadline_us = close_deadline_us;
  }

  bool tail_budget_exceeded(THD *thd, bool *exceeded) const {
    if (exceeded != nullptr) *exceeded = false;
    if (thd == nullptr) return false;
    Mysql_binlog_warmcopy_session *session = nullptr;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      const auto it = m_entries.find(thd->thread_id());
      if (it == m_entries.end() || it->second.preparing) return false;
      session = it->second.session;
    }
    if (session == nullptr) return true;
    return mysql_binlog_preserve_warmcopy_tail_budget_exceeded(
        thd, session, preserve_trx_warmcopy_tail_budget_bytes, exceeded);
  }

  void cleanup_warm_artifacts() {
    std::lock_guard<std::mutex> guard(m_mutex);
    for (auto &entry : m_entries) {
      if (entry.second.session != nullptr) {
        mysql_binlog_preserve_warmcopy_abort_session(entry.second.session);
        entry.second.session = nullptr;
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
  mutable std::mutex m_mutex;
  std::condition_variable m_condition;
  std::unordered_map<my_thread_id, Entry> m_entries;
  uint64_t m_total_bytes{0};
  ulonglong m_close_deadline_us{0};
  bool m_error{false};

  bool prepare_thd(THD *thd, uint64_t epoch) {
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
    bool has_current_blob = false;
    if (mysql_binlog_preserve_warmcopy_cache_length(thd, &cache_length,
                                                    &has_current_blob)) {
      std::lock_guard<std::mutex> guard(m_mutex);
      m_error = true;
      return false;
    }
    if (!has_current_blob) cache_length = 0;

    uint64_t reservation = 0;
    uint64_t begin_blob_limit = 0;
    uint64_t session_blob_limit = 0;

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
      if (warmcopy_effective_entry_blob_limit(
              m_total_bytes, 0, preserve_trx_warmcopy_max_total_bytes,
              preserve_trx_max_binlog_cache_bytes,
              &begin_blob_limit) ||
          warmcopy_reservation_with_tail_budget(
              cache_length, preserve_trx_warmcopy_tail_budget_bytes,
              begin_blob_limit, &reservation) ||
          warmcopy_accounted_session_reservation(reservation, reservation,
                                                &reservation) ||
          !can_account_replacement(0, reservation)) {
        m_error = true;
        return false;
      }
      session_blob_limit = reservation;
      Entry entry;
      entry.preparing = true;
      entry.warmcopy_id = warmcopy_id;
      entry.reserved_size = reservation;
      entry.session_blob_limit = session_blob_limit;
      m_entries.emplace(thread_id, entry);
      m_total_bytes += reservation;
    }

    bool has_blob = false;
    uint64_t prefix_bytes = 0;
    Mysql_binlog_warmcopy_session *session = nullptr;
    if (m_carrier == nullptr) {
      std::lock_guard<std::mutex> guard(m_mutex);
      release_entry_locked(thread_id);
      m_error = true;
      return false;
    }
    if (mysql_binlog_preserve_warmcopy_begin_session(
            thd, warmcopy_id, epoch, m_carrier.get(), session_blob_limit,
            &session, &has_blob, &prefix_bytes)) {
      std::lock_guard<std::mutex> guard(m_mutex);
      release_entry_locked(thread_id);
      m_error = true;
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
        uint64_t current_entry_limit = 0;
        uint64_t actual_reservation = 0;
        if (warmcopy_effective_entry_blob_limit(
                m_total_bytes, it->second.reserved_size,
                preserve_trx_warmcopy_max_total_bytes,
                preserve_trx_max_binlog_cache_bytes, &current_entry_limit) ||
            warmcopy_reservation_with_tail_budget(
                prefix_bytes, preserve_trx_warmcopy_tail_budget_bytes,
                current_entry_limit, &actual_reservation) ||
            warmcopy_accounted_session_reservation(
                it->second.session_blob_limit, actual_reservation,
                &actual_reservation) ||
            !can_account_replacement(it->second.reserved_size,
                                     actual_reservation)) {
          if (session != nullptr) {
            mysql_binlog_preserve_warmcopy_abort_session(session);
            session = nullptr;
          }
          release_entry_locked(thread_id);
          m_error = true;
          return false;
        }
        m_total_bytes =
            m_total_bytes - it->second.reserved_size + actual_reservation;
        it->second.reserved_size = actual_reservation;
        it->second.preparing = false;
        it->second.session = session;
        session = nullptr;
        m_condition.notify_all();
      }
    }

    if (session != nullptr) mysql_binlog_preserve_warmcopy_abort_session(session);
    return true;
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
      m_error = true;
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
      m_error = true;
      return false;
    }
    return !m_error;
  }

  bool can_account_replacement(uint64_t old_size, uint64_t new_size) const {
    if (old_size > m_total_bytes) return false;
    const uint64_t base_total = m_total_bytes - old_size;
    return new_size <= preserve_trx_max_binlog_cache_bytes &&
           new_size <= preserve_trx_warmcopy_max_total_bytes &&
           base_total <= preserve_trx_warmcopy_max_total_bytes - new_size;
  }

  void release_entry_locked(my_thread_id thread_id) {
    auto it = m_entries.find(thread_id);
    if (it == m_entries.end()) return;
    if (it->second.session != nullptr) {
      mysql_binlog_preserve_warmcopy_abort_session(it->second.session);
      it->second.session = nullptr;
    }
    if (it->second.reserved_size <= m_total_bytes)
      m_total_bytes -= it->second.reserved_size;
    else
      m_total_bytes = 0;
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
      ulonglong close_deadline_us)
      : m_generation(generation),
        m_target_thread_ids(target_thread_ids),
        m_provider(provider),
        m_epoch(epoch),
        m_close_deadline_us(close_deadline_us) {}

  void prepare(THD *candidate) {
    if (m_error || candidate == nullptr || m_provider == nullptr) return;
    if (warmcopy_close_deadline_expired(m_close_deadline_us)) {
      m_error = true;
      return;
    }

    if (!preserve_trx_quiesced_batch_target_is_valid(
            candidate, m_generation, m_target_thread_ids)) {
      m_error = true;
      return;
    }

    if (candidate->binlog_flush_pending_rows_event(true) ||
        !m_provider->prepare_blob_for_thd_if_present(candidate, m_epoch)) {
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
                                 preserve_trx_has_explicit_active_transaction(
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
                                      bool debug_fail_temp_only_prepare = false)
      : m_owner(owner),
        m_options(options),
        m_timeout_seconds(timeout_seconds),
        m_generation(generation),
        m_target_thread_id(target_thread_id),
        m_binlog_blob_provider(binlog_blob_provider),
        m_lock_warmcopy_artifact(lock_warmcopy_artifact),
        m_debug_fail_ha_prepare_low(debug_fail_ha_prepare_low),
        m_debug_fail_temp_only_prepare(debug_fail_temp_only_prepare) {}

  void run(THD *candidate) {
    if (m_visited_target || candidate == nullptr) return;

    const std::vector<my_thread_id> single_target{m_target_thread_id};
    if (!preserve_trx_quiesced_batch_target_is_valid(
            candidate, m_generation, single_target)) {
      m_error = true;
      m_visited_target = true;
      clear_target_after_error(candidate);
      return;
    }

    bool early_target_error = false;
    {
      Preserve_thd_context_switch switch_thd(m_owner, candidate, m_generation);
      if (!switch_thd.active()) {
        m_error = true;
        m_visited_target = true;
        early_target_error = true;
      } else if (preserve_trx_is_unsupported_common_context(candidate)) {
        m_error = true;
        m_visited_target = true;
        early_target_error = true;
      } else {
        m_error = preserve_trx_preserve_attached_transaction(
            candidate, m_options, m_timeout_seconds,
            Preserve_trx_delivery_mode::BATCH_MANAGER_DELIVERY, &m_result,
            m_binlog_blob_provider, m_lock_warmcopy_artifact,
            m_debug_fail_ha_prepare_low, m_debug_fail_temp_only_prepare);
      }
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
  const Preserve_trx_preserve_result &result() const { return m_result; }

 private:
  void clear_target_after_error(THD *candidate) const {
    if (candidate == nullptr) return;
    mysql_mutex_lock(&candidate->LOCK_thd_data);
    if (candidate->preserve_trx_batch_generation == m_generation &&
        candidate->preserve_trx_batch_state !=
            Preserve_trx_batch_thd_state::PRESERVED_DRAINED) {
      candidate->preserve_trx_batch_generation = 0;
      candidate->preserve_trx_batch_state = Preserve_trx_batch_thd_state::NONE;
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
  bool m_visited_target{false};
  bool m_error{false};
  Preserve_trx_preserve_result m_result;
};

std::mutex g_warmcopy_admission_mutex;
std::condition_variable g_warmcopy_admission_cond;
std::atomic<bool> g_warmcopy_admission_open{false};
std::shared_ptr<Warmcopy_batch_blob_provider> g_warmcopy_admission_provider;
uint64_t g_warmcopy_admission_epoch{0};
my_thread_id g_warmcopy_admission_owner_thread_id{0};
std::unordered_map<Warmcopy_batch_blob_provider *,
                   std::unordered_map<uint64_t, uint64_t>>
    g_warmcopy_admission_inflight;

uint64_t warmcopy_admission_inflight_count_locked(
    Warmcopy_batch_blob_provider *provider, uint64_t epoch) {
  auto provider_it = g_warmcopy_admission_inflight.find(provider);
  if (provider_it == g_warmcopy_admission_inflight.end()) return 0;
  auto epoch_it = provider_it->second.find(epoch);
  return epoch_it == provider_it->second.end() ? 0 : epoch_it->second;
}

void warmcopy_admission_increment_inflight_locked(
    Warmcopy_batch_blob_provider *provider, uint64_t epoch) {
  ++g_warmcopy_admission_inflight[provider][epoch];
}

void warmcopy_admission_decrement_inflight_locked(
    Warmcopy_batch_blob_provider *provider, uint64_t epoch) {
  auto provider_it = g_warmcopy_admission_inflight.find(provider);
  assert(provider_it != g_warmcopy_admission_inflight.end());
  auto epoch_it = provider_it->second.find(epoch);
  assert(epoch_it != provider_it->second.end() && epoch_it->second > 0);
  if (--epoch_it->second == 0) provider_it->second.erase(epoch_it);
  if (provider_it->second.empty())
    g_warmcopy_admission_inflight.erase(provider_it);
}

uint warmcopy_admission_destructor_close_timeout_ms() {
  return preserve_trx_warmcopy_close_timeout_ms == 0
             ? 1
             : preserve_trx_warmcopy_close_timeout_ms;
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

class Warmcopy_open_admission_scope {
 public:
  Warmcopy_open_admission_scope(
      std::shared_ptr<Warmcopy_batch_blob_provider> provider,
                                uint64_t epoch, my_thread_id owner_thread_id)
      : m_provider(std::move(provider)), m_epoch(epoch) {
    std::lock_guard<std::mutex> guard(g_warmcopy_admission_mutex);
    g_warmcopy_admission_provider = m_provider;
    g_warmcopy_admission_epoch = epoch;
    g_warmcopy_admission_owner_thread_id = owner_thread_id;
    g_warmcopy_admission_open.store(true, std::memory_order_release);
  }

  ~Warmcopy_open_admission_scope() {
    close(warmcopy_admission_destructor_close_timeout_ms());
  }

  bool close(uint timeout_ms = 0) {
    if (m_closed) return true;
    std::unique_lock<std::mutex> guard(g_warmcopy_admission_mutex);
    if (g_warmcopy_admission_provider == m_provider) {
      g_warmcopy_admission_open.store(false, std::memory_order_release);
      g_warmcopy_admission_provider.reset();
      g_warmcopy_admission_epoch = 0;
      g_warmcopy_admission_owner_thread_id = 0;
    }
    Warmcopy_batch_blob_provider *provider = m_provider.get();
    const uint64_t epoch = m_epoch;
    const auto no_inflight = [provider, epoch] {
      return warmcopy_admission_inflight_count_locked(provider, epoch) == 0;
    };
    const bool drained =
        timeout_ms == 0
            ? (g_warmcopy_admission_cond.wait(guard, no_inflight), true)
            : g_warmcopy_admission_cond.wait_for(
                  guard, std::chrono::milliseconds(timeout_ms), no_inflight);
    if (!drained) {
      m_closed = true;
      m_provider.reset();
      return false;
    }
    m_closed = true;
    m_provider.reset();
    return true;
  }

 private:
  std::shared_ptr<Warmcopy_batch_blob_provider> m_provider;
  uint64_t m_epoch{0};
  bool m_closed{false};
};

class Warmcopy_batch_drain_participant final
    : public Preserve_trx_drain_participant {
 public:
  Warmcopy_batch_drain_participant(THD *owner, ulonglong generation,
                                   unsigned long close_timeout_ms)
      : m_owner(owner),
        m_generation(generation),
        m_close_timeout_ms(close_timeout_ms) {}

  bool open_phase1() override {
    preserve_trx_warmcopy_reset_status();
    m_observation = {};
    m_observation.state = Preserve_trx_drain_participant_state::OPEN;
    m_observation.bytes_budget = preserve_trx_warmcopy_max_total_bytes;
    m_observation.phase1_progress = 1;
    m_provider =
        std::make_shared<Warmcopy_batch_blob_provider>(preserve_trx_default_dir());
    if (m_provider == nullptr || m_provider->error()) {
      mark_degraded("warm-copy provider open failed");
      return false;
    }
    m_provider->set_close_deadline_us(m_closing_deadline_us);

    m_admission_scope = std::make_unique<Warmcopy_open_admission_scope>(
        m_provider, m_generation, m_owner->thread_id());

    DEBUG_SYNC(m_owner, "preserve_trx_warmcopy_open");
    if (preserve_trx_warmcopy_min_open_ms != 0) {
      my_sleep(static_cast<ulong>(preserve_trx_warmcopy_min_open_ms) * 1000);
    }

    Warmcopy_prepare_idle_participants prepare_warmcopy(m_owner);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&prepare_warmcopy);
    for (const Preserve_trx_pinned_thd &target : prepare_warmcopy.targets()) {
      if (target.thd == nullptr ||
          !preserve_trx_has_explicit_active_transaction(target.thd) ||
          preserve_trx_is_unsupported_common_context(target.thd)) {
        continue;
      }
      (void)m_provider->prepare_blob_for_thd_if_present(target.thd,
                                                        m_generation);
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
      mark_degraded("warm-copy close deadline expired before admission close");
      return false;
    }
    const unsigned long close_timeout_ms =
        warmcopy_close_timeout_ms_until_deadline(m_closing_deadline_us,
                                                 m_close_timeout_ms);
    if (m_admission_scope != nullptr &&
        !m_admission_scope->close(close_timeout_ms)) {
      mark_degraded("warm-copy admission close timed out");
      return false;
    }
    m_admission_scope.reset();

    DEBUG_SYNC(m_owner, "preserve_trx_warmcopy_after_admission_close_before_batch");
    if (m_provider->error()) {
      mark_degraded("warm-copy provider failed after admission close");
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
    if (m_admission_scope != nullptr) {
      const unsigned long close_timeout_ms =
          warmcopy_close_timeout_ms_until_deadline(m_closing_deadline_us,
                                                   m_close_timeout_ms);
      (void)m_admission_scope->close(close_timeout_ms);
      m_admission_scope.reset();
    }
    m_provider.reset();
    m_closed = false;
    m_closing_deadline_us = 0;
    m_observation.state = Preserve_trx_drain_participant_state::ABANDONED;
    m_observation.owns_artifact = false;
    if (m_observation.failure_reason.empty())
      m_observation.failure_reason = "aborted";
  }

  void finalize_phase() override {
    m_admission_scope.reset();
    m_observation.state = Preserve_trx_drain_participant_state::FINALIZED;
    m_observation.owns_artifact = false;
  }

  Preserve_trx_drain_participant_observation observation() const override {
    return m_observation;
  }

  PreserveBinlogBlobProvider *provider() const { return m_provider.get(); }

  ulonglong closing_deadline_us() const { return m_closing_deadline_us; }

  void set_closing_deadline_us(ulonglong close_deadline_us) {
    m_closing_deadline_us = close_deadline_us;
    if (m_provider != nullptr)
      m_provider->set_close_deadline_us(close_deadline_us);
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
    Warmcopy_prepare_quiesced_targets prepare_quiesced_targets(
        m_generation, target_thread_ids, m_provider.get(), m_generation,
        close_deadline_us);
    Preserve_batch_quiesced_target_pin_collector targets(
        m_owner, m_generation, target_thread_ids);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&targets);
    if (targets.error() || targets.targets().size() != target_thread_ids.size()) {
      mark_degraded("warm-copy quiesced target pin failed");
      return false;
    }
    for (const Preserve_trx_pinned_thd &target : targets.targets()) {
      prepare_quiesced_targets.prepare(target.thd);
      if (prepare_quiesced_targets.error()) break;
    }
    refresh_observation_from_provider(m_observation.phase1_progress);
    if (prepare_quiesced_targets.error() || m_provider->error()) {
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
  std::shared_ptr<Warmcopy_batch_blob_provider> m_provider;
  std::unique_ptr<Warmcopy_open_admission_scope> m_admission_scope;
  ulonglong m_closing_deadline_us{0};
  bool m_closed{false};
  Preserve_trx_drain_participant_observation m_observation;
};

void preserve_trx_warmcopy_admit_current_thd_binlog_write_impl(THD *thd) {
  if (thd == nullptr ||
      !g_warmcopy_admission_open.load(std::memory_order_acquire))
    return;

  std::shared_ptr<Warmcopy_batch_blob_provider> provider;
  uint64_t epoch = 0;
  my_thread_id owner_thread_id = 0;
  {
    std::lock_guard<std::mutex> guard(g_warmcopy_admission_mutex);
    provider = g_warmcopy_admission_provider;
    epoch = g_warmcopy_admission_epoch;
    owner_thread_id = g_warmcopy_admission_owner_thread_id;
    if (provider != nullptr && epoch != 0 &&
        thd->thread_id() != owner_thread_id) {
      warmcopy_admission_increment_inflight_locked(provider.get(), epoch);
    }
  }
  if (provider == nullptr || epoch == 0 || thd->thread_id() == owner_thread_id)
    return;

  Warmcopy_batch_blob_provider *provider_key = provider.get();
  auto admission_done = [provider_key, epoch] {
    std::lock_guard<std::mutex> guard(g_warmcopy_admission_mutex);
    warmcopy_admission_decrement_inflight_locked(provider_key, epoch);
    g_warmcopy_admission_cond.notify_all();
  };

  if (preserve_trx_is_unsupported_common_context(thd) ||
      !preserve_trx_has_explicit_active_transaction(thd)) {
    admission_done();
    return;
  }
  (void)provider->prepare_current_thd_if_needed(thd, epoch);
  admission_done();
}

}  // namespace

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

void preserve_trx_warmcopy_admit_current_thd_binlog_write(THD *thd) {
  preserve_trx_warmcopy_admit_current_thd_binlog_write_impl(thd);
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

const Preserved_trx_column_metadata *preserved_trx_columns(size_t *count) {
  *count = sizeof(kPreservedTrxColumns) / sizeof(kPreservedTrxColumns[0]);
  return kPreservedTrxColumns;
}

bool preserved_trx_resolve_timeout_seconds(const Preserve_trx_options &options,
                                           ulonglong default_timeout,
                                           ulonglong min_timeout,
                                           ulonglong max_timeout,
                                           ulonglong *timeout_seconds) {
  if (timeout_seconds == nullptr) return false;

  if (min_timeout == 0 || max_timeout == 0 || min_timeout > max_timeout)
    return false;
  if (default_timeout < min_timeout || default_timeout > max_timeout)
    return false;

  const ulonglong effective_timeout =
      options.has_timeout ? options.timeout_seconds : default_timeout;
  if (effective_timeout < min_timeout || effective_timeout > max_timeout)
    return false;

  *timeout_seconds = effective_timeout;
  return true;
}

const char *preserved_trx_dir_value() {
  static const std::string dir = preserve_trx_default_dir();
  return dir.c_str();
}

void preserved_trx_mark_recovery_complete() {
  mark_preserved_trx_recovery_complete();
}

bool preserved_trx_validate_snapshot_support(bool allow_create_missing) {
  const std::string dir = preserve_trx_default_dir();
  return preserved_trx_default_carrier_support_is_valid(dir,
                                                        allow_create_missing);
}

bool preserved_trx_ensure_snapshot_support() {
  return preserved_trx_validate_snapshot_support(true);
}

Preserve_trx_manager_state preserved_trx_manager_state() {
  return preserve_trx_manager_state_owner_snapshot().state;
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

void preserved_trx_note_statement_response(THD *thd) {
  note_pending_token_delivery_statement_response(thd);
}

bool preserved_trx_defer_shutdown_signal() {
  if (!preserved_trx_shutdown_requested()) return false;

  std::lock_guard<std::mutex> lock(g_token_delivery_mutex);
  if (g_pending_token_delivery.empty()) return false;

  g_deferred_shutdown_signal_requested.store(true);
  return true;
}

static void preserved_trx_resume_deferred_shutdown_signal() {
  if (g_deferred_shutdown_signal_requested.exchange(false)) {
    kill_mysql();
  }
}

static bool preserve_trx_wait_target_timed_out(THD *thd,
                                              uint64_t *hard_deadline_us,
                                              uint *quiesced_wait_loops) {
  if (thd == nullptr || preserve_trx_drain_hard_timeout_ms == 0) return false;
  if (*hard_deadline_us == 0) {
    *hard_deadline_us = preserve_trx_monotonic_deadline_after_ms(
        preserve_trx_monotonic_us(), preserve_trx_drain_hard_timeout_ms);
  }
  if (preserve_trx_monotonic_deadline_expired_at(
          *hard_deadline_us, preserve_trx_monotonic_us())) {
    my_error(ER_PRESERVE_TRX_DRAIN_TIMEOUT, MYF(0));
    return true;
  }

  preserve_trx_note_quiesced_wait(quiesced_wait_loops);
  my_sleep(10000);
  return false;
}

bool preserved_trx_begin_command_read(THD *thd) {
  if (thd == nullptr) return false;

  uint64_t hard_deadline_us = 0;
  uint quiesced_wait_loops = 0;
  for (;;) {
    mysql_mutex_lock(&thd->LOCK_thd_data);
    thd->preserve_trx_inflight_risky_statement_depth = 0;
    thd->preserve_trx_inflight_unknown_query_depth = 0;
    assert(thd->preserve_trx_inflight_risky_statement_depth == 0);
    assert(thd->preserve_trx_inflight_unknown_query_depth == 0);
    if (thd->preserve_trx_batch_state ==
        Preserve_trx_batch_thd_state::PENDING_QUIESCE) {
      thd->m_server_idle = true;
      (void)preserve_trx_publish_pending_quiesce_at_idle_boundary(thd);
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
    if (preserve_trx_wait_target_timed_out(thd, &hard_deadline_us,
                                          &quiesced_wait_loops)) {
      return false;
    }
  }
}

bool preserved_trx_command_read_is_idle(THD *thd) {
  if (thd == nullptr) return false;

  uint64_t hard_deadline_us = 0;
  uint quiesced_wait_loops = 0;
  for (;;) {
    mysql_mutex_lock(&thd->LOCK_thd_data);
    if (!preserve_trx_batch_state_blocks_target_owner(
            thd->preserve_trx_batch_state)) {
      const bool was_idle = thd->m_server_idle;
      mysql_mutex_unlock(&thd->LOCK_thd_data);
      return was_idle;
    }
    mysql_mutex_unlock(&thd->LOCK_thd_data);
    if (preserve_trx_wait_target_timed_out(thd, &hard_deadline_us,
                                          &quiesced_wait_loops)) {
      return false;
    }
  }
}

bool preserved_trx_end_idle_for_command_packet(THD *thd) {
  if (thd == nullptr) return false;

  uint64_t hard_deadline_us = 0;
  uint quiesced_wait_loops = 0;
  for (;;) {
    mysql_mutex_lock(&thd->LOCK_thd_data);
    if (!preserve_trx_batch_state_blocks_target_owner(
            thd->preserve_trx_batch_state)) {
      const bool was_idle = thd->m_server_idle;
      if (was_idle) thd->m_server_idle = false;
      mysql_mutex_unlock(&thd->LOCK_thd_data);
      return was_idle;
    }
    mysql_mutex_unlock(&thd->LOCK_thd_data);
    if (preserve_trx_wait_target_timed_out(thd, &hard_deadline_us,
                                          &quiesced_wait_loops)) {
      return false;
    }
  }
}

bool preserved_trx_end_command_read(THD *thd) {
  if (thd == nullptr) return false;

  uint64_t hard_deadline_us = 0;
  uint quiesced_wait_loops = 0;
  for (;;) {
    mysql_mutex_lock(&thd->LOCK_thd_data);
    if (!preserve_trx_batch_state_blocks_target_owner(
            thd->preserve_trx_batch_state)) {
      thd->m_server_idle = false;
      mysql_mutex_unlock(&thd->LOCK_thd_data);
      return true;
    }
    mysql_mutex_unlock(&thd->LOCK_thd_data);
    if (preserve_trx_wait_target_timed_out(thd, &hard_deadline_us,
                                          &quiesced_wait_loops)) {
      return false;
    }
  }
}

bool preserved_trx_wait_if_batch_session_quiesced(THD *thd) {
  if (thd == nullptr) return false;

  uint64_t hard_deadline_us = 0;
  uint quiesced_wait_loops = 0;
  while (preserve_trx_batch_state_blocks_target_owner(
      preserve_trx_batch_state(thd))) {
    if (preserve_trx_wait_target_timed_out(thd, &hard_deadline_us,
                                          &quiesced_wait_loops)) {
      return true;
    }
  }

  return false;
}

bool preserved_trx_reject_if_batch_session_drained(THD *thd) {
  if (thd == nullptr) return false;

  if (preserve_trx_batch_state(thd) ==
      Preserve_trx_batch_thd_state::PRESERVED_DRAINED) {
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

  if (preserve_trx_batch_state(thd) ==
          Preserve_trx_batch_thd_state::PENDING_QUIESCE &&
      preserve_trx_thd_has_batch_inflight_statement(thd))
    return Preserve_trx_command_block_result::ALLOW;

  const my_thread_id owner_thread_id = manager_snapshot.owner_thread_id;
  if (owner_thread_id != 0 && thd->thread_id() == owner_thread_id)
    return Preserve_trx_command_block_result::ALLOW;

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

  if (state == Preserve_trx_manager_state::WARMCOPY_CLOSING &&
      preserve_trx_sql_command_may_create_trx_or_lock(thd, sql_command)) {
    return Preserve_trx_command_block_result::BLOCK_DRAINING;
  }

  if (preserve_trx_sql_command_may_create_trx_or_lock(thd, sql_command)) {
    return Preserve_trx_command_block_result::BLOCK_DRAINING;
  }

  return Preserve_trx_command_block_result::ALLOW;
}

Preserve_trx_command_block_result preserved_trx_protocol_command_block_result(
    THD *thd, enum enum_server_command command) {
  if (thd == nullptr) return Preserve_trx_command_block_result::ALLOW;
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

  if (preserve_trx_batch_state(thd) ==
          Preserve_trx_batch_thd_state::PENDING_QUIESCE &&
      preserve_trx_thd_has_batch_inflight_statement(thd))
    return Preserve_trx_command_block_result::ALLOW;

  const my_thread_id owner_thread_id = manager_snapshot.owner_thread_id;
  if (owner_thread_id != 0 && thd->thread_id() == owner_thread_id)
    return Preserve_trx_command_block_result::ALLOW;

  if (state == Preserve_trx_manager_state::WARMCOPY_DRAINING) {
    return Preserve_trx_command_block_result::ALLOW;
  }

  if (state == Preserve_trx_manager_state::WARMCOPY_CLOSING &&
      preserve_trx_protocol_command_may_create_trx_or_lock(command)) {
    return Preserve_trx_command_block_result::BLOCK_DRAINING;
  }

  if (preserve_trx_protocol_command_may_create_trx_or_lock(command)) {
    return Preserve_trx_command_block_result::BLOCK_DRAINING;
  }

  return Preserve_trx_command_block_result::ALLOW;
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
  if (!preserve_trx_has_explicit_active_transaction(thd) &&
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

  mysql_mutex_lock(&thd->LOCK_thd_data);
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

static bool delete_snapshot_files(const std::string &dir_arg,
                                  const std::string &token) {
  return delete_snapshot_files_with_status(dir_arg, token) !=
         Preserve_snapshot_delete_status::OK;
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

bool preserved_trx_recovery_read_failure_requires_startup_abort_for_unit_test(
    Preserve_snapshot_status status) {
  return preserved_trx_recovery_read_failure_requires_startup_abort(status);
}

bool preserved_trx_preflight_read_failure_requires_startup_abort_for_unit_test(
    Preserve_snapshot_status status) {
  return preserved_trx_preflight_read_failure_requires_startup_abort(status);
}

static bool delete_preserved_snapshot_files_or_log(const std::string &dir,
                                                   const std::string &token) {
  if (!delete_snapshot_files(dir, token)) return false;
  return log_preserved_trx_cleanup_failure(token,
                                           "failed to delete snapshot files");
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
                                          const std::string &token) {
  const Preserve_snapshot_status status = store.remove_stale_tmp_files(token);
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

static bool delete_preserved_snapshot_files_and_sidecars_or_log(
    const std::string &dir, const std::string &token,
    const Preserve_snapshot_metadata *metadata) {
  if (delete_preserved_snapshot_files_or_log(dir, token)) return true;

  if (metadata == nullptr) return false;
  release_temp_sidecar_space_id_reservations(*metadata);
  if (metadata->temp_table_manifest_payload.empty()) return false;

  return delete_preserved_temp_table_sidecars_or_log(dir, token, *metadata);
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

    const std::string message =
        "PRESERVE: failed to delete orphan preserved temporary table sidecars "
        "for token '" +
        preserved_trx_redacted_token(token) + "' during recovery, status " +
        std::to_string(static_cast<int>(status));
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    failed = true;
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
    const Preserve_snapshot_metadata *metadata = nullptr) {
  const std::string message =
      redacted_preserved_trx_log_subject(token) +
      " recovery forced rollback: " + reason;
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());

  XID xid;
  if (preserve_trx_token_to_xid(token, &xid)) {
    return log_preserved_trx_cleanup_failure(
        token, "failed to map durable transaction token to XID");
  }

  trx_t *trx = trx_preserve_claim_prepared(xid);
  if (trx != nullptr && trx_preserve_rollback_claimed(trx) != DB_SUCCESS) {
    return log_preserved_trx_cleanup_failure(
        token, "failed to rollback durable transaction");
  }

  audit_preserved_trx_event(
      current_thd, token, "rollback",
      reason.find("timeout") != std::string::npos ? "timeout" : "failure",
      reason.c_str());
  delete_detached_mdl_context(token);
  return delete_preserved_snapshot_files_and_sidecars_or_log(dir, token,
                                                             metadata);
}

static bool rollback_claimed_preserved_snapshot_or_log(
    const std::string &dir, const std::string &token, trx_t *trx,
    const std::string &reason,
    const Preserve_snapshot_metadata *metadata = nullptr) {
  const std::string message =
      redacted_preserved_trx_log_subject(token) +
      " recovery forced rollback: " + reason;
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());

  if (trx_preserve_rollback_claimed(trx) != DB_SUCCESS) {
    return log_preserved_trx_cleanup_failure(
        token, "failed to rollback durable transaction");
  }

  audit_preserved_trx_event(
      current_thd, token, "rollback",
      reason.find("timeout") != std::string::npos ? "timeout" : "failure",
      reason.c_str());
  delete_detached_mdl_context(token);
  return delete_preserved_snapshot_files_and_sidecars_or_log(dir, token,
                                                             metadata);
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
        store->mark_tainted(token) != Preserve_snapshot_status::OK;
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

  (void)preserved_trx_gc_failed_observable_records();

  constexpr uint kMaxExpiredRecordsPerPass = 16;
  for (uint i = 0; i < kMaxExpiredRecordsPerPass; ++i) {
    Preserved_trx_record record;
    if (!preserved_trx_claim_expired_resumable_record_for_reaper(&record)) break;
    (void)rollback_expired_resumable_record_by_reaper(record);
  }
}

static void preserved_trx_expired_reaper_thread() {
  my_thread_init();

  std::unique_lock<std::mutex> lock(g_preserved_trx_reaper_mutex);
  while (!g_preserved_trx_reaper_stop) {
    g_preserved_trx_reaper_cond.wait_for(lock, std::chrono::seconds(1));
    if (g_preserved_trx_reaper_stop) break;
    lock.unlock();
    preserved_trx_expired_reaper_scan_once();
    lock.lock();
  }

  lock.unlock();
  my_thread_end();
}

void preserved_trx_start_expired_reaper() {
  DBUG_EXECUTE_IF("preserve_trx_expired_reaper_skip", return;);
  std::lock_guard<std::mutex> lock(g_preserved_trx_reaper_mutex);
  if (g_preserved_trx_reaper_started) return;
  g_preserved_trx_reaper_stop = false;
  g_preserved_trx_reaper_thread =
      std::thread(preserved_trx_expired_reaper_thread);
  g_preserved_trx_reaper_started = true;
}

void preserved_trx_start_expired_reaper_if_ready() {
  if (!preserve_trx_is_enabled()) return;
  {
    std::lock_guard<std::mutex> lock(g_preserved_trx_mutex);
    if (!g_preserved_trx_recovery_done) return;
  }
  preserved_trx_start_expired_reaper();
}

void preserved_trx_stop_expired_reaper() {
  std::thread reaper;
  {
    std::lock_guard<std::mutex> lock(g_preserved_trx_reaper_mutex);
    if (!g_preserved_trx_reaper_started) return;
    g_preserved_trx_reaper_stop = true;
    reaper = std::move(g_preserved_trx_reaper_thread);
    g_preserved_trx_reaper_started = false;
  }
  g_preserved_trx_reaper_cond.notify_all();
  if (reaper.joinable()) reaper.join();
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
        &context, preserve_mdl_key_data(metadata.token), metadata.token.length());
  }

  context.release_transactional_locks();
  context.destroy();
  return error;
}

static bool create_detached_mdl_context(THD *thd, const std::string &token) {
  return MDL_context_backup_manager::instance().create_backup(
      &thd->mdl_context, preserve_mdl_key_data(token), token.length());
}

static bool restore_detached_mdl_context(THD *thd, const std::string &token) {
  DBUG_EXECUTE_IF("preserve_trx_fail_resume_transfer_mdl", return true;);
  return MDL_context_backup_manager::instance().restore_backup(
      &thd->mdl_context, preserve_mdl_key_data(token), token.length());
}

static void delete_detached_mdl_context(const std::string &token) {
  if (!token_is_filename_safe(token)) return;
  MDL_context_backup_manager::instance().delete_backup(
      preserve_mdl_key_data(token), token.length());
}

static void rollback_pending_token_delivery_record(const std::string &token) {
  Preserved_trx_record record;
  if (!preserved_trx_take_record(token, &record)) return;

  dberr_t rollback_status = DB_SUCCESS;
  DBUG_EXECUTE_IF("preserve_trx_fail_token_delivery_rollback",
                  rollback_status = DB_ERROR;);
  if (rollback_status == DB_SUCCESS)
    rollback_status = trx_preserve_rollback_claimed(record.trx);
  if (rollback_status != DB_SUCCESS) {
    const char rollback_error[] = "failed to rollback durable transaction";
    if (preserved_trx_add_record_with_error(record, rollback_error)) {
      log_redacted_token_delivery_cleanup_failure(
          token, "failed to restore record after rollback failure");
    }
    log_redacted_token_delivery_cleanup_failure(
        token,
        rollback_error);
    audit_preserved_trx_event(current_thd, token, "rollback", "failure",
                              rollback_error);
    return;
  }

  audit_preserved_trx_event(current_thd, token, "rollback", "failure",
                            "token delivery failed");
  delete_detached_mdl_context(token);
  if (delete_preserved_snapshot_files_and_sidecars_or_log(
          preserve_trx_default_dir(), token, &record.metadata))
    log_redacted_token_delivery_cleanup_failure(
        token,
        "failed to delete snapshot files after rollback");
}

static bool restore_batch_record_to_original_thd(
    THD *thd, const Preserve_trx_batch_item &item) {
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
    record.state = Preserved_trx_lifecycle_state::PRESERVED;
    record.resumable = true;
    (void)restore_record_after_resume_failure(record, reason);
    return true;
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

  thd->variables.sql_log_bin = record.metadata.session_sql_log_bin;
  if (record.metadata.option_bin_log)
    thd->variables.option_bits |= OPTION_BIN_LOG;
  else
    thd->variables.option_bits &= ~OPTION_BIN_LOG;
  thd->variables.option_bits |= OPTION_BEGIN;
  thd->server_status |= SERVER_STATUS_IN_TRANS;
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
    if (hydrate_logged_binlog_cache_payload_if_needed(&record, item.token))
      return restore_record_after_failure(
          "batch cleanup binlog cache read failure");
    Mysql_binlog_preserve_snapshot binlog_snapshot =
        metadata_to_binlog_cache_snapshot(record.metadata);
    discard_binlog_preserve_cache_and_reset_scopes(thd);
    if (mysql_binlog_preserve_import(thd, binlog_snapshot)) {
      return restore_record_after_failure(
          "batch cleanup binlog cache import failure");
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

  if (restore_savepoints_to_thd(thd, record.trx, record.metadata)) {
    return restore_record_after_failure("batch cleanup savepoint restore failure");
  }

  reset_preserve_statement_transaction_scope(thd);

  if (trx_preserve_activate_reattached_in_original_thd(record.trx, thd) !=
      DB_SUCCESS) {
    return restore_record_after_failure("batch cleanup undo activation failure");
  }

  const Preserve_snapshot_delete_status delete_status =
      delete_snapshot_files_with_status(preserve_trx_default_dir(), item.token);
  if (delete_status ==
      Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE) {
    log_preserved_trx_cleanup_failure(item.token,
                                      "failed to delete snapshot files");
    auto store = create_preserved_trx_default_store(preserve_trx_default_dir());
    if (store->mark_tainted(item.token) != Preserve_snapshot_status::OK) {
      return restore_record_after_failure("failed to mark stale snapshot tainted");
    }
    delete_detached_mdl_context(item.token);
    preserve_trx_set_batch_state(thd, 0, Preserve_trx_batch_thd_state::NONE);
    thd_state_guard.dismiss();
    return true;
  }
  const bool cleanup_failed_after_unlink =
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

class Preserve_batch_reattach_item final : public Do_THD_Impl {
 public:
  Preserve_batch_reattach_item(ulonglong generation,
                               const Preserve_trx_batch_item &item)
      : m_generation(generation), m_item(item) {}

  void operator()(THD *candidate) override {
    if (m_visited || candidate == nullptr) return;

    mysql_mutex_lock(&candidate->LOCK_thd_data);
    const bool target =
        candidate->thread_id() == m_item.original_thread_id &&
        candidate->preserve_trx_batch_generation == m_generation &&
        candidate->preserve_trx_batch_state ==
            Preserve_trx_batch_thd_state::PRESERVED_DRAINED;
    mysql_mutex_unlock(&candidate->LOCK_thd_data);

    if (!target) return;

    m_visited = true;
    if (restore_batch_record_to_original_thd(candidate, m_item)) {
      m_error = true;
      return;
    }

    preserve_trx_set_batch_state(candidate, 0,
                                 Preserve_trx_batch_thd_state::NONE);
  }

  bool visited() const { return m_visited; }
  bool error() const { return m_error; }

 private:
  ulonglong m_generation;
  const Preserve_trx_batch_item &m_item;
  bool m_visited{false};
  bool m_error{false};
};

static bool restore_preserved_batch_items_to_original_thds(
    ulonglong generation, const std::vector<Preserve_trx_batch_item> &items) {
  for (auto it = items.rbegin(); it != items.rend(); ++it) {
    Preserve_batch_reattach_item reattach(generation, *it);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&reattach);
    if (!reattach.visited() || reattach.error()) return true;
  }

  Preserve_batch_clear_generation clear(generation);
  Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
  return false;
}

static bool reattach_current_batch_preserve_failure_to_original_thd(
    THD *thd, trx_t *trx, const std::string &token, bool detached_mdl_context,
    bool snapshot_files_may_exist, bool *cleanup_failed, bool *left_preserved,
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
        if (!snapshot_files_may_exist || metadata == nullptr) return true;

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
    if (!snapshot_files_may_exist) return Preserve_snapshot_delete_status::OK;
    const Preserve_snapshot_delete_status delete_status =
        delete_snapshot_files_with_status(preserve_trx_default_dir(), token);
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
    if (delete_status !=
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

  if (trx_preserve_activate_reattached_in_original_thd(trx, thd) !=
      DB_SUCCESS) {
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
    if (store->mark_tainted(token) != Preserve_snapshot_status::OK) {
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

static bool reactivate_current_batch_prepared_failure_to_original_thd(THD *thd) {
  if (trx_preserve_reactivate_prepare_failure_in_original_thd(thd) !=
      DB_SUCCESS)
    return true;

  reset_preserve_xid_to_active_transaction_xid(thd);
  reset_preserve_statement_transaction_scope(thd);
  return false;
}

void preserved_trx_finalize_statement_response(THD *thd) {
  if (!preserved_trx_has_pending_token_delivery(thd)) return;

  DEBUG_SYNC(thd, "preserve_trx_finalize_token_delivery");

  std::string token;
  bool ok_delivered = false;
  if (!preserved_trx_begin_pending_token_delivery_finalization(
          thd, &token, &ok_delivered)) {
    return;
  }

  if (ok_delivered) {
    if (preserved_trx_mark_resumable(token)) {
      log_redacted_token_delivery_cleanup_failure(
          token,
          "failed to mark token delivery complete");
      rollback_pending_token_delivery_record(token);
      preserved_trx_erase_pending_token_delivery(thd);
      preserve_trx_store_manager_state_owner(Preserve_trx_manager_state::IDLE,
                                             0);
      preserved_trx_resume_deferred_shutdown_signal();
      return;
    }
    audit_preserved_trx_event(thd, token, "preserve", "success");
    preserved_trx_erase_pending_token_delivery(thd);
    g_deferred_shutdown_signal_requested.store(false);
    kill_mysql();
    return;
  }

  rollback_pending_token_delivery_record(token);
  preserved_trx_erase_pending_token_delivery(thd);
  preserve_trx_store_manager_state_owner(Preserve_trx_manager_state::IDLE, 0);
  preserved_trx_resume_deferred_shutdown_signal();
}

void preserved_trx_release_resources(THD *thd) {
  DEBUG_SYNC(thd, "preserve_trx_release_resources_before_finalize");
  DBUG_EXECUTE_IF("preserve_trx_trace_release_resources_pending", {
    if (preserved_trx_has_pending_token_delivery(thd)) {
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             "PRESERVE: release_resources is finalizing pending token "
             "delivery");
    }
  });
  preserved_trx_finalize_statement_response(thd);
}

void preserved_trx_wait_for_external_thd_use(THD *thd) {
  if (thd == nullptr) return;
  std::unique_lock<std::mutex> lock(g_preserved_trx_thd_pin_mutex);
  g_preserved_trx_thd_pin_cond.wait(lock, [thd] {
    return g_preserved_trx_thd_pin_counts.find(thd) ==
           g_preserved_trx_thd_pin_counts.end();
  });
}

bool preserved_trx_thd_has_external_use(THD *thd) {
  if (thd == nullptr) return false;
  std::lock_guard<std::mutex> lock(g_preserved_trx_thd_pin_mutex);
  return g_preserved_trx_thd_pin_counts.find(thd) !=
         g_preserved_trx_thd_pin_counts.end();
}

static bool recover_preserved_snapshot(const std::string &dir,
                                       const std::string &token,
                                       uint64_t recovery_anchor_wall_us,
                                       uint64_t recovery_anchor_monotonic_us) {
  auto store = create_preserved_trx_default_store(dir);
  Preserved_trx_bundle bundle;
  Preserve_snapshot_status status =
      store->read(token, true,
                  Preserved_trx_carrier::Payload_read_mode::METADATA_ONLY,
                  &bundle);
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
  Preserve_snapshot_metadata metadata = std::move(bundle.metadata);

  if (!metadata.temp_table_manifest_payload.empty()) {
    std::string reason;
    const Preserve_snapshot_status temp_sidecar_status =
        preserve_trx_temp_table_validate_sidecars(dir, token, metadata,
                                                  &reason);
    if (temp_sidecar_status != Preserve_snapshot_status::OK) {
      const std::string failure_reason =
          reason.empty() ? "invalid preserved temporary table sidecars" : reason;
      return rollback_preserved_snapshot_or_log(dir, token, failure_reason,
                                                &metadata);
    }
  }

  if (!recoverable_binlog_state(metadata.binlog_state)) {
    return rollback_preserved_snapshot_or_log(
        dir, token, "unsupported durable transaction binlog state", &metadata);
  }

  if (!binlog_state_matches_configured_mode(metadata)) {
    log_preserved_trx_rejected_binlog_mode(token, metadata);
    return rollback_preserved_snapshot_or_log(dir, token,
                                              "binlog mode mismatch",
                                              &metadata);
  }

  if (metadata.recovered_count == UINT32_MAX) {
    return rollback_preserved_snapshot_or_log(
        dir, token, "durable transaction recovery count overflow");
  }
  /*
    Evaluate the timeout before bumping recovered_count. The first recovery
    pass is the only one that receives preserve_trx_recovery_grace_seconds;
    later passes must use the original wall-clock expiry exactly as stored.
  */
  const bool recovery_deadline_expired =
      preserve_trx_recovery_deadline_expired(metadata, recovery_anchor_wall_us,
                                             recovery_anchor_monotonic_us);
  ++metadata.recovered_count;
  if (metadata.recovered_count >= preserve_trx_recovery_max_count) {
    preserved_trx_add_failed_observable_record(
        metadata, "recovery max count exceeded");
    return rollback_preserved_snapshot_or_log(dir, token,
                                              "recovery max count exceeded",
                                              &metadata);
  }
  if (recovery_deadline_expired) {
    preserved_trx_add_failed_observable_record(metadata,
                                               "recovery timeout expired");
    return rollback_preserved_snapshot_or_log(dir, token,
                                              "recovery timeout expired",
                                              &metadata);
  }

  XID xid;
  if (preserve_trx_token_to_xid(token, &xid)) {
    return log_preserved_trx_recovery_failure(
        token, "failed to map durable transaction token to XID");
  }

  [[maybe_unused]] auto fail_closed_without_claim = [&](const char *reason) {
    if (store->mark_tainted(token) != Preserve_snapshot_status::OK) {
      return log_preserved_trx_recovery_failure(
          token, "failed to mark snapshot tainted after recovery failure");
    }
    preserved_trx_add_failed_observable_record(metadata, reason);
    (void)log_preserved_trx_recovery_failure(token, reason);
    return false;
  };

  DBUG_EXECUTE_IF("preserve_trx_simulate_encrypted_tablespace_key_unavailable",
                  return fail_closed_without_claim(
                      "encrypted tablespace key unavailable"););

  trx_t *trx = trx_preserve_claim_prepared(xid);
  if (trx == nullptr && !metadata.temp_table_manifest_payload.empty()) {
    const uint64_t temp_owner_trx_id =
        preserve_trx_temp_table_owner_trx_id(metadata);
    if (temp_owner_trx_id != 0) {
      trx = trx_preserve_create_temp_only_claimed(xid, temp_owner_trx_id);
    }
  }
  if (trx == nullptr) {
    return delete_preserved_snapshot_files_and_sidecars_or_log(dir, token,
                                                               &metadata);
  }

  bool fail_recovered_count_rewrite = false;
  DBUG_EXECUTE_IF("preserve_trx_fail_recovered_count_rewrite", {
    fail_recovered_count_rewrite = true;
  });
  const Preserve_snapshot_status recovered_count_rewrite_status =
      fail_recovered_count_rewrite
          ? Preserve_snapshot_status::IO_ERROR
          : store->rewrite_recovered_count(token, metadata.recovered_count);
  if (recovered_count_rewrite_status != Preserve_snapshot_status::OK) {
    return rollback_claimed_preserved_snapshot_or_log(
        dir, token, trx, "failed to update durable transaction recovery count",
        &metadata);
  }

  const auto rollback_semantics_failure = [&](const char *component) {
    std::string reason =
        std::string("failed to restore durable transaction semantics: ") +
        component;
    if (strcmp(component, "record locks") == 0) {
      const char *detail = trx_preserve_last_record_lock_export_error();
      if (detail != nullptr && detail[0] != '\0') {
        reason.append(": ");
        reason.append(detail);
      }
    }
    return rollback_claimed_preserved_snapshot_or_log(
        dir, token, trx, reason, &metadata);
  };
  if (trx_preserve_set_isolation(trx, metadata.tx_isolation) != DB_SUCCESS) {
    return rollback_semantics_failure("isolation level");
  }
  if (trx_preserve_import_read_view(trx, metadata.read_view_payload) !=
      DB_SUCCESS) {
    return rollback_semantics_failure("read view");
  }
  if (trx_preserve_import_table_locks(trx, metadata.table_locks_payload) !=
      DB_SUCCESS) {
    return rollback_semantics_failure("table locks");
  }
  if (trx_preserve_import_record_locks(trx, metadata.record_locks_payload) !=
      DB_SUCCESS) {
    return rollback_semantics_failure("record locks");
  }
  if (trx_preserve_import_record_locks(trx, metadata.predicate_locks_payload) !=
      DB_SUCCESS) {
    return rollback_claimed_preserved_snapshot_or_log(
        dir, token, trx,
        "failed to restore durable transaction semantics: predicate locks",
        &metadata);
  }

  if (create_detached_mdl_context(metadata)) {
    return rollback_claimed_preserved_snapshot_or_log(
        dir, token, trx, "failed to restore durable transaction MDL context",
        &metadata);
  }

  DBUG_EXECUTE_IF(
      "preserve_trx_crash_after_recover_import_before_register", {
        LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
               "Preserved transaction recovery reached post-import "
               "pre-register crash point");
        DBUG_SUICIDE();
      });

  if (preserved_trx_add_record(metadata, trx, true,
                               Preserved_trx_lifecycle_state::PRESERVED,
                               std::move(bundle.blob_descriptors))) {
    delete_detached_mdl_context(token);
    return rollback_claimed_preserved_snapshot_or_log(
        dir, token, trx, "failed to register recovered durable transaction",
        &metadata);
  }

  audit_preserved_trx_event(current_thd, token, "recover", "success");
  return false;
}

bool preserved_trx_preflight_recoverability() {
  if (srv_force_recovery > 0) return false;

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

  for (const std::string &token : listing.snapshot_tokens) {
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

bool preserved_temp_images_bootstrap_preamble() {
  if (srv_force_recovery > 0) return false;

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

  if (delete_orphan_temp_table_sidecars_or_log(store.store(), listing))
    return true;

  auto taint_bootstrap_token_or_warn = [&](const std::string &token,
                                           const std::string &reason) {
    log_preserved_trx_recovery_warning(token, reason);
    if (store->mark_tainted(token) != Preserve_snapshot_status::OK) {
      (void)log_preserved_trx_cleanup_failure(
          token, "failed to mark snapshot tainted during temporary tablespace "
                 "bootstrap");
    }
  };

  for (const std::string &token : listing.snapshot_tokens) {
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
      if (!trx_preserve_temp_space_image_reserve_space_id(descriptor)) {
        reserve_failed = true;
        break;
      }
      reserved_space_ids.push_back(descriptor.source_space_id);
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
  }

  return false;
}

bool preserved_trx_recover_all() {
  const std::string dir = normalize_dir(preserve_trx_default_dir());
  auto store = create_preserved_trx_default_store(dir);
  const uint64_t recovery_anchor_wall_us = my_micro_time();
  const uint64_t recovery_anchor_monotonic_us = preserve_trx_monotonic_us();
  DBUG_EXECUTE_IF("preserve_trx_fail_recover_scan", {
    const std::string message =
        "Failed to scan preserved transaction directory '" + dir +
        "' during recovery, error " + std::to_string(EIO);
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    preserved_trx_mark_recovery_complete();
    return true;
  });

  Preserved_trx_carrier_listing listing;
  if (store->list_tokens(&listing) != Preserve_snapshot_status::OK) {
    const std::string message =
        "Failed to scan preserved transaction directory '" + dir +
        "' during recovery";
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    preserved_trx_mark_recovery_complete();
    return true;
  }

  std::set<std::string> snapshot_tokens = listing.snapshot_tokens;
  std::set<std::string> binlog_cache_tokens = listing.external_blob_tokens;
  std::set<std::string> tainted_tokens = listing.tainted_tokens;
  std::set<std::string> warm_external_blob_artifacts =
      listing.warm_external_blob_artifacts;

  if (srv_force_recovery > 0) {
    if (!snapshot_tokens.empty() || !binlog_cache_tokens.empty()) {
      std::set<std::string> preserved_tokens(snapshot_tokens);
      preserved_tokens.insert(binlog_cache_tokens.begin(),
                              binlog_cache_tokens.end());
      bool taint_error = false;
      for (const std::string &token : preserved_tokens) {
        if (store->mark_tainted(token) != Preserve_snapshot_status::OK) {
          taint_error = true;
          LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                 "PRESERVE: failed to mark preserved transaction snapshot "
                 "tainted while innodb_force_recovery is enabled");
        }
      }
      if (taint_error) {
        preserved_trx_mark_recovery_complete();
        return true;
      }
      LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
             "PRESERVE: preserved transaction recovery is not supported while "
             "innodb_force_recovery is enabled; snapshot and binlog-cache "
             "files are retained and marked tainted");
    }
    preserved_trx_mark_recovery_complete();
    return false;
  }

  std::vector<std::string> retained_snapshot_tokens(snapshot_tokens.begin(),
                                                    snapshot_tokens.end());
  uint32_t orphan_rollback_count = 0;
  if (trx_preserve_rollback_prepared_without_snapshot(
          retained_snapshot_tokens, &orphan_rollback_count) != DB_SUCCESS) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           "PRESERVE: failed to rollback prepared transaction without "
           "snapshot during recovery");
    preserved_trx_mark_recovery_complete();
    return true;
  }
  if (orphan_rollback_count != 0) {
    LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
           "PRESERVE: rolled back prepared transaction without snapshot "
           "during recovery");
  }

  bool error = false;
  for (const std::string &artifact_filename : warm_external_blob_artifacts) {
    if (delete_orphan_warm_external_blob_artifact_or_log(store.store(),
                                                         artifact_filename)) {
      error = true;
    }
  }
  for (const std::string &token : snapshot_tokens) {
    if (delete_stale_tmp_files_or_log(store.store(), token)) {
      error = true;
      continue;
    }
    if (tainted_tokens.find(token) != tainted_tokens.end()) {
      Preserved_trx_bundle cleanup_bundle;
      Preserve_snapshot_metadata *cleanup_metadata = nullptr;
      if (store->read(token, true,
                      Preserved_trx_carrier::Payload_read_mode::METADATA_ONLY,
                      &cleanup_bundle) == Preserve_snapshot_status::OK) {
        cleanup_metadata = &cleanup_bundle.metadata;
      }
      if (rollback_preserved_snapshot_or_log(
              dir, token,
              "preserved transaction snapshot tainted by innodb_force_recovery",
              cleanup_metadata))
        error = true;
      continue;
    }
    if (recover_preserved_snapshot(dir, token, recovery_anchor_wall_us,
                                   recovery_anchor_monotonic_us))
      error = true;
  }

  for (const std::string &token : binlog_cache_tokens) {
    if (snapshot_tokens.find(token) != snapshot_tokens.end()) continue;
    if (delete_orphan_binlog_cache_or_log(dir, token)) error = true;
  }
  for (const std::string &token : tainted_tokens) {
    if (snapshot_tokens.find(token) != snapshot_tokens.end()) continue;
    if (store->remove_taint(token) != Preserve_snapshot_status::OK) {
      error = true;
    }
  }

  preserved_trx_mark_recovery_complete();
  return error;
}

bool preserve_trx_kernel_preserve_attached_transaction(
    const Preserve_trx_kernel_request &request) {
  DBUG_TRACE;

  THD *target_thd = request.target_thd;
  const Preserve_trx_options &options = request.options;
  const ulonglong timeout_seconds = request.timeout_seconds;
  const Preserve_trx_delivery_mode delivery_mode = request.delivery_mode;
  Preserve_trx_preserve_result *result = request.result;
  PreserveBinlogBlobProvider *binlog_blob_provider =
      request.binlog_blob_provider;

  if (result != nullptr) *result = Preserve_trx_preserve_result{};
  auto set_stage = [result](Preserve_trx_preserve_stage stage) {
    if (result != nullptr) result->stage = stage;
  };
  auto set_failure_reason = [result](const char *reason) {
    if (result != nullptr && result->failure_reason == nullptr)
      result->failure_reason = reason;
  };

  THD *thd = target_thd;
  if (thd == nullptr) {
    set_failure_reason("null_target_thd");
    return preserve_trx_reject_unsupported();
  }

  const bool batch_delivery =
      delivery_mode == Preserve_trx_delivery_mode::BATCH_MANAGER_DELIVERY;
  auto reject_unsupported_for_delivery = [&](const char *reason = "unsupported") {
    set_failure_reason(reason);
    if (batch_delivery) {
      if (thd != nullptr && thd->is_error()) thd->clear_error();
      return true;
    }
    return preserve_trx_reject_unsupported();
  };

  if (timeout_seconds == 0)
    return reject_unsupported_for_delivery("timeout_zero");

  set_stage(Preserve_trx_preserve_stage::BINLOG_PREFLIGHT);
  Preserve_snapshot_binlog_state binlog_state =
      Preserve_snapshot_binlog_state::GLOBAL_OFF_NO_CACHE;
  Mysql_binlog_preserve_snapshot binlog_snapshot;
  PrebuiltBinlogCacheBlob prebuilt_binlog_blob;
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
    return reject_after_binlog_export("temp_table_no_redo_undo_unsupported");
  }

  DBUG_EXECUTE_IF("preserve_trx_fail_after_binlog_mode_validation",
                  return reject_after_binlog_export(
                      "debug_after_binlog_mode_validation"););

  set_stage(Preserve_trx_preserve_stage::LOCK_PREFLIGHT);
  const Preserve_trx_lock_warmcopy_options lock_warmcopy_options =
      preserve_trx_lock_warmcopy_current_options();
  const Preserve_trx_lock_warmcopy_route lock_warmcopy_route =
      preserve_trx_lock_warmcopy_route_artifact(request.lock_warmcopy_artifact,
                                                lock_warmcopy_options);
  if (lock_warmcopy_route.action ==
      Preserve_trx_lock_warmcopy_route_action::REJECT) {
    if (batch_delivery && lock_warmcopy_options.enabled) {
      preserve_trx_lock_warmcopy_note_route_reject(
          lock_warmcopy_route.reason);
    }
    return reject_after_binlog_export(
        preserve_trx_lock_warmcopy_reason_name(lock_warmcopy_route.reason));
  }
  if (batch_delivery && lock_warmcopy_options.enabled &&
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
  if (trx_preserve_export_read_view(thd, &read_view_payload,
                                    &rv_low_limit_no) != DB_SUCCESS) {
    return reject_after_binlog_export("read_view_export_failed");
  }

  Preserve_lock_limits lock_limits;
  lock_limits.max_lock_count = preserve_trx_max_lock_count;
  lock_limits.max_modified_tables = preserve_trx_max_modified_tables;
  lock_limits.max_scan_pages = preserve_trx_max_scan_pages;
  lock_limits.materialize_timeout_ms = preserve_trx_materialize_timeout_ms;
  bool materialized_any_implicit_lock = false;
  auto materialize_implicit_locks_for_live_export = [&]() -> const char * {
    materialized_any_implicit_lock = false;
    if (trx_preserve_materialize_implicit_locks(
            thd, lock_limits, &materialized_any_implicit_lock) ==
        DB_SUCCESS) {
      return nullptr;
    }
    if (materialized_any_implicit_lock) {
      push_warning(
          thd, Sql_condition::SL_WARNING, ER_PRESERVE_TRX_UNSUPPORTED,
          "PRESERVE materialized implicit record locks before rejecting the "
          "transaction; the transaction remains active with equivalent or "
          "stronger InnoDB record locks");
    }
    return "implicit_lock_materialize_failed";
  };
  if (!use_lock_warmcopy_artifact) {
    const char *materialize_failure =
        materialize_implicit_locks_for_live_export();
    if (materialize_failure != nullptr) {
      return reject_after_binlog_export(materialize_failure);
    }
  }

  DEBUG_SYNC(thd, "preserve_trx_after_lock_materialization");
  DBUG_EXECUTE_IF("preserve_trx_fail_after_lock_materialization",
                  return reject_after_binlog_export(
                      "debug_after_lock_materialization"););
  if (thd->killed) {
    thd->send_kill_message();
    return true;
  }

  std::string mdl_descriptors_payload;
  size_t mdl_descriptors_count = 0;
  auto export_live_mdl_descriptors = [&]() -> const char * {
    mdl_descriptors_payload.clear();
    mdl_descriptors_count = 0;
    if (thd->mdl_context.export_preserved_locks(&mdl_descriptors_payload,
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
      const char *materialize_failure =
          materialize_implicit_locks_for_live_export();
      if (materialize_failure != nullptr) {
        return reject_after_binlog_export(materialize_failure);
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
  std::vector<Preserve_modified_table_name> modified_tables;
  if (trx_preserve_export_modified_table_names(
          thd, &modified_tables, lock_limits.max_modified_tables) !=
          DB_SUCCESS ||
      preserve_trx_recheck_modified_table_privileges(thd, modified_tables)) {
    return reject_after_binlog_export("modified_table_export_or_privilege_failed");
  }

  std::string sql_savepoints_payload;
  uint32_t savepoint_count = 0;
  uint32_t sql_innodb_savepoint_count = 0;
  if (export_sql_savepoints(thd, binlog_state, &sql_savepoints_payload,
                            &savepoint_count,
                            &sql_innodb_savepoint_count)) {
    return reject_after_binlog_export("sql_savepoint_export_failed");
  }

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
                                         lock_limits.max_lock_count) !=
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
    table_locks_preflight_payload.clear();
    if (trx_preserve_export_table_locks(thd, &table_locks_preflight_payload,
                                        lock_limits.max_lock_count,
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
    const char *materialize_failure =
        materialize_implicit_locks_for_live_export();
    if (materialize_failure != nullptr) return materialize_failure;

    const char *mdl_failure = export_live_mdl_descriptors();
    if (mdl_failure != nullptr) return mdl_failure;

    const char *lock_preflight_failure = export_live_lock_preflight_payloads();
    if (lock_preflight_failure != nullptr) return lock_preflight_failure;

    use_lock_warmcopy_artifact = false;
    lock_warmcopy_artifact = nullptr;
    return nullptr;
  };

  if (use_lock_warmcopy_artifact) {
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
        lock_limits.max_lock_count) {
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

  std::string token;
  if (generate_preserve_trx_token(&token))
    return reject_after_binlog_export("token_generation_failed");
  if (result != nullptr) result->token = token;

  XID xid;
  if (preserve_trx_token_to_xid(token, &xid))
    return reject_after_binlog_export("token_to_xid_failed");

  auto mark_single_detached_cleanup_failure =
      [&](const char *reason,
          const Preserve_snapshot_metadata *observable_metadata = nullptr) {
        const std::string message =
            redacted_preserved_trx_log_subject(token) +
            " single preserve cleanup failed after detach: " +
            (reason == nullptr ? "cleanup failure" : reason) +
            "; killing session";
        LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
        if (result != nullptr) result->cleanup_failed_after_reattach = true;

        if (observable_metadata != nullptr) {
          preserved_trx_add_failed_observable_record(
              *observable_metadata,
              reason == nullptr ? "single preserve detached cleanup failure"
                                : reason);
        } else {
          Preserve_snapshot_metadata failure_metadata =
              make_no_cache_metadata(thd, token, binlog_state);
          preserved_trx_add_failed_observable_record(
              failure_metadata,
              reason == nullptr ? "single preserve detached cleanup failure"
                                : reason);
        }
        thd->killed = THD::KILL_CONNECTION;
      };

  auto reject_single_after_attached_cleanup =
      [&](bool reactivate_engine, const char *reason) {
        set_failure_reason(reason);
        bool cleanup_failed = false;
        if (reactivate_engine &&
            trx_preserve_reactivate_prepare_failure_in_original_thd(thd) !=
                DB_SUCCESS) {
          cleanup_failed = true;
        }
        if (!cleanup_failed) {
          reset_preserve_xid_to_active_transaction_xid(thd);
          thd->get_transaction()->reset_unsafe_rollback_flags(
              Transaction_ctx::STMT);
          thd->get_transaction()->reset_scope(Transaction_ctx::STMT);
        }
        if (!cleanup_failed && has_logged_binlog_cache &&
            mysql_binlog_preserve_reactivate_after_prepare_failure(
                thd, binlog_snapshot)) {
          cleanup_failed = true;
        }
        if (cleanup_failed) {
          LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                 "PRESERVE: failed to restore single-session transaction "
                 "after preserve failure; killing session");
          if (result != nullptr) {
            result->cleanup_failed_after_reattach = true;
          }
          thd->killed = THD::KILL_CONNECTION;
          return true;
        }
        if (thd->killed) return true;
        return thd->is_error() ? true : preserve_trx_reject_unsupported();
      };

  auto restore_batch_prepare_failure_or_rollback = [&]() {
    if (batch_delivery) {
      if (reactivate_current_batch_prepared_failure_to_original_thd(thd)) {
        if (result != nullptr) {
          result->cleanup_failed_after_reattach = true;
        }
        return reject_unsupported_for_delivery();
      }
      if (has_logged_binlog_cache) {
        if (mysql_binlog_preserve_reactivate_after_prepare_failure(
                thd, binlog_snapshot)) {
          if (result != nullptr) {
            result->reattached_to_original_thd = true;
            result->cleanup_failed_after_reattach = true;
          }
          return reject_unsupported_for_delivery();
        }
      }
      if (result != nullptr) {
        result->reattached_to_original_thd = true;
      }
      return reject_unsupported_for_delivery();
    }
    return reject_single_after_attached_cleanup(
        true, "single_prepare_failure_cleanup");
  };
  auto restore_unprepared_batch_prepare_failure_or_rollback = [&]() {
    if (batch_delivery) {
      reset_preserve_xid_to_active_transaction_xid(thd);
      thd->get_transaction()->reset_unsafe_rollback_flags(
          Transaction_ctx::STMT);
      thd->get_transaction()->reset_scope(Transaction_ctx::STMT);
      if (has_logged_binlog_cache) {
        if (mysql_binlog_preserve_reactivate_after_prepare_failure(
                thd, binlog_snapshot)) {
          if (result != nullptr) {
            result->reattached_to_original_thd = true;
            result->cleanup_failed_after_reattach = true;
          }
          return reject_unsupported_for_delivery();
        }
      }
      if (result != nullptr) {
        result->reattached_to_original_thd = true;
      }
      return reject_unsupported_for_delivery();
    }
    return reject_single_after_attached_cleanup(
        false, "single_unprepared_failure_cleanup");
  };

  set_stage(Preserve_trx_preserve_stage::UNDO_PREPARE);
  DEBUG_SYNC(thd, "preserve_trx_before_undo_prepare");
  if (thd->killed) {
    thd->send_kill_message();
    return true;
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
    const char *materialize_failure =
        materialize_implicit_locks_for_live_export();
    if (materialize_failure != nullptr) return materialize_failure;

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
    lock_warmcopy_trx_lock_fence_t current_record_fence;
    if (!trx_preserve_sample_lock_warmcopy_fence(thd,
                                                 &current_record_fence)) {
      if (!handle_lock_warmcopy_final_fence_failure(
              Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID,
              "lock_warmcopy_final_fence_sample_failed")) {
        return restore_unprepared_batch_prepare_failure_or_rollback();
      }
    }
    if (use_lock_warmcopy_artifact) {
      const Preserve_trx_lock_warmcopy_reason final_fence_reason =
          preserve_trx_lock_warmcopy_verify_record_final_fence(
              *lock_warmcopy_artifact, current_record_fence);
      if (final_fence_reason != Preserve_trx_lock_warmcopy_reason::OK &&
          !handle_lock_warmcopy_final_fence_failure(
              final_fence_reason,
              preserve_trx_lock_warmcopy_reason_name(final_fence_reason))) {
        return restore_unprepared_batch_prepare_failure_or_rollback();
      }
    }
    if (use_lock_warmcopy_artifact) {
      if (!trx_preserve_lock_warmcopy_conversion_freeze(
              thd, &lock_warmcopy_frozen_fence,
              &lock_warmcopy_frozen_trx)) {
        if (!handle_lock_warmcopy_final_fence_failure(
                Preserve_trx_lock_warmcopy_reason::ARTIFACT_INVALID,
                "lock_warmcopy_conversion_freeze_failed")) {
          return restore_unprepared_batch_prepare_failure_or_rollback();
        }
      } else {
        lock_warmcopy_frozen_fence_valid = true;
      }
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
  }
  if (use_lock_warmcopy_artifact) {
    std::string current_sql_savepoints_payload;
    uint32_t current_savepoint_count = 0;
    uint32_t current_sql_innodb_savepoint_count = 0;
    if (export_sql_savepoints(thd, binlog_state,
                              &current_sql_savepoints_payload,
                              &current_savepoint_count,
                              &current_sql_innodb_savepoint_count)) {
      thaw_lock_warmcopy_conversion();
      set_failure_reason("savepoint_final_fence_export_failed");
      return restore_unprepared_batch_prepare_failure_or_rollback();
    }
    DBUG_EXECUTE_IF(
        "preserve_trx_lock_warmcopy_force_savepoint_final_fence_changed",
        { current_sql_savepoints_payload.push_back('\1'); });
    if (current_savepoint_count != savepoint_count ||
        current_sql_innodb_savepoint_count != sql_innodb_savepoint_count ||
        current_sql_savepoints_payload != sql_savepoints_payload) {
      thaw_lock_warmcopy_conversion();
      set_failure_reason("savepoint_final_fence_changed");
      return restore_unprepared_batch_prepare_failure_or_rollback();
    }
  }
  if (ha_prepare_low(thd, true)) {
    thaw_lock_warmcopy_conversion();
    return restore_batch_prepare_failure_or_rollback();
  }
  /*
    Keep the per-trx conversion freeze beyond ha_prepare_low().  The lock
    payload contract is not fully frozen until the transaction is detached,
    claimed, and the preserved lock metadata below has been populated.
  */
  bool temp_prepare_failed =
      request.debug_fail_temp_only_prepare ||
      trx_preserve_prepare_current_temp_only(thd, xid) != DB_SUCCESS;
  DBUG_EXECUTE_IF("pfx_temp_prepare", { temp_prepare_failed = true; });
  if (temp_prepare_failed) {
    thaw_lock_warmcopy_conversion();
    return restore_batch_prepare_failure_or_rollback();
  }
  if (result != nullptr) result->durable_point_crossed = true;
  auto restore_prepared_batch_or_rollback = [&]() {
    if (batch_delivery &&
        !reactivate_current_batch_prepared_failure_to_original_thd(thd)) {
      if (has_logged_binlog_cache) {
        if (mysql_binlog_preserve_reactivate_after_prepare_failure(
                thd, binlog_snapshot)) {
          if (result != nullptr) {
            result->reattached_to_original_thd = true;
            result->cleanup_failed_after_reattach = true;
          }
          return reject_unsupported_for_delivery();
        }
      }
      if (result != nullptr) {
        result->reattached_to_original_thd = true;
      }
      return reject_unsupported_for_delivery();
    }
    return reject_single_after_attached_cleanup(
        true, "single_prepared_failure_cleanup");
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
    if (batch_delivery) return restore_prepared_batch_or_rollback();
    return reject_single_after_attached_cleanup(
        true, "single_kill_connection_after_prepare_cleanup");
  }
  if (thd->killed == THD::KILL_QUERY) thd->killed = THD::NOT_KILLED;

  set_stage(Preserve_trx_preserve_stage::DETACH);
  trx_t *trx = trx_preserve_detach_current_thd(thd);
  if (trx == nullptr) {
    thaw_lock_warmcopy_conversion();
    return restore_prepared_batch_or_rollback();
  }
  if (result != nullptr) result->detached_from_original_thd = true;

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
    bool inject_rollback_failure = false;
    DBUG_EXECUTE_IF("preserve_trx_fail_single_detached_rollback",
                    inject_rollback_failure = true;);
    const dberr_t rollback_status =
        inject_rollback_failure ? DB_ERROR : trx_preserve_rollback_claimed(trx);
    reset_thd_after_preserve_detach(thd);
    cleanup_original_binlog_cache_after_detach(thd, has_logged_binlog_cache);
    if (result != nullptr && batch_delivery) {
      result->cleanup_completed_after_detach_failure =
          rollback_status == DB_SUCCESS;
      result->cleanup_failed_after_reattach = rollback_status != DB_SUCCESS;
    }
    if (!batch_delivery && rollback_status != DB_SUCCESS) {
      mark_single_detached_cleanup_failure(
          "single preserve detached rollback failure");
      return true;
    }
    return reject_unsupported_for_delivery();
  };

  auto batch_reattached_after_detach_failure = [&]() {
    bool cleanup_failed = false;
    if (!batch_delivery ||
        reattach_current_batch_preserve_failure_to_original_thd(
            thd, trx, token, false, false, &cleanup_failed, nullptr, nullptr,
            has_logged_binlog_cache, &binlog_snapshot))
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

  if (trx_preserve_claim_detached_prepared(trx) != DB_SUCCESS) {
    thaw_lock_warmcopy_conversion();
    if (batch_reattached_after_detach_failure())
      return reject_unsupported_for_delivery();
    (void)trx_preserve_rollback_by_token(token.c_str());
    reset_thd_after_preserve_detach(thd);
    cleanup_original_binlog_cache_after_detach(thd, has_logged_binlog_cache);
    return reject_unsupported_for_delivery();
  }

  DBUG_EXECUTE_IF("preserve_trx_fail_after_detach_for_batch_reattach", {
    thaw_lock_warmcopy_conversion();
    return reject_after_detach_failure_or_rollback();
  });

  Preserve_snapshot_metadata metadata =
      make_no_cache_metadata(thd, token, binlog_state);
  metadata.mod_tables_count = trx_preserve_modified_table_count(trx);
  metadata.modified_table_names.reserve(modified_tables.size());
  for (const Preserve_modified_table_name &name : modified_tables) {
    metadata.modified_table_names.push_back(
        {name.schema_name, name.table_name});
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
    metadata.record_locks_payload =
        lock_warmcopy_artifact->record_locks_payload;
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
            trx, &prepared_table_locks_payload, lock_limits.max_lock_count,
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
      preserve_trx_lock_warmcopy_note_canonical_mismatch("table_post_prepare");
      if (!lock_warmcopy_options.fallback_to_live_export) {
        preserve_trx_lock_warmcopy_note_route_reject(
            Preserve_trx_lock_warmcopy_reason::TABLE_POST_PREPARE_DRIFT);
        thaw_lock_warmcopy_conversion();
        return reject_after_detach_failure_or_rollback();
      }
      preserve_trx_lock_warmcopy_note_route_fallback(
          Preserve_trx_lock_warmcopy_reason::TABLE_POST_PREPARE_DRIFT);
      metadata.table_locks_payload = std::move(prepared_table_locks_payload);
      metadata.autoinc_lock_owned =
          trx_preserve_table_locks_payload_has_autoinc(
              metadata.table_locks_payload);
    } else {
      metadata.table_locks_payload = lock_warmcopy_artifact->table_locks_payload;
    }
  } else {
    if (trx_preserve_export_table_locks(
            trx, &metadata.table_locks_payload, lock_limits.max_lock_count,
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

  if (create_detached_mdl_context(thd, token)) {
    thaw_lock_warmcopy_conversion();
    return reject_after_detach_failure_or_rollback();
  }

  std::vector<Preserved_trx_external_blob_descriptor> blob_descriptors;

  auto reject_after_snapshot_failure =
      [&](bool snapshot_files_may_exist,
          Preserve_snapshot_delete_status write_failure_delete_status =
              Preserve_snapshot_delete_status::OK) {
    thaw_lock_warmcopy_conversion();
    const bool effective_snapshot_files_may_exist =
        snapshot_files_may_exist ||
        write_failure_delete_status ==
            Preserve_snapshot_delete_status::ERROR_BEFORE_SNAPSHOT_DELETE;
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
    if (batch_delivery &&
        !reattach_current_batch_preserve_failure_to_original_thd(
            thd, trx, token, true, effective_snapshot_files_may_exist,
            &cleanup_failed, &left_preserved, &metadata, has_logged_binlog_cache,
            &binlog_snapshot, &blob_descriptors)) {
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
    if (batch_delivery && cleanup_failed) {
      preserved_trx_add_failed_observable_record(
          metadata, "batch cleanup reattach failure");
      if (result != nullptr) result->cleanup_failed_after_reattach = true;
    }
    delete_detached_mdl_context(token);
    bool inject_rollback_failure = false;
    DBUG_EXECUTE_IF("preserve_trx_fail_single_detached_rollback",
                    inject_rollback_failure = true;);
    const dberr_t rollback_status =
        inject_rollback_failure ? DB_ERROR : trx_preserve_rollback_claimed(trx);
    if (rollback_status != DB_SUCCESS &&
        trx_preserve_is_active_attached_to_thd(trx, thd)) {
      if (result != nullptr && batch_delivery) {
        result->reattached_to_original_thd = true;
        result->cleanup_failed_after_reattach = true;
      }
      preserve_trx_set_batch_state(thd, 0, Preserve_trx_batch_thd_state::NONE);
      return reject_unsupported_for_delivery();
    }
    if (!batch_delivery && rollback_status != DB_SUCCESS) {
      reset_thd_after_preserve_detach(thd);
      cleanup_original_binlog_cache_after_detach(thd, has_logged_binlog_cache);
      mark_single_detached_cleanup_failure(
          "single preserve snapshot cleanup rollback failure", &metadata);
      return true;
    }
    if (result != nullptr && batch_delivery) {
      result->cleanup_completed_after_detach_failure = true;
      result->cleanup_failed_after_reattach =
          result->cleanup_failed_after_reattach ||
          rollback_status != DB_SUCCESS;
    }
    if (batch_delivery) {
      preserved_trx_add_failed_observable_record(
          metadata, "batch cleanup detached transaction fallback");
    }
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
        if (store->mark_tainted(token) != Preserve_snapshot_status::OK) {
          log_preserved_trx_cleanup_failure(
              token,
              "failed to taint snapshot after snapshot write cleanup failure");
        }
        if (result != nullptr && batch_delivery)
          result->cleanup_failed_after_reattach = true;
      }
    }
    reset_thd_after_preserve_detach(thd);
    cleanup_original_binlog_cache_after_detach(thd, has_logged_binlog_cache);
    return reject_unsupported_for_delivery();
  };

  set_stage(Preserve_trx_preserve_stage::SNAPSHOT_WRITE);
  Preserved_trx_observable_state_guard snapshotting_state(
      metadata, trx, Preserved_trx_lifecycle_state::SNAPSHOTTING);

  bool prebuilt_binlog_blob_finalized = false;
  auto discard_prebuilt_binlog_blob_if_needed = [&]() {
    if (use_prebuilt_binlog_cache && prebuilt_binlog_blob_finalized) {
      binlog_blob_provider->discard_for_preserve(thd, token,
                                                 prebuilt_binlog_blob);
      prebuilt_binlog_blob_finalized = false;
    }
  };

  if (use_prebuilt_binlog_cache) {
    prebuilt_binlog_blob.metadata = binlog_snapshot;
    const Preserve_snapshot_status provider_status =
        binlog_blob_provider->finalize_for_preserve(thd, token,
                                                    &prebuilt_binlog_blob);
    if (provider_status != Preserve_snapshot_status::OK) {
      set_failure_reason("warmcopy_blob_finalize_failed");
      return reject_after_snapshot_failure(false);
    }
    prebuilt_binlog_blob_finalized = true;
  }

  const Preserve_snapshot_status temp_manifest_status =
      preserve_trx_temp_table_build_preserve_manifest(
          thd, trx, preserve_trx_default_dir(), token, &metadata);
  if (temp_manifest_status != Preserve_snapshot_status::OK) {
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
  Preserved_trx_bundle bundle;
  const Preserve_snapshot_status bundle_status =
      build_preserved_trx_bundle(bundle_input, &bundle);
  if (bundle_status != Preserve_snapshot_status::OK) {
    discard_prebuilt_binlog_blob_if_needed();
    return reject_after_snapshot_failure(false);
  }
  blob_descriptors = bundle.blob_descriptors;

  auto store = create_preserved_trx_default_store(preserve_trx_default_dir());
  bool durable_snapshot_may_exist = false;
  Preserve_snapshot_delete_status write_failure_delete_status =
      Preserve_snapshot_delete_status::OK;
  const Preserve_snapshot_status status =
      store->write(std::move(bundle), timeout_seconds, &metadata,
                   &durable_snapshot_may_exist, &write_failure_delete_status);
  if (status != Preserve_snapshot_status::OK) {
    discard_prebuilt_binlog_blob_if_needed();
    return reject_after_snapshot_failure(durable_snapshot_may_exist,
                                         write_failure_delete_status);
  }

  if (use_lock_warmcopy_artifact) {
    DEBUG_SYNC(thd,
               "preserve_trx_lock_warmcopy_after_snapshot_write_before_final_fence");
    DBUG_EXECUTE_IF(
        "preserve_trx_lock_warmcopy_simulate_conversion_after_snapshot_write",
        { inject_lock_warmcopy_conversion_after_freeze(); });
    if (!verify_lock_warmcopy_frozen_fence()) {
      preserve_trx_lock_warmcopy_note_final_fence_mismatch();
      preserve_trx_lock_warmcopy_note_route_reject(
          Preserve_trx_lock_warmcopy_reason::SEAL_FENCE_CHANGED);
      set_failure_reason("lock_warmcopy_conversion_freeze_changed");
      return reject_after_snapshot_failure(true);
    }
  }

  set_stage(Preserve_trx_preserve_stage::RECORD_REGISTER);
  const Preserved_trx_lifecycle_state registered_state =
      batch_delivery ? Preserved_trx_lifecycle_state::DRAINING
                     : Preserved_trx_lifecycle_state::SNAPSHOTTING;
  if (preserved_trx_add_record(metadata, trx, batch_delivery,
                               registered_state,
                               blob_descriptors)) {
    thaw_lock_warmcopy_conversion();
    discard_prebuilt_binlog_blob_if_needed();
    bool cleanup_failed = false;
    bool left_preserved = false;
    if (batch_delivery &&
        !reattach_current_batch_preserve_failure_to_original_thd(
            thd, trx, token, true, true, &cleanup_failed, &left_preserved,
            &metadata, has_logged_binlog_cache, &binlog_snapshot,
            &blob_descriptors)) {
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
    bool inject_rollback_failure = false;
    DBUG_EXECUTE_IF("preserve_trx_fail_single_detached_rollback",
                    inject_rollback_failure = true;);
    const dberr_t rollback_status =
        inject_rollback_failure ? DB_ERROR : trx_preserve_rollback_claimed(trx);
    if (!batch_delivery && rollback_status != DB_SUCCESS) {
      reset_thd_after_preserve_detach(thd);
      cleanup_original_binlog_cache_after_detach(thd, has_logged_binlog_cache);
      mark_single_detached_cleanup_failure(
          "single preserve record register cleanup rollback failure", &metadata);
      return true;
    }
    (void)delete_preserved_snapshot_files_and_sidecars_or_log(
        preserve_trx_default_dir(), token, &metadata);
    reset_thd_after_preserve_detach(thd);
    cleanup_original_binlog_cache_after_detach(thd, has_logged_binlog_cache);
    return reject_unsupported_for_delivery();
  }
  thaw_lock_warmcopy_conversion();
  snapshotting_state.remove();

  if (batch_delivery) {
    reset_thd_after_preserve_detach(thd);
    cleanup_original_binlog_cache_after_detach(thd, has_logged_binlog_cache);
    audit_preserved_trx_event(thd, token, "preserve", "success");
    set_stage(Preserve_trx_preserve_stage::COMPLETE);
    return false;
  }

  reset_thd_after_preserve_detach(thd);
  cleanup_original_binlog_cache_after_detach(thd, has_logged_binlog_cache);
  set_stage(Preserve_trx_preserve_stage::COMPLETE);
  return false;
}

bool preserve_trx_preserve_attached_transaction(
    THD *target_thd, const Preserve_trx_options &options,
    ulonglong timeout_seconds, Preserve_trx_delivery_mode delivery_mode,
    Preserve_trx_preserve_result *result,
    PreserveBinlogBlobProvider *binlog_blob_provider,
    const Preserve_trx_lock_warmcopy_artifact *lock_warmcopy_artifact,
    bool debug_fail_ha_prepare_low_override,
    bool debug_fail_temp_only_prepare_override) {
  const bool batch_delivery =
      delivery_mode == Preserve_trx_delivery_mode::BATCH_MANAGER_DELIVERY;
  if (target_thd == nullptr) {
    return preserve_trx_reject_unsupported();
  }
  std::unique_ptr<Preserve_trx_manager_state_guard> draining;
  if (!batch_delivery) {
    draining = std::make_unique<Preserve_trx_manager_state_guard>(
        Preserve_trx_manager_state::IDLE,
        Preserve_trx_manager_state::SOFT_DRAINING, target_thd->thread_id());
    if (!draining->active()) {
      return preserve_trx_reject_unsupported();
    }

    if (preserve_trx_drain_other_active_transactions(target_thd)) {
      if (target_thd->killed) {
        target_thd->send_kill_message();
        return true;
      }
      if (target_thd->is_error()) return true;
      return preserve_trx_reject_unsupported();
    }
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
                                      delivery_mode, effective_result,
                                      binlog_blob_provider,
                                      debug_fail_ha_prepare_low,
                                      debug_fail_temp_only_prepare,
                                      lock_warmcopy_artifact};
  const bool error = preserve_trx_kernel_preserve_attached_transaction(request);
  if (error || batch_delivery) return error;

  effective_result->stage = Preserve_trx_preserve_stage::TOKEN_DELIVERY;
  DEBUG_SYNC(target_thd, "preserve_trx_before_token_delivery");
  if (target_thd->killed == THD::KILL_CONNECTION) {
    rollback_pending_token_delivery_record(effective_result->token);
    return true;
  }
  if (target_thd->killed == THD::KILL_QUERY)
    target_thd->killed = THD::NOT_KILLED;

  preserved_trx_register_pending_token_delivery(target_thd,
                                                effective_result->token);
  draining->transition_to(Preserve_trx_manager_state::SHUTDOWN_REQUESTED);
  draining->dismiss();
  DEBUG_SYNC(target_thd, "preserve_trx_after_token_delivery_pending");
  effective_result->stage = Preserve_trx_preserve_stage::COMPLETE;
  my_ok(target_thd);
  return false;
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
    case Preserve_trx_preserve_stage::TOKEN_DELIVERY:
      return "TOKEN_DELIVERY";
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
        " phase1_progress=" +
        std::to_string(observation.phase1_progress);
    if (!observation.failure_reason.empty()) {
      message += " failure_reason=" + observation.failure_reason;
    }
    LogErr(level, ER_LOG_PRINTF_MSG, message.c_str());
  }
}

static bool preserve_trx_execute_prepare_shutdown_preserve(
    THD *thd, const Preserve_trx_options &options, bool is_regular_command) {
  DBUG_TRACE;

  if (!preserve_trx_is_enabled()) {
    my_error(ER_PRESERVE_TRX_DISABLED, MYF(0));
    return true;
  }

  if (check_global_access(thd, SHUTDOWN_ACL)) return true;

  if (preserve_trx_is_unsupported_common_context(thd) || !is_regular_command) {
    return preserve_trx_reject_unsupported();
  }

  if (!preserve_trx_has_explicit_active_transaction(thd)) {
    my_error(ER_PRESERVE_TRX_INVALID_STATE, MYF(0));
    return true;
  }

  ulonglong timeout_seconds = 0;
  if (!preserved_trx_resolve_timeout_seconds(
          options, thd->variables.preserve_trx_default_timeout,
          thd->variables.preserve_trx_min_timeout,
          thd->variables.preserve_trx_max_timeout, &timeout_seconds)) {
    return preserve_trx_reject_unsupported();
  }

  return preserve_trx_preserve_attached_transaction(
      thd, options, timeout_seconds,
      Preserve_trx_delivery_mode::CLIENT_TOKEN_DELIVERY, nullptr);
}

bool Sql_cmd_prepare_shutdown_preserve_transaction::execute(THD *thd) {
  return preserve_trx_execute_prepare_shutdown_preserve(thd, m_options,
                                                       is_regular());
}

bool Preserve_trx_drain_service::execute(
    THD *thd, const Preserve_trx_drain_request &request) {
  DBUG_TRACE;
  const Preserve_trx_options &options = request.options;

  if (!preserve_trx_is_enabled()) {
    my_error(ER_PRESERVE_TRX_DISABLED, MYF(0));
    return true;
  }

  if (check_global_access(thd, SHUTDOWN_ACL)) return true;

  if (preserve_trx_is_unsupported_common_context(thd))
    return preserve_trx_reject_unsupported();

  if (preserve_trx_has_explicit_active_transaction(thd)) {
    my_error(ER_PRESERVE_TRX_INVALID_STATE, MYF(0));
    return true;
  }

  ulonglong timeout_seconds = 0;
  if (!preserved_trx_resolve_timeout_seconds(
          options, thd->variables.preserve_trx_default_timeout,
          thd->variables.preserve_trx_min_timeout,
          thd->variables.preserve_trx_max_timeout, &timeout_seconds))
    return preserve_trx_reject_unsupported();

  const ulonglong generation = g_batch_generation.fetch_add(1) + 1;
  const bool binlog_warmcopy_enabled =
      preserve_trx_warmcopy_enable && opt_bin_log && mysql_bin_log.is_open();
  const bool lock_warmcopy_enabled = preserve_trx_lock_warmcopy_effective();
  const bool two_phase_enabled =
      preserve_trx_lock_warmcopy_requires_two_phase(binlog_warmcopy_enabled);
  Preserve_trx_drain_orchestrator drain_orchestrator(
      two_phase_enabled ? Preserve_trx_drain_phase_mode::TWO_PHASE
                        : Preserve_trx_drain_phase_mode::SINGLE_PHASE);
  std::unique_ptr<Warmcopy_batch_drain_participant> warmcopy_participant;
  if (binlog_warmcopy_enabled) {
    warmcopy_participant = std::make_unique<Warmcopy_batch_drain_participant>(
        thd, generation, preserve_trx_warmcopy_close_timeout_ms);
    drain_orchestrator.add_participant(warmcopy_participant.get());
  }
  std::unique_ptr<Preserve_trx_lock_warmcopy_drain_participant>
      lock_warmcopy_participant;
  if (lock_warmcopy_enabled) {
    lock_warmcopy_participant =
        std::make_unique<Preserve_trx_lock_warmcopy_drain_participant>(
            preserve_trx_lock_warmcopy_current_options());
    drain_orchestrator.add_participant(lock_warmcopy_participant.get());
  }
  PreserveBinlogBlobProvider *warmcopy_provider = nullptr;
  Preserve_trx_manager_state_guard draining(
      Preserve_trx_manager_state::IDLE,
      two_phase_enabled ? Preserve_trx_manager_state::WARMCOPY_DRAINING
                        : Preserve_trx_manager_state::BATCH_DRAINING,
      thd->thread_id());
  if (!draining.active()) return preserve_trx_reject_unsupported();
  std::unique_lock<std::mutex> warmcopy_status_guard;

  auto abort_drain_participants = [&](const char *stage) {
    drain_orchestrator.abort_participants();
    log_preserve_trx_drain_participant_observations(drain_orchestrator, stage,
                                                    INFORMATION_LEVEL);
  };

  auto finalize_drain_participants_for_shutdown = [&](const char *stage) {
    drain_orchestrator.finalize_participants_for_shutdown();
    log_preserve_trx_drain_participant_observations(
        drain_orchestrator, stage, INFORMATION_LEVEL);
  };

  if (binlog_warmcopy_enabled) {
    warmcopy_status_guard = std::unique_lock<std::mutex>(g_warmcopy_status_mutex);
  }
  if (two_phase_enabled) {
    if (drain_orchestrator.open_phase1_participants() !=
        Preserve_trx_drain_status::OK) {
      abort_drain_participants("open_phase1_failed");
      return preserve_trx_reject_unsupported();
    }
    if (warmcopy_participant != nullptr) {
      warmcopy_provider = warmcopy_participant->provider();
    }
    DEBUG_SYNC(thd, "preserve_trx_warmcopy_after_phase1_open");
  }

  ulonglong warmcopy_close_deadline_us = 0;
  if (two_phase_enabled) {
    /*
      Entering WARMCOPY_CLOSING is intentionally a two-step tightening:
      the manager state is published first, so command gates immediately block
      new transaction-capable work, then close_phase1_participants() closes
      epoch admission and waits for already-admitted mirror work to drain.
    */
    draining.transition_to(Preserve_trx_manager_state::WARMCOPY_CLOSING);
    if (warmcopy_participant != nullptr) {
      warmcopy_close_deadline_us = preserve_trx_monotonic_deadline_after_ms(
          preserve_trx_monotonic_us(), preserve_trx_warmcopy_close_timeout_ms);
      warmcopy_participant->set_closing_deadline_us(warmcopy_close_deadline_us);
    }
    DEBUG_SYNC(thd, "preserve_trx_warmcopy_after_closing_state_before_targets");
  }
  const ulonglong target_wait_deadline_us =
      warmcopy_close_deadline_us != 0 ? warmcopy_close_deadline_us
                                      : preserve_trx_batch_hard_deadline_us();

  Preserve_batch_target_counter counter(thd, generation);
  Global_THD_manager::get_instance()->do_for_all_thd_copy(&counter);
  size_t preserved_token_count = 0;

  auto finish_with_shutdown = [&]() {
    audit_preserved_trx_control_event(
        thd, "drain", "success", static_cast<longlong>(counter.target_count()),
        static_cast<longlong>(preserved_token_count));
    DBUG_EXECUTE_IF("preserve_trx_drain_skip_shutdown_after_audit_no_targets", {
      if (counter.target_count() == 0) {
        my_ok(thd);
        return false;
      }
    });
    finalize_drain_participants_for_shutdown("finish");
    draining.transition_to(Preserve_trx_manager_state::SHUTDOWN_REQUESTED);

    const bool shutdown_success = shutdown(thd, SHUTDOWN_DEFAULT);
    if (shutdown_success) {
      draining.dismiss();
      return false;
    }

    drain_orchestrator.cleanup_after_failed_shutdown();
    return true;
  };

  auto finish_cleanup_failure_without_shutdown = [&](const char *stage) {
    Preserve_batch_clear_generation clear(generation);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
    abort_drain_participants(stage);
    draining.transition_to(Preserve_trx_manager_state::DRAIN_CLEANUP_FAILED,
                           0);
    draining.dismiss();
    return preserve_trx_reject_batch_cleanup_failed();
  };

  auto close_warmcopy_participants_for_shutdown = [&](const char *stage) {
    if (!two_phase_enabled) return false;
    if (drain_orchestrator.close_phase1_participants() !=
            Preserve_trx_drain_status::OK ||
        drain_orchestrator.ensure_phase1_ready() !=
            Preserve_trx_drain_status::OK ||
        drain_orchestrator.phase2_preflight_participants() !=
            Preserve_trx_drain_status::OK) {
      abort_drain_participants(stage);
      return true;
    }
    return false;
  };

  if (counter.nonidle_transaction_count() != 0 ||
      counter.has_unsupported_transaction()) {
    Preserve_batch_clear_generation clear(generation);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
    abort_drain_participants("target_counter_rejected");
    return preserve_trx_reject_unsupported();
  }

  if (counter.target_count() == 0) {
    if (close_warmcopy_participants_for_shutdown("no_targets_close_failed"))
      return preserve_trx_reject_unsupported();
    return finish_with_shutdown();
  }

  std::vector<my_thread_id> quiesced_target_thread_ids;
  quiesced_target_thread_ids.reserve(counter.target_thread_ids().size());
  for (const my_thread_id target_thread_id : counter.target_thread_ids()) {
    Preserve_trx_batch_thd_state target_state{
        Preserve_trx_batch_thd_state::NONE};
    if (preserve_trx_batch_wait_target_ready(
            thd, generation, target_thread_id, &target_state,
            target_wait_deadline_us) ||
        target_state == Preserve_trx_batch_thd_state::PENDING_QUIESCE ||
        target_state == Preserve_trx_batch_thd_state::NONE) {
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      abort_drain_participants("target_wait_failed");
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
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      abort_drain_participants("target_state_rejected");
      return preserve_trx_reject_unsupported();
    }

    quiesced_target_thread_ids.push_back(target_thread_id);
  }

  if (quiesced_target_thread_ids.empty()) {
    if (close_warmcopy_participants_for_shutdown("no_quiesced_close_failed"))
      return preserve_trx_reject_unsupported();
    return finish_with_shutdown();
  }

  Preserve_batch_quiesced_target_counter ready_counter(
      thd, generation, quiesced_target_thread_ids);
  Global_THD_manager::get_instance()->do_for_all_thd_copy(&ready_counter);
  if (static_cast<size_t>(ready_counter.target_count()) !=
          quiesced_target_thread_ids.size() ||
      ready_counter.target_count() > preserve_trx_batch_max_transactions ||
      ready_counter.has_unsupported_transaction() ||
      !preserved_trx_batch_has_capacity(ready_counter.account_counts())) {
    Preserve_batch_clear_generation clear(generation);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
    abort_drain_participants("quiesced_counter_rejected");
    return preserve_trx_reject_unsupported();
  }

  if (warmcopy_participant != nullptr) {
    if (!warmcopy_participant->prepare_quiesced_targets(
            quiesced_target_thread_ids, warmcopy_close_deadline_us)) {
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      abort_drain_participants("participant_phase2_prepare_rejected");
      return preserve_trx_reject_unsupported();
    }
  }
  if (lock_warmcopy_participant != nullptr) {
    std::vector<uint64_t> lock_warmcopy_target_thread_ids;
    lock_warmcopy_target_thread_ids.reserve(quiesced_target_thread_ids.size());
    for (const my_thread_id target_thread_id : quiesced_target_thread_ids) {
      lock_warmcopy_target_thread_ids.push_back(
          static_cast<uint64_t>(target_thread_id));
    }
    if (!lock_warmcopy_participant->prepare_quiesced_targets(
            lock_warmcopy_target_thread_ids)) {
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      abort_drain_participants("lock_warmcopy_phase2_prepare_rejected");
      return preserve_trx_reject_unsupported();
    }
  }
  if (two_phase_enabled) {
    if (drain_orchestrator.close_phase1_participants() !=
        Preserve_trx_drain_status::OK) {
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      abort_drain_participants("close_phase1_failed");
      return preserve_trx_reject_unsupported();
    }
    if (drain_orchestrator.ensure_phase1_ready() !=
            Preserve_trx_drain_status::OK ||
        drain_orchestrator.phase2_preflight_participants() !=
            Preserve_trx_drain_status::OK) {
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      abort_drain_participants("phase1_not_ready");
      return preserve_trx_reject_unsupported();
    }
  }
  if (warmcopy_participant != nullptr) {
    if (!warmcopy_participant->tail_budget_within_limits(
            quiesced_target_thread_ids, warmcopy_close_deadline_us)) {
      Preserve_batch_clear_generation clear(generation);
      Global_THD_manager::get_instance()->do_for_all_thd_copy(&clear);
      abort_drain_participants("participant_phase2_budget_rejected");
      return preserve_trx_reject_unsupported();
    }
  }
  if (two_phase_enabled) {
    draining.transition_to(Preserve_trx_manager_state::BATCH_DRAINING);
  }

  DEBUG_SYNC(thd, "preserve_trx_batch_after_targets_quiesced_before_attach");

  std::vector<Preserve_trx_batch_item> preserved_batch_items;
  preserved_batch_items.reserve(quiesced_target_thread_ids.size());
  bool debug_fail_ha_prepare_low = false;
  bool debug_fail_temp_only_prepare = false;
  DBUG_EXECUTE_IF("pfx_prepare_low", { debug_fail_ha_prepare_low = true; });
  DBUG_EXECUTE_IF("pfx_temp_prepare",
                  { debug_fail_temp_only_prepare = true; });
  for (const my_thread_id target_thread_id : quiesced_target_thread_ids) {
    Preserve_batch_single_quiesced_target_pin target_pin(thd, generation,
                                                         target_thread_id);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&target_pin);

    const Preserve_trx_lock_warmcopy_artifact *lock_warmcopy_artifact =
        lock_warmcopy_participant == nullptr
            ? nullptr
            : lock_warmcopy_participant->artifact_for_thread(target_thread_id);
    Preserve_batch_quiesced_idle_target batch(thd, options, timeout_seconds,
                                              generation, target_thread_id,
                                              warmcopy_provider,
                                              lock_warmcopy_artifact,
                                              debug_fail_ha_prepare_low,
                                              debug_fail_temp_only_prepare);
    if (target_pin.found()) batch.run(target_pin.target().thd);

    if (target_pin.error() || !batch.visited_target() || batch.error() ||
        batch.result().stage != Preserve_trx_preserve_stage::COMPLETE) {
      const Preserve_trx_preserve_result &batch_result = batch.result();
      const std::string message =
          "PRESERVE: batch target preserve failed visited=" +
          std::to_string(batch.visited_target() ? 1 : 0) +
          " error=" + std::to_string(batch.error() ? 1 : 0) +
          " stage=" + preserve_trx_preserve_stage_name(batch_result.stage) +
          " reason=" +
          (batch_result.failure_reason == nullptr ? "unknown"
                                                  : batch_result.failure_reason) +
          " token_present=" +
          std::to_string(batch_result.token.empty() ? 0 : 1) +
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
          " logged_binlog_cache=" +
          std::to_string(batch_result.logged_binlog_cache ? 1 : 0);
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
      const bool prior_cleanup_error = restore_preserved_batch_items_to_original_thds(
          generation, preserved_batch_items);
      const bool cleanup_error =
          batch.result().cleanup_failed_after_reattach || prior_cleanup_error;
      if (cleanup_error) {
        if (!batch.result().token.empty()) {
          if (batch.result().left_preserved_after_cleanup_failure) {
            (void)preserved_trx_mark_preserved_with_last_error(
                batch.result().token,
                "batch cleanup failure after target preserve error");
          } else {
            (void)preserved_trx_update_record_error(
                batch.result().token,
                "batch cleanup failure after target preserve error");
          }
        }
        LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
               "Preserved transaction batch cleanup failed after target "
               "preserve error");
        return finish_cleanup_failure_without_shutdown("target_cleanup_failed");
      }
      abort_drain_participants("target_preserve_failed");
      return preserve_trx_reject_unsupported();
    }

    preserved_batch_items.push_back(
        {target_thread_id, batch.result().token,
         batch.result().logged_binlog_cache});
    preserved_token_count = preserved_batch_items.size();
    DEBUG_SYNC(thd, "preserve_trx_batch_after_one_target_preserved");
    DBUG_EXECUTE_IF("preserve_trx_batch_fail_after_one_target", {
      if (preserved_batch_items.size() == 1 &&
          quiesced_target_thread_ids.size() > 1) {
        if (restore_preserved_batch_items_to_original_thds(
                generation, preserved_batch_items)) {
          LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                 "Preserved transaction batch cleanup failed after debug "
                 "injected target error");
          return finish_cleanup_failure_without_shutdown(
              "debug_after_one_target_cleanup_failed");
        }
        abort_drain_participants("debug_after_one_target_failed");
        return preserve_trx_reject_unsupported();
      }
    });
  }

  if (binlog_warmcopy_enabled) {
    const ulonglong phase2_pause_us =
        preserve_trx_warmcopy_phase2_pause_us_status();
    const std::string message =
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
            preserve_trx_warmcopy_provider_full_copy_to_count_status());
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
  options.has_timeout = lex->preserve_trx_has_timeout;
  options.timeout_seconds = lex->preserve_trx_timeout_seconds;
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

bool preserve_trx_execute_command(THD *thd) {
  if (thd == nullptr || thd->lex == nullptr) {
    my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
    return true;
  }

  Preserve_trx_options options = preserve_trx_options_from_lex(thd->lex);
  switch (thd->lex->sql_command) {
    case SQLCOM_PREPARE_SHUTDOWN_PRESERVE: {
      const bool is_regular_command =
          thd->stmt_arena == nullptr || thd->stmt_arena->is_regular();
      return preserve_trx_execute_prepare_shutdown_preserve(
          thd, options, is_regular_command);
    }
    case SQLCOM_DRAIN_TRANSACTIONS_PRESERVE: {
      Preserve_trx_drain_request request{options};
      Preserve_trx_drain_service service;
      return service.execute(thd, request);
    }
    default:
      my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
      return true;
  }
}

bool Sql_cmd_resume_preserved_transaction::execute(THD *thd) {
  DBUG_TRACE;

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

  const std::string token(m_token.str, m_token.length);
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
      (void)delete_snapshot_files(preserve_trx_default_dir(), token);
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
  if (!preserved_trx_resume_allowed_for_account(
          owns_token, has_resume_any_privilege)) {
    my_error(ER_PRESERVE_TRX_ACCESS_DENIED, MYF(0));
    return true;
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
    (void)preserved_trx_update_record_error(
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

  DBUG_EXECUTE_IF("preserve_trx_fail_resume_set_isolation",
                  (void)restore_record_after_resume_failure(
                      record, "debug injected isolation restore failure");
                  return preserve_trx_reject_unsupported(););

  if (record.metadata.tx_isolation > ISO_SERIALIZABLE ||
      set_tx_isolation(thd,
                       static_cast<enum_tx_isolation>(
                           record.metadata.tx_isolation),
                       true)) {
    (void)restore_record_after_resume_failure(record,
                                              "isolation restore failure");
    return preserve_trx_reject_unsupported();
  }
  if (restore_preserved_session_variables(thd, record.metadata)) {
    (void)restore_record_after_resume_failure(
        record, "session state restore failure");
    return preserve_trx_reject_unsupported();
  }

  thd->variables.sql_log_bin = record.metadata.session_sql_log_bin;
  if (record.metadata.option_bin_log)
    thd->variables.option_bits |= OPTION_BIN_LOG;
  else
    thd->variables.option_bits &= ~OPTION_BIN_LOG;
  thd->variables.option_bits |= OPTION_BEGIN;
  thd->server_status |= SERVER_STATUS_IN_TRANS;
  restore_last_insert_id_state(thd, record.metadata);
  restore_forced_insert_id_state(thd, record.metadata);
  if (import_user_vars_payload(thd, record.metadata.user_vars_payload)) {
    (void)restore_record_after_resume_failure(record,
                                              "user variables restore failure");
    return preserve_trx_reject_unsupported();
  }

  bool binlog_imported = false;
  if (record.metadata.binlog_state ==
      Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE) {
    DBUG_EXECUTE_IF("preserve_trx_resume_clear_binlog_cache_payload",
                    record.metadata.binlog_cache_payload.clear(););
    if (hydrate_logged_binlog_cache_payload_if_needed(&record, token)) {
      (void)restore_record_after_resume_failure(
          record, "binlog cache read failure");
      return preserve_trx_reject_unsupported();
    }
    Mysql_binlog_preserve_snapshot binlog_snapshot =
        metadata_to_binlog_cache_snapshot(record.metadata);
    if (mysql_binlog_preserve_import(thd, binlog_snapshot)) {
      (void)restore_record_after_resume_failure(record,
                                                "binlog cache import failure");
      return preserve_trx_reject_unsupported();
    }
    binlog_imported = true;
  }

  bool mdl_transferred = false;
  bool gtid_restored = false;
  bool temp_tables_materialized = false;
  auto restore_preserved_record_after_failure =
      [&](const std::string &reason) {
        if (temp_tables_materialized) {
          (void)preserve_trx_temp_table_rollback_materialized_for_resume(
              thd, record.metadata);
          temp_tables_materialized = false;
        }
        const bool must_reset_thd_transaction_state = gtid_restored;
        rollback_restored_logged_cache_gtid_next(thd, &gtid_restored);
        if (binlog_imported) {
          discard_binlog_preserve_cache_and_reset_scopes(thd);
          binlog_imported = false;
        }
        if (must_reset_thd_transaction_state)
          reset_thd_after_preserve_detach(thd);
        else if (mdl_transferred)
          thd->mdl_context.release_transactional_locks();
        return restore_record_after_resume_failure(record, reason);
      };

  auto detach_resumed_after_failure = [&](const char *reason) {
    if (trx_preserve_detach_resumed_from_thd(record.trx, thd) == DB_SUCCESS)
      return false;

    const std::string retry_message =
        "Preserved transaction resume failed to detach transaction after " +
        std::string(reason) + "; retrying cleanup detach";
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, retry_message.c_str());
    if (trx_preserve_detach_resumed_from_thd_for_cleanup(record.trx, thd) ==
        DB_SUCCESS)
      return false;

    const std::string kill_message =
        "Preserved transaction resume failed to detach transaction after " +
        std::string(reason) + "; killing session";
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, kill_message.c_str());
    preserved_trx_add_resume_detach_failure_observable_record(record, reason);
    thd->killed = THD::KILL_CONNECTION;
    thd_state_guard.dismiss();
    return true;
  };

  if (restore_detached_mdl_context(thd, token)) {
    (void)restore_preserved_record_after_failure("MDL transfer failure");
    return preserve_trx_reject_unsupported();
  }
  mdl_transferred = true;

  DBUG_EXECUTE_IF(
      "preserve_trx_fail_resume_before_attach",
      (void)restore_preserved_record_after_failure("debug injected failure");
      return preserve_trx_reject_unsupported(););

  if (preserve_snapshot_allows_gtid_restore(record.metadata)) {
    if (restore_logged_cache_gtid_next(thd, record.metadata)) {
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
          preserve_trx_default_dir(), token, &record.metadata);
      return preserve_trx_reject_unsupported();
    }
    gtid_restored = true;

    if (trx_preserve_prepare_resumed_rollback_gtid(record.trx) !=
        DB_SUCCESS) {
      (void)restore_preserved_record_after_failure(
          "binlog GTID rollback undo preparation failure");
      return preserve_trx_reject_unsupported();
    }
  }

  const Preserve_snapshot_status temp_materialize_status =
      preserve_trx_temp_table_materialize_for_resume(
          thd, record.trx, preserve_trx_default_dir(), token, record.metadata);
  if (temp_materialize_status != Preserve_snapshot_status::OK) {
    (void)restore_preserved_record_after_failure(
        "temporary table materialization failure");
    return preserve_trx_reject_unsupported();
  }
  temp_tables_materialized =
      !record.metadata.temp_table_manifest_payload.empty() &&
      preserve_trx_temp_table_enable;

  if (trx_preserve_attach_to_thd(record.trx, thd) != DB_SUCCESS) {
    (void)restore_preserved_record_after_failure("attach failure");
    return preserve_trx_reject_unsupported();
  }

  if (restore_savepoints_to_thd(thd, record.trx, record.metadata)) {
    if (!detach_resumed_after_failure("savepoint restore failure")) {
      rollback_restored_logged_cache_gtid_next(thd, &gtid_restored);
      if (binlog_imported) {
        discard_binlog_preserve_cache_and_reset_scopes(thd);
        binlog_imported = false;
      }
      reset_thd_after_preserve_detach(thd);
      if (temp_tables_materialized) {
        (void)preserve_trx_temp_table_rollback_materialized_for_resume(
            thd, record.metadata);
        temp_tables_materialized = false;
      }
      (void)restore_record_after_resume_failure(record,
                                                "savepoint restore failure");
    }
    return preserve_trx_reject_unsupported();
  }

  if (trx_preserve_activate_resumed(record.trx) != DB_SUCCESS) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           "Preserved transaction resume failed to activate prepared undo state");
    if (!detach_resumed_after_failure("undo activation failure")) {
      reset_thd_after_preserve_detach(thd);
      (void)restore_preserved_record_after_failure("undo activation failure");
    }
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
  const Preserve_snapshot_delete_status delete_status =
      delete_snapshot_files_with_status(preserve_trx_default_dir(), token,
                                        remove_options);
  if (delete_status != Preserve_snapshot_delete_status::OK) {
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
