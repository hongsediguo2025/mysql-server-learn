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

#include "sql/preserve_trx_resource.h"

#include <algorithm>
#include <array>
#include <limits>
#include <map>
#include <mutex>
#include <utility>

#ifndef _WIN32
#include <sys/statvfs.h>
#endif

#include "my_sys.h"
#include "mysqld_error.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_lock_warmcopy.h"
#include "sql/mysqld.h"
#include "sql/preserve_trx_promotion.h"
#include "sql/preserve_trx_promotion_prepared.h"
#include "sql/preserve_trx_transfer.h"

ulonglong preserve_trx_memory_budget_bytes = 256ULL * 1024ULL * 1024ULL;
ulonglong preserve_trx_memory_per_token_bytes = 64ULL * 1024ULL * 1024ULL;
uint preserve_trx_spill_chunk_bytes = 4U * 1024U * 1024U;

namespace {

constexpr size_t kPreserveMemoryKindCount =
    static_cast<size_t>(Preserve_trx_memory_kind::COUNT);

size_t preserve_memory_kind_index(Preserve_trx_memory_kind kind) {
  return static_cast<size_t>(kind);
}

uint64_t preserve_memory_kind_cap(Preserve_trx_memory_kind kind,
                                  uint64_t global_budget) {
  switch (kind) {
    case Preserve_trx_memory_kind::PROMOTION_LOCK_PLAN:
      return global_budget / 10 * 6 + global_budget % 10 * 6 / 10;
    case Preserve_trx_memory_kind::PROMOTION_BINLOG_NATIVE_CACHE:
      return global_budget / 10 * 3 + global_budget % 10 * 3 / 10;
    default:
      return global_budget;
  }
}

struct Token_kind_key {
  std::string token;
  Preserve_trx_memory_kind kind{Preserve_trx_memory_kind::SNAPSHOT_CODEC_BUFFER};

  bool operator<(const Token_kind_key &other) const {
    if (token != other.token) return token < other.token;
    return static_cast<int>(kind) < static_cast<int>(other.kind);
  }
};

bool preserve_trx_capture_external_resource_limits(
    Preserve_trx_external_resource_limits *limits) {
  if (limits == nullptr || open_files_limit == 0) return false;
#ifdef _WIN32
  return false;
#else
  struct statvfs filesystem{};
  const char *const tmpdir = mysql_tmpdir;
  if (tmpdir == nullptr || statvfs(tmpdir, &filesystem) != 0 ||
      filesystem.f_frsize == 0 ||
      filesystem.f_bavail >
          std::numeric_limits<uint64_t>::max() / filesystem.f_frsize) {
    return false;
  }
  limits->open_files_limit = open_files_limit;
  limits->current_open_files =
      static_cast<uint64_t>(my_file_opened) + my_stream_opened;
  limits->tmpdir_free_bytes =
      static_cast<uint64_t>(filesystem.f_bavail) * filesystem.f_frsize;
  limits->snapshots_available = true;
  return true;
#endif
}

class Preserve_resource_manager {
 public:
  bool acquire(const std::string &token, Preserve_trx_memory_kind kind,
               uint64_t bytes) {
    std::lock_guard<std::mutex> guard(m_mutex);
    return acquire_memory_locked(token, kind, bytes);
  }

  void release(const std::string &token, Preserve_trx_memory_kind kind,
               uint64_t bytes) {
    std::lock_guard<std::mutex> guard(m_mutex);
    release_memory_locked(token, kind, bytes);
  }

  bool acquire_native_binlog(const std::string &token, uint64_t memory_bytes,
                             uint64_t fd_count, uint64_t tmpdir_bytes) {
    Preserve_trx_external_resource_limits limits;
#ifndef NDEBUG
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      if (m_external_limits_override) {
        limits = m_external_limits;
        return acquire_native_binlog_locked(token, memory_bytes, fd_count,
                                            tmpdir_bytes, limits);
      }
    }
#endif
    if (!preserve_trx_capture_external_resource_limits(&limits)) return false;
    std::lock_guard<std::mutex> guard(m_mutex);
    return acquire_native_binlog_locked(token, memory_bytes, fd_count,
                                        tmpdir_bytes, limits);
  }

  void release_native_binlog(const std::string &token, uint64_t memory_bytes,
                             uint64_t fd_count, uint64_t tmpdir_bytes) {
    std::lock_guard<std::mutex> guard(m_mutex);
    release_memory_locked(token,
                          Preserve_trx_memory_kind::PROMOTION_BINLOG_NATIVE_CACHE,
                          memory_bytes);
    auto fd_it = m_native_fd_by_token.find(token);
    const uint64_t released_fds =
        fd_it == m_native_fd_by_token.end()
            ? 0
            : std::min(fd_count, fd_it->second);
    m_reserved_native_fds -=
        std::min(m_reserved_native_fds, released_fds);
    if (fd_it != m_native_fd_by_token.end()) {
      if (released_fds >= fd_it->second)
        m_native_fd_by_token.erase(fd_it);
      else
        fd_it->second -= released_fds;
    }
    auto tmp_it = m_native_tmpdir_by_token.find(token);
    const uint64_t released_tmpdir =
        tmp_it == m_native_tmpdir_by_token.end()
            ? 0
            : std::min(tmpdir_bytes, tmp_it->second);
    m_reserved_native_tmpdir_bytes -=
        std::min(m_reserved_native_tmpdir_bytes, released_tmpdir);
    if (tmp_it != m_native_tmpdir_by_token.end()) {
      if (released_tmpdir >= tmp_it->second)
        m_native_tmpdir_by_token.erase(tmp_it);
      else
        tmp_it->second -= released_tmpdir;
    }
  }

#ifndef NDEBUG
  void set_external_limits_for_unit_test(
      const Preserve_trx_external_resource_limits &limits) {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_external_limits = limits;
    m_external_limits_override = limits.snapshots_available;
  }

  uint64_t reserved_native_fds_for_unit_test() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    return m_reserved_native_fds;
  }

  uint64_t reserved_native_tmpdir_bytes_for_unit_test() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    return m_reserved_native_tmpdir_bytes;
  }
#endif

 private:
  bool acquire_memory_locked(const std::string &token,
                             Preserve_trx_memory_kind kind, uint64_t bytes) {
    const size_t kind_index = preserve_memory_kind_index(kind);
    if (token.empty() || kind_index >= kPreserveMemoryKindCount) return false;
    /*
      Resource accounting is scoped by preserved token and memory kind. It
      protects preserve/resume staging buffers from exhausting the process, but
      it is not used by ordinary transaction execution when no preserve token is
      being built or resumed.
    */
    const uint64_t global_budget = preserve_trx_memory_budget_bytes;
    const uint64_t per_token_budget = preserve_trx_memory_per_token_bytes;
    if (bytes > global_budget || bytes > per_token_budget) return false;
    if (m_current_bytes > global_budget - bytes) return false;
    const uint64_t kind_cap = preserve_memory_kind_cap(kind, global_budget);
    if (bytes > kind_cap || m_by_kind[kind_index] > kind_cap - bytes) {
      return false;
    }
    const uint64_t token_bytes = m_by_token[token];
    if (token_bytes > per_token_budget - bytes) return false;
    m_current_bytes += bytes;
    m_peak_bytes = std::max(m_peak_bytes, m_current_bytes);
    m_by_token[token] = token_bytes + bytes;
    m_by_token_kind[{token, kind}] += bytes;
    m_by_kind[kind_index] += bytes;
    return true;
  }

  void release_memory_locked(const std::string &token,
                             Preserve_trx_memory_kind kind, uint64_t bytes) {
    const size_t kind_index = preserve_memory_kind_index(kind);
    if (token.empty() || kind_index >= kPreserveMemoryKindCount) return;
    Token_kind_key key{token, kind};
    auto kind_it = m_by_token_kind.find(key);
    const uint64_t released_bytes =
        kind_it == m_by_token_kind.end()
            ? 0
            : std::min(bytes, kind_it->second);
    m_current_bytes -= std::min(m_current_bytes, released_bytes);
    m_by_kind[kind_index] -=
        std::min(m_by_kind[kind_index], released_bytes);

    auto token_it = m_by_token.find(token);
    if (token_it != m_by_token.end()) {
      if (released_bytes >= token_it->second) {
        m_by_token.erase(token_it);
      } else {
        token_it->second -= released_bytes;
      }
    }

    if (kind_it != m_by_token_kind.end()) {
      if (released_bytes >= kind_it->second) {
        m_by_token_kind.erase(kind_it);
      } else {
        kind_it->second -= released_bytes;
      }
    }
  }

  bool acquire_native_binlog_locked(
      const std::string &token, uint64_t memory_bytes, uint64_t fd_count,
      uint64_t tmpdir_bytes,
      const Preserve_trx_external_resource_limits &limits) {
    if (token.empty() || memory_bytes == 0 || fd_count == 0 ||
        !limits.snapshots_available || limits.open_files_limit == 0) {
      return false;
    }
    const uint64_t fd_headroom =
        std::max<uint64_t>(64, limits.open_files_limit / 10);
    if (limits.current_open_files > limits.open_files_limit ||
        m_reserved_native_fds >
            limits.open_files_limit - limits.current_open_files ||
        fd_count > limits.open_files_limit - limits.current_open_files -
                       m_reserved_native_fds ||
        fd_headroom > limits.open_files_limit - limits.current_open_files -
                          m_reserved_native_fds - fd_count) {
      return false;
    }
    const uint64_t tmpdir_headroom = std::max<uint64_t>(
        1ULL * 1024ULL * 1024ULL * 1024ULL,
        limits.tmpdir_free_bytes / 10);
    if (m_reserved_native_tmpdir_bytes > limits.tmpdir_free_bytes ||
        tmpdir_bytes >
            limits.tmpdir_free_bytes - m_reserved_native_tmpdir_bytes ||
        tmpdir_headroom > limits.tmpdir_free_bytes -
                              m_reserved_native_tmpdir_bytes - tmpdir_bytes) {
      return false;
    }
    if (!acquire_memory_locked(
            token, Preserve_trx_memory_kind::PROMOTION_BINLOG_NATIVE_CACHE,
            memory_bytes)) {
      return false;
    }
    m_reserved_native_fds += fd_count;
    m_reserved_native_tmpdir_bytes += tmpdir_bytes;
    m_native_fd_by_token[token] += fd_count;
    m_native_tmpdir_by_token[token] += tmpdir_bytes;
    return true;
  }

 public:
  void note_spill_bytes(uint64_t bytes) {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_spill_bytes += bytes;
  }

  void note_spill_failure() {
    std::lock_guard<std::mutex> guard(m_mutex);
    ++m_spill_failures;
  }

  ulonglong current_bytes() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    return static_cast<ulonglong>(m_current_bytes);
  }

  ulonglong peak_bytes() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    return static_cast<ulonglong>(m_peak_bytes);
  }

  ulonglong spill_bytes() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    return static_cast<ulonglong>(m_spill_bytes);
  }

  ulonglong spill_failures() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    return static_cast<ulonglong>(m_spill_failures);
  }

  uint64_t kind_current_bytes(Preserve_trx_memory_kind kind) const {
    std::lock_guard<std::mutex> guard(m_mutex);
    const size_t kind_index = preserve_memory_kind_index(kind);
    return kind_index < kPreserveMemoryKindCount ? m_by_kind[kind_index] : 0;
  }

  void reset_for_unit_test() {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_current_bytes = 0;
    m_peak_bytes = 0;
    m_spill_bytes = 0;
    m_spill_failures = 0;
    m_by_token.clear();
    m_by_token_kind.clear();
    m_by_kind.fill(0);
    m_reserved_native_fds = 0;
    m_reserved_native_tmpdir_bytes = 0;
    m_native_fd_by_token.clear();
    m_native_tmpdir_by_token.clear();
  }

 private:
  mutable std::mutex m_mutex;
  uint64_t m_current_bytes{0};
  uint64_t m_peak_bytes{0};
  uint64_t m_spill_bytes{0};
  uint64_t m_spill_failures{0};
  std::map<std::string, uint64_t> m_by_token;
  std::map<Token_kind_key, uint64_t> m_by_token_kind;
  std::array<uint64_t, kPreserveMemoryKindCount> m_by_kind{};
  uint64_t m_reserved_native_fds{0};
  uint64_t m_reserved_native_tmpdir_bytes{0};
  std::map<std::string, uint64_t> m_native_fd_by_token;
  std::map<std::string, uint64_t> m_native_tmpdir_by_token;
#ifndef NDEBUG
  Preserve_trx_external_resource_limits m_external_limits;
  bool m_external_limits_override{false};
#endif
};

Preserve_resource_manager g_preserve_resource_manager;

int show_preserve_trx_ulonglong_status(ulonglong value, SHOW_VAR *var,
                                       char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  *((long long *)buf) = static_cast<long long>(value);
  return 0;
}

}  // namespace

bool preserve_trx_sysvar_check_enable(sys_var *, THD *, set_var *var) {
  const bool requested_enabled = var->save_result.ulonglong_value != 0;
  if (requested_enabled != preserve_trx_is_enabled()) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0),
             "preserve_trx_enable is a startup-only option");
    return true;
  }
  return false;
}

bool preserve_trx_sysvar_update_enable(sys_var *, THD *, enum_var_type) {
  preserve_trx_set_enable_value(preserve_trx_enable);
  return false;
}

#define DEFINE_PRESERVE_TRX_SHOW_FUNC(name, status_expr)        \
  int name(THD *, SHOW_VAR *var, char *buf) {                   \
    return show_preserve_trx_ulonglong_status(status_expr, var, \
                                              buf);             \
  }

DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_warmcopy_prefix_bytes,
    preserve_trx_warmcopy_prefix_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_warmcopy_digest_bytes,
    preserve_trx_warmcopy_digest_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_warmcopy_durable_bytes,
    preserve_trx_warmcopy_durable_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_warmcopy_provider_full_copy_to_count,
    preserve_trx_warmcopy_provider_full_copy_to_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_warmcopy_phase2_pause_us,
    preserve_trx_warmcopy_phase2_pause_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_lock_warmcopy_attempts,
    preserve_trx_lock_warmcopy_attempts_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_lock_warmcopy_artifact_bytes,
    preserve_trx_lock_warmcopy_artifact_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_lock_warmcopy_canonical_mismatch,
    preserve_trx_lock_warmcopy_canonical_mismatch_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_lock_warmcopy_conversion_freeze_waits,
    preserve_trx_lock_warmcopy_conversion_freeze_waits_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_lock_warmcopy_dirty_shards,
    preserve_trx_lock_warmcopy_dirty_shards_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_lock_warmcopy_final_fence_mismatch,
    preserve_trx_lock_warmcopy_final_fence_mismatch_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_lock_warmcopy_journal_bytes,
    preserve_trx_lock_warmcopy_journal_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_lock_warmcopy_live_fallback,
    preserve_trx_lock_warmcopy_live_fallback_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_phase2_total_us,
                              preserve_trx_phase2_total_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_phase2_target_wait_us,
                              preserve_trx_phase2_target_wait_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_phase2_participant_prepare_us,
    preserve_trx_phase2_participant_prepare_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_phase2_participant_close_us,
    preserve_trx_phase2_participant_close_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_phase2_participant_preflight_us,
    preserve_trx_phase2_participant_preflight_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_phase2_lock_seal_us,
                              preserve_trx_phase2_lock_seal_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_phase2_target_preserve_us,
                              preserve_trx_phase2_target_preserve_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_phase2_lock_preflight_us,
                              preserve_trx_phase2_lock_preflight_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_phase2_prepare_us,
                              preserve_trx_phase2_prepare_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_phase2_detach_claim_us,
                              preserve_trx_phase2_detach_claim_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_phase2_snapshot_write_us,
                              preserve_trx_phase2_snapshot_write_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_phase2_register_us,
                              preserve_trx_phase2_register_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_phase2_slo_miss_count,
                              preserve_trx_phase2_slo_miss_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_resume_total_us,
                              preserve_trx_resume_total_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_elapsed_us,
    preserve_trx_startup_recovery_elapsed_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_error,
    preserve_trx_startup_recovery_error_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_snapshot_tokens,
    preserve_trx_startup_recovery_snapshot_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_local_snapshot_tokens,
    preserve_trx_startup_recovery_local_snapshot_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_binlog_cache_tokens,
    preserve_trx_startup_recovery_binlog_cache_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_tainted_tokens,
    preserve_trx_startup_recovery_tainted_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_standby_pending_tokens,
    preserve_trx_startup_recovery_standby_pending_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_promotion_intent_tokens,
    preserve_trx_startup_recovery_promotion_intent_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_orphan_rollback_count,
    preserve_trx_startup_recovery_orphan_rollback_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_resurrection_index_candidates,
    preserve_trx_startup_resurrection_index_candidates_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_resurrection_index_hits,
    preserve_trx_startup_resurrection_index_hits_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_resurrection_index_fallbacks,
    preserve_trx_startup_resurrection_index_fallbacks_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_resurrection_undo_anchor_checks,
    preserve_trx_startup_resurrection_undo_anchor_checks_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_resurrection_undo_body_pages,
    preserve_trx_startup_resurrection_undo_body_pages_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_resurrection_undo_body_records,
    preserve_trx_startup_resurrection_undo_body_records_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_load_us,
    preserve_trx_startup_recovery_phase_snapshot_load_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_validate_us,
    preserve_trx_startup_recovery_phase_snapshot_validate_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_kernel_us,
    preserve_trx_startup_recovery_phase_snapshot_kernel_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_claim_us,
    preserve_trx_startup_recovery_phase_snapshot_claim_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_read_view_us,
    preserve_trx_startup_recovery_phase_snapshot_read_view_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_table_locks_us,
    preserve_trx_startup_recovery_phase_snapshot_table_locks_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_record_locks_us,
    preserve_trx_startup_recovery_phase_snapshot_record_locks_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_record_lock_entries,
    preserve_trx_startup_recovery_phase_snapshot_record_lock_entries_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_record_lock_stable_page_hits,
    preserve_trx_startup_recovery_phase_snapshot_record_lock_stable_page_hits_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_record_lock_image_resolves,
    preserve_trx_startup_recovery_phase_snapshot_record_lock_image_resolves_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_pages,
    preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_pages_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_bits,
    preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_bits_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_us,
    preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_count,
    preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_record_lock_table_open_us,
    preserve_trx_startup_recovery_phase_snapshot_record_lock_table_open_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_pages,
    preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_pages_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_bytes,
    preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_residency_pages,
    preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_residency_pages_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_resident_pages,
    preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_resident_pages_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_io_pending_pages,
    preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_io_pending_pages_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_missing_pages,
    preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_missing_pages_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_predicate_locks_us,
    preserve_trx_startup_recovery_phase_snapshot_predicate_locks_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_mdl_us,
    preserve_trx_startup_recovery_phase_snapshot_mdl_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_startup_recovery_phase_snapshot_register_us,
    preserve_trx_startup_recovery_phase_snapshot_register_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_elapsed_us,
    preserve_trx_promotion_gate_elapsed_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_token_count,
    preserve_trx_promotion_gate_token_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_adopted_count,
    preserve_trx_promotion_gate_adopted_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_abandoned_count,
    preserve_trx_promotion_gate_abandoned_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_skipped_count,
    preserve_trx_promotion_gate_skipped_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_max_worker_elapsed_us,
    preserve_trx_promotion_gate_max_worker_elapsed_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_p50_worker_elapsed_us,
    preserve_trx_promotion_gate_p50_worker_elapsed_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_p95_worker_elapsed_us,
    preserve_trx_promotion_gate_p95_worker_elapsed_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_status_code,
    preserve_trx_promotion_gate_status_code_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_record_lock_page_count,
    preserve_trx_promotion_gate_record_lock_page_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_record_lock_resident_pages,
    preserve_trx_promotion_gate_record_lock_resident_pages_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_record_lock_cold_page_gets,
    preserve_trx_promotion_gate_record_lock_cold_page_gets_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_ready_cache_miss_count,
    preserve_trx_promotion_gate_ready_cache_miss_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_over_budget_count,
    preserve_trx_promotion_gate_over_budget_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_worker_count,
    preserve_trx_promotion_gate_worker_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_worker_active_count,
    preserve_trx_promotion_gate_worker_active_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_gate_worker_idle_count,
    preserve_trx_promotion_gate_worker_idle_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_prepared_registered_tokens,
    preserve_trx_promotion_prepared_registered_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_prepared_prewarm_pending_tokens,
    preserve_trx_promotion_prepared_prewarm_pending_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_prepared_ready_tokens,
    preserve_trx_promotion_prepared_ready_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_prepared_adopting_tokens,
    preserve_trx_promotion_prepared_adopting_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_prepared_adopted_tokens,
    preserve_trx_promotion_prepared_adopted_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_prepared_tainted_tokens,
    preserve_trx_promotion_prepared_tainted_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_prewarm_record_lock_page_count,
    preserve_trx_promotion_prewarm_record_lock_page_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_prewarm_record_lock_resident_pages,
    preserve_trx_promotion_prewarm_record_lock_resident_pages_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_prewarm_record_lock_cold_page_gets,
    preserve_trx_promotion_prewarm_record_lock_cold_page_gets_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_resume_core_elapsed_us,
    preserve_trx_promotion_resume_core_elapsed_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_resume_core_count,
    preserve_trx_promotion_resume_core_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_resume_core_p50_us,
    preserve_trx_promotion_resume_core_p50_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_resume_core_p95_us,
    preserve_trx_promotion_resume_core_p95_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_resume_core_p99_us,
    preserve_trx_promotion_resume_core_p99_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_resume_core_max_us,
    preserve_trx_promotion_resume_core_max_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_resume_failure_count,
    preserve_trx_promotion_resume_failure_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_fence_lease_wait_us,
    preserve_trx_promotion_fence_lease_wait_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_fence_digest_compare_us,
    preserve_trx_promotion_fence_digest_compare_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_fence_revalidate_us,
    preserve_trx_promotion_fence_revalidate_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_lock_page_get_count,
    preserve_trx_promotion_lock_page_get_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_promotion_lock_page_get_us,
                              preserve_trx_promotion_lock_page_get_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_lock_image_resolves,
    preserve_trx_promotion_lock_image_resolves_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_promotion_lock_apply_us,
                              preserve_trx_promotion_lock_apply_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_lock_accounting_bits,
    preserve_trx_promotion_lock_accounting_bits_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_receiver_lock_plan_capacity_bytes,
    preserve_trx_receiver_lock_plan_capacity_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_receiver_lock_plan_epoch_peak_bytes,
    preserve_trx_receiver_lock_plan_epoch_peak_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_receiver_lock_plan_subpool_cap_bytes,
    preserve_trx_receiver_lock_plan_subpool_cap_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_resource_admission_open_failed_count,
    preserve_trx_resource_admission_open_failed_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_resume_binlog_payload_read_bytes,
    preserve_trx_resume_binlog_payload_read_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_resume_binlog_payload_write_bytes,
    preserve_trx_resume_binlog_payload_write_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_resume_binlog_rename_count,
                              preserve_trx_resume_binlog_rename_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_resume_binlog_attach_count,
                              preserve_trx_resume_binlog_attach_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_resume_physical_consistency_mode,
    preserve_trx_resume_physical_consistency_mode_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_resume_real_redo_apply,
                              preserve_trx_resume_real_redo_apply_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_throttled_milliseconds,
    preserve_trx_transfer_throttled_milliseconds_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_last_throttle_reason,
    preserve_trx_transfer_last_throttle_reason_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_queued_bytes,
    preserve_trx_transfer_receiver_queued_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_worker_active,
    preserve_trx_transfer_receiver_worker_active_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_worker_idle,
    preserve_trx_transfer_receiver_worker_idle_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_inflight_tokens,
    preserve_trx_transfer_receiver_inflight_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_inflight_bytes,
    preserve_trx_transfer_receiver_inflight_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_saved_online_tokens,
    preserve_trx_transfer_receiver_saved_online_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_failed_tokens,
    preserve_trx_transfer_receiver_failed_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_last_failed_token,
    preserve_trx_transfer_receiver_last_failed_token_status())

int show_preserve_trx_transfer_receiver_last_failed_reason(THD *,
                                                           SHOW_VAR *var,
                                                           char *buf) {
  const std::string reason =
      preserve_trx_transfer_receiver_last_failed_reason_status();
  var->type = SHOW_CHAR;
  var->value = buf;
  strmake(buf, reason.c_str(), SHOW_VAR_FUNC_BUFF_SIZE - 1);
  return 0;
}
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_auto_prewarm_tokens,
    preserve_trx_transfer_receiver_auto_prewarm_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_auto_prewarm_ready_tokens,
    preserve_trx_transfer_receiver_auto_prewarm_ready_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens,
    preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_auto_prewarm_last_status,
    preserve_trx_transfer_receiver_auto_prewarm_last_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_ready_monotonic_us,
    preserve_trx_transfer_receiver_ready_monotonic_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_first_frame_monotonic_us,
    preserve_trx_transfer_receiver_first_frame_monotonic_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_last_object_seal_monotonic_us,
    preserve_trx_transfer_receiver_last_object_seal_monotonic_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_prewarm_start_monotonic_us,
    preserve_trx_transfer_receiver_prewarm_start_monotonic_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_prewarm_end_monotonic_us,
    preserve_trx_transfer_receiver_prewarm_end_monotonic_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_seal_prewarm_tokens,
    preserve_trx_transfer_receiver_seal_prewarm_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_seal_prewarm_success_tokens,
    preserve_trx_transfer_receiver_seal_prewarm_success_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_seal_prewarm_not_ready_tokens,
    preserve_trx_transfer_receiver_seal_prewarm_not_ready_tokens_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_seal_prewarm_last_status,
    preserve_trx_transfer_receiver_seal_prewarm_last_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_object_prewarm_proof_count,
    preserve_trx_transfer_receiver_object_prewarm_proof_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_object_prewarm_miss_count,
    preserve_trx_transfer_receiver_object_prewarm_miss_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_object_prewarm_count,
    preserve_trx_transfer_receiver_object_prewarm_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_object_prewarm_us,
    preserve_trx_transfer_receiver_object_prewarm_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_object_prewarm_max_us,
    preserve_trx_transfer_receiver_object_prewarm_max_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_object_prewarm_first_start_monotonic_us,
    preserve_trx_transfer_receiver_object_prewarm_first_start_monotonic_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_object_prewarm_last_end_monotonic_us,
    preserve_trx_transfer_receiver_object_prewarm_last_end_monotonic_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_record_object_prewarm_count,
    preserve_trx_transfer_receiver_record_object_prewarm_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_record_object_prewarm_us,
    preserve_trx_transfer_receiver_record_object_prewarm_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_record_object_prewarm_max_us,
    preserve_trx_transfer_receiver_record_object_prewarm_max_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_strict_record_index_page_reads,
    preserve_trx_transfer_receiver_strict_record_index_page_reads_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_strict_ibuf_merges,
    preserve_trx_transfer_receiver_strict_ibuf_merges_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_strict_target_local_redo_bytes,
    preserve_trx_transfer_receiver_strict_target_local_redo_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_record_object_prewarm_first_start_monotonic_us,
    preserve_trx_transfer_receiver_record_object_prewarm_first_start_monotonic_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_record_object_prewarm_last_end_monotonic_us,
    preserve_trx_transfer_receiver_record_object_prewarm_last_end_monotonic_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_binlog_object_prewarm_first_start_monotonic_us,
    preserve_trx_transfer_receiver_binlog_object_prewarm_first_start_monotonic_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_binlog_object_prewarm_last_end_monotonic_us,
    preserve_trx_transfer_receiver_binlog_object_prewarm_last_end_monotonic_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_committed_epoch_fallback_count,
    preserve_trx_transfer_receiver_committed_epoch_fallback_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_staged_token_ready_cache_us,
    preserve_trx_transfer_receiver_staged_token_ready_cache_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_staged_token_total_us,
    preserve_trx_transfer_receiver_staged_token_total_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_staged_token_max_us,
    preserve_trx_transfer_receiver_staged_token_max_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_staged_token_active,
    preserve_trx_transfer_receiver_staged_token_active_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_staged_token_max_active,
    preserve_trx_transfer_receiver_staged_token_max_active_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_projection_publish_count,
    preserve_trx_transfer_receiver_projection_publish_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_projection_publish_us,
    preserve_trx_transfer_receiver_projection_publish_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_projection_publish_max_us,
    preserve_trx_transfer_receiver_projection_publish_max_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_projection_publish_p95_us,
    preserve_trx_transfer_receiver_projection_publish_p95_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_projection_lock_wait_us,
    preserve_trx_transfer_receiver_projection_lock_wait_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_projection_store_write_us,
    preserve_trx_transfer_receiver_projection_store_write_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_projection_marker_write_us,
    preserve_trx_transfer_receiver_projection_marker_write_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_projection_snapshot_write_us,
    preserve_trx_transfer_receiver_projection_snapshot_write_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_projection_external_blob_us,
    preserve_trx_transfer_receiver_projection_external_blob_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_projection_encode_us,
    preserve_trx_transfer_receiver_projection_encode_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_projection_token_state_us,
    preserve_trx_transfer_receiver_projection_token_state_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_epoch_ready_bind_attempts,
    preserve_trx_transfer_receiver_epoch_ready_bind_attempts_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_transfer_phase2_bulk_bytes,
                              preserve_trx_transfer_phase2_bulk_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase2_final_metadata_fsync_count,
    preserve_trx_transfer_phase2_final_metadata_fsync_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase2_final_metadata_ack_us,
    preserve_trx_transfer_phase2_final_metadata_ack_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase1_frame_count,
    preserve_trx_transfer_phase1_frame_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase1_network_send_count,
    preserve_trx_transfer_phase1_network_send_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase1_batch_count,
    preserve_trx_transfer_phase1_batch_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase1_batch_bytes_p50,
    preserve_trx_transfer_phase1_batch_bytes_p50_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase1_batch_bytes_p95,
    preserve_trx_transfer_phase1_batch_bytes_p95_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase1_batch_bytes_max,
    preserve_trx_transfer_phase1_batch_bytes_max_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase1_batch_tokens_p50,
    preserve_trx_transfer_phase1_batch_tokens_p50_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase1_batch_tokens_p95,
    preserve_trx_transfer_phase1_batch_tokens_p95_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase1_batch_tokens_max,
    preserve_trx_transfer_phase1_batch_tokens_max_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase1_record_batch_tokens_avg,
    preserve_trx_transfer_phase1_record_batch_tokens_avg_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase1_batch_linger_us_p95,
    preserve_trx_transfer_phase1_batch_linger_us_p95_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase1_batch_linger_us_max,
    preserve_trx_transfer_phase1_batch_linger_us_max_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase1_oversize_token_count,
    preserve_trx_transfer_phase1_oversize_token_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase1_record_first_batch_send_us,
    preserve_trx_transfer_phase1_record_first_batch_send_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_phase1_record_last_batch_send_us,
    preserve_trx_transfer_phase1_record_last_batch_send_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_ready_after_final_metadata_us,
    preserve_trx_transfer_receiver_ready_after_final_metadata_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_final_spool_ack_monotonic_us,
    preserve_trx_transfer_receiver_final_spool_ack_monotonic_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_ready_after_final_spool_ack_us,
    preserve_trx_transfer_receiver_ready_after_final_spool_ack_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_prewarm_backlog_at_phase2_end,
    preserve_trx_transfer_receiver_prewarm_backlog_at_phase2_end_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_record_lock_required_residency_bytes,
    preserve_trx_transfer_receiver_record_lock_required_residency_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_transfer_receiver_record_lock_reserved_residency_bytes,
    preserve_trx_transfer_receiver_record_lock_reserved_residency_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_lock_warmcopy_phase2_pause_us,
    preserve_trx_lock_warmcopy_phase2_pause_us_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_lock_warmcopy_spill_bytes,
                              preserve_trx_lock_warmcopy_spill_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_lock_warmcopy_spill_failures,
    preserve_trx_lock_warmcopy_spill_failures_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_lock_warmcopy_resource_limit,
    preserve_trx_lock_warmcopy_resource_limit_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_lock_warmcopy_sealed_invalid,
                              preserve_trx_lock_warmcopy_sealed_invalid_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_lock_warmcopy_sealed_valid,
                              preserve_trx_lock_warmcopy_sealed_valid_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_lock_warmcopy_strict_reject,
                              preserve_trx_lock_warmcopy_strict_reject_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_lock_warmcopy_unsupported_family,
    preserve_trx_lock_warmcopy_unsupported_family_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_memory_current_bytes,
                              preserve_trx_memory_current_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_memory_peak_bytes,
                              preserve_trx_memory_peak_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_spill_bytes,
                              preserve_trx_spill_bytes_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(show_preserve_trx_spill_failures,
                              preserve_trx_spill_failures_status())

#undef DEFINE_PRESERVE_TRX_SHOW_FUNC

Preserve_memory_lease::Preserve_memory_lease(
    std::string token, Preserve_trx_memory_kind kind, uint64_t bytes,
    bool acquired)
    : m_token(std::move(token)),
      m_kind(kind),
      m_bytes(bytes),
      m_acquired(acquired) {}

Preserve_memory_lease::Preserve_memory_lease(
    Preserve_memory_lease &&other) noexcept
    : m_token(std::move(other.m_token)),
      m_kind(other.m_kind),
      m_bytes(other.m_bytes),
      m_acquired(other.m_acquired) {
  other.m_bytes = 0;
  other.m_acquired = false;
}

Preserve_memory_lease &Preserve_memory_lease::operator=(
    Preserve_memory_lease &&other) noexcept {
  if (this != &other) {
    release();
    m_token = std::move(other.m_token);
    m_kind = other.m_kind;
    m_bytes = other.m_bytes;
    m_acquired = other.m_acquired;
    other.m_bytes = 0;
    other.m_acquired = false;
  }
  return *this;
}

Preserve_memory_lease::~Preserve_memory_lease() { release(); }

void Preserve_memory_lease::release() {
  if (!m_acquired) return;
  g_preserve_resource_manager.release(m_token, m_kind, m_bytes);
  m_bytes = 0;
  m_acquired = false;
}

Preserve_memory_lease preserve_trx_acquire_memory_lease(
    const std::string &token, Preserve_trx_memory_kind kind, uint64_t bytes) {
  if (!g_preserve_resource_manager.acquire(token, kind, bytes)) {
    return Preserve_memory_lease(token, kind, bytes, false);
  }
  return Preserve_memory_lease(token, kind, bytes, true);
}

Preserve_native_binlog_resource_lease::
    Preserve_native_binlog_resource_lease(std::string token,
                                          uint64_t memory_bytes,
                                          uint64_t fd_count,
                                          uint64_t tmpdir_bytes, bool acquired)
    : m_token(std::move(token)),
      m_memory_bytes(memory_bytes),
      m_fd_count(fd_count),
      m_tmpdir_bytes(tmpdir_bytes),
      m_acquired(acquired) {}

Preserve_native_binlog_resource_lease::
    Preserve_native_binlog_resource_lease(
        Preserve_native_binlog_resource_lease &&other) noexcept
    : m_token(std::move(other.m_token)),
      m_memory_bytes(other.m_memory_bytes),
      m_fd_count(other.m_fd_count),
      m_tmpdir_bytes(other.m_tmpdir_bytes),
      m_acquired(other.m_acquired) {
  other.m_memory_bytes = 0;
  other.m_fd_count = 0;
  other.m_tmpdir_bytes = 0;
  other.m_acquired = false;
}

Preserve_native_binlog_resource_lease &
Preserve_native_binlog_resource_lease::operator=(
    Preserve_native_binlog_resource_lease &&other) noexcept {
  if (this == &other) return *this;
  release();
  m_token = std::move(other.m_token);
  m_memory_bytes = other.m_memory_bytes;
  m_fd_count = other.m_fd_count;
  m_tmpdir_bytes = other.m_tmpdir_bytes;
  m_acquired = other.m_acquired;
  other.m_memory_bytes = 0;
  other.m_fd_count = 0;
  other.m_tmpdir_bytes = 0;
  other.m_acquired = false;
  return *this;
}

Preserve_native_binlog_resource_lease::
    ~Preserve_native_binlog_resource_lease() {
  release();
}

void Preserve_native_binlog_resource_lease::release() {
  if (!m_acquired) return;
  g_preserve_resource_manager.release_native_binlog(
      m_token, m_memory_bytes, m_fd_count, m_tmpdir_bytes);
  m_memory_bytes = 0;
  m_fd_count = 0;
  m_tmpdir_bytes = 0;
  m_acquired = false;
}

Preserve_native_binlog_resource_lease
preserve_trx_acquire_native_binlog_resource_lease(
    const std::string &token, uint64_t memory_bytes, uint64_t fd_count,
    uint64_t tmpdir_bytes) {
  const bool acquired = g_preserve_resource_manager.acquire_native_binlog(
      token, memory_bytes, fd_count, tmpdir_bytes);
  return Preserve_native_binlog_resource_lease(
      token, memory_bytes, fd_count, tmpdir_bytes, acquired);
}

bool preserve_trx_resource_acquire_memory(const std::string &token,
                                          Preserve_trx_memory_kind kind,
                                          uint64_t bytes) {
  return g_preserve_resource_manager.acquire(token, kind, bytes);
}

void preserve_trx_resource_release_memory(const std::string &token,
                                          Preserve_trx_memory_kind kind,
                                          uint64_t bytes) {
  g_preserve_resource_manager.release(token, kind, bytes);
}

void preserve_trx_resource_note_spill_bytes(uint64_t bytes) {
  g_preserve_resource_manager.note_spill_bytes(bytes);
}

void preserve_trx_resource_note_spill_failure() {
  g_preserve_resource_manager.note_spill_failure();
}

ulonglong preserve_trx_memory_current_bytes_status() {
  return g_preserve_resource_manager.current_bytes();
}

ulonglong preserve_trx_memory_peak_bytes_status() {
  return g_preserve_resource_manager.peak_bytes();
}

ulonglong preserve_trx_spill_bytes_status() {
  return g_preserve_resource_manager.spill_bytes();
}

ulonglong preserve_trx_spill_failures_status() {
  return g_preserve_resource_manager.spill_failures();
}

void preserve_trx_resource_manager_reset_for_unit_test() {
  g_preserve_resource_manager.reset_for_unit_test();
}

void preserve_trx_resource_manager_set_limits_for_unit_test(
    const Preserve_trx_resource_limits &limits) {
  preserve_trx_memory_budget_bytes =
      static_cast<ulonglong>(limits.global_memory_budget_bytes);
  preserve_trx_memory_per_token_bytes =
      static_cast<ulonglong>(limits.per_token_memory_budget_bytes);
}

uint64_t preserve_trx_resource_kind_current_bytes(
    Preserve_trx_memory_kind kind) {
  return g_preserve_resource_manager.kind_current_bytes(kind);
}

uint64_t preserve_trx_resource_kind_current_bytes_for_unit_test(
    Preserve_trx_memory_kind kind) {
  return preserve_trx_resource_kind_current_bytes(kind);
}

uint64_t preserve_trx_resource_kind_cap_bytes(
    Preserve_trx_memory_kind kind) {
  return preserve_memory_kind_cap(kind, preserve_trx_memory_budget_bytes);
}

uint64_t preserve_trx_resource_kind_cap_bytes_for_unit_test(
    Preserve_trx_memory_kind kind) {
  return preserve_trx_resource_kind_cap_bytes(kind);
}

#ifndef NDEBUG
void preserve_trx_resource_manager_set_external_limits_for_unit_test(
    const Preserve_trx_external_resource_limits &limits) {
  g_preserve_resource_manager.set_external_limits_for_unit_test(limits);
}

uint64_t preserve_trx_native_binlog_reserved_fd_count_for_unit_test() {
  return g_preserve_resource_manager.reserved_native_fds_for_unit_test();
}

uint64_t
preserve_trx_native_binlog_reserved_tmpdir_bytes_for_unit_test() {
  return g_preserve_resource_manager
      .reserved_native_tmpdir_bytes_for_unit_test();
}
#endif
