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
#include <map>
#include <mutex>
#include <utility>

#include "my_sys.h"
#include "mysqld_error.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_lock_warmcopy.h"
#include "sql/preserve_trx_promotion.h"
#include "sql/preserve_trx_transfer.h"

ulonglong preserve_trx_memory_budget_bytes = 256ULL * 1024ULL * 1024ULL;
ulonglong preserve_trx_memory_per_token_bytes = 64ULL * 1024ULL * 1024ULL;
uint preserve_trx_spill_chunk_bytes = 4U * 1024U * 1024U;

namespace {

struct Token_kind_key {
  std::string token;
  Preserve_trx_memory_kind kind{Preserve_trx_memory_kind::SNAPSHOT_CODEC_BUFFER};

  bool operator<(const Token_kind_key &other) const {
    if (token != other.token) return token < other.token;
    return static_cast<int>(kind) < static_cast<int>(other.kind);
  }
};

class Preserve_resource_manager {
 public:
  bool acquire(const std::string &token, Preserve_trx_memory_kind kind,
               uint64_t bytes) {
    std::lock_guard<std::mutex> guard(m_mutex);
    if (token.empty()) return false;
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

    const uint64_t token_bytes = m_by_token[token];
    if (token_bytes > per_token_budget - bytes) return false;

    m_current_bytes += bytes;
    m_peak_bytes = std::max(m_peak_bytes, m_current_bytes);
    m_by_token[token] = token_bytes + bytes;
    m_by_token_kind[{token, kind}] += bytes;
    return true;
  }

  void release(const std::string &token, Preserve_trx_memory_kind kind,
               uint64_t bytes) {
    std::lock_guard<std::mutex> guard(m_mutex);
    if (token.empty()) return;
    /*
      Release is tolerant of oversized cleanup requests because failure paths
      may have already released part of the staging buffer. Counters are clipped
      at zero rather than turning cleanup into a second preserve failure.
    */
    if (bytes > m_current_bytes) {
      m_current_bytes = 0;
    } else {
      m_current_bytes -= bytes;
    }

    auto token_it = m_by_token.find(token);
    if (token_it != m_by_token.end()) {
      if (bytes >= token_it->second) {
        m_by_token.erase(token_it);
      } else {
        token_it->second -= bytes;
      }
    }

    Token_kind_key key{token, kind};
    auto kind_it = m_by_token_kind.find(key);
    if (kind_it != m_by_token_kind.end()) {
      if (bytes >= kind_it->second) {
        m_by_token_kind.erase(kind_it);
      } else {
        kind_it->second -= bytes;
      }
    }
  }

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

  void reset_for_unit_test() {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_current_bytes = 0;
    m_peak_bytes = 0;
    m_spill_bytes = 0;
    m_spill_failures = 0;
    m_by_token.clear();
    m_by_token_kind.clear();
  }

 private:
  mutable std::mutex m_mutex;
  uint64_t m_current_bytes{0};
  uint64_t m_peak_bytes{0};
  uint64_t m_spill_bytes{0};
  uint64_t m_spill_failures{0};
  std::map<std::string, uint64_t> m_by_token;
  std::map<Token_kind_key, uint64_t> m_by_token_kind;
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
    show_preserve_trx_promotion_prewarm_record_lock_page_count,
    preserve_trx_promotion_prewarm_record_lock_page_count_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_prewarm_record_lock_resident_pages,
    preserve_trx_promotion_prewarm_record_lock_resident_pages_status())
DEFINE_PRESERVE_TRX_SHOW_FUNC(
    show_preserve_trx_promotion_prewarm_record_lock_cold_page_gets,
    preserve_trx_promotion_prewarm_record_lock_cold_page_gets_status())
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
