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

#ifndef SQL_PRESERVE_TRX_RESOURCE_INCLUDED
#define SQL_PRESERVE_TRX_RESOURCE_INCLUDED

#include <cstdint>
#include <string>

#include "my_inttypes.h"
#include "mysql/status_var.h"
#include "sql/set_var.h"

class THD;
class sys_var;
class set_var;

extern ulonglong preserve_trx_memory_budget_bytes;
extern ulonglong preserve_trx_memory_per_token_bytes;
extern uint preserve_trx_spill_chunk_bytes;

enum class Preserve_trx_memory_kind {
  /* Temporary-table physical image streaming buffer. */
  TEMP_IMAGE_STREAM_BUFFER,
  /* In-memory dirty-page queue before sidecar sealing. */
  TEMP_DIRTY_PAGE_QUEUE,
  /* Buffer used while reading temp-table sidecars during resume. */
  TEMP_SIDECAR_READ_BUFFER,
  /*
    Reserved for a future binlog warmcopy heap lease. Current binlog warmcopy
    capacity accounting is the warm external blob byte budget, not this resource
    manager kind.
  */
  BINLOG_WARMCOPY_BUFFER,
  /* Snapshot encode/decode working memory. */
  SNAPSHOT_CODEC_BUFFER
};

struct Preserve_trx_resource_limits {
  uint64_t global_memory_budget_bytes{256ULL * 1024ULL * 1024ULL};
  uint64_t per_token_memory_budget_bytes{64ULL * 1024ULL * 1024ULL};
};

class Preserve_memory_lease {
 public:
  Preserve_memory_lease() = default;
  Preserve_memory_lease(const Preserve_memory_lease &) = delete;
  Preserve_memory_lease &operator=(const Preserve_memory_lease &) = delete;
  Preserve_memory_lease(Preserve_memory_lease &&other) noexcept;
  Preserve_memory_lease &operator=(Preserve_memory_lease &&other) noexcept;
  ~Preserve_memory_lease();

  bool acquired() const { return m_acquired; }
  uint64_t bytes() const { return m_bytes; }
  void release();

 private:
  Preserve_memory_lease(std::string token, Preserve_trx_memory_kind kind,
                        uint64_t bytes, bool acquired);

  std::string m_token;
  Preserve_trx_memory_kind m_kind{
      Preserve_trx_memory_kind::SNAPSHOT_CODEC_BUFFER};
  uint64_t m_bytes{0};
  bool m_acquired{false};

  friend Preserve_memory_lease preserve_trx_acquire_memory_lease(
      const std::string &token, Preserve_trx_memory_kind kind, uint64_t bytes);
};

/*
  Acquire a token/kind scoped memory lease. The returned RAII object releases on
  destruction; an empty token or over-budget request returns an unacquired lease.
*/
Preserve_memory_lease preserve_trx_acquire_memory_lease(
    const std::string &token, Preserve_trx_memory_kind kind, uint64_t bytes);

/* Manual acquire/release helpers for callers that cannot hold an RAII object. */
bool preserve_trx_resource_acquire_memory(const std::string &token,
                                          Preserve_trx_memory_kind kind,
                                          uint64_t bytes);
/*
  Release is tolerant of repeated or oversized release attempts and clips the
  token/kind accounting at zero so cleanup error paths can remain idempotent.
*/
void preserve_trx_resource_release_memory(const std::string &token,
                                          Preserve_trx_memory_kind kind,
                                          uint64_t bytes);

/* Process-lifetime spill counters exposed through SHOW STATUS. */
void preserve_trx_resource_note_spill_bytes(uint64_t bytes);
void preserve_trx_resource_note_spill_failure();

ulonglong preserve_trx_memory_current_bytes_status();
ulonglong preserve_trx_memory_peak_bytes_status();
ulonglong preserve_trx_spill_bytes_status();
ulonglong preserve_trx_spill_failures_status();

void preserve_trx_resource_manager_reset_for_unit_test();
void preserve_trx_resource_manager_set_limits_for_unit_test(
    const Preserve_trx_resource_limits &limits);

bool preserve_trx_sysvar_check_enable(sys_var *self, THD *thd, set_var *var);
bool preserve_trx_sysvar_update_enable(sys_var *self, THD *thd,
                                       enum_var_type type);

int show_preserve_trx_warmcopy_prefix_bytes(THD *thd, SHOW_VAR *var,
                                            char *buf);
int show_preserve_trx_warmcopy_digest_bytes(THD *thd, SHOW_VAR *var,
                                            char *buf);
int show_preserve_trx_warmcopy_durable_bytes(THD *thd, SHOW_VAR *var,
                                             char *buf);
int show_preserve_trx_warmcopy_provider_full_copy_to_count(THD *thd,
                                                           SHOW_VAR *var,
                                                           char *buf);
int show_preserve_trx_warmcopy_phase2_pause_us(THD *thd, SHOW_VAR *var,
                                               char *buf);
int show_preserve_trx_lock_warmcopy_attempts(THD *thd, SHOW_VAR *var,
                                             char *buf);
int show_preserve_trx_lock_warmcopy_artifact_bytes(THD *thd, SHOW_VAR *var,
                                                   char *buf);
int show_preserve_trx_lock_warmcopy_canonical_mismatch(THD *thd,
                                                       SHOW_VAR *var,
                                                       char *buf);
int show_preserve_trx_lock_warmcopy_conversion_freeze_waits(THD *thd,
                                                            SHOW_VAR *var,
                                                            char *buf);
int show_preserve_trx_lock_warmcopy_dirty_shards(THD *thd, SHOW_VAR *var,
                                                 char *buf);
int show_preserve_trx_lock_warmcopy_final_fence_mismatch(THD *thd,
                                                         SHOW_VAR *var,
                                                         char *buf);
int show_preserve_trx_lock_warmcopy_journal_bytes(THD *thd, SHOW_VAR *var,
                                                  char *buf);
int show_preserve_trx_lock_warmcopy_live_fallback(THD *thd, SHOW_VAR *var,
                                                  char *buf);
int show_preserve_trx_phase2_total_us(THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_phase2_target_wait_us(THD *thd, SHOW_VAR *var,
                                            char *buf);
int show_preserve_trx_phase2_participant_prepare_us(THD *thd, SHOW_VAR *var,
                                                    char *buf);
int show_preserve_trx_phase2_participant_close_us(THD *thd, SHOW_VAR *var,
                                                  char *buf);
int show_preserve_trx_phase2_participant_preflight_us(THD *thd, SHOW_VAR *var,
                                                      char *buf);
int show_preserve_trx_phase2_lock_seal_us(THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_phase2_target_preserve_us(THD *thd, SHOW_VAR *var,
                                                char *buf);
int show_preserve_trx_phase2_lock_preflight_us(THD *thd, SHOW_VAR *var,
                                               char *buf);
int show_preserve_trx_phase2_prepare_us(THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_phase2_detach_claim_us(THD *thd, SHOW_VAR *var,
                                             char *buf);
int show_preserve_trx_phase2_snapshot_write_us(THD *thd, SHOW_VAR *var,
                                               char *buf);
int show_preserve_trx_phase2_register_us(THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_phase2_slo_miss_count(THD *thd, SHOW_VAR *var,
                                            char *buf);
int show_preserve_trx_resume_total_us(THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_elapsed_us(THD *thd, SHOW_VAR *var,
                                                  char *buf);
int show_preserve_trx_startup_recovery_error(THD *thd, SHOW_VAR *var,
                                             char *buf);
int show_preserve_trx_startup_recovery_snapshot_tokens(THD *thd,
                                                       SHOW_VAR *var,
                                                       char *buf);
int show_preserve_trx_startup_recovery_local_snapshot_tokens(THD *thd,
                                                             SHOW_VAR *var,
                                                             char *buf);
int show_preserve_trx_startup_recovery_binlog_cache_tokens(THD *thd,
                                                           SHOW_VAR *var,
                                                           char *buf);
int show_preserve_trx_startup_recovery_tainted_tokens(THD *thd, SHOW_VAR *var,
                                                      char *buf);
int show_preserve_trx_startup_recovery_standby_pending_tokens(THD *thd,
                                                              SHOW_VAR *var,
                                                              char *buf);
int show_preserve_trx_startup_recovery_promotion_intent_tokens(THD *thd,
                                                               SHOW_VAR *var,
                                                               char *buf);
int show_preserve_trx_startup_recovery_orphan_rollback_count(THD *thd,
                                                             SHOW_VAR *var,
                                                             char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_load_us(THD *thd,
                                                              SHOW_VAR *var,
                                                              char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_validate_us(THD *thd,
                                                                  SHOW_VAR *var,
                                                                  char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_kernel_us(THD *thd,
                                                                SHOW_VAR *var,
                                                                char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_claim_us(THD *thd,
                                                               SHOW_VAR *var,
                                                               char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_read_view_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_table_locks_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_record_locks_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_record_lock_entries(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_record_lock_stable_page_hits(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_record_lock_image_resolves(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_pages(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_record_lock_bitmap_bits(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_record_lock_page_get_count(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_record_lock_table_open_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_pages(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_bytes(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_residency_pages(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_resident_pages(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_io_pending_pages(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_record_lock_prefetch_missing_pages(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_predicate_locks_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_mdl_us(THD *thd,
                                                             SHOW_VAR *var,
                                                             char *buf);
int show_preserve_trx_startup_recovery_phase_snapshot_register_us(THD *thd,
                                                                  SHOW_VAR *var,
                                                                  char *buf);
int show_preserve_trx_promotion_gate_elapsed_us(THD *thd, SHOW_VAR *var,
                                                char *buf);
int show_preserve_trx_promotion_gate_token_count(THD *thd, SHOW_VAR *var,
                                                 char *buf);
int show_preserve_trx_promotion_gate_adopted_count(THD *thd, SHOW_VAR *var,
                                                   char *buf);
int show_preserve_trx_promotion_gate_abandoned_count(THD *thd, SHOW_VAR *var,
                                                     char *buf);
int show_preserve_trx_promotion_gate_skipped_count(THD *thd, SHOW_VAR *var,
                                                   char *buf);
int show_preserve_trx_promotion_gate_max_worker_elapsed_us(THD *thd,
                                                           SHOW_VAR *var,
                                                           char *buf);
int show_preserve_trx_promotion_gate_p50_worker_elapsed_us(THD *thd,
                                                           SHOW_VAR *var,
                                                           char *buf);
int show_preserve_trx_promotion_gate_p95_worker_elapsed_us(THD *thd,
                                                           SHOW_VAR *var,
                                                           char *buf);
int show_preserve_trx_promotion_gate_status_code(THD *thd, SHOW_VAR *var,
                                                 char *buf);
int show_preserve_trx_promotion_gate_record_lock_page_count(THD *thd,
                                                            SHOW_VAR *var,
                                                            char *buf);
int show_preserve_trx_promotion_gate_record_lock_resident_pages(THD *thd,
                                                                SHOW_VAR *var,
                                                                char *buf);
int show_preserve_trx_promotion_gate_record_lock_cold_page_gets(THD *thd,
                                                                SHOW_VAR *var,
                                                                char *buf);
int show_preserve_trx_promotion_gate_ready_cache_miss_count(THD *thd,
                                                            SHOW_VAR *var,
                                                            char *buf);
int show_preserve_trx_promotion_gate_over_budget_count(THD *thd, SHOW_VAR *var,
                                                       char *buf);
int show_preserve_trx_promotion_prewarm_record_lock_page_count(THD *thd,
                                                               SHOW_VAR *var,
                                                               char *buf);
int show_preserve_trx_promotion_prewarm_record_lock_resident_pages(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_promotion_prewarm_record_lock_cold_page_gets(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_auto_prewarm_tokens(THD *thd,
                                                            SHOW_VAR *var,
                                                            char *buf);
int show_preserve_trx_transfer_receiver_auto_prewarm_ready_tokens(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_auto_prewarm_last_status(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_ready_monotonic_us(THD *thd,
                                                           SHOW_VAR *var,
                                                           char *buf);
int show_preserve_trx_transfer_receiver_first_frame_monotonic_us(THD *thd,
                                                                 SHOW_VAR *var,
                                                                 char *buf);
int show_preserve_trx_transfer_receiver_last_object_seal_monotonic_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_prewarm_start_monotonic_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_prewarm_end_monotonic_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_seal_prewarm_tokens(THD *thd,
                                                            SHOW_VAR *var,
                                                            char *buf);
int show_preserve_trx_transfer_receiver_seal_prewarm_success_tokens(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_seal_prewarm_not_ready_tokens(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_seal_prewarm_last_status(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_object_prewarm_proof_count(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_object_prewarm_miss_count(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_object_prewarm_count(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_object_prewarm_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_object_prewarm_max_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_record_object_prewarm_count(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_record_object_prewarm_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_record_object_prewarm_max_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_committed_epoch_fallback_count(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_staged_token_publish_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_staged_token_ready_cache_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_staged_token_total_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_staged_token_max_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_staged_token_active(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_staged_token_max_active(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_projection_publish_count(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_projection_publish_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_projection_publish_max_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_projection_publish_p95_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_projection_lock_wait_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_projection_store_write_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_projection_marker_write_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_projection_snapshot_write_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_projection_external_blob_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_projection_encode_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_projection_token_state_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_epoch_ready_bind_attempts(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_phase2_bulk_bytes(THD *thd, SHOW_VAR *var,
                                                 char *buf);
int show_preserve_trx_transfer_phase2_receiver_prewarm_wait_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_phase2_final_metadata_fsync_count(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_phase2_final_metadata_ack_us(THD *thd,
                                                            SHOW_VAR *var,
                                                            char *buf);
int show_preserve_trx_transfer_phase1_business_enqueue_block_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_ready_after_final_metadata_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_final_spool_ack_monotonic_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_ready_after_final_spool_ack_us(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_prewarm_backlog_at_phase2_end(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_record_lock_required_residency_bytes(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_transfer_receiver_record_lock_reserved_residency_bytes(
    THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_lock_warmcopy_phase2_pause_us(THD *thd, SHOW_VAR *var,
                                                    char *buf);
int show_preserve_trx_lock_warmcopy_spill_bytes(THD *thd, SHOW_VAR *var,
                                                char *buf);
int show_preserve_trx_lock_warmcopy_spill_failures(THD *thd, SHOW_VAR *var,
                                                   char *buf);
int show_preserve_trx_lock_warmcopy_resource_limit(THD *thd, SHOW_VAR *var,
                                                   char *buf);
int show_preserve_trx_lock_warmcopy_sealed_invalid(THD *thd, SHOW_VAR *var,
                                                   char *buf);
int show_preserve_trx_lock_warmcopy_sealed_valid(THD *thd, SHOW_VAR *var,
                                                 char *buf);
int show_preserve_trx_lock_warmcopy_strict_reject(THD *thd, SHOW_VAR *var,
                                                  char *buf);
int show_preserve_trx_lock_warmcopy_unsupported_family(THD *thd, SHOW_VAR *var,
                                                       char *buf);
int show_preserve_trx_memory_current_bytes(THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_memory_peak_bytes(THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_spill_bytes(THD *thd, SHOW_VAR *var, char *buf);
int show_preserve_trx_spill_failures(THD *thd, SHOW_VAR *var, char *buf);

#endif  // SQL_PRESERVE_TRX_RESOURCE_INCLUDED
