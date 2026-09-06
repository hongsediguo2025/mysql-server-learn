/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef SQL_PRESERVE_TRX_PHASE1_PIPELINE_INCLUDED
#define SQL_PRESERVE_TRX_PHASE1_PIPELINE_INCLUDED

#include <stdint.h>

#include <array>
#include <memory>
#include <string>

class THD;
class Preserve_trx_phase1_binlog_provider_port;
struct Preserve_trx_phase1_binlog_prepared_payload;
struct Preserve_trx_phase1_record_capture_payload;
struct Preserve_trx_phase1_record_prepared_payload;

using Preserve_trx_phase1_binlog_prepared_handle =
    std::shared_ptr<const Preserve_trx_phase1_binlog_prepared_payload>;
using Preserve_trx_phase1_record_capture_handle =
    std::shared_ptr<Preserve_trx_phase1_record_capture_payload>;
using Preserve_trx_phase1_record_prepared_handle =
    std::shared_ptr<const Preserve_trx_phase1_record_prepared_payload>;

enum class Preserve_trx_phase1_pipeline_lifecycle : uint8_t {
  STARTING,
  RUNNING,
  FINALIZING,
  CANCELING,
  STOPPED
};

enum class Preserve_trx_phase1_pipeline_family : uint8_t {
  RECORD_LOCK,
  BINLOG_CACHE
};

enum class Preserve_trx_phase1_pipeline_submit_status : uint8_t {
  ADMITTED,
  NOT_RUNNING,
  INVALID_DESCRIPTOR,
  SINGLE_FLIGHT,
  NO_SLOT,
  NO_CREDIT,
  DEADLINE
};

enum class Preserve_trx_phase1_pipeline_credit_status : uint8_t {
  GRANTED,
  NOT_ADMITTED,
  NO_CREDIT,
  DEADLINE,
  CANCELLED
};

enum class Preserve_trx_phase1_pipeline_operation_kind : uint8_t {
  NATIVE_WAIT_CAPABLE,
  NO_WAIT_CHECK
};

enum class Preserve_trx_phase1_pipeline_result_disposition : uint8_t {
  DROP,
  ABSENT,
  RETAINED
};

enum class Preserve_trx_phase1_pipeline_publication_status : uint8_t {
  OK,
  FAILED,
  ACK_UNCERTAIN,
  ABORTED
};

enum class Preserve_trx_phase1_pipeline_result_status : uint8_t {
  PREPARED,
  ABSENT,
  NO_PROGRESS,
  DEFERRED_TO_FINAL,
  RETRYABLE,
  IDENTITY_STALE,
  STORE_FALLBACK,
  DEADLINE,
  CANCELLED,
  RESOURCE_EXHAUSTED,
  UNSUPPORTED,
  ADAPTER_NOT_INSTALLED
};

struct Preserve_trx_phase1_pipeline_config {
  uint64_t attempt_id{0};
  uint64_t drain_generation{0};
  uint64_t phase1_started_us{0};
  uint64_t stage_deadline_us{0};
  uint64_t phase2_budget_us{0};
  uint32_t worker_count{0};
  uint32_t ordinary_active_limit{0};
  uint64_t credit_bytes{0};
  uint64_t record_reserve_bytes{0};
  uint64_t binlog_reserve_bytes{0};
  uint64_t copy_chunk_bytes{0};
  uint64_t cleanup_reserve_us{0};
  uint64_t native_wait_worst_case_us{1000000ULL};
  uint64_t no_wait_worst_case_us{50000ULL};
  uint64_t record_store_snapshot_worst_case_us{250000ULL};
  uint32_t result_slots{0};
  uint64_t tail_record_credit_bytes{0};
#ifndef DBUG_OFF
  bool debug_fail_worker_init{false};
#endif
};

/*
  Queue records are deliberately value-only. Native owners and borrowed
  pointers must be reacquired by the family adapter after admission.
*/
struct Preserve_trx_phase1_work_descriptor {
  uint64_t attempt_id{0};
  uint64_t drain_generation{0};
  uint64_t target_thread_id{0};
  uint64_t target_incarnation{0};
  uint64_t family_version{0};
  uint64_t source_owner_cookie{0};
  uint64_t source_object_cookie{0};
  uint64_t warmcopy_epoch{0};
  uint64_t expected_immutable_trx_id{0};
  uint64_t expected_trx_version{0};
  uint64_t capture_generation{0};
  uint64_t item_limit{0};
  uint64_t capture_byte_limit{0};
  uint64_t estimated_credit_bytes{0};
  uint64_t expected_store_baseline_generation{0};
  uint64_t expected_lock_coordinate_generation{0};
  /* Last acknowledged binlog prefix; never a locally queued watermark. */
  uint64_t binlog_prefix_size{0};
  uint64_t binlog_prefix_truncate_generation{0};
  std::array<unsigned char, 32> binlog_prefix_digest{};
  uint64_t binlog_minimum_delta_bytes{0};
  uint64_t binlog_wire_chunk_bytes{0};
  Preserve_trx_phase1_pipeline_family family{
      Preserve_trx_phase1_pipeline_family::RECORD_LOCK};
  bool final_generation{false};
  bool use_record_store_snapshot{false};
  bool binlog_prefix_progress{false};
};

struct Preserve_trx_phase1_prepared_result {
  uint64_t admission_id{0};
  uint64_t attempt_id{0};
  uint64_t drain_generation{0};
  uint64_t target_thread_id{0};
  uint64_t target_incarnation{0};
  uint64_t family_version{0};
  uint64_t logical_bytes{0};
  Preserve_trx_phase1_pipeline_family family{
      Preserve_trx_phase1_pipeline_family::RECORD_LOCK};
  Preserve_trx_phase1_pipeline_result_status status{
      Preserve_trx_phase1_pipeline_result_status::ADAPTER_NOT_INSTALLED};
  Preserve_trx_phase1_binlog_prepared_handle binlog_payload;
  Preserve_trx_phase1_record_prepared_handle record_payload;
  std::string reason;
};

struct Preserve_trx_phase1_pipeline_snapshot {
  Preserve_trx_phase1_pipeline_lifecycle lifecycle{
      Preserve_trx_phase1_pipeline_lifecycle::STOPPED};
  uint64_t event_revision{0};
  uint64_t queued{0};
  uint64_t admitted{0};
  uint64_t inflight{0};
  uint64_t result_count{0};
  uint64_t credit_in_use_bytes{0};
  uint64_t record_credit_in_use_bytes{0};
  uint64_t binlog_credit_in_use_bytes{0};
  uint64_t tail_record_credit_consumed_bytes{0};
  uint64_t cancel_revision{0};
  uint64_t operation_cutoff_us{0};
  uint64_t active_operation_permits{0};
  uint64_t no_wait_active_operations{0};
  uint64_t operation_permits_started{0};
  uint64_t operation_permit_budget_rejected{0};
  uint64_t operation_budget_overruns{0};
  uint64_t ordinary_binlog_slow_operations{0};
  uint64_t final_record_capture_operation_samples{0};
  uint64_t final_record_capture_operation_us_total{0};
  uint64_t final_record_capture_operation_us_max{0};
  uint64_t final_record_capture_operation_overruns{0};
  uint64_t final_record_store_snapshot_operation_samples{0};
  uint64_t final_record_store_snapshot_operation_us_total{0};
  uint64_t final_record_store_snapshot_operation_us_max{0};
  uint64_t final_record_store_snapshot_operation_overruns{0};
  uint64_t final_record_prepare_operation_samples{0};
  uint64_t final_record_prepare_operation_us_total{0};
  uint64_t final_record_prepare_operation_us_max{0};
  uint64_t final_record_prepare_operation_overruns{0};
  uint64_t record_capture_latch_samples{0};
  uint64_t record_capture_latch_wait_us{0};
  uint64_t record_capture_latch_hold_us{0};
  uint64_t record_capture_latch_envelope_us{0};
  uint64_t record_capture_latch_max_envelope_us{0};
  uint64_t effective_pipeline_credit_bytes{0};
  uint64_t effective_record_reserve_bytes{0};
  uint64_t effective_binlog_reserve_bytes{0};
  uint64_t publication_failures{0};
  uint64_t publication_ack_uncertain{0};
  uint64_t publication_aborted{0};
  uint64_t submit_no_slot{0};
  uint64_t submit_no_credit{0};
  uint64_t submit_deadline{0};
  uint64_t invariant_failures{0};
  uint64_t ordinary_active{0};
  uint64_t ordinary_active_high_water{0};
  uint64_t ordinary_active_limit_deferrals{0};
  uint32_t workers_configured{0};
  uint32_t ordinary_active_limit_requested{0};
  uint32_t ordinary_active_limit_effective{0};
  uint32_t workers_ready{0};
  uint32_t init_failures{0};
  bool sequencer_ready{false};
};

class Preserve_trx_phase1_pipeline {
 public:
  explicit Preserve_trx_phase1_pipeline(
      const Preserve_trx_phase1_pipeline_config &config,
      Preserve_trx_phase1_binlog_provider_port *binlog_provider = nullptr);
  ~Preserve_trx_phase1_pipeline();

  Preserve_trx_phase1_pipeline(const Preserve_trx_phase1_pipeline &) = delete;
  Preserve_trx_phase1_pipeline &operator=(
      const Preserve_trx_phase1_pipeline &) = delete;

  bool start();
  Preserve_trx_phase1_pipeline_submit_status try_submit(
      const Preserve_trx_phase1_work_descriptor &descriptor);
  Preserve_trx_phase1_pipeline_submit_status try_submit_final(
      const Preserve_trx_phase1_work_descriptor &descriptor);
  /*
    Single-consumer handoff to the attempt drain owner. The caller must route
    by family and either settle the result or begin publication.
  */
  bool try_pop_result(Preserve_trx_phase1_prepared_result *result);
  bool settle_result(
      uint64_t admission_id,
      Preserve_trx_phase1_pipeline_result_disposition disposition);
  bool begin_publication(uint64_t admission_id);
  bool settle_publication(
      uint64_t admission_id,
      Preserve_trx_phase1_pipeline_publication_status status);
  uint64_t abort_residual_publications_after_sender_join();
  bool publish_stage_deadline(uint64_t stage_started_us,
                              uint64_t stage_deadline_us);
  bool begin_finalizing();
  bool open_final_admission(uint64_t final_deadline_us);
  void close_final_admission();
  bool finish_and_join();
  void cancel();
  bool wait_for_change(uint64_t observed_revision, uint64_t timeout_us,
                       uint64_t *new_revision);
  bool join_while_draining();
  Preserve_trx_phase1_pipeline_snapshot snapshot() const;

#ifndef DBUG_OFF
  /* Deterministic debug-build exercise used by MTR before adapters exist. */
  bool debug_exercise_core();
  bool debug_exercise_record_adapter(THD *owner_thd, THD *target_thd,
                                     bool expect_empty);
#endif

 private:
  class Impl;
  std::unique_ptr<Impl> m_impl;
};

#endif  // SQL_PRESERVE_TRX_PHASE1_PIPELINE_INCLUDED
