/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation. */

#ifndef SQL_PRESERVE_TRX_PHASE1_OWNER_INCLUDED
#define SQL_PRESERVE_TRX_PHASE1_OWNER_INCLUDED

#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

class THD;
class Preserve_trx_lock_warmcopy_drain_participant;
class Preserve_trx_phase1_pipeline;
struct Preserve_trx_phase1_prepared_result;

struct Preserve_trx_phase1_owner_config {
  uint64_t attempt_id{0};
  uint64_t drain_generation{0};
  uint64_t record_item_limit{0};
  uint64_t record_capture_bytes{0};
  uint64_t record_initial_credit_bytes{0};
  uint64_t record_final_job_credit_bytes{0};
  bool external_result_demux{false};
};

enum class Preserve_trx_phase1_owner_pump_status : uint8_t {
  PROGRESS,
  IDLE,
  COMPLETE,
  FAILED
};

/*
  Drain-owner-only registry and cooperative pump. It stores only value
  identities; THD pins are reacquired for a bounded observation and are never
  retained across pipeline queue operations or waits.
*/
class Preserve_trx_phase1_owner {
 public:
  Preserve_trx_phase1_owner(
      const Preserve_trx_phase1_owner_config &config,
      Preserve_trx_phase1_pipeline *pipeline,
      Preserve_trx_lock_warmcopy_drain_participant *record_participant);
  ~Preserve_trx_phase1_owner();

  Preserve_trx_phase1_owner(const Preserve_trx_phase1_owner &) = delete;
  Preserve_trx_phase1_owner &operator=(
      const Preserve_trx_phase1_owner &) = delete;

  bool reconcile_record_targets(THD *drain_owner);
  bool consume_result(Preserve_trx_phase1_prepared_result result);
  void finish_ordinary_submissions();
  Preserve_trx_phase1_owner_pump_status submit_record(uint32_t budget);
  Preserve_trx_phase1_owner_pump_status submit_final_record(uint32_t budget);
  bool record_baselines_complete() const;
  bool prepare_final_record_targets(
      THD *drain_owner, const std::vector<uint64_t> &final_thread_ids);
  bool final_record_baselines_complete() const;
  bool reconcile_final_record_targets(
      THD *drain_owner, const std::vector<uint64_t> &final_thread_ids);
  void log_event(const char *event) const;

#ifndef DBUG_OFF
  /* Debug-build MTR exercise using one exact target, not a synthetic job. */
  bool debug_exercise_record_target(THD *target);
#endif

 private:
  class Impl;
  std::unique_ptr<Impl> m_impl;
};

#endif  // SQL_PRESERVE_TRX_PHASE1_OWNER_INCLUDED
