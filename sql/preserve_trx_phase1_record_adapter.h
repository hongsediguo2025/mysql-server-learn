/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation. */

#ifndef SQL_PRESERVE_TRX_PHASE1_RECORD_ADAPTER_INCLUDED
#define SQL_PRESERVE_TRX_PHASE1_RECORD_ADAPTER_INCLUDED

#include <stdint.h>

#include <memory>
#include <string>

#include "sql/preserve_trx_phase1_pipeline.h"
#include "storage/innobase/include/lock0warmcopy.h"

class THD;
class Preserve_trx_lock_warmcopy_drain_participant;

using Preserve_trx_phase1_adapter_cancel_probe = bool (*)(void *context);
using Preserve_trx_phase1_adapter_credit_reserver =
    Preserve_trx_phase1_pipeline_credit_status (*)(void *context,
                                                    uint64_t total_bytes);

struct Preserve_trx_phase1_record_adapter_control {
  uint64_t deadline_us{0};
  Preserve_trx_phase1_adapter_cancel_probe cancel_probe{nullptr};
  void *cancel_context{nullptr};
  Preserve_trx_phase1_adapter_credit_reserver reserve_credit{nullptr};
  void *credit_context{nullptr};
};

struct Preserve_trx_phase1_record_adapter_outcome {
  Preserve_trx_phase1_pipeline_result_status status{
      Preserve_trx_phase1_pipeline_result_status::ADAPTER_NOT_INSTALLED};
  uint64_t logical_bytes{0};
  uint64_t required_credit_bytes{0};
  uint64_t global_latch_wait_us{0};
  uint64_t global_latch_hold_us{0};
  uint64_t global_latch_envelope_us{0};
  Preserve_trx_phase1_record_capture_handle capture_payload;
  Preserve_trx_phase1_record_prepared_handle prepared_payload;
  std::string reason;
};

struct Preserve_trx_phase1_record_adapter_install_result {
  Preserve_trx_phase1_pipeline_result_status status{
      Preserve_trx_phase1_pipeline_result_status::ADAPTER_NOT_INSTALLED};
  uint64_t publication_token{0};
  uint32_t record_lock_count{0};
  lock_warmcopy_record_store_compare_token_t installed_token;
  lock_warmcopy_trx_lock_fence_t captured_live_fence;
  std::string reason;
};

void preserve_trx_phase1_record_adapter_capture(
    const Preserve_trx_phase1_work_descriptor &descriptor,
    const Preserve_trx_phase1_record_adapter_control &control,
    Preserve_trx_phase1_record_adapter_outcome *outcome);

void preserve_trx_phase1_record_adapter_prepare(
    THD *worker_thd, const Preserve_trx_phase1_work_descriptor &descriptor,
    const Preserve_trx_phase1_record_capture_handle &capture_payload,
    const Preserve_trx_phase1_record_adapter_control &control,
    Preserve_trx_phase1_record_adapter_outcome *outcome);

void preserve_trx_phase1_record_adapter_owner_install(
    const Preserve_trx_phase1_work_descriptor &descriptor,
    const Preserve_trx_phase1_record_prepared_handle &prepared_payload,
    Preserve_trx_phase1_record_adapter_install_result *result);

void preserve_trx_phase1_record_adapter_owner_publish(
    const Preserve_trx_phase1_work_descriptor &descriptor,
    const Preserve_trx_phase1_record_prepared_handle &prepared_payload,
    Preserve_trx_lock_warmcopy_drain_participant *participant,
    bool active_scan,
    Preserve_trx_phase1_record_adapter_install_result *result);

/* Debug-build MTR exercise; it never participates in a production attempt. */
#ifndef DBUG_OFF
bool preserve_trx_phase1_record_adapter_debug_exercise(THD *worker_thd,
                                                        THD *target_thd,
                                                        bool expect_empty,
    Preserve_trx_lock_warmcopy_drain_participant *participant);
#endif

#endif  // SQL_PRESERVE_TRX_PHASE1_RECORD_ADAPTER_INCLUDED
