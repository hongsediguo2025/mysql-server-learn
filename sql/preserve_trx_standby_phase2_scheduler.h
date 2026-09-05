/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
   General Public License, version 2.0, for more details. */

#ifndef SQL_PRESERVE_TRX_STANDBY_PHASE2_SCHEDULER_INCLUDED
#define SQL_PRESERVE_TRX_STANDBY_PHASE2_SCHEDULER_INCLUDED

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

class THD;

namespace preserve_trx_phase2_scheduler {

struct Command_key {
  uint64_t connection_incarnation{0};
  uint64_t sequence{0};
};

struct Transaction_key {
  uint64_t connection_incarnation{0};
  uint64_t ordinal{0};
};

enum class Gate_action : uint8_t {
  DEFER_TO_CLASS_GATE,
  ENTER_BODY,
  CUTOFF_4020,
  NATIVE_PRE_BODY_EXIT,
  RETRY_NATIVE
};

enum class Command_class : uint8_t {
  TX_PROGRESS,
  TX_END,
  TX_END_BY_DDL,
  DEFAULT_DENY
};

enum class Engine_identity_state : uint8_t {
  NONE = 0,
  EXACT_ACTIVE,
  RETRY_LIFECYCLE,
  UNSUPPORTED
};

struct Transaction_observation {
  bool sql_transaction_active{false};
  Engine_identity_state engine_state{Engine_identity_state::NONE};
  uint64_t raw_engine_cookie{0};
  uint64_t engine_version{0};
  uint32_t isolation_level{0};
};

struct Admission_request {
  Command_key command;
  Command_class command_class{Command_class::DEFAULT_DENY};
  bool effective_no_chain{false};
  Transaction_observation transaction_observation;
};

struct Command_exit_fact {
  Command_key command;
  bool entered_body{false};
  uint64_t native_body_exit_us{0};
  uint64_t thread_id_projection{0};
  Transaction_observation transaction_observation;
};

enum class Terminal_result : uint8_t {
  RUNNING,
  HARD_QUIESCENT,
  HARD_DEADLINE,
  SAFETY_ABORT,
  OWNER_CANCELLED
};

enum class Finish_result : uint8_t { NATIVE_RESULT = 0, CUTOFF_4020 };

enum class Last_body_exit_state : uint8_t {
  EXACT = 0,
  NO_ELIGIBLE_BODY,
  COVERAGE_INCOMPLETE
};

struct Terminal_snapshot {
  uint64_t attempt_id{0};
  uint64_t generation{0};
  uint64_t policy_started_us{0};
  uint64_t terminal_published_us{0};
  uint64_t hard_published_us{0};
  uint64_t eligible_body_count{0};
  std::string eligible_body_key_digest_v1;
  Last_body_exit_state last_body_exit_state{
      Last_body_exit_state::NO_ELIGIBLE_BODY};
  bool exact_body_exit_coverage_complete{false};
  uint64_t last_body_exit_us{0};
  Command_key last_body_exit_command;
  uint64_t last_body_exit_thread_id{0};
  Terminal_result terminal_result{Terminal_result::RUNNING};
  uint32_t terminal_cause{0};
};

struct Summary_snapshot {
  uint64_t attempt_id{0};
  uint64_t generation{0};
  Terminal_result terminal_result{Terminal_result::RUNNING};
  uint32_t terminal_cause{0};
  uint64_t t0_scope_registered{0};
  uint64_t t0_executing_max{0};
  uint64_t scan_count{0};
  uint64_t scan_candidate{0};
  uint64_t scan_positive_result{0};
  uint64_t scan_negative_result{0};
  uint64_t scan_unsupported_result{0};
  uint64_t scan_unknown_result{0};
  uint64_t scan_stale_discarded{0};
  uint64_t scan_overrun{0};
  uint64_t support_edge_registered{0};
  uint64_t permit_issued{0};
  uint64_t returned_4020{0};
  uint64_t teardown_started{0};
  uint64_t lineage_unknown{0};
  uint64_t lock_proof_unknown{0};
  uint64_t tick_crossed_unserviced_progress_deadline{0};
  uint64_t execution_returned_4020_conflict{0};
  uint64_t invariant_violation_count{0};
};

struct Owner_config {
  uint64_t attempt_id{0};
  uint64_t generation{0};
  uint64_t owner_thread_id{0};
  uint64_t policy_started_us{0};
  uint64_t absolute_deadline_us{0};
};

class Attempt;
using Attempt_handle = std::shared_ptr<Attempt>;

bool capture_command(THD *command_thd, Command_key *command);
bool captured_command_key(THD *command_thd, Command_key *command);
bool dependency_transaction_is_active(THD *thd);
Attempt_handle publish_and_register_t0(THD *owner,
                                       const Owner_config &config);
Gate_action gate_command(THD *command_thd,
                         const Admission_request &request);
Finish_result finish_command(THD *command_thd,
                             const Command_exit_fact &fact);
void note_teardown_begin(uint64_t connection_incarnation);
void note_transaction_cleanup(uint64_t connection_incarnation,
                              const Transaction_key &transaction);
Terminal_result tick(const Attempt_handle &attempt, uint64_t now_us,
                     uint64_t tick_stop_us,
                     bool stop_is_progress_deadline = false);
bool terminal_snapshot(const Attempt_handle &attempt,
                       Terminal_snapshot *snapshot);
bool summary_snapshot(const Attempt_handle &attempt,
                      Summary_snapshot *snapshot);
bool reset_is_unsupported();
bool cutoff_response_handoff_pending();
void wait_for_cutoff_response_handoff();
void wait_for_change(const Attempt_handle &attempt, uint64_t wait_us);
void owner_cancel(const Attempt_handle &attempt, uint32_t cause);
void publish_native_admission_restored_and_retire_route(
    const Attempt_handle &attempt);
void release_cutoff_responses_and_retire_route(
    const Attempt_handle &attempt);

}  // namespace preserve_trx_phase2_scheduler

#endif  // SQL_PRESERVE_TRX_STANDBY_PHASE2_SCHEDULER_INCLUDED
