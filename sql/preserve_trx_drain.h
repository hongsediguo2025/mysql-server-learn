/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with the
   program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef SQL_PRESERVE_TRX_DRAIN_INCLUDED
#define SQL_PRESERVE_TRX_DRAIN_INCLUDED

#include <cstdint>
#include <string>
#include <vector>

#include "sql/preserve_trx.h"

class THD;

enum class Preserve_trx_drain_phase_mode {
  TWO_PHASE,
  SINGLE_PHASE,
};

enum class Preserve_trx_drain_status {
  OK,
  PARTICIPANT_OPEN_FAILED,
  PARTICIPANT_CLOSE_FAILED,
  PARTICIPANT_NOT_READY,
};

enum class Preserve_trx_drain_participant_state {
  NOT_STARTED,
  OPEN,
  READY,
  DEGRADED,
  ABANDONED,
  FINALIZED,
};

/*
  Participant observations are the stable reporting surface for one drain
  attempt. Values are cumulative within that attempt; callers must not infer
  process lifetime counters from them.
*/
struct Preserve_trx_drain_participant_observation {
  Preserve_trx_drain_participant_state state{
      Preserve_trx_drain_participant_state::NOT_STARTED};
  bool owns_artifact{false};
  uint64_t bytes_budget{0};
  uint64_t bytes_used{0};
  uint64_t phase1_record_prebuilt_target_count{0};
  uint64_t phase1_record_active_scan_target_count{0};
  uint64_t phase2_lock_seal_us{0};
  uint64_t phase2_full_lock_scan_count{0};
  uint64_t materialized_lock_payload_bytes_in_phase2{0};
  uint64_t phase2_record_lock_count{0};
  uint64_t phase2_table_lock_count{0};
  uint64_t phase2_mdl_descriptor_count{0};
  uint64_t phase2_table_live_export_target_count{0};
  uint64_t phase2_mdl_live_export_target_count{0};
  uint64_t phase2_record_prebuilt_target_count{0};
  uint64_t phase2_record_materialized_target_count{0};
  uint32_t phase2_seal_worker_count{0};
  bool phase2_slo_guaranteed{true};
  uint64_t phase2_slo_not_guaranteed_target_count{0};
  uint32_t phase1_progress{0};
  std::string failure_reason;
  std::string phase2_slo_reason;
};

class Preserve_trx_drain_participant {
 public:
  virtual ~Preserve_trx_drain_participant() = default;

  /*
    The phase contract is ordered by Preserve_trx_drain_orchestrator. A
    participant may degrade itself and later request live fallback, but it must
    not publish a durable artifact before the carrier adopts it.
  */
  virtual bool open_phase1() = 0;
  virtual bool close_phase1() = 0;
  virtual bool phase1_ready() const = 0;
  virtual bool phase2_preflight(Preserve_trx_drain_phase_mode mode) = 0;
  virtual void abort_phase() = 0;
  virtual void finalize_phase() = 0;
  virtual void finalize_phase_for_shutdown() { finalize_phase(); }
  virtual void cleanup_after_failed_shutdown() {}
  virtual Preserve_trx_drain_participant_observation observation() const {
    return {};
  }
};

class Preserve_trx_drain_orchestrator {
 public:
  explicit Preserve_trx_drain_orchestrator(
      Preserve_trx_drain_phase_mode mode);

  void add_participant(Preserve_trx_drain_participant *participant);
  Preserve_trx_drain_status open_phase1_participants();
  Preserve_trx_drain_status close_phase1_participants();
  Preserve_trx_drain_status ensure_phase1_ready();
  Preserve_trx_drain_status phase2_preflight_participants();
  Preserve_trx_drain_status prepare_before_quiesce();
  void abort_participants();
  void finalize_participants();
  void finalize_participants_for_shutdown();
  void cleanup_after_failed_shutdown();
  std::vector<Preserve_trx_drain_participant_observation> observations() const;

 private:
  /* Non-owning participant list; lifetime is bound to the enclosing drain. */
  Preserve_trx_drain_phase_mode m_mode;
  std::vector<Preserve_trx_drain_participant *> m_participants;
};

struct Preserve_trx_drain_request {
  explicit Preserve_trx_drain_request(const Preserve_trx_options &options_arg);

  Preserve_trx_options options;
};

class Preserve_trx_drain_service {
 public:
  bool execute(THD *thd, const Preserve_trx_drain_request &request);
};

class Preserve_trx_inflight_statement_guard {
 public:
  Preserve_trx_inflight_statement_guard() = default;
  Preserve_trx_inflight_statement_guard(
      const Preserve_trx_inflight_statement_guard &) = delete;
  Preserve_trx_inflight_statement_guard &operator=(
      const Preserve_trx_inflight_statement_guard &) = delete;
  ~Preserve_trx_inflight_statement_guard();

  void mark(THD *thd, enum_sql_command sql_command);
  void mark_unknown_query(THD *thd);

 private:
  /*
    The guard records either a known risky command or an unknown query marker,
    never both. This keeps command-boundary drain races visible without adding
    state to ordinary statement execution after the scope exits.
  */
  THD *m_thd{nullptr};
  bool m_active{false};
  bool m_unknown_query{false};
};

#endif  // SQL_PRESERVE_TRX_DRAIN_INCLUDED
