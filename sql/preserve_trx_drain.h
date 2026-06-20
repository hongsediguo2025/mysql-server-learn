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

struct Preserve_trx_drain_participant_observation {
  Preserve_trx_drain_participant_state state{
      Preserve_trx_drain_participant_state::NOT_STARTED};
  bool owns_artifact{false};
  uint64_t bytes_budget{0};
  uint64_t bytes_used{0};
  uint32_t phase1_progress{0};
  std::string failure_reason;
};

class Preserve_trx_drain_participant {
 public:
  virtual ~Preserve_trx_drain_participant() = default;

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

#endif  // SQL_PRESERVE_TRX_DRAIN_INCLUDED
