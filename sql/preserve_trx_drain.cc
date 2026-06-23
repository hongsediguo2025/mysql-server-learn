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

#include "sql/preserve_trx_drain.h"

#include <cassert>

Preserve_trx_inflight_statement_guard::~Preserve_trx_inflight_statement_guard() {
  if (!m_active) return;
  if (m_unknown_query)
    preserved_trx_clear_inflight_unknown_query(m_thd);
  else
    preserved_trx_clear_inflight_risky_statement(m_thd);
}

void Preserve_trx_inflight_statement_guard::mark(
    THD *thd, enum_sql_command sql_command) {
  assert(!m_active);
  if (preserved_trx_mark_inflight_risky_statement(thd, sql_command)) {
    m_thd = thd;
    m_active = true;
  }
}

void Preserve_trx_inflight_statement_guard::mark_unknown_query(THD *thd) {
  assert(!m_active);
  if (preserved_trx_mark_inflight_unknown_query(thd)) {
    m_thd = thd;
    m_active = true;
    m_unknown_query = true;
  }
}

Preserve_trx_drain_orchestrator::Preserve_trx_drain_orchestrator(
    Preserve_trx_drain_phase_mode mode)
    : m_mode(mode) {}

void Preserve_trx_drain_orchestrator::add_participant(
    Preserve_trx_drain_participant *participant) {
  if (participant != nullptr) m_participants.push_back(participant);
}

Preserve_trx_drain_status
Preserve_trx_drain_orchestrator::open_phase1_participants() {
  if (m_mode != Preserve_trx_drain_phase_mode::TWO_PHASE) {
    return Preserve_trx_drain_status::OK;
  }

  for (Preserve_trx_drain_participant *participant : m_participants) {
    if (!participant->open_phase1()) {
      return Preserve_trx_drain_status::PARTICIPANT_OPEN_FAILED;
    }
  }

  return Preserve_trx_drain_status::OK;
}

Preserve_trx_drain_status
Preserve_trx_drain_orchestrator::close_phase1_participants() {
  if (m_mode != Preserve_trx_drain_phase_mode::TWO_PHASE) {
    return Preserve_trx_drain_status::OK;
  }

  for (Preserve_trx_drain_participant *participant : m_participants) {
    if (!participant->close_phase1()) {
      return Preserve_trx_drain_status::PARTICIPANT_CLOSE_FAILED;
    }
  }

  return Preserve_trx_drain_status::OK;
}

Preserve_trx_drain_status
Preserve_trx_drain_orchestrator::ensure_phase1_ready() {
  if (m_mode != Preserve_trx_drain_phase_mode::TWO_PHASE) {
    return Preserve_trx_drain_status::OK;
  }

  for (const Preserve_trx_drain_participant *participant : m_participants) {
    if (!participant->phase1_ready()) {
      return Preserve_trx_drain_status::PARTICIPANT_NOT_READY;
    }
  }

  return Preserve_trx_drain_status::OK;
}

Preserve_trx_drain_status
Preserve_trx_drain_orchestrator::phase2_preflight_participants() {
  for (Preserve_trx_drain_participant *participant : m_participants) {
    if (!participant->phase2_preflight(m_mode)) {
      return Preserve_trx_drain_status::PARTICIPANT_NOT_READY;
    }
  }

  return Preserve_trx_drain_status::OK;
}

Preserve_trx_drain_status
Preserve_trx_drain_orchestrator::prepare_before_quiesce() {
  Preserve_trx_drain_status status = open_phase1_participants();
  if (status != Preserve_trx_drain_status::OK) return status;

  status = close_phase1_participants();
  if (status != Preserve_trx_drain_status::OK) return status;

  status = ensure_phase1_ready();
  if (status != Preserve_trx_drain_status::OK) return status;

  return phase2_preflight_participants();
}

void Preserve_trx_drain_orchestrator::abort_participants() {
  for (Preserve_trx_drain_participant *participant : m_participants) {
    participant->abort_phase();
  }
}

void Preserve_trx_drain_orchestrator::finalize_participants() {
  for (Preserve_trx_drain_participant *participant : m_participants) {
    participant->finalize_phase();
  }
}

void Preserve_trx_drain_orchestrator::finalize_participants_for_shutdown() {
  for (Preserve_trx_drain_participant *participant : m_participants) {
    participant->finalize_phase_for_shutdown();
  }
}

void Preserve_trx_drain_orchestrator::cleanup_after_failed_shutdown() {
  for (Preserve_trx_drain_participant *participant : m_participants) {
    participant->cleanup_after_failed_shutdown();
  }
}

std::vector<Preserve_trx_drain_participant_observation>
Preserve_trx_drain_orchestrator::observations() const {
  std::vector<Preserve_trx_drain_participant_observation> result;
  result.reserve(m_participants.size());
  for (const Preserve_trx_drain_participant *participant : m_participants) {
    result.push_back(participant->observation());
  }
  return result;
}
