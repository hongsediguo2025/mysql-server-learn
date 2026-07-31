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
  /*
    The guard is scoped to command dispatch. It only clears the marker that
    this object installed, so a DRAIN command sees a balanced in-flight view
    even when the statement exits through an error path.
  */
  if (m_unknown_query)
    preserved_trx_clear_inflight_unknown_query(m_thd);
  else
    preserved_trx_clear_inflight_risky_statement(m_thd);
}

void Preserve_trx_inflight_statement_guard::mark(
    THD *thd, enum_sql_command sql_command) {
  assert(!m_active);
  /*
    Known SQL commands can be classified before execution. Risky commands are
    excluded from the preserve target set until dispatch leaves this scope,
    which prevents phase-2 from selecting a transaction in the middle of a
    statement that can still acquire locks or mutate transactional state.
  */
  if (preserved_trx_mark_inflight_risky_statement(thd, sql_command)) {
    m_thd = thd;
    m_active = true;
  }
}

void Preserve_trx_inflight_statement_guard::mark_unknown_query(THD *thd) {
  assert(!m_active);
  /*
    COM_QUERY text is not always parsed at the point where drain admission is
    checked. Mark it as unknown first; text protocol keeps that conservative
    marker while command admission uses the parsed LEX, whereas prepared
    statement paths may install a known risky-command guard directly.
  */
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

  /*
    Phase 1 opens every participant before any participant is allowed to close
    admission. This gives current orchestrator participants, such as binlog and
    lock warmcopy, the same epoch boundary for background capture work.
  */
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

  /*
    Closing phase 1 is the participant/warmcopy epoch fence. SQL command
    admission has already been tightened by the manager state before production
    drain reaches this point. After close, participants must either have a
    sealable artifact or report that the target must fall back to the live path.
    No later step may silently add a new warmcopy target.
  */
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

  /*
    Readiness is checked after admission closes and before phase-2 preflight.
    It keeps long-running background builders from leaking into the user
    blocking window as unbounded work.
  */
  for (const Preserve_trx_drain_participant *participant : m_participants) {
    if (!participant->phase1_ready()) {
      return Preserve_trx_drain_status::PARTICIPANT_NOT_READY;
    }
  }

  return Preserve_trx_drain_status::OK;
}

Preserve_trx_drain_status
Preserve_trx_drain_orchestrator::phase2_preflight_participants() {
  /*
    Phase-2 preflight is the final orchestrator-level hook after the target set
    has quiesced and before per-target preserve consumes participant artifacts.
    A failure here must abort or route to a live fallback; continuing with a
    partial participant set would mix artifacts from different time points. Unit
    helpers may call the phase sequence without real targets, but production
    batch drain reaches this hook only for the closed/quiesced target set.
  */
  for (Preserve_trx_drain_participant *participant : m_participants) {
    if (!participant->phase2_preflight(m_mode)) {
      return Preserve_trx_drain_status::PARTICIPANT_NOT_READY;
    }
  }

  return Preserve_trx_drain_status::OK;
}

Preserve_trx_drain_status
Preserve_trx_drain_orchestrator::prepare_before_quiesce() {
  /*
    The ordering is intentional: open epoch, close admission, prove readiness,
    then run preflight. Reordering these calls can turn a bounded tail seal into
    a live scan inside the blocked phase.
  */
  Preserve_trx_drain_status status = open_phase1_participants();
  if (status != Preserve_trx_drain_status::OK) return status;

  status = close_phase1_participants();
  if (status != Preserve_trx_drain_status::OK) return status;

  status = ensure_phase1_ready();
  if (status != Preserve_trx_drain_status::OK) return status;

  return phase2_preflight_participants();
}

void Preserve_trx_drain_orchestrator::abort_participants() {
  /*
    Abort is used before a durable preserve point is reached. Participants must
    discard warm artifacts they still own, but they must not remove artifacts
    that may already be referenced by a snapshot.
  */
  for (Preserve_trx_drain_participant *participant : m_participants) {
    participant->abort_phase();
  }
}

void Preserve_trx_drain_orchestrator::finalize_participants() {
  /*
    Finalize is the success-side cleanup. By the time this runs, any durable
    descriptor has either been adopted by the carrier or the participant has
    already routed the target through a live export path.
  */
  for (Preserve_trx_drain_participant *participant : m_participants) {
    participant->finalize_phase();
  }
}

void Preserve_trx_drain_orchestrator::
    finalize_participants_for_terminal_handoff() {
  /*
    Terminal handoff finalization may intentionally defer process-local
    cleanup. The durable snapshot or accepted receiver epoch remains the
    ownership boundary; large in-memory stores are not part of the synchronous
    DRAIN completion path.
  */
  for (Preserve_trx_drain_participant *participant : m_participants) {
    participant->finalize_phase_for_terminal_handoff();
  }
}

void Preserve_trx_drain_orchestrator::cleanup_after_failed_shutdown() {
  /*
    A failed shutdown returns the server to normal operation, so participants
    must clear any state that would otherwise make the next drain inherit a
    half-closed epoch.
  */
  for (Preserve_trx_drain_participant *participant : m_participants) {
    participant->cleanup_after_failed_shutdown();
  }
}

std::vector<Preserve_trx_drain_participant_observation>
Preserve_trx_drain_orchestrator::observations() const {
  /*
    Observations are copied out after participant work so diagnostics can be
    emitted without keeping participant internals pinned or locked.
  */
  std::vector<Preserve_trx_drain_participant_observation> result;
  result.reserve(m_participants.size());
  for (const Preserve_trx_drain_participant *participant : m_participants) {
    result.push_back(participant->observation());
  }
  return result;
}
