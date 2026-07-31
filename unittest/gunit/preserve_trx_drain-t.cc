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

#include "my_config.h"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "sql/preserve_trx_drain.h"
#include "sql/preserve_trx.h"

namespace preserve_trx_drain_unittest {

class RecordingDrainParticipant final : public Preserve_trx_drain_participant {
 public:
  explicit RecordingDrainParticipant(std::vector<std::string> *events,
                                     std::string name)
      : m_events(events), m_name(std::move(name)) {}

  bool open_phase1() override {
    m_events->push_back(m_name + ".open");
    return open_ok;
  }

  bool close_phase1() override {
    m_events->push_back(m_name + ".close");
    return close_ok;
  }

  bool phase1_ready() const override {
    m_events->push_back(m_name + ".ready");
    return ready;
  }

  bool phase2_preflight(Preserve_trx_drain_phase_mode mode) override {
    m_events->push_back(m_name +
                        (mode == Preserve_trx_drain_phase_mode::TWO_PHASE
                             ? ".phase2.two"
                             : ".phase2.single"));
    return phase2_ok;
  }

  void abort_phase() override {
    m_events->push_back(m_name + ".abort");
    observation_value.state = Preserve_trx_drain_participant_state::ABANDONED;
  }

  void finalize_phase() override {
    m_events->push_back(m_name + ".finalize");
    observation_value.state = Preserve_trx_drain_participant_state::FINALIZED;
  }

  void finalize_phase_for_terminal_handoff() override {
    m_events->push_back(m_name + ".finalize_terminal_handoff");
    observation_value.state = Preserve_trx_drain_participant_state::FINALIZED;
  }

  Preserve_trx_drain_participant_observation observation() const override {
    return observation_value;
  }

  bool open_ok{true};
  bool close_ok{true};
  bool ready{true};
  bool phase2_ok{true};
  Preserve_trx_drain_participant_observation observation_value;

 private:
  std::vector<std::string> *m_events;
  std::string m_name;
};

TEST(PreserveTrxDrainOrchestrator, TwoPhaseRunsParticipantsInStableOrder) {
  std::vector<std::string> events;
  RecordingDrainParticipant binlog(&events, "binlog");
  RecordingDrainParticipant temp(&events, "temp");

  Preserve_trx_drain_orchestrator orchestrator(
      Preserve_trx_drain_phase_mode::TWO_PHASE);
  orchestrator.add_participant(&binlog);
  orchestrator.add_participant(&temp);

  EXPECT_EQ(Preserve_trx_drain_status::OK,
            orchestrator.prepare_before_quiesce());
  orchestrator.finalize_participants();

  const std::vector<std::string> expected{
      "binlog.open",      "temp.open",       "binlog.close",
      "temp.close",       "binlog.ready",    "temp.ready",
      "binlog.phase2.two", "temp.phase2.two", "binlog.finalize",
      "temp.finalize"};
  EXPECT_EQ(expected, events);
}

TEST(PreserveTrxDrainOrchestrator, SinglePhaseSkipsPhaseOneButKeepsPreflight) {
  std::vector<std::string> events;
  RecordingDrainParticipant binlog(&events, "binlog");

  Preserve_trx_drain_orchestrator orchestrator(
      Preserve_trx_drain_phase_mode::SINGLE_PHASE);
  orchestrator.add_participant(&binlog);

  EXPECT_EQ(Preserve_trx_drain_status::OK,
            orchestrator.prepare_before_quiesce());

  const std::vector<std::string> expected{"binlog.phase2.single"};
  EXPECT_EQ(expected, events);
}

TEST(PreserveTrxDrainOrchestrator, PhaseTwoFailureIsFailClosedAndAbortable) {
  std::vector<std::string> events;
  RecordingDrainParticipant binlog(&events, "binlog");
  RecordingDrainParticipant temp(&events, "temp");
  temp.phase2_ok = false;

  Preserve_trx_drain_orchestrator orchestrator(
      Preserve_trx_drain_phase_mode::TWO_PHASE);
  orchestrator.add_participant(&binlog);
  orchestrator.add_participant(&temp);

  EXPECT_EQ(Preserve_trx_drain_status::PARTICIPANT_NOT_READY,
            orchestrator.prepare_before_quiesce());
  orchestrator.abort_participants();

  const std::vector<std::string> expected{
      "binlog.open",      "temp.open",       "binlog.close",
      "temp.close",       "binlog.ready",    "temp.ready",
      "binlog.phase2.two", "temp.phase2.two", "binlog.abort",
      "temp.abort"};
  EXPECT_EQ(expected, events);
}

TEST(PreserveTrxDrainOrchestrator, OpenFailureStopsBeforeClose) {
  std::vector<std::string> events;
  RecordingDrainParticipant binlog(&events, "binlog");
  RecordingDrainParticipant temp(&events, "temp");
  binlog.open_ok = false;

  Preserve_trx_drain_orchestrator orchestrator(
      Preserve_trx_drain_phase_mode::TWO_PHASE);
  orchestrator.add_participant(&binlog);
  orchestrator.add_participant(&temp);

  EXPECT_EQ(Preserve_trx_drain_status::PARTICIPANT_OPEN_FAILED,
            orchestrator.prepare_before_quiesce());

  const std::vector<std::string> expected{"binlog.open"};
  EXPECT_EQ(expected, events);
}

TEST(PreserveTrxDrainOrchestrator, CloseFailureStopsBeforeReadyCheck) {
  std::vector<std::string> events;
  RecordingDrainParticipant binlog(&events, "binlog");
  RecordingDrainParticipant temp(&events, "temp");
  temp.close_ok = false;

  Preserve_trx_drain_orchestrator orchestrator(
      Preserve_trx_drain_phase_mode::TWO_PHASE);
  orchestrator.add_participant(&binlog);
  orchestrator.add_participant(&temp);

  EXPECT_EQ(Preserve_trx_drain_status::PARTICIPANT_CLOSE_FAILED,
            orchestrator.prepare_before_quiesce());

  const std::vector<std::string> expected{"binlog.open", "temp.open",
                                          "binlog.close", "temp.close"};
  EXPECT_EQ(expected, events);
}

TEST(PreserveTrxDrainOrchestrator, NotReadyStopsBeforePhaseTwoPreflight) {
  std::vector<std::string> events;
  RecordingDrainParticipant binlog(&events, "binlog");
  RecordingDrainParticipant temp(&events, "temp");
  binlog.ready = false;

  Preserve_trx_drain_orchestrator orchestrator(
      Preserve_trx_drain_phase_mode::TWO_PHASE);
  orchestrator.add_participant(&binlog);
  orchestrator.add_participant(&temp);

  EXPECT_EQ(Preserve_trx_drain_status::PARTICIPANT_NOT_READY,
            orchestrator.prepare_before_quiesce());

  const std::vector<std::string> expected{"binlog.open", "temp.open",
                                          "binlog.close", "temp.close",
                                          "binlog.ready"};
  EXPECT_EQ(expected, events);
}

TEST(PreserveTrxDrainParticipantObservation,
     CarriesArtifactBudgetProgressAndFailureReason) {
  std::vector<std::string> events;
  RecordingDrainParticipant participant(&events, "binlog");
  participant.observation_value.state =
      Preserve_trx_drain_participant_state::DEGRADED;
  participant.observation_value.owns_artifact = true;
  participant.observation_value.bytes_budget = 4096;
  participant.observation_value.bytes_used = 1024;
  participant.observation_value.phase1_progress = 75;
  participant.observation_value.failure_reason = "tail budget exceeded";

  const Preserve_trx_drain_participant_observation observation =
      participant.observation();

  EXPECT_EQ(Preserve_trx_drain_participant_state::DEGRADED,
            observation.state);
  EXPECT_TRUE(observation.owns_artifact);
  EXPECT_EQ(4096U, observation.bytes_budget);
  EXPECT_EQ(1024U, observation.bytes_used);
  EXPECT_EQ(75U, observation.phase1_progress);
  EXPECT_EQ("tail budget exceeded", observation.failure_reason);
}

TEST(PreserveTrxDrainParticipantObservation,
     OrchestratorReturnsAllParticipantObservations) {
  std::vector<std::string> events;
  RecordingDrainParticipant binlog(&events, "binlog");
  RecordingDrainParticipant temp(&events, "temp");
  binlog.observation_value.state = Preserve_trx_drain_participant_state::READY;
  binlog.observation_value.bytes_used = 1024;
  temp.observation_value.state = Preserve_trx_drain_participant_state::DEGRADED;
  temp.observation_value.failure_reason = "temp image budget exceeded";

  Preserve_trx_drain_orchestrator orchestrator(
      Preserve_trx_drain_phase_mode::TWO_PHASE);
  orchestrator.add_participant(&binlog);
  orchestrator.add_participant(&temp);

  const std::vector<Preserve_trx_drain_participant_observation> observations =
      orchestrator.observations();
  ASSERT_EQ(2U, observations.size());
  EXPECT_EQ(Preserve_trx_drain_participant_state::READY,
            observations[0].state);
  EXPECT_EQ(1024U, observations[0].bytes_used);
  EXPECT_EQ(Preserve_trx_drain_participant_state::DEGRADED,
            observations[1].state);
  EXPECT_EQ("temp image budget exceeded", observations[1].failure_reason);
}

TEST(PreserveTrxDrainOrchestrator, AbortAndFinalizeUpdateParticipantState) {
  std::vector<std::string> events;
  RecordingDrainParticipant participant(&events, "binlog");
  participant.observation_value.state =
      Preserve_trx_drain_participant_state::READY;

  Preserve_trx_drain_orchestrator orchestrator(
      Preserve_trx_drain_phase_mode::TWO_PHASE);
  orchestrator.add_participant(&participant);
  orchestrator.abort_participants();
  EXPECT_EQ(Preserve_trx_drain_participant_state::ABANDONED,
            participant.observation().state);

  participant.observation_value.state =
      Preserve_trx_drain_participant_state::READY;
  orchestrator.finalize_participants();
  EXPECT_EQ(Preserve_trx_drain_participant_state::FINALIZED,
            participant.observation().state);
}

TEST(PreserveTrxDrainOrchestrator,
     TerminalHandoffFinalizeUsesExplicitParticipantHook) {
  std::vector<std::string> events;
  RecordingDrainParticipant participant(&events, "lock");

  Preserve_trx_drain_orchestrator orchestrator(
      Preserve_trx_drain_phase_mode::TWO_PHASE);
  orchestrator.add_participant(&participant);
  orchestrator.finalize_participants_for_terminal_handoff();

  const std::vector<std::string> expected{
      "lock.finalize_terminal_handoff"};
  EXPECT_EQ(expected, events);
  EXPECT_EQ(Preserve_trx_drain_participant_state::FINALIZED,
            participant.observation().state);
}

TEST(PreserveTrxDrainRequest, OwnsOptionsCopy) {
  Preserve_trx_options options;
  options.has_timeout = false;
  options.timeout_seconds = 30;
  options.user_vars_mode = Preserve_trx_user_vars_mode::INCLUDE;

  Preserve_trx_drain_request request{options};
  options.has_timeout = true;
  options.timeout_seconds = 60;
  options.user_vars_mode = Preserve_trx_user_vars_mode::EXCLUDE;

  EXPECT_FALSE(request.options.has_timeout);
  EXPECT_EQ(30U, request.options.timeout_seconds);
  EXPECT_EQ(Preserve_trx_user_vars_mode::INCLUDE, request.options.user_vars_mode);
}

}  // namespace preserve_trx_drain_unittest
