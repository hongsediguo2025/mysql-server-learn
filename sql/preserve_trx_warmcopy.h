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
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef SQL_PRESERVE_TRX_WARMCOPY_INCLUDED
#define SQL_PRESERVE_TRX_WARMCOPY_INCLUDED

#include <cstdint>
#include <initializer_list>
#include <mutex>
#include <string>
#include <vector>

#include "sql/preserve_trx_bundle.h"
#include "sql/preserve_trx_drain.h"

enum class Binlog_warmcopy_participant_state {
  DISCOVERED,
  COPYING_PREFIX,
  MIRRORING,
  READY,
  ABANDONED,
  DEGRADED,
  FINALIZED
};

struct WarmcopyParticipantMetadata {
  uint64_t source_length{0};
  uint64_t destination_length{0};
  uint64_t event_counter{0};
  uint64_t copied_until{0};
  uint64_t durable_until{0};
  uint64_t digest_until{0};
  uint64_t descriptor_hwm{0};
  uint64_t truncate_generation{0};
  bool cache_flags_match{false};
  bool compression_metadata_match{false};
  bool savepoint_cache_state_match{false};
  bool mirror_active{false};
};

struct WarmcopyDrainMetrics {
  uint64_t participants_discovered{0};
  uint64_t participants_ready{0};
  uint64_t participants_abandoned{0};
  uint64_t participants_degraded{0};
  uint64_t prefix_bytes{0};
  uint64_t mirrored_bytes{0};
  uint64_t tail_bytes{0};
  uint64_t digested_bytes{0};
  uint64_t durable_bytes{0};
  uint64_t phase1_us{0};
  uint64_t phase2_pause_us{0};
};

struct Warmcopy_historical_work_counters {
  uint64_t prefix_copy_bytes{0};
  uint64_t digest_bytes{0};
  uint64_t durable_bytes{0};
  uint64_t scan_bytes{0};
};

struct WarmcopyObservabilitySnapshot {
  WarmcopyDrainMetrics metrics;
  Warmcopy_historical_work_counters phase2_historical_work;
  uint64_t descriptor_hwm{0};
};

class WarmcopyObservabilitySink {
 public:
  virtual ~WarmcopyObservabilitySink() = default;
  virtual void publish(const std::string &message) = 0;
};

class Warmcopy_descriptor_tracker {
 public:
  void note_range_covered(uint64_t offset, uint64_t length,
                          uint64_t truncate_generation);
  void note_truncate(uint64_t length, uint64_t truncate_generation);
  void note_flush_durable(uint64_t durable_until);
  bool advance_descriptor_hwm(uint64_t target_length);
  Preserved_trx_external_blob_descriptor descriptor() const;
  Warmcopy_historical_work_counters phase2_historical_work_counters() const;

  uint64_t covered_until() const { return m_covered_until; }
  uint64_t durable_until() const { return m_durable_until; }
  uint64_t digest_until() const { return m_digest_until; }
  uint64_t descriptor_hwm() const { return m_descriptor_hwm; }
  uint64_t truncate_generation() const { return m_truncate_generation; }

 private:
  struct Covered_range {
    uint64_t begin;
    uint64_t end;
  };

  void recompute_covered_until();

  std::vector<Covered_range> m_ranges;
  uint64_t m_truncate_generation{0};
  bool m_generation_initialized{false};
  uint64_t m_covered_until{0};
  uint64_t m_durable_until{0};
  uint64_t m_digest_until{0};
  uint64_t m_descriptor_hwm{0};
  Preserved_trx_external_blob_descriptor m_descriptor;
};

class WarmcopyParticipant {
 public:
  Binlog_warmcopy_participant_state state() const;
  bool ready() const;
  bool tail_within_budget(uint64_t current_source_length,
                          uint64_t tail_budget_bytes) const;
  void note_prefix_progress(uint64_t copied_until);
  void note_source_metadata(WarmcopyParticipantMetadata metadata);
  void mark_degraded(std::string reason);
  void mark_abandoned();
  void mark_finalized();

  const WarmcopyParticipantMetadata &metadata() const { return m_metadata; }
  const std::string &degraded_reason() const { return m_degraded_reason; }

 private:
  bool ready_requirements_met() const;
  bool terminal() const;

  Binlog_warmcopy_participant_state m_state{
      Binlog_warmcopy_participant_state::DISCOVERED};
  WarmcopyParticipantMetadata m_metadata;
  std::string m_degraded_reason;
};

enum class Binlog_warmcopy_coordinator_state {
  IDLE,
  OPEN,
  CLOSING,
  ABORTED,
  CLOSED
};

class WarmcopyParticipantCleanup {
 public:
  virtual ~WarmcopyParticipantCleanup() = default;
  virtual void remove_warm_artifact(
      const WarmcopyParticipant &participant) = 0;
};

class THD;

class WarmcopyDrainCoordinator {
 public:
  bool open_epoch(THD *coordinator_thd);
  bool publish_open_gate_before_enumeration();
  bool reconcile_open_admission_sequence();
  bool admit_thd_if_needed(THD *target_thd);
  bool admit_participant_for_test(WarmcopyParticipant *participant);
  bool begin_closing(unsigned long timeout_ms);
  bool all_active_participants_ready() const;
  bool has_active_degraded_participant() const;
  void abort_and_cleanup(WarmcopyParticipantCleanup *cleanup = nullptr);
  uint64_t closing_deadline_us() const;

  Binlog_warmcopy_coordinator_state state() const;
  bool admission_open() const;
  bool commands_blocked() const;
  uint64_t admission_sequence() const;
  uint64_t enumeration_started_sequence() const;
  size_t participant_count() const;

 private:
  bool participant_blocks_closing(const WarmcopyParticipant *participant) const;

  mutable std::mutex m_mutex;
  Binlog_warmcopy_coordinator_state m_state{
      Binlog_warmcopy_coordinator_state::IDLE};
  THD *m_coordinator_thd{nullptr};
  bool m_admission_open{false};
  bool m_commands_blocked{false};
  uint64_t m_closing_deadline_us{0};
  uint64_t m_admission_sequence{0};
  uint64_t m_enumeration_started_sequence{0};
  uint32_t m_next_participant_id{0};
  std::vector<WarmcopyParticipant *> m_participants;
};

class WarmcopyDrainParticipantAdapter final
    : public Preserve_trx_drain_participant {
 public:
  WarmcopyDrainParticipantAdapter(WarmcopyDrainCoordinator *coordinator,
                                  THD *coordinator_thd,
                                  unsigned long close_timeout_ms);

  bool open_phase1() override;
  bool close_phase1() override;
  bool phase1_ready() const override;
  bool phase2_preflight(Preserve_trx_drain_phase_mode mode) override;
  void abort_phase() override;
  void finalize_phase() override;

 private:
  WarmcopyDrainCoordinator *m_coordinator{nullptr};
  THD *m_coordinator_thd{nullptr};
  unsigned long m_close_timeout_ms{0};
};

WarmcopyDrainMetrics warmcopy_metrics_for_participants(
    std::initializer_list<const WarmcopyParticipant *> participants);

bool warmcopy_has_active_degraded_participant(
    std::initializer_list<const WarmcopyParticipant *> participants);

bool warmcopy_entry_blob_limit(uint64_t total_reserved_bytes,
                               uint64_t entry_reserved_bytes,
                               uint64_t max_total_bytes,
                               uint64_t *entry_blob_limit);

bool warmcopy_effective_entry_blob_limit(uint64_t total_reserved_bytes,
                                         uint64_t entry_reserved_bytes,
                                         uint64_t max_total_bytes,
                                         uint64_t max_entry_blob_bytes,
                                         uint64_t *entry_blob_limit);

bool warmcopy_reservation_with_tail_budget(uint64_t prefix_bytes,
                                           uint64_t tail_budget_bytes,
                                           uint64_t entry_blob_limit,
                                           uint64_t *reservation);

bool warmcopy_accounted_session_reservation(uint64_t session_blob_limit,
                                            uint64_t requested_reservation,
                                            uint64_t *accounted_reservation);

bool warmcopy_pending_range_limit_exceeded(uint64_t pending_range_count,
                                           uint64_t pending_range_bytes,
                                           uint64_t new_range_bytes,
                                           uint64_t max_range_count,
                                           uint64_t max_pending_bytes,
                                           uint64_t *next_pending_range_bytes);

void warmcopy_publish_observability(
    const WarmcopyObservabilitySnapshot &snapshot,
    WarmcopyObservabilitySink *sink);

void warmcopy_publish_observability_to_error_log(
    const WarmcopyObservabilitySnapshot &snapshot);

#endif  // SQL_PRESERVE_TRX_WARMCOPY_INCLUDED
