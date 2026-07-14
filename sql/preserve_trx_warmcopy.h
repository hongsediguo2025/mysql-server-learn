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
  /* Candidate transaction found but no prefix copy has started. */
  DISCOVERED,
  /* Existing binlog cache bytes are being copied outside the blocked window. */
  COPYING_PREFIX,
  /* Source cache writes are mirrored while the transaction continues running. */
  MIRRORING,
  /* Prefix, mirrored ranges, durable bytes, digest, and metadata all match. */
  READY,
  /* Caller abandoned this participant before publication. */
  ABANDONED,
  /* Warmcopy cannot be trusted; preserve must use fallback or reject. */
  DEGRADED,
  /* Preserve consumed or discarded the participant; no more writes allowed. */
  FINALIZED
};

/*
  Model/helper metadata sampled from one transaction binlog cache during a
  warmcopy epoch. Production sessions keep their own private watermarks; this
  structure is the shared reporting/comparison shape used by helpers and tests.

  The length and high-water fields describe which byte ranges are copied,
  durable, and safe to name in a descriptor. The match flags record cache
  semantics that must stay stable between phase 1 and final preserve; byte
  equality alone is not enough to prove a binlog cache can be resumed.
*/
struct WarmcopyParticipantMetadata {
  /*
    source_length is the current binlog cache length; destination_length is the
    mirror length that has been created so far. copied_until/durable_until/
    digest_until/descriptor_hwm are separate because copy, fsync, digest, and
    descriptor visibility advance at different times.
  */
  uint64_t source_length{0};
  uint64_t destination_length{0};
  uint64_t event_counter{0};
  uint64_t copied_until{0};
  uint64_t durable_until{0};
  uint64_t digest_until{0};
  uint64_t descriptor_hwm{0};
  /*
    truncate_generation changes whenever the binlog cache is truncated. Ranges
    copied before an incompatible generation cannot be reused for the final
    descriptor.
  */
  uint64_t truncate_generation{0};
  /*
    These booleans prove that the warm mirror still represents the same cache
    semantics as the source. A byte-complete mirror is rejected if flags,
    compression state, or savepoint cache state no longer match.
  */
  bool cache_flags_match{false};
  bool compression_metadata_match{false};
  bool savepoint_cache_state_match{false};
  /* True while Binlog_cache_storage still writes/truncates through the mirror. */
  bool mirror_active{false};
};

/*
  Per-drain binlog warmcopy metrics.

  prefix/mirrored/tail bytes describe where work happened. phase2_pause_us is
  the part that still contributes to the blocked drain window, so regressions
  that move prefix work back into phase 2 remain visible.
*/
struct WarmcopyDrainMetrics {
  /* Participant counts are per-drain, not process lifetime totals. */
  uint64_t participants_discovered{0};
  uint64_t participants_ready{0};
  uint64_t participants_abandoned{0};
  uint64_t participants_degraded{0};
  /*
    prefix_bytes are copied from existing cache ranges, mirrored_bytes are writes
    observed through the active mirror, and tail_bytes are source bytes beyond
    the copied prefix that must already be mirrored/durable by finalize.
  */
  uint64_t prefix_bytes{0};
  uint64_t mirrored_bytes{0};
  uint64_t tail_bytes{0};
  /* Bytes that reached digest/durable high-water marks for descriptor adoption. */
  uint64_t digested_bytes{0};
  uint64_t durable_bytes{0};
  /*
    phase1_us and phase2_pause_us are filled by the drain caller, not derived
    from participant byte counters.
  */
  uint64_t phase1_us{0};
  uint64_t phase2_pause_us{0};
};

/*
  Work counters retained when historical copy/digest/scan work is charged to
  phase 2. Some counters are zero for current paths but remain explicit so
  performance output can distinguish "not produced" from "work moved out of
  phase 2".
*/
struct Warmcopy_historical_work_counters {
  uint64_t prefix_copy_bytes{0};
  uint64_t digest_bytes{0};
  uint64_t durable_bytes{0};
  uint64_t scan_bytes{0};
};

/* Snapshot published to preserve/drain observability after an epoch. */
struct WarmcopyObservabilitySnapshot {
  WarmcopyDrainMetrics metrics;
  Warmcopy_historical_work_counters phase2_historical_work;
  uint64_t descriptor_hwm{0};
};

/*
  Optional sink for human-readable warmcopy events.

  The sink is not part of the correctness path; it exists so tests and
  diagnostics can observe progress without coupling to private participant
  fields.
*/
class WarmcopyObservabilitySink {
 public:
  virtual ~WarmcopyObservabilitySink() = default;
  virtual void publish(const std::string &message) = 0;
};

/*
  Model/helper used by preserve_trx_warmcopy gunit coverage.  The production
  drain path uses Mysql_binlog_warmcopy_session together with
  Warmcopy_batch_drain_participant in preserve_trx.cc.
*/
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

/*
  State model for one binlog warmcopy candidate.

  A participant starts as discovered, becomes ready only when copied/durable
  ranges and metadata agree, and degrades when it cannot prove the cache can be
  adopted safely. A degraded participant may still allow the transaction to be
  preserved through live export or single-phase copy, but it cannot publish its
  warm artifact as authoritative.
*/
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
  /* Latest source/mirror metadata snapshot used by ready_requirements_met(). */
  WarmcopyParticipantMetadata m_metadata;
  /* Latest reason that made the participant unusable for warm artifact adoption. */
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

/*
  Model/helper used by preserve_trx_warmcopy gunit coverage.  It is not the
  production DRAIN TRANSACTIONS PRESERVE coordinator.
*/
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

/*
  Adapter for the gunit coordinator model above.  Production warmcopy
  participation is wired through Warmcopy_batch_drain_participant.
*/
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

/*
  Derive the remaining per-entry capacity from total reserved bytes and the
  global warm external blob budget. false means a usable capacity was written to
  entry_blob_limit; true means invalid accounting or no safe capacity, so the
  caller must degrade or fail closed.
*/
bool warmcopy_entry_blob_limit(uint64_t total_reserved_bytes,
                               uint64_t entry_reserved_bytes,
                               uint64_t max_total_bytes,
                               uint64_t *entry_blob_limit);

/*
  Compute the maximum blob bytes one participant may name after accounting for
  the drain-wide byte budget and per-entry cap. These values are artifact
  reservation limits for warm external blobs; they are not a measurement of
  process heap memory.
*/
bool warmcopy_effective_entry_blob_limit(uint64_t total_reserved_bytes,
                                         uint64_t entry_reserved_bytes,
                                         uint64_t max_total_bytes,
                                         uint64_t max_entry_blob_bytes,
                                         uint64_t *entry_blob_limit);

/*
  Reserve the prefix plus as much tail budget as still fits in the entry limit.
  false means reservation was written. true means the prefix cannot be
  represented, the output pointer is invalid, or accounting is otherwise unsafe.
*/
bool warmcopy_reservation_with_tail_budget(uint64_t prefix_bytes,
                                           uint64_t tail_budget_bytes,
                                           uint64_t entry_blob_limit,
                                           uint64_t *reservation);

/*
  Keep accounting at least as large as the already opened session limit. The
  accounted value is what the drain coordinator reserves for the artifact, not
  bytes already copied or resident in memory.
*/
bool warmcopy_accounted_session_reservation(uint64_t session_blob_limit,
                                            uint64_t requested_reservation,
                                            uint64_t *accounted_reservation);

/*
  Enforce pending tail-range limits before adding a new mirrored range. Returning
  true means the participant must degrade because the range count or total
  pending bytes would exceed configured bounds, or because accounting overflow
  or invalid inputs made the range unsafe to represent.
*/
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
