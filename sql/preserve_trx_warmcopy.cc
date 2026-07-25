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

#include "sql/preserve_trx_warmcopy.h"

#include <algorithm>
#include <limits>
#include <sstream>
#include <utility>

#include "my_sys.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "sql/sql_class.h"

namespace {

constexpr uint64_t kWarmcopyMaxReservationChunkBytes = 1024 * 1024;

bool uint64_add_overflows(uint64_t left, uint64_t right) {
  return right > std::numeric_limits<uint64_t>::max() - left;
}

uint64_t saturating_subtract(uint64_t left, uint64_t right) {
  return left > right ? left - right : 0;
}

}  // namespace

class WarmcopyErrorLogObservabilitySink final
    : public WarmcopyObservabilitySink {
 public:
  void publish(const std::string &message) override {
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
  }
};

void Warmcopy_descriptor_tracker::note_range_covered(
    uint64_t offset, uint64_t length, uint64_t truncate_generation) {
  if (length == 0 || uint64_add_overflows(offset, length)) return;

  /*
    Covered ranges are accepted only within one truncate generation. A truncate
    changes the byte identity of later offsets, so ranges from an older
    generation are ignored rather than merged into the descriptor window.
  */
  if (!m_generation_initialized) {
    m_truncate_generation = truncate_generation;
    m_generation_initialized = true;
  }
  if (truncate_generation != m_truncate_generation) return;

  Covered_range next{offset, offset + length};
  std::vector<Covered_range> merged;
  bool inserted = false;
  for (const Covered_range &range : m_ranges) {
    if (range.end < next.begin) {
      merged.push_back(range);
      continue;
    }
    if (next.end < range.begin) {
      if (!inserted) {
        merged.push_back(next);
        inserted = true;
      }
      merged.push_back(range);
      continue;
    }
    next.begin = std::min(next.begin, range.begin);
    next.end = std::max(next.end, range.end);
  }
  if (!inserted) merged.push_back(next);
  m_ranges.swap(merged);
  recompute_covered_until();
}

void Warmcopy_descriptor_tracker::note_truncate(
    uint64_t length, uint64_t truncate_generation) {
  if (m_generation_initialized && truncate_generation < m_truncate_generation) {
    return;
  }
  /*
    Truncate rewinds all high-water marks because bytes past the new source
    length are no longer part of the cache. The descriptor can advance again
    only after coverage, digest and durable watermarks catch up.
  */
  m_truncate_generation = truncate_generation;
  m_generation_initialized = true;

  std::vector<Covered_range> trimmed;
  for (Covered_range range : m_ranges) {
    if (range.begin >= length) continue;
    range.end = std::min(range.end, length);
    trimmed.push_back(range);
  }
  m_ranges.swap(trimmed);

  m_durable_until = std::min(m_durable_until, length);
  m_digest_until = std::min(m_digest_until, length);
  m_descriptor_hwm = std::min(m_descriptor_hwm, length);
  m_descriptor.size = m_descriptor_hwm;
  recompute_covered_until();
}

void Warmcopy_descriptor_tracker::note_flush_durable(uint64_t durable_until) {
  m_durable_until = std::max(m_durable_until, durable_until);
}

bool Warmcopy_descriptor_tracker::advance_descriptor_hwm(
    uint64_t target_length) {
  /*
    The descriptor is publishable only for bytes that are both copied into the
    warm artifact and durable in the carrier. Digest progress is advanced with
    the descriptor so phase-2 accounting can report remaining historical work.
  */
  if (target_length > m_covered_until || target_length > m_durable_until) {
    return false;
  }
  if (target_length < m_descriptor_hwm) return false;

  m_digest_until = target_length;
  m_descriptor_hwm = target_length;
  m_descriptor.name = kPreservedTrxBlobBinlogCache;
  m_descriptor.size = target_length;
  return true;
}

Preserved_trx_external_blob_descriptor Warmcopy_descriptor_tracker::descriptor()
    const {
  Preserved_trx_external_blob_descriptor descriptor = m_descriptor;
  descriptor.name = kPreservedTrxBlobBinlogCache;
  descriptor.size = m_descriptor_hwm;
  return descriptor;
}

Warmcopy_historical_work_counters
Warmcopy_descriptor_tracker::phase2_historical_work_counters() const {
  Warmcopy_historical_work_counters counters;
  counters.digest_bytes = saturating_subtract(m_covered_until, m_digest_until);
  counters.durable_bytes = saturating_subtract(m_covered_until, m_durable_until);
  counters.scan_bytes = counters.digest_bytes;
  return counters;
}

void Warmcopy_descriptor_tracker::recompute_covered_until() {
  uint64_t covered_until = 0;
  for (const Covered_range &range : m_ranges) {
    if (range.begin > covered_until) break;
    covered_until = std::max(covered_until, range.end);
  }
  m_covered_until = covered_until;
}

Binlog_warmcopy_participant_state WarmcopyParticipant::state() const {
  /*
    State is derived from metadata while the participant is non-terminal. This
    keeps observations current even when mirror progress advances without an
    explicit state transition call.
  */
  if (terminal()) return m_state;
  if (ready_requirements_met()) return Binlog_warmcopy_participant_state::READY;
  if (m_metadata.mirror_active) {
    return Binlog_warmcopy_participant_state::MIRRORING;
  }
  if (m_metadata.copied_until > 0) {
    return Binlog_warmcopy_participant_state::COPYING_PREFIX;
  }
  return m_state;
}

bool WarmcopyParticipant::ready() const { return ready_requirements_met(); }

bool WarmcopyParticipant::tail_within_budget(uint64_t current_source_length,
                                             uint64_t tail_budget_bytes) const {
  if (current_source_length < m_metadata.copied_until) return false;
  return current_source_length - m_metadata.copied_until <= tail_budget_bytes;
}

void WarmcopyParticipant::note_prefix_progress(uint64_t copied_until) {
  if (terminal()) return;
  m_metadata.copied_until = std::max(m_metadata.copied_until, copied_until);
  m_state = Binlog_warmcopy_participant_state::COPYING_PREFIX;
}

void WarmcopyParticipant::note_source_metadata(
    WarmcopyParticipantMetadata metadata) {
  if (terminal()) return;
  m_metadata = metadata;
  if (ready_requirements_met()) {
    m_state = Binlog_warmcopy_participant_state::READY;
  } else if (m_metadata.mirror_active) {
    m_state = Binlog_warmcopy_participant_state::MIRRORING;
  } else if (m_metadata.copied_until > 0) {
    m_state = Binlog_warmcopy_participant_state::COPYING_PREFIX;
  } else {
    m_state = Binlog_warmcopy_participant_state::DISCOVERED;
  }
}

void WarmcopyParticipant::mark_degraded(std::string reason) {
  if (m_state == Binlog_warmcopy_participant_state::ABANDONED ||
      m_state == Binlog_warmcopy_participant_state::FINALIZED) {
    return;
  }
  m_state = Binlog_warmcopy_participant_state::DEGRADED;
  m_degraded_reason = std::move(reason);
}

void WarmcopyParticipant::mark_abandoned() {
  if (m_state == Binlog_warmcopy_participant_state::ABANDONED ||
      m_state == Binlog_warmcopy_participant_state::FINALIZED) {
    return;
  }
  m_state = Binlog_warmcopy_participant_state::ABANDONED;
}

void WarmcopyParticipant::mark_finalized() {
  if (terminal()) return;
  m_state = Binlog_warmcopy_participant_state::FINALIZED;
}

bool WarmcopyParticipant::ready_requirements_met() const {
  /*
    READY means the warm artifact is a complete byte-for-byte stand-in for the
    source cache at the sampled source length. Metadata and savepoint/cache
    flags are part of the contract, not just byte counts.
  */
  if (terminal()) return false;
  const uint64_t source_length = m_metadata.source_length;
  return m_metadata.destination_length == source_length &&
         m_metadata.copied_until >= source_length &&
         m_metadata.durable_until >= source_length &&
         m_metadata.digest_until >= source_length &&
         m_metadata.descriptor_hwm >= source_length &&
         m_metadata.cache_flags_match &&
         m_metadata.compression_metadata_match &&
         m_metadata.savepoint_cache_state_match && m_metadata.mirror_active;
}

bool WarmcopyParticipant::terminal() const {
  return m_state == Binlog_warmcopy_participant_state::ABANDONED ||
         m_state == Binlog_warmcopy_participant_state::DEGRADED ||
         m_state == Binlog_warmcopy_participant_state::FINALIZED;
}

bool WarmcopyDrainCoordinator::open_epoch(THD *coordinator_thd) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_state != Binlog_warmcopy_coordinator_state::IDLE &&
      m_state != Binlog_warmcopy_coordinator_state::CLOSED &&
      m_state != Binlog_warmcopy_coordinator_state::ABORTED) {
    return false;
  }
  m_coordinator_thd = coordinator_thd;
  m_participants.clear();
  m_commands_blocked = false;
  m_admission_open = true;
  m_closing_deadline_us = 0;
  ++m_admission_sequence;
  m_enumeration_started_sequence = m_admission_sequence;
  m_next_participant_id = 0;
  m_state = Binlog_warmcopy_coordinator_state::OPEN;
  return true;
}

bool WarmcopyDrainCoordinator::publish_open_gate_before_enumeration() {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_state != Binlog_warmcopy_coordinator_state::OPEN) return false;
  /*
    The open gate is published before target enumeration so sessions that enter
    warmcopy-capable code during enumeration can be assigned a participant id
    instead of being discovered only after phase 2 starts.
  */
  if (!m_admission_open) {
    m_admission_open = true;
    ++m_admission_sequence;
  }
  if (m_enumeration_started_sequence == 0) {
    m_enumeration_started_sequence = m_admission_sequence;
  }
  return true;
}

bool WarmcopyDrainCoordinator::reconcile_open_admission_sequence() {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_state != Binlog_warmcopy_coordinator_state::OPEN ||
      !m_admission_open) {
    return false;
  }
  /*
    A sequence change during enumeration means admission raced with the scanner.
    The caller must retry the enumeration pass so every admitted participant is
    represented before phase 1 can be considered complete.
  */
  if (m_admission_sequence != m_enumeration_started_sequence) {
    m_enumeration_started_sequence = m_admission_sequence;
    return false;
  }
  return true;
}

bool WarmcopyDrainCoordinator::admit_thd_if_needed(THD *target_thd) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_state != Binlog_warmcopy_coordinator_state::OPEN ||
      !m_admission_open) {
    return false;
  }
  if (target_thd != nullptr) {
    mysql_mutex_lock(&target_thd->LOCK_thd_data);
    if (target_thd->preserve_trx_warmcopy_participant_id == 0) {
      target_thd->preserve_trx_warmcopy_participant_id =
          ++m_next_participant_id;
      ++m_admission_sequence;
    }
    mysql_mutex_unlock(&target_thd->LOCK_thd_data);
  }
  return true;
}

bool WarmcopyDrainCoordinator::admit_participant_for_test(
    WarmcopyParticipant *participant) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_state != Binlog_warmcopy_coordinator_state::OPEN ||
      !m_admission_open) {
    return false;
  }
  if (participant != nullptr &&
      std::find(m_participants.begin(), m_participants.end(), participant) !=
          m_participants.end()) {
    return true;
  }
  m_participants.push_back(participant);
  ++m_admission_sequence;
  return true;
}

bool WarmcopyDrainCoordinator::begin_closing(unsigned long timeout_ms) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_state != Binlog_warmcopy_coordinator_state::OPEN) return false;
  /*
    Closing is the transition from background copying to the blocking boundary.
    New participants are no longer admitted, and command gates can start
    rejecting work that would create a new cache tail after the final seal.
  */
  m_admission_open = false;
  m_commands_blocked = true;
  m_closing_deadline_us =
      timeout_ms == 0 ? 0 : my_micro_time() + timeout_ms * 1000ULL;
  ++m_admission_sequence;
  m_state = Binlog_warmcopy_coordinator_state::CLOSING;
  return true;
}

bool WarmcopyDrainCoordinator::all_active_participants_ready() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  for (const WarmcopyParticipant *participant : m_participants) {
    if (participant_blocks_closing(participant)) return false;
  }
  return true;
}

bool WarmcopyDrainCoordinator::has_active_degraded_participant() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  for (const WarmcopyParticipant *participant : m_participants) {
    if (participant != nullptr &&
        participant->state() == Binlog_warmcopy_participant_state::DEGRADED) {
      return true;
    }
  }
  return false;
}

void WarmcopyDrainCoordinator::abort_and_cleanup(
    WarmcopyParticipantCleanup *cleanup) {
  std::lock_guard<std::mutex> guard(m_mutex);
  /*
    Abort removes only warm artifacts that were never finalized into a snapshot.
    Published snapshots are owned by the carrier/token lifecycle and are not
    cleaned through the drain coordinator.
  */
  for (const WarmcopyParticipant *participant : m_participants) {
    if (cleanup == nullptr || participant == nullptr) continue;
    const Binlog_warmcopy_participant_state state = participant->state();
    if (state == Binlog_warmcopy_participant_state::ABANDONED ||
        state == Binlog_warmcopy_participant_state::DEGRADED) {
      cleanup->remove_warm_artifact(*participant);
    }
  }
  m_admission_open = false;
  m_commands_blocked = false;
  m_closing_deadline_us = 0;
  m_state = Binlog_warmcopy_coordinator_state::ABORTED;
}

uint64_t WarmcopyDrainCoordinator::closing_deadline_us() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_closing_deadline_us;
}

Binlog_warmcopy_coordinator_state WarmcopyDrainCoordinator::state() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_state;
}

bool WarmcopyDrainCoordinator::admission_open() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_admission_open;
}

bool WarmcopyDrainCoordinator::commands_blocked() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_commands_blocked;
}

uint64_t WarmcopyDrainCoordinator::admission_sequence() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_admission_sequence;
}

uint64_t WarmcopyDrainCoordinator::enumeration_started_sequence() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_enumeration_started_sequence;
}

size_t WarmcopyDrainCoordinator::participant_count() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_participants.size();
}

bool WarmcopyDrainCoordinator::participant_blocks_closing(
    const WarmcopyParticipant *participant) const {
  if (participant == nullptr) return false;
  const Binlog_warmcopy_participant_state state = participant->state();
  if (state == Binlog_warmcopy_participant_state::ABANDONED ||
      state == Binlog_warmcopy_participant_state::FINALIZED) {
    return false;
  }
  return !participant->ready();
}

WarmcopyDrainParticipantAdapter::WarmcopyDrainParticipantAdapter(
    WarmcopyDrainCoordinator *coordinator, THD *coordinator_thd,
    unsigned long close_timeout_ms)
    : m_coordinator(coordinator),
      m_coordinator_thd(coordinator_thd),
      m_close_timeout_ms(close_timeout_ms) {}

bool WarmcopyDrainParticipantAdapter::open_phase1() {
  return m_coordinator != nullptr &&
         m_coordinator->open_epoch(m_coordinator_thd) &&
         m_coordinator->publish_open_gate_before_enumeration();
}

bool WarmcopyDrainParticipantAdapter::close_phase1() {
  return m_coordinator != nullptr &&
         m_coordinator->begin_closing(m_close_timeout_ms);
}

bool WarmcopyDrainParticipantAdapter::phase1_ready() const {
  return m_coordinator != nullptr &&
         m_coordinator->all_active_participants_ready();
}

bool WarmcopyDrainParticipantAdapter::phase2_preflight(
    Preserve_trx_drain_phase_mode mode) {
  /*
    The drain orchestrator treats the binlog participant like any other phase-2
    dependency: by preflight, admission is closed and every active participant
    must either be READY or the whole warmcopy route degrades.
  */
  return mode == Preserve_trx_drain_phase_mode::TWO_PHASE &&
         m_coordinator != nullptr &&
         m_coordinator->state() == Binlog_warmcopy_coordinator_state::CLOSING &&
         !m_coordinator->admission_open() &&
         m_coordinator->all_active_participants_ready() &&
         !m_coordinator->has_active_degraded_participant();
}

void WarmcopyDrainParticipantAdapter::abort_phase() {
  if (m_coordinator != nullptr) m_coordinator->abort_and_cleanup();
}

void WarmcopyDrainParticipantAdapter::finalize_phase() {}

WarmcopyDrainMetrics warmcopy_metrics_for_participants(
    std::initializer_list<const WarmcopyParticipant *> participants) {
  WarmcopyDrainMetrics metrics;
  for (const WarmcopyParticipant *participant : participants) {
    if (participant == nullptr) continue;
    ++metrics.participants_discovered;
    const WarmcopyParticipantMetadata &metadata = participant->metadata();
    switch (participant->state()) {
      case Binlog_warmcopy_participant_state::READY:
        ++metrics.participants_ready;
        break;
      case Binlog_warmcopy_participant_state::ABANDONED:
        ++metrics.participants_abandoned;
        break;
      case Binlog_warmcopy_participant_state::DEGRADED:
        ++metrics.participants_degraded;
        break;
      case Binlog_warmcopy_participant_state::DISCOVERED:
      case Binlog_warmcopy_participant_state::COPYING_PREFIX:
      case Binlog_warmcopy_participant_state::MIRRORING:
      case Binlog_warmcopy_participant_state::FINALIZED:
        break;
    }

    metrics.prefix_bytes += metadata.copied_until;
    metrics.mirrored_bytes +=
        saturating_subtract(metadata.destination_length, metadata.copied_until);
    metrics.tail_bytes +=
        saturating_subtract(metadata.source_length, metadata.copied_until);
    metrics.digested_bytes += metadata.digest_until;
    metrics.durable_bytes += metadata.durable_until;
  }
  return metrics;
}

bool warmcopy_has_active_degraded_participant(
    std::initializer_list<const WarmcopyParticipant *> participants) {
  for (const WarmcopyParticipant *participant : participants) {
    if (participant != nullptr &&
        participant->state() == Binlog_warmcopy_participant_state::DEGRADED) {
      return true;
    }
  }
  return false;
}

bool warmcopy_reserve_entry_growth(
    std::atomic<uint64_t> *total_reserved_bytes, uint64_t max_total_bytes,
    uint64_t max_entry_blob_bytes, uint64_t reservation_chunk_bytes,
    uint64_t required_bytes, uint64_t *entry_reserved_bytes) {
  if (total_reserved_bytes == nullptr || entry_reserved_bytes == nullptr ||
      reservation_chunk_bytes == 0 ||
      *entry_reserved_bytes > max_entry_blob_bytes ||
      required_bytes > max_entry_blob_bytes) {
    return true;
  }
  if (required_bytes <= *entry_reserved_bytes) return false;

  /*
    Prefix-copy chunks may be tuned much larger for throughput. Do not charge
    every small transaction that full copy chunk before those bytes exist.
  */
  const uint64_t accounting_chunk_bytes =
      std::min(reservation_chunk_bytes, kWarmcopyMaxReservationChunkBytes);
  const uint64_t chunks =
      required_bytes / accounting_chunk_bytes +
      (required_bytes % accounting_chunk_bytes == 0 ? 0 : 1);
  uint64_t target_reservation = max_entry_blob_bytes;
  if (chunks <=
      std::numeric_limits<uint64_t>::max() / accounting_chunk_bytes) {
    target_reservation =
        std::min(max_entry_blob_bytes, chunks * accounting_chunk_bytes);
  }
  if (target_reservation < required_bytes ||
      target_reservation < *entry_reserved_bytes) {
    return true;
  }

  const uint64_t delta = target_reservation - *entry_reserved_bytes;
  uint64_t total = total_reserved_bytes->load(std::memory_order_relaxed);
  for (;;) {
    if (total > max_total_bytes || delta > max_total_bytes - total) return true;
    if (total_reserved_bytes->compare_exchange_weak(
            total, total + delta, std::memory_order_relaxed,
            std::memory_order_relaxed)) {
      *entry_reserved_bytes = target_reservation;
      return false;
    }
  }
}

bool warmcopy_retain_entry_reservation(
    std::atomic<uint64_t> *total_reserved_bytes, uint64_t retained_bytes,
    uint64_t *entry_reserved_bytes) {
  if (total_reserved_bytes == nullptr || entry_reserved_bytes == nullptr ||
      retained_bytes > *entry_reserved_bytes) {
    return true;
  }
  const uint64_t released_bytes = *entry_reserved_bytes - retained_bytes;
  uint64_t total = total_reserved_bytes->load(std::memory_order_relaxed);
  for (;;) {
    if (released_bytes > total) return true;
    if (total_reserved_bytes->compare_exchange_weak(
            total, total - released_bytes, std::memory_order_relaxed,
            std::memory_order_relaxed)) {
      *entry_reserved_bytes = retained_bytes;
      return false;
    }
  }
}

bool warmcopy_unpublished_tail_bytes(uint64_t final_length,
                                     uint64_t published_high_watermark,
                                     uint64_t *tail_bytes) {
  if (tail_bytes == nullptr || published_high_watermark > final_length) {
    return true;
  }
  *tail_bytes = final_length - published_high_watermark;
  return false;
}

bool warmcopy_pending_range_limit_exceeded(uint64_t pending_range_count,
                                           uint64_t pending_range_bytes,
                                           uint64_t new_range_bytes,
                                           bool creates_new_range,
                                           uint64_t max_range_count,
                                           uint64_t max_pending_bytes,
                                           uint64_t *next_pending_range_bytes) {
  if (next_pending_range_bytes == nullptr) return true;
  *next_pending_range_bytes = 0;
  if (creates_new_range && pending_range_count >= max_range_count) return true;
  if (new_range_bytes >
      std::numeric_limits<uint64_t>::max() - pending_range_bytes) {
    return true;
  }

  const uint64_t next_bytes = pending_range_bytes + new_range_bytes;
  if (next_bytes > max_pending_bytes) return true;
  *next_pending_range_bytes = next_bytes;
  return false;
}

void warmcopy_publish_observability(
    const WarmcopyObservabilitySnapshot &snapshot,
    WarmcopyObservabilitySink *sink) {
  if (sink == nullptr) return;

  std::ostringstream message;
  message << "PRESERVE: warm-copy drain metrics"
          << " participants_discovered="
          << snapshot.metrics.participants_discovered
          << " participants_ready=" << snapshot.metrics.participants_ready
          << " participants_abandoned="
          << snapshot.metrics.participants_abandoned
          << " participants_degraded="
          << snapshot.metrics.participants_degraded
          << " prefix_bytes=" << snapshot.metrics.prefix_bytes
          << " mirrored_bytes=" << snapshot.metrics.mirrored_bytes
          << " tail_bytes=" << snapshot.metrics.tail_bytes
          << " digested_bytes=" << snapshot.metrics.digested_bytes
          << " durable_bytes=" << snapshot.metrics.durable_bytes
          << " descriptor_hwm=" << snapshot.descriptor_hwm
          << " phase1_us=" << snapshot.metrics.phase1_us
          << " phase2_pause_us=" << snapshot.metrics.phase2_pause_us
          << " phase2_copy_bytes="
          << snapshot.phase2_historical_work.prefix_copy_bytes
          << " phase2_digest_bytes="
          << snapshot.phase2_historical_work.digest_bytes
          << " phase2_durable_bytes="
          << snapshot.phase2_historical_work.durable_bytes
          << " phase2_scan_bytes=" << snapshot.phase2_historical_work.scan_bytes;
  sink->publish(message.str());
}

void warmcopy_publish_observability_to_error_log(
    const WarmcopyObservabilitySnapshot &snapshot) {
  WarmcopyErrorLogObservabilitySink sink;
  warmcopy_publish_observability(snapshot, &sink);
}
