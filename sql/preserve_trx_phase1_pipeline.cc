/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/preserve_trx_phase1_pipeline.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <new>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>

#include "my_loglevel.h"
#include "my_thread.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "sql/current_thd.h"
#include "sql/debug_sync.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_phase1_binlog_adapter.h"
#include "sql/preserve_trx_phase1_publication.h"
#include "sql/preserve_trx_phase1_record_adapter.h"
#include "sql/sql_class.h"
#include "sql/sql_thd_internal_api.h"
#include "storage/innobase/include/lock0warmcopy.h"
#include "storage/innobase/include/trx0preserve.h"

ulong preserve_trx_phase1_capture_mode =
    PRESERVE_TRX_PHASE1_BOUNDED_PIPELINE_V1;
uint preserve_trx_phase1_pipeline_workers = 6;
uint preserve_trx_phase1_pipeline_ordinary_active_limit = 64;
ulonglong preserve_trx_phase1_pipeline_credit_bytes = 1073741824ULL;
ulonglong preserve_trx_phase1_pipeline_record_reserve_bytes = 536870912ULL;
ulonglong preserve_trx_phase1_pipeline_binlog_reserve_bytes = 268435456ULL;
uint preserve_trx_phase1_pipeline_copy_chunk_bytes = 1048576;
ulonglong preserve_trx_phase1_pipeline_cleanup_reserve_us = 1000000ULL;
uint preserve_trx_phase1_pipeline_result_slots = 256;
ulonglong preserve_trx_phase1_pipeline_tail_record_credit_bytes = 67108864ULL;

namespace {

#ifndef DBUG_OFF
constexpr uint64_t kDebugExerciseTimeoutUs = 5000000ULL;
#endif

uint64_t monotonic_us() {
  using clock = std::chrono::steady_clock;
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          clock::now().time_since_epoch())
          .count());
}

size_t family_index(Preserve_trx_phase1_pipeline_family family) {
  return family == Preserve_trx_phase1_pipeline_family::RECORD_LOCK ? 0 : 1;
}

struct Phase1_pipeline_family_key {
  uint64_t target_thread_id{0};
  uint64_t target_incarnation{0};
  Preserve_trx_phase1_pipeline_family family{
      Preserve_trx_phase1_pipeline_family::RECORD_LOCK};
};

bool family_keys_equal(const Phase1_pipeline_family_key &left,
                       const Phase1_pipeline_family_key &right) {
  return left.target_thread_id == right.target_thread_id &&
         left.target_incarnation == right.target_incarnation &&
         left.family == right.family;
}

enum class Phase1_pipeline_slot_state : uint8_t {
  FREE,
  QUEUED,
  ADMITTED,
  RESULT_READY,
  OWNER_HELD,
  PUBLICATION_PENDING
};

enum class Phase1_pipeline_final_admission : uint8_t {
  NOT_OPENED,
  OPEN,
  CLOSED
};

struct Phase1_pipeline_slot {
  uint32_t generation{0};
  Phase1_pipeline_slot_state state{Phase1_pipeline_slot_state::FREE};
  Preserve_trx_phase1_work_descriptor descriptor;
  Preserve_trx_phase1_prepared_result result;
  Preserve_trx_phase1_record_capture_handle record_capture_payload;
  uint64_t credit_bytes{0};
  uint64_t cancel_revision{0};
  uint32_t active_native_operations{0};
  uint32_t active_no_wait_operations{0};
  bool executor_active{false};
  bool ordinary_active_slot{false};
};

class Phase1_pipeline_token_ring {
 public:
  void initialize(size_t capacity) {
    m_tokens.assign(capacity, 0);
    m_head = 0;
    m_size = 0;
  }

  bool push(uint64_t token) {
    if (token == 0 || m_tokens.empty() || m_size == m_tokens.size())
      return false;
    const size_t tail = (m_head + m_size) % m_tokens.size();
    m_tokens[tail] = token;
    ++m_size;
    return true;
  }

  bool pop(uint64_t *token) {
    if (token == nullptr || m_size == 0) return false;
    *token = m_tokens[m_head];
    m_tokens[m_head] = 0;
    m_head = (m_head + 1) % m_tokens.size();
    --m_size;
    return true;
  }

  bool empty() const { return m_size == 0; }
  size_t size() const { return m_size; }

 private:
  std::vector<uint64_t> m_tokens;
  size_t m_head{0};
  size_t m_size{0};
};

enum class Phase1_pipeline_operation_status : uint8_t {
  GRANTED,
  NOT_ADMITTED,
  CANCELLED,
  DEADLINE,
  CLOSED
};

enum class Phase1_pipeline_operation_stage : uint8_t {
  RECORD_CAPTURE,
  RECORD_STORE_SNAPSHOT,
  RECORD_PREPARE,
  BINLOG_PREPARE
};

class Phase1_pipeline_operation_permit {
 public:
  using Release_callback = void (*)(void *, uint64_t,
                                    Preserve_trx_phase1_pipeline_operation_kind,
                                    Phase1_pipeline_operation_stage, bool,
                                    uint64_t, uint64_t);

  Phase1_pipeline_operation_permit() = default;
  ~Phase1_pipeline_operation_permit() { release(); }

  Phase1_pipeline_operation_permit(
      const Phase1_pipeline_operation_permit &) = delete;
  Phase1_pipeline_operation_permit &operator=(
      const Phase1_pipeline_operation_permit &) = delete;

  explicit operator bool() const { return m_owner != nullptr; }

  void arm(void *owner, uint64_t admission_id,
           Preserve_trx_phase1_pipeline_operation_kind kind,
           Phase1_pipeline_operation_stage stage, bool final_generation,
           uint64_t started_us, uint64_t worst_case_us,
           Release_callback callback) {
    release();
    m_owner = owner;
    m_admission_id = admission_id;
    m_kind = kind;
    m_stage = stage;
    m_final_generation = final_generation;
    m_started_us = started_us;
    m_worst_case_us = worst_case_us;
    m_callback = callback;
  }

  void release() {
    if (m_owner == nullptr) return;
    void *owner = m_owner;
    m_owner = nullptr;
    m_callback(owner, m_admission_id, m_kind, m_stage, m_final_generation,
               m_started_us, m_worst_case_us);
  }

 private:
  void *m_owner{nullptr};
  uint64_t m_admission_id{0};
  Preserve_trx_phase1_pipeline_operation_kind m_kind{
      Preserve_trx_phase1_pipeline_operation_kind::NATIVE_WAIT_CAPABLE};
  Phase1_pipeline_operation_stage m_stage{
      Phase1_pipeline_operation_stage::RECORD_CAPTURE};
  bool m_final_generation{false};
  uint64_t m_started_us{0};
  uint64_t m_worst_case_us{0};
  Release_callback m_callback{nullptr};
};

const char *phase1_pipeline_lifecycle_name(
    Preserve_trx_phase1_pipeline_lifecycle lifecycle) {
  switch (lifecycle) {
    case Preserve_trx_phase1_pipeline_lifecycle::STARTING:
      return "STARTING";
    case Preserve_trx_phase1_pipeline_lifecycle::RUNNING:
      return "RUNNING";
    case Preserve_trx_phase1_pipeline_lifecycle::FINALIZING:
      return "FINALIZING";
    case Preserve_trx_phase1_pipeline_lifecycle::CANCELING:
      return "CANCELING";
    case Preserve_trx_phase1_pipeline_lifecycle::STOPPED:
      return "STOPPED";
  }
  return "UNKNOWN";
}

void destroy_pipeline_worker_thd(THD *worker_thd, bool thread_initialized) {
  if (worker_thd != nullptr) destroy_thd(worker_thd);
  if (thread_initialized) my_thread_end();
}

}  // namespace

class Preserve_trx_phase1_pipeline::Impl {
 public:
  Impl(const Preserve_trx_phase1_pipeline_config &config,
       Preserve_trx_phase1_binlog_provider_port *binlog_provider)
      : m_config(config), m_binlog_provider(binlog_provider) {}

  ~Impl() {
    const bool clean = join_while_draining();
    assert(clean);
  }

  bool start() {
    if (!config_valid()) {
      {
        std::lock_guard<std::mutex> invalid_guard(m_mutex);
        ++m_init_failures;
        m_lifecycle = Preserve_trx_phase1_pipeline_lifecycle::STOPPED;
        advance_revision_locked();
      }
      log_event("START_FAILED");
      return false;
    }
    m_pipeline_credit_bytes = m_config.credit_bytes;
    m_pipeline_record_reserve_bytes = m_config.record_reserve_bytes;
    m_pipeline_binlog_reserve_bytes = m_config.binlog_reserve_bytes;
    m_ordinary_active_limit =
        std::min(m_config.ordinary_active_limit, m_config.worker_count);

    std::unique_lock<std::mutex> guard(m_mutex);
    if (m_lifecycle != Preserve_trx_phase1_pipeline_lifecycle::STARTING ||
        !initialize_storage_locked()) {
      ++m_init_failures;
      m_lifecycle = Preserve_trx_phase1_pipeline_lifecycle::STOPPED;
      advance_revision_locked();
      guard.unlock();
      log_event("START_FAILED");
      return false;
    }

    const uint32_t expected_threads = m_config.worker_count + 1;
    uint32_t created_threads = 0;
    try {
      m_threads.reserve(expected_threads);
      m_threads.emplace_back([this]() { sequencer_main(); });
      ++created_threads;
      for (uint32_t worker_index = 0; worker_index < m_config.worker_count;
           ++worker_index) {
        m_threads.emplace_back(
            [this, worker_index]() { worker_main(worker_index); });
        ++created_threads;
      }
    } catch (...) {
      m_init_failures += expected_threads - created_threads;
      m_init_reports += expected_threads - created_threads;
    }

    m_condition.wait(guard,
                     [&] { return m_init_reports == expected_threads; });
    const bool ready =
        m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::STARTING &&
        m_init_failures == 0 && m_sequencer_ready &&
        m_workers_ready == m_config.worker_count;
    if (ready) {
      m_lifecycle = Preserve_trx_phase1_pipeline_lifecycle::RUNNING;
    } else if (m_lifecycle ==
               Preserve_trx_phase1_pipeline_lifecycle::STARTING) {
      m_lifecycle = Preserve_trx_phase1_pipeline_lifecycle::CANCELING;
      ++m_cancel_revision;
    }
    advance_revision_locked();
    guard.unlock();
    m_condition.notify_all();

    if (!ready) {
      join_threads();
      {
        std::lock_guard<std::mutex> stop_guard(m_mutex);
        m_lifecycle = Preserve_trx_phase1_pipeline_lifecycle::STOPPED;
        advance_revision_locked();
      }
      m_condition.notify_all();
      log_event("START_FAILED");
      return false;
    }

    log_event("STARTED");
    return true;
  }

  Preserve_trx_phase1_pipeline_submit_status try_submit(
      const Preserve_trx_phase1_work_descriptor &descriptor) {
    return try_submit_impl(descriptor, false);
  }

  Preserve_trx_phase1_pipeline_submit_status try_submit_final(
      const Preserve_trx_phase1_work_descriptor &descriptor) {
    return try_submit_impl(descriptor, true);
  }

#ifndef DBUG_OFF
  void note_family_demand(Preserve_trx_phase1_pipeline_family family,
                          bool has_demand) {
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      m_debug_family_demand[family_index(family)] = has_demand;
      advance_revision_locked();
    }
    m_condition.notify_all();
  }
#endif

  Preserve_trx_phase1_pipeline_credit_status try_grow_credit(
      uint64_t admission_id, uint64_t required_total_bytes) {
    Preserve_trx_phase1_pipeline_credit_status status =
        Preserve_trx_phase1_pipeline_credit_status::NOT_ADMITTED;
    bool changed = false;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      Phase1_pipeline_slot *slot = slot_for_token_locked(admission_id);
      if (slot == nullptr ||
          slot->state != Phase1_pipeline_slot_state::ADMITTED ||
          !slot->executor_active) {
        status = Preserve_trx_phase1_pipeline_credit_status::NOT_ADMITTED;
      } else if (!work_still_valid_locked(*slot)) {
        status = Preserve_trx_phase1_pipeline_credit_status::CANCELLED;
      } else if (deadline_reached_locked() &&
                 (slot->descriptor.final_generation ||
                  slot->descriptor.family !=
                      Preserve_trx_phase1_pipeline_family::BINLOG_CACHE)) {
        ++m_submit_deadline;
        status = Preserve_trx_phase1_pipeline_credit_status::DEADLINE;
      } else if (required_total_bytes <= slot->credit_bytes) {
        status = Preserve_trx_phase1_pipeline_credit_status::GRANTED;
      } else {
        const uint64_t additional = required_total_bytes - slot->credit_bytes;
        if (!can_reserve_credit_locked(slot->descriptor.family,
                                       slot->descriptor.final_generation,
                                       additional)) {
          ++m_submit_no_credit;
          status = Preserve_trx_phase1_pipeline_credit_status::NO_CREDIT;
        } else {
          reserve_credit_locked(slot, additional);
          advance_revision_locked();
          changed = true;
          status = Preserve_trx_phase1_pipeline_credit_status::GRANTED;
        }
      }
    }
    if (changed) m_condition.notify_all();
    return status;
  }

  bool try_pop_result(Preserve_trx_phase1_prepared_result *result) {
    if (result == nullptr) return false;
    std::lock_guard<std::mutex> guard(m_mutex);
    uint64_t token = 0;
    if (!m_result_queue.pop(&token)) return false;
    Phase1_pipeline_slot *slot = slot_for_token_locked(token);
    if (slot == nullptr ||
        slot->state != Phase1_pipeline_slot_state::RESULT_READY) {
      fail_invariant_locked();
      return false;
    }
    *result = std::move(slot->result);
    slot->state = Phase1_pipeline_slot_state::OWNER_HELD;
    advance_revision_locked();
    m_condition.notify_all();
    return true;
  }

  bool settle_result(
      uint64_t admission_id,
      Preserve_trx_phase1_pipeline_result_disposition disposition) {
    bool settled = false;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      Phase1_pipeline_slot *slot = slot_for_token_locked(admission_id);
      if (slot == nullptr ||
          slot->state != Phase1_pipeline_slot_state::OWNER_HELD) {
        return false;
      }
      if (disposition ==
              Preserve_trx_phase1_pipeline_result_disposition::ABSENT &&
          slot->result.status !=
              Preserve_trx_phase1_pipeline_result_status::ABSENT) {
        return false;
      }
      if (disposition ==
              Preserve_trx_phase1_pipeline_result_disposition::RETAINED &&
          (!slot->descriptor.final_generation ||
           (slot->result.status !=
                Preserve_trx_phase1_pipeline_result_status::PREPARED &&
            slot->result.status !=
                Preserve_trx_phase1_pipeline_result_status::ABSENT))) {
        return false;
      }
      const bool retained =
          disposition ==
          Preserve_trx_phase1_pipeline_result_disposition::RETAINED;
      if (retained && !commit_tail_credit_locked(slot)) {
        advance_revision_locked();
      } else {
        const bool rollback_tail_credit =
            slot->descriptor.final_generation && !retained;
        free_slot_locked(slot, rollback_tail_credit);
        advance_revision_locked();
        settled = true;
      }
    }
    m_condition.notify_all();
    return settled;
  }

  bool begin_publication(uint64_t admission_id) {
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      Phase1_pipeline_slot *slot = slot_for_token_locked(admission_id);
      if (slot == nullptr ||
          slot->state != Phase1_pipeline_slot_state::OWNER_HELD ||
          slot->result.status !=
              Preserve_trx_phase1_pipeline_result_status::PREPARED ||
          slot->cancel_revision != m_cancel_revision ||
          !publication_lifecycle_matches_locked(*slot)) {
        return false;
      }
      slot->state = Phase1_pipeline_slot_state::PUBLICATION_PENDING;
      advance_revision_locked();
    }
    m_condition.notify_all();
    return true;
  }

  bool settle_publication(
      uint64_t admission_id,
      Preserve_trx_phase1_pipeline_publication_status status) {
    bool completed = false;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      Phase1_pipeline_slot *slot = slot_for_token_locked(admission_id);
      if (slot == nullptr ||
          slot->state != Phase1_pipeline_slot_state::PUBLICATION_PENDING)
        return false;
      free_slot_locked(slot, false);
      if (status ==
          Preserve_trx_phase1_pipeline_publication_status::ABORTED) {
        ++m_publication_aborted;
      } else if (status !=
                 Preserve_trx_phase1_pipeline_publication_status::OK) {
        ++m_publication_failures;
        if (status ==
            Preserve_trx_phase1_pipeline_publication_status::ACK_UNCERTAIN)
          ++m_publication_ack_uncertain;
        enter_canceling_locked();
      }
      advance_revision_locked();
      completed = true;
    }
    if (completed) m_condition.notify_all();
    return completed;
  }

  uint64_t abort_residual_publications_after_sender_join() {
    uint64_t aborted = 0;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      if (m_lifecycle != Preserve_trx_phase1_pipeline_lifecycle::CANCELING)
        return 0;
      for (Phase1_pipeline_slot &slot : m_slots) {
        if (slot.state !=
            Phase1_pipeline_slot_state::PUBLICATION_PENDING)
          continue;
        free_slot_locked(&slot, false);
        ++aborted;
      }
      m_publication_aborted += aborted;
      if (aborted != 0) advance_revision_locked();
    }
    if (aborted != 0) m_condition.notify_all();
    return aborted;
  }

  bool publish_stage_deadline(uint64_t stage_started_us,
                              uint64_t stage_deadline_us) {
    bool published = false;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      const uint64_t now_us = monotonic_us();
      const bool budget_addition_valid =
          stage_started_us <= std::numeric_limits<uint64_t>::max() -
                                  m_config.phase2_budget_us;
      const uint64_t maximum_stage_deadline_us =
          budget_addition_valid
              ? stage_started_us + m_config.phase2_budget_us
              : 0;
      if (m_lifecycle != Preserve_trx_phase1_pipeline_lifecycle::RUNNING ||
          m_phase2_deadline_published || !m_wait_permit_admission_open ||
          stage_started_us < m_config.phase1_started_us ||
          stage_started_us > now_us || m_active_jobs != 0 ||
          ordinary_outstanding_locked() != 0 ||
          m_active_operation_permits != 0 || m_no_wait_active_operations != 0 ||
          !budget_addition_valid ||
          stage_deadline_us > maximum_stage_deadline_us ||
          !deadline_has_cleanup_reserve(stage_deadline_us)) {
        return false;
      }
      m_stage_deadline_us = stage_deadline_us;
      m_operation_cutoff_us = stage_deadline_us - m_config.cleanup_reserve_us;
      m_phase2_deadline_published = true;
      m_wait_permit_admission_open = false;
      m_ordinary_admission_open = false;
      ++m_cancel_revision;
      cancel_ordinary_queues_locked("stage_deadline_published");
      published =
          m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::RUNNING &&
          !m_wait_permit_admission_open && !m_ordinary_admission_open &&
          m_record_sequence_queue.empty() && m_record_ready_queue.empty() &&
          m_binlog_ready_queue.empty();
      advance_revision_locked();
    }
    m_condition.notify_all();
    return published;
  }

  bool begin_finalizing() {
    bool transitioned = false;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      if (m_lifecycle != Preserve_trx_phase1_pipeline_lifecycle::RUNNING ||
          !m_phase2_deadline_published ||
          m_wait_permit_admission_open || m_ordinary_admission_open ||
          m_active_jobs != 0 || ordinary_outstanding_locked() != 0 ||
          m_active_operation_permits != 0)
        return false;
      m_lifecycle = Preserve_trx_phase1_pipeline_lifecycle::FINALIZING;
      m_final_admission = Phase1_pipeline_final_admission::NOT_OPENED;
      ++m_cancel_revision;
      cancel_ordinary_queues_locked("finalizing_before_admit");
      transitioned =
          m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::FINALIZING &&
          m_record_sequence_queue.empty() && m_record_ready_queue.empty() &&
          m_binlog_ready_queue.empty();
      advance_revision_locked();
    }
    m_condition.notify_all();
    return transitioned;
  }

  bool open_final_admission(uint64_t final_deadline_us) {
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      if (m_lifecycle != Preserve_trx_phase1_pipeline_lifecycle::FINALIZING ||
          m_final_admission != Phase1_pipeline_final_admission::NOT_OPENED ||
          ordinary_outstanding_locked() != 0 ||
          m_active_operation_permits != 0 ||
          m_no_wait_active_operations != 0 ||
          final_deadline_us != m_stage_deadline_us ||
          !deadline_has_cleanup_reserve(final_deadline_us)) {
        return false;
      }
      m_final_admission = Phase1_pipeline_final_admission::OPEN;
      advance_revision_locked();
    }
    m_condition.notify_all();
    return true;
  }

  void close_final_admission() {
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      if (m_lifecycle != Preserve_trx_phase1_pipeline_lifecycle::FINALIZING ||
          m_final_admission != Phase1_pipeline_final_admission::OPEN) {
        return;
      }
      m_final_admission = Phase1_pipeline_final_admission::CLOSED;
      advance_revision_locked();
    }
    m_condition.notify_all();
  }

  bool finish_and_join() {
    {
      std::unique_lock<std::mutex> guard(m_mutex);
      if (m_lifecycle != Preserve_trx_phase1_pipeline_lifecycle::FINALIZING ||
          m_final_admission != Phase1_pipeline_final_admission::CLOSED ||
          !all_work_queues_empty_locked() ||
          !m_result_queue.empty() || outstanding_slots_locked() != 0 ||
          m_active_jobs != 0 || m_active_ordinary_jobs != 0 ||
          m_active_operation_permits != 0 ||
          m_no_wait_active_operations != 0 ||
          !credit_empty_and_balanced_locked()) {
        return false;
      }
      m_lifecycle = Preserve_trx_phase1_pipeline_lifecycle::STOPPED;
      advance_revision_locked();
    }
    m_condition.notify_all();
    join_threads();
    log_event("STOPPED");
    return true;
  }

  void cancel() {
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      enter_canceling_locked();
    }
    m_condition.notify_all();
  }

  bool wait_for_change(uint64_t observed_revision, uint64_t timeout_us,
                       uint64_t *new_revision) {
    std::unique_lock<std::mutex> guard(m_mutex);
    const bool changed = m_condition.wait_for(
        guard, std::chrono::microseconds(timeout_us),
        [&] { return m_event_revision != observed_revision; });
    if (new_revision != nullptr) *new_revision = m_event_revision;
    return changed;
  }

  bool join_while_draining() {
    cancel();
    std::vector<Preserve_trx_phase1_binlog_prepared_handle>
        binlog_cleanup;
    binlog_cleanup.reserve(m_slots.size());
    {
      std::unique_lock<std::mutex> guard(m_mutex);
      for (;;) {
        drain_abort_results_locked(&binlog_cleanup);
        if (m_active_jobs == 0) break;
        const uint64_t observed_revision = m_event_revision;
        m_condition.wait(guard, [&] {
          return m_event_revision != observed_revision || m_active_jobs == 0;
        });
      }
    }
    binlog_cleanup.clear();
    join_threads();
    bool should_log = false;
    bool clean = false;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      drain_abort_results_locked(&binlog_cleanup);
      clean = all_work_queues_empty_locked() && m_result_queue.empty() &&
              outstanding_slots_locked() == 0 && m_active_jobs == 0 &&
              m_active_ordinary_jobs == 0 &&
              m_active_operation_permits == 0 &&
              m_no_wait_active_operations == 0 &&
              credit_empty_and_balanced_locked();
      if (clean &&
          m_lifecycle != Preserve_trx_phase1_pipeline_lifecycle::STOPPED) {
        m_lifecycle = Preserve_trx_phase1_pipeline_lifecycle::STOPPED;
        advance_revision_locked();
        should_log = true;
      } else if (!clean) {
        ++m_invariant_failures;
        advance_revision_locked();
      }
    }
    m_condition.notify_all();
    if (should_log)
      log_event("STOPPED");
    else if (!clean)
      log_event("STOP_BLOCKED");
    return clean;
  }

  Preserve_trx_phase1_pipeline_snapshot snapshot() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    Preserve_trx_phase1_pipeline_snapshot result;
    result.lifecycle = m_lifecycle;
    result.event_revision = m_event_revision;
    result.queued = queued_count_locked();
    result.admitted = outstanding_slots_locked();
    result.inflight = inflight_slots_locked();
    result.result_count = result_slots_locked();
    result.credit_in_use_bytes = m_credit_in_use_bytes;
    result.record_credit_in_use_bytes = m_family_credit_in_use_bytes[0];
    result.binlog_credit_in_use_bytes = m_family_credit_in_use_bytes[1];
    result.tail_record_credit_consumed_bytes = m_tail_credit_consumed_bytes[0];
    result.cancel_revision = m_cancel_revision;
    result.operation_cutoff_us = m_operation_cutoff_us;
    result.active_operation_permits = m_active_operation_permits;
    result.no_wait_active_operations = m_no_wait_active_operations;
    result.operation_permits_started = m_operation_permits_started;
    result.operation_permit_budget_rejected =
        m_operation_permit_budget_rejected;
    result.operation_budget_overruns = m_operation_budget_overruns;
    result.ordinary_binlog_slow_operations = m_ordinary_binlog_slow_operations;
    result.final_record_capture_operation_samples =
        m_final_record_capture_operation_samples;
    result.final_record_capture_operation_us_total =
        m_final_record_capture_operation_us_total;
    result.final_record_capture_operation_us_max =
        m_final_record_capture_operation_us_max;
    result.final_record_capture_operation_overruns =
        m_final_record_capture_operation_overruns;
    result.final_record_store_snapshot_operation_samples =
        m_final_record_store_snapshot_operation_samples;
    result.final_record_store_snapshot_operation_us_total =
        m_final_record_store_snapshot_operation_us_total;
    result.final_record_store_snapshot_operation_us_max =
        m_final_record_store_snapshot_operation_us_max;
    result.final_record_store_snapshot_operation_overruns =
        m_final_record_store_snapshot_operation_overruns;
    result.final_record_prepare_operation_samples =
        m_final_record_prepare_operation_samples;
    result.final_record_prepare_operation_us_total =
        m_final_record_prepare_operation_us_total;
    result.final_record_prepare_operation_us_max =
        m_final_record_prepare_operation_us_max;
    result.final_record_prepare_operation_overruns =
        m_final_record_prepare_operation_overruns;
    result.record_capture_latch_samples = m_record_capture_latch_samples;
    result.record_capture_latch_wait_us = m_record_capture_latch_wait_us;
    result.record_capture_latch_hold_us = m_record_capture_latch_hold_us;
    result.record_capture_latch_envelope_us =
        m_record_capture_latch_envelope_us;
    result.record_capture_latch_max_envelope_us =
        m_record_capture_latch_max_envelope_us;
    result.effective_pipeline_credit_bytes = m_pipeline_credit_bytes;
    result.effective_record_reserve_bytes = m_pipeline_record_reserve_bytes;
    result.effective_binlog_reserve_bytes = m_pipeline_binlog_reserve_bytes;
    result.publication_failures = m_publication_failures;
    result.publication_ack_uncertain = m_publication_ack_uncertain;
    result.publication_aborted = m_publication_aborted;
    result.submit_no_slot = m_submit_no_slot;
    result.submit_no_credit = m_submit_no_credit;
    result.submit_deadline = m_submit_deadline;
    result.invariant_failures = m_invariant_failures;
    result.ordinary_active = m_active_ordinary_jobs;
    result.ordinary_active_high_water = m_ordinary_active_high_water;
    result.ordinary_active_limit_deferrals =
        m_ordinary_active_limit_deferrals;
    result.workers_configured = m_config.worker_count;
    result.ordinary_active_limit_requested =
        m_config.ordinary_active_limit;
    result.ordinary_active_limit_effective = m_ordinary_active_limit;
    result.workers_ready = m_workers_ready;
    result.init_failures = m_init_failures;
    result.sequencer_ready = m_sequencer_ready;
    return result;
  }

#ifndef DBUG_OFF
  bool debug_exercise_core() {
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      if (m_lifecycle != Preserve_trx_phase1_pipeline_lifecycle::RUNNING ||
          m_config.result_slots < 2 || m_config.record_reserve_bytes == 0 ||
          m_config.binlog_reserve_bytes == 0 ||
          m_config.tail_record_credit_bytes == 0) {
        return false;
      }
      m_debug_hold_executors = true;
      m_debug_prepare_results = true;
      m_debug_hold_first_ordinary =
          m_ordinary_active_limit < m_config.worker_count;
      m_debug_first_ordinary_held = false;
      m_debug_cap_deferral_observed = false;
    }
    bool passed = preserve_trx_phase1_publication_debug_exercise();
    note_family_demand(Preserve_trx_phase1_pipeline_family::RECORD_LOCK, true);
    note_family_demand(Preserve_trx_phase1_pipeline_family::BINLOG_CACHE, true);

    const uint64_t small_credit = std::min<uint64_t>(
        65536ULL, std::min(m_pipeline_record_reserve_bytes,
                          m_pipeline_binlog_reserve_bytes));
    Preserve_trx_phase1_work_descriptor record =
        debug_descriptor(101, Preserve_trx_phase1_pipeline_family::RECORD_LOCK,
                         small_credit, false);
    Preserve_trx_phase1_work_descriptor binlog =
        debug_descriptor(102, Preserve_trx_phase1_pipeline_family::BINLOG_CACHE,
                         small_credit, false);
    Preserve_trx_phase1_work_descriptor third =
        debug_descriptor(103, Preserve_trx_phase1_pipeline_family::RECORD_LOCK,
                         small_credit, false);
    Preserve_trx_phase1_work_descriptor invalid_capture_credit =
        debug_descriptor(109,
                         Preserve_trx_phase1_pipeline_family::RECORD_LOCK,
                         small_credit, false);
    invalid_capture_credit.capture_byte_limit = small_credit + 1;

    passed = passed &&
             try_submit(invalid_capture_credit) ==
                 Preserve_trx_phase1_pipeline_submit_status::
                     INVALID_DESCRIPTOR;

    passed = passed &&
             try_submit(record) ==
                 Preserve_trx_phase1_pipeline_submit_status::ADMITTED;
    passed = passed &&
             try_submit(record) ==
                 Preserve_trx_phase1_pipeline_submit_status::SINGLE_FLIGHT;

    const uint64_t shared_credit =
        m_pipeline_credit_bytes - m_pipeline_record_reserve_bytes -
        m_pipeline_binlog_reserve_bytes;
    if (m_pipeline_binlog_reserve_bytes != 0 &&
        m_pipeline_record_reserve_bytes <=
            std::numeric_limits<uint64_t>::max() - shared_credit - 1) {
      Preserve_trx_phase1_work_descriptor oversized = debug_descriptor(
          104, Preserve_trx_phase1_pipeline_family::RECORD_LOCK,
          m_pipeline_record_reserve_bytes + shared_credit + 1, false);
      passed = passed &&
               try_submit(oversized) ==
                   Preserve_trx_phase1_pipeline_submit_status::NO_CREDIT;
    }
    passed = passed &&
             try_submit(binlog) ==
                 Preserve_trx_phase1_pipeline_submit_status::ADMITTED;
    passed = passed &&
             try_submit(third) ==
                 Preserve_trx_phase1_pipeline_submit_status::NO_SLOT;

    {
      std::lock_guard<std::mutex> guard(m_mutex);
      m_debug_hold_executors = false;
      advance_revision_locked();
    }
    m_condition.notify_all();

    if (m_ordinary_active_limit < m_config.worker_count) {
      bool limit_observed = false;
      {
        std::unique_lock<std::mutex> guard(m_mutex);
        limit_observed = m_condition.wait_for(
            guard, std::chrono::microseconds(kDebugExerciseTimeoutUs), [&] {
              return m_debug_first_ordinary_held &&
                     m_debug_cap_deferral_observed;
            });
        m_debug_hold_first_ordinary = false;
        advance_revision_locked();
      }
      m_condition.notify_all();
      passed = passed && limit_observed;
    }

    passed = passed && wait_for_result_count(2, kDebugExerciseTimeoutUs);
    Preserve_trx_phase1_prepared_result held;
    passed = passed && try_pop_result(&held);
    passed = passed &&
             held.status == Preserve_trx_phase1_pipeline_result_status::PREPARED;
    passed = passed &&
             try_submit(third) ==
                 Preserve_trx_phase1_pipeline_submit_status::NO_SLOT;
    passed = passed && begin_publication(held.admission_id);
    passed = passed &&
             try_submit(third) ==
                 Preserve_trx_phase1_pipeline_submit_status::NO_SLOT;
    passed = passed && settle_publication(
                           held.admission_id,
                           Preserve_trx_phase1_pipeline_publication_status::OK);
    passed = passed &&
             try_submit(third) ==
                 Preserve_trx_phase1_pipeline_submit_status::ADMITTED;
    passed = passed && drop_debug_results(2, kDebugExerciseTimeoutUs);

    {
      std::lock_guard<std::mutex> guard(m_mutex);
      m_debug_prepare_results = false;
    }

    note_family_demand(Preserve_trx_phase1_pipeline_family::RECORD_LOCK, false);
    note_family_demand(Preserve_trx_phase1_pipeline_family::BINLOG_CACHE, false);

    /*
      T0 must reject outstanding ordinary work without changing its deadline
      or revision. The owner first drains that batch, then publishes T0.
    */
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      m_debug_hold_executors = true;
    }
    Preserve_trx_phase1_work_descriptor cutoff_binlog = debug_descriptor(
        108, Preserve_trx_phase1_pipeline_family::BINLOG_CACHE, small_credit,
        false);
    passed = passed &&
             try_submit(cutoff_binlog) ==
                 Preserve_trx_phase1_pipeline_submit_status::ADMITTED;
    const uint64_t phase2_started_us = monotonic_us();
    const uint64_t final_deadline =
        phase2_started_us <=
                std::numeric_limits<uint64_t>::max() -
                    m_config.phase2_budget_us
            ? phase2_started_us + m_config.phase2_budget_us
            : std::numeric_limits<uint64_t>::max();
    passed = passed && final_deadline > m_config.stage_deadline_us;
    if (final_deadline != std::numeric_limits<uint64_t>::max()) {
      passed = passed &&
               !publish_stage_deadline(phase2_started_us, final_deadline + 1);
    }
    const auto before_cutoff = snapshot();
    passed = passed &&
             !publish_stage_deadline(phase2_started_us, final_deadline);
    const Preserve_trx_phase1_pipeline_snapshot after_cutoff = snapshot();
    passed = passed && after_cutoff.queued == before_cutoff.queued &&
             after_cutoff.cancel_revision == before_cutoff.cancel_revision &&
             after_cutoff.operation_cutoff_us ==
                 before_cutoff.operation_cutoff_us &&
             after_cutoff.invariant_failures == 0;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      m_debug_prepare_results = true;
      m_debug_hold_executors = false;
      advance_revision_locked();
    }
    m_condition.notify_all();
    passed = passed && drop_debug_results(1, kDebugExerciseTimeoutUs);
    passed = passed && publish_stage_deadline(phase2_started_us, final_deadline);
    passed = passed && !publish_stage_deadline(phase2_started_us, final_deadline);
    passed = passed && begin_finalizing();
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      m_debug_prepare_results = false;
      m_debug_hold_executors = false;
      advance_revision_locked();
    }
    m_condition.notify_all();
    passed = passed &&
             try_submit(record) ==
                 Preserve_trx_phase1_pipeline_submit_status::NOT_RUNNING;
    passed = passed && open_final_admission(final_deadline);

    Preserve_trx_phase1_work_descriptor final_binlog = debug_descriptor(
        105, Preserve_trx_phase1_pipeline_family::BINLOG_CACHE, small_credit,
        true);
    passed = passed &&
             try_submit_final(final_binlog) ==
                 Preserve_trx_phase1_pipeline_submit_status::INVALID_DESCRIPTOR;
    if (m_config.tail_record_credit_bytes <
        std::numeric_limits<uint64_t>::max()) {
      Preserve_trx_phase1_work_descriptor tail_oversized = debug_descriptor(
          106, Preserve_trx_phase1_pipeline_family::RECORD_LOCK,
          m_config.tail_record_credit_bytes + 1, true);
      passed = passed &&
               try_submit_final(tail_oversized) ==
                   Preserve_trx_phase1_pipeline_submit_status::NO_CREDIT;
    }
    Preserve_trx_phase1_work_descriptor final_record = debug_descriptor(
        107, Preserve_trx_phase1_pipeline_family::RECORD_LOCK,
        std::min<uint64_t>(small_credit,
                           m_config.tail_record_credit_bytes),
        true);
    passed = passed &&
             try_submit_final(final_record) ==
                 Preserve_trx_phase1_pipeline_submit_status::ADMITTED;
    close_final_admission();
    passed = passed && drop_debug_results(1, kDebugExerciseTimeoutUs);

    const Preserve_trx_phase1_pipeline_snapshot before_finish = snapshot();
    passed = passed && before_finish.admitted == 0 &&
             before_finish.queued == 0 && before_finish.result_count == 0 &&
             before_finish.credit_in_use_bytes == 0 &&
             before_finish.active_operation_permits == 0 &&
             before_finish.operation_permits_started >= 4 &&
             before_finish.submit_no_slot >= 2 &&
             before_finish.submit_no_credit >= 2 &&
             before_finish.invariant_failures == 0;
    passed = passed && finish_and_join();
    if (passed) {
      log_event("CORE_EXERCISE_PASSED");
      return true;
    }
    join_while_draining();
    log_event("CORE_EXERCISE_FAILED");
    return false;
  }

  bool debug_exercise_record_adapter(THD *owner_thd, THD *target_thd,
                                     bool expect_empty) {
    if (owner_thd == nullptr || owner_thd != current_thd ||
        target_thd == nullptr || target_thd == owner_thd) {
      return false;
    }

    Preserve_trx_phase1_work_descriptor descriptor;
    Preserve_trx_external_thd_pin_handle target_pin;
    mysql_mutex_lock(&target_thd->LOCK_thd_data);
    if (target_thd->release_resources_done()) {
      mysql_mutex_unlock(&target_thd->LOCK_thd_data);
      return false;
    }
    descriptor.target_thread_id = target_thd->thread_id();
    descriptor.source_owner_cookie =
        static_cast<uint64_t>(reinterpret_cast<uintptr_t>(target_thd));
    descriptor.source_object_cookie =
        trx_preserve_phase1_peek_raw_cookie(target_thd);
    target_pin = preserve_trx_acquire_external_thd_pin_locked(target_thd);
    mysql_mutex_unlock(&target_thd->LOCK_thd_data);

    trx_preserve_phase1_identity identity;
    if (!target_pin || target_pin.thd() != target_thd ||
        !trx_preserve_phase1_owner_identity_snapshot(target_pin.thd(),
                                                     &identity) ||
        identity.owner_thd_cookie != descriptor.source_owner_cookie ||
        identity.raw_cookie != descriptor.source_object_cookie) {
      return false;
    }

    const uint64_t initial_credit = std::min<uint64_t>(
        m_pipeline_record_reserve_bytes, m_pipeline_credit_bytes);
    descriptor.attempt_id = m_config.attempt_id;
    descriptor.drain_generation = m_config.drain_generation;
    descriptor.target_incarnation = descriptor.source_owner_cookie;
    descriptor.family_version = 1;
    descriptor.expected_immutable_trx_id = identity.immutable_trx_id;
    descriptor.expected_trx_version = identity.trx_version;
    descriptor.capture_generation = 1;
    descriptor.item_limit = UINT32_MAX;
    descriptor.capture_byte_limit = initial_credit;
    descriptor.estimated_credit_bytes = initial_credit;
    descriptor.family = Preserve_trx_phase1_pipeline_family::RECORD_LOCK;
    if (descriptor.target_thread_id == 0 ||
        descriptor.source_object_cookie == 0 || initial_credit == 0) {
      return false;
    }

    struct Store_cleanup {
      explicit Store_cleanup(uint64_t id) : target_id(id) {}
      ~Store_cleanup() {
        lock_warmcopy_record_store_clear_for_target(target_id);
      }
      uint64_t target_id;
    } clear_store(descriptor.target_thread_id);
    lock_warmcopy_record_store_clear_for_target(descriptor.target_thread_id);

    lock_warmcopy_record_store_compare_token_t empty_store;
    if (!lock_warmcopy_record_store_compare_token_for_target(
            descriptor.target_thread_id, &empty_store) ||
        empty_store.epoch == 0) {
      return false;
    }
    descriptor.warmcopy_epoch = empty_store.epoch;
    target_pin = {};

    if (try_submit(descriptor) !=
        Preserve_trx_phase1_pipeline_submit_status::ADMITTED) {
      return false;
    }

    Preserve_trx_phase1_prepared_result prepared;
    const uint64_t deadline_us = monotonic_us() + kDebugExerciseTimeoutUs;
    for (;;) {
      if (try_pop_result(&prepared)) break;
      const Preserve_trx_phase1_pipeline_snapshot current = snapshot();
      if (current.lifecycle !=
              Preserve_trx_phase1_pipeline_lifecycle::RUNNING ||
          monotonic_us() >= deadline_us) {
        return false;
      }
      uint64_t ignored_revision = 0;
      const uint64_t remaining_us = deadline_us - monotonic_us();
      (void)wait_for_change(current.event_revision,
                            std::min<uint64_t>(remaining_us, 100000ULL),
                            &ignored_revision);
    }

    Preserve_trx_phase1_record_adapter_install_result installed;
    const Preserve_trx_phase1_pipeline_result_status expected_status =
        expect_empty ? Preserve_trx_phase1_pipeline_result_status::ABSENT
                     : Preserve_trx_phase1_pipeline_result_status::PREPARED;
    if (prepared.status == expected_status &&
        prepared.record_payload != nullptr) {
      preserve_trx_phase1_record_adapter_owner_install(
          descriptor, prepared.record_payload, &installed);
    }
    const bool installed_ok =
        installed.status == expected_status && installed.publication_token != 0 &&
        installed.installed_token.store_present &&
        (expect_empty ? installed.record_lock_count == 0
                      : installed.record_lock_count != 0);
    const bool settled = settle_result(
        prepared.admission_id,
        Preserve_trx_phase1_pipeline_result_disposition::DROP);
    if (!installed_ok || !settled) return false;

    std::ostringstream message;
    message << "PRESERVE_PHASE1_RECORD_ADAPTER_V1"
            << " event="
            << (expect_empty ? "EMPTY_PIPELINE_EXERCISE_PASSED"
                             : "PIPELINE_EXERCISE_PASSED")
            << " record_locks=" << installed.record_lock_count
            << " publication_token=" << installed.publication_token;
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.str().c_str());
    return true;
  }
#endif

 private:
  bool config_valid() const {
    if (m_config.attempt_id == 0 || m_config.drain_generation == 0 ||
        m_config.worker_count == 0 || m_config.ordinary_active_limit == 0 ||
        m_config.credit_bytes == 0 ||
        m_config.result_slots == 0 || m_config.copy_chunk_bytes == 0 ||
        m_config.phase2_budget_us <= m_config.cleanup_reserve_us ||
        m_config.cleanup_reserve_us < 1000000ULL ||
        m_config.native_wait_worst_case_us == 0 ||
        m_config.no_wait_worst_case_us == 0 ||
        m_config.record_store_snapshot_worst_case_us == 0 ||
        m_config.native_wait_worst_case_us > m_config.cleanup_reserve_us ||
        m_config.no_wait_worst_case_us > m_config.cleanup_reserve_us ||
        m_config.record_store_snapshot_worst_case_us >
            m_config.cleanup_reserve_us ||
        m_config.stage_deadline_us <= m_config.phase1_started_us) {
      return false;
    }
    if (m_config.record_reserve_bytes > m_config.credit_bytes ||
        m_config.binlog_reserve_bytes > m_config.credit_bytes ||
        m_config.record_reserve_bytes >
            m_config.credit_bytes - m_config.binlog_reserve_bytes) {
      return false;
    }
    if (m_config.tail_record_credit_bytes > m_config.credit_bytes) {
      return false;
    }
    return true;
  }

  bool initialize_storage_locked() {
    try {
      m_slots.resize(m_config.result_slots);
      m_record_sequence_queue.initialize(m_config.result_slots);
      m_record_ready_queue.initialize(m_config.result_slots);
      m_binlog_ready_queue.initialize(m_config.result_slots);
      m_final_record_sequence_queue.initialize(m_config.result_slots);
      m_final_ready_queue.initialize(m_config.result_slots);
      m_result_queue.initialize(m_config.result_slots);
    } catch (...) {
      return false;
    }
    m_stage_deadline_us = m_config.stage_deadline_us;
    m_operation_cutoff_us = m_config.stage_deadline_us;
    return true;
  }

  bool deadline_has_cleanup_reserve(uint64_t deadline_us) const {
    const uint64_t now_us = monotonic_us();
    return deadline_us > now_us &&
           deadline_us - now_us > m_config.cleanup_reserve_us;
  }

  bool deadline_reached_locked() const {
    return m_operation_cutoff_us == 0 || monotonic_us() >= m_operation_cutoff_us;
  }

  uint64_t operation_worst_case_us(
      Preserve_trx_phase1_pipeline_operation_kind kind,
      Phase1_pipeline_operation_stage stage) const {
    switch (kind) {
      case Preserve_trx_phase1_pipeline_operation_kind::NATIVE_WAIT_CAPABLE:
        return m_config.native_wait_worst_case_us;
      case Preserve_trx_phase1_pipeline_operation_kind::NO_WAIT_CHECK:
        return stage == Phase1_pipeline_operation_stage::RECORD_STORE_SNAPSHOT
                   ? m_config.record_store_snapshot_worst_case_us
                   : m_config.no_wait_worst_case_us;
    }
    return std::numeric_limits<uint64_t>::max();
  }

  static bool operation_is_wait_capable(
      Preserve_trx_phase1_pipeline_operation_kind kind) {
    return kind != Preserve_trx_phase1_pipeline_operation_kind::NO_WAIT_CHECK;
  }

  Phase1_pipeline_operation_status try_acquire_operation(
      uint64_t admission_id, uint64_t cancel_revision,
      Preserve_trx_phase1_pipeline_operation_kind kind,
      Phase1_pipeline_operation_stage stage, bool final_generation,
      Phase1_pipeline_operation_permit *permit) {
    if (permit == nullptr || static_cast<bool>(*permit))
      return Phase1_pipeline_operation_status::NOT_ADMITTED;

    Phase1_pipeline_operation_status status =
        Phase1_pipeline_operation_status::NOT_ADMITTED;
    uint64_t started_us = 0;
    uint64_t worst_case_us = 0;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      Phase1_pipeline_slot *slot = slot_for_token_locked(admission_id);
      if (slot == nullptr ||
          slot->state != Phase1_pipeline_slot_state::ADMITTED ||
          !slot->executor_active) {
        return status;
      }
      if (slot->cancel_revision != cancel_revision ||
          !work_still_valid_locked(*slot)) {
        return Phase1_pipeline_operation_status::CANCELLED;
      }
      const bool wait_capable = operation_is_wait_capable(kind);
      if (wait_capable && !m_wait_permit_admission_open) {
        return Phase1_pipeline_operation_status::CLOSED;
      }
      if (wait_capable &&
          m_lifecycle != Preserve_trx_phase1_pipeline_lifecycle::RUNNING) {
        return Phase1_pipeline_operation_status::CLOSED;
      }
      worst_case_us = operation_worst_case_us(kind, stage);
      started_us = monotonic_us();
      if (worst_case_us == 0 || m_operation_cutoff_us == 0 ||
          started_us >= m_operation_cutoff_us ||
          (final_generation &&
           worst_case_us > m_operation_cutoff_us - started_us)) {
        ++m_operation_permit_budget_rejected;
        advance_revision_locked();
        status = Phase1_pipeline_operation_status::DEADLINE;
      } else {
        switch (kind) {
          case Preserve_trx_phase1_pipeline_operation_kind::
              NATIVE_WAIT_CAPABLE:
            ++slot->active_native_operations;
            ++m_active_operation_permits;
            break;
          case Preserve_trx_phase1_pipeline_operation_kind::NO_WAIT_CHECK:
            ++slot->active_no_wait_operations;
            ++m_no_wait_active_operations;
            break;
        }
        ++m_operation_permits_started;
        advance_revision_locked();
        status = Phase1_pipeline_operation_status::GRANTED;
      }
    }
    m_condition.notify_all();
    if (status == Phase1_pipeline_operation_status::GRANTED) {
      permit->arm(this, admission_id, kind, stage, final_generation,
                  started_us, worst_case_us,
                  &Impl::release_operation_callback);
    }
    return status;
  }

  static void release_operation_callback(
      void *context, uint64_t admission_id,
      Preserve_trx_phase1_pipeline_operation_kind kind,
      Phase1_pipeline_operation_stage stage, bool final_generation,
      uint64_t started_us, uint64_t worst_case_us) {
    static_cast<Impl *>(context)->end_operation_internal(
        admission_id, kind, stage, final_generation, started_us,
        worst_case_us);
  }

  void end_operation_internal(
      uint64_t admission_id, Preserve_trx_phase1_pipeline_operation_kind kind,
      Phase1_pipeline_operation_stage stage, bool final_generation,
      uint64_t started_us, uint64_t worst_case_us) {
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      Phase1_pipeline_slot *slot = slot_for_token_locked(admission_id);
      bool counter_ok = slot != nullptr;
      if (counter_ok) {
        switch (kind) {
          case Preserve_trx_phase1_pipeline_operation_kind::
              NATIVE_WAIT_CAPABLE:
            counter_ok = slot->active_native_operations != 0 &&
                         m_active_operation_permits != 0;
            if (counter_ok) {
              --slot->active_native_operations;
              --m_active_operation_permits;
            }
            break;
          case Preserve_trx_phase1_pipeline_operation_kind::NO_WAIT_CHECK:
            counter_ok = slot->active_no_wait_operations != 0 &&
                         m_no_wait_active_operations != 0;
            if (counter_ok) {
              --slot->active_no_wait_operations;
              --m_no_wait_active_operations;
            }
            break;
        }
      }
      if (!counter_ok) {
        fail_invariant_locked();
      } else {
        const uint64_t finished_us = monotonic_us();
        const bool overrun = finished_us < started_us ||
                             finished_us - started_us > worst_case_us;
        const uint64_t elapsed_us =
            finished_us < started_us ? 0 : finished_us - started_us;
        note_final_record_operation_locked(stage, final_generation,
                                           elapsed_us, overrun);
        if (overrun) {
          // Synchronous prefix I/O has no hard wall-time bound. A slow
          // ordinary prepare is not corruption; cancellation still wins.
          if (finished_us >= started_us && !final_generation &&
              kind == Preserve_trx_phase1_pipeline_operation_kind::
                          NATIVE_WAIT_CAPABLE &&
              stage == Phase1_pipeline_operation_stage::BINLOG_PREPARE) {
            ++m_ordinary_binlog_slow_operations;
          } else {
            ++m_operation_budget_overruns;
            enter_canceling_locked();
          }
        }
      }
      advance_revision_locked();
    }
    m_condition.notify_all();
  }

  void report_thread_start(bool sequencer, bool ready) {
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      ++m_init_reports;
      if (ready) {
        if (sequencer)
          m_sequencer_ready = true;
        else
          ++m_workers_ready;
      } else {
        ++m_init_failures;
      }
      advance_revision_locked();
    }
    m_condition.notify_all();
  }

  THD *initialize_thread(bool force_failure, bool *thread_initialized) {
    *thread_initialized = false;
    if (force_failure || my_thread_init()) return nullptr;
    *thread_initialized = true;
    try {
      return create_thd(false, true, true, 0);
    } catch (...) {
      return nullptr;
    }
  }

  struct Adapter_control_context {
    Impl *pipeline{nullptr};
    uint64_t admission_id{0};
    uint64_t cancel_revision{0};
  };

  static bool adapter_cancel_probe(void *opaque) {
    const auto *context = static_cast<Adapter_control_context *>(opaque);
    if (context == nullptr || context->pipeline == nullptr) return true;
    return context->pipeline->adapter_work_cancelled(
        context->admission_id, context->cancel_revision);
  }

  static Preserve_trx_phase1_pipeline_credit_status adapter_reserve_credit(
      void *opaque, uint64_t required_total_bytes) {
    const auto *context = static_cast<Adapter_control_context *>(opaque);
    if (context == nullptr || context->pipeline == nullptr) {
      return Preserve_trx_phase1_pipeline_credit_status::NOT_ADMITTED;
    }
    return context->pipeline->try_grow_credit(context->admission_id,
                                               required_total_bytes);
  }

  bool adapter_work_cancelled(uint64_t admission_id,
                              uint64_t cancel_revision) {
    std::lock_guard<std::mutex> guard(m_mutex);
    Phase1_pipeline_slot *slot = slot_for_token_locked(admission_id);
    return slot == nullptr ||
           slot->state != Phase1_pipeline_slot_state::ADMITTED ||
           !slot->executor_active || slot->cancel_revision != cancel_revision ||
           !work_still_valid_locked(*slot);
  }

  static void saturating_add(uint64_t value, uint64_t *total) {
    if (total == nullptr) return;
    *total = value > std::numeric_limits<uint64_t>::max() - *total
                 ? std::numeric_limits<uint64_t>::max()
                 : *total + value;
  }

  void note_final_record_operation_locked(
      Phase1_pipeline_operation_stage stage, bool final_generation,
      uint64_t elapsed_us, bool overrun) {
    if (!final_generation) return;
    uint64_t *samples = nullptr;
    uint64_t *total_us = nullptr;
    uint64_t *max_us = nullptr;
    uint64_t *overruns = nullptr;
    switch (stage) {
      case Phase1_pipeline_operation_stage::RECORD_CAPTURE:
        samples = &m_final_record_capture_operation_samples;
        total_us = &m_final_record_capture_operation_us_total;
        max_us = &m_final_record_capture_operation_us_max;
        overruns = &m_final_record_capture_operation_overruns;
        break;
      case Phase1_pipeline_operation_stage::RECORD_STORE_SNAPSHOT:
        samples = &m_final_record_store_snapshot_operation_samples;
        total_us = &m_final_record_store_snapshot_operation_us_total;
        max_us = &m_final_record_store_snapshot_operation_us_max;
        overruns = &m_final_record_store_snapshot_operation_overruns;
        break;
      case Phase1_pipeline_operation_stage::RECORD_PREPARE:
        samples = &m_final_record_prepare_operation_samples;
        total_us = &m_final_record_prepare_operation_us_total;
        max_us = &m_final_record_prepare_operation_us_max;
        overruns = &m_final_record_prepare_operation_overruns;
        break;
      case Phase1_pipeline_operation_stage::BINLOG_PREPARE:
        return;
    }
    ++*samples;
    saturating_add(elapsed_us, total_us);
    *max_us = std::max(*max_us, elapsed_us);
    if (overrun) ++*overruns;
  }

  void note_record_capture_latch_locked(
      const Preserve_trx_phase1_record_adapter_outcome &outcome) {
    if (outcome.global_latch_envelope_us == 0) return;
    ++m_record_capture_latch_samples;
    saturating_add(outcome.global_latch_wait_us,
                   &m_record_capture_latch_wait_us);
    saturating_add(outcome.global_latch_hold_us,
                   &m_record_capture_latch_hold_us);
    saturating_add(outcome.global_latch_envelope_us,
                   &m_record_capture_latch_envelope_us);
    m_record_capture_latch_max_envelope_us =
        std::max(m_record_capture_latch_max_envelope_us,
                 outcome.global_latch_envelope_us);
  }

#ifndef DBUG_OFF
  bool debug_executors_held_locked() const { return m_debug_hold_executors; }
  bool debug_prepare_results_locked() const { return m_debug_prepare_results; }
  bool debug_worker_init_failure(uint32_t worker_index) const {
    return m_config.debug_fail_worker_init && worker_index == 0;
  }
#else
  static constexpr bool debug_executors_held_locked() { return false; }
  static constexpr bool debug_prepare_results_locked() { return false; }
  static constexpr bool debug_worker_init_failure(uint32_t) { return false; }
#endif

  void sequencer_main() {
    bool thread_initialized = false;
    THD *worker_thd = initialize_thread(false, &thread_initialized);
    report_thread_start(true, worker_thd != nullptr);
    if (worker_thd == nullptr) {
      destroy_pipeline_worker_thd(nullptr, thread_initialized);
      return;
    }

    for (;;) {
      uint64_t token = 0;
      uint64_t revision = 0;
      uint64_t operation_deadline_us = 0;
      bool final_generation = false;
      bool debug_prepare = false;
      Preserve_trx_phase1_work_descriptor descriptor;
      {
        std::unique_lock<std::mutex> guard(m_mutex);
        m_condition.wait(guard, [&] {
          return m_lifecycle ==
                     Preserve_trx_phase1_pipeline_lifecycle::CANCELING ||
                 m_lifecycle ==
                     Preserve_trx_phase1_pipeline_lifecycle::STOPPED ||
                 (!debug_executors_held_locked() &&
                  ((m_lifecycle ==
                        Preserve_trx_phase1_pipeline_lifecycle::RUNNING &&
                    m_ordinary_admission_open &&
                    !m_record_sequence_queue.empty()) ||
                   (m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::
                                           FINALIZING &&
                    !m_final_record_sequence_queue.empty())));
        });
        if (m_lifecycle ==
                Preserve_trx_phase1_pipeline_lifecycle::CANCELING ||
            m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::STOPPED) {
          break;
        }
        final_generation =
            m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::FINALIZING;
        if (!final_generation && !m_ordinary_admission_open) continue;
        Phase1_pipeline_token_ring *queue =
            final_generation ? &m_final_record_sequence_queue
                             : &m_record_sequence_queue;
        if (!queue->pop(&token)) continue;
        Phase1_pipeline_slot *slot = slot_for_token_locked(token);
        if (slot == nullptr ||
            slot->state != Phase1_pipeline_slot_state::QUEUED ||
            slot->descriptor.final_generation != final_generation) {
          fail_invariant_locked();
          continue;
        }
        slot->state = Phase1_pipeline_slot_state::ADMITTED;
        slot->cancel_revision = m_cancel_revision;
        slot->executor_active = true;
        ++m_active_jobs;
        revision = slot->cancel_revision;
        operation_deadline_us = m_operation_cutoff_us;
        debug_prepare = debug_prepare_results_locked();
        descriptor = slot->descriptor;
        advance_revision_locked();
      }
      m_condition.notify_all();

      Phase1_pipeline_operation_status operation_status;
      Preserve_trx_phase1_record_adapter_outcome capture_outcome;
      {
        Phase1_pipeline_operation_permit permit;
        const Preserve_trx_phase1_pipeline_operation_kind operation_kind =
            final_generation
                ? Preserve_trx_phase1_pipeline_operation_kind::NO_WAIT_CHECK
                : Preserve_trx_phase1_pipeline_operation_kind::
                      NATIVE_WAIT_CAPABLE;
        operation_status = try_acquire_operation(
            token, revision, operation_kind,
            descriptor.use_record_store_snapshot
                ? Phase1_pipeline_operation_stage::RECORD_STORE_SNAPSHOT
                : Phase1_pipeline_operation_stage::RECORD_CAPTURE,
            final_generation, &permit);
        if (permit) {
          DBUG_EXECUTE_IF(
              "preserve_trx_phase1_final_store_snapshot_delay_60ms", {
                if (final_generation &&
                    descriptor.use_record_store_snapshot) {
                  std::this_thread::sleep_for(std::chrono::milliseconds(60));
                }
              });
          if (debug_prepare) {
            capture_outcome.status =
                Preserve_trx_phase1_pipeline_result_status::PREPARED;
          } else {
            Adapter_control_context context;
            context.pipeline = this;
            context.admission_id = token;
            context.cancel_revision = revision;
            Preserve_trx_phase1_record_adapter_control control;
            control.deadline_us = operation_deadline_us;
            control.cancel_probe = &Impl::adapter_cancel_probe;
            control.cancel_context = &context;
            preserve_trx_phase1_record_adapter_capture(descriptor, control,
                                                       &capture_outcome);
          }
        }
      }

      DBUG_EXECUTE_IF("preserve_trx_phase1_record_handoff_delay", {
        if (!final_generation &&
            capture_outcome.status ==
                Preserve_trx_phase1_pipeline_result_status::PREPARED)
          std::this_thread::sleep_for(std::chrono::milliseconds(3500));
      });
      {
        std::lock_guard<std::mutex> guard(m_mutex);
        note_record_capture_latch_locked(capture_outcome);
        Phase1_pipeline_slot *slot = slot_for_token_locked(token);
        if (slot == nullptr) {
          fail_invariant_locked();
        } else {
          finish_executor_locked(slot);
          if (operation_status == Phase1_pipeline_operation_status::GRANTED &&
              work_still_valid_locked(*slot)) {
            if (capture_outcome.status ==
                    Preserve_trx_phase1_pipeline_result_status::PREPARED &&
                (debug_prepare ||
                 capture_outcome.capture_payload != nullptr) &&
                capture_outcome.required_credit_bytes <= slot->credit_bytes) {
              slot->record_capture_payload =
                  std::move(capture_outcome.capture_payload);
              Phase1_pipeline_token_ring *ready_queue =
                  slot->descriptor.final_generation ? &m_final_ready_queue
                                                    : &m_record_ready_queue;
              if (!ready_queue->push(token)) {
                fail_invariant_locked();
                publish_terminal_result_locked(
                    token,
                    Preserve_trx_phase1_pipeline_result_status::CANCELLED,
                    "record_ready_queue_invariant");
              }
            } else {
              if (capture_outcome.status ==
                      Preserve_trx_phase1_pipeline_result_status::PREPARED &&
                  !debug_prepare) {
                capture_outcome.status =
                    Preserve_trx_phase1_pipeline_result_status::
                        RESOURCE_EXHAUSTED;
                capture_outcome.reason =
                    "record_capture_exceeded_reserved_credit";
              }
              publish_adapter_result_locked(token, &capture_outcome);
            }
          } else {
            publish_operation_result_locked(token, operation_status,
                                            "record_sequence_cancelled");
          }
          advance_revision_locked();
        }
      }
      m_condition.notify_all();
    }
    destroy_pipeline_worker_thd(worker_thd, thread_initialized);
  }

  void worker_main(uint32_t worker_index) {
    bool thread_initialized = false;
    THD *worker_thd = initialize_thread(
        debug_worker_init_failure(worker_index), &thread_initialized);
    report_thread_start(false, worker_thd != nullptr);
    if (worker_thd == nullptr) {
      destroy_pipeline_worker_thd(nullptr, thread_initialized);
      return;
    }

    for (;;) {
      uint64_t token = 0;
      uint64_t revision = 0;
      uint64_t operation_deadline_us = 0;
      bool final_generation = false;
      bool debug_prepare = false;
      Preserve_trx_phase1_pipeline_family family =
          Preserve_trx_phase1_pipeline_family::RECORD_LOCK;
      Preserve_trx_phase1_work_descriptor descriptor;
      Preserve_trx_phase1_record_capture_handle capture_payload;
      {
        std::unique_lock<std::mutex> guard(m_mutex);
        for (;;) {
          if (m_lifecycle ==
                  Preserve_trx_phase1_pipeline_lifecycle::CANCELING ||
              m_lifecycle ==
                  Preserve_trx_phase1_pipeline_lifecycle::STOPPED) {
            break;
          }
          if (!debug_executors_held_locked() &&
              dispatchable_work_available_locked()) {
            break;
          }
          if (!debug_executors_held_locked() &&
              m_lifecycle ==
                  Preserve_trx_phase1_pipeline_lifecycle::RUNNING &&
              work_available_locked() &&
              !dispatchable_work_available_locked()) {
            ++m_ordinary_active_limit_deferrals;
#ifndef DBUG_OFF
            if (m_debug_hold_first_ordinary &&
                !m_debug_cap_deferral_observed) {
              m_debug_cap_deferral_observed = true;
              m_condition.notify_all();
            }
#endif
          }
          m_condition.wait(guard);
        }
        const bool stopping =
            m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::CANCELING ||
            m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::STOPPED;
        if (stopping) break;
        if (!take_work_locked(&token, &revision, &family,
                              &final_generation))
          continue;
        Phase1_pipeline_slot *slot = slot_for_token_locked(token);
        if (slot == nullptr) {
          fail_invariant_locked();
          continue;
        }
        descriptor = slot->descriptor;
        capture_payload = slot->record_capture_payload;
        operation_deadline_us = m_operation_cutoff_us;
        debug_prepare = debug_prepare_results_locked();
        advance_revision_locked();
      }
#ifndef DBUG_OFF
      if (!final_generation && debug_prepare) {
        std::unique_lock<std::mutex> guard(m_mutex);
        if (m_debug_hold_first_ordinary && !m_debug_first_ordinary_held) {
          m_debug_first_ordinary_held = true;
          advance_revision_locked();
          m_condition.notify_all();
          m_condition.wait(guard, [&] {
            return !m_debug_hold_first_ordinary ||
                   m_lifecycle !=
                       Preserve_trx_phase1_pipeline_lifecycle::RUNNING;
          });
        }
      }
#endif
      m_condition.notify_all();

      Phase1_pipeline_operation_status operation_status;
      Preserve_trx_phase1_record_adapter_outcome prepare_outcome;
      Preserve_trx_phase1_binlog_adapter_outcome binlog_prepare_outcome;
      {
        Phase1_pipeline_operation_permit permit;
        /* Both adapters reacquire native THD/trx identity before preparing. */
        const Preserve_trx_phase1_pipeline_operation_kind operation_kind =
            final_generation
                ? Preserve_trx_phase1_pipeline_operation_kind::NO_WAIT_CHECK
                : Preserve_trx_phase1_pipeline_operation_kind::
                      NATIVE_WAIT_CAPABLE;
        const Phase1_pipeline_operation_stage operation_stage =
            family == Preserve_trx_phase1_pipeline_family::RECORD_LOCK
                ? Phase1_pipeline_operation_stage::RECORD_PREPARE
                : Phase1_pipeline_operation_stage::BINLOG_PREPARE;
        operation_status = try_acquire_operation(
            token, revision, operation_kind, operation_stage,
            final_generation, &permit);
        if (permit) {
          if (debug_prepare) {
            if (family ==
                Preserve_trx_phase1_pipeline_family::RECORD_LOCK) {
              prepare_outcome.status =
                  Preserve_trx_phase1_pipeline_result_status::PREPARED;
            } else {
              binlog_prepare_outcome.status =
                  Preserve_trx_phase1_pipeline_result_status::PREPARED;
            }
          } else if (family ==
                     Preserve_trx_phase1_pipeline_family::RECORD_LOCK) {
            Adapter_control_context context;
            context.pipeline = this;
            context.admission_id = token;
            context.cancel_revision = revision;
            Preserve_trx_phase1_record_adapter_control control;
            control.deadline_us = operation_deadline_us;
            control.cancel_probe = &Impl::adapter_cancel_probe;
            control.cancel_context = &context;
            control.reserve_credit = &Impl::adapter_reserve_credit;
            control.credit_context = &context;
            preserve_trx_phase1_record_adapter_prepare(
                worker_thd, descriptor, capture_payload, control,
                &prepare_outcome);
          } else {
            Adapter_control_context context;
            context.pipeline = this;
            context.admission_id = token;
            context.cancel_revision = revision;
            Preserve_trx_phase1_binlog_adapter_control control;
            // Admission is bounded by Phase1. Finish this immutable prefix
            // after expiry, but keep all chunk cancellation checks active.
            control.deadline_us = 0;
            control.copy_chunk_bytes = m_config.copy_chunk_bytes;
            control.cancel_probe = &Impl::adapter_cancel_probe;
            control.cancel_context = &context;
            control.reserve_credit = &Impl::adapter_reserve_credit;
            control.credit_context = &context;
            if (m_binlog_provider == nullptr) {
              binlog_prepare_outcome.status =
                  Preserve_trx_phase1_pipeline_result_status::
                      ADAPTER_NOT_INSTALLED;
              binlog_prepare_outcome.reason =
                  "binlog_adapter_not_installed";
            } else {
              preserve_trx_phase1_binlog_adapter_prepare(
                  worker_thd, descriptor, m_binlog_provider, control,
                  &binlog_prepare_outcome);
              if (binlog_prepare_outcome.status ==
                  Preserve_trx_phase1_pipeline_result_status::PREPARED) {
                DBUG_EXECUTE_IF("preserve_trx_phase1_binlog_slow_prepare", {
                  std::this_thread::sleep_for(std::chrono::milliseconds(1200));
                });
              }
            }
          }
        }
      }

      DBUG_EXECUTE_IF("preserve_trx_phase1_hold_second_binlog_baseline", {
        if (family == Preserve_trx_phase1_pipeline_family::BINLOG_CACHE &&
            !descriptor.binlog_prefix_progress &&
            binlog_prepare_outcome.status ==
                Preserve_trx_phase1_pipeline_result_status::PREPARED &&
            m_debug_prepared_binlog_count.fetch_add(1) == 1) {
          const char action[] =
              "now SIGNAL continuous_baseline_held "
              "WAIT_FOR continuous_baseline_continue TIMEOUT 60";
          DBUG_ASSERT(!debug_sync_set_action(worker_thd,
                                             STRING_WITH_LEN(action)));
        }
      });

      {
        std::lock_guard<std::mutex> guard(m_mutex);
        Phase1_pipeline_slot *slot = slot_for_token_locked(token);
        if (slot == nullptr) {
          fail_invariant_locked();
        } else {
          finish_executor_locked(slot);
          slot->record_capture_payload.reset();
          if (operation_status == Phase1_pipeline_operation_status::GRANTED &&
              work_still_valid_locked(*slot)) {
            if (family ==
                Preserve_trx_phase1_pipeline_family::RECORD_LOCK) {
              publish_adapter_result_locked(token, &prepare_outcome);
            } else {
              publish_binlog_adapter_result_locked(token,
                                                   &binlog_prepare_outcome);
            }
          } else {
            publish_operation_result_locked(token, operation_status,
                                            "worker_cancelled");
          }
          advance_revision_locked();
        }
      }
      m_condition.notify_all();
    }
    destroy_pipeline_worker_thd(worker_thd, thread_initialized);
  }

  Preserve_trx_phase1_pipeline_submit_status try_submit_impl(
      const Preserve_trx_phase1_work_descriptor &descriptor,
      bool final_generation) {
    Preserve_trx_phase1_pipeline_submit_status status =
        Preserve_trx_phase1_pipeline_submit_status::INVALID_DESCRIPTOR;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      if (!descriptor_valid_locked(descriptor, final_generation)) return status;
      const bool ordinary_allowed =
          !final_generation && m_ordinary_admission_open &&
          m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::RUNNING;
      const bool final_allowed =
          final_generation &&
          m_final_admission == Phase1_pipeline_final_admission::OPEN &&
          m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::FINALIZING;
      if (!ordinary_allowed && !final_allowed)
        return Preserve_trx_phase1_pipeline_submit_status::NOT_RUNNING;
      if (deadline_reached_locked()) {
        ++m_submit_deadline;
        return Preserve_trx_phase1_pipeline_submit_status::DEADLINE;
      }
      const Phase1_pipeline_family_key key{
          descriptor.target_thread_id, descriptor.target_incarnation,
          descriptor.family};
      if (family_key_exists_locked(key))
        return Preserve_trx_phase1_pipeline_submit_status::SINGLE_FLIGHT;
      Phase1_pipeline_slot *slot = allocate_slot_locked();
      if (slot == nullptr) {
        ++m_submit_no_slot;
        return Preserve_trx_phase1_pipeline_submit_status::NO_SLOT;
      }
      if (!can_reserve_credit_locked(descriptor.family, final_generation,
                                     descriptor.estimated_credit_bytes)) {
        ++m_submit_no_credit;
        reset_unpublished_slot_locked(slot);
        return Preserve_trx_phase1_pipeline_submit_status::NO_CREDIT;
      }
      slot->descriptor = descriptor;
      slot->state = Phase1_pipeline_slot_state::QUEUED;
      reserve_credit_locked(slot, descriptor.estimated_credit_bytes);
      const uint64_t token = token_for_slot_locked(slot);
      Phase1_pipeline_token_ring *queue = nullptr;
      if (final_generation) {
        queue = &m_final_record_sequence_queue;
      } else if (descriptor.family ==
                 Preserve_trx_phase1_pipeline_family::RECORD_LOCK) {
        queue = &m_record_sequence_queue;
      } else {
        queue = &m_binlog_ready_queue;
      }
      if (!queue->push(token)) {
        fail_invariant_locked();
        release_credit_locked(slot, true);
        reset_unpublished_slot_locked(slot);
        return Preserve_trx_phase1_pipeline_submit_status::NO_SLOT;
      }
      advance_revision_locked();
      status = Preserve_trx_phase1_pipeline_submit_status::ADMITTED;
    }
    m_condition.notify_all();
    return status;
  }

  bool descriptor_valid_locked(
      const Preserve_trx_phase1_work_descriptor &descriptor,
      bool final_generation) const {
    if (descriptor.attempt_id != m_config.attempt_id ||
        descriptor.drain_generation != m_config.drain_generation ||
        descriptor.target_thread_id == 0 || descriptor.target_incarnation == 0 ||
        descriptor.family_version == 0 ||
        descriptor.estimated_credit_bytes == 0 ||
        descriptor.final_generation != final_generation) {
      return false;
    }
    if (descriptor.family !=
            Preserve_trx_phase1_pipeline_family::RECORD_LOCK &&
        descriptor.family !=
            Preserve_trx_phase1_pipeline_family::BINLOG_CACHE) {
      return false;
    }
    if (descriptor.family ==
            Preserve_trx_phase1_pipeline_family::RECORD_LOCK &&
        descriptor.capture_byte_limit > descriptor.estimated_credit_bytes) {
      return false;
    }
    if (descriptor.use_record_store_snapshot &&
        (!final_generation ||
         descriptor.family !=
             Preserve_trx_phase1_pipeline_family::RECORD_LOCK ||
         descriptor.expected_store_baseline_generation == 0)) {
      return false;
    }
    if (descriptor.binlog_prefix_progress &&
        (final_generation || descriptor.family !=
             Preserve_trx_phase1_pipeline_family::BINLOG_CACHE ||
         descriptor.binlog_prefix_size == 0 ||
         descriptor.binlog_minimum_delta_bytes == 0 ||
         descriptor.binlog_wire_chunk_bytes == 0)) {
      return false;
    }
    return !final_generation ||
           descriptor.family ==
               Preserve_trx_phase1_pipeline_family::RECORD_LOCK;
  }

  Phase1_pipeline_slot *allocate_slot_locked() {
    if (m_slots.empty()) return nullptr;
    for (size_t offset = 0; offset < m_slots.size(); ++offset) {
      const size_t index = (m_free_slot_cursor + offset) % m_slots.size();
      Phase1_pipeline_slot &slot = m_slots[index];
      if (slot.state != Phase1_pipeline_slot_state::FREE) continue;
      if (slot.generation == std::numeric_limits<uint32_t>::max()) {
        fail_invariant_locked();
        return nullptr;
      }
      ++slot.generation;
      slot.state = Phase1_pipeline_slot_state::QUEUED;
      m_free_slot_cursor = (index + 1) % m_slots.size();
      return &slot;
    }
    return nullptr;
  }

  void reset_unpublished_slot_locked(Phase1_pipeline_slot *slot) {
    if (slot == nullptr) return;
    const uint32_t generation = slot->generation;
    *slot = Phase1_pipeline_slot{};
    slot->generation = generation;
  }

  uint64_t token_for_slot_locked(const Phase1_pipeline_slot *slot) const {
    if (slot == nullptr || m_slots.empty()) return 0;
    const size_t index = static_cast<size_t>(slot - m_slots.data());
    if (index >= m_slots.size() || index >= UINT32_MAX) return 0;
    return (static_cast<uint64_t>(slot->generation) << 32) |
           static_cast<uint64_t>(index + 1);
  }

  Phase1_pipeline_slot *slot_for_token_locked(uint64_t token) {
    if (token == 0) return nullptr;
    const uint64_t raw_index = token & 0xffffffffULL;
    const uint32_t generation = static_cast<uint32_t>(token >> 32);
    if (raw_index == 0 || raw_index > m_slots.size() || generation == 0)
      return nullptr;
    Phase1_pipeline_slot &slot = m_slots[raw_index - 1];
    return slot.generation == generation ? &slot : nullptr;
  }

  bool family_key_exists_locked(const Phase1_pipeline_family_key &key) const {
    for (const Phase1_pipeline_slot &slot : m_slots) {
      if (slot.state == Phase1_pipeline_slot_state::FREE) continue;
      const Phase1_pipeline_family_key existing{
          slot.descriptor.target_thread_id,
          slot.descriptor.target_incarnation,
          slot.descriptor.family};
      if (family_keys_equal(existing, key)) return true;
    }
    return false;
  }

  bool publication_lifecycle_matches_locked(
      const Phase1_pipeline_slot &slot) const {
    return slot.descriptor.final_generation
               ? m_lifecycle ==
                     Preserve_trx_phase1_pipeline_lifecycle::FINALIZING
               : m_lifecycle ==
                     Preserve_trx_phase1_pipeline_lifecycle::RUNNING;
  }

  bool family_has_demand_locked(size_t index) const {
#ifndef DBUG_OFF
    if (m_debug_family_demand[index]) return true;
#endif
    for (const Phase1_pipeline_slot &slot : m_slots) {
      if (slot.state != Phase1_pipeline_slot_state::FREE &&
          family_index(slot.descriptor.family) == index) {
        return true;
      }
    }
    return false;
  }

  bool credit_empty_and_balanced_locked() const {
    if (m_family_credit_in_use_bytes[0] >
        std::numeric_limits<uint64_t>::max() -
            m_family_credit_in_use_bytes[1])
      return false;
    const uint64_t family_total = m_family_credit_in_use_bytes[0] +
                                  m_family_credit_in_use_bytes[1];
    return family_total == m_credit_in_use_bytes &&
           m_credit_in_use_bytes == 0 &&
           m_family_credit_in_use_bytes[0] == 0 &&
           m_family_credit_in_use_bytes[1] == 0;
  }

  bool can_reserve_credit_locked(Preserve_trx_phase1_pipeline_family family,
                                 bool final_generation,
                                 uint64_t additional_bytes) const {
    if (additional_bytes == 0 || additional_bytes > m_pipeline_credit_bytes ||
        m_credit_in_use_bytes >
            m_pipeline_credit_bytes - additional_bytes) {
      return false;
    }
    const size_t index = family_index(family);
    if (final_generation) {
      const uint64_t cap = m_config.tail_record_credit_bytes;
      return additional_bytes <= cap &&
             m_tail_credit_consumed_bytes[index] <= cap - additional_bytes;
    }

    const uint64_t reserve[2] = {m_pipeline_record_reserve_bytes,
                                 m_pipeline_binlog_reserve_bytes};
    const size_t other = 1 - index;
    const uint64_t shared =
        m_pipeline_credit_bytes - reserve[0] - reserve[1];
    uint64_t family_limit = reserve[index] + shared;
    if (!family_has_demand_locked(other)) family_limit += reserve[other];
    return additional_bytes <= family_limit &&
           m_family_credit_in_use_bytes[index] <=
               family_limit - additional_bytes;
  }

  void reserve_credit_locked(Phase1_pipeline_slot *slot,
                             uint64_t additional_bytes) {
    const size_t index = family_index(slot->descriptor.family);
    slot->credit_bytes += additional_bytes;
    m_credit_in_use_bytes += additional_bytes;
    m_family_credit_in_use_bytes[index] += additional_bytes;
    if (slot->descriptor.final_generation)
      m_tail_credit_consumed_bytes[index] += additional_bytes;
  }

  void release_credit_locked(Phase1_pipeline_slot *slot,
                             bool rollback_tail_credit) {
    if (slot == nullptr || slot->credit_bytes == 0) return;
    const size_t index = family_index(slot->descriptor.family);
    const uint64_t bytes = slot->credit_bytes;
    if (m_credit_in_use_bytes < bytes ||
        m_family_credit_in_use_bytes[index] < bytes ||
        (rollback_tail_credit && slot->descriptor.final_generation &&
         m_tail_credit_consumed_bytes[index] < bytes)) {
      fail_invariant_locked();
    } else {
      m_credit_in_use_bytes -= bytes;
      m_family_credit_in_use_bytes[index] -= bytes;
      if (rollback_tail_credit && slot->descriptor.final_generation)
        m_tail_credit_consumed_bytes[index] -= bytes;
    }
    slot->credit_bytes = 0;
  }

  bool commit_tail_credit_locked(Phase1_pipeline_slot *slot) {
    if (slot == nullptr || !slot->descriptor.final_generation ||
        (slot->result.status !=
             Preserve_trx_phase1_pipeline_result_status::PREPARED &&
         slot->result.status !=
             Preserve_trx_phase1_pipeline_result_status::ABSENT)) {
      fail_invariant_locked();
      return false;
    }
    const size_t index = family_index(slot->descriptor.family);
    const uint64_t retained_bytes =
        slot->result.status ==
                Preserve_trx_phase1_pipeline_result_status::ABSENT
            ? 0
            : slot->result.logical_bytes;
    if (retained_bytes > slot->credit_bytes ||
        m_tail_credit_consumed_bytes[index] < slot->credit_bytes) {
      fail_invariant_locked();
      return false;
    }
    m_tail_credit_consumed_bytes[index] -=
        slot->credit_bytes - retained_bytes;
    return true;
  }

  void free_slot_locked(Phase1_pipeline_slot *slot, bool rollback_tail_credit) {
    if (slot == nullptr) return;
    if (slot->executor_active || slot->ordinary_active_slot ||
        slot->active_native_operations != 0 ||
        slot->active_no_wait_operations != 0) {
      fail_invariant_locked();
      return;
    }
    release_credit_locked(slot, rollback_tail_credit);
    reset_unpublished_slot_locked(slot);
  }

  Preserve_trx_phase1_prepared_result make_result_locked(
      uint64_t token, const Phase1_pipeline_slot &slot,
      Preserve_trx_phase1_pipeline_result_status status,
      const char *reason) const {
    Preserve_trx_phase1_prepared_result result;
    result.admission_id = token;
    result.attempt_id = slot.descriptor.attempt_id;
    result.drain_generation = slot.descriptor.drain_generation;
    result.target_thread_id = slot.descriptor.target_thread_id;
    result.target_incarnation = slot.descriptor.target_incarnation;
    result.family_version = slot.descriptor.family_version;
    result.family = slot.descriptor.family;
    result.status = status;
    result.reason = reason == nullptr ? "" : reason;
    return result;
  }

  void publish_terminal_result_locked(
      uint64_t token, Preserve_trx_phase1_pipeline_result_status status,
      const char *reason) {
    Preserve_trx_phase1_record_adapter_outcome outcome;
    outcome.status = status;
    outcome.reason = reason == nullptr ? "" : reason;
    publish_adapter_result_locked(token, &outcome);
  }

  void publish_adapter_result_locked(
      uint64_t token, Preserve_trx_phase1_record_adapter_outcome *outcome) {
    Phase1_pipeline_slot *slot = slot_for_token_locked(token);
    if (outcome == nullptr || slot == nullptr ||
        slot->state != Phase1_pipeline_slot_state::ADMITTED ||
        slot->executor_active || slot->active_native_operations != 0 ||
        slot->active_no_wait_operations != 0) {
      fail_invariant_locked();
      return;
    }
    if (outcome->required_credit_bytes > slot->credit_bytes) {
      outcome->status =
          Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
      outcome->reason = "adapter_result_exceeded_reserved_credit";
      outcome->capture_payload.reset();
      outcome->prepared_payload.reset();
      outcome->logical_bytes = 0;
    }
    slot->result = make_result_locked(token, *slot, outcome->status,
                                      outcome->reason.c_str());
    slot->result.logical_bytes = outcome->logical_bytes;
    if (outcome->status ==
            Preserve_trx_phase1_pipeline_result_status::PREPARED ||
        outcome->status ==
            Preserve_trx_phase1_pipeline_result_status::ABSENT) {
      slot->result.record_payload = std::move(outcome->prepared_payload);
    }
    slot->state = Phase1_pipeline_slot_state::RESULT_READY;
    if (!m_result_queue.push(token)) {
      fail_invariant_locked();
      free_slot_locked(slot, slot->descriptor.final_generation);
    }
  }

  void publish_binlog_adapter_result_locked(
      uint64_t token, Preserve_trx_phase1_binlog_adapter_outcome *outcome) {
    Phase1_pipeline_slot *slot = slot_for_token_locked(token);
    if (outcome == nullptr || slot == nullptr ||
        slot->state != Phase1_pipeline_slot_state::ADMITTED ||
        slot->executor_active || slot->active_native_operations != 0 ||
        slot->active_no_wait_operations != 0) {
      fail_invariant_locked();
      return;
    }
    if (outcome->required_credit_bytes > slot->credit_bytes) {
      outcome->status =
          Preserve_trx_phase1_pipeline_result_status::RESOURCE_EXHAUSTED;
      outcome->reason = "adapter_result_exceeded_reserved_credit";
      outcome->logical_bytes = 0;
    }
    slot->result = make_result_locked(token, *slot, outcome->status,
                                      outcome->reason.c_str());
    slot->result.logical_bytes = outcome->logical_bytes;
    if (outcome->status ==
            Preserve_trx_phase1_pipeline_result_status::PREPARED ||
        outcome->status ==
            Preserve_trx_phase1_pipeline_result_status::ABSENT) {
      slot->result.binlog_payload = std::move(outcome->prepared_payload);
    }
    slot->state = Phase1_pipeline_slot_state::RESULT_READY;
    if (!m_result_queue.push(token)) {
      fail_invariant_locked();
      outcome->prepared_payload = std::move(slot->result.binlog_payload);
      free_slot_locked(slot, slot->descriptor.final_generation);
    }
  }

  void publish_operation_result_locked(
      uint64_t token, Phase1_pipeline_operation_status operation_status,
      const char *reason) {
    const Preserve_trx_phase1_pipeline_result_status status =
        operation_status == Phase1_pipeline_operation_status::DEADLINE
            ? Preserve_trx_phase1_pipeline_result_status::DEADLINE
            : Preserve_trx_phase1_pipeline_result_status::CANCELLED;
    publish_terminal_result_locked(token, status, reason);
  }

  void mark_queued_cancelled_locked(uint64_t token, const char *reason) {
    Phase1_pipeline_slot *slot = slot_for_token_locked(token);
    if (slot == nullptr || slot->executor_active ||
        (slot->state != Phase1_pipeline_slot_state::QUEUED &&
         slot->state != Phase1_pipeline_slot_state::ADMITTED)) {
      fail_invariant_locked();
      return;
    }
    slot->record_capture_payload.reset();
    release_credit_locked(slot, slot->descriptor.final_generation);
    slot->result = make_result_locked(
        token, *slot, Preserve_trx_phase1_pipeline_result_status::CANCELLED,
        reason);
    slot->state = Phase1_pipeline_slot_state::RESULT_READY;
    if (!m_result_queue.push(token)) {
      fail_invariant_locked();
      free_slot_locked(slot, slot->descriptor.final_generation);
    }
  }

  void cancel_queue_locked(Phase1_pipeline_token_ring *queue,
                           const char *reason) {
    uint64_t token = 0;
    while (queue != nullptr && queue->pop(&token))
      mark_queued_cancelled_locked(token, reason);
  }

  void cancel_ordinary_queues_locked(const char *reason) {
    const bool sweep_was_active = m_cancel_queue_sweep_active;
    m_cancel_queue_sweep_active = true;
    cancel_queue_locked(&m_record_sequence_queue, reason);
    cancel_queue_locked(&m_record_ready_queue, reason);
    cancel_queue_locked(&m_binlog_ready_queue, reason);
    m_cancel_queue_sweep_active = sweep_was_active;
  }

  void enter_canceling_locked() {
    if (m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::STOPPED) return;

    bool changed = false;
    if (m_lifecycle != Preserve_trx_phase1_pipeline_lifecycle::CANCELING) {
      m_lifecycle = Preserve_trx_phase1_pipeline_lifecycle::CANCELING;
      m_ordinary_admission_open = false;
      m_wait_permit_admission_open = false;
      m_final_admission = Phase1_pipeline_final_admission::CLOSED;
      ++m_cancel_revision;
      changed = true;
    }

    if (!m_cancel_queue_sweep_active) {
      m_cancel_queue_sweep_active = true;
      const uint64_t queued_before = queued_count_locked();
      cancel_queue_locked(&m_record_sequence_queue, "pipeline_cancelled");
      cancel_queue_locked(&m_record_ready_queue, "pipeline_cancelled");
      cancel_queue_locked(&m_binlog_ready_queue, "pipeline_cancelled");
      cancel_queue_locked(&m_final_record_sequence_queue,
                          "pipeline_cancelled");
      cancel_queue_locked(&m_final_ready_queue, "pipeline_cancelled");
      m_cancel_queue_sweep_active = false;
      changed = changed || queued_before != queued_count_locked();
    }

    if (changed) advance_revision_locked();
  }

  bool work_still_valid_locked(const Phase1_pipeline_slot &slot) const {
    if (slot.cancel_revision != m_cancel_revision) return false;
    return slot.descriptor.final_generation
               ? m_lifecycle ==
                     Preserve_trx_phase1_pipeline_lifecycle::FINALIZING
               : m_lifecycle ==
                     Preserve_trx_phase1_pipeline_lifecycle::RUNNING;
  }

  void finish_executor_locked(Phase1_pipeline_slot *slot) {
    if (slot == nullptr || !slot->executor_active || m_active_jobs == 0) {
      fail_invariant_locked();
      return;
    }
    if (slot->ordinary_active_slot) {
      if (m_active_ordinary_jobs == 0) {
        fail_invariant_locked();
        return;
      }
      slot->ordinary_active_slot = false;
      --m_active_ordinary_jobs;
    }
    slot->executor_active = false;
    --m_active_jobs;
  }

  bool work_available_locked() const {
    if (m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::FINALIZING)
      return !m_final_ready_queue.empty();
    return m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::RUNNING &&
           m_ordinary_admission_open &&
           (!m_record_ready_queue.empty() || !m_binlog_ready_queue.empty());
  }

  bool dispatchable_work_available_locked() const {
    if (m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::FINALIZING)
      return !m_final_ready_queue.empty();
    return work_available_locked() &&
           m_active_ordinary_jobs < m_ordinary_active_limit;
  }

  bool take_work_locked(uint64_t *token, uint64_t *revision,
                        Preserve_trx_phase1_pipeline_family *family,
                        bool *final_generation) {
    if (token == nullptr || revision == nullptr || family == nullptr ||
        final_generation == nullptr)
      return false;
    if (m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::FINALIZING) {
      if (!m_final_ready_queue.pop(token)) return false;
    } else if (m_lifecycle ==
                   Preserve_trx_phase1_pipeline_lifecycle::RUNNING &&
               m_ordinary_admission_open) {
      if (m_active_ordinary_jobs >= m_ordinary_active_limit) {
        ++m_ordinary_active_limit_deferrals;
        return false;
      }
      if (!m_record_ready_queue.empty() &&
          (m_binlog_ready_queue.empty() || m_record_dispatch_streak < 2)) {
        if (!m_record_ready_queue.pop(token)) return false;
        ++m_record_dispatch_streak;
      } else if (m_binlog_ready_queue.pop(token)) {
        m_record_dispatch_streak = 0;
      } else {
        return false;
      }
    } else {
      return false;
    }

    Phase1_pipeline_slot *slot = slot_for_token_locked(*token);
    if (slot == nullptr) {
      fail_invariant_locked();
      return false;
    }
    if (slot->state == Phase1_pipeline_slot_state::QUEUED) {
      slot->state = Phase1_pipeline_slot_state::ADMITTED;
      slot->cancel_revision = m_cancel_revision;
    }
    if (slot->state != Phase1_pipeline_slot_state::ADMITTED ||
        slot->executor_active || !work_still_valid_locked(*slot)) {
      fail_invariant_locked();
      return false;
    }
    slot->executor_active = true;
    ++m_active_jobs;
    if (!slot->descriptor.final_generation) {
      slot->ordinary_active_slot = true;
      ++m_active_ordinary_jobs;
      m_ordinary_active_high_water =
          std::max(m_ordinary_active_high_water, m_active_ordinary_jobs);
    }
    *revision = slot->cancel_revision;
    *family = slot->descriptor.family;
    *final_generation = slot->descriptor.final_generation;
    return true;
  }

  uint64_t ordinary_outstanding_locked() const {
    uint64_t count = 0;
    for (const Phase1_pipeline_slot &slot : m_slots) {
      if (slot.state != Phase1_pipeline_slot_state::FREE &&
          !slot.descriptor.final_generation) {
        ++count;
      }
    }
    return count;
  }

  uint64_t outstanding_slots_locked() const {
    return static_cast<uint64_t>(std::count_if(
        m_slots.begin(), m_slots.end(), [](const Phase1_pipeline_slot &slot) {
          return slot.state != Phase1_pipeline_slot_state::FREE;
        }));
  }

  uint64_t inflight_slots_locked() const {
    return static_cast<uint64_t>(std::count_if(
        m_slots.begin(), m_slots.end(), [](const Phase1_pipeline_slot &slot) {
          return slot.state == Phase1_pipeline_slot_state::ADMITTED;
        }));
  }

  uint64_t result_slots_locked() const {
    return static_cast<uint64_t>(std::count_if(
        m_slots.begin(), m_slots.end(), [](const Phase1_pipeline_slot &slot) {
          return slot.state == Phase1_pipeline_slot_state::RESULT_READY ||
                 slot.state == Phase1_pipeline_slot_state::OWNER_HELD ||
                 slot.state ==
                     Phase1_pipeline_slot_state::PUBLICATION_PENDING;
        }));
  }

  bool all_work_queues_empty_locked() const {
    return m_record_sequence_queue.empty() && m_record_ready_queue.empty() &&
           m_binlog_ready_queue.empty() &&
           m_final_record_sequence_queue.empty() && m_final_ready_queue.empty();
  }

  uint64_t queued_count_locked() const {
    return m_record_sequence_queue.size() + m_record_ready_queue.size() +
           m_binlog_ready_queue.size() +
           m_final_record_sequence_queue.size() + m_final_ready_queue.size();
  }

  void drain_abort_results_locked(
      std::vector<Preserve_trx_phase1_binlog_prepared_handle> *cleanup) {
    uint64_t token = 0;
    while (m_result_queue.pop(&token)) {
      Phase1_pipeline_slot *slot = slot_for_token_locked(token);
      if (slot == nullptr ||
          slot->state != Phase1_pipeline_slot_state::RESULT_READY) {
        fail_invariant_locked();
        continue;
      }
      if (slot->result.binlog_payload != nullptr && cleanup != nullptr) {
        cleanup->push_back(std::move(slot->result.binlog_payload));
      }
      free_slot_locked(slot, false);
    }
  }

#ifndef DBUG_OFF
  bool wait_for_result_count(size_t count, uint64_t timeout_us) {
    std::unique_lock<std::mutex> guard(m_mutex);
    return m_condition.wait_for(
        guard, std::chrono::microseconds(timeout_us), [&] {
          return m_result_queue.size() >= count ||
                 m_lifecycle ==
                     Preserve_trx_phase1_pipeline_lifecycle::CANCELING ||
                 m_lifecycle == Preserve_trx_phase1_pipeline_lifecycle::STOPPED;
        });
  }

  bool drop_debug_results(size_t count, uint64_t timeout_us) {
    for (size_t index = 0; index < count; ++index) {
      if (!wait_for_result_count(1, timeout_us)) return false;
      Preserve_trx_phase1_prepared_result result;
      if (!try_pop_result(&result) ||
          !settle_result(
              result.admission_id,
              Preserve_trx_phase1_pipeline_result_disposition::DROP)) {
        return false;
      }
    }
    return true;
  }

  Preserve_trx_phase1_work_descriptor debug_descriptor(
      uint64_t target_thread_id, Preserve_trx_phase1_pipeline_family family,
      uint64_t credit_bytes, bool final_generation) const {
    Preserve_trx_phase1_work_descriptor descriptor;
    descriptor.attempt_id = m_config.attempt_id;
    descriptor.drain_generation = m_config.drain_generation;
    descriptor.target_thread_id = target_thread_id;
    descriptor.target_incarnation = target_thread_id + 1000;
    descriptor.family_version = 1;
    descriptor.estimated_credit_bytes = credit_bytes;
    descriptor.family = family;
    descriptor.final_generation = final_generation;
    return descriptor;
  }
#endif

  void fail_invariant_locked() {
    ++m_invariant_failures;
    enter_canceling_locked();
  }

  void advance_revision_locked() { ++m_event_revision; }

  void join_threads() {
    std::vector<std::thread> threads;
    {
      std::lock_guard<std::mutex> guard(m_join_mutex);
      threads.swap(m_threads);
    }
    for (std::thread &thread : threads) {
      if (thread.joinable()) thread.join();
    }
  }

  void log_event(const char *event) const {
    const Preserve_trx_phase1_pipeline_snapshot current = snapshot();
    std::ostringstream message;
    message << "PRESERVE_PHASE1_PIPELINE_V1 event=" << event
            << " mode=BOUNDED_PIPELINE_V1"
            << " attempt_id=" << m_config.attempt_id
            << " generation=" << m_config.drain_generation
            << " lifecycle="
            << phase1_pipeline_lifecycle_name(current.lifecycle)
            << " workers_configured=" << current.workers_configured
            << " workers_ready=" << current.workers_ready
            << " sequencer_ready=" << (current.sequencer_ready ? 1 : 0)
            << " init_failures=" << current.init_failures
            << " admitted=" << current.admitted
            << " inflight=" << current.inflight
            << " queued=" << current.queued
            << " result_count=" << current.result_count
            << " credit_in_use_bytes=" << current.credit_in_use_bytes
            << " record_credit_in_use_bytes="
            << current.record_credit_in_use_bytes
            << " binlog_credit_in_use_bytes="
            << current.binlog_credit_in_use_bytes
            << " tail_record_credit_consumed_bytes="
            << current.tail_record_credit_consumed_bytes
            << " cancel_revision=" << current.cancel_revision
            << " active_operation_permits="
            << current.active_operation_permits
            << " no_wait_active_operations="
            << current.no_wait_active_operations
            << " operation_permits_started="
            << current.operation_permits_started
            << " operation_permit_budget_rejected="
            << current.operation_permit_budget_rejected
            << " operation_budget_overruns="
            << current.operation_budget_overruns
            << " ordinary_binlog_slow_operations="
            << current.ordinary_binlog_slow_operations
            << " final_record_capture_operation_samples="
            << current.final_record_capture_operation_samples
            << " final_record_capture_operation_us_total="
            << current.final_record_capture_operation_us_total
            << " final_record_capture_operation_us_max="
            << current.final_record_capture_operation_us_max
            << " final_record_capture_operation_overruns="
            << current.final_record_capture_operation_overruns
            << " final_record_store_snapshot_operation_samples="
            << current.final_record_store_snapshot_operation_samples
            << " final_record_store_snapshot_operation_us_total="
            << current.final_record_store_snapshot_operation_us_total
            << " final_record_store_snapshot_operation_us_max="
            << current.final_record_store_snapshot_operation_us_max
            << " final_record_store_snapshot_operation_overruns="
            << current.final_record_store_snapshot_operation_overruns
            << " final_record_prepare_operation_samples="
            << current.final_record_prepare_operation_samples
            << " final_record_prepare_operation_us_total="
            << current.final_record_prepare_operation_us_total
            << " final_record_prepare_operation_us_max="
            << current.final_record_prepare_operation_us_max
            << " final_record_prepare_operation_overruns="
            << current.final_record_prepare_operation_overruns
            << " record_capture_latch_samples="
            << current.record_capture_latch_samples
            << " record_capture_latch_wait_us="
            << current.record_capture_latch_wait_us
            << " record_capture_latch_hold_us="
            << current.record_capture_latch_hold_us
            << " record_capture_latch_envelope_us="
            << current.record_capture_latch_envelope_us
            << " record_capture_latch_max_envelope_us="
            << current.record_capture_latch_max_envelope_us
            << " effective_pipeline_credit_bytes="
            << current.effective_pipeline_credit_bytes
            << " effective_record_reserve_bytes="
            << current.effective_record_reserve_bytes
            << " effective_binlog_reserve_bytes="
            << current.effective_binlog_reserve_bytes
            << " publication_failures=" << current.publication_failures
            << " publication_ack_uncertain="
            << current.publication_ack_uncertain
            << " publication_aborted=" << current.publication_aborted
            << " submit_no_slot=" << current.submit_no_slot
            << " submit_no_credit=" << current.submit_no_credit
            << " submit_deadline=" << current.submit_deadline
            << " invariant_failures=" << current.invariant_failures
            << " ordinary_active_limit_requested="
            << current.ordinary_active_limit_requested
            << " ordinary_active_limit_effective="
            << current.ordinary_active_limit_effective
            << " ordinary_active=" << current.ordinary_active
            << " ordinary_active_high_water="
            << current.ordinary_active_high_water
            << " ordinary_active_limit_deferrals="
            << current.ordinary_active_limit_deferrals
            << " event_revision=" << current.event_revision;
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.str().c_str());
  }

  const Preserve_trx_phase1_pipeline_config m_config;
  Preserve_trx_phase1_binlog_provider_port *m_binlog_provider{nullptr};
  mutable std::mutex m_mutex;
  std::condition_variable m_condition;
  std::mutex m_join_mutex;
  Preserve_trx_phase1_pipeline_lifecycle m_lifecycle{
      Preserve_trx_phase1_pipeline_lifecycle::STARTING};
  bool m_ordinary_admission_open{true};
  bool m_wait_permit_admission_open{true};
  Phase1_pipeline_final_admission m_final_admission{
      Phase1_pipeline_final_admission::NOT_OPENED};
  bool m_sequencer_ready{false};
#ifndef DBUG_OFF
  bool m_debug_hold_executors{false};
  bool m_debug_prepare_results{false};
  bool m_debug_hold_first_ordinary{false};
  bool m_debug_first_ordinary_held{false};
  bool m_debug_cap_deferral_observed{false};
  bool m_debug_family_demand[2]{false, false};
#endif
  bool m_cancel_queue_sweep_active{false};
  uint32_t m_init_reports{0};
  uint32_t m_workers_ready{0};
  uint32_t m_init_failures{0};
  uint32_t m_record_dispatch_streak{0};
  size_t m_free_slot_cursor{0};
  uint64_t m_event_revision{0};
  uint64_t m_cancel_revision{1};
  uint64_t m_stage_deadline_us{0};
  uint64_t m_operation_cutoff_us{0};
  bool m_phase2_deadline_published{false};
  uint64_t m_pipeline_credit_bytes{0};
  uint64_t m_pipeline_record_reserve_bytes{0};
  uint64_t m_pipeline_binlog_reserve_bytes{0};
  uint64_t m_credit_in_use_bytes{0};
  uint64_t m_family_credit_in_use_bytes[2]{0, 0};
  uint64_t m_tail_credit_consumed_bytes[2]{0, 0};
  uint64_t m_active_jobs{0};
  uint32_t m_ordinary_active_limit{0};
  uint64_t m_active_ordinary_jobs{0};
  uint64_t m_ordinary_active_high_water{0};
  uint64_t m_ordinary_active_limit_deferrals{0};
  uint64_t m_active_operation_permits{0};
  uint64_t m_no_wait_active_operations{0};
  uint64_t m_operation_permits_started{0};
  uint64_t m_operation_permit_budget_rejected{0};
  uint64_t m_operation_budget_overruns{0};
  uint64_t m_ordinary_binlog_slow_operations{0};
#ifndef DBUG_OFF
  std::atomic<uint32_t> m_debug_prepared_binlog_count{0};
#endif
  uint64_t m_final_record_capture_operation_samples{0};
  uint64_t m_final_record_capture_operation_us_total{0};
  uint64_t m_final_record_capture_operation_us_max{0};
  uint64_t m_final_record_capture_operation_overruns{0};
  uint64_t m_final_record_store_snapshot_operation_samples{0};
  uint64_t m_final_record_store_snapshot_operation_us_total{0};
  uint64_t m_final_record_store_snapshot_operation_us_max{0};
  uint64_t m_final_record_store_snapshot_operation_overruns{0};
  uint64_t m_final_record_prepare_operation_samples{0};
  uint64_t m_final_record_prepare_operation_us_total{0};
  uint64_t m_final_record_prepare_operation_us_max{0};
  uint64_t m_final_record_prepare_operation_overruns{0};
  uint64_t m_record_capture_latch_samples{0};
  uint64_t m_record_capture_latch_wait_us{0};
  uint64_t m_record_capture_latch_hold_us{0};
  uint64_t m_record_capture_latch_envelope_us{0};
  uint64_t m_record_capture_latch_max_envelope_us{0};
  uint64_t m_publication_failures{0};
  uint64_t m_publication_ack_uncertain{0};
  uint64_t m_publication_aborted{0};
  uint64_t m_submit_no_slot{0};
  uint64_t m_submit_no_credit{0};
  uint64_t m_submit_deadline{0};
  uint64_t m_invariant_failures{0};
  std::vector<Phase1_pipeline_slot> m_slots;
  Phase1_pipeline_token_ring m_record_sequence_queue;
  Phase1_pipeline_token_ring m_record_ready_queue;
  Phase1_pipeline_token_ring m_binlog_ready_queue;
  Phase1_pipeline_token_ring m_final_record_sequence_queue;
  Phase1_pipeline_token_ring m_final_ready_queue;
  Phase1_pipeline_token_ring m_result_queue;
  std::vector<std::thread> m_threads;
};

Preserve_trx_phase1_pipeline::Preserve_trx_phase1_pipeline(
    const Preserve_trx_phase1_pipeline_config &config,
    Preserve_trx_phase1_binlog_provider_port *binlog_provider)
    : m_impl(new Impl(config, binlog_provider)) {}

Preserve_trx_phase1_pipeline::~Preserve_trx_phase1_pipeline() = default;

bool Preserve_trx_phase1_pipeline::start() { return m_impl->start(); }

Preserve_trx_phase1_pipeline_submit_status
Preserve_trx_phase1_pipeline::try_submit(
    const Preserve_trx_phase1_work_descriptor &descriptor) {
  return m_impl->try_submit(descriptor);
}

Preserve_trx_phase1_pipeline_submit_status
Preserve_trx_phase1_pipeline::try_submit_final(
    const Preserve_trx_phase1_work_descriptor &descriptor) {
  return m_impl->try_submit_final(descriptor);
}

bool Preserve_trx_phase1_pipeline::try_pop_result(
    Preserve_trx_phase1_prepared_result *result) {
  return m_impl->try_pop_result(result);
}

bool Preserve_trx_phase1_pipeline::settle_result(
    uint64_t admission_id,
    Preserve_trx_phase1_pipeline_result_disposition disposition) {
  return m_impl->settle_result(admission_id, disposition);
}

bool Preserve_trx_phase1_pipeline::begin_publication(uint64_t admission_id) {
  return m_impl->begin_publication(admission_id);
}

bool Preserve_trx_phase1_pipeline::settle_publication(
    uint64_t admission_id,
    Preserve_trx_phase1_pipeline_publication_status status) {
  return m_impl->settle_publication(admission_id, status);
}

uint64_t
Preserve_trx_phase1_pipeline::abort_residual_publications_after_sender_join() {
  return m_impl->abort_residual_publications_after_sender_join();
}

bool Preserve_trx_phase1_pipeline::publish_stage_deadline(
    uint64_t stage_started_us, uint64_t stage_deadline_us) {
  return m_impl->publish_stage_deadline(stage_started_us, stage_deadline_us);
}

bool Preserve_trx_phase1_pipeline::begin_finalizing() {
  return m_impl->begin_finalizing();
}

bool Preserve_trx_phase1_pipeline::open_final_admission(
    uint64_t final_deadline_us) {
  return m_impl->open_final_admission(final_deadline_us);
}

void Preserve_trx_phase1_pipeline::close_final_admission() {
  m_impl->close_final_admission();
}

bool Preserve_trx_phase1_pipeline::finish_and_join() {
  return m_impl->finish_and_join();
}

void Preserve_trx_phase1_pipeline::cancel() { m_impl->cancel(); }

bool Preserve_trx_phase1_pipeline::wait_for_change(
    uint64_t observed_revision, uint64_t timeout_us, uint64_t *new_revision) {
  return m_impl->wait_for_change(observed_revision, timeout_us, new_revision);
}

bool Preserve_trx_phase1_pipeline::join_while_draining() {
  return m_impl->join_while_draining();
}

Preserve_trx_phase1_pipeline_snapshot
Preserve_trx_phase1_pipeline::snapshot() const {
  return m_impl->snapshot();
}

#ifndef DBUG_OFF
bool Preserve_trx_phase1_pipeline::debug_exercise_core() {
  return m_impl->debug_exercise_core();
}

bool Preserve_trx_phase1_pipeline::debug_exercise_record_adapter(
    THD *owner_thd, THD *target_thd, bool expect_empty) {
  return m_impl->debug_exercise_record_adapter(owner_thd, target_thd,
                                                expect_empty);
}
#endif
