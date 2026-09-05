/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation. */

#include "sql/preserve_trx_phase1_owner.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <utility>
#include <vector>

#include "my_loglevel.h"
#include "mysql/components/services/log_builtins.h"
#include "sql/debug_sync.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_lock_warmcopy.h"
#include "sql/preserve_trx_phase1_pipeline.h"
#include "sql/preserve_trx_phase1_record_adapter.h"
#include "sql/sql_class.h"
#include "storage/innobase/include/lock0warmcopy.h"
#include "storage/innobase/include/trx0preserve.h"

namespace {

uint64_t monotonic_us() {
  using clock = std::chrono::steady_clock;
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          clock::now().time_since_epoch())
          .count());
}

constexpr uint64_t k_record_retry_base_us = 1000;

uint64_t saturating_add(uint64_t left, uint64_t right) {
  return right > UINT64_MAX - left ? UINT64_MAX : left + right;
}

struct Preserve_trx_phase1_owner_snapshot {
  uint64_t membership_rounds{0};
  uint64_t targets{0};
  uint64_t record_submitted{0};
  uint64_t record_results{0};
  uint64_t record_published{0};
  uint64_t record_absent{0};
  uint64_t record_deferred_to_final{0};
  uint64_t record_retries{0};
  uint64_t retry_delay_total_us{0};
  uint64_t retry_delay_max_us{0};
  uint64_t record_rebinds{0};
  uint64_t record_retired{0};
  uint64_t live_transaction_gaps{0};
  uint64_t binding_retries{0};
  uint64_t stale_results{0};
  uint64_t inflight{0};
  uint64_t final_targets{0};
  uint64_t final_capture_targets{0};
  uint64_t final_store_refresh_targets{0};
  uint64_t final_store_refreshed{0};
  uint64_t final_store_fallback_invalid{0};
  uint64_t final_store_fallback_coordinate{0};
  uint64_t final_store_fallback_unavailable{0};
  uint64_t final_deferred_targets{0};
  bool failed{false};
  std::string failure_reason;
};

struct Membership {
  uint64_t thread_id{0};
  uint64_t owner_cookie{0};
};

class Membership_collector final : public Do_THD_Impl {
 public:
  explicit Membership_collector(THD *owner) : m_owner(owner) {}

  void operator()(THD *candidate) override {
    if (candidate == nullptr || candidate == m_owner) return;
    m_memberships.push_back(
        {static_cast<uint64_t>(candidate->thread_id()),
         static_cast<uint64_t>(reinterpret_cast<uintptr_t>(candidate))});
  }

  std::vector<Membership> take() { return std::move(m_memberships); }

 private:
  THD *m_owner;
  std::vector<Membership> m_memberships;
};

struct Record_binding {
  uint64_t thread_id{0};
  uint64_t owner_cookie{0};
  uint64_t raw_trx_cookie{0};
  uint64_t immutable_trx_id{0};
  uint64_t trx_version{0};
  uint64_t warmcopy_epoch{0};
  lock_warmcopy_trx_lock_fence_t live_fence;
  bool active_scan{false};
  bool live_fence_valid{false};
};

enum class Record_binding_status : uint8_t {
  BOUND,
  LIVE_NO_TRANSACTION,
  RETRYABLE,
  TERMINAL_GONE
};

bool same_binding(const Record_binding &left, const Record_binding &right) {
  return left.thread_id == right.thread_id &&
         left.owner_cookie == right.owner_cookie &&
         left.raw_trx_cookie == right.raw_trx_cookie &&
         left.immutable_trx_id == right.immutable_trx_id &&
         left.trx_version == right.trx_version &&
         left.warmcopy_epoch == right.warmcopy_epoch;
}

Record_binding_status resolve_record_binding(
    const Membership &membership, Record_binding *binding,
    uint64_t final_drain_generation = 0) {
  if (binding == nullptr || membership.thread_id == 0 ||
      membership.thread_id > std::numeric_limits<my_thread_id>::max() ||
      membership.owner_cookie == 0) {
    return Record_binding_status::TERMINAL_GONE;
  }
  *binding = {};
  Find_thd_with_id finder(static_cast<my_thread_id>(membership.thread_id));
  THD *target = Global_THD_manager::get_instance()->find_thd(&finder);
  if (target == nullptr) return Record_binding_status::TERMINAL_GONE;

  const bool exact_owner =
      static_cast<uint64_t>(reinterpret_cast<uintptr_t>(target)) ==
      membership.owner_cookie;
  const bool batch_state_eligible =
      final_drain_generation == 0
          ? target->preserve_trx_batch_state ==
                Preserve_trx_batch_thd_state::NONE
          : target->preserve_trx_batch_generation == final_drain_generation &&
                (target->preserve_trx_batch_state ==
                     Preserve_trx_batch_thd_state::QUIESCED ||
                 target->preserve_trx_batch_state ==
                     Preserve_trx_batch_thd_state::PENDING_QUIESCE);
  const bool live_member =
      exact_owner && !target->release_resources_done() &&
      !target->is_system_thread() && target->killed == THD::NOT_KILLED &&
      batch_state_eligible;
  const bool unsupported =
      live_member && preserve_trx_phase1_target_unsupported_locked(target);
  const bool has_transaction =
      live_member && !unsupported &&
      preserve_trx_phase1_record_candidate_eligible_locked(target);
  if (!has_transaction) {
    mysql_mutex_unlock(&target->LOCK_thd_data);
    return live_member && !unsupported
               ? Record_binding_status::LIVE_NO_TRANSACTION
               : Record_binding_status::TERMINAL_GONE;
  }
  const bool active_scan = !target->m_server_idle;
  Preserve_trx_external_thd_pin_handle pin;
  pin = preserve_trx_acquire_external_thd_pin_locked(target);
  mysql_mutex_unlock(&target->LOCK_thd_data);
  if (!pin || pin.thd() != target) return Record_binding_status::RETRYABLE;

  trx_preserve_phase1_identity identity;
  if (!trx_preserve_phase1_owner_identity_snapshot(pin.thd(), &identity) ||
      identity.owner_thd_cookie != membership.owner_cookie ||
      identity.raw_cookie == 0 || identity.immutable_trx_id == 0 ||
      identity.trx_version == 0) {
    return Record_binding_status::LIVE_NO_TRANSACTION;
  }
  lock_warmcopy_trx_lock_fence_t live_fence;
  if (final_drain_generation != 0) {
    trx_preserve_phase1_identity identity_after_fence;
    if (!trx_preserve_sample_lock_warmcopy_fence(pin.thd(), &live_fence) ||
        !trx_preserve_phase1_owner_identity_snapshot(pin.thd(),
                                                     &identity_after_fence) ||
        identity_after_fence.raw_cookie != identity.raw_cookie ||
        identity_after_fence.owner_thd_cookie != identity.owner_thd_cookie ||
        identity_after_fence.immutable_trx_id != identity.immutable_trx_id ||
        identity_after_fence.trx_version != identity.trx_version) {
      return Record_binding_status::RETRYABLE;
    }
  }
  lock_warmcopy_record_store_head_t store;
  if (!lock_warmcopy_record_store_head_for_target(
          membership.thread_id, &store) ||
      store.epoch == 0 || store.target_id != membership.thread_id) {
    return Record_binding_status::RETRYABLE;
  }
  binding->thread_id = membership.thread_id;
  binding->owner_cookie = membership.owner_cookie;
  binding->raw_trx_cookie = identity.raw_cookie;
  binding->immutable_trx_id = identity.immutable_trx_id;
  binding->trx_version = identity.trx_version;
  binding->warmcopy_epoch = store.epoch;
  binding->live_fence = live_fence;
  binding->active_scan = active_scan;
  binding->live_fence_valid = final_drain_generation != 0;
  return Record_binding_status::BOUND;
}

}  // namespace

class Preserve_trx_phase1_owner::Impl {
 public:
  Impl(const Preserve_trx_phase1_owner_config &config,
       Preserve_trx_phase1_pipeline *pipeline,
       Preserve_trx_lock_warmcopy_drain_participant *record_participant)
      : m_config(config),
        m_pipeline(pipeline),
        m_record_participant(record_participant) {
    if (m_config.attempt_id == 0 || m_config.drain_generation == 0 ||
        m_config.record_item_limit == 0 ||
        m_config.record_capture_bytes == 0 ||
        m_config.record_initial_credit_bytes == 0 ||
        m_config.record_final_job_credit_bytes == 0 ||
        m_config.record_capture_bytes >
            m_config.record_initial_credit_bytes ||
        m_pipeline == nullptr || m_record_participant == nullptr) {
      fail("owner_config_invalid");
    }
  }

  bool reconcile_record_targets(THD *drain_owner) {
    if (m_failed || drain_owner == nullptr ||
        Global_THD_manager::get_instance() == nullptr) {
      return false;
    }
    Membership_collector collector(drain_owner);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&collector);
    return reconcile_memberships(collector.take());
  }

#ifndef DBUG_OFF
  Preserve_trx_phase1_owner_pump_status pump_record(uint32_t result_budget,
                                                     uint32_t submit_budget) {
    if (m_failed) return Preserve_trx_phase1_owner_pump_status::FAILED;
    if (m_config.external_result_demux && result_budget != 0) {
      fail("owner_external_result_demux_required");
      return Preserve_trx_phase1_owner_pump_status::FAILED;
    }
    bool progressed = false;
    for (uint32_t consumed = 0; consumed < result_budget; ++consumed) {
      Preserve_trx_phase1_prepared_result result;
      if (!m_pipeline->try_pop_result(&result)) break;
      progressed = true;
      if (!consume_record_result(std::move(result))) {
        return Preserve_trx_phase1_owner_pump_status::FAILED;
      }
    }
    const Preserve_trx_phase1_owner_pump_status submit_status =
        submit_record(submit_budget);
    if (submit_status == Preserve_trx_phase1_owner_pump_status::FAILED ||
        submit_status == Preserve_trx_phase1_owner_pump_status::COMPLETE) {
      return submit_status;
    }
    return progressed ||
                   submit_status == Preserve_trx_phase1_owner_pump_status::PROGRESS
               ? Preserve_trx_phase1_owner_pump_status::PROGRESS
               : Preserve_trx_phase1_owner_pump_status::IDLE;
  }
#endif

  Preserve_trx_phase1_owner_pump_status submit_record(uint32_t budget) {
    return submit_record_impl(budget, false);
  }

  Preserve_trx_phase1_owner_pump_status submit_final_record(uint32_t budget) {
    return submit_record_impl(budget, true);
  }

  Preserve_trx_phase1_owner_pump_status submit_record_impl(
      uint32_t budget, bool final_generation) {
    if (m_failed) return Preserve_trx_phase1_owner_pump_status::FAILED;
    bool progressed = false;
    for (uint32_t submitted = 0; submitted < budget; ++submitted) {
      Entry *entry = next_ready_entry(final_generation);
      if (entry == nullptr) break;
      const Preserve_trx_phase1_pipeline_submit_status status =
          final_generation ? m_pipeline->try_submit_final(entry->descriptor)
                           : m_pipeline->try_submit(entry->descriptor);
      if (status == Preserve_trx_phase1_pipeline_submit_status::ADMITTED) {
        entry->state = Entry_state::INFLIGHT;
        ++m_record_submitted;
        progressed = true;
        continue;
      }
      if (status == Preserve_trx_phase1_pipeline_submit_status::NO_SLOT ||
          status == Preserve_trx_phase1_pipeline_submit_status::NO_CREDIT) {
        break;
      }
      fail(status == Preserve_trx_phase1_pipeline_submit_status::DEADLINE
               ? "owner_submit_deadline"
               : status ==
                         Preserve_trx_phase1_pipeline_submit_status::SINGLE_FLIGHT
                     ? "owner_single_flight_invariant"
                     : "owner_submit_rejected");
      return Preserve_trx_phase1_owner_pump_status::FAILED;
    }
    if (final_generation ? final_record_baselines_complete()
                         : record_baselines_complete()) {
      return Preserve_trx_phase1_owner_pump_status::COMPLETE;
    }
    return progressed ? Preserve_trx_phase1_owner_pump_status::PROGRESS
                      : Preserve_trx_phase1_owner_pump_status::IDLE;
  }

#ifndef DBUG_OFF
  bool wait_for_record_baselines(uint64_t deadline_us) {
    if (deadline_us == 0 || m_config.external_result_demux) return false;
    for (;;) {
      const Preserve_trx_phase1_owner_pump_status status =
          pump_record(16, 16);
      if (status == Preserve_trx_phase1_owner_pump_status::COMPLETE) return true;
      if (status == Preserve_trx_phase1_owner_pump_status::FAILED) return false;
      const uint64_t now_us = monotonic_us();
      if (now_us >= deadline_us) {
        fail("owner_wait_deadline");
        return false;
      }
      if (status == Preserve_trx_phase1_owner_pump_status::IDLE) {
        const Preserve_trx_phase1_pipeline_snapshot pipeline_snapshot =
            m_pipeline->snapshot();
        uint64_t ignored_revision = 0;
        (void)m_pipeline->wait_for_change(
            pipeline_snapshot.event_revision,
            std::min<uint64_t>(deadline_us - now_us, 1000ULL),
            &ignored_revision);
      }
    }
  }
#endif

  bool record_baselines_complete() const {
    if (m_failed) return false;
    for (const auto &item : m_entries) {
      if (item.second.descriptor.final_generation) continue;
      if (item.second.state != Entry_state::PUBLISHED &&
          item.second.state != Entry_state::ABSENT &&
          item.second.state != Entry_state::REBIND_WAIT &&
          item.second.state != Entry_state::DEFERRED_TO_FINAL) {
        return false;
      }
    }
    return true;
  }

  bool prepare_final_record_targets(
      THD *drain_owner, const std::vector<uint64_t> &final_thread_ids) {
    if (m_failed || drain_owner == nullptr ||
        Global_THD_manager::get_instance() == nullptr ||
        m_config.record_final_job_credit_bytes == 0) {
      return false;
    }
    const std::set<uint64_t> unique_ids(final_thread_ids.begin(),
                                        final_thread_ids.end());
    if (unique_ids.size() != final_thread_ids.size()) {
      fail("owner_final_target_duplicate");
      return false;
    }
    Membership_collector collector(drain_owner);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&collector);
    std::map<uint64_t, Membership> memberships;
    for (const Membership &membership : collector.take()) {
      memberships.emplace(membership.thread_id, membership);
    }

    m_final_target_ids = unique_ids;
    m_final_deferred_ids.clear();
    m_final_capture_targets = 0;
    m_final_store_refresh_targets = 0;
    m_final_store_refreshed = 0;
    m_final_store_fallback_invalid = 0;
    m_final_store_fallback_coordinate = 0;
    m_final_store_fallback_unavailable = 0;
    std::vector<Record_binding> captures;
    captures.reserve(final_thread_ids.size());
    struct Store_refresh {
      Record_binding binding;
      uint64_t baseline_generation{0};
      uint64_t coordinate_generation{0};
    };
    std::vector<Store_refresh> store_refreshes;
    store_refreshes.reserve(final_thread_ids.size());
    for (const uint64_t thread_id : final_thread_ids) {
      const auto membership = memberships.find(thread_id);
      Record_binding binding;
      if (membership == memberships.end() ||
          resolve_record_binding(membership->second, &binding,
                                 m_config.drain_generation) !=
              Record_binding_status::BOUND) {
        m_final_deferred_ids.insert(thread_id);
        continue;
      }
      const auto entry = m_entries.find(thread_id);
      Preserve_trx_phase1_final_record_candidate candidate;
      candidate.thread_id = thread_id;
      if (entry != m_entries.end() &&
          same_binding(entry->second.binding, binding) &&
          (entry->second.state == Entry_state::PUBLISHED ||
           entry->second.state == Entry_state::ABSENT) &&
          entry->second.publication_token != 0 && binding.live_fence_valid) {
        candidate.target_incarnation =
            entry->second.descriptor.target_incarnation;
        candidate.capture_generation =
            entry->second.descriptor.capture_generation;
        candidate.publication_token = entry->second.publication_token;
        candidate.live_fence = binding.live_fence;
        candidate.current = true;
        candidate.absent = entry->second.state == Entry_state::ABSENT;
        candidate.live_fence_valid = true;
      }
      if (m_record_participant->bounded_phase1_record_candidate_current(
              candidate)) {
        continue;
      }
      bool refresh_from_store = false;
      uint64_t baseline_generation = 0;
      uint64_t coordinate_generation = 0;
      if (entry != m_entries.end() &&
          same_binding(entry->second.binding, binding) &&
          (entry->second.state == Entry_state::PUBLISHED ||
           entry->second.state == Entry_state::ABSENT) &&
          entry->second.publication_token != 0 &&
          entry->second.published_live_fence_valid &&
          binding.live_fence_valid) {
        if (binding.live_fence.coordinate_generation !=
                entry->second.published_live_fence.coordinate_generation ||
            binding.live_fence.conversion_attempt_after_freeze ||
            binding.live_fence.conversion_unhandled_after_freeze) {
          ++m_final_store_fallback_coordinate;
        } else {
          lock_warmcopy_record_store_head_t head;
          if (!lock_warmcopy_record_store_head_for_target(thread_id, &head) ||
              head.epoch != binding.warmcopy_epoch || !head.store_present ||
              head.baseline_generation != entry->second.publication_token) {
            ++m_final_store_fallback_unavailable;
          } else if (head.target_invalid) {
            ++m_final_store_fallback_invalid;
          } else {
            refresh_from_store = true;
            baseline_generation = entry->second.publication_token;
            coordinate_generation =
                entry->second.published_live_fence.coordinate_generation;
          }
        }
      }
      if (refresh_from_store) {
        store_refreshes.push_back(
            {binding, baseline_generation, coordinate_generation});
        continue;
      }
      captures.push_back(binding);
    }

    m_final_capture_targets = captures.size();
    m_final_store_refresh_targets = store_refreshes.size();
    for (const Store_refresh &refresh : store_refreshes) {
      auto found = m_entries.find(refresh.binding.thread_id);
      if (!install_binding(
              refresh.binding, found, true,
              m_config.record_final_job_credit_bytes, true,
              refresh.baseline_generation, refresh.coordinate_generation)) {
        return false;
      }
    }
    for (const Record_binding &binding : captures) {
      auto found = m_entries.find(binding.thread_id);
      if (!install_binding(binding, found, true,
                           m_config.record_final_job_credit_bytes)) {
        return false;
      }
    }
    return true;
  }

  bool final_record_baselines_complete() const {
    if (m_failed) return false;
    for (const uint64_t thread_id : m_final_target_ids) {
      if (m_final_deferred_ids.count(thread_id) != 0) continue;
      const auto entry = m_entries.find(thread_id);
      if (entry == m_entries.end() ||
          (entry->second.state != Entry_state::PUBLISHED &&
           entry->second.state != Entry_state::ABSENT) ||
          entry->second.publication_token == 0) {
        return false;
      }
    }
    return true;
  }

  bool reconcile_final_record_targets(
      THD *drain_owner, const std::vector<uint64_t> &final_thread_ids) {
    if (m_failed || drain_owner == nullptr ||
        Global_THD_manager::get_instance() == nullptr) {
      return false;
    }
    const std::set<uint64_t> unique_ids(final_thread_ids.begin(),
                                        final_thread_ids.end());
    if (unique_ids.size() != final_thread_ids.size()) {
      fail("owner_final_target_duplicate");
      return false;
    }
    if (m_final_target_ids != unique_ids) {
      fail("owner_final_target_set_changed");
      return false;
    }

    Membership_collector collector(drain_owner);
    Global_THD_manager::get_instance()->do_for_all_thd_copy(&collector);
    std::map<uint64_t, Membership> memberships;
    for (const Membership &membership : collector.take()) {
      memberships.emplace(membership.thread_id, membership);
    }

    std::vector<Preserve_trx_phase1_final_record_candidate> candidates;
    candidates.reserve(final_thread_ids.size());
    for (const uint64_t thread_id : final_thread_ids) {
      Preserve_trx_phase1_final_record_candidate candidate;
      candidate.thread_id = thread_id;
      const auto membership = memberships.find(thread_id);
      Record_binding final_binding;
      const bool binding_resolved =
          membership != memberships.end() &&
          resolve_record_binding(membership->second, &final_binding,
                                 m_config.drain_generation) ==
              Record_binding_status::BOUND;
      const auto entry = m_entries.find(thread_id);
      if (binding_resolved && entry != m_entries.end() &&
          same_binding(entry->second.binding, final_binding) &&
          (entry->second.state == Entry_state::PUBLISHED ||
           entry->second.state == Entry_state::ABSENT) &&
          entry->second.publication_token != 0) {
        candidate.target_incarnation =
            entry->second.descriptor.target_incarnation;
        candidate.capture_generation =
            entry->second.descriptor.capture_generation;
        candidate.publication_token = entry->second.publication_token;
        candidate.live_fence = final_binding.live_fence;
        candidate.current = true;
        candidate.absent = entry->second.state == Entry_state::ABSENT;
        candidate.live_fence_valid = final_binding.live_fence_valid;
      }
      if (!candidate.current) m_final_deferred_ids.insert(thread_id);
      candidates.push_back(candidate);
    }

    uint64_t retained = 0;
    uint64_t invalidated = 0;
    if (!m_record_participant->reconcile_bounded_final_record_candidates(
            candidates, &retained, &invalidated)) {
      fail("owner_final_participant_reconcile_failed");
      return false;
    }
    std::ostringstream message;
    message << "PRESERVE_PHASE1_OWNER_V1"
            << " event=FINAL_RECORD_RECONCILE_COMPLETE"
            << " targets=" << final_thread_ids.size()
            << " captured=" << m_final_capture_targets
            << " store_refresh_targets=" << m_final_store_refresh_targets
            << " store_refreshed=" << m_final_store_refreshed
            << " store_fallback_invalid=" << m_final_store_fallback_invalid
            << " store_fallback_coordinate="
            << m_final_store_fallback_coordinate
            << " store_fallback_unavailable="
            << m_final_store_fallback_unavailable
            << " deferred=" << m_final_deferred_ids.size()
            << " exact=" << retained
            << " invalidated=" << invalidated
            << " failed=0";
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.str().c_str());
    return true;
  }

  Preserve_trx_phase1_owner_snapshot snapshot() const {
    Preserve_trx_phase1_owner_snapshot result;
    result.membership_rounds = m_membership_round;
    result.targets = m_entries.size();
    result.record_submitted = m_record_submitted;
    result.record_results = m_record_results;
    result.record_published = m_record_published;
    result.record_absent = m_record_absent;
    result.record_deferred_to_final = m_record_deferred_to_final;
    result.record_retries = m_record_retries;
    result.retry_delay_total_us = m_retry_delay_total_us;
    result.retry_delay_max_us = m_retry_delay_max_us;
    result.record_rebinds = m_record_rebinds;
    result.record_retired = m_record_retired;
    result.live_transaction_gaps = m_live_transaction_gaps;
    result.binding_retries = m_binding_retries;
    result.stale_results = m_stale_results;
    result.final_targets = m_final_target_ids.size();
    result.final_capture_targets = m_final_capture_targets;
    result.final_store_refresh_targets = m_final_store_refresh_targets;
    result.final_store_refreshed = m_final_store_refreshed;
    result.final_store_fallback_invalid = m_final_store_fallback_invalid;
    result.final_store_fallback_coordinate = m_final_store_fallback_coordinate;
    result.final_store_fallback_unavailable =
        m_final_store_fallback_unavailable;
    result.final_deferred_targets = m_final_deferred_ids.size();
    for (const auto &item : m_entries) {
      if (item.second.state == Entry_state::INFLIGHT) ++result.inflight;
    }
    result.failed = m_failed;
    result.failure_reason = m_failure_reason;
    return result;
  }

  void log_event(const char *event) const {
    const Preserve_trx_phase1_owner_snapshot current = snapshot();
    std::ostringstream retry_reasons;
    bool first_retry_reason = true;
    for (const auto &item : m_retry_reasons) {
      if (!first_retry_reason) retry_reasons << ',';
      retry_reasons << item.first << ':' << item.second;
      first_retry_reason = false;
    }
    std::ostringstream message;
    message << "PRESERVE_PHASE1_OWNER_V1"
            << " event=" << (event == nullptr ? "UNKNOWN" : event)
            << " targets=" << current.targets
            << " submitted=" << current.record_submitted
            << " results=" << current.record_results
            << " published=" << current.record_published
            << " absent=" << current.record_absent
            << " deferred_to_final=" << current.record_deferred_to_final
            << " retries=" << current.record_retries
            << " retry_delay_total_us=" << current.retry_delay_total_us
            << " retry_delay_max_us=" << current.retry_delay_max_us
            << " max_capture_generation=" << m_max_capture_generation
            << " retry_reasons="
            << (first_retry_reason ? "NONE" : retry_reasons.str())
            << " rebinds=" << current.record_rebinds
            << " retired=" << current.record_retired
            << " live_gaps=" << current.live_transaction_gaps
            << " binding_retries=" << current.binding_retries
            << " stale_results=" << current.stale_results
            << " inflight=" << current.inflight
            << " final_targets=" << current.final_targets
            << " final_capture_targets=" << current.final_capture_targets
            << " final_store_refresh_targets="
            << current.final_store_refresh_targets
            << " final_store_refreshed=" << current.final_store_refreshed
            << " final_store_fallback_invalid="
            << current.final_store_fallback_invalid
            << " final_store_fallback_coordinate="
            << current.final_store_fallback_coordinate
            << " final_store_fallback_unavailable="
            << current.final_store_fallback_unavailable
            << " final_deferred_targets=" << current.final_deferred_targets
            << " legacy_record_scan_calls="
            << m_record_participant->phase1_legacy_record_scan_calls()
            << " legacy_store_rebuild_calls="
            << m_record_participant->phase1_legacy_store_rebuild_calls()
            << " failed=" << (current.failed ? 1 : 0)
            << " reason="
            << (current.failure_reason.empty() ? "NONE"
                                               : current.failure_reason);
    LogErr(current.failed ? ERROR_LEVEL : INFORMATION_LEVEL,
           ER_LOG_PRINTF_MSG, message.str().c_str());
  }

#ifndef DBUG_OFF
  bool debug_exercise_record_target(THD *target) {
    if (m_failed || target == nullptr) return false;
    bool expect_rebind = false;
    bool expect_gap_rebind = false;
    bool expect_retry = false;
    DBUG_EXECUTE_IF("preserve_trx_phase1_owner_expect_rebind",
                    { expect_rebind = true; });
    DBUG_EXECUTE_IF("preserve_trx_phase1_owner_expect_gap_rebind",
                    { expect_gap_rebind = true; });
    DBUG_EXECUTE_IF("preserve_trx_phase1_owner_inject_one_retry",
                    { expect_retry = true; });
    const Membership membership{
        static_cast<uint64_t>(target->thread_id()),
        static_cast<uint64_t>(reinterpret_cast<uintptr_t>(target))};
    if (!reconcile_memberships({membership})) return false;
    if (!wait_for_record_baselines(monotonic_us() + 5000000ULL)) {
      return false;
    }
    DEBUG_SYNC(current_thd,
               "preserve_trx_phase1_owner_after_record_reconcile");
    Preserve_trx_phase1_owner_snapshot current = snapshot();
    if (expect_rebind) {
      if (!reconcile_memberships({membership}) ||
          !wait_for_record_baselines(monotonic_us() + 5000000ULL)) {
        return false;
      }
      current = snapshot();
    }
    if (expect_gap_rebind) {
      if (!reconcile_memberships({membership})) return false;
      DEBUG_SYNC(current_thd, "preserve_trx_phase1_owner_after_live_gap");
      if (!reconcile_memberships({membership}) ||
          !wait_for_record_baselines(monotonic_us() + 5000000ULL)) {
        return false;
      }
      current = snapshot();
    }
    const uint64_t expected_published =
        expect_rebind || expect_gap_rebind ? 2 : 1;
    if (current.targets != 1 ||
        current.record_published != expected_published ||
        current.inflight != 0 || current.failed ||
        (expect_retry &&
         (current.record_retries != 1 ||
          current.retry_delay_total_us != k_record_retry_base_us ||
          current.retry_delay_max_us != k_record_retry_base_us)) ||
        current.record_rebinds !=
            (expect_rebind || expect_gap_rebind ? 1 : 0) ||
        (expect_gap_rebind && current.live_transaction_gaps == 0)) {
      return false;
    }
    std::ostringstream message;
    message << "PRESERVE_PHASE1_OWNER_V1"
            << " event="
            << (expect_retry
                    ? "OWNER_RETRY_EXERCISE_PASSED"
                : expect_gap_rebind
                    ? "OWNER_GAP_REBIND_EXERCISE_PASSED"
                    : expect_rebind ? "OWNER_REBIND_EXERCISE_PASSED"
                                    : "OWNER_PUMP_EXERCISE_PASSED")
            << " targets=" << current.targets
            << " published=" << current.record_published
            << " rebinds=" << current.record_rebinds
            << " retired=" << current.record_retired
            << " live_gaps=" << current.live_transaction_gaps
            << " retries=" << current.record_retries
            << " retry_delay_total_us=" << current.retry_delay_total_us
            << " retry_delay_max_us=" << current.retry_delay_max_us
            << " inflight=" << current.inflight;
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.str().c_str());
    return true;
  }
#endif

  bool consume_result(Preserve_trx_phase1_prepared_result result) {
    return consume_record_result(std::move(result));
  }

 private:
  enum class Entry_state : uint8_t {
    READY,
    INFLIGHT,
    RETRY_WAIT,
    PUBLISHED,
    ABSENT,
    REBIND_WAIT,
    DEFERRED_TO_FINAL
  };

  struct Entry {
    Record_binding binding;
    Preserve_trx_phase1_work_descriptor descriptor;
    Entry_state state{Entry_state::READY};
    uint64_t seen_round{0};
    uint64_t next_retry_us{0};
    uint64_t retry_streak{0};
    bool replacement_pending{false};
    bool retire_after_inflight{false};
    uint64_t publication_token{0};
    lock_warmcopy_trx_lock_fence_t published_live_fence;
    bool published_live_fence_valid{false};
  };

  bool reconcile_memberships(const std::vector<Membership> &memberships) {
    if (m_failed || m_membership_round == UINT64_MAX) {
      fail("owner_membership_round_overflow");
      return false;
    }
    ++m_membership_round;
    for (const Membership &membership : memberships) {
      Record_binding binding;
      const Record_binding_status binding_status =
          resolve_record_binding(membership, &binding);
      auto found = m_entries.find(membership.thread_id);
      if (found != m_entries.end() &&
          found->second.state == Entry_state::DEFERRED_TO_FINAL) {
        found->second.seen_round = m_membership_round;
        found->second.replacement_pending = false;
        found->second.retire_after_inflight = false;
        continue;
      }
      if (binding_status != Record_binding_status::BOUND) {
        if (found != m_entries.end() &&
            (binding_status == Record_binding_status::LIVE_NO_TRANSACTION ||
             binding_status == Record_binding_status::RETRYABLE)) {
          retain_unbound(found, binding_status);
        }
        continue;
      }
      if (m_warmcopy_epoch == 0) m_warmcopy_epoch = binding.warmcopy_epoch;
      if (binding.warmcopy_epoch != m_warmcopy_epoch) {
        fail("owner_warmcopy_epoch_changed");
        return false;
      }
      if (found != m_entries.end() &&
          same_binding(found->second.binding, binding)) {
        found->second.binding.active_scan = binding.active_scan;
        found->second.seen_round = m_membership_round;
        found->second.replacement_pending = false;
        found->second.retire_after_inflight = false;
        if (found->second.state == Entry_state::REBIND_WAIT) {
          schedule_retry(&found->second, "record_binding_became_active");
        }
        continue;
      }
      if (found != m_entries.end() &&
          found->second.state == Entry_state::INFLIGHT) {
        found->second.seen_round = m_membership_round;
        found->second.replacement_pending = true;
        found->second.retire_after_inflight = false;
        continue;
      }
      if (!install_binding(binding, found)) return false;
    }
    for (auto it = m_entries.begin(); it != m_entries.end();) {
      if (it->second.seen_round == m_membership_round ||
          it->second.state == Entry_state::INFLIGHT) {
        if (it->second.seen_round != m_membership_round) {
          it->second.replacement_pending = false;
          it->second.retire_after_inflight = true;
        }
        ++it;
      } else {
        ++m_record_retired;
        it = m_entries.erase(it);
      }
    }
    return true;
  }

  bool install_binding(const Record_binding &binding,
                       std::map<uint64_t, Entry>::iterator found,
                       bool final_generation = false,
                       uint64_t final_credit_bytes = 0,
                       bool use_record_store_snapshot = false,
                       uint64_t expected_store_baseline_generation = 0,
                       uint64_t expected_lock_coordinate_generation = 0) {
    if (m_next_incarnation == 0 || m_next_incarnation == UINT64_MAX) {
      fail("owner_incarnation_overflow");
      return false;
    }
    const uint64_t credit_bytes =
        final_generation ? final_credit_bytes
                         : m_config.record_initial_credit_bytes;
    if (credit_bytes == 0 ||
        (use_record_store_snapshot &&
         (!final_generation || expected_store_baseline_generation == 0))) {
      fail("owner_record_credit_invalid");
      return false;
    }
    Entry entry;
    entry.binding = binding;
    entry.seen_round = m_membership_round;
    entry.descriptor.attempt_id = m_config.attempt_id;
    entry.descriptor.drain_generation = m_config.drain_generation;
    entry.descriptor.target_thread_id = binding.thread_id;
    entry.descriptor.target_incarnation = m_next_incarnation++;
    entry.descriptor.family_version = 1;
    entry.descriptor.source_owner_cookie = binding.owner_cookie;
    entry.descriptor.source_object_cookie = binding.raw_trx_cookie;
    entry.descriptor.warmcopy_epoch = binding.warmcopy_epoch;
    entry.descriptor.expected_immutable_trx_id = binding.immutable_trx_id;
    entry.descriptor.expected_trx_version = binding.trx_version;
    entry.descriptor.capture_generation = 1;
    entry.descriptor.item_limit = m_config.record_item_limit;
    entry.descriptor.capture_byte_limit =
        final_generation ? credit_bytes : m_config.record_capture_bytes;
    entry.descriptor.estimated_credit_bytes = credit_bytes;
    entry.descriptor.expected_store_baseline_generation =
        expected_store_baseline_generation;
    entry.descriptor.expected_lock_coordinate_generation =
        expected_lock_coordinate_generation;
    entry.descriptor.family =
        Preserve_trx_phase1_pipeline_family::RECORD_LOCK;
    entry.descriptor.final_generation = final_generation;
    entry.descriptor.use_record_store_snapshot = use_record_store_snapshot;
    if (found == m_entries.end()) {
      m_entries.emplace(binding.thread_id, std::move(entry));
    } else {
      const bool binding_changed =
          !same_binding(found->second.binding, binding);
      found->second = std::move(entry);
      if (binding_changed) ++m_record_rebinds;
    }
    return true;
  }

  void retain_unbound(std::map<uint64_t, Entry>::iterator found,
                      Record_binding_status status) {
    found->second.seen_round = m_membership_round;
    found->second.replacement_pending = false;
    found->second.retire_after_inflight = false;
    if (found->second.state != Entry_state::INFLIGHT) {
      found->second.state = Entry_state::REBIND_WAIT;
      found->second.next_retry_us = 0;
      found->second.publication_token = 0;
    }
    if (status == Record_binding_status::LIVE_NO_TRANSACTION) {
      ++m_live_transaction_gaps;
    } else {
      ++m_binding_retries;
    }
  }

  Entry *next_ready_entry(bool final_generation) {
    const uint64_t now_us = monotonic_us();
    for (auto &item : m_entries) {
      Entry &entry = item.second;
      if (entry.descriptor.final_generation != final_generation) continue;
      if ((entry.state == Entry_state::READY ||
           entry.state == Entry_state::RETRY_WAIT) &&
          entry.next_retry_us <= now_us) {
        return &entry;
      }
    }
    return nullptr;
  }

  bool consume_record_result(Preserve_trx_phase1_prepared_result result) {
    if (m_failed ||
        result.family != Preserve_trx_phase1_pipeline_family::RECORD_LOCK ||
        result.attempt_id != m_config.attempt_id ||
        result.drain_generation != m_config.drain_generation ||
        result.binlog_payload != nullptr) {
      fail("owner_record_result_identity_invalid");
      return false;
    }
    ++m_record_results;
    auto found = m_entries.find(result.target_thread_id);
    const bool current =
        found != m_entries.end() &&
        found->second.descriptor.target_incarnation ==
            result.target_incarnation &&
        found->second.descriptor.family_version == result.family_version &&
        found->second.state == Entry_state::INFLIGHT;
    if (!current) {
      ++m_stale_results;
      result.record_payload.reset();
      if (!m_pipeline->settle_result(
              result.admission_id,
              Preserve_trx_phase1_pipeline_result_disposition::DROP)) {
        fail("owner_stale_result_settle_failed");
        return false;
      }
      return true;
    }

    Entry &entry = found->second;
    DBUG_EXECUTE_IF("preserve_trx_phase1_owner_inject_one_retry", {
      if (!entry.descriptor.final_generation &&
          !m_debug_retry_injected &&
          (result.status ==
               Preserve_trx_phase1_pipeline_result_status::PREPARED ||
           result.status ==
               Preserve_trx_phase1_pipeline_result_status::ABSENT)) {
        result.status = Preserve_trx_phase1_pipeline_result_status::RETRYABLE;
        result.reason = "record_retry_injected";
        m_debug_retry_injected = true;
      }
    });
    Preserve_trx_phase1_pipeline_result_disposition disposition =
        Preserve_trx_phase1_pipeline_result_disposition::DROP;
    bool retry_same_binding = false;
    bool refresh_binding = false;
    bool fallback_to_native = false;
    std::string retry_reason;
    if (result.status == Preserve_trx_phase1_pipeline_result_status::PREPARED ||
        result.status == Preserve_trx_phase1_pipeline_result_status::ABSENT) {
      Preserve_trx_phase1_record_adapter_install_result installed;
      preserve_trx_phase1_record_adapter_owner_publish(
          entry.descriptor, result.record_payload, m_record_participant,
          entry.binding.active_scan, &installed);
      if (installed.status ==
              Preserve_trx_phase1_pipeline_result_status::PREPARED ||
          installed.status ==
              Preserve_trx_phase1_pipeline_result_status::ABSENT) {
        entry.state = installed.status ==
                              Preserve_trx_phase1_pipeline_result_status::ABSENT
                          ? Entry_state::ABSENT
                          : Entry_state::PUBLISHED;
        if (entry.state == Entry_state::ABSENT) {
          ++m_record_absent;
          disposition =
              Preserve_trx_phase1_pipeline_result_disposition::ABSENT;
        } else {
          ++m_record_published;
        }
        entry.publication_token = installed.publication_token;
        entry.published_live_fence = installed.captured_live_fence;
        entry.published_live_fence_valid = true;
        if (entry.descriptor.final_generation &&
            entry.descriptor.use_record_store_snapshot) {
          ++m_final_store_refreshed;
        }
        if (entry.descriptor.final_generation) {
          disposition =
              Preserve_trx_phase1_pipeline_result_disposition::RETAINED;
        }
      } else if (installed.status ==
                     Preserve_trx_phase1_pipeline_result_status::
                         STORE_FALLBACK) {
        if (!entry.descriptor.final_generation ||
            !entry.descriptor.use_record_store_snapshot) {
          fail("owner_record_install_store_fallback_invalid");
        } else {
          fallback_to_native = true;
          retry_reason = installed.reason;
        }
      } else if (installed.status ==
                     Preserve_trx_phase1_pipeline_result_status::RETRYABLE ||
                 installed.status == Preserve_trx_phase1_pipeline_result_status::
                                         IDENTITY_STALE) {
        refresh_binding =
            installed.status ==
            Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE;
        retry_same_binding = !refresh_binding;
        retry_reason = installed.reason;
      } else {
        fail(installed.reason.empty() ? "owner_record_publish_failed"
                                      : installed.reason);
      }
    } else if (result.status ==
               Preserve_trx_phase1_pipeline_result_status::STORE_FALLBACK) {
      if (!entry.descriptor.final_generation ||
          !entry.descriptor.use_record_store_snapshot) {
        fail("owner_record_store_fallback_invalid");
      } else {
        fallback_to_native = true;
        retry_reason = result.reason;
      }
    } else if (result.status ==
               Preserve_trx_phase1_pipeline_result_status::DEFERRED_TO_FINAL) {
      entry.state = Entry_state::DEFERRED_TO_FINAL;
      ++m_record_deferred_to_final;
      if (entry.descriptor.final_generation) {
        m_final_deferred_ids.insert(result.target_thread_id);
      }
    } else if (result.status ==
                   Preserve_trx_phase1_pipeline_result_status::RETRYABLE ||
               result.status == Preserve_trx_phase1_pipeline_result_status::
                                    IDENTITY_STALE) {
      refresh_binding =
          result.status ==
          Preserve_trx_phase1_pipeline_result_status::IDENTITY_STALE;
      retry_same_binding = !refresh_binding;
      retry_reason = result.reason;
    } else {
      fail(result.reason.empty() ? "owner_record_result_terminal"
                                 : result.reason);
    }

    result.record_payload.reset();
    if (!m_pipeline->settle_result(result.admission_id, disposition)) {
      fail("owner_result_settle_failed");
      return false;
    }
    found = m_entries.find(result.target_thread_id);
    if (found == m_entries.end()) return true;
    if (found->second.replacement_pending) {
      assert(!found->second.descriptor.final_generation);
      if (found->second.descriptor.final_generation) {
        fail("owner_final_replacement_invariant");
        return false;
      }
      defer_to_final(&found->second);
      return true;
    }
    if (found->second.retire_after_inflight) {
      ++m_record_retired;
      m_entries.erase(found);
      return true;
    }
    if (fallback_to_native) {
      Entry &fallback = found->second;
      if (fallback.descriptor.capture_generation == UINT64_MAX) {
        fail("owner_capture_generation_overflow");
        return false;
      }
      ++fallback.descriptor.capture_generation;
      m_max_capture_generation =
          std::max(m_max_capture_generation,
                   fallback.descriptor.capture_generation);
      fallback.descriptor.use_record_store_snapshot = false;
      fallback.descriptor.expected_store_baseline_generation = 0;
      fallback.descriptor.expected_lock_coordinate_generation = 0;
      fallback.state = Entry_state::READY;
      fallback.next_retry_us = 0;
      fallback.retry_streak = 0;
      ++m_final_capture_targets;
      ++m_final_store_fallback_unavailable;
      ++m_retry_reasons[retry_reason.empty()
                            ? "record_store_snapshot_fallback"
                            : retry_reason];
      return !m_failed;
    }
    if (refresh_binding) {
      /* The current result is settled; binding refresh owns the next state. */
      found->second.state = Entry_state::RETRY_WAIT;
      if (found->second.descriptor.final_generation) {
        m_final_deferred_ids.insert(result.target_thread_id);
        m_entries.erase(found);
        return true;
      }
      defer_to_final(&found->second);
      return true;
    }
    if (retry_same_binding) {
      schedule_retry(&found->second,
                     retry_reason.empty() ? "record_retry_unknown"
                                          : retry_reason);
    }
    return !m_failed;
  }

  void schedule_retry(Entry *entry, const std::string &reason) {
    if (entry == nullptr || entry->descriptor.capture_generation == UINT64_MAX) {
      fail("owner_capture_generation_overflow");
      return;
    }
    if (!entry->descriptor.final_generation && entry->retry_streak >= 1) {
      defer_to_final(entry);
      return;
    }
    ++entry->descriptor.capture_generation;
    m_max_capture_generation =
        std::max(m_max_capture_generation,
                 entry->descriptor.capture_generation);
    entry->state = Entry_state::RETRY_WAIT;
    if (entry->retry_streak != UINT64_MAX) ++entry->retry_streak;
    const uint64_t delay_us = k_record_retry_base_us;
    entry->next_retry_us = saturating_add(monotonic_us(), delay_us);
    m_retry_delay_total_us =
        saturating_add(m_retry_delay_total_us, delay_us);
    m_retry_delay_max_us = std::max(m_retry_delay_max_us, delay_us);
    ++m_record_retries;
    ++m_retry_reasons[reason.empty() ? "record_retry_unknown" : reason];
  }

  void defer_to_final(Entry *entry) {
    if (entry == nullptr || entry->descriptor.final_generation ||
        entry->state == Entry_state::DEFERRED_TO_FINAL) {
      return;
    }
    entry->state = Entry_state::DEFERRED_TO_FINAL;
    entry->next_retry_us = 0;
    entry->replacement_pending = false;
    entry->retire_after_inflight = false;
    entry->publication_token = 0;
    ++m_record_deferred_to_final;
  }

  void fail(const std::string &reason) {
    if (m_failed) return;
    m_failed = true;
    m_failure_reason = reason;
  }

  Preserve_trx_phase1_owner_config m_config;
  Preserve_trx_phase1_pipeline *m_pipeline{nullptr};
  Preserve_trx_lock_warmcopy_drain_participant *m_record_participant{nullptr};
  std::map<uint64_t, Entry> m_entries;
  uint64_t m_membership_round{0};
  uint64_t m_next_incarnation{1};
  uint64_t m_warmcopy_epoch{0};
  uint64_t m_record_submitted{0};
  uint64_t m_record_results{0};
  uint64_t m_record_published{0};
  uint64_t m_record_absent{0};
  uint64_t m_record_deferred_to_final{0};
  uint64_t m_record_retries{0};
  uint64_t m_retry_delay_total_us{0};
  uint64_t m_retry_delay_max_us{0};
  uint64_t m_max_capture_generation{1};
  uint64_t m_record_rebinds{0};
  uint64_t m_record_retired{0};
  uint64_t m_live_transaction_gaps{0};
  uint64_t m_binding_retries{0};
  uint64_t m_stale_results{0};
  std::set<uint64_t> m_final_target_ids;
  std::set<uint64_t> m_final_deferred_ids;
  uint64_t m_final_capture_targets{0};
  uint64_t m_final_store_refresh_targets{0};
  uint64_t m_final_store_refreshed{0};
  uint64_t m_final_store_fallback_invalid{0};
  uint64_t m_final_store_fallback_coordinate{0};
  uint64_t m_final_store_fallback_unavailable{0};
  std::map<std::string, uint64_t> m_retry_reasons;
#ifndef DBUG_OFF
  bool m_debug_retry_injected{false};
#endif
  bool m_failed{false};
  std::string m_failure_reason;
};

Preserve_trx_phase1_owner::Preserve_trx_phase1_owner(
    const Preserve_trx_phase1_owner_config &config,
    Preserve_trx_phase1_pipeline *pipeline,
    Preserve_trx_lock_warmcopy_drain_participant *record_participant)
    : m_impl(new Impl(config, pipeline, record_participant)) {}

Preserve_trx_phase1_owner::~Preserve_trx_phase1_owner() = default;

bool Preserve_trx_phase1_owner::reconcile_record_targets(THD *drain_owner) {
  return m_impl->reconcile_record_targets(drain_owner);
}

bool Preserve_trx_phase1_owner::consume_result(
    Preserve_trx_phase1_prepared_result result) {
  return m_impl->consume_result(std::move(result));
}

Preserve_trx_phase1_owner_pump_status
Preserve_trx_phase1_owner::submit_record(uint32_t budget) {
  return m_impl->submit_record(budget);
}

Preserve_trx_phase1_owner_pump_status
Preserve_trx_phase1_owner::submit_final_record(uint32_t budget) {
  return m_impl->submit_final_record(budget);
}

bool Preserve_trx_phase1_owner::record_baselines_complete() const {
  return m_impl->record_baselines_complete();
}

bool Preserve_trx_phase1_owner::prepare_final_record_targets(
    THD *drain_owner, const std::vector<uint64_t> &final_thread_ids) {
  return m_impl->prepare_final_record_targets(drain_owner, final_thread_ids);
}

bool Preserve_trx_phase1_owner::final_record_baselines_complete() const {
  return m_impl->final_record_baselines_complete();
}

bool Preserve_trx_phase1_owner::reconcile_final_record_targets(
    THD *drain_owner, const std::vector<uint64_t> &final_thread_ids) {
  return m_impl->reconcile_final_record_targets(drain_owner, final_thread_ids);
}

void Preserve_trx_phase1_owner::log_event(const char *event) const {
  m_impl->log_event(event);
}

#ifndef DBUG_OFF
bool Preserve_trx_phase1_owner::debug_exercise_record_target(THD *target) {
  return m_impl->debug_exercise_record_target(target);
}
#endif
