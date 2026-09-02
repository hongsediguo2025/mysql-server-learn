/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
   General Public License, version 2.0, for more details. */

#include "sql/preserve_trx_standby_phase2_scheduler.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <limits>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "my_dbug.h"
#include "sha2.h"
#include "sql/current_thd.h"
#include "sql/debug_sync.h"
#include "sql/handler.h"
#include "sql/mdl.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/preserve_trx.h"
#include "sql/sql_class.h"
#include "storage/innobase/include/lock0preserve_plan.h"
#include "storage/innobase/include/trx0preserve.h"

namespace preserve_trx_phase2_scheduler {
namespace {

constexpr uint64_t kScanPeriodUs = 5000;
constexpr uint64_t kProbeStartReserveUs = 500;
constexpr uint64_t kSupportLifetimeUs = 10000;
constexpr uint64_t kMdlOwnerProofPeriodUs = 5000;
constexpr size_t kMdlMaxOwnerTickets = 256;
constexpr size_t kMdlMaxOwnerMatches = 256;
constexpr size_t kMdlMaxDemandCandidates = 256;
constexpr size_t kStableBoundaryHintBatchLimit = 16;

struct Command_key_less {
  bool operator()(const Command_key &left, const Command_key &right) const {
    if (left.connection_incarnation != right.connection_incarnation) {
      return left.connection_incarnation < right.connection_incarnation;
    }
    return left.sequence < right.sequence;
  }
};

struct Transaction_key_less {
  bool operator()(const Transaction_key &left,
                  const Transaction_key &right) const {
    if (left.connection_incarnation != right.connection_incarnation) {
      return left.connection_incarnation < right.connection_incarnation;
    }
    return left.ordinal < right.ordinal;
  }
};

bool same_transaction_key(const Transaction_key &left,
                          const Transaction_key &right) {
  return left.connection_incarnation == right.connection_incarnation &&
         left.ordinal == right.ordinal;
}

enum class Lock_domain : uint8_t { INNODB = 0, MDL };

struct Support_edge_key {
  Command_key waiter;
  Transaction_key blocker;
  Lock_domain domain{Lock_domain::INNODB};
};

struct Support_edge_key_less {
  bool operator()(const Support_edge_key &left,
                  const Support_edge_key &right) const {
    Command_key_less command_less;
    if (command_less(left.waiter, right.waiter)) return true;
    if (command_less(right.waiter, left.waiter)) return false;
    Transaction_key_less transaction_less;
    if (transaction_less(left.blocker, right.blocker)) return true;
    if (transaction_less(right.blocker, left.blocker)) return false;
    return static_cast<uint8_t>(left.domain) <
           static_cast<uint8_t>(right.domain);
  }
};

using Support_expiry_index =
    std::multimap<uint64_t, Support_edge_key>;

struct Support_edge {
  uint64_t expires_at_us{0};
  uint64_t mdl_demand_generation{0};
  Support_expiry_index::iterator expiry;
};

using Support_edge_set =
    std::set<Support_edge_key, Support_edge_key_less>;
using Support_by_waiter =
    std::map<Command_key, Support_edge_set, Command_key_less>;
using Support_by_blocker =
    std::map<Transaction_key, Support_edge_set, Transaction_key_less>;

struct Mdl_demand {
  MDL_key key;
  enum_mdl_type request_type{MDL_TYPE_END};
  uint64_t generation{0};
  uint64_t expires_at_us{0};
};

struct Mdl_demand_index_key {
  MDL_key key;
  Command_key waiter;
};

struct Mdl_demand_index_key_less {
  bool operator()(const Mdl_demand_index_key &left,
                  const Mdl_demand_index_key &right) const {
    const int key_order = left.key.cmp(&right.key);
    if (key_order != 0) return key_order < 0;
    return Command_key_less{}(left.waiter, right.waiter);
  }
};

using Mdl_demand_map =
    std::map<Command_key, Mdl_demand, Command_key_less>;
using Mdl_demand_index =
    std::map<Mdl_demand_index_key, uint64_t, Mdl_demand_index_key_less>;

struct Mdl_owner_probe_match {
  uint64_t demand_generation{0};
  bool non_releasing{false};
};

using Mdl_owner_probe_matches =
    std::map<Command_key, Mdl_owner_probe_match, Command_key_less>;

enum class Mdl_owner_probe_phase : uint8_t { TRANSACTION = 0, EXPLICIT };

struct Mdl_owner_probe_context {
  const Mdl_demand_map *demands{nullptr};
  const Mdl_demand_index *demand_index{nullptr};
  Mdl_owner_probe_matches *matches{nullptr};
  const MDL_context *owner_context{nullptr};
  uint64_t blocker_connection_incarnation{0};
  uint64_t now_us{0};
  size_t ticket_count{0};
  size_t candidate_count{0};
  size_t match_count{0};
  Mdl_owner_probe_phase phase{Mdl_owner_probe_phase::TRANSACTION};
  bool incomplete{false};
};

uint64_t scheduler_monotonic_us() {
  using clock = std::chrono::steady_clock;
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          clock::now().time_since_epoch())
          .count());
}

uint64_t bounded_lease_expiry(uint64_t sampled_at_us,
                              uint64_t absolute_deadline_us) {
  const uint64_t lease_expiry =
      sampled_at_us >
              std::numeric_limits<uint64_t>::max() - kSupportLifetimeUs
          ? std::numeric_limits<uint64_t>::max()
          : sampled_at_us + kSupportLifetimeUs;
  return absolute_deadline_us == 0
             ? lease_expiry
             : std::min(lease_expiry, absolute_deadline_us);
}

bool valid_mdl_type(enum_mdl_type type) {
  const int value = static_cast<int>(type);
  return value >= 0 && value < static_cast<int>(MDL_TYPE_END);
}

bool mdl_owner_ticket_visitor(const MDL_ticket *ticket, void *opaque) {
  auto *context = static_cast<Mdl_owner_probe_context *>(opaque);
  if (context == nullptr || context->demands == nullptr ||
      context->demand_index == nullptr || context->matches == nullptr ||
      context->owner_context == nullptr || ticket == nullptr ||
      ++context->ticket_count > kMdlMaxOwnerTickets ||
      ticket->get_ctx() != context->owner_context ||
      !valid_mdl_type(ticket->get_type())) {
    if (context != nullptr) context->incomplete = true;
    return true;
  }

  const MDL_key *const ticket_key = ticket->get_key();
  if (ticket_key == nullptr) {
    context->incomplete = true;
    return true;
  }

  Mdl_demand_index_key first_key;
  first_key.key.mdl_key_init(ticket_key);
  auto index_entry = context->demand_index->lower_bound(first_key);
  for (; index_entry != context->demand_index->end() &&
         index_entry->first.key.cmp(ticket_key) == 0;
       ++index_entry) {
    if (++context->candidate_count > kMdlMaxDemandCandidates) {
      context->incomplete = true;
      return true;
    }
    const auto demand = context->demands->find(index_entry->first.waiter);
    if (demand == context->demands->end() ||
        demand->second.generation != index_entry->second ||
        !demand->second.key.is_equal(ticket_key) ||
        !valid_mdl_type(demand->second.request_type)) {
      context->incomplete = true;
      return true;
    }
    if (demand->second.expires_at_us <= context->now_us ||
        demand->first.connection_incarnation ==
            context->blocker_connection_incarnation ||
        !ticket->is_incompatible_when_granted(
            demand->second.request_type)) {
      continue;
    }

    if (context->phase == Mdl_owner_probe_phase::TRANSACTION) {
      const auto inserted = context->matches->emplace(
          demand->first,
          Mdl_owner_probe_match{demand->second.generation, false});
      if (!inserted.second &&
          inserted.first->second.demand_generation !=
              demand->second.generation) {
        context->incomplete = true;
        return true;
      }
      if (inserted.second && ++context->match_count > kMdlMaxOwnerMatches) {
        context->incomplete = true;
        return true;
      }
    } else {
      auto match = context->matches->find(demand->first);
      if (match != context->matches->end()) {
        if (match->second.demand_generation != demand->second.generation) {
          context->incomplete = true;
          return true;
        }
        match->second.non_releasing = true;
      }
    }
  }
  return false;
}

std::atomic<uint64_t> g_next_connection_incarnation{1};
std::atomic<bool> g_route_active{false};
std::atomic<bool> g_callback_active{false};
std::mutex g_route_mutex;
Attempt_handle g_active_route;
Attempt_handle g_callback_attempt;

uint64_t next_connection_incarnation() {
  for (;;) {
    const uint64_t value =
        g_next_connection_incarnation.fetch_add(1, std::memory_order_relaxed);
    if (value != 0) return value;
  }
}

Attempt_handle active_route_snapshot() {
  if (!g_route_active.load(std::memory_order_acquire)) return nullptr;
  std::lock_guard<std::mutex> lock(g_route_mutex);
  if (!g_route_active.load(std::memory_order_relaxed)) return nullptr;
  return g_active_route;
}

Attempt_handle callback_attempt_snapshot() {
  if (!g_callback_active.load(std::memory_order_acquire)) return nullptr;
  std::lock_guard<std::mutex> lock(g_route_mutex);
  if (!g_callback_active.load(std::memory_order_relaxed)) return nullptr;
  return g_callback_attempt;
}

bool command_matches_thd(THD *thd, const Command_key &command) {
  if (thd == nullptr || command.connection_incarnation == 0 ||
      command.sequence == 0) {
    return false;
  }
  mysql_mutex_lock(&thd->LOCK_thd_data);
  const bool matches =
      thd->preserve_trx_phase2_connection_incarnation ==
          command.connection_incarnation &&
      thd->preserve_trx_phase2_aggregate_sequence == command.sequence;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
  return matches;
}

bool revocable_pre_body_stage(Preserve_trx_phase2_command_stage stage) {
  return stage == Preserve_trx_phase2_command_stage::ADMISSION_INFLIGHT ||
         stage == Preserve_trx_phase2_command_stage::T0_CLAIMED_PRE_GATE ||
         stage == Preserve_trx_phase2_command_stage::HELD ||
         stage == Preserve_trx_phase2_command_stage::PERMIT_RESERVED;
}

Preserve_trx_phase2_command_stage transition_revocable_stage(
    THD *thd, Preserve_trx_phase2_command_stage target) {
  Preserve_trx_phase2_command_stage stage =
      thd->preserve_trx_phase2_command_stage.load(std::memory_order_acquire);
  while (revocable_pre_body_stage(stage) &&
         !thd->preserve_trx_phase2_command_stage.compare_exchange_weak(
             stage, target, std::memory_order_acq_rel)) {
  }
  return stage;
}

}  // namespace

class Attempt {
 public:
  explicit Attempt(const Owner_config &owner_config) : config(owner_config) {}

  /* The type is private to this translation unit; helpers below form its API. */
  friend Attempt_handle publish_and_register_t0(THD *, const Owner_config &);
  friend Gate_action gate_command(THD *, const Admission_request &);
  friend Finish_result finish_command(THD *, const Command_exit_fact &);
  friend void take_stable_boundary_hints(
      const Attempt_handle &, std::vector<Stable_boundary_hint> *);
  friend void note_teardown_begin(uint64_t);
  friend void note_transaction_cleanup(uint64_t, const Transaction_key &);
  friend Terminal_result tick(const Attempt_handle &, uint64_t, uint64_t,
                              bool);
  friend bool terminal_snapshot(const Attempt_handle &, Terminal_snapshot *);
  friend bool summary_snapshot(const Attempt_handle &, Summary_snapshot *);
  friend void wait_for_change(const Attempt_handle &, uint64_t);
  friend void owner_cancel(const Attempt_handle &, uint32_t);
  friend void publish_native_admission_restored_and_retire_route(
      const Attempt_handle &);
  friend void wait_for_cutoff_response_handoff();
  friend void release_cutoff_responses_and_retire_route(
      const Attempt_handle &);

  struct Command_record {
    Command_key key;
    Transaction_key old_transaction;
    Command_class command_class{Command_class::DEFAULT_DENY};
    bool t0_member{false};
    bool entered_body{false};
    bool has_old_transaction{false};
    bool pending_t0_body_first_transaction{false};
    bool terminalizing{false};
    uint64_t last_mdl_proof_revision{0};
    uint64_t last_mdl_proof_us{0};
  };

  enum class Transaction_state : uint8_t {
    PENDING_T0_ACTIVE_IDENTITY,
    ACTIVE,
    TERMINALIZING,
    TERMINAL
  };

  struct Transaction_record {
    Transaction_key key;
    Transaction_state state{
        Transaction_state::PENDING_T0_ACTIVE_IDENTITY};
    uint64_t raw_engine_cookie{0};
    uint64_t engine_version{0};
    uint32_t isolation_level{0};
    bool identity_sealed{false};
  };

  struct Connection_record {
    THD *thd{nullptr};
    uint64_t thd_cookie{0};
    uint32_t isolation_level{0};
    Preserve_trx_external_thd_pin_handle pin;
    Transaction_key old_transaction;
    Command_key pending_t0_body_first_command;
    uint32_t probe_inflight{0};
    bool has_old_transaction{false};
    bool pin_release_pending{false};
  };

  struct Eligible_body_record {
    Command_key key;
    uint64_t thread_id_projection{0};
    uint64_t native_exit_us{0};
    bool native_exit_observed{false};
  };

  bool terminal_is_hard() const {
    return terminal == Terminal_result::HARD_QUIESCENT ||
           terminal == Terminal_result::HARD_DEADLINE;
  }

  void changed_locked() {
    ++revision;
    condition.notify_all();
  }

  Owner_config config;
  std::mutex mutex;
  std::condition_variable condition;
  std::map<Command_key, Command_record, Command_key_less> commands;
  std::map<Transaction_key, Transaction_record, Transaction_key_less>
      transactions;
  std::map<Support_edge_key, Support_edge, Support_edge_key_less>
      support_edges;
  Support_by_waiter support_by_waiter;
  Support_by_blocker support_by_blocker;
  Support_expiry_index support_expiry;
  Mdl_demand_map mdl_demands;
  Mdl_demand_index mdl_demand_index;
  std::set<Command_key, Command_key_less> retired_during_t0;
  std::map<Command_key, Command_exit_fact, Command_key_less>
      pending_exit_facts;
  std::map<uint64_t, Connection_record> connections;
  std::map<Command_key, Eligible_body_record, Command_key_less>
      eligible_bodies;
  std::map<std::pair<uint64_t, uint64_t>, Command_key>
      eligible_projection_index;
  std::map<uint64_t, Stable_boundary_hint> stable_boundary_hints;
  Terminal_result terminal{Terminal_result::RUNNING};
  uint32_t terminal_cause{0};
  Terminal_snapshot frozen_terminal_snapshot;
  bool terminal_snapshot_frozen{false};
  uint64_t revision{0};
  uint64_t mdl_demand_revision{0};
  uint64_t next_mdl_demand_generation{1};
  bool t0_registration_complete{false};
  bool native_admission_restored{false};
  bool hard_cutoff_responses_released{false};
  bool allow_new_probe_borrows{true};
  uint64_t next_scan_due_us{0};
  bool scan_round_open{false};
  size_t scan_cursor{0};
  std::vector<Command_key> scan_candidates;
  uint64_t t0_scope_registered{0};
  uint64_t t0_executing_max{0};
  uint64_t scan_count{0};
  uint64_t scan_candidate{0};
  uint64_t scan_positive_result{0};
  uint64_t scan_negative_result{0};
  uint64_t scan_unsupported_result{0};
  uint64_t scan_unknown_result{0};
  uint64_t scan_stale_discarded{0};
  std::atomic<uint64_t> scan_overrun{0};
  uint64_t support_edge_registered{0};
  uint64_t permit_issued{0};
  uint64_t returned_4020{0};
  uint64_t teardown_started{0};
  uint64_t lock_proof_unknown{0};
  uint64_t execution_returned_4020_conflict{0};
  uint64_t invariant_violation_count{0};
  std::atomic<uint64_t> tick_crossed_unserviced_progress_deadline{0};
};

namespace {

bool same_command_key(const Command_key &left, const Command_key &right) {
  return left.connection_incarnation == right.connection_incarnation &&
         left.sequence == right.sequence;
}

bool note_eligible_body_locked(Attempt *attempt, const Command_key &key,
                               uint64_t thread_id_projection) {
  if (attempt == nullptr || key.connection_incarnation == 0 ||
      key.sequence == 0 || thread_id_projection == 0) {
    return false;
  }
  auto existing = attempt->eligible_bodies.find(key);
  if (existing != attempt->eligible_bodies.end()) {
    return existing->second.thread_id_projection == thread_id_projection;
  }
  if (attempt->terminal_snapshot_frozen) return false;
  const std::pair<uint64_t, uint64_t> projection{thread_id_projection,
                                                 key.sequence};
  auto projected = attempt->eligible_projection_index.find(projection);
  if (projected != attempt->eligible_projection_index.end() &&
      !same_command_key(projected->second, key)) {
    return false;
  }
  Attempt::Eligible_body_record record;
  record.key = key;
  record.thread_id_projection = thread_id_projection;
  attempt->eligible_bodies.emplace(key, record);
  attempt->eligible_projection_index.emplace(projection, key);
  return true;
}

bool note_eligible_body_exit_locked(Attempt *attempt,
                                    const Command_exit_fact &fact) {
  if (!fact.entered_body || fact.native_body_exit_us == 0 ||
      !note_eligible_body_locked(attempt, fact.command,
                                 fact.thread_id_projection)) {
    return false;
  }
  auto eligible = attempt->eligible_bodies.find(fact.command);
  if (eligible == attempt->eligible_bodies.end()) return false;
  if (eligible->second.native_exit_observed) {
    return eligible->second.native_exit_us == fact.native_body_exit_us;
  }
  eligible->second.native_exit_observed = true;
  eligible->second.native_exit_us = fact.native_body_exit_us;
  return true;
}

bool eligible_body_coverage_complete_locked(const Attempt *attempt) {
  if (attempt == nullptr) return false;
  for (const auto &entry : attempt->eligible_bodies) {
    if (!entry.second.native_exit_observed) return false;
  }
  return true;
}

void append_u64_le(std::string *bytes, uint64_t value) {
  for (unsigned int shift = 0; shift < 64; shift += 8) {
    bytes->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

std::string eligible_body_digest_locked(const Attempt &attempt) {
  static constexpr char kDomain[] = "PRESERVE_PHASE2_ELIGIBLE_BODY_V1";
  std::string canonical(kDomain, sizeof(kDomain));
  append_u64_le(&canonical, attempt.config.generation);
  append_u64_le(&canonical, attempt.eligible_bodies.size());
  for (const auto &entry : attempt.eligible_projection_index) {
    append_u64_le(&canonical, entry.first.first);
    append_u64_le(&canonical, entry.first.second);
  }
  unsigned char digest[SHA256_DIGEST_LENGTH]{};
  SHA_EVP256(reinterpret_cast<const unsigned char *>(canonical.data()),
             canonical.size(), digest);
  static constexpr char kHex[] = "0123456789abcdef";
  std::string encoded(SHA256_DIGEST_LENGTH * 2, '0');
  for (size_t i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
    encoded[i * 2] = kHex[digest[i] >> 4];
    encoded[i * 2 + 1] = kHex[digest[i] & 0x0f];
  }
  return encoded;
}

void freeze_terminal_snapshot_locked(Attempt *attempt,
                                     uint64_t terminal_published_us) {
  if (attempt == nullptr || attempt->terminal_snapshot_frozen) return;
  Terminal_snapshot &snapshot = attempt->frozen_terminal_snapshot;
  snapshot.attempt_id = attempt->config.attempt_id;
  snapshot.generation = attempt->config.generation;
  snapshot.policy_started_us = attempt->config.policy_started_us;
  snapshot.terminal_published_us = terminal_published_us;
  snapshot.hard_published_us = attempt->terminal_is_hard()
                                   ? terminal_published_us
                                   : 0;
  snapshot.terminal_result = attempt->terminal;
  snapshot.terminal_cause = attempt->terminal_cause;
  snapshot.eligible_body_count = attempt->eligible_bodies.size();
  snapshot.eligible_body_key_digest_v1 =
      eligible_body_digest_locked(*attempt);

  const bool coverage_complete =
      eligible_body_coverage_complete_locked(attempt);
  snapshot.exact_body_exit_coverage_complete = coverage_complete;
  if (!coverage_complete) {
    snapshot.last_body_exit_state =
        Last_body_exit_state::COVERAGE_INCOMPLETE;
  } else if (attempt->eligible_bodies.empty()) {
    snapshot.last_body_exit_state = Last_body_exit_state::NO_ELIGIBLE_BODY;
  } else {
    snapshot.last_body_exit_state = Last_body_exit_state::EXACT;
  }

  Command_key_less command_less;
  for (const auto &entry : attempt->eligible_bodies) {
    const Attempt::Eligible_body_record &eligible = entry.second;
    if (!eligible.native_exit_observed) continue;
    if (eligible.native_exit_us > snapshot.last_body_exit_us ||
        (eligible.native_exit_us == snapshot.last_body_exit_us &&
         command_less(snapshot.last_body_exit_command, eligible.key))) {
      snapshot.last_body_exit_us = eligible.native_exit_us;
      snapshot.last_body_exit_command = eligible.key;
      snapshot.last_body_exit_thread_id = eligible.thread_id_projection;
    }
  }
  attempt->terminal_snapshot_frozen = true;
}

Attempt::Transaction_record *find_transaction_locked(
    Attempt *attempt, const Transaction_key &key) {
  if (attempt == nullptr || key.connection_incarnation == 0 ||
      key.ordinal == 0) {
    return nullptr;
  }
  auto transaction = attempt->transactions.find(key);
  return transaction == attempt->transactions.end() ? nullptr
                                                     : &transaction->second;
}

bool erase_support_edge_locked(Attempt *attempt,
                               const Support_edge_key &key) {
  auto edge = attempt->support_edges.find(key);
  if (edge == attempt->support_edges.end()) return false;
  attempt->support_expiry.erase(edge->second.expiry);
  auto waiter = attempt->support_by_waiter.find(key.waiter);
  DBUG_ASSERT(waiter != attempt->support_by_waiter.end());
  if (waiter != attempt->support_by_waiter.end()) {
    waiter->second.erase(key);
    if (waiter->second.empty()) attempt->support_by_waiter.erase(waiter);
  }
  auto blocker = attempt->support_by_blocker.find(key.blocker);
  DBUG_ASSERT(blocker != attempt->support_by_blocker.end());
  if (blocker != attempt->support_by_blocker.end()) {
    blocker->second.erase(key);
    if (blocker->second.empty()) attempt->support_by_blocker.erase(blocker);
  }
  attempt->support_edges.erase(edge);
  return true;
}

bool upsert_support_edge_locked(Attempt *attempt,
                                const Support_edge_key &key,
                                uint64_t expires_at_us,
                                uint64_t mdl_demand_generation = 0) {
  auto edge = attempt->support_edges.find(key);
  if (edge != attempt->support_edges.end()) {
    attempt->support_expiry.erase(edge->second.expiry);
    edge->second.expires_at_us = expires_at_us;
    edge->second.mdl_demand_generation = mdl_demand_generation;
    edge->second.expiry = attempt->support_expiry.emplace(expires_at_us, key);
    return false;
  }
  Support_edge value;
  value.expires_at_us = expires_at_us;
  value.mdl_demand_generation = mdl_demand_generation;
  value.expiry = attempt->support_expiry.emplace(expires_at_us, key);
  const bool inserted = attempt->support_edges.emplace(key, value).second;
  if (!inserted) {
    attempt->support_expiry.erase(value.expiry);
    return false;
  }
  attempt->support_by_waiter[key.waiter].insert(key);
  attempt->support_by_blocker[key.blocker].insert(key);
  ++attempt->support_edge_registered;
  return true;
}

bool erase_waiter_support_locked(Attempt *attempt,
                                 const Command_key &waiter,
                                 Lock_domain domain) {
  const auto indexed = attempt->support_by_waiter.find(waiter);
  if (indexed == attempt->support_by_waiter.end()) return false;
  std::vector<Support_edge_key> doomed;
  for (const Support_edge_key &key : indexed->second) {
    if (key.domain == domain) doomed.push_back(key);
  }
  for (const Support_edge_key &key : doomed) {
    (void)erase_support_edge_locked(attempt, key);
  }
  return !doomed.empty();
}

bool erase_blocker_support_locked(Attempt *attempt,
                                  const Transaction_key &blocker,
                                  Lock_domain domain) {
  const auto indexed = attempt->support_by_blocker.find(blocker);
  if (indexed == attempt->support_by_blocker.end()) return false;
  std::vector<Support_edge_key> doomed;
  for (const Support_edge_key &key : indexed->second) {
    if (key.domain == domain) doomed.push_back(key);
  }
  for (const Support_edge_key &key : doomed) {
    (void)erase_support_edge_locked(attempt, key);
  }
  return !doomed.empty();
}

bool erase_mdl_demand_locked(Attempt *attempt, const Command_key &waiter) {
  auto demand = attempt->mdl_demands.find(waiter);
  bool demand_removed = false;
  if (demand != attempt->mdl_demands.end()) {
    Mdl_demand_index_key index_key;
    index_key.key.mdl_key_init(&demand->second.key);
    index_key.waiter = waiter;
    const size_t index_removed = attempt->mdl_demand_index.erase(index_key);
    DBUG_ASSERT(index_removed == 1);
    attempt->mdl_demands.erase(demand);
    demand_removed = true;
  }
  const bool edge_removed =
      erase_waiter_support_locked(attempt, waiter, Lock_domain::MDL);
  if (demand_removed) ++attempt->mdl_demand_revision;
  return demand_removed || edge_removed;
}

bool upsert_mdl_demand_locked(Attempt *attempt, const Command_key &waiter,
                              const MDL_phase2_wait_snapshot &snapshot,
                              uint64_t expires_at_us,
                              bool *new_or_changed) {
  if (attempt == nullptr || new_or_changed == nullptr ||
      snapshot.request_type == MDL_TYPE_END || expires_at_us == 0) {
    return false;
  }
  *new_or_changed = false;
  auto existing = attempt->mdl_demands.find(waiter);
  if (existing != attempt->mdl_demands.end() &&
      existing->second.key.is_equal(&snapshot.key) &&
      existing->second.request_type == snapshot.request_type) {
    existing->second.expires_at_us = expires_at_us;
    return true;
  }

  if (existing != attempt->mdl_demands.end()) {
    Mdl_demand_index_key old_index_key;
    old_index_key.key.mdl_key_init(&existing->second.key);
    old_index_key.waiter = waiter;
    const size_t index_removed =
        attempt->mdl_demand_index.erase(old_index_key);
    DBUG_ASSERT(index_removed == 1);
    erase_waiter_support_locked(attempt, waiter, Lock_domain::MDL);
    attempt->mdl_demands.erase(existing);
  }
  uint64_t generation = attempt->next_mdl_demand_generation++;
  if (generation == 0) {
    generation = attempt->next_mdl_demand_generation++;
  }
  Mdl_demand demand;
  demand.key = snapshot.key;
  demand.request_type = snapshot.request_type;
  demand.generation = generation;
  demand.expires_at_us = expires_at_us;
  auto inserted = attempt->mdl_demands.emplace(waiter, std::move(demand));
  if (!inserted.second) return false;
  Mdl_demand_index_key index_key;
  index_key.key.mdl_key_init(&inserted.first->second.key);
  index_key.waiter = waiter;
  const bool index_inserted =
      attempt->mdl_demand_index
          .emplace(std::move(index_key), inserted.first->second.generation)
          .second;
  if (!index_inserted) {
    attempt->mdl_demands.erase(inserted.first);
    return false;
  }
  ++attempt->mdl_demand_revision;
  *new_or_changed = true;
  return true;
}

bool erase_transaction_support_locked(Attempt *attempt,
                                      const Transaction_key &transaction) {
  const auto indexed = attempt->support_by_blocker.find(transaction);
  if (indexed == attempt->support_by_blocker.end()) return false;
  const std::vector<Support_edge_key> doomed(indexed->second.begin(),
                                             indexed->second.end());
  for (const Support_edge_key &key : doomed) {
    (void)erase_support_edge_locked(attempt, key);
  }
  return !doomed.empty();
}

void clear_support_locked(Attempt *attempt) {
  attempt->support_edges.clear();
  attempt->support_by_waiter.clear();
  attempt->support_by_blocker.clear();
  attempt->support_expiry.clear();
}

bool expire_support_locked(Attempt *attempt, uint64_t now_us) {
  bool changed = false;
  while (!attempt->support_expiry.empty() &&
         attempt->support_expiry.begin()->first <= now_us) {
    const Support_edge_key key = attempt->support_expiry.begin()->second;
    if (erase_support_edge_locked(attempt, key)) {
      changed = true;
    } else {
      attempt->support_expiry.erase(attempt->support_expiry.begin());
    }
  }
  bool demand_expired = false;
  for (auto demand = attempt->mdl_demands.begin();
       demand != attempt->mdl_demands.end();) {
    if (demand->second.expires_at_us <= now_us) {
      const Command_key waiter = demand->first;
      Mdl_demand_index_key index_key;
      index_key.key.mdl_key_init(&demand->second.key);
      index_key.waiter = waiter;
      const size_t index_removed = attempt->mdl_demand_index.erase(index_key);
      DBUG_ASSERT(index_removed == 1);
      demand = attempt->mdl_demands.erase(demand);
      erase_waiter_support_locked(attempt, waiter, Lock_domain::MDL);
      demand_expired = true;
      changed = true;
    } else {
      ++demand;
    }
  }
  if (demand_expired) ++attempt->mdl_demand_revision;
  return changed;
}

bool has_fresh_support_locked(const Attempt *attempt,
                              const Transaction_key &blocker,
                              uint64_t now_us) {
  const auto indexed = attempt->support_by_blocker.find(blocker);
  if (indexed == attempt->support_by_blocker.end()) return false;
  for (const Support_edge_key &key : indexed->second) {
    const auto edge = attempt->support_edges.find(key);
    if (edge != attempt->support_edges.end() &&
        edge->second.expires_at_us > now_us) {
      return true;
    }
  }
  return false;
}

void enter_fail_closed_locked(Attempt *attempt, Terminal_result terminal,
                              uint32_t cause) {
  if (attempt == nullptr || attempt->terminal != Terminal_result::RUNNING) {
    return;
  }
  DBUG_ASSERT(terminal == Terminal_result::SAFETY_ABORT ||
              terminal == Terminal_result::OWNER_CANCELLED);
  attempt->allow_new_probe_borrows = false;
  attempt->terminal = terminal;
  attempt->terminal_cause = cause;
  clear_support_locked(attempt);
  attempt->mdl_demands.clear();
  attempt->mdl_demand_index.clear();
  attempt->scan_candidates.clear();
  attempt->scan_round_open = false;
  attempt->scan_cursor = 0;
  attempt->stable_boundary_hints.clear();
  for (auto &entry : attempt->commands) {
    entry.second.terminalizing = false;
  }
  for (auto &entry : attempt->transactions) {
    if (entry.second.state == Attempt::Transaction_state::TERMINALIZING) {
      entry.second.state = Attempt::Transaction_state::ACTIVE;
    }
  }
  for (auto &entry : attempt->connections) {
    if (entry.second.thd == nullptr) continue;
    (void)transition_revocable_stage(
        entry.second.thd,
        Preserve_trx_phase2_command_stage::WAIT_NATIVE_RESTORE);
  }
  freeze_terminal_snapshot_locked(attempt, scheduler_monotonic_us());
  attempt->changed_locked();
}

void mark_safety_abort_locked(Attempt *attempt) {
  if (attempt != nullptr && attempt->terminal == Terminal_result::RUNNING) {
    ++attempt->invariant_violation_count;
  }
  enter_fail_closed_locked(attempt, Terminal_result::SAFETY_ABORT, 1);
}

Attempt::Transaction_record *reserve_t0_transaction_locked(
    Attempt *attempt, Attempt::Connection_record *connection,
    uint64_t connection_incarnation, uint64_t raw_engine_cookie,
    uint32_t isolation_level);

Attempt::Transaction_record *find_native_transaction_locked(
    Attempt *attempt, const lock_preserve_phase2_identity &identity,
    bool *known_retiring_owner) {
  if (known_retiring_owner != nullptr) *known_retiring_owner = false;
  if (attempt == nullptr || known_retiring_owner == nullptr ||
      identity.raw_cookie == 0 || identity.version == 0 ||
      identity.owner_thd_cookie == 0) {
    return nullptr;
  }
  Attempt::Connection_record *matching_connection = nullptr;
  Attempt::Connection_record *retiring_connection = nullptr;
  uint64_t connection_incarnation = 0;
  for (auto &entry : attempt->connections) {
    Attempt::Connection_record &connection = entry.second;
    if (connection.thd_cookie != identity.owner_thd_cookie) continue;
    if (connection.thd == nullptr) {
      if (matching_connection != nullptr || retiring_connection != nullptr) {
        return nullptr;
      }
      retiring_connection = &connection;
      continue;
    }
    if (static_cast<uint64_t>(reinterpret_cast<uintptr_t>(connection.thd)) !=
            identity.owner_thd_cookie) {
      return nullptr;
    }
    if (matching_connection != nullptr || retiring_connection != nullptr) {
      return nullptr;
    }
    matching_connection = &connection;
    connection_incarnation = entry.first;
  }
  if (matching_connection == nullptr) {
    if (retiring_connection == nullptr) return nullptr;
    if (retiring_connection->has_old_transaction) {
      Attempt::Transaction_record *retiring_transaction =
          find_transaction_locked(attempt,
                                  retiring_connection->old_transaction);
      if (retiring_transaction == nullptr ||
          retiring_transaction->state ==
              Attempt::Transaction_state::TERMINAL ||
          (retiring_transaction->raw_engine_cookie != 0 &&
           retiring_transaction->raw_engine_cookie != identity.raw_cookie) ||
          (retiring_transaction->identity_sealed &&
           (retiring_transaction->raw_engine_cookie != identity.raw_cookie ||
            retiring_transaction->engine_version != identity.version))) {
        return nullptr;
      }
    }
    *known_retiring_owner = true;
    return nullptr;
  }

  Attempt::Transaction_record *match = nullptr;
  if (matching_connection->has_old_transaction) {
    match = find_transaction_locked(attempt,
                                    matching_connection->old_transaction);
  } else {
    auto pending = attempt->commands.find(
        matching_connection->pending_t0_body_first_command);
    if (pending == attempt->commands.end() || !pending->second.t0_member ||
        !pending->second.entered_body ||
        !pending->second.pending_t0_body_first_transaction ||
        pending->first.connection_incarnation != connection_incarnation) {
      return nullptr;
    }
    match = reserve_t0_transaction_locked(
        attempt, matching_connection, connection_incarnation,
        identity.raw_cookie, matching_connection->isolation_level);
    if (match != nullptr) {
      pending->second.old_transaction = match->key;
      pending->second.has_old_transaction = true;
      pending->second.pending_t0_body_first_transaction = false;
      matching_connection->pending_t0_body_first_command = {};
    }
  }
  if (match == nullptr ||
      match->state == Attempt::Transaction_state::TERMINAL ||
      (match->raw_engine_cookie != 0 &&
       match->raw_engine_cookie != identity.raw_cookie)) {
    return nullptr;
  }
  if (match->identity_sealed) {
    if (match->engine_version != identity.version) {
      return nullptr;
    }
  } else {
    match->raw_engine_cookie = identity.raw_cookie;
    match->engine_version = identity.version;
    match->identity_sealed = true;
    match->state = Attempt::Transaction_state::ACTIVE;
  }
  return match;
}

Attempt::Transaction_record *reserve_t0_transaction_locked(
    Attempt *attempt, Attempt::Connection_record *connection,
    uint64_t connection_incarnation, uint64_t raw_engine_cookie,
    uint32_t isolation_level) {
  if (attempt == nullptr || connection == nullptr ||
      connection_incarnation == 0) {
    return nullptr;
  }
  if (!connection->has_old_transaction) {
    connection->old_transaction = {connection_incarnation, 1};
    connection->has_old_transaction = true;
  }
  Attempt::Transaction_record transaction;
  transaction.key = connection->old_transaction;
  transaction.raw_engine_cookie = raw_engine_cookie;
  transaction.isolation_level = isolation_level;
  auto entry = attempt->transactions.emplace(transaction.key, transaction);
  if (!entry.second) {
    Attempt::Transaction_record &existing = entry.first->second;
    if (existing.state == Attempt::Transaction_state::TERMINAL) {
      mark_safety_abort_locked(attempt);
      return nullptr;
    }
    if (existing.raw_engine_cookie == 0) {
      existing.raw_engine_cookie = raw_engine_cookie;
    } else if (raw_engine_cookie != 0 &&
               existing.raw_engine_cookie != raw_engine_cookie) {
      mark_safety_abort_locked(attempt);
      return nullptr;
    }
  }
  return &entry.first->second;
}

bool seal_transaction_from_observation_locked(
    Attempt *attempt, Attempt::Transaction_record *transaction,
    const Transaction_observation &observation) {
  if (attempt == nullptr || transaction == nullptr ||
      !observation.sql_transaction_active) {
    return false;
  }
  switch (observation.engine_state) {
    case Engine_identity_state::EXACT_ACTIVE:
      if (observation.raw_engine_cookie == 0 ||
          observation.engine_version == 0 ||
          (transaction->raw_engine_cookie != 0 &&
           transaction->raw_engine_cookie != observation.raw_engine_cookie)) {
        mark_safety_abort_locked(attempt);
        return false;
      }
      if (transaction->identity_sealed &&
          (transaction->raw_engine_cookie != observation.raw_engine_cookie ||
           transaction->engine_version != observation.engine_version)) {
        mark_safety_abort_locked(attempt);
        return false;
      }
      transaction->raw_engine_cookie = observation.raw_engine_cookie;
      transaction->engine_version = observation.engine_version;
      transaction->isolation_level = observation.isolation_level;
      transaction->identity_sealed = true;
      transaction->state = Attempt::Transaction_state::ACTIVE;
      return true;
    case Engine_identity_state::NONE:
      /*
        An SQL transaction may legitimately exist before InnoDB starts its
        reusable trx object.  Keep the T0 ordinal pending until the first
        EXACT_ACTIVE observation; the raw cookie is only a candidate identity
        and has no lifecycle meaning while the engine reports NONE.
      */
      if (transaction->identity_sealed) {
        mark_safety_abort_locked(attempt);
        return false;
      }
      transaction->isolation_level = observation.isolation_level;
      return true;
    case Engine_identity_state::RETRY_LIFECYCLE:
    case Engine_identity_state::UNSUPPORTED:
      mark_safety_abort_locked(attempt);
      return false;
  }
  return false;
}

bool observation_is_same_transaction(
    const Attempt::Transaction_record &transaction,
    const Transaction_observation &observation) {
  if (!observation.sql_transaction_active || !transaction.identity_sealed) {
    return false;
  }
  return observation.engine_state == Engine_identity_state::EXACT_ACTIVE &&
         observation.raw_engine_cookie == transaction.raw_engine_cookie &&
         observation.engine_version == transaction.engine_version;
}

void close_transaction_locked(Attempt *attempt,
                              Attempt::Connection_record *connection,
                              Attempt::Transaction_record *transaction);

Attempt::Transaction_record *validate_old_transaction_at_gate_locked(
    Attempt *attempt, Attempt::Connection_record *connection,
    const Transaction_observation &observation) {
  if (attempt == nullptr || connection == nullptr ||
      !connection->has_old_transaction) {
    return nullptr;
  }
  Attempt::Transaction_record *transaction =
      find_transaction_locked(attempt, connection->old_transaction);
  if (transaction == nullptr ||
      transaction->state == Attempt::Transaction_state::TERMINAL) {
    connection->has_old_transaction = false;
    connection->old_transaction = {};
    return nullptr;
  }
  if (!observation.sql_transaction_active) {
    close_transaction_locked(attempt, connection, transaction);
    return nullptr;
  }
  if (!transaction->identity_sealed) {
    if (!seal_transaction_from_observation_locked(attempt, transaction,
                                                   observation)) {
      return nullptr;
    }
  } else if (!observation_is_same_transaction(*transaction, observation)) {
    mark_safety_abort_locked(attempt);
    return nullptr;
  }
  return transaction;
}

void close_transaction_locked(Attempt *attempt,
                              Attempt::Connection_record *connection,
                              Attempt::Transaction_record *transaction) {
  if (attempt == nullptr || connection == nullptr || transaction == nullptr) {
    return;
  }
  transaction->state = Attempt::Transaction_state::TERMINAL;
  erase_transaction_support_locked(attempt, transaction->key);
  attempt->stable_boundary_hints.erase(
      transaction->key.connection_incarnation);
  connection->has_old_transaction = false;
  connection->old_transaction = {};
}

bool prove_mdl_support_at_owner_gate_locked(
    Attempt *attempt, THD *command_thd,
    Attempt::Command_record *command_record,
    const Attempt::Connection_record &connection,
    const Attempt::Transaction_record &transaction, uint64_t sampled_at_us,
    bool *support_registered) {
  if (attempt == nullptr || command_thd == nullptr ||
      command_record == nullptr || support_registered == nullptr) {
    return false;
  }
  *support_registered = false;

  const Preserve_trx_phase2_command_stage stage =
      command_thd->preserve_trx_phase2_command_stage.load(
          std::memory_order_acquire);
  const bool owner_local =
      command_thd == current_thd && connection.thd == command_thd &&
      command_thd->mdl_context.get_owner() != nullptr &&
      command_thd->mdl_context.get_owner()->get_thd() == command_thd &&
      connection.has_old_transaction &&
      same_transaction_key(connection.old_transaction, transaction.key) &&
      stage != Preserve_trx_phase2_command_stage::EXECUTING &&
      stage != Preserve_trx_phase2_command_stage::CUTOFF &&
      stage != Preserve_trx_phase2_command_stage::NATIVE_PRE_BODY_EXIT &&
      stage != Preserve_trx_phase2_command_stage::WAIT_NATIVE_RESTORE;
  DBUG_ASSERT(owner_local);
  if (!owner_local) {
    mark_safety_abort_locked(attempt);
    attempt->changed_locked();
    return false;
  }

  Mdl_owner_probe_matches matches;
  Mdl_owner_probe_context context;
  context.demands = &attempt->mdl_demands;
  context.demand_index = &attempt->mdl_demand_index;
  context.matches = &matches;
  context.owner_context = &command_thd->mdl_context;
  context.blocker_connection_incarnation =
      transaction.key.connection_incarnation;
  context.now_us = sampled_at_us;

  bool stopped = command_thd->mdl_context.visit_tickets(
      MDL_TRANSACTION, mdl_owner_ticket_visitor, &context);
  if (!stopped && !context.incomplete && !matches.empty()) {
    context.phase = Mdl_owner_probe_phase::EXPLICIT;
    stopped = command_thd->mdl_context.visit_tickets(
        MDL_EXPLICIT, mdl_owner_ticket_visitor, &context);
  }
  const uint64_t finished_at_us = scheduler_monotonic_us();
  if (stopped || context.incomplete) {
    mark_safety_abort_locked(attempt);
    attempt->changed_locked();
    return false;
  }

  bool state_changed = expire_support_locked(attempt, finished_at_us);
  const bool had_fresh_support = has_fresh_support_locked(
      attempt, transaction.key, finished_at_us);
  size_t old_edge_count = 0;
  bool edge_set_changed = false;
  const auto indexed = attempt->support_by_blocker.find(transaction.key);
  if (indexed != attempt->support_by_blocker.end()) {
    for (const Support_edge_key &key : indexed->second) {
      if (key.domain != Lock_domain::MDL) continue;
      const auto edge = attempt->support_edges.find(key);
      DBUG_ASSERT(edge != attempt->support_edges.end());
      if (edge == attempt->support_edges.end()) {
        edge_set_changed = true;
        continue;
      }
      ++old_edge_count;
      const auto match = matches.find(key.waiter);
      const auto demand = attempt->mdl_demands.find(key.waiter);
      const bool still_supported =
          match != matches.end() && !match->second.non_releasing &&
          demand != attempt->mdl_demands.end() &&
          demand->second.generation == match->second.demand_generation &&
          demand->second.expires_at_us > finished_at_us &&
          edge->second.mdl_demand_generation ==
              match->second.demand_generation;
      edge_set_changed = edge_set_changed || !still_supported;
    }
  }

  erase_blocker_support_locked(attempt, transaction.key, Lock_domain::MDL);
  const uint64_t proof_expiry = bounded_lease_expiry(
      sampled_at_us, attempt->config.absolute_deadline_us);
  size_t published_count = 0;
  if (finished_at_us < proof_expiry) {
    for (const auto &match : matches) {
      if (match.second.non_releasing) continue;
      const auto demand = attempt->mdl_demands.find(match.first);
      if (demand == attempt->mdl_demands.end() ||
          demand->second.generation != match.second.demand_generation ||
          demand->second.expires_at_us <= finished_at_us) {
        continue;
      }
      const uint64_t expires_at_us =
          std::min(proof_expiry, demand->second.expires_at_us);
      Support_edge_key edge_key;
      edge_key.waiter = match.first;
      edge_key.blocker = transaction.key;
      edge_key.domain = Lock_domain::MDL;
      (void)upsert_support_edge_locked(
          attempt, edge_key, expires_at_us,
          match.second.demand_generation);
      ++published_count;
    }
  }

  command_record->last_mdl_proof_revision = attempt->mdl_demand_revision;
  command_record->last_mdl_proof_us = finished_at_us;
  edge_set_changed =
      edge_set_changed || old_edge_count != published_count;
  const bool has_fresh_support = has_fresh_support_locked(
      attempt, transaction.key, finished_at_us);
  *support_registered = !had_fresh_support && has_fresh_support;
  state_changed = state_changed || edge_set_changed || *support_registered;
  if (state_changed) attempt->changed_locked();
  return true;
}

void apply_command_exit_locked(
    Attempt *attempt, Attempt::Connection_record *connection,
    const Attempt::Command_record &command,
    const Command_exit_fact &fact) {
  if (attempt == nullptr) return;
  if (connection == nullptr) {
    if (fact.transaction_observation.sql_transaction_active) {
      mark_safety_abort_locked(attempt);
    }
    return;
  }

  if (connection->pending_t0_body_first_command.connection_incarnation ==
          command.key.connection_incarnation &&
      connection->pending_t0_body_first_command.sequence ==
          command.key.sequence) {
    connection->pending_t0_body_first_command = {};
  }
  if (command.pending_t0_body_first_transaction && fact.entered_body &&
      !connection->has_old_transaction &&
      fact.transaction_observation.sql_transaction_active) {
    const uint64_t raw_cookie =
        fact.transaction_observation.engine_state ==
                Engine_identity_state::EXACT_ACTIVE
            ? fact.transaction_observation.raw_engine_cookie
            : 0;
    Attempt::Transaction_record *transaction = reserve_t0_transaction_locked(
        attempt, connection, command.key.connection_incarnation, raw_cookie,
        fact.transaction_observation.isolation_level);
    if (transaction == nullptr ||
        !seal_transaction_from_observation_locked(
            attempt, transaction, fact.transaction_observation)) {
      mark_safety_abort_locked(attempt);
      return;
    }
  }

  if (!connection->has_old_transaction) return;
  Attempt::Transaction_record *transaction =
      find_transaction_locked(attempt, connection->old_transaction);
  if (transaction == nullptr) {
    mark_safety_abort_locked(attempt);
    return;
  }
  if (!fact.transaction_observation.sql_transaction_active) {
    close_transaction_locked(attempt, connection, transaction);
    return;
  }
  if (!transaction->identity_sealed) {
    if (!seal_transaction_from_observation_locked(
            attempt, transaction, fact.transaction_observation)) {
      mark_safety_abort_locked(attempt);
      return;
    }
  } else if (!observation_is_same_transaction(
                 *transaction, fact.transaction_observation)) {
    mark_safety_abort_locked(attempt);
    return;
  }
  if (command.terminalizing ||
      transaction->state == Attempt::Transaction_state::TERMINALIZING) {
    mark_safety_abort_locked(attempt);
    return;
  }
  if (fact.entered_body && fact.thread_id_projection != 0 &&
      attempt->terminal == Terminal_result::RUNNING) {
    attempt->stable_boundary_hints[command.key.connection_incarnation] =
        Stable_boundary_hint{command.key, fact.thread_id_projection};
  }
}

void retire_callback_if_drained(const Attempt_handle &attempt) {
  if (attempt == nullptr) return;
  std::lock_guard<std::mutex> route_lock(g_route_mutex);
  if (g_callback_attempt != attempt || g_active_route == attempt) return;
  std::lock_guard<std::mutex> attempt_lock(attempt->mutex);
  if (!attempt->commands.empty()) return;
  g_callback_active.store(false, std::memory_order_release);
  g_callback_attempt.reset();
}

void register_captured_command(THD *command_thd,
                               const Command_key &command) {
  Attempt_handle attempt = active_route_snapshot();
  if (attempt == nullptr) return;

  Preserve_trx_external_thd_pin_handle pin;
  uint64_t thd_cookie = 0;
  uint32_t isolation_level = 0;
  mysql_mutex_lock(&command_thd->LOCK_thd_data);
  const bool still_current =
      command_thd->preserve_trx_phase2_connection_incarnation ==
          command.connection_incarnation &&
      command_thd->preserve_trx_phase2_aggregate_sequence ==
          command.sequence &&
      command_thd->preserve_trx_phase2_command_stage.load(
          std::memory_order_acquire) !=
          Preserve_trx_phase2_command_stage::IDLE;
  if (still_current) {
    thd_cookie = static_cast<uint64_t>(
        reinterpret_cast<uintptr_t>(command_thd));
    isolation_level = static_cast<uint32_t>(command_thd->tx_isolation);
    pin = preserve_trx_acquire_external_thd_pin_locked(command_thd);
  }
  mysql_mutex_unlock(&command_thd->LOCK_thd_data);
  if (!still_current) return;

  bool pin_published = false;
  {
    std::lock_guard<std::mutex> lock(attempt->mutex);
    Preserve_trx_phase2_command_stage stage =
        command_thd->preserve_trx_phase2_command_stage.load(
            std::memory_order_acquire);
    if (!pin) {
      stage = transition_revocable_stage(
          command_thd,
          Preserve_trx_phase2_command_stage::WAIT_NATIVE_RESTORE);
      mark_safety_abort_locked(attempt.get());
      return;
    }

    Attempt::Command_record record;
    record.key = command;
    record.entered_body =
        stage == Preserve_trx_phase2_command_stage::EXECUTING;
    auto command_entry = attempt->commands.emplace(command, record).first;
    command_entry->second.entered_body =
        command_entry->second.entered_body || record.entered_body;
    if (record.entered_body &&
        !note_eligible_body_locked(attempt.get(), command,
                                   command_thd->thread_id())) {
      mark_safety_abort_locked(attempt.get());
    }

    if (attempt->terminal_is_hard()) {
      stage = transition_revocable_stage(
          command_thd, Preserve_trx_phase2_command_stage::CUTOFF);
    } else {
      auto connection =
          attempt->connections.find(command.connection_incarnation);
      if (connection == attempt->connections.end()) {
        Attempt::Connection_record connection_record;
        connection_record.thd = command_thd;
        connection_record.thd_cookie = thd_cookie;
        connection_record.isolation_level = isolation_level;
        connection_record.pin = std::move(pin);
        attempt->connections.emplace(command.connection_incarnation,
                                     std::move(connection_record));
        pin_published = true;
      } else if (connection->second.thd != command_thd ||
                 connection->second.thd_cookie != thd_cookie) {
        mark_safety_abort_locked(attempt.get());
      }
      if (attempt->terminal == Terminal_result::OWNER_CANCELLED ||
          attempt->terminal == Terminal_result::SAFETY_ABORT) {
        stage = transition_revocable_stage(
            command_thd,
            Preserve_trx_phase2_command_stage::WAIT_NATIVE_RESTORE);
      }
    }
    attempt->changed_locked();
  }
  if (pin_published &&
      preserve_trx_external_thd_scheduler_teardown_started(command_thd)) {
    note_teardown_begin(command.connection_incarnation);
  }
}

}  // namespace

bool capture_command(THD *command_thd, Command_key *command) {
  if (command != nullptr) *command = {};
  if (command_thd == nullptr ||
      !preserve_trx_standby_phase2_source_capture_enabled()) {
    return false;
  }

  mysql_mutex_lock(&command_thd->LOCK_thd_data);
  const Preserve_trx_phase2_command_stage stage =
      command_thd->preserve_trx_phase2_command_stage.load(
          std::memory_order_acquire);
  if (stage != Preserve_trx_phase2_command_stage::IDLE) {
    mysql_mutex_unlock(&command_thd->LOCK_thd_data);
    return false;
  }
  if (command_thd->preserve_trx_phase2_connection_incarnation == 0) {
    command_thd->preserve_trx_phase2_connection_incarnation =
        next_connection_incarnation();
  }
  ++command_thd->preserve_trx_phase2_aggregate_sequence;
  if (command_thd->preserve_trx_phase2_aggregate_sequence == 0) {
    ++command_thd->preserve_trx_phase2_aggregate_sequence;
  }
  const Command_key captured{
      command_thd->preserve_trx_phase2_connection_incarnation,
      command_thd->preserve_trx_phase2_aggregate_sequence};
  command_thd->preserve_trx_phase2_command_stage.store(
      Preserve_trx_phase2_command_stage::ADMISSION_INFLIGHT,
      std::memory_order_release);
  mysql_mutex_unlock(&command_thd->LOCK_thd_data);

  if (command != nullptr) *command = captured;
  DEBUG_SYNC(command_thd, "phase2_sched_after_admission_published");
  register_captured_command(command_thd, captured);
  return true;
}

bool captured_command_key(THD *command_thd, Command_key *command) {
  if (command != nullptr) *command = {};
  if (command_thd == nullptr ||
      !preserve_trx_standby_phase2_source_capture_enabled()) {
    return false;
  }
  mysql_mutex_lock(&command_thd->LOCK_thd_data);
  const Preserve_trx_phase2_command_stage stage =
      command_thd->preserve_trx_phase2_command_stage.load(
          std::memory_order_acquire);
  const Command_key captured{
      command_thd->preserve_trx_phase2_connection_incarnation,
      command_thd->preserve_trx_phase2_aggregate_sequence};
  mysql_mutex_unlock(&command_thd->LOCK_thd_data);
  if (stage == Preserve_trx_phase2_command_stage::IDLE ||
      captured.connection_incarnation == 0 || captured.sequence == 0) {
    return false;
  }
  if (command != nullptr) *command = captured;
  return true;
}

bool dependency_transaction_is_active(THD *thd) {
  return thd != nullptr &&
         preserve_trx_standby_phase2_source_capture_enabled() &&
         thd->in_active_multi_stmt_transaction() &&
         (thd->variables.option_bits &
          (OPTION_BEGIN | OPTION_NOT_AUTOCOMMIT)) != 0;
}

Attempt_handle publish_and_register_t0(THD *owner,
                                       const Owner_config &config) {
  if (owner == nullptr) return nullptr;
  Attempt_handle attempt = std::make_shared<Attempt>(config);
  {
    std::lock_guard<std::mutex> lock(g_route_mutex);
    if (g_active_route != nullptr || g_callback_attempt != nullptr) {
      return nullptr;
    }
    g_active_route = attempt;
    g_callback_attempt = attempt;
    g_callback_active.store(true, std::memory_order_release);
    g_route_active.store(true, std::memory_order_release);
  }

  DEBUG_SYNC(owner, "phase2_sched_after_publish_before_t0");

  class T0_collector final : public Do_THD_Impl {
   public:
    T0_collector(THD *attempt_owner, const Attempt_handle &attempt_handle)
        : m_owner(attempt_owner), m_attempt(attempt_handle) {}

    void operator()(THD *candidate) override {
      if (candidate == nullptr ||
          (m_attempt->config.owner_thread_id != 0 &&
           candidate->thread_id() == m_attempt->config.owner_thread_id)) {
        return;
      }

      mysql_mutex_lock(&candidate->LOCK_thd_data);
      if (preserve_trx_phase2_scheduler_bypass_actor(
              candidate, m_attempt->config.owner_thread_id)) {
        mysql_mutex_unlock(&candidate->LOCK_thd_data);
        return;
      }
      Preserve_trx_phase2_command_stage stage =
          candidate->preserve_trx_phase2_command_stage.load(
              std::memory_order_acquire);
      const bool sql_transaction_active =
          dependency_transaction_is_active(candidate);
      const uint64_t thd_cookie = static_cast<uint64_t>(
          reinterpret_cast<uintptr_t>(candidate));
      const uint32_t isolation_level =
          static_cast<uint32_t>(candidate->tx_isolation);
      const uint64_t raw_engine_cookie =
          sql_transaction_active
              ? trx_preserve_phase2_peek_raw_cookie(candidate)
              : 0;

      bool t0_claimed = false;
      if (stage == Preserve_trx_phase2_command_stage::ADMISSION_INFLIGHT) {
        Preserve_trx_phase2_command_stage expected = stage;
        if (candidate->preserve_trx_phase2_command_stage.compare_exchange_strong(
                expected,
                Preserve_trx_phase2_command_stage::T0_CLAIMED_PRE_GATE,
                std::memory_order_acq_rel)) {
          stage = Preserve_trx_phase2_command_stage::T0_CLAIMED_PRE_GATE;
          t0_claimed = true;
        } else {
          stage = expected;
        }
      }

      const bool command_is_t0 =
          stage == Preserve_trx_phase2_command_stage::T0_CLAIMED_PRE_GATE ||
          stage == Preserve_trx_phase2_command_stage::EXECUTING;
      if (!command_is_t0 && !sql_transaction_active) {
        mysql_mutex_unlock(&candidate->LOCK_thd_data);
        return;
      }

      if (candidate->preserve_trx_phase2_connection_incarnation == 0) {
        candidate->preserve_trx_phase2_connection_incarnation =
            next_connection_incarnation();
      }
      Command_key key{
          candidate->preserve_trx_phase2_connection_incarnation,
          command_is_t0
              ? candidate->preserve_trx_phase2_aggregate_sequence
              : 0};
      if (command_is_t0 && key.sequence == 0) {
        mysql_mutex_unlock(&candidate->LOCK_thd_data);
        return;
      }

      Preserve_trx_external_thd_pin_handle pin =
          preserve_trx_acquire_external_thd_pin_locked(candidate);
      if (!pin) {
        if (stage == Preserve_trx_phase2_command_stage::T0_CLAIMED_PRE_GATE) {
          candidate->preserve_trx_phase2_command_stage.store(
              Preserve_trx_phase2_command_stage::WAIT_NATIVE_RESTORE,
              std::memory_order_release);
        }
        mysql_mutex_unlock(&candidate->LOCK_thd_data);
        std::lock_guard<std::mutex> lock(m_attempt->mutex);
        mark_safety_abort_locked(m_attempt.get());
        return;
      }
      mysql_mutex_unlock(&candidate->LOCK_thd_data);

      if (t0_claimed) {
        DEBUG_SYNC(m_owner, "phase2_sched_after_t0_claim_before_register");
      }

      bool executing_registered = false;
      bool pin_published = false;
      {
        std::lock_guard<std::mutex> lock(m_attempt->mutex);
        auto connection =
            m_attempt->connections.find(key.connection_incarnation);
        if (connection == m_attempt->connections.end()) {
          Attempt::Connection_record connection_record;
          connection_record.thd = candidate;
          connection_record.thd_cookie = thd_cookie;
          connection_record.isolation_level = isolation_level;
          connection_record.pin = std::move(pin);
          connection =
              m_attempt->connections
                  .emplace(key.connection_incarnation,
                           std::move(connection_record))
                  .first;
          pin_published = true;
        } else if (connection->second.thd != candidate ||
                   connection->second.thd_cookie != thd_cookie) {
          mark_safety_abort_locked(m_attempt.get());
          return;
        }
        Attempt::Transaction_record *old_transaction = nullptr;
        if (sql_transaction_active) {
          old_transaction = reserve_t0_transaction_locked(
              m_attempt.get(), &connection->second,
              key.connection_incarnation, raw_engine_cookie,
              isolation_level);
        }
        if (!command_is_t0 && old_transaction != nullptr &&
            candidate->thread_id() != 0 &&
            m_attempt->terminal == Terminal_result::RUNNING) {
          /*
            An idle T0 transaction is already at a native command boundary.
            Publish the same disposable Phase1 refresh hint that a later BODY
            exit would publish; a new BODY erases it before entering EXECUTING.
          */
          m_attempt->stable_boundary_hints[key.connection_incarnation] =
              Stable_boundary_hint{key, candidate->thread_id()};
        }
        if (command_is_t0 &&
            m_attempt->retired_during_t0.count(key) == 0) {
          Attempt::Command_record record;
          record.key = key;
          record.t0_member = true;
          record.entered_body =
              stage == Preserve_trx_phase2_command_stage::EXECUTING;
          record.pending_t0_body_first_transaction =
              record.entered_body && !sql_transaction_active;
          if (old_transaction != nullptr) {
            record.old_transaction = old_transaction->key;
            record.has_old_transaction = true;
          }
          auto command = m_attempt->commands.emplace(key, record).first;
          command->second.t0_member = true;
          command->second.entered_body =
              command->second.entered_body || record.entered_body;
          executing_registered = command->second.entered_body;
          if (record.entered_body &&
              !note_eligible_body_locked(m_attempt.get(), key,
                                         candidate->thread_id())) {
            mark_safety_abort_locked(m_attempt.get());
          }
          command->second.pending_t0_body_first_transaction =
              command->second.pending_t0_body_first_transaction ||
              record.pending_t0_body_first_transaction;
          if (command->second.pending_t0_body_first_transaction) {
            if (connection->second.pending_t0_body_first_command.sequence != 0 &&
                (connection->second.pending_t0_body_first_command
                         .connection_incarnation != key.connection_incarnation ||
                 connection->second.pending_t0_body_first_command.sequence !=
                     key.sequence)) {
              mark_safety_abort_locked(m_attempt.get());
            } else {
              connection->second.pending_t0_body_first_command = key;
            }
          }
          if (old_transaction != nullptr) {
            command->second.old_transaction = old_transaction->key;
            command->second.has_old_transaction = true;
          }
        }
        m_attempt->changed_locked();
      }
      if (pin_published &&
          preserve_trx_external_thd_scheduler_teardown_started(candidate)) {
        note_teardown_begin(key.connection_incarnation);
      }
      if (executing_registered) {
        DEBUG_SYNC(m_owner,
                   "phase2_sched_after_t0_executing_registered");
      }
    }

   private:
    THD *m_owner;
    Attempt_handle m_attempt;
  };

  T0_collector collector(owner, attempt);
  Global_THD_manager::get_instance()->do_for_all_thd_copy(&collector);

  {
    std::lock_guard<std::mutex> lock(attempt->mutex);
    for (const auto &entry : attempt->pending_exit_facts) {
      auto connection = attempt->connections.find(
          entry.first.connection_incarnation);
      Attempt::Command_record retired;
      retired.key = entry.first;
      retired.t0_member = true;
      retired.entered_body = entry.second.entered_body;
      retired.pending_t0_body_first_transaction =
          entry.second.entered_body &&
          (connection == attempt->connections.end() ||
           !connection->second.has_old_transaction);
      apply_command_exit_locked(
          attempt.get(),
          connection == attempt->connections.end() ? nullptr
                                                    : &connection->second,
          retired, entry.second);
    }
    attempt->pending_exit_facts.clear();
    uint64_t t0_executing = 0;
    for (const auto &entry : attempt->commands) {
      if (!entry.second.t0_member) continue;
      ++attempt->t0_scope_registered;
      if (entry.second.entered_body) ++t0_executing;
    }
    attempt->t0_executing_max =
        std::max(attempt->t0_executing_max, t0_executing);
    attempt->t0_registration_complete = true;
    attempt->retired_during_t0.clear();
    attempt->changed_locked();
  }
  DEBUG_SYNC(owner, "phase2_sched_after_t0_registration");
  return attempt;
}

Gate_action gate_command(THD *command_thd,
                         const Admission_request &request) {
  if (command_thd == nullptr ||
      !command_matches_thd(command_thd, request.command)) {
    return Gate_action::NATIVE_PRE_BODY_EXIT;
  }

  for (;;) {
    Attempt_handle attempt = active_route_snapshot();
    if (attempt == nullptr) {
      if (preserve_trx_phase2_existing_closing_gate_active()) {
        const Preserve_trx_phase2_command_stage stage =
            transition_revocable_stage(
                command_thd, Preserve_trx_phase2_command_stage::CUTOFF);
        return stage == Preserve_trx_phase2_command_stage::EXECUTING
                   ? Gate_action::ENTER_BODY
                   : Gate_action::CUTOFF_4020;
      }

      DEBUG_SYNC(command_thd,
                 "phase2_sched_after_route_null_before_body_cas");
      if (g_route_active.load(std::memory_order_acquire)) continue;

      Preserve_trx_phase2_command_stage expected =
          Preserve_trx_phase2_command_stage::ADMISSION_INFLIGHT;
      if (command_thd->preserve_trx_phase2_command_stage
              .compare_exchange_strong(
                  expected, Preserve_trx_phase2_command_stage::EXECUTING,
                  std::memory_order_acq_rel)) {
        DEBUG_SYNC(command_thd, "phase2_sched_after_execution_entered");
        if (g_route_active.load(std::memory_order_acquire)) continue;
        return Gate_action::ENTER_BODY;
      }
      if (expected == Preserve_trx_phase2_command_stage::EXECUTING) {
        return Gate_action::ENTER_BODY;
      }
      if (expected == Preserve_trx_phase2_command_stage::CUTOFF) {
        return Gate_action::CUTOFF_4020;
      }
      if (expected == Preserve_trx_phase2_command_stage::NATIVE_PRE_BODY_EXIT) {
        return Gate_action::NATIVE_PRE_BODY_EXIT;
      }
      if (expected == Preserve_trx_phase2_command_stage::WAIT_NATIVE_RESTORE) {
        Preserve_trx_phase2_command_stage restore_expected = expected;
        if (!command_thd->preserve_trx_phase2_command_stage
                 .compare_exchange_strong(
                     restore_expected,
                     Preserve_trx_phase2_command_stage::ADMISSION_INFLIGHT,
                     std::memory_order_acq_rel)) {
          continue;
        }
        return Gate_action::RETRY_NATIVE;
      }
      if (expected == Preserve_trx_phase2_command_stage::IDLE) {
        return Gate_action::RETRY_NATIVE;
      }
      continue;
    }

    std::unique_lock<std::mutex> lock(attempt->mutex);
    if (!attempt->t0_registration_complete) {
      attempt->condition.wait_for(lock, std::chrono::milliseconds(5));
      continue;
    }

    Preserve_trx_phase2_command_stage stage =
        command_thd->preserve_trx_phase2_command_stage.load(
            std::memory_order_acquire);
    if (stage == Preserve_trx_phase2_command_stage::EXECUTING) {
      attempt->stable_boundary_hints.erase(
          request.command.connection_incarnation);
      Attempt::Command_record record;
      record.key = request.command;
      record.entered_body = true;
      attempt->commands.emplace(request.command, record);
      if (!note_eligible_body_locked(attempt.get(), request.command,
                                     command_thd->thread_id())) {
        mark_safety_abort_locked(attempt.get());
        continue;
      }
      return Gate_action::ENTER_BODY;
    }

    if (attempt->terminal_is_hard()) {
      stage = transition_revocable_stage(
          command_thd, Preserve_trx_phase2_command_stage::CUTOFF);
      if (stage == Preserve_trx_phase2_command_stage::EXECUTING) {
        return Gate_action::ENTER_BODY;
      }
      ++attempt->returned_4020;
      return Gate_action::CUTOFF_4020;
    }
    if (attempt->terminal == Terminal_result::OWNER_CANCELLED ||
        attempt->terminal == Terminal_result::SAFETY_ABORT) {
      if (!attempt->native_admission_restored) {
        stage = transition_revocable_stage(
            command_thd,
            Preserve_trx_phase2_command_stage::WAIT_NATIVE_RESTORE);
        if (stage ==
            Preserve_trx_phase2_command_stage::NATIVE_PRE_BODY_EXIT) {
          return Gate_action::NATIVE_PRE_BODY_EXIT;
        }
        if (stage == Preserve_trx_phase2_command_stage::CUTOFF) {
          ++attempt->returned_4020;
          return Gate_action::CUTOFF_4020;
        }
        attempt->condition.wait_for(lock, std::chrono::milliseconds(5));
        continue;
      }
      if (stage == Preserve_trx_phase2_command_stage::WAIT_NATIVE_RESTORE) {
        Preserve_trx_phase2_command_stage expected = stage;
        if (!command_thd->preserve_trx_phase2_command_stage
                 .compare_exchange_strong(
                     expected,
                     Preserve_trx_phase2_command_stage::ADMISSION_INFLIGHT,
                     std::memory_order_acq_rel)) {
          continue;
        }
        return Gate_action::RETRY_NATIVE;
      }
      if (stage == Preserve_trx_phase2_command_stage::EXECUTING) {
        auto command = attempt->commands.find(request.command);
        if (command != attempt->commands.end()) {
          command->second.entered_body = true;
        }
        return Gate_action::ENTER_BODY;
      }
      if (stage == Preserve_trx_phase2_command_stage::CUTOFF) {
        ++attempt->returned_4020;
        return Gate_action::CUTOFF_4020;
      }
      if (stage == Preserve_trx_phase2_command_stage::NATIVE_PRE_BODY_EXIT) {
        return Gate_action::NATIVE_PRE_BODY_EXIT;
      }
      if (stage == Preserve_trx_phase2_command_stage::IDLE) {
        return Gate_action::RETRY_NATIVE;
      }
      stage = transition_revocable_stage(
          command_thd,
          Preserve_trx_phase2_command_stage::WAIT_NATIVE_RESTORE);
      if (stage == Preserve_trx_phase2_command_stage::EXECUTING) continue;
      if (stage == Preserve_trx_phase2_command_stage::CUTOFF) {
        ++attempt->returned_4020;
        return Gate_action::CUTOFF_4020;
      }
      if (stage == Preserve_trx_phase2_command_stage::NATIVE_PRE_BODY_EXIT) {
        return Gate_action::NATIVE_PRE_BODY_EXIT;
      }
      continue;
    }

    auto command = attempt->commands.find(request.command);
    auto connection = attempt->connections.find(
        request.command.connection_incarnation);
    if (command == attempt->commands.end() ||
        connection == attempt->connections.end()) {
      mark_safety_abort_locked(attempt.get());
      continue;
    }

    Attempt::Command_record &command_record = command->second;
    Attempt::Connection_record &connection_record = connection->second;
    command_record.command_class = request.command_class;
    if (connection_record.has_old_transaction) {
      command_record.old_transaction = connection_record.old_transaction;
      command_record.has_old_transaction = true;
    }

    const bool transaction_cohort =
        command_record.t0_member || connection_record.has_old_transaction;
    if (!transaction_cohort) {
      stage = transition_revocable_stage(
          command_thd, Preserve_trx_phase2_command_stage::CUTOFF);
      attempt->changed_locked();
      if (stage == Preserve_trx_phase2_command_stage::EXECUTING) {
        return Gate_action::ENTER_BODY;
      }
      ++attempt->returned_4020;
      return Gate_action::CUTOFF_4020;
    }

    bool permit = false;
    bool terminalizing = false;
    Attempt::Transaction_record *transaction = nullptr;
    if (request.command_class != Command_class::DEFAULT_DENY &&
        connection_record.has_old_transaction) {
      transaction = validate_old_transaction_at_gate_locked(
          attempt.get(), &connection_record,
          request.transaction_observation);
      if (attempt->terminal == Terminal_result::SAFETY_ABORT) {
        attempt->changed_locked();
        continue;
      }
      if (transaction != nullptr) {
        command_record.old_transaction = transaction->key;
        command_record.has_old_transaction = true;
        if (request.command_class == Command_class::TX_END &&
            request.effective_no_chain) {
          permit = true;
          terminalizing = true;
        } else if (request.command_class == Command_class::TX_PROGRESS ||
                   request.command_class == Command_class::TX_END_BY_DDL) {
          const uint64_t support_now_us = scheduler_monotonic_us();
          if (expire_support_locked(attempt.get(), support_now_us)) {
            attempt->changed_locked();
          }
          permit = has_fresh_support_locked(
              attempt.get(), transaction->key, support_now_us);

          const bool proof_period_elapsed =
              command_record.last_mdl_proof_us == 0 ||
              support_now_us >= command_record.last_mdl_proof_us +
                                    kMdlOwnerProofPeriodUs;
          const bool mdl_proof_due =
              !permit && !attempt->mdl_demands.empty() &&
              (command_record.last_mdl_proof_revision !=
                   attempt->mdl_demand_revision ||
               proof_period_elapsed);
          if (mdl_proof_due) {
            bool support_registered = false;
            const bool proof_complete = prove_mdl_support_at_owner_gate_locked(
                attempt.get(), command_thd, &command_record,
                connection_record, *transaction, support_now_us,
                &support_registered);
            lock.unlock();
            if (support_registered) {
              DEBUG_SYNC(command_thd,
                         "phase2_sched_after_mdl_support_registered");
            }
            if (!proof_complete) continue;
            continue;
          }
          terminalizing =
              permit &&
              request.command_class == Command_class::TX_END_BY_DDL;
        }
      }
    }

    if (permit &&
        stage != Preserve_trx_phase2_command_stage::PERMIT_RESERVED) {
      Preserve_trx_phase2_command_stage expected = stage;
      if (!command_thd->preserve_trx_phase2_command_stage
               .compare_exchange_strong(
                   expected,
                   Preserve_trx_phase2_command_stage::PERMIT_RESERVED,
                   std::memory_order_acq_rel)) {
        continue;
      }
      stage = Preserve_trx_phase2_command_stage::PERMIT_RESERVED;
      ++attempt->permit_issued;
      command_record.terminalizing = terminalizing;
      if (terminalizing) {
        Attempt::Transaction_record *transaction = find_transaction_locked(
            attempt.get(), command_record.old_transaction);
        if (transaction == nullptr) {
          mark_safety_abort_locked(attempt.get());
          continue;
        }
        transaction->state = Attempt::Transaction_state::TERMINALIZING;
      }
    }

    if (stage == Preserve_trx_phase2_command_stage::PERMIT_RESERVED) {
      const bool support_dependent =
          command_record.command_class == Command_class::TX_PROGRESS ||
          command_record.command_class == Command_class::TX_END_BY_DDL;
      if (support_dependent) {
        const uint64_t revalidate_us = scheduler_monotonic_us();
        if (expire_support_locked(attempt.get(), revalidate_us)) {
          attempt->changed_locked();
        }
        if (transaction == nullptr ||
            !has_fresh_support_locked(attempt.get(), transaction->key,
                                      revalidate_us)) {
          Preserve_trx_phase2_command_stage expected = stage;
          if (command_thd->preserve_trx_phase2_command_stage
                  .compare_exchange_strong(
                      expected, Preserve_trx_phase2_command_stage::HELD,
                      std::memory_order_acq_rel)) {
            stage = Preserve_trx_phase2_command_stage::HELD;
            command_record.terminalizing = false;
            if (transaction != nullptr &&
                transaction->state ==
                    Attempt::Transaction_state::TERMINALIZING) {
              transaction->state = Attempt::Transaction_state::ACTIVE;
            }
            attempt->changed_locked();
          }
          continue;
        }
      }
      Preserve_trx_phase2_command_stage expected = stage;
      if (!command_thd->preserve_trx_phase2_command_stage
               .compare_exchange_strong(
                   expected, Preserve_trx_phase2_command_stage::EXECUTING,
                   std::memory_order_acq_rel)) {
        continue;
      }
      command_record.entered_body = true;
      attempt->stable_boundary_hints.erase(
          request.command.connection_incarnation);
      if (!note_eligible_body_locked(attempt.get(), request.command,
                                     command_thd->thread_id())) {
        mark_safety_abort_locked(attempt.get());
        continue;
      }
      attempt->changed_locked();
      lock.unlock();
      DEBUG_SYNC(command_thd, "phase2_sched_after_execution_entered");
      return Gate_action::ENTER_BODY;
    }

    if (stage == Preserve_trx_phase2_command_stage::ADMISSION_INFLIGHT ||
        stage == Preserve_trx_phase2_command_stage::T0_CLAIMED_PRE_GATE) {
      Preserve_trx_phase2_command_stage expected = stage;
      if (!command_thd->preserve_trx_phase2_command_stage
               .compare_exchange_strong(
                   expected, Preserve_trx_phase2_command_stage::HELD,
                   std::memory_order_acq_rel)) {
        continue;
      }
      stage = Preserve_trx_phase2_command_stage::HELD;
      attempt->changed_locked();
      lock.unlock();
      DEBUG_SYNC(command_thd, "phase2_sched_after_command_held");
      lock.lock();
    }

    if (stage != Preserve_trx_phase2_command_stage::HELD &&
        stage != Preserve_trx_phase2_command_stage::PERMIT_RESERVED) {
      if (stage == Preserve_trx_phase2_command_stage::CUTOFF) {
        ++attempt->returned_4020;
        return Gate_action::CUTOFF_4020;
      }
      continue;
    }

    /*
      This is only the bounded native-interrupt poll for a HELD command.  Do
      not register every held connection on a timed condition wait: at full
      pressure that creates millions of platform timed-wait calls while the
      owner already revalidates support and HARD under this mutex.  Sleeping
      outside the mutex preserves the same 5 ms bound and lets the next loop
      observe every revision and terminal transition.
    */
    lock.unlock();
    my_sleep(5000);
    if (command_thd->killed) {
      Preserve_trx_phase2_command_stage expected =
          command_thd->preserve_trx_phase2_command_stage.load(
              std::memory_order_acquire);
      while ((expected == Preserve_trx_phase2_command_stage::HELD ||
              expected ==
                  Preserve_trx_phase2_command_stage::PERMIT_RESERVED) &&
             !command_thd->preserve_trx_phase2_command_stage
                  .compare_exchange_weak(
                      expected,
                      Preserve_trx_phase2_command_stage::NATIVE_PRE_BODY_EXIT,
                      std::memory_order_acq_rel)) {
      }
      return Gate_action::NATIVE_PRE_BODY_EXIT;
    }
  }
}

Finish_result finish_command(THD *command_thd,
                             const Command_exit_fact &fact) {
  if (command_thd == nullptr || !command_matches_thd(command_thd, fact.command)) {
    return Finish_result::NATIVE_RESULT;
  }
  Finish_result finish_result = Finish_result::NATIVE_RESULT;
  const Preserve_trx_phase2_command_stage observed_stage =
      command_thd->preserve_trx_phase2_command_stage.load(
          std::memory_order_acquire);
  if (!fact.entered_body) {
    Preserve_trx_phase2_command_stage stage = observed_stage;
    while (revocable_pre_body_stage(stage) &&
           !command_thd->preserve_trx_phase2_command_stage
                .compare_exchange_weak(
                    stage,
                    Preserve_trx_phase2_command_stage::NATIVE_PRE_BODY_EXIT,
                    std::memory_order_acq_rel)) {
    }
    if (stage == Preserve_trx_phase2_command_stage::CUTOFF) {
      finish_result = Finish_result::CUTOFF_4020;
    }
    DEBUG_SYNC(command_thd,
               "phase2_sched_after_native_exit_cas_before_retire");
  }

  Attempt_handle attempt = callback_attempt_snapshot();
  if (attempt != nullptr && fact.entered_body) {
    DEBUG_SYNC(command_thd, "phase2_sched_before_execution_retire");
  }
  if (attempt != nullptr) {
    bool may_retire_callback = false;
    {
      std::lock_guard<std::mutex> lock(attempt->mutex);
      if (fact.entered_body &&
          observed_stage == Preserve_trx_phase2_command_stage::CUTOFF) {
        ++attempt->execution_returned_4020_conflict;
      }
      if (fact.entered_body &&
          !note_eligible_body_exit_locked(attempt.get(), fact)) {
        mark_safety_abort_locked(attempt.get());
      }
      auto command = attempt->commands.find(fact.command);
      if (!attempt->t0_registration_complete) {
        attempt->retired_during_t0.insert(fact.command);
        attempt->pending_exit_facts[fact.command] = fact;
      } else if (command != attempt->commands.end()) {
        auto connection = attempt->connections.find(
            fact.command.connection_incarnation);
        apply_command_exit_locked(
            attempt.get(),
            connection == attempt->connections.end() ? nullptr
                                                      : &connection->second,
            command->second, fact);
      }
      erase_waiter_support_locked(attempt.get(), fact.command,
                                  Lock_domain::INNODB);
      erase_mdl_demand_locked(attempt.get(), fact.command);
      attempt->commands.erase(fact.command);
      command_thd->preserve_trx_phase2_command_stage.store(
          Preserve_trx_phase2_command_stage::IDLE,
          std::memory_order_release);
      may_retire_callback = attempt->commands.empty();
      attempt->changed_locked();
    }
    if (may_retire_callback) retire_callback_if_drained(attempt);
    if (!fact.entered_body) {
      DEBUG_SYNC(command_thd, "phase2_sched_after_native_pre_body_exit");
    }
    return finish_result;
  }

  command_thd->preserve_trx_phase2_command_stage.store(
      Preserve_trx_phase2_command_stage::IDLE, std::memory_order_release);
  return finish_result;
}

void take_stable_boundary_hints(
    const Attempt_handle &attempt,
    std::vector<Stable_boundary_hint> *hints) {
  if (hints == nullptr) return;
  hints->clear();
  if (attempt == nullptr) return;
  std::lock_guard<std::mutex> lock(attempt->mutex);
  if (attempt->terminal != Terminal_result::RUNNING ||
      !attempt->t0_registration_complete) {
    return;
  }
  hints->reserve(std::min(kStableBoundaryHintBatchLimit,
                          attempt->stable_boundary_hints.size()));
  for (auto entry = attempt->stable_boundary_hints.begin();
       entry != attempt->stable_boundary_hints.end() &&
       hints->size() < kStableBoundaryHintBatchLimit;) {
    const auto connection = attempt->connections.find(entry->first);
    if (connection != attempt->connections.end() &&
        connection->second.thd != nullptr) {
      const Preserve_trx_phase2_command_stage stage =
          connection->second.thd->preserve_trx_phase2_command_stage.load(
              std::memory_order_acquire);
      if (stage == Preserve_trx_phase2_command_stage::IDLE ||
          stage == Preserve_trx_phase2_command_stage::HELD) {
        hints->push_back(entry->second);
      }
    }
    entry = attempt->stable_boundary_hints.erase(entry);
  }
}

void note_teardown_begin(uint64_t connection_incarnation) {
  Attempt_handle attempt = callback_attempt_snapshot();
  if (attempt == nullptr || connection_incarnation == 0) return;
  Preserve_trx_external_thd_pin_handle pin_to_release;
  bool may_retire_callback = false;
  {
    std::lock_guard<std::mutex> lock(attempt->mutex);
    auto connection = attempt->connections.find(connection_incarnation);
    if (connection == attempt->connections.end()) return;
    ++attempt->teardown_started;
    attempt->stable_boundary_hints.erase(connection_incarnation);
    connection->second.thd = nullptr;
    for (auto command = attempt->commands.begin();
         command != attempt->commands.end();) {
      if (command->first.connection_incarnation != connection_incarnation) {
        ++command;
        continue;
      }
      if (!attempt->t0_registration_complete) {
        attempt->retired_during_t0.insert(command->first);
      }
      erase_waiter_support_locked(attempt.get(), command->first,
                                  Lock_domain::INNODB);
      erase_mdl_demand_locked(attempt.get(), command->first);
      command = attempt->commands.erase(command);
    }
    if (connection->second.has_old_transaction) {
      erase_transaction_support_locked(
          attempt.get(), connection->second.old_transaction);
    }
    if (connection->second.probe_inflight == 0 && connection->second.pin) {
      pin_to_release = std::move(connection->second.pin);
    } else if (connection->second.pin) {
      connection->second.pin_release_pending = true;
    }
    may_retire_callback = attempt->commands.empty();
    attempt->changed_locked();
  }
  pin_to_release = {};
  DEBUG_SYNC(current_thd, "phase2_sched_after_teardown_begin");
  if (may_retire_callback) retire_callback_if_drained(attempt);
}

void note_transaction_cleanup(uint64_t connection_incarnation,
                              const Transaction_key &transaction_key) {
  Attempt_handle attempt = callback_attempt_snapshot();
  if (attempt == nullptr || connection_incarnation == 0) return;
  std::lock_guard<std::mutex> lock(attempt->mutex);
  auto connection = attempt->connections.find(connection_incarnation);
  if (connection == attempt->connections.end() ||
      !connection->second.has_old_transaction) {
    return;
  }
  if (transaction_key.connection_incarnation != 0 &&
      (transaction_key.connection_incarnation != connection_incarnation ||
       transaction_key.ordinal != connection->second.old_transaction.ordinal)) {
    mark_safety_abort_locked(attempt.get());
    attempt->changed_locked();
    return;
  }
  Attempt::Transaction_record *transaction = find_transaction_locked(
      attempt.get(), connection->second.old_transaction);
  if (transaction == nullptr) {
    mark_safety_abort_locked(attempt.get());
  } else {
    close_transaction_locked(attempt.get(), &connection->second, transaction);
  }
  attempt->changed_locked();
}

Terminal_result tick(const Attempt_handle &attempt, uint64_t now_us,
                     uint64_t tick_stop_us,
                     bool stop_is_progress_deadline) {
  if (attempt == nullptr) return Terminal_result::SAFETY_ABORT;

  struct Tick_budget_observer {
    Attempt *attempt{nullptr};
    uint64_t started_us{0};
    uint64_t stop_us{0};
    bool progress_deadline{false};

    ~Tick_budget_observer() {
      if (attempt == nullptr || stop_us <= started_us ||
          scheduler_monotonic_us() <= stop_us) {
        return;
      }
      attempt->scan_overrun.fetch_add(1, std::memory_order_relaxed);
      if (progress_deadline) {
        attempt->tick_crossed_unserviced_progress_deadline.fetch_add(
            1, std::memory_order_relaxed);
      }
    }
  } tick_budget_observer{attempt.get(), now_us, tick_stop_us,
                         stop_is_progress_deadline};

  struct Probe_work {
    Command_key command;
    THD *thd{nullptr};
    uint64_t expected_waiter_cookie{0};
    uint64_t expected_waiter_version{0};
    bool valid{false};
    bool completes_round{false};
  };

  for (;;) {
    Probe_work work;
    std::vector<Preserve_trx_external_thd_pin_handle> pins_to_release;
    {
      std::unique_lock<std::mutex> lock(attempt->mutex);
      if (attempt->terminal != Terminal_result::RUNNING) {
        return attempt->terminal;
      }
      if (!attempt->t0_registration_complete) {
        return Terminal_result::RUNNING;
      }

      if (expire_support_locked(attempt.get(), now_us)) {
        attempt->changed_locked();
      }

      bool executing = false;
      bool invariant_failure = false;
      for (auto &entry : attempt->commands) {
        auto connection = attempt->connections.find(
            entry.first.connection_incarnation);
        if (connection == attempt->connections.end() ||
            connection->second.thd == nullptr) {
          invariant_failure = true;
          continue;
        }
        THD *candidate = connection->second.thd;
        const Preserve_trx_phase2_command_stage stage =
            candidate->preserve_trx_phase2_command_stage.load(
                std::memory_order_acquire);
        if (stage == Preserve_trx_phase2_command_stage::EXECUTING) {
          entry.second.entered_body = true;
          if (!note_eligible_body_locked(attempt.get(), entry.first,
                                         candidate->thread_id())) {
            invariant_failure = true;
          }
          executing = true;
        }
      }

      if (invariant_failure) {
        mark_safety_abort_locked(attempt.get());
        return attempt->terminal;
      }

      const bool deadline = attempt->config.absolute_deadline_us != 0 &&
                            now_us >= attempt->config.absolute_deadline_us;
      if (!executing || deadline) {
        if (!deadline &&
            !eligible_body_coverage_complete_locked(attempt.get())) {
          mark_safety_abort_locked(attempt.get());
          return attempt->terminal;
        }
        const Terminal_result result =
            deadline ? Terminal_result::HARD_DEADLINE
                     : Terminal_result::HARD_QUIESCENT;
        bool body_won_cutoff = false;
        for (auto &entry : attempt->commands) {
          auto connection = attempt->connections.find(
              entry.first.connection_incarnation);
          if (connection == attempt->connections.end() ||
              connection->second.thd == nullptr) {
            continue;
          }
          THD *candidate = connection->second.thd;
          const Preserve_trx_phase2_command_stage stage =
              transition_revocable_stage(
                  candidate, Preserve_trx_phase2_command_stage::CUTOFF);
          if (stage == Preserve_trx_phase2_command_stage::EXECUTING) {
            entry.second.entered_body = true;
            if (!note_eligible_body_locked(attempt.get(), entry.first,
                                           candidate->thread_id())) {
              mark_safety_abort_locked(attempt.get());
              return attempt->terminal;
            }
            body_won_cutoff = true;
          }
        }
        if (body_won_cutoff && !deadline) {
          return Terminal_result::RUNNING;
        }
        attempt->allow_new_probe_borrows = false;
        attempt->terminal = result;
        attempt->terminal_cause = deadline ? 1 : 0;
        attempt->stable_boundary_hints.clear();
        clear_support_locked(attempt.get());
        attempt->mdl_demands.clear();
        attempt->mdl_demand_index.clear();
        for (auto &entry : attempt->connections) {
          if (entry.second.probe_inflight == 0 && entry.second.pin) {
            pins_to_release.push_back(std::move(entry.second.pin));
            entry.second.thd = nullptr;
          } else if (entry.second.pin) {
            entry.second.pin_release_pending = true;
          }
        }
        freeze_terminal_snapshot_locked(attempt.get(),
                                        scheduler_monotonic_us());
        /*
          CUTOFF is final now, but the existing CLOSING gate first takes its
          authoritative target snapshot.  Releasing 4020 here would let a
          client disconnect erase that connection before the native handoff.
        */
        ++attempt->revision;
        lock.unlock();
        DEBUG_SYNC(current_thd, "phase2_sched_after_hard_published");
        pins_to_release.clear();
        return result;
      }

      const uint64_t current_us = scheduler_monotonic_us();
      if (current_us >= tick_stop_us ||
          current_us > tick_stop_us -
                           std::min(tick_stop_us, kProbeStartReserveUs) ||
          attempt->next_scan_due_us > current_us) {
        return Terminal_result::RUNNING;
      }

      if (attempt->scan_candidates.empty() && !attempt->scan_round_open) {
        attempt->scan_round_open = true;
        attempt->scan_cursor = 0;
        ++attempt->scan_count;
        for (const auto &entry : attempt->commands) {
          const auto connection = attempt->connections.find(
              entry.first.connection_incarnation);
          if (connection == attempt->connections.end() ||
              connection->second.thd == nullptr) {
            continue;
          }
          if (connection->second.thd->preserve_trx_phase2_command_stage.load(
                  std::memory_order_acquire) ==
              Preserve_trx_phase2_command_stage::EXECUTING) {
            attempt->scan_candidates.push_back(entry.first);
          }
        }
        attempt->scan_candidate += attempt->scan_candidates.size();
      }

      while (attempt->scan_cursor < attempt->scan_candidates.size()) {
        const Command_key candidate_key =
            attempt->scan_candidates[attempt->scan_cursor++];
        auto command = attempt->commands.find(candidate_key);
        auto connection = attempt->connections.find(
            candidate_key.connection_incarnation);
        if (command == attempt->commands.end() ||
            connection == attempt->connections.end() ||
            connection->second.thd == nullptr ||
            !connection->second.pin || !attempt->allow_new_probe_borrows ||
            connection->second.thd->preserve_trx_phase2_command_stage.load(
                std::memory_order_acquire) !=
                Preserve_trx_phase2_command_stage::EXECUTING) {
          continue;
        }
        work.command = candidate_key;
        work.thd = connection->second.thd;
        if (connection->second.has_old_transaction) {
          Attempt::Transaction_record *transaction = find_transaction_locked(
              attempt.get(), connection->second.old_transaction);
          if (transaction != nullptr &&
              transaction->raw_engine_cookie != 0) {
            work.expected_waiter_cookie = transaction->raw_engine_cookie;
            if (transaction->identity_sealed) {
              work.expected_waiter_version = transaction->engine_version;
            }
          }
        }
        ++connection->second.probe_inflight;
        work.valid = true;
        work.completes_round =
            attempt->scan_cursor == attempt->scan_candidates.size();
        break;
      }
      if (!work.valid) {
        attempt->scan_candidates.clear();
        attempt->scan_round_open = false;
        attempt->scan_cursor = 0;
        attempt->next_scan_due_us =
            current_us > std::numeric_limits<uint64_t>::max() - kScanPeriodUs
                ? std::numeric_limits<uint64_t>::max()
                : current_us + kScanPeriodUs;
        return Terminal_result::RUNNING;
      }
    }

    DEBUG_SYNC(current_thd,
               "phase2_sched_after_probe_borrow_before_thd_snapshot");
    bool probe_input_stale = false;
    uint64_t raw_waiter_cookie = work.expected_waiter_cookie;
    uint64_t waiter_version = work.expected_waiter_version;
    mysql_mutex_lock(&work.thd->LOCK_thd_data);
    const bool command_current =
        work.thd->preserve_trx_phase2_connection_incarnation ==
            work.command.connection_incarnation &&
        work.thd->preserve_trx_phase2_aggregate_sequence ==
            work.command.sequence &&
        work.thd->preserve_trx_phase2_command_stage.load(
            std::memory_order_acquire) ==
            Preserve_trx_phase2_command_stage::EXECUTING;
    if (command_current && raw_waiter_cookie == 0) {
      raw_waiter_cookie =
          trx_preserve_phase2_peek_raw_cookie(work.thd);
    }
    mysql_mutex_unlock(&work.thd->LOCK_thd_data);
    probe_input_stale = !command_current;

    std::array<lock_preserve_phase2_blocker,
               LOCK_PRESERVE_PHASE2_MAX_QUEUE_PREDECESSORS>
        native_blockers{};
    MDL_phase2_wait_snapshot mdl_snapshot;
    MDL_phase2_wait_probe_status mdl_probe_status =
        MDL_phase2_wait_probe_status::NOT_WAITING;
    const uint64_t mdl_sampled_at_us = scheduler_monotonic_us();
    if (!probe_input_stale) {
      mdl_probe_status =
          work.thd->mdl_context.try_snapshot_phase2_wait(&mdl_snapshot);
    }

    lock_preserve_phase2_wait_snapshot native_snapshot;
    lock_preserve_phase2_probe_status probe_status =
        lock_preserve_phase2_probe_status::NOT_WAITING;
    const uint64_t innodb_sampled_at_us = scheduler_monotonic_us();
    const bool innodb_budget_available =
        innodb_sampled_at_us < tick_stop_us &&
        innodb_sampled_at_us <=
            tick_stop_us - std::min(tick_stop_us, kProbeStartReserveUs);
    if (!probe_input_stale && raw_waiter_cookie != 0 &&
        innodb_budget_available) {
      probe_status = lock_preserve_phase2_probe_wait(
          raw_waiter_cookie, waiter_version,
          static_cast<uint64_t>(reinterpret_cast<uintptr_t>(work.thd)),
          native_blockers.data(), native_blockers.size(), &native_snapshot);
    } else if (!probe_input_stale && raw_waiter_cookie != 0) {
      probe_status =
          lock_preserve_phase2_probe_status::RETRYABLE_LOCK_SYS_BUSY;
    }
    DBUG_EXECUTE_IF("phase2_sched_force_innodb_unknown", {
      if (!probe_input_stale && raw_waiter_cookie != 0) {
        probe_status =
            lock_preserve_phase2_probe_status::UNKNOWN_INCOMPLETE;
      }
    });
    if (!probe_input_stale &&
        probe_status == lock_preserve_phase2_probe_status::COMPLETE) {
      DEBUG_SYNC(current_thd,
                 "phase2_sched_after_innodb_probe_before_merge");
    }

    bool support_registered = false;
    bool mdl_demand_registered = false;
    bool mdl_shared_write_pending_priority_unsupported = false;
    bool retiring_blocker_stale_discarded = false;
    Preserve_trx_external_thd_pin_handle pin_to_release;
    {
      std::lock_guard<std::mutex> lock(attempt->mutex);
      auto connection = attempt->connections.find(
          work.command.connection_incarnation);
      auto command = attempt->commands.find(work.command);
      bool merge_current =
          !probe_input_stale && attempt->terminal == Terminal_result::RUNNING &&
          connection != attempt->connections.end() &&
          command != attempt->commands.end() &&
          connection->second.thd == work.thd &&
          work.thd->preserve_trx_phase2_command_stage.load(
              std::memory_order_acquire) ==
              Preserve_trx_phase2_command_stage::EXECUTING;

      if (!merge_current) ++attempt->scan_stale_discarded;

      if (merge_current) {
        switch (mdl_probe_status) {
          case MDL_phase2_wait_probe_status::NOT_WAITING:
          case MDL_phase2_wait_probe_status::NON_MDL_WAIT:
            ++attempt->scan_negative_result;
            if (erase_mdl_demand_locked(attempt.get(), work.command)) {
              attempt->changed_locked();
            }
            break;
          case MDL_phase2_wait_probe_status::UNSUPPORTED_DURATION:
            ++attempt->scan_unsupported_result;
            if (erase_mdl_demand_locked(attempt.get(), work.command)) {
              attempt->changed_locked();
            }
            break;
          case MDL_phase2_wait_probe_status::
              UNSUPPORTED_PENDING_PRIORITY_BLOCKER:
            ++attempt->scan_unsupported_result;
            mdl_shared_write_pending_priority_unsupported =
                mdl_snapshot.request_type == MDL_SHARED_WRITE;
            if (erase_mdl_demand_locked(attempt.get(), work.command)) {
              attempt->changed_locked();
            }
            break;
          case MDL_phase2_wait_probe_status::RETRYABLE_BUSY:
          case MDL_phase2_wait_probe_status::STALE:
            ++attempt->scan_stale_discarded;
            break;
          case MDL_phase2_wait_probe_status::UNKNOWN_INCOMPLETE:
            ++attempt->scan_unknown_result;
            ++attempt->lock_proof_unknown;
            mark_safety_abort_locked(attempt.get());
            attempt->changed_locked();
            break;
          case MDL_phase2_wait_probe_status::COMPLETE_TRANSACTION: {
            ++attempt->scan_positive_result;
            const uint64_t merge_now_us = scheduler_monotonic_us();
            const uint64_t expires_at_us = bounded_lease_expiry(
                mdl_sampled_at_us, attempt->config.absolute_deadline_us);
            if (merge_now_us >= expires_at_us ||
                !upsert_mdl_demand_locked(
                    attempt.get(), work.command, mdl_snapshot, expires_at_us,
                    &mdl_demand_registered)) {
              if (merge_now_us < expires_at_us) {
                mark_safety_abort_locked(attempt.get());
                attempt->changed_locked();
              }
              break;
            }
            if (mdl_demand_registered) attempt->changed_locked();
            break;
          }
        }

        if (attempt->terminal != Terminal_result::RUNNING) {
          merge_current = false;
        }
      }

      if (merge_current) {
        switch (probe_status) {
          case lock_preserve_phase2_probe_status::NOT_WAITING:
            ++attempt->scan_negative_result;
            if (erase_waiter_support_locked(
                    attempt.get(), work.command, Lock_domain::INNODB)) {
              attempt->changed_locked();
            }
            break;
          case lock_preserve_phase2_probe_status::
              UNSUPPORTED_PENDING_PREDECESSOR:
          case lock_preserve_phase2_probe_status::UNSUPPORTED_RELEASE_CLASS:
            ++attempt->scan_unsupported_result;
            if (erase_waiter_support_locked(
                    attempt.get(), work.command, Lock_domain::INNODB)) {
              attempt->changed_locked();
            }
            break;
          case lock_preserve_phase2_probe_status::
              RETRYABLE_LOCK_SYS_BUSY:
          case lock_preserve_phase2_probe_status::
              RETRYABLE_STALE_IDENTITY:
            ++attempt->scan_stale_discarded;
            break;
          case lock_preserve_phase2_probe_status::UNKNOWN_INCOMPLETE:
          case lock_preserve_phase2_probe_status::UNKNOWN_IDENTITY:
            ++attempt->scan_unknown_result;
            ++attempt->lock_proof_unknown;
            mark_safety_abort_locked(attempt.get());
            attempt->changed_locked();
            break;
          case lock_preserve_phase2_probe_status::COMPLETE: {
            ++attempt->scan_positive_result;
            const uint64_t merge_now_us = scheduler_monotonic_us();
            const uint64_t expires_at_us = bounded_lease_expiry(
                innodb_sampled_at_us, attempt->config.absolute_deadline_us);
            if (merge_now_us >= expires_at_us) break;
            if (native_snapshot.waiter.raw_cookie != raw_waiter_cookie ||
                (waiter_version != 0 &&
                 native_snapshot.waiter.version != waiter_version) ||
                native_snapshot.waiter.owner_thd_cookie !=
                    static_cast<uint64_t>(
                        reinterpret_cast<uintptr_t>(work.thd)) ||
                native_snapshot.blocker_count == 0 ||
                native_snapshot.blocker_count > native_blockers.size()) {
              mark_safety_abort_locked(attempt.get());
              attempt->changed_locked();
              break;
            }

            Attempt::Transaction_record *waiter_transaction = nullptr;
            if (connection->second.has_old_transaction) {
              waiter_transaction = find_transaction_locked(
                  attempt.get(), connection->second.old_transaction);
            } else if (command->second.t0_member &&
                       command->second.entered_body) {
              waiter_transaction = reserve_t0_transaction_locked(
                  attempt.get(), &connection->second,
                  work.command.connection_incarnation, raw_waiter_cookie,
                  connection->second.isolation_level);
            }
            Transaction_observation waiter_observation;
            waiter_observation.sql_transaction_active = true;
            waiter_observation.engine_state =
                Engine_identity_state::EXACT_ACTIVE;
            waiter_observation.raw_engine_cookie =
                native_snapshot.waiter.raw_cookie;
            waiter_observation.engine_version =
                native_snapshot.waiter.version;
            waiter_observation.isolation_level =
                waiter_transaction == nullptr
                    ? connection->second.isolation_level
                    : waiter_transaction->isolation_level;
            if (waiter_transaction == nullptr ||
                !seal_transaction_from_observation_locked(
                    attempt.get(), waiter_transaction, waiter_observation)) {
              mark_safety_abort_locked(attempt.get());
              attempt->changed_locked();
              break;
            }
            command->second.old_transaction = waiter_transaction->key;
            command->second.has_old_transaction = true;

            std::array<Transaction_key,
                       LOCK_PRESERVE_PHASE2_MAX_QUEUE_PREDECESSORS>
                mapped_blockers{};
            size_t mapped_count = 0;
            bool mapping_complete = true;
            bool mapping_stale = false;
            bool release_class_supported = true;
            for (size_t i = 0; i < native_snapshot.blocker_count; ++i) {
              bool known_retiring_owner = false;
              Attempt::Transaction_record *blocker =
                  find_native_transaction_locked(
                      attempt.get(), native_blockers[i].identity,
                      &known_retiring_owner);
              if (known_retiring_owner) {
                mapping_stale = true;
                break;
              }
              if (blocker == nullptr ||
                  same_transaction_key(blocker->key,
                                       waiter_transaction->key)) {
                mapping_complete = false;
                break;
              }
              if (native_blockers[i].release_class ==
                      lock_preserve_phase2_release_class::
                          RECORD_RR_OR_STRONGER &&
                  blocker->isolation_level !=
                      static_cast<uint32_t>(ISO_REPEATABLE_READ) &&
                  blocker->isolation_level !=
                      static_cast<uint32_t>(ISO_SERIALIZABLE)) {
                release_class_supported = false;
              }
              bool duplicate = false;
              for (size_t j = 0; j < mapped_count; ++j) {
                duplicate = duplicate ||
                            same_transaction_key(mapped_blockers[j],
                                                 blocker->key);
              }
              if (!duplicate) mapped_blockers[mapped_count++] = blocker->key;
            }
            if (mapping_stale) {
              ++attempt->scan_stale_discarded;
              retiring_blocker_stale_discarded = true;
              break;
            }
            if (!mapping_complete || mapped_count == 0) {
              mark_safety_abort_locked(attempt.get());
              attempt->changed_locked();
              break;
            }
            if (!release_class_supported) {
              if (erase_waiter_support_locked(
                      attempt.get(), work.command, Lock_domain::INNODB)) {
                attempt->changed_locked();
              }
              break;
            }

            size_t old_count = 0;
            bool edge_set_changed = false;
            const auto indexed =
                attempt->support_by_waiter.find(work.command);
            if (indexed != attempt->support_by_waiter.end()) {
              for (const Support_edge_key &key : indexed->second) {
                if (key.domain != Lock_domain::INNODB) continue;
                ++old_count;
                bool found = false;
                for (size_t i = 0; i < mapped_count; ++i) {
                  found = found || same_transaction_key(
                                       key.blocker, mapped_blockers[i]);
                }
                edge_set_changed = edge_set_changed || !found;
              }
            }
            edge_set_changed = edge_set_changed || old_count != mapped_count;
            support_registered = old_count == 0 && mapped_count != 0;
            erase_waiter_support_locked(attempt.get(), work.command,
                                        Lock_domain::INNODB);
            for (size_t i = 0; i < mapped_count; ++i) {
              Support_edge_key edge_key;
              edge_key.waiter = work.command;
              edge_key.blocker = mapped_blockers[i];
              edge_key.domain = Lock_domain::INNODB;
              (void)upsert_support_edge_locked(attempt.get(), edge_key,
                                               expires_at_us);
            }
            if (edge_set_changed) attempt->changed_locked();
            break;
          }
        }
      }

      if (connection != attempt->connections.end()) {
        DBUG_ASSERT(connection->second.probe_inflight > 0);
        if (connection->second.probe_inflight > 0) {
          --connection->second.probe_inflight;
        }
        if (connection->second.probe_inflight == 0 &&
            connection->second.pin_release_pending &&
            connection->second.pin) {
          pin_to_release = std::move(connection->second.pin);
          connection->second.pin_release_pending = false;
          connection->second.thd = nullptr;
        }
      }
      if (work.completes_round) {
        attempt->scan_candidates.clear();
        attempt->scan_round_open = false;
        attempt->scan_cursor = 0;
        const uint64_t round_done_us = scheduler_monotonic_us();
        attempt->next_scan_due_us =
            round_done_us >
                    std::numeric_limits<uint64_t>::max() - kScanPeriodUs
                ? std::numeric_limits<uint64_t>::max()
                : round_done_us + kScanPeriodUs;
      }
    }
    if (support_registered) {
      DEBUG_SYNC(current_thd,
                 "phase2_sched_after_innodb_support_registered");
    }
    if (mdl_demand_registered) {
      DEBUG_SYNC(current_thd,
                 "phase2_sched_after_mdl_demand_registered");
    }
    if (mdl_shared_write_pending_priority_unsupported) {
      DEBUG_SYNC(
          current_thd,
          "phase2_sched_after_mdl_shared_write_pending_priority_unsupported");
    }
    if (retiring_blocker_stale_discarded) {
      DEBUG_SYNC(current_thd,
                 "phase2_sched_after_retiring_blocker_stale");
    }
    pin_to_release = {};

    now_us = scheduler_monotonic_us();
    if (now_us >= tick_stop_us ||
        now_us > tick_stop_us - std::min(tick_stop_us,
                                         kProbeStartReserveUs)) {
      return Terminal_result::RUNNING;
    }
  }
}

bool reset_is_unsupported() {
  return g_route_active.load(std::memory_order_acquire);
}

bool cutoff_response_handoff_pending() {
  return g_route_active.load(std::memory_order_acquire);
}

void wait_for_cutoff_response_handoff() {
  Attempt_handle attempt = active_route_snapshot();
  if (attempt == nullptr) return;
  std::unique_lock<std::mutex> lock(attempt->mutex);
  attempt->condition.wait(lock, [&] {
    return attempt->hard_cutoff_responses_released;
  });
}

bool terminal_snapshot(const Attempt_handle &attempt,
                       Terminal_snapshot *snapshot) {
  if (attempt == nullptr || snapshot == nullptr) return false;
  std::lock_guard<std::mutex> lock(attempt->mutex);
  if (!attempt->terminal_snapshot_frozen) return false;
  *snapshot = attempt->frozen_terminal_snapshot;
  return true;
}

bool summary_snapshot(const Attempt_handle &attempt,
                      Summary_snapshot *snapshot) {
  if (attempt == nullptr || snapshot == nullptr) return false;
  std::lock_guard<std::mutex> lock(attempt->mutex);
  snapshot->attempt_id = attempt->config.attempt_id;
  snapshot->generation = attempt->config.generation;
  snapshot->terminal_result = attempt->terminal;
  snapshot->terminal_cause = attempt->terminal_cause;
  snapshot->t0_scope_registered = attempt->t0_scope_registered;
  snapshot->t0_executing_max = attempt->t0_executing_max;
  snapshot->scan_count = attempt->scan_count;
  snapshot->scan_candidate = attempt->scan_candidate;
  snapshot->scan_positive_result = attempt->scan_positive_result;
  snapshot->scan_negative_result = attempt->scan_negative_result;
  snapshot->scan_unsupported_result = attempt->scan_unsupported_result;
  snapshot->scan_unknown_result = attempt->scan_unknown_result;
  snapshot->scan_stale_discarded = attempt->scan_stale_discarded;
  snapshot->scan_overrun =
      attempt->scan_overrun.load(std::memory_order_relaxed);
  snapshot->support_edge_registered = attempt->support_edge_registered;
  snapshot->permit_issued = attempt->permit_issued;
  snapshot->returned_4020 = attempt->returned_4020;
  snapshot->teardown_started = attempt->teardown_started;
  snapshot->lock_proof_unknown = attempt->lock_proof_unknown;
  snapshot->lineage_unknown =
      attempt->invariant_violation_count > attempt->lock_proof_unknown
          ? attempt->invariant_violation_count - attempt->lock_proof_unknown
          : 0;
  snapshot->tick_crossed_unserviced_progress_deadline =
      attempt->tick_crossed_unserviced_progress_deadline.load(
          std::memory_order_relaxed);
  snapshot->execution_returned_4020_conflict =
      attempt->execution_returned_4020_conflict;
  snapshot->invariant_violation_count =
      attempt->invariant_violation_count;
  return true;
}

void wait_for_change(const Attempt_handle &attempt, uint64_t wait_us) {
  if (attempt == nullptr || wait_us == 0) return;
  std::unique_lock<std::mutex> lock(attempt->mutex);
  const uint64_t observed_revision = attempt->revision;
  attempt->condition.wait_for(lock, std::chrono::microseconds(wait_us), [&] {
    return attempt->revision != observed_revision ||
           attempt->terminal != Terminal_result::RUNNING;
  });
}

void owner_cancel(const Attempt_handle &attempt, uint32_t cause) {
  if (attempt == nullptr) return;
  std::lock_guard<std::mutex> lock(attempt->mutex);
  enter_fail_closed_locked(attempt.get(), Terminal_result::OWNER_CANCELLED,
                           cause);
}

void publish_native_admission_restored_and_retire_route(
    const Attempt_handle &attempt) {
  if (attempt == nullptr) return;
  std::vector<Preserve_trx_external_thd_pin_handle> pins_to_release;
  {
    std::lock_guard<std::mutex> lock(attempt->mutex);
    attempt->native_admission_restored = true;
    attempt->hard_cutoff_responses_released = true;
    for (auto &entry : attempt->connections) {
      if (entry.second.thd != nullptr) {
        (void)transition_revocable_stage(
            entry.second.thd,
            Preserve_trx_phase2_command_stage::WAIT_NATIVE_RESTORE);
      }
      if (entry.second.probe_inflight == 0 && entry.second.pin) {
        pins_to_release.push_back(std::move(entry.second.pin));
        entry.second.thd = nullptr;
      } else if (entry.second.pin) {
        entry.second.pin_release_pending = true;
      }
    }
    attempt->changed_locked();
  }
  {
    std::lock_guard<std::mutex> lock(g_route_mutex);
    if (g_active_route == attempt) {
      g_route_active.store(false, std::memory_order_release);
      g_active_route.reset();
    }
  }
  retire_callback_if_drained(attempt);
  pins_to_release.clear();
}

void release_cutoff_responses_and_retire_route(
    const Attempt_handle &attempt) {
  if (attempt == nullptr) return;
  {
    /* Never nest the attempt mutex with the process-global route mutex. */
    std::lock_guard<std::mutex> attempt_lock(attempt->mutex);
    attempt->hard_cutoff_responses_released = true;
    attempt->changed_locked();
  }
  {
    std::lock_guard<std::mutex> route_lock(g_route_mutex);
    if (g_active_route == attempt) {
      g_route_active.store(false, std::memory_order_release);
      g_active_route.reset();
    }
  }
  retire_callback_if_drained(attempt);
}

}  // namespace preserve_trx_phase2_scheduler
