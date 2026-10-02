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
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/preserve_trx_transfer.h"
#include "sql/preserve_trx_transfer_index.h"
#include "sql/preserve_trx_result_transfer.h"
#include "sql/preserve_trx_temp_transfer.h"
#include "sql/preserve_trx_temp_delta.h"
#include "sql/preserve_trx_temp_metrics.h"
#include "sql/preserve_trx_temp_restore.h"
#ifndef NDEBUG
#include "storage/innobase/include/trx0temp_preserve_import.h"
#endif
#ifndef NDEBUG
#include "sql/preserve_trx_temp_table_carrier.h"
#endif
#include "sql/preserve_trx_result_manifest.h"
#include "sql/preserve_trx_receiver_prepare.h"
#include "sql/preserve_trx_cursor.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <list>
#include <memory>
#include <new>
#include <mutex>
#include <set>
#include <sstream>
#include <system_error>
#include <sys/stat.h>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <utility>
#include <vector>

#include <openssl/evp.h>
#include <openssl/crypto.h>
#include <openssl/sha.h>

#include "my_dir.h"
#include "my_io.h"
#include "my_loglevel.h"
#include "my_rnd.h"
#include "my_sys.h"
#include "my_thread_local.h"
#include "mysql_version.h"
#include "mysqld_error.h"
#include "mysql.h"
#include "violite.h"
#include "mysql/components/services/log_builtins.h"
#include "sql_common.h"
#include "sql/mysqld.h"
#include "sql/log.h"
#include "sql/current_thd.h"
#include "sql/debug_sync.h"
#include "sql/binlog.h"
#include "sql/binlog_preserve_prepared.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_carrier_file.h"
#include "sql/preserve_trx_file.h"
#include "sql/preserve_trx_receiver_candidates.h"
#include "sql/preserve_trx_result_pretransfer.h"
#include "sql/preserve_trx_promotion.h"
#include "sql/preserve_trx_promotion_prepared.h"
#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_temp_gc.h"
#include "sql/preserve_trx_xid.h"
#include "sql/protocol_classic.h"
#include "sql/rpl_channel_credentials.h"
#include "sql/rpl_gtid.h"
#include "sql/sql_class.h"
#include "sql/sql_thd_internal_api.h"
#include "sql/ssl_acceptor_context_status.h"
#include "scope_guard.h"
#include "sql/preserve_trx_receiver_binlog_prewarm.h"
#include "storage/innobase/include/trx0preserve.h"

char *preserve_trx_transfer_target_host = nullptr;
uint preserve_trx_transfer_target_port = 0;
char *preserve_trx_transfer_target_user = nullptr;
char *preserve_trx_transfer_credential_name = nullptr;
char *preserve_trx_transfer_credential_secret_file = nullptr;
ulong preserve_trx_transfer_artifact_mode =
    PRESERVE_TRX_TRANSFER_ARTIFACT_STANDBY_TRANSFER_SAVE;
ulong preserve_trx_transfer_runtime_profile =
    PRESERVE_TRX_TRANSFER_RUNTIME_BUSINESS_FIRST;
bool preserve_trx_transfer_prewarm_paused = false;
ulonglong preserve_trx_transfer_max_inflight_bytes = 1073741824ULL;
const uint preserve_trx_transfer_receiver_prewarm_timeout_ms = 10000;
uint preserve_trx_token_retention_timeout_ms = 30000;

static constexpr char kBinlogPrewarmSeedObjectId[] =
    "binlog_prewarm_seed_v1";
static constexpr uint64_t kReceiverTerminalStatusRetentionUs = 60000000;
static constexpr char kReceiverCommitOperationId[] = "commit_epoch";
static constexpr char kSessionOnlyTokenSetMagic[] = "PTRX_SSET_V1";
/* 12-byte magic + 4-byte count + 8 bytes per ID must fit in 1 MiB. */
static constexpr uint32_t kMaxSessionOnlyTokenCount = 131070;

static std::atomic<uint> g_transfer_staging_cleanup_failures_for_unit_test{0};
static std::atomic<uint64_t> g_transfer_throttled_us{0};
static std::atomic<uint64_t> g_transfer_last_throttle_reason{0};
static std::atomic<uint64_t> g_receiver_queued_bytes{0};
static std::atomic<uint64_t> g_receiver_worker_active{0};
static std::atomic<uint64_t> g_receiver_worker_count{0};
static std::atomic<bool> g_receiver_prewarm_paused{false};

static std::atomic<uint64_t> g_receiver_auto_prewarm_tokens{0};
static std::atomic<uint64_t> g_receiver_auto_prewarm_ready_tokens{0};
static std::atomic<uint64_t> g_receiver_auto_prewarm_not_ready_tokens{0};
static std::atomic<uint64_t> g_receiver_auto_prewarm_last_status{0};
static std::atomic<uint64_t> g_receiver_ready_monotonic_us{0};
static std::atomic<uint64_t>
    g_receiver_final_metadata_accepted_monotonic_us{0};
static std::atomic<uint64_t>
    g_receiver_terminal_commit_admitted_monotonic_us{0};
static std::atomic<uint64_t>
    g_receiver_ready_after_final_metadata_accepted_us{0};
static std::atomic<uint64_t>
    g_receiver_ready_after_terminal_commit_admitted_us{0};
static std::atomic<uint64_t> g_receiver_admitted_frames{0};
static std::atomic<uint64_t> g_receiver_admitted_bytes{0};
static std::atomic<uint64_t> g_source_handoff_pending_epochs{0};
static std::atomic<uint64_t> g_source_commit_unknown_epochs{0};
static std::atomic<uint64_t> g_source_restore_guard_rejects{0};
static std::atomic<uint64_t> g_receiver_terminal_cas_wins{0};
static std::atomic<uint64_t> g_receiver_terminal_cas_duplicates{0};
static std::atomic<uint64_t> g_receiver_terminal_cas_conflicts{0};
static std::atomic<uint64_t> g_receiver_terminal_status_tombstones{0};
static std::atomic<uint64_t> g_receiver_terminal_status_tombstone_expiries{0};
static std::atomic<uint64_t> g_quarantine_epochs{0};
static std::atomic<uint64_t> g_quarantine_tokens{0};
static std::atomic<uint64_t> g_quarantine_bytes{0};
static std::atomic<uint64_t> g_quarantine_oldest_age_us{0};
static std::atomic<uint64_t> g_quarantine_started_monotonic_us{0};
static std::atomic<uint64_t> g_receiver_first_frame_monotonic_us{0};
static std::atomic<uint64_t> g_receiver_last_object_seal_monotonic_us{0};
static std::atomic<uint64_t> g_receiver_prewarm_start_monotonic_us{0};
static std::atomic<uint64_t> g_receiver_prewarm_end_monotonic_us{0};
static std::atomic<uint64_t> g_receiver_seal_prewarm_tokens{0};
static std::atomic<uint64_t> g_receiver_seal_prewarm_success_tokens{0};
static std::atomic<uint64_t> g_receiver_seal_prewarm_not_ready_tokens{0};
static std::atomic<uint64_t> g_receiver_seal_prewarm_last_status{0};
static std::atomic<uint64_t> g_receiver_object_prewarm_proof_count{0};
static std::atomic<uint64_t> g_receiver_object_prewarm_miss_count{0};
static std::atomic<uint64_t> g_receiver_object_prewarm_count{0};
static std::atomic<uint64_t> g_receiver_object_prewarm_us{0};
static std::atomic<uint64_t> g_receiver_object_prewarm_max_us{0};
static std::atomic<uint64_t> g_receiver_object_prewarm_first_start_us{0};
static std::atomic<uint64_t> g_receiver_object_prewarm_last_end_us{0};
static std::atomic<uint64_t> g_receiver_record_object_prewarm_count{0};
static std::atomic<uint64_t> g_receiver_record_object_prewarm_us{0};
static std::atomic<uint64_t> g_receiver_record_object_prewarm_max_us{0};
static std::atomic<uint64_t> g_receiver_record_object_prewarm_first_start_us{0};
static std::atomic<uint64_t> g_receiver_record_object_prewarm_last_end_us{0};
static std::atomic<uint64_t> g_receiver_strict_record_index_page_reads{0};
static std::atomic<uint64_t> g_receiver_strict_ibuf_merges{0};
static std::atomic<uint64_t> g_receiver_strict_target_local_redo_bytes{0};
static std::atomic<uint64_t> g_receiver_binlog_object_prewarm_first_start_us{0};
static std::atomic<uint64_t> g_receiver_binlog_object_prewarm_last_end_us{0};
static std::atomic<uint64_t> g_receiver_committed_epoch_fallback_count{0};
static std::atomic<uint64_t> g_receiver_staged_token_ready_cache_us{0};
static std::atomic<uint64_t> g_receiver_staged_token_total_us{0};
static std::atomic<uint64_t> g_receiver_staged_token_max_us{0};
static std::atomic<uint64_t> g_receiver_staged_token_active{0};
static std::atomic<uint64_t> g_receiver_staged_token_max_active{0};
static std::atomic<uint64_t> g_receiver_projection_publish_count{0};
static std::atomic<uint64_t> g_receiver_projection_publish_us{0};
static std::atomic<uint64_t> g_receiver_projection_publish_max_us{0};
static std::atomic<uint64_t> g_receiver_projection_lock_wait_us{0};
static std::atomic<uint64_t> g_receiver_projection_store_write_us{0};
static std::atomic<uint64_t> g_receiver_projection_marker_write_us{0};
static std::atomic<uint64_t> g_receiver_projection_snapshot_write_us{0};
static std::atomic<uint64_t> g_receiver_projection_external_blob_us{0};
static std::atomic<uint64_t> g_receiver_projection_encode_us{0};
static std::atomic<uint64_t> g_receiver_projection_token_state_us{0};
static std::atomic<uint64_t> g_receiver_epoch_ready_bind_attempts{0};
static std::atomic<uint64_t> g_transfer_phase2_bulk_bytes{0};
static std::atomic<uint64_t> g_transfer_phase2_snapshot_bundle_bytes{0};
static std::atomic<uint64_t> g_transfer_phase2_snapshot_bundle_count{0};
static std::atomic<uint64_t> g_transfer_phase2_final_metadata_frame_count{0};
static std::atomic<uint64_t> g_transfer_phase2_final_metadata_encoded_bytes{0};
static std::atomic<uint64_t> g_transfer_phase2_final_metadata_fsync_count{0};
static std::atomic<uint64_t> g_transfer_phase2_final_metadata_ack_us{0};
static std::atomic<uint64_t> g_transfer_phase1_frame_count{0};
static std::atomic<uint64_t> g_transfer_phase1_network_send_count{0};
static std::atomic<uint64_t> g_transfer_phase1_batch_count{0};
static std::atomic<uint64_t> g_transfer_phase1_oversize_token_count{0};
static std::atomic<uint64_t> g_transfer_phase1_record_first_batch_send_us{0};
static std::atomic<uint64_t> g_transfer_phase1_record_last_batch_send_us{0};
static std::mutex g_transfer_phase1_batch_metrics_mutex;
static std::vector<uint64_t> g_transfer_phase1_batch_bytes_samples;
static std::vector<uint64_t> g_transfer_phase1_batch_tokens_samples;
static std::vector<uint64_t> g_transfer_phase1_record_batch_tokens_samples;
static std::vector<uint64_t> g_transfer_phase1_batch_linger_us_samples;
static std::atomic<uint64_t> g_receiver_final_metadata_saved_us{0};
static std::atomic<uint64_t> g_receiver_ready_after_final_metadata_us{0};
static std::atomic<uint64_t> g_receiver_final_spool_ack_monotonic_us{0};
static std::atomic<uint64_t> g_receiver_ready_after_final_spool_ack_us{0};
static std::atomic<uint64_t> g_receiver_object_prewarm_start_monotonic_us{0};
static std::atomic<uint64_t> g_receiver_prewarm_backlog_at_phase2_end{0};
struct Receiver_epoch_ready_state {
  Preserve_trx_temp_final_timing temp_final;
  std::set<uint64_t> fact_tokens;
  std::map<uint64_t, Preserve_trx_receiver_failure_reason> token_results;
  std::map<uint64_t, size_t> fact_token_indexes;
  std::shared_ptr<const Preserve_trx_transfer_epoch_fact> fact;
  size_t ready_fact_token_count{0};
  size_t classified_fact_token_count{0};
  size_t failed_fact_token_count{0};
  uint64_t fact_loaded_monotonic_us{0};
  uint64_t final_spool_ack_monotonic_us{0};
  uint64_t ready_monotonic_us{0};
  bool fact_loaded{false};
  bool binding{false};
  bool bound{false};
  bool selection_published{false};
  bool global_failure{false};
  bool readiness_timing_logged{false};
#ifndef DBUG_OFF
  bool debug_failure_injected{false};
  bool debug_retry_injected{false};
#endif
  uint64_t binding_generation{0};
};
static std::mutex g_receiver_ready_epoch_mutex;
static std::map<std::pair<std::string, std::string>, Receiver_epoch_ready_state>
    g_receiver_ready_epoch_state;

static bool take_receiver_epoch_readiness_timing_locked(
    Receiver_epoch_ready_state *state, uint64_t *final_ack_us,
    uint64_t *ready_us) {
  if (state == nullptr || final_ack_us == nullptr || ready_us == nullptr ||
      state->readiness_timing_logged ||
      state->final_spool_ack_monotonic_us == 0 ||
      state->ready_monotonic_us == 0) {
    return false;
  }
  state->readiness_timing_logged = true;
  *final_ack_us = state->final_spool_ack_monotonic_us;
  *ready_us = state->ready_monotonic_us;
  return true;
}

static void log_receiver_epoch_readiness_timing(const std::string &epoch_id,
                                                uint64_t final_ack_us,
                                                uint64_t ready_us) {
  const uint64_t ready_after_final_ack_us =
      ready_us > final_ack_us ? ready_us - final_ack_us : 0;
  const std::string record =
      "PRESERVE_RECEIVER_READY_V1 epoch_id=" + epoch_id +
      " final_ack_us=" + std::to_string(final_ack_us) +
      " ready_us=" + std::to_string(ready_us) +
      " ready_after_final_ack_us=" +
      std::to_string(ready_after_final_ack_us);
  LogErr(SYSTEM_LEVEL, ER_LOG_PRINTF_MSG, record.c_str());
}
static std::atomic<bool> g_receiver_epoch_bind_bad_alloc_for_unit_test{false};
#ifndef NDEBUG
static std::atomic<Preserve_trx_transfer_before_final_fact_bind_hook>
    g_before_final_fact_bind_hook_for_unit_test{nullptr};
static std::atomic<Preserve_trx_transfer_before_final_fact_bind_hook>
    g_after_epoch_fact_cache_hook_for_unit_test{nullptr};
#endif

class Receiver_epoch_binding_guard {
 public:
  Receiver_epoch_binding_guard(std::string root_dir, std::string epoch_id)
      : m_key(std::move(root_dir), std::move(epoch_id)) {}
  Receiver_epoch_binding_guard(const Receiver_epoch_binding_guard &) = delete;
  Receiver_epoch_binding_guard &operator=(
      const Receiver_epoch_binding_guard &) = delete;
  ~Receiver_epoch_binding_guard() {
    if (!m_armed) return;
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    const auto found = g_receiver_ready_epoch_state.find(m_key);
    if (found != g_receiver_ready_epoch_state.end() &&
        found->second.binding_generation == m_generation) {
      found->second.binding = false;
    }
  }

  void arm(uint64_t generation) {
    m_generation = generation;
    m_armed = true;
  }
  void commit() { m_armed = false; }

 private:
  std::pair<std::string, std::string> m_key;
  uint64_t m_generation{0};
  bool m_armed{false};
};
struct Receiver_object_prewarm_key {
  std::string root_dir;
  std::string epoch_id;
  uint64_t token{0};
  std::string object_id;
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  uint64_t source_live_generation{0};
  std::array<unsigned char, kPreservedTrxSha256Length> source_live_digest{};

  bool operator<(const Receiver_object_prewarm_key &rhs) const {
    return std::tie(root_dir, epoch_id, token, object_id, digest,
                    source_live_generation, source_live_digest) <
           std::tie(rhs.root_dir, rhs.epoch_id, rhs.token, rhs.object_id,
                    rhs.digest, rhs.source_live_generation,
                    rhs.source_live_digest);
  }
};
struct Receiver_object_prewarm_proof {
  bool record_lock_object{false};
  bool metadata_only{false};
  uint64_t page_count{0};
  uint64_t resident_pages{0};
  uint64_t cold_gets{0};
  uint64_t bitmap_pages{0};
  uint64_t bitmap_bits{0};
};
struct Receiver_record_lock_prepared {
  std::unique_ptr<lock_preserve_metadata_plan_t> plan;
  lock_preserve_record_lock_metadata_facts_t facts;
  Preserve_memory_lease memory_lease;
};
static std::mutex g_receiver_object_prewarm_proof_mutex;
static std::map<Receiver_object_prewarm_key, Receiver_object_prewarm_proof>
    g_receiver_object_prewarm_proofs;
static std::mutex g_receiver_record_lock_prepared_mutex;
static std::map<Receiver_object_prewarm_key, Receiver_record_lock_prepared>
    g_receiver_record_lock_prepared;
struct Receiver_strict_record_lock_facts {
  lock_preserve_record_lock_metadata_facts_t facts;
  uint64_t plan_capacity_bytes{0};
};
struct Receiver_strict_binlog_facts {
  std::string handle_digest;
  uint64_t cache_length{0};
  uint64_t memory_bytes{0};
  bool file_backed{false};
};
enum class Receiver_binlog_prepared_state { QUEUED, BUILDING, READY };
struct Receiver_binlog_prepared {
  Receiver_binlog_prepared_state state{
      Receiver_binlog_prepared_state::QUEUED};
  Preserve_trx_prepared_token_key key;
  std::array<unsigned char, kPreservedTrxSha256Length> seed_digest{};
  std::array<unsigned char, kPreservedTrxSha256Length> payload_digest{};
  uint64_t payload_size{0};
  uint64_t instance{0};
  std::string facts_digest;
  Preserve_trx_prepared_token_resources resources;
};
using Receiver_strict_token_key =
    std::tuple<std::string, std::string, uint64_t>;
static std::mutex g_receiver_strict_record_lock_facts_mutex;
static std::map<Receiver_strict_token_key, Receiver_strict_record_lock_facts>
    g_receiver_strict_record_lock_facts;
static std::mutex g_receiver_strict_binlog_facts_mutex;
static std::map<Receiver_strict_token_key, Receiver_strict_binlog_facts>
    g_receiver_strict_binlog_facts;
static std::mutex g_receiver_binlog_prepared_mutex;
static uint64_t g_receiver_binlog_prepared_instance{0};
static std::map<Receiver_strict_token_key, Receiver_binlog_prepared>
    g_receiver_binlog_prepared;

static void erase_receiver_record_lock_object_generation(
    const std::string &root_dir, const std::string &epoch_id, uint64_t token) {
  {
    std::lock_guard<std::mutex> guard(g_receiver_object_prewarm_proof_mutex);
    for (auto it = g_receiver_object_prewarm_proofs.begin();
         it != g_receiver_object_prewarm_proofs.end();) {
      if (it->first.root_dir == root_dir && it->first.epoch_id == epoch_id &&
          it->first.token == token &&
          it->first.object_id == kPreservedTrxBlobRecordLocks) {
        it = g_receiver_object_prewarm_proofs.erase(it);
      } else {
        ++it;
      }
    }
  }
  {
    std::lock_guard<std::mutex> guard(g_receiver_record_lock_prepared_mutex);
    for (auto it = g_receiver_record_lock_prepared.begin();
         it != g_receiver_record_lock_prepared.end();) {
      if (it->first.root_dir == root_dir && it->first.epoch_id == epoch_id &&
          it->first.token == token) {
        it = g_receiver_record_lock_prepared.erase(it);
      } else {
        ++it;
      }
    }
  }
  std::lock_guard<std::mutex> guard(
      g_receiver_strict_record_lock_facts_mutex);
  g_receiver_strict_record_lock_facts.erase({root_dir, epoch_id, token});
}

static void erase_receiver_strict_record_lock_state(
    const std::string &root_dir, const std::string &epoch_id, uint64_t token) {
  erase_receiver_record_lock_object_generation(root_dir, epoch_id, token);
  std::lock_guard<std::mutex> binlog_guard(
      g_receiver_strict_binlog_facts_mutex);
  g_receiver_strict_binlog_facts.erase({root_dir, epoch_id, token});
}

bool receiver_record_lock_proof_gate_ready(
    const Receiver_object_prewarm_proof &proof) {
  return proof.record_lock_object &&
         (proof.metadata_only ||
          (proof.page_count == proof.resident_pages && proof.cold_gets == 0));
}

bool receiver_record_lock_proof_is_improvement(
    const Receiver_object_prewarm_proof &candidate,
    const Receiver_object_prewarm_proof &current) {
  if (!candidate.record_lock_object || !current.record_lock_object ||
      candidate.page_count != current.page_count) {
    return false;
  }
  if (candidate.metadata_only) return !current.metadata_only;
  if (current.metadata_only) return false;
  return candidate.resident_pages > current.resident_pages ||
         candidate.cold_gets < current.cold_gets;
}

static constexpr size_t kReceiverStagingFinalizeLockShardCount = 4096;
static std::array<std::mutex, kReceiverStagingFinalizeLockShardCount>
    g_receiver_staging_finalize_mutexes;
static constexpr std::array<uint64_t, 13> kProjectionPublishP95BucketsUs{
    100, 250, 500, 1000, 2000, 5000, 10000, 25000, 50000, 100000, 250000,
    500000, 1000000};
static constexpr uint64_t kReceiverRecordLockResidencyPollIntervalUs = 1000;
static constexpr uint kReceiverRecordLockObjectProofRetryLimit = 16;
static std::array<std::atomic<uint64_t>, kProjectionPublishP95BucketsUs.size()>
    g_receiver_projection_publish_histogram{};
static thread_local bool g_receiver_frame_sequence_disabled = false;

static uint64_t transfer_monotonic_us() {
  using clock = std::chrono::steady_clock;
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          clock::now().time_since_epoch())
          .count());
}

// These observations share the existing epoch lifecycle and never decide
// admission or readiness. Failed instrumentation cannot fail a transaction.
using Temp_final_field = Preserve_trx_temp_final_field;
static void note_receiver_temp_final(const std::string &root,
    const std::string &epoch, Temp_final_field field, uint64_t value = 1) {
  try {
    std::lock_guard<std::mutex> lock(g_receiver_ready_epoch_mutex);
    auto it = g_receiver_ready_epoch_state.find({root, epoch});
    if (it != g_receiver_ready_epoch_state.end())
      it->second.temp_final.note(field, value);
  } catch (...) { preserve_trx_temp_final_observation_dropped(); }
}
class Receiver_temp_activity_guard {
 public:
  Receiver_temp_activity_guard(const std::string &root, const std::string &epoch,
                               bool binding = false, bool enabled = true)
      : m_root(root), m_epoch(epoch), m_binding(binding) {
    if (!enabled) return;
    try {
      std::lock_guard<std::mutex> lock(g_receiver_ready_epoch_mutex);
      g_receiver_ready_epoch_state[{root, epoch}].temp_final.activity(
          transfer_monotonic_us(), binding ? 0 : 1, binding ? 1 : 0);
      m_active = true;
    } catch (...) { preserve_trx_temp_final_observation_dropped(); }
  }
  ~Receiver_temp_activity_guard() { change(false); }
  void binding() { if (!m_binding) change(true); }
 private:
  void change(bool binding) {
    if (!m_active) return;
    try {
      std::lock_guard<std::mutex> lock(g_receiver_ready_epoch_mutex);
      auto it = g_receiver_ready_epoch_state.find({m_root, m_epoch});
      if (it != g_receiver_ready_epoch_state.end())
        it->second.temp_final.activity(transfer_monotonic_us(),
            m_binding ? 0 : -1, binding ? 1 : (m_binding ? -1 : 0));
      m_binding = binding;
      if (!binding) m_active = false;
    } catch (...) { preserve_trx_temp_final_observation_dropped(); }
  }
  const std::string &m_root, &m_epoch;
  bool m_binding{false}, m_active{false};
};

Preserve_trx_transfer_runtime_policy
preserve_trx_transfer_runtime_policy_for_profile(uint profile) {
  Preserve_trx_transfer_runtime_policy policy;
  switch (profile) {
    case PRESERVE_TRX_TRANSFER_RUNTIME_BALANCED:
      policy.profile = profile;
      policy.receiver_workers = 4;
      policy.prewarm_workers = 3;
      policy.transfer_io_bytes_per_sec = 256ULL * 1024ULL * 1024ULL;
      policy.prewarm_io_bytes_per_sec = 512ULL * 1024ULL * 1024ULL;
      policy.prewarm_max_bytes = 512ULL * 1024ULL * 1024ULL;
      policy.warmcopy_chunk_bytes = 4U * 1024U * 1024U;
      policy.warmcopy_min_open_ms = 100;
      policy.commit_batch_tokens = 16;
      break;
    case PRESERVE_TRX_TRANSFER_RUNTIME_PROMOTION_PREPARE:
      policy.profile = profile;
      policy.receiver_workers = 8;
      policy.prewarm_workers = 8;
      policy.transfer_io_bytes_per_sec = 4ULL * 1024ULL * 1024ULL * 1024ULL;
      policy.prewarm_io_bytes_per_sec = 4ULL * 1024ULL * 1024ULL * 1024ULL;
      policy.prewarm_max_bytes = 1024ULL * 1024ULL * 1024ULL;
      policy.warmcopy_chunk_bytes = 16U * 1024U * 1024U;
      policy.warmcopy_min_open_ms = 1;
      policy.commit_batch_tokens = 32;
      policy.phase1_batch_bytes = 8ULL * 1024ULL * 1024ULL;
      policy.phase1_batch_linger_ms = 50;
      break;
    case PRESERVE_TRX_TRANSFER_RUNTIME_BUSINESS_FIRST:
    default:
      break;
  }
  return policy;
}

Preserve_trx_transfer_runtime_policy
preserve_trx_transfer_current_runtime_policy() {
  return preserve_trx_transfer_runtime_policy_for_profile(
      preserve_trx_transfer_runtime_profile);
}

uint64_t preserve_trx_transfer_throttled_milliseconds_status() {
  return g_transfer_throttled_us.load() / 1000;
}

uint64_t preserve_trx_transfer_last_throttle_reason_status() {
  return g_transfer_last_throttle_reason.load();
}

uint64_t preserve_trx_transfer_receiver_queued_bytes_status() {
  return g_receiver_queued_bytes.load();
}

uint64_t preserve_trx_transfer_receiver_worker_active_status() {
  return g_receiver_worker_active.load();
}

uint64_t preserve_trx_transfer_receiver_worker_idle_status() {
  const uint64_t workers = g_receiver_worker_count.load();
  const uint64_t active = g_receiver_worker_active.load();
  return workers > active ? workers - active : 0;
}

enum class Transfer_throttle_reason : uint64_t {
  SOURCE_IO = 1,
  RECEIVER_SAVED_IO = 2,
  RECEIVER_PREWARM_IO = 3,
  WORKER_YIELD = 4,
  PREWARM_PAUSED = 5
};

class Transfer_io_rate_limiter {
 public:
  void throttle(uint64_t bytes, uint64_t bytes_per_second,
                Transfer_throttle_reason reason) {
    if (bytes == 0 || bytes_per_second == 0) return;
    const uint64_t whole_seconds = bytes / bytes_per_second;
    const uint64_t remainder = bytes % bytes_per_second;
    if (whole_seconds > std::numeric_limits<uint64_t>::max() / 1000000ULL) {
      return;
    }
    uint64_t duration_us = whole_seconds * 1000000ULL;
    const uint64_t remainder_us =
        remainder == 0
            ? 0
            : 1 + ((remainder - 1) * 1000000ULL) / bytes_per_second;
    if (duration_us >
        std::numeric_limits<uint64_t>::max() - remainder_us) {
      return;
    }
    duration_us += remainder_us;

    uint64_t sleep_us = 0;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      const uint64_t now_us = transfer_monotonic_us();
      const uint64_t scheduled_us = std::max(now_us, m_next_available_us);
      sleep_us = scheduled_us - now_us;
      m_next_available_us =
          scheduled_us > std::numeric_limits<uint64_t>::max() - duration_us
              ? std::numeric_limits<uint64_t>::max()
              : scheduled_us + duration_us;
    }
    if (sleep_us == 0) return;
    g_transfer_last_throttle_reason.store(static_cast<uint64_t>(reason));
    g_transfer_throttled_us.fetch_add(sleep_us);
    while (sleep_us != 0) {
      const ulong slice = static_cast<ulong>(std::min<uint64_t>(
          sleep_us, std::numeric_limits<ulong>::max()));
      my_sleep(slice);
      sleep_us -= slice;
    }
  }

  void reset() {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_next_available_us = 0;
  }

 private:
  std::mutex m_mutex;
  uint64_t m_next_available_us{0};
};

static Transfer_io_rate_limiter g_source_io_rate_limiter;
static Transfer_io_rate_limiter g_receiver_io_rate_limiter;
static Transfer_io_rate_limiter g_receiver_prewarm_rate_limiter;

#ifndef NDEBUG
void preserve_trx_transfer_reset_io_rate_limiters_for_unit_test() {
  g_source_io_rate_limiter.reset();
  g_receiver_io_rate_limiter.reset();
  g_receiver_prewarm_rate_limiter.reset();
}
#endif

static void throttle_source_transfer_io(
    uint64_t bytes, const Preserve_trx_transfer_runtime_policy &policy) {
  g_source_io_rate_limiter.throttle(
      bytes, policy.transfer_io_bytes_per_sec,
      Transfer_throttle_reason::SOURCE_IO);
}

static void throttle_receiver_saved_io(
    uint64_t bytes, const Preserve_trx_transfer_runtime_policy &policy) {
  g_receiver_io_rate_limiter.throttle(
      bytes, policy.transfer_io_bytes_per_sec,
      Transfer_throttle_reason::RECEIVER_SAVED_IO);
}

static void throttle_receiver_saved_io(uint64_t bytes) {
  throttle_receiver_saved_io(
      bytes, preserve_trx_transfer_current_runtime_policy());
}

static void throttle_receiver_prewarm_io(
    uint64_t bytes, const Preserve_trx_transfer_runtime_policy &policy) {
  g_receiver_prewarm_rate_limiter.throttle(
      bytes, policy.prewarm_io_bytes_per_sec,
      Transfer_throttle_reason::RECEIVER_PREWARM_IO);
}

static void preserve_trx_transfer_worker_yield(uint64_t yield_us) {
  if (yield_us == 0) return;
  g_transfer_last_throttle_reason.store(
      static_cast<uint64_t>(Transfer_throttle_reason::WORKER_YIELD));
  g_transfer_throttled_us.fetch_add(yield_us);
  my_sleep(static_cast<ulong>(std::min<uint64_t>(
      yield_us, std::numeric_limits<ulong>::max())));
}

static void preserve_trx_transfer_worker_yield() {
  preserve_trx_transfer_worker_yield(
      preserve_trx_transfer_current_runtime_policy().worker_yield_us);
}

bool receiver_prewarm_work_batch_should_yield(uint64_t job_elapsed_us,
                                              uint64_t yield_quantum_us,
                                              uint64_t *active_work_us) {
  if (active_work_us == nullptr) return false;
  if (yield_quantum_us == 0) {
    *active_work_us = 0;
    return false;
  }
  if (job_elapsed_us >= yield_quantum_us ||
      *active_work_us >= yield_quantum_us - job_elapsed_us) {
    *active_work_us = 0;
    return true;
  }
  *active_work_us += job_elapsed_us;
  return false;
}

static uint64_t phase1_sample_percentile(const std::vector<uint64_t> &values,
                                         uint percentile) {
  if (values.empty()) return 0;
  std::vector<uint64_t> sorted(values);
  std::sort(sorted.begin(), sorted.end());
  const size_t index = std::min<size_t>(
      sorted.size() - 1,
      (sorted.size() * static_cast<size_t>(percentile) + 99) / 100 - 1);
  return sorted[index];
}

static void note_source_phase1_network_send(uint64_t frame_count,
                                            uint64_t encoded_bytes,
                                            uint64_t token_count,
                                            bool encoded_batch) {
  g_transfer_phase1_frame_count.fetch_add(frame_count);
  g_transfer_phase1_network_send_count.fetch_add(1);
  if (!encoded_batch) return;
  g_transfer_phase1_batch_count.fetch_add(1);
  std::lock_guard<std::mutex> guard(g_transfer_phase1_batch_metrics_mutex);
  g_transfer_phase1_batch_bytes_samples.push_back(encoded_bytes);
  g_transfer_phase1_batch_tokens_samples.push_back(token_count);
}

static void note_source_phase1_batch_linger(uint64_t linger_us) {
  std::lock_guard<std::mutex> guard(g_transfer_phase1_batch_metrics_mutex);
  g_transfer_phase1_batch_linger_us_samples.push_back(linger_us);
}

static void note_source_phase1_record_batch_sent(uint64_t token_count) {
  const uint64_t now_us = transfer_monotonic_us();
  uint64_t expected = 0;
  (void)g_transfer_phase1_record_first_batch_send_us.compare_exchange_strong(
      expected, now_us);
  g_transfer_phase1_record_last_batch_send_us.store(now_us);
  std::lock_guard<std::mutex> guard(g_transfer_phase1_batch_metrics_mutex);
  g_transfer_phase1_record_batch_tokens_samples.push_back(token_count);
}

void preserve_trx_transfer_reset_source_phase1_metrics() {
  g_transfer_phase1_frame_count.store(0);
  g_transfer_phase1_network_send_count.store(0);
  g_transfer_phase1_batch_count.store(0);
  g_transfer_phase1_oversize_token_count.store(0);
  g_transfer_phase1_record_first_batch_send_us.store(0);
  g_transfer_phase1_record_last_batch_send_us.store(0);
  std::lock_guard<std::mutex> guard(g_transfer_phase1_batch_metrics_mutex);
  g_transfer_phase1_batch_bytes_samples.clear();
  g_transfer_phase1_batch_tokens_samples.clear();
  g_transfer_phase1_record_batch_tokens_samples.clear();
  g_transfer_phase1_batch_linger_us_samples.clear();
}

uint64_t preserve_trx_transfer_phase1_frame_count_status() {
  return g_transfer_phase1_frame_count.load();
}

uint64_t preserve_trx_transfer_phase1_network_send_count_status() {
  return g_transfer_phase1_network_send_count.load();
}

uint64_t preserve_trx_transfer_phase1_batch_count_status() {
  return g_transfer_phase1_batch_count.load();
}

uint64_t preserve_trx_transfer_phase1_batch_bytes_p50_status() {
  std::lock_guard<std::mutex> guard(g_transfer_phase1_batch_metrics_mutex);
  return phase1_sample_percentile(g_transfer_phase1_batch_bytes_samples, 50);
}

uint64_t preserve_trx_transfer_phase1_batch_bytes_p95_status() {
  std::lock_guard<std::mutex> guard(g_transfer_phase1_batch_metrics_mutex);
  return phase1_sample_percentile(g_transfer_phase1_batch_bytes_samples, 95);
}

uint64_t preserve_trx_transfer_phase1_batch_bytes_max_status() {
  std::lock_guard<std::mutex> guard(g_transfer_phase1_batch_metrics_mutex);
  return g_transfer_phase1_batch_bytes_samples.empty()
             ? 0
             : *std::max_element(g_transfer_phase1_batch_bytes_samples.begin(),
                                 g_transfer_phase1_batch_bytes_samples.end());
}

uint64_t preserve_trx_transfer_phase1_batch_tokens_p50_status() {
  std::lock_guard<std::mutex> guard(g_transfer_phase1_batch_metrics_mutex);
  return phase1_sample_percentile(g_transfer_phase1_batch_tokens_samples, 50);
}

uint64_t preserve_trx_transfer_phase1_batch_tokens_p95_status() {
  std::lock_guard<std::mutex> guard(g_transfer_phase1_batch_metrics_mutex);
  return phase1_sample_percentile(g_transfer_phase1_batch_tokens_samples, 95);
}

uint64_t preserve_trx_transfer_phase1_batch_tokens_max_status() {
  std::lock_guard<std::mutex> guard(g_transfer_phase1_batch_metrics_mutex);
  return g_transfer_phase1_batch_tokens_samples.empty()
             ? 0
             : *std::max_element(g_transfer_phase1_batch_tokens_samples.begin(),
                                 g_transfer_phase1_batch_tokens_samples.end());
}

uint64_t preserve_trx_transfer_phase1_record_batch_tokens_avg_status() {
  std::lock_guard<std::mutex> guard(g_transfer_phase1_batch_metrics_mutex);
  if (g_transfer_phase1_record_batch_tokens_samples.empty()) return 0;
  uint64_t sum = 0;
  for (uint64_t value : g_transfer_phase1_record_batch_tokens_samples) {
    sum += value;
  }
  return sum / g_transfer_phase1_record_batch_tokens_samples.size();
}

uint64_t preserve_trx_transfer_phase1_batch_linger_us_p95_status() {
  std::lock_guard<std::mutex> guard(g_transfer_phase1_batch_metrics_mutex);
  return phase1_sample_percentile(g_transfer_phase1_batch_linger_us_samples, 95);
}

uint64_t preserve_trx_transfer_phase1_batch_linger_us_max_status() {
  std::lock_guard<std::mutex> guard(g_transfer_phase1_batch_metrics_mutex);
  return g_transfer_phase1_batch_linger_us_samples.empty()
             ? 0
             : *std::max_element(g_transfer_phase1_batch_linger_us_samples.begin(),
                                 g_transfer_phase1_batch_linger_us_samples.end());
}

uint64_t preserve_trx_transfer_phase1_oversize_token_count_status() {
  return g_transfer_phase1_oversize_token_count.load();
}

uint64_t preserve_trx_transfer_phase1_record_first_batch_send_us_status() {
  return g_transfer_phase1_record_first_batch_send_us.load();
}

uint64_t preserve_trx_transfer_phase1_record_last_batch_send_us_status() {
  return g_transfer_phase1_record_last_batch_send_us.load();
}

class Preserve_trx_transfer_phase1_batch_sender::Impl {
 public:
  using clock = std::chrono::steady_clock;

  struct Queued_request {
    Preserve_trx_transfer_phase1_blob_request request;
    clock::time_point enqueued_at;
  };

  Impl(const Preserve_trx_transfer_phase1_batch_options &options,
       Preserve_trx_transfer_phase1_batch_flush_callback flush_callback,
       void *flush_context)
      : m_options(options),
        m_flush_callback(flush_callback),
        m_flush_context(flush_context) {
    if (m_flush_callback == nullptr || m_options.max_inflight_bytes == 0) {
      m_status = Preserve_trx_transfer_status::INVALID_ARGUMENT;
      return;
    }
    if (m_options.max_batch_bytes > m_options.max_inflight_bytes) {
      m_status = Preserve_trx_transfer_status::UNSUPPORTED;
      return;
    }
    m_batching_enabled =
        m_options.max_batch_bytes != 0 && m_options.linger_ms != 0;
    if (m_batching_enabled) m_worker = std::thread([this]() { run(); });
  }

  ~Impl() {
    if (!m_batching_enabled) return;
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      m_stop = true;
      m_flush_requested = true;
      m_condition.notify_all();
    }
    if (m_worker.joinable()) m_worker.join();
  }

  Preserve_trx_transfer_status enqueue(
      const Preserve_trx_transfer_phase1_blob_request &request) {
    const bool inline_payload = !request.inline_payload.empty();
    if (request.transfer_token == 0 || request.object_id.empty() ||
        (!inline_payload &&
         (request.warmcopy_id.empty() || request.warmcopy_epoch == 0)) ||
        request.size == 0 ||
        request.preserved_prefix_size >= request.size) {
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }
    const uint64_t payload_bytes =
        request.inline_payload.empty()
            ? request.size - request.preserved_prefix_size
            : static_cast<uint64_t>(request.inline_payload.size());
    if (payload_bytes != request.size - request.preserved_prefix_size) {
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }
    if (!m_batching_enabled) {
      if (m_status != Preserve_trx_transfer_status::OK) return m_status;
      const Preserve_trx_transfer_status status =
          m_flush_callback(std::vector<Preserve_trx_transfer_phase1_blob_request>{
                               request},
                           m_flush_context);
      if (status != Preserve_trx_transfer_status::OK) m_status = status;
      return status;
    }

    std::unique_lock<std::mutex> guard(m_mutex);
    if (m_status != Preserve_trx_transfer_status::OK) return m_status;
    if (payload_bytes > m_options.max_inflight_bytes) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    m_condition.wait(guard, [&]() {
      const uint64_t used_bytes = m_queued_bytes + m_in_flight_bytes;
      return m_status != Preserve_trx_transfer_status::OK || m_stop ||
             used_bytes <=
                 m_options.max_inflight_bytes - payload_bytes;
    });
    if (m_status != Preserve_trx_transfer_status::OK) return m_status;
    if (m_stop) return Preserve_trx_transfer_status::UNSUPPORTED;

    m_queue.push_back({request, clock::now()});
    m_queued_bytes += payload_bytes;
    m_condition.notify_all();
    return Preserve_trx_transfer_status::OK;
  }

  Preserve_trx_transfer_status flush() {
    if (!m_batching_enabled) return m_status;
    std::unique_lock<std::mutex> guard(m_mutex);
    m_flush_requested = true;
    m_condition.notify_all();
    m_condition.wait(guard, [&]() {
      return m_status != Preserve_trx_transfer_status::OK ||
             (m_queue.empty() && !m_in_flight);
    });
    if (m_status == Preserve_trx_transfer_status::OK) {
      m_flush_requested = false;
    }
    return m_status;
  }

  void abort() {
    if (!m_batching_enabled) return;
    std::lock_guard<std::mutex> guard(m_mutex);
    m_status = Preserve_trx_transfer_status::UNSUPPORTED;
    m_stop = true;
    m_queue.clear();
    m_queued_bytes = 0;
    m_condition.notify_all();
  }

 private:
  void run() {
    if (my_thread_init()) {
      std::lock_guard<std::mutex> failed_guard(m_mutex);
      m_status = Preserve_trx_transfer_status::UNSUPPORTED;
      m_condition.notify_all();
      return;
    }
    auto thread_end_guard = create_scope_guard([] { my_thread_end(); });
    std::unique_lock<std::mutex> guard(m_mutex);
    while (true) {
      m_condition.wait(guard, [&]() {
        return m_stop || m_status != Preserve_trx_transfer_status::OK ||
               !m_queue.empty();
      });
      if ((m_stop || m_status != Preserve_trx_transfer_status::OK) &&
          m_queue.empty()) {
        return;
      }
      if (m_queue.empty()) continue;

      const auto deadline =
          m_queue.front().enqueued_at +
          std::chrono::milliseconds(m_options.linger_ms);
      if (!m_flush_requested &&
          m_queued_bytes < m_options.max_batch_bytes &&
          clock::now() < deadline) {
        m_condition.wait_until(guard, deadline);
        continue;
      }

      std::vector<Preserve_trx_transfer_phase1_blob_request> batch;
      uint64_t batch_bytes = 0;
      const uint64_t linger_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              clock::now() - m_queue.front().enqueued_at)
              .count());
      while (!m_queue.empty()) {
        const Preserve_trx_transfer_phase1_blob_request &front =
            m_queue.front().request;
        const uint64_t front_bytes =
            front.inline_payload.empty()
                ? front.size - front.preserved_prefix_size
                : static_cast<uint64_t>(front.inline_payload.size());
        if (!batch.empty() &&
            (batch_bytes >= m_options.max_batch_bytes ||
             front_bytes > m_options.max_batch_bytes - batch_bytes)) {
          break;
        }
        Queued_request queued = std::move(m_queue.front());
        m_queue.pop_front();
        m_queued_bytes -= front_bytes;
        batch_bytes += front_bytes;
        batch.push_back(std::move(queued.request));
        if (batch_bytes >= m_options.max_batch_bytes) break;
      }
      m_in_flight = true;
      m_in_flight_bytes = batch_bytes;
      guard.unlock();
      note_source_phase1_batch_linger(linger_us);
      Preserve_trx_transfer_status status = Preserve_trx_transfer_status::OK;
      try {
        status = m_flush_callback(batch, m_flush_context);
      } catch (...) {
        LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
               ("PRESERVE: phase1 batch callback threw requests=" +
                std::to_string(batch.size()) + " payload_bytes=" +
                std::to_string(batch_bytes))
                   .c_str());
        status = Preserve_trx_transfer_status::UNSUPPORTED;
      }
      if (status != Preserve_trx_transfer_status::OK) {
        LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
               ("PRESERVE: phase1 batch callback failed status=" +
                std::to_string(static_cast<int>(status)) + " requests=" +
                std::to_string(batch.size()) + " payload_bytes=" +
                std::to_string(batch_bytes))
                   .c_str());
      }
      guard.lock();
      m_in_flight = false;
      m_in_flight_bytes = 0;
      if (status != Preserve_trx_transfer_status::OK) {
        m_status = status;
        m_queue.clear();
        m_queued_bytes = 0;
      }
      m_condition.notify_all();
    }
  }

  Preserve_trx_transfer_phase1_batch_options m_options;
  Preserve_trx_transfer_phase1_batch_flush_callback m_flush_callback{nullptr};
  void *m_flush_context{nullptr};
  bool m_batching_enabled{false};
  bool m_stop{false};
  bool m_flush_requested{false};
  bool m_in_flight{false};
  Preserve_trx_transfer_status m_status{Preserve_trx_transfer_status::OK};
  std::mutex m_mutex;
  std::condition_variable m_condition;
  std::deque<Queued_request> m_queue;
  uint64_t m_queued_bytes{0};
  uint64_t m_in_flight_bytes{0};
  std::thread m_worker;
};

Preserve_trx_transfer_phase1_batch_sender::
    Preserve_trx_transfer_phase1_batch_sender(
        const Preserve_trx_transfer_phase1_batch_options &options,
        Preserve_trx_transfer_phase1_batch_flush_callback flush_callback,
        void *flush_context)
    : m_impl(new Impl(options, flush_callback, flush_context)) {}

Preserve_trx_transfer_phase1_batch_sender::~
    Preserve_trx_transfer_phase1_batch_sender() = default;

Preserve_trx_transfer_status
Preserve_trx_transfer_phase1_batch_sender::enqueue(
    const Preserve_trx_transfer_phase1_blob_request &request) {
  return m_impl == nullptr ? Preserve_trx_transfer_status::INVALID_ARGUMENT
                           : m_impl->enqueue(request);
}

Preserve_trx_transfer_status Preserve_trx_transfer_phase1_batch_sender::flush() {
  return m_impl == nullptr ? Preserve_trx_transfer_status::INVALID_ARGUMENT
                           : m_impl->flush();
}

void Preserve_trx_transfer_phase1_batch_sender::abort() {
  if (m_impl != nullptr) m_impl->abort();
}

uint64_t preserve_trx_transfer_receiver_auto_prewarm_tokens_status() {
  return g_receiver_auto_prewarm_tokens.load();
}

uint64_t preserve_trx_transfer_receiver_auto_prewarm_ready_tokens_status() {
  return g_receiver_auto_prewarm_ready_tokens.load();
}

uint64_t preserve_trx_transfer_receiver_auto_prewarm_not_ready_tokens_status() {
  return g_receiver_auto_prewarm_not_ready_tokens.load();
}

uint64_t preserve_trx_transfer_receiver_auto_prewarm_last_status() {
  return g_receiver_auto_prewarm_last_status.load();
}

uint64_t preserve_trx_transfer_receiver_ready_monotonic_us_status() {
  return g_receiver_ready_monotonic_us.load();
}

uint64_t
preserve_trx_transfer_receiver_final_metadata_accepted_monotonic_us_status() {
  return g_receiver_final_metadata_accepted_monotonic_us.load();
}

uint64_t
preserve_trx_transfer_receiver_terminal_commit_admitted_monotonic_us_status() {
  return g_receiver_terminal_commit_admitted_monotonic_us.load();
}

uint64_t
preserve_trx_transfer_receiver_ready_after_final_metadata_accepted_us_status() {
  return g_receiver_ready_after_final_metadata_accepted_us.load();
}

uint64_t
preserve_trx_transfer_receiver_ready_after_terminal_commit_admitted_us_status() {
  return g_receiver_ready_after_terminal_commit_admitted_us.load();
}

uint64_t preserve_trx_transfer_receiver_admitted_frames_status() {
  return g_receiver_admitted_frames.load();
}

uint64_t preserve_trx_transfer_receiver_admitted_bytes_status() {
  return g_receiver_admitted_bytes.load();
}

uint64_t preserve_trx_transfer_source_handoff_pending_epochs_status() {
  return g_source_handoff_pending_epochs.load();
}

uint64_t preserve_trx_transfer_source_commit_unknown_epochs_status() {
  return g_source_commit_unknown_epochs.load();
}

uint64_t preserve_trx_transfer_source_restore_guard_rejects_status() {
  return g_source_restore_guard_rejects.load();
}

void preserve_trx_transfer_note_source_handoff_pending() {
  g_source_handoff_pending_epochs.store(1);
}

void preserve_trx_transfer_note_source_handoff_committed() {
  g_source_handoff_pending_epochs.store(0);
  g_source_commit_unknown_epochs.store(0);
  g_quarantine_epochs.store(0);
  g_quarantine_tokens.store(0);
  g_quarantine_bytes.store(0);
  g_quarantine_oldest_age_us.store(0);
  g_quarantine_started_monotonic_us.store(0);
}

void preserve_trx_transfer_note_source_commit_unknown(uint64_t token_count,
                                                      uint64_t retained_bytes) {
  g_source_handoff_pending_epochs.store(0);
  g_source_commit_unknown_epochs.store(1);
  g_quarantine_epochs.store(1);
  g_quarantine_tokens.store(token_count);
  g_quarantine_bytes.store(retained_bytes);
  g_quarantine_oldest_age_us.store(0);
  g_quarantine_started_monotonic_us.store(transfer_monotonic_us());
}

void preserve_trx_transfer_note_source_restore_guard_reject() {
  g_source_restore_guard_rejects.fetch_add(1);
}

uint64_t preserve_trx_transfer_receiver_terminal_cas_wins_status() {
  return g_receiver_terminal_cas_wins.load();
}

uint64_t preserve_trx_transfer_receiver_terminal_cas_duplicates_status() {
  return g_receiver_terminal_cas_duplicates.load();
}

uint64_t preserve_trx_transfer_receiver_terminal_cas_conflicts_status() {
  return g_receiver_terminal_cas_conflicts.load();
}

uint64_t preserve_trx_transfer_receiver_terminal_status_tombstones_status() {
  return g_receiver_terminal_status_tombstones.load();
}

uint64_t
preserve_trx_transfer_receiver_terminal_status_tombstone_expiries_status() {
  return g_receiver_terminal_status_tombstone_expiries.load();
}

uint64_t preserve_trx_transfer_quarantine_epochs_status() {
  return g_quarantine_epochs.load();
}

uint64_t preserve_trx_transfer_quarantine_tokens_status() {
  return g_quarantine_tokens.load();
}

uint64_t preserve_trx_transfer_quarantine_bytes_status() {
  return g_quarantine_bytes.load();
}

uint64_t preserve_trx_transfer_quarantine_oldest_age_us_status() {
  if (g_quarantine_epochs.load() == 0) return 0;
  const uint64_t started_us = g_quarantine_started_monotonic_us.load();
  const uint64_t now_us = transfer_monotonic_us();
  const uint64_t age_us =
      started_us != 0 && now_us > started_us ? now_us - started_us : 0;
  g_quarantine_oldest_age_us.store(age_us);
  return age_us;
}

uint64_t preserve_trx_transfer_receiver_first_frame_monotonic_us_status() {
  return g_receiver_first_frame_monotonic_us.load();
}

uint64_t preserve_trx_transfer_receiver_last_object_seal_monotonic_us_status() {
  return g_receiver_last_object_seal_monotonic_us.load();
}

uint64_t preserve_trx_transfer_receiver_prewarm_start_monotonic_us_status() {
  return g_receiver_prewarm_start_monotonic_us.load();
}

uint64_t preserve_trx_transfer_receiver_prewarm_end_monotonic_us_status() {
  return g_receiver_prewarm_end_monotonic_us.load();
}

uint64_t preserve_trx_transfer_receiver_seal_prewarm_tokens_status() {
  return g_receiver_seal_prewarm_tokens.load();
}

uint64_t preserve_trx_transfer_receiver_seal_prewarm_success_tokens_status() {
  return g_receiver_seal_prewarm_success_tokens.load();
}

uint64_t preserve_trx_transfer_receiver_seal_prewarm_not_ready_tokens_status() {
  return g_receiver_seal_prewarm_not_ready_tokens.load();
}

uint64_t preserve_trx_transfer_receiver_seal_prewarm_last_status() {
  return g_receiver_seal_prewarm_last_status.load();
}

uint64_t preserve_trx_transfer_receiver_object_prewarm_proof_count_status() {
  return g_receiver_object_prewarm_proof_count.load();
}

uint64_t preserve_trx_transfer_receiver_object_prewarm_miss_count_status() {
  return g_receiver_object_prewarm_miss_count.load();
}

uint64_t preserve_trx_transfer_receiver_object_prewarm_count_status() {
  return g_receiver_object_prewarm_count.load();
}

uint64_t preserve_trx_transfer_receiver_object_prewarm_us_status() {
  return g_receiver_object_prewarm_us.load();
}

uint64_t preserve_trx_transfer_receiver_object_prewarm_max_us_status() {
  return g_receiver_object_prewarm_max_us.load();
}

uint64_t
preserve_trx_transfer_receiver_object_prewarm_first_start_monotonic_us_status() {
  return g_receiver_object_prewarm_first_start_us.load();
}

uint64_t
preserve_trx_transfer_receiver_object_prewarm_last_end_monotonic_us_status() {
  return g_receiver_object_prewarm_last_end_us.load();
}

uint64_t preserve_trx_transfer_receiver_record_object_prewarm_count_status() {
  return g_receiver_record_object_prewarm_count.load();
}

uint64_t preserve_trx_transfer_receiver_record_object_prewarm_us_status() {
  return g_receiver_record_object_prewarm_us.load();
}

uint64_t preserve_trx_transfer_receiver_record_object_prewarm_max_us_status() {
  return g_receiver_record_object_prewarm_max_us.load();
}

uint64_t
preserve_trx_transfer_receiver_strict_record_index_page_reads_status() {
  return g_receiver_strict_record_index_page_reads.load();
}

uint64_t preserve_trx_transfer_receiver_strict_ibuf_merges_status() {
  return g_receiver_strict_ibuf_merges.load();
}

uint64_t
preserve_trx_transfer_receiver_strict_target_local_redo_bytes_status() {
  return g_receiver_strict_target_local_redo_bytes.load();
}

uint64_t
preserve_trx_transfer_receiver_record_object_prewarm_first_start_monotonic_us_status() {
  return g_receiver_record_object_prewarm_first_start_us.load();
}

uint64_t
preserve_trx_transfer_receiver_record_object_prewarm_last_end_monotonic_us_status() {
  return g_receiver_record_object_prewarm_last_end_us.load();
}

uint64_t
preserve_trx_transfer_receiver_binlog_object_prewarm_first_start_monotonic_us_status() {
  return g_receiver_binlog_object_prewarm_first_start_us.load();
}

uint64_t
preserve_trx_transfer_receiver_binlog_object_prewarm_last_end_monotonic_us_status() {
  return g_receiver_binlog_object_prewarm_last_end_us.load();
}

uint64_t preserve_trx_transfer_receiver_committed_epoch_fallback_count_status() {
  return g_receiver_committed_epoch_fallback_count.load();
}

uint64_t preserve_trx_transfer_receiver_staged_token_ready_cache_us_status() {
  return g_receiver_staged_token_ready_cache_us.load();
}

uint64_t preserve_trx_transfer_receiver_staged_token_total_us_status() {
  return g_receiver_staged_token_total_us.load();
}

uint64_t preserve_trx_transfer_receiver_staged_token_max_us_status() {
  return g_receiver_staged_token_max_us.load();
}

uint64_t preserve_trx_transfer_receiver_staged_token_active_status() {
  return g_receiver_staged_token_active.load();
}

uint64_t preserve_trx_transfer_receiver_staged_token_max_active_status() {
  return g_receiver_staged_token_max_active.load();
}

uint64_t preserve_trx_transfer_receiver_projection_publish_count_status() {
  return g_receiver_projection_publish_count.load();
}

uint64_t preserve_trx_transfer_receiver_projection_publish_us_status() {
  return g_receiver_projection_publish_us.load();
}

uint64_t preserve_trx_transfer_receiver_projection_publish_max_us_status() {
  return g_receiver_projection_publish_max_us.load();
}

uint64_t preserve_trx_transfer_receiver_projection_publish_p95_us_status() {
  const uint64_t total = g_receiver_projection_publish_count.load();
  if (total == 0) return 0;
  const uint64_t target = (total * 95 + 99) / 100;
  uint64_t seen = 0;
  for (size_t i = 0; i < kProjectionPublishP95BucketsUs.size(); ++i) {
    seen += g_receiver_projection_publish_histogram[i].load();
    if (seen >= target) return kProjectionPublishP95BucketsUs[i];
  }
  return kProjectionPublishP95BucketsUs.back();
}

uint64_t preserve_trx_transfer_receiver_projection_lock_wait_us_status() {
  return g_receiver_projection_lock_wait_us.load();
}

uint64_t preserve_trx_transfer_receiver_projection_store_write_us_status() {
  return g_receiver_projection_store_write_us.load();
}

uint64_t preserve_trx_transfer_receiver_projection_marker_write_us_status() {
  return g_receiver_projection_marker_write_us.load();
}

uint64_t preserve_trx_transfer_receiver_projection_snapshot_write_us_status() {
  return g_receiver_projection_snapshot_write_us.load();
}

uint64_t preserve_trx_transfer_receiver_projection_external_blob_us_status() {
  return g_receiver_projection_external_blob_us.load();
}

uint64_t preserve_trx_transfer_receiver_projection_encode_us_status() {
  return g_receiver_projection_encode_us.load();
}

uint64_t preserve_trx_transfer_receiver_projection_token_state_us_status() {
  return g_receiver_projection_token_state_us.load();
}

uint64_t preserve_trx_transfer_receiver_epoch_ready_bind_attempts_status() {
  return g_receiver_epoch_ready_bind_attempts.load();
}

void preserve_trx_transfer_reset_source_phase2_metrics() {
  g_transfer_phase2_bulk_bytes.store(0);
  g_transfer_phase2_snapshot_bundle_bytes.store(0);
  g_transfer_phase2_snapshot_bundle_count.store(0);
  g_transfer_phase2_final_metadata_frame_count.store(0);
  g_transfer_phase2_final_metadata_encoded_bytes.store(0);
  g_transfer_phase2_final_metadata_ack_us.store(0);
}

uint64_t preserve_trx_transfer_phase2_bulk_bytes_status() {
  return g_transfer_phase2_bulk_bytes.load();
}

uint64_t preserve_trx_transfer_phase2_snapshot_bundle_bytes_status() {
  return g_transfer_phase2_snapshot_bundle_bytes.load();
}

uint64_t preserve_trx_transfer_phase2_snapshot_bundle_count_status() {
  return g_transfer_phase2_snapshot_bundle_count.load();
}

uint64_t preserve_trx_transfer_phase2_final_metadata_frame_count_status() {
  return g_transfer_phase2_final_metadata_frame_count.load();
}

uint64_t preserve_trx_transfer_phase2_final_metadata_encoded_bytes_status() {
  return g_transfer_phase2_final_metadata_encoded_bytes.load();
}

uint64_t preserve_trx_transfer_phase2_final_metadata_fsync_count_status() {
  return g_transfer_phase2_final_metadata_fsync_count.load();
}

uint64_t preserve_trx_transfer_phase2_final_metadata_ack_us_status() {
  return g_transfer_phase2_final_metadata_ack_us.load();
}

uint64_t preserve_trx_transfer_receiver_ready_after_final_metadata_us_status() {
  return g_receiver_ready_after_final_metadata_us.load();
}

uint64_t preserve_trx_transfer_receiver_final_spool_ack_monotonic_us_status() {
  return g_receiver_final_spool_ack_monotonic_us.load();
}

uint64_t preserve_trx_transfer_receiver_ready_after_final_spool_ack_us_status() {
  return g_receiver_ready_after_final_spool_ack_us.load();
}

uint64_t preserve_trx_transfer_receiver_prewarm_backlog_at_phase2_end_status() {
  return g_receiver_prewarm_backlog_at_phase2_end.load();
}

uint64_t
preserve_trx_transfer_receiver_record_lock_required_residency_bytes_status() {
  constexpr uint64_t kDefaultInnoDbPageSize = 16384;
  return preserve_trx_promotion_prewarm_record_lock_page_count_status() *
         kDefaultInnoDbPageSize;
}

uint64_t
preserve_trx_transfer_receiver_record_lock_reserved_residency_bytes_status() {
  constexpr uint64_t kDefaultInnoDbPageSize = 16384;
  return preserve_trx_promotion_prewarm_record_lock_resident_pages_status() *
         kDefaultInnoDbPageSize;
}

static bool preserve_trx_transfer_string_is_set(const char *value) {
  return value != nullptr && value[0] != '\0';
}

namespace {
bool preserve_trx_transfer_source_credential_ready();
}

static bool preserve_trx_transfer_source_endpoint_ready() {
  const bool has_tcp_host =
      preserve_trx_transfer_string_is_set(preserve_trx_transfer_target_host);
  const bool has_tcp_port = preserve_trx_transfer_target_port != 0;

  return preserve_trx_transfer_string_is_set(
             preserve_trx_transfer_target_user) &&
         preserve_trx_transfer_source_credential_ready() &&
         has_tcp_host && has_tcp_port;
}

Preserve_trx_transfer_artifact_decision
preserve_trx_transfer_artifact_decision() {
  if (preserve_trx_transfer_artifact_mode ==
      PRESERVE_TRX_TRANSFER_ARTIFACT_LOCAL_CARRIER) {
    return Preserve_trx_transfer_artifact_decision::LOCAL_CARRIER;
  }
  if (!preserve_trx_is_enabled()) {
    return Preserve_trx_transfer_artifact_decision::UNSUPPORTED;
  }
  if (preserve_trx_transfer_artifact_mode ==
          PRESERVE_TRX_TRANSFER_ARTIFACT_STANDBY_TRANSFER_SAVE) {
    if (!preserve_trx_transfer_source_endpoint_ready()) {
      return Preserve_trx_transfer_artifact_decision::UNSUPPORTED;
    }
    return Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE;
  }
  return Preserve_trx_transfer_artifact_decision::UNSUPPORTED;
}

void purge_receiver_epoch_derived_state(const std::string &root_dir,
                                        const std::string &epoch_id,
                                        const std::string &source_uuid);
void purge_receiver_epoch_prewarm_queues(const std::string &root_dir,
                                         const std::string &epoch_id);
bool receiver_epoch_expired_or_removed(
    const std::string &root_dir,
    Preserve_trx_transfer_receiver_registry *registry,
    const Preserve_trx_transfer_manifest &manifest,
    Preserve_trx_temp_id_contract *temp_id_contract = nullptr);

namespace {

bool transfer_component_safe(const std::string &component);

bool transfer_status_is_committed_outcome(
    Preserve_trx_transfer_status status) {
  return status == Preserve_trx_transfer_status::COMMITTED_READY ||
         status == Preserve_trx_transfer_status::COMMITTED_NOT_READY;
}

bool transfer_status_has_authenticated_ack(
    Preserve_trx_transfer_status status) {
  return status == Preserve_trx_transfer_status::OK ||
         transfer_status_is_committed_outcome(status) ||
         status == Preserve_trx_transfer_status::NOT_COMMITTED ||
         status == Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN;
}

constexpr char kTransferManifestMagic[] = {'P', 'T', 'R', 'X',
                                           'O', 'M', 'N', '1'};
constexpr size_t kTransferManifestMagicLength = sizeof(kTransferManifestMagic);
constexpr char kTransferObjectDescriptorMagic[] = {'P', 'T', 'R', 'X',
                                                   'O', 'D', 'V', '1'};
constexpr size_t kTransferObjectDescriptorMagicLength =
    sizeof(kTransferObjectDescriptorMagic);
constexpr uint16_t kTransferObjectDescriptorVersion = 1;
constexpr char kTransferBundleMagic[] = {'P', 'T', 'R', 'X',
                                         'B', 'N', 'D', '1'};
constexpr size_t kTransferBundleMagicLength = sizeof(kTransferBundleMagic);
constexpr char kTransferFrameMagic[] = {'P', 'T', 'R', 'X',
                                        'O', 'F', 'R', '1'};
constexpr size_t kTransferFrameMagicLength = sizeof(kTransferFrameMagic);
constexpr char kTransferFrameBatchMagic[] = {'P', 'T', 'R', 'X',
                                             'O', 'B', 'T', '1'};
constexpr size_t kTransferFrameBatchMagicLength =
    sizeof(kTransferFrameBatchMagic);
constexpr uint32_t kMaxTransferManifestStringBytes = 1024 * 1024;
constexpr uint32_t kMaxTransferManifestObjects = 1024 * 1024;
constexpr uint32_t kMaxTransferChunkBytes = 1024 * 1024;

bool transfer_protocol_version_is_decodable(uint16_t version) {
  return version == kPreserveTrxTransferProtocolVersion;
}

std::string bytes_to_lower_hex(const unsigned char *bytes, size_t length) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out(length * 2, '\0');
  for (size_t i = 0; i < length; ++i) {
    out[2 * i] = kHex[bytes[i] >> 4];
    out[2 * i + 1] = kHex[bytes[i] & 0x0f];
  }
  return out;
}

const std::string &transfer_source_epoch_boot_nonce() {
  static const std::string boot_nonce = []() {
    std::array<unsigned char, 16> random_bytes{};
    if (my_rand_buffer(random_bytes.data(), random_bytes.size()) != 0) {
      return std::string();
    }
    return bytes_to_lower_hex(random_bytes.data(), random_bytes.size());
  }();
  return boot_nonce;
}

std::string make_transfer_epoch_id(const std::string &monotonic_epoch_id) {
  const std::string &boot_nonce = transfer_source_epoch_boot_nonce();
  if (boot_nonce.empty() || monotonic_epoch_id.empty()) return {};
  return boot_nonce + "-" + monotonic_epoch_id;
}

void cleanse_transfer_secret(std::string *value) {
  if (value == nullptr) return;
  if (!value->empty()) OPENSSL_cleanse(&(*value)[0], value->size());
  value->clear();
}

struct Transfer_resolved_credential {
  std::string user;
  std::string password;
  std::string auth_plugin;

  ~Transfer_resolved_credential() { cleanse_transfer_secret(&password); }
};

class Transfer_epoch_password {
 public:
  Transfer_epoch_password(const unsigned char *password, size_t length)
      : m_bytes(password, password + length) {
    m_bytes.push_back(0);
  }

  ~Transfer_epoch_password() {
    if (!m_bytes.empty()) {
      OPENSSL_cleanse(m_bytes.data(), m_bytes.size());
    }
  }

  const unsigned char *data() const { return m_bytes.data(); }
  size_t size() const { return m_bytes.empty() ? 0 : m_bytes.size() - 1; }

 private:
  std::vector<unsigned char> m_bytes;
};

struct Transfer_epoch_credential {
  std::string user;
  std::string auth_plugin;
  std::shared_ptr<const Transfer_epoch_password> password;
};

std::mutex g_transfer_runtime_password_mutex;
std::shared_ptr<const Transfer_epoch_password> g_transfer_runtime_password;
bool g_transfer_runtime_password_managed{false};

bool preserve_trx_transfer_source_credential_ready() {
  std::lock_guard<std::mutex> guard(g_transfer_runtime_password_mutex);
  if (g_transfer_runtime_password_managed) {
    return g_transfer_runtime_password != nullptr;
  }
  return preserve_trx_transfer_string_is_set(
      preserve_trx_transfer_credential_name);
}

Preserve_trx_transfer_password_status
transfer_runtime_password_source_mode_status() {
  if (!preserve_trx_is_enabled()) {
    return Preserve_trx_transfer_password_status::FEATURE_DISABLED;
  }
  if (preserve_trx_transfer_artifact_mode !=
      PRESERVE_TRX_TRANSFER_ARTIFACT_STANDBY_TRANSFER_SAVE) {
    return Preserve_trx_transfer_password_status::WRONG_ROLE;
  }
  return Preserve_trx_transfer_password_status::OK;
}

std::shared_ptr<const Transfer_epoch_password> make_transfer_epoch_password(
    const unsigned char *password, size_t length) {
  try {
    return std::make_shared<const Transfer_epoch_password>(password, length);
  } catch (const std::bad_alloc &) {
    return {};
  }
}

bool transfer_credential_file_metadata_is_secure(uint64_t mode,
                                                 uint64_t owner_uid,
                                                 uint64_t effective_uid) {
  return MY_S_ISREG(mode) && owner_uid == effective_uid &&
         (mode & (S_IRWXG | S_IRWXO)) == 0;
}

bool read_transfer_credential_secret_file(const char *path,
                                          std::string *secret) {
  if (path == nullptr || path[0] == '\0' || secret == nullptr) return false;

  cleanse_transfer_secret(secret);

  constexpr size_t kMaxCredentialSecretBytes = 4096;
  File file =
      my_open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, MYF(0));
  if (file < 0) return false;

  auto close_file = create_scope_guard([&] {
    if (file >= 0) (void)my_close(file, MYF(0));
  });

  MY_STAT opened_stat;
  if (my_fstat(file, &opened_stat) != 0 || opened_stat.st_size <= 0 ||
      static_cast<uint64_t>(opened_stat.st_size) >
          kMaxCredentialSecretBytes ||
      !transfer_credential_file_metadata_is_secure(
          static_cast<uint64_t>(opened_stat.st_mode),
          static_cast<uint64_t>(opened_stat.st_uid),
          static_cast<uint64_t>(geteuid()))) {
    return false;
  }

  const size_t expected_bytes = static_cast<size_t>(opened_stat.st_size);
  std::vector<unsigned char> buffer(expected_bytes);
  auto cleanse_buffer = create_scope_guard([&] {
    if (!buffer.empty()) OPENSSL_cleanse(buffer.data(), buffer.size());
  });
  const size_t bytes =
      my_read(file, buffer.data(), buffer.size(), MYF(MY_FULL_IO));
  const int close_error = my_close(file, MYF(0));
  file = -1;
  if (bytes == MY_FILE_ERROR || bytes != expected_bytes || close_error != 0) {
    return false;
  }

  std::string value(reinterpret_cast<const char *>(buffer.data()), bytes);
  while (!value.empty() &&
         (value.back() == '\n' || value.back() == '\r')) {
    value.pop_back();
  }
  if (value.empty()) return false;
  secret->assign(value.data(), value.size());
  cleanse_transfer_secret(&value);
  return true;
}

bool resolve_transfer_credential(const std::string &credential_name,
                                 const std::string &configured_user,
                                 Transfer_resolved_credential *credential) {
  if (credential == nullptr || credential_name.empty()) return false;

  String_set user;
  String_set pass;
  String_set auth;
  auto cleanse_store_password =
      create_scope_guard([&] { cleanse_transfer_secret(&pass.second); });
  if (Rpl_channel_credentials::get_instance().get_credentials(
          credential_name.c_str(), user, pass, auth) == 0) {
    if (!pass.first || pass.second.empty()) return false;
    if (!configured_user.empty() && user.first && user.second != configured_user) {
      return false;
    }
    if (configured_user.empty()) {
      if (!user.first || user.second.empty()) return false;
      credential->user = user.second;
    } else {
      credential->user = configured_user;
    }
    credential->password = pass.second;
    credential->auth_plugin = auth.first ? auth.second : "";
    return true;
  }

  if (configured_user.empty()) {
    return false;
  }

  std::string secret;
  if (!read_transfer_credential_secret_file(
          preserve_trx_transfer_credential_secret_file, &secret)) {
    return false;
  }

  credential->user = configured_user;
  credential->password.assign(secret.data(), secret.size());
  cleanse_transfer_secret(&secret);
  return true;
}

bool snapshot_transfer_epoch_credential(
    const Preserve_trx_transfer_client_endpoint &endpoint,
    Transfer_epoch_credential *credential) {
  if (credential == nullptr) return false;

  {
    std::lock_guard<std::mutex> guard(g_transfer_runtime_password_mutex);
    if (g_transfer_runtime_password_managed) {
      if (g_transfer_runtime_password == nullptr || endpoint.user.empty()) {
        return false;
      }
      credential->user = endpoint.user;
      credential->auth_plugin = endpoint.auth_plugin;
      credential->password = g_transfer_runtime_password;
      return true;
    }
  }

  Transfer_resolved_credential resolved;
  if (!resolve_transfer_credential(endpoint.credential_name, endpoint.user,
                                   &resolved)) {
    return false;
  }
  std::shared_ptr<const Transfer_epoch_password> fallback_password =
      make_transfer_epoch_password(
          reinterpret_cast<const unsigned char *>(resolved.password.data()),
          resolved.password.size());
  if (fallback_password == nullptr) return false;

  {
    std::lock_guard<std::mutex> guard(g_transfer_runtime_password_mutex);
    if (g_transfer_runtime_password_managed) {
      if (g_transfer_runtime_password == nullptr || endpoint.user.empty()) {
        return false;
      }
      credential->user = endpoint.user;
      credential->auth_plugin = endpoint.auth_plugin;
      credential->password = g_transfer_runtime_password;
      return true;
    }
  }

  credential->user = std::move(resolved.user);
  credential->auth_plugin = std::move(resolved.auth_plugin);
  credential->password = std::move(fallback_password);
  return !credential->user.empty();
}

struct Transfer_client_connection {
  MYSQL *mysql{nullptr};
};

class Transfer_client_current_thd_guard {
 public:
  Transfer_client_current_thd_guard() : m_saved(current_thd) {
    current_thd = nullptr;
  }

  ~Transfer_client_current_thd_guard() { current_thd = m_saved; }

  Transfer_client_current_thd_guard(
      const Transfer_client_current_thd_guard &) = delete;
  Transfer_client_current_thd_guard &operator=(
      const Transfer_client_current_thd_guard &) = delete;

 private:
  THD *m_saved;
};

size_t cleanse_transfer_mysql_password_copies(MYSQL *mysql) {
  if (mysql == nullptr) return 0;
  std::array<char *, 2> copies{{mysql->passwd, mysql->options.password}};
  std::array<char *, 2> cleansed{{nullptr, nullptr}};
  size_t cleansed_count = 0;
  for (char *copy : copies) {
    if (copy == nullptr ||
        std::find(cleansed.begin(), cleansed.end(), copy) != cleansed.end()) {
      continue;
    }
    const size_t length = std::strlen(copy);
    if (length != 0) OPENSSL_cleanse(copy, length);
    cleansed[cleansed_count++] = copy;
  }
  return cleansed_count;
}

void secure_transfer_mysql_close(MYSQL *mysql) {
  if (mysql == nullptr) return;
  mysql->reconnect = false;
  (void)cleanse_transfer_mysql_password_copies(mysql);
  mysql_close(mysql);
}

Preserve_trx_transfer_status default_transfer_client_connect(
    const Preserve_trx_transfer_client_endpoint &endpoint,
    const unsigned char *password, size_t password_length,
    void **connection) {
  if (connection == nullptr || password == nullptr || password_length == 0 ||
      password_length > 256 ||
      std::find(password, password + password_length, '\0') !=
          password + password_length ||
      endpoint.user.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  *connection = nullptr;

  MYSQL *mysql = mysql_init(nullptr);
  if (mysql == nullptr) return Preserve_trx_transfer_status::IO_ERROR;
  auto close_mysql =
      create_scope_guard([&] { secure_transfer_mysql_close(mysql); });

  const uint operation_timeout_ms = endpoint.operation_timeout_ms == 0
                                        ? kPreserveTrxTransferOperationTimeoutMs
                                        : endpoint.operation_timeout_ms;
  uint timeout_seconds = static_cast<uint>(std::max<uint64_t>(
      1, std::min<uint64_t>(
             (static_cast<uint64_t>(operation_timeout_ms) + 999) /
                 1000,
             UINT_MAX32)));
  mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, &timeout_seconds);
  mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, &timeout_seconds);
  mysql_options(mysql, MYSQL_OPT_WRITE_TIMEOUT, &timeout_seconds);
  bool reconnect = false;
  mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);

  if (!endpoint.auth_plugin.empty()) {
    mysql_options(mysql, MYSQL_DEFAULT_AUTH, endpoint.auth_plugin.c_str());
  }
  mysql_options4(mysql, MYSQL_OPT_CONNECT_ATTR_ADD, "program_name", "mysqld");
  mysql_options4(mysql, MYSQL_OPT_CONNECT_ATTR_ADD, "_client_role",
                 "preserve_trx_transfer_sender");

  if (endpoint.host.empty() || endpoint.port == 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  uint protocol = MYSQL_PROTOCOL_TCP;
  if (mysql_options(mysql, MYSQL_OPT_PROTOCOL, &protocol) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  Transfer_client_current_thd_guard current_thd_guard;
  if (mysql_real_connect(
          mysql, endpoint.host.c_str(), endpoint.user.c_str(),
          reinterpret_cast<const char *>(password), nullptr, endpoint.port,
          nullptr, 0) == nullptr) {
    const std::string message =
        "PRESERVE: standby transfer client connect failed errno=" +
        std::to_string(mysql_errno(mysql)) + " error=" + mysql_error(mysql);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  Transfer_client_connection *client = new (std::nothrow) Transfer_client_connection;
  if (client == nullptr) {
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  client->mysql = mysql;
  *connection = client;
  mysql = nullptr;
  close_mysql.commit();
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status verify_transfer_frame_ack(
    const std::string &encoded_ack,
    const std::string &expected_receiver_process_nonce,
    const std::string &encoded_payload, Preserve_trx_transfer_frame_ack *ack);

Preserve_trx_transfer_status default_transfer_client_send(
    void *connection, const std::string &encoded_frame,
    Preserve_trx_transfer_frame_ack *out_ack) {
  Transfer_client_connection *client =
      static_cast<Transfer_client_connection *>(connection);
  if (client == nullptr || client->mysql == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  Transfer_client_current_thd_guard current_thd_guard;
  if (!simple_command(
          client->mysql, COM_PRESERVE_TRX_TRANSFER,
          reinterpret_cast<const unsigned char *>(encoded_frame.data()),
          encoded_frame.length(), 0)) {
    const char *info = client->mysql->info;
    if (info == nullptr || info[0] == '\0') {
      return Preserve_trx_transfer_status::ACK_UNCERTAIN;
    }
    Preserve_trx_transfer_frame_ack ack;
    const Preserve_trx_transfer_status ack_status =
        verify_transfer_frame_ack(info, "", encoded_frame, &ack);
    if (ack_status != Preserve_trx_transfer_status::OK) {
      return Preserve_trx_transfer_status::ACK_UNCERTAIN;
    }
    if (out_ack != nullptr) *out_ack = ack;
    return ack.status;
  }
  const std::string message =
      "PRESERVE: standby transfer client send failed errno=" +
      std::to_string(mysql_errno(client->mysql)) + " error=" +
      mysql_error(client->mysql) + " frame_bytes=" +
      std::to_string(encoded_frame.length());
  LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
  return Preserve_trx_transfer_status::IO_ERROR;
}

Preserve_trx_transfer_status default_transfer_client_set_operation_timeout(
    void *connection, uint operation_timeout_ms) {
  Transfer_client_connection *client =
      static_cast<Transfer_client_connection *>(connection);
  if (client == nullptr || client->mysql == nullptr ||
      client->mysql->net.vio == nullptr || operation_timeout_ms == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const int timeout_seconds = static_cast<int>(std::max<uint64_t>(
      1, std::min<uint64_t>(
             (static_cast<uint64_t>(operation_timeout_ms) + 999) / 1000,
             INT_MAX)));
  if (vio_timeout(client->mysql->net.vio, 0, timeout_seconds) != 0 ||
      vio_timeout(client->mysql->net.vio, 1, timeout_seconds) != 0) {
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  return Preserve_trx_transfer_status::OK;
}

void default_transfer_client_disconnect(void *connection) {
  Transfer_client_connection *client =
      static_cast<Transfer_client_connection *>(connection);
  if (client == nullptr) return;
  if (client->mysql != nullptr) secure_transfer_mysql_close(client->mysql);
  delete client;
}

void default_transfer_client_interrupt(void *connection) {
  Transfer_client_connection *client =
      static_cast<Transfer_client_connection *>(connection);
  if (client == nullptr || client->mysql == nullptr ||
      client->mysql->net.vio == nullptr) {
    return;
  }
  (void)vio_shutdown(client->mysql->net.vio);
}

const Preserve_trx_transfer_client_ops kDefault_transfer_client_ops = {
    default_transfer_client_connect, default_transfer_client_send,
    default_transfer_client_set_operation_timeout,
    default_transfer_client_interrupt,
    default_transfer_client_disconnect};

const Preserve_trx_transfer_client_ops *&unit_transfer_client_ops() {
  static const Preserve_trx_transfer_client_ops *ops = nullptr;
  return ops;
}

const Preserve_trx_transfer_client_ops *configured_transfer_client_ops() {
  const Preserve_trx_transfer_client_ops *ops = unit_transfer_client_ops();
  return ops == nullptr ? &kDefault_transfer_client_ops : ops;
}

Preserve_trx_transfer_codec_context_provider &unit_codec_context_provider() {
  static Preserve_trx_transfer_codec_context_provider provider = nullptr;
  return provider;
}

Preserve_trx_transfer_source_lsn_provider &unit_source_lsn_provider() {
  static Preserve_trx_transfer_source_lsn_provider provider = nullptr;
  return provider;
}

Preserve_trx_transfer_source_trx_id_store_provider &
unit_source_trx_id_store_provider() {
  static Preserve_trx_transfer_source_trx_id_store_provider provider = nullptr;
  return provider;
}

Preserve_trx_transfer_source_resurrection_provider &
unit_source_resurrection_provider() {
  static Preserve_trx_transfer_source_resurrection_provider provider = nullptr;
  return provider;
}

Preserve_trx_transfer_terminal_lock_proof_provider &
unit_terminal_lock_proof_provider() {
  static Preserve_trx_transfer_terminal_lock_proof_provider provider = nullptr;
  return provider;
}

bool transfer_bundle_codec_context_from_config(
    Preserved_trx_codec_context *context) {
  if (context == nullptr) return false;
  context->datadir_fingerprint.fill(0);
  context->server_uuid = "preserve-transfer-product-v1";
  return true;
}

bool transfer_bundle_codec_context(Preserved_trx_codec_context *context) {
  if (context == nullptr) return false;
  Preserve_trx_transfer_codec_context_provider provider =
      unit_codec_context_provider();
  if (provider != nullptr) return provider(context);
  return transfer_bundle_codec_context_from_config(context);
}

bool load_source_transfer_lsn_fact(uint64_t *source_freeze_lsn,
                                   uint64_t *source_epoch_commit_lsn) {
  if (source_freeze_lsn == nullptr || source_epoch_commit_lsn == nullptr) {
    return false;
  }

  Preserve_trx_transfer_source_lsn_provider provider =
      unit_source_lsn_provider();
  if (provider != nullptr) {
    return provider(source_freeze_lsn, source_epoch_commit_lsn) &&
           *source_freeze_lsn != 0 && *source_epoch_commit_lsn != 0;
  }

  const uint64_t lsn = trx_preserve_current_redo_lsn();
  if (lsn == 0) return false;
  *source_freeze_lsn = lsn;
  *source_epoch_commit_lsn = lsn;
  return true;
}

bool transfer_trx_id_store_fact_is_empty(
    const Preserve_trx_transfer_trx_id_store_fact &fact) {
  return fact.source_trx_id_store == 0 &&
         fact.source_trx_id_store_lsn == 0 &&
         fact.source_safe_next_trx_id_floor == 0;
}

bool transfer_trx_id_store_fact_is_valid(
    const Preserve_trx_transfer_trx_id_store_fact &fact,
    uint64_t source_fence_lsn) {
  return trx_preserve_validate_trx_id_store_fact(
      fact.source_trx_id_store, fact.source_trx_id_store_lsn,
      fact.source_safe_next_trx_id_floor, source_fence_lsn);
}

bool load_source_trx_id_store_fact(
    Preserve_trx_transfer_trx_id_store_fact *fact) {
  if (fact == nullptr) return false;
  Preserve_trx_transfer_source_trx_id_store_provider provider =
      unit_source_trx_id_store_provider();
  if (provider != nullptr) {
    return provider(fact) && fact->source_trx_id_store != 0 &&
           fact->source_trx_id_store_lsn != 0 &&
           fact->source_safe_next_trx_id_floor != 0;
  }

  trx_preserve_trx_id_store_fact_t native_fact;
  if (trx_preserve_capture_durable_trx_id_store_fact(&native_fact) !=
      DB_SUCCESS) {
    return false;
  }
  fact->source_trx_id_store = native_fact.store_value;
  fact->source_trx_id_store_lsn = native_fact.redo_commit_lsn;
  fact->source_safe_next_trx_id_floor = native_fact.safe_next_floor;
  return true;
}

Preserve_trx_transfer_status populate_source_commit_proof(
    Preserve_trx_transfer_frame *commit) {
  if (commit == nullptr ||
      commit->type != Preserve_trx_transfer_frame_type::COMMIT_EPOCH) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  Preserve_trx_transfer_trx_id_store_fact store_fact;
  if (!load_source_trx_id_store_fact(&store_fact)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  uint64_t sampled_freeze_lsn = 0;
  uint64_t source_fence_lsn = 0;
  if (!load_source_transfer_lsn_fact(&sampled_freeze_lsn,
                                     &source_fence_lsn) ||
      sampled_freeze_lsn > source_fence_lsn ||
      !transfer_trx_id_store_fact_is_valid(store_fact, source_fence_lsn)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  commit->protocol_version = kPreserveTrxTransferProtocolVersion;
  commit->chunk_offset = source_fence_lsn;
  commit->trx_id_store = store_fact;
  return Preserve_trx_transfer_status::OK;
}

std::mutex &transfer_object_stage_mutex(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id) {
  static std::array<std::mutex, 64> object_mutexes;
  std::string key;
  key.reserve(root_dir.length() + manifest.epoch_id.length() +
              object_id.length() + 48);
  key.append(root_dir);
  key.push_back('\0');
  key.append(manifest.epoch_id);
  key.push_back('\0');
  key.append(std::to_string(manifest.token));
  key.push_back('\0');
  key.append(object_id);
  const size_t shard =
      std::hash<std::string>{}(key) % object_mutexes.size();
  return object_mutexes[shard];
}

Preserve_trx_transfer_client_endpoint configured_transfer_client_endpoint() {
  Preserve_trx_transfer_client_endpoint endpoint;
  endpoint.operation_timeout_ms = kPreserveTrxTransferOperationTimeoutMs;
  if (preserve_trx_transfer_target_host != nullptr) {
    endpoint.host = preserve_trx_transfer_target_host;
  }
  endpoint.port = preserve_trx_transfer_target_port;
  if (preserve_trx_transfer_target_user != nullptr) {
    endpoint.user = preserve_trx_transfer_target_user;
  }
  if (preserve_trx_transfer_credential_name != nullptr) {
    endpoint.credential_name = preserve_trx_transfer_credential_name;
  }
  return endpoint;
}

bool transfer_digest_is_zero(
    const std::array<unsigned char, kPreservedTrxSha256Length> &digest);

bool build_epoch_status_query_payload(const std::string &encoded_payload,
                                      std::string *encoded_query) {
  if (encoded_query == nullptr) return false;
  std::vector<std::string> encoded_frames;
  Preserve_trx_transfer_frame single_frame;
  if (preserve_trx_transfer_decode_frame(encoded_payload, &single_frame) ==
      Preserve_trx_transfer_status::OK) {
    encoded_frames.push_back(encoded_payload);
  } else if (preserve_trx_transfer_decode_frame_batch(encoded_payload,
                                                      &encoded_frames) !=
             Preserve_trx_transfer_status::OK) {
    return false;
  }

  Preserve_trx_transfer_frame commit;
  bool found_commit = false;
  Preserve_trx_transfer_frame decoded;
  for (const std::string &encoded_frame : encoded_frames) {
    if (preserve_trx_transfer_decode_frame(encoded_frame, &decoded) !=
        Preserve_trx_transfer_status::OK) {
      return false;
    }
    if (decoded.type != Preserve_trx_transfer_frame_type::COMMIT_EPOCH) {
      continue;
    }
    if (found_commit) return false;
    commit = decoded;
    found_commit = true;
  }
  if (!found_commit) return false;
  if (transfer_digest_is_zero(commit.terminal_fact_digest)) return false;

  Preserve_trx_transfer_frame query;
  query.type = Preserve_trx_transfer_frame_type::QUERY_EPOCH_STATUS;
  query.protocol_version = kPreserveTrxTransferProtocolVersion;
  query.sequence = commit.sequence;
  query.epoch_id = commit.epoch_id;
  query.receiver_process_nonce = commit.receiver_process_nonce;
  query.token = commit.token;
  query.terminal_fact_digest = commit.terminal_fact_digest;
  return preserve_trx_transfer_encode_frame(query, encoded_query) ==
         Preserve_trx_transfer_status::OK;
}

bool build_epoch_abandon_payload(const std::string &encoded_commit,
                                 std::string *encoded_abandon) {
  if (encoded_abandon == nullptr) return false;
  Preserve_trx_transfer_frame commit;
  if (preserve_trx_transfer_decode_frame(encoded_commit, &commit) !=
          Preserve_trx_transfer_status::OK ||
      commit.type != Preserve_trx_transfer_frame_type::COMMIT_EPOCH ||
      transfer_digest_is_zero(commit.terminal_fact_digest)) {
    return false;
  }

  Preserve_trx_transfer_frame abandon;
  abandon.type =
      Preserve_trx_transfer_frame_type::ABANDON_EPOCH_IF_NOT_COMMITTED;
  abandon.protocol_version = kPreserveTrxTransferProtocolVersion;
  abandon.sequence = commit.sequence;
  abandon.epoch_id = commit.epoch_id;
  abandon.receiver_process_nonce = commit.receiver_process_nonce;
  abandon.token = commit.token;
  abandon.terminal_fact_digest = commit.terminal_fact_digest;
  return preserve_trx_transfer_encode_frame(abandon, encoded_abandon) ==
         Preserve_trx_transfer_status::OK;
}

class Preserve_trx_transfer_client_frame_sink final
    : public Preserve_trx_transfer_encoded_frame_sink {
 public:
  Preserve_trx_transfer_client_frame_sink(
      Preserve_trx_transfer_client_endpoint endpoint,
      std::shared_ptr<const Transfer_epoch_password> password,
      const Preserve_trx_transfer_client_ops *ops, uint32_t data_sessions)
      : m_endpoint(std::move(endpoint)),
        m_password(std::move(password)),
        m_operation_timeout_ms(m_endpoint.operation_timeout_ms),
        m_ops(ops) {
    const size_t session_count = std::max<uint32_t>(1, data_sessions);
    m_connections.resize(session_count, nullptr);
    m_uncertain_payloads.resize(session_count);
    m_operation_mutexes.reserve(session_count);
    m_connection_mutexes.reserve(session_count);
    for (size_t index = 0; index < session_count; ++index) {
      m_operation_mutexes.emplace_back(new std::mutex());
      m_connection_mutexes.emplace_back(new std::mutex());
    }
  }

  ~Preserve_trx_transfer_client_frame_sink() override {
    release_epoch_transport();
  }

  Preserve_trx_transfer_status send_encoded_frame(
      const std::string &encoded_frame) override {
    return send_encoded_frame_on_connection(
        encoded_frame, connection_index_for_frame(encoded_frame), nullptr);
  }

  Preserve_trx_transfer_status send_encoded_frame_on_session(
      const std::string &encoded_frame, size_t session_index) override {
    return send_encoded_frame_on_connection(
        encoded_frame, session_index % m_connections.size(), nullptr);
  }

  Preserve_trx_transfer_status open_epoch_transport(
      const std::string &epoch_id,
      uint64_t requested_terminal_status_retention_us,
      uint64_t absolute_monotonic_deadline_us,
      std::string *receiver_process_nonce,
      uint64_t *accepted_terminal_status_retention_us) override {
    if (receiver_process_nonce == nullptr ||
        accepted_terminal_status_retention_us == nullptr ||
        !transfer_component_safe(epoch_id) ||
        requested_terminal_status_retention_us == 0 ||
        absolute_monotonic_deadline_us <= transfer_monotonic_us()) {
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }
    Preserve_trx_temp_id_contract requested_contract;
    if (!preserve_trx_temp_id_local_contract(&requested_contract)) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    {
      std::lock_guard<std::mutex> guard(m_epoch_context_mutex);
      if (m_epoch_context_bound) {
        if (m_epoch_id != epoch_id ||
            m_requested_temp_id_contract != requested_contract ||
            m_requested_terminal_status_retention_us !=
                requested_terminal_status_retention_us ||
            m_absolute_monotonic_deadline_us !=
                absolute_monotonic_deadline_us) {
          return Preserve_trx_transfer_status::CORRUPT;
        }
        if (!m_receiver_process_nonce.empty()) {
          *receiver_process_nonce = m_receiver_process_nonce;
          *accepted_terminal_status_retention_us =
              m_accepted_terminal_status_retention_us;
          return Preserve_trx_transfer_status::OK;
        }
      } else {
        m_epoch_id = epoch_id;
        m_requested_temp_id_contract = requested_contract;
        m_requested_terminal_status_retention_us =
            requested_terminal_status_retention_us;
        m_absolute_monotonic_deadline_us = absolute_monotonic_deadline_us;
        m_epoch_context_bound = true;
      }
    }
    Preserve_trx_transfer_frame open;
    open.type = Preserve_trx_transfer_frame_type::OPEN_EPOCH;
    open.epoch_id = epoch_id;
    open.temp_id_contract = requested_contract;
    if (!requested_contract.empty()) {
      open.protocol_version = kPreserveTrxTransferOpenContractVersion;
    }
    open.requested_terminal_status_retention_us =
        requested_terminal_status_retention_us;
    std::string encoded_open;
    Preserve_trx_transfer_status status =
        preserve_trx_transfer_encode_frame(open, &encoded_open);
    if (status != Preserve_trx_transfer_status::OK) return status;
    Preserve_trx_transfer_frame_ack ack;
    status = send_encoded_frame_on_connection(encoded_open, 0, &ack);
    if (status != Preserve_trx_transfer_status::OK ||
        ack.protocol_version != open.protocol_version ||
        ack.temp_id_contract != requested_contract ||
        ack.receiver_process_nonce.length() != 32 ||
        ack.accepted_terminal_status_retention_us <
            requested_terminal_status_retention_us) {
      return status == Preserve_trx_transfer_status::OK
                 ? Preserve_trx_transfer_status::CORRUPT
                 : status;
    }
    {
      std::lock_guard<std::mutex> guard(m_epoch_context_mutex);
      m_receiver_process_nonce = ack.receiver_process_nonce;
      m_accepted_temp_id_contract = ack.temp_id_contract;
      m_accepted_terminal_status_retention_us =
          ack.accepted_terminal_status_retention_us;
      *receiver_process_nonce = m_receiver_process_nonce;
      *accepted_terminal_status_retention_us =
          m_accepted_terminal_status_retention_us;
    }
    return Preserve_trx_transfer_status::OK;
  }

  Preserve_trx_temp_id_contract negotiated_temp_id_contract() const override {
    std::lock_guard<std::mutex> guard(m_epoch_context_mutex);
    return m_accepted_temp_id_contract;
  }

  void request_cancel() override {
    m_cancel_requested.store(true, std::memory_order_release);
    if (m_ops == nullptr || m_ops->interrupt == nullptr) return;
    for (size_t index = 0; index < m_connections.size(); ++index) {
      std::lock_guard<std::mutex> connection_guard(
          *m_connection_mutexes[index]);
      void *connection = m_connections[index];
      if (connection != nullptr) m_ops->interrupt(connection);
    }
  }

  Preserve_trx_transfer_status prepare_cancelled_epoch_cleanup() override {
    release_epoch_transport();
    m_reconnect_attempts.store(0);
    m_cancel_requested.store(false, std::memory_order_release);
    return Preserve_trx_transfer_status::OK;
  }

  void release_epoch_transport() override {
    if (m_ops == nullptr || m_ops->disconnect == nullptr) return;
    for (size_t index = 0; index < m_connections.size(); ++index) {
      std::lock_guard<std::mutex> operation_guard(
          *m_operation_mutexes[index]);
      disconnect_owned_connection(index);
      m_uncertain_payloads[index].clear();
    }
  }

 private:
  bool cancel_requested() const {
    return m_cancel_requested.load(std::memory_order_acquire);
  }

  bool disconnect_if_cancelled(size_t connection_index) {
    if (!cancel_requested()) return false;
    disconnect_owned_connection(connection_index);
    return true;
  }

  void *connection_snapshot(size_t connection_index) {
    std::lock_guard<std::mutex> connection_guard(
        *m_connection_mutexes[connection_index]);
    return m_connections[connection_index];
  }

  void disconnect_owned_connection(size_t connection_index) {
    void *connection = nullptr;
    {
      std::lock_guard<std::mutex> connection_guard(
          *m_connection_mutexes[connection_index]);
      connection = m_connections[connection_index];
      m_connections[connection_index] = nullptr;
    }
    if (connection != nullptr && m_ops != nullptr &&
        m_ops->disconnect != nullptr) {
      m_ops->disconnect(connection);
    }
  }

  bool epoch_deadline_expired() const {
    uint64_t absolute_deadline_us = 0;
    {
      std::lock_guard<std::mutex> guard(m_epoch_context_mutex);
      absolute_deadline_us = m_absolute_monotonic_deadline_us;
    }
    return absolute_deadline_us != 0 &&
           transfer_monotonic_us() >= absolute_deadline_us;
  }

  Preserve_trx_transfer_status refresh_connection_operation_timeout(
      size_t connection_index) {
    if (m_ops == nullptr || m_ops->set_operation_timeout == nullptr) {
      return Preserve_trx_transfer_status::OK;
    }
    uint64_t absolute_deadline_us = 0;
    {
      std::lock_guard<std::mutex> guard(m_epoch_context_mutex);
      absolute_deadline_us = m_absolute_monotonic_deadline_us;
    }
    if (absolute_deadline_us == 0) return Preserve_trx_transfer_status::OK;
    const uint64_t now_us = transfer_monotonic_us();
    if (now_us >= absolute_deadline_us) {
      return Preserve_trx_transfer_status::IO_ERROR;
    }
    const uint64_t remaining_ms =
        std::max<uint64_t>(1, (absolute_deadline_us - now_us + 999) / 1000);
    const uint64_t configured_timeout_ms = m_operation_timeout_ms.load();
    const uint timeout_ms = static_cast<uint>(std::min<uint64_t>(
        configured_timeout_ms == 0
            ? remaining_ms
            : std::min<uint64_t>(configured_timeout_ms, remaining_ms),
        UINT_MAX32));
    void *connection = connection_snapshot(connection_index);
    if (connection == nullptr) {
      return Preserve_trx_transfer_status::IO_ERROR;
    }
    return m_ops->set_operation_timeout(connection, timeout_ms);
  }

  Preserve_trx_transfer_status connect_owned_connection(
      size_t connection_index, bool retrying_uncertain) {
    if (cancel_requested()) return Preserve_trx_transfer_status::UNSUPPORTED;
    std::unique_lock<std::mutex> reconnect_guard;
    if (retrying_uncertain) {
      reconnect_guard = std::unique_lock<std::mutex>(m_reconnect_mutex);
      uint reconnect_attempts = m_reconnect_attempts.load();
      while (true) {
        if (reconnect_attempts >= kMaxReconnectAttempts) {
          return Preserve_trx_transfer_status::ACK_UNCERTAIN;
        }
        if (m_reconnect_attempts.compare_exchange_weak(
                reconnect_attempts, reconnect_attempts + 1)) {
          break;
        }
      }
    }
    void *new_connection = nullptr;
    Preserve_trx_transfer_client_endpoint endpoint = m_endpoint;
    endpoint.operation_timeout_ms = m_operation_timeout_ms.load();
    uint64_t absolute_deadline_us = 0;
    {
      std::lock_guard<std::mutex> guard(m_epoch_context_mutex);
      absolute_deadline_us = m_absolute_monotonic_deadline_us;
    }
    if (absolute_deadline_us != 0) {
      const uint64_t now_us = transfer_monotonic_us();
      if (now_us >= absolute_deadline_us) {
        return retrying_uncertain
                   ? Preserve_trx_transfer_status::ACK_UNCERTAIN
                   : Preserve_trx_transfer_status::IO_ERROR;
      }
      const uint64_t remaining_ms =
          std::max<uint64_t>(1, (absolute_deadline_us - now_us + 999) / 1000);
      endpoint.operation_timeout_ms = static_cast<uint>(std::min<uint64_t>(
          endpoint.operation_timeout_ms == 0
              ? remaining_ms
              : std::min<uint64_t>(endpoint.operation_timeout_ms,
                                   remaining_ms),
          UINT_MAX32));
    }
    if (m_password == nullptr || m_password->size() == 0) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    const Preserve_trx_transfer_status connect_status =
        m_ops->connect(endpoint, m_password->data(), m_password->size(),
                       &new_connection);
    if (connect_status != Preserve_trx_transfer_status::OK) {
      if (connect_status == Preserve_trx_transfer_status::UNSUPPORTED) {
        LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
               "preserve standby transfer client transport is not "
               "configured; transfer artifact send is unsupported");
      }
      return retrying_uncertain
                 ? Preserve_trx_transfer_status::ACK_UNCERTAIN
                 : connect_status;
    }
    if (new_connection == nullptr) {
      return retrying_uncertain
                 ? Preserve_trx_transfer_status::ACK_UNCERTAIN
                 : Preserve_trx_transfer_status::IO_ERROR;
    }

    bool install_connection = false;
    {
      std::lock_guard<std::mutex> connection_guard(
          *m_connection_mutexes[connection_index]);
      if (!cancel_requested()) {
        m_connections[connection_index] = new_connection;
        install_connection = true;
      }
    }
    if (!install_connection) {
      m_ops->disconnect(new_connection);
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    return Preserve_trx_transfer_status::OK;
  }

  Preserve_trx_transfer_status send_on_owned_connection(
      size_t connection_index, const std::string &encoded_frame,
      Preserve_trx_transfer_frame_ack *ack) {
    const Preserve_trx_transfer_status timeout_status =
        refresh_connection_operation_timeout(connection_index);
    if (timeout_status != Preserve_trx_transfer_status::OK) {
      return timeout_status;
    }
    void *connection = connection_snapshot(connection_index);
    if (connection == nullptr) {
      return Preserve_trx_transfer_status::IO_ERROR;
    }
#ifndef DBUG_OFF
    if (debug_drop_commit_send(encoded_frame)) {
      return Preserve_trx_transfer_status::ACK_UNCERTAIN;
    }
#endif
    return m_ops->send_frame(connection, encoded_frame, ack);
  }

#ifndef DBUG_OFF
  bool debug_drop_commit_send(const std::string &encoded_frame) {
    Preserve_trx_transfer_frame frame;
    if (preserve_trx_transfer_decode_frame(encoded_frame, &frame) !=
            Preserve_trx_transfer_status::OK ||
        frame.type != Preserve_trx_transfer_frame_type::COMMIT_EPOCH) {
      return false;
    }
    if (!m_debug_commit_send_drop_budget_loaded) {
      m_debug_commit_send_drop_budget_loaded = true;
      DBUG_EXECUTE_IF("preserve_trx_transfer_drop_commit_send_twice",
                      { m_debug_commit_send_drop_remaining = 2; });
    }
    if (m_debug_commit_send_drop_remaining == 0) return false;
    --m_debug_commit_send_drop_remaining;
    DEBUG_SYNC(current_thd, "preserve_trx_transfer_commit_send_dropped");
    return true;
  }

  bool debug_drop_verified_commit_ack(
      const std::string &encoded_frame,
      Preserve_trx_transfer_status status) {
    if (!transfer_status_has_authenticated_ack(status)) return false;
    Preserve_trx_transfer_frame frame;
    if (preserve_trx_transfer_decode_frame(encoded_frame, &frame) !=
            Preserve_trx_transfer_status::OK ||
        frame.type != Preserve_trx_transfer_frame_type::COMMIT_EPOCH) {
      return false;
    }
    if (!m_debug_commit_ack_drop_budget_loaded) {
      m_debug_commit_ack_drop_budget_loaded = true;
      DBUG_EXECUTE_IF("preserve_trx_transfer_drop_commit_ack_once",
                      { m_debug_commit_ack_drop_remaining = 1; });
      DBUG_EXECUTE_IF("preserve_trx_transfer_drop_commit_ack_twice",
                      { m_debug_commit_ack_drop_remaining = 2; });
    }
    if (m_debug_commit_ack_drop_remaining == 0) return false;
    --m_debug_commit_ack_drop_remaining;
    DEBUG_SYNC(current_thd,
               "preserve_trx_transfer_verified_commit_ack_dropped");
    return true;
  }
#endif

  Preserve_trx_transfer_status send_encoded_frame_on_connection(
      const std::string &encoded_frame, size_t connection_index,
      Preserve_trx_transfer_frame_ack *out_ack) {
    if (m_ops == nullptr || m_ops->connect == nullptr ||
        m_ops->send_frame == nullptr || m_ops->disconnect == nullptr) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    std::lock_guard<std::mutex> operation_guard(
        *m_operation_mutexes[connection_index]);
    if (cancel_requested()) return Preserve_trx_transfer_status::UNSUPPORTED;

    std::string &uncertain_payload = m_uncertain_payloads[connection_index];
    std::string suspended_payload;
    bool abandon_clean = false;
    auto restore_uncertainty = create_scope_guard([&] {
      if (!suspended_payload.empty() && !abandon_clean) {
        uncertain_payload.swap(suspended_payload);
      }
    });
    if (!uncertain_payload.empty() && uncertain_payload != encoded_frame) {
      Preserve_trx_transfer_frame abandon;
      if (preserve_trx_transfer_decode_frame(encoded_frame, &abandon) !=
              Preserve_trx_transfer_status::OK ||
          abandon.type != Preserve_trx_transfer_frame_type::
                              ABANDON_EPOCH_IF_NOT_COMMITTED) {
        return Preserve_trx_transfer_status::ACK_UNCERTAIN;
      }
      {
        std::lock_guard<std::mutex> guard(m_epoch_context_mutex);
        if (!m_epoch_context_bound || m_receiver_process_nonce.empty() ||
            abandon.epoch_id != m_epoch_id ||
            abandon.receiver_process_nonce != m_receiver_process_nonce) {
          return Preserve_trx_transfer_status::ACK_UNCERTAIN;
        }
      }
      // Terminal CAS does not reuse the uncertain data sequence. Preserve the
      // original ACK debt until the receiver authenticates complete cleanup.
      uncertain_payload.swap(suspended_payload);
      disconnect_owned_connection(connection_index);
    }
    const bool retrying_uncertain = !uncertain_payload.empty();
    if (epoch_deadline_expired()) {
      disconnect_owned_connection(connection_index);
      return retrying_uncertain
                 ? Preserve_trx_transfer_status::ACK_UNCERTAIN
                 : Preserve_trx_transfer_status::IO_ERROR;
    }
    if (connection_snapshot(connection_index) == nullptr) {
      const Preserve_trx_transfer_status connect_status =
          connect_owned_connection(connection_index, retrying_uncertain);
      if (connect_status != Preserve_trx_transfer_status::OK) {
        return connect_status;
      }
    }
    if (disconnect_if_cancelled(connection_index)) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }

    Preserve_trx_transfer_frame_ack ack;
    Preserve_trx_transfer_status status =
        send_on_owned_connection(connection_index, encoded_frame, &ack);
    if (disconnect_if_cancelled(connection_index)) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    if (status != Preserve_trx_transfer_status::IO_ERROR &&
        status != Preserve_trx_transfer_status::ACK_UNCERTAIN) {
      if (transfer_status_has_authenticated_ack(status)) {
        if (!ack_matches_epoch(ack)) {
          disconnect_owned_connection(connection_index);
          return Preserve_trx_transfer_status::CORRUPT;
        }
#ifndef DBUG_OFF
        if (debug_drop_verified_commit_ack(encoded_frame, status)) {
          status = Preserve_trx_transfer_status::ACK_UNCERTAIN;
        } else
#endif
        {
          if (out_ack != nullptr) *out_ack = ack;
          uncertain_payload.clear();
          abandon_clean =
              status == Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN;
          return status;
        }
      }
      if (status != Preserve_trx_transfer_status::ACK_UNCERTAIN) return status;
    }

    /* An uncertain response retries the exact encoded payload and sequence. */
    disconnect_owned_connection(connection_index);
    if (disconnect_if_cancelled(connection_index)) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    status = connect_owned_connection(connection_index, true);
    if (status != Preserve_trx_transfer_status::OK) {
      if (cancel_requested() ||
          status == Preserve_trx_transfer_status::UNSUPPORTED) {
        return Preserve_trx_transfer_status::UNSUPPORTED;
      }
      uncertain_payload = encoded_frame;
      return Preserve_trx_transfer_status::ACK_UNCERTAIN;
    }
    if (disconnect_if_cancelled(connection_index)) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    ack = Preserve_trx_transfer_frame_ack();
    status = send_on_owned_connection(connection_index, encoded_frame, &ack);
    if (disconnect_if_cancelled(connection_index)) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    if (transfer_status_has_authenticated_ack(status)) {
      if (!ack_matches_epoch(ack)) {
        disconnect_owned_connection(connection_index);
        return Preserve_trx_transfer_status::CORRUPT;
      }
#ifndef DBUG_OFF
      if (debug_drop_verified_commit_ack(encoded_frame, status)) {
        status = Preserve_trx_transfer_status::ACK_UNCERTAIN;
      } else
#endif
      {
        if (out_ack != nullptr) *out_ack = ack;
        uncertain_payload.clear();
        abandon_clean =
            status == Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN;
        return status;
      }
    }
    if (status == Preserve_trx_transfer_status::IO_ERROR ||
        status == Preserve_trx_transfer_status::ACK_UNCERTAIN) {
      std::string encoded_query;
      if (disconnect_if_cancelled(connection_index)) {
        return Preserve_trx_transfer_status::UNSUPPORTED;
      }
      if (build_epoch_status_query_payload(encoded_frame, &encoded_query)) {
        disconnect_owned_connection(connection_index);
        const Preserve_trx_transfer_status reconnect_status =
            connect_owned_connection(connection_index, true);
        if (reconnect_status == Preserve_trx_transfer_status::OK) {
          if (disconnect_if_cancelled(connection_index)) {
            return Preserve_trx_transfer_status::UNSUPPORTED;
          }
          const Preserve_trx_transfer_status query_status =
              send_on_owned_connection(connection_index, encoded_query,
                                       &ack);
          if (disconnect_if_cancelled(connection_index)) {
            return Preserve_trx_transfer_status::UNSUPPORTED;
          }
          if (transfer_status_has_authenticated_ack(query_status)) {
            if (!ack_matches_epoch(ack)) {
              disconnect_owned_connection(connection_index);
              return Preserve_trx_transfer_status::CORRUPT;
            }
            if (transfer_status_is_committed_outcome(query_status) ||
                query_status ==
                    Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN) {
              if (out_ack != nullptr) *out_ack = ack;
              uncertain_payload.clear();
              return query_status;
            }
            if (query_status ==
                Preserve_trx_transfer_status::NOT_COMMITTED) {
              std::string encoded_abandon;
              if (build_epoch_abandon_payload(encoded_frame,
                                              &encoded_abandon)) {
                ack = Preserve_trx_transfer_frame_ack();
                const Preserve_trx_transfer_status abandon_status =
                    send_on_owned_connection(connection_index,
                                             encoded_abandon, &ack);
                if (transfer_status_has_authenticated_ack(abandon_status) &&
                    ack_matches_epoch(ack) &&
                    (abandon_status ==
                         Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN ||
                     transfer_status_is_committed_outcome(abandon_status))) {
                  if (out_ack != nullptr) *out_ack = ack;
                  uncertain_payload.clear();
                  return abandon_status;
                }
              }
            }
          }
        } else if (cancel_requested() ||
                   reconnect_status ==
                       Preserve_trx_transfer_status::UNSUPPORTED) {
          return Preserve_trx_transfer_status::UNSUPPORTED;
        }
      }
      uncertain_payload = encoded_frame;
      return Preserve_trx_transfer_status::ACK_UNCERTAIN;
    }
    return status;
  }

  bool ack_matches_epoch(const Preserve_trx_transfer_frame_ack &ack) const {
    std::lock_guard<std::mutex> guard(m_epoch_context_mutex);
    return m_receiver_process_nonce.empty() ||
           ack.receiver_process_nonce == m_receiver_process_nonce;
  }

  void set_operation_timeout_ms(uint timeout_ms) override {
    if (timeout_ms == 0 || m_operation_timeout_ms.load() == timeout_ms) return;
    m_operation_timeout_ms.store(timeout_ms);
    if (m_ops != nullptr && m_ops->disconnect != nullptr) {
      for (size_t index = 0; index < m_connections.size(); ++index) {
        std::lock_guard<std::mutex> operation_guard(
            *m_operation_mutexes[index]);
        disconnect_owned_connection(index);
      }
    }
  }

  size_t connection_index_for_frame(const std::string &encoded_frame) {
    if (m_connections.size() <= 1) return 0;
    std::vector<std::string> encoded_frames;
    Preserve_trx_transfer_frame single;
    if (preserve_trx_transfer_decode_frame(encoded_frame, &single) ==
        Preserve_trx_transfer_status::OK) {
      encoded_frames.push_back(encoded_frame);
    } else if (preserve_trx_transfer_decode_frame_batch(encoded_frame,
                                                        &encoded_frames) !=
               Preserve_trx_transfer_status::OK) {
      return 0;
    }
    uint64_t first_token = 0;
    for (const std::string &frame_bytes : encoded_frames) {
      Preserve_trx_transfer_frame frame;
      if (preserve_trx_transfer_decode_frame(frame_bytes, &frame) !=
          Preserve_trx_transfer_status::OK) {
        return 0;
      }
      if (frame.type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH ||
          frame.type ==
              Preserve_trx_transfer_frame_type::QUERY_EPOCH_STATUS ||
          frame.type ==
              Preserve_trx_transfer_frame_type::
                  ABANDON_EPOCH_IF_NOT_COMMITTED ||
          frame.type ==
              Preserve_trx_transfer_frame_type::PROMOTION_GATE_EPOCH) {
        return 0;
      }
      if (first_token == 0) first_token = frame.token;
    }
    return first_token == 0 ? 0 : first_token % m_connections.size();
  }

  Preserve_trx_transfer_client_endpoint m_endpoint;
  static constexpr uint kMaxReconnectAttempts = 3;
  std::shared_ptr<const Transfer_epoch_password> m_password;
  std::atomic<uint> m_operation_timeout_ms{0};
  const Preserve_trx_transfer_client_ops *m_ops{nullptr};
  std::vector<void *> m_connections;
  std::vector<std::unique_ptr<std::mutex>> m_operation_mutexes;
  std::vector<std::unique_ptr<std::mutex>> m_connection_mutexes;
  std::vector<std::string> m_uncertain_payloads;
  mutable std::mutex m_epoch_context_mutex;
  std::mutex m_reconnect_mutex;
  std::string m_epoch_id;
  bool m_epoch_context_bound{false};
  Preserve_trx_temp_id_contract m_requested_temp_id_contract;
  Preserve_trx_temp_id_contract m_accepted_temp_id_contract;
  uint64_t m_requested_terminal_status_retention_us{0};
  std::string m_receiver_process_nonce;
  uint64_t m_accepted_terminal_status_retention_us{0};
  uint64_t m_absolute_monotonic_deadline_us{0};
  std::atomic<uint> m_reconnect_attempts{0};
  std::atomic<bool> m_cancel_requested{false};
#ifndef DBUG_OFF
  bool m_debug_commit_send_drop_budget_loaded{false};
  uint m_debug_commit_send_drop_remaining{0};
  bool m_debug_commit_ack_drop_budget_loaded{false};
  uint m_debug_commit_ack_drop_remaining{0};
#endif
};

std::string normalize_dir(const std::string &dir) {
  if (dir.empty()) return dir;
  if (dir.back() == FN_LIBCHAR) return dir;
  return dir + FN_LIBCHAR;
}

std::string join_path(const std::string &dir, const std::string &name) {
  return normalize_dir(dir) + name;
}

std::string strip_trailing_directory_separators(std::string path) {
  while (path.size() > 1 &&
         (path.back() == FN_LIBCHAR
#ifdef _WIN32
          || path.back() == FN_LIBCHAR2
#endif
          )) {
    path.pop_back();
  }
  return path;
}

bool transfer_component_safe(const std::string &component) {
  if (component.empty() || component.length() > 128) return false;
  if (component == "." || component == "..") return false;
  return std::all_of(component.begin(), component.end(), [](unsigned char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= 'a' && ch <= 'z') || ch == '_' || ch == '-' || ch == '.';
  });
}

std::string transfer_token_component(uint64_t token) {
  return token == 0 ? std::string() : std::to_string(token);
}

bool is_dot_or_dotdot(const char *name) {
  return name != nullptr &&
         ((name[0] == '.' && name[1] == '\0') ||
          (name[0] == '.' && name[1] == '.' && name[2] == '\0'));
}

bool ensure_dir_exists(const std::string &dir) {
  if (my_mkdir(dir.c_str(), 0700, MYF(0)) == 0) return false;
  return my_errno() != EEXIST;
}

bool file_exists(const std::string &path, MY_STAT *stat_area = nullptr) {
  MY_STAT local_stat;
  return my_stat(path.c_str(), stat_area != nullptr ? stat_area : &local_stat,
                 MYF(0)) != nullptr;
}

std::string transfer_epoch_dir(const std::string &root_dir,
                               const Preserve_trx_transfer_manifest &manifest) {
  return join_path(join_path(root_dir, ".transfer"), manifest.epoch_id);
}

std::string transfer_epoch_dir_for_epoch(const std::string &root_dir,
                                         const std::string &epoch_id) {
  return join_path(join_path(root_dir, ".transfer"), epoch_id);
}

std::string transfer_token_dir(const std::string &root_dir,
                               const Preserve_trx_transfer_manifest &manifest) {
  return join_path(transfer_epoch_dir(root_dir, manifest),
                   transfer_token_component(manifest.token));
}

std::string transfer_epoch_commit_path(
    const std::string &root_dir, const Preserve_trx_transfer_manifest &manifest) {
  return join_path(transfer_epoch_dir(root_dir, manifest), "epoch.commit");
}

std::string transfer_epoch_fact_path(const std::string &root_dir,
                                     const std::string &epoch_id) {
  return join_path(transfer_epoch_dir_for_epoch(root_dir, epoch_id),
                   "epoch.fact");
}

std::string transfer_object_path(
    const std::string &root_dir, const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &object) {
  return join_path(transfer_token_dir(root_dir, manifest),
                   object.object_id + ".part");
}

std::string transfer_object_range_path(
    const std::string &root_dir, const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &object) {
  return join_path(transfer_token_dir(root_dir, manifest),
                   object.object_id + ".ranges");
}

Preserve_trx_transfer_status ensure_transfer_token_dir(
    const std::string &root_dir, const Preserve_trx_transfer_manifest &manifest) {
  if (root_dir.empty() || !transfer_component_safe(manifest.epoch_id) ||
      manifest.token == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const std::string transfer_root = join_path(root_dir, ".transfer");
  if (ensure_dir_exists(transfer_root) ||
      ensure_dir_exists(transfer_epoch_dir(root_dir, manifest)) ||
      ensure_dir_exists(transfer_token_dir(root_dir, manifest))) {
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status cleanup_transfer_token_staging(
    const std::string &root_dir, const std::string &epoch_id,
    uint64_t token) {
  const std::string token_component = transfer_token_component(token);
  if (root_dir.empty() || !transfer_component_safe(epoch_id) ||
      !transfer_component_safe(token_component)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  uint remaining =
      g_transfer_staging_cleanup_failures_for_unit_test.load();
  while (remaining != 0 &&
         !g_transfer_staging_cleanup_failures_for_unit_test
              .compare_exchange_weak(remaining, remaining - 1)) {
  }
  if (remaining != 0) return Preserve_trx_transfer_status::IO_ERROR;

  preserved_trx_receiver_binlog_prefix_erase(root_dir, epoch_id, token);
  const std::string epoch_dir =
      join_path(join_path(root_dir, ".transfer"), epoch_id);
  const std::string token_dir = join_path(epoch_dir, token_component);
  MY_DIR *dir_info =
      my_dir(token_dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir_info == nullptr) {
    return my_errno() == ENOENT ? Preserve_trx_transfer_status::OK
                                : Preserve_trx_transfer_status::IO_ERROR;
  }

  Preserve_trx_transfer_status status = Preserve_trx_transfer_status::OK;
  for (uint idx = 0; idx < dir_info->number_off_files; ++idx) {
    FILEINFO *file = dir_info->dir_entry + idx;
    if (file == nullptr || is_dot_or_dotdot(file->name)) continue;

    const std::string name(file->name);
    if (!transfer_component_safe(name) || file->mystat == nullptr ||
        !MY_S_ISREG(file->mystat->st_mode) ||
        my_delete(join_path(token_dir, name).c_str(), MYF(0)) != 0) {
      status = Preserve_trx_transfer_status::IO_ERROR;
      break;
    }
  }
  my_dirend(dir_info);
  if (status != Preserve_trx_transfer_status::OK) return status;

  if (rmdir(token_dir.c_str()) != 0 && errno != ENOENT) {
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  (void)rmdir(epoch_dir.c_str());
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status cleanup_transfer_object_staging(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &object) {
  if (root_dir.empty() || !transfer_component_safe(manifest.epoch_id) ||
      manifest.token == 0 || !transfer_component_safe(object.object_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  if (object.object_id == kPreservedTrxBlobBinlogCache)
    preserved_trx_receiver_binlog_prefix_erase(
        root_dir, manifest.epoch_id, manifest.token);
  for (const std::string &path :
       {transfer_object_path(root_dir, manifest, object),
        transfer_object_range_path(root_dir, manifest, object)}) {
    if (my_delete(path.c_str(), MYF(0)) != 0 && my_errno() != ENOENT) {
      return Preserve_trx_transfer_status::IO_ERROR;
    }
  }
  return Preserve_trx_transfer_status::OK;
}

bool transfer_filename_has_suffix(const std::string &name,
                                  const char *suffix) {
  const size_t suffix_length = std::strlen(suffix);
  return name.length() >= suffix_length &&
         name.compare(name.length() - suffix_length, suffix_length, suffix) ==
             0;
}

bool transfer_restart_token_component(const std::string &name) {
  return !name.empty() && name != "0" && name.length() <= 20 &&
         std::all_of(name.begin(), name.end(),
                     [](char value) { return value >= '0' && value <= '9'; });
}

Preserve_trx_transfer_status collect_receiver_restart_transfer_tokens(
    const std::string &transfer_root, std::set<std::string> *tokens) {
  if (tokens == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  MY_DIR *root_info =
      my_dir(transfer_root.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (root_info == nullptr) {
    return my_errno() == ENOENT ? Preserve_trx_transfer_status::OK
                                : Preserve_trx_transfer_status::IO_ERROR;
  }
  Preserve_trx_transfer_status status = Preserve_trx_transfer_status::OK;
  for (uint index = 0; index < root_info->number_off_files; ++index) {
    FILEINFO *epoch = root_info->dir_entry + index;
    if (epoch == nullptr || is_dot_or_dotdot(epoch->name) ||
        epoch->mystat == nullptr || !MY_S_ISDIR(epoch->mystat->st_mode)) {
      continue;
    }
    const std::string epoch_dir = join_path(transfer_root, epoch->name);
    MY_DIR *epoch_info =
        my_dir(epoch_dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
    if (epoch_info == nullptr) {
      status = Preserve_trx_transfer_status::IO_ERROR;
      break;
    }
    for (uint token_index = 0; token_index < epoch_info->number_off_files;
         ++token_index) {
      FILEINFO *token = epoch_info->dir_entry + token_index;
      if (token == nullptr || token->mystat == nullptr ||
          !MY_S_ISDIR(token->mystat->st_mode) ||
          !transfer_restart_token_component(token->name)) {
        continue;
      }
      tokens->insert(token->name);
    }
    my_dirend(epoch_info);
  }
  my_dirend(root_info);
  return status;
}

Preserve_trx_transfer_status remove_receiver_restart_tree(
    const std::string &path, uint depth, bool keep_temp_namespace = false) {
  struct stat stat_area;
  if (lstat(path.c_str(), &stat_area) != 0) {
    return errno == ENOENT ? Preserve_trx_transfer_status::OK
                           : Preserve_trx_transfer_status::IO_ERROR;
  }
  if (S_ISLNK(stat_area.st_mode) || S_ISREG(stat_area.st_mode)) {
    return unlink(path.c_str()) == 0 || errno == ENOENT
               ? Preserve_trx_transfer_status::OK
               : Preserve_trx_transfer_status::IO_ERROR;
  }
  if (!S_ISDIR(stat_area.st_mode) || depth > 4) {
    return Preserve_trx_transfer_status::IO_ERROR;
  }

  MY_DIR *dir_info = my_dir(path.c_str(), MYF(MY_DONT_SORT));
  if (dir_info == nullptr) {
    return my_errno() == ENOENT ? Preserve_trx_transfer_status::OK
                                : Preserve_trx_transfer_status::IO_ERROR;
  }
  Preserve_trx_transfer_status status = Preserve_trx_transfer_status::OK;
  bool kept_child = false;
  for (uint index = 0; index < dir_info->number_off_files; ++index) {
    FILEINFO *entry = dir_info->dir_entry + index;
    if (entry == nullptr || is_dot_or_dotdot(entry->name)) continue;
    if (keep_temp_namespace &&
        preserve_trx_temp_gc_private_entry(entry->name)) {
      kept_child = true;
      continue;
    }
    const Preserve_trx_transfer_status child_status =
        remove_receiver_restart_tree(join_path(path, entry->name), depth + 1);
    if (status == Preserve_trx_transfer_status::OK &&
        child_status != Preserve_trx_transfer_status::OK) {
      status = child_status;
    }
  }
  my_dirend(dir_info);
  if (!kept_child && rmdir(path.c_str()) != 0 && errno != ENOENT &&
      status == Preserve_trx_transfer_status::OK) {
    status = Preserve_trx_transfer_status::IO_ERROR;
  }
  return status;
}

Preserve_trx_transfer_status remove_receiver_restart_promotion_markers(
    const std::string &root_dir) {
  MY_DIR *dir_info =
      my_dir(root_dir.c_str(), MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (dir_info == nullptr) {
    return my_errno() == ENOENT ? Preserve_trx_transfer_status::OK
                                : Preserve_trx_transfer_status::IO_ERROR;
  }
  Preserve_trx_transfer_status status = Preserve_trx_transfer_status::OK;
  for (uint index = 0; index < dir_info->number_off_files; ++index) {
    FILEINFO *entry = dir_info->dir_entry + index;
    if (entry == nullptr || is_dot_or_dotdot(entry->name)) continue;
    const std::string name(entry->name);
    const bool promotion_marker =
        transfer_filename_has_suffix(name, ".promotion_adopted") ||
        transfer_filename_has_suffix(name, ".promotion_adopted.tmp") ||
        transfer_filename_has_suffix(name, ".promotion_abandoned") ||
        transfer_filename_has_suffix(name, ".promotion_abandoned.tmp") ||
        transfer_filename_has_suffix(name, ".promotion_intent") ||
        transfer_filename_has_suffix(name, ".promotion_intent.tmp");
    if (!promotion_marker) continue;
    struct stat stat_area;
    const std::string path = join_path(root_dir, name);
    if (lstat(path.c_str(), &stat_area) != 0 ||
        (!S_ISREG(stat_area.st_mode) && !S_ISLNK(stat_area.st_mode)) ||
        (unlink(path.c_str()) != 0 && errno != ENOENT)) {
      status = Preserve_trx_transfer_status::IO_ERROR;
      break;
    }
  }
  my_dirend(dir_info);
  return status;
}

const Preserve_trx_transfer_object_descriptor *find_object(
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id) {
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (object.object_id == object_id) return &object;
  }
  return nullptr;
}

bool transfer_digest_is_zero(
    const std::array<unsigned char, kPreservedTrxSha256Length> &digest) {
  return std::all_of(digest.begin(), digest.end(),
                     [](unsigned char value) { return value == 0; });
}

bool transfer_lock_plan_contract_equal(
    const Preserve_trx_transfer_lock_plan_contract &left,
    const Preserve_trx_transfer_lock_plan_contract &right) {
  return left.version == right.version &&
         left.source_live_generation == right.source_live_generation &&
         left.source_live_digest == right.source_live_digest &&
         left.record_store_fingerprint == right.record_store_fingerprint &&
         left.simulated_terminal_proof == right.simulated_terminal_proof &&
         left.terminal_proof == right.terminal_proof;
}

bool transfer_lock_plan_contract_valid(
    const Preserve_trx_transfer_object_descriptor &object) {
  const Preserve_trx_transfer_lock_plan_contract &contract = object.lock_plan;
  if (contract.version == 0) {
    return contract.source_live_generation == 0 &&
           transfer_digest_is_zero(contract.source_live_digest) &&
           transfer_digest_is_zero(contract.record_store_fingerprint) &&
           !contract.simulated_terminal_proof &&
           transfer_digest_is_zero(contract.terminal_proof);
  }
  if (contract.version != kPreserveTrxTransferLockPlanContractVersion ||
      object.object_id != kPreservedTrxBlobRecordLocks ||
      object.kind != Preserve_trx_transfer_object_kind::EXTERNAL_BLOB ||
      contract.source_live_generation == 0 ||
      transfer_digest_is_zero(contract.source_live_digest) ||
      transfer_digest_is_zero(contract.record_store_fingerprint)) {
    return false;
  }
  return contract.simulated_terminal_proof
             ? !transfer_digest_is_zero(contract.terminal_proof)
             : transfer_digest_is_zero(contract.terminal_proof);
}

Preserve_trx_transfer_status transfer_lock_plan_replacement_status(
    const Preserve_trx_transfer_object_descriptor &current,
    const Preserve_trx_transfer_object_descriptor &replacement) {
  const auto &old_contract = current.lock_plan;
  const auto &new_contract = replacement.lock_plan;
  if (old_contract.version == 0 && new_contract.version == 0) {
    return Preserve_trx_transfer_status::OK;
  }
  if (old_contract.version == 0) return Preserve_trx_transfer_status::OK;
  if (new_contract.version == 0 ||
      new_contract.source_live_generation <
          old_contract.source_live_generation) {
    return Preserve_trx_transfer_status::LOCK_PLAN_STALE;
  }
  if (new_contract.source_live_generation ==
      old_contract.source_live_generation) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status validate_manifest_components(
    const Preserve_trx_transfer_manifest &manifest,
    bool decoded_remote_manifest) {
  if (!transfer_component_safe(manifest.epoch_id) ||
      manifest.token == 0 ||
      manifest.objects.size() > kMaxTransferManifestObjects) {
    return decoded_remote_manifest ? Preserve_trx_transfer_status::CORRUPT
                                   : Preserve_trx_transfer_status::
                                         INVALID_ARGUMENT;
  }

  if (manifest.protocol_version != kPreserveTrxTransferProtocolVersion ||
      (manifest.strict_eligibility_flags &
       ~kPreserveTrxTransferStrictEligibilityKnownFlags) != 0) {
    return decoded_remote_manifest ? Preserve_trx_transfer_status::CORRUPT
                                   : Preserve_trx_transfer_status::
                                         INVALID_ARGUMENT;
  }

  std::set<std::string> object_ids;
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (!transfer_component_safe(object.object_id) ||
        !object_ids.insert(object.object_id).second ||
        !transfer_lock_plan_contract_valid(object)) {
      return decoded_remote_manifest ? Preserve_trx_transfer_status::CORRUPT
                                     : Preserve_trx_transfer_status::
                                           INVALID_ARGUMENT;
    }
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status transfer_manifest_inflight_bytes(
    const Preserve_trx_transfer_manifest &manifest,
    size_t manifest_payload_length, uint64_t *inflight_bytes) {
  if (inflight_bytes == nullptr)
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  uint64_t total = manifest_payload_length;
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (object.total_size > std::numeric_limits<uint64_t>::max() - total) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    total += object.total_size;
  }
  *inflight_bytes = total;
  return Preserve_trx_transfer_status::OK;
}

constexpr uint64_t kReceiverObjectReservationOverhead = 256;

Preserve_trx_transfer_status receiver_object_reserved_bytes(
    const Preserve_trx_transfer_object_descriptor &object,
    uint64_t *reserved_bytes) {
  if (reserved_bytes == nullptr)
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  if (object.total_size > std::numeric_limits<uint64_t>::max() -
                              kReceiverObjectReservationOverhead) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  *reserved_bytes = object.total_size + kReceiverObjectReservationOverhead;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status receiver_manifest_reserved_bytes(
    const Preserve_trx_transfer_manifest &manifest,
    uint64_t manifest_payload_bytes, uint64_t *reserved_bytes) {
  if (reserved_bytes == nullptr)
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  uint64_t total = manifest_payload_bytes;
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    uint64_t object_bytes = 0;
    const Preserve_trx_transfer_status status =
        receiver_object_reserved_bytes(object, &object_bytes);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (object_bytes > std::numeric_limits<uint64_t>::max() - total) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    total += object_bytes;
  }
  *reserved_bytes = total;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status read_existing_overlap(
    File file, uint64_t offset, size_t length,
    std::string *existing) {
  if (existing == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  existing->clear();
  if (length == 0) return Preserve_trx_transfer_status::OK;
  bool error = false;
  if (my_seek(file, offset, MY_SEEK_SET, MYF(0)) == MY_FILEPOS_ERROR) {
    error = true;
  } else {
    existing->resize(length);
    const size_t read_len =
        my_read(file, reinterpret_cast<unsigned char *>(&(*existing)[0]),
                length, MYF(0));
    if (read_len != length) error = true;
  }
  return error ? Preserve_trx_transfer_status::IO_ERROR
               : Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status append_range_to_file(const std::string &path,
                                                  uint64_t offset,
                                                  uint64_t length) {
  if (length == 0) return Preserve_trx_transfer_status::OK;
  const std::string record =
      std::to_string(offset) + " " + std::to_string(length) + "\n";
  File file = my_open(path.c_str(), O_WRONLY | O_APPEND, MYF(0));
  if (file < 0) {
    file = my_create(path.c_str(), 0600, O_WRONLY | O_CREAT | O_EXCL, MYF(0));
    if (file < 0) {
      if (my_errno() == EEXIST) {
        file = my_open(path.c_str(), O_WRONLY | O_APPEND, MYF(0));
      }
      if (file < 0) return Preserve_trx_transfer_status::IO_ERROR;
    }
  }
  const bool error =
      my_write(file, reinterpret_cast<const unsigned char *>(record.data()),
               record.length(), MYF(0)) != record.length();
  const bool close_error = my_close(file, MYF(0));
  return (error || close_error) ? Preserve_trx_transfer_status::IO_ERROR
                                : Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status read_whole_file(const std::string &path,
                                             std::string *payload);

std::string commit_marker_payload(
    const Preserve_trx_transfer_manifest &manifest) {
  return "PTRXFER_COMMIT_V1\n" + manifest.epoch_id + "\n";
}

std::string transfer_unique_tmp_path(const std::string &final_path,
                                     const std::string &component) {
  return final_path + "." + component + "." + std::to_string(my_micro_time()) +
         ".tmp";
}

Preserve_trx_transfer_status read_commit_marker(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, bool *committed) {
  if (committed == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  *committed = false;
  const std::string final_path = transfer_epoch_commit_path(root_dir, manifest);
  if (!file_exists(final_path)) return Preserve_trx_transfer_status::OK;

  std::string payload;
  const Preserve_trx_transfer_status read_status =
      read_whole_file(final_path, &payload);
  if (read_status != Preserve_trx_transfer_status::OK) return read_status;
  if (payload != commit_marker_payload(manifest)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  *committed = true;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status write_commit_marker_file(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest) {
  const Preserve_trx_transfer_status dir_status =
      ensure_transfer_token_dir(root_dir, manifest);
  if (dir_status != Preserve_trx_transfer_status::OK) return dir_status;

  const std::string final_path = transfer_epoch_commit_path(root_dir, manifest);
  bool already_committed = false;
  const Preserve_trx_transfer_status marker_status =
      read_commit_marker(root_dir, manifest, &already_committed);
  if (marker_status != Preserve_trx_transfer_status::OK) return marker_status;
  if (already_committed) return Preserve_trx_transfer_status::OK;

  const std::string tmp_path =
      transfer_unique_tmp_path(final_path, transfer_token_component(manifest.token));
  const std::string payload = commit_marker_payload(manifest);
  throttle_receiver_saved_io(payload.length());
  File file =
      my_create(tmp_path.c_str(), 0600, O_WRONLY | O_CREAT | O_EXCL, MYF(0));
  if (file < 0) return Preserve_trx_transfer_status::IO_ERROR;

  bool error =
      my_write(file, reinterpret_cast<const unsigned char *>(payload.data()),
               payload.length(), MYF(0)) != payload.length();
  if (my_close(file, MYF(0))) error = true;
  if (!error && my_rename(tmp_path.c_str(), final_path.c_str(), MYF(0))) {
    if (file_exists(final_path)) return Preserve_trx_transfer_status::OK;
    error = true;
  }
  if (error) {
    (void)my_delete(tmp_path.c_str(), MYF(0));
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status read_whole_file(const std::string &path,
                                             std::string *payload) {
  if (payload == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  MY_STAT stat_area;
  if (!file_exists(path, &stat_area)) return Preserve_trx_transfer_status::CORRUPT;
  payload->assign(static_cast<size_t>(stat_area.st_size), '\0');
  File file = my_open(path.c_str(), O_RDONLY, MYF(0));
  if (file < 0) return Preserve_trx_transfer_status::IO_ERROR;
  bool error = false;
  if (!payload->empty()) {
    const size_t read_len =
        my_read(file, reinterpret_cast<unsigned char *>(&(*payload)[0]),
                payload->length(), MYF(0));
    error = read_len != payload->length();
  }
  if (my_close(file, MYF(0))) error = true;
  return error ? Preserve_trx_transfer_status::IO_ERROR
               : Preserve_trx_transfer_status::OK;
}

std::array<unsigned char, kPreservedTrxSha256Length> sha256_digest(
    const std::string &payload) {
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  SHA256(reinterpret_cast<const unsigned char *>(payload.data()),
         payload.length(), digest.data());
  return digest;
}

Preserve_trx_transfer_status sha256_digest_file_streaming(
    const std::string &path, uint64_t expected_size,
    std::array<unsigned char, kPreservedTrxSha256Length> *digest) {
  if (digest == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  MY_STAT stat_area;
  if (!file_exists(path, &stat_area)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (stat_area.st_size < 0 ||
      static_cast<uint64_t>(stat_area.st_size) != expected_size) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  EVP_MD_CTX *ctx = EVP_MD_CTX_new();
  if (ctx == nullptr || EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
    if (ctx != nullptr) EVP_MD_CTX_free(ctx);
    return Preserve_trx_transfer_status::IO_ERROR;
  }

  File file = my_open(path.c_str(), O_RDONLY, MYF(0));
  if (file < 0) {
    EVP_MD_CTX_free(ctx);
    return Preserve_trx_transfer_status::IO_ERROR;
  }

  std::array<unsigned char, 64 * 1024> buffer{};
  uint64_t remaining = expected_size;
  bool error = false;
  while (remaining > 0 && !error) {
    const size_t to_read = static_cast<size_t>(
        std::min<uint64_t>(remaining, static_cast<uint64_t>(buffer.size())));
    const size_t read_len = my_read(file, buffer.data(), to_read, MYF(0));
    if (read_len != to_read ||
        EVP_DigestUpdate(ctx, buffer.data(), read_len) != 1) {
      error = true;
      break;
    }
    remaining -= read_len;
  }

  unsigned int digest_len = 0;
  if (!error &&
      (EVP_DigestFinal_ex(ctx, digest->data(), &digest_len) != 1 ||
       digest_len != digest->size())) {
    error = true;
  }
  EVP_MD_CTX_free(ctx);
  if (my_close(file, MYF(0))) error = true;

  return error ? Preserve_trx_transfer_status::IO_ERROR
               : Preserve_trx_transfer_status::OK;
}

std::string digest_hex(
    const std::array<unsigned char, kPreservedTrxSha256Length> &digest) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out;
  out.reserve(digest.size() * 2);
  for (unsigned char byte : digest) {
    out.push_back(kHex[(byte >> 4) & 0x0f]);
    out.push_back(kHex[byte & 0x0f]);
  }
  return out;
}

bool hex_value(char c, unsigned char *value) {
  if (value == nullptr) return false;
  if (c >= '0' && c <= '9') {
    *value = static_cast<unsigned char>(c - '0');
    return true;
  }
  if (c >= 'a' && c <= 'f') {
    *value = static_cast<unsigned char>(10 + c - 'a');
    return true;
  }
  if (c >= 'A' && c <= 'F') {
    *value = static_cast<unsigned char>(10 + c - 'A');
    return true;
  }
  return false;
}

bool parse_digest_hex(
    const std::string &hex,
    std::array<unsigned char, kPreservedTrxSha256Length> *digest) {
  if (digest == nullptr || hex.length() != digest->size() * 2) return false;
  std::array<unsigned char, kPreservedTrxSha256Length> parsed{};
  for (size_t i = 0; i < parsed.size(); ++i) {
    unsigned char high = 0;
    unsigned char low = 0;
    if (!hex_value(hex[i * 2], &high) || !hex_value(hex[i * 2 + 1], &low)) {
      return false;
    }
    parsed[i] = static_cast<unsigned char>((high << 4) | low);
  }
  *digest = parsed;
  return true;
}

bool parse_uint64_strict(const std::string &text, uint64_t *value) {
  if (value == nullptr || text.empty()) return false;
  char *end = nullptr;
  errno = 0;
  const unsigned long long parsed =
      std::strtoull(text.c_str(), &end, 10);  // NOLINT(runtime/int)
  if (errno != 0 || end == nullptr || *end != '\0') return false;
  *value = static_cast<uint64_t>(parsed);
  return true;
}

constexpr char kBinlogAppendPrefixReason[] =
    "PTRX_BINLOG_APPEND_PREFIX_V1";

std::string encode_binlog_append_prefix_reason(
    uint64_t prefix_size,
    const std::array<unsigned char, kPreservedTrxSha256Length>
        &prefix_digest) {
  return std::string(kBinlogAppendPrefixReason) + "\n" +
         std::to_string(prefix_size) + "\n" + digest_hex(prefix_digest);
}

bool decode_binlog_append_prefix_reason(
    const std::string &reason, uint64_t *prefix_size,
    std::array<unsigned char, kPreservedTrxSha256Length> *prefix_digest) {
  if (prefix_size == nullptr || prefix_digest == nullptr || reason.empty()) {
    return false;
  }
  const size_t first_newline = reason.find('\n');
  const size_t second_newline =
      first_newline == std::string::npos
          ? std::string::npos
          : reason.find('\n', first_newline + 1);
  if (first_newline == std::string::npos ||
      second_newline == std::string::npos ||
      reason.find('\n', second_newline + 1) != std::string::npos ||
      reason.compare(0, first_newline, kBinlogAppendPrefixReason) != 0) {
    return false;
  }
  uint64_t decoded_size = 0;
  if (!parse_uint64_strict(
          reason.substr(first_newline + 1,
                        second_newline - first_newline - 1),
          &decoded_size) ||
      decoded_size == 0 ||
      !parse_digest_hex(reason.substr(second_newline + 1), prefix_digest)) {
    return false;
  }
  *prefix_size = decoded_size;
  return true;
}

bool line_has_prefix(const std::string &line, const char *prefix,
                     std::string *value) {
  const size_t prefix_len = std::strlen(prefix);
  if (line.compare(0, prefix_len, prefix) != 0) return false;
  if (value != nullptr) *value = line.substr(prefix_len);
  return true;
}

std::vector<std::string> split_pipe_fields(const std::string &line) {
  std::vector<std::string> fields;
  size_t offset = 0;
  while (offset <= line.length()) {
    const size_t next = line.find('|', offset);
    if (next == std::string::npos) {
      fields.push_back(line.substr(offset));
      break;
    }
    fields.push_back(line.substr(offset, next - offset));
    offset = next + 1;
  }
  return fields;
}

bool staged_ranges_cover_object(
    const std::string &root_dir, const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &object) {
  if (object.total_size == 0) return true;

  std::string ranges_payload;
  if (read_whole_file(transfer_object_range_path(root_dir, manifest, object),
                      &ranges_payload) != Preserve_trx_transfer_status::OK) {
    return false;
  }

  std::vector<std::pair<uint64_t, uint64_t>> ranges;
  std::istringstream input(ranges_payload);
  uint64_t offset = 0;
  uint64_t length = 0;
  while (input >> offset >> length) {
    if (length == 0) continue;
    if (offset > object.total_size || length > object.total_size - offset) {
      return false;
    }
    ranges.emplace_back(offset, offset + length);
  }
  if (!input.eof()) return false;

  std::sort(ranges.begin(), ranges.end());
  uint64_t covered_until = 0;
  for (const auto &range : ranges) {
    if (range.first > covered_until) return false;
    if (range.second > covered_until) covered_until = range.second;
    if (covered_until == object.total_size) return true;
  }
  return false;
}

void append_u16(std::string *out, uint16_t value) {
  out->push_back(static_cast<char>(value & 0xff));
  out->push_back(static_cast<char>((value >> 8) & 0xff));
}

void append_u32(std::string *out, uint32_t value) {
  for (size_t i = 0; i < 4; ++i) {
    out->push_back(static_cast<char>((value >> (8 * i)) & 0xff));
  }
}

void append_u64(std::string *out, uint64_t value) {
  for (size_t i = 0; i < 8; ++i) {
    out->push_back(static_cast<char>((value >> (8 * i)) & 0xff));
  }
}

std::array<unsigned char, kPreservedTrxSha256Length>
simulated_terminal_lock_proof_digest(
    const std::string &epoch_id, uint64_t token,
    const Preserve_trx_transfer_lock_plan_contract &contract) {
  static constexpr char kDomain[] = "PTRX_SIM_TERMINAL_LOCK_V1";
  std::string material;
  material.reserve(sizeof(kDomain) - 1 + sizeof(uint64_t) * 3 +
                   sizeof(uint16_t) + epoch_id.size() +
                   contract.source_live_digest.size() +
                   contract.record_store_fingerprint.size());
  material.append(kDomain, sizeof(kDomain) - 1);
  append_u64(&material, epoch_id.size());
  material.append(epoch_id);
  append_u64(&material, token);
  append_u16(&material, contract.version);
  append_u64(&material, contract.source_live_generation);
  material.append(
      reinterpret_cast<const char *>(contract.source_live_digest.data()),
      contract.source_live_digest.size());
  material.append(reinterpret_cast<const char *>(
                      contract.record_store_fingerprint.data()),
                  contract.record_store_fingerprint.size());
  return sha256_digest(material);
}

bool simulated_terminal_lock_proof_matches(
    const std::string &epoch_id, uint64_t token,
    const Preserve_trx_transfer_object_descriptor &object) {
  const auto &contract = object.lock_plan;
  return object.object_id == kPreservedTrxBlobRecordLocks &&
         object.kind == Preserve_trx_transfer_object_kind::EXTERNAL_BLOB &&
         contract.version == kPreserveTrxTransferLockPlanContractVersion &&
         contract.source_live_generation != 0 &&
         !transfer_digest_is_zero(contract.source_live_digest) &&
         !transfer_digest_is_zero(contract.record_store_fingerprint) &&
         contract.simulated_terminal_proof &&
         contract.terminal_proof == simulated_terminal_lock_proof_digest(
                                        epoch_id, token, contract);
}

void maybe_attach_simulated_terminal_lock_proof(
    const std::string &epoch_id, uint64_t token,
    Preserve_trx_transfer_object_descriptor *object) {
  if (object == nullptr ||
      !transfer_lock_plan_contract_valid(*object) ||
      object->lock_plan.version !=
          kPreserveTrxTransferLockPlanContractVersion ||
      object->lock_plan.simulated_terminal_proof) {
    return;
  }
  Preserve_trx_transfer_terminal_lock_proof_provider provider =
      unit_terminal_lock_proof_provider();
  if (provider == nullptr ||
      !provider(epoch_id, token, object->lock_plan)) {
    return;
  }
  object->lock_plan.simulated_terminal_proof = true;
  object->lock_plan.terminal_proof = simulated_terminal_lock_proof_digest(
      epoch_id, token, object->lock_plan);
}

Preserve_trx_transfer_object_descriptor transfer_external_blob_descriptor(
    const std::string &epoch_id, uint64_t token,
    const Preserved_trx_external_blob &blob) {
  Preserve_trx_transfer_object_descriptor descriptor;
  descriptor.object_id = blob.name;
  descriptor.kind = Preserve_trx_transfer_object_kind::EXTERNAL_BLOB;
  descriptor.lock_plan.version = blob.lock_plan_contract_version;
  descriptor.lock_plan.source_live_generation =
      blob.source_live_lock_generation;
  descriptor.lock_plan.source_live_digest = blob.source_live_lock_digest;
  descriptor.lock_plan.record_store_fingerprint =
      blob.record_store_fingerprint;
  descriptor.total_size =
      blob.prebuilt ? blob.descriptor.size : blob.payload.length();
  descriptor.digest =
      blob.prebuilt ? blob.descriptor.digest : sha256_digest(blob.payload);
  maybe_attach_simulated_terminal_lock_proof(epoch_id, token, &descriptor);
  return descriptor;
}

bool receiver_frame_is_sequence_tracked(
    Preserve_trx_transfer_frame_type type) {
  switch (type) {
    case Preserve_trx_transfer_frame_type::OPEN_EPOCH:
      return false;
    case Preserve_trx_transfer_frame_type::DECLARE_TOKEN:
    case Preserve_trx_transfer_frame_type::DECLARE_OBJECT:
    case Preserve_trx_transfer_frame_type::BEGIN:
    case Preserve_trx_transfer_frame_type::OBJECT_CHUNK:
    case Preserve_trx_transfer_frame_type::SEAL_OBJECT:
    case Preserve_trx_transfer_frame_type::COMMIT_EPOCH:
    case Preserve_trx_transfer_frame_type::ABORT:
      return true;
    case Preserve_trx_transfer_frame_type::PROMOTION_PREWARM_TOKEN:
    case Preserve_trx_transfer_frame_type::PROMOTION_GATE_EPOCH:
    case Preserve_trx_transfer_frame_type::QUERY_EPOCH_STATUS:
    case Preserve_trx_transfer_frame_type::ABANDON_EPOCH_IF_NOT_COMMITTED:
      return false;
  }
  return false;
}

class Receiver_frame_sequence_disable_guard {
 public:
  Receiver_frame_sequence_disable_guard()
      : m_previous(g_receiver_frame_sequence_disabled) {
    g_receiver_frame_sequence_disabled = true;
  }

  ~Receiver_frame_sequence_disable_guard() {
    g_receiver_frame_sequence_disabled = m_previous;
  }

 private:
  bool m_previous;
};

bool append_string(std::string *out, const std::string &value) {
  if (value.length() > kMaxTransferManifestStringBytes ||
      value.length() > std::numeric_limits<uint32_t>::max()) {
    return true;
  }
  append_u32(out, static_cast<uint32_t>(value.length()));
  out->append(value);
  return false;
}

bool append_bytes64(std::string *out, const std::vector<unsigned char> &value) {
  append_u64(out, value.size());
  if (!value.empty()) {
    out->append(reinterpret_cast<const char *>(value.data()), value.size());
  }
  return false;
}

class Manifest_reader {
 public:
  explicit Manifest_reader(const std::string &bytes) : m_bytes(bytes) {}

  bool read_fixed(size_t length, const char **ptr) {
    if (ptr == nullptr || m_offset > m_bytes.length() ||
        length > m_bytes.length() - m_offset) {
      return true;
    }
    *ptr = m_bytes.data() + m_offset;
    m_offset += length;
    return false;
  }

  bool read_u16(uint16_t *value) {
    const char *ptr = nullptr;
    if (value == nullptr || read_fixed(2, &ptr)) return true;
    *value = static_cast<unsigned char>(ptr[0]) |
             (static_cast<uint16_t>(static_cast<unsigned char>(ptr[1])) << 8);
    return false;
  }

  bool read_u32(uint32_t *value) {
    const char *ptr = nullptr;
    if (value == nullptr || read_fixed(4, &ptr)) return true;
    uint32_t result = 0;
    for (size_t i = 0; i < 4; ++i) {
      result |= static_cast<uint32_t>(static_cast<unsigned char>(ptr[i]))
                << (8 * i);
    }
    *value = result;
    return false;
  }

  bool read_u64(uint64_t *value) {
    const char *ptr = nullptr;
    if (value == nullptr || read_fixed(8, &ptr)) return true;
    uint64_t result = 0;
    for (size_t i = 0; i < 8; ++i) {
      result |= static_cast<uint64_t>(static_cast<unsigned char>(ptr[i]))
                << (8 * i);
    }
    *value = result;
    return false;
  }

  bool read_string(std::string *value) {
    uint32_t length = 0;
    const char *ptr = nullptr;
    if (value == nullptr || read_u32(&length) ||
        length > kMaxTransferManifestStringBytes || read_fixed(length, &ptr)) {
      return true;
    }
    value->assign(ptr, length);
    return false;
  }

  bool read_bytes64(std::vector<unsigned char> *value) {
    uint64_t length = 0;
    const char *ptr = nullptr;
    if (value == nullptr || read_u64(&length) ||
        length > std::numeric_limits<size_t>::max() ||
        read_fixed(static_cast<size_t>(length), &ptr)) {
      return true;
    }
    value->assign(reinterpret_cast<const unsigned char *>(ptr),
                  reinterpret_cast<const unsigned char *>(ptr) +
                      static_cast<size_t>(length));
    return false;
  }

  bool eof() const { return m_offset == m_bytes.length(); }
  size_t remaining() const {
    return m_offset <= m_bytes.length() ? m_bytes.length() - m_offset : 0;
  }

 private:
  const std::string &m_bytes;
  size_t m_offset{0};
};

bool object_kind_supported(uint16_t raw_kind,
                           Preserve_trx_transfer_object_kind *kind);

void append_temp_id_contract(std::string *out,
                             const Preserve_trx_temp_id_contract &contract) {
  append_u16(out, contract.version);
  append_u16(out, contract.policy);
  append_u64(out, contract.table_id_begin);
  append_u64(out, contract.table_id_end);
  append_u64(out, contract.persistent_table_id_limit);
}

bool read_temp_id_contract(Manifest_reader *reader,
                           Preserve_trx_temp_id_contract *contract) {
  return reader->read_u16(&contract->version) ||
         reader->read_u16(&contract->policy) ||
         reader->read_u64(&contract->table_id_begin) ||
         reader->read_u64(&contract->table_id_end) ||
         reader->read_u64(&contract->persistent_table_id_limit);
}

bool open_contract_valid(uint16_t version, bool is_open,
                         const Preserve_trx_temp_id_contract &contract) {
  if (version == kPreserveTrxTransferOpenContractVersion) {
    return is_open && contract.supported();
  }
  return version == kPreserveTrxTransferProtocolVersion && contract.empty();
}

Preserve_trx_transfer_status encode_transfer_object_descriptor(
    const Preserve_trx_transfer_object_descriptor &object,
    std::string *encoded) {
  if (encoded == nullptr || !transfer_component_safe(object.object_id) ||
      !transfer_lock_plan_contract_valid(object)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::string out;
  out.append(kTransferObjectDescriptorMagic,
             kTransferObjectDescriptorMagicLength);
  append_u16(&out, kTransferObjectDescriptorVersion);
  if (append_string(&out, object.object_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  append_u16(&out, static_cast<uint16_t>(object.kind));
  append_u32(&out, object.flags);
  append_u64(&out, object.total_size);
  out.append(reinterpret_cast<const char *>(object.digest.data()),
             object.digest.size());
  append_u16(&out, object.lock_plan.version);
  append_u64(&out, object.lock_plan.source_live_generation);
  out.append(reinterpret_cast<const char *>(
                 object.lock_plan.source_live_digest.data()),
             object.lock_plan.source_live_digest.size());
  out.append(reinterpret_cast<const char *>(
                 object.lock_plan.record_store_fingerprint.data()),
             object.lock_plan.record_store_fingerprint.size());
  append_u16(&out, object.lock_plan.simulated_terminal_proof ? 1 : 0);
  out.append(reinterpret_cast<const char *>(
                 object.lock_plan.terminal_proof.data()),
             object.lock_plan.terminal_proof.size());
  *encoded = std::move(out);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status decode_transfer_object_descriptor(
    const std::string &encoded,
    Preserve_trx_transfer_object_descriptor *object) {
  if (object == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  Manifest_reader reader(encoded);
  const char *magic = nullptr;
  if (reader.read_fixed(kTransferObjectDescriptorMagicLength, &magic) ||
      std::memcmp(magic, kTransferObjectDescriptorMagic,
                  kTransferObjectDescriptorMagicLength) != 0) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  uint16_t version = 0;
  if (reader.read_u16(&version)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (version != kTransferObjectDescriptorVersion) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserve_trx_transfer_object_descriptor parsed;
  uint16_t raw_kind = 0;
  const char *digest = nullptr;
  if (reader.read_string(&parsed.object_id) || reader.read_u16(&raw_kind) ||
      !object_kind_supported(raw_kind, &parsed.kind) ||
      reader.read_u32(&parsed.flags) || reader.read_u64(&parsed.total_size) ||
      reader.read_fixed(parsed.digest.size(), &digest) ||
      !transfer_component_safe(parsed.object_id)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  std::memcpy(parsed.digest.data(), digest, parsed.digest.size());
  const char *source_live_digest = nullptr;
  const char *record_store_fingerprint = nullptr;
  const char *terminal_proof = nullptr;
  uint16_t terminal_proof_present = 0;
  if (reader.read_u16(&parsed.lock_plan.version) ||
      reader.read_u64(&parsed.lock_plan.source_live_generation) ||
      reader.read_fixed(parsed.lock_plan.source_live_digest.size(),
                        &source_live_digest) ||
      reader.read_fixed(parsed.lock_plan.record_store_fingerprint.size(),
                        &record_store_fingerprint) ||
      reader.read_u16(&terminal_proof_present) ||
      terminal_proof_present > 1 ||
      reader.read_fixed(parsed.lock_plan.terminal_proof.size(),
                        &terminal_proof) ||
      !reader.eof()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  parsed.lock_plan.simulated_terminal_proof =
      terminal_proof_present != 0;
  std::memcpy(parsed.lock_plan.source_live_digest.data(), source_live_digest,
              parsed.lock_plan.source_live_digest.size());
  std::memcpy(parsed.lock_plan.record_store_fingerprint.data(),
              record_store_fingerprint,
              parsed.lock_plan.record_store_fingerprint.size());
  std::memcpy(parsed.lock_plan.terminal_proof.data(), terminal_proof,
              parsed.lock_plan.terminal_proof.size());
  if (!transfer_lock_plan_contract_valid(parsed)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  *object = std::move(parsed);
  return Preserve_trx_transfer_status::OK;
}

bool object_kind_supported(uint16_t raw_kind,
                           Preserve_trx_transfer_object_kind *kind) {
  if (kind == nullptr) return false;
  switch (static_cast<Preserve_trx_transfer_object_kind>(raw_kind)) {
    case Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE:
    case Preserve_trx_transfer_object_kind::EXTERNAL_BLOB:
    case Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR:
    case Preserve_trx_transfer_object_kind::RESURRECTION_INDEX:
    case Preserve_trx_transfer_object_kind::CURSOR_RESULT:
      *kind = static_cast<Preserve_trx_transfer_object_kind>(raw_kind);
      return true;
  }
  return false;
}

bool frame_type_supported(uint16_t raw_type,
                          Preserve_trx_transfer_frame_type *type) {
  if (type == nullptr) return false;
  switch (static_cast<Preserve_trx_transfer_frame_type>(raw_type)) {
    case Preserve_trx_transfer_frame_type::OPEN_EPOCH:
    case Preserve_trx_transfer_frame_type::BEGIN:
    case Preserve_trx_transfer_frame_type::OBJECT_CHUNK:
    case Preserve_trx_transfer_frame_type::SEAL_OBJECT:
    case Preserve_trx_transfer_frame_type::COMMIT_EPOCH:
    case Preserve_trx_transfer_frame_type::ABORT:
    case Preserve_trx_transfer_frame_type::PROMOTION_PREWARM_TOKEN:
    case Preserve_trx_transfer_frame_type::PROMOTION_GATE_EPOCH:
    case Preserve_trx_transfer_frame_type::DECLARE_TOKEN:
    case Preserve_trx_transfer_frame_type::DECLARE_OBJECT:
    case Preserve_trx_transfer_frame_type::QUERY_EPOCH_STATUS:
    case Preserve_trx_transfer_frame_type::ABANDON_EPOCH_IF_NOT_COMMITTED:
      *type = static_cast<Preserve_trx_transfer_frame_type>(raw_type);
      return true;
  }
  return false;
}

Preserve_trx_transfer_status validate_frame_components(
    const Preserve_trx_transfer_frame &frame, bool decoded_remote_frame) {
  auto frame_error = [&]() {
    return decoded_remote_frame ? Preserve_trx_transfer_status::CORRUPT
                                : Preserve_trx_transfer_status::
                                      INVALID_ARGUMENT;
  };

  const bool control_only_commit =
      frame.type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH &&
      !frame.manifest_payload.empty() && frame.token == 0;
  const bool token_optional =
      frame.type == Preserve_trx_transfer_frame_type::OPEN_EPOCH ||
      frame.type == Preserve_trx_transfer_frame_type::PROMOTION_GATE_EPOCH ||
      frame.type == Preserve_trx_transfer_frame_type::QUERY_EPOCH_STATUS ||
      frame.type == Preserve_trx_transfer_frame_type::
                        ABANDON_EPOCH_IF_NOT_COMMITTED ||
      control_only_commit;
  if (!open_contract_valid(
          frame.protocol_version,
          frame.type == Preserve_trx_transfer_frame_type::OPEN_EPOCH,
          frame.temp_id_contract) ||
      !transfer_component_safe(frame.epoch_id) ||
      (!token_optional && frame.token == 0)) {
    return frame_error();
  }

  const bool object_required =
      frame.type == Preserve_trx_transfer_frame_type::DECLARE_OBJECT ||
      frame.type == Preserve_trx_transfer_frame_type::OBJECT_CHUNK ||
      frame.type == Preserve_trx_transfer_frame_type::SEAL_OBJECT;
  if (object_required && !transfer_component_safe(frame.object_id)) {
    return frame_error();
  }
  if (frame.type == Preserve_trx_transfer_frame_type::BEGIN &&
      frame.manifest_payload.empty()) {
    return frame_error();
  }
  const bool trx_id_store_fact_empty =
      transfer_trx_id_store_fact_is_empty(frame.trx_id_store);
  const bool terminal_fact_digest_empty =
      transfer_digest_is_zero(frame.terminal_fact_digest);
  if (frame.type != Preserve_trx_transfer_frame_type::COMMIT_EPOCH &&
      !trx_id_store_fact_empty) {
    return frame_error();
  }
  if (frame.type != Preserve_trx_transfer_frame_type::COMMIT_EPOCH &&
      frame.type != Preserve_trx_transfer_frame_type::QUERY_EPOCH_STATUS &&
      frame.type !=
          Preserve_trx_transfer_frame_type::ABANDON_EPOCH_IF_NOT_COMMITTED &&
      !terminal_fact_digest_empty) {
    return frame_error();
  }

  switch (frame.type) {
    case Preserve_trx_transfer_frame_type::OPEN_EPOCH:
      if (frame.sequence != 0 || frame.token != 0 ||
          !frame.receiver_process_nonce.empty() ||
          frame.requested_terminal_status_retention_us == 0 ||
          !frame.object_id.empty() || frame.chunk_offset != 0 ||
          !frame.manifest_payload.empty() || !frame.chunk_payload.empty() ||
          !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::DECLARE_TOKEN:
      if (frame.requested_terminal_status_retention_us != 0 ||
          !frame.object_id.empty() || frame.chunk_offset != 0 ||
          !frame.manifest_payload.empty() || !frame.chunk_payload.empty() ||
          !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::DECLARE_OBJECT:
      {
        if (frame.requested_terminal_status_retention_us != 0) {
          return frame_error();
        }
        uint64_t prefix_size = 0;
        std::array<unsigned char, kPreservedTrxSha256Length> prefix_digest{};
        const bool append_prefix = decode_binlog_append_prefix_reason(
            frame.reason, &prefix_size, &prefix_digest);
        if (frame.manifest_payload.empty() || !frame.chunk_payload.empty() ||
            (frame.reason.empty() ? frame.chunk_offset != 0
                                  : (!append_prefix ||
                                     frame.chunk_offset != prefix_size))) {
          return frame_error();
        }
      }
      break;
    case Preserve_trx_transfer_frame_type::BEGIN:
      if (frame.requested_terminal_status_retention_us != 0) {
        return frame_error();
      }
      if (!frame.object_id.empty() || frame.chunk_offset != 0 ||
          !frame.chunk_payload.empty() || !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::OBJECT_CHUNK:
      if (frame.requested_terminal_status_retention_us != 0) {
        return frame_error();
      }
      if (!frame.manifest_payload.empty() || !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::SEAL_OBJECT:
      if (frame.requested_terminal_status_retention_us != 0) {
        return frame_error();
      }
      if (frame.chunk_offset != 0 || !frame.manifest_payload.empty() ||
          !frame.chunk_payload.empty() || !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::COMMIT_EPOCH:
      if (frame.requested_terminal_status_retention_us != 0) {
        return frame_error();
      }
      if (!frame.object_id.empty() || !frame.chunk_payload.empty() ||
          !frame.reason.empty()) {
        return frame_error();
      }
      if (!frame.manifest_payload.empty()) {
        std::vector<uint64_t> session_only_tokens;
        if (preserve_trx_transfer_decode_session_only_tokens(
                frame.manifest_payload, &session_only_tokens) !=
            Preserve_trx_transfer_status::OK) {
          return frame_error();
        }
      }
      if (control_only_commit &&
          (trx_id_store_fact_empty || frame.chunk_offset == 0 ||
           terminal_fact_digest_empty)) {
        return frame_error();
      }
      if (!trx_id_store_fact_empty &&
           !transfer_trx_id_store_fact_is_valid(frame.trx_id_store,
                                                frame.chunk_offset)) {
        return frame_error();
      }
      if (!frame.receiver_process_nonce.empty() &&
          terminal_fact_digest_empty) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::ABORT:
      if (frame.requested_terminal_status_retention_us != 0) {
        return frame_error();
      }
      if (!frame.object_id.empty() || frame.chunk_offset != 0 ||
          !frame.manifest_payload.empty() || !frame.chunk_payload.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::PROMOTION_PREWARM_TOKEN:
      if (frame.requested_terminal_status_retention_us != 0) {
        return frame_error();
      }
      if (!frame.object_id.empty() || !frame.manifest_payload.empty() ||
          !frame.chunk_payload.empty() || !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::PROMOTION_GATE_EPOCH:
      if (frame.requested_terminal_status_retention_us != 0) {
        return frame_error();
      }
      if (frame.token != 0 || !frame.object_id.empty() ||
          !frame.manifest_payload.empty() || !frame.chunk_payload.empty() ||
          !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::QUERY_EPOCH_STATUS:
    case Preserve_trx_transfer_frame_type::ABANDON_EPOCH_IF_NOT_COMMITTED:
      if (frame.requested_terminal_status_retention_us != 0) {
        return frame_error();
      }
      if (frame.protocol_version != kPreserveTrxTransferProtocolVersion ||
          terminal_fact_digest_empty ||
          !frame.object_id.empty() || frame.chunk_offset != 0 ||
          !frame.manifest_payload.empty() || !frame.chunk_payload.empty() ||
          !frame.reason.empty()) {
        return frame_error();
      }
      break;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status map_snapshot_status_to_transfer(
    Preserve_snapshot_status status) {
  switch (status) {
    case Preserve_snapshot_status::OK:
      return Preserve_trx_transfer_status::OK;
    case Preserve_snapshot_status::CORRUPT:
      return Preserve_trx_transfer_status::CORRUPT;
    case Preserve_snapshot_status::INVALID_ARGUMENT:
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    case Preserve_snapshot_status::UNSUPPORTED:
      return Preserve_trx_transfer_status::UNSUPPORTED;
    case Preserve_snapshot_status::NOT_FOUND:
    case Preserve_snapshot_status::IO_ERROR:
      return Preserve_trx_transfer_status::IO_ERROR;
  }
  return Preserve_trx_transfer_status::IO_ERROR;
}

Preserve_snapshot_status map_transfer_status_to_snapshot(
    Preserve_trx_transfer_status status) {
  switch (status) {
    case Preserve_trx_transfer_status::OK:
      return Preserve_snapshot_status::OK;
    case Preserve_trx_transfer_status::INVALID_ARGUMENT:
      return Preserve_snapshot_status::INVALID_ARGUMENT;
    case Preserve_trx_transfer_status::CORRUPT:
      return Preserve_snapshot_status::CORRUPT;
    case Preserve_trx_transfer_status::UNSUPPORTED:
    case Preserve_trx_transfer_status::LOCK_PLAN_STALE:
      return Preserve_snapshot_status::UNSUPPORTED;
    case Preserve_trx_transfer_status::RESOURCE_EXHAUSTED:
    case Preserve_trx_transfer_status::ACK_UNCERTAIN:
      return Preserve_snapshot_status::IO_ERROR;
    case Preserve_trx_transfer_status::NOT_COMMITTED:
    case Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN:
      return Preserve_snapshot_status::UNSUPPORTED;
    case Preserve_trx_transfer_status::COMMITTED_READY:
    case Preserve_trx_transfer_status::COMMITTED_NOT_READY:
      return Preserve_snapshot_status::OK;
    case Preserve_trx_transfer_status::COMMITTED_CORRUPT:
      return Preserve_snapshot_status::CORRUPT;
    case Preserve_trx_transfer_status::IO_ERROR:
      return Preserve_snapshot_status::IO_ERROR;
  }
  return Preserve_snapshot_status::IO_ERROR;
}

Preserve_trx_transfer_status map_carrier_status_to_transfer(
    Preserved_trx_carrier_status status) {
  switch (status) {
    case Preserved_trx_carrier_status::OK:
      return Preserve_trx_transfer_status::OK;
    case Preserved_trx_carrier_status::ALREADY_EXISTS:
    case Preserved_trx_carrier_status::CORRUPT:
      return Preserve_trx_transfer_status::CORRUPT;
    case Preserved_trx_carrier_status::NOT_FOUND:
    case Preserved_trx_carrier_status::IO_ERROR:
    case Preserved_trx_carrier_status::IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST:
      return Preserve_trx_transfer_status::IO_ERROR;
  }
  return Preserve_trx_transfer_status::IO_ERROR;
}

std::string transfer_status_name(Preserve_trx_transfer_status status) {
  switch (status) {
    case Preserve_trx_transfer_status::OK:
      return "OK";
    case Preserve_trx_transfer_status::INVALID_ARGUMENT:
      return "INVALID_ARGUMENT";
    case Preserve_trx_transfer_status::CORRUPT:
      return "CORRUPT";
    case Preserve_trx_transfer_status::IO_ERROR:
      return "IO_ERROR";
    case Preserve_trx_transfer_status::UNSUPPORTED:
      return "UNSUPPORTED";
    case Preserve_trx_transfer_status::RESOURCE_EXHAUSTED:
      return "RESOURCE_EXHAUSTED";
    case Preserve_trx_transfer_status::COMMITTED_READY:
      return "COMMITTED_READY";
    case Preserve_trx_transfer_status::COMMITTED_NOT_READY:
      return "COMMITTED_NOT_READY";
    case Preserve_trx_transfer_status::COMMITTED_CORRUPT:
      return "COMMITTED_CORRUPT";
    case Preserve_trx_transfer_status::ACK_UNCERTAIN:
      return "ACK_UNCERTAIN";
    case Preserve_trx_transfer_status::LOCK_PLAN_STALE:
      return "LOCK_PLAN_STALE";
    case Preserve_trx_transfer_status::NOT_COMMITTED:
      return "NOT_COMMITTED";
    case Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN:
      return "NOT_COMMITTED_CLEAN";
  }
  return "UNKNOWN";
}

bool transfer_is_resource_object(const Preserve_trx_transfer_object_descriptor &object) {
  return object.kind == Preserve_trx_transfer_object_kind::CURSOR_RESULT ||
         object.kind == Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR;
}

bool transfer_is_temp_undo_object(const Preserve_trx_transfer_object_descriptor &object) {
  return object.kind == Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR &&
         object.flags == 0 && transfer_lock_plan_contract_valid(object) &&
         object.object_id.size() > 5 &&
         (object.object_id.compare(object.object_id.size() - 5, 5, ".undo") == 0 ||
          preserve_trx_temp_undo_delta_id(object.object_id, nullptr));
}

Preserve_trx_transfer_manifest receiver_record_manifest(
    const Preserve_trx_transfer_receiver_record &record) {
  Preserve_trx_transfer_manifest manifest;
  manifest.protocol_version = record.protocol_version;
  manifest.epoch_id = record.epoch_id;
  manifest.token = record.token;
  manifest.source_freeze_lsn = record.source_freeze_lsn;
  manifest.source_epoch_commit_lsn = record.source_epoch_commit_lsn;
  manifest.strict_eligibility_flags = record.strict_eligibility_flags;
  manifest.objects = record.objects;
  return manifest;
}

std::string receiver_boot_incarnation() {
  static const std::string nonce = []() {
    std::array<unsigned char, 16> random_bytes{};
    if (my_rand_buffer(random_bytes.data(), random_bytes.size()) != 0) {
      return std::string();
    }
    return bytes_to_lower_hex(random_bytes.data(), random_bytes.size());
  }();
  return nonce;
}

bool strict_prepared_key_for_receiver(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &semantic_token,
    Preserve_trx_prepared_token_key *key) {
  const std::string epoch_scope = receiver_boot_incarnation();
  if (key == nullptr || root_dir.empty() || epoch_scope.empty() ||
      manifest.epoch_id.empty() || semantic_token.empty() ||
      manifest.source_epoch_commit_lsn == 0) {
    return false;
  }
  key->preserve_dir = root_dir;
  key->epoch_scope = epoch_scope;
  key->epoch_id = manifest.epoch_id;
  key->token = semantic_token;
  key->target_boot_incarnation = receiver_boot_incarnation();
  key->generation = manifest.source_epoch_commit_lsn;
  return true;
}

bool receiver_prepared_key_equal(const Preserve_trx_prepared_token_key &lhs,
                                 const Preserve_trx_prepared_token_key &rhs) {
  return lhs.preserve_dir == rhs.preserve_dir &&
         lhs.epoch_scope == rhs.epoch_scope &&
         lhs.epoch_id == rhs.epoch_id && lhs.token == rhs.token &&
         lhs.target_boot_incarnation == rhs.target_boot_incarnation &&
         lhs.generation == rhs.generation;
}

Receiver_strict_token_key receiver_strict_token_key(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest) {
  return {root_dir, manifest.epoch_id, manifest.token};
}

bool queue_receiver_binlog_prepared(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &seed) {
  Preserve_trx_prepared_token_key key;
  if (seed.object_id != kBinlogPrewarmSeedObjectId ||
      !strict_prepared_key_for_receiver(
          root_dir, manifest, transfer_token_component(manifest.token), &key)) {
    return false;
  }

  Preserve_trx_prepared_token_resources retired;
  std::lock_guard<std::mutex> guard(g_receiver_binlog_prepared_mutex);
  const Receiver_strict_token_key token_key =
      receiver_strict_token_key(root_dir, manifest);
  auto found = g_receiver_binlog_prepared.find(token_key);
  if (found != g_receiver_binlog_prepared.end()) {
    if (receiver_prepared_key_equal(found->second.key, key) &&
        found->second.seed_digest == seed.digest) {
      return true;
    }
    retired = std::move(found->second.resources);
    g_receiver_binlog_prepared.erase(found);
  }
  Receiver_binlog_prepared pending;
  pending.instance = ++g_receiver_binlog_prepared_instance;
  pending.key = std::move(key);
  pending.seed_digest = seed.digest;
  g_receiver_binlog_prepared.emplace(token_key, std::move(pending));
  return true;
}

bool start_receiver_binlog_prepare(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &seed, uint64_t *instance) {
  std::lock_guard<std::mutex> guard(g_receiver_binlog_prepared_mutex);
  const auto found = g_receiver_binlog_prepared.find(
      receiver_strict_token_key(root_dir, manifest));
  if (found == g_receiver_binlog_prepared.end() ||
      found->second.seed_digest != seed.digest) {
    return false;
  }
  if (found->second.state == Receiver_binlog_prepared_state::READY) return false;
  if (found->second.state == Receiver_binlog_prepared_state::BUILDING)
    return false;
  found->second.state = Receiver_binlog_prepared_state::BUILDING;
  *instance = found->second.instance;
  return true;
}

void finish_receiver_binlog_prepare(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &seed, uint64_t instance,
    bool success,
    const Preserve_trx_transfer_object_descriptor *binlog_object,
    const Mysql_binlog_preserve_cache_facts *facts,
    Preserve_trx_prepared_token_resources resources) {
  Preserve_trx_prepared_token_resources retired;
  {
    std::lock_guard<std::mutex> guard(g_receiver_binlog_prepared_mutex);
    const auto found = g_receiver_binlog_prepared.find(
        receiver_strict_token_key(root_dir, manifest));
    if (found == g_receiver_binlog_prepared.end() ||
        found->second.seed_digest != seed.digest ||
        found->second.instance != instance ||
        found->second.state != Receiver_binlog_prepared_state::BUILDING) {
      return;
    }
    if (!success || binlog_object == nullptr || facts == nullptr ||
        !resources.has_native_binlog_handle()) {
      retired = std::move(found->second.resources);
      g_receiver_binlog_prepared.erase(found);
    } else {
      found->second.state = Receiver_binlog_prepared_state::READY;
      found->second.payload_digest = binlog_object->digest;
      found->second.payload_size = binlog_object->total_size;
      found->second.facts_digest = facts->canonical_digest;
      found->second.resources = std::move(resources);
    }
  }
}

enum class Receiver_binlog_take_status {
  ABSENT,
  PENDING,
  READY,
  MISMATCH
};

Receiver_binlog_take_status take_receiver_binlog_seed_prepared(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_prepared_token_key &key,
    const Preserve_trx_transfer_object_descriptor &binlog_object,
    const Mysql_binlog_preserve_cache_facts &facts,
    Preserve_trx_prepared_token_resources *resources) {
  if (resources == nullptr) return Receiver_binlog_take_status::MISMATCH;
  Preserve_trx_prepared_token_resources retired;
  std::lock_guard<std::mutex> guard(g_receiver_binlog_prepared_mutex);
  const auto found = g_receiver_binlog_prepared.find(
      receiver_strict_token_key(root_dir, manifest));
  if (found == g_receiver_binlog_prepared.end())
    return Receiver_binlog_take_status::ABSENT;
  if (found->second.state != Receiver_binlog_prepared_state::READY)
    return Receiver_binlog_take_status::PENDING;
  if (!receiver_prepared_key_equal(found->second.key, key) ||
      found->second.payload_digest != binlog_object.digest ||
      found->second.payload_size != binlog_object.total_size ||
      found->second.facts_digest != facts.canonical_digest ||
      !found->second.resources.has_native_binlog_handle()) {
    retired = std::move(found->second.resources);
    g_receiver_binlog_prepared.erase(found);
    return Receiver_binlog_take_status::MISMATCH;
  }
  *resources = std::move(found->second.resources);
  g_receiver_binlog_prepared.erase(found);
  return Receiver_binlog_take_status::READY;
}

bool receiver_binlog_prepare_pending(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest) {
  const auto *binlog = find_object(manifest, kPreservedTrxBlobBinlogCache);
  if (binlog != nullptr && preserved_trx_receiver_binlog_prefix_take(
          root_dir, manifest, *binlog) ==
              Preserve_trx_receiver_binlog_prefix_status::PENDING) return true;
  std::lock_guard<std::mutex> guard(g_receiver_binlog_prepared_mutex);
  const auto found = g_receiver_binlog_prepared.find(
      receiver_strict_token_key(root_dir, manifest));
  return found != g_receiver_binlog_prepared.end() &&
         found->second.state != Receiver_binlog_prepared_state::READY;
}

Receiver_binlog_take_status take_receiver_binlog_prepared(
    const std::string &root_dir, const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_prepared_token_key &key,
    const Preserve_trx_transfer_object_descriptor &binlog_object,
    const Mysql_binlog_preserve_cache_facts &facts,
    Preserve_trx_prepared_token_resources *resources) {
  const auto prepared = take_receiver_binlog_seed_prepared(
      root_dir, manifest, key, binlog_object, facts, resources);
  if (prepared != Receiver_binlog_take_status::ABSENT) return prepared;
  std::unique_ptr<Mysql_binlog_preserve_payload_builder> payload;
  const auto prefix = preserved_trx_receiver_binlog_prefix_take(
      root_dir, manifest, binlog_object, &payload);
  if (prefix == Preserve_trx_receiver_binlog_prefix_status::PENDING)
    return Receiver_binlog_take_status::PENDING;
  if (prefix == Preserve_trx_receiver_binlog_prefix_status::ABSENT)
    return Receiver_binlog_take_status::ABSENT;
  if (preserved_trx_acquire_prepared_token_resources(key, 0, 0, resources) !=
          Preserve_trx_prepared_status::OK ||
      resources->prepare_native_binlog_handle_for_receiver(
          facts, nullptr, payload.get()) != Mysql_binlog_preserve_cache_status::OK)
    return Receiver_binlog_take_status::MISMATCH;
  return Receiver_binlog_take_status::READY;
}

void erase_receiver_binlog_prepared(const std::string &root_dir,
                                    const std::string &epoch_id,
                                    uint64_t token = 0,
                                    bool keep_prefix = false) {
  if (!keep_prefix)
    preserved_trx_receiver_binlog_prefix_erase(root_dir, epoch_id, token);
  std::vector<Preserve_trx_prepared_token_resources> retired;
  std::lock_guard<std::mutex> guard(g_receiver_binlog_prepared_mutex);
  for (auto found = g_receiver_binlog_prepared.begin();
       found != g_receiver_binlog_prepared.end();) {
    if (std::get<0>(found->first) != root_dir ||
        std::get<1>(found->first) != epoch_id ||
        (token != 0 && std::get<2>(found->first) != token)) {
      ++found;
      continue;
    }
    retired.push_back(std::move(found->second.resources));
    found = g_receiver_binlog_prepared.erase(found);
  }
}

void purge_strict_prepared_token_for_receiver(
    const std::string &root_dir,
    const Preserve_trx_transfer_receiver_record &record) {
  Preserve_trx_prepared_token_key key;
  if (!strict_prepared_key_for_receiver(
          root_dir, receiver_record_manifest(record),
          transfer_token_component(record.token), &key)) {
    return;
  }
  (void)preserved_trx_strict_prepared_token_registry().purge_token(key);
}

bool receiver_record_lock_prepared_key(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    Receiver_object_prewarm_key *key) {
  if (key == nullptr) return false;
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, kPreservedTrxBlobRecordLocks);
  if (root_dir.empty() || object == nullptr || manifest.epoch_id.empty() ||
      manifest.token == 0) {
    return false;
  }
  key->root_dir = root_dir;
  key->epoch_id = manifest.epoch_id;
  key->token = manifest.token;
  key->object_id = object->object_id;
  key->digest = object->digest;
  key->source_live_generation = object->lock_plan.source_live_generation;
  key->source_live_digest = object->lock_plan.source_live_digest;
  return true;
}

bool put_receiver_record_lock_prepared(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    std::unique_ptr<lock_preserve_metadata_plan_t> plan,
    const lock_preserve_record_lock_metadata_facts_t &facts,
    Preserve_memory_lease memory_lease = Preserve_memory_lease{}) {
  Receiver_object_prewarm_key key;
  if (!receiver_record_lock_prepared_key(root_dir, manifest, &key) ||
      plan == nullptr || !plan->ready() || facts.unique_pages == 0 ||
      facts.bitmap_entries == 0 || facts.bitmap_bits == 0 ||
      facts.predicate_lock_present || facts.wait_lock_present ||
      facts.record_image_present ||
      facts.bitmap_entries != plan->entry_count() ||
      facts.bitmap_bits != plan->bitmap_bits()) {
    return false;
  }
  if (!memory_lease.acquired()) {
    const std::string resource_token =
        "receiver-lock-plan:" + root_dir + ":" + manifest.epoch_id + ":" +
        std::to_string(manifest.token) + ":" + digest_hex(key.digest);
    memory_lease = preserve_trx_acquire_memory_lease(
        resource_token, Preserve_trx_memory_kind::PROMOTION_LOCK_PLAN,
        plan->capacity_bytes());
    if (!memory_lease.acquired()) return false;
  }
  if (memory_lease.bytes() != plan->capacity_bytes()) return false;
  Receiver_record_lock_prepared prepared;
  prepared.plan = std::move(plan);
  prepared.facts = facts;
  prepared.memory_lease = std::move(memory_lease);
  std::lock_guard<std::mutex> guard(g_receiver_record_lock_prepared_mutex);
  auto found = g_receiver_record_lock_prepared.find(key);
  if (found != g_receiver_record_lock_prepared.end()) return true;
  g_receiver_record_lock_prepared.emplace(std::move(key), std::move(prepared));
  return true;
}

bool take_receiver_record_lock_prepared(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    Receiver_record_lock_prepared *prepared) {
  Receiver_object_prewarm_key key;
  if (prepared == nullptr ||
      !receiver_record_lock_prepared_key(root_dir, manifest, &key)) {
    return false;
  }
  std::lock_guard<std::mutex> guard(g_receiver_record_lock_prepared_mutex);
  auto found = g_receiver_record_lock_prepared.find(key);
  if (found == g_receiver_record_lock_prepared.end()) return false;
  *prepared = std::move(found->second);
  g_receiver_record_lock_prepared.erase(found);
  return true;
}

bool receiver_record_lock_prepared_exists(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest) {
  Receiver_object_prewarm_key key;
  if (!receiver_record_lock_prepared_key(root_dir, manifest, &key)) {
    return false;
  }
  std::lock_guard<std::mutex> guard(g_receiver_record_lock_prepared_mutex);
  const auto found = g_receiver_record_lock_prepared.find(key);
  return found != g_receiver_record_lock_prepared.end() &&
         found->second.plan != nullptr && found->second.plan->ready();
}

bool build_receiver_record_lock_prepared(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &payload) {
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, kPreservedTrxBlobRecordLocks);
  if (object == nullptr || manifest.source_epoch_commit_lsn == 0) return false;

  lock_preserve_record_lock_metadata_facts_t facts;
  if (lock_preserve_build_record_lock_metadata_facts(payload, &facts) !=
          lock_preserve_metadata_plan_status::OK ||
      facts.predicate_lock_present || facts.wait_lock_present ||
      facts.record_image_present) {
    return false;
  }

  lock_preserve_metadata_plan_validation_t validation;
  validation.object_generation = manifest.source_epoch_commit_lsn;
  validation.expected_object_generation = manifest.source_epoch_commit_lsn;
  validation.physical_fence_lsn = manifest.source_epoch_commit_lsn;
  validation.artifact_protocol_version = 1;
  validation.source_server_version = MYSQL_VERSION_ID;
  validation.object_digest = digest_hex(object->digest);
  validation.final_lock_generation_digest =
      facts.final_lock_generation_digest;
  validation.page_layout_digest = facts.page_layout_digest;
  validation.dictionary_generation_digest =
      facts.dictionary_generation_digest;
  validation.implicit_native_continuity_proven = true;
  validation.is_final_quiesced = true;

  auto plan = std::make_unique<lock_preserve_metadata_plan_t>();
  if (lock_preserve_build_record_lock_metadata_plan_with_default_dict_lease(
          payload, validation, plan.get()) !=
      lock_preserve_metadata_plan_status::OK) {
    return false;
  }
  return put_receiver_record_lock_prepared(root_dir, manifest, std::move(plan),
                                           facts);
}

class Receiver_binlog_staging_payload_reader final
    : public Mysql_binlog_preserve_payload_reader {
 public:
  Receiver_binlog_staging_payload_reader(const std::string &path,
                                          uint64_t length)
      : m_remaining(length), m_file(my_open(path.c_str(), O_RDONLY, MYF(0))) {}
  ~Receiver_binlog_staging_payload_reader() override {
    if (m_file >= 0) (void)my_close(m_file, MYF(0));
  }

  bool opened() const { return m_file >= 0; }

  Mysql_binlog_preserve_payload_read_status read(
      unsigned char *buffer, size_t capacity, size_t *bytes_read) override {
    if (buffer == nullptr || bytes_read == nullptr || capacity == 0 ||
        m_file < 0) {
      return Mysql_binlog_preserve_payload_read_status::ERROR;
    }
    *bytes_read = 0;
    if (m_remaining == 0)
      return Mysql_binlog_preserve_payload_read_status::END;
    const size_t request = static_cast<size_t>(
        std::min<uint64_t>(m_remaining, static_cast<uint64_t>(capacity)));
    const size_t read_bytes = my_read(m_file, buffer, request, MYF(0));
    if (read_bytes == MY_FILE_ERROR || read_bytes == 0 ||
        read_bytes > request) {
      return Mysql_binlog_preserve_payload_read_status::ERROR;
    }
    m_remaining -= read_bytes;
    *bytes_read = read_bytes;
    return Mysql_binlog_preserve_payload_read_status::DATA;
  }

 private:
  uint64_t m_remaining{0};
  File m_file{-1};
};

bool build_receiver_binlog_prepared_from_seed(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &seed,
    const Preserve_trx_transfer_runtime_policy &runtime_policy) {
  uint64_t instance = 0;
  if (!start_receiver_binlog_prepare(root_dir, manifest, seed, &instance))
    return false;

  Preserve_trx_prepared_token_resources resources;
  Mysql_binlog_preserve_cache_facts facts;
  const Preserve_trx_transfer_object_descriptor *binlog_object = nullptr;
  uint64_t native_bytes = 0;
  uint64_t native_tmpdir_bytes = 0;
  bool success = false;
  auto finish = create_scope_guard([&] {
    finish_receiver_binlog_prepare(
        root_dir, manifest, seed, instance, success, binlog_object,
        success ? &facts : nullptr, std::move(resources));
  });

  std::string encoded_seed;
  if (preserve_trx_transfer_read_sealed_object_payload(
          root_dir, manifest, seed.object_id, &encoded_seed, true) !=
      Preserve_trx_transfer_status::OK) {
    return false;
  }
  Preserved_trx_bundle bundle;
  if (preserve_trx_transfer_decode_portable_bundle(encoded_seed, &bundle) !=
          Preserve_trx_transfer_status::OK ||
      bundle.metadata.token != transfer_token_component(manifest.token) ||
      bundle.metadata.binlog_state !=
          Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE) {
    return false;
  }

  const auto descriptor = std::find_if(
      bundle.blob_descriptors.begin(), bundle.blob_descriptors.end(),
      [](const Preserved_trx_external_blob_descriptor &candidate) {
        return candidate.name == kPreservedTrxBlobBinlogCache;
      });
  binlog_object = find_object(manifest, kPreservedTrxBlobBinlogCache);
  if (descriptor == bundle.blob_descriptors.end() || binlog_object == nullptr ||
      descriptor->size != binlog_object->total_size ||
      descriptor->digest != binlog_object->digest) {
    return false;
  }

  Preserve_trx_prepared_token_key key;
  if (!strict_prepared_key_for_receiver(root_dir, manifest,
                                        bundle.metadata.token, &key)) {
    return false;
  }
  Mysql_binlog_preserve_token_identity identity;
  identity.epoch_scope = key.epoch_scope;
  identity.epoch_id = key.epoch_id;
  identity.token = key.token;
  identity.target_boot_incarnation = key.target_boot_incarnation;
  identity.generation = key.generation;
  const uint64_t binlog_incarnation =
      std::max<uint64_t>(1, static_cast<uint64_t>(server_start_time));
  if (!preserved_trx_build_native_binlog_cache_facts(
          bundle.metadata, identity, *descriptor, binlog_incarnation,
          key.generation, &facts)) {
    return false;
  }

  native_bytes = mysql_binlog_preserve_native_memory_bytes_required(facts);
  const uint64_t native_fd_count =
      mysql_binlog_preserve_native_fd_count_required(facts);
  native_tmpdir_bytes =
      mysql_binlog_preserve_native_tmpdir_bytes_required(facts);
  std::unique_ptr<Mysql_binlog_preserve_payload_builder> payload;
  const auto prefix_status = preserved_trx_receiver_binlog_prefix_take(
      root_dir, manifest, *binlog_object, &payload);
  if (prefix_status == Preserve_trx_receiver_binlog_prefix_status::PENDING)
    return false;  // The pool normally defers this job until its prefix completes.
  DBUG_EXECUTE_IF("preserve_trx_receiver_fail_seed_after_prefix_take", {
    if (payload != nullptr) {
      const char action[] = "now SIGNAL receiver_seed_prefix_discarded";
      DBUG_ASSERT(!debug_sync_set_action(current_thd, STRING_WITH_LEN(action)));
    }
    return false;
  });
  if (payload != nullptr) {
    if (preserved_trx_acquire_prepared_token_resources(key, 0, 0, &resources) !=
            Preserve_trx_prepared_status::OK ||
        resources.prepare_native_binlog_handle_for_receiver(
            facts, nullptr, payload.get()) != Mysql_binlog_preserve_cache_status::OK)
      return false;
    success = true;
    return true;
  }
  if (binlog_object->total_size > runtime_policy.prewarm_max_bytes ||
      seed.total_size > runtime_policy.prewarm_max_bytes - binlog_object->total_size)
    return false;
  if (native_bytes == 0 ||
      preserved_trx_acquire_prepared_token_resources(
          key, 0, native_bytes, native_fd_count, native_tmpdir_bytes,
          &resources) != Preserve_trx_prepared_status::OK) {
    return false;
  }

  Receiver_binlog_staging_payload_reader reader(
      transfer_object_path(root_dir, manifest, *binlog_object),
      binlog_object->total_size);
  throttle_receiver_prewarm_io(binlog_object->total_size, runtime_policy);
  if (!reader.opened() ||
      resources.prepare_native_binlog_handle_for_receiver(facts, &reader) !=
          Mysql_binlog_preserve_cache_status::OK) {
    return false;
  }
  success = true;
  return true;
}

bool transfer_object_uses_strict_v1_memory_staging(
    const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &object);

Preserve_trx_transfer_status read_receiver_sealed_object_payload(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id,
    Preserve_trx_transfer_receiver_registry *registry,
    std::shared_ptr<const std::string> *payload) {
  if (payload == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, object_id);
  if (object == nullptr) return Preserve_trx_transfer_status::CORRUPT;
  if (registry != nullptr &&
      transfer_object_uses_strict_v1_memory_staging(manifest, *object)) {
    return registry->read_strict_v1_object(manifest, object_id, payload);
  }

  auto staged_payload = std::make_shared<std::string>();
  const Preserve_trx_transfer_status status =
      preserve_trx_transfer_read_sealed_object_payload(
          root_dir, manifest, object_id, staged_payload.get(), true);
  if (status != Preserve_trx_transfer_status::OK) return status;
  *payload = std::move(staged_payload);
  return Preserve_trx_transfer_status::OK;
}

bool decode_receiver_resurrection_index(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    Preserve_trx_transfer_receiver_registry *registry,
    Preserve_trx_resurrection_index_entry *verified_entry) {
  constexpr uint64_t kStrictResurrectionIndexMaxBytes = 1024 * 1024;
  if (verified_entry == nullptr) return false;
  const auto *index_object =
      find_object(manifest, kPreserveTrxResurrectionIndexObjectId);
  const auto *snapshot_object = find_object(manifest, "snapshot");
  if (index_object == nullptr || snapshot_object == nullptr ||
      index_object->kind !=
          Preserve_trx_transfer_object_kind::RESURRECTION_INDEX ||
      index_object->total_size == 0 ||
      index_object->total_size > kStrictResurrectionIndexMaxBytes) {
    return false;
  }

  std::shared_ptr<const std::string> encoded_index;
  if (read_receiver_sealed_object_payload(
          root_dir, manifest, index_object->object_id, registry,
          &encoded_index) !=
      Preserve_trx_transfer_status::OK) {
    return false;
  }
  Preserved_trx_codec_context context;
  if (!transfer_bundle_codec_context(&context)) return false;
  Preserve_trx_resurrection_index index;
  if (encoded_index == nullptr ||
      preserve_trx_decode_resurrection_index(*encoded_index, context, &index) !=
      Preserve_trx_resurrection_index_status::OK) {
    return false;
  }
  if (index.epoch_id != manifest.epoch_id || index.entries.size() != 1) {
    return false;
  }

  const Preserve_trx_resurrection_index_entry &entry = index.entries.front();
  const std::string token = std::to_string(manifest.token);
  if (entry.authority_token != token ||
      entry.freeze_lsn != manifest.source_freeze_lsn ||
      entry.snapshot_digest != snapshot_object->digest) {
    return false;
  }
  *verified_entry = entry;
  return true;
}

bool resurrection_index_matches_receiver_bundle(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const Preserved_trx_bundle &bundle,
    Preserve_trx_transfer_receiver_registry *registry,
    Preserve_trx_resurrection_index_entry *verified_entry) {
  return decode_receiver_resurrection_index(root_dir, manifest, registry,
                                            verified_entry) &&
         verified_entry->modified_table_ids.size() ==
             bundle.metadata.mod_tables_count &&
         (bundle.metadata.recovery.basis == Preserve_trx_engine_recovery::LEGACY ||
          (bundle.metadata.recovery.basis == Preserve_trx_engine_recovery::REDO_RESURRECTION &&
           bundle.metadata.recovery.owner_trx_id == verified_entry->trx_id &&
           bundle.metadata.recovery.freeze_lsn == verified_entry->freeze_lsn));
}

enum class Receiver_staged_token_prewarm_outcome {
  READY,
  CONTINUE_PREPARATION,
  WAIT_DEPENDENCY,
  RETRYABLE_NOT_READY,
  TERMINAL_TOKEN_FAILURE,
  GLOBAL_FAILURE,
  EXPIRED
};

enum class Receiver_staged_token_prewarm_stage {
  NONE,
  READY_CACHE_RECORD_LOCK_PROOF,
  READY_CACHE_BUNDLE,
  STRICT_BINLOG_PREPARED,
  STRICT_RECORD_LOCK_PREPARED,
  STRICT_REGISTRY_BEGIN_PREPARE,
  DEBUG_RETRYABLE_NOT_READY
};

const char *receiver_staged_token_prewarm_outcome_name(
    Receiver_staged_token_prewarm_outcome outcome) {
  switch (outcome) {
    case Receiver_staged_token_prewarm_outcome::READY:
      return "READY";
    case Receiver_staged_token_prewarm_outcome::CONTINUE_PREPARATION:
      return "CONTINUE_PREPARATION";
    case Receiver_staged_token_prewarm_outcome::WAIT_DEPENDENCY:
      return "WAIT_DEPENDENCY";
    case Receiver_staged_token_prewarm_outcome::RETRYABLE_NOT_READY:
      return "RETRYABLE_NOT_READY";
    case Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE:
      return "TERMINAL_TOKEN_FAILURE";
    case Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE:
      return "GLOBAL_FAILURE";
    case Receiver_staged_token_prewarm_outcome::EXPIRED:
      return "EXPIRED";
  }
  return "UNKNOWN";
}

const char *receiver_staged_token_prewarm_stage_name(
    Receiver_staged_token_prewarm_stage stage) {
  switch (stage) {
    case Receiver_staged_token_prewarm_stage::NONE:
      return "NONE";
    case Receiver_staged_token_prewarm_stage::READY_CACHE_RECORD_LOCK_PROOF:
      return "READY_CACHE_RECORD_LOCK_PROOF";
    case Receiver_staged_token_prewarm_stage::READY_CACHE_BUNDLE:
      return "READY_CACHE_BUNDLE";
    case Receiver_staged_token_prewarm_stage::STRICT_BINLOG_PREPARED:
      return "STRICT_BINLOG_PREPARED";
    case Receiver_staged_token_prewarm_stage::STRICT_RECORD_LOCK_PREPARED:
      return "STRICT_RECORD_LOCK_PREPARED";
    case Receiver_staged_token_prewarm_stage::STRICT_REGISTRY_BEGIN_PREPARE:
      return "STRICT_REGISTRY_BEGIN_PREPARE";
    case Receiver_staged_token_prewarm_stage::DEBUG_RETRYABLE_NOT_READY:
      return "DEBUG_RETRYABLE_NOT_READY";
  }
  return "UNKNOWN";
}

struct Receiver_staged_token_prewarm_result {
  Receiver_staged_token_prewarm_outcome outcome{
      Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE};
  Preserve_trx_promotion_adopt_status status{
      Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT};
  Preserve_trx_receiver_failure_reason failure_reason{
      Preserve_trx_receiver_failure_reason::NONE};
  Receiver_staged_token_prewarm_stage stage{
      Receiver_staged_token_prewarm_stage::NONE};
};

Receiver_staged_token_prewarm_result receiver_staged_token_result(
    Receiver_staged_token_prewarm_outcome outcome,
    Preserve_trx_promotion_adopt_status status,
    Preserve_trx_receiver_failure_reason failure_reason =
        Preserve_trx_receiver_failure_reason::NONE,
    Receiver_staged_token_prewarm_stage stage =
        Receiver_staged_token_prewarm_stage::NONE) {
  return {outcome, status, failure_reason, stage};
}

Receiver_staged_token_prewarm_result prepare_strict_bundle_for_receiver(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    Preserved_trx_bundle &&bundle,
    Preserve_trx_transfer_receiver_registry *receiver_registry,
    const Preserve_trx_transfer_runtime_policy &runtime_policy,
    bool defer_binlog_read_budget,
    std::unique_ptr<Preserve_trx_result_restore::Ready> *result_ready,
    Preserve_memory_lease *bundle_memory,
    const Preserve_trx_temp_id_contract &temp_id_contract,
    Preserve_trx_temp_receiver_work::Owner *temp_ready = nullptr) {
  if (preserve_trx_transfer_validate_strict_eligibility(
          manifest, bundle.metadata, false, false, 1) !=
      Preserve_trx_transfer_strict_eligibility_status::OK) {
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
        Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT,
        Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
  }
  Preserve_trx_resurrection_index_entry resurrection_entry;
  const bool no_redo = bundle.metadata.recovery.needs_no_redo_context();
  const bool resource_only = bundle.metadata.recovery.resource_only();
  const bool temp_only = bundle.metadata.recovery.basis == Preserve_trx_engine_recovery::TEMP_UNDO;
  const bool has_results = !bundle.metadata.cursor_manifest_payload.empty();
  const auto &temp_manifest = bundle.metadata.temp_table_manifest_payload;
  const bool has_temp = !temp_manifest.empty();
  if (has_temp != (temp_ready != nullptr && *temp_ready != nullptr) ||
      (has_temp && !(*temp_ready)->matches(bundle.metadata.token,
          preserve_trx_digest(temp_manifest.data(), temp_manifest.size()),
          temp_id_contract)))
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
        Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
  if (has_results && (result_ready == nullptr || !*result_ready ||
      !(*result_ready)->matches(bundle.metadata.token,
          preserve_trx_digest(bundle.metadata.cursor_manifest_payload.data(),
                                 bundle.metadata.cursor_manifest_payload.size())))) {
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
        Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
  }
  if (!no_redo && !resource_only && !resurrection_index_matches_receiver_bundle(root_dir, manifest, bundle,
                                                  receiver_registry,
                                                  &resurrection_entry)) {
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
        Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
  }
  const auto *decoded_temp = has_temp ? (*temp_ready)->input()->manifest() : nullptr;
  if ((has_temp && decoded_temp->owner_trx_id !=
          (no_redo || resource_only ? bundle.metadata.recovery.owner_trx_id : resurrection_entry.trx_id)) ||
      (resource_only && has_temp && (!(*temp_ready)->input()->resource_only() ||
          !decoded_temp->undo_images.empty() || !decoded_temp->ownership_claims.empty())) ||
      (no_redo && (!trx_preserve_read_view_payload_matches_owner(
              bundle.metadata.read_view_payload,
              bundle.metadata.recovery.owner_trx_id) ||
          (temp_only ? !has_temp || decoded_temp->undo_images.empty()
                     : has_temp && !decoded_temp->undo_images.empty()))))
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
        Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
  const bool has_record_locks =
      find_object(manifest, kPreservedTrxBlobRecordLocks) != nullptr;
  const auto *binlog_object =
      find_object(manifest, kPreservedTrxBlobBinlogCache);
  const bool has_binlog_cache =
      bundle.metadata.binlog_state ==
      Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE;
  if (has_binlog_cache != (binlog_object != nullptr)) {
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
        Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
  }

  Receiver_record_lock_prepared prepared;
  uint64_t plan_capacity_bytes = 0;

  Preserve_trx_prepared_token_key key;
  if (!strict_prepared_key_for_receiver(root_dir, manifest,
                                        bundle.metadata.token, &key)) {
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
        Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
  }
  std::string encoded_manifest;
  if (preserve_trx_transfer_encode_manifest(manifest, &encoded_manifest) !=
      Preserve_trx_transfer_status::OK) {
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
  }
  const std::string object_set_digest =
      digest_hex(sha256_digest(encoded_manifest));

  Preserve_trx_prepared_token_snapshot existing;
  if (preserved_trx_strict_prepared_token_registry().snapshot(key, &existing) ==
          Preserve_trx_prepared_status::OK &&
      (existing.state == Preserve_trx_prepared_token_state::
                             PREWARMED_PENDING_FINAL_FACT ||
       existing.state ==
           Preserve_trx_prepared_token_state::READY_FACTS_PENDING_LEASE ||
       existing.state == Preserve_trx_prepared_token_state::READY_FOR_GATE)) {
    if (!existing.resource_temp_id_contract_bound ||
        !existing.temp_resources_ready ||
        existing.resource_temp_id_contract != temp_id_contract ||
        existing.prewarm_object_set_digest != object_set_digest ||
        (existing.state != Preserve_trx_prepared_token_state::
                               PREWARMED_PENDING_FINAL_FACT &&
         existing.facts.temp_id_contract != temp_id_contract)) {
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
          Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
          Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
    }
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::READY,
        Preserve_trx_promotion_adopt_status::OK);
  }

  Mysql_binlog_preserve_cache_facts binlog_facts;
  Preserve_trx_prepared_token_resources resources;
  std::unique_ptr<Receiver_binlog_staging_payload_reader> binlog_reader;
  uint64_t native_binlog_bytes = 0;
  uint64_t native_binlog_fd_count = 0;
  uint64_t native_binlog_tmpdir_bytes = 0;
  bool native_binlog_prepared = false;
  if (has_binlog_cache) {
    Preserved_trx_external_blob_descriptor descriptor;
    descriptor.name = binlog_object->object_id;
    descriptor.size = binlog_object->total_size;
    descriptor.digest = binlog_object->digest;
    Mysql_binlog_preserve_token_identity identity;
    identity.epoch_scope = key.epoch_scope;
    identity.epoch_id = key.epoch_id;
    identity.token = key.token;
    identity.target_boot_incarnation = key.target_boot_incarnation;
    identity.generation = key.generation;
    const uint64_t binlog_incarnation =
        std::max<uint64_t>(1, static_cast<uint64_t>(server_start_time));
    if (!preserved_trx_build_native_binlog_cache_facts(
            bundle.metadata, identity, descriptor, binlog_incarnation,
            key.generation, &binlog_facts)) {
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
          Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT,
          Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
    }
    native_binlog_bytes =
        mysql_binlog_preserve_native_memory_bytes_required(binlog_facts);
    native_binlog_fd_count =
        mysql_binlog_preserve_native_fd_count_required(binlog_facts);
    native_binlog_tmpdir_bytes =
        mysql_binlog_preserve_native_tmpdir_bytes_required(binlog_facts);
    if (native_binlog_bytes == 0) {
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
          Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT,
          Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
    }
    const Receiver_binlog_take_status prepared_status =
        take_receiver_binlog_prepared(root_dir, manifest, key, *binlog_object,
                                      binlog_facts, &resources);
    if (prepared_status == Receiver_binlog_take_status::PENDING) {
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::WAIT_DEPENDENCY,
          Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
          Preserve_trx_receiver_failure_reason::NONE,
          Receiver_staged_token_prewarm_stage::STRICT_BINLOG_PREPARED);
    }
    if (prepared_status == Receiver_binlog_take_status::MISMATCH) {
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
          Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
          Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
    }
    native_binlog_prepared =
        prepared_status == Receiver_binlog_take_status::READY;
    if (!native_binlog_prepared) {
      binlog_reader = std::make_unique<Receiver_binlog_staging_payload_reader>(
          transfer_object_path(root_dir, manifest, *binlog_object),
          binlog_object->total_size);
      if (!binlog_reader->opened()) {
        return receiver_staged_token_result(
            Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
            Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
            Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
      }
    }
  }

  if (has_record_locks &&
      (!take_receiver_record_lock_prepared(root_dir, manifest, &prepared) ||
       prepared.plan == nullptr || !prepared.plan->ready())) {
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::WAIT_DEPENDENCY,
        Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
        Preserve_trx_receiver_failure_reason::NONE,
        Receiver_staged_token_prewarm_stage::STRICT_RECORD_LOCK_PREPARED);
  }
  plan_capacity_bytes =
      prepared.plan == nullptr ? 0 : prepared.plan->capacity_bytes();
  auto restore_unconsumed_plan = create_scope_guard([&] {
    if (prepared.plan != nullptr) {
      (void)put_receiver_record_lock_prepared(
          root_dir, manifest, std::move(prepared.plan), prepared.facts,
          std::move(prepared.memory_lease));
    }
  });

  auto &registry = preserved_trx_strict_prepared_token_registry();
  Preserve_trx_prepare_lease prepare;
  const Preserve_trx_prepared_status begin_status =
      registry.begin_prepare(key, key.generation, &prepare);
  if (begin_status != Preserve_trx_prepared_status::OK) {
    if (begin_status == Preserve_trx_prepared_status::ALREADY_CLAIMED) {
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::RETRYABLE_NOT_READY,
          Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
          Preserve_trx_receiver_failure_reason::NONE,
          Receiver_staged_token_prewarm_stage::STRICT_REGISTRY_BEGIN_PREPARE);
    }
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
  }
  auto semantic_bundle =
      std::make_unique<Preserved_trx_bundle>(std::move(bundle));
  std::string().swap(semantic_bundle->metadata.binlog_cache_payload);
  std::vector<Preserved_trx_external_blob>().swap(
      semantic_bundle->external_blobs);
  uint64_t retained_bytes = 0;
  if (bundle_memory != nullptr &&
      (Preserve_trx_receiver_prepare_work::retained_bytes(
           *semantic_bundle, &retained_bytes) ||
       !bundle_memory->shrink_to(retained_bytes))) {
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
  }
  const uint64_t lock_plan_bytes_to_acquire =
      prepared.memory_lease.acquired() ? 0 : plan_capacity_bytes;
  if (!native_binlog_prepared) {
    const Preserve_trx_prepared_status acquire_status =
        preserved_trx_acquire_prepared_token_resources(
            key, lock_plan_bytes_to_acquire, native_binlog_bytes,
            native_binlog_fd_count, native_binlog_tmpdir_bytes, &resources);
    if (acquire_status != Preserve_trx_prepared_status::OK) {
      if (acquire_status == Preserve_trx_prepared_status::RESOURCE_EXHAUSTED) {
        return receiver_staged_token_result(
            Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
            Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
            Preserve_trx_receiver_failure_reason::TOKEN_RESOURCE_LIMIT);
      }
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
          Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
    }
    if (binlog_reader != nullptr) {
      if (defer_binlog_read_budget) {
        /* Metadata-only loading did not read the payload. Charge only this
           fallback; taking an already prepared native cache performs no IO. */
        throttle_receiver_prewarm_io(binlog_object->total_size, runtime_policy);
        DBUG_EXECUTE_IF("preserve_trx_receiver_trace_staged_io_budget", {
          LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                 ("PRESERVE: staged raw binlog file-read budget bytes=" +
                  std::to_string(binlog_object->total_size) + " epoch=" +
                  manifest.epoch_id + " token=" +
                  std::to_string(manifest.token)).c_str());
        });
      }
      if (resources.prepare_native_binlog_handle_for_receiver(
              binlog_facts, binlog_reader.get()) !=
          Mysql_binlog_preserve_cache_status::OK) {
        return receiver_staged_token_result(
            Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
            Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT,
            Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
      }
    }
  }
  if (resources.install_semantic_bundle(
          std::move(semantic_bundle),
          bundle_memory ? std::move(*bundle_memory) : Preserve_memory_lease{},
          temp_id_contract) !=
      Preserve_trx_prepared_status::OK) {
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
  }
  if (result_ready != nullptr && resources.install_result_ready(result_ready) !=
                                 Preserve_trx_prepared_status::OK) {
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
  }
  if (!no_redo && !resource_only && resources.install_resurrection_entry(
          std::make_unique<Preserve_trx_resurrection_index_entry>(
              std::move(resurrection_entry))) !=
      Preserve_trx_prepared_status::OK) {
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
  }
  if (prepared.plan != nullptr) {
    const Preserve_trx_prepared_status install_status =
        prepared.memory_lease.acquired()
            ? resources.install_record_lock_plan_with_memory_lease(
                  std::move(prepared.plan),
                  std::move(prepared.memory_lease))
            : resources.install_record_lock_plan(std::move(prepared.plan));
    if (install_status != Preserve_trx_prepared_status::OK) {
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
          Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
    }
  }
  if (!resources.acquired()) {
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
  }
  /* An early native manager retains its creation-time cache configuration. */
  native_binlog_bytes = resources.native_binlog_bytes();
  const bool native_binlog_file_backed = resources.native_binlog_file_backed();
  if ((has_results || has_temp) && receiver_epoch_expired_or_removed(root_dir, receiver_registry, manifest))
    return receiver_staged_token_result(Receiver_staged_token_prewarm_outcome::EXPIRED,
                                       Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY);
  if (has_temp && resources.install_temp_ready(temp_ready) !=
                      Preserve_trx_prepared_status::OK)
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
  const auto status = registry.publish_prewarmed(
      &prepare, object_set_digest, std::move(resources));
  if (status != Preserve_trx_prepared_status::OK &&
      status != Preserve_trx_prepared_status::IDEMPOTENT) {
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
  }
  if (has_record_locks) {
    preserved_trx_promotion_prepared_note_lock_plan_metrics(
        plan_capacity_bytes,
        preserve_trx_resource_kind_current_bytes(
            Preserve_trx_memory_kind::PROMOTION_LOCK_PLAN),
        preserve_trx_resource_kind_cap_bytes(
            Preserve_trx_memory_kind::PROMOTION_LOCK_PLAN));
  }

  if (has_record_locks) {
    Receiver_strict_record_lock_facts pending;
    pending.facts = prepared.facts;
    pending.plan_capacity_bytes = plan_capacity_bytes;
    std::lock_guard<std::mutex> guard(
        g_receiver_strict_record_lock_facts_mutex);
    g_receiver_strict_record_lock_facts[{root_dir, manifest.epoch_id,
                                         manifest.token}] = std::move(pending);
  }
  if (has_binlog_cache) {
    Receiver_strict_binlog_facts pending;
    pending.handle_digest = binlog_facts.canonical_digest;
    pending.cache_length = binlog_facts.cache_length;
    pending.memory_bytes = native_binlog_bytes;
    pending.file_backed = native_binlog_file_backed;
    std::lock_guard<std::mutex> guard(g_receiver_strict_binlog_facts_mutex);
    g_receiver_strict_binlog_facts[{root_dir, manifest.epoch_id,
                                    manifest.token}] = std::move(pending);
  }
  return receiver_staged_token_result(
      Receiver_staged_token_prewarm_outcome::READY,
      Preserve_trx_promotion_adopt_status::OK);
}

std::string strict_empty_set_digest(const char *domain) {
  return digest_hex(sha256_digest(std::string("PTRX_EMPTY_") + domain));
}

void bind_strict_prepared_tokens_from_epoch_fact(
    const std::string &root_dir,
    const Preserve_trx_transfer_epoch_fact &fact,
    Preserve_trx_transfer_receiver_registry *receiver_registry,
    const Preserve_trx_transfer_epoch_fact_token *single_token = nullptr) {
  if (!transfer_trx_id_store_fact_is_valid(fact.trx_id_store,
                                           fact.source_fence_lsn)) {
    return;
  }
  Preserve_trx_transfer_accepted_epoch accepted;
  if (receiver_registry == nullptr ||
      receiver_registry->query_accepted_epoch(root_dir, fact.epoch_id,
                                              &accepted) !=
          Preserve_trx_transfer_status::COMMITTED_NOT_READY) {
    return;
  }
  auto &registry = preserved_trx_strict_prepared_token_registry();
  uint64_t fact_loaded_us = 0;
  {
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    const auto found =
        g_receiver_ready_epoch_state.find({root_dir, fact.epoch_id});
    if (found != g_receiver_ready_epoch_state.end() &&
        found->second.fact_loaded) {
      fact_loaded_us = found->second.fact_loaded_monotonic_us;
    }
  }
  if (fact_loaded_us == 0) return;
  const std::string epoch_fact_digest = digest_hex(fact.fact_digest);

  const auto bind_token =
      [&](const Preserve_trx_transfer_epoch_fact_token &token) {
    const auto find_token_object = [&](const char *object_id) {
      return std::find_if(token.objects.begin(), token.objects.end(),
                          [&](const auto &object) {
                            return object.object_id == object_id;
                          });
    };
    const auto record_lock_object =
        find_token_object(kPreservedTrxBlobRecordLocks);
    const bool has_record_locks = record_lock_object != token.objects.end();
    const bool has_binlog_cache =
        find_token_object(kPreservedTrxBlobBinlogCache) != token.objects.end();
    if (has_record_locks &&
        record_lock_object->lock_plan.simulated_terminal_proof &&
        !simulated_terminal_lock_proof_matches(
            fact.epoch_id, token.token, *record_lock_object)) {
      return;
    }

    Receiver_strict_record_lock_facts record_lock_facts;
    if (has_record_locks) {
      std::lock_guard<std::mutex> guard(
          g_receiver_strict_record_lock_facts_mutex);
      const auto found = g_receiver_strict_record_lock_facts.find(
          {root_dir, fact.epoch_id, token.token});
      if (found == g_receiver_strict_record_lock_facts.end()) return;
      record_lock_facts = found->second;
    }
    Receiver_strict_binlog_facts binlog_facts;
    if (has_binlog_cache) {
      std::lock_guard<std::mutex> guard(
          g_receiver_strict_binlog_facts_mutex);
      const auto found = g_receiver_strict_binlog_facts.find(
          {root_dir, fact.epoch_id, token.token});
      if (found == g_receiver_strict_binlog_facts.end()) return;
      binlog_facts = found->second;
    }

    Preserve_trx_transfer_manifest manifest;
    manifest.epoch_id = fact.epoch_id;
    manifest.token = token.token;
    manifest.source_freeze_lsn = token.source_freeze_lsn;
    manifest.source_epoch_commit_lsn = token.source_epoch_commit_lsn;
    manifest.objects = token.objects;
    Preserve_trx_prepared_token_key key;
    if (!strict_prepared_key_for_receiver(root_dir, manifest,
                                          std::to_string(token.token), &key)) {
      return;
    }
    Preserve_trx_final_token_facts facts;
    facts.required_apply_lsn = token.source_epoch_commit_lsn;
    facts.physical_fence_lsn = fact.source_fence_lsn;
    facts.source_trx_id_store = fact.trx_id_store.source_trx_id_store;
    facts.source_trx_id_store_lsn =
        fact.trx_id_store.source_trx_id_store_lsn;
    facts.source_safe_next_trx_id_floor =
        fact.trx_id_store.source_safe_next_trx_id_floor;
    facts.epoch_fact_digest = epoch_fact_digest;
    facts.temp_id_contract = accepted.temp_id_contract;
    facts.final_lock_generation_digest =
        has_record_locks
            ? record_lock_facts.facts.final_lock_generation_digest
            : strict_empty_set_digest("LOCK_GENERATION");
    facts.page_layout_digest =
        has_record_locks ? record_lock_facts.facts.page_layout_digest
                         : strict_empty_set_digest("PAGE_LAYOUT");
    facts.dictionary_generation_digest =
        has_record_locks
            ? record_lock_facts.facts.dictionary_generation_digest
            : strict_empty_set_digest("DICTIONARY_GENERATION");
    facts.prewarm_object_set_digest = digest_hex(token.manifest_digest);
    facts.target_boot_incarnation = key.target_boot_incarnation;
    facts.epoch_prepare_deadline_us = accepted.deadline_monotonic_us;
    facts.client_resume_deadline_us = accepted.deadline_monotonic_us;
    if (has_record_locks) {
      facts.record_lock_unique_pages =
          record_lock_facts.facts.unique_pages;
      facts.record_lock_bitmap_entries =
          record_lock_facts.facts.bitmap_entries;
      facts.record_lock_bits = record_lock_facts.facts.bitmap_bits;
      facts.lock_plan_capacity_bytes =
          record_lock_facts.plan_capacity_bytes;
      facts.predicate_lock_present =
          record_lock_facts.facts.predicate_lock_present;
    }
    if (has_binlog_cache) {
      facts.native_binlog_capacity_bytes = binlog_facts.memory_bytes;
      facts.binlog_cache_length = binlog_facts.cache_length;
      facts.binlog_cache_memory_bytes = binlog_facts.memory_bytes;
      facts.binlog_cache_present = true;
      facts.binlog_cache_file_backed = binlog_facts.file_backed;
      facts.binlog_handle_digest = binlog_facts.handle_digest;
    }
    facts.semantic_validated = true;
    facts.lock_plan_ready = true;
    facts.binlog_handle_ready = true;
    facts.resources_reserved = true;

    Preserve_trx_prepared_token_snapshot existing;
    if (registry.snapshot(key, &existing) ==
            Preserve_trx_prepared_status::OK &&
        (existing.state ==
             Preserve_trx_prepared_token_state::READY_FACTS_PENDING_LEASE ||
         existing.state ==
             Preserve_trx_prepared_token_state::READY_FOR_GATE) &&
        existing.facts.required_apply_lsn == facts.required_apply_lsn &&
        existing.facts.physical_fence_lsn == facts.physical_fence_lsn &&
        existing.facts.epoch_fact_digest == facts.epoch_fact_digest &&
        existing.resource_temp_id_contract_bound &&
        existing.resource_temp_id_contract == facts.temp_id_contract &&
        existing.facts.temp_id_contract == facts.temp_id_contract &&
        existing.facts.final_lock_generation_digest ==
            facts.final_lock_generation_digest &&
        existing.facts.page_layout_digest == facts.page_layout_digest &&
        existing.facts.dictionary_generation_digest ==
            facts.dictionary_generation_digest &&
        existing.facts.prewarm_object_set_digest ==
            facts.prewarm_object_set_digest &&
        existing.facts.target_boot_incarnation ==
            facts.target_boot_incarnation &&
        existing.facts.record_lock_unique_pages ==
            facts.record_lock_unique_pages &&
        existing.facts.record_lock_bitmap_entries ==
            facts.record_lock_bitmap_entries &&
        existing.facts.record_lock_bits == facts.record_lock_bits &&
        existing.facts.lock_plan_capacity_bytes ==
            facts.lock_plan_capacity_bytes &&
        existing.facts.native_binlog_capacity_bytes ==
            facts.native_binlog_capacity_bytes &&
        existing.facts.binlog_cache_length == facts.binlog_cache_length &&
        existing.facts.binlog_cache_memory_bytes ==
            facts.binlog_cache_memory_bytes &&
        existing.facts.binlog_cache_present == facts.binlog_cache_present &&
        existing.facts.binlog_cache_file_backed ==
            facts.binlog_cache_file_backed &&
        existing.facts.binlog_handle_digest == facts.binlog_handle_digest) {
      return;
    }

#ifndef NDEBUG
    const auto hook = g_before_final_fact_bind_hook_for_unit_test.load();
    if (hook != nullptr) hook();
#endif
    const auto bind_status =
        registry.bind_final_facts(key, key.generation, std::move(facts));
    if (bind_status != Preserve_trx_prepared_status::OK &&
        bind_status != Preserve_trx_prepared_status::IDEMPOTENT) {
      return;
    }
    const auto ready_status =
        registry.mark_ready_for_gate(key, key.generation);
    if (has_record_locks &&
        (ready_status == Preserve_trx_prepared_status::OK ||
         ready_status == Preserve_trx_prepared_status::IDEMPOTENT)) {
      std::lock_guard<std::mutex> guard(
          g_receiver_strict_record_lock_facts_mutex);
      g_receiver_strict_record_lock_facts.erase(
          {root_dir, fact.epoch_id, token.token});
    }
    if (has_binlog_cache &&
        (ready_status == Preserve_trx_prepared_status::OK ||
         ready_status == Preserve_trx_prepared_status::IDEMPOTENT)) {
      std::lock_guard<std::mutex> guard(
          g_receiver_strict_binlog_facts_mutex);
      g_receiver_strict_binlog_facts.erase(
          {root_dir, fact.epoch_id, token.token});
    }
  };
  if (single_token != nullptr) {
    bind_token(*single_token);
    return;
  }
  for (const Preserve_trx_transfer_epoch_fact_token &token : fact.tokens) {
    bind_token(token);
  }
}

void cache_receiver_epoch_fact(
    const std::string &root_dir,
    std::shared_ptr<const Preserve_trx_transfer_epoch_fact> fact) {
  if (fact == nullptr) return;
  std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
  Receiver_epoch_ready_state &state =
      g_receiver_ready_epoch_state[{root_dir, fact->epoch_id}];
  if (state.fact_loaded) return;
  state.fact_tokens.clear();
  state.fact_token_indexes.clear();
  for (size_t index = 0; index < fact->tokens.size(); ++index) {
    const uint64_t token = fact->tokens[index].token;
    state.fact_tokens.insert(token);
    state.fact_token_indexes.emplace(token, index);
  }
  state.ready_fact_token_count = 0;
  state.classified_fact_token_count = 0;
  state.failed_fact_token_count = 0;
  for (const auto &result : state.token_results) {
    if (state.fact_tokens.count(result.first) == 0) continue;
    ++state.classified_fact_token_count;
    if (result.second == Preserve_trx_receiver_failure_reason::NONE) {
      ++state.ready_fact_token_count;
    } else {
      ++state.failed_fact_token_count;
    }
  }
  state.fact = std::move(fact);
  state.fact_loaded_monotonic_us =
      std::max<uint64_t>(1, transfer_monotonic_us());
  state.fact_loaded = true;
}

void bind_strict_prepared_token_from_cached_epoch_fact(
    const std::string &root_dir, const std::string &epoch_id, uint64_t token,
    Preserve_trx_transfer_receiver_registry *registry) {
  std::shared_ptr<const Preserve_trx_transfer_epoch_fact> fact;
  size_t token_index = 0;
  {
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    const auto state_it =
        g_receiver_ready_epoch_state.find({root_dir, epoch_id});
    if (state_it == g_receiver_ready_epoch_state.end() ||
        !state_it->second.fact_loaded || state_it->second.fact == nullptr) {
      return;
    }
    const auto token_it = state_it->second.fact_token_indexes.find(token);
    if (token_it == state_it->second.fact_token_indexes.end()) return;
    fact = state_it->second.fact;
    token_index = token_it->second;
  }
  if (token_index >= fact->tokens.size()) return;
  bind_strict_prepared_tokens_from_epoch_fact(
      root_dir, *fact, registry, &fact->tokens[token_index]);
}

void bind_strict_prepared_tokens_from_committed_epoch(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_receiver_registry *registry) {
  if (!preserve_trx_transfer_epoch_committed(root_dir, epoch_id)) return;
  Preserve_trx_transfer_epoch_fact loaded_fact;
  if (preserve_trx_transfer_read_epoch_fact(root_dir, epoch_id, &loaded_fact) !=
      Preserve_trx_transfer_status::OK) {
    return;
  }
  auto fact = std::make_shared<const Preserve_trx_transfer_epoch_fact>(
      std::move(loaded_fact));
  cache_receiver_epoch_fact(root_dir, fact);
  bind_strict_prepared_tokens_from_epoch_fact(root_dir, *fact, registry);
}

Preserve_trx_transfer_receiver_registry &default_receiver_registry() {
  static Preserve_trx_transfer_receiver_registry registry;
  return registry;
}

Preserve_trx_transfer_frame_sink_factory &configured_frame_sink_factory() {
  static Preserve_trx_transfer_frame_sink_factory factory = nullptr;
  return factory;
}

std::atomic<uint> &receiver_staged_prewarm_delay_ms_for_unit_test() {
  static std::atomic<uint> delay_ms{0};
  return delay_ms;
}

std::atomic<uint> &receiver_object_prewarm_delay_ms_for_unit_test() {
  static std::atomic<uint> delay_ms{0};
  return delay_ms;
}

uint64_t transfer_token_retention_timeout_seconds() {
  return (static_cast<uint64_t>(preserve_trx_token_retention_timeout_ms) +
          999) /
         1000;
}

void note_receiver_first_frame_saved() {
  uint64_t expected = 0;
  (void)g_receiver_first_frame_monotonic_us.compare_exchange_strong(
      expected, transfer_monotonic_us());
}

void note_receiver_prewarm_start() {
  uint64_t expected = 0;
  (void)g_receiver_prewarm_start_monotonic_us.compare_exchange_strong(
      expected, transfer_monotonic_us());
}

void note_receiver_prewarm_end() {
  const uint64_t finished_us = transfer_monotonic_us();
  uint64_t current = g_receiver_prewarm_end_monotonic_us.load();
  while (finished_us > current &&
         !g_receiver_prewarm_end_monotonic_us.compare_exchange_weak(
             current, finished_us)) {
  }
}

void note_receiver_staged_token_total_us(uint64_t elapsed_us) {
  g_receiver_staged_token_total_us.fetch_add(elapsed_us);
  uint64_t current = g_receiver_staged_token_max_us.load();
  while (elapsed_us > current &&
         !g_receiver_staged_token_max_us.compare_exchange_weak(current,
                                                               elapsed_us)) {
  }
}

void update_receiver_max_us(std::atomic<uint64_t> *max_value,
                            uint64_t elapsed_us) {
  if (max_value == nullptr) return;
  uint64_t current = max_value->load();
  while (elapsed_us > current &&
         !max_value->compare_exchange_weak(current, elapsed_us)) {
  }
}

void update_receiver_first_us(std::atomic<uint64_t> *first_value,
                              uint64_t started_us) {
  if (first_value == nullptr || started_us == 0) return;
  uint64_t current = first_value->load();
  while ((current == 0 || started_us < current) &&
         !first_value->compare_exchange_weak(current, started_us)) {
  }
}

void note_receiver_staged_token_job_started() {
  const uint64_t active = g_receiver_staged_token_active.fetch_add(1) + 1;
  uint64_t current = g_receiver_staged_token_max_active.load();
  while (active > current &&
         !g_receiver_staged_token_max_active.compare_exchange_weak(current,
                                                                   active)) {
  }
}

void note_receiver_staged_token_job_finished() {
  uint64_t current = g_receiver_staged_token_active.load();
  while (current != 0 &&
         !g_receiver_staged_token_active.compare_exchange_weak(current,
                                                               current - 1)) {
  }
}

void note_receiver_object_prewarm_elapsed(uint64_t started_us,
                                          uint64_t elapsed_us,
                                          bool record_lock_object,
                                          bool binlog_object) {
  const uint64_t finished_us = started_us + elapsed_us;
  uint64_t expected_start = 0;
  (void)g_receiver_object_prewarm_start_monotonic_us.compare_exchange_strong(
      expected_start, started_us);
  update_receiver_first_us(&g_receiver_object_prewarm_first_start_us,
                           started_us);
  update_receiver_max_us(&g_receiver_object_prewarm_last_end_us, finished_us);
  g_receiver_object_prewarm_count.fetch_add(1);
  g_receiver_object_prewarm_us.fetch_add(elapsed_us);
  update_receiver_max_us(&g_receiver_object_prewarm_max_us, elapsed_us);
  if (record_lock_object) {
    update_receiver_first_us(&g_receiver_record_object_prewarm_first_start_us,
                             started_us);
    update_receiver_max_us(&g_receiver_record_object_prewarm_last_end_us,
                           finished_us);
    g_receiver_record_object_prewarm_count.fetch_add(1);
    g_receiver_record_object_prewarm_us.fetch_add(elapsed_us);
    update_receiver_max_us(&g_receiver_record_object_prewarm_max_us,
                           elapsed_us);
  }
  if (binlog_object) {
    update_receiver_first_us(&g_receiver_binlog_object_prewarm_first_start_us,
                             started_us);
    update_receiver_max_us(&g_receiver_binlog_object_prewarm_last_end_us,
                           finished_us);
  }
}

void refresh_receiver_ready_after_final_metadata() {
  const uint64_t final_us = g_receiver_final_metadata_saved_us.load();
  const uint64_t ready_us = g_receiver_ready_monotonic_us.load();
  if (final_us == 0 || ready_us == 0) return;
  g_receiver_ready_after_final_metadata_us.store(
      ready_us > final_us ? ready_us - final_us : 0);
}

void refresh_receiver_ready_after_final_spool_ack() {
  const uint64_t ack_us = g_receiver_final_spool_ack_monotonic_us.load();
  const uint64_t ready_us = g_receiver_ready_monotonic_us.load();
  if (ack_us == 0 || ready_us == 0) return;
  g_receiver_ready_after_final_spool_ack_us.store(
      ready_us > ack_us ? ready_us - ack_us : 0);
}

void refresh_receiver_ready_after_final_metadata_accepted() {
  const uint64_t accepted_us =
      g_receiver_final_metadata_accepted_monotonic_us.load();
  const uint64_t ready_us = g_receiver_ready_monotonic_us.load();
  g_receiver_ready_after_final_metadata_accepted_us.store(
      accepted_us != 0 && ready_us > accepted_us ? ready_us - accepted_us : 0);
}

void refresh_receiver_ready_after_terminal_commit_admitted() {
  const uint64_t admitted_us =
      g_receiver_terminal_commit_admitted_monotonic_us.load();
  const uint64_t ready_us = g_receiver_ready_monotonic_us.load();
  g_receiver_ready_after_terminal_commit_admitted_us.store(
      admitted_us != 0 && ready_us > admitted_us ? ready_us - admitted_us : 0);
}

void note_receiver_seal_prewarm_status(
    Preserve_trx_promotion_adopt_status status) {
  g_receiver_seal_prewarm_tokens.fetch_add(1);
  g_receiver_seal_prewarm_last_status.store(static_cast<uint64_t>(status));
  if (status == Preserve_trx_promotion_adopt_status::OK) {
    g_receiver_seal_prewarm_success_tokens.fetch_add(1);
  } else {
    g_receiver_seal_prewarm_not_ready_tokens.fetch_add(1);
  }
}

bool note_receiver_epoch_token_result(
    const std::string &root_dir, const std::string &epoch_id, uint64_t token,
    Preserve_trx_receiver_failure_reason reason) {
  std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
  Receiver_epoch_ready_state &state =
      g_receiver_ready_epoch_state[{root_dir, epoch_id}];
  if (state.selection_published || state.binding) {
    return false;
  }
  const auto existing = state.token_results.find(token);
  if (existing != state.token_results.end() && existing->second != reason) {
    state.global_failure = true;
    return false;
  }
  const bool inserted = existing == state.token_results.end();
  const bool is_ready = reason == Preserve_trx_receiver_failure_reason::NONE;
  state.token_results[token] = reason;
  if (inserted && state.fact_loaded && state.fact_tokens.count(token) != 0) {
    ++state.classified_fact_token_count;
    if (is_ready) {
      ++state.ready_fact_token_count;
    } else {
      ++state.failed_fact_token_count;
    }
  }
  return state.fact_loaded && !state.global_failure &&
         state.classified_fact_token_count == state.fact_tokens.size() &&
         state.failed_fact_token_count != 0;
}

void note_receiver_epoch_global_failure(const std::string &root_dir,
                                        const std::string &epoch_id) {
  std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
  Receiver_epoch_ready_state &state =
      g_receiver_ready_epoch_state[{root_dir, epoch_id}];
  state.global_failure = true;
}

bool consume_receiver_token_local_failure_injection(
    const std::string &root_dir, const std::string &epoch_id) {
#ifdef DBUG_OFF
  (void)root_dir;
  (void)epoch_id;
  return false;
#else
  bool inject = false;
  const auto key = std::make_pair(root_dir, epoch_id);
  DBUG_EXECUTE_IF("preserve_trx_receiver_fail_one_token_prewarm", {
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    Receiver_epoch_ready_state &state = g_receiver_ready_epoch_state[key];
    if (!state.selection_published && !state.debug_failure_injected) {
      state.debug_failure_injected = true;
      inject = true;
    }
  });
  return inject;
#endif
}

bool consume_receiver_token_retryable_not_ready_injection(
    const std::string &root_dir, const std::string &epoch_id) {
#ifdef DBUG_OFF
  (void)root_dir;
  (void)epoch_id;
  return false;
#else
  bool inject = false;
  const auto key = std::make_pair(root_dir, epoch_id);
  DBUG_EXECUTE_IF("preserve_trx_receiver_retry_one_token_prewarm", {
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    Receiver_epoch_ready_state &state = g_receiver_ready_epoch_state[key];
    if (!state.selection_published && !state.debug_retry_injected) {
      state.debug_retry_injected = true;
      inject = true;
    }
  });
  return inject;
#endif
}

bool receiver_epoch_selection_complete_with_failures(
    const std::string &root_dir, const std::string &epoch_id) {
  std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
  const auto found = g_receiver_ready_epoch_state.find({root_dir, epoch_id});
  if (found == g_receiver_ready_epoch_state.end()) return false;
  const Receiver_epoch_ready_state &state = found->second;
  return state.fact_loaded && !state.global_failure && !state.binding &&
         !state.selection_published &&
         state.classified_fact_token_count == state.fact_tokens.size() &&
         state.failed_fact_token_count != 0;
}

bool receiver_epoch_token_result_known(const std::string &root_dir,
                                       const std::string &epoch_id,
                                       uint64_t token) {
  std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
  const auto epoch = g_receiver_ready_epoch_state.find({root_dir, epoch_id});
  return epoch != g_receiver_ready_epoch_state.end() &&
         epoch->second.token_results.count(token) != 0;
}

bool receiver_seal_prewarm_token_ok(const std::string &root_dir,
                                    const std::string &epoch_id,
                                    uint64_t token) {
  std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
  const auto epoch = g_receiver_ready_epoch_state.find({root_dir, epoch_id});
  if (epoch == g_receiver_ready_epoch_state.end()) return false;
  const auto result = epoch->second.token_results.find(token);
  return result != epoch->second.token_results.end() &&
         result->second == Preserve_trx_receiver_failure_reason::NONE;
}

bool receiver_strict_token_ready(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest) {
  Preserve_trx_prepared_token_key key;
  if (!strict_prepared_key_for_receiver(
          root_dir, manifest, transfer_token_component(manifest.token),
          &key)) {
    return false;
  }
  Preserve_trx_prepared_token_snapshot snapshot;
  return preserved_trx_strict_prepared_token_registry().snapshot(
             key, &snapshot) == Preserve_trx_prepared_status::OK &&
         snapshot.state == Preserve_trx_prepared_token_state::READY_FOR_GATE;
}

static uint64_t receiver_epoch_deadline_after_ms(uint64_t now_us,
                                                 uint64_t timeout_ms) {
  if (timeout_ms >
      (std::numeric_limits<uint64_t>::max() - now_us) / 1000ULL) {
    return std::numeric_limits<uint64_t>::max();
  }
  return now_us + timeout_ms * 1000ULL;
}

bool synchronize_receiver_epoch_ready_deadline(
    const std::string &root_dir,
    const Preserve_trx_transfer_epoch_fact &fact,
    Preserve_trx_transfer_receiver_registry *registry) {
  if (registry == nullptr) return true;
  Preserve_trx_transfer_accepted_epoch accepted;
  if (registry->query_accepted_epoch(root_dir, fact.epoch_id, &accepted) !=
      Preserve_trx_transfer_status::COMMITTED_NOT_READY) {
    return false;
  }

  const uint64_t now_us = transfer_monotonic_us();
  uint64_t ready_deadline_us = accepted.deadline_monotonic_us;
  if (accepted.lifecycle ==
      Preserve_trx_transfer_epoch_lifecycle::PREWARMING) {
    ready_deadline_us = receiver_epoch_deadline_after_ms(
        now_us, preserve_trx_token_retention_timeout_ms);
  } else if (accepted.lifecycle !=
             Preserve_trx_transfer_epoch_lifecycle::READY) {
    return false;
  }

  if (preserved_trx_strict_prepared_token_registry()
          .update_epoch_prepare_deadline(receiver_boot_incarnation(),
                                         fact.epoch_id, fact.tokens.size(),
                                         ready_deadline_us) !=
      Preserve_trx_prepared_status::OK) {
    return false;
  }
  if (accepted.lifecycle == Preserve_trx_transfer_epoch_lifecycle::READY) {
    return true;
  }
  return registry->mark_accepted_epoch_ready(root_dir, fact.epoch_id, now_us,
                                             ready_deadline_us) ==
         Preserve_trx_transfer_status::OK;
}

bool publish_receiver_epoch_ready_from_fact_if_possible(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_receiver_registry *registry);

bool publish_receiver_epoch_ready_from_seal_prewarm(
    const std::string &root_dir,
    const std::vector<Preserve_trx_transfer_manifest> &manifests,
    Preserve_trx_transfer_receiver_registry *registry) {
  if (manifests.empty()) return false;
  return publish_receiver_epoch_ready_from_fact_if_possible(
      root_dir, manifests.front().epoch_id, registry);
}

bool publish_receiver_epoch_ready_from_fact_if_possible_impl(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_receiver_registry *registry) {
  if (registry != nullptr &&
      registry->accepted_epoch_is_expired(root_dir, epoch_id)) {
    purge_receiver_epoch_derived_state(root_dir, epoch_id,
                                       receiver_boot_incarnation());
    return false;
  }

  Receiver_epoch_binding_guard binding_guard(root_dir, epoch_id);
  Receiver_temp_activity_guard temp_activity(root_dir, epoch_id, true);
  std::vector<uint64_t> tokens;
  std::shared_ptr<const Preserve_trx_transfer_epoch_fact> fact;
  bool already_bound = false;
  {
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    Receiver_epoch_ready_state &state =
        g_receiver_ready_epoch_state[{root_dir, epoch_id}];
    if (state.bound) {
      already_bound = true;
      fact = state.fact;
    }
    if (state.binding || state.selection_published) {
      return false;
    }
    if (!already_bound && state.fact_loaded) {
      if (state.fact == nullptr ||
          state.ready_fact_token_count != state.fact_tokens.size()) {
        return false;
      }
      state.binding = true;
      binding_guard.arm(++state.binding_generation);
      if (g_receiver_epoch_bind_bad_alloc_for_unit_test.load()) {
        throw std::bad_alloc();
      }
      fact = state.fact;
      tokens.assign(state.fact_tokens.begin(), state.fact_tokens.end());
    }
  }

  if (!already_bound && fact == nullptr) {
    if (!preserve_trx_transfer_epoch_committed(root_dir, epoch_id)) {
      return false;
    }
    Preserve_trx_transfer_epoch_fact loaded_fact;
    if (preserve_trx_transfer_read_epoch_fact(root_dir, epoch_id, &loaded_fact) !=
        Preserve_trx_transfer_status::OK) {
      return false;
    }
    auto loaded =
        std::make_shared<const Preserve_trx_transfer_epoch_fact>(
            std::move(loaded_fact));
    cache_receiver_epoch_fact(root_dir, loaded);
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    Receiver_epoch_ready_state &state =
        g_receiver_ready_epoch_state[{root_dir, epoch_id}];
    if (state.bound) {
      already_bound = true;
      fact = state.fact;
    }
    if (state.binding || state.selection_published) {
      return false;
    }
    if (!already_bound &&
        (state.fact == nullptr ||
         state.ready_fact_token_count != state.fact_tokens.size())) {
      return false;
    }
    if (!already_bound) {
      fact = state.fact;
      state.binding = true;
      binding_guard.arm(++state.binding_generation);
      if (g_receiver_epoch_bind_bad_alloc_for_unit_test.load()) {
        throw std::bad_alloc();
      }
      tokens.assign(state.fact_tokens.begin(), state.fact_tokens.end());
    }
  }

  if (registry != nullptr &&
      !registry->accepted_epoch_is_live(root_dir, epoch_id)) {
    purge_receiver_epoch_derived_state(root_dir, epoch_id,
                                       receiver_boot_incarnation());
    return false;
  }
  if (already_bound) {
    if (registry == nullptr) return true;
    if (fact != nullptr &&
        synchronize_receiver_epoch_ready_deadline(root_dir, *fact,
                                                  registry)) {
      return true;
    }
    purge_receiver_epoch_derived_state(root_dir, epoch_id,
                                       receiver_boot_incarnation());
    return false;
  }

  for (const Preserve_trx_transfer_epoch_fact_token &fact_token :
       fact->tokens) {
    Preserve_trx_transfer_manifest strict_manifest;
    strict_manifest.epoch_id = fact->epoch_id;
    strict_manifest.token = fact_token.token;
    strict_manifest.source_freeze_lsn = fact_token.source_freeze_lsn;
    strict_manifest.source_epoch_commit_lsn =
        fact_token.source_epoch_commit_lsn;
    strict_manifest.objects = fact_token.objects;
    if (!receiver_strict_token_ready(root_dir, strict_manifest)) return false;
  }

  uint64_t ready_tokens = 0;
  g_receiver_epoch_ready_bind_attempts.fetch_add(1);
  const Preserve_trx_promotion_adopt_status bind_status =
      preserved_trx_promotion_bind_prewarmed_epoch_fact_for_receiver(
          root_dir, *fact, tokens, &ready_tokens);
  if (bind_status != Preserve_trx_promotion_adopt_status::OK ||
      ready_tokens != tokens.size()) {
    return false;
  }

  if (!synchronize_receiver_epoch_ready_deadline(root_dir, *fact, registry)) {
    purge_receiver_epoch_derived_state(root_dir, epoch_id,
                                       receiver_boot_incarnation());
    return false;
  }

  const uint64_t total_tokens = tokens.size();
  uint64_t ready_monotonic_us = 0;
  uint64_t readiness_final_ack_us = 0;
  uint64_t readiness_ready_us = 0;
  bool log_readiness_timing = false;
  Preserve_trx_temp_final_sample temp_final;
  {
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    Receiver_epoch_ready_state &state =
        g_receiver_ready_epoch_state[{root_dir, epoch_id}];
    state.binding = false;
    if (state.bound) return true;
    ready_monotonic_us = transfer_monotonic_us();
    g_receiver_auto_prewarm_tokens.fetch_add(total_tokens);
    g_receiver_auto_prewarm_ready_tokens.fetch_add(total_tokens);
    g_receiver_auto_prewarm_last_status.store(
        static_cast<uint64_t>(Preserve_trx_promotion_adopt_status::OK));
    g_receiver_prewarm_backlog_at_phase2_end.store(0);
    g_receiver_ready_monotonic_us.store(ready_monotonic_us);
    state.ready_monotonic_us = ready_monotonic_us;
    state.bound = true;
    state.selection_published = true;
    temp_final = state.temp_final.finish(
        ready_monotonic_us, Preserve_trx_temp_final_outcome::READY);
    log_readiness_timing = take_receiver_epoch_readiness_timing_locked(
        &state, &readiness_final_ack_us, &readiness_ready_us);
  }
  binding_guard.commit();
  preserve_trx_temp_final_log(epoch_id, temp_final);
  refresh_receiver_ready_after_final_metadata();
  refresh_receiver_ready_after_final_spool_ack();
  refresh_receiver_ready_after_final_metadata_accepted();
  refresh_receiver_ready_after_terminal_commit_admitted();
  if (log_readiness_timing) {
    log_receiver_epoch_readiness_timing(epoch_id, readiness_final_ack_us,
                                        readiness_ready_us);
  }
  return true;
}

bool publish_receiver_epoch_ready_from_fact_if_possible(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_receiver_registry *registry) {
  try {
    return publish_receiver_epoch_ready_from_fact_if_possible_impl(
        root_dir, epoch_id, registry);
  } catch (...) {
    return false;
  }
}

bool publish_receiver_epoch_selection_if_possible(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_receiver_registry *registry, uint64_t now_us,
    bool deadline_reached) {
  if (registry == nullptr) return false;

  Preserve_trx_transfer_accepted_epoch accepted;
  if (registry->query_accepted_epoch(root_dir, epoch_id, &accepted) !=
          Preserve_trx_transfer_status::COMMITTED_NOT_READY ||
      accepted.fact == nullptr || accepted.selection_published ||
      accepted.lifecycle !=
          Preserve_trx_transfer_epoch_lifecycle::PREWARMING ||
      accepted.receiver_process_generation != receiver_boot_incarnation() ||
      accepted.fact_digest != accepted.fact->fact_digest) {
    return false;
  }
  deadline_reached = deadline_reached ||
                     accepted.deadline_monotonic_us <= now_us;

  std::vector<uint64_t> ready_tokens;
  std::vector<Preserve_trx_receiver_failed_token> failed_tokens;
  std::vector<Preserve_trx_prepared_token_key> ready_keys;
  std::vector<std::pair<Preserve_trx_prepared_token_key, uint64_t>> failed_result_keys;
  uint64_t classification_generation = 0;
  {
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    const auto found =
        g_receiver_ready_epoch_state.find({root_dir, epoch_id});
    if (found == g_receiver_ready_epoch_state.end() ||
        !found->second.fact_loaded || found->second.fact == nullptr ||
        found->second.fact_tokens.size() != accepted.tokens.size() ||
        !std::equal(found->second.fact_tokens.begin(),
                    found->second.fact_tokens.end(), accepted.tokens.begin()) ||
        found->second.global_failure || found->second.binding ||
        found->second.selection_published) {
      return false;
    }

    try {
      ready_tokens.reserve(accepted.tokens.size());
      failed_tokens.reserve(accepted.tokens.size());
      for (uint64_t token : accepted.tokens) {
        const auto result = found->second.token_results.find(token);
        if (result == found->second.token_results.end()) {
          if (!deadline_reached) return false;
          failed_tokens.push_back(
              {token, Preserve_trx_receiver_failure_reason::PREWARM_DEADLINE});
          continue;
        }
        if (result->second == Preserve_trx_receiver_failure_reason::NONE) {
          ready_tokens.push_back(token);
        } else {
          failed_tokens.push_back({token, result->second});
        }
      }
    } catch (const std::bad_alloc &) {
      return false;
    }
    if (failed_tokens.empty()) return false;
    found->second.binding = true;
    classification_generation = ++found->second.binding_generation;
  }

  bool published = false;
  Preserve_trx_temp_final_sample temp_final;
  Receiver_temp_activity_guard temp_activity(root_dir, epoch_id, true);
  auto finish_classification = create_scope_guard([&] {
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    const auto found =
        g_receiver_ready_epoch_state.find({root_dir, epoch_id});
    if (found == g_receiver_ready_epoch_state.end() ||
        found->second.binding_generation != classification_generation) {
      return;
    }
    found->second.binding = false;
    if (published) {
      found->second.selection_published = true;
      temp_final = found->second.temp_final.finish(
          transfer_monotonic_us(), Preserve_trx_temp_final_outcome::PARTIAL);
    }
  });

  try {
    ready_keys.reserve(ready_tokens.size());
    failed_result_keys.reserve(failed_tokens.size());
  } catch (const std::bad_alloc &) {
    return false;
  }
  for (const auto &fact_token : accepted.fact->tokens) {
    const bool ready = std::binary_search(ready_tokens.begin(),
                                          ready_tokens.end(), fact_token.token);
    Preserve_trx_transfer_manifest manifest;
    manifest.epoch_id = accepted.epoch_id;
    manifest.token = fact_token.token;
    manifest.source_freeze_lsn = fact_token.source_freeze_lsn;
    manifest.source_epoch_commit_lsn = fact_token.source_epoch_commit_lsn;
    manifest.objects = fact_token.objects;
    if (ready) {
      if (!receiver_strict_token_ready(root_dir, manifest)) return false;
      try {
        Preserve_trx_prepared_token_key key;
        if (!strict_prepared_key_for_receiver(
                root_dir, manifest, transfer_token_component(fact_token.token),
                &key)) {
          return false;
        }
        ready_keys.push_back(std::move(key));
      } catch (const std::bad_alloc &) {
        return false;
      }
      continue;
    }
    const bool has_results = std::any_of(
        manifest.objects.begin(), manifest.objects.end(),
        [](const Preserve_trx_transfer_object_descriptor &object) {
          return object.kind == Preserve_trx_transfer_object_kind::CURSOR_RESULT;
        });
    if (deadline_reached && !has_results) continue;
    Preserve_trx_prepared_token_key key;
    if (!strict_prepared_key_for_receiver(
            root_dir, manifest, transfer_token_component(fact_token.token),
            &key)) {
      return false;
    }
    if (has_results) {
      failed_result_keys.emplace_back(std::move(key), manifest.token);
      continue;
    }
    const auto purge_status =
        preserved_trx_strict_prepared_token_registry().purge_token(key);
    if (purge_status != Preserve_trx_prepared_status::OK &&
        purge_status != Preserve_trx_prepared_status::NOT_FOUND) {
      return false;
    }
  }

  const uint64_t ready_deadline_us = receiver_epoch_deadline_after_ms(
      now_us, preserve_trx_token_retention_timeout_ms);
  /* Deadline classification must retain the selected subset, not late work. */
  if (!ready_tokens.empty() &&
      preserved_trx_strict_prepared_token_registry()
              .update_selected_prepare_deadline(ready_keys, ready_deadline_us) !=
          Preserve_trx_prepared_status::OK) {
    return false;
  }
  if (registry->publish_accepted_epoch_selection(
          root_dir, epoch_id, now_us, ready_deadline_us, ready_tokens,
          failed_tokens) != Preserve_trx_transfer_status::OK) {
    return false;
  }
  published = true;
  finish_classification.rollback();
  preserve_trx_temp_final_log(epoch_id, temp_final);

  // Selection is already visible. A worker that publishes after this purge
  // repeats the failed-token check, covering either side of the race.
  for (const auto &failed : failed_result_keys) {
    (void)preserved_trx_strict_prepared_token_registry().purge_token(failed.first);
    try {
      preserved_trx_promotion_ready_cache_purge_token(root_dir, epoch_id,
                                                     failed.second);
    } catch (const std::bad_alloc &) {
      // This derived cache also expires with the epoch. Selection is final;
      // failure to allocate its lookup key must not undo the published result.
    }
  }

  for (const auto &failed : failed_tokens) {
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: receiver token excluded from ready selection token=" +
            std::to_string(failed.token) + " reason=" +
            std::to_string(static_cast<unsigned>(failed.reason)) +
            " deadline_us=" + std::to_string(accepted.deadline_monotonic_us) +
            " classified_us=" + std::to_string(now_us)).c_str());
  }
  g_receiver_auto_prewarm_tokens.fetch_add(accepted.tokens.size());
  g_receiver_auto_prewarm_ready_tokens.fetch_add(ready_tokens.size());
  g_receiver_auto_prewarm_not_ready_tokens.fetch_add(failed_tokens.size());
  g_receiver_prewarm_backlog_at_phase2_end.store(0);
  purge_receiver_epoch_prewarm_queues(root_dir, epoch_id);
  return true;
}

bool receiver_epoch_ready_is_bound(const std::string &root_dir,
                                   const std::string &epoch_id) {
  std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
  const auto found = g_receiver_ready_epoch_state.find({root_dir, epoch_id});
  return found != g_receiver_ready_epoch_state.end() && found->second.bound;
}

void retry_unbound_receiver_epochs_once(
    Preserve_trx_transfer_receiver_registry *registry) {
  std::vector<std::pair<std::string, std::string>> epochs;
  {
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    for (const auto &item : g_receiver_ready_epoch_state) {
      const Receiver_epoch_ready_state &state = item.second;
      if (!state.bound && !state.binding && state.fact_loaded &&
          state.fact != nullptr &&
          state.ready_fact_token_count == state.fact_tokens.size()) {
        epochs.push_back(item.first);
      }
    }
  }
  for (const auto &epoch : epochs) {
    (void)publish_receiver_epoch_ready_from_fact_if_possible(epoch.first,
                                                             epoch.second,
                                                             registry);
  }
}

std::vector<std::pair<std::string, std::string>> bound_receiver_epochs() {
  std::vector<std::pair<std::string, std::string>> epochs;
  std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
  for (const auto &item : g_receiver_ready_epoch_state) {
    if (item.second.bound) epochs.push_back(item.first);
  }
  return epochs;
}

bool transfer_manifest_has_snapshot_bundle(
    const Preserve_trx_transfer_manifest &manifest) {
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (object.kind == Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE) {
      return true;
    }
  }
  return false;
}

bool transfer_manifest_uses_strict_metadata_only_prewarm(
    const Preserve_trx_transfer_manifest &manifest) {
  const auto recovery = manifest.strict_eligibility_flags &
      (PRESERVE_TRX_TRANSFER_STRICT_ACTIVE_UNDO |
       PRESERVE_TRX_TRANSFER_STRICT_NO_REDO_CONTEXT |
       PRESERVE_TRX_TRANSFER_STRICT_RESOURCE_ONLY);
  return manifest.protocol_version == kPreserveTrxTransferProtocolVersion &&
         (manifest.strict_eligibility_flags &
          PRESERVE_TRX_TRANSFER_STRICT_PARTICIPANTS_AUTHENTICATED) != 0 &&
         (recovery == PRESERVE_TRX_TRANSFER_STRICT_ACTIVE_UNDO ||
          recovery == PRESERVE_TRX_TRANSFER_STRICT_NO_REDO_CONTEXT ||
          recovery == PRESERVE_TRX_TRANSFER_STRICT_RESOURCE_ONLY);
}

bool transfer_object_uses_strict_v1_memory_staging(
    const Preserve_trx_transfer_manifest &manifest,
    const Preserve_trx_transfer_object_descriptor &object) {
  return transfer_manifest_uses_strict_metadata_only_prewarm(manifest) &&
         (object.kind ==
              Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE ||
          object.kind ==
              Preserve_trx_transfer_object_kind::RESURRECTION_INDEX);
}

bool transfer_manifest_uses_v1_metadata_only_lock_prewarm(
    const Preserve_trx_transfer_manifest &manifest) {
  return manifest.protocol_version == kPreserveTrxTransferProtocolVersion;
}

bool lookup_receiver_object_prewarm_proof(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id, Receiver_object_prewarm_proof *proof) {
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, object_id);
  if (object == nullptr || proof == nullptr) return false;
  Receiver_object_prewarm_key key;
  key.root_dir = root_dir;
  key.epoch_id = manifest.epoch_id;
  key.token = manifest.token;
  key.object_id = object_id;
  key.digest = object->digest;
  key.source_live_generation = object->lock_plan.source_live_generation;
  key.source_live_digest = object->lock_plan.source_live_digest;

  std::lock_guard<std::mutex> guard(g_receiver_object_prewarm_proof_mutex);
  const auto found = g_receiver_object_prewarm_proofs.find(key);
  if (found == g_receiver_object_prewarm_proofs.end()) return false;
  *proof = found->second;
  return true;
}

Preserve_trx_promotion_adopt_status
prewarm_receiver_ready_cache_from_object_proof(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const Preserved_trx_bundle &bundle, bool *stale_record_lock_proof,
    const Preserve_trx_temp_receiver_work *temp_ready) {
  if (stale_record_lock_proof != nullptr) *stale_record_lock_proof = false;
  Receiver_object_prewarm_proof proof;
  if (!lookup_receiver_object_prewarm_proof(
          root_dir, manifest, kPreservedTrxBlobRecordLocks, &proof)) {
    return Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY;
  }
  if (!proof.record_lock_object) {
    return Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
  }
  if (stale_record_lock_proof != nullptr && !proof.metadata_only &&
      (proof.cold_gets != 0 || proof.resident_pages < proof.page_count)) {
    *stale_record_lock_proof = true;
  }
  return preserved_trx_promotion_prewarm_staged_bundle_with_record_lock_proof_for_receiver(
      root_dir, manifest.epoch_id, manifest.token,
      manifest.source_epoch_commit_lsn, bundle, proof.page_count,
      proof.resident_pages, proof.cold_gets, proof.bitmap_pages,
      proof.bitmap_bits, proof.metadata_only, temp_ready);
}

void note_receiver_epoch_pending_without_cold_fallback(
    const std::vector<Preserve_trx_transfer_manifest> &manifests,
    Preserve_trx_promotion_adopt_status status) {
  if (manifests.empty()) return;
  const uint64_t total_tokens = manifests.size();
  g_receiver_prewarm_backlog_at_phase2_end.store(total_tokens);
  g_receiver_auto_prewarm_last_status.store(static_cast<uint64_t>(status));
}

void signal_transfer_dispatch_error(THD *thd,
                                    Preserve_trx_transfer_status status) {
  switch (status) {
    case Preserve_trx_transfer_status::CORRUPT:
      my_error(ER_PRESERVE_TRX_CORRUPT_SNAPSHOT, MYF(0));
      return;
    case Preserve_trx_transfer_status::INVALID_ARGUMENT:
    case Preserve_trx_transfer_status::IO_ERROR:
    case Preserve_trx_transfer_status::UNSUPPORTED:
    case Preserve_trx_transfer_status::RESOURCE_EXHAUSTED:
    case Preserve_trx_transfer_status::ACK_UNCERTAIN:
    case Preserve_trx_transfer_status::LOCK_PLAN_STALE:
    case Preserve_trx_transfer_status::COMMITTED_CORRUPT:
      my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
      return;
    case Preserve_trx_transfer_status::COMMITTED_READY:
    case Preserve_trx_transfer_status::COMMITTED_NOT_READY:
    case Preserve_trx_transfer_status::NOT_COMMITTED:
    case Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN:
    case Preserve_trx_transfer_status::OK:
      my_ok(thd);
      return;
  }
  my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
}

Preserve_trx_transfer_status send_encoded_transfer_frame(
    Preserve_trx_transfer_encoded_frame_sink *sink,
    const Preserve_trx_transfer_frame &frame) {
  if (sink == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  std::string encoded_frame;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_encode_frame(frame, &encoded_frame);
  if (status != Preserve_trx_transfer_status::OK) return status;
  return sink->send_encoded_frame(encoded_frame);
}

Preserve_memory_lease acquire_transfer_decode_memory_lease(
    const std::string &encoded, uint64_t bytes) {
  const auto digest = sha256_digest(encoded);
  return preserve_trx_acquire_memory_lease(
      "transfer-decode-" + bytes_to_lower_hex(digest.data(), digest.size()),
      Preserve_trx_memory_kind::SNAPSHOT_CODEC_BUFFER,
      std::max<uint64_t>(1, bytes));
}

}  // namespace

Preserve_trx_transfer_status
preserve_trx_transfer_encode_session_only_tokens(
    const std::vector<uint64_t> &tokens, std::string *encoded) {
  if (encoded == nullptr || tokens.size() > kMaxSessionOnlyTokenCount) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  uint64_t previous_token = 0;
  for (uint64_t token : tokens) {
    if (token == 0 || token <= previous_token) {
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }
    previous_token = token;
  }

  try {
    std::string payload(kSessionOnlyTokenSetMagic,
                        sizeof(kSessionOnlyTokenSetMagic) - 1);
    payload.reserve(payload.size() + sizeof(uint32_t) +
                    tokens.size() * sizeof(uint64_t));
    append_u32(&payload, static_cast<uint32_t>(tokens.size()));
    for (uint64_t token : tokens) append_u64(&payload, token);
    encoded->swap(payload);
  } catch (...) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
preserve_trx_transfer_decode_session_only_tokens(
    const std::string &encoded, std::vector<uint64_t> *tokens) {
  if (tokens == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  Manifest_reader reader(encoded);
  const char *magic = nullptr;
  uint32_t count = 0;
  if (reader.read_fixed(sizeof(kSessionOnlyTokenSetMagic) - 1, &magic) ||
      std::memcmp(magic, kSessionOnlyTokenSetMagic,
                  sizeof(kSessionOnlyTokenSetMagic) - 1) != 0 ||
      reader.read_u32(&count) || count > kMaxSessionOnlyTokenCount ||
      reader.remaining() != static_cast<size_t>(count) * sizeof(uint64_t)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  std::vector<uint64_t> decoded;
  try {
    decoded.reserve(count);
    uint64_t previous_token = 0;
    for (uint32_t index = 0; index < count; ++index) {
      uint64_t token = 0;
      if (reader.read_u64(&token) || token == 0 || token <= previous_token) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      decoded.push_back(token);
      previous_token = token;
    }
  } catch (...) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  if (!reader.eof()) return Preserve_trx_transfer_status::CORRUPT;
  tokens->swap(decoded);
  return Preserve_trx_transfer_status::OK;
}

static Preserve_trx_transfer_status build_session_only_token_set(
    const std::vector<uint64_t> &tokens,
    std::unordered_set<uint64_t> *token_set) {
  if (token_set == nullptr || tokens.size() > kMaxSessionOnlyTokenCount) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::unordered_set<uint64_t> built;
  try {
    built.reserve(tokens.size());
    uint64_t previous_token = 0;
    for (uint64_t token : tokens) {
      if (token == 0 || token <= previous_token ||
          !built.insert(token).second) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      previous_token = token;
    }
  } catch (...) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  token_set->swap(built);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_resume_handoff
preserve_trx_transfer_begin_resume_handoff_for_token(
    const std::string &token, bool transaction_candidate) {
  if (token.empty() || token.front() == '0' ||
      std::any_of(token.begin(), token.end(),
                  [](unsigned char value) { return !std::isdigit(value); })) {
    return default_receiver_registry().begin_resume_handoff(0, false);
  }
  uint64_t parsed = 0;
  if (!parse_uint64_strict(token, &parsed) || parsed == 0 ||
      std::to_string(parsed) != token) {
    return default_receiver_registry().begin_resume_handoff(0, false);
  }
  return default_receiver_registry().begin_resume_handoff(
      parsed, transaction_candidate);
}

bool preserve_trx_transfer_consume_session_only_token(
    const Preserve_trx_transfer_resume_handoff &handoff) {
  if (!handoff.available || !handoff.session_only || handoff.token == 0) {
    return false;
  }
  return default_receiver_registry().consume_session_only_token(
      handoff.token, handoff.epoch_id, handoff.generation);
}

void preserve_trx_transfer_release_transaction_handoff(
    const Preserve_trx_transfer_resume_handoff &handoff) {
  if (!handoff.transaction_claimed || handoff.token == 0) return;
  default_receiver_registry().release_transaction_handoff(handoff.token,
                                                          handoff.generation);
}

static std::array<unsigned char, kPreservedTrxSha256Length>
session_only_control_fact_digest(
    const Preserve_trx_transfer_frame &commit) {
  static constexpr char kDomain[] = "PTRX_SSET_CONTROL_COMMIT_V1";
  try {
    std::string material;
    material.reserve(sizeof(kDomain) - 1 + commit.epoch_id.size() +
                     commit.manifest_payload.size() + sizeof(uint64_t) * 6);
    material.append(kDomain, sizeof(kDomain) - 1);
    append_u64(&material, commit.epoch_id.size());
    material.append(commit.epoch_id);
    append_u64(&material, commit.chunk_offset);
    append_u64(&material, commit.trx_id_store.source_trx_id_store);
    append_u64(&material, commit.trx_id_store.source_trx_id_store_lsn);
    append_u64(&material,
               commit.trx_id_store.source_safe_next_trx_id_floor);
    append_u64(&material, commit.manifest_payload.size());
    material.append(commit.manifest_payload);
    return sha256_digest(material);
  } catch (...) {
    return {};
  }
}

static std::array<unsigned char, kPreservedTrxSha256Length>
session_only_transaction_fact_digest(
    const std::array<unsigned char, kPreservedTrxSha256Length>
        &transaction_fact_digest,
    const Preserve_trx_transfer_frame &commit) {
  static constexpr char kDomain[] = "PTRX_SSET_TRANSACTION_COMMIT_V1";
  if (transfer_digest_is_zero(transaction_fact_digest)) return {};

  try {
    std::string material;
    material.reserve(sizeof(kDomain) - 1 + commit.epoch_id.size() +
                     transaction_fact_digest.size() +
                     commit.manifest_payload.size() + sizeof(uint64_t) * 6);
    material.append(kDomain, sizeof(kDomain) - 1);
    append_u64(&material, commit.epoch_id.size());
    material.append(commit.epoch_id);
    material.append(
        reinterpret_cast<const char *>(transaction_fact_digest.data()),
        transaction_fact_digest.size());
    append_u64(&material, commit.chunk_offset);
    append_u64(&material, commit.trx_id_store.source_trx_id_store);
    append_u64(&material, commit.trx_id_store.source_trx_id_store_lsn);
    append_u64(&material,
               commit.trx_id_store.source_safe_next_trx_id_floor);
    append_u64(&material, commit.manifest_payload.size());
    material.append(commit.manifest_payload);
    return sha256_digest(material);
  } catch (...) {
    return {};
  }
}

Preserve_trx_transfer_status
preserve_trx_transfer_copy_accepted_resurrection_entry(
    const std::string &root_dir,
    const Preserve_trx_transfer_accepted_epoch &accepted, uint64_t token,
    Preserve_trx_transfer_receiver_registry *registry,
    Preserve_trx_resurrection_index_entry *entry) {
  if (root_dir.empty() || accepted.fact == nullptr || token == 0 ||
      entry == nullptr || accepted.root_dir != root_dir ||
      accepted.epoch_id.empty() ||
      accepted.fact->epoch_id != accepted.epoch_id ||
      accepted.fact->source_fence_lsn != accepted.source_fence_lsn) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (registry == nullptr) registry = &default_receiver_registry();

  const auto fact_token = std::find_if(
      accepted.fact->tokens.begin(), accepted.fact->tokens.end(),
      [token](const auto &candidate) { return candidate.token == token; });
  if (fact_token == accepted.fact->tokens.end()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  Preserve_trx_transfer_receiver_record record;
  if (!registry->lookup(accepted.epoch_id, token, &record)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  const Preserve_trx_transfer_manifest manifest = receiver_record_manifest(record);
  std::string encoded_manifest;
  if (manifest.epoch_id != accepted.epoch_id || manifest.token != token ||
      manifest.source_freeze_lsn != fact_token->source_freeze_lsn ||
      manifest.source_epoch_commit_lsn !=
          fact_token->source_epoch_commit_lsn ||
      preserve_trx_transfer_encode_manifest(manifest, &encoded_manifest) !=
          Preserve_trx_transfer_status::OK ||
      sha256_digest(encoded_manifest) != fact_token->manifest_digest ||
      !decode_receiver_resurrection_index(root_dir, manifest, registry, entry)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
preserve_trx_transfer_cleanup_startup_root() {
  const std::string root_dir =
      strip_trailing_directory_separators(preserved_trx_dir_value());
  if (root_dir.empty()) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  const Preserve_trx_transfer_status cleanup_status =
      remove_receiver_restart_tree(root_dir, 0, true);
  const bool create_failed = ensure_dir_exists(root_dir);
  if (cleanup_status != Preserve_trx_transfer_status::OK) return cleanup_status;
  return create_failed ? Preserve_trx_transfer_status::IO_ERROR
                       : Preserve_trx_transfer_status::OK;
}

std::string preserve_trx_transfer_receiver_boot_incarnation() {
  return receiver_boot_incarnation();
}

Preserve_trx_transfer_status
preserve_trx_transfer_cleanup_receiver_restart_state(
    const std::string &root_dir) {
  if (root_dir.empty()) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  const std::string transfer_root = join_path(root_dir, ".transfer");
  std::set<std::string> transfer_tokens;
  Preserve_trx_transfer_status status =
      collect_receiver_restart_transfer_tokens(transfer_root,
                                               &transfer_tokens);
  if (status != Preserve_trx_transfer_status::OK) return status;

  status = remove_receiver_restart_promotion_markers(root_dir);
  if (status != Preserve_trx_transfer_status::OK) return status;

  Preserve_snapshot_write_options write_options;
  write_options.defer_file_fsync = true;
  write_options.defer_directory_fsync = true;
  Local_file_preserved_trx_carrier carrier(root_dir, write_options);
  Preserved_trx_carrier_listing listing;
  const Preserved_trx_carrier_status list_status =
      carrier.list_tokens(&listing);
  if (list_status != Preserved_trx_carrier_status::OK) {
    return list_status == Preserved_trx_carrier_status::CORRUPT
               ? Preserve_trx_transfer_status::CORRUPT
               : Preserve_trx_transfer_status::IO_ERROR;
  }
  transfer_tokens.insert(listing.standby_pending_tokens.begin(),
                         listing.standby_pending_tokens.end());
  for (const std::string &token : transfer_tokens) {
    if (carrier.remove_with_status(token) !=
        Preserve_snapshot_delete_status::OK) {
      return Preserve_trx_transfer_status::IO_ERROR;
    }
  }

  return remove_receiver_restart_tree(transfer_root, 0);
}

uint64_t preserve_trx_transfer_receiver_inflight_tokens_status() {
  return default_receiver_registry().status_counts().inflight_tokens;
}

uint64_t preserve_trx_transfer_receiver_inflight_bytes_status() {
  return default_receiver_registry().status_counts().inflight_bytes;
}

uint64_t preserve_trx_transfer_receiver_saved_online_tokens_status() {
  return default_receiver_registry().status_counts().saved_online_tokens;
}

uint64_t preserve_trx_transfer_receiver_failed_tokens_status() {
  return default_receiver_registry().status_counts().failed_tokens;
}

uint64_t preserve_trx_transfer_receiver_last_failed_token_status() {
  return default_receiver_registry().status_counts().last_failed_token;
}

std::string preserve_trx_transfer_receiver_last_failed_reason_status() {
  return default_receiver_registry().status_counts().last_failed_reason;
}

uint64_t preserve_trx_transfer_receiver_active_epochs_status() {
  return default_receiver_registry().active_epoch_count();
}

uint64_t preserve_trx_transfer_receiver_expired_epochs_status() {
  return default_receiver_registry().expired_epoch_count();
}

std::string preserve_trx_transfer_make_epoch_id(
    const std::string &monotonic_epoch_id) {
  return make_transfer_epoch_id(monotonic_epoch_id);
}

std::string preserve_trx_transfer_source_epoch_boot_nonce_for_unit_test() {
  return transfer_source_epoch_boot_nonce();
}

std::string preserve_trx_transfer_make_epoch_id_for_unit_test(
    const std::string &monotonic_epoch_id) {
  return make_transfer_epoch_id(monotonic_epoch_id);
}

void preserve_trx_transfer_set_staging_cleanup_failures_for_unit_test(
    uint failures) {
  g_transfer_staging_cleanup_failures_for_unit_test.store(failures);
}

bool preserve_trx_transfer_credential_file_metadata_is_secure_for_unit_test(
    uint64_t mode, uint64_t owner_uid, uint64_t effective_uid) {
  return transfer_credential_file_metadata_is_secure(mode, owner_uid,
                                                     effective_uid);
}

bool preserve_trx_transfer_read_credential_secret_file_for_unit_test(
    const char *path, std::string *secret) {
  return read_transfer_credential_secret_file(path, secret);
}

#ifndef NDEBUG
bool preserve_trx_transfer_strict_prepared_key_for_unit_test(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &semantic_token,
    Preserve_trx_prepared_token_key *key) {
  return strict_prepared_key_for_receiver(root_dir, manifest, semantic_token,
                                          key);
}

bool preserve_trx_transfer_put_receiver_record_lock_plan_for_unit_test(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    std::unique_ptr<lock_preserve_metadata_plan_t> plan,
    const lock_preserve_record_lock_metadata_facts_t &facts) {
  return put_receiver_record_lock_prepared(root_dir, manifest, std::move(plan),
                                           facts);
}
#endif

Preserve_trx_transfer_status preserve_trx_transfer_encode_manifest(
    const Preserve_trx_transfer_manifest &manifest, std::string *encoded) {
  if (encoded == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  std::string out;
  out.append(kTransferManifestMagic, kTransferManifestMagicLength);
  append_u16(&out, manifest.protocol_version);
  append_u64(&out, manifest.frame_sequence);
  append_u64(&out, manifest.source_freeze_lsn);
  append_u64(&out, manifest.source_epoch_commit_lsn);
  append_u32(&out, manifest.strict_eligibility_flags);
  if (append_string(&out, manifest.epoch_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  append_u64(&out, manifest.token);
  append_u32(&out, static_cast<uint32_t>(manifest.objects.size()));
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (object.object_id.empty() || append_string(&out, object.object_id)) {
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }
    append_u16(&out, static_cast<uint16_t>(object.kind));
    append_u32(&out, object.flags);
    append_u64(&out, object.total_size);
    out.append(reinterpret_cast<const char *>(object.digest.data()),
               object.digest.size());
    append_u16(&out, object.lock_plan.version);
    append_u64(&out, object.lock_plan.source_live_generation);
    out.append(reinterpret_cast<const char *>(
                   object.lock_plan.source_live_digest.data()),
               object.lock_plan.source_live_digest.size());
    out.append(reinterpret_cast<const char *>(
                   object.lock_plan.record_store_fingerprint.data()),
               object.lock_plan.record_store_fingerprint.size());
    append_u16(&out, object.lock_plan.simulated_terminal_proof ? 1 : 0);
    out.append(reinterpret_cast<const char *>(
                   object.lock_plan.terminal_proof.data()),
               object.lock_plan.terminal_proof.size());
  }

  *encoded = std::move(out);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_decode_manifest(
    const std::string &encoded, Preserve_trx_transfer_manifest *manifest) {
  if (manifest == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  Manifest_reader reader(encoded);
  const char *magic = nullptr;
  if (reader.read_fixed(kTransferManifestMagicLength, &magic) ||
      std::memcmp(magic, kTransferManifestMagic,
                  kTransferManifestMagicLength) != 0) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  Preserve_trx_transfer_manifest parsed;
  if (reader.read_u16(&parsed.protocol_version) ||
      reader.read_u64(&parsed.frame_sequence)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (!transfer_protocol_version_is_decodable(parsed.protocol_version)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (reader.read_u64(&parsed.source_freeze_lsn) ||
      reader.read_u64(&parsed.source_epoch_commit_lsn)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (reader.read_u32(&parsed.strict_eligibility_flags)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (reader.read_string(&parsed.epoch_id) || reader.read_u64(&parsed.token)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  uint32_t object_count = 0;
  if (reader.read_u32(&object_count) ||
      object_count > kMaxTransferManifestObjects) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  constexpr size_t kMinEncodedObjectBytes =
      sizeof(uint32_t) + 1 + sizeof(uint16_t) + sizeof(uint32_t) +
      sizeof(uint64_t) + kPreservedTrxSha256Length;
  if (object_count > reader.remaining() / kMinEncodedObjectBytes) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  const uint64_t decode_bytes =
      static_cast<uint64_t>(object_count) *
      sizeof(Preserve_trx_transfer_object_descriptor);
  Preserve_memory_lease decode_lease =
      acquire_transfer_decode_memory_lease(encoded, decode_bytes);
  if (!decode_lease.acquired()) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  try {
    parsed.objects.reserve(object_count);
    for (uint32_t i = 0; i < object_count; ++i) {
      Preserve_trx_transfer_object_descriptor object;
      uint16_t raw_kind = 0;
      const char *digest = nullptr;
      if (reader.read_string(&object.object_id) || reader.read_u16(&raw_kind) ||
          !object_kind_supported(raw_kind, &object.kind) ||
          reader.read_u32(&object.flags) ||
          reader.read_u64(&object.total_size) ||
          reader.read_fixed(object.digest.size(), &digest)) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      std::memcpy(object.digest.data(), digest, object.digest.size());
      const char *source_live_digest = nullptr;
      const char *record_store_fingerprint = nullptr;
      const char *terminal_proof = nullptr;
      uint16_t terminal_proof_present = 0;
      if (reader.read_u16(&object.lock_plan.version) ||
          reader.read_u64(&object.lock_plan.source_live_generation) ||
          reader.read_fixed(object.lock_plan.source_live_digest.size(),
                            &source_live_digest) ||
          reader.read_fixed(object.lock_plan.record_store_fingerprint.size(),
                            &record_store_fingerprint) ||
          reader.read_u16(&terminal_proof_present) ||
          terminal_proof_present > 1 ||
          reader.read_fixed(object.lock_plan.terminal_proof.size(),
                            &terminal_proof)) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      object.lock_plan.simulated_terminal_proof =
          terminal_proof_present != 0;
      std::memcpy(object.lock_plan.source_live_digest.data(), source_live_digest,
                  object.lock_plan.source_live_digest.size());
      std::memcpy(object.lock_plan.record_store_fingerprint.data(),
                  record_store_fingerprint,
                  object.lock_plan.record_store_fingerprint.size());
      std::memcpy(object.lock_plan.terminal_proof.data(), terminal_proof,
                  object.lock_plan.terminal_proof.size());
      parsed.objects.push_back(object);
    }
  } catch (const std::bad_alloc &) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }

  if (!reader.eof()) return Preserve_trx_transfer_status::CORRUPT;
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(parsed, true);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  *manifest = std::move(parsed);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_strict_eligibility_status
preserve_trx_transfer_validate_strict_eligibility(
    const Preserve_trx_transfer_manifest &manifest,
    const Preserve_snapshot_metadata &metadata, bool predicate_lock_present,
    bool wait_lock_present, size_t epoch_token_count) {
  if (manifest.protocol_version != kPreserveTrxTransferProtocolVersion) {
    return Preserve_trx_transfer_strict_eligibility_status::LEGACY_PROTOCOL;
  }
  const bool no_redo = metadata.recovery.needs_no_redo_context();
  const bool resource_only = metadata.recovery.resource_only();
  const uint32_t expected_recovery = resource_only
      ? PRESERVE_TRX_TRANSFER_STRICT_RESOURCE_ONLY : no_redo
      ? PRESERVE_TRX_TRANSFER_STRICT_NO_REDO_CONTEXT
      : PRESERVE_TRX_TRANSFER_STRICT_ACTIVE_UNDO;
  if ((manifest.strict_eligibility_flags &
       (PRESERVE_TRX_TRANSFER_STRICT_ACTIVE_UNDO |
        PRESERVE_TRX_TRANSFER_STRICT_NO_REDO_CONTEXT |
        PRESERVE_TRX_TRANSFER_STRICT_RESOURCE_ONLY)) != expected_recovery ||
      manifest.source_freeze_lsn == 0 ||
      manifest.source_epoch_commit_lsn == 0) {
    return Preserve_trx_transfer_strict_eligibility_status::
        TOKEN_NOT_PREPARED_REDO;
  }
  if ((manifest.strict_eligibility_flags &
       PRESERVE_TRX_TRANSFER_STRICT_PARTICIPANTS_AUTHENTICATED) == 0) {
    return Preserve_trx_transfer_strict_eligibility_status::
        PARTICIPANT_NOT_AUTHENTICATED;
  }
  const bool persistent_only = metadata.engine_shape ==
      Preserve_snapshot_engine_shape::PERSISTENT_ONLY && !metadata.has_temp_engine_state &&
      metadata.temp_table_manifest_payload.empty();
  const bool mixed = metadata.engine_shape == Preserve_snapshot_engine_shape::MIXED &&
      metadata.has_temp_engine_state && !metadata.temp_table_manifest_payload.empty() &&
      preserve_trx_temp_table_enable && preserve_trx_temp_id_namespace;
  const bool no_redo_valid = preserve_trx_temp_id_namespace &&
      preserve_trx_temp_table_enable && preserve_trx_recovery_payload_valid(metadata) &&
      (resource_only || metadata.recovery.freeze_lsn == manifest.source_freeze_lsn);
  if ((no_redo || resource_only ? !no_redo_valid
               : ((!persistent_only && !mixed) || !metadata.has_persistent_engine_state)) ||
      (!no_redo && !resource_only && metadata.recovery.basis != Preserve_trx_engine_recovery::LEGACY &&
       metadata.recovery.basis != Preserve_trx_engine_recovery::REDO_RESURRECTION) ||
      (!metadata.cursor_manifest_payload.empty() && !preserve_trx_result_capture_enable)) {
    return Preserve_trx_transfer_strict_eligibility_status::
        UNSUPPORTED_ENGINE_SHAPE;
  }
  if (!preserve_snapshot_gtid_state_is_strict_transfer_safe(metadata)) {
    return Preserve_trx_transfer_strict_eligibility_status::GTID_PRESENT;
  }
  if (predicate_lock_present || !metadata.predicate_locks_payload.empty()) {
    return Preserve_trx_transfer_strict_eligibility_status::
        PREDICATE_LOCK_PRESENT;
  }
  if (wait_lock_present) {
    return Preserve_trx_transfer_strict_eligibility_status::WAIT_LOCK_PRESENT;
  }
  if (epoch_token_count == 0) {
    return Preserve_trx_transfer_strict_eligibility_status::EMPTY_EPOCH;
  }
  const std::string manifest_token = std::to_string(manifest.token);
  if (metadata.token != manifest_token) {
    return Preserve_trx_transfer_strict_eligibility_status::
        TOKEN_IDENTITY_MISMATCH;
  }
  const Preserve_trx_transfer_object_descriptor *record_locks =
      find_object(manifest, kPreservedTrxBlobRecordLocks);
  if (record_locks != nullptr &&
      (record_locks->kind !=
           Preserve_trx_transfer_object_kind::EXTERNAL_BLOB ||
       record_locks->lock_plan.version !=
           kPreserveTrxTransferLockPlanContractVersion ||
       !transfer_lock_plan_contract_valid(*record_locks))) {
    return Preserve_trx_transfer_strict_eligibility_status::
        LOCK_PLAN_CONTRACT_MISSING;
  }
  const Preserve_trx_transfer_object_descriptor *resurrection_index =
      find_object(manifest, kPreserveTrxResurrectionIndexObjectId);
  if (no_redo || resource_only) return resurrection_index == nullptr
      ? Preserve_trx_transfer_strict_eligibility_status::OK
      : Preserve_trx_transfer_strict_eligibility_status::UNSUPPORTED_ENGINE_SHAPE;
  if (resurrection_index == nullptr ||
      resurrection_index->kind !=
          Preserve_trx_transfer_object_kind::RESURRECTION_INDEX ||
      resurrection_index->total_size == 0 ||
      std::all_of(resurrection_index->digest.begin(),
                  resurrection_index->digest.end(),
                  [](unsigned char value) { return value == 0; })) {
    return Preserve_trx_transfer_strict_eligibility_status::
        RESURRECTION_INDEX_MISSING;
  }
  return Preserve_trx_transfer_strict_eligibility_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_encode_epoch_fact(
    const Preserve_trx_transfer_epoch_fact &fact, std::string *encoded) {
  const bool empty_trx_id_store =
      transfer_trx_id_store_fact_is_empty(fact.trx_id_store);
  if (encoded == nullptr || !transfer_component_safe(fact.epoch_id) ||
      fact.source_fence_lsn == 0 || fact.tokens.empty() ||
      (!empty_trx_id_store &&
       !transfer_trx_id_store_fact_is_valid(fact.trx_id_store,
                                            fact.source_fence_lsn))) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::vector<Preserve_trx_transfer_epoch_fact_token> tokens = fact.tokens;
  std::sort(tokens.begin(), tokens.end(),
            [](const Preserve_trx_transfer_epoch_fact_token &left,
               const Preserve_trx_transfer_epoch_fact_token &right) {
              return left.token < right.token;
            });

  std::string body;
  body.append("PTRXFER_EPOCH_FACT_V1\n");
  body.append("epoch=").append(fact.epoch_id).append("\n");
  body.append("source_fence_lsn=")
      .append(std::to_string(fact.source_fence_lsn))
      .append("\n");
  body.append("source_trx_id_store=")
      .append(std::to_string(fact.trx_id_store.source_trx_id_store))
      .append("\n");
  body.append("source_trx_id_store_lsn=")
      .append(std::to_string(fact.trx_id_store.source_trx_id_store_lsn))
      .append("\n");
  body.append("source_safe_next_trx_id_floor=")
      .append(
          std::to_string(fact.trx_id_store.source_safe_next_trx_id_floor))
      .append("\n");
  body.append("token_count=").append(std::to_string(tokens.size())).append("\n");

  uint64_t previous_token = 0;
  for (const Preserve_trx_transfer_epoch_fact_token &token : tokens) {
    if (token.token == 0 || token.token <= previous_token ||
        token.source_freeze_lsn == 0 || token.source_epoch_commit_lsn == 0 ||
        token.source_freeze_lsn > fact.source_fence_lsn ||
        token.source_epoch_commit_lsn > fact.source_fence_lsn) {
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }
    previous_token = token.token;
    body.append("token=").append(std::to_string(token.token)).append("\n");
    body.append("source_freeze_lsn=")
        .append(std::to_string(token.source_freeze_lsn))
        .append("\n");
    body.append("source_epoch_commit_lsn=")
        .append(std::to_string(token.source_epoch_commit_lsn))
        .append("\n");
    body.append("manifest_digest=")
        .append(digest_hex(token.manifest_digest))
        .append("\n");
    body.append("object_count=")
        .append(std::to_string(token.objects.size()))
        .append("\n");
    std::set<std::string> object_ids;
    for (const Preserve_trx_transfer_object_descriptor &object :
         token.objects) {
      if (!transfer_component_safe(object.object_id) ||
          !object_ids.insert(object.object_id).second) {
        return Preserve_trx_transfer_status::INVALID_ARGUMENT;
      }
      body.append("object=")
          .append(object.object_id)
          .append("|")
          .append(std::to_string(static_cast<uint16_t>(object.kind)))
          .append("|")
          .append(std::to_string(object.flags))
          .append("|")
          .append(std::to_string(object.total_size))
          .append("|")
          .append(digest_hex(object.digest))
          .append("|")
          .append(std::to_string(object.lock_plan.version))
          .append("|")
          .append(std::to_string(object.lock_plan.source_live_generation))
          .append("|")
          .append(digest_hex(object.lock_plan.source_live_digest))
          .append("|")
          .append(digest_hex(object.lock_plan.record_store_fingerprint))
          .append("|")
          .append(object.lock_plan.simulated_terminal_proof ? "1" : "0")
          .append("|")
          .append(digest_hex(object.lock_plan.terminal_proof))
          .append("\n");
    }
  }
  const std::array<unsigned char, kPreservedTrxSha256Length> body_digest =
      sha256_digest(body);
  body.append("digest=").append(digest_hex(body_digest)).append("\n");
  *encoded = std::move(body);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_decode_epoch_fact(
    const std::string &encoded, Preserve_trx_transfer_epoch_fact *fact) {
  if (fact == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  const std::string digest_prefix = "digest=";
  const size_t digest_pos = encoded.rfind(digest_prefix);
  if (digest_pos == std::string::npos) return Preserve_trx_transfer_status::CORRUPT;
  const std::string body = encoded.substr(0, digest_pos);
  const std::string digest_line = encoded.substr(digest_pos);
  if (digest_line != "digest=" + digest_hex(sha256_digest(body)) + "\n") {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  std::istringstream input(body);
  std::string line;
  auto next_line = [&]() -> bool {
    if (!std::getline(input, line)) return false;
    return true;
  };

  Preserve_trx_transfer_epoch_fact parsed;
  if (!next_line() || line != "PTRXFER_EPOCH_FACT_V1")
    return Preserve_trx_transfer_status::CORRUPT;
  std::string value;
  if (!next_line() || !line_has_prefix(line, "epoch=", &parsed.epoch_id) ||
      !transfer_component_safe(parsed.epoch_id)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (!next_line() || !line_has_prefix(line, "source_fence_lsn=", &value) ||
      !parse_uint64_strict(value, &parsed.source_fence_lsn) ||
      parsed.source_fence_lsn == 0) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (!next_line() ||
      !line_has_prefix(line, "source_trx_id_store=", &value) ||
      !parse_uint64_strict(value,
                           &parsed.trx_id_store.source_trx_id_store) ||
      !next_line() ||
      !line_has_prefix(line, "source_trx_id_store_lsn=", &value) ||
      !parse_uint64_strict(value,
                           &parsed.trx_id_store.source_trx_id_store_lsn) ||
      !next_line() ||
      !line_has_prefix(line, "source_safe_next_trx_id_floor=", &value) ||
      !parse_uint64_strict(
          value, &parsed.trx_id_store.source_safe_next_trx_id_floor) ||
      (!transfer_trx_id_store_fact_is_empty(parsed.trx_id_store) &&
       !transfer_trx_id_store_fact_is_valid(parsed.trx_id_store,
                                            parsed.source_fence_lsn))) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  uint64_t token_count = 0;
  if (!next_line() || !line_has_prefix(line, "token_count=", &value) ||
      !parse_uint64_strict(value, &token_count) || token_count == 0 ||
      token_count > 1000000) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  uint64_t previous_token = 0;
  for (uint64_t i = 0; i < token_count; ++i) {
    Preserve_trx_transfer_epoch_fact_token token;
    if (!next_line() || !line_has_prefix(line, "token=", &value) ||
        !parse_uint64_strict(value, &token.token) || token.token == 0 ||
        token.token <= previous_token) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    previous_token = token.token;
    if (!next_line() ||
        !line_has_prefix(line, "source_freeze_lsn=", &value) ||
        !parse_uint64_strict(value, &token.source_freeze_lsn) ||
        token.source_freeze_lsn == 0) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    if (!next_line() ||
        !line_has_prefix(line, "source_epoch_commit_lsn=", &value) ||
        !parse_uint64_strict(value, &token.source_epoch_commit_lsn) ||
        token.source_epoch_commit_lsn == 0 ||
        token.source_freeze_lsn > parsed.source_fence_lsn ||
        token.source_epoch_commit_lsn > parsed.source_fence_lsn) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    if (!next_line() || !line_has_prefix(line, "manifest_digest=", &value) ||
        !parse_digest_hex(value, &token.manifest_digest)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    uint64_t object_count = 0;
    if (!next_line() || !line_has_prefix(line, "object_count=", &value) ||
        !parse_uint64_strict(value, &object_count) ||
        object_count > kMaxTransferManifestObjects) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    std::set<std::string> object_ids;
    for (uint64_t object_index = 0; object_index < object_count;
         ++object_index) {
      if (!next_line() || !line_has_prefix(line, "object=", &value)) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      const std::vector<std::string> fields = split_pipe_fields(value);
      if (fields.size() != 11U || !transfer_component_safe(fields[0]) ||
          !object_ids.insert(fields[0]).second) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      Preserve_trx_transfer_object_descriptor object;
      object.object_id = fields[0];
      uint64_t raw_kind = 0;
      uint64_t flags = 0;
      if (!parse_uint64_strict(fields[1], &raw_kind) ||
          raw_kind > std::numeric_limits<uint16_t>::max() ||
          !object_kind_supported(static_cast<uint16_t>(raw_kind),
                                 &object.kind) ||
          !parse_uint64_strict(fields[2], &flags) ||
          flags > std::numeric_limits<uint32_t>::max() ||
          !parse_uint64_strict(fields[3], &object.total_size) ||
          !parse_digest_hex(fields[4], &object.digest)) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      object.flags = static_cast<uint32_t>(flags);
      uint64_t contract_version = 0;
      uint64_t terminal_proof_present = 0;
      if (!parse_uint64_strict(fields[5], &contract_version) ||
          contract_version > std::numeric_limits<uint16_t>::max() ||
          !parse_uint64_strict(
              fields[6], &object.lock_plan.source_live_generation) ||
          !parse_digest_hex(fields[7],
                            &object.lock_plan.source_live_digest) ||
          !parse_digest_hex(fields[8],
                            &object.lock_plan.record_store_fingerprint) ||
          !parse_uint64_strict(fields[9], &terminal_proof_present) ||
          terminal_proof_present > 1 ||
          !parse_digest_hex(fields[10], &object.lock_plan.terminal_proof)) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      object.lock_plan.version = static_cast<uint16_t>(contract_version);
      object.lock_plan.simulated_terminal_proof =
          terminal_proof_present != 0;
      if (!transfer_lock_plan_contract_valid(object)) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      token.objects.push_back(object);
    }
    parsed.tokens.push_back(token);
  }
  if (next_line()) return Preserve_trx_transfer_status::CORRUPT;
  parsed.fact_digest = sha256_digest(body);
  *fact = std::move(parsed);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status build_epoch_fact_from_manifests(
    const std::vector<Preserve_trx_transfer_manifest> &manifests,
    uint64_t source_fence_lsn,
    const Preserve_trx_transfer_trx_id_store_fact &trx_id_store_fact,
    Preserve_trx_transfer_epoch_fact *fact) {
  if (fact == nullptr || manifests.empty())
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  Preserve_trx_transfer_epoch_fact built;
  built.epoch_id = manifests.front().epoch_id;
  built.source_fence_lsn = source_fence_lsn;
  built.trx_id_store = trx_id_store_fact;
  if (built.source_fence_lsn == 0) {
    for (const auto &manifest : manifests) {
      built.source_fence_lsn =
          std::max(built.source_fence_lsn, manifest.source_epoch_commit_lsn);
    }
  }
  std::set<uint64_t> tokens;
  for (size_t manifest_index = 0; manifest_index < manifests.size();
       ++manifest_index) {
    const Preserve_trx_transfer_manifest &manifest =
        manifests[manifest_index];
    const Preserve_trx_transfer_status validation_status =
        validate_manifest_components(manifest, false);
    if (validation_status != Preserve_trx_transfer_status::OK)
      return validation_status;
    if (manifest.epoch_id != built.epoch_id ||
        !tokens.insert(manifest.token).second) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    std::string encoded_manifest;
    const Preserve_trx_transfer_status encode_status =
        preserve_trx_transfer_encode_manifest(manifest, &encoded_manifest);
    if (encode_status != Preserve_trx_transfer_status::OK) return encode_status;
    Preserve_trx_transfer_epoch_fact_token token;
    token.token = manifest.token;
    token.source_freeze_lsn = manifest.source_freeze_lsn;
    token.source_epoch_commit_lsn = manifest.source_epoch_commit_lsn;
    token.manifest_digest = sha256_digest(encoded_manifest);
    token.objects = manifest.objects;
    built.tokens.push_back(std::move(token));
  }
  std::sort(built.tokens.begin(), built.tokens.end(),
            [](const Preserve_trx_transfer_epoch_fact_token &left,
               const Preserve_trx_transfer_epoch_fact_token &right) {
              return left.token < right.token;
            });
  std::string encoded_fact;
  const Preserve_trx_transfer_status encode_status =
      preserve_trx_transfer_encode_epoch_fact(built, &encoded_fact);
  if (encode_status != Preserve_trx_transfer_status::OK) return encode_status;
  built.fact_digest = sha256_digest(
      encoded_fact.substr(0, encoded_fact.rfind("digest=")));
  *fact = std::move(built);
  return Preserve_trx_transfer_status::OK;
}

bool transfer_object_descriptors_equal(
    const std::vector<Preserve_trx_transfer_object_descriptor> &left,
    const std::vector<Preserve_trx_transfer_object_descriptor> &right) {
  if (left.size() != right.size()) return false;
  for (size_t i = 0; i < left.size(); ++i) {
    if (left[i].object_id != right[i].object_id ||
        left[i].kind != right[i].kind || left[i].flags != right[i].flags ||
        left[i].total_size != right[i].total_size ||
        left[i].digest != right[i].digest ||
        !transfer_lock_plan_contract_equal(left[i].lock_plan,
                                           right[i].lock_plan)) {
      return false;
    }
  }
  return true;
}

bool transfer_object_descriptor_equal(
    const Preserve_trx_transfer_object_descriptor &left,
    const Preserve_trx_transfer_object_descriptor &right) {
  return left.object_id == right.object_id && left.kind == right.kind &&
         left.flags == right.flags && left.total_size == right.total_size &&
         left.digest == right.digest &&
         transfer_lock_plan_contract_equal(left.lock_plan, right.lock_plan);
}

static bool receiver_record_matches_manifest(
    const Preserve_trx_transfer_receiver_record &record,
    const Preserve_trx_transfer_manifest &manifest) {
  // Frame sequence belongs to transport replay, not the retained resource set.
  return record.epoch_id == manifest.epoch_id && record.token == manifest.token &&
         record.protocol_version == manifest.protocol_version &&
         record.strict_eligibility_flags == manifest.strict_eligibility_flags &&
         record.source_freeze_lsn == manifest.source_freeze_lsn &&
         record.source_epoch_commit_lsn == manifest.source_epoch_commit_lsn &&
         transfer_object_descriptors_equal(record.objects, manifest.objects);
}

bool epoch_fact_tokens_equal(
    const Preserve_trx_transfer_epoch_fact_token &left,
    const Preserve_trx_transfer_epoch_fact_token &right) {
  return left.token == right.token &&
         left.source_freeze_lsn == right.source_freeze_lsn &&
         left.source_epoch_commit_lsn == right.source_epoch_commit_lsn &&
         left.manifest_digest == right.manifest_digest &&
         transfer_object_descriptors_equal(left.objects, right.objects);
}

bool standby_token_artifact_published_in_listing(
    const Preserve_trx_transfer_manifest &manifest,
    const Preserved_trx_carrier_listing &listing) {
  const std::string token_component = transfer_token_component(manifest.token);
  return listing.snapshot_tokens.count(token_component) != 0 &&
         listing.standby_pending_tokens.count(token_component) != 0;
}

std::mutex &receiver_staging_finalize_mutex(const std::string &root_dir,
                                            const std::string &epoch_id,
                                            uint64_t token) {
  size_t hash = std::hash<std::string>{}(root_dir);
  hash ^= std::hash<std::string>{}(epoch_id) + 0x9e3779b9 + (hash << 6) +
          (hash >> 2);
  hash ^= std::hash<uint64_t>{}(token) + 0x9e3779b9 + (hash << 6) +
          (hash >> 2);
  return g_receiver_staging_finalize_mutexes[
      hash % g_receiver_staging_finalize_mutexes.size()];
}

Preserve_trx_transfer_status write_epoch_fact_file(
    const std::string &root_dir,
    const std::vector<Preserve_trx_transfer_manifest> &manifests,
    uint64_t source_fence_lsn,
    const Preserve_trx_transfer_trx_id_store_fact &trx_id_store_fact) {
  Preserve_trx_transfer_epoch_fact fact;
  Preserve_trx_transfer_status status =
      build_epoch_fact_from_manifests(manifests, source_fence_lsn,
                                      trx_id_store_fact, &fact);
  if (status != Preserve_trx_transfer_status::OK) return status;

  const std::string existing_fact_path =
      transfer_epoch_fact_path(root_dir, fact.epoch_id);
  if (file_exists(existing_fact_path)) {
    Preserve_trx_transfer_epoch_fact existing_fact;
    const Preserve_trx_transfer_status read_existing_status =
        preserve_trx_transfer_read_epoch_fact(root_dir, fact.epoch_id,
                                              &existing_fact);
    if (read_existing_status != Preserve_trx_transfer_status::OK) {
      return read_existing_status;
    }
    if (existing_fact.epoch_id != fact.epoch_id ||
        existing_fact.source_fence_lsn != fact.source_fence_lsn ||
        existing_fact.trx_id_store.source_trx_id_store !=
            fact.trx_id_store.source_trx_id_store ||
        existing_fact.trx_id_store.source_trx_id_store_lsn !=
            fact.trx_id_store.source_trx_id_store_lsn ||
        existing_fact.trx_id_store.source_safe_next_trx_id_floor !=
            fact.trx_id_store.source_safe_next_trx_id_floor) {
      return Preserve_trx_transfer_status::CORRUPT;
    }

    std::map<uint64_t, Preserve_trx_transfer_epoch_fact_token> merged_tokens;
    for (const Preserve_trx_transfer_epoch_fact_token &token :
         existing_fact.tokens) {
      if (!merged_tokens.emplace(token.token, token).second) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
    }
    for (const Preserve_trx_transfer_epoch_fact_token &token : fact.tokens) {
      const auto existing = merged_tokens.find(token.token);
      if (existing != merged_tokens.end()) {
        if (!epoch_fact_tokens_equal(existing->second, token)) {
          return Preserve_trx_transfer_status::CORRUPT;
        }
        continue;
      }
      merged_tokens.emplace(token.token, token);
    }

    fact.tokens.clear();
    fact.tokens.reserve(merged_tokens.size());
    for (const auto &entry : merged_tokens) {
      fact.tokens.push_back(entry.second);
    }
  }

  std::string encoded;
  status = preserve_trx_transfer_encode_epoch_fact(fact, &encoded);
  if (status != Preserve_trx_transfer_status::OK) return status;

  const std::string epoch_dir = transfer_epoch_dir_for_epoch(root_dir,
                                                            fact.epoch_id);
  if (ensure_dir_exists(epoch_dir)) return Preserve_trx_transfer_status::IO_ERROR;
  const std::string final_path = transfer_epoch_fact_path(root_dir, fact.epoch_id);
  const std::string tmp_path =
      transfer_unique_tmp_path(final_path, fact.epoch_id);
  throttle_receiver_saved_io(encoded.length());
  File file =
      my_create(tmp_path.c_str(), 0600, O_WRONLY | O_CREAT | O_EXCL, MYF(0));
  if (file < 0) return Preserve_trx_transfer_status::IO_ERROR;
  bool error =
      my_write(file, reinterpret_cast<const unsigned char *>(encoded.data()),
               encoded.length(), MYF(0)) != encoded.length();
  if (my_close(file, MYF(0))) error = true;
  if (!error && my_rename(tmp_path.c_str(), final_path.c_str(), MYF(0))) {
    error = true;
  }
  if (error) {
    (void)my_delete(tmp_path.c_str(), MYF(0));
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_encode_frame(
    const Preserve_trx_transfer_frame &frame, std::string *encoded) {
  if (encoded == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  const Preserve_trx_transfer_status validation_status =
      validate_frame_components(frame, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  std::string payload;
  if (append_string(&payload, frame.manifest_payload) ||
      append_string(&payload, frame.chunk_payload) ||
      append_string(&payload, frame.reason)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const auto payload_digest = sha256_digest(payload);

  std::string out;
  out.append(kTransferFrameMagic, kTransferFrameMagicLength);
  append_u16(&out, frame.protocol_version);
  append_u16(&out, static_cast<uint16_t>(frame.type));
  append_u64(&out, frame.sequence);
  if (append_string(&out, frame.epoch_id) ||
      append_string(&out, frame.receiver_process_nonce)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  append_u64(&out, frame.token);
  if (append_string(&out, frame.object_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  append_u64(&out, frame.chunk_offset);
  append_u64(&out, frame.trx_id_store.source_trx_id_store);
  append_u64(&out, frame.trx_id_store.source_trx_id_store_lsn);
  append_u64(&out, frame.trx_id_store.source_safe_next_trx_id_floor);
  append_u64(&out, frame.requested_terminal_status_retention_us);
  out.append(reinterpret_cast<const char *>(
                 frame.terminal_fact_digest.data()),
             frame.terminal_fact_digest.size());
  if (frame.protocol_version == kPreserveTrxTransferOpenContractVersion) {
    append_temp_id_contract(&out, frame.temp_id_contract);
  }
  append_u64(&out, payload.length());
  out.append(reinterpret_cast<const char *>(payload_digest.data()),
             payload_digest.size());
  append_u32(&out,
             static_cast<uint32_t>(my_checksum(
                 0, pointer_cast<const uchar *>(out.data()), out.length())));
  out.append(payload);

  *encoded = std::move(out);
  return Preserve_trx_transfer_status::OK;
}

#ifndef NDEBUG
namespace {
struct Receiver_decode_counts { uint64_t batches{0}, frames{0}; };
thread_local Receiver_decode_counts *receiver_decode_counts = nullptr;
}  // namespace
#endif

Preserve_trx_transfer_status preserve_trx_transfer_decode_frame(
    const std::string &encoded, Preserve_trx_transfer_frame *frame) {
#ifndef NDEBUG
  if (receiver_decode_counts) ++receiver_decode_counts->frames;
#endif
  if (frame == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  Manifest_reader reader(encoded);
  const char *magic = nullptr;
  if (reader.read_fixed(kTransferFrameMagicLength, &magic) ||
      std::memcmp(magic, kTransferFrameMagic, kTransferFrameMagicLength) != 0) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  Preserve_trx_transfer_frame parsed;
  uint16_t raw_type = 0;
  if (reader.read_u16(&parsed.protocol_version)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (!transfer_protocol_version_is_decodable(parsed.protocol_version) &&
      parsed.protocol_version != kPreserveTrxTransferOpenContractVersion) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (reader.read_u16(&raw_type) ||
      !frame_type_supported(raw_type, &parsed.type) ||
      reader.read_u64(&parsed.sequence) ||
      reader.read_string(&parsed.epoch_id) ||
      reader.read_string(&parsed.receiver_process_nonce) ||
      reader.read_u64(&parsed.token) ||
      reader.read_string(&parsed.object_id) ||
      reader.read_u64(&parsed.chunk_offset)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  uint64_t payload_length = 0;
  const char *payload_digest_bytes = nullptr;
  const char *terminal_fact_digest_bytes = nullptr;
  uint32_t stored_control_crc = 0;
  if (reader.read_u64(&parsed.trx_id_store.source_trx_id_store) ||
      reader.read_u64(&parsed.trx_id_store.source_trx_id_store_lsn) ||
      reader.read_u64(
          &parsed.trx_id_store.source_safe_next_trx_id_floor) ||
      reader.read_u64(&parsed.requested_terminal_status_retention_us) ||
      reader.read_fixed(parsed.terminal_fact_digest.size(),
                        &terminal_fact_digest_bytes) ||
      (parsed.protocol_version == kPreserveTrxTransferOpenContractVersion &&
       read_temp_id_contract(&reader, &parsed.temp_id_contract)) ||
      reader.read_u64(&payload_length) ||
      reader.read_fixed(kPreservedTrxSha256Length, &payload_digest_bytes) ||
      reader.read_u32(&stored_control_crc)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  std::memcpy(parsed.terminal_fact_digest.data(),
              terminal_fact_digest_bytes,
              parsed.terminal_fact_digest.size());
  if (payload_length != reader.remaining() ||
      payload_length > std::numeric_limits<size_t>::max()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  const size_t control_length =
      encoded.length() - static_cast<size_t>(payload_length) -
      sizeof(uint32_t);
  const uint32_t expected_control_crc = static_cast<uint32_t>(my_checksum(
      0, pointer_cast<const uchar *>(encoded.data()), control_length));
  if (expected_control_crc != stored_control_crc) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  const char *payload_bytes = nullptr;
  if (reader.read_fixed(static_cast<size_t>(payload_length), &payload_bytes) ||
      !reader.eof()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  const std::string payload(payload_bytes, static_cast<size_t>(payload_length));
  std::array<unsigned char, kPreservedTrxSha256Length> stored_payload_digest{};
  std::memcpy(stored_payload_digest.data(), payload_digest_bytes,
              stored_payload_digest.size());
  if (stored_payload_digest != sha256_digest(payload)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  Manifest_reader payload_reader(payload);
  if (payload_reader.read_string(&parsed.manifest_payload) ||
      payload_reader.read_string(&parsed.chunk_payload) ||
      payload_reader.read_string(&parsed.reason) || !payload_reader.eof()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  const Preserve_trx_transfer_status validation_status =
      validate_frame_components(parsed, true);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  *frame = std::move(parsed);
  return Preserve_trx_transfer_status::OK;
}

bool transfer_frame_batch_magic_matches(const std::string &encoded) {
  return encoded.length() >= kTransferFrameBatchMagicLength &&
         std::memcmp(encoded.data(), kTransferFrameBatchMagic,
                     kTransferFrameBatchMagicLength) == 0;
}

static Preserve_trx_transfer_status encode_frame_batch_with_limit(
    const std::string *encoded_frames, size_t frame_count, uint64_t max_bytes,
    std::string *encoded_batch) {
  if (encoded_batch == nullptr || encoded_frames == nullptr || !frame_count ||
      frame_count > kMaxTransferManifestObjects) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  constexpr uint64_t kBatchControlBytes =
      kTransferFrameBatchMagicLength + sizeof(uint16_t) + sizeof(uint32_t) +
      sizeof(uint64_t) + kPreservedTrxSha256Length + sizeof(uint32_t);
  uint64_t total_bytes = kBatchControlBytes;
  for (size_t i = 0; i < frame_count; ++i) {
    const std::string &encoded_frame = encoded_frames[i];
    Preserve_trx_transfer_frame ignored;
    const auto status = preserve_trx_transfer_decode_frame(encoded_frame, &ignored);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (encoded_frame.length() >
            std::numeric_limits<uint64_t>::max() - sizeof(uint64_t) ||
        encoded_frame.length() + sizeof(uint64_t) >
            std::numeric_limits<uint64_t>::max() - total_bytes) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    total_bytes += sizeof(uint64_t) + encoded_frame.length();
    if ((max_bytes != 0 && total_bytes > max_bytes) ||
        total_bytes > std::numeric_limits<size_t>::max()) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
  }
  // Assemble once in the final allocation. The header authenticates the same
  // payload range as before; no second payload or copied frame vector is needed.
  std::string out;
  out.reserve(static_cast<size_t>(total_bytes));
  out.append(kBatchControlBytes, '\0');
  for (size_t i = 0; i < frame_count; ++i) {
    append_u64(&out, encoded_frames[i].length());
    out.append(encoded_frames[i]);
  }
  const size_t payload_size = out.size() - kBatchControlBytes;
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  SHA256(reinterpret_cast<const unsigned char *>(out.data()) + kBatchControlBytes,
         payload_size, digest.data());
  std::string header;
  header.reserve(kBatchControlBytes);
  header.append(kTransferFrameBatchMagic, kTransferFrameBatchMagicLength);
  append_u16(&header, kPreserveTrxTransferProtocolVersion);
  append_u32(&header, static_cast<uint32_t>(frame_count));
  append_u64(&header, payload_size);
  header.append(reinterpret_cast<const char *>(digest.data()), digest.size());
  append_u32(&header, static_cast<uint32_t>(my_checksum(
                          0, pointer_cast<const uchar *>(header.data()),
                          header.length())));
  std::memcpy(&out[0], header.data(), header.size());
  *encoded_batch = std::move(out);
  return Preserve_trx_transfer_status::OK;
}

static Preserve_trx_transfer_status encode_frame_batches_with_limit(
    const std::vector<std::string> &encoded_frames, uint64_t max_bytes,
    std::vector<std::string> *encoded_batches) {
  if (encoded_batches == nullptr || encoded_frames.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  encoded_batches->clear();
  const uint64_t batch_overhead =
      kTransferFrameBatchMagicLength + sizeof(uint16_t) + sizeof(uint32_t) +
      sizeof(uint64_t) + kPreservedTrxSha256Length + sizeof(uint32_t);
  std::vector<std::string> batch;
  uint64_t batch_bytes = batch_overhead;
  for (const std::string &frame : encoded_frames) {
    if (frame.length() >
        std::numeric_limits<uint64_t>::max() - sizeof(uint64_t)) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    const uint64_t frame_bytes = sizeof(uint64_t) + frame.length();
    if (max_bytes != 0 && frame_bytes > max_bytes -
                                             std::min(max_bytes,
                                                      batch_overhead)) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    if (!batch.empty() && max_bytes != 0 &&
        frame_bytes > max_bytes - batch_bytes) {
      std::string encoded_batch;
      Preserve_trx_transfer_status status = encode_frame_batch_with_limit(
          batch.data(), batch.size(), max_bytes, &encoded_batch);
      if (status != Preserve_trx_transfer_status::OK) return status;
      encoded_batches->push_back(std::move(encoded_batch));
      batch.clear();
      batch_bytes = batch_overhead;
    }
    if (frame_bytes > std::numeric_limits<uint64_t>::max() - batch_bytes) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    batch.push_back(frame);
    batch_bytes += frame_bytes;
  }
  if (!batch.empty()) {
    std::string encoded_batch;
    Preserve_trx_transfer_status status =
        encode_frame_batch_with_limit(batch.data(), batch.size(), max_bytes,
                                      &encoded_batch);
    if (status != Preserve_trx_transfer_status::OK) return status;
    encoded_batches->push_back(std::move(encoded_batch));
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_encode_frame_batch(
    const std::vector<std::string> &encoded_frames, std::string *encoded_batch) {
  return encode_frame_batch_with_limit(
      encoded_frames.data(), encoded_frames.size(),
      preserve_trx_transfer_max_inflight_bytes, encoded_batch);
}

namespace {

// The decoded output and its memory charge stay owned by this dispatch until
// all apply workers have joined. Public encoded callers use the same checks.
Preserve_trx_transfer_status decode_frame_batch_impl(
    const std::string &encoded_batch, std::vector<std::string> *encoded_frames,
    std::vector<Preserve_trx_transfer_frame> *decoded_frames,
    Preserve_memory_lease *retained_memory) {
#ifndef NDEBUG
  if (receiver_decode_counts) ++receiver_decode_counts->batches;
#endif
  if ((!encoded_frames && !decoded_frames) ||
      (decoded_frames && !retained_memory)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (encoded_frames) encoded_frames->clear();
  if (decoded_frames) decoded_frames->clear();
  Manifest_reader reader(encoded_batch);
  const char *magic = nullptr;
  uint16_t version = 0;
  uint32_t count = 0;
  uint64_t payload_length = 0;
  const char *payload_digest_bytes = nullptr;
  uint32_t stored_control_crc = 0;
  if (reader.read_fixed(kTransferFrameBatchMagicLength, &magic) ||
      std::memcmp(magic, kTransferFrameBatchMagic,
                  kTransferFrameBatchMagicLength) != 0 ||
      reader.read_u16(&version)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (!transfer_protocol_version_is_decodable(version)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (reader.read_u32(&count) || count == 0 ||
      count > kMaxTransferManifestObjects ||
      reader.read_u64(&payload_length) ||
      reader.read_fixed(kPreservedTrxSha256Length, &payload_digest_bytes) ||
      reader.read_u32(&stored_control_crc) ||
      payload_length != reader.remaining() ||
      payload_length > std::numeric_limits<size_t>::max()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  const size_t control_length =
      encoded_batch.length() - static_cast<size_t>(payload_length) -
      sizeof(uint32_t);
  if (stored_control_crc !=
      static_cast<uint32_t>(my_checksum(
          0, pointer_cast<const uchar *>(encoded_batch.data()),
          control_length))) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  Manifest_reader payload_reader(reader);
  const char *payload_bytes = nullptr;
  if (reader.read_fixed(static_cast<size_t>(payload_length), &payload_bytes) ||
      !reader.eof()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  std::array<unsigned char, kPreservedTrxSha256Length> stored_payload_digest{};
  std::memcpy(stored_payload_digest.data(), payload_digest_bytes,
              stored_payload_digest.size());
  std::array<unsigned char, kPreservedTrxSha256Length> actual_payload_digest{};
  SHA256(reinterpret_cast<const unsigned char *>(payload_bytes),
         static_cast<size_t>(payload_length), actual_payload_digest.data());
  if (stored_payload_digest != actual_payload_digest) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (!preserve_trx_transfer_frame_batch_count_fits_payload(
          count, payload_reader.remaining())) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  uint64_t decode_bytes = static_cast<uint64_t>(count) *
      (decoded_frames ? sizeof(Preserve_trx_transfer_frame) : sizeof(std::string));
  if (decoded_frames) {
    if (encoded_batch.size() > (UINT64_MAX - decode_bytes) / 3)
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    // Peak: previously decoded frames plus the current inner wire string,
    // decoder payload and parsed frame. The outer payload is only a view.
    decode_bytes += 3ULL * encoded_batch.size();
  }
  Preserve_memory_lease decode_lease =
      acquire_transfer_decode_memory_lease(encoded_batch, decode_bytes);
  if (!decode_lease.acquired()) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }

  try {
    std::vector<std::string> out;
    std::vector<Preserve_trx_transfer_frame> decoded;
    if (encoded_frames) out.reserve(count);
    if (decoded_frames) decoded.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
      uint64_t length = 0;
      const char *ptr = nullptr;
      if (payload_reader.read_u64(&length) ||
          length > std::numeric_limits<size_t>::max() ||
          payload_reader.read_fixed(static_cast<size_t>(length), &ptr)) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      std::string encoded_frame(ptr, static_cast<size_t>(length));
      Preserve_trx_transfer_frame frame;
      const Preserve_trx_transfer_status frame_status =
          preserve_trx_transfer_decode_frame(encoded_frame, &frame);
      if (frame_status != Preserve_trx_transfer_status::OK) return frame_status;
      if (encoded_frames) out.push_back(std::move(encoded_frame));
      if (decoded_frames) decoded.push_back(std::move(frame));
    }
    if (!payload_reader.eof())
      return Preserve_trx_transfer_status::CORRUPT;
    if (encoded_frames) *encoded_frames = std::move(out);
    if (decoded_frames) {
      *decoded_frames = std::move(decoded);
      *retained_memory = std::move(decode_lease);
    }
  } catch (const std::bad_alloc &) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  return Preserve_trx_transfer_status::OK;
}

}  // namespace

Preserve_trx_transfer_status preserve_trx_transfer_decode_frame_batch(
    const std::string &encoded_batch, std::vector<std::string> *encoded_frames) {
  return decode_frame_batch_impl(encoded_batch, encoded_frames, nullptr, nullptr);
}

bool preserve_trx_transfer_frame_batch_count_fits_payload(
    uint32_t count, size_t remaining_bytes) {
  return count != 0 &&
         count <= remaining_bytes / sizeof(uint64_t);
}

namespace {

constexpr char kTransferAckMagic[] = {'P', 'T', 'R', 'X',
                                      'O', 'A', 'K', '1'};
constexpr size_t kTransferAckMagicLength = sizeof(kTransferAckMagic);

bool decode_lower_hex(const std::string &encoded, std::string *decoded) {
  if (decoded == nullptr || encoded.empty() || encoded.length() % 2 != 0) {
    return false;
  }
  auto nibble = [](char value, unsigned char *out) {
    if (value >= '0' && value <= '9') {
      *out = static_cast<unsigned char>(value - '0');
      return true;
    }
    if (value >= 'a' && value <= 'f') {
      *out = static_cast<unsigned char>(value - 'a' + 10);
      return true;
    }
    return false;
  };
  decoded->resize(encoded.length() / 2);
  for (size_t i = 0; i < decoded->size(); ++i) {
    unsigned char high = 0;
    unsigned char low = 0;
    if (!nibble(encoded[2 * i], &high) ||
        !nibble(encoded[2 * i + 1], &low)) {
      decoded->clear();
      return false;
    }
    (*decoded)[i] = static_cast<char>((high << 4) | low);
  }
  return true;
}

bool transfer_status_from_wire(uint16_t raw,
                               Preserve_trx_transfer_status *status) {
  if (status == nullptr ||
      raw > static_cast<uint16_t>(
                Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN)) {
    return false;
  }
  *status = static_cast<Preserve_trx_transfer_status>(raw);
  return true;
}

struct Payload_identity {
  std::string epoch;
  std::string nonce;
  uint64_t sequence{0};

  bool add(const Preserve_trx_transfer_frame &frame, size_t count, bool check_nonce) {
    if (epoch.empty()) epoch = frame.epoch_id;
    if (frame.epoch_id != epoch ||
        (sequence != 0 && frame.sequence != sequence + 1)) return false;
    sequence = frame.sequence;
    if (!check_nonce) return true;
    if (frame.type == Preserve_trx_transfer_frame_type::OPEN_EPOCH)
      return count == 1 && frame.receiver_process_nonce.empty();
    if (frame.receiver_process_nonce.length() != 32 ||
        !transfer_component_safe(frame.receiver_process_nonce)) return false;
    if (nonce.empty()) nonce = frame.receiver_process_nonce;
    return nonce == frame.receiver_process_nonce;
  }

  Preserve_trx_transfer_status finish(std::string *epoch_id, uint64_t *last_sequence,
                                      std::string *receiver_process_nonce) {
    if (epoch.empty()) return Preserve_trx_transfer_status::CORRUPT;
    *epoch_id = std::move(epoch);
    *last_sequence = sequence;
    if (receiver_process_nonce) *receiver_process_nonce = std::move(nonce);
    return Preserve_trx_transfer_status::OK;
  }
};

// Verify the original wire order before the application core sorts frames.
Preserve_trx_transfer_status decoded_payload_identity(
    const Preserve_trx_transfer_frame *frames, size_t count,
    std::string *epoch_id, uint64_t *last_sequence,
    std::string *receiver_process_nonce) {
  Payload_identity identity;
  for (size_t i = 0; i < count; ++i)
    if (!identity.add(frames[i], count, receiver_process_nonce != nullptr))
      return Preserve_trx_transfer_status::CORRUPT;
  return identity.finish(epoch_id, last_sequence, receiver_process_nonce);
}

Preserve_trx_transfer_status transfer_payload_identity(
    const std::string &encoded_payload, std::string *epoch_id,
    uint64_t *last_sequence, std::string *receiver_process_nonce = nullptr) {
  if (epoch_id == nullptr || last_sequence == nullptr || encoded_payload.empty())
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  std::vector<std::string> encoded_frames;
  if (transfer_frame_batch_magic_matches(encoded_payload)) {
    const auto status = preserve_trx_transfer_decode_frame_batch(encoded_payload,
                                                                 &encoded_frames);
    if (status != Preserve_trx_transfer_status::OK) return status;
  } else {
    encoded_frames.push_back(encoded_payload);
  }
  Payload_identity identity;
  for (const auto &encoded_frame : encoded_frames) {
    Preserve_trx_transfer_frame frame;
    const auto status = preserve_trx_transfer_decode_frame(encoded_frame, &frame);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (!identity.add(frame, encoded_frames.size(), receiver_process_nonce != nullptr))
      return Preserve_trx_transfer_status::CORRUPT;
  }
  return identity.finish(epoch_id, last_sequence, receiver_process_nonce);
}

Preserve_trx_transfer_status build_ack_body(
    const Preserve_trx_transfer_frame_ack &ack, std::string *body) {
  const bool open_ack = ack.sequence == 0;
  if (body == nullptr || ack.receiver_process_nonce.length() != 32 ||
      !open_contract_valid(ack.protocol_version, open_ack,
                           ack.temp_id_contract) ||
      !transfer_component_safe(ack.receiver_process_nonce) ||
      !transfer_component_safe(ack.epoch_id) ||
      (open_ack == (ack.accepted_terminal_status_retention_us == 0))) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  body->clear();
  body->append(kTransferAckMagic, kTransferAckMagicLength);
  append_u16(body, ack.protocol_version);
  if (append_string(body, ack.epoch_id) ||
      append_string(body, ack.receiver_process_nonce)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  append_u64(body, ack.sequence);
  body->append(reinterpret_cast<const char *>(ack.frame_digest.data()),
               ack.frame_digest.size());
  append_u16(body, static_cast<uint16_t>(ack.status));
  append_u64(body, ack.accepted_terminal_status_retention_us);
  if (ack.protocol_version == kPreserveTrxTransferOpenContractVersion) {
    append_temp_id_contract(body, ack.temp_id_contract);
  }
  return Preserve_trx_transfer_status::OK;
}

}  // namespace

Preserve_trx_transfer_status preserve_trx_transfer_build_frame_ack(
    const std::string &receiver_process_nonce,
    const std::string &encoded_payload, Preserve_trx_transfer_status status,
    Preserve_trx_transfer_frame_ack *ack) {
  if (ack == nullptr || receiver_process_nonce.length() != 32) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  Preserve_trx_transfer_frame_ack built;
  built.receiver_process_nonce = receiver_process_nonce;
  Preserve_trx_transfer_status identity_status = transfer_payload_identity(
      encoded_payload, &built.epoch_id, &built.sequence);
  if (identity_status != Preserve_trx_transfer_status::OK) {
    return identity_status;
  }
  built.frame_digest = sha256_digest(encoded_payload);
  built.status = status;
  Preserve_trx_transfer_frame payload_frame;
  if (built.sequence == 0 &&
      preserve_trx_transfer_decode_frame(encoded_payload, &payload_frame) ==
          Preserve_trx_transfer_status::OK &&
      payload_frame.type == Preserve_trx_transfer_frame_type::OPEN_EPOCH) {
    // The caller must supply the receiver-validated contract, never echo it.
    built.protocol_version = payload_frame.protocol_version;
    built.accepted_terminal_status_retention_us =
        payload_frame.requested_terminal_status_retention_us;
  }
  *ack = std::move(built);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_encode_frame_ack(
    const Preserve_trx_transfer_frame_ack &ack, std::string *encoded) {
  if (encoded == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  std::string body;
  Preserve_trx_transfer_status status = build_ack_body(ack, &body);
  if (status != Preserve_trx_transfer_status::OK) return status;
  append_u32(&body,
             static_cast<uint32_t>(my_checksum(
                 0, pointer_cast<const uchar *>(body.data()), body.length())));
  *encoded = bytes_to_lower_hex(
      reinterpret_cast<const unsigned char *>(body.data()), body.length());
  return Preserve_trx_transfer_status::OK;
}

namespace {
Preserve_trx_transfer_status verify_transfer_frame_ack(
    const std::string &encoded_ack,
    const std::string &expected_receiver_process_nonce,
    const std::string &encoded_payload, Preserve_trx_transfer_frame_ack *ack) {
  if (ack == nullptr ||
      (!expected_receiver_process_nonce.empty() &&
       expected_receiver_process_nonce.length() != 32)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::string raw;
  if (!decode_lower_hex(encoded_ack, &raw)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  Manifest_reader reader(raw);
  const char *magic = nullptr;
  Preserve_trx_transfer_frame_ack parsed;
  const char *digest = nullptr;
  uint16_t raw_status = 0;
  uint32_t stored_control_crc = 0;
  if (reader.read_fixed(kTransferAckMagicLength, &magic) ||
      std::memcmp(magic, kTransferAckMagic, kTransferAckMagicLength) != 0 ||
      reader.read_u16(&parsed.protocol_version)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (!transfer_protocol_version_is_decodable(parsed.protocol_version) &&
      parsed.protocol_version != kPreserveTrxTransferOpenContractVersion) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (reader.read_string(&parsed.epoch_id) ||
      reader.read_string(&parsed.receiver_process_nonce) ||
      reader.read_u64(&parsed.sequence) ||
      reader.read_fixed(parsed.frame_digest.size(), &digest) ||
      reader.read_u16(&raw_status) ||
      !transfer_status_from_wire(raw_status, &parsed.status) ||
      reader.read_u64(&parsed.accepted_terminal_status_retention_us) ||
      (parsed.protocol_version == kPreserveTrxTransferOpenContractVersion &&
       read_temp_id_contract(&reader, &parsed.temp_id_contract))) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (reader.read_u32(&stored_control_crc) || !reader.eof())
    return Preserve_trx_transfer_status::CORRUPT;
  std::memcpy(parsed.frame_digest.data(), digest, parsed.frame_digest.size());
  const size_t control_length = raw.length() - sizeof(uint32_t);
  if (stored_control_crc !=
      static_cast<uint32_t>(my_checksum(
          0, pointer_cast<const uchar *>(raw.data()), control_length))) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  std::string expected_epoch;
  uint64_t expected_sequence = 0;
  Preserve_trx_transfer_status status = Preserve_trx_transfer_status::OK;
  status = transfer_payload_identity(encoded_payload, &expected_epoch,
                                      &expected_sequence);
  if (status != Preserve_trx_transfer_status::OK) return status;
  if ((!expected_receiver_process_nonce.empty() &&
       parsed.receiver_process_nonce != expected_receiver_process_nonce) ||
      parsed.receiver_process_nonce.length() != 32 ||
      parsed.epoch_id != expected_epoch ||
      parsed.sequence != expected_sequence ||
      parsed.frame_digest != sha256_digest(encoded_payload)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (transfer_frame_batch_magic_matches(encoded_payload)) {
    if (parsed.protocol_version != kPreserveTrxTransferProtocolVersion ||
        !parsed.temp_id_contract.empty() ||
        parsed.accepted_terminal_status_retention_us != 0) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  } else {
    Preserve_trx_transfer_frame payload_frame;
    status = preserve_trx_transfer_decode_frame(encoded_payload, &payload_frame);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (parsed.protocol_version != payload_frame.protocol_version ||
        parsed.temp_id_contract != payload_frame.temp_id_contract) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    if (payload_frame.type == Preserve_trx_transfer_frame_type::OPEN_EPOCH) {
      if (parsed.accepted_terminal_status_retention_us <
          payload_frame.requested_terminal_status_retention_us) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
    } else if (parsed.accepted_terminal_status_retention_us != 0) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }
  *ack = std::move(parsed);
  return Preserve_trx_transfer_status::OK;
}
}  // namespace

#ifndef NDEBUG
Preserve_trx_transfer_status preserve_trx_transfer_verify_frame_ack(
    const std::string &encoded_ack,
    const std::string &expected_receiver_process_nonce,
    const std::string &encoded_payload, Preserve_trx_transfer_frame_ack *ack) {
  return verify_transfer_frame_ack(encoded_ack, expected_receiver_process_nonce,
                                    encoded_payload, ack);
}
#endif

Preserve_trx_transfer_status preserve_trx_transfer_validate_receiver_manifest(
    const Preserve_trx_transfer_manifest &manifest) {
  return validate_manifest_components(manifest, false);
}

Preserve_trx_transfer_status
preserve_trx_transfer_validate_receiver_manifest_from_config(
    const Preserve_trx_transfer_manifest &manifest) {
  return preserve_trx_transfer_validate_receiver_manifest(manifest);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::open_online_epoch(
    const std::string &epoch_id, const std::string &authenticated_principal,
    uint64_t requested_terminal_status_retention_us,
    const std::string &receiver_process_nonce,
    uint64_t *accepted_terminal_status_retention_us,
    const Preserve_trx_temp_id_contract &requested_temp_id_contract,
    Preserve_trx_temp_id_contract *accepted_temp_id_contract) {
  static constexpr uint64_t kMinimumRetentionUs = 60000000;
  static constexpr uint64_t kMaximumRetentionUs = 300000000;
  if (!transfer_component_safe(epoch_id) || authenticated_principal.empty() ||
      receiver_process_nonce.length() != 32 ||
      !transfer_component_safe(receiver_process_nonce) ||
      requested_terminal_status_retention_us == 0 ||
      requested_terminal_status_retention_us > kMaximumRetentionUs ||
      accepted_terminal_status_retention_us == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const uint64_t accepted_retention_us =
      std::max(requested_terminal_status_retention_us, kMinimumRetentionUs);
  Preserve_trx_temp_id_contract local_contract;
  if (!requested_temp_id_contract.empty() &&
      (!requested_temp_id_contract.supported() ||
       accepted_temp_id_contract == nullptr ||
       !preserve_trx_temp_id_local_contract(&local_contract) ||
       local_contract != requested_temp_id_contract)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  std::lock_guard<std::mutex> guard(m_mutex);
  auto existing = m_online_epochs.find(epoch_id);
  if (existing != m_online_epochs.end()) {
    if (existing->second.receiver_process_nonce != receiver_process_nonce ||
        existing->second.temp_id_contract != requested_temp_id_contract ||
        existing->second.authenticated_principal != authenticated_principal ||
        existing->second.requested_terminal_status_retention_us !=
            requested_terminal_status_retention_us ||
        existing->second.accepted_terminal_status_retention_us !=
            accepted_retention_us) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    *accepted_terminal_status_retention_us =
        existing->second.accepted_terminal_status_retention_us;
    if (accepted_temp_id_contract != nullptr) {
      *accepted_temp_id_contract = existing->second.temp_id_contract;
    }
    return Preserve_trx_transfer_status::OK;
  }
  // A retired transport must not rebind the surviving epoch's allocator.
  if (m_accepted_epochs.count(epoch_id) != 0 ||
      m_acknowledged_epochs.count(epoch_id) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  try {
    Online_epoch context;
    context.receiver_process_nonce = receiver_process_nonce;
    context.authenticated_principal = authenticated_principal;
    context.requested_terminal_status_retention_us =
        requested_terminal_status_retention_us;
    context.accepted_terminal_status_retention_us = accepted_retention_us;
    context.temp_id_contract = local_contract;
    context.runtime_policy = preserve_trx_transfer_current_runtime_policy();
    context.max_inflight_bytes = preserve_trx_transfer_max_inflight_bytes;
    m_online_epochs.emplace(epoch_id, std::move(context));
  } catch (...) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  *accepted_terminal_status_retention_us = accepted_retention_us;
  if (accepted_temp_id_contract != nullptr) {
    *accepted_temp_id_contract = local_contract;
  }
  return Preserve_trx_transfer_status::OK;
}

bool Preserve_trx_transfer_receiver_registry::online_epoch_runtime_policy(
    const std::string &epoch_id,
    Preserve_trx_transfer_runtime_policy *runtime_policy,
    uint64_t *max_inflight_bytes) const {
  if (runtime_policy == nullptr || max_inflight_bytes == nullptr) return false;
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_online_epochs.find(epoch_id);
  if (found == m_online_epochs.end()) return false;
  *runtime_policy = found->second.runtime_policy;
  *max_inflight_bytes = found->second.max_inflight_bytes;
  return true;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::validate_online_epoch(
    const std::string &epoch_id, const std::string &authenticated_principal,
    const std::string &receiver_process_nonce,
    uint64_t *accepted_terminal_status_retention_us) const {
  if (!transfer_component_safe(epoch_id) || authenticated_principal.empty() ||
      receiver_process_nonce.length() != 32 ||
      !transfer_component_safe(receiver_process_nonce)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto existing = m_online_epochs.find(epoch_id);
  if (existing == m_online_epochs.end()) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (existing->second.receiver_process_nonce != receiver_process_nonce ||
      existing->second.authenticated_principal != authenticated_principal) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (accepted_terminal_status_retention_us != nullptr) {
    *accepted_terminal_status_retention_us =
        existing->second.accepted_terminal_status_retention_us;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::declare_token(
    const std::string &epoch_id, uint64_t token) {
  if (!transfer_component_safe(epoch_id) || token == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  Preserve_trx_transfer_receiver_record record;
  record.epoch_id = epoch_id;
  record.token = token;
  record.state = Preserve_trx_transfer_receiver_state::DECLARED;

  std::lock_guard<std::mutex> guard(m_mutex);
  const auto online = m_online_epochs.find(epoch_id);
  if (online != m_online_epochs.end()) {
    record.temp_id_contract = online->second.temp_id_contract;
  }
  const Token_key key(epoch_id, token);
  if (m_records.find(key) != m_records.end()) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  m_records.emplace(key, std::move(record));
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::freeze_staged_manifest(
    const Preserve_trx_transfer_manifest &manifest) {
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_records.find(Token_key(manifest.epoch_id, manifest.token));
  if (found == m_records.end() ||
      found->second.state != Preserve_trx_transfer_receiver_state::RECEIVING ||
      !receiver_record_matches_manifest(found->second, manifest)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  auto &record = found->second;
  size_t snapshots = 0;
  for (const auto &object : record.objects) {
    if (record.sealed_objects.count(object.object_id) == 0)
      return Preserve_trx_transfer_status::UNSUPPORTED;
    if (object.kind == Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE)
      ++snapshots;
  }
  if (snapshots != 1) return Preserve_trx_transfer_status::UNSUPPORTED;
  record.staged_manifest_frozen = true;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::begin_receive(
    const Preserve_trx_transfer_manifest &manifest,
    uint64_t manifest_payload_bytes,
    std::vector<Staging_retirement> *retirements, bool *first_resource_final) {
  if (first_resource_final) *first_resource_final = false;
  if (retirements) retirements->clear();
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  uint64_t reserved_bytes = 0;
  const Preserve_trx_transfer_status reservation_status =
      receiver_manifest_reserved_bytes(manifest, manifest_payload_bytes,
                                       &reserved_bytes);
  if (reservation_status != Preserve_trx_transfer_status::OK) {
    return reservation_status;
  }

  Preserve_trx_transfer_receiver_record record;
  record.protocol_version = manifest.protocol_version;
  record.strict_eligibility_flags = manifest.strict_eligibility_flags;
  record.epoch_id = manifest.epoch_id;
  record.token = manifest.token;
  record.source_freeze_lsn = manifest.source_freeze_lsn;
  record.source_epoch_commit_lsn = manifest.source_epoch_commit_lsn;
  record.state = Preserve_trx_transfer_receiver_state::RECEIVING;
  record.objects = manifest.objects;
  record.has_resource_objects = std::any_of(
      record.objects.begin(), record.objects.end(), transfer_is_resource_object);
  record.has_snapshot_bundle = transfer_manifest_has_snapshot_bundle(manifest);
  record.reserved_bytes = reserved_bytes;
  for (const auto &object : record.objects) {
    if (object.kind == Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR ||
        object.kind == Preserve_trx_transfer_object_kind::CURSOR_RESULT)
      record.resource_reserved_bytes += object.total_size + kReceiverObjectReservationOverhead;
  }

  Preserve_trx_transfer_receiver_record::Sealed_files retired_files;
  std::lock_guard<std::mutex> guard(m_mutex);
  uint64_t max_inflight_bytes = preserve_trx_transfer_max_inflight_bytes;
  const auto online_epoch = m_online_epochs.find(manifest.epoch_id);
  if (online_epoch != m_online_epochs.end()) {
    max_inflight_bytes = online_epoch->second.max_inflight_bytes;
    record.temp_id_contract = online_epoch->second.temp_id_contract;
  }
  const Token_key key(manifest.epoch_id, manifest.token);
  auto existing_record = m_records.find(key);
  if (existing_record != m_records.end()) {
    if (existing_record->second.temp_id_contract != record.temp_id_contract) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    if ((existing_record->second.state !=
             Preserve_trx_transfer_receiver_state::DECLARED &&
         existing_record->second.state !=
             Preserve_trx_transfer_receiver_state::RECEIVING)) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    if (existing_record->second.staged_manifest_frozen) {
      return receiver_record_matches_manifest(existing_record->second, manifest)
                 ? Preserve_trx_transfer_status::OK
                 : Preserve_trx_transfer_status::UNSUPPORTED;
    }
    const auto &previous = existing_record->second;
    if (!previous.objects.empty() && !previous.object_index)
      return Preserve_trx_transfer_status::CORRUPT;
    for (const Preserve_trx_transfer_object_descriptor &object :
         manifest.objects) {
      const Preserve_trx_transfer_object_descriptor *existing =
          previous.object_index
              ? previous.object_index->find(previous.objects, object.object_id)
              : nullptr;
      if (existing == nullptr ||
          transfer_object_descriptor_equal(*existing, object)) {
        continue;
      }
      if (object.object_id != kPreservedTrxBlobRecordLocks ||
          object.kind != Preserve_trx_transfer_object_kind::EXTERNAL_BLOB) {
        return Preserve_trx_transfer_status::UNSUPPORTED;
      }
      const Preserve_trx_transfer_status replacement_status =
          transfer_lock_plan_replacement_status(*existing, object);
      if (replacement_status != Preserve_trx_transfer_status::OK) {
        return replacement_status;
      }
    }
  }
  const uint64_t old_reserved = existing_record == m_records.end()
      ? 0 : existing_record->second.reserved_bytes;
  const auto index_status = Preserve_trx_transfer_object_index::build(
      manifest.token, record.objects, kMaxTransferManifestObjects,
      &record.object_index);
  if (index_status != Preserve_trx_transfer_status::OK) return index_status;
  std::vector<uint64_t> retiring;
  uint64_t retiring_bytes = 0;
  if (existing_record != m_records.end()) {
    for (const auto &object : existing_record->second.objects) {
      const auto *selected = record.object_index->find(record.objects, object.object_id);
      if (selected && transfer_object_descriptor_equal(object, *selected)) continue;
      const auto status = stage_retirement_locked(existing_record->second, object,
                                                  &retiring, &retiring_bytes);
      if (status != Preserve_trx_transfer_status::OK) return status;
    }
  }
  if (reserved_bytes > UINT64_MAX - retiring_bytes)
    return Preserve_trx_transfer_status::UNSUPPORTED;
  const auto quota_status = check_reservation_locked(
      manifest.epoch_id, old_reserved, reserved_bytes + retiring_bytes, max_inflight_bytes);
  if (quota_status != Preserve_trx_transfer_status::OK) return quota_status;
  if (retirements) {
    try {
      retirements->reserve(retiring.size());
      for (const auto &item : retiring)
        retirements->push_back({item, m_retired_staging.find(item)->second.object_id});
    } catch (...) { return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED; }
  }
  const uint64_t first_ticket = m_next_staging_retirement;
  bool admitted = false;
  auto discard_tickets = create_scope_guard([&] {
    if (!admitted) discard_staging_tickets_locked(first_ticket);
  });
  try {
    for (const auto &object : record.objects) {
      if (!transfer_is_resource_object(object)) continue;
      uint64_t id = 0;
      if (existing_record != m_records.end()) {
        const auto old = existing_record->second.staging_tickets.find(object.object_id);
        if (old != existing_record->second.staging_tickets.end()) id = old->second;
      }
      if (!id) {
        const auto status = prepare_staging_ticket_locked(key, object, &id);
        if (status != Preserve_trx_transfer_status::OK) return status;
      }
      record.staging_tickets.emplace(object.object_id, id);
    }
  } catch (const std::bad_alloc &) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  if (existing_record == m_records.end()) {
    const auto inserted = m_records.emplace(key, std::move(record));
    inserted.first->second.reserved_bytes = 0;
    set_reservation_locked(inserted.first->second, reserved_bytes);
  } else {
    const auto &previous = existing_record->second;
    for (const auto &object : previous.objects) {
      if (object.kind == Preserve_trx_transfer_object_kind::CURSOR_RESULT &&
          !record.object_index->find(record.objects, object.object_id))
        return Preserve_trx_transfer_status::CORRUPT;
    }
    record.resource_candidates = previous.resource_candidates;
    record.resource_final_observed = previous.resource_final_observed;
    for (const Preserve_trx_transfer_object_descriptor &object :
         manifest.objects) {
      const Preserve_trx_transfer_object_descriptor *existing =
          previous.object_index
              ? previous.object_index->find(previous.objects, object.object_id)
              : nullptr;
      if (existing != nullptr &&
          transfer_object_descriptor_equal(*existing, object) &&
          existing_record->second.sealed_objects.count(object.object_id) != 0) {
        record.sealed_objects.insert(object.object_id);
        const auto file =
            existing_record->second.sealed_files.find(object.object_id);
        if (file != existing_record->second.sealed_files.end()) {
          record.sealed_files.emplace(file->first, file->second);
        }
      }
    }
    retired_files.swap(existing_record->second.sealed_files);
    commit_retirements_locked(retiring, retiring_bytes);
    set_reservation_locked(existing_record->second, reserved_bytes);
    existing_record->second = std::move(record);
    admitted = true;
    if (existing_record->second.resource_candidates)
      existing_record->second.resource_candidates->retain_selected_undo(existing_record->second);
  }
  admitted = true;
  auto &current = m_records.find(key)->second;
  if (current.has_resource_objects && current.has_snapshot_bundle &&
      !current.resource_final_observed) {
    current.resource_final_observed = true;
    if (first_resource_final) *first_resource_final = true;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::declare_object(
    const std::string &epoch_id, uint64_t token,
    const Preserve_trx_transfer_object_descriptor &descriptor,
    uint64_t *retirement_id) {
  if (retirement_id) *retirement_id = 0;
  std::shared_ptr<const Preserve_trx_sealed_file> retired_file;
  if (!transfer_component_safe(epoch_id) || token == 0 ||
      !transfer_component_safe(descriptor.object_id) ||
      !transfer_lock_plan_contract_valid(descriptor)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::lock_guard<std::mutex> guard(m_mutex);
  uint64_t max_inflight_bytes = preserve_trx_transfer_max_inflight_bytes;
  const auto online_epoch = m_online_epochs.find(epoch_id);
  if (online_epoch != m_online_epochs.end()) {
    max_inflight_bytes = online_epoch->second.max_inflight_bytes;
  }
  const Token_key key(epoch_id, token);
  auto found = m_records.find(key);
  if (found == m_records.end()) return Preserve_trx_transfer_status::CORRUPT;
  if (found->second.state != Preserve_trx_transfer_receiver_state::DECLARED &&
      found->second.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  uint64_t new_object_bytes = 0;
  const Preserve_trx_transfer_status reservation_status =
      receiver_object_reserved_bytes(descriptor, &new_object_bytes);
  if (reservation_status != Preserve_trx_transfer_status::OK) {
    return reservation_status;
  }
  auto &objects = found->second.objects;
  if (!objects.empty() && !found->second.object_index)
    return Preserve_trx_transfer_status::CORRUPT;
  const auto *existing = found->second.object_index
      ? found->second.object_index->find(objects, descriptor.object_id) : nullptr;
  if (found->second.staged_manifest_frozen) {
    return existing != nullptr &&
                   transfer_object_descriptor_equal(*existing, descriptor)
               ? Preserve_trx_transfer_status::OK
               : Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (existing != nullptr) {
    if (transfer_object_descriptor_equal(*existing, descriptor)) {
      return Preserve_trx_transfer_status::OK;
    }
    const Preserve_trx_transfer_status replacement_status =
        transfer_lock_plan_replacement_status(*existing, descriptor);
    if (replacement_status != Preserve_trx_transfer_status::OK) {
      return replacement_status;
    }
    uint64_t old_object_bytes = 0;
    const Preserve_trx_transfer_status old_reservation_status =
        receiver_object_reserved_bytes(*existing, &old_object_bytes);
    if (old_reservation_status != Preserve_trx_transfer_status::OK ||
        found->second.reserved_bytes < old_object_bytes) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    const uint64_t base_reserved_bytes =
        found->second.reserved_bytes - old_object_bytes;
    if (new_object_bytes > std::numeric_limits<uint64_t>::max() -
                               base_reserved_bytes) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    const uint64_t replacement_reserved_bytes = base_reserved_bytes + new_object_bytes;
    std::vector<uint64_t> retiring;
    uint64_t retiring_bytes = 0;
    const auto retirement_status = stage_retirement_locked(found->second, *existing,
                                                            &retiring, &retiring_bytes);
    if (retirement_status != Preserve_trx_transfer_status::OK) return retirement_status;
    if (replacement_reserved_bytes > UINT64_MAX - retiring_bytes)
      return Preserve_trx_transfer_status::UNSUPPORTED;
    const auto quota_status = check_reservation_locked(epoch_id,
        found->second.reserved_bytes, replacement_reserved_bytes + retiring_bytes, max_inflight_bytes);
    if (quota_status != Preserve_trx_transfer_status::OK) return quota_status;
    const uint64_t first_ticket = m_next_staging_retirement;
    bool admitted = false;
    auto discard_tickets = create_scope_guard([&] {
      if (!admitted) discard_staging_tickets_locked(first_ticket);
    });
    uint64_t next_ticket = 0;
    const auto ticket_status = prepare_staging_ticket_locked(key, descriptor, &next_ticket);
    if (ticket_status != Preserve_trx_transfer_status::OK) return ticket_status;
    try {
      auto replacement = descriptor;
      if (next_ticket) found->second.staging_tickets.emplace(descriptor.object_id, next_ticket);
      objects[existing - objects.data()] = std::move(replacement);
    } catch (...) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    if (next_ticket) found->second.staging_tickets.find(descriptor.object_id)->second = next_ticket;
    else found->second.staging_tickets.erase(descriptor.object_id);
    admitted = true;
    if (retirement_id && !retiring.empty()) *retirement_id = retiring.front();
    found->second.resource_reserved_bytes -= retiring_bytes;
    if (transfer_is_resource_object(descriptor))
      found->second.resource_reserved_bytes += new_object_bytes;
    commit_retirements_locked(retiring, retiring_bytes);
    set_reservation_locked(found->second, replacement_reserved_bytes);
    found->second.sealed_objects.erase(descriptor.object_id);
    const auto file = found->second.sealed_files.find(descriptor.object_id);
    if (file != found->second.sealed_files.end()) {
      retired_file = std::move(file->second);
      found->second.sealed_files.erase(file);
    }
    return Preserve_trx_transfer_status::OK;
  }

  if (new_object_bytes > std::numeric_limits<uint64_t>::max() -
                             found->second.reserved_bytes) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  const uint64_t record_reserved_bytes =
      found->second.reserved_bytes + new_object_bytes;
  const auto quota_status = check_reservation_locked(epoch_id,
      found->second.reserved_bytes, record_reserved_bytes, max_inflight_bytes);
  if (quota_status != Preserve_trx_transfer_status::OK) return quota_status;
  std::shared_ptr<const Preserve_trx_transfer_object_index> next_index;
  const auto index_status = Preserve_trx_transfer_object_index::build(
      token, found->second.objects, kMaxTransferManifestObjects, &next_index,
      &descriptor, found->second.object_index.get());
  if (index_status != Preserve_trx_transfer_status::OK) return index_status;
  const uint64_t first_ticket = m_next_staging_retirement;
  bool admitted = false;
  auto discard_tickets = create_scope_guard([&] {
    if (!admitted) {
      found->second.staging_tickets.erase(descriptor.object_id);
      discard_staging_tickets_locked(first_ticket);
    }
  });
  uint64_t ticket = 0;
  const auto ticket_status = prepare_staging_ticket_locked(key, descriptor, &ticket);
  if (ticket_status != Preserve_trx_transfer_status::OK) return ticket_status;
  try {
    if (ticket) found->second.staging_tickets.emplace(descriptor.object_id, ticket);
    found->second.objects.push_back(descriptor);
  } catch (...) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  admitted = true;
  found->second.has_resource_objects |= transfer_is_resource_object(descriptor);
  if (descriptor.kind == Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR ||
      descriptor.kind == Preserve_trx_transfer_object_kind::CURSOR_RESULT)
    found->second.resource_reserved_bytes += new_object_bytes;
  found->second.has_snapshot_bundle |=
      descriptor.kind == Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE;
  found->second.object_index = std::move(next_index);
  set_reservation_locked(found->second, record_reserved_bytes);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::stage_strict_v1_object_chunk(
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id, uint64_t chunk_offset,
    const std::string &chunk_payload) {
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK) {
    return validation_status;
  }
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, object_id);
  if (object == nullptr ||
      !transfer_object_uses_strict_v1_memory_staging(manifest, *object)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (object->total_size > std::numeric_limits<size_t>::max()) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  if (chunk_offset > object->total_size ||
      chunk_payload.length() > object->total_size - chunk_offset) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  std::lock_guard<std::mutex> guard(m_mutex);
  const Token_key key(manifest.epoch_id, manifest.token);
  const auto record = m_records.find(key);
  if (record == m_records.end() ||
      record->second.state !=
          Preserve_trx_transfer_receiver_state::RECEIVING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  const auto declared = std::find_if(
      record->second.objects.begin(), record->second.objects.end(),
      [&](const Preserve_trx_transfer_object_descriptor &candidate) {
        return candidate.object_id == object_id;
      });
  if (declared == record->second.objects.end() ||
      !transfer_object_descriptor_equal(*declared, *object)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  Strict_v1_object *staged = nullptr;
  try {
    auto &objects = m_strict_v1_objects[key];
    auto found = objects.find(object_id);
    if (found == objects.end()) {
      Strict_v1_object value;
      value.descriptor = *object;
      value.payload = std::make_shared<std::string>();
      value.payload->reserve(static_cast<size_t>(object->total_size));
      found = objects.emplace(object_id, std::move(value)).first;
    }
    staged = &found->second;
  } catch (...) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  if (staged == nullptr || staged->payload == nullptr ||
      !transfer_object_descriptor_equal(staged->descriptor, *object) ||
      chunk_offset > staged->payload->length()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  const size_t overlap = static_cast<size_t>(std::min<uint64_t>(
      chunk_payload.length(), staged->payload->length() - chunk_offset));
  if (overlap != 0 &&
      staged->payload->compare(static_cast<size_t>(chunk_offset), overlap,
                               chunk_payload, 0, overlap) != 0) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (overlap == chunk_payload.length()) {
    return Preserve_trx_transfer_status::OK;
  }
  if (staged->sealed) return Preserve_trx_transfer_status::CORRUPT;
  try {
    staged->payload->append(chunk_payload, overlap,
                            chunk_payload.length() - overlap);
  } catch (...) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  return staged->payload->length() <= object->total_size
             ? Preserve_trx_transfer_status::OK
             : Preserve_trx_transfer_status::CORRUPT;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::seal_strict_v1_object(
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id) {
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, object_id);
  if (object == nullptr ||
      !transfer_object_uses_strict_v1_memory_staging(manifest, *object)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  std::shared_ptr<std::string> payload;
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto token =
        m_strict_v1_objects.find(Token_key(manifest.epoch_id, manifest.token));
    if (token == m_strict_v1_objects.end()) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    const auto found = token->second.find(object_id);
    if (found == token->second.end() || found->second.payload == nullptr ||
        !transfer_object_descriptor_equal(found->second.descriptor, *object) ||
        found->second.payload->length() != object->total_size) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    if (found->second.sealed) return Preserve_trx_transfer_status::OK;
    payload = found->second.payload;
  }

  if (sha256_digest(*payload) != object->digest) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto token =
      m_strict_v1_objects.find(Token_key(manifest.epoch_id, manifest.token));
  if (token == m_strict_v1_objects.end()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  const auto found = token->second.find(object_id);
  if (found == token->second.end() || found->second.payload != payload ||
      !transfer_object_descriptor_equal(found->second.descriptor, *object) ||
      found->second.payload->length() != object->total_size) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  found->second.sealed = true;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::read_strict_v1_object(
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id,
    std::shared_ptr<const std::string> *payload) const {
  if (payload == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, object_id);
  if (object == nullptr ||
      !transfer_object_uses_strict_v1_memory_staging(manifest, *object)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto token =
      m_strict_v1_objects.find(Token_key(manifest.epoch_id, manifest.token));
  if (token == m_strict_v1_objects.end()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  const auto found = token->second.find(object_id);
  if (found == token->second.end() || !found->second.sealed ||
      found->second.payload == nullptr ||
      !transfer_object_descriptor_equal(found->second.descriptor, *object)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  *payload = found->second.payload;
  return Preserve_trx_transfer_status::OK;
}


void Preserve_trx_transfer_receiver_registry::erase_strict_v1_token_objects(
    const std::string &epoch_id, uint64_t token) {
  std::lock_guard<std::mutex> guard(m_mutex);
  m_strict_v1_objects.erase(Token_key(epoch_id, token));
}

size_t Preserve_trx_transfer_receiver_registry::retry_cleanup_debt_once(
    uint64_t now_us) {
  static constexpr uint kCleanupRetryLimit = 5;
  static constexpr uint64_t kCleanupRetryBaseUs = 1000000;
  std::vector<std::pair<Token_key, Cleanup_debt>> due;
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    for (const auto &item : m_cleanup_debts) {
      if (item.second.next_retry_us <= now_us) due.push_back(item);
    }
  }

  size_t completed = 0;
  for (const auto &item : due) {
    const Preserve_trx_transfer_status cleanup_status =
        cleanup_transfer_token_staging(item.second.root_dir, item.first.first,
                                       item.first.second);
    std::lock_guard<std::mutex> guard(m_mutex);
    auto debt = m_cleanup_debts.find(item.first);
    auto record = m_records.find(item.first);
    if (debt == m_cleanup_debts.end() || record == m_records.end() ||
        record->second.state !=
            Preserve_trx_transfer_receiver_state::CLEANUP_PENDING) {
      continue;
    }
    if (cleanup_status == Preserve_trx_transfer_status::OK) {
      mark_staging_token_deleted_locked(item.first);
      record->second.state = debt->second.target_state;
      record->second.reserved_bytes = 0;
      record->second.last_error.clear();
      subtract_cleanup_debt_bytes_locked(debt->second.reserved_bytes);
      m_cleanup_debts.erase(debt);
      ++completed;
      continue;
    }

    ++debt->second.attempts;
    record->second.last_error =
        "staging_cleanup_retry_failed:" + transfer_status_name(cleanup_status);
    if (debt->second.attempts >= kCleanupRetryLimit) {
      record->second.state =
          Preserve_trx_transfer_receiver_state::CLEANUP_TAINTED;
      debt->second.next_retry_us = std::numeric_limits<uint64_t>::max();
      m_last_failed_token = record->second.token;
      m_last_failed_reason = record->second.last_error;
      continue;
    }
    const uint shift = std::min<uint>(debt->second.attempts - 1, 6);
    const uint64_t delay = kCleanupRetryBaseUs << shift;
    debt->second.next_retry_us =
        now_us > std::numeric_limits<uint64_t>::max() - delay
            ? std::numeric_limits<uint64_t>::max()
            : now_us + delay;
  }
  return completed;
}

size_t
Preserve_trx_transfer_receiver_registry::cleanup_debt_count_for_unit_test()
    const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_cleanup_debts.size();
}

Preserve_trx_transfer_epoch_terminal_outcome
Preserve_trx_transfer_receiver_registry::query_epoch_terminal(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_epoch_terminal_status *terminal) const {
  if (root_dir.empty() || !transfer_component_safe(epoch_id)) {
    return Preserve_trx_transfer_epoch_terminal_outcome::EPOCH_NOT_FOUND;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto acknowledged = m_acknowledged_epochs.find(epoch_id);
  if (acknowledged != m_acknowledged_epochs.end() &&
      acknowledged->second.root_dir == root_dir &&
      acknowledged->second.terminal_operation !=
          Preserve_trx_transfer_epoch_terminal_operation::NONE) {
    if (terminal != nullptr) {
      terminal->operation = acknowledged->second.terminal_operation;
      terminal->outcome = acknowledged->second.terminal_outcome;
      terminal->receiver_process_generation =
          acknowledged->second.receiver_process_generation;
      terminal->receiver_process_nonce =
          acknowledged->second.receiver_process_nonce;
      terminal->authenticated_principal =
          acknowledged->second.authenticated_principal;
      terminal->operation_id =
          acknowledged->second.terminal_operation_id;
      terminal->fact_digest = acknowledged->second.fact_digest;
      terminal->terminal_cas_monotonic_us =
          acknowledged->second.terminal_cas_monotonic_us;
      terminal->retire_after_us = acknowledged->second.retire_after_us;
    }
    return acknowledged->second.terminal_outcome;
  }

  const bool receiving = std::any_of(
      m_records.begin(), m_records.end(), [&](const auto &item) {
        return item.first.first == epoch_id;
      });
  if (receiving) {
    if (terminal != nullptr) {
      *terminal = Preserve_trx_transfer_epoch_terminal_status();
      terminal->outcome =
          Preserve_trx_transfer_epoch_terminal_outcome::NOT_COMMITTED;
    }
    return Preserve_trx_transfer_epoch_terminal_outcome::NOT_COMMITTED;
  }
  return Preserve_trx_transfer_epoch_terminal_outcome::EPOCH_NOT_FOUND;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::query_epoch_terminal_authenticated(
    const std::string &root_dir, const std::string &epoch_id,
    const std::string &authenticated_principal,
    const std::string &receiver_process_nonce,
    const std::array<unsigned char, kPreservedTrxSha256Length> &fact_digest,
    Preserve_trx_transfer_epoch_terminal_status *terminal) const {
  if (root_dir.empty() || !transfer_component_safe(epoch_id) ||
      authenticated_principal.empty() ||
      receiver_process_nonce.length() != 32 ||
      !transfer_component_safe(receiver_process_nonce) ||
      transfer_digest_is_zero(fact_digest)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::lock_guard<std::mutex> guard(m_mutex);
  const auto acknowledged = m_acknowledged_epochs.find(epoch_id);
  if (acknowledged != m_acknowledged_epochs.end() &&
      acknowledged->second.root_dir == root_dir &&
      acknowledged->second.terminal_operation !=
          Preserve_trx_transfer_epoch_terminal_operation::NONE) {
    const Acknowledged_epoch &current = acknowledged->second;
    if (current.authenticated_principal != authenticated_principal ||
        current.receiver_process_nonce != receiver_process_nonce ||
        current.fact_digest != fact_digest) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    if (terminal != nullptr) {
      terminal->operation = current.terminal_operation;
      terminal->outcome = current.terminal_outcome;
      terminal->receiver_process_generation =
          current.receiver_process_generation;
      terminal->receiver_process_nonce = current.receiver_process_nonce;
      terminal->authenticated_principal = current.authenticated_principal;
      terminal->operation_id = current.terminal_operation_id;
      terminal->fact_digest = current.fact_digest;
      terminal->terminal_cas_monotonic_us = current.terminal_cas_monotonic_us;
      terminal->retire_after_us = current.retire_after_us;
    }
    return Preserve_trx_transfer_status::OK;
  }

  const auto online = m_online_epochs.find(epoch_id);
  if (online == m_online_epochs.end()) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (online->second.authenticated_principal != authenticated_principal ||
      online->second.receiver_process_nonce != receiver_process_nonce) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (terminal != nullptr) {
    *terminal = Preserve_trx_transfer_epoch_terminal_status();
    terminal->outcome =
        Preserve_trx_transfer_epoch_terminal_outcome::NOT_COMMITTED;
    terminal->receiver_process_nonce = receiver_process_nonce;
    terminal->authenticated_principal = authenticated_principal;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_epoch_terminal_outcome
Preserve_trx_transfer_receiver_registry::try_begin_epoch_abandon(
    const Preserve_trx_transfer_epoch_terminal_request &request) {
  if (request.root_dir.empty() ||
      !transfer_component_safe(request.epoch_id) ||
      request.receiver_process_generation.empty() ||
      request.operation_id.empty() ||
      transfer_digest_is_zero(request.fact_digest) ||
      request.retention_us == 0) {
    return Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
  }
  const uint64_t retire_after_us =
      request.now_us >
              std::numeric_limits<uint64_t>::max() - request.retention_us
          ? std::numeric_limits<uint64_t>::max()
          : request.now_us + request.retention_us;

  std::lock_guard<std::mutex> guard(m_mutex);
  auto acknowledged = m_acknowledged_epochs.find(request.epoch_id);
  if (acknowledged != m_acknowledged_epochs.end() &&
      acknowledged->second.terminal_operation !=
          Preserve_trx_transfer_epoch_terminal_operation::NONE) {
    Acknowledged_epoch &current = acknowledged->second;
    if (current.terminal_operation ==
        Preserve_trx_transfer_epoch_terminal_operation::COMMIT) {
      const bool authenticated_terminal =
          !current.root_dir.empty() &&
          !current.authenticated_principal.empty() &&
          !current.receiver_process_nonce.empty() &&
          !transfer_digest_is_zero(current.fact_digest);
      if (authenticated_terminal &&
          (current.root_dir != request.root_dir ||
           current.receiver_process_generation !=
               request.receiver_process_generation ||
           current.authenticated_principal !=
               request.authenticated_principal ||
           current.fact_digest != request.fact_digest)) {
        g_receiver_terminal_cas_conflicts.fetch_add(1);
        return Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
      }
      g_receiver_terminal_cas_conflicts.fetch_add(1);
      return current.terminal_outcome;
    }
    if (!current.root_dir.empty() && current.root_dir != request.root_dir) {
      current.terminal_phase = Terminal_phase::CORRUPT;
      current.terminal_outcome =
          Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
      g_receiver_terminal_cas_conflicts.fetch_add(1);
      return Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
    }
    const bool same_operation =
        current.root_dir == request.root_dir &&
        current.receiver_process_generation ==
            request.receiver_process_generation &&
        current.authenticated_principal ==
            request.authenticated_principal &&
        current.terminal_operation_id == request.operation_id &&
        current.fact_digest == request.fact_digest;
    if (same_operation) {
      g_receiver_terminal_cas_duplicates.fetch_add(1);
      return current.terminal_outcome;
    }
    current.terminal_phase = Terminal_phase::CORRUPT;
    current.terminal_outcome =
        Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
    const auto accepted = m_accepted_epochs.find(request.epoch_id);
    if (accepted != m_accepted_epochs.end()) {
      accepted->second.lifecycle =
          Preserve_trx_transfer_epoch_lifecycle::ABANDONING;
    }
    g_receiver_terminal_cas_conflicts.fetch_add(1);
    return Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
  }
  if (acknowledged != m_acknowledged_epochs.end() &&
      !acknowledged->second.root_dir.empty() &&
      acknowledged->second.root_dir != request.root_dir) {
    acknowledged->second.terminal_phase = Terminal_phase::CORRUPT;
    acknowledged->second.terminal_outcome =
        Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
    g_receiver_terminal_cas_conflicts.fetch_add(1);
    return Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
  }

  const auto accepted = m_accepted_epochs.find(request.epoch_id);
  if (accepted != m_accepted_epochs.end()) {
    if (accepted->second.root_dir != request.root_dir ||
        accepted->second.receiver_process_generation !=
            request.receiver_process_generation ||
        accepted->second.terminal_fact_digest != request.fact_digest) {
      g_receiver_terminal_cas_conflicts.fetch_add(1);
      return Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
    }
    g_receiver_terminal_cas_conflicts.fetch_add(1);
    return Preserve_trx_transfer_epoch_terminal_outcome::COMMITTED;
  }

  const bool payload_apply_active =
      m_active_payload_sequences.count(request.epoch_id) != 0 ||
      std::any_of(m_payload_apply_records.begin(),
                  m_payload_apply_records.end(), [&](const auto &item) {
                    return item.first.first == request.epoch_id;
                  });
  if (payload_apply_active) {
    return Preserve_trx_transfer_epoch_terminal_outcome::NOT_COMMITTED;
  }

  const bool receiving = std::any_of(
      m_records.begin(), m_records.end(), [&](const auto &item) {
        return item.first.first == request.epoch_id;
      });
  if (!receiving) {
    const auto online = m_online_epochs.find(request.epoch_id);
    if (online == m_online_epochs.end()) {
      return Preserve_trx_transfer_epoch_terminal_outcome::EPOCH_NOT_FOUND;
    }
    if (online->second.receiver_process_nonce !=
            request.receiver_process_generation ||
        online->second.authenticated_principal !=
            request.authenticated_principal) {
      return Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
    }
  }

  try {
    if (acknowledged == m_acknowledged_epochs.end()) {
      acknowledged =
          m_acknowledged_epochs.emplace(request.epoch_id, Acknowledged_epoch())
              .first;
    }
    Acknowledged_epoch &terminal = acknowledged->second;
    terminal.root_dir = request.root_dir;
    terminal.receiver_process_generation =
        request.receiver_process_generation;
    terminal.receiver_process_nonce = request.receiver_process_generation;
    terminal.authenticated_principal = request.authenticated_principal;
    terminal.terminal_operation_id = request.operation_id;
    terminal.terminal_operation =
        Preserve_trx_transfer_epoch_terminal_operation::ABANDON;
    terminal.terminal_outcome =
        Preserve_trx_transfer_epoch_terminal_outcome::ABANDONING;
    terminal.terminal_phase = Terminal_phase::ABANDONING;
    terminal.fact_digest = request.fact_digest;
    terminal.terminal_cas_monotonic_us = request.now_us;
    terminal.retire_after_us =
        std::max(terminal.retire_after_us, retire_after_us);
  } catch (...) {
    return Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
  }
  g_receiver_terminal_cas_wins.fetch_add(1);
  g_receiver_terminal_status_tombstones.fetch_add(1);
  m_sequence_condition.notify_all();
  return Preserve_trx_transfer_epoch_terminal_outcome::ABANDONING;
}

Preserve_trx_transfer_epoch_terminal_outcome
Preserve_trx_transfer_receiver_registry::complete_epoch_abandon(
    const Preserve_trx_transfer_epoch_terminal_request &request) {
  std::vector<std::shared_ptr<const void>> retired_files;
  if (request.root_dir.empty() ||
      !transfer_component_safe(request.epoch_id) ||
      request.receiver_process_generation.empty() ||
      request.operation_id.empty() ||
      transfer_digest_is_zero(request.fact_digest)) {
    return Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto acknowledged = m_acknowledged_epochs.find(request.epoch_id);
  if (acknowledged == m_acknowledged_epochs.end() ||
      acknowledged->second.terminal_operation ==
          Preserve_trx_transfer_epoch_terminal_operation::NONE) {
    return Preserve_trx_transfer_epoch_terminal_outcome::EPOCH_NOT_FOUND;
  }
  Acknowledged_epoch &current = acknowledged->second;
  if (current.terminal_operation ==
      Preserve_trx_transfer_epoch_terminal_operation::COMMIT) {
    g_receiver_terminal_cas_conflicts.fetch_add(1);
    return current.terminal_outcome;
  }
  const bool same_operation =
      current.root_dir == request.root_dir &&
      current.receiver_process_generation ==
          request.receiver_process_generation &&
      current.authenticated_principal == request.authenticated_principal &&
      current.terminal_operation_id == request.operation_id &&
      current.fact_digest == request.fact_digest;
  if (!same_operation) {
    current.terminal_phase = Terminal_phase::CORRUPT;
    current.terminal_outcome =
        Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
    g_receiver_terminal_cas_conflicts.fetch_add(1);
    return Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
  }
  if (current.terminal_outcome ==
      Preserve_trx_transfer_epoch_terminal_outcome::NOT_COMMITTED_CLEAN) {
    g_receiver_terminal_cas_duplicates.fetch_add(1);
    return current.terminal_outcome;
  }
  if (current.terminal_outcome !=
      Preserve_trx_transfer_epoch_terminal_outcome::ABANDONING) {
    return current.terminal_outcome;
  }
  if (!retain_epoch_resources_locked(request.epoch_id, &retired_files)) {
    return Preserve_trx_transfer_epoch_terminal_outcome::ABANDONING;
  }
  if (retire_epoch_staging_locked(request.epoch_id) != Preserve_trx_transfer_status::OK)
    return Preserve_trx_transfer_epoch_terminal_outcome::ABANDONING;
  current.terminal_outcome =
      Preserve_trx_transfer_epoch_terminal_outcome::NOT_COMMITTED_CLEAN;
  current.terminal_phase = Terminal_phase::NOT_COMMITTED_CLEAN;
  m_live_reserved_by_epoch.erase(request.epoch_id);
  for (auto record = m_records.begin(); record != m_records.end();) {
    record = record->first.first == request.epoch_id
                 ? m_records.erase(record)
                 : std::next(record);
  }
  for (auto object = m_strict_v1_objects.begin();
       object != m_strict_v1_objects.end();) {
    object = object->first.first == request.epoch_id
                 ? m_strict_v1_objects.erase(object)
                 : std::next(object);
  }
  for (auto debt = m_cleanup_debts.begin(); debt != m_cleanup_debts.end();) {
    if (debt->first.first == request.epoch_id) {
      subtract_cleanup_debt_bytes_locked(debt->second.reserved_bytes);
      debt = m_cleanup_debts.erase(debt);
    } else ++debt;
  }
  for (auto frame = m_frame_sequences.begin();
       frame != m_frame_sequences.end();) {
    frame = frame->first.first == request.epoch_id
                ? m_frame_sequences.erase(frame)
                : std::next(frame);
  }
  for (auto apply = m_payload_apply_records.begin();
       apply != m_payload_apply_records.end();) {
    apply = apply->first.first == request.epoch_id
                ? m_payload_apply_records.erase(apply)
                : std::next(apply);
  }
  for (auto queue = m_payload_apply_queue_by_token.begin();
       queue != m_payload_apply_queue_by_token.end();) {
    queue = queue->first.first == request.epoch_id
                ? m_payload_apply_queue_by_token.erase(queue)
                : std::next(queue);
  }
  m_next_sequence_by_epoch.erase(request.epoch_id);
  m_active_payload_sequences.erase(request.epoch_id);
  m_applied_sequence_by_epoch.erase(request.epoch_id);
  m_first_apply_failure_by_epoch.erase(request.epoch_id);
  m_accepted_epochs.erase(request.epoch_id);
  m_sequence_condition.notify_all();
  return current.terminal_outcome;
}

Preserve_trx_transfer_status
  Preserve_trx_transfer_receiver_registry::publish_accepted_epoch(
    const std::string &root_dir,
    std::shared_ptr<const Preserve_trx_transfer_epoch_fact> fact,
    const std::string &receiver_process_generation, uint64_t now_us,
    uint64_t prewarm_timeout_ms, bool flat_projection_published,
    const std::vector<uint64_t> *session_only_tokens,
    const std::array<unsigned char, kPreservedTrxSha256Length>
        *terminal_fact_digest) {
  if (fact == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  const auto &terminal_digest =
      terminal_fact_digest == nullptr ? fact->fact_digest
                                      : *terminal_fact_digest;
  if (root_dir.empty() || !transfer_component_safe(fact->epoch_id) ||
      receiver_process_generation.empty() || fact->source_fence_lsn == 0 ||
      fact->tokens.empty() ||
      transfer_digest_is_zero(fact->fact_digest) ||
      transfer_digest_is_zero(terminal_digest) ||
      prewarm_timeout_ms == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::unordered_set<uint64_t> next_session_only_tokens;
  std::string next_session_only_epoch_id;
  if (session_only_tokens != nullptr) {
    const Preserve_trx_transfer_status set_status =
        build_session_only_token_set(*session_only_tokens,
                                     &next_session_only_tokens);
    if (set_status != Preserve_trx_transfer_status::OK) return set_status;
    try {
      next_session_only_epoch_id = fact->epoch_id;
    } catch (...) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
  }

  Preserve_trx_transfer_accepted_epoch accepted;
  accepted.root_dir = root_dir;
  accepted.epoch_id = fact->epoch_id;
  accepted.receiver_process_generation = receiver_process_generation;
  accepted.source_fence_lsn = fact->source_fence_lsn;
  accepted.fact_digest = fact->fact_digest;
  accepted.terminal_fact_digest = terminal_digest;
  accepted.fact = fact;
  accepted.lifecycle = Preserve_trx_transfer_epoch_lifecycle::PREWARMING;
  accepted.deadline_monotonic_us =
      receiver_epoch_deadline_after_ms(now_us, prewarm_timeout_ms);
  accepted.flat_projection_published = flat_projection_published;
  try {
    accepted.tokens.reserve(fact->tokens.size());
    for (const Preserve_trx_transfer_epoch_fact_token &token : fact->tokens) {
      if (token.token == 0) return Preserve_trx_transfer_status::CORRUPT;
      accepted.tokens.push_back(token.token);
    }
  } catch (...) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  std::sort(accepted.tokens.begin(), accepted.tokens.end());
  if (std::adjacent_find(accepted.tokens.begin(), accepted.tokens.end()) !=
      accepted.tokens.end()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (session_only_tokens != nullptr &&
      std::any_of(accepted.tokens.begin(), accepted.tokens.end(),
                  [&](uint64_t token) {
                    return next_session_only_tokens.count(token) != 0;
                  })) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  std::set<uint64_t> expected_tokens;
  try {
    expected_tokens.insert(accepted.tokens.begin(), accepted.tokens.end());
  } catch (...) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }

  std::lock_guard<std::mutex> guard(m_mutex);
  const auto online = m_online_epochs.find(fact->epoch_id);
  const auto previous = m_accepted_epochs.find(fact->epoch_id);
  if (online != m_online_epochs.end()) {
    accepted.temp_id_contract = online->second.temp_id_contract;
  } else if (previous != m_accepted_epochs.end()) {
    accepted.temp_id_contract = previous->second.temp_id_contract;
  }
  if (previous != m_accepted_epochs.end() &&
      previous->second.temp_id_contract != accepted.temp_id_contract) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  size_t matching_records = 0;
  for (const auto &item : m_records) {
    const Preserve_trx_transfer_receiver_record &record = item.second;
    if (record.epoch_id != fact->epoch_id) continue;
    if (record.state == Preserve_trx_transfer_receiver_state::ABORTED) {
      continue;
    }
    if (record.temp_id_contract != accepted.temp_id_contract) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    if (record.state != Preserve_trx_transfer_receiver_state::RECEIVING &&
        record.state != Preserve_trx_transfer_receiver_state::SAVED_ONLINE) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    if (expected_tokens.count(record.token) == 0) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    for (const Preserve_trx_transfer_object_descriptor &object :
         record.objects) {
      if (record.sealed_objects.count(object.object_id) == 0) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
    }
    ++matching_records;
  }
  if (matching_records != accepted.tokens.size()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  auto acknowledged = m_acknowledged_epochs.find(fact->epoch_id);
  if (acknowledged != m_acknowledged_epochs.end() &&
      !acknowledged->second.root_dir.empty() &&
      acknowledged->second.root_dir != root_dir) {
    g_receiver_terminal_cas_conflicts.fetch_add(1);
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (acknowledged != m_acknowledged_epochs.end() &&
      acknowledged->second.terminal_operation !=
          Preserve_trx_transfer_epoch_terminal_operation::NONE) {
    Acknowledged_epoch &terminal = acknowledged->second;
    if (terminal.terminal_operation ==
            Preserve_trx_transfer_epoch_terminal_operation::COMMIT &&
        terminal.root_dir == root_dir &&
        terminal.receiver_process_generation == receiver_process_generation &&
        terminal.terminal_operation_id == kReceiverCommitOperationId &&
        terminal.fact_digest == terminal_digest &&
        terminal.terminal_outcome ==
            Preserve_trx_transfer_epoch_terminal_outcome::COMMITTED) {
      g_receiver_terminal_cas_duplicates.fetch_add(1);
      return Preserve_trx_transfer_status::OK;
    }
    if (terminal.terminal_operation ==
            Preserve_trx_transfer_epoch_terminal_operation::COMMIT &&
        terminal.terminal_phase == Terminal_phase::COMMIT_ADMITTED &&
        terminal.commit_snapshot_verified) {
      if ((!terminal.receiver_process_generation.empty() &&
           terminal.receiver_process_generation !=
               receiver_process_generation) ||
          (!transfer_digest_is_zero(terminal.fact_digest) &&
           terminal.fact_digest != terminal_digest)) {
        terminal.terminal_phase = Terminal_phase::CORRUPT;
        terminal.terminal_outcome =
            Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
        g_receiver_terminal_cas_conflicts.fetch_add(1);
        return Preserve_trx_transfer_status::CORRUPT;
      }
      /* Complete the exact COMMIT admission below. */
    } else if (terminal.terminal_operation ==
                   Preserve_trx_transfer_epoch_terminal_operation::COMMIT &&
               terminal.terminal_phase ==
                   Terminal_phase::COMMIT_ADMITTED) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    } else if (terminal.terminal_operation ==
                   Preserve_trx_transfer_epoch_terminal_operation::COMMIT &&
               terminal.fact_digest != terminal_digest) {
      terminal.terminal_phase = Terminal_phase::CORRUPT;
      terminal.terminal_outcome =
          Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
      const auto current = m_accepted_epochs.find(fact->epoch_id);
      if (current != m_accepted_epochs.end()) {
        current->second.lifecycle =
            Preserve_trx_transfer_epoch_lifecycle::ABANDONING;
      }
      g_receiver_terminal_cas_conflicts.fetch_add(1);
      return Preserve_trx_transfer_status::CORRUPT;
    } else {
      g_receiver_terminal_cas_conflicts.fetch_add(1);
      return terminal.terminal_outcome ==
                     Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT
                 ? Preserve_trx_transfer_status::CORRUPT
                 : Preserve_trx_transfer_status::UNSUPPORTED;
    }
  } else {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  const auto existing = m_accepted_epochs.find(fact->epoch_id);
  if (existing != m_accepted_epochs.end()) {
    const Preserve_trx_transfer_accepted_epoch &current = existing->second;
    if (current.root_dir != accepted.root_dir ||
        current.receiver_process_generation !=
            accepted.receiver_process_generation ||
        current.source_fence_lsn != accepted.source_fence_lsn ||
        current.tokens != accepted.tokens ||
        current.fact_digest != accepted.fact_digest ||
        current.terminal_fact_digest != accepted.terminal_fact_digest ||
        current.flat_projection_published !=
            accepted.flat_projection_published) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }

  bool accepted_inserted = false;
  try {
    if (existing == m_accepted_epochs.end()) {
      m_accepted_epochs.emplace(fact->epoch_id, std::move(accepted));
      accepted_inserted = true;
    }
    if (acknowledged == m_acknowledged_epochs.end()) {
      acknowledged =
          m_acknowledged_epochs.emplace(fact->epoch_id, Acknowledged_epoch())
              .first;
    }
    Acknowledged_epoch &terminal = acknowledged->second;
    terminal.root_dir = root_dir;
    terminal.receiver_process_generation = receiver_process_generation;
    if (online != m_online_epochs.end()) {
      terminal.receiver_process_nonce =
          online->second.receiver_process_nonce;
      terminal.authenticated_principal =
          online->second.authenticated_principal;
    }
    terminal.terminal_operation_id = kReceiverCommitOperationId;
    terminal.terminal_operation =
        Preserve_trx_transfer_epoch_terminal_operation::COMMIT;
    terminal.terminal_outcome =
        Preserve_trx_transfer_epoch_terminal_outcome::COMMITTED;
    terminal.terminal_phase = Terminal_phase::COMMITTED;
    terminal.fact_digest = terminal_digest;
    terminal.terminal_cas_monotonic_us = now_us;
    const uint64_t retention_us =
        online == m_online_epochs.end()
            ? kReceiverTerminalStatusRetentionUs
            : online->second.accepted_terminal_status_retention_us;
    terminal.retire_after_us =
        std::max(terminal.retire_after_us,
                 now_us > std::numeric_limits<uint64_t>::max() - retention_us
                     ? std::numeric_limits<uint64_t>::max()
                     : now_us + retention_us);
    publish_active_session_only_set(
        session_only_tokens == nullptr ? nullptr
                                       : &next_session_only_epoch_id,
        session_only_tokens == nullptr ? nullptr : &next_session_only_tokens);
  } catch (...) {
    if (accepted_inserted) m_accepted_epochs.erase(fact->epoch_id);
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::publish_control_only_epoch(
    const std::string &root_dir, const std::string &epoch_id,
    uint64_t sequence,
    const std::array<unsigned char, kPreservedTrxSha256Length> &frame_digest,
    const std::array<unsigned char, kPreservedTrxSha256Length> &fact_digest,
    uint64_t now_us,
    const std::vector<uint64_t> &session_only_tokens) {
  if (root_dir.empty() || !transfer_component_safe(epoch_id) ||
      sequence == 0 || transfer_digest_is_zero(frame_digest) ||
      transfer_digest_is_zero(fact_digest) || now_us == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
#ifndef DBUG_OFF
  bool inject_resource_exhausted = false;
  DBUG_EXECUTE_IF(
      "preserve_trx_receiver_control_only_publish_resource_exhausted_once", {
        inject_resource_exhausted =
            !m_debug_control_only_resource_exhausted_consumed.exchange(true);
      });
  if (inject_resource_exhausted) {
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: debug injected control-only publish "
            "RESOURCE_EXHAUSTED epoch=" +
            epoch_id)
               .c_str());
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
#endif
  std::unordered_set<uint64_t> next_session_only_tokens;
  Preserve_trx_transfer_status status = build_session_only_token_set(
      session_only_tokens, &next_session_only_tokens);
  if (status != Preserve_trx_transfer_status::OK) return status;
  std::string next_session_only_epoch_id;
  try {
    next_session_only_epoch_id = epoch_id;
  } catch (...) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }

  std::lock_guard<std::mutex> guard(m_mutex);
  const auto online = m_online_epochs.find(epoch_id);
  const auto acknowledged = m_acknowledged_epochs.find(epoch_id);
  if (online == m_online_epochs.end() ||
      acknowledged == m_acknowledged_epochs.end() ||
      m_accepted_epochs.count(epoch_id) != 0) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  Acknowledged_epoch &terminal = acknowledged->second;
  if (terminal.terminal_operation !=
          Preserve_trx_transfer_epoch_terminal_operation::COMMIT ||
      terminal.terminal_phase != Terminal_phase::COMMIT_ADMITTED ||
      terminal.admitted_commit_sequence != sequence ||
      terminal.admitted_commit_frame_digest != frame_digest ||
      terminal.root_dir != root_dir || terminal.fact_digest != fact_digest ||
      terminal.receiver_process_generation !=
          online->second.receiver_process_nonce ||
      !terminal.commit_snapshot_verified) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  for (const auto &item : m_records) {
    if (item.second.epoch_id == epoch_id &&
        item.second.state != Preserve_trx_transfer_receiver_state::ABORTED) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }

  terminal.terminal_outcome =
      Preserve_trx_transfer_epoch_terminal_outcome::COMMITTED;
  terminal.terminal_phase = Terminal_phase::COMMITTED;
  terminal.terminal_cas_monotonic_us = now_us;
  publish_active_session_only_set(&next_session_only_epoch_id,
                                  &next_session_only_tokens);
  return Preserve_trx_transfer_status::OK;
}

void Preserve_trx_transfer_receiver_registry::publish_active_session_only_set(
    std::string *epoch_id,
    std::unordered_set<uint64_t> *session_only_tokens) {
  std::unordered_set<uint64_t> retired_tokens;
  std::string retired_epoch_id;
  {
    std::lock_guard<std::mutex> guard(m_session_only_mutex);
    retired_tokens.swap(m_active_session_only_tokens);
    retired_epoch_id.swap(m_active_session_only_epoch_id);
    if (session_only_tokens == nullptr) {
      m_active_session_only_set_valid = false;
    } else {
      assert(epoch_id != nullptr && !epoch_id->empty());
      m_active_session_only_tokens.swap(*session_only_tokens);
      m_active_session_only_epoch_id.swap(*epoch_id);
      m_active_session_only_set_valid = true;
    }
    ++m_active_session_only_generation;
  }
}

Preserve_trx_transfer_resume_handoff
Preserve_trx_transfer_receiver_registry::begin_resume_handoff(
    uint64_t token, bool transaction_candidate) {
  Preserve_trx_transfer_resume_handoff handoff;
  handoff.token = token;
  std::lock_guard<std::mutex> guard(m_session_only_mutex);
  try {
    if (m_active_session_only_set_valid) {
      handoff.epoch_id = m_active_session_only_epoch_id;
      handoff.available = true;
      handoff.session_only =
          token != 0 && m_active_session_only_tokens.count(token) != 0;
      handoff.generation = m_active_session_only_generation;
    }
    if (token == 0 || !transaction_candidate || handoff.session_only) {
      return handoff;
    }
    for (const auto &claim : m_transaction_handoff_claims) {
      if (claim.second == token) return handoff;
    }
    handoff.transaction_claimed =
        m_transaction_handoff_claims.insert({handoff.generation, token})
            .second;
  } catch (...) {
    handoff.available = false;
    handoff.session_only = false;
    handoff.transaction_claimed = false;
    handoff.epoch_id.clear();
    handoff.generation = 0;
  }
  return handoff;
}

bool Preserve_trx_transfer_receiver_registry::consume_session_only_token(
    uint64_t token, const std::string &epoch_id, uint64_t generation) {
  if (token == 0 || epoch_id.empty() || generation == 0) return false;

  std::lock_guard<std::mutex> guard(m_session_only_mutex);
  if (!m_active_session_only_set_valid ||
      epoch_id != m_active_session_only_epoch_id ||
      generation != m_active_session_only_generation) {
    return false;
  }
  for (const auto &claim : m_transaction_handoff_claims) {
    if (claim.second == token) return false;
  }
  const auto found = m_active_session_only_tokens.find(token);
  if (found == m_active_session_only_tokens.end()) return false;
  m_active_session_only_tokens.erase(found);
  return true;
}

void Preserve_trx_transfer_receiver_registry::release_transaction_handoff(
    uint64_t token, uint64_t generation) {
  if (token == 0) return;
  std::lock_guard<std::mutex> guard(m_session_only_mutex);
  m_transaction_handoff_claims.erase({generation, token});
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::query_accepted_epoch(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_accepted_epoch *accepted) const {
  if (root_dir.empty() || !transfer_component_safe(epoch_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_accepted_epochs.find(epoch_id);
  if (found == m_accepted_epochs.end() || found->second.root_dir != root_dir ||
      found->second.lifecycle ==
          Preserve_trx_transfer_epoch_lifecycle::ABANDONING ||
      found->second.lifecycle ==
          Preserve_trx_transfer_epoch_lifecycle::EXPIRED) {
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  if (accepted != nullptr) *accepted = found->second;
  return Preserve_trx_transfer_status::COMMITTED_NOT_READY;
}

bool Preserve_trx_transfer_receiver_registry::accepted_epoch_is_live(
    const std::string &root_dir, const std::string &epoch_id) const {
  if (root_dir.empty() || !transfer_component_safe(epoch_id)) return false;
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_accepted_epochs.find(epoch_id);
  return found != m_accepted_epochs.end() &&
         found->second.root_dir == root_dir &&
         found->second.lifecycle !=
             Preserve_trx_transfer_epoch_lifecycle::ABANDONING &&
         found->second.lifecycle !=
             Preserve_trx_transfer_epoch_lifecycle::EXPIRED;
}

bool Preserve_trx_transfer_receiver_registry::accepted_epoch_is_expired(
    const std::string &root_dir, const std::string &epoch_id) const {
  if (root_dir.empty() || !transfer_component_safe(epoch_id)) return false;
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_accepted_epochs.find(epoch_id);
  return found != m_accepted_epochs.end() &&
         found->second.root_dir == root_dir &&
         found->second.lifecycle ==
             Preserve_trx_transfer_epoch_lifecycle::EXPIRED;
}

bool Preserve_trx_transfer_receiver_registry::accepted_epoch_rejects_token(
    const std::string &root_dir, const std::string &epoch_id,
    uint64_t token) const {
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_accepted_epochs.find(epoch_id);
  if (found == m_accepted_epochs.end() || found->second.root_dir != root_dir ||
      !found->second.selection_published) return false;
  const auto &ready = found->second.ready_tokens;
  return !std::binary_search(ready.begin(), ready.end(), token);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_accepted_epoch_ready(
    const std::string &root_dir, const std::string &epoch_id, uint64_t now_us,
    uint64_t ready_deadline_monotonic_us) {
  Preserve_trx_transfer_accepted_epoch accepted;
  const auto status =
      query_accepted_epoch(root_dir, epoch_id, &accepted);
  if (status != Preserve_trx_transfer_status::COMMITTED_NOT_READY) {
    return status;
  }
  if (accepted.lifecycle == Preserve_trx_transfer_epoch_lifecycle::READY) {
    return Preserve_trx_transfer_status::OK;
  }
  if (accepted.deadline_monotonic_us <= now_us) {
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  return publish_accepted_epoch_selection(
      root_dir, epoch_id, now_us, ready_deadline_monotonic_us,
      std::move(accepted.tokens), {});
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::publish_accepted_epoch_selection(
    const std::string &root_dir, const std::string &epoch_id, uint64_t now_us,
    uint64_t ready_deadline_monotonic_us,
    std::vector<uint64_t> ready_tokens,
    std::vector<Preserve_trx_receiver_failed_token> failed_tokens) {
  if (root_dir.empty() || !transfer_component_safe(epoch_id) ||
      ready_deadline_monotonic_us <= now_us) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_accepted_epochs.find(epoch_id);
  if (found == m_accepted_epochs.end() || found->second.root_dir != root_dir) {
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  Preserve_trx_transfer_accepted_epoch &accepted = found->second;
  if (accepted.selection_published) return Preserve_trx_transfer_status::OK;
  if (accepted.lifecycle !=
      Preserve_trx_transfer_epoch_lifecycle::PREWARMING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (ready_tokens.size() + failed_tokens.size() != accepted.tokens.size()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  size_t ready_index = 0;
  size_t failed_index = 0;
  for (uint64_t token : accepted.tokens) {
    const bool ready = ready_index < ready_tokens.size() &&
                       ready_tokens[ready_index] == token;
    const bool failed = failed_index < failed_tokens.size() &&
                        failed_tokens[failed_index].token == token;
    if (ready == failed ||
        (failed && failed_tokens[failed_index].reason ==
                       Preserve_trx_receiver_failure_reason::NONE)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    ready_index += ready ? 1 : 0;
    failed_index += failed ? 1 : 0;
  }
  if (ready_index != ready_tokens.size() ||
      failed_index != failed_tokens.size()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  accepted.ready_tokens = std::move(ready_tokens);
  accepted.failed_tokens = std::move(failed_tokens);
  accepted.selection_published = true;
  accepted.deadline_monotonic_us = ready_deadline_monotonic_us;
  accepted.lifecycle = accepted.failed_tokens.empty()
                           ? Preserve_trx_transfer_epoch_lifecycle::READY
                           : Preserve_trx_transfer_epoch_lifecycle::CLASSIFIED;
  return Preserve_trx_transfer_status::OK;
}

std::vector<std::pair<std::string, std::string>>
Preserve_trx_transfer_receiver_registry::prewarming_epochs_due(
    uint64_t now_us) const {
  std::vector<std::pair<std::string, std::string>> due;
  std::lock_guard<std::mutex> guard(m_mutex);
  try {
    for (const auto &item : m_accepted_epochs) {
      if (item.second.lifecycle ==
              Preserve_trx_transfer_epoch_lifecycle::PREWARMING &&
          item.second.deadline_monotonic_us != 0 &&
          item.second.deadline_monotonic_us <= now_us) {
        due.emplace_back(item.second.root_dir, item.first);
      }
    }
  } catch (const std::bad_alloc &) {
    due.clear();
  }
  return due;
}

Preserve_trx_receiver_promotion_lease_status
Preserve_trx_transfer_receiver_registry::
    try_acquire_accepted_epoch_promotion_lease(
        const std::string &root_dir, const std::string &epoch_id,
        uint64_t now_us,
        const std::string &expected_receiver_process_generation,
        Preserve_trx_transfer_accepted_epoch *accepted_snapshot) {
  if (root_dir.empty() || !transfer_component_safe(epoch_id) ||
      expected_receiver_process_generation.empty() ||
      accepted_snapshot == nullptr) {
    return Preserve_trx_receiver_promotion_lease_status::
        NOT_FOUND_OR_EXPIRED;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_accepted_epochs.find(epoch_id);
  if (found == m_accepted_epochs.end() || found->second.root_dir != root_dir) {
    return Preserve_trx_receiver_promotion_lease_status::
        NOT_FOUND_OR_EXPIRED;
  }
  Preserve_trx_transfer_accepted_epoch &accepted = found->second;
  if (accepted.lifecycle ==
          Preserve_trx_transfer_epoch_lifecycle::ABANDONING ||
      accepted.lifecycle == Preserve_trx_transfer_epoch_lifecycle::EXPIRED) {
    return Preserve_trx_receiver_promotion_lease_status::
        NOT_FOUND_OR_EXPIRED;
  }
  if (accepted.receiver_process_generation !=
          expected_receiver_process_generation ||
      accepted.lifecycle ==
          Preserve_trx_transfer_epoch_lifecycle::ADOPT_LEASED) {
    return Preserve_trx_receiver_promotion_lease_status::NOT_READY;
  }
  if (accepted.deadline_monotonic_us <= now_us) {
    accepted.lifecycle = Preserve_trx_transfer_epoch_lifecycle::EXPIRED;
    ++m_expired_epoch_count;
    return Preserve_trx_receiver_promotion_lease_status::
        NOT_FOUND_OR_EXPIRED;
  }
  const bool classified_partition =
      accepted.lifecycle == Preserve_trx_transfer_epoch_lifecycle::CLASSIFIED &&
      accepted.selection_published;
  if (accepted.lifecycle != Preserve_trx_transfer_epoch_lifecycle::READY &&
      !classified_partition) {
    return Preserve_trx_receiver_promotion_lease_status::NOT_READY;
  }
  try {
    *accepted_snapshot = accepted;
  } catch (const std::bad_alloc &) {
    return Preserve_trx_receiver_promotion_lease_status::NOT_READY;
  }
  accepted.lifecycle = Preserve_trx_transfer_epoch_lifecycle::ADOPT_LEASED;
  accepted_snapshot->lifecycle =
      Preserve_trx_transfer_epoch_lifecycle::ADOPT_LEASED;
  return Preserve_trx_receiver_promotion_lease_status::ACQUIRED;
}

Preserve_trx_transfer_status
  Preserve_trx_transfer_receiver_registry::
    abandon_accepted_epoch_promotion_lease(
        const Preserve_trx_transfer_accepted_epoch &accepted) {
  if (accepted.root_dir.empty() || accepted.epoch_id.empty() ||
      accepted.receiver_process_generation.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_accepted_epochs.find(accepted.epoch_id);
  if (found == m_accepted_epochs.end()) {
    return Preserve_trx_transfer_status::OK;
  }
  if (found->second.root_dir != accepted.root_dir ||
      found->second.receiver_process_generation !=
          accepted.receiver_process_generation) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (found->second.lifecycle ==
      Preserve_trx_transfer_epoch_lifecycle::ABANDONING) {
    return Preserve_trx_transfer_status::OK;
  }
  if (found->second.lifecycle !=
      Preserve_trx_transfer_epoch_lifecycle::ADOPT_LEASED) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  found->second.lifecycle =
      Preserve_trx_transfer_epoch_lifecycle::ABANDONING;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::
    complete_accepted_epoch_promotion_lease(
        const Preserve_trx_transfer_accepted_epoch &accepted) {
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_accepted_epochs.find(accepted.epoch_id);
  if (found == m_accepted_epochs.end() ||
      found->second.root_dir != accepted.root_dir ||
      found->second.receiver_process_generation !=
          accepted.receiver_process_generation ||
      found->second.lifecycle !=
          Preserve_trx_transfer_epoch_lifecycle::ADOPT_LEASED) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  m_accepted_epochs.erase(found);
  return Preserve_trx_transfer_status::OK;
}

size_t Preserve_trx_transfer_receiver_registry::expire_accepted_epochs_once(
    uint64_t now_us,
    std::vector<Preserve_trx_transfer_accepted_epoch> *expired) {
  if (expired == nullptr) return 0;
  expired->clear();
  std::lock_guard<std::mutex> guard(m_mutex);
  for (auto &item : m_accepted_epochs) {
    Preserve_trx_transfer_accepted_epoch &accepted = item.second;
    if (accepted.lifecycle ==
        Preserve_trx_transfer_epoch_lifecycle::ADOPT_LEASED) {
      continue;
    }
    if (accepted.lifecycle ==
        Preserve_trx_transfer_epoch_lifecycle::ABANDONING) {
      expired->push_back(accepted);
      continue;
    }
    if (accepted.lifecycle != Preserve_trx_transfer_epoch_lifecycle::EXPIRED) {
      if (accepted.deadline_monotonic_us == 0 ||
          accepted.deadline_monotonic_us > now_us) {
        continue;
      }
      accepted.lifecycle = Preserve_trx_transfer_epoch_lifecycle::EXPIRED;
      ++m_expired_epoch_count;
    }
    expired->push_back(accepted);
  }
  return expired->size();
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::erase_expired_epoch(
    const std::string &root_dir, const std::string &epoch_id) {
  return erase_cleaned_epoch(root_dir, epoch_id,
                             Preserve_trx_transfer_epoch_lifecycle::EXPIRED);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::erase_abandoning_epoch(
    const std::string &root_dir, const std::string &epoch_id) {
  return erase_cleaned_epoch(
      root_dir, epoch_id,
      Preserve_trx_transfer_epoch_lifecycle::ABANDONING);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::erase_cleaned_epoch(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_epoch_lifecycle expected_lifecycle) {
  std::vector<std::shared_ptr<const void>> retired_files;
  if (root_dir.empty() || !transfer_component_safe(epoch_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto accepted = m_accepted_epochs.find(epoch_id);
    if (accepted == m_accepted_epochs.end()) {
      return Preserve_trx_transfer_status::OK;
    }
    if (accepted->second.root_dir != root_dir ||
        accepted->second.lifecycle != expected_lifecycle) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    if (!retain_epoch_resources_locked(epoch_id, &retired_files)) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    const auto retirement_status = retire_epoch_staging_locked(epoch_id);
    if (retirement_status != Preserve_trx_transfer_status::OK) return retirement_status;

    m_live_reserved_by_epoch.erase(epoch_id);
    for (auto record = m_records.begin(); record != m_records.end();) {
      record = record->first.first == epoch_id ? m_records.erase(record)
                                               : std::next(record);
    }
    for (auto object = m_strict_v1_objects.begin();
         object != m_strict_v1_objects.end();) {
      object = object->first.first == epoch_id
                   ? m_strict_v1_objects.erase(object)
                   : std::next(object);
    }
    for (auto debt = m_cleanup_debts.begin(); debt != m_cleanup_debts.end();) {
      if (debt->first.first == epoch_id) {
        subtract_cleanup_debt_bytes_locked(debt->second.reserved_bytes);
        debt = m_cleanup_debts.erase(debt);
      } else ++debt;
    }
    for (auto frame = m_frame_sequences.begin();
         frame != m_frame_sequences.end();) {
      frame = frame->first.first == epoch_id
                  ? m_frame_sequences.erase(frame)
                  : std::next(frame);
    }
    for (auto apply = m_payload_apply_records.begin();
         apply != m_payload_apply_records.end();) {
      apply = apply->first.first == epoch_id
                  ? m_payload_apply_records.erase(apply)
                  : std::next(apply);
    }
    for (auto queue = m_payload_apply_queue_by_token.begin();
         queue != m_payload_apply_queue_by_token.end();) {
      queue = queue->first.first == epoch_id
                  ? m_payload_apply_queue_by_token.erase(queue)
                  : std::next(queue);
    }
    m_next_sequence_by_epoch.erase(epoch_id);
    m_active_payload_sequences.erase(epoch_id);
    m_applied_sequence_by_epoch.erase(epoch_id);
    m_first_apply_failure_by_epoch.erase(epoch_id);
    m_online_epochs.erase(epoch_id);
    m_accepted_epochs.erase(accepted);
  }
  m_sequence_condition.notify_all();
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::acknowledge_epoch(
    const std::string &root_dir, const std::string &epoch_id, uint64_t now_us,
    uint64_t grace_us) {
  if (root_dir.empty() || !transfer_component_safe(epoch_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const uint64_t retire_after_us =
      now_us > std::numeric_limits<uint64_t>::max() - grace_us
          ? std::numeric_limits<uint64_t>::max()
          : now_us + grace_us;
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    try {
      auto acknowledged = m_acknowledged_epochs.find(epoch_id);
      if (acknowledged == m_acknowledged_epochs.end()) {
        acknowledged =
            m_acknowledged_epochs.emplace(epoch_id, Acknowledged_epoch()).first;
        acknowledged->second.root_dir = root_dir;
      } else if (acknowledged->second.root_dir != root_dir) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      Acknowledged_epoch &ack = acknowledged->second;
      ack.retire_after_us = std::max(ack.retire_after_us, retire_after_us);
    } catch (...) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
  }
  return Preserve_trx_transfer_status::OK;
}

size_t
Preserve_trx_transfer_receiver_registry::retire_acknowledged_epochs_once(
    uint64_t now_us) {
  std::vector<std::pair<std::string, Acknowledged_epoch>> acknowledged;
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    for (const auto &item : m_acknowledged_epochs) {
      acknowledged.push_back(item);
    }
  }

  size_t retired = 0;
  for (auto &item : acknowledged) {
    std::vector<std::shared_ptr<const void>> retired_files;
    std::lock_guard<std::mutex> guard(m_mutex);
    auto ack = m_acknowledged_epochs.find(item.first);
    if (ack == m_acknowledged_epochs.end()) continue;
    if (ack->second.terminal_phase == Terminal_phase::COMMIT_ADMITTED) {
      continue;
    }
    if (ack->second.retire_after_us > now_us) {
      continue;
    }
    if (m_accepted_epochs.count(item.first) != 0) {
      if (ack->second.terminal_operation !=
          Preserve_trx_transfer_epoch_terminal_operation::NONE) {
        g_receiver_terminal_status_tombstones.fetch_sub(1);
        g_receiver_terminal_status_tombstone_expiries.fetch_add(1);
      }
      m_online_epochs.erase(item.first);
      m_acknowledged_epochs.erase(ack);
      ++retired;
      continue;
    }

    bool terminal = true;
    for (const auto &record : m_records) {
      if (record.first.first != item.first) continue;
      if (record.second.reserved_bytes != 0) {
        terminal = false;
        break;
      }
      if (record.second.state !=
              Preserve_trx_transfer_receiver_state::SAVED_ONLINE &&
          record.second.state != Preserve_trx_transfer_receiver_state::CORRUPT &&
          record.second.state != Preserve_trx_transfer_receiver_state::ABORTED) {
        terminal = false;
        break;
      }
    }
    if (!terminal) continue;
    const bool has_debt =
        std::any_of(m_cleanup_debts.begin(), m_cleanup_debts.end(),
                    [&](const auto &debt) {
                      return debt.first.first == item.first;
                    });
    const auto staging = m_staging_by_token.lower_bound(Token_key(item.first, 0));
    if (has_debt || (staging != m_staging_by_token.end() &&
                    staging->first.first == item.first)) continue;
    if (!retain_epoch_resources_locked(item.first, &retired_files)) continue;

    m_live_reserved_by_epoch.erase(item.first);
    for (auto record = m_records.begin(); record != m_records.end();) {
      record = record->first.first == item.first ? m_records.erase(record)
                                                 : std::next(record);
    }
    for (auto object = m_strict_v1_objects.begin();
         object != m_strict_v1_objects.end();) {
      object = object->first.first == item.first
                   ? m_strict_v1_objects.erase(object)
                   : std::next(object);
    }
    for (auto frame = m_frame_sequences.begin();
         frame != m_frame_sequences.end();) {
      frame = frame->first.first == item.first
                  ? m_frame_sequences.erase(frame)
                  : std::next(frame);
    }
    for (auto apply = m_payload_apply_records.begin();
         apply != m_payload_apply_records.end();) {
      apply = apply->first.first == item.first
                  ? m_payload_apply_records.erase(apply)
                  : std::next(apply);
    }
    for (auto queue = m_payload_apply_queue_by_token.begin();
         queue != m_payload_apply_queue_by_token.end();) {
      queue = queue->first.first == item.first
                  ? m_payload_apply_queue_by_token.erase(queue)
                  : std::next(queue);
    }
    m_next_sequence_by_epoch.erase(item.first);
    m_applied_sequence_by_epoch.erase(item.first);
    m_first_apply_failure_by_epoch.erase(item.first);
    m_active_payload_sequences.erase(item.first);
    if (ack->second.terminal_operation !=
        Preserve_trx_transfer_epoch_terminal_operation::NONE) {
      g_receiver_terminal_status_tombstones.fetch_sub(1);
      g_receiver_terminal_status_tombstone_expiries.fetch_add(1);
    }
    m_online_epochs.erase(item.first);
    m_acknowledged_epochs.erase(ack);
    ++retired;
  }
  return retired;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_object_sealed(
    const std::string &epoch_id, uint64_t token,
    const std::string &object_id,
    std::shared_ptr<const Preserve_trx_sealed_file> file) {
  std::shared_ptr<const Preserve_trx_sealed_file> retired_file;
  std::lock_guard<std::mutex> guard(m_mutex);
  const Token_key key(epoch_id, token);
  auto found = m_records.find(key);
  if (found == m_records.end()) return Preserve_trx_transfer_status::CORRUPT;
  if (found->second.state != Preserve_trx_transfer_receiver_state::DECLARED &&
      found->second.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  const auto *object_found = found->second.object_index
      ? found->second.object_index->find(found->second.objects, object_id) : nullptr;
  if (object_found == nullptr) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (file != nullptr) {
    if (!file->matches(object_found->total_size, object_found->digest)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    // Keep one verified owner for each immutable generation. Repeated SEAL
    // must not strand readers on another control block for this same inode.
    const auto sealed = found->second.sealed_files.find(object_id);
    if (sealed != found->second.sealed_files.end() &&
        sealed->second->matches(object_found->total_size, object_found->digest)) {
      found->second.sealed_objects.insert(object_id);
      return Preserve_trx_transfer_status::OK;
    }
    // Publish the canonical pin before optional workers can borrow it. A
    // failed map allocation must not leave an unaccounted candidate reader.
    const auto old = found->second.sealed_files.find(object_id);
    if (old != found->second.sealed_files.end()) retired_file = old->second;
    found->second.sealed_files[object_id] = file;
    const auto ticket = found->second.staging_tickets.find(object_id);
    if (ticket != found->second.staging_tickets.end())
      m_retired_staging.find(ticket->second)->second.file = file;
    const bool temp_undo = transfer_is_temp_undo_object(*object_found);
    const bool temp_candidate = preserve_trx_temp_candidate_object(*object_found);
    if ((object_found->kind == Preserve_trx_transfer_object_kind::CURSOR_RESULT ||
         temp_undo || temp_candidate) &&
        !found->second.resource_candidates) {
      found->second.resource_candidates =
          Preserve_trx_receiver_candidates::create(std::to_string(token));
      // Optional early preparation may run out of memory. The verified file
      // remains usable by the required final preparation under its own budget.
    }
    if (temp_candidate && found->second.resource_candidates)
      found->second.resource_candidates->register_temp(object_id, file);
    if (temp_undo && found->second.resource_candidates) {
      std::string base_id;
      std::shared_ptr<const Preserve_trx_sealed_file> base;
      if (preserve_trx_temp_undo_delta_id(object_id, &base_id)) {
        const auto entry = found->second.sealed_files.find(base_id);
        if (entry != found->second.sealed_files.end()) base = entry->second;
      }
      found->second.resource_candidates->register_undo(object_id, file, std::move(base));
    }
  } else if (object_found->kind ==
                 Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR ||
             object_found->kind == Preserve_trx_transfer_object_kind::CURSOR_RESULT) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  found->second.sealed_objects.insert(object_id);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::begin_payload_sequence(
    const std::string &epoch_id, uint64_t first_sequence,
    uint64_t last_sequence, uint64_t timeout_ms) {
  if (epoch_id.empty() || first_sequence == 0 ||
      first_sequence == std::numeric_limits<uint64_t>::max() ||
      last_sequence < first_sequence || timeout_ms == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::unique_lock<std::mutex> guard(m_mutex);
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
  for (;;) {
    const auto terminal = m_acknowledged_epochs.find(epoch_id);
    if (terminal != m_acknowledged_epochs.end() &&
        (terminal->second.terminal_phase == Terminal_phase::ABANDONING ||
         terminal->second.terminal_phase ==
             Terminal_phase::NOT_COMMITTED_CLEAN)) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    const auto expected_it = m_next_sequence_by_epoch.find(epoch_id);
    const uint64_t expected = expected_it == m_next_sequence_by_epoch.end()
                                  ? 1
                                  : expected_it->second;
    if (m_active_payload_sequences.count(epoch_id) == 0 &&
        first_sequence <= expected) {
      m_active_payload_sequences.insert(epoch_id);
      return Preserve_trx_transfer_status::OK;
    }
    if (m_sequence_condition.wait_until(guard, deadline) ==
        std::cv_status::timeout) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
  }
}

void Preserve_trx_transfer_receiver_registry::end_payload_sequence(
    const std::string &epoch_id) {
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_active_payload_sequences.erase(epoch_id);
  }
  m_sequence_condition.notify_all();
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::reserve_payload_apply(
    const std::string &epoch_id, uint64_t first_sequence,
    uint64_t last_sequence, const std::vector<uint64_t> &tokens,
    Preserve_trx_transfer_payload_apply_reservation *reservation) {
  if (epoch_id.empty() || first_sequence == 0 ||
      last_sequence < first_sequence || tokens.empty() ||
      reservation == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  Preserve_trx_transfer_payload_apply_reservation built;
  std::vector<uint64_t> sorted_tokens;
  try {
    built.epoch_id = epoch_id;
    built.first_sequence = first_sequence;
    built.last_sequence = last_sequence;
    sorted_tokens = tokens;
    std::sort(sorted_tokens.begin(), sorted_tokens.end());
    sorted_tokens.erase(
        std::unique(sorted_tokens.begin(), sorted_tokens.end()),
        sorted_tokens.end());
  } catch (...) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  if (sorted_tokens.front() == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::lock_guard<std::mutex> guard(m_mutex);
  const auto key = std::make_pair(epoch_id, first_sequence);
  const auto existing = m_payload_apply_records.find(key);
  if (existing != m_payload_apply_records.end()) {
    if (existing->second.last_sequence != last_sequence ||
        existing->second.tokens != sorted_tokens) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    *reservation = std::move(built);
    return Preserve_trx_transfer_status::OK;
  }

  try {
    Payload_apply_record record;
    record.last_sequence = last_sequence;
    record.tokens = sorted_tokens;
    m_payload_apply_records.emplace(key, std::move(record));
    for (uint64_t token : sorted_tokens) {
      m_payload_apply_queue_by_token[Token_key(epoch_id, token)].push_back(
          first_sequence);
    }
  } catch (...) {
    m_payload_apply_records.erase(key);
    for (uint64_t token : sorted_tokens) {
      const auto queue =
          m_payload_apply_queue_by_token.find(Token_key(epoch_id, token));
      if (queue == m_payload_apply_queue_by_token.end()) continue;
      if (!queue->second.empty() &&
          queue->second.back() == first_sequence) {
        queue->second.pop_back();
      }
      if (queue->second.empty()) m_payload_apply_queue_by_token.erase(queue);
    }
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  *reservation = std::move(built);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::wait_for_payload_apply_turn(
    const Preserve_trx_transfer_payload_apply_reservation &reservation,
    uint64_t timeout_ms, bool *apply_owner) {
  if (reservation.epoch_id.empty() || reservation.first_sequence == 0 ||
      reservation.last_sequence < reservation.first_sequence ||
      timeout_ms == 0 || apply_owner == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  *apply_owner = false;

  std::unique_lock<std::mutex> guard(m_mutex);
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
  const auto key =
      std::make_pair(reservation.epoch_id, reservation.first_sequence);
  for (;;) {
    auto record = m_payload_apply_records.find(key);
    if (record == m_payload_apply_records.end()) {
      return Preserve_trx_transfer_status::OK;
    }
    if (record->second.last_sequence != reservation.last_sequence) {
      return Preserve_trx_transfer_status::CORRUPT;
    }

    bool ready = !record->second.applying;
    for (uint64_t token : record->second.tokens) {
      const Token_key token_key(reservation.epoch_id, token);
      const auto queue = m_payload_apply_queue_by_token.find(token_key);
      if (queue == m_payload_apply_queue_by_token.end() ||
          queue->second.empty() ||
          queue->second.front() != reservation.first_sequence) {
        ready = false;
        break;
      }
    }
    if (ready) {
      record->second.applying = true;
      *apply_owner = true;
      return Preserve_trx_transfer_status::OK;
    }
    if (m_sequence_condition.wait_until(guard, deadline) ==
        std::cv_status::timeout) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
  }
}

void Preserve_trx_transfer_receiver_registry::finish_payload_apply(
    const Preserve_trx_transfer_payload_apply_reservation &reservation) {
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto key =
        std::make_pair(reservation.epoch_id, reservation.first_sequence);
    const auto record = m_payload_apply_records.find(key);
    if (record == m_payload_apply_records.end() ||
        record->second.last_sequence != reservation.last_sequence) {
      return;
    }
    for (uint64_t token : record->second.tokens) {
      const Token_key token_key(reservation.epoch_id, token);
      const auto queue = m_payload_apply_queue_by_token.find(token_key);
      if (queue == m_payload_apply_queue_by_token.end()) continue;
      const auto sequence = std::find(queue->second.begin(),
                                      queue->second.end(),
                                      reservation.first_sequence);
      if (sequence != queue->second.end()) queue->second.erase(sequence);
      if (queue->second.empty()) m_payload_apply_queue_by_token.erase(queue);
    }
    m_payload_apply_records.erase(record);
  }
  m_sequence_condition.notify_all();
}

Preserve_trx_transfer_status Preserve_trx_transfer_receiver_registry::
    wait_for_frame_sequence_applied_through(const std::string &epoch_id,
                                            uint64_t through_sequence,
                                            uint64_t timeout_ms) {
  if (epoch_id.empty() || timeout_ms == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (through_sequence == 0) return Preserve_trx_transfer_status::OK;

  std::unique_lock<std::mutex> guard(m_mutex);
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
  for (;;) {
    const auto failure = m_first_apply_failure_by_epoch.find(epoch_id);
    if (failure != m_first_apply_failure_by_epoch.end() &&
        failure->second.first <= through_sequence) {
      return failure->second.second;
    }
    const auto applied = m_applied_sequence_by_epoch.find(epoch_id);
    if (applied != m_applied_sequence_by_epoch.end() &&
        applied->second >= through_sequence) {
      return Preserve_trx_transfer_status::OK;
    }
    if (m_sequence_condition.wait_until(guard, deadline) ==
        std::cv_status::timeout) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
  }
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::consume_frame_sequence(
    const std::string &epoch_id, uint64_t sequence,
    Preserve_trx_transfer_frame_type frame_type,
    const std::array<unsigned char, kPreservedTrxSha256Length> &digest,
    const std::string *terminal_root_dir,
    const std::array<unsigned char, kPreservedTrxSha256Length>
        *terminal_fact_digest,
    uint64_t terminal_now_us) {
  if (epoch_id.empty() || sequence == 0 ||
      sequence == std::numeric_limits<uint64_t>::max()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::lock_guard<std::mutex> guard(m_mutex);
  const auto terminal = m_acknowledged_epochs.find(epoch_id);
  if (terminal != m_acknowledged_epochs.end()) {
    if (terminal->second.terminal_phase == Terminal_phase::CORRUPT) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    if (terminal->second.terminal_phase == Terminal_phase::ABANDONING ||
        terminal->second.terminal_phase ==
            Terminal_phase::NOT_COMMITTED_CLEAN) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
  }
  if (frame_type != Preserve_trx_transfer_frame_type::COMMIT_EPOCH &&
      frame_sequence_exceeds_commit_cutoff_locked(epoch_id, sequence)) {
    const auto acknowledged = m_acknowledged_epochs.find(epoch_id);
    if (acknowledged != m_acknowledged_epochs.end()) {
      mark_epoch_commit_admission_corrupt_locked(
          epoch_id, acknowledged->second.admitted_commit_sequence,
          acknowledged->second.admitted_commit_frame_digest);
    }
    return Preserve_trx_transfer_status::CORRUPT;
  }
  uint64_t &expected = m_next_sequence_by_epoch[epoch_id];
  if (expected == 0) expected = 1;
  if (sequence != expected) return Preserve_trx_transfer_status::CORRUPT;
  if (frame_type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH) {
    bool duplicate = false;
    const Preserve_trx_transfer_status admission_status =
        begin_epoch_commit_admission_locked(epoch_id, sequence, digest,
                                            &duplicate, terminal_root_dir,
                                            terminal_fact_digest,
                                            terminal_now_us);
    if (admission_status != Preserve_trx_transfer_status::OK) {
      return admission_status;
    }
  }
  expected = sequence + 1;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::admit_frame_sequence(
    const std::string &epoch_id, uint64_t sequence,
    const std::array<unsigned char, kPreservedTrxSha256Length> &digest,
    Preserve_trx_transfer_frame_type frame_type,
    Preserve_trx_transfer_sequence_admission *admission,
    const std::string *terminal_root_dir,
    const std::array<unsigned char, kPreservedTrxSha256Length>
        *terminal_fact_digest,
    uint64_t terminal_now_us) {
  if (epoch_id.empty() || sequence == 0 ||
      sequence == std::numeric_limits<uint64_t>::max() ||
      admission == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::lock_guard<std::mutex> guard(m_mutex);
  const auto terminal = m_acknowledged_epochs.find(epoch_id);
  if (terminal != m_acknowledged_epochs.end()) {
    if (terminal->second.terminal_phase == Terminal_phase::CORRUPT) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    if (terminal->second.terminal_phase == Terminal_phase::ABANDONING ||
        terminal->second.terminal_phase ==
            Terminal_phase::NOT_COMMITTED_CLEAN) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
  }
  const auto key = std::make_pair(epoch_id, sequence);
  const auto existing = m_frame_sequences.find(key);
  if (existing != m_frame_sequences.end()) {
    if (existing->second.corrupt || existing->second.digest != digest) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    if (frame_type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH) {
      bool duplicate = false;
      const Preserve_trx_transfer_status admission_status =
          begin_epoch_commit_admission_locked(epoch_id, sequence, digest,
                                              &duplicate, terminal_root_dir,
                                              terminal_fact_digest,
                                              terminal_now_us);
      if (admission_status != Preserve_trx_transfer_status::OK) {
        return admission_status;
      }
    } else if (frame_sequence_exceeds_commit_cutoff_locked(epoch_id,
                                                            sequence)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    *admission = existing->second.applied
                     ? Preserve_trx_transfer_sequence_admission::ALREADY_APPLIED
                     : Preserve_trx_transfer_sequence_admission::RETRY_PENDING;
    return Preserve_trx_transfer_status::OK;
  }

  if (frame_type != Preserve_trx_transfer_frame_type::COMMIT_EPOCH &&
      frame_sequence_exceeds_commit_cutoff_locked(epoch_id, sequence)) {
    const auto acknowledged = m_acknowledged_epochs.find(epoch_id);
    if (acknowledged != m_acknowledged_epochs.end()) {
      mark_epoch_commit_admission_corrupt_locked(
          epoch_id, acknowledged->second.admitted_commit_sequence,
          acknowledged->second.admitted_commit_frame_digest);
    }
    return Preserve_trx_transfer_status::CORRUPT;
  }
  uint64_t &expected = m_next_sequence_by_epoch[epoch_id];
  if (expected == 0) expected = 1;
  if (sequence != expected) return Preserve_trx_transfer_status::CORRUPT;
  std::map<std::pair<std::string, uint64_t>, Frame_sequence_record>::iterator
      inserted_frame;
  try {
    Frame_sequence_record record;
    record.digest = digest;
    const auto inserted =
        m_frame_sequences.emplace(key, std::move(record));
    if (!inserted.second) return Preserve_trx_transfer_status::CORRUPT;
    inserted_frame = inserted.first;
  } catch (...) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  if (frame_type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH) {
    bool duplicate = false;
    const Preserve_trx_transfer_status admission_status =
        begin_epoch_commit_admission_locked(epoch_id, sequence, digest,
                                            &duplicate, terminal_root_dir,
                                            terminal_fact_digest,
                                            terminal_now_us);
    if (admission_status != Preserve_trx_transfer_status::OK) {
      m_frame_sequences.erase(inserted_frame);
      return admission_status;
    }
  }
  expected = sequence + 1;
  *admission = Preserve_trx_transfer_sequence_admission::NEW_FRAME;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::begin_epoch_commit_admission_locked(
    const std::string &epoch_id, uint64_t sequence,
    const std::array<unsigned char, kPreservedTrxSha256Length> &frame_digest,
    bool *duplicate, const std::string *terminal_root_dir,
    const std::array<unsigned char, kPreservedTrxSha256Length>
        *terminal_fact_digest,
    uint64_t terminal_now_us) {
  if (transfer_digest_is_zero(frame_digest)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  *duplicate = false;
  auto acknowledged = m_acknowledged_epochs.find(epoch_id);
  if (acknowledged == m_acknowledged_epochs.end() &&
      m_accepted_epochs.count(epoch_id) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  Acknowledged_epoch empty_terminal;
  Acknowledged_epoch &terminal =
      acknowledged == m_acknowledged_epochs.end() ? empty_terminal
                                                  : acknowledged->second;
  switch (terminal.terminal_phase) {
    case Terminal_phase::OPEN: {
      if (terminal.terminal_operation !=
          Preserve_trx_transfer_epoch_terminal_operation::NONE) {
        terminal.terminal_phase = Terminal_phase::CORRUPT;
        terminal.terminal_outcome =
            Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
        return Preserve_trx_transfer_status::CORRUPT;
      }
      if (m_accepted_epochs.count(epoch_id) != 0) {
        return Preserve_trx_transfer_status::UNSUPPORTED;
      }
      const auto online = m_online_epochs.find(epoch_id);
      if (online != m_online_epochs.end() &&
          (terminal_root_dir == nullptr || terminal_root_dir->empty() ||
           terminal_fact_digest == nullptr ||
           transfer_digest_is_zero(*terminal_fact_digest) ||
           terminal_now_us == 0)) {
        return Preserve_trx_transfer_status::CORRUPT;
      }

      Acknowledged_epoch candidate;
      try {
        candidate = terminal;
        candidate.terminal_operation_id = kReceiverCommitOperationId;
        candidate.terminal_operation =
            Preserve_trx_transfer_epoch_terminal_operation::COMMIT;
        candidate.terminal_outcome =
            Preserve_trx_transfer_epoch_terminal_outcome::NOT_COMMITTED;
        candidate.terminal_phase = Terminal_phase::COMMIT_ADMITTED;
        candidate.admitted_commit_sequence = sequence;
        candidate.admitted_commit_frame_digest = frame_digest;
        candidate.commit_snapshot_verified = false;
        if (online != m_online_epochs.end()) {
          if (!candidate.root_dir.empty() &&
              candidate.root_dir != *terminal_root_dir) {
            return Preserve_trx_transfer_status::CORRUPT;
          }
          candidate.root_dir = *terminal_root_dir;
          candidate.receiver_process_generation =
              online->second.receiver_process_nonce;
          candidate.receiver_process_nonce =
              online->second.receiver_process_nonce;
          candidate.authenticated_principal =
              online->second.authenticated_principal;
          candidate.fact_digest = *terminal_fact_digest;
          candidate.terminal_cas_monotonic_us = terminal_now_us;
          const uint64_t retention_us =
              online->second.accepted_terminal_status_retention_us;
          const uint64_t retire_after_us =
              terminal_now_us >
                      std::numeric_limits<uint64_t>::max() - retention_us
                  ? std::numeric_limits<uint64_t>::max()
                  : terminal_now_us + retention_us;
          candidate.retire_after_us =
              std::max(candidate.retire_after_us, retire_after_us);
        }
      } catch (...) {
        return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
      }
      try {
        if (acknowledged == m_acknowledged_epochs.end()) {
          acknowledged =
              m_acknowledged_epochs.emplace(epoch_id, std::move(candidate))
                  .first;
        } else {
          acknowledged->second = std::move(candidate);
        }
      } catch (...) {
        return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
      }
      g_receiver_terminal_cas_wins.fetch_add(1);
      g_receiver_terminal_status_tombstones.fetch_add(1);
      return Preserve_trx_transfer_status::OK;
    }
    case Terminal_phase::COMMIT_ADMITTED:
      if (terminal.admitted_commit_sequence == sequence &&
          terminal.admitted_commit_frame_digest == frame_digest) {
        const bool authenticated_terminal =
            !terminal.authenticated_principal.empty() &&
            !terminal.receiver_process_nonce.empty() &&
            !transfer_digest_is_zero(terminal.fact_digest);
        if (authenticated_terminal &&
            (terminal_root_dir == nullptr ||
             *terminal_root_dir != terminal.root_dir ||
             terminal_fact_digest == nullptr ||
             *terminal_fact_digest != terminal.fact_digest)) {
          terminal.terminal_phase = Terminal_phase::CORRUPT;
          terminal.terminal_outcome =
              Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
          g_receiver_terminal_cas_conflicts.fetch_add(1);
          return Preserve_trx_transfer_status::CORRUPT;
        }
        *duplicate = true;
        return Preserve_trx_transfer_status::OK;
      }
      terminal.terminal_phase = Terminal_phase::CORRUPT;
      terminal.terminal_outcome =
          Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
      g_receiver_terminal_cas_conflicts.fetch_add(1);
      return Preserve_trx_transfer_status::CORRUPT;
    case Terminal_phase::COMMITTED:
      if (terminal.admitted_commit_sequence == sequence &&
          terminal.admitted_commit_frame_digest == frame_digest) {
        const bool authenticated_terminal =
            !terminal.authenticated_principal.empty() &&
            !terminal.receiver_process_nonce.empty() &&
            !transfer_digest_is_zero(terminal.fact_digest);
        if (!authenticated_terminal ||
            (terminal_root_dir != nullptr &&
             *terminal_root_dir == terminal.root_dir &&
             terminal_fact_digest != nullptr &&
             *terminal_fact_digest == terminal.fact_digest)) {
          *duplicate = true;
          return Preserve_trx_transfer_status::OK;
        }
      }
      g_receiver_terminal_cas_conflicts.fetch_add(1);
      return Preserve_trx_transfer_status::CORRUPT;
    case Terminal_phase::ABANDONING:
    case Terminal_phase::NOT_COMMITTED_CLEAN:
      g_receiver_terminal_cas_conflicts.fetch_add(1);
      return Preserve_trx_transfer_status::UNSUPPORTED;
    case Terminal_phase::CORRUPT:
      return Preserve_trx_transfer_status::CORRUPT;
  }
  return Preserve_trx_transfer_status::CORRUPT;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::begin_epoch_commit_admission(
    const std::string &epoch_id, uint64_t sequence,
    const std::array<unsigned char, kPreservedTrxSha256Length> &frame_digest,
    bool *duplicate, const std::string *terminal_root_dir,
    const std::array<unsigned char, kPreservedTrxSha256Length>
        *terminal_fact_digest,
    uint64_t terminal_now_us) {
  if (!transfer_component_safe(epoch_id) || sequence == 0 ||
      sequence == std::numeric_limits<uint64_t>::max() ||
      transfer_digest_is_zero(frame_digest) || duplicate == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  return begin_epoch_commit_admission_locked(epoch_id, sequence, frame_digest,
                                             duplicate, terminal_root_dir,
                                             terminal_fact_digest,
                                             terminal_now_us);
}

bool Preserve_trx_transfer_receiver_registry::
    frame_sequence_exceeds_commit_cutoff_locked(
        const std::string &epoch_id, uint64_t sequence) const {
  const auto acknowledged = m_acknowledged_epochs.find(epoch_id);
  if (acknowledged == m_acknowledged_epochs.end()) return false;
  const Acknowledged_epoch &terminal = acknowledged->second;
  return (terminal.terminal_phase == Terminal_phase::COMMIT_ADMITTED ||
          terminal.terminal_phase == Terminal_phase::COMMITTED) &&
         terminal.admitted_commit_sequence != 0 &&
         sequence > terminal.admitted_commit_sequence;
}

void Preserve_trx_transfer_receiver_registry::
    mark_epoch_commit_admission_corrupt_locked(
        const std::string &epoch_id, uint64_t sequence,
        const std::array<unsigned char, kPreservedTrxSha256Length>
            &frame_digest) {
  const auto acknowledged = m_acknowledged_epochs.find(epoch_id);
  if (acknowledged == m_acknowledged_epochs.end()) return;
  Acknowledged_epoch &terminal = acknowledged->second;
  if (terminal.terminal_operation !=
          Preserve_trx_transfer_epoch_terminal_operation::COMMIT ||
      terminal.admitted_commit_sequence != sequence ||
      terminal.admitted_commit_frame_digest != frame_digest ||
      terminal.terminal_phase == Terminal_phase::COMMITTED) {
    return;
  }
  terminal.terminal_phase = Terminal_phase::CORRUPT;
  terminal.terminal_outcome =
      Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
  const auto accepted = m_accepted_epochs.find(epoch_id);
  if (accepted != m_accepted_epochs.end()) {
    accepted->second.lifecycle =
        Preserve_trx_transfer_epoch_lifecycle::ABANDONING;
  }
}

void Preserve_trx_transfer_receiver_registry::
    mark_epoch_commit_admission_corrupt(
        const std::string &epoch_id, uint64_t sequence,
        const std::array<unsigned char, kPreservedTrxSha256Length>
            &frame_digest) {
  std::lock_guard<std::mutex> guard(m_mutex);
  mark_epoch_commit_admission_corrupt_locked(epoch_id, sequence,
                                             frame_digest);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::snapshot_epoch_for_commit(
    const std::string &epoch_id, uint64_t sequence,
    const std::array<unsigned char, kPreservedTrxSha256Length> &frame_digest,
    std::vector<Preserve_trx_transfer_receiver_record> *records,
    bool allow_empty_snapshot) {
  if (!transfer_component_safe(epoch_id) || sequence == 0 ||
      transfer_digest_is_zero(frame_digest) || records == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::vector<Preserve_trx_transfer_receiver_record> snapshot;
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto acknowledged = m_acknowledged_epochs.find(epoch_id);
  if (acknowledged == m_acknowledged_epochs.end()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  Acknowledged_epoch &terminal = acknowledged->second;
  if (terminal.terminal_phase == Terminal_phase::CORRUPT) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (terminal.terminal_phase == Terminal_phase::COMMITTED &&
      terminal.admitted_commit_sequence == sequence &&
      terminal.admitted_commit_frame_digest == frame_digest) {
    return Preserve_trx_transfer_status::COMMITTED_NOT_READY;
  }
  if (terminal.terminal_phase != Terminal_phase::COMMIT_ADMITTED ||
      terminal.admitted_commit_sequence != sequence ||
      terminal.admitted_commit_frame_digest != frame_digest) {
    mark_epoch_commit_admission_corrupt_locked(epoch_id, sequence,
                                               frame_digest);
    return Preserve_trx_transfer_status::CORRUPT;
  }

  try {
    for (const auto &entry : m_records) {
      const Preserve_trx_transfer_receiver_record &record = entry.second;
      if (record.epoch_id != epoch_id ||
          record.state == Preserve_trx_transfer_receiver_state::ABORTED) {
        continue;
      }
      if (record.state != Preserve_trx_transfer_receiver_state::RECEIVING &&
          record.state != Preserve_trx_transfer_receiver_state::SAVED_ONLINE) {
        mark_epoch_commit_admission_corrupt_locked(epoch_id, sequence,
                                                   frame_digest);
        return Preserve_trx_transfer_status::CORRUPT;
      }
      for (const Preserve_trx_transfer_object_descriptor &object :
           record.objects) {
        if (record.sealed_objects.count(object.object_id) == 0) {
          mark_epoch_commit_admission_corrupt_locked(epoch_id, sequence,
                                                     frame_digest);
          return Preserve_trx_transfer_status::CORRUPT;
        }
      }
      snapshot.push_back(record);
    }
  } catch (...) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  if (snapshot.empty() && !allow_empty_snapshot) {
    mark_epoch_commit_admission_corrupt_locked(epoch_id, sequence,
                                               frame_digest);
    return Preserve_trx_transfer_status::CORRUPT;
  }
  terminal.commit_snapshot_verified = true;
  records->swap(snapshot);
  return Preserve_trx_transfer_status::OK;
}

void Preserve_trx_transfer_receiver_registry::mark_frame_sequence_applied(
    const std::string &epoch_id, uint64_t sequence) {
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto found =
        m_frame_sequences.find(std::make_pair(epoch_id, sequence));
    if (found == m_frame_sequences.end() || found->second.corrupt) return;
    found->second.applied = true;
    found->second.apply_failure = Preserve_trx_transfer_status::OK;
    const auto first_failure = m_first_apply_failure_by_epoch.find(epoch_id);
    if (first_failure != m_first_apply_failure_by_epoch.end() &&
        first_failure->second.first == sequence) {
      m_first_apply_failure_by_epoch.erase(first_failure);
      auto next_failure =
          m_frame_sequences.lower_bound(std::make_pair(epoch_id, 0));
      for (; next_failure != m_frame_sequences.end() &&
             next_failure->first.first == epoch_id;
           ++next_failure) {
        const Frame_sequence_record &record = next_failure->second;
        if (record.applied ||
            record.apply_failure == Preserve_trx_transfer_status::OK) {
          continue;
        }
        m_first_apply_failure_by_epoch[epoch_id] = std::make_pair(
            next_failure->first.second, record.apply_failure);
        break;
      }
    }
    uint64_t &applied = m_applied_sequence_by_epoch[epoch_id];
    while (applied != std::numeric_limits<uint64_t>::max()) {
      const auto next =
          m_frame_sequences.find(std::make_pair(epoch_id, applied + 1));
      if (next == m_frame_sequences.end() || !next->second.applied ||
          next->second.corrupt) {
        break;
      }
      ++applied;
    }
  }
  m_sequence_condition.notify_all();
}

void Preserve_trx_transfer_receiver_registry::
    mark_frame_sequence_apply_failed(const std::string &epoch_id,
                                     uint64_t sequence,
                                     Preserve_trx_transfer_status status) {
  if (status == Preserve_trx_transfer_status::OK) return;
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto found =
        m_frame_sequences.find(std::make_pair(epoch_id, sequence));
    if (found == m_frame_sequences.end() || found->second.applied ||
        found->second.corrupt) {
      return;
    }
    found->second.apply_failure = status;
    auto failure = m_first_apply_failure_by_epoch.find(epoch_id);
    if (failure == m_first_apply_failure_by_epoch.end() ||
        sequence <= failure->second.first) {
      m_first_apply_failure_by_epoch[epoch_id] =
          std::make_pair(sequence, status);
    }
  }
  m_sequence_condition.notify_all();
}

void Preserve_trx_transfer_receiver_registry::mark_frame_sequence_corrupt(
    const std::string &epoch_id, uint64_t sequence) {
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto found =
        m_frame_sequences.find(std::make_pair(epoch_id, sequence));
    if (found == m_frame_sequences.end()) return;
    found->second.corrupt = true;
    found->second.apply_failure = Preserve_trx_transfer_status::CORRUPT;
    auto failure = m_first_apply_failure_by_epoch.find(epoch_id);
    if (failure == m_first_apply_failure_by_epoch.end() ||
        sequence <= failure->second.first) {
      m_first_apply_failure_by_epoch[epoch_id] =
          std::make_pair(sequence, Preserve_trx_transfer_status::CORRUPT);
    }
  }
  m_sequence_condition.notify_all();
}

bool Preserve_trx_transfer_receiver_registry::frame_sequence_applied(
    const std::string &epoch_id, uint64_t sequence) const {
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_frame_sequences.find(std::make_pair(epoch_id, sequence));
  return found != m_frame_sequences.end() && found->second.applied;
}

void Preserve_trx_transfer_receiver_registry::rollback_frame_sequence(
    const std::string &epoch_id, uint64_t sequence) {
  if (epoch_id.empty() || sequence == 0 ||
      sequence == std::numeric_limits<uint64_t>::max()) {
    return;
  }

  std::lock_guard<std::mutex> guard(m_mutex);
  auto found = m_next_sequence_by_epoch.find(epoch_id);
  if (found == m_next_sequence_by_epoch.end()) return;
  const auto frame = m_frame_sequences.find(std::make_pair(epoch_id, sequence));
  if (found->second == sequence + 1 &&
      (frame == m_frame_sequences.end() || !frame->second.applied)) {
    if (frame != m_frame_sequences.end()) m_frame_sequences.erase(frame);
    found->second = sequence;
  }
}

bool Preserve_trx_transfer_receiver_registry::all_objects_sealed(
    const std::string &epoch_id, uint64_t token, bool require_snapshot) const {
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_records.find(Token_key(epoch_id, token));
  if (found == m_records.end() ||
      found->second.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
    return false;
  }
  // Seal insertion accepts only an indexed object; BEGIN/replacement retain
  // or remove its seal together with the descriptor.
  return (!require_snapshot || found->second.has_snapshot_bundle) &&
         found->second.sealed_objects.size() == found->second.objects.size();
}

std::vector<Preserve_trx_transfer_receiver_record>
Preserve_trx_transfer_receiver_registry::receiving_records_for_epoch(
    const std::string &epoch_id) const {
  std::vector<Preserve_trx_transfer_receiver_record> records;
  std::lock_guard<std::mutex> guard(m_mutex);
  for (const auto &entry : m_records) {
    const Preserve_trx_transfer_receiver_record &record = entry.second;
    if (record.epoch_id == epoch_id &&
        (record.state == Preserve_trx_transfer_receiver_state::DECLARED ||
         record.state == Preserve_trx_transfer_receiver_state::RECEIVING)) {
      records.push_back(record);
    }
  }
  return records;
}

std::vector<Preserve_trx_transfer_receiver_record>
Preserve_trx_transfer_receiver_registry::sealed_receiving_records_for_epoch(
    const std::string &epoch_id) const {
  std::vector<Preserve_trx_transfer_receiver_record> records;
  std::lock_guard<std::mutex> guard(m_mutex);
  for (const auto &entry : m_records) {
    const Preserve_trx_transfer_receiver_record &record = entry.second;
    if (record.epoch_id != epoch_id ||
        record.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
      continue;
    }
    bool sealed = true;
    for (const Preserve_trx_transfer_object_descriptor &object :
         record.objects) {
      if (record.sealed_objects.count(object.object_id) == 0) {
        sealed = false;
        break;
      }
    }
    if (sealed) records.push_back(record);
  }
  return records;
}

bool Preserve_trx_transfer_receiver_registry::lookup(
    const std::string &epoch_id, uint64_t token,
    Preserve_trx_transfer_receiver_record *record) const {
  if (record == nullptr) return false;
  Preserve_trx_transfer_receiver_record snapshot;
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto found = m_records.find(Token_key(epoch_id, token));
    if (found == m_records.end()) return false;
    snapshot = found->second;
  }
  *record = std::move(snapshot);
  return true;
}

bool Preserve_trx_transfer_receiver_registry::lookup_state(
    const std::string &epoch_id, uint64_t token,
    Preserve_trx_transfer_receiver_state *state,
    Preserve_trx_temp_id_contract *contract, bool *has_resource_objects) const {
  if (!state) return false;
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_records.find(Token_key(epoch_id, token));
  if (found == m_records.end()) return false;
  *state = found->second.state;
  if (contract) *contract = found->second.temp_id_contract;
  if (has_resource_objects) *has_resource_objects = found->second.has_resource_objects;
  return true;
}

bool Preserve_trx_transfer_receiver_registry::lookup_object(
    const std::string &epoch_id, uint64_t token, const std::string &object_id,
    Preserve_trx_transfer_receiver_record *record, size_t *object_count,
    bool sealed_owners, const char *companion) const {
  if (record == nullptr || object_count == nullptr) return false;
  Preserve_trx_transfer_receiver_record snapshot;
  size_t count = 0;
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto found = m_records.find(Token_key(epoch_id, token));
    if (found == m_records.end()) return false;
    const auto &source = found->second;
    snapshot.temp_id_contract = source.temp_id_contract;
    snapshot.epoch_id = source.epoch_id;
    snapshot.token = source.token;
    snapshot.protocol_version = source.protocol_version;
    snapshot.strict_eligibility_flags = source.strict_eligibility_flags;
    snapshot.source_freeze_lsn = source.source_freeze_lsn;
    snapshot.source_epoch_commit_lsn = source.source_epoch_commit_lsn;
    snapshot.state = source.state;
    snapshot.reserved_bytes = source.reserved_bytes;
    snapshot.resource_reserved_bytes = source.resource_reserved_bytes;
    snapshot.staged_manifest_frozen = source.staged_manifest_frozen;
    count = source.objects.size();
    if (count <= kMaxTransferManifestObjects && source.object_index) {
      const auto copy_object = [&](const std::string &id) {
        const auto *object = source.object_index->find(source.objects, id);
        if (!object) return;
        snapshot.objects.push_back(*object);
        if (sealed_owners) {
          if (source.sealed_objects.count(id)) snapshot.sealed_objects.insert(id);
          const auto file = source.sealed_files.find(id);
          if (file != source.sealed_files.end()) snapshot.sealed_files.emplace(*file);
        }
      };
      copy_object(object_id);
      if (companion && object_id != companion) copy_object(companion);
      snapshot.has_resource_objects = source.has_resource_objects;
      if (sealed_owners) snapshot.resource_candidates = source.resource_candidates;
    }
  }
  *record = std::move(snapshot);
  *object_count = count;
  return true;
}

size_t Preserve_trx_transfer_receiver_registry::size() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_records.size();
}

size_t Preserve_trx_transfer_receiver_registry::active_epoch_count() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return std::count_if(
      m_accepted_epochs.begin(), m_accepted_epochs.end(), [](const auto &item) {
        return item.second.lifecycle !=
                   Preserve_trx_transfer_epoch_lifecycle::ABANDONING &&
               item.second.lifecycle !=
                   Preserve_trx_transfer_epoch_lifecycle::EXPIRED;
      });
}

uint64_t
Preserve_trx_transfer_receiver_registry::expired_epoch_count() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_expired_epoch_count;
}

Preserve_trx_transfer_receiver_status_counts
Preserve_trx_transfer_receiver_registry::status_counts() const {
  Preserve_trx_transfer_receiver_status_counts counts;
  std::lock_guard<std::mutex> guard(m_mutex);
  counts.last_failed_token = m_last_failed_token;
  counts.last_failed_reason = m_last_failed_reason;
  for (const auto &item : m_records) {
    if (item.second.reserved_bytes <=
        std::numeric_limits<uint64_t>::max() - counts.inflight_bytes)
      counts.inflight_bytes += item.second.reserved_bytes;
    else
      counts.inflight_bytes = std::numeric_limits<uint64_t>::max();
    switch (item.second.state) {
      case Preserve_trx_transfer_receiver_state::DECLARED:
      case Preserve_trx_transfer_receiver_state::RECEIVING:
      case Preserve_trx_transfer_receiver_state::CLEANUP_PENDING:
        ++counts.inflight_tokens;
        break;
      case Preserve_trx_transfer_receiver_state::SAVED_ONLINE:
        ++counts.saved_online_tokens;
        break;
      case Preserve_trx_transfer_receiver_state::CLEANUP_TAINTED:
      case Preserve_trx_transfer_receiver_state::CORRUPT:
      case Preserve_trx_transfer_receiver_state::ABORTED:
        ++counts.failed_tokens;
        break;
    }
  }
  counts.inflight_bytes = m_cleanup_debt_carries ||
      m_cleanup_debt_bytes > UINT64_MAX - counts.inflight_bytes
          ? UINT64_MAX : counts.inflight_bytes + m_cleanup_debt_bytes;
  return counts;
}

Preserve_trx_transfer_status materialize_prebuilt_external_blobs_for_transfer(
    const std::string &preserve_dir, Preserved_trx_bundle *bundle,
    const std::set<std::string> *presealed_prebuilt_objects = nullptr) {
  if (bundle == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  bool has_prebuilt = false;
  for (const Preserved_trx_external_blob &blob : bundle->external_blobs) {
    if (blob.prebuilt) {
      has_prebuilt = true;
      break;
    }
  }
  if (!has_prebuilt) return Preserve_trx_transfer_status::OK;

  const std::string source_dir =
      preserve_dir.empty() ? preserved_trx_dir_value() : preserve_dir;
  std::unique_ptr<Preserved_trx_warm_external_blob_carrier> warm_carrier =
      create_preserved_trx_default_warm_external_blob_carrier(source_dir);
  if (warm_carrier == nullptr) {
    return Preserve_trx_transfer_status::IO_ERROR;
  }

  std::vector<Preserved_trx_external_blob> materialized_blobs;
  materialized_blobs.reserve(bundle->external_blobs.size());
  const uint64_t max_blob_bytes =
      preserve_trx_transfer_max_inflight_bytes == 0
          ? std::numeric_limits<uint64_t>::max()
          : preserve_trx_transfer_max_inflight_bytes;

  for (const Preserved_trx_external_blob &blob : bundle->external_blobs) {
    if (!blob.prebuilt) {
      materialized_blobs.push_back(blob);
      continue;
    }
    if (presealed_prebuilt_objects != nullptr &&
        presealed_prebuilt_objects->count(blob.name) != 0) {
      materialized_blobs.push_back(blob);
      continue;
    }

    Preserved_trx_external_blob materialized;
    const Preserved_trx_carrier_status carrier_status =
        warm_carrier->read_warm_external_blob(
            blob.warmcopy_id, blob.name, blob.warmcopy_epoch, blob.descriptor,
            max_blob_bytes, &materialized);
    if (carrier_status != Preserved_trx_carrier_status::OK) {
      return map_carrier_status_to_transfer(carrier_status);
    }
    materialized.lock_plan_contract_version =
        blob.lock_plan_contract_version;
    materialized.source_live_lock_generation =
        blob.source_live_lock_generation;
    materialized.source_live_lock_digest = blob.source_live_lock_digest;
    materialized.record_store_fingerprint =
        blob.record_store_fingerprint;
    materialized_blobs.push_back(std::move(materialized));
  }

  bundle->external_blobs = std::move(materialized_blobs);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_encode_portable_bundle_impl(
    const Preserved_trx_bundle &bundle, std::string *encoded,
    bool allow_prebuilt_descriptors) {
  if (encoded == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  for (const Preserved_trx_external_blob &blob : bundle.external_blobs) {
    if (blob.prebuilt && !allow_prebuilt_descriptors) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
  }

  Preserved_trx_codec_context context;
  if (!transfer_bundle_codec_context(&context)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserved_trx_encoded_bundle snapshot;
  const Preserve_snapshot_status encode_status = encode_preserved_trx_bundle(
      context, bundle, &snapshot, nullptr);
  if (encode_status != Preserve_snapshot_status::OK) {
    return map_snapshot_status_to_transfer(encode_status);
  }

  std::string out;
  out.append(kTransferBundleMagic, kTransferBundleMagicLength);
  append_u16(&out, kPreserveTrxTransferProtocolVersion);
  append_bytes64(&out, snapshot.snapshot_bytes);
  *encoded = std::move(out);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_encode_portable_bundle(
    const Preserved_trx_bundle &bundle, std::string *encoded) {
  return preserve_trx_transfer_encode_portable_bundle_impl(
      bundle, encoded, false);
}

Preserve_trx_transfer_status preserve_trx_transfer_decode_portable_bundle(
    const std::string &encoded, Preserved_trx_bundle *bundle) {
  if (bundle == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  Manifest_reader reader(encoded);
  const char *magic = nullptr;
  uint16_t version = 0;
  std::vector<unsigned char> snapshot_bytes;
  if (reader.read_fixed(kTransferBundleMagicLength, &magic) ||
      std::memcmp(magic, kTransferBundleMagic, kTransferBundleMagicLength) !=
          0 ||
      reader.read_u16(&version) || reader.read_bytes64(&snapshot_bytes) ||
      !reader.eof()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (version != kPreserveTrxTransferProtocolVersion) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserved_trx_codec_context context;
  if (!transfer_bundle_codec_context(&context)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserved_trx_decoded_snapshot decoded;
  const Preserve_snapshot_status decode_status =
      decode_preserved_trx_snapshot_bytes(context, snapshot_bytes, false,
                                          &decoded);
  if (decode_status != Preserve_snapshot_status::OK) {
    return map_snapshot_status_to_transfer(decode_status);
  }

  Preserved_trx_bundle out;
  out.metadata = std::move(decoded.header_metadata);
  out.tlvs = std::move(decoded.tlvs);
  out.blob_descriptors = std::move(decoded.blob_descriptors);
  out.owns_current_temp_sidecars =
      !out.metadata.temp_table_manifest_payload.empty();
  *bundle = std::move(out);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_build_portable_objects_impl(
    const std::string &epoch_id, const Preserved_trx_bundle &bundle,
    uint64_t transfer_token, Preserve_trx_transfer_manifest *manifest,
    std::vector<Preserve_trx_transfer_object_payload> *objects,
    const std::set<std::string> *presealed_prebuilt_objects,
    const Preserve_trx_resurrection_index_entry *resurrection_entry,
    uint64_t fixed_source_freeze_lsn = 0,
    uint64_t fixed_source_epoch_commit_lsn = 0) {
  if (manifest == nullptr || objects == nullptr || transfer_token == 0) {
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           "PRESERVE: standby transfer portable object build invalid argument");
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::string portable_snapshot;
  const bool allow_prebuilt_descriptors =
      presealed_prebuilt_objects != nullptr &&
      std::all_of(bundle.external_blobs.begin(), bundle.external_blobs.end(),
                  [&](const Preserved_trx_external_blob &blob) {
                    return !blob.prebuilt ||
                           presealed_prebuilt_objects->count(blob.name) != 0;
                  });
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_encode_portable_bundle_impl(
          bundle, &portable_snapshot, allow_prebuilt_descriptors);
  if (status != Preserve_trx_transfer_status::OK) {
    const std::string message =
        "PRESERVE: standby transfer portable snapshot encode failed status=" +
        transfer_status_name(status) +
        " token=" + bundle.metadata.token +
        " external_blobs=" + std::to_string(bundle.external_blobs.size()) +
        " binlog_state=" +
        std::to_string(static_cast<int>(bundle.metadata.binlog_state));
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return status;
  }

  Preserve_trx_transfer_manifest built_manifest;
  built_manifest.epoch_id = epoch_id;
  built_manifest.token = transfer_token;
  Preserve_trx_resurrection_index_entry provided_entry;
  const Preserve_trx_resurrection_index_entry *effective_entry =
      resurrection_entry;
  Preserve_trx_transfer_source_resurrection_provider test_provider =
      unit_source_resurrection_provider();
  if (effective_entry == nullptr && test_provider != nullptr &&
      test_provider(bundle, transfer_token, &provided_entry)) {
    effective_entry = &provided_entry;
  }
  uint64_t sampled_freeze_lsn = 0;
  if ((fixed_source_freeze_lsn == 0) !=
      (fixed_source_epoch_commit_lsn == 0)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (fixed_source_epoch_commit_lsn != 0) {
    sampled_freeze_lsn = fixed_source_freeze_lsn;
    built_manifest.source_epoch_commit_lsn =
        fixed_source_epoch_commit_lsn;
  } else if (!load_source_transfer_lsn_fact(
                 &sampled_freeze_lsn,
                 &built_manifest.source_epoch_commit_lsn)) {
    const std::string message =
        "PRESERVE: standby transfer source LSN fact unavailable token=" +
        bundle.metadata.token;
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  built_manifest.source_freeze_lsn =
      bundle.metadata.recovery.needs_no_redo_context()
          ? bundle.metadata.recovery.freeze_lsn
          : effective_entry == nullptr ? sampled_freeze_lsn
                                 : effective_entry->freeze_lsn;

  std::vector<Preserve_trx_transfer_object_payload> built_objects;
  std::set<std::string> object_ids;

  Preserve_memory_lease result_descriptors_memory;
  std::vector<Preserve_trx_transfer_object_descriptor> result_objects;
  status = preserve_trx_result_transfer_descriptors(bundle.metadata.token,
      bundle.metadata.cursor_manifest_payload, &result_objects,
      &result_descriptors_memory);
  if (status != Preserve_trx_transfer_status::OK) return status;
  for (const auto &object : result_objects) {
    if (presealed_prebuilt_objects == nullptr ||
        presealed_prebuilt_objects->count(object.object_id) == 0)
      return Preserve_trx_transfer_status::UNSUPPORTED;
    object_ids.insert(object.object_id);
  }

  std::vector<Preserve_trx_transfer_object_descriptor> temp_objects;
  status = preserve_trx_temp_transfer_descriptors(std::to_string(transfer_token),
      bundle.metadata.temp_table_manifest_payload, &temp_objects);
  if (status != Preserve_trx_transfer_status::OK) return status;
  for (const auto &object : temp_objects) {
    if (presealed_prebuilt_objects == nullptr ||
        presealed_prebuilt_objects->count(object.object_id) == 0)
      return Preserve_trx_transfer_status::UNSUPPORTED;
    if (!object_ids.insert(object.object_id).second)
      return Preserve_trx_transfer_status::CORRUPT;
  }

  Preserve_trx_transfer_object_payload snapshot_object;
  snapshot_object.descriptor.object_id = "snapshot";
  snapshot_object.descriptor.kind =
      Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE;
  snapshot_object.descriptor.total_size = portable_snapshot.length();
  snapshot_object.descriptor.digest = sha256_digest(portable_snapshot);
  snapshot_object.payload = std::move(portable_snapshot);
  object_ids.insert(snapshot_object.descriptor.object_id);
  built_manifest.objects.push_back(snapshot_object.descriptor);
  built_objects.push_back(std::move(snapshot_object));

  if (effective_entry != nullptr) {
    if (bundle.metadata.recovery.needs_no_redo_context() || bundle.metadata.recovery.resource_only())
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    if (effective_entry->authority_token != std::to_string(transfer_token) ||
        effective_entry->freeze_lsn == 0 ||
        effective_entry->freeze_lsn >
            built_manifest.source_epoch_commit_lsn) {
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }
    Preserved_trx_codec_context context;
    if (!transfer_bundle_codec_context(&context)) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    Preserve_trx_resurrection_index index;
    index.local_instance_identity = context.server_uuid;
    index.epoch_id = epoch_id;
    index.entries.push_back(*effective_entry);
    index.entries.front().snapshot_digest =
        built_manifest.objects.front().digest;

    Preserve_trx_transfer_object_payload index_object;
    index_object.descriptor.object_id =
        kPreserveTrxResurrectionIndexObjectId;
    index_object.descriptor.kind =
        Preserve_trx_transfer_object_kind::RESURRECTION_INDEX;
    const Preserve_trx_resurrection_index_status index_status =
        preserve_trx_encode_resurrection_index(index, context,
                                               &index_object.payload);
    if (index_status != Preserve_trx_resurrection_index_status::OK) {
      return index_status ==
                     Preserve_trx_resurrection_index_status::RESOURCE_EXHAUSTED
                 ? Preserve_trx_transfer_status::RESOURCE_EXHAUSTED
                 : Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }
    index_object.descriptor.total_size = index_object.payload.size();
    index_object.descriptor.digest = sha256_digest(index_object.payload);
    object_ids.insert(index_object.descriptor.object_id);
    built_manifest.objects.push_back(index_object.descriptor);
    built_objects.push_back(std::move(index_object));
    built_manifest.strict_eligibility_flags =
        PRESERVE_TRX_TRANSFER_STRICT_ACTIVE_UNDO |
        PRESERVE_TRX_TRANSFER_STRICT_PARTICIPANTS_AUTHENTICATED;
  } else if (bundle.metadata.recovery.needs_no_redo_context() || bundle.metadata.recovery.resource_only()) {
    if (!preserve_trx_recovery_payload_valid(bundle.metadata) ||
        built_manifest.source_freeze_lsn > built_manifest.source_epoch_commit_lsn)
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    built_manifest.strict_eligibility_flags =
        (bundle.metadata.recovery.resource_only() ? PRESERVE_TRX_TRANSFER_STRICT_RESOURCE_ONLY
                                                : PRESERVE_TRX_TRANSFER_STRICT_NO_REDO_CONTEXT) |
        PRESERVE_TRX_TRANSFER_STRICT_PARTICIPANTS_AUTHENTICATED;
  }

  for (const Preserved_trx_external_blob &blob : bundle.external_blobs) {
    if (!transfer_component_safe(blob.name) ||
        !object_ids.insert(blob.name).second) {
      const std::string message =
          "PRESERVE: standby transfer portable object name invalid name=" +
          blob.name + " token=" + bundle.metadata.token;
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }

    Preserve_trx_transfer_object_payload object;
    object.descriptor =
        transfer_external_blob_descriptor(epoch_id, transfer_token, blob);
    if (blob.prebuilt) {
      if (presealed_prebuilt_objects == nullptr ||
          presealed_prebuilt_objects->count(blob.name) == 0 ||
          blob.descriptor.name != blob.name || blob.descriptor.size == 0 ||
          !blob.payload.empty()) {
        return Preserve_trx_transfer_status::UNSUPPORTED;
      }
      built_manifest.objects.push_back(object.descriptor);
      continue;
    }
    object.payload = blob.payload;
    built_manifest.objects.push_back(object.descriptor);
    built_objects.push_back(std::move(object));
  }

  built_manifest.objects.insert(built_manifest.objects.end(),
                                 result_objects.begin(), result_objects.end());
  built_manifest.objects.insert(built_manifest.objects.end(),
                                 temp_objects.begin(), temp_objects.end());
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(built_manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK) {
    const std::string message =
        "PRESERVE: standby transfer manifest validation failed status=" +
        transfer_status_name(validation_status) +
        " epoch=" + built_manifest.epoch_id +
        " token=" + std::to_string(built_manifest.token) +
        " objects=" + std::to_string(built_manifest.objects.size());
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return validation_status;
  }

  *manifest = std::move(built_manifest);
  *objects = std::move(built_objects);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_build_portable_objects(
    const std::string &epoch_id, const Preserved_trx_bundle &bundle,
    uint64_t transfer_token, Preserve_trx_transfer_manifest *manifest,
    std::vector<Preserve_trx_transfer_object_payload> *objects,
    const Preserve_trx_resurrection_index_entry *resurrection_entry) {
  return preserve_trx_transfer_build_portable_objects_impl(
      epoch_id, bundle, transfer_token, manifest, objects, nullptr,
      resurrection_entry);
}

Preserve_trx_transfer_status preserve_trx_transfer_build_frame_sequence(
    const Preserve_trx_transfer_manifest &manifest,
    const std::vector<Preserve_trx_transfer_object_payload> &objects,
    uint32_t chunk_bytes, std::vector<Preserve_trx_transfer_frame> *frames) {
  if (frames == nullptr || chunk_bytes == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (chunk_bytes > kMaxTransferChunkBytes) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  Preserve_trx_transfer_status status =
      validate_manifest_components(manifest, false);
  if (status != Preserve_trx_transfer_status::OK) return status;

  std::string manifest_payload;
  status = preserve_trx_transfer_encode_manifest(manifest, &manifest_payload);
  if (status != Preserve_trx_transfer_status::OK) return status;

  std::vector<Preserve_trx_transfer_frame> out;
  uint64_t sequence = manifest.frame_sequence + 1;

  Preserve_trx_transfer_frame begin;
  begin.type = Preserve_trx_transfer_frame_type::BEGIN;
  begin.sequence = sequence++;
  begin.epoch_id = manifest.epoch_id;
  begin.token = manifest.token;
  begin.manifest_payload = std::move(manifest_payload);
  out.push_back(std::move(begin));

  for (const Preserve_trx_transfer_object_descriptor &descriptor :
       manifest.objects) {
    const Preserve_trx_transfer_object_payload *object_payload = nullptr;
    for (const Preserve_trx_transfer_object_payload &candidate : objects) {
      if (candidate.descriptor.object_id == descriptor.object_id) {
        object_payload = &candidate;
        break;
      }
    }
    if (object_payload == nullptr ||
        object_payload->descriptor.kind != descriptor.kind ||
        object_payload->descriptor.flags != descriptor.flags ||
        object_payload->descriptor.total_size != descriptor.total_size ||
        object_payload->descriptor.digest != descriptor.digest ||
        object_payload->payload.length() != descriptor.total_size ||
        sha256_digest(object_payload->payload) != descriptor.digest) {
      return Preserve_trx_transfer_status::CORRUPT;
    }

    for (uint64_t offset = 0; offset < object_payload->payload.length();
         offset += chunk_bytes) {
      const size_t length = std::min<uint64_t>(
          chunk_bytes, object_payload->payload.length() - offset);
      Preserve_trx_transfer_frame chunk;
      chunk.type = Preserve_trx_transfer_frame_type::OBJECT_CHUNK;
      chunk.sequence = sequence++;
      chunk.epoch_id = manifest.epoch_id;
      chunk.token = manifest.token;
      chunk.object_id = descriptor.object_id;
      chunk.chunk_offset = offset;
      chunk.chunk_payload = object_payload->payload.substr(offset, length);
      out.push_back(std::move(chunk));
    }

    Preserve_trx_transfer_frame seal;
    seal.type = Preserve_trx_transfer_frame_type::SEAL_OBJECT;
    seal.sequence = sequence++;
    seal.epoch_id = manifest.epoch_id;
    seal.token = manifest.token;
    seal.object_id = descriptor.object_id;
    out.push_back(std::move(seal));
  }

  Preserve_trx_transfer_frame commit;
  commit.type = Preserve_trx_transfer_frame_type::COMMIT_EPOCH;
  commit.sequence = sequence++;
  commit.epoch_id = manifest.epoch_id;
  commit.token = manifest.token;
  status = populate_source_commit_proof(&commit);
  if (status != Preserve_trx_transfer_status::OK) return status;
  out.push_back(std::move(commit));

  *frames = std::move(out);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_send_epoch_begin_frames(
    const std::vector<Preserve_trx_transfer_manifest> &manifests,
    Preserve_trx_transfer_encoded_frame_sink *sink, uint64_t *next_sequence) {
  if (sink == nullptr || next_sequence == nullptr || *next_sequence == 0 ||
      manifests.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  const std::string &epoch_id = manifests.front().epoch_id;
  std::set<uint64_t> tokens;
  for (const Preserve_trx_transfer_manifest &manifest : manifests) {
    Preserve_trx_transfer_status status =
        validate_manifest_components(manifest, false);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (manifest.epoch_id != epoch_id ||
        !tokens.insert(manifest.token).second) {
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }
  }

  for (const Preserve_trx_transfer_manifest &manifest : manifests) {
    std::string manifest_payload;
    Preserve_trx_transfer_status status =
        preserve_trx_transfer_encode_manifest(manifest, &manifest_payload);
    if (status != Preserve_trx_transfer_status::OK) return status;

    Preserve_trx_transfer_frame begin;
    begin.type = Preserve_trx_transfer_frame_type::BEGIN;
    begin.sequence = *next_sequence;
    begin.epoch_id = manifest.epoch_id;
    begin.token = manifest.token;
    begin.manifest_payload = std::move(manifest_payload);
    status = send_encoded_transfer_frame(sink, begin);
    if (status != Preserve_trx_transfer_status::OK) return status;
    ++*next_sequence;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_send_epoch_declare_token_frame(
    const std::string &epoch_id, uint64_t transfer_token,
    Preserve_trx_transfer_encoded_frame_sink *sink, uint64_t *next_sequence) {
  if (sink == nullptr || next_sequence == nullptr || *next_sequence == 0 ||
      !transfer_component_safe(epoch_id) || transfer_token == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  Preserve_trx_transfer_frame declare;
  declare.type = Preserve_trx_transfer_frame_type::DECLARE_TOKEN;
  declare.sequence = *next_sequence;
  declare.epoch_id = epoch_id;
  declare.token = transfer_token;
  const Preserve_trx_transfer_status status =
      send_encoded_transfer_frame(sink, declare);
  if (status == Preserve_trx_transfer_status::OK) ++*next_sequence;
  return status;
}

Preserve_trx_transfer_status preserve_trx_transfer_send_epoch_object_frames(
    const Preserve_trx_transfer_manifest &manifest,
    const std::vector<Preserve_trx_transfer_object_payload> &objects,
    uint32_t chunk_bytes, Preserve_trx_transfer_encoded_frame_sink *sink,
    uint64_t *next_sequence) {
  if (sink == nullptr || next_sequence == nullptr || *next_sequence == 0 ||
      chunk_bytes == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  Preserve_trx_transfer_status status =
      validate_manifest_components(manifest, false);
  if (status != Preserve_trx_transfer_status::OK) return status;

  for (const Preserve_trx_transfer_object_descriptor &descriptor :
       manifest.objects) {
    const Preserve_trx_transfer_object_payload *object_payload = nullptr;
    for (const Preserve_trx_transfer_object_payload &candidate : objects) {
      if (candidate.descriptor.object_id == descriptor.object_id) {
        object_payload = &candidate;
        break;
      }
    }
    if (object_payload == nullptr ||
        object_payload->descriptor.kind != descriptor.kind ||
        object_payload->descriptor.flags != descriptor.flags ||
        object_payload->descriptor.total_size != descriptor.total_size ||
        object_payload->descriptor.digest != descriptor.digest ||
        object_payload->payload.length() != descriptor.total_size ||
        sha256_digest(object_payload->payload) != descriptor.digest) {
      return Preserve_trx_transfer_status::CORRUPT;
    }

    for (uint64_t offset = 0; offset < object_payload->payload.length();
         offset += chunk_bytes) {
      const size_t length = std::min<uint64_t>(
          chunk_bytes, object_payload->payload.length() - offset);
      Preserve_trx_transfer_frame chunk;
      chunk.type = Preserve_trx_transfer_frame_type::OBJECT_CHUNK;
      chunk.sequence = *next_sequence;
      chunk.epoch_id = manifest.epoch_id;
      chunk.token = manifest.token;
      chunk.object_id = descriptor.object_id;
      chunk.chunk_offset = offset;
      chunk.chunk_payload = object_payload->payload.substr(offset, length);
      status = send_encoded_transfer_frame(sink, chunk);
      if (status != Preserve_trx_transfer_status::OK) return status;
      ++*next_sequence;
    }

    Preserve_trx_transfer_frame seal;
    seal.type = Preserve_trx_transfer_frame_type::SEAL_OBJECT;
    seal.sequence = *next_sequence;
    seal.epoch_id = manifest.epoch_id;
    seal.token = manifest.token;
    seal.object_id = descriptor.object_id;
    status = send_encoded_transfer_frame(sink, seal);
    if (status != Preserve_trx_transfer_status::OK) return status;
    ++*next_sequence;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_send_epoch_commit_frame(
    const std::string &epoch_id, uint64_t token,
    Preserve_trx_transfer_encoded_frame_sink *sink, uint64_t *next_sequence) {
  if (sink == nullptr || next_sequence == nullptr || *next_sequence == 0 ||
      !transfer_component_safe(epoch_id) || token == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  Preserve_trx_transfer_frame commit;
  commit.type = Preserve_trx_transfer_frame_type::COMMIT_EPOCH;
  commit.sequence = *next_sequence;
  commit.epoch_id = epoch_id;
  commit.token = token;
  Preserve_trx_transfer_status status = populate_source_commit_proof(&commit);
  if (status != Preserve_trx_transfer_status::OK) return status;
  status = send_encoded_transfer_frame(sink, commit);
  if (status == Preserve_trx_transfer_status::OK) ++*next_sequence;
  return status;
}

Preserve_trx_transfer_source_epoch_session::
    Preserve_trx_transfer_source_epoch_session(
        const std::string &epoch_id, uint32_t chunk_bytes,
        Preserve_trx_transfer_encoded_frame_sink *sink)
    : m_epoch_id(epoch_id),
      m_runtime_policy(preserve_trx_transfer_current_runtime_policy()),
      m_chunk_bytes(chunk_bytes),
      m_max_inflight_bytes(preserve_trx_transfer_max_inflight_bytes),
      m_phase1_batch_bytes(m_runtime_policy.phase1_batch_bytes),
      m_sink(sink),
      m_source_process_generation(transfer_source_epoch_boot_nonce()) {}

Preserve_trx_transfer_source_epoch_session::
    Preserve_trx_transfer_source_epoch_session(
        const std::string &epoch_id,
        const Preserve_trx_transfer_source_epoch_options &options,
        Preserve_trx_transfer_encoded_frame_sink *sink)
    : m_epoch_id(epoch_id),
      m_runtime_policy(options.runtime_policy),
      m_chunk_bytes(options.chunk_bytes),
      m_max_inflight_bytes(options.max_inflight_bytes),
      m_phase1_batch_bytes(options.phase1_batch_bytes),
      m_sink(sink),
      m_before_commit_send(options.before_commit_send),
      m_before_commit_send_context(options.before_commit_send_context),
      m_final_ack_arbiter(options.final_ack_arbiter),
      m_final_ack_arbiter_context(options.final_ack_arbiter_context),
      m_source_process_generation(transfer_source_epoch_boot_nonce()) {}

Preserve_trx_handoff_resolution_context
Preserve_trx_transfer_source_epoch_session::handoff_resolution_context()
    const {
  std::lock_guard<std::mutex> guard(m_mutex);
  Preserve_trx_handoff_resolution_context context;
  context.epoch_id = m_epoch_id;
  context.final_fact_digest = m_terminal_fact_digest;
  context.source_process_generation = m_source_process_generation;
  context.receiver_process_generation = m_receiver_process_nonce;
  return context;
}

bool Preserve_trx_transfer_source_epoch_session::token_declared(
    uint64_t transfer_token) const {
  return m_declared_tokens.count(transfer_token) != 0;
}

bool Preserve_trx_transfer_source_epoch_session::token_resolved(
    uint64_t transfer_token) const {
  return m_finalized_tokens.count(transfer_token) != 0 ||
         m_aborted_tokens.count(transfer_token) != 0;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::open_epoch(
    uint64_t requested_terminal_status_retention_us,
    uint64_t absolute_monotonic_deadline_us) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_epoch_committed || m_commit_in_progress ||
      m_next_sequence != 1 || !m_declared_tokens.empty() ||
      requested_terminal_status_retention_us == 0 ||
      absolute_monotonic_deadline_us <= transfer_monotonic_us()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  Preserve_trx_temp_id_contract requested_contract;
  if (!preserve_trx_temp_id_local_contract(&requested_contract)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (m_epoch_transport_open) {
    return m_temp_id_contract == requested_contract &&
                   m_requested_terminal_status_retention_us ==
                       requested_terminal_status_retention_us &&
                   m_absolute_monotonic_deadline_us ==
                       absolute_monotonic_deadline_us
               ? Preserve_trx_transfer_status::OK
               : Preserve_trx_transfer_status::CORRUPT;
  }
  std::string receiver_process_nonce;
  uint64_t accepted_terminal_status_retention_us = 0;
  const Preserve_trx_transfer_status status = m_sink->open_epoch_transport(
      m_epoch_id, requested_terminal_status_retention_us,
      absolute_monotonic_deadline_us, &receiver_process_nonce,
      &accepted_terminal_status_retention_us);
  if (status != Preserve_trx_transfer_status::OK) return status;
  const auto accepted_contract = m_sink->negotiated_temp_id_contract();
  if (accepted_contract != requested_contract ||
      receiver_process_nonce.length() != 32 ||
      !transfer_component_safe(receiver_process_nonce) ||
      accepted_terminal_status_retention_us <
          requested_terminal_status_retention_us) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  m_requested_terminal_status_retention_us =
      requested_terminal_status_retention_us;
  m_absolute_monotonic_deadline_us = absolute_monotonic_deadline_us;
  m_receiver_process_nonce = std::move(receiver_process_nonce);
  m_accepted_terminal_status_retention_us =
      accepted_terminal_status_retention_us;
  m_temp_id_contract = accepted_contract;
  m_epoch_transport_open = true;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_temp_id_contract Preserve_trx_transfer_source_epoch_session::
    negotiated_temp_id_contract() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_temp_id_contract;
}

void Preserve_trx_transfer_source_epoch_session::
    stamp_online_epoch_context_locked(
        Preserve_trx_transfer_frame *frame) const {
  if (frame != nullptr && m_epoch_transport_open) {
    frame->receiver_process_nonce = m_receiver_process_nonce;
  }
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::emit_frame_locked(
    Preserve_trx_transfer_frame frame, bool queue_final_metadata) {
  if (m_sink == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  if (m_commit_in_progress) return Preserve_trx_transfer_status::UNSUPPORTED;
  if (m_ack_uncertain &&
      frame.type != Preserve_trx_transfer_frame_type::ABORT) {
    return Preserve_trx_transfer_status::ACK_UNCERTAIN;
  }
  stamp_online_epoch_context_locked(&frame);
  frame.sequence = m_next_sequence;
  if (queue_final_metadata) {
    m_pending_final_metadata_frames.push_back(std::move(frame));
    ++m_next_sequence;
    return Preserve_trx_transfer_status::OK;
  }
  DBUG_EXECUTE_IF("preserve_trx_early_force_second_dirty", {
    if (frame.type == Preserve_trx_transfer_frame_type::DECLARE_OBJECT &&
        frame.object_id == kPreservedTrxBlobRecordLocks) {
      const auto *pending = m_pending_final_metadata_frames.empty()
                                ? nullptr
                                : &m_pending_final_metadata_frames.front();
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE_LATE_DIRTY_TEST declare token=" +
              std::to_string(frame.token) +
              " sequence=" + std::to_string(frame.sequence) +
              " pending_count=" +
              std::to_string(m_pending_final_metadata_frames.size()) +
              " pending_first_sequence=" +
              std::to_string(pending == nullptr ? 0 : pending->sequence) +
              " pending_first_token=" +
              std::to_string(pending == nullptr ? 0 : pending->token))
                 .c_str());
    }
  });
  std::string encoded_frame;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_encode_frame(frame, &encoded_frame);
  if (status != Preserve_trx_transfer_status::OK) return status;
  status = m_sink->send_encoded_frame(encoded_frame);
  if (status == Preserve_trx_transfer_status::ACK_UNCERTAIN) {
    m_ack_uncertain = true;
  }
  if (status == Preserve_trx_transfer_status::OK) {
    ++m_next_sequence;
    if (m_phase1_metrics_enabled) {
      note_source_phase1_network_send(1, encoded_frame.length(), 1, false);
    }
  }
  return status;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::send_phase1_control_batches_locked(
    const std::vector<std::string> &encoded_frames,
    size_t *acknowledged_frame_count) {
  if (m_sink == nullptr || encoded_frames.empty() ||
      acknowledged_frame_count == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_ack_uncertain) return Preserve_trx_transfer_status::ACK_UNCERTAIN;
  *acknowledged_frame_count = 0;
  const uint64_t batch_overhead =
      kTransferFrameBatchMagicLength + sizeof(uint16_t) + sizeof(uint32_t);
  size_t first = 0;
  while (first < encoded_frames.size()) {
    size_t last = first;
    uint64_t encoded_bytes = batch_overhead;
    while (last < encoded_frames.size()) {
      const uint64_t frame_bytes =
          sizeof(uint64_t) + encoded_frames[last].length();
      if (frame_bytes > std::numeric_limits<uint64_t>::max() - encoded_bytes) {
        return Preserve_trx_transfer_status::UNSUPPORTED;
      }
      const uint64_t next_bytes = encoded_bytes + frame_bytes;
      if (last != first &&
          (m_phase1_batch_bytes == 0 || next_bytes > m_phase1_batch_bytes)) {
        break;
      }
      encoded_bytes = next_bytes;
      ++last;
      if (m_phase1_batch_bytes == 0 ||
          encoded_bytes >= m_phase1_batch_bytes) {
        break;
      }
    }
    std::string encoded_batch;
    Preserve_trx_transfer_status status = encode_frame_batch_with_limit(
        encoded_frames.data() + first, last - first, m_max_inflight_bytes,
        &encoded_batch);
    if (status != Preserve_trx_transfer_status::OK) return status;
    status = m_sink->send_encoded_frame(encoded_batch);
    if (status != Preserve_trx_transfer_status::OK) {
      if (status == Preserve_trx_transfer_status::ACK_UNCERTAIN) {
        m_ack_uncertain = true;
      }
      return status;
    }
    *acknowledged_frame_count += last - first;
    // Telemetry may allocate. Preserve ACK progress before it can throw.
    if (m_phase1_metrics_enabled) {
      note_source_phase1_network_send(last - first, encoded_batch.length(),
                                     last - first, true);
    }
    first = last;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::declare_token(
    uint64_t transfer_token) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_chunk_bytes == 0 ||
      !transfer_component_safe(m_epoch_id) || transfer_token == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_chunk_bytes > kMaxTransferChunkBytes) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (m_epoch_committed || m_commit_in_progress)
    return Preserve_trx_transfer_status::UNSUPPORTED;
  if (token_declared(transfer_token) || token_resolved(transfer_token)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserve_trx_transfer_frame declare;
  declare.type = Preserve_trx_transfer_frame_type::DECLARE_TOKEN;
  declare.epoch_id = m_epoch_id;
  declare.token = transfer_token;
  Preserve_trx_transfer_status status =
      emit_frame_locked(std::move(declare), false);
  if (status != Preserve_trx_transfer_status::OK) return status;
  m_declared_tokens.insert(transfer_token);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::declare_tokens_batch(
    const std::vector<uint64_t> &transfer_tokens) {
  if (transfer_tokens.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_chunk_bytes == 0 ||
      !transfer_component_safe(m_epoch_id) || m_epoch_committed ||
      m_commit_in_progress) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_chunk_bytes > kMaxTransferChunkBytes) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  std::set<uint64_t> unique_tokens;
  std::vector<uint64_t> ordered_tokens;
  std::vector<std::string> encoded_frames;
  ordered_tokens.reserve(transfer_tokens.size());
  encoded_frames.reserve(transfer_tokens.size());
  const uint64_t first_sequence = m_next_sequence;
  for (uint64_t transfer_token : transfer_tokens) {
    if (transfer_token == 0 || !unique_tokens.insert(transfer_token).second ||
        token_declared(transfer_token) || token_resolved(transfer_token)) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    ordered_tokens.push_back(transfer_token);
    Preserve_trx_transfer_frame declare;
    declare.type = Preserve_trx_transfer_frame_type::DECLARE_TOKEN;
    declare.sequence = m_next_sequence++;
    declare.epoch_id = m_epoch_id;
    declare.token = transfer_token;
    stamp_online_epoch_context_locked(&declare);
    std::string encoded_frame;
    Preserve_trx_transfer_status status =
        preserve_trx_transfer_encode_frame(declare, &encoded_frame);
    if (status != Preserve_trx_transfer_status::OK) return status;
    encoded_frames.push_back(std::move(encoded_frame));
  }

  size_t acknowledged_frame_count = 0;
  Preserve_trx_transfer_status status = send_phase1_control_batches_locked(
      encoded_frames, &acknowledged_frame_count);
  m_next_sequence = first_sequence + acknowledged_frame_count;
  m_declared_tokens.insert(
      ordered_tokens.begin(),
      ordered_tokens.begin() + acknowledged_frame_count);
  if (status != Preserve_trx_transfer_status::OK) {
    return status;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::declare_object(
    uint64_t transfer_token,
    const Preserve_trx_transfer_object_descriptor &descriptor,
    uint64_t preserved_prefix_size,
    const std::array<unsigned char, kPreservedTrxSha256Length>
        *preserved_prefix_digest) {
  std::lock_guard<std::mutex> guard(m_mutex);
  const bool append_sealed_binlog_prefix =
      preserved_prefix_size != 0 || preserved_prefix_digest != nullptr;
  if (m_sink == nullptr || transfer_token == 0 ||
      !transfer_component_safe(descriptor.object_id) ||
      !transfer_lock_plan_contract_valid(descriptor) ||
      (append_sealed_binlog_prefix &&
       (descriptor.object_id != kPreservedTrxBlobBinlogCache ||
        descriptor.kind !=
            Preserve_trx_transfer_object_kind::EXTERNAL_BLOB ||
        preserved_prefix_digest == nullptr ||
        preserved_prefix_size == 0 ||
        preserved_prefix_size >= descriptor.total_size))) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed || m_commit_in_progress ||
      !token_declared(transfer_token) ||
      token_resolved(transfer_token) ||
      m_streaming_manifests.count(transfer_token) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  auto &objects = m_streaming_declared_objects[transfer_token];
  auto existing = objects.find(descriptor.object_id);
  if (append_sealed_binlog_prefix &&
      (existing == objects.end() ||
       existing->second.total_size != preserved_prefix_size ||
       existing->second.digest != *preserved_prefix_digest ||
       m_streaming_sealed_objects[transfer_token].count(
           descriptor.object_id) == 0 ||
       m_streaming_object_written_bytes[transfer_token]
                                       [descriptor.object_id] !=
           preserved_prefix_size)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (existing != objects.end() &&
      transfer_object_descriptor_equal(existing->second, descriptor)) {
    return Preserve_trx_transfer_status::OK;
  }
  if (existing != objects.end()) {
    const Preserve_trx_transfer_status replacement_status =
        transfer_lock_plan_replacement_status(existing->second, descriptor);
    if (replacement_status != Preserve_trx_transfer_status::OK) {
      return replacement_status;
    }
  }

  std::string descriptor_payload;
  Preserve_trx_transfer_status status =
      encode_transfer_object_descriptor(descriptor, &descriptor_payload);
  if (status != Preserve_trx_transfer_status::OK) return status;
  Preserve_trx_transfer_frame declare;
  declare.type = Preserve_trx_transfer_frame_type::DECLARE_OBJECT;
  declare.epoch_id = m_epoch_id;
  declare.token = transfer_token;
  declare.object_id = descriptor.object_id;
  declare.manifest_payload = std::move(descriptor_payload);
  if (append_sealed_binlog_prefix) {
    declare.chunk_offset = preserved_prefix_size;
    declare.reason = encode_binlog_append_prefix_reason(
        preserved_prefix_size, *preserved_prefix_digest);
  }
  status = emit_frame_locked(std::move(declare), false);
  if (status != Preserve_trx_transfer_status::OK) return status;
  if (existing != objects.end()) {
    existing->second = descriptor;
    m_streaming_sealed_objects[transfer_token].erase(descriptor.object_id);
  } else {
    objects.emplace(descriptor.object_id, descriptor);
  }
  m_streaming_object_written_bytes[transfer_token][descriptor.object_id] =
      append_sealed_binlog_prefix ? preserved_prefix_size : 0;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::begin_token_objects(
    const Preserve_trx_transfer_manifest &manifest, bool queue_final_metadata) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_chunk_bytes == 0 || manifest.token == 0 ||
      manifest.epoch_id != m_epoch_id) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed || m_commit_in_progress ||
      !token_declared(manifest.token) ||
      token_resolved(manifest.token) ||
      m_streaming_manifests.count(manifest.token) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserve_trx_transfer_status status = validate_manifest_components(
      manifest, false);
  if (status != Preserve_trx_transfer_status::OK) return status;

  const auto declared_it = m_streaming_declared_objects.find(manifest.token);
  if (declared_it != m_streaming_declared_objects.end()) {
    size_t matched = 0;
    for (const auto &descriptor : manifest.objects) {
      if (descriptor.object_id == kBinlogPrewarmSeedObjectId) continue;
      const auto declared = declared_it->second.find(descriptor.object_id);
      if (declared == declared_it->second.end()) continue;
      if (!transfer_object_descriptor_equal(descriptor, declared->second)) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      ++matched;
    }
    if (matched != declared_it->second.size() -
                       declared_it->second.count(kBinlogPrewarmSeedObjectId)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }

  std::string manifest_payload;
  status = preserve_trx_transfer_encode_manifest(manifest, &manifest_payload);
  if (status != Preserve_trx_transfer_status::OK) return status;
  Preserve_trx_transfer_frame begin;
  begin.type = Preserve_trx_transfer_frame_type::BEGIN;
  begin.epoch_id = manifest.epoch_id;
  begin.token = manifest.token;
  begin.manifest_payload = std::move(manifest_payload);
  status = emit_frame_locked(std::move(begin), queue_final_metadata);
  if (status != Preserve_trx_transfer_status::OK) return status;
  if (queue_final_metadata) {
    m_final_metadata_tokens.insert(manifest.token);
  } else {
    m_final_metadata_tokens.erase(manifest.token);
  }

  m_streaming_declared_objects[manifest.token].erase(
      kBinlogPrewarmSeedObjectId);
  m_streaming_object_written_bytes[manifest.token].erase(
      kBinlogPrewarmSeedObjectId);
  m_streaming_sealed_objects[manifest.token].erase(
      kBinlogPrewarmSeedObjectId);
  std::map<std::string, uint64_t> written_bytes =
      m_streaming_object_written_bytes[manifest.token];
  for (const Preserve_trx_transfer_object_descriptor &descriptor :
       manifest.objects) {
    if (written_bytes.count(descriptor.object_id) == 0) {
      written_bytes[descriptor.object_id] = 0;
    }
  }
  m_streaming_manifests[manifest.token] = manifest;
  m_streaming_object_written_bytes[manifest.token] = std::move(written_bytes);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::begin_token_prewarm_manifest(
    uint64_t transfer_token, uint64_t required_source_freeze_lsn) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_chunk_bytes == 0 || transfer_token == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed || m_commit_in_progress ||
      !token_declared(transfer_token) ||
      token_resolved(transfer_token) ||
      m_streaming_manifests.count(transfer_token) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  const bool prewarm_manifest_started =
      m_prewarm_manifest_tokens.count(transfer_token) != 0;
  if (prewarm_manifest_started && required_source_freeze_lsn == 0) {
    return Preserve_trx_transfer_status::OK;
  }

  const auto declared_it = m_streaming_declared_objects.find(transfer_token);
  if (declared_it == m_streaming_declared_objects.end() ||
      declared_it->second.empty()) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserve_trx_transfer_manifest manifest;
  manifest.epoch_id = m_epoch_id;
  manifest.token = transfer_token;
  if (prewarm_manifest_started) {
    const auto lsn_fact = m_prewarm_lsn_facts.find(transfer_token);
    if (lsn_fact == m_prewarm_lsn_facts.end()) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    manifest.source_freeze_lsn = lsn_fact->second.first;
    manifest.source_epoch_commit_lsn = lsn_fact->second.second;
  } else {
    if (!load_source_transfer_lsn_fact(&manifest.source_freeze_lsn,
                                       &manifest.source_epoch_commit_lsn)) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
  }
  if (required_source_freeze_lsn != 0) {
    manifest.source_freeze_lsn = required_source_freeze_lsn;
    manifest.source_epoch_commit_lsn =
        std::max(manifest.source_epoch_commit_lsn,
                 required_source_freeze_lsn);
  }
  if (prewarm_manifest_started &&
      m_prewarm_lsn_facts[transfer_token] ==
          std::make_pair(manifest.source_freeze_lsn,
                         manifest.source_epoch_commit_lsn)) {
    return Preserve_trx_transfer_status::OK;
  }
  manifest.objects.reserve(declared_it->second.size());
  for (const auto &entry : declared_it->second) {
    manifest.objects.push_back(entry.second);
  }

  Preserve_trx_transfer_status status =
      validate_manifest_components(manifest, false);
  if (status != Preserve_trx_transfer_status::OK) return status;

  std::string manifest_payload;
  status = preserve_trx_transfer_encode_manifest(manifest, &manifest_payload);
  if (status != Preserve_trx_transfer_status::OK) return status;

  Preserve_trx_transfer_frame begin;
  begin.type = Preserve_trx_transfer_frame_type::BEGIN;
  begin.epoch_id = manifest.epoch_id;
  begin.token = manifest.token;
  begin.manifest_payload = std::move(manifest_payload);
  status = emit_frame_locked(std::move(begin), false);
  if (status != Preserve_trx_transfer_status::OK) return status;

  m_prewarm_manifest_tokens.insert(transfer_token);
  m_prewarm_lsn_facts[transfer_token] = {
      manifest.source_freeze_lsn, manifest.source_epoch_commit_lsn};
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::begin_token_prewarm_manifests_batch(
    const std::vector<uint64_t> &transfer_tokens) {
  if (transfer_tokens.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_chunk_bytes == 0 || m_epoch_committed ||
      m_commit_in_progress) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::vector<std::string> encoded_frames;
  std::vector<uint64_t> newly_started;
  std::vector<std::pair<uint64_t, uint64_t>> newly_started_lsn_facts;
  encoded_frames.reserve(transfer_tokens.size());
  newly_started.reserve(transfer_tokens.size());
  newly_started_lsn_facts.reserve(transfer_tokens.size());
  const uint64_t first_sequence = m_next_sequence;
  uint64_t next_sequence = first_sequence;
  for (uint64_t transfer_token : transfer_tokens) {
    if (transfer_token == 0 || !token_declared(transfer_token) ||
        token_resolved(transfer_token) ||
        m_streaming_manifests.count(transfer_token) != 0) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    if (m_prewarm_manifest_tokens.count(transfer_token) != 0) continue;
    const auto declared_it = m_streaming_declared_objects.find(transfer_token);
    if (declared_it == m_streaming_declared_objects.end() ||
        declared_it->second.empty()) {
      continue;
    }

    Preserve_trx_transfer_manifest manifest;
    manifest.epoch_id = m_epoch_id;
    manifest.token = transfer_token;
    if (!load_source_transfer_lsn_fact(&manifest.source_freeze_lsn,
                                       &manifest.source_epoch_commit_lsn)) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    manifest.objects.reserve(declared_it->second.size());
    for (const auto &entry : declared_it->second) {
      manifest.objects.push_back(entry.second);
    }
    Preserve_trx_transfer_status status =
        validate_manifest_components(manifest, false);
    if (status != Preserve_trx_transfer_status::OK) return status;
    std::string manifest_payload;
    status = preserve_trx_transfer_encode_manifest(manifest, &manifest_payload);
    if (status != Preserve_trx_transfer_status::OK) return status;
    Preserve_trx_transfer_frame begin;
    begin.type = Preserve_trx_transfer_frame_type::BEGIN;
    begin.sequence = next_sequence++;
    begin.epoch_id = manifest.epoch_id;
    begin.token = manifest.token;
    begin.manifest_payload = std::move(manifest_payload);
    stamp_online_epoch_context_locked(&begin);
    std::string encoded_frame;
    status = preserve_trx_transfer_encode_frame(begin, &encoded_frame);
    if (status != Preserve_trx_transfer_status::OK) return status;
    encoded_frames.push_back(std::move(encoded_frame));
    newly_started.push_back(transfer_token);
    newly_started_lsn_facts.push_back(
        {manifest.source_freeze_lsn, manifest.source_epoch_commit_lsn});
  }
  if (encoded_frames.empty()) return Preserve_trx_transfer_status::OK;

  size_t acknowledged_frame_count = 0;
  Preserve_trx_transfer_status status = send_phase1_control_batches_locked(
      encoded_frames, &acknowledged_frame_count);
  m_next_sequence = first_sequence + acknowledged_frame_count;
  m_prewarm_manifest_tokens.insert(
      newly_started.begin(),
      newly_started.begin() + acknowledged_frame_count);
  for (size_t index = 0; index < acknowledged_frame_count; ++index) {
    m_prewarm_lsn_facts[newly_started[index]] =
        newly_started_lsn_facts[index];
  }
  if (status != Preserve_trx_transfer_status::OK) return status;
  return Preserve_trx_transfer_status::OK;
}

bool Preserve_trx_transfer_source_epoch_session::token_prewarm_lsn_fact(
    uint64_t transfer_token, uint64_t *source_freeze_lsn,
    uint64_t *source_epoch_commit_lsn) const {
  if (source_freeze_lsn == nullptr || source_epoch_commit_lsn == nullptr) {
    return false;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_prewarm_lsn_facts.find(transfer_token);
  if (found == m_prewarm_lsn_facts.end()) return false;
  *source_freeze_lsn = found->second.first;
  *source_epoch_commit_lsn = found->second.second;
  return *source_freeze_lsn != 0 && *source_epoch_commit_lsn != 0;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::write_object_chunk(
    uint64_t transfer_token, const std::string &object_id,
    uint64_t chunk_offset, const std::string &chunk_payload) {
  /* Source serialization state is not locked while the rate budget waits. */
  throttle_source_transfer_io(chunk_payload.length(), m_runtime_policy);
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || transfer_token == 0 || object_id.empty() ||
      chunk_payload.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  auto manifest_it = m_streaming_manifests.find(transfer_token);
  if (m_epoch_committed || m_commit_in_progress ||
      token_resolved(transfer_token) ||
      m_streaming_sealed_objects[transfer_token].count(object_id) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  const Preserve_trx_transfer_object_descriptor *descriptor = nullptr;
  if (manifest_it != m_streaming_manifests.end()) {
    descriptor = find_object(manifest_it->second, object_id);
  } else {
    const auto declared_token = m_streaming_declared_objects.find(transfer_token);
    if (declared_token != m_streaming_declared_objects.end()) {
      const auto declared_object = declared_token->second.find(object_id);
      if (declared_object != declared_token->second.end()) {
        descriptor = &declared_object->second;
      }
    }
  }
  if (descriptor == nullptr) return Preserve_trx_transfer_status::CORRUPT;

  uint64_t &written =
      m_streaming_object_written_bytes[transfer_token][object_id];
  if (chunk_offset != written ||
      chunk_payload.length() > descriptor->total_size - written) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  Preserve_trx_transfer_frame chunk;
  chunk.type = Preserve_trx_transfer_frame_type::OBJECT_CHUNK;
  chunk.epoch_id = m_epoch_id;
  chunk.token = transfer_token;
  chunk.object_id = object_id;
  chunk.chunk_offset = chunk_offset;
  chunk.chunk_payload = chunk_payload;
  const Preserve_trx_transfer_status status =
      emit_frame_locked(std::move(chunk),
                        m_final_metadata_tokens.count(transfer_token) != 0);
  if (status != Preserve_trx_transfer_status::OK) return status;
  written += chunk_payload.length();
  return Preserve_trx_transfer_status::OK;
}





Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::seal_object(
    uint64_t transfer_token, const std::string &object_id) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || transfer_token == 0 || object_id.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  auto manifest_it = m_streaming_manifests.find(transfer_token);
  if (m_epoch_committed || m_commit_in_progress ||
      token_resolved(transfer_token) ||
      m_streaming_sealed_objects[transfer_token].count(object_id) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  const Preserve_trx_transfer_object_descriptor *descriptor = nullptr;
  if (manifest_it != m_streaming_manifests.end()) {
    descriptor = find_object(manifest_it->second, object_id);
  } else {
    const auto declared_token = m_streaming_declared_objects.find(transfer_token);
    if (declared_token != m_streaming_declared_objects.end()) {
      const auto declared_object = declared_token->second.find(object_id);
      if (declared_object != declared_token->second.end()) {
        descriptor = &declared_object->second;
      }
    }
  }
  if (descriptor == nullptr) return Preserve_trx_transfer_status::CORRUPT;

  const uint64_t written =
      m_streaming_object_written_bytes[transfer_token][object_id];
  if (written != descriptor->total_size) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  Preserve_trx_transfer_frame seal;
  seal.type = Preserve_trx_transfer_frame_type::SEAL_OBJECT;
  seal.epoch_id = m_epoch_id;
  seal.token = transfer_token;
  seal.object_id = object_id;
  const Preserve_trx_transfer_status status =
      emit_frame_locked(std::move(seal),
                        m_final_metadata_tokens.count(transfer_token) != 0);
  if (status != Preserve_trx_transfer_status::OK) return status;
  m_streaming_sealed_objects[transfer_token].insert(object_id);
  return Preserve_trx_transfer_status::OK;
}

bool Preserve_trx_transfer_source_epoch_session::object_presealed_for_token(
    uint64_t transfer_token,
    const Preserve_trx_transfer_object_descriptor &descriptor) const {
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto declared_token = m_streaming_declared_objects.find(transfer_token);
  if (declared_token == m_streaming_declared_objects.end()) return false;
  const auto declared_object =
      declared_token->second.find(descriptor.object_id);
  if (declared_object == declared_token->second.end()) return false;
  if (!transfer_object_descriptor_equal(declared_object->second, descriptor)) {
    return false;
  }
  const auto sealed_token = m_streaming_sealed_objects.find(transfer_token);
  if (sealed_token == m_streaming_sealed_objects.end() ||
      sealed_token->second.count(descriptor.object_id) == 0) {
    return false;
  }
  const auto written_token =
      m_streaming_object_written_bytes.find(transfer_token);
  if (written_token == m_streaming_object_written_bytes.end()) return false;
  const auto written_object = written_token->second.find(descriptor.object_id);
  return written_object != written_token->second.end() &&
         written_object->second == descriptor.total_size;
}

uint64_t Preserve_trx_transfer_source_epoch_session::
    presealed_object_source_live_generation(
        uint64_t transfer_token, const std::string &object_id) const {
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto declared_token = m_streaming_declared_objects.find(transfer_token);
  if (declared_token == m_streaming_declared_objects.end()) return 0;
  const auto declared_object = declared_token->second.find(object_id);
  if (declared_object == declared_token->second.end()) return 0;
  const auto sealed_token = m_streaming_sealed_objects.find(transfer_token);
  if (sealed_token == m_streaming_sealed_objects.end() ||
      sealed_token->second.count(object_id) == 0) {
    return 0;
  }
  return declared_object->second.lock_plan.source_live_generation;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::finalize_token_manifest(
    uint64_t transfer_token) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (transfer_token == 0) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  auto manifest_it = m_streaming_manifests.find(transfer_token);
  if (m_epoch_committed || m_commit_in_progress ||
      token_resolved(transfer_token) ||
      manifest_it == m_streaming_manifests.end()) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  const std::set<std::string> &sealed =
      m_streaming_sealed_objects[transfer_token];
  for (const Preserve_trx_transfer_object_descriptor &descriptor :
       manifest_it->second.objects) {
    if (sealed.count(descriptor.object_id) == 0) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }

  m_finalized_tokens.insert(transfer_token);
  m_finalized_manifests.push_back(manifest_it->second);
  m_streaming_manifests.erase(transfer_token);
  m_streaming_declared_objects.erase(transfer_token);
  m_streaming_object_written_bytes.erase(transfer_token);
  m_streaming_sealed_objects.erase(transfer_token);
  m_prewarm_lsn_facts.erase(transfer_token);
  m_final_metadata_tokens.erase(transfer_token);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::send_token_objects_locked(
    const Preserve_trx_transfer_manifest &manifest,
    const std::vector<Preserve_trx_transfer_object_payload> &objects,
    bool queue_final_metadata) {
  if (m_sink == nullptr || m_chunk_bytes == 0 || manifest.token == 0 ||
      manifest.epoch_id != m_epoch_id) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed || m_commit_in_progress)
    return Preserve_trx_transfer_status::UNSUPPORTED;
  if (!token_declared(manifest.token) || token_resolved(manifest.token) ||
      m_streaming_manifests.count(manifest.token) != 0 ||
      m_streaming_declared_objects.count(manifest.token) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserve_trx_transfer_status status =
      validate_manifest_components(manifest, false);
  if (status != Preserve_trx_transfer_status::OK) return status;

  std::vector<Preserve_trx_transfer_frame> frames;
  frames.reserve(manifest.objects.size() + 1);

  std::string manifest_payload;
  status = preserve_trx_transfer_encode_manifest(manifest, &manifest_payload);
  if (status != Preserve_trx_transfer_status::OK) return status;

  Preserve_trx_transfer_frame begin;
  begin.type = Preserve_trx_transfer_frame_type::BEGIN;
  begin.epoch_id = manifest.epoch_id;
  begin.token = manifest.token;
  begin.manifest_payload = std::move(manifest_payload);
  frames.push_back(std::move(begin));

  for (const Preserve_trx_transfer_object_descriptor &descriptor :
       manifest.objects) {
    const Preserve_trx_transfer_object_payload *object_payload = nullptr;
    for (const Preserve_trx_transfer_object_payload &candidate : objects) {
      if (candidate.descriptor.object_id == descriptor.object_id) {
        object_payload = &candidate;
        break;
      }
    }
    if (object_payload == nullptr ||
        object_payload->descriptor.kind != descriptor.kind ||
        object_payload->descriptor.flags != descriptor.flags ||
        object_payload->descriptor.total_size != descriptor.total_size ||
        object_payload->descriptor.digest != descriptor.digest ||
        object_payload->payload.length() != descriptor.total_size ||
        sha256_digest(object_payload->payload) != descriptor.digest) {
      return Preserve_trx_transfer_status::CORRUPT;
    }

    for (uint64_t offset = 0; offset < object_payload->payload.length();
         offset += m_chunk_bytes) {
      const size_t length = std::min<uint64_t>(
          m_chunk_bytes, object_payload->payload.length() - offset);
      Preserve_trx_transfer_frame chunk;
      chunk.type = Preserve_trx_transfer_frame_type::OBJECT_CHUNK;
      chunk.epoch_id = manifest.epoch_id;
      chunk.token = manifest.token;
      chunk.object_id = descriptor.object_id;
      chunk.chunk_offset = offset;
      chunk.chunk_payload = object_payload->payload.substr(offset, length);
      frames.push_back(std::move(chunk));
    }

    Preserve_trx_transfer_frame seal;
    seal.type = Preserve_trx_transfer_frame_type::SEAL_OBJECT;
    seal.epoch_id = manifest.epoch_id;
    seal.token = manifest.token;
    seal.object_id = descriptor.object_id;
    frames.push_back(std::move(seal));
  }

  for (Preserve_trx_transfer_frame &frame : frames) {
    status = emit_frame_locked(std::move(frame), queue_final_metadata);
    if (status != Preserve_trx_transfer_status::OK) return status;
  }

  m_finalized_tokens.insert(manifest.token);
  m_finalized_manifests.push_back(manifest);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::send_token_objects(
    const Preserve_trx_transfer_manifest &manifest,
    const std::vector<Preserve_trx_transfer_object_payload> &objects) {
  uint64_t payload_bytes = 0;
  for (const auto &object : objects) {
    if (object.payload.length() >
        std::numeric_limits<uint64_t>::max() - payload_bytes) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    payload_bytes += object.payload.length();
  }
  throttle_source_transfer_io(payload_bytes, m_runtime_policy);
  std::lock_guard<std::mutex> guard(m_mutex);
  return send_token_objects_locked(manifest, objects, false);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::send_token_objects_batch(
    const Preserve_trx_transfer_manifest &selected_manifest,
    const std::vector<Preserve_trx_transfer_object_payload> &objects,
    const std::set<std::string> &selected_presealed_objects,
    bool queue_final_metadata) {
  Preserve_trx_transfer_manifest manifest = selected_manifest;
  std::set<std::string> presealed_objects = selected_presealed_objects;
  if (!queue_final_metadata) {
    uint64_t payload_bytes = 0;
    for (const auto &object : objects) {
      if (presealed_objects.count(object.descriptor.object_id) != 0) continue;
      if (object.payload.length() >
          std::numeric_limits<uint64_t>::max() - payload_bytes) {
        return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
      }
      payload_bytes += object.payload.length();
    }
    throttle_source_transfer_io(payload_bytes, m_runtime_policy);
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_chunk_bytes == 0 || manifest.token == 0 ||
      manifest.epoch_id != m_epoch_id) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed || m_commit_in_progress ||
      !token_declared(manifest.token) ||
      token_resolved(manifest.token) ||
      m_streaming_manifests.count(manifest.token) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  // Both final callers pass the selected resources. Complete immutable result
  // generations remain part of the cumulative authenticated transport, even
  // after CLOSE/EOF/re-execute removed them from the final result selection.
  const auto cumulative = m_streaming_declared_objects.find(manifest.token);
  if (cumulative != m_streaming_declared_objects.end()) {
    std::shared_ptr<const Preserve_trx_transfer_object_index> selected;
    auto status = Preserve_trx_transfer_object_index::build(
        manifest.token, manifest.objects, kMaxTransferManifestObjects, &selected);
    if (status != Preserve_trx_transfer_status::OK) return status;
    std::vector<const Preserve_trx_transfer_object_descriptor *> append;
    for (const auto &entry : cumulative->second) {
      const auto &object = entry.second;
      if (object.kind != Preserve_trx_transfer_object_kind::CURSOR_RESULT) continue;
      const auto written = m_streaming_object_written_bytes.find(manifest.token);
      const auto sealed = m_streaming_sealed_objects.find(manifest.token);
      if (written == m_streaming_object_written_bytes.end() ||
          written->second.count(entry.first) == 0 ||
          written->second.at(entry.first) != object.total_size ||
          sealed == m_streaming_sealed_objects.end() ||
          sealed->second.count(entry.first) == 0)
        return Preserve_trx_transfer_status::CORRUPT;
      const auto *existing = selected->find(manifest.objects, entry.first);
      if (existing && !transfer_object_descriptor_equal(*existing, object))
        return Preserve_trx_transfer_status::CORRUPT;
      if (!existing) append.push_back(&object);
      presealed_objects.insert(entry.first);
    }
    if (append.size() > kMaxTransferManifestObjects - manifest.objects.size())
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    for (const auto *object : append) manifest.objects.push_back(*object);
  }
  Preserve_trx_transfer_status status =
      validate_manifest_components(manifest, false);
  if (status != Preserve_trx_transfer_status::OK) return status;

  const auto declared_it = m_streaming_declared_objects.find(manifest.token);
  if (declared_it != m_streaming_declared_objects.end()) {
    size_t required = 0;
    for (const auto &entry : declared_it->second) {
      required += presealed_objects.count(entry.first);
    }
    size_t matched = 0;
    for (const auto &descriptor : manifest.objects) {
      if (presealed_objects.count(descriptor.object_id) == 0) continue;
      const auto declared = declared_it->second.find(descriptor.object_id);
      if (declared == declared_it->second.end()) continue;
      if (!transfer_object_descriptor_equal(descriptor, declared->second)) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      ++matched;
    }
    if (matched != required) return Preserve_trx_transfer_status::CORRUPT;
  }

  // Index pointers only; payloads remain owned by the caller for this call.
  std::vector<const Preserve_trx_transfer_object_payload *> payload_index;
  try {
    payload_index.reserve(objects.size());
    for (const auto &object : objects) payload_index.push_back(&object);
    std::sort(payload_index.begin(), payload_index.end(),
              [](const Preserve_trx_transfer_object_payload *left,
                 const Preserve_trx_transfer_object_payload *right) {
                if (left->descriptor.object_id == right->descriptor.object_id) {
                  // Preserve the first-match behavior for duplicate input IDs.
                  return left < right;
                }
                return left->descriptor.object_id < right->descriptor.object_id;
              });
  } catch (const std::bad_alloc &) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }

  auto object_payload_for =
      [&](const Preserve_trx_transfer_object_descriptor &descriptor)
      -> const Preserve_trx_transfer_object_payload * {
    const auto found = std::lower_bound(
        payload_index.begin(), payload_index.end(), descriptor.object_id,
        [](const Preserve_trx_transfer_object_payload *candidate,
           const std::string &object_id) {
          return candidate->descriptor.object_id < object_id;
        });
    return found != payload_index.end() &&
                   (*found)->descriptor.object_id == descriptor.object_id
               ? *found
               : nullptr;
  };

  auto object_is_presealed =
      [&](const Preserve_trx_transfer_object_descriptor &descriptor) -> bool {
    if (presealed_objects.count(descriptor.object_id) == 0) return false;
    const auto declared_token =
        m_streaming_declared_objects.find(manifest.token);
    if (declared_token == m_streaming_declared_objects.end()) return false;
    const auto declared_object =
        declared_token->second.find(descriptor.object_id);
    if (declared_object == declared_token->second.end() ||
        !transfer_object_descriptor_equal(declared_object->second, descriptor)) {
      return false;
    }
    const auto sealed_token = m_streaming_sealed_objects.find(manifest.token);
    if (sealed_token == m_streaming_sealed_objects.end() ||
        sealed_token->second.count(descriptor.object_id) == 0) {
      return false;
    }
    const auto written_token =
        m_streaming_object_written_bytes.find(manifest.token);
    if (written_token == m_streaming_object_written_bytes.end()) return false;
    const auto written_object =
        written_token->second.find(descriptor.object_id);
    return written_object != written_token->second.end() &&
           written_object->second == descriptor.total_size;
  };

  std::vector<Preserve_trx_transfer_frame> frames;
  frames.reserve(manifest.objects.size() + 1);

  std::string manifest_payload;
  status = preserve_trx_transfer_encode_manifest(manifest, &manifest_payload);
  if (status != Preserve_trx_transfer_status::OK) return status;

  Preserve_trx_transfer_frame begin;
  begin.type = Preserve_trx_transfer_frame_type::BEGIN;
  begin.epoch_id = manifest.epoch_id;
  begin.token = manifest.token;
  begin.manifest_payload = std::move(manifest_payload);
  frames.push_back(std::move(begin));

  uint64_t snapshot_bundle_bytes = 0;
  uint64_t bulk_bytes = 0;
  bool snapshot_bundle_sent = false;
  for (const Preserve_trx_transfer_object_descriptor &descriptor :
       manifest.objects) {
    if (object_is_presealed(descriptor)) continue;

    const Preserve_trx_transfer_object_payload *object_payload =
        object_payload_for(descriptor);
    if (object_payload == nullptr ||
        object_payload->descriptor.kind != descriptor.kind ||
        object_payload->descriptor.flags != descriptor.flags ||
        object_payload->descriptor.total_size != descriptor.total_size ||
        object_payload->descriptor.digest != descriptor.digest ||
        object_payload->payload.length() != descriptor.total_size ||
        sha256_digest(object_payload->payload) != descriptor.digest) {
      return Preserve_trx_transfer_status::CORRUPT;
    }

    for (uint64_t offset = 0; offset < object_payload->payload.length();
         offset += m_chunk_bytes) {
      const size_t length = std::min<uint64_t>(
          m_chunk_bytes, object_payload->payload.length() - offset);
      Preserve_trx_transfer_frame chunk;
      chunk.type = Preserve_trx_transfer_frame_type::OBJECT_CHUNK;
      chunk.epoch_id = manifest.epoch_id;
      chunk.token = manifest.token;
      chunk.object_id = descriptor.object_id;
      chunk.chunk_offset = offset;
      chunk.chunk_payload = object_payload->payload.substr(offset, length);
      frames.push_back(std::move(chunk));

      if (descriptor.kind ==
          Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE) {
        snapshot_bundle_bytes += length;
        snapshot_bundle_sent = true;
      } else if (descriptor.kind ==
                     Preserve_trx_transfer_object_kind::EXTERNAL_BLOB ||
                 descriptor.kind ==
                     Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR) {
        bulk_bytes += length;
      }
    }

    Preserve_trx_transfer_frame seal;
    seal.type = Preserve_trx_transfer_frame_type::SEAL_OBJECT;
    seal.epoch_id = manifest.epoch_id;
    seal.token = manifest.token;
    seal.object_id = descriptor.object_id;
    frames.push_back(std::move(seal));
  }

  if (queue_final_metadata) {
    if (m_ack_uncertain) return Preserve_trx_transfer_status::ACK_UNCERTAIN;
    if (frames.size() >
        std::numeric_limits<uint64_t>::max() - m_next_sequence) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    const size_t original_size = m_pending_final_metadata_frames.size();
    try {
      uint64_t sequence = m_next_sequence;
      for (Preserve_trx_transfer_frame &frame : frames) {
        stamp_online_epoch_context_locked(&frame);
        frame.sequence = sequence++;
      }
      /* Keep amortized growth across tokens; exact reserve here is quadratic. */
      for (Preserve_trx_transfer_frame &frame : frames) {
        m_pending_final_metadata_frames.push_back(std::move(frame));
      }
      m_next_sequence = sequence;
    } catch (...) {
      m_pending_final_metadata_frames.resize(original_size);
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
  } else {
    const uint64_t first_sequence = m_next_sequence;
    auto sequence_guard = create_scope_guard(
        [&] { m_next_sequence = first_sequence; });
    std::vector<std::string> encoded_frames;
    encoded_frames.reserve(frames.size());
    for (Preserve_trx_transfer_frame &frame : frames) {
      stamp_online_epoch_context_locked(&frame);
      frame.sequence = m_next_sequence++;
      std::string encoded_frame;
      status = preserve_trx_transfer_encode_frame(frame, &encoded_frame);
      if (status != Preserve_trx_transfer_status::OK) return status;
      encoded_frames.push_back(std::move(encoded_frame));
    }

    std::string encoded_batch;
    status =
        encode_frame_batch_with_limit(encoded_frames.data(),
                                      encoded_frames.size(), m_max_inflight_bytes,
                                      &encoded_batch);
    if (status != Preserve_trx_transfer_status::OK) return status;
    status = m_sink->send_encoded_frame(encoded_batch);
    if (status != Preserve_trx_transfer_status::OK) return status;
    sequence_guard.commit();
    if (m_phase1_metrics_enabled) {
      note_source_phase1_network_send(encoded_frames.size(),
                                      encoded_batch.length(), 1, true);
    }
  }

  if (snapshot_bundle_bytes != 0) {
    g_transfer_phase2_snapshot_bundle_bytes.fetch_add(snapshot_bundle_bytes);
  }
  if (snapshot_bundle_sent) {
    g_transfer_phase2_snapshot_bundle_count.fetch_add(1);
  }
  if (bulk_bytes != 0) {
    g_transfer_phase2_bulk_bytes.fetch_add(bulk_bytes);
  }
  m_finalized_tokens.insert(manifest.token);
  m_finalized_manifests.push_back(manifest);
  m_streaming_manifests.erase(manifest.token);
  m_streaming_declared_objects.erase(manifest.token);
  m_streaming_object_written_bytes.erase(manifest.token);
  m_streaming_sealed_objects.erase(manifest.token);
  m_prewarm_lsn_facts.erase(manifest.token);
  m_final_metadata_tokens.erase(manifest.token);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::send_token_bundle(
    const Preserved_trx_bundle &bundle, uint64_t transfer_token,
    Preserve_trx_transfer_manifest *manifest) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_chunk_bytes == 0 || transfer_token == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed || m_commit_in_progress)
    return Preserve_trx_transfer_status::UNSUPPORTED;
  if (!token_declared(transfer_token) || token_resolved(transfer_token)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserve_trx_transfer_manifest built_manifest;
  std::vector<Preserve_trx_transfer_object_payload> objects;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_build_portable_objects(
          m_epoch_id, bundle, transfer_token, &built_manifest, &objects);
  if (status != Preserve_trx_transfer_status::OK) return status;

  status = send_token_objects_locked(built_manifest, objects, true);
  if (status != Preserve_trx_transfer_status::OK) return status;
  if (manifest != nullptr) *manifest = std::move(built_manifest);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status stream_prebuilt_external_blob_for_transfer(
    Preserve_trx_transfer_source_epoch_session *session,
    uint64_t transfer_token, const std::string &preserve_dir,
    const std::string &expected_name, const std::string &warmcopy_id,
    uint64_t warmcopy_epoch, uint64_t size,
    const std::array<unsigned char, kPreservedTrxSha256Length> &digest,
    const Preserve_trx_transfer_lock_plan_contract *lock_plan = nullptr,
    uint64_t preserved_prefix_size = 0,
    const std::array<unsigned char, kPreservedTrxSha256Length>
        *preserved_prefix_digest = nullptr) {
  const bool append_sealed_binlog_prefix =
      preserved_prefix_size != 0 || preserved_prefix_digest != nullptr;
  if (session == nullptr || session->chunk_bytes() == 0 ||
      transfer_token == 0 || expected_name.empty() || warmcopy_id.empty() ||
      size == 0 ||
      (append_sealed_binlog_prefix &&
       (expected_name != kPreservedTrxBlobBinlogCache ||
        preserved_prefix_digest == nullptr || preserved_prefix_size == 0 ||
        preserved_prefix_size >= size))) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  Preserve_trx_transfer_object_descriptor descriptor;
  descriptor.object_id = expected_name;
  descriptor.kind = Preserve_trx_transfer_object_kind::EXTERNAL_BLOB;
  descriptor.total_size = size;
  descriptor.digest = digest;
  if (lock_plan != nullptr) descriptor.lock_plan = *lock_plan;
  maybe_attach_simulated_terminal_lock_proof(
      session->epoch_id(), transfer_token, &descriptor);
  if (session->object_presealed_for_token(transfer_token, descriptor)) {
    return Preserve_trx_transfer_status::OK;
  }
  if (append_sealed_binlog_prefix) {
    Preserve_trx_transfer_object_descriptor prefix_descriptor = descriptor;
    prefix_descriptor.total_size = preserved_prefix_size;
    prefix_descriptor.digest = *preserved_prefix_digest;
    if (!session->object_presealed_for_token(transfer_token,
                                             prefix_descriptor)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }

  const std::string source_dir =
      preserve_dir.empty() ? preserved_trx_dir_value() : preserve_dir;
  std::unique_ptr<Preserved_trx_warm_external_blob_carrier> warm_carrier =
      create_preserved_trx_default_warm_external_blob_carrier(source_dir);
  if (warm_carrier == nullptr) return Preserve_trx_transfer_status::IO_ERROR;

  Preserved_trx_external_blob_descriptor warm_descriptor;
  warm_descriptor.name = expected_name;
  warm_descriptor.size = size;
  warm_descriptor.digest = digest;

  const uint64_t max_blob_bytes =
      preserve_trx_transfer_max_inflight_bytes == 0
          ? std::numeric_limits<uint64_t>::max()
          : preserve_trx_transfer_max_inflight_bytes;
  Preserved_trx_external_blob materialized;
  const Preserved_trx_carrier_status carrier_status =
      warm_carrier->read_warm_external_blob(
          warmcopy_id, expected_name, warmcopy_epoch, warm_descriptor,
          max_blob_bytes, &materialized);
  if (carrier_status != Preserved_trx_carrier_status::OK) {
    return map_carrier_status_to_transfer(carrier_status);
  }
  if (materialized.payload.length() != size ||
      materialized.descriptor.name != expected_name ||
      materialized.descriptor.size != size ||
      materialized.descriptor.digest != digest) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  Preserve_trx_transfer_status status =
      session->declare_object(transfer_token, descriptor,
                              preserved_prefix_size,
                              preserved_prefix_digest);
  if (status != Preserve_trx_transfer_status::OK) return status;
  if (expected_name == kPreservedTrxBlobRecordLocks) {
    const Preserve_trx_transfer_status begin_status =
        session->begin_token_prewarm_manifest(transfer_token);
    if (begin_status != Preserve_trx_transfer_status::OK &&
        begin_status != Preserve_trx_transfer_status::UNSUPPORTED) {
      return begin_status;
    }
  }
  for (uint64_t offset = preserved_prefix_size;
       offset < materialized.payload.length();
       offset += session->chunk_bytes()) {
    const size_t length = std::min<uint64_t>(
        session->chunk_bytes(), materialized.payload.length() - offset);
    status = session->write_object_chunk(
        transfer_token, descriptor.object_id, offset,
        materialized.payload.substr(offset, length));
    if (status != Preserve_trx_transfer_status::OK) return status;
  }
  return session->seal_object(transfer_token, descriptor.object_id);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::stream_prebuilt_blobs_batch(
    const std::string &preserve_dir,
    const std::vector<Preserve_trx_transfer_phase1_blob_request> &requests) {
  if (requests.empty()) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  struct Materialized_request {
    Preserve_trx_transfer_phase1_blob_request request;
    Preserve_trx_transfer_object_descriptor descriptor;
    std::string payload;
    uint64_t payload_offset{0};
  };
  const std::string source_dir =
      preserve_dir.empty() ? preserved_trx_dir_value() : preserve_dir;
  std::unique_ptr<Preserved_trx_warm_external_blob_carrier> warm_carrier =
      create_preserved_trx_default_warm_external_blob_carrier(source_dir);
  if (warm_carrier == nullptr) return Preserve_trx_transfer_status::IO_ERROR;

  std::vector<Materialized_request> materialized_requests;
  materialized_requests.reserve(requests.size());
  for (const Preserve_trx_transfer_phase1_blob_request &request : requests) {
    const uint64_t payload_offset = request.preserved_prefix_size;
    const uint64_t expected_payload_bytes = request.size - payload_offset;
    const bool inline_payload = !request.inline_payload.empty();
    const bool binlog_prewarm_seed =
        request.object_id == kBinlogPrewarmSeedObjectId;
    if (request.transfer_token == 0 || request.object_id.empty() ||
        (!inline_payload &&
         (request.warmcopy_id.empty() || request.warmcopy_epoch == 0)) ||
        request.size == 0 ||
        payload_offset >= request.size ||
        (inline_payload &&
         request.inline_payload.size() != expected_payload_bytes) ||
        (request.required_source_freeze_lsn != 0 &&
         !binlog_prewarm_seed) ||
        (payload_offset != 0 &&
         (request.object_id != kPreservedTrxBlobBinlogCache ||
          payload_offset >= request.size))) {
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: phase1 batch request invalid epoch=" + m_epoch_id +
              " token=" + std::to_string(request.transfer_token) +
              " object=" + request.object_id + " size=" +
              std::to_string(request.size) + " prefix=" +
              std::to_string(payload_offset) + " inline_bytes=" +
              std::to_string(request.inline_payload.size()))
                 .c_str());
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }
    Materialized_request prepared;
    prepared.request = request;
    prepared.descriptor.object_id = request.object_id;
    prepared.descriptor.kind =
        Preserve_trx_transfer_object_kind::EXTERNAL_BLOB;
    prepared.descriptor.total_size = request.size;
    prepared.descriptor.digest = request.digest;
    prepared.descriptor.lock_plan.version =
        request.lock_plan_contract_version;
    prepared.descriptor.lock_plan.source_live_generation =
        request.source_live_lock_generation;
    prepared.descriptor.lock_plan.source_live_digest =
        request.source_live_lock_digest;
    prepared.descriptor.lock_plan.record_store_fingerprint =
        request.record_store_fingerprint;
    maybe_attach_simulated_terminal_lock_proof(
        m_epoch_id, request.transfer_token, &prepared.descriptor);
    prepared.payload_offset = payload_offset;
    if (!request.inline_payload.empty()) {
      prepared.payload = std::move(prepared.request.inline_payload);
    } else {
      Preserved_trx_external_blob_descriptor warm_descriptor;
      warm_descriptor.name = request.object_id;
      warm_descriptor.size = request.size;
      warm_descriptor.digest = request.digest;
      Preserved_trx_external_blob materialized;
      const Preserved_trx_carrier_status carrier_status =
          warm_carrier->read_warm_external_blob(
              request.warmcopy_id, request.object_id, request.warmcopy_epoch,
              warm_descriptor, m_max_inflight_bytes, &materialized);
      if (carrier_status != Preserved_trx_carrier_status::OK) {
        LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
               ("PRESERVE: phase1 batch warm blob materialize failed epoch=" +
                m_epoch_id + " token=" +
                std::to_string(request.transfer_token) + " object=" +
                request.object_id + " carrier_status=" +
                std::to_string(static_cast<int>(carrier_status)))
                   .c_str());
        return map_carrier_status_to_transfer(carrier_status);
      }
      if (materialized.payload.length() != request.size ||
          materialized.descriptor.name != request.object_id ||
          materialized.descriptor.size != request.size ||
          materialized.descriptor.digest != request.digest) {
        LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
               ("PRESERVE: phase1 batch warm blob changed epoch=" + m_epoch_id +
                " token=" + std::to_string(request.transfer_token) +
                " object=" + request.object_id + " expected_size=" +
                std::to_string(request.size) + " actual_size=" +
                std::to_string(materialized.payload.length()))
                   .c_str());
        return Preserve_trx_transfer_status::CORRUPT;
      }
      prepared.payload.assign(materialized.payload.data() + payload_offset,
                              static_cast<size_t>(expected_payload_bytes));
    }
    materialized_requests.push_back(std::move(prepared));
  }

  uint64_t materialized_bytes = 0;
  for (const Materialized_request &prepared : materialized_requests) {
    const uint64_t payload_bytes = prepared.payload.length();
    if (payload_bytes >
        std::numeric_limits<uint64_t>::max() - materialized_bytes) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    materialized_bytes += payload_bytes;
  }
  /* The epoch mutex is deliberately acquired only after this wait. */
  throttle_source_transfer_io(materialized_bytes, m_runtime_policy);

  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_chunk_bytes == 0 || m_epoch_committed ||
      m_commit_in_progress) {
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: phase1 batch source session unavailable epoch=" +
            m_epoch_id + " sink=" + (m_sink == nullptr ? "0" : "1") +
            " chunk_bytes=" + std::to_string(m_chunk_bytes) +
            " committed=" + (m_epoch_committed ? "1" : "0") +
            " commit_in_progress=" + (m_commit_in_progress ? "1" : "0"))
               .c_str());
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::vector<std::string> encoded_frames;
  std::set<uint64_t> encoded_frame_tokens;
  const uint64_t batch_overhead =
      kTransferFrameBatchMagicLength + sizeof(uint16_t) + sizeof(uint32_t);
  uint64_t encoded_batch_bytes = batch_overhead;
  std::vector<size_t> sent_request_indexes;
  std::set<uint64_t> prewarm_started_tokens;
  std::map<uint64_t, std::pair<uint64_t, uint64_t>>
      prewarm_started_lsn_facts;
  std::map<uint64_t,
           std::map<std::string, Preserve_trx_transfer_object_descriptor>>
      projected_declared_objects;
  std::map<uint64_t, std::map<std::string, uint64_t>>
      projected_written_bytes;
  std::map<uint64_t, std::set<std::string>> projected_sealed_objects;
  const uint64_t first_sequence = m_next_sequence;
  uint64_t acknowledged_next_sequence = first_sequence;
  auto sequence_guard = create_scope_guard([&] {
    m_next_sequence = acknowledged_next_sequence;
  });
  auto flush_encoded_frames = [&]() {
    if (encoded_frames.empty()) return Preserve_trx_transfer_status::OK;
    std::string encoded_batch;
    Preserve_trx_transfer_status status = encode_frame_batch_with_limit(
        encoded_frames.data(), encoded_frames.size(), m_max_inflight_bytes,
        &encoded_batch);
    if (status != Preserve_trx_transfer_status::OK) {
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: phase1 frame batch encode failed epoch=" + m_epoch_id +
              " status=" + transfer_status_name(status) + " frames=" +
              std::to_string(encoded_frames.size()) + " estimated_bytes=" +
              std::to_string(encoded_batch_bytes))
                 .c_str());
      return status;
    }
    status = m_sink->send_encoded_frame(encoded_batch);
    if (status == Preserve_trx_transfer_status::ACK_UNCERTAIN) {
      m_ack_uncertain = true;
    }
    if (status != Preserve_trx_transfer_status::OK) {
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: phase1 frame batch send failed epoch=" + m_epoch_id +
              " status=" + transfer_status_name(status) + " frames=" +
              std::to_string(encoded_frames.size()) + " encoded_bytes=" +
              std::to_string(encoded_batch.length()) + " first_sequence=" +
              std::to_string(acknowledged_next_sequence) +
              " next_sequence=" + std::to_string(m_next_sequence))
                 .c_str());
      return status;
    }
    acknowledged_next_sequence += encoded_frames.size();
    if (m_phase1_metrics_enabled) {
      note_source_phase1_network_send(encoded_frames.size(),
                                      encoded_batch.length(),
                                      encoded_frame_tokens.size(), true);
    }
    encoded_frames.clear();
    encoded_frame_tokens.clear();
    encoded_batch_bytes = batch_overhead;
    return Preserve_trx_transfer_status::OK;
  };
  auto append_encoded_frame = [&](uint64_t transfer_token,
                                  std::string encoded_frame) {
    const uint64_t frame_bytes = sizeof(uint64_t) + encoded_frame.length();
    if (frame_bytes >
        std::numeric_limits<uint64_t>::max() - encoded_batch_bytes) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    if (!encoded_frames.empty() &&
        (m_phase1_batch_bytes == 0 ||
         encoded_batch_bytes + frame_bytes > m_phase1_batch_bytes)) {
      const Preserve_trx_transfer_status status = flush_encoded_frames();
      if (status != Preserve_trx_transfer_status::OK) return status;
    }
    encoded_batch_bytes += frame_bytes;
    encoded_frame_tokens.insert(transfer_token);
    encoded_frames.push_back(std::move(encoded_frame));
    if (m_phase1_batch_bytes == 0 ||
        encoded_batch_bytes >= m_phase1_batch_bytes) {
      return flush_encoded_frames();
    }
    return Preserve_trx_transfer_status::OK;
  };
  for (size_t request_index = 0;
       request_index < materialized_requests.size(); ++request_index) {
    const Materialized_request &prepared =
        materialized_requests[request_index];
    const uint64_t transfer_token = prepared.request.transfer_token;
    if (!token_declared(transfer_token) || token_resolved(transfer_token) ||
        m_streaming_manifests.count(transfer_token) != 0) {
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: phase1 batch token state rejected epoch=" + m_epoch_id +
              " token=" + std::to_string(transfer_token) + " declared=" +
              (token_declared(transfer_token) ? "1" : "0") + " resolved=" +
              (token_resolved(transfer_token) ? "1" : "0") +
              " final_manifest_started=" +
              (m_streaming_manifests.count(transfer_token) != 0 ? "1" : "0"))
                 .c_str());
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }

    auto projected = projected_declared_objects.find(transfer_token);
    if (projected == projected_declared_objects.end()) {
      std::map<std::string, Preserve_trx_transfer_object_descriptor> objects;
      const auto existing =
          m_streaming_declared_objects.find(transfer_token);
      if (existing != m_streaming_declared_objects.end()) {
        objects = existing->second;
      }
      projected =
          projected_declared_objects
              .emplace(transfer_token, std::move(objects))
              .first;
    }
    auto projected_written = projected_written_bytes.find(transfer_token);
    if (projected_written == projected_written_bytes.end()) {
      std::map<std::string, uint64_t> written;
      const auto existing =
          m_streaming_object_written_bytes.find(transfer_token);
      if (existing != m_streaming_object_written_bytes.end()) {
        written = existing->second;
      }
      projected_written =
          projected_written_bytes
              .emplace(transfer_token, std::move(written))
              .first;
    }
    auto projected_sealed = projected_sealed_objects.find(transfer_token);
    if (projected_sealed == projected_sealed_objects.end()) {
      std::set<std::string> sealed;
      const auto existing = m_streaming_sealed_objects.find(transfer_token);
      if (existing != m_streaming_sealed_objects.end()) {
        sealed = existing->second;
      }
      projected_sealed =
          projected_sealed_objects
              .emplace(transfer_token, std::move(sealed))
              .first;
    }

    bool already_presealed = false;
    const auto declared_object =
        projected->second.find(prepared.descriptor.object_id);
    if (declared_object != projected->second.end() &&
        transfer_object_descriptor_equal(declared_object->second,
                                           prepared.descriptor) &&
        projected_sealed->second.count(prepared.descriptor.object_id) != 0) {
      const auto written_object =
          projected_written->second.find(prepared.descriptor.object_id);
      already_presealed =
          written_object != projected_written->second.end() &&
          written_object->second == prepared.descriptor.total_size;
    }
    if (already_presealed) continue;
    if (prepared.payload_offset != 0) {
      Preserve_trx_transfer_object_descriptor prefix_descriptor =
          prepared.descriptor;
      prefix_descriptor.total_size = prepared.payload_offset;
      prefix_descriptor.digest =
          prepared.request.preserved_prefix_digest;
      bool prefix_presealed = false;
      const auto prefix_object =
          projected->second.find(prefix_descriptor.object_id);
      if (prefix_object != projected->second.end() &&
          transfer_object_descriptor_equal(prefix_object->second,
                                             prefix_descriptor) &&
          projected_sealed->second.count(prefix_descriptor.object_id) != 0) {
        const auto written_object =
            projected_written->second.find(prefix_descriptor.object_id);
        prefix_presealed =
            written_object != projected_written->second.end() &&
            written_object->second == prefix_descriptor.total_size;
      }
      if (!prefix_presealed) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
    }

    std::string descriptor_payload;
    Preserve_trx_transfer_status status = encode_transfer_object_descriptor(
        prepared.descriptor, &descriptor_payload);
    if (status != Preserve_trx_transfer_status::OK) return status;
    Preserve_trx_transfer_frame declare;
    declare.type = Preserve_trx_transfer_frame_type::DECLARE_OBJECT;
    declare.sequence = m_next_sequence++;
    declare.epoch_id = m_epoch_id;
    declare.token = transfer_token;
    declare.object_id = prepared.descriptor.object_id;
    declare.manifest_payload = std::move(descriptor_payload);
    if (prepared.payload_offset != 0) {
      declare.chunk_offset = prepared.payload_offset;
      declare.reason = encode_binlog_append_prefix_reason(
          prepared.payload_offset,
          prepared.request.preserved_prefix_digest);
    }
    stamp_online_epoch_context_locked(&declare);
    std::string encoded_declare;
    status = preserve_trx_transfer_encode_frame(declare, &encoded_declare);
    if (status != Preserve_trx_transfer_status::OK) return status;
    status =
        append_encoded_frame(transfer_token, std::move(encoded_declare));
    if (status != Preserve_trx_transfer_status::OK) return status;

    projected->second[prepared.descriptor.object_id] = prepared.descriptor;

    const bool record_lock_object =
        prepared.descriptor.object_id == kPreservedTrxBlobRecordLocks;
    const bool binlog_seed_object =
        prepared.descriptor.object_id == kBinlogPrewarmSeedObjectId;
    const auto pending_lsn_fact =
        prewarm_started_lsn_facts.find(transfer_token);
    const auto published_lsn_fact =
        m_prewarm_lsn_facts.find(transfer_token);
    const bool prewarm_manifest_started =
        pending_lsn_fact != prewarm_started_lsn_facts.end() ||
        m_prewarm_manifest_tokens.count(transfer_token) != 0;
    std::pair<uint64_t, uint64_t> prewarm_lsn_fact{0, 0};
    if (pending_lsn_fact != prewarm_started_lsn_facts.end()) {
      prewarm_lsn_fact = pending_lsn_fact->second;
    } else if (published_lsn_fact != m_prewarm_lsn_facts.end()) {
      prewarm_lsn_fact = published_lsn_fact->second;
    }
    bool publish_prewarm_manifest =
        record_lock_object && !prewarm_manifest_started;
    if (binlog_seed_object) {
      if (!prewarm_manifest_started) {
        publish_prewarm_manifest = true;
      } else if (prepared.request.required_source_freeze_lsn != 0) {
        std::pair<uint64_t, uint64_t> required_lsn_fact = prewarm_lsn_fact;
        required_lsn_fact.first =
            prepared.request.required_source_freeze_lsn;
        required_lsn_fact.second =
            std::max(required_lsn_fact.second,
                     prepared.request.required_source_freeze_lsn);
        publish_prewarm_manifest =
            required_lsn_fact != prewarm_lsn_fact;
      }
    }
    if (publish_prewarm_manifest) {
      Preserve_trx_transfer_manifest prewarm_manifest;
      prewarm_manifest.epoch_id = m_epoch_id;
      prewarm_manifest.token = transfer_token;
      if (prewarm_manifest_started) {
        prewarm_manifest.source_freeze_lsn = prewarm_lsn_fact.first;
        prewarm_manifest.source_epoch_commit_lsn = prewarm_lsn_fact.second;
      } else if (!load_source_transfer_lsn_fact(
                     &prewarm_manifest.source_freeze_lsn,
                     &prewarm_manifest.source_epoch_commit_lsn)) {
        return Preserve_trx_transfer_status::UNSUPPORTED;
      }
      if (prepared.request.required_source_freeze_lsn != 0) {
        prewarm_manifest.source_freeze_lsn =
            prepared.request.required_source_freeze_lsn;
        prewarm_manifest.source_epoch_commit_lsn =
            std::max(prewarm_manifest.source_epoch_commit_lsn,
                     prepared.request.required_source_freeze_lsn);
      }
      if (prewarm_manifest.source_freeze_lsn != 0 &&
          prewarm_manifest.source_epoch_commit_lsn != 0) {
        prewarm_started_lsn_facts[transfer_token] = {
            prewarm_manifest.source_freeze_lsn,
            prewarm_manifest.source_epoch_commit_lsn};
        prewarm_manifest.objects.reserve(projected->second.size());
        for (const auto &entry : projected->second) {
          prewarm_manifest.objects.push_back(entry.second);
        }
        status = validate_manifest_components(prewarm_manifest, false);
        if (status != Preserve_trx_transfer_status::OK) return status;
        std::string prewarm_payload;
        status = preserve_trx_transfer_encode_manifest(prewarm_manifest,
                                                       &prewarm_payload);
        if (status != Preserve_trx_transfer_status::OK) return status;
        Preserve_trx_transfer_frame begin;
        begin.type = Preserve_trx_transfer_frame_type::BEGIN;
        begin.sequence = m_next_sequence++;
        begin.epoch_id = m_epoch_id;
        begin.token = transfer_token;
        begin.manifest_payload = std::move(prewarm_payload);
        stamp_online_epoch_context_locked(&begin);
        std::string encoded_begin;
        status = preserve_trx_transfer_encode_frame(begin, &encoded_begin);
        if (status != Preserve_trx_transfer_status::OK) return status;
        status =
            append_encoded_frame(transfer_token, std::move(encoded_begin));
        if (status != Preserve_trx_transfer_status::OK) return status;
        prewarm_started_tokens.insert(transfer_token);
      }
    }

    for (uint64_t payload_index = 0;
         payload_index < prepared.payload.length();
         payload_index += m_chunk_bytes) {
      const size_t length = std::min<uint64_t>(
          m_chunk_bytes, prepared.payload.length() - payload_index);
      Preserve_trx_transfer_frame chunk;
      chunk.type = Preserve_trx_transfer_frame_type::OBJECT_CHUNK;
      chunk.sequence = m_next_sequence++;
      chunk.epoch_id = m_epoch_id;
      chunk.token = transfer_token;
      chunk.object_id = prepared.descriptor.object_id;
      chunk.chunk_offset = prepared.payload_offset + payload_index;
      chunk.chunk_payload = prepared.payload.substr(payload_index, length);
      stamp_online_epoch_context_locked(&chunk);
      std::string encoded_chunk;
      status = preserve_trx_transfer_encode_frame(chunk, &encoded_chunk);
      if (status != Preserve_trx_transfer_status::OK) return status;
      status =
          append_encoded_frame(transfer_token, std::move(encoded_chunk));
      if (status != Preserve_trx_transfer_status::OK) return status;
    }

    Preserve_trx_transfer_frame seal;
    seal.type = Preserve_trx_transfer_frame_type::SEAL_OBJECT;
    seal.sequence = m_next_sequence++;
    seal.epoch_id = m_epoch_id;
    seal.token = transfer_token;
    seal.object_id = prepared.descriptor.object_id;
    stamp_online_epoch_context_locked(&seal);
    std::string encoded_seal;
    status = preserve_trx_transfer_encode_frame(seal, &encoded_seal);
    if (status != Preserve_trx_transfer_status::OK) return status;
    status = append_encoded_frame(transfer_token, std::move(encoded_seal));
    if (status != Preserve_trx_transfer_status::OK) return status;
    projected_written->second[prepared.descriptor.object_id] =
        prepared.descriptor.total_size;
    projected_sealed->second.insert(prepared.descriptor.object_id);
    sent_request_indexes.push_back(request_index);
  }
  if (sent_request_indexes.empty()) {
    sequence_guard.commit();
    return Preserve_trx_transfer_status::OK;
  }

  Preserve_trx_transfer_status status = flush_encoded_frames();
  if (status != Preserve_trx_transfer_status::OK) return status;
  sequence_guard.commit();
  if (m_phase1_metrics_enabled) {
    for (size_t request_index : sent_request_indexes) {
      if (materialized_requests[request_index].request.object_id ==
          kPreservedTrxBlobRecordLocks) {
        size_t record_tokens = 0;
        for (size_t record_index : sent_request_indexes) {
          if (materialized_requests[record_index].request.object_id ==
              kPreservedTrxBlobRecordLocks) {
            ++record_tokens;
          }
        }
        note_source_phase1_record_batch_sent(record_tokens);
        break;
      }
    }
  }

  m_prewarm_manifest_tokens.insert(prewarm_started_tokens.begin(),
                                   prewarm_started_tokens.end());
  for (const auto &fact : prewarm_started_lsn_facts) {
    if (prewarm_started_tokens.count(fact.first) != 0) {
      m_prewarm_lsn_facts[fact.first] = fact.second;
    }
  }

  for (size_t request_index : sent_request_indexes) {
    const Materialized_request &prepared =
        materialized_requests[request_index];
    const uint64_t transfer_token = prepared.request.transfer_token;
    m_streaming_declared_objects[transfer_token][prepared.descriptor.object_id] =
        prepared.descriptor;
    m_streaming_object_written_bytes[transfer_token]
                                    [prepared.descriptor.object_id] =
        prepared.descriptor.total_size;
    m_streaming_sealed_objects[transfer_token].insert(
        prepared.descriptor.object_id);
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_stream_prebuilt_blobs_batch(
    Preserve_trx_transfer_source_epoch_session *session,
    const std::string &preserve_dir,
    const std::vector<Preserve_trx_transfer_phase1_blob_request> &requests,
    uint64_t max_batch_bytes) {
  if (session == nullptr || requests.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const bool has_inline_payload = std::any_of(
      requests.begin(), requests.end(),
      [](const Preserve_trx_transfer_phase1_blob_request &request) {
        return !request.inline_payload.empty();
      });
  if (max_batch_bytes == 0 && !has_inline_payload) {
    for (const Preserve_trx_transfer_phase1_blob_request &request : requests) {
      Preserve_trx_transfer_lock_plan_contract lock_plan;
      const Preserve_trx_transfer_lock_plan_contract *lock_plan_ptr = nullptr;
      if (request.lock_plan_contract_version != 0) {
        lock_plan.version = request.lock_plan_contract_version;
        lock_plan.source_live_generation =
            request.source_live_lock_generation;
        lock_plan.source_live_digest = request.source_live_lock_digest;
        lock_plan.record_store_fingerprint =
            request.record_store_fingerprint;
        lock_plan_ptr = &lock_plan;
      }
      const Preserve_trx_transfer_status status =
          stream_prebuilt_external_blob_for_transfer(
              session, request.transfer_token, preserve_dir, request.object_id,
              request.warmcopy_id, request.warmcopy_epoch, request.size,
              request.digest, lock_plan_ptr, request.preserved_prefix_size,
              request.preserved_prefix_size == 0
                  ? nullptr
                  : &request.preserved_prefix_digest);
      if (status != Preserve_trx_transfer_status::OK) return status;
      if (request.object_id == kPreservedTrxBlobRecordLocks) {
        note_source_phase1_record_batch_sent(1);
      }
    }
    return Preserve_trx_transfer_status::OK;
  }
  if (requests.size() == 1 && requests.front().size > max_batch_bytes) {
    g_transfer_phase1_oversize_token_count.fetch_add(1);
  }
  return session->stream_prebuilt_blobs_batch(preserve_dir, requests);
}

Preserve_trx_transfer_status
preserve_trx_transfer_stream_prebuilt_record_locks_blob(
    Preserve_trx_transfer_source_epoch_session *session,
    uint64_t transfer_token, const std::string &preserve_dir,
    const PrebuiltRecordLocksBlob &blob) {
  if (blob.name != kPreservedTrxBlobRecordLocks) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  Preserve_trx_transfer_lock_plan_contract lock_plan;
  lock_plan.version = blob.lock_plan_contract_version;
  lock_plan.source_live_generation = blob.source_live_lock_generation;
  lock_plan.source_live_digest = blob.source_live_lock_digest;
  lock_plan.record_store_fingerprint = blob.record_store_fingerprint;
  return stream_prebuilt_external_blob_for_transfer(
      session, transfer_token, preserve_dir, blob.name, blob.warmcopy_id,
      blob.warmcopy_epoch, blob.size, blob.digest,
      lock_plan.version == 0 ? nullptr : &lock_plan);
}

Preserve_trx_transfer_status
preserve_trx_transfer_stream_prebuilt_binlog_cache_blob(
    Preserve_trx_transfer_source_epoch_session *session,
    uint64_t transfer_token, const std::string &preserve_dir,
    const PrebuiltBinlogCacheBlob &blob) {
  if (blob.name != kPreservedTrxBlobBinlogCache) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  return stream_prebuilt_external_blob_for_transfer(
      session, transfer_token, preserve_dir, blob.name, blob.warmcopy_id,
      blob.warmcopy_epoch, blob.size, blob.digest);
}

Preserve_trx_transfer_status
preserve_trx_transfer_stage_deferred_candidate_external_objects(
    Preserve_trx_transfer_source_epoch_session *session,
    const std::string &preserve_dir,
    Preserve_trx_deferred_transfer_candidate *candidate,
    Preserve_trx_transfer_phase1_batch_sender *batch_sender,
    const Preserve_trx_transfer_object_descriptor
        *pending_final_binlog_descriptor) {
  if (session == nullptr || candidate == nullptr || !candidate->captured ||
      candidate->finalized || candidate->transfer_token == 0 ||
      candidate->epoch_id != session->epoch_id()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (candidate->external_objects_staged) {
    return pending_final_binlog_descriptor == nullptr
               ? Preserve_trx_transfer_status::OK
               : Preserve_trx_transfer_status::CORRUPT;
  }

  const auto temp_status = preserve_trx_temp_transfer_stream(
      session, candidate->transfer_token, preserve_dir,
      candidate->bundle.metadata.temp_table_manifest_payload);
  if (temp_status != Preserve_trx_transfer_status::OK) return temp_status;

  {
    const auto result_status = preserve_trx_result_transfer_stream(
        session, candidate->transfer_token,
        candidate->bundle.metadata.cursor_manifest_payload,
        candidate->bundle.source_cursor_results.get());
    if (result_status != Preserve_trx_transfer_status::OK) return result_status;
  }

  if (pending_final_binlog_descriptor != nullptr) {
    const auto pending_blob = std::find_if(
        candidate->bundle.external_blobs.begin(),
        candidate->bundle.external_blobs.end(),
        [](const Preserved_trx_external_blob &blob) {
          return blob.name == kPreservedTrxBlobBinlogCache;
        });
    if (pending_final_binlog_descriptor->object_id !=
            kPreservedTrxBlobBinlogCache ||
        pending_final_binlog_descriptor->kind !=
            Preserve_trx_transfer_object_kind::EXTERNAL_BLOB ||
        pending_blob == candidate->bundle.external_blobs.end() ||
        std::count_if(candidate->bundle.external_blobs.begin(),
                      candidate->bundle.external_blobs.end(),
                      [](const Preserved_trx_external_blob &blob) {
                        return blob.name == kPreservedTrxBlobBinlogCache;
                      }) != 1) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    const Preserve_trx_transfer_object_descriptor descriptor =
        transfer_external_blob_descriptor(candidate->epoch_id,
                                          candidate->transfer_token,
                                          *pending_blob);
    if (descriptor.object_id != pending_final_binlog_descriptor->object_id ||
        descriptor.kind != pending_final_binlog_descriptor->kind ||
        descriptor.total_size != pending_final_binlog_descriptor->total_size ||
        descriptor.digest != pending_final_binlog_descriptor->digest) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }

  std::string binlog_seed_payload;
  Preserve_trx_transfer_object_descriptor binlog_seed_descriptor;
  const bool has_binlog_cache =
      candidate->bundle.metadata.binlog_state ==
          Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE &&
      std::any_of(candidate->bundle.external_blobs.begin(),
                  candidate->bundle.external_blobs.end(),
                  [](const Preserved_trx_external_blob &blob) {
                    return blob.name == kPreservedTrxBlobBinlogCache;
                  });
  const bool stage_binlog_seed =
      has_binlog_cache && !candidate->binlog_prewarm_seed_staged &&
      !candidate->binlog_prewarm_seed_batch_pending;
  if (stage_binlog_seed) {
    Preserve_trx_transfer_status status =
        preserve_trx_transfer_encode_portable_bundle_impl(
            candidate->bundle, &binlog_seed_payload, true);
    if (status != Preserve_trx_transfer_status::OK) return status;
    binlog_seed_descriptor.object_id = kBinlogPrewarmSeedObjectId;
    binlog_seed_descriptor.kind =
        Preserve_trx_transfer_object_kind::EXTERNAL_BLOB;
    binlog_seed_descriptor.total_size = binlog_seed_payload.size();
    binlog_seed_descriptor.digest = sha256_digest(binlog_seed_payload);
  }

  for (const Preserved_trx_external_blob &blob :
       candidate->bundle.external_blobs) {
    const Preserve_trx_transfer_object_descriptor descriptor =
        transfer_external_blob_descriptor(candidate->epoch_id,
                                          candidate->transfer_token, blob);
    if (pending_final_binlog_descriptor != nullptr &&
        blob.name == kPreservedTrxBlobBinlogCache) {
      continue;
    }
    if (session->object_presealed_for_token(candidate->transfer_token,
                                            descriptor)) {
      continue;
    }

    Preserve_trx_transfer_status status = Preserve_trx_transfer_status::OK;
    const char *stage = "prebuilt_read";
    if (blob.prebuilt && blob.name == kPreservedTrxBlobRecordLocks &&
        batch_sender != nullptr && stage_binlog_seed) {
      /* The following seed keeps this candidate pending until the wave flush. */
      Preserve_trx_transfer_phase1_blob_request request;
      request.transfer_token = candidate->transfer_token;
      request.object_id = blob.name;
      request.warmcopy_id = blob.warmcopy_id;
      request.warmcopy_epoch = blob.warmcopy_epoch;
      request.size = descriptor.total_size;
      request.digest = descriptor.digest;
      request.lock_plan_contract_version = descriptor.lock_plan.version;
      request.source_live_lock_generation =
          descriptor.lock_plan.source_live_generation;
      request.source_live_lock_digest = descriptor.lock_plan.source_live_digest;
      request.record_store_fingerprint =
          descriptor.lock_plan.record_store_fingerprint;
      stage = "prebuilt_enqueue";
      status = batch_sender->enqueue(request);
    } else if (blob.prebuilt) {
      status = stream_prebuilt_external_blob_for_transfer(
          session, candidate->transfer_token, preserve_dir, blob.name,
          blob.warmcopy_id, blob.warmcopy_epoch, blob.descriptor.size,
          blob.descriptor.digest,
          blob.name == kPreservedTrxBlobRecordLocks ? &descriptor.lock_plan
                                                    : nullptr);
    } else {
      if (blob.name.empty() || blob.payload.empty())
        return Preserve_trx_transfer_status::INVALID_ARGUMENT;
      stage = "declare";
      status = session->declare_object(candidate->transfer_token, descriptor);
      for (uint64_t offset = 0;
           status == Preserve_trx_transfer_status::OK &&
           offset < blob.payload.length();
           offset += session->chunk_bytes()) {
        stage = "chunk";
        const size_t length = std::min<uint64_t>(
            session->chunk_bytes(), blob.payload.length() - offset);
        status = session->write_object_chunk(
            candidate->transfer_token, descriptor.object_id, offset,
            blob.payload.substr(offset, length));
      }
      if (status == Preserve_trx_transfer_status::OK) {
        stage = "seal";
        status = session->seal_object(candidate->transfer_token,
                                      descriptor.object_id);
      }
    }
    if (status != Preserve_trx_transfer_status::OK) {
      const std::string message =
          "PRESERVE: deferred transfer external object stage failed epoch=" +
          candidate->epoch_id +
          " token=" + std::to_string(candidate->transfer_token) +
          " object=" + blob.name +
          " prebuilt=" + (blob.prebuilt ? "1" : "0") +
          " stage=" + stage +
          " lock_contract_version=" +
          std::to_string(descriptor.lock_plan.version) +
          " lock_generation=" +
          std::to_string(descriptor.lock_plan.source_live_generation) +
          " status=" + transfer_status_name(status);
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
      return status;
    }
  }
  if (stage_binlog_seed) {
    const uint64_t required_source_freeze_lsn =
        candidate->has_resurrection_entry
            ? candidate->resurrection_entry.freeze_lsn
            : 0;
    if (batch_sender != nullptr) {
      Preserve_trx_transfer_phase1_blob_request request;
      request.transfer_token = candidate->transfer_token;
      request.object_id = binlog_seed_descriptor.object_id;
      request.size = binlog_seed_descriptor.total_size;
      request.digest = binlog_seed_descriptor.digest;
      request.inline_payload = std::move(binlog_seed_payload);
      request.required_source_freeze_lsn = required_source_freeze_lsn;
      const Preserve_trx_transfer_status status =
          batch_sender->enqueue(request);
      if (status != Preserve_trx_transfer_status::OK) return status;
      candidate->binlog_prewarm_seed_batch_pending = true;
    } else {
      Preserve_trx_transfer_status status = session->declare_object(
          candidate->transfer_token, binlog_seed_descriptor);
      if (status != Preserve_trx_transfer_status::OK) return status;
      status = session->begin_token_prewarm_manifest(
          candidate->transfer_token, required_source_freeze_lsn);
      if (status != Preserve_trx_transfer_status::OK) return status;
      if (!session->token_prewarm_lsn_fact(
              candidate->transfer_token, &candidate->source_freeze_lsn,
              &candidate->source_epoch_commit_lsn)) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      for (uint64_t offset = 0; offset < binlog_seed_payload.size();
           offset += session->chunk_bytes()) {
        const size_t length = std::min<uint64_t>(
            session->chunk_bytes(), binlog_seed_payload.size() - offset);
        status = session->write_object_chunk(
            candidate->transfer_token, binlog_seed_descriptor.object_id,
            offset, binlog_seed_payload.substr(offset, length));
        if (status != Preserve_trx_transfer_status::OK) return status;
      }
      status = session->seal_object(candidate->transfer_token,
                                    binlog_seed_descriptor.object_id);
      if (status != Preserve_trx_transfer_status::OK) return status;
      candidate->binlog_prewarm_seed_staged = true;
    }
  }
  candidate->external_objects_staged =
      !candidate->binlog_prewarm_seed_batch_pending;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
preserve_trx_transfer_replace_deferred_candidate_record_locks(
    Preserve_trx_deferred_transfer_candidate *candidate,
    Preserved_trx_external_blob replacement) {
  if (candidate == nullptr || !candidate->captured || candidate->finalized ||
      !candidate->external_objects_staged || replacement.prebuilt ||
      replacement.name != kPreservedTrxBlobRecordLocks ||
      replacement.payload.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  auto current = std::find_if(
      candidate->bundle.external_blobs.begin(),
      candidate->bundle.external_blobs.end(),
      [](const Preserved_trx_external_blob &blob) {
        return blob.name == kPreservedTrxBlobRecordLocks;
      });
  const Preserve_trx_transfer_object_descriptor replacement_descriptor =
      transfer_external_blob_descriptor(candidate->epoch_id,
                                        candidate->transfer_token, replacement);
  if (!transfer_lock_plan_contract_valid(replacement_descriptor)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (current == candidate->bundle.external_blobs.end()) {
    candidate->bundle.external_blobs.push_back(std::move(replacement));
    candidate->external_objects_staged = false;
    return Preserve_trx_transfer_status::OK;
  }

  const Preserve_trx_transfer_object_descriptor current_descriptor =
      transfer_external_blob_descriptor(candidate->epoch_id,
                                        candidate->transfer_token, *current);
  const Preserve_trx_transfer_status replacement_status =
      transfer_lock_plan_replacement_status(current_descriptor,
                                            replacement_descriptor);
  if (replacement_status != Preserve_trx_transfer_status::OK) {
    return replacement_status;
  }

  *current = std::move(replacement);
  candidate->external_objects_staged = false;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_finalize_deferred_candidate(
    Preserve_trx_transfer_source_epoch_session *session,
    Preserve_trx_deferred_transfer_candidate *candidate) {
  if (session == nullptr || candidate == nullptr || !candidate->captured ||
      !candidate->external_objects_staged || candidate->transfer_token == 0 ||
      candidate->epoch_id != session->epoch_id()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (candidate->finalized) return Preserve_trx_transfer_status::OK;

  std::set<std::string> presealed_external_objects;
  std::vector<Preserve_trx_transfer_object_descriptor> temp_objects;
  auto temp_status = preserve_trx_temp_transfer_descriptors(
      std::to_string(candidate->transfer_token),
      candidate->bundle.metadata.temp_table_manifest_payload, &temp_objects);
  if (temp_status != Preserve_trx_transfer_status::OK) return temp_status;
  for (const auto &object : temp_objects) {
    if (!session->object_presealed_for_token(candidate->transfer_token, object))
      return Preserve_trx_transfer_status::UNSUPPORTED;
    presealed_external_objects.insert(object.object_id);
  }
  Preserve_memory_lease result_descriptors_memory;
  std::vector<Preserve_trx_transfer_object_descriptor> result_objects;
  auto result_status = preserve_trx_result_transfer_descriptors(
      candidate->bundle.metadata.token,
      candidate->bundle.metadata.cursor_manifest_payload, &result_objects,
      &result_descriptors_memory);
  if (result_status != Preserve_trx_transfer_status::OK) return result_status;
  for (const auto &object : result_objects) {
    if (!session->object_presealed_for_token(candidate->transfer_token, object))
      return Preserve_trx_transfer_status::UNSUPPORTED;
    presealed_external_objects.insert(object.object_id);
  }
  for (const Preserved_trx_external_blob &blob :
       candidate->bundle.external_blobs) {
    const Preserve_trx_transfer_object_descriptor descriptor =
        transfer_external_blob_descriptor(candidate->epoch_id,
                                          candidate->transfer_token, blob);
    if (!session->object_presealed_for_token(candidate->transfer_token,
                                             descriptor)) {
      const std::string message =
          "PRESERVE: deferred transfer finalize rejected unsealed object "
          "epoch=" +
          candidate->epoch_id +
          " token=" + std::to_string(candidate->transfer_token) +
          " object=" + descriptor.object_id +
          " size=" + std::to_string(descriptor.total_size) +
          " lock_generation=" +
          std::to_string(descriptor.lock_plan.source_live_generation);
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    presealed_external_objects.insert(blob.name);
  }

  Preserve_trx_transfer_manifest manifest;
  std::vector<Preserve_trx_transfer_object_payload> objects;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_build_portable_objects_impl(
          candidate->epoch_id, candidate->bundle, candidate->transfer_token,
          &manifest, &objects,
          &presealed_external_objects,
          candidate->has_resurrection_entry ? &candidate->resurrection_entry
                                            : nullptr,
          candidate->source_freeze_lsn,
          candidate->source_epoch_commit_lsn);
  if (status != Preserve_trx_transfer_status::OK) {
    const std::string message =
        "PRESERVE: deferred transfer finalize build failed epoch=" +
        candidate->epoch_id +
        " token=" + std::to_string(candidate->transfer_token) +
        " source_freeze_lsn=" +
        std::to_string(candidate->source_freeze_lsn) +
        " source_epoch_commit_lsn=" +
        std::to_string(candidate->source_epoch_commit_lsn) +
        " resurrection_token=" +
        (candidate->has_resurrection_entry
             ? candidate->resurrection_entry.authority_token
             : std::string()) +
        " resurrection_freeze_lsn=" +
        std::to_string(candidate->has_resurrection_entry
                           ? candidate->resurrection_entry.freeze_lsn
                           : 0) +
        " binlog_seed_staged=" +
        (candidate->binlog_prewarm_seed_staged ? "1" : "0") +
        " status=" + transfer_status_name(status);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return status;
  }
  status = session->send_token_objects_batch(
      manifest, objects, presealed_external_objects, true);
  if (status != Preserve_trx_transfer_status::OK) {
    const std::string message =
        "PRESERVE: deferred transfer finalize publish failed epoch=" +
        candidate->epoch_id +
        " token=" + std::to_string(candidate->transfer_token) +
        " status=" + transfer_status_name(status);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
  }
  if (status == Preserve_trx_transfer_status::OK) candidate->finalized = true;
  return status;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::abort_token_locked(
    uint64_t transfer_token, const std::string &reason, bool allow_finalized,
    Pending_frame_cleanup cleanup) {
  if (m_sink == nullptr || !transfer_component_safe(m_epoch_id) ||
      transfer_token == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed || m_commit_in_progress)
    return Preserve_trx_transfer_status::UNSUPPORTED;
  // An uncertain data sequence cannot be replaced by a new token ABORT,
  // including on a different connection. Epoch cleanup resolves it after join.
  if (m_ack_uncertain) return Preserve_trx_transfer_status::ACK_UNCERTAIN;
  const bool finalized = m_finalized_tokens.count(transfer_token) != 0;
  if (!token_declared(transfer_token) ||
      m_aborted_tokens.count(transfer_token) != 0 ||
      (finalized && !allow_finalized)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (!allow_finalized && m_final_metadata_tokens.count(transfer_token) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  DBUG_EXECUTE_IF("preserve_trx_transfer_fail_abort_token", {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  });

  /*
    Deferred final frames already own their sequence numbers. A later token
    failure cannot send ABORT past that unsent range, otherwise the receiver
    waits forever for the missing sequence. This is a failure-only slow path;
    flush survivor frames in order before requesting the exact token abort.
  */
  if (!m_pending_final_metadata_frames.empty()) {
    for (const Preserve_trx_transfer_frame &pending :
         m_pending_final_metadata_frames) {
      std::string encoded_frame;
      Preserve_trx_transfer_status pending_status =
          preserve_trx_transfer_encode_frame(pending, &encoded_frame);
      if (pending_status != Preserve_trx_transfer_status::OK) {
        return pending_status;
      }
      pending_status = m_sink->send_encoded_frame(encoded_frame);
      if (pending_status == Preserve_trx_transfer_status::ACK_UNCERTAIN) {
        m_ack_uncertain = true;
      }
      if (pending_status != Preserve_trx_transfer_status::OK) {
        return pending_status;
      }
    }
    g_transfer_phase2_final_metadata_frame_count.fetch_add(
        m_pending_final_metadata_frames.size());
    m_pending_final_metadata_frames.clear();
  }

  Preserve_trx_transfer_frame abort;
  abort.type = Preserve_trx_transfer_frame_type::ABORT;
  abort.sequence = m_next_sequence;
  abort.epoch_id = m_epoch_id;
  abort.token = transfer_token;
  abort.reason = reason;
  stamp_online_epoch_context_locked(&abort);
  Preserve_trx_transfer_status status =
      send_encoded_transfer_frame(m_sink, abort);
  if (status == Preserve_trx_transfer_status::ACK_UNCERTAIN) {
    m_ack_uncertain = true;
  }
  if (status != Preserve_trx_transfer_status::OK) return status;
  ++m_next_sequence;
  if (finalized) {
    m_finalized_tokens.erase(transfer_token);
    m_finalized_manifests.erase(
        std::remove_if(m_finalized_manifests.begin(),
                       m_finalized_manifests.end(),
                       [&](const Preserve_trx_transfer_manifest &manifest) {
                         return manifest.token == transfer_token;
                       }),
        m_finalized_manifests.end());
  }
  if (cleanup == Pending_frame_cleanup::REQUIRED) {
#ifndef NDEBUG
    ++m_pending_frame_cleanup_invocations;
#endif
    m_pending_final_metadata_frames.erase(
        std::remove_if(m_pending_final_metadata_frames.begin(),
                       m_pending_final_metadata_frames.end(),
                       [&](const Preserve_trx_transfer_frame &frame) {
                         return frame.token == transfer_token;
                       }),
        m_pending_final_metadata_frames.end());
  }
  m_streaming_manifests.erase(transfer_token);
  m_streaming_declared_objects.erase(transfer_token);
  m_streaming_object_written_bytes.erase(transfer_token);
  m_streaming_sealed_objects.erase(transfer_token);
  m_prewarm_lsn_facts.erase(transfer_token);
  m_final_metadata_tokens.erase(transfer_token);
  m_aborted_tokens.insert(transfer_token);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::abort_token(
    uint64_t transfer_token, const std::string &reason) {
  std::lock_guard<std::mutex> guard(m_mutex);
  return abort_token_locked(transfer_token, reason, false,
                            Pending_frame_cleanup::REQUIRED);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::abort_epoch_locked(
    const std::string &reason) {
  if (m_sink == nullptr || !transfer_component_safe(m_epoch_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed || m_commit_in_progress ||
      (m_declared_tokens.empty() && !m_ack_uncertain)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  uint64_t first_pending_sequence = 0;
  for (const Preserve_trx_transfer_frame &frame :
       m_pending_final_metadata_frames) {
    if (first_pending_sequence == 0 || frame.sequence < first_pending_sequence) {
      first_pending_sequence = frame.sequence;
    }
  }
  if (first_pending_sequence != 0) {
    m_pending_final_metadata_frames.clear();
    m_final_metadata_tokens.clear();
    if (!m_final_metadata_accepted) {
      m_next_sequence = first_pending_sequence;
    }
  }

  Preserve_trx_transfer_status first_error = Preserve_trx_transfer_status::OK;
  for (uint64_t transfer_token : m_declared_tokens) {
    if (m_aborted_tokens.count(transfer_token) != 0) continue;
    const Preserve_trx_transfer_status status =
        abort_token_locked(transfer_token, reason, true,
                           Pending_frame_cleanup::ALREADY_CLEARED);
    if (status != Preserve_trx_transfer_status::OK &&
        first_error == Preserve_trx_transfer_status::OK) {
      first_error = status;
    }
  }
  if (first_error == Preserve_trx_transfer_status::OK && m_ack_uncertain) {
    first_error = Preserve_trx_transfer_status::ACK_UNCERTAIN;
  }
  if (first_error == Preserve_trx_transfer_status::OK ||
      !m_epoch_transport_open ||
      !m_frozen_commit_payload.empty()) {
    return first_error;
  }

  // The caller has joined all senders. An uncertain upload cannot safely be
  // replaced by token ABORT at the same sequence; use the epoch terminal CAS.
  std::string identity("PTRXFER_PRECOMMIT_ABANDON_V1");
  if (append_string(&identity, m_epoch_id) ||
      append_string(&identity, m_receiver_process_nonce)) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  Preserve_trx_transfer_frame abandon;
  abandon.type =
      Preserve_trx_transfer_frame_type::ABANDON_EPOCH_IF_NOT_COMMITTED;
  abandon.epoch_id = m_epoch_id;
  abandon.sequence = m_next_sequence;
  // Terminal cancellation is epoch-scoped, including an unacknowledged first
  // DECLARE for which no token has entered m_declared_tokens yet.
  abandon.token = 0;
  abandon.terminal_fact_digest = sha256_digest(identity);
  stamp_online_epoch_context_locked(&abandon);
  std::string encoded;
  auto status = preserve_trx_transfer_encode_frame(abandon, &encoded);
  if (status != Preserve_trx_transfer_status::OK) return status;
  const uint64_t deadline = std::min(
      m_absolute_monotonic_deadline_us,
      transfer_monotonic_us() +
          uint64_t{kPreserveTrxTransferOperationTimeoutMs} * 1000);
  do {
    status = m_sink->send_encoded_frame(encoded);
    if (status == Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN) {
      m_sink->release_epoch_transport();
      m_ack_uncertain = false;
      m_aborted_tokens = m_declared_tokens;
      return Preserve_trx_transfer_status::OK;
    }
    if (status != Preserve_trx_transfer_status::NOT_COMMITTED) return status;
    my_sleep(1000);
  } while (transfer_monotonic_us() < deadline);
  return Preserve_trx_transfer_status::ACK_UNCERTAIN;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::abort_epoch(
    const std::string &reason) {
  std::lock_guard<std::mutex> guard(m_mutex);
  return abort_epoch_locked(reason);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::cleanup_cancelled_epoch(
    const std::string &reason) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_epoch_committed || m_commit_in_progress) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  const Preserve_trx_transfer_status prepare_status =
      m_sink->prepare_cancelled_epoch_cleanup();
  if (prepare_status != Preserve_trx_transfer_status::OK) {
    return prepare_status;
  }
  if (m_frozen_commit_payload.empty()) {
    return abort_epoch_locked(reason);
  }

  std::string encoded_abandon;
  if (!build_epoch_abandon_payload(m_frozen_commit_payload, &encoded_abandon)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  const Preserve_trx_transfer_status status =
      m_sink->send_encoded_frame(encoded_abandon);
  if (status == Preserve_trx_transfer_status::OK ||
      status == Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN) {
    m_frozen_commit_payload.clear();
  }
  return status;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::commit_epoch() {
  return commit_epoch_impl(nullptr);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::commit_epoch(
    const std::vector<uint64_t> &session_only_tokens) {
  return commit_epoch_impl(&session_only_tokens);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::commit_epoch_impl(
    const std::vector<uint64_t> *session_only_tokens) {
  std::unique_lock<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || !transfer_component_safe(m_epoch_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed || m_commit_in_progress) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (m_ack_uncertain) return Preserve_trx_transfer_status::ACK_UNCERTAIN;
  if (session_only_tokens == nullptr &&
      (m_declared_tokens.empty() || m_finalized_tokens.empty())) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  for (uint64_t transfer_token : m_declared_tokens) {
    if (!token_resolved(transfer_token)) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
  }
  /* Every new COMMIT carries the canonical set codec, including an empty S. */
  const std::vector<uint64_t> empty_session_only_tokens;
  const std::vector<uint64_t> &commit_session_only_tokens =
      session_only_tokens == nullptr ? empty_session_only_tokens
                                     : *session_only_tokens;
  std::string canonical_session_only_payload;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_encode_session_only_tokens(
          commit_session_only_tokens, &canonical_session_only_payload);
  if (status != Preserve_trx_transfer_status::OK) return status;
  for (uint64_t finalized_token : m_finalized_tokens) {
    if (std::binary_search(commit_session_only_tokens.begin(),
                           commit_session_only_tokens.end(),
                           finalized_token)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }

  Preserve_trx_transfer_frame commit;
  std::string encoded_commit;
  if (!m_frozen_commit_payload.empty()) {
    status = preserve_trx_transfer_decode_frame(m_frozen_commit_payload,
                                                &commit);
    if (status != Preserve_trx_transfer_status::OK ||
        commit.type != Preserve_trx_transfer_frame_type::COMMIT_EPOCH ||
        commit.epoch_id != m_epoch_id ||
        commit.token !=
            (m_finalized_tokens.empty() ? 0 : *m_finalized_tokens.begin()) ||
        commit.sequence != m_next_sequence ||
        commit.manifest_payload != canonical_session_only_payload ||
        transfer_digest_is_zero(commit.terminal_fact_digest)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    try {
      encoded_commit = m_frozen_commit_payload;
    } catch (...) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    m_terminal_fact_digest = commit.terminal_fact_digest;
  } else {
    commit.type = Preserve_trx_transfer_frame_type::COMMIT_EPOCH;
    commit.epoch_id = m_epoch_id;
    commit.token =
        m_finalized_tokens.empty() ? 0 : *m_finalized_tokens.begin();
    commit.sequence = m_next_sequence;
    commit.manifest_payload = canonical_session_only_payload;
    stamp_online_epoch_context_locked(&commit);
    status = populate_source_commit_proof(&commit);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (m_finalized_tokens.empty()) {
      commit.terminal_fact_digest = session_only_control_fact_digest(commit);
    } else {
      Preserve_trx_transfer_epoch_fact source_fact;
      status = build_epoch_fact_from_manifests(
          m_finalized_manifests, commit.chunk_offset, commit.trx_id_store,
          &source_fact);
      if (status != Preserve_trx_transfer_status::OK) return status;
      commit.terminal_fact_digest =
          session_only_transaction_fact_digest(source_fact.fact_digest, commit);
    }
    if (transfer_digest_is_zero(commit.terminal_fact_digest)) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    m_terminal_fact_digest = commit.terminal_fact_digest;
    status = preserve_trx_transfer_encode_frame(commit, &encoded_commit);
    if (status != Preserve_trx_transfer_status::OK) return status;
  }

  const uint64_t source_fence_lsn = commit.chunk_offset;
  for (const auto &manifest : m_finalized_manifests) {
    if (manifest.source_freeze_lsn == 0 ||
        manifest.source_epoch_commit_lsn == 0 ||
        manifest.source_freeze_lsn > source_fence_lsn ||
        manifest.source_epoch_commit_lsn > source_fence_lsn) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }

  status = Preserve_trx_transfer_status::OK;
  std::vector<std::string> final_token_payloads;
  uint64_t final_encoded_bytes = 0;
  const bool send_pending_final_metadata = !m_final_metadata_accepted;
  if (send_pending_final_metadata &&
      !m_pending_final_metadata_frames.empty()) {
    std::vector<std::string> encoded_token_frames;
    uint64_t current_token = 0;
    uint32_t batched_tokens = 0;
    size_t final_token_count = 0;
    uint64_t counted_token = 0;
    for (const Preserve_trx_transfer_frame &frame :
         m_pending_final_metadata_frames) {
      if (frame.token != counted_token) {
        counted_token = frame.token;
        ++final_token_count;
      }
    }
    const size_t desired_payloads = std::min<size_t>(
        std::min<uint32_t>(
            std::max<uint32_t>(1, m_runtime_policy.sender_workers),
            std::max<uint32_t>(1, m_runtime_policy.data_sessions)),
        final_token_count);
    const size_t wire_batch_tokens =
        (final_token_count + desired_payloads - 1) / desired_payloads;
    auto flush_token_frames = [&]() {
      if (encoded_token_frames.empty()) {
        return Preserve_trx_transfer_status::OK;
      }
      std::vector<std::string> encoded_batches;
      Preserve_trx_transfer_status encode_status =
          encode_frame_batches_with_limit(encoded_token_frames,
                                          m_max_inflight_bytes,
                                          &encoded_batches);
      if (encode_status != Preserve_trx_transfer_status::OK) {
        return encode_status;
      }
      for (std::string &encoded_batch : encoded_batches) {
        if (encoded_batch.length() >
            std::numeric_limits<uint64_t>::max() - final_encoded_bytes) {
          return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
        }
        final_encoded_bytes += encoded_batch.length();
        final_token_payloads.push_back(std::move(encoded_batch));
      }
      encoded_token_frames.clear();
      batched_tokens = 0;
      return Preserve_trx_transfer_status::OK;
    };
    for (const Preserve_trx_transfer_frame &frame :
         m_pending_final_metadata_frames) {
      const bool next_token = current_token != frame.token;
      if (next_token && batched_tokens >= wire_batch_tokens) {
        status = flush_token_frames();
        if (status != Preserve_trx_transfer_status::OK) return status;
      }
      if (next_token) {
        current_token = frame.token;
        ++batched_tokens;
      }
      std::string encoded_frame;
      status = preserve_trx_transfer_encode_frame(frame, &encoded_frame);
      if (status != Preserve_trx_transfer_status::OK) return status;
      encoded_token_frames.push_back(std::move(encoded_frame));
    }
    status = flush_token_frames();
    if (status != Preserve_trx_transfer_status::OK) return status;
  }

  if (encoded_commit.length() >
      std::numeric_limits<uint64_t>::max() - final_encoded_bytes) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  final_encoded_bytes += encoded_commit.length();
  g_transfer_phase2_final_metadata_frame_count.fetch_add(
      (send_pending_final_metadata ? m_pending_final_metadata_frames.size()
                                   : 0) +
      1);
  g_transfer_phase2_final_metadata_encoded_bytes.fetch_add(final_encoded_bytes);

  Preserve_trx_transfer_encoded_frame_sink *const sink = m_sink;
  if (m_frozen_commit_payload.empty()) {
    try {
      m_frozen_commit_payload = encoded_commit;
    } catch (...) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
  }
  m_commit_in_progress = true;
  guard.unlock();
  if (!final_token_payloads.empty()) {
    const size_t worker_count = std::min<size_t>(
        std::min<uint32_t>(
            std::max<uint32_t>(1, m_runtime_policy.sender_workers),
            std::max<uint32_t>(1, m_runtime_policy.data_sessions)),
        final_token_payloads.size());
    std::mutex failure_mutex;
    Preserve_trx_transfer_status first_failure =
        Preserve_trx_transfer_status::OK;
    auto send_worker = [&](size_t worker_index) {
      for (size_t payload_index = worker_index;
           payload_index < final_token_payloads.size();
           payload_index += worker_count) {
        const std::string &payload = final_token_payloads[payload_index];
        throttle_source_transfer_io(payload.length(), m_runtime_policy);
        Preserve_trx_transfer_status send_status =
            Preserve_trx_transfer_status::OK;
        try {
          send_status =
              sink->send_encoded_frame_on_session(payload, worker_index);
        } catch (...) {
          send_status = Preserve_trx_transfer_status::UNSUPPORTED;
        }
        if (send_status != Preserve_trx_transfer_status::OK) {
          std::lock_guard<std::mutex> failure_guard(failure_mutex);
          if (first_failure == Preserve_trx_transfer_status::OK) {
            first_failure = send_status;
          }
        }
      }
    };
    std::vector<std::thread> workers;
    try {
      workers.reserve(worker_count);
      for (size_t worker = 0; worker < worker_count; ++worker) {
        workers.emplace_back(send_worker, worker);
      }
    } catch (...) {
      std::lock_guard<std::mutex> failure_guard(failure_mutex);
      first_failure = Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    for (std::thread &worker : workers) {
      if (worker.joinable()) worker.join();
    }
    status = first_failure;
  }
  const bool final_metadata_accepted_this_attempt =
      status == Preserve_trx_transfer_status::OK &&
      send_pending_final_metadata;

#ifndef DBUG_OFF
  if (!m_debug_commit_before_send_failure_consumed) {
    DBUG_EXECUTE_IF("preserve_trx_transfer_fail_commit_before_send_once", {
      m_debug_commit_before_send_failure_consumed = true;
      status = Preserve_trx_transfer_status::ACK_UNCERTAIN;
      DEBUG_SYNC(current_thd,
                 "preserve_trx_transfer_commit_before_send_failed");
    });
  }
#endif

  uint64_t ack_start_us = 0;
  uint64_t ack_end_us = 0;
  if (status == Preserve_trx_transfer_status::OK) {
    throttle_source_transfer_io(encoded_commit.length(), m_runtime_policy);
    bool send_allowed = true;
    DEBUG_SYNC(current_thd,
               "preserve_trx_transfer_before_commit_send_ownership_cas");
    if (m_before_commit_send != nullptr) {
      try {
        send_allowed =
            m_before_commit_send(m_before_commit_send_context);
      } catch (...) {
        send_allowed = false;
      }
    }
    if (!send_allowed) {
      status = Preserve_trx_transfer_status::UNSUPPORTED;
    } else {
      if (status == Preserve_trx_transfer_status::OK) {
        ack_start_us = transfer_monotonic_us();
        try {
          status = sink->send_encoded_frame(encoded_commit);
        } catch (...) {
          status = Preserve_trx_transfer_status::UNSUPPORTED;
        }
        ack_end_us = transfer_monotonic_us();
      }
    }
  }
  const bool committed_outcome = transfer_status_is_committed_outcome(status);
  const bool commit_acknowledged =
      status == Preserve_trx_transfer_status::OK || committed_outcome;
  if (commit_acknowledged) {
    DEBUG_SYNC(current_thd,
               "preserve_trx_transfer_after_commit_ack_before_final_ack");
  }
  guard.lock();
  m_commit_in_progress = false;
  if (final_metadata_accepted_this_attempt) {
    m_final_metadata_accepted = true;
  }
  if (commit_acknowledged &&
      ack_end_us >= ack_start_us) {
    g_transfer_phase2_final_metadata_ack_us.fetch_add(ack_end_us - ack_start_us);
  }
  bool final_ack_accepted = commit_acknowledged;
  if (final_ack_accepted && m_final_ack_arbiter != nullptr) {
    try {
      final_ack_accepted =
          m_final_ack_arbiter(m_final_ack_arbiter_context);
    } catch (...) {
      final_ack_accepted = false;
    }
    if (!final_ack_accepted) {
      status = Preserve_trx_transfer_status::UNSUPPORTED;
    }
  }
  if (final_ack_accepted) {
    ++m_next_sequence;
    m_pending_final_metadata_frames.clear();
    m_frozen_commit_payload.clear();
    m_epoch_committed = true;
  }
  const bool release_transport = m_epoch_committed;
  guard.unlock();
  if (release_transport) {
    try {
      sink->release_epoch_transport();
    } catch (...) {
      // FINAL_ACK already transferred ownership; transport teardown cannot
      // turn it back into a source/receiver protocol operation.
    }
  }
  return status;
}

Preserve_trx_transfer_status
preserve_trx_transfer_build_encoded_frame_sequence(
    const std::string &epoch_id, const Preserved_trx_bundle &bundle,
    uint64_t transfer_token, uint32_t chunk_bytes,
    std::vector<std::string> *encoded_frames,
    Preserve_trx_transfer_manifest *manifest) {
  if (encoded_frames == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  Preserve_trx_transfer_manifest built_manifest;
  std::vector<Preserve_trx_transfer_object_payload> objects;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_build_portable_objects(
          epoch_id, bundle, transfer_token, &built_manifest, &objects);
  if (status != Preserve_trx_transfer_status::OK) return status;

  std::vector<Preserve_trx_transfer_frame> frames;
  status =
      preserve_trx_transfer_build_frame_sequence(built_manifest, objects,
                                                chunk_bytes, &frames);
  if (status != Preserve_trx_transfer_status::OK) return status;

  std::vector<std::string> encoded;
  encoded.reserve(frames.size());
  for (const Preserve_trx_transfer_frame &frame : frames) {
    std::string encoded_frame;
    status = preserve_trx_transfer_encode_frame(frame, &encoded_frame);
    if (status != Preserve_trx_transfer_status::OK) return status;
    encoded.push_back(std::move(encoded_frame));
  }

  if (manifest != nullptr) *manifest = std::move(built_manifest);
  *encoded_frames = std::move(encoded);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_make_configured_frame_sink(
    std::unique_ptr<Preserve_trx_transfer_encoded_frame_sink> *sink,
    const Preserve_trx_transfer_runtime_policy *runtime_policy) {
  if (sink == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  sink->reset();
  if (!preserve_trx_transfer_source_endpoint_ready()) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  Preserve_trx_transfer_frame_sink_factory factory =
      configured_frame_sink_factory();
  if (factory != nullptr) return factory(sink);

  const Preserve_trx_transfer_client_ops *ops = configured_transfer_client_ops();
  if (ops == nullptr) return Preserve_trx_transfer_status::UNSUPPORTED;
  Preserve_trx_transfer_client_endpoint endpoint =
      configured_transfer_client_endpoint();
  Transfer_epoch_credential credential;
  if (!snapshot_transfer_epoch_credential(endpoint, &credential)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  endpoint.user = std::move(credential.user);
  endpoint.auth_plugin = std::move(credential.auth_plugin);
  const Preserve_trx_transfer_runtime_policy policy =
      runtime_policy == nullptr
          ? preserve_trx_transfer_current_runtime_policy()
          : *runtime_policy;
  sink->reset(new Preserve_trx_transfer_client_frame_sink(
      std::move(endpoint), std::move(credential.password), ops,
      policy.data_sessions));
  return Preserve_trx_transfer_status::OK;
}

namespace {

bool handoff_resolution_proof_matches_context(
    const Preserve_trx_handoff_resolution_proof &proof,
    const Preserve_trx_handoff_resolution_context &context) {
  return proof.epoch_id == context.epoch_id &&
         proof.final_fact_digest == context.final_fact_digest &&
         proof.source_process_generation == context.source_process_generation &&
         proof.receiver_process_generation ==
             context.receiver_process_generation;
}

bool same_handoff_resolution_proof(
    const Preserve_trx_handoff_resolution_proof &left,
    const Preserve_trx_handoff_resolution_proof &right) {
  return left.epoch_id == right.epoch_id &&
         left.final_fact_digest == right.final_fact_digest &&
         left.source_process_generation == right.source_process_generation &&
         left.receiver_process_generation ==
             right.receiver_process_generation &&
         left.ha_role_generation == right.ha_role_generation &&
         left.resolution == right.resolution;
}

}  // namespace

bool Preserve_trx_handoff_resolution_state::arm(
    const Preserve_trx_handoff_resolution_context &context) {
  if (context.epoch_id.empty() ||
      transfer_digest_is_zero(context.final_fact_digest) ||
      context.source_process_generation.empty() ||
      context.receiver_process_generation.empty()) {
    return false;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_started) return false;
  if (m_armed) {
    return context.epoch_id == m_context.epoch_id &&
           context.final_fact_digest == m_context.final_fact_digest &&
           context.source_process_generation ==
               m_context.source_process_generation &&
           context.receiver_process_generation ==
               m_context.receiver_process_generation;
  }
  m_context = context;
  m_armed = true;
  return true;
}

Preserve_trx_transfer_status Preserve_trx_handoff_resolution_state::begin(
    const Preserve_trx_ha_control_capability &capability,
    const Preserve_trx_handoff_resolution_proof &proof,
    bool *first_decision) {
  if (first_decision != nullptr) *first_decision = false;
  if (!capability.valid()) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (proof.ha_role_generation != capability.role_generation()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  std::lock_guard<std::mutex> guard(m_mutex);
  if (!m_armed || !handoff_resolution_proof_matches_context(proof, m_context)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (m_started) {
    if (!same_handoff_resolution_proof(proof, m_accepted_proof)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    return m_completed ? m_result
                       : Preserve_trx_transfer_status::ACK_UNCERTAIN;
  }

  m_accepted_proof = proof;
  m_started = true;
  if (first_decision != nullptr) *first_decision = true;
  return Preserve_trx_transfer_status::OK;
}

bool Preserve_trx_handoff_resolution_state::complete(
    const Preserve_trx_handoff_resolution_proof &proof,
    Preserve_trx_transfer_status result) {
  if (result == Preserve_trx_transfer_status::OK ||
      result == Preserve_trx_transfer_status::ACK_UNCERTAIN) {
    return false;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  if (!m_started || !same_handoff_resolution_proof(proof, m_accepted_proof)) {
    return false;
  }
  if (m_completed) return result == m_result;
  m_result = result;
  m_completed = true;
  return true;
}

bool Preserve_trx_handoff_resolution_state::matches_context(
    const Preserve_trx_handoff_resolution_proof &proof) const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_armed && handoff_resolution_proof_matches_context(proof, m_context);
}

Preserve_trx_transfer_password_status
preserved_trx_transfer_set_runtime_password(
    const unsigned char *password, size_t password_length) {
  const Preserve_trx_transfer_password_status role_status =
      transfer_runtime_password_source_mode_status();
  if (role_status != Preserve_trx_transfer_password_status::OK) {
    return role_status;
  }
  if (password == nullptr || password_length == 0 || password_length > 256 ||
      std::find(password, password + password_length, '\0') !=
          password + password_length) {
    return Preserve_trx_transfer_password_status::INVALID_ARGUMENT;
  }

  std::shared_ptr<const Transfer_epoch_password> replacement =
      make_transfer_epoch_password(password, password_length);
  if (replacement == nullptr) {
    return Preserve_trx_transfer_password_status::RESOURCE_EXHAUSTED;
  }

  std::shared_ptr<const Transfer_epoch_password> previous;
  {
    std::lock_guard<std::mutex> guard(g_transfer_runtime_password_mutex);
    previous = std::move(g_transfer_runtime_password);
    g_transfer_runtime_password = std::move(replacement);
    g_transfer_runtime_password_managed = true;
  }
  previous.reset();
  return Preserve_trx_transfer_password_status::OK;
}

Preserve_trx_transfer_password_status
preserved_trx_transfer_clear_runtime_password() {
  const Preserve_trx_transfer_password_status role_status =
      transfer_runtime_password_source_mode_status();
  if (role_status != Preserve_trx_transfer_password_status::OK) {
    return role_status;
  }

  std::shared_ptr<const Transfer_epoch_password> previous;
  {
    std::lock_guard<std::mutex> guard(g_transfer_runtime_password_mutex);
    previous = std::move(g_transfer_runtime_password);
    g_transfer_runtime_password_managed = true;
  }
  previous.reset();
  return Preserve_trx_transfer_password_status::OK;
}

void preserve_trx_transfer_set_client_ops_for_unit_test(
    const Preserve_trx_transfer_client_ops *ops) {
  unit_transfer_client_ops() = ops;
}

void preserve_trx_transfer_reset_runtime_password_for_unit_test() {
  std::shared_ptr<const Transfer_epoch_password> previous;
  {
    std::lock_guard<std::mutex> guard(g_transfer_runtime_password_mutex);
    previous = std::move(g_transfer_runtime_password);
    g_transfer_runtime_password_managed = false;
  }
  previous.reset();
}

size_t preserve_trx_transfer_cleanse_mysql_passwords_for_unit_test(
    MYSQL *mysql) {
  return cleanse_transfer_mysql_password_copies(mysql);
}

void preserve_trx_transfer_set_codec_context_provider_for_unit_test(
    Preserve_trx_transfer_codec_context_provider provider) {
  unit_codec_context_provider() = provider;
}

void preserve_trx_transfer_set_source_lsn_provider_for_unit_test(
    Preserve_trx_transfer_source_lsn_provider provider) {
  unit_source_lsn_provider() = provider;
}

void preserve_trx_transfer_set_source_trx_id_store_provider_for_unit_test(
    Preserve_trx_transfer_source_trx_id_store_provider provider) {
  unit_source_trx_id_store_provider() = provider;
}

void preserve_trx_transfer_set_source_resurrection_provider_for_unit_test(
    Preserve_trx_transfer_source_resurrection_provider provider) {
  unit_source_resurrection_provider() = provider;
}

void preserve_trx_transfer_set_terminal_lock_proof_provider_for_unit_test(
    Preserve_trx_transfer_terminal_lock_proof_provider provider) {
  unit_terminal_lock_proof_provider() = provider;
}

void preserve_trx_transfer_set_frame_sink_factory_for_unit_test(
    Preserve_trx_transfer_frame_sink_factory factory) {
  configured_frame_sink_factory() = factory;
}

void preserve_trx_transfer_set_receiver_staged_prewarm_delay_ms_for_unit_test(
    uint delay_ms) {
  receiver_staged_prewarm_delay_ms_for_unit_test().store(delay_ms);
}

void preserve_trx_transfer_set_receiver_object_prewarm_delay_ms_for_unit_test(
    uint delay_ms) {
  receiver_object_prewarm_delay_ms_for_unit_test().store(delay_ms);
}

void preserve_trx_transfer_put_receiver_object_prewarm_proof_for_unit_test(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id, uint64_t page_count,
    uint64_t resident_pages, uint64_t cold_gets, uint64_t bitmap_pages,
    uint64_t bitmap_bits, bool metadata_only) {
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, object_id);
  if (root_dir.empty() || object == nullptr || manifest.epoch_id.empty() ||
      manifest.token == 0) {
    return;
  }
  Receiver_object_prewarm_key key;
  key.root_dir = root_dir;
  key.epoch_id = manifest.epoch_id;
  key.token = manifest.token;
  key.object_id = object_id;
  key.digest = object->digest;
  key.source_live_generation = object->lock_plan.source_live_generation;
  key.source_live_digest = object->lock_plan.source_live_digest;

  Receiver_object_prewarm_proof proof;
  proof.record_lock_object = object_id == kPreservedTrxBlobRecordLocks;
  proof.page_count = page_count;
  proof.resident_pages = resident_pages;
  proof.cold_gets = cold_gets;
  proof.bitmap_pages = bitmap_pages;
  proof.bitmap_bits = bitmap_bits;
  proof.metadata_only = metadata_only;

  std::lock_guard<std::mutex> guard(g_receiver_object_prewarm_proof_mutex);
  g_receiver_object_prewarm_proofs[key] = proof;
}

bool preserve_trx_transfer_receiver_object_proof_metadata_only_for_unit_test(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id) {
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, object_id);
  if (root_dir.empty() || object == nullptr) return false;
  Receiver_object_prewarm_key key;
  key.root_dir = root_dir;
  key.epoch_id = manifest.epoch_id;
  key.token = manifest.token;
  key.object_id = object_id;
  key.digest = object->digest;
  key.source_live_generation = object->lock_plan.source_live_generation;
  key.source_live_digest = object->lock_plan.source_live_digest;
  std::lock_guard<std::mutex> guard(g_receiver_object_prewarm_proof_mutex);
  const auto found = g_receiver_object_prewarm_proofs.find(key);
  return found != g_receiver_object_prewarm_proofs.end() &&
         found->second.metadata_only;
}

Preserve_trx_transfer_status preserve_trx_transfer_send_bundle_frames(
    const std::string &epoch_id, const Preserved_trx_bundle &bundle,
    uint64_t transfer_token, uint32_t chunk_bytes,
    Preserve_trx_transfer_encoded_frame_sink *sink,
    Preserve_trx_transfer_manifest *manifest,
    const Preserve_trx_resurrection_index_entry *resurrection_entry) {
  if (sink == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  Preserve_trx_transfer_manifest built_manifest;
  std::vector<Preserve_trx_transfer_object_payload> objects;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_build_portable_objects(
          epoch_id, bundle, transfer_token, &built_manifest, &objects,
          resurrection_entry);
  if (status != Preserve_trx_transfer_status::OK) {
    const std::string message =
        "PRESERVE: standby transfer build portable objects failed status=" +
        transfer_status_name(status) + " epoch=" + epoch_id +
        " token=" + std::to_string(transfer_token);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return status;
  }

  std::string manifest_payload;
  status = preserve_trx_transfer_encode_manifest(built_manifest,
                                                 &manifest_payload);
  if (status != Preserve_trx_transfer_status::OK) {
    const std::string message =
        "PRESERVE: standby transfer encode manifest failed status=" +
        transfer_status_name(status) + " epoch=" + epoch_id +
        " token=" + std::to_string(transfer_token);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return status;
  }

  uint64_t inflight_bytes = manifest_payload.length();
  for (const Preserve_trx_transfer_object_payload &object : objects) {
    if (object.payload.length() >
        std::numeric_limits<uint64_t>::max() - inflight_bytes) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    inflight_bytes += object.payload.length();
  }
  if (inflight_bytes > preserve_trx_transfer_max_inflight_bytes) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }

  std::vector<Preserve_trx_transfer_frame> frames;
  status = preserve_trx_transfer_build_frame_sequence(built_manifest, objects,
                                                      chunk_bytes, &frames);
  if (status != Preserve_trx_transfer_status::OK) {
    const std::string message =
        "PRESERVE: standby transfer build frame sequence failed status=" +
        transfer_status_name(status) + " epoch=" + epoch_id +
        " token=" + std::to_string(transfer_token);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return status;
  }

  if (manifest != nullptr) *manifest = built_manifest;

  Preserve_trx_transfer_frame declare;
  declare.type = Preserve_trx_transfer_frame_type::DECLARE_TOKEN;
  declare.sequence = 1;
  declare.epoch_id = built_manifest.epoch_id;
  declare.token = built_manifest.token;
  std::string encoded_declare;
  status = preserve_trx_transfer_encode_frame(declare, &encoded_declare);
  if (status != Preserve_trx_transfer_status::OK) return status;
  status = sink->send_encoded_frame(encoded_declare);
  if (status != Preserve_trx_transfer_status::OK) return status;

  for (const Preserve_trx_transfer_frame &frame : frames) {
    Preserve_trx_transfer_frame outgoing_frame = frame;
    ++outgoing_frame.sequence;
    std::string encoded_frame;
    status = preserve_trx_transfer_encode_frame(outgoing_frame, &encoded_frame);
    if (status != Preserve_trx_transfer_status::OK) {
      const std::string message =
          "PRESERVE: standby transfer encode frame failed status=" +
          transfer_status_name(status) + " epoch=" + epoch_id +
          " token=" + std::to_string(transfer_token) +
          " frame_type=" +
          std::to_string(static_cast<int>(outgoing_frame.type));
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
      return status;
    }
    status = sink->send_encoded_frame(encoded_frame);
    if (status != Preserve_trx_transfer_status::OK) {
      Preserve_trx_transfer_frame abort;
      abort.type = Preserve_trx_transfer_frame_type::ABORT;
      abort.sequence = outgoing_frame.sequence + 1;
      abort.epoch_id = built_manifest.epoch_id;
      abort.token = built_manifest.token;
      abort.reason = "source_send_failed:" + transfer_status_name(status);
      std::string encoded_abort;
      if (preserve_trx_transfer_encode_frame(abort, &encoded_abort) ==
          Preserve_trx_transfer_status::OK) {
        (void)sink->send_encoded_frame(encoded_abort);
      }
      return status;
    }
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_send_epoch_bundles(
    const std::string &epoch_id,
    const std::vector<Preserved_trx_bundle> &bundles,
    const std::vector<uint64_t> &transfer_tokens, uint32_t chunk_bytes,
    Preserve_trx_transfer_encoded_frame_sink *sink,
    std::vector<Preserve_trx_transfer_manifest> *manifests) {
  if (sink == nullptr || chunk_bytes == 0 || bundles.empty() ||
      bundles.size() != transfer_tokens.size()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  struct Token_payload {
    Preserve_trx_transfer_manifest manifest;
    std::string manifest_payload;
    std::vector<Preserve_trx_transfer_object_payload> objects;
  };

  std::vector<Token_payload> tokens;
  tokens.reserve(bundles.size());
  std::set<uint64_t> token_names;
  uint64_t inflight_bytes = 0;
  for (const Preserved_trx_bundle &bundle : bundles) {
    Token_payload token;
    Preserve_trx_transfer_status status =
        preserve_trx_transfer_build_portable_objects(
            epoch_id, bundle, transfer_tokens[tokens.size()],
            &token.manifest, &token.objects);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (!token_names.insert(token.manifest.token).second) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    status = preserve_trx_transfer_encode_manifest(token.manifest,
                                                   &token.manifest_payload);
    if (status != Preserve_trx_transfer_status::OK) return status;

    if (token.manifest_payload.length() >
        std::numeric_limits<uint64_t>::max() - inflight_bytes) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    inflight_bytes += token.manifest_payload.length();
    for (const Preserve_trx_transfer_object_payload &object : token.objects) {
      if (object.payload.length() >
          std::numeric_limits<uint64_t>::max() - inflight_bytes) {
        return Preserve_trx_transfer_status::UNSUPPORTED;
      }
      inflight_bytes += object.payload.length();
    }
    tokens.push_back(std::move(token));
  }
  if (inflight_bytes > preserve_trx_transfer_max_inflight_bytes) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }

  uint64_t sequence = 1;
  std::vector<uint64_t> declared_tokens;
  auto send_frame = [&](const Preserve_trx_transfer_frame &frame) {
    std::string encoded_frame;
    Preserve_trx_transfer_status status =
        preserve_trx_transfer_encode_frame(frame, &encoded_frame);
    if (status != Preserve_trx_transfer_status::OK) return status;
    return sink->send_encoded_frame(encoded_frame);
  };
  auto abort_declared_tokens =
      [&](Preserve_trx_transfer_status original_status) {
    for (uint64_t token : declared_tokens) {
      Preserve_trx_transfer_frame abort;
      abort.type = Preserve_trx_transfer_frame_type::ABORT;
      abort.sequence = sequence++;
      abort.epoch_id = epoch_id;
      abort.token = token;
      abort.reason = "source_send_failed:" +
                     transfer_status_name(original_status);
      (void)send_frame(abort);
    }
  };

  for (const Token_payload &token : tokens) {
    Preserve_trx_transfer_frame declare;
    declare.type = Preserve_trx_transfer_frame_type::DECLARE_TOKEN;
    declare.sequence = sequence++;
    declare.epoch_id = token.manifest.epoch_id;
    declare.token = token.manifest.token;
    const Preserve_trx_transfer_status status = send_frame(declare);
    if (status != Preserve_trx_transfer_status::OK) {
      abort_declared_tokens(status);
      return status;
    }
    declared_tokens.push_back(token.manifest.token);
  }

  for (const Token_payload &token : tokens) {
    Preserve_trx_transfer_frame begin;
    begin.type = Preserve_trx_transfer_frame_type::BEGIN;
    begin.sequence = sequence++;
    begin.epoch_id = token.manifest.epoch_id;
    begin.token = token.manifest.token;
    begin.manifest_payload = token.manifest_payload;
    const Preserve_trx_transfer_status status = send_frame(begin);
    if (status != Preserve_trx_transfer_status::OK) {
      abort_declared_tokens(status);
      return status;
    }
  }

  for (const Token_payload &token : tokens) {
    for (const Preserve_trx_transfer_object_descriptor &descriptor :
         token.manifest.objects) {
      const Preserve_trx_transfer_object_payload *object_payload = nullptr;
      for (const Preserve_trx_transfer_object_payload &candidate :
           token.objects) {
        if (candidate.descriptor.object_id == descriptor.object_id) {
          object_payload = &candidate;
          break;
        }
      }
      if (object_payload == nullptr ||
          object_payload->descriptor.kind != descriptor.kind ||
          object_payload->descriptor.flags != descriptor.flags ||
          object_payload->descriptor.total_size != descriptor.total_size ||
          object_payload->descriptor.digest != descriptor.digest ||
          object_payload->payload.length() != descriptor.total_size ||
          sha256_digest(object_payload->payload) != descriptor.digest) {
        abort_declared_tokens(Preserve_trx_transfer_status::CORRUPT);
        return Preserve_trx_transfer_status::CORRUPT;
      }

      for (uint64_t offset = 0; offset < object_payload->payload.length();
           offset += chunk_bytes) {
        const size_t length = std::min<uint64_t>(
            chunk_bytes, object_payload->payload.length() - offset);
        Preserve_trx_transfer_frame chunk;
        chunk.type = Preserve_trx_transfer_frame_type::OBJECT_CHUNK;
        chunk.sequence = sequence++;
        chunk.epoch_id = token.manifest.epoch_id;
        chunk.token = token.manifest.token;
        chunk.object_id = descriptor.object_id;
        chunk.chunk_offset = offset;
        chunk.chunk_payload = object_payload->payload.substr(offset, length);
        const Preserve_trx_transfer_status status = send_frame(chunk);
        if (status != Preserve_trx_transfer_status::OK) {
          abort_declared_tokens(status);
          return status;
        }
      }

      Preserve_trx_transfer_frame seal;
      seal.type = Preserve_trx_transfer_frame_type::SEAL_OBJECT;
      seal.sequence = sequence++;
      seal.epoch_id = token.manifest.epoch_id;
      seal.token = token.manifest.token;
      seal.object_id = descriptor.object_id;
      const Preserve_trx_transfer_status status = send_frame(seal);
      if (status != Preserve_trx_transfer_status::OK) {
        abort_declared_tokens(status);
        return status;
      }
    }
  }

  {
    Preserve_trx_transfer_frame commit;
    commit.type = Preserve_trx_transfer_frame_type::COMMIT_EPOCH;
    commit.sequence = sequence++;
    commit.epoch_id = tokens.front().manifest.epoch_id;
    commit.token = tokens.front().manifest.token;
    Preserve_trx_transfer_status status = populate_source_commit_proof(&commit);
    if (status == Preserve_trx_transfer_status::OK) {
      status = send_frame(commit);
    }
    if (status != Preserve_trx_transfer_status::OK) {
      abort_declared_tokens(status);
      return status;
    }
  }

  if (manifests != nullptr) {
    manifests->clear();
    manifests->reserve(tokens.size());
    for (const Token_payload &token : tokens) {
      manifests->push_back(token.manifest);
    }
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_stage_object_chunk(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id, uint64_t chunk_offset,
    const std::string &chunk_payload) {
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, object_id);
  if (root_dir.empty() || !transfer_component_safe(manifest.epoch_id) ||
      manifest.token == 0 || object == nullptr ||
      !transfer_component_safe(object_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (chunk_offset > object->total_size ||
      chunk_payload.length() > object->total_size - chunk_offset) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  const Preserve_trx_transfer_status dir_status =
      ensure_transfer_token_dir(root_dir, manifest);
  if (dir_status != Preserve_trx_transfer_status::OK) return dir_status;

  /* Throttle before taking the per-object staging mutex. */
  throttle_receiver_saved_io(chunk_payload.length());

  std::mutex &object_mutex =
      transfer_object_stage_mutex(root_dir, manifest, object_id);
  std::lock_guard<std::mutex> object_guard(object_mutex);

  const std::string path = transfer_object_path(root_dir, manifest, *object);
  // Open once: cleanup may unlink a path while this writer is active. Never
  // compare one inode and then reopen the path to write a different inode.
  File file = my_open(path.c_str(), O_RDWR | O_NOFOLLOW, MYF(0));
  if (file < 0) {
    file = my_create(path.c_str(), 0600, O_RDWR | O_CREAT | O_EXCL, MYF(0));
    if (file < 0 && my_errno() == EEXIST) {
      file = my_open(path.c_str(), O_RDWR | O_NOFOLLOW, MYF(0));
    }
    if (file < 0) return Preserve_trx_transfer_status::IO_ERROR;
  }
  auto close_file = create_scope_guard([&] {
    if (file >= 0) my_close(file, MYF(0));
  });
  MY_STAT stat_area;
  if (my_fstat(file, &stat_area) != 0) {
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  if (!MY_S_ISREG(stat_area.st_mode) || stat_area.st_size < 0) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (object_id == kPreservedTrxBlobBinlogCache)
    preserved_trx_receiver_binlog_prefix_note_write(
        root_dir, manifest, chunk_offset, &stat_area);
  size_t overlap = 0;
  if (chunk_offset < static_cast<uint64_t>(stat_area.st_size)) {
    overlap =
        std::min<uint64_t>(chunk_payload.length(),
                           static_cast<uint64_t>(stat_area.st_size) -
                               chunk_offset);
    std::string existing;
    const Preserve_trx_transfer_status read_status =
        read_existing_overlap(file, chunk_offset, overlap, &existing);
    if (read_status != Preserve_trx_transfer_status::OK) return read_status;
    if (existing != chunk_payload.substr(0, overlap)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }
  // A verified prefix is immutable even on retransmission. Still append the
  // full range below: a previous data write may have outlived a range failure.
  const size_t tail = chunk_payload.size() - overlap;
  auto write_status = Preserve_trx_transfer_status::OK;
  if (tail != 0) {
    DBUG_EXECUTE_IF("preserve_transfer_fail_file_tail_write", {
      return Preserve_trx_transfer_status::IO_ERROR;
    });
    if (my_pwrite(file,
                  reinterpret_cast<const unsigned char *>(
                      chunk_payload.data() + overlap),
                  tail, chunk_offset + overlap, MYF(0)) != tail) {
      write_status = Preserve_trx_transfer_status::IO_ERROR;
    }
  }
  const bool close_error = my_close(file, MYF(0)) != 0;
  file = -1;
  if (close_error) write_status = Preserve_trx_transfer_status::IO_ERROR;
  const Preserve_trx_transfer_status status =
      write_status == Preserve_trx_transfer_status::OK
          ? append_range_to_file(
                transfer_object_range_path(root_dir, manifest, *object),
                chunk_offset, chunk_payload.length())
          : write_status;
  if (status != Preserve_trx_transfer_status::OK &&
      object_id == kPreservedTrxBlobBinlogCache)
    preserved_trx_receiver_binlog_prefix_note_write(root_dir, manifest, 0, nullptr);
  return status;
}

Preserve_trx_transfer_status preserve_trx_transfer_seal_staged_object(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id,
    std::shared_ptr<const Preserve_trx_sealed_file> *file) {
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, object_id);
  if (root_dir.empty() || !transfer_component_safe(manifest.epoch_id) ||
      manifest.token == 0 || object == nullptr ||
      !transfer_component_safe(object_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::mutex &object_mutex =
      transfer_object_stage_mutex(root_dir, manifest, object_id);
  std::lock_guard<std::mutex> object_guard(object_mutex);

  const std::string path = transfer_object_path(root_dir, manifest, *object);
  if (!staged_ranges_cover_object(root_dir, manifest, *object)) {
    if (object_id == kPreservedTrxBlobBinlogCache)
      preserved_trx_receiver_binlog_prefix_note_write(root_dir, manifest, 0, nullptr);
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (file != nullptr) {
    switch (Preserve_trx_sealed_file::open_verified(
        path, object->total_size, object->digest, file)) {
      case Preserve_trx_file_status::OK:
        return Preserve_trx_transfer_status::OK;
      case Preserve_trx_file_status::CORRUPT:
        return Preserve_trx_transfer_status::CORRUPT;
      case Preserve_trx_file_status::OUT_OF_MEMORY:
        return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
      case Preserve_trx_file_status::IO_ERROR:
        return Preserve_trx_transfer_status::IO_ERROR;
    }
  }
  Preserve_trx_transfer_status prefix_status;
  if (object_id == kPreservedTrxBlobBinlogCache &&
      preserved_trx_receiver_binlog_prefix_verify_file(
          root_dir, manifest, *object, path, &prefix_status)) return prefix_status;
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  const Preserve_trx_transfer_status digest_status =
      sha256_digest_file_streaming(path, object->total_size, &digest);
  if (digest_status != Preserve_trx_transfer_status::OK) return digest_status;
  return digest == object->digest ? Preserve_trx_transfer_status::OK
                                  : Preserve_trx_transfer_status::CORRUPT;
}

#ifndef NDEBUG
// Internal validation uses the same admission/cleanup transitions as wire
// traffic. No files need to be materialized to exercise reservation accounting.
static bool probe_receiver_reservation_ledger(const std::string &root) {
  using Status = Preserve_trx_transfer_status;
  using State = Preserve_trx_transfer_receiver_state;
  const auto saved_limit = preserve_trx_transfer_max_inflight_bytes;
  auto restore = create_scope_guard([&] {
    preserve_trx_transfer_max_inflight_bytes = saved_limit;
  });
  Preserve_trx_transfer_object_descriptor object;
  object.kind = Preserve_trx_transfer_object_kind::EXTERNAL_BLOB;
  object.object_id = "ledger-one";
  object.total_size = 100;
  uint64_t unit = 0;
  if (receiver_object_reserved_bytes(object, &unit) != Status::OK) return false;
  // A terminal marker must not release disk admission before deletion. Test
  // both terminal transitions, then move the same charge into global debt.
  for (const auto terminal : {State::ABORTED, State::CORRUPT}) {
    Preserve_trx_transfer_receiver_registry failed;
    preserve_trx_transfer_max_inflight_bytes = unit;
    if (failed.declare_token("terminal-a", 1) != Status::OK ||
        failed.declare_token("terminal-a", 2) != Status::OK ||
        failed.declare_token("terminal-b", 1) != Status::OK ||
        failed.declare_object("terminal-a", 1, object) != Status::OK)
      return false;
    const auto marked = terminal == State::ABORTED
        ? failed.mark_aborted("terminal-a", 1, "ledger-test")
        : failed.mark_corrupt("terminal-a", 1, "ledger-test");
    if (marked != Status::OK || failed.status_counts().inflight_bytes != unit ||
        failed.declare_object("terminal-a", 2, object) != Status::RESOURCE_EXHAUSTED ||
        failed.mark_cleanup_pending(root, "terminal-a", 1, 0, terminal,
                                     "ledger-test") != Status::OK ||
        failed.mark_cleanup_pending(root, "terminal-a", 1, 0, terminal,
                                     "ledger-test") != Status::OK ||
        failed.status_counts().inflight_bytes != unit ||
        failed.declare_object("terminal-b", 1, object) != Status::RESOURCE_EXHAUSTED ||
        failed.retry_cleanup_debt_once(1000000) != 1 ||
        failed.status_counts().inflight_bytes != 0 ||
        failed.declare_object("terminal-b", 1, object) != Status::OK)
      return false;
  }
  {
    // A failed replacement may already have published a smaller descriptor;
    // charge the retained old files once, without inflating the live ledger.
    Preserve_trx_transfer_receiver_registry replacement;
    preserve_trx_transfer_max_inflight_bytes = 2 * unit;
    if (replacement.declare_token("replacement-a", 1) != Status::OK ||
        replacement.declare_token("replacement-b", 1) != Status::OK ||
        replacement.declare_object("replacement-a", 1, object) != Status::OK ||
        replacement.mark_cleanup_pending(root, "replacement-a", 1, 0,
            State::CORRUPT, "ledger-test", 2 * unit) != Status::OK ||
        replacement.mark_cleanup_pending(root, "replacement-a", 1, 0,
            State::CORRUPT, "ledger-test", 2 * unit) != Status::OK ||
        replacement.status_counts().inflight_bytes != 2 * unit ||
        replacement.declare_object("replacement-b", 1, object) != Status::RESOURCE_EXHAUSTED ||
        replacement.retry_cleanup_debt_once(1000000) != 1 ||
        replacement.status_counts().inflight_bytes != 0 ||
        replacement.declare_object("replacement-b", 1, object) != Status::OK)
      return false;
  }
  for (const bool abort : {true, false}) {
    // Reach the production frame handlers, including a real staging file and
    // one failed deletion, rather than only testing registry transitions.
    Preserve_trx_transfer_receiver_registry failed;
    preserve_trx_transfer_max_inflight_bytes = saved_limit;
    Preserve_trx_transfer_manifest manifest;
    manifest.epoch_id = abort ? "ledger-wire-abort" : "ledger-wire-corrupt";
    manifest.token = 1;
    manifest.objects.push_back(object);
    auto cleanup = create_scope_guard([&] {
      preserve_trx_transfer_set_staging_cleanup_failures_for_unit_test(0);
      cleanup_transfer_token_staging(root, manifest.epoch_id, 1);
    });
    auto store = create_preserved_trx_process_local_store(root);
    if (failed.begin_receive(manifest) != Status::OK) return false;
    Preserve_trx_transfer_frame frame;
    frame.epoch_id = manifest.epoch_id;
    frame.token = 1;
    frame.sequence = 1;
    frame.type = Preserve_trx_transfer_frame_type::OBJECT_CHUNK;
    frame.object_id = object.object_id;
    frame.chunk_payload = "x";
    if (preserve_trx_transfer_apply_receiver_frame(
            root, frame, &store.store(), &failed, 30) != Status::OK)
      return false;
    const auto reserved = failed.status_counts().inflight_bytes;
    if (!reserved) return false;
    preserve_trx_transfer_max_inflight_bytes = reserved;
    ++frame.sequence;
    if (abort) {
      frame.type = Preserve_trx_transfer_frame_type::ABORT;
      frame.object_id.clear();
      frame.chunk_payload.clear();
    } else {
      frame.object_id = "unknown-object";
    }
    preserve_trx_transfer_set_staging_cleanup_failures_for_unit_test(1);
    const auto status = preserve_trx_transfer_apply_receiver_frame(
        root, frame, &store.store(), &failed, 30);
    Preserve_trx_transfer_receiver_record record;
    if (status != (abort ? Status::OK : Status::INVALID_ARGUMENT) ||
        !failed.lookup(manifest.epoch_id, 1, &record) ||
        record.state != State::CLEANUP_PENDING || record.reserved_bytes != 0 ||
        failed.status_counts().inflight_bytes != reserved ||
        failed.declare_token("ledger-wire-other", 1) != Status::OK ||
        failed.declare_object("ledger-wire-other", 1, object) != Status::RESOURCE_EXHAUSTED ||
        failed.retry_cleanup_debt_once(UINT64_MAX) != 1 ||
        failed.status_counts().inflight_bytes != 0 ||
        !failed.lookup(manifest.epoch_id, 1, &record) ||
        record.state != (abort ? State::ABORTED : State::CORRUPT) ||
        failed.declare_object("ledger-wire-other", 1, object) != Status::OK)
      return false;
  }
  preserve_trx_transfer_max_inflight_bytes = 3 * unit;
  Preserve_trx_transfer_receiver_registry registry;
  const auto declare = [&](const char *epoch, uint64_t token, const char *name) {
    auto descriptor = object;
    descriptor.object_id = name;
    return registry.declare_object(epoch, token, descriptor);
  };
  if (registry.declare_token("ledger-a", 1) != Status::OK ||
      registry.declare_token("ledger-b", 1) != Status::OK ||
      registry.declare_token("ledger-b", 2) != Status::OK ||
      declare("ledger-a", 1, "one") != Status::OK ||
      declare("ledger-a", 1, "two") != Status::OK ||
      declare("ledger-b", 1, "one") != Status::OK ||
      declare("ledger-b", 1, "two") != Status::OK ||
      registry.mark_cleanup_pending(root, "ledger-a", 1, 0, State::ABORTED,
                                      "ledger-test") != Status::OK ||
      registry.mark_cleanup_pending(root, "ledger-a", 1, 0, State::ABORTED,
                                      "ledger-test") != Status::OK ||
      declare("ledger-b", 2, "one") != Status::RESOURCE_EXHAUSTED ||
      registry.mark_corrupt("ledger-b", 1, "ledger-test") != Status::OK ||
      registry.complete_staging_cleanup("ledger-b", 1) != Status::OK ||
      registry.complete_staging_cleanup("ledger-b", 1) != Status::OK ||
      declare("ledger-b", 2, "one") != Status::OK ||
      declare("ledger-b", 2, "one") != Status::OK ||
      declare("ledger-b", 2, "two") != Status::RESOURCE_EXHAUSTED ||
      registry.retry_cleanup_debt_once(1000000) != 1 ||
      declare("ledger-b", 2, "two") != Status::OK ||
      declare("ledger-b", 2, "three") != Status::OK ||
      declare("ledger-b", 2, "four") != Status::RESOURCE_EXHAUSTED)
    return false;
  Preserve_trx_transfer_receiver_record before, after;
  if (!registry.lookup("ledger-b", 2, &before)) return false;
  auto manifest = receiver_record_manifest(before);
  if (registry.begin_receive(manifest, 1) != Status::RESOURCE_EXHAUSTED ||
      !registry.lookup("ledger-b", 2, &after) ||
      after.reserved_bytes != before.reserved_bytes ||
      registry.begin_receive(manifest, 0) != Status::OK ||
      registry.mark_saved_online("ledger-b", 2) != Status::OK ||
      registry.declare_token("ledger-b", 3) != Status::OK ||
      declare("ledger-b", 3, "one") != Status::OK ||
      registry.mark_aborted("ledger-b", 3, "ledger-test") != Status::OK ||
      registry.complete_staging_cleanup("ledger-b", 3) != Status::OK)
    return false;

  preserve_trx_transfer_max_inflight_bytes = UINT64_MAX;
  object.total_size = UINT64_MAX / 2 + 1 - (unit - 100);
  for (const char *epoch : {"ledger-over-a", "ledger-over-b"}) {
    if (registry.declare_token(epoch, 1) != Status::OK ||
        declare(epoch, 1, "huge") != Status::OK) return false;
  }
  for (const char *epoch : {"ledger-over-a", "ledger-over-b"}) {
    if (registry.mark_cleanup_pending(root, epoch, 1,
            std::string(epoch) == "ledger-over-a" ? 1000000 : 2000000,
            State::ABORTED, "ledger-test") != Status::OK) return false;
  }
  object.total_size = 100;
  if (registry.declare_token("ledger-over-c", 1) != Status::OK ||
      declare("ledger-over-c", 1, "small") != Status::UNSUPPORTED ||
      registry.retry_cleanup_debt_once(2000000) != 1 ||
      declare("ledger-over-c", 1, "small") != Status::OK ||
      registry.retry_cleanup_debt_once(3000000) != 1) return false;
  DBUG_PRINT("preserve_temp_import",
             ("receiver reservation ledger verified epoch_live=1 global_debt=1 duplicate=1 retry=1 overflow_recovery=1"));
  return true;
}

Preserve_trx_transfer_status preserve_trx_transfer_probe_temp_file(
    const std::string &root, const std::string &token,
    const std::string &source_path, uint64_t size,
    const std::array<unsigned char, 32> &digest,
    std::shared_ptr<const Preserve_trx_sealed_file> *output) {
  using Status = Preserve_trx_transfer_status;
  DBUG_EXECUTE_IF("preserve_transfer_reservation_ledger_probe", {
    if (!probe_receiver_reservation_ledger(root)) return Status::CORRUPT;
  });
  std::shared_ptr<const Preserve_trx_sealed_file> source;
  if (size == 0 || output == nullptr ||
      Preserve_trx_sealed_file::open_verified(source_path, size, digest,
                                             &source) !=
          Preserve_trx_file_status::OK) return Status::CORRUPT;
  Preserve_trx_transfer_manifest manifest;
  manifest.epoch_id = "temp-file-probe-" + token;
  manifest.token = 1;
  Preserve_trx_transfer_object_descriptor object;
  object.object_id = "temp-image";
  object.kind = Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR;
  object.total_size = size;
  object.digest = digest;
  manifest.objects.push_back(object);
  auto cleanup = create_scope_guard([&] {
    cleanup_transfer_token_staging(root, manifest.epoch_id, manifest.token);
  });
  Preserve_trx_transfer_receiver_registry registry;
  auto status = registry.begin_receive(manifest);
  if (status != Status::OK) return status;
  const auto stage_all = [&]() {
    std::string bytes;
    for (uint64_t offset = 0; offset < size;) {
      bytes.resize(static_cast<size_t>(std::min<uint64_t>(65536, size-offset)));
      if (!source->read_at(offset, reinterpret_cast<unsigned char *>(&bytes[0]),
                          bytes.size())) return Status::IO_ERROR;
      const auto result = preserve_trx_transfer_stage_object_chunk(
          root, manifest, object.object_id, offset, bytes);
      if (result != Status::OK) return result;
      offset += bytes.size();
    }
    return Status::OK;
  };
  // Positive control: a real first write must hit the same fault that a
  // complete retransmission below must avoid.
  DBUG_PUSH("+d,preserve_transfer_fail_file_tail_write");
  {
    auto restore_debug = create_scope_guard([] { DBUG_POP(); });
    status = stage_all();
  }
  if (status != Status::IO_ERROR) return Status::CORRUPT;
  status = stage_all();
  if (status != Status::OK) return status;
  std::shared_ptr<const Preserve_trx_sealed_file> file;
  status = preserve_trx_transfer_seal_staged_object(root, manifest,
                                                    object.object_id, &file);
  if (status != Status::OK) return status;
  status = registry.mark_object_sealed(manifest.epoch_id, manifest.token,
                                       object.object_id, file);
  if (status != Status::OK) return status;
  DBUG_EXECUTE_IF("preserve_transfer_reservation_ledger_probe", {
    Preserve_trx_transfer_receiver_registry peak;
    const auto limit = preserve_trx_transfer_max_inflight_bytes;
    auto restore = create_scope_guard([&] { preserve_trx_transfer_max_inflight_bytes = limit; });
    uint64_t old_bytes = 0;
    if (receiver_object_reserved_bytes(object, &old_bytes) != Status::OK)
      return Status::CORRUPT;
    preserve_trx_transfer_max_inflight_bytes = old_bytes * 2;
    auto next = object;
    ++next.total_size;
    if (peak.declare_token("temp-retired-peak", 1) != Status::OK ||
        peak.declare_object("temp-retired-peak", 1, object) != Status::OK ||
        peak.mark_object_sealed("temp-retired-peak", 1, object.object_id, file) != Status::OK ||
        peak.declare_object("temp-retired-peak", 1, next) != Status::RESOURCE_EXHAUSTED ||
        peak.status_counts().inflight_bytes != old_bytes) {
      DBUG_PRINT("preserve_temp_import", ("retired staging peak lost old generation charge"));
      return Status::CORRUPT;
    }
    // Admission reserves cancellation metadata. A later budget reduction must
    // reject another object without preventing the admitted owner from exiting.
    const auto memory_limit = preserve_trx_memory_budget_bytes;
    auto restore_memory = create_scope_guard([&] { preserve_trx_memory_budget_bytes = memory_limit; });
    preserve_trx_memory_budget_bytes = 1;
    auto another = object;
    another.object_id = "temp-second";
    if (peak.declare_object("temp-retired-peak", 1, another) != Status::RESOURCE_EXHAUSTED ||
        peak.mark_aborted("temp-retired-peak", 1, "budget_exit") != Status::OK ||
        peak.complete_staging_cleanup("temp-retired-peak", 1) != Status::OK ||
        peak.reap_retired_staging_once() != 0 ||
        peak.status_counts().inflight_bytes != old_bytes) return Status::CORRUPT;
    preserve_trx_memory_budget_bytes = memory_limit;
    // Canonical SEAL, BEGIN reordering and omitted-object retirement share the
    // exact owner. Dropping the last reader is required before quota is freed.
    preserve_trx_transfer_max_inflight_bytes = limit;
    Preserve_trx_transfer_receiver_registry lifetime;
    auto selected = manifest;
    selected.epoch_id = "temp-retired-lifetime";
    selected.objects.push_back(another);
    std::shared_ptr<const Preserve_trx_sealed_file> pin;
    std::shared_ptr<const Preserve_trx_sealed_file> duplicate;
    const auto path = transfer_object_path(root, manifest, object);
    if (Preserve_trx_sealed_file::open_verified(path, size, digest, &pin) != Preserve_trx_file_status::OK ||
        Preserve_trx_sealed_file::open_verified(path, size, digest, &duplicate) != Preserve_trx_file_status::OK ||
        lifetime.begin_receive(selected) != Status::OK ||
        lifetime.mark_object_sealed(selected.epoch_id, 1, object.object_id, pin) != Status::OK ||
        lifetime.mark_object_sealed(selected.epoch_id, 1, object.object_id, duplicate) != Status::OK)
      return Status::CORRUPT;
    duplicate.reset();
    Preserve_trx_transfer_receiver_record canonical;
    if (!lifetime.lookup(selected.epoch_id, 1, &canonical) ||
        canonical.sealed_files.at(object.object_id) != pin) return Status::CORRUPT;
    canonical.sealed_files.clear();
    std::reverse(selected.objects.begin(), selected.objects.end());
    if (lifetime.begin_receive(selected) != Status::OK) return Status::CORRUPT;
    selected.objects.clear();
    auto only = object;
    only.object_id = "temp-selected";
    selected.objects.push_back(only);
    std::vector<Preserve_trx_transfer_receiver_registry::Staging_retirement> retired;
    if (lifetime.begin_receive(selected, 0, &retired) != Status::OK ||
        retired.size() != 2 || retired[0].object_id != another.object_id ||
        retired[1].object_id != object.object_id) return Status::CORRUPT;
    for (const auto &ticket : retired) lifetime.mark_staging_object_deleted(ticket.id);
    if (lifetime.reap_retired_staging_once() != 1 ||
        lifetime.mark_aborted(selected.epoch_id, 1, "done") != Status::OK ||
        lifetime.complete_staging_cleanup(selected.epoch_id, 1) != Status::OK ||
        lifetime.reap_retired_staging_once() != 1 ||
        lifetime.status_counts().inflight_bytes != old_bytes) return Status::CORRUPT;
    pin.reset();
    if (lifetime.reap_retired_staging_once() != 1 ||
        lifetime.status_counts().inflight_bytes != 0) return Status::CORRUPT;
    DBUG_PRINT("preserve_temp_import", ("retired staging verified peak=1 canonical=1 reorder=1 budget_cancel=1 last_reader=1"));
  });
  // Replaying data after a lost range file must rebuild coverage without
  // writing the verified inode again. This bypasses wire sequence dedup.
  if (my_delete(transfer_object_range_path(root, manifest, object).c_str(),
                MYF(0)) != 0) return Status::IO_ERROR;
  DBUG_PUSH("+d,preserve_transfer_fail_file_tail_write");
  {
    auto restore_debug = create_scope_guard([] { DBUG_POP(); });
    status = stage_all();
  }
  if (status != Status::OK) return status;
  status = preserve_trx_transfer_seal_staged_object(root, manifest,
                                                    object.object_id);
  if (status != Status::OK) return status;
  std::string first(static_cast<size_t>(std::min<uint64_t>(size, 32)), '\0');
  if (!file->read_at(0, reinterpret_cast<unsigned char *>(&first[0]),
                     first.size())) return Status::IO_ERROR;
  auto conflict = first;
  conflict[0] ^= 1;
  if (preserve_trx_transfer_stage_object_chunk(root, manifest, object.object_id,
                                              0, conflict) != Status::CORRUPT) {
    return Status::CORRUPT;
  }
  // A repeated final manifest retains the same verified descriptor.
  status = registry.begin_receive(manifest);
  if (status != Status::OK) return status;
  Preserve_trx_transfer_receiver_record record;
  if (!registry.lookup(manifest.epoch_id, manifest.token, &record)) {
    return Status::CORRUPT;
  }
  const auto retained_file = record.sealed_files.find(object.object_id);
  if (retained_file == record.sealed_files.end() ||
      retained_file->second != file) return Status::CORRUPT;
  file.reset();
  file = std::move(retained_file->second);
  record.sealed_files.clear();
  status = cleanup_transfer_token_staging(root, manifest.epoch_id, manifest.token);
  if (status != Status::OK) return status;
  status = preserve_trx_transfer_stage_object_chunk(root, manifest,
                                                    object.object_id, 0, conflict);
  if (status != Status::OK) return status;
  status = cleanup_transfer_token_staging(root, manifest.epoch_id, manifest.token);
  if (status != Status::OK) return status;
  status = registry.mark_aborted(manifest.epoch_id, manifest.token, "probe_done");
  if (status != Status::OK) return status;
  status = registry.complete_staging_cleanup(manifest.epoch_id, manifest.token);
  if (status != Status::OK) return status;
  DBUG_EXECUTE_IF("preserve_transfer_reservation_ledger_probe", {
    uint64_t pinned_bytes = 0;
    if (receiver_object_reserved_bytes(object, &pinned_bytes) != Status::OK ||
        registry.status_counts().inflight_bytes != pinned_bytes) {
      DBUG_PRINT("preserve_temp_import", ("retired staging quota released before last reader"));
      return Status::CORRUPT;
    }
  });
  if (!registry.lookup(manifest.epoch_id, manifest.token, &record) ||
      !record.sealed_files.empty()) return Status::CORRUPT;
  std::string retained(first.size(), '\0');
  unsigned char byte = 0;
  if (!file->read_at(0, reinterpret_cast<unsigned char *>(&retained[0]),
                     retained.size()) || retained != first ||
      file->read_at(size, &byte, 1) || !file->read_at(size, nullptr, 0)) {
    return Status::CORRUPT;
  }
  // Exercise the production CHUNK dispatch and token-wide failure cleanup,
  // retaining the separately sealed inode across that cleanup.
  auto chunk_manifest = manifest;
  chunk_manifest.epoch_id += "-dispatch";
  chunk_manifest.token = 2;
  chunk_manifest.source_freeze_lsn = 123;
  chunk_manifest.source_epoch_commit_lsn = 456;
  chunk_manifest.objects.push_back(object);
  chunk_manifest.objects.back().object_id = "temp-image-second";
  auto chunk_cleanup = create_scope_guard([&] {
    cleanup_transfer_token_staging(root, chunk_manifest.epoch_id, 2);
  });
  Preserve_trx_transfer_receiver_registry chunk_registry;
  auto store = create_preserved_trx_process_local_store(root);
  status = chunk_registry.begin_receive(chunk_manifest);
  if (status != Status::OK) return status;
  Preserve_trx_transfer_frame frame;
  frame.type = Preserve_trx_transfer_frame_type::OBJECT_CHUNK;
  frame.epoch_id = chunk_manifest.epoch_id;
  frame.token = 2;
  frame.chunk_payload = first;
  for (const auto &entry : chunk_manifest.objects) {
    frame.object_id = entry.object_id;
    ++frame.sequence;
    status = preserve_trx_transfer_apply_receiver_frame(
        root, frame, &store.store(), &chunk_registry, 30);
    if (status != Status::OK ||
        my_access(transfer_object_path(root, chunk_manifest, entry).c_str(),
                  F_OK) != 0)
      return Status::CORRUPT;
    Preserve_trx_transfer_receiver_record chunk_record;
    size_t count = 0;
    if (!chunk_registry.lookup_object(frame.epoch_id, 2, entry.object_id,
                                             &chunk_record, &count) ||
        count != 2 || chunk_record.objects.size() != 1 ||
        !transfer_object_descriptor_equal(chunk_record.objects.front(), entry) ||
        !chunk_record.sealed_files.empty() ||
        chunk_record.source_freeze_lsn != 123 ||
        chunk_record.source_epoch_commit_lsn != 456)
      return Status::CORRUPT;
  }
  ++frame.sequence;
  frame.object_id = "unknown-object";
  status = preserve_trx_transfer_apply_receiver_frame(
      root, frame, &store.store(), &chunk_registry, 30);
  if (status != Status::INVALID_ARGUMENT ||
      !chunk_registry.lookup(frame.epoch_id, 2, &record) ||
      record.state != Preserve_trx_transfer_receiver_state::CORRUPT)
    return Status::CORRUPT;
  for (const auto &entry : chunk_manifest.objects) {
    if (my_access(transfer_object_path(root, chunk_manifest, entry).c_str(),
                  F_OK) == 0 ||
        my_access(transfer_object_range_path(root, chunk_manifest, entry).c_str(),
                  F_OK) == 0)
      return Status::CORRUPT;
  }
  if (!file->read_at(0, reinterpret_cast<unsigned char *>(&retained[0]),
                     retained.size()) || retained != first)
    return Status::CORRUPT;
  {
    // Index identity follows BEGIN ordering and atomic DECLARE publication.
    // A retained record snapshot must keep its original lookup after either.
    auto indexed = manifest;
    indexed.epoch_id += "-index";
    indexed.token = 3;
    indexed.objects.clear();
    for (const char *name : {"z-image", "a-image", "m-image"}) {
      auto item = object;
      item.object_id = name;
      indexed.objects.push_back(std::move(item));
    }
    Preserve_trx_transfer_receiver_registry indexes;
    if (indexes.begin_receive(indexed) != Status::OK) return Status::CORRUPT;
    Preserve_trx_transfer_receiver_record old;
    if (!indexes.lookup(indexed.epoch_id, 3, &old) || !old.object_index)
      return Status::CORRUPT;
    const auto matches = [&](const Preserve_trx_transfer_manifest &expected) {
      for (const auto &item : expected.objects) {
        Preserve_trx_transfer_receiver_record one;
        size_t count = 0;
        if (!indexes.lookup_object(expected.epoch_id, 3, item.object_id,
                                           &one, &count) ||
            count != expected.objects.size() || one.objects.size() != 1 ||
            one.object_index ||
            !transfer_object_descriptor_equal(one.objects.front(), item))
          return false;
      }
      return true;
    };
    if (!matches(indexed)) return Status::CORRUPT;
    auto reordered = indexed;
    std::reverse(reordered.objects.begin(), reordered.objects.end());
    reordered.objects.pop_back();
    DBUG_PUSH("+d,preserve_transfer_object_index_oom");
    {
      auto restore_debug = create_scope_guard([] { DBUG_POP(); });
      if (indexes.begin_receive(reordered) != Status::RESOURCE_EXHAUSTED)
        return Status::CORRUPT;
    }
    if (!matches(indexed) || indexes.begin_receive(reordered) != Status::OK ||
        !matches(reordered)) return Status::CORRUPT;
    auto appended = object;
    appended.object_id = "b-image";
    DBUG_PUSH("+d,preserve_transfer_object_index_oom");
    {
      auto restore_debug = create_scope_guard([] { DBUG_POP(); });
      if (indexes.declare_object(indexed.epoch_id, 3, appended) !=
          Status::RESOURCE_EXHAUSTED) return Status::CORRUPT;
    }
    if (!matches(reordered) ||
        indexes.declare_object(indexed.epoch_id, 3, appended) != Status::OK ||
        indexes.declare_object(indexed.epoch_id, 3, appended) != Status::OK)
      return Status::CORRUPT;
    reordered.objects.push_back(appended);
    if (!matches(reordered)) return Status::CORRUPT;
    for (const auto &item : old.objects) {
      const auto *lookup = old.object_index->find(old.objects, item.object_id);
      if (!lookup || !transfer_object_descriptor_equal(*lookup, item))
        return Status::CORRUPT;
    }
    auto snapshot = object;
    snapshot.kind = Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE;
    snapshot.object_id = "snapshot";
    if (indexes.declare_object(indexed.epoch_id, 3, snapshot) != Status::OK)
      return Status::CORRUPT;
    reordered.objects.push_back(snapshot);
    for (const auto &item : reordered.objects) {
      if (indexes.mark_object_sealed(indexed.epoch_id, 3, item.object_id, source) != Status::OK)
        return Status::CORRUPT;
    }
    if (indexes.freeze_staged_manifest(reordered) != Status::OK)
      return Status::CORRUPT;
    DBUG_PUSH("+d,preserve_transfer_object_index_oom");
    {
      auto restore_debug = create_scope_guard([] { DBUG_POP(); });
      if (indexes.begin_receive(reordered) != Status::OK ||
          indexes.begin_receive(indexed) != Status::UNSUPPORTED ||
          indexes.declare_object(indexed.epoch_id, 3, snapshot) != Status::OK)
        return Status::CORRUPT;
    }
    if (indexes.mark_aborted(indexed.epoch_id, 3, "index_probe_done") != Status::OK ||
        indexes.complete_staging_cleanup(indexed.epoch_id, 3) != Status::OK ||
        !indexes.lookup(indexed.epoch_id, 3, &record) || record.object_index ||
        !old.object_index->find(old.objects, "z-image")) return Status::CORRUPT;
    DBUG_PRINT("preserve_temp_import",
        ("temporary receiver object index checked reorder=1 append=1 oom_retry=1 snapshots=1 terminal=1"));
    DBUG_EXECUTE_IF("preserve_transfer_object_index_history_probe", {
      // Retained receiver snapshots must share unchanged index storage. Hold
      // every prefix, then check its original lookup after later declarations.
      std::vector<Preserve_trx_transfer_object_descriptor> objects;
      std::vector<std::shared_ptr<const Preserve_trx_transfer_object_index>> history;
      constexpr size_t count = 8192;
      objects.reserve(count);
      history.reserve(count);
      std::shared_ptr<const Preserve_trx_transfer_object_index> current;
      for (size_t i = 0; i < count; ++i) {
        auto item = object;
        item.object_id = "history-" + std::to_string((i * 4093) % count);
        std::shared_ptr<const Preserve_trx_transfer_object_index> next;
        const auto built = Preserve_trx_transfer_object_index::build(
            919191, objects, count, &next, &item, current.get());
        if (built != Status::OK) {
          DBUG_PRINT("preserve_temp_import",
              ("temporary object index history failed at=%zu status=%u", i,
               static_cast<unsigned>(built)));
          return built;
        }
        objects.push_back(std::move(item));
        current = std::move(next);
        history.push_back(current);
      }
      for (size_t i = count; i != 0; --i) {
        if (!history[i - 1]->find(objects, objects.front().object_id) ||
            !history[i - 1]->find(objects, objects.back().object_id))
          return Status::CORRUPT;
        objects.pop_back();
      }
      DBUG_PRINT("preserve_temp_import",
          ("temporary object index history verified snapshots=8192"));
    });
  }
  DBUG_PRINT("preserve_temp_import",
             ("temporary receiver chunk dispatch checked objects=2 copied=1 unknown_cleanup=1 retained=1"));
  *output = std::move(file);
  DBUG_PRINT("preserve_temp_import",
             ("temporary receiver file retained after retries replacement and cleanup"));
  return Status::OK;
}

Preserve_trx_transfer_status preserve_trx_temp_transfer_probe(
    const std::string &root, const std::string &source_token,
    const std::string &payload,
    std::unique_ptr<Preserve_trx_temp_transfer_input> *output) {
  using Status = Preserve_trx_transfer_status;
  Preserved_temp_table_manifest decoded;
  if (!preserve_trx_decode_temp_table_manifest(payload, &decoded)) return Status::CORRUPT;
  std::map<std::string, std::string> source_names;
  const auto retoken = [&](auto *file, const char *suffix) {
    const auto name = "1.tempts." + std::to_string(file->source_space_id) + suffix;
    source_names.emplace(name, file->blob_name);
    file->blob_name = name;
  };
  for (auto &table : decoded.tables) retoken(&table.image, ".image");
  for (auto &undo : decoded.undo_images) retoken(&undo, ".undo");
  for (auto &claim : decoded.ownership_claims) claim.token = "1";
  std::string manifest;
  if (!preserve_trx_encode_temp_table_manifest(decoded, &manifest)) return Status::CORRUPT;
  if (!decoded.ownership_claims.empty()) {
    // Retokened claims contain exactly 61 fixed bytes plus the one-byte token.
    auto truncated = manifest.substr(0, manifest.size() - decoded.ownership_claims.size() * 62 - 4);
    truncated.append("\0\0\1\0", 4);  // 65536 claims, no claim body.
    std::vector<Preserve_trx_transfer_object_descriptor> rejected;
    if (preserve_trx_temp_transfer_descriptors("1", truncated, &rejected) != Status::CORRUPT ||
        !rejected.empty()) return Status::CORRUPT;
  }
  Preserve_trx_transfer_manifest transfer;
  transfer.epoch_id = "temp-input-probe-" + source_token;
  transfer.token = 1;
  auto status = preserve_trx_temp_transfer_descriptors("1", manifest, &transfer.objects);
  if (status != Status::OK || transfer.objects.size() != source_names.size()) return Status::CORRUPT;
  auto cleanup = create_scope_guard([&] {
    cleanup_transfer_token_staging(root, transfer.epoch_id, transfer.token);
  });
  Preserve_trx_transfer_receiver_registry registry;
  status = registry.begin_receive(transfer);
  if (status != Status::OK) return status;
  for (const auto &object : transfer.objects) {
    std::shared_ptr<const Preserve_trx_sealed_file> source, received;
    if (Preserve_trx_sealed_file::open_verified(
            root + "/" + source_names.at(object.object_id), object.total_size,
            object.digest, &source) != Preserve_trx_file_status::OK) return Status::IO_ERROR;
    std::string chunk;
    for (uint64_t offset = 0; offset < object.total_size; offset += chunk.size()) {
      chunk.resize(static_cast<size_t>(std::min<uint64_t>(65536, object.total_size - offset)));
      if (!source->read_at(offset, reinterpret_cast<unsigned char *>(&chunk[0]), chunk.size()))
        return Status::IO_ERROR;
      status = preserve_trx_transfer_stage_object_chunk(root, transfer, object.object_id, offset, chunk);
      if (status != Status::OK) return status;
    }
    status = preserve_trx_transfer_seal_staged_object(root, transfer, object.object_id, &received);
    if (status != Status::OK) return status;
    status = registry.mark_object_sealed(transfer.epoch_id, 1, object.object_id, received);
    if (status != Status::OK) return status;
  }
  Preserve_trx_transfer_receiver_record record;
  if (!registry.lookup(transfer.epoch_id, 1, &record) ||
      !preserve_trx_temp_id_local_contract(&record.temp_id_contract)) return Status::CORRUPT;
  std::unique_ptr<Preserve_trx_temp_transfer_input> input;
  if (Preserve_trx_temp_transfer_input::load("2", manifest, record, &input) != Status::CORRUPT || input)
    return Status::CORRUPT;
  for (int fault = 0; fault < 14; ++fault) {
    auto broken = record;
    auto &object = broken.objects.front();
    const auto id = object.object_id;
    switch (fault) {
      case 0: broken.objects.erase(broken.objects.begin()); break;
      case 1: broken.objects.push_back(object); break;
      case 2: object.flags = 1; break;
      case 3: object.lock_plan.source_live_generation = 1; break;
      case 4: object.kind = Preserve_trx_transfer_object_kind::EXTERNAL_BLOB; break;
      case 5: object.digest[0] ^= 1; break;
      case 6: ++object.total_size; break;
      case 7: broken.sealed_objects.erase(id); break;
      case 8: broken.sealed_files.erase(id); break;
      case 9: broken.sealed_files[id].reset(); break;
      case 10: object.object_id += "-extra"; break;
      case 11: broken.sealed_files[id + "-extra"] = broken.sealed_files[id]; break;
      case 12: broken.sealed_objects.insert(id + "-extra"); break;
      case 13: broken.state = Preserve_trx_transfer_receiver_state::ABORTED; break;
    }
    if (Preserve_trx_temp_transfer_input::load("1", manifest, broken, &input) != Status::CORRUPT || input)
      return Status::CORRUPT;
  }
  const auto memory_before = preserve_trx_memory_current_bytes_status();
  DBUG_PUSH("+d,preserve_temp_transfer_input_allocation_failure");
  {
    const auto restore = create_scope_guard([]() { DBUG_POP(); });
    if (Preserve_trx_temp_transfer_input::load("1", manifest, record, &input) != Status::RESOURCE_EXHAUSTED ||
        input || preserve_trx_memory_current_bytes_status() != memory_before) return Status::CORRUPT;
  }
  status = Preserve_trx_temp_transfer_input::load("1", manifest, record, &input);
  if (status != Status::OK || !input->matches(manifest, record)) return Status::CORRUPT;
  auto changed = record;
  changed.objects.front().digest[0] ^= 1;
  if (input->matches(manifest, changed) || input->matches(manifest + "x", record))
    return Status::CORRUPT;
  changed = record;
  ++changed.token;
  if (input->matches(manifest, changed)) return Status::CORRUPT;
  changed = record;
  changed.epoch_id += "-other";
  if (input->matches(manifest, changed)) return Status::CORRUPT;
  changed = record;
  changed.temp_id_contract = {};
  if (input->matches(manifest, changed)) return Status::CORRUPT;
  auto *owner = input.get();
  if (Preserve_trx_temp_transfer_input::load("1", manifest, record, &input) != Status::INVALID_ARGUMENT ||
      input.get() != owner) return Status::CORRUPT;
  // Exercise replacement through the registry, retaining the old input pins.
  auto replacement = transfer;
  auto &replacement_object = replacement.objects.front();
  replacement_object.total_size = 3;
  replacement_object.digest = sha256_digest("bad");
  status = registry.declare_object(transfer.epoch_id, 1, replacement_object);
  if (status != Status::OK) return status;
  status = cleanup_transfer_token_staging(root, transfer.epoch_id, 1);
  if (status != Status::OK) return status;
  status = preserve_trx_transfer_stage_object_chunk(
      root, replacement, replacement_object.object_id, 0, "bad");
  if (status != Status::OK) return status;
  std::shared_ptr<const Preserve_trx_sealed_file> replacement_file;
  status = preserve_trx_transfer_seal_staged_object(
      root, replacement, replacement_object.object_id, &replacement_file);
  if (status != Status::OK) return status;
  status = registry.mark_object_sealed(
      transfer.epoch_id, 1, replacement_object.object_id, replacement_file);
  if (status != Status::OK) return status;
  if (!registry.lookup(transfer.epoch_id, 1, &changed)) return Status::CORRUPT;
  changed.temp_id_contract = record.temp_id_contract;
  if (input->matches(manifest, changed)) return Status::CORRUPT;

  // Keep the correct descriptors but substitute a different non-null pin.
  changed = record;
  changed.sealed_files[replacement_object.object_id] = replacement_file;
  std::unique_ptr<Preserve_trx_temp_transfer_input> rejected_input;
  if (input->matches(manifest, changed) ||
      Preserve_trx_temp_transfer_input::load("1", manifest, changed,
                                            &rejected_input) != Status::CORRUPT ||
      rejected_input) return Status::CORRUPT;
  status = registry.mark_aborted(transfer.epoch_id, 1, "probe_complete");
  if (status != Status::OK) return status;
  if (!registry.lookup(transfer.epoch_id, 1, &changed) || input->matches(manifest, changed))
    return Status::CORRUPT;
  status = cleanup_transfer_token_staging(root, transfer.epoch_id, 1);
  if (status != Status::OK) return status;
  // Recreate the same staging names with different bytes. The input must keep
  // the exact old inodes, not discover these replacements by pathname.
  status = registry.complete_staging_cleanup(transfer.epoch_id, 1);
  if (status != Status::OK) return status;
  for (const auto &object : transfer.objects) {
    status = preserve_trx_transfer_stage_object_chunk(root, transfer, object.object_id, 0, "bad");
    if (status != Status::OK) return status;
  }
  status = cleanup_transfer_token_staging(root, transfer.epoch_id, 1);
  if (status != Status::OK) return status;
  record.sealed_files.clear();
  changed.sealed_files.clear();
  for (const auto &file : input->files()) {
    unsigned char first = 0;
    if (!file.second->read_at(0, &first, 1)) return Status::IO_ERROR;
  }
  DBUG_PRINT("preserve_temp_import",
             ("temporary transfer input checked objects=%zu rejected=16 "
              "pinned_after_cleanup=1 replacement_checked=1 wrong_pin_checked=1",
              input->files().size()));
  *output = std::move(input);
  return Status::OK;
}


#endif

Preserve_trx_transfer_status preserve_trx_transfer_read_sealed_object_payload(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id, std::string *payload,
    bool objects_already_sealed) {
  if (payload == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  if (!objects_already_sealed) {
    const Preserve_trx_transfer_status seal_status =
        preserve_trx_transfer_seal_staged_object(root_dir, manifest, object_id);
    if (seal_status != Preserve_trx_transfer_status::OK) return seal_status;
  }

  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, object_id);
  if (object == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  std::string staged_payload;
  const Preserve_trx_transfer_status read_status =
      read_whole_file(transfer_object_path(root_dir, manifest, *object),
                      &staged_payload);
  if (read_status != Preserve_trx_transfer_status::OK) return read_status;
  if (staged_payload.length() != object->total_size) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  *payload = std::move(staged_payload);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_read_snapshot_bundle_payload(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, std::string *payload,
    bool objects_already_sealed) {
  if (payload == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  const Preserve_trx_transfer_object_descriptor *snapshot = nullptr;
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (object.kind != Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE) {
      continue;
    }
    if (snapshot != nullptr) return Preserve_trx_transfer_status::CORRUPT;
    snapshot = &object;
  }
  if (snapshot == nullptr) return Preserve_trx_transfer_status::CORRUPT;

  return preserve_trx_transfer_read_sealed_object_payload(
      root_dir, manifest, snapshot->object_id, payload, objects_already_sealed);
}

Preserve_trx_transfer_status preserve_trx_transfer_seal_manifest_objects(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest) {
  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  size_t snapshot_bundle_count = 0;
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (object.kind == Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE) {
      ++snapshot_bundle_count;
    }
    const Preserve_trx_transfer_status seal_status =
        preserve_trx_transfer_seal_staged_object(root_dir, manifest,
                                                object.object_id);
    if (seal_status != Preserve_trx_transfer_status::OK) return seal_status;
  }

  return snapshot_bundle_count == 1 ? Preserve_trx_transfer_status::OK
                                    : Preserve_trx_transfer_status::CORRUPT;
}

Preserve_trx_transfer_status hydrate_external_blobs_from_staging(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    Preserved_trx_bundle *bundle, bool objects_already_sealed = false) {
  if (bundle == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  bundle->external_blobs.clear();

  std::set<std::string> matched_external_objects;
  for (const Preserved_trx_external_blob_descriptor &descriptor :
       bundle->blob_descriptors) {
    const Preserve_trx_transfer_object_descriptor *object =
        find_object(manifest, descriptor.name);
    if (object == nullptr ||
        object->kind != Preserve_trx_transfer_object_kind::EXTERNAL_BLOB ||
        object->total_size != descriptor.size ||
        object->digest != descriptor.digest) {
      return Preserve_trx_transfer_status::CORRUPT;
    }

    std::string payload;
    const Preserve_trx_transfer_status read_status =
        preserve_trx_transfer_read_sealed_object_payload(
            root_dir, manifest, object->object_id, &payload,
            objects_already_sealed);
    if (read_status != Preserve_trx_transfer_status::OK) return read_status;

    Preserved_trx_external_blob blob;
    blob.name = descriptor.name;
    blob.payload = std::move(payload);
    blob.descriptor = descriptor;
    bundle->external_blobs.push_back(std::move(blob));
    matched_external_objects.insert(object->object_id);
  }

  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (object.kind == Preserve_trx_transfer_object_kind::EXTERNAL_BLOB &&
        matched_external_objects.count(object.object_id) == 0) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }

  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status validate_external_blob_descriptors(
    const Preserve_trx_transfer_manifest &manifest,
    const Preserved_trx_bundle &bundle) {
  std::set<std::string> matched_external_objects;
  for (const Preserved_trx_external_blob_descriptor &descriptor :
       bundle.blob_descriptors) {
    const Preserve_trx_transfer_object_descriptor *object =
        find_object(manifest, descriptor.name);
    if (object == nullptr ||
        object->kind != Preserve_trx_transfer_object_kind::EXTERNAL_BLOB ||
        object->total_size != descriptor.size ||
        object->digest != descriptor.digest) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    matched_external_objects.insert(object->object_id);
  }
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (object.kind == Preserve_trx_transfer_object_kind::EXTERNAL_BLOB &&
        matched_external_objects.count(object.object_id) == 0) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_load_standby_bundle_from_staging(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    Preserved_trx_bundle *bundle, bool objects_already_sealed = false,
    Preserve_trx_transfer_receiver_registry *registry = nullptr) {
  if (bundle == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  const Preserve_trx_transfer_object_descriptor *snapshot = nullptr;
  for (const Preserve_trx_transfer_object_descriptor &object : manifest.objects) {
    if (object.kind != Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE) {
      continue;
    }
    if (snapshot != nullptr) return Preserve_trx_transfer_status::CORRUPT;
    snapshot = &object;
  }
  if (snapshot == nullptr) return Preserve_trx_transfer_status::CORRUPT;

  std::shared_ptr<const std::string> online_snapshot;
  std::string file_snapshot;
  const std::string *portable_snapshot = nullptr;
  Preserve_trx_transfer_status status = Preserve_trx_transfer_status::OK;
  if (registry != nullptr &&
      transfer_object_uses_strict_v1_memory_staging(manifest, *snapshot)) {
    status = read_receiver_sealed_object_payload(
        root_dir, manifest, snapshot->object_id, registry, &online_snapshot);
    if (status == Preserve_trx_transfer_status::OK && online_snapshot != nullptr) {
      portable_snapshot = online_snapshot.get();
    }
  } else {
    status = preserve_trx_transfer_read_snapshot_bundle_payload(
        root_dir, manifest, &file_snapshot, objects_already_sealed);
    portable_snapshot = &file_snapshot;
  }
  if (status != Preserve_trx_transfer_status::OK) return status;
  if (portable_snapshot == nullptr) return Preserve_trx_transfer_status::CORRUPT;

  status = preserve_trx_transfer_decode_portable_bundle(*portable_snapshot,
                                                        bundle);
  if (status != Preserve_trx_transfer_status::OK) return status;
  status = preserve_trx_result_transfer_validate(std::to_string(manifest.token), manifest.objects,
                                             bundle->metadata.cursor_manifest_payload);
  if (status != Preserve_trx_transfer_status::OK) return status;
  if (registry) {
    Preserve_trx_transfer_receiver_record record;
    if (!registry->lookup(manifest.epoch_id, manifest.token, &record) ||
        !receiver_record_matches_manifest(record, manifest))
      return Preserve_trx_transfer_status::CORRUPT;
    status = preserve_trx_result_transfer_validate_files(
        record, bundle->metadata.cursor_manifest_payload);
    if (status != Preserve_trx_transfer_status::OK) return status;
  } else {
    // Cumulative generations require the online receiver's verified FD owner.
    Preserve_trx_result_manifest_view selected;
    if (!bundle->metadata.cursor_manifest_payload.empty() &&
        selected.read(bundle->metadata.cursor_manifest_payload))
      return Preserve_trx_transfer_status::CORRUPT;
    const auto count = std::count_if(manifest.objects.begin(), manifest.objects.end(),
        [](const auto &object) {
          return object.kind == Preserve_trx_transfer_object_kind::CURSOR_RESULT;
        });
    if (static_cast<size_t>(count) != selected.size())
      return Preserve_trx_transfer_status::CORRUPT;
  }
  status = preserve_trx_temp_transfer_validate(std::to_string(manifest.token),
      bundle->metadata.temp_table_manifest_payload, manifest.objects);
  if (status != Preserve_trx_transfer_status::OK) return status;

  if (registry != nullptr &&
      transfer_manifest_uses_strict_metadata_only_prewarm(manifest)) {
    bundle->external_blobs.clear();
    return validate_external_blob_descriptors(manifest, *bundle);
  }

  return hydrate_external_blobs_from_staging(root_dir, manifest, bundle,
                                            objects_already_sealed);
}

Preserve_trx_transfer_status preserve_trx_transfer_publish_standby_bundle(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_bundle bundle,
    Preserved_trx_store *store, uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata,
    Preserved_trx_store_write_stats *write_stats,
    bool objects_already_sealed) {
  const std::string token_component = transfer_token_component(manifest.token);
  if (store == nullptr || bundle.metadata.token != token_component) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  if (!objects_already_sealed) {
    const Preserve_trx_transfer_status seal_status =
        preserve_trx_transfer_seal_manifest_objects(root_dir, manifest);
    if (seal_status != Preserve_trx_transfer_status::OK) return seal_status;
  }

  Preserve_trx_standby_pending_artifact_sink sink(store);
  return map_snapshot_status_to_transfer(sink.publish_bundle(
      std::move(bundle), timeout_seconds, written_metadata, nullptr, nullptr,
      write_stats));
}

Preserve_trx_transfer_status
preserve_trx_transfer_publish_standby_bundle_from_staging(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_store *store,
    uint64_t timeout_seconds, Preserve_snapshot_metadata *written_metadata,
    Preserved_trx_bundle *loaded_bundle_for_ready_cache) {
  Preserved_trx_bundle bundle;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_load_standby_bundle_from_staging(root_dir, manifest,
                                                             &bundle);
  if (status != Preserve_trx_transfer_status::OK) return status;

  if (loaded_bundle_for_ready_cache != nullptr) {
    *loaded_bundle_for_ready_cache = bundle;
  }
  return preserve_trx_transfer_publish_standby_bundle(
      root_dir, manifest, std::move(bundle), store, timeout_seconds,
      written_metadata);
}

Preserve_trx_transfer_status
preserve_trx_transfer_publish_standby_bundle_from_staging(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_store *store,
    Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds, Preserve_snapshot_metadata *written_metadata,
    Preserved_trx_bundle *loaded_bundle_for_ready_cache) {
  if (store == nullptr || registry == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  Preserve_trx_transfer_receiver_record record;
  if (!registry->lookup(manifest.epoch_id, manifest.token, &record)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (record.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  const Preserve_trx_transfer_status status =
      preserve_trx_transfer_publish_standby_bundle_from_staging(
          root_dir, manifest, store, timeout_seconds, written_metadata,
          loaded_bundle_for_ready_cache);
  if (status == Preserve_trx_transfer_status::OK) {
    return registry->mark_saved_online(manifest.epoch_id, manifest.token);
  }

  const Preserve_trx_transfer_status mark_status = registry->mark_cleanup_pending(
      root_dir, manifest.epoch_id, manifest.token, transfer_monotonic_us(),
      Preserve_trx_transfer_receiver_state::CORRUPT,
      "publish_standby_bundle_from_staging:" + transfer_status_name(status));
  return mark_status == Preserve_trx_transfer_status::OK ? status : mark_status;
}

Preserve_trx_transfer_status commit_epoch_manifests(
    const std::string &root_dir,
    const std::vector<Preserve_trx_transfer_manifest> &manifests,
    const Preserve_trx_transfer_manifest &commit_manifest,
    Preserved_trx_store *store) {
  if (store == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  if (manifests.empty()) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  const uint32_t commit_batch_tokens = std::max<uint32_t>(
      1, preserve_trx_transfer_current_runtime_policy().commit_batch_tokens);
  for (size_t manifest_index = 0; manifest_index < manifests.size();
       ++manifest_index) {
    const Preserve_trx_transfer_manifest &manifest =
        manifests[manifest_index];
    const Preserve_trx_transfer_status seal_status =
        preserve_trx_transfer_seal_manifest_objects(root_dir, manifest);
    if (seal_status != Preserve_trx_transfer_status::OK) return seal_status;
    if ((manifest_index + 1) % commit_batch_tokens == 0 &&
        manifest_index + 1 < manifests.size()) {
      preserve_trx_transfer_worker_yield();
    }
  }

  Preserved_trx_carrier_listing listing;
  const Preserve_snapshot_status list_status = store->list_tokens(&listing);
  if (list_status != Preserve_snapshot_status::OK) {
    return map_snapshot_status_to_transfer(list_status);
  }
  for (const Preserve_trx_transfer_manifest &manifest : manifests) {
    const std::string token_component = transfer_token_component(manifest.token);
    if (listing.snapshot_tokens.count(token_component) == 0 ||
        listing.standby_pending_tokens.count(token_component) == 0) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }

  Preserve_trx_transfer_trx_id_store_fact trx_id_store_fact;
  if (!load_source_trx_id_store_fact(&trx_id_store_fact)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  uint64_t sampled_freeze_lsn = 0;
  uint64_t source_fence_lsn = 0;
  if (!load_source_transfer_lsn_fact(&sampled_freeze_lsn,
                                     &source_fence_lsn) ||
      sampled_freeze_lsn > source_fence_lsn ||
      !transfer_trx_id_store_fact_is_valid(trx_id_store_fact,
                                           source_fence_lsn)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  const Preserve_trx_transfer_status fact_status = write_epoch_fact_file(
      root_dir, manifests, source_fence_lsn, trx_id_store_fact);
  if (fact_status != Preserve_trx_transfer_status::OK) return fact_status;
  return write_commit_marker_file(root_dir, commit_manifest);
}

Preserve_trx_transfer_status commit_epoch_final_metadata(
    const std::string &root_dir,
    const std::vector<Preserve_trx_transfer_manifest> &manifests,
    const Preserve_trx_transfer_manifest &commit_manifest,
    uint64_t source_fence_lsn,
    const Preserve_trx_transfer_trx_id_store_fact &trx_id_store_fact) {
  if (root_dir.empty() || manifests.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  for (const Preserve_trx_transfer_manifest &manifest : manifests) {
    const Preserve_trx_transfer_status validation_status =
        validate_manifest_components(manifest, false);
    if (validation_status != Preserve_trx_transfer_status::OK) {
      return validation_status;
    }
  }
  const Preserve_trx_transfer_status fact_status =
      write_epoch_fact_file(root_dir, manifests, source_fence_lsn,
                            trx_id_store_fact);
  if (fact_status != Preserve_trx_transfer_status::OK) return fact_status;
  return write_commit_marker_file(root_dir, commit_manifest);
}

enum class Receiver_prewarm_job_kind { STAGED_TOKEN, OBJECT };

uint64_t receiver_manifest_object_bytes(
    const Preserve_trx_transfer_manifest &manifest) {
  uint64_t total = 0;
  for (const auto &object : manifest.objects) {
    if (object.total_size > std::numeric_limits<uint64_t>::max() - total) {
      return std::numeric_limits<uint64_t>::max();
    }
    total += object.total_size;
  }
  return total;
}

uint64_t receiver_staged_token_file_read_bytes(
    const Preserve_trx_transfer_manifest &manifest,
    bool defer_binlog_read_budget = false) {
  if (!transfer_manifest_uses_strict_metadata_only_prewarm(manifest)) {
    return receiver_manifest_object_bytes(manifest);
  }
  uint64_t total = 0;
  for (const auto &object : manifest.objects) {
    /* Record locks were consumed into a plan. TEMP sidecars are only validated
       here; their preparation steps charge the actual reads and writes. */
    if (transfer_object_uses_strict_v1_memory_staging(manifest, object) ||
        object.kind == Preserve_trx_transfer_object_kind::CURSOR_RESULT ||
        object.kind == Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR ||
        object.object_id == kPreservedTrxBlobRecordLocks ||
        (defer_binlog_read_budget &&
         object.object_id == kPreservedTrxBlobBinlogCache)) {
      continue;
    }
    if (object.total_size > std::numeric_limits<uint64_t>::max() - total) {
      return std::numeric_limits<uint64_t>::max();
    }
    total += object.total_size;
  }
  return total;
}

uint64_t receiver_object_prewarm_file_read_bytes(
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id, bool seed_metadata_only = false,
    const Preserve_trx_receiver_binlog_prefix_ref &prefix = nullptr) {
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, object_id);
  if (object == nullptr) return 0;
  if (object_id == kPreservedTrxBlobRecordLocks) return object->total_size;
  if (object_id == kPreservedTrxBlobBinlogCache)
    return preserved_trx_receiver_binlog_prefix_remaining(prefix, *object);
  if (object_id != kBinlogPrewarmSeedObjectId) return 0;
  const auto *binlog = find_object(manifest, kPreservedTrxBlobBinlogCache);
  if (binlog == nullptr ||
      binlog->total_size >
          std::numeric_limits<uint64_t>::max() - object->total_size) {
    return std::numeric_limits<uint64_t>::max();
  }
  /* The production seed path charges payload IO only if it actually falls back
     to a full copy, after attempting the final prefix handoff. */
  return object->total_size + (seed_metadata_only ? 0 : binlog->total_size);
}

#ifndef NDEBUG
struct Receiver_temp_probe_result {
  std::mutex mutex;
  std::condition_variable cv;
  bool done{false};
  Preserve_trx_temp_receiver_work::Owner temp;
  uint64_t batches{0};
  std::string cancelled_directory;
};
struct Receiver_temp_probe_completion {
  std::shared_ptr<Receiver_temp_probe_result> result;
  my_thread_id source_thread{0};
  bool cancel_after_temp_write{false};
  bool temp_sql_attach{false};
  Preserve_trx_temp_receiver_work::Owner temp;
  uint64_t batches{0};
  std::string cancelled_directory;
  ~Receiver_temp_probe_completion() {
    std::lock_guard<std::mutex> guard(result->mutex);
    result->temp = std::move(temp);
    result->batches = batches;
    result->cancelled_directory = std::move(cancelled_directory);
    result->done = true;
    result->cv.notify_all();
  }
};
#endif

struct Receiver_prewarm_job {
  Receiver_prewarm_job() = default;
  Receiver_prewarm_job(Receiver_prewarm_job &&) noexcept = default;
  Receiver_prewarm_job &operator=(Receiver_prewarm_job &&) = delete;
  ~Receiver_prewarm_job() {
    if (result_candidates) {
      if (temp_undo) result_candidates->abandon_undo(object_id, result_file);
      else if (!temp_candidate) result_candidates->abandon_result(object_id);
    }
  }
  Receiver_prewarm_job_kind kind{Receiver_prewarm_job_kind::STAGED_TOKEN};
  std::string root_dir;
  Preserve_trx_transfer_manifest manifest;
  std::string object_id;
  Receiver_object_prewarm_key object_key;
  Preserve_trx_transfer_receiver_registry *registry{nullptr};
  Preserve_trx_transfer_runtime_policy runtime_policy;
  bool runtime_policy_snapshotted{false};
  bool temp_id_contract_bound{false};
  Preserve_trx_temp_id_contract temp_id_contract;
  bool objects_already_sealed{false};
  bool retry_stale_record_lock_proof{false};
  uint staged_retry_attempts{0};
  uint object_retry_attempts{0};
  uint64_t estimated_io_bytes{0};
  uint64_t enqueued_us{0};
  bool resource_preparation_noted{false};
  Preserve_trx_receiver_binlog_prefix_ref binlog_prefix;
  std::shared_ptr<Preserve_trx_receiver_candidates> result_candidates;
  std::shared_ptr<const Preserve_trx_sealed_file> result_file;
  bool temp_undo{false}, temp_candidate{false};
#ifndef NDEBUG
  // Declared before work so completion signals after its memory is freed.
  std::unique_ptr<Receiver_temp_probe_completion> temp_probe;
#endif
  std::unique_ptr<Preserve_trx_receiver_prepare_work> work;

  void note_resource_prepared() {
    if (!resource_preparation_noted && enqueued_us && work &&
        (work->has_temp() || work->has_results())) {
      preserve_trx_temp_prepared_job_note(transfer_monotonic_us() - enqueued_us);
      resource_preparation_noted = true;
    }
  }
};

constexpr uint kReceiverStagedTokenRetryLimit = 3;

struct Receiver_staged_token_prewarm_key {
  std::string root_dir;
  std::string epoch_id;
  uint64_t token{0};

  bool operator<(const Receiver_staged_token_prewarm_key &rhs) const {
    return std::tie(root_dir, epoch_id, token) <
           std::tie(rhs.root_dir, rhs.epoch_id, rhs.token);
  }
};

std::mutex g_receiver_prewarm_mutex;
std::set<Receiver_strict_token_key> g_receiver_binlog_active_tokens;
std::condition_variable g_receiver_prewarm_cv;
std::list<Receiver_prewarm_job> g_receiver_staged_token_prewarm_jobs;
std::list<Receiver_prewarm_job> g_receiver_prewarm_jobs;
// Pool-wide: the next free worker gives object dependencies a turn between
// batches, even when a different thread wins the epoch's capacity slot.
bool g_receiver_prefer_object_after_prepare_batch = false;
std::vector<std::thread> g_receiver_prewarm_workers;
std::set<Receiver_staged_token_prewarm_key>
    g_receiver_staged_token_prewarm_inflight;
std::set<Receiver_staged_token_prewarm_key>
    g_receiver_staged_token_prewarm_done;
std::set<Receiver_staged_token_prewarm_key>
    g_receiver_staged_token_prewarm_deferred;
std::set<Receiver_object_prewarm_key> g_receiver_object_prewarm_inflight;
std::map<Receiver_object_prewarm_key, Preserve_trx_transfer_manifest>
    g_receiver_object_prewarm_deferred;
std::map<Receiver_object_prewarm_key, uint64_t>
    g_receiver_record_plan_attempted_generation;
using Receiver_prewarm_epoch_key =
    std::pair<Preserve_trx_transfer_receiver_registry *, std::string>;
std::map<Receiver_prewarm_epoch_key, size_t>
    g_receiver_prewarm_active_by_epoch;
constexpr uint kReceiverPrewarmPoolMaxWorkers = 8;
std::set<Preserve_trx_transfer_receiver_registry *>
    g_receiver_prewarm_retiring_registries;
bool g_receiver_prewarm_workers_started = false;
bool g_receiver_prewarm_workers_starting = false;
bool g_receiver_prewarm_workers_stopping = false;
bool g_receiver_prewarm_shutdown = false;
bool g_receiver_prewarm_runtime_stop = false;
bool g_receiver_prewarm_idle_stop_requested = false;
size_t g_receiver_prewarm_worker_init_reports = 0;
size_t g_receiver_prewarm_worker_init_failures = 0;
std::atomic<uint> g_receiver_prewarm_worker_init_index{0};
std::atomic<int> g_receiver_prewarm_fail_create_at_for_unit_test{-1};
std::atomic<int> g_receiver_prewarm_fail_init_at_for_unit_test{-1};

Receiver_prewarm_epoch_key receiver_prewarm_epoch_key(
    const Receiver_prewarm_job &job) {
  return {job.registry, job.manifest.epoch_id};
}

bool receiver_prewarm_job_has_capacity_locked(
    const Receiver_prewarm_job &job) {
  const auto found =
      g_receiver_prewarm_active_by_epoch.find(receiver_prewarm_epoch_key(job));
  const size_t active = found == g_receiver_prewarm_active_by_epoch.end()
                            ? 0
                            : found->second;
  return active < std::max<uint32_t>(1, job.runtime_policy.prewarm_workers);
}

void subtract_receiver_queued_bytes(uint64_t bytes) {
  uint64_t current = g_receiver_queued_bytes.load();
  while (!g_receiver_queued_bytes.compare_exchange_weak(
      current, current >= bytes ? current - bytes : 0)) {
  }
}

bool receiver_prewarm_work_idle_locked() {
  return g_receiver_staged_token_prewarm_jobs.empty() &&
         g_receiver_prewarm_jobs.empty() &&
         g_receiver_staged_token_prewarm_inflight.empty() &&
         g_receiver_staged_token_prewarm_deferred.empty() &&
         g_receiver_object_prewarm_inflight.empty() &&
         g_receiver_object_prewarm_deferred.empty() &&
         g_receiver_prewarm_active_by_epoch.empty() &&
         g_receiver_prewarm_retiring_registries.empty() &&
         g_receiver_worker_active.load() == 0;
}

void request_receiver_prewarm_idle_stop() {
  bool requested = false;
  {
    std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
    if (g_receiver_prewarm_workers_started &&
        !g_receiver_prewarm_workers_starting &&
        !g_receiver_prewarm_workers_stopping &&
        !g_receiver_prewarm_runtime_stop &&
        receiver_prewarm_work_idle_locked()) {
      g_receiver_prewarm_idle_stop_requested = true;
      requested = true;
    }
  }
  if (requested) preserved_trx_request_expired_reaper_scan();
}

void retire_receiver_prewarm_workers_if_idle() {
  std::vector<std::thread> workers;
  {
    std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
    if (!g_receiver_prewarm_idle_stop_requested ||
        !g_receiver_prewarm_workers_started ||
        g_receiver_prewarm_workers_starting ||
        g_receiver_prewarm_workers_stopping ||
        !receiver_prewarm_work_idle_locked()) {
      return;
    }
    g_receiver_prewarm_workers_stopping = true;
    g_receiver_prewarm_runtime_stop = true;
    g_receiver_prewarm_idle_stop_requested = false;
    workers.swap(g_receiver_prewarm_workers);
    g_receiver_prewarm_workers_started = false;
    g_receiver_worker_count.store(0);
  }
  g_receiver_prewarm_cv.notify_all();
  for (std::thread &worker : workers) {
    if (worker.joinable()) worker.join();
  }
  {
    std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
    g_receiver_prewarm_runtime_stop = false;
    g_receiver_prewarm_workers_stopping = false;
  }
  g_receiver_prewarm_cv.notify_all();
}

bool receiver_prewarm_job_matches_epoch(
    const Receiver_prewarm_job &job, const std::string &root_dir,
    const std::string &epoch_id) {
  if (job.root_dir != root_dir) return false;
  return job.manifest.epoch_id == epoch_id;
}

void purge_receiver_epoch_prewarm_queues(const std::string &root_dir,
                                         const std::string &epoch_id) {
  std::list<Receiver_prewarm_job> retired;
  {
    std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
    const auto erase_jobs = [&](std::list<Receiver_prewarm_job> *jobs) {
      for (auto job = jobs->begin(); job != jobs->end();) {
        if (!receiver_prewarm_job_matches_epoch(*job, root_dir, epoch_id)) {
          ++job;
          continue;
        }
        subtract_receiver_queued_bytes(job->estimated_io_bytes);
        const auto removed = job++;
        retired.splice(retired.end(), *jobs, removed);
      }
    };
    erase_jobs(&g_receiver_staged_token_prewarm_jobs);
    erase_jobs(&g_receiver_prewarm_jobs);

    const auto staged_matches = [&](const auto &key) {
      return key.root_dir == root_dir && key.epoch_id == epoch_id;
    };
    for (auto key = g_receiver_staged_token_prewarm_inflight.begin();
         key != g_receiver_staged_token_prewarm_inflight.end();) {
      key = staged_matches(*key)
                ? g_receiver_staged_token_prewarm_inflight.erase(key)
                : std::next(key);
    }
    for (auto key = g_receiver_staged_token_prewarm_done.begin();
         key != g_receiver_staged_token_prewarm_done.end();) {
      key = staged_matches(*key)
                ? g_receiver_staged_token_prewarm_done.erase(key)
                : std::next(key);
    }
    for (auto key = g_receiver_staged_token_prewarm_deferred.begin();
         key != g_receiver_staged_token_prewarm_deferred.end();) {
      key = staged_matches(*key)
                ? g_receiver_staged_token_prewarm_deferred.erase(key)
                : std::next(key);
    }

    const auto object_matches = [&](const auto &key) {
      return key.root_dir == root_dir && key.epoch_id == epoch_id;
    };
    for (auto key = g_receiver_object_prewarm_inflight.begin();
         key != g_receiver_object_prewarm_inflight.end();) {
      key = object_matches(*key) ? g_receiver_object_prewarm_inflight.erase(key)
                                 : std::next(key);
    }
    for (auto key = g_receiver_object_prewarm_deferred.begin();
         key != g_receiver_object_prewarm_deferred.end();) {
      key = object_matches(key->first)
                ? g_receiver_object_prewarm_deferred.erase(key)
                : std::next(key);
    }
    for (auto key = g_receiver_record_plan_attempted_generation.begin();
         key != g_receiver_record_plan_attempted_generation.end();) {
      key = object_matches(key->first)
                ? g_receiver_record_plan_attempted_generation.erase(key)
                : std::next(key);
    }
  }
  erase_receiver_binlog_prepared(root_dir, epoch_id);
  g_receiver_prewarm_cv.notify_all();
  request_receiver_prewarm_idle_stop();
}

void purge_receiver_epoch_derived_state(const std::string &root_dir,
                                        const std::string &epoch_id,
                                        const std::string &epoch_scope) {
  purge_receiver_epoch_prewarm_queues(root_dir, epoch_id);
  Preserve_trx_temp_final_sample temp_final;
  {
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    const auto it = g_receiver_ready_epoch_state.find({root_dir, epoch_id});
    if (it != g_receiver_ready_epoch_state.end()) {
      temp_final = it->second.temp_final.finish(
          transfer_monotonic_us(), Preserve_trx_temp_final_outcome::CANCELLED);
      g_receiver_ready_epoch_state.erase(it);
    }
  }
  preserve_trx_temp_final_log(epoch_id, temp_final);
  {
    std::lock_guard<std::mutex> guard(g_receiver_object_prewarm_proof_mutex);
    for (auto proof = g_receiver_object_prewarm_proofs.begin();
         proof != g_receiver_object_prewarm_proofs.end();) {
      proof = proof->first.root_dir == root_dir &&
                      proof->first.epoch_id == epoch_id
                  ? g_receiver_object_prewarm_proofs.erase(proof)
                  : std::next(proof);
    }
  }
  {
    std::lock_guard<std::mutex> guard(g_receiver_record_lock_prepared_mutex);
    for (auto plan = g_receiver_record_lock_prepared.begin();
         plan != g_receiver_record_lock_prepared.end();) {
      plan = plan->first.root_dir == root_dir &&
                     plan->first.epoch_id == epoch_id
                 ? g_receiver_record_lock_prepared.erase(plan)
                 : std::next(plan);
    }
  }
  {
    std::lock_guard<std::mutex> guard(
        g_receiver_strict_record_lock_facts_mutex);
    for (auto facts = g_receiver_strict_record_lock_facts.begin();
         facts != g_receiver_strict_record_lock_facts.end();) {
      facts = std::get<0>(facts->first) == root_dir &&
                      std::get<1>(facts->first) == epoch_id
                  ? g_receiver_strict_record_lock_facts.erase(facts)
                  : std::next(facts);
    }
  }
  {
    std::lock_guard<std::mutex> guard(g_receiver_strict_binlog_facts_mutex);
    for (auto facts = g_receiver_strict_binlog_facts.begin();
         facts != g_receiver_strict_binlog_facts.end();) {
      facts = std::get<0>(facts->first) == root_dir &&
                      std::get<1>(facts->first) == epoch_id
                  ? g_receiver_strict_binlog_facts.erase(facts)
                  : std::next(facts);
    }
  }
  preserved_trx_promotion_ready_cache_purge_epoch(root_dir, epoch_id);
  if (!epoch_scope.empty()) {
    preserved_trx_strict_prepared_token_registry().purge_epoch(epoch_scope,
                                                               epoch_id);
  }
}

void preserve_trx_transfer_set_prewarm_paused(bool paused) {
  preserve_trx_transfer_prewarm_paused = paused;
  g_receiver_prewarm_paused.store(paused, std::memory_order_release);
  g_receiver_prewarm_cv.notify_all();
}
bool g_receiver_prewarm_pause_init_report_for_unit_test = false;
std::atomic<int> g_temporary_worker_fail_create_at_for_unit_test{-1};
Preserve_trx_transfer_status enqueue_receiver_staged_token_prewarm(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    Preserve_trx_transfer_receiver_registry *registry);
Preserve_trx_transfer_status enqueue_receiver_object_prewarm(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id,
    Preserve_trx_transfer_receiver_registry *registry,
    bool retry_stale_record_lock_proof = false);
Preserve_trx_transfer_status enqueue_receiver_prewarm_job(
    Receiver_prewarm_job &&job);
bool receiver_epoch_expired_or_removed(
    const std::string &root_dir,
    Preserve_trx_transfer_receiver_registry *registry,
    const Preserve_trx_transfer_manifest &manifest,
    Preserve_trx_temp_id_contract *temp_id_contract);
void purge_receiver_epoch_derived_state_after_worker_stop(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_receiver_registry *registry);

Receiver_staged_token_prewarm_key receiver_staged_token_prewarm_key(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest) {
  Receiver_staged_token_prewarm_key key;
  key.root_dir = root_dir;
  key.epoch_id = manifest.epoch_id;
  key.token = manifest.token;
  return key;
}

bool receiver_object_prewarm_key(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id, Receiver_object_prewarm_key *key) {
  if (key == nullptr) return false;
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(manifest, object_id);
  if (object == nullptr) return false;
  key->root_dir = root_dir;
  key->epoch_id = manifest.epoch_id;
  key->token = manifest.token;
  key->object_id = object_id;
  key->digest = object->digest;
  key->source_live_generation = object->lock_plan.source_live_generation;
  key->source_live_digest = object->lock_plan.source_live_digest;
  return true;
}

enum class Receiver_object_prewarm_proof_state { MISSING, STALE, READY };

Receiver_object_prewarm_proof_state receiver_object_prewarm_proof_state(
    const Receiver_object_prewarm_key &key, bool require_metadata_only) {
  std::lock_guard<std::mutex> guard(g_receiver_object_prewarm_proof_mutex);
  const auto found = g_receiver_object_prewarm_proofs.find(key);
  if (found == g_receiver_object_prewarm_proofs.end())
    return Receiver_object_prewarm_proof_state::MISSING;
  if (require_metadata_only && found->second.record_lock_object &&
      !found->second.metadata_only) {
    return Receiver_object_prewarm_proof_state::STALE;
  }
  if (!found->second.record_lock_object ||
      receiver_record_lock_proof_gate_ready(found->second)) {
    return Receiver_object_prewarm_proof_state::READY;
  }
  return Receiver_object_prewarm_proof_state::STALE;
}

bool receiver_staged_token_prewarm_job_runnable(
    const Receiver_prewarm_job &job) {
  if (receiver_binlog_prepare_pending(job.root_dir, job.manifest)) return false;
  if (!transfer_manifest_uses_v1_metadata_only_lock_prewarm(job.manifest)) {
    return true;
  }
  Receiver_object_prewarm_key key;
  if (!receiver_object_prewarm_key(job.root_dir, job.manifest,
                                    kPreservedTrxBlobRecordLocks, &key)) {
    return true;
  }
  /*
    A strict staged job consumes the prepared lock plan. Let the object job
    publish its metadata-only proof first so readiness never depends on which
    queue an eligible worker observes first.
  */
  if (receiver_object_prewarm_proof_state(key, true) !=
      Receiver_object_prewarm_proof_state::READY) {
    return false;
  }
  return receiver_record_lock_prepared_exists(job.root_dir, job.manifest);
}

bool receiver_job_uses_binlog_prefix(const Receiver_prewarm_job &job) {
  return job.runtime_policy.profile ==
             PRESERVE_TRX_TRANSFER_RUNTIME_PROMOTION_PREPARE &&
         find_object(job.manifest, kPreservedTrxBlobBinlogCache) != nullptr &&
         (job.kind == Receiver_prewarm_job_kind::STAGED_TOKEN ||
          job.object_id == kPreservedTrxBlobBinlogCache ||
          job.object_id == kBinlogPrewarmSeedObjectId);
}

/* Called with the pool lock: a seed never occupies a worker waiting for bytes
   another worker in this pool must prepare. Different tokens remain parallel. */
bool receiver_binlog_job_runnable_locked(const Receiver_prewarm_job &job) {
  if (!receiver_job_uses_binlog_prefix(job)) return true;
  if (g_receiver_binlog_active_tokens.count(
          receiver_strict_token_key(job.root_dir, job.manifest)) != 0)
    return false;
  const auto *binlog = find_object(job.manifest, kPreservedTrxBlobBinlogCache);
  return job.object_id != kBinlogPrewarmSeedObjectId ||
         preserved_trx_receiver_binlog_prefix_take(
             job.root_dir, job.manifest, *binlog) !=
             Preserve_trx_receiver_binlog_prefix_status::PENDING;
}

const char *receiver_object_prewarm_proof_state_name(
    Receiver_object_prewarm_proof_state state) {
  switch (state) {
    case Receiver_object_prewarm_proof_state::MISSING:
      return "MISSING";
    case Receiver_object_prewarm_proof_state::STALE:
      return "STALE";
    case Receiver_object_prewarm_proof_state::READY:
      return "READY";
  }
  return "UNKNOWN";
}

void log_receiver_staged_token_prewarm_deferred(
    const Receiver_prewarm_job &job,
    const Receiver_staged_token_prewarm_result &result) {
  if (result.outcome !=
          Receiver_staged_token_prewarm_outcome::WAIT_DEPENDENCY &&
      result.outcome !=
          Receiver_staged_token_prewarm_outcome::RETRYABLE_NOT_READY) {
    return;
  }

  const bool binlog_pending =
      receiver_binlog_prepare_pending(job.root_dir, job.manifest);
  Receiver_object_prewarm_key record_lock_key;
  const bool has_record_lock_object = receiver_object_prewarm_key(
      job.root_dir, job.manifest, kPreservedTrxBlobRecordLocks,
      &record_lock_key);
  const char *record_lock_proof =
      has_record_lock_object
          ? receiver_object_prewarm_proof_state_name(
                receiver_object_prewarm_proof_state(record_lock_key, true))
          : "NOT_APPLICABLE";
  const bool record_lock_plan_prepared =
      has_record_lock_object &&
      receiver_record_lock_prepared_exists(job.root_dir, job.manifest);

  std::ostringstream message;
  message << "PRESERVE: receiver staged-token prewarm deferred"
          << " epoch=" << job.manifest.epoch_id
          << " token=" << job.manifest.token
          << " outcome="
          << receiver_staged_token_prewarm_outcome_name(result.outcome)
          << " stage=" << receiver_staged_token_prewarm_stage_name(result.stage)
          << " retry_attempt=" << job.staged_retry_attempts
          << " status="
          << preserve_trx_promotion_adopt_status_name(result.status)
          << " failure_reason="
          << static_cast<unsigned int>(result.failure_reason)
          << " binlog_pending=" << binlog_pending
          << " record_lock_object=" << has_record_lock_object
          << " record_lock_proof=" << record_lock_proof
          << " record_lock_plan_prepared=" << record_lock_plan_prepared;
  LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.str().c_str());
}

void finish_receiver_staged_token_prewarm_job(
    std::list<Receiver_prewarm_job> *active_job,
    Receiver_staged_token_prewarm_outcome outcome,
    bool expired_or_removed) {
  auto &job = active_job->front();
  Receiver_staged_token_prewarm_key key =
      receiver_staged_token_prewarm_key(job.root_dir, job.manifest);
  bool notify_retry = false;
  {
    std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
    const bool still_owned = g_receiver_staged_token_prewarm_inflight.count(key) != 0;
    g_receiver_staged_token_prewarm_deferred.erase(key);
    const bool dependency_wait =
        outcome == Receiver_staged_token_prewarm_outcome::WAIT_DEPENDENCY;
    const bool retryable_not_ready =
        outcome ==
        Receiver_staged_token_prewarm_outcome::RETRYABLE_NOT_READY;
    const bool retry_budget_available =
        job.staged_retry_attempts < kReceiverStagedTokenRetryLimit;
    const bool requeue =
        outcome == Receiver_staged_token_prewarm_outcome::CONTINUE_PREPARATION ||
        dependency_wait || (retryable_not_ready && retry_budget_available);
    if (outcome != Receiver_staged_token_prewarm_outcome::READY &&
        (expired_or_removed || !requeue || !still_owned ||
         g_receiver_prewarm_shutdown || g_receiver_prewarm_runtime_stop ||
         g_receiver_prewarm_retiring_registries.count(job.registry))) {
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: receiver resource preparation stopped token=" +
              std::to_string(job.manifest.token) + " outcome=" +
              receiver_staged_token_prewarm_outcome_name(outcome) +
              " expired=" + std::to_string(expired_or_removed) +
              " owned=" + std::to_string(still_owned)).c_str());
    }
    if (!expired_or_removed && requeue && still_owned && !g_receiver_prewarm_shutdown &&
               !g_receiver_prewarm_runtime_stop &&
               g_receiver_prewarm_retiring_registries.count(job.registry) ==
                   0) {
      if (retryable_not_ready) ++job.staged_retry_attempts;
      g_receiver_queued_bytes.fetch_add(job.estimated_io_bytes);
      g_receiver_staged_token_prewarm_jobs.splice(
          g_receiver_staged_token_prewarm_jobs.end(), *active_job);
      if (outcome == Receiver_staged_token_prewarm_outcome::CONTINUE_PREPARATION)
        g_receiver_prefer_object_after_prepare_batch = true;
      notify_retry = true;
    } else {
      // Insert before releasing ownership, so allocation failure is handled by
      // the worker's epoch-failure path rather than allowing a duplicate job.
      if (!expired_or_removed && still_owned)
        g_receiver_staged_token_prewarm_done.insert(key);
      g_receiver_staged_token_prewarm_inflight.erase(key);
    }
  }
  if (notify_retry) {
    if (outcome == Receiver_staged_token_prewarm_outcome::RETRYABLE_NOT_READY)
      note_receiver_temp_final(key.root_dir, key.epoch_id, Temp_final_field::RETRIES);
    else if (outcome == Receiver_staged_token_prewarm_outcome::WAIT_DEPENDENCY)
      note_receiver_temp_final(key.root_dir, key.epoch_id, Temp_final_field::WAIT_BATCHES);
    g_receiver_prewarm_cv.notify_all();
  }
}

bool finish_receiver_object_prewarm_job(
    const Receiver_object_prewarm_key &key,
    Preserve_trx_transfer_manifest *deferred_manifest) {
  std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
  g_receiver_object_prewarm_inflight.erase(key);
  const auto deferred = g_receiver_object_prewarm_deferred.find(key);
  if (deferred == g_receiver_object_prewarm_deferred.end()) return false;
  if (deferred_manifest != nullptr) {
    *deferred_manifest = std::move(deferred->second);
  }
  g_receiver_object_prewarm_deferred.erase(deferred);
  return deferred_manifest != nullptr;
}

Preserve_trx_transfer_status finalize_receiver_ready_token_staging(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    Preserve_trx_transfer_receiver_registry *registry) {
  if (registry == nullptr) return Preserve_trx_transfer_status::OK;
  const bool process_local_epoch_accepted =
      registry->query_accepted_epoch(root_dir, manifest.epoch_id) ==
      Preserve_trx_transfer_status::COMMITTED_NOT_READY;
  if (!process_local_epoch_accepted &&
      !preserve_trx_transfer_epoch_committed(root_dir, manifest.epoch_id)) {
    return Preserve_trx_transfer_status::OK;
  }
  if (!receiver_seal_prewarm_token_ok(root_dir, manifest.epoch_id,
                                      manifest.token)) {
    return Preserve_trx_transfer_status::OK;
  }
  if (!receiver_epoch_ready_is_bound(root_dir, manifest.epoch_id)) {
    return Preserve_trx_transfer_status::OK;
  }

  std::lock_guard<std::mutex> finalize_guard(receiver_staging_finalize_mutex(
      root_dir, manifest.epoch_id, manifest.token));
  Preserve_trx_transfer_receiver_record current;
  if (!registry->lookup(manifest.epoch_id, manifest.token, &current)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (current.state == Preserve_trx_transfer_receiver_state::SAVED_ONLINE ||
      current.state == Preserve_trx_transfer_receiver_state::CLEANUP_PENDING) {
    return Preserve_trx_transfer_status::OK;
  }
  if (current.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  /*
    Record-lock object prewarm reads the transfer staging object. Online READY is
    bound to the committed epoch fact and ready cache; staging cannot be removed
    until the token has actually populated the ready cache.
  */
  Preserve_trx_transfer_status status = cleanup_transfer_token_staging(
      root_dir, manifest.epoch_id, manifest.token);
  if (status != Preserve_trx_transfer_status::OK) {
    return registry->mark_cleanup_pending(
        root_dir, manifest.epoch_id, manifest.token, transfer_monotonic_us(),
        Preserve_trx_transfer_receiver_state::SAVED_ONLINE,
        "staging_cleanup_failed:" + transfer_status_name(status));
  }
  status = registry->mark_saved_online(manifest.epoch_id, manifest.token, true);
  if (status != Preserve_trx_transfer_status::UNSUPPORTED) return status;

  Preserve_trx_transfer_receiver_record record;
  if (registry->lookup(manifest.epoch_id, manifest.token, &record) &&
      record.state == Preserve_trx_transfer_receiver_state::SAVED_ONLINE) {
    return Preserve_trx_transfer_status::OK;
  }
  return status;
}

Preserve_trx_transfer_status
preserve_trx_transfer_finalize_receiver_staging_for_unit_test(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    Preserve_trx_transfer_receiver_registry *registry, uint64_t now_us) {
  if (registry == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  const Preserve_trx_transfer_status cleanup_status =
      cleanup_transfer_token_staging(root_dir, manifest.epoch_id,
                                     manifest.token);
  if (cleanup_status == Preserve_trx_transfer_status::OK) {
    return registry->mark_saved_online(manifest.epoch_id, manifest.token, true);
  }
  return registry->mark_cleanup_pending(
      root_dir, manifest.epoch_id, manifest.token, now_us,
      Preserve_trx_transfer_receiver_state::SAVED_ONLINE,
      "staging_cleanup_failed:" + transfer_status_name(cleanup_status));
}

Preserve_trx_transfer_status
preserve_trx_transfer_acknowledge_epoch_for_unit_test(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_receiver_registry *registry, uint64_t now_us,
    uint64_t grace_us) {
  if (registry == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  return registry->acknowledge_epoch(root_dir, epoch_id, now_us, grace_us);
}

void receiver_reaper_scan_once(
    uint64_t now_us, Preserve_trx_transfer_receiver_registry *registry) {
  if (registry == nullptr) return;
  for (const auto &epoch : registry->prewarming_epochs_due(now_us)) {
    (void)publish_receiver_epoch_selection_if_possible(
        epoch.first, epoch.second, registry, now_us, true);
  }
  std::vector<Preserve_trx_transfer_accepted_epoch> expired;
  (void)registry->expire_accepted_epochs_once(now_us, &expired);
  for (const Preserve_trx_transfer_accepted_epoch &accepted : expired) {
    const Preserve_trx_transfer_status destroy_status =
        preserve_trx_transfer_destroy_receiver_epoch_process_local(accepted,
                                                                   registry);
    if (destroy_status != Preserve_trx_transfer_status::OK) {
      const std::string message =
          "PRESERVE: receiver epoch cleanup remains pending epoch=" +
          accepted.epoch_id +
          " status=" + transfer_status_name(destroy_status);
      LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    }
  }
  retry_unbound_receiver_epochs_once(registry);
  for (const auto &epoch : bound_receiver_epochs()) {
    const auto records =
        registry->sealed_receiving_records_for_epoch(epoch.second);
    for (const auto &record : records) {
      (void)finalize_receiver_ready_token_staging(
          epoch.first, receiver_record_manifest(record), registry);
    }
  }
  (void)registry->retry_cleanup_debt_once(now_us);
  (void)registry->reap_retired_staging_once();
  (void)registry->retire_acknowledged_epochs_once(now_us);
}

Preserve_trx_transfer_status
preserve_trx_transfer_destroy_receiver_epoch_process_local(
    const Preserve_trx_transfer_accepted_epoch &accepted,
    Preserve_trx_transfer_receiver_registry *registry) {
  if (registry == nullptr || accepted.root_dir.empty() ||
      !transfer_component_safe(accepted.epoch_id) ||
      (accepted.lifecycle !=
           Preserve_trx_transfer_epoch_lifecycle::ABANDONING &&
       accepted.lifecycle != Preserve_trx_transfer_epoch_lifecycle::EXPIRED)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  purge_receiver_epoch_derived_state(
      accepted.root_dir, accepted.epoch_id, receiver_boot_incarnation());

  Preserve_trx_transfer_status cleanup_status =
      Preserve_trx_transfer_status::OK;
  if (accepted.flat_projection_published) {
    auto store = create_preserved_trx_process_local_store(accepted.root_dir);
    for (uint64_t token : accepted.tokens) {
      if (store->remove_with_status(transfer_token_component(token)) !=
          Preserve_snapshot_delete_status::OK) {
        cleanup_status = Preserve_trx_transfer_status::IO_ERROR;
      }
    }
  }

  const std::string epoch_dir =
      transfer_epoch_dir_for_epoch(accepted.root_dir, accepted.epoch_id);
  if (remove_receiver_restart_tree(epoch_dir, 0) !=
      Preserve_trx_transfer_status::OK) {
    cleanup_status = Preserve_trx_transfer_status::IO_ERROR;
  }
  if (cleanup_status != Preserve_trx_transfer_status::OK) {
    return cleanup_status;
  }

  return accepted.lifecycle ==
                 Preserve_trx_transfer_epoch_lifecycle::ABANDONING
             ? registry->erase_abandoning_epoch(accepted.root_dir,
                                                accepted.epoch_id)
             : registry->erase_expired_epoch(accepted.root_dir,
                                             accepted.epoch_id);
}

void preserve_trx_transfer_receiver_reaper_scan_once(uint64_t now_us) {
  receiver_reaper_scan_once(now_us, &default_receiver_registry());
  retire_receiver_prewarm_workers_if_idle();
}

Preserve_trx_receiver_promotion_lease_status
preserve_trx_transfer_try_acquire_receiver_promotion_lease(
    const std::string &root_dir, const std::string &epoch_id, uint64_t now_us,
    Preserve_trx_transfer_accepted_epoch *accepted) {
  return default_receiver_registry()
      .try_acquire_accepted_epoch_promotion_lease(
          root_dir, epoch_id, now_us, receiver_boot_incarnation(), accepted);
}

Preserve_trx_transfer_status
preserve_trx_transfer_abandon_receiver_promotion_lease_process_local(
    Preserve_trx_transfer_accepted_epoch *accepted) {
  if (accepted == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  auto &registry = default_receiver_registry();
  const auto abandon_status =
      registry.abandon_accepted_epoch_promotion_lease(*accepted);
  if (abandon_status == Preserve_trx_transfer_status::OK) {
    accepted->lifecycle = Preserve_trx_transfer_epoch_lifecycle::ABANDONING;
  }
  return abandon_status;
}

Preserve_trx_transfer_status
preserve_trx_transfer_complete_receiver_promotion_lease_process_local(
    const Preserve_trx_transfer_accepted_epoch &accepted) {
  return default_receiver_registry()
      .complete_accepted_epoch_promotion_lease(accepted);
}

Preserve_trx_transfer_status
preserve_trx_transfer_destroy_abandoning_receiver_epoch_process_local(
    const Preserve_trx_transfer_accepted_epoch &accepted) {
  return preserve_trx_transfer_destroy_receiver_epoch_process_local(
      accepted, &default_receiver_registry());
}

void preserve_trx_transfer_receiver_reaper_scan_for_unit_test(
    uint64_t now_us, Preserve_trx_transfer_receiver_registry *registry) {
  receiver_reaper_scan_once(now_us, registry);
}

Receiver_staged_token_prewarm_result run_receiver_staged_token_prewarm_job(
    Receiver_prewarm_job &job) {
  const auto &root_dir = job.root_dir;
  const auto &manifest = job.manifest;
  auto *registry = job.registry;
  const auto &runtime_policy = job.runtime_policy;
  note_receiver_staged_token_job_started();
  auto finish_active = create_scope_guard(
      [] { note_receiver_staged_token_job_finished(); });
  const uint64_t staged_started_us = transfer_monotonic_us();
  auto finish_total = create_scope_guard([&] {
    note_receiver_staged_token_total_us(transfer_monotonic_us() -
                                        staged_started_us);
  });
  const auto record_token_failure =
      [&](Preserve_trx_receiver_failure_reason reason) {
    const bool selection_complete_with_failures =
        note_receiver_epoch_token_result(root_dir, manifest.epoch_id,
                                         manifest.token, reason);
    if (selection_complete_with_failures) {
      (void)publish_receiver_epoch_selection_if_possible(
          root_dir, manifest.epoch_id, registry, transfer_monotonic_us(),
          false);
    }
  };
  Preserve_trx_temp_id_contract observed_contract;
  if (receiver_epoch_expired_or_removed(root_dir, registry, manifest,
                                         &observed_contract)) {
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::EXPIRED,
        Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY);
  }
  if (job.temp_id_contract_bound &&
      job.temp_id_contract != observed_contract) {
    record_token_failure(
        Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
        Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
  }
  job.temp_id_contract = observed_contract;
  job.temp_id_contract_bound = true;
  Receiver_temp_activity_guard temp_activity(root_dir, manifest.epoch_id,
      false, std::any_of(manifest.objects.begin(), manifest.objects.end(),
                        transfer_is_resource_object));
#ifndef NDEBUG
  if (job.temp_probe) {
    if (!job.work || !current_thd ||
        current_thd->thread_id() == job.temp_probe->source_thread ||
        job.work->step(current_thd)) {
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
    }
    if (job.temp_probe->cancel_after_temp_write && *job.work->temp_ready() &&
        (*job.work->temp_ready())->written_bytes() != 0) {
      job.temp_probe->cancelled_directory = (*job.work->temp_ready())->directory();
      job.temp_probe->batches = job.work->batches();
      purge_receiver_epoch_prewarm_queues(root_dir, manifest.epoch_id);
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::EXPIRED,
          Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY);
    }
    if (!job.work->complete()) return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::CONTINUE_PREPARATION,
        Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY);
    if (job.work->has_temp()) {
      const auto &bundle = job.work->bundle();
      std::string reason;
      auto *ready = job.work->temp_ready()->get();
      if (preserved_trx_dry_validate_loaded_bundle(
              root_dir, bundle.metadata.token, bundle, &reason, ready) !=
          Preserve_snapshot_status::OK)
        return receiver_staged_token_result(
            Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
            Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
      DBUG_PRINT("preserve_temp_import", ("temporary receiver validates prepared owner without local sidecars"));
    }
    if (job.work->batches() <= kReceiverStagedTokenRetryLimit)
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
          Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
    if (job.temp_probe->temp_sql_attach &&
        Preserve_trx_temp_restore::probe(job.work->temp_ready()))
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
          Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
    job.note_resource_prepared();
    job.temp_probe->temp = std::move(*job.work->temp_ready());
    job.temp_probe->batches = job.work->batches();
    return receiver_staged_token_result(Receiver_staged_token_prewarm_outcome::READY,
                                       Preserve_trx_promotion_adopt_status::OK);
  }
#endif
  DBUG_EXECUTE_IF("preserve_trx_receiver_fail_epoch_prewarm", {
    note_receiver_epoch_global_failure(root_dir, manifest.epoch_id);
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
  });
  const uint64_t estimated_bytes = receiver_manifest_object_bytes(manifest);
  if (estimated_bytes > runtime_policy.prewarm_max_bytes) {
    note_receiver_seal_prewarm_status(
        Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY);
    record_token_failure(
        Preserve_trx_receiver_failure_reason::TOKEN_RESOURCE_LIMIT);
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
        Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
        Preserve_trx_receiver_failure_reason::TOKEN_RESOURCE_LIMIT);
  }
  const bool defer_binlog_read_budget =
      registry != nullptr &&
      transfer_manifest_uses_strict_metadata_only_prewarm(manifest);
  const uint64_t staged_read_bytes =
      receiver_staged_token_file_read_bytes(manifest, defer_binlog_read_budget);
  if (!job.work) {
    throttle_receiver_prewarm_io(staged_read_bytes, runtime_policy);
    DBUG_EXECUTE_IF("preserve_trx_receiver_trace_staged_io_budget", {
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: staged token file-read budget bytes=" +
              std::to_string(staged_read_bytes) + " epoch=" + manifest.epoch_id +
              " token=" + std::to_string(manifest.token)).c_str());
    });
  }
  Preserved_trx_bundle loaded_bundle;
  auto &staged_bundle = job.work ? job.work->bundle() : loaded_bundle;
  DBUG_EXECUTE_IF("preserve_trx_receiver_trace_staged_bundle", {
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: staged bundle " + std::string(job.work ? "reuse" : "load") +
            " epoch=" + manifest.epoch_id + " token=" +
            std::to_string(manifest.token)).c_str());
  });
  const Preserve_trx_transfer_status load_status =
      job.work ? Preserve_trx_transfer_status::OK :
          preserve_trx_transfer_load_standby_bundle_from_staging(
              root_dir, manifest, &staged_bundle, job.objects_already_sealed, registry);
  if (load_status != Preserve_trx_transfer_status::OK) {
    note_receiver_seal_prewarm_status(
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
    note_receiver_epoch_global_failure(root_dir, manifest.epoch_id);
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
  }
  if (staged_bundle.metadata.global_log_bin != mysql_bin_log.is_open() ||
      !staged_bundle.metadata.has_binlog_gtid_mode ||
      staged_bundle.metadata.binlog_gtid_mode !=
          static_cast<uint8_t>(global_gtid_mode.get())) {
    const auto result = receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
        Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT,
        Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
    note_receiver_seal_prewarm_status(result.status);
    record_token_failure(result.failure_reason);
    return result;
  }

  DBUG_EXECUTE_IF("preserve_trx_receiver_probe_final_manifest", {
    if (!job.work && registry != nullptr) {
      using Status = Preserve_trx_transfer_status;
      Preserve_trx_transfer_receiver_record before;
      bool protected_manifest = registry->lookup(
          manifest.epoch_id, manifest.token, &before);
      protected_manifest &= registry->begin_receive(manifest) == Status::OK;
      const auto reject_change = [&](const Preserve_trx_transfer_manifest &changed) {
        const auto status = registry->begin_receive(changed);
        if (status == Status::OK) (void)registry->begin_receive(manifest);
        return status == Status::UNSUPPORTED;
      };
      auto changed = manifest;
      ++changed.source_freeze_lsn;
      protected_manifest &= reject_change(changed);
      changed = manifest;
      ++changed.source_epoch_commit_lsn;
      protected_manifest &= reject_change(changed);
      changed = manifest;
      changed.strict_eligibility_flags ^=
          PRESERVE_TRX_TRANSFER_STRICT_PARTICIPANTS_AUTHENTICATED;
      protected_manifest &= reject_change(changed);
      changed = manifest;
      std::swap(changed.objects.front(), changed.objects.back());
      protected_manifest &= reject_change(changed);
      auto extra = manifest.objects.front();
      extra.object_id = "probe-extra-object";
      const auto declare_status = registry->declare_object(
          manifest.epoch_id, manifest.token, extra);
      protected_manifest &= declare_status == Status::UNSUPPORTED;
      if (declare_status == Status::OK) (void)registry->begin_receive(manifest);
      protected_manifest &= registry->begin_receive(manifest) == Status::OK;
      protected_manifest &= registry->declare_object(
          manifest.epoch_id, manifest.token, manifest.objects.front()) == Status::OK;
      auto replaced = manifest.objects.front();
      replaced.digest[0] ^= 1;
      protected_manifest &= registry->declare_object(
          manifest.epoch_id, manifest.token, replaced) == Status::UNSUPPORTED;
      changed = manifest;
      ++changed.source_freeze_lsn;
      protected_manifest &= reject_change(changed);
      Preserve_trx_transfer_receiver_record after;
      protected_manifest &= registry->lookup(
          manifest.epoch_id, manifest.token, &after);
      protected_manifest &= after.staged_manifest_frozen &&
          receiver_record_matches_manifest(after, manifest) &&
          before.reserved_bytes == after.reserved_bytes &&
          before.sealed_objects == after.sealed_objects &&
          before.sealed_files == after.sealed_files;
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             protected_manifest ? "PRESERVE: final manifest immutable and idempotent"
                                : "PRESERVE: final manifest changed during preparation");
    }
  });

  DBUG_EXECUTE_IF("preserve_trx_receiver_probe_bundle_memory", {
    if (!job.work) {
      const bool accounted = preserve_trx_receiver_bundle_memory_probe(
          staged_bundle.metadata.token);
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             accounted ? "PRESERVE: retained bundle memory accounted and trimmed"
                       : "PRESERVE: retained bundle memory accounting mismatch");
    }
  });

  if (!job.work) {
    if (!staged_bundle.metadata.cursor_manifest_payload.empty() &&
        preserve_trx_transfer_validate_strict_eligibility(
            manifest, staged_bundle.metadata, false, false, 1) !=
            Preserve_trx_transfer_strict_eligibility_status::OK) {
      record_token_failure(Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
          Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
          Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
    }
    if (Preserve_trx_receiver_prepare_work::begin(
            &staged_bundle, &job.work)) {
      record_token_failure(Preserve_trx_receiver_failure_reason::TOKEN_RESOURCE_LIMIT);
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
          Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
          Preserve_trx_receiver_failure_reason::TOKEN_RESOURCE_LIMIT);
    }
  }
  if (job.work->needs_result_selection()) {
    Preserve_trx_transfer_receiver_record record;
    if (!registry || !registry->lookup(manifest.epoch_id, manifest.token, &record) ||
        job.work->begin_results(record)) {
      record_token_failure(Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
          Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
          Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
    }
  }
  const auto observe_final_debt = [&] {
    const auto counts = job.work->observe_final_debt();
    for (size_t n = 0; n < counts.size(); ++n)
      if (counts[n]) note_receiver_temp_final(root_dir, manifest.epoch_id,
          static_cast<Temp_final_field>(n), counts[n]);
  };
  if (job.work->has_temp()) {
    Preserve_trx_transfer_receiver_record record;
    using Start = Preserve_trx_receiver_prepare_work::Temp_start;
    const bool found = registry &&
        registry->lookup(manifest.epoch_id, manifest.token, &record);
    DBUG_EXECUTE_IF("preserve_temp_selected_base_missing", {
      Preserved_temp_table_manifest selected;
      if (found && preserve_trx_decode_temp_table_manifest(
              job.work->bundle().metadata.temp_table_manifest_payload, &selected)) {
        for (const auto &undo : selected.undo_images) {
          if (!undo.delta.name.empty() && record.sealed_files.count(undo.delta.name) &&
              record.sealed_files.erase(undo.base.name) == 1)
            DBUG_PRINT("preserve_temp_delta_validation",
                ("temporary selected BASE removed token=%llu base=%s",
                 (ulonglong)manifest.token, undo.base.name.c_str()));
        }
      }
    });
    const auto start = found ? job.work->begin_temp(root_dir, record) : Start::ERROR;
    observe_final_debt();
    if (start == Start::WAIT) return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::CONTINUE_PREPARATION,
        Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY);
    if (start == Start::ERROR) {
      record_token_failure(Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
      return receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
          Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
          Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
    }
  }
  observe_final_debt();
  const auto temp_input_current = [&] {
    if (!job.work->has_temp()) return true;
    Preserve_trx_transfer_receiver_record record;
    return registry && registry->lookup(manifest.epoch_id, manifest.token, &record) &&
           job.work->temp_matches(record);
  };
  if (!temp_input_current()) {
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           ("PRESERVE: receiver temporary input identity changed token=" +
            staged_bundle.metadata.token).c_str());
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::EXPIRED,
        Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY);
  }
  const auto batches_before = job.work->batches();
  const bool step_failed = job.work->step(current_thd);
  if (job.work->has_temp() || job.work->has_results()) {
    note_receiver_temp_final(root_dir, manifest.epoch_id, Temp_final_field::BATCHES,
                             job.work->batches() - batches_before);
    note_receiver_temp_final(root_dir, manifest.epoch_id, Temp_final_field::PROCESSED_BYTES,
                             job.work->scanned_bytes());
    if (step_failed)
      note_receiver_temp_final(root_dir, manifest.epoch_id, Temp_final_field::FAILED_BATCHES);
  }
  if (step_failed) {
    record_token_failure(Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
        Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
  }
  throttle_receiver_prewarm_io(job.work->scanned_bytes(), runtime_policy);
  if (!job.work->complete()) return receiver_staged_token_result(
      Receiver_staged_token_prewarm_outcome::CONTINUE_PREPARATION,
      Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY);
  // Account before publishing READY: RESUME may immediately remove the token.
  job.note_resource_prepared();
  temp_activity.binding();
  auto &prepared_bundle = job.work->bundle();
  const uint64_t ready_started_us = transfer_monotonic_us();
  note_receiver_prewarm_start();
  auto finish_prewarm = create_scope_guard([] { note_receiver_prewarm_end(); });
  Preserve_trx_promotion_adopt_status prewarm_status =
      Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY;
  const bool has_record_lock_object =
      find_object(manifest, kPreservedTrxBlobRecordLocks) != nullptr;
  if (has_record_lock_object) {
    bool stale_record_lock_proof = false;
    prewarm_status =
        prewarm_receiver_ready_cache_from_object_proof(root_dir, manifest,
                                                       prepared_bundle,
                                                       &stale_record_lock_proof,
                                                       job.work->temp_ready()->get());
    if (prewarm_status ==
            Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY &&
        stale_record_lock_proof) {
      (void)enqueue_receiver_object_prewarm(
          root_dir, manifest, kPreservedTrxBlobRecordLocks, registry, true);
    }
  }
  if (prewarm_status ==
          Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY &&
      !has_record_lock_object) {
    prewarm_status = preserved_trx_promotion_prewarm_staged_bundle_for_receiver(
        root_dir, manifest.epoch_id, manifest.token,
        manifest.source_epoch_commit_lsn, prepared_bundle,
        job.work->temp_ready()->get());
  }
  if (receiver_epoch_expired_or_removed(root_dir, registry, manifest)) {
    if ((job.work->has_results() || job.work->has_temp()) && registry && registry->accepted_epoch_rejects_token(
            root_dir, manifest.epoch_id, manifest.token))
      preserved_trx_promotion_ready_cache_purge_token(
          root_dir, manifest.epoch_id, manifest.token);
    purge_receiver_epoch_derived_state_after_worker_stop(
        root_dir, manifest.epoch_id, registry);
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::EXPIRED,
        Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY);
  }
  g_receiver_staged_token_ready_cache_us.fetch_add(transfer_monotonic_us() -
                                                   ready_started_us);
  const uint delay_ms =
      receiver_staged_prewarm_delay_ms_for_unit_test().load();
  if (delay_ms != 0) my_sleep(delay_ms * 1000ULL);

  if (prewarm_status != Preserve_trx_promotion_adopt_status::OK) {
    Receiver_staged_token_prewarm_result result;
    if (prewarm_status ==
        Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY) {
      result = receiver_staged_token_result(
          has_record_lock_object
              ? Receiver_staged_token_prewarm_outcome::WAIT_DEPENDENCY
              : Receiver_staged_token_prewarm_outcome::RETRYABLE_NOT_READY,
          prewarm_status, Preserve_trx_receiver_failure_reason::NONE,
          has_record_lock_object
              ? Receiver_staged_token_prewarm_stage::
                    READY_CACHE_RECORD_LOCK_PROOF
              : Receiver_staged_token_prewarm_stage::READY_CACHE_BUNDLE);
    } else if (prewarm_status ==
                   Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT ||
               prewarm_status ==
                   Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT) {
      result = receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
          prewarm_status,
          Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
      record_token_failure(result.failure_reason);
    } else {
      result = receiver_staged_token_result(
          Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
          prewarm_status);
      note_receiver_epoch_global_failure(root_dir, manifest.epoch_id);
    }
    note_receiver_seal_prewarm_status(result.status);
    return result;
  }

  if (consume_receiver_token_retryable_not_ready_injection(
          root_dir, manifest.epoch_id)) {
    note_receiver_seal_prewarm_status(
        Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY);
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::RETRYABLE_NOT_READY,
        Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
        Preserve_trx_receiver_failure_reason::NONE,
        Receiver_staged_token_prewarm_stage::DEBUG_RETRYABLE_NOT_READY);
  }

  Preserve_trx_prepared_token_key result_key;
  if ((job.work->has_results() || job.work->has_temp()) && !strict_prepared_key_for_receiver(
          root_dir, manifest, prepared_bundle.metadata.token, &result_key)) {
    record_token_failure(Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
        Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
  }
  if (!temp_input_current())
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::EXPIRED,
        Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY);
  Receiver_staged_token_prewarm_result strict_result =
      prepare_strict_bundle_for_receiver(root_dir, manifest,
                                         std::move(prepared_bundle), registry,
                                         runtime_policy, defer_binlog_read_budget,
                                         job.work->has_results() ? job.work->ready() : nullptr,
                                         job.work->memory(),
                                         job.temp_id_contract,
                                         job.work->has_temp() ? job.work->temp_ready() : nullptr);
  if (receiver_epoch_expired_or_removed(root_dir, registry, manifest)) {
    if ((job.work->has_results() || job.work->has_temp()) && registry && registry->accepted_epoch_rejects_token(
            root_dir, manifest.epoch_id, manifest.token)) {
      (void)preserved_trx_strict_prepared_token_registry().purge_token(result_key);
      preserved_trx_promotion_ready_cache_purge_token(
          root_dir, manifest.epoch_id, manifest.token);
    }
    purge_receiver_epoch_derived_state_after_worker_stop(
        root_dir, manifest.epoch_id, registry);
    return receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::EXPIRED,
        Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY);
  }
  if (strict_result.outcome !=
      Receiver_staged_token_prewarm_outcome::READY) {
    if (strict_result.outcome ==
        Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE) {
      record_token_failure(strict_result.failure_reason);
    } else if (strict_result.outcome ==
               Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE) {
      note_receiver_epoch_global_failure(root_dir, manifest.epoch_id);
    }
    note_receiver_seal_prewarm_status(strict_result.status);
    return strict_result;
  }

  bind_strict_prepared_token_from_cached_epoch_fact(
      root_dir, manifest.epoch_id, manifest.token, registry);
  DBUG_EXECUTE_IF("preserve_trx_receiver_hold_even_token_result", {
    if (manifest.token % 2 == 0) {
      const char action[] =
          "now SIGNAL receiver_result_held "
          "WAIT_FOR receiver_result_continue TIMEOUT 60";
      DBUG_ASSERT(!debug_sync_set_action(current_thd, STRING_WITH_LEN(action)));
    }
  });
  if (consume_receiver_token_local_failure_injection(root_dir,
                                                     manifest.epoch_id)) {
    const auto result = receiver_staged_token_result(
        Receiver_staged_token_prewarm_outcome::TERMINAL_TOKEN_FAILURE,
        Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT,
        Preserve_trx_receiver_failure_reason::UNSUPPORTED_TOKEN_SEMANTICS);
    note_receiver_seal_prewarm_status(result.status);
    record_token_failure(result.failure_reason);
    return result;
  }

  note_receiver_seal_prewarm_status(Preserve_trx_promotion_adopt_status::OK);
  const bool selection_complete_with_failures =
      note_receiver_epoch_token_result(
          root_dir, manifest.epoch_id, manifest.token,
          Preserve_trx_receiver_failure_reason::NONE);
  const bool epoch_bound = publish_receiver_epoch_ready_from_fact_if_possible(
      root_dir, manifest.epoch_id, registry);
  if (epoch_bound && registry != nullptr) {
    /*
      A worker can bind the whole epoch after sibling token workers already
      finished before COMMIT_EPOCH. Finalize every sealed token here so those
      siblings do not remain RECEIVING until the periodic reaper runs.
    */
    const auto records =
        registry->sealed_receiving_records_for_epoch(manifest.epoch_id);
    for (const auto &record : records) {
      (void)finalize_receiver_ready_token_staging(
          root_dir, receiver_record_manifest(record), registry);
    }
  } else {
    (void)finalize_receiver_ready_token_staging(root_dir, manifest, registry);
  }
  if (selection_complete_with_failures) {
    (void)publish_receiver_epoch_selection_if_possible(
        root_dir, manifest.epoch_id, registry, transfer_monotonic_us(), false);
  }
  return receiver_staged_token_result(
      Receiver_staged_token_prewarm_outcome::READY,
      Preserve_trx_promotion_adopt_status::OK);
}

uint64_t receiver_residency_deadline(uint64_t now_us, uint64_t timeout_us) {
  return timeout_us > std::numeric_limits<uint64_t>::max() - now_us
             ? std::numeric_limits<uint64_t>::max()
             : now_us + timeout_us;
}

bool wait_for_receiver_record_lock_residency(
    uint64_t page_count,
    const std::function<bool(trx_preserve_record_lock_residency_t *)> &sample,
    const std::function<uint64_t()> &now,
    const std::function<bool()> &cancelled,
    const std::function<void(uint64_t)> &sleep, uint64_t timeout_us,
    trx_preserve_record_lock_residency_t *last_residency,
    size_t *sample_count) {
  if (sample_count != nullptr) *sample_count = 0;
  const uint64_t deadline_us = receiver_residency_deadline(now(), timeout_us);
  for (;;) {
    if (cancelled()) return false;
    trx_preserve_record_lock_residency_t residency;
    if (!sample(&residency)) return false;
    if (sample_count != nullptr) ++*sample_count;
    if (last_residency != nullptr) *last_residency = residency;
    if (residency.page_count == page_count &&
        residency.resident_pages == page_count &&
        residency.io_pending_pages == 0 && residency.missing_pages == 0) {
      return true;
    }
    if (cancelled()) return false;
    const uint64_t now_us = now();
    if (now_us >= deadline_us) return false;
    const uint64_t sleep_us = std::min<uint64_t>(
        deadline_us - now_us, kReceiverRecordLockResidencyPollIntervalUs);
    if (cancelled()) return false;
    sleep(sleep_us);
  }
}

bool receiver_prewarm_job_cancelled(
    Preserve_trx_transfer_receiver_registry *registry,
    const Preserve_trx_transfer_manifest &manifest) {
  {
    std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
    if (g_receiver_prewarm_shutdown) return true;
  }
  if (registry == nullptr) return false;
  Preserve_trx_transfer_receiver_state state;
  if (!registry->lookup_state(manifest.epoch_id, manifest.token, &state)) return true;
  return state != Preserve_trx_transfer_receiver_state::DECLARED &&
         state != Preserve_trx_transfer_receiver_state::RECEIVING;
}

bool receiver_epoch_expired_or_removed(
    const std::string &root_dir,
    Preserve_trx_transfer_receiver_registry *registry,
    const Preserve_trx_transfer_manifest &manifest,
    Preserve_trx_temp_id_contract *temp_id_contract) {
  if (temp_id_contract != nullptr) *temp_id_contract = {};
  if (registry == nullptr) return false;
  if (registry->accepted_epoch_is_expired(root_dir, manifest.epoch_id)) {
    return true;
  }
  Preserve_trx_transfer_receiver_state state;
  bool has_resources = false;
  if (!registry->lookup_state(manifest.epoch_id, manifest.token, &state,
                               temp_id_contract, &has_resources)) return true;
  if (has_resources && registry->accepted_epoch_rejects_token(
          root_dir, manifest.epoch_id, manifest.token)) return true;
  return state != Preserve_trx_transfer_receiver_state::DECLARED &&
         state != Preserve_trx_transfer_receiver_state::RECEIVING;
}

void purge_receiver_epoch_derived_state_after_worker_stop(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_receiver_registry *registry) {
  if (registry != nullptr &&
      registry->accepted_epoch_is_live(root_dir, epoch_id)) {
    return;
  }
  purge_receiver_epoch_derived_state(root_dir, epoch_id,
                                     receiver_boot_incarnation());
}

static bool run_receiver_object_prewarm_job(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id,
    Preserve_trx_transfer_receiver_registry *registry,
    bool retry_stale_record_lock_proof, uint object_retry_attempts,
    const Preserve_trx_transfer_runtime_policy &runtime_policy,
    const Preserve_trx_receiver_binlog_prefix_ref &binlog_prefix = nullptr) {
  const uint64_t started_us = transfer_monotonic_us();
  const uint delay_ms =
      receiver_object_prewarm_delay_ms_for_unit_test().load();
  if (delay_ms != 0) my_sleep(delay_ms * 1000ULL);
  if (receiver_epoch_expired_or_removed(root_dir, registry, manifest)) {
    return false;
  }
  Preserve_trx_transfer_manifest effective_manifest = manifest;
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(effective_manifest, object_id);
  if (registry != nullptr) {
    Preserve_trx_transfer_receiver_record current_record;
    size_t ignored_count = 0;
    if (registry->lookup_object(manifest.epoch_id, manifest.token, object_id,
          &current_record, &ignored_count, false,
          object_id == kBinlogPrewarmSeedObjectId ? kPreservedTrxBlobBinlogCache : nullptr)) {
      Preserve_trx_transfer_manifest current_manifest =
          receiver_record_manifest(current_record);
      const Preserve_trx_transfer_object_descriptor *current_object =
          find_object(current_manifest, object_id);
      if (current_object != nullptr &&
          (object == nullptr ||
           transfer_object_descriptor_equal(*object, *current_object))) {
        effective_manifest = std::move(current_manifest);
        object = find_object(effective_manifest, object_id);
      }
    }
  }
  if (object == nullptr) {
    g_receiver_object_prewarm_miss_count.fetch_add(1);
    return false;
  }
  const uint64_t job_read_bytes =
      receiver_object_prewarm_file_read_bytes(effective_manifest, object_id,
                                              true, binlog_prefix);
  Receiver_object_prewarm_key inflight_key;
  const bool have_inflight_key =
      receiver_object_prewarm_key(root_dir, effective_manifest, object_id,
                                  &inflight_key);
  auto finish_inflight = create_scope_guard([&] {
    if (!have_inflight_key) return;
    Preserve_trx_transfer_manifest deferred_manifest;
    const bool have_deferred =
        finish_receiver_object_prewarm_job(inflight_key, &deferred_manifest);
    if (receiver_epoch_expired_or_removed(root_dir, registry,
                                          effective_manifest)) {
      erase_receiver_strict_record_lock_state(
          root_dir, effective_manifest.epoch_id, effective_manifest.token);
      erase_receiver_binlog_prepared(
          root_dir, effective_manifest.epoch_id, effective_manifest.token);
      return;
    }
    if (have_deferred) {
      (void)enqueue_receiver_object_prewarm(
          root_dir, deferred_manifest, object_id, registry);
    }
  });
  if (std::max(object->total_size, job_read_bytes) >
      runtime_policy.prewarm_max_bytes) {
    preserved_trx_receiver_binlog_prefix_fail(binlog_prefix);
    if (object_id == kBinlogPrewarmSeedObjectId)
      erase_receiver_binlog_prepared(root_dir, manifest.epoch_id,
                                      manifest.token, true);
    g_receiver_object_prewarm_miss_count.fetch_add(1);
    return false;
  }
  throttle_receiver_prewarm_io(job_read_bytes, runtime_policy);
  const bool record_lock_object = object_id == kPreservedTrxBlobRecordLocks;
  const bool binlog_seed_object = object_id == kBinlogPrewarmSeedObjectId;
  const bool binlog_object =
      object_id == kPreservedTrxBlobBinlogCache || binlog_seed_object;
  auto note_elapsed = [&]() {
    const uint64_t finished_us = transfer_monotonic_us();
    note_receiver_object_prewarm_elapsed(
        started_us, finished_us >= started_us ? finished_us - started_us : 0,
        record_lock_object, binlog_object);
  };

  Receiver_object_prewarm_key key;
  key.root_dir = root_dir;
  key.epoch_id = effective_manifest.epoch_id;
  key.token = effective_manifest.token;
  key.object_id = object_id;
  key.digest = object->digest;
  key.source_live_generation = object->lock_plan.source_live_generation;
  key.source_live_digest = object->lock_plan.source_live_digest;

  const auto enqueue_staged_if_complete = [&] {
    if (registry == nullptr) return;
    if (!registry->all_objects_sealed(effective_manifest.epoch_id,
                                      effective_manifest.token, true)) return;
    Preserve_trx_transfer_receiver_record sealed_record;
    if (!registry->lookup(effective_manifest.epoch_id,
                          effective_manifest.token, &sealed_record)) {
      return;
    }
    const Preserve_trx_transfer_manifest sealed_manifest =
        receiver_record_manifest(sealed_record);
    if (transfer_manifest_has_snapshot_bundle(sealed_manifest) &&
        registry->all_objects_sealed(sealed_manifest.epoch_id,
                                     sealed_manifest.token)) {
      (void)enqueue_receiver_staged_token_prewarm(root_dir, sealed_manifest,
                                                  registry);
    }
  };

  if (binlog_seed_object) {
    const bool prepared = build_receiver_binlog_prepared_from_seed(
        root_dir, effective_manifest, *object, runtime_policy);
    if (!prepared) g_receiver_object_prewarm_miss_count.fetch_add(1);
    note_elapsed();
    enqueue_staged_if_complete();
    return false;
  }

  if (binlog_prefix != nullptr) {
    const bool built = preserved_trx_receiver_binlog_prefix_build(
        binlog_prefix, transfer_object_path(root_dir, effective_manifest, *object),
        *object);
    if (!built) g_receiver_object_prewarm_miss_count.fetch_add(1);
    note_elapsed();
    enqueue_staged_if_complete();
    return false;
  }

  Receiver_object_prewarm_proof proof;
  if (record_lock_object) {
    std::string payload;
    const Preserve_trx_transfer_status read_status =
        preserve_trx_transfer_read_sealed_object_payload(
            root_dir, effective_manifest, object_id, &payload);
    if (read_status != Preserve_trx_transfer_status::OK) {
      g_receiver_object_prewarm_miss_count.fetch_add(1);
      return false;
    }

    /*
      Build the strict metadata-only plan while the phase-1 object is already
      resident in this worker. The plan is not publishable until a final
      manifest claims the same object digest.
    */
    const bool strict_plan_ready =
        effective_manifest.source_epoch_commit_lsn != 0 &&
        (receiver_record_lock_prepared_exists(root_dir, effective_manifest) ||
         build_receiver_record_lock_prepared(root_dir, effective_manifest,
                                             payload));
    const bool strict_metadata_only =
        transfer_manifest_uses_v1_metadata_only_lock_prewarm(
            effective_manifest);
    if (receiver_object_prewarm_proof_state(key, strict_metadata_only) ==
        Receiver_object_prewarm_proof_state::READY) {
      note_elapsed();
      if (strict_plan_ready) enqueue_staged_if_complete();
      return false;
    }

    trx_preserve_record_lock_page_plan_t page_plan;
    if (!trx_preserve_record_lock_payload_page_plan(payload, &page_plan)) {
      g_receiver_object_prewarm_miss_count.fetch_add(1);
      return false;
    }
    proof.record_lock_object = true;
    proof.page_count = page_plan.page_count;
    proof.bitmap_pages = page_plan.bitmap_pages;
    proof.bitmap_bits = page_plan.bitmap_bits;
    if (strict_metadata_only) {
      if (!strict_plan_ready) {
        g_receiver_object_prewarm_miss_count.fetch_add(1);
        return false;
      }
      /*
        The strict path consumes lock identity metadata only. Reading record or
        index pages here can trigger change-buffer merge and target-local redo,
        so page residency is deliberately not part of this proof.
      */
      proof.metadata_only = true;
    } else {
      trx_preserve_record_lock_import_metrics_t prefetch_metrics;
      const dberr_t prefetch_status =
          trx_preserve_prefetch_record_lock_pages_for_gate(payload,
                                                           &prefetch_metrics);

      trx_preserve_record_lock_residency_t residency;
      bool residency_parse_failed = false;
      if (prefetch_status == DB_SUCCESS && page_plan.page_count != 0) {
        (void)wait_for_receiver_record_lock_residency(
            page_plan.page_count,
            [&](trx_preserve_record_lock_residency_t *sample) {
              if (trx_preserve_record_lock_payload_residency(payload,
                                                             sample)) {
                return true;
              }
              residency_parse_failed = true;
              return false;
            },
            [] { return transfer_monotonic_us(); },
            [&] {
              return receiver_prewarm_job_cancelled(registry, manifest);
            },
            [](uint64_t sleep_us) {
              my_sleep(static_cast<ulong>(sleep_us));
            },
            static_cast<uint64_t>(preserve_trx_promotion_gate_timeout_ms) *
                1000,
            &residency, nullptr);
      } else if (!trx_preserve_record_lock_payload_residency(payload,
                                                             &residency)) {
        residency_parse_failed = true;
      }
      if (residency_parse_failed) {
        g_receiver_object_prewarm_miss_count.fetch_add(1);
        return false;
      }

      proof.resident_pages =
          prefetch_status == DB_SUCCESS ? residency.resident_pages : 0;
      proof.cold_gets = residency.page_count > residency.resident_pages
                            ? residency.page_count - residency.resident_pages
                            : 0;
      if (prefetch_status != DB_SUCCESS && proof.page_count != 0) {
        proof.cold_gets = proof.page_count;
      }
    }
  }

  bool proof_published = false;
  bool retry_after_finish = false;
  if (receiver_epoch_expired_or_removed(root_dir, registry,
                                        effective_manifest)) {
    return false;
  }
  {
    std::lock_guard<std::mutex> guard(g_receiver_object_prewarm_proof_mutex);
    auto existing = g_receiver_object_prewarm_proofs.find(key);
    if (existing != g_receiver_object_prewarm_proofs.end()) {
      if (record_lock_object && proof.metadata_only &&
          !existing->second.metadata_only) {
        existing->second = proof;
        proof_published = true;
      } else if (record_lock_object &&
          !receiver_record_lock_proof_gate_ready(existing->second) &&
          receiver_record_lock_proof_is_improvement(proof,
                                                    existing->second)) {
        existing->second = proof;
        proof_published = true;
      } else if (record_lock_object && retry_stale_record_lock_proof &&
                 !receiver_record_lock_proof_gate_ready(existing->second) &&
                 object_retry_attempts <
                     kReceiverRecordLockObjectProofRetryLimit) {
        retry_after_finish = true;
      } else {
        note_elapsed();
        return retry_after_finish;
      }
    } else {
      g_receiver_object_prewarm_proofs.emplace(std::move(key), proof);
      proof_published = true;
      g_receiver_object_prewarm_proof_count.fetch_add(1);
    }
    if (!proof_published) {
      note_elapsed();
      return retry_after_finish;
    }
  }
  if (receiver_epoch_expired_or_removed(root_dir, registry,
                                        effective_manifest)) {
    purge_receiver_epoch_derived_state_after_worker_stop(
        root_dir, effective_manifest.epoch_id, registry);
    return false;
  }
  note_elapsed();
  enqueue_staged_if_complete();
  return retry_after_finish;
}

// A resource object continues on the same bounded pool and keeps its inflight
// key between batches. It is not a record-lock retry and consumes no retry
// count. Final result preparation yields until this exact candidate is complete.
static bool run_receiver_resource_prewarm_job(Receiver_prewarm_job &job) {
  // Enqueue constructed this key before publishing inflight ownership. No
  // allocation may precede the cleanup guard on a continuation.
  const auto &key = job.object_key;
  bool more = false;
  const auto finish = create_scope_guard([&] {
    if (!more) {
      Preserve_trx_transfer_manifest ignored;
      (void)finish_receiver_object_prewarm_job(key, &ignored);
    }
  });
  if (receiver_epoch_expired_or_removed(job.root_dir, job.registry,
                                        job.manifest)) return false;
  Receiver_temp_activity_guard temp_activity(job.root_dir, job.manifest.epoch_id);
  const auto *object = find_object(job.manifest, job.object_id);
  if (!object || object->total_size > job.runtime_policy.prewarm_max_bytes)
    return false;
  job.temp_undo = transfer_is_temp_undo_object(*object);
  job.temp_candidate = preserve_trx_temp_candidate_object(*object);
  Preserve_trx_transfer_receiver_record candidate_record;
  if (job.temp_candidate && (!job.registry || !job.registry->lookup(
          job.manifest.epoch_id, job.manifest.token, &candidate_record))) return false;
  if (!job.result_candidates || job.temp_undo || job.temp_candidate) {
    Preserve_trx_transfer_receiver_record record;
    size_t ignored_count = 0;
    if (!job.registry || !job.registry->lookup_object(job.manifest.epoch_id,
          job.manifest.token, job.object_id, &record, &ignored_count, true) ||
        !record.resource_candidates) return false;
    const auto file = record.sealed_files.find(job.object_id);
    // Every undo continuation checks the current generation. An old pinned
    // inode is safe to read but must stop consuming budget after supersession.
    if (file == record.sealed_files.end() || !file->second ||
        !file->second->matches(object->total_size, object->digest)) return false;
    job.result_candidates = record.resource_candidates;
    job.result_file = file->second;
  }
  if (!job.result_file ||
      !job.result_file->matches(object->total_size, object->digest)) return false;
  bool complete = false;
  uint64_t scanned = 0;
  const bool failed = job.temp_candidate
      ? job.result_candidates->step_temp(job.root_dir, job.object_id, candidate_record,
          current_thd, 8ULL * 1024 * 1024, job.runtime_policy.prewarm_max_bytes,
          &complete, &scanned)
      : job.temp_undo
      ? job.result_candidates->step_undo(job.object_id, job.result_file, 128,
                                        &complete, &scanned)
      : job.result_candidates->step_result(job.object_id, job.result_file,
          current_thd, 4096, 8ULL * 1024 * 1024, &complete, &scanned);
  throttle_receiver_prewarm_io(scanned, job.runtime_policy);
  note_receiver_temp_final(job.root_dir, job.manifest.epoch_id, Temp_final_field::BATCHES);
  note_receiver_temp_final(job.root_dir, job.manifest.epoch_id,
                           Temp_final_field::PROCESSED_BYTES, scanned);
  if (failed) note_receiver_temp_final(job.root_dir, job.manifest.epoch_id,
                                       Temp_final_field::FAILED_BATCHES);
  if (failed || receiver_epoch_expired_or_removed(job.root_dir, job.registry,
                                                  job.manifest)) return false;
  DBUG_EXECUTE_IF("preserve_cursor_early_abandon_after_batch", {
    if (!job.temp_undo && !job.temp_candidate && !complete)
      throw std::bad_alloc();
  });
  DBUG_EXECUTE_IF("preserve_temp_undo_early_abandon_after_batch", {
    if (job.temp_undo && !complete) throw std::bad_alloc();
  });
  more = !complete;
  if (complete) {
    note_receiver_temp_final(job.root_dir, job.manifest.epoch_id,
                             Temp_final_field::COMPLETED_CANDIDATES);
    std::lock_guard<std::mutex> guard(g_receiver_object_prewarm_proof_mutex);
    if (g_receiver_object_prewarm_proofs.emplace(
            key, Receiver_object_prewarm_proof{}).second)
      ++g_receiver_object_prewarm_proof_count;
  }
  return more;
}

#ifndef NDEBUG
bool preserve_trx_transfer_receiver_residency_wait_for_unit_test(
    uint64_t page_count, const std::vector<uint64_t> &resident_page_samples,
    const std::vector<uint64_t> &monotonic_time_samples,
    size_t cancel_after_samples, uint64_t timeout_us, size_t *sample_count) {
  size_t resident_index = 0;
  size_t time_index = 0;
  return wait_for_receiver_record_lock_residency(
      page_count,
      [&](trx_preserve_record_lock_residency_t *residency) {
        if (resident_index >= resident_page_samples.size()) return false;
        *residency = {};
        residency->page_count = page_count;
        residency->resident_pages = resident_page_samples[resident_index++];
        return true;
      },
      [&] {
        if (monotonic_time_samples.empty()) return uint64_t{0};
        const size_t index =
            std::min(time_index++, monotonic_time_samples.size() - 1);
        return monotonic_time_samples[index];
      },
      [&] { return resident_index >= cancel_after_samples; },
      [](uint64_t) {}, timeout_us, nullptr, sample_count);
}
#endif

void receiver_prewarm_worker_main() {
  const uint init_index =
      g_receiver_prewarm_worker_init_index.fetch_add(1);
  const int fail_init_at =
      g_receiver_prewarm_fail_init_at_for_unit_test.load();
  bool init_failed =
      fail_init_at == -2 || fail_init_at == static_cast<int>(init_index) ||
      my_thread_init();
  THD *worker_thd = nullptr;
  if (!init_failed) {
    worker_thd = create_thd(false, true, true, 0);
    init_failed = worker_thd == nullptr;
  }
  {
    std::unique_lock<std::mutex> guard(g_receiver_prewarm_mutex);
    g_receiver_prewarm_cv.wait(guard, [] {
      return !g_receiver_prewarm_pause_init_report_for_unit_test;
    });
    ++g_receiver_prewarm_worker_init_reports;
    if (init_failed) ++g_receiver_prewarm_worker_init_failures;
  }
  g_receiver_prewarm_cv.notify_all();
  if (init_failed) {
    if (worker_thd != nullptr) destroy_thd(worker_thd);
    return;
  }
  auto thread_end_guard = create_scope_guard([] { my_thread_end(); });
  auto thd_guard = create_scope_guard([&] { destroy_thd(worker_thd); });
  uint64_t active_work_us = 0;
  for (;;) {
    // Cleanup owns no receiver registry or epoch; admission/pause cannot strand it.
    Preserve_trx_temp_receiver_work::reap_once();
    std::list<Receiver_prewarm_job> active_job;
    const Receiver_prewarm_epoch_key *active_epoch = nullptr;
    const Receiver_strict_token_key *active_binlog = nullptr;
    {
      std::unique_lock<std::mutex> guard(g_receiver_prewarm_mutex);
      for (;;) {
        if (g_receiver_prewarm_shutdown || g_receiver_prewarm_runtime_stop)
          return;

        if (!g_receiver_prewarm_shutdown && !g_receiver_prewarm_runtime_stop &&
            g_receiver_prewarm_paused.load(std::memory_order_acquire)) {
          const uint64_t pause_started_us = transfer_monotonic_us();
          g_receiver_prewarm_cv.wait(guard, [] {
            return g_receiver_prewarm_shutdown ||
                   g_receiver_prewarm_runtime_stop ||
                   !g_receiver_prewarm_paused.load(std::memory_order_acquire);
          });
          const uint64_t pause_finished_us = transfer_monotonic_us();
          g_transfer_last_throttle_reason.store(static_cast<uint64_t>(
              Transfer_throttle_reason::PREWARM_PAUSED));
          if (pause_finished_us >= pause_started_us) {
            g_transfer_throttled_us.fetch_add(pause_finished_us -
                                              pause_started_us);
          }
          continue;
        }

        auto runnable_staged =
            std::find_if(g_receiver_staged_token_prewarm_jobs.begin(),
                         g_receiver_staged_token_prewarm_jobs.end(),
                         [](const Receiver_prewarm_job &candidate) {
                           return receiver_prewarm_job_has_capacity_locked(
                                      candidate) &&
                                  receiver_binlog_job_runnable_locked(candidate) &&
                                  receiver_staged_token_prewarm_job_runnable(
                                      candidate);
                         });
        auto runnable_object =
            std::find_if(g_receiver_prewarm_jobs.begin(),
                         g_receiver_prewarm_jobs.end(),
                         [](const Receiver_prewarm_job &candidate) {
                           return receiver_prewarm_job_has_capacity_locked(candidate) &&
                                  receiver_binlog_job_runnable_locked(candidate);
                         });
        if (runnable_object != g_receiver_prewarm_jobs.end() &&
            (g_receiver_prefer_object_after_prepare_batch ||
             runnable_staged == g_receiver_staged_token_prewarm_jobs.end())) {
          active_job.splice(active_job.end(), g_receiver_prewarm_jobs,
                            runnable_object);
          subtract_receiver_queued_bytes(active_job.front().estimated_io_bytes);
          g_receiver_prefer_object_after_prepare_batch = false;
          break;
        }
        if (runnable_staged != g_receiver_staged_token_prewarm_jobs.end()) {
          active_job.splice(active_job.end(), g_receiver_staged_token_prewarm_jobs,
                            runnable_staged);
          subtract_receiver_queued_bytes(active_job.front().estimated_io_bytes);
          break;
        }
        if (g_receiver_prewarm_shutdown || g_receiver_prewarm_runtime_stop) {
          return;
        }
        g_receiver_prewarm_cv.wait(guard);
      }
      const auto &job = active_job.front();
      const auto active = g_receiver_prewarm_active_by_epoch.emplace(
          receiver_prewarm_epoch_key(job), 0).first;
      ++active->second;
      active_epoch = &active->first;
      if (receiver_job_uses_binlog_prefix(job)) {
        const auto binlog = g_receiver_binlog_active_tokens.insert(
            receiver_strict_token_key(job.root_dir, job.manifest)).first;
        active_binlog = &*binlog;
      }
    }

    auto &job = active_job.front();
    bool accepted_epoch_live = false;
    auto finish_registry_job = create_scope_guard([&] {
      {
        std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
        if (active_binlog) g_receiver_binlog_active_tokens.erase(*active_binlog);
        const auto active =
            g_receiver_prewarm_active_by_epoch.find(*active_epoch);
        if (active != g_receiver_prewarm_active_by_epoch.end()) {
          if (--active->second == 0) {
            g_receiver_prewarm_active_by_epoch.erase(active);
          }
        }
      }
      g_receiver_prewarm_cv.notify_all();
      if (accepted_epoch_live) {
        request_receiver_prewarm_idle_stop();
      }
    });
    g_receiver_worker_active.fetch_add(1);
    auto finish_worker_active = create_scope_guard(
        [] { g_receiver_worker_active.fetch_sub(1); });
    const uint64_t job_started_us = transfer_monotonic_us();
    Receiver_staged_token_prewarm_result staged_result =
        receiver_staged_token_result(
            Receiver_staged_token_prewarm_outcome::READY,
            Preserve_trx_promotion_adopt_status::OK);
    bool retry_object = false, continue_result = false;
    try {
      if (job.kind == Receiver_prewarm_job_kind::STAGED_TOKEN) {
        staged_result =
            run_receiver_staged_token_prewarm_job(job);
      } else {
        const auto *object = find_object(job.manifest, job.object_id);
        if (object && (object->kind == Preserve_trx_transfer_object_kind::CURSOR_RESULT ||
                       transfer_is_temp_undo_object(*object) ||
                       preserve_trx_temp_candidate_object(*object)))
          continue_result = run_receiver_resource_prewarm_job(job);
        else
          retry_object = run_receiver_object_prewarm_job(
              job.root_dir, job.manifest, job.object_id, job.registry,
              job.retry_stale_record_lock_proof, job.object_retry_attempts,
              job.runtime_policy, job.binlog_prefix);
        g_receiver_prewarm_cv.notify_all();
      }
    } catch (...) {
      if (job.kind == Receiver_prewarm_job_kind::STAGED_TOKEN) {
        staged_result = receiver_staged_token_result(
            Receiver_staged_token_prewarm_outcome::GLOBAL_FAILURE,
            Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
        note_receiver_epoch_global_failure(job.root_dir,
                                           job.manifest.epoch_id);
        note_receiver_seal_prewarm_status(
            Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
      } else {
        g_receiver_object_prewarm_miss_count.fetch_add(1);
      }
    }
    // Capacity pins the registry and immutable map keys through all finishing
    // operations, even after the unique candidate moves into the next batch.
    accepted_epoch_live = job.registry != nullptr &&
        job.registry->query_accepted_epoch(job.root_dir, job.manifest.epoch_id) ==
            Preserve_trx_transfer_status::COMMITTED_NOT_READY;
    const auto yield_us = job.runtime_policy.worker_yield_us;
    try {
      if (job.kind == Receiver_prewarm_job_kind::STAGED_TOKEN) {
        const bool expired_or_removed = receiver_epoch_expired_or_removed(
            job.root_dir, job.registry, job.manifest);
        log_receiver_staged_token_prewarm_deferred(job, staged_result);
        finish_receiver_staged_token_prewarm_job(
            &active_job, staged_result.outcome, expired_or_removed);
      } else if (continue_result) {
        const bool expired = receiver_epoch_expired_or_removed(
            job.root_dir, job.registry, job.manifest);
        const auto &key = job.object_key;
        const bool have_key = !key.object_id.empty();
        std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
        if (!expired && have_key && g_receiver_object_prewarm_inflight.count(key) &&
            !g_receiver_prewarm_shutdown && !g_receiver_prewarm_runtime_stop &&
            !g_receiver_prewarm_retiring_registries.count(job.registry)) {
          g_receiver_queued_bytes.fetch_add(job.estimated_io_bytes);
          g_receiver_prewarm_jobs.splice(g_receiver_prewarm_jobs.end(), active_job);
        } else if (have_key) {
          g_receiver_object_prewarm_inflight.erase(key);
          g_receiver_object_prewarm_deferred.erase(key);
        }
      } else if (retry_object && !receiver_epoch_expired_or_removed(
                                    job.root_dir, job.registry, job.manifest)) {
        job.retry_stale_record_lock_proof = true;
        ++job.object_retry_attempts;
        (void)enqueue_receiver_prewarm_job(std::move(job));
      }
    } catch (...) {
      // Both finishing paths transfer ownership only after allocation succeeds.
      note_receiver_epoch_global_failure(job.root_dir, job.manifest.epoch_id);
      purge_receiver_epoch_prewarm_queues(job.root_dir, job.manifest.epoch_id);
      note_receiver_seal_prewarm_status(
          Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
    }
    finish_worker_active.rollback();
    const uint64_t job_finished_us = transfer_monotonic_us();
    const uint64_t job_elapsed_us = std::max<uint64_t>(
        1, job_finished_us >= job_started_us
               ? job_finished_us - job_started_us
               : 1);
    if (receiver_prewarm_work_batch_should_yield(
            job_elapsed_us,
            yield_us,
            &active_work_us)) {
      preserve_trx_transfer_worker_yield(yield_us);
    }
  }
}

Preserve_trx_transfer_status ensure_receiver_prewarm_workers_locked(
    std::unique_lock<std::mutex> *lock, uint worker_count) {
  if (lock == nullptr || !lock->owns_lock()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (g_receiver_prewarm_shutdown) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  while (g_receiver_prewarm_workers_starting ||
         g_receiver_prewarm_workers_stopping) {
    g_receiver_prewarm_cv.wait(*lock);
    if (g_receiver_prewarm_shutdown) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
  }
  if (g_receiver_prewarm_workers_started) return Preserve_trx_transfer_status::OK;
  worker_count = std::max<uint>(1, worker_count);
  g_receiver_prewarm_workers_starting = true;
  g_receiver_prewarm_worker_init_reports = 0;
  g_receiver_prewarm_worker_init_failures = 0;
  g_receiver_prewarm_worker_init_index.store(0);

  std::vector<std::thread> workers;
  auto join_workers = create_scope_guard([&] {
    for (std::thread &worker : workers) {
      if (worker.joinable()) worker.join();
    }
  });
  bool create_failed = false;
  try {
    workers.reserve(worker_count);
    for (uint worker_index = 0; worker_index < worker_count; ++worker_index) {
      if (g_receiver_prewarm_fail_create_at_for_unit_test.load() ==
          static_cast<int>(worker_index)) {
        throw std::system_error(
            std::make_error_code(std::errc::resource_unavailable_try_again));
      }
      workers.emplace_back(receiver_prewarm_worker_main);
    }
  } catch (...) {
    create_failed = true;
  }

  if (!create_failed) {
    g_receiver_prewarm_cv.wait(*lock, [&] {
      return g_receiver_prewarm_worker_init_reports == workers.size();
    });
  }

  if (create_failed || g_receiver_prewarm_worker_init_failures != 0) {
    g_receiver_prewarm_shutdown = true;
    lock->unlock();
    g_receiver_prewarm_cv.notify_all();
    join_workers.rollback();
    lock->lock();
    g_receiver_prewarm_shutdown = false;
    g_receiver_prewarm_workers_starting = false;
    g_receiver_prewarm_worker_init_reports = 0;
    g_receiver_prewarm_worker_init_failures = 0;
    g_receiver_prewarm_cv.notify_all();
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }

  g_receiver_prewarm_workers.swap(workers);
  join_workers.commit();
  g_receiver_worker_count.store(worker_count);
  g_receiver_prewarm_workers_started = true;
  g_receiver_prewarm_workers_starting = false;
  g_receiver_prewarm_cv.notify_all();
  return Preserve_trx_transfer_status::OK;
}

void preserve_trx_transfer_set_temporary_worker_create_failure_for_unit_test(
    int fail_at_worker_index) {
  g_temporary_worker_fail_create_at_for_unit_test.store(fail_at_worker_index);
}

void preserve_trx_transfer_set_epoch_bind_bad_alloc_for_unit_test(
    bool enabled) {
  g_receiver_epoch_bind_bad_alloc_for_unit_test.store(enabled);
}

bool preserve_trx_transfer_epoch_binding_for_unit_test(
    const std::string &root_dir, const std::string &epoch_id) {
  std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
  const auto found = g_receiver_ready_epoch_state.find({root_dir, epoch_id});
  return found != g_receiver_ready_epoch_state.end() &&
         found->second.binding;
}

bool preserve_trx_transfer_epoch_bound_for_unit_test(
    const std::string &root_dir, const std::string &epoch_id) {
  std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
  const auto found = g_receiver_ready_epoch_state.find({root_dir, epoch_id});
  return found != g_receiver_ready_epoch_state.end() && found->second.bound;
}

Preserve_trx_transfer_status
preserve_trx_transfer_start_receiver_workers_for_unit_test(
    uint worker_count, int fail_create_at_worker_index,
    int fail_init_at_worker_index) {
  g_receiver_prewarm_fail_create_at_for_unit_test.store(
      fail_create_at_worker_index);
  g_receiver_prewarm_fail_init_at_for_unit_test.store(
      fail_init_at_worker_index);
  std::unique_lock<std::mutex> lock(g_receiver_prewarm_mutex);
  const Preserve_trx_transfer_status status =
      ensure_receiver_prewarm_workers_locked(&lock, worker_count);
  g_receiver_prewarm_fail_create_at_for_unit_test.store(-1);
  g_receiver_prewarm_fail_init_at_for_unit_test.store(-1);
  return status;
}

bool preserve_trx_transfer_receiver_workers_started_for_unit_test() {
  std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
  return g_receiver_prewarm_workers_started;
}

void preserve_trx_transfer_set_receiver_worker_init_pause_for_unit_test(
    bool pause) {
  {
    std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
    g_receiver_prewarm_pause_init_report_for_unit_test = pause;
  }
  g_receiver_prewarm_cv.notify_all();
}

void preserve_trx_transfer_set_prewarm_paused_for_unit_test(bool paused) {
  preserve_trx_transfer_set_prewarm_paused(paused);
}

bool preserve_trx_transfer_receiver_workers_starting_for_unit_test() {
  std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
  return g_receiver_prewarm_workers_starting;
}

#ifndef NDEBUG
void preserve_trx_transfer_set_before_final_fact_bind_hook_for_unit_test(
    Preserve_trx_transfer_before_final_fact_bind_hook hook) {
  g_before_final_fact_bind_hook_for_unit_test.store(hook);
}

void preserve_trx_transfer_set_after_epoch_fact_cache_hook_for_unit_test(
    Preserve_trx_transfer_before_final_fact_bind_hook hook) {
  g_after_epoch_fact_cache_hook_for_unit_test.store(hook);
}

size_t preserve_trx_transfer_receiver_active_jobs_for_unit_test(
    const Preserve_trx_transfer_receiver_registry *registry) {
  std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
  size_t active = 0;
  for (const auto &entry : g_receiver_prewarm_active_by_epoch) {
    if (registry == nullptr || entry.first.first == registry) {
      active += entry.second;
    }
  }
  return active;
}

Preserve_trx_transfer_status
preserve_trx_transfer_enqueue_blocked_staged_prewarm_for_unit_test(
    const std::string &root_dir, uint64_t token) {
  Preserve_trx_transfer_manifest manifest;
  manifest.protocol_version = kPreserveTrxTransferProtocolVersion;
  manifest.epoch_id = "unit-blocked-staged-prewarm";
  manifest.token = token;

  Preserve_trx_transfer_object_descriptor object;
  object.object_id = kPreservedTrxBlobRecordLocks;
  object.kind = Preserve_trx_transfer_object_kind::EXTERNAL_BLOB;
  object.total_size = 1;
  object.digest[0] = 1;
  manifest.objects.push_back(std::move(object));
  return enqueue_receiver_staged_token_prewarm(root_dir, manifest, nullptr);
}
#endif

Preserve_trx_transfer_status enqueue_receiver_prewarm_job(
    Receiver_prewarm_job &&job) {
  if (job.root_dir.empty()) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  if (!job.runtime_policy_snapshotted) {
    uint64_t ignored_max_inflight_bytes = 0;
    if (job.registry == nullptr ||
        !job.registry->online_epoch_runtime_policy(
            job.manifest.epoch_id, &job.runtime_policy,
            &ignored_max_inflight_bytes)) {
      job.runtime_policy = preserve_trx_transfer_current_runtime_policy();
    }
    job.runtime_policy_snapshotted = true;
  }
  if (job.kind == Receiver_prewarm_job_kind::OBJECT &&
      job.object_id == kPreservedTrxBlobBinlogCache &&
      job.runtime_policy.profile == PRESERVE_TRX_TRANSFER_RUNTIME_PROMOTION_PREPARE) {
    const auto *binlog = find_object(job.manifest, job.object_id);
    if (binlog != nullptr)
      job.binlog_prefix = preserved_trx_receiver_binlog_prefix_seal(
          job.root_dir, job.manifest, *binlog);
  }
  auto &object_key = job.object_key;
  const bool has_object_key =
      job.kind == Receiver_prewarm_job_kind::OBJECT &&
      receiver_object_prewarm_key(job.root_dir, job.manifest, job.object_id,
                                  &object_key);
  if (job.kind == Receiver_prewarm_job_kind::OBJECT && !has_object_key) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (job.kind == Receiver_prewarm_job_kind::OBJECT) {
    const auto *object = find_object(job.manifest, job.object_id);
    if (object == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    job.estimated_io_bytes = object->total_size;
  } else {
    job.estimated_io_bytes = receiver_manifest_object_bytes(job.manifest);
  }
  if (receiver_epoch_expired_or_removed(job.root_dir, job.registry,
                                        job.manifest)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (job.kind == Receiver_prewarm_job_kind::STAGED_TOKEN && job.registry
#ifndef NDEBUG
      && !job.temp_probe
#endif
  ) {
    // Fix the final resource identity before queue deduplication or bundle IO.
    // Phase1 object jobs remain free to follow the current prewarm manifest.
    const auto status = job.registry->freeze_staged_manifest(job.manifest);
    if (status != Preserve_trx_transfer_status::OK) return status;
    job.objects_already_sealed = true;
  }
  const bool record_plan_missing =
      has_object_key && job.object_id == kPreservedTrxBlobRecordLocks &&
      job.manifest.source_epoch_commit_lsn != 0 &&
      !receiver_record_lock_prepared_exists(job.root_dir, job.manifest);
  const bool strict_metadata_only =
      has_object_key && job.object_id == kPreservedTrxBlobRecordLocks &&
      transfer_manifest_uses_v1_metadata_only_lock_prewarm(job.manifest);
  {
    std::unique_lock<std::mutex> guard(g_receiver_prewarm_mutex);
    if (job.registry != nullptr &&
        g_receiver_prewarm_retiring_registries.count(job.registry) != 0) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    const Preserve_trx_transfer_status start_status =
        ensure_receiver_prewarm_workers_locked(
            &guard, kReceiverPrewarmPoolMaxWorkers);
    if (start_status != Preserve_trx_transfer_status::OK) return start_status;
    if (job.kind == Receiver_prewarm_job_kind::STAGED_TOKEN) {
      Receiver_staged_token_prewarm_key key =
          receiver_staged_token_prewarm_key(job.root_dir, job.manifest);
      if (g_receiver_staged_token_prewarm_done.count(key) != 0) {
        return Preserve_trx_transfer_status::OK;
      }
      if (g_receiver_staged_token_prewarm_inflight.count(key) != 0) {
        g_receiver_staged_token_prewarm_deferred.insert(std::move(key));
        return Preserve_trx_transfer_status::OK;
      }
      {
        // Serialize admission with queue purge: a late object completion must
        // not requeue a classified token whose prepared plan was consumed.
        std::lock_guard<std::mutex> ready_guard(g_receiver_ready_epoch_mutex);
        const auto epoch = g_receiver_ready_epoch_state.find(
            {job.root_dir, job.manifest.epoch_id});
        if (epoch != g_receiver_ready_epoch_state.end() &&
            (epoch->second.selection_published ||
             epoch->second.token_results.count(job.manifest.token) != 0)) {
          return Preserve_trx_transfer_status::OK;
        }
      }
      g_receiver_staged_token_prewarm_inflight.insert(std::move(key));
      const auto estimated_io_bytes = job.estimated_io_bytes;
      job.enqueued_us = transfer_monotonic_us();
      g_receiver_staged_token_prewarm_jobs.push_back(std::move(job));
      g_receiver_queued_bytes.fetch_add(estimated_io_bytes);
      /*
        Some pool threads can be parked by the active profile's worker limit.
        Wake the whole bounded pool so an eligible worker cannot miss the job.
      */
      g_receiver_prewarm_cv.notify_all();
      return Preserve_trx_transfer_status::OK;
    }
    if (has_object_key) {
      const auto attempted =
          g_receiver_record_plan_attempted_generation.find(object_key);
      const bool needs_record_plan =
          record_plan_missing &&
          (attempted == g_receiver_record_plan_attempted_generation.end() ||
           attempted->second < job.manifest.source_epoch_commit_lsn);
      if (g_receiver_object_prewarm_inflight.count(object_key) != 0) {
        if (needs_record_plan || strict_metadata_only || job.binlog_prefix != nullptr ||
            job.object_id == kBinlogPrewarmSeedObjectId) {
          auto &deferred = g_receiver_object_prewarm_deferred[object_key];
          if (deferred.source_epoch_commit_lsn <=
              job.manifest.source_epoch_commit_lsn) {
            deferred = job.manifest;
          }
        }
        return Preserve_trx_transfer_status::OK;
      }
      const Receiver_object_prewarm_proof_state proof_state =
          receiver_object_prewarm_proof_state(object_key,
                                              strict_metadata_only);
      if ((proof_state == Receiver_object_prewarm_proof_state::READY &&
           !needs_record_plan) ||
          (proof_state == Receiver_object_prewarm_proof_state::STALE &&
           !job.retry_stale_record_lock_proof && !strict_metadata_only)) {
        return Preserve_trx_transfer_status::OK;
      }
      if (needs_record_plan) {
        g_receiver_record_plan_attempted_generation[object_key] =
            job.manifest.source_epoch_commit_lsn;
      }
      g_receiver_object_prewarm_inflight.insert(object_key);
    }
    const auto estimated_io_bytes = job.estimated_io_bytes;
    g_receiver_prewarm_jobs.push_back(std::move(job));
    g_receiver_queued_bytes.fetch_add(estimated_io_bytes);
  }
  g_receiver_prewarm_cv.notify_all();
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status enqueue_receiver_staged_token_prewarm(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    Preserve_trx_transfer_receiver_registry *registry) {
  Receiver_prewarm_job job;
  job.kind = Receiver_prewarm_job_kind::STAGED_TOKEN;
  job.root_dir = root_dir;
  job.manifest = manifest;
  job.registry = registry;
  job.objects_already_sealed =
      registry != nullptr &&
      registry->all_objects_sealed(manifest.epoch_id, manifest.token);
  return enqueue_receiver_prewarm_job(std::move(job));
}

#ifndef NDEBUG
bool preserve_trx_receiver_temp_probe(
    const std::string &root,
    std::unique_ptr<Preserve_trx_temp_transfer_input> *input) {
  if (!input || !*input || !current_thd) return true;
  const auto retired_before = Preserve_trx_temp_receiver_work::retired_owners();
  static std::atomic<uint64_t> sequence{0};
  Receiver_prewarm_job job;
  job.root_dir = root;
  job.manifest.epoch_id = "temp-receiver-probe-" + std::to_string(++sequence);
  job.manifest.token = 1;
  job.runtime_policy = preserve_trx_transfer_current_runtime_policy();
  job.runtime_policy.prewarm_workers = 1;
  job.runtime_policy_snapshotted = true;
  Preserved_trx_bundle bundle;
  bundle.metadata.token = (*input)->token();
  bundle.metadata.temp_table_manifest_payload = (*input)->manifest_payload();
  if (Preserve_trx_receiver_prepare_work::begin(&bundle, &job.work) ||
      Preserve_trx_temp_receiver_work::begin_import(root, input,
          job.work->temp_ready()) != DB_SUCCESS) return true;
  auto result = std::make_shared<Receiver_temp_probe_result>();
  job.temp_probe = std::make_unique<Receiver_temp_probe_completion>();
  job.temp_probe->result = result;
  job.temp_probe->source_thread = current_thd->thread_id();
  bool cancel_after_write = false;
  DBUG_EXECUTE_IF("preserve_temp_receiver_pipeline_cancel", cancel_after_write = true;);
  job.temp_probe->cancel_after_temp_write = cancel_after_write;
  DBUG_EXECUTE_IF("preserve_temp_receiver_sql_attach_probe", {
    job.temp_probe->temp_sql_attach = true;
  });
  Preserve_trx_transfer_receiver_registry registry;
  job.registry = &registry;
  const auto epoch = job.manifest.epoch_id;
  auto cleanup = create_scope_guard([&] { purge_receiver_epoch_prewarm_queues(root, epoch); });
  if (registry.declare_token(epoch, 1) != Preserve_trx_transfer_status::OK ||
      enqueue_receiver_prewarm_job(std::move(job)) != Preserve_trx_transfer_status::OK)
    return true;
  std::unique_lock<std::mutex> lock(result->mutex);
  if (!result->cv.wait_for(lock, std::chrono::seconds(30), [&] { return result->done; }))
    return true;
  if (cancel_after_write) {
    const auto directory = result->cancelled_directory;
    const auto batches = result->batches;
    if (directory.empty() || result->temp || batches < 2) return true;
    lock.unlock();
    for (unsigned n = 0; n < 200 && (access(directory.c_str(), F_OK) == 0 ||
         Preserve_trx_temp_receiver_work::retired_owners() != retired_before); ++n)
      my_sleep(50000);
    if (access(directory.c_str(), F_OK) == 0 ||
        Preserve_trx_temp_receiver_work::retired_owners() != retired_before) return true;
    DBUG_PRINT("preserve_temp_import",
               ("temporary receiver pipeline cancelled after target writes batches=%llu",
                static_cast<unsigned long long>(batches)));
    return false;
  }
  if (!result->temp || !result->temp->ready()) return true;
  auto work = std::move(result->temp);
  const auto batches = result->batches;
  lock.unlock();
  const auto tables = work->input()->manifest()->tables.size();
  if (work->sql_table_count() != tables) return true;
  DBUG_EXECUTE_IF("preserve_temp_receiver_sql_metadata_probe", {
    DBUG_PRINT("preserve_temp_import",
               ("temporary receiver SQL metadata ready tables=%zu", tables));
  });
  const auto undo_records = work->plan()->source_undo_record_count();
  const auto directory = work->directory();
  Preserve_trx_prepared_token_key key;
  key.preserve_dir = root;
  key.epoch_scope = "temp-owner-probe";
  key.epoch_id = epoch;
  key.token = work->input()->token();
  key.target_boot_incarnation = "probe";
  key.generation = 1;
  Preserve_trx_prepared_token_resources resources;
  const auto semantic_manifest = work->input()->manifest_payload();
  const auto make_semantic = [&] {
    auto semantic = std::make_unique<Preserved_trx_bundle>();
    semantic->metadata.token = key.token;
    semantic->metadata.temp_table_manifest_payload = semantic_manifest;
    return semantic;
  };
  const auto contract = work->input()->contract();
  using Status = Preserve_trx_prepared_status;
  Preserve_trx_prepared_token_registry prepared_registry;
  Preserve_trx_prepare_lease prepare;
  const std::string object_digest(64, 'a');
  if (prepared_registry.begin_prepare(key, 1, &prepare) != Status::OK ||
      preserved_trx_acquire_prepared_token_resources(key, 0, 0, 0, 0, &resources) != Status::OK ||
      resources.install_semantic_bundle(make_semantic(), {}, contract) != Status::OK ||
      resources.temp_resources_ready() ||
      prepared_registry.publish_prewarmed(&prepare, object_digest, std::move(resources)) !=
          Status::INVALID_ARGUMENT) return true;
  if (preserved_trx_acquire_prepared_token_resources(key, 0, 0, 0, 0, &resources) != Status::OK ||
      resources.install_semantic_bundle(make_semantic(), {}, contract) != Status::OK ||
      resources.temp_resources_ready() || resources.install_temp_ready(&work) != Status::OK ||
      work || !resources.temp_resources_ready()) return true;
  Preserve_trx_prepared_token_snapshot snapshot;
  if (prepared_registry.publish_prewarmed(&prepare, object_digest, std::move(resources)) != Status::OK ||
      prepared_registry.snapshot(key, &snapshot) != Status::OK ||
      !snapshot.temp_resources_ready ||
      snapshot.state != Preserve_trx_prepared_token_state::PREWARMED_PENDING_FINAL_FACT)
    return true;
  // Retirement must work without a valid token/registry and after the worker
  // has exited. The ordinary reaper, not this probe, performs cancellation.
  prepared_registry.purge_epoch(key.epoch_scope, key.epoch_id);
  if (prepared_registry.snapshot(key, &snapshot) != Status::NOT_FOUND) return true;
  for (unsigned n = 0; n < 200 && (access(directory.c_str(), F_OK) == 0 ||
       Preserve_trx_temp_receiver_work::retired_owners() != retired_before); ++n)
    my_sleep(50000);
  if (access(directory.c_str(), F_OK) == 0 ||
      Preserve_trx_temp_receiver_work::retired_owners() != retired_before) return true;
  DBUG_PRINT("preserve_temp_import",
             ("temporary receiver pipeline checked tables=%zu undo_records=%zu "
              "batches=%llu cancelled=1", tables, undo_records,
              static_cast<unsigned long long>(batches)));
  return false;
}


#endif

Preserve_trx_transfer_status enqueue_receiver_object_prewarm(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id,
    Preserve_trx_transfer_receiver_registry *registry,
    bool retry_stale_record_lock_proof) {
  if (object_id.empty()) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  const bool binlog_seed_object = object_id == kBinlogPrewarmSeedObjectId;
  if (binlog_seed_object) {
    const auto *seed = find_object(manifest, object_id);
    if (seed == nullptr ||
        !queue_receiver_binlog_prepared(root_dir, manifest, *seed)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    if (registry != nullptr) {
      Preserve_trx_transfer_receiver_record record;
      size_t ignored_count = 0;
      if (!registry->lookup_object(manifest.epoch_id, manifest.token,
            kPreservedTrxBlobBinlogCache, &record, &ignored_count, true)) {
        erase_receiver_binlog_prepared(root_dir, manifest.epoch_id,
                                       manifest.token);
        return Preserve_trx_transfer_status::CORRUPT;
      }
      const Preserve_trx_transfer_manifest current_manifest =
          receiver_record_manifest(record);
      if (find_object(current_manifest, kPreservedTrxBlobBinlogCache) ==
              nullptr ||
          record.sealed_objects.count(kPreservedTrxBlobBinlogCache) == 0) {
        /*
          The seed describes the final binlog object, but phase-1 can publish it
          before the large payload has arrived. Keep the token-local prepare
          state queued; the binlog SEAL_OBJECT path enqueues this seed again.
        */
        return Preserve_trx_transfer_status::OK;
      }
    }
  }
  Receiver_prewarm_job job;
  job.kind = Receiver_prewarm_job_kind::OBJECT;
  job.root_dir = root_dir;
  job.manifest.protocol_version = manifest.protocol_version;
  job.manifest.epoch_id = manifest.epoch_id;
  job.manifest.token = manifest.token;
  job.manifest.source_freeze_lsn = manifest.source_freeze_lsn;
  job.manifest.source_epoch_commit_lsn = manifest.source_epoch_commit_lsn;
  job.manifest.strict_eligibility_flags = manifest.strict_eligibility_flags;
  if (const auto *object = find_object(manifest, object_id))
    job.manifest.objects.push_back(*object);
  if (binlog_seed_object) {
    if (const auto *payload = find_object(manifest, kPreservedTrxBlobBinlogCache))
      job.manifest.objects.push_back(*payload);
  }
  job.object_id = object_id;
  job.registry = registry;
  job.retry_stale_record_lock_proof = retry_stale_record_lock_proof;
  job.object_retry_attempts = 0;
  const Preserve_trx_transfer_status status =
      enqueue_receiver_prewarm_job(std::move(job));
  if (status != Preserve_trx_transfer_status::OK && binlog_seed_object) {
    erase_receiver_binlog_prepared(root_dir, manifest.epoch_id,
                                   manifest.token);
  }
  return status;
}

void preserve_trx_transfer_shutdown_receiver_prewarm_workers() {
  std::list<Receiver_prewarm_job> retired;
  std::vector<std::thread> workers;
  {
    std::unique_lock<std::mutex> guard(g_receiver_prewarm_mutex);
    g_receiver_prewarm_cv.wait(guard, [] {
      return !g_receiver_prewarm_workers_starting &&
             !g_receiver_prewarm_workers_stopping;
    });
    g_receiver_prewarm_workers_stopping = true;
    g_receiver_prewarm_shutdown = true;
    g_receiver_prewarm_idle_stop_requested = false;
    workers.swap(g_receiver_prewarm_workers);
    g_receiver_prewarm_workers_started = false;
    g_receiver_worker_count.store(0);
  }
  g_receiver_prewarm_cv.notify_all();
  for (std::thread &worker : workers) {
    if (worker.joinable()) worker.join();
  }
  preserved_trx_receiver_binlog_prefix_clear();
  {
    std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
    retired.splice(retired.end(), g_receiver_staged_token_prewarm_jobs);
    retired.splice(retired.end(), g_receiver_prewarm_jobs);
    g_receiver_staged_token_prewarm_inflight.clear();
    g_receiver_staged_token_prewarm_done.clear();
    g_receiver_staged_token_prewarm_deferred.clear();
    g_receiver_object_prewarm_inflight.clear();
    g_receiver_object_prewarm_deferred.clear();
    g_receiver_record_plan_attempted_generation.clear();
    g_receiver_prewarm_active_by_epoch.clear();
    g_receiver_prewarm_retiring_registries.clear();
    g_receiver_queued_bytes.store(0);
    g_receiver_worker_active.store(0);
    g_receiver_prewarm_shutdown = false;
    g_receiver_prewarm_runtime_stop = false;
    g_receiver_prefer_object_after_prepare_batch = false;
    g_receiver_prewarm_idle_stop_requested = false;
    g_receiver_prewarm_workers_stopping = false;
  }
  g_receiver_prewarm_cv.notify_all();
  decltype(g_receiver_ready_epoch_state) retired_timing;
  {
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    retired_timing.swap(g_receiver_ready_epoch_state);
  }
  for (auto &entry : retired_timing)
    preserve_trx_temp_final_log(entry.first.second, entry.second.temp_final.finish(
        transfer_monotonic_us(), Preserve_trx_temp_final_outcome::CANCELLED));
  {
    std::lock_guard<std::mutex> guard(g_receiver_object_prewarm_proof_mutex);
    g_receiver_object_prewarm_proofs.clear();
  }
  {
    std::lock_guard<std::mutex> guard(g_receiver_record_lock_prepared_mutex);
    g_receiver_record_lock_prepared.clear();
  }
  {
    std::lock_guard<std::mutex> guard(
        g_receiver_strict_record_lock_facts_mutex);
    g_receiver_strict_record_lock_facts.clear();
  }
  {
    std::lock_guard<std::mutex> guard(g_receiver_strict_binlog_facts_mutex);
    g_receiver_strict_binlog_facts.clear();
  }
  {
    std::lock_guard<std::mutex> guard(g_receiver_binlog_prepared_mutex);
    g_receiver_binlog_prepared.clear();
  }
  (void)preserved_trx_strict_prepared_token_registry()
      .discard_all_for_process_shutdown();
  retired.clear();
  default_receiver_registry().discard_records_for_process_shutdown();
}

void retire_receiver_prewarm_registry(
    Preserve_trx_transfer_receiver_registry *registry) {
  if (registry == nullptr) return;
  std::list<Receiver_prewarm_job> retired;
  std::unique_lock<std::mutex> guard(g_receiver_prewarm_mutex);
  g_receiver_prewarm_retiring_registries.insert(registry);

  auto cancel_jobs = [&](std::list<Receiver_prewarm_job> *jobs) {
    for (auto job = jobs->begin(); job != jobs->end();) {
      if (job->registry != registry) {
        ++job;
        continue;
      }
      subtract_receiver_queued_bytes(job->estimated_io_bytes);
      if (job->kind == Receiver_prewarm_job_kind::STAGED_TOKEN) {
        const Receiver_staged_token_prewarm_key key =
            receiver_staged_token_prewarm_key(job->root_dir, job->manifest);
        g_receiver_staged_token_prewarm_inflight.erase(key);
        g_receiver_staged_token_prewarm_deferred.erase(key);
      } else if (job->kind == Receiver_prewarm_job_kind::OBJECT) {
        Receiver_object_prewarm_key key;
        if (receiver_object_prewarm_key(job->root_dir, job->manifest,
                                        job->object_id, &key)) {
          g_receiver_object_prewarm_inflight.erase(key);
          g_receiver_object_prewarm_deferred.erase(key);
          g_receiver_record_plan_attempted_generation.erase(key);
        }
      }
      const auto removed = job++;
      retired.splice(retired.end(), *jobs, removed);
    }
  };
  cancel_jobs(&g_receiver_staged_token_prewarm_jobs);
  cancel_jobs(&g_receiver_prewarm_jobs);
  g_receiver_prewarm_cv.notify_all();
  g_receiver_prewarm_cv.wait(guard, [&] {
    return std::none_of(
        g_receiver_prewarm_active_by_epoch.begin(),
        g_receiver_prewarm_active_by_epoch.end(),
        [registry](const auto &entry) { return entry.first.first == registry; });
  });
  g_receiver_prewarm_retiring_registries.erase(registry);
  guard.unlock();
  request_receiver_prewarm_idle_stop();
}

Preserve_trx_transfer_receiver_registry::
    ~Preserve_trx_transfer_receiver_registry() {
  retire_receiver_prewarm_registry(this);
  uint64_t terminal_tombstones = 0;
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    terminal_tombstones =
        std::count_if(m_acknowledged_epochs.begin(),
                      m_acknowledged_epochs.end(), [](const auto &item) {
                        return item.second.terminal_operation !=
                               Preserve_trx_transfer_epoch_terminal_operation::
                                   NONE;
                      });
  }
  if (terminal_tombstones != 0) {
    g_receiver_terminal_status_tombstones.fetch_sub(terminal_tombstones);
  }
}

Preserve_trx_transfer_status mark_epoch_records_corrupt(
    Preserve_trx_transfer_receiver_registry *registry,
    const std::vector<Preserve_trx_transfer_receiver_record> &records,
    const std::string &reason) {
  Preserve_trx_transfer_status first_status = Preserve_trx_transfer_status::OK;
  for (const Preserve_trx_transfer_receiver_record &record : records) {
    Preserve_trx_transfer_receiver_record current;
    if (registry->lookup(record.epoch_id, record.token, &current) &&
        current.state ==
            Preserve_trx_transfer_receiver_state::CLEANUP_PENDING) {
      continue;
    }
    const Preserve_trx_transfer_status status =
        registry->mark_corrupt(record.epoch_id, record.token, reason);
    if (status != Preserve_trx_transfer_status::OK &&
        first_status == Preserve_trx_transfer_status::OK) {
      first_status = status;
    }
  }
  return first_status;
}

Preserve_trx_transfer_status cleanup_epoch_transfer_staging(
    const std::string &root_dir,
    const std::vector<Preserve_trx_transfer_receiver_record> &records,
    Preserve_trx_transfer_receiver_registry *registry) {
  Preserve_trx_transfer_status first_status = Preserve_trx_transfer_status::OK;
  for (const Preserve_trx_transfer_receiver_record &record : records) {
    erase_receiver_strict_record_lock_state(root_dir, record.epoch_id,
                                            record.token);
    erase_receiver_binlog_prepared(root_dir, record.epoch_id, record.token);
    purge_strict_prepared_token_for_receiver(root_dir, record);
    if (registry != nullptr) {
      registry->erase_strict_v1_token_objects(record.epoch_id, record.token);
    }
    const Preserve_trx_transfer_status cleanup_status =
        cleanup_transfer_token_staging(root_dir, record.epoch_id, record.token);
    if (cleanup_status == Preserve_trx_transfer_status::OK) {
      if (registry != nullptr) {
        Preserve_trx_transfer_receiver_state state;
        if (registry->lookup_state(record.epoch_id, record.token, &state) &&
            state != Preserve_trx_transfer_receiver_state::DECLARED &&
            state != Preserve_trx_transfer_receiver_state::RECEIVING) {
          const auto released = registry->complete_staging_cleanup(
              record.epoch_id, record.token);
          if (first_status == Preserve_trx_transfer_status::OK)
            first_status = released;
        }
        // ABANDON keeps live admission until complete_epoch_abandon atomically
        // retires the epoch. Deleting its directory alone is not that fence.
      }
      continue;
    }
    const Preserve_trx_transfer_status debt_status =
        registry == nullptr
            ? Preserve_trx_transfer_status::INVALID_ARGUMENT
            : registry->mark_cleanup_pending(
                  root_dir, record.epoch_id, record.token,
                  transfer_monotonic_us(),
                  Preserve_trx_transfer_receiver_state::CORRUPT,
                  "epoch_staging_cleanup_failed:" +
                      transfer_status_name(cleanup_status));
    if (first_status == Preserve_trx_transfer_status::OK) {
      first_status = debt_status == Preserve_trx_transfer_status::OK
                         ? cleanup_status
                         : debt_status;
    }
  }
  return first_status;
}

Preserve_trx_transfer_epoch_terminal_outcome
preserve_trx_transfer_abandon_receiver_epoch_if_not_committed(
    const Preserve_trx_transfer_epoch_terminal_request &request,
    Preserve_trx_transfer_receiver_registry *registry) {
  if (registry == nullptr) {
    return Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
  }
  const Preserve_trx_transfer_epoch_terminal_outcome begin =
      registry->try_begin_epoch_abandon(request);
  if (begin !=
      Preserve_trx_transfer_epoch_terminal_outcome::ABANDONING) {
    return begin;
  }

  const std::vector<Preserve_trx_transfer_receiver_record> records =
      registry->receiving_records_for_epoch(request.epoch_id);
  purge_receiver_epoch_derived_state(
      request.root_dir, request.epoch_id, receiver_boot_incarnation());
  if (cleanup_epoch_transfer_staging(request.root_dir, records, registry) !=
          Preserve_trx_transfer_status::OK ||
      remove_receiver_restart_tree(
          transfer_epoch_dir_for_epoch(request.root_dir, request.epoch_id),
          0) != Preserve_trx_transfer_status::OK) {
    return Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT;
  }
  return registry->complete_epoch_abandon(request);
}

Preserve_trx_transfer_status mark_epoch_records_corrupt_and_purge(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_receiver_registry *registry,
    const std::vector<Preserve_trx_transfer_receiver_record> &records,
    const std::string &reason) {
  const Preserve_trx_transfer_status status =
      mark_epoch_records_corrupt(registry, records, reason);
  purge_receiver_epoch_derived_state(root_dir, epoch_id,
                                     receiver_boot_incarnation());
  return status;
}

Preserve_trx_transfer_status preserve_trx_transfer_commit_epoch(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_store *store) {
  return commit_epoch_manifests(root_dir, {manifest}, manifest, store);
}

bool preserve_trx_transfer_epoch_committed(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest) {
  if (root_dir.empty() ||
      validate_manifest_components(manifest, false) !=
          Preserve_trx_transfer_status::OK) {
    return false;
  }
  bool committed = false;
  return read_commit_marker(root_dir, manifest, &committed) ==
             Preserve_trx_transfer_status::OK &&
         committed;
}

bool preserve_trx_transfer_epoch_committed(const std::string &root_dir,
                                           const std::string &epoch_id) {
  if (root_dir.empty() || !transfer_component_safe(epoch_id)) return false;
  const std::string commit_path =
      join_path(join_path(join_path(root_dir, ".transfer"), epoch_id),
                "epoch.commit");
  if (!file_exists(commit_path)) return false;

  std::string payload;
  if (read_whole_file(commit_path, &payload) !=
      Preserve_trx_transfer_status::OK) {
    return false;
  }
  if (payload != "PTRXFER_COMMIT_V1\n" + epoch_id + "\n") return false;

  Preserve_trx_transfer_epoch_fact fact;
  return preserve_trx_transfer_read_epoch_fact(root_dir, epoch_id, &fact) ==
             Preserve_trx_transfer_status::OK &&
         fact.epoch_id == epoch_id && !fact.tokens.empty();
}

Preserve_trx_transfer_status preserve_trx_transfer_query_epoch_commit_status(
    const std::string &root_dir, const std::string &epoch_id) {
  return preserve_trx_transfer_query_epoch_commit_status(
      root_dir, epoch_id, &default_receiver_registry(), nullptr);
}

Preserve_trx_transfer_status preserve_trx_transfer_query_epoch_commit_status(
    const std::string &root_dir, const std::string &epoch_id,
    const Preserve_trx_transfer_receiver_registry *registry,
    Preserve_trx_transfer_accepted_epoch *accepted) {
  if (registry == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const Preserve_trx_transfer_epoch_terminal_outcome terminal =
      registry->query_epoch_terminal(root_dir, epoch_id);
  switch (terminal) {
    case Preserve_trx_transfer_epoch_terminal_outcome::COMMITTED:
      if (accepted != nullptr) {
        (void)registry->query_accepted_epoch(root_dir, epoch_id, accepted);
      }
      return Preserve_trx_transfer_status::COMMITTED_NOT_READY;
    case Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT:
      return Preserve_trx_transfer_status::COMMITTED_CORRUPT;
    case Preserve_trx_transfer_epoch_terminal_outcome::NOT_COMMITTED:
    case Preserve_trx_transfer_epoch_terminal_outcome::ABANDONING:
    case Preserve_trx_transfer_epoch_terminal_outcome::NOT_COMMITTED_CLEAN:
    case Preserve_trx_transfer_epoch_terminal_outcome::EPOCH_NOT_FOUND:
      return Preserve_trx_transfer_status::IO_ERROR;
  }
  return Preserve_trx_transfer_status::IO_ERROR;
}

Preserve_trx_transfer_status preserve_trx_transfer_read_epoch_fact(
    const std::string &root_dir, const std::string &epoch_id,
    Preserve_trx_transfer_epoch_fact *fact) {
  if (fact == nullptr || root_dir.empty() || !transfer_component_safe(epoch_id))
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  std::string payload;
  const Preserve_trx_transfer_status read_status =
      read_whole_file(transfer_epoch_fact_path(root_dir, epoch_id), &payload);
  if (read_status != Preserve_trx_transfer_status::OK) return read_status;
  return preserve_trx_transfer_decode_epoch_fact(payload, fact);
}

static Preserve_trx_transfer_status
preserve_trx_transfer_apply_receiver_frame_internal(
    const std::string &root_dir, const Preserve_trx_transfer_frame &frame,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata,
    Preserve_trx_transfer_commit_accepted_callback commit_accepted,
    void *commit_accepted_context) {
  (void)timeout_seconds;
  (void)written_metadata;
  if (store == nullptr || registry == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (root_dir.empty()) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  if (!preserve_trx_is_enabled()) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  Preserve_trx_transfer_status status =
      validate_frame_components(frame, false);
  if (status != Preserve_trx_transfer_status::OK) return status;

  // Promotion is target-local HA control. A transfer data session must never
  // prewarm or adopt transactions on behalf of the source.
  if (frame.type ==
          Preserve_trx_transfer_frame_type::PROMOTION_PREWARM_TOKEN ||
      frame.type == Preserve_trx_transfer_frame_type::PROMOTION_GATE_EPOCH) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (frame.type == Preserve_trx_transfer_frame_type::OPEN_EPOCH) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  const bool sequence_frame =
      receiver_frame_is_sequence_tracked(frame.type);
  const bool commit_frame =
      frame.type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH;
  const bool control_only_commit = commit_frame && frame.token == 0;
  std::vector<uint64_t> session_only_tokens;
  const bool has_session_only_tokens =
      commit_frame && !frame.manifest_payload.empty();
  if (has_session_only_tokens) {
    status = preserve_trx_transfer_decode_session_only_tokens(
        frame.manifest_payload, &session_only_tokens);
    if (status != Preserve_trx_transfer_status::OK) return status;
  }
  std::array<unsigned char, kPreservedTrxSha256Length> commit_frame_digest{};
  if (commit_frame) {
    std::string encoded_commit;
    status = preserve_trx_transfer_encode_frame(frame, &encoded_commit);
    if (status != Preserve_trx_transfer_status::OK) return status;
    commit_frame_digest = sha256_digest(encoded_commit);
  }
  if (sequence_frame && !g_receiver_frame_sequence_disabled) {
    const std::string *terminal_root_dir = commit_frame ? &root_dir : nullptr;
    const auto *terminal_fact_digest =
        commit_frame ? &frame.terminal_fact_digest : nullptr;
    status = registry->consume_frame_sequence(
        frame.epoch_id, frame.sequence, frame.type, commit_frame_digest,
        terminal_root_dir, terminal_fact_digest,
        commit_frame ? transfer_monotonic_us() : 0);
    if (status != Preserve_trx_transfer_status::OK) return status;
    note_receiver_first_frame_saved();
  }

  if (frame.type == Preserve_trx_transfer_frame_type::QUERY_EPOCH_STATUS) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  if (frame.type == Preserve_trx_transfer_frame_type::DECLARE_TOKEN) {
    return registry->declare_token(frame.epoch_id, frame.token);
  }

  if (frame.type == Preserve_trx_transfer_frame_type::DECLARE_OBJECT) {
    Preserve_trx_transfer_object_descriptor descriptor;
    status = decode_transfer_object_descriptor(frame.manifest_payload,
                                               &descriptor);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (descriptor.object_id != frame.object_id) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    uint64_t append_prefix_size = 0;
    std::array<unsigned char, kPreservedTrxSha256Length>
        append_prefix_digest{};
    const bool append_sealed_binlog_prefix =
        decode_binlog_append_prefix_reason(
            frame.reason, &append_prefix_size, &append_prefix_digest);
    if (append_sealed_binlog_prefix &&
        (descriptor.object_id != kPreservedTrxBlobBinlogCache ||
         descriptor.kind !=
             Preserve_trx_transfer_object_kind::EXTERNAL_BLOB ||
         descriptor.total_size <= append_prefix_size)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    Preserve_trx_transfer_manifest existing_manifest;
    Preserve_trx_transfer_object_descriptor replaced_object;
    bool cleanup_replaced_object = false;
    Preserve_trx_transfer_receiver_record existing_record;
    size_t ignored_count = 0;
    if (registry->lookup_object(frame.epoch_id, frame.token,
          descriptor.object_id, &existing_record, &ignored_count, true) &&
        (existing_record.state == Preserve_trx_transfer_receiver_state::DECLARED ||
         existing_record.state == Preserve_trx_transfer_receiver_state::RECEIVING)) {
      try {
        existing_manifest = receiver_record_manifest(existing_record);
        const Preserve_trx_transfer_object_descriptor *existing_object =
            find_object(existing_manifest, descriptor.object_id);
        if (existing_object != nullptr &&
            !transfer_object_descriptor_equal(*existing_object, descriptor)) {
          replaced_object = *existing_object;
          cleanup_replaced_object = true;
        }
      } catch (...) {
        return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
      }
    }
    if (append_sealed_binlog_prefix &&
        (!cleanup_replaced_object ||
         existing_record.protocol_version !=
             kPreserveTrxTransferProtocolVersion ||
         replaced_object.object_id != kPreservedTrxBlobBinlogCache ||
         replaced_object.kind !=
             Preserve_trx_transfer_object_kind::EXTERNAL_BLOB ||
         replaced_object.total_size != append_prefix_size ||
         replaced_object.digest != append_prefix_digest ||
         existing_record.sealed_objects.count(replaced_object.object_id) ==
             0)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    uint64_t staging_retirement = 0;
    status = registry->declare_object(frame.epoch_id, frame.token, descriptor,
                                       &staging_retirement);
    if (status != Preserve_trx_transfer_status::OK) return status;
    const auto declare_native_prefix = [&] {
      if (descriptor.object_id != kPreservedTrxBlobBinlogCache ||
          !preserve_trx_is_enabled()) return;
      auto policy = preserve_trx_transfer_current_runtime_policy();
      uint64_t ignored_inflight = 0;
      (void)registry->online_epoch_runtime_policy(
          frame.epoch_id, &policy, &ignored_inflight);
      if (policy.profile == PRESERVE_TRX_TRANSFER_RUNTIME_PROMOTION_PREPARE)
        preserved_trx_receiver_binlog_prefix_declare(
            root_dir, frame.epoch_id, frame.token, descriptor,
            append_sealed_binlog_prefix ? &replaced_object : nullptr);
    };
    if (!cleanup_replaced_object) {
      declare_native_prefix();
      return status;
    }
    if (descriptor.object_id == kPreservedTrxBlobBinlogCache) {
      erase_receiver_binlog_prepared(root_dir, frame.epoch_id, frame.token,
                                      append_sealed_binlog_prefix);
    }
    if (append_sealed_binlog_prefix) {
      declare_native_prefix();
      return Preserve_trx_transfer_status::OK;
    }
    if (descriptor.object_id == kPreservedTrxBlobRecordLocks) {
      purge_strict_prepared_token_for_receiver(root_dir, existing_record);
      erase_receiver_record_lock_object_generation(
          root_dir, frame.epoch_id, frame.token);
    }
    status = cleanup_transfer_object_staging(root_dir, existing_manifest,
                                             replaced_object);
    if (status == Preserve_trx_transfer_status::OK) {
      registry->mark_staging_object_deleted(staging_retirement);
      declare_native_prefix();
      return status;
    }
    const Preserve_trx_transfer_status mark_status = registry->mark_cleanup_pending(
        root_dir, frame.epoch_id, frame.token, transfer_monotonic_us(),
        Preserve_trx_transfer_receiver_state::CORRUPT,
        "replacement_object_cleanup_failed:" + transfer_status_name(status),
        existing_record.reserved_bytes - existing_record.resource_reserved_bytes);
    purge_receiver_epoch_derived_state(
        root_dir, frame.epoch_id, receiver_boot_incarnation());
    erase_receiver_strict_record_lock_state(root_dir, frame.epoch_id, frame.token);
    erase_receiver_binlog_prepared(root_dir, frame.epoch_id, frame.token);
    purge_strict_prepared_token_for_receiver(root_dir, existing_record);
    return mark_status == Preserve_trx_transfer_status::OK ? status
                                                           : mark_status;
  }

  if (frame.type == Preserve_trx_transfer_frame_type::BEGIN) {
    Preserve_trx_transfer_manifest manifest;
    status = preserve_trx_transfer_decode_manifest(frame.manifest_payload,
                                                   &manifest);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (manifest.epoch_id != frame.epoch_id || manifest.token != frame.token) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    status = preserve_trx_transfer_validate_receiver_manifest_from_config(
        manifest);
    if (status != Preserve_trx_transfer_status::OK) return status;
    uint64_t inflight_bytes = 0;
    status = transfer_manifest_inflight_bytes(
        manifest, frame.manifest_payload.length(), &inflight_bytes);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (inflight_bytes > preserve_trx_transfer_max_inflight_bytes) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    Preserve_trx_transfer_manifest existing_manifest;
    std::vector<Preserve_trx_transfer_object_descriptor> objects_to_cleanup;
    Preserve_trx_transfer_receiver_record existing_record;
    if (registry->lookup(frame.epoch_id, frame.token, &existing_record) &&
        (existing_record.state == Preserve_trx_transfer_receiver_state::DECLARED ||
         existing_record.state == Preserve_trx_transfer_receiver_state::RECEIVING)) {
      try {
        existing_manifest = receiver_record_manifest(existing_record);
        if (!existing_record.objects.empty() && !existing_record.object_index)
          return Preserve_trx_transfer_status::CORRUPT;
        std::vector<unsigned char> retained(existing_record.objects.size(), 0);
        for (const auto &selected : manifest.objects) {
          const auto *previous = existing_record.object_index
              ? existing_record.object_index->find(existing_record.objects,
                                                   selected.object_id)
              : nullptr;
          if (previous && transfer_object_descriptor_equal(*previous, selected))
            retained[previous - existing_record.objects.data()] = 1;
        }
        objects_to_cleanup.reserve(existing_manifest.objects.size());
        for (size_t n = 0; n < existing_manifest.objects.size(); ++n)
          if (!retained[n])
            objects_to_cleanup.push_back(existing_manifest.objects[n]);
      } catch (...) {
        return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
      }
    }
    std::vector<Preserve_trx_transfer_receiver_registry::Staging_retirement> retirements;
    bool first_resource_final = false;
    status = registry->begin_receive(manifest, frame.manifest_payload.length(), &retirements,
                                     &first_resource_final);
    if (status == Preserve_trx_transfer_status::OK) {
      if (first_resource_final) {
        try {
          Preserve_trx_temp_final_counts counts{};
          counts[static_cast<size_t>(Temp_final_field::TOKENS)] = 1;
          for (const auto &object : manifest.objects) {
            if (!transfer_is_resource_object(object)) continue;
            ++counts[static_cast<size_t>(Temp_final_field::INPUT_OBJECTS)];
            const auto file = existing_record.sealed_files.find(object.object_id);
            if (file == existing_record.sealed_files.end() || !file->second ||
                !file->second->matches(object.total_size, object.digest)) {
              ++counts[static_cast<size_t>(Temp_final_field::PENDING_OBJECTS)];
              counts[static_cast<size_t>(Temp_final_field::PENDING_BYTES)] += object.total_size;
            }
          }
          std::lock_guard<std::mutex> lock(g_receiver_ready_epoch_mutex);
          g_receiver_ready_epoch_state[{root_dir, manifest.epoch_id}].temp_final.begin(
              transfer_monotonic_us(), counts);
        } catch (...) { preserve_trx_temp_final_observation_dropped(); }
      }
      size_t retirement = 0;
      for (const Preserve_trx_transfer_object_descriptor &existing_object :
           objects_to_cleanup) {
        /*
          The phase-1 binlog seed is not part of the final manifest, but an
          already queued object worker still owns it as input. Token-level
          staging cleanup removes it after READY, ABORT, CORRUPT, or expiry.
        */
        if (existing_object.object_id == kBinlogPrewarmSeedObjectId) continue;
        if (existing_object.object_id == kPreservedTrxBlobRecordLocks) {
          purge_strict_prepared_token_for_receiver(root_dir, existing_record);
          erase_receiver_record_lock_object_generation(
              root_dir, frame.epoch_id, frame.token);
        }
        const Preserve_trx_transfer_status cleanup_status =
            cleanup_transfer_object_staging(root_dir, existing_manifest,
                                            existing_object);
        if (cleanup_status != Preserve_trx_transfer_status::OK) {
          const Preserve_trx_transfer_status mark_status =
              registry->mark_cleanup_pending(
                  root_dir, frame.epoch_id, frame.token, transfer_monotonic_us(),
                  Preserve_trx_transfer_receiver_state::CORRUPT,
                  "replacement_begin_cleanup_failed:" +
                      transfer_status_name(cleanup_status),
                  existing_record.reserved_bytes - existing_record.resource_reserved_bytes);
          purge_receiver_epoch_derived_state(
              root_dir, frame.epoch_id, receiver_boot_incarnation());
          erase_receiver_strict_record_lock_state(root_dir, frame.epoch_id, frame.token);
          erase_receiver_binlog_prepared(root_dir, frame.epoch_id, frame.token);
          purge_strict_prepared_token_for_receiver(root_dir, existing_record);
          return mark_status == Preserve_trx_transfer_status::OK
                     ? cleanup_status
                     : mark_status;
        }
        if (retirement < retirements.size() &&
            retirements[retirement].object_id == existing_object.object_id) {
          registry->mark_staging_object_deleted(retirements[retirement].id);
          ++retirement;
        }
      }
      Preserve_trx_transfer_receiver_record record_after_begin;
      if (registry->lookup(frame.epoch_id, frame.token, &record_after_begin)) {
        const Preserve_trx_transfer_manifest begin_manifest =
            receiver_record_manifest(record_after_begin);
        for (const std::string &sealed_object :
             record_after_begin.sealed_objects) {
          status = enqueue_receiver_object_prewarm(root_dir, begin_manifest,
                                                   sealed_object, registry);
          if (status != Preserve_trx_transfer_status::OK) return status;
        }
      }
    }
    if (status == Preserve_trx_transfer_status::OK &&
        transfer_manifest_has_snapshot_bundle(manifest) &&
        registry->all_objects_sealed(frame.epoch_id, frame.token)) {
      status = enqueue_receiver_staged_token_prewarm(root_dir, manifest,
                                                     registry);
    }
    return status;
  }

  if (frame.type == Preserve_trx_transfer_frame_type::ABORT) {
    Preserve_trx_transfer_receiver_record record;
    if (!registry->lookup(frame.epoch_id, frame.token, &record)) {
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }
    if (record.state != Preserve_trx_transfer_receiver_state::DECLARED &&
        record.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    status = registry->mark_aborted(frame.epoch_id, frame.token, frame.reason);
    if (status != Preserve_trx_transfer_status::OK) return status;
    preserved_trx_promotion_ready_cache_purge_token(root_dir, frame.epoch_id,
                                                    frame.token);
    erase_receiver_strict_record_lock_state(root_dir, frame.epoch_id,
                                            frame.token);
    erase_receiver_binlog_prepared(root_dir, frame.epoch_id, frame.token);
    purge_strict_prepared_token_for_receiver(root_dir, record);
    registry->erase_strict_v1_token_objects(frame.epoch_id, frame.token);
    status =
        cleanup_transfer_token_staging(root_dir, frame.epoch_id, frame.token);
    if (status != Preserve_trx_transfer_status::OK) {
      return registry->mark_cleanup_pending(
          root_dir, frame.epoch_id, frame.token, transfer_monotonic_us(),
          Preserve_trx_transfer_receiver_state::ABORTED,
          "abort_cleanup_failed:" + transfer_status_name(status));
    }
    return registry->complete_staging_cleanup(frame.epoch_id, frame.token);
  }

  auto fail_commit_epoch = [&](Preserve_trx_transfer_status failure_status,
                               const char *reason) {
    registry->mark_epoch_commit_admission_corrupt(
        frame.epoch_id, frame.sequence, commit_frame_digest);
    const std::vector<Preserve_trx_transfer_receiver_record> records =
        registry->receiving_records_for_epoch(frame.epoch_id);
    const Preserve_trx_transfer_status mark_status =
        mark_epoch_records_corrupt_and_purge(root_dir, frame.epoch_id, registry,
                                             records, reason);
    const Preserve_trx_transfer_status cleanup_status =
        cleanup_epoch_transfer_staging(root_dir, records, registry);
    if (mark_status != Preserve_trx_transfer_status::OK) return mark_status;
    if (cleanup_status != Preserve_trx_transfer_status::OK) {
      return cleanup_status;
    }
    return failure_status;
  };

  if (control_only_commit) {
    std::vector<Preserve_trx_transfer_receiver_record> commit_records;
    status = registry->snapshot_epoch_for_commit(
        frame.epoch_id, frame.sequence, commit_frame_digest, &commit_records,
        true);
    if (status == Preserve_trx_transfer_status::COMMITTED_NOT_READY) {
      if (commit_accepted == nullptr) {
        return Preserve_trx_transfer_status::OK;
      }
      try {
        return commit_accepted(
            commit_accepted_context, frame.epoch_id,
            Preserve_trx_transfer_status::COMMITTED_NOT_READY);
      } catch (...) {
        return Preserve_trx_transfer_status::ACK_UNCERTAIN;
      }
    }
    if (status == Preserve_trx_transfer_status::RESOURCE_EXHAUSTED) {
      return status;
    }
    if (status != Preserve_trx_transfer_status::OK) {
      return fail_commit_epoch(status, "control_only_commit_snapshot_invalid");
    }
    if (!has_session_only_tokens || !commit_records.empty() ||
        frame.terminal_fact_digest !=
            session_only_control_fact_digest(frame)) {
      return fail_commit_epoch(Preserve_trx_transfer_status::CORRUPT,
                               "control_only_commit_fact_invalid");
    }
    status = registry->publish_control_only_epoch(
        root_dir, frame.epoch_id, frame.sequence, commit_frame_digest,
        frame.terminal_fact_digest, transfer_monotonic_us(),
        session_only_tokens);
    if (status == Preserve_trx_transfer_status::RESOURCE_EXHAUSTED) {
      return status;
    }
    if (status != Preserve_trx_transfer_status::OK) {
      return fail_commit_epoch(status, "control_only_commit_publish_failed");
    }
    g_receiver_terminal_commit_admitted_monotonic_us.store(
        transfer_monotonic_us());
    refresh_receiver_ready_after_terminal_commit_admitted();
    g_receiver_final_metadata_saved_us.store(transfer_monotonic_us());
    if (commit_accepted != nullptr) {
      try {
        return commit_accepted(
            commit_accepted_context, frame.epoch_id,
            Preserve_trx_transfer_status::COMMITTED_NOT_READY);
      } catch (...) {
        return Preserve_trx_transfer_status::ACK_UNCERTAIN;
      }
    }
    return Preserve_trx_transfer_status::OK;
  }
  Preserve_trx_transfer_receiver_record record;
  size_t chunk_object_count = 0;
  const bool chunk_frame =
      frame.type == Preserve_trx_transfer_frame_type::OBJECT_CHUNK;
  const bool record_found =
      (chunk_frame || frame.type == Preserve_trx_transfer_frame_type::SEAL_OBJECT)
          ? registry->lookup_object(frame.epoch_id, frame.token,
                                                  frame.object_id, &record,
                                                  &chunk_object_count)
                  : registry->lookup(frame.epoch_id, frame.token, &record);
  if (!record_found) {
    if (commit_frame) {
      return fail_commit_epoch(Preserve_trx_transfer_status::INVALID_ARGUMENT,
                               "commit_epoch_token_not_found");
    }
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const bool object_frame =
      frame.type == Preserve_trx_transfer_frame_type::OBJECT_CHUNK ||
      frame.type == Preserve_trx_transfer_frame_type::SEAL_OBJECT;
  if (record.state != Preserve_trx_transfer_receiver_state::RECEIVING &&
      !((object_frame || commit_frame) &&
        record.state == Preserve_trx_transfer_receiver_state::DECLARED)) {
    if (commit_frame) {
      return fail_commit_epoch(Preserve_trx_transfer_status::UNSUPPORTED,
                               "commit_epoch_token_state_invalid");
    }
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  const Preserve_trx_transfer_manifest manifest =
      receiver_record_manifest(record);
  std::vector<Preserve_trx_transfer_receiver_record> commit_records;

  switch (frame.type) {
    case Preserve_trx_transfer_frame_type::OBJECT_CHUNK: {
      // Keep the full-manifest admission limit even with a one-object snapshot.
      if (chunk_object_count > kMaxTransferManifestObjects) {
        status = Preserve_trx_transfer_status::INVALID_ARGUMENT;
        break;
      }
      const Preserve_trx_transfer_object_descriptor *object =
          find_object(manifest, frame.object_id);
      if (object != nullptr &&
          transfer_object_uses_strict_v1_memory_staging(manifest, *object)) {
        status = registry->stage_strict_v1_object_chunk(
            manifest, frame.object_id, frame.chunk_offset, frame.chunk_payload);
      } else {
        status = preserve_trx_transfer_stage_object_chunk(
            root_dir, manifest, frame.object_id, frame.chunk_offset,
            frame.chunk_payload);
      }
      break;
    }
    case Preserve_trx_transfer_frame_type::SEAL_OBJECT: {
      std::shared_ptr<const Preserve_trx_sealed_file> sealed_file;
      const Preserve_trx_transfer_object_descriptor *object =
          find_object(manifest, frame.object_id);
      if (object != nullptr &&
          transfer_object_uses_strict_v1_memory_staging(manifest, *object)) {
        status = registry->seal_strict_v1_object(manifest, frame.object_id);
      } else {
        status = preserve_trx_transfer_seal_staged_object(
            root_dir, manifest, frame.object_id,
            object != nullptr &&
                    (object->kind == Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR ||
                     object->kind == Preserve_trx_transfer_object_kind::CURSOR_RESULT)
                ? &sealed_file : nullptr);
      }
      if (status == Preserve_trx_transfer_status::OK) {
        g_receiver_last_object_seal_monotonic_us.store(transfer_monotonic_us());
        status = registry->mark_object_sealed(frame.epoch_id, frame.token,
                                             frame.object_id,
                                             std::move(sealed_file));
        if (status == Preserve_trx_transfer_status::OK) {
          Preserve_trx_transfer_receiver_record sealed_record;
          size_t ignored_count = 0;
          const char *companion = frame.object_id == kPreservedTrxBlobBinlogCache
              ? kBinlogPrewarmSeedObjectId
              : (frame.object_id == kBinlogPrewarmSeedObjectId
                     ? kPreservedTrxBlobBinlogCache : nullptr);
          if (registry->lookup_object(frame.epoch_id, frame.token, frame.object_id,
                                       &sealed_record, &ignored_count, true, companion)) {
            const auto sealed_manifest = receiver_record_manifest(sealed_record);
            status = enqueue_receiver_object_prewarm(
                root_dir, sealed_manifest, frame.object_id, registry);
            if (status == Preserve_trx_transfer_status::OK &&
                frame.object_id == kPreservedTrxBlobBinlogCache &&
                sealed_record.sealed_objects.count(kBinlogPrewarmSeedObjectId)) {
              status = enqueue_receiver_object_prewarm(
                  root_dir, sealed_manifest, kBinlogPrewarmSeedObjectId, registry);
            }
          }
          if (status == Preserve_trx_transfer_status::OK &&
              registry->all_objects_sealed(frame.epoch_id, frame.token, true) &&
              registry->lookup(frame.epoch_id, frame.token, &sealed_record)) {
            const auto final_manifest = receiver_record_manifest(sealed_record);
            if (transfer_manifest_has_snapshot_bundle(final_manifest))
              status = enqueue_receiver_staged_token_prewarm(
                  root_dir, final_manifest, registry);
          }
        }
      }
      break;
    }
    case Preserve_trx_transfer_frame_type::COMMIT_EPOCH:
      {
        status = registry->snapshot_epoch_for_commit(
            frame.epoch_id, frame.sequence, commit_frame_digest,
            &commit_records);
        if (status == Preserve_trx_transfer_status::COMMITTED_NOT_READY) {
          if (commit_accepted == nullptr) {
            return Preserve_trx_transfer_status::OK;
          }
          Preserve_trx_transfer_status callback_status =
              Preserve_trx_transfer_status::ACK_UNCERTAIN;
          try {
            callback_status = commit_accepted(
                commit_accepted_context, frame.epoch_id,
                Preserve_trx_transfer_status::COMMITTED_NOT_READY);
          } catch (...) {
            return Preserve_trx_transfer_status::ACK_UNCERTAIN;
          }
          if (callback_status == Preserve_trx_transfer_status::OK) {
            Preserve_trx_transfer_accepted_epoch accepted;
            if (registry->query_accepted_epoch(root_dir, frame.epoch_id,
                                               &accepted) ==
                    Preserve_trx_transfer_status::COMMITTED_NOT_READY &&
                accepted.fact != nullptr) {
              cache_receiver_epoch_fact(root_dir, accepted.fact);
              bind_strict_prepared_tokens_from_epoch_fact(
                  root_dir, *accepted.fact, registry);
              const bool ready_published =
                  publish_receiver_epoch_ready_from_fact_if_possible(
                      root_dir, frame.epoch_id, registry);
              if (!ready_published &&
                  receiver_epoch_selection_complete_with_failures(
                      root_dir, frame.epoch_id)) {
                (void)publish_receiver_epoch_selection_if_possible(
                    root_dir, frame.epoch_id, registry,
                    transfer_monotonic_us(), false);
              }
            }
          }
          return callback_status;
        }
        if (status == Preserve_trx_transfer_status::RESOURCE_EXHAUSTED) {
          return status;
        }
        if (status != Preserve_trx_transfer_status::OK) {
          const std::vector<Preserve_trx_transfer_receiver_record> records =
              registry->receiving_records_for_epoch(frame.epoch_id);
          const Preserve_trx_transfer_status mark_status =
              mark_epoch_records_corrupt_and_purge(
                  root_dir, frame.epoch_id, registry, records,
                  "commit_epoch_snapshot_invalid");
          (void)cleanup_epoch_transfer_staging(root_dir, records, registry);
          return mark_status == Preserve_trx_transfer_status::OK
                     ? status
                     : mark_status;
        }
      }
      {
        const std::vector<Preserve_trx_transfer_receiver_record> &records =
            commit_records;
        if (records.empty()) {
          registry->mark_epoch_commit_admission_corrupt(
              frame.epoch_id, frame.sequence, commit_frame_digest);
          const Preserve_trx_transfer_status mark_status =
              mark_epoch_records_corrupt_and_purge(
                  root_dir, frame.epoch_id, registry, records,
                  "commit_epoch_empty_snapshot");
          (void)cleanup_epoch_transfer_staging(root_dir, records, registry);
          return mark_status == Preserve_trx_transfer_status::OK
                     ? Preserve_trx_transfer_status::CORRUPT
                     : mark_status;
        }
        std::vector<Preserve_trx_transfer_manifest> epoch_manifests;
        std::vector<uint64_t> published_tokens;
        epoch_manifests.reserve(records.size());
        Preserved_trx_carrier_listing pre_commit_listing;
        const bool have_pre_commit_listing =
            store->list_tokens(&pre_commit_listing) == Preserve_snapshot_status::OK;
        for (const Preserve_trx_transfer_receiver_record &epoch_record :
             records) {
          const Preserve_trx_transfer_manifest epoch_manifest =
              receiver_record_manifest(epoch_record);
          epoch_manifests.push_back(epoch_manifest);
          if (have_pre_commit_listing &&
              standby_token_artifact_published_in_listing(epoch_manifest,
                                                          pre_commit_listing)) {
            published_tokens.push_back(epoch_manifest.token);
            continue;
          }
        }
        const bool strict_epoch = std::any_of(
            epoch_manifests.begin(), epoch_manifests.end(),
            [](const Preserve_trx_transfer_manifest &epoch_manifest) {
              return transfer_manifest_uses_strict_metadata_only_prewarm(
                  epoch_manifest);
            });
        const bool strict_online_epoch = std::all_of(
            epoch_manifests.begin(), epoch_manifests.end(),
            [](const Preserve_trx_transfer_manifest &epoch_manifest) {
              return transfer_manifest_uses_strict_metadata_only_prewarm(
                  epoch_manifest);
            });
        if (strict_epoch &&
            !transfer_trx_id_store_fact_is_valid(frame.trx_id_store,
                                                 frame.chunk_offset)) {
          registry->mark_epoch_commit_admission_corrupt(
              frame.epoch_id, frame.sequence, commit_frame_digest);
          const Preserve_trx_transfer_status mark_status =
              mark_epoch_records_corrupt_and_purge(
                  root_dir, frame.epoch_id, registry, records,
                  "commit_epoch_missing_or_invalid_trx_id_store_proof");
          (void)cleanup_epoch_transfer_staging(root_dir, records, registry);
          return mark_status == Preserve_trx_transfer_status::OK
                     ? Preserve_trx_transfer_status::CORRUPT
                     : mark_status;
        }
        if (status == Preserve_trx_transfer_status::OK) {
          Preserve_trx_transfer_epoch_fact built_fact;
          status = build_epoch_fact_from_manifests(
              epoch_manifests, frame.chunk_offset, frame.trx_id_store,
              &built_fact);
          std::shared_ptr<const Preserve_trx_transfer_epoch_fact> accepted_fact;
          if (status == Preserve_trx_transfer_status::OK) {
            const auto expected_terminal_fact_digest =
                has_session_only_tokens
                    ? session_only_transaction_fact_digest(
                          built_fact.fact_digest, frame)
                    : built_fact.fact_digest;
            if (((has_session_only_tokens ||
                  !frame.receiver_process_nonce.empty()) &&
                 transfer_digest_is_zero(frame.terminal_fact_digest)) ||
                (!transfer_digest_is_zero(frame.terminal_fact_digest) &&
                 frame.terminal_fact_digest !=
                     expected_terminal_fact_digest)) {
              status = Preserve_trx_transfer_status::CORRUPT;
            }
          }
          if (status == Preserve_trx_transfer_status::OK) {
            try {
              accepted_fact =
                  std::make_shared<const Preserve_trx_transfer_epoch_fact>(
                      std::move(built_fact));
            } catch (...) {
              status = Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
            }
          }
          if (status == Preserve_trx_transfer_status::OK &&
              !strict_online_epoch) {
            status = commit_epoch_final_metadata(
                root_dir, epoch_manifests, manifest, frame.chunk_offset,
                frame.trx_id_store);
          }
          if (status != Preserve_trx_transfer_status::OK) {
            if (status == Preserve_trx_transfer_status::RESOURCE_EXHAUSTED &&
                accepted_fact == nullptr) {
              return status;
            }
            registry->mark_epoch_commit_admission_corrupt(
                frame.epoch_id, frame.sequence, commit_frame_digest);
            const std::string message =
                "PRESERVE: standby transfer receiver commit epoch finalize failed status=" +
                transfer_status_name(status) + " epoch=" + frame.epoch_id +
                " token=" + std::to_string(frame.token);
            LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
            const Preserve_trx_transfer_status mark_status =
                mark_epoch_records_corrupt_and_purge(
                    root_dir, frame.epoch_id, registry, records,
                    "commit_epoch_finalize:" + transfer_status_name(status));
            for (uint64_t published_token : published_tokens) {
              (void)store->remove_with_status(
                  transfer_token_component(published_token));
            }
            (void)cleanup_epoch_transfer_staging(root_dir, records, registry);
            return mark_status == Preserve_trx_transfer_status::OK
                       ? status
                       : mark_status;
          }
          /* Legacy no-payload COMMIT maps to a fail-closed empty active S. */
          status = registry->publish_accepted_epoch(
              root_dir, accepted_fact, receiver_boot_incarnation(),
              transfer_monotonic_us(),
              preserve_trx_transfer_receiver_prewarm_timeout_ms,
              !strict_online_epoch, &session_only_tokens,
              transfer_digest_is_zero(frame.terminal_fact_digest)
                  ? nullptr
                  : &frame.terminal_fact_digest);
          if (status != Preserve_trx_transfer_status::OK) {
            if (status == Preserve_trx_transfer_status::RESOURCE_EXHAUSTED) {
              return status;
            }
            registry->mark_epoch_commit_admission_corrupt(
                frame.epoch_id, frame.sequence, commit_frame_digest);
            const Preserve_trx_transfer_status mark_status =
                mark_epoch_records_corrupt_and_purge(
                    root_dir, frame.epoch_id, registry, records,
                    "commit_epoch_accept:" + transfer_status_name(status));
            (void)cleanup_epoch_transfer_staging(root_dir, records, registry);
            return mark_status == Preserve_trx_transfer_status::OK
                       ? status
                       : mark_status;
          }
          g_receiver_terminal_commit_admitted_monotonic_us.store(
              transfer_monotonic_us());
          refresh_receiver_ready_after_terminal_commit_admitted();
          if (strict_online_epoch) {
            cache_receiver_epoch_fact(root_dir, accepted_fact);
#ifndef NDEBUG
            const auto hook =
                g_after_epoch_fact_cache_hook_for_unit_test.load();
            if (hook != nullptr) hook();
#endif
          }
          g_receiver_final_metadata_saved_us.store(transfer_monotonic_us());
          const Preserve_trx_transfer_status committed_status =
              Preserve_trx_transfer_status::COMMITTED_NOT_READY;
          if (commit_accepted != nullptr) {
            Preserve_trx_transfer_status callback_status =
                Preserve_trx_transfer_status::ACK_UNCERTAIN;
            try {
              callback_status = commit_accepted(
                  commit_accepted_context, frame.epoch_id, committed_status);
            } catch (...) {
              callback_status = Preserve_trx_transfer_status::ACK_UNCERTAIN;
            }
            if (callback_status != Preserve_trx_transfer_status::OK) {
              return callback_status;
            }
          }
          if (strict_online_epoch) {
            bind_strict_prepared_tokens_from_epoch_fact(root_dir,
                                                        *accepted_fact,
                                                        registry);
          } else {
            bind_strict_prepared_tokens_from_committed_epoch(root_dir,
                                                             frame.epoch_id,
                                                             registry);
          }
          /*
            SEAL_OBJECT already starts per-token prewarm as soon as each token's
            complete objects arrive. When every token reached that state before
            COMMIT_EPOCH, the final metadata frame only needs to bind the epoch
            fact to the existing ready cache. Falling back to a committed-epoch
            prewarm job would redo the full token scan after source phase 2 and
            turn receiver readiness into a gate tail.
          */
          const bool ready_published =
              publish_receiver_epoch_ready_from_seal_prewarm(
                  root_dir, epoch_manifests, registry);
          const bool selection_published =
              !ready_published &&
              receiver_epoch_selection_complete_with_failures(
                  root_dir, frame.epoch_id) &&
              publish_receiver_epoch_selection_if_possible(
                  root_dir, frame.epoch_id, registry, transfer_monotonic_us(),
                  false);
          if (!ready_published && !selection_published) {
            note_receiver_epoch_pending_without_cold_fallback(
                epoch_manifests,
                Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY);
            for (const Preserve_trx_transfer_manifest &epoch_manifest :
                 epoch_manifests) {
              if (receiver_epoch_token_result_known(
                      root_dir, epoch_manifest.epoch_id,
                      epoch_manifest.token)) {
                continue;
              }
              const Preserve_trx_transfer_status enqueue_status =
                  enqueue_receiver_staged_token_prewarm(root_dir,
                                                        epoch_manifest,
                                                        registry);
              if (enqueue_status != Preserve_trx_transfer_status::OK) {
                const std::string message =
                    "PRESERVE: committed standby transfer epoch deferred "
                    "receiver prewarm epoch=" +
                    frame.epoch_id + " token=" +
                    std::to_string(epoch_manifest.token) + " status=" +
                    transfer_status_name(enqueue_status);
                LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
              }
            }
          }
        }
      }
      if (status == Preserve_trx_transfer_status::OK) {
        /*
          Streaming receiver READY is an online handoff state. The ready cache is
          bound to the committed epoch fact, so local standby-pending projection
          is not part of the phase-2/READY critical path.
        */
        for (const Preserve_trx_transfer_receiver_record &epoch_record :
             commit_records) {
          const Preserve_trx_transfer_manifest epoch_manifest =
              receiver_record_manifest(epoch_record);
          if (!receiver_seal_prewarm_token_ok(
                  root_dir, epoch_manifest.epoch_id,
                  epoch_manifest.token) ||
              !receiver_strict_token_ready(root_dir, epoch_manifest)) {
            continue;
          }
          status = finalize_receiver_ready_token_staging(root_dir,
                                                         epoch_manifest,
                                                         registry);
          if (status != Preserve_trx_transfer_status::OK) {
            const std::string message =
                "PRESERVE: committed standby transfer epoch deferred ready "
                "staging finalization epoch=" +
                frame.epoch_id + " token=" +
                std::to_string(epoch_manifest.token) + " status=" +
                transfer_status_name(status);
            LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
            status = Preserve_trx_transfer_status::OK;
          }
        }
        request_receiver_prewarm_idle_stop();
        return Preserve_trx_transfer_status::OK;
      }
      break;
    case Preserve_trx_transfer_frame_type::OPEN_EPOCH:
    case Preserve_trx_transfer_frame_type::BEGIN:
    case Preserve_trx_transfer_frame_type::ABORT:
    case Preserve_trx_transfer_frame_type::PROMOTION_PREWARM_TOKEN:
    case Preserve_trx_transfer_frame_type::PROMOTION_GATE_EPOCH:
    case Preserve_trx_transfer_frame_type::DECLARE_TOKEN:
    case Preserve_trx_transfer_frame_type::DECLARE_OBJECT:
    case Preserve_trx_transfer_frame_type::QUERY_EPOCH_STATUS:
    case Preserve_trx_transfer_frame_type::ABANDON_EPOCH_IF_NOT_COMMITTED:
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  if (status != Preserve_trx_transfer_status::OK) {
    std::string reason =
        "apply_receiver_frame:" + transfer_status_name(status);
    const Preserve_trx_transfer_status mark_status =
        registry->mark_corrupt(frame.epoch_id, frame.token, reason);
    if (mark_status != Preserve_trx_transfer_status::OK) return mark_status;
    purge_receiver_epoch_derived_state(
        root_dir, frame.epoch_id, receiver_boot_incarnation());
    erase_receiver_strict_record_lock_state(root_dir, frame.epoch_id,
                                            frame.token);
    erase_receiver_binlog_prepared(root_dir, frame.epoch_id, frame.token);
    purge_strict_prepared_token_for_receiver(root_dir, record);
    registry->erase_strict_v1_token_objects(frame.epoch_id, frame.token);
    const Preserve_trx_transfer_status cleanup_status =
        cleanup_transfer_token_staging(root_dir, frame.epoch_id, frame.token);
    const bool cleanup_failed =
        cleanup_status != Preserve_trx_transfer_status::OK;
    if (cleanup_failed) {
      reason.append("; cleanup:");
      reason.append(transfer_status_name(cleanup_status));
      const Preserve_trx_transfer_status debt_status =
          registry->mark_cleanup_pending(
              root_dir, frame.epoch_id, frame.token, transfer_monotonic_us(),
              Preserve_trx_transfer_receiver_state::CORRUPT, reason);
      return debt_status == Preserve_trx_transfer_status::OK ? status
                                                             : debt_status;
    }
    const auto released =
        registry->complete_staging_cleanup(frame.epoch_id, frame.token);
    if (released != Preserve_trx_transfer_status::OK) return released;
    return status;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_apply_receiver_frame(
    const std::string &root_dir, const Preserve_trx_transfer_frame &frame,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata) {
  return preserve_trx_transfer_apply_receiver_frame_internal(
      root_dir, frame, store, registry, timeout_seconds, written_metadata,
      nullptr, nullptr);
}

#ifndef NDEBUG
Preserve_trx_transfer_status preserve_trx_transfer_handle_receiver_payload(
    const std::string &root_dir, const std::string &encoded_frame,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata) {
  if (transfer_frame_batch_magic_matches(encoded_frame)) {
    std::vector<std::string> encoded_frames;
    const Preserve_trx_transfer_status decode_status =
        preserve_trx_transfer_decode_frame_batch(encoded_frame,
                                                 &encoded_frames);
    if (decode_status != Preserve_trx_transfer_status::OK) {
      return decode_status;
    }
    return preserve_trx_transfer_handle_receiver_payload_batch(
        root_dir, encoded_frames, store, registry, timeout_seconds,
        preserve_trx_transfer_current_runtime_policy().receiver_workers,
        written_metadata);
  }

  Preserve_trx_transfer_frame frame;
  const Preserve_trx_transfer_status status =
      preserve_trx_transfer_decode_frame(encoded_frame, &frame);
  if (status != Preserve_trx_transfer_status::OK) return status;
  return preserve_trx_transfer_apply_receiver_frame(
      root_dir, frame, store, registry, timeout_seconds, written_metadata);
}
#endif

struct Receiver_payload_batch_apply_context {
  const std::string *root_dir{nullptr};
  Preserved_trx_store *store{nullptr};
  Preserve_trx_transfer_receiver_registry *registry{nullptr};
  Preserve_snapshot_metadata *written_metadata{nullptr};
  uint64_t timeout_seconds{0};
  bool sequence_pre_admitted{false};
  bool commit_apply_started{false};
  Preserve_trx_transfer_commit_accepted_callback commit_accepted{nullptr};
  void *commit_accepted_context{nullptr};
};

static Preserve_trx_transfer_status apply_receiver_payload_batch_frame(
    const Preserve_trx_transfer_frame &frame, void *context) {
  auto *batch_context =
      static_cast<Receiver_payload_batch_apply_context *>(context);
  if (batch_context == nullptr || batch_context->root_dir == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const bool sequence_frame =
      receiver_frame_is_sequence_tracked(frame.type);
  if (sequence_frame && batch_context->registry != nullptr &&
      batch_context->registry->frame_sequence_applied(frame.epoch_id,
                                                      frame.sequence)) {
    return Preserve_trx_transfer_status::OK;
  }
  std::unique_ptr<Receiver_frame_sequence_disable_guard> sequence_guard;
  if (batch_context->sequence_pre_admitted) {
    sequence_guard.reset(new Receiver_frame_sequence_disable_guard());
  }
  if (frame.type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH) {
    batch_context->commit_apply_started = true;
  }
  const Preserve_trx_transfer_status status =
      preserve_trx_transfer_apply_receiver_frame_internal(
      *batch_context->root_dir, frame, batch_context->store,
      batch_context->registry, batch_context->timeout_seconds,
      batch_context->written_metadata, batch_context->commit_accepted,
      batch_context->commit_accepted_context);
  if (status != Preserve_trx_transfer_status::OK) {
    const std::string message =
        "PRESERVE: standby transfer receiver semantic apply failed epoch=" +
        frame.epoch_id + " sequence=" + std::to_string(frame.sequence) +
        " frame_type=" + std::to_string(static_cast<int>(frame.type)) +
        " token=" + std::to_string(frame.token) + " object=" +
        frame.object_id + " status=" + transfer_status_name(status);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
  }
  if (status == Preserve_trx_transfer_status::OK && sequence_frame &&
      batch_context->registry != nullptr) {
    batch_context->registry->mark_frame_sequence_applied(frame.epoch_id,
                                                         frame.sequence);
  } else if (status != Preserve_trx_transfer_status::OK && sequence_frame &&
             batch_context->registry != nullptr) {
    batch_context->registry->mark_frame_sequence_apply_failed(
        frame.epoch_id, frame.sequence, status);
  }
  return status;
}

Preserve_trx_transfer_status pre_admit_receiver_batch_sequence(
    const std::string &root_dir,
    const std::vector<Preserve_trx_transfer_frame> &frames,
    Preserve_trx_transfer_receiver_registry *registry,
    std::array<unsigned char, kPreservedTrxSha256Length>
        *commit_frame_digest) {
  if (root_dir.empty() || registry == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (commit_frame_digest != nullptr) commit_frame_digest->fill(0);

  std::vector<const Preserve_trx_transfer_frame *> newly_admitted_frames;
  try {
    newly_admitted_frames.reserve(frames.size());
  } catch (...) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  uint64_t newly_admitted_bytes = 0;
  bool final_metadata_admitted = false;
  bool commit_digest_captured = false;
  auto rollback_new_admissions = [&](Preserve_trx_transfer_status status) {
    if (status == Preserve_trx_transfer_status::CORRUPT) {
      for (const auto *frame : newly_admitted_frames) {
        registry->mark_frame_sequence_corrupt(frame->epoch_id, frame->sequence);
      }
      return;
    }
    for (auto frame = newly_admitted_frames.rbegin();
         frame != newly_admitted_frames.rend(); ++frame) {
      registry->rollback_frame_sequence((*frame)->epoch_id, (*frame)->sequence);
    }
  };

  for (const Preserve_trx_transfer_frame &frame : frames) {
    if (!receiver_frame_is_sequence_tracked(frame.type)) continue;

    std::string encoded_frame;
    Preserve_trx_transfer_status status =
        preserve_trx_transfer_encode_frame(frame, &encoded_frame);
    if (status != Preserve_trx_transfer_status::OK) return status;
    Preserve_trx_transfer_sequence_admission admission =
        Preserve_trx_transfer_sequence_admission::NEW_FRAME;
    const bool commit_frame =
        frame.type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH;
    const std::string *terminal_root_dir = commit_frame ? &root_dir : nullptr;
    const auto *terminal_fact_digest =
        commit_frame ? &frame.terminal_fact_digest : nullptr;
    const auto frame_digest = sha256_digest(encoded_frame);
    if (commit_frame && commit_frame_digest != nullptr &&
        !commit_digest_captured) {
      *commit_frame_digest = frame_digest;
      commit_digest_captured = true;
    }
    try {
      newly_admitted_frames.push_back(&frame);
    } catch (...) {
      rollback_new_admissions(
          Preserve_trx_transfer_status::RESOURCE_EXHAUSTED);
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    status = registry->admit_frame_sequence(
        frame.epoch_id, frame.sequence, frame_digest, frame.type, &admission,
        terminal_root_dir, terminal_fact_digest,
        commit_frame ? transfer_monotonic_us() : 0);
    if (status != Preserve_trx_transfer_status::OK) {
      newly_admitted_frames.pop_back();
      rollback_new_admissions(status);
      return status;
    }

    if (admission == Preserve_trx_transfer_sequence_admission::NEW_FRAME) {
      if (encoded_frame.size() >
          std::numeric_limits<uint64_t>::max() - newly_admitted_bytes) {
        rollback_new_admissions(
            Preserve_trx_transfer_status::RESOURCE_EXHAUSTED);
        return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
      }
      newly_admitted_bytes += encoded_frame.size();
      final_metadata_admitted =
          final_metadata_admitted ||
          frame.type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH;
    } else {
      newly_admitted_frames.pop_back();
    }
  }
  if (!newly_admitted_frames.empty()) {
    note_receiver_first_frame_saved();
    g_receiver_admitted_frames.fetch_add(newly_admitted_frames.size());
    g_receiver_admitted_bytes.fetch_add(newly_admitted_bytes);
  }
  if (final_metadata_admitted) {
    g_receiver_final_metadata_accepted_monotonic_us.store(
        transfer_monotonic_us());
    refresh_receiver_ready_after_final_metadata_accepted();
  }
  return Preserve_trx_transfer_status::OK;
}

namespace {

using Transfer_receiver_batch_key = std::pair<std::string, uint64_t>;
using Transfer_receiver_frame_iterator =
    std::vector<Preserve_trx_transfer_frame>::const_iterator;

Preserve_trx_transfer_status apply_receiver_frame_segment_with_workers(
    Transfer_receiver_frame_iterator first, Transfer_receiver_frame_iterator last,
    uint worker_count,
    Preserve_trx_transfer_frame_apply_callback apply_frame, void *context) {
  if (first == last) return Preserve_trx_transfer_status::OK;
  if (worker_count <= 1) {
    for (auto frame = first; frame != last; ++frame) {
      try {
        const Preserve_trx_transfer_status status = apply_frame(*frame, context);
        if (status != Preserve_trx_transfer_status::OK) return status;
      } catch (...) {
        return Preserve_trx_transfer_status::UNSUPPORTED;
      }
    }
    return Preserve_trx_transfer_status::OK;
  }

  // The caller owns immutable frames until every worker has joined. Group
  // pointers, not payload copies; each token still has exactly one worker.
  std::map<Transfer_receiver_batch_key,
           std::vector<const Preserve_trx_transfer_frame *>>
      frames_by_token;
  std::vector<Transfer_receiver_batch_key> token_order;
  for (auto pos = first; pos != last; ++pos) {
    const auto &frame = *pos;
    Transfer_receiver_batch_key key(frame.epoch_id, frame.token);
    auto it = frames_by_token.find(key);
    if (it == frames_by_token.end()) {
      token_order.push_back(key);
      it = frames_by_token.emplace(
          key, std::vector<const Preserve_trx_transfer_frame *>())
               .first;
    }
    it->second.push_back(&frame);
  }

  const size_t actual_workers =
      std::min<size_t>(std::max<uint>(1, worker_count), token_order.size());
  std::atomic<size_t> next_token{0};
  std::atomic<bool> workers_released{false};
  std::atomic<bool> worker_abort{false};
  std::mutex status_mutex;
  Preserve_trx_transfer_status first_status =
      Preserve_trx_transfer_status::OK;

  auto remember_failure = [&](Preserve_trx_transfer_status status) {
    if (status == Preserve_trx_transfer_status::OK) return;
    std::lock_guard<std::mutex> guard(status_mutex);
    if (first_status == Preserve_trx_transfer_status::OK) {
      first_status = status;
    }
  };

  auto worker = [&]() {
    while (!workers_released.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    for (;;) {
      if (worker_abort.load(std::memory_order_acquire)) break;
      const size_t token_index = next_token.fetch_add(1);
      if (token_index >= token_order.size()) break;

      const auto group_it = frames_by_token.find(token_order[token_index]);
      if (group_it == frames_by_token.end()) {
        remember_failure(Preserve_trx_transfer_status::CORRUPT);
        continue;
      }
      for (const auto *frame : group_it->second) {
        try {
          const Preserve_trx_transfer_status status =
              apply_frame(*frame, context);
          if (status != Preserve_trx_transfer_status::OK) {
            remember_failure(status);
            break;
          }
        } catch (...) {
          remember_failure(Preserve_trx_transfer_status::UNSUPPORTED);
          break;
        }
      }
    }
  };

  std::vector<std::thread> workers;
  auto join_workers = create_scope_guard([&] {
    for (std::thread &thread : workers) {
      if (thread.joinable()) thread.join();
    }
  });
  try {
    workers.reserve(actual_workers);
    for (size_t i = 0; i < actual_workers; ++i) {
      if (g_temporary_worker_fail_create_at_for_unit_test.load() ==
          static_cast<int>(i)) {
        throw std::system_error(
            std::make_error_code(std::errc::resource_unavailable_try_again));
      }
      workers.emplace_back(worker);
    }
  } catch (...) {
    worker_abort.store(true, std::memory_order_release);
    remember_failure(Preserve_trx_transfer_status::RESOURCE_EXHAUSTED);
  }
  workers_released.store(true, std::memory_order_release);
  join_workers.rollback();
  return first_status;
}

}  // namespace



#ifndef NDEBUG
uint64_t
preserve_trx_transfer_receiver_staged_token_file_read_bytes_for_unit_test(
    const Preserve_trx_transfer_manifest &manifest) {
  return receiver_staged_token_file_read_bytes(manifest);
}

uint64_t
preserve_trx_transfer_receiver_object_prewarm_file_read_bytes_for_unit_test(
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id) {
  return receiver_object_prewarm_file_read_bytes(manifest, object_id);
}

bool preserve_trx_transfer_receiver_prewarm_work_batch_should_yield_for_unit_test(
    uint64_t job_elapsed_us, uint64_t yield_quantum_us,
    uint64_t *active_work_us) {
  return receiver_prewarm_work_batch_should_yield(
      job_elapsed_us, yield_quantum_us, active_work_us);
}
#endif

namespace {

Preserve_trx_transfer_status handle_decoded_receiver_payload_batch(
    const std::string &root_dir, std::vector<Preserve_trx_transfer_frame> &frames,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds, uint worker_count,
    Preserve_snapshot_metadata *written_metadata,
    Preserve_trx_transfer_after_admission_callback after_admission,
    void *after_admission_context,
    Preserve_trx_transfer_commit_accepted_callback commit_accepted,
    void *commit_accepted_context) {
  if (root_dir.empty() || store == nullptr || registry == nullptr || frames.empty())
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  std::stable_sort(frames.begin(), frames.end(),
                   [](const Preserve_trx_transfer_frame &lhs,
                      const Preserve_trx_transfer_frame &rhs) {
                     if (lhs.epoch_id != rhs.epoch_id) {
                       return lhs.epoch_id < rhs.epoch_id;
                     }
                     return lhs.sequence < rhs.sequence;
                   });

  std::string payload_epoch;
  uint64_t first_sequence = 0;
  uint64_t last_sequence = 0;
  std::set<uint64_t> payload_apply_tokens;
  const Preserve_trx_transfer_frame *commit_epoch_frame = nullptr;
  std::array<unsigned char, kPreservedTrxSha256Length> commit_frame_digest{};
  for (const Preserve_trx_transfer_frame &frame : frames) {
    if (!receiver_frame_is_sequence_tracked(frame.type)) continue;
    if (payload_epoch.empty()) payload_epoch = frame.epoch_id;
    if (payload_epoch != frame.epoch_id) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    if (first_sequence == 0 || frame.sequence < first_sequence) {
      first_sequence = frame.sequence;
    }
    last_sequence = std::max(last_sequence, frame.sequence);
    if (frame.type != Preserve_trx_transfer_frame_type::COMMIT_EPOCH) {
      payload_apply_tokens.insert(frame.token);
    } else if (commit_epoch_frame == nullptr) {
      commit_epoch_frame = &frame;
    }
  }
  bool payload_sequence_started = false;
  const uint64_t payload_timeout_ms =
      timeout_seconds > std::numeric_limits<uint64_t>::max() / 1000
          ? std::numeric_limits<uint64_t>::max()
          : std::max<uint64_t>(1, timeout_seconds * 1000);
  if (first_sequence != 0) {
    Preserve_trx_transfer_status gate_status =
        registry->begin_payload_sequence(payload_epoch, first_sequence,
                                         last_sequence,
                                         payload_timeout_ms);
    if (gate_status != Preserve_trx_transfer_status::OK) return gate_status;
    payload_sequence_started = true;
  }
  auto payload_sequence_guard = create_scope_guard([&] {
    if (payload_sequence_started) {
      registry->end_payload_sequence(payload_epoch);
    }
  });
  const bool contains_commit_epoch = commit_epoch_frame != nullptr;
  if (contains_commit_epoch && first_sequence > 1) {
    // Keep the admission gate while prior payloads finish. A failed upload
    // must remain abandonable, without publishing COMMIT_ADMITTED first.
    const auto status = registry->wait_for_frame_sequence_applied_through(
        payload_epoch, first_sequence - 1, payload_timeout_ms);
    if (status != Preserve_trx_transfer_status::OK) return status;
  }
  Preserve_trx_transfer_status status =
      pre_admit_receiver_batch_sequence(root_dir, frames, registry,
                                        &commit_frame_digest);
  if (status != Preserve_trx_transfer_status::OK) return status;
  Preserve_trx_transfer_payload_apply_reservation apply_reservation;
  bool apply_reserved = false;
  if (!payload_apply_tokens.empty()) {
    const std::vector<uint64_t> tokens(payload_apply_tokens.begin(),
                                       payload_apply_tokens.end());
    status = registry->reserve_payload_apply(
        payload_epoch, first_sequence, last_sequence, tokens,
        &apply_reservation);
    if (status != Preserve_trx_transfer_status::OK) return status;
    apply_reserved = true;
  }
  payload_sequence_guard.rollback();
  payload_sequence_started = false;
  if (after_admission != nullptr) {
    status =
        after_admission(after_admission_context, contains_commit_epoch);
    if (status != Preserve_trx_transfer_status::OK) return status;
  }
  bool apply_owner = true;
  if (apply_reserved) {
    status = registry->wait_for_payload_apply_turn(
        apply_reservation, payload_timeout_ms, &apply_owner);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (!apply_owner) return Preserve_trx_transfer_status::OK;
  }
  auto apply_reservation_guard = create_scope_guard([&] {
    if (apply_reserved && apply_owner) {
      registry->finish_payload_apply(apply_reservation);
    }
  });
  Receiver_payload_batch_apply_context context;
  context.root_dir = &root_dir;
  context.store = store;
  context.registry = registry;
  context.written_metadata = written_metadata;
  context.timeout_seconds = timeout_seconds;
  context.sequence_pre_admitted = true;
  context.commit_accepted = commit_accepted;
  context.commit_accepted_context = commit_accepted_context;
  status = preserve_trx_transfer_apply_receiver_frame_batch_with_workers(
      frames, written_metadata == nullptr ? worker_count : 1,
      apply_receiver_payload_batch_frame, &context);
  if (status != Preserve_trx_transfer_status::OK &&
      status != Preserve_trx_transfer_status::RESOURCE_EXHAUSTED &&
      commit_epoch_frame != nullptr && !context.commit_apply_started) {
    registry->mark_epoch_commit_admission_corrupt(
        commit_epoch_frame->epoch_id, commit_epoch_frame->sequence,
        commit_frame_digest);
    const std::vector<Preserve_trx_transfer_receiver_record> records =
        registry->receiving_records_for_epoch(commit_epoch_frame->epoch_id);
    const Preserve_trx_transfer_status mark_status =
        mark_epoch_records_corrupt_and_purge(
            root_dir, commit_epoch_frame->epoch_id, registry, records,
            "precommit_batch_apply_failed:" + transfer_status_name(status));
    const Preserve_trx_transfer_status cleanup_status =
        cleanup_epoch_transfer_staging(root_dir, records, registry);
    if (mark_status != Preserve_trx_transfer_status::OK) return mark_status;
    if (cleanup_status != Preserve_trx_transfer_status::OK) {
      return cleanup_status;
    }
  }
  return status;
}

}  // namespace

#ifndef NDEBUG
Preserve_trx_transfer_status preserve_trx_transfer_handle_receiver_payload_batch(
    const std::string &root_dir, const std::vector<std::string> &encoded_frames,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds, uint worker_count,
    Preserve_snapshot_metadata *written_metadata,
    Preserve_trx_transfer_after_admission_callback after_admission,
    void *after_admission_context,
    Preserve_trx_transfer_commit_accepted_callback commit_accepted,
    void *commit_accepted_context) {
  if (root_dir.empty() || store == nullptr || registry == nullptr ||
      encoded_frames.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  uint64_t encoded_bytes = 0;
  for (const std::string &encoded_frame : encoded_frames) {
    if (encoded_frame.length() >
        std::numeric_limits<uint64_t>::max() - encoded_bytes) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    encoded_bytes += encoded_frame.length();
  }
  if (encoded_bytes > std::numeric_limits<uint64_t>::max() / 3 ||
      encoded_frames.size() >
          std::numeric_limits<uint64_t>::max() /
              (sizeof(std::string) + sizeof(Preserve_trx_transfer_frame))) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  const uint64_t container_bytes =
      static_cast<uint64_t>(encoded_frames.size()) *
      (sizeof(std::string) + sizeof(Preserve_trx_transfer_frame));
  if (container_bytes >
      std::numeric_limits<uint64_t>::max() - encoded_bytes * 3) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  Preserve_memory_lease payload_lease = acquire_transfer_decode_memory_lease(
      encoded_frames.front(), encoded_bytes * 3 + container_bytes);
  if (!payload_lease.acquired()) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  std::vector<std::string> expanded_encoded_frames;
  expanded_encoded_frames.reserve(encoded_frames.size());
  for (const std::string &encoded_frame : encoded_frames) {
    if (!transfer_frame_batch_magic_matches(encoded_frame)) {
      expanded_encoded_frames.push_back(encoded_frame);
      continue;
    }
    std::vector<std::string> decoded_batch;
    const Preserve_trx_transfer_status status =
        preserve_trx_transfer_decode_frame_batch(encoded_frame,
                                                 &decoded_batch);
    if (status != Preserve_trx_transfer_status::OK) return status;
    expanded_encoded_frames.insert(expanded_encoded_frames.end(),
                                   decoded_batch.begin(),
                                   decoded_batch.end());
  }

  std::vector<Preserve_trx_transfer_frame> frames;
  frames.reserve(expanded_encoded_frames.size());
  for (const std::string &encoded_frame : expanded_encoded_frames) {
    Preserve_trx_transfer_frame frame;
    const Preserve_trx_transfer_status status =
        preserve_trx_transfer_decode_frame(encoded_frame, &frame);
    if (status != Preserve_trx_transfer_status::OK) return status;
    frames.push_back(std::move(frame));
  }
  return handle_decoded_receiver_payload_batch(root_dir, frames, store, registry,
      timeout_seconds, worker_count, written_metadata, after_admission,
      after_admission_context, commit_accepted, commit_accepted_context);
}
#endif

Preserve_trx_transfer_status
preserve_trx_transfer_apply_receiver_frame_batch_with_workers(
    const std::vector<Preserve_trx_transfer_frame> &frames, uint worker_count,
    Preserve_trx_transfer_frame_apply_callback apply_frame, void *context) {
  if (apply_frame == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (frames.empty()) return Preserve_trx_transfer_status::OK;

  auto first = frames.cbegin();
  for (auto pos = first; pos != frames.cend(); ++pos) {
    const auto &frame = *pos;
    const bool is_batch_barrier =
        frame.type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH;
    if (!is_batch_barrier) continue;

    Preserve_trx_transfer_status status =
        apply_receiver_frame_segment_with_workers(first, pos, worker_count,
                                                  apply_frame, context);
    if (status != Preserve_trx_transfer_status::OK) return status;
    try {
      status = apply_frame(frame, context);
    } catch (...) {
      status = Preserve_trx_transfer_status::UNSUPPORTED;
    }
    if (status != Preserve_trx_transfer_status::OK) return status;
    first = std::next(pos);
  }

  return apply_receiver_frame_segment_with_workers(
      first, frames.cend(), worker_count, apply_frame, context);
}

struct Receiver_admission_ack_context {
  THD *thd{nullptr};
  const std::string *encoded_payload{nullptr};
  std::string root_dir;
  // Valid only for this dispatch's immutable encoded_payload, after validation.
  std::string verified_epoch_id;
  uint64_t verified_sequence{0};
  uint64_t accepted_terminal_status_retention_us{0};
  Preserve_trx_temp_id_contract temp_id_contract;
  bool ack_sent{false};
  bool commit_ack_pending{false};
};

bool transfer_authenticated_principal(THD *thd, std::string *principal) {
  if (thd == nullptr || principal == nullptr ||
      thd->security_context() == nullptr) {
    return false;
  }
  const LEX_CSTRING user = thd->security_context()->priv_user();
  const LEX_CSTRING host = thd->security_context()->priv_host();
  if (user.str == nullptr || user.length == 0 || host.str == nullptr ||
      host.length == 0) {
    return false;
  }
  try {
    principal->clear();
    principal->reserve(user.length + host.length + 48);
    principal->append(std::to_string(user.length));
    principal->push_back(':');
    principal->append(user.str, user.length);
    principal->push_back('|');
    principal->append(std::to_string(host.length));
    principal->push_back(':');
    principal->append(host.str, host.length);
  } catch (...) {
    principal->clear();
    return false;
  }
  return true;
}

#ifndef NDEBUG
Preserve_trx_transfer_status
preserve_trx_transfer_validate_online_payload_identity(
    const std::string &encoded_payload, std::string *receiver_process_nonce,
    std::string *epoch_id, uint64_t *last_sequence) {
  if (receiver_process_nonce == nullptr || epoch_id == nullptr ||
      last_sequence == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  return transfer_payload_identity(encoded_payload, epoch_id, last_sequence,
                                   receiver_process_nonce);
}
#endif

static Preserve_trx_transfer_status send_receiver_authenticated_ack(
    Receiver_admission_ack_context *ack_context,
    Preserve_trx_transfer_status ack_status, bool acknowledge_epoch) {
  if (ack_context == nullptr || ack_context->thd == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  if (ack_context->thd->get_protocol_classic() == nullptr) {
    return Preserve_trx_transfer_status::IO_ERROR;
  }

  if (ack_context->encoded_payload == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  Preserve_trx_transfer_frame_ack ack;
  auto status = Preserve_trx_transfer_status::OK;
  if (ack_context->verified_sequence != 0) {
    ack.epoch_id = ack_context->verified_epoch_id;
    ack.sequence = ack_context->verified_sequence;
    ack.receiver_process_nonce = receiver_boot_incarnation();
    ack.frame_digest = sha256_digest(*ack_context->encoded_payload);
    ack.status = ack_status;
  } else {
    status = preserve_trx_transfer_build_frame_ack(
        receiver_boot_incarnation(), *ack_context->encoded_payload,
        ack_status, &ack);
    if (status != Preserve_trx_transfer_status::OK) return status;
  }
  const std::string &epoch_id = ack.epoch_id;
  if (ack.sequence == 0) {
    if (ack_context->accepted_terminal_status_retention_us == 0) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    ack.accepted_terminal_status_retention_us =
        ack_context->accepted_terminal_status_retention_us;
    ack.temp_id_contract = ack_context->temp_id_contract;
  }
  std::string encoded_ack;
  status = preserve_trx_transfer_encode_frame_ack(ack, &encoded_ack);
  if (status != Preserve_trx_transfer_status::OK) return status;

  /* The authenticated payload was admitted; semantic apply continues. */
  my_ok(ack_context->thd, 0, 0, encoded_ack.c_str());
  ack_context->thd->send_statement_status();
  ack_context->thd->get_stmt_da()->reset_diagnostics_area();
  ack_context->thd->get_stmt_da()->disable_status();
  ack_context->ack_sent = true;
  if (acknowledge_epoch) {
    const uint64_t ack_us = transfer_monotonic_us();
    uint64_t readiness_final_ack_us = 0;
    uint64_t readiness_ready_us = 0;
    bool log_readiness_timing = false;
    {
      std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
      const auto found = g_receiver_ready_epoch_state.find(
          {ack_context->root_dir, epoch_id});
      if (found != g_receiver_ready_epoch_state.end()) {
        found->second.final_spool_ack_monotonic_us = ack_us;
        log_readiness_timing = take_receiver_epoch_readiness_timing_locked(
            &found->second, &readiness_final_ack_us, &readiness_ready_us);
      }
    }
    g_receiver_final_spool_ack_monotonic_us.store(ack_us);
    refresh_receiver_ready_after_final_spool_ack();
    if (log_readiness_timing) {
      log_receiver_epoch_readiness_timing(epoch_id, readiness_final_ack_us,
                                          readiness_ready_us);
    }
    const Preserve_trx_transfer_status cleanup_status =
        default_receiver_registry().acknowledge_epoch(
            ack_context->root_dir, epoch_id, ack_us,
            ack_context->accepted_terminal_status_retention_us);
    if (cleanup_status != Preserve_trx_transfer_status::OK) {
      const std::string message =
          "PRESERVE: receiver acknowledged epoch but terminal retention "
          "registration failed epoch=" +
          epoch_id + " status=" + transfer_status_name(cleanup_status);
      LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    }
  }
  return Preserve_trx_transfer_status::OK;
}

static Preserve_trx_transfer_status send_receiver_admission_ack(
    void *context, bool contains_commit_epoch) {
  auto *ack_context = static_cast<Receiver_admission_ack_context *>(context);
  if (ack_context == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (contains_commit_epoch) {
    ack_context->commit_ack_pending = true;
    return Preserve_trx_transfer_status::OK;
  }
  return send_receiver_authenticated_ack(
      ack_context, Preserve_trx_transfer_status::OK, false);
}

static Preserve_trx_transfer_status send_receiver_commit_accepted_ack(
    void *context, const std::string &epoch_id,
    Preserve_trx_transfer_status committed_status) {
  auto *ack_context = static_cast<Receiver_admission_ack_context *>(context);
  if (ack_context == nullptr || epoch_id.empty() ||
      !transfer_status_is_committed_outcome(committed_status)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const Preserve_trx_transfer_status status = send_receiver_authenticated_ack(
      ack_context, committed_status, true);
  if (status == Preserve_trx_transfer_status::OK) {
    ack_context->commit_ack_pending = false;
  }
  return status;
}

void preserve_trx_transfer_dispatch_command(THD *thd) {
  /*
    The classic command is intentionally invisible unless Preserve/Resume is
    enabled. HA role admission belongs to the caller; this endpoint still
    enforces its dedicated global privilege and all transfer protocol checks.
  */
  if (thd == nullptr || !preserve_trx_is_enabled()) {
    my_error(ER_UNKNOWN_COM_ERROR, MYF(0));
    return;
  }
  if (!thd->security_context()
           ->has_global_grant(STRING_WITH_LEN("PRESERVE_TRX_TRANSFER_ADMIN"))
           .first) {
    my_error(ER_SPECIFIC_ACCESS_DENIED_ERROR, MYF(0),
             "PRESERVE_TRX_TRANSFER_ADMIN");
    return;
  }

  Protocol_classic *protocol = thd->get_protocol_classic();
  const uchar *raw_packet = protocol->get_raw_packet();
  const ulong raw_packet_length = protocol->get_packet_length();
  if (raw_packet == nullptr && raw_packet_length != 0) {
    my_error(ER_UNKNOWN_COM_ERROR, MYF(0));
    return;
  }

  std::string encoded_frame;
  if (raw_packet_length != 0) {
    encoded_frame.assign(reinterpret_cast<const char *>(raw_packet),
                         raw_packet_length);
  }

#ifndef NDEBUG
  Receiver_decode_counts decode_counts;
  DBUG_EXECUTE_IF("preserve_receiver_decode_counts", {
    receiver_decode_counts = &decode_counts;
  });
  const auto decode_observation = create_scope_guard([&] {
    const bool observed = receiver_decode_counts != nullptr;
    receiver_decode_counts = nullptr;
    if (observed && transfer_frame_batch_magic_matches(encoded_frame)) {
      Manifest_reader header(encoded_frame);
      const char *magic;
      uint16_t version;
      uint32_t expected = 0;
      if (!header.read_fixed(kTransferFrameBatchMagicLength, &magic) &&
          !header.read_u16(&version) && !header.read_u32(&expected)) {
        const std::string message = "PRESERVE_RX_DECODE expected=" +
            std::to_string(expected) + " batches=" +
            std::to_string(decode_counts.batches) + " frames=" +
            std::to_string(decode_counts.frames);
        LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
      }
    }
  });
#endif

  Preserve_memory_lease decoded_memory;
  std::vector<Preserve_trx_transfer_frame> decoded_frames;
  Preserve_trx_transfer_frame identity_frame;
  bool identity_frame_decoded = false;
  const bool is_batch = transfer_frame_batch_magic_matches(encoded_frame);
  bool validate_online_identity = is_batch;
  Preserve_trx_transfer_status identity_decode_status;
  if (is_batch) {
    identity_decode_status = decode_frame_batch_impl(
        encoded_frame, nullptr, &decoded_frames, &decoded_memory);
  } else {
    identity_decode_status =
        preserve_trx_transfer_decode_frame(encoded_frame, &identity_frame);
    identity_frame_decoded =
        identity_decode_status == Preserve_trx_transfer_status::OK;
    validate_online_identity =
        identity_frame_decoded &&
        (receiver_frame_is_sequence_tracked(identity_frame.type) ||
         identity_frame.type ==
             Preserve_trx_transfer_frame_type::QUERY_EPOCH_STATUS ||
         identity_frame.type ==
             Preserve_trx_transfer_frame_type::
                 ABANDON_EPOCH_IF_NOT_COMMITTED);
  }
  std::string authenticated_principal;
  if (!transfer_authenticated_principal(thd, &authenticated_principal)) {
    signal_transfer_dispatch_error(
        thd, Preserve_trx_transfer_status::UNSUPPORTED);
    return;
  }
  const std::string preserve_dir = preserved_trx_dir_value();
  Preserve_trx_transfer_runtime_policy receiver_runtime_policy =
      preserve_trx_transfer_current_runtime_policy();
  Receiver_admission_ack_context ack_context;
  ack_context.thd = thd;
  ack_context.encoded_payload = &encoded_frame;
  ack_context.root_dir = preserve_dir;
  if (identity_frame_decoded &&
      identity_frame.type == Preserve_trx_transfer_frame_type::OPEN_EPOCH) {
    Preserve_trx_transfer_status open_status =
        default_receiver_registry().open_online_epoch(
            identity_frame.epoch_id, authenticated_principal,
            identity_frame.requested_terminal_status_retention_us,
            receiver_boot_incarnation(),
            &ack_context.accepted_terminal_status_retention_us,
            identity_frame.temp_id_contract, &ack_context.temp_id_contract);
    if (open_status == Preserve_trx_transfer_status::OK) {
      open_status = send_receiver_authenticated_ack(
          &ack_context, Preserve_trx_transfer_status::OK, false);
    }
    if (open_status != Preserve_trx_transfer_status::OK) {
      signal_transfer_dispatch_error(thd, open_status);
    }
    return;
  }
  if (validate_online_identity) {
    std::string receiver_process_nonce;
    std::string epoch_id;
    uint64_t last_sequence = 0;
    Preserve_trx_transfer_status identity_status = identity_decode_status;
    if (identity_status == Preserve_trx_transfer_status::OK)
      identity_status = decoded_payload_identity(
          is_batch ? decoded_frames.data() : &identity_frame,
          is_batch ? decoded_frames.size() : 1, &epoch_id, &last_sequence,
          &receiver_process_nonce);
    if (identity_status == Preserve_trx_transfer_status::OK) {
      ack_context.verified_epoch_id = epoch_id;
      ack_context.verified_sequence = last_sequence;
    }
    const bool terminal_query =
        identity_frame_decoded &&
        identity_frame.type ==
            Preserve_trx_transfer_frame_type::QUERY_EPOCH_STATUS;
    const bool terminal_abandon =
        identity_frame_decoded &&
        identity_frame.type ==
            Preserve_trx_transfer_frame_type::
                ABANDON_EPOCH_IF_NOT_COMMITTED;
    if (identity_status == Preserve_trx_transfer_status::OK &&
        terminal_query) {
      Preserve_trx_transfer_epoch_terminal_status terminal;
      identity_status =
          default_receiver_registry().query_epoch_terminal_authenticated(
              preserve_dir, epoch_id, authenticated_principal,
              receiver_process_nonce, identity_frame.terminal_fact_digest,
              &terminal);
      if (identity_status == Preserve_trx_transfer_status::OK) {
        Preserve_trx_transfer_status query_status =
            Preserve_trx_transfer_status::IO_ERROR;
        switch (terminal.outcome) {
          case Preserve_trx_transfer_epoch_terminal_outcome::COMMITTED:
            query_status = Preserve_trx_transfer_status::COMMITTED_NOT_READY;
            break;
          case Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT:
            query_status = Preserve_trx_transfer_status::COMMITTED_CORRUPT;
            break;
          case Preserve_trx_transfer_epoch_terminal_outcome::NOT_COMMITTED:
          case Preserve_trx_transfer_epoch_terminal_outcome::ABANDONING:
            query_status = Preserve_trx_transfer_status::NOT_COMMITTED;
            break;
          case Preserve_trx_transfer_epoch_terminal_outcome::
              NOT_COMMITTED_CLEAN:
            query_status =
                Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN;
            break;
          case Preserve_trx_transfer_epoch_terminal_outcome::EPOCH_NOT_FOUND:
            break;
        }
        if (!transfer_status_has_authenticated_ack(query_status)) {
          signal_transfer_dispatch_error(thd, query_status);
          return;
        }
        const Preserve_trx_transfer_status ack_status =
            send_receiver_authenticated_ack(&ack_context, query_status, false);
        if (ack_status != Preserve_trx_transfer_status::OK) {
          signal_transfer_dispatch_error(thd, ack_status);
        }
        return;
      }
    } else if (identity_status == Preserve_trx_transfer_status::OK &&
               terminal_abandon) {
      identity_status = default_receiver_registry().validate_online_epoch(
          epoch_id, authenticated_principal, receiver_process_nonce,
          &ack_context.accepted_terminal_status_retention_us);
      if (identity_status == Preserve_trx_transfer_status::OK) {
        Preserve_trx_transfer_epoch_terminal_request request;
        request.root_dir = preserve_dir;
        request.epoch_id = epoch_id;
        request.receiver_process_generation = receiver_process_nonce;
        request.authenticated_principal = authenticated_principal;
        request.operation_id = "abandon-v1";
        request.fact_digest = identity_frame.terminal_fact_digest;
        request.now_us = transfer_monotonic_us();
        request.retention_us =
            ack_context.accepted_terminal_status_retention_us;
        const Preserve_trx_transfer_epoch_terminal_outcome outcome =
            preserve_trx_transfer_abandon_receiver_epoch_if_not_committed(
                request, &default_receiver_registry());
        Preserve_trx_transfer_status abandon_status =
            Preserve_trx_transfer_status::IO_ERROR;
        switch (outcome) {
          case Preserve_trx_transfer_epoch_terminal_outcome::COMMITTED:
            abandon_status =
                Preserve_trx_transfer_status::COMMITTED_NOT_READY;
            break;
          case Preserve_trx_transfer_epoch_terminal_outcome::
              NOT_COMMITTED_CLEAN:
            abandon_status =
                Preserve_trx_transfer_status::NOT_COMMITTED_CLEAN;
            break;
          case Preserve_trx_transfer_epoch_terminal_outcome::CORRUPT:
            abandon_status = Preserve_trx_transfer_status::CORRUPT;
            break;
          case Preserve_trx_transfer_epoch_terminal_outcome::NOT_COMMITTED:
          case Preserve_trx_transfer_epoch_terminal_outcome::ABANDONING:
            abandon_status = Preserve_trx_transfer_status::NOT_COMMITTED;
            break;
          case Preserve_trx_transfer_epoch_terminal_outcome::EPOCH_NOT_FOUND:
            break;
        }
        if (transfer_status_has_authenticated_ack(abandon_status)) {
          const Preserve_trx_transfer_status ack_status =
              send_receiver_authenticated_ack(&ack_context, abandon_status,
                                              false);
          if (ack_status != Preserve_trx_transfer_status::OK) {
            signal_transfer_dispatch_error(thd, ack_status);
          }
          return;
        }
        identity_status = abandon_status;
      }
    } else if (identity_status == Preserve_trx_transfer_status::OK) {
      identity_status = default_receiver_registry().validate_online_epoch(
          epoch_id, authenticated_principal, receiver_process_nonce,
          &ack_context.accepted_terminal_status_retention_us);
    }
    if (identity_status != Preserve_trx_transfer_status::OK) {
      LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
             ("PRESERVE: standby transfer receiver frame failed status=" +
              transfer_status_name(identity_status) +
              " stage=identity payload_bytes=" +
              std::to_string(raw_packet_length)).c_str());
      signal_transfer_dispatch_error(thd, identity_status);
      return;
    }
    uint64_t ignored_max_inflight_bytes = 0;
    if (!default_receiver_registry().online_epoch_runtime_policy(
            epoch_id, &receiver_runtime_policy,
            &ignored_max_inflight_bytes)) {
      signal_transfer_dispatch_error(
          thd, Preserve_trx_transfer_status::CORRUPT);
      return;
    }
  }
  auto store = create_preserved_trx_process_local_store(preserve_dir);
  Preserve_trx_transfer_status status = identity_decode_status;
  if (status == Preserve_trx_transfer_status::OK &&
      (is_batch || receiver_frame_is_sequence_tracked(identity_frame.type))) {
    if (!is_batch) {
      decoded_memory = acquire_transfer_decode_memory_lease(encoded_frame,
          3ULL * encoded_frame.size() + sizeof(Preserve_trx_transfer_frame));
      if (!decoded_memory.acquired()) {
        LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
               "PRESERVE: standby transfer receiver frame failed "
               "status=RESOURCE_EXHAUSTED stage=decode_memory");
        signal_transfer_dispatch_error(thd, Preserve_trx_transfer_status::RESOURCE_EXHAUSTED);
        return;
      }
      decoded_frames.push_back(std::move(identity_frame));
    }
    status = handle_decoded_receiver_payload_batch(
        preserve_dir, decoded_frames, &store.store(),
        &default_receiver_registry(), transfer_token_retention_timeout_seconds(),
        receiver_runtime_policy.receiver_workers, nullptr,
        send_receiver_admission_ack, &ack_context,
        send_receiver_commit_accepted_ack, &ack_context);
  } else if (status == Preserve_trx_transfer_status::OK) {
    status = preserve_trx_transfer_apply_receiver_frame(
        preserve_dir, identity_frame, &store.store(),
        &default_receiver_registry(), transfer_token_retention_timeout_seconds(),
        nullptr);
  }
  if (ack_context.commit_ack_pending) {
    const auto commit_status = preserve_trx_transfer_query_epoch_commit_status(
        preserve_dir, ack_context.verified_epoch_id);
    if (transfer_status_is_committed_outcome(commit_status)) {
      status = send_receiver_authenticated_ack(&ack_context, commit_status, true);
    } else if (status == Preserve_trx_transfer_status::OK) {
      status = commit_status == Preserve_trx_transfer_status::OK
                   ? Preserve_trx_transfer_status::CORRUPT : commit_status;
    }
  }
  if (status != Preserve_trx_transfer_status::OK) {
    const Preserve_trx_transfer_receiver_status_counts receiver_counts =
        default_receiver_registry().status_counts();
    const std::string frame_type =
        identity_frame_decoded
            ? std::to_string(static_cast<int>(identity_frame.type))
            : "decode_failed";
    const std::string message =
        "PRESERVE: standby transfer receiver frame failed status=" +
        transfer_status_name(status) + " frame_type=" + frame_type +
        " payload_bytes=" + std::to_string(raw_packet_length) +
        " last_failed_token=" +
        std::to_string(receiver_counts.last_failed_token) +
        " last_failed_reason=" + receiver_counts.last_failed_reason;
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
  }
  if (ack_context.ack_sent) return;
  signal_transfer_dispatch_error(thd, status);
}

Preserve_snapshot_status Preserve_trx_local_carrier_artifact_sink::publish_bundle(
    Preserved_trx_bundle bundle, uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata,
    bool *durable_snapshot_may_exist,
    Preserve_snapshot_delete_status *write_failure_delete_status,
    Preserved_trx_store_write_stats *write_stats) {
  if (m_store == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  return m_store->write(std::move(bundle), timeout_seconds, written_metadata,
                        durable_snapshot_may_exist,
                        write_failure_delete_status, write_stats);
}

Preserve_snapshot_status preserve_trx_transfer_capture_deferred_candidate(
    const std::string &epoch_id, uint64_t transfer_token,
    Preserved_trx_bundle bundle, uint64_t timeout_seconds,
    const Preserve_trx_resurrection_index_entry *resurrection_entry,
    Preserve_trx_deferred_transfer_candidate *candidate,
    Preserve_snapshot_metadata *written_metadata) {
  if (candidate == nullptr || candidate->captured || transfer_token == 0 ||
      timeout_seconds == 0 || epoch_id.empty()) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }
  if (!preserve_trx_is_enabled()) return Preserve_snapshot_status::UNSUPPORTED;

  const uint64_t created_at_us = my_micro_time();
  constexpr uint64_t kMicrosecondsPerSecond = 1000000ULL;
  if (timeout_seconds >
      (std::numeric_limits<uint64_t>::max() - created_at_us) /
          kMicrosecondsPerSecond) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }
  bundle.metadata.created_at_us = created_at_us;
  bundle.metadata.expires_at_us =
      created_at_us + timeout_seconds * kMicrosecondsPerSecond;

  candidate->epoch_id = epoch_id;
  candidate->transfer_token = transfer_token;
  candidate->timeout_seconds = timeout_seconds;
  candidate->bundle = std::move(bundle);
  if (resurrection_entry != nullptr) {
    candidate->resurrection_entry = *resurrection_entry;
    candidate->has_resurrection_entry = true;
  }
  candidate->captured = true;
  if (written_metadata != nullptr) *written_metadata = candidate->bundle.metadata;
  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status Preserve_trx_transfer_artifact_sink::publish_bundle(
    Preserved_trx_bundle bundle, uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata,
    bool *durable_snapshot_may_exist,
    Preserve_snapshot_delete_status *write_failure_delete_status,
    Preserved_trx_store_write_stats *write_stats) {
  (void)write_failure_delete_status;
  (void)write_stats;
  if (m_frame_sink == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  if (!preserve_trx_is_enabled()) return Preserve_snapshot_status::UNSUPPORTED;

  const uint64_t created_at_us = my_micro_time();
  constexpr uint64_t kMicrosecondsPerSecond = 1000000ULL;
  if (timeout_seconds == 0 ||
      timeout_seconds >
          (std::numeric_limits<uint64_t>::max() - created_at_us) /
              kMicrosecondsPerSecond) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }
  bundle.metadata.created_at_us = created_at_us;
  bundle.metadata.expires_at_us =
      created_at_us + timeout_seconds * kMicrosecondsPerSecond;

  const Preserve_trx_transfer_status materialize_status =
      materialize_prebuilt_external_blobs_for_transfer(m_preserve_dir, &bundle);
  if (materialize_status != Preserve_trx_transfer_status::OK) {
    const std::string message =
        "PRESERVE: standby transfer prebuilt blob materialize failed status=" +
        transfer_status_name(materialize_status) + " epoch=" + m_epoch_id +
        " token=" + std::to_string(m_transfer_token);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return map_transfer_status_to_snapshot(materialize_status);
  }

  const Preserve_snapshot_metadata metadata = bundle.metadata;
  const Preserve_trx_transfer_status status =
      preserve_trx_transfer_send_bundle_frames(
          m_epoch_id, bundle, m_transfer_token, m_chunk_bytes, m_frame_sink,
          nullptr,
          m_has_resurrection_entry ? &m_resurrection_entry : nullptr);
  if (status != Preserve_trx_transfer_status::OK) {
    const std::string message =
        "PRESERVE: standby transfer publish failed status=" +
        transfer_status_name(status) + " epoch=" + m_epoch_id +
        " token=" + std::to_string(m_transfer_token);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return map_transfer_status_to_snapshot(status);
  }
  if (written_metadata != nullptr) *written_metadata = metadata;
  if (durable_snapshot_may_exist != nullptr) *durable_snapshot_may_exist = true;
  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status
Preserve_trx_transfer_session_artifact_sink::publish_bundle(
    Preserved_trx_bundle bundle, uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata,
    bool *durable_snapshot_may_exist,
    Preserve_snapshot_delete_status *write_failure_delete_status,
    Preserved_trx_store_write_stats *write_stats) {
  (void)write_failure_delete_status;
  (void)write_stats;
  if (m_session == nullptr || m_transfer_token == 0) {
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }
  if (!preserve_trx_is_enabled()) return Preserve_snapshot_status::UNSUPPORTED;

  const uint64_t created_at_us = my_micro_time();
  constexpr uint64_t kMicrosecondsPerSecond = 1000000ULL;
  if (timeout_seconds == 0 ||
      timeout_seconds >
          (std::numeric_limits<uint64_t>::max() - created_at_us) /
              kMicrosecondsPerSecond) {
    (void)m_session->abort_token(m_transfer_token,
                                 "source_session_invalid_timeout");
    return Preserve_snapshot_status::INVALID_ARGUMENT;
  }
  bundle.metadata.created_at_us = created_at_us;
  bundle.metadata.expires_at_us =
      created_at_us + timeout_seconds * kMicrosecondsPerSecond;

  std::set<std::string> presealed_external_objects;
  auto resource_status = preserve_trx_temp_transfer_stream(m_session, m_transfer_token,
      m_preserve_dir, bundle.metadata.temp_table_manifest_payload);
  if (resource_status == Preserve_trx_transfer_status::OK) {
    resource_status = preserve_trx_result_transfer_stream(m_session, m_transfer_token,
        bundle.metadata.cursor_manifest_payload, bundle.source_cursor_results.get());
  }
  Preserve_memory_lease result_descriptors_memory;
  std::vector<Preserve_trx_transfer_object_descriptor> resource_objects;
  if (resource_status == Preserve_trx_transfer_status::OK)
    resource_status = preserve_trx_temp_transfer_descriptors(std::to_string(m_transfer_token),
        bundle.metadata.temp_table_manifest_payload, &resource_objects);
  if (resource_status == Preserve_trx_transfer_status::OK) {
    for (const auto &object : resource_objects) presealed_external_objects.insert(object.object_id);
    resource_status = preserve_trx_result_transfer_descriptors(
        bundle.metadata.token, bundle.metadata.cursor_manifest_payload,
        &resource_objects, &result_descriptors_memory);
  }
  if (resource_status != Preserve_trx_transfer_status::OK) {
    (void)m_session->abort_token(m_transfer_token, "source_session_resource_stream_failed");
    return map_transfer_status_to_snapshot(resource_status);
  }
  for (const auto &object : resource_objects) presealed_external_objects.insert(object.object_id);
  for (const Preserved_trx_external_blob &blob : bundle.external_blobs) {
    const Preserve_trx_transfer_object_descriptor descriptor =
        transfer_external_blob_descriptor(m_session->epoch_id(),
                                          m_transfer_token, blob);
    if (m_session->object_presealed_for_token(m_transfer_token, descriptor)) {
      presealed_external_objects.insert(blob.name);
      continue;
    }
    if (!blob.prebuilt) continue;
    (void)m_session->abort_token(
        m_transfer_token, "source_session_prebuilt_not_presealed");
    const std::string message =
        "PRESERVE: standby streaming transfer rejected unsealed prebuilt blob "
        "token=" +
        std::to_string(m_transfer_token) + " object=" + blob.name;
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return Preserve_snapshot_status::UNSUPPORTED;
  }

  const Preserve_trx_transfer_status materialize_status =
      materialize_prebuilt_external_blobs_for_transfer(
          m_preserve_dir, &bundle, &presealed_external_objects);
  if (materialize_status != Preserve_trx_transfer_status::OK) {
    (void)m_session->abort_token(
        m_transfer_token, "source_session_prebuilt_materialize_failed:" +
                              transfer_status_name(materialize_status));
    const std::string message =
        "PRESERVE: standby streaming transfer materialize failed status=" +
        transfer_status_name(materialize_status) +
        " token=" + std::to_string(m_transfer_token);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return map_transfer_status_to_snapshot(materialize_status);
  }

  const Preserve_snapshot_metadata metadata = bundle.metadata;
  Preserve_trx_transfer_manifest manifest;
  std::vector<Preserve_trx_transfer_object_payload> objects;
  const char *publish_step = "build_portable_objects";
  std::string publish_object_id;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_build_portable_objects_impl(
          m_session->epoch_id(), bundle, m_transfer_token, &manifest, &objects,
          &presealed_external_objects,
          m_has_resurrection_entry ? &m_resurrection_entry : nullptr);
  if (status == Preserve_trx_transfer_status::OK) {
    publish_step = "send_token_objects_batch";
    status = m_session->send_token_objects_batch(
        manifest, objects, presealed_external_objects, m_queue_final_metadata);
  }
  if (status != Preserve_trx_transfer_status::OK) {
    (void)m_session->abort_token(
        m_transfer_token,
        "source_session_publish_failed:" + transfer_status_name(status));
    const std::string message =
        "PRESERVE: standby streaming transfer publish failed status=" +
        transfer_status_name(status) +
        " step=" + std::string(publish_step) +
        " object=" + publish_object_id +
        " token=" + std::to_string(m_transfer_token);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return map_transfer_status_to_snapshot(status);
  }
  if (written_metadata != nullptr) *written_metadata = metadata;
  if (durable_snapshot_may_exist != nullptr) *durable_snapshot_may_exist = true;
  return Preserve_snapshot_status::OK;
}

Preserve_snapshot_status
Preserve_trx_standby_pending_artifact_sink::publish_bundle(
    Preserved_trx_bundle bundle, uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata,
    bool *durable_snapshot_may_exist,
    Preserve_snapshot_delete_status *write_failure_delete_status,
    Preserved_trx_store_write_stats *write_stats) {
  if (m_store == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  if (!preserve_trx_is_enabled()) return Preserve_snapshot_status::UNSUPPORTED;
  return m_store->write_standby_pending(
      std::move(bundle), timeout_seconds, written_metadata,
      durable_snapshot_may_exist, write_failure_delete_status, write_stats);
}

Preserve_snapshot_status preserve_trx_make_artifact_sink_for_decision(
    Preserve_trx_transfer_artifact_decision decision, Preserved_trx_store *store,
    const std::string &epoch_id, uint64_t transfer_token,
    uint32_t chunk_bytes, Preserve_trx_transfer_encoded_frame_sink *frame_sink,
    std::unique_ptr<Preserve_trx_artifact_sink> *sink,
    Preserve_trx_transfer_source_epoch_session *source_epoch_session,
    const std::string &preserve_dir,
    const Preserve_trx_resurrection_index_entry *resurrection_entry) {
  if (sink == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
  sink->reset();

  switch (decision) {
    case Preserve_trx_transfer_artifact_decision::LOCAL_CARRIER:
      if (store == nullptr) return Preserve_snapshot_status::INVALID_ARGUMENT;
      sink->reset(new Preserve_trx_local_carrier_artifact_sink(store));
      return Preserve_snapshot_status::OK;
    case Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE:
      if (source_epoch_session != nullptr) {
        if (transfer_token == 0) {
          return Preserve_snapshot_status::INVALID_ARGUMENT;
        }
        sink->reset(new Preserve_trx_transfer_session_artifact_sink(
            source_epoch_session, transfer_token, preserve_dir, true,
            resurrection_entry));
        return Preserve_snapshot_status::OK;
      }
      if (frame_sink == nullptr || transfer_token == 0 || epoch_id.empty() ||
          chunk_bytes == 0) {
        return Preserve_snapshot_status::INVALID_ARGUMENT;
      }
      sink->reset(new Preserve_trx_transfer_artifact_sink(
          epoch_id, transfer_token, chunk_bytes, frame_sink, preserve_dir,
          resurrection_entry));
      return Preserve_snapshot_status::OK;
    case Preserve_trx_transfer_artifact_decision::UNSUPPORTED:
      return Preserve_snapshot_status::UNSUPPORTED;
  }
  return Preserve_snapshot_status::UNSUPPORTED;
}
