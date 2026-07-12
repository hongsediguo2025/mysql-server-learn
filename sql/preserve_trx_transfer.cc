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

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
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
#include <openssl/hmac.h>
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
#include "mysql/components/services/log_builtins.h"
#include "sql_common.h"
#include "sql/mysqld.h"
#include "sql/log.h"
#include "sql/current_thd.h"
#include "sql/binlog_preserve_prepared.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_carrier_file.h"
#include "sql/preserve_trx_promotion.h"
#include "sql/preserve_trx_promotion_prepared.h"
#include "sql/preserve_trx_resource.h"
#include "sql/protocol_classic.h"
#include "sql/rpl_channel_credentials.h"
#include "sql/sql_class.h"
#include "sql/ssl_acceptor_context_status.h"
#include "scope_guard.h"
#include "storage/innobase/include/trx0preserve.h"

bool preserve_trx_transfer_receiver_enable = false;
char *preserve_trx_transfer_allowed_source_uuid = nullptr;
char *preserve_trx_transfer_target_server_uuid = nullptr;
char *preserve_trx_transfer_target_host = nullptr;
uint preserve_trx_transfer_target_port = 0;
char *preserve_trx_transfer_target_socket = nullptr;
char *preserve_trx_transfer_target_user = nullptr;
char *preserve_trx_transfer_credential_name = nullptr;
char *preserve_trx_transfer_credential_secret_file = nullptr;
ulong preserve_trx_transfer_artifact_mode =
    PRESERVE_TRX_TRANSFER_ARTIFACT_LOCAL_CARRIER;
uint preserve_trx_transfer_receiver_workers = 8;
uint preserve_trx_transfer_chunk_bytes = 1048576;
ulonglong preserve_trx_transfer_max_inflight_bytes = 1073741824ULL;
uint preserve_trx_transfer_commit_timeout_ms = 30000;
ulonglong preserve_trx_transfer_phase1_batch_bytes = 4194304ULL;
uint preserve_trx_transfer_phase1_batch_linger_ms = 20;

static std::atomic<bool> g_transfer_spool_short_write_for_unit_test{false};
static std::atomic<bool> g_transfer_spool_rollback_failure_for_unit_test{false};
static std::atomic<uint> g_transfer_staging_cleanup_failures_for_unit_test{0};

static std::atomic<uint64_t> g_receiver_auto_prewarm_tokens{0};
static std::atomic<uint64_t> g_receiver_auto_prewarm_ready_tokens{0};
static std::atomic<uint64_t> g_receiver_auto_prewarm_not_ready_tokens{0};
static std::atomic<uint64_t> g_receiver_auto_prewarm_last_status{0};
static std::atomic<uint64_t> g_receiver_ready_monotonic_us{0};
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
static std::atomic<uint64_t> g_receiver_binlog_object_prewarm_first_start_us{0};
static std::atomic<uint64_t> g_receiver_binlog_object_prewarm_last_end_us{0};
static std::atomic<uint64_t> g_receiver_committed_epoch_fallback_count{0};
static std::atomic<uint64_t> g_receiver_staged_token_publish_us{0};
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
static std::atomic<uint64_t> g_transfer_phase2_receiver_prewarm_wait_us{0};
static std::atomic<uint64_t> g_transfer_phase2_final_metadata_fsync_count{0};
static std::atomic<uint64_t> g_transfer_phase2_final_metadata_ack_us{0};
static std::atomic<uint64_t> g_transfer_phase1_business_enqueue_block_us{0};
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
static std::atomic<uint64_t> g_receiver_final_metadata_durable_us{0};
static std::atomic<uint64_t> g_receiver_ready_after_final_metadata_us{0};
static std::atomic<uint64_t> g_receiver_final_spool_ack_monotonic_us{0};
static std::atomic<uint64_t> g_receiver_ready_after_final_spool_ack_us{0};
static std::atomic<uint64_t> g_receiver_object_prewarm_start_monotonic_us{0};
static std::atomic<uint64_t> g_receiver_prewarm_backlog_at_phase2_end{0};
static std::mutex g_receiver_seal_prewarm_state_mutex;
static std::map<std::pair<std::string, uint64_t>,
                Preserve_trx_promotion_adopt_status>
    g_receiver_seal_prewarm_state;
struct Receiver_epoch_ready_state {
  std::set<uint64_t> fact_tokens;
  std::set<uint64_t> ready_tokens;
  std::map<uint64_t, size_t> fact_token_indexes;
  std::shared_ptr<const Preserve_trx_transfer_epoch_fact> fact;
  size_t ready_fact_token_count{0};
  bool fact_loaded{false};
  bool binding{false};
  bool bound{false};
  uint64_t binding_generation{0};
};
static std::mutex g_receiver_ready_epoch_mutex;
static std::map<std::pair<std::string, std::string>, Receiver_epoch_ready_state>
    g_receiver_ready_epoch_state;
static std::atomic<bool> g_receiver_epoch_bind_bad_alloc_for_unit_test{false};

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

  bool operator<(const Receiver_object_prewarm_key &rhs) const {
    return std::tie(root_dir, epoch_id, token, object_id, digest) <
           std::tie(rhs.root_dir, rhs.epoch_id, rhs.token, rhs.object_id,
                    rhs.digest);
  }
};
struct Receiver_object_prewarm_proof {
  bool record_lock_object{false};
  uint64_t page_count{0};
  uint64_t resident_pages{0};
  uint64_t cold_gets{0};
  uint64_t bitmap_pages{0};
  uint64_t bitmap_bits{0};
};
struct Receiver_record_lock_prepared {
  std::unique_ptr<lock_preserve_metadata_plan_t> plan;
  lock_preserve_record_lock_metadata_facts_t facts;
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
using Receiver_strict_token_key =
    std::tuple<std::string, std::string, uint64_t>;
static std::mutex g_receiver_strict_record_lock_facts_mutex;
static std::map<Receiver_strict_token_key, Receiver_strict_record_lock_facts>
    g_receiver_strict_record_lock_facts;
static std::mutex g_receiver_strict_binlog_facts_mutex;
static std::map<Receiver_strict_token_key, Receiver_strict_binlog_facts>
    g_receiver_strict_binlog_facts;

static void erase_receiver_strict_record_lock_state(
    const std::string &root_dir, const std::string &epoch_id, uint64_t token) {
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
  std::lock_guard<std::mutex> binlog_guard(
      g_receiver_strict_binlog_facts_mutex);
  g_receiver_strict_binlog_facts.erase({root_dir, epoch_id, token});
}

bool receiver_record_lock_proof_gate_ready(
    const Receiver_object_prewarm_proof &proof) {
  return proof.record_lock_object && proof.page_count == proof.resident_pages &&
         proof.cold_gets == 0;
}

bool receiver_record_lock_proof_is_improvement(
    const Receiver_object_prewarm_proof &candidate,
    const Receiver_object_prewarm_proof &current) {
  if (!candidate.record_lock_object || !current.record_lock_object ||
      candidate.page_count != current.page_count) {
    return false;
  }
  return candidate.resident_pages > current.resident_pages ||
         candidate.cold_gets < current.cold_gets;
}

static constexpr size_t kReceiverProjectionLockShardCount = 4096;
static std::array<std::mutex, kReceiverProjectionLockShardCount>
    g_receiver_standby_publish_mutexes;
static std::mutex g_receiver_standby_projection_key_mutex;
static std::set<std::string> g_receiver_standby_projection_key_ready_roots;
static constexpr std::array<uint64_t, 13> kProjectionPublishP95BucketsUs{
    100, 250, 500, 1000, 2000, 5000, 10000, 25000, 50000, 100000, 250000,
    500000, 1000000};
static constexpr uint64_t kReceiverRecordLockResidencyPollIntervalUs = 1000;
static constexpr uint kReceiverRecordLockObjectProofRetryLimit = 16;
static std::array<std::atomic<uint64_t>, kProjectionPublishP95BucketsUs.size()>
    g_receiver_projection_publish_histogram{};
static thread_local bool g_receiver_frame_spool_disabled = false;
static thread_local bool g_receiver_frame_sequence_disabled = false;

static uint64_t transfer_monotonic_us() {
  using clock = std::chrono::steady_clock;
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          clock::now().time_since_epoch())
          .count());
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
    if (request.transfer_token == 0 || request.object_id.empty() ||
        request.warmcopy_id.empty() || request.warmcopy_epoch == 0 ||
        request.size == 0) {
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
    if (request.size > m_options.max_inflight_bytes) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    m_condition.wait(guard, [&]() {
      const uint64_t used_bytes = m_queued_bytes + m_in_flight_bytes;
      return m_status != Preserve_trx_transfer_status::OK || m_stop ||
             used_bytes <=
                 m_options.max_inflight_bytes - request.size;
    });
    if (m_status != Preserve_trx_transfer_status::OK) return m_status;
    if (m_stop) return Preserve_trx_transfer_status::UNSUPPORTED;

    m_queue.push_back({request, clock::now()});
    m_queued_bytes += request.size;
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
        Queued_request queued = std::move(m_queue.front());
        m_queue.pop_front();
        m_queued_bytes -= queued.request.size;
        batch_bytes += queued.request.size;
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
        status = Preserve_trx_transfer_status::UNSUPPORTED;
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

uint64_t preserve_trx_transfer_receiver_staged_token_publish_us_status() {
  return g_receiver_staged_token_publish_us.load();
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

uint64_t preserve_trx_transfer_phase2_receiver_prewarm_wait_us_status() {
  return g_transfer_phase2_receiver_prewarm_wait_us.load();
}

uint64_t preserve_trx_transfer_phase2_final_metadata_fsync_count_status() {
  return g_transfer_phase2_final_metadata_fsync_count.load();
}

uint64_t preserve_trx_transfer_phase2_final_metadata_ack_us_status() {
  return g_transfer_phase2_final_metadata_ack_us.load();
}

uint64_t preserve_trx_transfer_phase1_business_enqueue_block_us_status() {
  return g_transfer_phase1_business_enqueue_block_us.load();
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

static bool preserve_trx_transfer_source_endpoint_ready() {
  const bool has_tcp_host =
      preserve_trx_transfer_string_is_set(preserve_trx_transfer_target_host);
  const bool has_tcp_port = preserve_trx_transfer_target_port != 0;
  const bool has_tcp_target = has_tcp_host && has_tcp_port;
  const bool has_socket_target =
      preserve_trx_transfer_string_is_set(preserve_trx_transfer_target_socket);

  return preserve_trx_transfer_string_is_set(
             preserve_trx_transfer_target_server_uuid) &&
         preserve_trx_transfer_string_is_set(
             preserve_trx_transfer_target_user) &&
         preserve_trx_transfer_string_is_set(
             preserve_trx_transfer_credential_name) &&
         has_tcp_host == has_tcp_port &&
         (has_tcp_target != has_socket_target);
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

Preserve_trx_transfer_artifact_decision
preserve_trx_transfer_artifact_decision_for_request(
    Preserve_trx_delivery_mode delivery_mode) {
  const Preserve_trx_transfer_artifact_decision decision =
      preserve_trx_transfer_artifact_decision();
  if (decision != Preserve_trx_transfer_artifact_decision::STANDBY_TRANSFER_SAVE)
    return decision;
  return delivery_mode == Preserve_trx_delivery_mode::BATCH_MANAGER_DELIVERY
             ? decision
             : Preserve_trx_transfer_artifact_decision::UNSUPPORTED;
}

namespace {

constexpr char kTransferManifestMagic[] = {'P', 'T', 'R', 'X',
                                           'F', 'E', 'R', '1'};
constexpr size_t kTransferManifestMagicLength = sizeof(kTransferManifestMagic);
constexpr char kTransferObjectDescriptorMagic[] = {'P', 'T', 'R', 'X',
                                                   'O', 'B', 'J', '1'};
constexpr size_t kTransferObjectDescriptorMagicLength =
    sizeof(kTransferObjectDescriptorMagic);
constexpr char kTransferBundleMagic[] = {'P', 'T', 'R', 'X',
                                         'B', 'N', 'D', '1'};
constexpr size_t kTransferBundleMagicLength = sizeof(kTransferBundleMagic);
constexpr char kTransferFrameMagic[] = {'P', 'T', 'R', 'X',
                                        'F', 'R', 'M', '1'};
constexpr size_t kTransferFrameMagicLength = sizeof(kTransferFrameMagic);
constexpr char kTransferFrameBatchMagic[] = {'P', 'T', 'R', 'X',
                                             'F', 'B', 'T', '1'};
constexpr size_t kTransferFrameBatchMagicLength =
    sizeof(kTransferFrameBatchMagic);
constexpr char kReceiverFrameSpoolRecordMagic[] = {'P', 'T', 'R', 'X',
                                                   'S', 'P', 'L', '1'};
constexpr size_t kReceiverFrameSpoolRecordMagicLength =
    sizeof(kReceiverFrameSpoolRecordMagic);
constexpr uint32_t kMaxTransferManifestStringBytes = 1024 * 1024;
constexpr uint32_t kMaxTransferManifestObjects = 1024 * 1024;
constexpr uint32_t kMaxTransferChunkBytes = 1024 * 1024;

std::string bytes_to_lower_hex(const unsigned char *bytes, size_t length) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out(length * 2, '\0');
  for (size_t i = 0; i < length; ++i) {
    out[2 * i] = kHex[bytes[i] >> 4];
    out[2 * i + 1] = kHex[bytes[i] & 0x0f];
  }
  return out;
}

const std::string &transfer_source_incarnation_id() {
  static const std::string incarnation = []() {
    std::array<unsigned char, 16> random_bytes{};
    if (my_rand_buffer(random_bytes.data(), random_bytes.size()) != 0) {
      return std::string();
    }
    return bytes_to_lower_hex(random_bytes.data(), random_bytes.size());
  }();
  return incarnation;
}

std::string qualify_transfer_epoch_id(const std::string &epoch_id) {
  const std::string &incarnation = transfer_source_incarnation_id();
  if (incarnation.empty() || epoch_id.empty()) return {};
  return incarnation + "-" + epoch_id;
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

bool transfer_tls_identity_config_is_valid(bool unix_socket,
                                           const std::string &ssl_ca,
                                           const std::string &ssl_capath) {
  return unix_socket || !ssl_ca.empty() || !ssl_capath.empty();
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

Preserve_trx_transfer_status default_transfer_client_connect(
    const Preserve_trx_transfer_client_endpoint &endpoint, void **connection) {
  if (connection == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  *connection = nullptr;

  Transfer_resolved_credential credential;
  if (!resolve_transfer_credential(endpoint.credential_name, endpoint.user,
                                   &credential)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  MYSQL *mysql = mysql_init(nullptr);
  if (mysql == nullptr) return Preserve_trx_transfer_status::IO_ERROR;

  uint timeout_seconds = static_cast<uint>(std::max<uint64_t>(
      1, std::min<uint64_t>(
             (static_cast<uint64_t>(preserve_trx_transfer_commit_timeout_ms) +
              999) /
                 1000,
             UINT_MAX32)));
  mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, &timeout_seconds);
  mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, &timeout_seconds);
  mysql_options(mysql, MYSQL_OPT_WRITE_TIMEOUT, &timeout_seconds);

  if (!credential.auth_plugin.empty()) {
    mysql_options(mysql, MYSQL_DEFAULT_AUTH, credential.auth_plugin.c_str());
  }
  mysql_options4(mysql, MYSQL_OPT_CONNECT_ATTR_ADD, "program_name", "mysqld");
  mysql_options4(mysql, MYSQL_OPT_CONNECT_ATTR_ADD, "_client_role",
                 "preserve_trx_transfer_sender");

  const char *host = endpoint.host.empty()
                         ? (endpoint.socket.empty() ? nullptr : "localhost")
                         : endpoint.host.c_str();
  const char *socket =
      endpoint.socket.empty() ? nullptr : endpoint.socket.c_str();
  const bool tcp_configured = !endpoint.host.empty() || endpoint.port != 0;
  const bool unix_socket = socket != nullptr;
  if (tcp_configured == unix_socket ||
      (tcp_configured && (endpoint.host.empty() || endpoint.port == 0))) {
    mysql_close(mysql);
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  uint protocol =
      unix_socket ? MYSQL_PROTOCOL_SOCKET : MYSQL_PROTOCOL_TCP;
  if (mysql_options(mysql, MYSQL_OPT_PROTOCOL, &protocol) != 0) {
    mysql_close(mysql);
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  std::string ssl_ca;
  std::string ssl_capath;
  enum mysql_ssl_mode ssl_mode = SSL_MODE_DISABLED;
  if (!unix_socket) {
    if (!Ssl_mysql_main_status::get_ssl_ca_and_capath(&ssl_ca, &ssl_capath) ||
        !transfer_tls_identity_config_is_valid(false, ssl_ca, ssl_capath)) {
      mysql_close(mysql);
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    if ((!ssl_ca.empty() &&
         mysql_options(mysql, MYSQL_OPT_SSL_CA, ssl_ca.c_str()) != 0) ||
        (!ssl_capath.empty() &&
         mysql_options(mysql, MYSQL_OPT_SSL_CAPATH, ssl_capath.c_str()) != 0)) {
      mysql_close(mysql);
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    ssl_mode = SSL_MODE_VERIFY_IDENTITY;
  }
  if (mysql_options(mysql, MYSQL_OPT_SSL_MODE, &ssl_mode) != 0) {
    mysql_close(mysql);
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  Transfer_client_current_thd_guard current_thd_guard;
  if (mysql_real_connect(mysql, host, credential.user.c_str(),
                         credential.password.c_str(), nullptr, endpoint.port,
                         socket, 0) == nullptr) {
    const std::string message =
        "PRESERVE: standby transfer client connect failed errno=" +
        std::to_string(mysql_errno(mysql)) + " error=" + mysql_error(mysql);
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    mysql_close(mysql);
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  if (!unix_socket) {
    const char *cipher = mysql_get_ssl_cipher(mysql);
    if (cipher == nullptr || cipher[0] == '\0') {
      mysql_close(mysql);
      return Preserve_trx_transfer_status::IO_ERROR;
    }
  }

  Transfer_client_connection *client = new (std::nothrow) Transfer_client_connection;
  if (client == nullptr) {
    mysql_close(mysql);
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  client->mysql = mysql;
  *connection = client;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status default_transfer_client_send(
    void *connection, const std::string &encoded_frame) {
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
        preserve_trx_transfer_verify_frame_ack(
            info, transfer_source_incarnation_id(), encoded_frame, &ack);
    return ack_status == Preserve_trx_transfer_status::OK
               ? ack.status
               : Preserve_trx_transfer_status::ACK_UNCERTAIN;
  }
  const std::string message =
      "PRESERVE: standby transfer client send failed errno=" +
      std::to_string(mysql_errno(client->mysql)) + " error=" +
      mysql_error(client->mysql) + " frame_bytes=" +
      std::to_string(encoded_frame.length());
  LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
  return Preserve_trx_transfer_status::IO_ERROR;
}

void default_transfer_client_disconnect(void *connection) {
  Transfer_client_connection *client =
      static_cast<Transfer_client_connection *>(connection);
  if (client == nullptr) return;
  if (client->mysql != nullptr) mysql_close(client->mysql);
  delete client;
}

const Preserve_trx_transfer_client_ops kDefault_transfer_client_ops = {
    default_transfer_client_connect, default_transfer_client_send,
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

void append_context_component(std::string *out, const std::string &component) {
  out->append(component);
  out->push_back('\0');
}

std::array<unsigned char, kPreservedTrxSha256Length> digest_context_material(
    const std::string &label, const std::string &credential_name,
    const std::string &target_server_uuid, const std::string &secret) {
  std::string material;
  material.reserve(label.length() + credential_name.length() +
                   target_server_uuid.length() + secret.length() + 8);
  append_context_component(&material, label);
  append_context_component(&material, credential_name);
  append_context_component(&material, target_server_uuid);
  append_context_component(&material, secret);

  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  SHA256(reinterpret_cast<const unsigned char *>(material.data()),
         material.length(), digest.data());
  cleanse_transfer_secret(&material);
  return digest;
}

bool transfer_bundle_codec_context_from_config(
    Preserved_trx_codec_context *context) {
  if (context == nullptr ||
      !preserve_trx_transfer_string_is_set(
          preserve_trx_transfer_credential_name) ||
      !preserve_trx_transfer_string_is_set(
          preserve_trx_transfer_target_server_uuid)) {
    return false;
  }

  Transfer_resolved_credential credential;
  if (!resolve_transfer_credential(
          preserve_trx_transfer_credential_name,
          preserve_trx_transfer_string_is_set(preserve_trx_transfer_target_user)
              ? preserve_trx_transfer_target_user
              : "",
          &credential))
    return false;

  const std::string credential_name = preserve_trx_transfer_credential_name;
  const std::string target_server_uuid = preserve_trx_transfer_target_server_uuid;
  if (target_server_uuid.length() > 36) return false;

  context->hmac_key = digest_context_material(
      "preserve-trx-transfer-hmac-v1", credential_name, target_server_uuid,
      credential.password);
  context->datadir_fingerprint = digest_context_material(
      "preserve-trx-transfer-fingerprint-v1", credential_name,
      target_server_uuid, credential.password);
  context->server_uuid = target_server_uuid;
  return true;
}

bool transfer_bundle_codec_context(Preserved_trx_codec_context *context) {
  if (context == nullptr) return false;
  Preserve_trx_transfer_codec_context_provider provider =
      unit_codec_context_provider();
  if (provider != nullptr) return provider(context);
  return transfer_bundle_codec_context_from_config(context);
}

bool load_source_transfer_lsn_fact(uint64_t *source_prepare_lsn,
                                   uint64_t *source_epoch_commit_lsn) {
  if (source_prepare_lsn == nullptr || source_epoch_commit_lsn == nullptr) {
    return false;
  }

  Preserve_trx_transfer_source_lsn_provider provider =
      unit_source_lsn_provider();
  if (provider != nullptr) {
    return provider(source_prepare_lsn, source_epoch_commit_lsn) &&
           *source_prepare_lsn != 0 && *source_epoch_commit_lsn != 0;
  }

  const uint64_t lsn = trx_preserve_current_redo_lsn();
  if (lsn == 0) return false;
  *source_prepare_lsn = lsn;
  *source_epoch_commit_lsn = lsn;
  return true;
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
  if (preserve_trx_transfer_target_server_uuid != nullptr) {
    endpoint.target_server_uuid = preserve_trx_transfer_target_server_uuid;
  }
  if (preserve_trx_transfer_target_host != nullptr) {
    endpoint.host = preserve_trx_transfer_target_host;
  }
  endpoint.port = preserve_trx_transfer_target_port;
  if (preserve_trx_transfer_target_socket != nullptr) {
    endpoint.socket = preserve_trx_transfer_target_socket;
  }
  if (preserve_trx_transfer_target_user != nullptr) {
    endpoint.user = preserve_trx_transfer_target_user;
  }
  if (preserve_trx_transfer_credential_name != nullptr) {
    endpoint.credential_name = preserve_trx_transfer_credential_name;
  }
  return endpoint;
}

class Preserve_trx_transfer_client_frame_sink final
    : public Preserve_trx_transfer_encoded_frame_sink {
 public:
  Preserve_trx_transfer_client_frame_sink(
      Preserve_trx_transfer_client_endpoint endpoint,
      const Preserve_trx_transfer_client_ops *ops)
      : m_endpoint(std::move(endpoint)), m_ops(ops) {
    m_connections.resize(1, nullptr);
  }

  ~Preserve_trx_transfer_client_frame_sink() override {
    if (m_ops == nullptr || m_ops->disconnect == nullptr) return;
    for (void *connection : m_connections) {
      if (connection != nullptr) m_ops->disconnect(connection);
    }
  }

  Preserve_trx_transfer_status send_encoded_frame(
      const std::string &encoded_frame) override {
    if (m_ops == nullptr || m_ops->connect == nullptr ||
        m_ops->send_frame == nullptr) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    if (!m_uncertain_payload.empty() &&
        m_uncertain_payload != encoded_frame) {
      return Preserve_trx_transfer_status::ACK_UNCERTAIN;
    }
    const bool retrying_uncertain = !m_uncertain_payload.empty();
    const size_t connection_index = connection_index_for_frame(encoded_frame);
    void *&connection = m_connections[connection_index];
    if (connection == nullptr) {
      void *new_connection = nullptr;
      const Preserve_trx_transfer_status connect_status =
          m_ops->connect(m_endpoint, &new_connection);
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
      connection = new_connection;
    }
    Preserve_trx_transfer_status status =
        m_ops->send_frame(connection, encoded_frame);
    if (status != Preserve_trx_transfer_status::IO_ERROR &&
        status != Preserve_trx_transfer_status::ACK_UNCERTAIN) {
      if (status == Preserve_trx_transfer_status::OK) {
        m_uncertain_payload.clear();
      }
      return status;
    }

    /* An uncertain response retries the exact encoded payload and sequence. */
    m_ops->disconnect(connection);
    connection = nullptr;
    void *new_connection = nullptr;
    status = m_ops->connect(m_endpoint, &new_connection);
    if (status != Preserve_trx_transfer_status::OK ||
        new_connection == nullptr) {
      m_uncertain_payload = encoded_frame;
      return Preserve_trx_transfer_status::ACK_UNCERTAIN;
    }
    connection = new_connection;
    status = m_ops->send_frame(connection, encoded_frame);
    if (status == Preserve_trx_transfer_status::OK) {
      m_uncertain_payload.clear();
      return status;
    }
    if (status == Preserve_trx_transfer_status::IO_ERROR ||
        status == Preserve_trx_transfer_status::ACK_UNCERTAIN) {
      m_uncertain_payload = encoded_frame;
      return Preserve_trx_transfer_status::ACK_UNCERTAIN;
    }
    return status;
  }

 private:
  size_t connection_index_for_frame(const std::string &encoded_frame) {
    /*
      Receiver admission currently enforces a single monotonically increasing
      frame sequence for the whole epoch. Splitting OBJECT_CHUNK frames onto
      data connections can let later control frames arrive first, so keep this
      transport ordered until the receiver has a durable reorder buffer.
    */
    (void)encoded_frame;
    return 0;
  }

  Preserve_trx_transfer_client_endpoint m_endpoint;
  const Preserve_trx_transfer_client_ops *m_ops{nullptr};
  std::vector<void *> m_connections;
  std::string m_uncertain_payload;
};

std::string normalize_dir(const std::string &dir) {
  if (dir.empty()) return dir;
  if (dir.back() == FN_LIBCHAR) return dir;
  return dir + FN_LIBCHAR;
}

std::string transfer_default_preserve_dir() {
  const char *datadir =
      mysql_real_data_home_ptr != nullptr ? mysql_real_data_home_ptr
                                          : mysql_real_data_home;
  return normalize_dir(normalize_dir(std::string(datadir)) + "preserve");
}

std::string join_path(const std::string &dir, const std::string &name) {
  return normalize_dir(dir) + name;
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

bool fsync_transfer_directory(const std::string &dir) {
  File fd = my_open(dir.c_str(), O_RDONLY, MYF(0));
  if (fd < 0) return true;
  const bool error = my_sync(fd, MYF(0)) != 0;
  if (my_close(fd, MYF(0))) return true;
  return error;
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

std::string transfer_receiver_frame_spool_path(const std::string &root_dir,
                                               const std::string &epoch_id) {
  return join_path(transfer_epoch_dir_for_epoch(root_dir, epoch_id),
                   "receiver.frames");
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

  for (const std::string &path :
       {transfer_object_path(root_dir, manifest, object),
        transfer_object_range_path(root_dir, manifest, object)}) {
    if (my_delete(path.c_str(), MYF(0)) != 0 && my_errno() != ENOENT) {
      return Preserve_trx_transfer_status::IO_ERROR;
    }
  }
  return Preserve_trx_transfer_status::OK;
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

Preserve_trx_transfer_status validate_manifest_components(
    const Preserve_trx_transfer_manifest &manifest,
    bool decoded_remote_manifest) {
  if (!transfer_component_safe(manifest.epoch_id) ||
      !transfer_component_safe(manifest.source_server_uuid) ||
      !transfer_component_safe(manifest.target_server_uuid) ||
      manifest.token == 0 ||
      manifest.objects.size() > kMaxTransferManifestObjects) {
    return decoded_remote_manifest ? Preserve_trx_transfer_status::CORRUPT
                                   : Preserve_trx_transfer_status::
                                         INVALID_ARGUMENT;
  }

  std::set<std::string> object_ids;
  for (const Preserve_trx_transfer_object_descriptor &object :
       manifest.objects) {
    if (!transfer_component_safe(object.object_id) ||
        !object_ids.insert(object.object_id).second) {
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
    const std::string &path, uint64_t offset, size_t length,
    std::string *existing) {
  if (existing == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  existing->clear();
  if (length == 0) return Preserve_trx_transfer_status::OK;
  File file = my_open(path.c_str(), O_RDONLY, MYF(0));
  if (file < 0) return Preserve_trx_transfer_status::IO_ERROR;
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
  if (my_close(file, MYF(0))) error = true;
  return error ? Preserve_trx_transfer_status::IO_ERROR
               : Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status write_chunk_to_file(const std::string &path,
                                                 uint64_t offset,
                                                 const std::string &payload) {
  File file = my_open(path.c_str(), O_RDWR, MYF(0));
  if (file < 0) {
    file = my_create(path.c_str(), 0600, O_RDWR | O_CREAT | O_EXCL, MYF(0));
    if (file < 0) {
      if (my_errno() == EEXIST) {
        file = my_open(path.c_str(), O_RDWR, MYF(0));
      }
      if (file < 0) return Preserve_trx_transfer_status::IO_ERROR;
    }
  }

  bool error =
      my_seek(file, offset, MY_SEEK_SET, MYF(0)) == MY_FILEPOS_ERROR ||
      (!payload.empty() &&
       my_write(file, reinterpret_cast<const unsigned char *>(payload.data()),
                payload.length(), MYF(0)) != payload.length());
  if (my_close(file, MYF(0))) error = true;
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
  return "PTRXFER_COMMIT\n" + manifest.epoch_id + "\n" +
         manifest.source_server_uuid + "\n" + manifest.target_server_uuid +
         "\n";
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
  File file =
      my_create(tmp_path.c_str(), 0600, O_WRONLY | O_CREAT | O_EXCL, MYF(0));
  if (file < 0) return Preserve_trx_transfer_status::IO_ERROR;

  bool error =
      my_write(file, reinterpret_cast<const unsigned char *>(payload.data()),
               payload.length(), MYF(0)) != payload.length() ||
      my_sync(file, MYF(0)) != 0;
  if (my_close(file, MYF(0))) error = true;
  if (!error && my_rename(tmp_path.c_str(), final_path.c_str(), MYF(0))) {
    if (file_exists(final_path)) return Preserve_trx_transfer_status::OK;
    error = true;
  }
  if (!error && fsync_transfer_directory(transfer_epoch_dir(root_dir, manifest)))
    error = true;
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

bool decode_declare_token_reason(const std::string &reason,
                                 std::string *source_server_uuid,
                                 std::string *target_server_uuid) {
  if (source_server_uuid == nullptr || target_server_uuid == nullptr)
    return false;
  const size_t newline = reason.find('\n');
  if (newline == std::string::npos ||
      reason.find('\n', newline + 1) != std::string::npos) {
    return false;
  }
  const std::string source = reason.substr(0, newline);
  const std::string target = reason.substr(newline + 1);
  if (!transfer_component_safe(source) || !transfer_component_safe(target)) {
    return false;
  }
  *source_server_uuid = source;
  *target_server_uuid = target;
  return true;
}

std::string encode_declare_token_reason(
    const std::string &source_server_uuid,
    const std::string &target_server_uuid) {
  return source_server_uuid + "\n" + target_server_uuid;
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

bool receiver_frame_should_spool(Preserve_trx_transfer_frame_type type) {
  switch (type) {
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
      return false;
  }
  return false;
}

Preserve_trx_transfer_status append_receiver_frame_spool_record(
    const std::string &root_dir, const Preserve_trx_transfer_frame &frame) {
  if (root_dir.empty() || !transfer_component_safe(frame.epoch_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::string encoded_frame;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_encode_frame(frame, &encoded_frame);
  if (status != Preserve_trx_transfer_status::OK) return status;

  const std::string transfer_root = join_path(root_dir, ".transfer");
  const std::string epoch_dir =
      transfer_epoch_dir_for_epoch(root_dir, frame.epoch_id);
  if (ensure_dir_exists(transfer_root) || ensure_dir_exists(epoch_dir)) {
    return Preserve_trx_transfer_status::IO_ERROR;
  }

  std::string record;
  record.append(kReceiverFrameSpoolRecordMagic,
                kReceiverFrameSpoolRecordMagicLength);
  append_u64(&record, encoded_frame.length());
  const std::array<unsigned char, kPreservedTrxSha256Length> digest =
      sha256_digest(encoded_frame);
  record.append(reinterpret_cast<const char *>(digest.data()), digest.size());
  record.append(encoded_frame);

  const std::string spool_path =
      transfer_receiver_frame_spool_path(root_dir, frame.epoch_id);
  File file = my_open(spool_path.c_str(), O_WRONLY | O_APPEND, MYF(0));
  if (file < 0) {
    file = my_create(spool_path.c_str(), 0600,
                     O_WRONLY | O_CREAT | O_EXCL, MYF(0));
    if (file < 0 && my_errno() == EEXIST) {
      file = my_open(spool_path.c_str(), O_WRONLY | O_APPEND, MYF(0));
    }
    if (file < 0) return Preserve_trx_transfer_status::IO_ERROR;
  }

  const my_off_t original_length = my_seek(file, 0, MY_SEEK_END, MYF(0));
  if (original_length == MY_FILEPOS_ERROR) {
    (void)my_close(file, MYF(0));
    return Preserve_trx_transfer_status::IO_ERROR;
  }
  size_t write_length = record.length();
  const bool inject_short_write =
      g_transfer_spool_short_write_for_unit_test.load() && write_length > 1;
  if (inject_short_write) write_length /= 2;
  const bool write_error =
      my_write(file, reinterpret_cast<const unsigned char *>(record.data()),
               write_length, MYF(0)) != write_length ||
      inject_short_write;
  bool rollback_confirmed = !write_error;
  if (write_error) {
    rollback_confirmed =
        !g_transfer_spool_rollback_failure_for_unit_test.load() &&
        my_chsize(file, original_length, 0, MYF(0)) == 0;
  }
  const bool close_error = my_close(file, MYF(0)) != 0;
  if ((write_error && !rollback_confirmed) || close_error) {
    File rollback_file = my_open(spool_path.c_str(), O_WRONLY, MYF(0));
    if (rollback_file >= 0) {
      const bool truncate_ok =
          !g_transfer_spool_rollback_failure_for_unit_test.load() &&
          my_chsize(rollback_file, original_length, 0, MYF(0)) == 0;
      const bool rollback_close_ok = my_close(rollback_file, MYF(0)) == 0;
      rollback_confirmed = truncate_ok && rollback_close_ok;
    } else {
      rollback_confirmed = false;
    }
  }
  if (!write_error && !close_error) return Preserve_trx_transfer_status::OK;
  return rollback_confirmed ? Preserve_trx_transfer_status::IO_ERROR
                            : Preserve_trx_transfer_status::CORRUPT;
}

class Receiver_frame_spool_disable_guard {
 public:
  Receiver_frame_spool_disable_guard()
      : m_previous(g_receiver_frame_spool_disabled) {
    g_receiver_frame_spool_disabled = true;
  }

  ~Receiver_frame_spool_disable_guard() {
    g_receiver_frame_spool_disabled = m_previous;
  }

 private:
  bool m_previous;
};

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

Preserve_trx_transfer_status encode_transfer_object_descriptor(
    const Preserve_trx_transfer_object_descriptor &object,
    std::string *encoded) {
  if (encoded == nullptr || !transfer_component_safe(object.object_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::string out;
  out.append(kTransferObjectDescriptorMagic,
             kTransferObjectDescriptorMagicLength);
  if (append_string(&out, object.object_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  append_u16(&out, static_cast<uint16_t>(object.kind));
  append_u32(&out, object.flags);
  append_u64(&out, object.total_size);
  out.append(reinterpret_cast<const char *>(object.digest.data()),
             object.digest.size());
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

  Preserve_trx_transfer_object_descriptor parsed;
  uint16_t raw_kind = 0;
  const char *digest = nullptr;
  if (reader.read_string(&parsed.object_id) || reader.read_u16(&raw_kind) ||
      !object_kind_supported(raw_kind, &parsed.kind) ||
      reader.read_u32(&parsed.flags) || reader.read_u64(&parsed.total_size) ||
      reader.read_fixed(parsed.digest.size(), &digest) || !reader.eof() ||
      !transfer_component_safe(parsed.object_id)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  std::memcpy(parsed.digest.data(), digest, parsed.digest.size());
  *object = std::move(parsed);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status read_receiver_spooled_frames(
    const std::string &root_dir, const std::string &epoch_id,
    std::vector<Preserve_trx_transfer_frame> *frames) {
  if (frames == nullptr || root_dir.empty() ||
      !transfer_component_safe(epoch_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  frames->clear();

  std::string payload;
  Preserve_trx_transfer_status status = read_whole_file(
      transfer_receiver_frame_spool_path(root_dir, epoch_id), &payload);
  if (status != Preserve_trx_transfer_status::OK) return status;

  Manifest_reader reader(payload);
  constexpr size_t kReceiverFrameSpoolRecordHeaderLength =
      kReceiverFrameSpoolRecordMagicLength + sizeof(uint64_t) +
      kPreservedTrxSha256Length;
  while (!reader.eof()) {
    const char *magic = nullptr;
    uint64_t encoded_length = 0;
    const char *digest_bytes = nullptr;
    const char *encoded_bytes = nullptr;
    /*
      Receiver ACK means the frame append reached durable storage. During a
      receiver crash, only the last append may be torn; replay treats that tail
      as not yet arrived so the sender can retransmit it. A complete record
      with bad magic, digest, or decoded payload remains corrupt.
    */
    if (reader.remaining() < kReceiverFrameSpoolRecordHeaderLength) {
      return Preserve_trx_transfer_status::OK;
    }
    if (reader.read_fixed(kReceiverFrameSpoolRecordMagicLength, &magic) ||
        std::memcmp(magic, kReceiverFrameSpoolRecordMagic,
                    kReceiverFrameSpoolRecordMagicLength) != 0 ||
        reader.read_u64(&encoded_length) ||
        encoded_length > kMaxTransferManifestStringBytes ||
        encoded_length > std::numeric_limits<size_t>::max()) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    if (reader.remaining() <
        kPreservedTrxSha256Length + static_cast<size_t>(encoded_length)) {
      return Preserve_trx_transfer_status::OK;
    }
    if (reader.read_fixed(kPreservedTrxSha256Length, &digest_bytes) ||
        reader.read_fixed(static_cast<size_t>(encoded_length),
                          &encoded_bytes)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    const std::string encoded_frame(encoded_bytes,
                                    static_cast<size_t>(encoded_length));
    const std::array<unsigned char, kPreservedTrxSha256Length> digest =
        sha256_digest(encoded_frame);
    if (std::memcmp(digest.data(), digest_bytes, digest.size()) != 0) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    Preserve_trx_transfer_frame frame;
    status = preserve_trx_transfer_decode_frame(encoded_frame, &frame);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (frame.epoch_id != epoch_id || !receiver_frame_should_spool(frame.type)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    frames->push_back(std::move(frame));
  }
  return Preserve_trx_transfer_status::OK;
}

bool object_kind_supported(uint16_t raw_kind,
                           Preserve_trx_transfer_object_kind *kind) {
  if (kind == nullptr) return false;
  switch (static_cast<Preserve_trx_transfer_object_kind>(raw_kind)) {
    case Preserve_trx_transfer_object_kind::SNAPSHOT_BUNDLE:
    case Preserve_trx_transfer_object_kind::EXTERNAL_BLOB:
    case Preserve_trx_transfer_object_kind::TEMP_TABLE_SIDECAR:
      *kind = static_cast<Preserve_trx_transfer_object_kind>(raw_kind);
      return true;
  }
  return false;
}

bool frame_type_supported(uint16_t raw_type,
                          Preserve_trx_transfer_frame_type *type) {
  if (type == nullptr) return false;
  switch (static_cast<Preserve_trx_transfer_frame_type>(raw_type)) {
    case Preserve_trx_transfer_frame_type::BEGIN:
    case Preserve_trx_transfer_frame_type::OBJECT_CHUNK:
    case Preserve_trx_transfer_frame_type::SEAL_OBJECT:
    case Preserve_trx_transfer_frame_type::COMMIT_EPOCH:
    case Preserve_trx_transfer_frame_type::ABORT:
    case Preserve_trx_transfer_frame_type::PROMOTION_PREWARM_TOKEN:
    case Preserve_trx_transfer_frame_type::PROMOTION_GATE_EPOCH:
    case Preserve_trx_transfer_frame_type::DECLARE_TOKEN:
    case Preserve_trx_transfer_frame_type::DECLARE_OBJECT:
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

  const bool token_optional =
      frame.type == Preserve_trx_transfer_frame_type::PROMOTION_GATE_EPOCH;
  if (frame.protocol_version != kPreserveTrxTransferProtocolVersion ||
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

  switch (frame.type) {
    case Preserve_trx_transfer_frame_type::DECLARE_TOKEN:
      if (!frame.object_id.empty() || frame.chunk_offset != 0 ||
          !frame.manifest_payload.empty() || !frame.chunk_payload.empty() ||
          frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::DECLARE_OBJECT:
      if (frame.chunk_offset != 0 || frame.manifest_payload.empty() ||
          !frame.chunk_payload.empty() || !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::BEGIN:
      if (!frame.object_id.empty() || frame.chunk_offset != 0 ||
          !frame.chunk_payload.empty() || !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::OBJECT_CHUNK:
      if (!frame.manifest_payload.empty() || !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::SEAL_OBJECT:
      if (frame.chunk_offset != 0 || !frame.manifest_payload.empty() ||
          !frame.chunk_payload.empty() || !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::COMMIT_EPOCH:
      if (!frame.object_id.empty() || !frame.manifest_payload.empty() ||
          !frame.chunk_payload.empty() ||
          !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::ABORT:
      if (!frame.object_id.empty() || frame.chunk_offset != 0 ||
          !frame.manifest_payload.empty() || !frame.chunk_payload.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::PROMOTION_PREWARM_TOKEN:
      if (!frame.object_id.empty() || !frame.manifest_payload.empty() ||
          !frame.chunk_payload.empty() || !frame.reason.empty()) {
        return frame_error();
      }
      break;
    case Preserve_trx_transfer_frame_type::PROMOTION_GATE_EPOCH:
      if (frame.token != 0 || !frame.object_id.empty() ||
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

Preserve_trx_transfer_status map_promotion_status_to_transfer(
    Preserve_trx_promotion_adopt_status status) {
  switch (status) {
    case Preserve_trx_promotion_adopt_status::OK:
      return Preserve_trx_transfer_status::OK;
    case Preserve_trx_promotion_adopt_status::NOT_ENABLED:
    case Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT:
      return Preserve_trx_transfer_status::UNSUPPORTED;
    case Preserve_trx_promotion_adopt_status::IO_ERROR:
      return Preserve_trx_transfer_status::IO_ERROR;
    case Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT:
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    case Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT:
      return Preserve_trx_transfer_status::CORRUPT;
    case Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS:
    case Preserve_trx_promotion_adopt_status::TOKEN_NOT_FOUND:
    case Preserve_trx_promotion_adopt_status::NOT_STANDBY_PENDING:
    case Preserve_trx_promotion_adopt_status::EPOCH_NOT_COMMITTED:
    case Preserve_trx_promotion_adopt_status::APPLY_BARRIER_NOT_REACHED:
    case Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY:
    case Preserve_trx_promotion_adopt_status::TOO_MANY_PROMOTION_TOKENS:
    case Preserve_trx_promotion_adopt_status::CLAIMED_IMPORT_FAILED:
    case Preserve_trx_promotion_adopt_status::TOKEN_ABANDONED:
    case Preserve_trx_promotion_adopt_status::CLEANUP_PENDING:
    case Preserve_trx_promotion_adopt_status::CLEANUP_TAINTED:
      return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  return Preserve_trx_transfer_status::UNSUPPORTED;
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
      return Preserve_snapshot_status::UNSUPPORTED;
    case Preserve_trx_transfer_status::RESOURCE_EXHAUSTED:
    case Preserve_trx_transfer_status::ACK_UNCERTAIN:
      return Preserve_snapshot_status::IO_ERROR;
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
    case Preserve_trx_transfer_status::ACK_UNCERTAIN:
      return "ACK_UNCERTAIN";
  }
  return "UNKNOWN";
}

Preserve_trx_transfer_manifest receiver_record_manifest(
    const Preserve_trx_transfer_receiver_record &record) {
  Preserve_trx_transfer_manifest manifest;
  manifest.epoch_id = record.epoch_id;
  manifest.source_server_uuid = record.source_server_uuid;
  manifest.target_server_uuid = record.target_server_uuid;
  manifest.token = record.token;
  manifest.source_prepare_lsn = record.source_prepare_lsn;
  manifest.source_epoch_commit_lsn = record.source_epoch_commit_lsn;
  manifest.objects = record.objects;
  return manifest;
}

std::string receiver_boot_incarnation() {
  return std::string(server_uuid) + ":" + std::to_string(server_start_time) +
         ":" + std::to_string(current_pid);
}

bool strict_prepared_key_for_receiver(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &semantic_token,
    Preserve_trx_prepared_token_key *key) {
  if (key == nullptr || root_dir.empty() || manifest.source_server_uuid.empty() ||
      manifest.epoch_id.empty() || semantic_token.empty() ||
      manifest.source_epoch_commit_lsn == 0) {
    return false;
  }
  key->preserve_dir = root_dir;
  key->source_uuid = manifest.source_server_uuid;
  key->epoch_id = manifest.epoch_id;
  key->token = semantic_token;
  key->target_boot_incarnation = receiver_boot_incarnation();
  key->generation = manifest.source_epoch_commit_lsn;
  return true;
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
  return true;
}

bool put_receiver_record_lock_prepared(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    std::unique_ptr<lock_preserve_metadata_plan_t> plan,
    const lock_preserve_record_lock_metadata_facts_t &facts) {
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
  Receiver_record_lock_prepared prepared;
  prepared.plan = std::move(plan);
  prepared.facts = facts;
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

bool prepare_strict_bundle_for_receiver(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const Preserved_trx_bundle &bundle) {
  const bool has_record_locks =
      find_object(manifest, kPreservedTrxBlobRecordLocks) != nullptr;
  const auto *binlog_object =
      find_object(manifest, kPreservedTrxBlobBinlogCache);
  const bool has_binlog_cache =
      bundle.metadata.binlog_state ==
      Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE;
  if (has_binlog_cache != (binlog_object != nullptr)) return false;

  Receiver_record_lock_prepared prepared;
  uint64_t plan_capacity_bytes = 0;

  Preserve_trx_prepared_token_key key;
  if (!strict_prepared_key_for_receiver(root_dir, manifest,
                                        bundle.metadata.token, &key)) {
    return false;
  }
  std::string encoded_manifest;
  if (preserve_trx_transfer_encode_manifest(manifest, &encoded_manifest) !=
      Preserve_trx_transfer_status::OK) {
    return false;
  }
  const std::string object_set_digest =
      digest_hex(sha256_digest(encoded_manifest));

  Mysql_binlog_preserve_cache_facts binlog_facts;
  std::unique_ptr<Receiver_binlog_staging_payload_reader> binlog_reader;
  uint64_t native_binlog_bytes = 0;
  uint64_t native_binlog_fd_count = 0;
  uint64_t native_binlog_tmpdir_bytes = 0;
  if (has_binlog_cache) {
    Preserved_trx_external_blob_descriptor descriptor;
    descriptor.name = binlog_object->object_id;
    descriptor.size = binlog_object->total_size;
    descriptor.digest = binlog_object->digest;
    Mysql_binlog_preserve_token_identity identity;
    identity.source_uuid = key.source_uuid;
    identity.epoch_id = key.epoch_id;
    identity.token = key.token;
    identity.target_boot_incarnation = key.target_boot_incarnation;
    identity.generation = key.generation;
    const uint64_t binlog_incarnation =
        std::max<uint64_t>(1, static_cast<uint64_t>(server_start_time));
    if (!preserved_trx_build_native_binlog_cache_facts(
            bundle.metadata, identity, descriptor, binlog_incarnation,
            key.generation, &binlog_facts)) {
      return false;
    }
    native_binlog_bytes =
        mysql_binlog_preserve_native_memory_bytes_required(binlog_facts);
    native_binlog_fd_count =
        mysql_binlog_preserve_native_fd_count_required(binlog_facts);
    native_binlog_tmpdir_bytes =
        mysql_binlog_preserve_native_tmpdir_bytes_required(binlog_facts);
    if (native_binlog_bytes == 0) return false;
    binlog_reader = std::make_unique<Receiver_binlog_staging_payload_reader>(
        transfer_object_path(root_dir, manifest, *binlog_object),
        binlog_object->total_size);
    if (!binlog_reader->opened()) return false;
  }

  if (has_record_locks &&
      (!take_receiver_record_lock_prepared(root_dir, manifest, &prepared) ||
       prepared.plan == nullptr || !prepared.plan->ready())) {
    return false;
  }
  plan_capacity_bytes =
      prepared.plan == nullptr ? 0 : prepared.plan->capacity_bytes();
  auto restore_unconsumed_plan = create_scope_guard([&] {
    if (prepared.plan != nullptr) {
      (void)put_receiver_record_lock_prepared(
          root_dir, manifest, std::move(prepared.plan), prepared.facts);
    }
  });

  auto &registry = preserved_trx_strict_prepared_token_registry();
  Preserve_trx_prepare_lease prepare;
  if (registry.begin_prepare(key, key.generation, &prepare) !=
      Preserve_trx_prepared_status::OK) {
    return false;
  }
  Preserve_trx_prepared_token_resources resources;
  auto semantic_bundle = std::make_unique<Preserved_trx_bundle>(bundle);
  semantic_bundle->metadata.binlog_cache_payload.clear();
  semantic_bundle->external_blobs.clear();
  if (preserved_trx_acquire_prepared_token_resources(
          key, plan_capacity_bytes, native_binlog_bytes,
          native_binlog_fd_count, native_binlog_tmpdir_bytes, &resources) !=
          Preserve_trx_prepared_status::OK ||
      (binlog_reader != nullptr &&
       resources.prepare_native_binlog_handle_for_receiver(
           binlog_facts, binlog_reader.get()) !=
           Mysql_binlog_preserve_cache_status::OK) ||
      resources.install_semantic_bundle(std::move(semantic_bundle)) !=
          Preserve_trx_prepared_status::OK ||
      (prepared.plan != nullptr &&
       resources.install_record_lock_plan(std::move(prepared.plan)) !=
           Preserve_trx_prepared_status::OK)) {
    return false;
  }
  const auto status = registry.publish_prewarmed(
      &prepare, object_set_digest, std::move(resources));
  if (status != Preserve_trx_prepared_status::OK &&
      status != Preserve_trx_prepared_status::IDEMPOTENT) {
    return false;
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
    pending.file_backed = native_binlog_tmpdir_bytes != 0;
    std::lock_guard<std::mutex> guard(g_receiver_strict_binlog_facts_mutex);
    g_receiver_strict_binlog_facts[{root_dir, manifest.epoch_id,
                                    manifest.token}] = std::move(pending);
  }
  return true;
}

std::string strict_empty_set_digest(const char *domain) {
  return digest_hex(sha256_digest(std::string("PTRX_EMPTY_") + domain));
}

void bind_strict_prepared_tokens_from_epoch_fact(
    const std::string &root_dir,
    const Preserve_trx_transfer_epoch_fact &fact,
    const Preserve_trx_transfer_epoch_fact_token *single_token = nullptr) {
  auto &registry = preserved_trx_strict_prepared_token_registry();
  const uint64_t now_us = transfer_monotonic_us();
  const uint64_t prepare_window_us =
      std::max<uint64_t>(1, preserve_trx_transfer_commit_timeout_ms) * 1000ULL;
  constexpr uint64_t kClientResumeWindowUs = 300ULL * 1000000ULL;
  const std::string epoch_fact_digest = digest_hex(fact.fact_digest);

  const auto bind_token =
      [&](const Preserve_trx_transfer_epoch_fact_token &token) {
    const auto has_object = [&](const char *object_id) {
      return std::any_of(token.objects.begin(), token.objects.end(),
                         [&](const auto &object) {
                           return object.object_id == object_id;
                         });
    };
    const bool has_record_locks = has_object(kPreservedTrxBlobRecordLocks);
    const bool has_binlog_cache = has_object(kPreservedTrxBlobBinlogCache);

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
    manifest.source_server_uuid = fact.source_server_uuid;
    manifest.target_server_uuid = fact.target_server_uuid;
    manifest.token = token.token;
    manifest.source_prepare_lsn = token.source_prepare_lsn;
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
    facts.epoch_fact_digest = epoch_fact_digest;
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
    facts.epoch_prepare_deadline_us = now_us + prepare_window_us;
    facts.client_resume_deadline_us = now_us + kClientResumeWindowUs;
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
  for (uint64_t ready_token : state.ready_tokens) {
    if (state.fact_tokens.count(ready_token) != 0) {
      ++state.ready_fact_token_count;
    }
  }
  state.fact = std::move(fact);
  state.fact_loaded = true;
}

void bind_strict_prepared_token_from_cached_epoch_fact(
    const std::string &root_dir, const std::string &epoch_id, uint64_t token) {
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
      root_dir, *fact, &fact->tokens[token_index]);
}

void bind_strict_prepared_tokens_from_committed_epoch(
    const std::string &root_dir, const std::string &epoch_id) {
  if (!preserve_trx_transfer_epoch_committed(root_dir, epoch_id)) return;
  Preserve_trx_transfer_epoch_fact loaded_fact;
  if (preserve_trx_transfer_read_epoch_fact(root_dir, epoch_id, &loaded_fact) !=
      Preserve_trx_transfer_status::OK) {
    return;
  }
  auto fact = std::make_shared<const Preserve_trx_transfer_epoch_fact>(
      std::move(loaded_fact));
  cache_receiver_epoch_fact(root_dir, fact);
  bind_strict_prepared_tokens_from_epoch_fact(root_dir, *fact);
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

uint64_t transfer_commit_timeout_seconds() {
  return (static_cast<uint64_t>(preserve_trx_transfer_commit_timeout_ms) + 999) /
         1000;
}

void note_receiver_first_frame_durable() {
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
  g_receiver_prewarm_end_monotonic_us.store(transfer_monotonic_us());
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

void note_receiver_projection_publish_us(uint64_t elapsed_us) {
  g_receiver_projection_publish_count.fetch_add(1);
  g_receiver_projection_publish_us.fetch_add(elapsed_us);
  update_receiver_max_us(&g_receiver_projection_publish_max_us, elapsed_us);
  for (size_t i = 0; i < kProjectionPublishP95BucketsUs.size(); ++i) {
    if (elapsed_us <= kProjectionPublishP95BucketsUs[i]) {
      g_receiver_projection_publish_histogram[i].fetch_add(1);
      return;
    }
  }
  g_receiver_projection_publish_histogram.back().fetch_add(1);
}

void refresh_receiver_ready_after_final_metadata() {
  const uint64_t final_us = g_receiver_final_metadata_durable_us.load();
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

void note_receiver_seal_prewarm_token_status(
    const std::string &epoch_id, uint64_t token,
    Preserve_trx_promotion_adopt_status status) {
  std::lock_guard<std::mutex> guard(g_receiver_seal_prewarm_state_mutex);
  g_receiver_seal_prewarm_state[std::make_pair(epoch_id, token)] = status;
}

bool receiver_seal_prewarm_token_ok(const std::string &epoch_id,
                                    uint64_t token) {
  std::lock_guard<std::mutex> guard(g_receiver_seal_prewarm_state_mutex);
  const auto found = g_receiver_seal_prewarm_state.find(
      std::make_pair(epoch_id, token));
  return found != g_receiver_seal_prewarm_state.end() &&
         found->second == Preserve_trx_promotion_adopt_status::OK;
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

bool publish_receiver_epoch_ready_from_fact_if_possible(
    const std::string &root_dir, const std::string &epoch_id);

void note_receiver_epoch_token_ready(const std::string &root_dir,
                                     const std::string &epoch_id,
                                     uint64_t token) {
  std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
  Receiver_epoch_ready_state &state =
      g_receiver_ready_epoch_state[{root_dir, epoch_id}];
  const bool inserted = state.ready_tokens.insert(token).second;
  if (inserted && state.fact_loaded && state.fact_tokens.count(token) != 0) {
    ++state.ready_fact_token_count;
  }
}

bool publish_receiver_epoch_ready_from_seal_prewarm(
    const std::string &root_dir,
    const std::vector<Preserve_trx_transfer_manifest> &manifests) {
  if (manifests.empty()) return false;
  return publish_receiver_epoch_ready_from_fact_if_possible(
      root_dir, manifests.front().epoch_id);
}

bool publish_receiver_epoch_ready_from_fact_if_possible_impl(
    const std::string &root_dir, const std::string &epoch_id) {
  if (!preserve_trx_transfer_epoch_committed(root_dir, epoch_id)) {
    return false;
  }

  Receiver_epoch_binding_guard binding_guard(root_dir, epoch_id);
  std::vector<uint64_t> tokens;
  std::shared_ptr<const Preserve_trx_transfer_epoch_fact> fact;
  {
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    Receiver_epoch_ready_state &state =
        g_receiver_ready_epoch_state[{root_dir, epoch_id}];
    if (state.bound) return true;
    if (state.binding) return false;
    if (state.fact_loaded) {
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

  if (fact == nullptr) {
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
    if (state.bound) return true;
    if (state.binding) return false;
    if (state.fact == nullptr ||
        state.ready_fact_token_count != state.fact_tokens.size()) {
      return false;
    }
    fact = state.fact;
    state.binding = true;
    binding_guard.arm(++state.binding_generation);
    if (g_receiver_epoch_bind_bad_alloc_for_unit_test.load()) {
      throw std::bad_alloc();
    }
    tokens.assign(state.fact_tokens.begin(), state.fact_tokens.end());
  }

  uint64_t ready_tokens = 0;
  g_receiver_epoch_ready_bind_attempts.fetch_add(1);
  const Preserve_trx_promotion_adopt_status bind_status =
      preserved_trx_promotion_bind_prewarmed_epoch_for_receiver(
          root_dir, epoch_id, tokens, &ready_tokens);
  if (bind_status != Preserve_trx_promotion_adopt_status::OK ||
      ready_tokens != tokens.size()) {
    return false;
  }

  {
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    Receiver_epoch_ready_state &state =
        g_receiver_ready_epoch_state[{root_dir, epoch_id}];
    state.binding = false;
    if (state.bound) return true;
    state.bound = true;
  }
  binding_guard.commit();
  const uint64_t total_tokens = tokens.size();
  g_receiver_auto_prewarm_tokens.fetch_add(total_tokens);
  g_receiver_auto_prewarm_ready_tokens.fetch_add(total_tokens);
  g_receiver_auto_prewarm_last_status.store(
      static_cast<uint64_t>(Preserve_trx_promotion_adopt_status::OK));
  g_receiver_prewarm_backlog_at_phase2_end.store(0);
  g_receiver_ready_monotonic_us.store(transfer_monotonic_us());
  refresh_receiver_ready_after_final_metadata();
  refresh_receiver_ready_after_final_spool_ack();
  return true;
}

bool publish_receiver_epoch_ready_from_fact_if_possible(
    const std::string &root_dir, const std::string &epoch_id) {
  try {
    return publish_receiver_epoch_ready_from_fact_if_possible_impl(root_dir,
                                                                   epoch_id);
  } catch (...) {
    return false;
  }
}

bool receiver_epoch_ready_is_bound(const std::string &root_dir,
                                   const std::string &epoch_id) {
  std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
  const auto found = g_receiver_ready_epoch_state.find({root_dir, epoch_id});
  return found != g_receiver_ready_epoch_state.end() && found->second.bound;
}

void retry_unbound_receiver_epochs_once() {
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
                                                             epoch.second);
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
    const Preserved_trx_bundle &bundle, bool *stale_record_lock_proof) {
  if (stale_record_lock_proof != nullptr) *stale_record_lock_proof = false;
  Receiver_object_prewarm_proof proof;
  if (!lookup_receiver_object_prewarm_proof(
          root_dir, manifest, kPreservedTrxBlobRecordLocks, &proof)) {
    return Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY;
  }
  if (!proof.record_lock_object) {
    return Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
  }
  if (stale_record_lock_proof != nullptr &&
      (proof.cold_gets != 0 || proof.resident_pages < proof.page_count)) {
    *stale_record_lock_proof = true;
  }
  return preserved_trx_promotion_prewarm_staged_bundle_with_record_lock_proof_for_receiver(
      root_dir, manifest.epoch_id, manifest.token,
      manifest.source_epoch_commit_lsn, bundle, proof.page_count,
      proof.resident_pages, proof.cold_gets, proof.bitmap_pages,
      proof.bitmap_bits);
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
      my_error(ER_PRESERVE_TRX_UNSUPPORTED, MYF(0));
      return;
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

std::string preserve_trx_transfer_qualify_epoch_id(
    const std::string &epoch_id) {
  return qualify_transfer_epoch_id(epoch_id);
}

std::string preserve_trx_transfer_source_incarnation_id_for_unit_test() {
  return transfer_source_incarnation_id();
}

std::string preserve_trx_transfer_qualify_epoch_id_for_unit_test(
    const std::string &epoch_id) {
  return qualify_transfer_epoch_id(epoch_id);
}

void preserve_trx_transfer_set_spool_short_write_for_unit_test(bool enabled) {
  g_transfer_spool_short_write_for_unit_test.store(enabled);
}

void preserve_trx_transfer_set_spool_rollback_failure_for_unit_test(
    bool enabled) {
  g_transfer_spool_rollback_failure_for_unit_test.store(enabled);
}

void preserve_trx_transfer_set_staging_cleanup_failures_for_unit_test(
    uint failures) {
  g_transfer_staging_cleanup_failures_for_unit_test.store(failures);
}

bool preserve_trx_transfer_tls_identity_config_is_valid_for_unit_test(
    bool unix_socket, const std::string &ssl_ca, const std::string &ssl_capath) {
  return transfer_tls_identity_config_is_valid(unix_socket, ssl_ca,
                                               ssl_capath);
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
  if (manifest.protocol_version != 2) {
    append_u64(&out, manifest.source_prepare_lsn);
    append_u64(&out, manifest.source_epoch_commit_lsn);
  }
  if (append_string(&out, manifest.epoch_id) ||
      append_string(&out, manifest.source_server_uuid) ||
      append_string(&out, manifest.target_server_uuid)) {
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
  if (parsed.protocol_version != kPreserveTrxTransferProtocolVersion) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (reader.read_u64(&parsed.source_prepare_lsn) ||
      reader.read_u64(&parsed.source_epoch_commit_lsn)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (reader.read_string(&parsed.epoch_id) ||
      reader.read_string(&parsed.source_server_uuid) ||
      reader.read_string(&parsed.target_server_uuid) ||
      reader.read_u64(&parsed.token)) {
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

Preserve_trx_transfer_status preserve_trx_transfer_encode_epoch_fact(
    const Preserve_trx_transfer_epoch_fact &fact, std::string *encoded) {
  if (encoded == nullptr || !transfer_component_safe(fact.epoch_id) ||
      !transfer_component_safe(fact.source_server_uuid) ||
      !transfer_component_safe(fact.target_server_uuid) ||
      fact.source_fence_lsn == 0 || fact.tokens.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::vector<Preserve_trx_transfer_epoch_fact_token> tokens = fact.tokens;
  std::sort(tokens.begin(), tokens.end(),
            [](const Preserve_trx_transfer_epoch_fact_token &left,
               const Preserve_trx_transfer_epoch_fact_token &right) {
              return left.token < right.token;
            });

  std::string body;
  body.append("PTRXFER_EPOCH_FACT_V2\n");
  body.append("epoch=").append(fact.epoch_id).append("\n");
  body.append("source=").append(fact.source_server_uuid).append("\n");
  body.append("target=").append(fact.target_server_uuid).append("\n");
  body.append("source_fence_lsn=")
      .append(std::to_string(fact.source_fence_lsn))
      .append("\n");
  body.append("token_count=").append(std::to_string(tokens.size())).append("\n");

  uint64_t previous_token = 0;
  for (const Preserve_trx_transfer_epoch_fact_token &token : tokens) {
    if (token.token == 0 || token.token <= previous_token ||
        token.source_prepare_lsn == 0 || token.source_epoch_commit_lsn == 0 ||
        token.source_prepare_lsn > fact.source_fence_lsn ||
        token.source_epoch_commit_lsn > fact.source_fence_lsn) {
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }
    previous_token = token.token;
    body.append("token=").append(std::to_string(token.token)).append("\n");
    body.append("source_prepare_lsn=")
        .append(std::to_string(token.source_prepare_lsn))
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
  if (!next_line() || line != "PTRXFER_EPOCH_FACT_V2")
    return Preserve_trx_transfer_status::CORRUPT;
  std::string value;
  if (!next_line() || !line_has_prefix(line, "epoch=", &parsed.epoch_id) ||
      !transfer_component_safe(parsed.epoch_id)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (!next_line() ||
      !line_has_prefix(line, "source=", &parsed.source_server_uuid) ||
      !transfer_component_safe(parsed.source_server_uuid)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (!next_line() ||
      !line_has_prefix(line, "target=", &parsed.target_server_uuid) ||
      !transfer_component_safe(parsed.target_server_uuid)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (!next_line() || !line_has_prefix(line, "source_fence_lsn=", &value) ||
      !parse_uint64_strict(value, &parsed.source_fence_lsn) ||
      parsed.source_fence_lsn == 0) {
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
        !line_has_prefix(line, "source_prepare_lsn=", &value) ||
        !parse_uint64_strict(value, &token.source_prepare_lsn) ||
        token.source_prepare_lsn == 0) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    if (!next_line() ||
        !line_has_prefix(line, "source_epoch_commit_lsn=", &value) ||
        !parse_uint64_strict(value, &token.source_epoch_commit_lsn) ||
        token.source_epoch_commit_lsn == 0 ||
        token.source_prepare_lsn > parsed.source_fence_lsn ||
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
      if (fields.size() != 5 || !transfer_component_safe(fields[0]) ||
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
    Preserve_trx_transfer_epoch_fact *fact) {
  if (fact == nullptr || manifests.empty())
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  Preserve_trx_transfer_epoch_fact built;
  built.epoch_id = manifests.front().epoch_id;
  built.source_server_uuid = manifests.front().source_server_uuid;
  built.target_server_uuid = manifests.front().target_server_uuid;
  built.source_fence_lsn = source_fence_lsn;
  if (built.source_fence_lsn == 0) {
    for (const auto &manifest : manifests) {
      built.source_fence_lsn =
          std::max(built.source_fence_lsn, manifest.source_epoch_commit_lsn);
    }
  }
  std::set<uint64_t> tokens;
  for (const Preserve_trx_transfer_manifest &manifest : manifests) {
    const Preserve_trx_transfer_status validation_status =
        validate_manifest_components(manifest, false);
    if (validation_status != Preserve_trx_transfer_status::OK)
      return validation_status;
    if (manifest.epoch_id != built.epoch_id ||
        manifest.source_server_uuid != built.source_server_uuid ||
        manifest.target_server_uuid != built.target_server_uuid ||
        !tokens.insert(manifest.token).second) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    std::string encoded_manifest;
    const Preserve_trx_transfer_status encode_status =
        preserve_trx_transfer_encode_manifest(manifest, &encoded_manifest);
    if (encode_status != Preserve_trx_transfer_status::OK) return encode_status;
    Preserve_trx_transfer_epoch_fact_token token;
    token.token = manifest.token;
    token.source_prepare_lsn = manifest.source_prepare_lsn;
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
        left[i].digest != right[i].digest) {
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
         left.digest == right.digest;
}

bool epoch_fact_tokens_equal(
    const Preserve_trx_transfer_epoch_fact_token &left,
    const Preserve_trx_transfer_epoch_fact_token &right) {
  return left.token == right.token &&
         left.source_prepare_lsn == right.source_prepare_lsn &&
         left.source_epoch_commit_lsn == right.source_epoch_commit_lsn &&
         left.manifest_digest == right.manifest_digest &&
         transfer_object_descriptors_equal(left.objects, right.objects);
}

bool epoch_fact_matches_manifest(const std::string &root_dir,
                                 const Preserve_trx_transfer_manifest &manifest) {
  Preserve_trx_transfer_epoch_fact fact;
  if (preserve_trx_transfer_read_epoch_fact(root_dir, manifest.epoch_id,
                                            &fact) !=
      Preserve_trx_transfer_status::OK) {
    return false;
  }
  if (fact.epoch_id != manifest.epoch_id ||
      fact.source_server_uuid != manifest.source_server_uuid ||
      fact.target_server_uuid != manifest.target_server_uuid) {
    return false;
  }

  Preserve_trx_transfer_epoch_fact built;
  if (build_epoch_fact_from_manifests(
          {manifest}, manifest.source_epoch_commit_lsn, &built) !=
      Preserve_trx_transfer_status::OK) {
    return false;
  }
  for (const Preserve_trx_transfer_epoch_fact_token &token : fact.tokens) {
    if (epoch_fact_tokens_equal(token, built.tokens.front())) return true;
  }
  return false;
}

bool standby_token_already_published_for_epoch(
    const std::string &root_dir, const Preserve_trx_transfer_manifest &manifest,
    Preserved_trx_store *store) {
  if (store == nullptr || !epoch_fact_matches_manifest(root_dir, manifest)) {
    return false;
  }
  Preserved_trx_carrier_listing listing;
  if (store->list_tokens(&listing) != Preserve_snapshot_status::OK) {
    return false;
  }
  const std::string token_component = transfer_token_component(manifest.token);
  return listing.snapshot_tokens.count(token_component) != 0 &&
         listing.standby_pending_tokens.count(token_component) != 0;
}

bool standby_token_artifact_published(
    const Preserve_trx_transfer_manifest &manifest, Preserved_trx_store *store) {
  if (store == nullptr) return false;
  Preserved_trx_carrier_listing listing;
  if (store->list_tokens(&listing) != Preserve_snapshot_status::OK) {
    return false;
  }
  const std::string token_component = transfer_token_component(manifest.token);
  return listing.snapshot_tokens.count(token_component) != 0 &&
         listing.standby_pending_tokens.count(token_component) != 0;
}

bool standby_token_artifact_published_in_listing(
    const Preserve_trx_transfer_manifest &manifest,
    const Preserved_trx_carrier_listing &listing) {
  const std::string token_component = transfer_token_component(manifest.token);
  return listing.snapshot_tokens.count(token_component) != 0 &&
         listing.standby_pending_tokens.count(token_component) != 0;
}

Preserve_snapshot_write_options receiver_standby_projection_write_options() {
  Preserve_snapshot_write_options options;
  options.fast_new_token_state = true;
  options.fast_prebuilt_blob_adopt = true;
  options.defer_file_fsync = true;
  options.defer_directory_fsync = true;
  options.shard_snapshot_files = true;
  options.shard_generic_external_blobs = true;
  return options;
}

bool receiver_standby_projection_exists(
    Local_file_preserved_trx_carrier *carrier,
    const Preserve_trx_transfer_manifest &manifest) {
  if (carrier == nullptr) return false;
  bool exists = false;
  if (carrier->standby_projection_exists(
          transfer_token_component(manifest.token), &exists) !=
      Preserved_trx_carrier_status::OK) {
    return false;
  }
  return exists;
}

std::mutex &receiver_standby_projection_mutex(uint64_t token) {
  const size_t shard =
      std::hash<uint64_t>{}(token) % g_receiver_standby_publish_mutexes.size();
  return g_receiver_standby_publish_mutexes[shard];
}

bool receiver_projection_blob_can_be_preinstalled(const std::string &name) {
  return name == kPreservedTrxBlobBinlogCache ||
         name == kPreservedTrxBlobRecordLocks;
}

Preserve_trx_transfer_status ensure_receiver_standby_projection_key_ready(
    const std::string &root_dir) {
  std::lock_guard<std::mutex> guard(g_receiver_standby_projection_key_mutex);
  if (g_receiver_standby_projection_key_ready_roots.count(root_dir) != 0) {
    return Preserve_trx_transfer_status::OK;
  }

  Local_file_preserved_trx_carrier carrier(root_dir);
  Preserved_trx_codec_context context;
  const Preserved_trx_carrier_status status = carrier.codec_context(
      &context, Preserved_trx_codec_context_purpose::WRITE_NEW);
  if (status != Preserved_trx_carrier_status::OK) {
    return map_carrier_status_to_transfer(status);
  }
  g_receiver_standby_projection_key_ready_roots.insert(root_dir);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
mark_receiver_projection_external_blobs_prebuilt(
    const Preserve_trx_transfer_manifest &manifest,
    Preserved_trx_bundle *bundle) {
  if (bundle == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  for (Preserved_trx_external_blob &blob : bundle->external_blobs) {
    if (!receiver_projection_blob_can_be_preinstalled(blob.name)) continue;
    const Preserve_trx_transfer_object_descriptor *object =
        find_object(manifest, blob.name);
    if (object == nullptr ||
        object->kind != Preserve_trx_transfer_object_kind::EXTERNAL_BLOB ||
        object->total_size != blob.descriptor.size ||
        object->digest != blob.descriptor.digest) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    blob.prebuilt = true;
    blob.payload.clear();
    blob.warmcopy_id = manifest.epoch_id;
    blob.warmcopy_epoch = manifest.source_epoch_commit_lsn == 0
                              ? 1
                              : manifest.source_epoch_commit_lsn;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status publish_receiver_standby_bundle_projection(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const Preserved_trx_bundle &staged_bundle,
    bool objects_already_sealed) {
  {
    Local_file_preserved_trx_carrier probe(root_dir);
    if (receiver_standby_projection_exists(&probe, manifest)) {
      return Preserve_trx_transfer_status::OK;
    }
  }

  const uint64_t total_started_us = transfer_monotonic_us();
  const uint64_t lock_started_us = total_started_us;
  std::unique_lock<std::mutex> guard(
      receiver_standby_projection_mutex(manifest.token));
  g_receiver_projection_lock_wait_us.fetch_add(transfer_monotonic_us() -
                                               lock_started_us);

  Local_file_preserved_trx_carrier existing_probe(root_dir);
  if (receiver_standby_projection_exists(&existing_probe, manifest)) {
    return Preserve_trx_transfer_status::OK;
  }

  const Preserve_trx_transfer_status key_status =
      ensure_receiver_standby_projection_key_ready(root_dir);
  if (key_status != Preserve_trx_transfer_status::OK) return key_status;

  Local_file_preserved_trx_carrier carrier(
      root_dir, receiver_standby_projection_write_options());
  Preserved_trx_store store(&carrier);
  Preserved_trx_bundle publish_bundle = staged_bundle;
  const Preserve_trx_transfer_status prebuilt_status =
      mark_receiver_projection_external_blobs_prebuilt(manifest,
                                                       &publish_bundle);
  if (prebuilt_status != Preserve_trx_transfer_status::OK)
    return prebuilt_status;
  Preserved_trx_store_write_stats write_stats;
  const uint64_t store_started_us = transfer_monotonic_us();
  const Preserve_trx_transfer_status status =
      preserve_trx_transfer_publish_standby_bundle(
      root_dir, manifest, std::move(publish_bundle), &store,
      transfer_commit_timeout_seconds(), nullptr, &write_stats,
      objects_already_sealed);
  g_receiver_projection_store_write_us.fetch_add(transfer_monotonic_us() -
                                                 store_started_us);
  g_receiver_projection_token_state_us.fetch_add(write_stats.token_state_us);
  g_receiver_projection_external_blob_us.fetch_add(
      write_stats.adopt_warm_blob_us + write_stats.write_new_blobs_us);
  g_receiver_projection_encode_us.fetch_add(write_stats.encode_us);
  g_receiver_projection_marker_write_us.fetch_add(
      write_stats.write_standby_pending_marker_us);
  g_receiver_projection_snapshot_write_us.fetch_add(
      write_stats.write_snapshot_us);
  note_receiver_projection_publish_us(transfer_monotonic_us() -
                                      total_started_us);
  return status;
}

Preserve_trx_transfer_status write_epoch_fact_file(
    const std::string &root_dir,
    const std::vector<Preserve_trx_transfer_manifest> &manifests,
    uint64_t source_fence_lsn) {
  Preserve_trx_transfer_epoch_fact fact;
  Preserve_trx_transfer_status status =
      build_epoch_fact_from_manifests(manifests, source_fence_lsn, &fact);
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
        existing_fact.source_server_uuid != fact.source_server_uuid ||
        existing_fact.target_server_uuid != fact.target_server_uuid ||
        existing_fact.source_fence_lsn != fact.source_fence_lsn) {
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
  File file =
      my_create(tmp_path.c_str(), 0600, O_WRONLY | O_CREAT | O_EXCL, MYF(0));
  if (file < 0) return Preserve_trx_transfer_status::IO_ERROR;
  bool error =
      my_write(file, reinterpret_cast<const unsigned char *>(encoded.data()),
               encoded.length(), MYF(0)) != encoded.length() ||
      my_sync(file, MYF(0)) != 0;
  if (my_close(file, MYF(0))) error = true;
  if (!error && my_rename(tmp_path.c_str(), final_path.c_str(), MYF(0))) {
    error = true;
  }
  if (!error && fsync_transfer_directory(epoch_dir)) error = true;
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

  std::string out;
  out.append(kTransferFrameMagic, kTransferFrameMagicLength);
  append_u16(&out, frame.protocol_version);
  append_u16(&out, static_cast<uint16_t>(frame.type));
  append_u64(&out, frame.sequence);
  if (append_string(&out, frame.epoch_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  append_u64(&out, frame.token);
  if (append_string(&out, frame.object_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  append_u64(&out, frame.chunk_offset);
  if (append_string(&out, frame.manifest_payload) ||
      append_string(&out, frame.chunk_payload) ||
      append_string(&out, frame.reason)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  *encoded = std::move(out);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_decode_frame(
    const std::string &encoded, Preserve_trx_transfer_frame *frame) {
  if (frame == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  Manifest_reader reader(encoded);
  const char *magic = nullptr;
  if (reader.read_fixed(kTransferFrameMagicLength, &magic) ||
      std::memcmp(magic, kTransferFrameMagic, kTransferFrameMagicLength) != 0) {
    return Preserve_trx_transfer_status::CORRUPT;
  }

  Preserve_trx_transfer_frame parsed;
  uint16_t raw_type = 0;
  if (reader.read_u16(&parsed.protocol_version) ||
      reader.read_u16(&raw_type) ||
      !frame_type_supported(raw_type, &parsed.type) ||
      reader.read_u64(&parsed.sequence) ||
      reader.read_string(&parsed.epoch_id) ||
      reader.read_u64(&parsed.token) ||
      reader.read_string(&parsed.object_id) ||
      reader.read_u64(&parsed.chunk_offset) ||
      reader.read_string(&parsed.manifest_payload) ||
      reader.read_string(&parsed.chunk_payload) ||
      reader.read_string(&parsed.reason) || !reader.eof()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (parsed.protocol_version != kPreserveTrxTransferProtocolVersion) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
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
    const std::vector<std::string> &encoded_frames, uint64_t max_bytes,
    std::string *encoded_batch) {
  if (encoded_batch == nullptr || encoded_frames.empty() ||
      encoded_frames.size() > kMaxTransferManifestObjects) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::string out;
  out.append(kTransferFrameBatchMagic, kTransferFrameBatchMagicLength);
  append_u16(&out, kPreserveTrxTransferProtocolVersion);
  append_u32(&out, static_cast<uint32_t>(encoded_frames.size()));
  uint64_t total_bytes = out.length();
  for (const std::string &encoded_frame : encoded_frames) {
    Preserve_trx_transfer_frame ignored;
    const Preserve_trx_transfer_status frame_status =
        preserve_trx_transfer_decode_frame(encoded_frame, &ignored);
    if (frame_status != Preserve_trx_transfer_status::OK) return frame_status;
    if (encoded_frame.length() >
            std::numeric_limits<uint64_t>::max() - total_bytes ||
        encoded_frame.length() > std::numeric_limits<size_t>::max()) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    total_bytes += encoded_frame.length();
    if (max_bytes != 0 && total_bytes > max_bytes) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    append_u64(&out, encoded_frame.length());
    out.append(encoded_frame);
  }
  *encoded_batch = std::move(out);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_encode_frame_batch(
    const std::vector<std::string> &encoded_frames, std::string *encoded_batch) {
  return encode_frame_batch_with_limit(
      encoded_frames, preserve_trx_transfer_max_inflight_bytes, encoded_batch);
}

Preserve_trx_transfer_status preserve_trx_transfer_decode_frame_batch(
    const std::string &encoded_batch, std::vector<std::string> *encoded_frames) {
  if (encoded_frames == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  encoded_frames->clear();
  Manifest_reader reader(encoded_batch);
  const char *magic = nullptr;
  uint16_t version = 0;
  uint32_t count = 0;
  if (reader.read_fixed(kTransferFrameBatchMagicLength, &magic) ||
      std::memcmp(magic, kTransferFrameBatchMagic,
                  kTransferFrameBatchMagicLength) != 0 ||
      reader.read_u16(&version) || reader.read_u32(&count) || count == 0 ||
      count > kMaxTransferManifestObjects) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  if (version != kPreserveTrxTransferProtocolVersion) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (!preserve_trx_transfer_frame_batch_count_fits_payload(
          count, reader.remaining())) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  const uint64_t decode_bytes =
      static_cast<uint64_t>(count) * sizeof(std::string);
  Preserve_memory_lease decode_lease =
      acquire_transfer_decode_memory_lease(encoded_batch, decode_bytes);
  if (!decode_lease.acquired()) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }

  try {
    std::vector<std::string> out;
    out.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
      uint64_t length = 0;
      const char *ptr = nullptr;
      if (reader.read_u64(&length) ||
          length > std::numeric_limits<size_t>::max() ||
          reader.read_fixed(static_cast<size_t>(length), &ptr)) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      std::string encoded_frame(ptr, static_cast<size_t>(length));
      Preserve_trx_transfer_frame ignored;
      const Preserve_trx_transfer_status frame_status =
          preserve_trx_transfer_decode_frame(encoded_frame, &ignored);
      if (frame_status != Preserve_trx_transfer_status::OK) return frame_status;
      out.push_back(std::move(encoded_frame));
    }
    if (!reader.eof()) return Preserve_trx_transfer_status::CORRUPT;
    *encoded_frames = std::move(out);
  } catch (const std::bad_alloc &) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  return Preserve_trx_transfer_status::OK;
}

bool preserve_trx_transfer_frame_batch_count_fits_payload(
    uint32_t count, size_t remaining_bytes) {
  return count != 0 &&
         count <= remaining_bytes / sizeof(uint64_t);
}

namespace {

constexpr char kTransferAckMagic[] = {'P', 'T', 'R', 'A', '1'};
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
                Preserve_trx_transfer_status::RESOURCE_EXHAUSTED)) {
    return false;
  }
  *status = static_cast<Preserve_trx_transfer_status>(raw);
  return true;
}

Preserve_trx_transfer_status transfer_payload_identity(
    const std::string &encoded_payload, std::string *epoch_id,
    uint64_t *last_sequence) {
  if (epoch_id == nullptr || last_sequence == nullptr ||
      encoded_payload.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::vector<std::string> encoded_frames;
  if (transfer_frame_batch_magic_matches(encoded_payload)) {
    const Preserve_trx_transfer_status status =
        preserve_trx_transfer_decode_frame_batch(encoded_payload,
                                                 &encoded_frames);
    if (status != Preserve_trx_transfer_status::OK) return status;
  } else {
    encoded_frames.push_back(encoded_payload);
  }
  std::string payload_epoch;
  uint64_t payload_sequence = 0;
  for (const std::string &encoded_frame : encoded_frames) {
    Preserve_trx_transfer_frame frame;
    const Preserve_trx_transfer_status status =
        preserve_trx_transfer_decode_frame(encoded_frame, &frame);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (payload_epoch.empty()) payload_epoch = frame.epoch_id;
    if (frame.epoch_id != payload_epoch ||
        (payload_sequence != 0 && frame.sequence != payload_sequence + 1)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    payload_sequence = frame.sequence;
  }
  if (payload_epoch.empty() || payload_sequence == 0) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  *epoch_id = std::move(payload_epoch);
  *last_sequence = payload_sequence;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status build_ack_body(
    const Preserve_trx_transfer_frame_ack &ack, std::string *body) {
  if (body == nullptr || ack.source_incarnation_id.length() != 32 ||
      !transfer_component_safe(ack.source_incarnation_id) ||
      !transfer_component_safe(ack.epoch_id) || ack.sequence == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  body->clear();
  body->append(kTransferAckMagic, kTransferAckMagicLength);
  if (append_string(body, ack.source_incarnation_id) ||
      append_string(body, ack.epoch_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  append_u64(body, ack.sequence);
  body->append(reinterpret_cast<const char *>(ack.frame_digest.data()),
               ack.frame_digest.size());
  append_u16(body, static_cast<uint16_t>(ack.status));
  return Preserve_trx_transfer_status::OK;
}

bool compute_ack_hmac(const std::string &body,
                      std::array<unsigned char, kPreservedTrxSha256Length> *out) {
  if (out == nullptr) return false;
  Preserved_trx_codec_context context;
  if (!transfer_bundle_codec_context(&context)) return false;
  unsigned int length = 0;
  unsigned char *result = HMAC(
      EVP_sha256(), context.hmac_key.data(), context.hmac_key.size(),
      reinterpret_cast<const unsigned char *>(body.data()), body.length(),
      out->data(), &length);
  return result != nullptr && length == out->size();
}

}  // namespace

Preserve_trx_transfer_status preserve_trx_transfer_build_frame_ack(
    const std::string &source_incarnation_id,
    const std::string &encoded_payload, Preserve_trx_transfer_status status,
    Preserve_trx_transfer_frame_ack *ack) {
  if (ack == nullptr || source_incarnation_id.length() != 32) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  Preserve_trx_transfer_frame_ack built;
  built.source_incarnation_id = source_incarnation_id;
  Preserve_trx_transfer_status identity_status = transfer_payload_identity(
      encoded_payload, &built.epoch_id, &built.sequence);
  if (identity_status != Preserve_trx_transfer_status::OK) {
    return identity_status;
  }
  built.frame_digest = sha256_digest(encoded_payload);
  built.status = status;
  std::string body;
  Preserve_trx_transfer_status body_status = build_ack_body(built, &body);
  if (body_status != Preserve_trx_transfer_status::OK) return body_status;
  if (!compute_ack_hmac(body, &built.hmac)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
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
  std::array<unsigned char, kPreservedTrxSha256Length> expected_hmac{};
  if (!compute_ack_hmac(body, &expected_hmac) || expected_hmac != ack.hmac) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  body.append(reinterpret_cast<const char *>(ack.hmac.data()), ack.hmac.size());
  *encoded = bytes_to_lower_hex(
      reinterpret_cast<const unsigned char *>(body.data()), body.length());
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_verify_frame_ack(
    const std::string &encoded_ack,
    const std::string &expected_source_incarnation_id,
    const std::string &encoded_payload, Preserve_trx_transfer_frame_ack *ack) {
  if (ack == nullptr || expected_source_incarnation_id.length() != 32) {
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
  const char *hmac = nullptr;
  uint16_t raw_status = 0;
  if (reader.read_fixed(kTransferAckMagicLength, &magic) ||
      std::memcmp(magic, kTransferAckMagic, kTransferAckMagicLength) != 0 ||
      reader.read_string(&parsed.source_incarnation_id) ||
      reader.read_string(&parsed.epoch_id) || reader.read_u64(&parsed.sequence) ||
      reader.read_fixed(parsed.frame_digest.size(), &digest) ||
      reader.read_u16(&raw_status) ||
      !transfer_status_from_wire(raw_status, &parsed.status) ||
      reader.read_fixed(parsed.hmac.size(), &hmac) || !reader.eof()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  std::memcpy(parsed.frame_digest.data(), digest, parsed.frame_digest.size());
  std::memcpy(parsed.hmac.data(), hmac, parsed.hmac.size());
  const size_t body_length = raw.length() - parsed.hmac.size();
  std::array<unsigned char, kPreservedTrxSha256Length> expected_hmac{};
  if (!compute_ack_hmac(raw.substr(0, body_length), &expected_hmac) ||
      CRYPTO_memcmp(expected_hmac.data(), parsed.hmac.data(),
                    parsed.hmac.size()) != 0) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  std::string expected_epoch;
  uint64_t expected_sequence = 0;
  Preserve_trx_transfer_status status = transfer_payload_identity(
      encoded_payload, &expected_epoch, &expected_sequence);
  if (status != Preserve_trx_transfer_status::OK) return status;
  if (parsed.source_incarnation_id != expected_source_incarnation_id ||
      parsed.epoch_id != expected_epoch ||
      parsed.sequence != expected_sequence ||
      parsed.frame_digest != sha256_digest(encoded_payload)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  *ack = std::move(parsed);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status preserve_trx_transfer_validate_receiver_manifest(
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &allowed_source_server_uuid,
    const std::string &local_target_server_uuid) {
  if (!preserve_trx_transfer_receiver_enable) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK)
    return validation_status;

  if (allowed_source_server_uuid.empty() ||
      manifest.source_server_uuid != allowed_source_server_uuid) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (local_target_server_uuid.empty() ||
      manifest.target_server_uuid != local_target_server_uuid) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
preserve_trx_transfer_validate_receiver_manifest_from_config(
    const Preserve_trx_transfer_manifest &manifest) {
  const std::string allowed_source =
      preserve_trx_transfer_allowed_source_uuid == nullptr
          ? std::string()
          : std::string(preserve_trx_transfer_allowed_source_uuid);
  const std::string target =
      preserve_trx_transfer_target_server_uuid == nullptr
          ? std::string()
          : std::string(preserve_trx_transfer_target_server_uuid);
  /*
    The configured target UUID is an admission policy, not receiver identity.
    A standby receiver must only accept artifacts addressed to this mysqld;
    otherwise a misconfigured process could publish artifacts for another
    server and later make promotion readiness look trustworthy.
  */
  if (target.empty() || target != server_uuid) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  return preserve_trx_transfer_validate_receiver_manifest(manifest,
                                                          allowed_source,
                                                          target);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::declare_token(
    const std::string &epoch_id, uint64_t token,
    const std::string &source_server_uuid,
    const std::string &target_server_uuid) {
  if (!transfer_component_safe(epoch_id) ||
      !transfer_component_safe(source_server_uuid) ||
      !transfer_component_safe(target_server_uuid) || token == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  Preserve_trx_transfer_receiver_record record;
  record.epoch_id = epoch_id;
  record.token = token;
  record.source_server_uuid = source_server_uuid;
  record.target_server_uuid = target_server_uuid;
  record.state = Preserve_trx_transfer_receiver_state::DECLARED;

  std::lock_guard<std::mutex> guard(m_mutex);
  const Token_key key(epoch_id, token);
  if (m_records.find(key) != m_records.end()) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  m_records.emplace(key, std::move(record));
  return Preserve_trx_transfer_status::OK;
}

uint64_t Preserve_trx_transfer_receiver_registry::
    cleanup_debt_reserved_bytes_locked(bool *overflow) const {
  uint64_t bytes = 0;
  if (overflow != nullptr) *overflow = false;
  for (const auto &item : m_cleanup_debts) {
    if (item.second.reserved_bytes >
        std::numeric_limits<uint64_t>::max() - bytes) {
      if (overflow != nullptr) *overflow = true;
      return 0;
    }
    bytes += item.second.reserved_bytes;
  }
  return bytes;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::begin_receive(
    const Preserve_trx_transfer_manifest &manifest,
    uint64_t manifest_payload_bytes) {
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
  record.epoch_id = manifest.epoch_id;
  record.token = manifest.token;
  record.source_server_uuid = manifest.source_server_uuid;
  record.target_server_uuid = manifest.target_server_uuid;
  record.source_prepare_lsn = manifest.source_prepare_lsn;
  record.source_epoch_commit_lsn = manifest.source_epoch_commit_lsn;
  record.state = Preserve_trx_transfer_receiver_state::RECEIVING;
  record.objects = manifest.objects;
  record.reserved_bytes = reserved_bytes;

  std::lock_guard<std::mutex> guard(m_mutex);
  const Token_key key(manifest.epoch_id, manifest.token);
  auto existing_record = m_records.find(key);
  if (existing_record != m_records.end()) {
    if ((existing_record->second.state !=
             Preserve_trx_transfer_receiver_state::DECLARED &&
         existing_record->second.state !=
             Preserve_trx_transfer_receiver_state::RECEIVING) ||
        existing_record->second.source_server_uuid !=
            manifest.source_server_uuid ||
        existing_record->second.target_server_uuid !=
            manifest.target_server_uuid) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
  }
  bool cleanup_debt_overflow = false;
  uint64_t epoch_reserved_bytes =
      cleanup_debt_reserved_bytes_locked(&cleanup_debt_overflow);
  if (cleanup_debt_overflow) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  for (const auto &entry : m_records) {
    if (entry.first == key) continue;
    const Preserve_trx_transfer_receiver_record &existing = entry.second;
    if (existing.epoch_id != manifest.epoch_id ||
        (existing.state != Preserve_trx_transfer_receiver_state::DECLARED &&
         existing.state != Preserve_trx_transfer_receiver_state::RECEIVING)) {
      continue;
    }
    if (existing.reserved_bytes >
        std::numeric_limits<uint64_t>::max() - epoch_reserved_bytes) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    epoch_reserved_bytes += existing.reserved_bytes;
  }
  if (reserved_bytes >
          std::numeric_limits<uint64_t>::max() - epoch_reserved_bytes ||
      epoch_reserved_bytes + reserved_bytes >
          preserve_trx_transfer_max_inflight_bytes) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (existing_record == m_records.end()) {
    m_records.emplace(key, std::move(record));
  } else {
    const Preserve_trx_transfer_manifest existing_manifest =
        receiver_record_manifest(existing_record->second);
    for (const Preserve_trx_transfer_object_descriptor &object :
         manifest.objects) {
      const Preserve_trx_transfer_object_descriptor *existing =
          find_object(existing_manifest, object.object_id);
      if (existing != nullptr &&
          transfer_object_descriptor_equal(*existing, object) &&
          existing_record->second.sealed_objects.count(object.object_id) != 0) {
        record.sealed_objects.insert(object.object_id);
      }
    }
    existing_record->second = std::move(record);
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::declare_object(
    const std::string &epoch_id, uint64_t token,
    const Preserve_trx_transfer_object_descriptor &descriptor) {
  if (!transfer_component_safe(epoch_id) || token == 0 ||
      !transfer_component_safe(descriptor.object_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::lock_guard<std::mutex> guard(m_mutex);
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
  const auto existing = std::find_if(
      found->second.objects.begin(), found->second.objects.end(),
      [&](const Preserve_trx_transfer_object_descriptor &candidate) {
        return candidate.object_id == descriptor.object_id;
      });
  if (existing != found->second.objects.end()) {
    if (transfer_object_descriptor_equal(*existing, descriptor)) {
      return Preserve_trx_transfer_status::OK;
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
    bool cleanup_debt_overflow = false;
    uint64_t other_reserved_bytes =
        cleanup_debt_reserved_bytes_locked(&cleanup_debt_overflow);
    if (cleanup_debt_overflow) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    for (const auto &entry : m_records) {
      if (entry.first == key || entry.second.epoch_id != epoch_id ||
          (entry.second.state !=
               Preserve_trx_transfer_receiver_state::DECLARED &&
           entry.second.state !=
               Preserve_trx_transfer_receiver_state::RECEIVING)) {
        continue;
      }
      if (entry.second.reserved_bytes >
          std::numeric_limits<uint64_t>::max() - other_reserved_bytes) {
        return Preserve_trx_transfer_status::UNSUPPORTED;
      }
      other_reserved_bytes += entry.second.reserved_bytes;
    }
    const uint64_t replacement_reserved_bytes =
        base_reserved_bytes + new_object_bytes;
    if (replacement_reserved_bytes >
            std::numeric_limits<uint64_t>::max() - other_reserved_bytes ||
        other_reserved_bytes + replacement_reserved_bytes >
            preserve_trx_transfer_max_inflight_bytes) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    try {
      *existing = descriptor;
    } catch (...) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
    found->second.reserved_bytes = replacement_reserved_bytes;
    found->second.sealed_objects.erase(descriptor.object_id);
    return Preserve_trx_transfer_status::OK;
  }

  if (new_object_bytes > std::numeric_limits<uint64_t>::max() -
                             found->second.reserved_bytes) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  const uint64_t record_reserved_bytes =
      found->second.reserved_bytes + new_object_bytes;
  bool cleanup_debt_overflow = false;
  uint64_t epoch_reserved_bytes =
      cleanup_debt_reserved_bytes_locked(&cleanup_debt_overflow);
  if (cleanup_debt_overflow) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  for (const auto &entry : m_records) {
    if (entry.first == key || entry.second.epoch_id != epoch_id ||
        (entry.second.state != Preserve_trx_transfer_receiver_state::DECLARED &&
         entry.second.state !=
             Preserve_trx_transfer_receiver_state::RECEIVING)) {
      continue;
    }
    if (entry.second.reserved_bytes >
        std::numeric_limits<uint64_t>::max() - epoch_reserved_bytes) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    epoch_reserved_bytes += entry.second.reserved_bytes;
  }
  if (record_reserved_bytes >
          std::numeric_limits<uint64_t>::max() - epoch_reserved_bytes ||
      epoch_reserved_bytes + record_reserved_bytes >
          preserve_trx_transfer_max_inflight_bytes) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  try {
    found->second.objects.push_back(descriptor);
  } catch (...) {
    return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
  }
  found->second.reserved_bytes = record_reserved_bytes;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_saved_online(
    const std::string &epoch_id, uint64_t token) {
  std::lock_guard<std::mutex> guard(m_mutex);
  const Token_key key(epoch_id, token);
  auto found = m_records.find(key);
  if (found == m_records.end()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (found->second.state ==
      Preserve_trx_transfer_receiver_state::SAVED_ONLINE) {
    return Preserve_trx_transfer_status::OK;
  }
  if (found->second.state != Preserve_trx_transfer_receiver_state::DECLARED &&
      found->second.state != Preserve_trx_transfer_receiver_state::RECEIVING &&
      found->second.state !=
          Preserve_trx_transfer_receiver_state::CLEANUP_PENDING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  found->second.state = Preserve_trx_transfer_receiver_state::SAVED_ONLINE;
  found->second.reserved_bytes = 0;
  found->second.last_error.clear();
  m_cleanup_debts.erase(key);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_cleanup_pending(
    const std::string &root_dir, const std::string &epoch_id, uint64_t token,
    uint64_t now_us, Preserve_trx_transfer_receiver_state target_state,
    const std::string &reason) {
  static constexpr uint64_t kCleanupRetryBaseUs = 1000000;
  if (root_dir.empty() ||
      (target_state != Preserve_trx_transfer_receiver_state::SAVED_ONLINE &&
       target_state != Preserve_trx_transfer_receiver_state::CORRUPT &&
       target_state != Preserve_trx_transfer_receiver_state::ABORTED)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  const Token_key key(epoch_id, token);
  auto found = m_records.find(key);
  if (found == m_records.end()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (found->second.state != Preserve_trx_transfer_receiver_state::DECLARED &&
      found->second.state != Preserve_trx_transfer_receiver_state::RECEIVING &&
      found->second.state !=
          Preserve_trx_transfer_receiver_state::CLEANUP_PENDING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  Cleanup_debt &debt = m_cleanup_debts[key];
  if (debt.attempts == 0) {
    debt.root_dir = root_dir;
    debt.target_state = target_state;
    debt.reserved_bytes = found->second.reserved_bytes;
    debt.attempts = 1;
    debt.next_retry_us =
        now_us > std::numeric_limits<uint64_t>::max() - kCleanupRetryBaseUs
            ? std::numeric_limits<uint64_t>::max()
            : now_us + kCleanupRetryBaseUs;
  } else if (debt.root_dir != root_dir || debt.target_state != target_state) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  found->second.state = Preserve_trx_transfer_receiver_state::CLEANUP_PENDING;
  found->second.reserved_bytes = 0;
  found->second.last_error = reason;
  return Preserve_trx_transfer_status::OK;
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
      record->second.state = debt->second.target_state;
      record->second.reserved_bytes = 0;
      record->second.last_error.clear();
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
      Acknowledged_epoch &ack = m_acknowledged_epochs[epoch_id];
      ack.root_dir = root_dir;
      ack.retire_after_us = retire_after_us;
      ack.spool_deleted = false;
    } catch (...) {
      return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
    }
  }
  const std::string spool_path =
      transfer_receiver_frame_spool_path(root_dir, epoch_id);
  const bool spool_deleted =
      my_delete(spool_path.c_str(), MYF(0)) == 0 || my_errno() == ENOENT;
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    const auto ack = m_acknowledged_epochs.find(epoch_id);
    if (ack != m_acknowledged_epochs.end()) {
      ack->second.spool_deleted = spool_deleted;
    }
  }
  return spool_deleted ? Preserve_trx_transfer_status::OK
                       : Preserve_trx_transfer_status::IO_ERROR;
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
    if (!item.second.spool_deleted) {
      const std::string spool_path = transfer_receiver_frame_spool_path(
          item.second.root_dir, item.first);
      item.second.spool_deleted =
          my_delete(spool_path.c_str(), MYF(0)) == 0 || my_errno() == ENOENT;
    }
    std::lock_guard<std::mutex> guard(m_mutex);
    auto ack = m_acknowledged_epochs.find(item.first);
    if (ack == m_acknowledged_epochs.end()) continue;
    ack->second.spool_deleted = item.second.spool_deleted;
    if (!ack->second.spool_deleted || ack->second.retire_after_us > now_us) {
      continue;
    }
    bool terminal = true;
    for (const auto &record : m_records) {
      if (record.first.first != item.first) continue;
      if (record.second.state !=
              Preserve_trx_transfer_receiver_state::SAVED_ONLINE &&
          record.second.state != Preserve_trx_transfer_receiver_state::CORRUPT &&
          record.second.state != Preserve_trx_transfer_receiver_state::ABORTED) {
        terminal = false;
        break;
      }
    }
    if (!terminal) continue;
    bool has_debt = false;
    for (const auto &debt : m_cleanup_debts) {
      if (debt.first.first == item.first) {
        has_debt = true;
        break;
      }
    }
    if (has_debt) continue;
    for (auto record = m_records.begin(); record != m_records.end();) {
      record = record->first.first == item.first ? m_records.erase(record)
                                                 : std::next(record);
    }
    for (auto frame = m_frame_sequences.begin();
         frame != m_frame_sequences.end();) {
      frame = frame->first.first == item.first
                  ? m_frame_sequences.erase(frame)
                  : std::next(frame);
    }
    m_next_sequence_by_epoch.erase(item.first);
    m_acknowledged_epochs.erase(ack);
    ++retired;
  }
  return retired;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_corrupt(
    const std::string &epoch_id, uint64_t token,
    const std::string &reason) {
  std::lock_guard<std::mutex> guard(m_mutex);
  return mark_terminal_locked(Token_key(epoch_id, token),
                              Preserve_trx_transfer_receiver_state::CORRUPT,
                              reason);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_aborted(
    const std::string &epoch_id, uint64_t token,
    const std::string &reason) {
  std::lock_guard<std::mutex> guard(m_mutex);
  return mark_terminal_locked(Token_key(epoch_id, token),
                              Preserve_trx_transfer_receiver_state::ABORTED,
                              reason);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_object_sealed(
    const std::string &epoch_id, uint64_t token,
    const std::string &object_id) {
  std::lock_guard<std::mutex> guard(m_mutex);
  const Token_key key(epoch_id, token);
  auto found = m_records.find(key);
  if (found == m_records.end()) return Preserve_trx_transfer_status::CORRUPT;
  if (found->second.state != Preserve_trx_transfer_receiver_state::DECLARED &&
      found->second.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  const auto object_found =
      std::find_if(found->second.objects.begin(), found->second.objects.end(),
                   [&](const Preserve_trx_transfer_object_descriptor &object) {
                     return object.object_id == object_id;
                   });
  if (object_found == found->second.objects.end()) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  found->second.sealed_objects.insert(object_id);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::consume_frame_sequence(
    const std::string &epoch_id, uint64_t sequence) {
  if (epoch_id.empty() || sequence == 0 ||
      sequence == std::numeric_limits<uint64_t>::max()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::lock_guard<std::mutex> guard(m_mutex);
  uint64_t &expected = m_next_sequence_by_epoch[epoch_id];
  if (expected == 0) expected = 1;
  if (sequence != expected) return Preserve_trx_transfer_status::CORRUPT;
  expected = sequence + 1;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::admit_frame_sequence(
    const std::string &epoch_id, uint64_t sequence,
    const std::array<unsigned char, kPreservedTrxSha256Length> &digest,
    Preserve_trx_transfer_sequence_admission *admission) {
  if (epoch_id.empty() || sequence == 0 ||
      sequence == std::numeric_limits<uint64_t>::max() ||
      admission == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::lock_guard<std::mutex> guard(m_mutex);
  const auto key = std::make_pair(epoch_id, sequence);
  const auto existing = m_frame_sequences.find(key);
  if (existing != m_frame_sequences.end()) {
    if (existing->second.corrupt || existing->second.digest != digest) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    *admission = existing->second.applied
                     ? Preserve_trx_transfer_sequence_admission::ALREADY_APPLIED
                     : Preserve_trx_transfer_sequence_admission::RETRY_PENDING;
    return Preserve_trx_transfer_status::OK;
  }

  uint64_t &expected = m_next_sequence_by_epoch[epoch_id];
  if (expected == 0) expected = 1;
  if (sequence != expected) return Preserve_trx_transfer_status::CORRUPT;
  Frame_sequence_record record;
  record.digest = digest;
  m_frame_sequences.emplace(key, std::move(record));
  expected = sequence + 1;
  *admission = Preserve_trx_transfer_sequence_admission::NEW_FRAME;
  return Preserve_trx_transfer_status::OK;
}

void Preserve_trx_transfer_receiver_registry::mark_frame_sequence_applied(
    const std::string &epoch_id, uint64_t sequence) {
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_frame_sequences.find(std::make_pair(epoch_id, sequence));
  if (found != m_frame_sequences.end()) found->second.applied = true;
}

void Preserve_trx_transfer_receiver_registry::mark_frame_sequence_corrupt(
    const std::string &epoch_id, uint64_t sequence) {
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_frame_sequences.find(std::make_pair(epoch_id, sequence));
  if (found != m_frame_sequences.end()) found->second.corrupt = true;
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
    const std::string &epoch_id, uint64_t token) const {
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_records.find(Token_key(epoch_id, token));
  if (found == m_records.end() ||
      found->second.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
    return false;
  }
  for (const Preserve_trx_transfer_object_descriptor &object :
       found->second.objects) {
    if (found->second.sealed_objects.count(object.object_id) == 0) {
      return false;
    }
  }
  return true;
}

bool Preserve_trx_transfer_receiver_registry::all_receiving_tokens_sealed(
    const std::string &epoch_id) const {
  std::lock_guard<std::mutex> guard(m_mutex);
  for (const auto &entry : m_records) {
    const Preserve_trx_transfer_receiver_record &record = entry.second;
    if (record.epoch_id != epoch_id) {
      continue;
    }
    if (record.state == Preserve_trx_transfer_receiver_state::DECLARED) {
      return false;
    }
    if (record.state != Preserve_trx_transfer_receiver_state::RECEIVING)
      continue;
    for (const Preserve_trx_transfer_object_descriptor &object :
         record.objects) {
      if (record.sealed_objects.count(object.object_id) == 0) return false;
    }
  }
  return true;
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
  std::lock_guard<std::mutex> guard(m_mutex);
  const auto found = m_records.find(Token_key(epoch_id, token));
  if (found == m_records.end()) return false;
  *record = found->second;
  return true;
}

size_t Preserve_trx_transfer_receiver_registry::size() const {
  std::lock_guard<std::mutex> guard(m_mutex);
  return m_records.size();
}

Preserve_trx_transfer_status
Preserve_trx_transfer_receiver_registry::mark_terminal_locked(
    const Token_key &key, Preserve_trx_transfer_receiver_state state,
    const std::string &reason) {
  auto found = m_records.find(key);
  if (found == m_records.end()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (found->second.state != Preserve_trx_transfer_receiver_state::DECLARED &&
      found->second.state != Preserve_trx_transfer_receiver_state::RECEIVING) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  found->second.state = state;
  found->second.reserved_bytes = 0;
  found->second.last_error = reason;
  return Preserve_trx_transfer_status::OK;
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
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, const Preserved_trx_bundle &bundle,
    uint64_t transfer_token, Preserve_trx_transfer_manifest *manifest,
    std::vector<Preserve_trx_transfer_object_payload> *objects,
    const std::set<std::string> *presealed_prebuilt_objects) {
  if (manifest == nullptr || objects == nullptr || transfer_token == 0) {
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           "PRESERVE: standby transfer portable object build invalid argument");
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (!bundle.metadata.temp_table_manifest_payload.empty()) {
    /*
      User temporary table state is not portable until the receiver can install
      both image and no-redo-undo sidecars before publishing .standby_pending.
      Rejecting here prevents a marker from advertising an artifact whose
      snapshot references local-only sidecar paths.
    */
    return Preserve_trx_transfer_status::UNSUPPORTED;
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
  built_manifest.source_server_uuid = source_server_uuid;
  built_manifest.target_server_uuid = target_server_uuid;
  built_manifest.token = transfer_token;
  if (!load_source_transfer_lsn_fact(&built_manifest.source_prepare_lsn,
                                     &built_manifest.source_epoch_commit_lsn)) {
    const std::string message =
        "PRESERVE: standby transfer source LSN fact unavailable token=" +
        bundle.metadata.token;
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  std::vector<Preserve_trx_transfer_object_payload> built_objects;
  std::set<std::string> object_ids;

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
    object.descriptor.object_id = blob.name;
    object.descriptor.kind = Preserve_trx_transfer_object_kind::EXTERNAL_BLOB;
    if (blob.prebuilt) {
      if (presealed_prebuilt_objects == nullptr ||
          presealed_prebuilt_objects->count(blob.name) == 0 ||
          blob.descriptor.name != blob.name || blob.descriptor.size == 0 ||
          !blob.payload.empty()) {
        return Preserve_trx_transfer_status::UNSUPPORTED;
      }
      object.descriptor.total_size = blob.descriptor.size;
      object.descriptor.digest = blob.descriptor.digest;
      built_manifest.objects.push_back(object.descriptor);
      continue;
    }
    object.descriptor.total_size = blob.payload.length();
    object.descriptor.digest = sha256_digest(blob.payload);
    object.payload = blob.payload;
    built_manifest.objects.push_back(object.descriptor);
    built_objects.push_back(std::move(object));
  }

  const Preserve_trx_transfer_status validation_status =
      validate_manifest_components(built_manifest, false);
  if (validation_status != Preserve_trx_transfer_status::OK) {
    const std::string message =
        "PRESERVE: standby transfer manifest validation failed status=" +
        transfer_status_name(validation_status) +
        " epoch=" + built_manifest.epoch_id +
        " source=" + built_manifest.source_server_uuid +
        " target=" + built_manifest.target_server_uuid +
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
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, const Preserved_trx_bundle &bundle,
    uint64_t transfer_token, Preserve_trx_transfer_manifest *manifest,
    std::vector<Preserve_trx_transfer_object_payload> *objects) {
  return preserve_trx_transfer_build_portable_objects_impl(
      epoch_id, source_server_uuid, target_server_uuid, bundle, transfer_token,
      manifest, objects, nullptr);
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
  const std::string &source_server_uuid = manifests.front().source_server_uuid;
  const std::string &target_server_uuid = manifests.front().target_server_uuid;
  std::set<uint64_t> tokens;
  for (const Preserve_trx_transfer_manifest &manifest : manifests) {
    Preserve_trx_transfer_status status =
        validate_manifest_components(manifest, false);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (manifest.epoch_id != epoch_id ||
        manifest.source_server_uuid != source_server_uuid ||
        manifest.target_server_uuid != target_server_uuid ||
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
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, uint64_t transfer_token,
    Preserve_trx_transfer_encoded_frame_sink *sink, uint64_t *next_sequence) {
  if (sink == nullptr || next_sequence == nullptr || *next_sequence == 0 ||
      !transfer_component_safe(epoch_id) ||
      !transfer_component_safe(source_server_uuid) ||
      !transfer_component_safe(target_server_uuid) || transfer_token == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  Preserve_trx_transfer_frame declare;
  declare.type = Preserve_trx_transfer_frame_type::DECLARE_TOKEN;
  declare.sequence = *next_sequence;
  declare.epoch_id = epoch_id;
  declare.token = transfer_token;
  declare.reason =
      encode_declare_token_reason(source_server_uuid, target_server_uuid);
  const Preserve_trx_transfer_status status =
      send_encoded_transfer_frame(sink, declare);
  if (status == Preserve_trx_transfer_status::OK) ++*next_sequence;
  return status;
}

Preserve_trx_transfer_status
preserve_trx_transfer_send_epoch_declare_object_frame(
    const std::string &epoch_id, uint64_t transfer_token,
    const Preserve_trx_transfer_object_descriptor &descriptor,
    Preserve_trx_transfer_encoded_frame_sink *sink, uint64_t *next_sequence) {
  if (sink == nullptr || next_sequence == nullptr || *next_sequence == 0 ||
      !transfer_component_safe(epoch_id) || transfer_token == 0 ||
      !transfer_component_safe(descriptor.object_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::string descriptor_payload;
  Preserve_trx_transfer_status status =
      encode_transfer_object_descriptor(descriptor, &descriptor_payload);
  if (status != Preserve_trx_transfer_status::OK) return status;

  Preserve_trx_transfer_frame declare;
  declare.type = Preserve_trx_transfer_frame_type::DECLARE_OBJECT;
  declare.sequence = *next_sequence;
  declare.epoch_id = epoch_id;
  declare.token = transfer_token;
  declare.object_id = descriptor.object_id;
  declare.manifest_payload = std::move(descriptor_payload);
  status = send_encoded_transfer_frame(sink, declare);
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
  const Preserve_trx_transfer_status status =
      send_encoded_transfer_frame(sink, commit);
  if (status == Preserve_trx_transfer_status::OK) ++*next_sequence;
  return status;
}

Preserve_trx_transfer_source_epoch_session::
    Preserve_trx_transfer_source_epoch_session(
        const std::string &epoch_id, const std::string &source_server_uuid,
        const std::string &target_server_uuid, uint32_t chunk_bytes,
        Preserve_trx_transfer_encoded_frame_sink *sink)
    : m_epoch_id(epoch_id),
      m_source_server_uuid(source_server_uuid),
      m_target_server_uuid(target_server_uuid),
      m_chunk_bytes(chunk_bytes),
      m_max_inflight_bytes(preserve_trx_transfer_max_inflight_bytes),
      m_phase1_batch_bytes(preserve_trx_transfer_phase1_batch_bytes),
      m_sink(sink) {}

Preserve_trx_transfer_source_epoch_session::
    Preserve_trx_transfer_source_epoch_session(
        const std::string &epoch_id, const std::string &source_server_uuid,
        const std::string &target_server_uuid,
        const Preserve_trx_transfer_source_epoch_options &options,
        Preserve_trx_transfer_encoded_frame_sink *sink)
    : m_epoch_id(epoch_id),
      m_source_server_uuid(source_server_uuid),
      m_target_server_uuid(target_server_uuid),
      m_chunk_bytes(options.chunk_bytes),
      m_max_inflight_bytes(options.max_inflight_bytes),
      m_phase1_batch_bytes(options.phase1_batch_bytes),
      m_sink(sink) {}

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
Preserve_trx_transfer_source_epoch_session::emit_frame_locked(
    Preserve_trx_transfer_frame frame, bool queue_final_metadata) {
  if (m_sink == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  frame.sequence = m_next_sequence;
  if (queue_final_metadata) {
    m_pending_final_metadata_frames.push_back(std::move(frame));
    ++m_next_sequence;
    return Preserve_trx_transfer_status::OK;
  }
  std::string encoded_frame;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_encode_frame(frame, &encoded_frame);
  if (status != Preserve_trx_transfer_status::OK) return status;
  status = m_sink->send_encoded_frame(encoded_frame);
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
    std::vector<std::string> batch(encoded_frames.begin() + first,
                                   encoded_frames.begin() + last);
    std::string encoded_batch;
    Preserve_trx_transfer_status status = encode_frame_batch_with_limit(
        batch, m_max_inflight_bytes, &encoded_batch);
    if (status != Preserve_trx_transfer_status::OK) return status;
    status = m_sink->send_encoded_frame(encoded_batch);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (m_phase1_metrics_enabled) {
      note_source_phase1_network_send(batch.size(), encoded_batch.length(),
                                      batch.size(), true);
    }
    *acknowledged_frame_count += batch.size();
    first = last;
  }
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::declare_token(
    uint64_t transfer_token) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_chunk_bytes == 0 ||
      !transfer_component_safe(m_epoch_id) ||
      !transfer_component_safe(m_source_server_uuid) ||
      !transfer_component_safe(m_target_server_uuid) || transfer_token == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_chunk_bytes > kMaxTransferChunkBytes) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (m_epoch_committed) return Preserve_trx_transfer_status::UNSUPPORTED;
  if (token_declared(transfer_token) || token_resolved(transfer_token)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserve_trx_transfer_frame declare;
  declare.type = Preserve_trx_transfer_frame_type::DECLARE_TOKEN;
  declare.epoch_id = m_epoch_id;
  declare.token = transfer_token;
  declare.reason =
      encode_declare_token_reason(m_source_server_uuid, m_target_server_uuid);
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
      !transfer_component_safe(m_epoch_id) ||
      !transfer_component_safe(m_source_server_uuid) ||
      !transfer_component_safe(m_target_server_uuid) || m_epoch_committed) {
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
    declare.reason =
        encode_declare_token_reason(m_source_server_uuid, m_target_server_uuid);
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
    const Preserve_trx_transfer_object_descriptor &descriptor) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || transfer_token == 0 ||
      !transfer_component_safe(descriptor.object_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed || !token_declared(transfer_token) ||
      token_resolved(transfer_token) ||
      m_streaming_manifests.count(transfer_token) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  auto &objects = m_streaming_declared_objects[transfer_token];
  auto existing = objects.find(descriptor.object_id);
  if (existing != objects.end() &&
      transfer_object_descriptor_equal(existing->second, descriptor)) {
    return Preserve_trx_transfer_status::OK;
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
  status = emit_frame_locked(std::move(declare), false);
  if (status != Preserve_trx_transfer_status::OK) return status;
  if (existing != objects.end()) {
    existing->second = descriptor;
    m_streaming_sealed_objects[transfer_token].erase(descriptor.object_id);
  } else {
    objects.emplace(descriptor.object_id, descriptor);
  }
  m_streaming_object_written_bytes[transfer_token][descriptor.object_id] = 0;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::begin_token_objects(
    const Preserve_trx_transfer_manifest &manifest, bool queue_final_metadata) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_chunk_bytes == 0 || manifest.token == 0 ||
      manifest.epoch_id != m_epoch_id ||
      manifest.source_server_uuid != m_source_server_uuid ||
      manifest.target_server_uuid != m_target_server_uuid) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed || !token_declared(manifest.token) ||
      token_resolved(manifest.token) ||
      m_streaming_manifests.count(manifest.token) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserve_trx_transfer_status status = validate_manifest_components(
      manifest, false);
  if (status != Preserve_trx_transfer_status::OK) return status;

  const auto declared_it = m_streaming_declared_objects.find(manifest.token);
  if (declared_it != m_streaming_declared_objects.end()) {
    for (const auto &entry : declared_it->second) {
      const Preserve_trx_transfer_object_descriptor *descriptor =
          find_object(manifest, entry.first);
      if (descriptor == nullptr ||
          !transfer_object_descriptor_equal(*descriptor, entry.second)) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
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
    uint64_t transfer_token) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_chunk_bytes == 0 || transfer_token == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed || !token_declared(transfer_token) ||
      token_resolved(transfer_token) ||
      m_streaming_manifests.count(transfer_token) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (m_prewarm_manifest_tokens.count(transfer_token) != 0) {
    return Preserve_trx_transfer_status::OK;
  }

  const auto declared_it = m_streaming_declared_objects.find(transfer_token);
  if (declared_it == m_streaming_declared_objects.end() ||
      declared_it->second.empty()) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserve_trx_transfer_manifest manifest;
  manifest.epoch_id = m_epoch_id;
  manifest.source_server_uuid = m_source_server_uuid;
  manifest.target_server_uuid = m_target_server_uuid;
  manifest.token = transfer_token;
  if (!load_source_transfer_lsn_fact(&manifest.source_prepare_lsn,
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
  begin.epoch_id = manifest.epoch_id;
  begin.token = manifest.token;
  begin.manifest_payload = std::move(manifest_payload);
  status = emit_frame_locked(std::move(begin), false);
  if (status != Preserve_trx_transfer_status::OK) return status;

  m_prewarm_manifest_tokens.insert(transfer_token);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::begin_token_prewarm_manifests_batch(
    const std::vector<uint64_t> &transfer_tokens) {
  if (transfer_tokens.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_chunk_bytes == 0 || m_epoch_committed) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::vector<std::string> encoded_frames;
  std::vector<uint64_t> newly_started;
  encoded_frames.reserve(transfer_tokens.size());
  newly_started.reserve(transfer_tokens.size());
  const uint64_t first_sequence = m_next_sequence;
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
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }

    Preserve_trx_transfer_manifest manifest;
    manifest.epoch_id = m_epoch_id;
    manifest.source_server_uuid = m_source_server_uuid;
    manifest.target_server_uuid = m_target_server_uuid;
    manifest.token = transfer_token;
    if (!load_source_transfer_lsn_fact(&manifest.source_prepare_lsn,
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
    begin.sequence = m_next_sequence++;
    begin.epoch_id = manifest.epoch_id;
    begin.token = manifest.token;
    begin.manifest_payload = std::move(manifest_payload);
    std::string encoded_frame;
    status = preserve_trx_transfer_encode_frame(begin, &encoded_frame);
    if (status != Preserve_trx_transfer_status::OK) return status;
    encoded_frames.push_back(std::move(encoded_frame));
    newly_started.push_back(transfer_token);
  }
  if (encoded_frames.empty()) return Preserve_trx_transfer_status::OK;

  size_t acknowledged_frame_count = 0;
  Preserve_trx_transfer_status status = send_phase1_control_batches_locked(
      encoded_frames, &acknowledged_frame_count);
  m_next_sequence = first_sequence + acknowledged_frame_count;
  m_prewarm_manifest_tokens.insert(
      newly_started.begin(),
      newly_started.begin() + acknowledged_frame_count);
  if (status != Preserve_trx_transfer_status::OK) return status;
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::write_object_chunk(
    uint64_t transfer_token, const std::string &object_id,
    uint64_t chunk_offset, const std::string &chunk_payload) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || transfer_token == 0 || object_id.empty() ||
      chunk_payload.empty()) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  auto manifest_it = m_streaming_manifests.find(transfer_token);
  if (m_epoch_committed || token_resolved(transfer_token) ||
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
  if (m_epoch_committed || token_resolved(transfer_token) ||
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

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::finalize_token_manifest(
    uint64_t transfer_token) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (transfer_token == 0) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  auto manifest_it = m_streaming_manifests.find(transfer_token);
  if (m_epoch_committed || token_resolved(transfer_token) ||
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
  m_final_metadata_tokens.erase(transfer_token);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::send_token_objects_locked(
    const Preserve_trx_transfer_manifest &manifest,
    const std::vector<Preserve_trx_transfer_object_payload> &objects,
    bool queue_final_metadata) {
  if (m_sink == nullptr || m_chunk_bytes == 0 || manifest.token == 0 ||
      manifest.epoch_id != m_epoch_id ||
      manifest.source_server_uuid != m_source_server_uuid ||
      manifest.target_server_uuid != m_target_server_uuid) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed) return Preserve_trx_transfer_status::UNSUPPORTED;
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
  std::lock_guard<std::mutex> guard(m_mutex);
  return send_token_objects_locked(manifest, objects, false);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::send_token_objects_batch(
    const Preserve_trx_transfer_manifest &manifest,
    const std::vector<Preserve_trx_transfer_object_payload> &objects,
    const std::set<std::string> &presealed_objects,
    bool queue_final_metadata) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_chunk_bytes == 0 || manifest.token == 0 ||
      manifest.epoch_id != m_epoch_id ||
      manifest.source_server_uuid != m_source_server_uuid ||
      manifest.target_server_uuid != m_target_server_uuid) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed || !token_declared(manifest.token) ||
      token_resolved(manifest.token) ||
      m_streaming_manifests.count(manifest.token) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserve_trx_transfer_status status =
      validate_manifest_components(manifest, false);
  if (status != Preserve_trx_transfer_status::OK) return status;

  const auto declared_it = m_streaming_declared_objects.find(manifest.token);
  if (declared_it != m_streaming_declared_objects.end()) {
    for (const auto &entry : declared_it->second) {
      if (presealed_objects.count(entry.first) == 0) continue;
      const Preserve_trx_transfer_object_descriptor *descriptor =
          find_object(manifest, entry.first);
      if (descriptor == nullptr ||
          !transfer_object_descriptor_equal(*descriptor, entry.second)) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
    }
  }

  auto object_payload_for =
      [&](const Preserve_trx_transfer_object_descriptor &descriptor)
      -> const Preserve_trx_transfer_object_payload * {
    for (const Preserve_trx_transfer_object_payload &candidate : objects) {
      if (candidate.descriptor.object_id == descriptor.object_id) {
        return &candidate;
      }
    }
    return nullptr;
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
      } else {
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
    for (Preserve_trx_transfer_frame &frame : frames) {
      status = emit_frame_locked(std::move(frame), true);
      if (status != Preserve_trx_transfer_status::OK) return status;
    }
  } else {
    const uint64_t first_sequence = m_next_sequence;
    auto sequence_guard = create_scope_guard(
        [&] { m_next_sequence = first_sequence; });
    std::vector<std::string> encoded_frames;
    encoded_frames.reserve(frames.size());
    for (Preserve_trx_transfer_frame &frame : frames) {
      frame.sequence = m_next_sequence++;
      std::string encoded_frame;
      status = preserve_trx_transfer_encode_frame(frame, &encoded_frame);
      if (status != Preserve_trx_transfer_status::OK) return status;
      encoded_frames.push_back(std::move(encoded_frame));
    }

    std::string encoded_batch;
    status =
        encode_frame_batch_with_limit(encoded_frames, m_max_inflight_bytes,
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
  if (m_epoch_committed) return Preserve_trx_transfer_status::UNSUPPORTED;
  if (!token_declared(transfer_token) || token_resolved(transfer_token)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserve_trx_transfer_manifest built_manifest;
  std::vector<Preserve_trx_transfer_object_payload> objects;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_build_portable_objects(
          m_epoch_id, m_source_server_uuid, m_target_server_uuid, bundle,
          transfer_token, &built_manifest, &objects);
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
    const std::array<unsigned char, kPreservedTrxSha256Length> &digest) {
  if (session == nullptr || session->chunk_bytes() == 0 ||
      transfer_token == 0 || expected_name.empty() || warmcopy_id.empty() ||
      size == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  Preserve_trx_transfer_object_descriptor descriptor;
  descriptor.object_id = expected_name;
  descriptor.kind = Preserve_trx_transfer_object_kind::EXTERNAL_BLOB;
  descriptor.total_size = size;
  descriptor.digest = digest;
  if (session->object_presealed_for_token(transfer_token, descriptor)) {
    return Preserve_trx_transfer_status::OK;
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
      session->declare_object(transfer_token, descriptor);
  if (status != Preserve_trx_transfer_status::OK) return status;
  for (uint64_t offset = 0; offset < materialized.payload.length();
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
  };
  const std::string source_dir =
      preserve_dir.empty() ? preserved_trx_dir_value() : preserve_dir;
  std::unique_ptr<Preserved_trx_warm_external_blob_carrier> warm_carrier =
      create_preserved_trx_default_warm_external_blob_carrier(source_dir);
  if (warm_carrier == nullptr) return Preserve_trx_transfer_status::IO_ERROR;

  std::vector<Materialized_request> materialized_requests;
  materialized_requests.reserve(requests.size());
  for (const Preserve_trx_transfer_phase1_blob_request &request : requests) {
    if (request.transfer_token == 0 || request.object_id.empty() ||
        request.warmcopy_id.empty() || request.warmcopy_epoch == 0 ||
        request.size == 0) {
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
    }
    Preserved_trx_external_blob_descriptor warm_descriptor;
    warm_descriptor.name = request.object_id;
    warm_descriptor.size = request.size;
    warm_descriptor.digest = request.digest;
    Preserved_trx_external_blob materialized;
    const Preserved_trx_carrier_status carrier_status =
        warm_carrier->read_warm_external_blob(
            request.warmcopy_id, request.object_id, request.warmcopy_epoch,
            warm_descriptor, m_max_inflight_bytes,
            &materialized);
    if (carrier_status != Preserved_trx_carrier_status::OK) {
      return map_carrier_status_to_transfer(carrier_status);
    }
    if (materialized.payload.length() != request.size ||
        materialized.descriptor.name != request.object_id ||
        materialized.descriptor.size != request.size ||
        materialized.descriptor.digest != request.digest) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    Materialized_request prepared;
    prepared.request = request;
    prepared.descriptor.object_id = request.object_id;
    prepared.descriptor.kind =
        Preserve_trx_transfer_object_kind::EXTERNAL_BLOB;
    prepared.descriptor.total_size = request.size;
    prepared.descriptor.digest = request.digest;
    prepared.payload = std::move(materialized.payload);
    materialized_requests.push_back(std::move(prepared));
  }

  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || m_chunk_bytes == 0 || m_epoch_committed) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::vector<std::string> encoded_frames;
  std::vector<size_t> sent_request_indexes;
  const uint64_t first_sequence = m_next_sequence;
  auto sequence_guard = create_scope_guard(
      [&] { m_next_sequence = first_sequence; });
  for (size_t request_index = 0;
       request_index < materialized_requests.size(); ++request_index) {
    const Materialized_request &prepared =
        materialized_requests[request_index];
    const uint64_t transfer_token = prepared.request.transfer_token;
    if (!token_declared(transfer_token) || token_resolved(transfer_token) ||
        m_streaming_manifests.count(transfer_token) != 0) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    bool already_presealed = false;
    const auto declared_token =
        m_streaming_declared_objects.find(transfer_token);
    if (declared_token != m_streaming_declared_objects.end()) {
      const auto declared_object =
          declared_token->second.find(prepared.descriptor.object_id);
      const auto sealed_token = m_streaming_sealed_objects.find(transfer_token);
      const auto written_token =
          m_streaming_object_written_bytes.find(transfer_token);
      if (declared_object != declared_token->second.end() &&
          transfer_object_descriptor_equal(declared_object->second,
                                           prepared.descriptor) &&
          sealed_token != m_streaming_sealed_objects.end() &&
          sealed_token->second.count(prepared.descriptor.object_id) != 0 &&
          written_token != m_streaming_object_written_bytes.end()) {
        const auto written_object =
            written_token->second.find(prepared.descriptor.object_id);
        already_presealed =
            written_object != written_token->second.end() &&
            written_object->second == prepared.descriptor.total_size;
      }
    }
    if (already_presealed) continue;

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
    std::string encoded_declare;
    status = preserve_trx_transfer_encode_frame(declare, &encoded_declare);
    if (status != Preserve_trx_transfer_status::OK) return status;
    encoded_frames.push_back(std::move(encoded_declare));

    for (uint64_t offset = 0; offset < prepared.payload.length();
         offset += m_chunk_bytes) {
      const size_t length = std::min<uint64_t>(
          m_chunk_bytes, prepared.payload.length() - offset);
      Preserve_trx_transfer_frame chunk;
      chunk.type = Preserve_trx_transfer_frame_type::OBJECT_CHUNK;
      chunk.sequence = m_next_sequence++;
      chunk.epoch_id = m_epoch_id;
      chunk.token = transfer_token;
      chunk.object_id = prepared.descriptor.object_id;
      chunk.chunk_offset = offset;
      chunk.chunk_payload = prepared.payload.substr(offset, length);
      std::string encoded_chunk;
      status = preserve_trx_transfer_encode_frame(chunk, &encoded_chunk);
      if (status != Preserve_trx_transfer_status::OK) return status;
      encoded_frames.push_back(std::move(encoded_chunk));
    }

    Preserve_trx_transfer_frame seal;
    seal.type = Preserve_trx_transfer_frame_type::SEAL_OBJECT;
    seal.sequence = m_next_sequence++;
    seal.epoch_id = m_epoch_id;
    seal.token = transfer_token;
    seal.object_id = prepared.descriptor.object_id;
    std::string encoded_seal;
    status = preserve_trx_transfer_encode_frame(seal, &encoded_seal);
    if (status != Preserve_trx_transfer_status::OK) return status;
    encoded_frames.push_back(std::move(encoded_seal));
    sent_request_indexes.push_back(request_index);
  }
  if (encoded_frames.empty()) {
    sequence_guard.commit();
    return Preserve_trx_transfer_status::OK;
  }

  std::string encoded_batch;
  Preserve_trx_transfer_status status =
      encode_frame_batch_with_limit(encoded_frames, m_max_inflight_bytes,
                                    &encoded_batch);
  if (status != Preserve_trx_transfer_status::OK) return status;
  status = m_sink->send_encoded_frame(encoded_batch);
  if (status != Preserve_trx_transfer_status::OK) return status;
  sequence_guard.commit();
  if (m_phase1_metrics_enabled) {
    note_source_phase1_network_send(encoded_frames.size(),
                                    encoded_batch.length(),
                                    sent_request_indexes.size(), true);
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
  if (max_batch_bytes == 0 ||
      (requests.size() == 1 && requests.front().size > max_batch_bytes)) {
    if (max_batch_bytes != 0 && requests.size() == 1 &&
        requests.front().size > max_batch_bytes) {
      g_transfer_phase1_oversize_token_count.fetch_add(1);
    }
    for (const Preserve_trx_transfer_phase1_blob_request &request : requests) {
      const Preserve_trx_transfer_status status =
          stream_prebuilt_external_blob_for_transfer(
              session, request.transfer_token, preserve_dir, request.object_id,
              request.warmcopy_id, request.warmcopy_epoch, request.size,
              request.digest);
      if (status != Preserve_trx_transfer_status::OK) return status;
      if (request.object_id == kPreservedTrxBlobRecordLocks) {
        note_source_phase1_record_batch_sent(1);
      }
    }
    return Preserve_trx_transfer_status::OK;
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
  return stream_prebuilt_external_blob_for_transfer(
      session, transfer_token, preserve_dir, blob.name, blob.warmcopy_id,
      blob.warmcopy_epoch, blob.size, blob.digest);
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
Preserve_trx_transfer_source_epoch_session::abort_token_locked(
    uint64_t transfer_token, const std::string &reason, bool allow_finalized) {
  if (m_sink == nullptr || !transfer_component_safe(m_epoch_id) ||
      transfer_token == 0) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed) return Preserve_trx_transfer_status::UNSUPPORTED;
  const bool finalized = m_finalized_tokens.count(transfer_token) != 0;
  if (!token_declared(transfer_token) ||
      m_aborted_tokens.count(transfer_token) != 0 ||
      (finalized && !allow_finalized)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  if (!allow_finalized && m_final_metadata_tokens.count(transfer_token) != 0) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }

  Preserve_trx_transfer_frame abort;
  abort.type = Preserve_trx_transfer_frame_type::ABORT;
  abort.sequence = m_next_sequence;
  abort.epoch_id = m_epoch_id;
  abort.token = transfer_token;
  abort.reason = reason;
  Preserve_trx_transfer_status status =
      send_encoded_transfer_frame(m_sink, abort);
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
  m_pending_final_metadata_frames.erase(
      std::remove_if(m_pending_final_metadata_frames.begin(),
                     m_pending_final_metadata_frames.end(),
                     [&](const Preserve_trx_transfer_frame &frame) {
                       return frame.token == transfer_token;
                     }),
      m_pending_final_metadata_frames.end());
  m_streaming_manifests.erase(transfer_token);
  m_streaming_declared_objects.erase(transfer_token);
  m_streaming_object_written_bytes.erase(transfer_token);
  m_streaming_sealed_objects.erase(transfer_token);
  m_final_metadata_tokens.erase(transfer_token);
  m_aborted_tokens.insert(transfer_token);
  return Preserve_trx_transfer_status::OK;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::abort_token(
    uint64_t transfer_token, const std::string &reason) {
  std::lock_guard<std::mutex> guard(m_mutex);
  return abort_token_locked(transfer_token, reason, false);
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::abort_epoch(
    const std::string &reason) {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || !transfer_component_safe(m_epoch_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed || m_declared_tokens.empty()) {
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
    m_next_sequence = first_pending_sequence;
  }

  Preserve_trx_transfer_status first_error = Preserve_trx_transfer_status::OK;
  for (uint64_t transfer_token : m_declared_tokens) {
    if (m_aborted_tokens.count(transfer_token) != 0) continue;
    const Preserve_trx_transfer_status status =
        abort_token_locked(transfer_token, reason, true);
    if (status != Preserve_trx_transfer_status::OK &&
        first_error == Preserve_trx_transfer_status::OK) {
      first_error = status;
    }
  }
  return first_error;
}

Preserve_trx_transfer_status
Preserve_trx_transfer_source_epoch_session::commit_epoch() {
  std::lock_guard<std::mutex> guard(m_mutex);
  if (m_sink == nullptr || !transfer_component_safe(m_epoch_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (m_epoch_committed) return Preserve_trx_transfer_status::UNSUPPORTED;
  if (m_declared_tokens.empty() || m_finalized_tokens.empty()) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  for (uint64_t transfer_token : m_declared_tokens) {
    if (!token_resolved(transfer_token)) {
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
  }
  Preserve_trx_transfer_frame commit;
  commit.type = Preserve_trx_transfer_frame_type::COMMIT_EPOCH;
  commit.epoch_id = m_epoch_id;
  commit.token = *m_finalized_tokens.begin();
  commit.sequence = m_next_sequence;
  uint64_t sampled_prepare_lsn = 0;
  uint64_t source_fence_lsn = 0;
  if (!load_source_transfer_lsn_fact(&sampled_prepare_lsn,
                                     &source_fence_lsn) ||
      sampled_prepare_lsn > source_fence_lsn) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  for (const auto &manifest : m_finalized_manifests) {
    if (manifest.source_prepare_lsn == 0 ||
        manifest.source_epoch_commit_lsn == 0 ||
        manifest.source_prepare_lsn > source_fence_lsn ||
        manifest.source_epoch_commit_lsn > source_fence_lsn) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }
  commit.chunk_offset = source_fence_lsn;

  const uint64_t ack_start_us = transfer_monotonic_us();
  Preserve_trx_transfer_status status = Preserve_trx_transfer_status::OK;
  if (m_pending_final_metadata_frames.empty()) {
    std::string encoded_commit;
    status = preserve_trx_transfer_encode_frame(commit, &encoded_commit);
    if (status != Preserve_trx_transfer_status::OK) return status;
    g_transfer_phase2_final_metadata_frame_count.fetch_add(1);
    g_transfer_phase2_final_metadata_encoded_bytes.fetch_add(
        encoded_commit.length());
    status = send_encoded_transfer_frame(m_sink, commit);
  } else {
    std::vector<std::string> encoded_frames;
    encoded_frames.reserve(m_pending_final_metadata_frames.size() + 1);
    for (const Preserve_trx_transfer_frame &frame :
         m_pending_final_metadata_frames) {
      std::string encoded_frame;
      status = preserve_trx_transfer_encode_frame(frame, &encoded_frame);
      if (status != Preserve_trx_transfer_status::OK) return status;
      encoded_frames.push_back(std::move(encoded_frame));
    }
    std::string encoded_commit;
    status = preserve_trx_transfer_encode_frame(commit, &encoded_commit);
    if (status != Preserve_trx_transfer_status::OK) return status;
    encoded_frames.push_back(std::move(encoded_commit));

    std::string encoded_batch;
    status =
        encode_frame_batch_with_limit(encoded_frames, m_max_inflight_bytes,
                                      &encoded_batch);
    if (status != Preserve_trx_transfer_status::OK) return status;
    g_transfer_phase2_final_metadata_frame_count.fetch_add(
        encoded_frames.size());
    g_transfer_phase2_final_metadata_encoded_bytes.fetch_add(
        encoded_batch.length());
    status = m_sink->send_encoded_frame(encoded_batch);
  }
  const uint64_t ack_end_us = transfer_monotonic_us();
  if (status == Preserve_trx_transfer_status::OK && ack_end_us >= ack_start_us) {
    g_transfer_phase2_final_metadata_ack_us.fetch_add(ack_end_us - ack_start_us);
  }
  if (status == Preserve_trx_transfer_status::OK) {
    ++m_next_sequence;
    m_pending_final_metadata_frames.clear();
    m_epoch_committed = true;
  }
  return status;
}

Preserve_trx_transfer_status
preserve_trx_transfer_build_encoded_frame_sequence(
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, const Preserved_trx_bundle &bundle,
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
          epoch_id, source_server_uuid, target_server_uuid, bundle,
          transfer_token,
          &built_manifest, &objects);
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
    std::unique_ptr<Preserve_trx_transfer_encoded_frame_sink> *sink) {
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
  sink->reset(new Preserve_trx_transfer_client_frame_sink(
      configured_transfer_client_endpoint(), ops));
  return Preserve_trx_transfer_status::OK;
}

void preserve_trx_transfer_set_client_ops_for_unit_test(
    const Preserve_trx_transfer_client_ops *ops) {
  unit_transfer_client_ops() = ops;
}

void preserve_trx_transfer_set_codec_context_provider_for_unit_test(
    Preserve_trx_transfer_codec_context_provider provider) {
  unit_codec_context_provider() = provider;
}

void preserve_trx_transfer_set_source_lsn_provider_for_unit_test(
    Preserve_trx_transfer_source_lsn_provider provider) {
  unit_source_lsn_provider() = provider;
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
    uint64_t bitmap_bits) {
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

  Receiver_object_prewarm_proof proof;
  proof.record_lock_object = object_id == kPreservedTrxBlobRecordLocks;
  proof.page_count = page_count;
  proof.resident_pages = resident_pages;
  proof.cold_gets = cold_gets;
  proof.bitmap_pages = bitmap_pages;
  proof.bitmap_bits = bitmap_bits;

  std::lock_guard<std::mutex> guard(g_receiver_object_prewarm_proof_mutex);
  g_receiver_object_prewarm_proofs[key] = proof;
}

Preserve_trx_transfer_status preserve_trx_transfer_send_bundle_frames(
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, const Preserved_trx_bundle &bundle,
    uint64_t transfer_token, uint32_t chunk_bytes,
    Preserve_trx_transfer_encoded_frame_sink *sink,
    Preserve_trx_transfer_manifest *manifest) {
  if (sink == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  Preserve_trx_transfer_manifest built_manifest;
  std::vector<Preserve_trx_transfer_object_payload> objects;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_build_portable_objects(
          epoch_id, source_server_uuid, target_server_uuid, bundle,
          transfer_token,
          &built_manifest, &objects);
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
    return Preserve_trx_transfer_status::UNSUPPORTED;
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
  declare.reason =
      encode_declare_token_reason(source_server_uuid, target_server_uuid);
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
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid,
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
            epoch_id, source_server_uuid, target_server_uuid, bundle,
            transfer_tokens[tokens.size()],
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
    return Preserve_trx_transfer_status::UNSUPPORTED;
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
    declare.reason = encode_declare_token_reason(source_server_uuid,
                                                 target_server_uuid);
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
    const Preserve_trx_transfer_status status = send_frame(commit);
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

  std::mutex &object_mutex =
      transfer_object_stage_mutex(root_dir, manifest, object_id);
  std::lock_guard<std::mutex> object_guard(object_mutex);

  const std::string path = transfer_object_path(root_dir, manifest, *object);
  MY_STAT stat_area;
  if (file_exists(path, &stat_area) &&
      chunk_offset < static_cast<uint64_t>(stat_area.st_size)) {
    const size_t overlap =
        std::min<uint64_t>(chunk_payload.length(),
                           static_cast<uint64_t>(stat_area.st_size) -
                               chunk_offset);
    std::string existing;
    const Preserve_trx_transfer_status read_status =
        read_existing_overlap(path, chunk_offset, overlap, &existing);
    if (read_status != Preserve_trx_transfer_status::OK) return read_status;
    if (existing != chunk_payload.substr(0, overlap)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
  }
  const Preserve_trx_transfer_status write_status =
      write_chunk_to_file(path, chunk_offset, chunk_payload);
  if (write_status != Preserve_trx_transfer_status::OK) return write_status;
  return append_range_to_file(transfer_object_range_path(root_dir, manifest,
                                                         *object),
                              chunk_offset, chunk_payload.length());
}

Preserve_trx_transfer_status preserve_trx_transfer_seal_staged_object(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id) {
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
    return Preserve_trx_transfer_status::CORRUPT;
  }
  std::array<unsigned char, kPreservedTrxSha256Length> digest{};
  const Preserve_trx_transfer_status digest_status =
      sha256_digest_file_streaming(path, object->total_size, &digest);
  if (digest_status != Preserve_trx_transfer_status::OK) return digest_status;
  return digest == object->digest ? Preserve_trx_transfer_status::OK
                                  : Preserve_trx_transfer_status::CORRUPT;
}

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

Preserve_trx_transfer_status preserve_trx_transfer_load_standby_bundle_from_staging(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    Preserved_trx_bundle *bundle, bool objects_already_sealed = false) {
  if (bundle == nullptr) return Preserve_trx_transfer_status::INVALID_ARGUMENT;

  std::string portable_snapshot;
  Preserve_trx_transfer_status status =
      preserve_trx_transfer_read_snapshot_bundle_payload(root_dir, manifest,
                                                         &portable_snapshot,
                                                         objects_already_sealed);
  if (status != Preserve_trx_transfer_status::OK) return status;

  status = preserve_trx_transfer_decode_portable_bundle(portable_snapshot,
                                                        bundle);
  if (status != Preserve_trx_transfer_status::OK) return status;

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

  const Preserve_trx_transfer_status mark_status = registry->mark_corrupt(
      manifest.epoch_id, manifest.token,
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

  for (const Preserve_trx_transfer_manifest &manifest : manifests) {
    const Preserve_trx_transfer_status seal_status =
        preserve_trx_transfer_seal_manifest_objects(root_dir, manifest);
    if (seal_status != Preserve_trx_transfer_status::OK) return seal_status;
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

  const Preserve_trx_transfer_status fact_status = write_epoch_fact_file(
      root_dir, manifests, commit_manifest.source_epoch_commit_lsn);
  if (fact_status != Preserve_trx_transfer_status::OK) return fact_status;
  return write_commit_marker_file(root_dir, commit_manifest);
}

Preserve_trx_transfer_status commit_epoch_final_metadata(
    const std::string &root_dir,
    const std::vector<Preserve_trx_transfer_manifest> &manifests,
    const Preserve_trx_transfer_manifest &commit_manifest,
    uint64_t source_fence_lsn) {
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
      write_epoch_fact_file(root_dir, manifests, source_fence_lsn);
  if (fact_status != Preserve_trx_transfer_status::OK) return fact_status;
  return write_commit_marker_file(root_dir, commit_manifest);
}

enum class Receiver_prewarm_job_kind { STAGED_TOKEN, COMMITTED_EPOCH, OBJECT };

struct Receiver_prewarm_job {
  Receiver_prewarm_job_kind kind{Receiver_prewarm_job_kind::STAGED_TOKEN};
  std::string root_dir;
  Preserve_trx_transfer_manifest manifest;
  std::vector<Preserve_trx_transfer_manifest> manifests;
  std::string object_id;
  Preserve_trx_transfer_receiver_registry *registry{nullptr};
  bool objects_already_sealed{false};
  bool retry_stale_record_lock_proof{false};
  uint object_retry_attempts{0};
};

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
std::condition_variable g_receiver_prewarm_cv;
std::deque<Receiver_prewarm_job> g_receiver_staged_token_prewarm_jobs;
std::deque<Receiver_prewarm_job> g_receiver_prewarm_jobs;
std::vector<std::thread> g_receiver_prewarm_workers;
std::set<Receiver_staged_token_prewarm_key>
    g_receiver_staged_token_prewarm_inflight;
std::set<Receiver_staged_token_prewarm_key>
    g_receiver_staged_token_prewarm_done;
std::set<Receiver_staged_token_prewarm_key>
    g_receiver_staged_token_prewarm_deferred;
std::set<Receiver_object_prewarm_key> g_receiver_object_prewarm_inflight;
bool g_receiver_prewarm_workers_started = false;
bool g_receiver_prewarm_workers_starting = false;
bool g_receiver_prewarm_workers_stopping = false;
bool g_receiver_prewarm_shutdown = false;
size_t g_receiver_prewarm_worker_init_reports = 0;
size_t g_receiver_prewarm_worker_init_failures = 0;
std::atomic<uint> g_receiver_prewarm_worker_init_index{0};
std::atomic<int> g_receiver_prewarm_fail_create_at_for_unit_test{-1};
std::atomic<int> g_receiver_prewarm_fail_init_at_for_unit_test{-1};
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
    Receiver_prewarm_job job);

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
  return true;
}

enum class Receiver_object_prewarm_proof_state { MISSING, STALE, READY };

Receiver_object_prewarm_proof_state receiver_object_prewarm_proof_state(
    const Receiver_object_prewarm_key &key) {
  std::lock_guard<std::mutex> guard(g_receiver_object_prewarm_proof_mutex);
  const auto found = g_receiver_object_prewarm_proofs.find(key);
  if (found == g_receiver_object_prewarm_proofs.end())
    return Receiver_object_prewarm_proof_state::MISSING;
  if (!found->second.record_lock_object ||
      receiver_record_lock_proof_gate_ready(found->second)) {
    return Receiver_object_prewarm_proof_state::READY;
  }
  return Receiver_object_prewarm_proof_state::STALE;
}

bool receiver_staged_token_prewarm_job_runnable(
    const Receiver_prewarm_job &job) {
  (void)job;
  return true;
}

void finish_receiver_staged_token_prewarm_job(
    const Receiver_prewarm_job &job, bool terminal) {
  Receiver_staged_token_prewarm_key key =
      receiver_staged_token_prewarm_key(job.root_dir, job.manifest);
  bool notify_retry = false;
  {
    std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
    g_receiver_staged_token_prewarm_inflight.erase(key);
    const bool deferred =
        g_receiver_staged_token_prewarm_deferred.erase(key) != 0;
    if (terminal) {
      g_receiver_staged_token_prewarm_done.insert(std::move(key));
    } else if (deferred) {
      g_receiver_staged_token_prewarm_inflight.insert(key);
      g_receiver_prewarm_jobs.push_back(job);
      notify_retry = true;
    }
  }
  if (notify_retry) g_receiver_prewarm_cv.notify_one();
}

void finish_receiver_object_prewarm_job(
    const Receiver_object_prewarm_key &key) {
  std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
  g_receiver_object_prewarm_inflight.erase(key);
}

Preserve_trx_transfer_status finalize_receiver_ready_token_staging(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    Preserve_trx_transfer_receiver_registry *registry) {
  if (registry == nullptr) return Preserve_trx_transfer_status::OK;
  if (!preserve_trx_transfer_epoch_committed(root_dir, manifest.epoch_id)) {
    return Preserve_trx_transfer_status::OK;
  }
  if (!receiver_seal_prewarm_token_ok(manifest.epoch_id, manifest.token)) {
    return Preserve_trx_transfer_status::OK;
  }
  if (!receiver_epoch_ready_is_bound(root_dir, manifest.epoch_id)) {
    return Preserve_trx_transfer_status::OK;
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
  status = registry->mark_saved_online(manifest.epoch_id, manifest.token);
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
    return registry->mark_saved_online(manifest.epoch_id, manifest.token);
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
  retry_unbound_receiver_epochs_once();
  for (const auto &epoch : bound_receiver_epochs()) {
    const auto records =
        registry->sealed_receiving_records_for_epoch(epoch.second);
    for (const auto &record : records) {
      (void)finalize_receiver_ready_token_staging(
          epoch.first, receiver_record_manifest(record), registry);
    }
  }
  (void)registry->retry_cleanup_debt_once(now_us);
  (void)registry->retire_acknowledged_epochs_once(now_us);
}

void preserve_trx_transfer_receiver_reaper_scan_once(uint64_t now_us) {
  receiver_reaper_scan_once(now_us, &default_receiver_registry());
}

void preserve_trx_transfer_receiver_reaper_scan_for_unit_test(
    uint64_t now_us, Preserve_trx_transfer_receiver_registry *registry) {
  receiver_reaper_scan_once(now_us, registry);
}

Preserve_trx_promotion_adopt_status run_receiver_staged_token_prewarm_job(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    Preserve_trx_transfer_receiver_registry *registry,
    bool objects_already_sealed) {
  note_receiver_staged_token_job_started();
  auto finish_active = create_scope_guard(
      [] { note_receiver_staged_token_job_finished(); });
  const uint64_t staged_started_us = transfer_monotonic_us();
  Preserved_trx_bundle staged_bundle;
  const Preserve_trx_transfer_status load_status =
      preserve_trx_transfer_load_standby_bundle_from_staging(
          root_dir, manifest, &staged_bundle, objects_already_sealed);
  if (load_status != Preserve_trx_transfer_status::OK) {
    note_receiver_seal_prewarm_status(
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT);
    return Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
  }

  const uint64_t ready_started_us = transfer_monotonic_us();
  note_receiver_prewarm_start();
  Preserve_trx_promotion_adopt_status prewarm_status =
      Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY;
  const bool has_record_lock_object =
      find_object(manifest, kPreservedTrxBlobRecordLocks) != nullptr;
  if (has_record_lock_object) {
    bool stale_record_lock_proof = false;
    prewarm_status =
        prewarm_receiver_ready_cache_from_object_proof(root_dir, manifest,
                                                       staged_bundle,
                                                       &stale_record_lock_proof);
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
        manifest.source_epoch_commit_lsn, staged_bundle);
  }
  note_receiver_prewarm_end();
  g_receiver_staged_token_ready_cache_us.fetch_add(transfer_monotonic_us() -
                                                   ready_started_us);
  note_receiver_staged_token_total_us(transfer_monotonic_us() -
                                      staged_started_us);
  const uint delay_ms =
      receiver_staged_prewarm_delay_ms_for_unit_test().load();
  if (delay_ms != 0) my_sleep(delay_ms * 1000ULL);
  const bool strict_prepared =
      prepare_strict_bundle_for_receiver(root_dir, manifest, staged_bundle);
  if (strict_prepared) {
    bind_strict_prepared_token_from_cached_epoch_fact(
        root_dir, manifest.epoch_id, manifest.token);
  }
  const Preserve_trx_promotion_adopt_status authoritative_status =
      prewarm_status == Preserve_trx_promotion_adopt_status::OK &&
              strict_prepared
          ? Preserve_trx_promotion_adopt_status::OK
          : Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY;
  note_receiver_seal_prewarm_status(authoritative_status);
  note_receiver_seal_prewarm_token_status(
      manifest.epoch_id, manifest.token, authoritative_status);
  if (authoritative_status == Preserve_trx_promotion_adopt_status::OK) {
    note_receiver_epoch_token_ready(root_dir, manifest.epoch_id,
                                    manifest.token);
    (void)publish_receiver_epoch_ready_from_fact_if_possible(
        root_dir, manifest.epoch_id);
    (void)finalize_receiver_ready_token_staging(root_dir, manifest, registry);
  }
  return authoritative_status;
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
  Preserve_trx_transfer_receiver_record record;
  if (!registry->lookup(manifest.epoch_id, manifest.token, &record)) return true;
  return record.state != Preserve_trx_transfer_receiver_state::DECLARED &&
         record.state != Preserve_trx_transfer_receiver_state::RECEIVING;
}

static bool run_receiver_object_prewarm_job(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id,
    Preserve_trx_transfer_receiver_registry *registry,
    bool retry_stale_record_lock_proof, uint object_retry_attempts) {
  const uint64_t started_us = transfer_monotonic_us();
  const uint delay_ms =
      receiver_object_prewarm_delay_ms_for_unit_test().load();
  if (delay_ms != 0) my_sleep(delay_ms * 1000ULL);
  Preserve_trx_transfer_manifest effective_manifest = manifest;
  const Preserve_trx_transfer_object_descriptor *object =
      find_object(effective_manifest, object_id);
  if (object == nullptr && registry != nullptr) {
    Preserve_trx_transfer_receiver_record current_record;
    if (registry->lookup(manifest.epoch_id, manifest.token, &current_record)) {
      Preserve_trx_transfer_manifest current_manifest =
          receiver_record_manifest(current_record);
      if (find_object(current_manifest, object_id) != nullptr) {
        effective_manifest = std::move(current_manifest);
        object = find_object(effective_manifest, object_id);
      }
    }
  }
  if (object == nullptr) {
    g_receiver_object_prewarm_miss_count.fetch_add(1);
    return false;
  }
  Receiver_object_prewarm_key inflight_key;
  const bool have_inflight_key =
      receiver_object_prewarm_key(root_dir, effective_manifest, object_id,
                                  &inflight_key);
  auto finish_inflight = create_scope_guard([&] {
    if (have_inflight_key) finish_receiver_object_prewarm_job(inflight_key);
  });
  const bool record_lock_object = object_id == kPreservedTrxBlobRecordLocks;
  const bool binlog_object = object_id == kPreservedTrxBlobBinlogCache;
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
    (void)build_receiver_record_lock_prepared(root_dir, effective_manifest,
                                              payload);

    trx_preserve_record_lock_page_plan_t page_plan;
    if (!trx_preserve_record_lock_payload_page_plan(payload, &page_plan)) {
      g_receiver_object_prewarm_miss_count.fetch_add(1);
      return false;
    }

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
            if (trx_preserve_record_lock_payload_residency(payload, sample)) {
              return true;
            }
            residency_parse_failed = true;
            return false;
          },
          [] { return transfer_monotonic_us(); },
          [&] { return receiver_prewarm_job_cancelled(registry, manifest); },
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

    proof.record_lock_object = true;
    proof.page_count = page_plan.page_count;
    proof.resident_pages =
        prefetch_status == DB_SUCCESS ? residency.resident_pages : 0;
    proof.cold_gets = residency.page_count > residency.resident_pages
                          ? residency.page_count - residency.resident_pages
                          : 0;
    if (prefetch_status != DB_SUCCESS && proof.page_count != 0) {
      proof.cold_gets = proof.page_count;
    }
    proof.bitmap_pages = page_plan.bitmap_pages;
    proof.bitmap_bits = page_plan.bitmap_bits;
  }

  bool proof_published = false;
  bool retry_after_finish = false;
  {
    std::lock_guard<std::mutex> guard(g_receiver_object_prewarm_proof_mutex);
    auto existing = g_receiver_object_prewarm_proofs.find(key);
    if (existing != g_receiver_object_prewarm_proofs.end()) {
      if (record_lock_object &&
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
  note_elapsed();

  if (registry == nullptr) return retry_after_finish;

  Preserve_trx_transfer_receiver_record sealed_record;
  if (!registry->lookup(effective_manifest.epoch_id, effective_manifest.token,
                        &sealed_record)) {
    return retry_after_finish;
  }
  const Preserve_trx_transfer_manifest sealed_manifest =
      receiver_record_manifest(sealed_record);
  if (transfer_manifest_has_snapshot_bundle(sealed_manifest) &&
      registry->all_objects_sealed(manifest.epoch_id, manifest.token)) {
    (void)enqueue_receiver_staged_token_prewarm(root_dir, sealed_manifest,
                                                registry);
  }
  return retry_after_finish;
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

static void run_receiver_committed_epoch_prewarm_job(
    const std::string &root_dir,
    const std::vector<Preserve_trx_transfer_manifest> &manifests) {
  if (manifests.empty()) return;

  std::vector<uint64_t> tokens;
  tokens.reserve(manifests.size());
  uint64_t required_apply_lsn = 0;
  for (const Preserve_trx_transfer_manifest &manifest : manifests) {
    tokens.push_back(manifest.token);
    required_apply_lsn =
        std::max(required_apply_lsn, manifest.source_epoch_commit_lsn);
  }

  Preserve_trx_promotion_adopt_result prewarm_result;
  note_receiver_prewarm_start();
  const Preserve_trx_promotion_adopt_status prewarm_status =
      preserved_trx_promotion_prewarm_standby_pending_tokens(
          root_dir, manifests.front().epoch_id, tokens, required_apply_lsn,
          preserve_trx_promotion_gate_workers, &prewarm_result);
  note_receiver_prewarm_end();
  uint64_t ready_tokens = 0;
  for (const Preserve_trx_promotion_token_result &token_result :
       prewarm_result.token_results) {
    if (token_result.status == Preserve_trx_promotion_adopt_status::OK) {
      ++ready_tokens;
    }
  }
  const uint64_t total_tokens = tokens.size();
  g_receiver_auto_prewarm_tokens.fetch_add(total_tokens);
  g_receiver_auto_prewarm_ready_tokens.fetch_add(ready_tokens);
  g_receiver_auto_prewarm_not_ready_tokens.fetch_add(total_tokens - ready_tokens);
  g_receiver_prewarm_backlog_at_phase2_end.store(total_tokens - ready_tokens);
  g_receiver_auto_prewarm_last_status.store(
      static_cast<uint64_t>(prewarm_status));
  if (prewarm_status == Preserve_trx_promotion_adopt_status::OK &&
      ready_tokens == total_tokens) {
    g_receiver_ready_monotonic_us.store(transfer_monotonic_us());
    refresh_receiver_ready_after_final_metadata();
    refresh_receiver_ready_after_final_spool_ack();
  }
  if (prewarm_status != Preserve_trx_promotion_adopt_status::OK) {
    /*
      COMMIT_EPOCH has already made the standby-pending artifact durable. A
      prewarm miss is readiness information for the future promotion gate, not
      a reason to fail the receiver publish path and make source drain fail.
    */
    const std::string message =
        std::string(
            "PRESERVE: standby transfer receiver automatic prewarm status=") +
        preserve_trx_promotion_adopt_status_name(prewarm_status) +
        " epoch=" + manifests.front().epoch_id +
        " tokens=" + std::to_string(tokens.size());
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
  }
}

void receiver_prewarm_worker_main() {
  const uint init_index =
      g_receiver_prewarm_worker_init_index.fetch_add(1);
  const int fail_init_at =
      g_receiver_prewarm_fail_init_at_for_unit_test.load();
  const bool init_failed =
      fail_init_at == -2 || fail_init_at == static_cast<int>(init_index) ||
      my_thread_init();
  {
    std::unique_lock<std::mutex> guard(g_receiver_prewarm_mutex);
    g_receiver_prewarm_cv.wait(guard, [] {
      return !g_receiver_prewarm_pause_init_report_for_unit_test;
    });
    ++g_receiver_prewarm_worker_init_reports;
    if (init_failed) ++g_receiver_prewarm_worker_init_failures;
  }
  g_receiver_prewarm_cv.notify_all();
  if (init_failed) return;
  auto thread_end_guard = create_scope_guard([] { my_thread_end(); });
  for (;;) {
    Receiver_prewarm_job job;
    {
      std::unique_lock<std::mutex> guard(g_receiver_prewarm_mutex);
      for (;;) {
        if (g_receiver_prewarm_shutdown &&
            g_receiver_staged_token_prewarm_jobs.empty() &&
            g_receiver_prewarm_jobs.empty()) {
          return;
        }

        auto runnable_staged =
            std::find_if(g_receiver_staged_token_prewarm_jobs.begin(),
                         g_receiver_staged_token_prewarm_jobs.end(),
                         receiver_staged_token_prewarm_job_runnable);
        if (runnable_staged != g_receiver_staged_token_prewarm_jobs.end()) {
          job = std::move(*runnable_staged);
          g_receiver_staged_token_prewarm_jobs.erase(runnable_staged);
          break;
        }
        if (!g_receiver_prewarm_jobs.empty()) {
          job = std::move(g_receiver_prewarm_jobs.front());
          g_receiver_prewarm_jobs.pop_front();
          break;
        }
        if (g_receiver_prewarm_shutdown) {
          return;
        }
        g_receiver_prewarm_cv.wait(guard);
      }
    }

    Preserve_trx_promotion_adopt_status staged_status =
        Preserve_trx_promotion_adopt_status::OK;
    try {
      if (job.kind == Receiver_prewarm_job_kind::STAGED_TOKEN) {
        staged_status =
            run_receiver_staged_token_prewarm_job(job.root_dir, job.manifest,
                                                  job.registry,
                                                  job.objects_already_sealed);
      } else if (job.kind == Receiver_prewarm_job_kind::COMMITTED_EPOCH) {
        run_receiver_committed_epoch_prewarm_job(job.root_dir, job.manifests);
      } else {
        const bool retry_object = run_receiver_object_prewarm_job(
            job.root_dir, job.manifest, job.object_id, job.registry,
            job.retry_stale_record_lock_proof, job.object_retry_attempts);
        g_receiver_prewarm_cv.notify_all();
        if (retry_object) {
          Receiver_prewarm_job retry_job = job;
          retry_job.retry_stale_record_lock_proof = true;
          retry_job.object_retry_attempts = job.object_retry_attempts + 1;
          (void)enqueue_receiver_prewarm_job(std::move(retry_job));
        }
      }
    } catch (...) {
      if (job.kind == Receiver_prewarm_job_kind::STAGED_TOKEN) {
        staged_status =
            Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT;
        note_receiver_seal_prewarm_status(
            Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT);
      } else if (job.kind == Receiver_prewarm_job_kind::COMMITTED_EPOCH) {
        g_receiver_auto_prewarm_last_status.store(static_cast<uint64_t>(
            Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT));
      } else {
        g_receiver_object_prewarm_miss_count.fetch_add(1);
      }
    }
    if (job.kind == Receiver_prewarm_job_kind::STAGED_TOKEN) {
      finish_receiver_staged_token_prewarm_job(
          job, staged_status !=
                   Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY);
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

bool preserve_trx_transfer_receiver_workers_starting_for_unit_test() {
  std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
  return g_receiver_prewarm_workers_starting;
}

Preserve_trx_transfer_status enqueue_receiver_prewarm_job(
    Receiver_prewarm_job job) {
  if (job.root_dir.empty()) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  Receiver_object_prewarm_key object_key;
  const bool has_object_key =
      job.kind == Receiver_prewarm_job_kind::OBJECT &&
      receiver_object_prewarm_key(job.root_dir, job.manifest, job.object_id,
                                  &object_key);
  if (job.kind == Receiver_prewarm_job_kind::OBJECT && !has_object_key) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  {
    std::unique_lock<std::mutex> guard(g_receiver_prewarm_mutex);
    const Preserve_trx_transfer_status start_status =
        ensure_receiver_prewarm_workers_locked(
            &guard, preserve_trx_transfer_receiver_workers);
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
      g_receiver_staged_token_prewarm_inflight.insert(std::move(key));
      g_receiver_staged_token_prewarm_jobs.push_back(std::move(job));
      g_receiver_prewarm_cv.notify_one();
      return Preserve_trx_transfer_status::OK;
    }
    if (has_object_key) {
      if (g_receiver_object_prewarm_inflight.count(object_key) != 0) {
        return Preserve_trx_transfer_status::OK;
      }
      const Receiver_object_prewarm_proof_state proof_state =
          receiver_object_prewarm_proof_state(object_key);
      if (proof_state == Receiver_object_prewarm_proof_state::READY ||
          (proof_state == Receiver_object_prewarm_proof_state::STALE &&
           !job.retry_stale_record_lock_proof)) {
        return Preserve_trx_transfer_status::OK;
      }
      g_receiver_object_prewarm_inflight.insert(object_key);
    }
    g_receiver_prewarm_jobs.push_back(std::move(job));
  }
  g_receiver_prewarm_cv.notify_one();
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

Preserve_trx_transfer_status enqueue_receiver_object_prewarm(
    const std::string &root_dir,
    const Preserve_trx_transfer_manifest &manifest,
    const std::string &object_id,
    Preserve_trx_transfer_receiver_registry *registry,
    bool retry_stale_record_lock_proof) {
  if (object_id.empty()) return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  Receiver_prewarm_job job;
  job.kind = Receiver_prewarm_job_kind::OBJECT;
  job.root_dir = root_dir;
  job.manifest = manifest;
  job.object_id = object_id;
  job.registry = registry;
  job.retry_stale_record_lock_proof = retry_stale_record_lock_proof;
  job.object_retry_attempts = 0;
  return enqueue_receiver_prewarm_job(std::move(job));
}

Preserve_trx_transfer_status enqueue_receiver_committed_epoch_prewarm(
    const std::string &root_dir,
    const std::vector<Preserve_trx_transfer_manifest> &manifests) {
  if (manifests.empty()) return Preserve_trx_transfer_status::OK;
  g_receiver_committed_epoch_fallback_count.fetch_add(1);
  Receiver_prewarm_job job;
  job.kind = Receiver_prewarm_job_kind::COMMITTED_EPOCH;
  job.root_dir = root_dir;
  job.manifests = manifests;
  g_receiver_prewarm_backlog_at_phase2_end.store(manifests.size());
  return enqueue_receiver_prewarm_job(std::move(job));
}

void preserve_trx_transfer_shutdown_receiver_prewarm_workers() {
  std::vector<std::thread> workers;
  {
    std::unique_lock<std::mutex> guard(g_receiver_prewarm_mutex);
    g_receiver_prewarm_cv.wait(guard, [] {
      return !g_receiver_prewarm_workers_starting &&
             !g_receiver_prewarm_workers_stopping;
    });
    if (!g_receiver_prewarm_workers_started) return;
    g_receiver_prewarm_workers_stopping = true;
    g_receiver_prewarm_shutdown = true;
    workers.swap(g_receiver_prewarm_workers);
    g_receiver_prewarm_workers_started = false;
  }
  g_receiver_prewarm_cv.notify_all();
  for (std::thread &worker : workers) {
    if (worker.joinable()) worker.join();
  }
  {
    std::lock_guard<std::mutex> guard(g_receiver_prewarm_mutex);
    g_receiver_staged_token_prewarm_jobs.clear();
    g_receiver_prewarm_jobs.clear();
    g_receiver_staged_token_prewarm_inflight.clear();
    g_receiver_staged_token_prewarm_done.clear();
    g_receiver_staged_token_prewarm_deferred.clear();
    g_receiver_object_prewarm_inflight.clear();
    g_receiver_prewarm_shutdown = false;
    g_receiver_prewarm_workers_stopping = false;
  }
  g_receiver_prewarm_cv.notify_all();
  {
    std::lock_guard<std::mutex> guard(g_receiver_seal_prewarm_state_mutex);
    g_receiver_seal_prewarm_state.clear();
  }
  {
    std::lock_guard<std::mutex> guard(g_receiver_ready_epoch_mutex);
    g_receiver_ready_epoch_state.clear();
  }
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
    purge_strict_prepared_token_for_receiver(root_dir, record);
    const Preserve_trx_transfer_status cleanup_status =
        cleanup_transfer_token_staging(root_dir, record.epoch_id, record.token);
    if (cleanup_status == Preserve_trx_transfer_status::OK) continue;
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
  const std::string prefix = "PTRXFER_COMMIT\n";
  if (payload.compare(0, prefix.length(), prefix) != 0) return false;
  std::vector<std::string> fields;
  size_t offset = prefix.length();
  while (offset <= payload.length()) {
    const size_t next = payload.find('\n', offset);
    if (next == std::string::npos) return false;
    fields.push_back(payload.substr(offset, next - offset));
    offset = next + 1;
    if (offset == payload.length()) break;
  }
  if (fields.size() != 3 || fields[0] != epoch_id ||
      !transfer_component_safe(fields[1]) || !transfer_component_safe(fields[2])) {
    return false;
  }

  Preserve_trx_transfer_epoch_fact fact;
  return preserve_trx_transfer_read_epoch_fact(root_dir, epoch_id, &fact) ==
             Preserve_trx_transfer_status::OK &&
         fact.epoch_id == epoch_id && fact.source_server_uuid == fields[1] &&
         fact.target_server_uuid == fields[2] && !fact.tokens.empty();
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

Preserve_trx_transfer_status preserve_trx_transfer_apply_receiver_frame(
    const std::string &root_dir, const Preserve_trx_transfer_frame &frame,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds,
    Preserve_snapshot_metadata *written_metadata) {
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

  const bool sequence_frame = receiver_frame_should_spool(frame.type);
  bool sequence_consumed = false;
  if (sequence_frame && !g_receiver_frame_sequence_disabled) {
    status = registry->consume_frame_sequence(frame.epoch_id, frame.sequence);
    if (status != Preserve_trx_transfer_status::OK) return status;
    sequence_consumed = true;
  }

  if (!g_receiver_frame_spool_disabled && sequence_frame) {
    status = append_receiver_frame_spool_record(root_dir, frame);
    if (status != Preserve_trx_transfer_status::OK) {
      if (sequence_consumed)
        registry->rollback_frame_sequence(frame.epoch_id, frame.sequence);
      return status;
    }
    note_receiver_first_frame_durable();
  }

  if (frame.type ==
      Preserve_trx_transfer_frame_type::PROMOTION_PREWARM_TOKEN) {
    Preserve_trx_promotion_adopt_result result;
    return map_promotion_status_to_transfer(
        preserved_trx_promotion_prewarm_standby_pending_tokens(
            root_dir, frame.epoch_id, {frame.token}, frame.chunk_offset,
            preserve_trx_promotion_gate_workers, &result));
  }

  if (frame.type == Preserve_trx_transfer_frame_type::PROMOTION_GATE_EPOCH) {
    Preserve_trx_promotion_adopt_all_request request;
    request.epoch_id = frame.epoch_id;
    request.required_apply_lsn = frame.chunk_offset;
    request.require_epoch_committed = true;
    request.require_apply_barrier = true;
    request.require_promotion_ready_cache = true;
    request.execute_adopt = true;
    Preserve_trx_promotion_adopt_result result;
    return map_promotion_status_to_transfer(
        preserved_trx_adopt_standby_pending_all_for_promotion(root_dir, request,
                                                              &result));
  }

  if (frame.type == Preserve_trx_transfer_frame_type::DECLARE_TOKEN) {
    std::string source_server_uuid;
    std::string target_server_uuid;
    if (!decode_declare_token_reason(frame.reason, &source_server_uuid,
                                     &target_server_uuid)) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    Preserve_trx_transfer_manifest manifest;
    manifest.epoch_id = frame.epoch_id;
    manifest.token = frame.token;
    manifest.source_server_uuid = source_server_uuid;
    manifest.target_server_uuid = target_server_uuid;
    status =
        preserve_trx_transfer_validate_receiver_manifest_from_config(manifest);
    if (status != Preserve_trx_transfer_status::OK) return status;
    return registry->declare_token(frame.epoch_id, frame.token,
                                   source_server_uuid, target_server_uuid);
  }

  if (frame.type == Preserve_trx_transfer_frame_type::DECLARE_OBJECT) {
    Preserve_trx_transfer_object_descriptor descriptor;
    status = decode_transfer_object_descriptor(frame.manifest_payload,
                                               &descriptor);
    if (status != Preserve_trx_transfer_status::OK) return status;
    if (descriptor.object_id != frame.object_id) {
      return Preserve_trx_transfer_status::CORRUPT;
    }
    Preserve_trx_transfer_manifest existing_manifest;
    Preserve_trx_transfer_object_descriptor replaced_object;
    bool cleanup_replaced_object = false;
    Preserve_trx_transfer_receiver_record existing_record;
    if (registry->lookup(frame.epoch_id, frame.token, &existing_record) &&
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
    status = registry->declare_object(frame.epoch_id, frame.token, descriptor);
    if (status != Preserve_trx_transfer_status::OK ||
        !cleanup_replaced_object) {
      return status;
    }
    status = cleanup_transfer_object_staging(root_dir, existing_manifest,
                                             replaced_object);
    if (status == Preserve_trx_transfer_status::OK) return status;
    const Preserve_trx_transfer_status mark_status = registry->mark_corrupt(
        frame.epoch_id, frame.token,
        "replacement_object_cleanup_failed:" + transfer_status_name(status));
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
      return Preserve_trx_transfer_status::UNSUPPORTED;
    }
    Preserve_trx_transfer_manifest existing_manifest;
    std::vector<Preserve_trx_transfer_object_descriptor> objects_to_cleanup;
    Preserve_trx_transfer_receiver_record existing_record;
    if (registry->lookup(frame.epoch_id, frame.token, &existing_record) &&
        (existing_record.state == Preserve_trx_transfer_receiver_state::DECLARED ||
         existing_record.state == Preserve_trx_transfer_receiver_state::RECEIVING)) {
      try {
        existing_manifest = receiver_record_manifest(existing_record);
        objects_to_cleanup.reserve(existing_manifest.objects.size());
        for (const Preserve_trx_transfer_object_descriptor &existing_object :
             existing_manifest.objects) {
          const Preserve_trx_transfer_object_descriptor *replacement =
              find_object(manifest, existing_object.object_id);
          if (replacement != nullptr &&
              transfer_object_descriptor_equal(*replacement,
                                               existing_object)) {
            continue;
          }
          objects_to_cleanup.push_back(existing_object);
        }
      } catch (...) {
        return Preserve_trx_transfer_status::RESOURCE_EXHAUSTED;
      }
    }
    status = registry->begin_receive(manifest, frame.manifest_payload.length());
    if (status == Preserve_trx_transfer_status::OK) {
      for (const Preserve_trx_transfer_object_descriptor &existing_object :
           objects_to_cleanup) {
        const Preserve_trx_transfer_status cleanup_status =
            cleanup_transfer_object_staging(root_dir, existing_manifest,
                                            existing_object);
        if (cleanup_status != Preserve_trx_transfer_status::OK) {
          const Preserve_trx_transfer_status mark_status =
              registry->mark_corrupt(
                  frame.epoch_id, frame.token,
                  "replacement_begin_cleanup_failed:" +
                      transfer_status_name(cleanup_status));
          return mark_status == Preserve_trx_transfer_status::OK
                     ? cleanup_status
                     : mark_status;
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
    erase_receiver_strict_record_lock_state(root_dir, frame.epoch_id,
                                            frame.token);
    purge_strict_prepared_token_for_receiver(root_dir, record);
    status =
        cleanup_transfer_token_staging(root_dir, frame.epoch_id, frame.token);
    if (status != Preserve_trx_transfer_status::OK) {
      return registry->mark_cleanup_pending(
          root_dir, frame.epoch_id, frame.token, transfer_monotonic_us(),
          Preserve_trx_transfer_receiver_state::ABORTED,
          "abort_cleanup_failed:" + transfer_status_name(status));
    }
    return registry->mark_aborted(frame.epoch_id, frame.token, frame.reason);
  }

  if (frame.type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH &&
      !registry->all_receiving_tokens_sealed(frame.epoch_id)) {
    const std::vector<Preserve_trx_transfer_receiver_record> records =
        registry->receiving_records_for_epoch(frame.epoch_id);
    (void)cleanup_epoch_transfer_staging(root_dir, records, registry);
    const Preserve_trx_transfer_status mark_status =
        mark_epoch_records_corrupt(registry, records,
                                   "commit_epoch_unsealed_receiving_token");
    return mark_status == Preserve_trx_transfer_status::OK
               ? Preserve_trx_transfer_status::CORRUPT
               : mark_status;
  }

  Preserve_trx_transfer_receiver_record record;
  if (!registry->lookup(frame.epoch_id, frame.token, &record)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const bool object_frame =
      frame.type == Preserve_trx_transfer_frame_type::OBJECT_CHUNK ||
      frame.type == Preserve_trx_transfer_frame_type::SEAL_OBJECT;
  if (record.state != Preserve_trx_transfer_receiver_state::RECEIVING &&
      !(object_frame &&
        record.state == Preserve_trx_transfer_receiver_state::DECLARED)) {
    return Preserve_trx_transfer_status::UNSUPPORTED;
  }
  const Preserve_trx_transfer_manifest manifest =
      receiver_record_manifest(record);

  switch (frame.type) {
    case Preserve_trx_transfer_frame_type::OBJECT_CHUNK:
      status = preserve_trx_transfer_stage_object_chunk(
          root_dir, manifest, frame.object_id, frame.chunk_offset,
          frame.chunk_payload);
      break;
    case Preserve_trx_transfer_frame_type::SEAL_OBJECT:
      status = preserve_trx_transfer_seal_staged_object(root_dir, manifest,
                                                        frame.object_id);
      if (status == Preserve_trx_transfer_status::OK) {
        g_receiver_last_object_seal_monotonic_us.store(transfer_monotonic_us());
        status = registry->mark_object_sealed(frame.epoch_id, frame.token,
                                             frame.object_id);
        if (status == Preserve_trx_transfer_status::OK) {
          Preserve_trx_transfer_receiver_record sealed_record;
          if (registry->lookup(frame.epoch_id, frame.token, &sealed_record)) {
            const Preserve_trx_transfer_manifest sealed_manifest =
                receiver_record_manifest(sealed_record);
            status = enqueue_receiver_object_prewarm(
                root_dir, sealed_manifest, frame.object_id, registry);
            if (status == Preserve_trx_transfer_status::OK &&
                transfer_manifest_has_snapshot_bundle(sealed_manifest) &&
                registry->all_objects_sealed(frame.epoch_id, frame.token)) {
              status = enqueue_receiver_staged_token_prewarm(root_dir,
                                                             sealed_manifest,
                                                             registry);
            }
          }
        }
      }
      break;
    case Preserve_trx_transfer_frame_type::COMMIT_EPOCH:
      if (!registry->all_receiving_tokens_sealed(frame.epoch_id)) {
        const std::vector<Preserve_trx_transfer_receiver_record> records =
            registry->receiving_records_for_epoch(frame.epoch_id);
        (void)cleanup_epoch_transfer_staging(root_dir, records, registry);
        const Preserve_trx_transfer_status mark_status =
            mark_epoch_records_corrupt(
                registry, records,
                "commit_epoch_unsealed_receiving_token");
        return mark_status == Preserve_trx_transfer_status::OK
                   ? Preserve_trx_transfer_status::CORRUPT
                   : mark_status;
      }
      {
        const std::vector<Preserve_trx_transfer_receiver_record> records =
            registry->sealed_receiving_records_for_epoch(frame.epoch_id);
        if (records.empty()) {
          status = Preserve_trx_transfer_status::CORRUPT;
          break;
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
        if (status == Preserve_trx_transfer_status::OK) {
          status = commit_epoch_final_metadata(
              root_dir, epoch_manifests, manifest,
              frame.chunk_offset);
          if (status != Preserve_trx_transfer_status::OK) {
            const std::string message =
                "PRESERVE: standby transfer receiver commit epoch finalize failed status=" +
                transfer_status_name(status) + " epoch=" + frame.epoch_id +
                " token=" + std::to_string(frame.token);
            LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
            for (uint64_t published_token : published_tokens) {
              (void)store->remove_with_status(
                  transfer_token_component(published_token));
            }
            (void)cleanup_epoch_transfer_staging(root_dir, records, registry);
            const Preserve_trx_transfer_status mark_status =
                mark_epoch_records_corrupt(
                    registry, records,
                    "commit_epoch_finalize:" + transfer_status_name(status));
            return mark_status == Preserve_trx_transfer_status::OK
                       ? status
                       : mark_status;
          }
          g_transfer_phase2_final_metadata_fsync_count.fetch_add(1);
          g_receiver_final_metadata_durable_us.store(transfer_monotonic_us());
          bind_strict_prepared_tokens_from_committed_epoch(root_dir,
                                                           frame.epoch_id);
          /*
            SEAL_OBJECT already starts per-token prewarm as soon as each token's
            durable objects arrive. When every token reached that state before
            COMMIT_EPOCH, the final metadata frame only needs to bind the epoch
            fact to the existing ready cache. Falling back to a committed-epoch
            prewarm job would redo the full token scan after source phase 2 and
            turn receiver readiness into a gate tail.
          */
          if (!publish_receiver_epoch_ready_from_seal_prewarm(root_dir,
                  epoch_manifests)) {
            note_receiver_epoch_pending_without_cold_fallback(
                epoch_manifests,
                Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY);
            for (const Preserve_trx_transfer_manifest &epoch_manifest :
                 epoch_manifests) {
              if (receiver_seal_prewarm_token_ok(epoch_manifest.epoch_id,
                                                 epoch_manifest.token)) {
                continue;
              }
              const Preserve_trx_transfer_status enqueue_status =
                  enqueue_receiver_staged_token_prewarm(root_dir,
                                                        epoch_manifest,
                                                        registry);
              if (enqueue_status != Preserve_trx_transfer_status::OK) {
                return enqueue_status;
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
        const std::vector<Preserve_trx_transfer_receiver_record> records =
            registry->sealed_receiving_records_for_epoch(frame.epoch_id);
        for (const Preserve_trx_transfer_receiver_record &epoch_record :
             records) {
          const Preserve_trx_transfer_manifest epoch_manifest =
              receiver_record_manifest(epoch_record);
          if (!receiver_seal_prewarm_token_ok(epoch_manifest.epoch_id,
                                              epoch_manifest.token) ||
              !receiver_strict_token_ready(root_dir, epoch_manifest)) {
            continue;
          }
          status = finalize_receiver_ready_token_staging(root_dir,
                                                         epoch_manifest,
                                                         registry);
          if (status != Preserve_trx_transfer_status::OK) return status;
        }
        return Preserve_trx_transfer_status::OK;
      }
      break;
    case Preserve_trx_transfer_frame_type::BEGIN:
    case Preserve_trx_transfer_frame_type::ABORT:
    case Preserve_trx_transfer_frame_type::PROMOTION_PREWARM_TOKEN:
    case Preserve_trx_transfer_frame_type::PROMOTION_GATE_EPOCH:
    case Preserve_trx_transfer_frame_type::DECLARE_TOKEN:
    case Preserve_trx_transfer_frame_type::DECLARE_OBJECT:
      return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  if (status != Preserve_trx_transfer_status::OK) {
    erase_receiver_strict_record_lock_state(root_dir, frame.epoch_id,
                                            frame.token);
    purge_strict_prepared_token_for_receiver(root_dir, record);
    const Preserve_trx_transfer_status cleanup_status =
        cleanup_transfer_token_staging(root_dir, frame.epoch_id, frame.token);
    const bool cleanup_failed =
        cleanup_status != Preserve_trx_transfer_status::OK;
    std::string reason =
        "apply_receiver_frame:" + transfer_status_name(status);
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
    const Preserve_trx_transfer_status mark_status = registry->mark_corrupt(
        frame.epoch_id, frame.token, reason);
    if (mark_status != Preserve_trx_transfer_status::OK) return mark_status;
    return cleanup_failed ? cleanup_status : status;
  }
  return Preserve_trx_transfer_status::OK;
}

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
        preserve_trx_transfer_receiver_workers, written_metadata);
  }

  Preserve_trx_transfer_frame frame;
  const Preserve_trx_transfer_status status =
      preserve_trx_transfer_decode_frame(encoded_frame, &frame);
  if (status != Preserve_trx_transfer_status::OK) return status;
  return preserve_trx_transfer_apply_receiver_frame(
      root_dir, frame, store, registry, timeout_seconds, written_metadata);
}

Preserve_trx_transfer_status preserve_trx_transfer_replay_receiver_spool(
    const std::string &root_dir, const std::string &epoch_id,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds) {
  if (root_dir.empty() || store == nullptr || registry == nullptr ||
      !transfer_component_safe(epoch_id)) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  std::vector<Preserve_trx_transfer_frame> frames;
  Preserve_trx_transfer_status status =
      read_receiver_spooled_frames(root_dir, epoch_id, &frames);
  if (status != Preserve_trx_transfer_status::OK) return status;

  std::map<uint64_t, Preserve_trx_transfer_manifest> replay_manifests_by_token;
  bool saw_commit_epoch = false;
  for (const Preserve_trx_transfer_frame &frame : frames) {
    if (frame.type == Preserve_trx_transfer_frame_type::BEGIN) {
      Preserve_trx_transfer_manifest manifest;
      status = preserve_trx_transfer_decode_manifest(frame.manifest_payload,
                                                     &manifest);
      if (status != Preserve_trx_transfer_status::OK) return status;
      if (manifest.epoch_id != epoch_id || manifest.token != frame.token) {
        return Preserve_trx_transfer_status::CORRUPT;
      }
      if (transfer_manifest_has_snapshot_bundle(manifest)) {
        replay_manifests_by_token[manifest.token] = std::move(manifest);
      }
    } else if (frame.type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH) {
      saw_commit_epoch = true;
    }
  }

  Receiver_frame_spool_disable_guard disable_spool;
  for (const Preserve_trx_transfer_frame &frame : frames) {
    status = preserve_trx_transfer_apply_receiver_frame(
        root_dir, frame, store, registry, timeout_seconds, nullptr);
    if (status != Preserve_trx_transfer_status::OK) return status;
  }
  if (saw_commit_epoch && !replay_manifests_by_token.empty()) {
    std::vector<Preserve_trx_transfer_manifest> replay_manifests;
    replay_manifests.reserve(replay_manifests_by_token.size());
    for (auto &entry : replay_manifests_by_token) {
      replay_manifests.push_back(std::move(entry.second));
    }
    /*
      Replay is a recovery path, not the live drain gate.  Rebuild the
      promotion-ready cache from durable standby-pending artifacts even when a
      previous in-memory epoch-ready marker survived the test process.  The live
      COMMIT_EPOCH path still avoids this cold fallback so source phase 2 only
      waits for durable final metadata.
    */
    run_receiver_committed_epoch_prewarm_job(root_dir, replay_manifests);
  }
  return Preserve_trx_transfer_status::OK;
}

struct Receiver_payload_batch_apply_context {
  const std::string *root_dir{nullptr};
  Preserved_trx_store *store{nullptr};
  Preserve_trx_transfer_receiver_registry *registry{nullptr};
  Preserve_snapshot_metadata *written_metadata{nullptr};
  uint64_t timeout_seconds{0};
  bool sequence_pre_admitted{false};
  bool spool_pre_appended{false};
};

static Preserve_trx_transfer_status apply_receiver_payload_batch_frame(
    const Preserve_trx_transfer_frame &frame, void *context) {
  auto *batch_context =
      static_cast<Receiver_payload_batch_apply_context *>(context);
  if (batch_context == nullptr || batch_context->root_dir == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  const bool sequence_frame = receiver_frame_should_spool(frame.type);
  if (sequence_frame && batch_context->registry != nullptr &&
      batch_context->registry->frame_sequence_applied(frame.epoch_id,
                                                      frame.sequence)) {
    return Preserve_trx_transfer_status::OK;
  }
  std::unique_ptr<Receiver_frame_sequence_disable_guard> sequence_guard;
  std::unique_ptr<Receiver_frame_spool_disable_guard> spool_guard;
  if (batch_context->sequence_pre_admitted) {
    sequence_guard.reset(new Receiver_frame_sequence_disable_guard());
  }
  if (batch_context->spool_pre_appended) {
    spool_guard.reset(new Receiver_frame_spool_disable_guard());
  }
  const Preserve_trx_transfer_status status =
      preserve_trx_transfer_apply_receiver_frame(
      *batch_context->root_dir, frame, batch_context->store,
      batch_context->registry, batch_context->timeout_seconds,
      batch_context->written_metadata);
  if (status == Preserve_trx_transfer_status::OK && sequence_frame &&
      batch_context->registry != nullptr) {
    batch_context->registry->mark_frame_sequence_applied(frame.epoch_id,
                                                         frame.sequence);
  }
  return status;
}

Preserve_trx_transfer_status pre_admit_receiver_batch_sequence(
    const std::string &root_dir,
    const std::vector<Preserve_trx_transfer_frame> &frames,
    Preserve_trx_transfer_receiver_registry *registry) {
  if (root_dir.empty() || registry == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  for (const Preserve_trx_transfer_frame &frame : frames) {
    if (!receiver_frame_should_spool(frame.type)) continue;

    std::string encoded_frame;
    Preserve_trx_transfer_status status =
        preserve_trx_transfer_encode_frame(frame, &encoded_frame);
    if (status != Preserve_trx_transfer_status::OK) return status;
    Preserve_trx_transfer_sequence_admission admission =
        Preserve_trx_transfer_sequence_admission::NEW_FRAME;
    status = registry->admit_frame_sequence(
        frame.epoch_id, frame.sequence, sha256_digest(encoded_frame),
        &admission);
    if (status != Preserve_trx_transfer_status::OK) return status;

    if (admission == Preserve_trx_transfer_sequence_admission::NEW_FRAME) {
      status = append_receiver_frame_spool_record(root_dir, frame);
      if (status != Preserve_trx_transfer_status::OK) {
        if (status == Preserve_trx_transfer_status::CORRUPT) {
          registry->mark_frame_sequence_corrupt(frame.epoch_id,
                                                frame.sequence);
        } else {
          registry->rollback_frame_sequence(frame.epoch_id, frame.sequence);
        }
        return status;
      }
      note_receiver_first_frame_durable();
    }
  }
  return Preserve_trx_transfer_status::OK;
}

namespace {

using Transfer_receiver_batch_key = std::pair<std::string, uint64_t>;

Preserve_trx_transfer_status apply_receiver_frame_segment_with_workers(
    const std::vector<Preserve_trx_transfer_frame> &frames, uint worker_count,
    Preserve_trx_transfer_frame_apply_callback apply_frame, void *context) {
  if (frames.empty()) return Preserve_trx_transfer_status::OK;
  if (worker_count <= 1) {
    for (const Preserve_trx_transfer_frame &frame : frames) {
      try {
        const Preserve_trx_transfer_status status = apply_frame(frame, context);
        if (status != Preserve_trx_transfer_status::OK) return status;
      } catch (...) {
        return Preserve_trx_transfer_status::UNSUPPORTED;
      }
    }
    return Preserve_trx_transfer_status::OK;
  }

  std::map<Transfer_receiver_batch_key,
           std::vector<Preserve_trx_transfer_frame>>
      frames_by_token;
  std::vector<Transfer_receiver_batch_key> token_order;
  for (const Preserve_trx_transfer_frame &frame : frames) {
    Transfer_receiver_batch_key key(frame.epoch_id, frame.token);
    auto it = frames_by_token.find(key);
    if (it == frames_by_token.end()) {
      token_order.push_back(key);
      it = frames_by_token.emplace(key, std::vector<Preserve_trx_transfer_frame>())
               .first;
    }
    it->second.push_back(frame);
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
      for (const Preserve_trx_transfer_frame &frame : group_it->second) {
        try {
          const Preserve_trx_transfer_status status =
              apply_frame(frame, context);
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

Preserve_trx_transfer_status preserve_trx_transfer_handle_receiver_payload_batch(
    const std::string &root_dir, const std::vector<std::string> &encoded_frames,
    Preserved_trx_store *store, Preserve_trx_transfer_receiver_registry *registry,
    uint64_t timeout_seconds, uint worker_count,
    Preserve_snapshot_metadata *written_metadata,
    Preserve_trx_transfer_after_spool_callback after_spool,
    void *after_spool_context) {
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
  std::stable_sort(frames.begin(), frames.end(),
                   [](const Preserve_trx_transfer_frame &lhs,
                      const Preserve_trx_transfer_frame &rhs) {
                     if (lhs.epoch_id != rhs.epoch_id) {
                       return lhs.epoch_id < rhs.epoch_id;
                     }
                     return lhs.sequence < rhs.sequence;
                   });

  Preserve_trx_transfer_status status =
      pre_admit_receiver_batch_sequence(root_dir, frames, registry);
  if (status != Preserve_trx_transfer_status::OK) return status;
  bool contains_commit_epoch = false;
  for (const Preserve_trx_transfer_frame &frame : frames) {
    if (frame.type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH) {
      contains_commit_epoch = true;
      break;
    }
  }
  Receiver_payload_batch_apply_context context;
  context.root_dir = &root_dir;
  context.store = store;
  context.registry = registry;
  context.written_metadata = written_metadata;
  context.timeout_seconds = timeout_seconds;
  context.sequence_pre_admitted = true;
  context.spool_pre_appended = true;
  status = preserve_trx_transfer_apply_receiver_frame_batch_with_workers(
      frames, written_metadata == nullptr ? worker_count : 1,
      apply_receiver_payload_batch_frame, &context);
  if (status != Preserve_trx_transfer_status::OK) return status;
  if (after_spool != nullptr) {
    status = after_spool(after_spool_context, contains_commit_epoch);
  }
  return status;
}

Preserve_trx_transfer_status
preserve_trx_transfer_apply_receiver_frame_batch_with_workers(
    const std::vector<Preserve_trx_transfer_frame> &frames, uint worker_count,
    Preserve_trx_transfer_frame_apply_callback apply_frame, void *context) {
  if (apply_frame == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  if (frames.empty()) return Preserve_trx_transfer_status::OK;

  std::vector<Preserve_trx_transfer_frame> pending_segment;
  pending_segment.reserve(frames.size());

  auto flush_segment = [&]() {
    const Preserve_trx_transfer_status status =
        apply_receiver_frame_segment_with_workers(pending_segment, worker_count,
                                                  apply_frame, context);
    pending_segment.clear();
    return status;
  };

  for (const Preserve_trx_transfer_frame &frame : frames) {
    const bool is_batch_barrier =
        frame.type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH ||
        frame.type == Preserve_trx_transfer_frame_type::PROMOTION_GATE_EPOCH;
    if (!is_batch_barrier) {
      pending_segment.push_back(frame);
      continue;
    }

    Preserve_trx_transfer_status status = flush_segment();
    if (status != Preserve_trx_transfer_status::OK) return status;
    try {
      status = apply_frame(frame, context);
    } catch (...) {
      status = Preserve_trx_transfer_status::UNSUPPORTED;
    }
    if (status != Preserve_trx_transfer_status::OK) return status;
  }

  return flush_segment();
}

struct Receiver_durable_ack_context {
  THD *thd{nullptr};
  const std::string *encoded_payload{nullptr};
  std::string root_dir;
  bool ack_sent{false};
};

bool source_incarnation_from_epoch(const std::string &epoch_id,
                                   std::string *incarnation) {
  if (incarnation == nullptr || epoch_id.length() <= 33 || epoch_id[32] != '-') {
    return false;
  }
  const std::string candidate = epoch_id.substr(0, 32);
  for (char value : candidate) {
    if (!((value >= '0' && value <= '9') ||
          (value >= 'a' && value <= 'f'))) {
      return false;
    }
  }
  *incarnation = candidate;
  return true;
}

Preserve_trx_transfer_status
preserve_trx_transfer_validate_online_payload_identity(
    const std::string &encoded_payload, std::string *source_incarnation_id,
    std::string *epoch_id, uint64_t *last_sequence) {
  if (source_incarnation_id == nullptr || epoch_id == nullptr ||
      last_sequence == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::string parsed_epoch;
  uint64_t parsed_sequence = 0;
  Preserve_trx_transfer_status status = transfer_payload_identity(
      encoded_payload, &parsed_epoch, &parsed_sequence);
  if (status != Preserve_trx_transfer_status::OK) return status;
  std::string parsed_incarnation;
  if (!source_incarnation_from_epoch(parsed_epoch, &parsed_incarnation)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  *source_incarnation_id = std::move(parsed_incarnation);
  *epoch_id = std::move(parsed_epoch);
  *last_sequence = parsed_sequence;
  return Preserve_trx_transfer_status::OK;
}

static Preserve_trx_transfer_status send_receiver_durable_spool_ack(
    void *context, bool contains_commit_epoch) {
  auto *ack_context = static_cast<Receiver_durable_ack_context *>(context);
  if (ack_context == nullptr || ack_context->thd == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }

  if (ack_context->thd->get_protocol_classic() == nullptr) {
    return Preserve_trx_transfer_status::IO_ERROR;
  }

  if (ack_context->encoded_payload == nullptr) {
    return Preserve_trx_transfer_status::INVALID_ARGUMENT;
  }
  std::string epoch_id;
  uint64_t sequence = 0;
  Preserve_trx_transfer_status status = transfer_payload_identity(
      *ack_context->encoded_payload, &epoch_id, &sequence);
  if (status != Preserve_trx_transfer_status::OK) return status;
  std::string source_incarnation_id;
  if (!source_incarnation_from_epoch(epoch_id, &source_incarnation_id)) {
    return Preserve_trx_transfer_status::CORRUPT;
  }
  Preserve_trx_transfer_frame_ack ack;
  status = preserve_trx_transfer_build_frame_ack(
      source_incarnation_id, *ack_context->encoded_payload,
      Preserve_trx_transfer_status::OK, &ack);
  if (status != Preserve_trx_transfer_status::OK) return status;
  std::string encoded_ack;
  status = preserve_trx_transfer_encode_frame_ack(ack, &encoded_ack);
  if (status != Preserve_trx_transfer_status::OK) return status;

  /* The semantic apply succeeded; publish the single authenticated response. */
  my_ok(ack_context->thd, 0, 0, encoded_ack.c_str());
  ack_context->thd->send_statement_status();
  ack_context->thd->get_stmt_da()->reset_diagnostics_area();
  ack_context->thd->get_stmt_da()->disable_status();
  ack_context->ack_sent = true;
  if (contains_commit_epoch) {
    const uint64_t ack_us = transfer_monotonic_us();
    g_receiver_final_spool_ack_monotonic_us.store(ack_us);
    refresh_receiver_ready_after_final_spool_ack();
    static constexpr uint64_t kReceiverRetryHistoryGraceUs = 60000000;
    const Preserve_trx_transfer_status cleanup_status =
        default_receiver_registry().acknowledge_epoch(
            ack_context->root_dir, epoch_id, ack_us,
            kReceiverRetryHistoryGraceUs);
    if (cleanup_status != Preserve_trx_transfer_status::OK) {
      const std::string message =
          "PRESERVE: receiver acknowledged epoch but frame spool cleanup "
          "requires retry epoch=" +
          epoch_id + " status=" + transfer_status_name(cleanup_status);
      LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    }
  }
  return Preserve_trx_transfer_status::OK;
}

void preserve_trx_transfer_dispatch_command(THD *thd) {
  /*
    The classic command is intentionally invisible unless Preserve/Resume and
    the receiver endpoint are enabled at startup. The source-side transfer
    switch controls artifact generation and sending on a primary; a standby may
    run as a receiver-only endpoint.
  */
  if (thd == nullptr || !preserve_trx_is_enabled() ||
      !preserve_trx_transfer_receiver_enable) {
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

  bool validate_online_identity =
      transfer_frame_batch_magic_matches(encoded_frame);
  if (!validate_online_identity) {
    Preserve_trx_transfer_frame identity_frame;
    const Preserve_trx_transfer_status identity_decode_status =
        preserve_trx_transfer_decode_frame(encoded_frame, &identity_frame);
    validate_online_identity =
        identity_decode_status == Preserve_trx_transfer_status::OK &&
        receiver_frame_should_spool(identity_frame.type);
  }
  if (validate_online_identity) {
    std::string source_incarnation_id;
    std::string epoch_id;
    uint64_t last_sequence = 0;
    const Preserve_trx_transfer_status identity_status =
        preserve_trx_transfer_validate_online_payload_identity(
            encoded_frame, &source_incarnation_id, &epoch_id,
            &last_sequence);
    if (identity_status != Preserve_trx_transfer_status::OK) {
      signal_transfer_dispatch_error(thd, identity_status);
      return;
    }
  }

  const std::string preserve_dir = transfer_default_preserve_dir();
  auto store = create_preserved_trx_default_store(preserve_dir);
  Receiver_durable_ack_context ack_context;
  ack_context.thd = thd;
  ack_context.encoded_payload = &encoded_frame;
  ack_context.root_dir = preserve_dir;
  Preserve_trx_transfer_status status = Preserve_trx_transfer_status::OK;
  if (transfer_frame_batch_magic_matches(encoded_frame)) {
    status = preserve_trx_transfer_handle_receiver_payload_batch(
        preserve_dir, std::vector<std::string>{encoded_frame}, &store.store(),
        &default_receiver_registry(), transfer_commit_timeout_seconds(),
        preserve_trx_transfer_receiver_workers, nullptr,
        send_receiver_durable_spool_ack, &ack_context);
  } else {
    Preserve_trx_transfer_frame decoded_frame;
    const Preserve_trx_transfer_status decode_status =
        preserve_trx_transfer_decode_frame(encoded_frame, &decoded_frame);
    if (decode_status == Preserve_trx_transfer_status::OK &&
        receiver_frame_should_spool(decoded_frame.type)) {
      status = preserve_trx_transfer_handle_receiver_payload_batch(
          preserve_dir, std::vector<std::string>{encoded_frame}, &store.store(),
          &default_receiver_registry(), transfer_commit_timeout_seconds(),
          preserve_trx_transfer_receiver_workers, nullptr,
          send_receiver_durable_spool_ack, &ack_context);
    } else {
      status = decode_status;
      if (status == Preserve_trx_transfer_status::OK) {
        status = preserve_trx_transfer_apply_receiver_frame(
            preserve_dir, decoded_frame, &store.store(),
            &default_receiver_registry(), transfer_commit_timeout_seconds(),
            nullptr);
      }
    }
  }
  if (status != Preserve_trx_transfer_status::OK) {
    Preserve_trx_transfer_frame decoded_frame;
    const Preserve_trx_transfer_status decode_status =
        preserve_trx_transfer_decode_frame(encoded_frame, &decoded_frame);
    const std::string frame_type =
        decode_status == Preserve_trx_transfer_status::OK
            ? std::to_string(static_cast<int>(decoded_frame.type))
            : "decode_failed";
    const std::string message =
        "PRESERVE: standby transfer receiver frame failed status=" +
        transfer_status_name(status) + " frame_type=" + frame_type +
        " payload_bytes=" + std::to_string(raw_packet_length);
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
          m_epoch_id, m_source_server_uuid, m_target_server_uuid, bundle,
          m_transfer_token, m_chunk_bytes, m_frame_sink, nullptr);
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
  for (const Preserved_trx_external_blob &blob : bundle.external_blobs) {
    Preserve_trx_transfer_object_descriptor descriptor;
    descriptor.object_id = blob.name;
    descriptor.kind = Preserve_trx_transfer_object_kind::EXTERNAL_BLOB;
    if (blob.prebuilt) {
      descriptor.total_size = blob.descriptor.size;
      descriptor.digest = blob.descriptor.digest;
    } else {
      descriptor.total_size = blob.payload.length();
      descriptor.digest = sha256_digest(blob.payload);
    }
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
          m_session->epoch_id(), m_session->source_server_uuid(),
          m_session->target_server_uuid(), bundle, m_transfer_token, &manifest,
          &objects, &presealed_external_objects);
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
    const std::string &epoch_id, const std::string &source_server_uuid,
    const std::string &target_server_uuid, uint64_t transfer_token,
    uint32_t chunk_bytes, Preserve_trx_transfer_encoded_frame_sink *frame_sink,
    std::unique_ptr<Preserve_trx_artifact_sink> *sink,
    Preserve_trx_transfer_source_epoch_session *source_epoch_session,
    const std::string &preserve_dir) {
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
            source_epoch_session, transfer_token, preserve_dir, true));
        return Preserve_snapshot_status::OK;
      }
      if (frame_sink == nullptr || transfer_token == 0 || epoch_id.empty() ||
          source_server_uuid.empty() || target_server_uuid.empty() ||
          chunk_bytes == 0) {
        return Preserve_snapshot_status::INVALID_ARGUMENT;
      }
      sink->reset(new Preserve_trx_transfer_artifact_sink(
          epoch_id, source_server_uuid, target_server_uuid, transfer_token,
          chunk_bytes, frame_sink));
      return Preserve_snapshot_status::OK;
    case Preserve_trx_transfer_artifact_decision::UNSUPPORTED:
      return Preserve_snapshot_status::UNSUPPORTED;
  }
  return Preserve_snapshot_status::UNSUPPORTED;
}
