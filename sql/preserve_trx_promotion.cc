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

#include "sql/preserve_trx_promotion.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "my_dir.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_systime.h"
#include "my_sys.h"
#include "mysqld_error.h"
#include "mysql/components/services/log_builtins.h"
#include "sha2.h"
#include "sql/mysqld.h"  // server_uuid
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_carrier.h"
#include "sql/preserve_trx_transfer.h"
#include "sql/preserve_trx_xid.h"
#include "sql/rpl_gtid.h"
#include "scope_guard.h"
#include "storage/innobase/include/trx0preserve.h"

uint preserve_trx_promotion_gate_batch_tokens = 3;
uint preserve_trx_promotion_gate_workers = 3;
uint preserve_trx_promotion_gate_timeout_ms = 1000;
uint preserve_trx_promotion_prewarm_workers = 1;
ulonglong preserve_trx_promotion_prewarm_io_bytes_per_sec = 33554432ULL;
ulonglong preserve_trx_promotion_prewarm_max_bytes = 268435456ULL;
ulonglong preserve_trx_promotion_ready_cache_max_bytes = 536870912ULL;

Preserve_trx_promotion_adopt_all_request::
    Preserve_trx_promotion_adopt_all_request()
    : gate_batch_tokens(preserve_trx_promotion_gate_batch_tokens),
      worker_count(preserve_trx_promotion_gate_workers),
      gate_timeout_ms(preserve_trx_promotion_gate_timeout_ms) {}

namespace {

constexpr char kPromotionAdoptedMarkerMagic[] =
    "PTRX_PROMOTION_ADOPTED_EPOCH_V1";
constexpr char kPromotionAbandonedMarkerMagic[] =
    "PTRX_PROMOTION_ABANDONED_EPOCH_V1";
constexpr char kPromotionIntentMarkerMagic[] =
    "PTRX_PROMOTION_INTENT_EPOCH_V1";
constexpr size_t kPromotionDigestBytes = 32;
constexpr uint64_t kRecordLockResidencyPollIntervalUs = 1000;
constexpr uint32_t kPromotionGateWorkerLimit = 8;
constexpr size_t kSub500SimulatorTokenLimit = 3;

Preserve_trx_promotion_apply_state_provider g_apply_state_provider = nullptr;
Preserve_trx_promotion_adopt_executor g_adopt_executor = nullptr;
std::atomic<uint64_t> g_promotion_gate_elapsed_us{0};
std::atomic<uint64_t> g_promotion_gate_token_count{0};
std::atomic<uint64_t> g_promotion_gate_adopted_count{0};
std::atomic<uint64_t> g_promotion_gate_abandoned_count{0};
std::atomic<uint64_t> g_promotion_gate_skipped_count{0};
std::atomic<uint64_t> g_promotion_gate_max_worker_elapsed_us{0};
std::atomic<uint64_t> g_promotion_gate_p50_worker_elapsed_us{0};
std::atomic<uint64_t> g_promotion_gate_p95_worker_elapsed_us{0};
std::atomic<uint64_t> g_promotion_gate_status_code{0};
std::atomic<uint64_t> g_promotion_gate_record_lock_page_count{0};
std::mutex g_promotion_gate_worker_mutex;
std::condition_variable g_promotion_gate_worker_cv;
std::deque<std::function<void()>> g_promotion_gate_worker_jobs;
std::vector<std::thread> g_promotion_gate_worker_threads;
bool g_promotion_gate_workers_starting{false};
bool g_promotion_gate_workers_stopping{false};
bool g_promotion_gate_workers_shutdown{false};
std::atomic<uint64_t> g_promotion_gate_worker_count{0};
std::atomic<uint64_t> g_promotion_gate_worker_active_count{0};
std::atomic<bool> g_physical_promotion_bootstrap_active{false};

uint64_t promotion_monotonic_us() {
  using clock = std::chrono::steady_clock;
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          clock::now().time_since_epoch())
          .count());
}

uint64_t promotion_wall_deadline_to_monotonic(uint64_t deadline_wall_us) {
  const uint64_t now_wall_us = my_micro_time();
  if (deadline_wall_us <= now_wall_us) return 0;
  const uint64_t now_monotonic_us = promotion_monotonic_us();
  const uint64_t remaining_us = deadline_wall_us - now_wall_us;
  return remaining_us >
                 std::numeric_limits<uint64_t>::max() - now_monotonic_us
             ? std::numeric_limits<uint64_t>::max()
             : now_monotonic_us + remaining_us;
}

std::string promotion_normalize_dir(std::string dir) {
  if (!dir.empty() && dir.back() != FN_LIBCHAR) dir.push_back(FN_LIBCHAR);
  return dir;
}

void promotion_gate_worker_main() {
  for (;;) {
    std::function<void()> job;
    {
      std::unique_lock<std::mutex> guard(g_promotion_gate_worker_mutex);
      g_promotion_gate_worker_cv.wait(guard, [] {
        return g_promotion_gate_workers_shutdown ||
               !g_promotion_gate_worker_jobs.empty();
      });
      if (g_promotion_gate_workers_shutdown &&
          g_promotion_gate_worker_jobs.empty()) {
        return;
      }
      job = std::move(g_promotion_gate_worker_jobs.front());
      g_promotion_gate_worker_jobs.pop_front();
    }
    g_promotion_gate_worker_active_count.fetch_add(1,
                                                   std::memory_order_acq_rel);
    job();
    g_promotion_gate_worker_active_count.fetch_sub(1,
                                                   std::memory_order_acq_rel);
  }
}

bool ensure_promotion_gate_workers(uint32_t requested) {
  if (requested == 0 || requested > kPromotionGateWorkerLimit) return false;
  std::unique_lock<std::mutex> guard(g_promotion_gate_worker_mutex);
  g_promotion_gate_worker_cv.wait(guard, [] {
    return !g_promotion_gate_workers_starting &&
           !g_promotion_gate_workers_stopping;
  });
  if (!g_promotion_gate_workers_shutdown &&
      g_promotion_gate_worker_threads.size() >= requested) {
    return true;
  }
  if (g_promotion_gate_workers_shutdown) return false;
  try {
    g_promotion_gate_worker_threads.reserve(requested);
  } catch (...) {
    return false;
  }
  g_promotion_gate_workers_starting = true;
  const size_t missing = requested - g_promotion_gate_worker_threads.size();
  guard.unlock();

  std::vector<std::thread> created;
  bool complete = true;
  try {
    created.reserve(missing);
    for (size_t i = 0; i < missing; ++i) {
      created.emplace_back(promotion_gate_worker_main);
    }
  } catch (...) {
    complete = false;
  }

  guard.lock();
  for (std::thread &worker : created) {
    g_promotion_gate_worker_threads.emplace_back(std::move(worker));
  }
  g_promotion_gate_worker_count.store(g_promotion_gate_worker_threads.size(),
                                      std::memory_order_release);
  complete = complete && g_promotion_gate_worker_threads.size() >= requested;
  g_promotion_gate_workers_starting = false;
  guard.unlock();
  g_promotion_gate_worker_cv.notify_all();
  return complete;
}

struct Promotion_gate_batch_completion {
  explicit Promotion_gate_batch_completion(uint32_t count) : remaining(count) {}

  void complete() {
    std::lock_guard<std::mutex> guard(mutex);
    if (remaining != 0) --remaining;
    if (remaining == 0) cv.notify_all();
  }

  void wait() {
    std::unique_lock<std::mutex> guard(mutex);
    cv.wait(guard, [&] { return remaining == 0; });
  }

  std::mutex mutex;
  std::condition_variable cv;
  uint32_t remaining;
};

bool run_promotion_gate_worker_batch(uint32_t worker_count,
                                     std::function<void()> worker) {
  if (!ensure_promotion_gate_workers(worker_count)) return false;
  std::shared_ptr<Promotion_gate_batch_completion> completion;
  std::shared_ptr<std::function<void()>> shared_worker;
  try {
    completion =
        std::make_shared<Promotion_gate_batch_completion>(worker_count);
    shared_worker =
        std::make_shared<std::function<void()>>(std::move(worker));
  } catch (...) {
    return false;
  }

  uint32_t submitted = 0;
  {
    std::lock_guard<std::mutex> guard(g_promotion_gate_worker_mutex);
    if (g_promotion_gate_workers_shutdown) return false;
    try {
      for (; submitted < worker_count; ++submitted) {
        g_promotion_gate_worker_jobs.emplace_back(
            [completion, shared_worker] {
              try {
                (*shared_worker)();
              } catch (...) {
              }
              completion->complete();
            });
      }
    } catch (...) {
    }
  }
  g_promotion_gate_worker_cv.notify_all();
  for (; submitted < worker_count; ++submitted) {
    try {
      (*shared_worker)();
    } catch (...) {
    }
    completion->complete();
  }
  completion->wait();
  return true;
}
std::atomic<uint64_t> g_promotion_gate_record_lock_resident_pages{0};
std::atomic<uint64_t> g_promotion_gate_record_lock_cold_page_gets{0};
std::atomic<uint64_t> g_promotion_gate_ready_cache_miss_count{0};
std::atomic<uint64_t> g_promotion_gate_over_budget_count{0};

struct Promotion_ready_cache_key {
  std::string preserve_dir;
  std::string epoch_id;
  uint64_t token{0};

  bool operator<(const Promotion_ready_cache_key &other) const {
    if (preserve_dir != other.preserve_dir) {
      return preserve_dir < other.preserve_dir;
    }
    if (epoch_id != other.epoch_id) return epoch_id < other.epoch_id;
    return token < other.token;
  }
};

struct Promotion_ready_cache_entry {
  Preserve_trx_promotion_ready_state state{
      Preserve_trx_promotion_ready_state::NOT_FOUND};
  uint64_t required_apply_lsn{0};
  bool has_epoch_fact{false};
  std::array<unsigned char, kPromotionDigestBytes> epoch_fact_digest{};
  bool has_ready_bundle{false};
  Preserved_trx_bundle ready_bundle;
  bool record_lock_metadata_only{false};
  bool record_lock_pages_prewarmed{false};
  uint64_t record_lock_page_count{0};
  uint64_t record_lock_prefetch_submitted_pages{0};
  uint64_t record_lock_resident_pages{0};
  uint64_t record_lock_io_pending_pages{0};
  uint64_t record_lock_missing_pages{0};
  uint64_t record_lock_cold_gate_page_gets{0};
  uint64_t record_lock_gate_page_get_us{0};
  uint64_t record_lock_bitmap_pages{0};
  uint64_t record_lock_bitmap_bits{0};
  std::string reason;
  uint64_t estimated_bytes{0};
  uint64_t access_sequence{0};
};

struct Promotion_record_lock_proof {
  bool available{false};
  bool metadata_only{false};
  uint64_t page_count{0};
  uint64_t resident_pages{0};
  uint64_t cold_gets{0};
  uint64_t bitmap_pages{0};
  uint64_t bitmap_bits{0};
};

std::mutex g_ready_cache_mutex;
std::map<Promotion_ready_cache_key, Promotion_ready_cache_entry> g_ready_cache;
std::atomic<uint64_t> g_ready_cache_bytes{0};
std::atomic<uint64_t> g_ready_cache_evictions{0};
uint64_t g_ready_cache_access_sequence{0};

void add_ready_cache_bytes(uint64_t value, uint64_t *total) {
  if (total == nullptr) return;
  *total = value > std::numeric_limits<uint64_t>::max() - *total
               ? std::numeric_limits<uint64_t>::max()
               : *total + value;
}

void add_ready_cache_string(const std::string &value, uint64_t *total) {
  add_ready_cache_bytes(value.capacity(), total);
}

uint64_t ready_cache_bundle_bytes(const Preserved_trx_bundle &bundle) {
  uint64_t total = sizeof(bundle);
  const Preserve_snapshot_metadata &metadata = bundle.metadata;
  const std::string *metadata_strings[] = {
      &metadata.token,
      &metadata.owner_user,
      &metadata.owner_host,
      &metadata.schema_name,
      &metadata.binlog_cache_payload,
      &metadata.binlog_gtid_next,
      &metadata.binlog_owned_gtid,
      &metadata.time_zone_name,
      &metadata.read_view_payload,
      &metadata.record_locks_payload,
      &metadata.predicate_locks_payload,
      &metadata.table_locks_payload,
      &metadata.mdl_descriptors_payload,
      &metadata.user_vars_payload,
      &metadata.sql_savepoints_payload,
      &metadata.innodb_savepoints_payload,
      &metadata.temp_table_manifest_payload};
  for (const std::string *value : metadata_strings) {
    add_ready_cache_string(*value, &total);
  }
  add_ready_cache_bytes(metadata.modified_table_names.capacity() *
                            sizeof(Preserve_snapshot_modified_table_name),
                        &total);
  for (const Preserve_snapshot_modified_table_name &table :
       metadata.modified_table_names) {
    add_ready_cache_string(table.schema_name, &total);
    add_ready_cache_string(table.table_name, &total);
  }
  add_ready_cache_bytes(metadata.session_participant_order.capacity() *
                            sizeof(Preserve_savepoint_participant),
                        &total);
  add_ready_cache_bytes(metadata.savepoint_suffix_ordinals.capacity() *
                            sizeof(uint16_t),
                        &total);
  add_ready_cache_bytes(bundle.tlvs.capacity() * sizeof(Preserve_snapshot_tlv),
                        &total);
  for (const Preserve_snapshot_tlv &tlv : bundle.tlvs) {
    add_ready_cache_string(tlv.value, &total);
  }
  add_ready_cache_bytes(bundle.external_blobs.capacity() *
                            sizeof(Preserved_trx_external_blob),
                        &total);
  for (const Preserved_trx_external_blob &blob : bundle.external_blobs) {
    add_ready_cache_string(blob.name, &total);
    add_ready_cache_string(blob.payload, &total);
    add_ready_cache_string(blob.descriptor.name, &total);
    add_ready_cache_string(blob.warmcopy_id, &total);
  }
  add_ready_cache_bytes(bundle.blob_descriptors.capacity() *
                            sizeof(Preserved_trx_external_blob_descriptor),
                        &total);
  for (const Preserved_trx_external_blob_descriptor &descriptor :
       bundle.blob_descriptors) {
    add_ready_cache_string(descriptor.name, &total);
  }
  return total;
}

uint64_t ready_cache_entry_bytes(const Promotion_ready_cache_key &key,
                                 const Promotion_ready_cache_entry &entry) {
  uint64_t total = sizeof(key) + sizeof(entry);
  add_ready_cache_string(key.preserve_dir, &total);
  add_ready_cache_string(key.epoch_id, &total);
  add_ready_cache_string(entry.reason, &total);
  if (entry.has_ready_bundle) {
    add_ready_cache_bytes(ready_cache_bundle_bytes(entry.ready_bundle),
                          &total);
  }
  return total;
}

void ready_cache_store_locked(Promotion_ready_cache_key key,
                              Promotion_ready_cache_entry entry) {
  auto existing = g_ready_cache.find(key);
  uint64_t current_bytes = g_ready_cache_bytes.load();
  if (existing != g_ready_cache.end()) {
    current_bytes = current_bytes >= existing->second.estimated_bytes
                        ? current_bytes - existing->second.estimated_bytes
                        : 0;
    g_ready_cache.erase(existing);
  }

  const uint64_t cap =
      std::max<uint64_t>(1, preserve_trx_promotion_ready_cache_max_bytes);
  entry.access_sequence = ++g_ready_cache_access_sequence;
  entry.estimated_bytes = ready_cache_entry_bytes(key, entry);
  if (entry.estimated_bytes > cap && entry.has_ready_bundle) {
    entry.has_ready_bundle = false;
    entry.ready_bundle = Preserved_trx_bundle{};
    entry.state = Preserve_trx_promotion_ready_state::APPLY_PENDING;
    entry.reason = "promotion ready cache byte budget exceeded";
    entry.estimated_bytes = ready_cache_entry_bytes(key, entry);
    g_ready_cache_evictions.fetch_add(1);
  }

  while (entry.estimated_bytes <= cap &&
         current_bytes > cap - entry.estimated_bytes &&
         !g_ready_cache.empty()) {
    auto victim = std::min_element(
        g_ready_cache.begin(), g_ready_cache.end(),
        [](const auto &left, const auto &right) {
          return left.second.access_sequence < right.second.access_sequence;
        });
    current_bytes = current_bytes >= victim->second.estimated_bytes
                        ? current_bytes - victim->second.estimated_bytes
                        : 0;
    g_ready_cache.erase(victim);
    g_ready_cache_evictions.fetch_add(1);
  }
  if (entry.estimated_bytes > cap ||
      current_bytes > cap - entry.estimated_bytes) {
    g_ready_cache_evictions.fetch_add(1);
    g_ready_cache_bytes.store(current_bytes);
    return;
  }
  current_bytes += entry.estimated_bytes;
  g_ready_cache.emplace(std::move(key), std::move(entry));
  g_ready_cache_bytes.store(current_bytes);
}

bool parse_uint64_strict(const std::string &text, uint64_t *value) {
  if (value == nullptr || text.empty()) return false;
  uint64_t result = 0;
  for (char ch : text) {
    if (ch < '0' || ch > '9') return false;
    const uint64_t digit = static_cast<uint64_t>(ch - '0');
    if (result >
        (std::numeric_limits<uint64_t>::max() - digit) / 10ULL) {
      return false;
    }
    result = result * 10ULL + digit;
  }
  *value = result;
  return true;
}

template <size_t N>
std::string digest_hex(const std::array<unsigned char, N> &digest) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string hex;
  hex.reserve(digest.size() * 2);
  for (unsigned char byte : digest) {
    hex.push_back(kHex[(byte >> 4) & 0x0f]);
    hex.push_back(kHex[byte & 0x0f]);
  }
  return hex;
}

std::string sha256_hex(const std::string &payload) {
  std::array<unsigned char, kPromotionDigestBytes> digest{};
  SHA_EVP256(reinterpret_cast<const unsigned char *>(payload.data()),
             payload.length(), digest.data());
  return digest_hex(digest);
}

bool append_required_field(const std::string &name, const std::string &value,
                           std::string *body) {
  if (body == nullptr || value.empty() ||
      value.find('\n') != std::string::npos) {
    return false;
  }
  body->append(name);
  body->push_back('=');
  body->append(value);
  body->push_back('\n');
  return true;
}

bool marker_tokens_are_valid(const std::vector<uint64_t> &tokens) {
  if (tokens.empty()) return false;
  return std::all_of(tokens.begin(), tokens.end(),
                     [](uint64_t token) { return token != 0; });
}

bool ready_cache_lookup(const std::string &preserve_dir,
                        const std::string &epoch_id, uint64_t token,
                        Promotion_ready_cache_entry *entry) {
  std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
  const auto found =
      g_ready_cache.find(Promotion_ready_cache_key{preserve_dir, epoch_id,
                                                   token});
  if (found == g_ready_cache.end()) return false;
  found->second.access_sequence = ++g_ready_cache_access_sequence;
  if (entry != nullptr) *entry = found->second;
  return true;
}

std::vector<std::pair<uint64_t, Promotion_ready_cache_entry>>
ready_cache_entries_for_epoch(const std::string &preserve_dir,
                              const std::string &epoch_id) {
  std::vector<std::pair<uint64_t, Promotion_ready_cache_entry>> entries;
  std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
  for (const auto &item : g_ready_cache) {
    if (item.first.preserve_dir == preserve_dir &&
        item.first.epoch_id == epoch_id) {
      entries.emplace_back(item.first.token, item.second);
    }
  }
  return entries;
}

std::string ready_cache_reason_or_status(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, Preserve_trx_promotion_adopt_status status) {
  Promotion_ready_cache_entry entry;
  if (ready_cache_lookup(preserve_dir, epoch_id, token, &entry) &&
      !entry.reason.empty()) {
    return entry.reason;
  }
  return preserve_trx_promotion_adopt_status_name(status);
}

bool ready_cache_entry_needs_record_lock_prewarm(
    const Promotion_ready_cache_entry &entry) {
  if (!entry.has_ready_bundle) return false;
  if (!entry.ready_bundle.metadata.record_locks_payload.empty()) return true;
  for (const Preserved_trx_external_blob &blob :
       entry.ready_bundle.external_blobs) {
    if (blob.name == kPreservedTrxBlobRecordLocks) return true;
  }
  return false;
}

bool select_record_lock_payload_for_prewarm(const Preserved_trx_bundle &bundle,
                                            const std::string **payload,
                                            std::string *reason) {
  if (payload == nullptr) return false;
  *payload = nullptr;
  if (!bundle.metadata.record_locks_payload.empty()) {
    *payload = &bundle.metadata.record_locks_payload;
    return true;
  }

  const Preserved_trx_external_blob *record_blob = nullptr;
  for (const Preserved_trx_external_blob &blob : bundle.external_blobs) {
    if (blob.name != kPreservedTrxBlobRecordLocks) continue;
    if (record_blob != nullptr) {
      if (reason != nullptr) *reason = "duplicate record-lock external blob";
      return false;
    }
    record_blob = &blob;
  }
  if (record_blob == nullptr) return true;

  if (record_blob->payload.empty() ||
      record_blob->descriptor.name != kPreservedTrxBlobRecordLocks ||
      record_blob->descriptor.size != record_blob->payload.length()) {
    if (reason != nullptr) *reason = "record-lock external blob is incomplete";
    return false;
  }
  *payload = &record_blob->payload;
  return true;
}

bool record_lock_pages_are_gate_ready(bool record_lock_pages_prewarmed,
                                      uint64_t record_lock_page_count,
                                      uint64_t prefetch_submitted_pages,
                                      uint64_t resident_pages) {
  if (record_lock_page_count == 0) return true;
  return record_lock_pages_prewarmed &&
         prefetch_submitted_pages == record_lock_page_count &&
         resident_pages == record_lock_page_count;
}

bool wait_for_record_lock_pages_to_be_gate_ready(
    bool record_lock_pages_prewarmed, uint64_t record_lock_page_count,
    uint64_t prefetch_submitted_pages, uint64_t deadline_us,
    const std::function<bool(trx_preserve_record_lock_residency_t *)> &sample,
    trx_preserve_record_lock_residency_t *last_residency,
    uint64_t *sample_count) {
  if (sample_count != nullptr) *sample_count = 0;
  if (last_residency != nullptr) {
    *last_residency = trx_preserve_record_lock_residency_t{};
  }
  if (record_lock_page_count == 0) return true;
  if (!record_lock_pages_prewarmed ||
      prefetch_submitted_pages != record_lock_page_count) {
    return false;
  }

  for (;;) {
    trx_preserve_record_lock_residency_t residency;
    if (!sample(&residency)) return false;
    if (sample_count != nullptr) ++*sample_count;
    if (last_residency != nullptr) *last_residency = residency;

    if (residency.page_count == record_lock_page_count &&
        record_lock_pages_are_gate_ready(record_lock_pages_prewarmed,
                                         record_lock_page_count,
                                         prefetch_submitted_pages,
                                         residency.resident_pages)) {
      return true;
    }

    const uint64_t now_us = my_micro_time();
    if (deadline_us == 0 || now_us >= deadline_us) return false;
    const uint64_t remaining_us = deadline_us - now_us;
    my_sleep(static_cast<ulong>(
        std::min<uint64_t>(remaining_us, kRecordLockResidencyPollIntervalUs)));
  }
}

std::string promotion_epoch_fact_path(const std::string &preserve_dir,
                                      const std::string &epoch_id) {
  constexpr char kPathSeparator = '/';
  std::string root = preserve_dir;
  if (!root.empty() && root.back() != kPathSeparator) {
    root.push_back(kPathSeparator);
  }
  root.append(".transfer");
  root.push_back(kPathSeparator);
  root.append(epoch_id);
  root.push_back(kPathSeparator);
  root.append("epoch.fact");
  return root;
}

bool promotion_epoch_fact_file_exists(const std::string &preserve_dir,
                                      const std::string &epoch_id) {
  MY_STAT stat_area;
  return my_stat(promotion_epoch_fact_path(preserve_dir, epoch_id).c_str(),
                 &stat_area, MYF(0)) != nullptr;
}

bool promotion_adopt_ready_bundle_default(
    const std::string &preserve_dir, const Preserved_trx_bundle &ready_bundle,
    uint64_t gate_deadline_us,
    Preserve_trx_promotion_token_result *token_result) {
  if (token_result == nullptr) return false;
  *token_result = {};
  uint64_t token = 0;
  if (!parse_uint64_strict(ready_bundle.metadata.token, &token) ||
      token == 0) {
    token_result->status =
        Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
    token_result->cleanup_state =
        Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED;
    token_result->reason = "ready record has invalid token";
    return false;
  }
  token_result->token = token;

  Preserved_trx_promotion_ready_adopt_result adopt_result;
  Preserved_trx_bundle bundle_copy = ready_bundle;
  if (preserved_trx_import_reserved_bundle_for_promotion(
          preserve_dir, std::move(bundle_copy), &adopt_result,
          gate_deadline_us)) {
    token_result->status = Preserve_trx_promotion_adopt_status::OK;
    token_result->claimed = true;
    token_result->cleanup_state = Preserve_trx_promotion_cleanup_state::NONE;
    token_result->reason = "adopted";
    return true;
  }

  token_result->claimed = adopt_result.claimed;
  token_result->reason =
      adopt_result.reason.empty() ? "promotion adopt failed"
                                  : adopt_result.reason;
  if (adopt_result.claimed) {
    token_result->status =
        Preserve_trx_promotion_adopt_status::CLAIMED_IMPORT_FAILED;
    token_result->cleanup_state =
        adopt_result.rolled_back
            ? Preserve_trx_promotion_cleanup_state::CLEANUP_ROLLED_BACK
            : Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED;
    return false;
  }

  token_result->status = Preserve_trx_promotion_adopt_status::TOKEN_NOT_FOUND;
  token_result->cleanup_state =
      Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING;
  return false;
}

bool field_value_is_safe(const std::string &value) {
  return !value.empty() && value.find('\n') == std::string::npos &&
         value.find('|') == std::string::npos;
}

const char *cleanup_state_name(Preserve_trx_promotion_cleanup_state state) {
  switch (state) {
    case Preserve_trx_promotion_cleanup_state::NONE:
      return "NONE";
    case Preserve_trx_promotion_cleanup_state::NOT_CLAIMED:
      return "NOT_CLAIMED";
    case Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING:
      return "CLEANUP_PENDING";
    case Preserve_trx_promotion_cleanup_state::CLEANUP_ROLLED_BACK:
      return "CLEANUP_ROLLED_BACK";
    case Preserve_trx_promotion_cleanup_state::CLEANUP_NOT_FOUND:
      return "CLEANUP_NOT_FOUND";
    case Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED:
      return "CLEANUP_TAINTED";
  }
  return "UNKNOWN";
}

bool parse_cleanup_state(const std::string &text,
                         Preserve_trx_promotion_cleanup_state *state) {
  if (state == nullptr) return false;
  if (text == "NONE") {
    *state = Preserve_trx_promotion_cleanup_state::NONE;
    return true;
  }
  if (text == "NOT_CLAIMED") {
    *state = Preserve_trx_promotion_cleanup_state::NOT_CLAIMED;
    return true;
  }
  if (text == "CLEANUP_PENDING") {
    *state = Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING;
    return true;
  }
  if (text == "CLEANUP_ROLLED_BACK") {
    *state = Preserve_trx_promotion_cleanup_state::CLEANUP_ROLLED_BACK;
    return true;
  }
  if (text == "CLEANUP_NOT_FOUND") {
    *state = Preserve_trx_promotion_cleanup_state::CLEANUP_NOT_FOUND;
    return true;
  }
  if (text == "CLEANUP_TAINTED") {
    *state = Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED;
    return true;
  }
  return false;
}

const char *intent_state_name(Preserve_trx_promotion_intent_state state) {
  switch (state) {
    case Preserve_trx_promotion_intent_state::CANDIDATE:
      return "CANDIDATE";
    case Preserve_trx_promotion_intent_state::ADOPTING:
      return "ADOPTING";
    case Preserve_trx_promotion_intent_state::ADOPTED:
      return "ADOPTED";
    case Preserve_trx_promotion_intent_state::ABANDONED:
      return "ABANDONED";
    case Preserve_trx_promotion_intent_state::CLEANUP_PENDING:
      return "CLEANUP_PENDING";
    case Preserve_trx_promotion_intent_state::CLEANUP_ROLLED_BACK:
      return "CLEANUP_ROLLED_BACK";
    case Preserve_trx_promotion_intent_state::CLEANUP_NOT_FOUND:
      return "CLEANUP_NOT_FOUND";
    case Preserve_trx_promotion_intent_state::CLEANUP_TAINTED:
      return "CLEANUP_TAINTED";
  }
  return "UNKNOWN";
}

bool parse_intent_state(const std::string &text,
                        Preserve_trx_promotion_intent_state *state) {
  if (state == nullptr) return false;
  if (text == "CANDIDATE") {
    *state = Preserve_trx_promotion_intent_state::CANDIDATE;
    return true;
  }
  if (text == "ADOPTING") {
    *state = Preserve_trx_promotion_intent_state::ADOPTING;
    return true;
  }
  if (text == "ADOPTED") {
    *state = Preserve_trx_promotion_intent_state::ADOPTED;
    return true;
  }
  if (text == "ABANDONED") {
    *state = Preserve_trx_promotion_intent_state::ABANDONED;
    return true;
  }
  if (text == "CLEANUP_PENDING") {
    *state = Preserve_trx_promotion_intent_state::CLEANUP_PENDING;
    return true;
  }
  if (text == "CLEANUP_ROLLED_BACK") {
    *state = Preserve_trx_promotion_intent_state::CLEANUP_ROLLED_BACK;
    return true;
  }
  if (text == "CLEANUP_NOT_FOUND") {
    *state = Preserve_trx_promotion_intent_state::CLEANUP_NOT_FOUND;
    return true;
  }
  if (text == "CLEANUP_TAINTED") {
    *state = Preserve_trx_promotion_intent_state::CLEANUP_TAINTED;
    return true;
  }
  return false;
}

bool parse_adopt_status(const std::string &text,
                        Preserve_trx_promotion_adopt_status *status) {
  if (status == nullptr) return false;
  static const Preserve_trx_promotion_adopt_status kStatuses[] = {
      Preserve_trx_promotion_adopt_status::OK,
      Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS,
      Preserve_trx_promotion_adopt_status::NOT_ENABLED,
      Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT,
      Preserve_trx_promotion_adopt_status::IO_ERROR,
      Preserve_trx_promotion_adopt_status::TOKEN_NOT_FOUND,
      Preserve_trx_promotion_adopt_status::NOT_STANDBY_PENDING,
      Preserve_trx_promotion_adopt_status::EPOCH_NOT_COMMITTED,
      Preserve_trx_promotion_adopt_status::APPLY_BARRIER_NOT_REACHED,
      Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
      Preserve_trx_promotion_adopt_status::TOO_MANY_PROMOTION_TOKENS,
      Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
      Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT,
      Preserve_trx_promotion_adopt_status::CLAIMED_IMPORT_FAILED,
      Preserve_trx_promotion_adopt_status::TOKEN_ABANDONED,
      Preserve_trx_promotion_adopt_status::CLEANUP_PENDING,
      Preserve_trx_promotion_adopt_status::CLEANUP_TAINTED};
  for (Preserve_trx_promotion_adopt_status candidate : kStatuses) {
    if (text == preserve_trx_promotion_adopt_status_name(candidate)) {
      *status = candidate;
      return true;
    }
  }
  return false;
}

bool abandoned_token_is_valid(
    const Preserve_trx_promotion_token_result &token) {
  if (token.token == 0 ||
      token.status == Preserve_trx_promotion_adopt_status::OK ||
      token.status ==
          Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS ||
      !field_value_is_safe(token.reason)) {
    return false;
  }

  switch (token.cleanup_state) {
    case Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING:
    case Preserve_trx_promotion_cleanup_state::CLEANUP_NOT_FOUND:
    case Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED:
      break;
    case Preserve_trx_promotion_cleanup_state::CLEANUP_ROLLED_BACK:
      if (!token.claimed) return false;
      if (token.status !=
              Preserve_trx_promotion_adopt_status::CLAIMED_IMPORT_FAILED &&
          token.status != Preserve_trx_promotion_adopt_status::TOKEN_ABANDONED) {
        return false;
      }
      break;
    case Preserve_trx_promotion_cleanup_state::NONE:
    case Preserve_trx_promotion_cleanup_state::NOT_CLAIMED:
      return false;
  }

  if (token.claimed) {
    if (token.cleanup_state !=
            Preserve_trx_promotion_cleanup_state::CLEANUP_ROLLED_BACK &&
        token.cleanup_state !=
            Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED) {
      return false;
    }
    if (token.status !=
            Preserve_trx_promotion_adopt_status::CLAIMED_IMPORT_FAILED &&
        token.status != Preserve_trx_promotion_adopt_status::TOKEN_ABANDONED) {
      return false;
    }
  }
  if (token.status ==
          Preserve_trx_promotion_adopt_status::CLAIMED_IMPORT_FAILED &&
      !token.claimed) {
    return false;
  }
  return true;
}

bool intent_token_is_valid(
    const Preserve_trx_promotion_intent_token &token) {
  if (token.token == 0 || !field_value_is_safe(token.reason)) return false;

  switch (token.state) {
    case Preserve_trx_promotion_intent_state::CANDIDATE:
    case Preserve_trx_promotion_intent_state::ADOPTING:
    case Preserve_trx_promotion_intent_state::ADOPTED:
      return token.cleanup_state == Preserve_trx_promotion_cleanup_state::NONE;
    case Preserve_trx_promotion_intent_state::ABANDONED:
    case Preserve_trx_promotion_intent_state::CLEANUP_PENDING:
      return token.cleanup_state ==
             Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING;
    case Preserve_trx_promotion_intent_state::CLEANUP_ROLLED_BACK:
      return token.cleanup_state ==
             Preserve_trx_promotion_cleanup_state::CLEANUP_ROLLED_BACK;
    case Preserve_trx_promotion_intent_state::CLEANUP_NOT_FOUND:
      return token.cleanup_state ==
             Preserve_trx_promotion_cleanup_state::CLEANUP_NOT_FOUND;
    case Preserve_trx_promotion_intent_state::CLEANUP_TAINTED:
      return token.cleanup_state ==
             Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED;
  }
  return false;
}

std::string encode_intent_token(
    const Preserve_trx_promotion_intent_token &token) {
  std::string encoded = std::to_string(token.token);
  encoded.push_back('|');
  encoded.append(intent_state_name(token.state));
  encoded.push_back('|');
  encoded.append(cleanup_state_name(token.cleanup_state));
  encoded.push_back('|');
  encoded.append(token.reason);
  return encoded;
}

bool parse_intent_token(const std::string &encoded,
                        Preserve_trx_promotion_intent_token *token) {
  if (token == nullptr) return false;
  const size_t first = encoded.find('|');
  if (first == std::string::npos) return false;
  const size_t second = encoded.find('|', first + 1);
  if (second == std::string::npos) return false;
  const size_t third = encoded.find('|', second + 1);
  if (third == std::string::npos ||
      encoded.find('|', third + 1) != std::string::npos) {
    return false;
  }

  Preserve_trx_promotion_intent_token parsed;
  if (!parse_uint64_strict(encoded.substr(0, first), &parsed.token) ||
      !parse_intent_state(encoded.substr(first + 1, second - first - 1),
                          &parsed.state) ||
      !parse_cleanup_state(encoded.substr(second + 1, third - second - 1),
                           &parsed.cleanup_state)) {
    return false;
  }
  parsed.reason = encoded.substr(third + 1);
  if (!intent_token_is_valid(parsed)) return false;
  *token = std::move(parsed);
  return true;
}

std::string encode_abandoned_token(
    const Preserve_trx_promotion_token_result &token) {
  std::string encoded = std::to_string(token.token);
  encoded.push_back('|');
  encoded.append(preserve_trx_promotion_adopt_status_name(token.status));
  encoded.push_back('|');
  encoded.append(token.claimed ? "1" : "0");
  encoded.push_back('|');
  encoded.append(cleanup_state_name(token.cleanup_state));
  encoded.push_back('|');
  encoded.append(token.reason);
  return encoded;
}

bool parse_abandoned_token(const std::string &encoded,
                           Preserve_trx_promotion_token_result *token) {
  if (token == nullptr) return false;
  size_t parts[4];
  size_t start = 0;
  for (size_t i = 0; i < 4; ++i) {
    parts[i] = encoded.find('|', start);
    if (parts[i] == std::string::npos) return false;
    start = parts[i] + 1;
  }
  if (encoded.find('|', start) != std::string::npos) return false;

  Preserve_trx_promotion_token_result parsed;
  if (!parse_uint64_strict(encoded.substr(0, parts[0]), &parsed.token) ||
      parsed.token == 0 ||
      !parse_adopt_status(encoded.substr(parts[0] + 1,
                                         parts[1] - parts[0] - 1),
                          &parsed.status)) {
    return false;
  }
  const std::string claimed = encoded.substr(parts[1] + 1,
                                             parts[2] - parts[1] - 1);
  if (claimed == "1") {
    parsed.claimed = true;
  } else if (claimed == "0") {
    parsed.claimed = false;
  } else {
    return false;
  }
  if (!parse_cleanup_state(
          encoded.substr(parts[2] + 1, parts[3] - parts[2] - 1),
          &parsed.cleanup_state)) {
    return false;
  }
  parsed.reason = encoded.substr(parts[3] + 1);
  if (!abandoned_token_is_valid(parsed)) return false;
  *token = std::move(parsed);
  return true;
}

void add_abandoned_token(Preserve_trx_promotion_adopt_result *result,
                         uint64_t token,
                         Preserve_trx_promotion_adopt_status status,
                         const std::string &reason,
                         Preserve_trx_promotion_cleanup_state cleanup_state) {
  if (result == nullptr) return;
  Preserve_trx_promotion_token_result token_result;
  token_result.token = token;
  token_result.status = status;
  token_result.claimed = false;
  token_result.cleanup_state = cleanup_state;
  token_result.reason = reason;
  result->token_results.push_back(token_result);
  ++result->abandoned_count;
  if (cleanup_state == Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING) {
    ++result->cleanup_pending_count;
  } else if (cleanup_state ==
             Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED) {
    ++result->cleanup_failed_count;
  }
}

void add_abandoned_token_result(
    Preserve_trx_promotion_adopt_result *result,
    const Preserve_trx_promotion_token_result &token_result) {
  if (result == nullptr) return;
  if (!abandoned_token_is_valid(token_result)) {
    Preserve_trx_promotion_token_result tainted = token_result;
    if (tainted.token == 0) tainted.token = 1;
    tainted.status = Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
    tainted.claimed = false;
    tainted.cleanup_state =
        Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED;
    tainted.reason = "invalid abandoned token result";
    result->token_results.push_back(std::move(tainted));
    ++result->abandoned_count;
    ++result->cleanup_failed_count;
    return;
  }
  result->token_results.push_back(token_result);
  ++result->abandoned_count;
  if (token_result.cleanup_state ==
      Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING) {
    ++result->cleanup_pending_count;
  } else if (token_result.cleanup_state ==
             Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED) {
    ++result->cleanup_failed_count;
  }
}

std::string encode_token_list(std::vector<uint64_t> tokens) {
  std::sort(tokens.begin(), tokens.end());
  std::string encoded;
  for (uint64_t token : tokens) {
    if (!encoded.empty()) encoded.push_back(',');
    encoded.append(std::to_string(token));
  }
  return encoded;
}

bool parse_token_list(const std::string &encoded,
                      std::vector<uint64_t> *tokens) {
  if (tokens == nullptr || encoded.empty()) return false;
  tokens->clear();
  size_t start = 0;
  while (start <= encoded.length()) {
    const size_t comma = encoded.find(',', start);
    const size_t end = comma == std::string::npos ? encoded.length() : comma;
    if (end == start) return false;
    uint64_t token = 0;
    if (!parse_uint64_strict(encoded.substr(start, end - start), &token) ||
        token == 0) {
      return false;
    }
    tokens->push_back(token);
    if (comma == std::string::npos) break;
    start = comma + 1;
  }
  std::vector<uint64_t> sorted = *tokens;
  std::sort(sorted.begin(), sorted.end());
  return sorted == *tokens;
}

bool read_marker_field(const std::vector<std::string> &lines,
                       const std::string &name, std::string *value) {
  if (value == nullptr) return false;
  const std::string prefix = name + "=";
  for (const std::string &line : lines) {
    if (line.compare(0, prefix.length(), prefix) == 0) {
      *value = line.substr(prefix.length());
      return !value->empty();
    }
  }
  return false;
}

std::vector<std::string> split_lines(const std::string &body) {
  std::vector<std::string> lines;
  size_t start = 0;
  while (start < body.length()) {
    const size_t end = body.find('\n', start);
    if (end == std::string::npos) {
      lines.push_back(body.substr(start));
      break;
    }
    lines.push_back(body.substr(start, end - start));
    start = end + 1;
  }
  return lines;
}

Preserve_trx_promotion_adopt_status carrier_status_to_promotion_status(
    Preserve_snapshot_status status) {
  switch (status) {
    case Preserve_snapshot_status::OK:
      return Preserve_trx_promotion_adopt_status::OK;
    case Preserve_snapshot_status::NOT_FOUND:
      return Preserve_trx_promotion_adopt_status::TOKEN_NOT_FOUND;
    case Preserve_snapshot_status::CORRUPT:
      return Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
    case Preserve_snapshot_status::UNSUPPORTED:
      return Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT;
    case Preserve_snapshot_status::INVALID_ARGUMENT:
      return Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT;
    case Preserve_snapshot_status::IO_ERROR:
    default:
      return Preserve_trx_promotion_adopt_status::IO_ERROR;
  }
}

const char *preserve_snapshot_status_name(Preserve_snapshot_status status) {
  switch (status) {
    case Preserve_snapshot_status::OK:
      return "OK";
    case Preserve_snapshot_status::NOT_FOUND:
      return "NOT_FOUND";
    case Preserve_snapshot_status::CORRUPT:
      return "CORRUPT";
    case Preserve_snapshot_status::UNSUPPORTED:
      return "UNSUPPORTED";
    case Preserve_snapshot_status::INVALID_ARGUMENT:
      return "INVALID_ARGUMENT";
    case Preserve_snapshot_status::IO_ERROR:
      return "IO_ERROR";
  }
  return "UNKNOWN";
}

void append_ready_summary_token_result(
    Preserve_trx_promotion_ready_summary *summary, uint64_t token,
    Preserve_trx_promotion_adopt_status status, const std::string &reason) {
  if (summary == nullptr) return;
  Preserve_trx_promotion_token_result token_result;
  token_result.token = token;
  token_result.status = status;
  token_result.reason = reason.empty() ? "promotion-ready state unavailable"
                                       : reason;
  if (status == Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT) {
    token_result.cleanup_state =
        Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED;
  } else if (status != Preserve_trx_promotion_adopt_status::OK) {
    token_result.cleanup_state =
        Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING;
  }
  summary->token_results.push_back(std::move(token_result));
}

void finish_result(Preserve_trx_promotion_adopt_result *result,
                   Preserve_trx_promotion_adopt_status status,
                   uint64_t started_us) {
  if (result == nullptr) return;
  result->status = status;
  result->elapsed_us = my_micro_time() - started_us;
}

void publish_promotion_gate_metrics(
    const Preserve_trx_promotion_adopt_result &result) {
  g_promotion_gate_elapsed_us.store(result.elapsed_us);
  g_promotion_gate_token_count.store(result.seen_tokens.size());
  g_promotion_gate_adopted_count.store(result.adopted_count);
  g_promotion_gate_abandoned_count.store(result.abandoned_count);
  g_promotion_gate_skipped_count.store(result.skipped_count);
  g_promotion_gate_max_worker_elapsed_us.store(result.max_worker_elapsed_us);
  g_promotion_gate_p50_worker_elapsed_us.store(result.p50_worker_elapsed_us);
  g_promotion_gate_p95_worker_elapsed_us.store(result.p95_worker_elapsed_us);
  g_promotion_gate_status_code.store(static_cast<uint64_t>(result.status));
  g_promotion_gate_record_lock_page_count.store(
      result.record_lock_page_count);
  g_promotion_gate_record_lock_resident_pages.store(
      result.record_lock_resident_pages);
  g_promotion_gate_record_lock_cold_page_gets.store(
      result.record_lock_cold_page_gets);
  g_promotion_gate_ready_cache_miss_count.store(
      result.ready_cache_miss_count);
  g_promotion_gate_over_budget_count.store(result.over_budget_count);
}

uint64_t promotion_gate_worker_elapsed_percentile(
    std::vector<uint64_t> elapsed_samples, uint32_t percentile) {
  if (elapsed_samples.empty()) return 0;
  std::sort(elapsed_samples.begin(), elapsed_samples.end());
  const size_t index =
      std::min(elapsed_samples.size() - 1,
               ((elapsed_samples.size() * percentile) + 99) / 100 - 1);
  return elapsed_samples[index];
}

bool promotion_cleanup_terminal_absence_proven(const std::string &token) {
  return preserved_trx_recovery_complete() &&
         !preserved_trx_local_record_exists(token) &&
         !trx_preserve_token_has_any_owner(token.c_str());
}

void append_result_message(Preserve_trx_promotion_adopt_result *result,
                           const std::string &message) {
  if (result == nullptr || message.empty()) return;
  if (!result->message.empty()) result->message.append("; ");
  result->message.append(message);
}

void write_abandoned_marker_for_result(
    Preserved_trx_store *store,
    const Preserve_trx_promotion_adopt_all_request &request,
    Preserve_trx_promotion_adopt_result *result) {
  if (store == nullptr || result == nullptr || result->abandoned_count == 0) {
    return;
  }

  Preserve_trx_promotion_abandoned_epoch_marker marker;
  marker.epoch_id = request.epoch_id;
  marker.source_server_uuid = "unknown-source";
  marker.target_server_uuid = "local-promotion-target";
  marker.applied_lsn = request.required_apply_lsn;
  marker.generated_at_us = my_micro_time();

  for (const Preserve_trx_promotion_token_result &token :
       result->token_results) {
    if (!abandoned_token_is_valid(token)) {
      ++result->cleanup_failed_count;
      append_result_message(result,
                            "abandoned token result could not be persisted");
      continue;
    }
    marker.tokens.push_back(token);
  }
  if (marker.tokens.empty()) return;

  std::string encoded;
  const uint64_t marker_started_us = my_micro_time();
  if (!preserved_trx_encode_promotion_abandoned_epoch_marker(marker,
                                                             &encoded)) {
    result->marker_us += my_micro_time() - marker_started_us;
    ++result->cleanup_failed_count;
    append_result_message(result, "failed to encode abandoned marker");
    return;
  }
  const Preserve_snapshot_status write_status =
      store->write_promotion_abandoned_epoch(request.epoch_id, encoded);
  result->marker_us += my_micro_time() - marker_started_us;
  if (write_status != Preserve_snapshot_status::OK) {
    ++result->cleanup_failed_count;
    append_result_message(result, "failed to write abandoned marker");
  }
}

bool write_adopted_marker_for_tokens(
    Preserved_trx_store *store,
    const Preserve_trx_promotion_adopt_all_request &request,
    const std::vector<uint64_t> &adopted_tokens,
    Preserve_trx_promotion_adopt_result *result) {
  if (store == nullptr || result == nullptr) return false;
  if (adopted_tokens.empty()) return true;

  Preserve_trx_promotion_adopted_epoch_marker marker;
  marker.epoch_id = request.epoch_id;
  marker.tokens = adopted_tokens;
  marker.source_server_uuid = "unknown-source";
  marker.target_server_uuid = "local-promotion-target";
  marker.applied_lsn = request.required_apply_lsn;
  marker.generated_at_us = my_micro_time();

  std::string encoded;
  const uint64_t marker_started_us = my_micro_time();
  if (!preserved_trx_encode_promotion_adopted_epoch_marker(marker, &encoded)) {
    result->marker_us += my_micro_time() - marker_started_us;
    ++result->cleanup_failed_count;
    append_result_message(result, "failed to encode adopted marker");
    return false;
  }
  const Preserve_snapshot_status write_status =
      store->write_promotion_adopted_epoch(request.epoch_id, encoded);
  result->marker_us += my_micro_time() - marker_started_us;
  if (write_status != Preserve_snapshot_status::OK) {
    ++result->cleanup_failed_count;
    append_result_message(result, "failed to write adopted marker");
    return false;
  }
  return true;
}

bool write_intent_marker(
    Preserved_trx_store *store,
    const Preserve_trx_promotion_adopt_all_request &request,
    const std::vector<Preserve_trx_promotion_intent_token> &tokens,
    Preserve_trx_promotion_adopt_result *result) {
  if (store == nullptr || result == nullptr || tokens.empty()) return false;

  Preserve_trx_promotion_intent_epoch_marker marker;
  marker.epoch_id = request.epoch_id;
  marker.source_server_uuid = "unknown-source";
  marker.target_server_uuid = "local-promotion-target";
  marker.required_apply_lsn = request.required_apply_lsn;
  marker.generated_at_us = my_micro_time();
  marker.tokens = tokens;

  std::string encoded;
  const uint64_t marker_started_us = my_micro_time();
  if (!preserved_trx_encode_promotion_intent_epoch_marker(marker, &encoded)) {
    result->marker_us += my_micro_time() - marker_started_us;
    ++result->cleanup_failed_count;
    append_result_message(result, "failed to encode promotion intent marker");
    return false;
  }
  const Preserve_snapshot_status write_status =
      store->write_promotion_intent_epoch(request.epoch_id, encoded);
  result->marker_us += my_micro_time() - marker_started_us;
  if (write_status != Preserve_snapshot_status::OK) {
    ++result->cleanup_failed_count;
    append_result_message(result, "failed to write promotion intent marker");
    return false;
  }
  return true;
}

bool write_intent_marker_for_tokens(
    Preserved_trx_store *store,
    const Preserve_trx_promotion_adopt_all_request &request,
    const std::vector<uint64_t> &tokens,
    Preserve_trx_promotion_adopt_result *result) {
  std::vector<Preserve_trx_promotion_intent_token> intent_tokens;
  intent_tokens.reserve(tokens.size());
  for (uint64_t token : tokens) {
    intent_tokens.push_back(
        {token, Preserve_trx_promotion_intent_state::ADOPTING,
         Preserve_trx_promotion_cleanup_state::NONE, "adopting"});
  }
  return write_intent_marker(store, request, intent_tokens, result);
}

Preserve_trx_promotion_intent_state intent_state_from_cleanup(
    Preserve_trx_promotion_cleanup_state cleanup_state) {
  switch (cleanup_state) {
    case Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING:
      return Preserve_trx_promotion_intent_state::CLEANUP_PENDING;
    case Preserve_trx_promotion_cleanup_state::CLEANUP_ROLLED_BACK:
      return Preserve_trx_promotion_intent_state::CLEANUP_ROLLED_BACK;
    case Preserve_trx_promotion_cleanup_state::CLEANUP_NOT_FOUND:
      return Preserve_trx_promotion_intent_state::CLEANUP_NOT_FOUND;
    case Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED:
      return Preserve_trx_promotion_intent_state::CLEANUP_TAINTED;
    case Preserve_trx_promotion_cleanup_state::NONE:
    case Preserve_trx_promotion_cleanup_state::NOT_CLAIMED:
      return Preserve_trx_promotion_intent_state::ABANDONED;
  }
  return Preserve_trx_promotion_intent_state::CLEANUP_TAINTED;
}

bool write_final_intent_marker_for_result(
    Preserved_trx_store *store,
    const Preserve_trx_promotion_adopt_all_request &request,
    const std::vector<uint64_t> &adopted_tokens,
    Preserve_trx_promotion_adopt_result *result) {
  if (result == nullptr) return false;
  std::vector<Preserve_trx_promotion_intent_token> intent_tokens;
  intent_tokens.reserve(adopted_tokens.size() + result->token_results.size());
  for (uint64_t token : adopted_tokens) {
    intent_tokens.push_back(
        {token, Preserve_trx_promotion_intent_state::ADOPTED,
         Preserve_trx_promotion_cleanup_state::NONE, "adopted"});
  }
  for (const Preserve_trx_promotion_token_result &token_result :
       result->token_results) {
    intent_tokens.push_back(
        {token_result.token, intent_state_from_cleanup(token_result.cleanup_state),
         token_result.cleanup_state, token_result.reason});
  }
  DBUG_EXECUTE_IF("preserve_trx_fail_write_final_promotion_intent_epoch",
                  return false;);
  return write_intent_marker(store, request, intent_tokens, result);
}

void taint_adopted_tokens_after_marker_failure(
    Preserve_trx_promotion_adopt_result *result, std::vector<uint64_t> *tokens,
    const std::string &reason) {
  if (result == nullptr || tokens == nullptr || tokens->empty()) return;
  if (result->adopted_count >= tokens->size()) {
    result->adopted_count -= tokens->size();
  } else {
    result->adopted_count = 0;
  }
  for (uint64_t token : *tokens) {
    add_abandoned_token(result, token,
                        Preserve_trx_promotion_adopt_status::CLEANUP_TAINTED,
                        reason,
                        Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED);
  }
  append_result_message(result, reason);
  tokens->clear();
}

}  // namespace

bool preserved_trx_promotion_start_gate_workers() {
  const uint32_t requested = std::max<uint32_t>(
      1, std::min<uint32_t>(preserve_trx_promotion_gate_workers,
                            kPromotionGateWorkerLimit));
  return ensure_promotion_gate_workers(requested);
}

void preserved_trx_promotion_shutdown_gate_workers() {
  std::vector<std::thread> workers;
  {
    std::unique_lock<std::mutex> guard(g_promotion_gate_worker_mutex);
    g_promotion_gate_worker_cv.wait(guard, [] {
      return !g_promotion_gate_workers_starting &&
             !g_promotion_gate_workers_stopping;
    });
    g_promotion_gate_workers_stopping = true;
    g_promotion_gate_workers_shutdown = true;
    workers.swap(g_promotion_gate_worker_threads);
    g_promotion_gate_worker_count.store(0, std::memory_order_release);
  }
  g_promotion_gate_worker_cv.notify_all();
  for (std::thread &worker : workers) {
    if (worker.joinable()) worker.join();
  }
  {
    std::lock_guard<std::mutex> guard(g_promotion_gate_worker_mutex);
    g_promotion_gate_worker_jobs.clear();
    g_promotion_gate_workers_shutdown = false;
    g_promotion_gate_workers_stopping = false;
  }
  g_promotion_gate_worker_cv.notify_all();
}

uint64_t preserve_trx_promotion_gate_worker_count_status() {
  return g_promotion_gate_worker_count.load(std::memory_order_acquire);
}

uint64_t preserve_trx_promotion_gate_worker_active_count_status() {
  return g_promotion_gate_worker_active_count.load(std::memory_order_acquire);
}

uint64_t preserve_trx_promotion_gate_worker_idle_count_status() {
  const uint64_t workers =
      g_promotion_gate_worker_count.load(std::memory_order_acquire);
  const uint64_t active =
      g_promotion_gate_worker_active_count.load(std::memory_order_acquire);
  return workers > active ? workers - active : 0;
}

const char *preserve_trx_promotion_adopt_status_name(
    Preserve_trx_promotion_adopt_status status) {
  switch (status) {
    case Preserve_trx_promotion_adopt_status::OK:
      return "OK";
    case Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS:
      return "OK_WITH_ABANDONED_TOKENS";
    case Preserve_trx_promotion_adopt_status::NOT_ENABLED:
      return "NOT_ENABLED";
    case Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT:
      return "INVALID_ARGUMENT";
    case Preserve_trx_promotion_adopt_status::IO_ERROR:
      return "IO_ERROR";
    case Preserve_trx_promotion_adopt_status::TOKEN_NOT_FOUND:
      return "TOKEN_NOT_FOUND";
    case Preserve_trx_promotion_adopt_status::NOT_STANDBY_PENDING:
      return "NOT_STANDBY_PENDING";
    case Preserve_trx_promotion_adopt_status::EPOCH_NOT_COMMITTED:
      return "EPOCH_NOT_COMMITTED";
    case Preserve_trx_promotion_adopt_status::APPLY_BARRIER_NOT_REACHED:
      return "APPLY_BARRIER_NOT_REACHED";
    case Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY:
      return "READY_CACHE_NOT_READY";
    case Preserve_trx_promotion_adopt_status::TOO_MANY_PROMOTION_TOKENS:
      return "TOO_MANY_PROMOTION_TOKENS";
    case Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT:
      return "CORRUPT_ARTIFACT";
    case Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT:
      return "UNSUPPORTED_ARTIFACT";
    case Preserve_trx_promotion_adopt_status::CLAIMED_IMPORT_FAILED:
      return "CLAIMED_IMPORT_FAILED";
    case Preserve_trx_promotion_adopt_status::TOKEN_ABANDONED:
      return "TOKEN_ABANDONED";
    case Preserve_trx_promotion_adopt_status::CLEANUP_PENDING:
      return "CLEANUP_PENDING";
    case Preserve_trx_promotion_adopt_status::CLEANUP_TAINTED:
      return "CLEANUP_TAINTED";
  }
  return "UNKNOWN";
}

uint64_t preserve_trx_promotion_gate_elapsed_us_status() {
  return g_promotion_gate_elapsed_us.load();
}

uint64_t preserve_trx_promotion_gate_token_count_status() {
  return g_promotion_gate_token_count.load();
}

uint64_t preserve_trx_promotion_gate_adopted_count_status() {
  return g_promotion_gate_adopted_count.load();
}

uint64_t preserve_trx_promotion_gate_abandoned_count_status() {
  return g_promotion_gate_abandoned_count.load();
}

uint64_t preserve_trx_promotion_gate_skipped_count_status() {
  return g_promotion_gate_skipped_count.load();
}

uint64_t preserve_trx_promotion_gate_max_worker_elapsed_us_status() {
  return g_promotion_gate_max_worker_elapsed_us.load();
}

uint64_t preserve_trx_promotion_gate_p50_worker_elapsed_us_status() {
  return g_promotion_gate_p50_worker_elapsed_us.load();
}

uint64_t preserve_trx_promotion_gate_p95_worker_elapsed_us_status() {
  return g_promotion_gate_p95_worker_elapsed_us.load();
}

uint64_t preserve_trx_promotion_gate_status_code_status() {
  return g_promotion_gate_status_code.load();
}

uint64_t preserve_trx_promotion_gate_record_lock_page_count_status() {
  return g_promotion_gate_record_lock_page_count.load();
}

uint64_t preserve_trx_promotion_gate_record_lock_resident_pages_status() {
  return g_promotion_gate_record_lock_resident_pages.load();
}

uint64_t preserve_trx_promotion_gate_record_lock_cold_page_gets_status() {
  return g_promotion_gate_record_lock_cold_page_gets.load();
}

uint64_t preserve_trx_promotion_gate_ready_cache_miss_count_status() {
  return g_promotion_gate_ready_cache_miss_count.load();
}

uint64_t preserve_trx_promotion_gate_over_budget_count_status() {
  return g_promotion_gate_over_budget_count.load();
}

uint64_t preserve_trx_promotion_prewarm_record_lock_page_count_status() {
  uint64_t page_count = 0;
  std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
  for (const auto &item : g_ready_cache) {
    const Promotion_ready_cache_entry &entry = item.second;
    if (ready_cache_entry_needs_record_lock_prewarm(entry)) {
      page_count += entry.record_lock_page_count;
    }
  }
  return page_count;
}

uint64_t preserve_trx_promotion_prewarm_record_lock_resident_pages_status() {
  uint64_t resident_pages = 0;
  std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
  for (const auto &item : g_ready_cache) {
    const Promotion_ready_cache_entry &entry = item.second;
    if (ready_cache_entry_needs_record_lock_prewarm(entry)) {
      resident_pages += entry.record_lock_resident_pages;
    }
  }
  return resident_pages;
}

uint64_t preserve_trx_promotion_prewarm_record_lock_cold_page_gets_status() {
  uint64_t cold_page_gets = 0;
  std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
  for (const auto &item : g_ready_cache) {
    const Promotion_ready_cache_entry &entry = item.second;
    if (ready_cache_entry_needs_record_lock_prewarm(entry) &&
        !entry.record_lock_metadata_only &&
        entry.record_lock_page_count > entry.record_lock_resident_pages) {
      cold_page_gets +=
          entry.record_lock_page_count - entry.record_lock_resident_pages;
    }
  }
  return cold_page_gets;
}

uint64_t preserve_trx_promotion_ready_cache_bytes_status() {
  return g_ready_cache_bytes.load();
}

uint64_t preserve_trx_promotion_ready_cache_evictions_status() {
  return g_ready_cache_evictions.load();
}

void preserved_trx_promotion_ready_cache_purge_epoch(
    const std::string &preserve_dir, const std::string &epoch_id) {
  if (preserve_dir.empty() || epoch_id.empty()) return;
  std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
  uint64_t current_bytes = g_ready_cache_bytes.load();
  for (auto entry = g_ready_cache.begin(); entry != g_ready_cache.end();) {
    if (entry->first.preserve_dir != preserve_dir ||
        entry->first.epoch_id != epoch_id) {
      ++entry;
      continue;
    }
    current_bytes =
        current_bytes >= entry->second.estimated_bytes
            ? current_bytes - entry->second.estimated_bytes
            : 0;
    entry = g_ready_cache.erase(entry);
  }
  g_ready_cache_bytes.store(current_bytes);
}

void preserved_trx_set_promotion_apply_state_provider(
    Preserve_trx_promotion_apply_state_provider provider) {
  g_apply_state_provider = provider;
}

void preserved_trx_set_promotion_apply_state_provider_for_unit_test(
    Preserve_trx_promotion_apply_state_provider provider) {
  preserved_trx_set_promotion_apply_state_provider(provider);
}

void preserved_trx_set_promotion_adopt_executor_for_unit_test(
    Preserve_trx_promotion_adopt_executor executor) {
  g_adopt_executor = executor;
}

void preserved_trx_promotion_ready_cache_clear_for_unit_test() {
  std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
  g_ready_cache.clear();
  g_ready_cache_bytes.store(0);
}

void preserved_trx_promotion_ready_cache_put_for_unit_test(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, Preserve_trx_promotion_ready_state state,
    uint64_t required_apply_lsn) {
  if (preserve_dir.empty() || epoch_id.empty() || token == 0) return;
  std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
  Promotion_ready_cache_entry entry;
  entry.state = state;
  entry.required_apply_lsn = required_apply_lsn;
  ready_cache_store_locked(
      Promotion_ready_cache_key{preserve_dir, epoch_id, token},
      std::move(entry));
}

void preserved_trx_promotion_ready_cache_put_bundle_for_unit_test(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, Preserve_trx_promotion_ready_state state,
    uint64_t required_apply_lsn, const Preserved_trx_bundle &ready_bundle) {
  if (preserve_dir.empty() || epoch_id.empty() || token == 0) return;
  std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
  Promotion_ready_cache_entry entry;
  entry.state = state;
  entry.required_apply_lsn = required_apply_lsn;
  entry.has_ready_bundle = true;
  entry.ready_bundle = ready_bundle;
  ready_cache_store_locked(
      Promotion_ready_cache_key{preserve_dir, epoch_id, token},
      std::move(entry));
}

void
preserved_trx_promotion_ready_cache_put_bundle_with_record_lock_proof_for_unit_test(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, Preserve_trx_promotion_ready_state state,
    uint64_t required_apply_lsn, const Preserved_trx_bundle &ready_bundle,
    uint64_t record_lock_page_count, uint64_t record_lock_bitmap_pages,
    uint64_t record_lock_bitmap_bits) {
  preserved_trx_promotion_ready_cache_put_bundle_with_record_lock_residency_for_unit_test(
      preserve_dir, epoch_id, token, state, required_apply_lsn, ready_bundle,
      record_lock_page_count, record_lock_page_count, record_lock_page_count,
      record_lock_bitmap_pages, record_lock_bitmap_bits);
}

void
preserved_trx_promotion_ready_cache_put_bundle_with_record_lock_residency_for_unit_test(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, Preserve_trx_promotion_ready_state state,
    uint64_t required_apply_lsn, const Preserved_trx_bundle &ready_bundle,
    uint64_t record_lock_page_count,
    uint64_t record_lock_prefetch_submitted_pages,
    uint64_t record_lock_resident_pages, uint64_t record_lock_bitmap_pages,
    uint64_t record_lock_bitmap_bits) {
  if (preserve_dir.empty() || epoch_id.empty() || token == 0) return;
  std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
  Promotion_ready_cache_entry entry;
  entry.state = state;
  entry.required_apply_lsn = required_apply_lsn;
  entry.has_ready_bundle = true;
  entry.ready_bundle = ready_bundle;
  entry.record_lock_pages_prewarmed = true;
  entry.record_lock_page_count = record_lock_page_count;
  entry.record_lock_prefetch_submitted_pages =
      record_lock_prefetch_submitted_pages;
  entry.record_lock_resident_pages = record_lock_resident_pages;
  entry.record_lock_bitmap_pages = record_lock_bitmap_pages;
  entry.record_lock_bitmap_bits = record_lock_bitmap_bits;
  ready_cache_store_locked(
      Promotion_ready_cache_key{preserve_dir, epoch_id, token},
      std::move(entry));
}

bool preserved_trx_promotion_record_lock_pages_gate_ready_for_unit_test(
    bool record_lock_pages_prewarmed, uint64_t record_lock_page_count,
    uint64_t record_lock_prefetch_submitted_pages,
    uint64_t record_lock_resident_pages) {
  return record_lock_pages_are_gate_ready(
      record_lock_pages_prewarmed, record_lock_page_count,
      record_lock_prefetch_submitted_pages, record_lock_resident_pages);
}

bool preserved_trx_promotion_record_lock_pages_wait_for_residency_for_unit_test(
    bool record_lock_pages_prewarmed, uint64_t record_lock_page_count,
    uint64_t record_lock_prefetch_submitted_pages,
    const std::vector<uint64_t> &resident_page_samples,
    uint64_t *final_resident_pages, uint64_t *sample_count) {
  size_t sample_index = 0;
  trx_preserve_record_lock_residency_t final_residency;
  const uint64_t deadline_us =
      my_micro_time() +
      std::max<uint64_t>(1, resident_page_samples.size()) *
          kRecordLockResidencyPollIntervalUs * 4;
  const bool ready = wait_for_record_lock_pages_to_be_gate_ready(
      record_lock_pages_prewarmed, record_lock_page_count,
      record_lock_prefetch_submitted_pages, deadline_us,
      [&](trx_preserve_record_lock_residency_t *residency) {
        if (sample_index >= resident_page_samples.size()) return false;
        *residency = trx_preserve_record_lock_residency_t{};
        residency->page_count = record_lock_page_count;
        residency->resident_pages = resident_page_samples[sample_index++];
        return true;
      },
      &final_residency, sample_count);
  if (final_resident_pages != nullptr) {
    *final_resident_pages = final_residency.resident_pages;
  }
  return ready;
}

Preserve_trx_promotion_adopt_status prewarm_loaded_bundle_into_ready_cache(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, uint64_t required_apply_lsn,
    const Preserved_trx_bundle &bundle, bool wait_for_final_epoch_fact,
    const Promotion_record_lock_proof *record_lock_proof) {
  const std::string token_string = std::to_string(token);
  std::string dry_validate_reason;
  const Preserve_snapshot_status dry_validate_status =
      preserved_trx_dry_validate_loaded_bundle(preserve_dir, token_string,
                                               bundle, &dry_validate_reason);
  if (dry_validate_status != Preserve_snapshot_status::OK) {
    if (dry_validate_status == Preserve_snapshot_status::CORRUPT ||
        dry_validate_status == Preserve_snapshot_status::UNSUPPORTED) {
      std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
      Promotion_ready_cache_entry entry;
      entry.state = Preserve_trx_promotion_ready_state::CORRUPT;
      entry.required_apply_lsn = required_apply_lsn;
      entry.reason =
          dry_validate_reason.empty()
              ? std::string("dry validate failed: ") +
                    preserve_snapshot_status_name(dry_validate_status)
              : std::string("dry validate failed: ") + dry_validate_reason;
      ready_cache_store_locked(
          Promotion_ready_cache_key{preserve_dir, epoch_id, token},
          std::move(entry));
    }
    return carrier_status_to_promotion_status(dry_validate_status);
  }

  Promotion_ready_cache_entry entry;
  entry.state = wait_for_final_epoch_fact
                    ? Preserve_trx_promotion_ready_state::
                          PREWARMED_PENDING_FINAL_FACT
                    : Preserve_trx_promotion_ready_state::READY;
  entry.required_apply_lsn = required_apply_lsn;
  entry.has_ready_bundle = true;
  entry.ready_bundle = bundle;
  const std::string *record_lock_payload = nullptr;
  std::string record_lock_payload_reason;
  if (!select_record_lock_payload_for_prewarm(bundle, &record_lock_payload,
                                              &record_lock_payload_reason)) {
    entry.state = Preserve_trx_promotion_ready_state::CORRUPT;
    entry.has_ready_bundle = false;
    entry.reason =
        record_lock_payload_reason.empty()
            ? "record-lock payload selection failed"
            : record_lock_payload_reason;
    std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
    ready_cache_store_locked(
        Promotion_ready_cache_key{preserve_dir, epoch_id, token},
        std::move(entry));
    return Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
  }
  if (record_lock_payload == nullptr || record_lock_payload->empty()) {
    entry.record_lock_pages_prewarmed = true;
  } else if (record_lock_proof != nullptr &&
             record_lock_proof->available) {
    entry.record_lock_metadata_only = record_lock_proof->metadata_only;
    entry.record_lock_pages_prewarmed = !record_lock_proof->metadata_only;
    entry.record_lock_page_count = record_lock_proof->page_count;
    entry.record_lock_prefetch_submitted_pages =
        record_lock_proof->metadata_only ? 0 : record_lock_proof->page_count;
    entry.record_lock_resident_pages = record_lock_proof->resident_pages;
    entry.record_lock_bitmap_pages = record_lock_proof->bitmap_pages;
    entry.record_lock_bitmap_bits = record_lock_proof->bitmap_bits;
    if (!record_lock_proof->metadata_only &&
        (record_lock_proof->cold_gets != 0 ||
         !record_lock_pages_are_gate_ready(
             entry.record_lock_pages_prewarmed, entry.record_lock_page_count,
             entry.record_lock_prefetch_submitted_pages,
             entry.record_lock_resident_pages))) {
      entry.state = Preserve_trx_promotion_ready_state::APPLY_PENDING;
      entry.reason = "record-lock object proof is not resident";
      std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
      ready_cache_store_locked(
          Promotion_ready_cache_key{preserve_dir, epoch_id, token},
          std::move(entry));
      return Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY;
    }
  } else {
    trx_preserve_record_lock_page_plan_t page_plan;
    if (!trx_preserve_record_lock_payload_page_plan(
            *record_lock_payload, &page_plan)) {
      entry.state = Preserve_trx_promotion_ready_state::CORRUPT;
      entry.has_ready_bundle = false;
      entry.reason = "record-lock page plan failed";
      std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
      ready_cache_store_locked(
          Promotion_ready_cache_key{preserve_dir, epoch_id, token},
          std::move(entry));
      return Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
    }
    trx_preserve_record_lock_import_metrics_t prefetch_metrics;
    const dberr_t prefetch_status =
        trx_preserve_prefetch_record_lock_pages_for_gate(*record_lock_payload,
                                                         &prefetch_metrics);
    if (prefetch_status != DB_SUCCESS) {
      if (prefetch_status == DB_TABLE_NOT_FOUND) {
        entry.state = Preserve_trx_promotion_ready_state::APPLY_PENDING;
        entry.record_lock_page_count = page_plan.page_count;
        entry.record_lock_bitmap_pages = page_plan.bitmap_pages;
        entry.record_lock_bitmap_bits = page_plan.bitmap_bits;
        entry.reason = trx_preserve_last_record_lock_export_error();
        if (entry.reason.empty()) {
          entry.reason = "record-lock page prefetch not ready";
        }
        std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
        ready_cache_store_locked(
            Promotion_ready_cache_key{preserve_dir, epoch_id, token},
            std::move(entry));
        return Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY;
      }
      entry.state = Preserve_trx_promotion_ready_state::CORRUPT;
      entry.has_ready_bundle = false;
      entry.reason = trx_preserve_last_record_lock_export_error();
      if (entry.reason.empty()) {
        entry.reason = "record-lock page prefetch failed";
      }
      std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
      ready_cache_store_locked(
          Promotion_ready_cache_key{preserve_dir, epoch_id, token},
          std::move(entry));
      return Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
    }
    entry.record_lock_pages_prewarmed = true;
    entry.record_lock_page_count = page_plan.page_count;
    entry.record_lock_prefetch_submitted_pages = prefetch_metrics.prefetch_pages;
    entry.record_lock_bitmap_pages = page_plan.bitmap_pages;
    entry.record_lock_bitmap_bits = page_plan.bitmap_bits;
    trx_preserve_record_lock_residency_t residency;
    bool residency_parse_failed = false;
    const uint64_t residency_deadline_us =
        my_micro_time() +
        static_cast<uint64_t>(preserve_trx_promotion_gate_timeout_ms) * 1000;
    const bool record_lock_pages_ready =
        wait_for_record_lock_pages_to_be_gate_ready(
            entry.record_lock_pages_prewarmed, entry.record_lock_page_count,
            entry.record_lock_prefetch_submitted_pages, residency_deadline_us,
            [&](trx_preserve_record_lock_residency_t *sample) {
              if (trx_preserve_record_lock_payload_residency(
                      *record_lock_payload, sample)) {
                return true;
              }
              residency_parse_failed = true;
              return false;
            },
            &residency, nullptr);
    if (residency_parse_failed) {
      entry.state = Preserve_trx_promotion_ready_state::CORRUPT;
      entry.has_ready_bundle = false;
      entry.reason = "record-lock page residency parse failed";
      std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
      ready_cache_store_locked(
          Promotion_ready_cache_key{preserve_dir, epoch_id, token},
          std::move(entry));
      return Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
    }
    entry.record_lock_resident_pages = residency.resident_pages;
    entry.record_lock_io_pending_pages = residency.io_pending_pages;
    entry.record_lock_missing_pages = residency.missing_pages;
    if (!record_lock_pages_ready) {
      entry.state = Preserve_trx_promotion_ready_state::APPLY_PENDING;
      entry.reason = "record-lock pages are not resident";
      std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
      ready_cache_store_locked(
          Promotion_ready_cache_key{preserve_dir, epoch_id, token},
          std::move(entry));
      return Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY;
    }
  }

  /*
    A ready record is a cache of facts that were checked before promotion.
    When an epoch fact exists, bind the cache entry to its digest so the gate
    can reject stale prewarm results if the accepted epoch fact is rewritten.
  */
  if (!wait_for_final_epoch_fact &&
      promotion_epoch_fact_file_exists(preserve_dir, epoch_id)) {
    Preserve_trx_transfer_epoch_fact fact;
    const Preserve_trx_transfer_status fact_status =
        preserve_trx_transfer_read_epoch_fact(preserve_dir, epoch_id, &fact);
    if (fact_status == Preserve_trx_transfer_status::OK) {
      bool token_found = false;
      for (const Preserve_trx_transfer_epoch_fact_token &fact_token :
           fact.tokens) {
        if (fact_token.token == token) {
          token_found = true;
          break;
        }
      }
      if (!token_found) {
        entry.state = Preserve_trx_promotion_ready_state::CORRUPT;
        entry.has_ready_bundle = false;
        entry.reason = "epoch fact does not contain token";
        std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
        ready_cache_store_locked(
            Promotion_ready_cache_key{preserve_dir, epoch_id, token},
            std::move(entry));
        return Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
      }
      entry.state = Preserve_trx_promotion_ready_state::READY;
      entry.has_epoch_fact = true;
      entry.epoch_fact_digest = fact.fact_digest;
    } else {
      entry.state = Preserve_trx_promotion_ready_state::CORRUPT;
      entry.has_ready_bundle = false;
      entry.reason = "epoch fact read failed";
      std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
      ready_cache_store_locked(
          Promotion_ready_cache_key{preserve_dir, epoch_id, token},
          std::move(entry));
      return Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
    }
  }

  {
    std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
    ready_cache_store_locked(
        Promotion_ready_cache_key{preserve_dir, epoch_id, token},
        std::move(entry));
  }
  return Preserve_trx_promotion_adopt_status::OK;
}

Preserve_trx_promotion_adopt_status
preserved_trx_promotion_prewarm_standby_pending_token(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, uint64_t required_apply_lsn) {
  if (preserve_dir.empty() || epoch_id.empty() || token == 0) {
    return Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT;
  }
  if (!preserve_trx_is_enabled()) {
    return Preserve_trx_promotion_adopt_status::NOT_ENABLED;
  }

  auto store = create_preserved_trx_default_store(preserve_dir);
  Preserved_trx_carrier_listing listing;
  const Preserve_snapshot_status list_status = store->list_tokens(&listing);
  if (list_status != Preserve_snapshot_status::OK) {
    return carrier_status_to_promotion_status(list_status);
  }

  const std::string token_string = std::to_string(token);
  if (listing.standby_pending_tokens.count(token_string) == 0) {
    return listing.snapshot_tokens.count(token_string) != 0
               ? Preserve_trx_promotion_adopt_status::NOT_STANDBY_PENDING
               : Preserve_trx_promotion_adopt_status::TOKEN_NOT_FOUND;
  }

  Preserved_trx_bundle bundle;
  const Preserve_snapshot_status read_status =
      preserved_trx_load_bundle_for_recover_or_prewarm(
          preserve_dir, token_string,
          Preserved_trx_recover_load_profile::WITH_SEMANTIC_EXTERNAL_BLOBS,
          &bundle);
  if (read_status != Preserve_snapshot_status::OK) {
    if (read_status == Preserve_snapshot_status::NOT_FOUND ||
        read_status == Preserve_snapshot_status::CORRUPT ||
        read_status == Preserve_snapshot_status::UNSUPPORTED) {
      std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
      Promotion_ready_cache_entry entry;
      entry.state = Preserve_trx_promotion_ready_state::CORRUPT;
      entry.required_apply_lsn = required_apply_lsn;
      entry.reason = std::string("bundle load failed: ") +
                     preserve_snapshot_status_name(read_status);
      ready_cache_store_locked(
          Promotion_ready_cache_key{preserve_dir, epoch_id, token},
          std::move(entry));
    }
    if (read_status == Preserve_snapshot_status::NOT_FOUND) {
      return Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
    }
    return carrier_status_to_promotion_status(read_status);
  }

  return prewarm_loaded_bundle_into_ready_cache(
      preserve_dir, epoch_id, token, required_apply_lsn, bundle, false,
      nullptr);
}

Preserve_trx_promotion_adopt_status
preserved_trx_promotion_prewarm_staged_bundle_for_receiver(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, uint64_t required_apply_lsn,
    const Preserved_trx_bundle &bundle) {
  if (preserve_dir.empty() || epoch_id.empty() || token == 0) {
    return Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT;
  }
  if (!preserve_trx_is_enabled()) {
    return Preserve_trx_promotion_adopt_status::NOT_ENABLED;
  }
  return prewarm_loaded_bundle_into_ready_cache(
      preserve_dir, epoch_id, token, required_apply_lsn, bundle, true, nullptr);
}

Preserve_trx_promotion_adopt_status
preserved_trx_promotion_prewarm_staged_bundle_with_record_lock_proof_for_receiver(
    const std::string &preserve_dir, const std::string &epoch_id,
    uint64_t token, uint64_t required_apply_lsn,
    const Preserved_trx_bundle &bundle, uint64_t record_lock_page_count,
    uint64_t record_lock_resident_pages, uint64_t record_lock_cold_gets,
    uint64_t record_lock_bitmap_pages, uint64_t record_lock_bitmap_bits,
    bool metadata_only) {
  if (preserve_dir.empty() || epoch_id.empty() || token == 0) {
    return Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT;
  }
  if (!preserve_trx_is_enabled()) {
    return Preserve_trx_promotion_adopt_status::NOT_ENABLED;
  }
  Promotion_record_lock_proof proof;
  proof.available = true;
  proof.metadata_only = metadata_only;
  proof.page_count = record_lock_page_count;
  proof.resident_pages = record_lock_resident_pages;
  proof.cold_gets = record_lock_cold_gets;
  proof.bitmap_pages = record_lock_bitmap_pages;
  proof.bitmap_bits = record_lock_bitmap_bits;
  return prewarm_loaded_bundle_into_ready_cache(
      preserve_dir, epoch_id, token, required_apply_lsn, bundle, true, &proof);
}

Preserve_trx_promotion_adopt_status
preserved_trx_promotion_bind_prewarmed_epoch_for_receiver(
    const std::string &preserve_dir, const std::string &epoch_id,
    const std::vector<uint64_t> &tokens, uint64_t *ready_tokens) {
  if (ready_tokens != nullptr) *ready_tokens = 0;
  if (preserve_dir.empty() || epoch_id.empty() || tokens.empty()) {
    return Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT;
  }
  if (!preserve_trx_is_enabled()) {
    return Preserve_trx_promotion_adopt_status::NOT_ENABLED;
  }

  Preserve_trx_transfer_epoch_fact fact;
  const Preserve_trx_transfer_status fact_status =
      preserve_trx_transfer_read_epoch_fact(preserve_dir, epoch_id, &fact);
  if (fact_status != Preserve_trx_transfer_status::OK) {
    return Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
  }
  return preserved_trx_promotion_bind_prewarmed_epoch_fact_for_receiver(
      preserve_dir, fact, tokens, ready_tokens);
}

Preserve_trx_promotion_adopt_status
preserved_trx_promotion_bind_prewarmed_epoch_fact_for_receiver(
    const std::string &preserve_dir,
    const Preserve_trx_transfer_epoch_fact &fact,
    const std::vector<uint64_t> &tokens, uint64_t *ready_tokens) {
  if (ready_tokens != nullptr) *ready_tokens = 0;
  if (preserve_dir.empty() || fact.epoch_id.empty() || tokens.empty()) {
    return Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT;
  }
  if (!preserve_trx_is_enabled()) {
    return Preserve_trx_promotion_adopt_status::NOT_ENABLED;
  }
  std::map<uint64_t, uint64_t> fact_lsn_by_token;
  for (const Preserve_trx_transfer_epoch_fact_token &fact_token :
       fact.tokens) {
    fact_lsn_by_token.emplace(fact_token.token,
                              fact_token.source_epoch_commit_lsn);
  }

  Preserve_trx_promotion_adopt_status first_failure =
      Preserve_trx_promotion_adopt_status::OK;
  uint64_t local_ready_tokens = 0;
  std::lock_guard<std::mutex> guard(g_ready_cache_mutex);
  for (uint64_t token : tokens) {
    if (token == 0 || fact_lsn_by_token.count(token) == 0) {
      first_failure = Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT;
      continue;
    }
    const auto key =
        Promotion_ready_cache_key{preserve_dir, fact.epoch_id, token};
    auto entry_it = g_ready_cache.find(key);
    if (entry_it == g_ready_cache.end()) {
      if (first_failure == Preserve_trx_promotion_adopt_status::OK) {
        first_failure =
            Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY;
      }
      continue;
    }
    Promotion_ready_cache_entry &entry = entry_it->second;
    if (entry.state == Preserve_trx_promotion_ready_state::READY) {
      ++local_ready_tokens;
      continue;
    }
    if (entry.state != Preserve_trx_promotion_ready_state::
                           PREWARMED_PENDING_FINAL_FACT) {
      if (first_failure == Preserve_trx_promotion_adopt_status::OK) {
        first_failure =
            entry.state == Preserve_trx_promotion_ready_state::CORRUPT
                ? Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT
                : Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY;
      }
      continue;
    }
    if (!entry.record_lock_metadata_only &&
        !record_lock_pages_are_gate_ready(
            entry.record_lock_pages_prewarmed, entry.record_lock_page_count,
            entry.record_lock_prefetch_submitted_pages,
            entry.record_lock_resident_pages)) {
      entry.state = Preserve_trx_promotion_ready_state::APPLY_PENDING;
      entry.reason = "record-lock pages are not resident";
      if (first_failure == Preserve_trx_promotion_adopt_status::OK) {
        first_failure =
            Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY;
      }
      continue;
    }
    entry.state = Preserve_trx_promotion_ready_state::READY;
    entry.has_epoch_fact = true;
    entry.epoch_fact_digest = fact.fact_digest;
    entry.required_apply_lsn = fact_lsn_by_token[token];
    ++local_ready_tokens;
  }
  if (ready_tokens != nullptr) *ready_tokens = local_ready_tokens;
  return local_ready_tokens == tokens.size()
             ? Preserve_trx_promotion_adopt_status::OK
             : first_failure;
}

Preserve_trx_promotion_adopt_status
preserved_trx_promotion_prewarm_standby_pending_tokens(
    const std::string &preserve_dir, const std::string &epoch_id,
    const std::vector<uint64_t> &tokens, uint64_t required_apply_lsn,
    uint worker_count, Preserve_trx_promotion_adopt_result *result) {
  if (result == nullptr || preserve_dir.empty() || epoch_id.empty() ||
      tokens.empty() || worker_count == 0) {
    return Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT;
  }
  if (!preserve_trx_is_enabled()) {
    return Preserve_trx_promotion_adopt_status::NOT_ENABLED;
  }

  const uint64_t started_us = my_micro_time();
  *result = {};
  result->seen_tokens = tokens;
  result->token_results.resize(tokens.size());

  std::atomic<size_t> next_token{0};
  std::atomic<bool> workers_released{false};
  std::atomic<bool> worker_abort{false};
  std::atomic<uint64_t> max_worker_elapsed_us{0};
  const size_t actual_workers =
      std::min<size_t>(std::max<uint>(1, worker_count), tokens.size());

  auto update_max_worker_elapsed = [&](uint64_t elapsed_us) {
    uint64_t current = max_worker_elapsed_us.load();
    while (elapsed_us > current &&
           !max_worker_elapsed_us.compare_exchange_weak(current, elapsed_us)) {
    }
  };

  auto run_worker = [&]() {
    while (!workers_released.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    for (;;) {
      if (worker_abort.load(std::memory_order_acquire)) break;
      const size_t token_index = next_token.fetch_add(1);
      if (token_index >= tokens.size()) break;

      const uint64_t token = tokens[token_index];
      Preserve_trx_promotion_token_result token_result;
      token_result.token = token;
      const uint64_t token_started_us = my_micro_time();
      try {
        token_result.status =
            preserved_trx_promotion_prewarm_standby_pending_token(
                preserve_dir, epoch_id, token, required_apply_lsn);
        token_result.reason =
            token_result.status == Preserve_trx_promotion_adopt_status::OK
                ? "prewarmed"
                : ready_cache_reason_or_status(preserve_dir, epoch_id, token,
                                               token_result.status);
      } catch (...) {
        token_result.status =
            Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT;
        token_result.reason = "promotion prewarm worker failed";
        worker_abort.store(true, std::memory_order_release);
      }
      result->token_results[token_index] = std::move(token_result);
      update_max_worker_elapsed(my_micro_time() - token_started_us);
    }
  };

  std::vector<std::thread> workers;
  auto join_workers = create_scope_guard([&] {
    for (std::thread &worker : workers) {
      if (worker.joinable()) worker.join();
    }
  });
  bool worker_creation_failed = false;
  try {
    workers.reserve(actual_workers);
    for (size_t i = 0; i < actual_workers; ++i) {
      workers.emplace_back(run_worker);
    }
  } catch (...) {
    worker_creation_failed = true;
    worker_abort.store(true, std::memory_order_release);
  }
  workers_released.store(true, std::memory_order_release);
  join_workers.rollback();
  if (worker_abort.load(std::memory_order_acquire)) {
    for (size_t i = 0; i < result->token_results.size(); ++i) {
      Preserve_trx_promotion_token_result &token_result =
          result->token_results[i];
      if (token_result.token != 0) continue;
      token_result.token = tokens[i];
      token_result.status =
          Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT;
      token_result.reason = worker_creation_failed
                                ? "promotion prewarm worker creation failed"
                                : "promotion prewarm worker aborted";
    }
  }

  Preserve_trx_promotion_adopt_status first_failure =
      Preserve_trx_promotion_adopt_status::OK;
  for (const Preserve_trx_promotion_token_result &token_result :
       result->token_results) {
    if (token_result.status == Preserve_trx_promotion_adopt_status::OK) {
      ++result->skipped_count;
      continue;
    }
    std::string message = "PRESERVE: promotion prewarm token=";
    message.append(std::to_string(token_result.token));
    message.append(" status=");
    message.append(preserve_trx_promotion_adopt_status_name(
        token_result.status));
    message.append(" reason=");
    message.append(token_result.reason);
    Promotion_ready_cache_entry cache_entry;
    if (ready_cache_lookup(preserve_dir, epoch_id, token_result.token,
                           &cache_entry)) {
      message.append(" record_lock_page_count=");
      message.append(std::to_string(cache_entry.record_lock_page_count));
      message.append(" record_lock_prefetch_submitted_pages=");
      message.append(
          std::to_string(cache_entry.record_lock_prefetch_submitted_pages));
      message.append(" record_lock_resident_pages=");
      message.append(std::to_string(cache_entry.record_lock_resident_pages));
      message.append(" record_lock_io_pending_pages=");
      message.append(std::to_string(cache_entry.record_lock_io_pending_pages));
      message.append(" record_lock_missing_pages=");
      message.append(std::to_string(cache_entry.record_lock_missing_pages));
    }
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, message.c_str());
    ++result->failed_count;
    if (first_failure == Preserve_trx_promotion_adopt_status::OK) {
      first_failure = token_result.status;
    }
  }
  result->elapsed_us = my_micro_time() - started_us;
  result->max_worker_elapsed_us = max_worker_elapsed_us.load();
  result->status = first_failure;
  if (first_failure == Preserve_trx_promotion_adopt_status::OK) {
    result->message = "prewarmed";
    return Preserve_trx_promotion_adopt_status::OK;
  }
  result->message = "prewarm failed";
  return first_failure;
}

Preserve_trx_promotion_adopt_status
preserved_trx_promotion_ready_summary_for_epoch(
    const std::string &preserve_dir, const std::string &epoch_id,
    Preserve_trx_promotion_ready_summary *summary) {
  if (preserve_dir.empty() || epoch_id.empty() || summary == nullptr) {
    return Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT;
  }
  *summary = {};
  summary->epoch_id = epoch_id;
  if (!preserve_trx_is_enabled()) {
    return Preserve_trx_promotion_adopt_status::NOT_ENABLED;
  }

  auto store = create_preserved_trx_default_store(preserve_dir);
  Preserved_trx_carrier_listing listing;
  const Preserve_snapshot_status list_status = store->list_tokens(&listing);
  if (list_status != Preserve_snapshot_status::OK) {
    return carrier_status_to_promotion_status(list_status);
  }
  std::set<uint64_t> reported_tokens;
  for (const std::string &token_string : listing.standby_pending_tokens) {
    uint64_t token = 0;
    if (!parse_uint64_strict(token_string, &token) || token == 0) {
      summary->corrupt_tokens.push_back(token);
      append_ready_summary_token_result(
          summary, token, Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
          "non-numeric standby-pending token");
      continue;
    }
    reported_tokens.insert(token);
    Promotion_ready_cache_entry cache_entry;
    if (!ready_cache_lookup(preserve_dir, epoch_id, token, &cache_entry)) {
      summary->pending_tokens.push_back(token);
      append_ready_summary_token_result(
          summary, token,
          Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
          "promotion-ready cache not built");
      continue;
    }
    if (cache_entry.state == Preserve_trx_promotion_ready_state::READY) {
      summary->ready_tokens.push_back(token);
      summary->max_required_apply_lsn =
          std::max(summary->max_required_apply_lsn,
                   cache_entry.required_apply_lsn);
    } else if (cache_entry.state ==
               Preserve_trx_promotion_ready_state::CORRUPT) {
      summary->corrupt_tokens.push_back(token);
      append_ready_summary_token_result(
          summary, token, Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
          cache_entry.reason.empty() ? "promotion-ready cache is corrupt"
                                     : cache_entry.reason);
    } else {
      summary->pending_tokens.push_back(token);
      summary->max_required_apply_lsn =
          std::max(summary->max_required_apply_lsn,
                   cache_entry.required_apply_lsn);
      append_ready_summary_token_result(
          summary, token,
          Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
          cache_entry.reason.empty() ? "promotion-ready cache not ready"
                                     : cache_entry.reason);
    }
  }
  for (const auto &cache_item :
       ready_cache_entries_for_epoch(preserve_dir, epoch_id)) {
    const uint64_t token = cache_item.first;
    const Promotion_ready_cache_entry &cache_entry = cache_item.second;
    if (reported_tokens.count(token) != 0) {
      continue;
    }
    if (cache_entry.state == Preserve_trx_promotion_ready_state::READY) {
      summary->ready_tokens.push_back(token);
      summary->max_required_apply_lsn =
          std::max(summary->max_required_apply_lsn,
                   cache_entry.required_apply_lsn);
    } else if (cache_entry.state == Preserve_trx_promotion_ready_state::CORRUPT) {
      summary->corrupt_tokens.push_back(token);
      append_ready_summary_token_result(
          summary, token, Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
          cache_entry.reason.empty() ? "promotion-ready cache is corrupt"
                                     : cache_entry.reason);
    } else {
      summary->pending_tokens.push_back(token);
      summary->max_required_apply_lsn =
          std::max(summary->max_required_apply_lsn,
                   cache_entry.required_apply_lsn);
      append_ready_summary_token_result(
          summary, token,
          Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
          cache_entry.reason.empty() ? "promotion-ready cache not ready"
                                     : cache_entry.reason);
    }
  }
  std::sort(summary->ready_tokens.begin(), summary->ready_tokens.end());
  std::sort(summary->pending_tokens.begin(), summary->pending_tokens.end());
  std::sort(summary->corrupt_tokens.begin(), summary->corrupt_tokens.end());
  if (summary->ready_tokens.empty() && summary->pending_tokens.empty() &&
      summary->corrupt_tokens.empty()) {
    summary->state = Preserve_trx_promotion_ready_state::NOT_FOUND;
    return Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY;
  }
  if (!summary->corrupt_tokens.empty()) {
    summary->state = Preserve_trx_promotion_ready_state::CORRUPT;
    return Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY;
  }
  if (summary->pending_tokens.empty()) {
    summary->state = Preserve_trx_promotion_ready_state::READY;
    return Preserve_trx_promotion_adopt_status::OK;
  }
  summary->state =
      Preserve_trx_promotion_ready_state::RECEIVED_CURRENT_PROCESS;
  return Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY;
}

Preserve_trx_promotion_adopt_status
preserved_trx_adopt_standby_pending_all_for_promotion(
    const std::string &preserve_dir,
    const Preserve_trx_promotion_adopt_all_request &request,
    Preserve_trx_promotion_adopt_result *result) {
  const uint64_t started_us = my_micro_time();
  if (result == nullptr) {
    return Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT;
  }
  *result = {};
  auto finish_gate_result = [&](Preserve_trx_promotion_adopt_status status) {
    finish_result(result, status, started_us);
    publish_promotion_gate_metrics(*result);
    return result->status;
  };
  if (preserve_dir.empty() || request.epoch_id.empty() ||
      request.worker_count == 0 || request.gate_timeout_ms == 0 ||
      request.gate_batch_tokens == 0) {
    return finish_gate_result(
        Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT);
  }
  if (request.execute_adopt && !request.require_promotion_ready_cache) {
    return finish_gate_result(
        Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT);
  }
  for (uint64_t token : request.tokens) {
    if (token == 0) {
      result->failed_count = 1;
      return finish_gate_result(
          Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT);
    }
  }
  if (!preserve_trx_is_enabled()) {
    return finish_gate_result(Preserve_trx_promotion_adopt_status::NOT_ENABLED);
  }

  auto store = create_preserved_trx_default_store(preserve_dir);
  std::vector<uint64_t> adopted_tokens;
  auto finish_with_optional_abandoned_marker =
      [&](Preserve_trx_promotion_adopt_status status) {
        if (request.execute_adopt) {
          if (!write_final_intent_marker_for_result(&store.store(), request,
                                                    adopted_tokens, result)) {
            taint_adopted_tokens_after_marker_failure(
                result, &adopted_tokens,
                "promotion intent marker not published after adopt");
            status =
                Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS;
          }
        }
        if (!adopted_tokens.empty() &&
            !write_adopted_marker_for_tokens(&store.store(), request,
                                             adopted_tokens, result)) {
          taint_adopted_tokens_after_marker_failure(
              result, &adopted_tokens,
              "promotion adopted marker not published after adopt");
          status =
              Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS;
          (void)write_final_intent_marker_for_result(&store.store(), request,
                                                     adopted_tokens, result);
        }
        if (request.execute_adopt && status ==
            Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS) {
          write_abandoned_marker_for_result(&store.store(), request, result);
        }
        return finish_gate_result(status);
      };
  Preserved_trx_carrier_listing listing;
  bool listing_loaded = false;
  auto load_listing = [&]() {
    if (listing_loaded) return Preserve_snapshot_status::OK;
    const Preserve_snapshot_status list_status = store->list_tokens(&listing);
    if (list_status == Preserve_snapshot_status::OK) listing_loaded = true;
    return list_status;
  };

  std::set<uint64_t> requested_tokens;
  bool requested_from_epoch_fact = false;
  if (request.tokens.empty()) {
    if (request.require_epoch_committed &&
        preserve_trx_transfer_epoch_committed(preserve_dir, request.epoch_id)) {
      Preserve_trx_transfer_epoch_fact fact;
      const Preserve_trx_transfer_status fact_status =
          preserve_trx_transfer_read_epoch_fact(preserve_dir, request.epoch_id,
                                                &fact);
      if (fact_status == Preserve_trx_transfer_status::OK) {
        for (const Preserve_trx_transfer_epoch_fact_token &fact_token :
             fact.tokens) {
          if (fact_token.token != 0) requested_tokens.insert(fact_token.token);
        }
        requested_from_epoch_fact = true;
      }
    }
    if (!requested_from_epoch_fact) {
      const Preserve_snapshot_status list_status = load_listing();
      if (list_status != Preserve_snapshot_status::OK) {
        return finish_gate_result(carrier_status_to_promotion_status(list_status));
      }
      for (const std::string &token_string : listing.standby_pending_tokens) {
        uint64_t token = 0;
        if (!parse_uint64_strict(token_string, &token) || token == 0) {
          add_abandoned_token(
              result, 0,
              Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
              "non-numeric standby-pending token",
              Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED);
          continue;
        }
        requested_tokens.insert(token);
      }
    }
  } else {
    requested_tokens.insert(request.tokens.begin(), request.tokens.end());
  }
  if (requested_tokens.size() > request.gate_batch_tokens) {
    result->failed_count = requested_tokens.size();
    append_result_message(result, "too_many_promotion_tokens");
    return finish_gate_result(
        Preserve_trx_promotion_adopt_status::TOO_MANY_PROMOTION_TOKENS);
  }
  if (requested_tokens.empty()) {
    if (result->abandoned_count > 0) {
      return finish_with_optional_abandoned_marker(
          Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS);
    }
    return finish_gate_result(
        Preserve_trx_promotion_adopt_status::TOKEN_NOT_FOUND);
  }

  const bool strict_online_epoch_gate =
      request.require_epoch_committed && request.require_promotion_ready_cache;
  for (uint64_t token : requested_tokens) {
    const std::string token_string = std::to_string(token);
    if (!strict_online_epoch_gate) {
      const Preserve_snapshot_status list_status = load_listing();
      if (list_status != Preserve_snapshot_status::OK) {
        return finish_gate_result(carrier_status_to_promotion_status(list_status));
      }
    }
    if (!strict_online_epoch_gate &&
        listing.standby_pending_tokens.count(token_string) == 0) {
      const Preserve_trx_promotion_adopt_status status =
          listing.snapshot_tokens.count(token_string) != 0
              ? Preserve_trx_promotion_adopt_status::NOT_STANDBY_PENDING
              : Preserve_trx_promotion_adopt_status::TOKEN_NOT_FOUND;
      add_abandoned_token(
          result, token, status,
          status == Preserve_trx_promotion_adopt_status::NOT_STANDBY_PENDING
              ? "token is local, not standby-pending"
              : "standby-pending token not found",
          Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING);
      continue;
    }
    result->seen_tokens.push_back(token);
  }
  std::sort(result->seen_tokens.begin(), result->seen_tokens.end());
  if (result->seen_tokens.empty()) {
    if (result->abandoned_count > 0) {
      return finish_with_optional_abandoned_marker(
          Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS);
    }
    return finish_gate_result(
        Preserve_trx_promotion_adopt_status::TOKEN_NOT_FOUND);
  }

  bool epoch_fact_verified = false;
  std::array<unsigned char, kPromotionDigestBytes> epoch_fact_digest{};
  if (request.require_epoch_committed) {
    if (!preserve_trx_transfer_epoch_committed(preserve_dir,
                                               request.epoch_id)) {
      for (uint64_t token : result->seen_tokens) {
        add_abandoned_token(
            result, token,
            Preserve_trx_promotion_adopt_status::EPOCH_NOT_COMMITTED,
            "promotion epoch commit marker not found",
            Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING);
      }
      return finish_with_optional_abandoned_marker(
          Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS);
    }
    Preserve_trx_transfer_epoch_fact fact;
    const Preserve_trx_transfer_status fact_status =
        preserve_trx_transfer_read_epoch_fact(preserve_dir, request.epoch_id,
                                              &fact);
    if (fact_status != Preserve_trx_transfer_status::OK) {
      for (uint64_t token : result->seen_tokens) {
        add_abandoned_token(
            result, token,
            Preserve_trx_promotion_adopt_status::EPOCH_NOT_COMMITTED,
            "promotion epoch transfer fact not readable",
            Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING);
      }
      return finish_with_optional_abandoned_marker(
          Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS);
    }
    std::set<uint64_t> fact_tokens;
    for (const Preserve_trx_transfer_epoch_fact_token &token : fact.tokens) {
      fact_tokens.insert(token.token);
    }
    for (uint64_t token : result->seen_tokens) {
      if (fact_tokens.count(token) == 0) {
        add_abandoned_token(
            result, token,
            Preserve_trx_promotion_adopt_status::EPOCH_NOT_COMMITTED,
            "promotion epoch transfer fact does not contain token",
            Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING);
      }
    }
    if (result->abandoned_count > 0) {
      return finish_with_optional_abandoned_marker(
          Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS);
    }
    epoch_fact_verified = true;
    epoch_fact_digest = fact.fact_digest;
  }
  if (request.require_apply_barrier) {
    uint64_t effective_required_apply_lsn = request.required_apply_lsn;
    if (request.require_promotion_ready_cache) {
      for (uint64_t token : result->seen_tokens) {
        Promotion_ready_cache_entry cache_entry;
        if (ready_cache_lookup(preserve_dir, request.epoch_id, token,
                               &cache_entry)) {
          effective_required_apply_lsn =
              std::max(effective_required_apply_lsn,
                       cache_entry.required_apply_lsn);
        }
      }
    }
    if (effective_required_apply_lsn == 0) {
      for (uint64_t token : result->seen_tokens) {
        add_abandoned_token(
            result, token,
            Preserve_trx_promotion_adopt_status::APPLY_BARRIER_NOT_REACHED,
            "apply barrier requires a non-zero LSN",
            Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING);
      }
      return finish_with_optional_abandoned_marker(
          Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS);
    }
    Preserve_trx_promotion_apply_state apply_state;
    bool apply_state_available = false;
    if (g_apply_state_provider != nullptr) {
      apply_state_available = g_apply_state_provider(&apply_state);
    }
    DBUG_EXECUTE_IF("preserve_trx_promotion_debug_apply_reached", {
      apply_state.apply_frozen = true;
      apply_state.applied_lsn = std::numeric_limits<uint64_t>::max();
      apply_state_available = true;
    });
    if (!apply_state_available || !apply_state.apply_frozen ||
        apply_state.applied_lsn < effective_required_apply_lsn) {
      for (uint64_t token : result->seen_tokens) {
        add_abandoned_token(
            result, token,
            Preserve_trx_promotion_adopt_status::APPLY_BARRIER_NOT_REACHED,
            "apply barrier not reached",
            Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING);
      }
      return finish_with_optional_abandoned_marker(
          Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS);
    }
  }
  if (request.execute_adopt &&
      !write_intent_marker_for_tokens(&store.store(), request,
                                      result->seen_tokens, result)) {
    for (uint64_t token : result->seen_tokens) {
      add_abandoned_token(
          result, token, Preserve_trx_promotion_adopt_status::IO_ERROR,
          "promotion intent marker not published before claim",
          Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED);
    }
    return finish_with_optional_abandoned_marker(
        Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS);
  }
  if (request.require_promotion_ready_cache) {
    uint64_t ready_count = 0;
    const uint64_t gate_deadline_us =
        my_micro_time() + request.gate_timeout_ms * 1000;
    struct Adopt_task {
      uint64_t token{0};
      Preserved_trx_bundle ready_bundle;
      bool adopted{false};
      uint64_t elapsed_us{0};
      Preserve_trx_promotion_token_result token_result;
    };
    std::vector<Adopt_task> adopt_tasks;
    for (uint64_t token : result->seen_tokens) {
      Promotion_ready_cache_entry cache_entry;
      const bool cache_hit =
          ready_cache_lookup(preserve_dir, request.epoch_id, token,
                             &cache_entry);
      if (cache_hit &&
          cache_entry.state == Preserve_trx_promotion_ready_state::READY) {
        if (ready_cache_entry_needs_record_lock_prewarm(cache_entry)) {
          result->record_lock_page_count += cache_entry.record_lock_page_count;
          result->record_lock_resident_pages +=
              cache_entry.record_lock_resident_pages;
        }
        if (request.require_epoch_committed &&
            (!epoch_fact_verified || !cache_entry.has_epoch_fact ||
             cache_entry.epoch_fact_digest != epoch_fact_digest)) {
          add_abandoned_token(
              result, token,
              Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
              "promotion-ready cache does not match accepted epoch fact",
              Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING);
          continue;
        }
        if (ready_cache_entry_needs_record_lock_prewarm(cache_entry) &&
            !cache_entry.record_lock_metadata_only &&
            !cache_entry.record_lock_pages_prewarmed) {
          add_abandoned_token(
              result, token,
              Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
              "record-lock pages were not prewarmed",
              Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING);
          continue;
        }
        if (ready_cache_entry_needs_record_lock_prewarm(cache_entry) &&
            !cache_entry.record_lock_metadata_only &&
            !record_lock_pages_are_gate_ready(
                cache_entry.record_lock_pages_prewarmed,
                cache_entry.record_lock_page_count,
                cache_entry.record_lock_prefetch_submitted_pages,
                cache_entry.record_lock_resident_pages)) {
          add_abandoned_token(
              result, token,
              Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
              "record-lock pages are not resident",
              Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING);
          continue;
        }
        if (request.execute_adopt) {
          if (!cache_entry.has_ready_bundle) {
            add_abandoned_token(
                result, token,
                Preserve_trx_promotion_adopt_status::READY_CACHE_NOT_READY,
                "promotion-ready record not built",
                Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING);
            continue;
          }

          Adopt_task task;
          task.token = token;
          task.ready_bundle = cache_entry.ready_bundle;
          adopt_tasks.push_back(std::move(task));
          continue;
        }
        ++ready_count;
        continue;
      }
      if (!cache_hit) ++result->ready_cache_miss_count;
      const bool corrupt_cache =
          cache_hit &&
          cache_entry.state == Preserve_trx_promotion_ready_state::CORRUPT;
      add_abandoned_token(
          result, token,
          corrupt_cache ? Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT
                        : Preserve_trx_promotion_adopt_status::
                              READY_CACHE_NOT_READY,
          corrupt_cache ? "promotion-ready cache is corrupt"
                        : "promotion-ready cache not built",
          Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING);
    }
    if (!adopt_tasks.empty()) {
      std::atomic<size_t> next_task{0};
      std::atomic<bool> workers_released{false};
      std::atomic<bool> worker_abort{false};
      const uint32_t worker_count = std::min<uint32_t>(
          request.worker_count, static_cast<uint32_t>(adopt_tasks.size()));
      auto run_worker = [&]() {
        while (!workers_released.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        for (;;) {
          if (worker_abort.load(std::memory_order_acquire)) break;
          const size_t index = next_task.fetch_add(1);
          if (index >= adopt_tasks.size()) break;
          Adopt_task &task = adopt_tasks[index];
          const uint64_t worker_started_us = my_micro_time();
          try {
            if (g_adopt_executor == nullptr) {
              task.adopted = promotion_adopt_ready_bundle_default(
                  preserve_dir, task.ready_bundle, gate_deadline_us,
                  &task.token_result);
            } else {
              task.adopted = g_adopt_executor(preserve_dir, task.ready_bundle,
                                              &task.token_result);
            }
          } catch (...) {
            task.token_result.token = task.token;
            task.token_result.status =
                Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT;
            task.token_result.reason = "promotion adopt worker failed";
            worker_abort.store(true, std::memory_order_release);
          }
          task.elapsed_us = my_micro_time() - worker_started_us;
        }
      };
      if (worker_count <= 1) {
        workers_released.store(true, std::memory_order_release);
        run_worker();
      } else {
        std::vector<std::thread> workers;
        auto join_workers = create_scope_guard([&] {
          for (std::thread &worker : workers) {
            if (worker.joinable()) worker.join();
          }
        });
        bool worker_creation_failed = false;
        try {
          workers.reserve(worker_count);
          for (uint32_t i = 0; i < worker_count; ++i) {
            workers.emplace_back(run_worker);
          }
        } catch (...) {
          worker_creation_failed = true;
          worker_abort.store(true, std::memory_order_release);
        }
        workers_released.store(true, std::memory_order_release);
        join_workers.rollback();
        if (worker_abort.load(std::memory_order_acquire)) {
          for (Adopt_task &task : adopt_tasks) {
            if (task.adopted || task.token_result.token != 0) continue;
            task.token_result.token = task.token;
            task.token_result.status =
                Preserve_trx_promotion_adopt_status::UNSUPPORTED_ARTIFACT;
            task.token_result.reason =
                worker_creation_failed ? "promotion adopt worker creation failed"
                                       : "promotion adopt worker aborted";
          }
        }
      }
      std::vector<uint64_t> worker_elapsed_samples;
      worker_elapsed_samples.reserve(adopt_tasks.size());
      for (Adopt_task &task : adopt_tasks) {
        result->max_worker_elapsed_us =
            std::max(result->max_worker_elapsed_us, task.elapsed_us);
        worker_elapsed_samples.push_back(task.elapsed_us);
        if (task.adopted) {
          ++result->adopted_count;
          adopted_tokens.push_back(task.token);
        } else {
          if (task.token_result.token == 0) task.token_result.token = task.token;
          add_abandoned_token_result(result, task.token_result);
        }
      }
      result->p50_worker_elapsed_us =
          promotion_gate_worker_elapsed_percentile(worker_elapsed_samples, 50);
      result->p95_worker_elapsed_us =
          promotion_gate_worker_elapsed_percentile(worker_elapsed_samples, 95);
    }
    result->skipped_count = ready_count;
    if (result->abandoned_count > 0) {
      return finish_with_optional_abandoned_marker(
          Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS);
    }
    return finish_with_optional_abandoned_marker(
        Preserve_trx_promotion_adopt_status::OK);
  }

  result->skipped_count = result->seen_tokens.size();
  return finish_with_optional_abandoned_marker(
      result->abandoned_count > 0
          ? Preserve_trx_promotion_adopt_status::OK_WITH_ABANDONED_TOKENS
          : Preserve_trx_promotion_adopt_status::OK);
}

namespace {

Preserve_trx_strict_physical_adopt_executor
    g_strict_physical_adopt_executor_for_unit_test = nullptr;
Preserve_trx_strict_physical_adopt_reversal_executor
    g_strict_physical_adopt_reversal_executor_for_unit_test = nullptr;

bool strict_prepared_snapshot_is_ready(
    const Preserve_trx_prepared_token_snapshot &snapshot) {
  const Preserve_trx_final_token_facts &facts = snapshot.facts;
  const bool has_record_locks = facts.record_lock_bitmap_entries != 0 ||
                                facts.record_lock_unique_pages != 0 ||
                                facts.record_lock_bits != 0;
  return (snapshot.state == Preserve_trx_prepared_token_state::
                                READY_FACTS_PENDING_LEASE ||
          snapshot.state ==
              Preserve_trx_prepared_token_state::READY_FOR_GATE) &&
         snapshot.semantic_bundle_owned && facts.semantic_validated &&
         snapshot.resurrection_entry_owned &&
         facts.lock_plan_ready && facts.binlog_handle_ready &&
         facts.resources_reserved && !facts.predicate_lock_present &&
         (!has_record_locks || snapshot.record_lock_plan_owned) &&
         (!facts.binlog_cache_present ||
          snapshot.native_binlog_handle_owned) &&
         !facts.canonical_digest.empty();
}

bool strict_prepared_snapshot_matches_fence(
    const Preserve_trx_prepared_token_snapshot &snapshot,
    const Preserve_trx_physical_fence_proof &proof) {
  const Preserve_trx_final_token_facts &facts = snapshot.facts;
  return strict_prepared_snapshot_is_ready(snapshot) &&
         facts.required_apply_lsn <= proof.target_frozen_lsn &&
         facts.physical_fence_lsn == proof.source_fence_lsn &&
         facts.epoch_fact_digest == proof.epoch_fact_digest &&
         facts.target_boot_incarnation == proof.target_boot_incarnation;
}

bool strict_physical_promotion_gtid_config_matches(
    const Preserve_trx_prepared_token_snapshot &snapshot,
    bool target_log_bin_configured, uint8_t target_gtid_mode) {
  return snapshot.semantic_bundle_owned &&
         snapshot.semantic_global_log_bin &&
         snapshot.semantic_has_binlog_gtid_mode &&
         snapshot.semantic_binlog_gtid_mode ==
             static_cast<uint8_t>(Gtid_mode::ON) &&
         target_log_bin_configured &&
         target_gtid_mode == static_cast<uint8_t>(Gtid_mode::ON);
}

bool strict_prepared_snapshot_matches_accepted_epoch(
    const Preserve_trx_prepared_token_snapshot &snapshot,
    const Preserve_trx_transfer_accepted_epoch &accepted,
    bool target_log_bin_configured, uint8_t target_gtid_mode) {
  const Preserve_trx_final_token_facts &facts = snapshot.facts;
  return strict_prepared_snapshot_is_ready(snapshot) &&
         strict_physical_promotion_gtid_config_matches(
             snapshot, target_log_bin_configured, target_gtid_mode) &&
         facts.required_apply_lsn <= accepted.source_fence_lsn &&
         facts.physical_fence_lsn == accepted.source_fence_lsn &&
         facts.epoch_fact_digest == digest_hex(accepted.fact_digest) &&
         facts.target_boot_incarnation == accepted.receiver_process_generation;
}

bool write_strict_promotion_intent(
    Preserved_trx_store *store,
    const Preserve_trx_strict_promotion_intent_epoch &intent,
    bool final_rewrite) {
  if (store == nullptr) return false;
  std::string encoded;
  if (!preserved_trx_encode_strict_promotion_intent_v1(intent, &encoded)) {
    return false;
  }
  if (final_rewrite) {
    DBUG_EXECUTE_IF(
        "preserve_trx_fail_write_final_strict_promotion_intent_epoch",
        return false;);
  }
  return store->write_promotion_intent_epoch(intent.epoch_id, encoded) ==
         Preserve_snapshot_status::OK;
}

Preserve_trx_physical_promotion_gate_status
adopt_ready_epoch_for_physical_promotion_impl(
    const std::string &preserve_dir,
    const Preserve_trx_physical_promotion_gate_request &request,
    bool use_test_provider, const std::vector<trx_t *> *verified_transactions,
    const Preserve_trx_transfer_accepted_epoch *accepted_epoch,
    const Preserve_trx_physical_promotion_pin_lease *prepared_token_pin,
    Preserve_trx_physical_promotion_gate_result *result) {
  if (result == nullptr) {
    return Preserve_trx_physical_promotion_gate_status::INVALID_ARGUMENT;
  }
  *result = {};
  const uint64_t started_us = my_micro_time();
  auto finish = [&](Preserve_trx_physical_promotion_gate_status status,
                    const char *message) {
    result->status = status;
    result->elapsed_us = my_micro_time() - started_us;
    result->message = message == nullptr ? "" : message;
    return status;
  };
  if (!preserve_trx_is_enabled()) {
    return finish(Preserve_trx_physical_promotion_gate_status::NOT_ENABLED,
                  "preserve/resume is disabled");
  }
  const auto required_mode =
      use_test_provider
          ? Preserve_trx_physical_consistency_mode::
                TEST_ONLY_PHYSICAL_FENCE_SIMULATOR
          : Preserve_trx_physical_consistency_mode::
                PRODUCTION_REDO_APPLY_FENCE;
  if (preserve_dir.empty() || request.epoch_id.empty() ||
      request.tokens.empty() || request.worker_count == 0 ||
      request.worker_count > kPromotionGateWorkerLimit ||
      (use_test_provider &&
       request.tokens.size() > kSub500SimulatorTokenLimit) ||
      (verified_transactions != nullptr &&
       verified_transactions->size() != request.tokens.size()) ||
      (use_test_provider &&
       (!preserved_trx_physical_fence_proof_is_valid(
            request.expected_fence) ||
        request.expected_fence.consistency_mode != required_mode ||
        accepted_epoch != nullptr || verified_transactions != nullptr)) ||
      (!use_test_provider &&
       (accepted_epoch == nullptr || accepted_epoch->fact == nullptr ||
        accepted_epoch->root_dir != preserve_dir ||
        accepted_epoch->epoch_id != request.epoch_id ||
        accepted_epoch->receiver_process_generation.empty() ||
        verified_transactions == nullptr))) {
    return finish(Preserve_trx_physical_promotion_gate_status::INVALID_ARGUMENT,
                  "invalid strict physical promotion request");
  }
  const std::string &expected_epoch_scope =
      use_test_provider ? request.expected_fence.source_lineage_uuid
                        : accepted_epoch->receiver_process_generation;
  const std::string &expected_target_boot =
      use_test_provider ? request.expected_fence.target_boot_incarnation
                        : accepted_epoch->receiver_process_generation;
  std::set<std::string> token_ids;
  for (const Preserve_trx_prepared_token_key &key : request.tokens) {
    if (key.preserve_dir != preserve_dir ||
        key.epoch_scope != expected_epoch_scope ||
        key.epoch_id != request.epoch_id || key.token.empty() ||
        key.target_boot_incarnation != expected_target_boot ||
        key.generation == 0 || !token_ids.insert(key.token).second) {
      return finish(
          Preserve_trx_physical_promotion_gate_status::INVALID_ARGUMENT,
          "strict token identity does not match receiver epoch");
    }
  }

  Preserve_trx_physical_fence_lease acquired_physical_lease;
  Preserve_trx_physical_fence_lease *physical_lease = nullptr;
  uint64_t fence_acquire_us = 0;
  Preserve_trx_physical_fence_status fence_status =
      Preserve_trx_physical_fence_status::OK;
  if (use_test_provider) {
    const uint64_t fence_acquire_started_us = my_micro_time();
    fence_status = preserved_trx_acquire_physical_fence_lease_for_unit_test(
        request.expected_fence, &acquired_physical_lease);
    fence_acquire_us = my_micro_time() - fence_acquire_started_us;
    physical_lease = &acquired_physical_lease;
  }
  if (fence_status == Preserve_trx_physical_fence_status::MISSING_PROVIDER) {
    return finish(
        Preserve_trx_physical_promotion_gate_status::
            PHYSICAL_FENCE_NOT_AVAILABLE,
        "test physical-fence provider is not registered");
  }
  if (fence_status != Preserve_trx_physical_fence_status::OK) {
    return finish(
        Preserve_trx_physical_promotion_gate_status::PHYSICAL_FENCE_MISMATCH,
        "test physical-fence acquisition failed");
  }
  preserved_trx_promotion_prepared_note_evidence(
      required_mode, !use_test_provider);

  const uint64_t digest_compare_started_us = my_micro_time();
  auto &registry = preserved_trx_strict_prepared_token_registry();
  const bool target_log_bin_configured = opt_bin_log;
  const uint8_t target_gtid_mode =
      static_cast<uint8_t>(Gtid_mode::sysvar_mode);
  std::vector<Preserve_trx_prepared_token_snapshot> snapshots;
  snapshots.reserve(request.tokens.size());
  for (const Preserve_trx_prepared_token_key &key : request.tokens) {
    Preserve_trx_prepared_token_snapshot snapshot;
    if (registry.snapshot(key, &snapshot) != Preserve_trx_prepared_status::OK ||
        !(use_test_provider
              ? strict_prepared_snapshot_matches_fence(
                    snapshot, request.expected_fence)
              : strict_prepared_snapshot_matches_accepted_epoch(
                    snapshot, *accepted_epoch, target_log_bin_configured,
                    target_gtid_mode))) {
      return finish(
          Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY,
          "strict prepared-token facts are not promotion ready");
    }
    snapshots.push_back(std::move(snapshot));
  }
  std::vector<Preserve_trx_epoch_physical_digest_input> digest_inputs;
  digest_inputs.reserve(snapshots.size());
  for (size_t i = 0; i < snapshots.size(); ++i) {
    digest_inputs.push_back(
        {request.tokens[i].token, request.tokens[i].generation,
         snapshots[i].facts.final_lock_generation_digest,
         snapshots[i].facts.page_layout_digest,
         snapshots[i].facts.dictionary_generation_digest});
  }
  std::string final_lock_generation_digest;
  std::string page_layout_digest;
  std::string dictionary_generation_digest;
  if (!preserved_trx_compute_epoch_physical_digest_commitments(
          digest_inputs, &final_lock_generation_digest, &page_layout_digest,
          &dictionary_generation_digest) ||
      (use_test_provider &&
       (final_lock_generation_digest !=
            request.expected_fence.final_lock_generation_digest ||
        page_layout_digest != request.expected_fence.page_layout_digest ||
        dictionary_generation_digest !=
            request.expected_fence.dictionary_generation_digest))) {
    preserved_trx_promotion_prepared_note_fence_metrics(
        fence_acquire_us, my_micro_time() - digest_compare_started_us, 0);
    return finish(
        Preserve_trx_physical_promotion_gate_status::PHYSICAL_FENCE_MISMATCH,
        "strict prepared-token epoch digest commitment mismatch");
  }
  const uint64_t digest_compare_us =
      my_micro_time() - digest_compare_started_us;
  uint64_t fence_revalidate_us = 0;
  Preserve_trx_physical_fence_status fence_revalidate_status =
      Preserve_trx_physical_fence_status::OK;
  if (use_test_provider) {
    const uint64_t fence_revalidate_started_us = my_micro_time();
    fence_revalidate_status = physical_lease->revalidate();
    fence_revalidate_us = my_micro_time() - fence_revalidate_started_us;
  }
  preserved_trx_promotion_prepared_note_fence_metrics(
      fence_acquire_us, digest_compare_us, fence_revalidate_us);
  if (fence_revalidate_status != Preserve_trx_physical_fence_status::OK) {
    return finish(
        Preserve_trx_physical_promotion_gate_status::PHYSICAL_FENCE_MISMATCH,
        "physical-fence revalidation failed before intent");
  }
  for (const Preserve_trx_prepared_token_key &key : request.tokens) {
    const auto ready_status =
        registry.mark_ready_for_gate(key, key.generation);
    if (ready_status != Preserve_trx_prepared_status::OK &&
        ready_status != Preserve_trx_prepared_status::IDEMPOTENT) {
      return finish(
          Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY,
          "strict prepared-token state changed before intent");
    }
  }

  const uint32_t worker_count = std::min<uint32_t>(
      request.worker_count, static_cast<uint32_t>(request.tokens.size()));
  if (!ensure_promotion_gate_workers(worker_count)) {
    return finish(Preserve_trx_physical_promotion_gate_status::ADOPT_FAILED,
                  "promotion gate worker pool is unavailable");
  }

  auto store = create_preserved_trx_process_local_store(preserve_dir);
  Preserve_trx_strict_promotion_intent_epoch intent;
  intent.epoch_id = request.epoch_id;
  intent.physical_fence = request.expected_fence;
  intent.generated_at_us = my_micro_time();
  intent.tokens.reserve(request.tokens.size());
  for (const Preserve_trx_prepared_token_key &key : request.tokens) {
    intent.tokens.push_back(
        {key.token, key.generation,
         Preserve_trx_strict_promotion_intent_state::ADOPTING});
  }
  if (use_test_provider &&
      !write_strict_promotion_intent(&store.store(), intent, false)) {
    return finish(Preserve_trx_physical_promotion_gate_status::INTENT_IO_ERROR,
                  "failed to write strict promotion intent");
  }

  const uint64_t deadline_us =
      request.operation_deadline_us != 0
          ? request.operation_deadline_us
          : started_us +
                static_cast<uint64_t>(preserve_trx_promotion_gate_timeout_ms) *
                    1000;
  const auto executor =
      g_strict_physical_adopt_executor_for_unit_test == nullptr
          ? preserved_trx_import_reserved_for_physical_promotion
          : g_strict_physical_adopt_executor_for_unit_test;
  struct Strict_adopt_task {
    Preserve_trx_strict_promotion_intent_state state{
        Preserve_trx_strict_promotion_intent_state::CLEANUP_TAINTED};
    uint64_t token{0};
  };
  std::vector<Strict_adopt_task> tasks(request.tokens.size());
  for (size_t i = 0; i < request.tokens.size(); ++i) {
    if (!parse_uint64_strict(request.tokens[i].token, &tasks[i].token) ||
        tasks[i].token == 0) {
      return finish(
          Preserve_trx_physical_promotion_gate_status::INVALID_ARGUMENT,
          "strict physical promotion token is not numeric");
    }
  }
  std::atomic<size_t> next_task{0};
  std::atomic<bool> worker_abort{false};
  std::atomic<bool> global_failure{false};
  auto run_worker = [&]() {
    for (;;) {
      if (worker_abort.load(std::memory_order_acquire)) break;
      const size_t i = next_task.fetch_add(1);
      if (i >= request.tokens.size()) break;
      const Preserve_trx_prepared_token_key &key = request.tokens[i];
      Preserve_trx_gate_adopt_lease adopt_lease;
      if (registry.begin_gate_adopt(key, key.generation, &adopt_lease,
                                    prepared_token_pin) !=
          Preserve_trx_prepared_status::OK) {
        global_failure.store(true, std::memory_order_release);
        worker_abort.store(true, std::memory_order_release);
        continue;
      }
      try {
        Preserved_trx_physical_adopt_result adopt_result;
        const auto adopt_status = executor(
            preserve_dir, &adopt_lease,
            verified_transactions == nullptr ? nullptr
                                             : (*verified_transactions)[i],
            physical_lease, deadline_us, &adopt_result);
        if (adopt_status == Preserved_trx_physical_adopt_status::OK &&
            registry.commit_gate_adopt(&adopt_lease) ==
                Preserve_trx_prepared_status::OK) {
          tasks[i].state =
              Preserve_trx_strict_promotion_intent_state::ADOPTED_LOCKED;
        } else if (adopt_result.rolled_back &&
                   registry.abort_gate_adopt(
                       &adopt_lease,
                       Preserve_trx_gate_abort_outcome::
                           ABANDONED_ROLLED_BACK) ==
                       Preserve_trx_prepared_status::OK) {
          tasks[i].state = Preserve_trx_strict_promotion_intent_state::
              ABANDONED_ROLLED_BACK;
        } else {
          if (adopt_lease.active()) {
            (void)registry.abort_gate_adopt(
                &adopt_lease,
                Preserve_trx_gate_abort_outcome::CLEANUP_TAINTED);
          }
          global_failure.store(true, std::memory_order_release);
          worker_abort.store(true, std::memory_order_release);
        }
      } catch (...) {
        global_failure.store(true, std::memory_order_release);
        worker_abort.store(true, std::memory_order_release);
        if (adopt_lease.active()) {
          (void)registry.abort_gate_adopt(
              &adopt_lease,
              Preserve_trx_gate_abort_outcome::CLEANUP_TAINTED);
        }
      }
    }
  };
  if (!run_promotion_gate_worker_batch(worker_count, run_worker)) {
    global_failure.store(true, std::memory_order_release);
    worker_abort.store(true, std::memory_order_release);
  }
  const auto reversal_executor =
      g_strict_physical_adopt_reversal_executor_for_unit_test == nullptr
          ? (use_test_provider
                 ? preserved_trx_reverse_simulated_promotion_adopt
                 : preserved_trx_rollback_physical_promotion_adopt)
          : g_strict_physical_adopt_reversal_executor_for_unit_test;
  auto reverse_adopted_tokens = [&]() {
    bool complete = true;
    for (size_t i = 0; i < tasks.size(); ++i) {
      if (tasks[i].state !=
          Preserve_trx_strict_promotion_intent_state::ADOPTED_LOCKED) {
        continue;
      }
      Preserve_trx_cleanup_lease cleanup;
      std::string reversal_reason;
      const bool cleanup_started =
          registry.begin_cleanup(
              request.tokens[i], request.tokens[i].generation,
              Preserve_trx_prepared_token_state::ADOPTED_LOCKED, &cleanup) ==
          Preserve_trx_prepared_status::OK;
      const bool reversed =
          cleanup_started &&
          reversal_executor(
              request.tokens[i],
              verified_transactions == nullptr ? nullptr
                                               : (*verified_transactions)[i],
              &cleanup, physical_lease, &reversal_reason);
      if (reversed &&
          registry.commit_cleanup(&cleanup, true) ==
              Preserve_trx_prepared_status::OK) {
        tasks[i].state = Preserve_trx_strict_promotion_intent_state::
            ABANDONED_ROLLED_BACK;
        continue;
      }
      if (cleanup.active()) (void)registry.commit_cleanup(&cleanup, false);
      tasks[i].state =
          Preserve_trx_strict_promotion_intent_state::CLEANUP_TAINTED;
      complete = false;
    }
    return complete;
  };

  if (global_failure.load(std::memory_order_acquire)) {
    (void)reverse_adopted_tokens();
  }

  auto summarize_tasks = [&]() {
    result->adopted_count = 0;
    result->rolled_back_count = 0;
    result->tainted_count = 0;
    for (size_t i = 0; i < tasks.size(); ++i) {
      intent.tokens[i].state = tasks[i].state;
      switch (tasks[i].state) {
        case Preserve_trx_strict_promotion_intent_state::ADOPTED_LOCKED:
          ++result->adopted_count;
          break;
        case Preserve_trx_strict_promotion_intent_state::
            ABANDONED_ROLLED_BACK:
          ++result->rolled_back_count;
          break;
        default:
          ++result->tainted_count;
          break;
      }
    }
  };
  summarize_tasks();

  intent.generated_at_us = my_micro_time();
  if (use_test_provider &&
      !write_strict_promotion_intent(&store.store(), intent, true)) {
    if (use_test_provider) {
      const bool reversed = reverse_adopted_tokens();
      summarize_tasks();
      return finish(
          reversed && result->tainted_count == 0
              ? Preserve_trx_physical_promotion_gate_status::INTENT_IO_ERROR
              : Preserve_trx_physical_promotion_gate_status::CLEANUP_TAINTED,
          reversed && result->tainted_count == 0
              ? "strict final process-local intent write failed; simulator "
                "epoch was "
                "fully reversed"
              : "strict final process-local intent write failed; simulator "
                "cleanup is "
                "tainted");
    }
    uint64_t newly_tainted = 0;
    for (size_t i = 0; i < request.tokens.size(); ++i) {
      if (intent.tokens[i].state !=
          Preserve_trx_strict_promotion_intent_state::ADOPTED_LOCKED) {
        continue;
      }
      Preserve_trx_cleanup_lease cleanup;
      if (registry.begin_cleanup(
              request.tokens[i], request.tokens[i].generation,
              Preserve_trx_prepared_token_state::ADOPTED_LOCKED,
              &cleanup) == Preserve_trx_prepared_status::OK &&
          registry.commit_cleanup(&cleanup, false) ==
              Preserve_trx_prepared_status::OK) {
        ++newly_tainted;
      }
    }
    result->tainted_count += newly_tainted;
    result->adopted_count =
        result->adopted_count >= newly_tainted
            ? result->adopted_count - newly_tainted
            : 0;
    return finish(Preserve_trx_physical_promotion_gate_status::CLEANUP_TAINTED,
                  "strict final process-local intent write failed");
  }
  if (result->tainted_count != 0) {
    return finish(Preserve_trx_physical_promotion_gate_status::CLEANUP_TAINTED,
                  "strict promotion left tainted tokens");
  }
  if (global_failure.load(std::memory_order_acquire)) {
    return finish(Preserve_trx_physical_promotion_gate_status::ADOPT_FAILED,
                  "strict promotion encountered an epoch-global failure");
  }
  try {
    result->adopted_tokens.reserve(result->adopted_count);
    result->ordinary_recovery_tokens.reserve(result->rolled_back_count);
    for (const Strict_adopt_task &task : tasks) {
      if (task.state ==
          Preserve_trx_strict_promotion_intent_state::ADOPTED_LOCKED) {
        result->adopted_tokens.push_back(task.token);
      } else if (task.state == Preserve_trx_strict_promotion_intent_state::
                                   ABANDONED_ROLLED_BACK) {
        result->ordinary_recovery_tokens.push_back(task.token);
      }
    }
  } catch (const std::bad_alloc &) {
    const bool reversed = reverse_adopted_tokens();
    summarize_tasks();
    return finish(
        reversed && result->tainted_count == 0
            ? Preserve_trx_physical_promotion_gate_status::ADOPT_FAILED
            : Preserve_trx_physical_promotion_gate_status::CLEANUP_TAINTED,
        "strict promotion result allocation failed");
  }
  return finish(Preserve_trx_physical_promotion_gate_status::OK, "ok");
}

}  // namespace

class Preserve_trx_physical_promotion_bootstrap_attempt::Impl {
 public:
  Preserve_trx_physical_promotion_gate_request gate_request;
  Preserve_trx_transfer_accepted_epoch accepted_epoch;
  Preserve_trx_physical_promotion_pin_lease prepared_token_pin;
  std::vector<trx_t *> reserved_transactions;
  bool gate_handoff_started{false};
  bool accepted_epoch_completed{false};
  bool active{false};
};

Preserve_trx_physical_promotion_bootstrap_attempt::
    Preserve_trx_physical_promotion_bootstrap_attempt()
    : m_impl(new (std::nothrow) Impl()) {}

Preserve_trx_physical_promotion_bootstrap_attempt::
    ~Preserve_trx_physical_promotion_bootstrap_attempt() {
  (void)abort();
}

Preserve_trx_physical_promotion_gate_status
Preserve_trx_physical_promotion_bootstrap_attempt::abort() {
  if (m_impl == nullptr || !m_impl->active) {
    return Preserve_trx_physical_promotion_gate_status::OK;
  }

  auto finish_attempt = [&] {
    trx_preserve_startup_resurrection_reset();
    m_impl->accepted_epoch = {};
    m_impl->gate_request = {};
    m_impl->reserved_transactions.clear();
    m_impl->gate_handoff_started = false;
    m_impl->accepted_epoch_completed = false;
    m_impl->active = false;
    g_physical_promotion_bootstrap_active.store(false,
                                                std::memory_order_release);
  };
  bool cleanup_tainted = false;
  const bool abandon_reservations = !m_impl->gate_handoff_started;

  if (m_impl->accepted_epoch_completed) {
    m_impl->prepared_token_pin.release_atomically();
    finish_attempt();
    return Preserve_trx_physical_promotion_gate_status::OK;
  }

  if (!m_impl->accepted_epoch.epoch_id.empty()) {
    bool receiver_reaper_owns_cleanup =
        m_impl->accepted_epoch.lifecycle ==
        Preserve_trx_transfer_epoch_lifecycle::ABANDONING;
    if (m_impl->accepted_epoch.lifecycle ==
        Preserve_trx_transfer_epoch_lifecycle::ADOPT_LEASED) {
      const auto abandon_status =
          preserve_trx_transfer_abandon_receiver_promotion_lease_process_local(
              &m_impl->accepted_epoch);
      receiver_reaper_owns_cleanup =
          abandon_status == Preserve_trx_transfer_status::OK;
      cleanup_tainted = cleanup_tainted || !receiver_reaper_owns_cleanup;
    }
    if (receiver_reaper_owns_cleanup) {
      m_impl->accepted_epoch.lifecycle =
          Preserve_trx_transfer_epoch_lifecycle::ABANDONING;
    }
    /*
      For a valid acquired snapshot, a non-OK abandon result proves that the
      registry no longer contains the same ADOPT_LEASED owner: exact identity
      validation and ADOPT_LEASED -> ABANDONING run under one registry mutex.
    */
    m_impl->prepared_token_pin.release_for_abandon();
    trx_preserve_startup_resurrection_reset();
    if (receiver_reaper_owns_cleanup &&
        preserve_trx_transfer_destroy_abandoning_receiver_epoch_process_local(
            m_impl->accepted_epoch) != Preserve_trx_transfer_status::OK) {
      cleanup_tainted = true;
    }
    if (!cleanup_tainted && abandon_reservations &&
        trx_preserve_abandon_active_undo_reservations(
            m_impl->reserved_transactions) != DB_SUCCESS) {
      cleanup_tainted = true;
    }
    /* Once ABANDONING is published, the receiver reaper owns retry. */
    finish_attempt();
    return cleanup_tainted
               ? Preserve_trx_physical_promotion_gate_status::CLEANUP_TAINTED
               : Preserve_trx_physical_promotion_gate_status::OK;
  } else {
    trx_preserve_startup_resurrection_reset();
  }
  finish_attempt();
  return Preserve_trx_physical_promotion_gate_status::OK;
}

bool Preserve_trx_physical_promotion_bootstrap_attempt::prepare_gate_handoff(
    const Preserve_trx_physical_promotion_gate_request **request,
    std::vector<trx_t *> *verified_transactions,
    const Preserve_trx_transfer_accepted_epoch **accepted_epoch,
    const Preserve_trx_physical_promotion_pin_lease **prepared_token_pin) {
  if (m_impl == nullptr || !m_impl->active || m_impl->accepted_epoch_completed ||
      request == nullptr ||
      verified_transactions == nullptr ||
      accepted_epoch == nullptr || prepared_token_pin == nullptr ||
      (m_impl->gate_request.tokens.empty()
           ? m_impl->prepared_token_pin.active()
           : !m_impl->prepared_token_pin.active()) ||
      my_micro_time() >= m_impl->gate_request.operation_deadline_us ||
      m_impl->accepted_epoch.lifecycle !=
          Preserve_trx_transfer_epoch_lifecycle::ADOPT_LEASED) {
    return false;
  }

  trx_preserve_startup_reservation_result reservation;
  try {
    verified_transactions->reserve(m_impl->gate_request.tokens.size());
    for (const auto &key : m_impl->gate_request.tokens) {
      trx_t *verified =
          trx_preserve_startup_resurrection_find_verified(key.token);
      if (verified == nullptr) return false;
      verified_transactions->push_back(verified);
    }
    if (trx_preserve_startup_reserve_verified(&reservation) != DB_SUCCESS ||
        !reservation.rejected_authorities.empty() ||
        reservation.reserved_authorities.size() !=
            verified_transactions->size()) {
      m_impl->reserved_transactions = *verified_transactions;
      return false;
    }
    for (size_t index = 0; index < verified_transactions->size(); ++index) {
      if (!trx_preserve_validate_reserved_authority(
              (*verified_transactions)[index],
              m_impl->gate_request.tokens[index].token)) {
        m_impl->reserved_transactions = *verified_transactions;
        return false;
      }
    }
    m_impl->reserved_transactions = *verified_transactions;
  } catch (const std::bad_alloc &) {
    return false;
  }

  *request = &m_impl->gate_request;
  *accepted_epoch = &m_impl->accepted_epoch;
  *prepared_token_pin = &m_impl->prepared_token_pin;
  m_impl->gate_handoff_started = true;
  return true;
}

bool Preserve_trx_physical_promotion_bootstrap_attempt::
    complete_gate_handoff() {
  if (m_impl == nullptr || !m_impl->active) return false;
  if (!m_impl->accepted_epoch_completed) {
    if (preserve_trx_transfer_complete_receiver_promotion_lease_process_local(
            m_impl->accepted_epoch) != Preserve_trx_transfer_status::OK) {
      return false;
    }
    m_impl->accepted_epoch_completed = true;
  }
  m_impl->prepared_token_pin.release_atomically();
  trx_preserve_startup_resurrection_reset();
  m_impl->accepted_epoch = {};
  m_impl->gate_request = {};
  m_impl->reserved_transactions.clear();
  m_impl->gate_handoff_started = false;
  m_impl->accepted_epoch_completed = false;
  m_impl->active = false;
  g_physical_promotion_bootstrap_active.store(false,
                                              std::memory_order_release);
  return true;
}

Preserve_trx_physical_promotion_gate_status
preserved_trx_prepare_before_trx_sys_init_for_physical_promotion(
    const Preserve_trx_physical_bootstrap_request &request,
    Preserve_trx_physical_promotion_bootstrap_attempt *attempt) {
  if (!preserve_trx_is_enabled()) {
    return Preserve_trx_physical_promotion_gate_status::NOT_ENABLED;
  }
  if (preserved_trx_server_startup_active()) {
    return Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY;
  }
  if (request.epoch_id.empty() || attempt == nullptr ||
      attempt->m_impl == nullptr ||
      attempt->m_impl->active ||
      request.operation_deadline_us <= my_micro_time() ||
      request.worker_count == 0 ||
      request.worker_count > kPromotionGateWorkerLimit) {
    return Preserve_trx_physical_promotion_gate_status::INVALID_ARGUMENT;
  }

  bool expected_inactive = false;
  if (!g_physical_promotion_bootstrap_active.compare_exchange_strong(
          expected_inactive, true, std::memory_order_acq_rel)) {
    return Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY;
  }

  auto *impl = attempt->m_impl.get();
  impl->active = true;
  auto fail_closed = [&](Preserve_trx_physical_promotion_gate_status status) {
    const auto cleanup_status = attempt->abort();
    return cleanup_status == Preserve_trx_physical_promotion_gate_status::OK
               ? status
               : cleanup_status;
  };

  const std::string root_dir =
      promotion_normalize_dir(preserved_trx_dir_value());
  const uint64_t operation_deadline_monotonic_us =
      promotion_wall_deadline_to_monotonic(request.operation_deadline_us);
  if (operation_deadline_monotonic_us == 0) {
    return fail_closed(
        Preserve_trx_physical_promotion_gate_status::INVALID_ARGUMENT);
  }
  const auto accepted_status =
      preserve_trx_transfer_try_acquire_receiver_promotion_lease(
          root_dir, request.epoch_id, promotion_monotonic_us(),
          &impl->accepted_epoch);
  if (accepted_status !=
      Preserve_trx_receiver_promotion_lease_status::ACQUIRED) {
    return fail_closed(
        Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
  }
  const auto &accepted = impl->accepted_epoch;
  if (accepted.fact == nullptr || accepted.epoch_id != request.epoch_id ||
      accepted.root_dir != root_dir ||
      accepted.receiver_process_generation.empty() ||
      accepted.source_fence_lsn == 0 ||
      accepted.fact->epoch_id != accepted.epoch_id ||
      accepted.fact->source_fence_lsn != accepted.source_fence_lsn ||
      accepted.fact->fact_digest != accepted.fact_digest) {
    return fail_closed(
        Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
  }

  std::vector<Preserve_trx_transfer_epoch_fact_token> fact_tokens;
  std::set<uint64_t> ready_tokens;
  std::set<uint64_t> failed_tokens;
  try {
    if (!accepted.selection_published) {
      return fail_closed(
          Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
    }
    ready_tokens.insert(accepted.ready_tokens.begin(),
                        accepted.ready_tokens.end());
    if (ready_tokens.size() != accepted.ready_tokens.size()) {
      return fail_closed(
          Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
    }
    for (const auto &failed : accepted.failed_tokens) {
      if (failed.token == 0 ||
          failed.reason == Preserve_trx_receiver_failure_reason::NONE ||
          !failed_tokens.insert(failed.token).second ||
          ready_tokens.count(failed.token) != 0) {
        return fail_closed(
            Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
      }
    }
    if (ready_tokens.size() + failed_tokens.size() != accepted.tokens.size()) {
      return fail_closed(
          Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
    }

    fact_tokens = accepted.fact->tokens;
    std::sort(fact_tokens.begin(), fact_tokens.end(),
              [](const auto &left, const auto &right) {
                return left.token < right.token;
              });
    if (fact_tokens.size() != accepted.tokens.size() || fact_tokens.empty()) {
      return fail_closed(
          Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
    }
    impl->gate_request.epoch_id = accepted.epoch_id;
    impl->gate_request.operation_deadline_us = request.operation_deadline_us;
    impl->gate_request.worker_count = request.worker_count;
    impl->gate_request.tokens.reserve(ready_tokens.size());
    for (size_t index = 0; index < fact_tokens.size(); ++index) {
      const auto &token = fact_tokens[index];
      if (token.token == 0 || token.source_freeze_lsn == 0 ||
          token.source_epoch_commit_lsn == 0 ||
          token.source_epoch_commit_lsn > accepted.source_fence_lsn ||
          token.token != accepted.tokens[index] ||
          (index != 0 && fact_tokens[index - 1].token == token.token) ||
          (ready_tokens.count(token.token) == 0) ==
              (failed_tokens.count(token.token) == 0)) {
        return fail_closed(
            Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
      }
      if (ready_tokens.count(token.token) != 0) {
        impl->gate_request.tokens.push_back(
            {root_dir, accepted.receiver_process_generation, accepted.epoch_id,
             std::to_string(token.token), accepted.receiver_process_generation,
             token.source_epoch_commit_lsn});
      }
    }
  } catch (const std::bad_alloc &) {
    return fail_closed(
        Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
  }

  auto &prepared_registry = preserved_trx_strict_prepared_token_registry();
  const bool target_log_bin_configured = opt_bin_log;
  const uint8_t target_gtid_mode =
      static_cast<uint8_t>(Gtid_mode::sysvar_mode);
  for (const auto &key : impl->gate_request.tokens) {
    Preserve_trx_prepared_token_snapshot snapshot;
    if (prepared_registry.snapshot(key, &snapshot) !=
            Preserve_trx_prepared_status::OK ||
        !strict_prepared_snapshot_is_ready(snapshot)) {
      return fail_closed(
          Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
    }
    if (!strict_physical_promotion_gtid_config_matches(
            snapshot, target_log_bin_configured, target_gtid_mode)) {
      LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
             "PRESERVE: physical promotion fast resume requires source and "
             "target log_bin=ON and gtid_mode=ON");
      return fail_closed(
          Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
    }
  }
  if (!impl->gate_request.tokens.empty() &&
      prepared_registry.pin_epoch_for_physical_promotion(
          impl->gate_request.tokens, promotion_monotonic_us(),
          operation_deadline_monotonic_us, &impl->prepared_token_pin) !=
          Preserve_trx_prepared_status::OK) {
    return fail_closed(
        Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
  }

  std::vector<Preserve_trx_resurrection_index_entry> ready_entries;
  try {
    ready_entries.reserve(impl->gate_request.tokens.size());
    size_t ready_index = 0;
    for (const auto &fact_token : fact_tokens) {
      if (ready_tokens.count(fact_token.token) == 0) continue;
      Preserve_trx_resurrection_index_entry entry;
      if (ready_index >= impl->gate_request.tokens.size() ||
          prepared_registry.copy_ready_resurrection_entry(
              impl->gate_request.tokens[ready_index], promotion_monotonic_us(),
              &entry) != Preserve_trx_prepared_status::OK ||
          entry.authority_token != std::to_string(fact_token.token) ||
          entry.freeze_lsn != fact_token.source_freeze_lsn) {
        return fail_closed(
            Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
      }
      ready_entries.push_back(std::move(entry));
      ++ready_index;
    }
    if (ready_index != impl->gate_request.tokens.size()) {
      return fail_closed(
          Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
    }
  } catch (const std::bad_alloc &) {
    return fail_closed(
        Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
  }
  if (ready_entries.empty()) {
    trx_preserve_startup_resurrection_reset();
  } else {
    if (preserved_trx_register_physical_resurrection_candidates(ready_entries)) {
      return fail_closed(
          Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY);
    }
  }
  return Preserve_trx_physical_promotion_gate_status::OK;
}

void preserved_trx_set_strict_physical_adopt_executor_for_unit_test(
    Preserve_trx_strict_physical_adopt_executor executor) {
  g_strict_physical_adopt_executor_for_unit_test = executor;
}

void preserved_trx_set_strict_physical_adopt_reversal_executor_for_unit_test(
    Preserve_trx_strict_physical_adopt_reversal_executor executor) {
  g_strict_physical_adopt_reversal_executor_for_unit_test = executor;
}

Preserve_trx_physical_promotion_gate_status
preserved_trx_adopt_ready_epoch_for_physical_promotion(
    Preserve_trx_physical_promotion_bootstrap_attempt *attempt,
    Preserve_trx_physical_promotion_gate_result *result) {
  if (result == nullptr) {
    return Preserve_trx_physical_promotion_gate_status::INVALID_ARGUMENT;
  }
  if (preserved_trx_server_startup_active()) {
    *result = {};
    result->status =
        Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY;
    result->message = "physical promotion is unavailable during server startup";
    return result->status;
  }
  const Preserve_trx_physical_promotion_gate_request *request = nullptr;
  std::vector<trx_t *> verified_transactions;
  const Preserve_trx_transfer_accepted_epoch *accepted_epoch = nullptr;
  const Preserve_trx_physical_promotion_pin_lease *prepared_token_pin = nullptr;
  if (attempt == nullptr ||
      !attempt->prepare_gate_handoff(&request, &verified_transactions,
                                     &accepted_epoch, &prepared_token_pin)) {
    const auto cleanup_status =
        attempt == nullptr
            ? Preserve_trx_physical_promotion_gate_status::OK
            : attempt->abort();
    *result = {};
    result->status =
        cleanup_status == Preserve_trx_physical_promotion_gate_status::OK
            ? Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY
            : cleanup_status;
    result->message = "physical promotion bootstrap attempt is not gate ready";
    return result->status;
  }

  std::vector<uint64_t> initially_failed_tokens;
  try {
    initially_failed_tokens.reserve(accepted_epoch->failed_tokens.size());
    for (const auto &failed : accepted_epoch->failed_tokens) {
      initially_failed_tokens.push_back(failed.token);
    }
    if (accepted_epoch->ready_tokens.size() != verified_transactions.size()) {
      (void)attempt->abort();
      *result = {};
      result->status =
          Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY;
      result->message = "physical promotion token partition changed";
      return result->status;
    }
  } catch (const std::bad_alloc &) {
    (void)attempt->abort();
    *result = {};
    result->status =
        Preserve_trx_physical_promotion_gate_status::REGISTRY_NOT_READY;
    result->message = "physical promotion result allocation failed";
    return result->status;
  }

  Preserve_trx_physical_promotion_gate_status status;
  if (request->tokens.empty()) {
    *result = {};
    result->status = Preserve_trx_physical_promotion_gate_status::OK;
    result->message = "no READY tokens; ordinary recovery only";
    status = result->status;
  } else {
    status = adopt_ready_epoch_for_physical_promotion_impl(
        promotion_normalize_dir(preserved_trx_dir_value()), *request, false,
        &verified_transactions, accepted_epoch, prepared_token_pin, result);
  }

  if (status == Preserve_trx_physical_promotion_gate_status::OK) {
    try {
      result->ordinary_recovery_tokens.insert(
          result->ordinary_recovery_tokens.end(),
          initially_failed_tokens.begin(), initially_failed_tokens.end());
      std::sort(result->ordinary_recovery_tokens.begin(),
                result->ordinary_recovery_tokens.end());
    } catch (const std::bad_alloc &) {
      result->status =
          Preserve_trx_physical_promotion_gate_status::CLEANUP_TAINTED;
      result->message =
          "physical promotion result allocation failed after exact adopt";
      return result->status;
    }
    if (!attempt->complete_gate_handoff()) {
      result->status =
          Preserve_trx_physical_promotion_gate_status::CLEANUP_TAINTED;
      result->message = "accepted receiver epoch cleanup failed after adopt";
      return result->status;
    }
  } else if (status !=
                 Preserve_trx_physical_promotion_gate_status::CLEANUP_TAINTED &&
             result->tainted_count == 0) {
    const bool all_ready_rolled_back =
        result->rolled_back_count == verified_transactions.size();
    if (result->rolled_back_count != 0 && !all_ready_rolled_back) {
      result->status =
          Preserve_trx_physical_promotion_gate_status::CLEANUP_TAINTED;
      result->message =
          "strict adopt failed with a partial reservation outcome";
    }
    const auto cleanup_status = attempt->abort();
    if (cleanup_status != Preserve_trx_physical_promotion_gate_status::OK) {
      result->status = cleanup_status;
      result->message =
          "physical promotion gate failed and epoch cleanup is tainted";
    } else if (!all_ready_rolled_back && result->rolled_back_count == 0 &&
               trx_preserve_abandon_active_undo_reservations(
                   verified_transactions) != DB_SUCCESS) {
      result->status =
          Preserve_trx_physical_promotion_gate_status::CLEANUP_TAINTED;
      result->message =
          "physical promotion authority was revoked but READY reservation "
          "cleanup failed";
    }
    result->adopted_tokens.clear();
    result->ordinary_recovery_tokens.clear();
    return result->status;
  } else {
    const auto cleanup_status = attempt->abort();
    if (cleanup_status != Preserve_trx_physical_promotion_gate_status::OK) {
      result->status = cleanup_status;
      result->message =
          "physical promotion gate failed and epoch cleanup is tainted";
      return cleanup_status;
    }
  }
  return status;
}

Preserve_trx_physical_promotion_gate_status
preserved_trx_adopt_ready_epoch_for_physical_promotion_for_unit_test(
    const std::string &preserve_dir,
    const Preserve_trx_physical_promotion_gate_request &request,
    Preserve_trx_physical_promotion_gate_result *result,
    const std::vector<trx_t *> *verified_transactions,
    const Preserve_trx_transfer_accepted_epoch *accepted_epoch) {
  const bool production_equivalent = verified_transactions != nullptr ||
                                     accepted_epoch != nullptr;
  if ((verified_transactions == nullptr) != (accepted_epoch == nullptr)) {
    if (result != nullptr) {
      *result = {};
      result->status =
          Preserve_trx_physical_promotion_gate_status::INVALID_ARGUMENT;
      result->message = "incomplete production-equivalent test inputs";
    }
    return Preserve_trx_physical_promotion_gate_status::INVALID_ARGUMENT;
  }
  return adopt_ready_epoch_for_physical_promotion_impl(
      preserve_dir, request, !production_equivalent, verified_transactions,
      accepted_epoch, nullptr, result);
}

Preserve_trx_promotion_adopt_status
preserved_trx_cleanup_abandoned_standby_promotion_epoch(
    const std::string &preserve_dir, const std::string &epoch_id,
    Preserve_trx_promotion_adopt_result *result) {
  const uint64_t started_us = my_micro_time();
  if (result == nullptr) {
    return Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT;
  }
  *result = {};
  if (preserve_dir.empty() || epoch_id.empty()) {
    finish_result(result, Preserve_trx_promotion_adopt_status::INVALID_ARGUMENT,
                  started_us);
    return result->status;
  }
  if (!preserve_trx_is_enabled()) {
    finish_result(result, Preserve_trx_promotion_adopt_status::NOT_ENABLED,
                  started_us);
    return result->status;
  }

  auto store = create_preserved_trx_default_store(preserve_dir);
  std::string encoded;
  Preserve_snapshot_status read_status =
      store->read_promotion_abandoned_epoch(epoch_id, &encoded);
  if (read_status != Preserve_snapshot_status::OK) {
    finish_result(result, carrier_status_to_promotion_status(read_status),
                  started_us);
    return result->status;
  }

  Preserve_trx_promotion_abandoned_epoch_marker marker;
  if (!preserved_trx_decode_promotion_abandoned_epoch_marker(encoded,
                                                             &marker)) {
    finish_result(result,
                  Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
                  started_us);
    return result->status;
  }

  Preserve_trx_promotion_abandoned_epoch_marker rewritten = marker;
  rewritten.tokens.clear();
  rewritten.generated_at_us = my_micro_time();

  for (Preserve_trx_promotion_token_result token : marker.tokens) {
    if (token.cleanup_state !=
        Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING) {
      rewritten.tokens.push_back(token);
      result->token_results.push_back(token);
      ++result->abandoned_count;
      if (token.cleanup_state ==
          Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED) {
        ++result->cleanup_failed_count;
      }
      continue;
    }

    const std::string token_string = std::to_string(token.token);
    const dberr_t rollback_status =
        trx_preserve_rollback_by_token(token_string.c_str());
    token.status = Preserve_trx_promotion_adopt_status::TOKEN_ABANDONED;
    token.claimed = rollback_status == DB_SUCCESS;
    if (rollback_status == DB_SUCCESS) {
      token.cleanup_state =
          Preserve_trx_promotion_cleanup_state::CLEANUP_ROLLED_BACK;
      token.reason = "prepared trx rolled back";
    } else if (rollback_status == DB_NOT_FOUND) {
      if (promotion_cleanup_terminal_absence_proven(token_string)) {
        token.cleanup_state =
            Preserve_trx_promotion_cleanup_state::CLEANUP_NOT_FOUND;
        token.reason = "prepared trx not found after terminal absence proof";
      } else {
        token.cleanup_state =
            Preserve_trx_promotion_cleanup_state::CLEANUP_PENDING;
        token.reason =
            "prepared trx not found; cleanup remains pending until terminal "
            "absence is proven";
        ++result->cleanup_pending_count;
      }
    } else {
      token.cleanup_state =
          Preserve_trx_promotion_cleanup_state::CLEANUP_TAINTED;
      token.reason = "prepared trx rollback failed";
      ++result->cleanup_failed_count;
    }
    rewritten.tokens.push_back(token);
    result->token_results.push_back(token);
    ++result->abandoned_count;
  }

  std::string rewritten_payload;
  if (!preserved_trx_encode_promotion_abandoned_epoch_marker(
          rewritten, &rewritten_payload)) {
    finish_result(result, Preserve_trx_promotion_adopt_status::CORRUPT_ARTIFACT,
                  started_us);
    return result->status;
  }
  const Preserve_snapshot_status write_status =
      store->write_promotion_abandoned_epoch(epoch_id, rewritten_payload);
  if (write_status != Preserve_snapshot_status::OK) {
    ++result->cleanup_failed_count;
    append_result_message(result, "failed to rewrite abandoned marker");
  }

  finish_result(result, Preserve_trx_promotion_adopt_status::OK, started_us);
  return result->status;
}

bool preserved_trx_encode_promotion_adopted_epoch_marker(
    const Preserve_trx_promotion_adopted_epoch_marker &marker,
    std::string *encoded) {
  if (encoded == nullptr || marker.epoch_id.empty() ||
      marker.source_server_uuid.empty() || marker.target_server_uuid.empty() ||
      !marker_tokens_are_valid(marker.tokens)) {
    return false;
  }
  std::string body;
  body.append(kPromotionAdoptedMarkerMagic);
  body.push_back('\n');
  if (!append_required_field("epoch_id", marker.epoch_id, &body) ||
      !append_required_field("source_server_uuid", marker.source_server_uuid,
                             &body) ||
      !append_required_field("target_server_uuid", marker.target_server_uuid,
                             &body) ||
      !append_required_field("applied_lsn", std::to_string(marker.applied_lsn),
                             &body) ||
      !append_required_field("generated_at_us",
                             std::to_string(marker.generated_at_us), &body) ||
      !append_required_field("tokens", encode_token_list(marker.tokens),
                             &body)) {
    return false;
  }

  *encoded = body;
  encoded->append("digest=");
  encoded->append(sha256_hex(body));
  encoded->push_back('\n');
  return true;
}

bool preserved_trx_decode_promotion_adopted_epoch_marker(
    const std::string &encoded,
    Preserve_trx_promotion_adopted_epoch_marker *marker) {
  if (marker == nullptr) return false;
  const size_t digest_pos = encoded.rfind("digest=");
  if (digest_pos == std::string::npos ||
      (digest_pos > 0 && encoded[digest_pos - 1] != '\n')) {
    return false;
  }
  const std::string body = encoded.substr(0, digest_pos);
  const std::string expected_digest_line = "digest=" + sha256_hex(body) + "\n";
  if (encoded.substr(digest_pos) != expected_digest_line) return false;

  const std::vector<std::string> lines = split_lines(body);
  if (lines.empty() || lines[0] != kPromotionAdoptedMarkerMagic) {
    return false;
  }

  Preserve_trx_promotion_adopted_epoch_marker parsed;
  std::string applied_lsn;
  std::string generated_at_us;
  std::string tokens;
  if (!read_marker_field(lines, "epoch_id", &parsed.epoch_id) ||
      !read_marker_field(lines, "source_server_uuid",
                         &parsed.source_server_uuid) ||
      !read_marker_field(lines, "target_server_uuid",
                         &parsed.target_server_uuid) ||
      !read_marker_field(lines, "applied_lsn", &applied_lsn) ||
      !read_marker_field(lines, "generated_at_us", &generated_at_us) ||
      !read_marker_field(lines, "tokens", &tokens) ||
      !parse_uint64_strict(applied_lsn, &parsed.applied_lsn) ||
      !parse_uint64_strict(generated_at_us, &parsed.generated_at_us) ||
      !parse_token_list(tokens, &parsed.tokens)) {
    return false;
  }
  *marker = std::move(parsed);
  return true;
}

bool preserved_trx_encode_promotion_abandoned_epoch_marker(
    const Preserve_trx_promotion_abandoned_epoch_marker &marker,
    std::string *encoded) {
  if (encoded == nullptr || marker.epoch_id.empty() ||
      marker.source_server_uuid.empty() || marker.target_server_uuid.empty() ||
      marker.tokens.empty()) {
    return false;
  }

  std::vector<Preserve_trx_promotion_token_result> tokens = marker.tokens;
  std::sort(tokens.begin(), tokens.end(),
            [](const Preserve_trx_promotion_token_result &left,
               const Preserve_trx_promotion_token_result &right) {
              return left.token < right.token;
            });
  for (const Preserve_trx_promotion_token_result &token : tokens) {
    if (!abandoned_token_is_valid(token)) return false;
  }

  std::string body;
  body.append(kPromotionAbandonedMarkerMagic);
  body.push_back('\n');
  if (!append_required_field("epoch_id", marker.epoch_id, &body) ||
      !append_required_field("source_server_uuid", marker.source_server_uuid,
                             &body) ||
      !append_required_field("target_server_uuid", marker.target_server_uuid,
                             &body) ||
      !append_required_field("applied_lsn", std::to_string(marker.applied_lsn),
                             &body) ||
      !append_required_field("generated_at_us",
                             std::to_string(marker.generated_at_us), &body) ||
      !append_required_field("abandoned_count", std::to_string(tokens.size()),
                             &body)) {
    return false;
  }

  for (size_t i = 0; i < tokens.size(); ++i) {
    if (!append_required_field("abandoned_" + std::to_string(i),
                               encode_abandoned_token(tokens[i]), &body)) {
      return false;
    }
  }

  *encoded = body;
  encoded->append("digest=");
  encoded->append(sha256_hex(body));
  encoded->push_back('\n');
  return true;
}

bool preserved_trx_decode_promotion_abandoned_epoch_marker(
    const std::string &encoded,
    Preserve_trx_promotion_abandoned_epoch_marker *marker) {
  if (marker == nullptr) return false;
  const size_t digest_pos = encoded.rfind("digest=");
  if (digest_pos == std::string::npos ||
      (digest_pos > 0 && encoded[digest_pos - 1] != '\n')) {
    return false;
  }
  const std::string body = encoded.substr(0, digest_pos);
  const std::string expected_digest_line = "digest=" + sha256_hex(body) + "\n";
  if (encoded.substr(digest_pos) != expected_digest_line) return false;

  const std::vector<std::string> lines = split_lines(body);
  if (lines.empty() || lines[0] != kPromotionAbandonedMarkerMagic) {
    return false;
  }

  Preserve_trx_promotion_abandoned_epoch_marker parsed;
  std::string applied_lsn;
  std::string generated_at_us;
  std::string abandoned_count_string;
  uint64_t abandoned_count = 0;
  if (!read_marker_field(lines, "epoch_id", &parsed.epoch_id) ||
      !read_marker_field(lines, "source_server_uuid",
                         &parsed.source_server_uuid) ||
      !read_marker_field(lines, "target_server_uuid",
                         &parsed.target_server_uuid) ||
      !read_marker_field(lines, "applied_lsn", &applied_lsn) ||
      !read_marker_field(lines, "generated_at_us", &generated_at_us) ||
      !read_marker_field(lines, "abandoned_count", &abandoned_count_string) ||
      !parse_uint64_strict(applied_lsn, &parsed.applied_lsn) ||
      !parse_uint64_strict(generated_at_us, &parsed.generated_at_us) ||
      !parse_uint64_strict(abandoned_count_string, &abandoned_count) ||
      abandoned_count == 0) {
    return false;
  }

  for (uint64_t i = 0; i < abandoned_count; ++i) {
    std::string encoded_token;
    Preserve_trx_promotion_token_result token;
    if (!read_marker_field(lines, "abandoned_" + std::to_string(i),
                           &encoded_token) ||
        !parse_abandoned_token(encoded_token, &token)) {
      return false;
    }
    parsed.tokens.push_back(std::move(token));
  }

  *marker = std::move(parsed);
  return true;
}

bool preserved_trx_encode_promotion_intent_epoch_marker(
    const Preserve_trx_promotion_intent_epoch_marker &marker,
    std::string *encoded) {
  if (encoded == nullptr || marker.epoch_id.empty() ||
      marker.source_server_uuid.empty() || marker.target_server_uuid.empty() ||
      marker.tokens.empty()) {
    return false;
  }

  std::vector<Preserve_trx_promotion_intent_token> tokens = marker.tokens;
  std::sort(tokens.begin(), tokens.end(),
            [](const Preserve_trx_promotion_intent_token &left,
               const Preserve_trx_promotion_intent_token &right) {
              return left.token < right.token;
            });
  for (const Preserve_trx_promotion_intent_token &token : tokens) {
    if (!intent_token_is_valid(token)) return false;
  }

  std::string body;
  body.append(kPromotionIntentMarkerMagic);
  body.push_back('\n');
  if (!append_required_field("epoch_id", marker.epoch_id, &body) ||
      !append_required_field("source_server_uuid", marker.source_server_uuid,
                             &body) ||
      !append_required_field("target_server_uuid", marker.target_server_uuid,
                             &body) ||
      !append_required_field("required_apply_lsn",
                             std::to_string(marker.required_apply_lsn),
                             &body) ||
      !append_required_field("generated_at_us",
                             std::to_string(marker.generated_at_us), &body) ||
      !append_required_field("token_count", std::to_string(tokens.size()),
                             &body)) {
    return false;
  }

  for (size_t i = 0; i < tokens.size(); ++i) {
    if (!append_required_field("token_" + std::to_string(i),
                               encode_intent_token(tokens[i]), &body)) {
      return false;
    }
  }

  *encoded = body;
  encoded->append("digest=");
  encoded->append(sha256_hex(body));
  encoded->push_back('\n');
  return true;
}

bool preserved_trx_decode_promotion_intent_epoch_marker(
    const std::string &encoded,
    Preserve_trx_promotion_intent_epoch_marker *marker) {
  if (marker == nullptr) return false;
  const size_t digest_pos = encoded.rfind("digest=");
  if (digest_pos == std::string::npos ||
      (digest_pos > 0 && encoded[digest_pos - 1] != '\n')) {
    return false;
  }
  const std::string body = encoded.substr(0, digest_pos);
  const std::string expected_digest_line = "digest=" + sha256_hex(body) + "\n";
  if (encoded.substr(digest_pos) != expected_digest_line) return false;

  const std::vector<std::string> lines = split_lines(body);
  if (lines.empty() || lines[0] != kPromotionIntentMarkerMagic) {
    return false;
  }

  Preserve_trx_promotion_intent_epoch_marker parsed;
  std::string required_apply_lsn;
  std::string generated_at_us;
  std::string token_count_string;
  uint64_t token_count = 0;
  if (!read_marker_field(lines, "epoch_id", &parsed.epoch_id) ||
      !read_marker_field(lines, "source_server_uuid",
                         &parsed.source_server_uuid) ||
      !read_marker_field(lines, "target_server_uuid",
                         &parsed.target_server_uuid) ||
      !read_marker_field(lines, "required_apply_lsn",
                         &required_apply_lsn) ||
      !read_marker_field(lines, "generated_at_us", &generated_at_us) ||
      !read_marker_field(lines, "token_count", &token_count_string) ||
      !parse_uint64_strict(required_apply_lsn, &parsed.required_apply_lsn) ||
      !parse_uint64_strict(generated_at_us, &parsed.generated_at_us) ||
      !parse_uint64_strict(token_count_string, &token_count) ||
      token_count == 0) {
    return false;
  }

  for (uint64_t i = 0; i < token_count; ++i) {
    std::string encoded_token;
    Preserve_trx_promotion_intent_token token;
    if (!read_marker_field(lines, "token_" + std::to_string(i),
                           &encoded_token) ||
        !parse_intent_token(encoded_token, &token)) {
      return false;
    }
    parsed.tokens.push_back(std::move(token));
  }

  *marker = std::move(parsed);
  return true;
}
