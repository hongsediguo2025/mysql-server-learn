/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation. */

#include "sql/preserve_trx_promotion_prepared.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <limits>
#include <map>
#include <mutex>
#include <utility>
#include <vector>

#include <openssl/sha.h>

#include "sql/binlog_preserve_prepared.h"
#include "sql/preserve_trx_bundle.h"
#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_resurrection_index.h"
#include "storage/innobase/include/trx0preserve.h"

struct Preserve_trx_targeted_publication_revocation_state {
  std::atomic<bool> active{true};
};

class Preserve_trx_physical_fence_lease_factory {
 public:
  static void install(
      Preserve_trx_physical_fence_lease *lease,
      const Preserve_trx_physical_fence_provider_ops &ops,
      const Preserve_trx_physical_fence_proof &expected,
      Preserve_trx_physical_fence_proof actual, void *opaque_lease) {
    lease->m_ops = ops;
    lease->m_expected = expected;
    lease->m_proof = std::move(actual);
    lease->m_opaque_lease = opaque_lease;
    lease->m_targeted_publication_state =
        std::make_shared<Preserve_trx_targeted_publication_revocation_state>();
  }
};

class Preserve_trx_prepared_token_resources::Impl {
 public:
  Preserve_memory_lease lock_plan_memory;
  Preserve_memory_lease semantic_bundle_memory;
  Preserve_native_binlog_resource_lease native_binlog_resources;
  std::unique_ptr<lock_preserve_metadata_plan_t> record_lock_plan;
  std::unique_ptr<Preserved_trx_bundle> semantic_bundle;
  std::unique_ptr<Preserve_trx_resurrection_index_entry> resurrection_entry;
  std::unique_ptr<trx_preserve_targeted_publication_journal>
      targeted_publication_journal;
  std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle>
      native_binlog_handle;
  Preserve_trx_prepared_token_key key;
  uint64_t lock_plan_bytes{0};
  uint64_t native_binlog_bytes{0};
  bool acquired{false};
};

struct Preserve_trx_prepared_token_publication {
  Preserve_trx_prepared_token_key key;
  Preserve_trx_final_token_facts facts;
};

struct Preserve_trx_prepared_token_entry {
  std::mutex mutex;
  std::atomic<Preserve_trx_prepared_token_state> state{
      Preserve_trx_prepared_token_state::OBJECTS_RECEIVING};
  Preserve_trx_prepared_token_key key;
  std::shared_ptr<const Preserve_trx_prepared_token_publication> publication;
  Preserve_trx_prepared_token_resources resources;
  std::string prewarm_object_set_digest;
  bool preparing{false};
  bool retired_from_registry{false};
  uint64_t preparing_generation{0};
  uint32_t physical_promotion_pins{0};
};

struct Preserve_trx_prepared_token_locator {
  std::string epoch_scope;
  std::string epoch_id;
  std::string token;

  bool operator<(const Preserve_trx_prepared_token_locator &other) const {
    if (epoch_scope != other.epoch_scope) return epoch_scope < other.epoch_scope;
    if (epoch_id != other.epoch_id) return epoch_id < other.epoch_id;
    return token < other.token;
  }
};

struct Preserve_trx_prepared_registry_state {
  std::mutex mutex;
  std::map<Preserve_trx_prepared_token_locator,
           std::shared_ptr<Preserve_trx_prepared_token_entry>>
      entries;
};

namespace {
bool prepared_token_keys_match(const Preserve_trx_prepared_token_key &lhs,
                               const Preserve_trx_prepared_token_key &rhs);
}  // namespace

Preserve_trx_physical_promotion_pin_lease::
    Preserve_trx_physical_promotion_pin_lease(
        Preserve_trx_physical_promotion_pin_lease &&other) noexcept
    : m_entries(std::move(other.m_entries)),
      m_guards(std::move(other.m_guards)),
      m_active(std::exchange(other.m_active, false)) {}

Preserve_trx_physical_promotion_pin_lease &
Preserve_trx_physical_promotion_pin_lease::operator=(
    Preserve_trx_physical_promotion_pin_lease &&other) noexcept {
  if (this != &other) {
    release_for_abandon();
    m_entries = std::move(other.m_entries);
    m_guards = std::move(other.m_guards);
    m_active = std::exchange(other.m_active, false);
  }
  return *this;
}

Preserve_trx_physical_promotion_pin_lease::
    ~Preserve_trx_physical_promotion_pin_lease() {
  release_for_abandon();
}

Preserve_trx_prepared_status
Preserve_trx_physical_promotion_pin_lease::renew_client_resume_deadline(
    uint64_t now_monotonic_us, uint64_t deadline_monotonic_us) {
  if (!m_active || m_entries.empty() || now_monotonic_us == 0 ||
      deadline_monotonic_us <= now_monotonic_us) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  std::vector<std::unique_lock<std::mutex>> guards;
  std::vector<std::shared_ptr<const Preserve_trx_prepared_token_publication>>
      publications;
  try {
    guards.reserve(m_entries.size());
    for (const auto &entry : m_entries) guards.emplace_back(entry->mutex);
    publications.reserve(m_entries.size());
    for (const auto &entry : m_entries) {
      const auto publication = std::atomic_load_explicit(
          &entry->publication, std::memory_order_acquire);
      if (entry->retired_from_registry || entry->physical_promotion_pins == 0 ||
          entry->state.load(std::memory_order_acquire) !=
              Preserve_trx_prepared_token_state::ADOPTED_LOCKED ||
          publication == nullptr ||
          !prepared_token_keys_match(publication->key, entry->key)) {
        return Preserve_trx_prepared_status::INVALID_STATE;
      }
      Preserve_trx_final_token_facts facts = publication->facts;
      facts.client_resume_deadline_us =
          std::max(facts.client_resume_deadline_us, deadline_monotonic_us);
      facts.canonical_digest.clear();
      if (!preserved_trx_finalize_token_facts(&facts)) {
        return Preserve_trx_prepared_status::INVALID_STATE;
      }
      publications.push_back(
          std::make_shared<const Preserve_trx_prepared_token_publication>(
              Preserve_trx_prepared_token_publication{publication->key,
                                                      std::move(facts)}));
    }
    for (size_t index = 0; index < m_entries.size(); ++index) {
      std::atomic_store_explicit(&m_entries[index]->publication,
                                 std::move(publications[index]),
                                 std::memory_order_release);
    }
  } catch (const std::bad_alloc &) {
    return Preserve_trx_prepared_status::RESOURCE_EXHAUSTED;
  }
  return Preserve_trx_prepared_status::OK;
}

void Preserve_trx_physical_promotion_pin_lease::release_atomically() noexcept {
  if (!m_active) return;
  m_guards.clear();
  DBUG_ASSERT(m_guards.capacity() >= m_entries.size());
  for (const auto &entry : m_entries) m_guards.emplace_back(entry->mutex);
  for (const auto &entry : m_entries) {
    DBUG_ASSERT(entry->physical_promotion_pins != 0);
    if (entry->physical_promotion_pins != 0) {
      --entry->physical_promotion_pins;
    }
  }
  m_active = false;
  m_guards.clear();
  m_entries.clear();
}

void Preserve_trx_physical_promotion_pin_lease::release_for_abandon() noexcept {
  if (!m_active) return;
  m_guards.clear();
  for (const auto &entry : m_entries) {
    std::lock_guard<std::mutex> guard(entry->mutex);
    DBUG_ASSERT(entry->physical_promotion_pins != 0);
    if (entry->physical_promotion_pins != 0) {
      --entry->physical_promotion_pins;
    }
  }
  m_active = false;
  m_entries.clear();
}

namespace {

void copy_semantic_binlog_configuration(
    const Preserved_trx_bundle *bundle,
    Preserve_trx_prepared_token_snapshot *snapshot) {
  snapshot->semantic_global_log_bin =
      bundle != nullptr && bundle->metadata.global_log_bin;
  snapshot->semantic_has_binlog_gtid_mode =
      bundle != nullptr && bundle->metadata.has_binlog_gtid_mode;
  snapshot->semantic_binlog_gtid_mode =
      bundle == nullptr ? 0 : bundle->metadata.binlog_gtid_mode;
}

uint64_t prepared_monotonic_us() {
  using clock = std::chrono::steady_clock;
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          clock::now().time_since_epoch())
          .count());
}

constexpr char kStrictAttachIntentProductV1Magic[] =
    "PTRX_STRICT_PROMOTION_ATTACH_INTENT_PRODUCT_V1";
constexpr size_t kStrictAttachIntentDigestHexLength = SHA256_DIGEST_LENGTH * 2;
constexpr size_t kStrictAttachIntentMaxBytes = 64U * 1024U * 1024U;
constexpr uint32_t kStrictEpochMaxTokens = 1000000;
constexpr uint32_t kStrictAttachIntentMaxStringBytes = 4096;

constexpr std::array<uint64_t, 20> kResumeCoreHistogramUpperBoundsUs{{
    10,
    25,
    50,
    100,
    250,
    500,
    1000,
    2500,
    5000,
    10000,
    25000,
    50000,
    100000,
    250000,
    500000,
    1000000,
    2500000,
    5000000,
    10000000,
    std::numeric_limits<uint64_t>::max(),
}};

std::mutex g_provider_mutex;
bool g_test_provider_registered{false};
Preserve_trx_physical_fence_provider_ops g_test_provider;

std::atomic<uint64_t> g_resume_core_elapsed_us{0};
std::atomic<uint64_t> g_resume_core_count{0};
std::array<std::atomic<uint64_t>, kResumeCoreHistogramUpperBoundsUs.size()>
    g_resume_core_histogram{};
std::atomic<uint64_t> g_resume_core_max_us{0};
std::atomic<uint64_t> g_resume_failure_count{0};
std::atomic<uint64_t> g_resume_physical_consistency_mode{0};
std::atomic<uint64_t> g_resume_real_redo_apply{0};
std::atomic<uint64_t> g_promotion_fence_lease_wait_us{0};
std::atomic<uint64_t> g_promotion_fence_digest_compare_us{0};
std::atomic<uint64_t> g_promotion_fence_revalidate_us{0};
std::atomic<uint64_t> g_promotion_lock_page_get_count{0};
std::atomic<uint64_t> g_promotion_lock_page_get_us{0};
std::atomic<uint64_t> g_promotion_lock_image_resolves{0};
std::atomic<uint64_t> g_promotion_lock_apply_us{0};
std::atomic<uint64_t> g_promotion_lock_accounting_bits{0};
std::atomic<uint64_t> g_receiver_lock_plan_capacity_bytes{0};
std::atomic<uint64_t> g_receiver_lock_plan_epoch_peak_bytes{0};
std::atomic<uint64_t> g_receiver_lock_plan_subpool_cap_bytes{0};
std::atomic<uint64_t> g_resource_admission_open_failed_count{0};
std::atomic<uint64_t> g_resume_binlog_payload_read_bytes{0};
std::atomic<uint64_t> g_resume_binlog_payload_write_bytes{0};
std::atomic<uint64_t> g_resume_binlog_rename_count{0};
std::atomic<uint64_t> g_resume_binlog_attach_count{0};

#ifndef NDEBUG
Preserve_trx_prepared_registry_probe g_prepared_registry_probe{nullptr};
void *g_prepared_registry_probe_context{nullptr};

void run_prepared_registry_probe(
    Preserve_trx_prepared_registry_probe_point point) {
  if (g_prepared_registry_probe != nullptr) {
    g_prepared_registry_probe(point, g_prepared_registry_probe_context);
  }
}
#endif

bool provider_ops_are_complete(
    const Preserve_trx_physical_fence_provider_ops &ops) {
  return ops.acquire != nullptr && ops.revalidate != nullptr &&
         ops.release != nullptr;
}

bool physical_consistency_mode_is_valid(
    Preserve_trx_physical_consistency_mode mode) {
  switch (mode) {
    case Preserve_trx_physical_consistency_mode::TEST_SAME_INSTANCE_ATTACH_ONLY:
    case Preserve_trx_physical_consistency_mode::
        TEST_ONLY_PHYSICAL_FENCE_SIMULATOR:
    case Preserve_trx_physical_consistency_mode::PRODUCTION_REDO_APPLY_FENCE:
      return true;
    case Preserve_trx_physical_consistency_mode::NONE:
      return false;
  }
  return false;
}

bool digest_is_sha256_hex(const std::string &digest) {
  if (digest.size() != 64) return false;
  for (const unsigned char ch : digest) {
    if (!std::isxdigit(ch)) return false;
  }
  return true;
}

bool proofs_match(const Preserve_trx_physical_fence_proof &expected,
                  const Preserve_trx_physical_fence_proof &actual) {
  return expected.consistency_mode == actual.consistency_mode &&
         expected.source_lineage_uuid == actual.source_lineage_uuid &&
         expected.target_server_uuid == actual.target_server_uuid &&
         expected.target_boot_incarnation == actual.target_boot_incarnation &&
         expected.provider_generation == actual.provider_generation &&
         expected.source_fence_lsn == actual.source_fence_lsn &&
         expected.target_frozen_lsn == actual.target_frozen_lsn &&
         expected.epoch_fact_digest == actual.epoch_fact_digest &&
         expected.final_lock_generation_digest ==
             actual.final_lock_generation_digest &&
         expected.page_layout_digest == actual.page_layout_digest &&
         expected.dictionary_generation_digest ==
             actual.dictionary_generation_digest &&
         actual.apply_frozen && actual.implicit_native_continuity_proven;
}

bool prepared_token_key_is_valid(const Preserve_trx_prepared_token_key &key) {
  return !key.preserve_dir.empty() && !key.epoch_scope.empty() &&
         !key.epoch_id.empty() && !key.token.empty() &&
         !key.target_boot_incarnation.empty() && key.generation != 0;
}

Preserve_trx_prepared_token_locator prepared_token_locator(
    const Preserve_trx_prepared_token_key &key) {
  return {key.epoch_scope, key.epoch_id, key.token};
}

bool prepared_token_keys_match(const Preserve_trx_prepared_token_key &lhs,
                               const Preserve_trx_prepared_token_key &rhs) {
  return lhs.preserve_dir == rhs.preserve_dir &&
         lhs.epoch_scope == rhs.epoch_scope && lhs.epoch_id == rhs.epoch_id &&
         lhs.token == rhs.token &&
         lhs.target_boot_incarnation == rhs.target_boot_incarnation &&
         lhs.generation == rhs.generation;
}

bool prepared_state_accepts_generation(
    Preserve_trx_prepared_token_state state) {
  switch (state) {
    case Preserve_trx_prepared_token_state::OBJECTS_RECEIVING:
    case Preserve_trx_prepared_token_state::PREWARMING:
    case Preserve_trx_prepared_token_state::PREWARMED_PENDING_FINAL_FACT:
    case Preserve_trx_prepared_token_state::READY_FACTS_PENDING_LEASE:
    case Preserve_trx_prepared_token_state::READY_FOR_GATE:
    case Preserve_trx_prepared_token_state::NOT_READY:
    case Preserve_trx_prepared_token_state::RESOURCE_EXHAUSTED:
    case Preserve_trx_prepared_token_state::STALE_GENERATION:
      return true;
    default:
      return false;
  }
}

bool prepared_state_has_live_or_ambiguous_owner(
    Preserve_trx_prepared_token_state state) {
  switch (state) {
    case Preserve_trx_prepared_token_state::ADOPTING:
    case Preserve_trx_prepared_token_state::ADOPTED_LOCKED:
    case Preserve_trx_prepared_token_state::ATTACHING:
    case Preserve_trx_prepared_token_state::ACTIVATING:
    case Preserve_trx_prepared_token_state::ACTIVE:
    case Preserve_trx_prepared_token_state::CLEANUP_PENDING:
    case Preserve_trx_prepared_token_state::CLEANUP_TAINTED:
    case Preserve_trx_prepared_token_state::ATTACH_TAINTED:
      return true;
    default:
      return false;
  }
}

std::shared_ptr<Preserve_trx_prepared_token_entry> find_prepared_entry(
    const std::shared_ptr<Preserve_trx_prepared_registry_state> &registry,
    const Preserve_trx_prepared_token_key &key) {
  if (registry == nullptr) return nullptr;
  std::lock_guard<std::mutex> guard(registry->mutex);
  const auto it = registry->entries.find(prepared_token_locator(key));
  return it == registry->entries.end() ? nullptr : it->second;
}

void append_canonical_u32(std::string *encoded, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    encoded->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

void append_canonical_u64(std::string *encoded, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    encoded->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

bool append_canonical_string(std::string *encoded, const std::string &value) {
  if (value.size() > std::numeric_limits<uint32_t>::max()) return false;
  append_canonical_u32(encoded, static_cast<uint32_t>(value.size()));
  encoded->append(value);
  return true;
}

void append_canonical_bool(std::string *encoded, bool value) {
  encoded->push_back(value ? '\x01' : '\x00');
}

bool read_canonical_u8(const std::string &encoded, size_t *offset,
                       uint8_t *value) {
  if (offset == nullptr || value == nullptr || *offset >= encoded.size()) {
    return false;
  }
  *value = static_cast<uint8_t>(encoded[*offset]);
  ++*offset;
  return true;
}

bool read_canonical_u32(const std::string &encoded, size_t *offset,
                        uint32_t *value) {
  if (offset == nullptr || value == nullptr ||
      encoded.size() - std::min(*offset, encoded.size()) < sizeof(uint32_t)) {
    return false;
  }
  uint32_t decoded = 0;
  for (size_t i = 0; i < sizeof(uint32_t); ++i) {
    decoded = (decoded << 8) |
              static_cast<unsigned char>(encoded[*offset + i]);
  }
  *offset += sizeof(uint32_t);
  *value = decoded;
  return true;
}

bool read_canonical_u64(const std::string &encoded, size_t *offset,
                        uint64_t *value) {
  if (offset == nullptr || value == nullptr ||
      encoded.size() - std::min(*offset, encoded.size()) < sizeof(uint64_t)) {
    return false;
  }
  uint64_t decoded = 0;
  for (size_t i = 0; i < sizeof(uint64_t); ++i) {
    decoded = (decoded << 8) |
              static_cast<unsigned char>(encoded[*offset + i]);
  }
  *offset += sizeof(uint64_t);
  *value = decoded;
  return true;
}

bool read_canonical_string(const std::string &encoded, size_t *offset,
                           std::string *value) {
  uint32_t length = 0;
  if (value == nullptr || !read_canonical_u32(encoded, offset, &length) ||
      length == 0 || length > kStrictAttachIntentMaxStringBytes ||
      encoded.size() - std::min(*offset, encoded.size()) < length) {
    return false;
  }
  value->assign(encoded.data() + *offset, length);
  *offset += length;
  return true;
}

bool strict_attach_intent_state_is_valid(
    Preserve_trx_strict_attach_intent_state state) {
  switch (state) {
    case Preserve_trx_strict_attach_intent_state::ATTACHING:
    case Preserve_trx_strict_attach_intent_state::ACTIVATING:
    case Preserve_trx_strict_attach_intent_state::ACTIVE:
    case Preserve_trx_strict_attach_intent_state::ATTACH_ROLLED_BACK:
    case Preserve_trx_strict_attach_intent_state::ATTACH_TAINTED:
    case Preserve_trx_strict_attach_intent_state::CLEANUP_PENDING:
    case Preserve_trx_strict_attach_intent_state::CLEANUP_ROLLED_BACK:
    case Preserve_trx_strict_attach_intent_state::CLEANUP_TAINTED:
      return true;
  }
  return false;
}

bool strict_attach_intent_is_valid(
    const Preserve_trx_strict_attach_intent &intent) {
  const auto string_is_valid = [](const std::string &value) {
    return !value.empty() &&
           value.size() <= kStrictAttachIntentMaxStringBytes;
  };
  return prepared_token_key_is_valid(intent.key) &&
         string_is_valid(intent.key.preserve_dir) &&
         string_is_valid(intent.key.epoch_scope) &&
         string_is_valid(intent.key.epoch_id) &&
         string_is_valid(intent.key.token) &&
         string_is_valid(intent.key.target_boot_incarnation) &&
         strict_attach_intent_state_is_valid(intent.state) &&
         intent.target_connection_id != 0 && intent.generated_at_us != 0;
}

std::string sha256_hex_string(const std::string &payload) {
  std::array<unsigned char, SHA256_DIGEST_LENGTH> digest{};
  SHA256(reinterpret_cast<const unsigned char *>(payload.data()),
         payload.size(), digest.data());
  static constexpr char kHex[] = "0123456789abcdef";
  std::string encoded;
  encoded.reserve(digest.size() * 2);
  for (const unsigned char byte : digest) {
    encoded.push_back(kHex[(byte >> 4) & 0x0f]);
    encoded.push_back(kHex[byte & 0x0f]);
  }
  return encoded;
}

bool final_token_facts_are_valid(const Preserve_trx_final_token_facts &facts) {
  return facts.required_apply_lsn != 0 &&
         facts.physical_fence_lsn != 0 &&
         facts.required_apply_lsn <= facts.physical_fence_lsn &&
         facts.source_trx_id_store != 0 &&
         facts.source_trx_id_store_lsn != 0 &&
         facts.source_trx_id_store_lsn <= facts.physical_fence_lsn &&
         facts.source_safe_next_trx_id_floor >= facts.source_trx_id_store &&
         digest_is_sha256_hex(facts.epoch_fact_digest) &&
         digest_is_sha256_hex(facts.final_lock_generation_digest) &&
         digest_is_sha256_hex(facts.page_layout_digest) &&
         digest_is_sha256_hex(facts.dictionary_generation_digest) &&
         digest_is_sha256_hex(facts.prewarm_object_set_digest) &&
         !facts.target_boot_incarnation.empty() && facts.semantic_validated &&
         facts.lock_plan_ready && facts.binlog_handle_ready &&
         facts.resources_reserved && facts.epoch_prepare_deadline_us != 0 &&
         facts.client_resume_deadline_us != 0 &&
         ((!facts.binlog_cache_present && facts.binlog_handle_digest.empty()) ||
          (facts.binlog_cache_present &&
           digest_is_sha256_hex(facts.binlog_handle_digest)));
}

std::string prepared_resource_token(
    const Preserve_trx_prepared_token_key &key) {
  return key.epoch_scope + "\x1f" + key.epoch_id + "\x1f" + key.token +
         "\x1f" + std::to_string(key.generation);
}

Mysql_binlog_preserve_token_identity prepared_binlog_identity(
    const Preserve_trx_prepared_token_key &key) {
  Mysql_binlog_preserve_token_identity identity;
  identity.epoch_scope = key.epoch_scope;
  identity.epoch_id = key.epoch_id;
  identity.token = key.token;
  identity.target_boot_incarnation = key.target_boot_incarnation;
  identity.generation = key.generation;
  return identity;
}

Preserve_trx_physical_fence_status acquire_with_provider(
    const Preserve_trx_physical_fence_provider_ops *ops,
    const Preserve_trx_physical_fence_proof &expected,
    Preserve_trx_physical_fence_lease *lease) {
  if (ops == nullptr) {
    return Preserve_trx_physical_fence_status::MISSING_PROVIDER;
  }
  if (lease == nullptr || lease->acquired() ||
      !preserved_trx_physical_fence_proof_is_valid(expected)) {
    return Preserve_trx_physical_fence_status::INVALID_ARGUMENT;
  }
  if (ops->consistency_mode != expected.consistency_mode) {
    return Preserve_trx_physical_fence_status::MODE_MISMATCH;
  }

  Preserve_trx_physical_fence_proof actual;
  void *opaque_lease = nullptr;
  if (!ops->acquire(expected, &actual, &opaque_lease) ||
      opaque_lease == nullptr) {
    if (opaque_lease != nullptr) ops->release(opaque_lease);
    return Preserve_trx_physical_fence_status::ACQUIRE_FAILED;
  }
  if (!preserved_trx_physical_fence_proof_is_valid(actual) ||
      !proofs_match(expected, actual)) {
    ops->release(opaque_lease);
    return Preserve_trx_physical_fence_status::PROVIDER_CONTRACT_VIOLATION;
  }

  Preserve_trx_physical_fence_lease_factory::install(
      lease, *ops, expected, std::move(actual), opaque_lease);
  return Preserve_trx_physical_fence_status::OK;
}

void update_max(std::atomic<uint64_t> *target, uint64_t value) {
  uint64_t current = target->load(std::memory_order_relaxed);
  while (current < value &&
         !target->compare_exchange_weak(current, value,
                                        std::memory_order_relaxed,
                                        std::memory_order_relaxed)) {
  }
}

uint64_t resume_core_percentile(uint64_t numerator, uint64_t denominator) {
  const uint64_t count = g_resume_core_count.load(std::memory_order_relaxed);
  if (count == 0) return 0;
  const uint64_t rank = (count * numerator + denominator - 1) / denominator;
  uint64_t cumulative = 0;
  for (size_t i = 0; i < g_resume_core_histogram.size(); ++i) {
    cumulative += g_resume_core_histogram[i].load(std::memory_order_relaxed);
    if (cumulative >= rank) return kResumeCoreHistogramUpperBoundsUs[i];
  }
  return kResumeCoreHistogramUpperBoundsUs.back();
}

}  // namespace

uint64_t preserved_trx_promotion_prepared_monotonic_us_for_unit_test() {
  return prepared_monotonic_us();
}

bool preserved_trx_compute_epoch_physical_digest_commitments(
    const std::vector<Preserve_trx_epoch_physical_digest_input> &inputs,
    std::string *final_lock_generation_digest,
    std::string *page_layout_digest,
    std::string *dictionary_generation_digest) {
  if (inputs.empty() || inputs.size() > kStrictEpochMaxTokens ||
      final_lock_generation_digest == nullptr ||
      page_layout_digest == nullptr ||
      dictionary_generation_digest == nullptr) {
    return false;
  }
  std::vector<Preserve_trx_epoch_physical_digest_input> sorted = inputs;
  std::sort(sorted.begin(), sorted.end(),
            [](const auto &left, const auto &right) {
              return left.token != right.token
                         ? left.token < right.token
                         : left.generation < right.generation;
            });

  std::string lock_commitment("PTRX_FINAL_LOCK_EPOCH_V1");
  std::string page_commitment("PTRX_PAGE_LAYOUT_EPOCH_V1");
  std::string dictionary_commitment("PTRX_DICTIONARY_EPOCH_V1");
  append_canonical_u32(&lock_commitment,
                       static_cast<uint32_t>(sorted.size()));
  append_canonical_u32(&page_commitment,
                       static_cast<uint32_t>(sorted.size()));
  append_canonical_u32(&dictionary_commitment,
                       static_cast<uint32_t>(sorted.size()));
  for (size_t i = 0; i < sorted.size(); ++i) {
    const auto &input = sorted[i];
    if (input.token.empty() || input.generation == 0 ||
        !digest_is_sha256_hex(input.final_lock_generation_digest) ||
        !digest_is_sha256_hex(input.page_layout_digest) ||
        !digest_is_sha256_hex(input.dictionary_generation_digest) ||
        (i != 0 && input.token == sorted[i - 1].token)) {
      return false;
    }
    for (std::string *commitment : {&lock_commitment, &page_commitment,
                                    &dictionary_commitment}) {
      if (!append_canonical_string(commitment, input.token)) return false;
      append_canonical_u64(commitment, input.generation);
    }
    if (!append_canonical_string(&lock_commitment,
                                 input.final_lock_generation_digest) ||
        !append_canonical_string(&page_commitment,
                                 input.page_layout_digest) ||
        !append_canonical_string(&dictionary_commitment,
                                 input.dictionary_generation_digest)) {
      return false;
    }
  }
  *final_lock_generation_digest = sha256_hex_string(lock_commitment);
  *page_layout_digest = sha256_hex_string(page_commitment);
  *dictionary_generation_digest = sha256_hex_string(dictionary_commitment);
  return true;
}

Preserve_trx_physical_fence_lease::Preserve_trx_physical_fence_lease(
    Preserve_trx_physical_fence_lease &&other) noexcept
    : m_ops(other.m_ops),
      m_expected(std::move(other.m_expected)),
      m_proof(std::move(other.m_proof)),
      m_opaque_lease(other.m_opaque_lease),
      m_targeted_publication_state(
          std::move(other.m_targeted_publication_state)) {
  other.m_ops = {};
  other.m_opaque_lease = nullptr;
}

Preserve_trx_physical_fence_lease &
Preserve_trx_physical_fence_lease::operator=(
    Preserve_trx_physical_fence_lease &&other) noexcept {
  if (this == &other) return *this;
  release();
  m_ops = other.m_ops;
  m_expected = std::move(other.m_expected);
  m_proof = std::move(other.m_proof);
  m_opaque_lease = other.m_opaque_lease;
  m_targeted_publication_state =
      std::move(other.m_targeted_publication_state);
  other.m_ops = {};
  other.m_opaque_lease = nullptr;
  return *this;
}

Preserve_trx_physical_fence_lease::~Preserve_trx_physical_fence_lease() {
  release();
}

Preserve_trx_physical_fence_status
Preserve_trx_physical_fence_lease::revalidate() {
  std::lock_guard<std::mutex> guard(g_provider_mutex);
  if (!acquired() || m_ops.revalidate == nullptr) {
    return Preserve_trx_physical_fence_status::INVALID_ARGUMENT;
  }
  Preserve_trx_physical_fence_proof actual;
  if (!m_ops.revalidate(m_opaque_lease, m_expected.provider_generation,
                        &actual)) {
    return Preserve_trx_physical_fence_status::REVALIDATE_FAILED;
  }
  if (!preserved_trx_physical_fence_proof_is_valid(actual) ||
      !proofs_match(m_expected, actual)) {
    return Preserve_trx_physical_fence_status::PROVIDER_CONTRACT_VIOLATION;
  }
  m_proof = std::move(actual);
  return Preserve_trx_physical_fence_status::OK;
}

bool Preserve_trx_targeted_publication_capability::valid_for(
    const Preserve_trx_prepared_token_key &key) const {
  const auto state = m_state.lock();
  return state != nullptr && state->active.load(std::memory_order_acquire) &&
         prepared_token_key_is_valid(key) &&
         key.epoch_scope == m_epoch_scope && key.epoch_id == m_epoch_id &&
         key.token == m_token &&
         key.target_boot_incarnation == m_target_boot_incarnation &&
         key.generation == m_generation && m_source_fence_lsn != 0 &&
         m_provider_generation != 0;
}

bool Preserve_trx_physical_fence_lease::
    make_targeted_publication_capability(
        const Preserve_trx_prepared_token_key &key,
        Preserve_trx_targeted_publication_capability *capability) const {
  if (capability == nullptr) return false;
  *capability = {};
  if (!acquired() ||
      m_proof.consistency_mode != Preserve_trx_physical_consistency_mode::
                                      TEST_ONLY_PHYSICAL_FENCE_SIMULATOR ||
      m_targeted_publication_state == nullptr ||
      !m_targeted_publication_state->active.load(std::memory_order_acquire) ||
      !prepared_token_key_is_valid(key) ||
      key.epoch_scope != m_proof.source_lineage_uuid ||
      key.target_boot_incarnation != m_proof.target_boot_incarnation) {
    return false;
  }
  capability->m_state = m_targeted_publication_state;
  capability->m_epoch_scope = key.epoch_scope;
  capability->m_epoch_id = key.epoch_id;
  capability->m_token = key.token;
  capability->m_target_boot_incarnation = key.target_boot_incarnation;
  capability->m_generation = key.generation;
  capability->m_source_fence_lsn = m_proof.source_fence_lsn;
  capability->m_provider_generation = m_proof.provider_generation;
  return true;
}

void Preserve_trx_physical_fence_lease::release() {
  if (m_targeted_publication_state != nullptr) {
    m_targeted_publication_state->active.store(false,
                                                std::memory_order_release);
  }
  if (m_opaque_lease != nullptr && m_ops.release != nullptr) {
    m_ops.release(m_opaque_lease);
  }
  m_ops = {};
  m_expected = {};
  m_proof = {};
  m_opaque_lease = nullptr;
  m_targeted_publication_state.reset();
}

void preserved_trx_set_physical_fence_provider_for_unit_test(
    const Preserve_trx_physical_fence_provider_ops *ops) {
  std::lock_guard<std::mutex> guard(g_provider_mutex);
  g_test_provider = {};
  g_test_provider_registered = false;
  if (ops == nullptr || !provider_ops_are_complete(*ops) ||
      ops->consistency_mode ==
          Preserve_trx_physical_consistency_mode::PRODUCTION_REDO_APPLY_FENCE ||
      ops->consistency_mode == Preserve_trx_physical_consistency_mode::NONE) {
    return;
  }
  g_test_provider = *ops;
  g_test_provider_registered = true;
}

Preserve_trx_physical_fence_status
preserved_trx_acquire_physical_fence_lease_for_unit_test(
    const Preserve_trx_physical_fence_proof &expected,
    Preserve_trx_physical_fence_lease *lease) {
  Preserve_trx_physical_fence_provider_ops ops;
  {
    std::lock_guard<std::mutex> guard(g_provider_mutex);
    if (!g_test_provider_registered) {
      return Preserve_trx_physical_fence_status::MISSING_PROVIDER;
    }
    ops = g_test_provider;
  }
  return acquire_with_provider(&ops, expected, lease);
}

bool preserved_trx_physical_fence_proof_is_valid(
    const Preserve_trx_physical_fence_proof &proof) {
  return physical_consistency_mode_is_valid(proof.consistency_mode) &&
         !proof.source_lineage_uuid.empty() &&
         !proof.target_server_uuid.empty() &&
         !proof.target_boot_incarnation.empty() &&
         proof.provider_generation != 0 && proof.source_fence_lsn != 0 &&
         proof.target_frozen_lsn == proof.source_fence_lsn &&
         digest_is_sha256_hex(proof.epoch_fact_digest) &&
         digest_is_sha256_hex(proof.final_lock_generation_digest) &&
         digest_is_sha256_hex(proof.page_layout_digest) &&
         digest_is_sha256_hex(proof.dictionary_generation_digest) &&
         proof.apply_frozen && proof.implicit_native_continuity_proven;
}

bool preserved_trx_encode_strict_attach_intent_v1(
    const Preserve_trx_strict_attach_intent &intent, std::string *encoded) {
  if (encoded == nullptr || !strict_attach_intent_is_valid(intent)) {
    return false;
  }
  std::string body(kStrictAttachIntentProductV1Magic,
                   sizeof(kStrictAttachIntentProductV1Magic) - 1);
  if (!append_canonical_string(&body, intent.key.preserve_dir) ||
      !append_canonical_string(&body, intent.key.epoch_scope) ||
      !append_canonical_string(&body, intent.key.epoch_id) ||
      !append_canonical_string(&body, intent.key.token) ||
      !append_canonical_string(&body,
                               intent.key.target_boot_incarnation)) {
    return false;
  }
  append_canonical_u64(&body, intent.key.generation);
  body.push_back(static_cast<char>(intent.state));
  append_canonical_u64(&body, intent.target_connection_id);
  append_canonical_u64(&body, intent.generated_at_us);
  if (body.size() >
      kStrictAttachIntentMaxBytes - kStrictAttachIntentDigestHexLength) {
    return false;
  }
  *encoded = body;
  encoded->append(sha256_hex_string(body));
  return true;
}

bool preserved_trx_decode_strict_attach_intent_v1(
    const std::string &encoded, Preserve_trx_strict_attach_intent *intent) {
  constexpr size_t kMagicLength =
      sizeof(kStrictAttachIntentProductV1Magic) - 1;
  if (intent == nullptr ||
      encoded.size() <= kMagicLength + kStrictAttachIntentDigestHexLength ||
      encoded.size() > kStrictAttachIntentMaxBytes) {
    return false;
  }
  const size_t body_length =
      encoded.size() - kStrictAttachIntentDigestHexLength;
  const std::string body = encoded.substr(0, body_length);
  if (body.compare(0, kMagicLength, kStrictAttachIntentProductV1Magic) != 0 ||
      encoded.compare(body_length, kStrictAttachIntentDigestHexLength,
                      sha256_hex_string(body)) != 0) {
    return false;
  }

  Preserve_trx_strict_attach_intent parsed;
  size_t offset = kMagicLength;
  uint8_t state = 0;
  if (!read_canonical_string(body, &offset, &parsed.key.preserve_dir) ||
      !read_canonical_string(body, &offset, &parsed.key.epoch_scope) ||
      !read_canonical_string(body, &offset, &parsed.key.epoch_id) ||
      !read_canonical_string(body, &offset, &parsed.key.token) ||
      !read_canonical_string(body, &offset,
                             &parsed.key.target_boot_incarnation) ||
      !read_canonical_u64(body, &offset, &parsed.key.generation) ||
      !read_canonical_u8(body, &offset, &state) ||
      !read_canonical_u64(body, &offset, &parsed.target_connection_id) ||
      !read_canonical_u64(body, &offset, &parsed.generated_at_us) ||
      offset != body.size()) {
    return false;
  }
  parsed.state = static_cast<Preserve_trx_strict_attach_intent_state>(state);
  if (!strict_attach_intent_is_valid(parsed)) return false;
  *intent = std::move(parsed);
  return true;
}

std::string preserved_trx_strict_attach_intent_journal_id(
    const Preserve_trx_prepared_token_key &key) {
  if (!prepared_token_key_is_valid(key)) return {};
  std::string identity;
  if (!append_canonical_string(&identity, key.preserve_dir) ||
      !append_canonical_string(&identity, key.epoch_scope) ||
      !append_canonical_string(&identity, key.epoch_id) ||
      !append_canonical_string(&identity, key.token) ||
      !append_canonical_string(&identity, key.target_boot_incarnation)) {
    return {};
  }
  append_canonical_u64(&identity, key.generation);
  return "attach-" + sha256_hex_string(identity);
}

bool preserved_trx_finalize_token_facts(
    Preserve_trx_final_token_facts *facts) {
  if (facts == nullptr || !final_token_facts_are_valid(*facts)) return false;
  std::string canonical;
  canonical.reserve(512);
  if (!append_canonical_string(&canonical, "PTRX_FINAL_TOKEN_FACTS_V1")) {
    return false;
  }
  append_canonical_u64(&canonical, facts->required_apply_lsn);
  append_canonical_u64(&canonical, facts->physical_fence_lsn);
  append_canonical_u64(&canonical, facts->source_trx_id_store);
  append_canonical_u64(&canonical, facts->source_trx_id_store_lsn);
  append_canonical_u64(&canonical, facts->source_safe_next_trx_id_floor);
  if (!append_canonical_string(&canonical, facts->epoch_fact_digest) ||
      !append_canonical_string(&canonical,
                               facts->final_lock_generation_digest) ||
      !append_canonical_string(&canonical, facts->page_layout_digest) ||
      !append_canonical_string(&canonical,
                               facts->dictionary_generation_digest) ||
      !append_canonical_string(&canonical,
                               facts->prewarm_object_set_digest) ||
      !append_canonical_string(&canonical,
                               facts->target_boot_incarnation)) {
    return false;
  }
  append_canonical_u64(&canonical, facts->record_lock_unique_pages);
  append_canonical_u64(&canonical, facts->record_lock_bitmap_entries);
  append_canonical_u64(&canonical, facts->record_lock_bits);
  append_canonical_u64(&canonical, facts->binlog_cache_length);
  append_canonical_u64(&canonical, facts->binlog_cache_memory_bytes);
  append_canonical_u64(&canonical, facts->lock_plan_capacity_bytes);
  append_canonical_u64(&canonical, facts->native_binlog_capacity_bytes);
  append_canonical_u64(&canonical, facts->epoch_prepare_deadline_us);
  append_canonical_u64(&canonical, facts->client_resume_deadline_us);
  append_canonical_bool(&canonical, facts->semantic_validated);
  append_canonical_bool(&canonical, facts->lock_plan_ready);
  append_canonical_bool(&canonical, facts->binlog_handle_ready);
  append_canonical_bool(&canonical, facts->resources_reserved);
  append_canonical_bool(&canonical, facts->predicate_lock_present);
  append_canonical_bool(&canonical, facts->binlog_cache_present);
  append_canonical_bool(&canonical, facts->binlog_cache_file_backed);
  if (!append_canonical_string(&canonical, facts->binlog_handle_digest)) {
    return false;
  }
  const std::string digest = sha256_hex_string(canonical);
  if (!facts->canonical_digest.empty() && facts->canonical_digest != digest) {
    return false;
  }
  facts->canonical_digest = digest;
  return true;
}

Preserve_trx_prepared_token_resources::Preserve_trx_prepared_token_resources()
    : m_impl(new Impl()) {}

Preserve_trx_prepared_token_resources::Preserve_trx_prepared_token_resources(
    Preserve_trx_prepared_token_resources &&other) noexcept = default;

Preserve_trx_prepared_token_resources &
Preserve_trx_prepared_token_resources::operator=(
    Preserve_trx_prepared_token_resources &&other) noexcept = default;

Preserve_trx_prepared_token_resources::~Preserve_trx_prepared_token_resources() =
    default;

bool Preserve_trx_prepared_token_resources::acquired() const {
  return m_impl != nullptr && m_impl->acquired;
}

uint64_t Preserve_trx_prepared_token_resources::lock_plan_bytes() const {
  return m_impl == nullptr ? 0 : m_impl->lock_plan_bytes;
}

uint64_t Preserve_trx_prepared_token_resources::native_binlog_bytes() const {
  return m_impl == nullptr ? 0 : m_impl->native_binlog_bytes;
}

bool Preserve_trx_prepared_token_resources::has_record_lock_plan() const {
  return m_impl != nullptr && m_impl->record_lock_plan != nullptr;
}

bool Preserve_trx_prepared_token_resources::has_semantic_bundle() const {
  return m_impl != nullptr && m_impl->semantic_bundle != nullptr;
}

bool Preserve_trx_prepared_token_resources::has_native_binlog_handle() const {
  return m_impl != nullptr && m_impl->native_binlog_handle != nullptr;
}

bool Preserve_trx_prepared_token_resources::has_resurrection_entry() const {
  return m_impl != nullptr && m_impl->resurrection_entry != nullptr;
}

void Preserve_trx_prepared_token_resources::reset() noexcept { m_impl.reset(); }

Preserve_trx_prepared_status
Preserve_trx_prepared_token_resources::install_record_lock_plan(
    std::unique_ptr<lock_preserve_metadata_plan_t> plan) {
  if (m_impl == nullptr || !m_impl->acquired || plan == nullptr ||
      m_impl->record_lock_plan != nullptr || !plan->ready() ||
      plan->capacity_bytes() != m_impl->lock_plan_bytes) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  m_impl->record_lock_plan = std::move(plan);
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_resources::
    install_record_lock_plan_with_memory_lease(
        std::unique_ptr<lock_preserve_metadata_plan_t> plan,
        Preserve_memory_lease &&memory_lease) {
  if (m_impl == nullptr || !m_impl->acquired || plan == nullptr ||
      m_impl->record_lock_plan != nullptr || !plan->ready() ||
      !memory_lease.acquired() ||
      memory_lease.bytes() != plan->capacity_bytes() ||
      m_impl->lock_plan_bytes != 0) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  m_impl->lock_plan_memory = std::move(memory_lease);
  m_impl->lock_plan_bytes = plan->capacity_bytes();
  m_impl->record_lock_plan = std::move(plan);
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_resources::install_semantic_bundle(
    std::unique_ptr<Preserved_trx_bundle> bundle) {
  if (m_impl == nullptr || !m_impl->acquired || bundle == nullptr ||
      m_impl->semantic_bundle != nullptr ||
      bundle->metadata.token != m_impl->key.token) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  Preserve_memory_lease memory = preserve_trx_acquire_memory_lease(
      prepared_resource_token(m_impl->key),
      Preserve_trx_memory_kind::PROMOTION_SEMANTIC_BUNDLE,
      preserved_trx_bundle_capacity_bytes(*bundle));
  if (!memory.acquired()) {
    return Preserve_trx_prepared_status::RESOURCE_EXHAUSTED;
  }
  m_impl->semantic_bundle_memory = std::move(memory);
  m_impl->semantic_bundle = std::move(bundle);
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_resources::install_resurrection_entry(
    std::unique_ptr<Preserve_trx_resurrection_index_entry> entry) {
  if (m_impl == nullptr || !m_impl->acquired || entry == nullptr ||
      m_impl->resurrection_entry != nullptr ||
      entry->authority_token.empty() ||
      entry->trx_id == 0 || entry->freeze_lsn == 0 ||
      entry->authority_token != m_impl->key.token) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  m_impl->resurrection_entry = std::move(entry);
  return Preserve_trx_prepared_status::OK;
}

Mysql_binlog_preserve_cache_status
Preserve_trx_prepared_token_resources::prepare_native_binlog_handle(
    const Preserve_trx_internal_operation_capability &capability,
    const Mysql_binlog_preserve_cache_facts &facts,
    Mysql_binlog_preserve_payload_reader *reader) {
  if (m_impl == nullptr || !m_impl->acquired ||
      m_impl->native_binlog_handle != nullptr ||
      !m_impl->native_binlog_resources.acquired()) {
    return Mysql_binlog_preserve_cache_status::INVALID_STATE;
  }
  bool inject_open_failure = false;
  DBUG_EXECUTE_IF("preserve_trx_fail_native_binlog_prepare_open",
                  inject_open_failure = true;);
  const auto status =
      inject_open_failure
          ? Mysql_binlog_preserve_cache_status::RESOURCE_EXHAUSTED
          : mysql_binlog_preserve_prepare_detached_cache(
                capability, facts, reader,
                std::move(m_impl->native_binlog_resources),
                &m_impl->native_binlog_handle);
  if (status == Mysql_binlog_preserve_cache_status::RESOURCE_EXHAUSTED) {
    preserved_trx_promotion_prepared_note_resource_open_failure();
  }
  if (status != Mysql_binlog_preserve_cache_status::OK)
    m_impl->acquired = false;
  return status;
}

Mysql_binlog_preserve_cache_status
Preserve_trx_prepared_token_resources::prepare_native_binlog_handle_for_receiver(
    const Mysql_binlog_preserve_cache_facts &facts,
    Mysql_binlog_preserve_payload_reader *reader) {
  if (m_impl == nullptr || !m_impl->acquired || reader == nullptr ||
      m_impl->key.epoch_scope != facts.identity.epoch_scope ||
      m_impl->key.epoch_id != facts.identity.epoch_id ||
      m_impl->key.token != facts.identity.token ||
      m_impl->key.target_boot_incarnation !=
          facts.identity.target_boot_incarnation ||
      m_impl->key.generation != facts.identity.generation ||
      facts.binlog_incarnation == 0 || facts.key_generation == 0) {
    return Mysql_binlog_preserve_cache_status::CAPABILITY_REJECTED;
  }
  Preserve_trx_internal_operation_capability capability;
  capability.m_operation =
      Preserve_trx_internal_operation::PREPARE_BINLOG_CACHE;
  capability.m_identity = facts.identity;
  capability.m_binlog_incarnation = facts.binlog_incarnation;
  capability.m_key_generation = facts.key_generation;
  return prepare_native_binlog_handle(capability, facts, reader);
}

Preserve_trx_prepared_status
preserved_trx_acquire_prepared_token_resources(
    const Preserve_trx_prepared_token_key &key, uint64_t lock_plan_bytes,
    uint64_t native_binlog_bytes,
    Preserve_trx_prepared_token_resources *resources) {
  if (native_binlog_bytes != 0)
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  return preserved_trx_acquire_prepared_token_resources(
      key, lock_plan_bytes, 0, 0, 0, resources);
}

Preserve_trx_prepared_status
preserved_trx_acquire_prepared_token_resources(
    const Preserve_trx_prepared_token_key &key, uint64_t lock_plan_bytes,
    uint64_t native_binlog_bytes, uint64_t native_binlog_fd_count,
    uint64_t native_binlog_tmpdir_bytes,
    Preserve_trx_prepared_token_resources *resources) {
  if (!prepared_token_key_is_valid(key) || resources == nullptr ||
      resources->acquired() ||
      ((native_binlog_bytes == 0) != (native_binlog_fd_count == 0)) ||
      (native_binlog_bytes == 0 && native_binlog_tmpdir_bytes != 0)) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  Preserve_trx_prepared_token_resources acquired;
  const std::string resource_token = prepared_resource_token(key);
  acquired.m_impl->lock_plan_memory = preserve_trx_acquire_memory_lease(
      resource_token, Preserve_trx_memory_kind::PROMOTION_LOCK_PLAN,
      lock_plan_bytes);
  if (!acquired.m_impl->lock_plan_memory.acquired()) {
    return Preserve_trx_prepared_status::RESOURCE_EXHAUSTED;
  }
  if (native_binlog_bytes != 0) {
    acquired.m_impl->native_binlog_resources =
        preserve_trx_acquire_native_binlog_resource_lease(
            resource_token, native_binlog_bytes, native_binlog_fd_count,
            native_binlog_tmpdir_bytes);
    if (!acquired.m_impl->native_binlog_resources.acquired()) {
      return Preserve_trx_prepared_status::RESOURCE_EXHAUSTED;
    }
  }
  acquired.m_impl->lock_plan_bytes = lock_plan_bytes;
  acquired.m_impl->native_binlog_bytes = native_binlog_bytes;
  acquired.m_impl->key = key;
  acquired.m_impl->acquired = true;
  *resources = std::move(acquired);
  return Preserve_trx_prepared_status::OK;
}

void Preserve_trx_prepare_lease::fail_closed() {
  if (!m_active || m_entry == nullptr) return;
  std::lock_guard<std::mutex> guard(m_entry->mutex);
  if (m_entry->preparing &&
      m_entry->preparing_generation == m_key.generation) {
    m_entry->preparing = false;
    m_entry->preparing_generation = 0;
    if (m_new_entry &&
        std::atomic_load_explicit(&m_entry->publication,
                                  std::memory_order_acquire) == nullptr) {
      m_entry->state.store(Preserve_trx_prepared_token_state::NOT_READY,
                           std::memory_order_release);
    }
  }
  m_active = false;
  m_entry.reset();
  m_registry.reset();
}

Preserve_trx_prepare_lease::Preserve_trx_prepare_lease(
    Preserve_trx_prepare_lease &&other) noexcept {
  *this = std::move(other);
}

Preserve_trx_prepare_lease &Preserve_trx_prepare_lease::operator=(
    Preserve_trx_prepare_lease &&other) noexcept {
  if (this == &other) return *this;
  fail_closed();
  m_registry = std::move(other.m_registry);
  m_entry = std::move(other.m_entry);
  m_key = std::move(other.m_key);
  m_new_entry = other.m_new_entry;
  m_active = other.m_active;
  other.m_new_entry = false;
  other.m_active = false;
  return *this;
}

Preserve_trx_prepare_lease::~Preserve_trx_prepare_lease() { fail_closed(); }

void Preserve_trx_gate_adopt_lease::fail_closed() {
  if (!m_active || m_entry == nullptr) return;
  auto expected = Preserve_trx_prepared_token_state::ADOPTING;
  m_entry->state.compare_exchange_strong(
      expected, Preserve_trx_prepared_token_state::CLEANUP_TAINTED,
      std::memory_order_acq_rel, std::memory_order_acquire);
  m_active = false;
  m_entry.reset();
}

Preserve_trx_gate_adopt_lease::Preserve_trx_gate_adopt_lease(
    Preserve_trx_gate_adopt_lease &&other) noexcept {
  *this = std::move(other);
}

Preserve_trx_gate_adopt_lease &Preserve_trx_gate_adopt_lease::operator=(
    Preserve_trx_gate_adopt_lease &&other) noexcept {
  if (this == &other) return *this;
  fail_closed();
  m_entry = std::move(other.m_entry);
  m_active = other.m_active;
  other.m_active = false;
  return *this;
}

Preserve_trx_gate_adopt_lease::~Preserve_trx_gate_adopt_lease() {
  fail_closed();
}

void Preserve_trx_attach_lease::fail_closed() {
  if (!m_active || m_entry == nullptr) return;
  auto expected = m_activation_started
                      ? Preserve_trx_prepared_token_state::ACTIVATING
                      : Preserve_trx_prepared_token_state::ATTACHING;
  m_entry->state.compare_exchange_strong(
      expected, Preserve_trx_prepared_token_state::ATTACH_TAINTED,
      std::memory_order_acq_rel, std::memory_order_acquire);
  m_active = false;
  m_activation_started = false;
  m_entry.reset();
}

Preserve_trx_attach_lease::Preserve_trx_attach_lease(
    Preserve_trx_attach_lease &&other) noexcept {
  *this = std::move(other);
}

Preserve_trx_attach_lease &Preserve_trx_attach_lease::operator=(
    Preserve_trx_attach_lease &&other) noexcept {
  if (this == &other) return *this;
  fail_closed();
  m_entry = std::move(other.m_entry);
  m_active = other.m_active;
  m_activation_started = other.m_activation_started;
  other.m_active = false;
  other.m_activation_started = false;
  return *this;
}

Preserve_trx_attach_lease::~Preserve_trx_attach_lease() { fail_closed(); }

Preserve_trx_prepared_status
Preserve_trx_attach_lease::take_native_binlog_handle(
    std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle> *out) {
  if (!m_active || m_entry == nullptr || m_activation_started || out == nullptr ||
      *out != nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_entry->mutex);
  if (m_entry->state.load(std::memory_order_acquire) !=
      Preserve_trx_prepared_token_state::ATTACHING) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  const auto publication = std::atomic_load_explicit(
      &m_entry->publication, std::memory_order_acquire);
  if (publication == nullptr || !publication->facts.binlog_cache_present ||
      m_entry->resources.m_impl == nullptr ||
      m_entry->resources.m_impl->native_binlog_handle == nullptr ||
      !m_entry->resources.m_impl->native_binlog_handle->matches(
          prepared_binlog_identity(publication->key),
          publication->facts.binlog_handle_digest,
          publication->facts.binlog_cache_length,
          publication->facts.binlog_cache_file_backed)) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  *out = std::move(m_entry->resources.m_impl->native_binlog_handle);
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_attach_lease::make_native_binlog_attach_capability(
    Preserve_trx_internal_operation_capability *out) const {
  if (!m_active || m_entry == nullptr || m_activation_started ||
      out == nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_entry->mutex);
  if (m_entry->state.load(std::memory_order_acquire) !=
          Preserve_trx_prepared_token_state::ATTACHING ||
      m_entry->resources.m_impl == nullptr ||
      m_entry->resources.m_impl->native_binlog_handle == nullptr ||
      !m_entry->resources.m_impl->native_binlog_handle->make_attach_capability(
          out)) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_attach_lease::restore_native_binlog_handle(
    std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle> *inout) {
  if (!m_active || m_entry == nullptr || m_activation_started ||
      inout == nullptr || *inout == nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_entry->mutex);
  if (m_entry->state.load(std::memory_order_acquire) !=
          Preserve_trx_prepared_token_state::ATTACHING ||
      m_entry->resources.m_impl == nullptr ||
      m_entry->resources.m_impl->native_binlog_handle != nullptr) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  const auto publication = std::atomic_load_explicit(
      &m_entry->publication, std::memory_order_acquire);
  if (publication == nullptr ||
      !(*inout)->matches(prepared_binlog_identity(publication->key),
                         publication->facts.binlog_handle_digest,
                         publication->facts.binlog_cache_length,
                         publication->facts.binlog_cache_file_backed)) {
    return Preserve_trx_prepared_status::DIGEST_CONFLICT;
  }
  m_entry->resources.m_impl->native_binlog_handle = std::move(*inout);
  return Preserve_trx_prepared_status::OK;
}

void Preserve_trx_cleanup_lease::fail_closed() {
  if (!m_active || m_entry == nullptr) return;
  auto expected = Preserve_trx_prepared_token_state::CLEANUP_PENDING;
  m_entry->state.compare_exchange_strong(
      expected, Preserve_trx_prepared_token_state::CLEANUP_TAINTED,
      std::memory_order_acq_rel, std::memory_order_acquire);
  m_active = false;
  m_entry.reset();
}

Preserve_trx_cleanup_lease::Preserve_trx_cleanup_lease(
    Preserve_trx_cleanup_lease &&other) noexcept {
  *this = std::move(other);
}

Preserve_trx_cleanup_lease &Preserve_trx_cleanup_lease::operator=(
    Preserve_trx_cleanup_lease &&other) noexcept {
  if (this == &other) return *this;
  fail_closed();
  m_entry = std::move(other.m_entry);
  m_active = other.m_active;
  other.m_active = false;
  return *this;
}

Preserve_trx_cleanup_lease::~Preserve_trx_cleanup_lease() { fail_closed(); }

trx_preserve_targeted_publication_journal *
Preserve_trx_cleanup_lease::targeted_publication_journal() const {
  if (!m_active || m_entry == nullptr) return nullptr;
  std::lock_guard<std::mutex> guard(m_entry->mutex);
  if (m_entry->state.load(std::memory_order_acquire) !=
          Preserve_trx_prepared_token_state::CLEANUP_PENDING ||
      m_entry->resources.m_impl == nullptr) {
    return nullptr;
  }
  return m_entry->resources.m_impl->targeted_publication_journal.get();
}

Preserve_trx_prepared_status
Preserve_trx_cleanup_lease::take_targeted_publication_journal(
    std::unique_ptr<trx_preserve_targeted_publication_journal> *out) {
  if (!m_active || m_entry == nullptr || out == nullptr || *out != nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_entry->mutex);
  if (m_entry->state.load(std::memory_order_acquire) !=
          Preserve_trx_prepared_token_state::CLEANUP_PENDING ||
      m_entry->resources.m_impl == nullptr ||
      m_entry->resources.m_impl->targeted_publication_journal == nullptr) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  *out = std::move(
      m_entry->resources.m_impl->targeted_publication_journal);
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_token_registry::Preserve_trx_prepared_token_registry()
    : m_state(std::make_shared<Preserve_trx_prepared_registry_state>()) {}

Preserve_trx_prepared_token_registry::~Preserve_trx_prepared_token_registry() =
    default;

Preserve_trx_prepared_status Preserve_trx_prepared_token_registry::begin_prepare(
    const Preserve_trx_prepared_token_key &key, uint64_t expected_generation,
    Preserve_trx_prepare_lease *lease) {
  if (!prepared_token_key_is_valid(key) || expected_generation == 0 ||
      expected_generation != key.generation || lease == nullptr ||
      lease->active()) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }

  std::shared_ptr<Preserve_trx_prepared_token_entry> entry;
  bool new_entry = false;
  {
    std::lock_guard<std::mutex> guard(m_state->mutex);
    const auto locator = prepared_token_locator(key);
    auto it = m_state->entries.find(locator);
    if (it == m_state->entries.end()) {
      entry = std::make_shared<Preserve_trx_prepared_token_entry>();
      entry->key = key;
      m_state->entries.emplace(locator, entry);
      new_entry = true;
    } else {
      entry = it->second;
    }
  }

#ifndef NDEBUG
  run_prepared_registry_probe(
      Preserve_trx_prepared_registry_probe_point::BEGIN_PREPARE_AFTER_LOOKUP);
#endif

  std::lock_guard<std::mutex> entry_guard(entry->mutex);
  if (entry->retired_from_registry) {
    return Preserve_trx_prepared_status::NOT_FOUND;
  }
  const auto state = entry->state.load(std::memory_order_acquire);
  if (!prepared_state_accepts_generation(state)) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  const auto publication = std::atomic_load_explicit(
      &entry->publication, std::memory_order_acquire);
  const uint64_t current_generation =
      publication == nullptr ? entry->key.generation
                             : publication->key.generation;
  if (expected_generation < current_generation) {
    return Preserve_trx_prepared_status::STALE_GENERATION;
  }
  if (entry->key.target_boot_incarnation != key.target_boot_incarnation ||
      entry->key.preserve_dir != key.preserve_dir) {
    return Preserve_trx_prepared_status::STALE_GENERATION;
  }
  if (entry->preparing) {
    if (expected_generation <= entry->preparing_generation) {
      return expected_generation < entry->preparing_generation
                 ? Preserve_trx_prepared_status::STALE_GENERATION
                 : Preserve_trx_prepared_status::ALREADY_CLAIMED;
    }
  }
  if (publication == nullptr) entry->key = key;
  entry->preparing = true;
  entry->preparing_generation = expected_generation;
  lease->m_registry = m_state;
  lease->m_entry = entry;
  lease->m_key = key;
  lease->m_new_entry = new_entry || publication == nullptr;
  lease->m_active = true;
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status Preserve_trx_prepared_token_registry::publish_ready(
    Preserve_trx_prepare_lease *lease, Preserve_trx_final_token_facts facts,
    Preserve_trx_prepared_token_resources resources) {
  if (lease == nullptr || !lease->active()) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  const Preserve_trx_prepared_token_key key = lease->m_key;
  const uint64_t generation = key.generation;
  const std::string prewarm_digest = facts.prewarm_object_set_digest;
  const auto prewarm_status = publish_prewarmed(
      lease, prewarm_digest, std::move(resources));
  if (prewarm_status != Preserve_trx_prepared_status::OK &&
      prewarm_status != Preserve_trx_prepared_status::IDEMPOTENT) {
    return prewarm_status;
  }
  return bind_final_facts(key, generation, std::move(facts));
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::publish_prewarmed(
    Preserve_trx_prepare_lease *lease,
    const std::string &prewarm_object_set_digest,
    Preserve_trx_prepared_token_resources resources) {
  if (lease == nullptr || !lease->active() || lease->m_entry == nullptr ||
      !resources.acquired() || !resources.has_semantic_bundle() ||
      !digest_is_sha256_hex(prewarm_object_set_digest)) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }

  Preserve_trx_prepared_token_resources retired_resources;
  Preserve_trx_prepared_status status = Preserve_trx_prepared_status::OK;
  {
    std::lock_guard<std::mutex> guard(lease->m_entry->mutex);
    if (!lease->m_entry->preparing ||
        lease->m_entry->preparing_generation != lease->m_key.generation) {
      status = Preserve_trx_prepared_status::STALE_GENERATION;
    } else {
      if (lease->m_entry->resources.acquired()) {
        if (lease->m_entry->key.generation == lease->m_key.generation &&
            lease->m_entry->prewarm_object_set_digest ==
                prewarm_object_set_digest) {
          status = Preserve_trx_prepared_status::IDEMPOTENT;
        } else {
          lease->m_entry->state.store(
              Preserve_trx_prepared_token_state::CORRUPT,
              std::memory_order_release);
          retired_resources = std::move(lease->m_entry->resources);
          status = Preserve_trx_prepared_status::DIGEST_CONFLICT;
        }
      } else {
        lease->m_entry->resources = std::move(resources);
        lease->m_entry->key = lease->m_key;
        lease->m_entry->prewarm_object_set_digest = prewarm_object_set_digest;
        lease->m_entry->state.store(
            Preserve_trx_prepared_token_state::PREWARMED_PENDING_FINAL_FACT,
            std::memory_order_release);
      }
      lease->m_entry->preparing = false;
      lease->m_entry->preparing_generation = 0;
    }
  }
  lease->m_active = false;
  lease->m_entry.reset();
  lease->m_registry.reset();
  return status;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::bind_final_facts(
    const Preserve_trx_prepared_token_key &key,
    uint64_t expected_generation, Preserve_trx_final_token_facts facts) {
  if (!prepared_token_key_is_valid(key) || expected_generation == 0 ||
      expected_generation != key.generation ||
      !preserved_trx_finalize_token_facts(&facts) ||
      facts.target_boot_incarnation != key.target_boot_incarnation) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  auto entry = find_prepared_entry(m_state, key);
  if (entry == nullptr) return Preserve_trx_prepared_status::NOT_FOUND;

  Preserve_trx_prepared_token_resources retired_resources;
  {
    std::lock_guard<std::mutex> guard(entry->mutex);
    if (entry->key.generation != expected_generation ||
        entry->key.preserve_dir != key.preserve_dir ||
        entry->key.target_boot_incarnation != key.target_boot_incarnation) {
      return Preserve_trx_prepared_status::STALE_GENERATION;
    }
    const auto current = std::atomic_load_explicit(
        &entry->publication, std::memory_order_acquire);
    if (current != nullptr) {
      if (current->key.generation == expected_generation &&
          current->facts.canonical_digest == facts.canonical_digest) {
        return Preserve_trx_prepared_status::IDEMPOTENT;
      }
      entry->state.store(Preserve_trx_prepared_token_state::CORRUPT,
                         std::memory_order_release);
      retired_resources = std::move(entry->resources);
      return Preserve_trx_prepared_status::DIGEST_CONFLICT;
    }
    if (entry->state.load(std::memory_order_acquire) !=
            Preserve_trx_prepared_token_state::
                PREWARMED_PENDING_FINAL_FACT ||
        !entry->resources.acquired() ||
        entry->prewarm_object_set_digest !=
            facts.prewarm_object_set_digest) {
      return Preserve_trx_prepared_status::INVALID_STATE;
    }

    const Preserved_trx_bundle *semantic_bundle =
        entry->resources.m_impl == nullptr
            ? nullptr
            : entry->resources.m_impl->semantic_bundle.get();
    if (semantic_bundle == nullptr) {
      return Preserve_trx_prepared_status::INVALID_STATE;
    }
    if (semantic_bundle->metadata.has_read_view &&
        !trx_preserve_read_view_payload_fits_source_horizon(
            semantic_bundle->metadata.read_view_payload,
            facts.source_safe_next_trx_id_floor)) {
      entry->state.store(Preserve_trx_prepared_token_state::NOT_READY,
                         std::memory_order_release);
      retired_resources = std::move(entry->resources);
      entry->prewarm_object_set_digest.clear();
      return Preserve_trx_prepared_status::READ_VIEW_HORIZON_MISMATCH;
    }

    const bool has_record_lock_facts =
        facts.record_lock_unique_pages != 0 ||
        facts.record_lock_bitmap_entries != 0 || facts.record_lock_bits != 0;
    const lock_preserve_metadata_plan_t *record_lock_plan =
        entry->resources.m_impl == nullptr
            ? nullptr
            : entry->resources.m_impl->record_lock_plan.get();
    if (facts.lock_plan_capacity_bytes !=
            entry->resources.lock_plan_bytes() ||
        facts.native_binlog_capacity_bytes !=
            entry->resources.native_binlog_bytes() ||
        (has_record_lock_facts &&
         (record_lock_plan == nullptr || !record_lock_plan->ready() ||
          facts.record_lock_unique_pages > record_lock_plan->entry_count() ||
          facts.record_lock_bitmap_entries !=
              record_lock_plan->entry_count() ||
          facts.record_lock_bits != record_lock_plan->bitmap_bits() ||
          facts.lock_plan_capacity_bytes !=
              record_lock_plan->capacity_bytes())) ||
        (!has_record_lock_facts && record_lock_plan != nullptr) ||
        (facts.binlog_cache_present &&
         (!facts.binlog_handle_ready ||
          !entry->resources.has_native_binlog_handle() ||
          !entry->resources.m_impl->native_binlog_handle->matches(
              prepared_binlog_identity(key), facts.binlog_handle_digest,
              facts.binlog_cache_length, facts.binlog_cache_file_backed))) ||
        (!facts.binlog_cache_present &&
         entry->resources.has_native_binlog_handle())) {
      return Preserve_trx_prepared_status::INVALID_ARGUMENT;
    }

    auto publication =
        std::make_shared<const Preserve_trx_prepared_token_publication>(
            Preserve_trx_prepared_token_publication{key, std::move(facts)});
    std::atomic_store_explicit(&entry->publication, std::move(publication),
                               std::memory_order_release);
    entry->state.store(
        Preserve_trx_prepared_token_state::READY_FACTS_PENDING_LEASE,
        std::memory_order_release);
  }
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::update_epoch_prepare_deadline(
    const std::string &epoch_scope, const std::string &epoch_id,
    size_t expected_token_count, uint64_t deadline_monotonic_us) {
  if (epoch_scope.empty() || epoch_id.empty() || expected_token_count == 0 ||
      deadline_monotonic_us == 0) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }

  std::vector<std::shared_ptr<Preserve_trx_prepared_token_entry>> entries;
  std::vector<std::unique_lock<std::mutex>> entry_guards;
  std::vector<std::shared_ptr<const Preserve_trx_prepared_token_publication>>
      publications;
  try {
    std::lock_guard<std::mutex> registry_guard(m_state->mutex);
    entries.reserve(expected_token_count);
    for (const auto &item : m_state->entries) {
      if (item.first.epoch_scope == epoch_scope &&
          item.first.epoch_id == epoch_id) {
        entries.push_back(item.second);
      }
    }
    if (entries.size() != expected_token_count) {
      return Preserve_trx_prepared_status::INVALID_STATE;
    }

    entry_guards.reserve(entries.size());
    for (const auto &entry : entries) entry_guards.emplace_back(entry->mutex);
    publications.reserve(entries.size());
    for (const auto &entry : entries) {
      const auto state = entry->state.load(std::memory_order_acquire);
      if (entry->retired_from_registry ||
          (state != Preserve_trx_prepared_token_state::
                        READY_FACTS_PENDING_LEASE &&
           state != Preserve_trx_prepared_token_state::READY_FOR_GATE)) {
        return Preserve_trx_prepared_status::INVALID_STATE;
      }
      const auto current = std::atomic_load_explicit(
          &entry->publication, std::memory_order_acquire);
      if (current == nullptr || current->key.epoch_scope != epoch_scope ||
          current->key.epoch_id != epoch_id) {
        return Preserve_trx_prepared_status::STALE_GENERATION;
      }
      if (current->facts.epoch_prepare_deadline_us == deadline_monotonic_us &&
          current->facts.client_resume_deadline_us ==
              deadline_monotonic_us) {
        publications.push_back(current);
        continue;
      }
      Preserve_trx_final_token_facts facts = current->facts;
      facts.epoch_prepare_deadline_us = deadline_monotonic_us;
      facts.client_resume_deadline_us = deadline_monotonic_us;
      facts.canonical_digest.clear();
      if (!preserved_trx_finalize_token_facts(&facts)) {
        return Preserve_trx_prepared_status::INVALID_STATE;
      }
      publications.push_back(
          std::make_shared<const Preserve_trx_prepared_token_publication>(
              Preserve_trx_prepared_token_publication{current->key,
                                                      std::move(facts)}));
    }
    for (size_t index = 0; index < entries.size(); ++index) {
      std::atomic_store_explicit(&entries[index]->publication,
                                 std::move(publications[index]),
                                 std::memory_order_release);
    }
  } catch (const std::bad_alloc &) {
    return Preserve_trx_prepared_status::RESOURCE_EXHAUSTED;
  }
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::update_selected_prepare_deadline(
    const std::vector<Preserve_trx_prepared_token_key> &keys,
    uint64_t deadline_monotonic_us) {
  if (keys.empty() || deadline_monotonic_us == 0) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }

  std::vector<Preserve_trx_prepared_token_key> ordered_keys;
  std::vector<std::shared_ptr<Preserve_trx_prepared_token_entry>> entries;
  std::vector<std::unique_lock<std::mutex>> entry_guards;
  std::vector<std::shared_ptr<const Preserve_trx_prepared_token_publication>>
      publications;
  try {
    ordered_keys = keys;
    for (const auto &key : ordered_keys) {
      if (!prepared_token_key_is_valid(key) ||
          key.epoch_scope != ordered_keys.front().epoch_scope ||
          key.epoch_id != ordered_keys.front().epoch_id) {
        return Preserve_trx_prepared_status::INVALID_ARGUMENT;
      }
    }
    std::sort(ordered_keys.begin(), ordered_keys.end(),
              [](const auto &left, const auto &right) {
                return left.token < right.token;
              });
    if (std::adjacent_find(
            ordered_keys.begin(), ordered_keys.end(),
            [](const auto &left, const auto &right) {
              return left.token == right.token;
            }) != ordered_keys.end()) {
      return Preserve_trx_prepared_status::INVALID_ARGUMENT;
    }

    {
      std::lock_guard<std::mutex> registry_guard(m_state->mutex);
      entries.reserve(ordered_keys.size());
      for (const auto &key : ordered_keys) {
        const auto found = m_state->entries.find(prepared_token_locator(key));
        if (found == m_state->entries.end() ||
            !prepared_token_keys_match(found->second->key, key)) {
          return Preserve_trx_prepared_status::NOT_FOUND;
        }
        entries.push_back(found->second);
      }
    }

    entry_guards.reserve(entries.size());
    for (const auto &entry : entries) entry_guards.emplace_back(entry->mutex);
    publications.reserve(entries.size());
    for (const auto &entry : entries) {
      const auto state = entry->state.load(std::memory_order_acquire);
      if (entry->retired_from_registry ||
          (state != Preserve_trx_prepared_token_state::
                        READY_FACTS_PENDING_LEASE &&
           state != Preserve_trx_prepared_token_state::READY_FOR_GATE)) {
        return Preserve_trx_prepared_status::INVALID_STATE;
      }
      const auto current = std::atomic_load_explicit(
          &entry->publication, std::memory_order_acquire);
      if (current == nullptr ||
          !prepared_token_keys_match(current->key, entry->key)) {
        return Preserve_trx_prepared_status::STALE_GENERATION;
      }
      if (current->facts.epoch_prepare_deadline_us == deadline_monotonic_us &&
          current->facts.client_resume_deadline_us ==
              deadline_monotonic_us) {
        publications.push_back(current);
        continue;
      }
      Preserve_trx_final_token_facts facts = current->facts;
      facts.epoch_prepare_deadline_us = deadline_monotonic_us;
      facts.client_resume_deadline_us = deadline_monotonic_us;
      facts.canonical_digest.clear();
      if (!preserved_trx_finalize_token_facts(&facts)) {
        return Preserve_trx_prepared_status::INVALID_STATE;
      }
      publications.push_back(
          std::make_shared<const Preserve_trx_prepared_token_publication>(
              Preserve_trx_prepared_token_publication{current->key,
                                                      std::move(facts)}));
    }
    for (size_t index = 0; index < entries.size(); ++index) {
      std::atomic_store_explicit(&entries[index]->publication,
                                 std::move(publications[index]),
                                 std::memory_order_release);
    }
  } catch (const std::bad_alloc &) {
    return Preserve_trx_prepared_status::RESOURCE_EXHAUSTED;
  }
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::pin_epoch_for_physical_promotion(
    const std::vector<Preserve_trx_prepared_token_key> &keys,
    uint64_t now_monotonic_us,
    Preserve_trx_physical_promotion_pin_lease *lease) {
  return pin_epoch_for_physical_promotion(keys, now_monotonic_us, 0, lease);
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::pin_epoch_for_physical_promotion(
    const std::vector<Preserve_trx_prepared_token_key> &keys,
    uint64_t now_monotonic_us, uint64_t renewed_deadline_monotonic_us,
    Preserve_trx_physical_promotion_pin_lease *lease) {
  if (keys.empty() || now_monotonic_us == 0 || lease == nullptr ||
      lease->active() ||
      (renewed_deadline_monotonic_us != 0 &&
       renewed_deadline_monotonic_us <= now_monotonic_us)) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  std::vector<std::shared_ptr<Preserve_trx_prepared_token_entry>> entries;
  std::vector<std::unique_lock<std::mutex>> guards;
  std::vector<std::shared_ptr<const Preserve_trx_prepared_token_publication>>
      publications;
  try {
    {
      std::lock_guard<std::mutex> registry_guard(m_state->mutex);
      entries.reserve(keys.size());
      for (const auto &key : keys) {
        if (!prepared_token_key_is_valid(key) ||
            key.epoch_scope != keys.front().epoch_scope ||
            key.epoch_id != keys.front().epoch_id) {
          return Preserve_trx_prepared_status::INVALID_ARGUMENT;
        }
        const auto found = m_state->entries.find(
            {key.epoch_scope, key.epoch_id, key.token});
        if (found == m_state->entries.end() ||
            !prepared_token_keys_match(found->second->key, key)) {
          return Preserve_trx_prepared_status::NOT_FOUND;
        }
        entries.push_back(found->second);
      }
    }
    std::sort(entries.begin(), entries.end(), [](const auto &left,
                                                 const auto &right) {
      return left->key.token < right->key.token;
    });
    if (std::adjacent_find(entries.begin(), entries.end()) != entries.end()) {
      return Preserve_trx_prepared_status::INVALID_ARGUMENT;
    }
    guards.reserve(entries.size());
    for (const auto &entry : entries) guards.emplace_back(entry->mutex);
    publications.reserve(entries.size());
    for (const auto &entry : entries) {
      const auto state = entry->state.load(std::memory_order_acquire);
      const auto publication = std::atomic_load_explicit(
          &entry->publication, std::memory_order_acquire);
      if (entry->retired_from_registry ||
          (state != Preserve_trx_prepared_token_state::
                        READY_FACTS_PENDING_LEASE &&
           state != Preserve_trx_prepared_token_state::READY_FOR_GATE) ||
          publication == nullptr ||
          !prepared_token_keys_match(publication->key, entry->key) ||
          (renewed_deadline_monotonic_us == 0 &&
           publication->facts.epoch_prepare_deadline_us <= now_monotonic_us) ||
          entry->physical_promotion_pins ==
              std::numeric_limits<uint32_t>::max()) {
        return Preserve_trx_prepared_status::INVALID_STATE;
      }
      if (renewed_deadline_monotonic_us == 0 ||
          (publication->facts.epoch_prepare_deadline_us >=
               renewed_deadline_monotonic_us &&
           publication->facts.client_resume_deadline_us >=
               renewed_deadline_monotonic_us)) {
        publications.push_back(publication);
        continue;
      }
      Preserve_trx_final_token_facts facts = publication->facts;
      facts.epoch_prepare_deadline_us =
          std::max(facts.epoch_prepare_deadline_us,
                   renewed_deadline_monotonic_us);
      facts.client_resume_deadline_us =
          std::max(facts.client_resume_deadline_us,
                   renewed_deadline_monotonic_us);
      facts.canonical_digest.clear();
      if (!preserved_trx_finalize_token_facts(&facts)) {
        return Preserve_trx_prepared_status::INVALID_STATE;
      }
      publications.push_back(
          std::make_shared<const Preserve_trx_prepared_token_publication>(
              Preserve_trx_prepared_token_publication{publication->key,
                                                      std::move(facts)}));
    }
    for (size_t index = 0; index < entries.size(); ++index) {
      std::atomic_store_explicit(&entries[index]->publication,
                                 std::move(publications[index]),
                                 std::memory_order_release);
    }
    for (const auto &entry : entries) ++entry->physical_promotion_pins;
    lease->m_entries = std::move(entries);
    lease->m_guards = std::move(guards);
    lease->m_guards.clear();
    lease->m_active = true;
  } catch (const std::bad_alloc &) {
    return Preserve_trx_prepared_status::RESOURCE_EXHAUSTED;
  }
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::mark_ready_for_gate(
    const Preserve_trx_prepared_token_key &key,
    uint64_t expected_generation) {
  if (!prepared_token_key_is_valid(key) ||
      expected_generation != key.generation) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  auto entry = find_prepared_entry(m_state, key);
  if (entry == nullptr) return Preserve_trx_prepared_status::NOT_FOUND;
  const auto current = entry->state.load(std::memory_order_acquire);
  auto expected =
      Preserve_trx_prepared_token_state::READY_FACTS_PENDING_LEASE;
  if (current != expected &&
      current != Preserve_trx_prepared_token_state::READY_FOR_GATE) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  const auto publication = std::atomic_load_explicit(
      &entry->publication, std::memory_order_acquire);
  if (publication == nullptr ||
      !prepared_token_keys_match(publication->key, key)) {
    return Preserve_trx_prepared_status::STALE_GENERATION;
  }
  if (current == Preserve_trx_prepared_token_state::READY_FOR_GATE) {
    return Preserve_trx_prepared_status::IDEMPOTENT;
  }
  if (!entry->state.compare_exchange_strong(
          expected, Preserve_trx_prepared_token_state::READY_FOR_GATE,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::copy_ready_resurrection_entry(
    const Preserve_trx_prepared_token_key &key, uint64_t now_monotonic_us,
    Preserve_trx_resurrection_index_entry *resurrection_entry) const {
  if (!prepared_token_key_is_valid(key) || now_monotonic_us == 0 ||
      resurrection_entry == nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  const auto entry = find_prepared_entry(m_state, key);
  if (entry == nullptr) return Preserve_trx_prepared_status::NOT_FOUND;
  std::lock_guard<std::mutex> guard(entry->mutex);
  const auto publication = std::atomic_load_explicit(
      &entry->publication, std::memory_order_acquire);
  const auto state = entry->state.load(std::memory_order_acquire);
  if (entry->retired_from_registry || entry->preparing ||
      (state != Preserve_trx_prepared_token_state::
                    READY_FACTS_PENDING_LEASE &&
       state != Preserve_trx_prepared_token_state::READY_FOR_GATE) ||
      publication == nullptr ||
      !prepared_token_keys_match(publication->key, key) ||
      (publication->facts.epoch_prepare_deadline_us <= now_monotonic_us &&
       entry->physical_promotion_pins == 0) ||
      entry->resources.m_impl == nullptr ||
      entry->resources.m_impl->resurrection_entry == nullptr) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  *resurrection_entry = *entry->resources.m_impl->resurrection_entry;
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::begin_gate_adopt(
    const Preserve_trx_prepared_token_key &key, uint64_t expected_generation,
    Preserve_trx_gate_adopt_lease *lease,
    const Preserve_trx_physical_promotion_pin_lease *physical_pin) {
  if (!prepared_token_key_is_valid(key) ||
      expected_generation != key.generation || lease == nullptr ||
      lease->active()) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  auto entry = find_prepared_entry(m_state, key);
  if (entry == nullptr) return Preserve_trx_prepared_status::NOT_FOUND;
  std::lock_guard<std::mutex> guard(entry->mutex);
  auto expected = Preserve_trx_prepared_token_state::READY_FOR_GATE;
  if (entry->state.load(std::memory_order_acquire) != expected) {
    return Preserve_trx_prepared_status::ALREADY_CLAIMED;
  }
  const auto publication = std::atomic_load_explicit(
      &entry->publication, std::memory_order_acquire);
  if (publication == nullptr ||
      !prepared_token_keys_match(publication->key, key)) {
    return Preserve_trx_prepared_status::STALE_GENERATION;
  }
  if (entry->physical_promotion_pins != 0) {
    if (entry->physical_promotion_pins != 1 || physical_pin == nullptr ||
        !physical_pin->m_active) {
      return Preserve_trx_prepared_status::ALREADY_CLAIMED;
    }
    const auto owned = std::lower_bound(
        physical_pin->m_entries.begin(), physical_pin->m_entries.end(),
        entry->key.token, [](const auto &candidate, const std::string &token) {
          return candidate->key.token < token;
        });
    if (owned == physical_pin->m_entries.end() ||
        owned->get() != entry.get()) {
      return Preserve_trx_prepared_status::ALREADY_CLAIMED;
    }
  }
  if (publication->facts.epoch_prepare_deadline_us <= prepared_monotonic_us() &&
      entry->physical_promotion_pins == 0) {
    if (entry->state.compare_exchange_strong(
            expected, Preserve_trx_prepared_token_state::NOT_READY,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
      entry->resources.reset();
    }
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  if (!entry->state.compare_exchange_strong(
          expected, Preserve_trx_prepared_token_state::ADOPTING,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    return Preserve_trx_prepared_status::ALREADY_CLAIMED;
  }
  lease->m_entry = std::move(entry);
  lease->m_active = true;
  return Preserve_trx_prepared_status::OK;
}

const lock_preserve_metadata_plan_t *
Preserve_trx_gate_adopt_lease::record_lock_plan() const {
  if (!m_active || m_entry == nullptr) return nullptr;
  std::lock_guard<std::mutex> guard(m_entry->mutex);
  return m_entry->resources.m_impl == nullptr
             ? nullptr
             : m_entry->resources.m_impl->record_lock_plan.get();
}

const Preserve_trx_resurrection_index_entry *
Preserve_trx_gate_adopt_lease::resurrection_entry() const {
  if (!m_active || m_entry == nullptr) return nullptr;
  std::lock_guard<std::mutex> guard(m_entry->mutex);
  return m_entry->resources.m_impl == nullptr
             ? nullptr
             : m_entry->resources.m_impl->resurrection_entry.get();
}

Preserve_trx_prepared_status
Preserve_trx_gate_adopt_lease::copy_publication(
    Preserve_trx_prepared_token_key *key,
    Preserve_trx_final_token_facts *facts) const {
  if (!m_active || m_entry == nullptr || key == nullptr || facts == nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_entry->mutex);
  if (m_entry->state.load(std::memory_order_acquire) !=
      Preserve_trx_prepared_token_state::ADOPTING) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  const auto publication = std::atomic_load_explicit(
      &m_entry->publication, std::memory_order_acquire);
  if (publication == nullptr ||
      !prepared_token_keys_match(publication->key, m_entry->key)) {
    return Preserve_trx_prepared_status::STALE_GENERATION;
  }
  *key = publication->key;
  *facts = publication->facts;
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_gate_adopt_lease::take_semantic_bundle(
    std::unique_ptr<Preserved_trx_bundle> *out) {
  if (!m_active || m_entry == nullptr || out == nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_entry->mutex);
  if (m_entry->resources.m_impl == nullptr ||
      m_entry->resources.m_impl->semantic_bundle == nullptr) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  if (*out != nullptr) return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  *out = std::move(m_entry->resources.m_impl->semantic_bundle);
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_gate_adopt_lease::restore_semantic_bundle(
    std::unique_ptr<Preserved_trx_bundle> *inout) {
  if (!m_active || m_entry == nullptr || inout == nullptr ||
      *inout == nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_entry->mutex);
  if (m_entry->resources.m_impl == nullptr ||
      m_entry->resources.m_impl->semantic_bundle != nullptr) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  m_entry->resources.m_impl->semantic_bundle = std::move(*inout);
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_gate_adopt_lease::install_targeted_publication_journal(
    std::unique_ptr<trx_preserve_targeted_publication_journal> &&journal) {
  if (!m_active || m_entry == nullptr || journal == nullptr ||
      !journal->active || journal->trx == nullptr || journal->trx_id == 0 ||
      journal->generation == 0 ||
      journal->origin == trx_preserve_targeted_publication_origin::NONE) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> guard(m_entry->mutex);
  if (m_entry->state.load(std::memory_order_acquire) !=
          Preserve_trx_prepared_token_state::ADOPTING ||
      m_entry->resources.m_impl == nullptr ||
      m_entry->resources.m_impl->targeted_publication_journal != nullptr ||
      journal->generation != m_entry->key.generation) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  m_entry->resources.m_impl->targeted_publication_journal =
      std::move(journal);
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::commit_gate_adopt(
    Preserve_trx_gate_adopt_lease *lease) {
  if (lease == nullptr || !lease->active() || lease->m_entry == nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  auto expected = Preserve_trx_prepared_token_state::ADOPTING;
  if (!lease->m_entry->state.compare_exchange_strong(
          expected, Preserve_trx_prepared_token_state::ADOPTED_LOCKED,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  lease->m_active = false;
  lease->m_entry.reset();
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::abort_gate_adopt(
    Preserve_trx_gate_adopt_lease *lease,
    Preserve_trx_gate_abort_outcome outcome) {
  if (lease == nullptr || !lease->active() || lease->m_entry == nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  Preserve_trx_prepared_token_state terminal;
  switch (outcome) {
    case Preserve_trx_gate_abort_outcome::ABANDONED_ROLLED_BACK:
      terminal = Preserve_trx_prepared_token_state::ABANDONED_ROLLED_BACK;
      break;
    case Preserve_trx_gate_abort_outcome::ABANDONED_NOT_FOUND_PROVEN:
      terminal =
          Preserve_trx_prepared_token_state::ABANDONED_NOT_FOUND_PROVEN;
      break;
    case Preserve_trx_gate_abort_outcome::CLEANUP_TAINTED:
      terminal = Preserve_trx_prepared_token_state::CLEANUP_TAINTED;
      break;
  }
  auto expected = Preserve_trx_prepared_token_state::ADOPTING;
  if (!lease->m_entry->state.compare_exchange_strong(
          expected, terminal, std::memory_order_acq_rel,
          std::memory_order_acquire)) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  if (terminal != Preserve_trx_prepared_token_state::CLEANUP_TAINTED) {
    std::lock_guard<std::mutex> guard(lease->m_entry->mutex);
    lease->m_entry->resources.reset();
  }
  lease->m_active = false;
  lease->m_entry.reset();
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status Preserve_trx_prepared_token_registry::begin_attach(
    const Preserve_trx_prepared_token_key &key, uint64_t expected_generation,
    Preserve_trx_attach_lease *lease) {
  if (!prepared_token_key_is_valid(key) ||
      expected_generation != key.generation || lease == nullptr ||
      lease->active()) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  auto entry = find_prepared_entry(m_state, key);
  if (entry == nullptr) return Preserve_trx_prepared_status::NOT_FOUND;
  std::lock_guard<std::mutex> guard(entry->mutex);
  auto expected = Preserve_trx_prepared_token_state::ADOPTED_LOCKED;
  if (entry->physical_promotion_pins != 0 ||
      entry->state.load(std::memory_order_acquire) != expected) {
    return Preserve_trx_prepared_status::ALREADY_CLAIMED;
  }
  const auto publication = std::atomic_load_explicit(
      &entry->publication, std::memory_order_acquire);
  if (publication == nullptr ||
      !prepared_token_keys_match(publication->key, key)) {
    return Preserve_trx_prepared_status::STALE_GENERATION;
  }
  if (publication->facts.client_resume_deadline_us <=
      prepared_monotonic_us()) {
    if (entry->state.compare_exchange_strong(
            expected, Preserve_trx_prepared_token_state::CLEANUP_PENDING,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
      entry->state.store(Preserve_trx_prepared_token_state::CLEANUP_TAINTED,
                         std::memory_order_release);
    }
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  if (!entry->state.compare_exchange_strong(
          expected, Preserve_trx_prepared_token_state::ATTACHING,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    return Preserve_trx_prepared_status::ALREADY_CLAIMED;
  }
  lease->m_entry = std::move(entry);
  lease->m_active = true;
  lease->m_activation_started = false;
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::begin_activation(
    Preserve_trx_attach_lease *lease,
    Preserve_trx_activation_intent_writer intent_writer,
    void *intent_context) {
  if (lease == nullptr || !lease->active() || lease->m_entry == nullptr ||
      lease->m_activation_started || intent_writer == nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  Preserve_trx_prepared_token_key key;
  {
    std::lock_guard<std::mutex> guard(lease->m_entry->mutex);
    const auto publication = std::atomic_load_explicit(
        &lease->m_entry->publication, std::memory_order_acquire);
    if (publication == nullptr ||
        (publication->facts.binlog_cache_present &&
         lease->m_entry->resources.has_native_binlog_handle())) {
      return Preserve_trx_prepared_status::INVALID_STATE;
    }
    key = publication->key;
  }
  if (!intent_writer(key, intent_context)) {
    return Preserve_trx_prepared_status::INTENT_IO_ERROR;
  }
  auto expected = Preserve_trx_prepared_token_state::ATTACHING;
  if (!lease->m_entry->state.compare_exchange_strong(
          expected, Preserve_trx_prepared_token_state::ACTIVATING,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    lease->m_activation_started = true;
    auto current = lease->m_entry->state.load(std::memory_order_acquire);
    if (current == Preserve_trx_prepared_token_state::ATTACHING) {
      (void)lease->m_entry->state.compare_exchange_strong(
          current, Preserve_trx_prepared_token_state::ATTACH_TAINTED,
          std::memory_order_acq_rel, std::memory_order_acquire);
    }
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  lease->m_activation_started = true;
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::commit_attach(
    Preserve_trx_attach_lease *lease,
    Preserve_trx_activation_intent_writer intent_writer,
    void *intent_context) {
  if (lease == nullptr || !lease->active() || lease->m_entry == nullptr ||
      !lease->m_activation_started || intent_writer == nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  Preserve_trx_prepared_token_key key;
  {
    std::lock_guard<std::mutex> guard(lease->m_entry->mutex);
    const auto publication = std::atomic_load_explicit(
        &lease->m_entry->publication, std::memory_order_acquire);
    if (publication == nullptr ||
        (publication->facts.binlog_cache_present &&
         lease->m_entry->resources.has_native_binlog_handle())) {
      return Preserve_trx_prepared_status::INVALID_STATE;
    }
    key = publication->key;
  }
  if (!intent_writer(key, intent_context)) {
    return Preserve_trx_prepared_status::INTENT_IO_ERROR;
  }
  auto expected = Preserve_trx_prepared_token_state::ACTIVATING;
  if (!lease->m_entry->state.compare_exchange_strong(
          expected, Preserve_trx_prepared_token_state::ACTIVE,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    auto current = lease->m_entry->state.load(std::memory_order_acquire);
    if (current == Preserve_trx_prepared_token_state::ACTIVATING) {
      (void)lease->m_entry->state.compare_exchange_strong(
          current, Preserve_trx_prepared_token_state::ATTACH_TAINTED,
          std::memory_order_acq_rel, std::memory_order_acquire);
    }
    lease->m_active = false;
    lease->m_activation_started = false;
    lease->m_entry.reset();
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  lease->m_active = false;
  lease->m_activation_started = false;
  lease->m_entry.reset();
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::rollback_attach_after_activation(
    Preserve_trx_attach_lease *lease,
    Preserve_trx_activation_intent_writer intent_writer,
    void *intent_context) {
  if (lease == nullptr || !lease->active() || lease->m_entry == nullptr ||
      !lease->m_activation_started || intent_writer == nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  Preserve_trx_prepared_token_key key;
  {
    std::lock_guard<std::mutex> guard(lease->m_entry->mutex);
    const auto publication = std::atomic_load_explicit(
        &lease->m_entry->publication, std::memory_order_acquire);
    if (publication == nullptr ||
        (publication->facts.binlog_cache_present &&
         lease->m_entry->resources.has_native_binlog_handle())) {
      return Preserve_trx_prepared_status::INVALID_STATE;
    }
    key = publication->key;
  }
  if (!intent_writer(key, intent_context)) {
    return Preserve_trx_prepared_status::INTENT_IO_ERROR;
  }
  auto expected = Preserve_trx_prepared_token_state::ACTIVATING;
  if (!lease->m_entry->state.compare_exchange_strong(
          expected, Preserve_trx_prepared_token_state::ATTACH_ROLLED_BACK,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  lease->m_active = false;
  lease->m_activation_started = false;
  lease->m_entry.reset();
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::abort_attach_after_full_unwind(
    Preserve_trx_attach_lease *lease) {
  if (lease == nullptr || !lease->active() || lease->m_entry == nullptr ||
      lease->m_activation_started) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  {
    std::lock_guard<std::mutex> guard(lease->m_entry->mutex);
    const auto publication = std::atomic_load_explicit(
        &lease->m_entry->publication, std::memory_order_acquire);
    if (publication == nullptr ||
        (publication->facts.binlog_cache_present &&
         !lease->m_entry->resources.has_native_binlog_handle())) {
      return Preserve_trx_prepared_status::INVALID_STATE;
    }
  }
  auto expected = Preserve_trx_prepared_token_state::ATTACHING;
  if (!lease->m_entry->state.compare_exchange_strong(
          expected, Preserve_trx_prepared_token_state::ADOPTED_LOCKED,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  lease->m_active = false;
  lease->m_entry.reset();
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status Preserve_trx_prepared_token_registry::taint_attach(
    Preserve_trx_attach_lease *lease) {
  if (lease == nullptr || !lease->active() || lease->m_entry == nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  auto expected = lease->m_activation_started
                      ? Preserve_trx_prepared_token_state::ACTIVATING
                      : Preserve_trx_prepared_token_state::ATTACHING;
  if (!lease->m_entry->state.compare_exchange_strong(
          expected, Preserve_trx_prepared_token_state::ATTACH_TAINTED,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  lease->m_active = false;
  lease->m_activation_started = false;
  lease->m_entry.reset();
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::begin_cleanup(
    const Preserve_trx_prepared_token_key &key, uint64_t expected_generation,
    Preserve_trx_prepared_token_state expected_state,
    Preserve_trx_cleanup_lease *lease) {
  if (!prepared_token_key_is_valid(key) ||
      expected_generation != key.generation || lease == nullptr ||
      lease->active() ||
      (expected_state != Preserve_trx_prepared_token_state::ADOPTED_LOCKED &&
       expected_state != Preserve_trx_prepared_token_state::ATTACH_TAINTED)) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  auto entry = find_prepared_entry(m_state, key);
  if (entry == nullptr) return Preserve_trx_prepared_status::NOT_FOUND;
  auto expected = expected_state;
  if (entry->state.load(std::memory_order_acquire) != expected) {
    return Preserve_trx_prepared_status::ALREADY_CLAIMED;
  }
  const auto publication = std::atomic_load_explicit(
      &entry->publication, std::memory_order_acquire);
  if (publication == nullptr ||
      !prepared_token_keys_match(publication->key, key)) {
    return Preserve_trx_prepared_status::STALE_GENERATION;
  }
  if (!entry->state.compare_exchange_strong(
          expected, Preserve_trx_prepared_token_state::CLEANUP_PENDING,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    return Preserve_trx_prepared_status::ALREADY_CLAIMED;
  }
  lease->m_entry = std::move(entry);
  lease->m_active = true;
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::commit_cleanup(
    Preserve_trx_cleanup_lease *lease, bool rollback_proven) {
  if (lease == nullptr || !lease->active() || lease->m_entry == nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  const auto terminal =
      rollback_proven ? Preserve_trx_prepared_token_state::CLEANUP_ROLLED_BACK
                      : Preserve_trx_prepared_token_state::CLEANUP_TAINTED;
  auto expected = Preserve_trx_prepared_token_state::CLEANUP_PENDING;
  if (!lease->m_entry->state.compare_exchange_strong(
          expected, terminal, std::memory_order_acq_rel,
          std::memory_order_acquire)) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  if (rollback_proven) {
    std::lock_guard<std::mutex> guard(lease->m_entry->mutex);
    lease->m_entry->resources.reset();
  }
  lease->m_active = false;
  lease->m_entry.reset();
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status Preserve_trx_prepared_token_registry::snapshot(
    const Preserve_trx_prepared_token_key &key,
    Preserve_trx_prepared_token_snapshot *snapshot) const {
  if (!prepared_token_key_is_valid(key) || snapshot == nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  auto entry = find_prepared_entry(m_state, key);
  if (entry == nullptr) return Preserve_trx_prepared_status::NOT_FOUND;
  std::lock_guard<std::mutex> guard(entry->mutex);
  const auto publication = std::atomic_load_explicit(
      &entry->publication, std::memory_order_acquire);
  if (publication == nullptr) {
    if (entry->key.generation != key.generation ||
        entry->key.preserve_dir != key.preserve_dir ||
        entry->key.target_boot_incarnation != key.target_boot_incarnation) {
      return Preserve_trx_prepared_status::STALE_GENERATION;
    }
    snapshot->key = entry->key;
    snapshot->facts = {};
    snapshot->state = entry->state.load(std::memory_order_acquire);
    snapshot->record_lock_plan_owned = false;
    snapshot->semantic_bundle_owned = false;
    copy_semantic_binlog_configuration(nullptr, snapshot);
    snapshot->native_binlog_handle_owned = false;
    snapshot->resurrection_entry_owned = false;
    snapshot->targeted_publication_journal_owned = false;
    return Preserve_trx_prepared_status::OK;
  }
  if (!prepared_token_keys_match(publication->key, key)) {
    return Preserve_trx_prepared_status::STALE_GENERATION;
  }
  snapshot->key = publication->key;
  snapshot->facts = publication->facts;
  snapshot->state = entry->state.load(std::memory_order_acquire);
  snapshot->record_lock_plan_owned = entry->resources.has_record_lock_plan();
  snapshot->semantic_bundle_owned = entry->resources.has_semantic_bundle();
  const auto *semantic_bundle = entry->resources.m_impl == nullptr
                                    ? nullptr
                                    : entry->resources.m_impl->semantic_bundle.get();
  copy_semantic_binlog_configuration(semantic_bundle, snapshot);
  snapshot->native_binlog_handle_owned =
      entry->resources.has_native_binlog_handle();
  snapshot->resurrection_entry_owned =
      entry->resources.has_resurrection_entry();
  snapshot->targeted_publication_journal_owned =
      entry->resources.m_impl != nullptr &&
      entry->resources.m_impl->targeted_publication_journal != nullptr;
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::find_unique_adopted(
    const std::string &epoch_id, const std::string &token,
    Preserve_trx_prepared_token_snapshot *snapshot) const {
  if (epoch_id.empty() || token.empty() || snapshot == nullptr) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  std::vector<std::shared_ptr<Preserve_trx_prepared_token_entry>> candidates;
  {
    std::lock_guard<std::mutex> guard(m_state->mutex);
    for (const auto &item : m_state->entries) {
      if (item.first.epoch_id == epoch_id && item.first.token == token) {
        candidates.push_back(item.second);
      }
    }
  }
  bool found = false;
  for (const auto &entry : candidates) {
    std::lock_guard<std::mutex> guard(entry->mutex);
    if (entry->physical_promotion_pins != 0 ||
        entry->state.load(std::memory_order_acquire) !=
            Preserve_trx_prepared_token_state::ADOPTED_LOCKED) {
      continue;
    }
    const auto publication = std::atomic_load_explicit(
        &entry->publication, std::memory_order_acquire);
    if (publication == nullptr) continue;
    if (found) return Preserve_trx_prepared_status::DIGEST_CONFLICT;
    snapshot->key = publication->key;
    snapshot->facts = publication->facts;
    snapshot->state = Preserve_trx_prepared_token_state::ADOPTED_LOCKED;
    snapshot->record_lock_plan_owned = entry->resources.has_record_lock_plan();
    snapshot->semantic_bundle_owned = entry->resources.has_semantic_bundle();
    const auto *semantic_bundle =
        entry->resources.m_impl == nullptr
            ? nullptr
            : entry->resources.m_impl->semantic_bundle.get();
    copy_semantic_binlog_configuration(semantic_bundle, snapshot);
    snapshot->native_binlog_handle_owned =
        entry->resources.has_native_binlog_handle();
    snapshot->resurrection_entry_owned =
        entry->resources.has_resurrection_entry();
    snapshot->targeted_publication_journal_owned =
        entry->resources.m_impl != nullptr &&
        entry->resources.m_impl->targeted_publication_journal != nullptr;
    found = true;
  }
  return found ? Preserve_trx_prepared_status::OK
               : candidates.empty() ? Preserve_trx_prepared_status::NOT_FOUND
                                    : Preserve_trx_prepared_status::INVALID_STATE;
}

Preserve_trx_prepared_registry_counts
Preserve_trx_prepared_token_registry::status_counts() const {
  Preserve_trx_prepared_registry_counts counts;
  std::vector<std::shared_ptr<Preserve_trx_prepared_token_entry>> entries;
  {
    std::lock_guard<std::mutex> guard(m_state->mutex);
    entries.reserve(m_state->entries.size());
    for (const auto &item : m_state->entries) entries.push_back(item.second);
  }
  for (const auto &entry : entries) {
    std::lock_guard<std::mutex> guard(entry->mutex);
    if (entry->retired_from_registry) continue;
    ++counts.registered_tokens;
    switch (entry->state.load(std::memory_order_acquire)) {
      case Preserve_trx_prepared_token_state::OBJECTS_RECEIVING:
      case Preserve_trx_prepared_token_state::PREWARMING:
      case Preserve_trx_prepared_token_state::PREWARMED_PENDING_FINAL_FACT:
      case Preserve_trx_prepared_token_state::READY_FACTS_PENDING_LEASE:
        ++counts.prewarm_pending_tokens;
        break;
      case Preserve_trx_prepared_token_state::READY_FOR_GATE:
        ++counts.ready_tokens;
        break;
      case Preserve_trx_prepared_token_state::ADOPTING:
        ++counts.adopting_tokens;
        break;
      case Preserve_trx_prepared_token_state::ADOPTED_LOCKED:
        ++counts.adopted_tokens;
        break;
      case Preserve_trx_prepared_token_state::CLEANUP_TAINTED:
      case Preserve_trx_prepared_token_state::ATTACH_TAINTED:
        ++counts.tainted_tokens;
        break;
      default:
        break;
    }
  }
  return counts;
}

void Preserve_trx_prepared_token_registry::invalidate_incarnation(
    const std::string &current_boot_incarnation) {
  std::vector<std::shared_ptr<Preserve_trx_prepared_token_entry>> entries;
  std::vector<Preserve_trx_prepared_token_resources> retired_resources;
  {
    std::lock_guard<std::mutex> guard(m_state->mutex);
    for (const auto &item : m_state->entries) entries.push_back(item.second);
  }
  for (const auto &entry : entries) {
    std::unique_lock<std::mutex> guard(entry->mutex);
    if (entry->key.target_boot_incarnation == current_boot_incarnation) {
      continue;
    }
    auto expected = entry->state.load(std::memory_order_acquire);
    if (entry->preparing || entry->physical_promotion_pins != 0 ||
        prepared_state_has_live_or_ambiguous_owner(expected)) {
      continue;
    }
#ifndef NDEBUG
    guard.unlock();
    run_prepared_registry_probe(
        Preserve_trx_prepared_registry_probe_point::INVALIDATE_BEFORE_RETIRE);
    guard.lock();
#endif
    expected = entry->state.load(std::memory_order_acquire);
    if (entry->preparing || entry->physical_promotion_pins != 0 ||
        prepared_state_has_live_or_ambiguous_owner(expected)) {
      continue;
    }
    if (!entry->state.compare_exchange_strong(
            expected, Preserve_trx_prepared_token_state::STALE_GENERATION,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
      continue;
    }
    retired_resources.push_back(std::move(entry->resources));
    entry->preparing = false;
    entry->preparing_generation = 0;
  }
}

size_t Preserve_trx_prepared_token_registry::expire_ready_facts_pending_lease(
    const std::string &epoch_scope, const std::string &epoch_id,
    uint64_t now_us) {
  std::vector<std::shared_ptr<Preserve_trx_prepared_token_entry>> entries;
  {
    std::lock_guard<std::mutex> guard(m_state->mutex);
    for (const auto &item : m_state->entries) {
      if (item.first.epoch_scope == epoch_scope &&
          item.first.epoch_id == epoch_id) {
        entries.push_back(item.second);
      }
    }
  }
  size_t expired = 0;
  for (const auto &entry : entries) {
    std::lock_guard<std::mutex> guard(entry->mutex);
    auto expected =
        Preserve_trx_prepared_token_state::READY_FACTS_PENDING_LEASE;
    if (entry->preparing ||
        entry->state.load(std::memory_order_acquire) != expected) {
      continue;
    }
    const auto publication = std::atomic_load_explicit(
        &entry->publication, std::memory_order_acquire);
    if (publication == nullptr || entry->physical_promotion_pins != 0 ||
        publication->facts.epoch_prepare_deadline_us > now_us) {
      continue;
    }
    if (!entry->state.compare_exchange_strong(
            expected, Preserve_trx_prepared_token_state::NOT_READY,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
      continue;
    }
    entry->resources.reset();
    ++expired;
  }
  return expired;
}

Preserve_trx_prepared_expire_result
Preserve_trx_prepared_token_registry::expire_once(uint64_t now_us) {
  Preserve_trx_prepared_expire_result result;
  std::vector<std::shared_ptr<Preserve_trx_prepared_token_entry>> entries;
  std::vector<Preserve_trx_prepared_token_key> terminal_entries;
  {
    std::lock_guard<std::mutex> guard(m_state->mutex);
    entries.reserve(m_state->entries.size());
    for (const auto &item : m_state->entries) entries.push_back(item.second);
  }

  for (const auto &entry : entries) {
    std::lock_guard<std::mutex> guard(entry->mutex);
    if (entry->preparing || entry->physical_promotion_pins != 0) continue;

    auto state = entry->state.load(std::memory_order_acquire);
    if (state == Preserve_trx_prepared_token_state::NOT_READY ||
        state ==
            Preserve_trx_prepared_token_state::ACTIVE_ARTIFACTS_CLEANED) {
      terminal_entries.push_back(entry->key);
      continue;
    }
    const auto publication = std::atomic_load_explicit(
        &entry->publication, std::memory_order_acquire);
    if (publication == nullptr) continue;
    if ((state ==
             Preserve_trx_prepared_token_state::READY_FACTS_PENDING_LEASE ||
         state == Preserve_trx_prepared_token_state::READY_FOR_GATE) &&
        publication->facts.epoch_prepare_deadline_us <= now_us) {
      if (entry->state.compare_exchange_strong(
              state, Preserve_trx_prepared_token_state::NOT_READY,
              std::memory_order_acq_rel, std::memory_order_acquire)) {
        entry->resources.reset();
        ++result.ready_expired;
      }
      continue;
    }

    if (state == Preserve_trx_prepared_token_state::ADOPTED_LOCKED &&
        publication->facts.client_resume_deadline_us <= now_us) {
      if (entry->state.compare_exchange_strong(
              state, Preserve_trx_prepared_token_state::CLEANUP_PENDING,
              std::memory_order_acq_rel, std::memory_order_acquire)) {
        /*
          The online receiver reaper does not own a physical-fence lease or a
          rollback-capable owner. Keep the adopted resources visible and taint
          rather than guessing that rollback is safe.
        */
        entry->state.store(Preserve_trx_prepared_token_state::CLEANUP_TAINTED,
                           std::memory_order_release);
        ++result.adopted_tainted;
      }
      continue;
    }

    if (state == Preserve_trx_prepared_token_state::ACTIVE) {
      if (entry->state.compare_exchange_strong(
              state,
              Preserve_trx_prepared_token_state::ACTIVE_ARTIFACTS_CLEANED,
              std::memory_order_acq_rel, std::memory_order_acquire)) {
        /* ACTIVE owns the user transaction; only Preserve-owned resources go. */
        entry->resources.reset();
        ++result.active_artifacts_cleaned;
      }
    }
  }
  for (const auto &key : terminal_entries) {
    (void)purge_token(key);
  }
  return result;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::purge_token(
    const Preserve_trx_prepared_token_key &key) {
  Preserve_trx_prepared_token_resources retired_resources;
  {
    std::lock_guard<std::mutex> guard(m_state->mutex);
    const auto found = m_state->entries.find(prepared_token_locator(key));
    if (found == m_state->entries.end()) {
      return Preserve_trx_prepared_status::NOT_FOUND;
    }
    std::lock_guard<std::mutex> entry_guard(found->second->mutex);
    auto expected = found->second->state.load(std::memory_order_acquire);
    if (found->second->preparing ||
        found->second->physical_promotion_pins != 0 ||
        prepared_state_has_live_or_ambiguous_owner(expected)) {
      return Preserve_trx_prepared_status::INVALID_STATE;
    }
    if (!found->second->state.compare_exchange_strong(
            expected, Preserve_trx_prepared_token_state::STALE_GENERATION,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
      return Preserve_trx_prepared_status::INVALID_STATE;
    }
    found->second->retired_from_registry = true;
    retired_resources = std::move(found->second->resources);
    found->second->preparing = false;
    found->second->preparing_generation = 0;
    m_state->entries.erase(found);
  }
  return Preserve_trx_prepared_status::OK;
}

void Preserve_trx_prepared_token_registry::purge_epoch(
    const std::string &epoch_scope, const std::string &epoch_id) {
  std::vector<Preserve_trx_prepared_token_resources> retired_resources;
  {
    std::lock_guard<std::mutex> guard(m_state->mutex);
    for (auto it = m_state->entries.begin(); it != m_state->entries.end();) {
      if (it->first.epoch_scope == epoch_scope &&
          it->first.epoch_id == epoch_id) {
        std::lock_guard<std::mutex> entry_guard(it->second->mutex);
        auto expected = it->second->state.load(std::memory_order_acquire);
        if (it->second->preparing ||
            it->second->physical_promotion_pins != 0 ||
            prepared_state_has_live_or_ambiguous_owner(expected)) {
          ++it;
          continue;
        }
        if (!it->second->state.compare_exchange_strong(
                expected,
                Preserve_trx_prepared_token_state::STALE_GENERATION,
                std::memory_order_acq_rel, std::memory_order_acquire)) {
          ++it;
          continue;
        }
        it->second->retired_from_registry = true;
        retired_resources.push_back(std::move(it->second->resources));
        it->second->preparing = false;
        it->second->preparing_generation = 0;
        it = m_state->entries.erase(it);
      } else {
        ++it;
      }
    }
  }
}

size_t
Preserve_trx_prepared_token_registry::discard_all_for_process_shutdown() {
  std::vector<Preserve_trx_prepared_token_resources> retired_resources;
  size_t discarded = 0;
  {
    std::lock_guard<std::mutex> guard(m_state->mutex);
    discarded = m_state->entries.size();
    retired_resources.reserve(discarded);
    for (auto &item : m_state->entries) {
      auto &entry = item.second;
      std::lock_guard<std::mutex> entry_guard(entry->mutex);
      entry->retired_from_registry = true;
      entry->preparing = false;
      entry->preparing_generation = 0;
      entry->state.store(Preserve_trx_prepared_token_state::STALE_GENERATION,
                         std::memory_order_release);
      std::atomic_store_explicit(
          &entry->publication,
          std::shared_ptr<const Preserve_trx_prepared_token_publication>{},
          std::memory_order_release);
      entry->prewarm_object_set_digest.clear();
      retired_resources.push_back(std::move(entry->resources));
    }
    m_state->entries.clear();
  }
  return discarded;
}

Preserve_trx_prepared_token_registry &
preserved_trx_strict_prepared_token_registry() {
  static Preserve_trx_prepared_token_registry registry;
  return registry;
}

uint64_t preserve_trx_promotion_prepared_registered_tokens_status() {
  return preserved_trx_strict_prepared_token_registry()
      .status_counts()
      .registered_tokens;
}

uint64_t preserve_trx_promotion_prepared_prewarm_pending_tokens_status() {
  return preserved_trx_strict_prepared_token_registry()
      .status_counts()
      .prewarm_pending_tokens;
}

uint64_t preserve_trx_promotion_prepared_ready_tokens_status() {
  return preserved_trx_strict_prepared_token_registry()
      .status_counts()
      .ready_tokens;
}

uint64_t preserve_trx_promotion_prepared_adopting_tokens_status() {
  return preserved_trx_strict_prepared_token_registry()
      .status_counts()
      .adopting_tokens;
}

uint64_t preserve_trx_promotion_prepared_adopted_tokens_status() {
  return preserved_trx_strict_prepared_token_registry()
      .status_counts()
      .adopted_tokens;
}

uint64_t preserve_trx_promotion_prepared_tainted_tokens_status() {
  return preserved_trx_strict_prepared_token_registry()
      .status_counts()
      .tainted_tokens;
}

#ifndef NDEBUG
void preserved_trx_prepared_registry_set_probe_for_unit_test(
    Preserve_trx_prepared_registry_probe probe, void *context) {
  g_prepared_registry_probe = probe;
  g_prepared_registry_probe_context = context;
}
#endif

void preserved_trx_promotion_prepared_metrics_reset_for_unit_test() {
  g_resume_core_elapsed_us.store(0, std::memory_order_relaxed);
  g_resume_core_count.store(0, std::memory_order_relaxed);
  for (auto &bucket : g_resume_core_histogram) {
    bucket.store(0, std::memory_order_relaxed);
  }
  g_resume_core_max_us.store(0, std::memory_order_relaxed);
  g_resume_failure_count.store(0, std::memory_order_relaxed);
  g_resume_physical_consistency_mode.store(0, std::memory_order_relaxed);
  g_resume_real_redo_apply.store(0, std::memory_order_relaxed);
  g_promotion_fence_lease_wait_us.store(0, std::memory_order_relaxed);
  g_promotion_fence_digest_compare_us.store(0, std::memory_order_relaxed);
  g_promotion_fence_revalidate_us.store(0, std::memory_order_relaxed);
  g_promotion_lock_page_get_count.store(0, std::memory_order_relaxed);
  g_promotion_lock_page_get_us.store(0, std::memory_order_relaxed);
  g_promotion_lock_image_resolves.store(0, std::memory_order_relaxed);
  g_promotion_lock_apply_us.store(0, std::memory_order_relaxed);
  g_promotion_lock_accounting_bits.store(0, std::memory_order_relaxed);
  g_receiver_lock_plan_capacity_bytes.store(0, std::memory_order_relaxed);
  g_receiver_lock_plan_epoch_peak_bytes.store(0, std::memory_order_relaxed);
  g_receiver_lock_plan_subpool_cap_bytes.store(0, std::memory_order_relaxed);
  g_resource_admission_open_failed_count.store(0, std::memory_order_relaxed);
  g_resume_binlog_payload_read_bytes.store(0, std::memory_order_relaxed);
  g_resume_binlog_payload_write_bytes.store(0, std::memory_order_relaxed);
  g_resume_binlog_rename_count.store(0, std::memory_order_relaxed);
  g_resume_binlog_attach_count.store(0, std::memory_order_relaxed);
}

void preserved_trx_promotion_resume_core_note(uint64_t elapsed_us,
                                               bool success) {
  g_resume_core_elapsed_us.store(elapsed_us, std::memory_order_relaxed);
  if (!success) {
    g_resume_failure_count.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  g_resume_core_count.fetch_add(1, std::memory_order_relaxed);
  update_max(&g_resume_core_max_us, elapsed_us);
  const auto bucket = std::lower_bound(kResumeCoreHistogramUpperBoundsUs.begin(),
                                       kResumeCoreHistogramUpperBoundsUs.end(),
                                       elapsed_us);
  const size_t bucket_index = static_cast<size_t>(
      std::distance(kResumeCoreHistogramUpperBoundsUs.begin(), bucket));
  g_resume_core_histogram[bucket_index].fetch_add(1,
                                                  std::memory_order_relaxed);
}

void preserved_trx_promotion_prepared_note_evidence(
    Preserve_trx_physical_consistency_mode mode, bool real_redo_apply) {
  g_resume_physical_consistency_mode.store(static_cast<uint64_t>(mode),
                                            std::memory_order_relaxed);
  g_resume_real_redo_apply.store(real_redo_apply ? 1 : 0,
                                 std::memory_order_relaxed);
}

void preserved_trx_promotion_prepared_note_fence_metrics(
    uint64_t lease_wait_us, uint64_t digest_compare_us,
    uint64_t revalidate_us) {
  g_promotion_fence_lease_wait_us.store(lease_wait_us,
                                        std::memory_order_relaxed);
  g_promotion_fence_digest_compare_us.store(digest_compare_us,
                                             std::memory_order_relaxed);
  g_promotion_fence_revalidate_us.store(revalidate_us,
                                        std::memory_order_relaxed);
}

void preserved_trx_promotion_prepared_note_lock_metrics(
    uint64_t page_get_count, uint64_t page_get_us, uint64_t image_resolves,
    uint64_t apply_us, uint64_t accounting_bits) {
  g_promotion_lock_page_get_count.store(page_get_count,
                                        std::memory_order_relaxed);
  g_promotion_lock_page_get_us.store(page_get_us, std::memory_order_relaxed);
  g_promotion_lock_image_resolves.store(image_resolves,
                                         std::memory_order_relaxed);
  g_promotion_lock_apply_us.store(apply_us, std::memory_order_relaxed);
  g_promotion_lock_accounting_bits.store(accounting_bits,
                                          std::memory_order_relaxed);
}

void preserved_trx_promotion_prepared_note_lock_plan_metrics(
    uint64_t capacity_bytes, uint64_t epoch_peak_bytes,
    uint64_t subpool_cap_bytes) {
  g_receiver_lock_plan_capacity_bytes.store(capacity_bytes,
                                             std::memory_order_relaxed);
  update_max(&g_receiver_lock_plan_epoch_peak_bytes, epoch_peak_bytes);
  g_receiver_lock_plan_subpool_cap_bytes.store(subpool_cap_bytes,
                                                std::memory_order_relaxed);
}

void preserved_trx_promotion_prepared_note_resource_open_failure() {
  g_resource_admission_open_failed_count.fetch_add(1,
                                                    std::memory_order_relaxed);
}

void preserved_trx_promotion_prepared_note_resume_binlog_io(
    uint64_t payload_read_bytes, uint64_t payload_write_bytes,
    uint64_t rename_count) {
  g_resume_binlog_payload_read_bytes.fetch_add(payload_read_bytes,
                                                std::memory_order_relaxed);
  g_resume_binlog_payload_write_bytes.fetch_add(payload_write_bytes,
                                                 std::memory_order_relaxed);
  g_resume_binlog_rename_count.fetch_add(rename_count,
                                          std::memory_order_relaxed);
  g_resume_binlog_attach_count.fetch_add(1, std::memory_order_relaxed);
}

uint64_t preserve_trx_promotion_resume_core_elapsed_us_status() {
  return g_resume_core_elapsed_us.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_promotion_resume_core_count_status() {
  return g_resume_core_count.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_promotion_resume_core_p50_us_status() {
  return resume_core_percentile(50, 100);
}

uint64_t preserve_trx_promotion_resume_core_p95_us_status() {
  return resume_core_percentile(95, 100);
}

uint64_t preserve_trx_promotion_resume_core_p99_us_status() {
  return resume_core_percentile(99, 100);
}

uint64_t preserve_trx_promotion_resume_core_max_us_status() {
  return g_resume_core_max_us.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_promotion_resume_failure_count_status() {
  return g_resume_failure_count.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_resume_physical_consistency_mode_status() {
  return g_resume_physical_consistency_mode.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_resume_real_redo_apply_status() {
  return g_resume_real_redo_apply.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_promotion_fence_lease_wait_us_status() {
  return g_promotion_fence_lease_wait_us.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_promotion_fence_digest_compare_us_status() {
  return g_promotion_fence_digest_compare_us.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_promotion_fence_revalidate_us_status() {
  return g_promotion_fence_revalidate_us.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_promotion_lock_page_get_count_status() {
  return g_promotion_lock_page_get_count.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_promotion_lock_page_get_us_status() {
  return g_promotion_lock_page_get_us.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_promotion_lock_image_resolves_status() {
  return g_promotion_lock_image_resolves.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_promotion_lock_apply_us_status() {
  return g_promotion_lock_apply_us.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_promotion_lock_accounting_bits_status() {
  return g_promotion_lock_accounting_bits.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_receiver_lock_plan_capacity_bytes_status() {
  return g_receiver_lock_plan_capacity_bytes.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_receiver_lock_plan_epoch_peak_bytes_status() {
  return g_receiver_lock_plan_epoch_peak_bytes.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_receiver_lock_plan_subpool_cap_bytes_status() {
  return g_receiver_lock_plan_subpool_cap_bytes.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_resource_admission_open_failed_count_status() {
  return g_resource_admission_open_failed_count.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_resume_binlog_payload_read_bytes_status() {
  return g_resume_binlog_payload_read_bytes.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_resume_binlog_payload_write_bytes_status() {
  return g_resume_binlog_payload_write_bytes.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_resume_binlog_rename_count_status() {
  return g_resume_binlog_rename_count.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_resume_binlog_attach_count_status() {
  return g_resume_binlog_attach_count.load(std::memory_order_relaxed);
}
