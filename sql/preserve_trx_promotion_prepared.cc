/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation. */

#include "sql/preserve_trx_promotion_prepared.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <limits>
#include <map>
#include <mutex>
#include <utility>
#include <vector>

#include <openssl/sha.h>

#include "sql/binlog_preserve_prepared.h"
#include "sql/preserve_trx_resource.h"

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
  }
};

class Preserve_trx_prepared_token_resources::Impl {
 public:
  Preserve_memory_lease lock_plan_memory;
  Preserve_native_binlog_resource_lease native_binlog_resources;
  std::unique_ptr<lock_preserve_metadata_plan_t> record_lock_plan;
  std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle>
      native_binlog_handle;
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
  bool preparing{false};
  bool retired_from_registry{false};
  uint64_t preparing_generation{0};
};

struct Preserve_trx_prepared_token_locator {
  std::string source_uuid;
  std::string epoch_id;
  std::string token;

  bool operator<(const Preserve_trx_prepared_token_locator &other) const {
    if (source_uuid != other.source_uuid) return source_uuid < other.source_uuid;
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
bool g_production_provider_registered{false};
Preserve_trx_physical_fence_provider_ops g_production_provider;
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
std::atomic<uint64_t> g_resume_real_ha_promotion{0};
std::atomic<uint64_t> g_promotion_fence_lease_wait_us{0};
std::atomic<uint64_t> g_promotion_fence_digest_compare_us{0};
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
         actual.apply_frozen;
}

bool prepared_token_key_is_valid(const Preserve_trx_prepared_token_key &key) {
  return !key.preserve_dir.empty() && !key.source_uuid.empty() &&
         !key.epoch_id.empty() && !key.token.empty() &&
         !key.target_boot_incarnation.empty() && key.generation != 0;
}

Preserve_trx_prepared_token_locator prepared_token_locator(
    const Preserve_trx_prepared_token_key &key) {
  return {key.source_uuid, key.epoch_id, key.token};
}

bool prepared_token_keys_match(const Preserve_trx_prepared_token_key &lhs,
                               const Preserve_trx_prepared_token_key &rhs) {
  return lhs.preserve_dir == rhs.preserve_dir &&
         lhs.source_uuid == rhs.source_uuid && lhs.epoch_id == rhs.epoch_id &&
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
         digest_is_sha256_hex(facts.epoch_fact_digest) &&
         digest_is_sha256_hex(facts.final_lock_generation_digest) &&
         digest_is_sha256_hex(facts.page_layout_digest) &&
         digest_is_sha256_hex(facts.dictionary_generation_digest) &&
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
  return key.source_uuid + "\x1f" + key.epoch_id + "\x1f" + key.token +
         "\x1f" + std::to_string(key.generation);
}

Mysql_binlog_preserve_token_identity prepared_binlog_identity(
    const Preserve_trx_prepared_token_key &key) {
  Mysql_binlog_preserve_token_identity identity;
  identity.source_uuid = key.source_uuid;
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

Preserve_trx_physical_fence_lease::Preserve_trx_physical_fence_lease(
    Preserve_trx_physical_fence_lease &&other) noexcept
    : m_ops(other.m_ops),
      m_expected(std::move(other.m_expected)),
      m_proof(std::move(other.m_proof)),
      m_opaque_lease(other.m_opaque_lease) {
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
  other.m_ops = {};
  other.m_opaque_lease = nullptr;
  return *this;
}

Preserve_trx_physical_fence_lease::~Preserve_trx_physical_fence_lease() {
  release();
}

Preserve_trx_physical_fence_status
Preserve_trx_physical_fence_lease::revalidate() {
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

void Preserve_trx_physical_fence_lease::release() {
  if (m_opaque_lease != nullptr && m_ops.release != nullptr) {
    m_ops.release(m_opaque_lease);
  }
  m_ops = {};
  m_expected = {};
  m_proof = {};
  m_opaque_lease = nullptr;
}

bool preserved_trx_register_production_physical_fence_provider(
    const Preserve_trx_physical_fence_provider_ops &ops) {
  if (ops.consistency_mode != Preserve_trx_physical_consistency_mode::
                                  PRODUCTION_REDO_APPLY_FENCE ||
      !provider_ops_are_complete(ops)) {
    return false;
  }
  std::lock_guard<std::mutex> guard(g_provider_mutex);
  if (g_production_provider_registered) return false;
  g_production_provider = ops;
  g_production_provider_registered = true;
  return true;
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
preserved_trx_acquire_production_physical_fence_lease(
    const Preserve_trx_physical_fence_proof &expected,
    Preserve_trx_physical_fence_lease *lease) {
  Preserve_trx_physical_fence_provider_ops ops;
  {
    std::lock_guard<std::mutex> guard(g_provider_mutex);
    if (!g_production_provider_registered) {
      return Preserve_trx_physical_fence_status::MISSING_PROVIDER;
    }
    ops = g_production_provider;
  }
  return acquire_with_provider(&ops, expected, lease);
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
  return proof.consistency_mode != Preserve_trx_physical_consistency_mode::NONE &&
         !proof.source_lineage_uuid.empty() &&
         !proof.target_server_uuid.empty() &&
         !proof.target_boot_incarnation.empty() &&
         proof.provider_generation != 0 && proof.source_fence_lsn != 0 &&
         proof.target_frozen_lsn == proof.source_fence_lsn &&
         digest_is_sha256_hex(proof.epoch_fact_digest) &&
         digest_is_sha256_hex(proof.final_lock_generation_digest) &&
         digest_is_sha256_hex(proof.page_layout_digest) &&
         digest_is_sha256_hex(proof.dictionary_generation_digest) &&
         proof.apply_frozen;
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
  if (!append_canonical_string(&canonical, facts->epoch_fact_digest) ||
      !append_canonical_string(&canonical,
                               facts->final_lock_generation_digest) ||
      !append_canonical_string(&canonical, facts->page_layout_digest) ||
      !append_canonical_string(&canonical,
                               facts->dictionary_generation_digest) ||
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

bool Preserve_trx_prepared_token_resources::has_native_binlog_handle() const {
  return m_impl != nullptr && m_impl->native_binlog_handle != nullptr;
}

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
  const auto status = mysql_binlog_preserve_prepare_detached_cache(
      capability, facts, reader, std::move(m_impl->native_binlog_resources),
      &m_impl->native_binlog_handle);
  if (status != Mysql_binlog_preserve_cache_status::OK)
    m_impl->acquired = false;
  return status;
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
  const bool has_record_lock_facts =
      facts.record_lock_unique_pages != 0 ||
      facts.record_lock_bitmap_entries != 0 || facts.record_lock_bits != 0;
  const lock_preserve_metadata_plan_t *record_lock_plan =
      resources.m_impl == nullptr ? nullptr
                                  : resources.m_impl->record_lock_plan.get();
  if (lease == nullptr || !lease->active() || lease->m_entry == nullptr ||
      !resources.acquired() || !preserved_trx_finalize_token_facts(&facts) ||
      facts.target_boot_incarnation !=
          lease->m_key.target_boot_incarnation ||
      facts.lock_plan_capacity_bytes != resources.lock_plan_bytes() ||
      facts.native_binlog_capacity_bytes != resources.native_binlog_bytes() ||
      (has_record_lock_facts &&
       (record_lock_plan == nullptr || !record_lock_plan->ready() ||
        facts.record_lock_unique_pages > record_lock_plan->entry_count() ||
        facts.record_lock_bitmap_entries != record_lock_plan->entry_count() ||
        facts.record_lock_bits != record_lock_plan->bitmap_bits() ||
        facts.lock_plan_capacity_bytes != record_lock_plan->capacity_bytes())) ||
      (!has_record_lock_facts && record_lock_plan != nullptr) ||
      (facts.binlog_cache_present &&
       (!facts.binlog_handle_ready ||
        !resources.has_native_binlog_handle() ||
        !resources.m_impl->native_binlog_handle->matches(
            prepared_binlog_identity(lease->m_key),
            facts.binlog_handle_digest, facts.binlog_cache_length,
            facts.binlog_cache_file_backed))) ||
      (!facts.binlog_cache_present && resources.has_native_binlog_handle())) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }

  Preserve_trx_prepared_token_resources retired_resources;
  Preserve_trx_prepared_status status = Preserve_trx_prepared_status::OK;
  {
    std::lock_guard<std::mutex> guard(lease->m_entry->mutex);
    if (!lease->m_entry->preparing ||
        lease->m_entry->preparing_generation != lease->m_key.generation) {
      lease->m_active = false;
      lease->m_entry.reset();
      lease->m_registry.reset();
      return Preserve_trx_prepared_status::STALE_GENERATION;
    }
    const auto current = std::atomic_load_explicit(
        &lease->m_entry->publication, std::memory_order_acquire);
    if (current != nullptr &&
        current->key.generation > lease->m_key.generation) {
      status = Preserve_trx_prepared_status::STALE_GENERATION;
    } else if (current != nullptr &&
               current->key.generation == lease->m_key.generation) {
      if (current->facts.canonical_digest == facts.canonical_digest) {
        status = Preserve_trx_prepared_status::IDEMPOTENT;
      } else {
        lease->m_entry->state.store(Preserve_trx_prepared_token_state::CORRUPT,
                                    std::memory_order_release);
        retired_resources = std::move(lease->m_entry->resources);
        status = Preserve_trx_prepared_status::DIGEST_CONFLICT;
      }
    } else {
      auto publication =
          std::make_shared<const Preserve_trx_prepared_token_publication>(
              Preserve_trx_prepared_token_publication{lease->m_key,
                                                       std::move(facts)});
      retired_resources = std::move(lease->m_entry->resources);
      lease->m_entry->resources = std::move(resources);
      lease->m_entry->key = lease->m_key;
      std::atomic_store_explicit(&lease->m_entry->publication,
                                 std::move(publication),
                                 std::memory_order_release);
      lease->m_entry->state.store(
          Preserve_trx_prepared_token_state::READY_FACTS_PENDING_LEASE,
          std::memory_order_release);
    }
    lease->m_entry->preparing = false;
    lease->m_entry->preparing_generation = 0;
  }
  lease->m_active = false;
  lease->m_entry.reset();
  lease->m_registry.reset();
  return status;
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
  auto expected =
      Preserve_trx_prepared_token_state::READY_FACTS_PENDING_LEASE;
  if (entry->state.load(std::memory_order_acquire) != expected) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  const auto publication = std::atomic_load_explicit(
      &entry->publication, std::memory_order_acquire);
  if (publication == nullptr ||
      !prepared_token_keys_match(publication->key, key)) {
    return Preserve_trx_prepared_status::STALE_GENERATION;
  }
  if (!entry->state.compare_exchange_strong(
          expected, Preserve_trx_prepared_token_state::READY_FOR_GATE,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::begin_gate_adopt(
    const Preserve_trx_prepared_token_key &key, uint64_t expected_generation,
    Preserve_trx_gate_adopt_lease *lease) {
  if (!prepared_token_key_is_valid(key) ||
      expected_generation != key.generation || lease == nullptr ||
      lease->active()) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  auto entry = find_prepared_entry(m_state, key);
  if (entry == nullptr) return Preserve_trx_prepared_status::NOT_FOUND;
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
    lease->m_entry->resources = {};
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
  auto expected = Preserve_trx_prepared_token_state::ADOPTED_LOCKED;
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
         lease->m_entry->resources.has_native_binlog_handle())) {
      return Preserve_trx_prepared_status::INVALID_STATE;
    }
  }
  auto expected = Preserve_trx_prepared_token_state::ATTACHING;
  if (!lease->m_entry->state.compare_exchange_strong(
          expected, Preserve_trx_prepared_token_state::ACTIVATING,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    return Preserve_trx_prepared_status::INVALID_STATE;
  }
  lease->m_activation_started = true;
  return Preserve_trx_prepared_status::OK;
}

Preserve_trx_prepared_status
Preserve_trx_prepared_token_registry::commit_attach(
    Preserve_trx_attach_lease *lease) {
  if (lease == nullptr || !lease->active() || lease->m_entry == nullptr ||
      !lease->m_activation_started) {
    return Preserve_trx_prepared_status::INVALID_ARGUMENT;
  }
  {
    std::lock_guard<std::mutex> guard(lease->m_entry->mutex);
    const auto publication = std::atomic_load_explicit(
        &lease->m_entry->publication, std::memory_order_acquire);
    if (publication == nullptr ||
        (publication->facts.binlog_cache_present &&
         lease->m_entry->resources.has_native_binlog_handle())) {
      return Preserve_trx_prepared_status::INVALID_STATE;
    }
  }
  auto expected = Preserve_trx_prepared_token_state::ACTIVATING;
  if (!lease->m_entry->state.compare_exchange_strong(
          expected, Preserve_trx_prepared_token_state::ACTIVE,
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
    lease->m_entry->resources = {};
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
    snapshot->native_binlog_handle_owned = false;
    return Preserve_trx_prepared_status::OK;
  }
  if (!prepared_token_keys_match(publication->key, key)) {
    return Preserve_trx_prepared_status::STALE_GENERATION;
  }
  snapshot->key = publication->key;
  snapshot->facts = publication->facts;
  snapshot->state = entry->state.load(std::memory_order_acquire);
  snapshot->record_lock_plan_owned = entry->resources.has_record_lock_plan();
  snapshot->native_binlog_handle_owned =
      entry->resources.has_native_binlog_handle();
  return Preserve_trx_prepared_status::OK;
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
    std::lock_guard<std::mutex> guard(entry->mutex);
    if (entry->key.target_boot_incarnation == current_boot_incarnation) {
      continue;
    }
    auto expected = entry->state.load(std::memory_order_acquire);
    if (entry->preparing ||
        prepared_state_has_live_or_ambiguous_owner(expected)) {
      continue;
    }
#ifndef NDEBUG
    run_prepared_registry_probe(
        Preserve_trx_prepared_registry_probe_point::INVALIDATE_BEFORE_RETIRE);
#endif
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
    const std::string &source_uuid, const std::string &epoch_id,
    uint64_t now_us) {
  std::vector<std::shared_ptr<Preserve_trx_prepared_token_entry>> entries;
  {
    std::lock_guard<std::mutex> guard(m_state->mutex);
    for (const auto &item : m_state->entries) {
      if (item.first.source_uuid == source_uuid &&
          item.first.epoch_id == epoch_id) {
        entries.push_back(item.second);
      }
    }
  }
  size_t expired = 0;
  for (const auto &entry : entries) {
    auto expected =
        Preserve_trx_prepared_token_state::READY_FACTS_PENDING_LEASE;
    if (entry->state.load(std::memory_order_acquire) != expected) continue;
    const auto publication = std::atomic_load_explicit(
        &entry->publication, std::memory_order_acquire);
    if (publication == nullptr ||
        publication->facts.epoch_prepare_deadline_us > now_us) {
      continue;
    }
    if (!entry->state.compare_exchange_strong(
            expected, Preserve_trx_prepared_token_state::NOT_READY,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
      continue;
    }
    std::lock_guard<std::mutex> guard(entry->mutex);
    entry->resources = {};
    ++expired;
  }
  return expired;
}

void Preserve_trx_prepared_token_registry::purge_epoch(
    const std::string &source_uuid, const std::string &epoch_id) {
  std::vector<Preserve_trx_prepared_token_resources> retired_resources;
  {
    std::lock_guard<std::mutex> guard(m_state->mutex);
    for (auto it = m_state->entries.begin(); it != m_state->entries.end();) {
      if (it->first.source_uuid == source_uuid &&
          it->first.epoch_id == epoch_id) {
        std::lock_guard<std::mutex> entry_guard(it->second->mutex);
        auto expected = it->second->state.load(std::memory_order_acquire);
        if (it->second->preparing ||
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

Preserve_trx_prepared_token_registry &
preserved_trx_strict_prepared_token_registry() {
  static Preserve_trx_prepared_token_registry registry;
  return registry;
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
  g_resume_real_ha_promotion.store(0, std::memory_order_relaxed);
  g_promotion_fence_lease_wait_us.store(0, std::memory_order_relaxed);
  g_promotion_fence_digest_compare_us.store(0, std::memory_order_relaxed);
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

void preserved_trx_promotion_prepared_set_evidence_for_unit_test(
    Preserve_trx_physical_consistency_mode mode, bool real_redo_apply,
    bool real_ha_promotion) {
  g_resume_physical_consistency_mode.store(static_cast<uint64_t>(mode),
                                            std::memory_order_relaxed);
  g_resume_real_redo_apply.store(real_redo_apply ? 1 : 0,
                                 std::memory_order_relaxed);
  g_resume_real_ha_promotion.store(real_ha_promotion ? 1 : 0,
                                   std::memory_order_relaxed);
}

void preserved_trx_promotion_prepared_note_fence_metrics(
    uint64_t lease_wait_us, uint64_t digest_compare_us) {
  g_promotion_fence_lease_wait_us.store(lease_wait_us,
                                        std::memory_order_relaxed);
  g_promotion_fence_digest_compare_us.store(digest_compare_us,
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
  g_receiver_lock_plan_epoch_peak_bytes.store(epoch_peak_bytes,
                                               std::memory_order_relaxed);
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

uint64_t preserve_trx_resume_real_ha_promotion_status() {
  return g_resume_real_ha_promotion.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_promotion_fence_lease_wait_us_status() {
  return g_promotion_fence_lease_wait_us.load(std::memory_order_relaxed);
}

uint64_t preserve_trx_promotion_fence_digest_compare_us_status() {
  return g_promotion_fence_digest_compare_us.load(std::memory_order_relaxed);
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
