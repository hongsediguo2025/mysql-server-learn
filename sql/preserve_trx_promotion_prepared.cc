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
#include <mutex>
#include <utility>

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
