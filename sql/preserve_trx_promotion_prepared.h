/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. */

#ifndef SQL_PRESERVE_TRX_PROMOTION_PREPARED_INCLUDED
#define SQL_PRESERVE_TRX_PROMOTION_PREPARED_INCLUDED

#include <cstdint>
#include <string>

enum class Preserve_trx_physical_consistency_mode : uint8_t {
  NONE = 0,
  TEST_SAME_INSTANCE_ATTACH_ONLY,
  TEST_FROZEN_DATADIR_COPY,
  PRODUCTION_REDO_APPLY_FENCE
};

struct Preserve_trx_physical_fence_proof {
  Preserve_trx_physical_consistency_mode consistency_mode{
      Preserve_trx_physical_consistency_mode::NONE};
  std::string source_lineage_uuid;
  std::string target_server_uuid;
  std::string target_boot_incarnation;
  uint64_t provider_generation{0};
  uint64_t source_fence_lsn{0};
  uint64_t target_frozen_lsn{0};
  std::string epoch_fact_digest;
  std::string final_lock_generation_digest;
  std::string page_layout_digest;
  std::string dictionary_generation_digest;
  bool apply_frozen{false};
};

enum class Preserve_trx_physical_fence_status : uint8_t {
  OK = 0,
  INVALID_ARGUMENT,
  MISSING_PROVIDER,
  MODE_MISMATCH,
  ACQUIRE_FAILED,
  REVALIDATE_FAILED,
  PROVIDER_CONTRACT_VIOLATION
};

struct Preserve_trx_physical_fence_provider_ops {
  Preserve_trx_physical_consistency_mode consistency_mode{
      Preserve_trx_physical_consistency_mode::NONE};
  bool (*acquire)(const Preserve_trx_physical_fence_proof &expected,
                  Preserve_trx_physical_fence_proof *actual,
                  void **opaque_lease){nullptr};
  bool (*revalidate)(void *opaque_lease, uint64_t expected_provider_generation,
                     Preserve_trx_physical_fence_proof *actual){nullptr};
  void (*release)(void *opaque_lease){nullptr};
};

class Preserve_trx_physical_fence_lease_factory;

class Preserve_trx_physical_fence_lease {
 public:
  Preserve_trx_physical_fence_lease() = default;
  Preserve_trx_physical_fence_lease(
      const Preserve_trx_physical_fence_lease &) = delete;
  Preserve_trx_physical_fence_lease &operator=(
      const Preserve_trx_physical_fence_lease &) = delete;
  Preserve_trx_physical_fence_lease(
      Preserve_trx_physical_fence_lease &&other) noexcept;
  Preserve_trx_physical_fence_lease &operator=(
      Preserve_trx_physical_fence_lease &&other) noexcept;
  ~Preserve_trx_physical_fence_lease();

  bool acquired() const { return m_opaque_lease != nullptr; }
  const Preserve_trx_physical_fence_proof &proof() const { return m_proof; }
  Preserve_trx_physical_fence_status revalidate();
  void release();

 private:
  Preserve_trx_physical_fence_provider_ops m_ops;
  Preserve_trx_physical_fence_proof m_expected;
  Preserve_trx_physical_fence_proof m_proof;
  void *m_opaque_lease{nullptr};

  friend class Preserve_trx_physical_fence_lease_factory;

  friend Preserve_trx_physical_fence_status
  preserved_trx_acquire_production_physical_fence_lease(
      const Preserve_trx_physical_fence_proof &,
      Preserve_trx_physical_fence_lease *);
  friend Preserve_trx_physical_fence_status
  preserved_trx_acquire_physical_fence_lease_for_unit_test(
      const Preserve_trx_physical_fence_proof &,
      Preserve_trx_physical_fence_lease *);
};

bool preserved_trx_register_production_physical_fence_provider(
    const Preserve_trx_physical_fence_provider_ops &ops);
void preserved_trx_set_physical_fence_provider_for_unit_test(
    const Preserve_trx_physical_fence_provider_ops *ops);

Preserve_trx_physical_fence_status
preserved_trx_acquire_production_physical_fence_lease(
    const Preserve_trx_physical_fence_proof &expected,
    Preserve_trx_physical_fence_lease *lease);
Preserve_trx_physical_fence_status
preserved_trx_acquire_physical_fence_lease_for_unit_test(
    const Preserve_trx_physical_fence_proof &expected,
    Preserve_trx_physical_fence_lease *lease);

bool preserved_trx_physical_fence_proof_is_valid(
    const Preserve_trx_physical_fence_proof &proof);

void preserved_trx_promotion_prepared_metrics_reset_for_unit_test();
void preserved_trx_promotion_resume_core_note(uint64_t elapsed_us,
                                               bool success);
void preserved_trx_promotion_prepared_set_evidence_for_unit_test(
    Preserve_trx_physical_consistency_mode mode, bool real_redo_apply,
    bool real_ha_promotion);
void preserved_trx_promotion_prepared_note_fence_metrics(
    uint64_t lease_wait_us, uint64_t digest_compare_us);
void preserved_trx_promotion_prepared_note_lock_metrics(
    uint64_t page_get_count, uint64_t page_get_us, uint64_t image_resolves,
    uint64_t apply_us, uint64_t accounting_bits);
void preserved_trx_promotion_prepared_note_lock_plan_metrics(
    uint64_t capacity_bytes, uint64_t epoch_peak_bytes,
    uint64_t subpool_cap_bytes);
void preserved_trx_promotion_prepared_note_resource_open_failure();
void preserved_trx_promotion_prepared_note_resume_binlog_io(
    uint64_t payload_read_bytes, uint64_t payload_write_bytes,
    uint64_t rename_count);

uint64_t preserve_trx_promotion_resume_core_elapsed_us_status();
uint64_t preserve_trx_promotion_resume_core_count_status();
uint64_t preserve_trx_promotion_resume_core_p50_us_status();
uint64_t preserve_trx_promotion_resume_core_p95_us_status();
uint64_t preserve_trx_promotion_resume_core_p99_us_status();
uint64_t preserve_trx_promotion_resume_core_max_us_status();
uint64_t preserve_trx_promotion_resume_failure_count_status();
uint64_t preserve_trx_resume_physical_consistency_mode_status();
uint64_t preserve_trx_resume_real_redo_apply_status();
uint64_t preserve_trx_resume_real_ha_promotion_status();
uint64_t preserve_trx_promotion_fence_lease_wait_us_status();
uint64_t preserve_trx_promotion_fence_digest_compare_us_status();
uint64_t preserve_trx_promotion_lock_page_get_count_status();
uint64_t preserve_trx_promotion_lock_page_get_us_status();
uint64_t preserve_trx_promotion_lock_image_resolves_status();
uint64_t preserve_trx_promotion_lock_apply_us_status();
uint64_t preserve_trx_promotion_lock_accounting_bits_status();
uint64_t preserve_trx_receiver_lock_plan_capacity_bytes_status();
uint64_t preserve_trx_receiver_lock_plan_epoch_peak_bytes_status();
uint64_t preserve_trx_receiver_lock_plan_subpool_cap_bytes_status();
uint64_t preserve_trx_resource_admission_open_failed_count_status();
uint64_t preserve_trx_resume_binlog_payload_read_bytes_status();
uint64_t preserve_trx_resume_binlog_payload_write_bytes_status();
uint64_t preserve_trx_resume_binlog_rename_count_status();

#endif  // SQL_PRESERVE_TRX_PROMOTION_PREPARED_INCLUDED
