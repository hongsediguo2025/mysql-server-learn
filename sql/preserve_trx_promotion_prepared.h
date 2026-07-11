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
#include <memory>
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

enum class Preserve_trx_prepared_token_state : uint8_t {
  NOT_FOUND = 0,
  OBJECTS_RECEIVING,
  PREWARMING,
  PREWARMED_PENDING_FINAL_FACT,
  READY_FACTS_PENDING_LEASE,
  READY_FOR_GATE,
  ADOPTING,
  ADOPTED_LOCKED,
  ATTACHING,
  ACTIVATING,
  ACTIVE,
  ACTIVE_ARTIFACTS_CLEANED,
  NOT_READY,
  CORRUPT,
  RESOURCE_EXHAUSTED,
  STALE_GENERATION,
  ABANDONED_ROLLED_BACK,
  ABANDONED_NOT_FOUND_PROVEN,
  CLEANUP_PENDING,
  CLEANUP_ROLLED_BACK,
  CLEANUP_TAINTED,
  ATTACH_TAINTED,
  ATTACH_ROLLED_BACK
};

enum class Preserve_trx_prepared_status : uint8_t {
  OK = 0,
  IDEMPOTENT,
  INVALID_ARGUMENT,
  NOT_FOUND,
  INVALID_STATE,
  ALREADY_CLAIMED,
  STALE_GENERATION,
  DIGEST_CONFLICT,
  RESOURCE_EXHAUSTED
};

enum class Preserve_trx_gate_abort_outcome : uint8_t {
  ABANDONED_ROLLED_BACK = 0,
  ABANDONED_NOT_FOUND_PROVEN,
  CLEANUP_TAINTED
};

struct Preserve_trx_prepared_token_key {
  std::string preserve_dir;
  std::string source_uuid;
  std::string epoch_id;
  std::string token;
  std::string target_boot_incarnation;
  uint64_t generation{0};
};

struct Preserve_trx_final_token_facts {
  uint64_t required_apply_lsn{0};
  uint64_t physical_fence_lsn{0};
  std::string epoch_fact_digest;
  std::string final_lock_generation_digest;
  std::string page_layout_digest;
  std::string dictionary_generation_digest;
  std::string target_boot_incarnation;
  uint64_t record_lock_unique_pages{0};
  uint64_t record_lock_bitmap_entries{0};
  uint64_t record_lock_bits{0};
  uint64_t binlog_cache_length{0};
  uint64_t binlog_cache_memory_bytes{0};
  uint64_t lock_plan_capacity_bytes{0};
  uint64_t native_binlog_capacity_bytes{0};
  uint64_t epoch_prepare_deadline_us{0};
  uint64_t client_resume_deadline_us{0};
  bool semantic_validated{false};
  bool lock_plan_ready{false};
  bool binlog_handle_ready{false};
  bool resources_reserved{false};
  bool predicate_lock_present{false};
  bool binlog_cache_present{false};
  bool binlog_cache_file_backed{false};
  std::string canonical_digest;
};

bool preserved_trx_finalize_token_facts(
    Preserve_trx_final_token_facts *facts);

class Preserve_trx_prepared_token_resources {
 public:
  Preserve_trx_prepared_token_resources();
  Preserve_trx_prepared_token_resources(
      const Preserve_trx_prepared_token_resources &) = delete;
  Preserve_trx_prepared_token_resources &operator=(
      const Preserve_trx_prepared_token_resources &) = delete;
  Preserve_trx_prepared_token_resources(
      Preserve_trx_prepared_token_resources &&other) noexcept;
  Preserve_trx_prepared_token_resources &operator=(
      Preserve_trx_prepared_token_resources &&other) noexcept;
  ~Preserve_trx_prepared_token_resources();

  bool acquired() const;
  uint64_t lock_plan_bytes() const;
  uint64_t native_binlog_bytes() const;

 private:
  class Impl;
  std::unique_ptr<Impl> m_impl;

  friend Preserve_trx_prepared_status
  preserved_trx_acquire_prepared_token_resources(
      const Preserve_trx_prepared_token_key &, uint64_t, uint64_t,
      Preserve_trx_prepared_token_resources *);
  friend class Preserve_trx_prepared_token_registry;
};

Preserve_trx_prepared_status
preserved_trx_acquire_prepared_token_resources(
    const Preserve_trx_prepared_token_key &key, uint64_t lock_plan_bytes,
    uint64_t native_binlog_bytes,
    Preserve_trx_prepared_token_resources *resources);

struct Preserve_trx_prepared_registry_state;
struct Preserve_trx_prepared_token_entry;

class Preserve_trx_prepare_lease {
 public:
  Preserve_trx_prepare_lease() = default;
  Preserve_trx_prepare_lease(const Preserve_trx_prepare_lease &) = delete;
  Preserve_trx_prepare_lease &operator=(const Preserve_trx_prepare_lease &) =
      delete;
  Preserve_trx_prepare_lease(Preserve_trx_prepare_lease &&other) noexcept;
  Preserve_trx_prepare_lease &operator=(
      Preserve_trx_prepare_lease &&other) noexcept;
  ~Preserve_trx_prepare_lease();
  bool active() const { return m_active; }

 private:
  void fail_closed();
  std::shared_ptr<Preserve_trx_prepared_registry_state> m_registry;
  std::shared_ptr<Preserve_trx_prepared_token_entry> m_entry;
  Preserve_trx_prepared_token_key m_key;
  bool m_new_entry{false};
  bool m_active{false};

  friend class Preserve_trx_prepared_token_registry;
};

class Preserve_trx_gate_adopt_lease {
 public:
  Preserve_trx_gate_adopt_lease() = default;
  Preserve_trx_gate_adopt_lease(const Preserve_trx_gate_adopt_lease &) =
      delete;
  Preserve_trx_gate_adopt_lease &operator=(
      const Preserve_trx_gate_adopt_lease &) = delete;
  Preserve_trx_gate_adopt_lease(
      Preserve_trx_gate_adopt_lease &&other) noexcept;
  Preserve_trx_gate_adopt_lease &operator=(
      Preserve_trx_gate_adopt_lease &&other) noexcept;
  ~Preserve_trx_gate_adopt_lease();
  bool active() const { return m_active; }

 private:
  void fail_closed();
  std::shared_ptr<Preserve_trx_prepared_token_entry> m_entry;
  bool m_active{false};
  friend class Preserve_trx_prepared_token_registry;
};

class Preserve_trx_attach_lease {
 public:
  Preserve_trx_attach_lease() = default;
  Preserve_trx_attach_lease(const Preserve_trx_attach_lease &) = delete;
  Preserve_trx_attach_lease &operator=(const Preserve_trx_attach_lease &) =
      delete;
  Preserve_trx_attach_lease(Preserve_trx_attach_lease &&other) noexcept;
  Preserve_trx_attach_lease &operator=(
      Preserve_trx_attach_lease &&other) noexcept;
  ~Preserve_trx_attach_lease();
  bool active() const { return m_active; }
  bool activation_started() const { return m_activation_started; }

 private:
  void fail_closed();
  std::shared_ptr<Preserve_trx_prepared_token_entry> m_entry;
  bool m_active{false};
  bool m_activation_started{false};
  friend class Preserve_trx_prepared_token_registry;
};

class Preserve_trx_cleanup_lease {
 public:
  Preserve_trx_cleanup_lease() = default;
  Preserve_trx_cleanup_lease(const Preserve_trx_cleanup_lease &) = delete;
  Preserve_trx_cleanup_lease &operator=(const Preserve_trx_cleanup_lease &) =
      delete;
  Preserve_trx_cleanup_lease(Preserve_trx_cleanup_lease &&other) noexcept;
  Preserve_trx_cleanup_lease &operator=(
      Preserve_trx_cleanup_lease &&other) noexcept;
  ~Preserve_trx_cleanup_lease();
  bool active() const { return m_active; }

 private:
  void fail_closed();
  std::shared_ptr<Preserve_trx_prepared_token_entry> m_entry;
  bool m_active{false};
  friend class Preserve_trx_prepared_token_registry;
};

struct Preserve_trx_prepared_token_snapshot {
  Preserve_trx_prepared_token_key key;
  Preserve_trx_final_token_facts facts;
  Preserve_trx_prepared_token_state state{
      Preserve_trx_prepared_token_state::NOT_FOUND};
};

class Preserve_trx_prepared_token_registry {
 public:
  Preserve_trx_prepared_token_registry();
  Preserve_trx_prepared_token_registry(
      const Preserve_trx_prepared_token_registry &) = delete;
  Preserve_trx_prepared_token_registry &operator=(
      const Preserve_trx_prepared_token_registry &) = delete;
  ~Preserve_trx_prepared_token_registry();

  Preserve_trx_prepared_status begin_prepare(
      const Preserve_trx_prepared_token_key &key,
      uint64_t expected_generation, Preserve_trx_prepare_lease *lease);
  Preserve_trx_prepared_status publish_ready(
      Preserve_trx_prepare_lease *lease, Preserve_trx_final_token_facts facts,
      Preserve_trx_prepared_token_resources resources);
  Preserve_trx_prepared_status mark_ready_for_gate(
      const Preserve_trx_prepared_token_key &key,
      uint64_t expected_generation);
  Preserve_trx_prepared_status begin_gate_adopt(
      const Preserve_trx_prepared_token_key &key,
      uint64_t expected_generation, Preserve_trx_gate_adopt_lease *lease);
  Preserve_trx_prepared_status commit_gate_adopt(
      Preserve_trx_gate_adopt_lease *lease);
  Preserve_trx_prepared_status abort_gate_adopt(
      Preserve_trx_gate_adopt_lease *lease,
      Preserve_trx_gate_abort_outcome outcome);
  Preserve_trx_prepared_status begin_attach(
      const Preserve_trx_prepared_token_key &key,
      uint64_t expected_generation, Preserve_trx_attach_lease *lease);
  Preserve_trx_prepared_status begin_activation(
      Preserve_trx_attach_lease *lease);
  Preserve_trx_prepared_status commit_attach(Preserve_trx_attach_lease *lease);
  Preserve_trx_prepared_status abort_attach_after_full_unwind(
      Preserve_trx_attach_lease *lease);
  Preserve_trx_prepared_status taint_attach(
      Preserve_trx_attach_lease *lease);
  Preserve_trx_prepared_status begin_cleanup(
      const Preserve_trx_prepared_token_key &key,
      uint64_t expected_generation,
      Preserve_trx_prepared_token_state expected_state,
      Preserve_trx_cleanup_lease *lease);
  Preserve_trx_prepared_status commit_cleanup(
      Preserve_trx_cleanup_lease *lease, bool rollback_proven);
  Preserve_trx_prepared_status snapshot(
      const Preserve_trx_prepared_token_key &key,
      Preserve_trx_prepared_token_snapshot *snapshot) const;
  void invalidate_incarnation(const std::string &current_boot_incarnation);
  size_t expire_ready_facts_pending_lease(const std::string &source_uuid,
                                          const std::string &epoch_id,
                                          uint64_t now_us);
  void purge_epoch(const std::string &source_uuid,
                   const std::string &epoch_id);

 private:
  std::shared_ptr<Preserve_trx_prepared_registry_state> m_state;
};

Preserve_trx_prepared_token_registry &
preserved_trx_strict_prepared_token_registry();

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
