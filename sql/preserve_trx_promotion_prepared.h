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
#include <vector>

#include "storage/innobase/include/lock0preserve_plan.h"

class Mysql_binlog_preserve_payload_reader;
class Mysql_binlog_preserve_prepared_cache_handle;
class Preserve_memory_lease;
struct Preserved_trx_bundle;
class Preserve_trx_internal_operation_capability;
struct Preserve_trx_prepared_token_key;
struct Preserve_trx_resurrection_index_entry;
struct Preserve_trx_targeted_publication_revocation_state;
struct trx_preserve_targeted_publication_journal;
struct Mysql_binlog_preserve_cache_facts;
enum class Mysql_binlog_preserve_cache_status : uint8_t;

enum class Preserve_trx_physical_consistency_mode : uint8_t {
  NONE = 0,
  TEST_SAME_INSTANCE_ATTACH_ONLY,
  TEST_ONLY_PHYSICAL_FENCE_SIMULATOR,
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
  bool implicit_native_continuity_proven{false};
};

struct Preserve_trx_epoch_physical_digest_input {
  std::string token;
  uint64_t generation{0};
  std::string final_lock_generation_digest;
  std::string page_layout_digest;
  std::string dictionary_generation_digest;
};

bool preserved_trx_compute_epoch_physical_digest_commitments(
    const std::vector<Preserve_trx_epoch_physical_digest_input> &inputs,
    std::string *final_lock_generation_digest,
    std::string *page_layout_digest,
    std::string *dictionary_generation_digest);

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

/*
  Capability for the no-physical-replication simulator only. It is bound to
  one registry key and is revoked with the test fence lease that minted it.
*/
class Preserve_trx_targeted_publication_capability {
 public:
  Preserve_trx_targeted_publication_capability() = default;
  bool valid_for(const Preserve_trx_prepared_token_key &key) const;
  uint64_t source_fence_lsn() const { return m_source_fence_lsn; }
  uint64_t provider_generation() const { return m_provider_generation; }

 private:
  std::weak_ptr<Preserve_trx_targeted_publication_revocation_state> m_state;
  std::string m_source_uuid;
  std::string m_epoch_id;
  std::string m_token;
  std::string m_target_boot_incarnation;
  uint64_t m_generation{0};
  uint64_t m_source_fence_lsn{0};
  uint64_t m_provider_generation{0};

  friend class Preserve_trx_physical_fence_lease;
};

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
  bool make_targeted_publication_capability(
      const Preserve_trx_prepared_token_key &key,
      Preserve_trx_targeted_publication_capability *capability) const;
  void release();

 private:
  Preserve_trx_physical_fence_provider_ops m_ops;
  Preserve_trx_physical_fence_proof m_expected;
  Preserve_trx_physical_fence_proof m_proof;
  void *m_opaque_lease{nullptr};
  std::shared_ptr<Preserve_trx_targeted_publication_revocation_state>
      m_targeted_publication_state;

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

enum class Preserve_trx_strict_promotion_intent_state : uint8_t {
  ADOPTING = 0,
  ADOPTED_LOCKED,
  ABANDONED_ROLLED_BACK,
  ABANDONED_NOT_FOUND_PROVEN,
  CLEANUP_TAINTED
};

struct Preserve_trx_strict_promotion_intent_token {
  std::string token;
  uint64_t generation{0};
  Preserve_trx_strict_promotion_intent_state state{
      Preserve_trx_strict_promotion_intent_state::ADOPTING};
};

struct Preserve_trx_strict_promotion_intent_epoch {
  std::string epoch_id;
  Preserve_trx_physical_fence_proof physical_fence;
  uint64_t generated_at_us{0};
  std::vector<Preserve_trx_strict_promotion_intent_token> tokens;
};

bool preserved_trx_encode_strict_promotion_intent_v2(
    const Preserve_trx_strict_promotion_intent_epoch &marker,
    std::string *encoded);
bool preserved_trx_decode_strict_promotion_intent_v2(
    const std::string &encoded,
    Preserve_trx_strict_promotion_intent_epoch *marker);

enum class Preserve_trx_strict_attach_intent_state : uint8_t {
  ATTACHING = 0,
  ACTIVATING,
  ACTIVE,
  ATTACH_ROLLED_BACK,
  ATTACH_TAINTED,
  CLEANUP_PENDING,
  CLEANUP_ROLLED_BACK,
  CLEANUP_TAINTED
};

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
  RESOURCE_EXHAUSTED,
  INTENT_IO_ERROR
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

struct Preserve_trx_strict_attach_intent {
  Preserve_trx_prepared_token_key key;
  Preserve_trx_strict_attach_intent_state state{
      Preserve_trx_strict_attach_intent_state::ATTACHING};
  uint64_t target_connection_id{0};
  uint64_t generated_at_us{0};
};

bool preserved_trx_encode_strict_attach_intent_v1(
    const Preserve_trx_strict_attach_intent &intent, std::string *encoded);
bool preserved_trx_decode_strict_attach_intent_v1(
    const std::string &encoded, Preserve_trx_strict_attach_intent *intent);
std::string preserved_trx_strict_attach_intent_journal_id(
    const Preserve_trx_prepared_token_key &key);

struct Preserve_trx_final_token_facts {
  uint64_t required_apply_lsn{0};
  uint64_t physical_fence_lsn{0};
  uint64_t source_trx_id_store{0};
  uint64_t source_trx_id_store_lsn{0};
  uint64_t source_safe_next_trx_id_floor{0};
  std::string epoch_fact_digest;
  std::string final_lock_generation_digest;
  std::string page_layout_digest;
  std::string dictionary_generation_digest;
  std::string prewarm_object_set_digest;
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
  std::string binlog_handle_digest;
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
  bool has_record_lock_plan() const;
  bool has_semantic_bundle() const;
  bool has_native_binlog_handle() const;
  bool has_resurrection_entry() const;
  void reset() noexcept;
  Preserve_trx_prepared_status install_record_lock_plan(
      std::unique_ptr<lock_preserve_metadata_plan_t> plan);
  Preserve_trx_prepared_status install_record_lock_plan_with_memory_lease(
      std::unique_ptr<lock_preserve_metadata_plan_t> plan,
      Preserve_memory_lease &&memory_lease);
  Preserve_trx_prepared_status install_semantic_bundle(
      std::unique_ptr<Preserved_trx_bundle> bundle);
  Preserve_trx_prepared_status install_resurrection_entry(
      std::unique_ptr<Preserve_trx_resurrection_index_entry> entry);
  Mysql_binlog_preserve_cache_status prepare_native_binlog_handle(
      const Preserve_trx_internal_operation_capability &capability,
      const Mysql_binlog_preserve_cache_facts &facts,
      Mysql_binlog_preserve_payload_reader *reader);
  Mysql_binlog_preserve_cache_status
  prepare_native_binlog_handle_for_receiver(
      const Mysql_binlog_preserve_cache_facts &facts,
      Mysql_binlog_preserve_payload_reader *reader);

 private:
  class Impl;
  std::unique_ptr<Impl> m_impl;

  friend Preserve_trx_prepared_status
  preserved_trx_acquire_prepared_token_resources(
      const Preserve_trx_prepared_token_key &, uint64_t, uint64_t, uint64_t,
      uint64_t, Preserve_trx_prepared_token_resources *);
  friend class Preserve_trx_prepared_token_registry;
  friend class Preserve_trx_gate_adopt_lease;
  friend class Preserve_trx_attach_lease;
  friend class Preserve_trx_cleanup_lease;
};

Preserve_trx_prepared_status
preserved_trx_acquire_prepared_token_resources(
    const Preserve_trx_prepared_token_key &key, uint64_t lock_plan_bytes,
    uint64_t native_binlog_bytes,
    Preserve_trx_prepared_token_resources *resources);
Preserve_trx_prepared_status
preserved_trx_acquire_prepared_token_resources(
    const Preserve_trx_prepared_token_key &key, uint64_t lock_plan_bytes,
    uint64_t native_binlog_bytes, uint64_t native_binlog_fd_count,
    uint64_t native_binlog_tmpdir_bytes,
    Preserve_trx_prepared_token_resources *resources);

struct Preserve_trx_prepared_registry_state;
struct Preserve_trx_prepared_token_entry;
using Preserve_trx_activation_intent_writer = bool (*)(
    const Preserve_trx_prepared_token_key &, void *context);

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
  const lock_preserve_metadata_plan_t *record_lock_plan() const;
  const Preserve_trx_resurrection_index_entry *resurrection_entry() const;
  Preserve_trx_prepared_status copy_publication(
      Preserve_trx_prepared_token_key *key,
      Preserve_trx_final_token_facts *facts) const;
  Preserve_trx_prepared_status take_semantic_bundle(
      std::unique_ptr<Preserved_trx_bundle> *out);
  Preserve_trx_prepared_status restore_semantic_bundle(
      std::unique_ptr<Preserved_trx_bundle> *inout);
  Preserve_trx_prepared_status install_targeted_publication_journal(
      std::unique_ptr<trx_preserve_targeted_publication_journal> &&journal);

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
  Preserve_trx_prepared_status take_native_binlog_handle(
      std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle> *out);
  Preserve_trx_prepared_status make_native_binlog_attach_capability(
      Preserve_trx_internal_operation_capability *out) const;
  Preserve_trx_prepared_status restore_native_binlog_handle(
      std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle> *inout);

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
  trx_preserve_targeted_publication_journal *targeted_publication_journal()
      const;
  Preserve_trx_prepared_status take_targeted_publication_journal(
      std::unique_ptr<trx_preserve_targeted_publication_journal> *out);

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
  bool record_lock_plan_owned{false};
  bool semantic_bundle_owned{false};
  bool native_binlog_handle_owned{false};
  bool resurrection_entry_owned{false};
  bool targeted_publication_journal_owned{false};
};

struct Preserve_trx_prepared_expire_result {
  size_t ready_expired{0};
  size_t adopted_tainted{0};
  size_t active_artifacts_cleaned{0};
};

struct Preserve_trx_prepared_registry_counts {
  uint64_t registered_tokens{0};
  uint64_t prewarm_pending_tokens{0};
  uint64_t ready_tokens{0};
  uint64_t adopting_tokens{0};
  uint64_t adopted_tokens{0};
  uint64_t tainted_tokens{0};
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
  Preserve_trx_prepared_status publish_prewarmed(
      Preserve_trx_prepare_lease *lease,
      const std::string &prewarm_object_set_digest,
      Preserve_trx_prepared_token_resources resources);
  Preserve_trx_prepared_status bind_final_facts(
      const Preserve_trx_prepared_token_key &key,
      uint64_t expected_generation, Preserve_trx_final_token_facts facts);
  Preserve_trx_prepared_status update_epoch_prepare_deadline(
      const std::string &source_uuid, const std::string &epoch_id,
      size_t expected_token_count, uint64_t deadline_monotonic_us);
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
      Preserve_trx_attach_lease *lease,
      Preserve_trx_activation_intent_writer intent_writer,
      void *intent_context);
  Preserve_trx_prepared_status commit_attach(
      Preserve_trx_attach_lease *lease,
      Preserve_trx_activation_intent_writer intent_writer,
      void *intent_context);
  Preserve_trx_prepared_status rollback_attach_after_activation(
      Preserve_trx_attach_lease *lease,
      Preserve_trx_activation_intent_writer intent_writer,
      void *intent_context);
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
  Preserve_trx_prepared_status find_unique_adopted(
      const std::string &epoch_id, const std::string &token,
      Preserve_trx_prepared_token_snapshot *snapshot) const;
  Preserve_trx_prepared_registry_counts status_counts() const;
  void invalidate_incarnation(const std::string &current_boot_incarnation);
  size_t expire_ready_facts_pending_lease(const std::string &source_uuid,
                                          const std::string &epoch_id,
                                          uint64_t now_us);
  Preserve_trx_prepared_expire_result expire_once(uint64_t now_us);
  Preserve_trx_prepared_status purge_token(
      const Preserve_trx_prepared_token_key &key);
  void purge_epoch(const std::string &source_uuid,
                   const std::string &epoch_id);
  /* Process shutdown only: discard receiver-owned prepared resources. */
  size_t discard_all_for_process_shutdown();

 private:
  std::shared_ptr<Preserve_trx_prepared_registry_state> m_state;
};

Preserve_trx_prepared_token_registry &
preserved_trx_strict_prepared_token_registry();

void preserved_trx_promotion_prepared_metrics_reset_for_unit_test();
uint64_t preserved_trx_promotion_prepared_monotonic_us_for_unit_test();
void preserved_trx_promotion_resume_core_note(uint64_t elapsed_us,
                                               bool success);
void preserved_trx_promotion_prepared_note_evidence(
    Preserve_trx_physical_consistency_mode mode, bool real_redo_apply);
void preserved_trx_promotion_prepared_note_fence_metrics(
    uint64_t lease_wait_us, uint64_t digest_compare_us,
    uint64_t revalidate_us);
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
uint64_t preserve_trx_promotion_fence_lease_wait_us_status();
uint64_t preserve_trx_promotion_fence_digest_compare_us_status();
uint64_t preserve_trx_promotion_fence_revalidate_us_status();
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
uint64_t preserve_trx_resume_binlog_attach_count_status();
uint64_t preserve_trx_promotion_prepared_registered_tokens_status();
uint64_t preserve_trx_promotion_prepared_prewarm_pending_tokens_status();
uint64_t preserve_trx_promotion_prepared_ready_tokens_status();
uint64_t preserve_trx_promotion_prepared_adopting_tokens_status();
uint64_t preserve_trx_promotion_prepared_adopted_tokens_status();
uint64_t preserve_trx_promotion_prepared_tainted_tokens_status();

#ifndef NDEBUG
enum class Preserve_trx_prepared_registry_probe_point : uint8_t {
  BEGIN_PREPARE_AFTER_LOOKUP = 0,
  INVALIDATE_BEFORE_RETIRE
};
using Preserve_trx_prepared_registry_probe = void (*)(
    Preserve_trx_prepared_registry_probe_point, void *);
void preserved_trx_prepared_registry_set_probe_for_unit_test(
    Preserve_trx_prepared_registry_probe probe, void *context);
#endif

#endif  // SQL_PRESERVE_TRX_PROMOTION_PREPARED_INCLUDED
