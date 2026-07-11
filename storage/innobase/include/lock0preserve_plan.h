/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. */

#ifndef lock0preserve_plan_h
#define lock0preserve_plan_h

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "db0err.h"

struct dict_index_t;
struct trx_t;

enum class lock_preserve_metadata_plan_status : uint8_t {
  OK = 0,
  INVALID_ARGUMENT,
  CORRUPT_METADATA,
  UNSUPPORTED_MODE,
  DIGEST_MISMATCH,
  NOT_FINAL,
  DICT_LEASE_FAILED,
  STALE_GENERATION
};

struct lock_preserve_metadata_plan_validation_t {
  uint64_t object_generation{0};
  uint64_t expected_object_generation{0};
  uint64_t physical_fence_lsn{0};
  uint32_t artifact_protocol_version{0};
  uint32_t source_server_version{0};
  std::string object_digest;
  std::string final_lock_generation_digest;
  std::string page_layout_digest;
  std::string dictionary_generation_digest;
  bool implicit_locks_materialized{false};
  bool implicit_native_continuity_proven{false};
  bool is_final_quiesced{false};
};

struct lock_preserve_record_lock_metadata_facts_t {
  std::string final_lock_generation_digest;
  std::string page_layout_digest;
  std::string dictionary_generation_digest;
  uint64_t unique_pages{0};
  uint64_t bitmap_entries{0};
  uint64_t bitmap_bits{0};
  bool predicate_lock_present{false};
  bool wait_lock_present{false};
  bool record_image_present{false};
};

struct lock_preserve_metadata_dict_lease_ops_t {
  void *context{nullptr};
  bool (*acquire)(void *context, uint64_t table_id, uint64_t index_id,
                  uint32_t space_id,
                  const std::string &dictionary_generation_digest,
                  void **opaque_lease, dict_index_t **index){nullptr};
  bool (*revalidate)(void *opaque_lease,
                     const std::string &dictionary_generation_digest,
                     dict_index_t **index){nullptr};
  void (*release)(void *opaque_lease){nullptr};
};

enum class lock_preserve_metadata_conflict_result : uint8_t {
  OK = 0,
  CONFLICT,
  UNSUPPORTED_MODE,
  CORRUPT_METADATA,
  DICT_LEASE_INVALID,
  DEADLINE_EXCEEDED
};

class lock_preserve_metadata_plan_t {
 public:
  lock_preserve_metadata_plan_t();
  lock_preserve_metadata_plan_t(const lock_preserve_metadata_plan_t &) =
      delete;
  lock_preserve_metadata_plan_t &operator=(
      const lock_preserve_metadata_plan_t &) = delete;
  lock_preserve_metadata_plan_t(
      lock_preserve_metadata_plan_t &&other) noexcept;
  lock_preserve_metadata_plan_t &operator=(
      lock_preserve_metadata_plan_t &&other) noexcept;
  ~lock_preserve_metadata_plan_t();

  bool ready() const;
  uint64_t entry_count() const;
  uint64_t bitmap_bits() const;
  uint64_t capacity_bytes() const;

 private:
  class Impl;
  std::unique_ptr<Impl> m_impl;

  friend lock_preserve_metadata_plan_status
  lock_preserve_build_record_lock_metadata_plan(
      const std::string &, const lock_preserve_metadata_plan_validation_t &,
      const lock_preserve_metadata_dict_lease_ops_t &,
      lock_preserve_metadata_plan_t *);
  friend lock_preserve_metadata_conflict_result
  lock_preserve_check_record_bitmap_conflicts_from_metadata(
      trx_t *, const lock_preserve_metadata_plan_t &, uint64_t);
  friend dberr_t lock_preserve_apply_record_lock_metadata_plan(
      trx_t *, const lock_preserve_metadata_plan_t &, uint64_t,
      class lock_preserve_import_journal_t *);
};

class lock_preserve_import_journal_t {
 public:
  lock_preserve_import_journal_t();
  lock_preserve_import_journal_t(const lock_preserve_import_journal_t &) =
      delete;
  lock_preserve_import_journal_t &operator=(
      const lock_preserve_import_journal_t &) = delete;
  lock_preserve_import_journal_t(
      lock_preserve_import_journal_t &&other) noexcept;
  lock_preserve_import_journal_t &operator=(
      lock_preserve_import_journal_t &&other) noexcept;
  ~lock_preserve_import_journal_t();
  size_t size() const;

 private:
  class Impl;
  std::unique_ptr<Impl> m_impl;
  friend dberr_t lock_preserve_apply_record_lock_metadata_plan(
      trx_t *, const lock_preserve_metadata_plan_t &, uint64_t,
      lock_preserve_import_journal_t *);
  friend dberr_t lock_preserve_unwind_record_lock_metadata_import(
      trx_t *, lock_preserve_import_journal_t *);
};

/** Derive canonical physical metadata facts without reading an InnoDB page. */
lock_preserve_metadata_plan_status
lock_preserve_build_record_lock_metadata_facts(
    const std::string &payload,
    lock_preserve_record_lock_metadata_facts_t *facts);

/** Build an immutable page-free plan with caller-provided dictionary leases. */
lock_preserve_metadata_plan_status
lock_preserve_build_record_lock_metadata_plan(
    const std::string &payload,
    const lock_preserve_metadata_plan_validation_t &validation,
    const lock_preserve_metadata_dict_lease_ops_t &dict_lease_ops,
    lock_preserve_metadata_plan_t *plan);

/** Build an immutable page-free plan with production dictionary leases. */
lock_preserve_metadata_plan_status
lock_preserve_build_record_lock_metadata_plan_with_default_dict_lease(
    const std::string &payload,
    const lock_preserve_metadata_plan_validation_t &validation,
    lock_preserve_metadata_plan_t *plan);

lock_preserve_metadata_conflict_result
lock_preserve_check_record_bitmap_conflicts_from_metadata(
    trx_t *trx, const lock_preserve_metadata_plan_t &plan,
    uint64_t operation_deadline_us);

dberr_t lock_preserve_apply_record_lock_metadata_plan(
    trx_t *trx, const lock_preserve_metadata_plan_t &plan,
    uint64_t operation_deadline_us, lock_preserve_import_journal_t *journal);

dberr_t lock_preserve_unwind_record_lock_metadata_import(
    trx_t *trx, lock_preserve_import_journal_t *journal);

#endif  // lock0preserve_plan_h
