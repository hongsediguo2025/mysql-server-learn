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

struct lock_preserve_metadata_dict_lease_ops_t;
struct lock_preserve_metadata_plan_validation_t;
struct trx_t;
enum class lock_preserve_metadata_plan_status : uint8_t;

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
