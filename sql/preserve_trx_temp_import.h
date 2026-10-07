/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_TEMP_IMPORT_INCLUDED
#define SQL_PRESERVE_TRX_TEMP_IMPORT_INCLUDED

#include <cstddef>
#include <cstdint>
#include <memory>
#include "storage/innobase/include/db0err.h"

class Preserve_trx_temp_transfer_input;
class trx_preserve_temp_import_plan;

/** Receiver source preparation: immutable SEALED input -> metadata -> private
dictionary -> decoded undo -> validated graph. Owns all cross-batch state;
retains no THD, shared lock or prepare lease. The scheduler must recheck the
input against the current record/generation before every batch and publication.
Completion permits target image preparation; it is not adoption or READY. */
class Preserve_trx_temp_import_work {
 public:
  static dberr_t begin(
      std::unique_ptr<Preserve_trx_temp_transfer_input> *input,
      std::unique_ptr<Preserve_trx_temp_import_work> *output);
  ~Preserve_trx_temp_import_work();
  /** Each call advances one phase. work_budget bounds metadata/DD/graph
  units (including metadata page reads); byte_budget additionally bounds graph
  decoding; page_budget bounds undo reader frames/chain checks. */
  dberr_t step(size_t work_budget, size_t byte_budget, size_t page_budget,
              const trx_preserve_temp_import_plan *previous = nullptr);
  bool complete() const;
  const Preserve_trx_temp_transfer_input *input() const;
  /** Read bytes recorded in the most recent batch; validation-only batches
  return zero. Failed setup/reads may have consumed additional uncounted IO. */
  uint64_t scanned_bytes() const;
  /** Derived-file fallback writes in the most recent batch. */
  uint64_t written_bytes() const;
#ifndef NDEBUG
  uint64_t batches() const;
#endif
  /** Move both owners only after completion. Outputs must be empty. The input
  still owns metadata/pins needed for generation checks and SQL TABLE adoption. */
  dberr_t take(std::unique_ptr<Preserve_trx_temp_transfer_input> *input,
               std::unique_ptr<trx_preserve_temp_import_plan> *plan);
  bool cancel_step(size_t work_budget);

 private:
  struct Impl;
  explicit Preserve_trx_temp_import_work(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> m_impl;
#ifndef NDEBUG
  friend dberr_t preserve_trx_temp_import_probe(
      std::unique_ptr<Preserve_trx_temp_transfer_input> *,
      std::unique_ptr<trx_preserve_temp_import_plan> *);
#endif
};

#ifndef NDEBUG
dberr_t preserve_trx_temp_import_probe(
    std::unique_ptr<Preserve_trx_temp_transfer_input> *input,
    std::unique_ptr<trx_preserve_temp_import_plan> *plan);
#endif
#endif
