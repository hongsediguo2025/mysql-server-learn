/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify it under
   the terms of the GNU General Public License, version 2.0, as published by the
   Free Software Foundation. */

#ifndef trx0temp_preserve_lob_h
#define trx0temp_preserve_lob_h

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include "db0err.h"

struct trx_preserve_temp_external_reference;
struct trx_preserve_temp_lob_diff;

/** Private receiver proof for uncompressed temporary LOBs. Collects metadata
from the existing source root reads and image conversion, never payload or live
buffer-pool objects. Validation and retirement are bounded worker operations;
no graph survives into native adoption or SQL RESUME. */
class trx_preserve_temp_lob {
 public:
  static dberr_t create(const std::string &token,
                       std::unique_ptr<trx_preserve_temp_lob> *output);
  ~trx_preserve_temp_lob();
  dberr_t add_table(uint64_t table, uint32_t space, uint32_t pages,
                    const unsigned char *root, size_t bytes);
  /** Register other native segments to exclude secondary/TOP or another
  table's allocations from a LOB leaf segment's provenance. */
  dberr_t add_other_segment(uint32_t space, uint32_t pages,
                            const unsigned char *header, size_t bytes);
  dberr_t collect_page(uint32_t space, uint32_t page_no,
                       const unsigned char *page, size_t bytes,
                       uint32_t allocation_state, uint64_t segment_id);
  /** Diff metadata borrows the immutable source undo graph, which must outlive
  this proof, including its bounded cancellation. */
  dberr_t add_reference(uint64_t table,
                        const trx_preserve_temp_external_reference &ref,
                        const trx_preserve_temp_lob_diff *diffs = nullptr,
                        size_t diff_count = 0);
  /** Call only after every source image and source undo reference is collected.
  One bounded page's metadata may exceed byte_budget, but no chain is atomic. */
  dberr_t step(size_t work_budget, size_t byte_budget, bool *complete);
  bool cancel_step(size_t work_budget);

 private:
  trx_preserve_temp_lob();
  struct Impl;
  std::unique_ptr<Impl> m_impl;
};
#endif
