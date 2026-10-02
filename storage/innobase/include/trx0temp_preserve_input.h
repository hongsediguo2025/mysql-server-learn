/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef trx0temp_preserve_input_h
#define trx0temp_preserve_input_h

#include <memory>
#include <string>
#include "trx0temp_preserve.h"

class Preserve_memory_lease;
class Preserve_trx_sealed_file;

/** Pure validation using the native sidecar predicates. No capture registry,
live rseg, buffer pool or dictionary access. */
bool trx_preserve_temp_undo_input_identity_valid(
    const trx_preserve_temp_space_image_descriptor &source,
    const trx_preserve_temp_no_redo_undo_log_anchor &insert,
    const trx_preserve_temp_no_redo_undo_log_anchor &update);
bool trx_preserve_temp_undo_input_page_valid(
    const trx_preserve_temp_space_image_descriptor &source,
    const trx_preserve_temp_no_redo_undo_page_image &image);

/** Bounded reader of one immutable receiver undo file. Admission reserves all
page storage before consuming source. step() processes at most the supplied
number of pages/chain links/index retirements. The owner must drive cancel_step
before destruction on its worker; the destructor only provides a safety fallback.
No live native resource is installed, and completion is not token READY. */
class trx_preserve_temp_undo_input {
 public:
  /** Decode an independent sidecar before final metadata exists. The canonical
  object name must use the header's rseg space. Final still validates ownership. */
  static dberr_t begin_independent(
      const std::string &token, const std::string &object_id,
      std::shared_ptr<const Preserve_trx_sealed_file> file,
      std::unique_ptr<trx_preserve_temp_undo_input> *output);
  static dberr_t begin(
      const std::string &token,
      std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
      std::shared_ptr<const Preserve_trx_sealed_file> file,
      std::unique_ptr<trx_preserve_temp_undo_input> *output);
  ~trx_preserve_temp_undo_input();
  dberr_t step(size_t page_budget, bool *complete);
  bool cancel_step(size_t page_budget);
#ifndef NDEBUG
  size_t loaded_pages() const;
#endif
  uint64_t read_bytes() const;
  const trx_preserve_temp_space_image_descriptor *source() const;
  /** Atomic ownership move after completion. Both outputs must be empty.
  Memory remains charged until the consumer destroys the decoded pages. */
  dberr_t take(
      std::unique_ptr<trx_preserve_temp_space_image_descriptor> *source,
      Preserve_memory_lease *memory);

 private:
  struct Impl;
  explicit trx_preserve_temp_undo_input(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> m_impl;
#ifndef NDEBUG
  friend dberr_t trx_preserve_temp_undo_input_probe(
      const std::string &, const trx_preserve_temp_space_image_descriptor &,
      std::shared_ptr<const Preserve_trx_sealed_file>);
#endif
};

#ifndef NDEBUG
dberr_t trx_preserve_temp_undo_input_probe(
    const std::string &token,
    const trx_preserve_temp_space_image_descriptor &source,
    std::shared_ptr<const Preserve_trx_sealed_file> file);
#endif
#endif
