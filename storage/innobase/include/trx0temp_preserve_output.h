/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef trx0temp_preserve_output_h
#define trx0temp_preserve_output_h
#include <memory>
#include "trx0temp_preserve.h"

/** Sequential encoder of the existing PTRUNDO1 format. The sealed descriptor
is borrowed and must stay immutable/alive until completion or destruction.
Each step emits/hashes at most byte_budget bytes, without a second page copy. */
class trx_preserve_temp_undo_output {
 public:
  using Write = dberr_t (*)(void *, uint64_t, const unsigned char *, size_t);
  trx_preserve_temp_undo_output();
  ~trx_preserve_temp_undo_output();
  dberr_t start(const trx_preserve_temp_space_image_descriptor &);
  dberr_t step(size_t byte_budget, void *, Write, bool *complete);
  uint64_t bytes_written() const;
  uint64_t size() const;
 private:
  struct Impl;
  std::unique_ptr<Impl> m_impl;
};
#endif
