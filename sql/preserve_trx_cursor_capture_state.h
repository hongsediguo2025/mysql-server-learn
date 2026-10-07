/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_CURSOR_CAPTURE_STATE_INCLUDED
#define SQL_PRESERVE_TRX_CURSOR_CAPTURE_STATE_INCLUDED

#include <memory>
#include <vector>
#include "sql/preserve_trx_resource.h"
struct evp_md_ctx_st;

/** Mutable, exclusively owned capture state. Never retained after seal. */
struct Preserve_trx_cursor_capture_state {
  ~Preserve_trx_cursor_capture_state();
  Preserve_memory_lease memory;
  std::unique_ptr<unsigned char[]> data, index, storage;
  std::vector<unsigned char> bookmarks;
  unsigned char *buffer{nullptr}, *index_buffer{nullptr};
  evp_md_ctx_st *hash{nullptr};
  int file{-1}, index_file{-1};
  uint64_t written{0}, index_copied{0};
  size_t pending{0}, hashed_pending{0}, index_pending{0}, capacity{0};
  bool index_started{false}, memory_only{false}, borrowed{false}, final_capture{false};
};
#endif
