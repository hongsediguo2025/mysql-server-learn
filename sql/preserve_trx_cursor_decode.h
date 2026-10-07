/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_CURSOR_DECODE_INCLUDED
#define SQL_PRESERVE_TRX_CURSOR_DECODE_INCLUDED

#include "sql/preserve_trx_cursor_file.h"
#include "field_types.h"
struct CHARSET_INFO;

/** Private schema and native Fields; each executor supplies its work buffers. No handler,
source TABLE or source SQL is needed. The caller must not use items after a
failed read; BLOB pointers remain valid until the next read or release_work(). */
class Preserve_trx_cursor_decoder {
 public:
  ~Preserve_trx_cursor_decoder();
  static Preserve_trx_file_status create(
      const std::string &token, THD *thd,
      std::unique_ptr<Preserve_trx_cursor_file> file,
      std::unique_ptr<Preserve_trx_cursor_decoder> *output);
  /** Invalidate old row/cache references; locating retains no workspace. */
  Preserve_trx_file_status seek(uint64_t row, Preserve_trx_cursor_workspace &);
  Preserve_trx_file_status read_next(Preserve_trx_cursor_workspace &);
  /** Before receiver READY, check the same cell bounds as FETCH without
  publishing Items or changing client progress. Call before seek/read_next.
  Each batch is bounded by rows and bytes; one oversized row is indivisible.
  Every exit releases work references; the caller unbinds the workspace. */
  Preserve_trx_file_status preflight_next(uint64_t row_budget,
                                         uint64_t byte_budget,
                                         Preserve_trx_cursor_workspace &,
                                         uint64_t *scanned_bytes = nullptr);
  void release_work() noexcept;
  bool values_validated() const;
  const mem_root_deque<Item *> &items() const;
  const enum_field_types *types() const;
  const CHARSET_INFO *result_charset() const;
  void bind(THD *thd);
  const Preserve_trx_cursor_descriptor &descriptor() const;
  bool owns_file(const std::shared_ptr<const Preserve_trx_sealed_file> &) const;

 private:
  struct Impl;
  explicit Preserve_trx_cursor_decoder(std::unique_ptr<Impl> impl);
  Preserve_trx_file_status read_row(bool publish_items, Preserve_trx_cursor_workspace &);
  std::unique_ptr<Impl> m_impl;
};

#ifndef NDEBUG
bool preserve_trx_cursor_decode_for_test(
    THD *thd, const Preserve_trx_cursor_result &artifact, uint64_t row,
    Preserve_trx_cursor_workspace &,
    std::unique_ptr<Preserve_trx_cursor_decoder> *decoder);
int show_preserve_trx_cursor_decoded_rows(THD *, SHOW_VAR *, char *);
int show_preserve_trx_cursor_preflight_rows(THD *, SHOW_VAR *, char *);
#endif
#endif
