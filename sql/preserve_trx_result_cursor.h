/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_RESULT_CURSOR_INCLUDED
#define SQL_PRESERVE_TRX_RESULT_CURSOR_INCLUDED
#include "sql/preserve_trx_cursor_decode.h"
#include "sql/sql_cursor.h"

/** Independent result-file reader; never executes or reparses the source SQL. */
class Preserve_trx_result_cursor final : public Server_side_cursor {
  friend class Preserve_trx_result_restore;
 public:
  static Preserve_trx_file_status create(
      const std::string &token, THD *thd, Preserve_trx_cursor_snapshot state,
      std::unique_ptr<Preserve_trx_cursor_decoder> decoder,
      std::unique_ptr<Preserve_trx_result_cursor> *output);
  ~Preserve_trx_result_cursor() override;
  static void operator delete(void *p, size_t) { ::operator delete(p); }
  bool is_open() const override { return m_state.open; }
  bool open(THD *thd) override;
  bool fetch(ulong count) override;
  void close() override;
  bool preserve_snapshot(Preserve_trx_cursor_snapshot *output) const override;

 private:
  /** Rebind only connection references; no file access or metadata rebuild. */
  bool bind(THD *thd);
  Preserve_trx_result_cursor(THD *thd, Preserve_trx_cursor_snapshot state,
                            std::unique_ptr<Preserve_trx_cursor_decoder> decoder,
                            Preserve_memory_lease memory);
  THD *m_thd;
  Preserve_trx_cursor_snapshot m_state;
  std::unique_ptr<Preserve_trx_cursor_decoder> m_decoder;
  Preserve_memory_lease m_memory;
  bool m_position_valid{true};
  bool m_first_fetch{true};
};

int show_preserve_trx_cursor_restored_cursors(THD *, SHOW_VAR *, char *);
#ifndef NDEBUG
class Prepared_statement;
bool preserve_trx_restore_cursor_for_test(Prepared_statement *stmt);
#endif
#endif
