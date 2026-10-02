/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_NO_REDO_CONTEXT_H
#define SQL_PRESERVE_TRX_NO_REDO_CONTEXT_H

#include <cstdint>
#include <memory>
#include "sql/xa.h"

struct trx_t;

/** Temporary adoption owner. Success hands the trx to the normal record;
exceptions roll it back. Unprovable cleanup keeps the native owner until exit. */
class Preserve_trx_no_redo_context {
 public:
  static std::unique_ptr<Preserve_trx_no_redo_context> create(
      const XID &, uint64_t owner, uint64_t freeze_lsn, uint64_t safe_next_floor);
  ~Preserve_trx_no_redo_context();
  trx_t *get() const;
  void release();
 private:
  struct State;
  std::unique_ptr<State> m_state;
};

#endif
