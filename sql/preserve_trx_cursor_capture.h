/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_CURSOR_CAPTURE_INCLUDED
#define SQL_PRESERVE_TRX_CURSOR_CAPTURE_INCLUDED

#include <memory>
#include "my_thread.h"
#include "sql/preserve_trx_cursor.h"

struct Preserve_trx_cursor_read_wait;
struct Preserve_trx_phase1_record_adapter_control;
enum class Preserve_trx_cursor_read_stage : unsigned char {
  UNAVAILABLE, AVAILABLE, BORROWED, NETWORK_WAITING
};

void preserve_trx_cursor_begin_read(THD *thd);
void preserve_trx_cursor_end_read(THD *thd);

/** A pinned Phase1 target is borrowed only inside the existing worker permit.
The calling owner retains the notification record between worker steps. */
class Preserve_trx_cursor_borrow {
 public:
  Preserve_trx_cursor_borrow(THD *worker, THD *target,
      std::shared_ptr<Preserve_trx_cursor_read_wait> *record,
      uint64_t incarnation, const Preserve_trx_phase1_record_adapter_control &);
  ~Preserve_trx_cursor_borrow();
  Preserve_trx_cursor_borrow(const Preserve_trx_cursor_borrow &) = delete;
  Preserve_trx_cursor_borrow &operator=(const Preserve_trx_cursor_borrow &) = delete;
  bool active() const { return m_active; }
 private:
  THD *m_worker, *m_target;
  my_thread_t m_real_id;
  const char *m_stack;
  std::shared_ptr<Preserve_trx_cursor_read_wait> m_record;
  bool m_active{false};
};

/** With row_limit zero, only create the live artifact identity/metadata. */
Preserve_trx_cursor_capture_status preserve_trx_cursor_capture_step(
    THD *thd, Server_side_cursor *cursor, uint64_t row_limit, uint64_t byte_limit);
/** Final's exclusive owner calls this before any transaction freeze/detach. */
bool preserve_trx_cursor_capture_final(THD *thd);
#endif
