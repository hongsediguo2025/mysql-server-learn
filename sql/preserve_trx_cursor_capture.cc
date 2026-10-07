/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_cursor_capture.h"
#include "sql/preserve_trx_cursor_stream.h"

#include <chrono>
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_mutex.h"
#include "scope_guard.h"
#include "sql/current_thd.h"
#include "sql/error_handler.h"
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_phase1_record_adapter.h"
#include "sql/sql_class.h"
#include "sql/sql_cursor.h"
#include "sql/sql_error.h"
#include "sql/sql_prepare.h"

struct Preserve_trx_cursor_read_wait {
  Preserve_memory_lease memory;
  mysql_cond_t condition;
  bool completed{true};  // LOCK_thd_data, never THD::enter_cond.
  Preserve_trx_cursor_read_wait() { mysql_cond_init(0, &condition); }
  ~Preserve_trx_cursor_read_wait() { mysql_cond_destroy(&condition); }
};

void preserve_trx_cursor_begin_read(THD *thd) {
  if (preserve_trx_cursor_capture_enabled(thd))
    thd->preserve_trx_cursor_read_stage.store(
        Preserve_trx_cursor_read_stage::AVAILABLE, std::memory_order_release);
}

void preserve_trx_cursor_end_read(THD *thd) {
  if (!thd) return;
  using Stage = Preserve_trx_cursor_read_stage;
  auto stage = thd->preserve_trx_cursor_read_stage.load(std::memory_order_acquire);
  for (;;) {
    if (stage == Stage::UNAVAILABLE) return;
    const auto next = stage == Stage::AVAILABLE ? Stage::UNAVAILABLE
                                              : Stage::NETWORK_WAITING;
    if (thd->preserve_trx_cursor_read_stage.compare_exchange_weak(
            stage, next, std::memory_order_acq_rel)) {
      if (next == Stage::UNAVAILABLE) return;
      break;
    }
  }
  mysql_mutex_lock(&thd->LOCK_thd_data);
  auto record = thd->preserve_trx_cursor_read_wait;
  // The borrower may already have returned before we acquired the mutex.
  while (record && !record->completed)
    mysql_cond_wait(&record->condition, &thd->LOCK_thd_data);
  mysql_mutex_unlock(&thd->LOCK_thd_data);
}

Preserve_trx_cursor_borrow::Preserve_trx_cursor_borrow(
    THD *worker, THD *target,
    std::shared_ptr<Preserve_trx_cursor_read_wait> *record,
    uint64_t incarnation, const Preserve_trx_phase1_record_adapter_control &control)
    : m_worker(worker), m_target(target) {
  if (!worker || worker != current_thd || !target || worker == target || !record)
    return;
  try {
    if (!*record) {
      auto memory = preserve_trx_acquire_memory_lease(
          std::to_string(target->thread_id()), Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
          sizeof(Preserve_trx_cursor_read_wait) + 64);
      if (!memory.acquired()) return;
      *record = std::make_shared<Preserve_trx_cursor_read_wait>();
      (*record)->memory = std::move(memory);
    }
  } catch (const std::bad_alloc &) { return; }
  m_record = *record;
  mysql_mutex_lock(&target->LOCK_thd_data);
  using Stage = Preserve_trx_cursor_read_stage;
  auto expected = Stage::AVAILABLE;
  const auto now = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
  if (reinterpret_cast<uintptr_t>(target) == incarnation && control.deadline_us &&
      static_cast<uint64_t>(now) < control.deadline_us &&
      (!control.cancel_probe || !control.cancel_probe(control.cancel_context)) &&
      preserve_trx_cursor_capture_enabled(target) && target->m_server_idle &&
      !target->killed && !target->release_resources_done() &&
      target->preserve_trx_batch_state == Preserve_trx_batch_thd_state::NONE &&
      !target->preserve_trx_cursor_read_wait) {
    target->preserve_trx_cursor_read_wait = m_record;
    m_record->completed = false;
    m_active = target->preserve_trx_cursor_read_stage.compare_exchange_strong(
        expected, Stage::BORROWED, std::memory_order_acq_rel);
    if (!m_active) {
      target->preserve_trx_cursor_read_wait.reset();
      m_record->completed = true;
    } else {
      m_real_id = target->real_id;
      m_stack = target->thread_stack;
    }
  }
  mysql_mutex_unlock(&target->LOCK_thd_data);
  if (!m_active) return;
  worker->restore_globals();
  target->thread_stack = worker->thread_stack;
  target->store_globals();
}

Preserve_trx_cursor_borrow::~Preserve_trx_cursor_borrow() {
  if (!m_active) return;
  m_target->restore_globals();
  m_target->real_id = m_real_id;
  m_target->thread_stack = m_stack;
  m_worker->store_globals();
  mysql_mutex_lock(&m_target->LOCK_thd_data);
  using Stage = Preserve_trx_cursor_read_stage;
  auto stage = m_target->preserve_trx_cursor_read_stage.load(std::memory_order_acquire);
  for (;;) {
    assert(stage == Stage::BORROWED || stage == Stage::NETWORK_WAITING);
    const auto next = stage == Stage::BORROWED ? Stage::AVAILABLE : Stage::UNAVAILABLE;
    if (m_target->preserve_trx_cursor_read_stage.compare_exchange_weak(
            stage, next, std::memory_order_acq_rel)) break;
  }
  m_record->completed = true;
  m_target->preserve_trx_cursor_read_wait.reset();
  mysql_cond_broadcast(&m_record->condition);
  mysql_mutex_unlock(&m_target->LOCK_thd_data);
}

namespace {
class Capture_conditions final : public Internal_error_handler {
  bool handle_condition(THD *, uint, const char *,
                        Sql_condition::enum_severity_level *, const char *) override {
    return true;  // Fatal/killed/engine rollback flags are checked separately.
  }
};
}

Preserve_trx_cursor_capture_status preserve_trx_cursor_capture_step(
    THD *thd, Server_side_cursor *cursor, uint64_t rows, uint64_t bytes) {
  using State = Preserve_trx_cursor_capture_status;
  if (!thd || thd != current_thd || !cursor || !cursor->is_open()) return State::FAILED;
  Preserve_trx_cursor_capture_input input;
  if (!cursor->preserve_capture_input(&input) || !*input.position_valid)
    return State::FAILED;
  Diagnostics_area diagnostics(false);
  Capture_conditions conditions;
  thd->push_diagnostics_area(&diagnostics, false);
  thd->push_internal_handler(&conditions);
  const auto restore = create_scope_guard([&] {
    thd->pop_internal_handler();
    thd->pop_diagnostics_area();
  });
  auto &artifact = *input.artifact;
  if (input.stream && *input.stream) {
    if (auto ready = (*input.stream)->result()) artifact = std::move(ready);
    else if (!(*input.stream)->abandoned()) return State::MORE;
    input.stream->reset();
  }
  if (artifact && artifact->failed()) artifact.reset();
  if (!artifact)
    artifact = Preserve_trx_cursor_result::create(thd, input.table,
        input.statement_id, *input.items, input.result_charset);
  State state = !artifact ? State::DEFERRED
      : artifact->sealed() ? State::COMPLETE
      : !rows ? State::MORE
      : artifact->capture_step(thd, input.table, input.fetched, rows, bytes,
                               input.rnd_inited);
  if (thd->killed || thd->is_fatal_error() || thd->transaction_rollback_request)
    state = State::FAILED;
  if (state == State::FAILED) *input.position_valid = false;
  if (state == State::DEFERRED || state == State::FAILED) artifact.reset();
  return state;
}

bool preserve_trx_cursor_capture_final(THD *thd) {
  if (!preserve_trx_cursor_capture_enabled(thd) ||
      !thd->preserve_trx_open_cursor_count.load(std::memory_order_acquire)) return false;
  for (const auto &entry : thd->stmt_map.st_hash) {
    auto &ps = *entry.second;
    if (!ps.cursor || !ps.cursor->is_open()) continue;
    Preserve_trx_cursor_snapshot snapshot;
    if (ps.cursor->preserve_snapshot(&snapshot)) continue;
    using State = Preserve_trx_cursor_capture_status;
    for (;;) {
      auto state = preserve_trx_cursor_capture_step(thd, ps.cursor, 1024, 1024 * 1024);
      if (state == State::FAILED) { ps.close_cursor(); return true; }
      if (state == State::DEFERRED) return true;
      Preserve_trx_cursor_capture_input input;
      if (!ps.cursor->preserve_capture_input(&input) || !*input.artifact) return true;
      if ((*input.artifact)->scan_complete())
        state = (*input.artifact)->seal_step(1024 * 1024);
      if (state == State::COMPLETE) break;
      if (state != State::MORE) { input.artifact->reset(); return true; }
    }
  }
  return false;
}
