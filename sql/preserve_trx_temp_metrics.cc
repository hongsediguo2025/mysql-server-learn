/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_temp_metrics.h"

#include <atomic>
#include <chrono>
#include "sql/preserve_trx.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"

namespace {
using Stage = Preserve_trx_temp_stage;
thread_local Preserve_trx_temp_stage_timer *active_final = nullptr;
enum Field { CALLS, US, MAX_US, READ_BYTES, WRITTEN_BYTES,
             SUCCESS_CALLS, FAILURE_CALLS, UNCLASSIFIED_CALLS, FIELDS };
std::atomic<uint64_t> metrics[static_cast<size_t>(Stage::NONE)][FIELDS]{};
void note(Stage stage, uint64_t elapsed, uint64_t read, uint64_t written,
          const bool *success = nullptr) {
  auto &counters = metrics[static_cast<size_t>(stage)];
  counters[CALLS].fetch_add(1, std::memory_order_relaxed);
  counters[US].fetch_add(elapsed, std::memory_order_relaxed);
  counters[READ_BYTES].fetch_add(read, std::memory_order_relaxed);
  counters[WRITTEN_BYTES].fetch_add(written, std::memory_order_relaxed);
  counters[success == nullptr ? UNCLASSIFIED_CALLS :
      (*success ? SUCCESS_CALLS : FAILURE_CALLS)].fetch_add(1, std::memory_order_relaxed);
  auto maximum = counters[MAX_US].load(std::memory_order_relaxed);
  while (maximum < elapsed && !counters[MAX_US].compare_exchange_weak(
      maximum, elapsed, std::memory_order_relaxed)) {}
}
uint64_t monotonic_us() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}
template <Stage stage, Field field>
int show(THD *, SHOW_VAR *var, char *buffer) {
  var->type = SHOW_LONGLONG;
  var->value = buffer;
  *reinterpret_cast<unsigned long long *>(buffer) =
      metrics[static_cast<size_t>(stage)][field].load(std::memory_order_relaxed);
  return 0;
}
}  // namespace

Preserve_trx_temp_stage_timer::Preserve_trx_temp_stage_timer(
    Stage stage, const bool *success, bool final_only)
    : m_stage(final_only && !active_final ? Stage::NONE : stage),
      m_started(m_stage == Stage::NONE ? 0 : monotonic_us()),
      m_success(success) {
  if (m_stage == Stage::SOURCE_FINAL) {
    m_previous_final = active_final;
    active_final = this;
  }
}

Preserve_trx_temp_stage_timer::~Preserve_trx_temp_stage_timer() {
  if (m_stage == Stage::SOURCE_FINAL) active_final = m_previous_final;
  if (m_stage == Stage::NONE) return;
  note(m_stage, monotonic_us() - m_started, m_read, m_written, m_success);
}

Preserve_trx_temp_first_dml_timer::Preserve_trx_temp_first_dml_timer(
    THD *thd, const LEX *lex) {
  if (!thd->preserve_trx_temp_first_dml_pending) return;
  if (!preserve_trx_is_enabled()) {
    thd->preserve_trx_temp_first_dml_pending = false;
    return;
  }
  if (thd->in_sub_stmt || thd->sp_runtime_ctx) return;
  if (!lex) lex = thd->lex;
  if (lex->is_explain()) return;
  switch (lex->sql_command) {
    case SQLCOM_INSERT: case SQLCOM_INSERT_SELECT:
    case SQLCOM_UPDATE: case SQLCOM_UPDATE_MULTI:
    case SQLCOM_DELETE: case SQLCOM_DELETE_MULTI:
    case SQLCOM_REPLACE: case SQLCOM_REPLACE_SELECT: case SQLCOM_LOAD:
      break;
    default: return;
  }
  thd->preserve_trx_temp_first_dml_pending = false;
  m_thd = thd;
  m_started = monotonic_us();
}

Preserve_trx_temp_first_dml_timer::~Preserve_trx_temp_first_dml_timer() {
  if (!m_thd) return;
  const bool success = !m_thd->is_error() && !m_thd->killed;
  note(Stage::FIRST_DML, monotonic_us() - m_started, 0, 0, &success);
}

void preserve_trx_temp_final_read(uint64_t bytes) {
  if (active_final) active_final->read(bytes);
}
void preserve_trx_temp_final_write(uint64_t bytes) {
  if (active_final) active_final->write(bytes);
}

void preserve_trx_temp_prepared_job_note(uint64_t elapsed_us) {
  note(Stage::RECEIVER_PREPARED, elapsed_us, 0, 0);
}

SHOW_VAR preserve_trx_temp_stage_status[] = {
#define PRESERVE_STAGE_SHOW(stage, name) \
  {#name "_calls", reinterpret_cast<char *>(&show<Stage::stage, CALLS>), SHOW_FUNC, SHOW_SCOPE_GLOBAL}, \
  {#name "_us", reinterpret_cast<char *>(&show<Stage::stage, US>), SHOW_FUNC, SHOW_SCOPE_GLOBAL}, \
  {#name "_max_us", reinterpret_cast<char *>(&show<Stage::stage, MAX_US>), SHOW_FUNC, SHOW_SCOPE_GLOBAL}, \
  {#name "_read_bytes", reinterpret_cast<char *>(&show<Stage::stage, READ_BYTES>), SHOW_FUNC, SHOW_SCOPE_GLOBAL}, \
  {#name "_written_bytes", reinterpret_cast<char *>(&show<Stage::stage, WRITTEN_BYTES>), SHOW_FUNC, SHOW_SCOPE_GLOBAL}, \
  {#name "_success_calls", reinterpret_cast<char *>(&show<Stage::stage, SUCCESS_CALLS>), SHOW_FUNC, SHOW_SCOPE_GLOBAL}, \
  {#name "_failure_calls", reinterpret_cast<char *>(&show<Stage::stage, FAILURE_CALLS>), SHOW_FUNC, SHOW_SCOPE_GLOBAL}, \
  {#name "_unclassified_calls", reinterpret_cast<char *>(&show<Stage::stage, UNCLASSIFIED_CALLS>), SHOW_FUNC, SHOW_SCOPE_GLOBAL},
  PRESERVE_TRX_TEMP_STAGES(PRESERVE_STAGE_SHOW)
#undef PRESERVE_STAGE_SHOW
  {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_UNDEF}
};
