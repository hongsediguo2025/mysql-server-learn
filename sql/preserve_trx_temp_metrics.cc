/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_temp_metrics.h"

#include <atomic>
#include <chrono>
#include <sstream>
#include "mysqld_error.h"
#include "sql/log.h"
#include "mysql/components/services/log_builtins.h"
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
    Stage stage, const bool *success)
    : m_stage(stage), m_started(stage == Stage::NONE ? 0 : monotonic_us()),
      m_success(success) {
  if (stage == Stage::SOURCE_FINAL) {
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

namespace {
using Final_field = Preserve_trx_temp_final_field;
std::atomic<uint64_t> final_counts[static_cast<size_t>(Final_field::COUNT)]{};
std::atomic<uint64_t> final_wall[4]{};
std::atomic<uint64_t> final_outcomes[3]{};
std::atomic<uint64_t> final_active{0}, final_dropped{0};
int show_atomic(std::atomic<uint64_t> &value, SHOW_VAR *var, char *buffer) {
  var->type = SHOW_LONGLONG;
  var->value = buffer;
  *reinterpret_cast<unsigned long long *>(buffer) = value.load(std::memory_order_relaxed);
  return 0;
}
template <Final_field field>
int show_final(THD *, SHOW_VAR *var, char *buffer) {
  return show_atomic(final_counts[static_cast<size_t>(field)], var, buffer);
}
template <size_t field>
int show_wall(THD *, SHOW_VAR *var, char *buffer) {
  return show_atomic(final_wall[field], var, buffer);
}
template <size_t field>
int show_outcome(THD *, SHOW_VAR *var, char *buffer) {
  return show_atomic(final_outcomes[field], var, buffer);
}
int show_active(THD *, SHOW_VAR *var, char *buffer) {
  return show_atomic(final_active, var, buffer);
}
int show_dropped(THD *, SHOW_VAR *var, char *buffer) {
  return show_atomic(final_dropped, var, buffer);
}
}  // namespace

void Preserve_trx_temp_final_timing::settle(uint64_t now) {
  if (!m_sample.started_us || m_sample.ended_us) return;
  const auto part = m_prepare ? (m_bind ? 2 : 0) : (m_bind ? 1 : 3);
  if (now > m_last_us) m_sample.wall_us[part] += now - m_last_us;
  m_last_us = now;
}
void Preserve_trx_temp_final_timing::begin(
    uint64_t now, const Preserve_trx_temp_final_counts &counts) {
  if (m_sample.ended_us) return;
  if (!m_sample.started_us) {
    m_sample.started_us = m_last_us = now;
    final_active.fetch_add(1, std::memory_order_relaxed);
  }
  for (size_t n = 0; n < counts.size(); ++n) {
    m_sample.counts[n] += counts[n];
    final_counts[n].fetch_add(counts[n], std::memory_order_relaxed);
  }
}
void Preserve_trx_temp_final_timing::activity(uint64_t now, int prepare, int bind) {
  settle(now);
  if (prepare > 0) ++m_prepare;
  else if (prepare < 0 && m_prepare) --m_prepare;
  if (bind > 0) ++m_bind;
  else if (bind < 0 && m_bind) --m_bind;
}
void Preserve_trx_temp_final_timing::note(Final_field field, uint64_t value) {
  if (!m_sample.started_us || m_sample.ended_us) return;
  m_sample.counts[static_cast<size_t>(field)] += value;
  final_counts[static_cast<size_t>(field)].fetch_add(value, std::memory_order_relaxed);
}
Preserve_trx_temp_final_sample Preserve_trx_temp_final_timing::finish(
    uint64_t now, Preserve_trx_temp_final_outcome outcome) {
  if (!m_sample.started_us || m_sample.ended_us) return {};
  settle(now);
  m_sample.ended_us = now;
  m_sample.outcome = outcome;
  for (size_t n = 0; n < 4; ++n)
    final_wall[n].fetch_add(m_sample.wall_us[n], std::memory_order_relaxed);
  final_outcomes[static_cast<size_t>(outcome)].fetch_add(1, std::memory_order_relaxed);
  final_active.fetch_sub(1, std::memory_order_relaxed);
  return m_sample;
}
void preserve_trx_temp_final_observation_dropped() {
  final_dropped.fetch_add(1, std::memory_order_relaxed);
}
void preserve_trx_temp_final_log(const std::string &epoch,
                                const Preserve_trx_temp_final_sample &s) {
  if (!s.started_us) return;
  try {
    std::ostringstream log;
    log << "PRESERVE_TEMP_FINAL_V1 epoch_id=" << epoch
        << " outcome=" << (s.outcome == Preserve_trx_temp_final_outcome::READY ? "READY" :
          s.outcome == Preserve_trx_temp_final_outcome::PARTIAL ? "PARTIAL" : "CANCELLED")
        << " begin_us=" << s.started_us << " end_us=" << s.ended_us
        << " wall_us=" << s.ended_us - s.started_us
        << " prepare_only_us=" << s.wall_us[0] << " bind_only_us=" << s.wall_us[1]
        << " overlap_us=" << s.wall_us[2] << " other_us=" << s.wall_us[3];
#define PRESERVE_FINAL_LOG(field, name) \
    log << " " #name "=" << s.counts[static_cast<size_t>(Final_field::field)];
    PRESERVE_TRX_TEMP_FINAL_FIELDS(PRESERVE_FINAL_LOG)
#undef PRESERVE_FINAL_LOG
    LogErr(SYSTEM_LEVEL, ER_LOG_PRINTF_MSG, log.str().c_str());
  } catch (...) { preserve_trx_temp_final_observation_dropped(); }
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
#define PRESERVE_FINAL_SHOW(field, name) \
  {"final_" #name, reinterpret_cast<char *>(&show_final<Final_field::field>), SHOW_FUNC, SHOW_SCOPE_GLOBAL},
  PRESERVE_TRX_TEMP_FINAL_FIELDS(PRESERVE_FINAL_SHOW)
#undef PRESERVE_FINAL_SHOW
  {"final_prepare_only_us", reinterpret_cast<char *>(&show_wall<0>), SHOW_FUNC, SHOW_SCOPE_GLOBAL},
  {"final_bind_only_us", reinterpret_cast<char *>(&show_wall<1>), SHOW_FUNC, SHOW_SCOPE_GLOBAL},
  {"final_overlap_us", reinterpret_cast<char *>(&show_wall<2>), SHOW_FUNC, SHOW_SCOPE_GLOBAL},
  {"final_other_us", reinterpret_cast<char *>(&show_wall<3>), SHOW_FUNC, SHOW_SCOPE_GLOBAL},
  {"final_ready_epochs", reinterpret_cast<char *>(&show_outcome<0>), SHOW_FUNC, SHOW_SCOPE_GLOBAL},
  {"final_partial_epochs", reinterpret_cast<char *>(&show_outcome<1>), SHOW_FUNC, SHOW_SCOPE_GLOBAL},
  {"final_cancelled_epochs", reinterpret_cast<char *>(&show_outcome<2>), SHOW_FUNC, SHOW_SCOPE_GLOBAL},
  {"final_active_epochs", reinterpret_cast<char *>(&show_active), SHOW_FUNC, SHOW_SCOPE_GLOBAL},
  {"final_dropped_observations", reinterpret_cast<char *>(&show_dropped), SHOW_FUNC, SHOW_SCOPE_GLOBAL},
  {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_UNDEF}
};
