/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_result_cursor.h"
#include "sql/preserve_trx_temp_metrics.h"

#include <atomic>
#include <climits>
#include <limits>
#include <new>

#include "scope_guard.h"
#include "sql/protocol_classic.h"
#include "sql/query_result.h"
#include "sql/sql_class.h"
#include "sql/sql_prepare.h"

namespace {
std::atomic<uint64_t> restored_cursors{0};
using Status = Preserve_trx_file_status;
}

Preserve_trx_result_cursor::Preserve_trx_result_cursor(
    THD *thd, Preserve_trx_cursor_snapshot state,
    std::unique_ptr<Preserve_trx_cursor_decoder> decoder,
    Preserve_memory_lease memory)
    : Server_side_cursor(nullptr), m_thd(thd), m_state(std::move(state)),
      m_decoder(std::move(decoder)), m_memory(std::move(memory)) {}

Preserve_trx_result_cursor::~Preserve_trx_result_cursor() {
  close();
  if (result != nullptr) destroy(result);
  mem_root.Clear();  // Release arena storage before returning its memory lease.
}

Status Preserve_trx_result_cursor::create(
    const std::string &token, THD *thd, Preserve_trx_cursor_snapshot state,
    std::unique_ptr<Preserve_trx_cursor_decoder> decoder,
    std::unique_ptr<Preserve_trx_result_cursor> *output) {
  if (!thd || !decoder || !output || !state.open || !state.file ||
      !state.file->matches(state.descriptor.size, state.descriptor.digest) ||
      state.fetch_count > state.descriptor.rows ||
      state.fetch_count > std::numeric_limits<ulong>::max() ||
      state.fetch_limit > std::numeric_limits<ulong>::max()) return Status::CORRUPT;
  if (!decoder->descriptor().matches(state.descriptor))
    return Status::CORRUPT;
  try {
    const uint64_t arena_bytes = 8192 + decoder->items().size() * 16;
    auto memory = preserve_trx_acquire_memory_lease(
        token, Preserve_trx_memory_kind::CURSOR_RESULT_BUFFER,
        sizeof(Preserve_trx_result_cursor) + arena_bytes);
    if (!memory.acquired()) return Status::OUT_OF_MEMORY;
    const auto status = decoder->seek(state.fetch_count);
    if (status != Status::OK) return status;
    decoder->bind(thd);
    std::unique_ptr<Preserve_trx_result_cursor> c(new Preserve_trx_result_cursor(
        thd, std::move(state), std::move(decoder), std::move(memory)));
    c->mem_root.set_max_capacity(arena_bytes);
    Query_arena backup;
    thd->swap_query_arena(c->m_arena, &backup);
    auto restore = create_scope_guard([&] { thd->swap_query_arena(backup, &c->m_arena); });
    DBUG_EXECUTE_IF("preserve_cursor_sender_factory_oom", {
      return Status::OUT_OF_MEMORY;
    });
    c->result = preserve_trx_create_cursor_sender(thd, &c->mem_root, *c->m_decoder);
    if (!c->result) return Status::OUT_OF_MEMORY;
    thd->swap_query_arena(backup, &c->m_arena);
    restore.commit();
    if (!preserve_trx_cursor_observe_generation(c->m_state.descriptor.generation))
      return Status::CORRUPT;
    *output = std::move(c);
    ++restored_cursors;
    return Status::OK;
  } catch (const std::bad_alloc &) {
    return Status::OUT_OF_MEMORY;
  }
}

bool Preserve_trx_result_cursor::open(THD *thd) {
  if (!is_open() || !m_decoder || thd != m_thd) return true;
  return false;  // Factory has initialized the sender; never resend metadata.
}

bool Preserve_trx_result_cursor::bind(THD *thd) {
  if (preserve_trx_bind_cursor_sender(static_cast<Query_result_send *>(result), thd))
    return true;
  m_thd = thd;
  m_decoder->bind(thd);
  return false;
}

bool Preserve_trx_result_cursor::fetch(ulong count) {
  if (!is_open()) return true;
  bool success = false;
  Preserve_trx_temp_stage_timer timer(m_first_fetch
      ? Preserve_trx_temp_stage::FIRST_FETCH : Preserve_trx_temp_stage::NONE, &success);
  m_first_fetch = false;
  result->begin_dataset();
  m_state.fetch_limit = static_cast<ulong>(m_state.fetch_limit) + count;
  for (; m_state.fetch_count < m_state.fetch_limit; ++m_state.fetch_count) {
    if (m_thd->killed) {
      m_thd->send_kill_message();
      close();
      return true;
    }
    if (m_state.fetch_count == m_state.descriptor.rows) {
      m_thd->server_status |= SERVER_STATUS_LAST_ROW_SENT;
      result->send_eof(m_thd);
      close();
      success = !m_thd->is_error();
      return false;
    }
    const auto status = m_decoder->read_next();
    if (status != Status::OK) {
      if (status == Status::OUT_OF_MEMORY) my_error(ER_OUT_OF_RESOURCES, MYF(0));
      else my_error(ER_INTERNAL_ERROR, MYF(0), "preserved cursor decode failed");
      close();
      return true;
    }
    if (result->send_data(m_thd, m_decoder->items())) {
      m_position_valid = false;
      return true;
    }
  }
  m_thd->server_status |= SERVER_STATUS_CURSOR_EXISTS;
  if (result->send_eof(m_thd) || m_thd->is_error()) m_position_valid = false;
  success = m_position_valid;
  return false;
}

void Preserve_trx_result_cursor::close() {
  m_state.open = false;
  m_decoder.reset();
  m_state.artifact.reset();
  m_state.file.reset();
}

bool Preserve_trx_result_cursor::preserve_snapshot(
    Preserve_trx_cursor_snapshot *output) const {
  if (!output || !is_open() || !m_position_valid) return false;
  *output = m_state;
  return true;
}

int show_preserve_trx_cursor_restored_cursors(THD *, SHOW_VAR *var, char *buffer) {
  var->type = SHOW_LONGLONG;
  var->value = buffer;
  *reinterpret_cast<long long *>(buffer) = restored_cursors.load();
  return 0;
}

bool Protocol_binary::initialize_preserved_metadata(
    uint columns, const enum_field_types *types, const CHARSET_INFO *charset) {
  if (columns == 0 || columns > UINT_MAX - 9 || types == nullptr ||
      packet->alloc((columns + 9) / 8 + 1)) return true;
  if (start_result_metadata(columns, 0, charset)) return true;
#ifndef DBUG_OFF
  if (m_thd->variables.resultset_metadata == RESULTSET_METADATA_FULL) {
    if (field_types == nullptr) return true;
    // Match send_field_metadata()'s Classic compatibility conversion.
    for (uint i = 0; i < columns; ++i)
      field_types[i] = types[i] == MYSQL_TYPE_VARCHAR ? MYSQL_TYPE_VAR_STRING : types[i];
  }
#endif
  return end_result_metadata();  // Both SEND_NUM_ROWS and SEND_EOF are absent.
}

bool Protocol_binary::bind_preserved(THD *thd) {
  // The target packet belongs to the new connection. Reserve its row bitmap
  // before changing ownership; init() would discard our cached column types.
  String *target_packet = thd ? thd->get_protocol_classic()->get_output_packet() : nullptr;
  if (target_packet && target_packet->alloc(bit_fields + 1)) return true;
  m_thd = thd;
  packet = target_packet;
  return false;
}
