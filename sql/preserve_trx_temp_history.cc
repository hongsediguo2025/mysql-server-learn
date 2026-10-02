/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_temp_history.h"

#include <algorithm>
#include <new>
#include <set>
#include "sql/preserve_trx.h"
#include "sql/preserve_trx_temp_table.h"
#include "scope_guard.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_parse.h"
#include "storage/innobase/include/trx0temp_preserve_source.h"

bool preserve_trx_temp_history_committing_ddl(const THD *thd) {
  if (!thd || !thd->lex ||
      (thd->lex->sql_command != SQLCOM_TRUNCATE &&
       thd->lex->sql_command != SQLCOM_ALTER_TABLE &&
       thd->lex->sql_command != SQLCOM_CREATE_INDEX &&
       thd->lex->sql_command != SQLCOM_DROP_INDEX)) return false;
  // Native full commit retires the old participant and undo. Do not introduce
  // a sticky batch rejection for the next transaction in the same packet.
  // CREATE/DROP INDEX use ALTER's implementation but retain their SQLCOM.
  return stmt_causes_implicit_commit(thd, CF_IMPLICIT_COMMIT_BEGIN) &&
         stmt_causes_implicit_commit(thd, CF_IMPLICIT_COMMIT_END);
}

bool preserve_trx_temp_history_only(const Preserved_temp_table_manifest &m) {
  if (!m.tables.empty() || !m.owner_trx_id || m.undo_images.size() != 1 ||
      m.ownership_claims.empty()) return false;
  return preserve_trx_temp_undo_is_independent(m.undo_images.front());
}

bool preserve_trx_temp_undo_is_independent(
    const Preserved_temp_table_undo_descriptor &undo) {
  return undo.source_space_id != 0 &&
      undo.source_space_id == undo.no_redo_undo_rseg_space_id &&
      undo.page_size >= 4096 && undo.page_size <= 65536 &&
      (undo.page_size & (undo.page_size - 1)) == 0;
}

dberr_t preserve_trx_temp_history_capture_undo(
    const trx_t *trx, const std::string &dir, const std::string &token,
    Preserved_temp_table_manifest *manifest) {
  if (!manifest || !manifest->tables.empty() || !manifest->undo_images.empty() ||
      !manifest->ownership_claims.empty()) return DB_ERROR;
  Preserve_memory_lease memory;
  trx_preserve_temp_space_image_descriptor source;
  uint64_t bytes = 0;
  if (!trx_preserve_temp_source_undo_identity(trx, &source) ||
      !trx_preserve_temp_source_undo_baseline_bytes(trx, &bytes)) return DB_UNSUPPORTED;
  memory = preserve_trx_acquire_memory_lease(
      token, Preserve_trx_memory_kind::TEMP_IMAGE_STREAM_BUFFER, bytes);
  if (!memory.acquired()) return DB_OUT_OF_MEMORY;
  auto reset = create_scope_guard([&] {
    trx_preserve_temp_space_image_reset_dirty_page_stream(&source);
  });
  auto error = trx_preserve_temp_space_image_capture_no_redo_undo_from_trx(&source, trx, true);
  if (error == DB_SUCCESS)
    error = trx_preserve_temp_space_image_seal_no_redo_undo_sidecar(&source);
  std::string payload;
  if (error == DB_SUCCESS)
    error = trx_preserve_temp_space_image_build_no_redo_undo_sidecar_payload(source, &payload);
  if (error != DB_SUCCESS) return error;
  if (payload.empty() || payload.size() > preserve_trx_max_temp_sidecar_bytes)
    return DB_OUT_OF_FILE_SPACE;
  auto undo = preserve_trx_temp_undo_descriptor(token, source, payload);
  Local_file_preserved_temp_table_image_carrier carrier(dir);
  bool own_warm = false, own_sealed = false, installed = false;
  auto cleanup = create_scope_guard([&] {
    if (own_warm) (void)carrier.remove_warm_undo(token, source.source_space_id);
    if (own_sealed && !installed)
      (void)carrier.remove_sealed_undo(token, source.source_space_id);
  });
  using Status = Preserved_trx_carrier_status;
  auto status = carrier.write_warm_undo(token, source.source_space_id,
      reinterpret_cast<const unsigned char *>(payload.data()), payload.size());
  own_warm = status == Status::OK || status == Status::IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST;
  if (status != Status::OK) return DB_IO_ERROR;
  status = carrier.seal_warm_undo(token, token, undo);
  own_sealed = status == Status::OK || status == Status::IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST;
  if (status != Status::OK) return DB_IO_ERROR;
  if (!preserve_trx_temp_table_append_ownership_claims_from_descriptor(
          token, undo, source, manifest)) return DB_CORRUPTION;
  manifest->undo_images.push_back(std::move(undo));
  installed = true;
  return DB_SUCCESS;
}

bool Preserve_trx_temp_history::request(
    const std::string &token, const Preserved_temp_retired_table &table) {
  constexpr size_t limit = 16384;
  if (!table.table_id || !table.source_space_id || !table.table_ordinal ||
      !table.generation || !table.drop_sequence || m_drops.size() >= limit ||
      std::any_of(m_drops.begin(), m_drops.end(), [&](const Pending &p) {
        return p.table.table_id == table.table_id ||
               p.table.drop_sequence == table.drop_sequence;
      })) return false;
  try {
    if (m_drops.size() == m_drops.capacity()) {
      const auto capacity = std::min(limit, std::max<size_t>(16, m_drops.size() * 2));
      // Charge old and replacement buffers during vector growth.
      const auto bytes = sizeof(*this) +
                         (capacity + m_drops.capacity()) * sizeof(Pending);
      if (!m_memory.acquired()) {
        m_memory = preserve_trx_acquire_memory_lease(
            token, Preserve_trx_memory_kind::SNAPSHOT_CODEC_BUFFER, bytes);
        if (!m_memory.acquired()) return false;
      } else if (!m_memory.grow_to(bytes)) return false;
      m_drops.reserve(capacity);
      (void)m_memory.shrink_to(sizeof(*this) + capacity * sizeof(Pending));
    }
    m_drops.push_back({table, false});
    return true;
  } catch (const std::bad_alloc &) { return false; }
}

void Preserve_trx_temp_history::confirm(uint64_t id, uint32_t space) {
  for (auto &drop : m_drops)
    if (drop.table.table_id == id && drop.table.source_space_id == space)
      drop.complete = true;
}

bool Preserve_trx_temp_history::confirmed(uint64_t sequence) const {
  for (const auto &drop : m_drops)
    if (drop.table.drop_sequence == sequence) return drop.complete;
  return false;
}

bool Preserve_trx_temp_history::export_retired(
    std::vector<Preserved_temp_retired_table> *out) const {
  if (!out || !out->empty()) return false;
  for (const auto &drop : m_drops) if (!drop.complete) return false;
  try {
    out->reserve(m_drops.size());
    for (const auto &drop : m_drops) out->push_back(drop.table);
    std::sort(out->begin(), out->end(), [](const auto &a, const auto &b) {
      return a.table_id < b.table_id;
    });
    return true;
  } catch (const std::bad_alloc &) { return false; }
}

void preserve_trx_temp_table_confirm_native_drop(
    THD *thd, uint64_t table_id, uint32_t source_space_id) {
  if (!preserve_trx_is_enabled() || !preserve_trx_temp_table_enable ||
      !preserve_trx_temp_id_namespace || !thd) return;
  auto participant = preserve_trx_temp_table_pin_participant(thd);
  if (participant) participant->confirm_native_drop(table_id, source_space_id);
}

bool preserve_trx_temp_history_valid(const Preserved_temp_table_manifest &m) {
  if (m.retired_tables.size() > 16384 ||
      (!m.retired_tables.empty() && !m.sealed_history_sequence)) return false;
  std::set<uint64_t> ids, sequences;
  std::set<uint32_t> ordinals;
  for (const auto &table : m.tables) {
    if (!table.generation || !ids.insert(table.image.image_table_id).second ||
        !ordinals.insert(table.table_ordinal).second) return false;
  }
  uint64_t last_id = 0;
  for (const auto &table : m.retired_tables) {
    if (table.table_id <= last_id || table.table_id == UINT64_MAX ||
        !table.source_space_id || table.source_space_id == UINT32_MAX ||
        !table.table_ordinal || !table.generation || !table.drop_sequence ||
        table.drop_sequence > m.sealed_history_sequence ||
        !ids.insert(table.table_id).second ||
        !ordinals.insert(table.table_ordinal).second ||
        !sequences.insert(table.drop_sequence).second) return false;
    last_id = table.table_id;
  }
  return true;
}

bool Temp_table_warmcopy_participant::undo_history_unchanged_since(
    uint64_t history_sequence, uint64_t data_generation) const {
  std::lock_guard<std::recursive_mutex> guard(m_state_mutex);
  return history_sequence < m_next_sequence &&
         history_sequence >= m_last_undo_mutation_sequence &&
         data_generation == m_data_generation.load(std::memory_order_acquire);
}

bool Temp_table_warmcopy_participant::note_native_drop(
    uint32_t ordinal, uint64_t table_id, uint32_t source_space_id,
    const std::string &token) {
  std::lock_guard<std::recursive_mutex> guard(m_state_mutex);
  const auto *table = find_table(ordinal);
  if (!table || !m_ddl_history.request(token,
          {table_id, source_space_id, ordinal, table->generation, m_next_sequence})) {
    mark_degraded("temporary DROP identity unavailable");
    return false;
  }
  return note_drop_table(ordinal);
}

void Temp_table_warmcopy_participant::confirm_native_drop(
    uint64_t table_id, uint32_t source_space_id) {
  std::lock_guard<std::recursive_mutex> guard(m_state_mutex);
  m_ddl_history.confirm(table_id, source_space_id);
}

bool Temp_table_warmcopy_participant::export_retired_tables(
    Preserved_temp_table_manifest *manifest) const {
  std::lock_guard<std::recursive_mutex> guard(m_state_mutex);
  if (!manifest || !m_ddl_history.export_retired(&manifest->retired_tables)) return false;
  manifest->sealed_history_sequence = m_next_sequence - 1;
  return true;
}

uint64_t Temp_table_warmcopy_participant::history_work_bytes() const {
  std::lock_guard<std::recursive_mutex> guard(m_state_mutex);
  // Manifest copy, encoded payload, and simultaneous validation indexes.
  return m_ddl_history.size() ? 65536 + 512 * m_ddl_history.size() +
                                         192 * m_tables.size() : 0;
}
