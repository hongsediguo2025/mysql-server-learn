/* Copyright (c) 2026, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation. This program is distributed without any warranty;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details. */

#include "trx0temp_preserve_id.h"

#include <atomic>

#include "my_dbug.h"

bool preserve_trx_temp_id_namespace = true;

namespace {

std::atomic<bool> namespace_checked{false};
std::atomic<uint64_t> last_table_id{TRX_PRESERVE_TEMP_TABLE_ID_BEGIN - 1};
std::atomic<uint64_t> last_index_id{0};

bool allocate_id(std::atomic<uint64_t> *counter, uint64_t last,
                 uint64_t *output) {
  auto current = counter->load(std::memory_order_relaxed);
  while (current < last) {
    if (counter->compare_exchange_weak(current, current + 1,
                                      std::memory_order_relaxed)) {
      *output = current + 1;
      return true;
    }
  }
  return false;
}

}  // namespace

dberr_t trx_preserve_temp_id_check_boot(uint64_t persistent_table_id,
                                       uint64_t upgrade_offset) {
  if (!preserve_trx_temp_id_namespace) return DB_SUCCESS;
  if (upgrade_offset >= TRX_PRESERVE_TEMP_TABLE_ID_BEGIN ||
      persistent_table_id >= TRX_PRESERVE_TEMP_TABLE_ID_BEGIN - upgrade_offset) {
    return DB_UNSUPPORTED;
  }
  namespace_checked.store(true, std::memory_order_release);
  return DB_SUCCESS;
}

bool trx_preserve_temp_id_namespace_ready() {
  return preserve_trx_temp_id_namespace &&
         namespace_checked.load(std::memory_order_acquire);
}

dberr_t trx_preserve_temp_allocate_ids(uint64_t *table_id, uint64_t *index_id) {
  if (!trx_preserve_temp_id_namespace_ready()) return DB_UNSUPPORTED;
  if (table_id == nullptr && index_id == nullptr) return DB_ERROR;
  uint64_t table = 0;
  uint64_t index = 0;
  DBUG_EXECUTE_IF("preserve_temp_table_id_exhausted", {
    if (table_id != nullptr) return DB_OUT_OF_FILE_SPACE;
  });
  DBUG_EXECUTE_IF("preserve_temp_index_id_exhausted", {
    if (index_id != nullptr) return DB_OUT_OF_FILE_SPACE;
  });
  if ((table_id != nullptr &&
       !allocate_id(&last_table_id, TRX_PRESERVE_TEMP_TABLE_ID_END - 1, &table)) ||
      (index_id != nullptr &&
       !allocate_id(&last_index_id, UINT32_MAX, &index))) {
    return DB_OUT_OF_FILE_SPACE;
  }
  if (table_id != nullptr) *table_id = table;
  if (index_id != nullptr) *index_id = index;
  return DB_SUCCESS;
}
