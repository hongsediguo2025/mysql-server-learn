/* Copyright (c) 2026, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation. This program is distributed without any warranty;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details. */

#include "sql/preserve_trx_temp_id_contract.h"

#include "storage/innobase/include/trx0temp_preserve_id.h"

namespace {
constexpr Preserve_trx_temp_id_contract kTemporaryNamespace{
    1, 1, TRX_PRESERVE_TEMP_TABLE_ID_BEGIN, TRX_PRESERVE_TEMP_TABLE_ID_END,
    TRX_PRESERVE_TEMP_TABLE_ID_BEGIN};
}

bool Preserve_trx_temp_id_contract::operator==(
    const Preserve_trx_temp_id_contract &other) const {
  return version == other.version && policy == other.policy &&
         table_id_begin == other.table_id_begin &&
         table_id_end == other.table_id_end &&
         persistent_table_id_limit == other.persistent_table_id_limit;
}

bool Preserve_trx_temp_id_contract::empty() const {
  return *this == Preserve_trx_temp_id_contract{};
}

bool Preserve_trx_temp_id_contract::supported() const {
  return *this == kTemporaryNamespace;
}

bool preserve_trx_temp_id_local_contract(Preserve_trx_temp_id_contract *out) {
  if (out == nullptr) return false;
  if (!preserve_trx_temp_id_namespace) {
    *out = {};
    return true;
  }
  if (!trx_preserve_temp_id_namespace_ready()) return false;
  *out = kTemporaryNamespace;
  return true;
}
