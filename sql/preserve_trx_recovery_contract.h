/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#ifndef SQL_PRESERVE_TRX_RECOVERY_CONTRACT_H
#define SQL_PRESERVE_TRX_RECOVERY_CONTRACT_H

#include <cstdint>

struct Preserve_snapshot_metadata;

/** Authenticated engine recovery basis, independent of retained resources.
LEGACY preserves the v1/v2 contract and never authorizes a new no-redo owner. */
enum class Preserve_trx_engine_recovery : uint8_t {
  LEGACY = 0,
  REDO_RESURRECTION = 1,
  TEMP_UNDO = 2,
  READ_CONTEXT = 3,
  NONE = 4
};

struct Preserve_trx_recovery_contract {
  Preserve_trx_engine_recovery basis{Preserve_trx_engine_recovery::LEGACY};
  bool sql_transaction_active{false};
  bool explicit_begin{false};
  uint64_t owner_trx_id{0};
  uint64_t freeze_lsn{0};
  bool resource_only() const {
    return basis == Preserve_trx_engine_recovery::NONE;
  }
  bool needs_no_redo_context() const {
    return basis == Preserve_trx_engine_recovery::TEMP_UNDO ||
           basis == Preserve_trx_engine_recovery::READ_CONTEXT;
  }
  bool operator==(const Preserve_trx_recovery_contract &other) const {
    return basis == other.basis && sql_transaction_active == other.sql_transaction_active &&
           explicit_begin == other.explicit_begin && owner_trx_id == other.owner_trx_id &&
           freeze_lsn == other.freeze_lsn;
  }
};

bool preserve_trx_recovery_header_valid(const Preserve_snapshot_metadata &);
bool preserve_trx_recovery_payload_valid(const Preserve_snapshot_metadata &);

/** Set the sole recovery classification and derive legacy display fields. */
bool preserve_trx_set_recovery_contract(
    Preserve_snapshot_metadata *, const Preserve_trx_recovery_contract &);

#endif
