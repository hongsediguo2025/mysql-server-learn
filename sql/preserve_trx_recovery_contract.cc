/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_recovery_contract.h"

#include <algorithm>

#include "sql/preserve_trx_bundle.h"

bool preserve_trx_recovery_header_valid(const Preserve_snapshot_metadata &m) {
  const auto &c = m.recovery;
  if (c.basis == Preserve_trx_engine_recovery::LEGACY)
    return !c.sql_transaction_active && !c.explicit_begin &&
           c.owner_trx_id == 0 && c.freeze_lsn == 0;
  if (c.explicit_begin && !c.sql_transaction_active) return false;
  if (c.sql_transaction_active && !c.explicit_begin && m.autocommit) return false;
  if (c.basis == Preserve_trx_engine_recovery::NONE)
    return c.owner_trx_id == 0 && c.freeze_lsn == 0 &&
           !m.has_persistent_engine_state && !m.has_logged_persistent_work &&
           m.engine_shape == Preserve_snapshot_engine_shape::NONE;
  if (c.owner_trx_id == 0 || c.owner_trx_id >= (1ULL << 48) ||
      c.freeze_lsn == 0 || !c.sql_transaction_active) return false;
  switch (c.basis) {
    case Preserve_trx_engine_recovery::TEMP_UNDO:
      return !m.has_persistent_engine_state && m.has_temp_engine_state &&
             !m.has_logged_persistent_work &&
             m.engine_shape == Preserve_snapshot_engine_shape::TEMP_ONLY;
    case Preserve_trx_engine_recovery::READ_CONTEXT:
      if (m.has_logged_persistent_work) return false;
      [[fallthrough]];
    case Preserve_trx_engine_recovery::REDO_RESURRECTION:
      return m.has_persistent_engine_state &&
             m.engine_shape == (m.has_temp_engine_state
                 ? Preserve_snapshot_engine_shape::MIXED
                 : Preserve_snapshot_engine_shape::PERSISTENT_ONLY);
    default:
      return false;
  }
}

bool preserve_trx_recovery_payload_valid(const Preserve_snapshot_metadata &m) {
  if (!preserve_trx_recovery_header_valid(m)) return false;
  switch (m.recovery.basis) {
    case Preserve_trx_engine_recovery::TEMP_UNDO:
      // Exact undo presence and owner are checked against the decoded manifest.
      return !m.temp_table_manifest_payload.empty();
    case Preserve_trx_engine_recovery::READ_CONTEXT:
      return !m.read_view_payload.empty() || !m.record_locks_payload.empty() ||
             !m.table_locks_payload.empty() ||
             ((!m.temp_table_manifest_payload.empty() ||
               !m.cursor_manifest_payload.empty()) &&
              std::find(m.session_participant_order.begin(),
                        m.session_participant_order.end(),
                        Preserve_savepoint_participant::INNODB) !=
                  m.session_participant_order.end());
    case Preserve_trx_engine_recovery::NONE:
      return m.read_view_payload.empty() && m.record_locks_payload.empty() &&
             m.table_locks_payload.empty() && m.predicate_locks_payload.empty() &&
             !m.has_read_view && m.rv_low_limit_no == 0 && !m.autoinc_lock_owned &&
             m.mod_tables_count == 0 && m.modified_table_names.empty() &&
             m.innodb_savepoints_payload.empty() &&
             m.session_participant_order.empty() &&
             m.binlog_state != Preserve_snapshot_binlog_state::LOGGED_WITH_CACHE &&
             !m.has_logged_persistent_work &&
             (!m.temp_table_manifest_payload.empty() || !m.cursor_manifest_payload.empty() ||
              (m.recovery.explicit_begin && m.recovery.sql_transaction_active));
    default:
      return true;
  }
}

bool preserve_trx_set_recovery_contract(
    Preserve_snapshot_metadata *m, const Preserve_trx_recovery_contract &c) {
  if (m == nullptr || c.basis == Preserve_trx_engine_recovery::LEGACY) return false;
  m->recovery = c;
  m->has_temp_engine_state = !m->temp_table_manifest_payload.empty();
  m->has_persistent_engine_state =
      c.basis == Preserve_trx_engine_recovery::REDO_RESURRECTION ||
      c.basis == Preserve_trx_engine_recovery::READ_CONTEXT;
  m->engine_shape = c.basis == Preserve_trx_engine_recovery::NONE
      ? Preserve_snapshot_engine_shape::NONE
      : m->has_persistent_engine_state
          ? (m->has_temp_engine_state ? Preserve_snapshot_engine_shape::MIXED
                                     : Preserve_snapshot_engine_shape::PERSISTENT_ONLY)
          : Preserve_snapshot_engine_shape::TEMP_ONLY;
  return preserve_trx_recovery_payload_valid(*m);
}
