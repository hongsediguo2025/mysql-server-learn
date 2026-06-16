/*****************************************************************************

Copyright (c) 2026, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is designed to work with certain software (including
but not limited to OpenSSL) that is licensed under separate terms,
as designated in a particular file or component or in included license
documentation.  The authors of MySQL hereby grant you an additional
permission to link the program and your derivative works with the
separately licensed software that they have either included with
the program or referenced in the documentation.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file trx/trx0preserve.cc
 Preserve transaction helper stubs for the 8.0.22 port. */

#include "trx0preserve.h"

#include <algorithm>

#include "lock0lock.h"
#include "read0read.h"
#include "sess0sess.h"
#include "sql/mysqld.h"
#include "sql/sql_class.h"
#include "sql/transaction_info.h"
#include "storage/innobase/handler/ha_innodb.h"
#include "trx0purge.h"
#include "trx0roll.h"
#include "trx0trx.h"
#include "trx0undo.h"
#include "ut0vec.h"

static void trx_preserve_append_le32(std::string *payload, uint32_t value) {
  for (size_t i = 0; i < 4; ++i) {
    payload->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

static bool trx_preserve_read_le32(const std::string &payload, size_t *offset,
                                   uint32_t *value) {
  if (offset == nullptr || value == nullptr || *offset + 4 > payload.size()) {
    return true;
  }

  uint32_t result = 0;
  for (size_t i = 0; i < 4; ++i) {
    result |= static_cast<uint32_t>(
                  static_cast<unsigned char>(payload[*offset + i]))
              << (i * 8);
  }
  *offset += 4;
  *value = result;
  return false;
}

static void trx_preserve_append_le64(std::string *payload, uint64_t value) {
  for (size_t i = 0; i < 8; ++i) {
    payload->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

static bool trx_preserve_read_le64(const std::string &payload, size_t *offset,
                                   uint64_t *value) {
  if (offset == nullptr || value == nullptr || *offset + 8 > payload.size()) {
    return true;
  }

  uint64_t result = 0;
  for (size_t i = 0; i < 8; ++i) {
    result |= static_cast<uint64_t>(
                  static_cast<unsigned char>(payload[*offset + i]))
              << (i * 8);
  }
  *offset += 8;
  *value = result;
  return false;
}

trx_t *trx_preserve_claim_prepared(const XID &xid) {
  (void)xid;
  return nullptr;
}

dberr_t trx_preserve_claim_detached_prepared(trx_t *trx) {
  (void)trx;
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_rollback_by_token(const char *token) {
  (void)token;
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_rollback_by_token_for_thd(const char *token, THD *thd) {
  (void)token;
  (void)thd;
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_rollback_claimed(trx_t *trx) {
  (void)trx;
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_rollback_prepared_without_snapshot(
    const std::vector<std::string> &snapshot_tokens, uint32_t *rolled_back) {
  (void)snapshot_tokens;
  if (rolled_back != nullptr) *rolled_back = 0;
  return DB_SUCCESS;
}

dberr_t trx_preserve_prepare_resumed_rollback_gtid(trx_t *trx) {
  (void)trx;
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_activate_resumed(trx_t *trx) {
  (void)trx;
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_reactivate_prepared_in_original_thd(THD *thd) {
  (void)thd;
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_reactivate_prepare_failure_in_original_thd(THD *thd) {
  (void)thd;
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_activate_reattached_in_original_thd(trx_t *trx,
                                                        THD *thd) {
  (void)trx;
  (void)thd;
  return DB_UNSUPPORTED;
}

bool trx_preserve_is_active_attached_to_thd(trx_t *trx, THD *thd) {
  (void)trx;
  (void)thd;
  return false;
}

dberr_t trx_preserve_prepare_current_temp_only(THD *thd, const XID &xid) {
  (void)thd;
  (void)xid;
  return DB_UNSUPPORTED;
}

trx_t *trx_preserve_create_temp_only_claimed(const XID &xid, uint64_t trx_id) {
  (void)xid;
  (void)trx_id;
  return nullptr;
}

uint64_t trx_preserve_trx_id(const trx_t *trx) {
  return trx != nullptr ? trx->id : 0;
}

void trx_preserve_release_claim_before_free(trx_t *trx) {
  if (trx != nullptr) {
    trx->preserve_trx_claimed = false;
  }
}

bool trx_preserve_current_thd_has_read_view(THD *thd) {
  if (thd == nullptr) {
    return false;
  }

  trx_t *trx = thd_to_trx(thd);
  return trx_preserve_trx_has_read_view(trx);
}

static trx_t *trx_preserve_current_thd_get_trx_if_available(THD *thd) {
  if (thd == nullptr || innodb_hton == nullptr || innodb_hton->slot < 0) {
    return nullptr;
  }

  Ha_data *ha_data = thd->get_ha_data(innodb_hton->slot);
  if (ha_data == nullptr || ha_data->ha_ptr == nullptr) {
    return nullptr;
  }

  return static_cast<innodb_session_t *>(ha_data->ha_ptr)->m_trx;
}

bool trx_preserve_current_thd_has_no_redo_undo(THD *thd) {
  trx_t *trx = trx_preserve_current_thd_get_trx_if_available(thd);
  if (trx == nullptr) {
    return false;
  }

  return trx->rsegs.m_noredo.insert_undo != nullptr ||
         trx->rsegs.m_noredo.update_undo != nullptr;
}

bool trx_preserve_current_thd_no_redo_undo_state(THD *thd, bool *present,
                                                 uint64_t *top_undo_no) {
  if (present != nullptr) *present = false;
  if (top_undo_no != nullptr) *top_undo_no = 0;
  if (thd == nullptr || present == nullptr || top_undo_no == nullptr) {
    return false;
  }

  trx_t *trx = trx_preserve_current_thd_get_trx_if_available(thd);
  if (trx == nullptr || trx->rsegs.m_noredo.rseg == nullptr) {
    return true;
  }

  bool local_present = false;
  uint64_t local_top = 0;
  if (trx->rsegs.m_noredo.insert_undo != nullptr) {
    local_present = true;
    local_top = std::max(
        local_top,
        static_cast<uint64_t>(trx->rsegs.m_noredo.insert_undo->top_undo_no));
  }
  if (trx->rsegs.m_noredo.update_undo != nullptr) {
    local_present = true;
    local_top = std::max(
        local_top,
        static_cast<uint64_t>(trx->rsegs.m_noredo.update_undo->top_undo_no));
  }

  *present = local_present;
  *top_undo_no = local_top;
  return true;
}

bool trx_preserve_current_thd_has_autoinc_locks(THD *thd) {
  trx_t *trx = trx_preserve_current_thd_get_trx_if_available(thd);
  return trx_preserve_trx_has_autoinc_locks(trx);
}

bool trx_preserve_trx_has_read_view(trx_t *trx) {
  return trx != nullptr && MVCC::is_view_active(trx->read_view);
}

bool trx_preserve_trx_has_autoinc_locks(trx_t *trx) {
  if (trx == nullptr) {
    return false;
  }

  trx_mutex_enter(trx);
  const bool has_autoinc_locks = trx->lock.autoinc_locks != nullptr &&
                                 !ib_vector_is_empty(trx->lock.autoinc_locks);
  trx_mutex_exit(trx);

  return has_autoinc_locks;
}

uint32_t trx_preserve_modified_table_count(trx_t *trx) {
  if (trx == nullptr) return 0;
  return static_cast<uint32_t>(trx->mod_tables.size());
}

dberr_t trx_preserve_export_modified_table_names(
    trx_t *trx, std::vector<Preserve_modified_table_name> *tables,
    uint32_t max_modified_tables) {
  if (trx == nullptr || tables == nullptr) return DB_ERROR;

  tables->clear();
  if (trx->mod_tables.size() > max_modified_tables) return DB_UNSUPPORTED;

  tables->reserve(trx->mod_tables.size());
  for (dict_table_t *table : trx->mod_tables) {
    if (table == nullptr) return DB_ERROR;

    Preserve_modified_table_name name;
    table->get_table_name(name.schema_name, name.table_name);
    if (name.schema_name.empty() || name.table_name.empty()) return DB_ERROR;

    tables->push_back(std::move(name));
  }

  return DB_SUCCESS;
}

dberr_t trx_preserve_export_modified_table_names(
    THD *thd, std::vector<Preserve_modified_table_name> *tables,
    uint32_t max_modified_tables) {
  if (thd == nullptr) return DB_ERROR;
  trx_t *trx = thd_to_trx(thd);
  return trx_preserve_export_modified_table_names(trx, tables,
                                                 max_modified_tables);
}

void trx_preserve_close_read_views_for_shutdown() {}

dberr_t trx_preserve_materialize_implicit_locks(
    THD *thd, const Preserve_lock_limits &limits, bool *materialized_any) {
  (void)thd;
  (void)limits;
  if (materialized_any != nullptr) *materialized_any = false;
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_export_read_view(THD *thd, std::string *payload,
                                      uint64_t *low_limit_no) {
  if (thd == nullptr || payload == nullptr || low_limit_no == nullptr) {
    return DB_ERROR;
  }

  if (payload != nullptr) payload->clear();
  if (low_limit_no != nullptr) *low_limit_no = 0;

  trx_t *trx = thd_to_trx(thd);
  if (trx == nullptr || !MVCC::is_view_active(trx->read_view)) {
    return DB_SUCCESS;
  }

  Preserve_read_view_snapshot snapshot;
  DBUG_EXECUTE_IF("preserve_trx_fail_export_read_view", return DB_ERROR;);
  if (!MVCC::preserve_export_view(trx->read_view, &snapshot)) {
    return DB_ERROR;
  }

  trx_preserve_append_le64(payload, snapshot.low_limit_id);
  trx_preserve_append_le64(payload, snapshot.up_limit_id);
  trx_preserve_append_le64(payload, snapshot.creator_trx_id);
  trx_preserve_append_le64(payload, snapshot.low_limit_no);
  trx_preserve_append_le64(payload, snapshot.ids.size());
  for (trx_id_t id : snapshot.ids) {
    trx_preserve_append_le64(payload, id);
  }

  *low_limit_no = snapshot.low_limit_no;
  return DB_SUCCESS;
}

static bool trx_preserve_parse_read_view_payload(
    const std::string &payload, Preserve_read_view_snapshot *snapshot) {
  if (snapshot == nullptr || payload.size() < 40 || payload.size() % 8 != 0) {
    return false;
  }

  uint64_t count = 0;
  size_t offset = 0;

  snapshot->ids.clear();
  if (trx_preserve_read_le64(payload, &offset, &snapshot->low_limit_id) ||
      trx_preserve_read_le64(payload, &offset, &snapshot->up_limit_id) ||
      trx_preserve_read_le64(payload, &offset, &snapshot->creator_trx_id) ||
      trx_preserve_read_le64(payload, &offset, &snapshot->low_limit_no) ||
      trx_preserve_read_le64(payload, &offset, &count)) {
    return false;
  }

  if (count != static_cast<uint64_t>((payload.size() - offset) / 8)) {
    return false;
  }
  if (snapshot->low_limit_no == 0) {
    return false;
  }

  snapshot->ids.reserve(static_cast<size_t>(count));
  for (uint64_t i = 0; i < count; ++i) {
    uint64_t id = 0;
    if (trx_preserve_read_le64(payload, &offset, &id)) {
      return false;
    }
    snapshot->ids.push_back(id);
  }

  return offset == payload.size();
}

dberr_t trx_preserve_import_read_view(trx_t *trx, const std::string &payload) {
  if (payload.empty()) {
    return DB_SUCCESS;
  }
  DBUG_EXECUTE_IF("preserve_trx_fail_import_read_view",
                  return DB_OUT_OF_MEMORY;);
  if (trx == nullptr || trx_sys == nullptr || trx_sys->mvcc == nullptr) {
    return DB_ERROR;
  }

  Preserve_read_view_snapshot snapshot;
  if (!trx_preserve_parse_read_view_payload(payload, &snapshot)) {
    return DB_ERROR;
  }
  const purge_state_t purge_state = trx_purge_state();
  if (purge_state != PURGE_STATE_INIT && purge_state != PURGE_STATE_DISABLED) {
    return DB_ERROR;
  }
  const trx_id_t next_trx_id = trx_sys_get_max_trx_id();
  if (snapshot.low_limit_no > next_trx_id ||
      snapshot.low_limit_id > next_trx_id) {
    return DB_ERROR;
  }

  return trx_sys->mvcc->preserve_import_view(trx->read_view, snapshot, trx);
}

dberr_t trx_preserve_debug_replace_current_thd_read_view(
    THD *thd, const std::string &payload) {
  if (thd == nullptr) return DB_ERROR;
  trx_t *trx = thd_to_trx(thd);
  if (trx == nullptr || trx_sys == nullptr || trx_sys->mvcc == nullptr) {
    return DB_ERROR;
  }

  const purge_state_t purge_state = trx_purge_state();
  if (purge_state != PURGE_STATE_INIT && purge_state != PURGE_STATE_DISABLED) {
    return DB_ERROR;
  }

  if (MVCC::is_view_active(trx->read_view)) {
    trx_sys_mutex_enter();
    trx_sys->mvcc->view_close(trx->read_view, true);
    trx_sys_mutex_exit();
  }

  return trx_preserve_import_read_view(trx, payload);
}

bool trx_preserve_read_view_payload_is_valid_for_import(
    const std::string &payload) {
  if (payload.empty()) {
    return true;
  }

  Preserve_read_view_snapshot snapshot;
  return trx_preserve_parse_read_view_payload(payload, &snapshot);
}

dberr_t trx_preserve_export_record_locks(trx_t *trx, std::string *payload) {
  return trx_preserve_export_record_locks(trx, payload, 0);
}

dberr_t trx_preserve_export_record_locks(trx_t *trx, std::string *payload,
                                         uint32_t max_lock_count) {
  (void)trx;
  (void)max_lock_count;
  if (payload != nullptr) payload->clear();
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_export_record_locks(THD *thd, std::string *payload,
                                         uint32_t max_lock_count) {
  (void)thd;
  (void)max_lock_count;
  if (payload != nullptr) payload->clear();
  return DB_UNSUPPORTED;
}

const char *trx_preserve_last_record_lock_export_error() {
  return "trx0preserve not yet ported";
}

dberr_t trx_preserve_import_record_locks(trx_t *trx,
                                         const std::string &payload) {
  (void)trx;
  (void)payload;
  return DB_UNSUPPORTED;
}

bool trx_preserve_record_locks_payload_is_valid_for_import(
    const std::string &payload) {
  (void)payload;
  return false;
}

bool trx_preserve_record_locks_payload_lock_count(
    const std::string &payload, uint32_t *lock_count) {
  (void)payload;
  if (lock_count != nullptr) *lock_count = 0;
  return false;
}

bool trx_preserve_split_record_and_predicate_locks(
    const std::string &payload, std::string *record_locks_payload,
    std::string *predicate_locks_payload) {
  (void)payload;
  if (record_locks_payload != nullptr) record_locks_payload->clear();
  if (predicate_locks_payload != nullptr) predicate_locks_payload->clear();
  return false;
}

dberr_t trx_preserve_export_table_locks(trx_t *trx, std::string *payload,
                                        uint32_t max_lock_count,
                                        uint32_t already_used) {
  return lock_preserve_export_table_locks(trx, payload, max_lock_count,
                                          already_used);
}

dberr_t trx_preserve_export_table_locks(THD *thd, std::string *payload,
                                        uint32_t max_lock_count,
                                        uint32_t already_used) {
  if (thd == nullptr) {
    if (payload != nullptr) payload->clear();
    return DB_ERROR;
  }
  return trx_preserve_export_table_locks(thd_to_trx(thd), payload,
                                         max_lock_count, already_used);
}

dberr_t trx_preserve_import_table_locks(trx_t *trx,
                                        const std::string &payload) {
  (void)trx;
  (void)payload;
  return DB_UNSUPPORTED;
}

bool trx_preserve_table_locks_payload_is_valid_for_import(
    const std::string &payload) {
  return lock_preserve_table_locks_payload_is_valid_for_import(payload);
}

bool trx_preserve_table_locks_payload_lock_count(
    const std::string &payload, uint32_t *lock_count) {
  return lock_preserve_table_locks_payload_lock_count(payload, lock_count);
}

bool trx_preserve_table_locks_payload_has_autoinc(const std::string &payload) {
  return lock_preserve_table_locks_payload_has_autoinc(payload);
}

dberr_t trx_preserve_export_savepoints(trx_t *trx, std::string *payload) {
  if (trx == nullptr || payload == nullptr) {
    return DB_ERROR;
  }

  payload->clear();
  if (trx->fts_trx != nullptr) {
    return DB_ERROR;
  }

  const uint32_t count =
      static_cast<uint32_t>(UT_LIST_GET_LEN(trx->trx_savepoints));
  if (count == 0) {
    return DB_SUCCESS;
  }

  trx_preserve_append_le32(payload, count);
  for (trx_named_savept_t *savep = UT_LIST_GET_FIRST(trx->trx_savepoints);
       savep != nullptr; savep = UT_LIST_GET_NEXT(trx_savepoints, savep)) {
    trx_preserve_append_le64(payload, savep->savept.least_undo_no);
    trx_preserve_append_le64(
        payload, static_cast<uint64_t>(savep->mysql_binlog_cache_pos));
  }

  return DB_SUCCESS;
}

dberr_t trx_preserve_export_savepoints(THD *thd, std::string *payload) {
  if (thd == nullptr) return DB_ERROR;
  trx_t *trx = thd_to_trx(thd);
  return trx_preserve_export_savepoints(trx, payload);
}

static void trx_preserve_clear_savepoints(trx_t *trx) {
  trx_roll_savepoints_free(trx, UT_LIST_GET_FIRST(trx->trx_savepoints));
}

static void trx_preserve_free_savepoint(trx_named_savept_t *savep) {
  if (savep == nullptr) return;
  ut_free(savep->name);
  ut_free(savep);
}

static void trx_preserve_free_savepoint_vector(
    std::vector<trx_named_savept_t *> *savepoints) {
  for (trx_named_savept_t *savep : *savepoints) {
    trx_preserve_free_savepoint(savep);
  }
  savepoints->clear();
}

dberr_t trx_preserve_import_savepoints(
    trx_t *trx, const std::string &payload,
    const std::vector<std::string> &savepoint_names) {
  uint32_t count = 0;
  if (trx == nullptr ||
      !trx_preserve_savepoints_payload_is_valid_for_import(payload, &count) ||
      count != savepoint_names.size()) {
    return DB_ERROR;
  }
  if (count != 0 && trx->fts_trx != nullptr) {
    return DB_ERROR;
  }

  if (count == 0) {
    trx_preserve_clear_savepoints(trx);
    return DB_SUCCESS;
  }

  std::vector<trx_named_savept_t *> imported_savepoints;
  imported_savepoints.reserve(count);

  size_t offset = 4;
  for (uint32_t i = 0; i < count; ++i) {
    uint64_t undo_no = 0;
    uint64_t binlog_pos = 0;
    if (trx_preserve_read_le64(payload, &offset, &undo_no) ||
        trx_preserve_read_le64(payload, &offset, &binlog_pos) ||
        undo_no > trx->undo_no || savepoint_names[i].empty()) {
      trx_preserve_free_savepoint_vector(&imported_savepoints);
      return DB_ERROR;
    }

    trx_named_savept_t *savep =
        static_cast<trx_named_savept_t *>(ut_malloc_nokey(sizeof(*savep)));
    if (savep == nullptr) {
      trx_preserve_free_savepoint_vector(&imported_savepoints);
      return DB_OUT_OF_MEMORY;
    }

    savep->name = mem_strdup(savepoint_names[i].c_str());
    if (savep->name == nullptr) {
      trx_preserve_free_savepoint(savep);
      trx_preserve_free_savepoint_vector(&imported_savepoints);
      return DB_OUT_OF_MEMORY;
    }

    savep->savept.least_undo_no = static_cast<undo_no_t>(undo_no);
    savep->mysql_binlog_cache_pos = static_cast<int64_t>(binlog_pos);
    imported_savepoints.push_back(savep);
  }

  if (offset != payload.size()) {
    trx_preserve_free_savepoint_vector(&imported_savepoints);
    return DB_ERROR;
  }

  trx_preserve_clear_savepoints(trx);
  for (trx_named_savept_t *savep : imported_savepoints) {
    UT_LIST_ADD_LAST(trx->trx_savepoints, savep);
  }
  return DB_SUCCESS;
}

dberr_t trx_preserve_import_current_thd_savepoints(THD *thd,
                                                  const std::string &payload) {
  if (thd == nullptr) return DB_ERROR;

  std::vector<std::string> savepoint_names;
  for (SAVEPOINT *sv = thd->get_transaction()->m_savepoints; sv != nullptr;
       sv = sv->prev) {
    char name[64];
    const void *engine_savepoint =
        reinterpret_cast<const unsigned char *>(sv + 1) +
        innodb_hton->savepoint_offset;
    longlong2str(reinterpret_cast<ulint>(engine_savepoint), name, 36);
    savepoint_names.push_back(name);
  }
  std::reverse(savepoint_names.begin(), savepoint_names.end());

  return trx_preserve_import_savepoints(thd_to_trx(thd), payload,
                                        savepoint_names);
}

bool trx_preserve_savepoints_payload_is_valid_for_import(
    const std::string &payload, uint32_t *savepoint_count) {
  if (savepoint_count != nullptr) *savepoint_count = 0;
  if (payload.empty()) return true;
  if (payload.size() < 4) return false;

  size_t offset = 0;
  uint32_t count = 0;
  if (trx_preserve_read_le32(payload, &offset, &count)) return false;
  if (count == 0 || payload.size() - offset != static_cast<size_t>(count) * 16)
    return false;

  uint64_t previous_undo_no = 0;
  for (uint32_t i = 0; i < count; ++i) {
    uint64_t undo_no = 0;
    uint64_t binlog_pos = 0;
    if (trx_preserve_read_le64(payload, &offset, &undo_no) ||
        trx_preserve_read_le64(payload, &offset, &binlog_pos)) {
      return false;
    }
    if (i > 0 && undo_no < previous_undo_no) return false;
    previous_undo_no = undo_no;
  }

  if (offset != payload.size()) return false;
  if (savepoint_count != nullptr) *savepoint_count = count;
  return true;
}

dberr_t trx_preserve_set_isolation(trx_t *trx, uint8_t tx_isolation) {
  if (trx == nullptr) return DB_ERROR;

  switch (static_cast<enum_tx_isolation>(tx_isolation)) {
    case ISO_READ_UNCOMMITTED:
    case ISO_READ_COMMITTED:
    case ISO_REPEATABLE_READ:
    case ISO_SERIALIZABLE:
      trx->isolation_level = innobase_trx_map_isolation_level(
          static_cast<enum_tx_isolation>(tx_isolation));
      return DB_SUCCESS;
  }

  return DB_ERROR;
}

dberr_t trx_preserve_set_current_thd_isolation(THD *thd,
                                               uint8_t tx_isolation) {
  if (thd == nullptr) return DB_ERROR;
  return trx_preserve_set_isolation(thd_to_trx(thd), tx_isolation);
}

bool trx_preserve_thd_can_accept_preserved_trx(THD *thd) {
  if (thd == nullptr || innodb_hton == nullptr ||
      innodb_hton->replace_native_transaction_in_thd == nullptr) {
    return false;
  }

  trx_t *trx = thd_to_trx(thd);
  return trx == nullptr || trx_state_eq(trx, TRX_STATE_NOT_STARTED);
}

bool trx_preserve_rseg_has_preserved_trx(const trx_rseg_t *rseg) {
  (void)rseg;
  return false;
}

void trx_preserve_collect_preserved_rsegs(
    std::vector<const trx_rseg_t *> *rsegs) {
  if (rsegs != nullptr) rsegs->clear();
}

trx_t *trx_preserve_detach_current_thd(THD *thd) {
  (void)thd;
  return nullptr;
}

dberr_t trx_preserve_attach_to_thd(trx_t *trx, THD *thd) {
  (void)trx;
  (void)thd;
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_reattach_preserved_to_original_thd(trx_t *trx, THD *thd) {
  (void)trx;
  (void)thd;
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_detach_resumed_from_thd(trx_t *trx, THD *thd) {
  (void)trx;
  (void)thd;
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_detach_resumed_from_thd_for_cleanup(trx_t *trx,
                                                        THD *thd) {
  (void)trx;
  (void)thd;
  return DB_UNSUPPORTED;
}

void trx_preserve_reset_thd_statement_registration(THD *thd) { (void)thd; }
