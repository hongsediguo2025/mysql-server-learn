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

#include "trx0trx.h"

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

void trx_preserve_release_claim_before_free(trx_t *trx) { (void)trx; }

bool trx_preserve_current_thd_has_read_view(THD *thd) {
  (void)thd;
  return false;
}

bool trx_preserve_current_thd_has_no_redo_undo(THD *thd) {
  (void)thd;
  return false;
}

bool trx_preserve_current_thd_no_redo_undo_state(THD *thd, bool *present,
                                                 uint64_t *top_undo_no) {
  (void)thd;
  if (present != nullptr) *present = false;
  if (top_undo_no != nullptr) *top_undo_no = 0;
  return false;
}

bool trx_preserve_trx_has_read_view(trx_t *trx) {
  (void)trx;
  return false;
}

bool trx_preserve_trx_has_autoinc_locks(trx_t *trx) {
  (void)trx;
  return false;
}

uint32_t trx_preserve_modified_table_count(trx_t *trx) {
  (void)trx;
  return 0;
}

dberr_t trx_preserve_export_modified_table_names(
    trx_t *trx, std::vector<Preserve_modified_table_name> *tables,
    uint32_t max_modified_tables) {
  (void)trx;
  (void)max_modified_tables;
  if (tables != nullptr) tables->clear();
  return DB_SUCCESS;
}

dberr_t trx_preserve_export_modified_table_names(
    THD *thd, std::vector<Preserve_modified_table_name> *tables,
    uint32_t max_modified_tables) {
  (void)thd;
  (void)max_modified_tables;
  if (tables != nullptr) tables->clear();
  return DB_SUCCESS;
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
  (void)thd;
  if (payload != nullptr) payload->clear();
  if (low_limit_no != nullptr) *low_limit_no = 0;
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_import_read_view(trx_t *trx, const std::string &payload) {
  (void)trx;
  (void)payload;
  return DB_UNSUPPORTED;
}

bool trx_preserve_read_view_payload_is_valid_for_import(
    const std::string &payload) {
  (void)payload;
  return false;
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
  (void)trx;
  (void)max_lock_count;
  (void)already_used;
  if (payload != nullptr) payload->clear();
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_export_table_locks(THD *thd, std::string *payload,
                                        uint32_t max_lock_count,
                                        uint32_t already_used) {
  (void)thd;
  (void)max_lock_count;
  (void)already_used;
  if (payload != nullptr) payload->clear();
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_import_table_locks(trx_t *trx,
                                        const std::string &payload) {
  (void)trx;
  (void)payload;
  return DB_UNSUPPORTED;
}

bool trx_preserve_table_locks_payload_is_valid_for_import(
    const std::string &payload) {
  (void)payload;
  return false;
}

bool trx_preserve_table_locks_payload_lock_count(
    const std::string &payload, uint32_t *lock_count) {
  (void)payload;
  if (lock_count != nullptr) *lock_count = 0;
  return false;
}

bool trx_preserve_table_locks_payload_has_autoinc(const std::string &payload) {
  (void)payload;
  return false;
}

dberr_t trx_preserve_export_savepoints(trx_t *trx, std::string *payload) {
  (void)trx;
  if (payload != nullptr) payload->clear();
  return DB_UNSUPPORTED;
}

dberr_t trx_preserve_import_savepoints(
    trx_t *trx, const std::string &payload,
    const std::vector<std::string> &savepoint_names) {
  (void)trx;
  (void)payload;
  (void)savepoint_names;
  return DB_UNSUPPORTED;
}

bool trx_preserve_savepoints_payload_is_valid_for_import(
    const std::string &payload, uint32_t *savepoint_count) {
  (void)payload;
  if (savepoint_count != nullptr) *savepoint_count = 0;
  return false;
}

dberr_t trx_preserve_set_isolation(trx_t *trx, uint8_t tx_isolation) {
  (void)trx;
  (void)tx_isolation;
  return DB_UNSUPPORTED;
}

bool trx_preserve_thd_can_accept_preserved_trx(THD *thd) {
  (void)thd;
  return false;
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
