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

/** @file include/trx0preserve.h
 Preserve transaction helpers. */

#ifndef trx0preserve_h
#define trx0preserve_h

#include <cstdint>
#include <string>
#include <vector>

#include "db0err.h"
#include "sql/preserve_trx_xid.h"
#include "storage/innobase/include/lock0warmcopy.h"

class THD;
struct trx_rseg_t;
struct trx_t;

bool trx_preserve_feature_enabled();
bool trx_preserve_xid_is_magic_active(const XID &xid);
bool trx_preserve_xid_should_be_protected(const XID &xid);

struct Preserve_lock_limits {
  uint32_t max_lock_count;
  uint32_t max_modified_tables;
  uint32_t max_scan_pages;
  uint32_t materialize_timeout_ms;
};

struct Preserve_modified_table_name {
  std::string schema_name;
  std::string table_name;
  uint32_t required_write_acls{0};
};

struct trx_preserve_record_lock_import_metrics_t {
  uint64_t record_entries{0};
  uint64_t stable_page_hits{0};
  uint64_t image_resolves{0};
  uint64_t bitmap_pages{0};
  uint64_t bitmap_bits{0};
  uint64_t page_get_us{0};
  uint64_t page_get_count{0};
  uint64_t table_open_us{0};
  uint64_t prefetch_pages{0};
  uint64_t prefetch_bytes{0};
  uint64_t prefetch_residency_pages{0};
  uint64_t prefetch_resident_pages{0};
  uint64_t prefetch_io_pending_pages{0};
  uint64_t prefetch_missing_pages{0};

  void add(const trx_preserve_record_lock_import_metrics_t &other) {
    record_entries += other.record_entries;
    stable_page_hits += other.stable_page_hits;
    image_resolves += other.image_resolves;
    bitmap_pages += other.bitmap_pages;
    bitmap_bits += other.bitmap_bits;
    page_get_us += other.page_get_us;
    page_get_count += other.page_get_count;
    table_open_us += other.table_open_us;
    prefetch_pages += other.prefetch_pages;
    prefetch_bytes += other.prefetch_bytes;
    prefetch_residency_pages += other.prefetch_residency_pages;
    prefetch_resident_pages += other.prefetch_resident_pages;
    prefetch_io_pending_pages += other.prefetch_io_pending_pages;
    prefetch_missing_pages += other.prefetch_missing_pages;
  }
};

struct trx_preserve_record_lock_page_plan_t {
  uint64_t page_count{0};
  uint64_t bitmap_pages{0};
  uint64_t bitmap_bits{0};
};

struct trx_preserve_record_lock_residency_t {
  uint64_t page_count{0};
  uint64_t resident_pages{0};
  uint64_t io_pending_pages{0};
  uint64_t missing_pages{0};
};

trx_t *trx_preserve_claim_prepared(const XID &xid);
bool trx_preserve_probe_detached_prepared(const XID &xid);
trx_t *trx_preserve_current_thd_trx(THD *thd);
uint64_t trx_preserve_current_redo_lsn();
dberr_t trx_preserve_claim_detached_prepared(trx_t *trx);
dberr_t trx_preserve_rollback_by_token(const char *token);
dberr_t trx_preserve_rollback_by_token_for_thd(const char *token, THD *thd);
dberr_t trx_preserve_rollback_claimed(trx_t *trx);
bool trx_preserve_token_has_any_owner(const char *token);
dberr_t trx_preserve_rollback_prepared_without_snapshot(
    const std::vector<std::string> &snapshot_tokens, uint32_t *rolled_back,
    std::vector<std::string> *rolled_back_tokens = nullptr);
dberr_t trx_preserve_prepare_resumed_rollback_gtid(trx_t *trx);
dberr_t trx_preserve_activate_resumed(trx_t *trx);
dberr_t trx_preserve_reactivate_prepared_in_original_thd(THD *thd);
enum class trx_preserve_thd_transition_failure {
  NONE,
  NULL_THD,
  INNODB_HANDLER_UNAVAILABLE,
  NO_TRX,
  NO_XID,
  XID_NOT_PRESERVE_MAGIC,
  NOT_PREPARED,
  NOT_ACTIVE_OR_PREPARED,
  NO_UPDATED_RSEG,
  NOT_IN_MYSQL_TRX_LIST,
  THD_MISMATCH,
  CLAIMED,
  UNDO_ACTIVATE_FAILED,
  RECORD_LOCK_RESTORE_FAILED
};

const char *trx_preserve_thd_transition_failure_name(
    trx_preserve_thd_transition_failure reason);

dberr_t trx_preserve_reactivate_prepare_failure_in_original_thd(
    THD *thd, trx_preserve_thd_transition_failure *reason = nullptr);
dberr_t trx_preserve_reactivate_prepare_failure_in_original_thd(
    THD *thd, const std::string &pre_prepare_record_locks_payload,
    trx_preserve_thd_transition_failure *reason = nullptr);
dberr_t trx_preserve_activate_reattached_in_original_thd(trx_t *trx, THD *thd);
bool trx_preserve_is_active_attached_to_thd(trx_t *trx, THD *thd);
dberr_t trx_preserve_prepare_current_temp_only(THD *thd, const XID &xid);
trx_t *trx_preserve_create_temp_only_claimed(const XID &xid, uint64_t trx_id);
bool trx_preserve_engine_state_facts(const trx_t *trx,
                                     bool *has_persistent_state,
                                     bool *has_temp_state);
uint64_t trx_preserve_trx_id(const trx_t *trx);
void trx_preserve_release_claim_before_free(trx_t *trx);
bool trx_preserve_current_thd_has_read_view(THD *thd);
bool trx_preserve_current_thd_has_record_locks(THD *thd);
bool trx_preserve_current_thd_has_no_redo_undo(THD *thd);
bool trx_preserve_current_thd_no_redo_undo_state(THD *thd, bool *present,
                                                 uint64_t *top_undo_no);
bool trx_preserve_current_thd_has_autoinc_locks(THD *thd);
bool trx_preserve_trx_has_read_view(trx_t *trx);
bool trx_preserve_trx_has_autoinc_locks(trx_t *trx);
uint32_t trx_preserve_modified_table_count(trx_t *trx);
dberr_t trx_preserve_export_modified_table_names(
    trx_t *trx, std::vector<Preserve_modified_table_name> *tables,
    uint32_t max_modified_tables);
dberr_t trx_preserve_export_modified_table_names(
    THD *thd, std::vector<Preserve_modified_table_name> *tables,
    uint32_t max_modified_tables);
void trx_preserve_close_read_views_for_shutdown();
dberr_t trx_preserve_materialize_implicit_locks(
    THD *thd, const Preserve_lock_limits &limits, bool *materialized_any);
dberr_t trx_preserve_export_read_view(THD *thd, std::string *payload,
                                      uint64_t *low_limit_no);
dberr_t trx_preserve_import_read_view(trx_t *trx, const std::string &payload);
dberr_t trx_preserve_debug_replace_current_thd_read_view(
    THD *thd, const std::string &payload);
bool trx_preserve_read_view_payload_is_valid_for_import(
    const std::string &payload);
dberr_t trx_preserve_export_record_locks(trx_t *trx, std::string *payload);
dberr_t trx_preserve_export_record_locks(trx_t *trx, std::string *payload,
                                         uint32_t max_lock_count);
dberr_t trx_preserve_export_record_locks(THD *thd, std::string *payload,
                                         uint32_t max_lock_count);
dberr_t trx_preserve_export_record_locks_stable_page_only(
    THD *thd, std::string *payload, uint32_t max_lock_count);
bool trx_preserve_sample_lock_warmcopy_fence(
    THD *thd, lock_warmcopy_trx_lock_fence_t *fence);
bool trx_preserve_sample_lock_warmcopy_fence(
    trx_t *trx, lock_warmcopy_trx_lock_fence_t *fence);
bool trx_preserve_lock_warmcopy_conversion_freeze(
    THD *thd, lock_warmcopy_trx_lock_fence_t *fence, trx_t **frozen_trx);
void trx_preserve_lock_warmcopy_conversion_thaw(trx_t *trx);
bool trx_preserve_lock_warmcopy_note_conversion_attempt_after_freeze(trx_t *trx);
bool trx_preserve_has_predicate_locks(THD *thd, bool *has_predicate_locks);
const char *trx_preserve_last_record_lock_export_error();
dberr_t trx_preserve_import_record_locks(trx_t *trx,
                                         const std::string &payload);
dberr_t trx_preserve_import_record_locks(
    trx_t *trx, const std::string &payload,
    trx_preserve_record_lock_import_metrics_t *metrics);
dberr_t trx_preserve_import_record_locks(
    trx_t *trx, const std::string &payload,
    trx_preserve_record_lock_import_metrics_t *metrics,
    bool (*deadline_expired)(void *), void *deadline_ctx);
dberr_t trx_preserve_prefetch_record_lock_pages(
    const std::string &payload,
    trx_preserve_record_lock_import_metrics_t *metrics);
dberr_t trx_preserve_prefetch_record_lock_pages_for_gate(
    const std::string &payload,
    trx_preserve_record_lock_import_metrics_t *metrics);
bool trx_preserve_record_locks_payload_is_valid_for_import(
    const std::string &payload);
bool trx_preserve_record_locks_payload_lock_count(
    const std::string &payload, uint32_t *lock_count);
bool trx_preserve_record_lock_payload_page_plan(
    const std::string &payload, trx_preserve_record_lock_page_plan_t *plan);
bool trx_preserve_record_lock_payload_residency(
    const std::string &payload,
    trx_preserve_record_lock_residency_t *residency);
bool trx_preserve_split_record_and_predicate_locks(
    const std::string &payload, std::string *record_locks_payload,
    std::string *predicate_locks_payload);
dberr_t trx_preserve_export_table_locks(trx_t *trx, std::string *payload,
                                        uint32_t max_lock_count,
                                        uint32_t already_used);
dberr_t trx_preserve_export_table_locks(THD *thd, std::string *payload,
                                        uint32_t max_lock_count,
                                        uint32_t already_used);
dberr_t trx_preserve_import_table_locks(trx_t *trx,
                                        const std::string &payload);
bool trx_preserve_table_locks_payload_is_valid_for_import(
    const std::string &payload);
bool trx_preserve_table_locks_payload_lock_count(
    const std::string &payload, uint32_t *lock_count);
bool trx_preserve_table_locks_payload_has_autoinc(const std::string &payload);

struct Preserve_table_lock_import_debug_result {
  dberr_t export_err{DB_ERROR};
  dberr_t import_err{DB_ERROR};
  dberr_t reexport_err{DB_ERROR};
  dberr_t release_err{DB_ERROR};
  bool valid{false};
  bool count_ok{false};
  bool reexport_count_ok{false};
  uint32_t count{0};
  uint32_t reexport_count{0};
};

void trx_preserve_debug_table_lock_import_roundtrip(
    THD *thd, uint32_t max_lock_count,
    Preserve_table_lock_import_debug_result *result);

dberr_t trx_preserve_export_savepoints(trx_t *trx, std::string *payload);
dberr_t trx_preserve_export_savepoints(THD *thd, std::string *payload);
dberr_t trx_preserve_import_savepoints(
    trx_t *trx, const std::string &payload,
    const std::vector<std::string> &savepoint_names);
dberr_t trx_preserve_import_current_thd_savepoints(THD *thd,
                                                  const std::string &payload);
bool trx_preserve_savepoints_payload_is_valid_for_import(
    const std::string &payload, uint32_t *savepoint_count);
dberr_t trx_preserve_set_isolation(trx_t *trx, uint8_t tx_isolation);
dberr_t trx_preserve_set_current_thd_isolation(THD *thd, uint8_t tx_isolation);
bool trx_preserve_thd_can_accept_preserved_trx(THD *thd);
bool trx_preserve_rseg_has_preserved_trx(const trx_rseg_t *rseg);
void trx_preserve_collect_preserved_rsegs(
    std::vector<const trx_rseg_t *> *rsegs);
void trx_preserve_note_rseg_owner_state_change(
    trx_t *trx, int old_state, int new_state);
void trx_preserve_note_rseg_owner_xid_reset(trx_t *trx);
void trx_preserve_note_rseg_owner_xid_restore(trx_t *trx);

struct Preserve_rseg_collection_debug_result {
  bool contains_redo{false};
  bool contains_noredo{false};
  uint32_t count{0};
};

void trx_preserve_debug_current_thd_rseg_collection(
    THD *thd, Preserve_rseg_collection_debug_result *result);

trx_t *trx_preserve_detach_current_thd(
    THD *thd, trx_preserve_thd_transition_failure *reason = nullptr);
dberr_t trx_preserve_attach_to_thd(trx_t *trx, THD *thd);
dberr_t trx_preserve_reattach_preserved_to_original_thd(trx_t *trx, THD *thd);
dberr_t trx_preserve_detach_resumed_from_thd(trx_t *trx, THD *thd);
dberr_t trx_preserve_detach_resumed_from_thd_for_cleanup(trx_t *trx, THD *thd);
void trx_preserve_reset_thd_statement_registration(THD *thd);
void trx_preserve_restore_dml_policy(trx_t *trx, bool foreign_key_checks,
                                     bool unique_checks);

#endif /* trx0preserve_h */
