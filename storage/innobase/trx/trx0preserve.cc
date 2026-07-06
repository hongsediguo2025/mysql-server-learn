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
 Preserve transaction bridge helpers between SQL and InnoDB state. */

#include "trx0preserve.h"

#include <algorithm>
#include <atomic>
#include <cstring>

#include "log0log.h"
#include "lock0lock.h"
#include "lock0warmcopy.h"
#include "my_dbug.h"
#include "read0read.h"
#include "sess0sess.h"
#include "sql/handler.h"
#include "sql/mysqld.h"
#include "sql/preserve_trx.h"
#include "sql/sql_class.h"
#include "sql/transaction_info.h"
#include "storage/innobase/handler/ha_innodb.h"
#include "trx0purge.h"
#include "trx0roll.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "trx0undo.h"
#include "ut0vec.h"

static bool trx_preserve_token_to_xid(const char *token, XID *xid) {
  if (token == nullptr || xid == nullptr) {
    return false;
  }

  const size_t token_length = std::strlen(token);
  if (token_length == 0 || token_length > PRESERVE_TRX_TOKEN_MAX_LENGTH ||
      token_length >
          XIDDATASIZE - static_cast<size_t>(PRESERVE_TRX_XID_GTRID_LENGTH)) {
    return false;
  }

  xid->set(PRESERVE_TRX_XID_FORMAT_ID, PRESERVE_TRX_XID_GTRID,
           PRESERVE_TRX_XID_GTRID_LENGTH, token,
           static_cast<long>(token_length));
  return true;
}

bool trx_preserve_feature_enabled() { return preserve_trx_is_enabled(); }

bool trx_preserve_xid_should_be_protected(const XID &xid) {
  return preserve_trx_magic_xid_should_be_protected(xid);
}

bool trx_preserve_xid_is_magic_active(const XID &xid) {
  return trx_preserve_xid_should_be_protected(xid);
}

static std::atomic<uint32_t> trx_preserve_active_preserved_rseg_owners{0};

static bool trx_preserve_rseg_owner_counts(const trx_t *trx, int state) {
  return trx != nullptr &&
         (state == TRX_STATE_PREPARED || state == TRX_STATE_PRESERVED) &&
         trx->xid != nullptr && trx_preserve_xid_is_magic_active(*trx->xid);
}

static void trx_preserve_active_preserved_rseg_owners_inc() {
  trx_preserve_active_preserved_rseg_owners.fetch_add(
      1, std::memory_order_release);
}

static void trx_preserve_active_preserved_rseg_owners_dec() {
  uint32_t current = trx_preserve_active_preserved_rseg_owners.load(
      std::memory_order_acquire);
  while (current != 0 &&
         !trx_preserve_active_preserved_rseg_owners.compare_exchange_weak(
             current, current - 1, std::memory_order_acq_rel,
             std::memory_order_acquire)) {
  }
}

void trx_preserve_note_rseg_owner_state_change(trx_t *trx,
                                               int old_state,
                                               int new_state) {
  const bool old_counted = trx_preserve_rseg_owner_counts(trx, old_state);
  const bool new_counted = trx_preserve_rseg_owner_counts(trx, new_state);
  if (!old_counted && new_counted) {
    trx_preserve_active_preserved_rseg_owners_inc();
  } else if (old_counted && !new_counted) {
    trx_preserve_active_preserved_rseg_owners_dec();
  }
}

void trx_preserve_note_rseg_owner_xid_reset(trx_t *trx) {
  if (trx_preserve_rseg_owner_counts(trx, trx != nullptr ? trx->state
                                                         : TRX_STATE_NOT_STARTED)) {
    trx_preserve_active_preserved_rseg_owners_dec();
  }
}

void trx_preserve_note_rseg_owner_xid_restore(trx_t *trx) {
  if (trx_preserve_rseg_owner_counts(trx, trx != nullptr ? trx->state
                                                         : TRX_STATE_NOT_STARTED)) {
    trx_preserve_active_preserved_rseg_owners_inc();
  }
}

trx_t *trx_preserve_current_thd_trx(THD *thd) {
  return thd == nullptr ? nullptr : thd_to_trx(thd);
}

uint64_t trx_preserve_current_redo_lsn() {
  if (log_sys == nullptr) return 0;
  return static_cast<uint64_t>(log_get_lsn(*log_sys));
}

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

static void trx_preserve_store_state_trx_sys_locked(trx_t *trx,
                                                    trx_state_t state) {
  ut_ad(trx != nullptr);
  ut_ad(trx_sys_mutex_own());
  const trx_state_t old_state = trx->state;
#ifdef UNIV_DEBUG
  ut_ad(old_state == TRX_STATE_ACTIVE || old_state == TRX_STATE_PREPARED ||
        old_state == TRX_STATE_PRESERVED);
  ut_ad(state == TRX_STATE_ACTIVE || state == TRX_STATE_PREPARED ||
        state == TRX_STATE_PRESERVED);
  ut_ad(trx->in_rw_trx_list || trx->in_mysql_trx_list ||
        trx->preserve_trx_claimed || trx->is_recovered ||
        trx->mysql_thd != nullptr);
#endif /* UNIV_DEBUG */

  /*
    Preserve/resume changes only ACTIVE/PREPARED/PRESERVED list-ownership
    states. Keep these transitions under trx_sys_mutex, matching InnoDB's
    ACTIVE->PREPARED path and the list/n_prepared_trx bookkeeping updated
    beside these stores.
  */
  trx->state = state;
  trx_preserve_note_rseg_owner_state_change(trx, old_state, state);
}

static void trx_preserve_store_private_state(trx_t *trx, trx_state_t state) {
  ut_ad(trx != nullptr);
  const trx_state_t old_state = trx->state;
#ifdef UNIV_DEBUG
  ut_ad(!trx->in_rw_trx_list);
  ut_ad(!trx->in_mysql_trx_list);
#endif /* UNIV_DEBUG */

  trx->state = state;
  trx_preserve_note_rseg_owner_state_change(trx, old_state, state);
}

static dberr_t trx_preserve_mark_preserved(trx_t *trx) {
  ut_ad(trx_sys_mutex_own());

  if (trx == nullptr || trx->xid == nullptr ||
      !xid_is_preserve_magic(*trx->xid)) {
    return DB_ERROR;
  }

  if (trx_state_eq(trx, TRX_STATE_PRESERVED)) {
    return DB_SUCCESS;
  }

  if (!trx_state_eq(trx, TRX_STATE_PREPARED)) {
    return DB_ERROR;
  }

  ut_a(trx_sys->n_prepared_trx > 0);
  --trx_sys->n_prepared_trx;
  trx_preserve_store_state_trx_sys_locked(trx, TRX_STATE_PRESERVED);

  return DB_SUCCESS;
}

static bool trx_preserve_state_allows_token_rollback(trx_state_t state) {
  return state == TRX_STATE_PREPARED || state == TRX_STATE_PRESERVED;
}

enum class trx_preserve_rollback_scope {
  DETACHED_ONLY,
  OWNER_ONLY,
  DETACHED_OR_OWNER
};

static bool trx_preserve_rollback_owner_matches(const trx_t *trx,
                                                const THD *owner_thd,
                                                trx_preserve_rollback_scope
                                                    scope) {
  switch (scope) {
    case trx_preserve_rollback_scope::DETACHED_ONLY:
      return trx->mysql_thd == nullptr;
    case trx_preserve_rollback_scope::OWNER_ONLY:
      return trx->mysql_thd == owner_thd;
    case trx_preserve_rollback_scope::DETACHED_OR_OWNER:
      return trx->mysql_thd == nullptr ||
             (owner_thd != nullptr && trx->mysql_thd == owner_thd);
  }

  ut_error;
}

static trx_t *trx_preserve_find_for_token_rollback(
    const XID &xid, const THD *owner_thd, trx_preserve_rollback_scope scope,
    XID *claimed_xid) {
  ut_ad(trx_sys_mutex_own());
  ut_ad(claimed_xid != nullptr);

  for (trx_t *trx = UT_LIST_GET_FIRST(trx_sys->rw_trx_list); trx != nullptr;
       trx = UT_LIST_GET_NEXT(trx_list, trx)) {
    assert_trx_in_rw_list(trx);

    if (trx->xid == nullptr ||
        !trx_preserve_rollback_owner_matches(trx, owner_thd, scope) ||
        !xid.eq(trx->xid)) {
      continue;
    }

    if (!trx_preserve_state_allows_token_rollback(trx->state) ||
        trx->preserve_trx_claimed) {
      continue;
    }

    if (trx_state_eq(trx, TRX_STATE_PREPARED) &&
        trx_preserve_mark_preserved(trx) != DB_SUCCESS) {
      return nullptr;
    }

    *claimed_xid = *trx->xid;
    trx_preserve_note_rseg_owner_xid_reset(trx);
    trx->xid->reset();
    return trx;
  }

  return nullptr;
}

static void trx_preserve_release_token_rollback_claim(trx_t *trx,
                                                      const XID &xid) {
  trx_sys_mutex_enter();
  if (trx->xid->is_null()) {
    *trx->xid = xid;
    trx_preserve_note_rseg_owner_xid_restore(trx);
  }
  trx_sys_mutex_exit();
}

trx_t *trx_preserve_claim_prepared(const XID &xid) {
  if (!xid_is_preserve_magic(xid)) {
    return nullptr;
  }

  trx_t *claimed = nullptr;

  trx_sys_mutex_enter();

  for (trx_t *trx = UT_LIST_GET_FIRST(trx_sys->rw_trx_list); trx != nullptr;
       trx = UT_LIST_GET_NEXT(trx_list, trx)) {
    assert_trx_in_rw_list(trx);

    if (trx->mysql_thd == nullptr && !trx->preserve_trx_claimed &&
        trx_state_eq(trx, TRX_STATE_PREPARED) && xid.eq(trx->xid) &&
        trx_preserve_mark_preserved(trx) == DB_SUCCESS) {
      trx->preserve_trx_claimed = true;
      claimed = trx;
      break;
    }
  }

  trx_sys_mutex_exit();

  return claimed;
}

bool trx_preserve_probe_detached_prepared(const XID &xid) {
  if (!xid_is_preserve_magic(xid) || trx_sys == nullptr) return false;

  bool found = false;

  trx_sys_mutex_enter();

  for (trx_t *trx = UT_LIST_GET_FIRST(trx_sys->rw_trx_list); trx != nullptr;
       trx = UT_LIST_GET_NEXT(trx_list, trx)) {
    assert_trx_in_rw_list(trx);

    if (trx->mysql_thd == nullptr && !trx->preserve_trx_claimed &&
        trx_state_eq(trx, TRX_STATE_PREPARED) && xid.eq(trx->xid)) {
      found = true;
      break;
    }
  }

  trx_sys_mutex_exit();

  return found;
}

dberr_t trx_preserve_claim_detached_prepared(trx_t *trx) {
  dberr_t err;

  trx_sys_mutex_enter();
  if (trx == nullptr || trx->mysql_thd != nullptr ||
      trx->preserve_trx_claimed || !trx_state_eq(trx, TRX_STATE_PREPARED)) {
    err = DB_ERROR;
  } else if (trx_preserve_mark_preserved(trx) != DB_SUCCESS) {
    err = DB_ERROR;
  } else {
    trx->preserve_trx_claimed = true;
    err = DB_SUCCESS;
  }
  trx_sys_mutex_exit();

  return err;
}

dberr_t trx_preserve_rollback_by_token(const char *token) {
  XID xid;

  if (!trx_preserve_token_to_xid(token, &xid)) {
    return DB_ERROR;
  }

  if (trx_sys == nullptr) {
    return DB_NOT_FOUND;
  }

  XID claimed_xid;
  trx_t *trx;

  trx_sys_mutex_enter();
  trx = trx_preserve_find_for_token_rollback(
      xid, nullptr, trx_preserve_rollback_scope::DETACHED_ONLY, &claimed_xid);
  trx_sys_mutex_exit();

  if (trx == nullptr) {
    return DB_NOT_FOUND;
  }

  const dberr_t err = trx_rollback_for_mysql(trx);

  if (err == DB_SUCCESS) {
    trx->is_registered = false;
    trx_free_for_background(trx);
  } else {
    trx_preserve_release_token_rollback_claim(trx, claimed_xid);
  }

  return err;
}

dberr_t trx_preserve_rollback_by_token_for_thd(const char *token, THD *thd) {
  if (thd == nullptr) {
    return DB_ERROR;
  }

  XID xid;

  if (!trx_preserve_token_to_xid(token, &xid)) {
    return DB_ERROR;
  }

  if (trx_sys == nullptr) {
    return DB_NOT_FOUND;
  }

  XID claimed_xid;
  trx_t *trx;

  trx_sys_mutex_enter();
  trx = trx_preserve_find_for_token_rollback(
      xid, thd, trx_preserve_rollback_scope::DETACHED_OR_OWNER, &claimed_xid);
  trx_sys_mutex_exit();

  if (trx == nullptr) {
    return DB_NOT_FOUND;
  }

  const dberr_t err = trx_rollback_for_mysql(trx);

  if (err == DB_SUCCESS) {
    trx->is_registered = false;
    if (trx->mysql_thd == nullptr) {
      trx_free_for_background(trx);
    }
  } else {
    trx_preserve_release_token_rollback_claim(trx, claimed_xid);
  }

  return err;
}

dberr_t trx_preserve_rollback_claimed(trx_t *trx) {
  if (trx == nullptr) {
    return DB_ERROR;
  }

  XID claimed_xid;
  bool claimed = false;

  trx_sys_mutex_enter();
  if (trx->mysql_thd == nullptr && trx->preserve_trx_claimed &&
      trx->xid != nullptr && xid_is_preserve_magic(*trx->xid) &&
      trx_preserve_state_allows_token_rollback(trx->state)) {
    if (trx_state_eq(trx, TRX_STATE_PREPARED) &&
        trx_preserve_mark_preserved(trx) != DB_SUCCESS) {
      trx_sys_mutex_exit();
      return DB_ERROR;
    }
    claimed_xid = *trx->xid;
    trx_preserve_note_rseg_owner_xid_reset(trx);
    trx->xid->reset();
    trx->preserve_trx_claimed = false;
    claimed = true;
  }
  trx_sys_mutex_exit();

  if (!claimed) {
    return DB_ERROR;
  }

  const dberr_t err = trx_rollback_for_mysql(trx);

  if (err == DB_SUCCESS) {
    trx->is_registered = false;
    trx_free_for_background(trx);
  } else {
    trx_sys_mutex_enter();
    if (trx->xid->is_null()) {
      *trx->xid = claimed_xid;
      trx_preserve_note_rseg_owner_xid_restore(trx);
      trx->preserve_trx_claimed = true;
    }
    trx_sys_mutex_exit();
  }

  return err;
}

bool trx_preserve_token_has_any_owner(const char *token) {
  XID xid;

  if (!trx_preserve_token_to_xid(token, &xid) || trx_sys == nullptr) {
    return false;
  }

  bool found = false;
  trx_sys_mutex_enter();

  for (trx_t *trx = UT_LIST_GET_FIRST(trx_sys->rw_trx_list); trx != nullptr;
       trx = UT_LIST_GET_NEXT(trx_list, trx)) {
    assert_trx_in_rw_list(trx);

    if (trx->xid != nullptr && xid.eq(trx->xid) &&
        trx_preserve_state_allows_token_rollback(trx->state)) {
      found = true;
      break;
    }
  }

  trx_sys_mutex_exit();
  return found;
}

static bool trx_preserve_xid_token_in(
    const XID &xid, const std::vector<std::string> &tokens) {
  if (!xid_is_preserve_magic(xid)) return false;

  const char *token =
      xid.get_data() + static_cast<size_t>(PRESERVE_TRX_XID_GTRID_LENGTH);
  const size_t token_length = static_cast<size_t>(xid.get_bqual_length());

  for (const std::string &snapshot_token : tokens) {
    if (snapshot_token.length() == token_length &&
        std::memcmp(snapshot_token.c_str(), token, token_length) == 0) {
      return true;
    }
  }

  return false;
}

dberr_t trx_preserve_rollback_prepared_without_snapshot(
    const std::vector<std::string> &snapshot_tokens, uint32_t *rolled_back) {
  if (rolled_back != nullptr) *rolled_back = 0;
  if (trx_sys == nullptr) return DB_SUCCESS;

  for (;;) {
    trx_t *trx = nullptr;

    trx_sys_mutex_enter();
    for (trx_t *candidate = UT_LIST_GET_FIRST(trx_sys->rw_trx_list);
         candidate != nullptr;
         candidate = UT_LIST_GET_NEXT(trx_list, candidate)) {
      assert_trx_in_rw_list(candidate);

      if (candidate->mysql_thd == nullptr &&
          !candidate->preserve_trx_claimed &&
          trx_state_eq(candidate, TRX_STATE_PREPARED) &&
          candidate->xid != nullptr &&
          xid_is_preserve_magic(*candidate->xid) &&
          !trx_preserve_xid_token_in(*candidate->xid, snapshot_tokens)) {
        candidate->preserve_trx_claimed = true;
        trx = candidate;
        break;
      }
    }
    trx_sys_mutex_exit();

    if (trx == nullptr) return DB_SUCCESS;

    const dberr_t err = trx_preserve_rollback_claimed(trx);
    if (err != DB_SUCCESS) return err;

    if (rolled_back != nullptr) ++*rolled_back;
  }
}

dberr_t trx_preserve_prepare_resumed_rollback_gtid(trx_t *trx) {
  if (trx == nullptr || trx->xid == nullptr ||
      !xid_is_preserve_magic(*trx->xid)) {
    return DB_ERROR;
  }

  if (!trx_state_eq(trx, TRX_STATE_PRESERVED) || trx->mysql_thd != nullptr ||
      !trx->preserve_trx_claimed) {
    return DB_ERROR;
  }

  return trx_undo_gtid_add_update_undo(trx, false, true);
}

static dberr_t trx_preserve_activate_undo_ptr_state(trx_t *trx,
                                                    trx_undo_ptr_t *undo_ptr,
                                                    bool no_redo) {
  if (undo_ptr->rseg == nullptr ||
      (undo_ptr->insert_undo == nullptr && undo_ptr->update_undo == nullptr)) {
    return DB_SUCCESS;
  }

  mtr_t mtr;

  mtr.start();
  if (no_redo) {
    mtr.set_log_mode(MTR_LOG_NO_REDO);
  }
  undo_ptr->rseg->latch();

  if (undo_ptr->insert_undo != nullptr) {
    if (!(no_redo && undo_ptr->insert_undo->state == TRX_UNDO_ACTIVE)) {
      trx_undo_set_state_at_prepare(trx, undo_ptr->insert_undo, true, &mtr);
      undo_ptr->insert_undo->state = TRX_UNDO_ACTIVE;
    }
  }

  if (undo_ptr->update_undo != nullptr) {
    if (!no_redo) {
      trx_undo_gtid_set(trx, undo_ptr->update_undo);
    }
    if (!(no_redo && undo_ptr->update_undo->state == TRX_UNDO_ACTIVE)) {
      trx_undo_set_state_at_prepare(trx, undo_ptr->update_undo, true, &mtr);
      undo_ptr->update_undo->state = TRX_UNDO_ACTIVE;
    }
  }

  undo_ptr->rseg->unlatch();
  mtr.commit();
  if (!no_redo) {
    const lsn_t lsn = mtr.commit_lsn();
    ut_ad(lsn > 0 || !mtr_t::s_logging.is_enabled());
    if (lsn > 0) {
      log_write_up_to(*log_sys, lsn, true);
    }
  }

  return DB_SUCCESS;
}

static dberr_t trx_preserve_activate_undo_state(trx_t *trx) {
  dberr_t err = trx_preserve_activate_undo_ptr_state(
      trx, &trx->rsegs.m_redo, false);
  if (err != DB_SUCCESS) return err;

  return trx_preserve_activate_undo_ptr_state(trx, &trx->rsegs.m_noredo, true);
}

dberr_t trx_preserve_activate_resumed(trx_t *trx) {
  if (trx == nullptr || trx->xid == nullptr ||
      !xid_is_preserve_magic(*trx->xid)) {
    return DB_ERROR;
  }

  if (!trx_state_eq(trx, TRX_STATE_ACTIVE) || trx->mysql_thd != current_thd ||
      trx->preserve_trx_claimed) {
    return DB_ERROR;
  }

  DBUG_EXECUTE_IF("preserve_trx_fail_activate_resumed", return DB_ERROR;);

  return trx_preserve_activate_undo_state(trx);
}

dberr_t trx_preserve_reactivate_prepared_in_original_thd(THD *thd) {
  if (thd == nullptr) return DB_ERROR;

  trx_t *trx = thd_to_trx(thd);
  if (trx == nullptr || trx->xid == nullptr ||
      !xid_is_preserve_magic(*trx->xid) || trx->mysql_thd != thd ||
      trx->preserve_trx_claimed) {
    return DB_ERROR;
  }

  dberr_t err = DB_SUCCESS;
  trx_sys_mutex_enter();
  if (!trx_state_eq(trx, TRX_STATE_PREPARED) || trx->mysql_thd != thd ||
      trx->preserve_trx_claimed) {
    err = DB_ERROR;
  } else {
    ut_a(trx_sys->n_prepared_trx > 0);
    --trx_sys->n_prepared_trx;
    trx_preserve_store_state_trx_sys_locked(trx, TRX_STATE_ACTIVE);
  }
  trx_sys_mutex_exit();

  if (err != DB_SUCCESS) return err;
  return trx_preserve_activate_undo_state(trx);
}

dberr_t trx_preserve_reactivate_prepare_failure_in_original_thd(THD *thd) {
  if (thd == nullptr) return DB_ERROR;

  trx_t *trx = thd_to_trx(thd);
  if (trx == nullptr || trx->xid == nullptr ||
      !xid_is_preserve_magic(*trx->xid) || trx->mysql_thd != thd ||
      trx->preserve_trx_claimed) {
    return DB_ERROR;
  }

  bool was_prepared = false;
  dberr_t err = DB_SUCCESS;
  trx_sys_mutex_enter();
  if (trx_state_eq(trx, TRX_STATE_PREPARED)) {
    if (trx->mysql_thd != thd || trx->preserve_trx_claimed) {
      err = DB_ERROR;
    } else {
      ut_a(trx_sys->n_prepared_trx > 0);
      --trx_sys->n_prepared_trx;
      trx_preserve_store_state_trx_sys_locked(trx, TRX_STATE_ACTIVE);
      was_prepared = true;
    }
  } else if (!trx_state_eq(trx, TRX_STATE_ACTIVE) || trx->mysql_thd != thd ||
             trx->preserve_trx_claimed) {
    err = DB_ERROR;
  }
  trx_sys_mutex_exit();

  if (err != DB_SUCCESS) return err;
  return was_prepared ? trx_preserve_activate_undo_state(trx) : DB_SUCCESS;
}

dberr_t trx_preserve_activate_reattached_in_original_thd(trx_t *trx,
                                                        THD *thd) {
  if (trx == nullptr || thd == nullptr || trx->xid == nullptr ||
      !xid_is_preserve_magic(*trx->xid)) {
    return DB_ERROR;
  }

  if (!trx_state_eq(trx, TRX_STATE_ACTIVE) || trx->mysql_thd != thd ||
      trx->preserve_trx_claimed) {
    return DB_ERROR;
  }

  DBUG_EXECUTE_IF("preserve_trx_fail_activate_reattached_original",
                  return DB_ERROR;);

  return trx_preserve_activate_undo_state(trx);
}

bool trx_preserve_is_active_attached_to_thd(trx_t *trx, THD *thd) {
  bool attached = false;

  trx_sys_mutex_enter();
  attached = trx != nullptr && thd != nullptr && trx->mysql_thd == thd &&
             !trx->preserve_trx_claimed &&
             trx_state_eq(trx, TRX_STATE_ACTIVE);
  trx_sys_mutex_exit();

  return attached;
}

static dberr_t trx_preserve_make_temp_only_claimable(trx_t *trx);

/*
  Prepare a transaction that only changed no-redo temporary-table state.

  Such a transaction may not have a normal redo rseg update, but preserve still
  needs a prepared boundary so snapshot cleanup can own or roll back its temp
  sidecars consistently. If the transaction has no temp rseg updates this is a
  no-op; if it has redo updates the normal ha_prepare_low() path must handle it.
*/
dberr_t trx_preserve_prepare_current_temp_only(THD *thd, const XID &xid) {
  if (thd == nullptr || !xid_is_preserve_magic(xid)) return DB_ERROR;

  trx_t *trx = thd_to_trx(thd);
  if (trx == nullptr) return DB_ERROR;

  if (trx_state_eq(trx, TRX_STATE_PREPARED)) {
    return trx->xid != nullptr && xid_is_preserve_magic(*trx->xid)
               ? DB_SUCCESS
               : DB_ERROR;
  }

  if (!trx_state_eq(trx, TRX_STATE_ACTIVE) ||
      !trx_is_temp_rseg_updated(trx) || trx_is_redo_rseg_updated(trx)) {
    return DB_SUCCESS;
  }

  const dberr_t claimable_err = trx_preserve_make_temp_only_claimable(trx);
  if (claimable_err != DB_SUCCESS) return claimable_err;

  *trx->xid = xid;
  return trx_prepare_for_mysql(trx);
}

static void trx_preserve_add_to_rw_trx_list_ordered(trx_t *trx) {
  ut_ad(trx_sys_mutex_own());
  ut_ad(trx != nullptr);
  ut_ad(!trx->in_rw_trx_list);

  trx_t *prev = nullptr;
  for (trx_t *candidate = UT_LIST_GET_FIRST(trx_sys->rw_trx_list);
       candidate != nullptr; candidate = UT_LIST_GET_NEXT(trx_list, candidate)) {
    if (candidate->id < trx->id) {
      if (prev == nullptr) {
        UT_LIST_ADD_FIRST(trx_sys->rw_trx_list, trx);
      } else {
        UT_LIST_INSERT_AFTER(trx_sys->rw_trx_list, prev, trx);
      }
      ut_d(trx->in_rw_trx_list = true);
      return;
    }
    prev = candidate;
  }

  UT_LIST_ADD_LAST(trx_sys->rw_trx_list, trx);
  ut_d(trx->in_rw_trx_list = true);
}

static bool trx_preserve_rw_trx_list_contains(trx_t *trx) {
  ut_ad(trx_sys_mutex_own());
  ut_ad(trx != nullptr);

  for (trx_t *candidate = UT_LIST_GET_FIRST(trx_sys->rw_trx_list);
       candidate != nullptr; candidate = UT_LIST_GET_NEXT(trx_list, candidate)) {
    if (candidate == trx) return true;
  }
  return false;
}

static dberr_t trx_preserve_make_temp_only_claimable(trx_t *trx) {
  if (trx == nullptr || trx->id == 0) return DB_ERROR;

  /*
    MySQL permits READ ONLY transactions to modify user temporary tables.
    InnoDB gives such a transaction a no-redo rseg and trx id, but keeps it off
    rw_trx_list because ordinary read-only execution does not need prepared
    recovery ownership. Preserve does need that ownership: once the temp-only
    trx is prepared and detached, claim/rollback/resume all find it through the
    preserved rw list. Add it only for this explicit preserve boundary.

    SQL read-only state has already been captured in the preserve metadata.
    From this point on the InnoDB object is a preserve-owned prepared/rollback
    object, and rw_trx_list debug invariants require !trx->read_only and a
    durable rseg bookkeeping assignment. The durable rseg does not mean the
    snapshot has ordinary redo undo for user data; it gives InnoDB's prepared
    owner lists the same shape as non-read-only temp-only preserve.
  */
  if (trx->rsegs.m_redo.rseg == nullptr) {
    trx_assign_rseg_durable(trx);
    if (trx->rsegs.m_redo.rseg == nullptr) return DB_ERROR;
  }

  trx_sys_mutex_enter();
  trx->read_only = false;
  if (!trx_preserve_rw_trx_list_contains(trx)) {
    trx_preserve_add_to_rw_trx_list_ordered(trx);
  }
  trx_sys_mutex_exit();
  return DB_SUCCESS;
}

/*
	  Recreate a claimed preserved trx for a temp-only snapshot.

	  Startup recovery can find a token whose only durable engine state is the
	  temporary-table manifest. There is no recovered prepared trx to claim, so this
	  helper allocates a background trx with the preserved owner id, inserts it into
	  the rw trx list in trx-id order, and marks it claimed/PRESERVED. The durable
	  rseg assignment below is trx bookkeeping needed for rollback/rw-list
	  invariants; it does not mean the snapshot contains ordinary redo undo for the
	  transaction. Any failure rolls the partially allocated trx back to NOT_STARTED
	  before freeing it.
*/
trx_t *trx_preserve_create_temp_only_claimed(const XID &xid, uint64_t trx_id) {
  if (!xid_is_preserve_magic(xid) || trx_id == 0 || trx_id >= TRX_ID_MAX) {
    return nullptr;
  }

  const trx_id_t recovered_trx_id = static_cast<trx_id_t>(trx_id);
  trx_t *trx = trx_allocate_for_background();
  if (trx == nullptr) return nullptr;

  *trx->xid = xid;
  trx->id = recovered_trx_id;
  /*
    SQL transaction access mode is restored on THD/session metadata. This
    synthetic InnoDB trx must remain rw-list-compatible because temp-only
    sidecar cleanup, rollback and prepared ownership all use rw_trx_list
    invariants that assert !trx->read_only for listed transactions.
  */
  trx->read_only = false;
  trx->auto_commit = false;
  trx->will_lock = 1;
  trx->is_recovered = true;
  trx->preserve_trx_claimed = true;
  trx_preserve_store_private_state(trx, TRX_STATE_PRESERVED);

  trx_assign_rseg_durable(trx);
  if (trx->rsegs.m_redo.rseg == nullptr) {
    trx->id = 0;
    trx_preserve_store_private_state(trx, TRX_STATE_NOT_STARTED);
    trx->xid->reset();
    trx->preserve_trx_claimed = false;
    trx->will_lock = 0;
    trx->is_recovered = false;
    trx_free_for_background(trx);
    return nullptr;
  }

  trx_sys_mutex_enter();
  if (trx_sys->max_trx_id <= recovered_trx_id) {
    trx_sys->max_trx_id = recovered_trx_id + 1;
  }
  auto pos = std::lower_bound(trx_sys->rw_trx_ids.begin(),
                              trx_sys->rw_trx_ids.end(), recovered_trx_id);
  if (pos != trx_sys->rw_trx_ids.end() && *pos == recovered_trx_id) {
    trx_sys_mutex_exit();
    trx->rsegs.m_redo.rseg->trx_ref_count--;
    trx->rsegs.m_redo.rseg = nullptr;
    trx->id = 0;
    trx_preserve_store_private_state(trx, TRX_STATE_NOT_STARTED);
    trx->xid->reset();
    trx->preserve_trx_claimed = false;
    trx->will_lock = 0;
    trx->is_recovered = false;
    trx_free_for_background(trx);
    return nullptr;
  }
  trx_sys->rw_trx_ids.insert(pos, recovered_trx_id);
  trx_preserve_add_to_rw_trx_list_ordered(trx);
  trx_sys_mutex_exit();

  trx_sys_rw_trx_add(trx);
  return trx;
}

uint64_t trx_preserve_trx_id(const trx_t *trx) {
  return trx != nullptr ? trx->id : 0;
}

void trx_preserve_release_claim_before_free(trx_t *trx) {
  if (trx != nullptr) {
    trx->preserve_trx_claimed = false;
  }
}

static trx_t *trx_preserve_current_thd_get_trx_if_available(THD *thd);

bool trx_preserve_current_thd_has_read_view(THD *thd) {
  if (thd == nullptr) {
    return false;
  }

  trx_t *trx = thd_to_trx(thd);
  return trx_preserve_trx_has_read_view(trx);
}

bool trx_preserve_current_thd_has_record_locks(THD *thd) {
  trx_t *trx = trx_preserve_current_thd_get_trx_if_available(thd);
  if (trx == nullptr) return false;

  /*
    This is only a phase-1 warmcopy candidate hint. The authoritative seal path
    samples the full lock fence and falls back if the prebuilt payload cannot be
    proven current, so this helper must not be used as a correctness fence.
  */
  return trx->lock.n_rec_locks.load() != 0;
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

/*
  Close live ReadView handles before shutdown after their payloads were captured.

  Once a transaction is preserved, resume must import from the serialized
  snapshot instead of keeping an in-memory ReadView pointer alive across process
  exit. Claimed prepared-preserve transactions are included because they may be
  waiting for snapshot cleanup or resume handling.
*/
void trx_preserve_close_read_views_for_shutdown() {
  if (trx_sys == nullptr || trx_sys->mvcc == nullptr) return;

  trx_sys_mutex_enter();
  for (trx_t *trx = UT_LIST_GET_FIRST(trx_sys->rw_trx_list); trx != nullptr;
       trx = UT_LIST_GET_NEXT(trx_list, trx)) {
    const bool is_preserved = trx_state_eq(trx, TRX_STATE_PRESERVED);
    const bool is_claimed_prepared_preserve_trx =
        trx_state_eq(trx, TRX_STATE_PREPARED) && trx->preserve_trx_claimed &&
        trx->xid != nullptr && xid_is_preserve_magic(*trx->xid);
    if ((is_preserved || is_claimed_prepared_preserve_trx) &&
        trx->read_view != nullptr) {
      trx_sys->mvcc->view_close(trx->read_view, true);
    }
  }
  trx_sys_mutex_exit();
}

dberr_t trx_preserve_materialize_implicit_locks(
    THD *thd, const Preserve_lock_limits &limits, bool *materialized_any) {
  if (thd == nullptr) {
    return DB_ERROR;
  }

  trx_t *trx = thd_to_trx(thd);
  if (trx == nullptr) {
    return DB_ERROR;
  }

  return lock_preserve_materialize_implicit_locks(trx, limits,
                                                 materialized_any);
}

/*
  Serialize the active ReadView as low/up/creator limits plus the active rw-trx
  id array captured in the view. low_limit_no is returned separately for
  observability. An empty payload means the transaction had no active consistent
  read view.
*/
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

/*
  Import a serialized ReadView into the resumed trx.

  The payload is trusted only if its structural count matches, low_limit_no is
  nonzero, purge is not running past the preserved visibility window, and the
  saved limits do not exceed the current trx-id/no horizon. Otherwise resume
  fails closed rather than installing a view that could expose purged versions or
  future transaction ids.
*/
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
  const trx_id_t next_trx_id_or_no = trx_sys_get_next_trx_id_or_no();
  if (snapshot.low_limit_no > next_trx_id_or_no ||
      snapshot.low_limit_id > next_trx_id_or_no) {
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
  return lock_preserve_export_record_locks(trx, payload);
}

dberr_t trx_preserve_export_record_locks(trx_t *trx, std::string *payload,
                                         uint32_t max_lock_count) {
  return lock_preserve_export_record_locks(trx, payload, max_lock_count);
}

dberr_t trx_preserve_export_record_locks(THD *thd, std::string *payload,
                                         uint32_t max_lock_count) {
  if (thd == nullptr || payload == nullptr) {
    return DB_ERROR;
  }

  trx_t *trx = thd_to_trx(thd);
  if (trx == nullptr) {
    return DB_ERROR;
  }

  return lock_preserve_export_record_locks(trx, payload, max_lock_count);
}

dberr_t trx_preserve_export_record_locks_stable_page_only(
    THD *thd, std::string *payload, uint32_t max_lock_count) {
  if (thd == nullptr || payload == nullptr) {
    return DB_ERROR;
  }

  trx_t *trx = thd_to_trx(thd);
  if (trx == nullptr) {
    return DB_ERROR;
  }

  return lock_preserve_export_record_locks_stable_page_only(
      trx, payload, max_lock_count);
}

bool trx_preserve_sample_lock_warmcopy_fence(
    trx_t *trx, lock_warmcopy_trx_lock_fence_t *fence) {
  if (trx == nullptr || fence == nullptr) return false;

  trx_mutex_enter(trx);
  ut_ad(trx_mutex_own(trx));
  const bool sampled = lock_warmcopy_trx_lock_fence_sample(&trx->lock, fence);
  trx_mutex_exit(trx);
  return sampled;
}

bool trx_preserve_sample_lock_warmcopy_fence(
    THD *thd, lock_warmcopy_trx_lock_fence_t *fence) {
  if (thd == nullptr) return false;

  return trx_preserve_sample_lock_warmcopy_fence(thd_to_trx(thd), fence);
}

void trx_preserve_lock_warmcopy_conversion_thaw(trx_t *trx) {
  if (trx == nullptr) return;

  trx_mutex_enter(trx);
  lock_warmcopy_trx_conversion_thaw(&trx->lock);
  trx_mutex_exit(trx);
}

bool trx_preserve_lock_warmcopy_conversion_freeze(
    THD *thd, lock_warmcopy_trx_lock_fence_t *fence, trx_t **frozen_trx) {
  if (frozen_trx != nullptr) *frozen_trx = nullptr;
  if (thd == nullptr || fence == nullptr || frozen_trx == nullptr) {
    return false;
  }

  trx_t *trx = thd_to_trx(thd);
  if (trx == nullptr) return false;

  trx_mutex_enter(trx);
  ut_ad(trx_mutex_own(trx));
  const uint64_t wait_epoch = trx->lock.lock_warmcopy_freeze_generation + 1;
  lock_warmcopy_trx_conversion_freeze(&trx->lock, wait_epoch);
  const bool sampled = lock_warmcopy_trx_lock_fence_sample(&trx->lock, fence);
  trx_mutex_exit(trx);

  if (!sampled) {
    trx_preserve_lock_warmcopy_conversion_thaw(trx);
    return false;
  }

  *frozen_trx = trx;
  return true;
}

bool trx_preserve_lock_warmcopy_note_conversion_attempt_after_freeze(
    trx_t *trx) {
  if (trx == nullptr) return false;

  trx_mutex_enter(trx);
  const bool attempted =
      lock_warmcopy_trx_conversion_note_attempt(&trx->lock);
  const bool handled =
      lock_warmcopy_trx_conversion_note_handled(&trx->lock);
  trx_mutex_exit(trx);

  return attempted && handled;
}

bool trx_preserve_has_predicate_locks(THD *thd, bool *has_predicate_locks) {
  if (thd == nullptr || has_predicate_locks == nullptr) return false;

  DBUG_EXECUTE_IF("preserve_trx_lock_warmcopy_fail_predicate_lock_probe", {
    return false;
  });
  DBUG_EXECUTE_IF("preserve_trx_lock_warmcopy_simulate_predicate_locks", {
    *has_predicate_locks = true;
    return true;
  });

  trx_t *trx = thd_to_trx(thd);
  if (trx == nullptr) return false;

  return lock_preserve_trx_has_predicate_locks(trx, has_predicate_locks);
}

const char *trx_preserve_last_record_lock_export_error() {
  return lock_preserve_last_record_lock_export_error();
}

dberr_t trx_preserve_import_record_locks(trx_t *trx,
                                         const std::string &payload) {
  return lock_preserve_import_record_locks(trx, payload);
}

dberr_t trx_preserve_import_record_locks(
    trx_t *trx, const std::string &payload,
    trx_preserve_record_lock_import_metrics_t *metrics) {
  return lock_preserve_import_record_locks(trx, payload, metrics);
}

dberr_t trx_preserve_import_record_locks(
    trx_t *trx, const std::string &payload,
    trx_preserve_record_lock_import_metrics_t *metrics,
    bool (*deadline_expired)(void *), void *deadline_ctx) {
  return lock_preserve_import_record_locks(trx, payload, metrics,
                                           deadline_expired, deadline_ctx);
}

dberr_t trx_preserve_prefetch_record_lock_pages(
    const std::string &payload,
    trx_preserve_record_lock_import_metrics_t *metrics) {
  return lock_preserve_prefetch_record_lock_pages(payload, metrics);
}

dberr_t trx_preserve_prefetch_record_lock_pages_for_gate(
    const std::string &payload,
    trx_preserve_record_lock_import_metrics_t *metrics) {
  return lock_preserve_prefetch_record_lock_pages_for_gate(payload, metrics);
}

bool trx_preserve_record_locks_payload_is_valid_for_import(
    const std::string &payload) {
  return lock_preserve_record_locks_payload_is_valid_for_import(payload);
}

bool trx_preserve_record_locks_payload_lock_count(
    const std::string &payload, uint32_t *lock_count) {
  return lock_preserve_record_locks_payload_lock_count(payload, lock_count);
}

bool trx_preserve_record_lock_payload_page_plan(
    const std::string &payload, trx_preserve_record_lock_page_plan_t *plan) {
  return lock_preserve_record_lock_payload_page_plan(payload, plan);
}

bool trx_preserve_record_lock_payload_residency(
    const std::string &payload,
    trx_preserve_record_lock_residency_t *residency) {
  return lock_preserve_record_lock_payload_residency(payload, residency);
}

bool trx_preserve_split_record_and_predicate_locks(
    const std::string &payload, std::string *record_locks_payload,
    std::string *predicate_locks_payload) {
  return lock_preserve_split_record_and_predicate_locks(
      payload, record_locks_payload, predicate_locks_payload);
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
  return lock_preserve_import_table_locks(trx, payload);
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

void trx_preserve_debug_table_lock_import_roundtrip(
    THD *thd, uint32_t max_lock_count,
    Preserve_table_lock_import_debug_result *result) {
  if (result == nullptr) return;
  *result = Preserve_table_lock_import_debug_result{};

  std::string payload;
  result->export_err =
      trx_preserve_export_table_locks(thd, &payload, max_lock_count, 0);
  result->valid = trx_preserve_table_locks_payload_is_valid_for_import(payload);
  result->count_ok =
      trx_preserve_table_locks_payload_lock_count(payload, &result->count);

  trx_t *import_trx = trx_allocate_for_background();
  trx_start_internal(import_trx);

  if (result->export_err == DB_SUCCESS && result->valid) {
    result->import_err = trx_preserve_import_table_locks(import_trx, payload);
  }

  std::string reexport_payload;
  if (result->import_err == DB_SUCCESS) {
    result->reexport_err = trx_preserve_export_table_locks(
        import_trx, &reexport_payload, max_lock_count, 0);
    result->reexport_count_ok =
        trx_preserve_table_locks_payload_lock_count(reexport_payload,
                                                   &result->reexport_count);
  }

  result->release_err = trx_commit_for_mysql(import_trx);
  if (result->release_err == DB_SUCCESS) {
    trx_free_for_background(import_trx);
  }
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
  if (rseg == nullptr) return false;
  if (trx_preserve_active_preserved_rseg_owners.load(
          std::memory_order_acquire) == 0) {
    return false;
  }

  bool found = false;
  trx_sys_mutex_enter();
  for (trx_t *trx = UT_LIST_GET_FIRST(trx_sys->rw_trx_list); trx != nullptr;
       trx = UT_LIST_GET_NEXT(trx_list, trx)) {
    assert_trx_in_rw_list(trx);
    const bool uses_rseg = trx->rsegs.m_redo.rseg == rseg ||
                           trx->rsegs.m_noredo.rseg == rseg;
    if (!uses_rseg) continue;

    if (trx_state_eq(trx, TRX_STATE_PRESERVED) ||
        (trx_state_eq(trx, TRX_STATE_PREPARED) && trx->xid != nullptr &&
         xid_is_preserve_magic(*trx->xid))) {
      found = true;
      break;
    }
  }
  trx_sys_mutex_exit();

  return found;
}

void trx_preserve_collect_preserved_rsegs(
    std::vector<const trx_rseg_t *> *rsegs) {
  if (rsegs == nullptr) return;

  rsegs->clear();
  if (trx_preserve_active_preserved_rseg_owners.load(
          std::memory_order_acquire) == 0) {
    return;
  }

  auto remember_rseg = [rsegs](const trx_rseg_t *rseg) {
    if (rseg != nullptr &&
        std::find(rsegs->begin(), rsegs->end(), rseg) == rsegs->end()) {
      rsegs->push_back(rseg);
    }
  };

  trx_sys_mutex_enter();
  for (trx_t *trx = UT_LIST_GET_FIRST(trx_sys->rw_trx_list); trx != nullptr;
       trx = UT_LIST_GET_NEXT(trx_list, trx)) {
    assert_trx_in_rw_list(trx);
    if (trx_state_eq(trx, TRX_STATE_PRESERVED) ||
        (trx_state_eq(trx, TRX_STATE_PREPARED) && trx->xid != nullptr &&
         xid_is_preserve_magic(*trx->xid))) {
      remember_rseg(trx->rsegs.m_redo.rseg);
      remember_rseg(trx->rsegs.m_noredo.rseg);
    }
  }
  trx_sys_mutex_exit();
}

void trx_preserve_debug_current_thd_rseg_collection(
    THD *thd, Preserve_rseg_collection_debug_result *result) {
  if (result == nullptr) return;
  *result = Preserve_rseg_collection_debug_result{};

  trx_t *trx = thd != nullptr ? thd_to_trx(thd) : nullptr;
  if (trx == nullptr) return;

  std::vector<const trx_rseg_t *> rsegs;
  trx_preserve_collect_preserved_rsegs(&rsegs);
  result->count = static_cast<uint32_t>(rsegs.size());

  const auto contains = [&rsegs](const trx_rseg_t *rseg) {
    return rseg != nullptr &&
           std::find(rsegs.begin(), rsegs.end(), rseg) != rsegs.end();
  };

  result->contains_redo = contains(trx->rsegs.m_redo.rseg);
  result->contains_noredo = contains(trx->rsegs.m_noredo.rseg);
}

/*
  Detach a freshly prepared preserve trx from its original THD.

  The caller still owns cleanup on failure. On success the trx is removed from
  mysql_trx_list, thd_to_trx() and ha_info are cleared, and the original THD no
  longer has a transaction scope for this engine trx.
*/
trx_t *trx_preserve_detach_current_thd(THD *thd) {
  if (thd == nullptr || innodb_hton == nullptr ||
      innodb_hton->replace_native_transaction_in_thd == nullptr) {
    return nullptr;
  }

  trx_t *trx = thd_to_trx(thd);
  if (trx == nullptr || trx->xid == nullptr ||
      !xid_is_preserve_magic(*trx->xid) ||
      !trx_state_eq(trx, TRX_STATE_PREPARED) ||
      !trx_is_rseg_updated(trx)) {
    return nullptr;
  }

  trx_sys_mutex_enter();
  if (
#ifdef UNIV_DEBUG
      !trx->in_mysql_trx_list ||
#endif /* UNIV_DEBUG */
      trx->mysql_thd != thd) {
    trx_sys_mutex_exit();
    return nullptr;
  }

  UT_LIST_REMOVE(trx_sys->mysql_trx_list, trx);
  ut_d(trx->in_mysql_trx_list = false);
  trx->mysql_thd = nullptr;
  ut_ad(trx_sys_validate_trx_list());
  trx_sys_mutex_exit();

  thd_to_trx(thd) = nullptr;
  thd->get_ha_data(innodb_hton->slot)
      ->ha_info[Transaction_ctx::SESSION]
      .reset();
  thd->get_ha_data(innodb_hton->slot)
      ->ha_info[Transaction_ctx::STMT]
      .reset();
  thd->get_transaction()->reset_scope(Transaction_ctx::SESSION);
  thd->get_transaction()->reset_scope(Transaction_ctx::STMT);

  return trx;
}

/*
  Attach a claimed PRESERVED trx to the RESUME THD.

  The target THD must not already own an active InnoDB transaction. On success
  the trx leaves claimed/PRESERVED state, enters ACTIVE, is inserted into
  mysql_trx_list, and is registered in the THD's handler transaction state.
*/
dberr_t trx_preserve_attach_to_thd(trx_t *trx, THD *thd) {
  if (trx == nullptr || thd == nullptr || trx->xid == nullptr ||
      !xid_is_preserve_magic(*trx->xid) || innodb_hton == nullptr ||
      innodb_hton->replace_native_transaction_in_thd == nullptr) {
    return DB_ERROR;
  }

  trx_t *existing_trx = thd_to_trx(thd);
  if (existing_trx != nullptr &&
      !trx_state_eq(existing_trx, TRX_STATE_NOT_STARTED)) {
    return DB_ERROR;
  }
  if (existing_trx != nullptr &&
      trx_rollback_for_mysql(existing_trx) != DB_SUCCESS) {
    return DB_ERROR;
  }

  DBUG_EXECUTE_IF("preserve_trx_fail_attach_to_thd", return DB_ERROR;);

  dberr_t err = DB_SUCCESS;

  trx_sys_mutex_enter();
  if (!trx_state_eq(trx, TRX_STATE_PRESERVED) || trx->mysql_thd != nullptr ||
      !trx->preserve_trx_claimed) {
    err = DB_ERROR;
  } else {
    trx->mysql_thd = thd;
    trx->preserve_trx_claimed = false;
    trx->is_recovered = false;
    trx_preserve_store_state_trx_sys_locked(trx, TRX_STATE_ACTIVE);
    UT_LIST_ADD_FIRST(trx_sys->mysql_trx_list, trx);
    ut_d(trx->in_mysql_trx_list = true);
  }
  trx_sys_mutex_exit();

  if (err != DB_SUCCESS) {
    return err;
  }

  trx->last_sql_stat_start.least_undo_no = trx->undo_no;

  innodb_hton->replace_native_transaction_in_thd(thd, trx, nullptr);
  innobase_register_trx(innodb_hton, thd, trx);
  {
    const ulonglong trx_id =
        static_cast<ulonglong>(trx_get_id_for_print(trx));
    trans_register_ha(thd, true, innodb_hton, &trx_id);
  }

  return DB_SUCCESS;
}

/*
  Reattach a preserve candidate to its original THD after preserve fails.

  This is the recovery path before a durable snapshot has taken ownership. It
  accepts a PREPARED candidate that still needs to leave n_prepared_trx, or a
  PRESERVED candidate left by a later failure, and restores ACTIVE handler state
  on the original THD so normal rollback/error handling can continue.
*/
dberr_t trx_preserve_reattach_preserved_to_original_thd(trx_t *trx, THD *thd) {
  if (trx == nullptr || thd == nullptr || trx->xid == nullptr ||
      !xid_is_preserve_magic(*trx->xid) || innodb_hton == nullptr ||
      innodb_hton->replace_native_transaction_in_thd == nullptr) {
    return DB_ERROR;
  }

  trx_t *existing_trx = thd_to_trx(thd);
  if (existing_trx != nullptr &&
      !trx_state_eq(existing_trx, TRX_STATE_NOT_STARTED)) {
    return DB_ERROR;
  }
  if (existing_trx != nullptr &&
      trx_rollback_for_mysql(existing_trx) != DB_SUCCESS) {
    return DB_ERROR;
  }

  DBUG_EXECUTE_IF("preserve_trx_fail_reattach_original_thd",
                  return DB_ERROR;);

  dberr_t err = DB_SUCCESS;

  trx_sys_mutex_enter();
  if ((!trx_state_eq(trx, TRX_STATE_PREPARED) &&
       !trx_state_eq(trx, TRX_STATE_PRESERVED)) ||
      trx->mysql_thd != nullptr) {
    err = DB_ERROR;
  } else {
    if (trx_state_eq(trx, TRX_STATE_PREPARED)) {
      ut_a(trx_sys->n_prepared_trx > 0);
      --trx_sys->n_prepared_trx;
    }
    trx->mysql_thd = thd;
    trx->preserve_trx_claimed = false;
    trx->is_recovered = false;
    trx_preserve_store_state_trx_sys_locked(trx, TRX_STATE_ACTIVE);
    UT_LIST_ADD_FIRST(trx_sys->mysql_trx_list, trx);
    ut_d(trx->in_mysql_trx_list = true);
  }
  trx_sys_mutex_exit();

  if (err != DB_SUCCESS) {
    return err;
  }

  trx->last_sql_stat_start.least_undo_no = trx->undo_no;

  innodb_hton->replace_native_transaction_in_thd(thd, trx, nullptr);
  innobase_register_trx(innodb_hton, thd, trx);
  {
    const ulonglong trx_id =
        static_cast<ulonglong>(trx_get_id_for_print(trx));
    trans_register_ha(thd, true, innodb_hton, &trx_id);
  }

  return DB_SUCCESS;
}

/*
  Detach an already resumed ACTIVE trx back into a claimed PRESERVED record.

  Resume failure and cleanup paths use this after attach has succeeded but the
  token must remain recoverable. The function removes THD ownership and handler
  registration, marks the trx claimed, and stores PRESERVED state; callers decide
  whether to restore the record, roll it back, or expose cleanup failure.
*/
static dberr_t trx_preserve_detach_resumed_from_thd_low(trx_t *trx, THD *thd) {
  if (trx == nullptr || thd == nullptr || trx->xid == nullptr ||
      !xid_is_preserve_magic(*trx->xid) || thd_to_trx(thd) != trx ||
      innodb_hton == nullptr) {
    return DB_ERROR;
  }

  trx_sys_mutex_enter();
  if (
#ifdef UNIV_DEBUG
      !trx->in_mysql_trx_list ||
#endif /* UNIV_DEBUG */
      trx->mysql_thd != thd || trx->preserve_trx_claimed ||
      !trx_state_eq(trx, TRX_STATE_ACTIVE)) {
    trx_sys_mutex_exit();
    return DB_ERROR;
  }

  UT_LIST_REMOVE(trx_sys->mysql_trx_list, trx);
  ut_d(trx->in_mysql_trx_list = false);
  trx->mysql_thd = nullptr;
  trx->preserve_trx_claimed = true;
  trx->is_registered = false;
  trx_preserve_store_state_trx_sys_locked(trx, TRX_STATE_PRESERVED);
  ut_ad(trx_sys_validate_trx_list());
  trx_sys_mutex_exit();

  thd_to_trx(thd) = nullptr;
  thd->get_ha_data(innodb_hton->slot)
      ->ha_info[Transaction_ctx::SESSION]
      .reset();
  thd->get_ha_data(innodb_hton->slot)->ha_info[Transaction_ctx::STMT].reset();
  thd->get_transaction()->reset_scope(Transaction_ctx::SESSION);
  thd->get_transaction()->reset_scope(Transaction_ctx::STMT);

  return DB_SUCCESS;
}

dberr_t trx_preserve_detach_resumed_from_thd(trx_t *trx, THD *thd) {
  if (trx == nullptr || thd == nullptr || trx->xid == nullptr ||
      !xid_is_preserve_magic(*trx->xid) || thd_to_trx(thd) != trx ||
      innodb_hton == nullptr) {
    return DB_ERROR;
  }

  DBUG_EXECUTE_IF("preserve_trx_fail_detach_resumed_from_thd",
                  return DB_ERROR;);

  return trx_preserve_detach_resumed_from_thd_low(trx, thd);
}

dberr_t trx_preserve_detach_resumed_from_thd_for_cleanup(trx_t *trx,
                                                        THD *thd) {
  DBUG_EXECUTE_IF("preserve_trx_fail_detach_resumed_from_thd_for_cleanup",
                  return DB_ERROR;);

  return trx_preserve_detach_resumed_from_thd_low(trx, thd);
}

void trx_preserve_reset_thd_statement_registration(THD *thd) {
  if (thd == nullptr || innodb_hton == nullptr) return;

  thd->get_ha_data(innodb_hton->slot)->ha_info[Transaction_ctx::STMT].reset();
  thd->get_transaction()->reset_scope(Transaction_ctx::STMT);
}
