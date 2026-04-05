/* Copyright (c) 2025, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file sql/ps_point_plan_cache.cc

  Implementation of per-prepared-statement single-slot plan template cache
  for single-table point SELECT queries (v1).

  Phase 0: skeleton — status counter helpers.
  Phase 1: static classification at PREPARE time.
  Phase 2: admission after first normal optimization.
*/

#include "sql/ps_point_plan_cache.h"

#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_func.h"
#include "sql/key.h"
#include "sql/sql_class.h"
#include "sql/sql_executor.h"
#include "sql/sql_lex.h"
#include "sql/sql_opt_exec_shared.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_prepare.h"
#include "sql/sql_select.h"
#include "sql/table.h"

/**
  Try to extract a single field=param equality from an Item_func_eq.
  Handles both (field, param) and (param, field) argument order.

  @param      eq_item   The EQ_FUNC item to inspect.
  @param      tbl       The target table — field must belong to it.
  @param[out] field_out Receives the Item_field pointer on success.
  @param[out] param_out Receives the Item_param pointer on success.
  @return true on success, false if pattern does not match.
*/
static bool extract_eq_field_param(Item_func *eq_item, const Table_ref *tbl,
                                   Item_field **field_out,
                                   Item_param **param_out) {
  if (eq_item->functype() != Item_func::EQ_FUNC) return false;
  if (eq_item->argument_count() != 2) return false;

  Item *a = eq_item->arguments()[0];
  Item *b = eq_item->arguments()[1];

  Item_field *fld = nullptr;
  Item_param *prm = nullptr;

  if (a->type() == Item::FIELD_ITEM && b->type() == Item::PARAM_ITEM) {
    fld = down_cast<Item_field *>(a);
    prm = down_cast<Item_param *>(b);
  } else if (a->type() == Item::PARAM_ITEM && b->type() == Item::FIELD_ITEM) {
    prm = down_cast<Item_param *>(a);
    fld = down_cast<Item_field *>(b);
  } else {
    return false;
  }

  if (fld->table_ref != tbl) return false;

  *field_out = fld;
  *param_out = prm;
  return true;
}

/**
  Classify a freshly prepared statement as a plan-cache candidate.

  Called once from Prepared_statement::prepare() (and again on
  reprepare) after the LEX tree is fully resolved.  This is a
  read-only shape check against the AST — no optimization has
  happened yet.

  Classification gates (any failure → return false, state unchanged
  or set to NEVER):

  1. Feature enabled (sysvar ON) and param count in [1, MAX_PARAMS].
  2. Simple single-query-block SELECT on the classic optimizer.
  3. Single base table, no outer joins, no subqueries.
  4. No GROUP BY, DISTINCT, ORDER BY, LIMIT, window functions, or
     FULLTEXT predicates.
  5. WHERE shape is pure field=? equality conjunction.
  6. Every ? parameter in the statement appears in a WHERE equality
     (param_count == m_param_count), ensuring no "extra" parameters
     exist outside the key predicate.

  @param  thd   Current thread.
  @param  stmt  The prepared statement being classified.
  @retval true  Classified as COLD (candidate for Phase 2 admission).
  @retval false Not a candidate (state NEVER or unchanged).
*/
bool ps_point_plan_classify(THD *thd, Prepared_statement *stmt) {
  /* Gate 1: feature switch and basic param count bounds. */
  if (!thd->variables.ps_point_plan_cache) return false;
  if (stmt->m_param_count < 1 || stmt->m_param_count > PS_PC_MAX_PARAMS)
    return false;

  /* Gate 2: must be a simple SELECT on the classic optimizer. */
  LEX *lex = stmt->m_lex;
  if (lex->sql_command != SQLCOM_SELECT) return false;
  if (lex->using_hypergraph_optimizer()) return false;

  Query_expression *unit = lex->unit;
  if (unit == nullptr || !unit->is_simple()) return false;

  /* Gate 3: single base table, no joins, no subqueries. */
  Query_block *qb = unit->first_query_block();
  if (qb->leaf_table_count != 1) return false;
  if (qb->outer_join != 0) return false;
  if (qb->first_inner_query_expression() != nullptr) return false;

  /* Gate 4: no aggregation, sorting, or complex features. */
  if (qb->is_grouped() || qb->is_distinct() || qb->is_ordered() ||
      qb->has_limit() || qb->has_windows() || qb->has_ft_funcs())
    return false;

  Table_ref *tbl = qb->leaf_tables;
  if (tbl == nullptr || !tbl->is_base_table()) return false;

  /* Gate 5: extract WHERE shape (field=? equalities). */
  PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();
  tpl = PsPointPlanTemplate{};
  tpl.table_ref = tbl;

  if (!ps_point_plan_extract_where_shape(qb, &tpl)) {
    stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);
    return false;
  }

  /*
    Gate 6: all parameters must appear in WHERE equalities.
    If param_count < m_param_count, some ? are in SELECT list or
    other clauses — the query is not a pure point lookup.
  */
  if (tpl.param_count != stmt->m_param_count) {
    stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);
    return false;
  }

  stmt->set_ps_point_plan_state(PsPointPlanState::COLD);
  ps_point_plan_mark_cold_classification(thd);
  return true;
}

/**
  Extract the WHERE-clause shape and populate template fields.

  Recognizes two supported shapes:

  - Shape A (single equality):
    @code WHERE field = ? @endcode
    Represented as a top-level Item_func_eq (FUNC_ITEM).

  - Shape B (composite equality conjunction):
    @code WHERE f1 = ? AND f2 = ? [AND ...] @endcode
    Represented as Item_cond_and (COND_ITEM) wrapping 2..MAX_PARAMS
    Item_func_eq items.

  For each recognized equality, the Item_param pointer and the
  field's 0-based index within the table are stored in the template
  arrays (params[] and field_indices[]).  The order follows the
  WHERE clause — may differ from key-part order; Phase 2 admission
  reorders to key-part order.

  @param  qb   The query block containing the WHERE clause.
  @param  tpl  Template to populate (table_ref must already be set).
  @retval true  Supported shape found; tpl fields populated.
  @retval false Unsupported shape; tpl partially populated (caller
                should set state to NEVER).
*/
bool ps_point_plan_extract_where_shape(Query_block *qb,
                                       PsPointPlanTemplate *tpl) {
  Item *where = qb->where_cond();
  if (where == nullptr) return false;

  const Table_ref *tbl = tpl->table_ref;
  tpl->plan_type = PsCachedPlanType::POINT_EQ_REF;
  tpl->param_count = 0;

  if (where->type() == Item::FUNC_ITEM) {
    /* Shape A: single equality  WHERE field = ? */
    Item_field *fld = nullptr;
    Item_param *prm = nullptr;
    if (!extract_eq_field_param(down_cast<Item_func *>(where), tbl, &fld, &prm))
      return false;
    tpl->params[0] = prm;
    tpl->field_indices[0] = fld->field_index;
    tpl->param_count = 1;
    return true;
  }

  if (where->type() == Item::COND_ITEM) {
    /*
      Shape B: composite AND of equalities.
      Every sub-item must be a field=? equality on the target table.
      OR conditions (COND_OR_FUNC) are rejected.
    */
    Item_cond *cond = down_cast<Item_cond *>(where);
    if (cond->functype() != Item_func::COND_AND_FUNC) return false;

    List<Item> *args = cond->argument_list();
    if (args->elements < 1 || args->elements > PS_PC_MAX_PARAMS) return false;

    uint idx = 0;
    List_iterator<Item> li(*args);
    Item *sub;
    while ((sub = li++)) {
      if (sub->type() != Item::FUNC_ITEM) return false;
      Item_field *fld = nullptr;
      Item_param *prm = nullptr;
      if (!extract_eq_field_param(down_cast<Item_func *>(sub), tbl, &fld, &prm))
        return false;
      tpl->params[idx] = prm;
      tpl->field_indices[idx] = fld->field_index;
      idx++;
    }
    tpl->param_count = idx;
    return true;
  }

  /* Unsupported WHERE shape (e.g. BETWEEN, IN, OR, etc.). */
  return false;
}

bool ps_point_plan_runtime_guard(THD *, Prepared_statement *, TABLE **,
                                 KEY **) {
  /* Phase 3: implement runtime guard. */
  return false;
}

/**
  Check whether the optimizer's plan qualifies for plan-cache admission.

  This is the core Phase 2 admission gate.  It inspects the finalized
  optimizer output (after push_to_engines()) and decides whether the
  plan is a strict single-table unique-key point lookup that matches
  the template captured in Phase 1.

  @note Why JT_CONST and not JT_EQ_REF?
  For WHERE unique_key = ?, Item_param::used_tables() returns
  INNER_TABLE_BIT.  The JOIN constructor initializes
  found_const_table_map with INNER_TABLE_BIT (sql_optimizer.cc:173),
  so extract_func_dependent_tables() sees the param as "already
  resolved" and marks the table as JT_CONST.  The row is read at
  optimize time via join_read_const_table().  Plan metadata lives
  on qep_tab[0] because join_tab is freed after get_best_combination.

  @note When the const-table read finds no matching row (non-existent
  PK value or NULL parameter), the optimizer sets zero_result_cause
  and short-circuits before reaching our hook.  In that case this
  function is never called and the PS stays COLD — allowing a
  subsequent EXECUTE with a valid parameter to trigger admission.

  @param  stmt  The prepared statement in COLD state.
  @param  join  The JOIN that just finished normal optimization.
  @retval true  Plan qualifies — caller should invoke ps_point_plan_admit().
  @retval false Plan does not qualify — caller should set state to NEVER.
*/
bool ps_point_plan_can_admit(Prepared_statement *stmt, JOIN *join) {
  if (stmt->ps_point_plan_state() != PsPointPlanState::COLD) return false;

  const PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();

  /*
    Check 1: Table topology.
    primary_tables includes const tables.  join->tables may be larger
    due to tmp tables added by make_tmp_tables_info().
    For a single-table point query, expect exactly one primary table
    that is also const (resolved to JT_CONST at optimize time).
  */
  if (join->primary_tables != 1 || join->const_tables != 1)
    return false;

  if (join->qep_tab == nullptr) return false;

  const QEP_TAB *tab = &join->qep_tab[0];

  /* Check 2: access type must be JT_CONST (see @note above). */
  if (tab->type() != JT_CONST) return false;

  /* Check 3: no HAVING — a HAVING clause would add post-read filtering. */
  if (join->having_cond != nullptr) return false;

  TABLE *table = tab->table();
  if (table == nullptr) return false;

  const Index_lookup &ref = tab->ref();

  /*
    Check 4: no subquery-related artifacts.
    cond_guards are set by subquery "Full scan on NULL key" logic;
    keypart_hash is used by subquery materialization.  Both indicate
    the ref lookup is not a simple parameterized point query.
  */
  if (ref.has_guarded_conds()) return false;
  if (ref.keypart_hash != nullptr) return false;

  /* Check 5: ref.key must be a valid index number. */
  if (ref.key < 0 || static_cast<uint>(ref.key) >= table->s->keys)
    return false;

  const KEY *keyinfo = &table->key_info[ref.key];

  /* Check 6: the index must be unique (PK or UNIQUE KEY). */
  if (!(actual_key_flags(keyinfo) & HA_NOSAME)) return false;

  /*
    Check 7: full unique-key coverage.
    user_defined_key_parts excludes any hidden key parts (e.g. the
    DB_ROW_ID in a secondary index).  All user-defined parts of the
    unique key must be covered by ref.key_parts.
  */
  if (keyinfo->user_defined_key_parts != ref.key_parts) return false;

  /*
    Check 8: parameter count consistency.
    ref.key_parts must equal tpl.param_count to ensure every WHERE
    parameter maps to exactly one key part and there are no "extra"
    conditions (e.g. WHERE pk = ? AND val = ?  where val is not part
    of the chosen key would have ref.key_parts=1 != param_count=2).
  */
  if (ref.key_parts != tpl.param_count) return false;

  /*
    Check 9: verify every ref item is one of the Item_param pointers
    captured in the template during Phase 1.

    The comparison uses pointer identity (not value equality).  This
    is safe because v1 scope excludes CTE / derived-table scenarios
    that clone Item_param (via Item_param::add_clone), and
    create_ref_for_key() stores keyuse->val directly for PARAM_ITEM.

    The search is O(n²) where n <= PS_PC_MAX_PARAMS (4), acceptable
    because the WHERE clause order may differ from key-part order
    (e.g. WHERE pk2=? AND pk1=? on PRIMARY KEY(pk1,pk2)).
  */
  for (uint i = 0; i < ref.key_parts; i++) {
    if (ref.items[i] == nullptr ||
        ref.items[i]->type() != Item::PARAM_ITEM)
      return false;

    bool found = false;
    for (uint j = 0; j < tpl.param_count; j++) {
      if (tpl.params[j] == ref.items[i]) {
        found = true;
        break;
      }
    }
    if (!found) return false;
  }

  return true;
}

/**
  Perform plan-cache admission: COLD -> HOT.

  Copies stable plan metadata from the optimizer's QEP_TAB into the
  PsPointPlanTemplate stored on the Prepared_statement.  This metadata
  does not change across executions of the same PS (assuming no DDL).

  Must only be called immediately after ps_point_plan_can_admit()
  returns true — all preconditions (JT_CONST, unique key, etc.)
  are assumed to hold.

  @param  thd   Current thread (for status counter update).
  @param  stmt  The prepared statement to promote to HOT.
  @param  join  The JOIN with finalized plan metadata.
*/
void ps_point_plan_admit(THD *thd, Prepared_statement *stmt, JOIN *join) {
  PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();
  const QEP_TAB *tab = &join->qep_tab[0];
  const Index_lookup &ref = tab->ref();
  TABLE *table = tab->table();
  const KEY *keyinfo = &table->key_info[ref.key];

  /* Copy stable key metadata from the optimizer's ref structure. */
  tpl.keyno = static_cast<uint>(ref.key);
  tpl.key_parts = ref.key_parts;
  tpl.key_length = ref.key_length;
  tpl.null_rejecting = ref.null_rejecting;

  /* Copy optimizer cost estimates for potential use by Phase 3+. */
  tpl.best_read = join->best_read;
  tpl.best_rowcount = static_cast<double>(join->best_rowcount);

  /*
    Reorder params[] and field_indices[] from WHERE-clause order
    (captured in Phase 1 by extract_where_shape) to key-part order.

    ref.items[] follows key-part order because create_ref_for_key()
    fills chosen_keyuses[i] for keypart == i and calls
    init_ref_part(part_no, val, ...) sequentially.

    This reordering is essential for Phase 3+ fast-path, which
    iterates key parts sequentially to build the lookup key buffer.

    Example: PRIMARY KEY (pk1, pk2), WHERE pk2 = ?1 AND pk1 = ?2
      Phase 1: params[0]=?1 (pk2), params[1]=?2 (pk1)
      Phase 2: params[0]=?2 (pk1), params[1]=?1 (pk2)  — key order

    field_indices[] is re-derived from KEY_PART_INFO::fieldnr rather
    than reordering the Phase 1 values, to avoid any ambiguity.
    fieldnr is 1-based; we store 0-based indices.
  */
  for (uint i = 0; i < ref.key_parts; i++) {
    Item_param *prm = down_cast<Item_param *>(ref.items[i]);
    tpl.params[i] = prm;
    tpl.field_indices[i] = keyinfo->key_part[i].fieldnr - 1;
    tpl.actual_types[i] = prm->data_type_actual();
    tpl.unsigned_actuals[i] = prm->is_unsigned_actual();
  }

  stmt->set_ps_point_plan_state(PsPointPlanState::HOT);
  ps_point_plan_mark_admission(thd);
}

void ps_point_plan_mark_hit(THD *thd) {
  thd->status_var.ps_point_plan_cache_hits++;
}

void ps_point_plan_mark_admission(THD *thd) {
  thd->status_var.ps_point_plan_cache_admissions++;
}

void ps_point_plan_mark_invalidation(THD *thd) {
  thd->status_var.ps_point_plan_cache_invalidations++;
}

void ps_point_plan_mark_runtime_fallback(THD *thd) {
  thd->status_var.ps_point_plan_cache_fallback_runtime++;
}

void ps_point_plan_mark_cold_classification(THD *thd) {
  thd->status_var.ps_point_plan_cache_cold_classifications++;
}
