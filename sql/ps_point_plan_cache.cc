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
*/

#include "sql/ps_point_plan_cache.h"

#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_func.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_prepare.h"

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

bool ps_point_plan_classify(THD *thd, Prepared_statement *stmt) {
  if (!thd->variables.ps_point_plan_cache) return false;
  if (stmt->m_param_count < 1 || stmt->m_param_count > PS_PC_MAX_PARAMS)
    return false;

  LEX *lex = stmt->m_lex;
  if (lex->sql_command != SQLCOM_SELECT) return false;
  if (lex->using_hypergraph_optimizer()) return false;

  Query_expression *unit = lex->unit;
  if (unit == nullptr || !unit->is_simple()) return false;

  Query_block *qb = unit->first_query_block();
  if (qb->leaf_table_count != 1) return false;
  if (qb->outer_join != 0) return false;
  if (qb->first_inner_query_expression() != nullptr) return false;
  if (qb->is_grouped() || qb->is_distinct() || qb->is_ordered() ||
      qb->has_limit() || qb->has_windows() || qb->has_ft_funcs())
    return false;

  Table_ref *tbl = qb->leaf_tables;
  if (tbl == nullptr || !tbl->is_base_table()) return false;

  PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();
  tpl = PsPointPlanTemplate{};
  tpl.table_ref = tbl;

  if (!ps_point_plan_extract_where_shape(qb, &tpl)) {
    stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);
    return false;
  }

  if (tpl.param_count != stmt->m_param_count) {
    stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);
    return false;
  }

  stmt->set_ps_point_plan_state(PsPointPlanState::COLD);
  ps_point_plan_mark_cold_classification(thd);
  return true;
}

bool ps_point_plan_extract_where_shape(Query_block *qb,
                                       PsPointPlanTemplate *tpl) {
  Item *where = qb->where_cond();
  if (where == nullptr) return false;

  const Table_ref *tbl = tpl->table_ref;
  tpl->plan_type = PsCachedPlanType::POINT_EQ_REF;
  tpl->param_count = 0;

  if (where->type() == Item::FUNC_ITEM) {
    /*
      Shape A: single equality  WHERE field = ?
    */
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
      Shape B: composite equality  WHERE f1 = ? AND f2 = ? [AND ...]
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

  return false;
}

bool ps_point_plan_runtime_guard(THD *, Prepared_statement *, TABLE **,
                                 KEY **) {
  /* Phase 3: implement runtime guard. */
  return false;
}

bool ps_point_plan_can_admit(Prepared_statement *, JOIN *) {
  /* Phase 2: implement admission check. */
  return false;
}

void ps_point_plan_admit(THD *, Prepared_statement *, JOIN *) {
  /* Phase 2: implement admission. */
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
