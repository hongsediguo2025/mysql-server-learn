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

  Phase 0: skeleton only — all helpers are stubs.
  Phase 1+: real implementations will be added incrementally.
*/

#include "sql/ps_point_plan_cache.h"

#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_prepare.h"

bool ps_point_plan_classify(THD *, Prepared_statement *) {
  /* Phase 1: implement static shape classification. */
  return false;
}

bool ps_point_plan_extract_where_shape(Query_block *,
                                       PsPointPlanTemplate *) {
  /* Phase 1: implement WHERE field = ? extraction.
     Phase 6: extend to composite key AND(field1=?, field2=?, ...).
     Phase 7: extend to field BETWEEN ? AND ?. */
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
