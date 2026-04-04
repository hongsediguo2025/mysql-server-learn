#ifndef SQL_PS_POINT_PLAN_CACHE_H
#define SQL_PS_POINT_PLAN_CACHE_H

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
  @file sql/ps_point_plan_cache.h

  Per-prepared-statement single-slot plan template cache for single-table
  point SELECT queries (v1).  Covers unique-key equality lookups
  (Phase 1-6) and primary-key range scans (Phase 7+).

  This module provides:
  - Static classification at PREPARE time (shape check).
  - Admission after the first normal optimization succeeds.
  - Runtime guard for HOT statements.
  - Status counter helpers.

  The plan cache state machine lives on Prepared_statement; this header
  defines the types, the template struct, and the helper function
  declarations that sql_prepare.cc, sql_optimizer.cc, and sql_executor.cc
  call into.
*/

#include "field_types.h"
#include "my_base.h"
#include "my_inttypes.h"
#include "sql/sql_const.h"

class Item_param;
class Prepared_statement;
class Query_block;
class Table_ref;
class THD;
class JOIN;
class KEY;
struct TABLE;

/**
  State of the per-PS point plan cache slot.

  NEVER   - permanently not a candidate (fast bypass on every execute).
  COLD    - static shape matches; awaiting first execution for admission.
  HOT     - template cached; fast path may be attempted.
  INVALID - template invalidated by structural change; awaits reprepare.
*/
enum class PsPointPlanState : uchar {
  NEVER = 0,
  COLD,
  HOT,
  INVALID
};

/**
  Cached plan type — determines which fast-path builder to invoke.
  Phase 1-6 use POINT_EQ_REF; Phase 7+ adds RANGE_PK_BETWEEN.
*/
enum class PsCachedPlanType : uchar {
  POINT_EQ_REF = 0,
  RANGE_PK_BETWEEN,
};

/// Maximum key parts in a cached plan template.
static constexpr uint PS_PC_MAX_KEY_PARTS = 4;
/// Maximum parameter slots in a cached plan template.
static constexpr uint PS_PC_MAX_PARAMS = 4;

/**
  Minimal plan template for a single-table point SELECT.

  Only stores information that is stable across executions of the same
  prepared statement.  No JOIN*, TABLE*, AccessPath*, Field*, or QEP_TAB*
  are kept here — those are per-execution objects.

  The arrays are sized to accommodate composite unique keys (up to
  PS_PC_MAX_KEY_PARTS columns) and BETWEEN predicates (2 parameters).
  Phase 1-5 use only params[0] / field_indices[0] with param_count == 1.
*/
struct PsPointPlanTemplate {
  Table_ref *table_ref{nullptr};
  PsCachedPlanType plan_type{PsCachedPlanType::POINT_EQ_REF};

  uint param_count{0};
  Item_param *params[PS_PC_MAX_PARAMS]{};
  uint field_indices[PS_PC_MAX_PARAMS]{};
  enum_field_types actual_types[PS_PC_MAX_PARAMS]{};
  bool unsigned_actuals[PS_PC_MAX_PARAMS]{};

  uint keyno{MAX_KEY};
  uint key_parts{0};
  uint key_length{0};
  key_part_map null_rejecting{0};
  double best_read{0.0};
  double best_rowcount{1.0};
};

/*
  -----------------------------------------------------------------------
  Helper functions — declared here, implemented in ps_point_plan_cache.cc.
  Will be filled in during Phase 1–9.
  -----------------------------------------------------------------------
*/

/**
  Classify a freshly prepared statement.  Called once from
  Prepared_statement::prepare() after prepare_query() succeeds.

  Sets the PS plan-cache state to COLD (candidate) or NEVER.
*/
bool ps_point_plan_classify(THD *thd, Prepared_statement *stmt);

/**
  Extract the WHERE shape and populate template fields.
  Phase 1: recognizes single Item_func_eq(field, param).
  Phase 6: recognizes Item_cond_and wrapping multiple Item_func_eq.
  Phase 7+: will also recognize Item_func_between(field, param, param).
  Sets plan_type, param_count, params[], and field_indices[] in @p tpl.
  @return true if a supported shape is found, false otherwise.
*/
bool ps_point_plan_extract_where_shape(Query_block *qb,
                                       PsPointPlanTemplate *tpl);

/**
  Runtime guard — O(1) checks before attempting the fast path.
  @return true if guard passes and fast path may proceed.
*/
bool ps_point_plan_runtime_guard(THD *thd, Prepared_statement *stmt,
                                 TABLE **table_out, KEY **keyinfo_out);

/**
  Check whether the first normal optimization result qualifies for
  admission into the HOT state.
*/
bool ps_point_plan_can_admit(Prepared_statement *stmt, JOIN *join);

/**
  Perform admission: copy stable plan metadata from @p join into the
  template stored on @p stmt, and transition state to HOT.
*/
void ps_point_plan_admit(THD *thd, Prepared_statement *stmt, JOIN *join);

/* Status counter helpers. */
void ps_point_plan_mark_hit(THD *thd);
void ps_point_plan_mark_admission(THD *thd);
void ps_point_plan_mark_invalidation(THD *thd);
void ps_point_plan_mark_runtime_fallback(THD *thd);

#endif  // SQL_PS_POINT_PLAN_CACHE_H
