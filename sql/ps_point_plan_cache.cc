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
  Phase 3: fast path — HOT statements bypass make_join_plan() via
           minimal one-table EQ_REF plan construction.
*/

#include "sql/ps_point_plan_cache.h"

#include <assert.h>
#include <new>
#include <type_traits>

#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_func.h"
#include "sql/item_sum.h"
#include "sql/filesort.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/key.h"
#include "sql/range_optimizer/path_helpers.h"
#include "sql/sql_class.h"
#include "sql/sql_executor.h"
#include "sql/sql_lex.h"
#include "sql/sql_opt_exec_shared.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_prepare.h"
#include "sql/sql_select.h"
#include "sql/table.h"
#include "sql/visible_fields.h"

#ifdef __APPLE__
#include <mach/mach_time.h>
#else
#include <time.h>
#endif

namespace {

constexpr ulonglong kPsPcRelevantOptimizerSwitchMask =
    OPTIMIZER_SWITCH_USE_INVISIBLE_INDEXES;

constexpr ulonglong kPsPcRelevantSqlModeMask =
    MODE_PAD_CHAR_TO_FULL_LENGTH | MODE_INVALID_DATES | MODE_NO_ZERO_DATE |
    MODE_NO_ZERO_IN_DATE | MODE_TIME_TRUNCATE_FRACTIONAL;

ulonglong ps_point_plan_relevant_optimizer_switch(const THD *thd) {
  return thd->variables.optimizer_switch & kPsPcRelevantOptimizerSwitchMask;
}

ulonglong ps_point_plan_relevant_sql_mode(const THD *thd) {
  return thd->variables.sql_mode & kPsPcRelevantSqlModeMask;
}

const CHARSET_INFO *ps_point_plan_actual_collation(const Item_param *param) {
  return is_string_type(param->data_type_actual()) ? param->collation_actual()
                                                   : nullptr;
}

Item_param *ps_point_plan_find_stable_param(const PsPointPlanTemplate &tpl,
                                            const Item_param *param) {
  for (uint i = 0; i < tpl.param_count; i++) {
    if (tpl.params[i] != nullptr &&
        tpl.params[i]->pos_in_query == param->pos_in_query) {
      return tpl.params[i];
    }
  }
  return nullptr;
}

void ps_point_plan_clear_hot_metadata(PsPointPlanTemplate *tpl) {
  tpl->keyno = MAX_KEY;
  tpl->key_parts = 0;
  tpl->key_length = 0;
  tpl->null_rejecting = 0;
  tpl->best_read = 0.0;
  tpl->best_rowcount = 1.0;
  tpl->range_flag = 0;
  tpl->range_rkey_func_flag = HA_READ_INVALID;
  tpl->range_mrr_flags = 0;
  tpl->range_mrr_buf_size = 0;
  tpl->range_need_rows_in_rowid_order = false;
  tpl->range_can_be_used_for_ror = false;
  tpl->range_can_be_used_for_imerge = false;
  tpl->range_reuse_handler = false;
  tpl->range_geometry = false;
  tpl->range_reverse = false;
  tpl->range_using_extended_key_parts = false;
  tpl->optimizer_switch = 0;
  tpl->table_ref_version = 0;
  tpl->relevant_sql_mode = 0;
  /*
    ref_cached / qep_cached stay true if previously built — the
    arena-allocated buffers survive demotion.  A compatibility check
    in ps_point_plan_admit() validates the cached Field clones match
    the new key layout before reuse; mismatches force a rebuild.
    Helper-layout metadata needed by that check is stored separately
    in cached_key_parts/cached_key_length so we can still clear the
    active HOT plan metadata here.
  */
  tpl->has_aggregate = false;
  tpl->aggregate_type = 0;
  tpl->aggregate_field_index = MAX_KEY;
  tpl->aggregate_field_type = MYSQL_TYPE_INVALID;
  tpl->aggregate_field_unsigned = false;

  tpl->has_order_by = false;
  tpl->order_field_index = MAX_KEY;
  tpl->order_direction_desc = false;
  tpl->order_field_type = MYSQL_TYPE_INVALID;
  tpl->order_field_unsigned = false;
  tpl->order_collation = nullptr;

  tpl->has_distinct = false;

  for (uint i = 0; i < PS_PC_MAX_PARAMS; i++) {
    tpl->actual_types[i] = MYSQL_TYPE_INVALID;
    tpl->unsigned_actuals[i] = false;
    tpl->actual_collations[i] = nullptr;
  }
}

void ps_point_plan_demote_to_cold(Prepared_statement *stmt) {
  /* Release plan count from global tracker (HOT → COLD). */
  if (stmt->ps_point_plan_state() == PsPointPlanState::HOT) {
    ps_plan_cache_tracker.remove_plan();
    /* arena_cached_bytes preserved — may be reused on re-admission */
  }
  ps_point_plan_clear_hot_metadata(&stmt->ps_point_plan_template());
  stmt->set_ps_point_plan_state(PsPointPlanState::COLD);
  stmt->set_ps_point_plan_retryable_cold(true);
}

bool ps_point_plan_bind_cached_ref_parts(TABLE *table,
                                         const PsPointPlanTemplate &tpl,
                                         Index_lookup *ref) {
  for (uint i = 0; i < tpl.key_parts; i++) {
    if (tpl.cached_to_fields[i] == nullptr || tpl.cached_store_keys[i] == nullptr)
      return false;

    tpl.cached_to_fields[i]->init(table);
    ref->items[i] = tpl.params[i];
    ref->cond_guards[i] = nullptr;
    ref->key_copy[i] = tpl.cached_store_keys[i];
  }

  return true;
}

bool ps_point_plan_cached_helpers_compatible(const PsPointPlanTemplate &tpl,
                                             const KEY *keyinfo,
                                             uint key_parts, uint key_length,
                                             const uint *field_indices) {
  if (!tpl.ref_cached) return false;
  if (tpl.cached_key_parts != key_parts ||
      tpl.cached_key_length != key_length) {
    return false;
  }

  for (uint i = 0; i < key_parts; i++) {
    const KEY_PART_INFO *kp = &keyinfo->key_part[i];
    if (tpl.cached_to_fields[i] == nullptr ||
        tpl.cached_to_fields[i]->field_index() != field_indices[i] ||
        tpl.cached_part_lengths[i] != kp->length ||
        tpl.cached_part_store_lengths[i] != kp->store_length) {
      return false;
    }
  }

  return true;
}

/**
  Get current monotonic time in seconds.
  Used for last_hit_time / admission_time tracking.
  Cost: ~5ns on modern hardware (vDSO / mach_absolute_time).
*/
uint64_t ps_point_plan_now_seconds() {
#ifdef __APPLE__
  static mach_timebase_info_data_t tb_info = {0, 0};
  if (tb_info.denom == 0) mach_timebase_info(&tb_info);
  uint64_t ns = mach_absolute_time() * tb_info.numer / tb_info.denom;
  return ns / 1000000000ULL;
#else
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
  return static_cast<uint64_t>(ts.tv_sec);
#endif
}

}  // namespace

/**
  Record a HOT hit: update last_hit_time and increment status counter.
  Called from build_fast_path on every successful fast-path construction.
*/
static void ps_point_plan_record_hit(THD *thd,
                                     const PsPointPlanTemplate &tpl) {
  tpl.last_hit_time.store(ps_point_plan_now_seconds(),
                          std::memory_order_relaxed);
  ps_point_plan_mark_hit(thd);
}

/**
  Validate that a query block contains exactly one simple aggregate
  function (SUM/COUNT/MIN/MAX) with no GROUP BY, suitable for
  RANGE_PK_BETWEEN_AGG caching.

  @param  qb   Query block to validate.
  @param  tpl  Template to populate with aggregate metadata.
  @retval true  Valid simple aggregate.
  @retval false Not a cacheable aggregate pattern.
*/
static bool ps_point_plan_validate_simple_aggregate(
    Query_block *qb, PsPointPlanTemplate *tpl) {
  if (qb->group_list.elements > 0) return false;

  uint sum_count = 0;
  for (Item *item : qb->fields) {
    if (item->type() == Item::SUM_FUNC_ITEM) {
      sum_count++;
      if (sum_count > 1) return false;

      Item_sum *sum_item = down_cast<Item_sum *>(item);

      if (sum_item->has_with_distinct()) return false;

      /*
        COUNT(*): internally represented as Item_sum_count with
        arg_count == 1 and get_arg(0) being Item_int.
      */
      if (sum_item->sum_func() == Item_sum::COUNT_FUNC) {
        if (sum_item->arg_count == 1 &&
            sum_item->get_arg(0)->type() == Item::INT_ITEM) {
          tpl->aggregate_type =
              static_cast<uint8>(sum_item->sum_func());
          tpl->aggregate_field_index = MAX_KEY;
          tpl->aggregate_field_type = MYSQL_TYPE_LONGLONG;
          tpl->aggregate_field_unsigned = false;
          continue;
        }
      }

      switch (sum_item->sum_func()) {
        case Item_sum::SUM_FUNC:
        case Item_sum::COUNT_FUNC:
        case Item_sum::MIN_FUNC:
        case Item_sum::MAX_FUNC:
          break;
        default:
          return false;
      }

      if (sum_item->arg_count != 1) return false;
      Item *arg = sum_item->get_arg(0);
      if (arg->type() != Item::FIELD_ITEM) return false;

      Item_field *field = down_cast<Item_field *>(arg);
      if (field->table_ref != tpl->table_ref) return false;

      tpl->aggregate_type =
          static_cast<uint8>(sum_item->sum_func());
      tpl->aggregate_field_index = field->field_index;
      tpl->aggregate_field_type = field->field->type();
      tpl->aggregate_field_unsigned = field->field->is_unsigned();

    } else if (item->type() != Item::INT_ITEM) {
      return false;
    }
  }

  return sum_count == 1;
}

/**
  Validate that a query block has a simple single-column ORDER BY
  suitable for RANGE_PK_BETWEEN_SORT caching.

  Accepts:
    ORDER BY <physical_column> [ASC|DESC]

  Rejects:
    - Multi-column ORDER BY
    - ORDER BY on expressions (e.g., ORDER BY c+1)
    - ORDER BY on columns from a different table
    - ORDER BY NULL / ORDER BY constant

  @param  qb   Query block to validate.
  @param  tpl  Template to populate with ORDER BY metadata.
  @retval true  Valid simple ORDER BY.
  @retval false Not a cacheable ORDER BY pattern.
*/
static bool ps_point_plan_validate_simple_order_by(
    Query_block *qb, PsPointPlanTemplate *tpl) {
  ORDER *order = qb->order_list.first;
  if (order == nullptr) return false;

  if (order->next != nullptr) return false;

  Item *item = order->item[0]->real_item();
  if (item->type() != Item::FIELD_ITEM) return false;

  Item_field *field = down_cast<Item_field *>(item);
  if (field->table_ref != tpl->table_ref) return false;

  if (field->field == nullptr) return false;

  tpl->order_field_index = field->field_index;
  tpl->order_direction_desc = (order->direction == ORDER_DESC);
  tpl->order_field_type = field->field->type();
  tpl->order_field_unsigned = field->field->is_unsigned();
  tpl->order_collation = field->field->charset();

  return true;
}

/**
  Validate that a query block has a simple DISTINCT suitable for
  RANGE_PK_BETWEEN_SORT_DISTINCT caching.

  Accepts:
    SELECT DISTINCT <single_physical_column> ... ORDER BY <same_column>

  Rejects:
    - Multi-column DISTINCT (SELECT DISTINCT c, k ...)
    - DISTINCT column differs from ORDER BY column
    - DISTINCT on expressions
    - DISTINCT without prior ORDER BY validation

  @param  qb   Query block to validate.
  @param  tpl  Template with ORDER BY metadata already populated.
  @retval true  Valid simple DISTINCT.
  @retval false Not a cacheable DISTINCT pattern.
*/
static bool ps_point_plan_validate_simple_distinct(
    Query_block *qb, PsPointPlanTemplate *tpl) {
  if (!tpl->has_order_by) return false;

  uint visible_count = 0;
  for (Item *item : VisibleFields(qb->fields)) {
    visible_count++;
    if (visible_count > 1) return false;

    Item *real = item->real_item();
    if (real->type() != Item::FIELD_ITEM) return false;

    Item_field *field = down_cast<Item_field *>(real);
    if (field->table_ref != tpl->table_ref) return false;
    if (field->field == nullptr) return false;
    if (field->field_index >= field->table_ref->table->s->fields) return false;
    if (field->field_index != tpl->order_field_index) return false;
  }
  return visible_count == 1;
}

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
  Try to extract a single field BETWEEN ? AND ? predicate.

  Accepts only the non-negated canonical form:
    field BETWEEN ? AND ?

  @param      between_item The BETWEEN item to inspect.
  @param      tbl          The target table — field must belong to it.
  @param[out] field_out    Receives the Item_field pointer on success.
  @param[out] low_out      Receives the lower-bound Item_param on success.
  @param[out] high_out     Receives the upper-bound Item_param on success.
  @return true on success, false if pattern does not match.
*/
static bool extract_between_field_params(Item_func *between_item,
                                         const Table_ref *tbl,
                                         Item_field **field_out,
                                         Item_param **low_out,
                                         Item_param **high_out) {
  if (between_item->functype() != Item_func::BETWEEN) return false;
  if (between_item->argument_count() != 3) return false;

  auto *between = down_cast<Item_func_between *>(between_item);
  if (between->negated) return false;

  Item *field_arg = between_item->arguments()[0];
  Item *low_arg = between_item->arguments()[1];
  Item *high_arg = between_item->arguments()[2];

  if (field_arg->type() != Item::FIELD_ITEM) return false;
  if (low_arg->type() != Item::PARAM_ITEM) return false;
  if (high_arg->type() != Item::PARAM_ITEM) return false;

  Item_field *field = down_cast<Item_field *>(field_arg);
  if (field->table_ref != tbl) return false;

  *field_out = field;
  *low_out = down_cast<Item_param *>(low_arg);
  *high_out = down_cast<Item_param *>(high_arg);
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
  stmt->set_ps_point_plan_retryable_cold(false);

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

  /* Gate 4: no LIMIT, window functions, FULLTEXT. */
  if (qb->has_limit() || qb->has_windows() || qb->has_ft_funcs())
    return false;

  /*
    Gate 4-pre: DISTINCT requires ORDER BY on the same column.
    DISTINCT without ORDER BY or with aggregates is not supported —
    the fast path relies on Filesort(remove_duplicates=true) which
    needs a sort key matching the DISTINCT key.
  */
  if (qb->is_distinct() && (!qb->is_ordered() || qb->agg_func_used())) {
    stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);
    return false;
  }

  /*
    Gate 4a: ORDER BY is only allowed for non-aggregate BETWEEN patterns.
    ORDER BY + aggregate (e.g. SELECT SUM(k) ... ORDER BY c) is not
    supported — the optimizer produces a different access path shape
    that we cannot reconstruct in the fast path.
  */
  if (qb->is_ordered() && qb->agg_func_used()) {
    stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);
    return false;
  }

  /* Reject explicit GROUP BY (implicit grouping via agg is OK). */
  if (qb->group_list.elements > 0) return false;

  Table_ref *tbl = qb->leaf_tables;
  if (tbl == nullptr || !tbl->is_base_table()) return false;

  /* Gate 4b: TABLE must be open; exclude partitioned tables. */
  if (tbl->table == nullptr) return false;
  if (tbl->table->part_info != nullptr) return false;

  /* Gate 5: extract WHERE shape (field=? equalities). */
  PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();
  /*
    Placement-new to reinitialize the template to default state.
    Direct assignment (tpl = PsPointPlanTemplate{}) is not possible
    because std::atomic<uint64_t> last_hit_time deletes the copy
    assignment operator.  PsPointPlanTemplate is trivially destructible
    so skipping the explicit destructor call is safe.
  */
  new (&tpl) PsPointPlanTemplate{};
  tpl.table_ref = tbl;

  /*
    Gate 4c: allow simple aggregates (no GROUP BY, single SUM/COUNT/MIN/MAX).
    Must be after tpl initialization since validate accesses tpl->table_ref.
  */
  if (qb->agg_func_used()) {
    if (!ps_point_plan_validate_simple_aggregate(qb, &tpl)) {
      stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);
      return false;
    }
    tpl.has_aggregate = true;
  }

  /*
    Gate 4d: validate simple ORDER BY shape.
    Must be after tpl initialization since validate accesses tpl->table_ref.
    Must be after Gate 4a (ORDER BY + aggregate rejection).
  */
  if (qb->is_ordered()) {
    if (!ps_point_plan_validate_simple_order_by(qb, &tpl)) {
      stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);
      return false;
    }
    tpl.has_order_by = true;
  }

  /*
    Gate 4e: validate simple DISTINCT.
    Requires single-column SELECT list matching the ORDER BY column.
    Must be after Gate 4d (ORDER BY validation) since validate reads
    tpl->has_order_by and tpl->order_field_index.
  */
  if (qb->is_distinct()) {
    if (!ps_point_plan_validate_simple_distinct(qb, &tpl)) {
      stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);
      return false;
    }
    tpl.has_distinct = true;
  }

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

  Recognizes three supported shapes:

  - Shape A (single equality):
    @code WHERE field = ? @endcode
    Represented as a top-level Item_func_eq (FUNC_ITEM).

  - Shape B (composite equality conjunction):
    @code WHERE f1 = ? AND f2 = ? [AND ...] @endcode
    Represented as Item_cond_and (COND_ITEM) wrapping 2..MAX_PARAMS
    Item_func_eq items.

  - Shape C (simple primary-key range candidate):
    @code WHERE field BETWEEN ? AND ? @endcode
    Represented as a top-level Item_func_between (FUNC_ITEM).

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
    Item_func *func = down_cast<Item_func *>(where);

    /* Shape C: simple range candidate  WHERE field BETWEEN ? AND ? */
    Item_field *between_field = nullptr;
    Item_param *low = nullptr;
    Item_param *high = nullptr;
    if (extract_between_field_params(func, tbl, &between_field, &low, &high)) {
      if (qb->agg_func_used()) {
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN_AGG;
      } else if (qb->is_distinct()) {
        assert(qb->is_ordered());
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN_SORT_DISTINCT;
      } else if (qb->is_ordered()) {
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN_SORT;
      } else {
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN;
      }
      tpl->params[0] = low;
      tpl->params[1] = high;
      /*
        Both params (low, high) bind to the same PK field, so
        field_indices[0] == field_indices[1] is intentional.
        G5 loops over key_parts (= 1 for single-column PK),
        so only field_indices[0] is checked at runtime.
      */
      tpl->field_indices[0] = between_field->field_index;
      tpl->field_indices[1] = between_field->field_index;
      tpl->param_count = 2;
      return true;
    }

    /* Shape A: single equality  WHERE field = ? */
    Item_field *fld = nullptr;
    Item_param *prm = nullptr;
    if (!extract_eq_field_param(func, tbl, &fld, &prm)) return false;
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

  /* Unsupported WHERE shape (e.g. IN, OR, mixed predicates, etc.). */
  return false;
}

bool ps_point_plan_runtime_guard(THD *thd, Prepared_statement *stmt,
                                 TABLE **table_out, KEY **keyinfo_out) {
  const PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();

  /*
    Structural guards — if any fails, the template is no longer valid
    and must be invalidated.  Invalidation demotes to COLD (non-retryable)
    with cache flags reset, so the next execution goes through the normal
    optimizer and can re-admit with fresh cached components.
  */

  /* G1: TABLE binding must be live. */
  if (tpl.table_ref == nullptr || tpl.table_ref->table == nullptr) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  TABLE *table = tpl.table_ref->table;

  /* G1b: TABLE_SHARE must be present. */
  if (table->s == nullptr) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  /*
    G1c: retryable environment drift — optimizer_switch.
    Does not make the statement permanently invalid; instead we
    demote HOT -> COLD so the current execution can re-optimize and
    potentially re-admit with refreshed metadata.
  */
  if (ps_point_plan_relevant_optimizer_switch(thd) != tpl.optimizer_switch) {
    ps_point_plan_demote_to_cold(stmt);
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  /*
    G1d: table_ref_version drift.
    table_ref_version tracks TABLE_SHARE identity (table_map_id),
    which changes when the share is reloaded into TDC.  This can
    reflect DDL that alters column definitions (charset, nullability,
    pack_length) without changing the column ordinal.  Use hard
    invalidation (cache flags reset) to ensure Field clones and
    key buffers are rebuilt, since field_index()-based compatibility
    checks cannot detect definition-level changes.

    In practice this guard rarely fires because
    check_and_update_table_version() in open_tables_for_query()
    detects version changes earlier and triggers reprepare, which
    swaps away the entire plan cache state.
  */
  if (table->s->get_table_ref_version() != tpl.table_ref_version) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  /* G11: sql_mode bits affecting comparison semantics. */
  if (ps_point_plan_relevant_sql_mode(thd) != tpl.relevant_sql_mode) {
    ps_point_plan_demote_to_cold(stmt);
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  /* G2: key index must still be within bounds. */
  if (tpl.keyno >= table->s->keys) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  KEY *keyinfo = &table->key_info[tpl.keyno];

  /* G3: key must still be unique. */
  if (!(actual_key_flags(keyinfo) & HA_NOSAME)) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  /* G4: key part count must still match. */
  if (keyinfo->user_defined_key_parts != tpl.key_parts) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  /* G5: field ordinals for each key part must still match. */
  for (uint i = 0; i < tpl.key_parts; i++) {
    if (keyinfo->key_part[i].fieldnr - 1 != tpl.field_indices[i]) {
      stmt->invalidate_ps_point_plan_cache();
      ps_point_plan_mark_invalidation(thd);
      return false;
    }
  }

  /*
    Parameter guards — per-execution checks.
    If any fails, this execution falls back to normal path but
    the template stays HOT for future executions.
  */

  for (uint i = 0; i < tpl.param_count; i++) {
    /* G6: param pointer sanity. */
    if (tpl.params[i] == nullptr) {
      stmt->invalidate_ps_point_plan_cache();
      ps_point_plan_mark_invalidation(thd);
      return false;
    }

    /* G7: NULL parameter → runtime fallback. */
    if (tpl.params[i]->param_state() == Item_param::NULL_VALUE) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    /* G8: parameter actual type must match admission snapshot. */
    if (tpl.params[i]->data_type_actual() != tpl.actual_types[i]) {
      ps_point_plan_demote_to_cold(stmt);
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    /* G9: unsigned flag must match admission snapshot. */
    if (tpl.params[i]->is_unsigned_actual() != tpl.unsigned_actuals[i]) {
      ps_point_plan_demote_to_cold(stmt);
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    /* G10: string collation drift must be re-optimized. */
    if (ps_point_plan_actual_collation(tpl.params[i]) !=
        tpl.actual_collations[i]) {
      ps_point_plan_demote_to_cold(stmt);
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
  }

  /* G12: aggregate field type drift detection. */
  if (tpl.has_aggregate && tpl.aggregate_field_index != MAX_KEY) {
    if (tpl.aggregate_field_index >= table->s->fields) {
      stmt->invalidate_ps_point_plan_cache();
      ps_point_plan_mark_invalidation(thd);
      return false;
    }
    Field *agg_field = table->field[tpl.aggregate_field_index];
    if (agg_field == nullptr ||
        agg_field->type() != tpl.aggregate_field_type ||
        agg_field->is_unsigned() != tpl.aggregate_field_unsigned) {
      stmt->invalidate_ps_point_plan_cache();
      ps_point_plan_mark_invalidation(thd);
      return false;
    }
  }

  /* G13: ORDER BY field type / collation drift detection. */
  if (tpl.has_order_by && tpl.order_field_index != MAX_KEY) {
    if (tpl.order_field_index >= table->s->fields) {
      stmt->invalidate_ps_point_plan_cache();
      ps_point_plan_mark_invalidation(thd);
      return false;
    }
    Field *order_field = table->field[tpl.order_field_index];
    if (order_field == nullptr ||
        order_field->type() != tpl.order_field_type ||
        order_field->is_unsigned() != tpl.order_field_unsigned) {
      stmt->invalidate_ps_point_plan_cache();
      ps_point_plan_mark_invalidation(thd);
      return false;
    }
    if (order_field->charset() != tpl.order_collation) {
      stmt->invalidate_ps_point_plan_cache();
      ps_point_plan_mark_invalidation(thd);
      return false;
    }
  }

  *table_out = table;
  *keyinfo_out = keyinfo;
  return true;
}

/**
  Build an INDEX_RANGE_SCAN AccessPath and QEP_TAB for a single-column
  PK BETWEEN range scan.

  Shared by RANGE_PK_BETWEEN and RANGE_PK_BETWEEN_AGG fast paths.
  Handles both arena-cached and per-execution allocation strategies.

  @param  thd     Current thread.
  @param  join    The JOIN being optimized.
  @param  tpl     The plan template with cached range metadata.
  @param  table   The resolved TABLE.
  @param  keyinfo The KEY descriptor for the PK.
  @param[out] out_qep_tab  Receives the constructed QEP_TAB array.
  @param[out] out_range_path Receives the INDEX_RANGE_SCAN AccessPath.
  @retval true  Range path constructed successfully.
  @retval false Construction failed; caller should fall back.
*/
static bool ps_point_plan_build_range_components(
    THD *thd, JOIN *join, const PsPointPlanTemplate &tpl,
    TABLE *table, KEY *keyinfo,
    QEP_TAB **out_qep_tab, AccessPath **out_range_path) {
  const KEY_PART_INFO *key_part = &keyinfo->key_part[0];
  const uint key_bytes = key_part->store_length;
  const key_part_map keypart_map = static_cast<key_part_map>(1);

  QEP_TAB *new_qep_tab;
  QEP_shared *qs;
  KEY_PART *used_key_part;
  QUICK_RANGE **ranges;
  uchar *min_key;
  uchar *max_key;

  if (tpl.range_arena_cached) {
    static_assert(std::is_trivially_destructible<QEP_shared>::value,
                  "QEP_shared must be trivially destructible");
    static_assert(std::is_trivially_destructible<QEP_TAB>::value,
                  "QEP_TAB must be trivially destructible");

    new_qep_tab = tpl.cached_range_qep_tab;
    new (tpl.cached_range_qep_shared) QEP_shared();
    new (&new_qep_tab[0]) QEP_TAB();

    qs = tpl.cached_range_qep_shared;
    used_key_part = tpl.cached_range_key_part;
    ranges = tpl.cached_range_array;
    min_key = tpl.cached_range_min_key;
    max_key = tpl.cached_range_max_key;

    tpl.cached_range_to_fields[0]->init(table);
    tpl.cached_range_to_fields[1]->init(table);

    if (tpl.cached_range_low_store->copy() != store_key::STORE_KEY_OK ||
        tpl.cached_range_high_store->copy() != store_key::STORE_KEY_OK ||
        thd->is_error()) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
  } else {
    const bool nullable = key_part->null_bit != 0;

    min_key = thd->mem_root->ArrayAlloc<uchar>(key_bytes + 1);
    max_key = thd->mem_root->ArrayAlloc<uchar>(key_bytes + 1);
    used_key_part = thd->mem_root->ArrayAlloc<KEY_PART>(1);
    ranges = thd->mem_root->ArrayAlloc<QUICK_RANGE *>(1);
    new_qep_tab = new (thd->mem_root) QEP_TAB[2];
    qs = new (thd->mem_root) QEP_shared;
    if (min_key == nullptr || max_key == nullptr ||
        used_key_part == nullptr || ranges == nullptr ||
        new_qep_tab == nullptr || qs == nullptr) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
    memset(min_key, 0, key_bytes + 1);
    memset(max_key, 0, key_bytes + 1);

    store_key low_store(thd, key_part->field, min_key + nullable,
                        nullable ? min_key : nullptr, key_part->length,
                        tpl.params[0]);
    store_key high_store(thd, key_part->field, max_key + nullable,
                         nullable ? max_key : nullptr, key_part->length,
                         tpl.params[1]);
    if (low_store.copy() != store_key::STORE_KEY_OK ||
        high_store.copy() != store_key::STORE_KEY_OK || thd->is_error()) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
  }

  if (key_cmp2(&keyinfo->key_part[0], min_key, key_bytes, max_key,
               key_bytes) > 0) {
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  QUICK_RANGE *range = new (thd->mem_root)
      QUICK_RANGE(thd->mem_root, min_key, key_bytes, keypart_map, max_key,
                  key_bytes, keypart_map, tpl.range_flag,
                  tpl.range_rkey_func_flag);
  AccessPath *range_path = new (thd->mem_root) AccessPath{};
  if (range == nullptr || range_path == nullptr) {
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }
  ranges[0] = range;
  used_key_part[0].key = 0;
  used_key_part[0].part = 0;
  used_key_part[0].store_length = key_part->store_length;
  used_key_part[0].length = key_part->length;
  used_key_part[0].null_bit = key_part->null_bit;
  used_key_part[0].flag = key_part->key_part_flag;
  used_key_part[0].field = key_part->field;
  used_key_part[0].image_type = Field::itRAW;

  range_path->type = AccessPath::INDEX_RANGE_SCAN;
  range_path->count_examined_rows = true;
  range_path->init_cost = 0.0;
  range_path->init_once_cost = 0.0;
  range_path->cost = range_path->cost_before_filter = tpl.best_read;
  range_path->set_num_output_rows(tpl.best_rowcount);
  range_path->num_output_rows_before_filter = tpl.best_rowcount;
  range_path->index_range_scan().index = tpl.keyno;
  range_path->index_range_scan().num_used_key_parts = 1;
  range_path->index_range_scan().used_key_part = used_key_part;
  range_path->index_range_scan().ranges = ranges;
  range_path->index_range_scan().num_ranges = 1;
  range_path->index_range_scan().mrr_flags = tpl.range_mrr_flags;
  range_path->index_range_scan().mrr_buf_size = tpl.range_mrr_buf_size;
  range_path->index_range_scan().can_be_used_for_ror =
      tpl.range_can_be_used_for_ror;
  range_path->index_range_scan().need_rows_in_rowid_order =
      tpl.range_need_rows_in_rowid_order;
  range_path->index_range_scan().can_be_used_for_imerge =
      tpl.range_can_be_used_for_imerge;
  range_path->index_range_scan().reuse_handler = tpl.range_reuse_handler;
  range_path->index_range_scan().geometry = tpl.range_geometry;
  range_path->index_range_scan().reverse = tpl.range_reverse;
  range_path->index_range_scan().using_extended_key_parts =
      tpl.range_using_extended_key_parts;

  QEP_TAB *tab = &new_qep_tab[0];
  tab->set_qs(qs);
  tab->set_join(join);
  tab->set_idx(0);
  tab->set_table(table);
  tab->table_ref = tpl.table_ref;
  tab->set_type(JT_RANGE);
  tab->set_condition(nullptr);
  tab->set_range_scan(range_path);

  *out_qep_tab = new_qep_tab;
  *out_range_path = range_path;
  return true;
}

/**
  Commit range plan components to the JOIN object.

  @param  join       The JOIN to finalize.
  @param  tpl        The plan template.
  @param  new_qep_tab The QEP_TAB array from range construction.
  @param  root_path  The root access path (INDEX_RANGE_SCAN or AGGREGATE).
*/
static void ps_point_plan_commit_range_to_join(
    JOIN *join, const PsPointPlanTemplate &tpl,
    QEP_TAB *new_qep_tab, AccessPath *root_path) {
  join->tables = 1;
  join->primary_tables = 1;
  join->const_tables = 0;
  join->best_read = tpl.best_read;
  join->best_rowcount = static_cast<ha_rows>(tpl.best_rowcount);
  join->where_cond = nullptr;
  join->having_cond = nullptr;
  join->qep_tab = new_qep_tab;
  join->set_root_access_path(root_path);
}

/**
  Construct a minimal one-table EQ_REF execution plan for a HOT
  prepared statement, bypassing the full optimizer pipeline.

  @pre  thd->variables.ps_point_plan_cache == true
  @pre  stmt->ps_point_plan_state() == PsPointPlanState::HOT
  @pre  !stmt->ps_point_plan_cursor_execution()
  @pre  !thd->lex->using_hypergraph_optimizer()

  @param  thd   Current thread.
  @param  join  The JOIN being optimized.
  @param  stmt  The owning Prepared_statement (HOT state).

  @retval true  Fast path plan constructed; caller should set
                PLAN_READY and return.
  @retval false Fast path declined; caller should continue to
                make_join_plan() (JOIN state is untouched).
*/
bool ps_point_plan_build_fast_path(THD *thd, JOIN *join,
                                   Prepared_statement *stmt) {
  const PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();

  /*
    MANDATORY INVARIANT: Do NOT modify the following JOIN members until ALL
    construction steps have succeeded:
      tables, primary_tables, const_tables, where_cond, having_cond,
      qep_tab, best_read, best_rowcount, m_root_access_path

    EXCEPTION for RANGE_PK_BETWEEN_AGG:
      The aggregate fast path modifies join->tmp_table_param and join->sum_funcs
      before range construction (count_field_types, alloc_func_list,
      make_sum_func_list, prepare_sum_aggregators, setup_sum_funcs). This is
      safe because:
      - count_field_types() overwrites tmp_table_param (same as normal path)
      - alloc_func_list() allocates on thd->mem_root, freed on query end
      - If any step fails, we fall back to normal optimizer which re-runs
        the same initialization sequence.

    Rationale for the invariant:
      - init_planner_arrays() asserts primary_tables == 0 && tables == 0.
        Violating this crashes debug builds.
      - V1.2: the fast path hook fires before get_optimizable_conditions(),
        so where_cond has not been set yet.  On failure we fall through
        to the normal preamble which will set it correctly.
  */

  /* --- Phase A: Runtime guard (read-only, no JOIN mutation) --- */
  TABLE *table = nullptr;
  KEY *keyinfo = nullptr;
  if (!ps_point_plan_runtime_guard(thd, stmt, &table, &keyinfo))
    return false;

  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_AGG) {
    /*
      Initialize aggregate function machinery.  The fast path fires
      before count_field_types / alloc_func_list in JOIN::optimize(),
      so join->sum_funcs is nullptr.  AggregateIterator::Read()
      dereferences sum_funcs, causing a crash without this init.

      count_field_types returns void — no error check needed.
    */
    count_field_types(join->query_block, &join->tmp_table_param,
                      *join->fields, false, false);
    if (join->alloc_func_list()) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
    if (join->make_sum_func_list(*join->fields, false)) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
    if (prepare_sum_aggregators(join->sum_funcs, /*need_distinct=*/false)) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
    if (setup_sum_funcs(thd, join->sum_funcs)) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    assert(join->implicit_grouping == true);
    assert(join->grouped == false);
    assert(join->group_optimized_away == false);

    QEP_TAB *new_qep_tab = nullptr;
    AccessPath *range_path = nullptr;
    if (!ps_point_plan_build_range_components(thd, join, tpl, table, keyinfo,
                                              &new_qep_tab, &range_path))
      return false;

    AccessPath *agg_path =
        NewAggregateAccessPath(thd, range_path, /*rollup=*/false);
    if (agg_path == nullptr) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
    agg_path->set_num_output_rows(1.0);
    agg_path->cost = range_path->cost;

    ps_point_plan_commit_range_to_join(join, tpl, new_qep_tab, agg_path);
    ps_point_plan_record_hit(thd, tpl);
    return true;
  }

  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_SORT) {
    QEP_TAB *new_qep_tab = nullptr;
    AccessPath *range_path = nullptr;
    if (!ps_point_plan_build_range_components(thd, join, tpl, table, keyinfo,
                                              &new_qep_tab, &range_path))
      return false;

    Filesort *filesort = new (thd->mem_root)
        Filesort(thd, {table}, /*keep_buffers=*/false, join->order.order,
                 /*limit_arg=*/HA_POS_ERROR,
                 /*remove_duplicates=*/false,
                 /*force_sort_rowids=*/false,
                 /*unwrap_rollup=*/false);
    if (filesort == nullptr || filesort->sortorder == nullptr) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    AccessPath *sort_path =
        NewSortAccessPath(thd, range_path, filesort, join->order.order,
                          /*count_examined_rows=*/true);
    if (sort_path == nullptr) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    ps_point_plan_commit_range_to_join(join, tpl, new_qep_tab, sort_path);
    ps_point_plan_record_hit(thd, tpl);
    return true;
  }

  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_SORT_DISTINCT) {
    QEP_TAB *new_qep_tab = nullptr;
    AccessPath *range_path = nullptr;
    if (!ps_point_plan_build_range_components(thd, join, tpl, table, keyinfo,
                                              &new_qep_tab, &range_path))
      return false;

    Filesort *filesort = new (thd->mem_root)
        Filesort(thd, {table}, /*keep_buffers=*/false, join->order.order,
                 /*limit_arg=*/HA_POS_ERROR,
                 /*remove_duplicates=*/true,
                 /*force_sort_rowids=*/false,
                 /*unwrap_rollup=*/false);
    if (filesort == nullptr || filesort->sortorder == nullptr) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    AccessPath *sort_path =
        NewSortAccessPath(thd, range_path, filesort, join->order.order,
                          /*count_examined_rows=*/true);
    if (sort_path == nullptr) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    ps_point_plan_commit_range_to_join(join, tpl, new_qep_tab, sort_path);
    ps_point_plan_record_hit(thd, tpl);
    return true;
  }

  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN) {
    QEP_TAB *new_qep_tab = nullptr;
    AccessPath *range_path = nullptr;
    if (!ps_point_plan_build_range_components(thd, join, tpl, table, keyinfo,
                                              &new_qep_tab, &range_path))
      return false;

    ps_point_plan_commit_range_to_join(join, tpl, new_qep_tab, range_path);
    ps_point_plan_record_hit(thd, tpl);
    return true;
  }

  if (tpl.plan_type != PsCachedPlanType::POINT_EQ_REF) return false;

  /* --- Phase B: Construct plan in local variables --- */

  QEP_TAB *new_qep_tab;
  QEP_TAB *tab;

  if (tpl.qep_cached && tpl.ref_cached) {
    /*
      V1.2 fully-cached path: reuse arena QEP_TAB, QEP_shared,
      pointer arrays, key buffers, and store_key objects.
      Only re-bind per-execution pointers (join, table) and
      serialize current parameter values via store_key::copy().

      Placement-new reinitializes the cached objects to their
      default-constructed state, resetting QEP_shared::m_idx and
      QEP_shared_owner::m_qs which have one-shot assertions in
      set_idx() / set_qs().  The arena memory is not freed.

      Both types are trivially destructible so skipping the explicit
      destructor call before placement-new is safe and well-defined.
      The static_asserts guard against future regressions.
    */
    static_assert(std::is_trivially_destructible<QEP_shared>::value,
                  "QEP_shared must be trivially destructible for "
                  "placement-new reuse without explicit dtor call");
    static_assert(std::is_trivially_destructible<QEP_TAB>::value,
                  "QEP_TAB must be trivially destructible for "
                  "placement-new reuse without explicit dtor call");
    new_qep_tab = tpl.cached_qep_tab;
    new (tpl.cached_qep_shared) QEP_shared();
    new (&new_qep_tab[0]) QEP_TAB();
    tab = &new_qep_tab[0];
    tab->set_qs(tpl.cached_qep_shared);
    tab->set_join(join);
    tab->set_idx(0);
    tab->set_table(table);
    tab->table_ref = tpl.table_ref;
    tab->set_type(JT_EQ_REF);

    Index_lookup &ref = tab->ref();
    ref.key_parts = tpl.key_parts;
    ref.key_length = tpl.key_length;
    ref.key = static_cast<int>(tpl.keyno);
    ref.key_buff = tpl.cached_key_buff;
    ref.key_buff2 = tpl.cached_key_buff2;
    ref.key_err = true;
    ref.null_rejecting = tpl.null_rejecting;
    ref.use_count = 0;
    ref.disable_cache = false;
    ref.null_ref_key = nullptr;
    ref.depend_map = 0;
    ref.key_copy = tpl.cached_key_copy;
    ref.items = tpl.cached_ref_items;
    ref.cond_guards = tpl.cached_cond_guards;

    if (!ps_point_plan_bind_cached_ref_parts(table, tpl, &ref)) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
  } else if (tpl.ref_cached) {
    /*
      V1.1 ref-cached path: reuse arena key buffers and store_key
      objects, but allocate QEP_TAB and pointer arrays per-execution.
    */
    new_qep_tab = new (thd->mem_root) QEP_TAB[2];
    if (new_qep_tab == nullptr) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
    QEP_shared *qs = new (thd->mem_root) QEP_shared;
    if (qs == nullptr) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    tab = &new_qep_tab[0];
    tab->set_qs(qs);
    tab->set_join(join);
    tab->set_idx(0);
    tab->set_table(table);
    tab->table_ref = tpl.table_ref;
    tab->set_type(JT_EQ_REF);

    Index_lookup &ref = tab->ref();
    ref.key_parts = tpl.key_parts;
    ref.key_length = tpl.key_length;
    ref.key = static_cast<int>(tpl.keyno);
    ref.key_buff = tpl.cached_key_buff;
    ref.key_buff2 = tpl.cached_key_buff2;
    ref.key_err = true;
    ref.null_rejecting = tpl.null_rejecting;
    ref.use_count = 0;
    ref.disable_cache = false;
    ref.null_ref_key = nullptr;
    ref.depend_map = 0;

    ref.key_copy = thd->mem_root->ArrayAlloc<store_key *>(tpl.key_parts);
    ref.items = thd->mem_root->ArrayAlloc<Item *>(tpl.key_parts);
    ref.cond_guards = thd->mem_root->ArrayAlloc<bool *>(tpl.key_parts);
    if (ref.key_copy == nullptr || ref.items == nullptr ||
        ref.cond_guards == nullptr) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    if (!ps_point_plan_bind_cached_ref_parts(table, tpl, &ref)) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
  } else {
    /* Slow path: no arena cache available, full construction. */
    new_qep_tab = new (thd->mem_root) QEP_TAB[2];
    if (new_qep_tab == nullptr) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
    QEP_shared *qs = new (thd->mem_root) QEP_shared;
    if (qs == nullptr) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    tab = &new_qep_tab[0];
    tab->set_qs(qs);
    tab->set_join(join);
    tab->set_idx(0);
    tab->set_table(table);
    tab->table_ref = tpl.table_ref;
    tab->set_type(JT_EQ_REF);

    if (init_ref(thd, tpl.key_parts, tpl.key_length, tpl.keyno,
                 &tab->ref())) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    uchar *key_buff = tab->ref().key_buff;
    for (uint i = 0; i < tpl.key_parts; i++) {
      const KEY_PART_INFO *key_part = &keyinfo->key_part[i];
      const bool null_rej = (tpl.null_rejecting >> i) & 1;

      if (init_ref_part(thd, i, tpl.params[i],
                        /*cond_guard=*/nullptr, null_rej,
                        /*const_tables=*/0,
                        tpl.params[i]->used_tables(),
                        key_part->null_bit != 0,
                        key_part, key_buff, &tab->ref())) {
        ps_point_plan_mark_runtime_fallback(thd);
        return false;
      }
      key_buff += key_part->store_length;
    }
    assert(key_buff == tab->ref().key_buff + tpl.key_length);
  }

  /* B4: Create AccessPath (always per-execution). */
  AccessPath *path =
      NewEQRefAccessPath(thd, table, &tab->ref(), /*count_examined_rows=*/true);
  if (path == nullptr) {
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  path->set_num_output_rows(tpl.best_rowcount);
  path->cost = tpl.best_read;
  path->init_cost = 0.0;
  path->init_once_cost = 0.0;

  /*
    --- Phase C: ALL construction succeeded — commit to JOIN ---

    This is the ONLY place where JOIN members are modified.
    If any step above failed and returned false, we reach here with
    the JOIN completely untouched, so fallback to the normal optimizer
    preamble proceeds with correct state.
  */
  join->tables = 1;
  join->primary_tables = 1;
  join->const_tables = 0;
  join->best_read = tpl.best_read;
  join->best_rowcount = static_cast<ha_rows>(tpl.best_rowcount);
  join->where_cond = nullptr;
  join->having_cond = nullptr;
  join->qep_tab = new_qep_tab;
  join->set_root_access_path(path);

  ps_point_plan_record_hit(thd, tpl);
  return true;
}

/**
  Common admission checks shared by RANGE_PK_BETWEEN and RANGE_PK_BETWEEN_AGG.

  Verifies table topology, QEP_TAB access type, primary-key range scan
  structure, unique single-column PK constraints, and parameter validity.

  @param  tpl   The plan template (param_count must be 2).
  @param  join  The JOIN that just finished normal optimization.
  @retval true  All common range-between checks pass.
  @retval false Not eligible for admission.
*/
static bool ps_point_plan_can_admit_range_between(
    const PsPointPlanTemplate &tpl, JOIN *join) {
  if (tpl.param_count != 2) return false;
  if (join->primary_tables != 1) return false;
  if (join->qep_tab == nullptr) return false;

  const QEP_TAB *tab = &join->qep_tab[0];
  if (tab->type() != JT_RANGE) return false;
  if (join->having_cond != nullptr) return false;

  TABLE *table = tab->table();
  if (table == nullptr || table->s == nullptr) return false;
  if (table->s->primary_key == MAX_KEY) return false;

  AccessPath *range_scan = tab->range_scan();
  if (range_scan == nullptr) return false;
  if (range_scan->type != AccessPath::INDEX_RANGE_SCAN) return false;
  if (used_index(range_scan) != table->s->primary_key) return false;
  if (get_used_key_parts(range_scan) != 1) return false;
  if (range_scan->index_range_scan().num_ranges != 1) return false;
  if (range_scan->index_range_scan().used_key_part == nullptr) return false;
  if (range_scan->index_range_scan().ranges == nullptr) return false;
  if (range_scan->index_range_scan().ranges[0] == nullptr) return false;

  KEY_PART *used_key_part = range_scan->index_range_scan().used_key_part;
  if (used_key_part[0].field == nullptr) return false;
  if (used_key_part[0].field->field_index() != tpl.field_indices[0])
    return false;

  const KEY *keyinfo = &table->key_info[table->s->primary_key];
  if (!(actual_key_flags(keyinfo) & HA_NOSAME)) return false;
  if (keyinfo->user_defined_key_parts != 1) return false;
  if (keyinfo->key_part[0].fieldnr - 1 != tpl.field_indices[0]) return false;
  if (keyinfo->key_part[0].null_bit != 0) return false;

  for (uint i = 0; i < tpl.param_count; i++) {
    if (tpl.params[i] == nullptr) return false;
  }
  return true;
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

  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN) {
    return ps_point_plan_can_admit_range_between(tpl, join);
  }

  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_AGG) {
    if (!ps_point_plan_can_admit_range_between(tpl, join)) return false;

    /* Verify AGGREGATE access path structure. */
    AccessPath *root_path = join->root_access_path();
    if (root_path == nullptr ||
        root_path->type != AccessPath::AGGREGATE)
      return false;
    if (root_path->aggregate().rollup) return false;

    AccessPath *child = root_path->aggregate().child;
    if (child == nullptr) return false;

    /*
      The optimizer may insert a FILTER layer between AGGREGATE and
      INDEX_RANGE_SCAN for WHERE conditions. For simple BETWEEN predicates
      on the primary key, the filter condition is already applied by the
      range scan itself, so the FILTER is redundant. We skip past it to
      find the actual INDEX_RANGE_SCAN.

      Example: SELECT SUM(k) FROM t WHERE pk BETWEEN ? AND ?
      Optimizer creates: AGGREGATE -> FILTER -> INDEX_RANGE_SCAN
      Fast path builds: AGGREGATE -> INDEX_RANGE_SCAN
    */
    AccessPath *scan_path = child;
    if (child->type == AccessPath::FILTER) {
      scan_path = child->filter().child;
    }

    if (scan_path == nullptr || scan_path->type != AccessPath::INDEX_RANGE_SCAN)
      return false;
    if (used_index(scan_path) != join->qep_tab[0].table()->s->primary_key)
      return false;

    /* Verify sum_funcs is allocated and contains exactly one entry. */
    if (join->sum_funcs == nullptr) return false;
    uint sum_count = 0;
    for (Item_sum **f = join->sum_funcs; *f != nullptr; f++) {
      sum_count++;
    }
    if (sum_count != 1) return false;

    return true;
  }

  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_SORT) {
    if (!ps_point_plan_can_admit_range_between(tpl, join)) return false;

    AccessPath *root_path = join->root_access_path();
    if (root_path == nullptr || root_path->type != AccessPath::SORT)
      return false;

    if (root_path->sort().filesort == nullptr) return false;
    if (root_path->sort().order == nullptr) return false;

    AccessPath *child = root_path->sort().child;
    if (child == nullptr) return false;

    AccessPath *scan_path = child;
    if (child->type == AccessPath::FILTER) {
      scan_path = child->filter().child;
    }

    if (scan_path == nullptr ||
        scan_path->type != AccessPath::INDEX_RANGE_SCAN)
      return false;
    if (used_index(scan_path) !=
        join->qep_tab[0].table()->s->primary_key)
      return false;

    return true;
  }

  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_SORT_DISTINCT) {
    if (!ps_point_plan_can_admit_range_between(tpl, join)) return false;

    /*
      The optimizer converts DISTINCT to GROUP BY and uses temp table
      materialization, producing a complex AccessPath tree.  We don't
      match the exact tree shape — only verify the base table access
      is a valid PK range scan via can_admit_range_between().
      The fast path builds a simpler SORT(Filesort with dedup) ->
      INDEX_RANGE_SCAN tree which is semantically equivalent.
    */
    AccessPath *root_path = join->root_access_path();
    if (root_path == nullptr) return false;

    return true;
  }

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
    Check 9: verify every ref item maps back to one of the stable
    Item_param masters captured in the template during Phase 1.

    Matching uses Item_param::pos_in_query instead of pointer identity.
    This avoids caching per-execution optimizer clones while still
    reusing the permanent parameter objects from the prepared-statement
    parse tree.
  */
  for (uint i = 0; i < ref.key_parts; i++) {
    if (ref.items[i] == nullptr ||
        ref.items[i]->type() != Item::PARAM_ITEM)
      return false;

    if (ps_point_plan_find_stable_param(
            tpl, down_cast<Item_param *>(ref.items[i])) == nullptr) {
      return false;
    }
  }

  return true;
}

/**
  Capture range scan metadata from the optimizer's QEP_TAB into tpl.

  Shared by RANGE_PK_BETWEEN and RANGE_PK_BETWEEN_AGG admission.
  Populates key layout, range flags, cost estimates, param snapshots,
  and environment-drift detection fields.

  @param  thd   Current thread (for optimizer_switch / sql_mode).
  @param  tpl   Template to populate.
  @param  join  The JOIN with finalized plan metadata.
*/
static void ps_point_plan_admit_range_metadata(
    THD *thd, PsPointPlanTemplate &tpl, JOIN *join) {
  const QEP_TAB *tab = &join->qep_tab[0];
  TABLE *table = tab->table();
  AccessPath *range_scan = tab->range_scan();
  const KEY_PART *used_key_part =
      range_scan->index_range_scan().used_key_part;
  const uint keyno = used_index(range_scan);
  const KEY *keyinfo = &table->key_info[keyno];
  const uint field_index = used_key_part[0].field->field_index();

  tpl.keyno = keyno;
  tpl.key_parts = 1;
  tpl.key_length = keyinfo->key_part[0].store_length;
  tpl.null_rejecting = 0;
  tpl.best_read = join->best_read;
  tpl.best_rowcount = static_cast<double>(join->best_rowcount);
  tpl.range_flag = range_scan->index_range_scan().ranges[0]->flag;
  tpl.range_rkey_func_flag =
      range_scan->index_range_scan().ranges[0]->rkey_func_flag;
  tpl.range_mrr_flags = range_scan->index_range_scan().mrr_flags;
  tpl.range_mrr_buf_size = range_scan->index_range_scan().mrr_buf_size;
  tpl.range_need_rows_in_rowid_order =
      range_scan->index_range_scan().need_rows_in_rowid_order;
  tpl.range_can_be_used_for_ror =
      range_scan->index_range_scan().can_be_used_for_ror;
  tpl.range_can_be_used_for_imerge =
      range_scan->index_range_scan().can_be_used_for_imerge;
  tpl.range_reuse_handler = range_scan->index_range_scan().reuse_handler;
  tpl.range_geometry = range_scan->index_range_scan().geometry;
  tpl.range_reverse = range_scan->index_range_scan().reverse;
  tpl.range_using_extended_key_parts =
      range_scan->index_range_scan().using_extended_key_parts;
  tpl.optimizer_switch = ps_point_plan_relevant_optimizer_switch(thd);
  tpl.table_ref_version = table->s->get_table_ref_version();
  tpl.relevant_sql_mode = ps_point_plan_relevant_sql_mode(thd);
  tpl.field_indices[0] = field_index;
  tpl.field_indices[1] = field_index;

  for (uint i = 0; i < tpl.param_count; i++) {
    Item_param *prm = tpl.params[i];
    assert(prm != nullptr);
    tpl.actual_types[i] = prm->data_type_actual();
    tpl.unsigned_actuals[i] = prm->is_unsigned_actual();
    tpl.actual_collations[i] = ps_point_plan_actual_collation(prm);
  }
}

/**
  Build arena-cached components for the range fast path.

  Allocates QEP_TAB, QEP_shared, KEY_PART, pointer array, key buffers,
  and store_key objects (with Field clones) on the PS arena so that
  HOT executions only need 2 per-execution mem_root allocations
  (QUICK_RANGE + AccessPath) instead of ~8.

  Shared by RANGE_PK_BETWEEN and RANGE_PK_BETWEEN_AGG admission.

  @param  thd   Current thread (arena swap target).
  @param  stmt  The prepared statement owning the arena.
  @param  tpl   Template to populate with cached objects.
*/
static void ps_point_plan_admit_range_arena_cache(
    THD *thd, Prepared_statement *stmt, PsPointPlanTemplate &tpl) {
  if (tpl.range_arena_cached) return;

  TABLE *table = tpl.table_ref->table;
  const KEY *keyinfo = &table->key_info[tpl.keyno];

  const bool had_error = thd->is_error();
  Query_arena backup;
  thd->swap_query_arena(stmt->m_arena, &backup);

  const uint key_bytes = keyinfo->key_part[0].store_length;
  bool cache_ok = true;

  QEP_TAB *qt = new (thd->mem_root) QEP_TAB[2];
  QEP_shared *qs = new (thd->mem_root) QEP_shared;
  KEY_PART *kp = thd->mem_root->ArrayAlloc<KEY_PART>(1);
  QUICK_RANGE **ra = thd->mem_root->ArrayAlloc<QUICK_RANGE *>(1);
  uchar *min_buf = thd->mem_root->ArrayAlloc<uchar>(key_bytes + 1);
  uchar *max_buf = thd->mem_root->ArrayAlloc<uchar>(key_bytes + 1);

  if (qt == nullptr || qs == nullptr || kp == nullptr || ra == nullptr ||
      min_buf == nullptr || max_buf == nullptr) {
    cache_ok = false;
  }

  store_key *low_sk = nullptr;
  store_key *high_sk = nullptr;
  if (cache_ok) {
    memset(min_buf, 0, key_bytes + 1);
    memset(max_buf, 0, key_bytes + 1);

    const KEY_PART_INFO *kpi = &keyinfo->key_part[0];
    Field *orig_field = table->field[tpl.field_indices[0]];
    const bool nullable = (kpi->null_bit != 0);

    low_sk = new (thd->mem_root)
        store_key(thd, orig_field, min_buf + nullable,
                  nullable ? min_buf : nullptr, kpi->length, tpl.params[0]);
    high_sk = new (thd->mem_root)
        store_key(thd, orig_field, max_buf + nullable,
                  nullable ? max_buf : nullptr, kpi->length, tpl.params[1]);

    if (low_sk == nullptr || low_sk->store_field() == nullptr ||
        high_sk == nullptr || high_sk->store_field() == nullptr ||
        thd->is_error()) {
      cache_ok = false;
    }
  }

  if (cache_ok) {
    tpl.cached_range_qep_tab = qt;
    tpl.cached_range_qep_shared = qs;
    tpl.cached_range_key_part = kp;
    tpl.cached_range_array = ra;
    tpl.cached_range_min_key = min_buf;
    tpl.cached_range_max_key = max_buf;
    tpl.cached_range_key_bytes = key_bytes;
    tpl.cached_range_low_store = low_sk;
    tpl.cached_range_high_store = high_sk;
    tpl.cached_range_to_fields[0] = low_sk->store_field();
    tpl.cached_range_to_fields[1] = high_sk->store_field();
    tpl.range_arena_cached = true;
  }

  thd->swap_query_arena(backup, &stmt->m_arena);
  if (!had_error && thd->is_error()) thd->clear_error();
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

  /* Snapshot arena size before admission allocations. */
  const size_t arena_before = stmt->m_arena.mem_root->allocated_size();

  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN ||
      tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_AGG ||
      tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_SORT ||
      tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_SORT_DISTINCT) {
    ps_point_plan_admit_range_metadata(thd, tpl, join);
    ps_point_plan_admit_range_arena_cache(thd, stmt, tpl);

    /* --- Quota check: memory + plan count --- */
    const size_t arena_after = stmt->m_arena.mem_root->allocated_size();
    const size_t arena_delta =
        (arena_after >= arena_before) ? (arena_after - arena_before) : 0;

    bool mem_ok = ps_plan_cache_tracker.try_reserve(arena_delta);
    if (!mem_ok) {
      ps_point_plan_try_evict_idle(thd);
      mem_ok = ps_plan_cache_tracker.try_reserve(arena_delta);
    }
    if (mem_ok) {
      bool plan_ok = ps_plan_cache_tracker.try_add_plan();
      if (!plan_ok) {
        ps_point_plan_try_evict_idle(thd);
        plan_ok = ps_plan_cache_tracker.try_add_plan();
      }
      if (!plan_ok) {
        ps_plan_cache_tracker.release(arena_delta);
        mem_ok = false;
      }
    }
    if (!mem_ok) {
      ps_point_plan_mark_admission_refused(thd);
      /* Stay COLD; next can_admit fail → NEVER. */
      stmt->set_ps_point_plan_retryable_cold(false);
      return;
    }

    tpl.arena_cached_bytes = arena_delta;
    const uint64_t now = ps_point_plan_now_seconds();
    tpl.admission_time = now;
    tpl.last_hit_time.store(now, std::memory_order_relaxed);

    stmt->set_ps_point_plan_state(PsPointPlanState::HOT);
    stmt->set_ps_point_plan_retryable_cold(false);
    ps_point_plan_mark_admission(thd);
    return;
  }

  const QEP_TAB *tab = &join->qep_tab[0];
  const Index_lookup &ref = tab->ref();
  TABLE *table = tab->table();
  const KEY *keyinfo = &table->key_info[ref.key];
  Item_param *stable_params[PS_PC_MAX_PARAMS]{};
  uint field_indices[PS_PC_MAX_PARAMS]{};
  enum_field_types actual_types[PS_PC_MAX_PARAMS]{};
  bool unsigned_actuals[PS_PC_MAX_PARAMS]{};
  const CHARSET_INFO *actual_collations[PS_PC_MAX_PARAMS]{};

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
    stable_params[i] = ps_point_plan_find_stable_param(
        tpl, down_cast<Item_param *>(ref.items[i]));
    assert(stable_params[i] != nullptr);
  }

  for (uint i = 0; i < ref.key_parts; i++) {
    Item_param *prm = stable_params[i];
    field_indices[i] = keyinfo->key_part[i].fieldnr - 1;
    actual_types[i] = prm->data_type_actual();
    unsigned_actuals[i] = prm->is_unsigned_actual();
    actual_collations[i] = ps_point_plan_actual_collation(prm);
  }

  /*
    Validate cached helpers are compatible with the current key choice.
    After a retryable HOT->COLD demotion (type drift, optimizer_switch,
    etc.), the optimizer usually picks the same key, but if the key or
    field layout changed, the cached store_key / key_buff objects are
    stale and must be rebuilt.

    Compare against the previously admitted key layout before tpl is
    overwritten with the new plan metadata below.  This protects the
    retryable HOT->COLD path from reusing stale helper objects after a
    same-field unique-index switch with a different serialized layout.
  */
  if (tpl.ref_cached) {
    if (!ps_point_plan_cached_helpers_compatible(
            tpl, keyinfo, ref.key_parts, ref.key_length, field_indices)) {
      tpl.ref_cached = false;
      tpl.qep_cached = false;
      tpl.cached_key_parts = 0;
      tpl.cached_key_length = 0;
    }
  }

  /* Copy stable key metadata from the optimizer's ref structure. */
  tpl.keyno = static_cast<uint>(ref.key);
  tpl.key_parts = ref.key_parts;
  tpl.key_length = ref.key_length;
  tpl.null_rejecting = ref.null_rejecting;

  /* Copy optimizer cost estimates for potential use by Phase 3+. */
  tpl.best_read = join->best_read;
  tpl.best_rowcount = static_cast<double>(join->best_rowcount);
  tpl.optimizer_switch = ps_point_plan_relevant_optimizer_switch(thd);
  tpl.table_ref_version = table->s->get_table_ref_version();
  tpl.relevant_sql_mode = ps_point_plan_relevant_sql_mode(thd);

  for (uint i = 0; i < ref.key_parts; i++) {
    tpl.params[i] = stable_params[i];
    tpl.field_indices[i] = field_indices[i];
    tpl.actual_types[i] = actual_types[i];
    tpl.unsigned_actuals[i] = unsigned_actuals[i];
    tpl.actual_collations[i] = actual_collations[i];
  }

  /*
    Build cached components on PS arena so the fast path can reuse them
    across executions, avoiding per-execution allocation overhead.

    V1.1: Index_lookup components (key buffers, store_key, Field clones).
    V1.2: QEP skeleton (QEP_TAB, QEP_shared, pointer arrays).

    Built once per component group.  After retryable demotion the
    validation above ensures stale helpers are discarded before rebuild.

    Lifecycle coupling: V1.2 (qep) depends on V1.1 (ref) — the
    cached key_copy array references store_key objects from ref.
    If ref allocation fails, skip qep to avoid a partial-cache state
    where qep_cached=true but ref_cached=false (which the fast path
    handles but makes reasoning harder).
  */
  if (!tpl.ref_cached || !tpl.qep_cached) {
    const bool had_error_before_cache_build = thd->is_error();
    Query_arena backup;
    thd->swap_query_arena(stmt->m_arena, &backup);

    if (!tpl.ref_cached) {
      const uint aligned_len = ALIGN_SIZE(tpl.key_length);
      uchar *cached_key_buff = thd->mem_root->ArrayAlloc<uchar>(aligned_len);
      uchar *cached_key_buff2 = thd->mem_root->ArrayAlloc<uchar>(aligned_len);
      store_key *cached_store_keys[PS_PC_MAX_PARAMS]{};
      Field *cached_to_fields[PS_PC_MAX_PARAMS]{};
      uint cached_part_lengths[PS_PC_MAX_PARAMS]{};
      uint cached_part_store_lengths[PS_PC_MAX_PARAMS]{};

      bool cache_ok = (cached_key_buff != nullptr && cached_key_buff2 != nullptr);

      uchar *kb_pos = cached_key_buff;
      for (uint i = 0; i < tpl.key_parts && cache_ok; i++) {
        const KEY_PART_INFO *kp = &keyinfo->key_part[i];
        Field *orig_field = table->field[tpl.field_indices[i]];
        const bool nullable = (kp->null_bit != 0);

        store_key *sk = new (thd->mem_root)
            store_key(thd, orig_field, kb_pos + nullable,
                      nullable ? kb_pos : nullptr, kp->length, tpl.params[i]);
        if (sk == nullptr || sk->store_field() == nullptr || thd->is_error()) {
          cache_ok = false;
          break;
        }

        cached_store_keys[i] = sk;
        cached_to_fields[i] = sk->store_field();
        cached_part_lengths[i] = kp->length;
        cached_part_store_lengths[i] = kp->store_length;
        kb_pos += kp->store_length;
      }
      if (cache_ok) {
        tpl.cached_key_buff = cached_key_buff;
        tpl.cached_key_buff2 = cached_key_buff2;
        tpl.cached_key_parts = tpl.key_parts;
        tpl.cached_key_length = tpl.key_length;
        for (uint i = 0; i < tpl.key_parts; i++) {
          tpl.cached_store_keys[i] = cached_store_keys[i];
          tpl.cached_to_fields[i] = cached_to_fields[i];
          tpl.cached_part_lengths[i] = cached_part_lengths[i];
          tpl.cached_part_store_lengths[i] = cached_part_store_lengths[i];
        }
        tpl.ref_cached = true;
      }
    }

    if (tpl.ref_cached && !tpl.qep_cached) {
      QEP_TAB *qt = new (thd->mem_root) QEP_TAB[2];
      QEP_shared *qs = new (thd->mem_root) QEP_shared;
      store_key **kc = thd->mem_root->ArrayAlloc<store_key *>(tpl.key_parts);
      Item **ri = thd->mem_root->ArrayAlloc<Item *>(tpl.key_parts);
      bool **cg = thd->mem_root->ArrayAlloc<bool *>(tpl.key_parts);

      if (qt != nullptr && qs != nullptr &&
          kc != nullptr && ri != nullptr && cg != nullptr) {
        for (uint i = 0; i < tpl.key_parts; i++) {
          ri[i] = tpl.params[i];
          cg[i] = nullptr;
        }
        tpl.cached_qep_tab = qt;
        tpl.cached_qep_shared = qs;
        tpl.cached_key_copy = kc;
        tpl.cached_ref_items = ri;
        tpl.cached_cond_guards = cg;
        tpl.qep_cached = true;
      }
    }

    thd->swap_query_arena(backup, &stmt->m_arena);
    if (!had_error_before_cache_build && thd->is_error()) thd->clear_error();
  }

  /* --- Quota check: memory + plan count (EQ_REF path) --- */
  {
    const size_t arena_after = stmt->m_arena.mem_root->allocated_size();
    const size_t arena_delta =
        (arena_after >= arena_before) ? (arena_after - arena_before) : 0;

    bool mem_ok = ps_plan_cache_tracker.try_reserve(arena_delta);
    if (!mem_ok) {
      ps_point_plan_try_evict_idle(thd);
      mem_ok = ps_plan_cache_tracker.try_reserve(arena_delta);
    }
    if (mem_ok) {
      bool plan_ok = ps_plan_cache_tracker.try_add_plan();
      if (!plan_ok) {
        ps_point_plan_try_evict_idle(thd);
        plan_ok = ps_plan_cache_tracker.try_add_plan();
      }
      if (!plan_ok) {
        ps_plan_cache_tracker.release(arena_delta);
        mem_ok = false;
      }
    }
    if (!mem_ok) {
      ps_point_plan_mark_admission_refused(thd);
      /* Stay COLD; next can_admit fail → NEVER. */
      stmt->set_ps_point_plan_retryable_cold(false);
      return;
    }

    tpl.arena_cached_bytes = arena_delta;
    const uint64_t now = ps_point_plan_now_seconds();
    tpl.admission_time = now;
    tpl.last_hit_time.store(now, std::memory_order_relaxed);
  }

  stmt->set_ps_point_plan_state(PsPointPlanState::HOT);
  stmt->set_ps_point_plan_retryable_cold(false);
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

void ps_point_plan_mark_admission_refused(THD *thd) {
  thd->status_var.ps_point_plan_cache_admission_refused++;
}

void ps_point_plan_mark_eviction(THD *thd) {
  thd->status_var.ps_point_plan_cache_evictions++;
}

/* --- Global plan cache memory tracker implementation --- */

Ps_plan_cache_mem_tracker ps_plan_cache_tracker;

bool Ps_plan_cache_mem_tracker::try_reserve(size_t bytes) {
  ulonglong max_mem = ps_point_plan_cache_max_mem_size;
  if (max_mem == 0) {
    m_total_bytes.fetch_add(bytes, std::memory_order_relaxed);
    return true;
  }
  size_t current = m_total_bytes.load(std::memory_order_relaxed);
  while (true) {
    if (current + bytes > max_mem) return false;
    if (m_total_bytes.compare_exchange_weak(
            current, current + bytes,
            std::memory_order_acq_rel, std::memory_order_relaxed))
      return true;
  }
}

void Ps_plan_cache_mem_tracker::release(size_t bytes) {
  m_total_bytes.fetch_sub(bytes, std::memory_order_relaxed);
}

bool Ps_plan_cache_mem_tracker::try_add_plan() {
  ulong max_plans = ps_point_plan_cache_max_cached_plans;
  if (max_plans == 0) {
    m_total_plans.fetch_add(1, std::memory_order_relaxed);
    return true;
  }
  size_t current = m_total_plans.load(std::memory_order_relaxed);
  while (true) {
    if (current >= max_plans) return false;
    if (m_total_plans.compare_exchange_weak(
            current, current + 1,
            std::memory_order_acq_rel, std::memory_order_relaxed))
      return true;
  }
}

void Ps_plan_cache_mem_tracker::remove_plan() {
  m_total_plans.fetch_sub(1, std::memory_order_relaxed);
}

/* --- TTL-based eviction for idle HOT plan cache entries --- */

/**
  Scan the current THD's prepared statements for idle HOT entries
  and evict (demote to COLD) those whose last_hit_time is older than
  the configured idle threshold.

  Connection-local only — iterates thd->stmt_map.for_each() without
  cross-thread locking.  This is safe because stmt_map belongs to the
  current connection and is only modified by the owning thread.

  Called from ps_point_plan_admit() when a quota check fails, to free
  up global memory/plan-count quota for the new admission.

  Evicted entries are demoted to COLD (non-retryable): the next
  execution will re-optimize and can re-admit if quota allows.
  Arena-allocated objects are orphaned but cannot be freed from the
  bump allocator; they are reclaimed when the PS is destroyed.

  @param  thd  Current thread whose stmt_map is scanned.
*/
void ps_point_plan_try_evict_idle(THD *thd) {
  const ulong idle_seconds = ps_point_plan_cache_eviction_idle_seconds;
  if (idle_seconds == 0) return;  /* eviction disabled */

  const uint64_t now = ps_point_plan_now_seconds();
  const uint64_t cutoff = (now > idle_seconds) ? (now - idle_seconds) : 0;

  thd->stmt_map.for_each([&](Prepared_statement *ps) {
    if (ps->ps_point_plan_state() != PsPointPlanState::HOT) return;

    PsPointPlanTemplate &tpl = ps->ps_point_plan_template();
    const uint64_t last_hit =
        tpl.last_hit_time.load(std::memory_order_relaxed);

    /* Evict only entries idle longer than the threshold. */
    if (last_hit > 0 && last_hit < cutoff) {
      /* Release from global tracker. */
      ps_plan_cache_tracker.remove_plan();
      if (tpl.arena_cached_bytes > 0) {
        ps_plan_cache_tracker.release(tpl.arena_cached_bytes);
        tpl.arena_cached_bytes = 0;
      }

      /* Demote HOT → COLD (non-retryable). */
      ps_point_plan_clear_hot_metadata(&tpl);
      ps->set_ps_point_plan_state(PsPointPlanState::COLD);
      ps->set_ps_point_plan_retryable_cold(false);

      ps_point_plan_mark_eviction(thd);
    }
  });
}
