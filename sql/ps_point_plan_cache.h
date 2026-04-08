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

struct CHARSET_INFO;
class Field;
class Item_param;
class Prepared_statement;
class Query_block;
class Table_ref;
class THD;
class JOIN;
class KEY;
class QEP_shared;
class QEP_TAB;
class store_key;
struct TABLE;

/**
  State machine for the per-PS point plan cache slot.

  @verbatim
    PREPARE                         first EXECUTE
    ┌───────────┐  classify OK  ┌──────────────────────┐  admit OK
    │  (new PS) ├──────────────>│        COLD          ├────────────> HOT
    └─────┬─────┘               └──────────┬───────────┘     ┌────────┘
          │                                │ admit fail      │ guard fail /
          │ classify fail                  v                 │ type drift
          └─────────────────────────> NEVER <────────────────┘
                                                          (non-retryable COLD
                                                           re-admit fails)
  @endverbatim

  - NEVER   — permanently not a candidate; fast bypass on every EXECUTE.
              Set when static classification fails, or when the first
              normal optimization produces a plan that does not match
              the expected point-query shape.
  - COLD    — static WHERE shape matches; awaiting first EXECUTE to
              inspect the optimizer output and decide admission.
              Also used as the demotion target from HOT when a runtime
              guard or environment drift invalidates the template.
  - HOT     — template successfully cached; the fast path (Phase 3+)
              may be attempted on subsequent EXECUTEs.
  - INVALID — reserved (unused); kept for enum stability.
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

  Stores information that is stable across executions of the same
  prepared statement.  V1.2 also caches arena-allocated QEP_TAB,
  QEP_shared, and Index_lookup pointer arrays to eliminate
  per-execution allocation overhead on thd->mem_root.

  The arrays are sized to accommodate composite unique keys (up to
  PS_PC_MAX_KEY_PARTS columns) and BETWEEN predicates (2 parameters).
  Phase 1-5 use only params[0] / field_indices[0] with param_count == 1.
*/
struct PsPointPlanTemplate {
  /*
    --- Phase 1 fields (populated by ps_point_plan_classify) ---
    Filled during PREPARE by inspecting the WHERE-clause AST.
    These capture the "shape" of the query before any optimization.
  */

  /// The single leaf Table_ref from the Query_block.
  Table_ref *table_ref{nullptr};

  /// Determines which fast-path builder to invoke (Phase 3+).
  PsCachedPlanType plan_type{PsCachedPlanType::POINT_EQ_REF};

  /// Number of ? parameters found in WHERE equalities.
  uint param_count{0};

  /**
    Stable Item_param pointers from the parse tree.  After Phase 1
    these are in WHERE-clause order; after Phase 2 admission they are
    reordered to key-part order (matching the optimizer's ref.items[]).

    Pointer identity is stable across executions because v1 excludes
    CTE/derived-table scenarios that clone Item_param.
  */
  Item_param *params[PS_PC_MAX_PARAMS]{};

  /**
    0-based field ordinal within TABLE for each parameter's column.
    After Phase 2 admission, reordered to key-part order and filled
    from KEY_PART_INFO::fieldnr - 1 (1-based to 0-based conversion).
  */
  uint field_indices[PS_PC_MAX_PARAMS]{};

  /**
    Snapshot of each parameter's actual data type at first EXECUTE.
    Populated during Phase 2 admission from Item_param::data_type_actual().
    Used by Phase 3+ fast-path to detect type-change invalidation.
  */
  enum_field_types actual_types[PS_PC_MAX_PARAMS]{};

  /// Whether each parameter's actual integer value is unsigned.
  bool unsigned_actuals[PS_PC_MAX_PARAMS]{};

  /// Actual string collation snapshot for each parameter at admission.
  const CHARSET_INFO *actual_collations[PS_PC_MAX_PARAMS]{};

  /*
    --- Phase 2 fields (populated by ps_point_plan_admit) ---
    Filled during the first EXECUTE by inspecting the optimizer's
    QEP_TAB / Index_lookup output.  These are stable plan metadata
    that do not change across executions of the same PS.
  */

  /// Index number within TABLE::key_info[].
  uint keyno{MAX_KEY};

  /// Number of key parts used in the ref lookup.
  uint key_parts{0};

  /// Total serialized key length in bytes.
  uint key_length{0};

  /// Bitmask of key parts that reject NULL (from Index_lookup).
  key_part_map null_rejecting{0};

  /// Optimizer's best_read cost estimate for the plan.
  double best_read{0.0};

  /// Optimizer's best_rowcount estimate (cast from ha_rows).
  double best_rowcount{1.0};

  /// Relevant optimizer_switch bits captured at admission time.
  ulonglong optimizer_switch{0};

  /// Current TABLE_SHARE::get_table_ref_version() at admission time.
  ulonglong table_ref_version{0};

  /// Relevant sql_mode bits (e.g. PAD_CHAR_TO_FULL_LENGTH) at admission.
  ulonglong relevant_sql_mode{0};

  /*
    --- Cached Index_lookup components (allocated on PS m_arena) ---
    Populated during admission to avoid per-execution Field cloning
    and buffer allocation in the fast path.  The store_key objects
    and their Field clones live on the PS arena and survive across
    executions; only to_field->table needs re-patching each time.
  */

  /// Pre-allocated key serialization buffer on PS arena.
  uchar *cached_key_buff{nullptr};

  /// Secondary key buffer (for EQRefIterator key comparison).
  uchar *cached_key_buff2{nullptr};

  /// Cached store_key objects (one per key part) on PS arena.
  store_key *cached_store_keys[PS_PC_MAX_PARAMS]{};

  /// Cached Field clones inside store_key (for re-patching table ptr).
  Field *cached_to_fields[PS_PC_MAX_PARAMS]{};

  /// True when cached_key_buff et al. are populated and usable.
  bool ref_cached{false};

  /*
    --- V1.2: Arena-cached QEP skeleton ---
    Allocated on PS m_arena during admission.  Reused across HOT
    executions to avoid per-execution QEP_TAB / pointer-array
    allocation on thd->mem_root.
  */

  /// Arena-cached QEP_TAB[2] (1 real + 1 sentinel).
  QEP_TAB *cached_qep_tab{nullptr};

  /// Arena-cached QEP_shared for qep_tab[0].
  QEP_shared *cached_qep_shared{nullptr};

  /// Arena-cached store_key* array for Index_lookup::key_copy.
  store_key **cached_key_copy{nullptr};

  /// Arena-cached Item* array for Index_lookup::items.
  Item **cached_ref_items{nullptr};

  /// Arena-cached bool* array for Index_lookup::cond_guards.
  bool **cached_cond_guards{nullptr};

  /// True when the QEP skeleton above is populated and usable.
  bool qep_cached{false};
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
  Phase 1: recognizes single Item_func_eq(field, param) and
           Item_cond_and wrapping multiple Item_func_eq for composite keys.
  Phase 7+: will also recognize Item_func_between(field, param, param).
  Sets plan_type, param_count, params[], and field_indices[] in @p tpl.
  @return true if a supported shape is found, false otherwise.
*/
bool ps_point_plan_extract_where_shape(Query_block *qb,
                                       PsPointPlanTemplate *tpl);

/**
  Runtime guard — O(1) checks before attempting the fast path.
  Structural changes invalidate the cached HOT template; retryable
  environment changes demote the PS back to COLD so the current
  execution can re-optimize and potentially re-admit.
  @return true if guard passes and fast path may proceed.
*/
bool ps_point_plan_runtime_guard(THD *thd, Prepared_statement *stmt,
                                 TABLE **table_out, KEY **keyinfo_out);

/**
  Construct a minimal one-table EQ_REF execution plan for a HOT
  prepared statement, bypassing the entire optimizer pipeline.

  V1.2: Called from the early fast-path hook at the TOP of
  JOIN::optimize(), before any optimizer preamble code
  (count_field_types, alloc_func_list, get_optimizable_conditions,
  optimize_cond, etc.).  This allows HOT point-selects to skip
  the full optimizer preamble.

  Uses delayed-write: JOIN members are only modified after all
  construction steps succeed.  When both qep_cached and ref_cached
  are true, reuses arena-allocated QEP_TAB / QEP_shared / pointer
  arrays, minimizing per-execution allocation.

  @param  thd   Current thread.
  @param  join  The JOIN being optimized.
  @param  stmt  The owning Prepared_statement (HOT state).

  @retval true  Fast path plan constructed; caller should set
                set_optimized(), tables_list, PLAN_READY and return.
  @retval false Fast path declined; caller should continue to
                the normal optimizer preamble (JOIN state is untouched).
*/
bool ps_point_plan_build_fast_path(THD *thd, JOIN *join,
                                   Prepared_statement *stmt);

/**
  Check whether the first normal optimization result qualifies for
  admission into the HOT state.

  Called from the admission hook in JOIN::optimize() after
  push_to_engines() succeeds, only for COLD statements when the
  ps_point_plan_cache sysvar is ON.

  The optimizer resolves single-table PS point queries as JT_CONST
  (not JT_EQ_REF) because Item_param::used_tables() returns
  INNER_TABLE_BIT and const_for_execution() is true.  Plan metadata
  lives on qep_tab[0] since join_tab is freed after
  get_best_combination().

  Admission criteria (all must hold):
  - Exactly one primary table, marked const (primary_tables==1,
    const_tables==1, type==JT_CONST)
  - No HAVING clause
  - The chosen key is unique (HA_NOSAME)
  - All user-defined key parts are covered (full unique key match)
  - ref.key_parts == tpl.param_count (no extra WHERE predicates)
  - Every ref.items[i] matches an Item_param from the template
  - No guarded conditions (subquery-triggered guards)
  - No keypart_hash (subquery materialization hash)

  @param  stmt  The prepared statement in COLD state.
  @param  join  The JOIN that just finished normal optimization.
  @return true if plan qualifies for admission, false otherwise.
*/
bool ps_point_plan_can_admit(Prepared_statement *stmt, JOIN *join);

/**
  Perform admission: copy stable plan metadata from @p join into the
  template stored on @p stmt, and transition state COLD -> HOT.

  Must only be called after ps_point_plan_can_admit() returns true.
  Copies key metadata (keyno, key_parts, key_length, null_rejecting)
  and optimizer cost estimates from the QEP_TAB into the template.

  Reorders params[] and field_indices[] from WHERE-clause order
  (captured in Phase 1) to key-part order (as determined by the
  optimizer's create_ref_for_key()), so that Phase 3+ fast-path
  can use sequential key-part iteration.

  @param  thd   Current thread (for status counter).
  @param  stmt  The prepared statement to promote to HOT.
  @param  join  The JOIN with finalized plan metadata.
*/
void ps_point_plan_admit(THD *thd, Prepared_statement *stmt, JOIN *join);

/* Status counter helpers. */
void ps_point_plan_mark_hit(THD *thd);
void ps_point_plan_mark_admission(THD *thd);
void ps_point_plan_mark_invalidation(THD *thd);
void ps_point_plan_mark_runtime_fallback(THD *thd);
void ps_point_plan_mark_cold_classification(THD *thd);

#endif  // SQL_PS_POINT_PLAN_CACHE_H
