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

#include <atomic>

#include "field_types.h"
#include "my_base.h"
#include "my_inttypes.h"
#include "sql/sql_const.h"

struct CHARSET_INFO;
class Field;
class Item_param;
class Item_sum;
class Prepared_statement;
class Query_block;
class Table_ref;
class THD;
class JOIN;
class KEY;
class QEP_shared;
class QEP_TAB;
class store_key;
struct KEY_PART;
class QUICK_RANGE;
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

enum class PsPointPlanResetReason : uchar {
  DEMOTE_RETRYABLE = 0,
  INVALIDATE_DDL,
  EVICT_IDLE,
  QUOTA_REFUSED,
  DESTROY
};

enum class PsPointPlanAdmitResult : uchar {
  ADMITTED = 0,
  REFUSED_BY_QUOTA,
  BUILD_FAILED
};

/**
  Cached plan type — determines which fast-path builder to invoke.
  Phase 1-6 use POINT_EQ_REF; Phase 7+ adds RANGE_PK_BETWEEN.
*/
enum class PsCachedPlanType : uchar {
  POINT_EQ_REF = 0,
  RANGE_PK_BETWEEN,
  RANGE_PK_BETWEEN_AGG,
  RANGE_PK_BETWEEN_SORT,
  RANGE_PK_BETWEEN_SORT_DISTINCT,
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

  /// Cached QUICK_RANGE::flag for RANGE_PK_BETWEEN.
  uint16 range_flag{0};

  /// Cached QUICK_RANGE::rkey_func_flag for RANGE_PK_BETWEEN.
  enum ha_rkey_function range_rkey_func_flag{HA_READ_INVALID};

  /// Cached MRR flags from the admitted INDEX_RANGE_SCAN path.
  unsigned range_mrr_flags{0};

  /// Cached MRR buffer size from the admitted INDEX_RANGE_SCAN path.
  unsigned range_mrr_buf_size{0};

  /// Cached INDEX_RANGE_SCAN row-order requirement.
  bool range_need_rows_in_rowid_order{false};

  /// Cached INDEX_RANGE_SCAN ROR capability.
  bool range_can_be_used_for_ror{false};

  /// Cached INDEX_RANGE_SCAN index-merge capability.
  bool range_can_be_used_for_imerge{false};

  /// Cached INDEX_RANGE_SCAN handler reuse flag.
  bool range_reuse_handler{false};

  /// Cached INDEX_RANGE_SCAN geometry flag.
  bool range_geometry{false};

  /// Cached INDEX_RANGE_SCAN reverse-scan flag.
  bool range_reverse{false};

  /// Cached INDEX_RANGE_SCAN extended-keyparts flag.
  bool range_using_extended_key_parts{false};

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

  /// Cached KEY_PART_INFO::length values for helper compatibility checks.
  uint cached_part_lengths[PS_PC_MAX_PARAMS]{};

  /// Cached KEY_PART_INFO::store_length values for key-buffer layout checks.
  uint cached_part_store_lengths[PS_PC_MAX_PARAMS]{};

  /// Cached helper layout key-part count, kept across retryable demotion.
  uint cached_key_parts{0};

  /// Cached helper layout serialized key length, kept across retryable demotion.
  uint cached_key_length{0};

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

  /*
    --- Phase 7b: Arena-cached range fast-path components ---
    Allocated on PS m_arena during RANGE_PK_BETWEEN admission.
    Reused across HOT executions to reduce per-execution mem_root
    allocations from ~8 to 2 (only QUICK_RANGE + AccessPath remain
    per-execution).
  */

  /// Arena-cached store_key for BETWEEN low bound.
  store_key *cached_range_low_store{nullptr};
  /// Arena-cached store_key for BETWEEN high bound.
  store_key *cached_range_high_store{nullptr};

  /// Arena-cached Field clones inside range store_keys (for re-patching table ptr).
  Field *cached_range_to_fields[2]{};

  /// Arena-cached KEY_PART for range fast path.
  KEY_PART *cached_range_key_part{nullptr};

  /// Arena-cached QUICK_RANGE pointer array (1 element).
  QUICK_RANGE **cached_range_array{nullptr};

  /// Arena-cached min_key buffer (serialization target for low bound).
  uchar *cached_range_min_key{nullptr};
  /// Arena-cached max_key buffer (serialization target for high bound).
  uchar *cached_range_max_key{nullptr};
  /// Cached key buffer size (bytes), equals key_part[0].store_length.
  uint cached_range_key_bytes{0};

  /// Arena-cached QEP_TAB[2] for range fast path.
  QEP_TAB *cached_range_qep_tab{nullptr};
  /// Arena-cached QEP_shared for range fast path.
  QEP_shared *cached_range_qep_shared{nullptr};

  /// True when range arena components are populated and usable.
  bool range_arena_cached{false};

  /*
    --- Phase 8: Aggregate fields for RANGE_PK_BETWEEN_AGG ---
    Metadata only; Item_sum objects are NOT cached (clone_item()
    returns nullptr).  The fast path reinitializes sum_funcs via
    count_field_types + alloc_func_list + make_sum_func_list.
  */

  /// True when the query contains a single aggregate function.
  bool has_aggregate{false};

  // TODO: aggregate_type is captured at classify time but not yet consumed
  // by any guard or fast-path logic.  Future phases may use it for runtime
  // validation that the aggregate function kind has not changed.
  /// Aggregate function type (SUM_FUNC / COUNT_FUNC / MIN_FUNC / MAX_FUNC).
  uint8 aggregate_type{0};

  /// Aggregate field index in TABLE::field[] (MAX_KEY for COUNT(*)).
  uint aggregate_field_index{MAX_KEY};

  /// Snapshot of the aggregate field's data type (for drift detection).
  enum_field_types aggregate_field_type{MYSQL_TYPE_INVALID};

  /// Whether the aggregate field is unsigned.
  bool aggregate_field_unsigned{false};

  /*
    --- Phase 9: ORDER BY fields for RANGE_PK_BETWEEN_SORT ---
    Captured at classify time from the ORDER BY clause.
    The Filesort object is NOT cached — it holds THD* and
    Mem_root_array members that are per-execution.  Each HOT
    execution constructs a fresh Filesort on thd->mem_root.
  */

  /// True when the query has a cacheable single-column ORDER BY.
  bool has_order_by{false};

  /// 0-based field ordinal of the ORDER BY column in TABLE::field[].
  uint order_field_index{MAX_KEY};

  /// True for DESC, false for ASC.
  bool order_direction_desc{false};

  /// Data type of the ORDER BY column (for drift detection).
  enum_field_types order_field_type{MYSQL_TYPE_INVALID};

  /// Whether the ORDER BY column is unsigned.
  bool order_field_unsigned{false};

  /// Character set / collation of the ORDER BY column.
  const CHARSET_INFO *order_collation{nullptr};

  /// True when the query has DISTINCT matching the ORDER BY column.
  bool has_distinct{false};

  /*
    --- Memory limit tracking fields ---
    Used by the global plan cache memory tracker to account for
    arena-allocated bytes and support TTL-based eviction.
  */

  /// Total arena bytes charged to the global tracker for this PS.
  /// Set during admission; released on invalidation/deallocation.
  size_t arena_cached_bytes{0};

  /// Timestamp of last successful HOT hit (THD query start seconds).
  /// Prepared statements are connection-local, and idle eviction only scans
  /// the owning THD's statement map, so no atomic access is required here.
  uint64_t last_hit_time{0};

  /// Timestamp of admission (monotonic clock, seconds).
  uint64_t admission_time{0};
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
PsPointPlanAdmitResult ps_point_plan_admit(THD *thd,
                                           Prepared_statement *stmt,
                                           JOIN *join);

/* Status counter helpers. */
void ps_point_plan_mark_hit(THD *thd);
void ps_point_plan_mark_admission(THD *thd);
void ps_point_plan_mark_invalidation(THD *thd);
void ps_point_plan_mark_runtime_fallback(THD *thd);
void ps_point_plan_mark_cold_classification(THD *thd);
void ps_point_plan_mark_admission_refused(THD *thd);
void ps_point_plan_mark_eviction(THD *thd);

/**
  Scan the current THD's prepared statements for idle HOT entries
  and evict them to free global plan-cache quota.

  Connection-local only — iterates thd->stmt_map without cross-thread
  locking.  Called from admission when quota is exhausted.

  @param  thd  Current thread whose stmt_map is scanned.
*/
void ps_point_plan_try_evict_idle(THD *thd);

/*
  -----------------------------------------------------------------------
  Global plan cache memory tracker.
  -----------------------------------------------------------------------

  Thread-safe via std::atomic; lock-free on the HOT-hit fast path.
  The tracker only participates in the admission path (COLD → HOT),
  never on the HOT-hit fast path, so its overhead is negligible in
  steady state.
*/

/// Global sysvar variables — defined in mysqld.cc, declared here for
/// inline access by the tracker methods.
extern ulonglong ps_point_plan_cache_max_mem_size;
extern ulong ps_point_plan_cache_max_cached_plans;
extern uint ps_point_plan_cache_eviction_pct;
extern ulong ps_point_plan_cache_eviction_idle_seconds;

class Ps_plan_cache_mem_tracker {
 public:
  /** Try to reserve @p bytes.  @return true on success. */
  bool try_reserve(size_t bytes);
  /** Release @p bytes back to the pool. */
  void release(size_t bytes);
  /** Try to increment the HOT plan count.  @return true on success. */
  bool try_add_plan();
  /** Decrement the HOT plan count. */
  void remove_plan();

  size_t current_mem_used() const {
    return m_total_bytes.load(std::memory_order_relaxed);
  }
  size_t current_plan_count() const {
    return m_total_plans.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<size_t> m_total_bytes{0};
  std::atomic<size_t> m_total_plans{0};
};

/// Global singleton — defined in ps_point_plan_cache.cc.
extern Ps_plan_cache_mem_tracker ps_plan_cache_tracker;

#endif  // SQL_PS_POINT_PLAN_CACHE_H
