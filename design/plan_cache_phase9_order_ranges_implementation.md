# Plan Cache Phase 9: order_ranges Implementation

## Overview

本文档是 `plan_cache_phase7_10_read_only_coverage.md` Phase 9 的详细实现方案。
目标：支持 `SELECT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c` 查询模式的计划缓存。

**预期收益**：order_ranges 约占 sysbench oltp_read_only 优化器资源消耗的 25-35%，
缓存后可提升总体 QPS 10-15%。这是 oltp_read_only 五种SQL模式中优化器开销最大的
单条SQL（simple_ranges/sum_ranges 已在 Phase 7/8 覆盖）。

---

## 目标SQL模式

```sql
-- 支持的模式（Phase 9 范围）
SELECT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c
SELECT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c ASC
SELECT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c DESC

-- 特点：
-- 1. 单表查询
-- 2. 主键范围扫描 (BETWEEN)
-- 3. ORDER BY 单列物理字段（非主键列，索引无法提供排序）
-- 4. 无 GROUP BY, DISTINCT, LIMIT, HAVING, 聚合函数
-- 5. SELECT list 可包含任意列/表达式（不影响计划形状）
```

**优化器正常路径产出的 AccessPath 树：**

```
SORT(filesort on column c)
 └── INDEX_RANGE_SCAN(pk: id BETWEEN ? AND ?)
```

由于 ORDER BY 列 `c` 不是主键列，`test_if_skip_sort_order()` 判定索引无法
提供排序，优化器必须插入 `Filesort` 执行内存排序。

---

## 关键设计决策

### 决策 1: Filesort 不缓存，每次执行重新构造

**原因**：

1. `Filesort::m_thd` 保存当前 THD 指针——不同连接/执行使用不同 THD，
   缓存在 PS arena 上会导致 stale THD 引用。
2. `Filesort::tables` 是 `Mem_root_array<TABLE*>`，其内存归属于构造时的
   MEM_ROOT（`thd->mem_root`），跨执行复用会导致 dangling pointer。
3. `Filesort::sortorder` 通过 `THR_MALLOC->Alloc()` 分配在当前线程的
   `mem_root` 上，同样不能跨执行复用。

**开销评估**：每次执行新建 `Filesort` + `st_sort_field` + `SORT AccessPath`
约 ~300 bytes（在 `thd->mem_root` 上，查询结束自动释放），与已有的
`QUICK_RANGE` + `INDEX_RANGE_SCAN AccessPath` 相当。

### 决策 2: 复用 `build_range_components` 构造范围扫描

SORT 快速路径在 INDEX_RANGE_SCAN 层复用 Phase 7/8 的
`ps_point_plan_build_range_components()` 辅助函数，不重复实现。
仅在外层包装 `Filesort` + `SORT AccessPath`。

### 决策 3: ORDER 指针直接取自 JOIN 对象

`JOIN::order.order` 在 JOIN 构造函数中从 `query_block->order_list.first`
初始化，指向解析树中的 `ORDER` 链表。该链表在 PS 生命周期内稳定
（不随执行变化），且在 `JOIN::optimize()` 起始时 `REF_SLICE_ACTIVE`
已激活，`ORDER::item[0]` 能正确解析到原始 `Item_field`。

---

## 修改文件清单

| 文件 | 修改类型 | 行数估算 |
|------|----------|----------|
| `sql/ps_point_plan_cache.h` | 修改 | +20 |
| `sql/ps_point_plan_cache.cc` | 修改 | +120 |
| **总计** | | **~140 行** |

---

## 详细实现步骤

### Step 1: 扩展数据结构

**文件**: `sql/ps_point_plan_cache.h`

#### 1.1 扩展 PsCachedPlanType 枚举

```cpp
enum class PsCachedPlanType : uchar {
  POINT_EQ_REF = 0,        // Phase 1-6: WHERE pk = ?
  RANGE_PK_BETWEEN,        // Phase 7:  WHERE pk BETWEEN ? AND ?
  RANGE_PK_BETWEEN_AGG,    // Phase 8:  SELECT SUM() WHERE pk BETWEEN ? AND ?
  RANGE_PK_BETWEEN_SORT,   // Phase 9:  SELECT ... WHERE pk BETWEEN ? AND ? ORDER BY col
};
```

#### 1.2 扩展 PsPointPlanTemplate 结构体

在结构体末尾（`aggregate_field_unsigned` 之后）添加：

```cpp
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
```

无需额外的前向声明——`Filesort`、`ORDER` 等类型仅在 `.cc` 文件中使用。

---

### Step 2: 实现 ORDER BY 验证函数

**文件**: `sql/ps_point_plan_cache.cc`

在 `ps_point_plan_validate_simple_aggregate()` 之后添加：

```cpp
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

  // Only single-column ORDER BY
  if (order->next != nullptr) return false;

  Item *item = order->item[0]->real_item();
  if (item->type() != Item::FIELD_ITEM) return false;

  Item_field *field = down_cast<Item_field *>(item);
  if (field->table_ref != tpl->table_ref) return false;

  // Table must be open for field metadata access
  if (field->field == nullptr) return false;

  tpl->order_field_index = field->field_index;
  tpl->order_direction_desc = (order->direction == ORDER_DESC);
  tpl->order_field_type = field->field->type();
  tpl->order_field_unsigned = field->field->is_unsigned();
  tpl->order_collation = field->field->charset();

  return true;
}
```

---

### Step 3: 修改 classify 函数

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_classify()` 的 Gate 4（当前第 381-383 行）。

**当前代码**：
```cpp
  if (qb->is_distinct() || qb->is_ordered() ||
      qb->has_limit() || qb->has_windows() || qb->has_ft_funcs())
    return false;
```

**修改后**：
```cpp
  /* Gate 4: no DISTINCT, LIMIT, window functions, FULLTEXT. */
  if (qb->is_distinct() || qb->has_limit() ||
      qb->has_windows() || qb->has_ft_funcs())
    return false;

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
```

在 tpl 初始化之后（当前第 404 行 aggregate 验证之后），添加 ORDER BY 验证：

```cpp
  /*
    Gate 4c: validate simple ORDER BY shape.
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
```

---

### Step 4: 修改 WHERE shape 提取

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_extract_where_shape()` 中 Shape C 的 plan_type 决策
（当前第 479-481 行）。

**当前代码**：
```cpp
      tpl->plan_type = qb->agg_func_used()
                            ? PsCachedPlanType::RANGE_PK_BETWEEN_AGG
                            : PsCachedPlanType::RANGE_PK_BETWEEN;
```

**修改后**：
```cpp
      if (qb->agg_func_used()) {
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN_AGG;
      } else if (qb->is_ordered()) {
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN_SORT;
      } else {
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN;
      }
```

优先级：AGG > SORT > plain RANGE。Gate 4a 已确保 AGG + SORT 不共存。

---

### Step 5: 修改 can_admit

**文件**: `sql/ps_point_plan_cache.cc`

在 `ps_point_plan_can_admit()` 的 `RANGE_PK_BETWEEN_AGG` 分支之后、
`POINT_EQ_REF` 分支之前，添加 `RANGE_PK_BETWEEN_SORT` 分支：

```cpp
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_SORT) {
    if (!ps_point_plan_can_admit_range_between(tpl, join)) return false;

    /*
      Verify that the optimizer produced a SORT access path as root.
      The expected structure is:
        SORT -> INDEX_RANGE_SCAN
      or (with redundant filter):
        SORT -> FILTER -> INDEX_RANGE_SCAN
    */
    AccessPath *root_path = join->root_access_path();
    if (root_path == nullptr || root_path->type != AccessPath::SORT)
      return false;

    /*
      Verify the SORT node has a valid Filesort (the optimizer always
      creates one for ORDER BY that cannot be satisfied by index order).
    */
    if (root_path->sort().filesort == nullptr) return false;
    if (root_path->sort().order == nullptr) return false;

    /*
      Walk through SORT's child: allow optional FILTER (redundant
      for pure PK BETWEEN), require INDEX_RANGE_SCAN on the PK.
    */
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
```

**设计说明**：

- 复用 `can_admit_range_between()` 验证 QEP_TAB 层的范围扫描（JT_RANGE,
  单列 PK, 参数有效性等）。
- 额外验证 AccessPath 根节点是 `SORT`，且 `Filesort` 和 `ORDER` 指针非空。
- 允许 SORT → FILTER → INDEX_RANGE_SCAN（与 AGG 路径相同的 FILTER 容忍策略，
  原因一致：classify() 限定 WHERE 仅为 `pk BETWEEN ? AND ?`，FILTER 是冗余的）。

---

### Step 6: 修改 admit

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_admit()` 中的范围类型分支（当前第 1547-1548 行条件）。

**当前代码**：
```cpp
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN ||
      tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_AGG) {
    ps_point_plan_admit_range_metadata(thd, tpl, join);
    ps_point_plan_admit_range_arena_cache(thd, stmt, tpl);
    ...
  }
```

**修改后**：
```cpp
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN ||
      tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_AGG ||
      tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_SORT) {
    ps_point_plan_admit_range_metadata(thd, tpl, join);
    ps_point_plan_admit_range_arena_cache(thd, stmt, tpl);

    stmt->set_ps_point_plan_state(PsPointPlanState::HOT);
    stmt->set_ps_point_plan_retryable_cold(false);
    ps_point_plan_mark_admission(thd);
    return;
  }
```

**设计说明**：

- `RANGE_PK_BETWEEN_SORT` 的 range 层 metadata 与 `RANGE_PK_BETWEEN` 完全
  一致（相同的 PK 范围扫描），直接复用两个 helper。
- ORDER BY 列的元数据（`order_field_index`、`order_direction_desc`、
  `order_field_type`、`order_collation`）已在 classify 阶段由
  `validate_simple_order_by()` 捕获，不需要在 admission 阶段重新提取。
- Arena 缓存同样复用——QEP_TAB、store_key、key buffer 等与 range 路径共享。
  Filesort 不缓存（参见「决策 1」）。

---

### Step 7: 修改 build_fast_path

**文件**: `sql/ps_point_plan_cache.cc`

在 `ps_point_plan_build_fast_path()` 中，`RANGE_PK_BETWEEN_AGG` 分支之后、
`RANGE_PK_BETWEEN` 分支之前，添加 `RANGE_PK_BETWEEN_SORT` 分支：

```cpp
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_SORT) {
    QEP_TAB *new_qep_tab = nullptr;
    AccessPath *range_path = nullptr;
    if (!ps_point_plan_build_range_components(thd, join, tpl, table, keyinfo,
                                              &new_qep_tab, &range_path))
      return false;

    /*
      Construct a fresh Filesort on thd->mem_root.

      Constructor arguments:
        thd            - current thread
        {table}        - single-element Mem_root_array<TABLE*>
        keep_buffers   - false (not an uncacheable subquery)
        order          - ORDER* from query_block->order_list (stable)
        limit          - HA_POS_ERROR (no LIMIT; classify rejects LIMIT)
        remove_dups    - false
        force_rowids   - false (pure SELECT, no weedout)
        unwrap_rollup  - false (no rollup)

      make_sortorder() inside the constructor converts the ORDER list
      to st_sort_field[] on THR_MALLOC.  At this point in optimize(),
      REF_SLICE_ACTIVE is selected, so ORDER::item[0] resolves to the
      original Item_field from the parse tree.
    */
    Filesort *filesort = new (thd->mem_root)
        Filesort(thd, Mem_root_array<TABLE *>{thd->mem_root, &table, 1},
                 /*keep_buffers=*/false, join->order.order,
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
    ps_point_plan_mark_hit(thd);
    return true;
  }
```

**`Mem_root_array<TABLE*>` 构造说明**：

`Filesort` 构造函数接收 `Mem_root_array<TABLE*>` by value（move），
需要确保初始化列表正确。标准调用模式（参考 `add_sorting_to_table`）是：

```cpp
Filesort(thd, {tab->table()}, keep_buffers, ...)
```

此处 `{table}` 是 brace-init 为 `Mem_root_array<TABLE*>`，其内部数组
分配在当前 `thd->mem_root` 上。如果编译器对 brace-init 有歧义，
可显式构造：

```cpp
Mem_root_array<TABLE *> sort_tables(thd->mem_root);
sort_tables.push_back(table);
Filesort *filesort = new (thd->mem_root)
    Filesort(thd, std::move(sort_tables), ...);
```

**AccessPath 树结构**：

```
sort_path (SORT)
 ├── filesort: Filesort* (per-execution, on thd->mem_root)
 ├── order: ORDER* (from parse tree, stable)
 └── child: range_path (INDEX_RANGE_SCAN)
              ├── index: tpl.keyno (primary key)
              ├── ranges[0]: QUICK_RANGE (min_key, max_key from params)
              └── used_key_part[0]: PK key part
```

**MANDATORY INVARIANT 兼容性**：

与 `RANGE_PK_BETWEEN_AGG` 不同，SORT 路径**不需要**在构建范围组件之前修改
任何 JOIN 成员。`Filesort` 和 `SORT AccessPath` 的构造是纯分配操作，
不改变 JOIN 状态。只有 `commit_range_to_join` 最终一次性修改 JOIN 成员。
因此完全符合 MANDATORY INVARIANT。

---

### Step 8: 修改 runtime_guard

**文件**: `sql/ps_point_plan_cache.cc`

在 `ps_point_plan_runtime_guard()` 的 G12（aggregate field drift detection）
之后，添加 G13（ORDER BY field drift detection）：

```cpp
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
    /*
      Collation change affects sort order semantics.
      Use hard invalidation (not demotion) because the Filesort result
      would be sorted in the wrong order with a stale collation.
    */
    if (order_field->charset() != tpl.order_collation) {
      stmt->invalidate_ps_point_plan_cache();
      ps_point_plan_mark_invalidation(thd);
      return false;
    }
  }
```

**设计说明**：

- 字段类型变化（DDL 如 `ALTER TABLE ... MODIFY COLUMN c`）→ hard invalidation。
- 字符集/排序规则变化 → hard invalidation（因为排序语义改变，demote 后
  re-admit 可能缓存错误的排序结果）。
- `table_ref_version` drift（G1d）通常已经覆盖了 DDL 场景，但 G13 作为
  belt-and-suspenders 进一步保护。

---

### Step 9: 修改 clear_hot_metadata

**文件**: `sql/ps_point_plan_cache.cc`

在 `ps_point_plan_clear_hot_metadata()` 中（当前第 121-125 行 aggregate
字段清理之后），添加 ORDER BY 字段清理：

```cpp
  tpl->has_order_by = false;
  tpl->order_field_index = MAX_KEY;
  tpl->order_direction_desc = false;
  tpl->order_field_type = MYSQL_TYPE_INVALID;
  tpl->order_field_unsigned = false;
  tpl->order_collation = nullptr;
```

---

## 需要添加的 #include

**文件**: `sql/ps_point_plan_cache.cc`

```cpp
#include "sql/filesort.h"    // Filesort constructor
```

`NewSortAccessPath` 的声明在 `sql/join_optimizer/access_path.h`（已包含）。
`ORDER` 的定义在 `sql/table.h`（已间接包含）。

---

## 异常场景处理

| 场景 | 阶段 | 处理方式 |
|------|------|---------|
| ORDER BY 多列 | classify | `validate_simple_order_by` 返回 false → NEVER |
| ORDER BY 表达式（非物理列） | classify | `validate_simple_order_by` 返回 false → NEVER |
| ORDER BY + 聚合 | classify | Gate 4a 拒绝 → NEVER |
| ORDER BY + DISTINCT | classify | Gate 4 拒绝（`is_distinct()`）→ false |
| ORDER BY + LIMIT | classify | Gate 4 拒绝（`has_limit()`）→ false |
| 低/高参数 NULL | runtime_guard | G7 → runtime fallback（模板保持 HOT） |
| 低 > 高（空范围） | build_fast_path | `key_cmp2` 检查 → runtime fallback |
| 参数类型变化 | runtime_guard | G8 → demote to COLD |
| 排序列类型 DDL 变更 | runtime_guard | G13 → hard invalidation |
| 排序列字符集变更 | runtime_guard | G13 → hard invalidation |
| Filesort 内存分配失败 | build_fast_path | `filesort == nullptr` → runtime fallback |
| `NewSortAccessPath` 分配失败 | build_fast_path | `sort_path == nullptr` → runtime fallback |
| sort_buffer_size 不足 | 执行期 | Filesort 自动 spill 到磁盘临时文件（正常行为） |

---

## AccessPath 生命周期

```
┌─────────────────────────────────────────────────────────────────┐
│                     PS arena (跨执行存活)                         │
│  PsPointPlanTemplate:                                           │
│    - range metadata (keyno, key_parts, range_flag, ...)         │
│    - ORDER BY metadata (order_field_index, direction, ...)      │
│    - cached_range_qep_tab, cached_range_qep_shared              │
│    - cached_range_key_part, cached_range_array                  │
│    - cached_range_min_key, cached_range_max_key                 │
│    - cached_range_low_store, cached_range_high_store            │
│                                                                 │
│  NOTE: Filesort is NOT here                                     │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                  thd->mem_root (每次执行释放)                     │
│  Per-execution:                                                  │
│    - QUICK_RANGE (from build_range_components)                  │
│    - INDEX_RANGE_SCAN AccessPath (from build_range_components)  │
│    - Filesort object (new this execution)                       │
│    - st_sort_field[] (allocated by make_sortorder via THR_MALLOC)│
│    - SORT AccessPath (from NewSortAccessPath)                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## MTR 测试用例

建议在 `mysql-test/t/ps_point_plan_cache_order.test` 中添加：

```sql
-- 基础功能测试
CREATE TABLE sbtest1 (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  k INT NOT NULL DEFAULT 0,
  c CHAR(120) NOT NULL DEFAULT '',
  pad CHAR(60) NOT NULL DEFAULT ''
) ENGINE=InnoDB;

INSERT INTO sbtest1 (k, c, pad)
  SELECT seq, CONCAT('val-', seq), CONCAT('pad-', seq)
  FROM (SELECT @rownum := @rownum + 1 AS seq
        FROM information_schema.COLUMNS a,
             information_schema.COLUMNS b LIMIT 100) t,
       (SELECT @rownum := 0) r;

SET GLOBAL ps_point_plan_cache = ON;

-- Test 1: 基础 order_ranges 缓存
PREPARE stmt1 FROM 'SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c';
SET @lo = 1, @hi = 10;
EXECUTE stmt1 USING @lo, @hi;
-- 第一次执行: COLD → HOT (admission)

SET @lo = 20, @hi = 30;
EXECUTE stmt1 USING @lo, @hi;
-- 第二次执行: 应命中 cache (HOT fast path)

-- 验证命中
SELECT VARIABLE_VALUE INTO @hits_after
  FROM performance_schema.session_status
  WHERE VARIABLE_NAME = 'Ps_point_plan_cache_hits';
-- @hits_after 应 >= 1

-- Test 2: ORDER BY DESC
PREPARE stmt2 FROM 'SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c DESC';
EXECUTE stmt2 USING @lo, @hi;
EXECUTE stmt2 USING @lo, @hi;

-- Test 3: NULL 参数 fallback
SET @lo = NULL;
EXECUTE stmt1 USING @lo, @hi;
-- 应 fallback 到正常优化器

-- Test 4: 空范围 fallback
SET @lo = 100, @hi = 1;
EXECUTE stmt1 USING @lo, @hi;
-- 应 fallback (low > high)

-- Test 5: DDL invalidation
ALTER TABLE sbtest1 MODIFY COLUMN c VARCHAR(200) NOT NULL DEFAULT '';
SET @lo = 1, @hi = 10;
EXECUTE stmt1 USING @lo, @hi;
-- 应 reprepare 或 invalidate

-- Test 6: ORDER BY + aggregate 被拒绝
PREPARE stmt_bad FROM 'SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c';
-- 应该成功 prepare 但状态为 NEVER (不进入 cache)

-- Test 7: ORDER BY + LIMIT 被拒绝
PREPARE stmt_limit FROM 'SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c LIMIT 10';
-- 状态应为 NEVER

-- Test 8: 多列 ORDER BY 被拒绝
PREPARE stmt_multi FROM 'SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c, k';
-- 状态应为 NEVER

-- Cleanup
DROP PREPARE stmt1;
DROP PREPARE stmt2;
DROP TABLE sbtest1;
SET GLOBAL ps_point_plan_cache = DEFAULT;
```

---

## 与现有代码的复用关系

```
ps_point_plan_classify()
  ├── Gate 4: 放宽 is_ordered() [修改]
  ├── Gate 4a: ORDER BY + AGG 拒绝 [新增]
  ├── validate_simple_order_by() [新增]
  └── extract_where_shape() → RANGE_PK_BETWEEN_SORT [修改]

ps_point_plan_can_admit()
  ├── can_admit_range_between() [复用]
  └── SORT AccessPath 结构验证 [新增]

ps_point_plan_admit()
  ├── admit_range_metadata() [复用]
  └── admit_range_arena_cache() [复用]

ps_point_plan_build_fast_path()
  ├── runtime_guard() [复用]
  ├── build_range_components() [复用]
  ├── Filesort 构造 [新增, 每次执行]
  ├── NewSortAccessPath() [新增]
  └── commit_range_to_join() [复用]

ps_point_plan_runtime_guard()
  ├── G1-G12 [复用]
  └── G13: ORDER BY field drift [新增]

ps_point_plan_clear_hot_metadata()
  └── ORDER BY 字段清理 [新增]
```

---

## 未来扩展（Phase 10 预备）

Phase 10 (distinct_ranges: `SELECT DISTINCT c ... ORDER BY c`) 可在 SORT 基础上
扩展，主要差异：

1. `Filesort` 构造时 `remove_duplicates = true`（Filesort 内建去重）
2. 或在 SORT 之上添加 `REMOVE_DUPLICATES_ON_INDEX` / `REMOVE_DUPLICATES`
   AccessPath 层
3. classify 放宽 `is_distinct()` 门控
4. 新增 plan type `RANGE_PK_BETWEEN_SORT_DISTINCT`

Phase 9 的 SORT 路径基础设施（Filesort 构造、ORDER 指针传递、
sort field drift guard）可直接被 Phase 10 复用。
