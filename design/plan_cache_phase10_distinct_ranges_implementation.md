# Plan Cache Phase 10: distinct_ranges Implementation

## Overview

本文档是 `plan_cache_phase7_10_read_only_coverage.md` Phase 10 的详细实现方案。
目标：支持 `SELECT DISTINCT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c` 查询模式的计划缓存。

**预期收益**：distinct_ranges 约占 sysbench oltp_read_only 优化器资源消耗的 30-40%，
是四种 range 模式中最重的单条SQL。缓存后可实现 oltp_read_only 16条SQL的 100% 覆盖，
预计总体 QPS 额外提升 10-20%（在 Phase 7-9 基础上）。

---

## 目标SQL模式

```sql
-- 支持的模式（Phase 10 范围）
SELECT DISTINCT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c
SELECT DISTINCT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c ASC
SELECT DISTINCT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c DESC

-- 特点：
-- 1. 单表查询
-- 2. 主键范围扫描 (BETWEEN)
-- 3. DISTINCT + ORDER BY 同一列物理字段
-- 4. SELECT list 仅包含 DISTINCT/ORDER BY 的那一列
-- 5. 无 GROUP BY, LIMIT, HAVING, 聚合函数, 窗口函数
```

**优化器正常路径行为：**

优化器在 `optimize_distinct_group_order()`（`sql/sql_optimizer.cc:1601-1671`）中
将 DISTINCT 转换为 GROUP BY，产生包含临时表物化的复杂 AccessPath 树：

```
SORT (ORDER BY c, on tmp table)
 └── MATERIALIZE (GROUP BY c, temp table)
      └── INDEX_RANGE_SCAN (pk: id BETWEEN ? AND ?)
```

**快速路径构建的等价 AccessPath 树：**

```
SORT (Filesort with remove_duplicates=true, ORDER BY c)
 └── INDEX_RANGE_SCAN (pk: id BETWEEN ? AND ?)
```

快速路径不复制优化器的 DISTINCT → GROUP BY → 临时表 路径，而是使用
`Filesort(remove_duplicates=true)` 在单次排序中同时完成排序和去重。

---

## 关键设计决策

### 决策 1: Filesort(remove_duplicates=true) 实现排序+去重一体化

**方案选择**：

| 方案 | 描述 | 优劣 |
|------|------|------|
| A. 复制优化器路径 | GROUP BY + 临时表物化 | 复杂度极高，需构建 MATERIALIZE AccessPath |
| B. SORT + REMOVE_DUPLICATES | 两个独立 AccessPath 节点 | 旧执行器不使用 REMOVE_DUPLICATES 节点 |
| **C. Filesort(dedup)** | **单个 SORT 节点，Filesort 内建去重** | **最简，复用 Phase 9 基础设施** |

选择方案 C。`Filesort` 构造函数接受 `remove_duplicates` 参数：

```cpp
Filesort::Filesort(THD *thd, Mem_root_array<TABLE *> tables_arg,
                   bool keep_buffers_arg, ORDER *order, ha_rows limit_arg,
                   bool remove_duplicates, ...)  // <-- 此参数
```

当 `remove_duplicates=true` 时：

1. **内存排序路径**（`filesort_utils.cc:147-165`）：排序后调用 `std::unique`
   移除相邻重复行（基于排序键比较）。
2. **外部归并路径**（`filesort.cc:1977-1987`）：在 `merge_buffers()` 中跳过
   排序键与上一行相同的行。
3. **优先队列（LIMIT）优化被禁用**（`filesort.cc:1664-1668`）：
   去重时不能使用 nth_element 预过滤。

**正确性约束**：Filesort 的去重键 = 排序键 = ORDER BY 列。对于
`SELECT DISTINCT c ... ORDER BY c`，去重键和 DISTINCT 键都是 `c`，语义一致。
**仅当 DISTINCT 列与 ORDER BY 列完全一致时**此方案才正确。Gate 4e 强制保证此约束。

### 决策 2: 宽松的 can_admit() AccessPath 树匹配

优化器首次执行产出的 AccessPath 树是复杂的 GROUP BY + 临时表结构，
与快速路径构建的 `SORT → INDEX_RANGE_SCAN` 结构完全不同。

`can_admit()` 不严格匹配根节点的 AccessPath 类型，仅验证：
1. `can_admit_range_between()` — 基表 QEP_TAB[0] 是有效的 PK 范围扫描
2. `root_access_path() != nullptr` — 优化器成功产出了执行计划

这是安全的，因为：
- `classify()` 的多层 Gate 已验证查询语义（DISTINCT + 单列 ORDER BY + 同列匹配）
- `runtime_guard()` 的 G13 检查排序列漂移
- 快速路径构建的树是语义等价的

### 决策 3: 快速路径直接使用 join->order.order

快速路径钩子在 `sql/sql_optimizer.cc:381` 触发，此时：

1. `join->order.order` 仍指向解析树的 ORDER BY 链表（未被 `optimize_distinct_group_order()` 修改）
2. `join->select_distinct` 仍为 true（未被转换为 GROUP BY）
3. `ORDER::item[0]` 解析到原始 `Item_field`，`Field*` 指针有效（表已打开）

因此 Filesort 构造可直接使用 `join->order.order` 作为排序顺序。

### 决策 4: 严格限定 DISTINCT 列 = ORDER BY 列

仅支持以下模式：
- SELECT list 恰好一个可见物理列
- 该列与 ORDER BY 列相同
- 不支持多列 DISTINCT、DISTINCT 列 ≠ ORDER BY 列

此限制确保 Filesort 去重键与 DISTINCT 语义完全一致。覆盖 sysbench 模式足够。

---

## 修改文件清单

| 文件 | 修改类型 | 行数估算 |
|------|----------|----------|
| `sql/ps_point_plan_cache.h` | 修改 | +5 |
| `sql/ps_point_plan_cache.cc` | 修改 | +100 |
| **总计** | | **~105 行** |

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
  RANGE_PK_BETWEEN_SORT_DISTINCT,  // Phase 10: SELECT DISTINCT col ... ORDER BY col
};
```

#### 1.2 扩展 PsPointPlanTemplate 结构体

在 `order_collation` 之后添加：

```cpp
  /// True when the query has DISTINCT matching the ORDER BY column.
  bool has_distinct{false};
```

无需其他新字段——DISTINCT 列与 ORDER BY 列相同（由 Gate 4e 保证），
ORDER BY 元数据（`order_field_index`、`order_direction_desc`、`order_field_type`、
`order_collation`）已完整描述该列。

---

### Step 2: 实现 DISTINCT 验证函数

**文件**: `sql/ps_point_plan_cache.cc`

在 `ps_point_plan_validate_simple_order_by()` 之后添加：

```cpp
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
    if (field->field_index != tpl->order_field_index) return false;
  }
  return visible_count == 1;
}
```

需要添加头文件：

```cpp
#include "sql/visible_fields.h"    // VisibleFields iteration
```

---

### Step 3: 修改 classify 函数

**文件**: `sql/ps_point_plan_cache.cc`

#### 3.1 放宽 Gate 4 的 DISTINCT 限制

**当前代码**：
```cpp
  /* Gate 4: no DISTINCT, LIMIT, window functions, FULLTEXT. */
  if (qb->is_distinct() || qb->has_limit() ||
      qb->has_windows() || qb->has_ft_funcs())
    return false;
```

**修改后**：
```cpp
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
```

#### 3.2 在 ORDER BY 验证之后添加 DISTINCT 验证

在 Gate 4d（ORDER BY 验证）之后添加：

```cpp
  /*
    Gate 4e: validate simple DISTINCT.
    Requires single-column SELECT list matching the ORDER BY column.
    Must be after Gate 4d (ORDER BY validation) since validate_simple_distinct
    reads tpl->has_order_by and tpl->order_field_index.
  */
  if (qb->is_distinct()) {
    if (!ps_point_plan_validate_simple_distinct(qb, &tpl)) {
      stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);
      return false;
    }
    tpl.has_distinct = true;
  }
```

---

### Step 4: 修改 WHERE shape 提取

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_extract_where_shape()` 中 Shape C 的 plan_type 决策。

**当前代码**（三路分支）：
```cpp
      if (qb->agg_func_used()) {
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN_AGG;
      } else if (qb->is_ordered()) {
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN_SORT;
      } else {
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN;
      }
```

**修改后**（四路分支）：
```cpp
      if (qb->agg_func_used()) {
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN_AGG;
      } else if (qb->is_distinct()) {
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN_SORT_DISTINCT;
      } else if (qb->is_ordered()) {
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN_SORT;
      } else {
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN;
      }
```

优先级：AGG > DISTINCT > SORT > plain RANGE。

- Gate 4a 已确保 AGG + ORDER BY 不共存。
- Gate 4-pre 已确保 DISTINCT 必须伴随 ORDER BY 且无 AGG。
- `is_distinct()` 蕴含 `is_ordered()`（由 Gate 4-pre 保证），所以
  DISTINCT 分支优先于 SORT 分支是正确的。

---

### Step 5: 修改 can_admit

**文件**: `sql/ps_point_plan_cache.cc`

在 `ps_point_plan_can_admit()` 的 `RANGE_PK_BETWEEN_SORT` 分支之后添加：

```cpp
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_SORT_DISTINCT) {
    if (!ps_point_plan_can_admit_range_between(tpl, join)) return false;

    /*
      The optimizer converts DISTINCT to GROUP BY and uses temp table
      materialization, producing a complex AccessPath tree (typically
      SORT -> MATERIALIZE -> INDEX_RANGE_SCAN).  We don't match the
      exact tree shape — only verify the base table access is a valid
      PK range scan via can_admit_range_between().

      The fast path builds a simpler SORT(Filesort with dedup) ->
      INDEX_RANGE_SCAN tree which is semantically equivalent.

      classify() gates (4-pre, 4d, 4e) have already validated:
        - DISTINCT + ORDER BY on the same single physical column
        - No aggregates, LIMIT, windows, etc.
    */
    AccessPath *root_path = join->root_access_path();
    if (root_path == nullptr) return false;

    return true;
  }
```

**设计说明**：

- 复用 `can_admit_range_between()` 验证 QEP_TAB 层的范围扫描。
- 不严格匹配根 AccessPath 类型——优化器的 DISTINCT → GROUP BY 转换
  产生多种可能的树结构（SORT → MATERIALIZE、STREAM 等），逐一匹配
  既复杂又脆弱。
- 仅验证 `root_access_path() != nullptr`（优化器成功产出计划）。

---

### Step 6: 修改 admit

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_admit()` 中的范围类型分支条件。

**当前代码**：
```cpp
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN ||
      tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_AGG ||
      tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_SORT) {
```

**修改后**：
```cpp
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN ||
      tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_AGG ||
      tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_SORT ||
      tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_SORT_DISTINCT) {
```

**设计说明**：

- range 层 metadata 与 `RANGE_PK_BETWEEN_SORT` 完全一致（相同的 PK 范围扫描）。
- ORDER BY + DISTINCT 列的元数据已在 classify 阶段捕获。
- Arena 缓存同样复用——QEP_TAB、store_key、key buffer 与 range 路径共享。

---

### Step 7: 修改 build_fast_path

**文件**: `sql/ps_point_plan_cache.cc`

在 `ps_point_plan_build_fast_path()` 中，`RANGE_PK_BETWEEN_SORT` 分支之后添加：

```cpp
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_SORT_DISTINCT) {
    QEP_TAB *new_qep_tab = nullptr;
    AccessPath *range_path = nullptr;
    if (!ps_point_plan_build_range_components(thd, join, tpl, table, keyinfo,
                                              &new_qep_tab, &range_path))
      return false;

    /*
      Construct a fresh Filesort with remove_duplicates=true.

      This is the ONLY difference from RANGE_PK_BETWEEN_SORT:
      the Filesort sorts by ORDER BY column and simultaneously
      removes adjacent duplicate sort keys, implementing DISTINCT.

      Correctness relies on Gate 4e ensuring DISTINCT column ==
      ORDER BY column, so the sort key == dedup key == DISTINCT key.
    */
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
    ps_point_plan_mark_hit(thd);
    return true;
  }
```

**AccessPath 树结构**：

```
sort_path (SORT)
 ├── filesort: Filesort* (per-execution, remove_duplicates=true)
 ├── order: ORDER* (from parse tree, stable)
 └── child: range_path (INDEX_RANGE_SCAN)
              ├── index: tpl.keyno (primary key)
              ├── ranges[0]: QUICK_RANGE (min_key, max_key)
              └── used_key_part[0]: PK key part
```

**与 Phase 9 SORT 分支的差异**：

仅 Filesort 构造函数的 `remove_duplicates` 参数不同：
- Phase 9: `/*remove_duplicates=*/false`
- Phase 10: `/*remove_duplicates=*/true`

**MANDATORY INVARIANT 兼容性**：

与 `RANGE_PK_BETWEEN_SORT` 相同，此路径不在构建范围组件之前修改任何
JOIN 成员。`Filesort` 和 `SORT AccessPath` 的构造是纯分配操作。
完全符合 MANDATORY INVARIANT。

---

### Step 8: 修改 clear_hot_metadata

**文件**: `sql/ps_point_plan_cache.cc`

在 `ps_point_plan_clear_hot_metadata()` 中 ORDER BY 字段清理之后添加：

```cpp
  tpl->has_distinct = false;
```

---

### Step 9: 添加 #include

**文件**: `sql/ps_point_plan_cache.cc`

```cpp
#include "sql/visible_fields.h"    // VisibleFields iteration
```

---

## Runtime Guard — 无需新增

G13（ORDER BY field drift detection）已保护 DISTINCT 列，因为
DISTINCT 列 IS ORDER BY 列（由 Gate 4e 强制保证）。

如果 ORDER BY 列的类型或排序规则发生变化（DDL 如 `ALTER TABLE MODIFY COLUMN`），
G13 触发 hard invalidation，同时使 DISTINCT 语义失效。

不需要独立的 DISTINCT 列漂移检查。

---

## 异常场景处理

| 场景 | 阶段 | 处理方式 |
|------|------|---------|
| DISTINCT 多列 | classify | `validate_simple_distinct` → NEVER |
| DISTINCT 列 ≠ ORDER BY 列 | classify | `validate_simple_distinct` → NEVER |
| DISTINCT + 聚合 | classify | Gate 4-pre → NEVER |
| DISTINCT 无 ORDER BY | classify | Gate 4-pre → NEVER |
| DISTINCT + LIMIT | classify | Gate 4 → false（不设 NEVER） |
| DISTINCT + GROUP BY | classify | Gate 拒绝 GROUP BY → false |
| 低/高参数 NULL | runtime_guard | G7 → runtime fallback |
| 低 > 高（空范围） | build_fast_path | `key_cmp2` → runtime fallback |
| 参数类型变化 | runtime_guard | G8 → demote to COLD |
| 排序列类型 DDL 变更 | runtime_guard | G13 → hard invalidation |
| 排序列字符集变更 | runtime_guard | G13 → hard invalidation |
| Filesort 内存分配失败 | build_fast_path | → runtime fallback |
| `NewSortAccessPath` 分配失败 | build_fast_path | → runtime fallback |
| sort_buffer_size 不足 | 执行期 | Filesort 自动 spill 到磁盘 |

---

## AccessPath 生命周期

```
┌─────────────────────────────────────────────────────────────────┐
│                     PS arena (跨执行存活)                         │
│  PsPointPlanTemplate:                                           │
│    - range metadata (keyno, key_parts, range_flag, ...)         │
│    - ORDER BY metadata (order_field_index, direction, ...)      │
│    - has_distinct = true                                        │
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
│    - Filesort object (remove_duplicates=true)                   │
│    - st_sort_field[] (allocated by make_sortorder via THR_MALLOC)│
│    - SORT AccessPath (from NewSortAccessPath)                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 与现有代码的复用关系

```
ps_point_plan_classify()
  ├── Gate 4: 放宽 is_distinct() [修改]
  ├── Gate 4-pre: DISTINCT + !ORDER BY 或 + AGG 拒绝 [新增]
  ├── Gate 4e: validate_simple_distinct() [新增]
  └── extract_where_shape() → RANGE_PK_BETWEEN_SORT_DISTINCT [修改]

ps_point_plan_can_admit()
  ├── can_admit_range_between() [复用]
  └── 宽松根路径检查 [新增]

ps_point_plan_admit()
  ├── admit_range_metadata() [复用]
  └── admit_range_arena_cache() [复用]

ps_point_plan_build_fast_path()
  ├── runtime_guard() [复用]
  ├── build_range_components() [复用]
  ├── Filesort 构造 (remove_duplicates=true) [新增，与 Phase 9 仅此一差]
  ├── NewSortAccessPath() [复用]
  └── commit_range_to_join() [复用]

ps_point_plan_runtime_guard()
  ├── G1-G12 [复用]
  └── G13: ORDER BY field drift [复用，同时保护 DISTINCT 列]

ps_point_plan_clear_hot_metadata()
  └── has_distinct 清理 [新增]
```

---

## Filesort 去重机制详解

### 内存排序路径

```
输入行: [c='val3', c='val1', c='val3', c='val2', c='val1']
         ↓ sort by c
排序后: [c='val1', c='val1', c='val2', c='val3', c='val3']
         ↓ std::unique (remove adjacent duplicates)
去重后: [c='val1', c='val2', c='val3']
```

代码路径：`sql/filesort_utils.cc:147-165`

```cpp
sort(it_begin, it_end, comp);
if (param->m_remove_duplicates) {
  num_input_rows =
      unique(it_begin, it_end,
             Equality_from_less<Mem_compare_varlen_key>(comp)) -
      it_begin;
}
```

### 外部归并路径

当数据量超过 `sort_buffer_size` 时，Filesort 使用外部归并排序。
去重在归并阶段完成（`sql/filesort.cc:1977-1987`）：

```cpp
if (param->m_remove_duplicates) {
  if (seen_any_records &&
      !mcl.key_is_greater_than(merge_chunk->current_key(),
                               param->m_last_key_seen)) {
    is_duplicate = true;  // skip this row
  } else {
    seen_any_records = true;
    memcpy(param->m_last_key_seen, merge_chunk->current_key(), ...);
  }
}
```

去重基于编码后的排序键比较，而非逐行 SQL `=` 语义比较。排序键由
`make_sortorder()` 从 ORDER 列表生成。

---

## MTR 测试用例

建议在 `mysql-test/t/ps_point_plan_cache_distinct.test` 中添加：

```sql
-- 基础功能测试
CREATE TABLE sbtest1 (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  k INT NOT NULL DEFAULT 0,
  c CHAR(120) NOT NULL DEFAULT '',
  pad CHAR(60) NOT NULL DEFAULT ''
) ENGINE=InnoDB;

INSERT INTO sbtest1 (k, c, pad)
  SELECT seq, CONCAT('val-', seq % 20), CONCAT('pad-', seq)
  FROM (SELECT @rownum := @rownum + 1 AS seq
        FROM information_schema.COLUMNS a,
             information_schema.COLUMNS b LIMIT 100) t,
       (SELECT @rownum := 0) r;

SET GLOBAL ps_point_plan_cache = ON;

-- Test 1: 基础 distinct_ranges 缓存
PREPARE stmt1 FROM 'SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c';
SET @lo = 1, @hi = 50;
EXECUTE stmt1 USING @lo, @hi;
-- 第一次执行: COLD → HOT (admission)

SET @lo = 20, @hi = 80;
EXECUTE stmt1 USING @lo, @hi;
-- 第二次执行: 应命中 cache (HOT fast path)

-- 验证命中
SELECT VARIABLE_VALUE INTO @hits_after
  FROM performance_schema.session_status
  WHERE VARIABLE_NAME = 'Ps_point_plan_cache_hits';
-- @hits_after 应 >= 1

-- Test 2: ORDER BY DESC
PREPARE stmt2 FROM 'SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c DESC';
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
SET @lo = 1, @hi = 50;
EXECUTE stmt1 USING @lo, @hi;
-- 应 reprepare 或 invalidate

-- Test 6: DISTINCT + aggregate 被拒绝
PREPARE stmt_bad1 FROM 'SELECT DISTINCT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c';
-- 状态应为 NEVER

-- Test 7: DISTINCT + LIMIT 被拒绝
PREPARE stmt_bad2 FROM 'SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c LIMIT 10';
-- 被 Gate 4 拦截

-- Test 8: 多列 DISTINCT 被拒绝
PREPARE stmt_bad3 FROM 'SELECT DISTINCT c, k FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c';
-- 状态应为 NEVER

-- Test 9: DISTINCT 列 ≠ ORDER BY 列 被拒绝
PREPARE stmt_bad4 FROM 'SELECT DISTINCT k FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c';
-- 状态应为 NEVER (DISTINCT k != ORDER BY c)

-- Test 10: DISTINCT 无 ORDER BY 被拒绝
PREPARE stmt_bad5 FROM 'SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ?';
-- 状态应为 NEVER

-- Test 11: 验证去重正确性
SET @lo = 1, @hi = 100;
EXECUTE stmt1 USING @lo, @hi;
-- 结果应该只包含不重复的 c 值，且按 c 排序
-- 验证行数 <= 20 (因为 c = 'val-' || seq%20，最多20个不同值)

-- Cleanup
DROP PREPARE stmt1;
DROP PREPARE stmt2;
DROP TABLE sbtest1;
SET GLOBAL ps_point_plan_cache = DEFAULT;
```

---

## 完成后的 oltp_read_only 覆盖率

```
SQL 类型              Plan Type                     Phase  状态
─────────────────────────────────────────────────────────────────
point_selects (x10)   POINT_EQ_REF                  1-6    ✓
simple_ranges         RANGE_PK_BETWEEN              7      ✓
sum_ranges            RANGE_PK_BETWEEN_AGG          8      ✓
order_ranges          RANGE_PK_BETWEEN_SORT         9      ✓
distinct_ranges       RANGE_PK_BETWEEN_SORT_DISTINCT 10    ← 本次
─────────────────────────────────────────────────────────────────
覆盖率: 14/14 SELECT = 100%   (BEGIN/COMMIT 不经过优化器)
```
