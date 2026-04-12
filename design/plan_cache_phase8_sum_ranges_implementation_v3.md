# Plan Cache Phase 8: sum_ranges Implementation (v3 Final)

> **版本说明**:
> - v1: 初始设计（存在 P0 bug: `clone_item()` 返回 nullptr）
> - v2: 修复 v1 的多个问题，但引入新的 P0 bug（fast path 中 `sum_funcs` 为 nullptr）
> - **v3**: 修复 v2 的 P0/P1/P2 问题，添加完整的聚合初始化流程和 admission guards

## Overview

本文档是 `plan_cache_phase7_10_read_only_coverage.md` Phase 8 的详细实现方案（最终版）。
目标：支持 `SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?` 查询模式的计划缓存。

**预期收益**：sum_ranges 约占 sysbench oltp_read_only 优化器资源消耗的 15-20%，缓存后可提升总体 QPS 5-10%。

---

## v3 修正总结

| 问题 | 严重程度 | v3 修正方案 |
|------|----------|-------------|
| fast path 中 `sum_funcs` 为 nullptr | P0 阻塞 | 入口处手动调用 5 个聚合初始化函数 |
| `implicit_grouping` 未设置 | P0 阻塞 | 显式设置 `join->implicit_grouping = true` |
| Admission 缺失 12 项安全检查 | P1 重要 | 复用 `RANGE_PK_BETWEEN` 完整检查代码 |
| COUNT(*) early return 跳过检查 | P2 建议 | 改为 `continue`，完整遍历 SELECT list |
| `arg_count == 0` 死代码 | P2 建议 | 移除该分支 |
| fast-path 注释过时 | P3 建议 | 更新 `sql_optimizer.cc` 注释 |

---

## 目标SQL模式

```sql
-- 支持的模式
SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?
SELECT COUNT(*) FROM sbtest WHERE id BETWEEN ? AND ?
SELECT COUNT(k) FROM sbtest WHERE id BETWEEN ? AND ?
SELECT MIN(k) FROM sbtest WHERE id BETWEEN ? AND ?
SELECT MAX(k) FROM sbtest WHERE id BETWEEN ? AND ?

-- 特点：
-- 1. 单表查询
-- 2. 主键范围扫描 (BETWEEN)
-- 3. 单一聚合函数 (SUM/COUNT/MIN/MAX)
-- 4. 无 GROUP BY (隐式分组)
-- 5. 无 ORDER BY, DISTINCT, LIMIT, HAVING
```

---

## 修改文件清单

| 文件 | 修改类型 | 行数估算 |
|------|----------|----------|
| `sql/ps_point_plan_cache.h` | 修改 | +25 |
| `sql/ps_point_plan_cache.cc` | 修改 | +220 |
| `sql/sql_optimizer.cc` | 修改 | +15 (更新注释) |
| **总计** | | **~260 行** |

---

## 详细实现步骤

### Step 1: 扩展数据结构

**文件**: `sql/ps_point_plan_cache.h`

#### 1.1 添加前向声明

在文件顶部（约第60行，其他前向声明附近）添加：

```cpp
class Item_sum;  // Phase 8: aggregate function support
```

#### 1.2 扩展 PsCachedPlanType 枚举

```cpp
enum class PsCachedPlanType : uchar {
  POINT_EQ_REF = 0,        // Phase 1-6: WHERE pk = ?
  RANGE_PK_BETWEEN,        // Phase 7:  WHERE pk BETWEEN ? AND ?
  RANGE_PK_BETWEEN_AGG,    // Phase 8:  SELECT SUM() WHERE pk BETWEEN ? AND ?
};
```

#### 1.3 扩展 PsPointPlanTemplate 结构体

在结构体末尾（约第336行，`range_arena_cached` 之后）添加：

```cpp
  /// True when range arena components are populated and usable.
  bool range_arena_cached{false};

  /*
    --- Phase 8: 聚合相关字段 ---
    用于 RANGE_PK_BETWEEN_AGG 类型的计划缓存

    设计决策：不缓存 Item_sum 对象，在 fast path 中通过调用
    count_field_types + alloc_func_list + make_sum_func_list 重新初始化。
    这避免了 Item_sum::clone_item() 返回 nullptr 的问题。
  */

  /// 是否包含聚合函数
  bool has_aggregate{false};

  /// 聚合函数类型 (SUM/COUNT/MIN/MAX)
  Item_sum::Sumfunctype aggregate_type{
      Item_sum::Sumfunctype::SUM_FUNC};

  /// 聚合字段在 TABLE::field[] 中的索引 (COUNT(*) 时为 MAX_KEY)
  uint aggregate_field_index{MAX_KEY};

  /// 聚合字段的数据类型快照（用于运行时类型drift检测）
  enum_field_types aggregate_field_type{MYSQL_TYPE_INVALID};

  /// 聚合字段是否为 unsigned
  bool aggregate_field_unsigned{false};
```

---

### Step 2: 实现聚合验证函数（v3 修正版）

**文件**: `sql/ps_point_plan_cache.cc`

在 `ps_point_plan_extract_where_shape()` 函数之前（约第254行）添加：

```cpp
/**
  Validate simple aggregate for RANGE_PK_BETWEEN_AGG.

  只允许无GROUP BY的单聚合函数 (SUM/COUNT/MIN/MAX)。

  v3 修正:
  - COUNT(*) 使用 continue 而非 return true，确保完整遍历 SELECT list
  - 移除 arg_count == 0 死代码分支

  @param  qb   Query block to validate
  @param  tpl  Template to populate with aggregate metadata
  @retval true  Valid simple aggregate
  @retval false Invalid
*/
static bool ps_point_plan_validate_simple_aggregate(
    Query_block *qb, PsPointPlanTemplate *tpl) {

  // 拒绝显式 GROUP BY
  if (qb->group_list.elements > 0) return false;

  uint sum_count = 0;
  for (Item *item : *qb->fields) {
    if (item->type() == Item::SUM_FUNC_ITEM) {
      sum_count++;
      if (sum_count > 1) return false;  // 多聚合直接拒绝

      Item_sum *sum_item = down_cast<Item_sum *>(item);

      // 拒绝 DISTINCT 聚合
      if (sum_item->has_with_distinct()) return false;

      // COUNT(*) 特殊处理（v3: 使用 continue 确保完整遍历）
      if (sum_item->sum_func() == Item_sum::COUNT_FUNC) {
        // MySQL 8.0 中 COUNT(*) 的 arg_count 始终为 1，args()[0] 是 Item_int
        if (sum_item->arg_count == 1 &&
            sum_item->args()[0]->type() == Item::INT_ITEM) {
          // COUNT(*) 允许，记录元数据后继续检查其余项
          tpl->aggregate_type = sum_item->sum_func();
          tpl->aggregate_field_index = MAX_KEY;
          tpl->aggregate_field_type = MYSQL_TYPE_LONGLONG;
          tpl->aggregate_field_unsigned = true;
          continue;  // v3: 改为 continue，而非 return true
        }
        // COUNT(field) 会继续下面的处理
      }

      // 只允许 SUM/COUNT/MIN/MAX（非DISTINCT变体）
      switch (sum_item->sum_func()) {
        case Item_sum::SUM_FUNC:
        case Item_sum::COUNT_FUNC:
        case Item_sum::MIN_FUNC:
        case Item_sum::MAX_FUNC:
          break;
        default:
          return false;  // AVG/STD/GROUP_CONCAT等不支持
      }

      // 聚合参数必须是单列引用（不能是表达式）
      if (sum_item->arg_count != 1) return false;
      Item *arg = sum_item->args()[0];

      if (arg->type() != Item::FIELD_ITEM) return false;
      Item_field *field = down_cast<Item_field *>(arg);

      // 字段必须来自目标表
      if (field->table_ref != tpl->table_ref) return false;

      // 记录聚合元数据
      tpl->aggregate_type = sum_item->sum_func();
      tpl->aggregate_field_index = field->field_index;
      tpl->aggregate_field_type = field->data_type();
      tpl->aggregate_field_unsigned = field->is_unsigned();

    } else if (item->type() != Item::INT_ITEM) {
      // SELECT list 只允许聚合函数和整型常量
      return false;
    }
  }

  // 必须恰好有一个聚合函数
  return sum_count == 1;
}
```

---

### Step 3: 修改 classify 函数（v3 修正版）

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_classify()` 函数的 Gate 4（约第302-305行）：

```cpp
  /* Gate 4: 拒绝复杂特性（拆分显式GROUP BY和隐式聚合）*/
  if (qb->is_distinct() || qb->is_ordered() ||
      qb->has_limit() || qb->has_windows() || qb->has_ft_funcs())
    return false;

  /* 拒绝显式 GROUP BY */
  if (qb->group_list.elements > 0) return false;

  /*
    Gate 4b: 允许简单聚合（无GROUP BY的单聚合函数）
    使用 agg_func_used() 而非 sum_func_count（后者不存在于 Query_block）
  */
  if (qb->agg_func_used()) {
    if (!ps_point_plan_validate_simple_aggregate(qb, &tpl))
      return false;
    tpl.has_aggregate = true;
  }
```

---

### Step 4: 修改 WHERE shape 提取

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_extract_where_shape()` 函数中的 Shape C 处理（约第381-393行）：

```cpp
    /* Shape C: simple range candidate  WHERE field BETWEEN ? AND ? */
    Item_field *between_field = nullptr;
    Item_param *low = nullptr;
    Item_param *high = nullptr;
    if (extract_between_field_params(func, tbl, &between_field, &low, &high)) {
      // 根据是否有聚合决定plan类型
      Query_block *qb = tbl->query_block;
      if (qb->agg_func_used()) {
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN_AGG;
      } else {
        tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN;
      }
      tpl->params[0] = low;
      tpl->params[1] = high;
      tpl->field_indices[0] = between_field->field_index;
      tpl->field_indices[1] = between_field->field_index;
      tpl->param_count = 2;
      return true;
    }
```

---

### Step 5: 修改 admission 检查（v3 完整版）

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_can_admit()` 函数，在 RANGE_PK_BETWEEN 检查后添加（约第1043行后）：

```cpp
  /* RANGE_PK_BETWEEN_AGG admission checks (v3: 完整 guards) */
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_AGG) {
    // === 基础检查 ===
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

    // === v3: 补齐 RANGE_PK_BETWEEN 的完整检查 ===

    // 检查使用的 key parts 数量
    if (get_used_key_parts(range_scan) != 1) return false;

    // 检查 range 数量
    const QUICK_RANGE *const *ranges = range_scan->index_range_scan().ranges;
    const uint num_ranges = range_scan->index_range_scan().num_ranges;
    if (num_ranges != 1) return false;

    // 空指针检查
    if (ranges == nullptr) return false;
    if (ranges[0] == nullptr) return false;

    const KEY_PART *used_key_part = range_scan->index_range_scan().used_key_part;
    if (used_key_part == nullptr) return false;

    // 字段验证
    if (used_key_part[0].field == nullptr) return false;
    const uint field_index = used_key_part[0].field->field_index();
    if (field_index != tpl.field_indices[0]) return false;

    // 索引属性验证
    const uint keyno = used_index(range_scan);
    const KEY *keyinfo = &table->key_info[keyno];
    if (!(actual_key_flags(keyinfo) & HA_NOSAME)) return false;  // 必须是唯一索引
    if (keyinfo->user_defined_key_parts != 1) return false;      // 必须是单列索引

    // Key part 验证
    if (keyinfo->key_part[0].fieldnr - 1 != tpl.field_indices[0]) return false;
    if (keyinfo->key_part[0].null_bit != 0) return false;        // 拒绝 nullable PK

    // 参数验证
    for (uint i = 0; i < tpl.param_count; i++) {
      if (tpl.params[i] == nullptr) return false;
    }

    // === 聚合路径结构验证 ===
    AccessPath *root_path = join->root_access_path();
    if (root_path == nullptr || root_path->type != AccessPath::AGGREGATE)
      return false;

    if (root_path->aggregate().rollup) return false;  // 无ROLLUP

    // 验证 child 是 INDEX_RANGE_SCAN on primary key（不要求指针相等）
    AccessPath *child = root_path->aggregate().child;
    if (child == nullptr || child->type != AccessPath::INDEX_RANGE_SCAN)
      return false;
    if (used_index(child) != table->s->primary_key) return false;

    // 验证 sum_funcs 非空且只有一个
    if (join->sum_funcs == nullptr) return false;
    int sum_count = 0;
    for (Item_sum **f = join->sum_funcs; *f != nullptr; f++) {
      sum_count++;
    }
    if (sum_count != 1) return false;

    return true;
  }
```

---

### Step 6: 修改 admission 保存元数据

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_admit()` 函数，在 RANGE_PK_BETWEEN 处理后添加（约第1265行后）：

```cpp
  /* RANGE_PK_BETWEEN_AGG admission */
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_AGG) {
    const QEP_TAB *tab = &join->qep_tab[0];
    TABLE *table = tab->table();
    AccessPath *range_scan = tab->range_scan();
    const KEY_PART *used_key_part = range_scan->index_range_scan().used_key_part;
    const uint keyno = used_index(range_scan);
    const KEY *keyinfo = &table->key_info[keyno];
    const uint field_index = used_key_part[0].field->field_index();

    // 复用 RANGE_PK_BETWEEN 的 range metadata 保存
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

    /*
      构建 arena 缓存组件（仅 range 部分）
      注意：不缓存 Item_sum，在 fast path 中通过重新初始化创建
    */
    if (!tpl.range_arena_cached) {
      const bool had_error = thd->is_error();
      Query_arena backup;
      thd->swap_query_arena(stmt->m_arena, &backup);

      const uint key_bytes = keyinfo->key_part[0].store_length;
      bool cache_ok = true;

      // 分配 range 组件
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

    stmt->set_ps_point_plan_state(PsPointPlanState::HOT);
    stmt->set_ps_point_plan_retryable_cold(false);
    ps_point_plan_mark_admission(thd);
    return;
  }
```

---

### Step 7: 修改 fast path 构建（v3 完整版）

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_build_fast_path()` 函数，在 RANGE_PK_BETWEEN 处理后添加（约第779行后）：

```cpp
  /* RANGE_PK_BETWEEN_AGG fast path (v3: 完整聚合初始化) */
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_AGG) {
    /*
      === v3 P0 修复: 手动初始化聚合函数 ===

      Fast path 在 JOIN::optimize() 的最开头执行，此时 join->sum_funcs
      尚未被 alloc_func_list() 初始化。对于聚合查询，必须手动调用
      完整的聚合初始化流程，否则 AggregateIterator 会解引用 nullptr 崩溃。

      性能影响: 这些函数开销约 200-400ns，远小于被跳过的
      test_quick_select() (~1500-3000ns)，净收益仍然显著。
    */
    if (count_field_types(join->query_block, &join->tmp_table_param,
                          *join->fields, false, false)) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
    if (join->alloc_func_list()) {  // 初始化 join->sum_funcs
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
    if (setup_sum_funcs(join->thd, join->sum_funcs)) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    /*
      === v3 P0 修复: 设置隐式分组标志 ===

      AggregateIterator 依赖这些字段判断空集返回行为。
      对于 SELECT SUM() FROM ... WHERE，implicit_grouping 应为 true。
    */
    join->implicit_grouping = true;
    join->grouped = false;
    join->group_optimized_away = false;

    // === 构建 range scan ===
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
      // 复用 arena 缓存的组件
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
      // 非缓存路径（备用）
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

    // 验证 low <= high
    if (key_cmp2(&keyinfo->key_part[0], min_key, key_bytes, max_key,
                 key_bytes) > 0) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    // 构建 QUICK_RANGE
    QUICK_RANGE *range = new (thd->mem_root)
        QUICK_RANGE(thd->mem_root, min_key, key_bytes, keypart_map, max_key,
                    key_bytes, keypart_map, tpl.range_flag,
                    tpl.range_rkey_func_flag);

    // 构建 INDEX_RANGE_SCAN AccessPath
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
    range_path->index_range_scan().can_be_used_for_ror = tpl.range_can_be_used_for_ror;
    range_path->index_range_scan().need_rows_in_rowid_order =
        tpl.range_need_rows_in_rowid_order;
    range_path->index_range_scan().can_be_used_for_imerge =
        tpl.range_can_be_used_for_imerge;
    range_path->index_range_scan().reuse_handler = tpl.range_reuse_handler;
    range_path->index_range_scan().geometry = tpl.range_geometry;
    range_path->index_range_scan().reverse = tpl.range_reverse;
    range_path->index_range_scan().using_extended_key_parts =
        tpl.range_using_extended_key_parts;

    // 构建 AGGREGATE AccessPath
    AccessPath *agg_path = NewAggregateAccessPath(thd, range_path,
                                                  /*rollup=*/false);
    if (agg_path == nullptr) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    // 设置 QEP_TAB
    QEP_TAB *tab = &new_qep_tab[0];
    tab->set_qs(qs);
    tab->set_join(join);
    tab->set_idx(0);
    tab->set_table(table);
    tab->table_ref = tpl.table_ref;
    tab->set_type(JT_RANGE);
    tab->set_condition(nullptr);
    tab->set_range_scan(range_path);

    // 设置 JOIN 状态
    join->tables = 1;
    join->primary_tables = 1;
    join->const_tables = 0;
    join->best_read = tpl.best_read;
    join->best_rowcount = static_cast<ha_rows>(tpl.best_rowcount);
    join->where_cond = nullptr;
    join->having_cond = nullptr;
    join->qep_tab = new_qep_tab;
    join->set_root_access_path(agg_path);

    // 注意: join->sum_funcs 已在上文手动初始化

    ps_point_plan_mark_hit(thd);
    return true;
  }
```

---

### Step 8: 扩展 runtime guard

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_runtime_guard()` 函数，在参数guards之后添加（约第578行后）：

```cpp
  /* G12: Phase 8 - 聚合字段类型 guard */
  if (tpl.has_aggregate && tpl.aggregate_field_index != MAX_KEY) {
    // 验证聚合字段类型未变化
    if (tpl.aggregate_field_index >= table->s->fields) {
      stmt->invalidate_ps_point_plan_cache();
      ps_point_plan_mark_invalidation(thd);
      return false;
    }

    Field *agg_field = table->field[tpl.aggregate_field_index];
    if (agg_field == nullptr ||
        agg_field->data_type() != tpl.aggregate_field_type ||
        agg_field->is_unsigned() != tpl.aggregate_field_unsigned) {
      stmt->invalidate_ps_point_plan_cache();
      ps_point_plan_mark_invalidation(thd);
      return false;
    }
  }

  *table_out = table;
  *keyinfo_out = keyinfo;
  return true;
}
```

---

### Step 9: 扩展清理函数

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_clear_hot_metadata()` 函数，添加聚合字段清理（约第125行后）：

```cpp
  // ... 现有清理 ...

  /*
    Phase 8: 清理聚合相关元数据
  */
  tpl->has_aggregate = false;
  tpl->aggregate_type = Item_sum::Sumfunctype::SUM_FUNC;
  tpl->aggregate_field_index = MAX_KEY;
  tpl->aggregate_field_type = MYSQL_TYPE_INVALID;
  tpl->aggregate_field_unsigned = false;

  // ... 现有清理 ...
}
```

---

### Step 10: 更新 fast-path 注释

**文件**: `sql/sql_optimizer.cc`

修改 fast-path hook 的注释（约第355-362行）：

```cpp
/*
    V1.3: Early fast path for HOT prepared statements.

    Fire BEFORE the entire optimizer preamble (Opt_trace, count_field_types,
    alloc_func_list, get_optimizable_conditions, optimize_cond, etc.) so
    that a HOT point-select skips all of it.

    Phase 8 扩展: 支持 RANGE_PK_BETWEEN_AGG 类型（单聚合函数查询）。
    对于此类查询，fast path 手动调用 count_field_types + alloc_func_list +
    make_sum_func_list + prepare_sum_aggregators + setup_sum_funcs 初始化
    join->sum_funcs，因为 AggregateIterator 需要这些数据。

    分类 gates 保证 HOT 语句是单表查询，无子查询、derived table、
    windows、HAVING 或显式 GROUP BY。
*/
```

---

## 测试用例

### 1. 功能测试

```sql
CREATE TABLE sbtest (
  id INT PRIMARY KEY,
  k INT DEFAULT '0' NOT NULL,
  c CHAR(120) DEFAULT '' NOT NULL,
  pad CHAR(60) DEFAULT '' NOT NULL
) ENGINE=InnoDB;
INSERT INTO sbtest VALUES (1, 100, 'test', 'pad'), (2, 200, 'test2', 'pad2');

SET SESSION ps_point_plan_cache = ON;

-- SUM 测试
PREPARE stmt_sum FROM 'SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?';
SET @low = 1, @high = 100;
EXECUTE stmt_sum USING @low, @high;  -- 首次执行，应 admission
SHOW STATUS LIKE 'Ps_point_plan_cache%';  -- admissions = 1
EXECUTE stmt_sum USING @low, @high;  -- 第二次，应命中
SHOW STATUS LIKE 'Ps_point_plan_cache%';  -- hits = 1

-- COUNT(*) 测试
PREPARE stmt_count FROM 'SELECT COUNT(*) FROM sbtest WHERE id BETWEEN ? AND ?';
EXECUTE stmt_count USING 1, 100;
EXECUTE stmt_count USING 1, 100;  -- 应命中

-- MIN/MAX 测试
PREPARE stmt_min FROM 'SELECT MIN(k) FROM sbtest WHERE id BETWEEN ? AND ?';
PREPARE stmt_max FROM 'SELECT MAX(k) FROM sbtest WHERE id BETWEEN ? AND ?';
EXECUTE stmt_min USING 1, 100;
EXECUTE stmt_max USING 1, 100;
```

### 2. 边界测试

```sql
-- NULL 参数
EXECUTE stmt_sum USING NULL, 100;  -- 应 fallback

-- 空范围
EXECUTE stmt_sum USING 9999, 10000;  -- 应返回 NULL（空集聚合）

-- 反向范围
EXECUTE stmt_sum USING 100, 1;  -- 应 fallback
```

### 3. 负向测试

```sql
-- 多个聚合函数
PREPARE stmt_multi FROM 'SELECT SUM(k), COUNT(*) FROM sbtest WHERE id BETWEEN ? AND ?';
EXECUTE stmt_multi USING 1, 100;  -- 应 NEVER

-- GROUP BY
PREPARE stmt_group FROM 'SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ? GROUP BY c';
EXECUTE stmt_group USING 1, 100;  -- 应 NEVER

-- ORDER BY
PREPARE stmt_order FROM 'SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY SUM(k)';
EXECUTE stmt_order USING 1, 100;  -- 应 NEVER

-- AVG
PREPARE stmt_avg FROM 'SELECT AVG(k) FROM sbtest WHERE id BETWEEN ? AND ?';
EXECUTE stmt_avg USING 1, 100;  -- 应 NEVER

-- SELECT list 含非法项
PREPARE stmt_invalid FROM 'SELECT SUM(k), c FROM sbtest WHERE id BETWEEN ? AND ?';
EXECUTE stmt_invalid USING 1, 100;  -- 应 NEVER
```

### 4. 空集行为验证

```sql
-- 空集应返回 NULL（非 0）
DELETE FROM sbtest;
EXECUTE stmt_sum USING 1, 100;  -- 应返回 NULL
EXECUTE stmt_count USING 1, 100;  -- 应返回 0
```

---

## 验收标准

1. **功能正确性**: 所有聚合函数结果与优化器路径完全一致（包括空集行为）
2. **性能提升**: sum_ranges 查询 QPS 提升 > 30%
3. **无性能回退**: 非候选语句 QPS 变化 < 1%
4. **内存安全**: Valgrind 检测无内存泄漏
5. **代码质量**: 遵循现有代码风格，编译无警告
6. **稳定性**: 压力测试无崩溃

---

## 风险与缓解（v3）

| 风险 | 缓解措施 | 状态 |
|------|----------|------|
| ~~Item_sum 克隆不完整~~ | 不缓存 Item_sum，fast path 重新初始化 | ✅ 已解决 |
| ~~sum_funcs 为 nullptr~~ | fast path 入口手动调用初始化函数 | ✅ 已解决 |
| ~~implicit_grouping 未设置~~ | fast path 显式设置 | ✅ 已解决 |
| Admission 检查不足 | 复用 RANGE_PK_BETWEEN 完整检查 | ✅ 已解决 |
| 聚合语义不一致 | 完全复用现有 `AggregateIterator` | ✅ 无风险 |
| 空集处理差异 | `AggregateIterator` 的 NULL 处理自然兼容 | ✅ 无风险 |
| 内存泄漏 | Range 组件分配在 PS m_arena 上 | ✅ 无风险 |
| 性能回退 | classify 阶段 early reject | ✅ 无风险 |

---

## 实现检查清单

- [ ] Step 1: 扩展 `PsPointPlanTemplate`
- [ ] Step 2: 添加 `Item_sum` 前向声明
- [ ] Step 3: 扩展 `PsCachedPlanType` 枚举
- [ ] Step 4: 实现 `ps_point_plan_validate_simple_aggregate()`（v3: continue 而非 return）
- [ ] Step 5: 修改 `ps_point_plan_classify()` Gate 4
- [ ] Step 6: 修改 `ps_point_plan_extract_where_shape()` Shape C
- [ ] Step 7: 修改 `ps_point_plan_can_admit()`（v3: 补齐 12 项检查）
- [ ] Step 8: 修改 `ps_point_plan_admit()`
- [ ] Step 9: 修改 `ps_point_plan_build_fast_path()`（v3: 手动初始化 sum_funcs）
- [ ] Step 10: 扩展 `ps_point_plan_runtime_guard()`
- [ ] Step 11: 扩展 `ps_point_plan_clear_hot_metadata()`
- [ ] Step 12: 更新 `sql_optimizer.cc` 注释
- [ ] Step 13: 编写并运行测试用例
- [ ] Step 14: 性能验证
- [ ] Step 15: Valgrind 内存检测

---

## v3 vs v2 差异速查

| 组件 | v2 | v3 |
|------|----|----|
| `validate_simple_aggregate()` | COUNT(*) 使用 `return true` | 改为 `continue` |
| `validate_simple_aggregate()` | `arg_count == 0 \|\| ...` | 移除死代码分支 |
| `can_admit()` | 12 项检查缺失 | 完整复用 RANGE_PK_BETWEEN 检查 |
| `build_fast_path()` | 假设 sum_funcs 已初始化 | 手动调用 5 个初始化函数 |
| `build_fast_path()` | 未设置 implicit_grouping | 显式设置 `implicit_grouping = true` |
| `sql_optimizer.cc` 注释 | "no aggregates" | 说明 AGG 类型特殊处理 |

---

## Design Review (v3)

> **审查时间**: 2026-04-12
> **审查方法**: 逐步对照 v3 设计代码与 MySQL 源码（`sql_optimizer.cc`、`sql_select.h`、`sql_lex.h`、`composite_iterators.cc`、`access_path.h`、`item.h`、`sql_optimizer.h`），验证函数签名、变量作用域、初始化顺序。

### 总评

v3 正确修复了 v2 的核心问题（`sum_funcs` nullptr、`implicit_grouping` 未设置、admission guards 缺失），但引入了 **2 个新的 P0 编译/逻辑错误**，需要在实现前修正。

---

### P0 [阻塞] 1: `count_field_types()` 返回 `void`，不能作为 `if` 条件

**位置**: Step 7 (fast path)，约文档第 513-517 行

**问题**: v3 代码：
```cpp
if (count_field_types(join->query_block, &join->tmp_table_param,
                      *join->fields, false, false)) {
  ps_point_plan_mark_runtime_fallback(thd);
  return false;
}
```

但 `count_field_types()` 的签名是：
```cpp
// sql/sql_select.h:776, sql/sql_select.cc:3901
void count_field_types(const Query_block *query_block, Temp_table_param *param,
                       const mem_root_deque<Item *> &fields,
                       bool reset_with_sum_func, bool save_sum_fields);
```

返回类型为 `void`，不能用于布尔表达式。**此代码无法通过编译。**

**修复方案**: 改为无条件调用：
```cpp
count_field_types(join->query_block, &join->tmp_table_param,
                  *join->fields, false, false);
```

该函数内部不会失败（只做计数赋值），无需错误检查。

---

### P0 [阻塞] 2: Gate 4b 使用了尚未声明的 `tpl` 变量

**位置**: Step 3 (classify)，文档说"修改约第302-305行"

**问题**: 当前 `ps_point_plan_classify()` 的代码结构为：

```
Line 302-305:  Gate 4 (原有) ← v3 在此处插入 Gate 4b
Line 307:      Table_ref *tbl = qb->leaf_tables;
Line 308-312:  tbl 验证
Line 315:      PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();  ← tpl 在此声明
Line 316:      tpl = PsPointPlanTemplate{};
Line 317:      tpl.table_ref = tbl;
Line 319:      ps_point_plan_extract_where_shape(qb, &tpl)
```

v3 的 Gate 4b 代码在第 302 行位置使用了 `&tpl` 和 `tpl.has_aggregate`：
```cpp
if (qb->agg_func_used()) {
  if (!ps_point_plan_validate_simple_aggregate(qb, &tpl))  // tpl 尚未声明!
    return false;
  tpl.has_aggregate = true;
}
```

这导致两个连锁问题：

1. **编译错误**: `tpl` 在第 315 行才声明，第 302 行不可见
2. **即使重排使 `tpl` 在作用域内**: 第 316 行 `tpl = PsPointPlanTemplate{}` 会**清零所有聚合元数据**（`has_aggregate`、`aggregate_type`、`aggregate_field_index` 等），使 Gate 4b 的赋值完全丢失
3. **`validate_simple_aggregate()` 中访问 `tpl->table_ref`** (文档第 193 行): 在 Gate 4 位置调用时 `tpl->table_ref` 尚未赋值，导致字段归属检查 `field->table_ref != tpl->table_ref` 始终失败（与 nullptr 比较），**所有聚合查询都会被拒绝**

**修复方案**: 将 Gate 4b 移至 `tpl.table_ref = tbl;`（第 317 行）之后、`extract_where_shape()`（第 319 行）之前：

```cpp
  // Line 302-305 (修改后): 去掉 is_grouped()，保留其他检查
  if (qb->is_distinct() || qb->is_ordered() ||
      qb->has_limit() || qb->has_windows() || qb->has_ft_funcs())
    return false;
  if (qb->group_list.elements > 0) return false;  // 拒绝显式 GROUP BY

  Table_ref *tbl = qb->leaf_tables;
  if (tbl == nullptr || !tbl->is_base_table()) return false;
  if (tbl->table == nullptr) return false;
  if (tbl->table->part_info != nullptr) return false;

  PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();
  tpl = PsPointPlanTemplate{};
  tpl.table_ref = tbl;

  // ← Gate 4b 应在这里，tpl 已声明且 table_ref 已设置
  if (qb->agg_func_used()) {
    if (!ps_point_plan_validate_simple_aggregate(qb, &tpl)) {
      stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);  // 见 P2-1
      return false;
    }
    tpl.has_aggregate = true;
  }

  if (!ps_point_plan_extract_where_shape(qb, &tpl)) { ... }
```

---

### P2 [建议] 1: Gate 4b 验证失败时应设置 `NEVER` 状态

**位置**: Step 3 (classify)

**问题**: 当 `ps_point_plan_validate_simple_aggregate()` 返回 false（如多聚合函数、AVG、DISTINCT 聚合等），v3 直接 `return false` 但未设置 `PsPointPlanState::NEVER`。这导致该 prepared statement 每次 EXECUTE 都重新进入 classify 逻辑，浪费 CPU。

对于确定不支持的查询模式（如 `SELECT AVG(k) FROM ...`），应与其他 Gate 一致，设置 NEVER：

```cpp
if (!ps_point_plan_validate_simple_aggregate(qb, &tpl)) {
  stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);
  return false;
}
```

---

### P2 [建议] 2: Fast path 中 `implicit_grouping` / `grouped` / `group_optimized_away` 赋值冗余

**位置**: Step 7 (fast path)，文档第 541-543 行

**问题**: v3 代码：
```cpp
join->implicit_grouping = true;
join->grouped = false;
join->group_optimized_away = false;
```

但 JOIN 构造函数已经正确初始化了这些值：

```cpp
// sql/sql_optimizer.cc:166-195 JOIN::JOIN() 初始化列表
grouped(select->is_explicitly_grouped()),          // false (无 GROUP BY)
implicit_grouping(select->is_implicitly_grouped()), // true (有聚合无 GROUP BY)
```
```cpp
// sql/sql_optimizer.h:378
bool group_optimized_away{false};  // 默认成员初始化
```

对于 `SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?`：
- `is_explicitly_grouped()` = false → `grouped = false` ✓
- `is_implicitly_grouped()` = true → `implicit_grouping = true` ✓
- `group_optimized_away` 默认为 false ✓

**建议**: 保留代码但改为防御性注释，明确表明这些赋值是冗余的安全保障：

```cpp
/*
  Defensive: these values should already be correct from the JOIN
  constructor (grouped=false, implicit_grouping=true,
  group_optimized_away=false), but set explicitly to guard against
  future constructor changes.
*/
assert(join->implicit_grouping == true);
assert(join->grouped == false);
assert(join->group_optimized_away == false);
```

---

### P2 [建议] 3: `setup_sum_funcs()` 参数使用 `join->thd` 而非局部 `thd`

**位置**: Step 7 (fast path)，文档第 530 行

**问题**:
```cpp
if (setup_sum_funcs(join->thd, join->sum_funcs)) {
```

`ps_point_plan_build_fast_path()` 函数签名中已有 `THD *thd` 参数，且其余调用（如 `ps_point_plan_mark_runtime_fallback(thd)`）均使用局部 `thd`。虽然 `join->thd` 与局部 `thd` 是同一对象，但为一致性应统一使用局部 `thd`：

```cpp
if (setup_sum_funcs(thd, join->sum_funcs)) {
```

---

### P3 [小建议] AGGREGATE AccessPath 未设置 cost 信息

**位置**: Step 7 (fast path)，文档第 670 行

**问题**: `NewAggregateAccessPath()` 创建的 `agg_path` 的 cost / num_output_rows 保持默认值 0。虽然不影响执行正确性，但会导致 EXPLAIN 输出中聚合节点的代价显示为 0。

**建议**: 设置基本代价估算：
```cpp
agg_path->set_num_output_rows(1.0);  // 隐式分组总是返回 1 行
agg_path->cost = range_path->cost;   // 至少等于子路径代价
```

---

### 已修复问题确认

| v2 问题 | v3 状态 | 验证结论 |
|---------|---------|---------|
| P0: `sum_funcs` nullptr | ✅ 已修复 | 手动调用 5 个初始化函数（但 `count_field_types` 调用方式需修正，见 P0-1） |
| P0: `implicit_grouping` 未设置 | ✅ 已修复 | 显式设置（实际上构造函数已设置，见 P2-2） |
| P1: 12 项 admission guards 缺失 | ✅ 已修复 | 完整复用 RANGE_PK_BETWEEN 检查 |
| P2: COUNT(*) early return | ✅ 已修复 | 改为 `continue` |
| P2: `arg_count == 0` 死代码 | ✅ 已修复 | 已移除 |

---

### AggregateIterator 兼容性验证

对 v3 fast path 构建的执行环境，逐项验证 `AggregateIterator` 依赖的 JOIN 成员：

| JOIN 成员 | AggregateIterator 用途 | fast path 值来源 | 状态 |
|-----------|----------------------|-----------------|------|
| `sum_funcs` | `Read()` 中遍历执行聚合 | `alloc_func_list` + `make_sum_func_list` | ✅ |
| `grouped` | 空集行为判断 (line 248) | 构造函数 `false` | ✅ |
| `group_optimized_away` | 空集行为判断 (line 248) | 默认初始化 `false` | ✅ |
| `implicit_grouping` | `Init()` slice 判断 (line 223) | 构造函数 `true` | ✅ |
| `send_group_parts` | `SetRollupLevel` (line 294) | 默认初始化 `{0}` | ✅ |
| `group_fields` | 分组边界检测 (line 285, 352) | 构造函数空 `List<>` | ✅ |
| `fields` / `get_current_fields()` | 空集 `no_rows_in_result` (line 256) | 构造函数 `&select->fields` | ✅ |
| `tables_list` | `clear_fields()` (line 270) | 由 `sql_optimizer.cc:377` 设置 | ✅ |
| `tmp_table_param.precomputed_group_by` | `Init()` assert (line 192) | 默认 `false` | ✅ |
| `current_ref_item_slice` | `get_current_fields()` | 构造函数 `REF_SLICE_SAVED_BASE` | ✅ |

---

### v3 修正要点速查

| # | 严重度 | 问题 | 修复方案 | 工作量 |
|---|--------|------|----------|--------|
| 1 | **P0** | `count_field_types()` 返回 void | 去掉 `if` 包装，直接调用 | 1 行 |
| 2 | **P0** | Gate 4b 在 `tpl` 声明前使用 | 移到 `tpl.table_ref = tbl` 之后 | 代码块重排 |
| 3 | P2 | Gate 4b 失败未设 NEVER | 添加 `set_ps_point_plan_state(NEVER)` | 1 行 |
| 4 | P2 | implicit_grouping 等赋值冗余 | 改为 assert 或添加防御性注释 | 3 行 |
| 5 | P2 | `join->thd` vs 局部 `thd` | 统一使用局部 `thd` | 1 行 |
| 6 | P3 | agg_path cost 为 0 | 设置基本代价估算 | 2 行 |

**结论**: v3 的整体架构正确，核心问题（sum_funcs 初始化、admission guards）已妥善解决。修正上述 2 个 P0 后即可进入实现阶段。
