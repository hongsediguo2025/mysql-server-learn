# Plan Cache Phase 8: sum_ranges Implementation (Revised v2)

> **版本说明**: 本文档是基于 Design Review 反馈的修正版本，解决了原设计中的1个P0阻塞性bug和多个P1重要问题。

## Overview

本文档是 `plan_cache_phase7_10_read_only_coverage.md` Phase 8 的详细实现方案（已修正）。
目标：支持 `SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?` 查询模式的计划缓存。

**预期收益**：sum_ranges 约占 sysbench oltp_read_only 优化器资源消耗的 15-20%，缓存后可提升总体 QPS 5-10%。

---

## Design Review 修正总结

| 问题 | 严重程度 | 修正方案 |
|------|----------|----------|
| `clone_item()` 返回 nullptr | P0 阻塞 | 不缓存 Item_sum，复用 JOIN 的 sum_funcs |
| `qb->sum_func_count` 不存在 | P1 重要 | 改用 `qb->agg_func_used()` + 手动计数 |
| Gate 4 `is_grouped()` 语义 | P1 重要 | 分离显式 GROUP BY 和隐式聚合 |
| AGGREGATE child 指针比较过严 | P1 重要 | 改为验证 type + index，不要求指针相等 |
| COUNT(*) arg_count 检查顺序 | P2 建议 | 将特殊处理移到 arg_count 检查之前 |
| 非缓存路径 `max_buf` typo | P2 建议 | 修正为 `max_key` |
| 代码重复 | P2 建议 | 可选：提取公共函数（本文档保持与 Phase 7 一致的复制粘贴风格）|

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
| `sql/ps_point_plan_cache.cc` | 修改 | +180 |
| **总计** | | **~205 行** |

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

    注意：不缓存 Item_sum 对象，复用 JOIN 正常初始化的 sum_funcs
    （通过 alloc_func_list() + make_sum_func_list()）
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

### Step 2: 实现聚合验证函数（修正版）

**文件**: `sql/ps_point_plan_cache.cc`

在 `ps_point_plan_extract_where_shape()` 函数之前（约第254行）添加：

```cpp
/**
  Validate simple aggregate for RANGE_PK_BETWEEN_AGG.

  只允许无GROUP BY的单聚合函数 (SUM/COUNT/MIN/MAX)。
  注意：Query_block 没有 sum_func_count 字段，需手动遍历计数。

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

      // COUNT(*) 特殊处理（arg_count 可能为 0）
      if (sum_item->sum_func() == Item_sum::COUNT_FUNC) {
        if (sum_item->arg_count == 0 ||
            (sum_item->arg_count == 1 &&
             sum_item->args()[0]->type() == Item::INT_ITEM)) {
          // COUNT(*) 允许，不需要记录字段信息
          tpl->aggregate_type = sum_item->sum_func();
          tpl->aggregate_field_index = MAX_KEY;
          tpl->aggregate_field_type = MYSQL_TYPE_LONGLONG;
          tpl->aggregate_field_unsigned = true;
          return true;
        }
        // COUNT(field) 会继续下面的处理
      }

      // 只允许 SUM/COUNT/MIN/MAX（非DISTINCT变体）
      // DISTINCT变体 (SUM_DISTINCT_FUNC等) 已被 has_with_distinct() 拦截
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

### Step 3: 修改 classify 函数（修正版）

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

### Step 4: 修改 WHERE shape 提取（修正版）

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

### Step 5: 修改 admission 检查（修正版）

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_can_admit()` 函数，在 RANGE_PK_BETWEEN 检查后添加（约第1043行后）：

```cpp
  /* RANGE_PK_BETWEEN_AGG admission checks */
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_AGG) {
    // 基础检查
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

    // 验证聚合路径结构（修正：放松child指针比较）
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

### Step 6: 修改 admission 保存元数据（修正版）

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
      注意：不缓存 Item_sum，复用 JOIN 正常初始化的 sum_funcs
      （通过 JOIN::optimize() 的 alloc_func_list() + make_sum_func_list()）
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

      // 注意：不再分配 cached_sum_funcs，复用 JOIN 的 sum_funcs

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

### Step 7: 修改 fast path 构建（修正版）

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_build_fast_path()` 函数，在 RANGE_PK_BETWEEN 处理后添加（约第779行后）：

```cpp
  /* RANGE_PK_BETWEEN_AGG fast path */
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_AGG) {
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

      // 修正：max_buf → max_key
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

    // 注意：不设置 join->sum_funcs，复用 optimizer 已初始化的值
    // join->sum_funcs 已通过 JOIN::optimize() 的 alloc_func_list() + make_sum_func_list() 初始化

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
    注意：不再有 cached_sum_funcs 字段
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
```

### 2. 边界测试

```sql
-- NULL 参数
EXECUTE stmt_sum USING NULL, 100;  -- 应 fallback

-- 空范围
EXECUTE stmt_sum USING 9999, 10000;  -- 应返回 NULL

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
```

---

## 验收标准

1. **功能正确性**: 所有聚合函数结果与优化器路径完全一致
2. **性能提升**: sum_ranges 查询 QPS 提升 > 30%
3. **无性能回退**: 非候选语句 QPS 变化 < 1%
4. **内存安全**: Valgrind 检测无内存泄漏
5. **代码质量**: 遵循现有代码风格，编译无警告

---

## 风险与缓解（修正版）

| 风险 | 缓解措施 |
|------|----------|
| ~~Item_sum 克隆不完整~~ | **不缓存 Item_sum，复用 JOIN 正常初始化的 sum_funcs** |
| 聚合语义不一致 | 完全复用现有 `AggregateIterator` |
| 空集处理差异 | `AggregateIterator` 的 NULL 处理自然兼容 |
| 内存泄漏 | Range 组件分配在 PS m_arena 上；Item_sum 使用 JOIN 正常生命周期 |
| 性能回退 | classify 阶段 early reject |
| ~~sum_func_count 不存在~~ | **改用 `qb->agg_func_used()` + 手动遍历计数** |

---

## 实现检查清单

- [ ] Step 1: 扩展 `PsPointPlanTemplate`（不含 cached_sum_funcs）
- [ ] Step 2: 添加 `Item_sum` 前向声明
- [ ] Step 3: 扩展 `PsCachedPlanType` 枚举
- [ ] Step 4: 实现 `ps_point_plan_validate_simple_aggregate()`（使用 agg_func_used）
- [ ] Step 5: 修改 `ps_point_plan_classify()` Gate 4（分离显式GROUP BY和聚合）
- [ ] Step 6: 修改 `ps_point_plan_extract_where_shape()` Shape C
- [ ] Step 7: 修改 `ps_point_plan_can_admit()`（放松child指针比较）
- [ ] Step 8: 修改 `ps_point_plan_admit()`（移除Item_sum缓存）
- [ ] Step 9: 修改 `ps_point_plan_build_fast_path()`（修正max_buf typo）
- [ ] Step 10: 扩展 `ps_point_plan_runtime_guard()` 添加聚合类型guard
- [ ] Step 11: 扩展 `ps_point_plan_clear_hot_metadata()`（移除cached_sum_funcs清理）
- [ ] Step 12: 编写并运行测试用例
- [ ] Step 13: 性能验证
- [ ] Step 14: Valgrind 内存检测

---
---

# Design Review (v2)

> 基于 `plan_cache_range` 分支代码（`ps_point_plan_cache.cc` ~1470 行、`sql_optimizer.cc` fast-path hook）
> 和 `AggregateIterator` / `sum_funcs` 生命周期的完整追踪，对 v2 方案进行审查。

## Review 总结

v2 正确修复了 v1 的多个问题（`clone_item` 返回 nullptr、`qb->sum_func_count` 不存在、
Gate 4 语义、child 指针比较、`max_buf` typo），但引入了 **1 个新的 P0 阻塞性 bug**
和若干遗漏。核心问题是 v2 对 `join->sum_funcs` 生命周期的假设是错误的。

---

## P0 [阻塞] Fast path 在 `alloc_func_list()` 之前执行，`join->sum_funcs` 为 nullptr

**位置**: Step 7 — fast path 构建，第 617-618 行注释

**v2 设计假设**:

```
// 注意：不设置 join->sum_funcs，复用 optimizer 已初始化的值
// join->sum_funcs 已通过 JOIN::optimize() 的 alloc_func_list() + make_sum_func_list() 初始化
```

**实际代码流（`sql/sql_optimizer.cc`）**:

```
JOIN::optimize() {
  // ---- line 375: FAST PATH FIRES HERE ----
  //   如果 HOT，调用 ps_point_plan_build_fast_path()
  //   成功则 set_optimized() + PLAN_READY + return
  //   ↑↑ 以下所有步骤全部被跳过 ↑↑

  // line 396: count_field_types()      ← 设置 tmp_table_param.sum_func_count
  // line 409: alloc_func_list()        ← 分配 join->sum_funcs 数组
  // ...
  // line 1068: count_field_types()     ← 第二次调用
  // line 1070: create_access_paths()   ← 创建 access path + 迭代器
}
```

fast path hook 位于 `JOIN::optimize()` 的**最开头**（line 367-384），在
`count_field_types()` (line 396) 和 `alloc_func_list()` (line 409) **之前**。
当前代码的注释甚至明确说明了这一点（line 355-362）:

```cpp
/*
    V1.2: Early fast path for HOT prepared statements.

    Fire BEFORE the entire optimizer preamble (Opt_trace, count_field_types,
    alloc_func_list, get_optimizable_conditions, optimize_cond, etc.) so
    that a HOT point-select skips all of it.  The classify gates guarantee
    that any HOT statement is a single-table point SELECT with no aggregates,
    subqueries, derived tables, windows, or LIMIT — none of the preamble
    output is needed.
*/
```

当 fast path 成功返回时:
1. `join->sum_funcs` = **`nullptr`**（从未分配，初始值即为 nullptr）
2. `tmp_table_param.sum_func_count` = **0**（`count_field_types` 未调用）
3. `make_sum_func_list()` 从未调用
4. `prepare_sum_aggregators()` 从未调用
5. `setup_sum_funcs()` 从未调用

**崩溃路径**:

```
fast path 返回 true
  → set_plan_state(PLAN_READY)
  → Query_expression::optimize() 继续
  → create_access_paths(thd)  [line 1089]
    → m_root_access_path = join->root_access_path()  [AGGREGATE → INDEX_RANGE_SCAN]
  → CreateIteratorFromAccessPath()  [line 1137]
    → 遇到 AGGREGATE path → 创建 AggregateIterator(thd, ..., join, ...)
  → AggregateIterator::Read()
    → for (Item_sum **item = m_join->sum_funcs; *item != nullptr; ++item)
       ↑ m_join->sum_funcs == nullptr → 解引用空指针 → SEGFAULT
```

**影响**: Phase 8 的 fast path 每次命中都会导致 MySQL 服务端崩溃。

**建议修复方案**: 在 fast path 的 `RANGE_PK_BETWEEN_AGG` 分支入口手动调用聚合初始化:

```cpp
if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_AGG) {
    // 聚合查询需要 sum_funcs，fast path 在 alloc_func_list 之前执行，
    // 必须手动初始化
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
    if (setup_sum_funcs(join->thd, join->sum_funcs)) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    // ... 然后继续构建 range scan + AGGREGATE path ...
}
```

**性能影响评估**: `count_field_types` + `alloc_func_list` + `make_sum_func_list` +
`prepare_sum_aggregators` + `setup_sum_funcs` 的总开销约 200-400ns，
远小于被跳过的 `test_quick_select()` (~1500-3000ns)，净收益仍然显著。

同时需要更新 `sql_optimizer.cc` line 355-362 的注释，移除 "no aggregates" 的断言。

---

## P1 [重要] Admission 检查缺失大量安全 guard

**位置**: Step 5 — `RANGE_PK_BETWEEN_AGG` admission

v2 的 `RANGE_PK_BETWEEN_AGG` admission 比现有的 `RANGE_PK_BETWEEN` admission
（`ps_point_plan_cache.cc` line 1005-1043）缺失以下检查:

| 缺失的检查 | 现有位置 | 风险 |
|------------|----------|------|
| `get_used_key_parts(range_scan) != 1` | line 1022 | 放过复合 key 部分 |
| `num_ranges != 1` | line 1023 | 放过多范围扫描 |
| `used_key_part == nullptr` | line 1024 | 空指针解引用 |
| `ranges == nullptr` | line 1025 | 空指针解引用 |
| `ranges[0] == nullptr` | line 1026 | 空指针解引用 |
| `used_key_part[0].field == nullptr` | line 1029 | 空指针解引用 |
| `field_index() != tpl.field_indices[0]` | line 1030-1031 | 字段错位 |
| `actual_key_flags(keyinfo) & HA_NOSAME` | line 1034 | 放过非唯一索引 |
| `user_defined_key_parts != 1` | line 1035 | 放过复合 PK |
| `key_part[0].fieldnr - 1 != tpl.field_indices[0]` | line 1036 | 字段错位 |
| `key_part[0].null_bit != 0` | line 1037 | Nullable PK |
| `tpl.params[i] == nullptr` | line 1039-1041 | 空指针参数 |

其中空指针检查（`used_key_part == nullptr`、`ranges == nullptr`、
`ranges[0] == nullptr`、`field == nullptr`）的缺失可能导致后续
`ps_point_plan_admit()` 中解引用空指针崩溃。

**建议**: 直接复制 `RANGE_PK_BETWEEN` 的全部检查代码作为基础，
然后在其后追加 AGGREGATE 特有的检查。或提取公共函数。

---

## P2 [建议] validate 函数 COUNT(*) early return 跳过 SELECT list 完整性检查

**位置**: Step 2 — `ps_point_plan_validate_simple_aggregate()`

```cpp
if (sum_item->sum_func() == Item_sum::COUNT_FUNC) {
    if (sum_item->arg_count == 0 ||
        (sum_item->arg_count == 1 &&
         sum_item->args()[0]->type() == Item::INT_ITEM)) {
      // COUNT(*) 允许
      tpl->aggregate_type = sum_item->sum_func();
      ...
      return true;  // ← 提前返回
    }
}
```

**问题**: 当 COUNT(*) 被识别后直接 `return true`，跳过了对 SELECT list 其余项的检查。
如果查询是 `SELECT expr, COUNT(*) FROM sbtest WHERE id BETWEEN ? AND ?`，
遍历到 COUNT(*) 时立即返回 true，而 `expr` 未被验证。

虽然 `sum_count` 在遇到 COUNT(*) 时仍然是 1（因为只遍历到这里），
但 `sum_count == 1` 的最终检查 (`return sum_count == 1`) 被跳过了。
更重要的是，**如果 SELECT list 顺序是 COUNT(*) 排在非法 item 前面**，
非法 item 不会被检查到。

**建议**: 将 COUNT(*) 的元数据记录改为 `continue`（而非 `return true`），
让循环继续检查 SELECT list 的其余项:

```cpp
if (...) {
  tpl->aggregate_type = sum_item->sum_func();
  tpl->aggregate_field_index = MAX_KEY;
  tpl->aggregate_field_type = MYSQL_TYPE_LONGLONG;
  tpl->aggregate_field_unsigned = true;
  continue;  // 继续检查其余 SELECT list 项
}
```

然后在循环结束后通过 `return sum_count == 1` 统一返回。

---

## P2 [建议] `arg_count == 0` 的 COUNT(*) 分支是死代码

**位置**: Step 2 — COUNT(*) 特殊处理

```cpp
if (sum_item->arg_count == 0 ||
    (sum_item->arg_count == 1 &&
     sum_item->args()[0]->type() == Item::INT_ITEM)) {
```

在 MySQL 8.0 中，`COUNT(*)` 的 AST 表示是 `Item_sum_count(Item_int *number)`
（`item_sum.h` line 1074），其 `arg_count` 始终为 **1**（继承自 `Item_sum(Item *a)` 
构造函数），`args()[0]` 是一个 `Item_int`。

因此 `arg_count == 0` 分支永远不会被执行。虽然无害，但建议移除以避免误导:

```cpp
if (sum_item->arg_count == 1 &&
    sum_item->args()[0]->type() == Item::INT_ITEM) {
```

---

## P3 [小建议] `sql_optimizer.cc` fast-path 注释需更新

`sql_optimizer.cc` line 355-362 的注释明确说:

> "The classify gates guarantee that any HOT statement is a single-table
>  point SELECT with **no aggregates**, subqueries, derived tables, windows,
>  or LIMIT — **none of the preamble output is needed**."

Phase 8 引入了 `RANGE_PK_BETWEEN_AGG` 类型，打破了 "no aggregates" 的假设。
如果采用 P0 建议的修复方案（在 AGG fast path 中手动调用 preamble 函数），
需要更新该注释说明 AGG 类型的特殊处理。

---

## P3 [小建议] `JOIN::implicit_grouping` 未在 fast path 中设置

`AggregateIterator::Init()` 在 line 223 检查:

```cpp
if (!(m_join->implicit_grouping || m_join->group_optimized_away) &&
    !thd()->lex->using_hypergraph_optimizer()) {
  m_output_slice = m_join->get_ref_item_slice();
}
```

`AggregateIterator::Read()` 在 line 248 检查:

```cpp
if (m_join->grouped || m_join->group_optimized_away) {
  // ...
  return -1;
} else {
  // 隐式分组路径：即使无输入行也输出一行
}
```

对于 `SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?`，
`implicit_grouping` 应为 `true`，`grouped` 应为 `false`。
这些值在正常 `JOIN::optimize()` 流程中被设置。如果 fast path 没有正确设置这些字段，
`AggregateIterator` 的行为会出错（特别是空集返回行为）。

**建议**: 在 fast path 中显式设置:

```cpp
join->implicit_grouping = true;
join->grouped = false;
join->group_optimized_away = false;
```

---

## 修正后的实现要点总结

| 步骤 | v2 状态 | 需要的修正 |
|------|---------|-----------|
| Step 1: 数据结构 | 正确 | 无需修改 |
| Step 2: validate | 基本正确 | COUNT(*) early return 改为 continue；移除 `arg_count==0` 死代码 |
| Step 3: classify Gate 4 | 正确 | 无需修改 |
| Step 4: Shape C | 正确 | 无需修改 |
| Step 5: admission | **缺失 12 项检查** | 补齐所有 RANGE_PK_BETWEEN 已有的 guard |
| Step 6: admit | 正确 | 无需修改（arena 缓存仅包含 range 部分） |
| Step 7: fast path | **P0 — sum_funcs 为 nullptr** | 入口处手动调用聚合初始化（count_field_types / alloc_func_list / make_sum_func_list / prepare_sum_aggregators / setup_sum_funcs）；设置 implicit_grouping |
| Step 8: runtime guard | 正确 | 无需修改 |
| Step 9: clear metadata | 正确 | 无需修改 |

---

## 建议优先级

1. **[P0 必须]** 在 AGG fast path 入口手动调用 `count_field_types` + `alloc_func_list` + `make_sum_func_list` + `prepare_sum_aggregators` + `setup_sum_funcs`
2. **[P0 必须]** 在 AGG fast path 中设置 `join->implicit_grouping = true`
3. **[P1 必须]** 补齐 admission 中缺失的 12 项安全检查
4. **[P2 建议]** 修复 validate 中 COUNT(*) early return 问题
5. **[P3 可选]** 更新 `sql_optimizer.cc` fast-path 注释
6. **[P3 可选]** 移除 `arg_count == 0` 死代码
