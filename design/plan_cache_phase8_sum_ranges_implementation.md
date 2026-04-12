# Plan Cache Phase 8: sum_ranges Implementation

## Overview

本文档是 `plan_cache_phase7_10_read_only_coverage.md` Phase 8 的详细实现方案。
目标：支持 `SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?` 查询模式的计划缓存。

**预期收益**：sum_ranges 约占 sysbench oltp_read_only 优化器资源消耗的 15-20%，缓存后可提升总体 QPS 5-10%。

---

## 目标SQL模式

```sql
-- 支持的模式
SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?
SELECT COUNT(*) FROM sbtest WHERE id BETWEEN ? AND ?
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
| `sql/ps_point_plan_cache.h` | 修改 | +30 |
| `sql/ps_point_plan_cache.cc` | 修改 | +200 |
| **总计** | | **~230 行** |

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
  */

  /// 是否包含聚合函数
  bool has_aggregate{false};

  /// 聚合函数类型 (SUM/COUNT/MIN/MAX)
  Item_sum::Sumfunctype aggregate_type{
      Item_sum::Sumfunctype::SUM_FUNC};

  /// 聚合字段在 TABLE::field[] 中的索引
  uint aggregate_field_index{MAX_KEY};

  /// 聚合字段的数据类型快照（用于运行时类型drift检测）
  enum_field_types aggregate_field_type{MYSQL_TYPE_INVALID};

  /// 聚合字段是否为 unsigned
  bool aggregate_field_unsigned{false};

  /// Arena-cached Item_sum 对象数组（以nullptr结尾）
  /// 分配在 PS m_arena 上，避免每次执行创建
  Item_sum **cached_sum_funcs{nullptr};
};
```

---

### Step 2: 实现聚合验证函数

**文件**: `sql/ps_point_plan_cache.cc`

在 `ps_point_plan_extract_where_shape()` 函数之前（约第254行）添加：

```cpp
/**
  Validate simple aggregate for RANGE_PK_BETWEEN_AGG.

  只允许无GROUP BY的单聚合函数 (SUM/COUNT/MIN/MAX)。

  @param  qb   Query block to validate
  @param  tpl  Template to populate with aggregate metadata
  @retval true  Valid simple aggregate
  @retval false Invalid (多聚合/GROUP BY/DISTINCT/不支持的函数类型)
*/
static bool ps_point_plan_validate_simple_aggregate(
    Query_block *qb, PsPointPlanTemplate *tpl) {

  // 只允许单一聚合函数
  if (qb->sum_func_count != 1) return false;

  // 拒绝显式 GROUP BY（虽然 is_grouped() 对无GROUP BY的聚合也返回true）
  if (qb->group_list.elements > 0) return false;

  for (Item *item : *qb->fields) {
    if (item->type() == Item::SUM_FUNC_ITEM) {
      Item_sum *sum_item = down_cast<Item_sum *>(item);

      // 拒绝 DISTINCT 聚合
      if (sum_item->has_with_distinct()) return false;

      // 只允许 SUM/COUNT/MIN/MAX
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

      // COUNT(*) 特殊处理
      if (sum_item->sum_func() == Item_sum::COUNT_FUNC &&
          arg->type() == Item::INT_ITEM) {
        // COUNT(*) 允许，但不需要记录字段信息
        tpl->aggregate_type = sum_item->sum_func();
        tpl->aggregate_field_index = MAX_KEY;
        tpl->aggregate_field_type = MYSQL_TYPE_LONGLONG;
        tpl->aggregate_field_unsigned = true;
        return true;
      }

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

  return true;
}
```

---

### Step 3: 修改 classify 函数

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_classify()` 函数的 Gate 4（约第302-305行）：

```cpp
  /* Gate 4: 拒绝显式GROUP BY/DISTINCT/ORDER BY/LIMIT等复杂特性 */
  if (qb->group_list.elements > 0 || qb->is_distinct() || qb->is_ordered() ||
      qb->has_limit() || qb->has_windows() || qb->has_ft_funcs())
    return false;

  /*
    Gate 4b: 允许简单聚合（无GROUP BY的单聚合函数）
    如果有聚合，验证是否为支持的简单聚合模式
  */
  if (qb->sum_func_count > 0) {
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
      if (qb->sum_func_count > 0) {
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

### Step 5: 修改 admission 检查

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_can_admit()` 函数，在 RANGE_PK_BETWEEN 检查后添加（约第1043行后）：

```cpp
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN) {
    // ... 现有 RANGE_PK_BETWEEN 检查代码 ...
    return true;
  }

  /* RANGE_PK_BETWEEN_AGG admission checks */
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN_AGG) {
    // 基础range检查（复用RANGE_PK_BETWEEN逻辑）
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

    // 验证聚合路径结构
    AccessPath *root_path = join->root_access_path();
    if (root_path == nullptr || root_path->type != AccessPath::AGGREGATE)
      return false;

    // 验证 AGGREGATE.child 是我们的 RANGE_SCAN
    if (root_path->aggregate().child != range_scan) return false;
    if (root_path->aggregate().rollup) return false;  // 无ROLLUP

    // 验证 sum_funcs 非空且只有一个
    if (join->sum_funcs == nullptr) return false;
    int sum_count = 0;
    for (Item_sum **f = join->sum_funcs; *f != nullptr; f++) {
      sum_count++;
    }
    if (sum_count != 1) return false;

    return true;
  }

  /* POINT_EQ_REF admission checks continue... */
```

---

### Step 6: 修改 admission 保存元数据

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_admit()` 函数，在 RANGE_PK_BETWEEN 处理后添加（约第1265行后）：

```cpp
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN) {
    // ... 现有 RANGE_PK_BETWEEN admission 代码 ...
    return;
  }

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
      构建 arena 缓存组件（range + aggregate）
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

      // 分配 aggregate 组件：Item_sum*数组（以nullptr结尾）
      Item_sum **sum_funcs = nullptr;
      if (cache_ok && join->sum_funcs != nullptr) {
        // 分配2个元素的数组：[Item_sum*, nullptr]
        sum_funcs = thd->mem_root->ArrayAlloc<Item_sum *>(2);
        if (sum_funcs == nullptr) {
          cache_ok = false;
        } else {
          // 深拷贝 Item_sum 对象到 PS arena
          // 使用 copy constructor: Item_sum(THD*, const Item_sum*)
          sum_funcs[0] = down_cast<Item_sum *>(
              join->sum_funcs[0]->clone_item());
          sum_funcs[1] = nullptr;

          if (sum_funcs[0] == nullptr || thd->is_error()) {
            cache_ok = false;
          }
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
        tpl.cached_sum_funcs = sum_funcs;
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

  /* POINT_EQ_REF admission continues... */
```

---

### Step 7: 修改 fast path 构建

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_build_fast_path()` 函数，在 RANGE_PK_BETWEEN 处理后添加（约第779行后）：

```cpp
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN) {
    // ... 现有 RANGE_PK_BETWEEN fast path 代码 ...
    return true;
  }

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
      // 非缓存路径（不应该发生，但保持兼容）
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
                           nullable ? max_buf : nullptr, key_part->length,
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

    // 设置 sum_funcs（使用缓存的 Item_sum 数组）
    if (tpl.range_arena_cached && tpl.cached_sum_funcs != nullptr) {
      join->sum_funcs = tpl.cached_sum_funcs;
    } else {
      // 非缓存路径：不应该发生
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    ps_point_plan_mark_hit(thd);
    return true;
  }

  /* POINT_EQ_REF fast path continues... */
```

---

### Step 8: 扩展 runtime guard

**文件**: `sql/ps_point_plan_cache.cc`

修改 `ps_point_plan_runtime_guard()` 函数，在参数guards之后添加（约第578行后）：

```cpp
  /* G11: sql_mode bits affecting comparison semantics. */
  if (ps_point_plan_relevant_sql_mode(thd) != tpl.relevant_sql_mode) {
    ps_point_plan_demote_to_cold(stmt);
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

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
    Phase 8: 清理聚合相关元数据
    注意：cached_sum_funcs 由 PS arena 统一管理，这里只清空指针
  */
  tpl->has_aggregate = false;
  tpl->aggregate_type = Item_sum::Sumfunctype::SUM_FUNC;
  tpl->aggregate_field_index = MAX_KEY;
  tpl->aggregate_field_type = MYSQL_TYPE_INVALID;
  tpl->aggregate_field_unsigned = false;
  tpl->cached_sum_funcs = nullptr;

  /*
    ref_cached / qep_cached stay true if previously built — the
    arena-allocated buffers survive demotion.  A compatibility check
    in ps_point_plan_admit() validates the cached Field clones match
    the new key layout before reuse; mismatches force a rebuild.
    Helper-layout metadata needed by that check is stored separately
    in cached_key_parts/cached_key_length so we can still clear the
    active HOT plan metadata here.
  */
  for (uint i = 0; i < PS_PC_MAX_PARAMS; i++) {
    tpl->actual_types[i] = MYSQL_TYPE_INVALID;
    tpl->unsigned_actuals[i] = false;
    tpl->actual_collations[i] = nullptr;
  }
}
```

---

## 测试用例

### 1. 功能测试

```sql
-- 基础测试
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
EXECUTE stmt_min USING 1, 100;

PREPARE stmt_max FROM 'SELECT MAX(k) FROM sbtest WHERE id BETWEEN ? AND ?';
EXECUTE stmt_max USING 1, 100;
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

-- DISTINCT
PREPARE stmt_distinct FROM 'SELECT SUM(DISTINCT k) FROM sbtest WHERE id BETWEEN ? AND ?';
EXECUTE stmt_distinct USING 1, 100;  -- 应 NEVER

-- AVG
PREPARE stmt_avg FROM 'SELECT AVG(k) FROM sbtest WHERE id BETWEEN ? AND ?';
EXECUTE stmt_avg USING 1, 100;  -- 应 NEVER
```

### 4. 性能验证

```bash
sysbench oltp_read_only.lua \
  --mysql-host=localhost \
  --mysql-port=3306 \
  --mysql-db=test \
  --tables=1 \
  --table-size=1000000 \
  --threads=8 \
  --time=60 \
  run
```

**预期**: sum_ranges 的 QPS 提升 > 30%，整体 QPS 提升 5-10%

---

## 验收标准

1. **功能正确性**: 所有聚合函数结果与优化器路径完全一致
2. **性能提升**: sum_ranges 查询 QPS 提升 > 30%
3. **无性能回退**: 非候选语句 QPS 变化 < 1%
4. **内存安全**: Valgrind 检测无内存泄漏
5. **代码质量**: 遵循现有代码风格，编译无警告

---

## 风险与缓解

| 风险 | 缓解措施 |
|------|----------|
| Item_sum 克隆不完整 | 使用标准 `clone_item()` 接口 |
| 聚合语义不一致 | 完全复用现有 `AggregateIterator` |
| 空集处理差异 | `AggregateIterator` 的 NULL 处理自然兼容 |
| 内存泄漏 | 所有组件分配在 PS m_arena 上 |
| 性能回退 | classify 阶段 early reject |

---

## 后续工作

本实现为 Phase 8，后续 Phase 需要支持：
- Phase 9: order_ranges (ORDER BY + Filesort)
- Phase 10: distinct_ranges (DISTINCT + 去重)

---
---

# Design Review

> 基于 `plan_cache_range` 分支代码（`ps_point_plan_cache.h` ~458 行、`ps_point_plan_cache.cc` ~1470 行）
> 和 Phase 7 code review（`plan_cache_phase7_simple_ranges_code_review.md`）的审查，
> 对照上述 Phase 8 实现方案逐步骤评估。

## Review 总结

设计方案的整体思路正确：在 Phase 7 `RANGE_PK_BETWEEN` 基础上叠加 `AGGREGATE` 路径支持，
修改范围控制合理（~230行），且复用了大量 Phase 7 基础设施。
但存在 **1 个 P0 阻塞性 bug** 和若干需要修正的设计问题。

---

## P0 [阻塞] `clone_item()` 对 `Item_sum` 返回 nullptr

**位置**: Step 6 — admission 保存元数据，arena 缓存构建

**设计代码**:

```cpp
// 深拷贝 Item_sum 对象到 PS arena
sum_funcs[0] = down_cast<Item_sum *>(
    join->sum_funcs[0]->clone_item());
sum_funcs[1] = nullptr;
```

**问题**: `Item_sum` 及其子类（`Item_sum_sum`、`Item_sum_count`、`Item_sum_min`、`Item_sum_max`）
**均未 override** `clone_item()`。基类 `Item::clone_item()` 的默认实现是：

```cpp
// sql/item.h line ~2233
virtual Item *clone_item() const { return nullptr; }
```

因此 `sum_funcs[0]` **恒为 `nullptr`**。虽然后续 `cache_ok` 检查会捕获空指针
（`if (sum_funcs[0] == nullptr || thd->is_error()) cache_ok = false;`），
但这意味着 **所有 `RANGE_PK_BETWEEN_AGG` 类型的 arena 缓存都会失败**，
fast path（Step 7）中 `tpl.cached_sum_funcs` 始终为 `nullptr`，触发 fallback。

**影响**: Phase 8 的 fast path 永远无法命中。

**建议修复方案**:

不做 `Item_sum` 深拷贝。在 fast path 中直接复用 `join->sum_funcs`。
具体有两个可选路径：

**方案 A（推荐）: 不缓存 Item_sum，复用 JOIN 正常初始化的 sum_funcs**

在 fast path 入口确保 `join->alloc_func_list()` 和 `join->make_sum_func_list()`
已被正确调用。从代码看：

- `alloc_func_list()` 在 `JOIN::optimize()` 第 409 行被调用
- `make_sum_func_list()` 在多个路径被调用（optimizer ~8164、executor ~289-302）

如果 fast path 完全跳过了 `JOIN::optimize()`，需要手动补充这两个调用。
这样可以去掉 `cached_sum_funcs` 字段和整个 clone 逻辑，大幅简化方案。

**方案 B: 手动构造 Item_sum 子类**

在 PS arena 上使用各子类的 copy constructor：

```cpp
switch (tpl.aggregate_type) {
  case Item_sum::SUM_FUNC:
    sum_funcs[0] = new (thd->mem_root)
        Item_sum_sum(thd, down_cast<Item_sum_sum *>(join->sum_funcs[0]));
    break;
  case Item_sum::COUNT_FUNC:
    sum_funcs[0] = new (thd->mem_root)
        Item_sum_count(thd, down_cast<Item_sum_count *>(join->sum_funcs[0]));
    break;
  // MIN_FUNC, MAX_FUNC ...
}
```

此方案复杂度更高，且需要处理 `Item_sum` 内部状态（`result_field`、aggregator 等）
在跨执行复用时的生命周期问题。

---

## P1 [重要] Gate 4 / validate 使用了不存在的 `qb->sum_func_count`

**位置**: Step 2（validate 函数）、Step 3（Gate 4b）、Step 4（Shape C）

**设计代码**:

```cpp
// Step 2 line 123
if (qb->sum_func_count != 1) return false;

// Step 3 line 201
if (qb->sum_func_count > 0) { ... }

// Step 4 line 224
if (qb->sum_func_count > 0) { ... }
```

**问题**: `Query_block`（即 `qb`）上 **没有** `sum_func_count` 字段。
该字段位于 `JOIN::tmp_table_param.sum_func_count`（定义在 `sql/temp_table_param.h` line 132），
但在 classify 阶段（PREPARE 时）`JOIN` 尚未构造。

`Query_block` 上可用的聚合相关 API：

| API | 类型 | 含义 |
|-----|------|------|
| `qb->agg_func_used()` | `bool` | 是否使用了聚合函数 |
| `qb->is_implicitly_grouped()` | `bool` | 有聚合但无 GROUP BY |
| `qb->is_grouped()` | `bool` | `group_list.elements > 0 \|\| m_agg_func_used` |
| 遍历 `qb->fields` | 手动 | 逐个检查 `Item::type() == Item::SUM_FUNC_ITEM` |

**建议修复**:

- Gate 4b（classify 阶段）: 使用 `qb->agg_func_used()` 作为 gate
- validate 函数中: 遍历 `qb->fields` 手动计数聚合函数数量
- Shape C 中: 使用 `qb->agg_func_used()` 代替 `qb->sum_func_count > 0`
- admission 阶段（可用 JOIN）: 可使用 `join->tmp_table_param.sum_func_count` 做二次验证

修正后的 validate 函数签名/逻辑：

```cpp
static bool ps_point_plan_validate_simple_aggregate(
    Query_block *qb, PsPointPlanTemplate *tpl) {

  if (qb->group_list.elements > 0) return false;

  uint sum_count = 0;
  for (Item *item : *qb->fields) {
    if (item->type() == Item::SUM_FUNC_ITEM) {
      sum_count++;
      if (sum_count > 1) return false;  // 多聚合直接拒绝
      // ... 现有 Item_sum 验证逻辑 ...
    } else if (item->type() != Item::INT_ITEM) {
      return false;
    }
  }
  if (sum_count != 1) return false;
  return true;
}
```

---

## P1 [重要] Gate 4 修改的 `is_grouped()` 语义分析

**位置**: Step 3 — Gate 4 修改

**现有代码** (`ps_point_plan_cache.cc` line 302-305):

```cpp
/* Gate 4: no aggregation, sorting, or complex features. */
if (qb->is_grouped() || qb->is_distinct() || qb->is_ordered() ||
    qb->has_limit() || qb->has_windows() || qb->has_ft_funcs())
  return false;
```

**`is_grouped()` 的定义** (`sql/sql_lex.h` line 1286):

```cpp
bool is_grouped() const { return group_list.elements > 0 || m_agg_func_used; }
```

对于 `SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?`，
`m_agg_func_used = true`，所以 `is_grouped() = true`。
设计方案将 `is_grouped()` 替换为 `group_list.elements > 0`，
使隐式分组查询不再被 Gate 4 拒绝，而是流入 Gate 4b 验证。

**这个意图是正确的**，但建议修改为更精确的形式：

```cpp
/* Gate 4: reject complex features */
if (qb->is_distinct() || qb->is_ordered() ||
    qb->has_limit() || qb->has_windows() || qb->has_ft_funcs())
  return false;
if (qb->group_list.elements > 0) return false;  // 显式 GROUP BY

/* Gate 4b: if aggregate present, validate it's simple */
if (qb->agg_func_used()) {
  if (!ps_point_plan_validate_simple_aggregate(qb, &tpl))
    return false;
  tpl.has_aggregate = true;
}
```

这样更清晰地分离了各种拒绝条件。

---

## P1 [重要] Admission 中 AGGREGATE 路径 child 指针比较过于严格

**位置**: Step 5 — admission 检查

**设计代码**:

```cpp
// 验证 AGGREGATE.child 是我们的 RANGE_SCAN
if (root_path->aggregate().child != range_scan) return false;
```

**问题**: 优化器生成的 access path 树结构可能在 `AGGREGATE` 和 `INDEX_RANGE_SCAN`
之间插入其他节点（如 `FILTER`）。例如如果 WHERE 条件没有被完全下推到
range scan 中，可能会有 `AGGREGATE -> FILTER -> INDEX_RANGE_SCAN` 的结构。

**建议**: 放松为只验证结构特征而非严格比较 child 指针：

```cpp
AccessPath *root_path = join->root_access_path();
if (root_path == nullptr || root_path->type != AccessPath::AGGREGATE)
  return false;
if (root_path->aggregate().rollup) return false;

// 验证 child 是 INDEX_RANGE_SCAN on primary key（不要求指针相等）
AccessPath *child = root_path->aggregate().child;
if (child == nullptr || child->type != AccessPath::INDEX_RANGE_SCAN)
  return false;
if (used_index(child) != table->s->primary_key) return false;
```

注意：`aggregate()` 访问器内部 **assert** `path->type == AGGREGATE`
（`access_path.h` line 727-728），在非 AGGREGATE 节点上调用会导致 debug build crash，
所以前面的 type 检查是必须的。

---

## P1 [重要] Fast path 中 `join->sum_funcs` 生命周期问题

**位置**: Step 7 — fast path 构建

**设计代码**:

```cpp
if (tpl.range_arena_cached && tpl.cached_sum_funcs != nullptr) {
  join->sum_funcs = tpl.cached_sum_funcs;
} else {
  ps_point_plan_mark_runtime_fallback(thd);
  return false;
}
```

**问题**: 即使解决了 P0 的 `clone_item` 问题，将 PS arena 上的 `Item_sum` 对象
赋给 `join->sum_funcs` 也有以下风险：

1. **状态污染**: `AggregateIterator` 执行时会调用 `sum_funcs[i]->clear()`、
   `sum_funcs[i]->add()`、`sum_funcs[i]->val_*()` 等方法修改 `Item_sum` 内部状态。
   PS arena 对象在下次执行时不会被自动重置（`JOIN::reset()` 通过
   `join->sum_funcs` 调用 `clear()` 来重置，但前提是 `sum_funcs` 指向的是
   同一组对象）。

2. **result_field 绑定**: 执行过程中 `Item_sum` 的 `result_field` 可能需要
   指向当前执行上下文中的临时表字段，PS arena 上的克隆对象的 `result_field`
   可能指向已失效的内存。

3. **aggregator 生命周期**: `Item_sum` 内部的 `Aggregator` 对象可能持有
   与当前执行上下文绑定的状态。

**建议**: 采用 P0 修复方案 A — 不缓存 `Item_sum`，确保 `join->sum_funcs`
通过正常的 `alloc_func_list()` + `make_sum_func_list()` 初始化。
这彻底避免了跨执行状态污染问题。

---

## P2 [建议] `has_with_distinct()` 与 `sum_func()` 交互的文档说明

**位置**: Step 2 — validate 函数

**设计代码**:

```cpp
if (sum_item->has_with_distinct()) return false;
switch (sum_item->sum_func()) {
  case Item_sum::SUM_FUNC:
  case Item_sum::COUNT_FUNC:
  ...
```

**说明**: 对于 `Item_sum_sum`（SUM），当 `has_with_distinct() = true` 时，
`sum_func()` 实际返回 `SUM_DISTINCT_FUNC` 而非 `SUM_FUNC`
（`item_sum.h` line 1043-1045）。由于先检查了 `has_with_distinct()` 并提前
返回 false，不会出现逻辑错误。但 switch 中的 case 列表只含非-DISTINCT 变体，
阅读者可能会疑惑这些 DISTINCT 变体（`SUM_DISTINCT_FUNC`、`COUNT_DISTINCT_FUNC`）
在哪里被处理。

**建议**: 在 switch 的 default 分支添加注释说明 DISTINCT 变体已被前置检查拦截。

---

## P2 [建议] COUNT(*) 处理可能被 arg_count 检查提前拒绝

**位置**: Step 2 — validate 函数

**设计代码**:

```cpp
// 聚合参数必须是单列引用（不能是表达式）
if (sum_item->arg_count != 1) return false;
Item *arg = sum_item->args()[0];

// COUNT(*) 特殊处理
if (sum_item->sum_func() == Item_sum::COUNT_FUNC &&
    arg->type() == Item::INT_ITEM) { ... }
```

**问题**: 需要验证 MySQL 8.0 中 `COUNT(*)` 的 AST 表示。
如果 `COUNT(*)` 的 `arg_count == 0`（无参数），那么前面的
`if (sum_item->arg_count != 1) return false;` 会先将其拒绝。

**建议**: 确认 `COUNT(*)` 的实际 `arg_count` 值。如果为 0，需要将
COUNT(*) 特殊处理移到 `arg_count` 检查之前：

```cpp
// COUNT(*) 特殊处理（arg_count 可能为 0）
if (sum_item->sum_func() == Item_sum::COUNT_FUNC) {
  if (sum_item->arg_count == 0 ||
      (sum_item->arg_count == 1 && sum_item->args()[0]->type() == Item::INT_ITEM)) {
    tpl->aggregate_type = sum_item->sum_func();
    tpl->aggregate_field_index = MAX_KEY;
    tpl->aggregate_field_type = MYSQL_TYPE_LONGLONG;
    tpl->aggregate_field_unsigned = true;
    return true;
  }
}
// 其他聚合：参数必须是单列引用
if (sum_item->arg_count != 1) return false;
```

---

## P2 [建议] Admit / Fast path 大量代码重复

**位置**: Step 5-7

Step 5 的 `RANGE_PK_BETWEEN_AGG` admission 检查与 `RANGE_PK_BETWEEN`
高度重复（range 相关检查完全相同，只多了 AGGREGATE path 验证）。
Step 6 的 admission 保存和 Step 7 的 fast path 构建同样如此
（整个 INDEX_RANGE_SCAN 构建、QEP_TAB 设置都是复制粘贴）。

**建议**: 将公共逻辑提取为辅助函数：

| 辅助函数 | 用途 |
|----------|------|
| `ps_point_plan_can_admit_range_common()` | 公共 range admission 检查 |
| `ps_point_plan_admit_range_metadata()` | 公共 range metadata 保存 |
| `ps_point_plan_build_range_scan_common()` | 公共 INDEX_RANGE_SCAN 构建 |

AGG 类型的代码在调用公共函数后做差异化处理（AGGREGATE path 验证/构建、sum_funcs 设置）。
这既减少了代码量，也降低了两份代码不同步的风险。

---

## P2 [建议] Fast path 非缓存路径的变量名 typo

**位置**: Step 7 — 非缓存路径

```cpp
store_key high_store(thd, key_part->field, max_key + nullable,
                     nullable ? max_buf : nullptr, key_part->length,
                     tpl.params[1]);
```

`max_buf` 应为 `max_key`。（从 Phase 7 的 `RANGE_PK_BETWEEN` fast path
对照可知 line 697-698 使用的是 `max_key`。）

---

## P3 [小建议] Runtime guard G12 — 位置合理

Step 8 的 G12 聚合字段类型 guard 放在 runtime_guard 函数末尾（所有 range guard 之后），
这是合理的。聚合字段类型变化的场景（ALTER TABLE 改列类型）通常会同时改变
`table_ref_version`，被更前面的 G1d guard 捕获。G12 是兜底检查。

---

## P3 [小建议] 清理函数中 `cached_sum_funcs` 的处理

如果采用 P0 建议的方案 A（不缓存 `Item_sum`），则 `cached_sum_funcs` 字段
和 Step 9 中的清理逻辑都可以移除，进一步简化 `PsPointPlanTemplate` 结构体。

---

## 缺失的测试场景

当前测试用例覆盖了基本功能和常见负向场景，但以下场景建议补充：

| 场景 | 用途 | 建议 |
|------|------|------|
| DDL 后聚合字段类型变化 | 验证 G12 guard | `ALTER TABLE sbtest MODIFY k BIGINT` 后重新执行，应 invalidate |
| 聚合字段被删除 | 验证 guard | `ALTER TABLE sbtest DROP k` 后执行，应 invalidate |
| `SUM(id)` — 聚合字段同时是 range 字段 | 验证 field_index 不冲突 | 应正常缓存和命中 |
| `COUNT(k)` — 非 COUNT(*) 的 COUNT | 验证非星号 COUNT | 应正常缓存 |
| `SUM(k+1)` — 聚合含表达式 | 验证 classify 拒绝 | 应 NEVER |
| 空表 SUM 返回 NULL | 验证空集 aggregate 语义 | 结果应与普通路径一致 |
| `SELECT 1, SUM(k)` — SELECT list 含常量 | 验证 validate 中 INT_ITEM 分支 | 设计允许，需确认正确性 |
| concurrent DDL + PS EXECUTE | 验证 table_ref_version guard | 应 invalidate |
| `PREPARE` 后切换 `ps_point_plan_cache = OFF` | 验证 fallback | 应走普通路径 |
| `SUM(k)` 在 RANGE_PK_BETWEEN admission 后再执行 | 确认两种 plan_type 互不干扰 | 不同 PS 各自独立 |

---

## 风险表修正

原风险表中 "`Item_sum` 克隆不完整 → 使用标准 `clone_item()` 接口" 的缓解措施不成立
（`clone_item()` 返回 nullptr）。修正后：

| 风险 | 缓解措施 |
|------|----------|
| ~~Item_sum 克隆不完整~~ **`clone_item()` 返回 nullptr** | **方案 A: 不缓存 Item_sum，复用 JOIN 正常初始化的 sum_funcs** |
| 聚合语义不一致 | 完全复用现有 `AggregateIterator` |
| 空集处理差异 | `AggregateIterator` 的 NULL 处理自然兼容 |
| 内存泄漏 | Range 组件分配在 PS m_arena 上；Item_sum 使用 JOIN 正常生命周期 |
| 性能回退 | classify 阶段 early reject |
| `sum_func_count` 不存在 | 改用 `qb->agg_func_used()` + 手动遍历计数 |

---

## 方案可行性总结

| 维度 | 评估 | 说明 |
|------|------|------|
| 整体架构 | **合理** | 新增枚举值 + 各阶段分支扩展，不影响现有 POINT_EQ_REF 和 RANGE_PK_BETWEEN |
| classify 改动 | **需修正** | `sum_func_count` 字段不存在于 `Query_block`，需改用 `agg_func_used()` + 手动计数 |
| admission 改动 | **基本合理** | AGGREGATE path 验证建议放松 child 指针比较 |
| fast path 改动 | **需重新设计** | `clone_item()` 返回 nullptr 是阻塞性 bug；建议不缓存 Item_sum |
| runtime guard | **合理** | 聚合字段类型 drift 检查方向正确，位置合理 |
| 代码量估算 | **偏保守** | 如果提取公共函数实际 ~230 行可达；复制粘贴式会更多 |
| 性能预期 | **合理** | 15-20% 资源占比、5-10% 总体 QPS 提升的估算与 Phase 7 review 分析一致 |

**建议优先级**:

1. **[P0 必须]** 解决 `clone_item()` 返回 nullptr 问题 — 推荐方案 A（不缓存 Item_sum）
2. **[P1 必须]** 将 `qb->sum_func_count` 替换为 `qb->agg_func_used()` + 手动遍历
3. **[P1 必须]** 放松 AGGREGATE child 指针比较，改为 type + index 验证
4. **[P2 建议]** 提取 admit / fast path 公共代码为辅助函数，减少重复
5. **[P2 建议]** 确认 COUNT(*) 的 arg_count 值，调整检查顺序
6. **[P2 建议]** 修复非缓存路径 `max_buf` → `max_key` typo
7. **[P3 可选]** 补充缺失的测试场景（DDL 后 drift、空表、表达式聚合等）
