# Phase 7: simple_ranges Plan Cache 详细实现设计

## 目标SQL

```sql
SELECT c FROM sbtest WHERE id BETWEEN ? AND ?
```

---

## 一、功能步骤分解

### Step 1: WHERE Shape 提取 (Phase 1 - Prepare阶段)

**目标**: 识别 `field BETWEEN param AND param` 形状的WHERE条件

**位置**: `ps_point_plan_extract_where_shape()` 扩展

**实现**:

```cpp
// 在 ps_point_plan_cache.cc 中扩展
bool ps_point_plan_extract_where_shape(Query_block *qb,
                                       PsPointPlanTemplate *tpl) {
  Item *where = qb->where_cond();
  if (where == nullptr) return false;

  const Table_ref *tbl = tpl->table_ref;
  tpl->plan_type = PsCachedPlanType::POINT_EQ_REF;
  tpl->param_count = 0;

  // 现有: 单个等值
  if (where->type() == Item::FUNC_ITEM) {
    Item_func *eq_func = down_cast<Item_func *>(where);
    if (eq_func->functype() == Item_func::EQ_FUNC) {
      // ... 现有等值逻辑
      return true;
    }

    // ======== 新增: BETWEEN 识别 ========
    if (eq_func->functype() == Item_func::BETWEEN) {
      // BETWEEN 有3个参数: field, low, high
      if (eq_func->argument_count() != 3) return false;

      Item *arg0 = eq_func->arguments()[0];  // field
      Item *arg1 = eq_func->arguments()[1];  // low
      Item *arg2 = eq_func->arguments()[2];  // high

      // 验证: arg0必须是field, arg1/arg2必须是param
      if (arg0->type() != Item::FIELD_ITEM) return false;
      if (arg1->type() != Item::PARAM_ITEM) return false;
      if (arg2->type() != Item::PARAM_ITEM) return false;

      Item_field *field = down_cast<Item_field *>(arg0);
      Item_param *param_low = down_cast<Item_param *>(arg1);
      Item_param *param_high = down_cast<Item_param *>(arg2);

      // 验证field属于目标表
      if (field->table_ref != tbl) return false;

      // 检查NOT BETWEEN (暂不支持)
      if (eq_func->negated) return false;

      // 设置模板
      tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN;
      tpl->param_count = 2;
      tpl->params[0] = param_low;
      tpl->params[1] = param_high;
      tpl->field_indices[0] = field->field_index;

      return true;
    }
  }

  // 现有: AND连接的多个等值
  // ...

  return false;
}
```

**关键点**:
1. BETWEEN参数顺序固定: `field BETWEEN low AND high`
2. 拒绝 `NOT BETWEEN`
3. 拒绝 `param BETWEEN field AND field` 形式

---

### Step 2: Admission 条件判定 (Phase 2 - 首次Execute后)

**目标**: 验证优化器选择了正确的range scan计划

**位置**: `ps_point_plan_can_admit()` 扩展

```cpp
bool ps_point_plan_can_admit(Prepared_statement *stmt, JOIN *join) {
  const PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();

  // ... 现有的 POINT_EQ_REF 检查 ...

  // ======== 新增: RANGE_PK_BETWEEN admission ========
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN) {
    return can_admit_range_scan(stmt, join, tpl);
  }

  // ...
}

// 新增helper函数
static bool can_admit_range_scan(Prepared_statement *stmt, JOIN *join,
                                 const PsPointPlanTemplate &tpl) {
  if (join->primary_tables != 1) return false;
  if (join->qep_tab == nullptr) return false;

  const QEP_TAB *tab = &join->qep_tab[0];

  // 检查访问类型: 必须是range scan
  // 注意: 优化器可能选择 JT_RANGE 或 JT_INDEX_RANGE_SCAN
  if (tab->type() != JT_RANGE && tab->type() != JT_INDEX_RANGE_SCAN) {
    return false;
  }

  TABLE *table = tab->table();
  if (table == nullptr) return false;

  // 检查使用的是主键
  const KEY *keyinfo = &table->key_info[tab->ref().key];
  if (!(keyinfo->flags & HA_NOSAME)) {
    return false;  // 必须是唯一索引(主键)
  }

  // 单列范围
  if (keyinfo->user_defined_key_parts != 1) {
    return false;
  }

  // 检查ref结构(对于range scan, ref可能为空或部分填充)
  // RANGE扫描使用 QUICK_RANGE 而非传统的ref结构

  // 检查没有额外条件
  if (tab->condition() != nullptr) {
    return false;
  }

  // 检查没有HAVING
  if (join->having_cond != nullptr) {
    return false;
  }

  // 检查 QUICK_RANGE 结构存在
  // 注意: 在 JT_RANGE 情况下，quick需有效
  if (tab->quick() == nullptr) {
    return false;
  }

  // 验证quick是范围扫描
  if (tab->quick()->get_type() != QUICK_SELECT_I::QS_TYPE_RANGE_SCAN) {
    return false;
  }

  return true;
}
```

---

### Step 3: 模板数据填充 (Phase 2 - Admission)

**目标**: 将优化器的range scan元数据复制到模板

**位置**: `ps_point_plan_admit()` 扩展

```cpp
void ps_point_plan_admit(THD *thd, Prepared_statement *stmt, JOIN *join) {
  PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();
  const QEP_TAB *tab = &join->qep_tab[0];

  // ... 现有 POINT_EQ_REF 逻辑 ...

  // ======== 新增: RANGE_PK_BETWEEN admission ========
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN) {
    admit_range_scan(thd, stmt, join, tab, tpl);
    return;
  }

  // ...
}

static void admit_range_scan(THD *thd, Prepared_statement *stmt,
                            JOIN *join, const QEP_TAB *tab,
                            PsPointPlanTemplate &tpl) {
  TABLE *table = tab->table();

  // 获取range scan信息
  QUICK_RANGE_SELECT *quick = down_cast<QUICK_RANGE_SELECT *>(tab->quick());
  if (quick == nullptr) {
    stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);
    return;
  }

  // 复制key元数据
  tpl.keyno = quick->index;
  tpl.key_parts = 1;  // 单列范围
  tpl.key_length = table->key_info[quick->index].key_length;

  // 复制参数类型快照
  for (uint i = 0; i < 2; i++) {
    tpl.actual_types[i] = tpl.params[i]->data_type_actual();
    tpl.unsigned_actuals[i] = tpl.params[i]->is_unsigned_actual();
    tpl.actual_collations[i] = ps_point_plan_actual_collation(tpl.params[i]);
  }

  // 复制优化器成本估算
  tpl.best_read = join->best_read;
  tpl.best_rowcount = static_cast<double>(join->best_rowcount);

  // 环境快照
  tpl.optimizer_switch = ps_point_plan_relevant_optimizer_switch(thd);
  tpl.table_ref_version = table->s->get_table_ref_version();
  tpl.relevant_sql_mode = ps_point_plan_relevant_sql_mode(thd);

  // Arena缓存range相关结构
  cache_range_scan_helpers(thd, stmt, table, tab, tpl);

  stmt->set_ps_point_plan_state(PsPointPlanState::HOT);
  ps_point_plan_mark_admission(thd);
}
```

---

### Step 4: Arena缓存Range Scan组件

**目标**: 在PS arena上缓存range scan需要的组件

```cpp
static void cache_range_scan_helpers(THD *thd, Prepared_statement *stmt,
                                    TABLE *table, const QEP_TAB *tab,
                                    PsPointPlanTemplate &tpl) {
  QUICK_RANGE_SELECT *quick = down_cast<QUICK_RANGE_SELECT *>(tab->quick());
  const KEY *key_info = &table->key_info[tpl.keyno];

  // 切换到PS arena
  Query_arena backup;
  thd->swap_query_arena(stmt->m_arena, &backup);
  const bool had_error_before_cache_build = thd->is_error();

  // ======== 缓存QUICK_RANGE数组 ========
  // 从quick中提取ranges
  QUICK_RANGE **ranges = nullptr;
  uint num_ranges = 0;

  if (quick-> != nullptr) {
    // 获取range数量
    num_ranges = quick->get_num_ranges();

    // 分配range数组
    ranges = thd->mem_root->ArrayAlloc<QUICK_RANGE*>(num_ranges);
    if (ranges != nullptr) {
      // 深拷贝每个QUICK_RANGE
      for (uint i = 0; i < num_ranges && !thd->is_error(); i++) {
        const QUICK_RANGE *src_range = quick->get_range(i);

        // 分配新QUICK_RANGE
        QUICK_RANGE *dst_range = new (thd->mem_root) QUICK_RANGE(*src_range);

        // 拷贝key buffer
        if (src_range->min_key) {
          dst_range->min_key = thd->mem_root->ArrayAlloc<uchar>(
              key_info->key_length);
          memcpy(dst_range->min_key, src_range->min_key,
                 src_range->min_length);
        }
        if (src_range->max_key) {
          dst_range->max_key = thd->mem_root->ArrayAlloc<uchar>(
              key_info->key_length);
          memcpy(dst_range->max_key, src_range->max_key,
                 src_range->max_length);
        }

        ranges[i] = dst_range;
      }
    }
  }

  // ======== 缓存QEP组件 (复用V1.2逻辑) ========
  if (!tpl.qep_cached) {
    // ... 现有的QEP_TAB/QEP_shared缓存逻辑 ...
  }

  // 保存到模板
  tpl.cached_ranges = ranges;
  tpl.num_cached_ranges = num_ranges;
  tpl.range_cached = (ranges != nullptr && !thd->is_error());

  // 恢复arena
  thd->swap_query_arena(backup, &stmt->m_arena);
  if (!had_error_before_cache_build && thd->is_error()) {
    thd->clear_error();
  }
}
```

**模板扩展**:

```cpp
// 在 ps_point_plan_cache.h 中扩展
struct PsPointPlanTemplate {
  // ... 现有字段 ...

  // ======== Phase 7: Range scan 相关 ========
  /// 缓存的QUICK_RANGE数组
  QUICK_RANGE **cached_ranges{nullptr};
  /// 缓存的range数量
  uint num_cached_ranges{0};
  /// range缓存是否有效
  bool range_cached{false};
};
```

---

### Step 5: Runtime Guard扩展

**目标**: 执行前验证所有条件仍满足

```cpp
bool ps_point_plan_runtime_guard(THD *thd, Prepared_statement *stmt,
                                 TABLE **table_out, KEY **keyinfo_out) {
  const PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();

  // ... 现有guard逻辑 ...

  // ======== 新增: RANGE_PK_BETWEEN guard ========
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN) {
    if (!runtime_guard_range_scan(thd, stmt, table_out, keyinfo_out)) {
      return false;
    }
  }

  // ...
}

static bool runtime_guard_range_scan(THD *thd, Prepared_statement *stmt,
                                    TABLE **table_out, KEY **keyinfo_out) {
  const PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();

  // G1: TABLE binding
  if (tpl.table_ref == nullptr || tpl.table_ref->table == nullptr) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  TABLE *table = tpl.table_ref->table;

  // G2: TABLE_SHARE
  if (table->s == nullptr) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  // G3: optimizer_switch drift
  if (ps_point_plan_relevant_optimizer_switch(thd) != tpl.optimizer_switch) {
    ps_point_plan_demote_to_cold(stmt);
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  // G4: table_ref_version drift
  if (table->s->get_table_ref_version() != tpl.table_ref_version) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  // G5: sql_mode drift
  if (ps_point_plan_relevant_sql_mode(thd) != tpl.relevant_sql_mode) {
    ps_point_plan_demote_to_cold(stmt);
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  // G6: key index valid
  if (tpl.keyno >= table->s->keys) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  KEY *keyinfo = &table->key_info[tpl.keyno];

  // G7: key still unique
  if (!(actual_key_flags(keyinfo) & HA_NOSAME)) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  // G8: key part count still matches
  if (keyinfo->user_defined_key_parts != 1) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  // G9: field ordinal still matches
  if (keyinfo->key_part[0].fieldnr - 1 != tpl.field_indices[0]) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  // ======== 新增: BETWEEN参数guard ========
  for (uint i = 0; i < 2; i++) {
    // G10: param pointer sanity
    if (tpl.params[i] == nullptr) {
      stmt->invalidate_ps_point_plan_cache();
      ps_point_plan_mark_invalidation(thd);
      return false;
    }

    // G11: NULL参数 → runtime fallback
    if (tpl.params[i]->param_state() == Item_param::NULL_VALUE) {
      // NULL参数可能导致意外的空范围，让优化器处理
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    // G12: 参数实际类型匹配
    if (tpl.params[i]->data_type_actual() != tpl.actual_types[i]) {
      ps_point_plan_demote_to_cold(stmt);
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    // G13: unsigned标志匹配
    if (tpl.params[i]->is_unsigned_actual() != tpl.unsigned_actuals[i]) {
      ps_point_plan_demote_to_cold(stmt);
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    // G14: 字符串collation匹配
    if (ps_point_plan_actual_collation(tpl.params[i]) !=
        tpl.actual_collations[i]) {
      ps_point_plan_demote_to_cold(stmt);
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
  }

  *table_out = table;
  *keyinfo_out = keyinfo;
  return true;
}
```

---

### Step 6: Fast Path构建

**目标**: 直接构建INDEX_RANGE_SCAN AccessPath

```cpp
bool ps_point_plan_build_fast_path(THD *thd, JOIN *join,
                                   Prepared_statement *stmt) {
  const PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();

  // ... 现有 POINT_EQ_REF 逻辑 ...

  // ======== 新增: RANGE_PK_BETWEEN fast path ========
  if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN) {
    return build_range_scan_fast_path(thd, join, stmt, tpl);
  }

  // ...
}

static bool build_range_scan_fast_path(THD *thd, JOIN *join,
                                     Prepared_statement *stmt,
                                     const PsPointPlanTemplate &tpl) {
  // Phase A: Runtime guard
  TABLE *table = nullptr;
  KEY *keyinfo = nullptr;
  if (!runtime_guard_range_scan(thd, stmt, &table, &keyinfo)) {
    return false;  // fallback到正常优化器
  }

  // Phase B: 构建QEP_TAB
  QEP_TAB *new_qep_tab;
  QEP_TAB *tab;

  if (tpl.qep_cached) {
    // 复用arena缓存的QEP组件
    new_qep_tab = tpl.cached_qep_tab;
    new (tpl.cached_qep_shared) QEP_shared();
    new (&new_qep_tab[0]) QEP_TAB();
    tab = &new_qep_tab[0];
    tab->set_qs(tpl.cached_qep_shared);
    tab->set_join(join);
    tab->set_idx(0);
    tab->set_table(table);
    tab->table_ref = tpl.table_ref;
    tab->set_type(JT_RANGE);  // 设置为RANGE类型

    // 设置quick(需要在thd->mem_root上创建)
    // ...
  } else {
    // 在thd->mem_root上分配
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
    tab->set_type(JT_RANGE);
  }

  // Phase C: 从参数值构建实际的range
  QUICK_RANGE **dynamic_ranges = nullptr;
  if (!build_dynamic_ranges_from_params(thd, table, keyinfo, tpl,
                                       &dynamic_ranges)) {
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  // Phase D: 创建QUICK_RANGE_SELECT
  QUICK_RANGE_SELECT *quick = new (thd->mem_root) QUICK_RANGE_SELECT(
      thd, table, tpl.keyno, true,  // true = sorted
      dynamic_ranges, tpl.num_cached_ranges,
      /*where_cond=*/nullptr,
      /*use_cached_key_parts=*/true);

  if (quick == nullptr || quick->error) {
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  tab->set_quick(quick);

  // Phase E: 创建AccessPath
  AccessPath *path = NewIndexRangeScanAccessPath(
      thd, table, tpl.keyno,
      dynamic_ranges, tpl.num_cached_ranges,
      /*count_examined_rows=*/true);

  if (path == nullptr) {
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  path->set_num_output_rows(tpl.best_rowcount);
  path->cost = tpl.best_read;
  path->init_cost = 0.0;
  path->init_once_cost = 0.0;

  // Phase F: 提交JOIN状态
  join->tables = 1;
  join->primary_tables = 1;
  join->const_tables = 0;
  join->best_read = tpl.best_read;
  join->best_rowcount = static_cast<ha_rows>(tpl.best_rowcount);
  join->where_cond = nullptr;
  join->having_cond = nullptr;
  join->qep_tab = new_qep_tab;
  join->set_root_access_path(path);

  ps_point_plan_mark_hit(thd);
  return true;
}
```

---

### Step 7: 动态Range构建

**目标**: 从当前参数值构建QUICK_RANGE数组

```cpp
static bool build_dynamic_ranges_from_params(
    THD *thd, TABLE *table, const KEY *keyinfo,
    const PsPointPlanTemplate &tpl,
    QUICK_RANGE ***ranges_out) {

  // 从参数获取值
  Item_param *param_low = tpl.params[0];
  Item_param *param_high = tpl.params[1];

  // 确保参数已求值
  if (!param_low->is_null_value && !param_low->is_evaluated()) {
    param_low->val_real();  // 触发求值
  }
  if (!param_high->is_null_value && !param_high->is_evaluated()) {
    param_high->val_real();
  }

  // 检查NULL参数(已在guard中处理，这里防御性检查)
  if (param_low->null_value || param_high->null_value) {
    return false;
  }

  // 序列化参数值到key buffer
  const KEY_PART_INFO *key_part = &keyinfo->key_part[0];
  uchar min_key_buffer[MAX_KEY_LENGTH];
  uchar max_key_buffer[MAX_KEY_LENGTH];

  // 序列化low值
  uint min_length = make_key_image(key_part, param_low,
                                  min_key_buffer, /*is_null=*/false);
  if (min_length == 0) return false;

  // 序列化high值
  uint max_length = make_key_image(key_part, param_high,
                                  max_key_buffer, /*is_null=*/false);
  if (max_length == 0) return false;

  // 检查low > high的情况
  int cmp = key_part->field->key_cmp(min_key_buffer, max_key_buffer);
  if (cmp > 0) {
    // low > high, 空范围
    return false;  // 让优化器处理
  }

  // 创建QUICK_RANGE
  QUICK_RANGE *range = new (thd->mem_root) QUICK_RANGE;
  if (range == nullptr) return false;

  range->min_key = thd->mem_root->ArrayAlloc<uchar>(keyinfo->key_length);
  range->max_key = thd->mem_root->ArrayAlloc<uchar>(keyinfo->key_length);
  if (range->min_key == nullptr || range->max_key == nullptr) {
    return false;
  }

  memcpy(range->min_key, min_key_buffer, min_length);
  memcpy(range->max_key, max_key_buffer, max_length);

  range->min_length = min_length;
  range->max_length = max_length;
  range->flag = 0;  // 闭区间 [low, high]

  // 设置key part map
  range->min_keypart_map = 1;
  range->max_keypart_map = 1;

  // 分配range数组
  QUICK_RANGE **ranges = thd->mem_root->ArrayAlloc<QUICK_RANGE*>(1);
  if (ranges == nullptr) return false;
  ranges[0] = range;

  *ranges_out = ranges;
  return true;
}
```

---

## 二、对象生命周期管理

### 2.1 需要缓存的对象

| 对象 | 位置 | 生命周期 | 失效条件 |
|------|------|----------|----------|
| `QUICK_RANGE*[]` | PS m_arena | PS生命周期 | DDL, invalidate |
| `QEP_TAB[2]` | PS m_arena | PS生命周期 | 已在V1.2实现 |
| `QEP_shared` | PS m_arena | PS生命周期 | 已在V1.2实现 |
| `actual_types[]` | 模板 | PS生命周期 | 参数类型变化 |
| `actual_collations[]` | 模板 | PS生命周期 | 字符集变化 |

### 2.2 每次执行创建的对象

| 对象 | 位置 | 生命周期 |
|------|------|----------|
| `QUICK_RANGE*[]` (动态) | thd->mem_root | 单次执行 |
| `QUICK_RANGE_SELECT` | thd->mem_root | 单次执行 |
| `AccessPath` | thd->mem_root | 单次执行 |

### 2.3 失效场景

```cpp
// 在 ps_point_plan_cache.h 中定义失效条件
enum class PsInvalidationReason {
  // ... 现有原因 ...
  KEY_STRUCTURE_CHANGED,      // 索引结构变化
  FIELD_TYPE_CHANGED,         // 字段类型变化
  COLLATION_CHANGED,          // 字符集变化
  RANGE_PARAM_COUNT_CHANGED,  // 参数数量变化
};
```

---

## 三、回退机制

### 3.1 回退触发点

| 触发点 | 回退类型 | 状态变化 |
|--------|----------|----------|
| Classify失败 | 永久bypass | NEVER |
| Admission失败 | 永久bypass | NEVER |
| Runtime guard失败(类型) | 临时bypass | COLD (可重试) |
| Runtime guard失败(NULL) | 临时bypass | HOT (下次重试) |
| Runtime guard失败(结构) | 永久bypass | INVALID |
| Fast path构建失败 | 临时bypass | HOT (下次重试) |

### 3.2 回退处理

```cpp
// 所有回退都通过返回false实现
// 调用方会fall through到正常优化器路径

// 示例: fast path构建失败
if (path == nullptr) {
  ps_point_plan_mark_runtime_fallback(thd);
  return false;  // 让JOIN::optimize()继续
}
```

---

## 四、Status Counters扩展

```cpp
// 在 system_variables.h 中新增
struct System_status_var {
  // ... 现有计数器 ...

  // Phase 7: Range scan相关
  ulonglong ps_point_plan_cache_range_hits;
  ulonglong ps_point_plan_cache_range_admissions;
  ulonglong ps_point_plan_cache_range_null_param_fallback;
  ulonglong ps_point_plan_cache_range_type_mismatch_fallback;
  ulonglong ps_point_plan_cache_range_inverted_fallback;
};
```

---

## 五、完整代码流程

### 5.1 Prepare流程

```
Prepared_statement::prepare()
  └── prepare_query()
      └── ps_point_plan_classify()
          ├── 检查sysvar
          ├── 检查基本条件
          ├── 检查单表/无join
          ├── 检查无聚合/排序
          └── ps_point_plan_extract_where_shape()
              ├── 识别 field = ?      → POINT_EQ_REF
              └── 识别 field BETWEEN ? AND ?  → RANGE_PK_BETWEEN
                  └── 设置模板: plan_type, params[2], field_indices[0]
```

### 5.2 首次Execute流程

```
Prepared_statement::execute() [COLD状态]
  └── JOIN::optimize()
      ├── optimizer preamble
      ├── make_join_plan()
      │   └── 生成 JT_RANGE 计划
      └── ps_point_plan_can_admit()
          └── can_admit_range_scan()
              ├── 检查 JT_RANGE
              ├── 检查主键
              ├── 检查单列
              └── ps_point_plan_admit()
                  └── admit_range_scan()
                      ├── 复制key元数据
                      ├── 缓存参数类型
                      └── cache_range_scan_helpers()
                          └── 在PS arena分配QUICK_RANGE[]
      └── 状态: COLD → HOT
```

### 5.3 后续Execute流程

```
Prepared_statement::execute() [HOT状态]
  └── JOIN::optimize()
      └── [early hook] ps_point_plan_build_fast_path()
          └── build_range_scan_fast_path()
              ├── runtime_guard_range_scan()
              │   ├── 检查TABLE binding
              │   ├── 检查key结构未变
              │   ├── 检查参数类型匹配
              │   └── 检查参数非NULL
              ├── build_dynamic_ranges_from_params()
              │   ├── 序列化参数值
              │   ├── 检查 low <= high
              │   └── 创建QUICK_RANGE
              ├── 创建QUICK_RANGE_SELECT
              ├── 创建NewIndexRangeScanAccessPath
              └── 设置JOIN状态
                  └── 返回true (跳过正常优化器)
```

---

## 六、异常场景处理

| 异常 | 检测点 | 处理 |
|------|--------|------|
| low参数为NULL | runtime guard | fallback (不缓存) |
| high参数为NULL | runtime guard | fallback (不缓存) |
| low > high | 动态range构建 | fallback |
| 参数类型变化 | runtime guard | COLD demotion |
| 索引被删除 | runtime guard | INVALID |
| 字段类型变化 | table_ref_version | INVALID |
| low = high | 正常处理 | 支持退化到点查询 |
| 范围极小(1行) | 正常处理 | 仍然走range路径 |
| 范围极大(全表) | 正常处理 | 优化器会处理 |

---

## 七、测试用例需求

### 7.1 正常场景

```sql
-- 基本BETWEEN
PREPARE stmt1 FROM 'SELECT c FROM sbtest WHERE id BETWEEN ? AND ?';
EXECUTE stmt1 USING 1, 100;
EXECUTE stmt1 USING 101, 200;  -- 应命中

-- 相同边界
EXECUTE stmt1 USING 50, 50;   -- 应命中

-- 大范围
EXECUTE stmt1 USING 1, 1000000;  -- 应命中
```

### 7.2 异常场景

```sql
-- NULL参数
EXECUTE stmt1 USING NULL, 100;   -- 应fallback
EXECUTE stmt1 USING 1, NULL;     -- 应fallback

-- 反向范围
EXECUTE stmt1 USING 100, 1;      -- 应fallback

-- 类型不匹配
SET @v = 'abc';
EXECUTE stmt1 USING @v, 100;     -- 应fallback
```

### 7.3 DDL场景

```sql
-- 删除索引
ALTER TABLE sbtest DROP PRIMARY KEY;
EXECUTE stmt1 USING 1, 100;      -- 应invalidate

-- 重建索引
ALTER TABLE sbtest ADD PRIMARY KEY (id);
EXECUTE stmt1 USING 1, 100;      -- 应reprepare后重新admit
```
