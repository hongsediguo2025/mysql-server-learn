# Phase 7: simple_ranges Plan Cache 详细实现设计 (修订版)

> 基于4个agents的代码审查结果修订，涵盖逻辑漏洞修复、异常处理补充、测试用例扩展和代码规范改进。

## 目标SQL

```sql
SELECT c FROM sbtest WHERE id BETWEEN ? AND ?
```

---

## 一、功能步骤分解

### Step 1: WHERE Shape 提取 (Phase 1 - Prepare阶段)

**目标**: 识别 `field BETWEEN param AND param` 形状的WHERE条件

**位置**: `ps_point_plan_extract_where_shape()` 扩展

**实现代码**:

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
      return extract_between_shape(eq_func, tbl, tpl);
    }
  }

  // ... 现有 AND 连接逻辑 ...
  return false;
}

// 新增helper: 提取BETWEEN形状
static bool extract_between_shape(Item_func *between_func,
                                 const Table_ref *tbl,
                                 PsPointPlanTemplate *tpl) {
  // 检查参数数量
  if (between_func->argument_count() != 3) return false;

  Item *arg0 = between_func->arguments()[0];  // field
  Item *arg1 = between_func->arguments()[1];  // low
  Item *arg2 = between_func->arguments()[2];  // high

  // 只支持: field BETWEEN param AND param 形式
  // 明确拒绝: param BETWEEN field AND field
  if (arg0->type() != Item::FIELD_ITEM) return false;
  if (arg1->type() != Item::PARAM_ITEM) return false;
  if (arg2->type() != Item::PARAM_ITEM) return false;

  // 拒绝 NOT BETWEEN
  if (between_func->negated) {
    // NOT BETWEEN 会走正常优化路径
    return false;
  }

  Item_field *field = down_cast<Item_field *>(arg0);
  Item_param *param_low = down_cast<Item_param *>(arg1);
  Item_param *param_high = down_cast<Item_param *>(arg2);

  // 验证field属于目标表
  if (field->table_ref != tbl) return false;

  // 检查字段类型是否支持范围扫描
  // 某些类型(如GEOMETRY)可能不支持
  if (field->field_type() == MYSQL_TYPE_GEOMETRY) {
    return false;
  }

  // 设置模板
  tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN;
  tpl->param_count = 2;
  tpl->params[0] = param_low;
  tpl->params[1] = param_high;
  tpl->field_indices[0] = field->field_index;

  return true;
}
```

**修复说明**:
- 分离BETWEEN识别逻辑到独立函数
- 明确拒绝NOT BETWEEN
- 添加字段类型检查
- 添加注释说明只支持一种参数顺序

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
  // 检查表数量
  if (join->primary_tables != 1) return false;
  if (join->qep_tab == nullptr) return false;

  const QEP_TAB *tab = &join->qep_tab[0];

  // 检查访问类型
  // JT_RANGE: 传统range scan
  // JT_INDEX_RANGE_SCAN: 新hypergraph optimizer的range
  if (tab->type() != JT_RANGE && tab->type() != JT_INDEX_RANGE_SCAN) {
    return false;
  }

  TABLE *table = tab->table();
  if (table == nullptr) return false;

  // 检查使用的是主键
  // 修复: 使用quick->index而不是ref().key
  if (tab->quick() == nullptr) return false;

  uint used_index = tab->quick()->index;
  const KEY *keyinfo = &table->key_info[used_index];

  if (!(keyinfo->flags & HA_NOSAME)) {
    return false;  // 必须是唯一索引(主键)
  }

  // 单列范围
  if (keyinfo->user_defined_key_parts != 1) {
    return false;
  }

  // 检查原始WHERE只有BETWEEN条件(无额外条件)
  if (tab->condition() != nullptr) {
    return false;
  }

  // 检查没有HAVING
  if (join->having_cond != nullptr) {
    return false;
  }

  // 检查QUICK_RANGE结构
  QUICK_SELECT_I *quick = tab->quick();
  if (quick->get_type() != QUICK_SELECT_I::QS_TYPE_RANGE_SCAN) {
    // 修复: 也允许range group min/max
    return false;
  }

  // 检查range数量(应该为1)
  QUICK_RANGE_SELECT *range_quick = down_cast<QUICK_RANGE_SELECT*>(quick);
  if (range_quick->get_num_ranges() != 1) {
    return false;  // 只支持单范围
  }

  return true;
}
```

**修复说明**:
- 修复使用quick->index而非ref().key
- 添加range数量检查
- 检查原始WHERE条件
- 区分不同的quick类型

---

### Step 3: 模板数据填充 (Phase 2 - Admission)

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
  QUICK_RANGE_SELECT *quick = down_cast<QUICK_RANGE_SELECT*>(tab->quick());

  if (quick == nullptr) {
    stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);
    return;
  }

  // 复制key元数据
  tpl.keyno = quick->index;
  tpl.key_parts = 1;  // 单列范围
  const KEY *key_info = &table->key_info[tpl.keyno];
  tpl.key_length = key_info->key_length;

  // 修复: 保存key part信息
  tpl.null_rejecting = 0;  // BETWEEN不拒绝NULL(除非字段定义拒绝)

  // 复制参数类型快照
  for (uint i = 0; i < 2; i++) {
    if (tpl.params[i] == nullptr) {
      stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);
      return;
    }
    tpl.actual_types[i] = tpl.params[i]->data_type_actual();
    tpl.unsigned_actuals[i] = tpl.params[i]->is_unsigned_actual();
    tpl.actual_collations[i] = ps_point_plan_actual_collation(tpl.params[i]);
  }

  // 修复: 添加字符串长度/精度信息
  for (uint i = 0; i < 2; i++) {
    if (is_string_type(tpl.actual_types[i])) {
      tpl.actual_lengths[i] = tpl.params[i]->max_char_length();
    } else if (is_decimal_type(tpl.actual_types[i])) {
      tpl.actual_decimals[i] = tpl.params[i]->decimals;
    }
  }

  // 复制优化器成本估算
  tpl.best_read = join->best_read;
  tpl.best_rowcount = static_cast<double>(join->best_rowcount);

  // 环境快照
  tpl.optimizer_switch = ps_point_plan_relevant_optimizer_switch(thd);
  tpl.table_ref_version = table->s->get_table_ref_version();
  tpl.relevant_sql_mode = ps_point_plan_relevant_sql_mode(thd);

  // Arena缓存range相关结构
  if (!cache_range_scan_helpers(thd, stmt, table, tab, tpl)) {
    // 缓存失败不代表admission失败，只是没有arena缓存
    // 仍然可以进入HOT状态，但每次需要在thd->mem_root上分配
  }

  stmt->set_ps_point_plan_state(PsPointPlanState::HOT);
  ps_point_plan_mark_admission(thd);
}
```

**修复说明**:
- 添加key part信息保存
- 添加字符串长度/精度信息
- 缓存失败不阻止admission
- 更好的错误处理

---

### Step 4: Arena缓存Range Scan组件

```cpp
static bool cache_range_scan_helpers(THD *thd, Prepared_statement *stmt,
                                     TABLE *table, const QEP_TAB *tab,
                                     PsPointPlanTemplate &tpl) {
  QUICK_RANGE_SELECT *quick = down_cast<QUICK_RANGE_SELECT*>(tab->quick());
  const KEY *key_info = &table->key_info[tpl.keyno];

  // 切换到PS arena
  Query_arena backup;
  thd->swap_query_arena(stmt->m_arena, &backup);
  const bool had_error_before_cache_build = thd->is_error();

  bool success = false;

  // ======== 缓存QUICK_RANGE模板 ========
  // 只缓存range结构模板，不缓存具体值
  QUICK_RANGE *range_template = nullptr;

  if (quick->get_num_ranges() > 0) {
    const QUICK_RANGE *src_range = quick->get_range(0);
    if (src_range != nullptr) {
      // 修复: 检查QUICK_RANGE是否可拷贝
      range_template = new (thd->mem_root) QUICK_RANGE;
      if (range_template != nullptr) {
        // 分配key buffer
        range_template->min_key = thd->mem_root->ArrayAlloc<uchar>(
            key_info->key_length);
        range_template->max_key = thd->mem_root->ArrayAlloc<uchar>(
            key_info->key_length);

        if (range_template->min_key != nullptr &&
            range_template->max_key != nullptr) {
          // 只拷贝结构，不拷贝值
          range_template->min_length = src_range->min_length;
          range_template->max_length = src_range->max_length;
          range_template->flag = src_range->flag;
          range_template->min_keypart_map = src_range->min_keypart_map;
          range_template->max_keypart_map = src_range->max_keypart_map;

          tpl.cached_range_template = range_template;
          success = true;
        }
      }
    }
  }

  // ======== 复用V1.2的QEP缓存逻辑 ========
  if (success && !tpl.qep_cached) {
    // ... 现有的QEP_TAB/QEP_shared缓存逻辑 ...
  }

  // 恢复arena
  thd->swap_query_arena(backup, &stmt->m_arena);
  if (!had_error_before_cache_build && thd->is_error()) {
    thd->clear_error();
  }

  return success;
}
```

**模板扩展**:

```cpp
// 在 ps_point_plan_cache.h 中扩展
struct PsPointPlanTemplate {
  // ... 现有字段 ...

  // ======== Phase 7: Range scan 相关 ========
  /// 缓存的QUICK_RANGE模板(不含值)
  QUICK_RANGE *cached_range_template{nullptr};

  /// 参数字符串长度(用于字符串类型)
  uint32_t actual_lengths[PS_PC_MAX_PARAMS]{};

  /// 参数小数位数(用于DECIMAL类型)
  uint8_t actual_decimals[PS_PC_MAX_PARAMS]{};

  /// range缓存是否有效
  bool range_cached{false};
};
```

**修复说明**:
- 只缓存range模板结构，不缓存值
- 分离值和结构的缓存
- 添加字符串长度和小数位数信息
- 更好的内存分配检查

---

### Step 5: Runtime Guard扩展

```cpp
static bool runtime_guard_range_scan(THD *thd, Prepared_statement *stmt,
                                     TABLE **table_out, KEY **keyinfo_out) {
  const PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();

  // ======== G1: 基础绑定检查 ========
  if (tpl.table_ref == nullptr || tpl.table_ref->table == nullptr) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  TABLE *table = tpl.table_ref->table;

  if (table->s == nullptr) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  // ======== G2: 环境漂移检查 ========
  if (ps_point_plan_relevant_optimizer_switch(thd) != tpl.optimizer_switch) {
    ps_point_plan_demote_to_cold(stmt);
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  if (table->s->get_table_ref_version() != tpl.table_ref_version) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  if (ps_point_plan_relevant_sql_mode(thd) != tpl.relevant_sql_mode) {
    ps_point_plan_demote_to_cold(stmt);
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  // ======== G3: 索引有效性检查 ========
  if (tpl.keyno >= table->s->keys) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  KEY *keyinfo = &table->key_info[tpl.keyno];

  if (!(actual_key_flags(keyinfo) & HA_NOSAME)) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  if (keyinfo->user_defined_key_parts != 1) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  if (keyinfo->key_part[0].fieldnr - 1 != tpl.field_indices[0]) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  // ======== G4: 修复竞态条件 - 先检查NULL再求值 ========
  for (uint i = 0; i < 2; i++) {
    if (tpl.params[i] == nullptr) {
      stmt->invalidate_ps_point_plan_cache();
      ps_point_plan_mark_invalidation(thd);
      return false;
    }

    // 修复: 先检查NULL状态
    if (tpl.params[i]->param_state() == Item_param::NULL_VALUE) {
      // NULL参数直接fallback，不尝试求值
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
  }

  // ======== G5: 参数类型匹配检查 ========
  for (uint i = 0; i < 2; i++) {
    // 基本类型匹配
    if (tpl.params[i]->data_type_actual() != tpl.actual_types[i]) {
      ps_point_plan_demote_to_cold(stmt);
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    // unsigned标志匹配
    if (tpl.params[i]->is_unsigned_actual() != tpl.unsigned_actuals[i]) {
      ps_point_plan_demote_to_cold(stmt);
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    // 字符串collation匹配
    const CHARSET_INFO *actual_coll = ps_point_plan_actual_collation(tpl.params[i]);
    if (actual_coll != nullptr && actual_coll != tpl.actual_collations[i]) {
      ps_point_plan_demote_to_cold(stmt);
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    // 修复: 字符串长度匹配检查
    if (is_string_type(tpl.actual_types[i])) {
      uint32_t actual_length = tpl.params[i]->max_char_length();
      if (actual_length > tpl.actual_lengths[i]) {
        // 字符串长度变长可能导致key buffer溢出
        ps_point_plan_demote_to_cold(stmt);
        ps_point_plan_mark_runtime_fallback(thd);
        return false;
      }
    }

    // 修复: DECIMAL精度匹配检查
    if (is_decimal_type(tpl.actual_types[i])) {
      uint8_t actual_decimals = tpl.params[i]->decimals;
      if (actual_decimals != tpl.actual_decimals[i]) {
        ps_point_plan_demote_to_cold(stmt);
        ps_point_plan_mark_runtime_fallback(thd);
        return false;
      }
    }
  }

  *table_out = table;
  *keyinfo_out = keyinfo;
  return true;
}
```

**修复说明**:
- 先检查NULL再求值(修复竞态)
- 添加字符串长度检查
- 添加DECIMAL精度检查
- 更清晰的分段注释

---

### Step 6: Fast Path构建

```cpp
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
  QEP_TAB *new_qep_tab = nullptr;
  QEP_TAB *tab = nullptr;
  bool qep_needs_cleanup = false;

  if (tpl.qep_cached) {
    // 复用arena缓存的QEP组件
    new_qep_tab = tpl.cached_qep_tab;

    // 修复: 使用placement new前先调用析构(如果有非trivial析构函数)
    // QEP_TAB和QEP_shared已确认是trivially destructible
    new (tpl.cached_qep_shared) QEP_shared();
    new (&new_qep_tab[0]) QEP_TAB();

    tab = &new_qep_tab[0];
    tab->set_qs(tpl.cached_qep_shared);
    tab->set_join(join);
    tab->set_idx(0);
    tab->set_table(table);
    tab->table_ref = tpl.table_ref;
    tab->set_type(JT_RANGE);

    qep_needs_cleanup = false;  // arena内存会自动清理
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

    qep_needs_cleanup = true;  // 需要手动清理
  }

  // Phase C: 从参数值构建实际的range
  QUICK_RANGE **dynamic_ranges = nullptr;
  uint num_ranges = 0;

  if (!build_dynamic_ranges_from_params(thd, table, keyinfo, tpl,
                                       &dynamic_ranges, &num_ranges)) {
    if (qep_needs_cleanup) {
      // 修复: 清理已分配的资源
      destroy(tab->quick());
      // QEP_TAB和QEP_shared会在mem_root清理时自动释放
    }
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  // Phase D: 创建QUICK_RANGE_SELECT
  QUICK_RANGE_SELECT *quick = new (thd->mem_root) QUICK_RANGE_SELECT(
      thd, table, tpl.keyno, true,  // sorted
      dynamic_ranges, num_ranges,
      /*where_cond=*/nullptr,
      /*use_cached_key_parts=*/true);

  if (quick == nullptr || quick->error) {
    if (qep_needs_cleanup && quick != nullptr) {
      destroy(quick);
    }
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  tab->set_quick(quick);

  // Phase E: 创建AccessPath
  AccessPath *path = NewIndexRangeScanAccessPath(
      thd, table, tpl.keyno,
      dynamic_ranges, num_ranges,
      /*count_examined_rows=*/true);

  if (path == nullptr) {
    if (qep_needs_cleanup) {
      destroy(quick);
    }
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  // 修复: 完整设置AccessPath
  path->set_num_output_rows(tpl.best_rowcount);
  path->cost = tpl.best_read;
  path->init_cost = 0.0;
  path->init_once_cost = 0.0;

  // 设置filter conditions(即使为nullptr也要显式设置)
  path->set_filter_cond(nullptr);

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

**修复说明**:
- 添加资源清理逻辑
- 完整设置AccessPath属性
- 区分arena和mem_root分配的清理策略
- 更好的错误处理

---

### Step 7: 动态Range构建

```cpp
static bool build_dynamic_ranges_from_params(
    THD *thd, TABLE *table, const KEY *keyinfo,
    const PsPointPlanTemplate &tpl,
    QUICK_RANGE ***ranges_out, uint *num_ranges_out) {

  Item_param *param_low = tpl.params[0];
  Item_param *param_high = tpl.params[1];

  // 修复: 先检查NULL(虽然guard已检查，防御性编程)
  if (param_low->null_value || param_high->null_value) {
    return false;
  }

  // 确保参数已求值
  if (!param_low->is_evaluated()) {
    param_low->val_real();  // 触发求值
    if (thd->is_error()) {
      // 求值失败
      return false;
    }
  }
  if (!param_high->is_evaluated()) {
    param_high->val_real();
    if (thd->is_error()) {
      return false;
    }
  }

  // 再次检查NULL(求值后可能变成NULL)
  if (param_low->null_value || param_high->null_value) {
    return false;
  }

  const KEY_PART_INFO *key_part = &keyinfo->key_part[0];

  // 修复: 使用正确的buffer大小
  const uint key_buffer_size = key_info->key_length;
  uchar *min_key_buffer = (uchar*)thd->mem_root->Alloc(key_buffer_size);
  uchar *max_key_buffer = (uchar*)thd->mem_root->Alloc(key_buffer_size);

  if (min_key_buffer == nullptr || max_key_buffer == nullptr) {
    return false;
  }

  // 序列化参数值到key buffer
  // 修复: 使用正确的key序列化函数
  uint min_length = make_sort_key(key_part, param_low,
                                  min_key_buffer, key_buffer_size);
  if (min_length == 0) return false;

  uint max_length = make_sort_key(key_part, param_high,
                                  max_key_buffer, key_buffer_size);
  if (max_length == 0) return false;

  // 检查low > high的情况
  // 修复: 对于字符串，使用正确的比较函数
  int cmp;
  if (key_part->field->result_type() == STRING_RESULT) {
    // 字符串比较: 使用collation
    const CHARSET_INFO *cs = key_part->field->charset();
    cmp = cs->coll->strnncollsp(
        cs, min_key_buffer, min_length,
        max_key_buffer, max_length, 0);
  } else {
    // 数值比较: 直接memcmp
    size_t cmp_len = std::min(min_length, max_length);
    cmp = memcmp(min_key_buffer, max_key_buffer, cmp_len);
    if (cmp == 0) {
      cmp = static_cast<int>(min_length) - static_cast<int>(max_length);
    }
  }

  if (cmp > 0) {
    // low > high, 空范围
    // 创建空range
    return create_empty_range(thd, ranges_out, num_ranges_out);
  }

  // 创建QUICK_RANGE
  QUICK_RANGE *range = new (thd->mem_root) QUICK_RANGE;
  if (range == nullptr) return false;

  range->min_key = min_key_buffer;
  range->max_key = max_key_buffer;
  range->min_length = min_length;
  range->max_length = max_length;
  range->flag = 0;  // 闭区间 [low, high]
  range->min_keypart_map = 1;
  range->max_keypart_map = 1;

  // 分配range数组
  QUICK_RANGE **ranges = thd->mem_root->ArrayAlloc<QUICK_RANGE*>(1);
  if (ranges == nullptr) return false;
  ranges[0] = range;

  *ranges_out = ranges;
  *num_ranges_out = 1;
  return true;
}

// 辅助函数: 创建空range
static bool create_empty_range(THD *thd,
                               QUICK_RANGE ***ranges_out,
                               uint *num_ranges_out) {
  // 创建一个永远不会匹配的range
  QUICK_RANGE *range = new (thd->mem_root) QUICK_RANGE;
  if (range == nullptr) return false;

  // 使用 (MAX, MIN) 这种永远不会匹配的组合
  // 或者设置特殊标志
  range->min_key = nullptr;
  range->max_key = nullptr;
  range->min_length = 0;
  range->max_length = 0;
  range->flag = QUICK_RANGE::EMPTY_RANGE;  // 新增标志
  range->min_keypart_map = 0;
  range->max_keypart_map = 0;

  QUICK_RANGE **ranges = thd->mem_root->ArrayAlloc<QUICK_RANGE*>(1);
  if (ranges == nullptr) return false;
  ranges[0] = range;

  *ranges_out = ranges;
  *num_ranges_out = 1;
  return true;
}
```

**修复说明**:
- 添加求值失败检查
- 添加字符串正确比较
- 添加内存分配检查
- 单独的空range创建函数
- 使用正确的key序列化函数

---

## 二、对象生命周期管理

### 2.1 需要缓存的对象

| 对象 | 位置 | 生命周期 | 失效条件 |
|------|------|----------|----------|
| `QUICK_RANGE` 模板 | PS m_arena | PS生命周期 | DDL, invalidate |
| `QEP_TAB[2]` | PS m_arena | PS生命周期 | 已在V1.2实现 |
| `QEP_shared` | PS m_arena | PS生命周期 | 已在V1.2实现 |
| `actual_types[]` | 模板 | PS生命周期 | 参数类型变化 |
| `actual_lengths[]` | 模板 | PS生命周期 | 字符串长度变化 |
| `actual_decimals[]` | 模板 | PS生命周期 | DECIMAL精度变化 |

### 2.2 每次执行创建的对象

| 对象 | 位置 | 生命周期 | 清理时机 |
|------|------|----------|----------|
| `QUICK_RANGE[]` (含值) | thd->mem_root | 单次执行 | mem_root清理 |
| `QUICK_RANGE_SELECT` | thd->mem_root | 单次执行 | mem_root清理 |
| `AccessPath` | thd->mem_root | 单次执行 | mem_root清理 |
| key buffer | thd->mem_root | 单次执行 | mem_root清理 |

### 2.3 失效场景枚举

```cpp
enum class PsInvalidationReason : uint8_t {
  NONE = 0,
  TABLE_DELETED,
  KEY_DROPPED,
  KEY_ALTERED,
  FIELD_TYPE_CHANGED,
  FIELD_NULLABLE_CHANGED,
  COLLATION_CHANGED,
  PARAM_TYPE_MISMATCH,
  PARAM_LENGTH_INCREASED,
  DECIMAL_PRECISION_CHANGED
};
```

---

## 三、回退机制

### 3.1 回退触发点

| 触发点 | 回退类型 | 状态变化 | 下次行为 |
|--------|----------|----------|----------|
| Classify失败 | 永久bypass | NEVER | 永不再尝试 |
| Admission失败 | 永久bypass | NEVER | 永不再尝试 |
| Runtime guard: 结构变化 | 永久bypass | INVALID | 等待reprepare |
| Runtime guard: 环境漂移 | 临时bypass | COLD | 重新admit |
| Runtime guard: NULL参数 | 临时bypass | HOT | 下次重试 |
| Runtime guard: 类型变化 | 临时bypass | COLD | 重新admit |
| Fast path构建失败 | 临时bypass | HOT | 下次重试 |

### 3.2 回退处理

```cpp
// 统一的回退处理宏/函数
#define PS_RANGE_FALLBACK(thd, reason) do { \
  ps_point_plan_mark_runtime_fallback(thd); \
  if (debug_checks) { \
    /* 记录回退原因 */ \
  } \
  return false; \
} while(0)

// 使用示例
if (param_low->null_value) {
  PS_RANGE_FALLBACK(thd, "NULL parameter");
}
```

---

## 四、Status Counters扩展

```cpp
// 在 system_variables.h 中新增
struct System_status_var {
  // ... 现有计数器 ...

  // Phase 7: Range scan相关
  ulonglong ps_point_plan_cache_range_hits;          // 命中次数
  ulonglong ps_point_plan_cache_range_admissions;     // admission次数
  ulonglong ps_point_plan_cache_range_invalidations;  // 失效次数

  // 细分fallback计数器
  ulonglong ps_point_plan_cache_range_null_param_fallback;
  ulonglong ps_point_plan_cache_range_type_mismatch_fallback;
  ulonglong ps_point_plan_cache_range_inverted_fallback;
  ulonglong ps_point_plan_cache_range_build_failure_fallback;

  // 调试计数器
  ulonglong ps_point_plan_cache_range_low_gt_high;
  ulonglong ps_point_plan_cache_range_collation_mismatch;
  ulonglong ps_point_plan_cache_range_decimal_mismatch;
};
```

---

## 五、完整代码流程

### 5.1 Prepare流程

```
Prepared_statement::prepare()
  └── prepare_query()
      └── ps_point_plan_classify()
          ├── 检查sysvar ON
          ├── 检查 1 <= param_count <= 2
          ├── 检查 SELECT, 单表, 无join
          ├── 检查 无聚合/排序/limit
          └── ps_point_plan_extract_where_shape()
              ├── extract_between_shape()
              │   ├── 检查 Item_func::BETWEEN
              │   ├── 检查 field BETWEEN param AND param 顺序
              │   ├── 拒绝 NOT BETWEEN
              │   ├── 拒绝 GEOMETRY类型
              │   └── 设置 plan_type=RANGE_PK_BETWEEN
              └── 状态: COLD 或 NEVER
```

### 5.2 首次Execute流程 (COLD → HOT)

```
Prepared_statement::execute() [COLD状态]
  └── JOIN::optimize()
      ├── optimizer preamble (完整执行)
      ├── make_join_plan()
      │   └── 生成 JT_RANGE 计划
      │       └── 使用 QUICK_RANGE_SELECT
      └── ps_point_plan_can_admit()
          └── can_admit_range_scan()
              ├── 检查 JT_RANGE / JT_INDEX_RANGE_SCAN
              ├── 检查使用主键 (quick->index)
              ├── 检查单列范围
              ├── 检查无额外条件
              ├── 检查QUICK_RANGE数量=1
              └── ps_point_plan_admit()
                  └── admit_range_scan()
                      ├── 复制key元数据 (keyno, key_length)
                      ├── 缓存参数类型 (actual_types, lengths, decimals)
                      ├── 缓存环境快照
                      └── cache_range_scan_helpers()
                          └── 在PS arena分配QUICK_RANGE模板
      └── 状态: COLD → HOT
```

### 5.3 后续Execute流程 (HOT hit)

```
Prepared_statement::execute() [HOT状态]
  └── JOIN::optimize() [early hook]
      └── ps_point_plan_build_fast_path()
          └── build_range_scan_fast_path()
              ├── runtime_guard_range_scan()
              │   ├── 检查TABLE binding
              │   ├── 检查table_ref_version
              │   ├── 检查optimizer_switch
              │   ├── 检查key结构未变
              │   ├── 先检查NULL再求值 (修复竞态)
              │   ├── 检查参数类型匹配
              │   ├── 检查字符串长度
              │   └── 检查DECIMAL精度
              ├── 复用或分配QEP_TAB
              ├── build_dynamic_ranges_from_params()
              │   ├── 确保参数已求值
              │   ├── 检查求值错误
              │   ├── 序列化到key buffer
              │   ├── 字符串/数值正确比较
              │   └── 处理low>high(空range)
              ├── 创建QUICK_RANGE_SELECT
              ├── 创建NewIndexRangeScanAccessPath
              └── 设置JOIN状态
                  └── 返回true → 跳过正常优化器
```

---

## 六、异常场景处理矩阵

| 异常场景 | 检测点 | 处理方式 | 状态变化 | 计数器 |
|---------|--------|---------|----------|--------|
| low参数为NULL | runtime guard (先检查) | fallback | HOT不变 | null_param_fallback |
| high参数为NULL | runtime guard (先检查) | fallback | HOT不变 | null_param_fallback |
| 两者都NULL | runtime guard (先检查) | fallback | HOT不变 | null_param_fallback |
| low > high | 动态range构建 | 空range或fallback | HOT不变 | inverted_fallback |
| 参数未绑定 | runtime guard | fallback | HOT不变 | type_mismatch |
| 求值失败 | 动态range构建 | fallback | HOT不变 | build_failure |
| 类型变化 | runtime guard | COLD demotion | HOT→COLD | type_mismatch |
| 字符集变化 | runtime guard | COLD demotion | HOT→COLD | collation_mismatch |
| 字符串变长 | runtime guard | COLD demotion | HOT→COLD | type_mismatch |
| 精度变化 | runtime guard | COLD demotion | HOT→COLD | decimal_mismatch |
| 索引删除 | runtime guard | INVALID | HOT→INVALID | invalidation |
| 字段类型变化 | table_ref_version | INVALID | HOT→INVALID | invalidation |
| low = high | 动态range构建 | 正常处理 | HOT不变 | (正常) |
| 空范围(0行) | 优化器/执行 | 正常返回 | HOT不变 | (正常) |

---

## 七、测试用例 (正交法设计)

### 测试维度

1. **参数值**: 正常, NULL(low), NULL(high), 两者NULL, MIN, MAX, 反向, 相等
2. **参数类型**: INT, BIGINT, VARCHAR, DATE, DATETIME, DECIMAL
3. **范围大小**: 单行, 小, 中等, 大, 全表, 空
4. **索引状态**: 主键存在, 主键删除, 二级索引, 无索引
5. **并发DDL**: 无, ALTER字段, DROP INDEX, CREATE INDEX
6. **字符集**: utf8mb4, latin1, binary, 字符集变更

### P0级测试 (必须通过)

| ID | 场景 | SQL | 输入 | 预期 |
|----|------|-----|------|------|
| P0-001 | 基本功能 | `SELECT c FROM t WHERE id BETWEEN ? AND ?` | `1, 100` | hit, 100行 |
| P0-002 | 小范围 | 同上 | `1, 100` | hit |
| P0-003 | 中等范围 | 同上 | `1, 10000` | hit |
| P0-004 | 大范围 | 同上 | `1, 1000000` | hit |
| P0-005 | 全表范围 | 同上 | `1, MAX` | hit |
| P0-006 | low=NULL | 同上 | `NULL, 100` | fallback |
| P0-007 | high=NULL | 同上 | `1, NULL` | fallback |
| P0-008 | 两者NULL | 同上 | `NULL, NULL` | fallback |
| P0-009 | MIN边界 | 同上 | `0, 100` | hit |
| P0-010 | MAX边界 | 同上 | `MAX-100, MAX` | hit |
| P0-011 | 反向范围 | 同上 | `100, 1` | fallback或空 |
| P0-012 | 相等边界 | 同上 | `50, 50` | hit, 1行 |

### P1级测试 (类型覆盖)

| ID | 类型 | 验证点 |
|----|------|--------|
| P1-001 | BIGINT | 大整数支持 |
| P1-002 | VARCHAR | collation处理 |
| P1-003 | DATE | 日期范围 |
| P1-004 | DATETIME | 时间戳范围 |
| P1-005 | DECIMAL | 精度保持 |

### P2级测试 (DDL/索引)

| ID | 场景 | 预期 |
|----|------|------|
| P2-001 | 主键删除后查询 | invalidate |
| P2-002 | 删除索引后查询 | invalidate |
| P2-003 | 字段类型变更 | invalidate |
| P2-004 | 二级索引BETWEEN | 不cache |

### P3级测试 (边界/并发)

| ID | 场景 | 验证点 |
|----|------|--------|
| P3-001 | 字符集变更 | collation检查 |
| P3-002 | 并发DDL | table_ref_version |
| P3-003 | 极限范围 | 不溢出 |
| P3-004 | 长字符串 | length检查 |

### MTR测试脚本结构

```sql
-- file: mysql-test/t/ps_point_plan_cache_ranges_basic.test

-- P0-001: 基本BETWEEN功能
--echo #
--echo # P0-001: 基本BETWEEN功能
--echo #
CREATE TABLE t_range (
  id INT PRIMARY KEY,
  c INT
) ENGINE=InnoDB;

INSERT INTO t_range VALUES
  (1,1), (2,2), (3,3), (4,4), (5,5);

PREPARE stmt FROM 'SELECT c FROM t_range WHERE id BETWEEN ? AND ?';

-- 首次执行: admission
EXECUTE stmt USING 1, 5;

-- 检查admission计数器
--let $admission = query_get_value("SHOW STATUS LIKE 'ps_point_plan_cache_range_admissions'", "Value", 1);
--echo $admission 应该为 1

-- 第二次执行: 应该命中
EXECUTE stmt USING 1, 5;

-- 检查hit计数器
--let $hits = query_get_value("SHOW STATUS LIKE 'ps_point_plan_cache_range_hits'", "Value", 1);
--echo $hits 应该为 1

-- 不同范围
EXECUTE stmt USING 6, 10;

-- P0-012: 相等边界
EXECUTE stmt USING 3, 3;
--echo 应该返回1行

-- P0-006: NULL参数
EXECUTE stmt USING NULL, 5;
--echo 应该fallback

-- 检查NULL fallback计数器
--let $null_fallback = query_get_value("SHOW STATUS LIKE 'ps_point_plan_cache_range_null_param_fallback'", "Value", 1);
--echo $null_fallback 应该为 1

-- P0-011: 反向范围
EXECUTE stmt USING 10, 1;
--echo 应该返回空结果或fallback

-- Cleanup
DEALLOCATE PREPARE stmt;
DROP TABLE t_range;
```

---

## 八、常见问题和解决方案

### Q1: 为什么只支持单列范围？

A: 多列范围范围扫描的复杂度显著增加：
- 需要处理复合索引的key part顺序
- QUICK_RANGE的序列化更复杂
- 参数顺序验证更困难

单列范围覆盖sysbench的主要场景，多列范围可以在后续Phase添加。

### Q2: 为什么拒绝NOT BETWEEN？

A: NOT BETWEEN的语义是 `field < low OR field > high`，这不是连续的范围扫描，优化器会使用不同的策略。缓存NOT BETWEEN需要额外的复杂度，收益不大。

### Q3: 如何处理low > high？

A: 有两种处理策略：
1. **fallback**: 让优化器处理（推荐）
2. **空range**: 创建一个永远不会匹配的range

当前实现使用策略1，因为优化器可能有更好的处理方式（如使用索引跳过）。

### Q4: 为什么在runtime guard先检查NULL？

A: 存在竞态条件：另一个线程可能在参数求值前将其设置为NULL。先检查NULL可以避免对NULL值求值可能导致的错误。

### Q5: arena缓存失败会影响admission吗？

A: 不会。arena缓存只是性能优化，即使失败，PS仍然可以进入HOT状态，只是每次执行需要在thd->mem_root上分配组件。

### Q6: 如何调试cache行为？

A: 使用status counters和诊断日志：

```sql
-- 查看cache状态
SHOW STATUS LIKE 'ps_point_plan_cache%';

-- 开启诊断
SET GLOBAL ps_point_plan_cache_debug=ON;

-- 查看执行计划
EXPLAIN SELECT c FROM sbtest WHERE id BETWEEN 1 AND 100;
```
