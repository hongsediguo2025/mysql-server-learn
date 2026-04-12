# Plan Cache 扩展：支持 Sysbench 只读场景完整覆盖

## Context

当前 `ps_point_plan_cache` V1.2 实现已支持 `POINT_EQ_REF`（主键等值点查），覆盖 sysbench `oltp_read_only` 16条SQL中的10条（62.5%）。

为实现100%覆盖，需要扩展支持以下4种SQL类型：
1. **simple_ranges**: `SELECT c FROM sbtest WHERE id BETWEEN ? AND ?`
2. **sum_ranges**: `SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?`
3. **order_ranges**: `SELECT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c`
4. **distinct_ranges**: `SELECT DISTINCT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c`

按 optimize 资源消耗排序（从低到高）：
- point_selects (已支持): ~3-5%
- **simple_ranges**: ~8-12%
- **sum_ranges**: ~15-20%
- **order_ranges**: ~25-35%
- **distinct_ranges**: ~30-40%

---

## 实施原则

1. **最小侵入**: 复用现有V1.2架构，新增代码集中在 `ps_point_plan_cache.cc`
2. **快速bypass**: 不满足条件的语句在classify阶段快速拒绝（NEVER状态）
3. **安全降级**: runtime失败立即fallback到正常优化器，不缓存错误路径
4. **内存安全**: arena缓存在PS m_arena上，invalidation正确清理
5. **分层实施**: 按复杂度递增顺序实施，每个Phase独立可验证

---

## 关键文件

### 核心文件
- `sql/ps_point_plan_cache.h` - 模板结构定义
- `sql/ps_point_plan_cache.cc` - 主要实现逻辑
- `sql/sql_prepare.h` - Prepared_statement 状态管理

### 优化器相关（参考，不修改）
- `sql/item_cmpfunc.cc` - Item_func_between
- `sql/item_sum.h/cc` - Item_sum 聚合函数
- `sql/range_optimizer/range_analysis.cc` - range 树构建
- `sql/filesort.h/cc` - Filesort
- `sql/iterators/composite_iterators.h` - AggregateIterator, RemoveDuplicatesIterator
- `sql/join_optimizer/access_path.h` - AccessPath 类型定义

---

## Phase 7: simple_ranges 支持

### 目标SQL
```sql
SELECT c FROM sbtest WHERE id BETWEEN ? AND ?
```

### 设计方案

#### 1. WHERE shape 提取扩展

在 `ps_point_plan_extract_where_shape()` 中添加 BETWEEN 识别：

```cpp
// 检查 BETWEEN 形状
if (where->type() == Item::FUNC_ITEM) {
  Item_func *func = down_cast<Item_func *>(where);
  if (func->functype() == Item_func::BETWEEN) {
    // 验证参数顺序: field BETWEEN param AND param
    Item *a = func->arguments()[0];  // field
    Item *b = func->arguments()[1];  // low param
    Item *c = func->arguments()[2];  // high param

    if (a->type() == Item::FIELD_ITEM &&
        b->type() == Item::PARAM_ITEM &&
        c->type() == Item::PARAM_ITEM &&
        down_cast<Item_field*>(a)->table_ref == tbl) {
      tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN;
      tpl->params[0] = down_cast<Item_param*>(b);
      tpl->params[1] = down_cast<Item_param*>(c);
      tpl->field_indices[0] = down_cast<Item_field*>(a)->field_index;
      tpl->param_count = 2;
      return true;
    }
  }
}
```

#### 2. Admission 条件扩展

在 `ps_point_plan_can_admit()` 中添加 range 扫描支持：

```cpp
if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN) {
  // 检查访问类型
  if (tab->type() != JT_INDEX_RANGE_SCAN &&
      tab->type() != JT_RANGE) {
    return false;
  }

  // 必须使用主键
  const KEY *keyinfo = &table->key_info[tab->ref().key];
  if (!(actual_key_flags(keyinfo) & HA_NOSAME)) {
    return false;
  }

  // 单列范围
  if (keyinfo->user_defined_key_parts != 1) {
    return false;
  }

  // 验证range条件
  // ... 检查 QUICK_RANGE 结构
}
```

#### 3. Fast path 构建

```cpp
if (tpl.plan_type == PsCachedPlanType::RANGE_PK_BETWEEN) {
  // 构建范围边界
  key_range min_key, max_key;
  build_key_range_from_params(params[0], params[1], &min_key, &max_key);

  // 构建 INDEX_RANGE_SCAN AccessPath
  AccessPath *path = NewIndexRangeScanAccessPath(
      thd, table, tpl.keyno,
      &min_key, &max_key,
      /*count_examined_rows=*/true);

  // 设置成本估算
  path->num_output_rows = tpl.best_rowcount;
  path->cost = tpl.best_read;
}
```

#### 4. 异常场景处理

| 场景 | 处理方式 |
|------|---------|
| low > high | 运行时fallback（让优化器处理） |
| 参数为NULL | 运行时fallback |
| 空范围 | 运行时fallback |
| 参数类型变化 | COLD demotion + 重新admission |

---

## Phase 8: sum_ranges 支持

### 目标SQL
```sql
SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?
```

### 设计方案

#### 1. Classify gate 放宽

```cpp
// 当前: 拒绝所有聚合
if (qb->is_grouped()) return false;

// 放宽后: 允许无GROUP BY的聚合
if (qb->is_grouped() && qb->group_list.elements > 0) {
  return false;  // 拒绝显式GROUP BY
}

// 允许隐式分组（无GROUP BY的聚合）
if (qb->sum_func_count > 0 && qb->group_list.elements == 0) {
  tpl->has_aggregate = true;
  // 检查聚合类型: 只允许SUM/COUNT/MIN/MAX单一聚合
  if (!validate_simple_aggregate(qb, tpl)) return false;
}
```

#### 2. 模板扩展

```cpp
struct PsPointPlanTemplate {
  // ... 现有字段

  /// 聚合相关
  bool has_aggregate{false};
  Item_sum::Sumfunctype aggregate_type{Item_sum::Sumfunctype::SUM_FUNC};
  uint aggregate_field_index{MAX_KEY};
  enum_field_types aggregate_field_type{MYSQL_TYPE_INVALID};
};
```

#### 3. Fast path 构建

```cpp
// 构建两层: AGGREGATE → RANGE_SCAN
AccessPath *range_path = build_range_scan_path(...);

AccessPath *agg_path = new (thd->mem_root) AccessPath;
agg_path->type = AccessPath::AGGREGATE;
agg_path->aggregate().child = range_path;
agg_path->aggregate().rollup = false;

// 设置聚合函数
agg_path->aggregate().sum_funcs = build_cached_sum_funcs(tpl);
```

#### 4. 异常场景处理

| 场景 | 处理方式 |
|------|---------|
| 空集SUM | 正常处理，返回NULL |
| 聚合溢出 | 运行时fallback |
| 多个聚合函数 | 不支持，classify拒绝 |
| SUM(DISTINCT) | 不支持，classify拒绝 |

---

## Phase 9: order_ranges 支持

### 目标SQL
```sql
SELECT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c
```

### 设计方案

#### 1. Classify gate 放宽

```cpp
// 允许简单ORDER BY
if (qb->is_ordered()) {
  if (!validate_simple_order_by(qb, tpl)) {
    return false;  // 复杂ORDER BY不支持
  }
  tpl->has_order_by = true;
}
```

`validate_simple_order_by()` 检查：
- 单列排序
- 排序字段是物理列（非表达式）
- 不支持 `ORDER BY NULL` 或其他特殊情况

#### 2. 模板扩展

```cpp
struct PsPointPlanTemplate {
  // ... 现有字段

  /// ORDER BY 相关
  bool has_order_by{false};
  bool requires_filesort{true};  // 默认需要filesort
  uint order_field_index{MAX_KEY};
  bool order_direction{true};  // ASC=true, DESC=false
  enum_field_types order_field_type{MYSQL_TYPE_INVALID};
  const CHARSET_INFO *order_collation{nullptr};
};
```

#### 3. Admission 时决策

```cpp
if (tpl.has_order_by) {
  // 检查是否可跳过排序（使用索引）
  if (test_if_skip_sort_order(tab, order, ...)) {
    tpl->requires_filesort = false;
    tpl->sort_index_no = best_idx;
  } else {
    tpl->requires_filesort = true;

    // 创建缓存的Filesort对象
    Filesort *filesort = new (stmt->m_arena) Filesort(...);
    tpl->cached_filesort = filesort;
  }
}
```

#### 4. Fast path 构建

```cpp
if (tpl.requires_filesort) {
  // 构建 SORT → RANGE_SCAN
  AccessPath *range_path = build_range_scan_path(...);

  AccessPath *sort_path = NewSortAccessPath(
      thd, range_path,
      tpl.cached_filesort,
      /*order=*/nullptr,  // 已在filesort中
      /*limit=*/HA_POS_ERROR,
      /*remove_duplicates=*/false);

  return sort_path;
} else {
  // 使用索引排序，只需RANGE_SCAN
  return build_range_scan_path(...);
}
```

#### 5. 异常场景处理

| 场景 | 处理方式 |
|------|---------|
| 排序列类型变化 | COLD demotion |
| 字符集变化 | COLD demotion |
| sort_buffer_size过小 | 运行时fallback |
| 磁盘临时文件失败 | 运行时fallback |

---

## Phase 10: distinct_ranges 支持

### 目标SQL
```sql
SELECT DISTINCT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c
```

### 设计方案

#### 1. Classify gate 放宽

```cpp
// 允许DISTINCT（需配合ORDER BY）
if (qb->is_distinct()) {
  if (!qb->is_ordered()) {
    return false;  // DISTINCT必须配合ORDER BY
  }
  if (!validate_distinct_with_order(qb, tpl)) {
    return false;
  }
  tpl->has_distinct = true;
}
```

`validate_distinct_with_order()` 检查：
- DISTINCT列与ORDER BY列相同
- 单列DISTINCT

#### 2. 模板扩展

```cpp
struct PsPointPlanTemplate {
  // ... 现有字段

  /// DISTINCT 相关
  bool has_distinct{false};
  bool distinct_same_as_order{false};  // DISTINCT列=ORDER BY列
};
```

#### 3. Fast path 构建

```cpp
// 构建三层: REMOVE_DUPLICATES → SORT → RANGE_SCAN
AccessPath *range_path = build_range_scan_path(...);
AccessPath *sort_path = NewSortAccessPath(thd, range_path, ...);

AccessPath *distinct_path = new (thd->mem_root) AccessPath;
distinct_path->type = AccessPath::REMOVE_DUPLICATES;
distinct_path->remove_duplicates().child = sort_path;
distinct_path->remove_duplicates().items = &tpl.distinct_item;
distinct_path->remove_duplicates().items_size = 1;

return distinct_path;
```

#### 4. 异常场景处理

| 场景 | 处理方式 |
|------|---------|
| DISTINCT列与ORDER BY列不一致 | classify拒绝 |
| BLOB/TEXT类型DISTINCT | 运行时fallback |
| 临时表创建失败 | 运行时fallback |

---

## 通用保障机制

### 1. Runtime Guard 扩展

```cpp
bool ps_point_plan_runtime_guard(THD *thd, Prepared_statement *stmt,
                                 TABLE **table_out, KEY **keyinfo_out) {
  // ... 现有guard

  // Phase 7+: BETWEEN参数guard
  if (tpl.param_count == 2) {
    for (uint i = 0; i < 2; i++) {
      if (tpl.params[i]->param_state() == Item_param::NULL_VALUE) {
        return false;  // NULL参数fallback
      }
      if (tpl.params[i]->data_type_actual() != tpl.actual_types[i]) {
        ps_point_plan_demote_to_cold(stmt);
        return false;
      }
    }
  }

  // Phase 8+: 聚合guard
  if (tpl.has_aggregate) {
    // 检查聚合字段类型未变化
  }

  // Phase 9+: ORDER BY guard
  if (tpl.has_order_by) {
    // 检查排序列collation未变化
  }

  // Phase 10+: DISTINCT guard
  if (tpl.has_distinct) {
    // 检查DISTINCT列类型未变化
  }
}
```

### 2. 内存管理

```cpp
// Arena缓存在PS m_arena上，invalidation时清理
void ps_point_plan_clear_hot_metadata(PsPointPlanTemplate *tpl) {
  // ... 现有清理

  // Phase 9+: Filesort清理
  if (tpl->cached_filesort) {
    destroy(tpl->cached_filesort);
    tpl->cached_filesort = nullptr;
  }
}
```

### 3. Status Counters 扩展

```sql
-- 新增状态变量
SHOW STATUS LIKE 'Ps_point_plan_cache_range_hits';
SHOW STATUS LIKE 'Ps_point_plan_cache_aggregate_hits';
SHOW STATUS LIKE 'Ps_point_plan_cache_order_by_hits';
SHOW STATUS LIKE 'Ps_point_plan_cache_distinct_hits';
SHOW STATUS LIKE 'Ps_point_plan_cache_fallback_type_mismatch';
```

---

## 验证测试

### 单元测试

```sql
-- Phase 7 测试
PREPARE stmt1 FROM 'SELECT c FROM sbtest WHERE id BETWEEN ? AND ?';
EXECUTE stmt1 USING 1, 100;
EXECUTE stmt1 USING 101, 200;  -- 应命中cache

-- Phase 8 测试
PREPARE stmt2 FROM 'SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?';
EXECUTE stmt2 USING 1, 100;
EXECUTE stmt2 USING 101, 200;  -- 应命中cache

-- Phase 9 测试
PREPARE stmt3 FROM 'SELECT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c';
EXECUTE stmt3 USING 1, 100;
EXECUTE stmt3 USING 101, 200;  -- 应命中cache

-- Phase 10 测试
PREPARE stmt4 FROM 'SELECT DISTINCT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c';
EXECUTE stmt4 USING 1, 100;
EXECUTE stmt4 USING 101, 200;  -- 应命中cache
```

### 异常测试

```sql
-- NULL参数测试
EXECUTE stmt1 USING NULL, 100;  -- 应fallback

-- 类型变化测试
SET @v = CAST('1' AS CHAR);
EXECUTE stmt1 USING @v, 100;  -- 可能fallback

-- DDL失效测试
ALTER TABLE sbtest MODIFY COLUMN c VARCHAR(200);
EXECUTE stmt1 USING 1, 100;  -- 应invalidate
```

### 性能验证

```bash
# 运行完整oltp_read_only测试
sysbench oltp_read_only.lua \
  --mysql-host=localhost \
  --mysql-port=3306 \
  --mysql-db=test \
  --mysql-user=root \
  --tables=1 \
  --table-size=1000000 \
  --threads=8 \
  --time=60 \
  --rate=0 \
  run

# 预期: 100% cache命中率，QPS提升30-50%
```

---

## 实施顺序

1. **Phase 7** (simple_ranges) - 1-2周
   - BETWEEN识别
   - range scan admission
   - INDEX_RANGE_SCAN fast path

2. **Phase 8** (sum_ranges) - 1-2周
   - 聚合classify放宽
   - AGGREGATE AccessPath构建

3. **Phase 9** (order_ranges) - 2-3周
   - ORDER BY classify放宽
   - Filesort缓存
   - SORT AccessPath构建

4. **Phase 10** (distinct_ranges) - 1-2周
   - DISTINCT classify放宽
   - 去重逻辑集成

**总计**: 5-9周

---

## 风险与缓解

| 风险 | 缓解措施 |
|------|---------|
| Range边界计算错误 | 严格单元测试，边界值验证 |
| Filesort内存泄漏 | 使用arena分配，严格cleanup |
| 聚合溢出处理 | Runtime fallback机制 |
| DISTINCT语义不一致 | 复用优化器重写逻辑 |
| 性能回退 | 每Phase独立benchmark |

---

## 成功标准

1. **功能**: 16/16条SQL全部命中cache
2. **性能**: oltp_read_only QPS提升30-50%
3. **稳定性**: 无内存泄漏，无crash
4. **可观测性**: Status counters准确反映命中/失效情况
