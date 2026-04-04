# PS Plan Cache V2 深度缓存分析：缓存更多 Plan 树对象以获得更显著性能提升

## 1. 文档目的

本文基于 V1 的实现经验和对 MySQL 内核 plan 树各层对象生命周期的深入分析，系统性地讨论：

- 当前 V1 的性能收益来源与局限
- 缓存更多 plan 树对象的可行性、方案与风险
- 覆盖更多查询类型（尤其是 sysbench `oltp_read_only` 中的 range 查询）
- 投入产出矩阵与推荐优先级

本文是 V2 的前期技术分析文稿，不是最终设计。

> **与当前代码的关系：** 本文写作时 `PsPointPlanTemplate` 使用单参数字段。
> Phase 0 已根据本文分析完成了模板结构的数组化泛化（`params[]`、`field_indices[]` 等），
> 具体结构见 `sql/ps_point_plan_cache.h`。本文中涉及旧版单字段结构的描述已标注为历史记录。
> V1 的功能范围仅覆盖 Phase 0-5（单表唯一键等值点查），
> 本文讨论的 range/aggregate 扩展属于独立的后续工作。

## 2. V1 的性能收益来源与局限

### 2.1 V1 跳过了什么

在 `JOIN::optimize()` (`sql/sql_optimizer.cc` 337-695 行) 中，对于单表点查，V1 的 fast path 跳过了：

- `optimize_cond()` — WHERE 条件化简
- `prune_table_partitions()` — 分区裁剪
- `optimize_aggregated_query()` — 聚合优化尝试
- 整个 `make_join_plan()` (`sql/sql_optimizer.cc` 5308-5424 行)：
  - `init_planner_arrays()` — 数组初始化
  - `update_ref_and_keys()` — keyuse / sargable 分析
  - `extract_const_tables()` — 常量表识别
  - `estimate_rowcount()` → `test_quick_select()` — range 优化器分析
  - `choose_table_order()` → `greedy_search()` — join 顺序搜索
  - `get_best_combination()` — QEP 生成
- `create_access_paths()` — QEP_TAB → AccessPath 转换

### 2.2 V1 每次执行仍然必须做的

| 阶段 | 操作 | 是否可进一步优化 |
|------|------|-----------------|
| `open_tables()` | MDL 获取 + TABLE 绑定 | 需要大改 |
| `lock_tables()` | 行级锁初始化 | 需要大改 |
| `LEX::clear_execution()` | 重置执行状态 | 不可避免 |
| `restore_cmd_properties()` | 恢复 Table_ref → TABLE 属性 | 不可避免 |
| `new JOIN(thd, this)` | 分配 fresh JOIN 对象 | 可优化 |
| `alloc_indirection_slices()` | ref_items 分配 | 可优化 |
| `new QEP_TAB[2]` + `init_ref()` | QEP 构造 + Index_lookup | 可优化 |
| `NewEQRefAccessPath()` | AccessPath 节点创建 | 可优化 |
| `CreateIteratorFromAccessPath()` | Iterator 树构建 | 可优化 |
| `Iterator::Init()` → `ha_index_init()` | handler 索引初始化 | 不可避免 |

### 2.3 V1 的查询覆盖局限

V1 仅覆盖 `SELECT ... WHERE pk/uk = ?` 的单列唯一键点查。

对于 `oltp_read_only`，每个事务包含 14 条 SQL：

| 类型 | 语句模式 | 数量 | V1 是否覆盖 |
|------|---------|------|-----------|
| point select | `SELECT c FROM sbtest WHERE id=?` | 10 | 是 |
| simple range | `SELECT c FROM sbtest WHERE id BETWEEN ? AND ?` | 1 | 否 |
| sum range | `SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?` | 1 | 否 |
| order range | `SELECT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c` | 1 | 否 |
| distinct range | `SELECT DISTINCT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c` | 1 | 否 |

V1 仅覆盖 14 条中的 10 条，对其余 4 条 range 查询无加速效果。

## 3. Plan 树各层对象的生命周期分析

### 3.1 四层对象模型

```
Layer 1: 逻辑层 (LEX / Query_block / Item)
  生存期: PS 的 m_arena，跨执行稳定
  
Layer 2: 物理计划层 (JOIN / JOIN_TAB / QEP_TAB / POSITION)
  生存期: JOIN::optimize() → JOIN::destroy()，单次执行内
  分配在: thd->mem_root，dispatch_command 结束时回收

Layer 3: 统一计划层 (AccessPath 树)
  生存期: optimize 完成 → cleanup(true)，单次执行内
  分配在: thd->mem_root (trivially destructible)

Layer 4: 运行时层 (RowIterator 树)
  生存期: CreateIteratorFromAccessPath() → m_root_iterator.reset()
  分配在: thd->mem_root
```

### 3.2 关键生命周期约束

**TABLE\* 每次执行都会变化：**

```
Table_ref::reset()          →  table = nullptr    (sql/table.cc 4427)
open_tables() → open_table  →  table_list->table = table  (sql/sql_base.cc 3466)
close_thread_tables()       →  关闭表引用         (sql/sql_parse.cc 4959)
```

`TABLE*` 在每次 PS 执行时被重新绑定。虽然通常从 thread table cache 获取同一个 `TABLE` 实例，但指针值不保证稳定。

**JOIN 必须为 nullptr：**

```cpp
// sql/sql_lex.cc line 4829
void Query_block::restore_cmd_properties() {
  // ...
  assert(join == nullptr);  // 每次执行前的 hard constraint
  // ...
}
```

这意味着不可能在多次执行间直接复用旧 `JOIN*`。

**thd->mem_root 在 dispatch_command 结束时回收：**

```cpp
// sql/sql_parse.cc 2508-2526
// 每次命令处理结束时：
thd->mem_root->ClearForReuse();  // 或 Clear()
```

所有分配在 `thd->mem_root` 上的对象（JOIN、QEP_TAB、AccessPath、Iterator）在命令结束后全部失效。

**Iterator 在 cleanup(true) 时销毁：**

```cpp
// sql/sql_lex.h 879-882
void Query_expression::clear_root_access_path() {
  m_root_access_path = nullptr;
  m_root_iterator.reset();  // 销毁 iterator 树
}

// sql/sql_union.cc 1853-1854
// cleanup(full=true) 调用 clear_root_access_path()
```

### 3.3 跨执行稳定的对象

以下对象分配在 PS 的 `m_arena.mem_root` 上，跨执行稳定：

| 对象 | 位置 | 稳定性 |
|------|------|--------|
| `Table_ref*` | LEX 解析树 | 稳定，结构不变 |
| `Item_param*` | LEX 解析树 | 稳定，每次 execute 只更新值 |
| `Item_field*` | LEX 解析树 | 稳定，但 `field` 指针需要 re-bind |
| `Query_block*` | LEX 解析树 | 稳定 |

### 3.4 Handler 状态

`handler` 对象（`TABLE::file`）在 `open_table()` 时创建或从 cache 获取。`handler::inited` 字段标记当前扫描状态：

```
ha_index_init()     → inited = INDEX
ha_index_end()      → inited = NONE
ha_rnd_init()       → inited = RND
ha_rnd_end()        → inited = NONE
```

`JOIN::cleanup()` / `qs_cleanup()` 会调用 `ha_index_or_rnd_end()` 确保 handler 回到 NONE 状态。这意味着即使缓存了 Iterator，每次执行仍需调用 `ha_index_init()`。

## 4. 三个优化维度

### 4.1 维度 A：扩展查询覆盖范围

让更多查询类型（尤其是 range 查询）进入 fast path。

### 4.2 维度 B：减少 per-execution 构建开销

在 fast path 内部，缓存更多执行对象（Index_lookup、AccessPath、Iterator），减少每次重建的成本。

### 4.3 维度 C：优化 open/lock 路径

减少每次 PS 执行时 `open_tables()` / `lock_tables()` / `close_thread_tables()` 的开销。

## 5. 维度 A 详细方案：扩展查询覆盖范围

### 5.1 A1: 支持 PK BETWEEN range scan

**目标查询：** `SELECT c FROM sbtest WHERE id BETWEEN ? AND ?`

**V1 不支持的原因：**

- WHERE 不是 `field = ?`，而是 `field BETWEEN ? AND ?`
- 优化器走 `test_quick_select()` → `INDEX_RANGE_SCAN` 路径，不是 `JT_EQ_REF`
- range 边界依赖参数值

**PsPointPlanTemplate 扩展：**

> **注：** Phase 0 已经完成了模板结构的数组化泛化。当前代码使用 `params[]`、
> `field_indices[]`、`actual_types[]` 数组和 `PsCachedPlanType` 枚举，
> 而非下方所描述的旧版单字段 + V2 追加字段的方案。
> 实际结构见 `sql/ps_point_plan_cache.h`。以下旧版描述作为分析记录保留。

```cpp
// ====== 旧版分析方案（已被数组化实现替代）======
enum class PsCachedPlanType : uchar {
  POINT_EQ_REF = 0,
  RANGE_PK_BETWEEN,
};

struct PsPointPlanTemplate {
  // --- V1 existing fields (旧版) ---
  Table_ref *table_ref{nullptr};
  Item_param *param{nullptr};
  uint field_index{UINT_MAX};
  uint keyno{MAX_KEY};
  uint key_length{0};
  key_part_map null_rejecting{0};
  double best_read{0.0};
  double best_rowcount{1.0};
  enum_field_types actual_type{MYSQL_TYPE_INVALID};
  bool unsigned_actual{false};

  // --- V2 additions (旧版方案，已由数组化替代) ---
  PsCachedPlanType plan_type{PsCachedPlanType::POINT_EQ_REF};

  // range 专用字段
  Item_param *param_low{nullptr};
  Item_param *param_high{nullptr};
  enum_field_types actual_type_low{MYSQL_TYPE_INVALID};
  enum_field_types actual_type_high{MYSQL_TYPE_INVALID};
  bool unsigned_actual_low{false};
  bool unsigned_actual_high{false};
};
```

**静态分类扩展：**

在 `ps_point_plan_classify()` 中增加对 BETWEEN 的识别：

```cpp
// WHERE 识别逻辑：
// Case 1 (V1): Item_func_eq(Item_field, Item_param)  → POINT_EQ_REF candidate
// Case 2 (V2): Item_func_between(Item_field, Item_param, Item_param) → RANGE candidate

bool ps_point_plan_extract_between(Query_block *qb, PsPointPlanTemplate *tpl) {
  Item *where = qb->where_cond();
  if (where == nullptr || where->type() != Item::FUNC_ITEM) return false;
  
  auto *func = down_cast<Item_func *>(where);
  if (func->functype() != Item_func::BETWEEN) return false;
  
  // BETWEEN 有 3 个参数: field, low, high
  Item *field_item = func->arguments()[0];
  Item *low_item   = func->arguments()[1];
  Item *high_item  = func->arguments()[2];
  
  // 验证 field 是 Item_field，low/high 是 Item_param
  // 验证 field 属于 leaf table
  // 记录 field_index, param_low, param_high
  // ...
  
  tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN;
  return true;
}
```

**Admission 条件（普通优化后验证）：**

- `JOIN::tables == 1`, `primary_tables == 1`
- 优化器选择了 `INDEX_RANGE_SCAN` 或 `JT_RANGE` 且用的是 PK
- `actual_key_flags(keyinfo) & HA_NOSAME`（唯一索引）
- key 只有一个 part

**Fast path 构建：**

```cpp
bool JOIN::build_pk_range_for_ps_cache(const PsPointPlanTemplate &tpl) {
  TABLE *table = tpl.table_ref->table;
  KEY *keyinfo = &table->key_info[tpl.keyno];

  // 从 Item_param 构建 key_range
  // min_key: param_low 的值序列化
  // max_key: param_high 的值序列化
  // flag: HA_READ_KEY_EXACT (closed range)

  // 构造 AccessPath (INDEX_RANGE_SCAN)
  // 调用 NewIndexRangeScanAccessPath() 或等效构造

  m_root_access_path = path;
  return false;
}
```

**关键技术挑战：**

1. `INDEX_RANGE_SCAN` 的 AccessPath 构造比 `EQ_REF` 复杂，涉及 `QUICK_RANGE` / `Bounds_checked_array<QUICK_RANGE*>` 的分配
2. 需要理解 `get_ranges_from_tree()` 的输出格式并手动构造
3. 行数估算：可以用 `handler::records_in_range()` 做快速估算，或直接使用 template 中缓存的值

**风险等级：中等。** range scan 的变数比 EQ_REF 多，但在 "单表 PK BETWEEN" 场景下，优化策略是确定性的——PK range scan 始终是最优解。

### 5.2 A2: 支持带聚合的 PK range (SUM/COUNT)

**目标查询：** `SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?`

**额外复杂度：**

- 存在聚合函数但无 `GROUP BY`（`is_grouped()` 为 false）
- 聚合在 iterator 层通过 `AggregateIterator` 处理
- AccessPath 树变为: `AGGREGATE → INDEX_RANGE_SCAN`

**分类条件调整：**

- 允许 select list 中存在单一聚合函数（SUM/COUNT/MIN/MAX）
- 仍要求无 GROUP BY、无 HAVING、无子查询
- 聚合列必须属于 leaf table

**模板扩展：**

```cpp
struct PsPointPlanTemplate {
  // ...
  bool has_aggregate{false};
  // 聚合类型不需要记录——optimizer 的 AggregateIterator
  // 由 AccessPath 自动处理，只要 base path 正确即可
};
```

**fast path 构建需要构建两层 AccessPath：**

```
AGGREGATE
  └── INDEX_RANGE_SCAN (PK BETWEEN ?, ?)
```

这需要调用 `NewAggregateAccessPath()` 包裹底层的 range scan path。

**风险等级：中等偏高。** 需要确保 AGGREGATE AccessPath 的构造与 iterator 框架正确对接。

### 5.3 A3: 支持 ORDER BY

**目标查询：** `SELECT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c`

**额外复杂度：**

- ORDER BY 可能需要 filesort，也可能用索引消除 sort（`test_skip_sort()`）
- 如果 ORDER BY 列有索引覆盖且与 range scan 方向一致，可以避免 sort
- 否则需要构建 `SORT → INDEX_RANGE_SCAN` 的 AccessPath 树

**关键决策点：**

对于 sysbench 的 `ORDER BY c`，`c` 列通常没有适合的索引（sbtest 表的 PK 是 id），所以通常需要 filesort。这意味着 fast path 需要：

1. 构建 `Filesort` 对象
2. 构建 `SORT` AccessPath 包裹 range scan

**Filesort 的生命周期：** `Filesort` 对象在 `JOIN::optimize()` 中创建，在 `JOIN::destroy()` 中通过 `Temp_table_param` 清理。需要在 fast path 中正确构造。

**风险等级：高。** filesort 的构造涉及 temp table 决策、sort buffer 管理等，是 `JOIN::optimize()` 后半段最复杂的部分之一。

### 5.4 A4: 支持 DISTINCT + ORDER BY

**目标查询：** `SELECT DISTINCT c FROM sbtest WHERE id BETWEEN ? AND ? ORDER BY c`

在 A3 的基础上增加 `REMOVE_DUPLICATES` AccessPath 节点。如果 A3 完成，A4 的增量工作量较小。

**AccessPath 树：**

```
REMOVE_DUPLICATES
  └── SORT
        └── INDEX_RANGE_SCAN (PK BETWEEN ?, ?)
```

**风险等级：高（依赖 A3 完成）。**

### 5.5 A5: 支持复合唯一键（多列 PK/UK）

**目标查询：** `SELECT ... FROM t WHERE pk_col1 = ? AND pk_col2 = ?`

**相比 V1 的增量变化：**

- `param_count` 从 `== 1` 放宽到 `>= 1`
- WHERE 识别从单个 `Item_func_eq` 扩展到 `Item_cond_and` 包裹的多个 `Item_func_eq`
- 模板需要记录多个 `Item_param*` 和对应的 key part 映射
- `init_ref()` 的 `keyparts` 参数从 1 变为 N

**模板扩展：**

```cpp
struct PsPointPlanTemplate {
  // ...
  static constexpr uint MAX_CACHED_KEY_PARTS = 4;
  uint key_parts{0};
  Item_param *params[MAX_CACHED_KEY_PARTS]{};
  uint field_indices[MAX_CACHED_KEY_PARTS]{};
  enum_field_types actual_types[MAX_CACHED_KEY_PARTS]{};
  bool unsigned_actuals[MAX_CACHED_KEY_PARTS]{};
};
```

**风险等级：低。** 这是 V1 的自然扩展，逻辑结构一致，只是从单 key part 推广到多 key parts。

## 6. 维度 B 详细方案：缓存执行对象

### 6.1 B1: 缓存 Index_lookup

**当前状态：** 每次执行调用 `init_ref()` + `init_ref_part()` 在 `thd->mem_root` 上分配 Index_lookup 的 key buffer 和 store_key 数组。

**分析：**

```
Index_lookup 包含:
  key_buff / key_buff2  — 分配在 thd->mem_root 上的 key buffer
  key_length, key_parts — 常量
  items[]               — 指向 Item_param (在 m_arena 上，跨执行稳定)
  store_key[]           — 用于把 Item 值序列化到 key_buff
```

**方案：**

在首次 admission 后，将 Index_lookup 的 key buffer 和 store_key 分配在 PS 的 `m_arena.mem_root` 上，跨执行复用。

```cpp
void ps_point_plan_cache_ref(Prepared_statement *stmt, JOIN *join) {
  MEM_ROOT *arena = &stmt->m_arena.mem_root;
  auto &tpl = stmt->ps_point_plan_template();

  // 在 PS arena 上分配 key buffer
  tpl.cached_key_buff = (uchar *)arena->Alloc(tpl.key_length);
  tpl.cached_key_buff2 = (uchar *)arena->Alloc(tpl.key_length);

  // 在 PS arena 上构建 store_key
  // store_key 引用 Item_param (也在 arena 上) → 指针稳定
  // ...
}
```

**每次执行时：**

```cpp
// 不调 init_ref()，直接使用缓存的 ref
// 只需调用 store_key->copy() 把最新参数值写入 key buffer
```

**需要修改的代码：**

- `sql/sql_select.h` 中 `init_ref()` 和 `init_ref_part()` 需要接受外部 `MEM_ROOT*`
- 或者新增一个 `restore_ref_from_template()` 函数

**预期收益：** 省去 `init_ref()` 的内存分配和 `init_ref_part()` 的 store_key 构造。

**风险等级：低。** Index_lookup 的依赖关系简单，Item_param 指针在 arena 上稳定。

### 6.2 B2: 缓存 AccessPath

**当前状态：** 每次执行通过 `NewEQRefAccessPath()` 或类似函数创建新的 AccessPath 节点。

**分析：**

```
AccessPath (EQ_REF variant):
  type = EQ_REF               → 常量
  TABLE *table                → 每次执行可能不同
  Index_lookup *ref           → 如果 B1 缓存了可以复用
  cost, num_output_rows       → 常量 (来自 template)
```

**方案：**

- 在 PS arena 上缓存 AccessPath 结构
- 每次执行只 re-patch `TABLE*` 指针

```cpp
struct PsPointPlanTemplate {
  // ...
  AccessPath *cached_access_path{nullptr};
};

// 首次 admission 后
void cache_access_path(Prepared_statement *stmt, AccessPath *path) {
  MEM_ROOT *arena = &stmt->m_arena.mem_root;
  auto &tpl = stmt->ps_point_plan_template();

  // 深拷贝 AccessPath 到 arena (AccessPath 是 trivially copyable)
  tpl.cached_access_path = new (arena) AccessPath(*path);
}

// 每次 fast path 执行时
void reuse_access_path(PsPointPlanTemplate &tpl, TABLE *table) {
  // re-patch TABLE 指针
  tpl.cached_access_path->eq_ref().table = table;
  // re-patch Index_lookup 指针 (如果 B1 已缓存)
  tpl.cached_access_path->eq_ref().ref = tpl.cached_ref;
}
```

**预期收益：** 省去 AccessPath 节点的创建。对于单节点树收益有限，对于多层树（SORT → AGGREGATE → RANGE_SCAN）收益更明显。

**风险等级：低。** AccessPath 是 trivially destructible 的 tagged union，拷贝和 re-patch 都是安全操作。但需要注意：如果缓存的 AccessPath 引用了其他 AccessPath 子节点，需要递归缓存整棵树。

### 6.3 B3: 缓存 Iterator 树

**当前状态：** `CreateIteratorFromAccessPath()` 遍历 AccessPath 树，在 `thd->mem_root` 上分配 Iterator 对象。

**分析：**

```
EQRefIterator:
  Index_lookup *m_ref      → 如果 B1 缓存了可以复用
  TABLE *table()           → 每次执行需要更新
  THD *m_thd               → 同一连接内不变
  m_first_record_since_init → Init() 时重置
  m_examined_rows          → 指向 JOIN 的计数器 (每次执行新 JOIN)
```

**核心障碍：**

1. `thd->mem_root` 在 `dispatch_command` 结束时回收 → Iterator 失效
2. `m_root_iterator.reset()` 在 `cleanup(true)` 时销毁 Iterator
3. `m_examined_rows` 指向当前 JOIN 的计数器，JOIN 每次执行重建

**方案：**

- Iterator 分配在 PS arena 上
- 修改 `Query_expression::cleanup()` 对缓存 Iterator 的处理
- 每次执行时 re-patch TABLE* 和 examined_rows 指针

**需要修改的核心代码路径：**

```cpp
// sql/sql_union.cc — Query_expression::cleanup()
// 需要在 full cleanup 时跳过对缓存 iterator 的销毁

// sql/join_optimizer/access_path.cc — CreateIteratorFromAccessPath()
// 需要支持传入外部 MEM_ROOT

// sql/iterators/ref_row_iterators.h — EQRefIterator
// 需要增加 re-patch 接口
```

**预期收益：** 省去 `CreateIteratorFromAccessPath()` 的遍历和分配。对于单节点 Iterator 收益有限。

**风险等级：高。** 修改 cleanup 路径影响面广，需要确保所有 execution 结束路径都正确处理缓存 Iterator。

### 6.4 B4: 在 execute_inner() 层截断

**当前状态：** PS 每次执行都走 `Sql_cmd_dml::execute_inner()` → `Query_expression::optimize()` → `Query_block::optimize()` → `JOIN::optimize()`。

**方案：**

在 `Sql_cmd_dml::execute_inner()` 层面直接截断，跳过整个 `optimize()` 调用链：

```cpp
bool Sql_cmd_dml::execute_inner(THD *thd) {
  Query_expression *unit = lex->unit;

  // V2 ultra-fast path: 完全跳过 optimize
  if (try_ultra_fast_ps_point_plan(thd, unit)) {
    return unit->execute(thd);  // 直接执行
  }

  // 正常路径
  if (unit->optimize(thd, ...)) return true;
  return unit->execute(thd);
}
```

**需要手动管理的状态：**

- `Query_expression::set_optimized()` / `set_executed()`
- `Query_block::join` 的创建和销毁
- `m_root_access_path` 和 `m_root_iterator` 的设置
- `PLAN_READY` 状态

**预期收益：** 最大化跳过——不仅跳过 `make_join_plan()`，还跳过 `JOIN` 对象创建、`alloc_indirection_slices()` 等所有 optimize 前置步骤。

**风险等级：极高。** 绕过 `Query_expression::optimize()` 意味着需要手动管理大量状态。任何遗漏都可能导致 crash 或结果错误。

## 7. 维度 C 详细方案：优化 open/lock 路径

### 7.1 PS 执行的完整路径

```
Prepared_statement::execute()
  → LEX::clear_execution()        // 重置执行状态
    → Table_ref::reset()           // table = nullptr
  → open_tables_for_query()        // MDL acquire + TABLE attach
  → restore_cmd_properties()       // 恢复元数据
  → lock_tables()                  // 行级锁准备
  → Sql_cmd_dml::execute()
    → execute_inner()
      → optimize() + execute()
  → lex->cleanup(true)             // 清理
  → close_thread_tables()          // 关闭表引用
```

每次 PS 执行都经历完整的 open → lock → execute → cleanup → close 循环。

### 7.2 C1: TABLE 缓存 pinning

**思路：** 如果表结构没有变化，在 PS 执行间保持 TABLE* 绑定。

**需要的改动：**

| 改动点 | 内容 |
|--------|------|
| `Table_ref::reset()` | 条件性跳过 `table = nullptr` |
| `open_tables()` | 增加 "already pinned" 快速检查 |
| MDL | 保持 MDL ticket 不释放，只做 version check |
| `close_thread_tables()` | 跳过 pinned TABLE 的关闭 |
| schema version | 增加快速版本比较 |
| Reprepare_observer | 仍需正确触发 DDL 后的 reprepare |

**预期收益：** 省去 `open_tables()` 和 `close_thread_tables()` 的 MDL 轮转和 TABLE cache 查找。

**核心难点：**

- MDL 是防止并发 DDL 的核心机制，完全跳过会破坏 DDL 安全性
- `close_thread_tables()` 是 `mysql_execute_command()` 中 hard-coded 的清理路径
- 需要处理 TABLE 被 invalidate 后的安全回收

**风险等级：极高。** 等同于实现 MySQL 版本的 "session cursor cache"（类似 Oracle `SESSION_CACHED_CURSORS`），影响 MDL、table cache、handler 生命周期等多个子系统。

### 7.3 C2: 轻量级 MDL 快速路径

**思路：** 不完全跳过 `open_tables()`，而是为 plan cache HOT 查询提供一条更轻的 MDL 获取路径。

- 缓存上次 open_table 时获得的 `TABLE_SHARE::get_table_ref_version()`
- 在 execute 开始时，做一个 O(1) 的 version check
- 如果 version 匹配 → 快速获取 shared MDL（可能仍需 `mdl_context.acquire_lock()`，但可以优化为 try_acquire）
- 如果 version 不匹配 → 走正常 open_tables 路径 + invalidate plan cache

**实现方案：**

```cpp
struct PsPointPlanTemplate {
  // ...
  // TABLE pinning 相关
  ulonglong cached_table_version{0};
};

// 在 PS execute 的 open_tables 之前
bool try_fast_open_for_ps_cache(THD *thd, Prepared_statement *stmt) {
  auto &tpl = stmt->ps_point_plan_template();
  if (tpl.cached_table_version == 0) return false;

  Table_ref *tl = thd->lex->query_tables;
  // 尝试快速 MDL 获取
  // 检查 TABLE_SHARE version
  // 如果匹配，直接绑定 TABLE* 并返回 true
  // 否则返回 false，走正常路径
}
```

**风险等级：高。** 比 C1 安全，但仍然涉及 MDL 和 table cache 的核心逻辑。

## 8. 架构约束总结

### 8.1 不可动摇的约束（短期内不应挑战）

| 约束 | 位置 | 原因 |
|------|------|------|
| `assert(join == nullptr)` | `sql/sql_lex.cc:4829` | `restore_cmd_properties()` 的 hard constraint |
| `Table_ref::reset()` 清空 table | `sql/table.cc:4427` | `clear_execution()` 的核心语义 |
| `thd->mem_root` 命令结束时回收 | `sql/sql_parse.cc:2522` | 内存管理基石 |
| `cleanup(true)` 销毁 join/iterator | `sql/sql_union.cc:2081` | 清理链 |
| `ha_index_init()` 每次扫描必须调用 | `sql/handler.cc:2874` | handler 状态管理 |

### 8.2 可安全利用的不变量

| 不变量 | 说明 |
|--------|------|
| `Table_ref*` 在 m_arena 上 | 跨执行稳定，可安全缓存指针 |
| `Item_param*` 在 m_arena 上 | 跨执行稳定，每次 execute 只更新值 |
| `keyno` / `key_length` / `field_index` | 表结构不变时稳定 |
| 同一连接的 `THD*` | 跨执行不变 |
| 单表点查/range 的最优计划 | 确定性的，不依赖其他表统计信息 |
| AccessPath 是 trivially destructible | 可安全拷贝和 re-patch |

## 9. 投入产出矩阵

| 方案 | 开发量 | 覆盖查询增量 | 预期收益 | 风险 |
|------|--------|------------|---------|------|
| A5: 复合唯一键 | 1 周 | 其他 workload | 中等 | 低 |
| A1: PK BETWEEN range | 2-3 周 | +7% (oltp_read_only 1/14) | 确定 | 中 |
| A2: PK BETWEEN + SUM | 1-2 周 | +7% (1/14) | 确定 | 中高 |
| A3: ORDER BY | 2-3 周 | +7% (1/14) | 确定 | 高 |
| A4: DISTINCT + ORDER | 1-2 周 | +7% (1/14) | 确定 | 高 |
| B1: 缓存 Index_lookup | 1-2 周 | 全部 HOT 查询 | 边际 | 低 |
| B2: 缓存 AccessPath | 1 周 | 全部 HOT 查询 | 边际 | 低 |
| B3: 缓存 Iterator | 3-4 周 | 全部 HOT 查询 | 中等 | 高 |
| B4: execute_inner 截断 | 4-6 周 | 全部 HOT 查询 | 显著 | 极高 |
| C1: TABLE pinning | 8-12 周 | 全部 PS 查询 | 显著 | 极高 |
| C2: 轻量 MDL | 4-6 周 | 全部 PS 查询 | 中等 | 高 |

## 10. 推荐实施路径

### Phase V2-1: 扩展覆盖面（低风险优先）

1. A5: 复合唯一键支持
2. B1 + B2: 缓存 Index_lookup + AccessPath（配合 V1 fast path 优化）

**验收：** 复合 PK/UK 点查可以命中 fast path。

### Phase V2-2: 覆盖 range 查询

1. A1: PK BETWEEN range scan
2. A2: PK BETWEEN + SUM 聚合

**验收：** `oltp_read_only` 的 simple_range 和 sum_range 可以命中 fast path。命中覆盖从 10/14 提升到 12/14。

### Phase V2-3: 覆盖 ORDER/DISTINCT（视收益评估）

1. A3: ORDER BY range
2. A4: DISTINCT + ORDER BY range

**验收：** `oltp_read_only` 全部 14 条 SQL 均可命中 fast path（命中率 100%）。

### Phase V2-4: 性能基准与长期评估

1. 完整 sysbench 基准测试
2. 评估 B3/B4/C1/C2 的投入产出
3. 形成 V3 架构设计（如有必要）

## 11. 与业界方案的对比参考

| 系统 | 方案 | 缓存内容 | 共享级别 |
|------|------|---------|---------|
| MySQL V1 (当前) | per-PS single-slot template | 元数据 (keyno, key_length) | per-connection |
| MySQL V2 (本文) | per-PS template + cached objects | 元数据 + Index_lookup + AccessPath | per-connection |
| PostgreSQL | generic plan | 完整 plan tree (PlannedStmt) | per-connection |
| Oracle | shared cursor cache | 完整 cursor + execution plan | cross-session (library cache) |
| SQL Server | plan cache | 完整 compiled plan | cross-session |
| TiDB | prepared plan cache | 物理计划 | per-session |

MySQL 的方案比其他数据库更保守，这是由其架构约束决定的（per-execution TABLE binding、`thd->mem_root` 回收、`assert(join == nullptr)`）。V2 在保持安全性的前提下，通过扩展覆盖范围和缓存更多执行对象，逐步接近业界水平。

## 12. 结论

要获得更显著的性能提升，优先级最高的方向是**扩展查询覆盖范围**（维度 A），而非深度缓存执行对象（维度 B/C）。原因：

1. `oltp_read_only` 中 4/14 的 range 查询完全不被 V1 覆盖，是最直接的收益来源
2. 对于已覆盖的点查，per-execution 构建开销中最大的部分（`open_tables` / `lock_tables`）需要极大改动才能优化
3. 缓存 Index_lookup / AccessPath 的边际收益有限，因为单表简单查询的构建成本本身不高

**推荐路线：先做 A5 + A1 + A2（覆盖复合键和 range），再做 B1 + B2（缓存执行对象），最后评估 A3 + A4 + C1/C2 的必要性。**
