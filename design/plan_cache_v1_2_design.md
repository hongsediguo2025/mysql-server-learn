# Plan Cache V1.2 设计文档：Optimizer Preamble 旁路 + 深度缓存

## 1. 文档定位

本文档记录 V1.2 的设计决策和实现细节。V1.2 是 V1.1 的性能深化迭代，
目标是将 fast path 拦截点从 `JOIN::optimize()` 中部提前到入口处，跳过
全部 optimizer preamble，并将更多每次执行的构建产物缓存到 PS arena，
消除不必要的 per-execution 分配。

**与 V1.1 的关系**：V1.2 不改变状态机、sysvar、admission 判定标准或
runtime guard 逻辑。仅改变 fast path 的拦截时机和构建产物的缓存层级。

**性能目标**：在 V1.1 基础上（8 线程 +6.79%、4 线程 +2.65%）再提升
+4~7%，使 8 线程总提升达到 +10~13%。

## 2. 问题分析

### 2.1 V1.1 fast path 拦截点过晚

V1.1 将 fast path hook 放在 `JOIN::optimize()` 第 703 行，位于
`make_join_plan()` 调用之前。但在 hook 之前，有约 360 行 optimizer
preamble 代码在每次 HOT 执行时完整运行，其产物随后被 fast path 丢弃：

```
JOIN::optimize() 执行序列（line 339-716）:
                                                    每次执行?  fast path 用到?
  ├── Opt_trace 对象构造 (358-362)                       ✓          ✗
  ├── count_field_types() (364)                          ✓          ✗
  ├── alloc_func_list() (377)                            ✓          ✗
  ├── get_optimizable_conditions() (379)                 ✓          ✗  ← 复制 WHERE
  ├── alloc_indirection_slices() (393)                   ✓          ✗
  ├── 派生表优化循环 (403-421)                            ✓          ✗
  ├── row_limit / m_select_limit 计算 (446-465)          ✓          ✗
  ├── optimize_cond() WHERE (474-487)                    ✓          ✗  ← 条件简化
  ├── optimize_cond() HAVING (488-501)                   ✓          ✗
  ├── prune_table_partitions() (503-507)                 ✓          ✗
  ├── optimize_aggregated_query() (515-572)              ✓          ✗
  ├── Window / sort_by_table (586-599)                   ✓          ✗
  ├── substitute_gc() (601-607)                          ✓          ✗
  └── ★ FAST PATH HOOK (703-716)                         ✓          ✓
```

对于点查场景（无聚合、无 GROUP BY、无 ORDER BY、无 HAVING、无派生表），
上述所有步骤的输出要么为空，要么被 fast path 丢弃。

### 2.2 per-execution 分配可进一步消除

V1.1 每次 HOT 执行仍在 `thd->mem_root` 上分配：

| 对象 | 大小 | 次数/execution | 结构是否固定 |
|------|------|---------------|-------------|
| `QEP_TAB[2]` | ~200 bytes | 1 | 是（除 table 指针外） |
| `QEP_shared` | ~100 bytes | 1 | 是（除 table 指针外） |
| `key_copy[]` | 8*key_parts bytes | 1 | 值固定（依赖 null_key） |
| `items[]` | 8*key_parts bytes | 1 | 值固定（= tpl.params[i]） |
| `cond_guards[]` | 8*key_parts bytes | 1 | 全 nullptr |
| `AccessPath` | ~100 bytes | 1 | 是（除 TABLE* 外） |

虽然 `thd->mem_root` 是 bump allocator（分配极快），但对象构造仍有
非零开销，且分配越多，mem_root 的重置和释放成本越高。

## 3. 设计方案

### 3.1 总体架构变更

```
V1.1 执行路径 (HOT hit):
  Sql_cmd_dml::execute
    → open_tables_for_query           [完整执行]
    → check_privileges                [完整执行]
    → lock_tables                     [完整执行]
    → execute_inner
       → JOIN::optimize
          → preamble (360 lines)      [完整执行，产物丢弃] ← 瓶颈
          → fast path hook            [构建最小 plan]
       → executor

V1.2 执行路径 (HOT hit):
  Sql_cmd_dml::execute
    → open_tables_for_query           [完整执行，Phase B 优化]
    → check_privileges                [完整执行]
    → lock_tables                     [完整执行]
    → execute_inner
       → JOIN::optimize
          → ★ fast path hook (入口)   [跳过全部 preamble]
       → executor
```

### 3.2 变更 1：将 fast path hook 移到 JOIN::optimize 入口

**目标**：在 `JOIN::optimize()` 入口处（`if (optimized) return false;`
之后、任何 preamble 代码之前）插入 fast path 检查。

**当前代码** (`sql/sql_optimizer.cc:339-716`):

```cpp
bool JOIN::optimize(bool finalize_access_paths) {
  // ... assertions (346-349)
  if (optimized) return false;                    // line 352

  // --- 以下为 optimizer preamble，V1.2 在 HOT hit 时全部跳过 ---
  THD_STAGE_INFO(thd, stage_optimizing);          // line 356
  Opt_trace_context *const trace = ...;           // line 358-362
  count_field_types(...);                         // line 364
  alloc_func_list();                              // line 377
  get_optimizable_conditions(...);                // line 379
  set_optimized();                                // line 389
  tables_list = query_block->leaf_tables;         // line 391
  alloc_indirection_slices();                     // line 393
  // ... 300+ lines of condition optimization, partition pruning, etc.

  // V1.1 hook (line 703-716)
  if (thd->variables.ps_point_plan_cache) { ... }
```

**V1.2 改造后**:

```cpp
bool JOIN::optimize(bool finalize_access_paths) {
  // ... assertions (346-349)
  if (optimized) return false;

  // V1.2: early fast path — bypass entire optimizer preamble
  if (thd->variables.ps_point_plan_cache &&
      !thd->lex->using_hypergraph_optimizer()) {
    Sql_cmd *sql_cmd = thd->lex->m_sql_cmd;
    Prepared_statement *ps_owner =
        (sql_cmd != nullptr) ? sql_cmd->owner() : nullptr;
    if (ps_owner != nullptr &&
        ps_owner->ps_point_plan_state() == PsPointPlanState::HOT &&
        !ps_owner->ps_point_plan_cursor_execution()) {
      if (ps_point_plan_build_fast_path(thd, this, ps_owner)) {
        set_optimized();
        tables_list = query_block->leaf_tables;
        set_plan_state(PLAN_READY);
        DEBUG_SYNC(thd, "after_join_optimize");
        error = 0;
        return false;
      }
    }
  }

  // --- normal optimizer preamble continues unchanged ---
  THD_STAGE_INFO(thd, stage_optimizing);
  // ...
```

**关键变更点**：

1. hook 从 line 703 移到 line 352 之后（`if (optimized)` 之后）
2. fast path 成功时，自行调用 `set_optimized()` 和 `tables_list = ...`
3. 不再依赖 `get_optimizable_conditions()` 预先设置 `where_cond`

**依赖分析 — 为何 fast path 不依赖 preamble**：

| preamble 步骤 | fast path 是否使用 | 说明 |
|--------------|-------------------|------|
| `count_field_types` → `tmp_table_param` | 否 | fast path 不创建 tmp table |
| `alloc_func_list` → `sum_funcs` | 否 | 点查无聚合函数 |
| `get_optimizable_conditions` → `where_cond` | 否 | fast path 直接 `join->where_cond = nullptr` |
| `alloc_indirection_slices` → `ref_items` | 否 | fast path 不使用 indirection slices |
| `optimize_cond` | 否 | 条件优化结果被丢弃 |
| `set_optimized()` | 是 | V1.2 在 fast path 内自行调用 |
| `tables_list` | 是 | V1.2 在 fast path 内自行赋值 |

**对 COLD / fallback 路径的影响**：无。hook 失败时 fall through 到原有
preamble 代码，与 V1.1 完全一致。

### 3.3 变更 2：消除 build_fast_path 中对 where_cond 的依赖

V1.1 `build_fast_path` 注释中说明（line 479-481）：

> `where_cond is already set to the real WHERE by
>  get_optimizable_conditions().  Clearing it and then falling back
>  would make the normal optimizer miss the predicate.`

V1.2 中 fast path 在 preamble 之前执行，`where_cond` 尚未设置（仍为
初始值 nullptr 或上次执行残留值）。需要调整 invariant 注释，并确认：

- fast path 成功：`join->where_cond = nullptr`，正确（EQ_REF plan 不需要 WHERE）
- fast path 失败：fall through 到正常 preamble，`get_optimizable_conditions()`
  将正确设置 `where_cond`，与 V1.1 行为一致

因此无需代码逻辑变更，仅需更新注释。

### 3.4 变更 3：Arena 缓存 QEP_TAB / QEP_shared / 指针数组

将 V1.1 每次在 `thd->mem_root` 上分配的固定结构，改为 admission 时
一次性在 PS arena 上分配，后续 HOT 执行直接复用。

**新增模板字段** (`ps_point_plan_cache.h`):

```cpp
struct PsPointPlanTemplate {
  // ... existing fields ...

  // --- V1.2 新增：arena 缓存的执行计划骨架 ---

  /// Arena-cached QEP_TAB[2] (1 real + 1 sentinel).
  QEP_TAB *cached_qep_tab{nullptr};

  /// Arena-cached QEP_shared for qep_tab[0].
  QEP_shared *cached_qep_shared{nullptr};

  /// Arena-cached pointer arrays for Index_lookup.
  store_key **cached_key_copy{nullptr};
  Item **cached_items{nullptr};
  bool **cached_cond_guards{nullptr};

  /// True when cached_qep_tab et al. are populated and usable.
  bool qep_cached{false};
};
```

**Arena 分配时机**：在 `ps_point_plan_admit()` 中，与 `ref_cached`
组件的 arena 分配同步进行（已有 `swap_query_arena` 机制）。

```cpp
// 在 ps_point_plan_admit() 的 arena 分配块中追加:
if (!tpl.qep_cached) {
    // 已在 PS arena 中 (swap_query_arena 已调用)
    tpl.cached_qep_tab = new (thd->mem_root) QEP_TAB[2];
    tpl.cached_qep_shared = new (thd->mem_root) QEP_shared;
    tpl.cached_key_copy = thd->mem_root->ArrayAlloc<store_key*>(tpl.key_parts);
    tpl.cached_items = thd->mem_root->ArrayAlloc<Item*>(tpl.key_parts);
    tpl.cached_cond_guards = thd->mem_root->ArrayAlloc<bool*>(tpl.key_parts);

    if (all allocations succeeded) {
      // 预填充固定值
      for (uint i = 0; i < tpl.key_parts; i++) {
        tpl.cached_items[i] = tpl.params[i];
        tpl.cached_cond_guards[i] = nullptr;
      }
      tpl.qep_cached = true;
    }
}
```

**Fast path 复用**：`build_fast_path()` 中，当 `tpl.qep_cached` 为
true 时，直接使用缓存对象而非重新分配：

```cpp
if (tpl.qep_cached) {
  // 复用 arena 缓存的 QEP_TAB
  QEP_TAB *tab = &tpl.cached_qep_tab[0];
  tab->set_qs(tpl.cached_qep_shared);
  tab->set_join(join);          // 每次不同
  tab->set_table(table);        // 每次不同（重新绑定）
  tab->table_ref = tpl.table_ref;
  tab->set_type(JT_EQ_REF);

  // 复用 ref 指针数组
  Index_lookup &ref = tab->ref();
  ref.key_copy = tpl.cached_key_copy;
  ref.items = tpl.cached_items;
  ref.cond_guards = tpl.cached_cond_guards;
  // ... 其余 ref 元数据从模板复制（同 V1.1）

  // key_copy[] 需要根据当前 null_key 状态更新
  for (uint i = 0; i < tpl.key_parts; i++) {
    store_key *sk = tpl.cached_store_keys[i];
    tpl.cached_to_fields[i]->init(table);
    (void)sk->copy();
    ref.key_copy[i] = sk->null_key ? sk : nullptr;
  }
} else {
  // fallback: 在 thd->mem_root 上分配（同 V1.1）
}
```

**对 invalidation 的影响**：`invalidate_ps_point_plan_cache()` 已在
V1.1 中保留 arena 缓存组件（`ref_cached`, `cached_key_buff` 等）。
V1.2 同样保留 `qep_cached`, `cached_qep_tab`, `cached_qep_shared`,
`cached_key_copy`, `cached_items`, `cached_cond_guards`。

### 3.5 变更 4：AccessPath 缓存策略

AccessPath 不适合缓存在 arena 上，因为：

1. `NewEQRefAccessPath()` 返回的对象内部持有 `TABLE*` 指针，每次执行
   的 TABLE 实例可能不同（虽然在当前 V1 场景下通常相同）
2. AccessPath 的 cost/rows 来自模板，但 `eq_ref_path.table` 和
   `eq_ref_path.ref` 指向每次不同的 QEP_TAB::ref()

**决策**：保持 AccessPath 在 `thd->mem_root` 上每次分配。分配成本极低
（单次 bump alloc + 几个赋值），不值得为缓存它引入额外的指针 re-patch
复杂度。

但若 QEP_TAB 被 arena 缓存且结构固定，则 AccessPath 中的 `ref`
指针始终指向同一个 `cached_qep_tab[0].ref()`，无需每次 re-patch。
这使 AccessPath 在未来也有缓存可能。V1.2 暂不实施。

## 4. 对象生命周期模型 (V1.2 更新)

```
Layer 0: 永久对象 (PS m_arena)
  ├── Table_ref*              — parse tree，跨执行稳定
  ├── Item_param*             — 参数占位符，每次 execute 更新值
  ├── PsPointPlanTemplate     — 缓存元数据
  ├── cached_key_buff/2       — 序列化 key 缓冲区        [V1.1]
  ├── store_key objects       — Field clone + 序列化逻辑  [V1.1]
  ├── Field clones            — 在 store_key 内部        [V1.1]
  ├── cached_qep_tab[2]       — 执行计划骨架             [V1.2 新增]
  ├── cached_qep_shared       — QEP 共享结构             [V1.2 新增]
  ├── cached_key_copy[]       — store_key 指针数组       [V1.2 新增]
  ├── cached_items[]          — Item_param 指针数组      [V1.2 新增]
  └── cached_cond_guards[]    — guard 指针数组（全 null） [V1.2 新增]

Layer 1: 每次执行 (thd->mem_root)
  ├── JOIN object             — 不可跨执行保持
  └── AccessPath              — NewEQRefAccessPath()

Layer 2: 每次绑定 (open_tables → close_thread_tables)
  ├── TABLE*                  — 通过 table_ref->table 获取
  └── handler/file            — ha_index_init / ha_index_end
```

**相比 V1.1 的变化**：QEP_TAB、QEP_shared、3 个指针数组从 Layer 1
上移到 Layer 0。Layer 1 仅剩 JOIN 本身和 AccessPath。

## 5. re-patch 安全性分析 (V1.2 扩展)

### 5.1 QEP_TAB re-patch

arena 缓存的 `QEP_TAB` 在每次 fast path 执行时需要更新：

| 字段 | 操作 | 安全性 |
|------|------|--------|
| `set_join(join)` | 赋值当前 JOIN* | 安全 — JOIN 是本次的临时对象 |
| `set_table(table)` | 赋值当前 TABLE* | 安全 — open_tables 确保有效 |
| `set_idx(0)` | 常量 | 安全 |
| `table_ref` | 从模板取 | 安全 — parse tree 稳定 |
| `set_type(JT_EQ_REF)` | 常量 | 安全 |

`set_qs(cached_qep_shared)` 指向同一 arena 上的对象，无生命周期问题。

### 5.2 QEP_shared re-patch

QEP_shared 在 `set_table()` 时会更新内部 table 指针。其余字段
（`m_idx`, `m_type` 等）都是在 QEP_TAB 级别通过 setter 设置的，
不会残留上次执行的状态——因为每次 fast path 开头会完整重设这些字段。

### 5.3 key_copy[] 的 null_key 依赖

`key_copy[i]` 的值取决于 `store_key::copy()` 之后的 `null_key` 状态，
这是参数值相关的（当参数为 NULL 时 `null_key = true`）。因此
`cached_key_copy[]` 的预填充值不能在 admission 时确定，必须在每次
fast path 的 `copy()` 之后更新。

解决方案：`cached_key_copy[]` 在 arena 上分配空间，但内容在每次
fast path 中由 `copy()` 后的 `sk->null_key` 决定。这仍然节省了
每次在 `thd->mem_root` 上分配 3 个数组的开销。

## 6. 实现步骤

### Step 1: 移动 fast path hook

**文件**: `sql/sql_optimizer.cc`

1. 在 `JOIN::optimize()` 的 `if (optimized) return false;` 之后，
   `THD_STAGE_INFO` 之前，插入 V1.2 fast path 检查块
2. fast path 成功时调用 `set_optimized()` 和 `tables_list = ...`
3. 删除原 line 703-716 的 V1.1 hook 块（避免双重检查）
4. 更新 `build_fast_path()` 注释，移除对 `get_optimizable_conditions`
   的依赖说明

### Step 2: 扩展 PsPointPlanTemplate

**文件**: `sql/ps_point_plan_cache.h`

1. 新增 `cached_qep_tab`, `cached_qep_shared`, `cached_key_copy`,
   `cached_items`, `cached_cond_guards`, `qep_cached` 字段

### Step 3: Arena 分配 QEP 组件

**文件**: `sql/ps_point_plan_cache.cc` (`ps_point_plan_admit`)

1. 在已有 arena 分配块中追加 QEP_TAB/QEP_shared/数组分配
2. 预填充固定值（items[i] = params[i], cond_guards[i] = nullptr）

### Step 4: Fast path 复用 arena 缓存

**文件**: `sql/ps_point_plan_cache.cc` (`build_fast_path`)

1. 当 `tpl.qep_cached && tpl.ref_cached` 时进入全缓存路径
2. 重新绑定 join/table 指针
3. 在 `copy()` 之后更新 `cached_key_copy[i]`
4. 保留 AccessPath 的 per-execution 分配

### Step 5: 更新 invalidation 保留列表

**文件**: `sql/sql_prepare.h` (`invalidate_ps_point_plan_cache`)

1. 在已有的保留列表中追加 `qep_cached`, `cached_qep_tab`,
   `cached_qep_shared`, `cached_key_copy`, `cached_items`,
   `cached_cond_guards`

### Step 6: 测试更新

**文件**: `mysql-test/t/ps_point_plan_cache_coverage.test`

1. 追加 "multi-round DDL → re-admit → hit" 场景，验证 arena 缓存
   的 QEP 组件在 invalidation + re-admission 后正确复用
2. 验证 fallback 路径（arena 分配失败）仍正确走 thd->mem_root 分配

## 7. 风险分析

### 7.1 QEP_TAB 残留状态

**风险**：arena 缓存的 QEP_TAB 可能残留上次执行的状态。

**缓解**：fast path 在使用前完整重设所有必要字段（join, table, idx,
type, ref 元数据）。不需要 memset，因为所有被读取的字段都在 fast path
中显式赋值。需要审计 QEP_TAB 的所有被 executor 读取的字段，确保无
遗漏。

### 7.2 JOIN::optimize 内部状态一致性

**风险**：移动 hook 到入口后，fast path 成功时 `optimized` 标记和
`tables_list` 等 JOIN 成员的设置可能不完整。

**缓解**：审计 fast path 返回后，`JOIN::optimize()` 的调用方
(`Query_block::optimize()`, `Sql_cmd_dml::execute_inner()`) 对 JOIN
成员的假设。关键检查项：

- `set_optimized()` → `Query_block::optimize()` 在 `JOIN::optimize()`
  返回后不再读取 JOIN internal state
- `set_plan_state(PLAN_READY)` → executor 检查此状态决定是否执行
- `tables_list` → executor 迭代器构建需要此值

### 7.3 EXPLAIN 兼容性

**风险**：fast path 跳过 preamble 后，`EXPLAIN` 可能缺少必要的
trace 和优化器信息。

**缓解**：V1 的 classify gate 已排除 EXPLAIN（classify 只对
非-EXPLAIN PS 生效）。如果未来需要支持 EXPLAIN + plan cache，
需要在 EXPLAIN 路径上 bypass fast path。当前无风险。

### 7.4 arena 内存增长

**风险**：QEP_TAB[2] + QEP_shared + 3 个指针数组在 PS arena 上
永驻，增加每个 HOT PS 约 400 bytes 的 arena 内存。

**缓解**：这是一次性增长，不随执行次数增长。一个连接上活跃的 HOT PS
数量有限（通常 < 100），额外内存 < 40 KB，可接受。

## 8. 跳过的 optimizer preamble 函数审计

逐一确认 fast path 不依赖每个被跳过的函数：

| 函数 | 产物 | 点查是否需要 | 说明 |
|------|------|-------------|------|
| `count_field_types()` | `tmp_table_param.{sum_func_count, func_count}` | 否 | 点查无聚合，不创建 tmp table |
| `Window::setup_windows2()` | window 函数初始化 | 否 | classify gate 已排除 `has_windows()` |
| `optimize_rollup()` | ROLLUP 优化 | 否 | classify gate 已排除 `is_grouped()` |
| `alloc_func_list()` | `sum_funcs[]` 数组 | 否 | 点查 sum_func_count == 0 |
| `get_optimizable_conditions()` | `where_cond`, `having_cond` | 否 | fast path 设 nullptr |
| `alloc_indirection_slices()` | `ref_items[]`, `tmp_fields[]` | 否 | fast path 不使用 indirection |
| 派生表优化循环 | `access_path_for_derived` | 否 | classify gate 已排除 derived/view |
| `optimize_cond()` | 简化后的 WHERE/HAVING | 否 | fast path 不用 |
| `prune_table_partitions()` | 分区裁剪 | 否 | Gate 4b 已排除分区表 |
| `optimize_aggregated_query()` | 聚合优化 | 否 | 无聚合函数 |
| `substitute_gc()` | generated column 替换 | 否 | 不影响 EQ_REF key lookup |
| `Opt_trace_*` 构造/析构 | trace JSON | 否 | 非 EXPLAIN 路径 |

**结论**：所有被跳过的函数在点查场景下都不被需要，跳过是安全的。
这一安全性的根本保障来自 Phase 1 `ps_point_plan_classify()` 的
严格 gate（单表、无聚合、无排序、无 LIMIT、无子查询、无分区），
它确保了任何通过 classify 的语句都不可能需要上述 preamble 的输出。

## 9. 性能模型

### 9.1 节省的 CPU 开销（每次 HOT 执行）

| 来源 | 预估节省 |
|------|---------|
| 跳过 `count_field_types` 遍历 | ~50 ns |
| 跳过 `alloc_func_list` calloc | ~30 ns |
| 跳过 `get_optimizable_conditions` 条件复制 | ~100 ns |
| 跳过 `alloc_indirection_slices` 两次 ArrayAlloc | ~50 ns |
| 跳过 `optimize_cond` 条件树遍历 | ~200 ns |
| 跳过 `substitute_gc` 循环 | ~30 ns |
| 跳过 `Opt_trace` 构造/析构 | ~50 ns |
| 跳过派生表循环 + limit 计算 + aggregated_query 检查 | ~50 ns |
| 消除 QEP_TAB + QEP_shared 分配 | ~30 ns |
| 消除 3 个指针数组分配 | ~20 ns |
| **总计** | **~600 ns / query** |

### 9.2 预估 QPS 提升

在 8 线程 sysbench `oltp_point_select` 下：
- 当前 per-query 延迟约 27.6 μs (= 1/290426 * 8 threads)
- 节省 600 ns ≈ 2.2% per-query 延迟减少
- 考虑并发放大效应（减少 CPU 竞争）：实际提升预估 +4~7%

总体预估：

| 并发 | V1.1 提升 | V1.2 额外提升 | V1.2 总提升 |
|------|----------|-------------|-----------|
| 4 线程 | +2.65% | +2~4% | +5~7% |
| 8 线程 | +6.79% | +4~7% | +10~13% |

## 10. 未来优化方向 (V1.3+)

V1.2 解决了 `JOIN::optimize()` 内部的浪费。以下是更外层的优化方向，
需要更深入的架构改动：

### 10.1 open_tables / lock_tables 快速验证路径

对 HOT PS 的单表场景，`open_tables_for_query()` 可以被替换为一个
轻量级的 "table handle 仍有效" 检查（通过 `table_ref_version` +
MDL 持有状态），跳过完整的 table cache lookup 和 MDL 请求流程。

预估额外提升：+2~3%。风险：中等（需要确保 DDL 并发安全性）。

### 10.2 read-only autocommit 事务优化

对纯读 autocommit SELECT，`trans_commit_stmt()` 中 InnoDB 已经在
`external_lock(F_UNLCK)` 时完成了实际的提交。server 层的
`ha_commit_trans()` 调用可以在确认 `trx->read_only` 时短路。

预估额外提升：+0.5~1%。风险：低。

### 10.3 Sql_cmd_dml::execute 层面拦截

最激进的方案：对 HOT PS 在 `Sql_cmd_dml::execute()` 层面构建一个
完全绕过 `execute_inner()` → `JOIN::optimize()` 链路的超快路径，
直接从 cached template + table handle 构建 executor iterator
并执行。这将跳过 `Query_expression::optimize()` → `Query_block::optimize()`
→ `JOIN::optimize()` 的整个调用链。

预估额外提升：+2~3%。风险：高（需要 deep integration）。

## 11. 测试覆盖矩阵 (V1.2 增量)

| 场景 | 验证目标 | 预期行为 |
|------|---------|---------|
| HOT hit 跳过 preamble | status counters 确认 hit | 无 optimizer_trace 输出 |
| HOT fallback → 正常 preamble | G7 NULL param → fallback | 正常优化成功 |
| Arena QEP 缓存复用 | 多轮 execute → hit | 无内存增长 |
| Invalidation 保留 QEP 缓存 | DDL → invalidate → reprepare → re-admit → hit | 复用 arena QEP |
| QEP_TAB re-patch 正确性 | 连续 100 次 execute 结果一致 | 每次返回正确行 |
| EXPLAIN 不走 fast path | EXPLAIN SELECT ... | 正常 EXPLAIN 输出 |

## 12. 实施计划 (Implementation Plan)

### Step 1: 扩展 PsPointPlanTemplate — 新增 QEP 缓存字段
- **文件**: `sql/ps_point_plan_cache.h`
- **内容**: 在 `ref_cached` 之后新增 `cached_qep_tab`, `cached_qep_shared`,
  `cached_key_copy`, `cached_items`, `cached_cond_guards`, `qep_cached` 字段
- **前置依赖**: 无

### Step 2: 更新 invalidation 保留列表
- **文件**: `sql/sql_prepare.h`
- **内容**: 在 `invalidate_ps_point_plan_cache()` 中保留 V1.2 新增的
  arena 缓存字段（`qep_cached`, `cached_qep_tab` 等）
- **前置依赖**: Step 1

### Step 3: Arena 分配 QEP 组件
- **文件**: `sql/ps_point_plan_cache.cc` (`ps_point_plan_admit`)
- **内容**: 在已有 arena 分配块（`!tpl.ref_cached` 分支）中追加
  QEP_TAB/QEP_shared/指针数组的 arena 分配，并预填充固定值
- **前置依赖**: Step 1

### Step 4: build_fast_path 复用 arena 缓存
- **文件**: `sql/ps_point_plan_cache.cc` (`ps_point_plan_build_fast_path`)
- **内容**: 当 `tpl.qep_cached && tpl.ref_cached` 时进入全缓存路径，
  复用 arena QEP_TAB/QEP_shared/指针数组，仅重新绑定 join/table 指针
  和执行 store_key::copy()
- **前置依赖**: Step 3

### Step 5: 移动 fast path hook 到 JOIN::optimize 入口
- **文件**: `sql/sql_optimizer.cc`
- **内容**: 将 fast path 检查从 line 703 移到 `if (optimized)` 之后，
  删除原位置的 hook 块。成功时自行设 `set_optimized()` + `tables_list`
- **前置依赖**: Step 4

### Step 6: 更新注释
- **文件**: `sql/ps_point_plan_cache.cc`, `sql/ps_point_plan_cache.h`
- **内容**: 更新 `build_fast_path` 的 MANDATORY INVARIANT 注释，移除
  对 `get_optimizable_conditions` 的依赖描述；更新头文件文档注释
- **前置依赖**: Step 5

### Step 7: 编译验证 + MTR 测试
- 编译 debug build
- 运行全部 plan cache 相关 MTR 测试
- **前置依赖**: Step 6

### Step 8: 性能验证
- 编译 release build
- 运行 sysbench oltp_point_select 8 线程 / 4 线程对比
- **前置依赖**: Step 7

## 12. 实施计划 (Implementation Plan)

### Step 1: 扩展 PsPointPlanTemplate 结构体
**文件**: `sql/ps_point_plan_cache.h`
- 在 `ref_cached` 之后新增 V1.2 arena 缓存字段：
  `cached_qep_tab`, `cached_qep_shared`, `cached_key_copy`,
  `cached_items`, `cached_cond_guards`, `qep_cached`
- 新增必要的前向声明 (`QEP_TAB`, `QEP_shared`)

### Step 2: 更新 invalidation 保留列表
**文件**: `sql/sql_prepare.h`
- 在 `invalidate_ps_point_plan_cache()` 中追加保留 V1.2 新增的
  6 个 arena 缓存字段

### Step 3: Arena 分配 QEP 组件
**文件**: `sql/ps_point_plan_cache.cc` (`ps_point_plan_admit`)
- 在已有 ref_cached arena 分配块中追加 QEP_TAB/QEP_shared/数组分配
- 预填充固定值 `items[i] = params[i]`, `cond_guards[i] = nullptr`

### Step 4: 改造 build_fast_path 使用 arena 缓存
**文件**: `sql/ps_point_plan_cache.cc` (`ps_point_plan_build_fast_path`)
- 当 `tpl.qep_cached && tpl.ref_cached` 时进入全缓存路径
- 复用 arena QEP_TAB/QEP_shared/指针数组
- 仅重新绑定 join/table 指针和 re-copy key 值
- 更新 MANDATORY INVARIANT 注释

### Step 5: 移动 fast path hook 到 JOIN::optimize 入口
**文件**: `sql/sql_optimizer.cc`
- 在 `if (optimized) return false;` 之后插入 V1.2 early fast path
- fast path 成功时自行调用 `set_optimized()` + `tables_list = ...`
- 删除原 line 695-717 的 V1.1 hook 块
- 保留 Phase 2 admission hook 不变

### Step 6: 编译验证
- Debug build 编译通过
- 运行全部 plan cache MTR 测试

### Step 7: Release build + Benchmark
- Release build 编译
- 运行 sysbench 4 线程 + 8 线程 point_select 对比
