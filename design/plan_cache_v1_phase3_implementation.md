# Phase 3: fast path 接管 — 详细代码设计与实施计划

## 目标

HOT 语句在后续 EXECUTE 中跳过通用优化路径，直接在 `JOIN::optimize()` 早期构造最小执行计划：

- 在 `make_join_plan()` 之前截断
- 跳过 `alloc_qep()`、`init_ref_access()`、`make_join_readinfo()`、`make_tmp_tables_info()`、`create_access_paths()`、`push_to_engines()`
- 手工构造一表 `QEP_TAB(JT_EQ_REF)` + `Index_lookup` + `AccessPath`
- 命中后设置 `PLAN_READY`，走原生 iterator 执行链

## 前置状态（Phase 0 + 1 + 2 完成后）

Phase 2 admission 完成后，HOT 语句的 `PsPointPlanTemplate` 包含：

| 字段 | 来源 | Phase 3 用途 |
|------|------|------|
| `table_ref` | Phase 1 classify | 绑定 TABLE* |
| `plan_type` | Phase 1 classify | 选择 fast path builder (POINT_EQ_REF) |
| `param_count` | Phase 1 classify | key part 数量 |
| `params[]` (key-part order) | Phase 2 admit | `init_ref_part()` 的 val 参数 |
| `field_indices[]` (key-part order) | Phase 2 admit | runtime guard 校验列位置 |
| `actual_types[]` | Phase 2 admit | runtime guard 检查参数类型一致性 |
| `unsigned_actuals[]` | Phase 2 admit | runtime guard 检查有符号/无符号一致性 |
| `keyno` | Phase 2 admit | `init_ref()` 的 keyno 参数 |
| `key_parts` | Phase 2 admit | `init_ref()` 的 keyparts 参数 |
| `key_length` | Phase 2 admit | `init_ref()` 的 length 参数 |
| `null_rejecting` | Phase 2 admit | `init_ref_part()` 的 null_rejecting 参数 |
| `best_read` | Phase 2 admit | 回填 JOIN 代价 |
| `best_rowcount` | Phase 2 admit | 回填 JOIN 行数 |

现有 stub：
- `ps_point_plan_runtime_guard()` — `sql/ps_point_plan_cache.cc:248` — 返回 false
- `ps_point_plan_mark_hit()` — 已实现但无调用点
- `ps_point_plan_mark_runtime_fallback()` — 已实现但无调用点
- `ps_point_plan_mark_invalidation()` — 已实现但无调用点

## 关键设计决策

### 1. 使用 JT_EQ_REF（而非 JT_CONST）

**Phase 2 的关键发现：** 正常优化器将单表 PS 点查识别为 `JT_CONST`（因为 `Item_param::used_tables()` 返回 `INNER_TABLE_BIT`，`const_for_execution()` 为 true），行在优化阶段由 `join_read_const_table()` 读取。

**fast path 选择 `JT_EQ_REF` 的原因：**

| 维度 | JT_CONST（正常路径） | JT_EQ_REF（fast path） |
|------|---------------------|----------------------|
| 行读取时机 | 优化阶段 (`join_read_const_table`) | 执行阶段 (`EQRefIterator::Read`) |
| 复杂度 | 需复制 const table 读取逻辑 | 只需 `init_ref` + `init_ref_part` |
| handler 管理 | 需手动 `ha_index_init/end` | iterator 自动管理 |
| 错误处理 | 需处理行不存在/NULL 结果 | iterator 自然返回 EOF |
| 与 cleanup 的兼容性 | 需要对齐 const table cleanup | iterator cleanup 自动 |

**`init_ref_part` 对 `INNER_TABLE_BIT` 参数的行为：**

```
used_tables = INNER_TABLE_BIT
条件: used_tables & ~INNER_TABLE_BIT == 0
→ 进入 ELSE 分支（const table path）
→ 立即调用 s_key->copy() 将参数值写入 key_buff
→ 如果 !s_key->null_key，设置 ref->key_copy[i] = nullptr（已复制，不需要执行时再复制）
```

**执行时行为（EQRefIterator）：**

1. `Init()` → `construct_lookup()` → 遍历 `key_copy[]`
   - `key_copy[i] == nullptr` → 跳过（值已在 init_ref_part 时写入 key_buff）
2. `Init()` → `table->file->ha_index_init(ref.key, false)`
3. `Read()` → `table->file->ha_index_read_map(record[0], key_buff, ...)`
4. 返回一行或 EOF

值的正确性：每次 fast path 在 `thd->mem_root` 上分配新的 `Index_lookup`，`init_ref_part` 从当前执行的 `Item_param` 获取最新参数值。

### 2. 插入点：`make_join_plan()` 之前

在 `JOIN::optimize()` old optimizer 路径中，紧接 hypergraph assert 之后、`make_join_plan()` 之前插入。

```
JOIN::optimize() 代码流
├── 339: 函数入口
├── 364: count_field_types()
├── 379: get_optimizable_conditions()
├── 389: set_optimized()
├── 391: tables_list = leaf_tables
├── 393: alloc_indirection_slices()
├── 396: ref_items[REF_SLICE_ACTIVE] = base_ref_items
├── 404-430: 派生表优化（单 base table → 无操作）
├── 432-607: 各种优化检查（对 HOT PS 无操作）
├── 682: assert(!using_hypergraph_optimizer())
├── 【Phase 3 fast path 插入点】          ← HERE
├── 696: THD_STAGE_INFO(stage_statistics)
├── 697: make_join_plan()                  ← 昂贵操作开始
├── 1012: alloc_qep()
├── 1038: create_access_paths()
├── 1065: push_to_engines()
├── 1088-1100: Phase 2 admission hook
├── 1103: set_plan_state(PLAN_READY)
└── 1108: return false
```

**跳过的操作及其安全性：**

| 被跳过的操作 | 为什么安全 |
|-------------|----------|
| 404-430: 派生表优化 | HOT PS 只含 base table |
| 432-607: semi-join / 零结果 / 聚合优化 | HOT PS 无 SJ/agg/impossible WHERE |
| `make_join_plan()` | fast path 直接构造 QEP |
| `alloc_qep()` | fast path 手工分配 QEP_TAB |
| `init_ref_access()` | fast path 用 `init_ref/init_ref_part` |
| `make_join_readinfo()` | fast path 手工设置 JT_EQ_REF |
| `make_tmp_tables_info()` | 单表点查无需临时表 |
| `create_access_paths()` | fast path 直接构造 AccessPath |
| `push_to_engines()` | 单表查询无 pushed join |
| Phase 2 admission hook | HOT 语句已完成 admission |

### 3. 函数组织

Phase 3 的核心逻辑作为自由函数实现在 `ps_point_plan_cache.cc` 中，通过 `ps_point_plan_cache.h` 暴露接口。在 `JOIN::optimize()` 中通过 inline 调用。

不在 `JOIN` 类上新增 private 方法，原因：
- 减少对 `sql_optimizer.h` 的修改（该头文件被广泛 include）
- Plan cache 逻辑集中在 `ps_point_plan_cache.cc` 中，便于代码审查和维护
- 自由函数接收 `JOIN *` 参数即可访问所有需要的状态

## 迭代步骤

### Step 1: 实现 Runtime Guard

替换 `ps_point_plan_runtime_guard()` 的 stub。

### Step 2: 实现 fast path 主函数

新增 `ps_point_plan_build_fast_path()` 函数，执行 QEP_TAB + Index_lookup + AccessPath 构造。

### Step 3: 在 `JOIN::optimize()` 中挂载 fast path hook

在 `make_join_plan()` 前插入 fast path 调用。

### Step 4: MTR 测试

新增 `ps_point_plan_cache_fast_path.test`，覆盖 hit、fallback、结果正确性。

### Step 5: 更新已有 MTR 测试

更新 `ps_point_plan_cache_show_vars` 和 `ps_point_plan_cache_admission` 的期望值。

### Step 6: 编译验证 + 全量回归

确认无 warning、无崩溃、已有 MTR 全部通过。

## 核心修改点

### 1. 实现 `ps_point_plan_runtime_guard()` — `sql/ps_point_plan_cache.cc`

替换第 248 行的 stub：

```cpp
bool ps_point_plan_runtime_guard(THD *thd, Prepared_statement *stmt,
                                 TABLE **table_out, KEY **keyinfo_out) {
  const PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();

  /*
    Structural guards — if any fails, the template is no longer valid
    and must be invalidated.  The PS stays INVALID until reprepare.
  */

  /* G1: TABLE binding must be live. */
  if (tpl.table_ref == nullptr || tpl.table_ref->table == nullptr) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  TABLE *table = tpl.table_ref->table;

  /* G1b: TABLE_SHARE must be present (defensive; always true for
     tables opened via open_table_from_share, but the TABLE struct
     default-initializes s to nullptr). */
  if (table->s == nullptr) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  /* G2: key index must still be within bounds. */
  if (tpl.keyno >= table->s->keys) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  KEY *keyinfo = &table->key_info[tpl.keyno];

  /* G3: key must still be unique. */
  if (!(actual_key_flags(keyinfo) & HA_NOSAME)) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  /* G4: key part count must still match. */
  if (keyinfo->user_defined_key_parts != tpl.key_parts) {
    stmt->invalidate_ps_point_plan_cache();
    ps_point_plan_mark_invalidation(thd);
    return false;
  }

  /* G5: field ordinals for each key part must still match. */
  for (uint i = 0; i < tpl.key_parts; i++) {
    if (keyinfo->key_part[i].fieldnr - 1 != tpl.field_indices[i]) {
      stmt->invalidate_ps_point_plan_cache();
      ps_point_plan_mark_invalidation(thd);
      return false;
    }
  }

  /*
    Parameter guards — per-execution checks.
    If any fails, this execution falls back to normal path but
    the template stays HOT for future executions.
  */

  for (uint i = 0; i < tpl.param_count; i++) {
    /* G6: param pointer sanity (should never be nullptr after admission;
       defensive check against memory corruption or internal bugs). */
    if (tpl.params[i] == nullptr) {
      stmt->invalidate_ps_point_plan_cache();
      ps_point_plan_mark_invalidation(thd);
      return false;
    }

    /* G7: NULL parameter → runtime fallback (NULL != NULL in SQL). */
    if (tpl.params[i]->param_state() == Item_param::NULL_VALUE) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    /* G8: parameter actual type must match admission snapshot. */
    if (tpl.params[i]->data_type_actual() != tpl.actual_types[i]) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }

    /* G9: unsigned flag must match admission snapshot. */
    if (tpl.params[i]->is_unsigned_actual() != tpl.unsigned_actuals[i]) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
  }

  *table_out = table;
  *keyinfo_out = keyinfo;
  return true;
}
```

**Guard 分类说明：**

| Guard | 类别 | 后果 | 原因 |
|-------|------|------|------|
| G1: TABLE 无效 | invalidate | → INVALID | 表结构可能已变 |
| G1b: TABLE_SHARE null | invalidate | → INVALID | 防御性检查（正常路径不应发生） |
| G2: keyno 越界 | invalidate | → INVALID | DDL 删除了索引 |
| G3: key 非唯一 | invalidate | → INVALID | DDL 修改了索引属性 |
| G4: key part 数量变 | invalidate | → INVALID | DDL 修改了索引结构 |
| G5: field 位置变 | invalidate | → INVALID | DDL 修改了列顺序 |
| G6: param 指针 null | invalidate | → INVALID | 防御性检查（admission 后不应发生） |
| G7: 参数为 NULL | fallback | 保持 HOT | 本次参数不适合，下次可能非 NULL |
| G8: 参数类型变 | fallback | 保持 HOT | 本次类型不同，下次可能恢复 |
| G9: unsigned 变 | fallback | 保持 HOT | 同上 |

**注意：** 外层调用点（JOIN::optimize 的 fast path hook）已检查：
- `thd->variables.ps_point_plan_cache == true`
- `stmt->ps_point_plan_state() == HOT`
- `!stmt->ps_point_plan_cursor_execution()`
- `!thd->lex->using_hypergraph_optimizer()`（由 line 682 的 assert 保证）

因此 `ps_point_plan_runtime_guard()` 不重复这些检查。

### 2. 实现 `ps_point_plan_build_fast_path()` — `sql/ps_point_plan_cache.cc`

新增函数，构造最小一表 `JT_EQ_REF` 执行计划。

**MANDATORY INVARIANT — 延迟写入 JOIN 状态：**

`make_join_plan()` → `init_planner_arrays()` 包含硬性断言
（`sql/sql_optimizer.cc:5480`）：

```cpp
assert(primary_tables == 0 && tables == 0);
```

如果 fast path 修改了 `join->tables` 或 `join->primary_tables` 后失败，
控制流回到 `make_join_plan()`，此 assert 将在 debug 构建中触发。
此外，`where_cond` 在 `get_optimizable_conditions()`（line 379）中设置为
真正的 WHERE 条件；若被清空为 nullptr 后回退，`make_join_plan()` 将在
缺少 WHERE 条件的情况下运行，产生错误的全表扫描计划。

因此，**所有 JOIN 状态修改必须推迟到全部构造步骤成功之后**。
构造过程使用局部变量完成；只有在最终确认成功时才一次性写入 JOIN。

```cpp
/**
  Construct a minimal one-table EQ_REF execution plan for a HOT
  prepared statement, bypassing the full optimizer pipeline.

  @pre  thd->variables.ps_point_plan_cache == true
  @pre  stmt->ps_point_plan_state() == PsPointPlanState::HOT
  @pre  !stmt->ps_point_plan_cursor_execution()
  @pre  !thd->lex->using_hypergraph_optimizer()

  @param  thd   Current thread.
  @param  join  The JOIN being optimized.
  @param  stmt  The owning Prepared_statement (HOT state).

  @retval true  Fast path plan constructed; caller should set
                PLAN_READY and return.
  @retval false Fast path declined; caller should continue to
                make_join_plan() (JOIN state is untouched).
*/
bool ps_point_plan_build_fast_path(THD *thd, JOIN *join,
                                   Prepared_statement *stmt) {
  const PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();

  /*
    MANDATORY INVARIANT: Do NOT modify any JOIN member (tables,
    primary_tables, const_tables, where_cond, having_cond, qep_tab,
    best_read, best_rowcount, m_root_access_path) until ALL
    construction steps below have succeeded.

    Rationale:
      - init_planner_arrays() asserts primary_tables == 0 && tables == 0
        (sql/sql_optimizer.cc:5480).  Violating this crashes debug builds.
      - where_cond is already set to the real WHERE by
        get_optimizable_conditions() (line 379).  Clearing it and then
        falling back would make the normal optimizer miss the predicate.
  */

  /* --- Phase A: Runtime guard (read-only, no JOIN mutation) --- */
  TABLE *table = nullptr;
  KEY *keyinfo = nullptr;
  if (!ps_point_plan_runtime_guard(thd, stmt, &table, &keyinfo))
    return false;

  /* --- Phase B: Construct all objects in local variables --- */

  /* B1: Allocate QEP_TAB[2] (1 real + 1 sentinel) */
  QEP_TAB *new_qep_tab = new (thd->mem_root) QEP_TAB[2];
  if (new_qep_tab == nullptr) {
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  /* B2: Allocate and link QEP_shared */
  QEP_shared *qs = new (thd->mem_root) QEP_shared;
  if (qs == nullptr) {
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  QEP_TAB *tab = &new_qep_tab[0];
  tab->set_qs(qs);
  tab->set_join(join);
  tab->set_idx(0);
  tab->set_table(table);       // also sets table->reginfo.qep_tab
  tab->table_ref = tpl.table_ref;
  tab->set_type(JT_EQ_REF);

  /* B3: Build Index_lookup via init_ref + init_ref_part */
  if (init_ref(thd, tpl.key_parts, tpl.key_length, tpl.keyno,
               &tab->ref())) {
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  uchar *key_buff = tab->ref().key_buff;
  for (uint i = 0; i < tpl.key_parts; i++) {
    const KEY_PART_INFO *key_part = &keyinfo->key_part[i];
    const bool null_rej = (tpl.null_rejecting >> i) & 1;

    if (init_ref_part(thd, i, tpl.params[i],
                      /*cond_guard=*/nullptr, null_rej,
                      /*const_tables=*/0,
                      tpl.params[i]->used_tables(),
                      key_part->null_bit != 0,
                      key_part, key_buff, &tab->ref())) {
      ps_point_plan_mark_runtime_fallback(thd);
      return false;
    }
    key_buff += key_part->store_length;
  }
  assert(key_buff == tab->ref().key_buff + tpl.key_length);

  /* B4: Create AccessPath */
  AccessPath *path =
      NewEQRefAccessPath(thd, table, &tab->ref(), /*count_examined_rows=*/true);
  if (path == nullptr) {
    ps_point_plan_mark_runtime_fallback(thd);
    return false;
  }

  path->set_num_output_rows(tpl.best_rowcount);
  path->cost = tpl.best_read;
  path->init_cost = 0.0;
  path->init_once_cost = 0.0;

  /*
    --- Phase C: ALL construction succeeded — commit to JOIN ---

    This is the ONLY place where JOIN members are modified.
    If any step above failed and returned false, we reach here with
    the JOIN completely untouched, so fallback to make_join_plan()
    proceeds with correct state (tables==0, where_cond intact, etc.).
  */
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

**关键实现细节说明：**

**为什么 `const_tables = 0`：**

正常路径中 `const_tables = 1`（优化器识别为 const table），但 fast path 使用 `JT_EQ_REF`（执行时读行），不走 const table 逻辑，因此 `const_tables = 0`。

**为什么 `QEP_TAB[2]`（多分配一个）：**

与 `alloc_qep()` 一致——最后一个元素作为 sentinel（`JOIN::optimize()` 中某些遍历依赖 `qep_tab[tables]` 可达）。

**`key_buff` 指针推进：**

`init_ref_part()` 将第 i 个 key part 的值写入 `key_buff` 起始处，写入长度为 `key_part->store_length`。循环体末尾需要按 `store_length` 推进 `key_buff` 指针，确保每个 key part 的值写入正确的偏移位置。这与 `create_ref_for_key()` 中的处理一致（`sql/sql_select.cc:2466`）。

**为什么不需要调用 `table->file->ha_index_init()`：**

handler 的索引初始化由 `EQRefIterator::Init()` 在执行阶段处理。fast path 只构造 plan，不触碰 handler 状态。

**为什么不需要 `push_to_engines()`：**

`push_to_engines()` 遍历 AccessPath 树，将可下推的部分发给存储引擎（主要用于 NDB pushed join）。对于单表 EQ_REF 查询：
- InnoDB: 无 pushed join 可处理
- 其他引擎: 单表无 join 可下推
- 跳过此步不影响执行正确性

**错误处理策略：**

任何构造步骤失败（内存不足等）→ 标记 runtime fallback → 返回 false → 外层继续走正常优化路径。`thd->mem_root` 上的部分分配在命令结束时统一回收，不需要手动清理。

### 3. 挂载 fast path hook — `sql/sql_optimizer.cc`

**Hook 位置：** `JOIN::optimize()` 中 hypergraph assert 之后（第 693 行）、`make_join_plan()` 之前（第 697 行）。

```cpp
  assert(!thd->optimizer_switch_flag(OPTIMIZER_SWITCH_HYPERGRAPH_OPTIMIZER) ||
         !thd->stmt_arena->is_regular());

  /*
    ps_point_plan_cache Phase 3: fast path for HOT prepared statements.

    Before entering the expensive make_join_plan() pipeline, check if
    this is a HOT PS that can skip optimization entirely.  On success,
    a minimal one-table EQ_REF plan is constructed directly and we jump
    to PLAN_READY.  On failure, fall through to the normal optimizer.

    Prerequisites already met at this point:
      - set_optimized()                 (line 389)
      - tables_list = leaf_tables       (line 391)
      - alloc_indirection_slices()      (line 393)
      - ref_items[REF_SLICE_ACTIVE]     (line 396)

    Additional checks performed inside:
      - sysvar ON, state == HOT, not cursor, not hypergraph
      - runtime structural + parameter guards
  */
  if (thd->variables.ps_point_plan_cache) {
    Sql_cmd *sql_cmd = thd->lex->m_sql_cmd;
    Prepared_statement *ps_owner =
        (sql_cmd != nullptr) ? sql_cmd->owner() : nullptr;
    if (ps_owner != nullptr &&
        ps_owner->ps_point_plan_state() == PsPointPlanState::HOT &&
        !ps_owner->ps_point_plan_cursor_execution()) {
      if (ps_point_plan_build_fast_path(thd, this, ps_owner)) {
        set_plan_state(PLAN_READY);
        DEBUG_SYNC(thd, "after_join_optimize");
        error = 0;
        return false;
      }
    }
  }

  // Set up join order and initial access paths
  THD_STAGE_INFO(thd, stage_statistics);
  if (make_join_plan()) {
```

**外层 guard 说明：**

| 顺序 | 条件 | 目的 |
|------|------|------|
| 1 | `sysvar ON` | kill switch（最先检查，feature OFF 时零开销） |
| 2 | `sql_cmd != nullptr` | 排除非 SQL_CMD 路径 |
| 3 | `ps_owner != nullptr` | 排除非 PS 查询（普通 SQL） |
| 4 | `state == HOT` | 只有已 admission 的 PS 才尝试 fast path |
| 5 | `!cursor_execution` | cursor 场景走普通路径 |

**条件顺序原则：** sysvar 检查必须是最外层判断，与 Phase 2 admission hook
（`sql/sql_optimizer.cc:1088`）保持一致。当 feature 关闭时，不做任何指针
追踪（`thd->lex->m_sql_cmd` → `sql_cmd->owner()`），对非 plan cache 用户
的 `JOIN::optimize()` 调用零额外开销。

Hypergraph 排除由第 682 行的 assert 保证——如果到达此处，一定是 old optimizer。

**fast path 失败后的行为：**

`ps_point_plan_build_fast_path()` 返回 false → 控制流继续到
`make_join_plan()` → 走完整的正常优化路径。

由于 `ps_point_plan_build_fast_path()` 采用**延迟写入**模式（见核心修改点
第 2 节），失败时 JOIN 状态完全未被修改：`tables == 0`、`primary_tables == 0`、
`where_cond` 仍指向真实 WHERE 条件。`make_join_plan()` → `init_planner_arrays()`
的 `assert(primary_tables == 0 && tables == 0)`（`sql/sql_optimizer.cc:5480`）
正常通过，后续优化路径完全不受影响。

**注意：** fast path 成功后的 `return false` 跳过了 Phase 2 的 admission hook（第 1088-1100 行），这是正确行为——HOT PS 已完成 admission，不需要再次检查。

### 4. 需要新增的 include

**`sql/ps_point_plan_cache.cc`** 新增：
- `"sql/join_optimizer/access_path.h"` — `NewEQRefAccessPath()`、`AccessPath` 类型

**`sql/ps_point_plan_cache.h`** 新增：
- `ps_point_plan_build_fast_path()` 声明

已有 include（Phase 0-2 已加入）中 `sql/sql_select.h`（`init_ref`/`init_ref_part`）、`sql/sql_executor.h`（`QEP_TAB`）、`sql/sql_opt_exec_shared.h`（`QEP_shared`/`Index_lookup`）、`sql/sql_optimizer.h`（`JOIN`）均已可用。

### 5. MTR 测试 — `mysql-test/t/ps_point_plan_cache_fast_path.test`

**测试 schema：**

```sql
CREATE TABLE t1 (
  id INT PRIMARY KEY,
  val VARCHAR(50),
  uk INT NOT NULL,
  UNIQUE KEY idx_uk (uk)
);

CREATE TABLE t_comp (
  pk1 INT,
  pk2 INT,
  val INT,
  PRIMARY KEY (pk1, pk2)
);

INSERT INTO t1 VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30);
INSERT INTO t_comp VALUES (1, 1, 100), (1, 2, 200), (2, 1, 300);
```

**测试矩阵：**

| 用例 | 预期 | 验证方法 |
|------|------|---------|
| PK 点查 PREPARE + 二次 EXECUTE | 第二次 EXECUTE hit | hits +1 |
| PK 点查结果正确性 | 返回正确行 | SELECT 结果验证 |
| UK 点查 hit | 第二次 EXECUTE hit | hits +1 |
| UK 点查结果正确性 | 返回正确行 | SELECT 结果验证 |
| 复合 PK 点查 hit | 第二次 EXECUTE hit | hits +1 |
| 复合 PK 点查结果正确性 | 返回正确行 | SELECT 结果验证 |
| 复合 PK 反序 WHERE hit | 第二次 EXECUTE hit（params 已在 Phase 2 重排） | hits +1 |
| 不存在的 PK 值 | 返回空结果 | 0 rows |
| NULL 参数 | runtime fallback（不 hit） | fallback_runtime +1, hits 不变 |
| sysvar OFF 后 EXECUTE | bypass（不 hit） | hits 不变 |
| sysvar ON→OFF→ON 切换 | 恢复 hit | hits +1 |
| cursor 场景 | bypass（不 hit） | hits 不变 |
| 多次 EXECUTE 连续 hit | hits 每次 +1 | 累计 hits 正确 |
| 不同参数值连续 EXECUTE | 每次返回正确行 + hit | 结果正确 + hits 正确 |
| EXPLAIN 输出差异 | fast path 显示 `eq_ref`（非 `const`） | EXPLAIN type 列验证 |
| DDL 后 reprepare + re-classify | ALTER TABLE 后首次 EXECUTE 触发 reprepare，重新 COLD→HOT | admissions 再增 +1 |

**辅助断言模式：**

```sql
FLUSH STATUS;

PREPARE stmt FROM 'SELECT * FROM t1 WHERE id = ?';

-- 第一次 EXECUTE: 触发 admission
SET @p = 1;
EXECUTE stmt USING @p;

-- 第二次 EXECUTE: 应 hit fast path
--let $hits_before = query_get_value(SHOW STATUS LIKE 'Ps_point_plan_cache_hits', Value, 1)
SET @p = 2;
EXECUTE stmt USING @p;
--let $hits_after = query_get_value(SHOW STATUS LIKE 'Ps_point_plan_cache_hits', Value, 1)
--let $hits_delta = `SELECT $hits_after - $hits_before`
--echo hits delta after second EXECUTE: $hits_delta
-- 期望: 1

-- 验证结果正确
EXECUTE stmt USING @p;
-- 期望: id=2, val='two', uk=20
```

**EXPLAIN 行为差异验证（已知 trade-off）：**

fast path 使用 `JT_EQ_REF`，EXPLAIN 输出为 `eq_ref` 而非正常路径的 `const`。
此为已知行为差异，需显式测试并记录。

```sql
FLUSH STATUS;
SET @ps_pc = @@global.ps_point_plan_cache;
SET GLOBAL ps_point_plan_cache = ON;

CREATE TABLE t_explain (id INT PRIMARY KEY, val VARCHAR(50));
INSERT INTO t_explain VALUES (1, 'one'), (2, 'two');

PREPARE stmt_exp FROM 'SELECT * FROM t_explain WHERE id = ?';

-- 第一次 EXECUTE: admission (正常路径, EXPLAIN 显示 const)
SET @p = 1;
EXECUTE stmt_exp USING @p;

-- 第二次 EXECUTE: fast path hit (EXPLAIN 显示 eq_ref)
SET @p = 2;
EXECUTE stmt_exp USING @p;
--let $hits = query_get_value(SHOW STATUS LIKE 'Ps_point_plan_cache_hits', Value, 1)
-- 验证 fast path 确实被使用
-- 期望: hits >= 1

-- 通过 EXPLAIN FOR CONNECTION 或独立 EXPLAIN 验证
-- 注意: EXPLAIN <stmt> 对 PS 不直接可用，需要通过 SHOW WARNINGS 或
-- EXPLAIN FOR CONNECTION 检查。使用 status variable 间接验证即可。
-- fast path 的 type=eq_ref 差异记录在 Known Limitations 中。

DEALLOCATE PREPARE stmt_exp;
DROP TABLE t_explain;
SET GLOBAL ps_point_plan_cache = @ps_pc;
```

**DDL 期间 reprepare + re-classification 验证：**

验证 `ALTER TABLE` 在两次 EXECUTE 之间触发 reprepare，
PS 从 HOT → INVALID → 重新 prepare → COLD → 首次 EXECUTE → HOT。

```sql
FLUSH STATUS;
SET @ps_pc = @@global.ps_point_plan_cache;
SET GLOBAL ps_point_plan_cache = ON;

CREATE TABLE t_ddl (id INT PRIMARY KEY, val VARCHAR(50));
INSERT INTO t_ddl VALUES (1, 'one'), (2, 'two');

PREPARE stmt_ddl FROM 'SELECT * FROM t_ddl WHERE id = ?';

-- 第一次 EXECUTE: admission → HOT
SET @p = 1;
EXECUTE stmt_ddl USING @p;
--let $adm1 = query_get_value(SHOW STATUS LIKE 'Ps_point_plan_cache_admissions', Value, 1)
-- 期望: adm1 = 1

-- 第二次 EXECUTE: fast path hit
SET @p = 2;
EXECUTE stmt_ddl USING @p;
--let $hits1 = query_get_value(SHOW STATUS LIKE 'Ps_point_plan_cache_hits', Value, 1)
-- 期望: hits1 = 1

-- DDL 使表版本变更 → 下次 EXECUTE 触发 reprepare
ALTER TABLE t_ddl ADD COLUMN extra INT DEFAULT 0;

-- 第三次 EXECUTE: reprepare → 重新 classify → 首次正常 optimize → re-admit
SET @p = 1;
EXECUTE stmt_ddl USING @p;
--let $adm2 = query_get_value(SHOW STATUS LIKE 'Ps_point_plan_cache_admissions', Value, 1)
-- 期望: adm2 = 2 (重新 admission)
-- 结果应包含 extra 列: (1, 'one', 0)

-- 第四次 EXECUTE: 重新 HOT → fast path hit
SET @p = 2;
EXECUTE stmt_ddl USING @p;
--let $hits2 = query_get_value(SHOW STATUS LIKE 'Ps_point_plan_cache_hits', Value, 1)
-- 期望: hits2 = 2
-- 结果: (2, 'two', 0)

DEALLOCATE PREPARE stmt_ddl;
DROP TABLE t_ddl;
SET GLOBAL ps_point_plan_cache = @ps_pc;
```

### 6. 更新已有 MTR 测试

**`ps_point_plan_cache_show_vars`：**
- Phase 3 激活后，HOT PS 的后续 EXECUTE 会产生 hit
- 需要更新 Section 13 的 `Ps_point_plan_cache_hits` 期望值
- 具体数值取决于测试中 EXECUTE 的次数（每个 HOT PS 的每次后续 EXECUTE 产生 1 个 hit）

**`ps_point_plan_cache_admission`：**
- admission 测试中的后续 EXECUTE 现在会 hit
- 需要更新 hit counter 期望值
- admission counter 不变（admission 只在首次 EXECUTE 发生）

**`ps_point_plan_cache_binary_proto`：**
- 该测试使用字面值而非参数，`m_param_count == 0` → NEVER → 不受影响

**`ps_point_plan_cache_classify` / `classify_ext`：**
- 只做 PREPARE 不做 EXECUTE → 不受影响

## 执行计划的上层兼容性

### Query_expression 层

fast path 成功后：
- `JOIN::m_root_access_path` 已设置为 EQRefAccessPath
- `set_plan_state(PLAN_READY)` 已调用

上层调用链：
1. `Query_expression::optimize()` → 读取 `join->root_access_path()` ✓
2. `Query_expression::force_create_iterators()` → 从 AccessPath 创建 EQRefIterator ✓
3. `Query_expression::execute()` → Iterator::Init() + Read() ✓

### SELECT list 字段

字段解析在 `prepare()`/`open_tables()` 时完成（早于 `JOIN::optimize()`）。`Item_field` 指向 `TABLE::field[]` 中的正确列。EQRefIterator 读行后，字段从 `table->record[0]` 获取值。fast path 不影响字段解析。

### ref_items[REF_SLICE_ACTIVE]

由 `alloc_indirection_slices()` 分配并在第 396 行设置。fast path 在其之后执行，ref_items 已就绪。`set_plan_state(PLAN_READY)` 后，执行框架通过 ref_items 访问输出列。

## 风险控制

### 原则

- Guard 不通过时一律回退普通优化路径，不改变 PS 的 HOT 状态（除非是结构性变化）
- 构造失败时回退普通路径，不返回错误
- 任何不确定场景都 fallback

### 潜在风险与缓解

| 风险 | 缓解措施 |
|------|---------|
| init_ref_part 对 Item_param 的处理假设 | 已验证 INNER_TABLE_BIT → 立即复制值，与正常路径一致 |
| EQRefIterator 对 key_copy 全 nullptr 的处理 | construct_lookup 跳过 nullptr 条目，key_buff 已有值 |
| 跳过 push_to_engines 影响 NDB | 单表无 pushed join |
| thd->mem_root 上的 partial allocations | 命令结束时统一回收 |
| fast path 失败后 make_join_plan 读到错误的 JOIN 状态 | 延迟写入模式保证失败时 JOIN 状态完全未修改（见"MANDATORY INVARIANT"） |
| EXPLAIN FOR CONNECTION 看到 fast path plan | 正常工作——PLAN_READY 已设置，QEP_TAB 已构造 |

### 回退安全性验证

`ps_point_plan_build_fast_path()` 采用**延迟写入**模式（Phase A/B/C 三阶段），
所有 JOIN 状态修改集中在 Phase C（仅在全部构造成功后执行）。

**若在 Phase B 中任一步骤失败（例如 `init_ref()` 分配失败）：**

1. `join->tables` 仍为 0，`join->primary_tables` 仍为 0
   → 满足 `init_planner_arrays()` 的 `assert(primary_tables == 0 && tables == 0)`
2. `join->where_cond` 仍为 `get_optimizable_conditions()` 设置的真实 WHERE 条件
   → `make_join_plan()` 正常识别 ref 候选
3. `join->having_cond` 仍为 `get_optimizable_conditions()` 设置的值
4. `join->qep_tab` 仍为 nullptr → `alloc_qep()` 正常分配
5. `thd->mem_root` 上的 partial allocations（QEP_TAB、QEP_shared、key_buff 等）
   在命令结束时统一回收，无泄漏

**延迟写入不是可选优化——它是正确性的硬性要求。**

替代方案（save/restore）也可行，但更复杂且需要保存
`reinterpret_cast<Item *>(1)` 等 sentinel 值。延迟写入更简洁安全。

## 需要新增/修改的文件清单

| 文件 | 修改类型 | 内容 |
|------|---------|------|
| `sql/ps_point_plan_cache.h` | 修改 | 新增 `ps_point_plan_build_fast_path()` 声明 |
| `sql/ps_point_plan_cache.cc` | 修改 | 实现 runtime guard + build fast path |
| `sql/sql_optimizer.cc` | 修改 | 插入 Phase 3 fast path hook |
| `mysql-test/t/ps_point_plan_cache_fast_path.test` | 新增 | fast path 功能测试 |
| `mysql-test/r/ps_point_plan_cache_fast_path.result` | 新增 | 对应 result 文件 |
| `mysql-test/r/ps_point_plan_cache_show_vars.result` | 修改 | 更新 hits 期望值 |
| `mysql-test/r/ps_point_plan_cache_admission.result` | 修改 | 更新 hits 期望值 |

**不需要修改的文件：**
- `sql/sql_optimizer.h` — 不在 JOIN 类上新增方法
- `sql/sql_executor.h/cc` — 不新增执行器分支
- `sql/sql_prepare.h/cc` — 不修改 PS 生命周期
- `sql/system_variables.h` — 不新增变量
- `sql/sys_vars.cc` — 不新增 sysvar
- `sql/mysqld.cc` — 不新增 status

## 注意事项

- **`init_ref_part` 的 `const_tables` 参数：** 传 0 即可。该参数用于 `get_store_key()` 内部判断是否从 const table 获取值，对 `Item_param`（`INNER_TABLE_BIT`）无影响。
- **`init_ref_part` 的 `used_tables` 参数：** 传 `tpl.params[i]->used_tables()`（即 `INNER_TABLE_BIT`）。这决定了值是立即复制还是延迟到执行时。对 `Item_param`，值被立即复制。
- **`init_ref_part` 的 `nullable` 参数：** 传 `key_part->null_bit != 0`。这决定 key part 是否允许 NULL。
- **`QEP_shared_owner::set_qs()` 的 assert：** `assert(!m_qs)` 要求 QEP_TAB 之前没有关联过 QEP_shared。新分配的 QEP_TAB 默认 `m_qs = nullptr`，满足此 assert。
- **`QEP_TAB::set_table()` 的副作用：** 设置 `table->reginfo.qep_tab = this`，将 TABLE 反向关联到 QEP_TAB。执行框架依赖此关联。
- **EXPLAIN FOR CONNECTION 兼容性：** fast path 设置了 `PLAN_READY` 和 `qep_tab`，`EXPLAIN` 可以正常读取计划。`QEP_TAB::type() = JT_EQ_REF` 将在 EXPLAIN 中显示为 `eq_ref`（而非正常路径的 `const`）。这是一个可观察的行为差异，但不影响正确性。
- **`key_buff` 指针推进的正确性：** `key_part->store_length` 包含 key part 值的序列化长度（含 nullable byte 和前缀长度）。与 `create_ref_for_key()` 中的推进逻辑一致（`sql/sql_select.cc:2466`）。构造完成后通过 `assert(key_buff == tab->ref().key_buff + tpl.key_length)` 进行边界验证。

## Known Limitations (Phase 3 / V1)

以下为 Phase 3 实现中已知的行为差异和限制，均经过评估确认不影响正确性。

### 1. EXPLAIN 显示 `eq_ref` 而非 `const`

**现象：** 正常优化路径将单表唯一键点查识别为 `JT_CONST`，EXPLAIN 输出
`type = const`。fast path 使用 `JT_EQ_REF`，EXPLAIN 输出 `type = eq_ref`。

**影响范围：**
- `EXPLAIN` 及 `EXPLAIN FORMAT=JSON` 中的 `access_type` 字段
- `EXPLAIN FOR CONNECTION` 对 fast path 查询的输出
- 依赖 `EXPLAIN` 输出进行计划分析的外部工具和监控系统

**设计选择原因：** `JT_CONST` 在优化阶段即读取行（`join_read_const_table`），
fast path 复制此逻辑需要额外 handler 管理和错误处理。`JT_EQ_REF` 将行读取
推迟到执行阶段（`EQRefIterator::Read`），实现更简洁安全。

**正确性影响：** 无。查询结果完全一致。仅 EXPLAIN 展示的 access type 不同。

**MTR 覆盖：** `ps_point_plan_cache_fast_path.test` 包含 EXPLAIN 行为差异验证。

### 2. 参数类型检查范围有限

**现象：** Runtime guard（G8/G9）仅检查 `Item_param::data_type_actual()` 和
`Item_param::is_unsigned_actual()`，不检查以下细粒度属性：

| 属性 | 检查状态 | 发生 fallback 时的行为 |
|------|---------|----------------------|
| DECIMAL 精度/标度变化 | 未检查 | 类型仍为 `MYSQL_TYPE_NEWDECIMAL`，通过 guard → fast path 执行。`init_ref_part` 按当前精度写入 `key_buff`，`EQRefIterator` 用此值查找。若精度导致截断，行为与正常路径用截断后值查找一致。 |
| 字符集/排序规则变化 | 未检查 | V1 目标为整数 PK，字符集不适用。若扩展到字符串列，需在 guard 中检查 `Item_param::collation`。 |
| TIME/DATETIME 小数秒精度 | 未检查 | 与 DECIMAL 类似，`store_key::copy_inner()` 按列定义截断。行为一致。 |

**设计选择原因：** V1 目标工作负载为 sysbench `oltp_point_select`（整数 PK）。
上述边界情况在此场景下极少发生，且即使发生也不会产生错误结果——只是使用
当前参数值查找，与正常优化器执行相同的查找语义。

**未来改进方向（V2）：** 若覆盖范围扩展到更多数据类型，应在 admission 阶段
额外快照 `decimals`、`collation` 等属性，并在 runtime guard 中比对。

### 3. 不适用的 Guard（已评估排除）

以下 guard 在审查中被建议添加，经评估确认对 V1 范围不需要：

| 建议 Guard | 排除原因 |
|-----------|---------|
| G10: LOCK TABLES 检测 | `LOCK TABLES` 不改变单表唯一键查询的计划选择。EQRefIterator 的 `ha_index_read_map()` 尊重已有锁。 |
| G11: 事务隔离级别变化 | 对 `SELECT ... WHERE pk = ?`，优化器在任何隔离级别下都选择唯一键查找。不同隔离级别影响 MVCC 可见性（由存储引擎处理），不影响计划。 |

### 4. QEP_shared 字段默认值

以下 `QEP_shared` 字段在审查中被建议显式设置，经验证对 `JT_EQ_REF` 路径不需要：

| 字段 | 默认值 | 排除原因 |
|------|--------|---------|
| `prefix_tables_map` | 0 | 仅被 `DynamicRangeIterator` 读取（用于 `JT_ALL`/`JT_RANGE`） |
| `added_tables_map` | 0 | 同上 |
| `m_keys` | 空 `Key_map` | 不被 `EQRefIterator` 使用 |
| `m_records` | 0 | 用于优化器代价估算，fast path 跳过此阶段 |

`EQRefIterator` 构造函数仅接受 `TABLE*`、`Index_lookup*` 和 `ha_rows*`，
不读取 `QEP_TAB` 或 `QEP_shared`（`sql/iterators/ref_row_iterators.h:101-123`）。
