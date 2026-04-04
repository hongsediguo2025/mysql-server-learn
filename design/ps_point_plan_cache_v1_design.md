# PS Point Plan Cache V1 代码级设计

## 1. 文档目的

本文给出 `ps_point_plan_cache` 的 v1 代码级方案。该方案面向如下目标：

- 默认开启。
- 重点优化 `sysbench` 标准 prepared statement 只读模型中的 `oltp_point_select.lua` 点查语句，并将复合唯一键等值查询纳入 v1 支持范围。
- 在不同并发下力争让 `oltp_point_select` 获得显著收益；复合唯一键等值作为同类 supported shape，性能收益作为加分项，不单独设置硬性 KPI；对 `oltp_read_only` 整体不引入回退。
- 对其他场景不引入明显副作用，不导致性能下跌。
- 方案保持足够窄，不做通用 plan cache，不做跨连接共享，不做复杂淘汰和多计划管理。

## 2. 现状与问题

### 2.1 当前 Prepared Statement 的执行链路

MySQL 当前 prepared statement 是 per-connection 的，生命周期挂在 `THD::stmt_map`。

- `sql/sql_class.h`: `Prepared_statement_map`
- `sql/sql_class.h`: `THD::stmt_map`

Prepared statement 的主要执行链路如下：

1. `mysqld_stmt_execute()`
2. `Prepared_statement::execute_loop()`
3. `Prepared_statement::execute()`
4. `mysql_execute_command()`
5. `Sql_cmd_dml::execute()`
6. `Sql_cmd_dml::execute_inner()`
7. `Query_expression::optimize()`
8. `Query_block::optimize()`
9. `JOIN::optimize()`

也就是说，虽然 PS 已经缓存了解析树和参数信息，但每次执行仍然会重新走优化器。

### 2.2 为什么不能直接缓存整棵 JOIN

prepared statement 每次执行前都会清理 execution state，并重新做字段和表对象绑定：

- `LEX::clear_execution()`
- `Query_expression::clear_execution()`
- `Query_block::restore_cmd_properties()`

其中 `Query_block::restore_cmd_properties()` 明确要求 `join == nullptr`。这意味着：

- 不能在多次执行之间直接复用旧 `JOIN *`
- 不能跨执行保留旧 `TABLE *`
- 不能缓存带运行时指针的整棵旧 plan object

因此 v1 只能缓存“**最小计划模板**”，然后在每次命中时基于当前执行上下文快速重建一个最小 fresh plan。

## 3. v1 设计原则

### 3.1 默认 ON，但默认静默

v1 默认开启，但默认不积极介入。核心原则是：

- `bypass first, cache second`
- 非候选语句必须极早、极轻量地退出
- 只有极窄白名单才真正进入 plan cache fast path

### 3.2 只做 PS，不做文本 SQL

v1 只支持 binary protocol prepared statement：

- 支持 `COM_STMT_PREPARE / COM_STMT_EXECUTE`
- 不支持 SQL 语法 `PREPARE/EXECUTE`
- 不支持文本协议 SQL

这样可以把风险收敛在 `Prepared_statement` 链路内，避免影响普通文本 SQL。

### 3.3 只做单表唯一键点查

v1 只支持下面这一类语句：

- `SELECT`
- 单 query block
- 单 base table
- 无 join / 子查询 / derived / view / union / group / having / order / distinct / limit / window / FT
- 谓词为 PK/UK 等值（单列或复合键各列均为 `= ?`）

这正对齐 sysbench 的标准 `point_select` 模式，同时把同一类 `JT_EQ_REF` 路径上的复合唯一键等值查询一并纳入 v1。

### 3.4 只做 per-Prepared_statement 单槽位模板

v1 的 cache 直接挂在 `Prepared_statement` 对象内部：

- 每个 prepared statement 最多一个模板
- 不需要全局哈希表
- 不需要 LRU
- 不需要 eviction
- 不需要跨连接共享

这可以最大限度降低锁竞争、内存复杂度和副作用。

### 3.5 只支持 old optimizer

v1 只支持 traditional optimizer 路径：

- `LEX::using_hypergraph_optimizer() == false`

默认 `optimizer_switch` 并不启用 hypergraph optimizer，因此这不会影响默认 sysbench 场景。

## 4. 目标 workload 和连接假设

标准 sysbench `oltp_*.lua` prepared statement 模型具备以下特点：

- 线程级长连接
- 在 `thread_init()` 建连并 prepare
- 在 `thread_done()` 才断连
- 默认 `--reconnect=0`

因此 per-connection、per-PS 的单槽位模板方案天然适配 sysbench 标准模型，不会因为短连接频繁失效。

## 5. 总体结构

### 5.1 新能力名称

建议使用如下命名：

- sysvar: `ps_point_plan_cache`
- status:
  - `Ps_point_plan_cache_hits`
  - `Ps_point_plan_cache_admissions`
  - `Ps_point_plan_cache_invalidations`
  - `Ps_point_plan_cache_fallback_runtime`

### 5.2 状态机

在 `Prepared_statement` 上引入如下状态：

- `NEVER`
- `COLD`
- `HOT`
- `INVALID`

语义如下：

- `NEVER`: 经过静态分类后确认永远不是候选，后续执行始终快速 bypass
- `COLD`: 静态 shape 看起来可能是候选，但还没有 admission，第一次正常执行后再决定
- `HOT`: 已经成功 admission，后续执行可以尝试走 fast path
- `INVALID`: 曾经是 `HOT`，但运行时 guard 失败或元数据变化导致模板失效，后续只走普通路径，直到 reprepare 后重新分类

状态迁移如下：

- `prepare` 不符合 shape: `NEVER`
- `prepare` 符合 shape: `COLD`
- `COLD` 首次普通优化成功且满足 admission: `HOT`
- `COLD` 首次普通优化成功但不满足 admission: `NEVER`
- `HOT` 运行时结构 guard 失败: `INVALID`
- `INVALID` reprepare 成功后重新按 prepare 逻辑分类

## 6. 数据结构

建议在 `Prepared_statement` 中新增如下结构：

```cpp
enum class PsPointPlanState : uchar {
  NEVER = 0,
  COLD,
  HOT,
  INVALID
};

enum class PsCachedPlanType : uchar {
  POINT_EQ_REF = 0,   // Phase 1-6: WHERE pk = ? (or composite)
  RANGE_PK_BETWEEN,    // Phase 7+:  WHERE pk BETWEEN ? AND ?
};

static constexpr uint PS_PC_MAX_KEY_PARTS = 4;
static constexpr uint PS_PC_MAX_PARAMS = 4;

struct PsPointPlanTemplate {
  Table_ref *table_ref{nullptr};
  PsCachedPlanType plan_type{PsCachedPlanType::POINT_EQ_REF};
  uint param_count{0};
  Item_param *params[PS_PC_MAX_PARAMS]{};
  uint field_indices[PS_PC_MAX_PARAMS]{};
  enum_field_types actual_types[PS_PC_MAX_PARAMS]{};
  bool unsigned_actuals[PS_PC_MAX_PARAMS]{};
  uint keyno{MAX_KEY};
  uint key_parts{0};
  uint key_length{0};
  key_part_map null_rejecting{0};
  double best_read{0.0};
  double best_rowcount{1.0};
};
```

并在 `Prepared_statement` 私有成员中新增：

```cpp
PsPointPlanState m_ps_pc_state{PsPointPlanState::NEVER};
PsPointPlanTemplate m_ps_pc;
bool m_ps_pc_cursor_execution{false};
```

### 6.1 为什么只保存这些字段

`PsPointPlanTemplate` 只保留执行间稳定的信息：

- `Table_ref *`
- `Item_param *`
- key 元信息
- 运行时 guard 需要的参数类型信息
- 用于回填 `JOIN` 估算值的 `best_read/best_rowcount`

v1 不保存：

- `JOIN *`
- `TABLE *`
- `AccessPath *`
- `Field *`
- `QEP_TAB *`

这些对象都绑定于某一次具体执行，跨执行复用风险过高。

## 7. prepare 阶段的静态分类

### 7.1 挂点

在 `Prepared_statement::prepare()` 中，`prepare_query(thd)` 成功后、`m_lex->cleanup(true)` 前，新增一段分类逻辑：

- 只做 shape 判断
- 不分配额外复杂结构
- 不访问执行态对象

### 7.2 分类条件

静态分类只保留最严格的一类语句：

- `thd->variables.ps_point_plan_cache == true`
- `!is_sql_prepare()`
- `m_param_count >= 1 && m_param_count <= PS_PC_MAX_PARAMS`
- `m_lex->sql_command == SQLCOM_SELECT`
- `!m_lex->using_hypergraph_optimizer()`
- `unit->is_simple()`
- `query_block->leaf_table_count == 1`
- `query_block->outer_join == 0`
- `query_block->first_inner_query_expression() == nullptr`
- `!query_block->is_grouped()`
- `!query_block->is_distinct()`
- `!query_block->is_ordered()`
- `!query_block->has_limit()`
- `!query_block->has_windows()`
- `!query_block->has_ft_funcs()`
- `leaf_table` 必须是 base table，不能是 view / derived / table function / schema table
- `WHERE` 必须能识别成单个 `Item_func_eq(field, param)` 或 `Item_func_eq(param, field)`

如果不满足，直接置为 `NEVER`。

如果满足，则记录：

- `table_ref`
- `plan_type`（`POINT_EQ_REF` 或后续阶段的 `RANGE_PK_BETWEEN`）
- `param_count` 和 `params[]`
- `field_indices[]`

并置状态为 `COLD`。

### 7.3 静态分类为什么只做 shape

v1 不在 `prepare` 阶段尝试做最终 admission，原因有两个：

1. 最终是否是 `JT_EQ_REF` 取决于优化后的路径
2. 我们不希望在 `prepare` 阶段复制 optimizer 的复杂判定逻辑

因此 `prepare` 只做粗筛，真正 admission 延后到第一次普通优化成功之后。

## 8. fast path 的执行位置

### 8.1 推荐挂点

v1 的 fast path 不建议挂在 `Sql_cmd_dml::execute_inner()` 之前直接重建所有执行结构，而应挂在 old optimizer 的 `JOIN::optimize()` 早期。

原因：

- 进入 `JOIN::optimize()` 之前，open tables、restore/bind、privilege check 已经完成
- 此时 `Query_block::optimize()` 已经创建了 fresh `JOIN`
- 可以直接跳过最昂贵的 join planning 路径
- 不会破坏 `Prepared_statement::execute()` 的整体执行语义

### 8.2 插入点

建议在 `JOIN::optimize()` 内完成下面几步之后尝试 fast path：

- `set_optimized()`
- `tables_list = query_block->leaf_tables`
- `alloc_indirection_slices()`
- `ref_items[REF_SLICE_ACTIVE] = query_block->base_ref_items`

在这之后增加：

```cpp
if (try_apply_ps_point_plan_cache()) {
  set_plan_state(PLAN_READY);
  error = 0;
  return false;
}
```

### 8.3 为什么不是 `Sql_cmd_dml::execute_inner()`

`Sql_cmd_dml::execute_inner()` 的层次太高，如果在这里直接接管：

- 需要绕过 `Query_expression::optimize()` 的更多状态管理
- 更容易与 `Query_expression::create_access_paths()`、`set_optimized()`、`force_create_iterators()` 的既有约束打架

而在 `JOIN::optimize()` 里接管时，query block 和 join 的 fresh 生命周期是最自然的。

## 9. fast path 如何构造执行计划

### 9.1 核心思想

fast path 命中时，不复用旧 `JOIN`，也不复用旧 `AccessPath`，而是：

- 基于当前 fresh `JOIN`
- 手工构造一个最小的一表 `QEP_TAB`
- 使用现有 `init_ref()` / `init_ref_part()` 重新创建 `Index_lookup`
- 通过 `QEP_TAB::access_path()` 生成 `EQ_REF` 的 `AccessPath`
- 再走原生的 iterator 执行链

### 9.2 需要填充的 JOIN 状态

对于一表点查 fast path，需要把当前 `JOIN` 填到足以执行的最小状态：

- `tables = 1`
- `primary_tables = 1`
- `const_tables = 0`
- `best_rowcount = template.best_rowcount`
- `best_read = template.best_read`
- `where_cond = nullptr`
- `having_cond = nullptr`
- `qep_tab = new QEP_TAB[2]`

然后构造第一个 `QEP_TAB`：

- `set_join(this)`
- `set_idx(0)`
- `set_table(table_ref->table)`
- `table_ref = template.table_ref`
- `set_type(JT_EQ_REF)`

### 9.3 ref lookup 的初始化

不自己手写 handler 调用，而是复用现有 helper：

- `init_ref()`
- `init_ref_part()`

这样可以最大限度沿用既有的 ref key 构造逻辑，包括：

- key buffer
- null rejecting
- `store_key`
- `Item_param` 的运行时取值

### 9.4 AccessPath 的创建

构造好 `QEP_TAB::ref()` 后，通过现有 `QEP_TAB::access_path()` 生成一表路径。

`QEP_TAB::access_path()` 在 `JT_EQ_REF` 情况下会直接调用：

- `NewEQRefAccessPath()`

这比自己绕开执行器单独做 handler read 更安全、更一致。

### 9.5 Query_expression 层的状态

`JOIN::optimize()` fast path 成功后：

- `JOIN::m_root_access_path` 已经设置
- `Query_expression::create_access_paths()` 会在上层正常读取 `join->root_access_path()`
- `Query_expression::set_optimized()` 仍按原链路执行
- `Query_expression::force_create_iterators()` / `execute()` 也仍走原链路

因此 v1 不需要在 `JOIN::optimize()` 里直接干预 `Query_expression` 的公开状态。

## 10. 首次 admission 逻辑

### 10.1 admission 时机

`COLD` 语句第一次执行时完全走普通优化链路。  
只有当普通优化成功后，才尝试 admission。

### 10.2 admission 条件

admission 只接受最严格的成功样本：

- 当前 statement 仍处于 `COLD`
- `tables == 1`
- `primary_tables == 1`
- `const_tables == 0`
- `qep_tab != nullptr`
- `qep_tab[0].type() == JT_EQ_REF`
- `qep_tab[0].condition() == nullptr`
- `ref.key_parts == 1`
- `actual_key_flags(keyinfo) & HA_NOSAME`
- `actual_key_parts(keyinfo) == 1`
- `ref.items[0] == template.param`

只有全部满足，才把模板补全为 `HOT`。

### 10.3 admission 后保存的内容

首次 admission 成功后，保存：

- `keyno`
- `key_length`
- `null_rejecting`
- `best_rowcount`
- `best_read`
- `param->data_type_actual()`
- `param->is_unsigned_actual()`

随后：

- `COLD -> HOT`
- `Ps_point_plan_cache_admissions++`

### 10.4 admission 失败策略

如果第一次普通优化成功后仍不满足 admission 条件，则直接：

- `COLD -> NEVER`

原因是：

- 该语句 shape 虽然长得像候选，但真实执行路径不是 v1 目标
- 没必要在后续每次执行里继续尝试

## 11. HOT 运行期 guard

### 11.1 guard 原则

`HOT` 路径必须只做 O(1) 级别判断，不能重新遍历复杂语法树。

### 11.2 guard 内容

在尝试 fast path 前做以下 guard：

- `thd->variables.ps_point_plan_cache == false` -> bypass
- `owner() == nullptr` -> bypass
- `owner()->m_ps_pc_state != HOT` -> bypass
- `owner()->m_ps_pc_cursor_execution == true` -> bypass
- `thd->lex->using_hypergraph_optimizer()` -> bypass
- `template.table_ref == nullptr || template.table_ref->table == nullptr` -> `INVALID`
- `template.param == nullptr` -> `INVALID`
- `template.param->param_state() == NULL_VALUE` -> runtime fallback
- `template.param->data_type_actual()` 与 admission 记录不一致 -> runtime fallback
- `template.param->is_unsigned_actual()` 与 admission 记录不一致 -> runtime fallback
- 当前表的 `key_info[keyno]` 不再满足单列唯一键 -> `INVALID`
- 当前 field index 和模板不一致 -> `INVALID`

### 11.3 fallback 与 invalidate 的区别

- **runtime fallback**：本次参数或运行时条件不适合 fast path，但模板结构仍可能有效，下次可继续尝试
- **invalidate**：模板依赖的表结构或 key 结构已经变化，后续不应继续尝试

因此：

- 参数值异常、参数类型异常 -> fallback
- key 结构变化、table_ref 失效 -> invalidate

## 12. reprepare 和失效

### 12.1 不发明新的 invalidation 链路

metadata 变化和 `ER_NEED_REPREPARE` 继续走 MySQL 现有机制：

- `ask_to_reprepare()`
- `Prepared_statement::reprepare()`
- `LEX::check_preparation_invalid()`

v1 只在自身检测到模板结构失效时把状态置为 `INVALID`，然后回退普通路径。

### 12.2 swap_prepared_statement 要覆盖新增字段

`Prepared_statement::swap_prepared_statement()` 必须把下面这些一并 swap：

- `m_ps_pc_state`
- `m_ps_pc`
- `m_ps_pc_cursor_execution`

这样 reprepare 成功后新旧 statement 的状态能正确切换。

## 13. 系统变量与状态变量

### 13.1 sysvar

增加 session 级 bool：

- `ps_point_plan_cache`
- 默认值：`true`

v1 不建议一开始就做全局复杂配置，仅保留一个总开关作为 kill switch。

### 13.2 status 计数

建议只保留低冲突、必要的 4 个计数器：

- `hits`
- `admissions`
- `invalidations`
- `fallback_runtime`

不建议在 execute 热路径上对 `bypass_not_candidate` 大量计数，否则会让默认 ON 的 bypass 本身变成成本来源。

## 14. 测试要求

### 14.1 功能测试

至少覆盖以下 mtr 用例：

- 变量可见性和默认值
- candidate 点查 admission + hit
- 非候选 select 的永久 bypass
- SQL `PREPARE/EXECUTE` bypass
- hypergraph bypass
- `NULL` 参数 runtime fallback
- DDL 后 reprepare / invalidate
- cursor 场景 bypass

### 14.2 性能验收

至少用两组 sysbench：

- `oltp_point_select.lua`
- `oltp_read_only.lua`

线程建议至少覆盖：

- 1
- 8
- 32
- 64
- 128

同时要测负向回归：

- text SQL
- SQL PREPARE
- join-heavy
- range-heavy
- 非 PS 普通场景

要求默认 ON 下无明显回退。

## 15. 非目标

v1 明确不做：

- text SQL plan cache
- 通用 SQL 参数化
- 跨连接共享 cache
- 全局哈希表
- LRU / eviction
- 多 plan
- baseline / SPM
- range / order / distinct / aggregation 的缓存
- 复用旧 `JOIN` / 旧 `AccessPath`

> **注：** 模板结构体 `PsPointPlanTemplate` 使用了数组化设计（`params[]`、`field_indices[]`），
> 这是一项结构性预留，避免后续扩展时重构骨架代码。但 v1 的功能范围仅限于单表唯一键等值点查。
> range / aggregate 的扩展属于独立的后续工作，参见 `ps_plan_cache_v2_deep_caching_analysis.md`。

## 16. 预期收益与风险

### 16.1 收益来源

收益主要来自于跳过：

- `make_join_plan()`
- ref candidate 搜索
- best combination 计算
- QEP 生成的通用路径

对于 sysbench 的标准 point select，这部分是重复且稳定的，适合通过模板快速重建。

### 16.2 风险控制

风险控制依赖以下原则：

- 默认 ON，但默认静默
- 非候选在 prepare 阶段一次性打成 `NEVER`
- 只允许最小白名单进入 `COLD`
- `COLD` 首次必须走原优化器
- `HOT` 只接受极轻量 guard
- 任何不确定场景都立刻 fallback
- 结构变化才 invalidate

### 16.3 KPI 口径

v1 的性能 KPI 仍以 sysbench 标准 `oltp_point_select` 为主：

- 单列唯一键点查是主 KPI 场景
- 复合唯一键等值查询属于 v1 功能范围，但不额外承诺统一的性能提升百分比
- 如果复合唯一键等值在补充 micro-benchmark 中获得收益，视为 v1 的附加收益

## 17. 结论

v1 的正确姿势不是“通用 plan cache”，而是：

**一个默认开启、默认旁路、只在 Prepared Statement 的单表唯一键点查场景下生效的单槽位 fast path。**

这套方案对 sysbench 标准只读模型最有机会拿到收益，同时把默认开启带来的副作用和风险控制在最小范围内。

## 18. 后续演进方向（非 V1 范围）

V1 的模板结构采用了数组化设计，为后续扩展预留了结构空间。复合唯一键等值查询已纳入
V1 功能范围，但以下方向 **不属于 V1 的功能范围**，将作为独立的后续工作推进：

- PK range 查询（`WHERE id BETWEEN ? AND ?`）
- 聚合 + range（`SELECT SUM(k) ... BETWEEN`）
- `oltp_read_only` 全覆盖

详细技术分析见 `ps_plan_cache_v2_deep_caching_analysis.md`。
