# ps_point_plan_cache 深入讲解

> 面向“对 SQL 查询优化器还不太熟，但希望真正看懂这套实现”的工程师  
> 综合代码、design 文档、测试、benchmark、code review 记录，基于工作树状态 `2026-04-09`

## 1. 这份文档想解决什么问题

仓库里已经有不少 plan cache 设计文档，但它们各自解决的是不同问题：

- `design/ps_point_plan_cache_v1_design.md` 讲的是 v1 总体设计
- `design/plan_cache_v1_phase1_implementation.md` 到 `phase5` 讲的是分阶段实施
- `design/plan_cache_v1_1_design.md` 讲的是 V1.1 的健壮性和 `Index_lookup` 缓存
- `design/plan_cache_v1_2_design.md` 讲的是 V1.2 的更早 hook 和更深缓存
- `design/mysql_query_optimizer_architecture.md` 讲的是优化器全景
- `design/ps_plan_cache_v1_scope_expansion_analysis.md` 和 `design/ps_plan_cache_v2_deep_caching_analysis.md` 讲的是范围和未来演进

这些材料都很有价值，但如果你是第一次系统理解这套代码，会遇到两个困难：

1. 设计文档默认你已经知道 MySQL 优化器、`JOIN`、`QEP_TAB`、`AccessPath`、`Item_param` 这些对象是什么。
2. 历史 commit、当前代码、未提交工作树改动之间的“因果关系”不在一张图里。

这份文档的目标就是把这三件事揉成一条线：

- 先补足优化器背景
- 再讲当前 `ps_point_plan_cache` 的真实代码逻辑
- 最后按 commit 时间线解释它为什么一步步长成现在这样

如果只想先记住结论，可以先看下一节。

## 2. 先记住三句话

1. MySQL 的 Prepared Statement 只缓存了解析树和参数位，不缓存执行计划；所以每次 `EXECUTE` 正常都还会再跑一遍优化器。
2. `ps_point_plan_cache` 不是通用 plan cache，而是一个“极窄范围、挂在单个 `Prepared_statement` 上的单槽位模板缓存”。
3. 它的核心思想不是“把旧 plan 整棵复用”，而是“先学习一次稳定元数据，然后在后续执行里快速重建一个最小 fresh plan”。

这三句话几乎决定了整个设计。

## 3. 材料来源

本文综合了下面几类材料：

- 代码
  - `sql/ps_point_plan_cache.h`
  - `sql/ps_point_plan_cache.cc`
  - `sql/sql_prepare.h`
  - `sql/sql_prepare.cc`
  - `sql/sql_optimizer.cc`
  - `sql/sql_select.cc`
  - `sql/sql_executor.cc`
  - `sql/iterators/ref_row_iterators.cc`
- design 文档
  - `design/mysql_query_optimizer_architecture.md`
  - `design/ps_point_plan_cache_v1_design.md`
  - `design/plan_cache_v1_1_design.md`
  - `design/plan_cache_v1_2_design.md`
  - `design/ps_plan_cache_v1_scope_expansion_analysis.md`
  - `design/ps_plan_cache_v2_deep_caching_analysis.md`
- 测试
  - `mysql-test/t/ps_point_plan_cache_*.test`
- benchmark
  - `bench/ps_point_plan_cache/`
  - `bench/ps_point_plan_cache/results/20260406_114436/report.md`
- 本地 code review / 知识材料
  - `.code_review/ps_point_plan_cache_review.md`
  - `.code_review/plan_cache_code_review_dismiss_problem`
  - `.code_review/plan_cache_code_review_dismiss_problem2`
  - `.code_review/plan_cache_code_review_dismiss_problem2_response2`
  - `.code_review/mysql_query_result_cache_ecosystem_research.md`
- 外部知识库快照
  - `/Users/a1234/project/mysql_code_knowledge_base/README.md`
  - `/Users/a1234/project/mysql_code_knowledge_base/serena-status.md`
  - `/Users/a1234/project/mysql_code_knowledge_base/indexes/core_tree.txt`
  - `/Users/a1234/project/mysql_code_knowledge_base/indexes/core_counts.tsv`
  - `/Users/a1234/project/mysql_code_knowledge_base/indexes/core_hot_files.tsv`
  - `/Users/a1234/project/mysql_code_knowledge_base/indexes/serena_index_summary.json`
  - `/Users/a1234/project/mysql_code_knowledge_base/indexes/serena_root_symbols.jsonl`
  - `/Users/a1234/project/mysql_code_knowledge_base/indexes/tags/focused_cpp.tags`

关于这份知识库，需要特别说明两个事实：

1. 它的强项是“给出 MySQL 核心源码的稳定骨架地图”。
  也就是 `sql/`、`storage/innobase/`、`include/`、`mysys/`、`vio/` 这五大块的目录、热点文件、符号入口和语义索引。
2. 它并不等于“当前工作树的完整真相”。
  从知识库文件时间戳和索引内容看，这份快照生成于 2026-04-01/02 左右；而 `ps_point_plan_cache` 这条实现主线是在 2026-04-04 之后才逐步落地。  
   更直接的证据是：在知识库的 `source_manifest`、`serena_indexed_files`、`focused_cpp.tags` 里都搜不到 `ps_point_plan_cache`，说明它主要覆盖的是 **plan cache 落地前就已经存在的优化器/执行器/InnoDB 骨架**。

所以本文的写法，刻意采用三层叠加：

- 用知识库解释稳定的“老骨架”
- 用 commit 时间线解释“新设计是怎么长出来的”
- 用当前未提交代码解释“最后一层 correctness hardening 在补什么”

## 4. 先补优化器背景：一条 SQL 到底经历了什么

先别急着看 plan cache。要理解它，必须先知道一条 SQL 在 MySQL 里分哪几层。

### 4.1 SQL 的大生命周期

可以把一条 `SELECT` 想成 4 个阶段：

```text
SQL 文本
  -> Parse / Resolve
  -> Optimize
  -> Execute
  -> 返回结果
```

在仓库里的主调用链大致是：

```text
mysql_execute_command()
  -> Sql_cmd_dml::execute()
    -> Sql_cmd_dml::execute_inner()
      -> Query_expression::optimize()
        -> Query_block::optimize()
          -> JOIN::optimize()
      -> Query_expression::execute()
        -> CreateIteratorFromAccessPath()
        -> RowIterator::Read()
```

其中真正和 plan cache 强相关的核心入口有两个：

- `Prepared_statement::prepare()`：只做 prepare，不做 optimize/execute
- `JOIN::optimize()`：每次执行正常都要进来，这就是 plan cache 试图截断的地方

### 4.2 MySQL 里“plan”不是一个单独对象，而是四层表示

`design/mysql_query_optimizer_architecture.md` 里有一个非常关键的观点：MySQL 里的“计划”不是单个结构，而是分层存在的。

```text
Layer 1: 逻辑层
  Query_block + Table_ref + Item 条件树
  表示“要做什么”

Layer 2: 物理计划层
  Legacy 路径下主要是 JOIN_TAB / QEP_TAB / POSITION / Key_use
  表示“怎么做”

Layer 3: 统一计划层
  AccessPath 树

Layer 4: 运行时层
  RowIterator 树
```

这件事非常重要，因为它直接解释了为什么 v1 不能“直接缓存整个旧 plan”。

### 4.3 为什么不能直接缓存整棵旧 `JOIN`

这是理解整套设计的第一原则。

在 PS 场景下，每次执行前 MySQL 都会清执行状态并重新绑定表对象。design 文档明确指出：

- `Query_block::restore_cmd_properties()` 要求 `join == nullptr`
- `TABLE *` 是每次执行重新打开和重新绑定的
- `thd->mem_root` 上的很多计划对象在本次命令结束后就回收

所以：

- 旧 `JOIN *` 不能跨执行直接复用
- 旧 `QEP_TAB *` 不能不加处理地跨执行直接复用
- 旧 `AccessPath *` 和 `RowIterator *` 都绑定了当次执行对象

因此 v1 的根本策略只能是：

```text
不缓存整棵计划
而是缓存最小模板
然后在每次命中时，基于当前 fresh 执行上下文快速重建
```

## 5. Prepared Statement 为什么还需要 plan cache

很多人会自然地误以为：

> 既然是 Prepared Statement，那 plan 不就已经准备好了吗？

实际上不是。

Prepared Statement 在 MySQL 里缓存的是：

- `LEX`
- `Query_expression`
- `Query_block`
- `Item_param`
- 解析和 resolve 后的树

但每次 `COM_STMT_EXECUTE` 仍然会：

- 绑定当前参数值
- 重置执行状态
- 正常进入 `mysql_execute_command()`
- 再次走 `optimize`
- 再走 `execute`

这就是 plan cache 的动机。

对于非常简单的点查，比如：

```sql
SELECT c FROM sbtest1 WHERE id = ?
```

每次都完整跑一遍 `JOIN::optimize()`，明显是浪费。  
`ps_point_plan_cache` 要优化的就是这类“形状极稳定、参数值变化但访问路径几乎不变”的语句。

## 6. 这套实现到底是什么，不是什么

### 6.1 它是什么

当前实现是一个：

- per-Prepared-statement
- single-slot
- binary PS 优先
- legacy optimizer only
- 单表唯一键等值点查专用
- 以 correctness 为最高优先级的模板缓存

它缓存的是“模板”和部分可安全复用的 helper 对象，不是通用计划缓存系统。

从当前代码路径看，它的主要目标链路仍然是 `COM_STMT_PREPARE / COM_STMT_EXECUTE` 这类 binary protocol PS。`SQL PREPARE/EXECUTE` 在 classify 上可能经过同一套只读 shape 检查，但并不是这套 fast path 的主要优化目标。

### 6.2 它不是什么

它不是：

- 文本 SQL 的 plan cache
- 全局 plan cache
- 跨 session 共享的 cache
- 带 LRU / eviction 的复杂缓存系统
- 支持 join / 子查询 / group by / order by / range 的通用缓存
- hypergraph optimizer 的通用模板层

所以你必须把它理解成一个“针对单点瓶颈的内核级特化优化”，而不是“大而全的数据库计划缓存框架”。

## 7. 当前范围：哪些语句会被考虑，哪些不会

### 7.1 候选语句

当前代码和 design 一致，主要支持：

- `SELECT`
- 单 query block
- 单 base table
- 无 join
- 无子查询
- 无 derived / view / union
- 无 group by / having / order by / distinct / limit / window / FT
- `WHERE` 形状是 `field = ?` 或多个 `field = ?` 的 `AND`
- 最终优化器选出的是“完整覆盖唯一键”的访问路径

最典型的目标就是：

```sql
SELECT * FROM t WHERE pk = ?
SELECT * FROM t WHERE uk1 = ? AND uk2 = ?
```

### 7.2 明确排除

当前明确排除：

- 文本协议普通 SQL
- 复杂 SQL PREPARE 场景里的 fast path
- cursor 执行
- 分区表
- hypergraph optimizer
- range / BETWEEN
- 非唯一索引 ref
- 多表 join

这些排除不是“懒得做”，而是 v1/v1.1/v1.2 主动收窄风险的核心设计。

## 8. 关键对象解剖：名字、字段、关系，一次讲清楚

如果你对优化器不熟，最容易卡住的地方不是算法，而是“同一条 SQL 在不同层里到底长成什么对象”。  
`ps_point_plan_cache` 真正缓存的，不是一坨抽象的“计划”，而是把这些层里的信息拆开后，只缓存其中跨执行稳定、且能安全重建 fast path 的那一部分。

### 8.1 先分层：这些对象分别属于哪一层

先把全景图立住：


| 层次      | 典型对象                                                        | 它回答的问题               | plan cache 在这一层做什么                        |
| ------- | ----------------------------------------------------------- | -------------------- | ----------------------------------------- |
| 语法/语义层  | `Query_block`、`Item_param`、`Item_func_eq`                   | SQL 写了什么             | prepare 时看 WHERE 形状，做静态 classify          |
| 候选访问路径层 | `Key_use`                                                   | 哪些等值条件可以喂给哪些索引列      | 正常 optimizer 用它搜索；plan cache 不直接缓存它       |
| 代价比较层   | `POSITION`                                                  | 这些候选里谁更便宜            | 第一次执行靠 optimizer 做决策；plan cache 不缓存整块搜索状态 |
| 已选执行槽位层 | `QEP_shared`、`QEP_TAB`、`join_type`、`Index_lookup`           | 最终打算怎么访问表            | admission 从这里读出已选结论                       |
| 统一物理计划层 | `AccessPath`                                                | 最终生成哪个 `RowIterator` | HOT 时 fast path 直接构造最小 `AccessPath`       |
| 执行层     | `EQRefIterator`、`read_const()`、`construct_lookup()`、handler | 真正如何取行               | fast path 和 normal path 在这里会合             |


可以把它记成一句话：

```text
AST(Query_block/Item_param)
  -> 候选边(Key_use)
  -> 胜出结果(POSITION)
  -> 执行槽位(QEP_TAB + Index_lookup + join_type)
  -> 统一路径(AccessPath)
  -> 迭代器/handler
```

`ps_point_plan_cache` 的关键，不是跳过执行，而是跳过“从 AST 一路搜索到执行槽位”的那段优化开销。

### 8.2 `Prepared_statement`：plan cache 真正挂载的位置

plan cache 的宿主不是 `JOIN`，不是 `Query_block`，而是 `Prepared_statement`。  
这是整个设计最重要的第一性原理。

当前与本特性直接相关的字段是：

- `m_ps_pc_state`：状态机，当前是 `NEVER/COLD/HOT/INVALID`
- `m_ps_pc`：`PsPointPlanTemplate`，真正保存模板和缓存 helper
- `m_ps_pc_cursor_execution`：本次 execute 是否处在 cursor 模式
- `m_ps_pc_retryable_cold`：这次降回 `COLD` 之后，是否允许本轮正常优化后再尝试 admission

这意味着：

- 同一条 SQL 文本，如果 prepare 成两个不同的 PS，它们各自有独立 plan cache 状态。
- `JOIN` 是“每次执行临时创建/重建的运行时对象”，而 `Prepared_statement` 才是“跨多次 execute 持久存在的对象”。
- 所以能跨执行保存的东西，必须最终挂到 `Prepared_statement` 或它拥有的 statement arena 上。

### 8.3 `PsPointPlanTemplate`：plan cache 的核心模板对象

`PsPointPlanTemplate` 在 `sql/ps_point_plan_cache.h` 里定义，是本文最值得逐字段吃透的对象。  
当前数组上限来自 `PS_PC_MAX_PARAMS = 4`，也就是 v1 明确只覆盖很窄的小参数规模点查。

它大致分四组字段。

第一组是 prepare/classify 阶段的“静态形状”字段：

- `table_ref`
- `plan_type`
- `param_count`
- `params[]`
- `field_indices[]`

这组里最容易误解的是三个点。

第一，`table_ref` 不是 `TABLE`。  
它是 parse tree 里的 `Table_ref *`，跨 execute 稳定；而 `table_ref->table` 是每次执行重新 open 出来的 `TABLE *`，不稳定。

第二，`params[]` 存的是稳定的 `Item_param *` 指针。  
这就是 PS 能做这种缓存的根基之一：参数节点本身在 statement arena 上稳定存在，变化的是每次 execute 给它灌进去的值。

第三，`field_indices[]` 在 prepare 后和 admission 后语义不完全一样。  
prepare/classify 时它按 WHERE 里看到参数的顺序记录；admission 后会按最终 key part 顺序重排，并且由 `KEY_PART_INFO::fieldnr - 1` 重新推导。  
这一步非常关键，因为执行层真正关心的是“索引列顺序”，不是“SQL 文本里写条件的顺序”。

第二组是第一次正常执行后 admission 写入的“已选计划快照”：

- `actual_types[]`
- `unsigned_actuals[]`
- `actual_collations[]`
- `keyno`
- `key_parts`
- `key_length`
- `null_rejecting`
- `best_read`
- `best_rowcount`
- `optimizer_switch`
- `table_ref_version`
- `relevant_sql_mode`

这组字段回答的是：“第一次 admission 时，实际参数长什么样，optimizer 最终又选中了哪个唯一索引点查，以及当时依赖了什么环境前提。”

- `actual_types[]`：第一次 admission 时每个参数的 `data_type_actual()` 快照
- `unsigned_actuals[]`：第一次 admission 时每个整型参数的 unsigned 属性快照
- `actual_collations[]`：第一次 admission 时每个字符串参数的实际 collation 快照
- `keyno`：命中的索引号，对应 `TABLE::key_info[keyno]`
- `key_parts`：本次 lookup 实际用了几个 key part
- `key_length`：最终序列化出来的总 key 长度
- `null_rejecting`：哪个 key part 是严格 `=` 且 `NULL` 不可能匹配
- `best_read` / `best_rowcount`：第一次优化得到的代价和行数估计
- `optimizer_switch`：admission 当时相关 optimizer 开关快照
- `table_ref_version`：当时的 `TABLE_SHARE` 版本，配合 reprepare/metadata 检查
- `relevant_sql_mode`：当前实现里尤其要关注 `PAD_CHAR_TO_FULL_LENGTH` 之类会影响 key 比较语义的位

第三组是 V1.1 引入的 `Index_lookup` helper cache：

- `cached_key_buff`
- `cached_key_buff2`
- `cached_store_keys[]`
- `cached_to_fields[]`
- `cached_part_lengths[]`
- `cached_part_store_lengths[]`
- `ref_cached`

这组字段解决的是“每次 HOT fast path 都重新 new 一套 `store_key` / `Field clone` / key buffer 太贵”的问题。

- `cached_key_buff`：主 key buffer
- `cached_key_buff2`：`EQRefIterator` 的 secondary key buffer，用于比较这次 lookup 是否和上次相同
- `cached_store_keys[]`：每个 key part 对应一个 `store_key`
- `cached_to_fields[]`：`store_key` 里持有的 `Field` clone
- `cached_part_lengths[]` / `cached_part_store_lengths[]`：校验新元数据布局是否仍兼容 cached helper
- `ref_cached`：上面这整套缓存是否可直接复用

第四组是 V1.2 引入的 QEP skeleton cache：

- `cached_qep_tab`
- `cached_qep_shared`
- `cached_key_copy`
- `cached_ref_items`
- `cached_cond_guards`
- `qep_cached`

它的核心目的只有一个：避免 HOT 路径上每次都在 `thd->mem_root` 重建最小 `QEP_TAB` 和 `Index_lookup` 指针数组。

要点是：

- `cached_qep_shared` 里真正装着 `type/ref/table/position` 这一类共享槽位
- `cached_qep_tab` 是执行器看到的表槽位包装
- `cached_key_copy` 对应 `Index_lookup::key_copy`
- `cached_ref_items` 对应 `Index_lookup::items`
- `cached_cond_guards` 对应 `Index_lookup::cond_guards`
- `qep_cached` 代表这套骨架是否已经在 PS arena 上准备好

一句话总结：  
`PsPointPlanTemplate` 不是“整棵计划树”，而是“静态形状 + 已选索引元数据 + 可复用 helper + 可复用最小 QEP 骨架”的组合。

### 8.4 `Query_block` 与 `Item_param`：语法树层的稳定锚点

`Query_block` 是 query block 级别的逻辑对象。  
对 plan cache 来说最关键的成员是：

- `leaf_tables`
- `where_cond()`
- `having_cond`
- `join`

prepare 时的 `ps_point_plan_extract_where_shape()` 基本只在这个层次工作。  
它不关心成本，不关心最终索引号，它只关心：

- 是不是单表
- WHERE 是否是可识别的等值形状
- 参数是不是稳定的 `Item_param`
- 有没有 v1 明确排除的结构

`Item_param` 则是另一个核心锚点。  
它最重要的性质不是“表示参数”，而是“节点身份稳定、值可变”。

plan cache 围绕它关心的是：

- 当前值是否为 `NULL`
- `data_type_actual()` 是否变了
- unsigned 属性是否变了
- 实际 collation 是否变了

因此你可以这样理解：

- `Query_block` 负责告诉我们“这条 SQL 长什么样”
- `Item_param` 负责告诉我们“这次 execute 填进来的实参长什么样”

前者驱动 classify，后者驱动 runtime guard。

### 8.5 `Key_use`：优化器里的“候选等值边”

`Key_use` 定义在 `sql/sql_select.h`，它不是最终计划，而是 optimizer 在搜索阶段用的候选边。

完整字段可以按语义分成三组。

第一组是“这条边连的是谁”：

- `table_ref`：被查表，也就是拥有索引的一侧
- `val`：拿来做 lookup 的表达式，可能是常量、`Item_param`、外表字段，或者更复杂表达式
- `used_tables`：`val` 依赖哪些前驱表

第二组是“它想使用哪个索引、哪一段 key part”：

- `key`：索引号
- `keypart`：第几个 key part
- `optimize`：附加优化标记，如 `KEY_OPTIMIZE_EXISTS`、`KEY_OPTIMIZE_REF_OR_NULL`
- `keypart_map`：`keypart` 的 bitmap 形式
- `ref_table_rows`：这个 key value 的估计 fanout
- `null_rejecting`：`val IS NULL` 时是否可立即判定无匹配
- `cond_guard`：子查询 trigger condition guard，可开可关
- `sj_pred_no`：半连接相关编号

第三组是“随着 join prefix 不断变化而变化的搜索状态”：

- `bound_keyparts`
- `fanout`
- `read_cost`

`Key_use` 最好的直觉不是“索引条件”，而是“有向边”：

```text
值的提供者  --(可用于 keypart i 的等值条件)-->  被访问表的某个索引列
```

在单表点查里，`used_tables` 通常是 0，`val` 通常就是 `Item_param`。  
但即使在这种最简单场景里，`Key_use` 也仍然只是候选，不是最终执行对象。

一个很关键的层次关系是：

- `Key_use` 说的是“这个条件可以喂给某个索引列”
- `Index_lookup` 说的是“最终已经决定这么喂，并且已经准备好 key buffer 和 helper 了”

所以 plan cache 不直接缓存 `Key_use`，因为它太早、太候选、太依赖 join-order 搜索上下文。

### 8.6 `POSITION`：搜索阶段的“当前胜出结果”

`POSITION` 也是 `sql/sql_select.h` 里的核心结构。  
它记录“某张表在当前 join order 下，被选成怎样访问、成本是多少”。

完整字段可以分四组理解。

第一组是成本和行数：

- `rows_fetched`
- `read_cost`
- `filter_effect`
- `prefix_rowcount`
- `prefix_cost`

第二组是本表当前选中的访问方式：

- `table`
- `key`
- `ref_depend_map`
- `use_join_buffer`

这里最关键的是 `key`。  
它不是 `Index_lookup *`，而是一个 `Key_use *`，指向当前胜出的候选 key 信息。

第三组是半连接/LooseScan/FirstMatch/DupsWeedout 相关状态：

- `sj_strategy`
- `n_sj_tables`
- `dups_producing_tables`
- `first_loosescan_table`
- `loosescan_need_tables`
- `loosescan_key`
- `loosescan_parts`
- `first_firstmatch_table`
- `cur_embedding_map`
- `first_firstmatch_rtbl`
- `firstmatch_need_tables`
- `first_dupsweedout_table`
- `dupsweedout_tables`
- `sjm_scan_last_inner`
- `sjm_scan_need_tables`

第四组是 lateral 依赖：

- `m_suffix_lateral_deps`

对本文的一表点查而言，真正重要的是前两组。  
但理解第三、第四组的存在也很重要，因为这恰好说明为什么 plan cache 不能“整块缓存 POSITION”。

`POSITION` 是搜索过程的工作内存，夹杂了大量与 join prefix、半连接策略、外层依赖有关的临时状态。  
一表点查 fast path 不需要这些，所以只提取其中稳定、可证明安全的那一小部分结果。

### 8.7 `join_type`：`JT_EQ_REF`、`JT_CONST` 到底分别意味着什么

`join_type` 枚举定义在 `sql/sql_opt_exec_shared.h`。  
它描述的是“这张表最终要用哪种访问方法”。

完整枚举如下：

- `JT_UNKNOWN`：还没决定
- `JT_SYSTEM`：系统表/恰好一行
- `JT_CONST`：至多一行，并且在本次优化时就能当常量表处理
- `JT_EQ_REF`：对唯一索引做等值 lookup，对每个外层组合最多返回一行
- `JT_REF`：对非唯一索引做等值 lookup，可能返回多行
- `JT_ALL`：全表扫描
- `JT_RANGE`：范围扫描
- `JT_INDEX_SCAN`：扫描索引叶子
- `JT_FT`：全文索引
- `JT_REF_OR_NULL`：`ref` 外加对 `NULL` 的补充搜索
- `JT_INDEX_MERGE`：多个 range 结果做 merge

如果你习惯从 `EXPLAIN` 看世界，可以粗略把它们对应成：

- `JT_CONST` -> `type = const`
- `JT_EQ_REF` -> `type = eq_ref`
- `JT_REF` -> `type = ref`
- `JT_ALL` -> `type = ALL`
- `JT_RANGE` -> `type = range`

这里最需要深刻理解的是 `JT_CONST`、`JT_EQ_REF`、`JT_REF` 的差别。

`JT_REF` 的语义最宽。  
只要能拿一个 key value 去做等值索引 lookup，但结果可能有多行，它就是 `JT_REF`。

`JT_EQ_REF` 更强。  
它要求：最终选中的访问是“唯一索引 + 所有必要 key part 都被等值绑定”，于是对每个外层行组合，最多命中一行。  
所以 `eq_ref` 的本质不是“用了索引”，而是“用了能证明至多一行的唯一等值访问”。

`JT_CONST` 又比 `JT_EQ_REF` 更特殊。  
它表示这张表在当前优化上下文里已经可以当常量表提前读掉，读出来的列值后面都能像常量一样使用。  
对于单表 `SELECT ... WHERE pk = ?` 这类 PS，在第一次正常 `EXECUTE` 中，optimizer 很可能把它看成 `JT_CONST`，因为参数在 execute 时已经有值，且没有外表依赖。

这就是本文最经典、也最容易误解的一点：

- 第一次正常执行 admission 看见的常常是 `JT_CONST`
- HOT fast path 手工构造的却是 `JT_EQ_REF`

这不是矛盾，而是两个层次的不同工程选择。

- `JT_CONST` 更像“优化阶段就先把这行读出来”
- `JT_EQ_REF` 更像“把 executor 需要的唯一点查槽位搭好，让执行阶段去读”

Phase 3 design 明确选择 fast path 用 `JT_EQ_REF`，原因是它更简单、更稳妥，不必重演 const-table 的预读和特殊 handler 生命周期。

### 8.8 `KEY` / `KEY_PART_INFO`：静态索引元数据长什么样

如果说 `Index_lookup` 是“本次 lookup 的运行时实例”，那么 `KEY` 和 `KEY_PART_INFO` 就是“表定义里的静态索引蓝图”。

`KEY` 的重要字段是：

- `key_length`：整个 key 的总长度
- `flags`
- `actual_flags`
- `user_defined_key_parts`
- `actual_key_parts`
- `unused_key_parts`
- `usable_key_parts`
- `block_size`
- `algorithm`
- `is_algorithm_explicit`
- `parser`
- `parser_name`
- `key_part`
- `name`
- `rec_per_key`
- `is_visible`
- `table`
- `comment`

其中最重要的是：

- `user_defined_key_parts`：用户真正定义的 key part 数
- `actual_key_parts`：包括隐藏扩展列后的实际 key part 数
- `key_part`：指向 `KEY_PART_INFO[]`

对 point plan cache，尤其需要理解为什么代码里经常看 `user_defined_key_parts`。  
因为 admission 和 fast path 都只想处理“用户语义上真正参与唯一判定的那几个 key part”，不希望把隐藏附加 part 混进来，破坏模板的稳定性。

`KEY_PART_INFO` 的完整字段是：

- `field`
- `offset`
- `null_offset`
- `length`
- `store_length`
- `fieldnr`
- `key_part_flag`
- `type`
- `null_bit`
- `bin_cmp`

其中三个字段特别关键。

第一，`fieldnr`。  
这是 1-based 的字段编号，所以模板里保存时总会做 `fieldnr - 1` 变成 0-based `field_indices[]`。

第二，`length`。  
它表示 key part 纯值部分的字节长度，不包含 NULL 标志和变长额外长度字节。

第三，`store_length`。  
它表示真正塞进 key buffer 时需要占多少字节，可能比 `length` 更大，因为还要算上：

- NULL 标志字节
- 变长列的长度字节

当前工作树里 helper compatibility 检查从“只看 field 编号”升级为“连 `length/store_length` 布局也看”，本质上就是把这个区别当成 correctness 边界认真对待了。

### 8.9 `Index_lookup`：最终执行点查时最核心的运行时对象

`Index_lookup` 定义在 `sql/sql_opt_exec_shared.h`。  
它是“最终已经决定使用某个索引 lookup 后，执行器真正拿来干活的对象”。

完整字段如下：

- `key_err`
- `key_parts`
- `key_length`
- `key`
- `key_buff`
- `key_buff2`
- `key_copy`
- `items`
- `cond_guards`
- `null_rejecting`
- `depend_map`
- `null_ref_key`
- `use_count`
- `disable_cache`
- `keypart_hash`

逐个理解它们。

`key_err`：最近一次构造 key 是否失败。  
`EQRefIterator::Read()` 每次都会先调用 `construct_lookup()` 重新用当前参数值构 key；如果失败，`key_err` 为真，本次 lookup 直接不读行。

`key_parts`：本次 lookup 实际绑定了几个 key part。  
对于复合唯一键 `(a,b)` 的点查，它通常是 2。

`key_length`：当前 key buffer 的总长度。

`key`：索引号，对应 `TABLE::key_info[key]`。

`key_buff`：真正送给 handler 的序列化 key bytes。  
它不是抽象条件，而是已经按每个 key part 的存储布局拼好的二进制 buffer。

`key_buff2`：secondary key buffer。  
`EQRefIterator` 用它实现“一行缓存”：如果这次构出的 key 和上次一样，就不必重复下钻 handler 读取同一行。

`key_copy`：`store_key *` 数组。  
每个 key part 对应一个 helper，负责把当前 `Item` 的值拷进 `key_buff` 的相应位置。  
如果某个位置是常量且不需要重复求值，则对应元素可以为空。

`items`：每个 key part 当前对应的 `Item *`。  
在我们的场景里通常就是若干 `Item_param *`。

`cond_guards`：triggered condition guard 数组。  
主要服务于子查询里可开关的条件；在本文的一表点查主路径中通常为空，但 cached QEP 仍需把它的数组形状准备好。

`null_rejecting`：bitmap。  
第 `i` 位为 1 表示第 `i` 个 key part 是严格 `=` 语义，若对应 `items[i]` 为 `NULL`，则可直接判定“不可能有匹配”。  
`impossible_null_ref()` 就是用它做快速判空。

`depend_map`：当前 lookup 依赖哪些前驱表。  
单表点查一般是 0；多表 `eq_ref` 时它反映参数来自哪些外表。

`null_ref_key`：`REF_OR_NULL` 访问时表示“查 NULL”的标志位位置。  
对我们的 `POINT_EQ_REF` 场景通常不活跃。

`use_count`：当前行被 join executor 使用的次数，和 unlock 行时机相关。

`disable_cache`：即使 key 相同，也不能安全复用上一行缓存。  
典型原因是 Index Condition Pushdown 等场景可能让“同 key 不一定等价于同结果”。

`keypart_hash`：把所有 key part 做 hash 的变体支持，主要给某些半连接/唯一约束实现使用。  
对本文主路径通常不活跃。

对于第 `i` 个 key part，可以把几个对象的一一对应关系记成：

```text
KEY::key_part[i]         -> 目标列的静态布局定义
Index_lookup::items[i]   -> 当前执行的值来源(Item_param/常量/表达式)
Index_lookup::key_copy[i]-> 如何把这个值序列化进去(store_key)
Index_lookup::key_buff   -> 最终落地的二进制 key bytes
```

更细一点说：

- `KEY_PART_INFO` 决定“这一段应该长什么样”
- `Item_param` 决定“这次要写进去什么值”
- `store_key` 决定“怎么把这个值按目标列语义写进去”
- `key_buff` 则是“写完后的结果”

`Index_lookup` 还自带两个很重要的 helper 语义：

- `impossible_null_ref()`：结合 `null_rejecting` 和 `items[]` 判断这次是否可直接得出“不可能命中”
- `has_guarded_conds()`：判断是否存在 triggered condition guard，从而决定 ref access 是否可能被动态关掉

最重要的认识是：

- `KEY/KEY_PART_INFO` 是表定义的静态蓝图
- `Key_use` 是 optimizer 的候选边
- `Index_lookup` 是最终执行 lookup 的运行时实例

它们绝不是一个层次的东西。

### 8.10 `store_key`：把参数值真正序列化进 key buffer 的 helper

`store_key` 定义在 `sql/sql_select.h`。  
它不是“索引元数据”，而是“把当前 Item 值写进 key buffer 某一段”的执行 helper。

它的关键字段和接口是：

- `null_key`
- `to_field`
- `item`
- `copy()`
- `store_field()`
- `copy_inner()`

含义分别是：

- `null_key`：这次复制后，该 key part 是否为 NULL
- `to_field`：目标 `Field`，通常是按目标索引列布局 clone 出来的 field
- `item`：数据来源，通常就是 `Item_param`
- `copy()`：带好错误/告警语义后，调用真正的 `copy_inner()`
- `store_field()`：返回内部持有的目标 `Field *`
- `copy_inner()`：实际做类型转换和字节写入

它还有一个派生类 `store_key_hash_item`，额外维护：

- `hash`

用于“写入后顺便计算 hash”这类场景。

为什么 `store_key` 在 V1.1/V1.2 如此关键？  
因为真正贵而且脆弱的，不只是“再跑一次优化器”，还包括每次 HOT fast path 都重新建这些 helper：

- 要重新分配 `store_key`
- 要重新 clone 与 key part 类型一致的 `Field`
- 要重新准备 key buffer 布局

因此 V1.1 开始把它们搬进 PS arena 做缓存。  
但一旦缓存，就立刻引入另一个问题：新一次 execute 打开的 `TABLE`、新的 `KEY_PART_INFO` 布局，是否还和缓存 helper 兼容。  
当前工作树里新增的大量 compatibility 检查，正是在给这件事补保险。

### 8.11 `QEP_shared` / `QEP_TAB`：已选执行槽位的载体

`QEP_shared` 是一个很容易被忽视、但对理解 fast path 非常关键的对象。  
它保存 `JOIN_TAB` 和 `QEP_TAB` 共有的执行槽位信息。

`QEP_shared` 的完整核心字段是：

- `m_join`
- `m_idx`
- `m_table`
- `m_position`
- `m_sj_mat_exec`
- `m_first_sj_inner`
- `m_last_sj_inner`
- `m_first_inner`
- `m_last_inner`
- `m_first_upper`
- `m_ref`
- `m_index`
- `m_type`
- `m_condition`
- `m_condition_is_pushed_to_sort`
- `m_keys`
- `m_records`
- `m_range_scan`
- `prefix_tables_map`
- `added_tables_map`
- `m_ft_func`
- `m_skip_records_in_range`

对单表点查最关键的是：

- `m_table`：当前执行真正打开的表对象
- `m_position`：指回 optimizer 选出的 `POSITION`
- `m_ref`：也就是 `Index_lookup`
- `m_type`：也就是 `JT_CONST/JT_EQ_REF/JT_REF/...`
- `m_range_scan`：range 场景用；本文主路径通常为空

`QEP_shared_owner` 则是一个薄包装器。  
`JOIN_TAB` 和 `QEP_TAB` 都继承它，并把 `type()/ref()/table()/position()` 这些访问转发给内部同一个 `QEP_shared`。

这个设计意味着：

- `JOIN_TAB` 更偏优化阶段视角
- `QEP_TAB` 更偏执行阶段视角
- 但它们对“这张表最终怎么访问”的核心结论，其实共享一份底层槽位

这正是 V1.2 能缓存最小 `QEP_shared + QEP_TAB` 骨架的根本原因。

`QEP_TAB` 本身很大，包含 filesort、weedout、tmp table、semijoin 等许多字段。  
对本文的一表点查路径，我们优先关注这些成员/行为：

- `table_ref`
- `type()`
- `ref()`
- `table()`
- `position()`
- `use_order()`
- `access_path()`

其中 `access_path()` 最关键。  
它会按 `type()` 分派：

- `JT_CONST` -> `NewConstTableAccessPath(...)`
- `JT_EQ_REF` -> `NewEQRefAccessPath(...)`
- `JT_REF` -> `NewRefAccessPath(...)`
- `JT_REF_OR_NULL` -> `NewRefOrNullAccessPath(...)`

所以你可以把 `QEP_TAB` 看成“把已选槽位转成统一物理路径的桥接点”。

### 8.12 `AccessPath`：统一计划层，最终通向 `RowIterator`

`AccessPath` 是新旧优化器共享的统一计划层对象。  
legacy optimizer 最终也会把结果转成它，再去生成迭代器。

它有两类信息。

第一类是通用元信息：

- `type`
- `safe_for_rowid`
- `count_examined_rows`
- `cost`
- `init_cost`
- `init_once_cost`
- `num_output_rows`
- `filter_predicates`
- `delayed_predicates`
- `parameter_tables`

第二类是按 `type` 选择的 union 分支。  
对本文最重要的几个分支，字段如下：

- `ref`：`table`、`ref`、`use_order`、`reverse`
- `ref_or_null`：`table`、`ref`、`use_order`
- `eq_ref`：`table`、`ref`
- `const_table`：`table`、`ref`

这就能看出一个关键事实：

- `AccessPath` 不是在重新表达 SQL 条件
- 它只是把“这张表用哪种访问方法、依赖哪个 `Index_lookup`”编码成统一节点

所以 plan cache fast path 并不是直接“缓存 AccessPath 树”。  
它做的是：

1. 先确认模板和当前执行环境仍兼容
2. 再把最小 `QEP_TAB`/`Index_lookup` 槽位快速搭好
3. 然后调用同样的 `QEP_TAB::access_path()` 生成 `AccessPath`

也就是说，缓存的仍是更底层、可证明更稳定的构件，而不是最上层的整棵 path 树。

### 8.13 `JOIN`：为什么 plan cache 的目标是绕过它的大段 optimize

`JOIN` 是 legacy optimizer 的中心协调器。  
它既连着 query block 语义，也连着最终 QEP 和 execution path。

对本文最重要的直觉是：

- `JOIN::optimize()` 前半段会做大量条件处理、候选路径生成、代价计算
- `ps_point_plan_cache` 的收益主要来自跳过这一段
- 但后面的执行器、handler、真正取行的成本并没有被跳过

这也是为什么 bench 里收益通常是“明显但不会夸张到数量级变化”的原因。

### 8.14 把关系串起来：从 `JT_EQ_REF` 到 `Index_lookup` 到执行器

把上面这些对象连起来，单表点查的 normal path 可以抽象成：

```text
WHERE t.pk = ?
  -> Query_block / Item_func_eq / Item_param
  -> Key_use(候选：这个参数可绑定到 keypart 0)
  -> POSITION(胜出：这个索引最便宜)
  -> create_ref_for_key()
  -> QEP_shared.m_ref = Index_lookup
  -> QEP_shared.m_type = JT_CONST 或 JT_EQ_REF
  -> QEP_TAB::access_path()
  -> AccessPath::CONST_TABLE 或 AccessPath::EQ_REF
  -> read_const() 或 EQRefIterator::Read()
  -> construct_lookup()
  -> store_key::copy()
  -> handler::ha_index_read_map()
```

而 plan cache 的三阶段，则是在不同层抓不同信息：

```text
PREPARE/classify
  抓的是 Query_block/Item_param 层的“静态形状”

第一次 EXECUTE/admission
  抓的是 QEP_TAB/Index_lookup/join_type 层的“已选结果”

后续 HOT EXECUTE/fast path
  用模板重建最小 QEP_TAB + Index_lookup + AccessPath
  然后在 construct_lookup()/handler 这一层与 normal path 会合
```

这也是为什么你必须分清 `Key_use`、`Index_lookup`、`AccessPath`：

- `Key_use` 是“可能怎么走”
- `Index_lookup` 是“已经决定这么走，并且把 key 构造规则准备好了”
- `AccessPath` 是“把这件事包装成统一物理计划节点”

如果把这三层混成一个概念，就会永远看不懂 plan cache 到底缓存了什么。

### 8.15 一张总地图：plan 里的关键对象、关键成员、子目标、缓存方式

如果你想把整套 plan 看成“为了一次唯一键点查而协同工作的对象网络”，下面这张表最值得反复看。


| 层次           | 关键对象                                          | 最该盯住的成员                                                                                                                                      | 它承担的子目标                                    | 和谁衔接                                                                                       | 在 plan cache 里怎么处理                                                                   |
| ------------ | --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ |
| PS 宿主层       | `Prepared_statement`                          | `m_ps_pc_state`、`m_ps_pc`、`m_ps_pc_cursor_execution`、`m_ps_pc_retryable_cold`                                                                | 保存跨执行状态，决定当前是 `NEVER/COLD/HOT` 哪一种生命周期     | 上接 prepare/execute/reprepare 主循环，下接 `PsPointPlanTemplate`                                  | 这是 plan cache 真正挂载点，本身就是缓存宿主                                                         |
| 模板层          | `PsPointPlanTemplate`                         | `table_ref`、`plan_type`、`params[]`、`field_indices[]`、`keyno`、`key_parts`、`key_length`、`null_rejecting`、`actual_types[]`、`optimizer_switch` 等 | 把“这条 SQL 的静态形状”和“第一次执行选出的稳定计划元数据”固化下来      | 上接 `Prepared_statement`，下接 runtime guard / fast path builder                               | 这是 plan cache 的核心缓存对象                                                                |
| 语法树层         | `Query_block`                                 | `leaf_tables`、`where_cond()`、`having_cond`、`join`                                                                                            | 回答“SQL 写了什么”，让 classify 知道是否是支持的一表点查形状     | 上接 parser/prepare，下接 `ps_point_plan_extract_where_shape()`                                 | 不直接整块缓存；只抽取 `Table_ref *` 和稳定参数位信息                                                   |
| 参数层          | `Item_param`                                  | 当前值、`data_type_actual()`、unsigned 属性、实际 collation                                                                                            | 回答“这次 execute 填进来的实参长什么样”                  | 上接执行参数绑定，下接 `store_key` / runtime guard                                                    | `Item_param *` 本身缓存；参数值不缓存，只在每次 execute 读取当前值                                        |
| 候选条件层        | `Key_use`                                     | `table_ref`、`val`、`used_tables`、`key`、`keypart`、`null_rejecting`、`cond_guard`、`bound_keyparts`、`fanout`、`read_cost`                          | 把 WHERE/ON 中可用于索引等值访问的条件组织成候选边             | 上接 `update_ref_and_keys()`，下接 `POSITION` / `find_best_ref()`                               | 不缓存；它是搜索阶段的候选工作集                                                                     |
| 成本胜出层        | `POSITION`                                    | `rows_fetched`、`read_cost`、`filter_effect`、`prefix_rowcount`、`prefix_cost`、`key`、`ref_depend_map`                                            | 在当前 join prefix 下记录“哪条候选边胜出、代价多少”          | 上接 `best_access_path()` / `find_best_ref()`，下接 `QEP_TAB`                                   | 不缓存；它是带 join-prefix 状态的搜索内存                                                          |
| 静态索引蓝图层      | `KEY` / `KEY_PART_INFO`                       | `user_defined_key_parts`、`actual_key_parts`、`key_part[]`；`fieldnr`、`length`、`store_length`、`field`                                           | 定义目标索引列的静态布局，告诉执行层 key part 应该长什么样         | 上接 `TABLE::key_info[]`，下接 `create_ref_for_key()` / helper compatibility                    | 不整块缓存复制；只缓存 `keyno/key_parts/field_indices/length/store_length/null_rejecting` 等稳定投影 |
| 运行时 lookup 层 | `Index_lookup`                                | `key`、`key_parts`、`key_length`、`key_buff`、`key_buff2`、`key_copy`、`items`、`cond_guards`、`null_rejecting`、`depend_map`、`disable_cache`         | 把“选中哪个索引、当前值怎么变成 lookup key”落成可执行结构        | 上接 `create_ref_for_key()` 或 fast path builder，下接 `construct_lookup()` / iterator / handler | 不直接缓存整个旧对象；缓存其稳定组成件和 helper，再在 HOT 执行中快速重建                                           |
| 序列化 helper 层 | `store_key`                                   | `to_field`、`item`、`null_key`、`copy()`                                                                                                        | 把当前 `Item_param` 值按目标列语义写进 key buffer 的某一段 | 上接 `Index_lookup::key_copy[]`，下接 `construct_lookup()`                                      | V1.1 起缓存 `store_key` 和 `Field clone`，并在每次执行 re-bind 当前 `TABLE *`                     |
| 已选执行槽位层      | `QEP_shared` / `QEP_TAB`                      | `m_table`、`m_position`、`m_ref`、`m_type`、`m_range_scan`；以及 `QEP_TAB::type()/ref()/table()/access_path()`                                      | 承载“最终这张表怎么访问”的结论，并负责把该结论交给执行层              | 上接 optimizer 选出的结果，下接 `AccessPath`                                                         | V1.2 起缓存最小 `QEP_shared + QEP_TAB` 骨架，但每次执行仍要修补 fresh `TABLE *` 等成员                   |
| 统一物理计划层      | `AccessPath`                                  | `type`、`cost`、`num_output_rows`，以及 `eq_ref/ref/const_table` 等 union 分支                                                                       | 把已选访问方法包装成统一物理计划节点                         | 上接 `QEP_TAB::access_path()`，下接 `CreateIteratorFromAccessPath()`                            | 不缓存；按次构造                                                                             |
| 迭代器执行层       | `EQRefIterator` / `read_const()`              | `Read()`、`construct_lookup()` 调用点                                                                                                            | 真正发起唯一点查                                   | 上接 `AccessPath`，下接 handler                                                                 | 不缓存；按次构造/调用                                                                          |
| 引擎边界层        | `handler` / `ha_innodb` / `row_search_mvcc()` | `ha_index_read_map()`、InnoDB row search 入口                                                                                                   | 真正下钻存储引擎、做 MVCC 可见性和行读取                    | 上接 executor，下接 InnoDB 存储层                                                                  | 不缓存；一直是 fresh 执行成本                                                                   |


如果把这张表压缩成一句话，就是：

> plan 的大目标是“把一条 SQL 变成一次正确的唯一键读取”；  
> 每一层对象都只负责这个大目标里的一个子目标；  
> plan cache 只缓存那些“跨执行稳定且可证明安全”的子目标结果，不缓存整个旧计划树。

### 8.16 这些对象之间的关联关系，到底怎样一步步完成子目标

把它按“子目标分解”来看会更清楚。

#### 子目标 A：识别“这条 SQL 值不值得做 plan cache”

用到的对象是：

- `Prepared_statement`
- `Query_block`
- `Item_param`
- `PsPointPlanTemplate`

它们的协作方式是：

```text
Prepared_statement::prepare
  -> Query_block / WHERE AST
  -> 提取稳定的 Table_ref 和 Item_param
  -> 写入 PsPointPlanTemplate 的 classify 字段
  -> 状态进入 COLD 或 NEVER
```

这个阶段达成的目标不是“确定用哪个索引”，而是：

- 确认它是不是支持的一表点查形状
- 找到稳定参数位
- 建立后续能复用的模板骨架

#### 子目标 B：从条件里提取“哪些索引列可以被等值绑定”

用到的对象是：

- `Key_use`
- `KEY`
- `KEY_PART_INFO`

它们的协作方式是：

```text
WHERE 条件
  -> update_ref_and_keys()
  -> 生成一批 Key_use 候选边
  -> 每条边都指向某个 KEY 的某个 key part
```

这个阶段达成的目标是：

- 把逻辑条件翻译成“可喂给索引的候选等值边”
- 明确每个候选边对应哪个索引、哪个 key part

plan cache 不缓存这层，因为它还只是“候选空间”。

#### 子目标 C：在候选边里选出“当前最便宜且满足唯一点查语义”的那条路径

用到的对象是：

- `POSITION`
- `Key_use`
- `join_type`

它们的协作方式是：

```text
Key_use candidates
  -> Optimize_table_order::find_best_ref()
  -> Optimize_table_order::best_access_path()
  -> POSITION 记录胜出候选和代价
  -> 最终决定这张表是 JT_CONST / JT_EQ_REF / JT_REF / ...
```

这个阶段达成的目标是：

- 不再停留在“可能怎么走”
- 而是选出“本次真的打算怎么走”

plan cache 不缓存整个 `POSITION`，因为它携带大量 join 搜索上下文；  
但会在 admission 时把其中稳定的胜出结果投影到模板里。

#### 子目标 D：把“胜出结论”落成执行期真正可用的 lookup 结构

用到的对象是：

- `KEY` / `KEY_PART_INFO`
- `Index_lookup`
- `store_key`
- `QEP_shared`

它们的协作方式是：

```text
胜出的 Key_use / POSITION
  -> create_ref_for_key()
  -> init_ref() / init_ref_part()
  -> 生成 Index_lookup
  -> 把 lookup 挂进 QEP_shared.m_ref
```

这个阶段达成的目标是：

- 明确“用哪个索引”
- 明确“用几个 key part”
- 明确“每个 key part 的值从哪个 Item 来、如何写进 key buffer”

plan cache 对这层最看重，所以缓存也最深：

- 缓存 `keyno/key_parts/key_length/null_rejecting`
- 缓存参数与 key part 的顺序映射
- 缓存 `store_key` / `Field clone` / key buffer
- 当前 WIP 继续补的是这套 helper 的 compatibility 和 re-bind 安全性

#### 子目标 E：把 lookup 结构包装成执行器能消费的 plan 节点

用到的对象是：

- `QEP_TAB`
- `QEP_shared`
- `AccessPath`

它们的协作方式是：

```text
QEP_shared.m_type + m_ref + m_table
  -> QEP_TAB::access_path()
  -> NewEQRefAccessPath / NewConstTableAccessPath / ...
  -> AccessPath
```

这个阶段达成的目标是：

- 把 optimizer 的内部结论变成 executor 可统一处理的物理节点

plan cache 在这层只缓存“最小骨架”，不缓存最终 `AccessPath *`：

- `cached_qep_shared`
- `cached_qep_tab`
- `cached_key_copy`
- `cached_ref_items`
- `cached_cond_guards`

原因是：

- `AccessPath *` 绑定当前执行内存和当前 `TABLE *`
- 真正稳定的是其更底层的槽位和指针数组形状

#### 子目标 F：把当前 execute 的实参真正落地成一次索引读取

用到的对象是：

- `Index_lookup`
- `store_key`
- `EQRefIterator`
- `handler`
- `row_search_mvcc`

它们的协作方式是：

```text
Index_lookup
  -> construct_lookup()
  -> store_key::copy()
  -> key_buff 形成当前实参对应的二进制 key
  -> EQRefIterator::Read() / read_const()
  -> handler::ha_index_read_map()
  -> row_search_mvcc()
```

这个阶段达成的目标是：

- 用“本次 execute 的当前参数值”真正读到那一行

这里 plan cache 不缓存“结果行读取”本身，只缓存为了更快走到这一步所需的稳定模板和 helper。

### 8.17 从“缓存视角”再看一遍：每个对象到底缓存了什么，没缓存什么

很多人第一次读到这里，会以为 plan cache 是把下面整条链都缓存了：

```text
Query_block -> Key_use -> POSITION -> QEP_TAB -> AccessPath -> Iterator
```

其实不是。  
更准确的说法是：它按层做“选择性缓存”。

#### 第一类：直接缓存稳定身份

- `Table_ref *`
- `Item_param *`

原因：它们在 PS 生命周期内身份稳定，是后续所有映射的锚点。

#### 第二类：缓存稳定元数据投影，而不是整对象

- 从 `KEY/KEY_PART_INFO` 投影出 `field_indices[]`
- 从第一次胜出路径投影出 `keyno/key_parts/key_length/null_rejecting`
- 从第一次 admission 投影出 `actual_types[]/unsigned/collation`
- 从环境中投影出 `optimizer_switch/table_ref_version/relevant_sql_mode`

原因：这些东西稳定且 guard 可验证，但原始对象本身要么太大，要么带执行期/搜索期状态。

#### 第三类：缓存可复用 helper

- `cached_key_buff`
- `cached_key_buff2`
- `cached_store_keys[]`
- `cached_to_fields[]`

原因：这批对象构造频繁、成本可观、并且在证明布局兼容后可跨执行复用。

#### 第四类：缓存最小执行槽位骨架

- `cached_qep_shared`
- `cached_qep_tab`
- `cached_key_copy`
- `cached_ref_items`
- `cached_cond_guards`

原因：它们正好卡在“足够接近执行层，又还没有绑定整次执行的全部临时状态”这个甜点位置。

#### 第五类：明确不缓存

- `Key_use`
- `POSITION`
- `JOIN *`
- `TABLE *`
- `AccessPath *`
- `RowIterator *`
- handler 打开的即时状态

不缓存的原因分别是：

- `Key_use` / `POSITION`：属于搜索时工作集，依赖当次 join-prefix 上下文
- `JOIN *`：每次执行都要求 fresh
- `TABLE *`：每次 open_tables 都会重新打开和重新绑定
- `AccessPath *` / `RowIterator *`：绑定当次 `thd->mem_root` 和执行态
- handler 状态：天然属于单次执行 / 单次读行过程

所以，plan cache 真正的策略不是：

> “把整棵 plan 缓起来”

而是：

> “把 plan 形成过程中最稳定、最值钱、最容易重复构造的那几层构件缓存起来，  
> 然后在每次 execute 时用这些构件快速重建一份 fresh 但更便宜的最小执行 plan。”

## 9. 这套 plan cache 的总状态机

当前头文件 `sql/ps_point_plan_cache.h` 里，状态机是：

- `NEVER`
- `COLD`
- `HOT`
- `INVALID`

但有一个非常重要的演进点：

> `INVALID` 现在基本是保留枚举，当前实现主要通过“降到 COLD + 清 helper cache”来恢复，而不是把语句打到一个不可恢复的死状态。

也就是说，**设计最早的想法**和**当前代码的成熟版本**已经有一点差异。

可以把当前真实生命周期理解成：

```text
PREPARE
  -> classify 失败       -> NEVER
  -> classify 成功       -> COLD

第一次 EXECUTE
  -> 正常 optimize
  -> admit 成功          -> HOT
  -> admit 失败          -> NEVER

后续 EXECUTE
  -> runtime guard 通过   -> 尝试 fast path
  -> 软漂移               -> HOT -> COLD，当前次回退正常优化，后续可再学习
  -> 结构性失效           -> 清 helper cache -> COLD
  -> 临时条件失败         -> 仅本次 fallback，不一定降级
```

其中最关键的 3 个阶段是：

1. classify：看“形状像不像”
2. admission：看“优化器实际选出的计划是不是我们要的”
3. fast path：看“在当前运行时环境里，这个模板还能不能安全复用”

## 10. 一条候选 SQL 的完整旅程

下面我们用一条典型 SQL 把整套逻辑串起来：

```sql
SELECT c FROM sbtest1 WHERE id = ?
```

### 10.1 第一次：`PREPARE`

在 `Prepared_statement::prepare()` 末尾，`sql/sql_prepare.cc` 会调用：

```cpp
ps_point_plan_classify(thd, this);
```

这一步只看语法树和语义树，不看优化器输出。

如果 shape 合法，状态进入 `COLD`。

### 10.2 第二次：第一次 `EXECUTE`

这次仍然走正常优化器。  
在 `JOIN::optimize()` 里，优化器先照常做自己的工作。

等到计划基本成形后，在 admission hook 里：

```cpp
if (ps_owner->ps_point_plan_state() == PsPointPlanState::COLD) {
  if (ps_point_plan_can_admit(ps_owner, this)) {
    ps_point_plan_admit(thd, ps_owner, this);
  }
}
```

这一步做两件事：

1. 验证优化器选出的计划是不是“单表唯一键完整覆盖点查”
2. 把稳定元数据抄到模板里

如果成功，状态进入 `HOT`。

### 10.3 第三次及之后：后续 `EXECUTE`

后续再进入 `JOIN::optimize()` 时，如果这个 PS 是 `HOT`，当前代码会在入口很早的位置尝试 fast path：

```cpp
if (ps_owner != nullptr &&
    ps_owner->ps_point_plan_state() == PsPointPlanState::HOT &&
    !ps_owner->ps_point_plan_cursor_execution()) {
  if (ps_point_plan_build_fast_path(thd, this, ps_owner)) {
    set_optimized();
    tables_list = query_block->leaf_tables;
    set_plan_state(PLAN_READY);
    return false;
  }
}
```

一旦成功，就直接绕过正常 optimizer preamble 和 `make_join_plan()` 主路径。

## 11. 第一阶段：Prepare 时的静态分类

### 11.1 为什么先 classify，不直接缓存

因为 prepare 阶段还没跑优化器，它根本不知道最后会不会真走唯一键点查。  
所以这一步只回答一个问题：

> “这条 SQL 的形状，值不值得让它在第一次执行时再观察一下？”

也就是先筛出潜在候选，进入 `COLD`。

### 11.2 classify 主要做什么

`ps_point_plan_classify()` 的门禁大致是：

- sysvar 开启
- 参数数量在 `[1, PS_PC_MAX_PARAMS]`
- 必须是 `SELECT`
- 必须走 classic optimizer
- 必须是 simple query
- 单表、无子查询、无 outer join
- 无 grouped / distinct / ordered / limit / windows / FT
- `TABLE` 已经可用
- 非分区表
- `WHERE` 必须可被识别为纯 `field = ?` conjunction
- 所有 `?` 都必须出现在这些等值条件里

### 11.3 `ps_point_plan_extract_where_shape()` 的价值

这一步的核心不是“选索引”，而是把语义树降成一个非常小的 shape：

- `param_count`
- `params[]`
- `field_indices[]`
- `plan_type`

这里有一个很重要的认识：

> classify 关心的是“谓词长得像不像点查”，不是“最终会不会命中某个唯一索引”。

最终是否能 admit，要等第一次正常优化后再说。

### 11.4 为什么把分区表排除

V1.1 design 里明确说明，分区表被排除是因为：

- 分区裁剪在正常优化路径里会发生
- fast path 会跳过这部分
- 分区表的 const / ref 语义和普通表不完全一样

这是一个典型例子：  
为了守住 correctness，宁可少命中，不去强行扩大范围。

## 12. 第二阶段：第一次正常执行后的 Admission

### 12.1 Admission 的思想

prepare 阶段只知道“像不像”。  
admission 阶段才第一次真正知道：

- 优化器选了哪个 key
- 这个 key 是不是 unique
- 所有参数是否完整覆盖该 unique key
- 最终 `ref` 结构长什么样

所以 admission 是“从候选变成正式缓存”的真正闸门。

### 12.2 最容易误解的一点：为什么 admission 看见的是 `JT_CONST`

design 和代码都强调了一件很反直觉的事：

> 对 `WHERE unique_key = ?` 这类查询，正常优化时经常看到的是 `JT_CONST`，不是 `JT_EQ_REF`。

这不是 bug，而是 MySQL legacy optimizer 的行为。

简化理解：

- 优化器在正常 optimize 时，会把这种“唯一键 + 执行时可视为常量”的访问当成 const table 处理
- 于是第一次正常执行读到的计划槽位是 `JT_CONST`
- 但 fast path 里我们重建的是 `JT_EQ_REF` 风格的最小执行计划

所以：

- admission 看的是“优化器怎样学到这条计划”
- fast path 造的是“后续执行怎样最便宜地把它跑出来”

这两者不是一模一样的表示层，但表达的是同一类唯一键点查。

### 12.3 `ps_point_plan_can_admit()` 到底在验证什么

当前代码里这一步大致在验证：

- 单表拓扑成立
- `qep_tab` 存在
- 访问类型是 `JT_CONST`
- 没有 `HAVING`
- 没有 subquery 相关 guarded cond / keypart_hash
- `ref.key` 合法
- 目标 key 是 unique
- `ref.key_parts` 完整覆盖 user-defined unique key
- `ref.key_parts == tpl.param_count`
- `ref.items[]` 里的参数都能回映到 prepare 阶段记录的稳定 `Item_param`

这一组检查的本质是：

> “第一次真实优化产生的计划，是否和我们想缓存的极窄模式严格一致？”

### 12.4 为什么要把参数顺序从 WHERE 顺序改成 key part 顺序

这是 admission 阶段里一个非常关键、但初学者很容易忽略的细节。

例子：

```sql
PRIMARY KEY (pk1, pk2)
WHERE pk2 = ?1 AND pk1 = ?2
```

prepare 阶段按 WHERE 顺序记下来的是：

- `params[0] = ?1` 对应 `pk2`
- `params[1] = ?2` 对应 `pk1`

但 fast path 构 key buffer 时，必须按 key part 顺序序列化：

- 第 0 个 key part 先写 `pk1`
- 第 1 个 key part 再写 `pk2`

所以 admission 会把 `params[]` 重排到 key 顺序。

这是一个很典型的“prepare 记录语义形状，admission 转成执行友好布局”的过程。

### 12.5 全工程索引视角下：第一次 `EXECUTE` 的真实“取 key”链

如果你把整个工程的调用链连起来看，第一次正常执行里“`WHERE` 条件最终是怎么变成 `ref` lookup”的过程大致是：

```text
JOIN::optimize()
  -> make_join_plan()
    -> update_ref_and_keys()
    -> Optimize_table_order::choose_table_order()
      -> best_access_path()
        -> find_best_ref()
    -> get_best_combination()
      -> create_ref_for_key()
```

这条链里每一步做的事不一样：

- `update_ref_and_keys()` 扫描 `WHERE` / `ON` 条件，把可能用于索引等值访问的谓词提取成 `Key_use`
- `best_access_path()` 基于这些 `Key_use` 候选去比较 ref / range / scan 的成本
- `get_best_combination()` 在确定 join order 和访问方式后，把结果实体化成 `QEP_TAB`
- `create_ref_for_key()` 把选中的 key 真正变成 `Index_lookup`，填充 `ref.items[]`、`ref.key_parts`、`ref.key_length`、`ref.key_copy[]` 等执行期需要的结构

这里有两个对 plan cache 特别关键的认识：

第一，prepare 阶段完全看不到这些信息。  
也就是说：

- prepare 只能看见 `WHERE` 的语义 shape
- admission 必须等到正常优化器把 `Key_use -> best ref -> QEP_TAB::ref()` 这条链跑完，才能确认“这条语句真的长成了我们想缓存的点查”

第二，`create_ref_for_key()` 是 normal path 和 fast path 之间的重要对照物。  
normal path 里它会：

- 根据选中的 `Key_use` 计算 `keyparts` 和 `length`
- 调 `init_ref()`
- 再逐 part 调 `init_ref_part()`
- 最后把 join type 判成 `JT_CONST` / `JT_REF` / `JT_EQ_REF`

而 fast path 所做的事，本质上就是在“我们已经知道结论”的前提下，绕过这套大流程，直接按模板快速拼出一份等价的最小 `Index_lookup + QEP_TAB + AccessPath`。

## 13. 第三阶段：HOT 执行时的 Fast Path

### 13.1 V1.2 之前和之后最大的区别

V1.2 是整个实现里非常重要的一步。

早期 fast path hook 只是绕过 `make_join_plan()` 一部分逻辑；  
V1.2 把 hook 提前到了 `JOIN::optimize()` 的入口处，直接跳过整个 optimizer preamble。

也就是说，HOT 命中时跳过的不再只是：

- join order 搜索

而是更大一段：

- `Opt_trace`
- `count_field_types`
- `alloc_func_list`
- `get_optimizable_conditions`
- `optimize_cond`
- 一系列 preamble 步骤

这也是为什么 V1.2 设计文档把它称为“Optimizer Preamble 旁路 + 深度缓存”。

### 13.2 `ps_point_plan_build_fast_path()` 的核心原则

这个函数里最重要的不是某一行代码，而是它遵守的工程原则：

> 在所有局部构造完全成功之前，不要修改 `JOIN` 的全局状态。

它采用的是“delayed-write”模式：

1. 先做 runtime guard
2. 再在局部变量里构造 `QEP_TAB` / `Index_lookup` / `AccessPath`
3. 只有全部成功，才一次性写回 `JOIN`

这样做的好处是：

- fast path 失败时，`JOIN` 仍然是干净的
- 后面可以无缝 fall through 到正常优化器
- debug build 不会因为半初始化状态触发断言

这套模式是整个实现稳不稳的关键。

### 13.3 Runtime Guard 其实是三层策略，不是一层

很多人第一次看 `ps_point_plan_runtime_guard()`，会把它看成一个“大 if”。  
其实它背后是 3 种不同语义：

#### 第一类：结构性失效

例如：

- `Table_ref` / `TABLE` / `TABLE_SHARE` 不对
- key 编号越界
- key 不再 unique
- key part 数量不匹配
- key part 对应字段序号不匹配
- 参数指针坏掉

这类问题说明模板本身不可信了。  
当前成熟实现会把 cache 清成“可重新学习”的状态，而不是继续冒险。

#### 第二类：软漂移，降到 `COLD`

例如：

- `optimizer_switch` 变化
- `sql_mode` 相关位变化
- 参数实际类型变化
- unsigned 属性变化
- 字符串 collation 变化

这类问题的特点是：

- 语句形状没有变
- 可能只是环境或参数语义变了
- 当前次不能直接相信旧模板
- 但未来仍可能重新学回来

所以动作是：

```text
HOT -> COLD
本次走正常优化器
再尝试 re-admit
```

#### 第三类：仅本次 fallback

最典型的是：

- 参数为 NULL
- 某些临时构造失败

这类问题不一定说明模板坏了，只是当前次不适合走 fast path。

### 13.4 当前工作树下，`sql_mode` guard 比 design 早期版本更保守

这是一个值得特别指出的演进点。

早期 V1.1 design 文档强调只追踪：

- `MODE_PAD_CHAR_TO_FULL_LENGTH`

但当前工作树未提交改动里，`kPsPcRelevantSqlModeMask` 已经扩展到：

- `MODE_PAD_CHAR_TO_FULL_LENGTH`
- `MODE_INVALID_DATES`
- `MODE_NO_ZERO_DATE`
- `MODE_NO_ZERO_IN_DATE`
- `MODE_TIME_TRUNCATE_FRACTIONAL`

这说明当前作者对“参数值序列化 / 比较语义是否受 `sql_mode` 影响”的判断，比早期 design 更保守。

换句话说：

> design 的早期假设是“只要追踪 PAD_CHAR 即可”；  
> 当前工作树的最新判断则是“还有更多 `sql_mode` 位可能影响 key 构造或比较语义，因此也应纳入 guard”。

这是一个很典型的“实现比最早设计更谨慎”的例子。

## 14. Fast Path 命中后，真正读行时到底发生了什么

很多人会误以为：

> fast path 命中后，参数值在 `ps_point_plan_build_fast_path()` 里就已经序列化完了。

其实不是。

真正关键的运行链路是：

```text
ps_point_plan_build_fast_path()
  -> 造好 Index_lookup / AccessPath
  -> 后续执行器生成 EQRefIterator
  -> EQRefIterator::Read()
  -> construct_lookup()
  -> store_key::copy()
  -> handler::ha_index_read_map()
```

也就是说：

- fast path 负责快速构计划骨架
- 真正把当前参数值写进 key buffer，是在读取阶段通过 `construct_lookup()` 触发的

这对理解 V1.1/V1.2/WIP 很重要。

因为一旦你明白真正的 key 构造发生在这里，就会明白为什么下面这些问题这么敏感：

- `store_key` 里的 `Field clone` 能不能安全复用
- `cached_to_fields[]` 需要不需要 `init(table)`
- key part length / store_length 一旦变了为什么必须重建 helper
- `store_key::copy()` 返回 fatal 时为什么必须 fallback

### 14.1 normal path 和 fast path 在执行层其实会“会合”

第一次正常执行时，单表唯一键点查经常是以 `JT_CONST` 的样子被优化器执行。  
这条路径里一个很关键的函数是 `read_const()`：

```text
read_const(table, ref)
  -> construct_lookup(current_thd, table, ref)
  -> ha_index_init()
  -> ha_index_read_map(..., HA_READ_KEY_EXACT)
```

而 HOT fast path 成功后，后续通常会走 `EQRefIterator::Read()`：

```text
EQRefIterator::Read()
  -> construct_lookup(thd(), table(), ref)
  -> 后续精确索引读取
```

所以两条路径虽然在优化层的表示不同：

- 正常第一次执行常见是 `JT_CONST`
- fast path 里我们手工构的是 `JT_EQ_REF`

但它们在更下层其实重新汇合到了同一个关键步骤：

- 用 `construct_lookup()` 按当前参数值构造 lookup key
- 用精确索引读取去找那一行

这也是为什么 plan cache v1 能成立：

> 它并没有发明一套新的存储引擎访问协议，  
> 它只是更早、更便宜地把“应该做一次精确唯一键读取”这个结论交给了执行层。

### 14.2 `construct_lookup()` 才是参数值真正“落地”为索引 key 的地方

`construct_lookup()` 的核心逻辑非常朴素：

- 遍历 `ref->key_parts`
- 取出每个 `ref->key_copy[part_no]`
- 对应调用 `store_key::copy()`
- 把当前 `Item_param` 的值写进 `key_buff`

所以对于 plan cache 而言，真正跨执行稳定、值得缓存的是两类东西：

- “如何把第 i 个参数映射到第 i 个 key part”的结构信息
- `store_key` / `Field clone` / key buffer 这类 helper

而真正必须每次重新做的，是：

- 当前参数值的序列化
- 当前 `TABLE *` 的 re-bind

这也解释了为什么当前实现反复强调：

- `cached_to_fields[i]->init(table)` 不能漏
- helper layout 一旦变化就必须重建
- `store_key::copy()` 虽然是小函数，却是 correctness 的最后一道关卡

## 15. 这套缓存到底缓存了什么，没缓存什么

下面这张表是理解 V1、V1.1、V1.2 的关键。


| 对象                                                             | 当前是否缓存  | 生命周期            | 为什么                             |
| -------------------------------------------------------------- | ------- | --------------- | ------------------------------- |
| `Table_ref *`                                                  | 是       | PS arena        | 语法树对象，跨执行稳定                     |
| `Item_param *`                                                 | 是       | PS arena        | 参数位对象稳定，只是值变化                   |
| `field_indices[]` / `keyno` / `key_parts`                      | 是       | PS arena        | 稳定元数据                           |
| `actual_types[]` / unsigned / collation 快照                     | 是       | PS arena        | runtime guard 需要                |
| `optimizer_switch` / `table_ref_version` / relevant `sql_mode` | 是       | PS arena        | guard 快照                        |
| `cached_key_buff` / `cached_key_buff2`                         | V1.1+ 是 | PS arena        | 减少每次 key buffer 分配              |
| `cached_store_keys[]` / `cached_to_fields[]`                   | V1.1+ 是 | PS arena        | 减少每次 store_key 和 Field clone 构造 |
| `cached_qep_tab` / `cached_qep_shared` / 指针数组                  | V1.2+ 是 | PS arena        | 减少每次 QEP skeleton 分配            |
| `JOIN *`                                                       | 否       | 单次执行            | 每次执行前要求 fresh                   |
| `TABLE *`                                                      | 否       | 单次 open/execute | 每次执行重新绑定                        |
| `AccessPath *`                                                 | 否       | `thd->mem_root` | 仍按次构造                           |
| `RowIterator *`                                                | 否       | `thd->mem_root` | 仍按次构造                           |
| handler 初始化状态                                                  | 否       | 单次执行            | 仍要 `ha_index_init()`            |


所以当前实现不是“零构造”，而是“把最值得缓存、且能安全缓存的那部分 helper 逐步往 PS arena 挪”。

## 16. Reprepare、DDL、失效和恢复

### 16.1 DDL 不只是 plan cache 的问题，而是整个 PS 生命周期的问题

当表结构变化时，MySQL 本来就有一套 reprepare 机制，plan cache 只是接入这套机制，而不是自己重新发明一套：

```text
open_tables_for_query()
  -> check_and_update_table_version()
    -> ask_to_reprepare()
      -> Reprepare_observer::report_error()
        -> 设置 ER_NEED_REPREPARE
          -> 回到 Prepared_statement::execute_loop()
            -> reprepare()
            -> 再次执行
```

把这条链拆开看会更清楚。

### 16.2 metadata 版本检查到底发生在什么位置

`open_tables_for_query()` 在打开表之后，会调用：

```cpp
check_and_update_table_version(thd, tables, tables->table->s)
```

这个检查不是 plan cache 自己做的，而是整个 PS 体系对 metadata 漂移的通用保护。

一旦发现 `Table_ref` 里记录的 `table_ref_id` 与当前 `TABLE_SHARE` 不一致，就会：

- 调 `ask_to_reprepare(thd)`
- 进到 `Reprepare_observer::report_error()`
- 把 `Diagnostics_area` 设成 `ER_NEED_REPREPARE`
- 标记 observer 已 invalidated

这一步的关键点是：

> 这里不是“直接重 prepare”，而是先在当前执行里设置一个内部错误状态，让调用栈自然回退到 `Prepared_statement::execute_loop()`，由上层统一决定是否重试。

这也是为什么 plan cache 的 `table_ref_version` guard 更像一道补充安全网，而不是主失效机制本身。

### 16.3 `execute_loop()` 是怎样接住 `ER_NEED_REPREPARE` 并重试的

`Prepared_statement::execute_loop()` 做了两件非常重要的事：

第一，它在执行前装上 `Reprepare_observer`。  
第二，它在 `execute()` 返回后检查：

- 本次是否报错
- 是否是 `CF_REEXECUTION_FRAGILE`
- `reprepare_observer.is_invalidated()` 是否为真

如果是 metadata 变化导致的内部失效，并且还允许重试，就会：

- `clear_error()`
- 调 `reprepare(thd)`
- `goto reexecute`

所以“reprepare 不是一次异常分支”，而是一条内建的 retry path。

这对理解 plan cache 很重要，因为它解释了：

- 为什么很多结构性变化不需要 plan cache 自己设计复杂恢复机理
- 为什么 `INVALID` 死状态后来被移出主恢复路径
- 为什么 worktree 里的很多 guard 更倾向于“降级 + 重学”，而不是制造新状态

### 16.4 `reprepare()` 真正做了什么

`Prepared_statement::reprepare()` 的做法很有代表性：

1. 先创建一个临时 `Prepared_statement copy`
2. 用 `swap_prepared_statement(&copy)` 把原 statement 的 arena 数据整体挪过去
3. 在原 statement 上重新 `prepare()`
4. `validate_metadata()`
5. 把旧参数数组的值换回新 statement
6. 清除 reprepare 过程中对用户不该可见的告警

这意味着 reprepare 不是“在老对象上修修补补”，而是：

> 先把旧状态整体搬走，  
> 再在一个 fresh statement 上重新搭建，  
> 成功后把用户关心的运行时值接回去。

这也是 plan cache 字段必须参加 `swap_prepared_statement()` 的根本原因。

### 16.5 参数类型变化：什么时候 full reprepare，什么时候只是 `HOT -> COLD`

这是很多人第一次看代码时最容易混淆的一点。  
参数变化其实有两层检查，而且层次完全不同。

#### 第一层：`check_parameter_types()`，关心的是 SQL 语义合同

在 `execute_loop()` 里，真正执行前就会先跑：

```cpp
if (!check_parameter_types()) {
  if (reprepare(thd)) return true;
  goto reexecute;
}
```

这里关注的问题是：

- 当前参数的实际类型，是否还符合这条语句在 prepare/resolve 时推导出来的类型合同

例如一些字符串、整数、时间、日期、decimal、double 的组合，如果继续沿用旧的 resolve 结果，可能会改变 SQL 语义，那么这里就必须 full reprepare。

#### 第二层：`ps_point_plan_runtime_guard()`，关心的是缓存模板还能不能安全复用

到了 HOT fast path 里，G8 / G9 / G10 检查的是：

- `data_type_actual()`
- unsigned flag
- `collation_actual()`

但这一步关注的不是“整条语句的语义是否必须重 resolve”，而是：

- admission 时学到的 plan template
- 在当前参数形态下
- 是否还可以安全地按旧模板直接 fast path

所以你会看到两个不同动作：

- `check_parameter_types()` 失败 -> full reprepare
- runtime guard 失败 -> demote 到 `COLD` 或本次 fallback

简化记忆就是：

```text
语义合同可能变了 -> reprepare
plan template 可能不稳了 -> 先别 fast path，回到正常优化器重新学习
```

### 16.6 `swap_prepared_statement()` 为什么重要

这一步会交换：

- `m_ps_pc_state`
- `m_ps_pc`
- `m_ps_pc_cursor_execution`
- `m_ps_pc_retryable_cold`

这是 plan cache 能在 reprepare 场景下“不悬空、不串状态”的关键。

### 16.7 为什么当前成熟代码不再依赖 `INVALID`

早期设计和早期 review 曾经暴露过一个问题：

> 如果某些结构性 guard 把状态打到 `INVALID`，但系统并没有额外触发 reprepare，那么这个 statement 可能进入一个不可恢复的死状态。

后续演进把策略改成了：

- 不把它留在 `INVALID`
- 而是 demote 到 `COLD`
- 同时清空 helper cache 标志
- 让下一次正常执行重新学习

这是一次非常重要的工程化修正：

```text
让失败路径也能恢复
比保留“理论上更精确”的状态机更重要
```

### 16.8 先给一张“案例地图”：最值得反复演练的 9 个例子

前面我们已经把对象、字段、状态机、调用链都拆开讲了。  
但如果没有具体例子，这些知识很容易停留在“看懂了文字，脑子里却没有动态过程”。

下面这 9 个例子，基本覆盖了 plan cache 最核心的几类真实生命周期。


| 案例                  | 典型状态变化                              | 主要想说明什么                              |
| ------------------- | ----------------------------------- | ------------------------------------ |
| A. 标准 admission     | `NEVER -> COLD -> HOT`              | 一条支持的一表唯一键点查，第一次执行如何“学会”             |
| B. HOT fast path 命中 | `HOT -> HOT`                        | 后续 execute 如何跳过 optimizer 大段前置流程     |
| C. 复合唯一键重排          | `COLD -> HOT`                       | SQL 文本参数顺序和索引 key part 顺序不同，模板如何重排映射 |
| D. `sql_mode` 漂移    | `HOT -> COLD -> HOT`                | cache 失效不一定意味着 reprepare，可能只是降级重学    |
| E. `NULL` 参数        | `HOT -> HOT`（仅本次 fallback）          | 模板不坏，只是这次不适合 fast path               |
| F. DDL 后重建 cache    | `HOT -> reprepare -> COLD -> HOT`   | 表结构变化如何通过 PS 通用 reprepare 机制重建模板     |
| G. DDL 后再也学不回来      | `HOT -> reprepare -> COLD -> NEVER` | 支撑前提消失时，为什么重建失败是正确行为                 |
| H. 数据变了但 cache 不失效  | `HOT -> HOT`                        | plan cache 缓存的是计划，不是结果               |
| I. cursor bypass    | `HOT -> HOT`（cursor 本次 bypass）      | 为什么 cursor 执行不走 fast path，但语句仍保留 HOT |


再额外补一个“负例”也很重要：


| 负例          | 典型状态变化                    | 主要想说明什么          |
| ----------- | ------------------------- | ---------------- |
| J. 从一开始就不支持 | `NEVER` 或 `COLD -> NEVER` | 哪些语句不是这套缓存要服务的对象 |


下面逐个展开。

### 16.9 案例 A：一条标准 PS 点查，是如何第一次被缓存的

先看最标准的例子：

```sql
CREATE TABLE t_pk (
  id INT PRIMARY KEY,
  payload VARCHAR(32)
);

PREPARE ps1 FROM 'SELECT payload FROM t_pk WHERE id = ?';
SET @p = 1;
EXECUTE ps1 USING @p;
```

这条语句第一次走完整生命周期时，大致发生下面几步。

第一步，`PREPARE`。

- `Prepared_statement::prepare()` 结束前触发 `ps_point_plan_classify()`
- `Query_block` 告诉 classify：这是一表、`WHERE id = ?`
- `PsPointPlanTemplate` 记录：
  - `table_ref`
  - `plan_type = POINT_EQ_REF`
  - `param_count = 1`
  - `params[0] = Item_param*`
  - `field_indices[0] = id` 对应字段序号
- 语句状态进入 `COLD`

这时还**没有**缓存“最终计划”，只有：

- 静态形状
- 稳定参数位
- 后续 admission 所需的模板骨架

第二步，第一次 `EXECUTE`。

- 这次还不能走 fast path，因为当前状态只是 `COLD`
- `JOIN::optimize()` 正常跑一遍老优化器链路
- `update_ref_and_keys()` 把 `id = ?` 变成 `Key_use`
- `best_access_path()` / `find_best_ref()` 判断这是主键唯一点查
- `create_ref_for_key()` 把胜出结果落成 `Index_lookup`
- 在这种单表唯一键场景里，第一次正常执行常常看到的是 `JT_CONST`

第三步，admission。

- `ps_point_plan_can_admit()` 检查：
  - 当前状态是不是 `COLD`
  - 是不是单表
  - 选中的是不是唯一等值点查
  - key part 数量、字段顺序、参数指针是否都对得上
- `ps_point_plan_admit()` 把第一次执行学到的稳定结论写回 `PsPointPlanTemplate`

此时缓存进去的核心内容包括：

- `keyno`
- `key_parts`
- `key_length`
- `null_rejecting`
- `actual_types[]`
- `unsigned_actuals[]`
- `actual_collations[]`
- `optimizer_switch`
- `table_ref_version`
- `relevant_sql_mode`

如果是 V1.1/V1.2 代码，还会进一步缓存：

- `cached_key_buff`
- `cached_key_buff2`
- `cached_store_keys[]`
- `cached_to_fields[]`
- `cached_qep_shared`
- `cached_qep_tab`
- `cached_key_copy`
- `cached_ref_items`
- `cached_cond_guards`

最后，状态从：

```text
NEVER（初始枚举值）
  -> COLD（prepare/classify 成功）
  -> HOT（第一次 execute 正常优化后 admit 成功）
```

这就是“语句第一次被缓存”的完整含义。  
不是 prepare 时直接缓存整棵计划，而是第一次 execute 先用正常优化器学会，再把稳定部分固化成 HOT 模板。

### 16.10 案例 B：同一条 PS，是如何被 fast path bypass optimizer 的

继续沿用上面的 `ps1`：

```sql
SET @p = 2;
EXECUTE ps1 USING @p;
```

这次执行和第一次最大的区别是：  
`ps1` 现在已经是 `HOT` 了。

执行开始后，关键路径变成：

```text
Prepared_statement::execute_loop()
  -> execute()
  -> JOIN::optimize() 顶部的 fast path hook
  -> ps_point_plan_build_fast_path()
  -> 直接构造最小 QEP_TAB / Index_lookup / AccessPath
  -> 后续进入 EQRefIterator::Read()
```

被 bypass 掉的，不是整个执行器，而是 optimizer 前面那大段“从 WHERE 重新推 Key_use、重新算 best_access_path、重新 create_ref_for_key”的工作。

也就是说，这次不会再完整重走：

- `update_ref_and_keys()`
- `Optimize_table_order::find_best_ref()`
- `Optimize_table_order::best_access_path()`
- `create_ref_for_key()`

取而代之的是：

- runtime guard 先验证模板仍可信
- `ps_point_plan_build_fast_path()` 直接按模板拼出：
  - `QEP_shared`
  - `QEP_TAB`
  - `Index_lookup`
  - `AccessPath`

但这里有个非常重要的细节：

> fast path bypass 的是“重新做计划选择”，不是“重新读取当前参数值”。

所以到了真正读行时，仍然会走：

```text
EQRefIterator::Read()
  -> construct_lookup()
  -> store_key::copy()
  -> ha_index_read_map()
```

这也就是说：

- 计划骨架复用了缓存
- 当前 execute 的参数值仍然是 fresh 序列化
- 最终行读取仍然是 fresh handler / InnoDB 访问

这就是“fast path bypass”最精确的定义：

> 绕过 optimizer 的大段前置构造，  
> 但不绕过执行层的真实 key 构造和行读取。

### 16.11 案例 C：复合唯一键下，参数顺序是如何被重排并缓存的

这个例子专门用来理解为什么 `field_indices[]`、`params[]`、`cached_store_keys[]` 都不能只按 SQL 文本顺序记。

```sql
CREATE TABLE t_ab (
  a INT NOT NULL,
  b INT NOT NULL,
  payload VARCHAR(32),
  UNIQUE KEY uk_ab (a, b)
);

PREPARE ps2 FROM 'SELECT payload FROM t_ab WHERE b = ? AND a = ?';
SET @p1 = 20;   -- 对应 b
SET @p2 = 10;   -- 对应 a
EXECUTE ps2 USING @p1, @p2;
```

这条语句的关键点在于：

- SQL 文本顺序是 `b = ?` 然后 `a = ?`
- 但索引 `uk_ab` 的 key part 顺序是 `(a, b)`

所以生命周期会分成两个顺序。

prepare/classify 阶段，模板先按 WHERE 文本顺序记下来：

```text
params[]        = [param_for_b, param_for_a]
field_indices[] = [b, a]
```

第一次正常执行后，optimizer 最终选中的是 `uk_ab(a,b)`。  
于是 admission 必须把模板改写成“按 key part 顺序”：

```text
params[]        = [param_for_a, param_for_b]
field_indices[] = [a, b]
```

这样 HOT fast path 里：

- `cached_store_keys[0]` 才会对应 key part `a`
- `cached_store_keys[1]` 才会对应 key part `b`
- `construct_lookup()` 序列化出来的 `key_buff` 才和 `KEY_PART_INFO[0]`,`KEY_PART_INFO[1]` 的布局一致

这个例子特别重要，因为它告诉你：

> plan cache 缓存的不是“原 SQL 的参数出现顺序”，  
> 而是“最终执行 lookup 所要求的 key part 顺序”。

也正因为如此，`PsPointPlanTemplate` 里很多看起来只是“数组重排”的动作，其实是在保证：

- key part 语义正确
- `Index_lookup::items[]`
- `Index_lookup::key_copy[]`
- `store_key`
- `KEY_PART_INFO`

这四层之间始终一一对应。

### 16.12 案例 D：同一条 HOT 语句，是如何因为环境漂移而 cache 失效的

下面看一个“不是 DDL、不是 reprepare、但 cache 仍然会失效”的例子。

```sql
CREATE TABLE t_char (
  code CHAR(4) PRIMARY KEY,
  payload VARCHAR(32)
);

PREPARE ps3 FROM 'SELECT payload FROM t_char WHERE code = ?';
SET @p = 'A';
EXECUTE ps3 USING @p;   -- 第一次学会，进入 HOT

SET SESSION sql_mode = CONCAT(@@sql_mode, ',PAD_CHAR_TO_FULL_LENGTH');
EXECUTE ps3 USING @p;
```

第二次执行时，最关键的不是 metadata，而是 environment snapshot。

admission 时模板里已经记下过：

- `relevant_sql_mode`

而当前 execute 进入 fast path 前，runtime guard 会重新比较：

- 现在的 relevant `sql_mode`
- 是否和 admission 时快照一致

如果不一致，就说明：

- 当前参数如何被解释/比较
- 字符串 padding 语义
- 甚至更保守版本代码里的一些日期/时间语义

都有可能和旧模板对应的环境不同。

所以动作不是直接 reprepare，而是：

```text
HOT -> COLD
本次放弃 fast path
改走正常优化器
如果新环境下仍然是支持的一表唯一点查，则可再次 admit -> HOT
```

这就是“cache 失效”最典型的一类：

- 失效的是旧模板
- 不是整条 PS 的语义树
- 所以不需要 full reprepare
- 只需要回到正常优化器重新学习

### 16.13 案例 E：同一条 HOT 语句，是如何因为 `NULL` 参数只做本次 fallback 的

这个例子非常适合用来区分：

- “模板坏了”
- 和“这次输入值不适合直接 fast path”

```sql
CREATE TABLE t_uk_nullable (
  uk INT NULL,
  payload VARCHAR(32),
  UNIQUE KEY uk_idx (uk)
);

PREPARE ps4 FROM 'SELECT payload FROM t_uk_nullable WHERE uk = ?';
SET @p = 1;
EXECUTE ps4 USING @p;      -- 学会，进入 HOT

SET @p = NULL;
EXECUTE ps4 USING @p;      -- 本次 fallback

SET @p = 2;
EXECUTE ps4 USING @p;      -- 可以再次命中 fast path
```

为什么第二次 `NULL` 不会把模板摧毁？

因为这件事说明的是：

- 当前这次 execute 的参数值不适合直接按旧模板走 fast path

而不是说明：

- 索引结构变了
- key part 映射变了
- helper 布局变了
- 语句的 prepare 语义合同变了

所以这类情况的动作是：

- 记一次 runtime fallback
- 本次改走正常路径
- 但保留 `HOT` 模板

后面当参数恢复成普通非 NULL 值时，这条语句仍可以继续命中。

这个例子非常适合拿来强调：

> plan cache 缓存的是“如何构造 lookup 的模板”，  
> 不是“这次参数值一定能直接走同一路径”的保证。

### 16.14 案例 F：表结构变化后，cache 是如何失效并重建的

现在看用户明确点名要有的案例：  
**表变化如何引起 cache 失效，并在后续执行里重建。**

```sql
CREATE TABLE t_rebuild (
  id INT PRIMARY KEY,
  payload VARCHAR(32)
);

PREPARE ps5 FROM 'SELECT payload FROM t_rebuild WHERE id = ?';
SET @p = 1;
EXECUTE ps5 USING @p;      -- 学会，进入 HOT

ALTER TABLE t_rebuild ADD COLUMN extra INT;

EXECUTE ps5 USING @p;
```

这时发生的不是 runtime guard 那种“软漂移”，而是 metadata-level 变化。

关键链路是：

```text
open_tables_for_query()
  -> check_and_update_table_version()
  -> ask_to_reprepare()
  -> Reprepare_observer::report_error()
  -> execute_loop() 看到 ER_NEED_REPREPARE
  -> reprepare()
  -> prepare() 重新 classify
  -> 新 statement 回到 COLD
  -> 当前次或下次执行再次正常 optimize
  -> re-admit
  -> HOT
```

这里要特别强调两个“重建”。

第一，PS 层重建。

- 旧 statement 的 arena 状态被 `swap_prepared_statement()` 搬走
- 在 fresh statement 上重新 `prepare()`
- plan cache 相关字段跟着 PS 生命周期一起切换

第二，plan cache 模板重建。

- `PsPointPlanTemplate` 重新填充
- `table_ref_version` 重新快照
- `keyno/key_parts/key_length` 重新学习
- `cached_store_keys[]` / `cached_to_fields[]` 重新建立
- `cached_qep_shared` / `cached_qep_tab` 重新建立

也就是说，DDL 后不是“修补旧 cache”，而是：

> 借助 MySQL 原生 reprepare 机制，  
> 在 fresh prepare + fresh first execute 的基础上，  
> 重新学出一份新的 cache。

### 16.15 案例 G：表结构变化后，为什么有时 cache 重建失败是正确的

并不是所有 DDL 后都应该“重新变 HOT”。  
如果支撑前提已经没了，那么重建失败才是正确行为。

```sql
CREATE TABLE t_no_longer_supported (
  uk INT NOT NULL,
  payload VARCHAR(32),
  UNIQUE KEY uk_idx (uk)
);

PREPARE ps6 FROM 'SELECT payload FROM t_no_longer_supported WHERE uk = ?';
SET @p = 1;
EXECUTE ps6 USING @p;      -- 学会，进入 HOT

ALTER TABLE t_no_longer_supported DROP INDEX uk_idx;

EXECUTE ps6 USING @p;
```

这条链路也会先触发 reprepare。  
但 reprepare 之后，结果和案例 F 不同。

原因是：

- WHERE 形状 `uk = ?` 仍然存在
- 所以 classify 可能仍然成功，语句先回到 `COLD`
- 但第一次正常执行再跑优化器时，`uk_idx` 已不存在
- optimizer 不会再给出“唯一键等值点查”的结论
- admission 检查失败
- 最终状态会落到 `NEVER`

也就是说：

```text
HOT
  -> metadata 变化
  -> reprepare
  -> COLD
  -> 正常 optimize
  -> admit 失败
  -> NEVER
```

这正是正确行为，因为 plan cache 的服务前提已经没了。

这个案例很重要，它说明：

- “表变化”不等于“一定能重建 cache”
- reprepare 只是给你一次重新学习的机会
- 如果新的物理事实不再满足唯一点查模板，系统就应该老老实实退回普通路径

### 16.16 案例 H：数据变了但 cache 不失效，因为它缓存的是 plan 不是结果

这个例子经常能帮人彻底摆脱“把 plan cache 想成 result cache”的误解。

```sql
CREATE TABLE t_data_change (
  id INT PRIMARY KEY,
  payload VARCHAR(32)
);

PREPARE ps7 FROM 'SELECT payload FROM t_data_change WHERE id = ?';
SET @p = 1;
EXECUTE ps7 USING @p;      -- 学会，进入 HOT

DELETE FROM t_data_change WHERE id = 1;

EXECUTE ps7 USING @p;      -- 返回 0 行
```

第二次执行即使仍然命中 fast path，也完全可能返回 0 行。  
原因很简单：

- fast path 只复用了“如何构造唯一点查计划”
- 真正的行是否存在，还是每次都去 handler / InnoDB 里现查

所以这里不会发生：

- cache 失效
- reprepare
- 重建模板

状态仍然可以保持 `HOT`。  
变的只是：

- `ha_index_read_map()` 这次查不到对应记录
- 执行结果为空

这个例子一定要记住，因为它最能说明：

> plan cache 不缓存行内容，不缓存结果集，不缓存“上一回查到的是哪一行”。  
> 它只缓存“怎样更快地形成这次点查计划”。

### 16.17 案例 I：同一条语句，cursor execute 为什么 bypass fast path

这个例子对应 Phase 4/5 特别补的 cursor 路径。

```sql
PREPARE ps8 FROM 'SELECT payload FROM t_pk WHERE id = ?';

SET @p = 1;
EXECUTE ps8 USING @p;      -- 第一次学会，进入 HOT

SET @p = 2;
EXECUTE ps8 USING @p;      -- 非 cursor，再次命中 fast path

-- 接着通过 cursor 方式执行同一条 parameterized PS
-- open_cursor = true
```

cursor 执行时，`Prepared_statement::execute()` 会把：

- `m_ps_pc_cursor_execution = true`

于是 `JOIN::optimize()` 顶部的 fast path hook 会看到：

- 当前是 cursor 模式
- 本次直接 bypass fast path

但这里要注意两点。

第一，bypass 的是“本次 fast path”，不是把语句彻底打回 `COLD/NEVER`。  
第二，等下一次再用普通 non-cursor execute 调它时，只要模板仍可信，它还可以继续命中。

所以这个案例里的状态理解应该是：

```text
HOT
  -> cursor execute（本次 bypass）
  -> 仍然 HOT
  -> 下次 non-cursor execute 仍可 hit
```

这说明 cursor 不是“模板失效”，而是“执行模式不允许走这条捷径”。

### 16.18 案例 J：还有哪些语句从一开始就不该进入 cache

最后必须补一个“负例组”，否则读者很容易误以为任何 `field = ?` 都会被缓存。

最典型有两种。

第一种：WHERE 形状像，但最终不是唯一点查。

```sql
CREATE TABLE t_ref_only (
  k INT,
  payload VARCHAR(32),
  KEY k_idx (k)
);

PREPARE ps9 FROM 'SELECT payload FROM t_ref_only WHERE k = ?';
```

这条语句：

- prepare/classify 可能先进入 `COLD`
- 但第一次正常执行后，optimizer 只会得到 `JT_REF`，不是唯一点查
- admission 失败
- 最终落到 `NEVER`

第二种：形状本身就被 v1 明确排除。

例如分区表：

```sql
CREATE TABLE t_part (
  id INT NOT NULL,
  payload VARCHAR(32),
  PRIMARY KEY (id)
)
PARTITION BY HASH(id) PARTITIONS 4;

PREPARE ps10 FROM 'SELECT payload FROM t_part WHERE id = ?';
```

这类语句在 classify 阶段就会被排除，直接进入 `NEVER`。  
根本原因不是“语法看不懂”，而是：

- v1 刻意把生命周期证明做得很窄
- 不愿在分区、复杂路由、更多元数据耦合上冒险

所以“看起来相似”不等于“真的在这套缓存的服务范围里”。

### 16.19 这几个案例，分别在告诉你什么

如果把上面所有案例压缩成一组口诀，可以记成：

- 案例 A/B：先学会，再命中
- 案例 C：缓存的是 key part 顺序，不是 SQL 文本顺序
- 案例 D：环境漂移通常是 `HOT -> COLD -> 重学`
- 案例 E：临时输入值问题通常只是“本次 fallback”
- 案例 F：DDL 后依靠 reprepare 重建 cache
- 案例 G：如果物理前提消失，重建失败是正确结果
- 案例 H：数据变化不会让 plan cache 失效，因为它不缓存结果
- 案例 I：cursor 是执行模式 bypass，不是模板损坏
- 案例 J：不是所有 `field = ?` 都属于这套缓存的服务对象

### 16.20 时序图 1：标准 admission 是怎样发生的

下面改用纯文本时序图，把“第一次学会”的关键责任边界画出来。  
这样在普通 Markdown、终端、代码审阅器里都能直接看，不依赖 `mermaid` 渲染。

```text
Client            Prepared_statement     ps_point_plan_cache     Optimizer         Executor         Handler/InnoDB
  |                       |                       |                  |                  |                  |
  | PREPARE SELECT ...    |                       |                  |                  |                  |
  | --------------------> |                       |                  |                  |                  |
  |                       | classify(...)         |                  |                  |                  |
  |                       | --------------------> |                  |                  |                  |
  |                       | state=COLD, tpl=静态形状 |               |                  |                  |
  |                       | <-------------------- |                  |                  |                  |
  |                       |                       |                  |                  |                  |
  | EXECUTE #1            |                       |                  |                  |                  |
  | --------------------> | JOIN::optimize() 正常路径                 |                  |                  |
  |                       | ---------------------------------------> |                  |                  |
  |                       |                       | update_ref_and_keys/find_best_ref/create_ref_for_key   |
  |                       |                       | <----------------------------------------------->     |
  |                       |                       | can_admit()/admit()                                 |
  |                       | --------------------> |                                                      |
  |                       | state=HOT, tpl+=keyno/key_parts/...                                         |
  |                       | <-------------------- |                                                      |
  |                       | 本次正常计划                                              |                  |
  |                       | --------------------------------------------------------> |                  |
  |                       |                                                          | 精确索引读取      |
  |                       |                                                          | ----------------> |
  |                       |                                                          | 返回行            |
  |                       |                                                          | <---------------- |
```

这张图最应该记住的是两点。

第一，`PREPARE` 只做 classify，不做最终计划缓存。  
第二，真正把语句变成 `HOT` 的，是第一次正常 `EXECUTE` 末尾的 admission。

### 16.21 时序图 2：HOT 之后，fast path 是怎样 bypass optimizer 的

```text
Client            Prepared_statement     JOIN::optimize hook     ps_point_plan_cache     Executor/EQRefIterator     Handler/InnoDB
  |                       |                       |                       |                         |                         |
  | EXECUTE #2            |                       |                       |                         |                         |
  | --------------------> |                       |                       |                         |                         |
  |                       | 进入 JOIN::optimize() |                       |                         |                         |
  |                       | --------------------> |                       |                         |                         |
  |                       |                       | state == HOT ?        |                         |                         |
  |                       |                       | --------------------> |                         |                         |
  |                       |                       | runtime_guard()       |                         |                         |
  |                       |                       | --------------------> |                         |                         |
  |                       |                       | build_fast_path()     |                         |                         |
  |                       |                       | --------------------> |                         |                         |
  |                       |                       | 最小 QEP_TAB + Index_lookup + AccessPath          |                         |
  |                       |                       | <-------------------- |                         |                         |
  |                       | 跳过大段正常优化前置流程 |                       |                         |                         |
  |                       | <-------------------- |                       |                         |                         |
  |                       | 进入 EQRefIterator::Read()                                               |                         |
  |                       | -----------------------------------------------------------------------> |                         |
  |                       |                                                                          | construct_lookup()      |
  |                       |                                                                          | store_key::copy()       |
  |                       |                                                                          | ha_index_read_map() --> |
  |                       |                                                                          | <---------------------- |
```

这张图说明：

- bypass 的是 optimizer 前段的大量重建工作
- 没有 bypass 当前参数值的序列化
- 也没有 bypass handler / InnoDB 真实查行

### 16.22 时序图 3：`sql_mode` 漂移时，为什么是 `HOT -> COLD -> HOT`

```text
Client            Prepared_statement     ps_point_plan_cache     Optimizer
  |                       |                       |                  |
  | EXECUTE (语句已 HOT)  |                       |                  |
  | --------------------> |                       |                  |
  |                       | runtime_guard()       |                  |
  |                       | --------------------> |                  |
  |                       | 对比 relevant_sql_mode snapshot            |
  |                       | --------------------> |                  |
  |                       | mismatch: HOT -> COLD |                  |
  |                       | <-------------------- |                  |
  |                       | 本次回到正常优化器    |                  |
  |                       | --------------------------------------->  |
  |                       |                       | can_admit()/admit() |
  |                       | --------------------> |                  |
  |                       | 在新环境下重新学习 -> HOT                 |
  |                       | <-------------------- |                  |
```

这里最重要的是理解：

- 失效的是旧模板
- 不是 parse tree
- 不是 metadata
- 更不是必须 full reprepare 的 SQL 语义合同

所以正确动作是“降级重学”，不是“整条 PS 重 prepare”。

### 16.23 时序图 4：DDL 后，cache 是怎样失效并重建的

```text
Session 2 / DDL        Session 1 / PS         open_tables_for_query     Reprepare_observer     reprepare()        ps_point_plan_cache     Optimizer
      |                       |                        |                        |                     |                     |                  |
      | ALTER TABLE ...       |                        |                        |                     |                     |                  |
      | --------------------> |                        |                        |                     |                     |                  |
      |                       | 下一次 EXECUTE         |                        |                     |                     |                  |
      |                       | ---------------------> |                        |                     |                     |                  |
      |                       |                        | check_and_update_table_version()               |                     |                  |
      |                       |                        | --------------------> |                     |                     |                  |
      |                       |                        | ER_NEED_REPREPARE    |                     |                     |                  |
      |                       |                        | <-------------------- |                     |                     |                  |
      |                       | reprepare()           |                        |                     |                     |                  |
      |                       | -------------------------------------------------------------> |                     |                  |
      |                       |                     swap_prepared_statement() + prepare()      |                     |                  |
      |                       |                                                                         re-classify -> COLD/NEVER  |
      |                       | -------------------------------------------------------------------------------> |                  |
      |                       | 当前次或下次正常 optimize                                                                      |
      |                       | ------------------------------------------------------------------------------------------------> |
      |                       |                                                                                  re-admit -> HOT  |
      |                       | <------------------------------------------------------------------------------- |                  |
```

这张图最关键的工程点是：

- DDL 不是由 plan cache 自己“私有处理”
- 它复用的是 MySQL 现成的 PS reprepare 主链
- plan cache 只是跟着 fresh PS 生命周期一起重建模板

### 16.24 时序图 5：`NULL` 参数为什么通常只是“本次 fallback”

```text
Client            Prepared_statement     ps_point_plan_cache     Optimizer
  |                       |                       |                  |
  | EXECUTE (参数 = NULL) |                       |                  |
  | --------------------> |                       |                  |
  |                       | runtime_guard()       |                  |
  |                       | --------------------> |                  |
  |                       | 当前次不适合 fast path |                  |
  |                       | <-------------------- |                  |
  |                       | 本次走正常优化器      |                  |
  |                       | --------------------------------------->  |
  |                       | 返回正确结果          |                  |
  |                       | <---------------------------------------  |
  |                       | 注：模板通常仍保持 HOT |                  |
  |                       |                       |                  |
  | EXECUTE (参数恢复非 NULL)                     |                  |
  | --------------------> |                       |                  |
  |                       | 再次命中 fast path    |                  |
  |                       | --------------------> |                  |
```

这类例子是拿来提醒自己：

- runtime fallback 不等于模板损坏
- “本次不能快走” 和 “以后都不能快走” 不是一回事

### 16.25 新增案例 K：第一次执行 0 行，为什么通常保持 `COLD`

这个例子很值钱，因为它说明：

- “第一次 execute 没有命中数据”
- 和“这条语句不值得缓存”

并不是一回事。

```sql
CREATE TABLE t_zero (
  id INT PRIMARY KEY,
  payload VARCHAR(32)
);

PREPARE ps11 FROM 'SELECT payload FROM t_zero WHERE id = ?';

SET @p = 100;
EXECUTE ps11 USING @p;     -- 当前表里没有 id=100
```

在这类场景里，第一次正常执行可能因为 `zero_result_cause` 等短路路径，导致 admission hook 根本没有机会执行。  
于是结果往往是：

- 返回 0 行
- 语句仍保持 `COLD`
- 还没有真正学成 `HOT`

接着如果后来数据出现：

```sql
INSERT INTO t_zero VALUES (100, 'hello');
EXECUTE ps11 USING @p;     -- 这次正常优化 + admit
EXECUTE ps11 USING @p;     -- 再下一次才会 stable hit
```

于是状态变化就可能是：

```text
PREPARE -> COLD
EXECUTE #1 (0 行) -> 仍然 COLD
EXECUTE #2 (命中行) -> HOT
EXECUTE #3 -> fast path hit
```

这个例子告诉你：

> admission 的前提，不只是“语句形状像点查”，  
> 还包括“本次执行真的走到了可验证、可固化的那条优化/执行路径”。

### 16.26 新增案例 L：参数类型变化，其实有两条分叉

很多人会把“参数类型变化”统一理解成“肯定 reprepare”或“肯定 fallback”。  
其实当前代码里这件事至少分两类。

#### L1：语义合同变化，触发 full reprepare

例如：

```sql
CREATE TABLE t_num (
  id INT PRIMARY KEY,
  payload VARCHAR(32)
);

PREPARE ps12 FROM 'SELECT payload FROM t_num WHERE id = ?';
SET @p = 1;
EXECUTE ps12 USING @p;     -- 学会，进入 HOT

SET @p = 1.5;
EXECUTE ps12 USING @p;
```

这类场景下，`check_parameter_types()` 可能认为：

- 当前参数实际类型已经不再满足旧的 resolve 合同

于是动作是：

```text
check_parameter_types() 失败
  -> full reprepare
  -> reexecute
```

这里重点不是 plan cache，而是 **整个 PS 的 SQL 语义合同**。

#### L2：不一定 full reprepare，但旧模板不再适合 fast path

另一些场景下，参数变化未必先触发 full reprepare，  
但会让 runtime guard 认为旧模板不再安全。

可以把它抽象理解成：

```text
check_parameter_types() 允许继续
  -> 进入 fast path guard
  -> actual type / unsigned / collation 与 admission 快照不一致
  -> HOT -> COLD 或本次 fallback
```

这类情况更强调的是：

- 旧 template 还能不能直接复用

而不是：

- SQL 语义树是否必须重 resolve

所以一个非常值得记住的总分叉是：

```text
参数变化
  -> 先问：SQL 语义合同是否变化？是 -> full reprepare
  -> 否则再问：旧模板还能否安全 fast path？否 -> COLD/fallback
```

### 16.27 如果你拿这些案例去对源码，最该盯哪几个责任点

案例读完之后，最好的下一步不是背例子，而是拿例子去反打源码。

你可以按责任点来对：

- “为什么 prepare 后只是 `COLD`”  
看 `ps_point_plan_classify()`
- “为什么第一次 execute 才会变 `HOT`”  
看 `ps_point_plan_can_admit()` / `ps_point_plan_admit()`
- “为什么后续 execute 会 bypass optimizer”  
看 `ps_point_plan_build_fast_path()`
- “为什么 `NULL` 不一定摧毁模板”  
看 `ps_point_plan_runtime_guard()`
- “为什么 DDL 走 reprepare 而不是 plan cache 私有逻辑”  
看 `check_and_update_table_version()`、`Reprepare_observer`、`Prepared_statement::reprepare()`
- “为什么数据变化不影响 cache”  
看 `construct_lookup()`、`EQRefIterator::Read()`、`ha_index_read_map()`

如果你能把案例、责任点、函数调用三者对起来，这套 plan cache 才算真正吃透。

### 16.28 把这些案例真正跑起来：2026-04-10 的实测状态轨迹

为了让上面的时序图不是“纸上推演”，我在 `2026-04-10` 对当前工作树做了两类实测：

- 跑当前仓库的 MTR 用例：`main.ps_point_plan_cache_fast_path`、`main.ps_point_plan_cache_runtime_drift`、`main.ps_point_plan_cache_env_drift`、`main.ps_point_plan_cache_edge`，四组都 `pass`
- 用 `mysql-test-run.pl --start` 拉起测试实例，再用同一条 session 手工重放 `PREPARE/EXECUTE` 场景，并在每一步读取 `performance_schema.session_status`

这组实测最重要的价值，不只是“证明功能能跑”，而是把几个最容易讲抽象的状态量真正落到数字上：

- `Ps_point_plan_cache_admissions`
- `Ps_point_plan_cache_hits`
- `Ps_point_plan_cache_fallback_runtime`
- `Ps_point_plan_cache_invalidations`
- `Ps_point_plan_cache_cold_classifications`
- `Com_stmt_reprepare`

先看几组最有代表性的实测轨迹。

#### 16.28.1 标准 PK 点查：第一次学会，后面连续命中


| 步骤                | 返回结果   | admissions | hits | fallback_runtime | invalidations | cold_classifications |
| ----------------- | ------ | ---------- | ---- | ---------------- | ------------- | -------------------- |
| `S1_after_first`  | `id=1` | 1          | 0    | 0                | 0             | 1                    |
| `S1_after_second` | `id=2` | 1          | 1    | 0                | 0             | 1                    |
| `S1_after_third`  | `id=3` | 1          | 2    | 0                | 0             | 1                    |


这条轨迹非常“教科书”：

- 第一次 `EXECUTE` 把模板从“只知道它像点查”推进到“已经成功学成 HOT”
- 后续每次快走，主要增长的是 `hits`
- `admissions` 不是“每次执行都加 1”，而是“每次重新学成 HOT 才加 1”

#### 16.28.2 `NULL` 参数：本次 fallback，但模板通常还活着


| 步骤                 | 返回结果   | admissions | hits | fallback_runtime | invalidations | cold_classifications |
| ------------------ | ------ | ---------- | ---- | ---------------- | ------------- | -------------------- |
| `S2_after_admit`   | `id=1` | 1          | 0    | 0                | 0             | 1                    |
| `S2_after_null`    | 0 行    | 1          | 0    | 1                | 0             | 1                    |
| `S2_after_recover` | `id=3` | 1          | 1    | 1                | 0             | 1                    |


这组数字把案例 E 说得很透：

- `NULL` 那一次主要增加的是 `fallback_runtime`
- `admissions` 没变，说明模板没有被重学
- 下一次参数恢复正常后，`hits` 继续增长，说明模板仍可复用

所以“本次 fallback”和“模板已经坏了”绝对不能画等号。

#### 16.28.3 第一次 0 行：为什么通常仍然停在 `COLD`


| 步骤                   | 返回结果   | admissions | hits | fallback_runtime | invalidations | cold_classifications |
| -------------------- | ------ | ---------- | ---- | ---------------- | ------------- | -------------------- |
| `S6_after_miss`      | 0 行    | 0          | 0    | 0                | 0             | 1                    |
| `S6_after_first_row` | `id=1` | 1          | 0    | 0                | 0             | 1                    |
| `S6_after_hit`       | `id=2` | 1          | 1    | 0                | 0             | 1                    |


这组轨迹非常值得放到脑子里：

- 第一次没找到行，不代表语句永久不适合缓存
- 但它确实可能导致当前这一次没有完成 admission
- 所以常见时序是：`0 行 -> 仍 COLD -> 第一次命中行时才学成 HOT -> 再下一次才稳定 hit`

#### 16.28.4 `sql_mode` 漂移：先 demote，再等待合适时机重学

这是一个很适合拿来打破“环境一恢复就立刻重建 HOT”这种直觉的例子。


| 步骤            | 环境/参数                             | admissions | hits | fallback_runtime | invalidations | cold_classifications |
| ------------- | --------------------------------- | ---------- | ---- | ---------------- | ------------- | -------------------- |
| `S3_after_A1` | 默认 `sql_mode`，`'ABC'`             | 1          | 0    | 0                | 0             | 1                    |
| `S3_after_A2` | 默认 `sql_mode`，`'DEF'`             | 1          | 1    | 0                | 0             | 1                    |
| `S3_after_A3` | `PAD_CHAR_TO_FULL_LENGTH`，`'ABC'` | 1          | 1    | 1                | 0             | 1                    |
| `S3_after_A4` | `PAD_CHAR_TO_FULL_LENGTH`，`'DEF'` | 1          | 1    | 1                | 0             | 1                    |
| `S3_after_A5` | 恢复默认 `sql_mode`，`'GHI'`           | 2          | 1    | 1                | 0             | 1                    |


这里最有启发性的地方是：

- 漂移发生时，首先看到的是 `fallback_runtime` 增长
- 但这次手工重放里，并不是一漂移就立刻重新 `admit`
- 真正的新一轮 `admission`，出现在环境恢复、并且这次正常执行再次落回可固化路径之后

换句话说，实测告诉你：

> `HOT -> COLD` 之后，不一定“下一次就立刻恢复 HOT”；  
> 更准确的说法是“等下一次真正走到可再次固化的路径时才恢复 HOT”。

#### 16.28.5 `use_invisible_indexes` 漂移：当前仓库的真实值是“多次 fallback + 多次重学”


| 步骤            | 环境/参数                           | admissions | hits | fallback_runtime | invalidations | cold_classifications |
| ------------- | ------------------------------- | ---------- | ---- | ---------------- | ------------- | -------------------- |
| `S4_after_A1` | `use_invisible_indexes=on`，`10` | 1          | 0    | 0                | 0             | 1                    |
| `S4_after_A2` | 切到 `off`，`20`                   | 2          | 0    | 1                | 0             | 1                    |
| `S4_after_A3` | 切回 `on`，`20`                    | 3          | 0    | 2                | 0             | 1                    |
| `S4_after_A4` | 继续 `on`，`10`                    | 3          | 1    | 2                | 0             | 1                    |


这一组尤其重要，因为它暴露了一个很容易误读仓库的点：

- `mysql-test/t/ps_point_plan_cache_runtime_drift.test` 里的英文注释还写着 “A2 admissions 期待仍为 1、A3 期待变成 2”
- 但当前真正被 MTR 校验并且 `pass` 的，是 `mysql-test/r/ps_point_plan_cache_runtime_drift.result`
- `.result` 里的真实输出恰好和这次手工重放一致：`A2=2`、`A3=3`

这说明当前分支上的真实行为更接近：

```text
环境漂移
  -> fast path guard 失败
  -> 记一次 runtime fallback
  -> 同次 EXECUTE 走正常优化/执行
  -> 如果本次又落回可固化点查路径，就再次 admission
```

所以这里不要机械背 `.test` 里写的 “expect 注释”，而要以：

- 通过的 `.result`
- 当前工作树的实测轨迹

作为真正语义。

#### 16.28.6 DDL 导致 reprepare：看 `Com_stmt_reprepare` 比单看 `invalidations` 更靠谱

先看“表变了，但还能重建 HOT”的情况。


| 步骤                     | 结果/事件                           | admissions | hits | fallback_runtime | invalidations | cold_classifications | Com_stmt_reprepare |
| ---------------------- | ------------------------------- | ---------- | ---- | ---------------- | ------------- | -------------------- | ------------------ |
| `S5_after_admit`       | 首次学会                            | 1          | 0    | 0                | 0             | 1                    | 0                  |
| `S5_after_hit`         | 一次 fast path 命中                 | 1          | 1    | 0                | 0             | 1                    | 0                  |
| `S5_after_ddl_execute` | `ALTER TABLE ADD COLUMN` 后第一次执行 | 2          | 1    | 0                | 0             | 2                    | 1                  |
| `S5_after_rehit`       | 新 schema 下再次命中                  | 2          | 2    | 0                | 0             | 2                    | 1                  |


这组数字说明：

- DDL 之后，真正最能说明“PS 被迫重新准备”的信号是 `Com_stmt_reprepare=1`
- 同时 `cold_classifications` 和 `admissions` 都各增加了一次，说明它经历了“重新分类 -> 重新学成 HOT”
- 但 `Ps_point_plan_cache_invalidations` 在这条链路上仍然是 `0`

因此一个非常实战的结论是：

> `invalidations=0` 不等于“没有发生 cache 失效/重建”；  
> 在 DDL 这条链路上，更可靠的观测组合往往是  
> `Com_stmt_reprepare + admissions + cold_classifications`。

#### 16.28.7 DDL 导致 reprepare，但物理前提消失时不会重建 HOT

再看“表变了，而且唯一键前提消失，所以重建失败是正确的”的情况。


| 步骤                      | 结果/事件                      | admissions | hits | fallback_runtime | invalidations | cold_classifications | Com_stmt_reprepare |
| ----------------------- | -------------------------- | ---------- | ---- | ---------------- | ------------- | -------------------- | ------------------ |
| `S7_after_hit`          | UK 点查已经学成 HOT 并命中过一次       | 1          | 1    | 0                | 0             | 1                    | 0                  |
| `S7_after_drop_execute` | `DROP INDEX idx_uk` 后第一次执行 | 1          | 1    | 0                | 0             | 2                    | 1                  |


这条轨迹几乎就是案例 G 的实测版：

- `Com_stmt_reprepare=1` 说明 DDL 的确迫使 PS 重准备
- `cold_classifications=2` 说明它又重新走了一遍 classify
- 但 `admissions` 没有变，说明这次没有重新学成 HOT

这不是失败，而是**正确行为**，因为支撑点查模板的唯一键条件已经被拿掉了。

### 16.29 如何解读这些实测值：最容易误读的 4 件事

#### 16.29.1 `cold_classifications` 增长，不等于“又生成了一个 HOT cache entry”

它只说明：

- 语句又被拿去走了一遍 classify/cold 阶段

是否真的重新变成 `HOT`，还要继续看：

- `admissions` 有没有增长

#### 16.29.2 `fallback_runtime` 增长，也不等于“这次执行彻底没有重学”

从 `S4` 这组可见，当前分支上完全可能出现：

```text
同一次 EXECUTE
  先 fast path guard 失败
  记一次 fallback_runtime
  再走正常优化器
  最后又完成一次 admission
```

也就是说：

- `fallback_runtime` 记录的是“快走失败了”
- 不是“这次最终一定没有重新进入 HOT”

#### 16.29.3 `invalidations=0`，并不代表“表变化没有影响 plan cache”

`S5` 和 `S7` 都显示：

- `Com_stmt_reprepare=1`
- 但 `Ps_point_plan_cache_invalidations=0`

这说明在当前实现里，DDL / table-version 漂移更多是通过 **通用 PS reprepare 链** 被处理，而不是简单映射成一个 session 级 `invalidations++`。

所以如果你在现场排障，千万不要只盯着 `Ps_point_plan_cache_invalidations` 一个数。

#### 16.29.4 `.test` 里的 “expect 注释” 不是最终真理，`.result` 才是当前分支被校验的真值

这次实测里，最典型的是：

- `mysql-test/t/ps_point_plan_cache_runtime_drift.test`
- `mysql-test/t/ps_point_plan_cache_env_drift.test`

它们的注释文字还写着旧预期，但当前仓库中真正 `pass` 的 `.result` 已经体现出更新后的真实行为。  
因此做知识梳理时，优先级应该是：

```text
当前工作树的源码
  > 当前工作树能跑通的 MTR 结果
  > 当前仓库 checked-in 的 .result
  > .test 里的 expect 注释
```

这一条很重要，因为 plan cache 正在快速演进，注释往往比实现和结果文件更容易滞后。

## 17. Commit 时间线：它是怎么一步步长成现在这样的

下面按时间顺序看关键提交。  
这是理解“为什么代码看起来像现在这样”的最好方式。

### 17.1 2026-04-04 `fd3c55d8d93`

提交：`ps_point_plan_cache: Phase 0 skeleton + design docs`

这一步是搭骨架，不改变执行路径。

主要建立：

- `PsPointPlanState`
- `PsPointPlanTemplate`
- sysvar `ps_point_plan_cache`
- status counters
- `Prepared_statement` 上的 plan cache 状态字段
- 一整套 design 文档

关键意义：

> 先把“承载结构”和“设计边界”搭好，再做真正的优化逻辑。

### 17.2 2026-04-05 `adbb3d279e7`

提交：`ps_point_plan_cache: Phase 1 static classification at PREPARE time`

这一步实现：

- `ps_point_plan_extract_where_shape()`
- `ps_point_plan_classify()`
- prepare 阶段 classify hook

关键意义：

> 先用很轻的 AST shape 检查把大多数非候选语句挡在门外。

### 17.3 2026-04-05 `f4afd9f64de`

提交：`ps_point_plan_cache: Phase 2 admission after first normal optimization`

这一步实现：

- `ps_point_plan_can_admit()`
- `ps_point_plan_admit()`
- admission hook

关键意义：

> 第一次把“prepare 阶段的语义候选”变成“优化器证明过的真实可缓存计划”。

也是从这一步开始，这套实现不再只是“shape classifier”，而是真正开始缓存计划模板。

### 17.4 2026-04-05 `56a0e45bd1b`

提交：`ps_point_plan_cache: Phase 3 fast path for HOT prepared statements`

这一步真正引入：

- runtime guard
- `ps_point_plan_build_fast_path()`
- `JOIN::optimize()` 里的 fast path hook

关键意义：

> 这才是第一次真正“省掉优化器工作量”的版本。

### 17.5 2026-04-06 `8d35f9a45ec`

提交：`ps_point_plan_cache: Phase 4 reprepare/invalidation/fallback polishing`

这一步重点解决执行生命周期边角：

- cursor 场景 bypass
- `execute()` 退出时清 runtime state
- 边界测试和 cursor protocol 测试

关键意义：

> 它解决的不是“更快”，而是“默认 ON 时不出幺蛾子”。

### 17.6 2026-04-07 `36b953be35d`

提交：`ps_point_plan_cache V1.1: Index_lookup caching, env guards, type coverage`

这是第一个明显的“性能 + 健壮性双提升”版本。

新增重点：

- `Index_lookup` 相关 helper 缓存到 PS arena
- `store_key` / `Field clone` 复用
- `retryable_cold`
- `sql_mode` guard
- type / env drift 测试矩阵

关键意义：

> 从“只缓存元数据”进入“开始缓存 helper 对象”的阶段。

### 17.7 2026-04-07 `9b35e825340`

提交：`ps_point_plan_cache: harden classification gate and fix arena leak on invalidation`

这一步做了两类事情：

- 修正 classify gate 的健壮性
- 增加 coverage 测试

但从后续 review 记录看，这一阶段对“保留 helper cache 以减少重建”的策略有些过于乐观，后来又引出了“re-admission 可能复用 stale helper”这一轮新问题。

换句话说，这个提交的重要性不只在它改了什么，还在它暴露了新的生命周期矛盾：

```text
想复用更多 helper
vs
不能复用错 helper
```

这是 V1.2 和当前工作树 WIP 继续收敛的背景。

### 17.8 2026-04-08 `12480223920`

提交：`ps_point_plan_cache V1.2: QEP caching, defensive hardening, and extended test coverage`

这是当前已提交代码的成熟版本。

核心变化：

- fast path hook 提前到 `JOIN::optimize()` 入口
- 缓存 QEP skeleton
- 用 placement-new 解决 `QEP_TAB` / `QEP_shared` 的 one-shot 初始化断言问题
- invalidation 不再把状态扔进不可恢复的死角
- 对 `table_ref_version` 漂移更谨慎
- coverage 测试继续扩展

关键意义：

> 这是“更早截断优化器 + 更深缓存 + 更强 defensive hardening”的版本。

## 18. 本地 review 记录告诉了我们什么

如果只看 commit，很容易以为 V1.2 已经“差不多完了”。  
但本地 `.code_review/` 记录能看到非常有价值的一层：

### 18.1 第一轮 review 识别出 3 类核心风险

`.code_review/plan_cache_code_review_dismiss_problem` 里指出了 3 个问题：

1. `QEP_TAB/QEP_shared` 跨执行复用会撞上 one-shot 初始化断言
2. re-admission 可能复用第一次 plan 的 stale helper
3. `INVALID` 可能形成死状态

### 18.2 V1.2 实际上已经修掉了前两类中的一部分

后续 review 反馈显示：

- `INVALID` 死状态问题已修
- `QEP_TAB/QEP_shared` one-shot 问题已通过 placement-new 修复

但“retryable demote 后 helper 复用是否真的安全”这个问题仍然残留。

### 18.3 当前工作树 WIP 继续沿着这个问题深入

也就是说，当前未提交改动并不是随意的小修，而是明确延续了 review 的主线：

> 把 helper 复用从“理论上大多安全”推进到“代码上明确验证兼容再复用”。

这是理解当前 WIP 的最好入口。

## 19. 当前未提交代码：它在补什么

`git status` 显示当前有 4 个与 plan cache 相关的未提交修改：

- `sql/ps_point_plan_cache.cc`
- `sql/ps_point_plan_cache.h`
- `sql/sql_prepare.h`
- `sql/sql_select.cc`

这组改动的主线非常清晰：**继续收紧 helper cache 的正确性边界**。

一个顺手能看出来的小结论是：

- 当前工作树里没有看到与这 4 个代码改动配套的未提交 `mysql-test` 变更

所以更准确地说，这一轮更像“正确性 hardening 正在收口中”的代码改动，而不是“代码和测试已经一起收尾”的最终提交态。

### 19.1 WIP 变化一：`sql_mode` guard 继续扩大

如前所述，当前工作树把 relevant `sql_mode` mask 扩大到了更多位。

这说明当前实现比早期 design 更谨慎，倾向于：

- 宁可多一次 `HOT -> COLD`
- 也不要在可能受 `sql_mode` 影响的序列化路径上复用旧模板

### 19.2 WIP 变化二：helper compatibility 检查从“字段序号相同”升级为“布局也相同”

当前未提交代码给 `PsPointPlanTemplate` 新增了：

- `cached_part_lengths[]`
- `cached_part_store_lengths[]`

并新增了 `ps_point_plan_cached_helpers_compatible()`。

这意味着现在判断“旧 helper 能不能复用”时，不再只看：

- field index 是否一致

而是进一步看：

- `key_parts` 是否一致
- `key_length` 是否一致
- 每个 key part 的 `length` 是否一致
- 每个 key part 的 `store_length` 是否一致

这是一个很关键的升级。

因为对 `store_key` / key buffer 来说，**字段相同不等于二进制布局相同**。  
只要序列化长度或存储布局变了，继续复用旧 helper 就可能把 key 写坏。

### 19.3 WIP 变化三：compatibility 检查的时机前移到了“覆盖新元数据之前”

当前 `ps_point_plan_admit()` 先把本次优化得到的：

- `field_indices[]`
- `actual_types[]`
- unsigned
- collation

放进局部变量，然后在真正覆盖 `tpl` 之前做 compatibility 检查。

这一步非常重要，因为它避免了“先把旧快照覆盖掉，再拿已经变新的 `tpl` 去验证旧 helper”的时序错误。

简化理解：

```text
旧 cache 是否兼容新计划
必须在“旧 cache 还可见、而新 metadata 也已算出”的瞬间判断
```

这正是当前 WIP 在做的事情。

### 19.4 WIP 变化四：helper 构建改成“先局部成功，再整体提交”

当前未提交代码不再一边构建一边直接往 `tpl.cached_*` 里写，而是先写到局部变量：

- `cached_key_buff`
- `cached_key_buff2`
- `cached_store_keys[]`
- `cached_to_fields[]`
- `cached_part_lengths[]`
- `cached_part_store_lengths[]`

只有整体构建成功，才一次性写回 `tpl`。

这其实和 fast path 的 delayed-write 原则是一致的，只不过这里应用到了“缓存 helper 的 admission 构建”上。

作用是：

- 避免 partial cache state
- 避免 `tpl.ref_cached == true` 但里面混着半初始化指针
- 更容易推断失败路径

### 19.5 WIP 变化五：`ps_point_plan_bind_cached_ref_parts()` 统一复用路径

当前 WIP 把“把 cached helper 重新绑定到当前 `TABLE *` 和当前 `Index_lookup`”的逻辑抽出来做成：

- `ps_point_plan_bind_cached_ref_parts()`

这让两个路径都共享同一套检查：

- fully cached path
- ref cached path

好处是：

- 逻辑集中
- 空指针检查集中
- 更不容易出现一条路径漏掉某个赋值

### 19.6 WIP 变化六：`store_key` 本身也被补强了空指针防御

工作树里 `sql/sql_select.cc` 的改动主要是：

- 构造函数显式把 `to_field` 初始化为 `nullptr`
- 分配失败时不立刻解引用 `to_field`
- `store_key::copy()` 如果 `to_field == nullptr`，直接返回 `STORE_KEY_FATAL`

这类修改很像“最后一道保险丝”。

它的意义不是提高命中率，而是防止：

- helper 构造失败
- Field clone 分配失败
- 某些极端内存/错误路径下的空指针使用

从架构上看，这说明当前作者已经把注意力从“主干 happy path 跑通”转向了“失败路径也必须可解释、可收敛”。

### 19.7 WIP 的整体结论

如果把当前未提交改动概括成一句话，那就是：

> 它在把 plan cache 从“缓存更多东西”继续推进到“只有明确兼容时才缓存并复用这些东西”。

这其实是一个非常健康的方向。

## 20. 为什么这套实现的收益通常不是夸张的大幅提升

看 benchmark 和 design，会发现一个常见现象：

- 这套优化通常有正收益
- 但收益经常是个位数百分比
- 很少出现“翻倍”这种夸张数字

这不是实现无效，而是符合系统结构。

### 20.1 它省掉了什么

主要省掉的是：

- optimizer preamble
- `make_join_plan()` 主路径
- 一些 per-execution helper 构造

### 20.2 它没省掉什么

每次执行仍然需要：

- open tables / lock tables
- fresh `JOIN`
- 一部分 `QEP` / `AccessPath` / iterator 构造
- `construct_lookup()`
- `store_key::copy()`
- `handler::ha_index_read_map()`
- InnoDB 真正的点查路径
- 协议层和结果返回

所以对于热点数据已在 buffer pool 的点查，CPU 时间的大头经常仍在：

- 执行器
- 存储引擎
- 协议栈

优化器只是一部分。

### 20.3 仓库里的实测结果也支持这个判断

例如 `bench/ps_point_plan_cache/results/20260406_114436/report.md` 里：

- `Point Select` 在 1 / 8 / 32 / 64 / 128 线程下大致是 `+2.30% / +2.31% / +2.76% / +0.56% / +2.00%`
- `Read Only` 也有小幅正收益，但幅度更小

而 `design/plan_cache_v1_phase5_implementation.md` 对 V1 的结论也很明确：

- 常见是约 `1%–5%`
- 没有回退比“强行追求 >5%”更重要
- 想继续大幅提升，必须扩大缓存范围或更深缓存

这和 `design/ps_plan_cache_v2_deep_caching_analysis.md` 的判断是吻合的。

## 21. 这套设计最值得学习的几个工程原则

如果你不是为了改 plan cache，而是为了学习优化器工程，这几条尤其值得记住。

### 21.1 正确性优先于命中率

整套实现宁可：

- 早退出
- 降回 `COLD`
- 本次 fallback

也不愿在 helper 生命周期有疑点时硬命中。

### 21.2 先做极窄场景，再逐步加深缓存

演进路径很清楚：

- Phase 0：先搭骨架
- Phase 1：先只做 classify
- Phase 2：再做 admission
- Phase 3：再做 fast path
- V1.1：再缓存 `Index_lookup` helper
- V1.2：再缓存 QEP skeleton
- 当前 WIP：再把 helper compatibility 和失败路径收紧

这是一条很典型的内核优化演进路线。

### 21.3 “缓存更多对象”永远伴随着“证明生命周期安全”

V1.1/V1.2/WIP 最核心的矛盾就是：

```text
想减少每次执行构造开销
就要缓存更多 helper

但缓存更多 helper
就必须证明这些 helper 在下次执行时仍然语义正确
```

所以每一轮 deeper cache，都会伴随一轮新的 guard、compatibility check 和 invalidation 设计。

### 21.4 失败路径要可恢复

`INVALID` 死状态之所以后来被修掉，本质上就是因为：

> 一个无法自动恢复的失败状态，在长生命周期的 PS 系统里是很危险的。

这对所有缓存系统都适用。

## 22. 接下来应该怎样读代码

如果你准备真正开始读这套实现，推荐按下面顺序。

### 22.1 第一遍：先建立全局图

先读：

- `design/mysql_query_optimizer_architecture.md`
- `design/ps_point_plan_cache_v1_design.md`
- 本文

目标不是抠细节，而是先搞清：

- SQL 的四层 plan 表示
- PS 为什么每次还要 optimize
- 这套 plan cache 为什么只能做模板缓存

### 22.2 第二遍：看 hook 和状态机

再读：

- `sql/sql_prepare.cc`
- `sql/sql_prepare.h`
- `sql/sql_optimizer.cc`

重点看：

- classify hook
- admission hook
- early fast-path hook
- reprepare swap
- runtime state reset

### 22.3 第三遍：啃核心实现

然后集中读：

- `sql/ps_point_plan_cache.h`
- `sql/ps_point_plan_cache.cc`

建议按函数顺序读：

1. `ps_point_plan_classify`
2. `ps_point_plan_extract_where_shape`
3. `ps_point_plan_can_admit`
4. `ps_point_plan_admit`
5. `ps_point_plan_runtime_guard`
6. `ps_point_plan_build_fast_path`

### 22.4 第四遍：补索引执行细节

再读：

- `sql/sql_select.cc`
- `sql/sql_executor.cc`
- `sql/iterators/ref_row_iterators.cc`

重点理解：

- `init_ref`
- `init_ref_part`
- `store_key`
- `construct_lookup`
- `EQRefIterator::Read`

这一步会把“模板缓存”真正落到“索引 key 是怎么被构出来”的层面。

### 22.5 第五遍：用测试反推边界

最后看测试：

- `mysql-test/t/ps_point_plan_cache_classify.test`
- `mysql-test/t/ps_point_plan_cache_admission.test`
- `mysql-test/t/ps_point_plan_cache_fast_path.test`
- `mysql-test/t/ps_point_plan_cache_edge.test`
- `mysql-test/t/ps_point_plan_cache_env_drift.test`
- `mysql-test/t/ps_point_plan_cache_runtime_drift.test`
- `mysql-test/t/ps_point_plan_cache_coverage.test`

如果能把这些测试为什么存在讲清楚，你基本就真的掌握这套代码了。

### 22.6 如果你手里有工程语义索引，优先串这 12 个符号

如果你不是纯 `grep` 阅读，而是手里已经有全工程符号索引，那么最值得串起来的一组 symbol 是：

1. `Prepared_statement::prepare`
2. `ps_point_plan_classify`
3. `Prepared_statement::execute_loop`
4. `Prepared_statement::check_parameter_types`
5. `open_tables_for_query`
6. `check_and_update_table_version`
7. `Prepared_statement::reprepare`
8. `update_ref_and_keys`
9. `Optimize_table_order::best_access_path`
10. `create_ref_for_key`
11. `construct_lookup`
12. `EQRefIterator::Read`

这 12 个点基本覆盖了：

- prepare 阶段
- first execute 正常优化阶段
- metadata 失效与 reprepare 阶段
- fast path 执行阶段
- 最终落到存储引擎读 key 的阶段

也就是说，如果你用索引工具顺着这组符号往下看，看到的几乎就是整套 plan cache 的骨架。

### 22.7 如果要继续下潜到存储引擎，下一跳看哪里

如果你已经把 SQL 层看明白，接下来最自然的下一跳是：

```text
construct_lookup()
  -> handler::ha_index_read_map()      // sql/handler.cc
    -> ha_innobase::*                  // storage/innobase/handler/ha_innodb.cc
      -> row_search_mvcc()             // storage/innobase/row/row0sel.cc
```

这条链解释了为什么 benchmark 里经常看到：

- plan cache 明明已经命中
- `JOIN::optimize()` 的样本明显下降
- 但总 QPS 只提升几个点

原因不是 plan cache 没起作用，而是热点点查的大头常常已经落在：

- handler 层接口
- InnoDB 行搜索
- buffer pool / latch / MVCC 可见性判断

所以从整个工程视角看，plan cache 优化的是“SQL 层的计划构建成本”，不是“存储引擎层的查行成本”。

### 22.8 结合 `/Users/a1234/project/mysql_code_knowledge_base`，先建立一张“大地图”

这份知识库的语义索引 scope 明确覆盖：

- `sql/`
- `storage/innobase/`
- `include/`
- `mysys/`
- `vio/`

按 `indexes/core_counts.tsv` 和 `indexes/serena_index_summary.json`，它覆盖了：

- `sql`：1517 个文件，其中 1496 个代码文件
- `storage/innobase`：549 个文件，其中 465 个代码文件
- `include`：448 个文件，其中 422 个代码文件
- `mysys`：119 个文件，其中 116 个代码文件
- `vio`：11 个文件，其中 10 个代码文件
- 总计：2509 个文件被完整语义索引，失败数为 0

这组数字很重要，因为它告诉你：

- 对理解 `ps_point_plan_cache`，知识库已经足够覆盖“优化器 + 执行器 + InnoDB 点查”的主骨架
- 但它的重点仍然是 `sql/` 和 `storage/innobase/`
- `mysys/`、`vio/` 对本文不是主战场，只是公共底层设施

如果你想先建立目录级直觉，`indexes/core_tree.txt` 给出的最有价值几条分支是：

- `sql/join_optimizer`
- `sql/range_optimizer`
- `sql/iterators`
- `sql/dd`
- `storage/innobase/handler`
- `storage/innobase/row`

其中和本文最直接相关的是：

- `sql/`：Prepared Statement、legacy optimizer、QEP、executor 都在这里
- `storage/innobase/handler`：SQL 层到 InnoDB 的 handler 边界
- `storage/innobase/row`：真正做 MVCC 行搜索的地方

### 22.9 从知识库热点文件看，这条链最该先读哪几处

知识库的 `core_hot_files.tsv` 和 `serena_file_symbol_counts.tsv` 其实已经帮我们筛过一遍“哪些文件信息密度最高”。

对 `ps_point_plan_cache` 这条链，最值得优先建立定位感的文件是：


| 文件                                      | 知识库里的符号量 | 为什么重要                                                        |
| --------------------------------------- | -------- | ------------------------------------------------------------ |
| `sql/table.h`                           | 1078     | `TABLE`、字段、索引元数据的大本营；很多对象最终都要落回这里                            |
| `sql/join_optimizer/access_path.h`      | 515      | 统一物理计划层 `AccessPath` 的总定义                                    |
| `sql/sql_select.h`                      | 187      | `Key_use`、`POSITION`、`store_key` 等核心结构定义                     |
| `sql/sql_prepare.cc`                    | 179      | PS 执行、参数检查、reprepare 主流程                                     |
| `sql/sql_executor.h`                    | 170      | `QEP_TAB` 定义和执行期桥接对象                                         |
| `sql/sql_opt_exec_shared.h`             | 170      | `Index_lookup`、`join_type`、`QEP_shared` 所在地                  |
| `sql/sql_optimizer.cc`                  | 170      | `JOIN::optimize`、`update_ref_and_keys`、QEP 构造主逻辑             |
| `sql/sql_select.cc`                     | 123      | `init_ref` / `init_ref_part` / `create_ref_for_key` 在这里落地    |
| `sql/sql_executor.cc`                   | 110      | `QEP_TAB::access_path`、`read_const`、`construct_lookup` 在这里会合 |
| `sql/sql_planner.cc`                    | 48       | `find_best_ref`、`best_access_path` 真正做“挑谁更便宜”                |
| `sql/iterators/ref_row_iterators.cc`    | 45       | `EQRefIterator::Read` 真正读唯一点查                                |
| `sql/key.h`                             | 71       | `KEY`、`KEY_PART_INFO` 的静态索引元数据                               |
| `storage/innobase/handler/ha_innodb.cc` | 2401     | SQL 层点查最终进入 InnoDB 的巨大入口文件                                   |
| `storage/innobase/row/row0sel.cc`       | 72       | `row_search_mvcc()` 所在地，点查真正的行搜索核心                           |


读法上可以分成两层。

第一层是“概念定义层”，先读：

- `sql/sql_opt_exec_shared.h`
- `sql/sql_select.h`
- `sql/key.h`
- `sql/table.h`
- `sql/join_optimizer/access_path.h`

第二层是“动作落地层”，再读：

- `sql/sql_prepare.cc`
- `sql/sql_optimizer.cc`
- `sql/sql_planner.cc`
- `sql/sql_select.cc`
- `sql/sql_executor.cc`
- `sql/iterators/ref_row_iterators.cc`
- `storage/innobase/handler/ha_innodb.cc`
- `storage/innobase/row/row0sel.cc`

这种顺序的好处是：  
你先知道对象“是什么”，再去看对象“怎么流动”，认知负担会小很多。

### 22.10 从知识库根符号索引看，真正串起全链路的是哪些入口

知识库里的 `serena_root_symbols.jsonl` 和 `focused_cpp.tags` 给了一个很有价值的信息：  
它们不只是告诉你文件存在，还告诉你“哪些函数/类型就是一等入口点”。

知识库明确能定位到下面这组符号：

- `JOIN::optimize` 在 `sql/sql_optimizer.cc`
- `update_ref_and_keys` 在 `sql/sql_optimizer.cc`
- `Optimize_table_order::find_best_ref` 在 `sql/sql_planner.cc`
- `Optimize_table_order::best_access_path` 在 `sql/sql_planner.cc`
- `create_ref_for_key` 在 `sql/sql_select.cc`
- `init_ref` 在 `sql/sql_select.cc`
- `init_ref_part` 在 `sql/sql_select.cc`
- `QEP_TAB::access_path` 在 `sql/sql_executor.cc`
- `construct_lookup` 在 `sql/sql_executor.cc`
- `read_const` 在 `sql/sql_executor.cc`
- `NewEQRefAccessPath` 在 `sql/join_optimizer/access_path.h`
- `EQRefIterator::Read` 在 `sql/iterators/ref_row_iterators.cc`
- `Index_lookup` 在 `sql/sql_opt_exec_shared.h`
- `POSITION` 在 `sql/sql_select.h`
- `row_search_mvcc` 在 `storage/innobase/row/row0sel.cc`

也就是说，知识库本身已经把这条主链的“老骨架”勾出来了：

```text
JOIN::optimize
  -> update_ref_and_keys
  -> Optimize_table_order::best_access_path
  -> Optimize_table_order::find_best_ref
  -> create_ref_for_key
  -> init_ref / init_ref_part
  -> QEP_TAB::access_path
  -> NewEQRefAccessPath
  -> construct_lookup / read_const
  -> EQRefIterator::Read
  -> handler::ha_index_read_map
  -> row_search_mvcc
```

这条链几乎正好就是我们在本文里反复讲的：

- 候选提取
- 成本比较
- 选中 key
- 构建 `Index_lookup`
- 转成 `AccessPath`
- 落入 iterator
- 落入 handler
- 落入 InnoDB

换句话说，知识库对“点查从 optimizer 到 InnoDB 的原生骨架”支持是非常强的。

### 22.11 把知识库地图翻译成“阅读任务单”

如果你真要靠这份知识库继续深挖，最有效的不是泛读，而是按问题拆开读。

问题一：PS 为什么需要单独的 plan cache，而不是直接复用旧 `JOIN`？

先看：

- `sql/sql_prepare.h`
- `sql/sql_prepare.cc`
- `sql/sql_optimizer.cc`

因为这会让你先看明白：

- `Prepared_statement` 才是跨执行稳定对象
- `JOIN` 是按执行重建的运行时对象
- reprepare、metadata 检查、参数类型合同都围绕 PS 生命周期展开

问题二：第一次正常执行里，优化器到底怎么决定“这是唯一点查”？

先看：

- `sql/sql_optimizer.cc`
- `sql/sql_planner.cc`
- `sql/sql_select.cc`

重点串：

- `update_ref_and_keys`
- `Optimize_table_order::best_access_path`
- `Optimize_table_order::find_best_ref`
- `create_ref_for_key`

因为这四步基本就是：

- 提取候选
- 比较成本
- 选中最优
- 产出最终 `Index_lookup`

问题三：为什么 `Index_lookup` / `store_key` / `KEY_PART_INFO` 是 plan cache 最难做对的一层？

先看：

- `sql/sql_opt_exec_shared.h`
- `sql/sql_select.h`
- `sql/sql_select.cc`
- `sql/key.h`
- `sql/table.h`

这会让你明白：

- 静态索引元数据长什么样
- 当前参数值如何映射到每个 key part
- 类型/NULL/collation/布局变化为什么会破坏 helper 复用

问题四：fast path 命中后，到底怎样真正读到这一行？

先看：

- `sql/sql_executor.cc`
- `sql/iterators/ref_row_iterators.cc`
- `sql/handler.cc`
- `storage/innobase/handler/ha_innodb.cc`
- `storage/innobase/row/row0sel.cc`

这会让你真正理解：

- `AccessPath` 怎么变成 `RowIterator`
- `construct_lookup()` 才是参数值真正落地为 key bytes 的地方
- 计划缓存优化掉的是“建计划”，不是“查行”

### 22.12 这份知识库的强项和盲区，必须分开看

如果把这份知识库和当前工作树混成一份材料，你反而会越读越乱。  
最好的做法，是明确把它分成“强项”和“盲区”。

它的强项是：

- `JOIN` / `QEP_TAB` / `QEP_shared` / `AccessPath` / `Index_lookup` 这些老对象的稳定定义
- optimizer 到 executor 到 InnoDB 的主干调用链
- 目录、文件、符号三级地图
- 大文件热点和语义索引，帮助你快速判断“先读哪里更值”

它的盲区是：

- `sql/ps_point_plan_cache.h`
- `sql/ps_point_plan_cache.cc`
- 2026-04-04 之后那几组 plan cache commit 的新增逻辑
- 当前 2026-04-10 工作树里还未提交的 WIP hardening

所以最正确的读法不是“只靠知识库”，而是：

```text
知识库
  负责解释老骨架

commit 时间线
  负责解释 plan cache 是怎样嫁接到老骨架上的

当前工作树
  负责解释为什么 helper cache / metadata / sql_mode / Field 布局
  还需要继续补 correctness
```

你也可以把本文前面所有章节压缩成一句方法论：

> 先用知识库把“原生优化器/执行器/InnoDB 地图”立起来，  
> 再用 commit 把 `ps_point_plan_cache` 这层新逻辑覆盖上去，  
> 最后再用未提交代码去理解最后一层边界修补。

## 23. 最后做一个总括

`ps_point_plan_cache` 的本质，不是“在 MySQL 里补一个通用 plan cache”。

它更准确的定义是：

> 在 Prepared Statement 这条已经有稳定语法树和稳定参数位的链路上，  
> 针对“单表唯一键等值点查”这一极窄而高频的场景，  
> 缓存足够稳定的模板和 helper，  
> 然后在后续执行里用更低的成本重建最小 fresh plan。

如果你把这句话真正理解了，下面这些看起来复杂的细节就都会顺起来：

- 为什么要分 `classify -> admission -> fast path`
- 为什么 prepare 阶段不能直接缓存 plan
- 为什么 admission 看的是 `JT_CONST`
- 为什么 fast path 造的是 `JT_EQ_REF`
- 为什么 `TABLE *`、`JOIN *`、`AccessPath *` 不能简单跨执行复用
- 为什么 V1.1/V1.2/WIP 都在围绕 helper cache 的生命周期打磨
- 为什么收益通常是正但有限

最后一句话总结这条演进线：

```text
Phase 0-3 解决“能不能做”
Phase 4-V1.2 解决“能不能稳地做”
当前未提交 WIP 在解决“能不能更严格、更可证明地做”
```

这正是一个成熟内核优化功能该有的成长轨迹。