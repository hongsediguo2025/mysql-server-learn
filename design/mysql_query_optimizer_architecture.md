# MySQL 查询优化器完整架构指南

> 面向存储引擎工程师的查询优化器速成手册
> 从 plan cache 出发，系统掌握 SQL 引擎核心

## 目录

1. [全局视角：一条 SQL 的完整生命周期](#1-全局视角一条-sql-的完整生命周期)
2. [核心数据结构图谱](#2-核心数据结构图谱)
3. [解析层：从文本到语法树](#3-解析层从文本到语法树)
4. [名称解析层：语义绑定](#4-名称解析层语义绑定)
5. [优化层：Legacy Optimizer（当前默认）](#5-优化层legacy-optimizer当前默认)
6. [优化层：Hypergraph Optimizer（新一代）](#6-优化层hypergraph-optimizer新一代)
7. [代价模型](#7-代价模型)
8. [执行层：Iterator 模型](#8-执行层iterator-模型)
9. [Plan 的四层表示](#9-plan-的四层表示)
10. [Prepared Statement 与优化器的交互](#10-prepared-statement-与优化器的交互)
11. [Plan Cache：从点查快路径到通用缓存](#11-plan-cache从点查快路径到通用缓存)
12. [与存储引擎的接口边界](#12-与存储引擎的接口边界)
13. [关键源文件速查表](#13-关键源文件速查表)
14. [学习路径与阅读顺序](#14-学习路径与阅读顺序)

---

## 1. 全局视角：一条 SQL 的完整生命周期

```
客户端 SQL 文本
    │
    ▼
┌─────────────────────────────────────────────────┐
│ 1. PARSE (sql/sql_yacc.yy → LEX + Item tree)    │
│    输出: LEX, Query_expression, Query_block,     │
│          Table_ref, Item 树                       │
└─────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────┐
│ 2. RESOLVE (sql/sql_resolver.cc)                 │
│    名称解析、类型推导、子查询转换、视图展开       │
│    输出: 绑定后的 Item 树 + Table_ref 链          │
└─────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────┐
│ 3. OPTIMIZE                                      │
│    ┌──────────────────┐  ┌────────────────────┐  │
│    │ Legacy Optimizer │  │ Hypergraph Optimizer│  │
│    │ (sql_planner.cc) │  │ (join_optimizer/)  │  │
│    │ 贪心前缀搜索     │  │ DPhyp 枚举         │  │
│    └──────┬───────────┘  └──────┬─────────────┘  │
│           │                     │                 │
│           ▼                     ▼                 │
│    JOIN_TAB/QEP_TAB        AccessPath 树          │
│    join order + ref        (统一计划表示)          │
│           │                     │                 │
│           └─────────┬───────────┘                 │
│                     ▼                             │
│              AccessPath 树                        │
│              (两条路径最终都生成)                  │
└─────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────┐
│ 4. EXECUTE                                       │
│    CreateIteratorFromAccessPath()                 │
│    → RowIterator 树                              │
│    → handler::ha_index_read / ha_rnd_next ...    │
└─────────────────────────────────────────────────┘
    │
    ▼
  结果集返回客户端
```

### 入口函数调用链（SELECT 为例）

```
mysql_execute_command()                   // sql/sql_cmd_dml.cc
  → Sql_cmd_dml::execute()
    → Sql_cmd_dml::execute_inner()
      → Query_expression::optimize()      // sql/sql_union.cc
        → Query_block::optimize()         // sql/sql_select.cc
          → JOIN::optimize()              // sql/sql_optimizer.cc  ★核心★
      → Query_expression::execute()
        → CreateIteratorFromAccessPath()  // sql/join_optimizer/access_path.cc
        → RowIterator::Read() 循环
```

---

## 2. 核心数据结构图谱

```
LEX (语句级顶层容器)
 ├── sql_command: SQLCOM_SELECT / SQLCOM_UPDATE / ...
 ├── Query_expression *unit         (查询表达式，即 UNION/INTERSECT/EXCEPT 的根)
 │   ├── Query_term 树              (SQL 标准的 query expression 抽象)
 │   ├── Query_block *first_query_block()
 │   │   ├── Table_ref *leaf_tables (叶表链表，优化器真正操作的表)
 │   │   ├── Item *where_cond       (WHERE 条件)
 │   │   ├── Item *having_cond      (HAVING 条件)
 │   │   ├── ORDER *order_list      (ORDER BY)
 │   │   ├── ORDER *group_list      (GROUP BY)
 │   │   ├── JOIN *join             (优化后指向 JOIN 对象) ★
 │   │   └── Query_expression *first_inner_query_expression() (子查询)
 │   ├── m_root_access_path         (hypergraph 路径最终 plan)
 │   └── m_root_iterator            (执行器 iterator 根)
 └── using_hypergraph_optimizer()   (是否启用新优化器)

JOIN (per-query-block 优化器 + 运行时协调器)
 ├── Query_block *query_block
 ├── THD *thd
 ├── tables / primary_tables / const_tables  (表计数)
 ├── table_count / const_table_map
 ├── Key_use_array keyuse_array     (可用索引引用)
 ├── JOIN_TAB *join_tab             (Legacy: 优化期间的每表槽位)
 ├── QEP_TAB *qep_tab              (Legacy: 执行计划每表槽位)
 ├── POSITION *best_positions       (Legacy: 最优 join order)
 ├── AccessPath *m_root_access_path (最终执行计划) ★
 ├── RowIterator *m_root_iterator   (执行器根)
 ├── where_cond / having_cond       (当前条件)
 ├── best_read / best_rowcount      (最优估算)
 └── optimize() / create_iterators() / ...

AccessPath (统一的物理计划节点，tagged union)
 ├── type: TABLE_SCAN / INDEX_SCAN / REF / EQ_REF /
 │         NESTED_LOOP_JOIN / HASH_JOIN / SORT /
 │         AGGREGATE / MATERIALIZE / FILTER / ...
 ├── num_output_rows / cost / init_cost / init_once_cost
 ├── 各 type 特有的 payload (union 成员)
 └── 可递归组合成树

Table_ref (解析树中的表引用)
 ├── TABLE *table                   (open table 后的物理表)
 ├── db / table_name / alias
 ├── select_lex (所属 Query_block)
 ├── join_cond() (ON 条件)
 └── nested_join / embedding (嵌套/外连接结构)

QEP_TAB (Legacy 优化器: 查询执行计划中的单表槽位)
 ├── TABLE *table()
 ├── Table_ref *table_ref
 ├── join_type type() (JT_EQ_REF / JT_REF / JT_ALL / ...)
 ├── Index_lookup &ref() (索引查找参数)
 ├── Item *condition()
 ├── AccessPath *access_path() (转换为 AccessPath)
 └── QUICK *quick() (range 优化器结果)
```

---

## 3. 解析层：从文本到语法树

### 关键文件

| 文件 | 职责 |
|------|------|
| `sql/sql_yacc.yy` | Bison 语法文件，定义 SQL 语法规则 |
| `sql/sql_lex.h` | `LEX`、`Query_block`、`Query_expression` 定义 |
| `sql/sql_lex.cc` | 词法/语法支撑函数 |
| `sql/item.h` / `sql/item_*.h` | 表达式节点（`Item` 层次体系） |
| `sql/parse_tree_*.h` | 语法树中间节点（PT_xxx） |

### 核心概念

- **LEX**: 一条 SQL 语句的解析结果容器。一个 `THD` 在某一时刻只有一个活跃 `LEX`。
- **Query_expression**: 对应 SQL 标准的 "query expression"，即 `SELECT ... UNION SELECT ...` 这样的整体。简单 SELECT 也是一个退化的 `Query_expression`。
- **Query_block**: 对应 SQL 标准的 "query specification"，即一个 `SELECT ... FROM ... WHERE ...` 子句。每个 `Query_block` 在优化阶段会生成一个 `JOIN` 对象。
- **Item**: 所有表达式的基类。`Item_field` 是列引用，`Item_int` 是整数常量，`Item_func_eq` 是等值比较，`Item_param` 是 prepared statement 的参数占位符。

### 从存储引擎视角理解

你熟悉的 `TABLE`（`handler` 对象的宿主）在这一层还不可见。解析器只创建 `Table_ref`（表名引用），真正的 `TABLE *` 在后续 open_tables 阶段才会关联上。

---

## 4. 名称解析层：语义绑定

### 关键文件

| 文件 | 职责 |
|------|------|
| `sql/sql_resolver.h/.cc` | 名称解析主逻辑 |
| `sql/item.h` | `Item::fix_fields()` — 表达式的语义绑定 |
| `sql/sql_derived.cc` | derived table / CTE 的处理 |

### 核心流程

1. **open_tables_for_query()**: 打开所有引用的表，建立 `Table_ref → TABLE` 的绑定。这一步是优化器与存储引擎的第一个交汇点——会调用 `handler::open()`。
2. **Query_block::prepare()**: 解析列名、函数名，做类型推导、隐式转换，展开 `*`，处理子查询转换（如 semi-join 转换）。
3. **Item::fix_fields()**: 每个 `Item` 节点的语义绑定入口。`Item_field` 在这里关联到具体的 `Field` 对象（而 `Field` 是你熟悉的 —— 它直接对应 InnoDB 行中的列）。

---

## 5. 优化层：Legacy Optimizer（当前默认）

### 关键文件

| 文件 | 职责 |
|------|------|
| `sql/sql_optimizer.h/.cc` | `JOIN` 类定义与 `JOIN::optimize()` 主流程 |
| `sql/sql_planner.h/.cc` | 贪心 join order 搜索（`Optimize_table_order`） |
| `sql/sql_select.h` | `Key_use`、`POSITION` 等辅助结构 |
| `sql/opt_costmodel.h/.cc` | 服务端代价模型 |
| `sql/range_optimizer/` | range 分析（`QUICK_SELECT` 系列） |
| `sql/sql_executor.h/.cc` | `QEP_TAB` 和执行计划构造 |
| `sql/opt_trace.h/.cc` | optimizer trace 输出 |

### JOIN::optimize() 主干流程

```cpp
JOIN::optimize() {
  // (1) 常量表识别 + 条件化简
  propagate_dependencies();
  pull_out_semijoin_tables();
  simplify_joins();
  update_const_equal_items();

  // (2) range 分析: 为每张表评估可能的 range scan
  for each table: test_quick_select()  → QUICK_SELECT_I

  // (3) join order 搜索 (贪心 + 穷举混合)
  make_join_plan()
    → Optimize_table_order::choose_table_order()
      → greedy_search()              // 贪心搜索
         → best_extension_by_limited_search()  // 递归穷举(受 depth 限制)
            → best_access_path()     // 为单表找最佳访问方法 ★

  // (4) 基于 join order 构造 QEP
  get_best_combination()   → 创建 QEP_TAB 数组
  make_join_readinfo()     → 决定每个 QEP_TAB 的读策略
  create_access_paths()    → QEP_TAB → AccessPath 转换

  // (5) 后处理: ORDER BY 优化、LIMIT 下推、临时表决策
  test_skip_sort() / create_tmp_table_for_ordering() / ...
}
```

### best_access_path(): 单表访问方法选择

这是 legacy optimizer 最核心的函数之一。对于每张表，它评估：

| 访问方法 | join_type | 含义 |
|----------|-----------|------|
| `JT_SYSTEM` | const table，0或1行 | 系统表 |
| `JT_CONST` | 主键/唯一键常量查找 | 直接定值 |
| `JT_EQ_REF` | 主键/唯一键等值关联 | 一对一查找 ★ |
| `JT_REF` | 非唯一索引等值查找 | 索引查找 |
| `JT_RANGE` | 范围扫描 | range scan |
| `JT_INDEX_SCAN` | 全索引扫描 | covering index |
| `JT_ALL` | 全表扫描 | table scan |

`JT_EQ_REF` 就是 plan cache v1 所针对的唯一目标访问方式。

### Key_use: 索引使用描述

```cpp
struct Key_use {
  Table_ref *table_ref;     // 引用的表
  Item *val;                // 等值比较的值表达式
  table_map used_tables;    // val 依赖的表 bitmap
  uint key;                 // 索引编号
  uint keypart;             // 索引列序号
  // ...
};
```

优化器通过 `update_ref_and_keys()` 扫描 WHERE/ON 条件，提取所有可能的 `Key_use`，然后 `best_access_path()` 用这些信息评估每种访问方法的代价。

---

## 6. 优化层：Hypergraph Optimizer（新一代）

### 关键文件

| 文件 | 职责 |
|------|------|
| `sql/join_optimizer/join_optimizer.h/.cc` | 入口 `FindBestQueryPlan()` |
| `sql/join_optimizer/hypergraph.h/.cc` | 超图数据结构 |
| `sql/join_optimizer/make_join_hypergraph.h/.cc` | 从 Query_block 构建超图 |
| `sql/join_optimizer/access_path.h/.cc` | `AccessPath` 定义 + iterator 构造 |
| `sql/join_optimizer/cost_model.h/.cc` | 代价估算函数 |
| `sql/join_optimizer/subgraph_enumeration.h` | DPhyp 子图枚举算法 |
| `sql/join_optimizer/finalize_plan.h/.cc` | 最终化处理 |
| `sql/join_optimizer/compare_access_paths.h` | 计划比较与裁剪 |

### 与 Legacy 的核心区别

| 方面 | Legacy | Hypergraph |
|------|--------|-----------|
| 搜索算法 | 贪心 + 有限深度穷举 | DPhyp（基于超图的动态规划） |
| 计划表示 | `JOIN_TAB[]` → `QEP_TAB[]` → `AccessPath` | 直接构建 `AccessPath` 树 |
| join 类型 | 主要 NLJ，有限 hash join | NLJ + hash join 自由选择 |
| join reorder | 只处理 inner join 和部分 outer join | 理论上支持所有 join 类型 |
| 子查询 | 多种策略(semi-join, materialization, exists) | 统一框架 |
| 启用方式 | 默认 | `SET optimizer_switch='hypergraph_optimizer=on'` |

### FindBestQueryPlan() 算法概要

1. 构建 `JoinHypergraph`: 表是节点，join 条件是超边
2. `EnumerateAllConnectedSubgraphs()`: DPhyp 算法枚举所有合法子图对
3. 对每对子图，尝试所有 join 方法（NLJ、hash join），选最优
4. 自底向上合成完整的 `AccessPath` 树
5. 加上 GROUP BY / ORDER BY / LIMIT 等后处理步骤

### AccessPath 类型速查

AccessPath 的 `type` 枚举覆盖了所有可能的物理操作：

**叶节点（表访问）:**
- `TABLE_SCAN`, `INDEX_SCAN`, `REF`, `REF_OR_NULL`, `EQ_REF`
- `PUSHED_JOIN_REF`, `FULL_TEXT_SEARCH`, `CONST_TABLE`
- `MRR`, `FOLLOW_TAIL`, `INDEX_RANGE_SCAN`, `INDEX_MERGE`
- `INDEX_SKIP_SCAN`, `GROUP_INDEX_SKIP_SCAN`
- `DYNAMIC_INDEX_RANGE_SCAN`, `TABLE_VALUE_CONSTRUCTOR`
- `FAKE_SINGLE_ROW`, `ZERO_ROWS`, `ZERO_ROWS_AGGREGATED`
- `MATERIALIZED_TABLE_FUNCTION`, `UNQUALIFIED_COUNT`

**中间节点（join 和转换）:**
- `NESTED_LOOP_JOIN`, `NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL`
- `BKA_JOIN`, `HASH_JOIN`
- `FILTER`, `SORT`, `AGGREGATE`
- `TEMPTABLE_AGGREGATE`, `LIMIT_OFFSET`
- `STREAM`, `MATERIALIZE`, `MATERIALIZE_INFORMATION_SCHEMA_TABLE`
- `APPEND`, `WINDOW`, `WEEDOUT`, `REMOVE_DUPLICATES`
- `REMOVE_DUPLICATES_ON_INDEX`, `ALTERNATIVE`
- `CACHE_INVALIDATOR`, `DELETE_ROWS`, `UPDATE_ROWS`

---

## 7. 代价模型

### 关键文件

| 文件 | 职责 |
|------|------|
| `sql/opt_costmodel.h/.cc` | `Cost_model_server`、`Cost_model_table` |
| `sql/opt_costconstants.h/.cc` | 代价常量定义 |
| `sql/join_optimizer/cost_model.h/.cc` | hypergraph 代价函数 |
| `sql/opt_statistics.h/.cc` | 列直方图统计 |

### 代价模型的核心假设

MySQL 的代价模型是 **I/O + CPU 两维度**的：

```
total_cost = io_cost + cpu_cost

io_cost ≈ pages_to_read × page_read_cost
cpu_cost ≈ rows_to_evaluate × row_evaluate_cost
```

从存储引擎视角理解：
- `page_read_cost` 区分内存命中和磁盘读取——这对应你熟悉的 buffer pool hit ratio
- `rec_per_key[]` / `rec_per_key_float[]` / `records()` / `data_file_length` 等信息都由存储引擎通过 `handler` 接口提供

### 统计信息的来源

```
handler::info(HA_STATUS_VARIABLE)  → TABLE_SHARE::stats (行数、平均行长等)
handler::records_in_range()         → 范围行数估算
handler::index_flags()             → 索引能力声明
KEY::rec_per_key_float[]           → 索引基数倒数
Column_statistics (直方图)          → mysql.column_statistics 持久化
```

这些是优化器做决策的 **唯一数据来源**。如果存储引擎返回的统计信息不准，优化器的决策就会出错。这是存储引擎工程师能直接影响优化器行为的核心接口。

---

## 8. 执行层：Iterator 模型

### 关键文件

| 文件 | 职责 |
|------|------|
| `sql/iterators/row_iterator.h` | `RowIterator` 基类 |
| `sql/iterators/basic_row_iterators.h/.cc` | 基础扫描 iterator |
| `sql/iterators/ref_row_iterators.h/.cc` | 索引查找 iterator |
| `sql/iterators/hash_join_iterator.h/.cc` | hash join iterator |
| `sql/iterators/sorting_iterator.h/.cc` | 排序 iterator |
| `sql/iterators/composite_iterators.h/.cc` | 组合 iterator |
| `sql/join_optimizer/access_path.cc` | `CreateIteratorFromAccessPath()` |

### Iterator 调用模型

```
Init()  → 初始化/重置状态
Read()  → 返回下一行（0=成功，-1=EOF，1=错误）
         对于 join，内层 iterator 的 Read() 被外层驱动调用
```

### 从 AccessPath 到 Iterator

`CreateIteratorFromAccessPath()` 是一个大的 switch-case，按 `AccessPath::type` 创建对应的 `RowIterator`。例如：

| AccessPath type | Iterator |
|----------------|----------|
| `TABLE_SCAN` | `TableScanIterator` |
| `EQ_REF` | `EQRefIterator` |
| `REF` | `RefIterator` |
| `INDEX_RANGE_SCAN` | `IndexRangeScanIterator` |
| `NESTED_LOOP_JOIN` | `NestedLoopIterator` |
| `HASH_JOIN` | `HashJoinIterator` |
| `SORT` | `SortingIterator` |
| `FILTER` | `FilterIterator` |

每个 iterator 最终通过 `handler` API 访问存储引擎：
- `handler::ha_index_read_map()` ← `EQRefIterator`
- `handler::ha_index_next_same()` ← `RefIterator`
- `handler::ha_rnd_next()` ← `TableScanIterator`
- `handler::ha_index_first/next()` ← `IndexScanIterator`

---

## 9. Plan 的四层表示

MySQL 中 "plan" 这个概念在不同阶段有不同的物理表示：

```
┌─────────────────────────────────────────────────────────┐
│ Layer 1: 逻辑层 (Logical)                                │
│ 表示: Query_block + Item 条件 + Table_ref 链             │
│ 语义: "要做什么" — 表、条件、投影、分组、排序             │
│ 生存期: parse → 整个语句生命周期                         │
│ 文件: sql/sql_lex.h, sql/item.h                         │
└─────────────────────────────────────────────────────────┘
         │ optimize
         ▼
┌─────────────────────────────────────────────────────────┐
│ Layer 2: 物理计划层 — Legacy (Physical Plan)              │
│ 表示: JOIN_TAB[] → QEP_TAB[] + POSITION + Key_use       │
│ 语义: "怎么做" — join 顺序、每表访问方法、索引选择       │
│ 生存期: JOIN::optimize() 内部                            │
│ 文件: sql/sql_optimizer.h, sql/sql_select.h,            │
│       sql/sql_executor.h, sql/sql_planner.h             │
│                                                         │
│ 或                                                       │
│                                                         │
│ Layer 2': 物理计划层 — Hypergraph                         │
│ 表示: AccessPath 树 (直接构建)                            │
│ 语义: 同上，但用 DPhyp 搜索空间更大                      │
│ 生存期: FindBestQueryPlan() → FinalizePlanForQueryBlock() │
│ 文件: sql/join_optimizer/                                │
└─────────────────────────────────────────────────────────┘
         │ create_access_paths() 或直接输出
         ▼
┌─────────────────────────────────────────────────────────┐
│ Layer 3: 统一计划层 (Unified Plan)                        │
│ 表示: AccessPath 树 (JOIN::m_root_access_path)           │
│ 语义: 与 iterator 一一对应的物理操作树                   │
│ 生存期: optimize 完成 → execute 完成                     │
│ 文件: sql/join_optimizer/access_path.h                   │
└─────────────────────────────────────────────────────────┘
         │ CreateIteratorFromAccessPath()
         ▼
┌─────────────────────────────────────────────────────────┐
│ Layer 4: 运行时层 (Runtime)                               │
│ 表示: RowIterator 树                                     │
│ 语义: 实际运行的代码对象，持有 handler 引用               │
│ 生存期: execute 阶段                                     │
│ 文件: sql/iterators/                                     │
└─────────────────────────────────────────────────────────┘
```

### 为什么 Plan Cache v1 只缓存 Template 而非完整 Plan

从上图可以看出：

- Layer 2–4 都绑定了 **per-execution** 的运行时对象（`TABLE *`、`handler`、`QEP_TAB`、`RowIterator`）
- `Query_block::restore_cmd_properties()` 在每次 PS 执行前会清空 `join`
- 即使 Layer 3 的 `AccessPath` 是纯 "描述"，它也引用了 `TABLE *`、`KEY *` 等指针

因此 v1 的策略是只缓存一个 **极小的 metadata template**（keyno、key_length、field_index 等），然后在每次执行时基于当前 fresh `JOIN` 快速重建 Layer 2–4。

---

## 10. Prepared Statement 与优化器的交互

### PS 生命周期

```
COM_STMT_PREPARE
  → Prepared_statement::prepare()
    → prepare_query(thd)
      → lex->sql_command dispatch
        → Sql_cmd_dml::prepare()
          → Query_block::prepare()    // resolve 阶段
    → 保存 LEX 到 stmt->m_lex (在 stmt 自己的 MEM_ROOT 上)
    → 不做 optimize，不做 execute

COM_STMT_EXECUTE (每次执行)
  → Prepared_statement::execute_loop()
    → 绑定参数 (Item_param::set_value)
    → Prepared_statement::execute()
      → reinit_stmt_before_use()       // 重置 LEX/Query_block 执行状态
      → mysql_execute_command()        // 走正常的 execute 流程
        → optimize → execute
      → 如果遇到 ER_NEED_REPREPARE:
        → reprepare() → swap_prepared_statement()
        → 最多重试 3 次
```

### 关键点：每次 execute 都走 optimize

这就是 plan cache 的动机——对于简单的点查，每次都走完整的 `JOIN::optimize()` 是浪费。v1 的 fast path 在 `JOIN::optimize()` 早期截断，跳过 `make_join_plan()` 等昂贵操作。

### reprepare 机制

当表结构变化（DDL）时，`Reprepare_observer` 会在 open_tables 阶段检测到版本不匹配，触发 `ER_NEED_REPREPARE`。上层 `execute_loop()` 会：

1. 重新 prepare（重新解析、resolve）
2. `swap_prepared_statement()` 交换新旧 statement 的内部状态
3. 用新 LEX 重新执行

plan cache v1 的字段也参与这个 swap，确保新 statement 能正确继承或重置缓存状态。

---

## 11. Plan Cache：从点查快路径到通用缓存

### 11.1 当前 v1 设计总结

v1 是一个 **极窄范围的 per-PS single-slot plan template cache**：

| 维度 | v1 选择 |
|------|---------|
| 范围 | 仅 PS binary protocol |
| 语句类型 | 仅单表唯一键等值 SELECT |
| 缓存位置 | Prepared_statement 内部单槽位 |
| 缓存内容 | 元数据 template（无运行时指针） |
| 淘汰策略 | 无需淘汰 |
| 共享 | 不跨连接 |
| 优化器路径 | 仅 legacy optimizer |

### 11.2 v1 之后可能的演进方向（供知识储备）

| 方向 | 难度 | 说明 |
|------|------|------|
| 扩展到复合唯一键 | 低 | 支持多列 PK/UK 等值 |
| 扩展到非唯一索引 ref | 中 | JT_REF，需考虑 rows 估算变化 |
| 支持 hypergraph optimizer | 中 | AccessPath 树的 template 化更自然 |
| 支持 range scan 缓存 | 高 | range 的 key range 依赖参数值 |
| 通用 plan cache (SQL 文本) | 极高 | 需要 SQL 参数化、全局哈希、并发控制 |
| 跨连接共享 | 极高 | 需要 plan 引用计数、thread-safe |
| SPM (SQL Plan Management) | 极高 | plan baseline、plan evolution、hint |

### 11.3 业界对比

| 系统 | Plan Cache 策略 |
|------|----------------|
| MySQL 8.x (当前) | 无 plan cache，PS 每次 re-optimize |
| PostgreSQL | generic plan vs custom plan 自动切换 |
| Oracle | shared cursor + bind peeking + adaptive cursor |
| SQL Server | plan cache + forced parameterization |
| TiDB | prepared plan cache + 非 prepare 的 plan cache |

---

## 12. 与存储引擎的接口边界

作为存储引擎工程师，你最需要关注的 optimizer ↔ engine 接口：

### 12.1 统计信息接口

```cpp
handler::info(uint flag)
  HA_STATUS_VARIABLE: stats.records, stats.data_file_length, ...
  HA_STATUS_CONST:    max_data_file_length, keys, key_parts, ...

handler::records_in_range(uint keynr, key_range *min_key, key_range *max_key)
  → 估算给定 key 范围内的行数

handler::index_flags(uint keynr, uint part, bool all_parts)
  → 声明索引能力: HA_READ_NEXT, HA_READ_ORDER, HA_KEYREAD_ONLY, ...

KEY::rec_per_key_float[part]
  → 索引基数的倒数，用于 join selectivity 估算
```

### 12.2 执行时访问接口

```cpp
handler::ha_index_read_map()     // EQ_REF / REF 的核心
handler::ha_index_next_same()    // REF 连续读
handler::ha_index_next()         // INDEX_SCAN
handler::ha_index_read_last()    // 反向扫描
handler::ha_rnd_next()           // TABLE_SCAN
handler::ha_rnd_pos()            // MRR 回表
handler::ha_index_init/end()     // 索引打开/关闭
handler::ha_rnd_init/end()       // 随机扫描打开/关闭
```

### 12.3 影响优化器决策的关键 handler 方法

| 方法 | 影响的优化决策 |
|------|---------------|
| `records_in_range()` | range 分析精度，index merge 选择 |
| `info(HA_STATUS_VARIABLE)` | 全表扫描代价、行数估算 |
| `rec_per_key_float[]` | ref 和 eq_ref 的 fanout 估算 |
| `index_flags()` | 是否考虑 covering index scan |
| `primary_key_is_clustered()` | 聚簇索引 vs 堆表的 I/O 模型 |
| `table_cache_type()` | 是否参与 query cache |
| `extra(HA_EXTRA_USE_READ_SET)` | 列裁剪下推 |

---

## 13. 关键源文件速查表

### 核心优化流程

| 文件 | 一句话描述 |
|------|-----------|
| `sql/sql_optimizer.h/.cc` | `JOIN` 类 + `JOIN::optimize()` 主流程 |
| `sql/sql_planner.h/.cc` | 贪心 join order 搜索 |
| `sql/sql_select.h` | `Key_use`、`POSITION`、`Sql_cmd_select` |
| `sql/sql_resolver.h/.cc` | 名称解析和语义绑定 |
| `sql/sql_executor.h/.cc` | `QEP_TAB` + 执行计划构造 |
| `sql/sql_lex.h` | `LEX`、`Query_block`、`Query_expression` |
| `sql/sql_prepare.h/.cc` | `Prepared_statement` 全生命周期 |

### 新优化器 (Hypergraph)

| 文件 | 一句话描述 |
|------|-----------|
| `sql/join_optimizer/join_optimizer.h/.cc` | `FindBestQueryPlan()` 入口 |
| `sql/join_optimizer/access_path.h/.cc` | `AccessPath` 定义 + iterator 转换 |
| `sql/join_optimizer/hypergraph.h/.cc` | 超图数据结构 |
| `sql/join_optimizer/make_join_hypergraph.h/.cc` | Query_block → 超图 |
| `sql/join_optimizer/cost_model.h/.cc` | 代价函数 |
| `sql/join_optimizer/subgraph_enumeration.h` | DPhyp 算法 |
| `sql/join_optimizer/finalize_plan.h/.cc` | 计划最终化 |

### 代价模型与统计

| 文件 | 一句话描述 |
|------|-----------|
| `sql/opt_costmodel.h/.cc` | `Cost_model_server`、`Cost_model_table` |
| `sql/opt_costconstants.h/.cc` | 代价常量 |
| `sql/opt_statistics.h/.cc` | 列直方图 |

### Range 优化器

| 文件 | 一句话描述 |
|------|-----------|
| `sql/range_optimizer/range_optimizer.h/.cc` | range 分析入口 |
| `sql/range_optimizer/index_range_scan_plan.h/.cc` | 单索引 range |
| `sql/range_optimizer/index_merge_plan.h/.cc` | 多索引合并 |

### 执行 Iterator

| 文件 | 一句话描述 |
|------|-----------|
| `sql/iterators/row_iterator.h` | `RowIterator` 基类 |
| `sql/iterators/basic_row_iterators.h/.cc` | 基础扫描 |
| `sql/iterators/ref_row_iterators.h/.cc` | 索引查找 |
| `sql/iterators/hash_join_iterator.h/.cc` | Hash join |
| `sql/iterators/composite_iterators.h/.cc` | 组合操作 |

### EXPLAIN 与 Trace

| 文件 | 一句话描述 |
|------|-----------|
| `sql/opt_explain.h/.cc` | EXPLAIN 框架 |
| `sql/opt_explain_json.h/.cc` | JSON 格式 |
| `sql/opt_trace.h/.cc` | optimizer trace |
| `sql/join_optimizer/explain_access_path.h/.cc` | EXPLAIN FORMAT=tree |

### Plan Cache (v1)

| 文件 | 一句话描述 |
|------|-----------|
| `sql/ps_point_plan_cache.h/.cc` | 点查 plan cache helper |
| `sql/sql_prepare.h` | Prepared_statement 中的 cache 字段 |
| `design/ps_point_plan_cache_v1_design.md` | 设计文档 |
| `design/ps_point_plan_cache_v1_implementation_plan.md` | 实现计划 |

---

## 14. 学习路径与阅读顺序

### 第一阶段：建立全局认知（1-2 天）

**目标**: 理解 SQL 从文本到结果的完整链路，知道每个阶段做什么。

1. 通读本文档的 §1–§2，建立整体架构图
2. 用 GDB 或 optimizer trace 跟踪一条简单 SELECT：
   ```sql
   SET optimizer_trace = 'enabled=on';
   SELECT * FROM t1 WHERE id = 1;
   SELECT * FROM information_schema.optimizer_trace\G
   ```
3. 阅读 `sql/sql_cmd_dml.cc` 中 `Sql_cmd_dml::execute()` 和 `execute_inner()`，理解顶层分发
4. 浏览 `sql/sql_lex.h` 中 `LEX`、`Query_block`、`Query_expression` 的类定义（不必读完，先看成员列表和注释）

### 第二阶段：深入 Plan 结构（2-3 天）

**目标**: 理解 "plan" 在 MySQL 中的四层表示。

1. 阅读 `sql/sql_optimizer.h` 中 `JOIN` 类的成员（重点关注 `optimize()`、`m_root_access_path`、`qep_tab`、`best_positions`）
2. 阅读 `sql/join_optimizer/access_path.h` 中 `AccessPath` 的定义（重点看 type 枚举和代价字段）
3. 阅读 `sql/sql_select.h` 中 `Key_use`、`POSITION` 的定义
4. 阅读 `sql/sql_executor.h` 中 `QEP_TAB` 的定义（重点看 `type()`、`ref()`、`access_path()`）
5. 通读 §9（Plan 的四层表示），对照代码建立映射

### 第三阶段：Legacy Optimizer 细节（3-5 天）

**目标**: 能看懂 `JOIN::optimize()` 的主干流程。

1. 从 `sql/sql_optimizer.cc` 中 `JOIN::optimize()` 开始，按函数调用顺序阅读
2. 重点理解：
   - `make_join_plan()` → `choose_table_order()` → `greedy_search()`
   - `best_access_path()` — 这是最核心的单表优化决策点
   - `get_best_combination()` — join order → QEP_TAB 的转换
   - `create_access_paths()` — QEP_TAB → AccessPath 的转换
3. 阅读 `sql/sql_planner.cc` 中 `greedy_search()` 和 `best_extension_by_limited_search()`
4. 用 `EXPLAIN FORMAT=JSON` 和 `optimizer_trace` 验证你的理解

### 第四阶段：Prepared Statement + Plan Cache（2-3 天）

**目标**: 完全理解 PS 生命周期和 plan cache v1 设计。

1. 阅读 `sql/sql_prepare.cc` 中 `Prepared_statement::prepare()` 和 `execute_loop()`
2. 通读 `design/ps_point_plan_cache_v1_design.md`（已有设计文档，非常详细）
3. 通读 `design/ps_point_plan_cache_v1_implementation_plan.md`
4. 阅读 `sql/ps_point_plan_cache.h` 和 `.cc`
5. 理解 plan cache 的状态机 `NEVER → COLD → HOT → INVALID`

### 第五阶段：Hypergraph Optimizer（3-5 天）

**目标**: 理解新一代优化器的设计理念。

1. 阅读 `sql/join_optimizer/join_optimizer.h` 顶部长注释
2. 阅读 `sql/join_optimizer/subgraph_enumeration.h` 了解 DPhyp 算法
3. 阅读 `sql/join_optimizer/make_join_hypergraph.cc` 理解如何从 Query_block 构建超图
4. 阅读 `sql/join_optimizer/join_optimizer.cc` 中 `FindBestQueryPlan()` 的主流程

### 第六阶段：代价模型与存储引擎接口（2-3 天）

**目标**: 理解优化器如何利用存储引擎提供的信息做决策。

1. 阅读 `sql/opt_costmodel.h/.cc`，理解代价计算公式
2. 阅读 `sql/handler.h` 中统计信息相关的虚函数
3. 阅读 `storage/innobase/handler/ha_innodb.cc` 中 `records_in_range()` 的实现
4. 阅读 §12（与存储引擎的接口边界）

### 第七阶段：Iterator 执行模型（1-2 天）

**目标**: 理解 plan 如何变成实际运行的代码。

1. 阅读 `sql/join_optimizer/access_path.cc` 中 `CreateIteratorFromAccessPath()`
2. 阅读 `sql/iterators/ref_row_iterators.cc` 中 `EQRefIterator` 的实现（对应 plan cache 的目标场景）
3. 理解 Iterator 与 handler API 的映射关系

### 推荐的实践方法

| 方法 | 目的 |
|------|------|
| `EXPLAIN FORMAT=TREE` | 直观看到 AccessPath 树 |
| `EXPLAIN FORMAT=JSON` | 看到详细的代价估算 |
| `SET optimizer_trace='enabled=on'` | 看到优化器的完整决策过程 |
| GDB 设断点 `JOIN::optimize` | 跟踪优化主流程 |
| GDB 设断点 `best_access_path` | 跟踪单表访问方法选择 |
| GDB 设断点 `CreateIteratorFromAccessPath` | 跟踪 plan → iterator 转换 |
| sysbench `oltp_point_select` + perf | profile 热路径 |
| `SHOW STATUS LIKE 'Ps_point_plan_cache%'` | 观察 plan cache 状态 |

### 推荐扩展阅读

| 资源 | 说明 |
|------|------|
| MySQL 官方 Internals Manual | 整体架构概述 |
| "Join Ordering Problem" (Moerkotte) | DPhyp 算法原论文 |
| "How We Built a Cost-Based SQL Optimizer" (MySQL blog) | hypergraph 设计博客 |
| `sql/join_optimizer/README` (如有) | 代码内的开发者笔记 |
| PostgreSQL EXPLAIN 文档 | 对比理解不同系统的 plan 表示 |
| "Access Path Selection in a Relational Database Management System" (Selinger 1979) | 奠基论文，system R 优化器 |
| "Query Processing in Main-Memory Database Management Systems" | 现代内存数据库优化 |
| TiDB / CockroachDB 的 plan cache 设计文档 | 分布式系统的 plan cache 参考 |

---

## 附录 A: 常用调试命令速查

```sql
-- 查看查询执行计划 (树形, 推荐)
EXPLAIN FORMAT=TREE SELECT ...;

-- 查看 JSON 格式计划 (含代价)
EXPLAIN FORMAT=JSON SELECT ...;

-- 查看优化器决策trace
SET optimizer_trace = 'enabled=on';
SELECT ...;
SELECT * FROM information_schema.optimizer_trace\G

-- 查看 PS plan cache 状态
SHOW STATUS LIKE 'Ps_point_plan_cache%';

-- 查看当前优化器开关
SELECT @@optimizer_switch\G

-- 查看表统计信息 (优化器的输入)
SELECT * FROM mysql.innodb_table_stats WHERE table_name = 'xxx';
SELECT * FROM mysql.innodb_index_stats WHERE table_name = 'xxx';

-- 查看列直方图
ANALYZE TABLE t1 UPDATE HISTOGRAM ON col1;
SELECT * FROM information_schema.column_statistics;
```

## 附录 B: 术语表

| 术语 | 含义 |
|------|------|
| `LEX` | 语句级解析结果容器 |
| `Query_block` | 单个 SELECT 子句（SQL 标准: query specification） |
| `Query_expression` | 完整查询（含 UNION/INTERSECT/EXCEPT） |
| `Table_ref` | 解析树中的表引用（旧名 TABLE_LIST） |
| `Item` | 表达式节点基类 |
| `Item_param` | PS 参数占位符 |
| `JOIN` | per-query-block 的优化器上下文 |
| `JOIN_TAB` | Legacy 优化期间的表槽位 |
| `QEP_TAB` | Legacy 执行计划中的表槽位 |
| `POSITION` | join order 中一步的代价描述 |
| `Key_use` | 可用的索引等值条件 |
| `AccessPath` | 统一物理计划节点（tagged union） |
| `RowIterator` | 运行时执行算子 |
| `handler` | 存储引擎抽象接口 |
| `JT_EQ_REF` | 唯一键等值查找 join type |
| `JT_REF` | 非唯一索引等值查找 join type |
| `JT_ALL` | 全表扫描 join type |
| DPhyp | Dynamic Programming on Hypergraphs（超图动态规划） |
| NLJ | Nested Loop Join |
| BKA | Batched Key Access |
| MRR | Multi-Range Read |
| SPM | SQL Plan Management |
| PS | Prepared Statement |
