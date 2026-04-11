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
15. [端到端案例：从简单查询到复杂语句形态](#15-端到端案例从简单查询到复杂语句形态)
16. [附录 A：常用调试命令速查](#附录-a-常用调试命令速查)
17. [附录 B：术语速查表](#附录-b-术语速查表)
18. [附录 C：扩展术语详解](#附录-c-扩展术语详解)

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

### 2.1 为什么优化器里会有这么多对象

第一次看 MySQL 查询优化器代码，很多人都会有同一个困惑：

> 为什么一条 SQL 要同时出现 `LEX`、`Query_expression`、`Query_block`、`Table_ref`、`JOIN`、`QEP_TAB`、`AccessPath`、`RowIterator` 这么多对象？

根本原因不是“代码风格啰嗦”，而是：

> 一条 SQL 在不同阶段，要解决的问题不同、依赖的数据不同、对象生命周期也不同。

可以把它理解成四层职责分离：

1. **逻辑层**
   - 关心“SQL 写了什么”
   - 代表对象：`LEX`、`Query_expression`、`Query_block`、`Item`
   - 生命周期长，通常从 parse 一直活到语句结束，PS 场景下甚至能跨多次执行保留

2. **语义绑定层**
   - 关心“名字到底指向哪个表、哪一列、什么类型”
   - 代表对象：`Table_ref`、`Field`、`Item_field::fix_fields()` 绑定结果
   - 在这层之前，列名还只是“文本”；在这层之后，列名才变成真正的列对象

3. **物理计划层**
   - 关心“怎么做最划算”
   - 代表对象：legacy 路径的 `JOIN_TAB`、`POSITION`、`QEP_TAB`，以及统一层的 `AccessPath`
   - 这是优化器真正做搜索、比较和裁剪的地方

4. **运行时层**
   - 关心“现在这一行怎么读、下一行怎么读”
   - 代表对象：`RowIterator` 体系
   - 它已经不再讨论“哪个计划更好”，而是把已选计划翻译成可执行代码对象

如果把这些对象混成一个大对象，会马上遇到三个问题：

- parse 阶段拿不到执行时才有的 `TABLE *`
- optimize 阶段需要大量临时对象，但它们不应该长期保留
- execute 阶段需要可重入、可重置的运行时状态，而不是一堆解析树细节

所以这些对象分层，本质上是为了把三个维度拆开：

- **职责拆分**
- **生命周期拆分**
- **内存归属拆分**

### 2.2 四组最容易混淆的对象

#### `Query_expression` vs `Query_block`

- `Query_expression` 对应 SQL 标准里的完整查询表达式，可能含 `UNION`
- `Query_block` 对应一个具体的 `SELECT ... FROM ... WHERE ...`

可以简单记：

- 一个简单 SELECT：一个 `Query_expression`，里面只有一个 `Query_block`
- 一个 `UNION`：一个 `Query_expression`，里面有多个 `Query_block`

#### `Table_ref` vs `TABLE`

- `Table_ref` 是解析树里的“表名引用”
- `TABLE` 是 open table 之后真正可访问数据的运行时表对象

存储引擎工程师最熟悉的是 `TABLE` 和它下面的 `handler`。  
但优化器在更早阶段经常只看得见 `Table_ref`。

#### `QEP_TAB` vs `AccessPath` vs `RowIterator`

- `QEP_TAB`：legacy 优化器的“每表执行槽位”
- `AccessPath`：统一物理计划树节点
- `RowIterator`：真正执行的运行时代码对象

可以记成：

- `QEP_TAB` 更像 legacy 时代的“计划中间件”
- `AccessPath` 更像统一的“物理计划 IR”
- `RowIterator` 才是真正执行的对象

#### `MEM_ROOT` vs `Query_arena`

- `MEM_ROOT` 是 arena allocator，本质是内存池
- `Query_arena` 是“这个阶段/这个 statement 应该往哪个 mem_root 上分配对象”的策略与状态容器

可以记成：

- `MEM_ROOT` 解决“内存从哪儿来”
- `Query_arena` 解决“当前这类对象该分到哪个生命周期里去”

这一组概念在 Prepared Statement、reprepare 和 plan cache 里尤其关键。

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

## 15. 端到端案例：从简单查询到复杂语句形态

这一章的目标不是“背 SQL”，而是把前面抽象的优化器对象和流程，放回到真实 SQL 上去看。

阅读这一章时，建议始终带着 4 个问题：

1. 这条 SQL 对优化器提出的核心问题是什么？
2. 优化器会优先在哪一层做判断：resolve、单表访问、join reorder，还是后处理？
3. 最终 plan 更像哪一类 `AccessPath` / `RowIterator`？
4. 如果统计信息、索引、数据分布变化，这个 plan 最容易在哪一步发生变化？

### 15.1 使用本章的方式

这章里的案例遵循下面几个原则：

- 统一使用一套示例 schema
- 每个案例都给出具体 SQL
- 每个案例都说明“为什么会产生这种优化问题”
- 每个案例都指出典型的内部代码路径
- 每个案例都强调“真实计划依赖统计信息和数据分布”

需要特别注意：

- 这里描述的是**典型**计划形态，不是对所有数据分布都 100% 保证的 EXPLAIN 输出
- 同一条 SQL 在不同数据量、不同直方图、不同 `optimizer_switch` 下可能走不同路径
- 如果你要验证某个案例，应该用 `EXPLAIN FORMAT=TREE`、`EXPLAIN FORMAT=JSON`、`optimizer_trace` 三者交叉看

### 15.2 统一示例 Schema

下面这套 schema 用于贯穿后续所有案例。  
它故意包含了：

- 主键
- 唯一键
- 非唯一二级索引
- 复合索引
- 典型一对多关系
- 排序/聚合常见列

```sql
CREATE TABLE regions (
  id BIGINT PRIMARY KEY,
  country_code CHAR(2) NOT NULL,
  province VARCHAR(64) NOT NULL,
  city VARCHAR(64) NOT NULL,
  KEY idx_country_province_city (country_code, province, city)
);

CREATE TABLE customers (
  id BIGINT PRIMARY KEY,
  email VARCHAR(128) NOT NULL,
  region_id BIGINT NOT NULL,
  customer_level TINYINT NOT NULL,
  status TINYINT NOT NULL,
  created_at DATETIME NOT NULL,
  UNIQUE KEY uk_email (email),
  KEY idx_region_level (region_id, customer_level),
  KEY idx_status_created (status, created_at)
);

CREATE TABLE products (
  id BIGINT PRIMARY KEY,
  sku VARCHAR(64) NOT NULL,
  category_id BIGINT NOT NULL,
  brand_id BIGINT NOT NULL,
  status TINYINT NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  created_at DATETIME NOT NULL,
  UNIQUE KEY uk_sku (sku),
  KEY idx_category_price (category_id, price),
  KEY idx_status_price (status, price),
  KEY idx_brand_category (brand_id, category_id)
);

CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  status TINYINT NOT NULL,
  total_amount DECIMAL(12,2) NOT NULL,
  created_at DATETIME NOT NULL,
  paid_at DATETIME NULL,
  KEY idx_customer_created (customer_id, created_at),
  KEY idx_status_created (status, created_at),
  KEY idx_created (created_at)
);

CREATE TABLE order_items (
  id BIGINT PRIMARY KEY,
  order_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  quantity INT NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  KEY idx_order_product (order_id, product_id),
  KEY idx_product_order (product_id, order_id)
);
```

为了让案例更贴近真实线上环境，建议假设以下数据分布：

- `orders` 明显大于 `customers`
- `order_items` 明显大于 `orders`
- `status` 是低基数字段
- `customer_id`、`product_id` 是高基数 join key
- 某些时间范围是热点
- `sku`、`email` 是高选择性唯一键

### 15.3 案例总览

| 编号 | 主题 | 关键知识点 |
|------|------|------------|
| 0 | 最简单的端到端链路 | parse → resolve → optimize → iterator |
| 1 | 无索引过滤导致全表扫描 | `JT_ALL` / `TABLE_SCAN` |
| 2 | 主键等值查找 | `const` / 唯一键访问 |
| 3 | 唯一二级索引等值查找 | `uk_email`、唯一键语义 |
| 4 | 非唯一索引等值查找 | `JT_REF` |
| 5 | 范围扫描 | `JT_RANGE` / range optimizer |
| 6 | 复合索引左前缀 | key parts 与前缀中断 |
| 7 | 覆盖索引 | `Using index` / 减少回表 |
| 8 | ORDER BY 走索引 | 避免 filesort |
| 9 | ORDER BY 触发 filesort | 排序后处理 |
| 10 | GROUP BY + 聚合 | aggregate / temp table |
| 11 | DISTINCT | 去重与可能的排序/临时表 |
| 12 | 两表 inner join | `eq_ref` / `ref` / join order |
| 13 | 多表 join | fanout 与 join reorder |
| 14 | LEFT JOIN | 外连接限制与 NULL-complemented row |
| 15 | `IN (subquery)` | semijoin / materialization 候选 |
| 16 | `EXISTS` 相关子查询 | semijoin / correlated execution |
| 17 | derived table / 子查询 in FROM | merge vs materialize |
| 18 | UNION ALL 与 UNION | set operation / duplicate removal |
| 19 | 窗口函数 | Window / sort / iterator 组合 |
| 20 | UPDATE ... WHERE | DML 也要 optimize |
| 21 | DELETE ... JOIN | DML + join plan |
| 22 | Prepared Statement 点查 | PS 生命周期与 plan cache 关联 |
| 23 | GROUP BY + HAVING | WHERE 与 HAVING 的边界 |
| 24 | GROUP BY + ORDER BY 聚合结果 | aggregate 后再 sort |
| 25 | OR 谓词 | index merge 候选 |
| 26 | 前缀 LIKE | B-tree 前缀范围 |
| 27 | 函数包裹列 | 非 SARGable 谓词 |
| 28 | ORDER BY + LIMIT + OFFSET | 索引顺序与跳过成本 |
| 29 | MIN/MAX + GROUP BY | 松散索引扫描/顺序聚合候选 |
| 30 | SELECT ... FOR UPDATE | 访问路径与锁范围 |
| 31 | SELECT 列表中的标量子查询 | correlated scalar subquery |
| 32 | NOT EXISTS | anti-semi join |
| 33 | 可 merge 的 derived table | merge vs materialize 对照 |
| 34 | 非递归 CTE | CTE 与 derived table 的关系 |
| 35 | 递归 CTE | working table / 迭代执行 |
| 36 | 多表 UPDATE | DML + join + 修改目标 |
| 37 | STRAIGHT_JOIN | 人工固定 join order |
| 38 | INSERT ... SELECT | 读路径 + 写路径组合 |

### 15.4 案例 0：先看一条最简单查询的完整链路

```sql
SELECT total_amount
FROM orders
WHERE id = 1000001;
```

#### 这个案例为什么重要

它几乎是理解 MySQL 查询优化器的最短路径：

- 有单表
- 有谓词
- 有主键
- 没有 join、排序、聚合、子查询干扰

如果这个案例你能完整讲清楚，后面复杂案例基本都只是往这条主线叠加新问题。

#### 优化器要解决的问题

这条 SQL 看上去非常简单，但它仍然要经历完整链路：

1. 语法是否合法？
2. `orders` 这张表到底是谁？
3. `id` 这列到底是哪一列、什么类型？
4. 是否应该用主键访问？
5. 最终如何把这个计划翻译成真正执行代码？

#### 典型内部流程

1. **Parse**
   - 生成 `LEX`
   - 生成 `Query_expression`
   - 生成 `Query_block`
   - 生成 `Item_field(id)`、`Item_int(1000001)`、`Item_func_eq`

2. **Resolve**
   - `open_tables_for_query()` 打开 `orders`
   - `Item_field::fix_fields()` 把 `id` 绑定到真实 `Field`
   - `Query_block::prepare()` 完成语义绑定

3. **Optimize**
   - `JOIN::optimize()` 启动
   - `update_ref_and_keys()` 从 `id = const` 中提取可用 key 信息
   - `best_access_path()` 识别出主键等值查找最优
   - 生成 `QEP_TAB` / `AccessPath`

4. **Execute**
   - `CreateIteratorFromAccessPath()` 把计划转成 `EQRefIterator` 或 const table 相关执行路径
   - iterator 调 `handler::ha_index_read_map()`

#### 你在 EXPLAIN 里通常会看到什么

典型会是：

- `const`
- 或非常接近主键单点查找的树形 plan

#### 这个案例和 plan cache 的关系

这类查询是最适合缓存计划模板的典型形态，因为：

- 语句形状稳定
- 访问路径极稳定
- 每次执行主要变的是常量/参数值

### 15.5 单表访问案例

#### 案例 1：没有合适索引时的全表扫描

```sql
SELECT *
FROM orders
WHERE total_amount > 5000;
```

#### 背景

如果 `total_amount` 上没有索引，优化器再聪明也不能凭空生成索引访问路径。

#### 需要解决的问题

优化器要判断：

- 是不是仍然有别的索引值得走
- 还是直接全表扫描最便宜

#### 核心逻辑

这里最典型的落点是：

- `update_ref_and_keys()` 提不出有效 `Key_use`
- range optimizer 也构不出合适 range path
- `best_access_path()` 最后落到 `JT_ALL`

#### 典型计划形态

- legacy：`type=ALL`
- unified plan：`TABLE_SCAN`
- iterator：`TableScanIterator`

#### 代码观察点

- `Optimize_table_order::best_access_path()`
- `CreateIteratorFromAccessPath()` -> `TableScanIterator`

#### 为什么这个案例重要

它告诉你一个基本事实：

> 优化器不是“总能走索引”，而是在给定元数据约束下做代价最小化。

#### 案例 2：主键等值查找

```sql
SELECT *
FROM customers
WHERE id = 42;
```

#### 背景

主键等值查找是 OLTP 里最稳定的一类访问路径。

#### 需要解决的问题

这条语句几乎没有 join order 问题，核心就是：

- 能否直接识别成主键唯一查找

#### 核心逻辑

- `id` 是主键
- 条件是等值
- fanout 接近 1

于是优化器倾向于：

- `JT_CONST`
- 或等价的唯一键单点访问

#### 典型代码路径

- `update_ref_and_keys()`
- `best_access_path()`
- `QEP_TAB::ref()`

#### 需要特别注意的点

如果谓词右边不是常量，而是外表驱动值，那么类似访问在 join 里更可能表现为：

- `JT_EQ_REF`

#### 案例 3：唯一二级索引等值查找

```sql
SELECT id, region_id
FROM customers
WHERE email = 'alice@example.com';
```

#### 背景

很多业务键不是主键，但有唯一约束，例如邮箱、手机号、SKU。

#### 需要解决的问题

优化器要判断：

- 唯一二级索引是否和主键一样，能把 fanout 限定在 1

#### 核心逻辑

这里依赖的是：

- `uk_email` 的唯一性
- 等值谓词

所以它通常仍然是“唯一查找”语义，只是物理上可能先走二级索引，再回主键。

#### 代码观察点

- `key_info`
- `actual_key_flags()`
- `best_access_path()`

#### 这个案例告诉你的事

优化器眼中的“好路径”，不一定非得是主键；  
关键在于：

- 唯一性
- 等值性
- 成本

#### 案例 4：非唯一索引等值查找（`JT_REF`）

```sql
SELECT id, total_amount
FROM orders
WHERE customer_id = 42;
```

#### 背景

`customer_id` 不是唯一键，一位客户可能有很多订单。

#### 需要解决的问题

优化器要判断：

- 用 `idx_customer_created` 的前缀 `customer_id`
- 是否比全表扫描更划算

#### 核心逻辑

因为它不是唯一键，所以即便是等值查找，也通常是：

- `JT_REF`

这里成本非常依赖：

- `rec_per_key`
- 统计行数
- fanout 估算

#### 典型计划形态

- access type：`ref`
- iterator：`RefIterator`

#### 为什么这个案例重要

它是从“唯一等值”过渡到“非唯一等值”的第一步，也是理解 fanout 的最简单入口。

#### 案例 5：范围扫描（`JT_RANGE`）

```sql
SELECT id, customer_id, total_amount
FROM orders
WHERE created_at BETWEEN '2026-01-01' AND '2026-01-31';
```

#### 背景

范围条件是 OLTP 和报表混合 workload 里最常见的形态之一。

#### 需要解决的问题

优化器要先问：

- 是否存在合适索引支持范围访问
- 范围预估行数是多少
- 走 range 后是否还需要大量回表

#### 核心逻辑

这里 range optimizer 会真正介入：

- `test_quick_select()`
- 生成 `QUICK` / range 相关结构

然后 `best_access_path()` 再把 range path 与：

- ref
- all
- index scan

做比较。

#### 典型计划形态

- access type：`range`
- iterator：`IndexRangeScanIterator`

#### 为什么这个案例重要

一旦进入 range，事情就明显比等值复杂：

- 边界更复杂
- 统计误差更大
- 后续可能接 MRR/BKA

#### 案例 6：复合索引左前缀

```sql
SELECT id, customer_id, created_at
FROM orders
WHERE customer_id = 42
  AND created_at >= '2026-01-01';
```

#### 背景

`idx_customer_created (customer_id, created_at)` 是非常典型的复合索引。

#### 需要解决的问题

优化器要判断：

- 这个谓词是否满足左前缀规则
- 第一列等值 + 第二列范围是否足够好

#### 核心逻辑

这里最典型的索引利用方式是：

- 第 1 keypart：等值
- 第 2 keypart：范围

它能很好展示 MySQL 中“复合索引并不是全有或全无，而是按 keypart 逐步使用”的逻辑。

#### 对比一个会破坏前缀的例子

```sql
SELECT id
FROM orders
WHERE created_at >= '2026-01-01';
```

如果只有 `(customer_id, created_at)` 而没有单列 `created_at` 索引，那么它不能直接高效利用这个复合索引的第二列做普通前导范围。

#### 代码观察点

- `Key_use.keypart`
- range key part 选择
- `key_len` 在 EXPLAIN 中的变化

#### 案例 7：覆盖索引

```sql
SELECT customer_id, created_at
FROM orders
WHERE customer_id = 42
ORDER BY created_at;
```

#### 背景

如果查询所需列都在同一个索引里，就有机会避免回表。

#### 需要解决的问题

优化器会考虑：

- 是否能用 `(customer_id, created_at)` 同时满足过滤、投影和排序

#### 核心逻辑

这是“好索引”的理想状态：

- 先按前缀过滤
- 再按索引顺序输出
- 所需列全在索引上

典型 EXPLAIN 里可能看到：

- `Using index`

#### 代码观察点

- `index_flags()`
- covering index 判断
- `IndexScanIterator` / `RefIterator` 的选择

#### 案例 8：ORDER BY 被索引顺序满足

```sql
SELECT id, created_at
FROM orders
WHERE customer_id = 42
ORDER BY created_at
LIMIT 20;
```

#### 背景

排序是非常贵的后处理步骤。  
如果索引顺序正好能满足 `ORDER BY`，可以直接省掉 filesort。

#### 需要解决的问题

优化器要判断：

- 当前访问路径的输出顺序，能否天然满足 ORDER BY

#### 核心逻辑

如果走 `(customer_id, created_at)` 索引前缀：

- `customer_id = 42` 固定
- 后续记录天然按 `created_at` 有序

那么就可能：

- 不需要 filesort
- LIMIT 也能更早生效

#### 为什么这个案例重要

它很好地展示了：

> 优化器选访问路径，不只是看 WHERE，也要看 ORDER BY / LIMIT 是否能一起受益。

#### 案例 9：ORDER BY 触发 filesort

```sql
SELECT id, total_amount
FROM orders
WHERE customer_id = 42
ORDER BY total_amount;
```

#### 背景

过滤列和排序列不在同一个有序索引路径上时，排序通常无法白嫖。

#### 需要解决的问题

优化器要决定：

- 是走过滤更优的索引，再 filesort
- 还是走另一个索引但过滤变差

#### 核心逻辑

如果没有 `(customer_id, total_amount)` 之类索引，常见结果是：

- 先按 `customer_id` 过滤
- 再 filesort

#### 代码观察点

- ORDER BY 后处理
- `test_skip_sort()`
- `SortingIterator`
- `filesort.cc`

#### 案例 10：GROUP BY + 聚合

```sql
SELECT customer_id, COUNT(*) AS cnt, SUM(total_amount) AS total
FROM orders
WHERE created_at >= '2026-01-01'
GROUP BY customer_id;
```

#### 背景

聚合类查询和点查完全是两种世界。  
这里优化器关心的不再只是“如何找行”，还要关心“如何分组、如何聚合”。

#### 需要解决的问题

- 先过滤还是先聚合
- 分组是否可以借助已有顺序
- 是否需要临时表

#### 核心逻辑

这类查询经常会引入：

- aggregate 节点
- temp table
- sort

具体是否使用临时表，取决于：

- 输入是否已按 group key 有序
- 聚合函数类型
- 访问路径

#### 代码观察点

- `count_field_types()`
- aggregate 相关后处理
- `AccessPath::AGGREGATE`

#### 案例 11：DISTINCT

```sql
SELECT DISTINCT customer_id
FROM orders
WHERE created_at >= '2026-01-01';
```

#### 背景

`DISTINCT` 看起来简单，但优化器要处理“如何去重”。

#### 需要解决的问题

- 能不能直接利用索引顺序去重
- 是否需要 sort / temp table

#### 核心逻辑

如果输入顺序和 distinct key 对齐，代价会更低。  
否则就可能需要额外去重步骤。

#### 代码观察点

- duplicate removal
- `REMOVE_DUPLICATES`
- `WEEDOUT`

### 15.6 Join 案例

#### 案例 12：最典型的两表 inner join

```sql
SELECT o.id, c.email, o.total_amount
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.id = 1000001;
```

#### 背景

这是最经典的主从表 join：先定位一条订单，再通过外键去找唯一客户。

#### 需要解决的问题

- 先从哪张表开始
- 第二张表用什么访问方式

#### 核心逻辑

典型思路是：

1. `orders.id = const` 先唯一定位订单
2. `customers.id = o.customer_id` 再走唯一键访问

所以第二步经常是：

- `JT_EQ_REF`

#### 为什么这个案例重要

这是理解 `eq_ref` 最直观的案例：

> 外表一行驱动内表唯一键查找，最多返回一行。

#### 案例 13：多表 join 与 fanout

```sql
SELECT o.id, oi.product_id, p.price
FROM orders o
JOIN order_items oi ON oi.order_id = o.id
JOIN products p ON p.id = oi.product_id
WHERE o.customer_id = 42
  AND p.status = 1;
```

#### 背景

一旦有三张表，核心问题就不再是“某张表怎么访问”，而是：

> 哪张表先来，哪张表后来，总成本差多少？

#### 需要解决的问题

- join order
- 外层 fanout 放大效应
- 第三张表的访问成本是否被前两张表放大

#### 核心逻辑

这正是 `POSITION`、`best_access_path()`、`choose_table_order()` 发力的地方。

优化器会不断比较：

- 如果先从 `orders(customer_id=42)` 开始
- 再到 `order_items(order_id=o.id)`
- 再到 `products(id=oi.product_id and status=1)`

与其他顺序相比，哪个更划算。

#### 为什么这个案例重要

它是理解 fanout 的最直接案例：

- 外层结果集一旦放大
- 内层代价会被乘上去

#### 案例 14：LEFT JOIN

```sql
SELECT c.id, c.email, o.id AS recent_order_id
FROM customers c
LEFT JOIN orders o
  ON o.customer_id = c.id
 AND o.created_at >= '2026-01-01'
WHERE c.status = 1;
```

#### 背景

外连接比内连接更麻烦，因为它要求：

- 即使右表没有匹配，也要保留左表行

#### 需要解决的问题

- join reorder 的自由度下降
- 执行器需要构造 NULL-complemented row

#### 核心逻辑

outer join 的难点不是“能不能连上”，而是：

- 语义上不能随便重排
- Read() 到 EOF 时还要产生 NULL 行

这也是 `RowIterator::SetNullRowFlag()` 存在的背景之一。

#### 代码观察点

- `Query_block::outer_join`
- `simplify_joins()`
- iterator 的 NULL row 处理

### 15.7 子查询、派生表与集合操作案例

#### 案例 15：`IN (subquery)` 与 semijoin

```sql
SELECT c.id, c.email
FROM customers c
WHERE c.id IN (
  SELECT o.customer_id
  FROM orders o
  WHERE o.status = 1
);
```

#### 背景

很多人会把这类 SQL 理解成“外层循环、内层反复执行子查询”。  
但优化器通常会尝试把它改写成更好的形态。

#### 需要解决的问题

- 这类子查询能否转换成 semijoin
- 是否适合 materialization
- 是否需要 duplicate removal

#### 核心逻辑

`IN` 子查询是 semijoin 转换的典型场景。

优化器会比较：

- 转成 semijoin 是否更好
- 先 materialize 子查询结果是否更好

#### 代码观察点

- `sql/sql_resolver.cc`
- `pull_out_semijoin_tables()`
- semijoin 相关路径

#### 案例 16：相关 `EXISTS` 子查询

```sql
SELECT c.id, c.email
FROM customers c
WHERE EXISTS (
  SELECT 1
  FROM orders o
  WHERE o.customer_id = c.id
    AND o.status = 1
);
```

#### 背景

这是相关子查询，因为内层依赖外层的 `c.id`。

#### 需要解决的问题

- 是按相关子查询逐行执行
- 还是改写成 semijoin
- 还是做其它转换

#### 核心逻辑

这类查询非常适合用来观察：

- `used_tables`
- outer references
- 子查询转换

#### 为什么这个案例重要

它直接连接了：

- `Item_subselect`
- `fix_fields()`
- semijoin/materialization 优化

#### 案例 17：FROM 子句中的 derived table

```sql
SELECT dt.customer_id, dt.cnt
FROM (
  SELECT customer_id, COUNT(*) AS cnt
  FROM orders
  WHERE created_at >= '2026-01-01'
  GROUP BY customer_id
) AS dt
WHERE dt.cnt >= 5;
```

#### 背景

派生表是很多复杂 SQL 的中间结构。

#### 需要解决的问题

- derived table 能不能 merge 进外层
- 还是必须 materialize

#### 核心逻辑

如果 derived table 里有聚合，通常更容易 materialize。  
如果语义足够简单，也可能 merge。

#### 代码观察点

- `sql/sql_derived.cc`
- materialization 与 merge 路径

#### 案例 18：UNION ALL vs UNION

```sql
SELECT id FROM customers WHERE status = 1
UNION ALL
SELECT id FROM customers WHERE customer_level >= 3;
```

```sql
SELECT id FROM customers WHERE status = 1
UNION
SELECT id FROM customers WHERE customer_level >= 3;
```

#### 背景

`UNION ALL` 和 `UNION` 语义差别很小，但优化器处理差很多。

#### 需要解决的问题

- 是否需要去重
- 去重是否需要 temp table / sort

#### 核心逻辑

- `UNION ALL` 更像 append
- `UNION` 需要 duplicate elimination

这很好地体现了：

> SQL 语义上的一个小差别，可能带来完全不同的后处理代价。

#### 代码观察点

- `Query_expression`
- set operation
- `APPEND`
- `REMOVE_DUPLICATES`

#### 案例 19：窗口函数

```sql
SELECT
  o.customer_id,
  o.id,
  o.total_amount,
  ROW_NUMBER() OVER (
    PARTITION BY o.customer_id
    ORDER BY o.created_at DESC
  ) AS rn
FROM orders o
WHERE o.created_at >= '2026-01-01';
```

#### 背景

窗口函数不是普通聚合。  
它既要保留明细行，又要在某个分区和顺序上做分析。

#### 需要解决的问题

- 是否需要按 window key 排序
- 分区/排序后如何做窗口计算

#### 核心逻辑

窗口函数往往会叠加：

- sort
- window node

这类查询很适合观察 “Query 逻辑层很简单，但执行层操作树很丰富” 的现象。

#### 代码观察点

- `Query_block::has_windows()`
- window 相关 iterator / access path

### 15.8 DML 与 Prepared Statement 案例

#### 案例 20：UPDATE 也要走优化器

```sql
UPDATE orders
SET status = 2
WHERE customer_id = 42
  AND status = 1;
```

#### 背景

很多人误以为优化器主要服务 SELECT。  
实际上 UPDATE/DELETE 一样需要决定“先怎么找行”。

#### 需要解决的问题

- 先用什么索引定位待更新行
- 行锁会如何受访问路径影响

#### 核心逻辑

这条语句的“找行阶段”仍然是普通优化问题。  
区别在于执行阶段除了读，还会：

- 加锁
- 修改

#### 代码观察点

- `sql/sql_update.cc`
- 访问路径仍由优化器决定

#### 案例 21：DELETE ... JOIN

```sql
DELETE o
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE c.status = 0;
```

#### 背景

DELETE 也可能是多表 join 语义。

#### 需要解决的问题

- join order 仍然成立
- 但最终要删除的是哪张表的行

#### 核心逻辑

优化器仍然要先解决：

- 从谁开始最省
- 第二张表怎么连

然后执行器再按 DELETE 语义处理。

#### 代码观察点

- `sql/sql_delete.cc`
- `CreateIteratorFromAccessPath()`

#### 案例 22：Prepared Statement 点查与 plan cache

```sql
PREPARE s1 FROM
'SELECT total_amount
   FROM orders
  WHERE id = ?';

SET @id = 1000001;
EXECUTE s1 USING @id;

SET @id = 1000002;
EXECUTE s1 USING @id;
```

#### 背景

这是把前面整篇文档和 plan cache 串起来的关键案例。

#### 需要解决的问题

标准 MySQL PS 默认只缓存：

- 解析树
- 参数位

但每次 `EXECUTE` 仍然要重新 optimize。  
于是问题变成：

> 能不能在不破坏 correctness 的前提下，跳过这类点查的重复优化开销？

#### 核心逻辑

当前 `ps_point_plan_cache` 的基本思路是：

1. `PREPARE` 时识别 shape
2. 第一次正常执行时 admission
3. 后续 HOT 命中时在 `JOIN::optimize()` 早期截断
4. 基于当前 fresh runtime context 快速重建最小 plan

#### 为什么这个案例重要

它把前面所有术语连成了一条线：

- `LEX`
- `Item_param`
- `Query_arena`
- `Prepared_statement`
- `JOIN::optimize()`
- `QEP_TAB`
- `AccessPath`
- `RowIterator`

#### 最推荐的观测方式

```sql
SHOW STATUS LIKE 'Ps_point_plan_cache%';
EXPLAIN FORMAT=TREE SELECT total_amount FROM orders WHERE id = 1000001;
```

再结合：

- `optimizer_trace`
- 断点 `JOIN::optimize()`
- 断点 `ps_point_plan_build_fast_path()`

就可以把整个链路看得很完整。

### 15.9 高频组合与易踩坑案例

前面的 0-22 号案例已经把优化器主干路径串起来了，但真实业务 SQL 往往不是“单一特征”，而是多个子句叠加在一起。

这一节专门补那些线上特别常见、也特别容易让人误判的组合形态。

#### 案例 23：`GROUP BY` 之后再用 `HAVING`

```sql
SELECT customer_id, COUNT(*) AS cnt
FROM orders
WHERE created_at >= '2026-01-01'
GROUP BY customer_id
HAVING COUNT(*) >= 5;
```

#### 背景

很多人第一次接触 `HAVING` 时，会把它当成“晚一点执行的 WHERE”。  
这个理解方向不算错，但不够精确。

#### 需要解决的问题

优化器要区分两类过滤：

- 哪些条件能在分组前过滤行
- 哪些条件只能在聚合后过滤组

#### 核心逻辑

这里：

- `created_at >= '2026-01-01'` 属于**分组前过滤**
- `COUNT(*) >= 5` 属于**分组后过滤**

所以典型执行顺序是：

1. 先找出满足时间条件的订单
2. 按 `customer_id` 聚合
3. 再对聚合结果应用 `HAVING`

这个案例非常适合讲清楚：

> `WHERE` 过滤“行”，`HAVING` 过滤“组”。

#### 代码观察点

- `Query_block::having_cond`
- aggregate 相关 `AccessPath`
- aggregate 之后再接 `FILTER` 的组合

#### 案例 24：`GROUP BY` 后按聚合结果排序

```sql
SELECT customer_id, SUM(total_amount) AS total
FROM orders
WHERE status = 1
GROUP BY customer_id
ORDER BY total DESC
LIMIT 20;
```

#### 背景

这类报表 SQL 在线上非常常见。  
它把三个代价很重的动作叠在了一起：

- 过滤
- 聚合
- 排序

#### 需要解决的问题

优化器要决定：

- 先用什么方式读取 `status = 1` 的记录
- 聚合阶段是否能利用已有顺序
- `ORDER BY total` 是否能利用索引

#### 核心逻辑

这里的 `ORDER BY total` 排的是**聚合结果列**，而不是原表中的某个现成索引列。  
所以即使前面过滤能用索引，后面通常仍然要：

- 先聚合
- 再排序

这类案例能把一个关键事实讲清楚：

> “前半段用了索引”，并不代表“后半段就没有 temp table / filesort”。

#### 代码观察点

- `AccessPath::AGGREGATE`
- `filesort`
- 聚合结果物化后的排序

#### 案例 25：`OR` 谓词与 `index merge`

```sql
SELECT id, brand_id, status
FROM products
WHERE status = 1
   OR brand_id = 10;
```

#### 背景

`OR` 是单表访问里最常见的“复杂一点点”的谓词形式。  
它经常打破大家对“一个查询就走一个索引”的直觉。

#### 需要解决的问题

优化器要比较至少三类方案：

- 走 `idx_status_price`
- 走 `idx_brand_category`
- 用 `index merge union`
- 直接全表扫描

#### 核心逻辑

如果两个分支各自都有较便宜的索引范围，优化器可能构造：

- 分别扫描两个索引范围
- 合并 rowid / 主键
- 去重后回表

这就是典型的 `index merge union` 思路。

但它并不是“有 OR 就一定更好”，因为：

- 两边结果集如果都很大
- merge 与回表成本很高

那么全表扫描反而可能更便宜。

#### 代码观察点

- range optimizer
- index merge 相关路径
- `EXPLAIN` 中 `type=index_merge`

#### 案例 26：前缀 `LIKE` 可以转成范围扫描

```sql
SELECT id, email
FROM customers
WHERE email LIKE 'alice%';
```

#### 背景

很多工程师会笼统地说“`LIKE` 不能走索引”，这其实不准确。

#### 需要解决的问题

优化器要判断：

- 这个模式是不是一个可转成 B-tree 范围的前缀匹配

#### 核心逻辑

`email LIKE 'alice%'` 的本质不是“任意模糊匹配”，而更像：

- 从 `'alice'` 开始
- 到某个前缀上界为止

因此在 `uk_email` 上，它通常可以转成范围访问。

这个案例特别适合讲清楚：

> 前缀 `LIKE` 往往是 SARGable 的，前导通配符 `LIKE '%xxx'` 通常不是。

#### 代码观察点

- range optimizer
- 字符串前缀范围构造

#### 案例 27：函数包裹列会破坏索引可用性

```sql
SELECT id, email
FROM customers
WHERE LOWER(email) = 'alice@example.com';
```

#### 背景

这是最典型的“逻辑上等价，物理上完全不同”的写法差异。

#### 需要解决的问题

优化器要看：

- 条件里是否还能抽出“直接作用在索引列上的查找键”

#### 核心逻辑

一旦列被函数包裹，优化器看到的就不再是：

- `email = const`

而是：

- `LOWER(email) = const`

这意味着普通 B-tree 索引通常不能直接拿来做等值查找。  
于是计划很可能退化成：

- 扫描
- 对每行计算 `LOWER(email)`
- 再判断是否相等

除非你显式引入：

- 函数索引
- 或等价的 generated column + index

#### 代码观察点

- `Item_func`
- `update_ref_and_keys()` 无法抽出直接 `Key_use`

#### 案例 28：`ORDER BY + LIMIT + OFFSET`

```sql
SELECT id, created_at
FROM orders
WHERE status = 1
ORDER BY created_at DESC
LIMIT 1000, 20;
```

#### 背景

很多分页 SQL 在 EXPLAIN 里“看着不错”，但运行时仍然很慢。  
这个案例就是最典型的例子。

#### 需要解决的问题

优化器要判断：

- `idx_status_created` 是否能同时满足过滤和排序

执行器还要面对另一个问题：

- 即使顺序对了，也必须跳过前 1000 行后才能返回 20 行

#### 核心逻辑

如果索引顺序可用，优化器通常会优先选它，因为可以避免 filesort。  
但这并不等于这条 SQL 很便宜，因为 `OFFSET` 本身就意味着：

- 前面的匹配行也得先读出来
- 只是最后不返回给客户端

这个案例特别适合强调：

> 有些代价不是“计划选错了”，而是 SQL 形状本身就要求先走过一大段结果。

#### 代码观察点

- `test_if_skip_sort_order()`
- limit / offset 相关执行路径

#### 案例 29：`MIN/MAX + GROUP BY` 与顺序聚合

```sql
SELECT customer_id, MIN(created_at) AS first_order_time
FROM orders
GROUP BY customer_id;
```

#### 背景

这个案例是讲“索引顺序不仅能帮过滤和排序，也能帮聚合”的好材料。

#### 需要解决的问题

优化器要看：

- `idx_customer_created(customer_id, created_at)` 的顺序
- 是否足以让每个 `customer_id` 的最小 `created_at` 更便宜地求出来

#### 核心逻辑

在理想情况下，优化器可以利用索引顺序，把这类问题做成：

- 按 `customer_id` 分组顺序读取
- 更早地得到每组最小值

某些版本/场景下，你会看到类似：

- `Using index for group-by`
- 或者与 loose index scan / streaming aggregate 非常接近的执行形态

这个案例的价值不在于死记某个 plan 名字，而在于建立一个直觉：

> “分组”不一定意味着“先把所有行攒起来再算”；如果输入天然有序，很多事情都能流式做。

#### 代码观察点

- group by 优化
- range/group access 相关逻辑

#### 案例 30：`SELECT ... FOR UPDATE`

```sql
SELECT *
FROM orders
WHERE id = 1000001
FOR UPDATE;
```

#### 背景

从优化器视角看，这和普通主键点查很像；  
从事务与锁视角看，这和普通 `SELECT` 完全不是一回事。

#### 需要解决的问题

优化器要先选访问路径，执行器和存储引擎还要进一步决定：

- 锁哪些记录
- 锁范围多大

#### 核心逻辑

这条 SQL 通常仍会走主键点查。  
但它特别适合拿来讲清楚一个经常被忽略的事实：

> 访问路径不仅影响性能，还影响锁覆盖范围和并发行为。

如果是唯一键点查，锁的 footprint 往往更小；  
如果退化成范围扫或全表扫，锁行为也可能明显变重。

#### 代码观察点

- locking read 相关执行路径
- `handler` 层加锁读取

### 15.10 子查询、CTE 与派生结构进阶案例

#### 案例 31：`SELECT` 列表中的标量子查询

```sql
SELECT
  c.id,
  c.email,
  (
    SELECT COUNT(*)
    FROM orders o
    WHERE o.customer_id = c.id
  ) AS order_cnt
FROM customers c
WHERE c.status = 1;
```

#### 背景

很多应用 SQL 会把“每个用户的统计值”直接写成标量子查询。  
它在语义上很直观，但在执行上不一定便宜。

#### 需要解决的问题

优化器要判断：

- 这个相关子查询能否改写
- 还是必须对外层每一行去执行一次内层统计

#### 核心逻辑

这类查询的难点在于：

- 外层 `customers` 先产出一批行
- 每一行都携带不同的 `c.id`
- 内层 `orders` 要用这个 `c.id` 重新做一遍查找或聚合

如果外层基数很大，这种“外层行数 × 内层代价”的乘法效应会很明显。

#### 代码观察点

- `Item_subselect`
- outer reference
- correlated subquery 执行路径

#### 案例 32：`NOT EXISTS` 与 anti-join

```sql
SELECT c.id, c.email
FROM customers c
WHERE NOT EXISTS (
  SELECT 1
  FROM orders o
  WHERE o.customer_id = c.id
    AND o.status = 1
);
```

#### 背景

“查找没有下过已支付订单的客户”这类需求在线上非常常见。  
它的语义看起来只是把 `EXISTS` 反过来，但优化挑战并不小。

#### 需要解决的问题

优化器要判断：

- 能否把它变成 anti-semi join
- 还是保留相关子查询逐行判断

#### 核心逻辑

`NOT EXISTS` 的关键是：

- 一旦找到一条匹配行，就可以判定外层当前行失败
- 真正需要的是“是否存在”，而不是“有多少条”

所以它非常适合讲解：

- semijoin / antijoin 语义
- early-out 的价值

也非常适合顺手对比：

- `NOT EXISTS`
- `NOT IN`

二者在 `NULL` 语义上并不完全相同。

#### 代码观察点

- semijoin / antijoin 转换
- correlated EXISTS 路径

#### 案例 33：可 merge 的 derived table

```sql
SELECT dt.id, dt.customer_id
FROM (
  SELECT id, customer_id
  FROM orders
  WHERE status = 1
) AS dt
WHERE dt.customer_id = 42;
```

#### 背景

这个案例和前面的“带聚合的 derived table”正好相反。  
它故意写成一个很“薄”的派生表，方便观察 merge。

#### 需要解决的问题

优化器要决定：

- 是先物化 `dt`
- 还是把 `dt` 直接并回外层查询块

#### 核心逻辑

因为子查询里没有：

- 聚合
- DISTINCT
- 窗口函数
- set operation

它更有机会被 merge 回外层。  
一旦 merge 成功，优化器就能把原来分两层的条件放在一个 query block 里统一考虑。

这个案例很适合和案例 17 对着看：

- 案例 17 更容易 materialize
- 案例 33 更容易 merge

#### 代码观察点

- `sql/sql_derived.cc`
- derived merge 判定逻辑

#### 案例 34：非递归 CTE

```sql
WITH active_customers AS (
  SELECT id, region_id
  FROM customers
  WHERE status = 1
)
SELECT ac.id, r.city
FROM active_customers ac
JOIN regions r ON r.id = ac.region_id;
```

#### 背景

CTE 在写法上像“给子查询起名字”，但优化器不能只把它当作语法糖。

#### 需要解决的问题

优化器要判断：

- 这个 CTE 是更像一个可 merge 的 derived table
- 还是更适合作为物化结果被复用

#### 核心逻辑

非递归 CTE 在很多方面和 derived table 很像。  
所以读这类计划时，一个非常实用的心法是：

> 先别把 CTE 神秘化，先问自己：如果把它改写成 FROM 里的子查询，优化问题是不是本质一样？

通常答案是“很接近”。

#### 代码观察点

- CTE 与 derived 共享的 materialize / merge 逻辑
- `Query_expression` 层的组织方式

#### 案例 35：递归 CTE

```sql
WITH RECURSIVE seq(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1
  FROM seq
  WHERE n < 10
)
SELECT n
FROM seq;
```

#### 背景

递归 CTE 是一个非常好的提醒：

> 不是所有 SQL 的优化核心都是 join order。

#### 需要解决的问题

这里的重点不再是：

- 先扫哪张表

而是：

- anchor member 怎么执行
- recursive member 怎么反复执行
- 中间 working table 怎么维护

#### 核心逻辑

递归 CTE 往往会引入一个“迭代求不动点”的执行框架：

1. 先执行 anchor 部分
2. 把结果写入工作集
3. 反复执行 recursive 部分，直到没有新行

这类计划非常适合拿来讲清楚：

- materialization 不是只有性能含义，也可能是语义执行模型的一部分

#### 代码观察点

- recursive CTE 相关物化执行路径
- working table / temp table

### 15.11 更多 DML、锁与人工干预案例

#### 案例 36：多表 `UPDATE`

```sql
UPDATE orders o
JOIN customers c ON c.id = o.customer_id
SET o.status = 9
WHERE c.status = 0
  AND o.status = 1;
```

#### 背景

这是“先按 join 找到目标行，再修改其中一张表”的典型案例。  
它比单表 UPDATE 更能体现优化器和执行器的配合。

#### 需要解决的问题

优化器仍然要解决：

- 先从 `customers` 还是 `orders` 开始
- 哪一侧的过滤性更强
- 如何减少将被修改的目标行集合

#### 核心逻辑

就优化问题而言，它和普通 join 查询没有本质区别。  
区别在于最终执行时：

- 不是把结果返回客户端
- 而是对目标表的对应行做修改

这个案例很适合说明：

> DML 的“读阶段”本质上仍是查询优化问题。

#### 代码观察点

- `sql/sql_update.cc`
- join plan 到 update 执行的桥接

#### 案例 37：`STRAIGHT_JOIN` 人工固定 join 顺序

```sql
SELECT STRAIGHT_JOIN
  c.id, o.id, oi.product_id
FROM customers c
JOIN orders o ON o.customer_id = c.id
JOIN order_items oi ON oi.order_id = o.id
WHERE c.status = 1;
```

#### 背景

大多数时候我们希望优化器自由选择 join order。  
但在统计信息不准、已知某种顺序更好时，业务和 DBA 有时会显式干预。

#### 需要解决的问题

这条 SQL 的重点不再是“优化器会选什么”，而是：

- 当用户固定顺序后，优化器还能保留多少自由度

#### 核心逻辑

`STRAIGHT_JOIN` 的含义可以粗略理解成：

- 按 SQL 中给出的表顺序驱动 join

这样做可能：

- 修正错误估算导致的坏计划
- 也可能反过来把优化器本来能找到的更好顺序堵死

这个案例很适合在文档里强调一个成熟系统必须支持的现实：

> 再好的优化器，也要给用户留“纠偏”和“保底”的手段。

#### 代码观察点

- join order 选择阶段
- hint / straight join 对搜索空间的约束

#### 案例 38：`INSERT ... SELECT`

```sql
INSERT INTO order_items (
  id, order_id, product_id, quantity, price
)
SELECT
  oi.id + 1000000000,
  oi.order_id,
  oi.product_id,
  oi.quantity,
  oi.price
FROM order_items oi
WHERE oi.order_id = 1000001;
```

#### 背景

这类语句把“查询计划”和“写入执行”直接串在了一起。

#### 需要解决的问题

优化器主要负责的是 `SELECT` 部分：

- 如何高效找出源行

执行器随后还要处理：

- 插入目标表
- 唯一键检查
- 可能的触发器/约束副作用

#### 核心逻辑

它很适合作为一个边界案例，帮助读者建立这样的认识：

> 优化器并不负责 SQL 的全部成本，但它负责“把输入行集合高效找出来”这一大块。

一旦 `SELECT` 部分退化成坏计划，整个 `INSERT ... SELECT` 也会被拖慢。

#### 代码观察点

- `Sql_cmd_insert_select`
- `SELECT` 计划与写入路径的衔接

### 15.12 如何继续扩案例

上面的 39 个小案例已经覆盖了优化器最常见、最核心的一批形态，但仍然不是“所有 MySQL 语句形态”的完整枚举。

如果你继续深挖，建议沿下面几个方向扩展：

1. 分区表
   - 重点看 `prune_table_partitions()`

2. JSON / GIS / fulltext
   - 重点看特殊函数和专用索引如何改变可用访问路径

3. 多列统计与直方图
   - 重点看 selectivity 估算漂移

4. 窗口函数组合排序
   - 重点看多次排序与 temp table 交互

5. UPDATE/DELETE 在不同事务隔离级别下的锁行为
   - 重点看“访问路径如何影响加锁”

6. 分布式执行 / 并行执行
   - 重点看本地优化器与分布式调度之间的边界

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

## 附录 B: 术语速查表

这一节是“速记版词典”，用于快速回查。  
如果想看每个术语更完整的背景、解决的问题、核心逻辑和代码场景，请继续看附录 C。

| 术语 | 速记含义 | 典型代码落点 |
|------|----------|--------------|
| `THD` | 单连接/单线程上下文，几乎所有语句级状态的根对象 | `sql/sql_class.h` |
| `MEM_ROOT` | arena allocator，按生命周期批量分配/释放内存 | `include/my_alloc.h` |
| `Query_arena` | 决定“对象应该分配到哪个 mem_root、处于哪种语句生命周期”的容器 | `sql/sql_class.h` |
| `LEX` | 一条 SQL 的顶层解析结果容器 | `sql/sql_lex.h` |
| `Query_expression` | 完整查询表达式，可能包含 UNION/INTERSECT/EXCEPT | `sql/sql_lex.h` |
| `Query_block` | 单个 SELECT 子句 | `sql/sql_lex.h` |
| `Table_ref` | 解析树中的表引用，表示“SQL 写到的那张表” | `sql/table.h` |
| `TABLE` | 打开后的真实表对象，持有 `handler` | `sql/table.h` |
| `Field` | 具体列对象，直接映射到表列元数据 | `sql/field.h`, `sql/table.h` |
| `Item` | 表达式节点基类 | `sql/item.h` |
| `Item_field` | 列引用表达式 | `sql/item.h` |
| `Item_param` | Prepared Statement 的参数占位符 | `sql/item.h` |
| `fix_fields()` | 表达式语义绑定入口，把“列名”绑到真实列对象 | `sql/item.cc` |
| `open_tables_for_query()` | 打开表、绑定 `Table_ref -> TABLE` 的入口 | `sql/sql_base.cc` |
| `JOIN` | per-query-block 的优化器上下文，也是 legacy 路径的总协调器 | `sql/sql_optimizer.h` |
| `JOIN_TAB` | legacy 优化期间的每表槽位 | `sql/sql_select.h`, `sql/sql_optimizer.h` |
| `QEP_TAB` | legacy 执行计划中的每表槽位 | `sql/sql_executor.h` |
| `POSITION` | join order 搜索中某一步的代价/行数描述 | `sql/sql_select.h` |
| `Key_use` | 优化器从条件中提取出的“可用索引等值条件” | `sql/sql_select.h` |
| `Index_lookup` | 一次 ref/eq_ref 索引查找所需的 key buffer 和参数描述 | `sql/sql_opt_exec_shared.h` |
| `QUICK` / `QUICK_SELECT_I` | range optimizer 产出的快速范围访问结构 | `sql/range_optimizer/` |
| `AccessPath` | 统一物理计划节点，tagged union | `sql/join_optimizer/access_path.h` |
| `RowIterator` | 运行时执行算子基类 | `sql/iterators/row_iterator.h` |
| `handler` | 存储引擎抽象接口 | `sql/handler.h` |
| `Legacy Optimizer` | 当前默认优化器主路径，核心是 `JOIN::optimize()` | `sql/sql_optimizer.cc`, `sql/sql_planner.cc` |
| `Hypergraph Optimizer` | 新一代 join optimizer，直接构建 `AccessPath` 树 | `sql/join_optimizer/` |
| `DPhyp` | Hypergraph optimizer 的核心枚举算法 | `sql/join_optimizer/subgraph_enumeration.h` |
| `selectivity` | 条件过滤比例估算 | `sql/opt_statistics.h`, `sql/opt_costmodel.cc` |
| `fanout` | 外层一行驱动内层平均会产出多少行 | `sql/sql_planner.cc` |
| `const table` | 优化期就能确定为 0/1 行的表 | `sql/sql_optimizer.cc` |
| `JT_CONST` | const table 的 join/access type | `sql/sql_opt_exec_shared.h` |
| `JT_EQ_REF` | 唯一键等值查找，一行驱动最多命中一行 | `sql/sql_opt_exec_shared.h` |
| `JT_REF` | 非唯一索引等值查找 | `sql/sql_opt_exec_shared.h` |
| `JT_RANGE` | 范围扫描 | `sql/sql_opt_exec_shared.h` |
| `JT_ALL` | 全表扫描 | `sql/sql_opt_exec_shared.h` |
| `HAVING` | 聚合完成后再对“组结果”做过滤的条件 | `sql/sql_lex.h`, `Query_block` |
| `SARGable` | 谓词能否直接转成索引可利用查找条件的性质 | `sql/sql_optimizer.cc`, `sql/range_optimizer/` |
| `index merge` | 单表上组合多个索引结果的访问方式 | `sql/range_optimizer/` |
| `loose index scan` | 借助索引顺序跳读部分记录的 group/min-max 优化形态 | `sql/range_optimizer/` |
| `semijoin` | 为 `IN/EXISTS` 等子查询变换出的半连接语义 | `sql/sql_resolver.cc`, `sql/sql_optimizer.cc` |
| `anti-join` | 为 `NOT EXISTS` 等反存在判断变换出的连接语义 | `sql/sql_resolver.cc`, `sql/sql_optimizer.cc` |
| `materialization` | 先把中间结果算出并存起来，再在外层消费 | `sql/sql_derived.cc`, `sql/join_optimizer/` |
| `CTE` | Common Table Expression，命名子查询块 | `sql/sql_lex.h`, `sql/sql_derived.cc` |
| `recursive CTE` | 带 working table 的递归公共表表达式执行形态 | `sql/sql_derived.cc` |
| `filesort` | MySQL 的排序框架，不等于“一定落磁盘文件” | `sql/filesort.cc` |
| `temporary table` | 承载中间结果、聚合或排序结果的临时表 | `sql/sql_tmp_table.cc` |
| `locking read` | `FOR UPDATE`/`FOR SHARE` 这类带锁读 | `sql/sql_select.cc`, `sql/handler.h` |
| `NLJ` | Nested Loop Join | `sql/iterators/composite_iterators.h` |
| `Hash Join` | 基于哈希表的 join 实现 | `sql/iterators/hash_join_iterator.h` |
| `BKA` | Batched Key Access，批量键访问优化 | `sql/iterators/bka_iterator.h` |
| `MRR` | Multi-Range Read，多范围读/批量回表优化 | `sql/handler.h`, `sql/iterators/ref_row_iterators.cc` |
| `STRAIGHT_JOIN` | 强制优化器按书写顺序做 join 的语义开关 | `sql/sql_lex.h`, `sql/sql_planner.cc` |
| `EXPLAIN FORMAT=TREE` | 最接近 `AccessPath` 树的展示方式 | `sql/join_optimizer/explain_access_path.cc` |
| `optimizer_trace` | 优化器决策过程的可观测输出 | `sql/opt_trace.cc` |
| `PS` | Prepared Statement | `sql/sql_prepare.h/.cc` |
| `reprepare` | DDL 等导致旧解析/计划失效后重新 prepare | `sql/sql_prepare.cc` |
| `ER_NEED_REPREPARE` | 触发 reprepare 的关键错误码 | `sql/sql_prepare.cc`, `sql/sql_class.h` |
| `plan template` | plan cache 中只缓存稳定元数据、每次执行再重建计划的思路 | `sql/ps_point_plan_cache.h/.cc` |
| `SPM` | SQL Plan Management，业界常见的计划治理能力 | Oracle 概念，见相关设计文档 |

## 附录 C: 扩展术语详解

本附录按“背景 -> 要解决的问题 -> 核心逻辑 -> 代码场景”的顺序解释术语。  
重点不是背定义，而是理解：

- 为什么会有这个术语
- 它在优化器架构中的位置
- 读代码时该在什么场景下想到它

### C.1 生命周期与内存

#### `THD`

**背景**

MySQL 是 per-connection/per-thread 模型。每个客户端连接都需要一个“总上下文对象”，保存：

- 当前语句
- 会话变量
- 错误状态
- 打开的表
- 当前 `LEX`
- 内存 arena

**要解决的问题**

如果没有一个统一根对象，parse、resolve、optimize、execute 各阶段就无法共享会话级状态。

**核心逻辑**

`THD` 是语句执行期间几乎所有对象的“根环境”：

- `current_thd`
- `thd->lex`
- `thd->mem_root`
- `thd->variables`
- `thd->stmt_arena`

很多核心函数的第一个参数都是 `THD *`，本质上是在说：

> 这个操作一定依赖当前连接/当前语句环境。

**代码场景**

- 类定义：`sql/sql_class.h`
- 顶层执行：`mysql_execute_command()`
- 优化入口：`JOIN::optimize()`
- Prepared statement：`Prepared_statement::prepare()` / `execute()`

#### `MEM_ROOT`

**背景**

优化器和执行器会创建大量短生命周期对象。  
如果每个对象都单独 `malloc/free`，开销高、碎片多、清理复杂。

**要解决的问题**

把“生命周期相近的一批对象”放进一个 arena，一次性释放。

**核心逻辑**

`MEM_ROOT` 是 MySQL 经典的 arena allocator：

- 分配很快
- 通常不做单对象 free
- 一批对象生命周期结束后整体清空

这非常适合：

- parse tree
- optimize 临时对象
- execute 临时对象

但也决定了一个重要约束：

> 只要对象分配到了某个 `MEM_ROOT` 上，就必须尊重这个 memroot 的生命周期。

这也是很多 plan cache 设计不能直接复用对象的根本原因。

**代码场景**

- 定义：`include/my_alloc.h`
- 语句执行结束时清理 `thd->mem_root`
- Prepared statement 使用独立 memroot 保存可跨执行对象

#### `Query_arena`

**背景**

只有 `MEM_ROOT` 还不够，因为系统还需要知道：

- 当前是在 prepare 阶段，还是 execute 阶段
- 当前新创建的 `Item` 应该分到 statement arena 还是 execution arena

**要解决的问题**

给同样的 arena allocator 加上“语义”和“生命周期状态”。

**核心逻辑**

`Query_arena` 持有：

- `MEM_ROOT *mem_root`
- statement state
- item list

它回答的问题是：

> 现在创建出来的对象，应该活多久？

在 PS 场景里尤其关键：

- prepare 时，一些对象要保存在 statement arena 上，跨执行保留
- execute 时，另一些对象只该活到本次执行结束

**代码场景**

- 定义：`sql/sql_class.h`
- PS 生命周期：`Prepared_statement::prepare()`, `execute()`
- reprepare：`swap_query_arena()`

### C.2 逻辑查询结构

#### `LEX`

**背景**

SQL parse 完成后，需要一个顶层容器把整条语句的解析结果挂起来。

**要解决的问题**

把“这条 SQL 的所有语法/语义对象”组织成一棵可继续 resolve、optimize、execute 的结构。

**核心逻辑**

`LEX` 是一条语句的顶层解析容器，里面挂着：

- `sql_command`
- `unit` / `Query_expression`
- hints
- query tables
- 各种语句级设置

它是 parse 阶段结束后的“总入口”。

**代码场景**

- 定义：`sql/sql_lex.h`
- parse 后保存在 `thd->lex`
- PS 场景中保存在 `Prepared_statement::m_lex`

#### `Query_expression`

**背景**

SQL 标准里的“完整查询表达式”不一定只是一个 SELECT，还可能是：

- `SELECT ... UNION SELECT ...`
- `INTERSECT`
- `EXCEPT`

**要解决的问题**

需要一个对象表示“一个完整查询”，不管它内部有几个 SELECT block。

**核心逻辑**

`Query_expression` 是完整查询表达式的根：

- 简单 SELECT 时，它是一个退化容器，里面只有一个 `Query_block`
- set operation 时，它管理多个 `Query_block`/`Query_term`

它还持有：

- `m_root_access_path`
- `m_root_iterator`

所以它也是 optimize 与 execute 的桥梁。

**代码场景**

- 定义：`sql/sql_lex.h`
- 优化入口：`Query_expression::optimize()` in `sql/sql_union.cc`
- 执行阶段：`Query_expression::execute()`

#### `Query_block`

**背景**

一个 `UNION` 里的每个 `SELECT`，其实都需要单独 resolve 和 optimize。

**要解决的问题**

给单个 `SELECT ... FROM ... WHERE ...` 一个独立的语义与优化单位。

**核心逻辑**

`Query_block` 对应一个单独的 SELECT block，包含：

- `leaf_tables`
- `where_cond`
- `having_cond`
- `group_list`
- `order_list`
- `JOIN *join`

优化阶段通常是：

- 一个 `Query_block`
- 对应一个 `JOIN`

所以阅读 legacy optimizer 时，`Query_block` 是通往 `JOIN::optimize()` 的上游入口。

**代码场景**

- 定义：`sql/sql_lex.h`
- resolve：`Query_block::prepare()` in `sql/sql_resolver.cc`
- optimize：`Query_block::optimize()` in `sql/sql_select.cc`

#### `Table_ref`

**背景**

在 parse 阶段，系统只看到了：

- 库名
- 表名
- alias

此时还没有真正打开表。

**要解决的问题**

先把“SQL 里提到的表”表示出来，等后面 open table 时再绑定成真实 `TABLE *`。

**核心逻辑**

`Table_ref` 是解析树中的表引用对象。  
它更接近“名字级”的表，而不是“已打开的运行时表”。

这是理解优化器与存储引擎边界的关键：

- parse/resolve 早期主要处理 `Table_ref`
- 真正访问存储引擎时才依赖 `TABLE`

**代码场景**

- 定义：`sql/table.h`
- `Query_block::leaf_tables`
- `open_tables_for_query()` 把 `Table_ref -> TABLE` 绑定起来

#### `TABLE`

**背景**

真正读写数据时，光有表名不够，必须有：

- 打开的表句柄
- 列元数据
- 存储引擎 handler

**要解决的问题**

为执行阶段提供真正可读写的数据对象。

**核心逻辑**

`TABLE` 是运行时真实表对象，典型成员包括：

- `field[]`
- `key_info[]`
- `TABLE_SHARE *s`
- `handler *file`

在很多优化器决策里，`TABLE` 也是必需的，因为：

- 统计信息在这里
- 索引信息在这里
- handler 能力也在这里

**代码场景**

- 定义：`sql/table.h`
- 由 `open_tables_for_query()` 建立
- 由 iterator 最终通过 `table->file` 调引擎

#### `Item`

**背景**

SQL 里的表达式形式很多：

- 常量
- 列引用
- 比较
- 函数
- 子查询

**要解决的问题**

用一套统一的对象体系表示所有表达式节点。

**核心逻辑**

`Item` 是表达式树基类。  
它下面分出大量子类，例如：

- `Item_field`
- `Item_int`
- `Item_func_eq`
- `Item_param`
- `Item_subselect`

优化器的很多工作，本质上是在变换或分析 `Item` 树：

- 条件下推
- 常量折叠
- 等值传播
- key 提取

**代码场景**

- 定义：`sql/item.h`
- 语义绑定：`fix_fields()`
- 条件分析：`update_ref_and_keys()`

#### `Item_field`

**背景**

SQL 里写的列名，parse 完成时只是一个名字，不是实际列对象。

**要解决的问题**

把“列名引用”绑定到真正的列定义。

**核心逻辑**

`Item_field` 是列引用表达式。  
它在 `fix_fields()` 中完成真正的列绑定，最终能关联到：

- `Field`
- 所属 `TABLE`

**代码场景**

- 定义：`sql/item.h`
- 绑定入口：`Item_field::fix_fields()` in `sql/item.cc`

#### `Item_param`

**背景**

Prepared Statement 需要在 prepare 时先保留参数位置，等 execute 时再填入值。

**要解决的问题**

让解析树在没有实际参数值时也能存在，并在执行前安全注入具体值。

**核心逻辑**

`Item_param` 是 PS 的参数占位符节点。  
prepare 阶段它只是“洞位”；execute 阶段才有真实值和真实类型。

这也是 plan cache 要特别关注它的原因：

- 参数值每次都变
- 参数类型/unsigned/collation 也可能变

**代码场景**

- 定义：`sql/item.h`
- `Item_param::fix_fields()` in `sql/item.cc`
- PS 绑定参数：`Prepared_statement::execute_loop()`

#### `fix_fields()`

**背景**

表达式树 parse 完后，很多节点还只是“语法上合法”，并没有绑定到真实对象。

**要解决的问题**

做语义绑定和类型修正，让表达式从“文本树”变成“可优化、可执行的语义树”。

**核心逻辑**

`fix_fields()` 是 `Item` 层次的核心语义绑定入口。  
不同子类在这里完成：

- 列绑定
- 子表达式递归绑定
- 类型推导
- 某些重写

可以把它理解成：

> parse 之后，表达式真正“长骨架”的一步。

**代码场景**

- 基类：`Item::fix_fields()` in `sql/item.cc`
- 列引用：`Item_field::fix_fields()`
- 参数：`Item_param::fix_fields()`

#### `open_tables_for_query()`

**背景**

优化器和执行器不可能只靠表名字符串工作，必须真正打开表并拿到元数据锁、`TABLE` 对象和 handler。

**要解决的问题**

在语句执行前建立：

- `Table_ref -> TABLE`
- metadata lock
- 版本校验

**核心逻辑**

`open_tables_for_query()` 是优化器与存储引擎的第一个大交汇点。  
很多“这条 SQL 为什么需要 reprepare”“为什么表版本变化后缓存失效”的故事，都从这里开始。

**代码场景**

- 定义：`sql/sql_base.cc`
- 被 SELECT/UPDATE/DELETE 等路径在 optimize 前调用

### C.3 Legacy 优化结构

#### `JOIN`

**背景**

legacy optimizer 需要一个总对象，在一个 `Query_block` 上集中管理：

- 表集合
- 条件
- join order 搜索
- 代价估算
- 最终计划

**要解决的问题**

把一个 query block 的优化过程和执行前状态集中到一个协调器对象里。

**核心逻辑**

`JOIN` 不是“join 算子”这么简单，它更像：

> legacy 优化器对一个 query block 的总控制器。

里面同时有：

- 优化期结构：`join_tab`, `best_positions`, `keyuse_array`
- 执行前结构：`qep_tab`
- 统一计划层：`m_root_access_path`

**代码场景**

- 定义：`sql/sql_optimizer.h`
- 主流程：`JOIN::optimize()` in `sql/sql_optimizer.cc`

#### `JOIN_TAB`

**背景**

join order 搜索期，需要对“每一张候选表”保存大量搜索过程中的临时信息。

**要解决的问题**

为 legacy optimizer 的搜索阶段提供 per-table 工作槽位。

**核心逻辑**

`JOIN_TAB` 更偏“优化期间使用的槽位”，不是最终执行计划。

可以简单记：

- `JOIN_TAB`：优化期
- `QEP_TAB`：执行计划期

**代码场景**

- `JOIN::join_tab`
- `best_access_path()`
- `make_join_plan()`

#### `POSITION`

**背景**

join order 搜索不是只要顺序，还要记录“这个顺序走到这里时的代价和行数估算”。

**要解决的问题**

把 join order 搜索过程中每一步的代价状态保留下来，方便比较和回溯。

**核心逻辑**

`POSITION` 记录某一步的：

- 代价
- fanout
- access method
- 已绑定 key parts 等

它是 legacy 搜索算法里的核心状态对象之一。

**代码场景**

- 定义：`sql/sql_select.h`
- 使用：`Optimize_table_order::choose_table_order()`

#### `Key_use`

**背景**

WHERE/ON 条件里可能有很多索引可利用的信息，但在真正比较访问路径前，需要先把它们提取出来。

**要解决的问题**

把“条件里的索引可用性”从表达式树中抽取成结构化数据。

**核心逻辑**

`Key_use` 记录：

- 哪张表
- 哪个索引
- 哪个 key part
- 对应哪个等值条件
- 这个值依赖哪些外表

它是 `best_access_path()` 决定 ref/eq_ref 的重要输入。

**代码场景**

- 定义：`sql/sql_select.h`
- 提取：`update_ref_and_keys()` in `sql/sql_optimizer.cc`

#### `Index_lookup`

**背景**

优化器决定用 ref/eq_ref 之后，执行器还需要一个结构来保存“这次索引查找如何序列化 key、用哪些参数、怎样调用 handler”。

**要解决的问题**

把一次索引等值查找描述成执行器可直接消费的结构。

**核心逻辑**

`Index_lookup` 里面有：

- `key`
- `key_parts`
- `key_length`
- `key_buff`
- `items`
- `key_copy`
- `null_rejecting`

它是从“逻辑条件”落地到“如何构造索引 key buffer”的桥梁。

**代码场景**

- 定义：`sql/sql_opt_exec_shared.h`
- 使用：`QEP_TAB::ref()`
- 目标 iterator：`EQRefIterator`, `RefIterator`

#### `QEP_TAB`

**背景**

join order 确定后，legacy optimizer 需要把“搜索结果”转换成“执行计划中的每表槽位”。

**要解决的问题**

承载最终 legacy 执行计划中的 per-table 信息。

**核心逻辑**

`QEP_TAB` 比 `JOIN_TAB` 更接近执行器：

- 有 `table()`
- 有 `type()`
- 有 `ref()`
- 有 `quick()`

它是从 legacy optimizer 通往 executor/AccessPath 的最后一道中间层。

**代码场景**

- 定义：`sql/sql_executor.h`
- 生成：`get_best_combination()`
- 转换：`create_access_paths()`

#### `best_access_path()`

**背景**

决定一张表到底怎么访问，是整个优化器最核心的决策之一。

**要解决的问题**

在候选访问方式之间选出代价最低者：

- const
- eq_ref
- ref
- range
- index scan
- table scan

**核心逻辑**

`best_access_path()` 会综合：

- `Key_use`
- range 分析
- fanout/selectivity
- 成本模型

最终决定单表最优访问方式。

**代码场景**

- `Optimize_table_order::best_access_path()` in `sql/sql_planner.cc`

#### `QUICK` / `test_quick_select()`

**背景**

范围扫描不是简单的等值匹配，需要专门的 range optimizer 分析可能的范围访问路径。

**要解决的问题**

决定是否存在值得采用的 range scan，以及它的预计代价和行数。

**核心逻辑**

`test_quick_select()` 是 range optimizer 的重要入口之一。  
它会尝试构造 `QUICK`/range access 相关结构，为后续访问路径选择提供依据。

**代码场景**

- `sql/range_optimizer/`
- 被 `JOIN::optimize()` 和某些 iterator 路径调用

### C.4 Unified Plan 与运行时对象

#### `AccessPath`

**背景**

legacy 路径原本有很多历史结构，hypergraph 又直接产出另一套表示。  
需要一个统一的物理计划表示，既能承接旧路径，也能服务新路径和执行器。

**要解决的问题**

给所有物理操作提供统一 IR（中间表示）。

**核心逻辑**

`AccessPath` 是 tagged union：

- 一个 `type`
- 一组成本字段
- 一段 type-specific payload

叶节点可以是：

- `EQ_REF`
- `INDEX_RANGE_SCAN`
- `TABLE_SCAN`

中间节点可以是：

- `NESTED_LOOP_JOIN`
- `HASH_JOIN`
- `SORT`
- `FILTER`

**代码场景**

- 定义：`sql/join_optimizer/access_path.h`
- 创建 iterator：`CreateIteratorFromAccessPath()`
- legacy 和 hypergraph 最终都汇聚到这里

#### `RowIterator`

**背景**

优化器选完计划后，还需要真正执行。  
而执行器最关心的是：

- Init
- Read next row
- EOF / error

**要解决的问题**

把物理计划翻译成一棵真正可执行、可重置、可组合的运行时代码对象树。

**核心逻辑**

`RowIterator` 提供统一接口：

- `Init()`
- `Read()`
- `SetNullRowFlag()`

它本质上是 Volcano 风格 iterator 接口。

**代码场景**

- 定义：`sql/iterators/row_iterator.h`
- 工厂：`CreateIteratorFromAccessPath()`
- 具体实现：`EQRefIterator`, `TableScanIterator`, `HashJoinIterator`

#### `CreateIteratorFromAccessPath()`

**背景**

计划对象不是直接可执行代码对象，还需要一步翻译。

**要解决的问题**

把 `AccessPath` 树映射为对应的 `RowIterator` 树。

**核心逻辑**

这是一个大 switch：

- `EQ_REF` -> `EQRefIterator`
- `TABLE_SCAN` -> `TableScanIterator`
- `HASH_JOIN` -> `HashJoinIterator`
- `SORT` -> `SortingIterator`

它是“计划层”和“执行层”的转换边界。

**代码场景**

- 定义：`sql/join_optimizer/access_path.cc`
- 调用方：`Query_expression::execute()` 等

### C.5 算法、访问方式与常见优化术语

#### `selectivity`

**背景**

优化器最重要的问题之一是：

> 这个条件到底能过滤掉多少行？

**要解决的问题**

估算条件过滤比例，决定是走索引、扫描还是不同 join 顺序。

**核心逻辑**

`selectivity` 近似就是“保留下来的比例”。  
选择性越高，往往越适合索引访问。

**代码场景**

- 代价模型
- `records_in_range()`
- `rec_per_key_float[]`
- 直方图统计

#### `fanout`

**背景**

join 顺序选择的关键不是单表多少行，而是：

> 外层进来一行，会把内层放大成多少行？

**要解决的问题**

估算 join 链条中的放大效应。

**核心逻辑**

`fanout` 可以粗略理解成：

- 外层一行
- 驱动内层平均返回多少行

fanout 大，后续 join 成本会被快速放大。

**代码场景**

- `best_access_path()`
- `POSITION` 里的代价估算

#### `JT_CONST` / `const table`

**背景**

有些表在优化阶段就能确定只有 0/1 行，例如主键常量查找。

**要解决的问题**

尽量提前把这类表“消化掉”，减少 join 搜索空间。

**核心逻辑**

`JT_CONST` 表示：

- 这张表在优化期就被当作常量表处理
- 后续 join 搜索时它不再像普通表那样参与复杂排列

**代码场景**

- `join->const_tables`
- `QEP_TAB::type() == JT_CONST`

#### `JT_EQ_REF`

**背景**

唯一键等值查找是 OLTP 最重要、也最稳定的访问模式之一。

**要解决的问题**

表达“外层一行驱动内层唯一索引查找，最多命中一行”的访问语义。

**核心逻辑**

`JT_EQ_REF` 意味着：

- 唯一键
- 等值
- 一对一

这类访问路径代价可预测、缓存潜力高，所以也是 point plan cache 的核心目标。

**代码场景**

- `QEP_TAB::type()`
- `EQRefIterator`
- `ps_point_plan_cache`

#### `JT_REF`

**背景**

很多索引查找不是唯一键，只能保证“按索引等值过滤”，不能保证只返回一行。

**要解决的问题**

表达“等值索引查找，但可能命中多行”的访问语义。

**核心逻辑**

和 `JT_EQ_REF` 相比，`JT_REF` 的核心区别是：

- fanout 不再固定接近 1
- 对统计信息更敏感

**代码场景**

- `RefIterator`
- join order 成本估算

#### `JT_RANGE`

**背景**

`BETWEEN`、`>`, `<`, `IN` 等常见条件都可能退化到范围访问。

**要解决的问题**

表达“使用索引范围扫描”的访问方式。

**核心逻辑**

range 访问的关键难点是：

- 边界与参数有关
- 行数估算更难
- 可能需要 MRR/BKA 等后续优化

**代码场景**

- `test_quick_select()`
- `IndexRangeScanIterator`

#### `JT_ALL`

**背景**

不是所有查询都适合索引访问。  
当过滤不明显或代价更高时，全表扫描反而更优。

**要解决的问题**

表达“整张表扫描”的访问方式。

**核心逻辑**

`JT_ALL` 是最朴素的回退路径，也是很多复杂估算出错后的最终落点。

**代码场景**

- `TableScanIterator`
- EXPLAIN 中的 `ALL`

#### `HAVING`

**背景**

SQL 既需要过滤“原始行”，也需要过滤“聚合后的组结果”。  
如果只有 `WHERE`，就很难表达“统计完再筛掉不满足的组”。

**要解决的问题**

把“分组前过滤”和“分组后过滤”分开表达，并在计划里放到正确的位置。

**核心逻辑**

`HAVING` 的本质不是另一个写法奇怪的 `WHERE`，而是：

- 先聚合
- 再对聚合结果做条件判断

因此：

- `WHERE` 过滤输入行
- `HAVING` 过滤输出组

这也是为什么 `COUNT(*) >= 5` 这种条件不能简单下推到扫描阶段。

**代码场景**

- `Query_block::having_cond`
- aggregate 之后再接 `FILTER`

#### `SARGable`

**背景**

工程里经常会听到一句话：“这条条件不 SARGable。”  
它听起来像黑话，但其实是在描述一个非常实用的问题。

**要解决的问题**

判断某个谓词能不能被优化器直接翻译成：

- 等值查找
- 范围查找
- 其它索引可利用的搜索条件

**核心逻辑**

SARGable 可以粗略理解为：

> 这个条件是不是“Search ARGument able”，能不能变成索引搜索参数。

例如：

- `email = 'a@x.com'` 往往是 SARGable
- `email LIKE 'alice%'` 通常也是
- `LOWER(email) = 'a@x.com'` 往往不是

这个概念特别重要，因为它解释了很多“看起来只差一点点，计划却差很多”的 SQL。

**代码场景**

- `update_ref_and_keys()`
- range optimizer 抽取索引条件

#### `index merge`

**背景**

单表查询并不总是只能“选一个索引”。  
当 `OR` 或某些复杂条件出现时，多个索引结果有时可以先各自算，再合并。

**要解决的问题**

在单索引访问不够理想时，给优化器一条“多个索引结果先取并/交，再回表”的折中路径。

**核心逻辑**

index merge 的典型模式是：

- 分别做多个索引范围扫描
- 合并 rowid / 主键集合
- 去重或取交集
- 再取真实行

它不是银弹，因为：

- merge 本身有成本
- 回表也有成本

所以优化器必须把它和全表扫描、单索引扫描一起比较。

**代码场景**

- `sql/range_optimizer/`
- `EXPLAIN` 中的 `type=index_merge`

#### `loose index scan`

**背景**

有些 `GROUP BY`、`MIN/MAX` 查询，并不需要把每组的每一行都认真读完。  
如果索引顺序足够合适，优化器可以“跳着读”。

**要解决的问题**

减少为分组或最值问题读取的索引记录数。

**核心逻辑**

loose index scan 的直觉可以理解成：

- 我只需要每组里最关键的那几条索引记录
- 不需要把每组所有叶子项都完整扫完

它常出现于：

- `GROUP BY`
- `MIN/MAX`
- 输入顺序和分组键高度一致

但能不能触发，强依赖：

- 索引布局
- 查询形状
- 版本和优化规则

**代码场景**

- group by / range 优化路径
- `EXPLAIN` 中常见 `Using index for group-by`

#### `semijoin`

**背景**

`IN (subquery)`、`EXISTS (subquery)` 这类语义如果直接按嵌套子查询执行，通常效率很差。

**要解决的问题**

把某些子查询改写成更适合优化器搜索和执行器处理的连接形式。

**核心逻辑**

semijoin 的要点是：

- 我只关心“是否存在匹配”
- 不关心把内层所有匹配行都真正返回出来

这会引出：

- duplicate removal
- first match
- materialization 等策略

**代码场景**

- resolve/optimizer 期间的子查询转换
- `pull_out_semijoin_tables()`

#### `materialization`

**背景**

有些子查询、派生表、聚合结果，如果反复按需重算，代价很高。

**要解决的问题**

先把中间结果算出来并存起来，让外层反复消费。

**核心逻辑**

materialization 的本质是：

- 先算
- 先存
- 后复用

它常常和：

- derived table
- subquery
- temp table

放在一起出现。

**代码场景**

- `sql/sql_derived.cc`
- `AccessPath::MATERIALIZE`

#### `anti-join`

**背景**

`NOT EXISTS`、部分 `NOT IN` 场景本质上是在问：

- “有没有匹配行？”
- “如果没有，才保留外层行”

这和普通 inner join 的目标不一样。

**要解决的问题**

给“反存在判断”一个比逐行子查询更适合优化和执行的表达方式。

**核心逻辑**

anti-join 可以看成 semijoin 的“反面”：

- semijoin：有匹配就保留
- anti-join：有匹配就丢弃

它的关键价值在于：

- 一旦找到一条匹配，就能早停
- 不需要真的把所有匹配行都产出来

**代码场景**

- `NOT EXISTS` 相关子查询转换
- resolve/optimizer 中的半连接与反连接路径

#### `CTE`

**背景**

CTE = Common Table Expression。  
从写法上看，它像“给一个子查询起名字”。

**要解决的问题**

让复杂 SQL 更可读，同时给优化器一个可显式命名、可复用的中间查询块。

**核心逻辑**

理解 CTE 最实用的方式不是把它想得很神秘，而是先问：

- 它更像可 merge 的 derived table？
- 还是更像需要 materialize 的中间结果？

很多非递归 CTE 的优化逻辑，和 derived table 是共通的。

**代码场景**

- `Query_expression`
- `sql/sql_derived.cc`

#### `recursive CTE`

**背景**

一旦 CTE 允许引用自己，执行模型就不再是普通单次查询块。

**要解决的问题**

为层级遍历、图遍历、序列生成等问题提供 SQL 层的递归表达能力。

**核心逻辑**

recursive CTE 往往不是“选一个 join order”这么简单，而是：

1. 执行 anchor member
2. 维护 working table
3. 反复执行 recursive member
4. 直到不再产生新行

所以它和 materialization、temp table、迭代执行框架天然绑定。

**代码场景**

- recursive CTE 物化执行路径
- working table / temp table 管理

#### `filesort`

**背景**

MySQL 里“排序”历史上叫 filesort，但这不表示一定落磁盘文件。

**要解决的问题**

提供独立于存储引擎顺序能力的通用排序框架。

**核心逻辑**

只要现有访问路径不能天然满足 `ORDER BY`，优化器就可能引入 filesort。

名字虽然带 file，但它可能：

- 全在内存
- 部分落磁盘

取决于数据量和 sort buffer。

**代码场景**

- `sql/filesort.cc`
- `SortingIterator`

#### `temporary table`

**背景**

排序、聚合、去重、某些 derived table/materialization 场景都需要一个中间结果容器。

**要解决的问题**

为中间结果提供一个可写、可读、可复用的数据承载体。

**核心逻辑**

临时表不是某种特殊 SQL 语法，而是优化器/执行器为了完成计划而引入的执行时对象。

**代码场景**

- `sql/sql_tmp_table.cc`
- `TEMPTABLE_AGGREGATE`

#### `locking read`

**背景**

很多 SQL 不只是“读出来看看”，还要求：

- 读的时候就把相关记录锁住

典型就是 `FOR UPDATE`、`FOR SHARE`。

**要解决的问题**

把“读取哪些行”和“锁住哪些行”结合起来，同时尽量控制锁 footprint。

**核心逻辑**

locking read 的关键点不只是事务语义，也和访问路径直接相关：

- 唯一键点查，锁范围通常更小
- 范围扫、全表扫，锁影响面通常更大

所以它是连接“优化器”和“并发控制”的一个非常重要的概念。

**代码场景**

- locking select 执行路径
- `handler` 层加锁读接口

#### `NLJ`

**背景**

最经典、最普遍的 join 算法，就是外层一行一行驱动内层访问。

**要解决的问题**

用统一、简单的方式组合多表访问。

**核心逻辑**

Nested Loop Join 的本质是：

- 外层拿一行
- 用这行作为参数去驱动内层
- 内层再返回匹配行

它特别适合：

- ref/eq_ref
- 外层较小
- 内层有索引

**代码场景**

- `NestedLoopIterator`
- legacy optimizer 大量依赖它

#### `Hash Join`

**背景**

当索引不合适、连接条件适合哈希匹配时，NLJ 可能不是最优。

**要解决的问题**

为某些 join 形态提供比 NLJ 更合适的执行算法。

**核心逻辑**

- build side 建哈希表
- probe side 逐行探测

它更像现代优化器的标准组件，在 hypergraph 路径里更自然。

**代码场景**

- `sql/iterators/hash_join_iterator.h/.cc`
- `AccessPath::HASH_JOIN`

#### `BKA`

**背景**

普通 NLJ + ref lookup 可能产生大量随机 I/O。  
如果能把很多 key lookup 批起来做，局部性更好。

**要解决的问题**

降低大量索引点查带来的随机访问成本。

**核心逻辑**

BKA = Batched Key Access。  
它把一批 probe keys 聚在一起，再去内表做更高效的访问。

**代码场景**

- `sql/iterators/bka_iterator.h/.cc`

#### `MRR`

**背景**

范围或二级索引回表时，随机访问代价可能很高。

**要解决的问题**

把多个范围/rowid 访问批量化，改善访问顺序和局部性。

**核心逻辑**

MRR = Multi-Range Read。  
可以把它看成“批量范围读 / 批量回表优化”。

**代码场景**

- `handler` 能力接口
- `ref_row_iterators.cc`

#### `STRAIGHT_JOIN`

**背景**

大多数时候，我们希望优化器自由搜索 join order。  
但现实世界里总会有统计信息失真、极端数据分布、临时保底需求。

**要解决的问题**

给用户一个明确手段，限制或固定 join order，避免优化器选出已知坏计划。

**核心逻辑**

`STRAIGHT_JOIN` 可以粗略理解为：

- 按 SQL 书写顺序做 join

这会缩小搜索空间，也会减少优化器自由度。  
它既可能是救命绳，也可能让原本更优的计划失去机会。

所以它是一个非常适合讲“自动优化”和“人工干预”边界的术语。

**代码场景**

- parser/LEX 中的 straight join 标记
- join order 搜索阶段对搜索空间的约束

#### `Hypergraph Optimizer` 与 `DPhyp`

**背景**

legacy optimizer 的 join 搜索空间较有限，更偏贪心前缀搜索。  
当查询复杂度上升时，需要更系统的 join reorder 框架。

**要解决的问题**

在更大的 join 搜索空间中，系统性地枚举并比较更多候选计划。

**核心逻辑**

Hypergraph optimizer 的核心思想是：

- 用超图表示 join 关系
- 用 DPhyp 做连通子图枚举
- 自底向上合成完整最优计划

相比 legacy，它更像：

> 直接围绕 AccessPath 树做优化。

**代码场景**

- 入口：`FindBestQueryPlan()` in `sql/join_optimizer/join_optimizer.cc`
- 算法：`sql/join_optimizer/subgraph_enumeration.h`

### C.6 Prepared Statement 与计划缓存相关术语

#### `Prepared Statement (PS)`

**背景**

应用侧经常会重复执行同一条 SQL，只是参数不同。

**要解决的问题**

避免每次都重新 parse，并让参数绑定更加高效、安全。

**核心逻辑**

PS 会缓存：

- 解析树
- 参数位
- 部分语义绑定结果

但标准 MySQL 行为下：

- 仍然会在每次 execute 时重新 optimize

这正是 plan cache 出现的动机。

**代码场景**

- `sql/sql_prepare.h/.cc`
- `Prepared_statement::prepare()`
- `Prepared_statement::execute_loop()`

#### `reprepare`

**背景**

PS 可以跨多次执行复用解析树，但 DDL 可能让旧解析/旧绑定失效。

**要解决的问题**

当对象版本变化后，安全地重新解析并替换旧 statement 内部状态。

**核心逻辑**

当系统检测到：

- 表结构版本变化
- 旧语义绑定已不可信

就会抛出 `ER_NEED_REPREPARE`，上层再走 reprepare 流程。

**代码场景**

- `ER_NEED_REPREPARE`
- `Prepared_statement::execute_loop()`
- `swap_prepared_statement()` in `sql/sql_prepare.cc`

#### `plan template`

**背景**

在 MySQL 当前对象生命周期约束下，跨执行直接复用 `JOIN`/`TABLE`/`RowIterator` 风险很高。

**要解决的问题**

缓存最稳定、最小的一组计划元数据，而不是缓存整棵执行时对象。

**核心逻辑**

plan template 的思路是：

- 缓存稳定元数据
- 每次执行基于 fresh runtime context 快速重建 plan

这比“整棵计划对象跨执行复用”保守，但 correctness 风险更低。

**代码场景**

- `sql/ps_point_plan_cache.h/.cc`
- 当前 `ps_point_plan_cache` 的核心设计

#### `SPM`

**背景**

这是业界常见术语，不是 MySQL 当前内核已有能力。  
它常在 plan cache 演进讨论中出现。

**要解决的问题**

当自动选出的计划不稳定时，需要：

- 白名单
- 强制计划
- 演进/回滚

**核心逻辑**

SPM = SQL Plan Management。  
它属于“计划治理层”，不是简单的“计划热缓存层”。

**代码场景**

- 当前 MySQL 主线无对等能力
- 但在 plan cache 的中长期演进设计里，经常作为目标层出现
