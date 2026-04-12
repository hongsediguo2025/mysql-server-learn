# PS Point Plan Cache: `simple_ranges` 支持详细设计

## 1. 文档目的

本文给出 `ps_point_plan_cache` 支持 sysbench `simple_ranges` 的最终可实施设计，并以 **TDD 优先** 的方式组织内容：

1. 先定义 MTR 测试矩阵与验收标准。
2. 再倒推 classify、admission、fast path、runtime guard 的实现边界。
3. 所有 range MTR 绿灯后，才进入 benchmark 与 README 收口。

本文是一个 **独立新文档**，不覆盖已有的 `plan_cache_phase7_simple_ranges_implementation*.md` 历史稿。

## 2. 背景与范围

当前代码已经为 range 场景留下了结构预留，但链路尚未打通：

- `sql/ps_point_plan_cache.h` 中已经存在 `PsCachedPlanType::RANGE_PK_BETWEEN`
- 模板数组化已经完成，`params[]` / `field_indices[]` / `actual_types[]` 足以承载双参数 range
- 现有实现仍只支持 point lookup，`ps_point_plan_extract_where_shape()` 只接受 `field = ?` 或等值 AND
- 现有测试中，`BETWEEN` 仍被当作 `NEVER`

本轮只支持 sysbench `simple_ranges`，即：

```sql
SELECT c FROM sbtest%u WHERE id BETWEEN ? AND ?
```

这里的目标 workload 来自 sysbench `oltp_common.lua` 中的 `simple_ranges` 定义，而 **不是** 独立脚本 `select_random_ranges.lua`。两者的复杂度不同：

- `simple_ranges`: `id BETWEEN ? AND ?`，单表、单范围、主键区间
- `select_random_ranges`: `k BETWEEN ? AND ? OR ...`，二级列、多范围、OR 组合

### 2.1 本轮明确不做的内容

- `sum_ranges`
- `order_ranges`
- `distinct_ranges`
- `select_random_ranges.lua`
- `id > ?` / `id < ?` / `>=` / `<=` 的泛化 range 支持
- 二级索引 range
- 多 range / OR 合并 range
- JOIN、聚合、排序、LIMIT、子查询上的 range fast path

### 2.2 为什么本轮只做 `simple_ranges`

原因有三点：

1. `simple_ranges` 与当前 `ps_point_plan_cache` 的单表 PS fast path 架构最接近。
2. 它直接对应 sysbench 标准只读链路中的一条固定 SQL，投入产出明确。
3. 一旦把范围扩张到 `select_random_ranges`，实现就会立刻跨入多范围、二级索引、OR 合并、更多 range optimizer 细节，不适合 Phase 7 的最小闭环。

## 3. TDD 原则与开发顺序

本功能必须按测试先行推进。开发顺序固定如下：

1. 先写 `range_classify` MTR，再实现 classify。
2. 再写 `range_admission` MTR，再实现 admission。
3. 再写 `range_fast_path` MTR，再实现 fast path。
4. 再写 `range_edge` MTR，再补 runtime fallback、retryable demotion、reprepare。
5. 最后写 `range_cursor_proto` MTR，确认 cursor bypass 不回归。
6. 所有 range MTR 在默认模式与 `--ps-protocol` 下通过后，再接 benchmark 与 README。

这里的关键原则是：

- 先把 **状态预期** 写成测试，再决定实现。
- 先把 **失败分流** 写清楚，再补代码。
- 不允许先实现一大段 range 逻辑，再靠人工跑 case 去“找测试”。

## 4. 设计约束

### 4.1 Classify 约束

classify 只接受以下形状：

```sql
SELECT <projection> FROM <single_base_table> WHERE <field> BETWEEN ? AND ?
```

具体要求：

- 单表
- 顶层单个 `BETWEEN`
- 参数顺序必须是 `field BETWEEN ? AND ?`
- `field` 必须属于 leaf table
- 语句仍必须满足现有 point fast path 的通用 gate

classify 阶段直接拒绝：

- `NOT BETWEEN`
- `>` / `>=` / `<` / `<=`
- `BETWEEN` 与其它条件通过 `AND` / `OR` 混合
- JOIN
- `GROUP BY`
- `ORDER BY`
- `DISTINCT`
- `LIMIT`
- 子查询
- 非 SELECT

### 4.2 Admission 约束

首次正常优化后，只放行最终优化为 **单范围 PK range scan** 的语句。

admission 只接受：

- `join->primary_tables == 1`
- `join->qep_tab != nullptr`
- `tab->type() == JT_RANGE`
- `tab->range_scan() != nullptr`
- `tab->range_scan()->type == AccessPath::INDEX_RANGE_SCAN`
- `used_index(tab->range_scan()) == table->s->primary_key`
- `get_used_key_parts(tab->range_scan()) == 1`
- `tab->range_scan()->index_range_scan().num_ranges == 1`
- 无 `HAVING`
- 无额外 residual filter

明确拒绝：

- 非唯一二级索引 range
- 唯一但非 PK 的 range
- 复合 PK 前缀 range
- 多 range
- 任何需要普通优化器继续处理额外过滤、排序、聚合的情况

### 4.3 Fast path 约束

hot path 必须：

- 直接构造 `JT_RANGE`
- 直接构造 `AccessPath::INDEX_RANGE_SCAN`
- 只构造一个 `QUICK_RANGE`
- 不重新调用 `test_quick_select()`
- 不走 `DynamicRangeIterator`
- 构造失败时完全回落到普通优化路径

## 5. 状态机与失败分流

本轮实现的关键不是“支持 range”本身，而是把失败路径分流清楚。

### 5.1 状态定义

- `NEVER`: 静态上不支持，或首次正常优化证明其不属于目标 plan 形状
- `COLD`: 静态形状可候选，但尚未成功 admission
- `HOT`: 已缓存 simple range 模板，后续可尝试 range fast path

### 5.2 三类失败的分界

#### A. Classify 失败

这些失败属于 **语句结构不支持**，直接进入 `NEVER`：

- `id > ?`
- `id BETWEEN ? AND ? AND k = ?`
- `ORDER BY`
- `JOIN`
- 聚合
- 子查询

#### B. Admission 失败

这些失败属于 **首次优化后的 plan 形状不支持**，首次执行后进入 `NEVER`：

- `BETWEEN` 语句最终走的不是 `JT_RANGE`
- range 不是 PK
- `num_ranges != 1`
- 需要 residual filter

#### C. Retryable 执行时失败

这些失败是 **参数值或环境变化导致本次不能走 fast path**，不应永久拉黑：

- 首次执行 low/high 为 `NULL`
- 首次执行 `low > high`
- HOT 状态下 low/high 为 `NULL`
- HOT 状态下 `low > high`
- 参数类型漂移
- optimizer_switch / sql_mode 漂移

其中状态预期写死为：

- 首次执行遇到 `NULL` 或 `low > high`: 保持 `COLD`，`admissions` 不增长，后续合法参数可重新尝试 admission
- HOT 状态遇到 `NULL` 或 `low > high`: 本次 `fallback_runtime +1`，`hits` 不增长，保留 `HOT`
- HOT 状态遇到参数类型/环境漂移: demote 到 `COLD`，后续合法执行可重新 admission

### 5.3 空结果的预期

“空结果”必须分两类处理：

1. **合法 bounds 但无匹配行**
   - 例如 `BETWEEN 900 AND 950`，表里没有对应行
   - 这是合法 simple range
   - 首次执行若计划形状满足要求，允许 admission
   - HOT 路径下返回空集是正确行为，不应视为失败

2. **非法 bounds，`low > high`**
   - 属于本次执行不可走 fast path 的 retryable 条件
   - 首次执行保持 `COLD`
   - HOT 执行时做 runtime fallback

## 6. MTR 用例总体设计

### 6.1 新增测试文件

建议新增以下测试文件，并对应新增 `.result`：

- `mysql-test/t/ps_point_plan_cache_range_classify.test`
- `mysql-test/t/ps_point_plan_cache_range_admission.test`
- `mysql-test/t/ps_point_plan_cache_range_fast_path.test`
- `mysql-test/t/ps_point_plan_cache_range_edge.test`
- `mysql-test/t/ps_point_plan_cache_range_cursor_proto.test`

这些文件均为新增独立 suite，不把大量 range case 塞进现有 point-query 测试文件。

### 6.2 回归脚本接线

`bench/ps_point_plan_cache/run_mtr_regression.sh` 需要同步更新：

- `MAIN_TESTS` 中加入：
  - `ps_point_plan_cache_range_classify`
  - `ps_point_plan_cache_range_admission`
  - `ps_point_plan_cache_range_fast_path`
  - `ps_point_plan_cache_range_edge`
- `CURSOR_TESTS` 中加入：
  - `ps_point_plan_cache_range_cursor_proto`

运行模式约束：

- `range_classify` / `range_admission` / `range_fast_path` / `range_edge`
  - 默认模式
  - `--ps-protocol`
- `range_cursor_proto`
  - `--ps-protocol --cursor-protocol`

## 7. 公共测试夹具设计

各 range 测试建议使用统一 schema 风格，便于状态对比。

### 7.1 基础表

```sql
CREATE TABLE t_range_pk (
  id INT PRIMARY KEY,
  k INT NOT NULL,
  c VARCHAR(64),
  pad CHAR(32)
);
```

用途：

- `id` 是目标 simple range 的主键
- `c` 对应 sysbench `SELECT c ...`
- `k` 用于构造非目标条件或对照

建议插入连续与非连续混合数据，覆盖：

- 单行区间
- 多行区间
- 合法但空结果区间

### 7.2 非目标表

```sql
CREATE TABLE t_range_nonuniq (
  id INT PRIMARY KEY,
  k INT NOT NULL,
  c VARCHAR(64),
  KEY idx_k (k)
);

CREATE TABLE t_range_uk (
  id INT PRIMARY KEY,
  uk INT NOT NULL,
  c VARCHAR(64),
  UNIQUE KEY idx_uk (uk)
);

CREATE TABLE t_range_comp (
  pk1 INT NOT NULL,
  pk2 INT NOT NULL,
  c VARCHAR(64),
  PRIMARY KEY (pk1, pk2)
);

CREATE TABLE t_range_aux (
  id INT PRIMARY KEY,
  range_id INT NOT NULL,
  memo VARCHAR(64)
);
```

用途：

- `t_range_nonuniq`: admission 失败，非唯一二级索引
- `t_range_uk`: admission 失败，唯一但非 PK
- `t_range_comp`: admission 失败，复合 PK 前缀
- `t_range_aux`: JOIN / 子查询负例

### 7.3 状态断言约定

所有 range MTR 使用与现有 point MTR 相同的断言方式：

- `Ps_point_plan_cache_cold_classifications`
- `Ps_point_plan_cache_admissions`
- `Ps_point_plan_cache_hits`
- `Ps_point_plan_cache_fallback_runtime`
- `Ps_point_plan_cache_invalidations`

每类测试都应同时检查：

- SQL 结果是否正确
- 对应 status counter 是否按预期变化

## 8. `range_classify` 详细设计

### 8.1 测试目标

只验证 PREPARE 阶段的静态分类，不触发 fast path。

### 8.2 正向场景

以下 case 期望 `cold_classifications +1`：

1. `SELECT * FROM t_range_pk WHERE id BETWEEN ? AND ?`
2. `SELECT id, c FROM t_range_pk WHERE id BETWEEN ? AND ?`
3. `SELECT * FROM t_range_pk AS a WHERE a.id BETWEEN ? AND ?`

### 8.3 负向场景

以下 case 期望 `cold_classifications` 不变：

1. `id > ?`
2. `id >= ?`
3. `id < ?`
4. `id <= ?`
5. `id NOT BETWEEN ? AND ?`
6. `? BETWEEN id AND ?`
7. `id BETWEEN 1 AND ?`
8. `id BETWEEN ? AND 10`
9. `id BETWEEN ? AND ? AND k = ?`
10. `id BETWEEN ? AND ? OR k = ?`
11. `SELECT ... ORDER BY c`
12. `SELECT ... LIMIT 1`
13. `SELECT SUM(k) ... BETWEEN`
14. `JOIN ... WHERE t1.id BETWEEN ? AND ?`
15. 子查询中使用 `BETWEEN`

### 8.4 设计要求

文档里必须明确：

- 上述负例全部在 classify 阶段拒绝
- 不把这些 case 留给 admission 再“兜底”
- `BETWEEN` 扩展不会改变 point query 的既有分类语义

## 9. `range_admission` 详细设计

### 9.1 测试目标

验证首次 EXECUTE 之后，range 候选是进入 `HOT` 还是被拒绝。

### 9.2 成功场景

case：

```sql
PREPARE stmt_ok FROM 'SELECT c FROM t_range_pk WHERE id BETWEEN ? AND ?';
SET @lo = 2;
SET @hi = 5;
EXECUTE stmt_ok USING @lo, @hi;
```

预期：

- `Ps_point_plan_cache_admissions +1`
- 第二次合法 EXECUTE 不应再次增长 `admissions`

### 9.3 失败场景

以下 case 首次 EXECUTE 后 `admissions` 不增长：

1. 非唯一二级索引：`SELECT c FROM t_range_nonuniq WHERE k BETWEEN ? AND ?`
2. 唯一但非 PK：`SELECT c FROM t_range_uk WHERE uk BETWEEN ? AND ?`
3. 复合 PK 前缀：`SELECT c FROM t_range_comp WHERE pk1 BETWEEN ? AND ?`
4. 首次 EXECUTE low 为 `NULL`
5. 首次 EXECUTE high 为 `NULL`
6. 首次 EXECUTE `low > high`

### 9.4 首次执行空结果的预期

case：

```sql
PREPARE stmt_empty FROM 'SELECT c FROM t_range_pk WHERE id BETWEEN ? AND ?';
SET @lo = 900;
SET @hi = 950;
EXECUTE stmt_empty USING @lo, @hi;
```

预期写死为：

- 若最终计划仍为目标 simple range 形状，则 **允许 admission**
- 空结果本身不是拒绝 admission 的理由

### 9.5 状态预期

文档中要明确写出：

- 非目标 plan 形状：首次执行后进入 `NEVER`
- `NULL` / `low > high`: 首次执行后保持 `COLD`
- 合法空结果：可进入 `HOT`

## 10. `range_fast_path` 详细设计

### 10.1 测试目标

验证 HOT 后的真实命中与结果正确性。

### 10.2 核心场景

1. 第一次执行 admission，第二次执行 `hits +1`
2. 单行区间返回正确值
3. 多行区间返回正确值
4. 合法空结果区间返回空集
5. 多次连续执行，`hits` 累积增长
6. DML 后再执行，结果必须反映最新数据

### 10.3 DML 可见性场景

建议至少覆盖：

- `INSERT` 新行后，range 可见新行
- `UPDATE` 已有行后，range 返回更新后的值
- `DELETE` 行后，range 返回空或减少一行

这里的设计目标是验证 range fast path 只缓存 plan，不缓存结果。

## 11. `range_edge` 详细设计

### 11.1 测试目标

覆盖 runtime guard、fallback、retryable demotion、reprepare。

### 11.2 必测场景

#### A. HOT + `NULL` 参数

- low 为 `NULL`
- high 为 `NULL`

预期：

- `Ps_point_plan_cache_fallback_runtime +1`
- `Ps_point_plan_cache_hits` 不增长
- 语句保持 `HOT`
- 下一次合法执行仍可 hit

#### B. HOT + `low > high`

预期：

- `fallback_runtime +1`
- `hits` 不增长
- 保持 `HOT`
- 下一次合法执行仍可 hit

#### C. 参数实际类型漂移

例如：

- 首次 admission 用整数
- 后续使用字符串或其它导致 `data_type_actual()` 漂移的值

预期：

- 本次执行走普通路径
- 触发 retryable demotion 到 `COLD`
- 下一次恢复合法类型后可重新 admission

#### D. `SET SESSION ps_point_plan_cache = OFF`

预期：

- fast path 被 bypass
- `hits` 不增长
- 恢复 `ON` 后可继续命中

#### E. DDL reprepare

场景：

- 先 admission
- 执行一次命中
- `ALTER TABLE`
- 下一次执行触发 reprepare
- 再次 admission
- 之后再次命中

预期：

- `admissions` 再增长
- `hits` 在 re-admission 后恢复增长

#### F. 环境漂移 guard

如果实现沿用现有 point fast path 的 guard，则各放 1 个触发例子：

- `optimizer_switch` 漂移
- `sql_mode` 漂移

预期：

- 触发 retryable demotion 到 `COLD`
- 后续重新 admission

### 11.3 设计要求

这份测试必须始终同时校验：

- counter 变化
- 结果正确性
- 下次执行是否还能继续命中或重新 admission

不能只验证“本次没崩”，必须验证后续状态机也正确。

## 12. `range_cursor_proto` 详细设计

### 12.1 测试目标

验证 simple range hot path 不破坏已有 cursor bypass 语义。

### 12.2 场景

1. 先把 simple range 跑到 `HOT`
2. 非 cursor 执行一次，`hits +1`
3. cursor 执行一次，`hits` 不增长
4. 再次非 cursor 执行，`hits` 恢复增长
5. fetch 结果正确，无 crash / assert

### 12.3 运行模式

这份测试只在：

- `--ps-protocol --cursor-protocol`

下运行。

## 13. 核心实现设计

## 13.1 `ps_point_plan_extract_where_shape()`

新增独立 helper，例如：

```cpp
static bool extract_between_field_params(Item_func *func,
                                         const Table_ref *tbl,
                                         PsPointPlanTemplate *tpl);
```

职责：

- 验证 `func->functype() == Item_func::BETWEEN`
- 验证参数顺序为 `field BETWEEN param AND param`
- 拒绝 `NOT BETWEEN`
- 把 low/high 分别写入 `tpl->params[0]` / `tpl->params[1]`
- `tpl->param_count = 2`
- `tpl->plan_type = PsCachedPlanType::RANGE_PK_BETWEEN`
- `tpl->field_indices[0] = field->field_index`

说明：

- `field_indices[1]` 不必重复记录同一列
- point query 原有逻辑保持不变

## 13.2 `ps_point_plan_can_admit()`

在现有 `POINT_EQ_REF` 分支旁新增 `RANGE_PK_BETWEEN` 分支。

range 分支检查：

- 单表
- `JT_RANGE`
- `AccessPath::INDEX_RANGE_SCAN`
- `PRIMARY KEY`
- `num_ranges == 1`
- `num_used_key_parts == 1`
- 无 `HAVING`
- 无 residual filter

只要首次优化结果不满足以上条件，就按非 retryable admission fail 处理，进入 `NEVER`。

## 13.3 `ps_point_plan_admit()`

本函数需要把首次优化得到的 range 元数据写入模板。

### 需要记录的元数据

最小集合如下：

- `plan_type = RANGE_PK_BETWEEN`
- `keyno`
- `key_parts = 1`
- `key_length`
- `best_read`
- `best_rowcount`
- `optimizer_switch`
- `table_ref_version`
- `relevant_sql_mode`
- low/high 两个参数的实际类型、unsigned 标记、collation 快照
- 单 range 所需的 endpoint 元数据：
  - `min_length`
  - `max_length`
  - `min_keypart_map`
  - `max_keypart_map`
  - `flag`
- 构造 `used_key_part` 所需的 keypart 布局元数据：
  - field index
  - `store_length`
  - `length`
  - `null_bit`
  - `flag`

注意：

- 本轮不缓存完整 `AccessPath`
- 本轮也不缓存 `QUICK_RANGE`
- 因为 range endpoint 依赖执行期参数值，每次 HOT 执行仍需按当前 low/high 重新序列化 key buffer

## 13.4 `ps_point_plan_build_fast_path()`

按 `plan_type` 分派：

- `POINT_EQ_REF`: 保持现状
- `RANGE_PK_BETWEEN`: 新增 `build_between_range_fast_path()`

range builder 需要：

1. 复用现有 structural guard
2. 检查 low/high 当前值是否可走 fast path
3. 在 `thd->mem_root` 上构造：
   - 当前执行的 min/max key buffer
   - 单个 `QUICK_RANGE`
   - 单元素 `QUICK_RANGE *` 数组
   - 单元素 `KEY_PART` 数组
   - `AccessPath::INDEX_RANGE_SCAN`
4. 最后统一写回：
   - `join->tables`
   - `join->primary_tables`
   - `join->const_tables`
   - `join->best_read`
   - `join->best_rowcount`
   - `join->qep_tab`
   - `join->set_root_access_path(path)`

### 为什么不走 `DynamicRangeIterator`

原因写死在文档里：

- `DynamicRangeIterator` 本质仍要在执行期重跑 `test_quick_select()`
- 这会把 simple range hot path 的主要收益重新丢回 range optimizer
- 本轮目标是复用“已 admission 的目标 plan 形状”，而不是再次求解 plan

## 13.5 Runtime guard 与 fallback

在现有 point fast path guard 基础上补 range 专属分支：

### 本次执行 fallback，但保留 HOT

- low 为 `NULL`
- high 为 `NULL`
- `low > high`

动作：

- `Ps_point_plan_cache_fallback_runtime +1`
- 返回 `false`，走普通路径
- 不 demote

### 本次执行 demote 到 `COLD`

- low/high 实际类型漂移
- unsigned 实际属性漂移
- collation 漂移
- `optimizer_switch` 漂移
- `sql_mode` 漂移

动作：

- demote 到 `COLD`
- `fallback_runtime +1`
- 本次走普通路径
- 后续允许重新 admission

## 14. Benchmark 与文档收口

只有在全部 range MTR 通过后，才进入 benchmark 收口。

### 14.1 新增 benchmark

建议新增：

- `bench/ps_point_plan_cache/sysbench_simple_ranges.sh`

基于 `oltp_read_only`，固定参数：

- `--point_selects=0`
- `--simple_ranges=1`
- `--sum_ranges=0`
- `--order_ranges=0`
- `--distinct_ranges=0`

### 14.2 负例 benchmark 保留

`sysbench_negative.sh` 中的：

- `select_random_ranges`

继续保留，作为“本轮不支持场景”的负向基线。

### 14.3 README 更新时机

README 与报告模板只在以下条件满足后更新：

- 新增 range MTR 全部通过
- 原有 point-query MTR 无回退
- simple_ranges benchmark 至少无回退

## 15. 分支覆盖矩阵

| 实现分支 | 说明 | 对应 MTR |
|----------|------|----------|
| classify 正例 | 顶层 PK `BETWEEN` 候选进入 `COLD` | `range_classify` |
| classify 负例 | `NOT BETWEEN` / 混合条件 / 排序 / JOIN / 聚合等 | `range_classify` |
| admission 成功 | 首次执行成为单范围 PK range scan | `range_admission` |
| admission 失败 | 非 PK / 非单范围 / 非目标 plan | `range_admission` |
| hot path 命中 | 第二次及以后命中 `hits` | `range_fast_path` |
| 合法空结果 | 合法 bounds 的空结果 | `range_fast_path` |
| runtime fallback | `NULL` / `low > high` | `range_edge` |
| retryable demote | 类型漂移 / 环境漂移 | `range_edge` |
| DDL reprepare | re-classify / re-admit / re-hit | `range_edge` |
| cursor bypass | range fast path 与 cursor 共存 | `range_cursor_proto` |

## 16. 验收标准

功能验收标准：

- 新增 range MTR 在默认模式和 `--ps-protocol` 下全部通过
- `range_cursor_proto` 在 `--ps-protocol --cursor-protocol` 下通过
- 原有 point-query MTR 不回退
- 首次执行失败路径的状态分流与本文一致
- HOT 路径下结果与普通路径一致

过程验收标准：

- 先有测试，再有实现
- benchmark 工作只在 range MTR 全绿后开始
- README / benchmark 脚本只在功能闭环后修改

## 17. 实施计划

本节给出推荐的执行阶段、每阶段交付物、以及进入下一阶段前必须满足的门禁条件。目标是让实现过程保持小步快跑，并避免把多个状态机分支混在一个大改动里。

### 17.1 阶段 1：Classify

目标：

- 让顶层 `field BETWEEN ? AND ?` 能在 PREPARE 阶段进入 `COLD`
- 保持所有非目标语句在 classify 阶段被稳定拒绝

交付物：

- `mysql-test/t/ps_point_plan_cache_range_classify.test`
- `mysql-test/r/ps_point_plan_cache_range_classify.result`
- `sql/ps_point_plan_cache.cc` 中的 `BETWEEN` shape helper

建议实现顺序：

1. 先写 `range_classify` 的正例与负例 MTR
2. 再实现 `extract_between_field_params()` 与 `ps_point_plan_extract_where_shape()` 接线
3. 最后修正文档中与 classify 相关的状态预期

阶段门禁：

- `range_classify` 默认模式通过
- 现有 `ps_point_plan_cache_classify*` 无回退
- `BETWEEN` 以外的 point query classify 行为不变

### 17.2 阶段 2：Admission

目标：

- 让 simple range 候选在首次正常优化后能正确分流到 `HOT` / `NEVER` / 保持 `COLD`

交付物：

- `mysql-test/t/ps_point_plan_cache_range_admission.test`
- `mysql-test/r/ps_point_plan_cache_range_admission.result`
- `ps_point_plan_can_admit()` 的 `RANGE_PK_BETWEEN` 分支
- `ps_point_plan_admit()` 的 range 模板写入

建议实现顺序：

1. 先写成功 case
2. 再写失败 case：非 PK、非唯一、复合前缀、首次 `NULL`、首次 `low > high`
3. 最后补“合法空结果仍允许 admission”的 case

阶段门禁：

- `range_admission` 默认模式通过
- 首次执行失败的 `NEVER/COLD` 分流与本文一致
- `admissions` 只在目标 simple range 上增长

### 17.3 阶段 3：Fast Path

目标：

- 让 `HOT` 的 simple range 在第二次及以后执行时真实命中 fast path

交付物：

- `mysql-test/t/ps_point_plan_cache_range_fast_path.test`
- `mysql-test/r/ps_point_plan_cache_range_fast_path.result`
- `PsPointPlanTemplate` 的最小 range 元数据扩展
- `ps_point_plan_build_fast_path()` 中的 `RANGE_PK_BETWEEN` builder

建议实现顺序：

1. 先写“第二次执行命中”的最小测试
2. 再写单行、多行、合法空结果
3. 最后写 DML 可见性

阶段门禁：

- `range_fast_path` 默认模式通过
- 第二次执行起 `hits` 增长
- 返回结果与普通路径一致
- 不引入新的 point-query 回退

### 17.4 阶段 4：Edge / Guard

目标：

- 覆盖所有 retryable failure、runtime fallback、demotion、reprepare 分支

交付物：

- `mysql-test/t/ps_point_plan_cache_range_edge.test`
- `mysql-test/r/ps_point_plan_cache_range_edge.result`
- range 专属 runtime fallback 与 demote 逻辑

建议实现顺序：

1. 先写 HOT + `NULL`
2. 再写 HOT + `low > high`
3. 再写类型漂移与环境漂移
4. 最后写 DDL reprepare 与 OFF/ON kill switch

阶段门禁：

- `range_edge` 默认模式通过
- 每个异常分支都同时验证结果、counter、后续状态机
- HOT 保留 / demote 到 COLD / 重新 admission 的行为与本文一致

### 17.5 阶段 5：Cursor 协议

目标：

- 确认 simple range fast path 不破坏已有 cursor bypass 语义

交付物：

- `mysql-test/t/ps_point_plan_cache_range_cursor_proto.test`
- `mysql-test/r/ps_point_plan_cache_range_cursor_proto.result`
- `bench/ps_point_plan_cache/run_mtr_regression.sh` 的接线更新

建议实现顺序：

1. 先把语句跑到 `HOT`
2. 再验证非 cursor hit
3. 再验证 cursor bypass
4. 最后验证恢复到非 cursor 后继续 hit

阶段门禁：

- `range_cursor_proto` 在 `--ps-protocol --cursor-protocol` 下通过
- 原有 `ps_point_plan_cache_cursor_proto` 不回退

### 17.6 阶段 6：全量回归与 Benchmark

目标：

- 在功能稳定后完成 bench 侧闭环和文档收口

交付物：

- `bench/ps_point_plan_cache/sysbench_simple_ranges.sh`
- `README.md`、`REPORT_TEMPLATE.md` 更新
- 全量 MTR / bench 结果记录

建议实现顺序：

1. 先跑新增 range MTR
2. 再跑原有 point MTR
3. 再跑 `run_mtr_regression.sh`
4. 全绿后才新增 benchmark 脚本与 README

阶段门禁：

- 新增 range MTR 全绿
- 原有 point-query MTR 无回退
- `simple_ranges` benchmark 至少无回退

### 17.7 推荐代码改动顺序

为降低冲突与回归风险，推荐按如下文件顺序提交改动：

1. `mysql-test/t/ps_point_plan_cache_range_classify.test`
2. `mysql-test/r/ps_point_plan_cache_range_classify.result`
3. `sql/ps_point_plan_cache.cc`
4. `sql/ps_point_plan_cache.h`
5. `mysql-test/t/ps_point_plan_cache_range_admission.test`
6. `mysql-test/r/ps_point_plan_cache_range_admission.result`
7. `mysql-test/t/ps_point_plan_cache_range_fast_path.test`
8. `mysql-test/r/ps_point_plan_cache_range_fast_path.result`
9. `mysql-test/t/ps_point_plan_cache_range_edge.test`
10. `mysql-test/r/ps_point_plan_cache_range_edge.result`
11. `mysql-test/t/ps_point_plan_cache_range_cursor_proto.test`
12. `mysql-test/r/ps_point_plan_cache_range_cursor_proto.result`
13. `bench/ps_point_plan_cache/run_mtr_regression.sh`
14. 最后再改 benchmark 与 README

### 17.8 推荐提交粒度

如果按多次提交推进，建议按以下粒度切分：

1. `range_classify` 测试 + classify 实现
2. `range_admission` 测试 + admission 实现
3. `range_fast_path` 测试 + fast path 实现
4. `range_edge` 测试 + fallback/demote/reprepare 实现
5. `range_cursor_proto` + 回归脚本接线
6. benchmark 与 README 收口

这样每一步都可以独立验证，也更便于回滚和 review。

## 18. 实施任务列表（可追踪、可回退）

本节把上面的阶段计划进一步压缩成“任务卡片”形式，目的是让实现工作可分配、可追踪、可单步回退。任务粒度以“一个主题提交”为单位，不允许跨任务混改。

### 18.1 任务总表

| ID | 任务 | 依赖 | 交付物 | 完成标准 | 回退点 |
|----|------|------|--------|----------|--------|
| T0 | 建立任务基线 | 无 | 任务看板、分支命名、提交粒度约定 | 明确每个任务的状态、负责人、提交范围、验收命令 | 不改代码，仅建跟踪项，无需回退 |
| T1 | `range_classify` MTR 草拟 | T0 | `ps_point_plan_cache_range_classify.test/.result` 初版 | 正反 case 全覆盖，预期状态与本文一致 | 仅回退新增测试文件 |
| T2 | Classify 实现 | T1 | `ps_point_plan_extract_where_shape()` 的 `BETWEEN` helper | `range_classify` 通过，原 `ps_point_plan_cache_classify*` 不回退 | 回退 classify 实现，保留测试 |
| T3 | `range_admission` MTR 草拟 | T2 | `ps_point_plan_cache_range_admission.test/.result` | 成功、失败、合法空结果、首次 `NULL/low>high` 预期齐全 | 仅回退新增测试文件 |
| T4 | Admission 实现 | T3 | `ps_point_plan_can_admit()` range 分支；`ps_point_plan_admit()` range 模板写入 | `range_admission` 通过；`admissions` 只在目标 simple range 增长 | 回退 admission 实现，保留测试 |
| T5 | `range_fast_path` MTR 草拟 | T4 | `ps_point_plan_cache_range_fast_path.test/.result` | 命中、单行、多行、空结果、DML 可见性场景齐全 | 仅回退新增测试文件 |
| T6 | Fast path 实现 | T5 | `PsPointPlanTemplate` 最小 range 元数据；`RANGE_PK_BETWEEN` builder | `range_fast_path` 通过；第二次执行起 `hits` 增长；结果正确 | 回退 fast path 与模板扩展，保留测试 |
| T7 | `range_edge` MTR 草拟 | T6 | `ps_point_plan_cache_range_edge.test/.result` | `NULL`、`low>high`、类型漂移、OFF/ON、DDL reprepare、环境漂移场景齐全 | 仅回退新增测试文件 |
| T8 | Edge/Guard 实现 | T7 | runtime fallback、retryable demote、re-admit 逻辑 | `range_edge` 通过；每个异常分支都验证结果、counter、后续状态 | 回退 edge 逻辑，保留测试 |
| T9 | `range_cursor_proto` MTR 草拟 | T8 | `ps_point_plan_cache_range_cursor_proto.test/.result` | HOT 后非 cursor hit、cursor bypass、恢复 hit 场景齐全 | 仅回退新增测试文件 |
| T10 | Cursor 接线与回归脚本更新 | T9 | `run_mtr_regression.sh` 接入全部 range tests | `range_cursor_proto` 在 `--ps-protocol --cursor-protocol` 下通过 | 回退脚本接线与 cursor 专项改动 |
| T11 | 全量回归 | T10 | 默认模式、`--ps-protocol`、cursor 模式回归记录 | 新增 range MTR 全绿，原 point-query MTR 无回退 | 回退到最后一个通过门禁的任务点 |
| T12 | Benchmark/README 收口 | T11 | `sysbench_simple_ranges.sh`、README、`REPORT_TEMPLATE.md` 更新 | `simple_ranges` benchmark 至少无回退，文档与实现一致 | 回退 bench/README 改动，不影响功能代码 |

### 18.2 任务状态与跟踪字段

每个任务状态固定为：

- `todo`
- `doing`
- `blocked`
- `review`
- `done`
- `rolled_back`

每个任务都必须记录以下字段：

- 目标文件
- 验证命令
- 通过日志位置或输出摘要
- 是否允许进入下一任务
- 对应提交或提交范围

推荐的任务跟踪载体：

- 现有设计文档中的状态表
- 外部 issue / 看板

本轮不要求新增仓库内的跟踪脚本或自动化管理文件。

### 18.3 提交边界

推荐按“测试 + 对应实现”成对推进：

- `T1 + T2`
- `T3 + T4`
- `T5 + T6`
- `T7 + T8`
- `T9 + T10`
- `T11 + T12`

每个任务对只允许一个主题，禁止把多个阶段的实现压进同一提交。

### 18.4 门禁规则

门禁失败时：

- 不允许继续后续任务
- 必须先回退到上一个 `done` 任务点
- 修复完成后重新执行当前任务的门禁命令

进入下一任务的前提：

- 当前任务状态为 `done`
- 对应门禁全部通过
- 没有留下未解释的 point-query 回退

## 19. 回退策略

回退单位以“任务对”为准，不做半回退。

### 19.1 测试先行任务失败

- 直接删除或回退对应新增测试文件
- 返回上一任务点
- 不保留错误的 `.result` 作为临时基线

### 19.2 实现任务失败

- 保留测试，回退实现代码到上一门禁通过点
- 重新修正实现，直到同一组测试通过
- 不通过修改测试去迎合错误实现

### 19.3 回归脚本失败

- 回退 `run_mtr_regression.sh` 接线
- 不回退已经通过门禁的核心功能

### 19.4 Benchmark/README 失败

- 只回退 benchmark 和文档改动
- 不影响 range 功能主线

### 19.5 推荐安全回退顺序

1. bench / README
2. 回归脚本与 cursor
3. edge / guard
4. fast path
5. admission
6. classify

## 20. 分阶段验证门禁

### 20.1 T2 门禁

- `ps_point_plan_cache_range_classify`
- `ps_point_plan_cache_classify`
- `ps_point_plan_cache_classify_ext`

### 20.2 T4 门禁

- `ps_point_plan_cache_range_admission`
- 原 `ps_point_plan_cache_admission*`

### 20.3 T6 门禁

- `ps_point_plan_cache_range_fast_path`
- 原 `ps_point_plan_cache_fast_path`

### 20.4 T8 门禁

- `ps_point_plan_cache_range_edge`
- 原 `ps_point_plan_cache_edge`

### 20.5 T10 门禁

- `ps_point_plan_cache_range_cursor_proto`
- 原 `ps_point_plan_cache_cursor_proto`

### 20.6 T11 门禁

- `bench/ps_point_plan_cache/run_mtr_regression.sh` 全套通过

## 21. 实施清单

按 TDD 顺序的实际落地清单如下：

1. 新增 `range_classify` 测试与结果文件
2. 实现 `BETWEEN` classify helper
3. 新增 `range_admission` 测试与结果文件
4. 实现 `RANGE_PK_BETWEEN` admission
5. 新增 `range_fast_path` 测试与结果文件
6. 实现 range fast path builder
7. 新增 `range_edge` 测试与结果文件
8. 补齐 runtime fallback / demote / reprepare
9. 新增 `range_cursor_proto` 测试与结果文件
10. 更新 `run_mtr_regression.sh`
11. 跑全量 MTR，确认 point-query 无回退
12. 再进入 `sysbench_simple_ranges.sh` 与 README 收口

## 22. 结论

`simple_ranges` 是 `ps_point_plan_cache` 扩展到 range 查询的最小闭环入口。它既复用了现有单表 PS fast path 的主体架构，又能在不引入多范围、排序、聚合复杂度的前提下建立完整的 classify -> admission -> fast path -> fallback 机制。

本轮成败的关键不在于“能否构造一个 range path”，而在于：

- 测试先行
- 首次执行失败与运行时失败严格分流
- 明确只支持 sysbench `simple_ranges`
- 不把更大范围的设计偷偷带进来

只有当这些原则全部通过 MTR 固化之后，后续的 `sum_ranges`、`order_ranges`、`distinct_ranges` 才有稳定的扩展基础。
