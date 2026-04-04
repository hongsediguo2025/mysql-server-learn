# PS Point Plan Cache V1 实现 Checklist 与阶段计划

## 1. 文档目的

本文把 `ps_point_plan_cache` v1 设计拆成：

- 按文件拆分的实现 checklist
- 伪 patch 级函数签名
- 分步骤和阶段实施计划

目标是让实现工作可以按清单直接推进，并且每个阶段都可单独编译、测试和回归。

## 2. 文件级实现 Checklist

### 2.1 `sql/sql_prepare.h`

- [ ] 为 `Prepared_statement` 新增状态枚举 `PsPointPlanState`
- [ ] 为 `Prepared_statement` 新增模板结构 `PsPointPlanTemplate`
- [ ] 新增成员：
  - [ ] `m_ps_pc_state`
  - [ ] `m_ps_pc`
  - [ ] `m_ps_pc_cursor_execution`
- [ ] 新增对外最小 helper：
  - [ ] `ps_point_plan_state() const`
  - [ ] `ps_point_plan_template()`
  - [ ] `reset_ps_point_plan_runtime_state()`
- [ ] 新增私有 helper 声明：
  - [ ] `classify_ps_point_plan_cache(THD *thd)`
  - [ ] `invalidate_ps_point_plan_cache()`

### 2.2 `sql/sql_prepare.cc`

- [ ] 在 `Prepared_statement::prepare()` 成功后加入静态分类
- [ ] 在 `Prepared_statement::execute()` 进入执行时记录 `open_cursor` 到 `m_ps_pc_cursor_execution`
- [ ] 在 `Prepared_statement::swap_prepared_statement()` 中交换新字段
- [ ] 在 `Prepared_statement::reprepare()` 后沿用新 classify 逻辑
- [ ] 在需要的位置清理 runtime-only 状态

### 2.3 `sql/sql_optimizer.h`

- [ ] 为 `JOIN` 增加 fast path helper 声明：
  - [ ] `bool try_apply_ps_point_plan_cache();`
  - [ ] `bool try_admit_ps_point_plan_cache();`
  - [ ] `bool build_qep_for_ps_point_plan_cache(const PsPointPlanTemplate &tpl);`
- [ ] 视需要增加最小辅助 helper 声明

### 2.4 `sql/sql_optimizer.cc`

- [ ] 在 `JOIN::optimize()` old optimizer 路径早期插入 fast path 尝试
- [ ] 在普通优化成功后插入 admission 尝试
- [ ] 实现运行期 guard
- [ ] 实现 invalidate / fallback 逻辑
- [ ] 不改变非候选路径的控制流和代价

### 2.5 `sql/sql_executor.cc`

- [ ] 新增一个专用 helper，把一表 `JT_EQ_REF` 的 `QEP_TAB` 转成 root access path
- [ ] 尽量复用 `QEP_TAB::access_path()`
- [ ] 不引入新的执行器分支，不直接下沉到 handler API

### 2.6 `sql/sql_executor.h`

- [ ] 若需要，为新 helper 增加声明

### 2.7 `sql/system_variables.h`

- [ ] 在 `System_variables` 中新增 `bool ps_point_plan_cache`
- [ ] 在 `System_status_var` 中新增：
  - [ ] `ps_point_plan_cache_hits`
  - [ ] `ps_point_plan_cache_admissions`
  - [ ] `ps_point_plan_cache_invalidations`
  - [ ] `ps_point_plan_cache_fallback_runtime`
- [ ] 更新 `LAST_STATUS_VAR`
- [ ] 确保新增状态变量仍位于连续可聚合区域内

### 2.8 `sql/sys_vars.cc`

- [ ] 增加 session 级 sysvar `ps_point_plan_cache`
- [ ] 默认值设为 `true`
- [ ] 不增加复杂子配置项，先保留单一 kill switch

### 2.9 `sql/mysqld.cc`

- [ ] 在 `status_vars[]` 中注册新增 status
- [ ] 命名统一为 `Ps_point_plan_cache_*`

### 2.10 `sql/CMakeLists.txt`

- [ ] 新增 `sql/ps_point_plan_cache.cc`
- [ ] 新增 `sql/ps_point_plan_cache.h`
- [ ] 接入 `sql_main` 编译

### 2.11 `sql/ps_point_plan_cache.h`

- [ ] 放候选抽取、guard、admission 的公共声明
- [ ] 定义与 `Prepared_statement` / `JOIN` 协作的最小接口
- [ ] 避免把过多逻辑散落在 `sql_prepare.cc` / `sql_optimizer.cc`

### 2.12 `sql/ps_point_plan_cache.cc`

- [ ] 实现静态分类 helper
- [ ] 实现 WHERE shape 提取（`field = ?`，单列或复合键）
- [ ] 实现 runtime guard helper
- [ ] 实现 key 结构验证 helper
- [ ] 实现 status 更新 helper

### 2.13 `mysql-test/t/*.test` 和 `mysql-test/r/*.result`

- [x] `ps_point_plan_cache_show_vars` — Phase 0 sysvar/status 可见性与会话隔离
- [x] `ps_point_plan_cache_binary_proto` — Phase 0 binary PS 协议路径惰性验证（含 Com_stmt_execute 自验证）
- [ ] `ps_point_plan_cache_point_select_hit`
- [ ] `ps_point_plan_cache_composite_unique_hit`
- [ ] `ps_point_plan_cache_noncandidate_bypass`
- [ ] `ps_point_plan_cache_sql_prepare_bypass`
- [ ] `ps_point_plan_cache_null_param_fallback`
- [ ] `ps_point_plan_cache_reprepare`
- [ ] `ps_point_plan_cache_cursor_bypass`
- [ ] `ps_point_plan_cache_hypergraph_bypass`

## 3. 伪 patch 级函数签名

下面给出建议的函数签名，供后续落代码时参考。

## 3.1 `sql/sql_prepare.h`

```cpp
enum class PsPointPlanState : uchar {
  NEVER = 0,
  COLD,
  HOT,
  INVALID
};

enum class PsCachedPlanType : uchar {
  POINT_EQ_REF = 0,   // Phase 1-6
  RANGE_PK_BETWEEN,    // Phase 7+
};

static constexpr uint PS_PC_MAX_KEY_PARTS = 4;
static constexpr uint PS_PC_MAX_PARAMS = 4;

struct PsPointPlanTemplate {
  Table_ref *table_ref;
  PsCachedPlanType plan_type;
  uint param_count;
  Item_param *params[PS_PC_MAX_PARAMS];
  uint field_indices[PS_PC_MAX_PARAMS];
  enum_field_types actual_types[PS_PC_MAX_PARAMS];
  bool unsigned_actuals[PS_PC_MAX_PARAMS];
  uint keyno;
  uint key_parts;
  uint key_length;
  key_part_map null_rejecting;
  double best_read;
  double best_rowcount;
};
```

```cpp
class Prepared_statement final {
  ...
 public:
  PsPointPlanState ps_point_plan_state() const { return m_ps_pc_state; }
  const PsPointPlanTemplate &ps_point_plan_template() const { return m_ps_pc; }
  PsPointPlanTemplate &ps_point_plan_template() { return m_ps_pc; }
  void invalidate_ps_point_plan_cache();
  void reset_ps_point_plan_runtime_state();

 private:
  bool classify_ps_point_plan_cache(THD *thd);

 private:
  PsPointPlanState m_ps_pc_state;
  PsPointPlanTemplate m_ps_pc;
  bool m_ps_pc_cursor_execution;
};
```

## 3.2 `sql/ps_point_plan_cache.h`

```cpp
bool PsPointPlanClassifyPreparedStatement(THD *thd, Prepared_statement *stmt);

bool ps_point_plan_extract_where_shape(Query_block *qb,
                                       PsPointPlanTemplate *tpl);

bool PsPointPlanRuntimeGuard(THD *thd, Prepared_statement *stmt,
                             TABLE **table_out, KEY **keyinfo_out);

bool PsPointPlanCanAdmitFromJoin(Prepared_statement *stmt, JOIN *join);

void PsPointPlanAdmitFromJoin(Prepared_statement *stmt, JOIN *join);

void PsPointPlanMarkHit(THD *thd);
void PsPointPlanMarkAdmission(THD *thd);
void PsPointPlanMarkInvalidation(THD *thd);
void PsPointPlanMarkRuntimeFallback(THD *thd);
```

## 3.3 `sql/sql_prepare.cc`

```cpp
bool Prepared_statement::classify_ps_point_plan_cache(THD *thd);

void Prepared_statement::invalidate_ps_point_plan_cache() {
  m_ps_pc_state = PsPointPlanState::INVALID;
  m_ps_pc = {};
}

void Prepared_statement::reset_ps_point_plan_runtime_state() {
  m_ps_pc_cursor_execution = false;
}
```

prepare 内伪 patch：

```cpp
if (!error) {
  error = prepare_query(thd);
  if (!error) classify_ps_point_plan_cache(thd);
}
```

execute 内伪 patch：

```cpp
m_ps_pc_cursor_execution = open_cursor;
auto runtime_guard = create_scope_guard([&]() { reset_ps_point_plan_runtime_state(); });
```

swap 内伪 patch：

```cpp
std::swap(m_ps_pc_state, copy->m_ps_pc_state);
std::swap(m_ps_pc, copy->m_ps_pc);
std::swap(m_ps_pc_cursor_execution, copy->m_ps_pc_cursor_execution);
```

## 3.4 `sql/sql_optimizer.h`

```cpp
class JOIN {
  ...
 private:
  bool try_apply_ps_point_plan_cache();
  bool try_admit_ps_point_plan_cache();
  bool build_qep_for_ps_point_plan_cache(const PsPointPlanTemplate &tpl);
  bool build_eq_ref_qep_tab_for_ps_point_plan_cache(const PsPointPlanTemplate &tpl);
  bool create_access_paths_for_ps_point_plan_cache();
};
```

## 3.5 `sql/sql_optimizer.cc`

`JOIN::optimize()` 伪 patch：

```cpp
if (!thd->lex->using_hypergraph_optimizer()) {
  if (try_apply_ps_point_plan_cache()) {
    set_plan_state(PLAN_READY);
    error = 0;
    DEBUG_SYNC(thd, "after_join_optimize");
    return false;
  }
}
```

普通优化成功后的 admission：

```cpp
if (!thd->lex->using_hypergraph_optimizer()) {
  if (try_admit_ps_point_plan_cache()) {
    // no-op; admission only warms template
  }
}
```

`try_apply_ps_point_plan_cache()` 伪签名：

```cpp
bool JOIN::try_apply_ps_point_plan_cache() {
  Sql_cmd *cmd = thd->lex->m_sql_cmd;
  Prepared_statement *stmt = cmd != nullptr ? cmd->owner() : nullptr;
  if (stmt == nullptr) return false;
  if (stmt->ps_point_plan_state() != PsPointPlanState::HOT) return false;
  if (stmt->is_sql_prepare()) return false;
  if (stmt->ps_point_plan_template().table_ref == nullptr) return false;
  if (stmt->ps_point_plan_template().param_count == 0) return false;
  if (stmt->ps_point_plan_template().params[0] == nullptr) return false;
  if (stmt->m_ps_pc_cursor_execution) return false;

  TABLE *table = nullptr;
  KEY *keyinfo = nullptr;
  if (!PsPointPlanRuntimeGuard(thd, stmt, &table, &keyinfo)) return false;

  if (build_qep_for_ps_point_plan_cache(stmt->ps_point_plan_template())) {
    PsPointPlanMarkRuntimeFallback(thd);
    return false;
  }

  PsPointPlanMarkHit(thd);
  return true;
}
```

`try_admit_ps_point_plan_cache()` 伪签名：

```cpp
bool JOIN::try_admit_ps_point_plan_cache() {
  Sql_cmd *cmd = thd->lex->m_sql_cmd;
  Prepared_statement *stmt = cmd != nullptr ? cmd->owner() : nullptr;
  if (stmt == nullptr) return false;
  if (stmt->ps_point_plan_state() != PsPointPlanState::COLD) return false;
  if (!PsPointPlanCanAdmitFromJoin(stmt, this)) {
    stmt->invalidate_ps_point_plan_cache();
    stmt->m_ps_pc_state = PsPointPlanState::NEVER;
    return false;
  }

  PsPointPlanAdmitFromJoin(stmt, this);
  stmt->m_ps_pc_state = PsPointPlanState::HOT;
  PsPointPlanMarkAdmission(thd);
  return true;
}
```

## 3.6 `sql/sql_executor.cc`

```cpp
bool JOIN::create_access_paths_for_ps_point_plan_cache() {
  assert(qep_tab != nullptr);
  assert(tables == 1);
  AccessPath *path = qep_tab[0].access_path();
  if (path == nullptr) return true;
  path = attach_access_paths_for_having_and_limit(path);
  m_root_access_path = path;
  return false;
}
```

`build_qep_for_ps_point_plan_cache()` 伪签名：

```cpp
bool JOIN::build_qep_for_ps_point_plan_cache(const PsPointPlanTemplate &tpl) {
  tables = primary_tables = 1;
  const_tables = 0;
  best_rowcount = tpl.best_rowcount;
  best_read = tpl.best_read;
  where_cond = nullptr;
  having_cond = nullptr;

  qep_tab = new (thd->mem_root) QEP_TAB[2];
  if (qep_tab == nullptr) return true;

  if (build_eq_ref_qep_tab_for_ps_point_plan_cache(tpl)) return true;
  if (create_access_paths_for_ps_point_plan_cache()) return true;
  return false;
}
```

`build_eq_ref_qep_tab_for_ps_point_plan_cache()` 伪签名：

```cpp
bool JOIN::build_eq_ref_qep_tab_for_ps_point_plan_cache(
    const PsPointPlanTemplate &tpl) {
  QEP_TAB *tab = &qep_tab[0];
  tab->set_join(this);
  tab->set_idx(0);
  tab->set_table(tpl.table_ref->table);
  tab->table_ref = tpl.table_ref;
  tab->set_type(JT_EQ_REF);

  if (init_ref(thd, tpl.key_parts, tpl.key_length, tpl.keyno, &tab->ref()))
    return true;

  KEY *keyinfo = &tab->table()->key_info[tpl.keyno];
  uchar *key_buff = tab->ref().key_buff;
  if (init_ref_part(thd, 0, tpl.params[0], /*cond_guard=*/nullptr,
                    tpl.null_rejecting != 0, const_table_map,
                    tpl.params[0]->used_tables(), keyinfo->key_part[0].null_bit,
                    &keyinfo->key_part[0], key_buff, &tab->ref()))
    return true;

  return false;
}
```

## 3.7 `sql/sys_vars.cc`

```cpp
static Sys_var_bool Sys_ps_point_plan_cache(
    "ps_point_plan_cache",
    "Enable v1 point-select plan fast path for prepared statements",
    SESSION_VAR(ps_point_plan_cache), CMD_LINE(OPT_ARG), DEFAULT(true));
```

## 3.8 `sql/mysqld.cc`

`status_vars[]` 伪 patch：

```cpp
{"Ps_point_plan_cache_hits",
 (char *)offsetof(System_status_var, ps_point_plan_cache_hits),
 SHOW_LONGLONG_STATUS, SHOW_SCOPE_ALL},
{"Ps_point_plan_cache_admissions",
 (char *)offsetof(System_status_var, ps_point_plan_cache_admissions),
 SHOW_LONGLONG_STATUS, SHOW_SCOPE_ALL},
{"Ps_point_plan_cache_invalidations",
 (char *)offsetof(System_status_var, ps_point_plan_cache_invalidations),
 SHOW_LONGLONG_STATUS, SHOW_SCOPE_ALL},
{"Ps_point_plan_cache_fallback_runtime",
 (char *)offsetof(System_status_var, ps_point_plan_cache_fallback_runtime),
 SHOW_LONGLONG_STATUS, SHOW_SCOPE_ALL},
```

## 4. 分步骤实施计划

## Phase 0: 文档与骨架

目标：

- 先把接口和文档定住
- 不改执行逻辑

步骤：

1. 新增设计文档
2. 新增 `ps_point_plan_cache.h/.cc`
3. 新增 `Prepared_statement` 字段和枚举
4. 新增 sysvar 和 status 骨架

验收：

- 能编译
- `SHOW VARIABLES LIKE 'ps_point_plan_cache'`
- `SHOW STATUS LIKE 'Ps_point_plan_cache%'`
- MTR `ps_point_plan_cache_show_vars` 通过（sysvar/status 可见性、会话隔离、DDL reprepare、多 PS 并行）
- MTR `ps_point_plan_cache_binary_proto` 通过（binary PS 路径惰性验证 + Com_stmt_execute 自验证）
- 所有 4 个 status 计数器均为 0（Phase 0 完全惰性）

## Phase 1: prepare 阶段静态分类

目标：

- 默认 ON 下，非候选语句在 prepare 阶段一次性打成 `NEVER`
- 执行期只剩一个状态判断

步骤：

1. 实现 `WHERE field = ?` 和复合唯一键等值 `AND(field1=?, field2=?, ...)` 的 shape 提取
2. 在 `Prepared_statement::prepare()` 成功后执行分类
3. 支持下列分类结果：
   - `NEVER`
   - `COLD`
4. 不引入任何 fast path

验收：

- `point_select` -> `COLD`
- 复合唯一键等值 -> `COLD`
- 非候选 select -> `NEVER`
- join/range/order/distinct/limit/SQL PREPARE/hypergraph -> `NEVER`

风险控制：

- 这一阶段不接管优化器，因此不会引入执行语义风险

## Phase 2: admission-only 预热

目标：

- 第一次执行后，仅记录模板，不改变执行路径

步骤：

1. 在普通 `JOIN::optimize()` 成功后检查是否满足 admission
2. 仅支持最严格的单表 `JT_EQ_REF`（单列或复合唯一键）
3. 满足则 `COLD -> HOT`
4. 不满足则 `COLD -> NEVER`

验收：

- `point_select` 首次执行后出现 admission 计数
- 复合唯一键等值首轮执行后可 admission
- 其他 shape 无 admission

风险控制：

- 这一阶段仍不改变执行路径，只做“观察 + 记录”

## Phase 3: fast path 接管

目标：

- `HOT` 语句在后续执行中跳过通用优化路径

步骤：

1. 在 `JOIN::optimize()` old optimizer 路径前半段加入 `try_apply_ps_point_plan_cache()`
2. 实现 runtime guard
3. 实现一表 `QEP_TAB + EQ_REF AccessPath` 的最小构造
4. 命中后直接设置 `PLAN_READY`

验收：

- `point_select` 第二次执行起命中
- 复合唯一键等值第二次执行起命中
- hit 计数增长
- 结果正确
- `EXPLAIN FOR CONNECTION` / 正常执行不崩溃

风险控制：

- guard 不通过时一律回退普通优化路径
- 只支持 unique-key equality lookup；性能 KPI 仍以 single-column point_select 为主

## Phase 4: reprepare / invalidation / fallback 打磨

目标：

- 把边界条件补齐，保证默认 ON 下的健壮性

步骤：

1. 参数 `NULL` / 类型变化 -> runtime fallback
2. key 结构变化 -> invalidate
3. DDL 后 reprepare 正常重新分类
4. cursor 场景 bypass
5. SQL PREPARE bypass

验收：

- 边界条件都有 mtr
- 回退后结果正确
- 无死循环、无重复 reprepare

## Phase 5: 性能回归与灰度收口

目标：

- 证明收益
- 确认默认 ON 下对非目标场景无显著回退

步骤：

1. 基准测试 `oltp_point_select.lua`
2. 补充 composite unique equality micro-benchmark
3. 基准测试 `oltp_read_only.lua`
4. 做线程矩阵
5. 做负向回归测试矩阵
6. 若必要，只调小 guard 范围，不扩大范围

验收：

- `oltp_point_select` 明显收益
- 复合唯一键等值补充基准不出现明显回退，有收益则记为加分项
- `oltp_read_only` 整体不引入回退
- 非 PS / 非候选 / join-heavy / range-heavy 无明显回退

## 5. 每阶段的提交建议

建议按以下提交粒度推进：

1. `phase-0-doc-and-skeleton`
2. `phase-1-prepare-classification`
3. `phase-2-admission-only`
4. `phase-3-fast-path`
5. `phase-4-invalidation-and-fallback`
6. `phase-5-tests-and-bench`

这样每一步都可独立 review、定位问题和回滚。

## 6. 建议的先后顺序

具体开发顺序建议如下：

1. 先做 sysvar/status 和文档骨架
2. 再做 `Prepared_statement` 分类
3. 再做 admission-only
4. 最后才上 fast path 接管
5. 接管完成后再补齐 invalidation 和性能测试

这样可以把最大风险放在最后一步，而且前两步就能先把“默认 ON、快速 bypass”这件事稳定下来。

## 7. 里程碑定义

### Milestone A

- 编译通过
- sysvar/status 可见
- prepare 分类可用

### Milestone B

- admission-only 可用
- 功能正确
- 无执行路径变化

### Milestone C

- fast path 命中
- point_select 收益可见

### Milestone D

- invalidation / reprepare / fallback 完整
- 默认 ON 下无明显负向回归

## 8. 最终交付标准

v1 交付标准建议定义为：

- 功能默认开启
- 非候选场景在 prepare 阶段即快速落到 `NEVER`
- 目标点查场景可稳定 `COLD -> HOT`
- `HOT` 场景可通过 fast path 跳过通用优化
- 任何风险场景都能快速 fallback
- reprepare 后行为正确
- sysbench 标准只读模型获得可复现收益
- 其他默认场景不出现明显性能回退

---

> **注：** 复合唯一键等值属于 V1 范围；V1 的性能 KPI 仍以 sysbench 单列 `point_select` 为主。
> PK range、聚合等扩展方向不属于 V1 范围。
> V1 的模板结构已做了数组化预留，但功能边界止于 Phase 0-5（单表唯一键等值点查）。
> 扩展方向的技术分析见 `ps_plan_cache_v2_deep_caching_analysis.md`。
