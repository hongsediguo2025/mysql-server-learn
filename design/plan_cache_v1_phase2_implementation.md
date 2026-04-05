# Phase 2: admission-only 预热 — 实施计划

## 目标

COLD 语句在**第一次正常 `JOIN::optimize()` 成功后**，检查优化结果：
- 如果是严格的单表 `JT_CONST`（单列或复合唯一键），将 key 元信息写入模板，`COLD -> HOT`
- 如果不满足 admission 条件，`COLD -> NEVER`
- **仍不改变执行路径**——Phase 2 只做"观察 + 记录"

## 前置状态（Phase 0 + Phase 1 完成后）

Phase 1 分类已在 `prepare()` 阶段运行，COLD 语句的 `PsPointPlanTemplate` 包含：
- `table_ref`、`plan_type`、`param_count`、`params[]`、`field_indices[]`

以下字段仍为空值，由本阶段 admission 填充：
- `keyno`、`key_parts`、`key_length`
- `null_rejecting`
- `best_read`、`best_rowcount`
- `actual_types[]`、`unsigned_actuals[]`

相关 stub 函数已在 Phase 0 创建：
- `ps_point_plan_can_admit()` — `sql/ps_point_plan_cache.cc:183`
- `ps_point_plan_admit()` — `sql/ps_point_plan_cache.cc:188`
- status counter helper `ps_point_plan_mark_admission()` — `sql/ps_point_plan_cache.cc:196`

## 状态机迁移（Phase 2 新增路径）

```
COLD ──── first optimize 成功 + JT_CONST unique key ──── > HOT
  │
  └──── first optimize 成功 + 不满足 admission ──── > NEVER

HOT  ──── 后续执行（Phase 2 不做 fast path）──── > HOT（无变化）
NEVER ──── 后续执行 ──── > NEVER（永久 bypass）
```

已有迁移（Phase 1）：
- `prepare` 符合 shape → `COLD`
- `prepare` 不符合 shape → `NEVER`
- `reprepare` → 重新分类

## 迭代步骤

### Step 1: 实现 `ps_point_plan_can_admit()`
- 替换 stub，实现完整的 admission 条件检查
- 编译通过

### Step 2: 实现 `ps_point_plan_admit()`
- 替换 stub，保存 key 元信息到模板
- 重排 params 为 key-part 顺序
- 保存参数实际类型
- 设置 HOT + 递增 admissions counter

### Step 3: 挂载 admission hook 到 `JOIN::optimize()`
- 在 old optimizer 路径的 `push_to_engines()` 之后、`set_plan_state(PLAN_READY)` 之前插入
- 通过 `thd->lex->m_sql_cmd->owner()` 获取 `Prepared_statement*`

### Step 4: 新增 admission MTR 测试
- 编写 `ps_point_plan_cache_admission.test`
- 覆盖 PK/UK/复合键 admission 成功、各种 admission 失败场景

### Step 5: 更新已有 MTR 测试
- 更新 `ps_point_plan_cache_show_vars` 的 admissions 期望值和注释
- 验证 `ps_point_plan_cache_binary_proto` 无影响
- 验证 `ps_point_plan_cache_classify` / `classify_ext` 无影响

### Step 6: 编译验证 + 运行全部测试
- 检查新增 include 的必要性
- 编译相关编译单元，确认无 warning
- 运行全部 ps_point_plan_cache 相关 MTR

## 核心修改点

### 1. 实现 `ps_point_plan_can_admit()` — `sql/ps_point_plan_cache.cc`

替换当前 stub（第 183 行），实现完整的 admission 检查：

```cpp
bool ps_point_plan_can_admit(Prepared_statement *stmt, JOIN *join) {
  if (stmt->ps_point_plan_state() != PsPointPlanState::COLD) return false;

  const PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();

  // 单表点查在优化器中被识别为 JT_CONST（见下方说明）
  // primary_tables 包含 const tables；join->tables 还包含 tmp tables
  if (join->primary_tables != 1 || join->const_tables != 1)
    return false;

  if (join->qep_tab == nullptr) return false;

  const QEP_TAB *tab = &join->qep_tab[0];

  // 必须是 JT_CONST 访问方式（见下方说明）
  if (tab->type() != JT_CONST) return false;

  // 无 HAVING
  if (join->having_cond != nullptr) return false;

  TABLE *table = tab->table();
  if (table == nullptr) return false;

  const Index_lookup &ref = tab->ref();

  // 防御性检查：subquery 触发条件守卫（cond_guards 数组总是被分配，
  // 需要用 has_guarded_conds() 检查是否有非 nullptr 元素）
  if (ref.has_guarded_conds()) return false;

  // 防御性检查：子查询物化 hash key
  if (ref.keypart_hash != nullptr) return false;

  // ref key 编号有效
  if (ref.key < 0 || static_cast<uint>(ref.key) >= table->s->keys)
    return false;

  const KEY *keyinfo = &table->key_info[ref.key];

  // key 必须是唯一键（PK 或 UNIQUE KEY）
  if (!(actual_key_flags(keyinfo) & HA_NOSAME)) return false;

  // 所有用户定义 key part 都被 ref 覆盖
  if (keyinfo->user_defined_key_parts != ref.key_parts) return false;

  // ref key parts 数量必须等于模板的 param_count
  if (ref.key_parts != tpl.param_count) return false;

  // 每个 ref item 必须是模板 params[] 中的某个 Item_param。
  // 指针比较安全性：v1 作用域内 Item_param 不会被克隆（无 CTE、无
  // derived table 条件下推），且 create_ref_for_key() 通过 init_ref_part()
  // 直接存储 keyuse->val（对 PARAM_ITEM 不做 wrapping），因此
  // ref.items[i] 与 tpl.params[j] 指向同一对象。
  for (uint i = 0; i < ref.key_parts; i++) {
    if (ref.items[i] == nullptr ||
        ref.items[i]->type() != Item::PARAM_ITEM)
      return false;

    bool found = false;
    for (uint j = 0; j < tpl.param_count; j++) {
      if (tpl.params[j] == ref.items[i]) {
        found = true;
        break;
      }
    }
    if (!found) return false;
  }

  return true;
}
```

**关键发现：为什么是 JT_CONST 而非 JT_EQ_REF**

对于单表点查 `SELECT * FROM t1 WHERE pk = ?`，`Item_param::used_tables()` 返回
`INNER_TABLE_BIT`，该值使 `const_for_execution()` 返回 true。`extract_func_dependent_tables()`
中 `create_ref_for_key()` 为 const table 建立 ref access，`join_read_const_table()` 在
优化阶段读取行。结果：

- `join->primary_tables = 1`（包含 const table）
- `join->const_tables = 1`
- `join->tables = 2`（加上 make_tmp_tables_info 追加的 temp table）
- `join->join_tab = nullptr`（get_best_combination 后释放）
- `qep_tab[0]->type() = JT_CONST`

因此 admission 检查必须匹配 `JT_CONST`（而非最初设计假设的 `JT_EQ_REF`），
并通过 `qep_tab[0]`（而非 `join_tab`）访问计划元数据。const table 的 ref 信息由
`create_ref_for_key()` 填充并保持在 `QEP_shared` 中，与 JT_EQ_REF 场景结构一致。

**admission 条件逐项说明：**

| 条件 | 原因 |
|------|------|
| `state == COLD` | 只对首次执行的候选语句做 admission |
| `primary_tables == 1, const_tables == 1` | 唯一的 primary table 就是 const table |
| `qep_tab != nullptr` | 安全守卫，排除异常路径 |
| `type() == JT_CONST` | 单表唯一键点查在优化器中被识别为 const table |
| `having_cond == nullptr` | 安全守卫，v1 候选查询不含 HAVING |
| `!has_guarded_conds()` | 防御性：subquery 触发条件守卫不应存在于 v1 单表点查 |
| `keypart_hash == nullptr` | 防御性：子查询物化 hash key 不应存在于 v1 单表点查 |
| `actual_key_flags & HA_NOSAME` | key 必须是 unique 的 |
| `user_defined_key_parts == ref.key_parts` | 所有用户定义的 key part 都被 ref 使用 |
| `ref.key_parts == tpl.param_count` | key part 数量和模板 param 数量一致 |
| 每个 `ref.items[i]` 是模板 `params[]` 之一 | ref 使用的值正好是我们在分类阶段捕获的 `Item_param` |

**为什么用 `user_defined_key_parts` 而不是 `actual_key_parts`：**

InnoDB 对二级索引会追加隐藏的 PK 列，导致 `actual_key_parts > user_defined_key_parts`。例如：
- `UNIQUE KEY uk(val)` 在 InnoDB 上：`user_defined_key_parts = 1`，但 `actual_key_parts = 2`（val + 隐藏 pk）
- `ref.key_parts = 1`（只有 val 上的等值条件）

如果用 `actual_key_parts` 做比较，UK 点查的 admission 会被错误拒绝。

**为什么不再需要 `condition() == nullptr` 检查：**

JT_CONST 表在优化阶段已被完全解析（行已读取、WHERE 条件已评估）。如果 WHERE 中有
ref 无法覆盖的额外条件（如 `WHERE pk = ? AND val = ?`），优化器不会将表标记为
JT_CONST（因为 `ref.key_parts != tpl.param_count` 会拒绝），或条件已被 const
propagation 消除。因此无需单独检查残余条件。

### 2. 实现 `ps_point_plan_admit()` — `sql/ps_point_plan_cache.cc`

替换当前 stub（第 188 行），保存 key 元信息并转 HOT：

```cpp
void ps_point_plan_admit(THD *thd, Prepared_statement *stmt, JOIN *join) {
  PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();
  const QEP_TAB *tab = &join->qep_tab[0];
  const Index_lookup &ref = tab->ref();
  TABLE *table = tab->table();
  const KEY *keyinfo = &table->key_info[ref.key];

  // 保存 key 元信息
  tpl.keyno = static_cast<uint>(ref.key);
  tpl.key_parts = ref.key_parts;
  tpl.key_length = ref.key_length;
  tpl.null_rejecting = ref.null_rejecting;
  tpl.best_read = join->best_read;
  tpl.best_rowcount = static_cast<double>(join->best_rowcount);

  // 按 key-part 顺序重排 params[] 和 field_indices[]，
  // 同时保存运行时参数类型（Phase 3 guard 需要）。
  //
  // 背景：Phase 1 的 extract_where_shape() 按 WHERE 子句顺序存储
  // params[]/field_indices[]。但优化器的 ref.items[] 按 key part 顺序
  // 排列（由 create_ref_for_key() → calc_length_and_keyparts() 保证：
  // chosen_keyuses[i] 对应 keypart == i，init_ref_part(part_no, ...) 按序填入）。
  // Phase 3 的 init_ref_part() 需要按 key-part 顺序调用，
  // 因此在 admission 时将模板重排为 key-part 顺序。
  for (uint i = 0; i < ref.key_parts; i++) {
    Item_param *prm = down_cast<Item_param *>(ref.items[i]);
    tpl.params[i] = prm;
    tpl.field_indices[i] = keyinfo->key_part[i].fieldnr - 1;
    tpl.actual_types[i] = prm->data_type_actual();
    tpl.unsigned_actuals[i] = prm->is_unsigned_actual();
  }

  stmt->set_ps_point_plan_state(PsPointPlanState::HOT);
  ps_point_plan_mark_admission(thd);
}
```

**保存内容说明：**

| 字段 | 来源 | Phase 3 用途 |
|------|------|------|
| `keyno` | `ref.key` | `init_ref()` 的 keyno 参数 |
| `key_parts` | `ref.key_parts` | `init_ref()` 的 keyparts 参数 |
| `key_length` | `ref.key_length` | `init_ref()` 的 length 参数 |
| `null_rejecting` | `ref.null_rejecting` | 判断 NULL 参数是否需要 reject |
| `best_read` | `join->best_read` | 回填 fast-path `JOIN` 的代价估算 |
| `best_rowcount` | `join->best_rowcount` | 回填 fast-path `JOIN` 的行数估算 |
| `params[]` (重排) | `ref.items[]` | 按 key-part 顺序存储，`init_ref_part()` 可直接使用 |
| `field_indices[]` (重排) | `key_part[i].fieldnr - 1` | Phase 3 runtime guard 校验列位置 |
| `actual_types[]` | `param->data_type_actual()` | Phase 3 runtime guard 检查参数类型一致性 |
| `unsigned_actuals[]` | `param->is_unsigned_actual()` | Phase 3 runtime guard 检查有符号/无符号一致性 |

**关于 `fieldnr - 1` 的转换：**

`KEY_PART_INFO::fieldnr` 是 1-based 的（UNIREG 编号），而 `Item_field::field_index` 是 0-based 的（在 `TABLE::field[]` 数组中的位置）。因此 `fieldnr - 1` 产生与 Phase 1 中 `field_index` 一致的值。

**关于 `down_cast<Item_param *>`：**

安全性由 `ps_point_plan_can_admit()` 保证——它已验证每个 `ref.items[i]` 的 `type() == Item::PARAM_ITEM`。

### 3. 挂载 admission hook — `sql/sql_optimizer.cc`

**Hook 位置：** `JOIN::optimize()` old optimizer 路径中，`push_to_engines()` 成功后（第 1063 行）、`set_plan_state(PLAN_READY)` 前（第 1066 行）。

```cpp
  if (push_to_engines()) return true;

  // ps_point_plan_cache: attempt admission after first normal optimization.
  // COLD statements that produce a strict single-table JT_CONST plan
  // are promoted to HOT (template populated with key metadata).
  // Others are demoted to NEVER for permanent fast bypass.
  {
    Sql_cmd *sql_cmd = thd->lex->m_sql_cmd;
    Prepared_statement *ps_owner =
        (sql_cmd != nullptr) ? sql_cmd->owner() : nullptr;
    if (ps_owner != nullptr &&
        ps_owner->ps_point_plan_state() == PsPointPlanState::COLD) {
      if (ps_point_plan_can_admit(ps_owner, this)) {
        ps_point_plan_admit(thd, ps_owner, this);
      } else {
        ps_owner->set_ps_point_plan_state(PsPointPlanState::NEVER);
      }
    }
  }

  // Make plan visible for EXPLAIN
  set_plan_state(PLAN_READY);
```

**为什么选在 `push_to_engines()` 之后：**

- `make_join_plan()` 已完成：`best_ref`、join order、ref candidates 已确定
- `alloc_qep()` 已完成：`qep_tab` 已分配并从 `JOIN_TAB` 初始化
- `init_ref_access()` 已完成：`ref.key`、`ref.key_parts`、`ref.items[]` 已填充
- `make_join_readinfo()` 已完成：access type (`JT_CONST`) 已确定
- `finalize_table_conditions()` 已完成：残余条件已分配到 `qep_tab[0].condition()`
- `create_access_paths()` 已完成：`m_root_access_path` 已构建
- `push_to_engines()` 已完成：引擎级下推已完成

这是 legacy optimizer 正常路径的最后一步，plan 已完全定型。此时检查 plan 状态是最安全的。

**为什么不在 `make_join_plan()` 之后立即检查：**

`make_join_plan()` 之后，`qep_tab` 尚未分配、ref access 尚未初始化，无法检查 `JT_CONST` 等关键信息。

**通过 `thd->lex->m_sql_cmd->owner()` 获取 `Prepared_statement*`：**

- `Sql_cmd::owner()` 在 `Prepared_statement::prepare()` 中通过 `m_lex->m_sql_cmd->set_owner(this)` 绑定（`sql/sql_prepare.cc:2544`）
- 对于非 PS 执行（普通 SQL），`owner()` 返回 `nullptr`，admission 被跳过
- 对于 SQL `PREPARE/EXECUTE` 和 `COM_STMT_PREPARE/EXECUTE`，`owner()` 都返回正确的 PS

**index subquery shortcut 路径（第 868 行）不需要 hook：**

该路径仅对子查询生效，而 COLD 语句在 Phase 1 分类时已排除了子查询（`first_inner_query_expression() != nullptr` → NEVER）。

### 4. 需要新增的 include

**`sql/ps_point_plan_cache.cc`** 新增：
- `"sql/sql_select.h"` — `actual_key_flags()` 函数声明
- `"sql/sql_executor.h"` — `QEP_TAB` 类定义
- `"sql/sql_opt_exec_shared.h"` — `Index_lookup`、`join_type`、`JT_CONST`
- `"sql/key.h"` — `KEY` 结构体（`user_defined_key_parts`、`flags`、`key_part`）
- `"sql/table.h"` — `TABLE`、`TABLE_SHARE`
- `"sql/sql_optimizer.h"` — `JOIN` 类定义（`tables`、`primary_tables`、`qep_tab` 等）

**`sql/sql_optimizer.cc`** 新增：
- `"sql/ps_point_plan_cache.h"` — `PsPointPlanState`、`ps_point_plan_can_admit()`、`ps_point_plan_admit()`
- `"sql/sql_prepare.h"` — `Prepared_statement` 类定义

（`Sql_cmd::owner()` 已通过 `sql/sql_lex.h` → `sql/sql_cmd.h` 的传递 include 可用）

### 5. MTR 测试 — `mysql-test/t/ps_point_plan_cache_admission.test`

新增 MTR 测试，通过 `Ps_point_plan_cache_admissions` 和 `Ps_point_plan_cache_cold_classifications` 两个 status counter 验证 admission 行为。

**测试 schema：**

```sql
CREATE TABLE t1 (
  id INT PRIMARY KEY,
  val VARCHAR(50),
  uk INT NOT NULL,
  name VARCHAR(50),
  UNIQUE KEY idx_uk (uk),
  KEY idx_name (name)
);

CREATE TABLE t_comp (
  pk1 INT,
  pk2 INT,
  val INT,
  PRIMARY KEY (pk1, pk2)
);

CREATE TABLE t_comp_uk (
  id INT PRIMARY KEY,
  uk1 INT NOT NULL,
  uk2 INT NOT NULL,
  val INT,
  UNIQUE KEY idx_uk12 (uk1, uk2)
);
```

**测试矩阵：**

| 用例 | Phase 1 分类 | Phase 2 admission | 验证方法 |
|------|------|------|---------|
| `SELECT * FROM t1 WHERE id = ?` (PK) | COLD | 成功 → HOT | admissions +1 |
| `SELECT * FROM t1 WHERE uk = ?` (UK) | COLD | 成功 → HOT | admissions +1 |
| `SELECT * FROM t_comp WHERE pk1 = ? AND pk2 = ?` (复合 PK) | COLD | 成功 → HOT | admissions +1 |
| `SELECT * FROM t_comp_uk WHERE uk1 = ? AND uk2 = ?` (复合 UK) | COLD | 成功 → HOT | admissions +1 |
| `SELECT * FROM t_comp WHERE pk2 = ? AND pk1 = ?` (复合 PK，WHERE 顺序与 key 定义相反) | COLD | 成功 → HOT（params 重排为 key-part 顺序） | admissions +1 |
| `SELECT * FROM t_comp_uk WHERE uk2 = ? AND uk1 = ?` (复合 UK，WHERE 顺序与 key 定义相反) | COLD | 成功 → HOT（params 重排为 key-part 顺序） | admissions +1 |
| `SELECT * FROM t1 WHERE name = ?` (非唯一索引) | COLD | 失败（JT_REF 非 EQ_REF）→ NEVER | admissions 不变 |
| `SELECT * FROM t1 WHERE val = ?` (无索引列) | COLD | 失败（JT_ALL 全表扫描）→ NEVER | admissions 不变 |
| `SELECT * FROM t_comp WHERE pk1 = ?` (部分复合键) | COLD | 失败（`key_parts != param_count`）→ NEVER | admissions 不变 |
| `SELECT * FROM t1 WHERE id = ? AND val = ?` (PK + 非索引列) | COLD | 失败（`key_parts != param_count` 或有残余条件）→ NEVER | admissions 不变 |
| `SELECT * FROM t1 AS a WHERE a.id = ?` (表别名) | COLD | 成功 → HOT | admissions +1 |
| `SELECT id, val FROM t1 WHERE uk = ?` (非 SELECT *) | COLD | 成功 → HOT | admissions +1 |
| HOT 后二次执行 | — | 不重复 admission | admissions 不变 |
| HOT 后二次执行结果正确 | — | — | 查询结果正确 |

**辅助断言模式：**

```sql
FLUSH STATUS;

-- 验证 cold_classifications
--let $cold_before = query_get_value(SHOW STATUS LIKE 'Ps_point_plan_cache_cold_classifications', Value, 1)
PREPARE stmt FROM 'SELECT * FROM t1 WHERE id = ?';
--let $cold_after = query_get_value(SHOW STATUS LIKE 'Ps_point_plan_cache_cold_classifications', Value, 1)
--let $cold_delta = `SELECT $cold_after - $cold_before`
--echo cold_classifications delta after PREPARE: $cold_delta
-- 期望: 1

-- 验证 admissions
--let $adm_before = query_get_value(SHOW STATUS LIKE 'Ps_point_plan_cache_admissions', Value, 1)
SET @p = 1;
EXECUTE stmt USING @p;
--let $adm_after = query_get_value(SHOW STATUS LIKE 'Ps_point_plan_cache_admissions', Value, 1)
--let $adm_delta = `SELECT $adm_after - $adm_before`
--echo admissions delta after first EXECUTE: $adm_delta
-- 期望: 1

-- 验证不重复 admission
--let $adm_before2 = query_get_value(SHOW STATUS LIKE 'Ps_point_plan_cache_admissions', Value, 1)
EXECUTE stmt USING @p;
--let $adm_after2 = query_get_value(SHOW STATUS LIKE 'Ps_point_plan_cache_admissions', Value, 1)
--let $adm_delta2 = `SELECT $adm_after2 - $adm_before2`
--echo admissions delta after second EXECUTE: $adm_delta2
-- 期望: 0
```

### 6. 更新已有 MTR 测试 — `ps_point_plan_cache_show_vars`

**需要更新的位置：**

**Section 13 注释**（`mysql-test/t/ps_point_plan_cache_show_vars.test:283-288`）：
- 旧注释："Phase 2+ not yet active"
- 新注释：Phase 2 admission 已激活，admissions 预期为 9

**Section 13 预期结果**（`mysql-test/r/ps_point_plan_cache_show_vars.result:344`）：
- `Ps_point_plan_cache_admissions` 从 `0` 变为 `9`
- `Ps_point_plan_cache_cold_classifications` 保持 `10`
- 其余 counter 保持 `0`

**Admissions 计数 = 9 的推导：**

| PS | 查询 | COLD? | 首次 EXECUTE 行 | Admission 结果 |
|-----|------|-------|-------|------|
| ps_pk | `SELECT * FROM t1 WHERE id = ?` | 是 | 122 | 成功 (+1) |
| ps_uk | `SELECT * FROM t1 WHERE uk = ?` | 是 | 131 | 成功 (+1) |
| ps_multi | `SELECT * FROM t1 WHERE id = ? AND val = ?` | 是 | 186 | **失败**（PK 只覆盖 id，`ref.key_parts=1 != param_count=2`）→ NEVER |
| ps_repr | `SELECT * FROM t1 WHERE id = ?` | 是 | 216 | 成功 (+1) |
| ps_repr (reprepare #1) | 同上，ALTER 后 | 重新 COLD | 221 | 成功 (+1) |
| ps_repr (reprepare #2) | 同上，ALTER 后 | 重新 COLD | 226 | 成功 (+1) |
| ps_sql | `SELECT val FROM t1 WHERE id = ?` | 是 | 237 | 成功 (+1) |
| ps_a | `SELECT id FROM t1 WHERE id = ?` | 是 | 252 | 成功 (+1) |
| ps_b | `SELECT val FROM t1 WHERE uk = ?` | 是 | 254 | 成功 (+1) |
| ps_toggle | `SELECT * FROM t1 WHERE id = ?` | 是 | 272 | 成功 (+1) |
| **合计** | | | | **9** |

不 admission 的 PS（NEVER from Phase 1）：ps_join、ps_range、ps_agg、ps_sub、ps_ins、ps_upd、ps_del、ps_c。

**FLUSH STATUS 后的期望：**
- FLUSH STATUS 重置 session counters 为 0，因此第二次 `SHOW STATUS LIKE 'Ps_point_plan_cache%'` 所有 counter 均为 0（与 Phase 1 行为一致）。

**`ps_point_plan_cache_binary_proto` 无需修改：**
- 该测试在 `--ps-protocol` 下运行，但查询使用字面值而非 `?` 参数
- `m_param_count == 0` → Phase 1 分类返回 false → 状态留在 NEVER
- 所有 counter 仍为 0

**`ps_point_plan_cache_classify` / `classify_ext` 无需修改：**
- 这些测试只做 PREPARE，不做 EXECUTE，不会触发 admission

## 关键设计决策

### 为什么 admission 在 `push_to_engines()` 之后，不在更早位置

整个 old optimizer 路径的执行顺序是：

```
JOIN::optimize()
├── set_optimized()                    // 标记已优化
├── tables_list = leaf_tables          // 获取表列表
├── alloc_indirection_slices()         // 分配间接引用
├── make_join_plan()                   // ← join planning，确定 ref candidates
├── alloc_qep(tables)                  // ← 分配 QEP_TAB，从 JOIN_TAB 初始化
├── init_ref_access()                  // ← 初始化 ref lookup
├── make_join_readinfo()               // ← 确定 access type (JT_CONST 等)
├── finalize_table_conditions()        // ← 分配残余条件到 qep_tab
├── make_tmp_tables_info()             // 临时表决策
├── create_access_paths()              // 创建 AccessPath 树
├── push_to_engines()                  // 引擎级下推
├── 【admission hook 位置】            // ← plan 已完全定型
└── set_plan_state(PLAN_READY)         // 对外可见
```

在 `push_to_engines()` 之后，所有 plan 信息（`qep_tab`、`ref`、`type()`、`condition()`）均已最终确定，是检查 admission 条件的安全时机。

### 为什么 admission 失败直接 NEVER，不保留 COLD 重试

设计原则（V1 设计文档 §10.4）：
- 该语句的 shape 虽然静态看像候选，但真实优化结果不是 v1 目标路径
- 保留 COLD 只会在后续每次执行中重复尝试不必要的 admission 检查
- 直接 NEVER 保证后续执行的 O(1) bypass

**例外：** 如果优化本身失败（`JOIN::optimize()` return true），admission hook 不会执行，状态保持 COLD，下次执行会重试。这是合理行为——优化失败可能是暂时的。

### 为什么要重排 params 为 key-part 顺序

Phase 1 的 `extract_where_shape()` 按 WHERE 子句中出现的顺序存储 `params[]` 和 `field_indices[]`。例如：

```sql
-- 表定义: PRIMARY KEY (pk1, pk2)
-- WHERE pk2 = ? AND pk1 = ?
-- Phase 1 存储: params[0] = param_for_pk2, params[1] = param_for_pk1
```

但 Phase 3 的 fast path 需要按 key part 顺序调用 `init_ref_part()`：

```cpp
// key_part[0] → pk1, key_part[1] → pk2
init_ref_part(thd, 0, tpl.params[0], ...);  // 必须是 pk1 的 param
init_ref_part(thd, 1, tpl.params[1], ...);  // 必须是 pk2 的 param
```

因此在 admission 时一次性重排，后续 Phase 3 可以直接按序使用。

重排的方式：直接从 `ref.items[]` 取值（已按 key-part 顺序排列），覆盖 `tpl.params[]`。同时从 `key_part[i].fieldnr - 1` 重新计算 `field_indices[i]`。

### sysvar 关闭对 admission 的影响

Phase 1 分类检查 sysvar：
```cpp
if (!thd->variables.ps_point_plan_cache) return false;  // → NEVER
```

Phase 2 admission hook 检查 `thd->variables.ps_point_plan_cache`。如果用户在 PREPARE 后 EXECUTE 前关闭 sysvar，admission hook 被跳过，状态保持 COLD（不会 NEVER）。后续重新开启 sysvar 时，COLD 状态的 PS 可以正常 admit。

## 不在本阶段做的事

- **不做 fast path / 不接管执行路径**——Phase 3 的职责
- **不做 runtime guard**——Phase 3 的职责
- **不做 cursor bypass**——Phase 4 的职责（`m_ps_pc_cursor_execution` 仅在 fast path 时相关）
- **不做 invalidation / reprepare**——Phase 4 的职责
- **不修改 `sql/sql_optimizer.h`**——不需要在 `JOIN` 类上新增方法，admission 通过 inline 代码 + 自由函数实现
- **不新增 status counter**——复用已有的 `Ps_point_plan_cache_admissions`
- **不修改 `sql/sql_prepare.cc`**——不需要在 prepare/execute 链路上新增代码

## 注意事项

- **`ref.key` 类型为 `int`（不是 `uint`）：** 需要先检查 `ref.key >= 0` 再转为 `uint` 使用。
- **`join->best_rowcount` 类型为 `ha_rows`（即 `ulonglong`）：** 保存到模板时需要转为 `double`，与模板声明一致。
- **`const_tables == 1` 的原因：** `Item_param::used_tables()` 返回 `INNER_TABLE_BIT`，而 `const_for_execution()` 检查 `!(used_tables() & ~INNER_TABLE_BIT)`，对 `Item_param` 为 true。`extract_func_dependent_tables()` 据此将唯一键等值查找的表标记为 const（`JT_CONST`），在优化阶段读取行。因此 admission 检查 `const_tables == 1` 和 `primary_tables == 1`。注意 `found_const_table_map` 初始化包含 `INNER_TABLE_BIT`（`sql_optimizer.cc:173`），这是 `Item_param` 能通过 const ref 检查的前提。
- **`JT_CONST`（非 `JT_EQ_REF`）：** 对于 `WHERE unique_key = ?` 的 PS 查询，优化器通过 `extract_func_dependent_tables()` 将表标记为 `JT_CONST`，行在 `JOIN::optimize()` 阶段读取。计划元数据存储在 `qep_tab[0]`（`join_tab` 在 `get_best_combination()` 后释放）。Admission 检查 `tab->type() == JT_CONST`。
- **`keypart_hash` 非 nullptr 的情况：** 当 `keypart_hash != nullptr` 时，ref lookup 使用 hash 而非逐列比较。v1 不支持这种场景。`ps_point_plan_can_admit()` 中已加入 `ref.keypart_hash != nullptr` 防御性守卫。

## Review 核实：已排除的潜在风险

### Item_param 克隆机制与指针比较

**问题：** `Item_param` 有 `m_clones` 数组（`sql/item.h:4938`），用于 CTE reparse 和 derived table 条件下推场景（`sql/item.cc:3666-3698`）。如果 `ref.items[]` 包含克隆 param 而 `tpl.params[]` 包含原始 param，`ps_point_plan_can_admit()` 中的指针比较 `tpl.params[j] == ref.items[i]` 会失败。

**实际情况：** 克隆仅在以下两个场景创建：
1. CTE reparse（`lex->reparse_common_table_expr_at != 0`）— `sql/item.cc:3670`
2. Derived table 条件下推（`lex->reparse_derived_table_params_at` 非空）— `sql/item.cc:3686`

Phase 1 已排除了所有会触发克隆的场景：
- `unit->is_simple()` 排除了 UNION、CTE
- `first_inner_query_expression() != nullptr → NEVER` 排除了子查询和 derived table
- `leaf_table_count == 1` + `is_base_table()` 排除了复杂的表引用

对于 v1 支持的简单单表点查 `SELECT * FROM t WHERE pk = ?`，`Item_param::itemize()` 不会调用 `add_clone()`，`param_list` 中的 `Item_param*` 和 WHERE 树中的 `Item_param*` 是同一个对象，优化器的 `ref.items[i]` 也指向同一对象。指针比较是安全的。

**后续扩展注意：** 如果未来 v2+ 扩展到支持 derived table 或 CTE 场景，指针比较需要替换为 `pos_in_query` 匹配或 `m_clones` 检查。

### 表别名场景与指针比较

**问题：** 对于 `SELECT * FROM t1 AS alias WHERE alias.id = ?`，`Item_field::table_ref` 是否与 `qb->leaf_tables` 指向同一 `Table_ref` 对象？

**已验证：** 是同一对象。调用链：
1. Parser 创建一个 `Table_ref`（`alias = "alias"`），挂入 `Query_block::leaf_tables`
2. `open_tables_for_query()` 打开表，设置 `TABLE::pos_in_table_list = tl`（`sql/table.cc:4171`）
3. 名称解析时 `Item_field::set_field(field)` 设置 `table_ref = field->table->pos_in_table_list`（`sql/item.cc:2908`）

由于第 2 步和第 3 步引用同一个 `Table_ref` 对象，Phase 1 中 `fld->table_ref != tbl` 的指针比较对有别名的表也是正确的。测试矩阵中已补充表别名用例。

### VIEW 的处理

VIEW 在 Phase 1 分类时被 `tbl->is_base_table()` 排除（VIEW 不是 base table），直接归类为 NEVER。这是正确的保守行为——v1 不支持 VIEW 上的 plan cache。

### cond_guards 与 keypart_hash 防御性检查

**问题：** `Index_lookup` 包含 `cond_guards`（子查询触发条件守卫）和 `keypart_hash`（子查询物化 hash key）两个字段。如果它们在 v1 作用域内被激活，admission 后的模板可能与实际执行路径不匹配。

**分析：**

`cond_guards` 是 `bool**` 数组，由 `create_ref_for_key()` 在 `sql/sql_select.cc:2312` 总是分配（因此 `ref.cond_guards != nullptr` 对任何有效 ref 都为真，不能作为检查条件）。每个 `cond_guards[i]` 由 `init_ref_part()` 从 `Key_use::cond_guard` 设置（`sql/sql_select.cc:2328`）。`Key_use::cond_guard` 仅在 `add_key_fields()` 中为 `Item_func_trig_cond` 包裹的子查询等值条件设置为非 nullptr。

`keypart_hash` 是 `ulonglong*`，初始化为 `nullptr`（`sql/sql_opt_exec_shared.h:128`），仅在 `subselect_hash_sj_engine::setup()` 中被设置为非 nullptr（`sql/item_subselect.cc:3317`，当临时表使用 hash field 时）。

**v1 作用域内的状态：** 对于 v1 支持的单表点查（Phase 1 已排除子查询），`cond_guards[i]` 全部为 `nullptr`，`keypart_hash` 也为 `nullptr`。

**已采取措施：** 在 `ps_point_plan_can_admit()` 中加入两个防御性检查：
- `if (ref.has_guarded_conds()) return false;` — 使用 `Index_lookup::has_guarded_conds()` 遍历检查是否有非 nullptr 守卫
- `if (ref.keypart_hash != nullptr) return false;`

这两个检查在 v1 作用域内不会触发（因为 Phase 1 已排除相关场景），属于零成本的安全网。

### create_ref_for_key() 不会 wrap Item_param

**问题：** `ps_point_plan_can_admit()` 中使用指针比较 `tpl.params[j] == ref.items[i]` 验证 ref 使用的参数与模板中记录的 `Item_param` 是同一对象。如果优化器在构建 ref access 时对 `Item_param` 做了包装或替换，指针比较可能失败。

**已验证：** `create_ref_for_key()` 对每个 key part 的处理逻辑如下（`sql/sql_select.cc:2455-2471`）：
1. 如果 `keyuse->val->type() == FIELD_ITEM`，调用 `get_best_field()` 替换为更优的 `Item_field`
2. 调用 `init_ref_part(thd, part_no, keyuse->val, ...)`，其中 `ref->items[part_no] = val` 直接存储指针

对于 `PARAM_ITEM`，不满足 `type() == FIELD_ITEM` 的条件，不会经过 `get_best_field()`，也没有其他包装路径。`ref.items[i]` 就是 WHERE 条件中的原始 `Item_param*`。

### ref.items[] 的 key-part 顺序保证

**问题：** `ps_point_plan_admit()` 直接从 `ref.items[]` 按序取值覆盖 `tpl.params[]`，假设 `ref.items[i]` 对应 `keyinfo->key_part[i]`。如果此假设不成立，参数重排将出错。

**已验证调用链：**
1. `calc_length_and_keyparts()`（`sql/sql_select.cc:2265-2288`）：选择条件为 `keyparts == keyuse->keypart`，保证 `chosen_keyuses[i]` 对应第 `i` 个 key part
2. `create_ref_for_key()`（`sql/sql_select.cc:2455`）：`for (part_no = 0; part_no < keyparts; part_no++)` 从 `chosen_keyuses[part_no]` 取值
3. `init_ref_part(thd, part_no, keyuse->val, ...)`（`sql/sql_select.cc:2327`）：`ref->items[part_no] = val`

因此 `ref.items[i]` 严格对应 `keyinfo->key_part[i]`，顺序由 `create_ref_for_key()` 的循环结构保证。无需额外 assert，通过 MTR 测试（`WHERE pk2 = ? AND pk1 = ?` 反序用例）进行运行时验证。
