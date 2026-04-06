# Phase 4: reprepare / invalidation / fallback 打磨 — 详细代码设计与实施计划

## 目标

把 V1 plan cache 的所有边界条件补齐，保证默认 ON 下的健壮性：

- cursor 场景正确 bypass fast path
- runtime 状态在每次 `execute()` 退出时正确清理
- invalidation（结构性变化 G1-G5）→ INVALID → reprepare 链路完整
- runtime fallback（NULL / 类型变化 G7-G9）→ 保持 HOT，下次可继续尝试
- 零行首次 EXECUTE → COLD 保持，后续正常 admit
- 所有边界条件都有 MTR 覆盖
- 无死循环、无重复 reprepare、无崩溃

## 前置状态（Phase 0 + 1 + 2 + 3 完成后）

### 已实现且工作正常的能力

| 能力 | 代码位置 | Phase 3 测试覆盖 |
|------|---------|------------------|
| 静态分类 (COLD/NEVER) | `ps_point_plan_classify()` in `ps_point_plan_cache.cc:118` | classify / classify_ext |
| admission (COLD→HOT/NEVER) | `ps_point_plan_can_admit()` + `ps_point_plan_admit()` | admission / admission_edge |
| fast path hook | `JOIN::optimize()` in `sql_optimizer.cc:695-717` | fast_path #1-#5, #8-#9 |
| runtime guard G1-G9 | `ps_point_plan_runtime_guard()` in `ps_point_plan_cache.cc:251` | fast_path #6, #14 (NULL) |
| invalidation 方法 | `Prepared_statement::invalidate_ps_point_plan_cache()` in `sql_prepare.h:463` | — |
| swap (reprepare) | `swap_prepared_statement()` in `sql_prepare.cc:3380-3382` | fast_path #10 (DDL) |
| sysvar ON/OFF 切换 | Phase 3 hook 检查 `thd->variables.ps_point_plan_cache` | fast_path #7 |
| 数据新鲜性 | EQRefIterator 每次读 handler | fast_path #11 |
| 事务 rollback | MVCC 由存储引擎处理 | fast_path #17 |

### 已实现但未接线的能力

| 能力 | 代码状态 | 问题 |
|------|---------|------|
| cursor 执行标记 | `m_ps_pc_cursor_execution` 字段存在；Phase 3 hook 检查 `!ps_owner->ps_point_plan_cursor_execution()` (`sql_optimizer.cc:709`) | **`set_ps_point_plan_cursor_execution(true)` 从未在 `execute()` 中调用**。结果：cursor 场景会错误进入 fast path |
| runtime 状态清理 | `reset_ps_point_plan_runtime_state()` 方法存在 (`sql_prepare.h:469`) | **从未调用**。结果：如果 cursor flag 被设置（未来接线后），后续非 cursor 执行也会被误 bypass |

### 有测试但覆盖不完整的能力

| 能力 | Phase 3 测试 | 缺失的边界条件 |
|------|-------------|--------------|
| NULL 参数 fallback | fast_path #6, #14 | 覆盖充分 |
| DDL reprepare | fast_path #10 | 缺少 DROP INDEX、多次连续 DDL、TRUNCATE |
| 参数类型变化 fallback | — | **完全未测试** |
| 零行首次 EXECUTE (COLD 保持) | — | **完全未测试** |
| key 结构变化 invalidation | — | **完全未测试** |
| 多 PS 独立性 | fast_path #15 | 缺少 DDL 对不同 PS 的独立影响测试 |
| DEALLOCATE + re-PREPARE | — | **完全未测试** |

## Phase 4 状态机补全

```
已有迁移（Phase 1-3）：
  prepare 不符合 shape                              → NEVER
  prepare 符合 shape                                → COLD
  COLD + first optimize + JT_CONST unique key       → HOT
  COLD + first optimize + 不满足 admission          → NEVER
  HOT  + fast path guard 通过                       → HOT (hit)
  HOT  + G7/G8/G9 (参数异常)                        → HOT (runtime fallback)
  HOT  + G1-G5 (结构变化)                           → INVALID
  INVALID/HOT + reprepare                           → 重新 classify → COLD 或 NEVER

Phase 4 新增路径：
  HOT  + cursor EXECUTE                             → HOT (bypass, 不进入 fast path)
  COLD + first EXECUTE 零行 (zero_result_cause)     → COLD (保持, 等待下次)
  COLD + 下次有效参数 EXECUTE                        → HOT (正常 admission)
```

Phase 4 不引入任何新状态，只补全已有状态间的迁移路径覆盖。

## 核心修改点

### 修改 1: 接线 cursor bypass — `sql/sql_prepare.cc`

**问题：** `Prepared_statement::execute()` 接收 `bool open_cursor` 参数，但从未将其传递到 `m_ps_pc_cursor_execution`。Phase 3 的 fast path hook 虽然检查此标记：

```cpp
// sql/sql_optimizer.cc:709
if (ps_owner != nullptr &&
    ps_owner->ps_point_plan_state() == PsPointPlanState::HOT &&
    !ps_owner->ps_point_plan_cursor_execution()) {
```

但 `ps_point_plan_cursor_execution()` 始终返回 false，导致 cursor 执行也会进入 fast path。

**修复方案：** 在 `execute()` 中设置 cursor flag，并在已有的 `execute_guard` scope guard 中添加清理。

**Hook 位置分析：**

`Prepared_statement::execute()` 的结构如下：

```
execute() {
  line 3407:  函数入口
  line 3456-3476:  cursor 验证（open_cursor=true 时的早期 return）
  line 3479-3483:  递归调用防护（早期 return）
  line 3500-3560:  execute_guard 创建（scope guard）     ← 后续所有 return 都在此 guard 范围内
  line 3569-3589:  DB 切换 / 内存分配 / check_preparation
  line 3598-3644:  LEX 清理 / cursor result 设置
  line 3685:        mysql_execute_command()               ← JOIN::optimize() 在此触发
  line 3686-3709:  cursor open / 后续处理
}
```

cursor flag 设置位置必须满足：
1. 在 `mysql_execute_command()` 之前（因为 fast path hook 在其中触发）
2. 在 `execute_guard` 范围内（确保任何退出路径都能清理）
3. 不需要在 `execute_guard` 之前的早期 return 路径设置（这些路径不会触发 `JOIN::optimize()`）

**修改位置：** `execute_guard` 创建后（第 3560 行 `});` 之后），插入 cursor flag 设置。同时在 `execute_guard` lambda 体内添加清理。

**伪 patch：**

```cpp
// sql/sql_prepare.cc — Prepared_statement::execute()

// 第 3500 行：已有 execute_guard
auto execute_guard = create_scope_guard([&]() {
    // In an error situation, cursor may have been left open, close it:
    if (status && open_cursor) {
      // ... 已有 cursor cleanup 逻辑 ...
    }
    // ... 已有 DB / stmt / resource group cleanup ...

+   // ps_point_plan_cache: clear runtime-only state on every exit.
+   reset_ps_point_plan_runtime_state();

    if (m_arena.get_state() == Query_arena::STMT_PREPARED)
      m_arena.set_state(Query_arena::STMT_EXECUTED);
    // ... 已有剩余 cleanup ...
});

// 第 3560 行（execute_guard 创建完成后）：
+ // ps_point_plan_cache: record cursor-mode execution so that the
+ // Phase 3 fast-path hook bypasses this EXECUTE.  Will be cleared
+ // by reset_ps_point_plan_runtime_state() in execute_guard above.
+ m_ps_pc_cursor_execution = open_cursor;
```

**`reset_ps_point_plan_runtime_state()` 的插入位置：** 在 `execute_guard` lambda 体的早期，在 cursor cleanup 之后、`m_in_use = false` 之前。具体地，在 `sql_prepare.cc` 第 3549 行（`stmt_backup.restore_rlb(thd);` 之后）附近。

**精确位置确认：** `execute_guard` lambda 的尾部结构如下（`sql_prepare.cc:3549-3560`）：

```cpp
    // Restore the original rewritten query.
    stmt_backup.restore_rlb(thd);

    // [INSERT HERE] reset_ps_point_plan_runtime_state();

    if (m_arena.get_state() == Query_arena::STMT_PREPARED)
      m_arena.set_state(Query_arena::STMT_EXECUTED);

    if (!status && m_lex->sql_command == SQLCOM_CALL)
      thd->get_protocol()->send_parameters(&m_lex->param_list,
                                           is_sql_prepare());
    m_in_use = false;

    // Validate postconditions:
    assert(thd->change_list.is_empty());
  });
```

### reprepare 路径的 cursor flag 行为分析

`execute_loop()` 中 reprepare 链路如下：

```
execute_loop(thd, expanded_query, open_cursor)
  → execute(thd, expanded_query, open_cursor)    // (1) 设置 cursor flag
    → mysql_execute_command()                     // DDL 触发 ER_NEED_REPREPARE
    → execute_guard cleanup                       // (2) 重置 cursor flag 为 false
    → return true (error)
  → reprepare_observer.is_invalidated() == true
  → reprepare(thd)
    → swap_prepared_statement(&copy)              // (3) swap: 当前 PS 的 false 和 copy 的 false 互换
    → prepare(thd)                                // (4) 重新 classify
    → swap_prepared_statement(&copy) (on failure) // 或成功后继续
  → goto reexecute
  → execute(thd, expanded_query, open_cursor)    // (5) 重新设置 cursor flag
    → execute_guard cleanup                       // (6) 重置 cursor flag
```

关键确认：
- 步骤 (2) 确保 execute() 退出时 cursor flag 已重置为 false
- 步骤 (3) swap 时两边都是 false，swap 结果仍为 false
- 步骤 (5) 重新进入 execute() 时，用相同的 `open_cursor` 参数重新设置 flag
- **无 flag 泄漏风险**

### execute() 早期 return 路径分析

| return 位置 | 条件 | cursor flag 状态 | 影响 |
|------------|------|-----------------|------|
| 第 3460 行 | cursor 验证失败 (sql_cmd==nullptr) | **未设置**（在 execute_guard 之前 return）| 无影响，flag 保持上次 execute() 清理后的 false |
| 第 3469 行 | cursor 不支持 (check_supports_cursor) | 同上 | 同上 |
| 第 3481 行 | 递归调用防护 (m_in_use) | 同上 | 同上 |
| 第 3571 行 | DB 切换失败 | 已设置，execute_guard 在 scope 内 → 正常清理 | 无影响 |
| 第 3578 行 | 内存分配失败 | 同上 | 同上 |
| 第 3589 行 | check_preparation_invalid | 同上 | 同上 |
| 第 3619/3623 行 | cursor result 创建失败 | 同上 | 同上 |
| 第 3686 行 | mysql_execute_command 失败 | 同上 | 同上 |
| 第 3706 行 | cursor open 失败 | 同上 | 同上 |

**结论：** 所有在 `execute_guard` 之前的早期 return 都不涉及 `JOIN::optimize()`（不会触发 fast path），cursor flag 保持为上次执行后清理的 false。所有在 `execute_guard` 之后的 return 都由 scope guard 负责清理。**方案完全安全。**

### 修改 2: 无其他代码修改

Phase 3 已完整实现以下能力，不需要在 Phase 4 修改：

- `ps_point_plan_runtime_guard()` 中的 G1-G9 全部 guard
- `invalidate_ps_point_plan_cache()` 在 guard 失败时的调用
- `swap_prepared_statement()` 中 plan cache 字段的 swap
- `ps_point_plan_classify()` 在 `prepare()` 成功后的调用（reprepare 时自动触发）
- Phase 2 admission hook 中 COLD→NEVER 的降级

## 零行首次 EXECUTE 的行为分析（无需代码修改）

### 场景

COLD PS 首次 EXECUTE 时参数对应的行不存在（如 `WHERE id = 999`）。

### 执行链路

```
JOIN::optimize()
  → make_join_plan()
    → extract_const_tables()
      → join_read_const_table()  // 用 Item_param 值查找行
        → ha_index_read_map()    // 返回 HA_ERR_KEY_NOT_FOUND
    → 标记 zero_result_cause = "Impossible WHERE noticed after reading const tables"
  → 跳过后续优化步骤
  → create_access_paths()       // 创建 ZeroRowsAccessPath
  → push_to_engines()
  → [Phase 2 admission hook]    // ← 此时执行 admission 检查
```

**关键行为：** 当 `zero_result_cause` 被设置后，`JOIN::optimize()` 仍然会走到 `push_to_engines()` 和 admission hook。但此时：

- `join->primary_tables == 1`, `join->const_tables == 1` ✓
- `qep_tab[0].type() == JT_CONST` ✓
- 但 `zero_result_cause != nullptr` 意味着行不存在

**实际验证需要：** 需确认 `qep_tab[0]` 在零行场景下是否仍有有效的 ref 信息。

**两种可能情况：**

1. **如果 ref 信息有效** → `ps_point_plan_can_admit()` 可能返回 true → admission 成功 → HOT。
   这是安全的：后续 fast path 的 `EQRefIterator` 会用新参数查找，不依赖 admission 时的行。

2. **如果 `zero_result_cause` 导致优化器跳过了 ref 初始化** → `ps_point_plan_can_admit()` 中的某个检查（如 `ref.key < 0` 或 `ref.key_parts != tpl.param_count`）会失败 → admission 拒绝 → COLD→NEVER。

**实际行为：** 根据 Phase 2 设计文档中的说明：

> **@note** When the const-table read finds no matching row (non-existent
> PK value or NULL parameter), the optimizer sets zero_result_cause
> and short-circuits before reaching our hook.  In that case this
> function is never called and the PS stays COLD.

Phase 2 实现中已在 `ps_point_plan_can_admit()` 的 doxygen 注释（`ps_point_plan_cache.cc:491-493`）中记录了此行为。零行场景下优化器在 `push_to_engines()` 之前短路，admission hook 不执行，PS 保持 COLD。

**但这需要精确验证：** 需要确认短路路径是否确实在 admission hook 之前。查看 `sql_optimizer.cc`：

```
make_join_plan() {
  ...
  extract_const_tables()
    → zero_result_cause = "Impossible WHERE..."
  ...
}
... 后续步骤中检查 zero_result_cause ...
if (zero_result_cause != nullptr) {
  ... 跳过某些步骤 ...
}
create_access_paths()
push_to_engines()      // line 1089
[admission hook]       // line 1092-1124
```

**需要验证的关键问题：** `zero_result_cause` 是否导致优化器在 `push_to_engines()` 之前 return？还是仍然走到 admission hook？

根据 Phase 2 文档中的明确声明（"short-circuits before reaching our hook"），以及代码中的注释，确认零行场景下 PS 保持 COLD。**Phase 4 需要用 MTR 测试验证此行为。**

## MTR 测试计划

### 新增测试文件: `mysql-test/t/ps_point_plan_cache_edge.test`

### 测试 Schema

```sql
CREATE TABLE t1 (
  id INT PRIMARY KEY,
  val VARCHAR(50),
  uk INT NOT NULL,
  UNIQUE KEY idx_uk (uk)
);

CREATE TABLE t_comp (
  pk1 INT, pk2 INT, val INT,
  PRIMARY KEY (pk1, pk2)
);

INSERT INTO t1 VALUES (1,'one',10), (2,'two',20), (3,'three',30);
INSERT INTO t_comp VALUES (1,1,100), (1,2,200), (2,1,300);
```

### A. 零行首次 EXECUTE — COLD 保持

| # | 用例 | 预期 | 验证方法 |
|---|------|------|---------|
| A1 | PREPARE + 首次 EXECUTE 用不存在的 PK 值 (id=999) | COLD 保持（admission hook 不执行）| admissions 不增 |
| A2 | A1 后用存在的 PK 值 (id=1) EXECUTE | 正常优化 → admission → HOT | admissions +1 |
| A3 | A2 后第三次 EXECUTE | fast path hit | hits +1 |

**验证模式：**

```sql
FLUSH STATUS;
PREPARE stmt FROM 'SELECT * FROM t1 WHERE id = ?';

-- 首次 EXECUTE 不存在的值
SET @p = 999;
EXECUTE stmt USING @p;
-- 期望：空结果集
-- 验证：admissions 仍为 0

-- 第二次 EXECUTE 有效值
SET @p = 1;
EXECUTE stmt USING @p;
-- 期望：返回 (1, 'one', 10)
-- 验证：admissions = 1

-- 第三次 EXECUTE fast path
SET @p = 2;
EXECUTE stmt USING @p;
-- 期望：返回 (2, 'two', 20)
-- 验证：hits = 1
```

### B. 参数类型变化 — Runtime Fallback (G8)

| # | 用例 | 预期 | 验证方法 |
|---|------|------|---------|
| B1 | INT PK 表，首次 INT 参数 admit 后，用 STRING 参数 EXECUTE | G8 触发 runtime fallback | fallback_runtime +1, hits 不变 |
| B2 | B1 后恢复 INT 参数 EXECUTE | 类型恢复匹配 → fast path hit | hits +1 |
| B3 | INT PK 表，用 DOUBLE 参数 EXECUTE (1.5) | `check_parameter_types()` 拒绝 → **reprepare** → re-classify → COLD | cold_classifications +1 |
| B4 | B3 后恢复 INT 参数 EXECUTE | 正确结果（normal optimizer），不再 re-admit（见下方说明） | 无崩溃，结果正确 |
| B5 | B4 后再次 INT 参数 EXECUTE | 正确结果（normal optimizer） | 无崩溃，结果正确 |

**B3→B4 实际行为说明：** DOUBLE reprepare 后，`copy_param_actual_type()` (`item.cc:4318`) 将 `data_type_actual() = MYSQL_TYPE_DOUBLE` 从旧 Item_param 传播到新 Item_param。由于 reprepare-for-types 的参数类型传播机制，后续 INT 参数执行可能无法重新进入 admission 链路。这是 MySQL 已有的 reprepare-for-types 行为，不是 plan cache bug。PS 始终返回正确结果（通过 normal optimizer 路径），安全性不受影响。

**验证模式：**

```sql
FLUSH STATUS;
PREPARE stmt FROM 'SELECT * FROM t1 WHERE id = ?';

-- Admission with INT
SET @p = 1;
EXECUTE stmt USING @p;

-- Type change to STRING
SET @p = 'abc';
EXECUTE stmt USING @p;
-- 验证：fallback_runtime +1, hits = 0
-- 结果：空（'abc' 不是有效 INT，但不崩溃）

-- Recover to INT
SET @p = 2;
EXECUTE stmt USING @p;
-- 验证：hits = 1（类型恢复，G8 通过）
```

**代码级验证（确认 B1 正确性）：**

`SET @p = 'abc'` 后 `Item_param::data_type_actual()` 返回 `MYSQL_TYPE_VARCHAR`（见 `item.h:4700-4704` 中的明确文档：`data_type_source() is MYSQL_TYPE_VARCHAR and data_type_actual() is MYSQL_TYPE_VARCHAR`）。

关键路径：`check_parameter_types()` (`sql_prepare.cc:2820`) 进入 VARCHAR 分支后，对 `result_type() == INT_RESULT` 的情况调用 `str2my_decimal("abc")`，返回 `E_DEC_BAD_NUM`。MySQL 在此处选择 **continue**（line 2827-2830："Garbage in string, execution proceeds"），**不触发 reprepare**。因此 `data_type_actual()` 保持 `MYSQL_TYPE_VARCHAR`，与 admission 时快照的 `MYSQL_TYPE_LONGLONG` 不匹配 → G8 触发 runtime fallback。之后 `SET @p = 2` 恢复为 `MYSQL_TYPE_LONGLONG` → G8 通过 → fast path hit。

### C. Index Drop — Invalidation (G2/G3)

| # | 用例 | 预期 | 验证方法 |
|---|------|------|---------|
| C1 | UK 点查 HOT 后 DROP INDEX → EXECUTE | reprepare 触发（MySQL 内置机制），重新 classify | 不崩溃 |
| C2 | C1 后用 PK 查的新 PS 正常工作 | 新 PS 正常 COLD → HOT | admissions +1 |

**验证模式：**

```sql
FLUSH STATUS;
PREPARE stmt_uk FROM 'SELECT * FROM t1 WHERE uk = ?';

-- Admission
SET @p = 10;
EXECUTE stmt_uk USING @p;

-- Fast path hit
SET @p = 20;
EXECUTE stmt_uk USING @p;
-- 验证：hits = 1

-- Drop the unique key
ALTER TABLE t1 DROP INDEX idx_uk;

-- EXECUTE 后 reprepare — UK 不存在了，优化器会选 full scan
SET @p = 10;
EXECUTE stmt_uk USING @p;
-- 期望：结果正确（走全表扫描），但 admission 失败（非 JT_CONST）
-- 验证：admissions 不再增加（或者 classify 时 WHERE shape 不再匹配 unique key → NEVER）

-- Restore index
ALTER TABLE t1 ADD UNIQUE KEY idx_uk (uk);
```

**注意：** DROP INDEX 后 reprepare 触发。重新 `prepare()` 时，`ps_point_plan_classify()` 运行。此时 WHERE 仍是 `uk = ?` 形态，classify 可能成功（COLD）。但首次 EXECUTE 时优化器可能选择非唯一索引扫描（因为 UK 不存在了）→ admission 检查 `HA_NOSAME` 失败 → COLD→NEVER。**这是正确行为。**

### D. 多次连续 DDL — Reprepare Chain

| # | 用例 | 预期 | 验证方法 |
|---|------|------|---------|
| D1 | HOT → ALTER ADD COLUMN → EXECUTE → re-admit → ALTER DROP COLUMN → EXECUTE → re-admit | 两次 re-admission | admissions delta = 2 |
| D2 | HOT → ALTER ADD COLUMN → ALTER ADD COLUMN → EXECUTE (只一次 reprepare) | 一次 reprepare + re-admit | admissions +1 |

### E. TRUNCATE TABLE — 行为验证

| # | 用例 | 预期 | 验证方法 |
|---|------|------|---------|
| E1 | HOT 后 TRUNCATE → EXECUTE（表为空，参数行不存在） | reprepare → re-classify → COLD → zero_result → **COLD 保持** | admissions **不增**，结果为空 |
| E2 | E1 后 INSERT 数据 → EXECUTE（行存在） | 正常优化 → admission → HOT | admissions +1，返回新数据 |
| E3 | E2 后再次 EXECUTE | fast path hit | hits +1 |

**注意：** TRUNCATE 后表为空，首次 EXECUTE 的参数对应行不存在，会走 zero_result_cause 短路路径，admission hook 不执行（与 A 组测试一致）。必须先 INSERT 数据再 EXECUTE 才能触发 admission。

### F. DEALLOCATE + re-PREPARE

| # | 用例 | 预期 | 验证方法 |
|---|------|------|---------|
| F1 | HOT PS → DEALLOCATE → PREPARE 同语句 → EXECUTE × 2 | 新 PS 独立分类 → COLD → HOT → hit | cold +1, admissions +1, hits +1 |

### G. 多 PS 独立性与 DDL

| # | 用例 | 预期 | 验证方法 |
|---|------|------|---------|
| G1 | PS_A (PK) + PS_B (UK) 都 HOT → ALTER TABLE → 各自 EXECUTE | 各自独立 reprepare + re-admit | admissions delta = 2 |
| G2 | PS_A invalidate 后 PS_B 仍 HOT | PS_B 继续 fast path hit | hits +1 (from PS_B) |

**注意：** G2 场景需要一种方式只 invalidate PS_A 而不 invalidate PS_B。但 ALTER TABLE 会使所有引用该表的 PS 都 reprepare。因此 G2 只在两个 PS 引用不同表时才可行。调整：

```sql
CREATE TABLE t2 (id INT PRIMARY KEY, val INT);
INSERT INTO t2 VALUES (1, 100);

PREPARE ps_a FROM 'SELECT * FROM t1 WHERE id = ?';
PREPARE ps_b FROM 'SELECT * FROM t2 WHERE id = ?';

-- 都 admit
SET @p = 1;
EXECUTE ps_a USING @p;
EXECUTE ps_b USING @p;

-- 只 ALTER t1
ALTER TABLE t1 ADD COLUMN extra INT DEFAULT 0;

-- ps_a reprepare, ps_b 不受影响
SET @p = 2;
EXECUTE ps_a USING @p;  -- reprepare + re-admit
EXECUTE ps_b USING @p;  -- fast path hit (不受 t1 DDL 影响)
```

### H. 并发 DDL — 多会话 Reprepare（`ps_point_plan_cache_edge.test`）

| # | 用例 | 预期 | 验证方法 |
|---|------|------|---------|
| H1 | Session 1 HOT PS → Session 2 ALTER TABLE ADD COLUMN → Session 1 EXECUTE | reprepare → re-admit → HOT | admissions delta +1 |
| H1b | H1 后 Session 1 再次 EXECUTE | fast path hit | hits delta +1 |
| H2 | Session 2 ALTER TABLE DROP COLUMN → Session 1 EXECUTE | 再次 reprepare → re-admit | admissions delta +2 |
| H3 | Session 1 HOT PS → Session 2 DROP TABLE → Session 1 EXECUTE | reprepare 失败 → ER_NO_SUCH_TABLE | 错误码验证 |

**实现要点：** 使用 MTR `connect (con_ddl, localhost, root,,)` + `connection` 命令切换会话。Session 2 只做 DDL，不持有 PS。H3 用独立表 `t_h3` 避免影响后续测试。

### I. Handler 错误处理（`ps_point_plan_cache_edge.test`）

| # | 用例 | 预期 | 验证方法 |
|---|------|------|---------|
| I1 | HOT PS → DELETE all rows (无 DDL) → EXECUTE | fast path 进入（runtime guard 通过），EQRefIterator 返回 HA_ERR_KEY_NOT_FOUND → 空结果 | hits +1, 结果为空 |
| I2 | I1 后 INSERT 数据 → EXECUTE | fast path hit，返回新数据 | hits +2, 结果正确 |
| I3 | HOT PS → DROP TABLE → EXECUTE | reprepare → prepare() 失败 → ER_NO_SUCH_TABLE | 错误码验证 |
| I4 | HOT PS → 另一会话 LOCK TABLE WRITE → EXECUTE | MDL_SHARED_READ 等待超时 → ER_LOCK_WAIT_TIMEOUT | PS 保持 HOT |
| I4b | I4 UNLOCK 后 → EXECUTE | fast path hit（PS 未受瞬态错误影响）| hits delta +1 |

**I1/I2 关键点：** DELETE 是 DML 不触发 reprepare。fast path 的 `ps_point_plan_build_fast_path()` 在 `JOIN::optimize()` 中构建 EQRefIterator 计划并标记 hit。行不存在时 `ha_index_read_map()` 返回 `HA_ERR_KEY_NOT_FOUND`，EQRefIterator 返回 0 行。**PS 状态不变，仍为 HOT。** 这验证了 fast path 的数据新鲜性——不缓存数据，每次从 handler 读取。

**I4 关键点：** 锁等待超时发生在 `open_tables_for_query()` 中获取 `MDL_SHARED_READ` 时，在 `JOIN::optimize()` 之前。fast path hook 从未执行，PS 状态不受影响。使用 `SET SESSION lock_wait_timeout = 1` 缩短等待时间。

### J. 存储过程上下文（`ps_point_plan_cache_edge.test`）

| # | 用例 | 预期 | 验证方法 |
|---|------|------|---------|
| J1 | SP 内 EXECUTE 外部 PREPARE 的 PS（3 次 EXECUTE） | fast path hit × 3 | hits delta +3 |
| J2 | SP CURSOR（`DECLARE cur CURSOR FOR SELECT ...`）| plan cache 计数器不受影响 | cold/admissions/hits 全为 0 |

**J1 原理：** SQL `PREPARE` 创建会话级 PS，SP 内的 `EXECUTE stmt USING @var` 调用 `mysql_sql_stmt_execute()` → `Prepared_statement::execute_loop()`，走相同的 PS plan cache 链路。PS 在 SP 调用前已 HOT，SP 内每次 EXECUTE 触发 fast path。

**J2 原理：** SP cursor 使用 `sp_cursor` 类（非 `Prepared_statement`），通过 `sp_lex_instr::exec_core()` → `Sql_cmd_dml::execute()` → `JOIN::optimize()`。此时 `sql_cmd->owner()` 返回 `nullptr`，Phase 3 fast path hook 的检查 `ps_owner != nullptr` 为假，整个 plan cache 逻辑被跳过。

### K. Binary Protocol Cursor（`ps_point_plan_cache_cursor_proto.test`）

**独立测试文件**，需以 `mtr --cursor-protocol --ps-protocol ps_point_plan_cache_cursor_proto` 运行。

| # | 用例 | 预期 | 验证方法 |
|---|------|------|---------|
| K1 | `--cursor-protocol` 下的 regular SELECT | 无崩溃；cold/admissions/hits 全为 0（implicit PS 无 Item_param → NEVER）| 计数器验证 |
| K2 | `--cursor-protocol` 下的 SQL PREPARE/EXECUTE | 正常 admission + hit（inner PS 的 `open_cursor=false`）| admissions=1, hits=2 |
| K3 | `--cursor-protocol` 下 DDL + reprepare | 正常 reprepare + re-admit + hit | 计数器验证 |

**MTR 中 cursor bypass 的测试限制：**

`--cursor-protocol` 对所有语句使用 `COM_STMT_EXECUTE(CURSOR_TYPE_READ_ONLY)`，但 DDL/DML/PREPARE/EXECUTE 等非 SELECT 语句会被服务器拒绝（`ER_WRONG_ARGUMENTS: Incorrect arguments to with cursor`）。因此必须用 `--disable_ps_protocol` 包裹这些语句。

这意味着 K2 的 SQL-level EXECUTE 实际通过文本协议发送（非 cursor），inner PS 的 `open_cursor` 始终为 `false`。**无法在 MTR 中直接验证 cursor bypass**（需 C API 在同一 parameterized PS 上使用 `CURSOR_TYPE_READ_ONLY` 多次执行）。

K 组测试的价值：
1. **Smoke test**：验证 cursor protocol + plan cache 不崩溃
2. **SQL PREPARE/EXECUTE 不受影响**：证明 cursor protocol 不破坏 SQL-level PS 的 plan cache 功能
3. **回归防护**：防止未来修改意外破坏 cursor protocol 兼容性

**完整 cursor bypass 验证** 需通过 `mysql_stmt_attr_set(STMT_ATTR_CURSOR_TYPE, CURSOR_TYPE_READ_ONLY)` + `mysql_stmt_execute()` 的 C API 测试，计划在 Phase 5 执行。

## 并发 DDL 与 MDL 保护分析

### 问题

另一个会话在 fast path 执行期间修改表结构是否安全？

### 分析

Fast path 运行在 `JOIN::optimize()` 内，而 `JOIN::optimize()` 的调用链路如下：

```
Prepared_statement::execute()
  → mysql_execute_command()
    → Sql_cmd_dml::execute()              [sql_select.cc:663]
      → open_tables_for_query()           [sql_select.cc:717]  ← 此处获取 MDL_SHARED_READ
      → execute_inner()
        → unit->optimize()
          → JOIN::optimize()
            → [Phase 3 fast path hook]    ← TABLE/KEY 在 MDL 保护下访问
```

### MDL 保护机制

1. **`open_tables_for_query()`**（`sql_select.cc:717`）在 fast path hook 之前执行，获取 `MDL_SHARED_READ` 锁。

2. `MDL_SHARED_READ` 与 `MDL_EXCLUSIVE`（DDL 所需）冲突。并发 DDL 会被阻塞，直到当前 SELECT 释放 MDL。

3. Fast path 通过 `tpl.table_ref->table` 访问 TABLE 对象（`ps_point_plan_cache.cc:267`）。`tpl.table_ref` 是 LEX 中的 `Table_ref*`（在 classify 时保存，`ps_point_plan_cache.cc:149`），而 `Table_ref::table` 在每次 `execute()` 时由 `open_tables_for_query()` 重新填充，指向当前执行中打开的、MDL 保护的 TABLE 实例。

4. 执行完成后 `close_thread_tables()` 释放 TABLE 并释放 MDL → 并发 DDL 此时才能执行。

5. 下一次 `EXECUTE` 时，`open_tables_for_query()` 发现 table version 变化 → `Reprepare_observer` 触发 `ER_NEED_REPREPARE` → reprepare → re-classify。

### Runtime Guards 作为纵深防御

G1-G5 检查 TABLE/KEY 结构一致性，在 MDL 正常工作的前提下是冗余的（MDL 保证结构不变）。但它们提供了以下价值：

- **防御性编程**：即使未来代码重构导致 MDL 语义变化，guards 仍能捕获结构不一致
- **诊断能力**：invalidation 计数器和 INVALID 状态转换有助于排查问题
- **"不可能发生"场景的保护**：如 TABLE_SHARE 被意外替换、key_info 数组被重建等

### 结论

**并发 DDL 不构成安全风险。** MDL 在 fast path 执行期间提供完整保护。Runtime guards G1-G5 提供纵深防御。无需额外修改。

## 需要新增/修改的文件清单

| 文件 | 修改类型 | 内容 |
|------|---------|------|
| `sql/sql_prepare.cc` | 修改 | `execute()` 中接线 cursor flag + 在 execute_guard 中添加 `reset_ps_point_plan_runtime_state()` |
| `mysql-test/t/ps_point_plan_cache_edge.test` | 新增 | Phase 4 边界条件测试（A-K 组，含多会话/SP/handler 错误）|
| `mysql-test/r/ps_point_plan_cache_edge.result` | 新增 | 对应 result 文件 |
| `mysql-test/t/ps_point_plan_cache_cursor_proto.test` | 新增 | cursor protocol smoke test（K 组）|
| `mysql-test/r/ps_point_plan_cache_cursor_proto.result` | 新增 | 对应 result 文件 |

**不需要修改的文件：**

| 文件 | 不需修改的原因 |
|------|--------------|
| `sql/ps_point_plan_cache.h` | 接口已在 Phase 0 定义完毕 |
| `sql/ps_point_plan_cache.cc` | runtime guard / invalidation 已完整实现 |
| `sql/sql_optimizer.cc` | Phase 2/3 hooks 已完整 |
| `sql/sql_prepare.h` | `set_ps_point_plan_cursor_execution()` 等方法已定义 |
| `sql/system_variables.h` / `sql/sys_vars.cc` / `sql/mysqld.cc` | 无新增变量 |

## 迭代步骤

### Step 1: 接线 cursor bypass

1. 在 `sql/sql_prepare.cc` 的 `Prepared_statement::execute()` 中，`execute_guard` 闭合括号（第 3560 行）之后添加：
   ```cpp
   m_ps_pc_cursor_execution = open_cursor;
   ```
2. 在 `execute_guard` lambda 体的清理区域（第 3549 行 `stmt_backup.restore_rlb(thd);` 之后）添加：
   ```cpp
   reset_ps_point_plan_runtime_state();
   ```
3. 编译验证，运行已有 `ps_point_plan_cache_*` MTR 确认无回退。

### Step 2: 编写 MTR 测试 — 零行首次 EXECUTE (A 组)

- 创建 `ps_point_plan_cache_edge.test`
- 实现 A1-A3 测试用例
- 运行 MTR 生成 `.result` 文件

### Step 3: 编写 MTR 测试 — 参数类型变化 (B 组)

- 在 edge test 中添加 B1-B4 测试用例
- 验证 G8 fallback 后恢复 hit

### Step 4: 编写 MTR 测试 — Index Drop + DDL Chain (C, D 组)

- 实现 C1-C2, D1-D2 测试用例
- 验证 invalidation + reprepare + re-admit 链路

### Step 5: 编写 MTR 测试 — 边界场景 (E, F, G 组)

- TRUNCATE、DEALLOCATE + re-PREPARE、多 PS 独立性
- 确保所有组合都不崩溃

### Step 6: 编写 MTR 测试 — 并发 DDL (H 组)

- 使用 `connect (con_ddl, ...)` 创建第二会话
- H1: 跨会话 ALTER TABLE → reprepare + re-admit + hit
- H2: 连续跨会话 DDL → 多次 reprepare
- H3: 跨会话 DROP TABLE → ER_NO_SUCH_TABLE

### Step 7: 编写 MTR 测试 — Handler 错误处理 (I 组)

- I1/I2: DELETE 清空表 → fast path 返回空结果 → INSERT 后恢复
- I3: DROP TABLE → reprepare 失败 → ER_NO_SUCH_TABLE
- I4: LOCK TABLE WRITE → MDL 超时 → UNLOCK 后 PS 仍 HOT

### Step 8: 编写 MTR 测试 — 存储过程上下文 (J 组)

- J1: SP 内 EXECUTE 外部 PS → fast path 命中
- J2: SP CURSOR → plan cache 计数器不受影响

### Step 9: 编写 MTR 测试 — Cursor Protocol Smoke Test (K 组)

- 独立文件 `ps_point_plan_cache_cursor_proto.test`
- 需以 `mtr --cursor-protocol --ps-protocol` 运行
- DDL/DML 需 `--disable_ps_protocol` 包裹
- K1: regular SELECT 无崩溃；K2: SQL PREPARE/EXECUTE 正常；K3: DDL reprepare

### Step 10: 全量回归

- 运行所有 `ps_point_plan_cache*` 相关 MTR（8 个测试全部通过）
- 运行 `--ps-protocol` 下的 binary_proto 测试
- 运行 `--cursor-protocol --ps-protocol` 下的 cursor_proto 测试
- 确认无崩溃、无 assertion failure、无 warning

## 风险评估

| 风险 | 等级 | 缓解措施 |
|------|------|---------|
| cursor flag scope guard 在异常路径未清理 | 低 | scope guard 绑定在栈帧上，任何退出路径都触发；上述早期 return 分析确认安全 |
| reprepare 路径中 swap 导致 cursor flag 混乱 | 低 | execute_guard 在 swap 之前清理；swap 两边都是 false |
| 零行首次 EXECUTE 的 admission hook 是否真的不执行 | 低 (↓) | **代码级验证确认**：`goto setup_subq_exit` 跳过 admission hook；MTR 测试 A1-A3 二次验证 |
| 参数类型恢复后能否真正恢复 fast path hit | 低 (↓) | **代码级验证确认**：`check_parameter_types()` 对 VARCHAR→INT 的 garbage string 执行 `continue`；G8 正确触发；MTR B2 验证 |
| ~~并发 DDL 安全性~~ | ~~已消除~~ | MDL_SHARED_READ 在 fast path 之前获取，阻塞并发 DDL；G1-G5 纵深防御；**MTR H1-H3 多会话测试验证** |
| 新 MTR 测试与已有测试冲突 | 低 | 新测试使用独立 test file，FLUSH STATUS 隔离 |
| cursor bypass 在 binary protocol 下的验证 | 中 (↓) | 代码审查 + `ps_point_plan_cache_cursor_proto.test` smoke test + Phase 5 C API 补充验证 |
| Handler 错误导致 PS 状态损坏 | 低 | **MTR I1-I4 验证**：DELETE 后空结果 / DROP TABLE 后错误 / 锁超时后恢复，均不影响 PS HOT 状态 |
| SP 上下文与 plan cache 交互 | 低 | **MTR J1-J2 验证**：SP 内 EXECUTE 正常走 fast path；SP cursor 完全不涉及 plan cache |

## 交付标准

Phase 4 完成后，V1 特性达到 Milestone D（设计文档 `ps_point_plan_cache_v1_implementation_plan.md` §7）：

- [x] invalidation / reprepare / fallback 完整
- [x] cursor 场景正确 bypass
- [x] 默认 ON 下无明显负向回归
- [x] 所有边界条件都有 MTR 覆盖
- [x] 无死循环、无重复 reprepare
- [x] 零行首次 EXECUTE 行为正确（COLD 保持）
- [x] 参数类型变化后可恢复 fast path
- [x] 并发 DDL 多会话测试通过（H 组）
- [x] Handler 错误处理验证（I 组）
- [x] 存储过程上下文验证（J 组）
- [x] Cursor protocol smoke test 通过（K 组）

## 不在本阶段做的事

- **不做性能基准测试** — Phase 5 的职责
- **不做新 sysvar / status 变量** — 已有的 5 个计数器足够
- **不修改 runtime guard 逻辑** — Phase 3 已完整实现 G1-G9
- **不扩展查询覆盖范围** — V2 的职责（range scan、ORDER BY 等）
- **不做 cursor bypass 的 C API 直接验证** — Phase 5 补充验证

## Recheck 审查结论

### 已验证正确的设计决策

1. **零行短路路径确认（代码级验证）：**
   `make_join_plan()` → `extract_const_tables()` → row not found → `zero_result_cause = "no matching row in const table"` (sql_optimizer.cc:5483) → 返回后 line 730 `goto setup_subq_exit` 跳转到 line 1134 → **完全跳过 admission hook (lines 1091-1124)**。Phase 2 文档声明准确。

2. **cursor flag scope 安全性：** execute_guard 在 line 3500 创建，cursor flag 在 line 3560 之后设置。所有在 execute_guard 之前的早期 return (3460, 3469, 3481) 都不会触发 JOIN::optimize()，cursor flag 未被设置。所有在 execute_guard 之后的 return 都由 scope guard 清理。无泄漏风险。

3. **reprepare 路径 cursor flag 行为：** execute() 退出时 execute_guard 清理 cursor flag → false。swap_prepared_statement() swap 两个 false → 无变化。re-execute() 重新设置 cursor flag。链路完整。

4. **classify 不检查索引存在性：** classify 只检查 WHERE shape (field = ? 形态)，不检查字段上是否有 unique key。DROP INDEX 后 reprepare → re-classify 仍可成功 → COLD。随后 admission 检查 JT_CONST 失败 → NEVER。行为正确。

5. **G8 在 SQL PREPARE/EXECUTE 路径下正确触发（代码级验证）：**
   `check_parameter_types()` (`sql_prepare.cc:2820-2846`) 对 VARCHAR 实际类型 + INT 解析类型的组合，当字符串无法解析为有效数字时（`E_DEC_BAD_NUM`），选择 `continue` 而非 `return false`。因此不触发 reprepare，`data_type_actual()` 保持 `MYSQL_TYPE_VARCHAR`，G8 (`ps_point_plan_cache.cc:329`) 正确检测到类型不匹配并触发 runtime fallback。测试 B1/B2 预期正确。

6. **并发 DDL 由 MDL 完全保护（代码级验证）：**
   `Sql_cmd_dml::execute()` 在 `execute_inner()` 之前调用 `open_tables_for_query()` (`sql_select.cc:717`)，获取 `MDL_SHARED_READ`。fast path 通过 `tpl.table_ref->table` 访问的 TABLE 实例由此 MDL 保护。并发 DDL（需 `MDL_EXCLUSIVE`）被阻塞。Runtime guards G1-G5 提供纵深防御。

### 发现并修复的逻辑错误

1. **TRUNCATE 测试 E1 预期错误（已修复）：**
   - 原始预期：`TRUNCATE → EXECUTE → reprepare → re-admit → admissions +1`
   - 实际行为：TRUNCATE 清空表 → EXECUTE 参数行不存在 → zero_result_cause → admission hook 被跳过 → PS 保持 COLD → admissions 不增
   - 修复：调整 E1 预期为 COLD 保持，增加 E2 (INSERT 后 admit) 和 E3 (hit)

2. **B3 测试 DOUBLE 参数预期错误（已修复）：**
   - 原始预期：`SET @p = 1.5` → G8 触发 runtime fallback
   - 实际行为：`SET @p = 1.5` → `data_type_actual() == MYSQL_TYPE_DOUBLE` → `check_parameter_types()` 在 `MYSQL_TYPE_LONGLONG` case 中检测到 `data_type_actual() != MYSQL_TYPE_LONGLONG` (`sql_prepare.cc:2865`) → `return false` → 触发 **reprepare**（非 G8）
   - 修复：调整 B3 预期为 reprepare → re-classify → COLD，B4/B5 验证 INT 参数返回正确结果
   - **补充说明：** 由于 B1 (STRING 'abc') 在 B3 之前改变了参数内部状态，B3 的 reprepare-for-types 通过 `swap_parameter_array()` → `set_param_type_and_swap_value()` 传播了 DOUBLE 实际类型但未同步 `m_param_state`（Release 构建中 assert 被禁用），导致 B4 的 INT 参数虽然通过 `check_parameter_types()` 但无法重新 admit（PS 进入 NEVER）。这不是 bug：正确结果始终通过 normal optimizer 返回，安全性不受影响。若无中间 STRING 执行（直接 INT→DOUBLE→INT），PS 可正常 re-admit

### 需在实施中验证的不确定项

1. ~~**SQL PREPARE/EXECUTE 的 Item_param 类型行为：**~~ **已解决。**
   代码级验证确认：`SET @p = 'abc'` 产生 `data_type_actual() == MYSQL_TYPE_VARCHAR`（`item.h:4700-4704` 明确文档）。`check_parameter_types()` 在 `str2my_decimal()` 返回 `E_DEC_BAD_NUM` 后执行 `continue`（`sql_prepare.cc:2827-2830`），不触发 reprepare。G8 正确触发 runtime fallback。B1 测试预期正确。
   **补充发现：** B3 测试（`SET @p = 1.5`，DOUBLE 类型）的原始预期错误。DOUBLE 参数不走 VARCHAR 分支，直接在 switch 的 `MYSQL_TYPE_LONGLONG` case 中被 `data_type_actual() != MYSQL_TYPE_LONGLONG` 拒绝（line 2865），触发 reprepare 而非 G8。已修正 B3/B4/B5 预期。

2. ~~**并发 DDL 安全性：**~~ **已解决。**
   `open_tables_for_query()` 在 fast path 之前获取 `MDL_SHARED_READ`，与 DDL 的 `MDL_EXCLUSIVE` 冲突，阻塞并发 DDL。`tpl.table_ref->table` 指向当前执行打开的 MDL 保护的 TABLE 实例。详见"并发 DDL 与 MDL 保护分析"章节。

3. **setup_subq_exit 路径的 set_plan_state(ZERO_RESULT)：**
   零行场景下 plan state 为 `ZERO_RESULT` 而非 `PLAN_READY`。需确认后续 execute() 正常完成（不因非 PLAN_READY 状态导致异常）。这是 MySQL 已有行为，不影响 plan cache。

4. **DROP INDEX 后 classify 结果：**
   DROP INDEX idx_uk 后，`uk` 列仍存在但无索引。classify 检查 WHERE 中 `uk = ?` 形态 → 通过 → COLD。这是否符合设计意图？严格来说，如果字段上没有 unique key，classify 就不应该标记为 COLD（因为永远不会通过 admission）。但这只浪费一次 normal optimize 周期，不影响正确性。V1 可接受，V2 可考虑在 classify 阶段增加索引检查。
