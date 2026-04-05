# Phase 1: prepare 阶段静态分类 — 实施计划

## 目标

在 `Prepared_statement::prepare()` 成功后、`m_lex->cleanup(true)` 前，对语句进行静态 shape 分类：
- 符合条件的 point-select → `COLD`
- 不符合条件 → `NEVER`（永久快速 bypass）
- **不引入任何 fast path / 不改变执行路径**

## 迭代步骤

### Step 1: 基础分类框架
- 实现 `ps_point_plan_classify()` 的所有静态条件检查（不含 WHERE shape）
- WHERE shape 暂时返回 false
- 挂载到 `prepare()` 中
- 编译通过

### Step 2: 单列等值 WHERE shape
- 实现 `ps_point_plan_extract_where_shape()` 的形态 A（`WHERE field = ?`）
- 验证 `SELECT * FROM t WHERE pk = ?` 走到 COLD

### Step 3: 复合键等值 WHERE shape
- 扩展 `ps_point_plan_extract_where_shape()` 支持形态 B（`WHERE f1 = ? AND f2 = ?`）
- 验证复合唯一键走到 COLD

### Step 4: 新增 status counter + MTR 测试
- 添加 `Ps_point_plan_cache_cold_classifications` counter
- 编写完整的 MTR 测试用例（分类正确性矩阵）
- 更新已有 `.result` 文件
- 运行全部 ps_point_plan_cache 相关测试

### Step 5: 代码 review + 编译验证
- 检查所有新增 include 的必要性
- 确认不影响已有测试
- 编译相关编译单元，确认无 warning

## 核心修改点

### 1. 实现 `ps_point_plan_classify()` — `sql/ps_point_plan_cache.cc`

将当前 stub 替换为真正的分类逻辑。分类条件按设计文档 §7.2 严格对齐：

```cpp
bool ps_point_plan_classify(THD *thd, Prepared_statement *stmt) {
  // 快速排除
  if (!thd->variables.ps_point_plan_cache) return false;
  if (stmt->m_param_count < 1 ||
      stmt->m_param_count > PS_PC_MAX_PARAMS) return false;

  LEX *lex = stmt->m_lex;
  if (lex->sql_command != SQLCOM_SELECT) return false;
  if (lex->using_hypergraph_optimizer()) return false;

  Query_expression *unit = lex->unit;
  if (unit == nullptr || !unit->is_simple()) return false;

  Query_block *qb = unit->first_query_block();
  if (qb->leaf_table_count != 1) return false;
  if (qb->outer_join != 0) return false;
  if (qb->first_inner_query_expression() != nullptr) return false;
  if (qb->is_grouped() || qb->is_distinct() ||
      qb->is_ordered() || qb->has_limit() ||
      qb->has_windows() || qb->has_ft_funcs()) return false;

  Table_ref *tbl = qb->leaf_tables;
  if (tbl == nullptr || !tbl->is_base_table()) return false;

  // WHERE shape 提取
  PsPointPlanTemplate &tpl = stmt->ps_point_plan_template();
  tpl = PsPointPlanTemplate{};  // 清空
  tpl.table_ref = tbl;

  if (!ps_point_plan_extract_where_shape(qb, &tpl)) {
    stmt->set_ps_point_plan_state(PsPointPlanState::NEVER);
    return false;
  }

  stmt->set_ps_point_plan_state(PsPointPlanState::COLD);
  return true;
}
```

需要额外 include：`sql/sql_lex.h` 以访问 `Query_block` / `Query_expression` 方法。

### 2. 实现 `ps_point_plan_extract_where_shape()` — `sql/ps_point_plan_cache.cc`

Phase 1 需要处理两种 WHERE 形态：

**形态 A — 单列等值：** `WHERE field = ?` 或 `WHERE ? = field`
- `where_cond->type() == Item::FUNC_ITEM`
- `down_cast<Item_func*>(where_cond)->functype() == Item_func::EQ_FUNC`
- `args[0]`/`args[1]` 分别是 `Item_field` + `Item_param`（顺序不限）

**形态 B — 复合键等值（AND）：** `WHERE field1 = ? AND field2 = ?`
- `where_cond->type() == Item::COND_ITEM`
- `down_cast<Item_cond*>(where_cond)->functype() == Item_func::COND_AND_FUNC`
- 遍历 `argument_list()`，每个子项必须是 `EQ_FUNC(field, param)`
- 子项数量 `<= PS_PC_MAX_PARAMS`

**关键 API：**
- `Item_field::field_index`（`uint16`，列在 `TABLE::field` 数组中的位置）— `sql/item.h:4255`
- `Item_param` 判别：`item->type() == Item::PARAM_ITEM` — `sql/item.h:4769`
- `Item_field` 判别：`item->type() == Item::FIELD_ITEM` — `sql/item.h:4305`
- `Item_cond::argument_list()` — `sql/item_cmpfunc.h:2476`

**对模板的填充：**
- `tpl->plan_type = PsCachedPlanType::POINT_EQ_REF`
- `tpl->param_count` = 匹配到的 `field=param` 对数
- `tpl->params[i]` = 对应的 `Item_param*`
- `tpl->field_indices[i]` = 对应的 `Item_field::field_index`

**注意事项：**
- 不检查参数是否重复指向同一列（这属于 Phase 2 admission 的职责）
- 不检查 key 信息（prepare 阶段 table 还未 open，无法访问 `TABLE::key_info`）
- `keyno`、`key_parts`、`key_length` 留空（Phase 2 admission 时填充）

### 3. 挂载分类调用 — `sql/sql_prepare.cc`

**Hook 位置：** `Prepared_statement::prepare()` 第 2597 行之后、第 2607 行（`m_lex->cleanup(true)`）之前。

```cpp
  // line 2597: } (end of first if (error == 0) block)

  // ps_point_plan_cache: classify before LEX cleanup while tree is accessible.
  if (error == 0) {
    ps_point_plan_classify(thd, this);
  }

  assert(error || !thd->is_error());
  // line 2607: m_lex->cleanup(true);
```

**为什么在 `m_lex->cleanup(true)` 之前：** cleanup 会释放 LEX 树上的临时对象。分类需要遍历 `where_cond()` 和 `leaf_tables`，这些在 cleanup 后不再安全。

**关于 `is_sql_prepare()`：** 最终实现移除了 `!is_sql_prepare()` 守卫。原因：
1. 分类是纯只读 shape 检查，对 SQL PREPARE 路径无副作用
2. `reprepare()` 会临时覆盖 `is_sql_prepare()`，加守卫会导致 COM_STMT_PREPARE 在 reprepare 后丢失分类
3. SQL PREPARE 是 MTR 中创建参数化语句的唯一方式，移除守卫使分类可测试
4. SQL PREPARE 的 fast-path 排除将在 Phase 3 的执行时 guard 中实现

### 4. reprepare 后的重分类 — 已解决

移除 `!is_sql_prepare()` 守卫后，reprepare 问题自然解决：`reprepare()` 调用内层 `prepare()`
时，分类 hook 在 `if (error == 0)` 块中无条件运行，LEX 树在 cleanup 之前仍可访问，
重分类正确执行。

### 5. MTR 测试 — `mysql-test/t/ps_point_plan_cache_classify.test`

新增 MTR 测试用例，通过 status 变量间接验证分类结果。

**Phase 1 的验证挑战：** 分类结果（COLD/NEVER）存储在 `Prepared_statement` 的私有成员中，外部无法直接查询。验证方式：

**方案 A（推荐）：增加一个 status counter**
- 在 `ps_point_plan_classify()` 成功时（COLD），递增一个新的 status counter `Ps_point_plan_cache_cold_classifications`
- 这样测试可以通过 `SHOW STATUS LIKE 'Ps_point_plan_cache_cold%'` 断言分类是否成功

**方案 B：依赖 Phase 2+ 的 admission 间接验证**
- Phase 1 的分类正确性只能通过 Phase 2 的 admission 行为间接验证
- 缺点：Phase 1 无法独立验收

**测试矩阵（使用方案 A）：**

| 用例 | 预期 | 验证方法 |
|------|------|---------|
| `SELECT * FROM t WHERE pk = ?` | COLD | counter +1 |
| `SELECT * FROM t WHERE uk = ?` | COLD | counter +1 |
| `SELECT * FROM t WHERE pk1 = ? AND pk2 = ?` (composite) | COLD | counter +1 |
| `SELECT * FROM t WHERE id > ?` (range) | NEVER | counter 不变 |
| `SELECT * FROM t1 JOIN t2 ON ...` | NEVER | counter 不变 |
| `SELECT * FROM t WHERE pk = ? ORDER BY val` | NEVER | counter 不变 |
| `SELECT * FROM t WHERE pk = ? LIMIT 1` | NEVER | counter 不变 |
| `SELECT COUNT(*) FROM t WHERE pk = ?` (grouped) | NEVER | counter 不变 |
| `SELECT DISTINCT val FROM t WHERE pk = ?` | NEVER | counter 不变 |
| `INSERT INTO t VALUES (?, ?)` | NEVER | counter 不变 |
| SQL PREPARE `PREPARE stmt FROM 'SELECT...'` | NEVER | counter 不变 |
| sysvar OFF 时 | NEVER | counter 不变 |

**binary protocol 测试：** 在 `ps_point_plan_cache_binary_proto.test` 中追加 Com_stmt_execute 后检查分类 counter。

### 6. 新增 Status Counter — 需要修改的文件

采用方案 A，需要在以下位置添加 `Ps_point_plan_cache_cold_classifications`：

- `sql/system_variables.h` — `System_status_var` 新增 `ulonglong ps_point_plan_cache_cold_classifications`
- `sql/mysqld.cc` — `status_vars[]` 数组新增一行
- `sql/system_variables.h` — `LAST_STATUS_VAR` 宏可能需要更新（如果新 counter 排序在当前 last 之后）
- `sql/ps_point_plan_cache.cc` — 分类成功时 `thd->status_var.ps_point_plan_cache_cold_classifications++`
- `.result` 文件需要更新（`SHOW STATUS LIKE 'Ps_point_plan_cache%'` 输出多了一行）

### 7. 需要新增的 include

`sql/ps_point_plan_cache.cc` 需要额外 include：
- `sql/item.h` — `Item::Type`, `Item_field`, `Item_param`
- `sql/item_cmpfunc.h` — `Item_func_eq`, `Item_cond`, `Item_cond_and`
- `sql/item_func.h` — `Item_func::Functype`

## 注意事项

- **不访问 TABLE / KEY：** prepare 阶段 table 未 open，`Table_ref::table` 为 nullptr。不要尝试访问 `TABLE::key_info`。key 相关信息（`keyno`、`key_parts`、`key_length`）留给 Phase 2 的 admission 填充。
- **不修改执行路径：** 分类结果只设置状态（COLD/NEVER），不插入任何 fast path 逻辑。
- **Item_param 的 table_ref 校验：** 提取 `Item_field` 时应验证 `Item_field::table_ref == tpl->table_ref`，确保 field 属于目标表而非外部表（虽然单表场景下理论上不会出现）。
- **`where_cond()` 可能为 nullptr：** 无 WHERE 子句时 `qb->where_cond()` 返回 nullptr，需提前排除。
- **down_cast vs dynamic_cast：** MySQL 内核使用 `down_cast<>` 替代 `dynamic_cast<>`，但在不确定类型时应先检查 `type()` / `functype()` 再做 cast。
