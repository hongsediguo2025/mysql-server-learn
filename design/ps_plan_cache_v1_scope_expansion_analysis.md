# PS Plan Cache V1 范围扩充分析

## 1. 文档目的

本文记录 V1 设计过程中做出的一个关键结构性决策：**是否应在 Phase 0 阶段就对模板结构进行泛化，以避免后续扩展时的重构成本？**

**结论：已执行。** Phase 0 的 `PsPointPlanTemplate` 已完成数组化泛化。但 V1 的
**功能范围仍然保持极窄**——只覆盖单表唯一键等值点查（Phase 0-5），功能上包含
单列和复合唯一键等值；PK range、聚合等扩展属于独立的后续工作，不在 V1 范围内。

基于对 V2 深度缓存分析（见 `ps_plan_cache_v2_deep_caching_analysis.md`）的结论，本文记录了分析的推导过程和最终采纳的结构调整。

## 2. V1 的设计定位

V1 定义了一个极窄范围的 per-PS single-slot plan template cache：

- 只做单表唯一键等值点查（`SELECT ... WHERE pk_col = ?`，单列或复合键）
- 只支持 `COM_STMT_PREPARE / COM_STMT_EXECUTE`
- 只支持 old optimizer 路径
- 不做 range / order / distinct / aggregation 的缓存

这个定位在功能层面是正确的——它把风险控制在最小范围内，对齐 sysbench `oltp_point_select` 场景。

## 3. 历史问题：初始骨架的泛化不足

> **注：本节描述的是泛化之前的旧结构，作为决策记录保留。当前代码已是泛化后的版本。**

### 3.1 旧版 PsPointPlanTemplate 的局限

泛化之前的模板结构：

```cpp
// ====== 旧版（已被替换）======
struct PsPointPlanTemplate {
  Table_ref *table_ref{nullptr};
  Item_param *param{nullptr};       // ← 单数：只能存一个参数
  uint field_index{UINT_MAX};       // ← 单数：只能存一个 field
  uint keyno{MAX_KEY};
  uint key_length{0};
  key_part_map null_rejecting{0};
  double best_read{0.0};
  double best_rowcount{1.0};
  enum_field_types actual_type{MYSQL_TYPE_INVALID};   // ← 单数
  bool unsigned_actual{false};                         // ← 单数
};
```

这个结构体是为 "单列唯一键 + 单个参数" 量身定制的。如果 Phase 1-5 按此结构实现完毕，后续扩展到以下场景时必须重构：

| 场景 | 需要多个参数 | 需要多个 field | 需要 plan 类型区分 |
|------|------------|--------------|-------------------|
| 复合唯一键 `WHERE pk1=? AND pk2=?` | 是 (N params) | 是 (N fields) | 否 |
| PK BETWEEN `WHERE id BETWEEN ? AND ?` | 是 (2 params) | 否 | 是 |
| PK BETWEEN + SUM `SELECT SUM(k) ...` | 是 (2 params) | 否 | 是 |

重构影响面：

- `PsPointPlanTemplate` 结构体本身
- 所有 `ps_point_plan_*` helper 函数的签名和实现
- `Prepared_statement` 中的 accessor
- 所有已编写的 MTR 测试用例
- 需要重新验证已通过的全部 Phase

### 3.2 旧版 Helper 函数签名的局限

泛化之前的声明：

```cpp
// ====== 旧版（已重命名为 ps_point_plan_extract_where_shape）======
bool ps_point_plan_extract_eq_field_param(Query_block *qb,
                                          PsPointPlanTemplate *tpl);
```

函数名 `extract_eq_field_param` 明确绑定到 `field = param` 这一种 WHERE 形态。扩展到 BETWEEN 时需要新函数或重命名。

### 3.3 设计文档中的硬约束

`ps_point_plan_cache_v1_design.md` 第 224 行：

```
- `m_param_count == 1`
```

这个约束把所有复合键场景排除在外。复合唯一键（多列 PK/UK 等值）的风险和复杂度与单列几乎相同，不应该被设计层面排除。

## 4. 已完成的结构泛化

### 4.1 PsPointPlanTemplate → 支持多参数（已实施）

核心改动：引入计划类型枚举，将单参数字段扩展为小数组。当前 `sql/ps_point_plan_cache.h` 中的结构即为以下版本：

```cpp
enum class PsCachedPlanType : uchar {
  POINT_EQ_REF = 0,   // Phase 1-5: WHERE pk = ?  (或复合键 WHERE pk1=? AND pk2=?)
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

要点：

- `PS_PC_MAX_KEY_PARTS = 4` 覆盖绝大多数实际场景（多数 PK 不超过 4 列）
- `PS_PC_MAX_PARAMS = 4` 覆盖复合键和 BETWEEN（最多 4 个参数）
- V1 的 Phase 1-5 只使用 `param_count == 1`、`plan_type == POINT_EQ_REF`，行为与旧结构完全一致
- 后续扩展自然利用数组化结构，无需重构

### 4.2 Helper 函数 → 更通用的命名（已实施）

已将 `ps_point_plan_extract_eq_field_param()` 重命名为 `ps_point_plan_extract_where_shape()`，支持多种 WHERE 模式识别。

V1 的实现中，`extract_where_shape()` 内部只识别 `field = ?`，返回 `plan_type = POINT_EQ_REF`。后续扩展增加 BETWEEN 识别时，函数内部增加分支，外部接口不变。

### 4.3 设计文档 → 放宽 param_count 约束（已实施）

已将分类条件从 `m_param_count == 1` 改为 `m_param_count >= 1 && m_param_count <= PS_PC_MAX_PARAMS`，同时保留其他所有约束不变。

## 5. 应该保持不变的部分

| 组件 | 保持不变的理由 |
|------|--------------|
| 状态机 NEVER/COLD/HOT/INVALID | 已足够通用，适用于所有 plan 类型 |
| hook 点 `JOIN::optimize()` | 对所有查询类型都是正确的截断点 |
| sysvar `ps_point_plan_cache` | 单一 kill switch，不需要增加复杂配置 |
| 4 个 status counter | 语义不绑定具体查询类型 |
| Phase 0-5 的阶段策略 | 仍然是最安全的开发方式，先完成最窄场景 |
| `swap_prepared_statement()` 中的 swap | 数组字段同样可以被正确 swap |
| invalidate 逻辑 | 清空模板 + 设 INVALID 的语义不变 |

## 6. 应该等后续 Phase 再做的部分

| 方向 | 推迟的理由 |
|------|----------|
| 缓存 Index_lookup 到 PS arena | 需要修改 init_ref() 的 MEM_ROOT 管理，与模板泛化无关 |
| 缓存 AccessPath / Iterator | 涉及 cleanup 路径修改，风险级别不同 |
| TABLE pinning / 轻量 MDL | 独立的架构课题 |
| ORDER BY / DISTINCT / filesort | 复杂度高，应在 range 基础稳定后再做 |

## 7. V1 阶段规划（最终）

| Phase | 内容 | 风险 | 状态 |
|-------|------|------|------|
| 0 | 骨架 + sysvar + status + 模板泛化 | 低 | **已完成** |
| 1 | prepare 阶段静态分类（单列/复合唯一键 EQ_REF） | 低 | 待实施 |
| 2 | admission-only 预热 | 低 | 待实施 |
| 3 | fast path 接管 | 中 | 待实施 |
| 4 | invalidation / fallback 打磨 | 中 | 待实施 |
| 5 | 性能基准 (oltp_point_select) | 低 | 待实施 |

> **V1 的功能边界止于 Phase 5。** 其中复合唯一键等值已纳入 V1 功能范围，
> 但性能 KPI 仍以单列 `oltp_point_select` 为主；PK range、聚合等扩展作为独立的后续工作推进。
> 技术分析见 `ps_plan_cache_v2_deep_caching_analysis.md`。

## 8. 结论

本文分析的核心建议——**在 Phase 0 阶段就泛化模板结构**——已于 Phase 0 中执行完毕。

最终采纳的策略：

1. **已完成**：泛化 `PsPointPlanTemplate`（多参数数组）、泛化 helper 函数命名、放宽 `param_count` 约束
2. **V1 范围已更新**：Phase 0-5 实现唯一键等值点查（`plan_type == POINT_EQ_REF`），功能上覆盖单列和复合唯一键；性能 KPI 仍以单列 `oltp_point_select` 为主
3. **扩展为独立工作**：PK range、聚合等方向利用已泛化的结构，但作为独立的后续工作推进，不纳入 V1

这样的策略在不改变 V1 功能边界的前提下，避免了后续必然发生的结构体重构。泛化的成本是 Phase 0 代码的小幅修改（结构体字段从单数变数组），但避免的成本是 V1 全部完成后的回炉重做。
