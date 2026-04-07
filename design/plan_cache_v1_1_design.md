# Plan Cache V1.1 设计文档：点查全场景 + 最小化克隆 + 环境感知 + 按需失效

## 1. 文档定位

本文档记录 V1.1 的设计决策和实现细节。V1.1 是 V1 Phase 0-5 的增强迭代，
目标是使 plan cache 在 sysbench `oltp_point_select` 全场景下健壮运行，
并通过缓存 Index_lookup 组件优化 fast path 性能。

**与 V1 的关系**：V1.1 不改变状态机、hook 点或 sysvar 设计，仅增强守卫覆盖、
测试矩阵和 per-execution 构建效率。

## 2. 变更清单

### 2.1 P0 — 正确性与健壮性

| 变更 | 文件 | 说明 |
|------|------|------|
| Gate 4b: 分区表排除 | `ps_point_plan_cache.cc` classify | `tbl->table->part_info != nullptr` → 不进入 COLD |
| G11: sql_mode 守卫 | `ps_point_plan_cache.{h,cc}` | `PAD_CHAR_TO_FULL_LENGTH` 位 snapshot + runtime 比较 |
| 类型变体测试 | `ps_point_plan_cache_type_variants.test` | VARCHAR PK, VARBINARY PK, nullable UK, ENUM UK, generated column UK, composite mixed-type, 分区表排除, SELECT * vs SELECT col |
| 环境漂移测试 | `ps_point_plan_cache_env_drift.test` | sql_mode drift, ANALYZE TABLE, RENAME INDEX, ADD COLUMN reprepare |

### 2.2 P1 — Index_lookup 缓存优化

| 变更 | 文件 | 说明 |
|------|------|------|
| 模板扩展 | `ps_point_plan_cache.h` | 新增 `cached_key_buff`, `cached_key_buff2`, `cached_store_keys[]`, `cached_to_fields[]`, `ref_cached` |
| Arena 分配 | `ps_point_plan_cache.cc` admit | admission 时通过 `swap_query_arena` 将 store_key + Field clone 分配到 PS arena |
| Fast ref path | `ps_point_plan_cache.cc` build_fast_path | `ref_cached` 为 true 时跳过 `init_ref`/`init_ref_part`，复用缓存组件 |
| store_key 访问器 | `sql_select.h` | 新增 `store_field()` 公开方法 |

## 3. 最小化克隆架构

### 3.1 对象生命周期模型

```
Layer 0: 永久对象 (PS m_arena)
  ├── Table_ref*         — parse tree，跨执行稳定
  ├── Item_param*        — 参数占位符，每次 execute 更新值
  ├── PsPointPlanTemplate — 缓存元数据
  ├── cached_key_buff    — 序列化 key 缓冲区   [V1.1 新增]
  ├── store_key objects  — Field clone + 序列化逻辑 [V1.1 新增]
  └── Field clones       — 在 store_key 内部     [V1.1 新增]

Layer 1: 每次执行 (thd->mem_root，命令结束回收)
  ├── JOIN object        — 不可跨执行保持 (assert join==nullptr)
  ├── QEP_TAB[2]         — fast path 构造
  ├── QEP_shared         — fast path 构造
  ├── key_copy[] / items[] / cond_guards[] — 指针数组
  └── AccessPath          — NewEQRefAccessPath()

Layer 2: 每次绑定 (open_tables → close_thread_tables)
  ├── TABLE*             — 通过 table_ref->table 获取
  └── handler/file       — ha_index_init / ha_index_end
```

### 3.2 克隆策略

| 对象 | V1 | V1.1 | 原因 |
|------|-----|------|------|
| Table_ref* | 存指针 | 存指针 | parse tree 上稳定 |
| TABLE* | 每次 re-bind | 每次 re-bind | 每次执行重新打开 |
| Item_param* | 存指针 | 存指针 | arena 上稳定，execute 更新值 |
| store_key | 每次创建 | **arena 缓存** | 避免 Field clone 开销 |
| Field clone | 每次创建 | **arena 缓存 + re-patch table** | `init(table)` 更新 TABLE 指针 |
| key_buff | thd->mem_root | **arena 缓存** | 大小固定，可跨执行复用 |
| QEP_TAB | thd->mem_root | thd->mem_root | 引用 JOIN 等每次对象 |
| AccessPath | thd->mem_root | thd->mem_root | 引用 TABLE* 每次变化 |

### 3.3 re-patch 安全性分析

缓存的 Field clone 在 store_key 内部，`to_field->table` 在每次 fast path
执行时通过 `Field::init(current_table)` 更新。此更新是安全的因为：

1. `init()` 只设置 `table` 和 `table_name` 两个指针
2. Field 的类型、长度、charset 等元数据不依赖 TABLE 实例
3. `store_key::copy_inner()` 中 `to_field->table` 仅用于 `write_set` bitmap 访问
4. 同一连接内 TABLE 实例的 `write_set` 布局在表结构不变时一致

## 4. 环境变化感知

### 4.1 完整守卫表

| Guard | 层级 | 检查内容 | 变化时动作 |
|-------|------|---------|-----------|
| G1    | Hard | Table_ref/TABLE/TABLE_SHARE 存活 | INVALID |
| G1c   | Soft | optimizer_switch (use_invisible_indexes) | demote to COLD |
| G1d   | Soft | table_ref_version | demote to COLD |
| G2-G5 | Hard | key index/unique/parts/fields 一致性 | INVALID |
| G6    | Hard | param pointer 非空 | INVALID |
| G7    | Fallback | param 非 NULL | runtime fallback |
| G8-G9 | Soft | param data_type_actual / unsigned | demote to COLD |
| G10   | Soft | string param collation | demote to COLD |
| **G11** | **Soft** | **sql_mode (PAD_CHAR_TO_FULL_LENGTH)** | **demote to COLD** |

### 4.2 守卫分层原则

- **Hard invalidate (INVALID)**：表结构/索引的物理变化，需 reprepare 重建
- **Soft demote (COLD)**：会话环境变化，当次走正常优化器，下次可能重新 admit
- **Per-execution fallback (HOT preserved)**：临时条件（NULL 参数、OOM），不降级

### 4.3 sql_mode 守卫设计决策

只追踪 `MODE_PAD_CHAR_TO_FULL_LENGTH`，因为它是唯一影响等值比较语义的 sql_mode 位。
其他 sql_mode 位（`STRICT_*`、`NO_ZERO_*` 等）影响写入路径，不影响 SELECT 点查的
key lookup 行为。

不追踪的决策（记录在案）：
- `DERIVED_MERGE`：V1 已排除子查询/derived，不会触发
- `index_merge` / `mrr` 等 optimizer_switch：仅影响 range/join，不影响点查
- 连接字符集：G10 通过 `param->collation_actual()` 间接覆盖

## 5. 按需失效原则

### 5.1 设计哲学

1. **不做预防性失效**：只在 runtime guard 检测到实际不匹配时才反应
2. **不做过度保守的全量失效**：type drift → COLD（可恢复），不是 INVALID
3. **分级响应**：structural break → hard; environment drift → soft; transient → fallback

### 5.2 retryable_cold 机制

当 HOT → COLD（soft demote）时，设置 `retryable_cold = true`。
当次执行走正常优化器后，admission hook 尝试重新 admit：
- 新环境计划匹配 → re-admit 为 HOT
- 不匹配但 `retryable_cold=true` → 保持 COLD（不立即 NEVER）

这允许 sql_mode / charset 变化后快速恢复，无需 reprepare。

### 5.3 "Item 参数被优化掉" 的处理

classify gate 要求 `tpl.param_count == stmt->m_param_count`，确保所有 `?`
都在 WHERE 等值中。额外的 `?`（SELECT list、非等值条件）导致 NEVER。

优化器 constant-fold（如 `WHERE pk = ? AND 1=1`）不影响 fast path，
因为 fast path 直接按 template 重建，不依赖优化器的条件简化结果。

## 6. 分区表排除

分区表在 classify 阶段排除（`TABLE::part_info != nullptr` → 不进入 COLD），
原因是分区表的 `JT_CONST` 语义与非分区表不同：分区裁剪发生在
`prune_table_partitions()` 中，而 fast path 跳过了该步骤。

## 7. 测试覆盖矩阵

### 7.1 类型变体测试 (`ps_point_plan_cache_type_variants.test`)

| 场景 | 表/列类型 | 预期行为 |
|------|----------|---------|
| A | VARCHAR(64) PK | admit + hit |
| B | VARBINARY(64) PK | admit + hit |
| C | Nullable UNIQUE KEY | admit + hit + NULL fallback |
| D | ENUM UK | admit + hit |
| E | Generated (STORED) column UK | admit + hit |
| F | Composite (INT, VARCHAR) PK | admit + hit |
| G | Partitioned table | NEVER (excluded) |
| H | SELECT * vs SELECT col | both admit + hit independently |

### 7.2 环境漂移测试 (`ps_point_plan_cache_env_drift.test`)

| 场景 | 环境变化 | 预期行为 |
|------|---------|---------|
| A | sql_mode PAD_CHAR_TO_FULL_LENGTH 切换 | demote + re-admit |
| B | ANALYZE TABLE | 通常不影响 (table_ref_version 稳定) |
| C | RENAME INDEX via ALTER | reprepare + re-classify + re-admit |
| D | ALTER TABLE ADD COLUMN | reprepare + re-admit |

## 8. 性能收益预期

Index_lookup 缓存优化避免了每次 fast path 执行的：
- Field clone 创建（`Field::new_key_field()` + 内存分配）
- `store_key` 对象构造

保留的每次执行成本：
- `store_key::copy()` — 参数值序列化到 key buffer（不可避免）
- 3 个小数组分配 (`key_copy[]`, `items[]`, `cond_guards[]`) — bump 分配，纳秒级
- QEP_TAB / QEP_shared / AccessPath 创建 — bump 分配

预期影响：对 INT PK 的 `oltp_point_select` 为边际改善（Field_long clone 开销极小）；
对 VARCHAR/VARBINARY PK 场景可能有更明显收益（Field_varstring clone 更重）。
验证方法：Release build 下运行 `bench/ps_point_plan_cache/run_bench.sh`。
