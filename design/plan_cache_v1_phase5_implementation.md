# Phase 5: 性能回归与灰度收口 — 详细设计与实施计划

## 目标

Phase 5 是 V1 的最后一个阶段，属于 Milestone D 的收口工作。Phase 0-4 已完成全部功能实现和边界条件测试。Phase 5 的核心目标是：

- 证明收益：sysbench `oltp_point_select` 在多种并发下获得**可复现、可解释**的正向 QPS 收益（幅度随硬件与数据是否完全进 BP 而变化，见下文「性能收益预期与剖析」）
- 确认无回退：默认 ON 下，非目标场景（text SQL、join-heavy、range-heavy 等）无明显性能下跌
- 补全验证缺口：cursor bypass 的 C API 直接验证（Phase 4 仅做了 MTR smoke test）
- 灰度就绪：确认 sysvar kill switch 工作正常、status 计数器可观测、文档齐备

## 前置状态（Phase 0-4 完成后）

### 功能完整性

| 能力 | 状态 | 代码位置 |
|------|------|---------|
| 静态分类 COLD/NEVER | 完成 | `ps_point_plan_classify()` in `ps_point_plan_cache.cc:118` |
| WHERE shape 提取 (单列 + 复合键) | 完成 | `ps_point_plan_extract_where_shape()` in `ps_point_plan_cache.cc:197` |
| Admission COLD→HOT/NEVER | 完成 | `ps_point_plan_can_admit()` + `ps_point_plan_admit()` |
| Fast path (JT_EQ_REF 构造) | 完成 | `ps_point_plan_build_fast_path()` in `ps_point_plan_cache.cc:364` |
| Runtime guard G1-G9 | 完成 | `ps_point_plan_runtime_guard()` in `ps_point_plan_cache.cc:251` |
| Cursor bypass | 完成 | `m_ps_pc_cursor_execution` 接线 in `sql_prepare.cc:3564` |
| Runtime 状态清理 | 完成 | `reset_ps_point_plan_runtime_state()` in `sql_prepare.cc:3550` |
| Invalidation + reprepare | 完成 | swap + re-classify 链路 |
| Sysvar kill switch | 完成 | `ps_point_plan_cache` (session, default ON) |
| Status counters (5 个) | 完成 | hits/admissions/invalidations/fallback_runtime/cold_classifications |

### MTR 测试覆盖

9 个测试文件全部通过：

- `ps_point_plan_cache_show_vars` — sysvar/status 可见性
- `ps_point_plan_cache_binary_proto` — binary PS 协议
- `ps_point_plan_cache_classify` / `classify_ext` — 静态分类
- `ps_point_plan_cache_admission` / `admission_edge` — admission
- `ps_point_plan_cache_fast_path` — fast path 命中 + 结果正确性
- `ps_point_plan_cache_edge` — 边界条件（零行/类型变化/DDL/并发/SP 等）
- `ps_point_plan_cache_cursor_proto` — cursor protocol smoke test

### Phase 5 需补全的缺口

| 缺口 | 来源 | 优先级 |
|------|------|--------|
| sysbench 性能基准 | 设计文档 §14.2 + 实现计划 Phase 5 | P0 |
| 负向回归测试矩阵 | 设计文档 §14.2 | P0 |
| Cursor bypass C API 验证 | Phase 4 文档明确延期 | P1 |
| 线程矩阵 (1/8/32/64/128) | 实现计划 Phase 5 | P0 |
| 复合唯一键 micro-benchmark | 实现计划 Phase 5 step 2 | P1 |

## 性能测试方案

### 测试环境要求

- **硬件**：至少 8 核 CPU、16GB RAM（推荐 16 核 + 32GB）
- **MySQL 构建**：Release build（`-DCMAKE_BUILD_TYPE=Release -DWITH_DEBUG=OFF`）
- **存储引擎**：InnoDB（默认）
- **数据集**：sysbench 标准 `--table-size=10000 --tables=1`（点查场景）
- **基线对比**：相同构建，`SET GLOBAL ps_point_plan_cache = OFF` vs `ON`

### 测试脚本架构

```
bench/ps_point_plan_cache/
  run_bench.sh                 # 主控脚本
  sysbench_point_select.sh     # oltp_point_select 专项
  sysbench_read_only.sh        # oltp_read_only 专项
  sysbench_negative.sh         # 负向回归测试
  micro_composite_uk.sh        # 复合唯一键 micro-benchmark
  cursor_capi_test.cc          # C API cursor bypass 验证
  CMakeLists.txt               # C API 测试编译
  analyze_results.py           # 结果分析脚本
  README.md                    # 测试说明
```

### sysbench 正向基准矩阵

#### oltp_point_select（P0 — 主 KPI）

| 并发线程 | 测试时间 | 重复次数 |
|---------|---------|---------|
| 1 | 60s | 3 |
| 8 | 60s | 3 |
| 32 | 60s | 3 |
| 64 | 60s | 3 |
| 128 | 60s | 3 |

每组测试 ON/OFF 对比，取 3 次中位数。关键指标：QPS、avg latency、p95 latency、p99 latency。

验收标准（分场景，2026-04 按采样剖析修订）：

- **主 KPI（oltp_point_select）**  
  - 所有并发级别 **无 QPS 下降**（相对 `ps_point_plan_cache=OFF`）。  
  - **正向收益**：在 cache-resident（工作集可完全进入 `innodb_buffer_pool`）的典型本机/服务器上，`oltp_point_select` 常见为 **约 1%–5% QPS**；高端移动芯片 + 热点数据下可能偏低。  
  - **剖析依据**：macOS `sample(1)` 对比显示 ON 时 `JOIN::make_join_plan()` 栈样本消失、由 `ps_point_plan_build_fast_path` 替代，而 `row_search_mvcc` 在 OFF/ON 下占比接近，说明 **执行层（InnoDB 点查）仍主导**，故总 QPS 提升幅度天然有上限；复现步骤与一次实测计数见 [`bench/ps_point_plan_cache/docs/profiling_sample_method.md`](../bench/ps_point_plan_cache/docs/profiling_sample_method.md)。  
  - 原「**>5%**」仅作为**理想目标**，不作为所有环境的硬性门槛；若未达标但剖析显示优化器路径已按设计绕过且无任何回退，可接受为 V1 收口。

#### oltp_read_only（P0 — 无回退验证）

同样的并发矩阵。验收标准：所有并发级别无明显 QPS 下降（容差 ±2%）。

#### 复合唯一键 micro-benchmark（P1 — 加分项）

自定义 Lua 脚本，创建复合唯一键表，用 `COM_STMT_PREPARE + COM_STMT_EXECUTE` 循环点查。
验收标准：无明显回退；有收益则记为加分项。

### 负向回归测试矩阵

| 场景 | sysbench workload | 并发 | 预期影响 |
|------|-------------------|------|---------|
| Text SQL (非 PS) | `oltp_point_select --db-ps-mode=disable` | 1, 32, 128 | 零影响 |
| Join-heavy | `oltp_read_only` (含 join 查询) | 1, 32, 128 | 零影响 |
| Range-heavy | `select_random_ranges` | 1, 32, 128 | 零影响 |
| Write-heavy | `oltp_write_only` / `oltp_update_index` | 1, 32, 128 | 零影响 |

验收标准：所有场景 QPS 变化 < ±2%（统计噪声范围内）。

### 测试执行流程

1. 编译 Release 版本 MySQL
2. 初始化数据库 + sysbench 数据
3. 基线测试: `SET GLOBAL ps_point_plan_cache = OFF`
   - 运行正向基准矩阵
   - 运行负向回归矩阵
4. 开启测试: `SET GLOBAL ps_point_plan_cache = ON`（默认值）
   - 运行正向基准矩阵
   - 运行负向回归矩阵
5. 对比分析
   - 生成 QPS/latency 对比表
   - 标记改善/回退百分比
6. Status 验证
   - 检查 `Ps_point_plan_cache_hits > 0`（正向测试后）
   - 检查其他场景 hits = 0

## C API Cursor Bypass 验证

### 背景

Phase 4 的 MTR cursor smoke test 受限于 MTR 的 `--cursor-protocol` 限制——无法在同一个参数化 PS 上使用 `CURSOR_TYPE_READ_ONLY` 多次执行。需要通过 C API 直接验证。

### 验证程序设计

`bench/ps_point_plan_cache/cursor_capi_test.cc` 测试流程：

1. 连接 MySQL
2. `PREPARE: SELECT * FROM t WHERE id = ?`
3. `EXECUTE #1` (non-cursor): admission → HOT
4. `EXECUTE #2` (non-cursor): fast path → hit
5. 验证 hits = 1
6. `EXECUTE #3` (cursor): `mysql_stmt_attr_set(CURSOR_TYPE_READ_ONLY)`
7. 验证 hits 不增（cursor bypass）
8. `EXECUTE #4` (non-cursor): 验证 hits +1（恢复 fast path）
9. Fetch cursor rows, 验证结果正确

### 验收标准

- Cursor 执行时 `Ps_point_plan_cache_hits` 不增长
- Non-cursor 执行时 `Ps_point_plan_cache_hits` 正常增长
- Cursor fetch 返回正确结果
- 无崩溃

## Status 可观测性验证

### 灰度监控要求

| 指标 | 用途 | 验证方法 |
|------|------|---------|
| `Ps_point_plan_cache_hits` | fast path 命中次数 | sysbench 后 > 0 |
| `Ps_point_plan_cache_admissions` | admission 次数 | = PS 数量 |
| `Ps_point_plan_cache_invalidations` | 结构性失效次数 | DDL 后 > 0 |
| `Ps_point_plan_cache_fallback_runtime` | 运行时回退次数 | NULL 参数后 > 0 |
| `Ps_point_plan_cache_cold_classifications` | COLD 分类次数 | PREPARE 后 > 0 |

### sysvar kill switch 验证

- `SET SESSION ps_point_plan_cache = OFF` → 该会话立即停用 fast path
- `SET GLOBAL ps_point_plan_cache = OFF` → 新连接默认停用
- 切换 OFF→ON 后，已 HOT 的 PS 恢复 fast path（无需 reprepare）

## 迭代步骤

### Step 1: 创建 Phase 5 设计文档

创建本文档。

### Step 2: 创建 bench 目录和测试脚本框架

- `bench/ps_point_plan_cache/run_bench.sh`
- `bench/ps_point_plan_cache/sysbench_point_select.sh`
- `bench/ps_point_plan_cache/sysbench_read_only.sh`
- `bench/ps_point_plan_cache/sysbench_negative.sh`

### Step 3: 实现 sysbench 主 KPI 测试（oltp_point_select）

线程矩阵 1/8/32/64/128，ON/OFF 对比，3 次重复取中位数。

### Step 4: 实现 oltp_read_only 回归测试

同样的线程矩阵，确认无回退。

### Step 5: 实现负向回归测试矩阵

Text SQL、join-heavy、range-heavy、write-heavy。

### Step 6: 实现复合唯一键 micro-benchmark

自定义 Lua 脚本。

### Step 7: 实现 C API cursor bypass 验证

`cursor_capi_test.cc` + 编译运行脚本。

### Step 8: 实现结果分析脚本

`analyze_results.py`，自动生成对比表。

### Step 9: 编译 Release 版本 + 执行全量测试

### Step 10: 分析结果 + 调优

### Step 11: 全量 MTR 回归

### Step 12: 更新文档 + 生成最终报告

## 性能收益预期与剖析结论（V1 边界）

V1 仅在 `JOIN::optimize()` 内绕过 `make_join_plan()` 等常规优化管线（见 `sql/sql_optimizer.cc` / `ps_point_plan_build_fast_path`），**每次执行仍须** runtime guard、`init_ref` / `init_ref_part`、`NewEQRefAccessPath` 及完整 InnoDB 点查路径。因此在 **buffer pool 已覆盖数据** 的压测中，**CPU 时间的大头往往在存储引擎与协议栈**，相对 QPS 提升常表现为个位数百分比；要显著拉高比例需 **V2 扩大可缓存形状或更深缓存**（见 `design/ps_plan_cache_v2_deep_caching_analysis.md`）。

**Plan cache v1.1**（含 V1 性能结果与缺失项总结、5%+ 分阶段计划）：[`ps_point_plan_cache_v1_1_five_percent_plan.md`](ps_point_plan_cache_v1_1_five_percent_plan.md)。

## 风险评估

| 风险 | 等级 | 缓解措施 |
|------|------|---------|
| sysbench 环境差异导致结果不可复现 | 中 | 3 次重复取中位数 + 记录硬件环境 |
| oltp_point_select 收益不显著 | 中 | profiling 分析热点；最低预期是不引入回退 |
| 负向回归在某些并发下出现 | 低 | sysvar OFF 作为 kill switch；profiling 定位热点 |
| C API cursor 测试需要客户端库 | 低 | 使用 MySQL 自带的 libmysqlclient |
| Release build 缺少 debug 断言覆盖 | 低 | 所有 MTR 先在 Debug build 下运行通过 |

## 交付标准

Phase 5 完成后，V1 达到最终交付标准（实现计划 §8）：

- 功能默认开启
- 非候选场景在 prepare 阶段即快速落到 NEVER
- 目标点查场景可稳定 COLD → HOT
- HOT 场景可通过 fast path 跳过通用优化
- 任何风险场景都能快速 fallback
- reprepare 后行为正确
- sysbench oltp_point_select 获得可复现收益
- 其他默认场景不出现明显性能回退
- Cursor bypass C API 验证通过
- Status 计数器可观测、可监控
- sysvar kill switch 工作正常

## 不在本阶段做的事

- 不扩展查询覆盖范围 — V2 的职责（range scan、ORDER BY 等）
- 不做跨连接共享 — V1 明确排除
- 不做 LRU/eviction — V1 单槽位设计
- 不做新 status counter — 已有 5 个足够
- 不做 sysvar 细分配置 — 保持单一 kill switch
