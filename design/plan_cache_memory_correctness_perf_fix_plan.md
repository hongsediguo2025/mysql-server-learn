# Plan Cache Memory Correctness And Performance Fix Plan

## 1. Summary

本文档针对 `ps_point_plan_cache` 新增内存管理代码的 review findings
制定修正方案和实施计划。目标不是继续扩展 plan cache 支持的 SQL 形态，
而是先把当前内存管理闭环做稳：

- 内存使用量可控：任何持久化 plan-cache helper 内存都必须被全局 tracker
  计费，且不能因为 quota 拒绝、demote、evict、reprepare 而留下未计费内存。
- 内存正确性 OK：reserve / release / plan-count 增减必须一一对应，状态转换
  必须幂等，重复执行和异常分支不能造成 over-count 或 under-count。
- 性能不受影响：HOT fast path 不允许引入系统时间调用、全局 tracker 原子操作、
  eviction 扫描或其他与本次内存管理相关的可测开销。

本文默认写入 `design/`，与现有
`plan_cache_memory_limit_and_eviction_design.md` 和
`plan_cache_comprehensive_test_design.md` 形成修正版设计基线。

## 2. Review Findings To Fix

| ID | 问题 | 根因 | 风险 | 修正方向 |
|---|---|---|---|---|
| F1 | quota refusal 可被下一次 EXECUTE 绕过 | admission 在 quota 检查前已向 PS arena 分配 helper，并设置 cached 标记 | COLD 重试时 `arena_delta=0`，原本拒绝的 plan 被 HOT | admission 使用可释放的候选 cache arena；quota 失败时销毁候选对象，不留下 helper |
| F2 | HOT -> COLD demotion 破坏内存计费 | `arena_cached_bytes` 保留或清零的时机与 helper 生命周期不一致 | tracker 永久 over-count / under-count，DEALLOCATE 无法正确释放 | 统一 helper ownership；demote/evict/reset 通过一个幂等函数释放 cache arena 和 tracker |
| F3 | `eviction_pct` sysvar 被忽略 | admission quota failure 后无条件调用 eviction | `eviction_pct=0` 仍可能淘汰；watermark 语义不生效 | admission 前统一通过 `should_try_evict()` 判断 watermark 和禁用语义 |
| F4 | MTR 检查不存在的 status variables | `mem_used` / `cached_plans` 未注册到 SHOW STATUS | 内存测试空跑，无法约束实际行为 | 暴露全局 status variables，MTR 必须验证它们有值且变化正确 |
| F5 | 新 MTR 未接入回归脚本 | `run_mtr_regression.sh` 仍只跑旧核心用例 | 主回归不保护内存管理逻辑 | 新增 memory test group，并在默认/PS/cursor 必要模式下执行 |
| F6 | `PsPointPlanTemplate` 含 atomic 却被 memcpy swap | `last_hit_time` 使用 `std::atomic`，reprepare 用 byte-swap | C++ 对象生命周期不 portable | 去掉 atomic，改为连接内普通 `uint64_t` 或字段级 swap |
| F7 | HOT hit 更新时间戳有性能和 macOS race 风险 | 每次 hit 调 `ps_point_plan_now_seconds()` 并 atomic store | HOT path 增加 syscall/clock/atomic 风险，macOS 初始化 race | HOT path 不调用 clock；使用 THD 已有 query start seconds + plain store，或禁用 hit-time update |

## 3. Target Invariants

实现完成后必须满足以下不变量。后续代码 review 和 MTR 都以这些不变量为准。

| Invariant | 说明 | 必须覆盖的测试 |
|---|---|---|
| I1: No uncharged persistent bytes | admission 返回后，任何仍可被 HOT fast path 复用的 helper 内存都必须体现在 `Ps_point_plan_cache_mem_used` 中 | quota bypass、memory_limit、arena_lifecycle |
| I2: Refusal leaves no reusable helper | quota 拒绝后，同一个 PS 再次 EXECUTE 不能因为复用上次 helper 而绕过限制 | quota_bypass |
| I3: Reserve/release exactly once | HOT admission 成功 reserve；demote/evict/invalidate/deallocate/connection close release；重复 reset 不重复 release | quota_reclaim、edge_cases |
| I4: Plan count tracks HOT only | `cached_plans` 表示当前 HOT PS 数量；COLD/NEVER 不计入 | memory_limit、eviction、multi_connection |
| I5: Memory bytes track retained helper memory | `mem_used` 表示当前保留的 plan-cache helper arena bytes；释放 cache arena 后必须下降 | arena_lifecycle |
| I6: Eviction policy matches sysvars | `eviction_pct=0` 不扫描；`idle_seconds=0` 不淘汰；非 0 watermark 只在达到水位或硬限制前尝试 | eviction |
| I7: HOT hit does not touch global quota | fast path hit 不调用 tracker reserve/release，不扫描 stmt_map，不调用 system clock | fast_path + benchmark + code inspection |
| I8: Reprepare is object-lifetime safe | `PsPointPlanTemplate` 不再通过 memcpy 操作含 atomic 类型 | DDL reprepare + sanitizer/ASAN build |

## 4. Corrected Memory Ownership Model

### 4.1 选择：独立可释放 cache arena

当前实现把 cached helper 分配在 `Prepared_statement::m_arena` 上。该 arena
是 bump allocator，helper 无法单独释放。因此“eviction 释放 helper 内存”
在当前模型下不成立。

修正方案改为：plan-cache helper 不再分配到 PS 主 arena，而是分配到
Prepared_statement 拥有的独立 cache MEM_ROOT / Query_arena 中。

核心语义：

| 对象 | 生命周期 | 释放时机 | 是否计入 quota |
|---|---|---|---|
| parse tree / Item_param / statement 基础对象 | PS 主 arena | DEALLOCATE / reprepare swap | 否 |
| plan-cache helper: store_key / Field clone / QEP skeleton / range helper | plan-cache cache arena | demote / evict / invalidate / deallocate / failed admission | 是 |
| per-execution AccessPath / QUICK_RANGE / Filesort | THD execution mem_root | statement execution end | 否 |

这个模型的好处：

- quota 失败后可以销毁候选 cache arena，避免 F1 的“失败后残留 helper”。
- demote/evict 可以真正释放 helper 内存，避免 F2 的计费失配。
- HOT path 仍然复用稳定 helper 指针，不需要在 hit 时访问 tracker。

### 4.2 Prepared_statement 增加 cache root

建议在 `Prepared_statement` 中新增 plan-cache 专用 root，而不是把 MEM_ROOT
塞进 `PsPointPlanTemplate`：

```c++
MEM_ROOT m_ps_pc_mem_root;
Query_arena m_ps_pc_arena;
bool m_ps_pc_mem_root_inited{false};
```

原因：

- `Prepared_statement` 已负责 `m_arena` 生命周期，放在同一层更容易在
  destructor、cleanup、swap_prepared_statement 中做统一释放。
- `PsPointPlanTemplate` 保持为“metadata + helper pointers”，避免模板本身
  变成复杂 owner。
- reprepare swap 时可以用字段级 swap 处理 root 和 template，避免 memcpy。

如果现有代码风格不适合在 `Prepared_statement` 里直接嵌入 `MEM_ROOT`，
也可以封装为 `PsPointPlanCacheArena` 小结构体，但 owner 仍应是
Prepared_statement。

### 4.3 统一 reset helper

新增一个唯一入口释放 plan-cache helper：

```c++
void Prepared_statement::reset_ps_point_plan_cache_helpers(
    PsPointPlanResetReason reason);
```

该函数必须幂等，负责：

- 如果当前状态为 HOT，先 `remove_plan()`。
- 如果 `m_ps_pc.arena_cached_bytes > 0`，调用
  `ps_plan_cache_tracker.release(bytes)`，然后清零。
- 清空所有 cached helper 指针和 cached flags。
- 清空 admission/runtime metadata：`keyno`、`key_parts`、`actual_types`、
  `table_ref_version`、range/order/agg/distinct metadata 中仅 HOT 有效的部分。
- 释放或重置 `m_ps_pc_mem_root`。
- 按 reason 设置下一状态：
  - `DemoteRetryable`: COLD + retryable
  - `EvictIdle`: COLD + non-retryable 或 NEVER，按现有语义选择但必须文档化
  - `InvalidateDDL`: COLD + retryable
  - `QuotaRefused`: COLD + quota backoff，不保留 helper
  - `Destroy`: 不关心状态，只释放

所有现有路径禁止直接手写 `remove_plan()` / `release()` / 清指针，必须调用
这个 reset 入口。

## 5. Admission And Quota Flow

### 5.1 `ps_point_plan_admit()` 改为返回状态

把接口从 `void` 改成明确结果：

```c++
enum class PsPointPlanAdmitResult {
  ADMITTED,
  REFUSED_BY_QUOTA,
  BUILD_FAILED
};

PsPointPlanAdmitResult ps_point_plan_admit(
    THD *thd, Prepared_statement *stmt, JOIN *join);
```

`JOIN::optimize()` admission hook 按结果处理：

| 结果 | 状态 | counter | 后续 |
|---|---|---|---|
| `ADMITTED` | HOT | `admissions +1` | 后续 EXECUTE 可走 fast path |
| `REFUSED_BY_QUOTA` | COLD + quota backoff | `admission_refused +1` | 当前执行继续普通路径；后续可在 quota 改变或 backoff 到期后重试 |
| `BUILD_FAILED` | NEVER 或 COLD retryable | 可选新增 counter | 避免部分 helper 残留 |

### 5.2 两阶段 admission

所有 plan type 使用同一 admission 骨架：

1. `ps_point_plan_can_admit()` 只做 plan shape 和 optimizer result 判定，不分配
   持久 helper。
2. `ps_point_plan_prepare_candidate_cache()` 在独立 cache arena 中构建 helper，
   但不修改 HOT 状态，不增加 global plan count。
3. 读取 candidate cache arena 的 `allocated_size()`，得到 exact
   `candidate_bytes`。
4. 通过 `ps_point_plan_maybe_evict_before_admission(thd, candidate_bytes, 1)`
   按 watermark 语义尝试释放当前连接 idle HOT entries。
5. `try_reserve(candidate_bytes)` 成功后，再 `try_add_plan()`。
6. 若任一 quota 步骤失败，销毁 candidate cache arena，清空 cached flags，
   标记 `admission_refused`，返回 `REFUSED_BY_QUOTA`。
7. 若 quota 成功，提交 metadata，设置 `arena_cached_bytes=candidate_bytes`，
   设置 HOT，记录 admission。

这个顺序允许 admission 构建时有短暂临时内存，但 admission 失败后不会留下
持久未计费 helper。若必须严格限制瞬时峰值，可以在下一阶段增加保守 preflight
估算；本轮目标先保证“返回后状态正确且持久内存可控”。

### 5.3 Quota refused 重试策略

当前 `retryable_cold=false` 语义容易把 quota refused 和 shape fail 混在一起。
建议新增一个明确 reason：

```c++
enum class PsPointPlanColdReason {
  InitialCandidate,
  RetryableRuntimeDrift,
  QuotaRefused,
  EvictedIdle,
  DdlInvalidated
};
```

最低成本实现可以先不加 enum，但必须实现等价行为：

- shape/admission 条件不符合：转 NEVER。
- quota refused：保持 COLD，但不保留 helper；同一 EXECUTE 不再 admission。
- quota refused 后下一次 EXECUTE 是否重试由 backoff 控制。
- quota 增大、DEALLOCATE、connection close、eviction 释放资源后，允许重新 admission。

推荐增加 per-PS `quota_refused_at` 或 `quota_refused_epoch`，避免高频场景每次
EXECUTE 都重新构建 candidate cache：

- 默认 backoff 为 1 秒或 64 次 EXECUTE。
- MTR 可以通过 `SET GLOBAL ps_point_plan_cache_max_cached_plans` 增大 quota 后验证
  重新 admission。
- 如果不做 backoff，功能仍正确，但 quota 满时会有 admission 构建开销；需要在
  performance section 记录风险。

## 6. Eviction Policy Fix

### 6.1 `eviction_pct` 语义

新增统一判断函数：

```c++
bool ps_point_plan_should_try_evict(size_t candidate_bytes,
                                    uint candidate_plans);
```

规则：

| 配置 | 行为 |
|---|---|
| `eviction_pct == 0` | 禁止 eviction 扫描；quota 不足直接拒绝 |
| `eviction_idle_seconds == 0` | 禁止 TTL eviction；quota 不足直接拒绝 |
| 对应 quota 上限为 0 | 该维度 unlimited，不参与 watermark |
| 当前使用量 + candidate 低于 watermark | 不扫描，直接尝试 reserve |
| 当前使用量 + candidate 达到 watermark | admission 前扫描当前连接 idle HOT entries |
| 已达到硬上限 | 如果 eviction 开启，扫描后重试；否则拒绝 |

`eviction_pct=100` 的含义固定为：每次 admission 都允许扫描 idle entries，
但只淘汰满足 TTL 的 HOT PS。

### 6.2 Eviction 范围

本轮保持当前连接内 opportunistic eviction，不做跨连接扫描。原因是跨连接
Prepared_statement map 需要额外锁和 owner-thread 安全设计，风险高。

文档和 sysvar 描述必须同步改为：

- quota 是 global。
- eviction scan 是 current THD local。
- 其他连接占用的 quota 只能通过 DEALLOCATE、connection close、DDL invalidation
  或该连接自己触发 eviction 释放。

若未来需要真正 global eviction，应另立设计，不能在本轮顺手实现。

## 7. HOT Path Performance Fix

### 7.1 去掉 hot path clock 和 atomic

`PsPointPlanTemplate::last_hit_time` 改为普通 `uint64_t`：

```c++
uint64_t last_hit_time{0};
```

理由：

- Prepared_statement 由所属 THD 单线程访问，当前 local eviction 也只扫描当前
  THD 的 `stmt_map`。
- 没有跨线程读写需求，不需要 atomic。
- 去掉 atomic 后 `PsPointPlanTemplate` 可以字段级 swap，避免 memcpy UB。

HOT hit 更新时间改为：

```c++
tpl.last_hit_time = static_cast<uint64_t>(thd->query_start_in_secs());
```

要求：

- 不调用 `clock_gettime()` / `mach_absolute_time()`。
- 不使用 global tracker。
- 不扫描 stmt_map。
- 只做连接内普通字段写入。

如果 benchmark 显示这一条普通写仍有可测影响，则降级为 sampling：

- 每 64 或 256 次 hit 更新一次 `last_hit_time`。
- eviction 判断允许近似，只要 MTR 明确该语义。
- 默认先不采样，避免 TTL MTR 引入不确定性。

### 7.2 `ps_point_plan_now_seconds()`

保留该 helper 只用于 admission / eviction slow path，或者完全替换为
`thd->query_start_in_secs()`。

如果仍保留 macOS monotonic helper，必须修复初始化：

```c++
static const mach_timebase_info_data_t tb_info = [] {
  mach_timebase_info_data_t info;
  mach_timebase_info(&info);
  return info;
}();
```

但优先方案是不在 HOT hit 路径使用该 helper。

### 7.3 Reprepare swap

禁止对 `PsPointPlanTemplate` 做 memcpy swap。

修正选择：

- 优先：去掉 `std::atomic` 后使用 `std::swap(m_ps_pc, copy->m_ps_pc)`。
- 如果模板仍包含不可 copy/move 字段，则实现 `swap_ps_point_plan_template(a, b)`，
  字段级 swap 所有 scalar、pointer、array 字段。
- cache arena owner 也必须参与 swap 或 reset，确保新 statement 得到 clean slate，
  旧 statement 销毁时释放旧 helper。

## 8. Status Variables And Observability

新增或修复以下 SHOW GLOBAL STATUS：

| Status | 来源 | 语义 |
|---|---|---|
| `Ps_point_plan_cache_mem_used` | `ps_plan_cache_tracker.current_mem_used()` | 当前已 reserve 且未 release 的 helper bytes |
| `Ps_point_plan_cache_cached_plans` | `ps_plan_cache_tracker.current_plan_count()` | 当前 HOT PS 数量 |
| `Ps_point_plan_cache_admission_refused` | 已有 per-THD status 聚合 | quota refused 次数 |
| `Ps_point_plan_cache_evictions` | 已有 per-THD status 聚合 | idle HOT eviction 次数 |

实现建议：

- `mem_used` / `cached_plans` 是 global atomic tracker，不适合作为
  `System_status_var` per-THD offset。
- 使用 `SHOW_VAR_FUNC` 或等价机制注册动态 SHOW STATUS 函数。
- MTR 必须验证 `SHOW STATUS LIKE` 至少返回一行，禁止空结果被接受。

MTR helper 约定：

```sql
--let $mem = query_get_value(SHOW GLOBAL STATUS LIKE 'Ps_point_plan_cache_mem_used', Value, 1)
--let $plans = query_get_value(SHOW GLOBAL STATUS LIKE 'Ps_point_plan_cache_cached_plans', Value, 1)
```

所有内存测试使用 `SHOW GLOBAL STATUS`，避免 session status 与 global tracker
语义混淆。

## 9. MTR Fix Plan

### 9.1 先修测试可靠性

第一阶段先修测试，不改功能逻辑：

- 所有 `expected:` echo 不允许和实际结果相反。
- 对需要数值比较的地方使用 `--let` + `--error` / SQL 条件断言，不只打印。
- `SHOW STATUS LIKE 'Ps_point_plan_cache_mem_used'` 和
  `cached_plans` 必须先证明变量存在。
- 为 `ps_point_plan_cache_arena_estimation.test` 补 `.result`，或暂时从测试计划
  中移除，不能留下半成品。

### 9.2 新增/修正用例矩阵

| 测试文件 | 目标 | 关键断言 |
|---|---|---|
| `ps_point_plan_cache_quota_bypass.test` | 覆盖 F1 | quota refused 后，同一 PS 第二次 EXECUTE 仍 refused；`admissions/hits/cached_plans` 不增长；`mem_used` 不残留 |
| `ps_point_plan_cache_accounting_lifecycle.test` | 覆盖 F2 | admission、runtime demote、eviction、DDL invalidation、DEALLOCATE、connection close 后 mem/plans 对称变化 |
| `ps_point_plan_cache_eviction.test` | 覆盖 F3 | `eviction_pct=0` 不 evict；`100` 可扫描；watermark 到达才扫描；`idle_seconds=0` 不 evict |
| `ps_point_plan_cache_status_vars.test` | 覆盖 F4 | `mem_used` / `cached_plans` 存在、为数字、和 tracker 行为一致 |
| `ps_point_plan_cache_reprepare_swap.test` | 覆盖 F6 | DDL reprepare 后可重新 classify/admit/hit，无 crash、无 double release |
| `ps_point_plan_cache_hot_path_overhead.test` | 覆盖 F7 的功能面 | HOT hit 只增加 hits，不改变 mem/plans，不触发 eviction/refused |

### 9.3 更新现有 MTR

需要修正的现有文件：

- `ps_point_plan_cache_edge_cases.test/.result`
- `ps_point_plan_cache_eviction.test/.result`
- `ps_point_plan_cache_memory_limit.test/.result`
- `ps_point_plan_cache_quota_reclaim.test/.result`
- `ps_point_plan_cache_multi_connection.test/.result`
- `ps_point_plan_cache_ttl.test/.result`
- `ps_point_plan_cache_arena_estimation.test/.result`

重点修正：

- 把“打印 expected 但不 fail”的段落改成真实断言。
- 所有 quota 测试在 admission 前后记录 `mem_used` 和 `cached_plans`。
- quota 失败场景必须验证 helper 没有残留可复用状态：下一次 EXECUTE 不能 hit。
- DDL/reprepare 场景必须验证 release 后 mem/plans 下降，再重新 admission。

### 9.4 回归脚本

`bench/ps_point_plan_cache/run_mtr_regression.sh` 增加分组：

```bash
MEMORY_TESTS=(
  ps_point_plan_cache_memory_limit
  ps_point_plan_cache_eviction
  ps_point_plan_cache_quota_reclaim
  ps_point_plan_cache_ttl
  ps_point_plan_cache_multi_connection
  ps_point_plan_cache_edge_cases
  ps_point_plan_cache_quota_bypass
  ps_point_plan_cache_accounting_lifecycle
  ps_point_plan_cache_status_vars
)

ADVANCED_TESTS=(
  ps_point_plan_cache_agg
  ps_point_plan_cache_orderby
  ps_point_plan_cache_distinct
  ps_point_plan_cache_distinct_large
)
```

执行策略：

- 默认模式跑 `MAIN_TESTS + MEMORY_TESTS + ADVANCED_TESTS + SYSVAR_TESTS`。
- `--ps-protocol` 跑同一组，除非某个测试明确只适合 text protocol。
- cursor 模式只跑 cursor 专项和必要的 range cursor 专项。
- 新增测试必须进入脚本后才算验收完成。

## 10. Performance Acceptance Plan

### 10.1 静态性能门禁

代码 review 必须确认 HOT fast path：

- 不调用 `ps_plan_cache_tracker.try_reserve/release/try_add_plan/remove_plan`。
- 不调用 `ps_point_plan_try_evict_idle()`。
- 不调用 `clock_gettime()` / `mach_absolute_time()`。
- 不访问跨连接结构。
- 只允许已有 per-THD hit counter 增长，以及可选的 plain `last_hit_time` 写入。

### 10.2 MTR 性能安全断言

MTR 不做 QPS 判断，但要保证 HOT hit 不改变内存管理状态：

- admission 后记录 `mem_used` 和 `cached_plans`。
- 连续 HOT hit 100 次。
- 验证 `hits` 增长 100。
- 验证 `mem_used` 不变。
- 验证 `cached_plans` 不变。
- 验证 `evictions` / `admission_refused` 不变。

### 10.3 Sysbench simple_ranges benchmark

正式性能比拼只跑 sysbench `simple_ranges`：

- 数据规模：`TABLES=64`，`TABLE_SIZE=20000`。
- 并发：`threads=1` 和 `threads=4`。
- 每轮：5 分钟。
- 每轮开始前全量数据加载到 buffer pool。
- 比较：`ps_point_plan_cache=ON` vs `OFF`。
- 顺序：建议 ON/OFF 交错，至少两轮；若时间紧，跑一轮但必须记录噪声风险。

buffer pool 预热要求：

- 在每个 ON/OFF 测试前执行全表覆盖读取，例如对 64 张表执行
  `SELECT COUNT(*), SUM(id), SUM(k) FROM sbtestN`。
- 或使用 sysbench prepare 后的 warmup 脚本确保每张表主键和二级索引页被访问。
- 预热完成后再开始 5 分钟计时，避免把 IO 差异计入 plan cache 对比。

验收阈值：

- `threads=1`: ON 相比 OFF 不允许低于 1%。如果低于 1%，必须用 perf/profile
  证明不是内存管理新逻辑造成，或继续优化。
- `threads=4`: ON 相比 OFF 不允许低于 1%。
- 与修复前 plan-cache ON 基线相比，QPS 不允许出现超过 1% 的下降。
- 报告必须包含 QPS、avg latency、p95 latency、buffer pool read requests/reads、
  plan cache hits/admissions/refused/evictions/mem_used/cached_plans。

## 11. Implementation Task List

| ID | 任务 | 交付物 | 验收 | 回退 |
|---|---|---|---|---|
| M0 | 建立修正基线 | 本文档 | review findings 均映射到任务 | 文档可单独回退 |
| M1 | 暴露 status vars | `mem_used` / `cached_plans` SHOW GLOBAL STATUS | status MTR 通过，变量非空 | 回退 status 注册 |
| M2 | 引入独立 cache arena 与 reset helper | Prepared_statement cache root；幂等 reset 函数 | deallocate/invalidate 不泄漏、不 double release | 回退 cache arena 改动 |
| M3 | 改 admission 返回值和两阶段流程 | `ps_point_plan_admit()` 返回 enum；quota 失败销毁 candidate | quota_bypass MTR 通过 | 回退 admission 流程 |
| M4 | 修 demote/evict/reprepare 生命周期 | 所有 release 统一走 reset helper；去掉散落 release | accounting_lifecycle MTR 通过 | 回退生命周期改动 |
| M5 | 实现 `eviction_pct` watermark | `should_try_evict()`；eviction sysvar 语义一致 | eviction MTR 通过 | 回退 eviction policy |
| M6 | 移除 hot path clock/atomic | `last_hit_time` 普通字段；字段级 swap | reprepare/hot_path MTR 通过 | 回退 timestamp 改动 |
| M7 | 修正新增 MTR `.result` | 所有 expected 与实际一致；新增缺失 result | memory MTR 全绿 | 回退测试改动 |
| M8 | 接入回归脚本 | `MEMORY_TESTS` / `ADVANCED_TESTS` | `run_mtr_regression.sh` 全绿 | 回退脚本接线 |
| M9 | 性能验证 | simple_ranges ON/OFF 报告 | 1/4 线程均满足阈值 | 仅回退性能脚本/报告，不回退已绿功能 |

推荐提交边界：

- `M1`
- `M2 + M3`
- `M4 + M5`
- `M6`
- `M7 + M8`
- `M9`

## 12. Rollback Strategy

回退按风险从外到内执行：

1. Benchmark/report 脚本。
2. 回归脚本接线。
3. MTR 新增/修正结果。
4. hot path timestamp/atomic 调整。
5. eviction watermark policy。
6. lifecycle reset helper。
7. two-phase admission。
8. cache arena owner。
9. status vars。

关键原则：

- 不做半回退：如果 `M2+M3` 已合并，不能只回退 admission 而保留一半 cache arena。
- 测试先行：每个实现任务失败时，保留失败测试作为行为锁定，回退实现重新修。
- 对用户已有未提交修改零侵入：只回退本任务涉及的提交，不用 `git reset --hard`。

## 13. Final Acceptance Criteria

代码层面：

- quota refused 后无 reusable helper 残留。
- demote/evict/invalidate/deallocate/connection close 后 tracker 与状态一致。
- `eviction_pct` 和 `eviction_idle_seconds` 行为与 sysvar 文档一致。
- `PsPointPlanTemplate` 不再 memcpy 操作含 atomic 对象。
- HOT hit 路径无 clock syscall、无 global quota atomic、无 eviction scan。

测试层面：

- 新增/修正 memory MTR 在默认模式和 `--ps-protocol` 下通过。
- cursor 专项在 `--ps-protocol --cursor-protocol` 下通过。
- 原有 point/range/agg/order/distinct MTR 不回退。
- `bench/ps_point_plan_cache/run_mtr_regression.sh` 包含 memory tests 并全绿。

性能层面：

- sysbench `simple_ranges`，64 × 20000，threads 1/4，每轮 5 分钟，buffer pool
  全量预热。
- ON vs OFF 不出现超过 1% 的负向回退。
- 修复后 ON vs 修复前 ON 不出现超过 1% 的负向回退。
- 若结果低于阈值，必须附 profile，确认热点不来自内存管理新增逻辑后才能放行。

## 14. Implementation Status 2026-04-23

本轮已完成的实现闭环：

- `mem_used` / `cached_plans` 已暴露为全局 status variables。
- plan-cache helper 已迁移到 `Prepared_statement` 专用 cache arena。
- admission 改为返回 `PsPointPlanAdmitResult`，quota refused / build failed 不再留下可复用 helper。
- demote / evict / invalidate / destroy 统一走 `reset_ps_point_plan_cache_helpers()`，负责 release tracker、清 cached flags、释放 cache arena。
- `eviction_pct` / `eviction_idle_seconds` 已接入 admission 前 watermark 判断，`0` 按禁用语义处理。
- `last_hit_time` 改为普通 `uint64_t`，HOT hit 使用 `thd->query_start_in_secs()`，移除 hot path clock helper 和 atomic。
- `swap_prepared_statement()` 不再 memcpy `PsPointPlanTemplate`。
- DISTINCT 参数类型漂移使用 one-shot runtime fallback 并保留原 HOT 模板，避免 advanced DISTINCT 重 admission 的 arena 生命周期风险；simple range 仍保留 demote + re-admit 行为。
- `ps_point_plan_cache_quota_bypass.test` 已新增，专门覆盖 quota refused 后同一 PS 重试不能绕过 quota，且 `mem_used/cached_plans` 不残留。
- `run_mtr_regression.sh` 已接入 memory tests 和 advanced tests。

已验证命令：

```bash
cmake --build build-adaptive --target mysqld -j4
bench/ps_point_plan_cache/run_mtr_regression.sh build-adaptive
git diff --check
```

脚本级结果：

- Main tests default: PASS
- Main tests `--ps-protocol`: PASS
- Binary proto `--ps-protocol`: PASS
- Cursor proto `--ps-protocol --cursor-protocol`: PASS

未在本轮新增独立文件、但后续可继续加强的测试：

- `ps_point_plan_cache_accounting_lifecycle.test`
- `ps_point_plan_cache_status_vars.test`
- `ps_point_plan_cache_hot_path_overhead.test`

当前覆盖依赖现有 `memory_limit`、`quota_bypass`、`eviction`、`quota_reclaim`、
`ttl`、`multi_connection`、`edge_cases`、`fast_path`、`range_fast_path` 以及
advanced/cursor 回归。若后续继续推进内存测试精细化，应优先把上述三个独立
测试补齐，并将它们加入 `MEMORY_TESTS`。
