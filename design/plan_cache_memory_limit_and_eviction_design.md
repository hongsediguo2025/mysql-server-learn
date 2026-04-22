# Plan Cache 内存上限控制与淘汰机制设计文档

## 1. 文档定位

本文档记录 plan cache 内存上限控制与淘汰机制的设计决策和实现细节。
目标是为 `ps_point_plan_cache` 提供全局内存和数量控制能力，配合基于
TTL 的惰性淘汰策略，防止在大规模场景（万表 × 千并发）下出现非预期 OOM。

**与现有设计的关系**：本文档是 V1/V1.1/V1.2 的增强迭代，不改变状态机、
fast path 构建逻辑或 admission 判定标准。仅在 admission 入口增加配额
检查和淘汰触发逻辑，在 HOT hit 路径增加轻量时间戳记录。

---

## 2. 新增参数总览与使用指南

### 2.1 参数一览表

| 参数名 | 类型 | 默认值 | 范围 | 说明 |
|--------|------|--------|------|------|
| `ps_point_plan_cache_max_mem_size` | GLOBAL, ulonglong | 1073741824 (1 GB) | [0, 1TB] | Plan cache 允许使用的最大 arena 内存总量（所有连接合计）。**0 = 不限制**。 |
| `ps_point_plan_cache_max_cached_plans` | GLOBAL, ulong | 500000 | [0, 1000000] | 允许同时处于 HOT 状态的 PS（prepared statement）总数。**0 = 不限制**。 |
| `ps_point_plan_cache_eviction_pct` | GLOBAL, uint | 75 | [0, 100] | 触发淘汰扫描的水位线百分比。当内存或 PS 数量超过 `max × pct / 100` 时，admission 会尝试淘汰空闲 PS。**0 = 禁用淘汰**（仅靠硬上限拒绝）。 |
| `ps_point_plan_cache_eviction_idle_seconds` | GLOBAL, ulong | 300 (5分钟) | [0, 86400] | HOT PS 在此时间内未被 fast-path 命中则可被淘汰。**0 = 禁用 TTL 淘汰**。 |

### 2.2 参数之间的协作关系

四个参数构成三级防护体系：

```
  ┌─────────────────────────────────────────────────────────────────┐
  │                     参数协作关系图                                │
  │                                                                 │
  │  max_mem_size / max_cached_plans                                │
  │      │                                                          │
  │      │ × eviction_pct / 100                                     │
  │      ▼                                                          │
  │  ┌─────────┐          ┌──────────────┐        ┌──────────┐     │
  │  │ 安全区   │          │ 淘汰活跃区    │        │ 拒绝区    │     │
  │  │ 直接放行 │          │ 淘汰idle后放行│        │ 淘汰+重试 │     │
  │  └─────────┘          └──────────────┘        └──────────┘     │
  │  0       watermark                        max_limit             │
  │          (max×pct%)                                             │
  │                                                                 │
  │  eviction_idle_seconds: 决定哪些 HOT PS 算"空闲"可被淘汰        │
  └─────────────────────────────────────────────────────────────────┘
```

**参数依赖链**：

1. `max_mem_size` 和 `max_cached_plans` 是**硬上限**，决定系统最多允许多少 plan cache 资源
2. `eviction_pct` 是**软阈值**，基于硬上限计算水位线，决定何时开始淘汰
3. `eviction_idle_seconds` 是**淘汰粒度**，决定哪些 HOT PS 可以被淘汰

### 2.3 参数含义详解

#### `ps_point_plan_cache_max_mem_size`

**含义**：全局所有连接的 plan cache arena 内存总量硬上限。

**工作方式**：
- 每次 PS 从 COLD 进入 HOT（admission）时，预估所需 arena 内存并从全局配额中扣减
- 当 PS 被 invalidation、淘汰或连接断开时，归还配额
- 若剩余配额不足以容纳新 PS 的 arena 内存，拒绝 admission（PS 保持 COLD，走正常优化器）

**设置 0 表示不限制**。此时仍然追踪实际使用量（可通过 Status 变量查看），但不做任何拒绝。

**如何设置**：
- 参考公式：`期望覆盖的 HOT PS 数量 × 平均每 PS ~800 bytes`
- 通常设为可用内存的 1%~5%（例如 128 GB 服务器设 1~6 GB）

#### `ps_point_plan_cache_max_cached_plans`

**含义**：全局同时处于 HOT 状态的 PS 总数硬上限。

**工作方式**：
- 与 `max_mem_size` 互补 — 即使每个 PS 内存很小，PS 数量过多也会消耗大量系统资源
- 当前 HOT PS 数量达到此上限时，拒绝新的 admission

**设置 0 表示不限制**。

**如何设置**：
- 公式：`并发连接数 × 每连接预期活跃表数 × 每表 PS 数（通常 5）`
- 例如 128 并发 × 128 表 × 5 = 81,920，设 100,000 留有余量

#### `ps_point_plan_cache_eviction_pct`

**含义**：淘汰触发的水位线，以硬上限的百分比表示。

**工作方式**：
- 当已用量低于 `max × pct / 100` 时 → **安全区**，直接 admission，不触发淘汰
- 当已用量高于水位线时 → 在 admission 前扫描当前连接的 HOT PS，淘汰空闲条目

**设计意图**：确保常规场景（如 128 并发 × 128 表 ≈ 60 MB）不触发淘汰扫描。
只有当使用量接近上限时，才启动淘汰以腾出空间。

**设置 0** 表示禁用淘汰扫描（始终视为安全区），此时只靠硬上限拒绝新 admission。
**设置 100** 表示总是触发淘汰扫描（每次 admission 都检查）。

#### `ps_point_plan_cache_eviction_idle_seconds`

**含义**：HOT PS 超过多长时间未被 fast-path 命中，就视为"空闲"可被淘汰。

**工作方式**：
- 每次 HOT PS 被 fast-path 命中时，记录当前时间戳（monotonic clock）
- 淘汰扫描时，只淘汰 `当前时间 - last_hit_time > idle_seconds` 的 HOT PS
- 被淘汰的 PS 降级为 NEVER（永久不再尝试 plan cache），后续 EXECUTE 走正常优化器

**设置 0** 表示禁用 TTL 淘汰。此时即使超过水位线也不会淘汰任何 PS，只靠硬上限拒绝。

**如何设置**：
- 业务切换频率高（频繁访问不同表）→ 设小值（如 60 秒）
- 业务稳定（长期访问固定表集合）→ 设大值（如 600 秒）或 0

### 2.4 典型配置示例

#### 示例 1：小规模场景（无需关注 — 使用默认值）

```sql
-- 128 并发 × 128 表，~60 MB，远低于默认 1 GB 上限
-- 默认值即可，不触发淘汰
-- 无需任何设置
```

#### 示例 2：中大规模场景（256 GB 内存，1000 并发 × 1000 表）

```sql
-- 预估: 1000 × 1000 × 5 PS × 800 bytes ≈ 3.8 GB
-- 服务器 256 GB，buffer pool 200 GB，可分 4 GB 给 plan cache
SET GLOBAL ps_point_plan_cache_max_mem_size = 4294967296;    -- 4 GB
SET GLOBAL ps_point_plan_cache_max_cached_plans = 0;         -- 不限数量，靠内存限制
SET GLOBAL ps_point_plan_cache_eviction_pct = 75;            -- 75% 水位线 = 3 GB
SET GLOBAL ps_point_plan_cache_eviction_idle_seconds = 300;  -- 5 分钟空闲可淘汰
```

#### 示例 3：极端场景（128 GB 内存，1000 并发 × 12800 表）

```sql
-- 预估稳态: ~46 GB，超过可用内存
-- 限制 plan cache 为 2 GB，通过淘汰保持最热的 PS
SET GLOBAL ps_point_plan_cache_max_mem_size = 2147483648;    -- 2 GB
SET GLOBAL ps_point_plan_cache_max_cached_plans = 1000000;   -- 100 万
SET GLOBAL ps_point_plan_cache_eviction_pct = 70;            -- 70% 水位线 = 1.4 GB
SET GLOBAL ps_point_plan_cache_eviction_idle_seconds = 60;   -- 1 分钟空闲可淘汰
-- 效果: 每连接约缓存最活跃的 ~260 张表的 plan
```

#### 示例 4：保守模式（完全由 DBA 手动控制）

```sql
-- 禁用淘汰，仅靠硬上限拒绝
SET GLOBAL ps_point_plan_cache_max_mem_size = 1073741824;    -- 1 GB
SET GLOBAL ps_point_plan_cache_max_cached_plans = 500000;
SET GLOBAL ps_point_plan_cache_eviction_pct = 0;             -- 禁用淘汰
SET GLOBAL ps_point_plan_cache_eviction_idle_seconds = 0;    -- 禁用 TTL
-- 效果: 先到先得，满了就拒绝新的 admission
```

#### 示例 5：完全不限制（信任应用不会导致 OOM）

```sql
SET GLOBAL ps_point_plan_cache_max_mem_size = 0;             -- 不限内存
SET GLOBAL ps_point_plan_cache_max_cached_plans = 0;         -- 不限数量
-- 效果: 所有符合条件的 PS 都会被 admission，与不加此功能时的行为一致
```

### 2.5 监控方式

```sql
-- 查看当前 plan cache 资源使用
SHOW GLOBAL STATUS LIKE 'Ps_point_plan_cache%';

-- 关键指标:
-- Ps_point_plan_cache_mem_used            当前总内存使用(bytes)
-- Ps_point_plan_cache_cached_plans        当前 HOT PS 数量
-- Ps_point_plan_cache_admission_refused   被拒绝的 admission 次数
-- Ps_point_plan_cache_evictions           被淘汰的 PS 数量
-- Ps_point_plan_cache_hits                HOT 命中次数（已有）
-- Ps_point_plan_cache_admissions          成功 admission 次数（已有）

-- 查看限制参数
SHOW GLOBAL VARIABLES LIKE 'ps_point_plan_cache%';
```

### 2.6 参数调优建议

| 目标 | 调整方向 |
|------|---------|
| 内存使用过高 | 减小 `max_mem_size`，减小 `eviction_idle_seconds` |
| admission_refused 过多（命中率低） | 增大 `max_mem_size` / `max_cached_plans` |
| evictions 过多（频繁淘汰重建） | 增大 `eviction_idle_seconds`，增大 `max_mem_size` |
| 性能无提升（PS 数量远多于限制） | 增大 `max_mem_size`，或接受部分 PS 走正常优化器 |
| 想彻底禁用 plan cache | `SET SESSION ps_point_plan_cache = OFF`（已有功能） |

---

## 3. 问题分析

### 3.1 当前内存模型

当前 plan cache 采用 per-PS (Prepared Statement) single-slot 架构，
每个 PS 在其 `m_arena` (`Query_arena`) 上分配 arena-cached 组件：

```
Layer 0: 永久对象 (PS m_arena，跨执行存活)
  ├── PsPointPlanTemplate          — 嵌入 PS 对象（~600 bytes）
  ├── cached_key_buff / key_buff2  — 序列化 key 缓冲区
  ├── store_key objects + Field clones — 参数序列化组件
  ├── cached_qep_tab[2] + QEP_shared — 执行计划骨架
  ├── cached_key_copy[] / ref_items[] / cond_guards[] — 指针数组
  └── Range 专属: store_key ×2 + key buffers + KEY_PART — range 组件

Layer 1: 每次执行 (thd->mem_root，命令结束释放)
  ├── AccessPath / QUICK_RANGE     — ~200 bytes/execution
  └── Filesort (仅 SORT/DISTINCT)  — per-execution

Layer 2: 每次绑定 (open_tables → close_thread_tables)
  ├── TABLE*                       — 通过 table_ref->table 获取
  └── handler/file                 — ha_index_init / ha_index_end
```

### 3.2 每个 HOT PS 的 Arena 内存估算

#### POINT_EQ_REF（point_select）

| 组件 | 字节 |
|------|------|
| `cached_key_buff` + `cached_key_buff2` | 2 × ALIGN_SIZE(key_length) |
| `store_key` ×N（含 Field clone） | ~120 bytes × key_parts |
| `cached_qep_tab[2]` | ~200 |
| `cached_qep_shared` | ~100 |
| `cached_key_copy[]` / `cached_ref_items[]` / `cached_cond_guards[]` | 3 × 8 × key_parts |
| MEM_ROOT 开销（block header + alignment） | ×1.3 |
| **典型 INT PK 合计** | **~600 bytes** |

#### RANGE_PK_BETWEEN（simple_ranges / sum_ranges / order_ranges / distinct_ranges）

| 组件 | 字节 |
|------|------|
| `cached_range_qep_tab[2]` + `cached_range_qep_shared` | ~300 |
| `cached_range_key_part` (KEY_PART ×1) | ~64 |
| `cached_range_array` (QUICK_RANGE* ×1) | 8 |
| `cached_range_min_key` / `cached_range_max_key` | 2 × (key_bytes + 1) |
| `store_key` ×2（low + high, 含 Field clones） | ~240 |
| MEM_ROOT 开销 | ×1.3 |
| **典型 INT PK 合计** | **~810 bytes** |

### 3.3 OOM 风险场景分析

#### 基线场景：128 并发 × 128 表（oltp_read_only）

```
每连接 HOT PS: 128 表 × 5 PS/表 = 640 PS
每连接 arena:  128 × (600 + 810×4) = 128 × 3,840 ≈ 480 KB
全局:          128 × 480 KB ≈ 60 MB
全局 HOT PS:   128 × 640 = 81,920
```

**结论**：~60 MB，对任何生产服务器完全可接受。

#### 极端场景：1000 并发 × 12800 表（oltp_read_only）

```
每连接 HOT PS: 12,800 表 × 5 PS/表 = 64,000 PS（稳态）
每连接 arena:  12,800 × 3,840 ≈ 46.9 MB
全局:          1,000 × 46.9 MB ≈ 45.8 GB
全局 HOT PS:   1,000 × 64,000 = 64,000,000
```

**结论**：~46 GB 不可接受，将导致 OOM。

#### 按运行时长的逐步增长（1000 并发 × 12800 表）

| 运行时长 | 每连接触及表数(估) | 每连接 Arena 内存 | 全局总内存 |
|---------|-------------------|------------------|-----------|
| 10 秒 | ~500 | ~1.9 MB | ~1.8 GB |
| 60 秒 | ~3,000 | ~11.2 MB | ~10.9 GB |
| 5 分钟 | ~8,000 | ~30 MB | ~29.3 GB |
| 稳态 | 12,800 | ~46.9 MB | ~45.8 GB |

### 3.4 Arena 内存的根本约束

`MEM_ROOT` 是 bump allocator，不支持释放单个对象。Plan cache 的
arena-cached 组件生命周期绑定 PS 对象，只有 PS 被 `DEALLOCATE` 或
连接断开才会释放。因此：

- 无法通过释放单个 plan cache 条目来回收 arena 内存
- 只能通过 **淘汰**（demote HOT → NEVER）来释放全局配额计数
- 已分配的 arena 内存只在 PS 销毁时才能真正回收
- 反复 invalidation + re-admission 导致 arena 内存单调增长

## 4. 设计目标

1. 提供 **全局** 参数控制 plan cache 内存使用上限和 HOT PS 数量上限
2. 超过上限时 **拒绝新的 admission**（PS 保持 COLD，走正常优化器路径）
3. 引入 **基于 TTL 的惰性淘汰**：当配额不足时，淘汰长时间未命中的 HOT PS
4. 引入 **水位线机制**：低于水位线时不触发淘汰，避免常规场景的不必要开销
5. 提供 **可观测性**：Status 变量显示内存使用量和淘汰统计
6. **不影响 HOT hit 性能**：fast path 仅增加一次时间戳记录（~5ns）

## 5. 总体架构

```
                    ┌─────────────────────────────────────┐
                    │     ps_point_plan_cache_max_mem_size │  ← 全局 sysvar
                    │     (default: 1 GB / 0=unlimited)    │
                    └──────────────┬──────────────────────┘
                                   │
                ┌──────────────────▼──────────────────────┐
                │   Global Plan Cache Memory Tracker      │
                │   ┌────────────────────────────────┐    │
                │   │ std::atomic<size_t> total_bytes │    │
                │   │ std::atomic<size_t> total_plans │    │
                │   └────────────────────────────────┘    │
                │                                         │
                │   admission check:                      │
                │     safe zone → direct admit            │
                │     above watermark → try evict + admit │
                │     hard limit → refuse                 │
                └─────────────────────────────────────────┘
                       ▲               ▲               ▲
                       │               │               │
                ┌──────┴──┐    ┌──────┴──┐     ┌──────┴──┐
                │ Conn 1  │    │ Conn 2  │     │ Conn N  │
                │ HOT PS  │    │ HOT PS  │     │ HOT PS  │
                │ list    │    │ list    │     │ list    │
                └─────────┘    └─────────┘     └─────────┘
```

### 5.1 三级内存区间模型

```
  0              watermark (max×pct/100)         max_limit
  ├─── 安全区 ────────┤──── 淘汰活跃区 ──────────┤── 拒绝区 ──┤
  │ 不检查淘汰        │ admission 时检查淘汰     │ 淘汰+重试  │
  │ 直接 admission    │ 超水位则尝试淘汰idle PS  │ 仍不够则拒绝│
```

- **安全区**（0 ~ watermark）：直接 admission，零额外开销
- **淘汰活跃区**（watermark ~ max）：admission 时触发连接内淘汰扫描
- **拒绝区**（超过 max）：先淘汰，仍不够则拒绝 admission

### 5.2 水位线默认值推导

基线场景（128 并发 × 128 表）= ~60 MB / ~82K PS。

使用百分比水位线 `eviction_pct = 75`，在默认 `max_mem_size = 1 GB` 下：
- 水位线 = 1 GB × 75% = 768 MB
- 60 MB << 768 MB → **基线场景始终在安全区内**

| 场景 | 内存 | 行为 |
|------|------|------|
| 128 并发 × 128 表 | ~60 MB | 安全区，不触发淘汰 |
| 256 并发 × 256 表 | ~480 MB | 安全区，不触发淘汰 |
| 128 并发 × 260 表 | ~128 MB | 安全区，不触发淘汰 |
| 512 并发 × 512 表 | ~1.9 GB | 超过硬上限，触发淘汰 |
| 1000 并发 × 12800 表 | ~46 GB | 远超硬上限，大量淘汰+拒绝 |

## 6. 全局内存追踪器

### 6.1 数据结构

**文件**: `sql/ps_point_plan_cache.h`

```cpp
/**
  Global tracker for plan cache memory usage.
  Thread-safe via std::atomic; lock-free on the admission hot path.
*/
class Ps_plan_cache_mem_tracker {
 public:
  bool try_reserve(size_t bytes);
  void release(size_t bytes);
  bool try_add_plan();
  void remove_plan();

  size_t current_mem_used() const {
    return m_total_bytes.load(std::memory_order_relaxed);
  }
  size_t current_plan_count() const {
    return m_total_plans.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<size_t> m_total_bytes{0};
  std::atomic<size_t> m_total_plans{0};
};

extern Ps_plan_cache_mem_tracker ps_plan_cache_tracker;
```

### 6.2 实现

**文件**: `sql/ps_point_plan_cache.cc`

```cpp
Ps_plan_cache_mem_tracker ps_plan_cache_tracker;

bool Ps_plan_cache_mem_tracker::try_reserve(size_t bytes) {
  ulonglong max_mem = ps_point_plan_cache_max_mem_size;
  if (max_mem == 0) {
    m_total_bytes.fetch_add(bytes, std::memory_order_relaxed);
    return true;
  }
  size_t current = m_total_bytes.load(std::memory_order_relaxed);
  while (true) {
    if (current + bytes > max_mem) return false;
    if (m_total_bytes.compare_exchange_weak(
            current, current + bytes,
            std::memory_order_acq_rel, std::memory_order_relaxed))
      return true;
  }
}

void Ps_plan_cache_mem_tracker::release(size_t bytes) {
  m_total_bytes.fetch_sub(bytes, std::memory_order_relaxed);
}

bool Ps_plan_cache_mem_tracker::try_add_plan() {
  ulong max_plans = ps_point_plan_cache_max_cached_plans;
  if (max_plans == 0) {
    m_total_plans.fetch_add(1, std::memory_order_relaxed);
    return true;
  }
  size_t current = m_total_plans.load(std::memory_order_relaxed);
  while (true) {
    if (current >= max_plans) return false;
    if (m_total_plans.compare_exchange_weak(
            current, current + 1,
            std::memory_order_acq_rel, std::memory_order_relaxed))
      return true;
  }
}

void Ps_plan_cache_mem_tracker::remove_plan() {
  m_total_plans.fetch_sub(1, std::memory_order_relaxed);
}
```

## 7. 每 PS 内存计账与时间戳

### 7.1 模板扩展

**文件**: `sql/ps_point_plan_cache.h`

在 `PsPointPlanTemplate` 末尾新增：

```cpp
struct PsPointPlanTemplate {
  // ... existing fields ...

  /// Total arena bytes allocated for this PS's plan cache components.
  size_t arena_cached_bytes{0};

  /// Timestamp of last successful HOT hit (monotonic clock, seconds).
  std::atomic<uint64_t> last_hit_time{0};

  /// Timestamp of admission (monotonic clock, seconds).
  uint64_t admission_time{0};
};
```

### 7.2 估算函数

```cpp
static size_t ps_point_plan_estimate_arena_bytes(
    const PsPointPlanTemplate &tpl) {
  size_t bytes = 0;
  if (tpl.plan_type == PsCachedPlanType::POINT_EQ_REF) {
    const size_t aligned_key = ALIGN_SIZE(tpl.key_length);
    bytes += aligned_key * 2;
    bytes += tpl.key_parts * 200;
    bytes += sizeof(QEP_TAB) * 2 + sizeof(QEP_shared);
    bytes += tpl.key_parts * 8 * 3;
  } else {
    bytes += sizeof(QEP_TAB) * 2 + sizeof(QEP_shared);
    bytes += 64 + 8;
    bytes += (tpl.key_length + 1) * 2;
    bytes += 2 * 200;
  }
  bytes = bytes * 13 / 10;  // MEM_ROOT overhead factor
  return bytes;
}
```

### 7.3 时间戳机制

```cpp
static inline uint64_t ps_pc_monotonic_seconds() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
  return static_cast<uint64_t>(ts.tv_sec);
}
```

HOT hit 路径在 `ps_point_plan_mark_hit()` 前增加：
```cpp
tpl.last_hit_time.store(ps_pc_monotonic_seconds(), std::memory_order_relaxed);
```

Admission 路径初始化：
```cpp
uint64_t now = ps_pc_monotonic_seconds();
tpl.admission_time = now;
tpl.last_hit_time.store(now, std::memory_order_relaxed);
```

## 8. 淘汰策略

### 8.1 方案选择：TTL + 惰性淘汰

**为什么不用 LRU**：全局 LRU 链表需要互斥锁，每次 HOT hit 更新链表位置 → 严重锁竞争。

**TTL + 惰性淘汰**：每个 PS 记录最后 hit 时间戳（atomic store, ~5ns），淘汰检查只在 admission 配额不足时触发。

### 8.2 只做连接内淘汰

PS 对象归属于单个连接，跨连接修改需要全局锁，风险极高。
连接内淘汰足够：当连接自己的 admission 被阻塞时，先淘汰自己的空闲 PS。

### 8.3 淘汰的实际效果

| 层次 | 操作 | 效果 | Arena 内存释放? |
|------|------|------|----------------|
| 轻量淘汰 | HOT → NEVER | 释放全局配额 | ❌（PS 销毁时释放） |
| 深度释放 | DEALLOCATE PS / 连接断开 | PS 整体销毁 | ✅ |

### 8.4 淘汰扫描函数

```cpp
static uint ps_point_plan_evict_idle_entries(THD *thd,
                                              size_t needed_bytes,
                                              size_t needed_plans) {
  const ulong idle_threshold = ps_point_plan_cache_eviction_idle_seconds;
  if (idle_threshold == 0) return 0;

  const uint64_t now = ps_pc_monotonic_seconds();
  const uint64_t cutoff = (now > idle_threshold) ? now - idle_threshold : 0;

  uint evicted = 0;
  size_t freed_bytes = 0;
  size_t freed_plans = 0;

  // Iterate current connection's prepared statements.
  // For each HOT PS with last_hit_time < cutoff:
  //   - set state to NEVER
  //   - release arena_cached_bytes from global tracker
  //   - release plan count from global tracker
  //   - increment eviction counter
  //   - early exit if freed enough

  return evicted;
}
```

## 9. Admission 集成

### 9.1 修改后的流程

```
ps_point_plan_admit(thd, stmt, join)
  │
  ├── estimated_bytes = estimate_arena_bytes(tpl)
  │
  ├── [安全区判断]
  │     current < max × eviction_pct / 100 ?
  │       → 是: try_reserve + try_add_plan → 直接 admission
  │
  ├── [超水位线]
  │     try_reserve + try_add_plan
  │       → 成功: 允许 admission
  │       → 失败: 尝试淘汰 idle PS
  │
  ├── [淘汰]
  │     evict_idle_entries(thd, needed_bytes, 1)
  │       → 淘汰成功 + 重试成功: admission
  │       → 仍失败: 拒绝
  │
  └── [拒绝]
        mark_admission_refused(thd)
        PS stays COLD
```

### 9.2 关键不变量

- **HOT hit 路径不检查内存限制**
- **现有 admission 判定标准不变**（`ps_point_plan_can_admit()` 逻辑完全不变）
- **拒绝 admission 后 PS 保持 COLD**，下次 EXECUTE 走正常优化器

## 10. Invalidation / Deallocation 回收

### 10.1 `invalidate_ps_point_plan_cache()` 修改

在现有逻辑最前面增加：
```cpp
if (m_ps_pc_state == PsPointPlanState::HOT) {
  ps_plan_cache_tracker.remove_plan();
  if (m_ps_pc.arena_cached_bytes > 0) {
    ps_plan_cache_tracker.release(m_ps_pc.arena_cached_bytes);
    m_ps_pc.arena_cached_bytes = 0;
  }
}
```

### 10.2 `ps_point_plan_demote_to_cold()` 修改

```cpp
if (stmt->ps_point_plan_state() == PsPointPlanState::HOT) {
  ps_plan_cache_tracker.remove_plan();
  // arena_cached_bytes preserved — may be reused on re-admission
}
```

### 10.3 `~Prepared_statement()` 最终回收

```cpp
if (m_ps_pc_state == PsPointPlanState::HOT) {
  ps_plan_cache_tracker.remove_plan();
}
if (m_ps_pc.arena_cached_bytes > 0) {
  ps_plan_cache_tracker.release(m_ps_pc.arena_cached_bytes);
}
```

## 11. 性能影响分析

| 路径 | 原有开销 | 新增开销 | 说明 |
|------|---------|---------|------|
| HOT hit (fast path) | ~200ns | **+5ns** | 时间戳记录 |
| COLD → HOT admission (安全区) | ~5μs | **+15ns** | relaxed load + CAS |
| COLD → HOT admission (淘汰) | ~5μs | **+1~50μs** | 扫描连接内 PS |
| HOT → COLD demote | ~50ns | **+5ns** | atomic decrement |
| PS deallocation | ~100ns | **+10ns** | atomic decrement ×2 |

## 12. 测试矩阵

### 12.1 功能测试

| 场景 | 预期行为 |
|------|---------|
| 默认限制，少量 PS | 正常 admission/hit |
| `max_mem_size = 4096` | 少量 PS admission，超限 refused |
| `max_cached_plans = 5` | 前 5 个 HOT，第 6 个 refused |
| `max_mem_size = 0` + `max_cached_plans = 0` | 不限制 |
| `eviction_pct = 0` | 禁用淘汰 |
| `eviction_idle_seconds = 0` | 禁用 TTL 淘汰 |

### 12.2 淘汰测试

| 场景 | 预期行为 |
|------|---------|
| 配额满 + 有 idle PS | 淘汰后新 PS 成功 admission |
| 配额满 + 无 idle PS | 拒绝 admission |
| 被淘汰 PS 再次 EXECUTE | 走正常优化器，结果正确 |

### 12.3 回收测试

| 场景 | 预期行为 |
|------|---------|
| HOT PS invalidation | mem_used 减少 |
| 连接断开 | mem_used 减少 |
| DEALLOCATE PREPARE | mem_used 减少 |

### 12.4 回归测试

| 场景 | 预期行为 |
|------|---------|
| 现有 point-query MTR | 不回退 |
| 现有 range MTR | 不回退 |
| sysbench point_select 性能 | 不回退 |

## 13. 完整实施计划与策略

### 13.1 实施原则

1. **TDD 优先**：先写 MTR 测试，再实现功能
2. **小步快跑**：每个阶段独立可验证、可回退
3. **不破坏现有功能**：每步完成后跑全量 plan cache 回归
4. **按依赖顺序推进**：基础设施 → 核心逻辑 → 集成 → 测试

### 13.2 阶段总览

```
阶段 1: 基础设施（追踪器 + 模板扩展 + 系统变量）
  │
  ▼
阶段 2: 核心逻辑（估算 + 时间戳 + 配额检查）
  │
  ▼
阶段 3: 淘汰机制（扫描函数 + admission 集成）
  │
  ▼
阶段 4: 回收路径（invalidation + demote + 析构）
  │
  ▼
阶段 5: 可观测性（Status 变量 + 监控）
  │
  ▼
阶段 6: 测试 + 回归 + 性能验证
```

### 13.3 阶段 1：基础设施

**目标**：搭建全局追踪器、扩展模板结构、注册系统变量。此阶段不改变任何
运行时行为 — 追踪器存在但无人调用，变量存在但无人读取。

#### Task 1.1：全局追踪器声明

**文件**: `sql/ps_point_plan_cache.h`

**改动内容**：
- 在 `#endif` 之前新增 `Ps_plan_cache_mem_tracker` 类声明
- 声明全局单例 `extern Ps_plan_cache_mem_tracker ps_plan_cache_tracker`
- 新增 `#include <atomic>` 头文件

**改动量**：~40 行新增

#### Task 1.2：全局追踪器实现

**文件**: `sql/ps_point_plan_cache.cc`

**改动内容**：
- 实现 `try_reserve()`, `release()`, `try_add_plan()`, `remove_plan()`
- 定义全局单例 `Ps_plan_cache_mem_tracker ps_plan_cache_tracker`

**改动量**：~50 行新增

#### Task 1.3：模板结构扩展

**文件**: `sql/ps_point_plan_cache.h`

**改动内容**：
- 在 `PsPointPlanTemplate` 末尾新增 3 个字段：
  - `size_t arena_cached_bytes{0}`
  - `std::atomic<uint64_t> last_hit_time{0}`
  - `uint64_t admission_time{0}`

**改动量**：~15 行新增

#### Task 1.4：全局变量声明

**文件**: `sql/mysqld.h` 或 `sql/system_variables.h`

**改动内容**：
- 声明 4 个全局变量：
  ```cpp
  extern ulonglong ps_point_plan_cache_max_mem_size;
  extern ulong ps_point_plan_cache_max_cached_plans;
  extern uint ps_point_plan_cache_eviction_pct;
  extern ulong ps_point_plan_cache_eviction_idle_seconds;
  ```

**改动量**：~10 行新增

#### Task 1.5：系统变量注册

**文件**: `sql/sys_vars.cc`

**改动内容**：
- 在已有的 `Sys_ps_point_plan_cache` 附近注册 4 个新 sysvar：
  ```cpp
  static Sys_var_ulonglong Sys_ps_point_plan_cache_max_mem_size(...)
  static Sys_var_ulong Sys_ps_point_plan_cache_max_cached_plans(...)
  static Sys_var_uint Sys_ps_point_plan_cache_eviction_pct(...)
  static Sys_var_ulong Sys_ps_point_plan_cache_eviction_idle_seconds(...)
  ```
- 在 `sql/mysqld.cc` 中定义全局变量初始值

**改动量**：~40 行新增

#### 阶段 1 门禁

- [ ] Debug build 编译通过
- [ ] `SHOW GLOBAL VARIABLES LIKE 'ps_point_plan_cache%'` 显示所有新变量
- [ ] `SET GLOBAL` 动态修改新变量生效
- [ ] 现有 plan cache MTR 全绿（行为未改变）

---

### 13.4 阶段 2：核心逻辑

**目标**：实现估算函数、时间戳记录、配额检查。此阶段开始改变 admission
路径行为（增加配额检查），但不实现淘汰扫描。

#### Task 2.1：时间戳辅助函数

**文件**: `sql/ps_point_plan_cache.cc`

**改动内容**：
- 在 namespace {} 内新增 `ps_pc_monotonic_seconds()` 函数
- 需要 `#include <time.h>`

**改动量**：~10 行新增

#### Task 2.2：估算函数

**文件**: `sql/ps_point_plan_cache.cc`

**改动内容**：
- 在 namespace {} 内新增 `ps_point_plan_estimate_arena_bytes()` 函数
- 根据 `plan_type` 分别估算 POINT_EQ_REF 和 RANGE 变体的 arena 内存

**改动量**：~30 行新增

#### Task 2.3：HOT hit 路径增加时间戳更新

**文件**: `sql/ps_point_plan_cache.cc`

**改动内容**：
- 在 `ps_point_plan_build_fast_path()` 中，每个 `ps_point_plan_mark_hit(thd); return true;` 之前增加一行时间戳更新
- 涉及位置：
  - RANGE_PK_BETWEEN_AGG 的 hit return（约 line 1149）
  - RANGE_PK_BETWEEN_SORT 的 hit return（约 line 1180）
  - RANGE_PK_BETWEEN_SORT_DISTINCT 的 hit return
  - RANGE_PK_BETWEEN 的 hit return（约 line 1223）
  - POINT_EQ_REF 的 hit return（约 line 1414）

**改动量**：~5 行修改（每处 1 行）

**注意**：`tpl` 当前声明为 `const PsPointPlanTemplate &`，但 `last_hit_time`
是 `std::atomic<uint64_t>`，对 atomic 的 store 操作在 const 引用上是合法的
（atomic 的 store 是 const-qualified member function）。如果编译器不支持，
需要将 `tpl` 改为非 const 引用或使用 `mutable` 修饰 `last_hit_time`。

#### Task 2.4：admission 路径增加配额检查

**文件**: `sql/ps_point_plan_cache.cc`

**改动内容**：
- 在 `ps_point_plan_admit()` 函数入口（现有 admission 逻辑之前）增加：
  1. 估算 arena 内存
  2. 读取水位线和当前使用量
  3. 安全区判断 → 直接 try_reserve + try_add_plan
  4. 超水位线 → try_reserve + try_add_plan（此阶段不做淘汰，失败直接拒绝）
  5. 拒绝时 return（PS 保持 COLD）
  6. 成功时设置 `tpl.arena_cached_bytes` 和时间戳
- 新增 `ps_point_plan_mark_admission_refused(THD *thd)` helper

**改动量**：~40 行新增/修改

#### Task 2.5：新增 status counter helper

**文件**: `sql/ps_point_plan_cache.{h,cc}`, `sql/system_variables.h`

**改动内容**：
- 声明并实现 `ps_point_plan_mark_admission_refused(THD *thd)`
- 在 `System_status_var` 中新增 `ps_point_plan_cache_admission_refused` 字段
- 在 `System_status_var` 中新增 `ps_point_plan_cache_evictions` 字段（预留）

**改动量**：~15 行新增

#### 阶段 2 门禁

- [ ] Debug build 编译通过
- [ ] 设置 `max_mem_size = 4096` 后，只有少量 PS 能 admission
- [ ] 设置 `max_cached_plans = 3` 后，只有 3 个 PS 能成为 HOT
- [ ] 设置 `max_mem_size = 0` 时行为与之前一致（不限制）
- [ ] 现有 plan cache MTR 全绿（默认参数下行为不变）

---

### 13.5 阶段 3：淘汰机制

**目标**：实现连接内 idle PS 淘汰扫描，在 admission 配额不足时触发。

#### Task 3.1：确认 THD PS 遍历接口

**文件**: `sql/sql_class.h`, `sql/sql_prepare.cc`

**改动内容**：
- 调研 `THD` 上遍历 PS 的接口（`Statement_map` / `Prepared_statement_map`）
- 确认遍历方式：
  - 如果有 `begin()/end()` 迭代器 → 直接使用
  - 如果只有 `find(id)` → 需要在 THD 上维护 HOT PS 链表
- 如果需要 HOT PS 链表，在 `THD` 上新增 intrusive list 头指针

**改动量**：视接口情况，0~30 行

#### Task 3.2：淘汰扫描函数实现

**文件**: `sql/ps_point_plan_cache.cc`

**改动内容**：
- 实现 `ps_point_plan_evict_idle_entries(THD *thd, size_t needed_bytes, size_t needed_plans)`
- 逻辑：
  1. 检查 `eviction_idle_seconds == 0` → return 0
  2. 计算 cutoff 时间
  3. 遍历当前连接的 PS
  4. 对每个 HOT PS：检查 `last_hit_time < cutoff`
  5. 淘汰：set state to NEVER, release tracker quota
  6. 累计释放量，达到 needed 后 early exit
- 新增 eviction counter increment

**改动量**：~60 行新增

#### Task 3.3：admission 集成淘汰重试

**文件**: `sql/ps_point_plan_cache.cc`

**改动内容**：
- 修改 Task 2.4 中的配额检查逻辑，在失败时增加淘汰 + 重试分支：
  1. 配额检查失败
  2. 调用 `evict_idle_entries()`
  3. 若 evicted > 0 → 重试 try_reserve + try_add_plan
  4. 重试成功 → 继续 admission
  5. 重试失败 → 拒绝

**改动量**：~25 行修改

#### 阶段 3 门禁

- [ ] Debug build 编译通过
- [ ] 设置小上限 + 先 prepare 多个 PS 并执行 → 等待 idle → 再 prepare 新 PS → 旧 PS 被淘汰，新 PS 成功 admission
- [ ] 被淘汰的 PS 再次 EXECUTE → 走正常优化器，结果正确
- [ ] `eviction_idle_seconds = 0` 时淘汰被禁用
- [ ] 现有 plan cache MTR 全绿

---

### 13.6 阶段 4：回收路径

**目标**：确保所有 PS 生命周期终点都正确归还全局配额。

#### Task 4.1：invalidation 回收

**文件**: `sql/sql_prepare.h`

**改动内容**：
- 在 `invalidate_ps_point_plan_cache()` 最前面增加：
  ```cpp
  if (m_ps_pc_state == PsPointPlanState::HOT) {
    ps_plan_cache_tracker.remove_plan();
    if (m_ps_pc.arena_cached_bytes > 0) {
      ps_plan_cache_tracker.release(m_ps_pc.arena_cached_bytes);
      m_ps_pc.arena_cached_bytes = 0;
    }
  }
  ```
- 需要在 `sql_prepare.h` 中 `#include` 追踪器头文件（已通过 `ps_point_plan_cache.h` 包含）

**改动量**：~10 行新增

#### Task 4.2：demote 回收

**文件**: `sql/ps_point_plan_cache.cc`

**改动内容**：
- 在 `ps_point_plan_demote_to_cold()` 开头增加：
  ```cpp
  if (stmt->ps_point_plan_state() == PsPointPlanState::HOT) {
    ps_plan_cache_tracker.remove_plan();
  }
  ```
- 注意：demote 时 **不释放** `arena_cached_bytes`，因为 arena 组件可能在 re-admission 时被复用

**改动量**：~5 行新增

#### Task 4.3：析构回收

**文件**: `sql/sql_prepare.cc` 或 `sql/sql_prepare.h`

**改动内容**：
- 在 `Prepared_statement::~Prepared_statement()` 中增加：
  ```cpp
  if (m_ps_pc_state == PsPointPlanState::HOT) {
    ps_plan_cache_tracker.remove_plan();
  }
  if (m_ps_pc.arena_cached_bytes > 0) {
    ps_plan_cache_tracker.release(m_ps_pc.arena_cached_bytes);
  }
  ```

**改动量**：~8 行新增

#### Task 4.4：`ps_point_plan_clear_hot_metadata()` 扩展

**文件**: `sql/ps_point_plan_cache.cc`

**改动内容**：
- 在 `ps_point_plan_clear_hot_metadata()` 中增加 `last_hit_time` 和 `admission_time` 的重置（可选，因为 COLD 状态不读取这些字段）

**改动量**：~3 行新增

#### 阶段 4 门禁

- [ ] Debug build 编译通过
- [ ] `DEALLOCATE PREPARE stmt` → `cached_plans` 和 `mem_used` 减少
- [ ] DDL 触发 invalidation → `cached_plans` 和 `mem_used` 减少
- [ ] 参数类型漂移触发 demote → `cached_plans` 减少（`mem_used` 保持）
- [ ] 连接断开 → `cached_plans` 和 `mem_used` 归零
- [ ] 多轮 admission + invalidation + re-admission → tracker 值始终非负
- [ ] 现有 plan cache MTR 全绿

---

### 13.7 阶段 5：可观测性

**目标**：注册 Status 变量，让 DBA 可以通过 `SHOW STATUS` 监控。

#### Task 5.1：Status 变量注册

**文件**: `sql/mysqld.cc`

**改动内容**：
- 在 `SHOW_VAR status_vars[]` 数组中，已有的 `Ps_point_plan_cache_*` 附近新增：
  ```cpp
  {"Ps_point_plan_cache_mem_used", ...}
  {"Ps_point_plan_cache_cached_plans", ...}
  {"Ps_point_plan_cache_admission_refused", ...}
  {"Ps_point_plan_cache_evictions", ...}
  ```
- `mem_used` 和 `cached_plans` 是全局实时值（需要回调函数从 tracker 读取）
- `admission_refused` 和 `evictions` 是 per-session 累积计数（同已有 counters）

**改动量**：~30 行新增

#### Task 5.2：System_status_var 扩展

**文件**: `sql/system_variables.h`

**改动内容**：
- 在 `System_status_var` 结构体中新增：
  ```cpp
  ulonglong ps_point_plan_cache_admission_refused;
  ulonglong ps_point_plan_cache_evictions;
  ```
- 更新 `LAST_STATUS_VAR` 宏（如果需要）

**改动量**：~5 行新增

#### 阶段 5 门禁

- [ ] `SHOW GLOBAL STATUS LIKE 'Ps_point_plan_cache%'` 显示全部新 counter
- [ ] `mem_used` 在 admission 后增长，在 invalidation/deallocate 后减少
- [ ] `cached_plans` 与 `mem_used` 一致变化
- [ ] `admission_refused` 在上限触发时增长
- [ ] `evictions` 在淘汰触发时增长
- [ ] 现有 plan cache MTR 全绿

---

### 13.8 阶段 6：测试 + 回归 + 性能验证

**目标**：完整 MTR 测试覆盖、全量回归、性能验证。

#### Task 6.1：新增 MTR 测试

**文件**: `mysql-test/t/ps_point_plan_cache_mem_limit.test`
**文件**: `mysql-test/r/ps_point_plan_cache_mem_limit.result`

**测试内容**：

```
--- 功能测试 ---
A. 默认参数，正常 admission + hit（基线确认）
B. max_mem_size = 极小值 → 少量 PS admission，后续 refused
C. max_cached_plans = 5 → 前 5 个 HOT，第 6 个 refused
D. max_mem_size = 0 → 不限制，全部可 admission
E. max_cached_plans = 0 → 不限制

--- 淘汰测试 ---
F. max_cached_plans = 3, idle_seconds = 1 → prepare 3 个 PS 并 hit →
   sleep 2 秒 → prepare 第 4 个 → 旧 PS 被淘汰 → 新 PS admission 成功
G. 被淘汰 PS 再次 EXECUTE → 正常路径，结果正确
H. eviction_idle_seconds = 0 → 淘汰禁用，admission 直接 refused

--- 回收测试 ---
I. HOT PS → DEALLOCATE → cached_plans 和 mem_used 减少
J. HOT PS → DDL invalidation → cached_plans 减少
K. 连接断开 → cached_plans 和 mem_used 归零

--- 水位线测试 ---
L. eviction_pct = 0 → 禁用淘汰扫描
M. eviction_pct = 100 → 每次 admission 都检查

--- 动态调参测试 ---
N. 运行中 SET GLOBAL max_mem_size 缩小 → 已有 HOT 不受影响
O. 运行中 SET GLOBAL max_mem_size 增大 → 新 PS 可 admission
```

**改动量**：~200 行新增

#### Task 6.2：回归脚本更新

**文件**: `.cursor/plan_cache_pipeline.sh` 或 `bench/ps_point_plan_cache/run_mtr_regression.sh`

**改动内容**：
- 将 `ps_point_plan_cache_mem_limit` 加入回归列表

**改动量**：~3 行修改

#### Task 6.3：全量 MTR 回归

**执行命令**：

```bash
# 新增测试
cd build-debug && ./mysql-test/mtr ps_point_plan_cache_mem_limit

# 全量 plan cache 回归（默认模式 + ps-protocol）
./mysql-test/mtr ps_point_plan_cache_classify
./mysql-test/mtr ps_point_plan_cache_coverage
./mysql-test/mtr ps_point_plan_cache_type_variants
./mysql-test/mtr ps_point_plan_cache_env_drift
./mysql-test/mtr ps_point_plan_cache_range_classify
./mysql-test/mtr ps_point_plan_cache_range_admission
./mysql-test/mtr ps_point_plan_cache_range_fast_path
./mysql-test/mtr ps_point_plan_cache_range_edge
./mysql-test/mtr ps_point_plan_cache_range_cursor_proto

# 或使用流水线
bash /Users/a1234/project/mysql-server/.cursor/plan_cache_pipeline.sh
```

#### Task 6.4：Release build + 性能验证

**执行命令**：

```bash
# 编译 Release
cd build-release && make -j$(nproc)

# sysbench point_select 对比（确认 HOT hit +5ns 无回退）
bash bench/ps_point_plan_cache/sysbench_point_select_bench.sh

# sysbench oltp_read_only 对比
bash bench/sysbench_multi_thread_bench.sh
```

**验收标准**：
- oltp_point_select QPS 回退 < 0.5%
- oltp_read_only QPS 回退 < 0.5%

#### 阶段 6 门禁

- [ ] `ps_point_plan_cache_mem_limit` MTR 默认模式通过
- [ ] `ps_point_plan_cache_mem_limit` --ps-protocol 模式通过
- [ ] 全量 plan cache MTR 回归全绿
- [ ] Release build 性能无回退

---

### 13.9 任务总表与状态跟踪

| ID | 任务 | 阶段 | 依赖 | 涉及文件 | 改动量(估) | 状态 |
|----|------|------|------|---------|-----------|------|
| 1.1 | 追踪器类声明 | 1 | 无 | `ps_point_plan_cache.h` | ~40 行 | todo |
| 1.2 | 追踪器实现 | 1 | 1.1 | `ps_point_plan_cache.cc` | ~50 行 | todo |
| 1.3 | 模板字段扩展 | 1 | 无 | `ps_point_plan_cache.h` | ~15 行 | todo |
| 1.4 | 全局变量声明 | 1 | 无 | `mysqld.h` / `system_variables.h` | ~10 行 | todo |
| 1.5 | 系统变量注册 | 1 | 1.4 | `sys_vars.cc`, `mysqld.cc` | ~40 行 | todo |
| 2.1 | 时间戳函数 | 2 | 无 | `ps_point_plan_cache.cc` | ~10 行 | todo |
| 2.2 | 估算函数 | 2 | 1.3 | `ps_point_plan_cache.cc` | ~30 行 | todo |
| 2.3 | hit 路径时间戳 | 2 | 1.3, 2.1 | `ps_point_plan_cache.cc` | ~5 行 | todo |
| 2.4 | admission 配额检查 | 2 | 1.2, 2.2 | `ps_point_plan_cache.cc` | ~40 行 | todo |
| 2.5 | refused counter | 2 | 无 | `*.h`, `*.cc` | ~15 行 | todo |
| 3.1 | PS 遍历接口调研 | 3 | 无 | `sql_class.h` | ~0-30 行 | todo |
| 3.2 | 淘汰扫描函数 | 3 | 3.1, 2.1 | `ps_point_plan_cache.cc` | ~60 行 | todo |
| 3.3 | admission 淘汰集成 | 3 | 3.2, 2.4 | `ps_point_plan_cache.cc` | ~25 行 | todo |
| 4.1 | invalidation 回收 | 4 | 1.2 | `sql_prepare.h` | ~10 行 | todo |
| 4.2 | demote 回收 | 4 | 1.2 | `ps_point_plan_cache.cc` | ~5 行 | todo |
| 4.3 | 析构回收 | 4 | 1.2 | `sql_prepare.cc/h` | ~8 行 | todo |
| 4.4 | clear_hot_metadata 扩展 | 4 | 1.3 | `ps_point_plan_cache.cc` | ~3 行 | todo |
| 5.1 | Status 变量注册 | 5 | 1.2 | `mysqld.cc` | ~30 行 | todo |
| 5.2 | System_status_var 扩展 | 5 | 无 | `system_variables.h` | ~5 行 | todo |
| 6.1 | MTR 测试 | 6 | 全部 | `mysql-test/t/`, `mysql-test/r/` | ~200 行 | todo |
| 6.2 | 回归脚本更新 | 6 | 6.1 | `pipeline.sh` | ~3 行 | todo |
| 6.3 | 全量回归 | 6 | 6.1 | — | — | todo |
| 6.4 | 性能验证 | 6 | 6.3 | — | — | todo |

**总改动量估算**：~700 行（含测试）

### 13.10 推荐提交粒度

按阶段成对提交：

| 提交 | 内容 | 验证 |
|------|------|------|
| Commit 1 | 阶段 1 全部（基础设施） | 编译通过 + 变量可见 |
| Commit 2 | 阶段 2 全部（核心逻辑） | 编译通过 + 配额检查生效 |
| Commit 3 | 阶段 3 全部（淘汰机制） | 编译通过 + 淘汰生效 |
| Commit 4 | 阶段 4 全部（回收路径） | 编译通过 + 回收正确 |
| Commit 5 | 阶段 5 全部（可观测性） | 编译通过 + Status 可见 |
| Commit 6 | 阶段 6（测试 + 回归） | MTR 全绿 + 性能无回退 |

### 13.11 回退策略

每个阶段独立可回退：

| 阶段 | 回退方式 | 影响 |
|------|---------|------|
| 阶段 1 | revert commit 1 | 无功能影响 |
| 阶段 2 | revert commit 2 | 恢复无限制 admission |
| 阶段 3 | revert commit 3 | 仅靠硬上限拒绝，无淘汰 |
| 阶段 4 | revert commit 4 | tracker 值可能不准 → 需同时 revert 2 |
| 阶段 5 | revert commit 5 | 仅失去 Status 变量 |
| 阶段 6 | revert commit 6 | 仅失去 MTR 测试 |

### 13.12 风险缓解计划

| 风险 | 概率 | 影响 | 缓解 |
|------|------|------|------|
| `std::atomic` 在 PsPointPlanTemplate 中导致 non-trivially-copyable | 中 | 编译错误 | 使用 `mutable` 或独立 atomic 字段 |
| THD 无 PS 遍历接口 | 低 | 需要新增 intrusive list | 在 THD 上维护 HOT PS 链表 |
| `CLOCK_MONOTONIC_COARSE` 在 macOS 上不可用 | 中 | 编译错误 | macOS 使用 `mach_absolute_time()` 或 `CLOCK_MONOTONIC` |
| 性能回退超过 0.5% | 低 | 需要优化 | 检查时间戳更新是否可省略（如仅每 N 次更新） |
| 全局 tracker 在极高并发下 CAS 竞争 | 低 | admission 延迟 | CAS 失败概率低，重试成本纳秒级 |

## 14. 风险分析

### 14.1 Atomic 计数精度

高并发下 CAS 可能短暂不精确，但误差不超过一个 PS 的大小（~1 KB），可接受。

### 14.2 淘汰扫描延迟

64,000 HOT PS 扫描约 ~50μs。只在超水位线时触发，安全区内不扫描。

### 14.3 估算偏差

使用 1.3x 保守系数。偏差只影响限制精度，不影响正确性。

### 14.4 demote 后 arena 残留

轻量淘汰（HOT → NEVER）释放配额但不释放 arena 物理内存。
物理内存在 PS 销毁时回收。DBA 可通过减小参数控制增长速率。

## 15. 未来扩展方向

- 会话级内存限制 (`ps_point_plan_cache_session_max_mem_size`)
- 主动淘汰后台线程（需跨线程安全机制）
- `INFORMATION_SCHEMA.PS_PLAN_CACHE` 视图
- 自适应内存限制（联动 InnoDB buffer pool 使用率）
