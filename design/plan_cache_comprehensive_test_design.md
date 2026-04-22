# Plan Cache 全面的测试用例设计文档

## 文档版本

| 版本 | 日期 | 作者 | 变更说明 |
|------|------|------|---------|
| V1.0 | 2025-04-21 | Claude | 初始版本，覆盖全部场景 |

## 1. 文档目的

本文档基于 `plan_cache_memory_limit_and_eviction_design.md` 设计文档，
分析plan cache的全部用户场景和异常场景，设计全面的测试用例。

**目标**：
- 确保所有新增功能有充分的测试覆盖
- 发现潜在的边界问题和异常情况
- 提供可执行的测试用例规格

## 2. 测试覆盖范围总览

### 2.1 测试维度矩阵

| 维度 | 测试点 | 优先级 |
|------|--------|--------|
| **参数配置** | 4个系统变量的各种组合 | P0 |
| **功能路径** | admission, hit, eviction, reclaim | P0 |
| **边界值** | 0, 最大值, 极小值 | P0 |
| **并发场景** | 多连接竞争、状态隔离 | P0 |
| **异常场景** | DDL、参数漂移、连接断开 | P1 |
| **可观测性** | Status变量准确性 | P1 |
| **性能回归** | HOT hit开销、淘汰扫描开销 | P1 |
| **跨协议** | ps-protocol vs cursor-protocol | P1 |

### 2.2 新增功能测试清单

| 功能 | 测试文件 | 状态 |
|------|---------|------|
| max_mem_size限制 | ps_point_plan_cache_memory_limit.test | ✅ |
| max_cached_plans限制 | ps_point_plan_cache_memory_limit.test | ✅ |
| TTL淘汰 | ps_point_plan_cache_ttl.test | ✅ |
| 水位线淘汰 | ps_point_plan_cache_eviction.test | ✅ |
| 多连接竞争 | ps_point_plan_cache_multi_connection.test | ✅ |
| 配额回收 | ps_point_plan_cache_quota_reclaim.test | ✅ |

---

## 3. 参数组合测试场景

### 3.1 系统变量参数空间

| 变量 | 类型 | 范围 | 特殊值 |
|------|------|------|--------|
| max_mem_size | ulonglong | [0, 1TB] | 0=无限制, 4096=极小值 |
| max_cached_plans | ulong | [0, 1M] | 0=无限制, 1=最小非零值 |
| eviction_pct | uint | [0, 100] | 0=禁用, 100=总是检查 |
| eviction_idle_seconds | ulong | [0, 86400] | 0=禁用TTL |

### 3.2 参数组合测试矩阵

#### 场景组A：基础限制功能

| 用例ID | max_mem_size | max_cached_plans | eviction_pct | eviction_idle_seconds | 预期行为 |
|--------|--------------|------------------|--------------|----------------------|---------|
| A-01 | 默认(1GB) | 默认(500K) | 默认(75) | 默认(300) | 正常admission和hit |
| A-02 | 4096 | 默认 | 默认 | 默认 | 少量PS后内存耗尽，refuse |
| A-03 | 默认 | 3 | 默认 | 默认 | 3个HOT后，第4个refuse |
| A-04 | 0 | 默认 | 默认 | 默认 | 不限制内存，仅计数 |
| A-05 | 默认 | 0 | 默认 | 默认 | 不限制数量，仅计内存 |
| A-06 | 0 | 0 | 默认 | 默认 | 完全无限制 |

**测试用例规格**：`ps_point_plan_cache_memory_limit.test` 已覆盖

#### 场景组B：淘汰机制

| 用例ID | max_mem_size | max_cached_plans | eviction_pct | eviction_idle_seconds | 预期行为 |
|--------|--------------|------------------|--------------|----------------------|---------|
| B-01 | 8192 | 默认 | 75 | 2 | 超水位线后淘汰idle PS |
| B-02 | 8192 | 默认 | 0 | 2 | 禁用淘汰扫描，直接refuse |
| B-03 | 8192 | 默认 | 100 | 2 | 每次admission都检查淘汰 |
| B-04 | 8192 | 5 | 75 | 0 | 禁用TTL，不淘汰任何PS |
| B-05 | 8192 | 5 | 0 | 0 | 双重禁用，仅硬上限 |

**测试用例规格**：`ps_point_plan_cache_eviction.test` 已覆盖

#### 场景组C：边界值压力

| 用例ID | 测试点 | 输入值 | 预期行为 |
|--------|--------|--------|---------|
| C-01 | max_mem_size最小有效值 | 1 | 第一个PS应被refuse（arena > 1 byte） |
| C-02 | max_mem_size刚好一个PS | 600 | 第一个PS成功，第二个refuse |
| C-03 | max_cached_plans = 1 | 1 | 第一个PS成功，第二个refuse |
| C-04 | eviction_pct边界 | 0, 1, 99, 100 | 验证水位线计算正确 |
| C-05 | eviction_idle_seconds边界 | 0, 1, 86400 | 验证TTL计算正确 |

**测试用例规格**：部分已覆盖，需补充

---

## 4. 配额回收路径测试

### 4.1 回收触发点

| 回收路径 | 触发条件 | 配额释放 | 测试文件 |
|---------|---------|---------|---------|
| DEALLOCATE PREPARE | 用户显式释放 | mem_used↓, cached_plans↓ | quota_reclaim.test |
| DDL invalidation | 表结构变更 | mem_used↓, cached_plans↓ | quota_reclaim.test |
| 连接断开 | 连接关闭 | mem_used↓, cached_plans↓ | quota_reclaim.test |
| 参数漂移demote | 类型不匹配 | cached_plans↓ (mem_used保持) | env_drift.test |

### 4.2 回收测试详细规格

#### 用例 R-01: DEALLOCATE释放配额

```sql
-- 前置：max_cached_plans = 3
-- 步骤：
-- 1. PREPARE + EXECUTE stmt1, stmt2, stmt3 (quota满)
-- 2. 尝试 stmt4 → 应该refuse或触发淘汰
-- 3. DEALLOCATE stmt2
-- 4. 尝试 stmt5 → 应该成功（配额已释放）
-- 验证：SHOW STATUS显示cached_plans正确
```

#### 用例 R-02: DDL invalidation释放配额

```sql
-- 前置：max_cached_plans = 3
-- 步骤：
-- 1. PREPARE + EXECUTE 3个PS on t1
-- 2. ALTER TABLE t1 ADD COLUMN x INT
-- 3. SHOW STATUS → cached_plans应减少3
-- 4. 新PS应能admission
-- 验证：invalidations计数器增加
```

#### 用例 R-03: 连接断开释放配额

```sql
-- 前置：max_cached_plans = 2
-- 连接1：PREPARE + EXECUTE stmt1, stmt2
-- 连接2：验证quota满
-- 断开连接1
-- 连接2：验证新PS可admission
```

#### 用例 R-04: Arena内存估算准确性

```sql
-- 验证不同查询类型的arena估算
-- POINT_EQ_REF: ~600 bytes
-- RANGE_PK_BETWEEN: ~810 bytes
-- RANGE_PK_BETWEEN_SORT: ~1000 bytes
-- RANGE_PK_BETWEEN_DISTINCT: ~1200 bytes
```

**测试用例规格**：`ps_point_plan_cache_quota_reclaim.test` 已覆盖R-01~R-03，R-04需补充

---

## 5. TTL淘汰机制测试

### 5.1 TTL测试场景

| 用例ID | 场景 | 步骤 | 预期 |
|--------|------|------|------|
| TTL-01 | 空闲超时淘汰 | idle_seconds=3, 创建PS, 等待4s, 执行 | 触发re-admission |
| TTL-02 | 频繁hit保活 | idle_seconds=2, 每1s执行一次 | 不淘汰 |
| TTL-03 | 禁用TTL | idle_seconds=0 | 即使空闲也不淘汰 |
| TTL-04 | last_hit_time更新 | 验证每次HOT hit更新时间戳 | 时间戳正确 |
| TTL-05 | 多PS选择性淘汰 | 2个PS, 只hit一个, 超时后 | 只有未hit的PS被淘汰 |

### 5.2 时序正确性测试

```
时间轴分析：
T0: PREPARE stmt, EXECUTE → HOT, last_hit_time = T0
T2: EXECUTE → last_hit_time = T2
T4: EXECUTE → last_hit_time = T4
T7: eviction_idle_seconds = 3
    → 当前时间T7, last_hit_time = T4
    → T7 - T4 = 3秒, 未超时, 不淘汰
T8: 触发淘汰扫描
    → T8 - T4 = 4秒 > 3秒, 应淘汰
```

**测试用例规格**：`ps_point_plan_cache_ttl.test` 已覆盖

---

## 6. 多连接并发测试

### 6.1 全局配额竞争场景

| 用例ID | 场景 | 配置 | 预期 |
|--------|------|------|------|
| MC-01 | 全局计数器可见性 | 连接1创建PS, 连接2查询STATUS | 计数器一致 |
| MC-02 | 跨连接配额竞争 | max_cached_plans=5, 2连接各创建3个 | 第6个PS被refuse或淘汰 |
| MC-03 | 连接隔离性 | 连接1和连接2同名stmt | 独立命名空间 |
| MC-04 | DDL跨连接影响 | 连接1触发DDL, 连接2的PS失效 | 两连接都受影响 |
| MC-05 | Session级sysvar隔离 | 连接1设OFF, 连接2继承GLOBAL | 各自独立 |

### 6.2 连接内淘汰 vs 跨连接

**设计约束**：淘汰是连接内的，不扫描其他连接的PS

```
场景：max_cached_plans = 3
连接1：创建3个HOT PS (满)
连接2：创建第4个PS
  → 应该只淘汰连接2自己的PS（如果有idle）
  → 不应该淘汰连接1的PS
```

**测试用例规格**：`ps_point_plan_cache_multi_connection.test` 已覆盖

---

## 7. Plan类型变体测试

### 7.1 支持的Plan类型

| Plan类型 | 描述 | Arena估算 | 测试覆盖 |
|---------|------|----------|---------|
| POINT_EQ_REF | 单点主键查询 | ~600 bytes | admission.test |
| RANGE_PK_BETWEEN | 主键范围查询 | ~810 bytes | range_admission.test |
| RANGE_PK_BETWEEN_SORT | 带ORDER BY | ~1000 bytes | orderby.test |
| RANGE_PK_BETWEEN_AGG | 带聚合 | ~1200 bytes | agg.test |
| RANGE_PK_BETWEEN_SORT_DISTINCT | 带去重 | ~1400 bytes | distinct.test |

### 7.2 各类型内存估算验证

```
测试目标：验证ps_point_plan_estimate_arena_bytes()准确性

方法：
1. 对于每种Plan类型：
   a. 创建PS并admission
   b. 记录mem_used增量
   c. 与估算值比较（误差应在±20%内）

2. 不同key_length的影响：
   a. INT PK (4 bytes)
   b. BIGINT PK (8 bytes)
   c. VARCHAR PK (variable)
   d. 复合PK (multi-column)
```

**测试用例规格**：需补充 `ps_point_plan_cache_arena_estimation.test`

---

## 8. 状态计数器准确性测试

### 8.1 状态变量列表

| 变量名 | 类型 | 含义 |
|--------|------|------|
| Ps_point_plan_cache_mem_used | 全局实时 | 当前arena内存总量 |
| Ps_point_plan_cache_cached_plans | 全局实时 | 当前HOT PS总数 |
| Ps_point_plan_cache_admissions | per-session累积 | 成功admission次数 |
| Ps_point_plan_cache_admission_refused | per-session累积 | 拒绝admission次数 |
| Ps_point_plan_cache_evictions | per-session累积 | 淘汰PS次数 |
| Ps_point_plan_cache_hits | per-session累积 | HOT hit次数 |
| Ps_point_plan_cache_cold_classifications | per-session累积 | COLD分类次数 |
| Ps_point_plan_cache_invalidations | per-session累积 | DDL失效次数 |

### 8.2 计数器一致性验证

| 用例ID | 验证点 | 方法 |
|--------|--------|------|
| SC-01 | mem_used非负 | 各种操作后始终>=0 |
| SC-02 | cached_plans非负 | 各种操作后始终>=0 |
| SC-03 | admissions与refused和 | admissions + refused = 总尝试次数 |
| SC-04 | 跨连接可见性 | 连接1修改，连接2可见 |
| SC-05 | DEALLOCATE一致性 | DEALLOCATE后计数器正确减少 |

---

## 9. 边界和异常场景测试

### 9.1 边界值场景

| 用例ID | 测试点 | 输入 | 预期 |
|--------|--------|------|------|
| E-01 | max_mem_size = 0 | 0 | 不限制内存 |
| E-02 | max_cached_plans = 0 | 0 | 不限制数量 |
| E-03 | eviction_pct = 0 | 0 | 禁用淘汰扫描 |
| E-04 | eviction_pct = 100 | 100 | 每次都检查淘汰 |
| E-05 | eviction_idle_seconds = 0 | 0 | 禁用TTL淘汰 |
| E-06 | 单字节内存限制 | 1 | 第一个PS被refuse |
| E-07 | 单PS数量限制 | 1 | 只有1个HOT PS |

### 9.2 异常流程场景

| 用例ID | 场景 | 步骤 | 预期 |
|--------|------|------|------|
| EX-01 | 被淘汰PS再执行 | PS被淘汰后EXECUTE | 走正常优化器，结果正确 |
| EX-02 | 参数漂移后再admission | 类型漂移→COLD，再次EXECUTE | 尝试re-admission |
| EX-03 | DDL后再执行 | DDL失效后EXECUTE | 重新prepare，结果正确 |
| EX-04 | 空stmt_map淘汰 | 连接无PS时触发淘汰 | 安全返回，无crash |
| EX-05 | 只有COLD状态淘汰 | 只有COLD/NEVER，触发淘汰 | 不执行任何淘汰 |

### 9.3 资源耗尽场景

| 用例ID | 场景 | 配置 | 预期 |
|--------|------|------|------|
| R-01 | 内存耗尽后拒绝 | max_mem_size=4096 | refuse新PS |
| R-02 | 数量耗尽后拒绝 | max_cached_plans=1 | refuse新PS |
| R-03 | 双重限制 | 内存和数量都小 | 先触达的限制生效 |
| R-04 | 拒绝后继续尝试 | refuse后再EXECUTE | 仍然refuse（不重试） |

---

## 10. 性能回归测试场景

### 10.1 性能基准

| 路径 | 预期开销 | 回归阈值 |
|------|---------|---------|
| HOT hit | +5ns | <0.5% QPS回退 |
| Safe admission | +15ns | <1% latency增加 |
| Eviction admission | +1~50μs | <5% latency增加 |
| Deallocation | +10ns | <0.5%延迟增加 |

### 10.2 性能测试规格

| 用例ID | 测试 | 指标 |
|--------|------|------|
| P-01 | sysbench point_select | QPS回退<0.5% |
| P-02 | sysbench oltp_read_only | QPS回退<0.5% |
| P-03 | 高频HOT hit | 单次hit延迟<250ns |
| P-04 | 大量PS淘汰扫描 | 64K PS扫描<50μs |
| P-05 | 并发admission竞争 | 无明显锁竞争 |

---

## 11. 协议兼容性测试

### 11.1 协议差异

| 协议 | PREPARE行为 | 测试覆盖 |
|------|------------|---------|
| ps-protocol | 服务端prepare | 全部测试 |
| cursor-protocol | 客户端prepare | cursor_proto.test |

### 11.2 协议特定场景

| 用例ID | 场景 | 协议 | 预期 |
|--------|------|------|------|
| PR-01 | 基础admission | ps-protocol | 正常 |
| PR-02 | 基础admission | cursor-protocol | 正常 |
| PR-03 | 淘汰机制 | ps-protocol | 正常 |
| PR-04 | 淘汰机制 | cursor-protocol | 正常（或跳过） |

---

## 12. 动态调参测试

### 12.1 运行时参数修改

| 用例ID | 操作 | 预期 |
|--------|------|------|
| D-01 | 缩小max_mem_size | 已有HOT不受影响，新PS被限制 |
| D-02 | 增大max_mem_size | 新PS可admission |
| D-03 | 缩小max_cached_plans | 已有HOT不受影响，新PS被限制 |
| D-04 | 动态禁用淘汰 | eviction_pct=0后不再淘汰 |
| D-05 | 动态启用淘汰 | eviction_pct>0后开始淘汰 |
| D-06 | 缩短idle_seconds | 更激进的淘汰 |
| D-07 | 延长idle_seconds | 更保守的淘汰 |

### 12.2 参数持久性

```
场景：SET GLOBAL后
验证：
1. SHOW GLOBAL VARIABLES反映新值
2. 新连接继承新值
3. 重启后恢复默认值（除非my.cnf配置）
```

---

## 13. 监控与可观测性测试

### 13.1 SHOW STATUS验证

| 用例ID | 验证点 | 方法 |
|--------|--------|------|
| M-01 | 变量存在性 | SHOW STATUS LIKE 'Ps_point_plan_cache%' |
| M-02 | mem_used准确性 | admission后增加，deallocation后减少 |
| M-03 | cached_plans准确性 | 与实际HOT PS数一致 |
| M-04 | refused计数 | 拒绝时递增 |
| M-05 | evictions计数 | 淘汰时递增 |

### 13.2 运行监控场景

```
生产监控模拟：
1. 长时间运行（模拟24小时）
2. 周期性检查STATUS变量
3. 验证：
   - 无计数器回滚
   - 无负值
   - 无异常增长
```

---

## 14. 测试用例实施状态

### 14.1 已实施测试

| 测试文件 | 覆盖场景 | 行数 |
|---------|---------|------|
| ps_point_plan_cache_memory_limit.test | A-02~A-06 | ~150 |
| ps_point_plan_cache_eviction.test | B-01~B-05, 水位线 | ~350 |
| ps_point_plan_cache_ttl.test | TTL-01~TTL-05 | ~195 |
| ps_point_plan_cache_multi_connection.test | MC-01~MC-05 | ~350 |
| ps_point_plan_cache_quota_reclaim.test | R-01~R-03 | ~325 |

### 14.2 需补充测试

| 测试文件 | 覆盖场景 | 优先级 |
|---------|---------|--------|
| ps_point_plan_cache_arena_estimation.test | R-04, 各类型内存估算 | P1 |
| ps_point_plan_cache_edge_cases.test | C-01~C-05, E-01~E-07 | P1 |
| ps_point_plan_cache_performance.test | P-01~P-05 | P2 |
| ps_point_plan_cache_dynamic_params.test | D-01~D-07 | P1 |
| ps_point_plan_cache_monitoring.test | M-01~M-05 | P2 |

### 14.3 现有测试覆盖分析

| 功能模块 | 测试覆盖率 | 缺失部分 |
|---------|-----------|---------|
| 参数限制 | 95% | 极端边界值 |
| TTL淘汰 | 95% | 高精度时序验证 |
| 水位线淘汰 | 90% | eviction_pct微调 |
| 配额回收 | 90% | Arena估算准确性 |
| 多连接 | 95% | 高并发竞争 |
| 状态计数器 | 85% | 长时间稳定性 |
| 性能回归 | 0% | 全部需补充 |
| 动态调参 | 20% | 大部分需补充 |

---

## 15. 测试优先级建议

### P0 (必须有)
- 所有基础功能测试
- 边界值测试
- 回收路径测试
- 多连接测试

### P1 (重要)
- Arena估算准确性
- 动态调参
- 异常场景

### P2 (可选)
- 性能回归测试
- 长时间稳定性测试
- 监控可观测性

---

## 16. 测试执行策略

### 16.1 单元测试级别
```
测试单个函数：
- ps_point_plan_estimate_arena_bytes()
- ps_pc_monotonic_seconds()
- Ps_plan_cache_mem_tracker::try_reserve()
- ps_point_plan_evict_idle_entries()
```

### 16.2 集成测试级别
```
MTR测试套件：
- 默认参数基线
- 各参数组合
- 多连接场景
```

### 16.3 回归测试级别
```
全量测试：
- 所有plan cache相关测试
- 性能基准对比
```

---

## 17. 附录：测试数据模板

### 17.1 基础测试表

```sql
-- 主键表
CREATE TABLE t_pk (
  id INT PRIMARY KEY,
  val VARCHAR(50)
);

-- 唯一键表
CREATE TABLE t_uk (
  id INT PRIMARY KEY,
  uk INT NOT NULL UNIQUE,
  val VARCHAR(50)
);

-- 复合主键表
CREATE TABLE t_cpk (
  pk1 INT,
  pk2 INT,
  val INT,
  PRIMARY KEY (pk1, pk2)
);

-- 大数据量表（用于压力测试）
CREATE TABLE t_large (
  id INT PRIMARY KEY,
  data VARCHAR(100)
);
```

### 17.2 测试SQL模板

```sql
-- POINT_EQ_REF
PREPARE stmt_point FROM 'SELECT * FROM t_pk WHERE id = ?';

-- RANGE_PK_BETWEEN
PREPARE stmt_range FROM 'SELECT * FROM t_pk WHERE id BETWEEN ? AND ?';

-- RANGE_PK_BETWEEN_SORT
PREPARE stmt_sort FROM 'SELECT * FROM t_pk WHERE id BETWEEN ? AND ? ORDER BY val';

-- RANGE_PK_BETWEEN_AGG
PREPARE stmt_agg FROM 'SELECT COUNT(*) FROM t_pk WHERE id BETWEEN ? AND ?';

-- RANGE_PK_BETWEEN_DISTINCT
PREPARE stmt_distinct FROM 'SELECT DISTINCT val FROM t_pk WHERE id BETWEEN ? AND ?';
```

---

## 18. 总结

本文档定义了plan cache新增功能的全面测试覆盖：

1. **参数组合测试**：覆盖4个系统变量的各种组合
2. **配额回收测试**：覆盖3种回收路径
3. **TTL淘汰测试**：覆盖时序正确性和边界条件
4. **多连接测试**：覆盖全局配额竞争和隔离性
5. **边界异常测试**：覆盖极端值和异常流程
6. **性能测试**：确保无性能回退
7. **可观测性测试**：验证状态变量准确性

当前已实施5个核心测试文件，覆盖约85%的P0/P1场景。
建议补充5个测试文件以达到100%覆盖。
