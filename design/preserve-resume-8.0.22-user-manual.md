# Preserve/Resume 8.0.22 用户手册

本文档面向当前 MySQL 8.0.22 preserve-port 分支，说明
Preserve/Resume transaction 特性的使用方式、实现原理、参数体系、典型
场景和约束边界。本文只以当前 8.0.22 worktree 中的源码为依据，不把旧
8.0.45 知识库当作事实源。

核心源码锚点：

- SQL 语法：`sql/sql_yacc.yy`
- 系统变量：`sql/sys_vars.cc`
- Preserve/Resume 主流程：`sql/preserve_trx.cc`
- warmcopy 会话和 binlog cache mirror：`sql/binlog.cc`
- binlog cache copy buffer：`sql/binlog_ostream.cc`
- P_S 表定义：`storage/perfschema/table_preserved_transactions.cc`
- SHOW STATUS：`sql/mysqld.cc`

## 1. 特性概念

Preserve/Resume 的目标是在受控停机、重启或维护窗口中，把一个尚未提交
的显式事务转换成可恢复的 durable token。服务端重启后，客户端可以用该
token 把事务恢复到新的连接上，然后继续执行 `COMMIT`、`ROLLBACK` 或后续
SQL。

它不是普通 XA 的用户接口包装，也不是自动提交机制。它保留的是一个仍处于
事务语义中的未提交事务，包括 InnoDB prepared transaction、可恢复的锁/读
视图/保存点/MDL 元数据、binlog cache sidecar，以及可选的用户变量和临时
表 sidecar。恢复后，事务仍由用户决定提交或回滚。

这个特性的基本原则是 fail closed：

- 如果某个事务不能被完整、安全地保存，命令应失败，而不是生成不可恢复或
  语义不完整的 token。
- 在 durable token 发布前失败，优先保持原事务仍然可用或可清理。
- 在事务已经 detach 或 durable prepare 后失败，代码会尽力 reattach、
  rollback 或记录 observable failure；无法安全恢复时会杀掉相关 session，
  避免继续运行未经验证的事务状态。
- 恢复时会重新检查 binlog 模式、对象权限、token 所属权、事务接收能力和
  sidecar 完整性。

## 2. 用户可见操作

### 2.1 单事务 preserve

单事务 preserve 作用于当前连接上的显式活跃事务。

```sql
START TRANSACTION;
UPDATE t SET v = v + 1 WHERE id = 1;
PREPARE SHUTDOWN PRESERVE TRANSACTION WITH TIMEOUT 300;
```

可选子句：

```sql
PREPARE SHUTDOWN PRESERVE TRANSACTION;
PREPARE SHUTDOWN PRESERVE TRANSACTION WITH TIMEOUT 300;
PREPARE SHUTDOWN PRESERVE TRANSACTION WITH USER VARS;
PREPARE SHUTDOWN PRESERVE TRANSACTION WITH NO USER VARS;
PREPARE SHUTDOWN PRESERVE TRANSACTION WITH TIMEOUT 300 WITH USER VARS;
```

约束：

- 当前连接必须有显式活跃事务，即 `START TRANSACTION`/`BEGIN` 后的事务。
- 执行用户需要 `SHUTDOWN` 权限。
- 当前连接不能处于不支持的上下文，例如复制线程、组复制、升级、force
  recovery、XA 非空状态、存储过程子语句等。
- 成功响应交付后，服务端会把 token 标记为 resumable，并触发 mysqld shutdown。
  用户需要在重启后的新连接上用 token 恢复。

### 2.2 批量 drain preserve

批量 drain 作用于实例上的可保存事务，用于停机前批量保存空闲事务。

```sql
DRAIN TRANSACTIONS PRESERVE WITH TIMEOUT 300;
```

可选子句：

```sql
DRAIN TRANSACTIONS PRESERVE;
DRAIN TRANSACTIONS PRESERVE WITH TIMEOUT 300;
DRAIN TRANSACTIONS PRESERVE WITH USER VARS;
DRAIN TRANSACTIONS PRESERVE WITH NO USER VARS;
DRAIN TRANSACTIONS PRESERVE WITH TIMEOUT 300 WITH USER VARS;
DRAIN TRANSACTIONS PRESERVE WITH TIMEOUT 300 NO USER VARS;
```

和单事务命令不同，batch drain 额外接受不带 `WITH` 的 `NO USER VARS`。

约束：

- 执行 drain 的控制连接本身不能处于显式活跃事务中。
- 执行用户需要 `SHUTDOWN` 权限。
- batch 会阻止新的高风险事务工作，并按 `preserve_trx_drain_mode` 对正在
  活跃执行的事务做 SOFT/HARD drain。
- 默认最多保存 `preserve_trx_batch_max_transactions=256` 个目标事务。

### 2.3 resume

恢复 token：

```sql
RESUME PRESERVED TRANSACTION '<token>';
```

约束：

- 当前 session 必须能接收 preserved transaction，不能已经处于多语句事务
  中。
- 默认只能恢复自己账号生成的 token。
- 跨账号恢复需要动态权限
  `RESUME_ANY_PRESERVED_TRANSACTION`。
- resume 时会重新检查对象权限。拥有跨账号恢复权限不等于绕过表、库、例程、
  trigger、tablespace 等对象权限。
- `read_only` 或 `super_read_only` 下不支持 resume。
- 当前 binlog open/GTID 模式必须与 token 中记录的 binlog 状态兼容。

### 2.4 查看 token

```sql
SHOW PRESERVED TRANSACTIONS;
```

也可以查询 P_S 表：

```sql
SELECT TOKEN, USER, HOST, STATE, EXPIRES_AT, BINLOG_STATE,
       BINLOG_CACHE_SIZE, BINLOG_WARMCOPY_STATE, TEMP_TABLE_STATE,
       LAST_ERROR
  FROM performance_schema.preserved_transactions;
```

`SHOW PRESERVED TRANSACTIONS` 和 P_S 表使用同一套可见性规则：

- 拥有 token 的账号可以看到自己的 preserved transactions。
- 拥有 `PROCESS` 权限或 `RESUME_ANY_PRESERVED_TRANSACTION` 动态权限的账号可以
  看到其他账号的 preserved transactions。
- 没有 `PROCESS` 权限时，输出中的 token 会被脱敏；真正用于 resume 的完整 token
  只应来自 preserve 成功响应或具备足够权限的管理查询。
- P_S 表本身仍受 P_S 表访问权限控制；没有 P_S 读取权限的账号会被拒绝查询。

## 3. 生命周期和原理

### 3.1 单事务 preserve 的阶段

单事务 preserve 按当前代码的关键顺序执行：

1. 入口检查：功能开关、`SHUTDOWN` 权限、classic protocol、非复制/非升级/
   非 force recovery/非 XA、显式活跃事务。
2. timeout 解析：`WITH TIMEOUT` 或 session 默认值会经过 min/max 边界检查。
3. binlog preflight：判断当前事务属于 `GLOBAL_OFF_NO_CACHE`、
   `SESSION_OFF_NO_CACHE`、`LOGGED_EMPTY` 还是 `LOGGED_WITH_CACHE`；no-cache
   状态还要求 GTID state clean。
4. binlog cache 处理：只有存在 logged transaction binlog cache 时才需要 sidecar。
   batch warmcopy provider 已经预建 blob 时复用 warm blob；否则对 logged cache 走
   single-phase copy。
5. 事务内容检查：拒绝非事务表修改，以及 InnoDB/binlog 之外的 read-write
   handlerton 参与者。
6. 临时表 preflight：支持的用户 InnoDB 临时表会被记录；不支持的临时表状态
   fail closed。
7. read view 导出：保存可恢复的 MVCC 读视图信息。
8. implicit lock materialization：按 lock/table/page/time 参数把隐式锁显式
   化或导出，失败则拒绝。
9. MDL 和对象权限 recheck：导出 MDL 描述符，并重新检查当前用户仍有相关对象
   权限。
10. modified table、savepoint、record lock、table lock 预检查。
11. 生成 token，并把 token 映射成内部 XID。
12. 调用 `ha_prepare_low(thd, true)` 完成存储引擎/binlog prepare，再调用
    temp-only prepare。到这里开始跨过 durable point。
13. detach 当前 THD 上的事务，保留 prepared transaction。
14. 生成 snapshot metadata：使用 pre-prepare record-lock payload，导出 InnoDB
    savepoints、table locks、read view、MDL、SQL savepoints、用户变量。
15. 进入 snapshot write：finalize warmcopy blob，构建临时表 manifest/sidecar 描述，
    构建 preserved bundle，写入 durable snapshot，并注册内存记录。
16. 向客户端交付 token；响应完成后将 token 标记为 resumable，并触发 mysqld
    shutdown。之后 token 可在重启后恢复。

### 3.2 batch drain 的阶段

batch drain 在单事务 preserve 外面加了一层 shutdown drain 管理：

1. 控制连接检查：功能开关、`SHUTDOWN` 权限、控制连接不能已有显式事务。
2. 进入全局 manager state，阻止并发 preserve/drain/resume 冲突。
3. 如果 warmcopy 可用，进入 warmcopy phase 1，给已经存在的 binlog cache
   建立 warm external blob。
4. 按 `preserve_trx_drain_mode` drain 活跃事务：
   - `SOFT`：先阻止新的高风险语句，等待活跃事务自然变为空闲。
   - 超过 `preserve_trx_drain_grace_ms` 后升级到 HARD。
   - `HARD`：杀掉其他活跃用户事务并等待。
5. 关闭 warmcopy phase 1 admission，等待已接收的 mirror 工作收敛。
6. 统计 quiesced targets，并检查 batch 个数和 per-user/global 容量。
7. 对每个目标事务执行 preserve kernel。warmcopy 目标在 phase 2 只处理
   bounded tail，不重新复制历史 binlog cache。
8. 完成 participant cleanup/observability，然后进入 shutdown。

### 3.3 recovery 和超时回滚

token 的 timeout 是 wall-clock 语义。重启恢复时：

- 第一次 recovery 可以获得 `preserve_trx_recovery_grace_seconds` 的宽限期，
  用于处理原始 timeout 已经到期的 token。
- 同一 token 最多经历 `preserve_trx_recovery_max_count` 次 startup recovery。
- 超时或恢复次数超过上限时，代码会强制 rollback preserved transaction。
- 运行中也有 expired-token reaper，周期扫描过期 token 并尝试 rollback 和
  清理。
- 如果 rollback 或 cleanup 失败，P_S 会记录 `EXPIRED_CLEANUP_FAILED`、`FAILED`
  或带 `LAST_ERROR` 的 observable record。

## 4. 完整参数表

### 4.1 功能开关和存储目录

| 参数 | 默认值 | 范围/单位 | 作用域 | 含义 |
|---|---:|---|---|---|
| `preserve_trx_enable` | `ON` | `ON`/`OFF` | global | 总开关。关闭时 preserve/drain/resume 都拒绝。已有 preserve/drain 活动时不能关闭。 |
| `preserve_trx_dir` | datadir 下 `preserve/` | path | global/read-only | preserved snapshot、binlog cache sidecar、temp sidecar、warm external blob 使用的目录。 |

`preserve_trx_enable=ON` 要求启动时 snapshot support 可用；启动校验失败会拒绝启用。
`preserve_trx_dir` 由实例 datadir 派生，用于展示实际目录，不是普通运行时调参项。

### 4.2 token 数量和 batch 容量

| 参数 | 默认值 | 范围/单位 | 作用域 | 含义 |
|---|---:|---|---|---|
| `preserve_trx_max_total` | `256` | `1..UINT_MAX32` 个 | global | 实例上最多保留的 preserved transactions 总数。 |
| `preserve_trx_max_pending_per_user` | `256` | `1..UINT_MAX32` 个 | global | 单个账号最多保留的 pending tokens 数。 |
| `preserve_trx_batch_max_transactions` | `256` | `1..UINT_MAX32` 个 | global | 单次 `DRAIN TRANSACTIONS PRESERVE` 最多保存的目标事务数。 |

注意：batch max 是 quiesced-target 计数阶段的上限。warmcopy phase 1 先于该计数
阶段运行；eligible target 的 prefix copy 发生在 batch max 计数之前，因此后续仍会因
batch max 或容量不足 fail closed。

### 4.3 timeout 和 recovery

| 参数 | 默认值 | 范围/单位 | 作用域 | 含义 |
|---|---:|---|---|---|
| `preserve_trx_default_timeout` | `300` | `1..UINT_MAX32` 秒 | session | 未显式 `WITH TIMEOUT` 时使用的 token timeout。 |
| `preserve_trx_min_timeout` | `60` | `1..UINT_MAX32` 秒 | session | 用户指定 timeout 的最小值。 |
| `preserve_trx_max_timeout` | `86400` | `1..UINT_MAX32` 秒 | session | 用户指定 timeout 的最大值。 |
| `preserve_trx_recovery_max_count` | `3` | `1..UINT_MAX32` 次 | global | 同一 token 允许 startup recovery 的最大次数。 |
| `preserve_trx_recovery_grace_seconds` | `120` | `30..1800` 秒 | global | 第一次 recovery 的过期 token 宽限秒数。 |

`WITH TIMEOUT` 只接受整数秒。超过 min/max 或格式错误会失败。

### 4.4 snapshot、binlog sidecar、temp sidecar 尺寸

| 参数 | 默认值 | 范围/单位 | 作用域 | 含义 |
|---|---:|---|---|---|
| `preserve_trx_max_snapshot_bytes` | `16777216` | `1..ULLONG_MAX` 字节 | global | snapshot metadata 文件最大 16 MiB。 |
| `preserve_trx_max_binlog_cache_bytes` | `1073741824` | `1..ULLONG_MAX` 字节 | global | 单个 preserved binlog cache sidecar 最大 1 GiB。warmcopy 和非 warmcopy 最终 sidecar 都受它约束。 |
| `preserve_trx_temp_table_enable` | `ON` | `ON`/`OFF` | global | 开启受支持的用户 InnoDB 临时表 preserve/resume。 |
| `preserve_trx_max_temp_sidecar_bytes` | `1073741824` | `1..1073741824` 字节 | global | 单个临时表 image 或 undo sidecar 最大 1 GiB。 |

这些都是 artifact/文件尺寸限制，不是堆内存上限。

### 4.5 通用内存和 spill

| 参数 | 默认值 | 范围/单位 | 作用域 | 含义 |
|---|---:|---|---|---|
| `preserve_trx_memory_budget_bytes` | `268435456` | `4096..ULLONG_MAX` 字节 | global | Preserve/Resume 通用 heap lease 总预算，默认 256 MiB。 |
| `preserve_trx_memory_per_token_bytes` | `67108864` | `4096..ULLONG_MAX` 字节 | global | 单个 token 的通用 heap lease 预算，默认 64 MiB。 |
| `preserve_trx_spill_chunk_bytes` | `4194304` | `4096..67108864` 字节 | global | 大 artifact 流式写入 spill/backend 时的 scratch chunk，默认 4 MiB。 |

这组参数不控制 warm external blob 总大小。它控制通用 Preserve/Resume 内存租约
和 spill 行为，也不是 record/table lock 导出数量的第一控制面。锁数量由
`preserve_trx_max_lock_count` 控制；binlog warmcopy 总量由
`preserve_trx_warmcopy_max_total_bytes` 控制。

### 4.6 lock、modified table 和 materialization

| 参数 | 默认值 | 范围/单位 | 作用域 | 含义 |
|---|---:|---|---|---|
| `preserve_trx_max_lock_count` | `2000` | `0..UINT_MAX32` 个 | global | preserve 可 materialize/export 的 record/table lock 数量上限。 |
| `preserve_trx_max_modified_tables` | `64` | `0..UINT_MAX32` 个 | global | preserve 为 implicit lock 扫描/导出的 modified InnoDB tables 上限。 |
| `preserve_trx_max_scan_pages` | `20000` | `0..UINT_MAX32` 页 | global | materialize implicit locks 时最多扫描的 clustered index pages。 |
| `preserve_trx_materialize_timeout_ms` | `5000` | `0..UINT_MAX32` 毫秒 | global | materialize implicit locks 的最大耗时。 |

这些参数用于把事务可见的锁语义转成可恢复的 durable contract。上限过小会导致
事务被拒绝 preserve；上限调大后，代码允许扫描更多页、导出更多锁，preserve 阶段
耗时上界和 snapshot payload 上界也随之扩大。

明确规则：

- `preserve_trx_max_lock_count` 是单个事务的共享锁数量预算，不是全实例预算。
  record locks、materialized implicit record locks、table locks 使用同一个预算。
  table lock 导出时会扣除已经导出的 record lock 数；两者合计超过该值会拒绝。
- `preserve_trx_max_modified_tables` 也是单个事务上限。它同时用于 modified table
  name 导出和 implicit lock materialization 前的扫描准入。事务修改表数超过该值
  时 fail closed。
- `preserve_trx_max_scan_pages` 和 `preserve_trx_materialize_timeout_ms` 只控制
  implicit lock materialization。已经显式存在的 record/table lock 仍受
  `preserve_trx_max_lock_count` 控制。
- 把这些值调大只能放宽数量、扫描页数和耗时限制。它不能让 unsupported lock mode、
  spatial predicate lock、online DDL index、临时表/ibd 缺失等不支持形态变成可导出。
- 锁 payload 最终会进入 snapshot metadata。大幅调高
  `preserve_trx_max_lock_count` 后，如果导出的锁 payload 使 snapshot 超过
  `preserve_trx_max_snapshot_bytes`，失败点会从 lock count 限制转移为 snapshot
  size 限制。

### 4.7 drain 控制

| 参数 | 默认值 | 范围/单位 | 作用域 | 含义 |
|---|---:|---|---|---|
| `preserve_trx_drain_mode` | `SOFT` | `SOFT`/`HARD` | global | batch drain 模式。`SOFT` 先等待，超时后升级；`HARD` 直接杀其他活跃用户事务。 |
| `preserve_trx_drain_grace_ms` | `30000` | `0..UINT_MAX32` 毫秒 | global | SOFT 模式等待活跃事务自然结束的时间。 |
| `preserve_trx_drain_hard_timeout_ms` | `30000` | `1..UINT_MAX32` 毫秒 | global | HARD 模式等待活跃事务消失的最长时间。 |

如果 HARD deadline 到期仍有无法 drain 的事务，drain 失败。

### 4.8 binlog warmcopy

| 参数 | 默认值 | 范围/单位 | 作用域 | 含义 |
|---|---:|---|---|---|
| `preserve_trx_warmcopy_enable` | `ON` | `ON`/`OFF` | global | batch drain 中启用 binlog cache 两阶段 warmcopy。 |
| `preserve_trx_warmcopy_min_open_ms` | `1000` | `0..UINT_MAX32` 毫秒 | global | warmcopy drain phase 1 至少保持 open admission 的时间。 |
| `preserve_trx_warmcopy_close_timeout_ms` | `30000` | `0..UINT_MAX32` 毫秒 | global | 关闭 phase 1 admission 并等待 mirror 收敛的最长时间。 |
| `preserve_trx_warmcopy_chunk_bytes` | `1048576` | `1..UINT_MAX32` 字节 | global | phase 1 prefix copy 外层循环的目标 chunk，默认 1 MiB。 |
| `preserve_trx_warmcopy_tail_budget_bytes` | `1048576` | `0..UINT_MAX32` 字节 | global | phase 1 copied prefix 之后允许的 phase 2 tail 字节数，默认 1 MiB。 |
| `preserve_trx_warmcopy_max_total_bytes` | `10737418240` | `1..ULLONG_MAX` 字节 | global | 单个 drain epoch 可接收的 warm external blob 总预算，默认 10 GiB。 |
| `preserve_trx_warmcopy_pending_range_limit` | `1024` | `0..UINT_MAX32` 个 | global | 单 participant 允许保留的 out-of-order mirror ranges 数。 |
| `preserve_trx_warmcopy_pending_bytes_limit` | `67108864` | `0..ULLONG_MAX` 字节 | global | 单 participant 允许保留的 out-of-order mirror payload 字节数，默认 64 MiB。 |
| `preserve_trx_single_phase_max_binlog_cache_bytes` | `ULLONG_MAX` | `0..ULLONG_MAX` 字节 | global | 没有 warmcopy provider 时允许单阶段完整复制的 logged binlog cache 上限。 |

warmcopy 生效条件：

- `preserve_trx_warmcopy_enable=ON`
- global binlog open，即 `opt_bin_log && mysql_bin_log.is_open()`
- 源 session 的 `sql_log_bin` 为 ON
- 事务有可 warmcopy 的 transaction binlog cache

如果 global binlog 未开启、session `sql_log_bin=OFF`、transaction binlog cache 为空，
该事务没有 warmcopy blob，也不需要复制 binlog cache sidecar。只有存在 logged
transaction binlog cache 但没有可用 warmcopy provider 时，preserve 才走
single-phase copy，并受 `preserve_trx_single_phase_max_binlog_cache_bytes` 和
`preserve_trx_max_binlog_cache_bytes` 同时约束。

### 4.9 lock warmcopy

lock warmcopy 与 binlog warmcopy 独立。`preserve_trx_warmcopy_enable` 只控制 binlog
cache warmcopy；锁语义 warmcopy 由下面这组参数控制。

| 参数 | 默认值 | 范围/单位 | 作用域 | 含义 |
|---|---:|---|---|---|
| `preserve_trx_lock_warmcopy_enable` | `ON` | `ON`/`OFF` | global | 启用锁语义 warmcopy。当前目标默认是 ON；发布前仍以完整测试和 NFR 门禁为准。 |
| `preserve_trx_lock_warmcopy_fallback_to_live_export` | `ON` | `ON`/`OFF` | global | lock warmcopy artifact 无效时，是否丢弃该 target 的全部 warmcopy artifact 并回退现有 live export。关闭时 fail closed/reject。 |
| `preserve_trx_lock_warmcopy_max_memory_bytes` | `268435456` | `1..ULLONG_MAX` 字节 | global | lock warmcopy artifact payload 在内存中的保留预算；超出后优先 spill artifact。它不是 mysqld 堆内存总上限，也不覆盖 record store、journal、candidate 采集成本。 |
| `preserve_trx_lock_warmcopy_max_journal_bytes` | `1073741824` | `1..ULLONG_MAX` 字节 | global | 单次 drain epoch lock warmcopy journal 字节上限。 |
| `preserve_trx_lock_warmcopy_max_dirty_shards` | `100000` | `0..UINT_MAX32` 个 | global | phase 2 可重验 dirty record shards 上限。 |
| `preserve_trx_lock_warmcopy_max_mdl_descriptors` | `100000` | `0..UINT_MAX32` 个 | global | MDL transaction-duration descriptors 上限。 |
| `preserve_trx_lock_warmcopy_seal_threads` | `0` | `0..1024` | global | 预留参数；当前实现按 target 串行 seal，保持 0。 |
| `preserve_trx_parallel_preserve_threads` | `0` | `0..1024` | global | batch drain phase 2 target preserve worker 数。`0=auto`，`1=legacy serial`，大于 1 时允许并行 preserve 目标事务；只有 lock warmcopy batch drain 会使用自动并行，普通单事务 preserve 不依赖它。 |
| `preserve_trx_lock_warmcopy_conversion_wait_timeout_ms` | `30000` | `0..UINT_MAX32` 毫秒 | global | 其它会话撞到目标事务 conversion freeze 后，释放 latch/mtr 并等待 freeze 清除再重试的上限；实际等待还受 drain deadline、KILL 和 shutdown 约束。 |

当前实现的真实边界：

- phase 2 artifact 中的 `record_locks_payload` 来自 lock warmcopy record store 的 sealed
  payload；live export 只用于 canonical equivalence comparator 和 fallback。
- target quiesce 时，当前 live record export 会作为 base seed 写入 record warmcopy
  store；如果 phase 1 中已经观察到该 target 的 record-lock delta，seed 后对应 shard
  会被视为 dirty，受 `preserve_trx_lock_warmcopy_max_dirty_shards` 限制。无法形成可验证
  base shard 时，target 会 invalid 并按 fallback/reject 处理。
- 如果 record/table/MDL 任一 required family invalid，fallback 开启时按 per-target
  all-or-live 回退，关闭时 reject，不能混用同一个 target 的部分 warmcopy 和部分 live
  payload。
- R-tree / spatial predicate locks 不做 lock warmcopy 优化；fallback 开启时走现有 live
  export/import 的 `predicate_locks_payload`，关闭时 reject。
- 当前尚未实现 exact implicit coverage store 时，不应把 `implicit_native_validated` 当作
  true 能力；显式 record payload 使用 warmcopy，implicit record X-lock 依赖 preserved
  trx id 和 InnoDB 原生 running-trx 连续性。

## 5. warmcopy 资源和内存模型

warmcopy 的关键结论：`preserve_trx_warmcopy_max_total_bytes` 是 warm external
blob 的总预算，不是堆内存预算。

每个 participant 的有效 entry limit 由两个上限共同决定：

```text
entry_limit = min(
  preserve_trx_max_binlog_cache_bytes,
  preserve_trx_warmcopy_max_total_bytes - already_reserved_for_other_entries
)
```

provider 预留的大小近似为：

```text
reservation = prefix_bytes + preserve_trx_warmcopy_tail_budget_bytes
```

如果一批 participant 的 prefix size 相同，phase 1 admission 的上限可以按下式估算：

```text
max_admitted_by_reservation =
  floor(preserve_trx_warmcopy_max_total_bytes /
        (prefix_bytes + preserve_trx_warmcopy_tail_budget_bytes))
```

例如默认总预算 10 GiB、默认 tail budget 1 MiB、每个 participant copied prefix
正好 1 GiB 时，`floor(10 GiB / (1 GiB + 1 MiB)) = 9`。第 10 个 participant
在 phase 1 reservation 阶段会被拒绝，而不是等到真正分配 10 GiB 堆内存后失败。

最终 finalize 时还会用实际 sidecar size 重新检查：

- `actual_size <= preserve_trx_max_binlog_cache_bytes`
- `actual_size <= preserve_trx_warmcopy_max_total_bytes`
- aggregate total 不超过 `preserve_trx_warmcopy_max_total_bytes`

因此：

- `preserve_trx_warmcopy_max_total_bytes` 控制一个 drain epoch 可接收的 warm
  blob artifact 总量。
- `preserve_trx_max_binlog_cache_bytes` 控制单事务 preserved binlog sidecar
  上限。
- 即使把 `preserve_trx_warmcopy_max_total_bytes` 设置成 1 TiB，也不会预分配
  1 TiB 堆内存。
- 但它会允许更多/更大的 warm blob 被接收，从而增加磁盘空间、磁盘 IO、
  phase 1 复制时间、参与者数量，以及大量并发 participant 的元数据和 pending
  range 聚合风险。

phase 1 copy 的内存特点：

- 外层循环每次最多按 `preserve_trx_warmcopy_chunk_bytes` 复制。
- 底层 `IO_CACHE_binlog_cache_storage::copy_range_to()` 对磁盘部分使用 8192
  字节 buffer。
- 已在 source binlog cache 写 buffer 里的内容直接从已有 buffer 读，不再为
  整个 binlog cache 分配同等大小的新堆内存。

mirror/finalize 的堆内存风险主要来自：

- 每个 `Mysql_binlog_warmcopy_session` 的元数据、writer、digest context、
  watermarks。
- out-of-order mirror writes 的
  `std::map<uint64_t, std::string> m_pending_ranges`。
- `preserve_trx_warmcopy_pending_range_limit` 和
  `preserve_trx_warmcopy_pending_bytes_limit` 对单 participant pending payload
  做上限保护。

## 6. 场景说明

### 6.1 单连接维护前保存一个大事务

适合场景：应用有一个明确的长事务，维护窗口需要重启 mysqld，但不希望直接
回滚该事务。

流程：

```sql
START TRANSACTION;
UPDATE orders SET status = 'processing' WHERE id = 1001;
PREPARE SHUTDOWN PRESERVE TRANSACTION WITH TIMEOUT 3600;
```

重启后：

```sql
RESUME PRESERVED TRANSACTION '<token>';
COMMIT;
```

需要关注：

- token timeout 要覆盖预计停机和人工恢复时间。
- 如果事务包含很大的 binlog cache，需要调大
  `preserve_trx_max_binlog_cache_bytes`。
- 如果使用用户变量并希望恢复后也保留，必须加 `WITH USER VARS`。

### 6.2 停机前批量保存空闲业务事务

适合场景：服务上有多条长连接，部分连接持有空闲事务。DBA 希望进入停机前
批量保存这些事务。

```sql
DRAIN TRANSACTIONS PRESERVE WITH TIMEOUT 1800 WITH USER VARS;
```

需要关注：

- `preserve_trx_batch_max_transactions` 控制单次 drain 可保存多少事务。
- `preserve_trx_max_total` 和 `preserve_trx_max_pending_per_user` 控制全局和
  单账号 token 容量。
- `preserve_trx_drain_mode=SOFT` 更温和；`HARD` 会杀其他活跃事务，适合更强
  的停机收敛要求。

### 6.3 1000 个正在执行的大写事务，binlog 打开

普通执行阶段的内存/磁盘由 MySQL 自身 binlog cache 行为主导；Preserve/Resume
尚未介入。1000 个正在执行的写事务本身已经会产生 1000 份事务 binlog cache，
这些 cache 的内存 buffer、临时文件和 `max_binlog_cache_size` 行为属于 MySQL
原有 binlog cache 机制，不由 `preserve_trx_warmcopy_max_total_bytes` 控制。

执行 `DRAIN TRANSACTIONS PRESERVE` 后：

- batch drain 只 preserve quiesced/idle target。正在执行 SQL 的事务在 `SOFT`
  模式下先等待自然变为空闲，超过 grace 后升级到 `HARD`；`HARD` 会 kill 活跃用户
  事务，因此被 kill 的事务不会生成 preserved token。
- eligible participant 会进入 warmcopy phase 1，把已存在的 binlog cache prefix
  流式复制到 warm external blob。
- 默认 `preserve_trx_warmcopy_max_total_bytes=10 GiB`，
  `preserve_trx_max_binlog_cache_bytes=1 GiB`，
  `preserve_trx_warmcopy_tail_budget_bytes=1 MiB`。如果每个事务 phase 1 prefix
  正好 1 GiB，按 reservation 公式只能接收 9 个 participant，第 10 个会耗尽
  warmcopy 总预算并 fail closed。若每个 prefix 小于 1 GiB，最大接收数按
  `floor(10 GiB / (prefix + 1 MiB))` 计算。
- 后续 participant 会被拒绝，drain fail closed。
- 这不会变成 `1000 * binlog_cache_size` 的 warmcopy 堆内存占用；主要新增堆
  风险是每 session 元数据和 out-of-order pending ranges。
- 但磁盘 IO、preserve directory 空间、phase 1 时长和参与者管理开销会显著
  增加。
- binlog 预算通过后，每个事务仍要单独通过 InnoDB 语义导出：record/table lock
  合计不能超过 `preserve_trx_max_lock_count`，modified table 数不能超过
  `preserve_trx_max_modified_tables`，implicit lock materialization 不能超过
  `preserve_trx_max_scan_pages` 和 `preserve_trx_materialize_timeout_ms`。
- 锁、read view、savepoint、MDL、临时表 manifest 都会进入 snapshot metadata。
  如果这些 payload 的序列化结果超过 `preserve_trx_max_snapshot_bytes`，preserve
  会在 bundle/snapshot 写入前 fail closed。

如果要支持 1000 个大事务，需要同时评估：

- `preserve_trx_batch_max_transactions`
- `preserve_trx_max_total`
- `preserve_trx_max_pending_per_user`
- `preserve_trx_max_lock_count`
- `preserve_trx_max_modified_tables`
- `preserve_trx_max_scan_pages`
- `preserve_trx_materialize_timeout_ms`
- `preserve_trx_max_snapshot_bytes`
- `preserve_trx_max_binlog_cache_bytes`
- `preserve_trx_warmcopy_max_total_bytes`
- preserve directory 可用空间
- drain timeout 和 warmcopy close timeout
- pending range 上限

### 6.4 单事务有 10 GiB binlog event/cache

只把 `preserve_trx_warmcopy_max_total_bytes` 调到 1 TiB 不够。默认
`preserve_trx_max_binlog_cache_bytes=1 GiB` 会先拒绝单个 sidecar。

需要至少：

```sql
SET GLOBAL preserve_trx_max_binlog_cache_bytes = 10737418240;
SET GLOBAL preserve_trx_warmcopy_max_total_bytes = 1099511627776;
```

含义：

- 第一个参数允许单 token 的 binlog cache sidecar 到 10 GiB。
- 第二个参数允许一个 drain epoch 总 warm blob 到 1 TiB。
- 两者都不会预分配同等大小堆内存。
- 但实际会产生对应规模的文件、IO 和耗时。
- 如果 phase 1 high-water mark 之后事务继续写入超过
  `preserve_trx_warmcopy_tail_budget_bytes`，即使总预算足够，phase 2 也会因为
  tail 超限而 fail closed。

### 6.5 希望 phase 1 搬迁 buffer 更小，例如 128 KiB

```sql
SET GLOBAL preserve_trx_warmcopy_chunk_bytes = 131072;
```

这只改变 phase 1 外层 copy loop 的目标 chunk。它不会减少需要复制的总字节数，
也不会把总预算变成内存预算。chunk 更小意味着单次 copy burst 更小；在同等数据量
下循环次数增加，调度和函数调用次数增加，总耗时不能因此变短。

### 6.6 无 binlog 或 session `sql_log_bin=OFF`

binlog state 会归类为以下四种：

- `GLOBAL_OFF_NO_CACHE`：global binlog 未打开。
- `SESSION_OFF_NO_CACHE`：global binlog 打开，但 session 不写 binlog。
- `LOGGED_EMPTY`：binlog 打开且 session 写 binlog，但事务 cache 为空。
- `LOGGED_WITH_CACHE`：事务有 logged binlog cache，需要 sidecar 或 warmcopy。

无 binlog cache 时，不涉及 warmcopy 大 blob 预算，但仍需要保存 InnoDB、锁、
read view、MDL、savepoint 等事务状态。

no-cache binlog state 还要求当前 GTID state clean：`GTID_NEXT` 必须是
`AUTOMATIC`，不能有 `GTID_NEXT_LIST`，也不能已经持有 anonymous/assigned GTID。
否则 preserve 会拒绝，避免恢复后生成错误或重复的 GTID 语义。

### 6.7 用户变量

默认或 `WITH NO USER VARS` 不导出用户变量。

`WITH USER VARS` 会导出当前 session 的用户变量集合，支持
`STRING_RESULT`、`REAL_RESULT`、`INT_RESULT`、`DECIMAL_RESULT`。恢复时 token 中
的用户变量 payload 会导入新 session。

注意：

- 如果 preserve 时使用 `WITH USER VARS`，即使当时用户变量集合为空，也会记录
  一个空集合 payload；resume 会把当前 session 的用户变量替换成这个空集合。
- 如果没有使用 `WITH USER VARS`，payload 为空，resume 不会导入用户变量。
- 不支持的用户变量类型、非法 charset/collation/decimal payload 会导致
  preserve 或 resume fail closed。

### 6.8 用户临时表

`preserve_trx_temp_table_enable=ON` 时，支持的用户 InnoDB temporary table 状态
会通过 temp sidecar 保存。

需要关注：

- 当前实现只接受事务型用户临时表，即内部标记为 `TRANSACTIONAL_TMP_TABLE` 的
  temporary table。非事务型临时表或元数据不可序列化的临时表会拒绝。
- preserve 前会检查临时表 schema/table name、InnoDB source metadata、DD metadata
  clone、column/index metadata 是否可支持；任一项不可导出都会 fail closed。
- 单个 temp image/undo sidecar 受 `preserve_trx_max_temp_sidecar_bytes` 限制。
- 已存在用户 InnoDB 临时表的事务型 DML 可以通过 physical image、dirty page
  overlay 和 no-redo undo sidecar 保持事务语义；resume 后继续 `COMMIT` 或
  `ROLLBACK` 时，临时表和持久表的可见性应与未 preserve 的事务一致。
- 用户临时表 DDL/元数据变更仍不支持：`CREATE/DROP/TRUNCATE/ALTER/RENAME
  TEMPORARY TABLE`、无法完整捕获的 savepoint/statement rollback 交叠、缺失或
  损坏的 no-redo undo sidecar，都会在 preserve 或 resume 阶段 fail closed。
- sidecar 写入、digest、bootstrap 读取、materialize for resume 任一步缺失或损坏，
  都会 fail closed；bootstrap IO 失败属于启动恢复严重故障路径，不能当作可忽略
  的普通 unsupported token。
- 如果 resume 时临时表功能被动态关闭，相关 token 会按临时表 unsupported 路径拒绝
  当前 resume；功能重新开启且 sidecar 可 materialize 后，才能继续恢复。

### 6.9 跨账号恢复

默认只能恢复自己账号生成的 token。跨账号恢复需要：

```sql
GRANT RESUME_ANY_PRESERVED_TRANSACTION ON *.* TO 'u'@'h';
```

即便有该动态权限，resume 仍会重新检查对象权限：

- MDL 描述涉及的库、表、例程、trigger、tablespace 等权限。
- modified tables 的写权限。
- 如果权限不满足，resume fail closed。

## 7. 约束和不支持场景

### 7.1 上下文限制

以下上下文不支持 preserve/drain/resume：

- 非 classic protocol。
- server initialize、bootstrap system thread、server upgrade thread。
- `innodb_force_recovery > 0`。
- replication SQL/thread、binlog applier、group replication running、存在已配置
  replica channel。
- 当前 THD 的 XA 状态不是 `XA_NOTR`。
- data dictionary update statement。
- `LOCK TABLES` 模式。
- 用户级 locks 存在。
- open HANDLER tables 存在。
- open server-side cursor 存在。
- sub-statement 或 stored routine runtime context。

### 7.2 事务内容限制

不支持：

- 修改过非事务表。
- read-write handlerton 参与者不是 InnoDB，也不是合法 binlog participant。
- 超过 lock/materialization 上限。
- 对象权限 recheck 失败。
- binlog cache 或 temp sidecar 超过配置上限。
- binlog GTID / log-bin 模式与当前实例配置不兼容。
- no-cache binlog preserve 时 GTID state 不 clean，例如 `GTID_NEXT` 非
  `AUTOMATIC` 或当前 THD 已持有 GTID。

Preserve 支持保存 read view、savepoints、record locks、table locks、MDL 等
事务语义状态。限制不在于这些能力本身不支持，而在于当前事务的这些状态必须能
被当前实现成功导出、序列化并在恢复路径校验。如果某个事务的状态超过配置上限、
包含暂不支持的锁/对象形态，或者导出/权限校验失败，则该事务不能生成 durable
token，本次 preserve/drain 会 fail closed。

“不能成功导出”不是随机概率事件，而是当前实现的确定性能力边界。相同事务内容、
相同配置、相同对象状态且没有并发对象形态变化时，会按同一检查点稳定成功或稳定
失败。具体包括：

- read view：没有 active read view 不是失败，会成功导出为空 payload。失败条件是
  内部 read view 状态无法被 `MVCC::preserve_export_view()` 转成 durable
  snapshot，或恢复路径校验发现 snapshot 与当前 purge/recovery 状态不兼容。
- SQL savepoint：savepoint 名为空或过长、savepoint handler 不是 InnoDB/binlog、
  binlog savepoint 需要 cache 但事务不是 `LOGGED_WITH_CACHE`、binlog cache
  checkpoint 查询失败、MDL savepoint ordinal 无法导出，都会拒绝。
- MDL：只导出 transaction-duration 的受支持 MDL。存在 statement-duration 或
  explicit-duration MDL ticket、MDL namespace/type 不支持、key 为空或超长、payload
  ordinal 校验不一致时，拒绝 preserve。
- modified table：事务修改的 InnoDB 表数超过
  `preserve_trx_max_modified_tables`，或表名/schema 无法可靠导出时，拒绝 preserve。
- implicit record locks：materialization 需要扫描 modified tables/indexes。超过
  `preserve_trx_max_scan_pages`、超过 `preserve_trx_materialize_timeout_ms`、超过
  `preserve_trx_max_lock_count`、遇到临时表、缺失 ibd、spatial clustered index、
  未 committed 或 online DDL 中的 secondary index，都会拒绝。
- record locks：等待中的 record lock、unsupported record lock mode、spatial
  predicate lock、predicate payload 导出失败、record lock bit 数超过
  `preserve_trx_max_lock_count`，都会拒绝。
- table locks：只支持 `IS`、`IX`、`S`、`X`、`AUTO_INC`。等待中的 table lock、
  unsupported table lock mode、table identity 异常、record lock 与 table lock 合计
  超过 `preserve_trx_max_lock_count`，都会拒绝。
- AUTO_INC：preserve 会交叉检查事务是否持有 autoinc lock 与 table-lock payload
  是否一致。如果捕获和导出之间状态不一致，说明事务锁状态发生变化，拒绝 preserve。
- 权限和对象状态：MDL 对象、modified tables、resume owner 权限需要重新校验。
  权限变化、对象形态变化或恢复账号不满足 owner/权限规则时 fail closed。

按事务形态判断：

- 普通 InnoDB OLTP 小中事务，如果满足以下条件，属于当前代码的主要支持路径：只修改
  InnoDB 事务表；record/table lock 合计不超过 `preserve_trx_max_lock_count`；
  modified tables 不超过 `preserve_trx_max_modified_tables`；implicit lock
  materialization 未超过页数/耗时限制；没有 `LOCK TABLES`、HANDLER、open cursor、
  user lock、unsupported MDL、unsupported lock mode、unsupported temp table 状态；
  binlog cache、temp sidecar、snapshot 均未超过尺寸上限。
- 大批量写事务、一次修改很多行或很多表、持有大量隐式锁、使用 spatial/FTS/online
  DDL 相关对象、持有特殊 MDL/LOCK TABLES/HANDLER/cursor/user lock，会触发上面的确定性
  检查点之一；命中即 fail closed。
- 如果失败原因是数量上限，调大对应参数可以放宽该数量限制；如果失败原因是不支持
  形态，调大参数不会改变代码能力边界。

### 7.3 命令级限制

单事务 preserve：

- 必须在当前 session 有显式活跃事务。
- 不能在 prepared statement 非 regular execution 上下文中使用。

batch drain：

- 控制 session 不能有显式活跃事务。
- drain 期间会阻止新的高风险语句；部分 session 会被标记为 drained。

resume：

- 当前 session 不能已经处于多语句事务中。
- 当前 session 必须能接受 preserved transaction。
- `read_only`/`super_read_only` 下拒绝。
- token 过期会触发 rollback，而不是恢复。

### 7.4 warmcopy 特定限制

warmcopy fail closed 的常见原因：

- copied prefix 超过 session blob limit。
- final binlog cache size 超过 `preserve_trx_max_binlog_cache_bytes`。
- aggregate warm blob accounting 超过
  `preserve_trx_warmcopy_max_total_bytes`。
- phase 1 后新增 tail 超过
  `preserve_trx_warmcopy_tail_budget_bytes`。
- out-of-order pending ranges 超过 range 或 bytes 上限。
- source binlog cache truncate generation 变化。
- close timeout 到期。
- provider entry 缺失或 finalization invariant 不满足。

## 8. 可观测性

### 8.1 P_S 表字段

`performance_schema.preserved_transactions` 字段：

| 字段 | 含义 |
|---|---|
| `TOKEN` | resume 使用的 token，最长 128。 |
| `USER`, `HOST` | token owner。 |
| `STATE` | 生命周期状态。 |
| `CREATED_AT`, `EXPIRES_AT`, `RECOVERED_COUNT`, `AGE_SECONDS` | 创建、过期、startup recovery 次数和年龄。 |
| `SCHEMA_NAME`, `ISOLATION` | preserve 时 session schema 和隔离级别。 |
| `MOD_TABLES_COUNT`, `LOCKS_COUNT`, `HAS_READ_VIEW`, `RV_LOW_LIMIT_NO` | InnoDB 可恢复语义摘要。 |
| `SAVEPOINT_COUNT` | SQL/InnoDB savepoint 数。 |
| `BINLOG_STATE` | binlog preserve 状态。 |
| `WROTE_TO_CACHE`, `BINLOG_CACHE_SIZE`, `BINLOG_WARMCOPY_STATE` | binlog cache 和 warmcopy 状态。 |
| `SESSION_SQL_LOG_BIN`, `GLOBAL_LOG_BIN`, `GTID_NEXT` | binlog/GTID 模式。 |
| `AUTOINC_LOCK_OWNED` | 是否持有 autoinc lock。 |
| `TEMP_TABLE_STATE`, `TEMP_IMAGE_BYTES`, `TEMP_UNDO_BYTES`, `TEMP_SIDECARS_COMPLETE` | 临时表 sidecar 状态。 |
| `LAST_ERROR`, `LAST_ERROR_AT` | 最近错误。 |

`STATE` 可能值包括：

- `DRAINING`
- `SNAPSHOTTING`
- `PRESERVED`
- `RESUMING`
- `ROLLING_BACK`
- `EXPIRED_ROLLBACK`
- `EXPIRED_CLEANUP_FAILED`
- `FAILED`

`BINLOG_STATE` 可能值包括：

- `GLOBAL_OFF_NO_CACHE`
- `SESSION_OFF_NO_CACHE`
- `LOGGED_EMPTY`
- `LOGGED_WITH_CACHE`

`BINLOG_WARMCOPY_STATE`：

- `NONE`：没有 logged binlog cache payload。
- `READY`：使用 warmcopy prebuilt binlog cache sidecar。
- `NOT_USED`：有 logged binlog cache，但不是 warmcopy 生成。

`TEMP_TABLE_STATE`：

- `NONE`：无临时表 sidecar。
- `READY`：临时表 sidecar 完整。
- `INCOMPLETE`：sidecar 不完整。
- `CORRUPT`：manifest 损坏。

### 8.2 SHOW STATUS

```sql
SHOW GLOBAL STATUS LIKE 'Preserve_trx_warmcopy_%';
SHOW GLOBAL STATUS LIKE 'Preserve_trx_lock_warmcopy_%';
SHOW GLOBAL STATUS LIKE 'Preserve_trx_memory_%';
SHOW GLOBAL STATUS LIKE 'Preserve_trx_spill_%';
```

| 状态变量 | 含义 |
|---|---|
| `Preserve_trx_warmcopy_prefix_bytes` | phase 1 prefix copy 字节数。 |
| `Preserve_trx_warmcopy_digest_bytes` | warmcopy digest 处理字节数。 |
| `Preserve_trx_warmcopy_durable_bytes` | 推进到 warmcopy durable watermark 的字节数。 |
| `Preserve_trx_warmcopy_phase2_pause_us` | warmcopy phase 2 finalize 耗时，单位微秒。 |
| `Preserve_trx_warmcopy_provider_full_copy_to_count` | 没有 warmcopy provider 时 fallback full-copy 次数。 |
| `Preserve_trx_memory_current_bytes` | 当前通用 Preserve/Resume heap leases。 |
| `Preserve_trx_memory_peak_bytes` | 通用 heap leases 峰值。 |
| `Preserve_trx_spill_bytes` | 通用 spill 写入字节数。 |
| `Preserve_trx_spill_failures` | 通用 spill 失败次数。 |

lock warmcopy 状态变量是进程生命周期累计口径，不是单次 drain report：

| 状态变量 | 含义 |
|---|---|
| `Preserve_trx_lock_warmcopy_attempts` | lock warmcopy target 尝试次数。 |
| `Preserve_trx_lock_warmcopy_sealed_valid` | seal 后可用的 warmcopy artifact 次数。 |
| `Preserve_trx_lock_warmcopy_sealed_invalid` | seal/validation 后 artifact invalid 次数。 |
| `Preserve_trx_lock_warmcopy_live_fallback` | warmcopy invalid 后回退 live export 次数。 |
| `Preserve_trx_lock_warmcopy_strict_reject` | fallback 关闭或不可回退时 strict reject 次数。 |
| `Preserve_trx_lock_warmcopy_artifact_bytes` | 已生成/计入的 lock warmcopy artifact payload 字节。 |
| `Preserve_trx_lock_warmcopy_journal_bytes` | lock warmcopy journal 字节累计。 |
| `Preserve_trx_lock_warmcopy_spill_bytes` | lock warmcopy spill 写入字节累计。 |
| `Preserve_trx_lock_warmcopy_spill_failures` | lock warmcopy spill 失败次数。 |
| `Preserve_trx_lock_warmcopy_canonical_mismatch` | warmcopy payload 与 live export canonical comparator 不等价次数。 |
| `Preserve_trx_lock_warmcopy_conversion_freeze_waits` | 其它会话因目标事务 conversion freeze 等待/重试次数。 |
| `Preserve_trx_lock_warmcopy_dirty_shards` | seal 过程中记录的 dirty shards 累计。 |
| `Preserve_trx_lock_warmcopy_final_fence_mismatch` | final fence recheck 不一致次数。 |
| `Preserve_trx_lock_warmcopy_phase2_pause_us` | lock warmcopy phase 2 pause 累计微秒。 |
| `Preserve_trx_lock_warmcopy_resource_limit` | 资源预算导致 invalid/reject 的次数。 |
| `Preserve_trx_lock_warmcopy_unsupported_family` | 不支持锁族导致 invalid/reject 的次数。 |

这些指标要分开看：`Preserve_trx_warmcopy_prefix_bytes` 很大，不代表
`Preserve_trx_memory_current_bytes` 同样大，因为 prefix bytes 是 external blob
payload，不是 heap lease。

## 9. 调参建议

### 9.1 想降低内存风险

优先控制：

- `preserve_trx_warmcopy_pending_range_limit`
- `preserve_trx_warmcopy_pending_bytes_limit`
- `preserve_trx_lock_warmcopy_max_memory_bytes`
- `preserve_trx_lock_warmcopy_max_journal_bytes`
- `preserve_trx_lock_warmcopy_max_dirty_shards`
- `preserve_trx_memory_budget_bytes`
- `preserve_trx_memory_per_token_bytes`
- `preserve_trx_batch_max_transactions`

不要把 `preserve_trx_warmcopy_max_total_bytes` 当成内存上限。它主要是磁盘
artifact admission 上限。也不要把 `preserve_trx_lock_warmcopy_max_memory_bytes`
理解成 mysqld 堆内存总上限；它约束 lock warmcopy 最终 artifact payload 在内存中的
保留量。

### 9.2 想支持更大的 binlog cache

同时评估：

- 单事务上限：`preserve_trx_max_binlog_cache_bytes`
- batch 总 warmcopy 上限：`preserve_trx_warmcopy_max_total_bytes`
- single-phase 上限：`preserve_trx_single_phase_max_binlog_cache_bytes`
- preserve directory 可用空间
- phase 1 复制耗时和 drain timeout

### 9.3 想让停机 drain 更快收敛

可以考虑：

- `preserve_trx_drain_mode=HARD`
- 降低 `preserve_trx_drain_grace_ms`
- 合理设置 `preserve_trx_drain_hard_timeout_ms`
- 降低 batch 一次接收的事务数，分批处理

代价是只要 HARD drain 发现其他活跃用户事务并执行 kill，被 kill 的事务就会走原有
回滚/重试路径，不会生成 preserved token；应用必须能处理这类回滚。

### 9.4 想减少 phase 1 单次 IO burst

```sql
SET GLOBAL preserve_trx_warmcopy_chunk_bytes = 131072;
```

这会把外层 copy chunk 调到 128 KiB。总复制量不变；单次 copy 调用目标更小，循环
次数增加，因此不能用它缩短总复制时间。

### 9.5 想提高 1000 个大事务的 preserve 成功率

不能只调一个参数。需要同时覆盖四类限制：事务数量、锁导出、snapshot/artifact
大小、drain/warmcopy 时间窗口。

事务数量准入：

- `preserve_trx_batch_max_transactions`
- `preserve_trx_max_total`
- `preserve_trx_max_pending_per_user`

这三个参数控制“能接收多少个事务”。默认都是 256，不能支撑一次 drain 1000 个
目标事务。它们不控制单个事务内部有多少 record/table lock。

锁导出和 materialization：

- `preserve_trx_max_lock_count`
- `preserve_trx_max_modified_tables`
- `preserve_trx_max_scan_pages`
- `preserve_trx_materialize_timeout_ms`

这四个参数控制“单个事务的 InnoDB 并发语义能不能导出”。大事务常见失败点是
record/table lock 合计超过 `preserve_trx_max_lock_count`，或者 implicit lock
materialization 扫描页数/耗时超过上限。

snapshot、binlog 和 warmcopy artifact：

- `preserve_trx_max_snapshot_bytes`
- `preserve_trx_max_binlog_cache_bytes`
- `preserve_trx_warmcopy_max_total_bytes`
- `preserve_trx_single_phase_max_binlog_cache_bytes`
- `preserve_trx_warmcopy_tail_budget_bytes`
- `preserve_trx_warmcopy_close_timeout_ms`
- `preserve_trx_lock_warmcopy_max_memory_bytes`
- `preserve_trx_lock_warmcopy_max_journal_bytes`
- `preserve_trx_lock_warmcopy_max_dirty_shards`
- `preserve_trx_lock_warmcopy_max_mdl_descriptors`
- `preserve_trx_drain_grace_ms`
- `preserve_trx_drain_hard_timeout_ms`

这些参数控制 metadata、binlog sidecar、warmcopy 总 artifact、phase 2 tail 和等待
窗口。它们不提高 InnoDB lock 导出能力；lock warmcopy 的 memory bytes 仍只是 artifact
payload 内存预算，不是 record store / journal 的总 heap cap。

示例方向：

```sql
SET GLOBAL preserve_trx_batch_max_transactions = 1000;
SET GLOBAL preserve_trx_max_total = 1000;
SET GLOBAL preserve_trx_max_pending_per_user = 1000;

SET GLOBAL preserve_trx_max_lock_count = 50000;
SET GLOBAL preserve_trx_max_modified_tables = 256;
SET GLOBAL preserve_trx_max_scan_pages = 200000;
SET GLOBAL preserve_trx_materialize_timeout_ms = 60000;
SET GLOBAL preserve_trx_max_snapshot_bytes = 268435456;
```

如果每个事务还有很大的 binlog cache，再按实际 binlog cache 大小设置：

```sql
SET GLOBAL preserve_trx_max_binlog_cache_bytes = 10737418240;
SET GLOBAL preserve_trx_warmcopy_max_total_bytes = 1099511627776;
SET GLOBAL preserve_trx_warmcopy_close_timeout_ms = 60000;
```

这些值只是容量方向，不是通用推荐值。上线前必须用真实业务事务估算并压测：

- 单事务最大 record/table lock 数。
- 单事务 modified table 数。
- materialize implicit locks 的扫描页数和耗时。
- snapshot metadata 大小。
- 单事务 binlog cache 大小。
- 1000 个事务的 preserve directory 磁盘空间和 IO 峰值。

调大 admission 只解决“允许多少事务进入 preserve”。如果磁盘、IO、lock payload、
snapshot size、warmcopy tail、权限校验或 unsupported lock/object shape 不满足，
drain 仍然会 fail closed。

## 10. 使用检查清单

执行 preserve/drain 前：

- 确认 `@@GLOBAL.preserve_trx_enable=ON`。
- 确认执行账号有 `SHUTDOWN` 权限。
- 确认事务只涉及支持的 InnoDB/binlog 参与者。
- 估算 binlog cache、snapshot、temp sidecar、lock 数和 modified tables。
- 估算 preserve directory 可用空间。
- 决定是否需要 `WITH USER VARS`。
- 为维护窗口设置足够的 timeout。

执行 resume 前：

- 确认 token 未过期。
- 确认当前 session 不在事务中。
- 确认 binlog/GTID 模式与 token 匹配。
- 确认恢复账号是 owner，或具有
  `RESUME_ANY_PRESERVED_TRANSACTION`。
- 确认恢复账号仍有相关对象权限。
- 查看 P_S 的 `LAST_ERROR` 和 sidecar 状态。

排查失败时：

- 查 `performance_schema.preserved_transactions`。
- 查 `SHOW GLOBAL STATUS LIKE 'Preserve_trx_%'`。
- 查 error log 中 `PRESERVE:` 前缀日志。
- 区分是 common context、事务内容、权限、binlog 模式、sidecar、warmcopy、
  recovery timeout 还是 cleanup failure。

## 11. 一句话总结关键预算关系

- `preserve_trx_warmcopy_max_total_bytes`：一次 drain epoch 中 warm external
  blob 总量预算，主要影响磁盘/IO/准入数量，不是堆内存上限。
- `preserve_trx_max_binlog_cache_bytes`：单个 preserved binlog cache sidecar
  上限。
- `preserve_trx_max_lock_count`：单个事务 record lock、materialized implicit
  lock、table lock 的共享导出数量预算。
- `preserve_trx_max_modified_tables`、`preserve_trx_max_scan_pages`、
  `preserve_trx_materialize_timeout_ms`：单个事务 implicit lock materialization
  的表数、扫描页数和耗时边界。
- `preserve_trx_max_snapshot_bytes`：snapshot metadata 文件大小上限；锁 payload
  调大后必须同步估算 snapshot payload，避免从 lock count 限制转移到 snapshot
  size 限制。
- `preserve_trx_warmcopy_chunk_bytes`：phase 1 copy 的外层 chunk，不改变总量。
- `preserve_trx_warmcopy_pending_*`：out-of-order mirror writes 的单 participant
  堆内存保护。
- `preserve_trx_lock_warmcopy_max_memory_bytes`：lock warmcopy 最终 artifact payload
  在内存中的保留预算，超出后优先 spill；不是 record store / journal / candidate 的
  总堆内存 cap。
- `preserve_trx_lock_warmcopy_max_journal_bytes`、`preserve_trx_lock_warmcopy_max_dirty_shards`、
  `preserve_trx_lock_warmcopy_max_mdl_descriptors`：分别约束 lock warmcopy journal、
  dirty shard 重验和 MDL descriptor 数量。
- `preserve_trx_memory_*`：通用 Preserve/Resume heap lease 保护。
- `preserve_trx_batch_max_transactions` 和 `preserve_trx_max_total`：数量准入。

## 12. 源码对账入口

本手册的关键结论对应以下实现入口：

| 手册主题 | 源码入口 | 对账结论 |
|---|---|---|
| 参数默认值、范围、作用域 | `sql/sys_vars.cc` 中 `Sys_preserve_trx_*` | 表 4 的默认值、范围和 scope 来自 `DEFAULT()`、`VALID_RANGE()`、`GLOBAL_VAR()`、`SESSION_VAR()`。 |
| 单事务 preserve 顺序 | `preserve_trx_kernel_preserve_attached_transaction()`、`preserve_trx_preserve_attached_transaction()` | validation、binlog preflight、lock preflight、prepare、detach、snapshot write、record register、token delivery 是当前代码顺序。 |
| 成功后停机 | `preserved_trx_register_pending_token_delivery()`、`preserved_trx_finalize_statement_response()` | 单事务 preserve 成功响应交付后，token 才标记为 resumable，并触发 mysqld shutdown。 |
| common context 限制 | `preserve_trx_is_unsupported_common_context()` | 表 7.1 列出的 replication、group replication、force recovery、XA 非空、LOCK TABLES、user locks、HANDLER、cursor、stored routine runtime context 会在入口拒绝。 |
| 事务内容限制 | `preserve_trx_has_unsupported_transaction_contents()` | 非事务表修改和非 InnoDB/binlog read-write handlerton participant 会拒绝。 |
| lock/read view/savepoint/MDL 导出 | `trx_preserve_export_read_view()`、`trx_preserve_materialize_implicit_locks()`、`MDL_context::export_preserved_locks()`、`export_sql_savepoints()`、`trx_preserve_export_record_locks()`、`trx_preserve_export_table_locks()` | 这些状态是支持导出的事务语义；失败条件来自数量上限、unsupported mode/object shape、payload 校验失败。 |
| binlog/GTID 状态 | `mysql_binlog_preserve_export()`、`no_cache_gtid_state_is_clean()`、`binlog_state_matches_current_mode()` | no-cache 状态也要求 GTID clean；resume 会重新校验当前 binlog 模式与 token 记录一致。 |
| warmcopy admission 和内存模型 | `Warmcopy_batch_blob_provider::prepare_thd()`、`Mysql_binlog_warmcopy_session::begin()`、`Mysql_binlog_warmcopy_session::finalize()` | `m_total_bytes`、`reserved_size`、`session_blob_limit` 是 warm external blob artifact 记账，不是等量 heap 分配；phase 1 copy 受 chunk 控制，phase 2 受 tail budget 和 pending range 限制。 |
| binlog copy buffer | `IO_CACHE_binlog_cache_storage::copy_range_to()` | 磁盘部分使用 8192 字节 buffer；`preserve_trx_warmcopy_chunk_bytes` 是外层 copy loop 目标 chunk。 |
| 用户变量 | `export_user_vars_payload()`、`import_user_vars_payload()` | 只有 `WITH USER VARS` 才导出；空集合 payload 会在 resume 时清空当前 session 用户变量；无 payload 不导入。 |
| 临时表 sidecar | `preserve_trx_temp_table_preflight_preserve()`、`preserve_trx_temp_table_build_preserve_manifest()`、`preserve_trx_temp_table_validate_sidecars()`、`preserve_trx_temp_table_materialize_for_resume()` | 支持当前实现可导出/可 materialize 的用户 InnoDB transactional temporary table DML；DDL、元数据变更、缺失/损坏 no-redo undo 或 sidecar 走 fail closed。 |
| P_S/SHOW 可见性 | `preserved_trx_snapshot()`、`Sql_cmd_show_preserved_transactions::execute()` | owner 可见自己的 token；`PROCESS` 或 `RESUME_ANY_PRESERVED_TRANSACTION` 可见其他账号记录；没有 `PROCESS` 时 token 字段脱敏。 |
| P_S 字段 | `storage/perfschema/table_preserved_transactions.cc` | 表 8.1 字段与 P_S 插件表定义一致。 |
| 状态变量 | `sql/mysqld.cc` | 表 8.2 只列当前实现注册的 `Preserve_trx_warmcopy_*`、`Preserve_trx_lock_warmcopy_*`、`Preserve_trx_memory_*`、`Preserve_trx_spill_*` 状态变量。 |
