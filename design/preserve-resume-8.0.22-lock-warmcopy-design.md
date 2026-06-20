# MySQL 8.0.22 Preserve/Resume 锁 Warmcopy 设计文档

本文档描述 MySQL 8.0.22 Preserve/Resume 移植分支上的第一版锁语义
warmcopy 设计。设计目标是降低 `DRAIN TRANSACTIONS PRESERVE` 第二阶段
因为锁导出造成的阻塞时间，同时严格保持当前 Preserve/Resume 已经支持的
事务语义范围。

这版方案把锁语义明确分成两类：

- implicit record X-lock 不作为必须搬迁的显式锁对象。只要恢复后原事务以原
  `trx_id` 回到 InnoDB `rw_trx_list` / `rw_trx_set`，并且 page、undo、版本链
  仍能支持 InnoDB 原生判断，implicit lock 就应继续按原生逻辑懒推导。
- gap、next-key、record-not-gap、insert-intention、predicate、InnoDB table /
  AUTO_INC、MDL transaction ticket 都有显式对象或显式 descriptor，必须纳入
  warmcopy / export / import 体系，不能靠 page + `trx_id` 自动完整恢复。

第一版只覆盖当前代码已经能够导出、序列化、恢复的锁语义；不把
`LOCK TABLES`、`GET_LOCK()`、HANDLER、global read lock、backup lock 等
显式会话锁或全局锁纳入支持范围。

## 1. 背景

当前 `preserve_trx_warmcopy_enable=ON` 主要优化 binlog cache。满足以下条件时，
batch drain 会进入两阶段 warmcopy：

```text
preserve_trx_warmcopy_enable=ON
binlog enabled
mysql_bin_log.is_open()
```

现有两阶段流程大致是：

1. phase 1 进入 `WARMCOPY_DRAINING`，业务事务仍可继续执行；
2. binlog cache prefix 在后台 mirror 到 warmcopy blob；
3. phase 2 进入 `WARMCOPY_CLOSING`，阻塞目标会话新的事务/锁相关操作；
4. 关闭 warmcopy admission，等待已接收的 mirror 工作收敛；
5. 对每个目标事务执行 preserve kernel。

这套机制能显著降低大 binlog cache 的第二阶段复制成本，但它没有提前处理锁语义。
当前 preserve kernel 仍在 `LOCK_PREFLIGHT` 阶段现场处理 read view、implicit
locks、MDL、savepoints、record locks、table locks 等事务语义。关键代码路径是：

- `sql/preserve_trx.cc:7811`：进入 `LOCK_PREFLIGHT`；
- `sql/preserve_trx.cc:7814`：导出 read view；
- `sql/preserve_trx.cc:7825`：当前 live/fallback 路径会物化 implicit locks；
- `sql/preserve_trx.cc:7848`：导出 MDL descriptors；
- `sql/preserve_trx.cc:7864`：导出 SQL savepoints；
- `sql/preserve_trx.cc:7880`：导出 InnoDB record locks preflight payload；
- `sql/preserve_trx.cc:7902`：导出 InnoDB table locks preflight payload；
- `sql/preserve_trx.cc:8191`：pre-prepare record-lock payload 成为 durable lock contract。

因此，超大事务即使 binlog cache 已经在 phase 1 搬完，phase 2 仍可能花大量时间
完成锁相关工作，包括：

- 对 implicit lock 做现场扫描/物化，或在新设计中做 native 条件校验；
- 遍历 `trx->lock.trx_locks`；
- 导出 record lock bitmap；
- 捕获 page identity；
- 拆分 predicate locks；
- 导出 table/AUTO_INC locks；
- 导出有序 MDL descriptors；
- 校验 savepoint 和 MDL ordinal。

如果一个事务有 10 万级别的显式锁，或者一个 batch 中存在上千个大事务，这部分工作
可能成为用户可感知的 drain pause 主因。锁 warmcopy 的目的，是把显式锁对象和
MDL descriptor 尽量提前复制到 phase 1，在 phase 2 只做收口、校验、少量重扫和
payload 生成；对 implicit record X-lock，则尽量避免把它们在 phase 2 全量物化，
改为验证 InnoDB 原生懒判断在 resume 后仍成立。

## 2. 设计原则与约束

### 2.1 warmcopy 是优化，不是新的事实源

锁 warmcopy 不能改变 Preserve/Resume 的语义边界。最终成功写入 snapshot 的内容，
仍必须是当前 resume 路径已经能识别的 payload：

```text
record_locks_payload
predicate_locks_payload
table_locks_payload
mdl_descriptors_payload
autoinc_lock_owned
```

这些字段定义在 `sql/preserve_trx_bundle.h:149-158`。内部 warmcopy store 只是
临时实现细节，不能成为新的持久化格式，也不能要求 resume 读一套新格式。

### 2.2 锁 warmcopy 搬迁的是信息对象，不是业务语义

phase 1 不能通过真正的 `lock()`、`unlock()`、MDL acquire/release 来重放业务语义。
它应该记录锁信息对象的增删改状态，例如：

```text
UPSERT_OBJECT
PATCH_BITMAP
PATCH_TICKET_TYPE
MOVE_TICKET_DURATION
DELETE_OBJECT
INVALIDATE_SHARD
INVALIDATE_CONTEXT
SEAL
```

这样才能处理幂等、乱序、删除已不存在对象、base scan 与增量 journal 交错等问题。

### 2.3 pre-prepare record-lock contract 是硬约束

当前代码有一个非常重要的语义点：record-lock preflight payload 在
`ha_prepare_low()` 之前导出，并且后续注释明确说明这是 durable lock contract。
原因是 InnoDB XA prepare 可能释放 RC 风格事务中的 gap locks，但 Preserve/Resume
必须保留用户在 prepare 之前可观察到的 next-key/gap lock 语义。

因此：

- 有效的 record-lock warmcopy payload 必须等价于 prepare 前的锁状态；
- 如果 warmcopy 无效且允许 fallback，live export 必须仍发生在 `LOCK_PREFLIGHT`
  阶段、`ha_prepare_low()` 之前，用于 record/predicate pre-prepare contract；
- 不能在 prepare 之后重新导出 record locks，然后声称语义等价。

### 2.3.1 record_locks_payload 的阶段归属

`record_locks_payload` 不是一个在 phase 1 一生成就可以直接写入 snapshot 的最终
事实。它在两阶段 drain 中应当按下面的方式理解：

```text
phase 1:
  预构建 record_locks_payload 所需的大部分信息
  包括 lock entry / bitmap / page identity / record image / dirty shard
  业务仍可继续运行，因此这些信息只是候选快照

phase 2:
  关闭 lock warmcopy admission
  等待已进入 journal 的增量事件收敛
  阻塞目标事务继续产生锁变化
  seal / revalidate 候选快照
  生成或确认最终 record_locks_payload

ha_prepare_low():
  只能在最终 record_locks_payload 已经成立之后进入
```

也就是说，锁 warmcopy 的目标是让 `record_locks_payload` 的大部分采集、构造和
序列化成本前移到 phase 1；但它作为 durable pre-prepare contract，必须在 phase 2
seal 成功后才成立。这个区分很关键，因为 phase 1 期间仍可能发生以下变化：

- 事务又获取了新的 record/gap/next-key lock；
- 某个 record lock 被释放或 bitmap 中的 heap bit 发生变化；
- base scan 看到了一把锁，但增量 journal 随后记录了删除；
- 增量 journal 先到达一个删除事件，而 base scan 还没有把对应对象放入 object
  store；
- page split / merge / same-page reorganize 导致 page identity 或 record image 需要
  重验；
- 如果未来扩展 predicate warmcopy，predicate page identity 也需要重验；v1 对
  R-tree/spatial predicate locks 不做 warmcopy 优化，按 §7.7 fallback/reject 处理；
- 锁数量超过 `preserve_trx_max_lock_count`；
- record image 出现当前导出格式不支持的字段形态。

因此，phase 1 的结果只能叫 `warmcopy candidate` 或 `prebuilt fragments`。只有
phase 2 在阻止目标继续变化后完成 tail journal apply、dirty shard 重验、page
identity 重验、lock count 校验和 payload validator 校验，才能把它提升为最终
`record_locks_payload`。如果校验失败，fallback `ON` 时必须丢弃该 target 的所有锁
warmcopy 结果并回到当前 live export；fallback `OFF` 时必须在 prepare 前 fail
closed。

### 2.3.2 table_locks_payload 是 post-prepare current-parity family

record locks 和 table locks 在当前 8.0.22 preserve 代码中的事实点并不相同：

```text
record_locks_preflight_payload:
  LOCK_PREFLIGHT / ha_prepare_low() 前导出
  ha_prepare_low() 后原样复用为 durable record-lock contract

table_locks_preflight_payload:
  LOCK_PREFLIGHT / ha_prepare_low() 前只做 preflight 和 count 校验

metadata.table_locks_payload:
  ha_prepare_low() 后重新调用 trx_preserve_export_table_locks() 导出
```

因此 v1 不能把 `table_locks_payload` 错套进 record-lock 的 pre-prepare contract。
table lock warmcopy 只是提前维护 table/AUTO_INC 候选信息，用来降低 post-prepare 导出点
的扫描和校验成本；最终 snapshot 中的 `table_locks_payload` 必须保持 current parity：

- 在 `LOCK_PREFLIGHT` 前置校验支持性和数量上限，确保失败时用户事务仍可继续；
- 在 `ha_prepare_low()` 之后、现有 table-lock export 语义点重验 table/AUTO_INC 状态；
- 只有 post-prepare 重验能证明 warmcopy candidate 与当前现场 table locks 等价，才可
  复用 warmcopy 生成 payload；
- 否则 fallback `ON` 走现有 post-prepare live export，fallback `OFF` 在写 snapshot 前
  reject；
- 若后续要把 table locks 改成 pre-prepare durable contract，必须作为单独语义变更，
  更新当前代码路径和 warmcopy-on/off 等价测试，不能在 v1 中隐式完成。

### 2.4 implicit lock 走 native continuity，不默认物化

implicit record X-lock 的事实源不是 `lock_t` 对象，而是：

```text
clustered / secondary page evidence
+ undo / version chain
+ preserved trx_id 仍在 InnoDB running trx structures
```

因此 v1 的默认路径不把 implicit lock 作为 warmcopy payload 对象，也不在 phase 1
调用 `lock_rec_add_to_queue()` 安装显式锁。它只要求在 preserve/recover 合同中证明：

- preserved trx 用原 `trx_id` 恢复；
- recovered/preserved trx 在 `rw_trx_list` / `rw_trx_set` 中可被
  `trx_rw_is_active()` 等原生路径识别；
- page、undo、clustered record、secondary page max trx id 等原生判断依赖的信息
  没有被破坏；
- rollback preserved trx 时，事务从 running trx structures 移除，implicit lock
  自然释放。

需要特别区分“warmcopy 主动物化”和“InnoDB 引擎侧物化”。warmcopy 本身不能主动把
implicit lock 物化成 explicit lock；但业务线程在 phase 1 继续运行时，InnoDB 可能因
读/写冲突检查调用 `lock_rec_convert_impl_to_expl()`，把某条 implicit X-lock 转成
显式 `LOCK_REC | LOCK_X | LOCK_REC_NOT_GAP`。这种情况按 `Explicit Wins` 处理：

```text
engine-side implicit -> explicit materialization
  -> 该 record 进入 explicit record family
  -> 从 implicit native validation 覆盖集合中排除
  -> seal 时做 cross-family disjointness 校验
```

也就是说，同一 `{table_id,index_id,space_id,page_no,heap_no}` 不能同时被声明为
native implicit 覆盖和 exported explicit 覆盖。若无法证明二者 disjoint，该 target
的 lock warmcopy artifact invalid，fallback `ON` 时 live export，fallback `OFF` 时
prepare 前 reject。这样避免同一条记录既被 native continuity 声明有效、又被
`record_locks_payload` 显式恢复的 double-count。

当前 `trx_preserve_materialize_implicit_locks()` 保留为 live/fallback 兼容路径：
当 native implicit validation 失败且 `fallback_to_live_export=ON` 时，可以在
`LOCK_PREFLIGHT`、`ha_prepare_low()` 前走当前现场物化与导出；如果 fallback 关闭，
则 fail closed。

### 2.5 场景约束：只保存事务，不保存 SQL session/global state

第一版 Preserve/Resume 的对象边界是“可恢复的事务语义”，不是完整 SQL 连接状态，也
不是服务器级状态。因此以下场景继续不支持：

- `LOCK TABLES`：它进入 `locked_tables_mode`，维护 `MYSQL_LOCK`、
  `Locked_tables_list`、打开表对象和 `MDL_EXPLICIT`，并且可以跨 `COMMIT` 保持。
  支持它等价于恢复 SQL session 的 locked tables mode，而不是只恢复一个事务。
- `GET_LOCK()` / `RELEASE_LOCK()`：它使用 `MDL_key::USER_LEVEL_LOCK` +
  `MDL_EXPLICIT`，同时在 `THD::ull_hash` 中维护 named lock map 和递归引用计数。
  仅恢复 MDL ticket 不足以恢复 `RELEASE_LOCK()` 和 `RELEASE_ALL_LOCKS()` 语义。
- global read lock / `FLUSH TABLES WITH READ LOCK`：它是服务器级阻塞状态，包含
  `GLOBAL` 与 `COMMIT` namespace 的显式 MDL，并有“先阻止新写、flush/close
  tables、再阻止 commit”的两阶段协议。跨 restart 恢复它会影响全实例可用性。

这些限制不是因为它们没有锁对象，而是因为它们的生命周期和归属不是当前
Preserve/Resume 的事务快照模型。

这些场景属于 target/session eligibility reject，不属于 lock warmcopy seal invalid。
因此它们不受 `preserve_trx_lock_warmcopy_fallback_to_live_export` 控制：

```text
target/session eligibility reject
  -> 不创建可用 lock warmcopy artifact
  -> 不回退 live export
  -> 按当前 DRAIN/PRESERVE unsupported 语义拒绝

lock warmcopy seal invalid
  -> 该 target 的 lock warmcopy artifact 不可用
  -> fallback ON 时丢弃 artifact 并 live export
  -> fallback OFF 时 prepare 前 fail closed

live export failed
  -> 使用当前 live export 失败原因
  -> 不再回到 warmcopy candidate
```

也就是说，fallback 只处理“同一事务语义在 warmcopy 路径上无法证明”的情况；不把
SQL session/global state 变成可保存对象。

### 2.6 第一版不扩大支持范围

第一版只覆盖当前 preserve 已经支持的锁：

- InnoDB record locks；
- InnoDB predicate locks；
- InnoDB table locks；
- InnoDB AUTO_INC locks；
- 当前 `MDL_context::export_preserved_locks()` 支持的 `MDL_TRANSACTION`
  descriptors。

以下继续保持 unsupported / fail closed：

- `LOCK TABLES`；
- HANDLER locks；
- `GET_LOCK()` / `RELEASE_LOCK()` 用户锁；
- global read lock；
- backup lock；
- locking service locks；
- ACL/resource group/column statistics 等当前不支持的 MDL namespace；
- 运行时内部 mutex、rwlock、latch、`MDL_lock *`、PSI handle、fast-path counter。

## 3. 当前代码事实

### 3.1 durable snapshot 中的锁字段

`Preserve_snapshot_metadata` 当前包含：

```text
autoinc_lock_owned
read_view_payload
record_locks_payload
predicate_locks_payload
table_locks_payload
mdl_descriptors_payload
sql_savepoints_payload
innodb_savepoints_payload
```

resume 侧按现有逻辑恢复 table locks、record locks、predicate locks，并基于
`mdl_descriptors_payload` 重建 MDL requests。锁 warmcopy 必须生成这些既有字段，
不能改变 snapshot schema 的核心恢复语义。

### 3.2 record/predicate lock 导出

`lock_preserve_export_record_locks()` 当前做这些事：

1. 在 `locksys::Global_exclusive_latch_guard` 和 `trx_mutex` 下遍历
   `trx->lock.trx_locks`；
2. 只处理 `LOCK_REC`；
3. 拒绝 waiting lock；
4. 拒绝不支持的 `type_mode`；
5. 统计 bitmap 中 live bits，受 `preserve_trx_max_lock_count` 约束；
6. 导出 `table_id`、`index_id`、`space_id`、`page_no`、`type_mode`、`n_bits`、
   bitmap；
7. 普通 record lock 捕获 heap_no 列表和 `record_images`；
8. predicate lock 按类型捕获 MBR/op payload 或 page identity；
9. 捕获并校验 page/record identity，identity drift 会导致导出失败。

predicate locks 与普通 record locks 使用同一类 payload 结构，后续通过
`lock_preserve_split_record_and_predicate_locks()` 拆到
`record_locks_payload` 和 `predicate_locks_payload`。但设计上应当把 predicate
当成独立 object family，因为它的 drift、payload、unsupported shape 与普通
bitmap record lock 的失败模式不同。

### 3.3 record lock bitmap 不是只靠对象创建/销毁就能跟踪

InnoDB record lock 的一个关键特点是：一个 `lock_t` 可以覆盖同一页上的多个
record bit。`lock_rec_set_nth_bit()` 只是设置 bitmap bit 并增加
`n_rec_locks`，不一定创建新的 `lock_t`，也不会 bump `trx_locks_version`。

因此，lock warmcopy 不能只监听 `trx->lock.trx_locks` list 的 add/remove。
它还必须覆盖：

- bitmap bit set；
- bitmap bit reset；
- record lock discard；
- page split/merge；
- record move；
- heap number 变化；
- B-tree reorganize 引起的 identity drift。

对 page/heap 变化，第一版应优先标记 dirty shard，而不是尝试完整重放底层
B-tree 移动逻辑。

### 3.4 table/AUTO_INC lock 导出

`lock_preserve_export_table_locks()` 当前支持：

```text
LOCK_IS
LOCK_IX
LOCK_S
LOCK_X
LOCK_AUTO_INC
```

导出字段是：

```text
table_id
lock_mode
type_mode_bits
```

AUTO_INC 本质上是 table lock 的一个 lock mode，但 metadata 里还有
`autoinc_lock_owned`，并且当前代码会校验 `table_locks_payload` 中是否含有
AUTO_INC 与 `autoinc_lock_owned` 是否一致。锁 warmcopy 必须保留这个交叉校验。

### 3.5 MDL 导出

`MDL_context::export_preserved_locks()` 当前有几个硬规则：

- `MDL_STATEMENT` 非空则失败；
- `MDL_EXPLICIT` 非空则失败；
- 只导出 `MDL_TRANSACTION` tickets；
- descriptor 中写入 namespace、ticket type、duration、ordinal、key payload；
- ordinal 来自 transaction-duration list 顺序。

支持的 namespace：

```text
GLOBAL
TABLESPACE
SCHEMA
TABLE
COMMIT
FOREIGN_KEY
CHECK_CONSTRAINT
FUNCTION
PROCEDURE
TRIGGER
```

不支持的 namespace：

```text
BACKUP_LOCK
EVENT
USER_LEVEL_LOCK
LOCKING_SERVICE
SRID
ACL_CACHE
COLUMN_STATISTICS
RESOURCE_GROUPS
NAMESPACE_END
```

MDL savepoint ordinal 也依赖 duration list 顺序。因此 MDL warmcopy 不能只用
无序 map 保存 descriptors，必须维护 transaction-duration ticket 的有序列表。

### 3.6 当前 locks_count 不包括 MDL

`preserved_trx_metadata_locks_count()` 当前统计：

```text
record_locks_payload
predicate_locks_payload
table_locks_payload
```

它不统计 `mdl_descriptors_payload`。因此新设计中 MDL descriptor 数量应该有单独
预算和状态变量，不能混入 InnoDB lock count 解释。

### 3.7 会话/全局锁不属于当前事务快照

当前 MDL 子系统把 duration 分成 `MDL_STATEMENT`、`MDL_TRANSACTION`、
`MDL_EXPLICIT`。`MDL_EXPLICIT` 的注释明确包括 HANDLER、`LOCK TABLES`、
`GET_LOCK()` / `RELEASE_LOCK()` 用户锁以及 global read lock。这些锁跨 statement、
transaction、savepoint 存在，需要显式释放。

当前 `MDL_context::export_preserved_locks()` 的入口规则是：

```text
MDL_STATEMENT non-empty -> export failure
MDL_EXPLICIT non-empty -> export failure
only MDL_TRANSACTION tickets are exported
```

因此第一版不能把这些场景解释成“当前事务锁很多所以也可以 preserve”。它们需要
额外保存 SQL session/global state：

- `LOCK TABLES`：`locked_tables_mode`、`Locked_tables_list`、`MYSQL_LOCK`、
  `THR_LOCK_DATA`、open table objects、table reopen 规则；
- `GET_LOCK()`：`THD::ull_hash` 中的 named-lock map、`User_level_lock::refs`、
  `RELEASE_LOCK()` / `RELEASE_ALL_LOCKS()` 返回语义；
- global read lock：`Global_read_lock` 的 state、`GLOBAL` 和 `COMMIT` namespace
  MDL ticket、两阶段阻塞协议和全实例可用性影响。

这些状态后续可以单独设计“session/global state preserve”，但不应混入 v1 事务锁
warmcopy。

## 4. 配置参数

参数必须按能力域细分，不能继续把 `warmcopy` 当成一个单一总开关。binlog cache
warmcopy 和 lock warmcopy 解决的问题不同、预算不同、失败策略也不同：

- binlog cache warmcopy 只负责提前搬迁 binlog cache bytes；
- lock warmcopy 只负责提前构造事务锁语义 payload；
- 两者可以同时开启，也可以分别关闭；
- lock warmcopy 默认 `ON` 不依赖 binlog 是否开启，也不受 binlog warmcopy 开关控制；
- `two_phase_drain_enabled` 由实际启用的 participant 派生，不再只由 binlog
  warmcopy 决定。

### 4.1 binlog cache warmcopy 参数

以下是现有 binlog cache warmcopy 参数。`preserve_trx_warmcopy_enable` 是历史命名，
在引入 lock warmcopy 后应明确解释为“binlog cache warmcopy enable”，不是所有
warmcopy 能力的总开关。

| 参数 | 默认值 | 范围 | 含义 |
|---|---:|---|---|
| `preserve_trx_warmcopy_enable` | `ON` | boolean | 启用 binlog cache warmcopy；不控制 lock warmcopy。 |
| `preserve_trx_warmcopy_close_timeout_ms` | `30000` | `0..UINT_MAX32` | binlog warmcopy closing 等待超时。 |
| `preserve_trx_warmcopy_min_open_ms` | `1000` | `0..UINT_MAX32` | binlog warmcopy phase 1 最小打开时间。 |
| `preserve_trx_warmcopy_chunk_bytes` | `1048576` | `1..UINT_MAX32` | binlog cache warmcopy chunk 大小。 |
| `preserve_trx_warmcopy_tail_budget_bytes` | `1048576` | `0..UINT_MAX32` | phase 2 允许现场补搬的 binlog tail bytes。 |
| `preserve_trx_warmcopy_max_total_bytes` | `10737418240` | `1..ULLONG_MAX` | 单次 drain epoch 的 binlog warm external blob 总预算。 |
| `preserve_trx_warmcopy_pending_range_limit` | `1024` | `0..UINT_MAX32` | binlog warmcopy pending range 数量上限。 |
| `preserve_trx_warmcopy_pending_bytes_limit` | `67108864` | `0..ULLONG_MAX` | binlog warmcopy pending bytes 上限。 |

为了减少误读，后续可以新增只读 alias 或文档别名
`preserve_trx_binlog_warmcopy_enable`，但第一版不要求重命名现有 sysvar；现有参数名
保持兼容。

### 4.2 lock warmcopy 参数

新增锁语义 warmcopy 参数：

| 参数 | 默认值 | 范围 | 含义 |
|---|---:|---|---|
| `preserve_trx_lock_warmcopy_enable` | `ON` | boolean | 启用锁语义 warmcopy。 |
| `preserve_trx_lock_warmcopy_fallback_to_live_export` | `ON` | boolean | 锁 warmcopy 校验失败时，是否在 phase 2 回退当前现场导出路径。 |
| `preserve_trx_lock_warmcopy_max_memory_bytes` | `268435456` | `1..ULLONG_MAX` | 锁 warmcopy 内存状态上限。 |
| `preserve_trx_lock_warmcopy_max_journal_bytes` | `1073741824` | `1..ULLONG_MAX` | 单次 drain epoch 锁 warmcopy journal 字节上限。 |
| `preserve_trx_lock_warmcopy_max_dirty_shards` | `100000` | `0..UINT_MAX32` | phase 2 可重验 dirty record shards 上限。 |
| `preserve_trx_lock_warmcopy_max_mdl_descriptors` | `100000` | `0..UINT_MAX32` | MDL warmcopy 可接受的 transaction-duration descriptors 上限。 |
| `preserve_trx_lock_warmcopy_seal_threads` | `0` | `0..1024` | phase 2 seal/validation 并行度；`0` 表示按 target 数和 CPU 自动选择有界并行度。 |
| `preserve_trx_lock_warmcopy_conversion_wait_timeout_ms` | `30000` | `0..UINT_MAX32` | 其它会话撞到目标事务 conversion freeze 后，释放 latch/mtr 并等待 freeze 清除再重试的上限；实际等待还受 drain 剩余超时、KILL、shutdown 约束。 |

这些参数只影响 lock participant。比如：

- `preserve_trx_lock_warmcopy_enable=ON` 且 `preserve_trx_warmcopy_enable=OFF`：
  只开启锁 warmcopy，不做 binlog cache warmcopy；
- `preserve_trx_lock_warmcopy_enable=OFF` 且 `preserve_trx_warmcopy_enable=ON`：
  保持现有 binlog cache warmcopy 行为，锁仍在 `LOCK_PREFLIGHT` 现场导出；
- 两者都 `ON`：binlog participant 和 lock participant 同时参与两阶段 drain；
- 两者都 `OFF`：回到当前 single-phase live export 行为。

### 4.3 共享 preserve 限制参数

以下现有参数仍然有效，它们约束最终 preserve 语义或 legacy fallback，不属于某个
warmcopy participant 的私有预算：

- `preserve_trx_max_lock_count`：约束 explicit record/predicate/table locks；native
  implicit lock 不按行计入这个上限；
- `preserve_trx_max_modified_tables`：约束 implicit native validation 和 legacy
  materialization 涉及的 modified tables；
- `preserve_trx_max_scan_pages`：主要约束 legacy live materialization/fallback 的扫描页数；
- `preserve_trx_materialize_timeout_ms`：主要约束 legacy live materialization/fallback 的时间；
- `preserve_trx_max_snapshot_bytes`：约束最终 snapshot metadata 大小。

### 4.4 fallback 参数语义

`preserve_trx_lock_warmcopy_fallback_to_live_export` 只控制“已经尝试 lock warmcopy
但 seal/校验失败”的处理方式。它不改变 `preserve_trx_lock_warmcopy_enable=OFF`
时的现有行为。

行为矩阵：

| `preserve_trx_lock_warmcopy_enable` | `fallback_to_live_export` | 行为 |
|---|---|---|
| `OFF` | 任意 | 不做锁 warmcopy，直接走当前 live export。 |
| `ON` | `ON` | 优先使用锁 warmcopy；warmcopy 无效时丢弃 warmcopy payload，回退当前 live export。 |
| `ON` | `OFF` | 优先使用锁 warmcopy；warmcopy 无效时直接 fail closed。 |

关键点：fallback 不是拿不完整 warmcopy 结果凑合用。fallback 的含义是：

```text
warmcopy 结果不可靠
  -> 丢弃该 target 的所有 lock warmcopy payload
  -> 在 LOCK_PREFLIGHT 中调用当前已有 live export 路径
  -> live export 成功则 preserve 继续
  -> live export 失败则 preserve 失败
```

## 5. drain 两阶段决策

当前 two-phase drain 只由 binlog warmcopy 决定：

```text
preserve_trx_warmcopy_enable && opt_bin_log && mysql_bin_log.is_open()
```

引入 lock warmcopy 后，应拆成两个独立开关：

```text
binlog_warmcopy_enabled =
  preserve_trx_warmcopy_enable && opt_bin_log && mysql_bin_log.is_open()

lock_warmcopy_enabled =
  preserve_trx_lock_warmcopy_enable

two_phase_drain_enabled =
  binlog_warmcopy_enabled || lock_warmcopy_enabled
```

这里的 `preserve_trx_warmcopy_enable` 只代表 binlog cache warmcopy。它为 `OFF`
时，只关闭 binlog participant；不会关闭 lock participant。这样即使 binlog 关闭、
binlog warmcopy 关闭，或者目标事务没有 binlog cache，只要
`preserve_trx_lock_warmcopy_enable=ON`，仍可进入两阶段 drain，从而把锁语义搬迁工作
前移到 phase 1。

如果 binlog warmcopy 和 lock warmcopy 都关闭，则继续走当前 single-phase drain。

行为矩阵：

| binlog warmcopy effective | lock warmcopy effective | drain 模式 | binlog cache 处理 | 锁语义处理 |
|---|---|---|---|---|
| `OFF` | `OFF` | single-phase | 不做 binlog warmcopy | 当前 live export |
| `ON` | `OFF` | two-phase | binlog participant warmcopy | 当前 live export |
| `OFF` | `ON` | two-phase | 不做 binlog warmcopy | lock participant warmcopy / seal |
| `ON` | `ON` | two-phase | binlog participant warmcopy | lock participant warmcopy / seal |

`effective` 的含义是“开关打开且运行条件满足”。例如
`preserve_trx_warmcopy_enable=ON` 但 binlog 未打开时，binlog warmcopy effective 仍为
`OFF`；这不影响 lock warmcopy effective。

实现上不能继续用 `warmcopy_participant != nullptr` 代表整个 two-phase 流程。需要用
`two_phase_drain_enabled` 或 participant 集合驱动 phase 1/phase 2 生命周期；binlog
tail budget、binlog blob finalize 只属于 binlog participant，lock seal、lock
fallback 只属于 lock participant。

## 6. 总体架构

### 6.1 新增 drain participant

新增独立 participant：

```text
Lock_warmcopy_batch_drain_participant
```

它与现有 `Warmcopy_batch_drain_participant` 并列：

- binlog participant 负责 binlog cache warmcopy；
- lock participant 负责锁语义 warmcopy；
- 两者共享 drain orchestrator 的 open/close/finalize/abort 生命周期；
- 两者可以独立启用。

lock participant 负责：

- drain epoch id；
- per-target lock warmcopy session；
- journal admission 状态；
- 内存和 journal 字节预算；
- per-family 统计；
- 生成最终 lock warmcopy artifacts；
- 向 preserve kernel 提供 artifacts。

participant 的返回值语义必须和 per-target fallback 分开：

- `close_phase1()` / `phase2_preflight()` 返回失败只表示 participant 全局不可用，
  例如 journal 基础设施损坏、epoch 不一致、无法关闭 admission、内存结构整体不可
  信。这类失败会按 drain orchestrator 当前合同中止整个 drain。
- 单个 target 的 `OPEN_UNSUPPORTED`、`SEALED_INVALID`、预算耗尽、dirty shard 无法重验
  等，不能通过 participant 返回失败表达。它们必须落到该 target 的 artifact：
  `available=false`、`invalid_reason` / `fallback_reason`，由 preserve kernel 按
  fallback 参数决定 live export 或 reject。
- 因此实现 Slice 1 必须扩展 `Preserve_trx_kernel_request` 或 batch target wrapper，
  让 kernel 能按 target 读取 lock warmcopy artifact；不能只在 drain participant 内部
  决定整批成功/失败。

### 6.2 per-target session

每个目标事务对应一个 session：

```text
Lock_warmcopy_target_session {
  epoch_id
  target_thread_id
  trx_pointer_identity
  trx_id_or_no
  state
  record_store
  predicate_store
  table_lock_store
  mdl_store
  implicit_native_validation
  journal_cursor
  invalid_reason
  fallback_reason
}
```

状态机：

```text
NEW
  -> JOURNAL_OPEN
  -> BASE_SCAN_RUNNING
  -> OPEN_VALID / OPEN_DIRTY / OPEN_UNSUPPORTED
  -> SEALING
  -> SEALED_VALID / SEALED_INVALID / SEALED_UNSUPPORTED
  -> CONSUMED / ABORTED
```

`OPEN_UNSUPPORTED` 或 `SEALED_INVALID` 只表示 warmcopy 结果不能用，不等于当前
事务一定不能 preserve。最终是否走 live export 或 fail closed 由 fallback 参数决定。

但 target/session eligibility reject 不是 `OPEN_UNSUPPORTED` 的普通降级。例如
`locked_tables_mode`、user-level lock、HANDLER open table、global read lock 等当前
common-context 或 MDL namespace gate 已经判定为不可保存的场景，应保持 unsupported，
不进入 lock warmcopy fallback。

### 6.3 preserve kernel artifacts

preserve kernel 接收可选 artifacts：

```text
Preserve_trx_lock_warmcopy_artifacts {
  bool available;
  std::string record_locks_payload;
  std::string predicate_locks_payload;
  std::string table_locks_payload;
  std::string mdl_descriptors_payload;
  bool autoinc_lock_owned;
  bool implicit_native_validated;
  std::string implicit_native_validation_reason;
  uint32 record_predicate_table_lock_count;
  uint32 mdl_descriptor_count;
}
```

如果 artifacts 有效，kernel 在当前 live export 的逻辑位置消费它们。
kernel 仍必须执行：

- read view export；
- modified table names export；
- 权限 recheck；
- MDL object privilege recheck；
- SQL/InnoDB savepoint export 和数量校验；
- payload validator；
- snapshot byte limit 校验。

如果 artifacts 无效：

- fallback `ON`：在 `LOCK_PREFLIGHT` 中走当前 live export；
- fallback `OFF`：在 prepare 前 reject。

## 7. 对象模型

### 7.1 通用对象字段

所有对象族都有这些基础字段：

```text
object_key
target_id
epoch_id
seq
state: PRESENT | TOMBSTONE | DIRTY | UNSUPPORTED
first_seen_source: BASE_SCAN | JOURNAL
last_update_source: BASE_SCAN | JOURNAL | RESCAN
diagnostic_reason
```

幂等规则：

- `DELETE` 一个不存在的对象是合法 no-op，并记录 tombstone；
- 相同 `seq` 的 `UPSERT` 是 no-op；
- 旧 `seq` 的 `PATCH` 忽略；
- base scan 看到的旧对象不能复活已有 tombstone；
- 出现 seq gap，至少使该 target 的对应对象族 invalid；
- dirty shard 如果 phase 2 不能重验成功，则该对象族 invalid。

对象族实现不能只定义字段而不定义操作边界。每个 family 至少要提供下面这些方法级合同，
并在代码 review 中逐项映射到实现函数：

```text
begin_epoch(epoch_id)
apply_base_scan_entry(entry, seq)
append_journal_delta_under_mutation_cs(delta, seq)
mark_dirty_under_mutation_cs(object_or_shard_key, reason, seq)
mark_invalid_under_mutation_cs(reason, seq)
close_journal_and_wait_inflight()
seal_revalidate(fence)
build_canonical_entries()
build_existing_payload()
discard_artifacts(reason)
```

`append_journal_delta_under_mutation_cs()` / `mark_dirty_under_mutation_cs()` /
`mark_invalid_under_mutation_cs()` 的名字刻意带有 `under_mutation_cs`：调用点必须仍在
实际 lock/MDL/savepoint mutation 的同一临界区内，不能先修改内核对象，出临界区后再补
warmcopy 状态。

### 7.2 RecordLockShardObject

record lock 按 shard 存储，不按每一行单独建对象。自然 shard 边界接近当前
InnoDB `lock_t` record lock：

```text
RecordLockShardObject {
  table_id
  index_id
  space_id
  page_no
  type_mode
  n_bits
  bitmap
  page_lsn
  page_n_heap
  heap_offsets
  record_images
  seq
  state
}
```

`record_images` 是当前普通 record lock import 路径的稳定身份字段，不能省略。它是
按索引字段序列化出的逻辑 record image，不是物理 byte offset；每个 bitmap set bit
对应一份 record image。`heap_offsets` 当前承载的是 heap_no 列表，保留用于 payload
格式兼容。`page_n_heap` 和 `page_lsn` 可以作为诊断或 fast-fail 辅助，但普通 record
lock 的权威恢复校验是：

```text
table/index/page/type
+ bitmap bounds
+ heap_no list shape
+ current record_images == payload record_images
```

不能把 `page_n_heap` 或普通 `heap_offsets` 当成强身份。这样可以避免同页重组导致
record byte offset 变化时误拒绝，也避免 record 内容已经漂移但 page 诊断字段仍看似
匹配时误接受。

这样能让对象数量主要随 page shard 和 bitmap 增长，而不是每一条锁记录都建一个 C++
对象；但 payload/内存字节仍会随被锁 record 的 `record_images` 增长。

必须覆盖的变化：

- lock object add/remove；
- bitmap bit set/reset；
- page split/merge；
- record move；
- heap number 变化；
- lock discard；
- rollback / rollback to savepoint 影响。

实现时需要给 InnoDB 变化路径建函数级 hook map。至少要区分：

- `lock_rec_set_nth_bit` / `lock_rec_reset_nth_bit`：记录 bitmap delta；
- record lock object 加入/移出 `trx_locks`：记录 UPSERT/DELETE；
- B-tree split/merge/reorganize、record move、discard、inherit/update 路径：优先
  MARK_DIRTY 或 INVALIDATE shard，在 phase 2 rescan/revalidate；
- rollback/rollback-to-savepoint：记录 DELETE/PATCH，无法精确映射时标 dirty。

### 7.3 Predicate / R-tree locks

predicate lock 沿用 record payload 容器，但它只出现在 spatial index / R-tree 场景。
当前 live export/import 路径能识别两类 predicate payload：

- `LOCK_PREDICATE`：保存 predicate op + MBR payload；
- `LOCK_PRDT_PAGE`：保存 page identity，且 `record_images` 必须为空。

v1 lock warmcopy 不优化 R-tree / spatial predicate locks。原因是 R-tree split / parent
update / page move 会在 old/new page 之间移动或复制 predicate/page locks；若在 phase 1
尝试按 page shard 增量维护，很容易把 predicate coverage 线性化问题扩大到 R-tree 内部
结构变化。第一版选择 current preserve parity：保留现有 live export/import 能力，但
warmcopy participant 不生成 `predicate_locks_payload`。

规则如下：

```text
target has no LOCK_PREDICATE / LOCK_PRDT_PAGE
  -> lock warmcopy 可以继续生成 ordinary record/table/MDL payload

target has LOCK_PREDICATE / LOCK_PRDT_PAGE
  -> mark lock warmcopy artifact invalid
  -> fallback ON: discard all warmcopy payloads for this target and run live export
  -> fallback OFF: reject before prepare
```

这不是 eligibility reject：Preserve/Resume 整体不拒绝 R-tree 场景。它只是
`warmcopy unsupported family`，受 per-target all-or-live fallback 控制。live fallback
仍可生成并导入现有 `predicate_locks_payload`。

独立诊断原因：

```text
predicate_lock_warmcopy_spatial_unsupported
predicate_lock_warmcopy_unsupported_shape
```

因此 v1 不维护 R-tree split/merge 的 old/new page dirty，也不实现 predicate warmcopy
store。后续若要优化 spatial workload，需要单独设计 R-tree predicate lock warmcopy。

### 7.4 TableLockObject

table lock 对象：

```text
TableLockObject {
  table_id
  lock_mode
  type_mode_bits
  is_autoinc
  seq
  state
}
```

合法 `lock_mode`：

```text
LOCK_IS
LOCK_IX
LOCK_S
LOCK_X
LOCK_AUTO_INC
```

payload/validator 结构上可以识别 `LOCK_S` / `LOCK_X`，但 v1 不因此承诺支持用户
`LOCK TABLES READ/WRITE` 场景。普通事务主要产生 `LOCK_IS` / `LOCK_IX`，AUTO_INC
作为特殊 table lock 处理；`LOCK TABLES` 仍因 SQL session locked-tables state 被
eligibility reject。

AUTO_INC 作为 table-lock subtype 处理。seal 时必须确保：

```text
table_locks_payload_has_autoinc == metadata.autoinc_lock_owned
```

不一致时 table family invalid。

table lock family 的 seal 点分两段：

```text
pre-prepare:
  验证 table lock shape、count limit、AUTO_INC candidate
  生成 candidate payload / count

post-prepare:
  在当前代码重新导出 table_locks_payload 的位置重验
  比较 table lock generation、table_id/mode/AUTO_INC set 和 candidate payload
  通过后才复用 warmcopy payload
```

如果 post-prepare 重验发现 table lock set、AUTO_INC ownership、table lock count 或
payload canonical form 与 candidate 不一致，table family invalid。fallback `ON` 时走现有
post-prepare `trx_preserve_export_table_locks()` live export；fallback `OFF` 时在写 snapshot
前 reject。record-lock final fence 不替代 table-lock post-prepare revalidate。

### 7.5 MdlTicketObject

MDL ticket 对象：

```text
MdlTicketObject {
  key_namespace
  key_payload
  mdl_type
  duration
  ordinal_in_transaction_list
  seq
  state
}
```

`MDL_ticket` 在 8.0.22 中没有可持久依赖的稳定 ticket id。`MdlTicketObject` 的身份
不能建立在虚构的 `ticket_local_id` 上；v1 使用
`{key_namespace, key_payload, mdl_type, duration}` 加 transaction-duration list ordinal
与现有 exporter 顺序做 seal 校验。指针地址、fast-path 标记和 wait/granted queue
内部状态都不能进入 snapshot 语义。

v1 只接受：

```text
duration == MDL_TRANSACTION
namespace in mdl_preserve_namespace_supported()
```

不持久化：

```text
MDL_lock *
m_is_fast_path
m_hton_notified
PSI handle
fast-path counters
MDL_lock wait/granted queue 内部状态
```

MDL store 必须保存 transaction-duration list 顺序，因为：

- `export_preserved_locks()` 用 list 顺序生成 descriptor ordinal；
- `export_savepoint_ordinals()` 也按 list 顺序定位 savepoint sentinel；
- rollback to savepoint 依赖“从 front 释放到 sentinel”的语义。

MDL delta：

```text
UPSERT_TICKET
DELETE_TICKET
PATCH_TICKET_TYPE
MOVE_TICKET_DURATION
INVALIDATE_MDL_CONTEXT
SEAL_MDL_CONTEXT
```

如果出现 `MDL_STATEMENT` 或 `MDL_EXPLICIT`，v1 MDL warmcopy invalid。fallback
开启时，live export 也会按当前规则拒绝这类 context；fallback 关闭时，直接以
明确 warmcopy reason 拒绝。

`mdl_descriptors_payload` 必须始终使用当前 `export_preserved_locks()` 的 wire
format。即使没有任何 transaction-duration MDL ticket，也不能生成空串，而必须生成
合法的 zero-count payload：

```text
u32 descriptor_count = 0
```

snapshot/recovery 路径要求 MDL descriptor TLV 存在且至少包含 count 前缀。lock
warmcopy seal 阶段必须用同一格式生成 payload，并让 validator 覆盖 zero-count case。

### 7.6 ImplicitNativeValidation

当前 preserve 会调用 `trx_preserve_materialize_implicit_locks()`，把隐式锁语义
物化为可导出的显式 record locks。这一步可能很重，也会改变 live transaction
的显式锁集合。

v1 的默认设计不把 implicit lock 当作需要 warmcopy 的对象，也不把它们生成到
`record_locks_payload`。implicit record X-lock 的恢复依赖 InnoDB 原生逻辑：

```text
clustered record DB_TRX_ID / secondary page max trx id / undo version chain
+ recovered preserved trx id
+ rw_trx_list / rw_trx_set active membership
= native implicit X-lock can still be inferred lazily
```

phase 1 可以做非侵入式 native validation exact coverage / proof metadata，但它只用于
证明 resume 后原生判断可继续成立，不用于生成显式锁 payload。

implicit coverage 必须是 exact record-key set，不允许 page/table 级近似集合被标记为
validation success。每个 implicit entry 至少包含：

```text
table_id
index_id
space_id
page_no
heap_no
record_image_digest
trx_id
```

`record_image_digest` 必须来自与普通 record lock `record_images` 同源的逻辑 record
image，不能来自物理 byte offset。secondary index implicit lock 需要能回查或证明
clustered record / undo version chain 仍能把该 record 归属到原 `trx_id`。如果扫描预算
不足、页或 undo 信息不可读、record image 不可生成、index/table 形态暂不支持，或者只能
形成 page/table 级近似覆盖，native validation 必须 invalid。

exact coverage 的构造算法必须显式写进实现，而不能只保存一个“validation summary”：

1. 枚举范围来自该 target 事务已经参与修改的 InnoDB table/index/page 证据，受
   `preserve_trx_max_modified_tables`、`preserve_trx_max_scan_pages`、内存预算和
   phase-local timeout 共同约束；不能在 drain 中无界全库扫描；
2. clustered index entry 必须能从 record image、`DB_TRX_ID` 或 undo/version chain
   证明该 record 当前 implicit X-lock 归属原 `trx_id`；
3. secondary index entry 只能把 page max trx id 当成过滤条件，不能把它当成成功证明。
   它还必须回查或证明对应 clustered record / undo version chain，确认该 secondary
   record 仍能按 InnoDB 原生路径命中原 `trx_id`；
4. 每个 successful proof 生成一条 exact entry，并记录 proof source generation。若
   同一 record 在 journal open 期间被 engine-side conversion 物化为 explicit `lock_t`，
   该 entry 必须从 implicit set 删除或标记 excluded；
5. 枚举过程中遇到 missing page、purged/不可读 undo、unsupported index shape、temp /
   missing table、spatial predicate shape、预算超限或 proof cost 超限，不能降级成
   page/table 级成功，只能让 implicit native validation invalid；
6. invalid 后的行为仍由 fallback 参数决定：fallback `ON` 走当前 live materialization /
   export，fallback `OFF` 在 prepare 前 reject。

validation 谓词：

1. preserved trx 将以原 `trx_id` 恢复进 `rw_trx_list` / `rw_trx_set` 等 running trx
   structures；
2. clustered record 的 `DB_TRX_ID`、secondary page max trx id、undo/version chain 与
   page record image 能重新证明 exact coverage 中每条 record 的 implicit X-lock；
3. 不安装显式锁，不调用 `lock_rec_add_to_queue()`；
4. 不把 implicit-only record 计入 `preserve_trx_max_lock_count`；
5. explicit record family 覆盖的 record 从 implicit coverage 中排除；
6. implicit coverage 与 explicit record coverage exact disjoint；
7. native validation 失败时，fallback `ON` 走当前 live materialization/export；
8. native validation 失败且 fallback `OFF` 时，prepare 前 reject。

explicit coverage 的 authority 是 record warmcopy store 和 phase 2 seal 后生成的
payload；implicit native validation 不能独立声明这些 record 仍需要 native implicit
恢复。若 phase 1 期间引擎侧物化导致 explicit lock 出现，这些 record 按 explicit lock
路径恢复，implicit coverage store 只保留剩余 implicit-only exact set。

legacy materialization 仍保留，但定位改变为兼容兜底路径：

```text
native implicit validation success
  -> 不导出 implicit lock payload，resume 后依赖 InnoDB 原生懒判断

native implicit validation failure + fallback ON
  -> 在 LOCK_PREFLIGHT 中调用当前 materialize/export 路径

native implicit validation failure + fallback OFF
  -> fail closed
```

这样能避免大事务 implicit-only 写集合在 phase 2 被按行物化，同时仍能在无法证明
native 语义成立时回到当前保守路径。

## 8. Journal 与 hook 点

### 8.1 journal 顺序

必须先打开 journal，再开始 base scan。否则可能出现：

```text
scanner 已扫过某容器
新锁创建
journal 尚未打开
=> 这把锁既不在 base snapshot，也不在 delta journal
```

正确顺序：

```text
open epoch
open per-target journal
publish journal admission
start base scan
business continues and appends deltas
enter WARMCOPY_CLOSING
close journal admission
drain admitted deltas
seal per-target snapshot
```

当前 DRAIN 流程不是一开始就拥有最终 target list：participant phase 1 先打开，随后
target counter/quiesce 才形成需要 preserve 的目标集合。因此 lock warmcopy 的 journal
admission 必须明确覆盖范围：

1. phase 1 打开时发布 epoch-level admission，允许当前和后续候选 THD 在发生锁变化时
   追加 journal；
2. target 被 drain 发现时，为该 target 绑定或创建 `Lock_warmcopy_target_session`；
3. 如果 target 在 session 创建前已经有 journal 覆盖，则从对应 cursor 开始 base scan
   并应用后续 delta；
4. 如果无法证明该 target 从 base scan 前就被 journal 覆盖，不能把它标成
   `SEALED_VALID`；
5. 对未覆盖 target，fallback `ON` 时直接 live export，fallback `OFF` 时 prepare 前
   reject；
6. 预算耗尽或 admission gap 只影响对应 target，除非是 epoch-level 基础设施失败。

每条 delta 至少携带：

```text
epoch_id
target_id
family
object_key
seq
operation
payload
```

`seq` 不要求全局有序，但同一 target/family 内必须足以发现丢失或乱序。

### 8.1.1 Hook CS + Fence 线性化契约

v1 采用 `Hook CS + Fence`，不把 phase 2 全量 live rescan 作为正常路径。它的含义是：

```text
base scan 负责建立 candidate
hook delta 负责记录 scan 后变化
phase 2 fence 负责证明 candidate 没有漏变更
```

具体约束：

- journal append 必须与对应锁 mutation 在同一临界区内完成。record bitmap/object hook
  必须绑定 InnoDB page shard latch / `trx_mutex` 保护的 mutation；MDL hook 必须绑定
  MDL context/list mutation。不能先改 lock bitmap/list，再异步尝试补 journal。
- 如果某条 mutation 无法在同一临界区内可靠追加 delta，必须在同一临界区内至少把
  对应 shard/family 标记为 dirty 或 invalid。
- clean shard 只有同时满足 cursor 覆盖、无 seq gap、无 dirty 标记、final fence
  未变化，才能复用 phase 1 candidate。
- dirty shard 必须在 phase 2 阻止目标继续变化后重验；重验失败或无法证明覆盖时，
  fallback `ON` 走 live export，fallback `OFF` reject。
- clean/dirty 判定不能只依赖 `trx_locks_version`。bitmap set/reset 可能改变
  `n_rec_locks` 和 bitmap，但不一定增加 `trx_locks_version`，因此 record family 必须有
  mandatory per-shard mutation generation 和 canonical shard fingerprint。
- `n_rec_locks` 只能在全局 lock_sys latch 下作为 cross-check 或 diagnostic fast-fail。
  它不能作为未加 latch 的 lightweight correctness fence。
- final fence recheck 到 `ha_prepare_low()` 入口之间必须是原子的，但不能通过持有
  `lock_sys` global exclusive latch 跨 `ha_prepare_low()` 来实现。InnoDB prepare 路径
  可能写/flush redo，源码也要求此处不能持 mutex/latch。v1 必须以 per-trx
  conversion admission barrier 作为 primary 机制，阻止其它会话在 recheck 之后为目标
  trx 执行 `lock_rec_convert_impl_to_expl_for_trx()`。不能只依赖 journal cursor、
  dirty generation 或 shard fingerprint 来覆盖这个窗口；这些 fence 只能证明 recheck
  采样之前没有漏变更。

推荐 fence 由以下信息组成：

```text
trx_locks_version
n_rec_locks (global-latch cross-check only)
record lock count
table preflight count (diagnostic only; final table payload is post-prepare)
per-family journal cursor
per-family dirty generation
inflight journal count
mandatory per-shard mutation generation / canonical shard fingerprint
per-trx conversion freeze generation
conversion_freeze_wait_epoch
conversion_unhandled_after_freeze (must be false)
```

final-recheck protection 的 primary 机制是 per-trx conversion admission barrier：

```text
set freeze:
  hold trx->mutex
  set trx.lock_warmcopy_conversion_frozen = true
  bump trx.lock_warmcopy_freeze_generation
  clear conversion_attempt_after_freeze
  initialize/bump conversion_freeze_wait_epoch

conversion path:
  lock_rec_convert_impl_to_expl_for_trx() already enters trx->mutex
  before lock_rec_add_to_queue()
  if lock_warmcopy_conversion_frozen:
    do not add explicit LOCK_REC_NOT_GAP
    set conversion_attempt_after_freeze
    return internal status LOCK_IMPL_CONVERT_FROZEN
    do not translate this to DB_SUCCESS or DB_UNSUPPORTED inside the helper

clear freeze:
  after ha_prepare_low() returns or after fallback/reject is selected
  hold trx->mutex
  clear lock_warmcopy_conversion_frozen
  bump conversion_freeze_wait_epoch
  broadcast conversion-freeze waiters
```

如果 conversion attempt 在 freeze 后发生，不能让调用者当作“没有冲突”继续执行，也不能
静默安装 explicit lock。这里的合同不是“wait / retry / unsupported 三选一”，而是
唯一的 frozen 状态传播合同：

1. `lock_rec_convert_impl_to_expl_for_trx()` 返回一个内部可区分的
   `LOCK_IMPL_CONVERT_FROZEN` 状态；
2. `lock_rec_convert_impl_to_expl()`、`lock_rec_convert_active_impl_to_expl()` 以及
   preserve materialization 调用点必须把该状态向上传播，不能继续执行后续
   `lock_rec_lock(..., impl=true)` 并把它当作没有显式冲突；
3. 常规读写路径在收到 frozen 状态后必须释放 page shard latch、page latch、mtr 和
   `trx_mutex`，再等待目标 trx 的 `conversion_freeze_wait_epoch` 变化或 freeze 清除；
4. 等待有界：上限为
   `min(preserve_trx_lock_warmcopy_conversion_wait_timeout_ms, drain remaining timeout)`，
   并且必须响应 `KILL QUERY`、`KILL CONNECTION`、shutdown、drain abort；
5. 等待结束后从调用者的最外层 lock check 重新开始，重新走 implicit 判断、conversion 和
   `lock_rec_lock()`；不能在旧 page 指针、旧 record 指针或旧 mtr 上继续；
6. `SELECT NOWAIT` / `SELECT ... SKIP LOCKED` 不能在 preserve gate 上长时间等待；如果
   conversion freeze 阻止了显式化，它们按“该 record 当前不可立即取得”处理，分别返回
   nowait / skip-locked 语义，而不是放行读取；
7. 普通读写如果等待超时或被中断，不能返回一个没有注册等待对象的裸 `DB_LOCK_WAIT`。
   实现要么通过一个可唤醒的 preserve-gate wait object 返回现有等待路径，要么返回现有
   lock-wait-timeout / interrupted 错误，并记录
   `record_lock_warmcopy_conversion_wait_timeout` 或
   `record_lock_warmcopy_conversion_freeze_conflict`。

因此，conversion barrier 不负责替业务会话安装 waiting `lock_t`。它只负责在不能安全物化
目标 trx implicit lock 时，阻止“无显式冲突则成功”的错误路径，并把冲突会话导向可唤醒、
有超时、有中断处理的 preserve-gate wait/retry。一次被正确传播并正确等待/重试的
frozen attempt 不应单独使 target invalid；只有状态被吞掉、调用者继续按无冲突成功、
wait object 无法注册/唤醒、freeze 生命周期丢失或超时策略无法闭合时，才进入
fallback/reject。若已经进入 prepare，conversion 仍不能改变 frozen lock set。

`conversion_attempt_after_freeze` 是 observation；`conversion_unhandled_after_freeze` 才是
correctness failure。测试必须能分别制造“正确处理的 attempt”和“状态被吞掉/未传播”的
fault injection，避免把正常冲突访问误判为 payload 不正确。

`lock_sys` global exclusive latch 只能用于短临界区诊断或 `n_rec_locks` cross-check，
不能作为跨 `ha_prepare_low()` 的 protection。final-recheck protection window 内不得做
page IO、redo flush、record image 重算或任何可能获取 buffer pool page latch / mtr 的操作。

per-shard fence 必须有统一定义，避免不同实现各自发明：

```text
shard_key = {
  table_id,
  index_id,
  space_id,
  page_no,
  lock_type_mode,
  n_bits
}

mutation_generation:
  uint64, per target/family/shard, monotonic
  updated in the same critical section as every bitmap set/reset,
  record lock object add/remove, record image replacement,
  tombstone, dirty/invalid mark, and implicit->explicit conversion.

canonical_shard_fingerprint:
  SHA-256 over canonical_shard_semantic_bytes_v1.

diagnostic_page_fingerprint:
  optional SHA-256 over diagnostic_page_bytes_v1.
```

`canonical_shard_semantic_bytes_v1` 必须按下面的稳定 wire 规则构造：

```text
u32 version = 1
u64 table_id                 little-endian
u64 index_id                 little-endian
u32 space_id                 little-endian
u32 page_no                  little-endian
u32 lock_type_mode           little-endian
u32 n_bits                   little-endian
u32 bitmap_len               little-endian, must be ceil(n_bits / 8)
bytes normalized_bitmap      bits >= n_bits in the last byte must be zero
u32 set_bit_count            little-endian
repeat set_bit_count ordered by heap_no:
  u32 heap_no                little-endian
  bytes32 record_image_sha256
u32 shard_state_flags        dirty/invalid/tombstone bits
u64 mutation_generation      little-endian
u64 implicit_exclusion_generation little-endian
```

`diagnostic_page_bytes_v1` 只用于 fast-fail / dirty hint，不参与 canonical payload
equivalence，也不能作为普通 record lock 的强身份：

```text
u32 version = 1
u32 space_id                 little-endian
u32 page_no                  little-endian
u64 page_lsn                 little-endian
u32 page_n_heap              little-endian
```

如果 `diagnostic_page_fingerprint` 变化，正确动作是把 shard 标 dirty 并在 phase 2 用
record image 和 bitmap 重新证明；不能仅凭 `page_lsn` 或 `page_n_heap` drift 直接把
语义等价判为失败。反过来，即使 diagnostic page 字段未变，也不能绕过 record image
校验。

`record_image_sha256` 是对现有 record image payload bytes 的 SHA-256；它只能在 base scan、
dirty rescan 或 seal 中已经安全持有 page S-latch/mtr 的位置生成。final-recheck
protection window 内只能比较已保存的 semantic digest、generation、bitmap 和 diagnostic
page observation，不得重新读取 page 或重算 record image digest。

hot path 必须优先使用 cheap `mutation_generation`。`canonical_shard_fingerprint` 可以在
base scan、dirty rescan、seal 和 final recheck 时在相应 latch 下计算；如果实现选择
维护 rolling fingerprint，也必须和实际 mutation 在同一临界区更新。不能在
`lock_rec_set_nth_bit()` / `lock_rec_reset_nth_bit()` 热路径里引入 record image 哈希、
内存分配或额外 latch。验收时 clean shard 复用要求 generation 和 canonical fingerprint
都与 seal/final 样本一致；任何字段无法读取或序列化时，shard 不能保持 clean。

### 8.2 InnoDB hook

需要覆盖：

- record lock object 加入/移出 `trx_locks`；
- record bitmap set/reset；
- `lock_rec_convert_impl_to_expl_for_trx()`；
- `lock_rec_convert_active_impl_to_expl()`；
- record lock discard；
- table lock add/remove；
- AUTO_INC lock add/remove；
- page split/merge/reorganize；
- record move；
- savepoint create/release 对 ordinal/bookkeeping 的影响；
- rollback/rollback-to-savepoint 对锁的影响。

hook 关闭态必须接近零成本：只允许一个轻量分支判断，不得分配内存，不得获取额外 latch，
不得修改锁路径的既有锁顺序。开启态下，journal append、dirty 标记或 invalid 标记必须
发生在与锁 mutation 相同的临界区内。

实现时不能只按上面的类别做宽泛 hook。必须维护函数级 hook coverage table，并以
`lock0lock.cc` 中所有 record/table `Shard_latch_guard` mutation site 为审计入口。每个
site 必须分类为：

```text
JOURNAL_DELTA      mutation 可精确表达为 journal delta
DIRTY_SHARD        mutation 影响 page/record identity，phase 2 必须重验
INVALID_TARGET     v1 不支持该形态，target artifact invalid
READ_ONLY          只读检查，不改变 lock object / bitmap / trx_locks
UNREACHABLE        需要源码断言或测试证明 preserve target 不会走到
```

v1 最少要显式覆盖这些函数族；实际实现时若源码 grep 出更多 mutation site，不能因为本表
没列出就默认 safe：

```text
lock_rec_add_to_queue
lock_rec_set_nth_bit
lock_rec_reset_nth_bit
lock_rec_discard
lock_rec_reset_and_release_wait_low
lock_rec_move_low
lock_rec_move
lock_update_discard
lock_rec_reset_and_inherit_gap_locks
lock_rec_inherit_to_gap
lock_rec_inherit_to_gap_if_gap_lock
lock_rec_convert_impl_to_expl_for_trx
lock_rec_convert_active_impl_to_expl
lock_table_create / lock_table_enqueue_waiting / table lock add/remove sites
lock_unlock_table_autoinc
```

任何未分类 mutation site 都必须让 `record_lock_warmcopy_hook_coverage_incomplete` 触发，
不能把 target 标成 `SEALED_VALID`。

`lock_rec_convert_impl_to_expl()` 和 `lock_rec_convert_active_impl_to_expl()` 当前是普通
conversion wrapper；v1 实现不能保持“内部 conversion 失败但 wrapper 仍返回 void”的形态。
它们必须改成可传播 frozen 状态，或增加等价的 out-param / status channel，并要求
`lock_clust_rec_modify_check_and_lock()`、`lock_sec_rec_modify_check_and_lock()`、
`lock_sec_rec_read_check_and_lock()`、`lock_clust_rec_read_check_and_lock()` 等调用者在
进入后续 `lock_rec_lock()` 前处理该状态。否则 `lock_rec_lock(..., impl=true)` 只看显式锁
队列，可能把被 freeze 阻止显式化的 implicit X-lock 当成没有冲突。

implicit->explicit conversion 是 `Explicit Wins` 的关键 hook 点。journal open 期间发生
conversion 时，必须在同一 page shard latch / `trx_mutex` 临界区记录 explicit record
delta，并更新 implicit exclusion generation。若 conversion 发生在 seal 完成或 journal
close 之后，target artifact invalid；fallback `ON` 走 live export，fallback `OFF`
reject。

特别注意：`lock_rec_convert_impl_to_expl_for_trx()` 可能由其它会话的冲突访问触发。
因此目标会话的 command gate 只能阻止目标自己继续执行 lock/trx-capable 操作，不能阻止
其它会话把目标事务的 implicit lock 物化为 explicit lock。final recheck 之后必须通过
per-trx conversion admission barrier 关闭这条路径，直到 `ha_prepare_low()` 完成。这个
barrier 必须在 `lock_rec_convert_impl_to_expl_for_trx()` 已持有的 `trx_mutex` 临界区内
检查，位置必须早于 `lock_rec_add_to_queue()`。否则 recheck 之后新发生的 conversion
不会进入已经冻结的 payload。

还要处理 explicit->implicit 反向变化。若某条 record 因 engine-side conversion 被
`Explicit Wins` 排除出 implicit coverage，随后对应 explicit bit/object 又被释放、discard
或 move 到其它 record/page，不能让该 record 从两个集合中同时消失：

- v1 不在 hot hook 中 inline 重新加入 implicit exact coverage 作为 clean 成功路径；
- 必须把对应 implicit family / record shard 标 dirty，并在 phase 2 重新执行 exact
  implicit proof；proof 成功后才能重新纳入 implicit coverage；
- 若无法证明 implicit coverage，必须把 record shard 或 target 标记 invalid；
- 若这种反向变化发生在 seal/freeze 之后，target artifact invalid，fallback `ON` 回到
  live export，fallback `OFF` reject。

对复杂 B-tree 移动，不在 hook 中重放完整语义。优先：

```text
mark page shard dirty
phase2 rescan/revalidate
success -> seal
fail -> fallback or reject
```

R-tree / spatial predicate locks 不进入 warmcopy store。检测到 `LOCK_PREDICATE` 或
`LOCK_PRDT_PAGE` 时，直接把该 target 的 lock warmcopy artifact 标记 invalid；fallback
`ON` 丢弃该 target 全部 warmcopy 结果并走现有 live export，fallback `OFF` reject。

`ROLLBACK TO SAVEPOINT` 按 range-delete barrier 处理：如果不能精确映射出 savepoint 后
所有被释放或回退的 lock bits/objects，必须 dirty 对应 record/predicate/table family，
并在 phase 2 重验。高频 savepoint rollback 可能抵消 phase 1 收益，但不能 fail-open。

`SAVEPOINT` 创建和 `RELEASE SAVEPOINT` 也必须进入 bookkeeping：它们不一定直接改变
record bitmap，但会改变 savepoint ordinal、MDL sentinel / list position 以及
rollback-to-savepoint 的范围边界。无法精确维护 ordinal 和 release range 时，必须把
相关 family 或 target 标记 dirty/invalid，不能继续使用旧 ordinal seal。

### 8.3 MDL hook

需要覆盖：

- `MDL_ticket_store::push_front()`；
- `MDL_ticket_store::remove()`；
- `MDL_ticket::downgrade_lock()`；
- `MDL_context::set_lock_duration()`；
- `MDL_ticket_store::move_all_to_explicit_duration()`；
- `MDL_ticket_store::move_explicit_to_transaction_duration()`；
- rollback-to-savepoint release range；
- statement/explicit duration 出现。

bulk duration move 中如果无法精确维护 transaction list 和 explicit list 的边界，
应直接标记 MDL context invalid。

MDL duration 必须来自 `MDL_ticket_store` duration-list context。不能依赖
`MDL_ticket::get_duration()`，因为该字段在 8.0.22 release build 中是 debug-only。

MDL ordinal 也必须在 seal 时从最终 `MDL_TRANSACTION` list 重新派生，派生方式要与
`MDL_context::export_preserved_locks()` 的 list walk 保持一致。journal 事件里可以记录
ticket identity、duration list generation 和 dirty/invalid 标记，但不能把事件发生时的
ordinal 当成持久身份；`remove()`、`downgrade_lock()`、duration move 或 rollback-to-savepoint
range release 都可能让后续 ticket 的 ordinal 左移。

## 9. phase 1 流程

在 `WARMCOPY_DRAINING`：

1. drain orchestrator 打开 lock warmcopy participant；
2. 每个 target 创建 `Lock_warmcopy_target_session`；
3. session 打开 journal；
4. base scan 采集当前支持的显式锁对象；
5. 采集 implicit native validation exact coverage / proof metadata，不安装显式锁；
6. 业务继续运行，显式锁变化通过 journal 追加；
7. object store 应用 delta 或标记 dirty；
8. unsupported family 记录明确原因。

phase 1 不能长时间持有全局锁，也不能阻塞普通 lock acquire/release。hook 开销必须
小且有预算保护。预算耗尽时 degrade target，而不是无限增长内存。

对 record locks，phase 1 的产物不是最终 snapshot 字段，而是可被 seal 的候选状态：

- base scan 产生 `{table_id, index_id, space_id, page_no, type_mode, n_bits}` 维度的
  lock entry；
- bitmap 表示该 page 上哪些 `heap_no` 当前被这个 lock entry 覆盖；
- record image / diagnostic page observation 可以在 phase 1 预采集，用来减少 phase 2 的
  页访问；
- 后续 acquire / release / bitmap 变化写入 journal；
- 无法直接证明顺序或状态的 shard 标记为 dirty；
- 如果出现 unsupported lock mode、unsupported record image shape、预算超限或
  journal gap，该 target 进入 warmcopy invalid / unsupported 状态。

这个阶段允许“候选状态与实时状态短暂不一致”，因为业务仍在运行。正确性依赖
phase 2 的 admission close、tail apply 和 seal，而不是依赖 phase 1 某个瞬间的
快照天然稳定。

## 10. phase 2 流程

在 `WARMCOPY_CLOSING`：

1. command gate 阻塞目标会话新的 lock/trx-capable 操作，并保持到
   `ha_prepare_low()` 返回；
2. 关闭 lock warmcopy journal admission；
3. 等待已接收 delta 收敛；
4. 重验 dirty shards；
5. 重验 MDL ordered list；
6. 重验 implicit native continuity 条件；
7. 校验 diagnostic page observation、bitmap、table/AUTO_INC、MDL namespace，并确认没有 warmcopy
   unsupported predicate locks；
8. 生成现有 preserve payload；
9. 标记 target 为 `SEALED_VALID`、`SEALED_INVALID` 或 `SEALED_UNSUPPORTED`。

seal 必须校验：

- 没有 seq gap；
- dirty shard 均已解决；
- 普通 record lock 的 bitmap bounds 和 `record_images` 仍匹配；
- 不存在 `LOCK_PREDICATE` / `LOCK_PRDT_PAGE`；若存在则 lock warmcopy artifact invalid，
  按 fallback/reject 处理；
- record lock count 和 table-lock preflight count 不超过 `preserve_trx_max_lock_count`；
- MDL ticket 顺序可证明；
- MDL namespace/type/duration 当前支持；
- implicit native validation 成功，或 fallback 策略允许回到 live materialization；
- `autoinc_lock_owned` 与 table-lock candidate 一致，并等待 post-prepare 重验；
- 生成的 payload 能通过现有 validator；
- 最终 snapshot 不超过 `preserve_trx_max_snapshot_bytes`。
- seal 完成后到 `ha_prepare_low()` 入口的 final fence 在 protection window 内保持不变。

对 record locks，phase 2 的核心是把 phase 1 候选状态提升为 pre-prepare contract：

1. 停止接受新的 lock warmcopy journal 事件；
2. 等待已经进入的 acquire/release/bitmap delta 全部应用；
3. 在目标事务被阻止继续变化后，对 dirty shard 做最终重验；
4. 对仍然有效的普通 record entry 重验 bitmap bounds 和 record image；
5. 确认 target 没有 spatial predicate locks；若存在则 artifact invalid；
6. 重新计算 record lock count；table lock count 只作为 preflight diagnostic，最终 table
   payload 以后面的 post-prepare revalidate 为准；
7. 序列化成现有 resume 能识别的 `record_locks_payload`，warmcopy 路径不生成
   `predicate_locks_payload`；
8. 调用现有 validator，确保 payload 能被 import 路径解析；
9. 保存 seal fence；
10. 在 preserve kernel 进入 `ha_prepare_low()` 前，持 `trx_mutex` 设置 per-trx
    conversion freeze，并发布可唤醒的 `conversion_freeze_wait_epoch`；
11. freeze 生效后做 final fence recheck，且 recheck 不能做 page IO 或 record image
    重算；
12. recheck 通过后进入 `ha_prepare_low()`；期间不持全局 `lock_sys` latch，依赖 per-trx
    conversion freeze 阻止其它会话物化目标 trx 的 implicit lock；
13. `ha_prepare_low()` 返回后清除 conversion freeze 并唤醒 preserve-gate waiters，或在
    fallback/reject 前清除并唤醒。

final fence 至少比较：

```text
trx_locks_version
n_rec_locks (global-latch cross-check only)
record lock count
inflight journal count
dirty shard set/generation
mandatory per-shard mutation generation / canonical shard fingerprint
MDL ordered-list generation
implicit/explicit disjointness generation
per-trx conversion freeze generation
conversion_freeze_wait_epoch
conversion_unhandled_after_freeze (must be false)
```

final-recheck protection window 的实现要求是硬约束：

```text
acquire trx->mutex
set per-trx conversion freeze
release trx->mutex
sample final fence
compare with seal fence
enter ha_prepare_low()
return from ha_prepare_low()
acquire trx->mutex
clear per-trx conversion freeze
bump conversion_freeze_wait_epoch
broadcast waiters
release trx->mutex
```

如果实现无法证明 `lock_rec_convert_impl_to_expl_for_trx()` 在 freeze 后一定先检查
conversion barrier 再创建 explicit lock，则该 target 的 warmcopy artifact invalid。不能
用下一次 generation/fingerprint recheck 弥补，因为已经冻结的 payload 不会包含窗口内新
发生的 implicit->explicit conversion。

如果第 3 至第 13 步任意失败，不能把 phase 1 的候选 payload 勉强写入 snapshot。
fallback `ON` 时丢弃该 target 的锁 warmcopy 结果并在 `LOCK_PREFLIGHT` 现场导出；
fallback `OFF` 时直接拒绝该 target 的 preserve。

`ha_prepare_low()` / InnoDB prepare 内部本身可能发生锁集合变化，例如 READ COMMITTED 及
以下隔离级别会在 prepare 中释放部分 GAP/read locks。v1 的目标不是改变这个语义，而是
保持 warmcopy-on 与当前 live export 路径等价：record lock payload 仍是当前代码的
pre-prepare contract，prepare-internal release/inheritance 必须进入源码复核清单和等价
测试。也就是说，如果当前 live export 会在 `LOCK_PREFLIGHT` 记录某个 pre-prepare lock，
warmcopy 也必须记录；如果未来要把 record payload 改成 post-prepare parity，那是另一个
语义变更，不能混入本设计。

gate 连续性是硬约束：不能 seal 后释放目标会话，再在稍后重新获取 gate 进入
`ha_prepare_low()`。final-recheck protection 连续性同样是硬约束：不能 recheck 后清除
per-trx conversion freeze，再在稍后重新 freeze 进入 prepare。如果任一保护发生语义上的
release/reacquire，必须把该 target 的 warmcopy artifact 标记 invalid 并按 fallback/reject
处理。

对 table locks，phase 2 不能只依赖 pre-prepare fence。它必须在 `ha_prepare_low()` 后、
当前代码会重新导出 `table_locks_payload` 的语义点执行 post-prepare revalidate：

1. 读取当前 table/AUTO_INC lock set；
2. 与 warmcopy candidate 的 canonical table payload 比较；
3. 校验 `autoinc_lock_owned` 与 table payload 的 AUTO_INC bit 一致；
4. 若完全一致，可复用 warmcopy payload；
5. 若不一致，fallback `ON` 走现有 post-prepare live export，fallback `OFF` reject；
6. post-prepare table drift 必须记录 `table_lock_warmcopy_post_prepare_drift`。

## 11. preserve kernel 消费规则

preserve 某个 target 时：

```text
if lock_warmcopy_snapshot.valid:
    use warmcopy-generated explicit lock payloads
    accept implicit native validation result
else if preserve_trx_lock_warmcopy_fallback_to_live_export:
    discard all lock warmcopy payloads for this target
    run current live export path in LOCK_PREFLIGHT
else:
    reject preserve before prepare
```

v1 fallback 是 per-target all-or-live，不做 per-family 混用。也就是说，只要某个
target 的任一必需显式锁族或 implicit native validation invalid，fallback 开启时
该 target 的所有 lock warmcopy payload 都丢弃，统一走 live export。

这样做是为了避免这些语义来自不同时间点：

- record lock pre-prepare contract；
- predicate payload identity（live fallback only）；
- table/AUTO_INC cross-check；
- MDL descriptor order；
- savepoint ordinal。

后续可以设计 per-family fallback，但 v1 不做。

### 11.1 canonical payload equivalence

payload 等价门禁比较的是语义集合，不是当前 wire bytes。live export 通常按
`trx_locks` list / MDL duration list 顺序输出，warmcopy 则按 object store / shard
顺序生成；直接比较原始 bytes 会把非语义顺序差异误判为失败，也可能漏掉 MDL ordinal
这类真正有语义的顺序差异。

debug/test 模式下的 comparator 必须：

1. 分别 parse live export 与 warmcopy seal 生成的 payload；
2. 转成 typed canonical entries；
3. 按每个 family 的稳定规则排序或保留语义顺序；
4. 重新编码成 versioned canonical bytes，或直接做 typed semantic equality；
5. 失败时输出 family、entry key、字段差异和来源路径，不能只输出 raw digest mismatch。

canonical key 规则：

```text
record_locks_payload:
  sort by {
    table_id,
    index_id,
    space_id,
    page_no,
    lock_type_mode,
    n_bits
  }
  compare:
    normalized bitmap bytes,
    set heap_no list,
    record_image_digest for every set bit,
    canonical_shard_semantic_fingerprint.
  diagnostic_page_fingerprint is only a dirty/fast-fail hint and is not part of
  semantic equality.

table_locks_payload:
  sort by {
    table_id,
    lock_mode,
    autoinc_owned
  }
  compare:
    exact table lock mode and AUTO_INC cross-check fields.

mdl_descriptors_payload:
  preserve transaction-duration list ordinal as semantic order.
  ordinal must be derived at seal time by walking the final MDL_TRANSACTION
  duration list, matching export_preserved_locks(); journal-time ordinal is not
  persistent because mid-list remove/duration move can shift positions.
  entry key is {
    transaction_list_ordinal,
    namespace,
    key_payload,
    mdl_type,
    duration
  }.
  duplicate descriptors are not collapsed.
  savepoint ordinal mapping must be compared with the same ordinal source.

predicate_locks_payload:
  lock warmcopy valid path must not generate predicate payload in v1.
  If spatial predicate locks exist, this test expects fallback/reject behavior.
  Predicate payload equivalence is only compared for live export fallback paths,
  using the existing LOCK_PREDICATE / LOCK_PRDT_PAGE wire shape parser.
```

canonical comparator 可以参考现有 binlog warmcopy 等价测试中“parse 后规范化再比较”的
思路，但锁 payload comparator 必须独立定义上述 entry schema，不能复用 binlog event
byte-level comparator。

## 12. 失败原因

必须保留稳定、可观测的失败原因。建议原因集合：

target/session eligibility reason：

```text
session_locked_tables_mode_present
session_user_level_lock_present
session_handler_open_present
global_read_lock_present
backup_lock_present
locking_service_lock_present
session_or_global_state_not_preservable
```

这些 reason 表示当前目标不属于 v1 Preserve/Resume 事务快照模型，不受
`preserve_trx_lock_warmcopy_fallback_to_live_export` 控制。

lock warmcopy seal / validation reason：

```text
lock_warmcopy_journal_not_open
lock_warmcopy_journal_incomplete
lock_warmcopy_seq_gap
lock_warmcopy_fence_mismatch
lock_warmcopy_memory_budget_exceeded
lock_warmcopy_journal_budget_exceeded
lock_warmcopy_spill_failed
lock_warmcopy_spill_checksum_mismatch
lock_warmcopy_spill_torn_segment
record_lock_warmcopy_dirty_shard_unresolved
record_lock_warmcopy_diagnostic_page_drift
record_lock_warmcopy_count_limit
record_lock_warmcopy_bitmap_atomicity_lost
record_lock_warmcopy_hook_coverage_incomplete
record_lock_warmcopy_fingerprint_format_mismatch
record_lock_warmcopy_conversion_freeze_conflict
record_lock_warmcopy_conversion_wait_timeout
record_lock_warmcopy_conversion_status_not_propagated
predicate_lock_warmcopy_spatial_unsupported
predicate_lock_warmcopy_unsupported_shape
table_lock_warmcopy_invalid_mode
table_lock_warmcopy_autoinc_mismatch
table_lock_warmcopy_post_prepare_drift
mdl_warmcopy_statement_duration_present
mdl_warmcopy_explicit_duration_present
mdl_warmcopy_unsupported_namespace
mdl_warmcopy_order_drift
mdl_warmcopy_descriptor_limit
implicit_native_validation_failed
implicit_native_unsupported_shape
implicit_native_budget_exceeded
implicit_explicit_overlap_detected
seal_prepare_gate_released
final_recheck_protection_lost
savepoint_ordinal_drift
canonical_payload_equivalence_failed
replica_applier_target_unsupported
import_time_lock_conflict
```

fallback `ON` 时，lock warmcopy seal / validation reason 是 fallback reason；
fallback `OFF` 时，这些就是 reject reason。

如果 fallback 后 live export 也失败，最终对用户呈现 live export 的失败原因，因为
live export 是当前权威行为。

## 13. 资源与内存模型

锁 warmcopy 保存的是元数据，不是每行数据的完整副本。但内存增长仍需要严格约束。

主要内存来源：

- record shard map；
- bitmap；
- diagnostic page observation；
- heap offsets；
- ordinary record `record_images`；
- spatial predicate unsupported marker；
- ordered MDL ticket list；
- table lock entries；
- journal entries；
- dirty shard set；
- implicit native validation exact coverage / proof metadata。

record locks 应优先按 page bitmap 聚合，而不是每个 row lock 建一个 C++ 对象。
如果 10 万个 row locks 集中在少量 page 上，对象数量主要随 bitmap 和 shard 数增长；
如果锁分散在大量 page 上，metadata 成本会随 page shard 增长。但普通 record lock
仍需要为每个 bitmap set bit 保存 record image，因此字节成本还会随被锁 record 数量
和索引字段长度增长。这部分最终也受 `preserve_trx_max_snapshot_bytes` 约束。

资源模型至少要分别观测：

- record shard count；
- bitmap bytes；
- ordinary record image bytes；
- spatial predicate unsupported count；
- MDL descriptor bytes；
- journal bytes；
- spill bytes。

对于 1000 个大事务、每个事务 10 万锁的场景，仅开启 lock warmcopy 不够。还需要
评估并调大：

- `preserve_trx_max_lock_count`；
- `preserve_trx_max_snapshot_bytes`；
- drain timeout；
- warmcopy close timeout；
- `preserve_trx_lock_warmcopy_max_memory_bytes`；
- `preserve_trx_lock_warmcopy_max_journal_bytes`；
- `preserve_trx_lock_warmcopy_max_dirty_shards`；
- `preserve_trx_lock_warmcopy_max_mdl_descriptors`；
- spill file quota / filesystem budget。

v1 要求支持 spill-to-file。内存预算耗尽时必须先把 journal/object store 的可 spill
部分落盘；spill 仍失败、超出 spill 预算或无法保证顺序/完整性时，才 degrade target 并
根据 fallback 策略处理。任何情况下都不能无界增长内存，也不能在 spill 后丢失 seq /
dirty / fence 信息。

spill 文件是 drain 过程内的临时资产，不是跨重启恢复事实源。每个 spill segment 必须
携带 length、sequence range、checksum 和 target id；读取时校验 checksum 与 sequence
连续性。torn read/write、fsync/rename 失败、checksum mismatch、segment 缺失或 orphan
spill cleanup 失败，都必须让对应 target invalid 并按 fallback/reject 处理，不能把
不完整 spill 合成为 `SEALED_VALID`。

spill 布局必须避免跨 target 共享可写 segment。推荐形态：

```text
<server-local-preserve-tmp>/
  lock-warmcopy/
    batch-<batch_id>/
      target-<target_id>/
        manifest.tmp
        manifest
        segment-000001.tmp
        segment-000001.dat
        segment-000002.tmp
        segment-000002.dat
```

写入协议：

1. 每个 target 独占自己的目录和 segment 序列；
2. segment 先写 `.tmp`，写入 header、payload、footer checksum 和 sequence range；
3. fsync segment，再原子 rename 为 `.dat`；
4. manifest 记录 segment 列表、总 bytes、target id、batch id、highest seq、dirty /
   invalid generation；
5. fsync manifest 和父目录；
6. seal 读取时按 manifest 顺序校验每个 segment，不允许跳过坏 segment。

清理协议：

- target 成功 seal 并写入 snapshot 后立即删除本 target spill 目录；
- target invalid / fallback / reject / command abort 时删除本 target spill 目录；
- mysqld 重启或下一次 drain 初始化时扫描 `lock-warmcopy/batch-*`，清理没有活动 batch
  持有的 orphan spill；
- cleanup 失败要进入 observation 和 status，不能静默忽略。如果 cleanup 失败发生在
  seal 前，target invalid；发生在 seal 后，不影响已写 snapshot，但必须可审计。

## 14. 可观测性

现有 `Preserve_trx_warmcopy_*` 状态变量继续表示 binlog cache warmcopy，例如 prefix
bytes、digest bytes、durable bytes、provider full-copy count 和 binlog warmcopy phase2
pause。它们不应混入 lock warmcopy 统计。

新增 lock warmcopy 状态变量统一使用 `Preserve_trx_lock_warmcopy_*` 前缀：

```text
Preserve_trx_lock_warmcopy_attempts
Preserve_trx_lock_warmcopy_success
Preserve_trx_lock_warmcopy_fallback_to_live_export
Preserve_trx_lock_warmcopy_rejects
Preserve_trx_lock_warmcopy_eligibility_rejects
Preserve_trx_lock_warmcopy_memory_bytes
Preserve_trx_lock_warmcopy_journal_bytes
Preserve_trx_lock_warmcopy_spill_bytes
Preserve_trx_lock_warmcopy_dirty_shards
Preserve_trx_lock_warmcopy_fence_mismatches
Preserve_trx_lock_warmcopy_phase2_pause_us
Preserve_trx_lock_warmcopy_record_shards
Preserve_trx_lock_warmcopy_predicate_unsupported
Preserve_trx_lock_warmcopy_table_locks
Preserve_trx_lock_warmcopy_mdl_descriptors
Preserve_trx_lock_warmcopy_implicit_native_validated
Preserve_trx_lock_warmcopy_implicit_native_rejects
Preserve_trx_lock_warmcopy_payload_equivalence_failures
```

drain participant observation 中增加：

```text
target_thread_id
state
record_state
predicate_state
table_state
mdl_state
implicit_native_validation_state
eligibility_state
used_warmcopy
used_live_fallback
strict_reject
invalid_reason
fallback_reason
eligibility_reason
phase1_scan_us
phase2_seal_us
phase2_fence_recheck_us
spill_bytes
payload_equivalence_state
```

InnoDB lock count 和 MDL descriptor count 必须分开展示。status 变量需要定义清楚
统计口径：累计值、最近一次 drain 值、还是 phase-local 值。低基数 reason 应稳定，便于
MTR 和线上审计区分 `LOCK TABLES`、user-level lock、HANDLER、global read lock、
backup lock、locking service 与普通 warmcopy seal invalid。

## 15. 实现分片

### 15.1 Slice 1：框架和 no-op participant

- 添加 sysvars；
- 添加 status vars；
- 添加 `Lock_warmcopy_batch_drain_participant`；
- drain two-phase decision 改为 binlog warmcopy 或 lock warmcopy；
- 添加 no-op per-target session；
- 扩展 `Preserve_trx_kernel_request` 或 batch target wrapper 以传递 per-target lock
  warmcopy artifact；
- 明确 participant false 只表示全局基础设施失败，per-target invalid 通过 artifact
  表达；
- 处理 target discovery 晚于 epoch admission 的 reconcile / fallback / reject 规则；
- 添加 observation；
- 验证 lock warmcopy 关闭时现有行为不变。

### 15.2 Slice 2：table/AUTO_INC warmcopy

- 实现 `TableLockObject`；
- 添加 table lock journal hooks；
- pre-prepare seal 只生成 candidate payload / count；
- `ha_prepare_low()` 后在现有 table-lock export 语义点做 post-prepare revalidate；
- revalidate 通过才复用 warmcopy `table_locks_payload`，否则 fallback `ON` 走 live export，
  fallback `OFF` reject；
- 保留 `autoinc_lock_owned` cross-check；
- 覆盖 AUTO_INC mismatch、post-prepare drift、fallback、strict reject 测试。

### 15.3 Slice 3：MDL transaction ticket warmcopy

- 实现 ordered `MdlTicketObject` store；
- journal ticket push/remove/downgrade/duration move；
- 拒绝 statement/explicit duration；
- 拒绝 unsupported namespace；
- seal 生成现有 `mdl_descriptors_payload`；
- 覆盖 zero-count MDL payload，禁止把无 MDL 误写成空串；
- 复用现有 privilege recheck；
- 测试 savepoint ordinal、order drift、unsupported namespace、fallback。

### 15.4 Slice 4：record warmcopy 与 predicate fallback

- 实现 `RecordLockShardObject`；
- ordinary record lock store 必须保存 `record_images`；
- 检测 `LOCK_PREDICATE` / `LOCK_PRDT_PAGE` 并把 target 标记为 warmcopy invalid；
- journal bitmap set/reset；
- hook lock object add/remove；
- hook `lock_rec_convert_impl_to_expl_for_trx()` /
  `lock_rec_convert_active_impl_to_expl()`；
- 建立函数级 hook map，覆盖 bitmap set/reset、record move、split/merge/reorg、
  discard/inherit/update、savepoint create/release、rollback/rollback-to-savepoint；
- journal append 与 bitmap/object mutation 在同一临界区完成，无法做到时标 dirty /
  invalid；
- 每个 shard 维护 mandatory mutation generation 和 canonical shard fingerprint；
- explicit record family 与 implicit native validation 做 disjointness 校验；
- 实现 per-trx conversion freeze，作为 final-recheck protection window 的 primary 机制；
- 改造 conversion wrapper，使 frozen 状态能传播到 read/modify caller，并覆盖 bounded
  wait/retry、NOWAIT/SKIP LOCKED、中断和超时；
- seal 后保存 fence，freeze 后 final recheck，随后不持全局 latch 进入 `ha_prepare_low()`；
- page/heap drift 标记 dirty shard；
- seal 生成现有 record payload；predicate payload 仅由 live fallback 生成；
- 测试 next-key、gap、rec-not-gap、insert-intention、predicate fallback、diagnostic page
  drift。

### 15.5 Slice 5：implicit native validation

- 实现 implicit native validation exact coverage store 和 proof metadata；
- 实现 exact implicit coverage set：`{table_id,index_id,space_id,page_no,heap_no,
  record_image_digest,trx_id}`；
- 实现 clustered / secondary exact coverage 枚举算法，并把 modified table、scan page、
  memory、timeout 预算作为 validation 输入；
- 验证 preserved trx 以原 `trx_id` 恢复进 running trx structures；
- 验证 clustered/secondary page evidence、undo/version chain、secondary page max trx id
  能重新证明 exact coverage；
- 验证 implicit-only 写集合不生成显式 record lock payload；
- validation 预算超限、无法扫描、unsupported shape 或证据不完整时 invalid；
- native validation 失败时按参数进入 live materialization fallback 或 strict reject；
- 测试聚簇索引 implicit lock、二级索引 implicit lock、implicit 不阻塞 gap insert、
  modified table count、scan page budget、timeout、temp/missing/spatial unsupported 场景。

### 15.6 Slice 6：v1 spill 和大 batch 加固

- 添加 v1 必需的 journal/object spill；
- 定义 per-target spill 目录、manifest、segment header/footer、fsync/rename 和 orphan
  cleanup 协议；
- spill 后保留 seq、dirty、fence、target state、segment checksum 和 sequence range；
- torn read/write、checksum mismatch、fsync/rename failure、spill failure / quota
  exceeded 时 degrade target 并按 fallback/reject；
- 添加 abort cleanup；
- 添加 orphan artifact cleanup；
- 添加大量事务、大量锁 stress tests。

## 16. 测试计划

### 16.1 单元测试

- object store 幂等：`UPSERT`、stale `PATCH`、duplicate `DELETE`、tombstone
  precedence、seq gap；
- warmcopy 参数矩阵：binlog-only、lock-only、both-on、both-off 的 two-phase 决策；
- record shard bitmap merge；
- bitmap mutation + journal append atomicity；
- per-shard mutation generation 和 canonical shard fingerprint；
- canonical shard semantic bytes v1 / SHA-256 fingerprint 格式，以及 diagnostic page
  fingerprint 不参与语义等价；
- ordinary record image payload validation；
- live fallback predicate payload validation，分别覆盖 `LOCK_PREDICATE` 和
  `LOCK_PRDT_PAGE`；
- table/AUTO_INC cross-check；
- MDL ordered list、downgrade、duration move、ordinal export、zero-count payload；
- target discovery / journal admission 覆盖性状态机；
- seal fence / final recheck 状态机，包括 per-trx conversion freeze acquire/release
  约束；
- conversion frozen 状态传播：wrapper 不能吞掉 frozen，read/modify caller 不能继续走
  “无显式冲突则成功”的后续路径；
- implicit coverage exact set 和 implicit/explicit coverage disjointness；
- eligibility reject 与 warmcopy seal invalid 的 fallback 分层；
- implicit native validation state machine；
- fallback policy state machine；
- canonical payload comparator；
- canonical comparator 的 MDL ordinal、duplicate descriptor 和 predicate fallback case；
- memory/journal budget spill、checksum/torn read 和 degrade；
- spill fault injection：short write、fsync failure、rename failure、manifest torn、
  parent-dir fsync failure、orphan cleanup failure。

### 16.2 MTR

- `preserve_trx_lock_warmcopy_sysvars`：参数默认值、动态修改、不写 binlog，并验证
  `preserve_trx_warmcopy_enable` 只控制 binlog participant、
  `preserve_trx_lock_warmcopy_enable` 只控制 lock participant；
- `batch_drain_warmcopy_parameter_matrix`：覆盖 binlog-only、lock-only、both-on、
  both-off；验证 no-binlog + lock warmcopy 默认 `ON` 时仍进入 two-phase drain；
- `batch_drain_lock_warmcopy_table_autoinc`：table/AUTO_INC payload、post-prepare
  revalidate 和 cross-check；
- `batch_drain_lock_warmcopy_table_post_prepare_drift`：table lock 在 prepare 前后发生
  current-parity drift 时，fallback `ON` live export，fallback `OFF` reject；
- `batch_drain_lock_warmcopy_mdl_order`：MDL descriptor order 和 savepoint ordinal；
- `batch_drain_lock_warmcopy_mdl_zero_count`：无 transaction-duration MDL 时生成合法
  zero-count payload；
- `batch_drain_lock_warmcopy_mdl_unsupported`：statement/explicit duration、user、
  backup、locking service namespace 仍拒绝，且 reason 稳定；
- `batch_drain_lock_warmcopy_session_unsupported`：`LOCK TABLES`、`GET_LOCK()`、
  HANDLER 属于 eligibility reject，不受 fallback ON/OFF 影响；
- `batch_drain_lock_warmcopy_global_unsupported`：global read lock / backup lock 等
  server-level state 不生成可消费 artifact；
- `batch_drain_lock_warmcopy_payload_equivalence`：同一事务 warmcopy-on seal payload 与
  warmcopy-off live export payload 先 parse、按 canonical key 排序并重新编码后等价；
- `batch_drain_lock_warmcopy_implicit_explicit_overlap`：引擎侧 implicit->explicit 物化后，
  explicit wins 且 implicit coverage 排除该 record；必须覆盖其它会话冲突访问触发
  `lock_rec_convert_impl_to_expl_for_trx()` 的路径；
- `batch_drain_lock_warmcopy_seal_prepare_fence`：seal 后到 `ha_prepare_low()` 前任意锁
  fence 变化必须 fallback/reject；必须用 deterministic DEBUG_SYNC 或等价注入点覆盖
  final recheck 之后、prepare 入口之前的 conversion 窗口；
- `batch_drain_lock_warmcopy_final_recheck_atomic`：在 final fence 采样和
  `ha_prepare_low()` 入口之间尝试由其它会话触发
  `lock_rec_convert_impl_to_expl_for_trx()`，验证 per-trx conversion freeze 能阻止
  explicit lock 安装，并证明 frozen 状态被上层 caller 处理，不能继续走“无显式冲突则成功”
  的路径；
- `batch_drain_lock_warmcopy_conversion_wait_retry`：普通冲突 DML 撞到 conversion freeze 后，
  必须释放 latch/mtr，等待 freeze 清除或 wait epoch 变化，再从外层重试；覆盖 KILL、
  shutdown、drain abort 和 timeout 唤醒；
- `batch_drain_lock_warmcopy_conversion_nowait_skiplocked`：`NOWAIT` / `SKIP LOCKED` 撞到
  conversion freeze 时返回 nowait / skip-locked 语义，不能放行读取，也不能长时间阻塞；
- `batch_drain_lock_warmcopy_explicit_to_implicit`：engine-side conversion 后 explicit
  lock 又被 release/discard/move 时，必须 dirty 后 phase 2 reproof 或 invalid，不能在 hot
  hook 中 inline re-add 后继续声明 clean；
- `batch_drain_lock_warmcopy_bitmap_atomicity`：bitmap set/reset 与 journal delta 不能
  出现漏记 clean shard；同一 bit set/reset 后最终 bitmap 不变时，mutation generation
  仍必须变化；canonical shard fingerprint 必须能证明 clean shard 复用；
- `batch_drain_lock_warmcopy_record_bitmap`：phase 1 bitmap 增删；
- `batch_drain_lock_warmcopy_record_images`：普通 record lock payload 必须携带并校验
  record image；
- `batch_drain_lock_warmcopy_record_page_drift`：dirty shard fallback / strict reject；
- `batch_drain_lock_warmcopy_predicate_fallback`：spatial predicate locks 下 fallback
  `ON` 时 live export 成功，fallback `OFF` 时 reject；
- `batch_drain_lock_warmcopy_rollback_to_savepoint_barrier`：rollback-to-savepoint 作为
  range-delete barrier，无法精确更新时 dirty/revalidate；
- `batch_drain_lock_warmcopy_savepoint_bookkeeping`：覆盖 `SAVEPOINT` 创建、
  `RELEASE SAVEPOINT`、重复名称覆盖、MDL savepoint sentinel 和 ordinal drift；
- `batch_drain_lock_warmcopy_late_target`：target 在 `WARMCOPY_DRAINING` 期间才被发现时，
  根据 journal 覆盖性进入 seal、fallback 或 reject；
- `batch_drain_lock_warmcopy_implicit_insert_resume`：insert/update 后 preserve/restart，
  resume 前后同一 record 的 implicit X-lock 仍能阻塞冲突修改；
- `batch_drain_lock_warmcopy_implicit_secondary`：二级索引访问仍能通过 page max trx id、
  clustered record 和版本链撞到 preserved trx；
- `batch_drain_lock_warmcopy_implicit_gap_not_blocked`：implicit X-lock 不错误扩展成 gap /
  next-key lock；
- `batch_drain_lock_warmcopy_fallback`：fallback `ON` 下 warmcopy 失效但 live export 成功；
- `batch_drain_lock_warmcopy_no_fallback`：fallback `OFF` 下稳定 reject；
- `batch_drain_lock_warmcopy_interrupt_matrix`：disconnect、`KILL QUERY`、`KILL CONNECTION`、
  SIGTERM/shutdown、crash injection、lock wait during phase 1/seal/fallback；
- `batch_drain_lock_warmcopy_xa_autocommit_matrix`：XA PREPARE target、autocommit target、
  temp table、instant DDL metadata drift、其它会话持有 FTWRL/backup lock 时的
  eligibility / fallback / reject 行为；
- `batch_drain_lock_warmcopy_replica_applier`：replica/applier 线程目标进入明确
  eligibility / unsupported / reject 路径，不生成不可恢复 artifact；
- `batch_drain_lock_warmcopy_import_conflict`：resume/import record/table/MDL payload 时
  遇到现有锁冲突，必须 fail closed 并保留可审计 reason；
- `batch_drain_lock_warmcopy_reason_codes`：§12 每个 reason code 至少有一个
  deterministic MTR 或 fault-injection 覆盖；
- `batch_drain_lock_warmcopy_spill_budget`：memory budget 超限先 spill，spill 失败、
  checksum mismatch 或 torn segment 才 degrade；
- `batch_drain_lock_warmcopy_spill_fault_injection`：分别注入 segment short write、
  segment fsync failure、rename failure、manifest short write、manifest fsync failure、
  parent-dir fsync failure 和 orphan cleanup failure；
- `batch_drain_lock_warmcopy_no_binlog`：无 binlog 也能由 lock warmcopy 驱动 two-phase drain。

### 16.3 GUnit

- drain orchestrator：binlog-only、lock-only、both、neither；
- per-target session 状态机；
- 各锁族 payload generation validators；
- status counter accounting；
- cleanup 和 abort paths。
- canonical payload comparator；
- implicit coverage exact-set validator；
- MDL duration-list context validator；
- spill checksum / torn-read validator。
- final-recheck per-trx conversion freeze validator。
- conversion frozen status propagation validator。
- reason-code determinism validator：每个 §12 reason 必须有 deterministic unit / MTR
  覆盖，或显式标注只由 fault injection 触发。

### 16.4 live/stress

- lock warmcopy enabled 的 baseline 对比 preserve/resume 内容一致性；
- `lock_warmcopy_large_lockset_phase2_baseline` 固定默认 workload：
  1000 并发写事务、每事务 100000 record locks、每事务 100 table/MDL descriptors、
  3 次 warmcopy run + 3 次 live export baseline run，报告 p50/p95/p99/max；
- fallback ON/OFF 大锁数场景；
- page split 和 secondary index churn；
- force-kill：phase 1、phase 2 seal、seal 后 snapshot write 前、fallback live export 中；
- `BM_InnoDBLockWarmcopyDisabledRecordHotPath`：关闭态无分配、无额外 latch、无
  lock order 变化，record-lock acquire/release hot path 回归不超过 2%；
- `BM_InnoDBLockWarmcopyEnabledRecordHotPath`：开启态、未命中 active target 时只允许
  cheap branch/generation 判断；命中 active target 时单次 acquire/release 开销必须有
  单独报告，不能只用 phase2 pause 掩盖 hot path 回归；
- `lock_warmcopy_large_lockset_phase2_baseline`：phase 2 pause p95 必须低于 live export
  baseline，并在测试报告中同时输出 warmcopy / live p95、p99 和最大值；
- payload equivalence debug run：同一事务分别走 warmcopy seal 和 live export，比较
  `record_locks_payload`、`predicate_locks_payload`、`table_locks_payload`、
  `mdl_descriptors_payload` 的 canonical equivalence。

最高风险并发点必须有 deterministic 注入点，不能只依赖 stress 触发：

```text
lock_warmcopy_before_bitmap_mutation_journal
lock_warmcopy_after_bitmap_mutation_before_dirty_mark
lock_warmcopy_impl_to_expl_after_freeze_check
lock_warmcopy_impl_to_expl_before_return_frozen
lock_warmcopy_conversion_waiter_registered
lock_warmcopy_conversion_freeze_before_broadcast
lock_warmcopy_after_impl_to_expl_before_delta
lock_warmcopy_after_seal_before_final_recheck
lock_warmcopy_after_final_recheck_before_prepare
lock_warmcopy_spill_after_tmp_short_write
lock_warmcopy_spill_fsync_fail
lock_warmcopy_spill_after_tmp_write_before_rename
lock_warmcopy_spill_rename_fail
lock_warmcopy_spill_manifest_short_write
lock_warmcopy_spill_manifest_fsync_fail
lock_warmcopy_spill_after_rename_before_manifest_fsync
lock_warmcopy_spill_parent_dir_fsync_fail
lock_warmcopy_spill_orphan_cleanup_fail
```

这些名字是设计级要求，最终实现可调整命名，但测试必须能稳定卡住对应窗口。

## 17. 验收标准

实现满足以下条件才算完成：

1. `preserve_trx_lock_warmcopy_enable=OFF` 时，现有 preserve/drain 行为不变；
2. lock warmcopy 有效时，phase 2 使用 warmcopy payload，跳过对应昂贵 live lock export；
3. lock warmcopy 无效且 fallback `ON` 时，丢弃该 target 所有 lock warmcopy payload，
   并在 prepare 前走当前 live export；
4. lock warmcopy 无效且 fallback `OFF` 时，prepare 前 fail closed；
5. target/session eligibility reject 不受 fallback 控制，当前不支持的 explicit/session/global
   locks 继续不支持；
6. participant false 只表达全局基础设施失败，per-target invalid 通过 artifact 进入
   kernel 决策；
7. record-lock payload 保持 pre-prepare contract，ordinary record payload 包含
   `record_images`；
8. live fallback 生成的 predicate payload 区分 `LOCK_PREDICATE` 与 `LOCK_PRDT_PAGE`；
9. implicit-only 写集合不依赖 phase 2 全量物化，resume 后仍由原生 implicit lock 逻辑生效；
10. 如果 InnoDB 引擎侧把 implicit lock 物化为 explicit `lock_t`，必须采用
    `Explicit Wins`：该 record 由 explicit record family 负责，implicit native
    validation 覆盖集合必须排除该 record；
11. seal 时必须通过 cross-family disjointness 校验：同一
    `{table_id,index_id,space_id,page_no,heap_no}` 不能同时被声明为 native implicit 和
    exported explicit，无法证明则 target invalid；
12. base scan + journal 必须满足 `Hook CS + Fence`：journal append 与锁 mutation 在同一
    临界区完成，clean shard 只有在 cursor 覆盖、无 seq gap、无 dirty 标记、final fence
    未变化时才能复用；
13. bitmap set/reset、record lock object add/remove、implicit->explicit conversion、MDL
    list mutation 都必须有可证明的 journal / dirty / invalid 记录；不能证明时不能标记
    `SEALED_VALID`；
14. 从 phase 2 seal 开始到 `ha_prepare_low()` 返回前，目标事务不得再执行任何 lock /
    trx-capable 操作；如果 gate release/reacquire，artifact 必须 invalid；
15. 进入 `ha_prepare_low()` 前必须设置 per-trx conversion freeze，并在 freeze 后做
    final fence recheck；freeze 必须覆盖到 `ha_prepare_low()` 返回或 fallback/reject 决策
    完成。不能持全局 `lock_sys` latch 跨 `ha_prepare_low()`；如果 freeze 语义上
    release/reacquire 或无法证明 `lock_rec_convert_impl_to_expl_for_trx()` 会先检查
    barrier，artifact 必须 invalid；
    conversion freeze 的冲突会话行为必须使用 frozen 状态传播 + bounded wait/retry
    合同，wrapper 不能吞掉状态，caller 不能在没有显式化 implicit lock 的情况下继续当作
    无冲突成功；
16. final fence recheck 至少覆盖
    `trx_locks_version`、record lock count、inflight journal count、dirty shard
    set/generation、mandatory per-shard mutation generation / canonical shard fingerprint、
    per-trx conversion freeze generation、conversion-freeze-wait-epoch、
    conversion-unhandled-after-freeze=false、MDL ordered-list generation 和
    implicit/explicit disjointness generation；
    `n_rec_locks` 只能作为全局 latch 下的 cross-check / diagnostic fast-fail；
17. per-shard mutation generation 和 canonical shard fingerprint 必须定义统一输入字段；
    fingerprint 必须使用本文定义的 `canonical_shard_semantic_bytes_v1` 和 SHA-256，覆盖
    shard key、normalized bitmap、set-bit record image digest、dirty/invalid/tombstone
    状态和 implicit exclusion generation；`page_lsn` / `page_n_heap` 只属于
    diagnostic page fingerprint，不参与语义等价；
18. warmcopy-on 与 warmcopy-off 必须通过 canonical payload equivalence 门禁；禁止裸比
    当前 wire bytes，必须按本文定义的 typed canonical entries 比较 record/table/MDL，
    predicate payload 只在 live fallback 路径比较；
19. table locks 必须按 post-prepare current-parity family 处理：pre-prepare 只生成
    candidate，`ha_prepare_low()` 后必须重验 table/AUTO_INC set；drift 时 fallback `ON`
    走现有 post-prepare live export，fallback `OFF` reject；
20. MDL descriptor order 和 savepoint ordinal 稳定，无 MDL 时也生成合法 zero-count
    payload；MDL ordinal 必须在 seal 时从最终 transaction-duration list 重新派生，不能
    使用 journal-time ordinal；
21. MDL 持久身份不能依赖虚构的 `ticket_local_id`；v1 只能使用
    `{namespace,key,type,duration}+transaction-list ordinal` 和现有 exporter order 进行
    seal 校验；
22. R-tree / spatial predicate locks 不进入 lock warmcopy；fallback `ON` 走现有 live
    export/import 的 `predicate_locks_payload`，fallback `OFF` reject；
23. `SAVEPOINT`、`RELEASE SAVEPOINT` 和 `ROLLBACK TO SAVEPOINT` 必须维护 savepoint
    ordinal / sentinel / range-delete bookkeeping；无法精确更新 object store 时 dirty 对应
    family 并在 phase 2 重验或 target invalid；
24. implicit exact coverage 构造算法必须受 modified table、scan page、memory 和 timeout
    预算约束；clustered/secondary proof 不完整、预算超限或只能得到近似覆盖时 invalid；
25. explicit->implicit 反向变化必须被捕获：被 explicit family 覆盖过的 record 在 explicit
    bit/object 删除后，v1 必须 dirty 后 phase 2 reproof 或 invalid，不能 hot-hook inline
    re-add 后继续声明 clean，也不能从 explicit 和 implicit 两个集合中同时消失；
26. memory/journal 有明确上限，v1 必须支持 spill-to-file；spill segment 必须带
    length/checksum/sequence range，spill 失败、超 quota、checksum mismatch、torn
    segment、orphan cleanup 失败或无法保证 seq/dirty/fence 完整性时才 degrade target；
27. lock warmcopy disabled hook 必须无分配、无额外 latch、无 lock order 变化，只允许
    轻量分支；`BM_InnoDBLockWarmcopyDisabledRecordHotPath` 回归不得超过 2%；
28. enabled hook 路径必须有单独 benchmark 报告：未命中 active target 和命中 active
    target 分开统计，不能只看 phase2 pause；
29. 函数级 hook coverage table 必须覆盖所有 record/table lock mutation site；未分类
    mutation site 必须触发 `record_lock_warmcopy_hook_coverage_incomplete`，不能
    `SEALED_VALID`；
30. `lock_warmcopy_large_lockset_phase2_baseline` 下 phase 2 pause p95 必须低于 live
    export baseline，测试报告必须输出 warmcopy/live p95、p99、max；
31. 日志和 status 能区分 eligibility reject、warmcopy success、live fallback、strict
    reject、fence mismatch、spill failure、payload equivalence failure；
32. interrupt matrix 覆盖 disconnect、`KILL QUERY`、`KILL CONNECTION`、SIGTERM/shutdown、
    crash injection、phase 1/seal/fallback 中 lock wait，并补充 XA PREPARE target、
    autocommit、temp table、instant DDL metadata drift、其它会话 FTWRL/backup lock 和
    savepoint create/release、replica/applier target 和 import-time conflict；
33. §12 每个 reason code 必须有 deterministic 测试或明确 fault-injection 入口；
34. 最高风险并发窗口有 deterministic DEBUG_SYNC 或等价注入点，覆盖 bitmap/fence、
    implicit->explicit、conversion frozen 状态传播、seal->prepare 和 spill torn/fsync/rename
    window；
35. debug/release GUnit、目标 MTR、完整 preserve suite、live stress 验证通过。

## 18. 源码复核后对方案的修正点

这份设计不是直接照搬初稿，而是按当前代码复核后做了以下修正：

1. 明确 record-lock payload 是 pre-prepare contract，fallback live export 必须在
   `ha_prepare_low()` 前发生；
2. lock warmcopy 可以独立驱动 two-phase drain，不能依赖 binlog warmcopy；
3. MDL warmcopy 必须维护 transaction-duration list 顺序，不能只用 unordered map；
4. `preserve_trx_max_lock_count` 当前约束 record/predicate/table locks，MDL descriptor
   数量单独统计；
5. implicit lock 不在 phase 1 默认物化，也不默认生成 payload；默认走 native
   continuity validation，legacy materialization 仅作为 live/fallback 路径；
6. v1 fallback 采用 per-target all-or-live，不混用不同来源的锁族 payload；
7. unsupported explicit/session/global locks 属于 eligibility reject，不受 lock warmcopy
   fallback 控制，也不因 MDL object 模型存在而扩大支持范围；
8. ordinary record lock payload 必须保存 `record_images`，`page_n_heap` / 普通
   `heap_offsets` 不能被当成强身份；
9. predicate payload 必须区分 `LOCK_PREDICATE` 和 `LOCK_PRDT_PAGE` 的 wire shape，但
   v1 lock warmcopy 不生成 predicate payload；它只保留给 live fallback；
10. `mdl_descriptors_payload` 即使无 MDL ticket 也必须使用 zero-count wire format；
11. participant 合同必须区分全局基础设施失败和 per-target artifact invalid；
12. target discovery 晚于 phase 1 admission 时，必须证明 journal 覆盖性，否则按
    fallback 或 strict reject 处理；
13. InnoDB 引擎侧可能在普通访问路径把 implicit lock 物化为 explicit `lock_t`，所以
    implicit native validation 必须排除 explicit record family 已覆盖的 record；
14. `lock_rec_set_nth_bit()` / `lock_rec_reset_nth_bit()` 这类 bitmap mutation 不是都能
    只靠 `trx_locks_version` 证明完整性，record family 需要 bitmap/shard/family 级
    fence；
15. base scan + journal 不是简单的“先扫再补增量”，必须把 journal append 和锁 mutation
    放在同一临界区，或在同一临界区标记 dirty / invalid；
16. seal 到 `ha_prepare_low()` 之间仍有窗口，且 `lock_rec_convert_impl_to_expl_for_trx()`
    可能由其它会话触发；由于 `ha_prepare_low()` / InnoDB prepare 可能写/flush redo，
    不能持全局 `lock_sys` latch 跨 prepare，v1 必须使用 per-trx conversion admission
    barrier 作为 primary 保护；barrier 不能只阻止安装 explicit lock，还必须把 frozen
    状态传播到上层 read/modify caller，避免后续 `lock_rec_lock(..., impl=true)` 当成
    无显式冲突成功；
17. MySQL 8.0.22 的 `MDL_ticket` 没有适合作为持久身份的原生稳定 ticket id，因此文档不再
    使用 `ticket_local_id`；
18. predicate R-tree split/merge 会造成 page 归属变化，v1 不为它设计 warmcopy；检测到
    spatial predicate locks 时让 target artifact invalid，fallback `ON` 走现有 live
    export；
19. `ROLLBACK TO SAVEPOINT` 对锁集合是范围删除语义，v1 不能用单个 object-key delete
    含糊表达；无法精确更新时必须 dirty family 并重验；
20. spill-to-file 是 v1 required，不是后续增强；否则大锁数场景会退化为内存预算失败或
    phase 2 live export 峰值；
21. payload 等价不能裸比当前 wire bytes，因为 live export 按 `trx_locks` list 顺序输出，
    warmcopy 按 object/shard store 输出；必须使用 canonical comparator；
22. implicit native validation 必须定义 exact coverage set 和判定谓词，不能只写
    validation summary；
23. `n_rec_locks` 读取要么持全局 lock_sys latch 做 cross-check，要么只作诊断 fast-fail，
    不能作为无 latch correctness fence；
24. MDL duration 要从 duration-list context 获得，不能依赖 release build 不存在的
    debug-only ticket duration 字段；
25. per-shard fingerprint 必须定义输入字段和维护时机；热路径以 generation 为主，
    semantic canonical fingerprint 在 latch 下生成/校验，diagnostic page fingerprint
    只能作为 dirty/fast-fail hint，避免把 `page_lsn` / `page_n_heap` 当成语义身份，也避免
    把 record image hash 放进每次 bitmap mutation；
26. `SAVEPOINT` create/release 会改变 ordinal/bookkeeping，不能只处理
    `ROLLBACK TO SAVEPOINT`；
27. spill-to-file 必须定义 per-target segment/manifest 布局、fsync/rename 顺序和 orphan
    cleanup，不是一个抽象的“可落盘”开关；
28. 等价测试必须使用 typed canonical payload comparator，并对 MDL ordinal、duplicate
    descriptor 和 predicate fallback case 有明确规则；
29. 最高风险并发窗口必须有 deterministic DEBUG_SYNC 或等价注入点，不能只依赖 stress。
30. table locks 在当前代码中是 post-prepare metadata export family，pre-prepare table
    export 只是 preflight；lock warmcopy 不能把它隐式改成 record-lock 式 pre-prepare
    durable contract；
31. canonical shard fingerprint 必须指定 versioned bytes、字节序、bitmap padding 和 hash
    算法，否则无法作为验收门禁；
32. explicit->implicit 反向变化必须进入 dirty/reproof/invalid 体系，v1 不能在 hot hook
    中 inline re-add 后继续声明 clean，否则 conversion 后又 release 的 record 可能从
    explicit 和 implicit 两个集合中同时消失；
33. 函数级 hook map 必须覆盖所有 record/table lock mutation site，不能只列抽象类别；
34. replica/applier target、import-time lock conflict 和每个 reason code 的 deterministic
    测试是实现前必须补齐的测试设计项；
35. MDL ordinal 必须 seal-time 从最终 transaction-duration list 派生，journal-time ordinal
    只能用于诊断，不能作为持久身份；
36. spill fault injection 必须覆盖 short write、fsync/rename failure、manifest torn、
    parent-dir fsync failure 和 orphan cleanup failure。
