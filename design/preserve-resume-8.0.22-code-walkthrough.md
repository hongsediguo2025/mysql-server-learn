# Preserve / Resume 8.0.22 完整代码解读

> 文档类型：当前实现的代码事实手册，作为《Preserve / Resume 事务架构》的代码级扩展
> 基线：2026-07-13 当前工作树；源码事实以工作树为准，旧设计只用于解释动机
> 范围：本地 preserve/drain/startup/resume、InnoDB 内核对象、artifact、跨节点 transfer、receiver prewarm、promotion gate/attach
> 不在范围：物理复制实现、生产 HA 编排器、Proxy peerid 服务本身

## 0. 阅读约定

这份文档不再从“未来想做什么”出发，而是从当前代码回答六个问题：

1. 一个未提交事务在 THD、InnoDB、文件和内存 registry 之间如何转移所有权。
2. preserve 为什么不是 SQL 重放，resume 又如何回到同一个事务语义。
3. 本地单事务、本地批量 drain、跨节点 transfer 三条入口如何复用同一 preserve 内核。
4. MDL、MVCC ReadView、record/table/predicate lock、binlog cache、GTID、savepoint、临时表分别保存什么，何时冻结、持久化、导入和激活。
5. receiver 保存、prewarm、promotion adopt、目标 THD attach 为什么是四个不同阶段。
6. 当前仓库已经实现到哪里，哪些只是测试/接口预埋，哪些必须由外部物理备机与 HA 系统补齐。

本文用以下标签约束结论：

| 标签 | 含义 |
|---|---|
| **已实现** | 当前工作树存在可调用代码路径 |
| **测试/模拟** | 只由 GUnit、MTR、test plugin 或 simulator 驱动，不等同生产 HA |
| **接口预埋** | 数据结构、provider 或 core 已有，但缺生产调用者 |
| **仓库外集成** | 物理复制、Proxy peerid、角色切换等不属于本仓库 |
| **当前约束** | 代码存在，但在线 promotion 仍有必须正视的边界 |

配套架构总览见 [preserve-resume-8.0.22-architecture.md](preserve-resume-8.0.22-architecture.md)。

## 1. 一张图看完整实现

图 1 把本地路径与跨节点路径放在同一张所有权图中。两条路径复用 preserve bundle、XID、InnoDB claim/import 和底层 attach helper，但调度时机完全不同；strict THD attach 仍有自己的 registry/intent 编排。

~~~mermaid
flowchart TD
  thd["源 THD<br/>ACTIVE trx + SQL runtime state"]
  admission["command admission / drain manager<br/>阻断新命令并收口事务"]
  phase1["phase 1 warmcopy / transfer<br/>binlog · locks · temp objects"]
  quiesce["phase 2 quiesce<br/>目标 THD 停在命令边界"]
  kernel["shared preserve kernel<br/>preflight → prepare → detach → publish"]
  engine["InnoDB prepared/preserved trx<br/>undo · rseg · engine state"]
  local["local v9 snapshot + blobs<br/>random local token"]
  transfer["source epoch session<br/>uint64 source connection id"]
  receiver["receiver spool + staging<br/>object ranges · seal · epoch fact"]
  registry["strict prepared registry<br/>prewarm resources + final facts"]
  startup["startup preflight / recover_all<br/>before purge threads"]
  gate["promotion physical gate<br/>fence lease + adopt"]
  record["SQL preserved record<br/>PRESERVED or ADOPTED_FOR_PROMOTION"]
  resume["resume / strict attach helpers<br/>session · MDL · GTID · temp · THD attach"]
  active["target THD<br/>same trx ACTIVE"]

  thd --> admission --> phase1 --> quiesce --> kernel
  kernel --> engine
  kernel --> local
  kernel --> transfer --> receiver --> registry
  local --> startup --> record
  registry --> gate --> record
  engine --> startup
  engine --> gate
  record --> resume --> active
~~~

关键点不是“把一份文件读回来”，而是让四类所有权按顺序闭环：

| 所有权 | preserve 前 | prepare/detach 后 | recover/adopt 后 | resume/attach 后 |
|---|---|---|---|---|
| InnoDB 事务 | 源 THD | detached prepared/preserved trx | SQL record 持有 claimed trx | 目标 THD |
| SQL 语义 | 源 THD 内存对象 | authenticated bundle / external blobs | 已导入对象 + bundle | 目标 THD runtime |
| 客户端入口 | 原连接可执行 | command gate 阻断 | token 可见或 promotion 私有 | 新连接继续执行 |
| 跨机一致性 | 不适用 | transfer staging | epoch fact + physical fence | attach/activate intent |

## 2. 文件级模块地图

### 2.1 SQL 主干与编排

| 文件 | 主要职责 | 关键入口/对象 |
|---|---|---|
| [sql/preserve_trx.cc](../sql/preserve_trx.cc) | preserve/resume 主实现、内存 record、startup recovery、共享 recover/adopt、peer THD 接管 | <code>preserve_trx_kernel_preserve_attached_transaction()</code>、<code>preserved_trx_recover_all()</code>、<code>preserved_trx_recover_or_adopt_bundle_shared()</code>、<code>preserved_trx_resume_record_on_thd()</code> |
| [sql/preserve_trx.h](../sql/preserve_trx.h) | SQL command、manager stage/result、公共 API | <code>Sql_cmd_prepare_shutdown_preserve_transaction</code>、<code>Sql_cmd_drain_transactions_preserve</code>、<code>Sql_cmd_resume_preserved_transaction</code> |
| [sql/preserve_trx_drain.cc](../sql/preserve_trx_drain.cc) | 两阶段 drain participant 编排 | <code>Preserve_trx_drain_orchestrator</code> |
| [sql/preserve_trx_drain.h](../sql/preserve_trx_drain.h) | participant 协议和 observation | <code>open_phase1/close_phase1/phase2_preflight</code> |
| [sql/preserve_trx_bundle.cc](../sql/preserve_trx_bundle.cc) | bundle TLV 编解码、语义校验、外部对象描述 | <code>Preserved_trx_bundle</code>、<code>Preserve_snapshot_metadata</code> |
| [sql/preserve_trx_carrier.cc](../sql/preserve_trx_carrier.cc) | artifact 抽象、工件列举和类型过滤 | local、standby-pending、promotion marker 分类 |
| [sql/preserve_trx_carrier_file.cc](../sql/preserve_trx_carrier_file.cc) | v9 snapshot 与 blob 的文件存储、原子发布、HMAC/CRC | <code>Preserved_trx_store</code> |
| [sql/preserve_trx_resource.cc](../sql/preserve_trx_resource.cc) | 内存/文件/worker resource lease 和状态变量 | resource governor、SHOW STATUS 函数 |
| [sql/preserve_trx_rewrite.cc](../sql/preserve_trx_rewrite.cc) | SQL 重写/审计辅助 | preserve/resume SQL 展示 |
| [sql/preserve_trx_xid.h](../sql/preserve_trx_xid.h) | Preserve magic XID 常量 | token 与 XID 的协议边界 |

### 2.2 语义对象模块

| 文件 | 保存的对象 | 关键职责 |
|---|---|---|
| [sql/binlog_warmcopy.cc](../sql/binlog_warmcopy.cc) | binlog cache prefix/tail | phase 1 镜像、pending range、seal/finalize |
| [sql/binlog_warmcopy.h](../sql/binlog_warmcopy.h) | warmcopy 状态与产物 | prebuilt external blob |
| [sql/binlog_preserve_prepared.h](../sql/binlog_preserve_prepared.h) | promotion 原生 binlog cache handle | prepare、attach journal、commit/abort ownership |
| [sql/preserve_trx_lock_warmcopy.cc](../sql/preserve_trx_lock_warmcopy.cc) | record/table/predicate/MDL 预复制状态 | baseline、dirty tracking、final fence、fallback |
| [sql/preserve_trx_lock_warmcopy.h](../sql/preserve_trx_lock_warmcopy.h) | lock warmcopy 目标状态机 | route decision、artifact、canonical digest |
| [sql/preserve_trx_temp_table.cc](../sql/preserve_trx_temp_table.cc) | 临时表发现、DDL/DML journal、manifest | phase 1 baseline、phase 2 seal、resume materialize |
| [sql/preserve_trx_temp_table_carrier.cc](../sql/preserve_trx_temp_table_carrier.cc) | 临时表 page image / no-redo undo sidecar | 文件发布、读取、清理 |
| [sql/preserve_trx_warmcopy.cc](../sql/preserve_trx_warmcopy.cc) | 通用 warmcopy 辅助 | blob builder、range 和摘要 |

### 2.3 Transfer 与 Promotion

| 文件 | 主要职责 | 不应混淆的边界 |
|---|---|---|
| [sql/preserve_trx_transfer.cc](../sql/preserve_trx_transfer.cc) | source epoch、classic-protocol sender、receiver spool/staging、seal/commit、后台 prewarm | 传输保存不等于 adopt/resume |
| [sql/preserve_trx_transfer.h](../sql/preserve_trx_transfer.h) | frame v3、manifest、ACK、receiver registry | transfer token 是 <code>uint64_t</code> |
| [sql/preserve_trx_promotion.cc](../sql/preserve_trx_promotion.cc) | legacy ready cache、prewarm、promotion gate | 单机/模拟路径与 strict physical gate 要分开 |
| [sql/preserve_trx_promotion.h](../sql/preserve_trx_promotion.h) | gate request/result、ready state、fence provider API | 默认 gate batch/workers 为 3 |
| [sql/preserve_trx_promotion_prepared.cc](../sql/preserve_trx_promotion_prepared.cc) | strict prepared registry、resource lease、intent journal | promotion-ready 的权威内存状态 |
| [sql/preserve_trx_promotion_prepared.h](../sql/preserve_trx_promotion_prepared.h) | strict key/facts/state、fence proof、attach intent | boot incarnation + generation 防 ABA |
| [plugin/test_preserve_trx_promotion](../plugin/test_preserve_trx_promotion) | 测试 promotion provider/入口 | **不是生产 HA 插件** |

### 2.4 InnoDB 内核对象

| 文件 | 对象/职责 | 典型 API |
|---|---|---|
| [storage/innobase/trx/trx0preserve.cc](../storage/innobase/trx/trx0preserve.cc) | trx prepare/preserve/claim/detach/attach/activate、XID 映射 | <code>trx_preserve_claim_prepared()</code>、<code>trx_preserve_attach_to_thd()</code> |
| [storage/innobase/include/trx0preserve.h](../storage/innobase/include/trx0preserve.h) | preserve 内核 API 合同 | ReadView、锁、savepoint、modified table 导入导出 |
| [storage/innobase/lock/lock0preserve.cc](../storage/innobase/lock/lock0preserve.cc) | record/table/predicate lock 序列化与重建 | export/import、page prefetch |
| [storage/innobase/lock/lock0warmcopy.cc](../storage/innobase/lock/lock0warmcopy.cc) | 锁变更 journal 与 warmcopy epoch | native lock mutation 的薄 hook |
| [storage/innobase/include/lock0preserve_plan.h](../storage/innobase/include/lock0preserve_plan.h) | promotion record-lock 预建计划 | plan validation/apply journal |
| [storage/innobase/trx/trx0temp_preserve.cc](../storage/innobase/trx/trx0temp_preserve.cc) | temp space、dict、no-redo undo 的保留/领养 | bootstrap、materialize、reseed |
| [storage/innobase/include/trx0trx.h](../storage/innobase/include/trx0trx.h) | <code>TRX_STATE_PRESERVED</code>、claim 标记 | 事务状态本体 |
| [storage/innobase/include/read0read.h](../storage/innobase/include/read0read.h) | ReadView 导出/导入接口 | MVCC list 重建 |

### 2.5 原生 MySQL 路径上的薄集成点

| 文件/区域 | Preserve hook | OFF-path 要求 |
|---|---|---|
| [sql/sql_parse.cc](../sql/sql_parse.cc) | 命令读边界、inflight guard、transfer COM dispatch、响应交付确认 | feature OFF 时保持普通命令调度 |
| [sql/mysqld.cc](../sql/mysqld.cc) | startup preflight、status var、服务生命周期 | 仅在 feature/policy 允许时运行 |
| [storage/innobase/handler/ha_innodb.cc](../storage/innobase/handler/ha_innodb.cc) | DDL recovery 后、purge 线程前调用 recover | read-only standby 明确 defer |
| binlog cache/ostream 路径 | warmcopy mirror、export/import、ownership transfer | 没有 active epoch 时只做极薄分支 |
| MDL manager | strict backup/restore policy | 普通 MDL 语义不改变 |
| lock mutation 路径 | warmcopy bookkeeping | hook 不得让原生加锁/解锁失败 |
| temp/FSP/undo 路径 | page image、space/rseg/undo identity 保留 | 仅 temp preserve candidate 生效 |

### 2.6 关键接口头文件

这些头文件不是重复声明；它们定义了模块间 ownership 合同：

| 文件 | 合同 |
|---|---|
| [sql/preserve_trx_kernel.h](../sql/preserve_trx_kernel.h) | shared preserve kernel request/result；注入 target THD、delivery mode、prebuilt binlog/lock artifact、transfer epoch session、preselected token 和 provenance policy |
| [sql/preserve_trx_bundle.h](../sql/preserve_trx_bundle.h) | metadata、TLV、external blob、codec context 和 v9 固定字段 |
| [sql/preserve_trx_carrier.h](../sql/preserve_trx_carrier.h) | snapshot/blob/marker 的抽象 carrier、payload read mode、artifact listing |
| [sql/preserve_trx_carrier_file.h](../sql/preserve_trx_carrier_file.h) | filesystem carrier 和原子文件操作 |
| [sql/preserve_trx_resource.h](../sql/preserve_trx_resource.h) | memory/file/lock/native-binlog resource lease |
| [sql/preserve_trx_warmcopy.h](../sql/preserve_trx_warmcopy.h) | 通用 warm external blob/provider 合同 |
| [sql/preserve_trx_temp_table.h](../sql/preserve_trx_temp_table.h) | temp participant、manifest、journal、resume API |
| [sql/preserve_trx_temp_table_carrier.h](../sql/preserve_trx_temp_table_carrier.h) | temp page/undo sidecar carrier |
| [storage/innobase/include/lock0warmcopy.h](../storage/innobase/include/lock0warmcopy.h) | InnoDB lock warmcopy epoch 与 native hook 合同 |
| [storage/innobase/include/trx0temp_preserve.h](../storage/innobase/include/trx0temp_preserve.h) | temp space/dict/no-redo undo 的 bootstrap/adopt/cleanup 合同 |

### 2.7 原生集成文件按对象归类

下面列的是 Preserve 语义真正触达的原生 MySQL/InnoDB 区域；这些文件应优先做非侵入与 OFF-path review：

| 对象 | 文件 |
|---|---|
| parser/command | <code>sql/sql_yacc.yy</code>、<code>sql/sql_lex.*</code>、<code>sql/sql_parse.cc</code>、<code>sql/sql_prepare.cc</code> |
| THD lifecycle | <code>sql/sql_class.*</code>、<code>sql/sql_thd_internal_api.cc</code>、<code>sql/conn_handler/init_net_server_extension.cc</code> |
| privilege/sysvar/startup | <code>sql/auth/dynamic_privileges_impl.cc</code>、<code>sql/sys_vars.cc</code>、<code>sql/system_variables.h</code>、<code>sql/mysqld.cc</code> |
| binlog/GTID | <code>sql/binlog.*</code>、<code>sql/binlog_ostream.*</code>、<code>sql/transaction.cc</code> |
| MDL/schema/temp DDL | <code>sql/mdl.*</code>、<code>sql/mdl_context_backup.*</code>、<code>sql/sql_base.cc</code>、<code>sql/sql_table.cc</code>、<code>sql/sql_truncate.*</code>、<code>sql/handler.cc</code> |
| XA/protection | <code>sql/xa.cc</code>、<code>storage/innobase/clone/clone0repl.cc</code> |
| InnoDB lock/read | <code>lock0lock.*</code>、<code>lock0priv.*</code>、<code>read0read.*</code>、<code>btr0cur.cc</code> |
| trx/undo/rseg/purge | <code>trx0trx.*</code>、<code>trx0undo.*</code>、<code>trx0rseg.cc</code>、<code>trx0sys.*</code>、<code>trx0roll.cc</code>、<code>trx0purge.cc</code>、<code>row0undo.*</code>、<code>row0uins.cc</code>、<code>row0umod.cc</code> |
| temp tablespace/page | <code>fil0fil.*</code>、<code>fsp0fsp.*</code>、<code>srv0tmp.*</code>、<code>srv0start.cc</code>、<code>mtr0mtr.cc</code> |
| build integration | <code>sql/CMakeLists.txt</code>、<code>storage/innobase/CMakeLists.txt</code>、test plugin CMake |

这些文件中有的是几行 gate/include，有的是内核状态扩展。文件出现在表里不代表允许继续堆业务逻辑；共享热路径应保持薄，复杂调度必须留在独立 <code>preserve_trx*</code> 模块。

### 2.8 测试与运行脚本

| 目录/文件族 | 作用 |
|---|---|
| [mysql-test/suite/preserve_trx](../mysql-test/suite/preserve_trx) | SQL、restart、failure injection、OFF-path、transfer/promotion MTR |
| [unittest/gunit](../unittest/gunit) 中 <code>preserve_trx*-t.cc</code> | bundle/carrier/resource/transfer/promotion/temp 单元测试 |
| <code>unittest/gunit/innodb/lock0warmcopy-t.cc</code> | lock warmcopy native bookkeeping/plan |
| [plugin/test_preserve_trx_promotion](../plugin/test_preserve_trx_promotion) | 测试 fence/provider/attach 入口 |
| <code>scripts/resumable_trx_longrun_e2e.py</code>、<code>scripts/resumable_trx_business_e2e.py</code>、<code>scripts/resumable_trx_nfr2_benchmark.py</code> | 真 mysqld、故障注入、长跑与性能证据 |

## 3. 不是一个状态机，而是六个正交状态机

把所有状态压成一个枚举会丢失所有权信息。当前实现至少有六组状态，每组回答不同问题。

### 3.1 全局 manager 状态

<code>Preserve_trx_manager_state</code> 管一次 drain/shutdown 的全局命令窗口：

~~~text
IDLE
  → SOFT_DRAINING / HARD_DRAINING
  → WARMCOPY_DRAINING
  → WARMCOPY_CLOSING
  → BATCH_DRAINING
  → SHUTDOWN_REQUESTED

任意阶段失败：
  → IDLE（已完整恢复）
  → DRAIN_CLEANUP_FAILED（所有权/清理无法证明）
~~~

<code>SNAPSHOTTING</code> 和 <code>HARD_DRAINING</code> 保留兼容语义；实际批量路径主要使用 warmcopy/batch 状态。manager 同时保存 owner thread id，避免两个 drain owner 并发推进。

### 3.2 每个源 THD 的 batch 状态

<code>Preserve_trx_batch_thd_state</code> 位于 THD，负责让 command boundary 与 drain owner 协作：

~~~text
NONE
  → PENDING_QUIESCE
  → QUIESCED
  → ATTACHING

无事务：DRAINED_NO_TRANSACTION
成功 preserve：PRESERVED_DRAINED
~~~

<code>PENDING_QUIESCE</code> 只表示“本条命令结束后不要再读下一条”；<code>QUIESCED</code> 才表示 drain 可以安全读取和冻结这个 THD 的事务对象。promotion 目标 THD 也复用 <code>ATTACHING</code> 作为受保护接管状态。

### 3.3 preserve kernel stage

<code>Preserve_trx_preserve_stage</code> 是单 token 的执行进度：

~~~text
VALIDATION
→ BINLOG_PREFLIGHT
→ LOCK_PREFLIGHT
→ UNDO_PREPARE
→ DETACH
→ SNAPSHOT_WRITE
→ RECORD_REGISTER
→ TOKEN_DELIVERY
→ COMPLETE
~~~

它不是持久化状态机，而是错误处理的切分点。<code>Preserve_trx_preserve_result</code> 记录当前 stage、耗时、是否越过 durable point、是否 detach/reattach、snapshot identity 和 source rollback image。这里的 <code>durable_point_crossed</code> 精确定义为“已经越过引擎 prepare 或 temp-only prepare 边界”，并不表示 snapshot 已经发布。

### 3.4 SQL preserved record 状态

<code>g_preserved_trx_records</code> 中每个 token 的 lifecycle：

~~~mermaid
stateDiagram-v2
  [*] --> SNAPSHOTTING
  SNAPSHOTTING --> PRESERVED: OK packet delivered
  SNAPSHOTTING --> ROLLING_BACK: delivery failed
  DRAINING --> PRESERVED: startup handoff
  PRESERVED --> RESUMING: SQL claim
  ADOPTED_FOR_PROMOTION --> RESUMING: internal promotion claim
  PRESERVED --> EXPIRED_ROLLBACK: timeout/reaper
  EXPIRED_ROLLBACK --> EXPIRED_CLEANUP_FAILED: cleanup failed
  RESUMING --> [*]: attach + activate
  ROLLING_BACK --> [*]: rollback + artifact cleanup
~~~

<code>resumable</code> 是独立 claim gate，不等同 lifecycle：

- 单事务 preserve 刚注册时为 <code>SNAPSHOTTING + resumable=false</code>。
- 只有带 token 的 OK packet 已确认发出，finalizer 才改成 <code>PRESERVED + resumable=true</code>。
- promotion adopt 注册为 <code>ADOPTED_FOR_PROMOTION + resumable=false</code>，普通 SQL <code>RESUME</code> 不能抢占。
- receiver 保存的 token **不会**进入这个 map；它有独立 registry。

### 3.5 InnoDB trx 状态

事务本体的状态变化是：

~~~text
ACTIVE
  -- ha_prepare_low / temp-only prepare -->
PREPARED
  -- claim + mark preserved -->
PRESERVED (claimed, detached)
  -- attach + activate -->
ACTIVE
  -- client decision -->
COMMITTED / ROLLED_BACK
~~~

<code>TRX_STATE_PRESERVED</code> 与 <code>preserve_trx_claimed</code> 是引擎级所有权事实。SQL record 没有凭空创建事务；它必须能以 Preserve magic XID 找到并 claim 一个 prepared trx，或按 temp manifest 创建受控的 temp-only claimed trx。

### 3.6 Receiver 与 strict promotion 状态

Receiver registry 只描述“对象是否接收并保存”：

~~~text
DECLARED → RECEIVING → SAVED_ONLINE
                  ├→ CLEANUP_PENDING → CLEANUP_TAINTED
                  ├→ CORRUPT
                  └→ ABORTED
~~~

Strict prepared registry 描述“是否具备 promotion 条件”：

~~~mermaid
stateDiagram-v2
  [*] --> OBJECTS_RECEIVING
  OBJECTS_RECEIVING --> PREWARMING
  PREWARMING --> PREWARMED_PENDING_FINAL_FACT
  PREWARMED_PENDING_FINAL_FACT --> READY_FACTS_PENDING_LEASE
  READY_FACTS_PENDING_LEASE --> READY_FOR_GATE
  READY_FOR_GATE --> ADOPTING
  ADOPTING --> ADOPTED_LOCKED
  ADOPTED_LOCKED --> ATTACHING
  ATTACHING --> ACTIVATING
  ACTIVATING --> ACTIVE
  ACTIVE --> ACTIVE_ARTIFACTS_CLEANED
  ADOPTING --> ABANDONED_ROLLED_BACK
  ADOPTING --> ABANDONED_NOT_FOUND_PROVEN
  ADOPTING --> CLEANUP_TAINTED
  ATTACHING --> ATTACH_ROLLED_BACK
  ATTACHING --> ATTACH_TAINTED
~~~

此外还有 <code>NOT_READY</code>、<code>CORRUPT</code>、<code>RESOURCE_EXHAUSTED</code>、<code>STALE_GENERATION</code> 等 fail-closed 终态。registry key 组合 preserve dir、source UUID、epoch、token、target boot incarnation 和 generation，用来防止旧进程/旧 epoch 的缓存被新 promotion 误用。

### 3.7 内存结构与 durable 结构对照

| 层 | 进程内结构 | durable source | 重启/丢缓存后的处理 |
|---|---|---|---|
| 源端 drain | manager owner、THD batch state、participant artifacts、per-token result | InnoDB prepare、provenance intent、本地 snapshot 或已发送 epoch | drain 中途按 source rollback/participant cleanup 和解 |
| 本地 resumable token | <code>g_preserved_trx_records</code> | magic-XID prepared trx + v9 snapshot/blobs + provenance | startup <code>recover_all()</code> 重建 record |
| transfer receiver | receiver registry、range map、worker queues | spool、<code>.transfer/&lt;epoch&gt;</code> object、epoch fact/commit | replay helper 可重建；普通 startup 当前没有自动调用者 |
| prewarm | legacy ready cache、strict registry resources | sealed objects + committed epoch fact + standby projection | cache 可丢弃并重新解析/prewarm |
| promotion adopted | promotion-owned SQL record、claim/attach leases | target prepared trx + promotion/attach intents + standby artifact | 按 intent/fence/HA policy 和解；不得凭缓存猜测 |
| ACTIVE | Ty 上的 THD/session/binlog/MDL/<code>trx_t</code> | InnoDB undo/redo/binlog 原生事务事实 | 回归普通事务 crash recovery |

内存 cache 的作用是把 gate 延迟降到毫秒级，不是取代 durable artifact。任何 READY 判断都必须能追溯到已提交 epoch fact、对象 digest、boot/generation 和有效 resource lease。

## 4. 身份链：token、XID、provenance 和 peer connection

### 4.1 本地 token 与 transfer token

当前代码故意保留两套取值策略：

| 场景 | token 值 | 原因 |
|---|---|---|
| 本地单事务/本地 batch | 随机 16 字节，显示为 32 位十六进制 | 延续存量行为与碰撞模型 |
| standby transfer batch | 源端目标 THD 的 <code>thread_id()</code>，作为 <code>uint64_t</code>，bundle/XID 中转成十进制文本 | 未来用 Proxy peerid 的 Tx 定位目标 Ty |

transfer manifest 不再重复保存 <code>source_connection_id</code>；<code>token</code> 本身就是 Tx。这样不改变存量本地 token，也避免两个字段不一致。

Standby 远端 sink 只允许 batch-manager delivery 选择；面向单连接客户端交付 token 的 single preserve 仍走 local carrier，不能因全局 artifact mode 而把 <code>CLIENT_TOKEN_DELIVERY</code> 偷换成远端传输。

MySQL 的 <code>Global_THD_manager::get_new_thread_id()</code> 在锁下递增计数并跳过仍在使用的 id；同一进程正常寿命内不会把一个仍存活连接的 id 分给另一个 THD。它仍不是永久全局 ID：连接关闭、极端计数回绕或 mysqld 重启后都不能脱离 source UUID/incarnation/epoch 单独使用。源连接在 transfer 全批完成前断开会使 peerid 合同失效，drain 应 abort/restore，而不是把 Tx 转交给别的连接。

### 4.2 token 如何变成 InnoDB XID

<code>trx_preserve_token_to_xid()</code> 构造固定格式：

- <code>formatID = PRESERVE_TRX_XID_FORMAT_ID</code>
- GTRID 是 Preserve 固定 magic
- BQUAL 是 token 文本

反向映射只接受该严格格式。仅长得像 magic XID 不足以得到保护；SQL 层的 XID provenance ledger 还要证明该 XID 对应真实 snapshot/intent，避免普通 XA 或伪造 XID 被错误保留。

### 4.3 peerid 的当前边界

目标 attach 需要的是 Ty，而 transfer token 记录的是 Tx。当前仓库提供 <code>Preserved_trx_peer_thd_handle</code> 与 resolver 接口，句柄会：

- 通过 THD 查找纪律固定目标对象生命周期；
- 在 <code>LOCK_thd_data</code> 保护下检查目标连接 idle、pristine、batch state 为 <code>NONE</code>；
- 把目标状态置为 <code>ATTACHING</code>；
- attach 期间延迟 KILL/teardown；
- RAII release 时恢复状态并执行 deferred kill。

默认 resolver 只能按本机 connection id 查 THD。生产 Tx→Ty 映射必须由 **仓库外 Proxy/HA peerid 服务**提供，不能把当前默认 resolver 描述成已完成跨节点映射。

### 4.4 UUID、incarnation 与 generation 不是重复 token

token 回答“这是哪一个事务”，节点与代际字段回答“这份事实属于哪一次传输/哪一个目标进程”：

| 字段 | 作用 |
|---|---|
| source server UUID | 稳定标识源节点，防止不同源的同值 connection id 混淆 |
| target server UUID | receiver 拒绝发往错误目标节点的 manifest |
| source incarnation id | 限定 frame sequence/ACK replay domain；源进程重新启动后旧序列不能冒充新 epoch |
| target boot incarnation | 目标重启后使旧 prewarm cache/lease 失效 |
| epoch id | 一批 token 的 publication identity |
| generation | strict registry 的 ABA 防护；当前通常绑定非零 <code>source_epoch_commit_lsn</code> |

这些字段不能替代 token，也不应再复制一个 <code>source_connection_id</code>。它们共同解决跨节点、跨进程、跨 epoch 的身份重用问题。

## 5. SQL 入口与命令边界

### 5.1 语法和命令对象

语法入口位于 [sql/sql_yacc.yy](../sql/sql_yacc.yy)：

- <code>DRAIN TRANSACTIONS PRESERVE ...</code>
- <code>RESUME PRESERVED TRANSACTION &lt;token&gt;</code>

命令对象在 <code>preserve_trx.h</code>，执行实现落在 <code>preserve_trx.cc</code>。Transfer 不走普通 SQL 文本，而是 classic protocol 的 <code>COM_PRESERVE_TRX_TRANSFER</code>。

实际调用链：

~~~text
single:
Sql_cmd_prepare_shutdown_preserve_transaction::execute
  → preserve_trx_execute_prepare_shutdown_preserve
  → preserve_trx_preserve_attached_transaction
  → preserve_trx_kernel_preserve_attached_transaction

batch:
Sql_cmd_drain_transactions_preserve::execute
  → Preserve_trx_drain_service::execute
  → participant phase1/quiesce/phase2
  → parallel preserve_trx_kernel_preserve_attached_transaction

local resume:
Sql_cmd_resume_preserved_transaction::execute
  → preserved_trx_resume_record_on_thd

strict promotion:
preserved_trx_adopt_prepared_epoch_for_physical_promotion
  → preserved_trx_recover_or_adopt_bundle_shared
  → ADOPTED_FOR_PROMOTION record
  → preserved_trx_resume_adopted_for_promotion_on_thd
~~~

### 5.2 为什么 drain 必须接入 command read 边界

只检查“当前没有语句执行”不够，因为连接可能刚准备读取下一条命令。<code>sql_parse.cc::do_command()</code> 在读 packet 前后调用：

1. <code>preserved_trx_begin_command_read()</code>
2. 标记 command inflight
3. <code>preserved_trx_end_command_read()</code>
4. 若连接已被 drain，拒绝进入下一条命令

<code>mysql_execute_command()</code> 再用 inflight guard 和 command-block result 封住执行窗口。这样 <code>PENDING_QUIESCE</code> 会在完整语句响应边界收敛成 <code>QUIESCED</code>，而不是在 SQL 执行中途抓取半变化对象。

### 5.3 token 交付也是事务边界

单事务 preserve 的 durable snapshot 可能已经写好，但客户端未必收到 token。代码用 <code>Pending_token_delivery</code> 记录：

- statement response 是否被观察；
- OK 是否成功交付；
- 是否正在 finalizing。

<code>send_statement_status()</code> 后的 <code>preserved_trx_finalize_statement_response()</code> 才决定：

- OK 已交付：record 变为 resumable，审计完成并触发 shutdown；
- 未交付：隐藏 token 对应的事务回滚并清理，manager 回到 IDLE。

因此系统不会留下“服务端认为可 resume、客户端却永远不知道 token”的孤儿事务。

## 6. 单事务 preserve：共享内核的最小入口

单事务路径先做权限、上下文、容量、binlog 模式与事务资格检查，再调用 <code>preserve_trx_kernel_preserve_attached_transaction()</code>。该内核同时被 batch drain 使用，差别由 options、预建 warmcopy 对象和 artifact sink 注入。

### 6.1 VALIDATION

这一阶段确认：

- feature、server mode、事务状态和 THD 上下文允许 preserve；
- token/owner/timeout/resource policy 合法；
- 没有不支持的语句状态或引擎组合；
- sink 是本地 store 还是 transfer source epoch；
- 本次失败是否仍可把原事务留在源 THD。

### 6.2 BINLOG_PREFLIGHT

导出 <code>Mysql_binlog_preserve_snapshot</code>。它不只是 cache bytes，还包括：

- GLOBAL/SESSION binlog 是否关闭；
- LOGGED_EMPTY 或 LOGGED_WITH_CACHE；
- <code>gtid_next</code>、owned GTID、event counter；
- statement/row/XID/content 标记；
- cache start/end/previous position/length；
- compression state/type/level。

单事务可直接导出 live cache；batch 可以消费 phase 1 生成的 prebuilt warmcopy blob。大 payload 通过 external blob descriptor 外置，bundle 内只保留语义描述与摘要。

### 6.3 LOCK_PREFLIGHT

这一阶段同时冻结多个对象：

1. 导出 MVCC ReadView。
2. 必要时把隐式 record lock 物化，避免仅靠聚簇记录状态无法跨进程重建。
3. 导出 transaction-duration MDL descriptor，并重新验证对象权限。
4. 导出 modified table name 和 write mask。
5. 导出 SQL/InnoDB savepoint 与 binlog participant topology。
6. 导出 record/table/predicate lock，或消费 lock warmcopy 产物。
7. 做 canonical digest 和 final fence 校验。

lock warmcopy 使用 all-family 规则：任何必需 family 的预复制不完整，就整组回退 live export 或拒绝，不能把 warmcopy record locks 与另一时刻的 table/predicate/MDL 拼成假一致快照。

### 6.4 UNDO_PREPARE

持久事务调用 <code>ha_prepare_low()</code>，使用 token 对应的 Preserve magic XID。temp-only 事务没有普通 persistent prepared trx，走 <code>trx_preserve_prepare_current_temp_only()</code>，并把 owner trx id 和 temp manifest 放进 bundle，供 recover/adopt 时创建受控 claimed trx。

### 6.5 DETACH：真正的所有权切换

内核先按 token 创建 detached MDL backup，然后调用 <code>trx_preserve_detach_current_thd()</code>：

- 从 mysql trx list 和 THD→trx 映射移除；
- 清空 <code>mysql_thd</code> 与 handler/session scope；
- 保留 prepared trx、undo、锁和 XID；
- 再以 XID claim 为 Preserve 所有。

从 detach 起，原 THD 不再拥有事务；但 <code>durable_point_crossed</code> 在前一阶段 prepare 成功时已经置位。prepare/detach 状态与 source rollback image 共同决定后续失败是“原位返回”、显式 reattach，还是只能 rollback/taint。

### 6.6 SNAPSHOT_WRITE

这一阶段：

- finalize prebuilt binlog cache；
- 构造 temp-table manifest；
- 判定 engine shape：<code>PERSISTENT_ONLY</code>、<code>TEMP_ONLY</code> 或 <code>MIXED</code>；
- 获取 codec memory/resource lease；
- 构造 bundle 和 external blob descriptors；
- 调用 local sink 或 transfer sink；
- artifact 发布后绑定 XID provenance 的 snapshot identity；
- 最后复核 lock warmcopy frozen fence。

### 6.7 RECORD_REGISTER 与 TOKEN_DELIVERY

本地运行时注册 <code>Preserved_trx_record</code>：

- batch record 初始可为 <code>DRAINING</code>，由 manager 持有全局命令门；
- single record 初始是 <code>SNAPSHOTTING + resumable=false</code>；
- promotion 由另一条 shared recovery 路径注册为 <code>ADOPTED_FOR_PROMOTION</code>。

单事务随后进入 TOKEN_DELIVERY；batch 不依赖逐 token SQL OK，而是由 drain 的全批成功与 shutdown ownership 决定。

## 7. 本地批量 drain 的两阶段流程

<code>Preserve_trx_drain_service::execute()</code> 是本地批量编排主线。它不是“遍历 THD 然后逐个调用 preserve”，而是为了减少最终停顿，把可复制工作移到 phase 1。

### 7.1 participant 协议

<code>Preserve_trx_drain_orchestrator</code> 统一管理 temp、binlog warmcopy、lock warmcopy participant：

~~~text
open_phase1
  → 业务仍可推进，participant 建 baseline / mirror / journal
close_phase1
  → command block 已发布，不再允许新业务变化
ensure_phase1_ready
  → 检查基础产物可用
phase2_preflight
  → 对最终边界做 seal、coverage、digest 和预算检查
finalize / finalize_for_shutdown

失败：
abort → 恢复 participant
cleanup_failed_shutdown → 无法恢复时进入 fail-closed
~~~

orchestrator 在工作结束后复制 observation，避免观测线程看到 participant 正在变化的内部状态。

### 7.2 phase 1：边运行边准备

满足 transfer streaming、temp phase1 或 warmcopy 条件时，manager 进入 <code>WARMCOPY_DRAINING</code>：

1. 创建 source epoch session；epoch id 绑定 source incarnation、generation 和单调时间。
2. 预声明候选 token。transfer 路径直接使用候选 THD 的 source thread id。
3. 扫描 active/idle lock warmcopy targets。
4. binlog mirror 复制稳定 prefix，后续 append/truncate 通过薄 hook 记录 tail/range。
5. lock warmcopy 建 baseline、dirty shard 和 journal。
6. temp participant 复制 page baseline、记录 DDL/DML/savepoint journal；该能力当前用于本地 artifact，跨节点 portable builder 对非空 temp manifest 仍 fail closed。
7. transfer phase 1 可直接流式发送 prebuilt record-lock/blob 对象。

这时事务仍在原 THD 上执行，所以 phase 1 产物不能单独成为最终 snapshot；它必须在 close/final fence 后证明覆盖了最终状态。

### 7.3 收口 command boundary

进入 <code>WARMCOPY_CLOSING</code> 前做一次 nonblocking rescan，把刚出现的候选纳入。随后：

1. 发布 command block；
2. 枚举所有 THD 并 pin 生命周期；
3. 将目标置为 <code>PENDING_QUIESCE</code>；
4. 等待命令结束，目标转为 <code>QUIESCED</code> 或 <code>DRAINED_NO_TRANSACTION</code>；
5. 按 deadline 失败关闭；
6. 重新计数并检查 batch/per-user/global capacity，防止 phase 1 观察集与最终目标集不同。

### 7.4 phase 2：最终 seal 与 catch-up

所有目标静止后：

- participant close；
- 校验 phase 1 是否 ready；
- 在 tail budget 内补齐 binlog ranges；
- 对 lock artifact 做 all-family coverage、canonical digest 和 generation fence；
- seal temp journal，并验证 page/undo ownership completeness；
- transfer 补发 phase 1 未覆盖或摘要已变化的对象；
- manager 进入 <code>BATCH_DRAINING</code>。

所有 sleep/yield 应发生在不持关键 THD/MDL/InnoDB 锁的位置；phase 2 主要受 drain hard deadline，而不是无限等待。

### 7.5 并行 preserve

最终目标会先全部 pin，然后由 preserve worker 并行执行共享 kernel。<code>preserve_trx_parallel_preserve_threads=0</code> 是 auto：当前实现按硬件并发映射到至少 4、最多 10，再受 token 数量限制。

每个 worker 使用 <code>Preserve_batch_quiesced_idle_target</code> 保证目标仍是 quiesced、idle、未被 teardown。worker 可以并行构造 bundle 和本地/transfer artifact，但整个 batch 的最终 publication 仍有统一边界。

### 7.6 token、provenance 和 epoch commit

本地 batch 生成随机 token；transfer batch 使用 THD thread id 的十进制 token。两者都做 runtime record、XID provenance 和文件碰撞检查。

批量路径在真正 prepare 前写 PREPARED_INTENT；全部 token 成功后将 provenance 作为一组绑定，必要时把目录 fsync 延后到 batch barrier 统一完成。

transfer 的 <code>COMMIT_EPOCH</code> 只有在以下条件满足后才发送：

- 每个源事务都已 prepare/detach；
- 每个 token 的最终 bundle/object 已写入 source epoch；
- 本地 provenance/durability 操作成功；
- participant final fence 未失效。

任一 token 或 epoch commit 失败，都 abort transfer epoch，并利用每个 item 的 source rollback image 把已 detach 的事务恢复到原 THD；若无法证明恢复完整，则进入 cleanup-failed，而不是继续 shutdown。

### 7.7 shutdown ownership

全批成功后记录审计、finalize participants、manager 进入 <code>SHUTDOWN_REQUESTED</code> 并触发 shutdown。到这里本地路径的下一任 owner 是下次 startup recovery；跨节点路径的下一任 owner则由备端 receiver/prewarm/promotion 管理。

## 8. 内核对象的完整生命周期

### 8.1 InnoDB trx、undo 与 rollback segment

| 阶段 | 内存/存储形态 | 关键保护 |
|---|---|---|
| preserve 前 | <code>ACTIVE trx_t</code>，属于源 THD | 原生事务生命周期 |
| prepare 后 | <code>PREPARED</code>，undo 持久，magic XID | prepared trx 不参与普通 rollback |
| claim 后 | <code>PRESERVED + preserve_trx_claimed</code> | 从 prepared 计数迁移，受 preserve policy 保护 |
| restart | recovery 重建 prepared trx，preflight/provenance 识别 Preserve XID | orphan rollback 前完成筛选 |
| resume | attach 后调用 <code>trx_preserve_activate_resumed()</code> | redo/no-redo undo 恢复 ACTIVE |
| 最终结束 | 普通 COMMIT/ROLLBACK | 回归原生事务路径 |

Preserve 还保护相关 rollback segment：recovery/purge 不能在 snapshot 尚可能恢复时把它们当作普通 orphan 清走。XID provenance 是区分“真实 preserve XID”和“碰巧相同格式”的第二道门。

### 8.2 MVCC ReadView

ReadView payload 保存：

- <code>low_limit_id</code>
- <code>up_limit_id</code>
- <code>creator_trx_id</code>
- <code>low_limit_no</code>
- active trx id 有序集合

导出由 <code>trx_preserve_export_read_view()</code> 调用 MVCC preserve API；shutdown 前会关闭 live ReadView 指针，因为跨进程可保存的是值语义而不是指针。

导入会校验 creator、上下界、数量和 trx id 顺序，再将新 ReadView 插入 MVCC list。当前 InnoDB API 明确要求 purge 状态是 INIT 或 DISABLED，因此本地 startup 把 recover 放在 purge/background threads 启动前。

> **当前在线 promotion 约束**
> strict promotion 复用同一 shared import 内核。对包含非空 ReadView 的事务，生产在线 gate 必须提供明确的 purge/visibility 隔离合同，或引入适配在线 adopt 的安全导入机制。仅有 physical apply fence 不能自动证明 MVCC list 并发导入安全；当前代码不能据此宣称任意 ReadView 都已具备无停顿在线 promotion 条件。

### 8.3 MDL

preserve 导出 transaction-duration MDL ticket 的描述符与顺序。live detach 时：

1. 从源 THD 克隆到 token 对应的 <code>MDL_context_backup_manager</code>；
2. detach 后备份 context 独立持有 MDL；
3. resume 时再克隆到目标 THD；
4. activate/cleanup 后删除 backup。

startup/adopt 没有 live THD 可克隆，会从 metadata 解析 <code>MDL_request</code>，按 oldest→newest acquire，再放入 detached backup。反向顺序处理用于抵消 clone 内部顺序翻转，保持原 ticket 顺序。

当前 shared recover/adopt 使用 <code>LONG_TIMEOUT</code> 获取 detached MDL。这对 startup 合理，但与“promotion gate 增量小于 1 秒”存在直接张力：prewarm 期间又不能提前持有业务 MDL，因此生产 gate 必须采用 deadline-aware acquisition、冲突预检或 fail-fast policy，不能把长等待隐藏在 1 秒指标外。

### 8.4 Record/Table/Predicate locks

record lock metadata 包括页/记录标识、mode、bitmap/image 等；恢复前可按 page id 预取或构建 <code>lock_preserve_metadata_plan_t</code>。table lock、AUTO_INC、predicate lock 也有独立语义。

promotion strict path的目标不是在 gate 内重新解析大 blob，而是：

- receiver prewarm 读取 object；
- 构造 lock plan 与 proof；
- registry 持有 resource lease；
- gate 在 physical fence 下校验 generation/page/dictionary digest；
- shared adopt 应用 prebuilt plan。

锁 warmcopy 状态大致为：

~~~text
NEW → JOURNAL_OPEN → BASE_SCAN_RUNNING
  → OPEN_VALID / OPEN_DIRTY / OPEN_UNSUPPORTED
  → SEALING
  → SEALED_VALID / SEALED_INVALID / SEALED_UNSUPPORTED
  → CONSUMED / ABORTED
~~~

其中部分状态是协议词汇，当前实现不一定经过每个状态。路由结果只有 USE_WARM_COPY、FALLBACK_TO_LIVE_EXPORT、REJECT；不能局部拼接不同时刻的 lock family。

### 8.5 Binlog cache、GTID 和 savepoint topology

binlog cache 的语义不等于一段裸 bytes。snapshot 还要保存 cache state、事件边界、compression、GTID ownership 以及 savepoint 对 binlog participant 的位置。

本地 resume：

1. metadata-only 复核后按需 hydrate cache blob；
2. 导入 THD binlog cache；
3. 恢复 GTID ownership；
4. 准备失败回滚所需的 GTID undo；
5. 恢复 SQL/InnoDB savepoint 与 binlog checkpoint/topology。

promotion strict prewarm 使用 <code>Mysql_binlog_preserve_prepared_cache_handle</code>：

- receiver 从 file-backed payload reader 构造 native handle；
- registry 通过 resource lease 持有它；
- attach 开始后 ownership 可 abort 回 registry；
- attach commit 后 ownership 转给 Ty；
- gate 内不应整文件 hydrate/copy。

binlog warmcopy 的 finalize 条件包括当前 cache 长度、tail budget、destination HWM、digest、durable HWM 和 pending range 全部闭合。append/truncate hook 只在 active warmcopy epoch 下镜像，不改变普通 binlog cache 写语义。

### 8.6 临时表

临时表不是普通表元数据，也不能靠 SQL DDL/DML 重放恢复。其 durable source of truth 是：

- 临时表 page baseline/image；
- dirty page overlay；
- no-redo rollback segment / undo sidecar；
- serialized DD table、dict binding 和 index roots；
- ownership claim 与摘要。

DDL/DML journal 只证明 baseline 之后的变化已被捕获，并记录 CREATE/DROP/TRUNCATE/ALTER/RENAME、行变更标记和 savepoint marker；journal 本身不重放 SQL 行。

<code>Preserved_temp_table_manifest</code> 包含 owner trx id、native adoption 能力、table image、undo image 和 ownership claims。startup 的 <code>preserved_temp_images_bootstrap_preamble()</code> 必须在新 temp space/rseg 分配前预留原 identity，resume 再 materialize/adopt fil_space、dict 和 no-redo undo，最后 reseed runtime baseline。

shared proof page 可以复用，但摘要必须一致；private undo page/slot 必须独占。遇到不支持的 DDL、savepoint rollback、identity 冲突或 capture 不完整时 fail closed。

本节描述的 temp image/undo 保存与 materialize 在本地 preserve/startup/resume 已实现。Transfer 协议虽预留 <code>TEMP_TABLE_SIDECAR</code> object kind，但当前 source portable object builder 遇到非空 <code>temp_table_manifest_payload</code> 会返回 <code>UNSUPPORTED</code>；因此不能宣称 temp-table transaction 已支持跨节点 promotion。

### 8.7 Session、user vars 和访问合同

bundle 同时保存 isolation、read/write policy、SQL mode/options、schema、insert id、modified table/write mask 和 privilege descriptors。用户变量只有显式 <code>WITH USER VARS</code>/<code>INCLUDE</code> 才进入 payload；DEFAULT/EXCLUDE 不写 payload，resume 也不会顺手清空目标 THD 已有 user vars。

resume 不是简单复制 THD 内存；它先重新验证当前用户是否有 token owner/RESUME_ANY 和对象权限，再按序恢复 session fields，避免旧权限环境直接越权。

promotion internal policy 可以跳过 SQL privilege check，但该 policy 只能由内部 strict attach 构造，不能暴露给用户可达入口。

## 9. Bundle 与本地持久化结构

### 9.1 bundle 的内存模型

<code>Preserve_snapshot_metadata</code> 是恢复语义的结构化内存模型，包含：

- snapshot identity、owner、created/expiry、recovered count；
- engine shape 与 transaction/session policy；
- binlog、GTID、ReadView；
- modified tables、record/table/predicate locks；
- MDL descriptor；
- SQL/InnoDB savepoints 与 participant topology；
- user vars；
- temp-table manifest；
- external blob descriptors。

<code>Preserved_trx_bundle</code> 再把 metadata、TLV payload、外部对象和 temp sidecar ownership 组合成发布单元。大对象可以外置，但 descriptor 的长度、摘要、类型和 generation 必须仍被 bundle 认证。

### 9.2 v9 snapshot 格式

当前本地文件格式版本是 9，magic 为 <code>MSP_PRES</code>，固定 header 为 552 字节。关键字段布局：

| 偏移 | 内容 |
|---:|---|
| 8 | format version |
| 10 | header size |
| 12 | MySQL version |
| 16 | flags |
| 20 | binlog state |
| 25 | recovered count |
| 32 / 40 | created / expiry |
| 48 / 56 | payload size / binlog cache size |
| 64 | server UUID，36 bytes |
| 100 | datadir fingerprint |
| 132 | token，最多 128 bytes |
| 260 / 324 / 452 | user / host / schema |
| 516 | HMAC-SHA256 |
| 548 | CRC32 |

编码先把 HMAC/CRC 字段清零，对最终布局计算 HMAC；填入 HMAC 后，再在 CRC 字段清零的条件下计算 CRC。CRC 发现介质损坏，HMAC 证明 artifact 未被跨 datadir/UUID 替换。

### 9.3 TLV 语义

必需 TLV 包括：

| type | 内容 |
|---:|---|
| <code>0x10</code> | InnoDB core |
| <code>0x11</code> | modified tables |
| <code>0x50</code> | session |
| <code>0x51</code> | MDL |
| <code>0x53</code> | transaction access |
| <code>0x54</code> | semantic contract |
| <code>0x42</code> | savepoint topology |
| <code>0x62</code> | auto increment |

可选 TLV 覆盖 modified table access、ReadView、三类锁、SQL/InnoDB savepoint、user vars、binlog metadata/cache、temp manifest 和 external descriptors。loader 对未知 optional TLV 可按版本策略跳过，但缺失必需 TLV、重复冲突或摘要不一致必须拒绝。

### 9.4 文件布局与发布顺序

<code>Preserved_trx_store::write_impl()</code> 的逻辑顺序是：

1. 检查 token 是否与已有 snapshot/marker/provenance 冲突；
2. 写入或领养 external blobs；
3. 编码 bundle；
4. standby projection 时写 standby marker；
5. 以临时文件、fsync、rename、directory fsync 发布 snapshot；
6. 返回 snapshot identity 给 provenance 绑定。

主要工件：

~~~text
<preserve_dir>/
  <token>.bin
  <token>.blob.<name>
  <token>.tainted
  <token>.consume_state
  <token>.standby_pending
  <epoch>.promotion_intent
  ... promotion adopted / abandoned / attach intent ...
  ... temp carrier owned sidecars ...
~~~

fast-directory 模式可能对 token 分片，不能假设所有 <code>.bin</code> 都平铺在根目录。所有扫描应走 carrier/store API，而不是手写目录匹配。

### 9.5 standby artifact 为什么仍在 default dir

receiver 最终 projection 直接写入目标 mysqld 的 preserve default dir，并加 <code>.standby_pending</code> 标记。这样：

- promotion 不需要再 copy 到另一个目录；
- artifact 继续复用现有 v9 读写、HMAC、external blob、temp sidecar 和清理代码；
- startup listing 能看见它用于 OFF-policy/运维审计；
- local startup recovery 通过 marker filter 明确排除它，不会误当成本地 shutdown token。

这不是“启动时恢复备机事务”。物理备机 read-only startup 会 defer preserve recovery；standby-pending 只有在线升主流程显式进入 gate 后才可能 adopt。

### 9.6 为什么 transfer 不直接复制源端 v9 文件

源端 v9 HMAC 绑定源 server UUID 和 datadir fingerprint。原样复制到目标会使认证身份错误，也会把源端文件布局细节变成跨节点协议。

Transfer 发送的是 portable semantic bundle/object contract；receiver 校验 source frame/manifest 后，再通过目标本地 store 重新编码 target-local v9 projection。共享的是 serializer、descriptor 和 store 能力，不是复制一个已经绑定源机器的最终文件。

### 9.7 artifact sink 是松耦合边界

preserve kernel 不直接写死“本地文件”或“网络”。它面向 <code>Preserve_trx_artifact_sink</code>：

| sink | 使用位置 | 结果 |
|---|---|---|
| <code>Preserve_trx_local_carrier_artifact_sink</code> | 本地 single/batch | 写 local v9 snapshot |
| <code>Preserve_trx_transfer_artifact_sink</code> | portable transfer 辅助路径 | 构造/发送跨节点对象 |
| <code>Preserve_trx_transfer_session_artifact_sink</code> | batch source epoch | 把 final bundle 纳入已有严格有序 epoch |
| <code>Preserve_trx_standby_pending_artifact_sink</code> | receiver projection | 用目标 store 写 target-local snapshot + standby marker |

这样复用 bundle serializer、store 和 cleanup，而不让 network/session 调度进入 InnoDB preserve kernel。未来增加多 data sessions 时，应扩展 source scheduler/sink 实现，不应复制 prepare/detach/serialize 逻辑。

## 10. Startup preflight 与 recover_all

### 10.1 启动时序

本地 shutdown/resume 的关键不是“recover_all 调得早”，而是它前后对象的顺序：

~~~mermaid
sequenceDiagram
  participant M as mysqld startup
  participant P as preserve preflight
  participant I as InnoDB recovery
  participant R as preserved_trx_recover_all
  participant B as purge/background

  M->>M: initialize server UUID / datadir identity
  M->>P: scan local recoverable artifacts
  P->>P: HMAC / TLV / provenance / binlog-mode validate
  M->>I: tc_log open + ha_recover + redo/DDL recovery
  I->>R: rebuild and claim matching prepared trx
  R->>R: import ReadView/locks/MDL, register records
  R->>R: reconcile orphan / consume / taint
  R-->>I: recovery_done
  I->>B: start purge and post-DDL background threads
~~~

[sql/mysqld.cc](../sql/mysqld.cc) 在 TC/storage recovery 前调用 <code>preserved_trx_preflight_recoverability()</code>；[storage/innobase/handler/ha_innodb.cc](../storage/innobase/handler/ha_innodb.cc) 在 redo/DDL recovery 后、purge/background threads 前调用 <code>preserved_trx_recover_all()</code>。

### 10.2 preflight 做什么

preflight 只读取 local-recoverable snapshot，不 claim trx：

- 加载 XID provenance；
- carrier 扫描并过滤 standby-pending；
- metadata/snapshot-only decode；
- 校验 v9 HMAC、CRC、server/datadir identity；
- 校验 required TLV、snapshot identity 和 provenance 绑定；
- 校验 binlog mode、engine shape 和 feature support；
- 在 InnoDB rollback 前建立“哪些 magic XID 应保护”的事实。

临时表路径还在新 temp space/rseg 分配前执行 <code>preserved_temp_images_bootstrap_preamble()</code>，预留 source space id、page、rseg 和 undo slot。

### 10.3 recover_all 如何防止事务回滚

redo recovery 重建 prepared trx 后，普通 XA/orphan cleanup 可能回滚无人认领事务。Preserve 通过三层约束阻止这一点：

1. XID 使用 Preserve 专用 format/GTRID。
2. provenance/preflight 证明该 XID 对应可认证 snapshot 或有效 intent。
3. InnoDB recovery rollback hook 查询 <code>trx_preserve_xid_should_be_protected()</code>，在 preserve recovery 完成前不把它当普通 orphan。

随后 <code>preserved_trx_recover_all()</code> 以共享 recover/adopt kernel claim 并转为 <code>PRESERVED</code>。未被 snapshot/provenance 保留的真正 orphan Preserve XID 才会被回滚。

### 10.4 recover_all 的其他和解

它还处理：

- consume-state 中断；
- stale temporary files；
- tainted artifacts；
- recovery attempt/recovered count；
- orphan external blobs；
- temp sidecar ownership；
- force-recovery 的 defer/taint policy；
- 并行 startup recovery worker。

<code>preserve_trx_startup_recovery_threads</code> 驱动 Auto_THD workers 并行恢复 token，但每个 token 仍经过同一 shared kernel。

### 10.5 physical standby startup 的明确规则

read-only InnoDB startup 分支会标记 preserve recovery deferred，并直接启动只读服务，不调用 local <code>recover_all()</code> 去 adopt standby artifact。这符合物理备机模型：

- transfer/prewarm 可在线后台执行；
- standby-pending artifact 可落盘；
- 正在运行的只读查询不被 MDL/trx attach 影响；
- 只有在线 promotion coordinator 显式冻结 apply、获取 physical fence 后，才调用 strict gate。

因此 startup 与 promotion **复用对象恢复内核**，但不是同一个调度入口，也不能让备机通过重启来完成升主。

### 10.6 feature OFF 时的 artifact policy

<code>preserve_trx_enable=OFF</code> 不等于可以无视磁盘上已有事务。startup-only <code>preserve_trx_off_artifact_policy</code> 有四种显式语义：

| policy | 行为 |
|---|---|
| <code>FAIL_IF_PRESENT</code> | 默认；listing 发现 local snapshot、standby-pending 或 promotion intent 等 Preserve artifact 时启动预检失败 |
| <code>IGNORE</code> | 显式忽略 artifact，不进入 recover/abandon；这是高风险运维选择 |
| <code>RECOVER</code> | SQL surface 仍关闭，但 startup 复用 recovery 扫描恢复已有 local token |
| <code>ABANDON</code> | SQL surface 关闭，显式回滚 Preserve prepared trx 并清理 artifact |

listing 把 standby/promotion marker 也纳入 FAIL_IF_PRESENT 预检；而正常 ON-path local recovery 仍会过滤 standby-pending。两者不矛盾：前者防止管理员在未知 artifact 下悄悄关特性，后者防止无 fence 的误 adopt。

## 11. Shared recover/adopt kernel

<code>preserved_trx_recover_or_adopt_bundle_shared()</code> 是 startup 与 promotion 的核心复用点。policy 分为：

- <code>LOCAL_STARTUP_RECOVERY</code>
- <code>STANDBY_PROMOTION_ADOPT</code>
- <code>STANDBY_PROMOTION_PHYSICAL_FENCE</code>

options 可以带 deadline、record-lock page 已预热事实、prebuilt record-lock plan 和 physical fence lease。

### 11.1 固定执行顺序

1. 校验 bundle/binlog/fence/deadline。
2. token 转 Preserve magic XID。
3. <code>trx_preserve_claim_prepared(xid)</code>；找不到时仅在合法 temp manifest 下走 temp-only claimed trx。
4. 将 claimed prepared trx 标记 <code>PRESERVED</code>。
5. 导入 isolation 与 ReadView。
6. 导入 table locks。
7. 导入 record locks；strict physical policy 消费 prebuilt plan、conflict check 和 apply journal。
8. 导入 predicate locks；strict physical 当前拒绝不支持的 predicate 组合。
9. 从 metadata 创建 detached MDL context。
10. 注册 SQL record。
11. 在关键步骤前后 revalidate physical fence/deadline。

startup 注册 <code>PRESERVED + resumable=true</code>；promotion 注册 <code>ADOPTED_FOR_PROMOTION + resumable=false</code>。

### 11.2 startup/promotion 的失败非对称

| 失败位置 | startup | promotion |
|---|---|---|
| claim 前 | 不改变 trx，报告 snapshot 错误 | token 保持 standby durable，gate fail |
| claim 后、register 前 | rollback claimed trx，并清理/taint 本地 snapshot | rollback claimed trx，但保留 standby durable artifact |
| register 后 | record/cleanup 和解 | durable intent + registry lease 决定 rollback/taint |

promotion 不能因为一次 gate 失败删除接收端唯一事实；它必须留下可审计 marker，并由明确 cleanup/retry policy 和解。

## 12. 本地 SQL RESUME

<code>preserved_trx_resume_record_on_thd()</code> 是已经抽出的 policy-aware resume core。当前 SQL wrapper 直接调用它；legacy/internal promotion policy 也可以使用其 <code>PROMOTION_RESUME_ON_THD</code> 分支。production strict prepared path 为了消费 registry 中的 native binlog handle、attach lease 和 durable intent，当前走独立入口 <code>preserved_trx_resume_adopted_for_promotion_on_thd()</code>，并没有直接调用这个函数。

通用 policy 为：

- <code>SQL_RESUME</code>
- <code>PROMOTION_RESUME_ON_THD</code>

后者是内部免权限策略，不可从 SQL 用户入口构造。两条 attach 路径复用了底层 session/MDL/temp/InnoDB helper，但 strict attach 仍有一段与普通 resume 重叠的恢复顺序；这是当前仍可继续统一、但不能为了形式复用而破坏 prepared-resource ownership 的实现边界。

### 12.1 claim 前检查

- feature 开启、非 read-only、非 force recovery；
- startup preserve recovery 已完成；
- token owner/RESUME_ANY 和对象权限；
- token 未过期、record resumable 符合 policy；
- temp preclaim/namespace 可用；
- 目标 THD pristine；
- binlog mode/format 兼容；
- 从 durable artifact 再读 metadata-only snapshot，防止只信进程内 record。

SQL policy 从 <code>PRESERVED</code> take record；promotion policy 只从 <code>ADOPTED_FOR_PROMOTION</code> take，且普通 SQL 无法 claim 后者。

### 12.2 THD 恢复顺序

<code>Resume_thd_state_guard</code> 负责失败回滚。主要顺序：

1. 恢复 isolation、read/write policy、session options、sql_log_bin。
2. 恢复 insert id、user vars 和 DML policy。
3. lazy hydrate/import binlog cache。
4. 恢复 detached MDL。
5. 恢复 GTID ownership，准备 GTID undo。
6. materialize temp sidecars。
7. 调用 <code>trx_preserve_attach_to_thd()</code>。
8. 恢复 SQL/InnoDB savepoints 和 participant topology。
9. reseed temp baseline。
10. 写 <code>CONSUME_PENDING</code>。
11. 调用 <code>trx_preserve_activate_resumed()</code>。
12. 写 <code>ACTIVE_CONSUMED</code>。
13. 删除 detached MDL backup、snapshot、blob、sidecar 和 provenance。

### 12.3 activation 是不可逆业务边界

activation 前失败，可以把 claimed trx 还回 token record 或 rollback；activation 后，事务已经属于目标 THD，artifact cleanup 失败不能再把活事务撤销。代码会保持 ACTIVE 并写 taint/cleanup 状态，由后续和解清理残留工件。

## 13. Transfer 协议与 Source 端

### 13.1 协议对象

当前 wire protocol 为 v3，主要 frame：

| Frame | 作用 |
|---|---|
| <code>DECLARE_TOKEN</code> | 声明 token/source/target/LSN |
| <code>DECLARE_OBJECT</code> | 声明对象 kind、size、digest |
| <code>BEGIN</code> | 打开 token 接收上下文 |
| <code>OBJECT_CHUNK</code> | 写入对象 range |
| <code>SEAL_OBJECT</code> | 校验 coverage 与 streaming digest |
| <code>COMMIT_EPOCH</code> | 发布 epoch fact/commit barrier |
| <code>ABORT</code> | token/epoch 失败清理 |
| <code>PROMOTION_PREWARM_TOKEN</code> | 触发 legacy/receiver prewarm |
| <code>PROMOTION_GATE_EPOCH</code> | 协议已有的 gate 调用面，不能等同生产 HA 入口 |

object kind 枚举包括 <code>SNAPSHOT_BUNDLE</code>、<code>EXTERNAL_BLOB</code> 和 <code>TEMP_TABLE_SIDECAR</code>。第三种是协议预留能力；当前 portable sender 对非空 temp-table manifest 返回 <code>UNSUPPORTED</code>，尚没有完整跨节点 temp sidecar 交付。

<code>PROMOTION_PREWARM_TOKEN</code>/<code>PROMOTION_GATE_EPOCH</code> 当前驱动的是 legacy ready-cache/apply-barrier 入口，最终调用 <code>preserved_trx_adopt_standby_pending_all_for_promotion()</code>。strict production 入口是 <code>preserved_trx_adopt_prepared_epoch_for_physical_promotion()</code>；它消费 strict registry 和 production physical-fence lease，当前没有被 wire frame 当作生产 HA 控制面调用。

### 13.2 manifest 与 epoch fact

每个 token manifest 保存：

- <code>uint64_t token</code>，即 source connection id；
- source/target server UUID；
- <code>source_prepare_lsn</code>；
- <code>source_epoch_commit_lsn</code>；
- 对象 descriptor 集合。

epoch fact 再保存 source fence LSN、token facts 和 fact digest。当前 strict prewarm 选择：

<code>required_apply_lsn = source_epoch_commit_lsn</code>

并要求 <code>source_epoch_commit_lsn &lt;= source_fence_lsn</code>。这是当前代码事实；物理复制组件仍需证明源端 LSN 与目标 apply provider 的坐标可比且连续。

### 13.3 source epoch session

<code>Preserve_trx_transfer_source_epoch_session</code> 串联一次 epoch：

~~~text
declare token(s)
→ declare objects
→ begin prewarm manifest
→ stream phase1 prebuilt objects
→ queue final metadata
→ finalize each token
→ commit epoch

失败：abort token / abort epoch
~~~

source preserve workers可以并行构造 token/bundle/object，但一个 epoch session 对 frame sequence 加 mutex，确保 sequence、digest 和 ACK 重试是确定的。phase1 batch sender 在后台合并多个小对象/帧，减少网络往返；它不改变最终 barrier。

### 13.4 classic-protocol sender

configured direct sender 使用独立后台 classic-protocol session 连接目标 mysqld，带 receiver credential/TLS policy，不复用 HA 控制面。接收 ACK 后校验：

- source incarnation；
- epoch + sequence；
- frame digest；
- status；
- ACK HMAC。

状态包括 OK、INVALID_ARGUMENT、CORRUPT、IO_ERROR、UNSUPPORTED、RESOURCE_EXHAUSTED、ACK_UNCERTAIN。ACK_UNCERTAIN 必须按 sequence/idempotency 重查或重试，不能假定目标没收到。

### 13.5 当前并行度事实

当前实现不是“所有东西只有一个 session”：

- 源端 batch preserve workers 可并行构造 token；
- source epoch sequence 仍由 session 串行化；
- phase1 有后台 batch sender；
- receiver 会按 token group 并行；
- persistent prewarm worker pool 已实现。

当前源码的 <code>preserve_trx_transfer_receiver_workers</code> 默认值是 **8**；promotion gate 的 batch/workers 默认值才是 **3**。早期“data/sender/receiver 默认 3”方案不是当前 sysvar 事实，文档和运维基线必须以源码为准。

## 14. Receiver：先认证保存，再异步处理

### 14.1 classic dispatch 与访问控制

<code>COM_PRESERVE_TRX_TRANSFER</code> 只有在 feature、receiver startup option 和动态权限都满足时进入 <code>preserve_trx_transfer_dispatch_command()</code>。普通业务连接不能伪造 receiver frame。

### 14.2 ACK 的精确定义

receiver 首先把 frame 追加到 replay spool。单条 spool record 包含：

~~~text
magic + encoded_length + frame_digest + encoded_frame
~~~

代码对 short write 会 truncate 回原长度，并在 append 后 close 文件。**当前 append 路径没有对 spool file 和父目录执行 fsync。**

ACK callback 在 pre-admission/spool append 成功后、semantic apply 前发送。因此当前 ACK 的准确语义是：

> 目标端已经认证该 frame，并把可重放字节追加/关闭，接受其进入后续处理。

ACK **不表示**：

- token 已经 <code>SAVED_ONLINE</code>；
- epoch 已 commit；
- standby v9 projection 已发布；
- token 已 promotion-ready；
- 掉电后 spool 必然存活。

如果产品合同要求 power-loss durable ACK，必须补 spool fsync/directory fsync 或调整 ACK 时点，不能只改文档措辞。

<code>preserve_trx_transfer_replay_receiver_spool()</code> 已提供 frame replay 能力并有 GUnit 覆盖，但当前普通 mysqld startup 没有生产调用者。目标进程重启后的 receiver bootstrap、staging scan 与 strict registry 重建仍需显式接入；这与“read-only startup 不 adopt/resume standby token”不冲突，前者只恢复接收/prewarm 元数据，后者禁止提前 claim 事务。

### 14.3 receiver 并行调度

batch handler：

1. 解包 batch；
2. 按 epoch/sequence 排序；
3. 做 pre-admission 和 idempotency；
4. 追加 spool；
5. 发送 ACK；
6. 按 token 分组投递 receiver workers；
7. 将 <code>COMMIT_EPOCH</code> 和 promotion gate 作为串行 barrier。

token A/B 的 chunk/seal 可以并行，但同 token 的 sequence 和 range 状态不能乱序。commit barrier 必须看到 epoch 内所有 token 均达到 sealed 条件。

### 14.4 staging 目录与对象状态

接收中的对象放在：

~~~text
<preserve_dir>/.transfer/<epoch>/
  receiver spool
  token manifests
  token object/range files
  epoch.fact
  epoch.commit
~~~

<code>DECLARE_TOKEN/BEGIN</code> 建 receiver record 并预留 inflight bytes；<code>DECLARE_OBJECT</code> 固化 size/digest；<code>OBJECT_CHUNK</code> 写 range；<code>SEAL_OBJECT</code> 检查完整 coverage 并对文件做 streaming SHA-256，不整文件一次性读入。

超过 <code>preserve_trx_transfer_max_inflight_bytes</code> 时当前代码在多个 admission 点直接拒绝。它已经具备阈值 backpressure，但尚不是完整的 BUSINESS_FIRST/BALANCED/PROMOTION_PREPARE runtime profile，也没有统一 IO bytes/sec、commit batch、yield sysvar。那些仍属于设计目标，不能写成已实现资源治理。

### 14.5 COMMIT_EPOCH

commit barrier 要求 epoch 内 receiving token 的对象全部 sealed，然后：

1. 构造并持久化 epoch fact；
2. 写 epoch commit marker；
3. 使用 fsync/rename/directory fsync 发布 final fact；
4. 将 committed fact 绑定到 strict prepared registry；
5. 尝试复用 seal 阶段已完成的 prewarm；
6. 排队 final token staging/projection。

standby projection 不在 strict READY 的唯一关键路径上。receiver 可以先从 staging/prebuilt resources 建 ready registry，再由后台发布 target-local v9 snapshot；无论顺序如何，promotion gate 都必须同时校验 registry final facts 与 durable epoch fact。

### 14.6 receiver record 与 SQL record 完全分离

<code>Preserve_trx_transfer_receiver_record</code> 明确不进入 <code>g_preserved_trx_records</code>。因此 transfer/prewarm 期间：

- 不 claim InnoDB prepared trx；
- 不注册 SQL RESUME token；
- 不挂目标 THD；
- 不获取业务 MDL；
- 不改变正在运行的 read-only transaction；
- prewarm cache 失败不破坏已 sealed/durable object。

这正是备机仍承载只读流量时的核心隔离边界。

## 15. Receiver prewarm 与 prepared resources

### 15.1 persistent worker pool

receiver prewarm workers 由 <code>preserve_trx_transfer_receiver_workers</code> 驱动并按需启动，处理三类 job：

- object-level record-lock prewarm；
- staged-token semantic prewarm；
- committed-epoch final-fact binding。

队列对 inflight/done/deferred key 去重；同 token 新 generation 可以覆盖旧 attempt，但 boot incarnation/generation 不匹配会进入 stale，而不是复用缓存。

### 15.2 semantic prewarm

<code>prepare_strict_bundle_for_receiver()</code> 的主要工作：

1. 从 encoded manifest 计算 object-set digest。
2. 获取预建 record-lock plan 和 page/generation proof。
3. 为 binlog cache 建 file-backed staging payload reader。
4. 计算 native binlog cache resource facts、所需 bytes、fd 和 tmpdir。
5. registry <code>begin_prepare</code>。
6. 获取 lock-plan 与 native-binlog resource leases。
7. 安装 semantic bundle；bundle 内 binlog body 清空、external body 移除，避免重复持有大 payload。
8. 安装 lock plan 与 native binlog handle。
9. <code>publish_prewarmed</code>。

这些对象是可丢弃内存缓存，durable source 仍是 receiver staging、epoch fact 和 standby projection。cache eviction 后可以重新 prewarm，不能反过来把 cache 当唯一事实。

### 15.3 final facts

commit 后绑定：

- <code>required_apply_lsn = source_epoch_commit_lsn</code>；
- <code>physical_fence_lsn = epoch_fact.source_fence_lsn</code>；
- epoch fact digest；
- final lock generation digest；
- page layout digest；
- dictionary generation digest；
- prewarm object-set digest；
- target boot incarnation；
- deadline/resource facts。

只有 facts 和 resource leases 都成功绑定，registry 才从 <code>PREWARMED_PENDING_FINAL_FACT</code> 走到 <code>READY_FOR_GATE</code>。

### 15.4 对只读业务的资源边界

当前实现已经有 max inflight、resource lease、worker queue、后台 prewarm 和 streaming digest，但仍应把以下事实区分开：

| 能力 | 当前状态 |
|---|---|
| receiver worker 并行 | 已实现 |
| threshold inflight rejection | 已实现 |
| seal streaming SHA-256 | 已实现 |
| file-backed native binlog prepare | 已实现 |
| strict lease/resource facts | 已实现 |
| 统一 runtime profile | 未实现 |
| 全局 IO bytes/sec token bucket | 未实现 |
| commit publication bounded batch sysvar | 未实现 |
| 只读负载反馈自动 pause | 未实现 |
| ready cache 全局 bytes cap/eviction policy | 仍需完整治理 |

因此当前代码能避免很多 gate 内重 IO，但还不能仅凭 worker pool 宣称对 read-only workload 有完整 QoS 隔离。

## 16. Promotion：fence、adopt、attach、activate

### 16.1 两套 promotion 内存结构

当前代码同时存在：

1. legacy/phase-B ready cache：状态包括 RECEIVED_DURABLE、HYDRATING、DRY_VALIDATED、PREWARMED_PENDING_FINAL_FACT、APPLY_PENDING、APPLY_REACHED、READY、CORRUPT；
2. strict prepared registry：持有完整 key、facts、resource lease、lock plan、semantic bundle、native binlog handle 和 intent。

生产物理升主应以 strict registry + production physical fence 为权威；legacy cache 主要保留兼容、测试和阶段性路径，不能与 production-ready 等价。

### 16.2 physical fence provider

fence mode：

- <code>NONE</code>
- <code>TEST_SAME_INSTANCE_ATTACH_ONLY</code>
- <code>TEST_ONLY_PHYSICAL_FENCE_SIMULATOR</code>
- <code>PRODUCTION_REDO_APPLY_FENCE</code>

proof 绑定 source lineage、target/boot/provider generation、source/target LSN、epoch/final-lock/page-layout/dictionary digest、apply frozen 和 continuity。lease 支持 acquire/revalidate/release。

strict physical gate 只接受 production mode。当前仓库有 provider 注册接口和测试 provider，但没有生产 redo-apply provider，也没有 HA 角色切换流程的生产 caller；这是明确的 **接口预埋/仓库外集成**。

### 16.3 gate 的准确工作

promotion gate 不做 resume 到 Ty。它只把已经物理存在的 prepared trx 与已 prewarm 语义绑定为 promotion-owned record：

1. 校验 consistency mode、worker 上限和 deadline。
2. 获取 production physical fence lease。
3. 校验 registry READY、boot/generation/facts/digests。
4. 聚合 epoch physical digests 并与 proof 对比。
5. revalidate fence。
6. 写 durable <code>ADOPTING</code> intent。
7. worker 并行执行 shared recover/adopt kernel。
8. 成功写 <code>ADOPTED_LOCKED</code>；失败写 rolled-back/not-found/tainted outcome。

strict gate 只消费 registry 中已经发布且持有 lease 的 semantic bundle、lock plan 和 native resources。READY/cache miss 必须返回 not-ready；不能在 gate 内临时扫描 standby files、全量 hydrate external blobs 或回退到无限等待，否则 1 秒 deadline 与 read-only 业务隔离都失去意义。

当前 <code>preserve_trx_promotion_gate_batch_tokens=3</code>、<code>preserve_trx_promotion_gate_workers=3</code>、timeout 默认 1000ms。worker_count 已用于并行 strict/legacy gate，不是只做参数校验。

但 batch cap 的代码边界并不完全统一：<code>gate_batch_tokens</code> 由 legacy options 构造器读取，strict physical gate implementation 目前只要求 tokens 非空并限制 <code>worker_count &lt;= 8</code>，没有直接读取全局 batch-token sysvar。因此“1 秒”只对明确受限的 N 有意义；生产 HA caller 必须限批，或后续把同一 cap 下沉到 strict API，不能对任意 token 数作常数时间承诺。

### 16.4 durable intent 的崩溃语义

promotion intent 状态：

~~~text
ADOPTING
→ ADOPTED_LOCKED
→ ABANDONED_ROLLED_BACK
→ ABANDONED_NOT_FOUND_PROVEN
→ CLEANUP_TAINTED
~~~

attach intent 状态：

~~~text
ATTACHING → ACTIVATING → ACTIVE
          → ATTACH_ROLLED_BACK / ATTACH_TAINTED
          → CLEANUP_PENDING → CLEANUP_ROLLED_BACK / CLEANUP_TAINTED
~~~

intent 必须先于相应不可逆内存 ownership 变化落盘，用于 crash 后判断“可以重试、已经 rollback、已激活但工件待清理，还是所有权无法证明”。listing 已能观察 promotion intent；完整生产 reconciliation 仍需与实际 HA startup/promotion policy 一起闭环，不能把 marker 存在本身当作和解完成。

### 16.5 adopt 后为何不能被 SQL 抢走

shared kernel 为 promotion 注册：

<code>ADOPTED_FOR_PROMOTION + resumable=false</code>

普通 SQL RESUME 只接受普通 <code>PRESERVED</code> 可 resumable record。strict attach 通过内部 take API 领取 promotion record，因此 gate→attach 窗口没有 token 可见性抢占。

### 16.6 strict attach 到 Ty

attach 前外部 peerid/provider 把 Tx 映射到 Ty，并返回受保护的 <code>Preserved_trx_peer_thd_handle</code>。strict attach 的核心顺序：

1. 先写 durable <code>ATTACHING</code> intent。
2. registry 从 <code>ADOPTED_LOCKED</code> begin attach。
3. take <code>ADOPTED_FOR_PROMOTION</code> SQL record，并把执行上下文切到受保护 Ty。
4. 恢复 isolation、session、DML policy、binlog flags、insert id 和 user vars。
5. 从 attach lease 取 native binlog handle 并开始 ownership transfer；失败可 abort 回 registry。
6. clone detached MDL，恢复 GTID ownership。
7. materialize temp sidecars。
8. attach claimed <code>trx_t</code> 到 Ty，再恢复 savepoint topology。
9. deadline 复核通过后，registry <code>begin_activation()</code> 通过回调写 <code>ACTIVATING</code> intent，并提交 activation ownership boundary。
10. commit native binlog ownership，再激活 InnoDB undo/trx。
11. registry <code>commit_attach()</code> 通过回调写 <code>ACTIVE</code>，registry 进入 ACTIVE。
12. attach 函数清理 standby artifact/intent；registry 保持 <code>ACTIVE</code>，后续 registry expire/reaper 释放 Preserve-owned resources，并转为 <code>ACTIVE_ARTIFACTS_CLEANED</code>。

Ty 在整个窗口不能读新客户端命令，不能被释放；KILL/teardown 被 handle 延迟。写入 ACTIVATING intent 后但函数尚未成功返回时，代码仍会尝试在受控路径中回滚；如果 ownership 已 taint，则保留给运维和解。只有 ACTIVE 已提交并把成功返回给调用者后，后续 artifact cleanup 失败才只能保留事务并进入 cleanup-pending/taint，不能偷偷回滚已经交给客户端的事务。

## 17. Source 与 Receiver 的逻辑对照

| Source 动作 | Wire/事实 | Receiver 动作 | promotion 使用 |
|---|---|---|---|
| phase1 发现 THD | DECLARE_TOKEN | 建 receiver record、预留 bytes | 建 token identity |
| warmcopy blob 完成 range | DECLARE_OBJECT + CHUNK | 写 staging ranges | prewarm payload source |
| object final fence | SEAL_OBJECT | coverage + streaming digest | 当前 lock/binlog proof；temp kind 尚未端到端启用 |
| preserve kernel 构造 final bundle | SNAPSHOT_BUNDLE | semantic decode/validate | shared recover input |
| 全 token durable | COMMIT_EPOCH | epoch.fact + epoch.commit | final fact digest |
| source physical LSN | prepare/commit/fence LSN | registry required_apply_lsn | physical fence compare |
| source abort/rollback | ABORT | mark/cleanup staging | token 不可 gate |

Source 与 receiver 不共享指针、THD 或 InnoDB trx。它们只通过 manifest、对象摘要、epoch fact 和 token 身份关联。真正的 InnoDB 事务本体由物理复制/redo 在目标端重建，transfer 只补 SQL/运行时语义；promotion fence 才证明这两条数据面在同一 epoch 上对齐。

## 18. 失败边界与所有权矩阵

| 失败点 | trx owner | artifact 状态 | 正确动作 |
|---|---|---|---|
| prepare 前 | 源 THD | 无/phase1 临时对象 | abort participant，原事务继续 |
| prepare 后、detach 前 | 源 THD/引擎 prepare | 未发布 | reactivation/rollback，删除临时对象 |
| detach 后、snapshot 前 | Preserve kernel | 未 durable | reattach source 或显式 rollback |
| local snapshot durable、OK 未交付 | hidden record | local artifact | rollback token，不能留未知 token |
| batch 某 token 失败 | 已成功 token 可能 detached | 部分 artifact | 全批 source rollback/reattach |
| transfer ACK uncertain | source 仍保留 epoch ownership | spool 是否存在未知 | sequence/idempotent retry，不能 silent drop |
| object sealed、epoch 未 commit | receiver staging | 不可 promotion | 等待 commit 或 abort cleanup |
| epoch committed、prewarm 失败 | target durable staging/projection | durable | 保留 artifact，可重试 prewarm |
| gate claim 前失败 | target prepared trx 未 claim | standby durable | fail closed，保留重试证据 |
| gate claim 后失败 | gate lease owner | intent 已写 | rollback claimed trx 或 taint |
| attach before activation 失败 | promotion record/registry | attach intent | 恢复 record/resource ownership 或 rollback |
| activation 后 cleanup 失败 | Ty 上 ACTIVE trx | artifact 残留 | 保持事务，标记 cleanup pending/taint |

总原则：越过 durable/activation 边界后，不能用“删文件”或“回滚一切”掩盖所有权不确定；必须留下可审计状态并 fail closed。

## 19. 只读备机隔离与非侵入边界

### 19.1 transfer/prewarm 期间允许做的事

- 写 receiver spool/staging 和 standby artifact；
- streaming digest；
- 解析 manifest/metadata；
- dry semantic validation；
- 构造 lock plan；
- 构造 file-backed native binlog handle；
- 获取 Preserve 专属内存/文件 resource lease；
- 构建可丢弃内存 cache。

### 19.2 gate 前明确禁止

- claim/改变 InnoDB <code>trx_t</code>；
- 调用 <code>preserved_trx_add_record()</code>；
- 向 Ty attach；
- 获取业务 MDL；
- 导入 ReadView/locks；
- 激活 undo；
- 让 standby token 进入普通 SQL RESUME；
- 删除唯一 durable artifact。

### 19.3 原生热路径约束

command、binlog、MDL、lock、temp/FSP/undo 的 hook 必须由以下至少一层 gate 隔离：

- <code>preserve_trx_enable</code>；
- temp/transfer/warmcopy 子特性 sysvar；
- active drain/warmcopy epoch；
- internal promotion policy；
- target THD batch state。

OFF 时普通 MySQL 8.0.22 行为必须不变。lock/binlog 的热 hook 只能记录 Preserve side state；bookkeeping 失败不应让原生加锁或 cache append 失败，除非已经进入显式 Preserve 命令并由该命令承担失败。

## 20. 配置与观测面

### 20.1 主要配置族

| 配置族 | 代表参数 |
|---|---|
| feature/storage | <code>preserve_trx_enable</code>、default dir、temp enable |
| token capacity | max total、per user、batch |
| timeout | default/min/max、recovery count、grace |
| payload | snapshot/binlog/temp max bytes |
| resource | total memory、per-token memory、spill chunk |
| locks | max locks/tables/pages、materialize timeout |
| drain | mode、grace、hard timeout、parallel preserve threads |
| startup | startup recovery threads |
| binlog warmcopy | enable、close/min-open、chunk、tail、max total、pending |
| lock warmcopy | enable、fallback、memory、journal、dirty shards、seal threads |
| transfer | receiver enable、source/target endpoint、credential、artifact mode、receiver workers、chunk、max inflight、commit timeout、phase1 batch/linger |
| promotion | gate batch tokens、workers、timeout |

当前关键默认值：

| 参数 | 当前默认 |
|---|---:|
| max total / per user / batch | 256 |
| default preserve timeout | 300 s |
| recovery count / grace | 3 / 120 s |
| snapshot max | 16 MiB |
| binlog/temp max | 1 GiB |
| total/per-token memory | 256 MiB / 64 MiB |
| spill chunk | 4 MiB |
| record locks/tables/pages | 2000 / 64 / 20000 |
| drain grace/hard timeout | 30 s / 30 s |
| transfer receiver workers | 8 |
| transfer chunk/max inflight | 1 MiB / 1 GiB |
| transfer commit timeout | 30 s |
| phase1 batch/linger | 4 MiB / 20 ms |
| promotion gate batch/workers/timeout | 3 / 3 / 1000 ms |

具体上限和 dynamic/read-only 属性必须回到 [sql/sys_vars.cc](../sql/sys_vars.cc) 核对。

### 20.2 metrics

<code>preserve_trx_resource.cc</code> 使用 <code>DEFINE_PRESERVE_TRX_SHOW_FUNC</code> 暴露 status variables，并在 mysqld status table 注册。观测至少要分四层：

- drain：manager state、target/quiesce/preserve counts、stage latency；
- resource：memory/file/worker lease、spill、拒绝；
- transfer：frame/ACK、inflight bytes、receiver queue、sealed/durable token；
- promotion：prewarm/ready/gate/adopt/attach state、deadline、last failure。

现有 metrics 能覆盖大量资源与阶段事实，但“runtime profile、throttled ms、last throttle reason、worker active/idle、ready cache total bytes”应以源码是否注册为准，不能从设计文档推定已经可 SHOW STATUS。

## 21. 测试地图与证据强度

| 测试层 | 适合证明 | 不能单独证明 |
|---|---|---|
| GUnit | codec、state machine、resource lease、plan、failure branch | 两个真实 mysqld 的 durability/时序 |
| source-shape lint MTR | OFF-path gate、内部 policy 不可达、调用分层 | runtime 语义 |
| preserve_trx MTR | SQL 行为、startup/restart、artifact、权限、错误合同 | 真实跨节点网络与物理 apply |
| business E2E | 真 mysqld、真实客户端、SIGKILL、语义对账 | production HA provider 本身 |
| NFR benchmark | P50/P95/P99、1 秒 gate 分段、资源回归 | 未覆盖对象类型的正确性 |
| test promotion plugin/simulator | provider API、gate/attach 功能 | 生产 redo apply fence |

已有长跑/E2E 基建包括 <code>resumable_trx_longrun_e2e.py</code>、<code>resumable_trx_business_e2e.py</code> 和 <code>resumable_trx_nfr2_benchmark.py</code>。新的双 mysqld transfer/promotion 验证应扩展现有 harness，而不是再造一套不共享启动、故障注入和指标报告的脚本。

对 transfer/promotion 的“等价 E2E”至少要走真实 COM dispatch、真实 carrier 写入、真实 epoch fact、真实 prewarm/gate 和客户端可读 metrics；单进程手写 staging 文件只能算 fixture/GUnit。

## 22. 当前实现边界与待闭环项

### 22.1 已实现的主干能力

- 本地单事务与批量 drain；
- 两阶段 binlog/lock/temp warmcopy；
- parallel preserve；
- v9 authenticated snapshot、external blobs、temp sidecars；
- startup preflight/recover_all；
- shared startup/promotion recover-adopt kernel；
- SQL resume core；
- transfer protocol/source/receiver/spool/staging/epoch fact；
- receiver token-group worker 与 persistent prewarm pool；
- strict prepared registry/resource lease/intent；
- physical fence provider interface；
- parallel promotion gate；
- promotion-owned record 隔离；
- protected peer THD handle 与 strict attach/activate core。

### 22.2 接口已预埋但生产未闭环

- production redo-apply physical fence provider；
- HA 角色切换流程对 strict gate/attach 的调用；
- Proxy peerid Tx→Ty resolver；
- receiver restart 时对 spool/staging 的生产 replay/bootstrap；
- crash-during-promotion intent 的生产启动/再次晋升 reconciliation policy；
- online non-empty ReadView 导入与 purge/visibility 合同；
- temp-table image/no-redo undo sidecar 的跨节点 portable transfer；
- promotion MDL 的 1 秒 deadline-aware/fail-fast 策略；
- strict physical gate 与 legacy gate 的统一 token batch cap/SLO 预算；
- strict attach 与 SQL resume 之间尚未完全抽取的 session/MDL/GTID/temp/savepoint 公共恢复步骤；
- read-only workload feedback 与完整 transfer/prewarm QoS governor；
- ready/prepared cache 的全局 bytes cap 和 eviction；
- power-loss durable ACK 合同；
- 真实双 mysqld + 物理 apply 的 release E2E。

### 22.3 不应再沿用的旧结论

- “transfer/promotion 尚未有代码”：错误，当前已有较完整骨架和 worker/registry/gate/attach。
- “receiver workers 只是 reserved 参数”：错误，当前已驱动 receiver/prewarm 并行。
- “worker_count 未驱动 gate 并行”：错误，当前 strict/legacy gate 已并行。
- “resume core 尚未抽出”：错误，当前存在 <code>preserved_trx_resume_record_on_thd()</code>。
- “standby artifact 要从 inbox copy 到 default dir”：错误，receiver projection 本来就写 default dir，以 marker 隔离。
- “备机重启时恢复 transfer token”：错误，物理备机 read-only startup defer；promotion 是在线流程。
- “ACK 等于 artifact durable/ready”：错误，当前 ACK 时点早于 semantic apply 且 spool 未 fsync。
- “runtime profile 与 IO BPS 已实现”：错误，当前只有部分阈值、lease、queue 和 streaming 能力。

## 23. 推荐源码阅读顺序

### 路线 A：先看本地 preserve/resume

1. <code>preserve_trx.h</code>：manager、stage、SQL command。
2. <code>sql_parse.cc</code>：command read/inflight/response boundary。
3. <code>Preserve_trx_drain_service::execute()</code>：批量总时序。
4. <code>preserve_trx_kernel_preserve_attached_transaction()</code>：共享 preserve kernel。
5. <code>trx0preserve.cc</code>：prepare/claim/detach/attach/activate。
6. <code>preserve_trx_bundle.cc</code> 与 <code>carrier_file.cc</code>：语义到文件。
7. <code>preserved_trx_preflight_recoverability()</code> 和 <code>preserved_trx_recover_all()</code>。
8. <code>preserved_trx_resume_record_on_thd()</code>。

### 路线 B：再看对象

1. ReadView：<code>trx0preserve.cc</code>、<code>read0read.h</code>。
2. locks：<code>lock0preserve.cc</code>、<code>lock0warmcopy.cc</code>、<code>lock0preserve_plan.h</code>。
3. binlog：<code>binlog_warmcopy.cc</code>、binlog cache integration、<code>binlog_preserve_prepared.h</code>。
4. temp：<code>preserve_trx_temp_table.cc</code>、temp carrier、<code>trx0temp_preserve.cc</code>。
5. savepoints/user vars/privileges：<code>preserve_trx.cc</code> 对应 helper 区域。

### 路线 C：最后看跨节点

1. <code>preserve_trx_transfer.h</code>：frame、manifest、ACK、receiver record。
2. source epoch session 与 classic sender。
3. receiver dispatch/spool/object handlers/commit barrier。
4. receiver prewarm workers 与 <code>prepare_strict_bundle_for_receiver()</code>。
5. <code>preserve_trx_promotion_prepared.h/.cc</code>：registry、facts、lease、intent。
6. strict physical gate。
7. <code>preserved_trx_recover_or_adopt_bundle_shared()</code>。
8. peer handle 与 strict attach/activate。

## 24. 关键符号索引

| 目标 | 建议搜索符号 |
|---|---|
| 全局 drain | <code>Preserve_trx_drain_service::execute</code> |
| participant | <code>Preserve_trx_drain_orchestrator</code> |
| preserve kernel | <code>preserve_trx_kernel_preserve_attached_transaction</code> |
| single token finalizer | <code>preserved_trx_finalize_statement_response</code> |
| startup preflight | <code>preserved_trx_preflight_recoverability</code> |
| startup recovery | <code>preserved_trx_recover_all</code> |
| shared adopt | <code>preserved_trx_recover_or_adopt_bundle_shared</code> |
| SQL/policy-aware resume core | <code>preserved_trx_resume_record_on_thd</code> |
| strict promotion attach | <code>preserved_trx_resume_adopted_for_promotion_on_thd</code> |
| InnoDB claim | <code>trx_preserve_claim_prepared</code> |
| InnoDB detach/attach | <code>trx_preserve_detach_current_thd</code> / <code>trx_preserve_attach_to_thd</code> |
| undo activation | <code>trx_preserve_activate_resumed</code> |
| ReadView | <code>trx_preserve_export_read_view</code> / <code>trx_preserve_import_read_view</code> |
| local store | <code>Preserved_trx_store::write_impl</code> |
| source epoch | <code>Preserve_trx_transfer_source_epoch_session</code> |
| receiver dispatch | <code>preserve_trx_transfer_dispatch_command</code> |
| strict prewarm | <code>prepare_strict_bundle_for_receiver</code> |
| strict registry | <code>Preserve_trx_prepared_token_registry</code> |
| legacy promotion gate | <code>preserved_trx_adopt_standby_pending_all_for_promotion</code> |
| strict physical gate | <code>preserved_trx_adopt_prepared_epoch_for_physical_promotion</code>、<code>begin_gate_adopt</code> |
| target THD protection | <code>Preserved_trx_peer_thd_handle</code> |
| promotion attach | 搜索 <code>strict_attach</code> 与 <code>ACTIVATING</code> intent |

## 25. 总结：代码中真正共享的是什么

Preserve / Resume 的共享核心不是某一个“大接口”，而是四层稳定合同：

1. **身份合同**：token ↔ Preserve XID ↔ provenance/epoch。
2. **对象合同**：structured bundle + authenticated descriptors + external payload。
3. **引擎合同**：prepare/claim/import/detached MDL/register/attach/activate。
4. **失败合同**：durable intent、ownership lease、rollback/taint 和 fail-closed。

本地 startup 负责在 purge 启动前恢复这些合同；物理备机 promotion 负责在 production physical fence 下在线恢复同一合同。两者应继续复用 shared kernel 和 resume core，但调度、资源准备和安全前提不能混为一谈：

- startup 可以扫描本地工件、在 purge-disabled 窗口导入对象；
- transfer/prewarm 必须不打扰备机只读业务；
- promotion gate 必须只消费 READY/prewarmed 资源并受毫秒 deadline；
- attach 必须在受保护 Ty 上完成；
- 任何“已准备好”的结论最终都要由 durable epoch fact、physical fence、registry generation 和 intent 共同证明。
