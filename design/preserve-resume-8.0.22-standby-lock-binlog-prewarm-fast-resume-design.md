# MySQL 8.0.22 Preserve/Resume 备机 Lock + Binlog Prewarm 与 100ms Resume 设计

| 字段 | 值 |
|---|---|
| 日期 | 2026-07-11 |
| 分支 | `codex/user-temp-table-phase1-drain-resume` |
| 源码核查 HEAD | `9ff05366b7e0` |
| 修订状态 | 多轮独立 review 后的物理一致性与生命周期收敛版 |
| 设计范围 | Receiver prewarm、升主前 epoch gate、连接级 fast resume |
| 单连接性能目标 | epoch gate 完成后，服务端 Preserve/Resume resume core `<= 100ms` |
| 产品边界 | 本仓库没有真实物理备机、redo apply coordinator、HA promote hook 和生产 client-to-THD 映射 |

## 1. 摘要

未来物理备机升主时，Preserve/Resume 不能在新连接执行 resume 时再做大对象读取、
record-lock 页面冷读、binlog cache 整体复制或文件重写。本设计把流程拆成三个不可混淆
的阶段：

```text
receiver prewarm
  -> promotion epoch gate
  -> per-connection fast resume
```

Receiver prewarm 在业务仍运行时完成 artifact 接收、lock plan 解析、native binlog cache
构造和资源预留，但不创建 live `lock_t`。

Promotion epoch gate 发生在新主开放业务之前。它在 physical-layout lease 保护下 claim
prepared trx，导入 read view、table/record lock，验证第一版不含 predicate lock，再创建
detached MDL，并注册
`ADOPTED_FOR_PROMOTION` record。只有 required token 全部进入安全终态，且成功 token 的锁
已经安装，service-open barrier 才能放行。

Per-connection fast resume 发生在 service open 后。它不再 claim 事务或导入 record lock，
只把已经 adopted、已经持锁的事务，以及预构造 binlog/session 状态挂到目标 THD。

本设计基于最新讨论做出两个关键结论。

第一，record lock 在 InnoDB 内核中的物理表示是：

```text
{index, space_id, page_no, heap_no/bitmap, lock mode, trx}
```

在严格的物理复制 planned-promotion 合同下，如果 source 在最终物理 fence 上捕获 lock
metadata，receiver 已 apply 到相同物理状态并冻结 apply，那么 source 和 receiver 的 B+Tree
页面布局、page number 和 heap number 应保持一致。此时 promotion 快路径不应再次读取
InnoDB 数据页来证明同一件事，而应直接使用 final lock metadata 创建 `lock_t`。

因此，record-lock prewarm 的目标调整为：

```text
校验和解析 final lock metadata
-> 构建不可变 metadata-only import plan
-> 绑定 final physical fence / epoch fact / generation
```

它不再要求把相关数据页预取进 Buffer Pool，也不再以 `resident_pages == page_count` 作为
READY 条件。真正的 metadata-only import 在 promotion epoch gate 中完成，把 bitmap 直接
安装到 `lock_sys`，并要求 `page_get_count == 0`。不能把锁导入推迟到连接 resume，否则
service open 到旧连接恢复之间会出现 preserved lock 缺失的隔离窗口。

第二，transactional binlog cache 必须在 prewarm 阶段构造成目标 MySQL 的原生 cache：

```text
THD 尚未建立所有权时：token -> detached native binlog cache handle
THD resume 时：         detached handle -> THD ha_data
```

小 cache 使用原生 `IO_CACHE` 内存 buffer；超过 `binlog_cache_size` 后由目标 MySQL 原生
路径创建 `ML*` 临时文件。Token 与 cache 的对应关系由 prepared registry 保证，不依赖
文件名。Resume 只转移 native manager 的所有权，不读取、复制、重写或 rename binlog
payload。

### 1.1 与既有设计的关系

本设计只在 `STANDBY_PROMOTION_PHYSICAL_FENCE` strict path 上 supersede 旧的
`resident_pages == page_count` READY 条件：

| 路径 | Record-lock 策略 | 权威文档 |
|---|---|---|
| Local startup recovery | 现有 page-based import，可选 page prefetch | 既有 startup/reuse 设计 |
| Legacy promotion/no physical lease | 不允许 metadata-only；保持现有 fail-closed | 既有 promotion 设计 |
| Strict physical-fence promotion | final metadata plan + page-free import | 本设计 |

Supersede 边界还包括：

- `standby-promotion-1s-reuse-optimization` 中 gate 前 record page residency/prefetch，只对 strict
  physical-fence path 由 final metadata plan 取代；local startup 和 legacy promotion 保持原行为；
- `standby-streaming-transfer-prewarm` 继续负责 phase1 transport、object proof 和 receiver 实时
  prepare，但 strict READY 不再以 InnoDB 数据页 resident 为前提；
- `standby-promotion-completion` 中 gate 内 hydrate/cold import 继续禁止；其 copyable ready bundle
  仅服务 legacy path，不能成为 strict path 的第二权威；
- 现有 local startup page-based import、ordinary SQL `RESUME` 和 unsupported artifact 的
  fail-closed 语义不被本设计替换。

`standby-streaming-transfer-prewarm` 中的 receiver readiness `100ms`、本设计的 promotion gate
`1s` 和 per-connection resume `100ms` 是三个独立指标，不得相互替代。旧 cold-startup 或
startup-equivalent 数据只能作为 fallback/诊断证据，不能证明 metadata-only gate。

本设计不引入第二套 claim/import/register 语义。现有
`preserved_trx_recover_or_adopt_bundle_shared()` 增加第三种 policy：

```text
LOCAL_STARTUP_RECOVERY
STANDBY_PROMOTION_ADOPT
STANDBY_PROMOTION_PHYSICAL_FENCE
```

Promotion wrapper 仍不得直接 claim/import/register。第三 policy 只替换 shared kernel 内部的
record-lock apply strategy，其余 read view、table lock、MDL、register 和 cleanup 顺序继续
复用 shared kernel。

本设计的验证证据严格分为三类，不能相互替代：

```text
TEST_SAME_INSTANCE_ATTACH_ONLY  -> 只证明同一 InnoDB 实例中的 attach core
TEST_FROZEN_DATADIR_COPY        -> 证明 QUIESCED 后静态物理副本中的 gate/attach 等价性
PRODUCTION_REDO_APPLY_FENCE     -> 未来真实 HA/redo apply provider 才能签发
```

任意两个没有上述物理一致性证据的 mysqld 之间，不得执行 metadata-only lock import、事务
attach 或继续 DML；只能返回 `PHYSICAL_CONSISTENCY_NOT_PROVEN`。

## 2. 性能口径

### 2.1 Receiver READY

Receiver READY 表示 final artifact、record-lock plan、native binlog handle、XID/undo facts、
epoch fact 和资源预留已经就绪。它不表示锁已经安装，也不表示 mysqld 已升主。

### 2.2 Promotion epoch gate

```text
promotion_preserve_gate_elapsed_us =
  physical-layout lease acquired
  -> all required tokens reach a safe terminal state
  -> service-open barrier may release
```

Gate 计入 fact/fence 校验、durable intent、claim、read view/table/record lock、predicate
presence validation、MDL 导入、preserved record 注册，以及失败 token 的同步 rollback。任何
已 claim token 若不能证明 rollback 完成，整个 epoch 保持 service closed。

Planned promotion 的 Preserve/Resume gate 目标为 `<=1000000us`，且 page get 和 record image
resolve 均为 0。当前仓库只能用 release simulator 验证，不能写成真实 HA promotion 证据。

### 2.3 单连接 100ms 目标

正式目标定义为：目标 THD 已建立，服务端入口已解析出 token 字符串后立即记录
`resume_admission_start_us`，到该 THD 上 preserved transaction 可继续执行的 elapsed time。
Authorization、registry lookup 和 token/fact 校验均发生在计时内：

```text
promotion_resume_core_elapsed_us <= 100000
```

该指标使用 operation-local start/end timestamp 和 histogram。现有
`Preserve_trx_resume_total_us` 是 last-operation scalar，不是累计值或分布；只有埋点范围完全
一致时才可作为单次诊断值，不能据此推导 P95。正式证据必须新增明确命名的
`promotion_resume_core_elapsed_us` histogram。

`promotion_resume_core_*` 是 server status、PFS 和 E2E JSON 的唯一正式前缀；文档、测试和
脚本不得再输出无 `promotion_` 前缀的 `resume_core_*` 别名，避免同一计时区间出现双名。

当前仓库中的 `100ms` 结果只能作为同实例或冻结 datadir 副本上的 server-side attach-core
证据，不是在线物理备机升主后的业务续作证据。报告必须同时标明 consistency mode、
`real_redo_apply` 和 `real_ha_promotion`；只有未来 production provider 模式才能声明真实跨节点
事务继续。

计入：

- token、owner 和权限检查；
- atomic attach lease；
- native binlog cache handle attach；
- detached MDL backup 向 THD 克隆 transactional tickets；
- session、GTID、savepoint、temp ownership、last-insert-id 等状态恢复；
- 已 adopted trx attach/activate。

不计入：

- TCP/TLS 连接建立；
- 用户认证；
- proxy/HA 路由；
- SQL 命令网络传输；
- 真实物理升主本身；
- 物理 redo apply 等待；
- epoch gate 中的 claim 和 lock import。

### 2.4 预算分解和适用包络

Promotion gate 的 1s wall budget：

| 阶段 | P95/最大预算 |
|---|---:|
| Fact、lease、incarnation、digest compare | 40ms |
| Durable intent | 40ms |
| Claim + read view/table lock | 80ms |
| Dict/index lookup | 60ms |
| Metadata-only record-lock apply | 500ms |
| Detached MDL + register | 80ms |
| 失败终态与 Preserve barrier | 50ms |
| 未分配抖动余量 | 150ms |

Per-connection resume 的 100ms budget：

| 阶段 | P95/最大预算 |
|---|---:|
| Authorization、facts、attach lease | 8ms |
| Session facts + native binlog attach | 20ms |
| MDL clone + GTID | 20ms |
| Temp ownership + savepoints | 20ms |
| Trx attach/activate + commit_attach | 17ms |
| 未分配抖动余量 | 15ms |

上述 Preserve gate 预算不包含未来 HA 产品的 single-primary fencing 和 role commit。正式产品
RTO 必须分别报告 `preserve_gate_us`、`ha_role_fence_us` 和二者总和；当前仓库只能验收前者，
不得以 simulator 的空 role-fence 证明生产总 RTO。表内阶段预算是 P0 microbenchmark 前的
目标 envelope，不是已证明事实；任何阶段实测突破预算时必须重新冻结支持包络，不能挪用全部
抖动余量后仍声称 P99/max 有保证。

上述预算只适用于 planned fast path：所有 payload 已 prewarm、THD pristine、没有外部 MDL
等待、没有 gate 内文件 hydrate、没有资源临时申请、没有 unsupported temp/predicate artifact。
进入 `ACTIVATING` 前的所有 MDL/dict/resource 等等待都必须接收同一个 absolute deadline，超时
后停止新步骤并撤销 attach 侧已完成的可逆 staging；不能继续使用 `LONG_TIMEOUT`，也不能偷偷
转为 cold path。这里的撤销不是用户事务 `ROLLBACK`。用户事务 rollback 只属于 physical lease
仍有效时的 gate 失败、独立的过期清理，或 activation 已越过不可逆 native 边界后的受控失败
收敛；普通 activation 前 attach abort 不得结束用户事务。
`ACTIVATING` 会写 undo/redo 并可能等待 redo flush，是不可取消点。进入它之前必须预留明确的
activation budget；进入后超时只使本次 release SLO 失败，不能中断 mtr 或假装回到
`ADOPTED_LOCKED`。

`promotion_resume_core_p95/max` 的成功样本集合只包含满足上述 admission contract 且最终 `ACTIVE` 的
token；所有 rejected/timeout/tainted 样本必须按原因单独计数，不能从总样本中静默删除。
可取消阶段的 deadline miss 必须在 100ms 边界内停止新步骤并进入可证明的撤销状态。成功
样本的 P95/max 是 release 观测硬线，不等同于内核能强制取消 redo flush；所有失败、超时和
不可逆 tainted 样本必须另行计数，不能用“失败样本不计 SLO”掩盖长时间阻塞。

预算不是靠估算宣布通过。P0 必须先做 lock-plan apply、native binlog attach 和 token attach
intent 持久化 microbenchmark，得到每 bitmap page、每 set bit、每 manager attach 以及
`ATTACHING/ACTIVATING/ACTIVE` journal rewrite 的实际成本，再冻结 full-pressure worker
数量和支持包络。Release report 必须记录 CPU 型号/核数、RAM、filesystem、buffer pool、
tmpdir、worker 数、token/bitmap/binlog 分布和同时业务负载。

Lock apply 成本模型至少报告：

```text
T_apply = bitmap_entries * C_entry
        + imported_set_bits * C_bit
        + lock_sys_shard_wait_us
        + worker_schedule_and_merge_us
```

`lockset_batch_size` 不能直接当成 plan entry 数或独立 `lock_t` 数。P0 必须使用真实
`bitmap_entries/imported_set_bits/shard distribution` 测量 `C_entry/C_bit`；在测量前，“数 GiB”
或“至少数秒”都只能作为风险假设，不能写成已证实容量或耗时结论。

### 2.5 批量目标不能与单连接混淆

“每个连接 resume `<=100ms`”不能自动推出“1000 个连接全部 resume 的总 wall time
`<=100ms`”。两个批量阶段也必须分开：promotion gate batch 受 bitmap pages、lock_sys shard
和 claim 并发影响；connection resume batch 受 THD 调度、native handle attach、MDL clone
和 CPU 数量影响。

Release 报告必须同时输出：

```text
per_token_resume_p50_us
per_token_resume_p95_us
per_token_resume_max_us
batch_resume_wall_us
batch_token_count
gate_worker_count
resume_worker_count
```

本设计要求单连接 P95 和 max 都 `<=100ms`。1000-token batch wall time 是独立指标，需要
真实 release 压测后再确定产品硬线。

## 3. 非目标

- 不要求 cold startup lock-heavy recovery `<=100ms`。
- 不在 transfer 中传输完整 InnoDB 数据页。
- 不在 prewarm 阶段提前创建并发布 live `lock_t`。
- 不在 per-connection resume 中 claim prepared trx 或导入 record lock。
- 不根据 `ML*` 文件名或 `<token>.binlog_cache` 文件名寻找目标连接。
- 不在 resume 阶段 rename 大 binlog 文件。
- 不为 metadata-only lock import 保留页面副本或 Buffer Pool pin。
- 不让缺少 physical-fence 证明的 artifact 进入 page-free 快路径。
- 不实现真实物理备机和 HA fencing。
- 不把无 redo apply 的双 mysqld simulator 当作真实跨节点事务续作证据。
- 不为 detached/adopted trx 创建 placeholder THD；prepared/preserved trx 的锁和 undo 所有权
  继续由 `trx_t` 表达。
- 不让 receiver mysqld 重启后继续使用进程退出前的 prepared handle。
- 不实现新的 InnoDB 持久化 no-commit quarantine；rollback 失败时以阻断 epoch 保证安全。
- 不改变 `preserve_trx_enable=OFF` 时的原生 MySQL 8.0.22 行为。

## 4. 当前代码事实

### 4.1 当前 record-lock 表示

InnoDB 的普通 record lock 不是按主键值存储，而是按页面和页内 record bitmap 存储。
`lock_rec_t` 的核心字段是：

```cpp
page_id_t page_id;
uint32_t n_bits;
// lock bitmap 紧跟在 lock_t 后
```

源码位置：`storage/innobase/include/lock0priv.h`。

Preserve payload 当前已经包含：

```text
table_id
index_id
space_id
page_no
type_mode
n_bits
page_lsn
page_n_heap
heap_offsets
record_images
bitmap
```

源码位置：`storage/innobase/lock/lock0preserve.cc` 中
`Preserve_record_lock_entry` 及其 codec。

### 4.2 当前 import 为什么访问页面

当前 `lock_preserve_import_record_lock()` 会：

```text
buf_page_get(page_id)
-> 校验 page index id
-> resolve record identity
-> 检查冲突
-> lock_preserve_add_record_bitmap_for_import(block, ...)
```

`lock_preserve_add_record_bitmap_for_import()` 当前必须接收 `buf_block_t*`，并用：

```cpp
page_dir_get_n_heap(block->frame)
```

计算最终 lock bitmap 大小，再通过：

```cpp
RecLock(index, block, first_set_heap_no, type_mode)
```

创建锁。

因此当前访问页面是“现有 API、bitmap sizing 和防御性 identity 校验”的要求，不代表
`lock_sys` 理论上必须长期持有 resident page。最终 `lock_rec_t` 保存的是 `page_id` 和
bitmap，不保存 `buf_block_t*`。

当前 bitmap helper 存在一个必须先修的 Preserve 正确性 bug：`RecLock::create()` 只为 anchor
bit 调用 `lock_rec_set_nth_bit()`，而后续 `memcpy()` 不会为其他 set bits 增加
`trx->lock.n_rec_locks`。页面移动、split/merge/purge 和 lock cleanup 却按每个 set bit 递减，
因此旧 page-based import 可能触发 debug assert，release 下还可能发生计数下溢。该问题不能只在
新 metadata-only helper 中修复；现有 page-based helper 和新 helper 必须复用同一个
native-equivalent bitmap install/accounting 原语。

### 4.3 当前 stable-page payload 的限制

当前 phase1 stable-page warmcopy payload 可以省略 `record_images`，但源码注释明确指出：

- phase1 捕获后目标事务可能继续 DML；
- `page_lsn`、`page_n_heap` 可能继续变化；
- 当前 import 因而仍读取页面，确认 set bits 对应当前有效 heap slots。

结论：任意 phase1 provisional payload 不能直接用于 page-free import。Metadata-only 快路径
必须消费所有目标 QUIESCED 后生成或确认的 final generation，并绑定最终物理 fence。

### 4.4 当前 binlog prewarm

Receiver staging 文件为：

```text
<preserve_dir>/.transfer/<epoch_id>/<token>/binlog_cache.part
<preserve_dir>/.transfer/<epoch_id>/<token>/binlog_cache.ranges
```

Carrier projection 文件为：

```text
<preserve_dir>/<token>.binlog_cache
```

当前 `preserve_trx_transfer_load_standby_bundle_from_staging()` 会调用
`hydrate_external_blobs_from_staging()`，把 binlog cache external blob 整体读入
`std::string`。随后 `prewarm_loaded_bundle_into_ready_cache()` 把完整 bundle 放入
`Promotion_ready_cache_entry::ready_bundle`。Resume 时又通过
`mysql_binlog_preserve_import()` 把 payload 写入目标 THD 的新 `Binlog_cache_storage`。

当前数据流是：

```text
external file
-> std::string
-> ready_bundle
-> target THD Binlog_cache_storage
```

该路径功能正确，但内存峰值和 resume 成本随 binlog payload 大小增长。

### 4.5 当前 promotion 与 resume 已经部分分层

当前 shared recover/adopt kernel 在 promotion adopt 中已经执行 claim、semantic import，并
注册不可被 ordinary SQL `RESUME` 消费的 `ADOPTED_FOR_PROMOTION` record。内部 promotion
resume 再将该 record 挂到 pinned peer THD。本设计延续该方向，而不是把 lock import 搬回
单连接 resume。

### 4.6 当前 apply provider 证明力不足

当前 `Preserve_trx_promotion_apply_state` 只有 `apply_frozen` 和 `applied_lsn`。它不能证明
target boot incarnation、dictionary generation、final lock generation，也不能保证从最终
校验到全部 lock install 完成期间页面布局保持不变。因此现有 provider 不能直接授权
metadata-only import，未来 HA 接口必须升级为有生命周期的 physical-layout lease。

## 5. 物理一致性合同

### 5.1 Metadata-only import 的必要条件

Record-lock page-free 快路径必须依赖未来物理备机产品提供的强合同和 lease：

```text
source lineage == receiver lineage
source final capture 在 source physical-layout capture fence 下完成
source final lock metadata 对应 physical fence LSN = F
source 在 F 后不再产生可能改变相关页面布局的 redo
receiver 已 apply 到 F
receiver 在 F 冻结 apply
final epoch fact 与 lock object digest/generation 一致
```

不能只检查：

```text
receiver.applied_lsn >= source_prepare_lsn
```

如果 receiver apply 到了更晚状态，而更晚 redo 发生 B+Tree split、merge、purge 或 record
movement，原 heap number 可能不再适用。最简单可靠的 planned-promotion 合同是：

```text
source fence writes
-> 捕获 final metadata 和 fence F
-> receiver apply precisely through F
-> freeze
-> acquire lease
-> consume metadata bound to F and install all locks
-> complete single-primary fencing and receiver role transition while lease remains held
-> atomically commit service-open barrier
-> release lease only after redo apply cannot restart and the new-primary role is committed
```

未来 HA provider 若允许 `applied_lsn > F`，必须额外证明 page layout 与 F 对应 metadata
兼容，不能由 Preserve/Resume 自行猜测。

Lease 至少绑定：

```text
source lineage UUID
target server UUID and boot incarnation
apply provider generation
source final fence LSN
target frozen applied LSN
epoch fact digest
final lock generation digest
page layout digest
dictionary/index generation
page size/compression/encryption generation
lease id / owner generation
```

Physical-layout lease 是 holder-owned、不可撤销的冻结所有权，不是到期即失效的观测快照。
Provider 一旦成功返回 lease，必须保证 holder 调用 `release()` 前 apply、相关 DDL 和页面布局
不会恢复变化。Gate 的 operation deadline 与 lease 生命周期分离：deadline 到达只禁止开始新
步骤，并要求在仍有效的 lease 下撤销 import journal 和 rollback 已 claim/adopted trx。若 provider
无法提供这种不可撤销合同，则不得接入 metadata-only 路径。

接口必须 machine-checkable，例如：

```cpp
struct Preserve_trx_physical_fence_proof {
  std::string source_lineage_uuid;
  std::string target_server_uuid;
  std::string target_boot_incarnation;
  uint64_t provider_generation;
  uint64_t source_fence_lsn;
  uint64_t target_frozen_lsn;
  std::string epoch_fact_digest;
  std::string final_lock_generation_digest;
  std::string page_layout_digest;
  std::string dictionary_generation_digest;
};

enum class Preserve_trx_physical_consistency_mode {
  TEST_SAME_INSTANCE_ATTACH_ONLY,
  TEST_FROZEN_DATADIR_COPY,
  PRODUCTION_REDO_APPLY_FENCE
};

struct Preserve_trx_physical_fence_lease;

bool acquire_physical_fence_lease(epoch_fact, proof, &lease);
bool revalidate_physical_fence_lease(lease, expected_provider_generation);
void release_physical_fence_lease(lease);
```

Consistency mode 是 provider/test component 在进程初始化时注册的不可变能力，不是 per-call
选项，更不能来自 SQL、sysvar 或 request 字段。Production mode 只能由未来 production provider
slot 随 lease 返回；TEST mode 只能由 TEST_ONLY component 在进程启动/组件加载时安装，同一进程
不得从 TEST mode 动态升级为 production mode。调用者只能消费 lease，不能选择或伪造 mode。

硬谓词：

```text
required_apply_lsn != 0
source_fence_lsn != 0
target_frozen_lsn == source_fence_lsn
apply_frozen for the full lease lifetime
target boot incarnation and provider generation unchanged
epoch/final-lock/dictionary digests match
```

第一版 planned strict path 不接受 `target_frozen_lsn > source_fence_lsn` 的 layout compatibility
escape hatch。未来若物理复制产品需要接受更晚 LSN，必须另行冻结 machine-checkable proof
schema、允许的 redo/layout 变化集合和验证算法，不能在本接口中用布尔值或散文证明放宽。

`page_layout_digest` 不是页面内容副本，而是 final generation 中所有 page entry 的 canonical
commitment。它至少覆盖 `{space_id, page_no, page_index_id, final_page_lsn,
final_page_n_heap, page_size, compression/encryption generation}`，并被
`final_lock_generation_digest` 和 epoch fact 同时引用。Provider 只返回全局 LSN、却不能证明
该 digest 对应其冻结物理状态时，不得签发 metadata-only lease。

Digest 合同在 P0 固定为 SHA-256，并与 transport authentication 分离：SHA-256 证明对象内容和
generation identity；既有 HMAC/credential 只证明 transport authenticity。Canonical serialization
统一使用 versioned length-delimited binary format：整数为固定宽度 big-endian，字符串前置
`uint32` 字节长度，token/page/index entry 按协议 identity 和
`{space_id,page_no,index_id,type_mode}` 稳定排序，禁止依赖 C++ struct layout、locale 或 map
迭代偶然顺序。`object_digest` 由 source sealed-object producer 计算、receiver seal 时校验；
`page_layout_digest` 由 source final capture 计算、physical provider 对冻结布局签名确认；
`final_lock_generation_digest` 绑定有序 object/page digest 和 final generation；epoch fact 再绑定
token set、上述 digest、LSN 和 UUID。Prewarm 完成 canonicalization 和 digest 构建；gate 只能
比较固定长度 digest，禁止在 gate 内重新遍历所有 page entry。

上述三个 physical digest 在 lease proof 中是 **epoch 级 commitment**，不是要求每个 token
具有相同 digest。Receiver 对每个 token 保留独立的 lock/page/dictionary digest，再按
`{token,generation}` 稳定排序，以 domain-separated SHA-256 分别聚合为 epoch
`final_lock_generation_digest`、`page_layout_digest` 和
`dictionary_generation_digest`。Gate 可以在既有 token preflight 循环中聚合这些固定长度
token facts，但不得重新解析 lock payload 或遍历 page entry。重复 token、空 digest、顺序不稳定
或聚合结果与 lease proof 不一致均 fail closed。

缺 production provider、provider 返回 false、debug executor 为空或默认 executor 回落，都不
得让 metadata-only policy 可达。Debug simulator 只能由显式 test policy 安装，不能复用
production provider slot。

Gate 不能自行 release lease 后再让 HA 决定是否升主。它必须把 lease ownership 交还给同一个
HA role-transition coordinator，或由该 coordinator 从开始到 service-open commit 一直持有。
在 lease 有效且 operation deadline 到达时，整个 epoch 保持 service closed，并在仍冻结的物理
状态下 rollback 全部已 adopted token；任一 rollback 无法证明完成时不得开放业务。若 provider
违反不可撤销合同，导致物理状态是否仍一致不可证明，则直接将 epoch 标为 tainted 并阻断
service-open，不得在未知布局上猜测性 rollback。

Lease 必须保证从 gate 开始到 single-primary fencing、角色切换和 service-open commit 期间，
redo apply、用户写和 relevant DDL 不会使页面布局漂移。读取一次 `apply_frozen=true` 不是 lease。
角色切换完成后，新主自身的 B-tree 变化由原生 InnoDB lock update 机制维护，不要求 lease
覆盖后续 per-connection resume。

只让 preserved target transaction `QUIESCED` 仍不够：其他事务、purge 或 DDL 也可能使
同一 B-tree page split/merge/relocate。Source 必须在捕获第一条 final page metadata 前取得
source-side layout capture fence，并保持到 final generation/fact 发布；或者由物理复制产品
提供等价的 per-page generation attestation。否则不同页面可能来自不同物理时刻，不能授权
page-free import。

Source capture fence 必须覆盖所有能改变相关物理身份的 actor：inflight DDL、purge、change
buffer merge、page split/merge、tablespace/page reuse、压缩/加密 metadata 变化。Fence F
必须位于完整 mini-transaction/redo 边界，不能引用尚未完整发布的页面状态。

### 5.2 Prepared transaction continuity

物理页面一致不能证明 prepared trx 可续作。Ready facts 还必须绑定：

```text
token/XID/trx id
undo and rseg identity
prepare redo boundary
read-view generation
source/target UUID
target boot incarnation
```

Gate claim 前进行 read-only probe，claim 后复核 claimed trx 身份。Implicit lock 必须在
source final QUIESCED capture 中物化为 explicit metadata；无法物化的 token fail closed。

Implicit-to-explicit materialization 必须发生在 source layout capture fence 内，并与 final
bitmap/page generation 发布属于同一原子 capture interval：先冻结能改变相关 record/page
identity 的 actor，再完成 implicit owner 判定和 explicit metadata 物化，最后发布 final digest。
不能先物化 implicit lock、释放子 fence后再采 page generation，否则 final plan 仍可能跨两个
物理时刻。

Gate完成后、连接attach前，`ADOPTED_LOCKED` trx保持`mysql_thd == nullptr`。这是prepared/
preserved事务的既有detached ownership形态，不创建placeholder THD。Record lock由`lock_t -> trx_t`
拥有，preserved rseg/undo保护也按trx/XID状态判断；placeholder THD反而会引入虚假的session、
deadlock和status ownership。实现前必须用runtime测试证明detached trx的冲突锁、delete-mark/purge
lock inheritance和rseg保护正确；如果这些测试暴露真实null-THD缺口，只修对应诊断/内核分支，
不能用placeholder THD掩盖。

### 5.3 当前仓库能证明什么

当前仓库没有真实物理备机和生产 apply-state provider，因此只能证明：

- source/receiver transfer、durable spool 和 physical-copy E2E 已存在，但不等于真实 redo
  replication/apply 或 HA promotion；
- metadata-only plan 构建正确；
- 在 QUIESCED 后冻结 datadir 副本测试中，page-free lock import 与当前 page-based import
  产生等价 lock_sys 状态；
- gate 不发生 page IO，per-connection resume 不再执行 lock import；
- 缺少 provider/fence 时 fail closed。

其中测试模式的权限边界固定为：

- `TEST_SAME_INSTANCE_ATTACH_ONLY` 只能消费同一 InnoDB 实例中已经安全 adopted 的事务，不能
  授权跨实例 metadata-only import；
- `TEST_FROZEN_DATADIR_COPY` 必须在所有目标 QUIESCED、source 停止后复制完整 datadir，并将
  receiver 已接收的 standby carrier/spool 作为 overlay 恢复；它只能证明静态物理副本等价；
- `PRODUCTION_REDO_APPLY_FENCE` 只能由未来 production provider slot 生成，TEST_ONLY 插件、
  SQL、DBUG 和 simulator 均不能构造。

当前 `--receiver-physical-copy-before-drain` 在业务负载之前复制 datadir，之后没有 redo apply，
因此不能用于跨节点 attach/DML 证明。该模式只保留 transfer/prewarm 测试价值。

当前仓库不能声明真实物理升主合同已经完成。该边界必须写入 release 结论。

### 5.4 Source final generation 与 Phase2 边界

Phase1 持续发送 provisional record-lock/binlog objects、prewarm manifest 和 generation。
Receiver 可提前 parse/prepare，但 provisional generation 不能进入 gate-ready。

进入 `WARMCOPY_CLOSING` 后，先等待所有 final target `QUIESCED`，再执行一次有界 final
catch-up：

1. 在 source layout capture fence 下捕获 final lock bitmap、`final_page_n_heap` 和
   implicit-lock materialization 结果；
2. 捕获 final binlog tail/state；
3. 对 descriptor mismatch/missing object 发送 bounded tail；
4. Receiver ACK durable received，不等待 prewarm；
5. 发布 final generation、digest 和 epoch fact。

Receiver 不得逐字段原地发布 final facts。它先在 worker 私有对象中完整构造并校验 immutable
`Final_token_facts`，再以 release-store 一次发布 `{generation, facts pointer}`；prewarm/gate 以
acquire-load 读取同一 generation。Epoch bind 只能引用已经发布的 token facts digest，不能看到
“新 generation + 旧 digest”或反向组合。Stale worker completion 只能丢弃，不能覆盖新对象。

Phase2 不能无界追逐业务变化。Tail 必须有 per-token/per-epoch bytes、token count 和 deadline
上限。超过上限时 token 不得伪装 READY；未 claim token 保持 not-ready，已 claim token 必须
rollback，rollback 未证明成功则阻断 epoch。这样才能同时维护 final metadata 正确性和 source
phase2 `<=2s` 目标。

## 6. Prepared State、Registry 与 Durable Intent

### 6.1 Copyable facts 与 move-only resources

当前 `Promotion_ready_cache_entry` 会被复制，不适合持有 move-only 资源。设计区分可复制
事实和单一所有者资源，但二者必须由同一个 token entry 原子发布。

可复制 ready facts：

```text
state
required_apply_lsn / physical_fence_lsn
epoch_fact_digest
object_generation
record_lock_plan_ready
record_lock_metadata_only_eligible
record_lock_unique_pages/bitmap_entries/bits
binlog_cache_present
binlog_cache_prepared
binlog_cache_length
binlog_cache_memory_bytes
binlog_cache_file_backed
reason
XID/undo/rseg/prepare facts
target boot incarnation
resource reservation summary
semantic_validated
lock_plan_ready
binlog_handle_ready
resources_reserved
predicate_lock_present
has_read_view / savepoint_count
temp_artifact_mode
```

Move-only resources：

```text
unique Record_lock_metadata_plan
unique Mysql_binlog_preserve_prepared_cache_handle
dictionary/index lease if required
resource reservation lease
```

`IO_CACHE`、native manager、FD、cipher 和 mutex 不能通过对象字节 memcpy/move。Handle 必须
拥有固定地址 placement-new manager，并通过 binlog 私有 deleter 销毁；attach 只转移 owning
pointer。

### 6.2 Atomic token entry

```text
Prepared_token_entry {
  {preserve_dir, epoch_id, token, generation}
  copyable facts
  move-only resources
  lifecycle state
  adopt/attach lease owner
}
```

Registry 提供：

```cpp
Prepare_lease begin_prepare(const Prepared_token_key &key,
                            uint64_t expected_generation);
Publish_status publish_ready(Prepare_lease &&lease,
                             Final_token_facts facts,
                             Prepared_token_resources resources);
Gate_adopt_lease begin_gate_adopt(const Prepared_token_key &key,
                                  uint64_t expected_generation);
void commit_gate_adopt(Gate_adopt_lease &&lease,
                       Adopted_preserved_record record);
void abort_gate_adopt(Gate_adopt_lease &&lease,
                      Preserve_trx_terminal_outcome outcome);
Attach_lease begin_attach(const Prepared_token_key &key,
                          uint64_t expected_generation,
                          Protected_thd_handle &thd);
bool begin_activation(Attach_lease &lease);
void commit_attach(Attach_lease &&lease);
void abort_attach_after_full_unwind(Attach_lease &&lease);
void taint_attach(Attach_lease &&lease, Attach_taint_reason reason);
Cleanup_lease begin_cleanup(const Prepared_token_key &key,
                            Prepared_token_state expected_state);
void invalidate_incarnation(const std::string &boot_id);
```

上述 lease 均 move-only、析构默认 fail closed；只有显式 `commit/abort/taint` 才能结束 ownership。
Public header 只暴露 opaque lease 和必要状态，不暴露 native binlog manager、`trx_t*` 或可伪造
physical mode 的构造器。

不能先从 registry 永久移除资源，再尝试 claim 或 attach。`begin_*` 返回 RAII lease；成功
commit，失败恢复所有权或进入可观测终态。相同 key+digest 幂等复用，digest 冲突标记
corrupt，stale generation completion 丢弃，boot incarnation 变化使全部内存 handle 失效。

Map mutex 只定位 entry；状态领取使用 per-entry mutex 或 acquire-release CAS：

```text
READY_FOR_GATE -> ADOPTING
ADOPTED_LOCKED -> ATTACHING
ATTACHING -> ACTIVATING
```

CAS 失败方不接触 move-only resource，返回 deterministic `INVALID_STATE/ALREADY_CLAIMED` token
status；SQL 映射为 `ER_PRESERVE_TRX_INVALID_STATE`。Entry lock 不跨 file IO、lock import、MDL
transfer 或 THD attach 持有。

Gate commit 后可以释放已消费的 lock plan，但必须把 native binlog handle 和 adopted record
留在同一 token entry 中，状态改为 `ADOPTED_LOCKED`。Gate 失败并 rollback 的 token 必须销毁
未消费 binlog handle 和全部 resource reservation。

`abort_attach_after_full_unwind()` 只能在 attach journal 的所有已置位步骤都已逆序撤销后调用；
它以 release-store 发布回 `ADOPTED_LOCKED`。在此之前状态保持 `ATTACHING`，cleanup 无资格领取。
一旦重新发布成功，后续 attach 或 cleanup 可重新竞争同一 CAS，但不会与旧 staging 并发。

### 6.3 Durable intent

Claim 前必须已有 durable `ADOPTING` intent。Intent 状态至少为：

```text
CANDIDATE -> ADOPTING -> ADOPTED_LOCKED
ADOPTED_LOCKED -> ATTACHING -> ACTIVATING -> ACTIVE
ACTIVE -> ACTIVE_ARTIFACTS_CLEANED
ADOPTED_LOCKED -> CLEANUP_PENDING -> CLEANUP_ROLLED_BACK
ABANDONED_ROLLED_BACK
ABANDONED_NOT_FOUND_PROVEN
CLEANUP_TAINTED
ATTACH_TAINTED
ATTACH_ROLLED_BACK
```

Gate intent 的 durable 粒度固定为 epoch 级：一个 epoch intent 文件包含 final fact 的完整 token
set 和每个 token 的逻辑状态。Gate 开始前一次 atomic write 将全部 candidate 纳入 `ADOPTING`；
worker 完成后一次 final rewrite 写入各 token 终态。Gate 不为 1000 个 token 分别 fsync，也不在
每个 worker 完成时重写整个 epoch。进程崩溃停在 `ADOPTING` 时整个 epoch fail closed，这正是
本设计选择的恢复语义。

Service-open 后的 `ATTACHING/ACTIVATING/ACTIVE` 是 token 级 intent，因为每次只消费一个目标
连接；它与 gate epoch intent 分文件或分 journal 管理，不能为更新一个 attach token 重写整个
1000-token gate intent。`CLEANUP_PENDING`、`CLEANUP_ROLLED_BACK`、`ATTACH_ROLLED_BACK` 和
`ATTACH_TAINTED` 也写入同族 token journal，不能只保存在进程内 registry。

非 happy-path 转换必须完整列举，不能让实现者临时发明状态跳转：

```text
OBJECTS_RECEIVING | PREWARMING -> CORRUPT | RESOURCE_EXHAUSTED | STALE_GENERATION
PREWARMED_PENDING_FINAL_FACT -> NOT_READY | CORRUPT | RESOURCE_EXHAUSTED |
                                  READY_FACTS_PENDING_LEASE
READY_FACTS_PENDING_LEASE -> READY_FOR_GATE | NOT_READY |
                             PHYSICAL_FENCE_MISMATCH | STALE_GENERATION
READY_FOR_GATE -> NOT_READY | PHYSICAL_FENCE_MISMATCH | ADOPTING
ADOPTING -> ADOPTED_LOCKED | ABANDONED_ROLLED_BACK |
            ABANDONED_NOT_FOUND_PROVEN | CLEANUP_TAINTED
ADOPTED_LOCKED -> ATTACHING | CLEANUP_PENDING
CLEANUP_PENDING -> CLEANUP_ROLLED_BACK | CLEANUP_TAINTED
ATTACHING -> ADOPTED_LOCKED (fully reversible abort)
ATTACHING -> ATTACH_TAINTED (ownership cannot be proven)
ATTACHING -> ACTIVATING (irreversible boundary)
ACTIVATING -> ACTIVE | ATTACH_ROLLED_BACK | ATTACH_TAINTED
ATTACH_TAINTED -> CLEANUP_PENDING (only when cleanup_capability=ROLLBACK_ALLOWED)
CLEANUP_PENDING -> CLEANUP_ROLLED_BACK | CLEANUP_TAINTED
any in-memory prepared state -> STALE_GENERATION (boot incarnation changed)
```

Intent、registry 和 preserved record 的转换顺序必须固定，不能产生“intent 已 adopted 但
锁未安装”或“resource 已消费但 THD 未 attach”的窗口。

本设计不支持 strict online promotion 在 receiver mysqld 重启后自动继续。启动时发现未终结的
`ADOPTING/ATTACHING/ACTIVATING` intent，或发现 intent 的 boot incarnation 与当前进程不同，
必须阻断该 epoch 和 service-open；普通 local startup recovery 不得消费这些 standby token。
HA 只能丢弃并重建该 standby，或显式进入 destructive abandon/rollback 流程。不得仅凭 intent
猜测 claim、lock import 或 activation 已完成，也不得自动重建 native handle 后继续 promotion。

Gate rollback 成功的 durable proof 固定为：native rollback 返回成功、prepared XID lookup
不存在、active owner 不存在，并将 epoch intent 原子 rewrite 为 `ABANDONED_ROLLED_BACK`。只满足
其中一项不能放行；无法完成全部证明时写 `CLEANUP_TAINTED` 并阻断 service-open。

### 6.4 Per-epoch readiness accounting

每个 token READY 只做 O(1) mark。Expected token set 以 final epoch fact 为唯一权威；phase1
declare count 只用于提前调度。只在 final fact 到达或最后一个 fact token READY 时执行一次
epoch bind，不能每完成一个 token 就扫描全部 token，也不能每次重读 epoch fact 文件。

### 6.5 READY 单一权威与现有 enum 映射

Strict physical path 以 `Prepared_token_entry` 为唯一事实源。现有
`Preserve_trx_promotion_ready_state` API 保留为只读 projection，不再拥有一份可独立变化的
strict-path state 或完整 `ready_bundle`：

| 现有 ready state | Strict registry state | 含义 |
|---|---|---|
| `NOT_FOUND` | no entry | 未接收或已 purge |
| `RECEIVED_DURABLE` | `OBJECTS_RECEIVING` | 至少一个 durable object |
| `HYDRATING` | `PREWARMING` | bounded parse/prepare 进行中 |
| `DRY_VALIDATED` | `PREWARMING` 且 `semantic_validated && lock_plan_ready && binlog_handle_ready` | 全部语义对象完成，资源/final fact 未齐 |
| `PREWARMED_PENDING_FINAL_FACT` | 同名状态 | plan/handle 完成，缺 final fact |
| `APPLY_PENDING` | `READY_FACTS_PENDING_LEASE` | final fact 到达，apply/freeze 条件未满足 |
| `APPLY_REACHED` | `READY_FACTS_PENDING_LEASE` | LSN 已到达，但 holder-owned lease 尚未成功签发 |
| `READY` | `READY_FOR_GATE` | lease 已签发并 revalidate，可被 strict gate 原子领取 |
| `CORRUPT` | `CORRUPT` | terminal until epoch purge |

Projection 不维护独立可变 cache。每次读取都从同一个 registry entry 的 acquire snapshot
计算；状态转换的 release-publish 是唯一刷新点，因此不存在“registry 已变、projection 未刷”
窗口。Gate 只调用 registry 的
`begin_gate_adopt(expected_generation)`，不得先读取 copyable READY 再从另一容器领取资源。
Legacy/no-fence policy 可保留旧 ready cache，但其 key space、入口和 executor 与 strict path
隔离，不能把 legacy `READY` 当作 metadata-only READY。

反向 projection 也必须确定：strict `PREWARMING` 在任一 lock/binlog semantic object 未完成时
投影为 `HYDRATING`；只有 `semantic_validated`、`lock_plan_ready` 和
`binlog_handle_ready` 全部为真，但资源/final fact 未齐时才投影为 `DRY_VALIDATED`。这些 progress
bits 属于同一个 registry entry 的 copyable facts，不是第二套状态机；其他 strict state 按上表
唯一投影。

### 6.6 Token terminal record、reaper 和 epoch purge

已消费身份使用 `{source_uuid, epoch_id, token, generation}`，不能使用进程全局 flat-token
tombstone。`ACTIVE/ACTIVE_ARTIFACTS_CLEANED/ATTACH_ROLLED_BACK/ATTACH_TAINTED/
ABANDONED/CLEANUP_TAINTED` terminal record 至少保留到 epoch purge，拒绝
同一 identity 再次 publish/attach。

`{preserve_dir, epoch_id, token, generation}` 只是本机 registry/carrier locator；
`{source_uuid, epoch_id, token, generation}` 才是跨实例协议 identity。Entry 必须同时保存二者，
不得用目录路径参与跨节点身份比较，也不得用 source UUID 拼接未经校验的本地路径。

Registry 必须提供：

```text
begin_cleanup(key, expected_state)
purge_epoch(epoch_id, terminal_policy)
expire_pending_final_fact(epoch_id, deadline)
expire_adopted_locked(epoch_id, resume_deadline)
```

`purge_epoch()` 统一销毁 plan、native handle、FD、dictionary/resource lease 和 ready
projection。Final fact 超时的 token 进入 not-ready/abandoned cleanup，不能永久占用资源。

`READY_FACTS_PENDING_LEASE` 和 `PREWARMED_PENDING_FINAL_FACT` 都必须有 epoch deadline；过期只
释放尚未 claim 的 plan/handle/resource lease并进入 NOT_READY，不得创建 durable rollback
状态。Terminal registry record 是当前进程内 tombstone，不跨 receiver restart 自动 replay；
跨重启安全只依赖 durable intent 的阻断语义。

当前 expired reaper 只选择 `resumable=true` record，promotion-owned record 为
`resumable=false`，所以现有路径没有 reviewer 所描述的直接竞争。未来 registry reaper 仍
必须通过 `begin_cleanup()` 与 `begin_attach()` 使用同一 token state CAS，禁止绕过 registry
直接 rollback `ADOPTED_LOCKED/ATTACHING/ACTIVATING` trx；`ACTIVATING` 尤其不可被 cleanup
线程当作可回滚状态。

`ADOPTED_LOCKED` 必须携带独立的 client-resume deadline，不能永久持锁。Deadline 到达后，
registry reaper 通过 `begin_cleanup(ADOPTED_LOCKED)` 与 `begin_attach()` 做同一 CAS：成功取得
cleanup lease 后进入 `CLEANUP_PENDING`，在物理一致的新主上 rollback 用户事务；成功进入
`CLEANUP_ROLLED_BACK`，失败进入 `CLEANUP_TAINTED` 并持续告警。该清理不属于 P5 attach 失败
回退；同实例/冻结副本测试可以验证状态机，任意异构 receiver 不得执行该 rollback。

Client-resume deadline 由未来 HA coordinator 在 gate request 中提供并冻结到 epoch/token facts；
TEST_ONLY profile 使用测试配置常量。它不是用户动态 sysvar，也不能由目标连接延长。生产 provider
未提供 deadline 时 strict gate fail closed。

`ATTACH_TAINTED` 必须记录 `cleanup_capability=ROLLBACK_ALLOWED|INSTANCE_REBUILD_ONLY`。只有 trx、
binlog、MDL 和 THD ownership journal 仍能证明且 controlled rollback 可安全调用时，operator
才能通过 `begin_tainted_cleanup()` CAS 进入 `CLEANUP_PENDING`；成功为
`CLEANUP_ROLLED_BACK`，失败为 `CLEANUP_TAINTED`。Ownership 不可证明时固定为
`INSTANCE_REBUILD_ONLY`，不提供进程内 rollback/kill 出口，只能阻断并重建实例。

## 7. Record-Lock Metadata Prewarm 设计

### 7.1 共享 parse/apply 拆分

从现有 record-lock import 中提取共享操作：

```text
parse_record_lock_payload(payload, digest)
  -> immutable Record_lock_metadata_plan

validate_metadata_plan_for_physical_fence(plan, fact, provider)
  -> METADATA_ONLY_READY / NOT_READY

apply_record_lock_metadata_plan(claimed_trx, plan, deadline)
  -> existing lock_sys ownership
```

Local startup 保持当前 page-based 行为：

```text
parse -> page identity validation/resolve -> apply
```

Promotion strict physical path 使用：

```text
parse -> physical-fence validation -> metadata-only apply
```

两个 wrapper 必须共用 payload codec、类型校验、bitmap 校验、table/index 查找、冲突检查、
bitmap install/accounting 和失败清理，不维护第二套锁语义。P2 开始前先修复现有
`lock_preserve_add_record_bitmap_for_import()` 的多 bit accounting；“local startup 保持
page-based”只表示仍读取页面和校验 identity，不表示保留错误计数行为。

这里的 apply 发生在 promotion epoch gate，而不是 per-connection resume。

### 7.2 Metadata plan 内容

Plan 按 page/index 分组，包含：

```text
table_id
index_id
space_id
page_no
page_index_id
type_mode
source_n_bits
final_page_n_heap / final_native_n_bits
final_page_lsn
page_size / compression / encryption generation
bitmap
first_set_heap_no
max_set_heap_no
set_bits
heap_offsets_digest（只用于 source final capture 的结构承诺，gate 不读页复算）
object_digest
generation
physical_fence_lsn
schema/index generation
implicit_lock_materialized
is_final_quiesced
artifact protocol/server version
```

Plan 不保存：

- InnoDB page 内容；
- `buf_block_t*`；
- 长期 `dict_table_t*` / `dict_index_t*`；
- `trx_t*`；
- live `lock_t*`；
- Buffer Pool lease/pin。

Plan 可以持有 `Validated_dict_index_lease` 这类 opaque、受 physical/dictionary generation lease
约束的引用，但不得持有无生命周期证明的裸 `dict_table_t*`/`dict_index_t*`。Gate helper 从该
lease 短期取得 native table/index 指针；generation 变化、drop/truncate 或 lease 失效必须在
安装任何 lock 前拒绝。`heap_offsets_digest` 只做 canonical digest compare，不允许 gate 回退
读取 page record offsets 验证。

### 7.3 Final bitmap sizing

当前 import 读取页面主要是为了按当前 `n_heap` 计算 lock bitmap size。Page-free import 必须
从 final metadata 得到等价信息。

Final quiesced capture 必须提供权威 `final_page_n_heap`。最终 native bitmap bytes 按原生
公式计算：

```text
1 + ((final_page_n_heap + LOCK_PAGE_BITMAP_MARGIN) / 8)
```

并执行边界校验：

- 所有 set bit 必须小于 `final_native_n_bits`；
- serialized bitmap 超出 final size 的尾部只能包含 0 bit；
- 允许裁剪纯零尾部，不要求 final bytes 大于等于 serialized bytes；
- `first_set_heap_no` 和 `max_set_heap_no` 在 final 范围内；
- 不超过基于 InnoDB page size 推导的安全上限；
- predicate lock 不使用普通 record bitmap 规则；
- malformed/overflow 一律 fail closed。

Supremum ordinary record lock 仍按 `final_page_n_heap` 使用上述普通 record-lock bitmap sizing，
不存在 1-byte/8-bit 特例。它只执行原生 mode normalization：拒绝 `LOCK_REC_NOT_GAP`，并剥离
`LOCK_GAP/LOCK_REC_NOT_GAP`。1-byte sizing 只属于 predicate/page-predicate 结构；第一版整
token 拒绝该类 artifact，不能把 predicate sizing 套到 ordinary supremum lock。

如果 final metadata 不能证明 `final_page_n_heap`，该 token 不得进入 metadata-only 快路径。

### 7.4 Preserve 专用 page-free lock 安装 API

新增严格隔离的 Preserve-only helper。它不接受可相互矛盾的 loose arguments，而只接受
shared kernel 已验证的 entry：

```cpp
struct lock_preserve_validated_metadata_entry_t;
struct lock_preserve_import_journal_t;

enum class lock_preserve_metadata_conflict_result {
  OK,
  CONFLICT,
  UNSUPPORTED_MODE,
  CORRUPT_METADATA,
  DEADLINE_EXCEEDED
};

lock_preserve_metadata_conflict_result
lock_preserve_check_record_bitmap_conflicts_from_metadata(
    trx_t *trx,
    const lock_preserve_validated_metadata_entry_t &entry,
    uint64_t operation_deadline_us);

dberr_t lock_preserve_add_record_bitmap_from_physical_metadata(
    trx_t *trx,
    const lock_preserve_validated_metadata_entry_t &entry,
    lock_preserve_import_journal_t *journal);
```

`entry` 必须包含 normalized type mode、`final_page_n_heap`、`final_native_n_bits`、normalized
bitmap、page/index/schema generation、opaque validated dict/index lease 和已经验证的 physical
lease generation。Helper 入口再次断言 policy、lease、dict lease 和 entry generation 一致。

内部执行：

```text
按 page_id 获取 lock_sys shard latch
-> 用从原生 lock wait 判定抽出的 page-free predicate 检查显式锁冲突
-> 根据 metadata 分配 lock_t + bitmap
-> 设置 lock_rec.page_id / n_bits
-> 为每个 set bit 执行 native-equivalent accounting
-> 插入 lock_sys rec_hash
-> 挂入 trx lock list
-> 把本次创建的 lock_t 追加到 import journal
```

冲突检查不能重新实现一套 Preserve compatibility matrix。必须复用/抽取原生
`lock_has_to_wait()`/record queue 判定所使用的 type-mode 语义，只把 `buf_block_t*` identity
替换为已经验证的 `{page_id,index,heap bitmap}`：覆盖 ordinary record、gap、next-key、
`LOCK_REC_NOT_GAP`、insert-intention 和 supremum normalization。Strict gate 恢复的是 source
fence 时已经 granted 的锁，因此任何会形成 waiting lock 的结果都视为 artifact/fence 冲突并
fail closed；helper 不创建 waiting queue entry。Predicate/spatial lock 第一版仍在 helper 入口
显式拒绝。

Conflict helper 在对应 page shard latch 下只扫描 `entry.page_id` 的 rec-hash queue；对每个与
normalized bitmap 至少一个 heap bit 重叠的 granted lock，调用抽出的原生 wait predicate。
`CONFLICT` 不携带或插入 waiting `lock_t`；`UNSUPPORTED_MODE/CORRUPT_METADATA` 分别用于 mode
不在第一版支持面和 metadata 自相矛盾；deadline 只在开始下一 entry/queue scan 前检查。Apply
helper 只有在 conflict result 为 `OK` 时才允许分配并发布 lock。

Plan 在构建时按 `lock_sys shard -> page_id -> index` 排序，单 token 内连续处理同 shard/page 的
entry，以减少重复 latch 获取；是否能跨 entry 持有 shard latch 必须由抽出的 native primitive
合同决定，不能为了批量化绕过原生冲突检查。跨 token 不做一把大锁下的全局合并；gate worker
数量由现有并发参数限制，并以 `promotion_lock_shard_wait_us` 决定是否需要进一步调度。

该 helper 必须完整维护：

```text
trx->lock.n_rec_locks == imported set-bit count
table->n_rec_locks == imported lock_t count
trx lock list and trx_locks_version
lock monitor counters
PFS data-lock identity
rec hash/list ownership
```

不能只创建一个 anchor bit 后 `memcpy()` 其余 bitmap。实现应从现有 `RecLock::create()` 中
抽取并复用 lock allocation、`lock_add()`、rec-hash、trx-list、PFS 和 monitor 原语；唯一跳过
的是 `buf_block_t`/page identity 读取。不得复制一套近似 lock_add。

当前 `lock_preserve_add_record_bitmap_for_import()` 的 anchor-bit `create()` 后直接 `memcpy()`
整张 bitmap，会漏记其余 set bit 的 `trx->lock.n_rec_locks`。该缺陷必须作为独立 RED test 先
修复；page-based 与 metadata-only helper 随后共同复用修复后的 native-equivalent bitmap
publish/remove 原语。不得让新 helper 复制当前错误行为。

一个 token 的 apply 开始时创建空 `lock_preserve_import_journal_t`。每个 lock 完整插入后才
记录指针；第 N 个 entry 失败时，持有相同 native latch 顺序逆序 discard journal 中本次创建
的 locks，恢复 per-bit/per-lock accounting。Journal 不扫描或删除 claimed trx 在本次 apply
前已经拥有的 lock。Unwind 失败必须立即 rollback claimed trx；rollback 未证明成功时标记
`CLEANUP_TAINTED` 并阻断整个 epoch，不能继续 register 或 service-open。

该 helper 不接收 `buf_block_t*`，不调用：

```text
buf_page_get
buf_read_page
buf_read_page_background
page_find_rec_with_heap_no
record image resolver
```

Gate 不采用“尽量从 Buffer Pool 读取 page header 再验证”的折中方案：page 不 resident 时它会
退化为 cold IO，而且单页 header 不能证明整个 epoch 的布局合同。Provider 无法证明
page/index identity 时直接 not-ready，不用 page read 掩盖合同缺失。

它只在 shared recover/adopt kernel 的 `STANDBY_PROMOTION_PHYSICAL_FENCE` 分支内可见，
不导出给 promotion wrapper。普通 DML、local startup、legacy promotion、默认/null executor
和 `preserve_trx_enable=OFF` 路径不可进入。

### 7.5 Dict/index metadata

不读取数据页不等于零 cold IO。`dd_table_open_on_id()`、dict/index lookup 仍可能阻塞。
Receiver prewarm 必须验证 table/index/schema generation，并准备 bounded dictionary lease
或等价可验证 cache entry。Gate 单独统计 dict lookup/open 时间，不得把它隐藏在 lock apply。
Prewarm lease 只用于准备和资源 admission，不能直接授权 import；gate 取得 physical-layout lease
后、claim和第一条 lock install 前必须按 physical provider generation、dictionary digest和当前
schema/index generation 强制 revalidate。Revalidate 失败按 §11.1 判定表收敛。
Table/index 必须存在、非 temporary、space/page size 匹配且未经历 drop/recreate。Table drop、
truncate、page free/reuse 或 schema generation 改变使整个 token not-ready，不能只跳过单页。
Online DDL/rebuild 中的 index、临时 index identity或无法取得稳定 dictionary generation lease 的
index 第一版整 token拒绝；不能仅凭 `index_id` 相同推断 native RecLock 不变量仍成立。

### 7.6 第一版支持面

Metadata-only 第一版支持：

- ordinary record lock；
- gap lock；
- next-key lock；
- insert intention；
- final stable page bitmap payload；
- 最终 quiesced generation；
- 已物化为 explicit metadata 的 implicit lock；
- 有完整 physical-fence 证明的本地表空间。

第一版不支持：

- 需要 `record_images` 重新定位记录的 artifact；
- spatial predicate lock；
- temp-table 跨机 physical identity 未闭合的锁；
- phase1 provisional generation；
- implicit lock 未 materialize 的 artifact；
- final metadata 含 `LOCK_WAIT`、waiting queue ownership、未完成 conversion 或 source wait
  dependency 的 artifact；
- applied LSN 或 lineage 不完整的 token；
- final page sizing 事实缺失的 token。

这些 token 返回 `READY_CACHE_NOT_READY` 或 `UNSUPPORTED_ARTIFACT`，不能在 promotion
gate 中回退为 page cold import。Receiver/source 不得 strip `LOCK_WAIT` 或 waiting state 后把
剩余 mode伪装成 granted lock；predicate-empty 和 wait-free preflight 必须在 claim/import 前完成。

### 7.7 页面后续变化

锁安装完成后，未来新主上的 B+Tree split、merge、record movement 仍由原生 InnoDB
lock update 机制维护。Metadata-only helper 只负责创建与物理 fence 状态一致的初始
`lock_t`，不修改后续原生页面操作和锁继承逻辑。

### 7.8 Warmcopy hot-path 隔离

`lock_rec_set_nth_bit()` 等原生 hot hook 必须先经过两个 O(1) 门：

```text
lock_warmcopy_epoch != 0
AND trx is an active target of this warmcopy epoch
```

非目标事务不得取得 record-store partition mutex，不得访问 `store_by_target[]`，不得创建
target map entry。Active-target membership 第一版使用 `trx_t` 上的 epoch-scoped atomic
generation：epoch coordinator 只在 target admission/removal 时更新 `warmcopy_target_epoch`，
hot hook 先读全局 active epoch，再以 acquire-load 比较 trx generation。只有两者相等才进入
既有 warmcopy helper；不能在每次 set/reset bit 时扫描 THD 列表、查 map、取 mutex或分配。
该字段只承载 Preserve membership，不改变原生 lock ownership。OFF、epoch 从未打开、epoch
已关闭和非 target 四类路径都必须做 source-shape 与 release throughput NFR；实现前先核对
现有 `trx_t` warmcopy 字段并优先复用，禁止平行增加第二套 membership。

## 8. Binlog Cache Native Prewarm 设计

### 8.1 目标

每个有 transactional binlog cache 的 token 对应一个 prepared native cache handle。

Prewarm 后：

```text
token
  -> Mysql_binlog_preserve_prepared_cache_handle
       -> native binlog_cache_mngr
       -> transactional Binlog_cache_storage
       -> IO_CACHE memory or native ML* file
```

Resume 时只转移 manager 所有权，不再处理 payload。

### 8.2 Opaque handle

在 `sql/binlog.cc` 内定义私有实现，对外只暴露 opaque Preserve API：

```cpp
class Mysql_binlog_preserve_prepared_cache_handle final {
 public:
  ~Mysql_binlog_preserve_prepared_cache_handle();  // out-of-line in binlog.cc
  Mysql_binlog_preserve_prepared_cache_handle(
      const Mysql_binlog_preserve_prepared_cache_handle &) = delete;
  Mysql_binlog_preserve_prepared_cache_handle &operator=(
      const Mysql_binlog_preserve_prepared_cache_handle &) = delete;

 private:
  struct Impl;
  Impl *m_impl{nullptr};
  // Only the private prepare/attach/destroy functions may construct or detach.
};
class Mysql_binlog_preserve_payload_reader;
struct Mysql_binlog_preserve_cache_facts;
struct Mysql_binlog_preserve_attach_journal;

enum class Mysql_binlog_preserve_cache_status;

Mysql_binlog_preserve_cache_status prepare_detached_cache(
    const Preserve_trx_internal_operation_capability &capability,
    const Mysql_binlog_preserve_cache_facts &facts,
    Mysql_binlog_preserve_payload_reader *reader,
    Preserve_memory_lease memory_lease,
    std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle> *out);

Mysql_binlog_preserve_cache_status attach_detached_cache(
    const Preserve_trx_internal_operation_capability &capability,
    THD *protected_thd,
    std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle> *inout,
    Mysql_binlog_preserve_attach_journal *journal);

void destroy_detached_cache(
    std::unique_ptr<Mysql_binlog_preserve_prepared_cache_handle> handle);
```

`prepare_detached_cache()` 只有在完整 seal 后才设置 `out`。`attach_detached_cache()` 成功时
消费并清空 `inout`；任何可重试失败都必须保留 handle 所有权并通过 journal 撤销 THD 侧状态；
若 native 状态已不可逆，则销毁 handle、标记 attach tainted，不能让调用者猜测所有权。

公开 header 只暴露固定大小 RAII 壳和 out-of-line destructor，不暴露 manager 定义。`Impl`
在 `sql/binlog.cc` 内用与当前 `THD::binlog_setup_trx_data()` 相同的 `my_malloc + placement new`
构造 manager；handle 析构时调用私有 destructor + `my_free`。Attach 成功后 `Impl` 中的 manager
指针转交 THD `ha_data` 并清空 handle，后续唯一销毁者是原生 `binlog_close_connection()`。

Transfer/promotion 模块不能直接访问私有 `binlog_cache_mngr`。

两个入口都必须自门闩，不能只信任 caller：`preserve_trx_enable=OFF`、capability 不是 strict
prewarm/attach、boot incarnation/generation 不匹配时立即拒绝且不分配 manager/FD。对于需要
logged transactional cache 的 token，prepare 和 attach 都要求
`opt_bin_log && mysql_bin_log.is_open()`，attach 还必须匹配 session `OPTION_BIN_LOG`、
`sql_log_bin`、binlog incarnation 和 key generation。Capability 只能由 strict registry/core
创建，用户 SQL、transfer frame和 TEST_ONLY component 都不能直接构造。

Binlog handle ownership 矩阵固定为：

| 阶段 | 唯一 owner | 允许操作 | 失败收敛 |
|---|---|---|---|
| prepare construction | prepare worker local `unique_ptr` | stream write/seal | local destroy，释放 FD/memory |
| READY/ADOPTED_LOCKED | strict registry entry | validate/purge/begin_attach | registry destroy 或移交 attach lease |
| ATTACHING pre-boundary | move-only attach lease | 暂挂 THD、journal unwind | 完整 unwind 后归还 registry |
| ACTIVATING/ACTIVE | THD `ha_data` | native DML/savepoint/commit/rollback | 受控 rollback或原生 close |
| ATTACH_TAINTED | protected THD + taint cleanup lease | 仅审计/受控 cleanup | rolled back 或 instance rebuild |

同一时刻只能有一行 owner。Registry、attach lease和 THD 不能同时各保留一份 owning pointer；
状态 projection 只保存 facts，不保存 manager ownership。

`IO_CACHE` 和 manager 含内部指针、FD、cipher 和 mutex，不能通过对象字节 memcpy 或 C++
默认 move。Opaque handle 必须拥有固定地址 placement-new manager；attach 只转移 owning
pointer，不能搬动 manager 本体。

Payload 必须通过现有 stream abstraction 或等价 bounded reader 输入，不能要求完整
`std::string`，也不能把 Preserve carrier path 暴露给原生 binlog API。

### 8.3 Prewarm 工作

Prewarm 完成：

1. 校验 cache length、digest、event counter 和最大尺寸；
2. 使用目标端当前 `binlog_cache_size` / `max_binlog_cache_size`；
3. 初始化 native `binlog_cache_mngr`；
4. 通过 native `Binlog_cache_storage::write()` 将 sealed payload 流式写入 transactional
   cache，禁止 raw write 或直接拼接 `ML*` 文件；
5. 恢复 cache flags、previous position 和 cache-state map；
6. 校验最终长度和 digest；
7. seal handle；
8. 原子发布到 prepared registry。

Compression session facts 属于目标 THD，不把 detached cache 当成已经完成最终 compression
session 绑定。Attach 时先恢复 THD compression facts，再完成 manager attach。

Detached manager 不能永久保存普通 THD status counter 指针。Prepare/discard 使用 detached
accounting；attach 时才安全绑定 native counters，避免 prewarm 和淘汰污染普通状态变量。

Prepared facts 记录 binlog enabled/mode、cache encryption generation、keyring generation 和
target binlog incarnation。Key rotation 或 binlog mode 变化后，旧 handle 不得 attach；应销毁
并重新 prewarm，或将 token 标记 not-ready。不能在 attach 中临时重写整个 payload。

### 8.4 小 cache 形态

当 payload `<= binlog_cache_size`，默认 32 KiB：

```text
prepared handle
  -> native IO_CACHE buffer
  -> file == -1
```

没有文件名，token 直接映射到 handle。

### 8.5 大 cache 形态

超过 native buffer 后：

```text
prepared handle
  -> IO_CACHE buffer
  -> open native ML* temporary file descriptor
```

文件由目标端原生：

```text
open_cached_file(mysql_tmpdir, "ML", ...)
mysql_file_create_temp(..., UNLINK_FILE, ...)
```

创建。它可能创建后立即 unlink，只通过 FD 存活，因此文件名不能作为 token 或连接索引。

真正映射是：

```text
{epoch, token, digest, generation}
  -> prepared handle
  -> native IO_CACHE/FD
```

### 8.6 为什么不 rename carrier 文件

`<token>.binlog_cache` 是 transfer/carrier artifact，不是原生 runtime cache。Rename 不能
转移：

- IO_CACHE buffer；
- FD ownership；
- read/write position；
- encryption context；
- event counter/cache flags；
- savepoint/truncate state；
- commit/rollback/reset lifecycle。

因此 prewarm 直接构造最终 native cache。Resume 连 rename 都不需要。

现有 `<token>.binlog_cache` 第一阶段继续作为 carrier/staging artifact，不改变 local recovery
filtering 和清理语义。Fast resume 不使用它定位 THD。成功 attach 后可由现有 ownership
规则异步清理。

### 8.7 Attach

Attach 前提：

- target THD 已被 caller pin/protect；
- target THD 没有现存 binlog manager 或 active cache；
- token/epoch/digest/generation 与 preserved record 一致；
- handle 为 `SEALED_READY`；
- registry 已成功取得该 token 的 RAII attach lease；
- attach 不与第二个 THD 并发消费。

Attach 执行：

```text
校验 binlog_hton slot、THD 无旧 manager、SESSION/STMT Ha_trx_info pristine
-> 保证 caller 已 pin THD，完成前禁止该 THD DML/teardown
-> 恢复 THD binlog mode/compression session facts
-> 暂挂 fixed-address manager 到 ha_data
-> 显式注册 transaction/statement callbacks 和 read-write scope
-> 保留 preserved prev_position，不能按 cache end 重建隐式 savepoint
-> 绑定 native counters/plugin ownership
-> 所有权转给 THD
```

不能把 populated manager 挂入 `ha_data` 后模糊调用普通 `register_binlog_handler()`。该函数
只在 `prev_position == MY_OFF_T_UNDEF` 时注册和创建隐式 savepoint，直接复用可能漏注册，
或者覆盖 preserved statement rollback 位置。必须提供 binlog 私有的 preserve attach API。

SQL savepoint 恢复只能发生在 binlog handler/Ha_trx_info 已正确建立后。

成功后，continued DML、savepoint truncate、commit、rollback、flush 和 close 全部走原生
MySQL 路径，不在这些热路径增加 Preserve 分支。

## 9. Token 与新连接的对应关系

未来 HA/proxy 负责：

```text
old logical session -> token -> protected new THD
```

Preserve/Resume 负责：

```text
gate: token -> prepared lock plan -> ADOPTED_LOCKED trx
resume: token -> ADOPTED_LOCKED trx + native binlog handle
```

Promotion gate：

```text
HA 提供 committed epoch + physical-layout lease
-> validate required token set
-> durable ADOPTING intent
-> claim prepared trx
-> metadata-only lock import
-> register ADOPTED_FOR_PROMOTION/resumable=false
-> service-open barrier
```

Per-connection resume：

```text
HA 提供 token + protected THD
-> begin_attach(token)
-> consume ADOPTED_LOCKED record and native binlog handle
-> attach native binlog manager
-> attach/activate already-adopted trx
-> commit_attach
```

`Protected_thd_handle` 不是 raw `THD*` 包装，必须同时拥有：

1. **Lifetime pin**：复用 Preserve external-THD pin count，使 `THD::release_resources()` 在 handle
   释放前等待；
2. **Command-admission ownership**：resolver 在 `LOCK_thd_data` 下确认目标 THD idle/pristine，
   设置 per-THD `PRESERVE_ATTACH_OWNED` 原子状态。既有 Preserve command-read/dispatch hook 在该
   状态下不得让新 packet 进入执行；OFF/无 ownership 时只做一个 O(1) early return；
3. **Kill deferral guard**：复用/泛化 THD 已有 kill-immunizer 语义，在 handle 持有期间把
   `KILL QUERY/KILL CONNECTION` 记录为 pending kill，不设置可触发 teardown 的最终 killed
   state，也不关闭 attach 中的 ownership。不得只依赖 lifetime pin，因为它不能阻止
   `THD::awake()` 设置 kill。

`ACTIVE` durable 或受控 rollback 成功后，先释放 command gate，再释放 kill deferral；若期间
存在 pending kill，随后交回原生 `THD::awake()` 处理。进入 `ATTACH_TAINTED` 时三种保护继续由
tainted cleanup owner 持有，连接不可执行命令；在 ownership 收敛或实例重建前不得释放 pin后
让普通 teardown 抢走 binlog/MDL/trx ownership。Shutdown 也遵循同一 deferred-kill 规则并报告
blocked token，不能绕过 handle。

以下信息不参与绑定：

- `ML*` 文件名；
- `<token>.binlog_cache` 路径以外的目录顺序；
- receiver worker 顺序；
- connection id；
- source thread id；
- resume 到达顺序。

## 10. 状态机、幂等与 Service-Open Barrier

```text
DECLARED
  -> OBJECTS_RECEIVING
  -> OBJECTS_SEALED
  -> PREWARMING
       -> LOCK_METADATA_PARSED
       -> BINLOG_NATIVE_PREPARED
  -> PREWARMED_PENDING_FINAL_FACT
  -> READY_FACTS_PENDING_LEASE
  -> READY_FOR_GATE
  -> ADOPTING
  -> ADOPTED_LOCKED
       -> ATTACHING
            -> ACTIVATING
            -> ACTIVE
            -> ACTIVE_ARTIFACTS_CLEANED
            -> ATTACH_ROLLED_BACK
            -> ATTACH_TAINTED
       -> CLEANUP_PENDING
            -> CLEANUP_ROLLED_BACK
            -> CLEANUP_TAINTED
```

失败状态：

```text
NOT_READY
CORRUPT
STALE_GENERATION
PHYSICAL_FENCE_MISMATCH
RESOURCE_EXHAUSTED
CLEANUP_PENDING
CLEANUP_ROLLED_BACK
ABANDONED_ROLLED_BACK
ABANDONED_NOT_FOUND_PROVEN
CLEANUP_TAINTED
ATTACH_TAINTED
ATTACH_ROLLED_BACK
```

幂等规则：

- same key + same digest + READY_FOR_GATE：复用已有结果；
- same key + same digest + PREWARMING：合并任务或返回 pending；
- 新 generation 到达：取消/淘汰旧 prepared object；
- same key + conflicting digest：标记 corrupt；
- stale worker completion：按 generation 丢弃；
- duplicate resume：只有一次 attach lease 成功；
- `ATTACHING` 中 activation 前失败：逆序撤销并返回 `ADOPTED_LOCKED`；
- 进入 `ACTIVATING` 后不得回退 `ADOPTED_LOCKED`；失败时先在同一 THD/native trx ownership 下
  执行受控 rollback。Rollback 成功进入 `ATTACH_ROLLED_BACK` 后才能关闭 THD；rollback 失败
  进入 `ATTACH_TAINTED`，保持 protected THD 不再接收命令，不能先走普通 kill/teardown。
- `ADOPTED_LOCKED` 超过 client-resume deadline：只有 `begin_cleanup()` CAS 成功者进入
  `CLEANUP_PENDING`；cleanup 与 attach 不能同时消费同一事务。

Service open 前，epoch fact 中每个 required token 必须是：

```text
ADOPTED_LOCKED
ABANDONED_ROLLED_BACK
ABANDONED_NOT_FOUND_PROVEN
```

`CLEANUP_PENDING` 和 `CLEANUP_TAINTED` 都不是 service-open 安全终态。本轮不实现 engine
级持久化 no-commit quarantine；已 claim token 只有 rollback 成功，或严格证明 prepared trx
不存在，才能作为失败终态放行。“service-first”不等于带着未知 prepared trx 和缺失锁开放
业务。

功能语义允许已经安全 rollback 或严格证明不存在的 token 以 `ABANDONED_*` 终态通过 barrier；
但 full-pressure release success profile 仍要求 `abandoned=0`、全部 expected token adopted。前者
是故障降级正确性，后者是性能/完整性验收，不能互相替代。

Service-open 之后仍停留在 `ADOPTED_LOCKED` 的 token 由上述 client-resume deadline 管理。
其过期清理失败不会伪装成成功，也不能重新发放 token；它进入 `CLEANUP_TAINTED`，保留锁和
审计状态并持续告警。生产运维必须能按 epoch/token 观察和显式处理该状态。

## 11. Promotion Gate 与 Resume Core 顺序

### 11.1 Promotion epoch gate

```text
1. 读取并验证 epoch fact/token set
2. acquire physical-layout lease
3. 校验 boot incarnation/provider/final facts generation
4. 在 physical lease 已持有的前提下，逐 token revalidate dict/index lease、schema generation、
   predicate-empty 和 final metadata 不含 `LOCK_WAIT`/waiting ownership；任一失败不得 claim
5. 将通过 preflight 的 fact token 从
   `READY_FACTS_PENDING_LEASE` 原子推进为 `READY_FOR_GATE`
6. 写 durable ADOPTING intent
7. 并行 begin_gate_adopt
8. claim 前再次 revalidate physical lease/provider generation；随后 lease 在 holder release 前不可撤销
9. claim prepared trx，复核 XID/undo/rseg
10. 导入 read view/table locks
11. metadata-only record-lock import with per-token journal
12. 创建 detached MDL
13. 注册 ADOPTED_FOR_PROMOTION，resumable=false
14. 每 token commit `ADOPTED_LOCKED` registry state；全部 worker 收敛后一次 final rewrite epoch intent
15. operation deadline 到达或 token 失败时停止新任务，在仍有效 lease 下同步 rollback；任一
    rollback 未证明成功则阻断整个 epoch
16. 检查 service-open barrier
17. 将 lease 保持/移交给 HA role-transition coordinator
18. 完成 single-primary fencing、角色切换并原子 commit service-open
19. 确认 redo apply 不可恢复后 release lease
```

这一步在普通业务开放前完成。新主第一条业务语句开始就必须看到 preserved locks。
`required_apply_lsn==0`、production provider 缺失、lease revalidate 失败或 default/null executor
均在 claim 前 fail closed；不得回落现有真实 claim 路径后再选择 metadata-only helper。
Metadata-only import 内部只检查 operation deadline/cancellation，不周期性“续租”。如果 lease
可能在 import 中自行过期，该 provider 不符合接口合同。

Gate 启动后若 boot incarnation、provider generation 或 immutable final facts generation 发生
变化，立即停止领取新 token；对已 claim token按 import journal 逆序 unwind并同步 rollback，
全部成功前阻断 epoch。Strict policy 的 executor/provider dispatch 必须在现有 null/default
executor fallback 之前完成；null production provider 不能落入 legacy real-claim executor。

Physical/dictionary revalidation 与 provider contract violation 的处置固定为：

| 检测点 | Physical lease 是否仍可信 | 处置 |
|---|---|---|
| Acquire 前 generation/fact/dict mismatch | 未取得 | 不 claim，NOT_READY |
| Lease 取得后、claim 前普通 revalidate mismatch | 可信 | 不 claim，释放 lease/resource，NOT_READY |
| Claim 后发现普通 dict/fact mismatch，provider 合同仍成立 | 可信 | journal unwind + rollback；证明成功后 abandoned，否则 taint |
| Operation deadline 到达，provider 合同仍成立 | 可信 | 停止新任务，在 lease 下 unwind/rollback |
| Provider 在 holder 持有期间恢复 apply/layout 或无法证明 lease 仍有效 | 不可信 | 禁止猜测 unwind/rollback，epoch taint并阻断 service-open |

### 11.2 Per-connection staged fast resume 与不可逆 activation 边界

单连接 fast resume 推荐顺序：

```text
1. 记录 admission timestamp/deadline
2. 校验 owner/可信 HA authorization、ADOPTED_LOCKED/fact/incarnation
3. 写 token-scoped ATTACHING intent
4. begin_attach(token, expected_generation, protected THD)
5. 校验 THD pristine state
6. 恢复 session/access/binlog facts，包括受支持的 user variables；无法完整恢复的 user-var
   artifact 必须在 attach 前判 unsupported，不能静默丢失
7. attach native binlog manager 并注册 SESSION/STMT handlers
8. 从 detached MDL backup 克隆 transactional tickets 到 THD；backup 继续保留
9. 恢复 GTID ownership
10. materialize supported temp ownership
11. attach already-adopted trx to THD
12. 恢复 SQL/InnoDB savepoints
13. 确认剩余 deadline 足够覆盖 activation budget，调用 `begin_activation()`；只有 durable
    `ACTIVATING` intent 成功后才提交 activation boundary
14. `begin_activation()` 成功后立即 activate resumed trx；此后不可取消、不可回退
15. commit_attach and write ACTIVE terminal state
16. 异步清理 carrier/staging artifact
```

这里不再 claim prepared trx，也不导入 read view/table/record/predicate lock。

`begin_attach()` 返回 RAII lease。在写入 `ACTIVATING` 前，任一步失败必须按相反顺序撤销
handler、ha_data、MDL clone、GTID、temp ownership 和 trx attach；可以完整撤销时回到
`ADOPTED_LOCKED` 允许重试。Detached MDL backup 在整个 `ATTACHING` 期间保持权威且不删除；
abort 只释放 THD 上克隆出的 transactional tickets，只有 `ACTIVE` durable 后才能删除 backup。

`ACTIVATING` 只有一个权威边界：`begin_activation()` 成功提交 durable `ACTIVATING` intent。
该函数在 attach lease 独占下先写 durable intent；写失败则状态仍为 `ATTACHING`，
`activation_boundary_committed=false`，可执行完整 staging unwind。Intent 写成功后立即将内存
journal 的 `activation_boundary_committed/activation_started` 以 release-store 置 true并返回
成功。进程若崩溃在 durable write 成功与内存 flag 更新之间，restart 仍以 durable intent 为
权威，按边界已提交处理，绝不恢复为 `ADOPTED_LOCKED`。这构成 crash-consistent 的逻辑原子性，
不声称文件写和内存 store 是硬件原子操作。

第一个 `trx_preserve_activate_undo_ptr_state()` mtr 必须紧随该边界执行，但不再定义第二个状态
边界。此前所有 session/binlog/MDL/GTID/temp/savepoint/trx staging 必须已完成。Redo undo
activation 会提交 mtr并可能 `log_write_up_to()`，随后才处理 no-redo undo；因此 durable
`ACTIVATING` 成功后，即使尚未进入第一个 mtr，也不能使用现有 detach helper 假装回到
`PRESERVED/ADOPTED_LOCKED`。

Activation 失败时必须在仍持有 protected THD 和 native trx ownership 的同一控制路径执行受控
用户事务 rollback。Rollback 成功并证明 XID/owner 均已终结后写 `ATTACH_ROLLED_BACK`，随后
才能关闭连接。Rollback 失败或 ownership 不可证明时写 `ATTACH_TAINTED`，保持 THD pinned 且
禁止新 command；不得先调用普通 `THD::cleanup()`/kill，因为该路径会自行 rollback、释放 MDL
并销毁 binlog manager，使 taint ownership 无法审计。Carrier 只能在 `ACTIVE` terminal durable
后清理；`ACTIVE_ARTIFACTS_CLEANED` 只是 artifact cleanup flag，不表示用户事务已经结束。

上述“撤销”在 `activation_boundary_committed=false` 时只撤销 attach 侧 staging，不调用用户事务
`ROLLBACK`。进入不可逆 activation 后，受控 rollback 是唯一允许关闭该 THD 的收敛路径，不是
普通可重试 attach failure。任意异构 receiver 没有物理一致性证据，根本不能进入 attach，因而
也不得执行此 rollback。

Attach lease 内维护逐步 journal，例如 `session_facts_applied`、`binlog_manager_attached`、
`handlers_registered`、`mdl_cloned`、`gtid_owned`、`temp_materialized`、`trx_attached` 和
`activation_boundary_committed`、`activation_started`、`activated`。前两个标志由
`begin_activation()` 在 durable intent 成功后作为同一逻辑边界提交；`activated` 只在 native
activation 全部成功后置位。只有 `activation_boundary_committed=false` 才允许 staging unwind；
一旦为 true，失败只能走受控 rollback或taint，不能用一个笼统的“attach failed”分支猜测
资源所有权。

Attach 后 binlog `cache_state_map` 的 ownership 同时切换给 THD/native savepoint 路径；prepared
handle 只在 attach 前 seed 一次初始 state，attach 后不得再次 import 或由 registry worker 更新。
Attach admission 必须同时校验 global binlog open/incarnation、session `OPTION_BIN_LOG`、
`sql_log_bin`、cache encryption/key generation 和 handle `SEALED_READY`，任一不匹配在 ownership
转移前 fail closed。

### 11.3 Authorization

普通 SQL `RESUME` 继续检查 owner/`RESUME_ANY` 和对象权限。未来 HA 内部入口可使用可信
authorization policy，但只能由用户 SQL 无法构造的 protected THD handle 调用。当前
`skip_sql_privilege_check` 不能成为用户可控参数。

## 12. 资源治理

### 12.1 Record-lock 内存

新设计不要求 record pages resident，不占用额外 Buffer Pool 工作集，也不建立数十万页面
pin。只保存 compact plan：

```text
page/index identifiers
bitmap
final sizing facts
digest/generation/fence
```

主要内存与 `bitmap bytes + plan metadata` 成正比，不与 InnoDB page bytes 成正比。
Lock plan 的 vector/map/string capacity 和 generation overlap 全部计入现有
`preserve_trx_memory_budget_bytes` 与 `preserve_trx_memory_per_token_bytes`，新增内部 memory
kind `PROMOTION_LOCK_PLAN`，不能另建不受全局预算约束的 cache。

Plan builder 必须在扩容前增量 acquire memory lease；reservation 计入 container capacity、
normalized bitmap、page/index descriptor、journal 上界和 generation overlap。不能先构造完整
plan、再在发布 READY 时补做预算检查。

Closed-form accounting 使用真实 container capacity 而不是 logical size：

```text
token_plan_bytes = sizeof(plan/facts)
                 + page_group_vector.capacity * sizeof(page_group)
                 + entry_vector.capacity * sizeof(entry)
                 + sum(bitmap.capacity + normalized_bitmap.capacity)
                 + import_journal.capacity * sizeof(lock_t*)
                 + dict_lease_refs + strings/allocator overhead

epoch_plan_peak = sum(token_plan_bytes)
                + replacing_generation_bytes
                + registry/map node overhead
```

Strict epoch 初始内部子池硬线为：lock plan 最多使用全局 Preserve memory budget 的 60%，native
binlog manager/buffer 最多 30%，至少保留 10% 给 registry、intent、generation overlap 和其他
同时存在的 Preserve 元数据；单 token 同时受 `_per_token_bytes` 限制。比例是内部 admission
合同，不新增 public sysvar。P0 使用 full-pressure 实际 entries/bits/capacity 验证后可以在设计
评审中调整，但不能由实现代码静默改变。

Full-pressure success profile 不允许用 memory not-ready 比例换取通过：运行前必须证明 1000 个
required token 的 `epoch_plan_peak` 能进入上述子池；不足时该 profile 配置无效，应显式增加
现有全局 budget 或缩小支持包络，而不是接受部分 READY。

### 12.2 Binlog 内存和文件

默认每 token transactional cache buffer 约 32 KiB。初版为了 O(1) attach 和避免修改
原生 hot path，可以在 prewarm 阶段准备完整 manager，因此还可能包含空 statement cache
buffer，默认约 32 KiB。

只计算两个 32 KiB buffer 不是完整资源上界。1000 token 的最低 buffer 估算近似：

```text
32 MiB transactional cache
+ 32 MiB empty statement cache
= 64 MiB + manager/cipher/map/FD/accounting overhead
```

大 cache 超出 buffer 的部分位于 `mysql_tmpdir` 原生文件。READY 前必须检查：

- file-backed token count；
- open FD budget；
- tmpdir free bytes；
- max single-token bytes；
- total logical/physical bytes；
- `max_binlog_cache_size`。
- carrier/staging 与 native cache 双份文件窗口；
- generation replacement overlap。

资源不足时 token not-ready，不能在 resume 阶段临时重建。

Native manager/buffer 使用同一个 `Preserve_memory_lease`，内部 memory kind 为
`PROMOTION_BINLOG_NATIVE_CACHE`。File-backed payload 的内存 buffer 仍计内存预算，文件物理
bytes 和 FD 另由 resource ledger 管理。

### 12.3 Atomic reservation ledger

READY 前按 per-token、per-epoch 和 global 三层原子预留：

```text
lock plan bytes
native binlog memory bytes
file-backed logical/physical bytes
open FD count
mysql_tmpdir and preserve_dir headroom
dictionary lease count
generation overlap bytes
```

新 generation 必须先 reserve，再替换旧 generation。`ADOPTED_LOCKED` token 不可被普通
LRU 淘汰；多 epoch 采用公平调度，不能让一个 epoch 耗尽全部资源。

初版不新增 public memory/FD sysvar。Ledger 的原子性只覆盖 Preserve/Resume 自身预留，不能
把一次 OS/process FD 快照误写成对 MySQL 全部模块的全局锁：

- memory hard limit 复用 `preserve_trx_memory_budget_bytes` 和
  `preserve_trx_memory_per_token_bytes`；
- single cache logical limit 复用 `max_binlog_cache_size`；
- Preserve-owned FD 先在 process-wide Preserve ledger 中原子 reserve，多个 prewarm worker 不得
  基于同一快照重复超卖；
- `current_open_fds + preserve_reserved_fds + requested_fds +
  max(64, open_files_limit/10) <= open_files_limit` 仅作为保守 admission 条件；
- 原生 file open 结果才是最终权威；其它 MySQL 模块并发打开 FD 导致实际 open 失败时，必须
  同时释放 FD reservation、对应 `Preserve_memory_lease`、tmpdir bytes reservation 和未发布
  handle，并将 token 标记 resource-exhausted/not-ready；
- tmpdir admission 在 reservation 后仍保留 `max(1GiB, free_bytes/10)` headroom；
- receiver/gate concurrency 复用现有 receiver workers 和 promotion gate workers。

无法可靠取得 current-open-FD 或 free-space snapshot 时，file-backed handle admission fail
closed。后续只有在现有参数无法表达真实运维需求时才新增 public sysvar，避免重新制造无效
参数。

### 12.4 锁顺序

- ready-cache mutex 下禁止 file IO、digest、manager construction 和 lock import；
- prepared registry mutex 只保护 map/state transition；
- per-token preparation 串行；
- 不同 token 可并行；
- `begin_gate_adopt()` 和 `begin_attach()` 都必须 O(1)，不能扫描整个 epoch；
- attach native manager 时不持有 registry mutex；
- metadata-only import 使用既有 `lock_sys page shard latch -> trx mutex` 顺序；
- registry/ready projection/token mutex 与 lock_sys/trx mutex 不得同时持有；
- carrier/staging IO 和 digest 在所有上述 mutex 外执行；
- rollback journal 使用与正向 import 相同的 shard->trx 顺序，禁止反向取锁。
- attach 正向顺序固定为 protected-THD pin -> session/binlog -> MDL clone -> GTID -> temp -> trx
  attach -> savepoint；activation 前 unwind 严格逆序，且每一步不得持有 registry map mutex。
- `abort_attach_after_full_unwind()` 发布 `ADOPTED_LOCKED` 前，必须已经释放 THD clone MDL、handler
  registration、ha_data 和所有 staging ownership；cleanup CAS 只能在发布后竞争。
- dict/index lease acquisition 位于 lock_sys shard latch 之前；resource ledger reservation 位于
  dict lease 和 native file open 之前，二者都不能在 lock_sys/trx latch 内获取。

## 13. 失败和在线生命周期

| 场景 | 结果 |
|---|---|
| Lock digest/codec 错误 | `CORRUPT`，不发布 plan |
| Phase1 provisional generation 未 final | `NOT_READY` |
| Physical fence/lineage 缺失 | `NOT_READY` |
| Acquire/revalidate 前发现 incarnation/provider generation 漂移 | 不开始 claim 或在仍有效 lease 下 rollback；全部成功前不得 service-open |
| Provider 在 holder release 前违反不可撤销 lease 合同 | 物理一致性不可证明，epoch `CLEANUP_TAINTED` 并阻断 service-open；不得猜测性 rollback |
| Receiver frozen LSN 不等于 source final fence LSN | `PHYSICAL_FENCE_MISMATCH`；第一版无 compatibility escape hatch |
| Artifact 需要 record-image relocation | 不进入 metadata-only 快路径 |
| Artifact/protocol/server version 不兼容 | unsupported，保留审计直到 epoch purge |
| Implicit lock 未物化 | unsupported/not-ready |
| XID/undo/rseg/prepare fact 不一致 | 不 claim 或 rollback |
| Dict/schema generation 不一致 | not-ready |
| Metadata bitmap sizing 越界 | `CORRUPT` |
| Binlog 超 max cache | unsupported/not-ready |
| Native tmp file/FD/space 不足 | resource-exhausted |
| THD 已有 binlog manager | attach 前拒绝 |
| 同 token 两次 resume | 一次成功，另一次失败 |
| Gate claim 后 lock/MDL/register 失败 | shared kernel 立即 rollback claimed trx；成功为 `ABANDONED_ROLLED_BACK`，rollback 失败为 `CLEANUP_TAINTED` 并阻断 epoch |
| `ATTACHING` 中 activation 前失败且可完整撤销 | 回到 `ADOPTED_LOCKED` |
| `ACTIVATING` 后失败 | protected THD 上受控 rollback；成功为 `ATTACH_ROLLED_BACK` 后关闭连接，失败为 `ATTACH_TAINTED` 并保持 THD pinned/禁止新命令 |
| `ADOPTED_LOCKED` 超过 client-resume deadline | CAS 进入 `CLEANUP_PENDING`；物理一致的新主 rollback 成功后 `CLEANUP_ROLLED_BACK`，否则 `CLEANUP_TAINTED` |
| Receiver mysqld 退出 | handle/READY 失效；未终结 strict intent 阻断 startup service/promotion |

本设计使用在线语义，不支持 crash 后自动续接 strict promotion。Receiver crash 后 native FD
自动关闭，进程内 plan/handle 消失；外部 HA 必须丢弃旧 READY。启动 preflight 若发现未终结
strict intent，只能拒绝 service/promotion，并要求重建 standby 或显式 destructive abandon。
普通 startup 不得消费 standby token。

### 13.1 Status 与 SQL error 映射

| Internal result | SQL/internal surface |
|---|---|
| token/epoch 不存在 | `ER_PRESERVE_TRX_NOT_FOUND` |
| owner/authorization 失败 | `ER_PRESERVE_TRX_ACCESS_DENIED` |
| THD dirty、duplicate attach、错误 lifecycle | `ER_PRESERVE_TRX_INVALID_STATE` |
| codec/digest/bitmap corrupt | `ER_PRESERVE_TRX_CORRUPT_SNAPSHOT` |
| binlog mode/key generation mismatch | `ER_PRESERVE_TRX_BINLOG_MODE_MISMATCH` 或内部 not-ready |
| lease/fence/unsupported | `ER_PRESERVE_TRX_UNSUPPORTED`，并保留精确 token reason/status |
| Preserve memory/FD/tmpdir admission 失败 | `ER_PRESERVE_TRX_RESOURCE_EXHAUSTED` 或等价独立内部错误，不与 unsupported artifact 混淆 |
| gate rollback 未证明成功 | token `CLEANUP_TAINTED`，整个 epoch/service-open blocked |
| activation 前 attach staging 无法完整撤销 | token `ATTACH_TAINTED`，保持 protected THD 和 ownership journal；不得先走普通 kill/teardown，也不得返回普通 retryable |
| 缺少允许当前路径的物理一致性证据 | `PHYSICAL_CONSISTENCY_NOT_PROVEN`，claim/attach/rollback 均不执行 |

Promotion gate 不依赖 SQL error 表达批量结果；每个 token 必须返回精确 status、cleanup state
和 redacted reason。SQL 层只做现有错误码映射，避免为了内部状态无界增加错误码。

运维 runbook 必须把 `CLEANUP_TAINTED/ATTACH_TAINTED` 作为需人工介入的阻断态：记录 epoch、
token、XID、THD/owner、最后成功 journal step 和 rollback evidence；禁止自动删除 artifact、自动
kill pinned THD或把状态降级为 NOT_FOUND。允许的处置只有在同一新主上完成受控 cleanup，或由
HA 丢弃并重建 standby/实例；destructive abandon 必须显式授权并留审计记录。

### 13.2 参数与 kill switch

| 能力 | 参数/门 |
|---|---|
| 总特性 | `preserve_trx_enable`，startup-only |
| Receiver | `preserve_trx_transfer_receiver_enable` |
| Memory | `preserve_trx_memory_budget_bytes` / `_per_token_bytes` |
| Gate batch/concurrency/deadline | 现有 `preserve_trx_promotion_gate_*` |
| Strict metadata-only | 非 public sysvar；生产只允许 production physical lease provider + explicit internal policy；测试只允许隔离的 same-instance/frozen-copy policy |

本轮不增加 `allow_page_fallback` 或 metadata-only 强制开启参数。缺 lease/provider 时 strict path
不可达；这就是 kill switch。Gate 不能为了“可用性”现场退回 page-based cold import。

## 14. 可观测指标

### 14.1 Lock

```text
receiver_lock_metadata_parse_us
receiver_lock_digest_build_us
receiver_lock_plan_bytes
receiver_lock_plan_capacity_bytes
receiver_lock_plan_epoch_peak_bytes
receiver_lock_plan_subpool_cap_bytes
receiver_lock_plan_bytes_p50/p95/max
receiver_lock_unique_pages
receiver_lock_bitmap_entries
receiver_lock_bitmap_bits
receiver_lock_final_generation
receiver_lock_physical_fence_lsn
receiver_lock_metadata_only_eligible_tokens
promotion_preserve_gate_elapsed_us
promotion_ha_role_fence_us
promotion_fence_lease_wait_us
promotion_fence_digest_compare_us
promotion_intent_write_us
promotion_dict_lookup_us
promotion_lock_metadata_only_import_entries
promotion_lock_page_get_count
promotion_lock_page_get_us
promotion_lock_image_resolves
promotion_lock_apply_us
promotion_lock_conflict_count
promotion_lock_shard_wait_us
promotion_lock_import_journal_unwind_count/failures
promotion_lock_accounting_bits
promotion_service_open_blocked_tokens
promotion_epoch_blocked_tokens
promotion_cleanup_tainted_tokens
promotion_adopted_locked_tokens
promotion_adopted_locked_expired_tokens
promotion_adopted_cleanup_pending_tokens
promotion_adopted_cleanup_rolled_back_tokens
promotion_gate_expected_tokens
promotion_gate_adopted_tokens
promotion_gate_abandoned_tokens
promotion_gate_skipped_tokens
promotion_gate_failed_tokens
resource_admission_open_failed_count
resource_admission_memory_lease_release_failures
lock_warmcopy_hot_hook_target_hits
lock_warmcopy_hot_hook_non_target_skips
lock_warmcopy_hot_hook_non_target_partition_locks
lock_warmcopy_hot_hook_partition_wait_us
```

硬要求：

```text
promotion_lock_page_get_count == 0
promotion_lock_page_get_us == 0
promotion_lock_image_resolves == 0
promotion_lock_metadata_only_import_entries == expected_bitmap_entries
promotion_lock_accounting_bits == imported_set_bits
promotion_gate_adopted_tokens == promotion_gate_expected_tokens
promotion_gate_abandoned_tokens == 0
promotion_gate_skipped_tokens == 0
promotion_gate_failed_tokens == 0
promotion_cleanup_tainted_tokens == 0
promotion_epoch_blocked_tokens == 0 at barrier release
promotion_fence_digest_compare_us only covers fixed-size compare
receiver_lock_plan_epoch_peak_bytes <= receiver_lock_plan_subpool_cap_bytes
```

`unique_pages` 是去重后的 `{space_id,page_no}` 数量；`bitmap_entries` 是可产生独立 `lock_t`
的 plan entry 数量，通常按 `{page,index,type_mode}` 区分。同一 page 可以有多个 entry，二者不得
混用。`promotion_service_open_blocked_tokens` 可保留为运行时诊断，但 barrier 放行时必为 0，
不能替代 adopted/failed 数量硬线。

### 14.2 Binlog

```text
receiver_binlog_prepare_tokens
receiver_binlog_prepare_us
receiver_binlog_prepare_input_bytes
receiver_binlog_prepare_memory_bytes
receiver_binlog_prepare_file_bytes
receiver_binlog_prepare_file_backed_tokens
receiver_binlog_prepare_open_fds
receiver_binlog_prepare_failures
receiver_binlog_prepare_native_write_bytes
receiver_binlog_key_generation_mismatch
resume_binlog_attach_us
resume_binlog_payload_read_bytes
resume_binlog_payload_write_bytes
resume_binlog_rename_count
```

硬要求：

```text
resume_binlog_payload_read_bytes == 0
resume_binlog_payload_write_bytes == 0
resume_binlog_rename_count == 0
```

### 14.3 Resume

```text
promotion_resume_core_elapsed_us
promotion_resume_core_p50_us
promotion_resume_core_p95_us
promotion_resume_core_p99_us
promotion_resume_core_max_us
resume_admission_us
resume_registry_attach_us
resume_mdl_transfer_us
resume_binlog_attach_us
resume_gtid_us
resume_temp_us
resume_trx_attach_us
resume_savepoint_us
resume_session_restore_us
resume_activate_us
resume_ready_miss_count
resume_deadline_miss_count
resume_attach_abort_count
resume_attach_tainted_count
resume_attach_rolled_back_count
resume_failure_count_by_state
resume_expected_tokens
resume_active_tokens
resume_physical_consistency_mode
resume_real_redo_apply
resume_real_ha_promotion
```

Gate、prewarm 和 resume elapsed 第一版复用 Performance Schema histogram/timer bucket 或本仓库
已有固定边界 mergeable buckets；不得引入每次操作动态分配的通用 HDR 实现。不能只暴露
last-operation scalar 或 process-lifetime 累计值后由 Python 计算伪 P95。报告输出每轮 count、
P50、P95、P99、max 和 failure count，并标明 bucket scheme/version。

所有 E2E JSON 指标必须来自原始 server status/test hook，Python 不能根据“没有报错”推导
page get、cold IO 或 payload copy 为 0。

硬要求同时包含：

```text
promotion_resume_core_p95_us <= 100000
promotion_resume_core_max_us <= 100000
resume_active_tokens == resume_expected_tokens
resume_deadline_miss_count == 0
resume_attach_tainted_count == 0
sum(resume_failure_count_by_state) == 0
resume_binlog_payload_read_bytes == 0
resume_binlog_payload_write_bytes == 0
```

## 15. 代码边界

预计修改：

- `sql/preserve_trx_transfer.cc/.h`
  - sealed external object streaming reader；
  - lock/binlog prepared work enqueue；
  - final generation/fence 绑定；
  - fast path 不整体 hydrate binlog blob。
- `sql/preserve_trx_promotion.cc/.h`
  - copyable ready facts；
  - single-owner prepared registry；
  - RAII adopt/attach lease、service-open barrier 和 metrics。
  - strict physical policy 在现有 null-executor/default-executor fallback 之前显式 fail closed；
    legacy policy 保留现有 default executor，不做全局语义反转。
- `sql/preserve_trx.cc/.h`
  - shared recover/adopt kernel 第三 policy 消费 validated plan；
  - promotion wrapper 不直接 claim/import/register；
  - local startup wrapper 保持 page-based identity validation，但复用修复后的多 bit accounting。
- `storage/innobase/lock/lock0preserve.cc`
  - shared payload parse/validation；
  - final sizing plan。
- `storage/innobase/lock/lock0lock.cc` 与 Preserve 专用声明
  - metadata-only bitmap allocation/import helper；
  - 不修改普通 record lock API 行为。
- `storage/innobase/trx/trx0preserve.cc`
  - XID/undo/rseg/prepare fact 校验；
  - rollback 成功证明与 epoch-blocking tainted 终态。
- `sql/binlog.cc/.h`
  - opaque detached native cache prepare/attach/destroy API；
  - `binlog_cache_mngr` 保持私有。

禁止：

- 在普通 `Binlog_cache_storage::write/truncate/reset/flush/close` 增加 Preserve 分支；
- 修改原生 `ML*` naming、encryption 和 cleanup；
- 改变普通 DML lock acquisition 的原生语义；允许把既有 Preserve warmcopy hook 收窄为
  epoch + active-target 两层 O(1) 门，但非 target/OFF 路径不得增加 mutex、map lookup 或分配；
- 在 `preserve_trx_enable=OFF` 时创建 registry、plan、manager 或 worker；
- 为追求快路径放宽 physical-fence、digest、token 和 ownership 校验。

必须增加四类 source-shape guard：

1. Page-free helper 只有 shared kernel physical-fence policy 一个生产调用点；
2. Promotion wrapper 不能直接调用 helper/claim/import/register；
3. Binlog native manager 仍为 `sql/binlog.cc` 私有，transfer/promotion 不解引用；
4. 普通 `Binlog_cache_storage` hot methods 不出现 Preserve 分支；普通 DML lock path 只允许
   精确 allowlist 中已有的单个 inline warmcopy hook，并强制 epoch + active-target 两层 O(1)
   early return，禁止第二个 Preserve 分支或非 target mutex/map/allocation。

Strict registry 不以现有 `g_ready_cache` 全局 map 为 authority。若 strict 实现完全隔离并只使用
per-entry CAS，不要求为了本设计顺手重构 legacy ready cache；只有 profiling 证明 strict path
仍取得其全局 mutex 时，才把该调用点移除或分片。现有 abandoned-marker cleanup/reaper 也不因
“range-for 使用值拷贝”而自动判 bug：它将更新后的 token 放入 rewritten vector再整体持久化；
只有新的 runtime test 证明状态丢失时才修改 legacy reaper。本设计新增的 strict cleanup 必须
通过 registry `begin_cleanup()`，但不扩大为无证据的 legacy 重写。

此外，`preserve_trx_enable=OFF`、从未打开 warmcopy epoch、以及“曾打开但已关闭 epoch”三种
状态都必须与原生 baseline 做 DML/lock throughput NFR，不能只靠 lint 证明零侵入。

## 16. 测试设计

### 16.0 测试基建、基线和统计方法

当前 preserve_trx MTR 没有第二个真实 mysqld 实例，因此职责固定为：

- GUnit：内核数据结构、state CAS、fault injection、mock lease；
- MTR：单实例 SQL/权限/OFF/local-startup 行为和 source-shape guard；
- `resumable_trx_business_e2e.py`：当前 pre-drain physical-copy 只覆盖 source + receiver
  transfer/prewarm/gate，不具备负载后的物理一致性，不能执行跨节点 attach/DML 证明；
- test-only protected-THD component：只在同实例或 QUIESCED 后冻结 datadir 副本模式中覆盖
  attach core 行为和性能；
- 未来真实 HA 项目：production redo/apply/fence/provider 证明。

当前 E2E runner 只执行到 promotion gate，没有实际为目标连接调用
`preserved_trx_resume_adopted_for_promotion_on_thd()`，因此现有报告不能证明 per-connection
100ms。P0 必须增加一个不进入生产 mysqld 用户接口的 test-only plugin/component：它只能在
显式 `TEST_SAME_INSTANCE_ATTACH_ONLY` 或 `TEST_FROZEN_DATADIR_COPY` 证据下，先建立目标连接并
记录 connection id，再通过 `Preserved_trx_peer_thd_resolver` 获得 protected handle并调用内部
attach API。组件不得创建 adopted record、伪造 production lease或绕过 registry/gate。
Release E2E 必须使用 release mysqld 加载该测试组件，不能用 mock attach 或 Python sleep 代替
真实 server-side attach；但测试结果仍不得标记为真实 HA 续作。

TEST_ONLY 组件合同固定为：

```text
build target: plugin/test_preserve_trx_promotion (MODULE_ONLY + TEST_ONLY)
internal header: sql/preserve_trx_promotion_test_service.h
entry: test_preserve_trx_attach(epoch, token, connection_id)
production core: preserved_trx_resume_adopted_for_promotion_on_thd(...)
```

组件只允许定位并 pin 一个 pristine/idle THD、调用唯一 production attach core、返回结构化状态；
不得创建 adopted record、直接 claim/import/register、写 production provider slot、伪造 LSN/digest、
绕过 authorization/registry，或从用户可安装的 production package 默认加载。Release test build
必须显式启用组件；普通 release packaging/link map 不包含该 entry。Same-instance 只证明 attach
core，frozen-copy 证明静态副本 gate+attach，二者都不证明真实 redo apply/HA。
Consistency mode 在组件加载时冻结，不由该 entry 的参数选择。

Mock provider 必须返回完整 proof/lease、支持 acquire/revalidate失败、operation deadline和
“provider违反不可撤销合同”的故障注入，并与 production provider slot 隔离；不得只设置
`applied_lsn=UINT64_MAX` 作为“物理一致”。

性能改动前先用同一 release harness、硬件和 full-pressure 模型记录当前 page-based/copying
baseline。实现后至少连续三轮，每轮 `tokens=1000`，三轮均满足硬线；报告使用最差轮
P95/P99/max，不选择最佳轮。每轮必须包含成功/失败样本总数，少于 expected token count 的
报告无效。

P0 RED 退出条件：

```text
current promotion page_get_count > 0
current resume payload_write_bytes > 0 for logged-cache token
multi-bit bitmap direct-copy accounting test fails on current helper
populated-manager ordinary registration-order test fails
missing/acquire-failed/revalidate-drift lease rejects strict policy
provider contract violation taints and blocks instead of guessing rollback
```

### 16.1 GUnit：metadata-only lock import

1. Payload codec 和 plan 与当前 import 解析结果一致。
2. Final `page_n_heap` 计算正确 `n_bits`，纯零 bitmap 尾部可安全裁剪。
3. 修复后的 page-based import 与 metadata-only import 创建的 `lock_rec_t`
   page_id/n_bits/bitmap/mode/accounting 等价。
4. 新旧两条 import 路径中，多 bit lock 的 `trx->lock.n_rec_locks` 都等于 set-bit count。
5. `table->n_rec_locks`、trx list/version、monitor/PFS 与 native create/remove 对称。
6. Split/merge/delete/rollback 后计数回到 0，无 underflow/assert。
7. Import 过程 `buf_page_get` 和 image resolve count 为 0。
8. Gap、next-key、record-not-gap、insert-intention 语义正确。
9. 同 page 多 bitmap 和同 trx 多 record 正确合并/挂链。
10. Existing explicit lock 冲突正确返回。
11. Phase1 provisional generation 不可进入快路径。
12. Fence/lineage/digest/incarnation mismatch fail closed。
13. `record_images` 或 implicit-lock-unmaterialized artifact 不进入第一版。
14. Bitmap overflow、heap bound、supremum mode 校验 fail closed；ordinary supremum 仍按整页
    sizing，predicate 1-byte sizing 不可误用。
15. Claim 后部分 import 失败可完整 rollback 已安装锁和 accounting。
16. Operation deadline 到达后不再开始新 import，但已开始任务仍在有效 lease 下安全 unwind。
17. Page/index/schema generation、drop/recreate、page reuse proof mismatch fail closed。
18. 同一 identity duplicate publish/adopt 被 terminal record 拒绝。
19. Epoch purge 关闭全部 FD 并释放全部 plan/resource lease。
20. Role transition/service-open commit 前 revalidate 失败会在仍有效 lease 下 rollback 全部
    adopted token并阻断 barrier；provider违反不可撤销合同时只能taint/block，不能猜测rollback。
21. `CLEANUP_TAINTED` 不能作为 service-open 安全终态。
22. `ADOPTED_LOCKED + mysql_thd=null` 的锁继续阻塞冲突DML，purge/delete-mark lock inheritance
    和preserved rseg保护不依赖placeholder THD。
23. Provider 在第 N 个 entry import 中违反不可撤销合同，epoch 只能 taint/block，不能猜测
    unwind；正常 deadline cancellation 仍在有效 lease 下完整 journal unwind。
24. Final facts release-publish 前 worker 看不到新 generation；stale completion 不能覆盖新 facts。
25. 1000-token plan capacity accounting 与实际 allocator/container capacity 一致，超过 lock-plan
    subpool 时在 claim 前 fail closed。
26. Predicate payload 非空或 final metadata 含 `LOCK_WAIT`/waiting ownership 时在 claim 前拒绝；
    source/receiver 不得 strip 标志后进入 granted-lock import。

### 16.2 GUnit：binlog native handle

1. 小 payload 保持 memory-backed、无文件。
2. 大 payload 使用原生 file-backed cache。
3. Manager 固定地址，handle 不 memcpy/move manager object bytes；RAII 壳 out-of-line destructor
   与 THD `binlog_close_connection()` 使用匹配的 allocator/deleter。
4. Detached accounting 不污染普通 status。
5. 目标端 binlog encryption 语义正确。
6. Token A 不能消费 token B handle，handle 只能 attach 一次。
7. Epoch/generation/digest mismatch 在 ownership transfer 前失败。
8. THD 已有 manager 或 dirty Ha_trx_info 时拒绝。
9. SESSION/STMT handler、read-write scope、previous position 正确。
10. Event counter、flags、compression session facts、cache-state map 保持。
11. Attach 后 continued DML、savepoint/statement rollback truncate 正确。
12. Commit 输出 prewarm event + post-resume event，内容和顺序正确。
13. Rollback/reset/close 无 FD/buffer/temp-file 泄漏。
14. Durable `ACTIVATING` intent 前的 fault point 可逆；intent 写成功后、内存 flag store 前、
    第一个 undo-state mtr 前、redo undo activation 后、no-redo activation 前后分别注入失败。
    Durable boundary 成功后不得返回 `ADOPTED_LOCKED`：受控 rollback 成功进入
    `ATTACH_ROLLED_BACK`，失败进入 `ATTACH_TAINTED` 且普通 THD teardown 不得先运行。
15. Fast path payload read/write/rename 均为 0。
16. Key rotation/binlog incarnation 变化使旧 handle not-ready。
17. FD/tmpdir/memory admission 超限不产生 partial handle。
18. `begin_attach()` 与 `begin_cleanup()` 并发只有一个 CAS 成功。
19. MDL restore 只 clone tickets；activation 前 abort 释放 THD clone但保留 detached backup，
    ACTIVE 后才删除 backup。
20. `OPTION_BIN_LOG`/`sql_log_bin`/global binlog open/incarnation mismatch 在 ownership transfer 前拒绝。
21. Attach 后 `cache_state_map` 只有 native THD/savepoint 路径可写，registry worker 不再更新。
22. User variables 完整恢复；unsupported user-var artifact 在 attach 前拒绝。
23. `preserve_trx_enable=OFF` 或 capability 非 strict 时，prepare/attach 自身拒绝且不分配
    manager、FD 或 memory lease。
24. `opt_bin_log=false`、`mysql_bin_log.is_open()==false` 或 session binlog mode 不匹配时，
    prepare/attach 在 ownership transfer 前拒绝。
25. `ATTACH_ROLLED_BACK` terminal tombstone 在 epoch purge 前拒绝同 identity 再次 publish/attach。
26. Protected handle 同时持有 lifetime pin、command gate 和 kill deferral；attach 中并发
    `KILL QUERY/KILL CONNECTION` 不触发 teardown，ACTIVE/rollback 后 pending kill 才交回原生路径。
27. `ATTACH_TAINTED` 保持 command blocked；`ROLLBACK_ALLOWED` 可进入 cleanup，
    `INSTANCE_REBUILD_ONLY` 无进程内 kill/rollback 出口。
28. `begin_activation()` intent write 失败保持 `ATTACHING` 且可完整 unwind；durable write 成功后
    在内存 flag store 或第一个 undo mtr 前 crash，restart 仍按 boundary committed 阻断/cleanup。

### 16.3 MTR

- 单 mysqld `TEST_SAME_INSTANCE_ATTACH_ONLY` 下多连接、多 token、多 binlog cache，按 token
  attach后不串cache；该用例只证明attach core；
- owner 与 `RESUME_ANY` 权限不被 fast path 绕过；
- primary/secondary index record locks、gap/next-key locks 恢复后阻塞语义正确；
- gate 完成但连接尚未 resume 时，preserved locks 已阻塞新业务；
- gate 未完成时 service-open barrier 不放行；
- 同实例或QUIESCED后冻结datadir副本中，attach后继续UPDATE/INSERT/DELETE、savepoint rollback、
  commit/rollback正确；任意异构receiver必须在attach前拒绝；
- GTID、compression、session state 和 binlog event 内容正确；
- unsupported predicate/temp/artifact 明确 fail closed；
- local startup 继续使用原 page-based import；
- ordinary SQL `RESUME` 不能消费 promotion-owned token；
- trusted HA attach 入口不能由用户 SQL 构造；
- attach fault 后 token 可重试或明确 tainted；
- attach activation前故障只撤销attach staging，不rollback用户事务；
- activation开始后的故障必须先受控rollback；rollback失败保持protected THD pinned并进入
  `ATTACH_TAINTED`，普通kill/teardown不得抢先运行；
- MDL clone abort后detached backup仍存在并可重试，ACTIVE后才删除；
- `ADOPTED_LOCKED` deadline cleanup与`begin_attach()`竞争时只有一个CAS成功；
- attach full unwind发布回`ADOPTED_LOCKED`前，cleanup不得取得lease；发布后只有一个CAS成功；
- attach 中目标连接发送新 command 或被 `KILL QUERY/KILL CONNECTION` 时不能越过 protected
  handle；ACTIVE/rollback 后 pending kill 按原生语义生效，tainted 时保持阻断；
- required apply LSN 为 0、provider 缺失、null/default executor 均 fail closed；
- unsupported artifact 保留审计状态，直到 rollback 成功或阻断 epoch 后显式清理；
- 未终结 `ADOPTING/ATTACHING/ACTIVATING` intent 在 receiver restart 后必须阻断
  service/promotion，普通 startup 不得自动消费或 replay；
- OFF path 不创建 prepared state；
- source-shape guard 禁止 fast path 调 `buf_page_get`、blob hydrate 和 payload import。

新增 source-shape contract：

```text
standby_promotion_metadata_only_shared_kernel_lint
standby_promotion_metadata_only_no_page_io_lint
standby_binlog_native_handle_privacy_lint
preserve_off_hot_path_isolation_lint
standby_promotion_metadata_only_unreachable_before_p4_lint
```

Guard 必须带明确 deny-list，而不是只有测试名：metadata-only no-page-IO 禁止
`buf_page_get/buf_read_page/buf_read_page_background/page_find_rec_with_heap_no` 和 record-image
resolver；shared-kernel guard 禁止 promotion wrapper 直接调用 claim/import/register/page-free
helper；binlog privacy guard 禁止 transfer/promotion 解引用 `binlog_cache_mngr` 或调用普通
`register_binlog_handler()`；hot-path guard 禁止 Preserve 第二分支、map lookup、mutex 和 allocation，
只允许现有单一 inline warmcopy hook。Binlog guard 与 lock hot-path guard 分成独立 MTR，避免一个
regex 同时承担两个边界。

P2 unreachable guard 在 P4 接线提交前要求 page-free helper 的非测试生产 caller count 为 0；
P4 接线后反转为“唯一 caller 必须是 shared kernel physical-fence policy”，并继续拒绝
legacy/default executor 和 wrapper 调用。

MTR 的 test provider 只能证明入口、状态和失败语义，不能计作跨实例 physical-fence 证据。

### 16.4 Release E2E

使用 approved full-pressure family：

```text
tokens = 1000
statements_per_tx = 100000
lockset_batch_size = 100000
record-lock bitmap pages/bits > 0
binlog cache tokens 和 bytes 显式报告
receiver workers 和资源参数显式报告
```

先 scaled，再连续至少三轮 full release。不同证据profile使用不同硬指标，不能把不可执行的
gate/attach字段填0后混成一张“成功”报告。

Pre-drain physical-copy transfer/prewarm profile：

```text
receiver_ready_tokens == standby_tokens
receiver_not_ready_tokens == 0
ready_cache_miss_count == 0
lock_warmcopy_hot_hook_non_target_partition_locks == 0
resource_admission_unaccounted_bytes == 0
```

该profile不得输出`promotion_gate_adopted_tokens`或`resume_active_tokens`成功断言。

Frozen-copy gate profile：

```text
promotion_gate_expected_tokens == standby_tokens
promotion_gate_adopted_tokens == promotion_gate_expected_tokens
promotion_gate_abandoned_tokens == 0
promotion_gate_skipped_tokens == 0
promotion_gate_failed_tokens == 0
promotion_cleanup_tainted_tokens == 0
promotion_preserve_gate_elapsed_us <= 1000000
promotion_lock_page_get_count == 0
promotion_lock_page_get_us == 0
promotion_lock_image_resolves == 0
promotion_lock_accounting_bits == imported_set_bits
promotion_lock_metadata_only_import_entries == expected_bitmap_entries
receiver_lock_plan_epoch_peak_bytes <= receiver_lock_plan_subpool_cap_bytes
promotion_fence_digest_compare_us reports fixed-size compare only
lock_warmcopy_hot_hook_non_target_partition_locks == 0
resource_admission_unaccounted_bytes == 0
```

Same-instance/frozen-copy attach profile：

```text
resume_expected_tokens == promotion_gate_adopted_tokens
resume_active_tokens == resume_expected_tokens
promotion_resume_core_p95_us <= 100000
promotion_resume_core_max_us <= 100000
resume_deadline_miss_count == 0
resume_attach_tainted_count == 0
sum(resume_failure_count_by_state) == 0
resume_binlog_payload_read_bytes == 0
resume_binlog_payload_write_bytes == 0
resume_binlog_rename_count == 0
```

Batch wall time、CPU、lock_sys shard contention 另外报告，不得用单连接 `<=100ms` 掩盖
1000-token 总耗时。

由于当前没有真实物理备机，Release证据拆成三类独立profile：

1. 现有pre-drain physical-copy profile只验证transfer/prewarm/gate准备链路，不执行strict adopt
   或跨节点attach；
2. same-instance profile只验证同一InnoDB实例上的protected-THD attach core；
3. frozen-copy profile在所有目标QUIESCED、source停止后复制完整datadir，保留receiver UUID并恢复
   receiver standby carrier/spool，再使用`TEST_FROZEN_DATADIR_COPY` lease验证静态物理副本上的
   gate和attach。

Frozen-copy harness 操作顺序固定为：

1. 停止 receiver，保存 receiver `auto.cnf`、server UUID、carrier、frame spool 和 epoch fact；
2. 确认 source 所有目标 QUIESCED 并停止 source，记录 fence LSN；
3. 复制完整 source datadir 到 receiver staging；
4. 删除副本中 source-local Preserve carrier/intent/token artifact，禁止它们进入 receiver local
   startup recovery；
5. overlay receiver carrier/spool/epoch fact，恢复 receiver `auto.cnf`/UUID；
6. 启动前 listing 验证不存在 source-local snapshot token，standby token set 与 epoch fact 相等；
7. 启动 receiver并只安装 `TEST_FROZEN_DATADIR_COPY` provider/component。

Pre-drain和frozen-copy profile必须实际启动source/receiver两个mysqld或等价独立进程，不能用
单进程复制map替代artifact transfer/native handle构建。三类profile都不构成真实redo apply或
HA promotion证据。
报告必须标注：

```text
physical_consistency_mode=pre_drain_copy|same_instance|frozen_datadir_copy
simulated_physical_fence=true
real_redo_apply=false
production_ha_promotion=false
real_business_continuation_proven=false
```

E2E 必须覆盖 final tail cap、gate service-open barrier 和 attach failure staging unwind。Sequential
Python SQL、source-shape lint、小 smoke 或 cold startup 不能替代 release behavior 证据。
Same-instance和frozen-copy attach profile必须创建`resume_expected_tokens`个真实目标THD，通过
test-only protected-handle组件逐个调用生产内部attach core。仅这两个物理一致性模式可以在
目标THD上继续DML/commit或rollback验证所有权与binlog内容；pre-drain copy profile不得执行。
这些结果证明100ms attach core，不证明真实物理备机业务续作。

## 17. 实施顺序

### P0：合同、指标和 RED guard

- 冻结 SHA-256 canonical serialization、digest producer/verifier 和 exact-LSN fence，删除未定义的
  layout-compatibility escape hatch；
- 冻结 metadata-only helper 的原生等价 conflict predicate、validated dict/index lease 和
  no-wait-import 语义；
- 定义 production physical lease interface、strict shared-kernel policy 和 service-open barrier；
- 定义 same-instance、frozen-copy、production-redo-apply 三类 consistency evidence，测试入口不能
  构造 production mode；
- 将 lease 冻结为 holder 持有期间不可撤销，operation deadline 与 lease lifetime 分离；
- 冻结现有 ready enum 到 strict registry 的单一权威映射；
- 增加 page-get、image-resolve、lock accounting、binlog payload IO、attach 分段指标；
- 用 RED test 证明当前 promotion import 会访问页面；
- 用 RED test 证明当前 page-based bitmap accounting 和 binlog handler 顺序缺口；
- 修正 ordinary supremum sizing 合同，冻结 bitmap entry/unique page 指标语义；
- 定义 lease ownership 贯穿 role commit、`ACTIVATING` 不可逆边界和 crash-intent fail-closed
  规则；
- 增加只随测试构建的 protected-THD attach component，保证 release E2E 能调用真实内部 core；
- 明确 P5 activation 前只撤销 attach staging；activation 开始后的失败只能受控 rollback 或
  `ATTACH_TAINTED`，不得先走普通 THD kill/teardown；
- 冻结 strict registry C++/RAII API、final facts release/acquire publication 和 projection read
  contract；
- 建立 lock-plan closed-form accounting、60/30/10 子池合同和 entry/bit microbenchmark；
- 增加 strict helper/shared-kernel/binlog-private/native-hot-path 四类 source-shape guard。

### P1：Final fact 与 prepared continuity

- 区分 phase1 provisional 与 final quiesced generation；
- 冻结 final sizing、tail cap 和 physical fence 字段；
- 加入 XID/undo/rseg/prepare/read-view facts；
- 加入 implicit-lock materialization 和 target incarnation/schema generation；
- 加入 page/index/compression/encryption/version generation；
- gate 使用 epoch-level ADOPTING intent；attach 使用 token-level intent；
- 增加 `ADOPTED_LOCKED` client-resume deadline 和 cleanup/attach CAS 合同；
- 缺 provider/fence 默认 not-ready。

### P2：Metadata-only lock helper

- 在 Preserve-only 入口直接按 page_id/n_bits/bitmap 创建 lock_t；
- 完整维护 native per-bit/per-lock accounting 和 unwind；
- 增加 per-token import journal 和 hot-hook active-target gate；
- 保持普通 lock acquisition 语义和 local startup 的 page-based identity validation；修复现有
  Preserve bitmap import 的多 bit accounting；
- 跑 lock semantics、rollback、split/delete 和 page-free tests。
- P2 结束时 helper 对生产路径必须仍不可达：只有 GUnit/internal test symbol 可调用，production
  caller count 为 0；source-shape/link guard 禁止 legacy promotion、default executor、wrapper 和
  local startup 调用。只有 P4 完成 physical lease、strict registry和shared-kernel policy接线后，
  才允许出现唯一生产调用点。

### P3：Opaque native binlog handle

- 添加 stream prepare/destroy 和 fixed-address handle；
- 添加 detached accounting 和 explicit handler registration；
- 加入 keyring/binlog incarnation revalidation；
- 验证 small memory / large ML* 形态；
- 建立 attach/detach fault matrix。

### P4：Epoch gate 与 service-open barrier

- 发布 atomic facts/resources entry；
- 接入现有 memory lease，增加 FD/tmpdir/epoch purge admission；
- durable intent + RAII adopt lease；
- process-wide Preserve resource ledger 原子管理 Preserve-owned FD/bytes，实际 file open 失败仍
  fail closed并释放reservation；
- promotion gate 使用 metadata-only lock import；
- rollback-success terminal policy 与 tainted epoch-blocking policy；
- lease 交接到模拟 HA role transition，service-open commit 后才释放；holder期间provider不可
  恢复apply或撤销lease；
- release simulator gate `<=1s` 证据。

### P5：物理一致性分级 Protected THD Attach Core 与 100ms 证据

- atomic attach lease 和 authorization policy；
- session/binlog/MDL/GTID/temp/savepoint 顺序、`ACTIVATING` intent 和不可逆 activation；
- 同实例只验证attach core；frozen-copy验证静态物理副本上的gate/attach；pre-drain copy不执行
  跨节点attach；
- activation 前 attach 失败只撤销 staging；activation 后受控 rollback，失败进入 pinned
  `ATTACH_TAINTED`；`ADOPTED_LOCKED`过期清理由独立registry reaper处理；
- targeted GUnit/MTR 全部通过；
- Preserve/Resume standard regression；
- release scaled；
- full-pressure 连续三轮；
- 不用 small smoke、cold startup 或 N=3 simulator 替代 full-pressure 结果。

## 18. 验收标准

本设计完成必须同时满足：

证据标签按验收项编号固定如下；同一项可同时属于多个层级，低层证据不能签发高层结论：

| 标签 | 可覆盖的验收项 | 含义 |
|---|---|---|
| `IN_REPO` | 1、2、5-14、16、17、20、22-30、32-39 | 当前仓库 GUnit/MTR/source-shape/release harness 可直接证明 |
| `SIMULATOR` | 3-5、13、15、18、19、29、31、33、34、38、39 | same-instance、frozen-copy 或 TEST_ONLY provider 可证明等价合同，不是真实 HA |
| `HA_BLOCKED` | 3-5、15、18、19、21、31、33、34 | 必须由未来 redo apply/provider/single-primary role transition 重新签发生产证据 |

Release JSON 对每项输出实际 evidence mode；`HA_BLOCKED` 项在当前仓库只能报告
`not_executed/blocked`，不能填 0、复用 simulator elapsed 或标 success。

1. Transfer 只传 lock metadata，不传 InnoDB 数据页。
2. Receiver prewarm 不发布 live record lock。
3. Promotion gate 在 service open 前安装所有 adopted token 的锁；full-pressure success profile
   要求所有 expected token 均 adopted。
4. Physical-layout lease 覆盖 metadata-only import、single-primary fencing、角色切换和
   service-open commit，gate 不得提前释放；holder持有期间lease不可撤销；第一版严格要求
   `target_frozen_lsn == source_fence_lsn`。
5. XID/undo/rseg/prepare 和 implicit-lock 合同完整。
6. Metadata-only import 不调用 `buf_page_get` 或 record-image resolver。
7. 现有 page-based 与新 metadata-only import 中，每个 set bit 的 native accounting 都完整且
   可逆。
8. Local startup 和 unsupported artifact 保留现有安全路径。
9. Binlog payload 不再进入 fast-path copyable ready bundle。
10. Native manager/IO_CACHE 不被 memcpy/move object bytes。
11. Handler、prev-position 和 savepoint 顺序正确。
12. Token 关联 native handle，不关联文件名。
13. Attach 在 `ACTIVATING` 前可逆；进入 activation 后不可回退 token，失败时受控 rollback
    成功或保持 pinned `ATTACH_TAINTED`，不得先普通 kill；artifact 只在 `ACTIVE` durable 后清理。
14. Resume 不读、写或 rename binlog payload。
15. 同实例和frozen-copy模式下attach后DML、savepoint、commit、rollback、GTID、compression
    语义正确；任意异构receiver必须拒绝attach。
16. 普通 SQL 权限和可信 HA authorization 明确隔离。
17. OFF/native MySQL 8.0.22 路径不变。
18. Release simulator promotion gate `<=1000000us`，page IO 为 0，且 expected/adopted 数量
    相等、abandoned/skipped/failed/tainted 均为 0。
19. Same-instance/frozen-copy attach profile 的单连接 release P95 和 max 都 `<=100000us`，且
    expected/ACTIVE 数量相等、deadline miss和failure count均为0；pre-drain copy profile不生成
    该证据。
20. Full-pressure batch wall、CPU、内存、FD、tmpdir 和 contention 真实报告。
21. Release 报告明确没有真实物理备机/HA promotion 证据。
22. Strict READY 只有 registry 一个权威，legacy projection 不可独立判 READY。
23. Required apply LSN 为 0、provider/lease 缺失或 null/default executor 均在 claim 前拒绝。
24. Memory/FD/tmpdir admission 和 epoch purge 全部通过 fault tests。
25. OFF、从未开启 epoch、已关闭 epoch三种状态的前台锁吞吐均在 baseline 容差内。
26. 四类 source-shape guard 和对应 runtime behavior tests 均通过。
27. Ordinary supremum lock 使用整页 bitmap sizing，仅执行原生 mode normalization。
28. Receiver restart 发现未终结 strict intent 时阻断 service/promotion，不自动 replay 或本地
    recovery。
29. Release E2E 通过 test-only protected-handle 组件调用真实内部 resume core，而不是只运行
    promotion gate。
30. Gate intent为单个epoch-level durable record，不发生1000次per-token fsync；attach intent
    独立按token记录。
31. `ADOPTED_LOCKED`具有明确client-resume deadline；cleanup与attach通过同一CAS互斥。
32. Detached/adopted trx保持`mysql_thd=null`时，锁、purge和rseg语义通过runtime测试；不引入
    placeholder THD。
33. 报告显式输出physical consistency mode、`real_redo_apply=false`、
    `production_ha_promotion=false`和`real_business_continuation_proven=false`。
34. Canonical digest 在 prewarm 构建，gate 只做固定长度比较；digest/schema/index/final facts
    的 generation 通过单次 release/acquire publication 保持一致。
35. `READY_FACTS_PENDING_LEASE` 有明确转换和过期出口，只有 `READY_FOR_GATE` 投影为 READY。
36. Multi-bit page-based 与 metadata-only import 的 `n_rec_locks` accounting 都与 set-bit 数相等。
37. Full-pressure 1000 token 全部进入 lock-plan/native-binlog 子池，不能用 resource not-ready
    比例换取通过。
38. Activation 前 MDL abort 只释放 THD clone并保留 detached backup；ACTIVE 后才删除 backup。
39. User variables、binlog mode和 cache-state ownership 在 fast attach 中不丢失、不双写。

## 19. 未来 HA 集成接口

未来物理备机产品必须提供：

- source/receiver lineage；
- target boot incarnation；
- final physical-layout fence lease 和生命周期；
- lease holder 持有期间 apply/layout 不可恢复变化的不可撤销合同；
- receiver 已 apply 到兼容物理状态的证明与 schema/index generation；
- redo apply frozen；
- prepared XID/undo continuity proof；
- target server UUID；
- protected target THD；
- logical session 到 token 的映射；
- client-resume deadline 与未恢复 `ADOPTED_LOCKED` 事务的产品清理策略；
- single-primary fencing；
- service-open barrier 调用点；
- 从 gate 到 single-primary role commit 的 lease ownership transfer/release 协议；
- receiver restart 后旧 READY 失效的保证。

Lease 不使用自行到期的 TTL/heartbeat 代替 holder ownership。Holder/coordinator crash 时 provider
保持 apply/layout frozen，直到新的 coordinator 基于 lease id/owner generation 显式 release 或
将实例标记 tainted并重建；不得因心跳超时自动恢复 apply，否则无法保证 import 中途安全。

`target boot incarnation` 是每次 mysqld 启动生成的不可复用随机值，随 READY/fence response
返回给 HA。当前在线模型不要求为它额外 fsync 一个单调计数器；进程重启后值变化即可使旧
READY/handle 失效。

接入后只能调用本设计的 prepared-object 接口。HA 层不得重新解析 lock payload、读取
InnoDB 页面、hydrate binlog sidecar 或维护第三套 resume 逻辑。

在这些 production 合同全部接入前，本仓库只能报告 attach-core 和 frozen-copy 等价验证；
不得把 simulator、pre-drain datadir copy 或TEST_ONLY provider标记为真实业务续作。

## 20. 风险登记册

| 风险 | 严重度 | Owner/边界 | 必须证据或处置 |
|---|---|---|---|
| Production provider 错误证明 page layout | Critical external | 未来 HA/redo apply provider | Provider conformance、frozen-copy 等价测试；仓库报告始终标注非 production proof |
| Metadata-only conflict/accounting 与原生不等价 | Critical in-repo | InnoDB lock helper | shared native predicate、multi-bit accounting RED/GUnit、split/merge/rollback 对称测试 |
| Activation 部分写入后普通 THD teardown破坏 ownership | Critical in-repo | Attach core | 四个 activation fault point；受控 rollback 或 pinned taint，禁止先 kill |
| Detached trx `mysql_thd=null` 的 purge/rseg/lock 缺口 | High in-repo | InnoDB trx/lock | conflict DML、purge/delete-mark inheritance、rseg runtime tests；不引入 placeholder THD |
| Final facts/generation 半发布 | High in-repo | Receiver registry | immutable object release/acquire publication 和 stale-worker race tests |
| Attach/cleanup 同 token 竞争 | High in-repo | Registry | 同一 CAS、full unwind 后才 republish、ownership fault matrix |
| Lock-plan 内存或 gate apply 超出支持包络 | High performance | Resource ledger/gate | closed-form capacity、entry/bit microbench、full-pressure all-ready 与最差轮报告 |
| Native binlog handle 所有权、mode或key漂移 | High in-repo | `sql/binlog.cc` private API | ownership table、mode/incarnation/key checks、savepoint/commit/rollback tests |
| Tainted epoch/token 无自动 quarantine | High operational | HA/operator | service/connection block、持续指标告警、destructive cleanup/rebuild runbook |
| OFF/非 target hot-hook 回归 | High invasive | Warmcopy hook | 双 atomic early return、source-shape guard、OFF/inactive/non-target throughput NFR |
| TEST_ONLY 结果被误报为真实 HA | High evidence | E2E/reporting | consistency-mode labels、build guard、禁止 production provider slot、分 profile 报告 |

风险登记册不是替代 fail-closed 条件。Critical/High in-repo 项未通过对应 RED/GUnit/MTR/E2E 前，
不得进入下一实施阶段；external provider 风险只能由未来产品接口和 release evidence 消除，当前
仓库不得宣称已经解决。
