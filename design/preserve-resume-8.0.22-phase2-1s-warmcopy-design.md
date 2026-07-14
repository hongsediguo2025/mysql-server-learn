# MySQL 8.0.22 Preserve/Resume Drain Phase 2 1s Warmcopy 设计方案

本文档描述 Preserve/Resume 在 `DRAIN TRANSACTIONS PRESERVE` 第二阶段实现
约 1 秒用户可见暂停的系统性设计。它是
`preserve-resume-8.0.22-lock-warmcopy-design.md` 的增强方案，重点解决第一版
lock warmcopy 中仍会把 phase 2 拖长的瓶颈：大锁 payload 仍在 phase 2 生成、
artifact 被重新 materialize、非 record-lock 仍走现场导出、batch target 串行
preserve、snapshot 写入逐事务串行，以及缺少细粒度耗时拆分。

这里的 1 秒是性能 SLO，不是功能失败条件。默认运行模式必须保证：即使 phase 2
超过 1 秒，或者某个 target 退回 live export，Preserve/Resume 仍继续按正确语义完成。
只有在测试/NFR 的 strict SLO 模式下，超过 1 秒才作为测试失败或发布门禁失败。

本文档是目标架构和实施约束，不是当前实现状态说明。当前 8.0.22 preserve-port
代码仍处在 inline payload、串行 seal、串行 per-target preserve、table/MDL live
compare 的形态；后文所有标为“必须”的条款都是进入 1s 发布门禁前需要实现并验证的
条件。

## 1. 背景与当前瓶颈

当前 lock warmcopy 已经把 record lock 的部分采集提前到了两阶段 drain，但它还不是
完整的 phase 1 artifact pipeline。现有关键路径如下：

- `Preserve_trx_lock_warmcopy_drain_participant::open_phase1()` 只打开 epoch，
  清理旧 spill，并没有为 target 预先生成 durable lock artifact。
- `prepare_quiesced_targets()` 在目标事务 quiesced 后采样 live fence，导出 record
  candidate，并把 candidate seed 到 record store；table lock 和 MDL 也是在这里
  取一次 live payload。
- `phase2_preflight()` 串行遍历所有 target，调用
  `lock_warmcopy_record_store_seal_for_target()`，再由后者导出完整
  `record_locks_payload`。
- `spill_artifact_to_file()` 发生在完整 payload 已经生成之后；它降低常驻内存，
  但不能消除 phase 2 的序列化成本。
- `artifact_for_thread()` 发现 spilled artifact 时会 materialize 回内存，后续
  preserve kernel 仍读取 `record_locks_payload` / `table_locks_payload` /
  `mdl_descriptors_payload` 字符串。
- `Preserved_trx_bundle` 仍以内联 TLV 方式保存锁 payload；大 payload 会参与 bundle
  构建、编码、HMAC、snapshot 写入。
- batch preserve 对 `quiesced_target_thread_ids` 串行执行 `batch.run()`；1000 个
  target 意味着每个 target 平均只有 1ms 预算，几乎没有余量。
- warmcopy 路径仍会对 MDL、table lock、savepoint、modified table、privilege 做
  现场导出或最终比较；这些成本在 1000 target 下会被串行放大。
- 当前 `Preserve_trx_bundle` format version 仍是 v8，lock payload 只有
  `record_locks_payload` / `predicate_locks_payload` / `table_locks_payload` /
  `mdl_descriptors_payload` inline TLV；startup recover 和 interactive resume 也直接
  从这些 metadata 字段 import，尚无 lock artifact descriptor hydrate 入口。
- 当前 bundle decoder 在 `apply_bundle_semantics()` 阶段仍要求 inline
  `kTlvMdlDescriptors`；如果只新增 descriptor-only snapshot 而不拆 decode/hydrate
  管线，新格式会在 hydrate 前被判为 corrupt。
- 当前 `preserve_trx_lock_warmcopy_seal_threads` 已暴露但语义是 reserved/serial；
  `preserve_trx_parallel_preserve_threads` 尚不存在。

最近 full warmcopy-only NFR 的 evidence
`build-release/lock-warmcopy-reports/nfr2-full-warmcopy-only-current.json` 显示，
1000 target、每 target 执行一次 100000-row range UPDATE large-lockset workload，
3 轮共 sealed `3000/3000`，但 phase 2 样本仍约
`179.3s / 173.1s / 170.4s`。该 workload 预期制造大量 explicit record X lock，但
“每 target 恰好 10 万显式 record lock”必须以 server-side `record_lock_count` /
payload count 指标为准，不能只由 runner 配置名称推断。这与 release handoff 中的
scaled NFR 不是同一 workload：scaled run 是 16 sessions、小锁集，release handoff
记录 lock warmcopy phase2 p95 约 `30.955ms`，当前 `nfr2-scaled-current.json`
记录约 `23.18ms`；full run 才是本设计要解决的大锁数压力场景。当前 full 结果说明
瓶颈不是 warmcopy 理论本身，而是大对象生产、编码、写入和 target preserve 仍集中在
phase 2。

## 2. 目标与非目标

### 2.1 目标

- 用户可见 `DRAIN TRANSACTIONS PRESERVE` phase 2 暂停 p95 约 1 秒，NFR 中按
  `phase2_total_ms` 衡量。这里的 `phase2_total_ms` 是从 `WARMCOPY_CLOSING`
  导致全局 lock-capable command gate 收紧开始，到目标 snapshot/register 完成的
  blocked business window；shutdown 请求和进程退出另计。
- record locks、table/AUTO_INC locks、MDL transaction-duration tickets 都进入
  phase 1 authoritative lock-object mirror；full 1s 路径不能等到 target
  `QUIESCED` 后才第一次全量扫描锁。
- 保留当前 v1 的 quiesce+freeze 安全边界作为最终 seal/prepare 边界：phase 1
  mirror 负责提前生成可证明的序列化事实，target `QUIESCED` 后只做 bounded tail、
  frozen fence 校验、descriptor adopt 和少量元数据写入。
- full 1s fast path 的 O(lock count) scan、排序、编码、checksum 和 artifact
  write 必须在 phase 1 并行完成；strict phase 2 不允许重新构建大 payload。
- 大锁 payload 不再以内联 `std::string` 形式穿过 phase 2，也不再写入 snapshot
  主体。
- batch target preserve 支持 bounded parallel execution。
- SLO miss 不影响功能正确性；fallback ON 时继续 live export，fallback OFF 时按
  fail-closed/reject 语义执行。
- 所有新增热路径 hook 必须保持低侵入：disabled path 只有轻量分支，enabled path
  只做 O(1) generation / dirty / journal append，不构造大对象、不做 I/O。
- business-running 阶段允许生成 authoritative mirror，但必须满足本文定义的
  `Hook CS + Fence`、base-scan/delta linearizability、crash ordering 和
  cross-family epoch 约束；无法证明时只能作为非权威预热，并把 target 标为
  `SLO_NOT_GUARANTEED`。

上述目标只有在以下前置能力全部落地后才能宣称达成：

- external lock artifact 可被 encode、decode、recover、interactive resume 和 cleanup；
- external descriptor-only snapshot 有明确的 versioned decode/hydrate 管线，且旧 v8
  inline snapshot 仍可读取；
- record/table/MDL/savepoint 都有可证明的 generation/fingerprint 或等价 fence；
- phase 1 mirror 的 base scan、delta journal、external segment prewrite 能证明同一
  target epoch 内线性化，且 phase 2 tail 有明确预算；
- all-or-live fallback 不再混用不同时间点的锁族 payload；
- target preserve worker 具备独立 THD/current_thd 执行上下文或 preserve kernel 已去除
  共享 owner THD context switch；
- NFR 报告具备 phase2 breakdown，并在 full workload 上给出不少于 3 轮样本。

### 2.2 非目标

- v1 不支持 `LOCK TABLES`、`GET_LOCK()`、global read lock、backup lock、HANDLER
  等会话级或全局状态 preserve。
- v1 不为 R-tree/spatial predicate locks 做 lock warmcopy 优化。遇到该族时：
  fallback ON 走现有 live export；fallback OFF reject；该 target 不承诺 1s SLO。
  观测口径不能假定当前实现一定报 `UNSUPPORTED_FAMILY`；如果 predicate candidate
  在 record-store seed 阶段变成 `ARTIFACT_INVALID`，报告必须把它归因到 spatial
  predicate unsupported，而不是静默归为普通 artifact invalid。
- v1 不改变 InnoDB 原生 lock ownership，也不把运行时 `lock_t` 或 MDL ticket
  对象搬到磁盘。磁盘上只保存 Preserve/Resume 需要的序列化信息对象。
- 不在业务运行阶段执行锁语义动作：lock warmcopy 只维护锁信息对象的增删改镜像，
  不调用业务语义上的 lock/unlock，不改变 InnoDB 原生 lock ownership。
- 不接受未证明线性化的 live journal。若 hook 与原 mutation 不在同一临界区，或
  base scan/delta replay 不能证明覆盖，则该 target 不能进入 full 1s fast path。

## 3. 核心设计原则

### 3.1 SLO 与功能正确性分离

1 秒是性能门禁，不是运行时语义门禁。默认模式下：

- phase 2 超过 1 秒：继续执行，记录 `phase2_slo_miss_count` 和慢阶段明细；
- artifact 校验失败且 fallback ON：丢弃该 target 的 warmcopy artifact，走 live
  export，功能继续；
- artifact 校验失败且 fallback OFF：若还未进入 prepare，则 fail closed/reject；若已经越过
  prepare/detach/snapshot durable point，则按 preserve kernel 的 rollback/taint/cleanup
  语义收敛，不进入不可靠状态；
- strict SLO 测试模式：超过 SLO 时测试失败，但服务端路径仍不能为了 SLO 破坏功能。

### 3.2 保留 Quiesce+Freeze 作为最终 Seal 边界

full 1s 路线不能把 quiesce 当成第一次全量构建 artifact 的开始点。quiesce 仍然是
正确性边界，但它的职责应降级为最终 seal：确认 phase 1 mirror 产生的事实仍可用，
处理 bounded tail，并进入 prepare。这样可以同时满足两类约束：

- target 自身不再执行 DML、savepoint、MDL/table/record-lock 变异；
- 其他会话触发的 implicit-to-explicit conversion 由 conversion freeze/fence 发现并
  fallback/reject；fallback OFF 或 strict fail-closed 场景才 reject。

因此本文的“phase 1 生产事实”在 full 1s 路线中的准确含义是：业务仍运行时已经创建
authoritative lock-object mirror，并将大 segment 并行写入 external artifact。strict
phase 2 的职责只剩：

```text
close admission
wait in-flight supported mirror work
apply bounded tail delta
freeze implicit-to-explicit conversion
sample final fence / generation after freeze
validate checksum / lock count / family eligibility
adopt prebuilt external artifact descriptor
run per-target prepare/detach/snapshot/register
```

如果 phase 2 仍需要对 1000 * 10 万锁重新导出、排序、编码、HMAC 或写入主
snapshot，就不可能达到 1 秒。

record family 的最终 fence 顺序必须是 `freeze -> sample/compare -> prepare`。不能先把
artifact fence 与当前 fence 比对成功，再设置 conversion freeze；否则其他会话可能在两者之间
触发 implicit-to-explicit conversion，导致 artifact 已过期而 frozen fence 仍保持稳定。

### 3.2.1 Phase 1 权威 Mirror 契约

phase 1 mirror 不是“提前 lock/unlock”，而是维护锁信息对象的增删改镜像。每个
family 都必须有稳定 object identity、mutation generation、deterministic serialization
和 final fence。最小契约如下：

- base scan 与每个 lock mutation 线性化；
- hook append 与原 mutation 在同一临界区，或有等价 fence 能证明无丢失/重排；
- record/table/MDL/savepoint 四个 family 使用同一 target snapshot epoch；
- seed 前 delta、scan 中 delta、scan 后 tail delta 都有确定 replay 顺序；
- crash/orphan cleanup 与 snapshot commit ordering 明确；
- 性能门禁单独度量 live hook enabled path 的开销。

record family 的 object identity 至少包含 `{trx_id, table_id, index_id, space_id,
page_no, heap_no, lock_type_mode, record_image_digest 或等价 identity}`。现有
`lock_warmcopy_record_shard_key_t` 是 `{table_id,index_id,space_id,page_no,
lock_type_mode,n_bits}` 的 shard/bitmap 分区 key，不等同于 v2 的 durable object
identity；`heap_no` 和 `record_image_digest` 当前是 shard 内 entry 信息。v2 不能把
现有 shard key 直接宣称为 object key，必须二选一：

1. 保留 shard-keyed store 作为物理分区，新增 shard 内 entry identity，并让
   deterministic serialization/fingerprint 按 `{shard_key without volatile capacity,
   heap_no, lock_type_mode, record_identity}` 排序；
2. 迁移到 record-keyed store，把每个 record lock bit 作为独立 object 维护。

`n_bits` 只能作为 bitmap capacity/version 诊断或 shard shape 校验，不能成为跨 page
重组、lock move 或 restore 语义下的 durable record identity。table、AUTO_INC、MDL、
savepoint family 也必须定义自己的 object identity 和 generation。hook 只能做 O(1)
delta append、generation bump、dirty/invalid 标记；record image 解析、排序、segment
编码、checksum 和文件 I/O 必须由后台 builder 在线程池中完成。

base scan 的推荐模型是 per-shard cursor + journal seq：

```text
publish target epoch
open per-family journal admission
base scan shard N at base_seq
append mutations under Hook CS with seq > base_seq
replay delta in seq order
write deterministic segment
record segment_generation / fingerprint / checksum
```

如果出现 seq gap、dirty shard 无法收敛、unsupported shape、hook failure、预算超限或
cross-family epoch 不一致，该 target 不能进入 `SLO_GUARANTEED`；fallback ON 仍可走
live export，fallback OFF 按 reject/cleanup 语义处理。

Hook CS 的可发布定义必须比“调用了 hook”更强。当前代码中的 record bitmap hook 在
native bitmap 已 mutation 后再进入独立 `lock_warmcopy_record_store_mutex`，这只能作为
功能候选/诊断基础，不能证明 business-running mirror 的线性化。v2 `SLO_GUARANTEED`
路径必须满足：

- hook admission 在 target/family epoch 打开后可观测，close 后不再接收新 delta；
- 每个 hook 在 native lock mutation 的同一临界区内写入 delta，或在同一临界区内 bump
  native generation 并留下可被 base scan/final fence 证明的 dirty/invalid 标记；
- 若 hook 需要 warmcopy shard latch，锁顺序必须固定为 native lock mutation latch
  -> warmcopy target/shard latch，并禁止任何反向获取；
- close/adopt 前必须等待 `inflight_hook_count == 0`，且等待受 drain deadline、
  KILL/shutdown/cancel 控制；
- 如果只能做到 mutation 后异步补 delta，而无法证明无丢失/重排，则该 family 只能作为
  非权威预热，target 进入 `SLO_NOT_GUARANTEED`。

### 3.2.2 Quiesced-Only 路线的 SLO 限制

如果实现不做 phase 1 authoritative mirror，而是等 target `QUIESCED` 后才全量扫描
1000 * 10 万锁并生成 artifact，那么它只能作为功能正确路线或中小锁集优化。external
artifact 可以减少 snapshot inline、HMAC 和堆内存压力，但不能消除 blocked window 内的
O(lock count) scan、排序、编码、checksum 和写盘。该路线必须报告
`SLO_NOT_GUARANTEED`，不得宣称 full workload 达成 1s。

### 3.3 大对象外部化

所有可能超过小阈值的 lock payload 都必须变成外部 artifact。它不能复用当前只为
binlog warmcopy 预构建 blob 开放的 adopt 路径；lock artifact 需要自己的 carrier
contract：

```text
snapshot TLV:
  lock artifact descriptor only

external artifact:
  record/table/MDL family segments
  deterministic order
  length
  checksum
  target identity
  epoch
  final fence
```

inline 阈值建议固定为 64KB。超过阈值必须 external，不允许 phase 2 重新
materialize 为 `std::string` 再写入 snapshot。

当前 `spill_artifact_to_file()` 只能视为 drain epoch 内临时 spill：它在 preserve 前会被
`artifact_for_thread()` 重新 materialize，并且不会作为跨重启事实源。1s 方案必须新增
token-scoped durable lock artifact，而不是把当前 spill 文件改名当成可恢复 artifact。

durable lock artifact 必须有独立 magic 和 parser dispatch，例如 `LWCDV1`/`LWCDM1`
这类新格式。不得扩展当前 `LWCSPV1/LWCSMF1` 临时 spill magic 来承载可恢复事实源，
因为当前 parser 假设单 segment、四个大 payload 串行打包，并且 preserve 前会全量
materialize 回 `std::string`。复用该 magic 会让 v1 spill parser 误读或拒绝 v2 durable
artifact。

### 3.4 全锁族 warmcopy

只优化 record lock 不够。目标是 current preserve parity 下所有已支持锁族都纳入
warmcopy：

- explicit record/gap/next-key/insert-intention locks；
- implicit-to-explicit conversion 产生的 explicit record lock；
- InnoDB table locks 和 AUTO_INC state；
- MDL transaction-duration descriptors；
- SQL/InnoDB savepoint 与 MDL ordinal 的 fence 校验。

## 4. 新的数据模型

### 4.1 Lock Artifact Descriptor

snapshot 中新增 lock artifact descriptor TLV，并 bump snapshot format version。descriptor
是小对象，必须能在 phase 2 常驻内存：

```text
format_version
epoch
target_thread_id
target_trx_id
family_mask
artifact_id
segment_count
total_payload_bytes
total_lock_count
record_lock_count
table_lock_count
mdl_descriptor_count
savepoint_count
final_fence_digest
checksum_algorithm
checksum
flags
```

`flags` 至少包含：

- `HAS_RECORD`
- `HAS_TABLE`
- `HAS_MDL`
- `HAS_SAVEPOINT_FENCE`
- `HAS_EXTERNAL_SEGMENTS`
- `SLO_GUARANTEED`
- `FALLBACK_USED`
- `UNSUPPORTED_FAMILY_SEEN`

descriptor 只描述 artifact，不包含大 payload。

实现要求：

- 为 lock artifact 分配独立 TLV 类型，不能复用现有 `kTlvRecordLocks` /
  `kTlvTableLocks` / `kTlvMdlDescriptors` 的 inline 语义。
- bundle decoder 必须同时支持当前 v8 inline snapshot 和新 external-lock snapshot。
- decoder 必须拆成三段：
  1. structural decode：只校验 header、CRC/HMAC、TLV 格式和 version range；
  2. descriptor hydrate：识别 lock artifact descriptor，读取并校验 external artifact，
     填充 metadata lock payload view 或等价 lazy view；
  3. versioned semantic validation：再执行 MDL descriptor、record/table/predicate
     payload、savepoint 等语义校验。
- 新格式读入后必须在语义 import 之前 hydrate lock artifact，填充或替代当前
  `Preserve_snapshot_metadata` 中的 lock payload 视图。
- 新格式不能沿用当前 `kRequiredTlvs` 对 `kTlvMdlDescriptors` 的硬要求；v8 inline
  snapshot 继续要求 inline MDL TLV，新 external-lock snapshot 则由 descriptor hydrate
  后提供等价的 MDL metadata view。
- descriptor 与 inline payload 同时存在时，只有在 debug/compat 指定路径下允许；
  默认必须拒绝不一致或来源不明确的 snapshot。
- startup recover、interactive resume、`INFORMATION_SCHEMA`/status 显示、taint/cleanup
  都必须使用同一套 descriptor 解析和校验逻辑。
- snapshot format bump 是向前兼容，不是 downgrade 兼容。旧二进制读到新 external-lock
  snapshot 会拒绝更高 format version；发布文档必须明确 downgrade 禁止，或在启动时
  提供阻止旧二进制误删/rollback 新格式 snapshot 的保护策略。

### 4.2 Segment Layout

artifact 文件由 header、segment manifest、segment body 构成：

```text
file_header:
  magic
  format_version
  datadir_fingerprint
  server_uuid
  epoch
  target_thread_id
  target_trx_id
  segment_count
  manifest_length
  manifest_checksum

segment_manifest_entry:
  family
  ordinal
  offset
  length
  entry_count
  generation_start
  generation_end
  checksum

segment_body:
  deterministic payload bytes
```

每个 segment 必须支持独立 checksum 校验。durable lock artifact 的 header、manifest
和 segment body checksum 默认使用 SHA-256 或同等强度算法；FNV1a-64 这类临时
checksum 只能保留给 drain 内 spill 诊断，不得作为可恢复事实源的完整性依据。phase 2
adopt 只校验 manifest 和尾部 segment 的 checksum，不重新读取全量 body。resume/import
才读取 segment body。

与当前 spill 格式的关系必须硬切开。当前已有 `LWCSPV1` segment 和 `LWCSMF1`
manifest，但它们是临时 spill 格式：body 中仍按四个大 payload 串行打包，并在 preserve
前 materialize 回内存。v2 external artifact 必须新增独立 durable magic/parser，例如
`LWCDV1/LWCDM1`，并保留当前 spill 仅作为 drain 内临时 fallback。

不能同时维护两套语义相近但生命周期不同的 durable lock artifact 格式。代码必须明确当前
`LWCSPV1/LWCSMF1` 永远不是 recover/resume 事实源；v2 durable parser 也不得接受
`LWCSPV1/LWCSMF1` 作为可恢复 artifact。

artifact 生命周期：

```text
phase1 builder:
  create temp artifact under preserve trx artifact dir
  write/patch/append segments
  fsync according to durability policy

phase2 adopt:
  verify owner marker / datadir fingerprint / server_uuid / epoch / target id
  atomically rename temp artifact to token-scoped durable artifact
  fsync artifact parent directory
  write descriptor into snapshot

recover/resume:
  read descriptor
  verify durable artifact exists, size/checksum/manifest match
  hydrate required family body before import

cleanup:
  remove durable lock artifact only with its snapshot token
  remove temp/stale artifact only when owner marker matches this instance
```

owner marker 至少包含 datadir fingerprint、server UUID、pid、startup epoch 和 artifact
kind。pid/startup epoch 只用于 temp/stale artifact 的 owner 判定，不得作为 durable
token-scoped artifact 的删除条件。cleanup 不得删除 foreign owner 的 artifact，但也不能把
同一 datadir/server UUID 的 dead prior startup epoch 永久视为 foreign。清理规则必须区分：

- live foreign instance：datadir/server UUID 不匹配，或 owner marker 显示仍属于另一个
  活跃 mysqld；不得删除；
- dead prior epoch：datadir/server UUID 匹配，pid/startup epoch 已确认不属于活跃实例；
  可按 temp/stale artifact cleanup 回收；
- durable snapshot token 仍引用的 artifact：无论 owner epoch 如何，都只能由该 token
  的 snapshot cleanup/recover 逻辑处理。

snapshot 与 artifact 的 commit ordering：

- temp artifact write/fsync 成功后才能生成 descriptor；
- durable rename/fsync 成功后才能写 snapshot descriptor；
- snapshot write 明确返回“snapshot 不存在”失败时，必须删除本 token 的 durable artifact；
  删除失败则记录 orphan，并在下次 cleanup 只按 owner marker 回收；
- snapshot write 返回 `durable_snapshot_may_exist` 或等价不确定状态时，不能删除
  durable artifact。必须保留 artifact，记录 indeterminate handoff，recover/resume 时按
  snapshot descriptor 与 artifact checksum 重新 reconcile；若 snapshot 最终不存在，再由
  token/owner cleanup 回收 orphan；
- snapshot 已成功但 artifact 缺失/损坏时，recover/resume 必须 fail closed，不得回退到
  stale inline payload。

phase 1 in-flight segment 的 cleanup 规则必须单独定义，不能只复用 phase 2 durable
artifact cleanup。phase 1 builder 可能在 base scan、delta replay、segment write、fsync
或 rename 期间被 KILL/shutdown/crash 打断；这些 temp segment 尚未被 snapshot token 引用。
发布门禁要求：

- temp segment 路径必须带 owner marker、startup epoch、target epoch、target id 和
  artifact kind；
- cleanup 只能删除本实例 live owner 或已确认 dead prior epoch 的 temp/stale artifact；
- dead prior epoch 的 liveness 检测必须使用 pid + startup epoch + owner marker 文件，
  不能只靠 datadir/server UUID；
- 已 rename 为 token-scoped durable artifact 的文件，即使 owner epoch 已死，也只能由
  snapshot token reconcile/cleanup 处理；
- cleanup 失败必须可观测为 orphan，不能静默删除 foreign owner 文件。

lock external artifact 的 carrier/adopt 还必须给出锁顺序图。默认顺序为：

```text
target THD pin / batch target state
  -> lock warmcopy target artifact descriptor lock
  -> binlog warmcopy provider descriptor lock
  -> carrier token/adopt lock
  -> global preserved-record list lock
```

任何实现若需要不同顺序，必须同时更新死锁测试；并行 preserve、binlog warmcopy adopt、
lock artifact adopt、snapshot write 和 token register 不能各自发明局部锁顺序。

### 4.3 Family Payload

family payload 继续使用现有 import 能识别的逻辑格式，但物理承载从 snapshot inline
变为 external segment。

- record segment：等价于现有 `record_locks_payload`；
- predicate segment：v1 只由 fallback live export 生成，不走 warmcopy 优化；
- table segment：等价于现有 `table_locks_payload`；
- MDL segment：等价于现有 `mdl_descriptors_payload`；
- savepoint fence segment：保存 SQL savepoint ordinal 与 InnoDB savepoint count 的
  final validation 证据，小对象可 inline 到 descriptor。

resume 兼容策略：

- 新格式 snapshot：先读 descriptor，再按 descriptor 读取 external lock artifact，在
  `trx_preserve_import_table_locks()`、`trx_preserve_import_record_locks()`、
  `create_detached_mdl_context()`、savepoint restore 前完成 hydrate；
- 旧格式 snapshot：继续读取 v8 inline payload；
- descriptor 与 inline 同时存在且不一致：fail closed，并标记 snapshot tainted；
- external artifact 缺失、checksum 不匹配、segment 越界、family count 不一致：
  startup recover rollback preserved snapshot；interactive resume 返回明确错误。

## 5. Phase 1 流程

### 5.1 Target Discovery

进入 `WARMCOPY_DRAINING` 后立刻枚举候选 target，而不是等 quiesced 后才第一次
准备锁候选。当前代码的实际顺序是 `open_phase1_participants()` 先打开 epoch，
随后进入 `WARMCOPY_CLOSING` 才执行 target counter/quiesce，最后才调用
`prepare_quiesced_targets()`。因此本节是 orchestrator 重构要求，不是当前实现状态。
full 1s 路线必须在这里发布 target epoch 并打开 lock-object mirror admission；若实现只做
候选集合、容量、预算和非权威预热，则只能进入 `SLO_NOT_GUARANTEED` 路线。

```text
open_phase1:
  allocate epoch
  discover drainable target candidates
  publish epoch admission
  create per-target estimate/budget record
  open per-target/per-family mirror store
  start background segment builder
```

如果 phase 1 期间发现新的 drainable target，可按下一轮 drain 处理；v1 不要求动态
扩展 target 集合，因为稳定集合更容易证明 1s SLO。

重构边界：

- target discovery、batch capacity check、per-account capacity check 必须提前到 phase 1；
- phase 1 target set 一旦发布，后续 quiesce 阶段只能缩小集合，不能隐式新增 target；
- target thread id 不能作为唯一长期身份，builder key 必须至少包含 drain epoch 和
  target trx id/fence，防止连续 drain 污染；
- 如果无法提前证明 target eligibility，必须把该 target 标为 `SLO_NOT_GUARANTEED`，
  但功能路径仍可在 phase 2 使用现有 live export。

### 5.2 Authoritative Lock-Object Mirror

phase 1 mirror 的输入是锁信息对象的增删改，而不是 lock/unlock 语义。每个 target 至少有
四个 family store：

```text
record_store[target_epoch]:
  object_key -> {mode, record_identity, generation, tombstone}

table_store[target_epoch]:
  object_key -> {table_id/name, lock_mode, autoinc_state, generation, tombstone}

mdl_store[target_epoch]:
  object_key -> {namespace, key, type, duration, ordinal, generation, tombstone}

savepoint_store[target_epoch]:
  object_key -> {sql_ordinal, innodb_count, mdl_ordinal_anchor, generation}
```

mutation hook 必须和原 mutation 在同一临界区或等价 fence 下更新 mirror：

- add/update：写 object UPSERT delta，bump family/shard generation；
- remove：写 tombstone delta，bump generation；
- move/downgrade/range-delete：优先表达为 object update；无法精确表达时 mark family
  dirty/invalid；
- implicit-to-explicit conversion：写 explicit record delta，并更新 implicit exclusion
  generation；若发生在 seal 后则 target invalid；
- rollback/savepoint barrier：能精确表达 range-delete 才 replay，否则 dirty 对应 family。

后台 builder 对每个 family 并行执行 base scan + delta replay：

```text
for each target/family/shard:
  capture base cursor and base_seq
  scan current objects into sorted segment buffer
  replay journal seq > base_seq
  if seq gap or unsupported mutation: mark shard invalid
  write segment.tmp
  fsync segment.tmp
  rename segment.tmp -> segment
  fsync parent dir
  publish segment descriptor, generation, fingerprint, checksum
```

segment 写盘在 phase 1 完成。phase 2 不得重新生成完整 payload，也不得把 segment
materialize 为 `std::string` 后再写 snapshot。若 phase 1 builder 落后，phase 2 只能处理
配置允许的 bounded tail；超出 tail budget 时 target 标记 `SLO_NOT_GUARANTEED`，功能路径
仍可 fallback。

### 5.3 Bounded Quiesced Tail Seal

target 进入 `QUIESCED` 后，不应再第一次执行 O(lock count) artifact build。此阶段只允许
对 phase 1 mirror 做尾部封口：

- 等待 in-flight hook append barrier 归零；
- replay phase 1 builder 未处理的小 tail delta；
- freeze record implicit-to-explicit conversion；
- sample frozen fence/generation；
- 校验 external artifact descriptor、segment checksum、lock count、family eligibility；
- 采集 modified table list、privilege precheck 所需的小型 metadata snapshot。

这段工作处在用户可见 drain pause 中，因此必须纳入 `phase2_total_ms` 或单独的
`tail_seal_ms` 指标；不能因为它发生在 `phase2_preflight()` 之前就从 SLO 统计中隐藏。
如果此阶段仍需要全量扫描 1000 * 10 万锁，说明 phase 1 mirror 没有完成，不能进入 full
1s fast path。

tail seal 的安全规则：

- target 自身不得再执行 DML、SAVEPOINT、ROLLBACK TO SAVEPOINT、MDL/table/record-lock
  变异；
- record family 仍必须处理其他会话触发的 implicit-to-explicit conversion，依赖
  conversion freeze/fence；
- record base scan 必须有明确的 lock queue 稳定性契约。`SLO_GUARANTEED` 路径至少
  需要以下二选一：
  1. 在 record base scan、record fence sample 和 artifact descriptor seal 的关键窗口持有
     `locksys::Global_exclusive_latch_guard` 或等价的全局 lock_sys 排他保护；
  2. 使用 per-shard consistent snapshot + per-shard generation/fingerprint，能证明 scan
     期间 gap-lock inheritance、lock move、discard、implicit-to-explicit conversion 等
     lock queue 变化均被覆盖。
  如果两者都不能证明，只能把该 target 标为 `SLO_NOT_GUARANTEED`，并通过 final
  fence mismatch 走 fallback/reject，不能声称 1s fast path 成功。
- table/MDL/savepoint 若尚无 phase 1 mirror generation hook，必须保留 live export +
  final fence compare，不得声称已进入 1s SLO fast path；
- fingerprint 必须覆盖 entry identity、lock mode/type、record image digest 或等价
  identity、MDL duration-list order、savepoint ordinal；
- final fence 不匹配时，fallback ON 在 prepare 前整 target live export；fallback OFF
  fail closed。

### 5.4 Delta Scope

full 1s 路线必须支持业务运行阶段的 authoritative delta journal，但只限于可线性化的
object-level delta。受支持 delta 来源包括：

- phase 1 mirror 的 record/table/MDL/savepoint object UPSERT/DELETE/UPDATE；
- bounded quiesced tail replay；
- 当前生产路径主要是 quiesced
  `prepare_quiesced_targets()` seed，现有 journal upsert/patch/delete API 仍主要是
  unit-test/helper 形态，不等于已完成生产 mirror；
- engine-side implicit-to-explicit conversion attempt 只能触发 fence invalidation、
  wait/retry 或 explicit record delta；若无法证明 replay，则 fallback/reject；
- debug/fault injection；
- 明确标记为 dirty/invalid 的 unsupported mutation。

delta 至少覆盖以下类型：

```text
record:
  UPSERT_BIT
  RESET_BIT
  UPSERT_RECORD_IMAGE
  IMPLICIT_TO_EXPLICIT
  INVALIDATE_SHARD

table:
  ADD_TABLE_LOCK
  REMOVE_TABLE_LOCK
  UPDATE_AUTO_INC
  INVALIDATE_TABLE_FAMILY

MDL:
  ADD_TICKET
  REMOVE_TICKET
  MOVE_DURATION
  DOWNGRADE_TYPE
  INVALIDATE_MDL_CONTEXT

savepoint:
  CREATE_SAVEPOINT
  RELEASE_SAVEPOINT
  ROLLBACK_TO_SAVEPOINT_BARRIER
```

delta append 必须与原 mutation 在同一临界区或可证明等价的 fence 下完成；hook 失败、
sequence gap、unsupported mutation 都将 target 标记为 `dirty_or_invalid`。dirty target
仍可 fallback/live export 完成功能，但不能作为 full 1s fast path 成功样本。

`wait in-flight hook append` 必须是可实现的等待契约，而不是仅靠后续 fence mismatch
概率性发现。当前 close epoch + sample fence 能把迟到 hook 降级为 invalid/fallback，但这
不能算作 1s fast path 的 wait 证明。v2 若声称 SLO guaranteed，必须维护 in-flight hook
计数或等价 barrier，并在 close/adopt 前等待其归零。

### 5.5 External Build 与 Phase 1 Prewrite

full 1s 路线必须做业务运行阶段的 authoritative background merge。优化重点是：在
`WARMCOPY_DRAINING` 期间就把大 payload 变成 external artifact segment，target quiesced
后只做 tail/fence/adopt：

```text
for each target:
  scan/mirror lock facts under Hook CS + Fence
  replay deterministic delta
  write deterministic external segments
  fsync according to durability policy
  update builder generation and checksum
```

这样 strict phase 2 只处理最后一个很短的 tail/fence/adopt。若 phase 1 builder 未完成、
tail 超过配置阈值，或需要 quiesced 后重新全量构建，target 标记 `SLO_NOT_GUARANTEED`，
但功能仍继续。

任何 O(lock count) build 时间都必须被观测。若它发生在 command 已经阻塞业务之后，
就必须计入用户暂停预算；如果必须全量 live export，必须显式走 fallback 并报告
`LIVE_FALLBACK_USED`。

## 6. Phase 2 流程

### 6.1 总体顺序

本节是目标顺序，不是当前源码顺序。当前 8.0.22 preserve-port 是先进入
`WARMCOPY_CLOSING`，再 target counter/quiesce，再调用 `prepare_quiesced_targets()`，
最后才 `close_phase1_participants()` 和 `phase2_preflight_participants()`。v2 可以重构
顺序，但必须在文档、代码和指标中清楚区分“当前顺序”和“目标顺序”。

```text
WARMCOPY_DRAINING / phase 1:
  discover target set
  open per-target lock-object mirror
  scan base objects with cursor/seq
  replay mutation delta
  write external artifact segments
  publish descriptor generation/fingerprint

WARMCOPY_CLOSING:
  block global lock-capable command admission
  publish target set / shrink only
  wait targets reach QUIESCED
  close lock warmcopy admission
  wait in-flight hook append by explicit barrier

parallel seal:
  apply bounded tail delta only
  freeze implicit-to-explicit conversion
  sample final generation/fence after freeze
  validate descriptor/manifest/checksum/counts
  adopt artifact descriptor

parallel preserve:
  run ha_prepare_low / temp prepare
  detach and claim trx
  write small snapshot descriptor
  register preserved token

shutdown:
  finalize participants for shutdown
```

### 6.2 Phase 2 严禁做的事

为了 1s SLO，phase 2 fast path 禁止：

- 在 target quiesced 后第一次执行 full lock-object base scan；
- 全量遍历 record lock store 并生成完整 payload；
- 把 external lock artifact materialize 回内存；
- 将大 record/table/MDL payload inline 到 snapshot TLV；
- 串行处理 1000 个 target 的 seal；
- 串行处理 1000 个 target 的 prepare/detach/snapshot/register；
- 在正常 warmcopy fast path 执行 canonical live export compare；
- 因 SLO miss abort 功能路径。

debug/test 模式可以开启 canonical compare，但必须标记为 `slo_not_applicable`。

当前源码仍违反这些 fast path 约束：`phase2_preflight()` 串行 target seal，
`artifact_for_thread()` 会 materialize spill，snapshot metadata 仍接收 inline lock
payload。实施时必须先删除这些 phase2 大对象路径，再开启 strict SLO 门禁。

如果任何 O(lock count) artifact build 发生在 command gate 已阻塞业务之后，它也属于
用户可见 pause，并使 full 1s fast path 失效。strict SLO 不能只统计
`phase2_preflight()` 函数耗时；必须统计从 `WARMCOPY_CLOSING` 阻塞全局
lock-capable command 开始，到 snapshot/register 完成的 blocked business window。
shutdown 请求、进程退出和 OS teardown 作为独立 `shutdown_ms`/`post_preserve_shutdown_ms`
报告，不能混入 1s handoff SLO，也不能被忽略。

### 6.3 Tail Delta Budget

新增配置建议：

```text
preserve_trx_lock_warmcopy_phase2_tail_max_bytes = 4MB
preserve_trx_lock_warmcopy_phase2_tail_max_records = 8192
preserve_trx_lock_warmcopy_phase2_tail_max_ms = 100
```

超过 tail budget 时：

- fallback ON：继续 live export 或慢 seal，功能继续，记录 `SLO_NOT_GUARANTEED`；
- fallback OFF：若尚未进入 prepare，则 reject；若已经越过 prepare/detach durable
  boundary，只能按 preserve kernel 的受控 rollback/taint/fail-closed 语义收敛，不能再
  声称“prepare 前 reject”；
- strict SLO test：报告失败。

tail budget 的用途不是限制总事务大小，而是限制 phase 2 还需要处理的剩余变化量。
例如 phase 1 已经写好 10GB lock artifact，phase 2 只剩 2MB delta，则仍可进入 fast
path；反之即使总锁量只有几百 MB，只要 phase 2 需要重新扫描/重编码全量，就不能通过
strict SLO。

## 7. 并行化设计

### 7.1 Worker Pool

新增两个 bounded worker pool：

```text
lock_seal_pool:
  用于 phase2 artifact seal/adopt

target_preserve_pool:
  用于 per-target ha_prepare_low/detach/snapshot/register
```

线程数配置：

```text
preserve_trx_lock_warmcopy_seal_threads = 0
preserve_trx_parallel_preserve_threads = 0
```

`0=auto`，建议：

```text
auto = min(64, max(8, cpu_count * 4))
```

若 CPU/IO 压力过高，可手动降低。

当前 `preserve_trx_lock_warmcopy_seal_threads` 只是 reserved/serial 配置；在实现
lock seal pool 之前，任何 full NFR 结果都只能说明串行实现的表现。新增
`preserve_trx_parallel_preserve_threads` 前，用户手册和 release handoff 必须明确该参数
尚不可用。

`lock_seal_pool` 的前置条件：

- record store 不能在一个全局 mutex 内完成完整 payload 导出；
- seal 必须按 target 或 shard 拆分锁粒度，或在锁外构建 descriptor/segment；
- 每个 worker 输出只读 descriptor，不返回大 `std::string` payload；
- cancel 时必须等待所有 in-flight seal worker 收敛，清理其 temp artifact。

当前 `lock_warmcopy_record_store_seal_for_target()` 在全局
`lock_warmcopy_record_store_mutex` 下检查 fence 并调用
`export_record_payload_from_store_locked()` 构建完整 payload。若不先拆分这个锁或把大对象
构建移到锁外，增加 seal worker 只会排队争用同一 mutex，不能作为 1s SLO 的并行化证据。

推荐分区方案：

- `lock_warmcopy_record_store_mutex` 只保留为 epoch/target registry 的短临界区锁，不再
  覆盖 payload export；
- 每个 target store 拥有独立 mutex，或按 `{target_id, shard_id}` stripe 到固定数量的
  shard mutex；
- hook 只在对应 target/shard 分区内做 O(1) bitmap/generation/dirty 更新；
- seal worker 在短锁内冻结 target store view、复制 shard descriptor/generation 指针或
  轻量 metadata，随后释放分区锁，在锁外进行 record image 排序、segment 编码、SHA-256
  checksum 和文件写入；
- final fence 比较必须覆盖分区 generation/fingerprint，证明锁外编码期间该 target store
  没有变化；若变化，target 进入 fallback/reject。

如果实现仍保留单一全局 mutex 包围 `export_record_payload_from_store_locked()`，则
`preserve_trx_lock_warmcopy_seal_threads > 1` 只能视为 reserved 配置，不能进入
`SLO_GUARANTEED` 发布口径。

### 7.2 Per-Target Isolation

并行 preserve 的安全边界：

- 一个 target 只能被一个 worker 处理；
- worker 获取 target THD pin 后才能运行 preserve kernel；
- worker 必须拥有独立可用的 THD/current_thd 执行上下文，或者 preserve kernel 必须先
  重构为不依赖共享 drain owner THD 的 `Preserve_thd_context_switch`；
- target 的 artifact descriptor 只读；
- shared result vector、preserved token list、error state 通过 mutex 保护；
- 第一个不可恢复错误触发 cancel token，但已越过 durable point 的 target 必须按现有
  cleanup/observable 语义收敛。

当前串行代码会把 drain owner THD 的 globals/thread_stack 临时切到 target THD。这个
机制不能被多个 worker 共享使用。并行 preserve 的发布门禁必须包括：

- worker THD 的创建、初始化、权限/安全上下文、MEM_ROOT 生命周期；
- target THD pin 与 `THD::release_resources()` / disconnect / KILL / shutdown 的等待协议；
- target batch state 从 `QUIESCED` 到 `ATTACHING` 到 terminal state 的 CAS 或锁保护；
- snapshot write、token register、observable record 写入的并发成功/失败收敛；
- binlog warmcopy provider、carrier、global preserved record list 的锁顺序文档化。

### 7.3 降级路径

并行 preserve 不能牺牲功能正确性。以下情况自动降级为串行 preserve：

- 当前 THD manager pin 机制无法证明 target ownership；
- debug/fault injection 要求 deterministic order；
- worker pool 创建失败；
- 检测到全局对象需要串行持有；
- release owner 配置 `preserve_trx_parallel_preserve_threads=1`。

降级后功能继续，但记录 `parallel_preserve_disabled` 和 SLO miss。

降级为串行 preserve 时，不得宣称 full workload 达成 1s SLO。串行降级只保证功能
正确性和可观测的 `SLO_NOT_GUARANTEED`。

## 8. 非 Record-Lock Warmcopy

本节描述目标能力，不是当前实现。当前代码对 table、MDL、savepoint 主要仍是
one-time snapshot + live/final compare：这条路径保证正确性，但不能作为 1s fast path
成功口径。只有 generation/fingerprint hook 落地并通过等价测试后，相关 family 才能
纳入 `SLO_GUARANTEED`。

### 8.1 Table / AUTO_INC

full 1s 路线中，table/AUTO_INC 也必须是 phase 1 mirror 产物，而不是等 target
`QUIESCED` 后再全量导出。phase 1 table mirror 维护 table lock descriptor 和
generation/fingerprint，并 hook：

- table lock add/remove；
- AUTO_INC lock acquire/release；
- lock mode/type 变化；
- table lock family invalidation。

phase 2 只校验：

- bounded tail delta 已 replay；
- table lock generation/fingerprint 与 sealed generation/fingerprint 一致；
- autoinc flag 与 descriptor 一致；
- lock count 未超过 `preserve_trx_max_lock_count`；
- unsupported table shape 未出现。

table generation/fingerprint 通过发布门禁后，post-prepare drift compare 不再属于正常
fast path；它被 generation/fingerprint 取代。debug/test 可开启 live canonical compare。

要删除当前 table live compare，必须先实现以下字段和 hook：

- table family generation：每次 table lock add/remove、AUTOINC ownership 变化都递增；
- table fingerprint：覆盖 table id/name identity、lock mode、AUTOINC flag、entry order；
- final table generation 必须在 `ha_prepare_low()` 前后保持一致，或者由同 worker 的
  seal-to-prepare critical section 证明没有 drift；
- 如果 table drift 只能在 post-prepare 才发现，fallback ON 也不能只替换 table payload；
  当前源码这种 partial family fallback 必须从 fast path 中删除。目标实现只能在 prepare
  前重新走整 target live path；如果 drift 到 prepare 后才发现，则只能 reject/rollback/taint
  并进入受控 cleanup，不能再称为 fallback。

未实现 table generation/fingerprint 前，table family 的 v2 行为是 snapshot+final
compare；若它触发 live export、post-prepare compare 或 quiesced 全量导出，该 target
必须标记 `SLO_NOT_GUARANTEED`。功能继续按 all-or-live/fallback 处理，但不能作为
full 1s fast path 通过。

### 8.2 MDL

full 1s 路线中，MDL descriptor 也由 phase 1 MDL mirror 从 duration-list context
持续维护；它不能把单个 `MDL_ticket::get_duration()` accessor 当成持久身份来源。
hook 覆盖：

- ticket acquire；
- ticket release；
- duration move；
- downgrade；
- explicit-to-transaction duration move；
- bulk list move。

MDL generation/fingerprint 通过发布门禁后，phase 2 只校验 MDL context generation、
descriptor count、duration list fingerprint；MDL payload 不再在正常 fast path 做 live
export compare。权限 recheck 可以在 phase 1 builder 做预检，phase 2 只做小对象最终确认；
如果权限状态变化，功能路径 fail closed 或 fallback。

MDL fingerprint 不能只比较 count。它必须覆盖：

- transaction-duration list 的 namespace/key/type/duration；
- ticket 在 duration-list 中的 deterministic ordinal；
- downgrade/type change；
- explicit-to-transaction duration move、bulk list move 后的新 order；
- SQL savepoint payload 中引用的 MDL ordinal。

权限 recheck 不能只依赖 descriptor count。若 MDL body external，resume attach 前的对象
权限复检要么读取 MDL segment body，要么 descriptor 中必须包含足够的小对象 identity
用于复检；不足时 fail closed。

当前实现必须从 `MDL_ticket_store` 的 duration-list context 或等价结构取得 duration，
并把 ticket acquire/release/downgrade/move 与 mirror mutation 绑定到同一临界区或等价
fence。未实现 MDL generation/fingerprint 前，MDL family 仍是 live export/compare，
不计入 1s fast path 成功；如果 phase 2 需要重新枚举整个 MDL list，该 target 必须报告
`SLO_NOT_GUARANTEED`。

### 8.3 Savepoint

savepoint 影响 MDL ordinal 和 InnoDB lock rollback boundary。phase 1 savepoint mirror
维护：

- SQL savepoint list generation；
- InnoDB savepoint count；
- MDL savepoint ordinal snapshot；
- `ROLLBACK TO SAVEPOINT` barrier。

如果 `ROLLBACK TO SAVEPOINT` 发生且无法精确表达 range-delete，target 标记
`SLO_NOT_GUARANTEED`。fallback ON 时丢弃该 target 的全部 warmcopy artifact 并走整
target live export；fallback OFF reject。

savepoint warmcopy 必须覆盖 `SAVEPOINT`、`RELEASE SAVEPOINT` 和
`ROLLBACK TO SAVEPOINT`。只处理 rollback barrier 不够，因为 create/release 会改变
SQL savepoint order，也会影响 MDL ordinal 的可恢复解释。

未实现 savepoint generation 前，savepoint 仍依赖当前 preserve path 的前后 payload
比较。该比较是正确性 fence，不是 warmcopy 性能优化；触发该路径时应报告
`SLO_NOT_GUARANTEED`，除非 NFR 证明其耗时在目标预算内。

因此，非 record-lock 的优化原则与 record family 一致：phase 1 维护权威锁对象镜像并写出
可采用的 artifact；phase 2 只允许 bounded tail、fence 和 descriptor adopt。任何 family
在 phase 2 才开始全量枚举、排序、编码或写大 payload，都只能走功能正确路径，不能计入
1s SLO。

## 9. 正确性与失败处理

### 9.1 Per-Target All-Or-Live

任一 required lock family artifact invalid 时，该 target 的所有 lock warmcopy artifact
都丢弃：

```text
fallback ON:
  target 走 live export
  SLO_NOT_GUARANTEED

fallback OFF:
  target reject before prepare
```

不能混用不同时间点的 record warmcopy、table live export、MDL stale descriptor。

这条规则必须在 durable point 前完成判定。当前工作树仍存在 post-prepare table drift 后
仅替换 table payload 的路径；在该路径被修复前，lock warmcopy fast path 不能进入发布门禁，
后续 external artifact、parallel preserve 或 table/MDL generation fast path 的 enable 测试
也必须被 source-shape guard 阻断。目标实现必须修成以下二选一：

1. `prepare` 前通过 record/table/MDL/savepoint generation fence 证明所有 required family
   一致，之后不再允许 fallback 到单一 family；
2. 如果无法证明一致，且 fallback ON，则在进入 `ha_prepare_low()` 前丢弃整个 target 的
   warmcopy artifact，走现有 live export path。

一旦 `ha_prepare_low()`、detach、snapshot write 或 token register 已经发生，后续失败只能
按 preserve kernel 已有的 rollback/taint/cleanup 语义处理，不能再被描述为普通
all-or-live fallback。

发布前门禁：源码中不得再存在 post-prepare drift 后只替换 `table_locks_payload` 或
`autoinc_lock_owned` 的成功路径。对应测试必须证明 table drift 发生在 prepare 后时不会
产生 record warmcopy + table live export 的混合 snapshot。

### 9.2 Seal-to-Prepare 原子性

phase 2 final fence 与 `ha_prepare_low()` 之间必须没有目标事务可见的锁变化窗口：

- target 已 blocked new lock-capable operations；
- record family 的 in-flight conversion/hook append 已等待完成；
- final fence sample 与 artifact descriptor seal 在同一 target seal critical section；
- preserve worker 获取 sealed descriptor 后立即进入 prepare，期间 conversion freeze
  继续有效；
- 若其他会话触发 implicit-to-explicit conversion，必须等待或标记 target invalid，
  不能绕过 final fence。

conversion freeze 的作用域只覆盖 InnoDB record implicit-to-explicit conversion。它不能证明
table lock、AUTO_INC、MDL duration-list、SQL savepoint 或 InnoDB savepoint 没有变化。
这些 family 在各自 generation/fingerprint hook 落地前，必须继续依赖 live export/final
compare 作为正确性 fence，并把该 target 标记为 `SLO_NOT_GUARANTEED`。文档和代码不得
把 record conversion freeze 描述成全锁族保护。

推荐实现顺序是“每 target seal + freeze + prepare 在同一个 worker 内串接”。如果仍保留
当前“phase2_preflight seal 后再进入 batch preserve”的两段式流程，则 preserve kernel
必须重新 sample final fence，并保持 conversion freeze 到 snapshot/register 完成。两段式
流程不能只依赖 seal 阶段的 fence。

强制顺序：

```text
freeze target conversion
sample frozen fence
compare frozen fence with sealed artifact fence
run ha_prepare_low
keep freeze until lock metadata populated and snapshot/register durable point resolved
```

如果实现需要先做一次 cheap fence compare 过滤明显无效 artifact，也必须在 freeze 后再做
一次 authoritative compare；freeze 前的 compare 不能作为进入 prepare 的正确性依据。

### 9.3 SLO Miss 行为

SLO miss 原因分类：

```text
TAIL_DELTA_TOO_LARGE
LIVE_FALLBACK_USED
UNSUPPORTED_FAMILY
PARALLEL_DISABLED
SNAPSHOT_WRITE_SLOW
PREPARE_SLOW
POST_PRESERVE_SHUTDOWN_SLOW
IO_SPILL_SLOW
```

这些原因只影响性能观测和 NFR 判定，不改变默认功能行为。

## 10. 观测指标

新增或扩展 status / report 字段。以下是目标字段；当前源码只实现了部分
`Preserve_trx_lock_warmcopy_*` counter，尚未实现 `Preserve_trx_phase2_*` breakdown、
external artifact count、parallel preserve threads 或 tail budget sysvar：

```text
Preserve_trx_phase2_total_us
Preserve_trx_phase2_lock_seal_us
Preserve_trx_phase2_target_preserve_us
Preserve_trx_phase2_prepare_us
Preserve_trx_phase2_snapshot_write_us
Preserve_trx_phase2_register_us
Preserve_trx_post_preserve_shutdown_us
Preserve_trx_phase2_slo_miss_count
Preserve_trx_lock_warmcopy_external_artifact_bytes
Preserve_trx_lock_warmcopy_external_artifact_count
Preserve_trx_lock_warmcopy_tail_delta_bytes
Preserve_trx_lock_warmcopy_live_fallback_count
Preserve_trx_parallel_preserve_threads_used
```

当前源码只有 `Preserve_trx_lock_warmcopy_phase2_pause_us` 等粗粒度 lock-warmcopy
counter，不能区分 seal、artifact adopt、prepare、snapshot write、register、shutdown。
这些字段没有落地前，只能说“观察到总 pause”，不能证明 1s 目标为什么达成或为什么失败。
当前 NFR runner 也只输出 `wall_ms`、`phase2_pause_samples_ms`、
`phase2_pause_summary_ms` 和可选 `warmcopy_metrics`；full warmcopy-only 当前报告中的
`warmcopy_metrics` 为空数组。因此在 runner/server 计时拆分落地前，任何 full NFR 只能
作为粗粒度 baseline，不能作为 1s 发布门禁证据。

NFR 报告必须输出每轮：

- phase2 总耗时；
- lock seal/adopt 耗时；
- per-target preserve p50/p95/max；
- snapshot write p50/p95/max；
- fallback target 数；
- SLO miss reason 分布。

没有这些拆分，不能宣称 1s 目标已达成。

report schema 至少包含：

```text
phase1_mirror_open_ms
phase1_base_scan_ms
phase1_delta_replay_ms
phase1_segment_encode_ms
phase1_segment_write_ms
phase1_segment_fsync_ms
phase2_total_ms
phase2_tail_replay_ms
phase2_lock_seal_ms
phase2_artifact_adopt_ms
phase2_target_prepare_ms
phase2_detach_claim_ms
phase2_snapshot_write_ms
phase2_register_ms
post_preserve_shutdown_ms
target_count
fallback_target_count
slo_not_guaranteed_target_count
slo_miss_reasons[]
external_lock_artifact_bytes
external_lock_artifact_count
materialized_lock_payload_bytes_in_phase2
phase2_full_lock_scan_count
server_side_record_lock_count
server_side_table_lock_count
server_side_mdl_ticket_count
tail_delta_bytes
tail_delta_object_count
```

`materialized_lock_payload_bytes_in_phase2` 必须为 0，`phase2_full_lock_scan_count` 也必须为
0，才能通过 strict SLO gate。`server_side_*_count` 用于证明 workload 的真实锁形态；
`phase1_*` 与 `phase2_*` 的拆分用于证明 O(lock count) 工作已经在 phase 1 完成，而
blocked business window 只剩 bounded tail、fence、descriptor adopt 和 prepare/register。

## 11. 测试方案

### 11.1 GUnit

- external lock artifact descriptor encode/decode；
- v8 inline snapshot 与 new external-lock snapshot 兼容 decode；
- descriptor+inline 冲突拒绝；
- external artifact hydrate before record/table/MDL import；
- segment manifest checksum、length、offset、torn write；
- owner marker、foreign owner cleanup、orphan temp cleanup；
- record/table/MDL generation fence；
- tail delta apply；
- phase 1 mirror object key canonicalization；
- phase 1 mirror add/update/delete/tombstone idempotency；
- base scan cursor + journal seq replay，覆盖 seq gap、dirty shard、unsupported shape；
- per-shard generation/fingerprint 在 mutation 临界区内更新；
- background segment builder 在锁外排序/编码/checksum/write；
- phase 2 strict path 不 materialize 完整 lock payload、不执行 O(lock count) full scan；
- artifact descriptor 不 materialize 大 payload；
- parallel target preserve executor success/failure/cancel；
- worker THD/current_thd isolation；
- target THD pin 与 release_resources/disconnect/KILL/shutdown lifetime；
- carrier concurrent distinct-token write/adopt/cleanup；
- descriptor-only snapshot structural decode -> hydrate -> semantic validation；
- indeterminate snapshot write returns `durable_snapshot_may_exist` while artifact is retained；
- freeze-before-final-fence order and conversion between pre-freeze compare and freeze；
- post-prepare table drift 不得存在 single-family fallback 成功路径；静态 guard 必须拒绝
  `use_lock_warmcopy_artifact` 分支里只替换 `metadata.table_locks_payload` /
  `metadata.autoinc_lock_owned` 后继续写 snapshot 的形态；
- 现有 `batch_drain_lock_warmcopy_table_post_prepare_drift` 当前断言
  `table_lock_warmcopy_post_prepare_drift` 后 live fallback 成功；实现 v2 correctness gate
  时必须删除或反转该用例，不能同时保留旧成功断言和新 all-or-live 发布门禁；
- SLO miss 不改变功能结果；
- fallback ON 与 fallback OFF 行为差异。

### 11.2 MTR

新增或重构用例：

```text
batch_drain_lock_warmcopy_external_artifact_record
batch_drain_lock_warmcopy_table_mdl_artifacts
batch_drain_lock_warmcopy_parallel_phase2
batch_drain_lock_warmcopy_slo_miss_continues
batch_drain_lock_warmcopy_fallback_slo_not_guaranteed
batch_drain_lock_warmcopy_spatial_fallback
batch_drain_lock_warmcopy_no_materialize_large_payload
batch_drain_lock_warmcopy_descriptor_only_recover
batch_drain_lock_warmcopy_descriptor_only_resume
batch_drain_lock_warmcopy_descriptor_inline_conflict
batch_drain_lock_warmcopy_all_or_live_no_mixed_family
batch_drain_lock_warmcopy_table_post_prepare_no_partial_fallback
batch_drain_lock_warmcopy_worker_thd_isolation
batch_drain_lock_warmcopy_phase1_mirror_linearizable
batch_drain_lock_warmcopy_tail_budget_slo_miss
batch_drain_lock_warmcopy_no_quiesced_full_scan
batch_drain_lock_warmcopy_lock_count_report
batch_drain_lock_warmcopy_phase2_no_materialize_payload
```

`batch_drain_lock_warmcopy_table_post_prepare_no_partial_fallback` 是现有
`batch_drain_lock_warmcopy_table_post_prepare_drift` 的替代语义：同样用 deterministic
debug injection 制造 post-prepare table drift，但期望结果必须是“prepare 前整 target live
export”或“prepare 后 reject/taint/cleanup”，不得出现 record warmcopy + table live export
混合 snapshot 后继续 RESUME/COMMIT 成功。对应 `.result` 也必须删除
`action=live_fallback.*table_lock_warmcopy_post_prepare_drift` 作为成功 preserve 的断言。

`batch_drain_lock_warmcopy_parallel_phase2` 不能只是 smoke test。它必须覆盖：

- 多 target 同时进入 prepare/detach/snapshot/register；
- 一个 worker 失败时其他 in-flight worker 收敛；
- disconnect/KILL/shutdown 与 target pin 等待；
- snapshot write concurrent distinct-token 成功；
- binlog warmcopy provider 与 lock artifact adopt 的锁顺序无死锁。

`batch_drain_lock_warmcopy_phase1_mirror_linearizable` 必须使用 deterministic injection
覆盖 base scan 期间的 add/remove/update、同一对象 delete 后重复 delete、delete 后 re-add、
implicit-to-explicit conversion、table/MDL duration move、savepoint create/release/rollback
barrier。验收不是“没有崩溃”，而是 final descriptor 的 canonical object set 与 live export
canonical set 等价。

`batch_drain_lock_warmcopy_no_quiesced_full_scan` 必须在 debug/test 模式打开计数器，若 target
进入 `QUIESCED` 后仍调用 full record/table/MDL/savepoint scanner、完整 payload encoder、
大字符串 materialization 或 segment full writer，则测试失败。该测试是防止 1s 路线退化为
“Phase 2 仍 O(lock count)”的核心门禁。

现有 v1 安全网必须继续保留并在 v2 变更后重跑，不能被未来 external/parallel 用例替代：

```text
batch_drain_lock_warmcopy_payload_equivalence
batch_drain_lock_warmcopy_interrupt_matrix
batch_drain_lock_warmcopy_implicit_insert_resume
batch_drain_lock_warmcopy_implicit_secondary
batch_drain_lock_warmcopy_implicit_explicit_overlap
batch_drain_lock_warmcopy_conversion_freeze_fence
batch_drain_lock_warmcopy_final_recheck_atomic
batch_drain_lock_warmcopy_rollback_to_savepoint_barrier
```

### 11.3 NFR

full workload：

```text
sessions = 1000
workload = 100000-row range UPDATE large-lockset per target
expected_lock_shape = explicit record X locks, confirmed by server-side lock count
binlog = ON
lock_warmcopy = ON
cycles >= 3
```

验收：

- preserve/resume 功能成功；
- warmcopy sealed target 成功率可解释；
- report 必须输出 server-side record/table/MDL lock count，确认 workload 实际制造的锁形态；
- fallback target 数为 0 时，phase2 p95 约 1s；
- fallback target 数非 0 时，报告 `SLO_NOT_GUARANTEED`，功能仍成功；
- phase2 breakdown 证明没有大 payload materialization。
- phase2 breakdown 证明 `phase2_full_lock_scan_count == 0`，且 scan/encode/checksum/write/fsync
  的主要耗时都出现在 `phase1_*` 字段。
- 报告必须记录 `phase2_total_ms` 的计量窗口起止点，确认它覆盖 blocked business
  window，而不是只覆盖 `phase2_preflight()`。

full workload 需要显式 override 默认容量参数，否则默认 `preserve_trx_max_total`、
`preserve_trx_batch_max_transactions`、`preserve_trx_max_lock_count` 无法覆盖
`1000 * 100000` 锁场景。NFR 命令必须在报告中记录：

```text
preserve_trx_max_total
preserve_trx_batch_max_transactions
preserve_trx_max_lock_count
preserve_trx_lock_warmcopy_max_memory_bytes
preserve_trx_lock_warmcopy_max_journal_bytes
preserve_trx_lock_warmcopy_seal_threads
preserve_trx_parallel_preserve_threads
phase2_total_ms_measurement_start
phase2_total_ms_measurement_end
```

当前证据只能作为 baseline：scaled NFR 显示小规模 warmcopy 明显快于 live baseline；
full warmcopy-only 3 轮 sealed `3000/3000`，但 phase2 p95 约 `179s`，且缺 live baseline
和 breakdown。因此当前 full 结果不能视为 1s 方案通过。

报告必须区分：

- scaled NFR：用于快速回归和比较 warmcopy/live 趋势，不能外推到 full 1000 x 10 万锁；
- full warmcopy-only NFR：用于暴露大锁数瓶颈，可证明 sealed 成功率和绝对暂停，但缺
  live baseline 时不能证明相对提升；
- full live-vs-warmcopy NFR：发布前必需，至少 3 轮，且必须包含 phase2 breakdown。
  当前 `nfr2-full-current.log` 只有失败日志，没有成功 JSON；文档和 release handoff 必须
  明确当前只有 warmcopy-only full baseline，不得暗示 full live-vs-warmcopy 已通过。

## 12. 实施切片

1. 增加 phase2 breakdown 计时，先证明 full workload 当前 179s 由哪些阶段构成，并把
   scaled handoff 约 31ms / current scaled 约 23ms 与 full 179s 分别记录为不同
   baseline。
2. 先修 v1 correctness gate，且作为所有 fast-path slice 的前置：
   - all-or-live：post-prepare table drift 不得只替换 `table_locks_payload` /
     `autoinc_lock_owned`；fallback ON 必须在 prepare 前整 target live export，
     prepare 后只能走 reject/rollback/taint/cleanup；
   - freeze order：record artifact 进入 prepare 前必须执行 `freeze -> sample frozen
     fence -> compare frozen fence with sealed artifact fence`；freeze 前 cheap compare
     只能用于提前淘汰明显无效 artifact，不能作为进入 prepare 的正确性依据；
   - 测试反转：删除或重写当前把 `table_lock_warmcopy_post_prepare_drift` live fallback
     当成功 preserve 的 MTR，新增 no-partial-fallback 断言；
   - source-shape/static guard：只要源码仍存在 post-prepare drift 后 single-family swap
     的成功路径，或存在 pre-freeze compare 后直接进入 prepare 的路径，后续 slice 的
     fast-path enable 测试必须失败。
   这一步必须早于任何删除 table live compare、启用 table generation fast path、
   parallel preserve 或 external artifact 发布。
3. 修正 report schema 和 runner：输出 blocked business window 起止点、phase2 breakdown、
   full workload 容量参数、fallback/SLO reason，以及 full live-vs-warmcopy 是否成功。
4. 增加 external lock artifact descriptor TLV、snapshot format bump、structural decode
   -> descriptor hydrate -> semantic validation API，保持 v8 inline snapshot 兼容。
5. 定义新 durable lock artifact magic/parser；`LWCSPV1/LWCSMF1` 继续只作为 drain 内
   临时 spill，不进入 recover/resume 事实源；durable artifact checksum 使用 SHA-256 或
   同等强度算法。
6. 为 carrier 增加 lock external artifact prebuilt/adopt contract，不能复用只允许
   `binlog_cache` 的现有 warm external blob 限制；`durable_snapshot_may_exist` 已是
   现有 snapshot write 的不确定状态信号，v2 需要补的是该信号与 external lock artifact
   的 retain/reconcile 规则。
7. 建立 phase 1 authoritative lock-object mirror：先覆盖 record family，定义 object
   key、Hook CS、per-shard generation/fingerprint、base scan cursor、journal seq 和
   delta replay；business-running hook 只能做 O(1) mirror 更新。
8. record family 改为 phase 1 external segment builder，strict phase2 只 replay bounded
   tail、freeze、sample/compare、adopt descriptor；quiesced-only full scan 只能标记
   `SLO_NOT_GUARANTEED`。
9. 禁止 spilled artifact 在 phase2 materialize；resume/import 阶段再读取 durable
   external artifact。
10. 明确并实现 record base scan 的稳定性契约：Hook CS + Fence、per-shard cursor/seq、
   consistent snapshot + generation/fingerprint。不能证明时，target 只能
   `SLO_NOT_GUARANTEED`。
11. 在 external/parallel 重构中保持 slice #2 的 correctness gate：如果保留两段式
   seal/preflight，preserve kernel 仍必须在 freeze 后重新用 frozen fence 对 artifact
   fence 做 authoritative compare；任何新 descriptor/adopt 路径都不能绕过该 gate。
12. table/AUTO_INC family 增加 phase 1 object mirror、generation/fingerprint warmcopy；
   只有 all-or-live bug 已修复且 generation/fingerprint 通过门禁后，才能删除正常
   fast path 的 post-prepare live compare。
13. MDL family 增加 phase 1 duration-list mirror、generation/fingerprint warmcopy，覆盖 order、bulk
   move、savepoint ordinal 和权限复检。
14. savepoint fence 纳入 phase 1 warmcopy generation，覆盖 create/release/rollback-to-savepoint
   barrier。
15. 实现 lock mirror/build worker pool；前置是拆掉 record store seal 的全局大锁和大字符串返回，
    并落地 target/shard 分区锁、锁外 segment 编码、后台 checksum/fsync 方案。
16. 实现 target preserve worker pool；前置是 worker THD/current_thd ownership 和 target
    lifetime 协议。
17. 增加 phase 2 tail budget sysvar 和 per-target observation；tail 超限时 fallback ON
    功能继续但 `SLO_NOT_GUARANTEED`，strict SLO gate 失败。
18. 完成 MTR/GUnit/NFR 和全量 preserve/resume MTR；现有 canonical equivalence、
    implicit、interrupt、freeze/fence、savepoint barrier 用例必须继续作为回归门禁。

## 13. 发布准入

功能准入：

- fallback ON 默认功能成功；
- fallback OFF fail closed；
- old inline snapshot 和 new external lock artifact 都可 resume；
- 新格式 snapshot 的 descriptor hydrate 在 semantic validation 前完成；
- 旧二进制/downgrade 策略明确，不会把新 external-lock snapshot 当 corrupt 垃圾误删；
- snapshot write 返回 durable-may-exist 时 artifact 保留并可 reconcile；
- crash/shutdown/restart 后 artifact 校验和 cleanup 正确。

性能准入：

- disabled hot path regression <= 2%；
- enabled hot path 输出独立指标；
- full NFR 中 fallback=0 的 warmcopy phase2 p95 <= 1000ms，或使用文档明确批准的
  临时容差；报告必须列出样本数和 p95 计算方式；
- 若 fallback>0，报告必须明确 SLO 不适用，不得声称达成 1s。
- full NFR 至少 3 轮，报告必须包含 phase2 breakdown 和容量参数 override；
- full live-vs-warmcopy 至少 3 轮有成功 JSON；只有 warmcopy-only full baseline 不足以
  证明相对提升；
- strict SLO gate 必须检查 `materialized_lock_payload_bytes_in_phase2 == 0`。
- strict SLO gate 必须证明 O(lock count) scan/encode/checksum/write 发生在 phase 1
  mirror/build 阶段，不发生在 blocked business window。

文档准入：

- 用户手册必须说明 1s 是 SLO，不是功能失败条件；
- 参数说明必须区分 fallback、strict SLO、worker threads、artifact disk budget；
- release handoff 必须列出已跑测试、未跑门禁、当前 full NFR 结果和回滚开关。
