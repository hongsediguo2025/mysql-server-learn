# MySQL 8.0.22 Preserve/Resume 备机直传保存设计

本文档描述 `DRAIN TRANSACTIONS PRESERVE` 阶段把 preserve 序列化对象发送到未来物理
备机，并由备机在线接收、校验、保存的设计。本文把“当前已实现的协议/receiver/save
骨架”和“后续生产 source transport/worker pool”明确分层；不覆盖后续在线升主
apply/resume 的实现。

核心结论：

- 这是与当前“本机持久化、shutdown、重启 recovery/resume”并行的第二套场景。
- 源端不能等本地 `Preserved_trx_store::write()` 发布本地 token 后再复制文件。
- 源端应在 preserve 语义对象构建完成后直接发送 portable transfer envelope。
- 备机最终保存到自己的 `preserve_trx_default_dir()` 兼容布局，未来在线升主直接读取，
  不需要从独立 inbox copy。
- 备机运行期间只接收、校验、落盘、维护 receiver 私有内存索引，不 claim prepared
  transaction，不导入锁、read view、MDL，也不注册普通 resumable token。
- 本物理主备直传模式下，mysqld startup 阶段绝不执行 preserve/resume/preflight/recovery
  处理；直传 artifact 只能由运行期 receiver 或未来在线升主 apply 流程识别。
- 当前实现状态：portable bundle/frame、receiver staging、target-side re-encode、
  `.standby_pending` 发布、普通 startup/recovery 过滤 standby pending token，以及
  test-injected source sender 边界已经具备；默认 production source transport 仍然
  fail-closed，等待 credential-name resolver 和真实 classic-protocol client 接入。
- 并行参数已经预留，`data_sessions` 当前只影响 test-injected encoded-frame sink 的
  connection slot 选择；`sender_workers` 和 `receiver_workers` 是 reserved 配置，真实
  worker pool 尚未实现。
- 代码实现必须松耦合，但不能重复实现 preserve_trx 已有能力：内核对象序列化、
  snapshot encode/decode、external blob descriptor、carrier 文件布局、持久化写入、
  读取和反序列化都应通过现有边界复用或小幅扩展。

### 0.1 当前实现状态与发布边界

当前代码已经落地了以下能力：

- `Preserve_trx_artifact_sink` 分流点，支持 local carrier 与 standby-transfer-save 决策。
- portable transfer manifest、bundle object、encoded frame codec。
- receiver 侧 `COM_PRESERVE_TRX_TRANSFER` 分发，要求
  `PRESERVE_TRX_TRANSFER_ADMIN` 动态权限，并受 receiver enable/source UUID/target UUID
  配置约束。
- receiver staging、object seal、digest 校验、target-side snapshot re-encode、最终
  `.bin` 与 `.standby_pending` marker 发布。
- 多 token 同 epoch 的 `COMMIT_EPOCH` 屏障：同一 epoch 内所有 receiving token 都 sealed
  后才发布 epoch commit marker。
- 普通 startup recovery、temporary tablespace bootstrap 和 local recovery 过滤
  standby-pending token，不把它们当成本机可 resume/rollback token。

当前仍未实现的生产能力：

- 默认 production source transport 是 fail-closed：`default_transfer_client_connect()` 和
  `default_transfer_client_send()` 返回 `UNSUPPORTED`。GUnit 通过注入 client ops 验证
  sender 边界，但真实 mysqld 还不能凭配置主动连接另一台 mysqld 发送 frame。
- `preserve_trx_transfer_credential_name` 尚未接入 credential resolver；因此不能把
  target user/credential 当成可工作的生产连接凭据。
- `sender_workers` reserved、`receiver_workers` reserved；当前没有生产 source worker
  pool，也没有 receiver worker pool。receiver 在执行 `COM_PRESERVE_TRX_TRANSFER` 的
  dispatch session 上解析、校验并落盘单个 frame。
- `HELLO`、`BEGIN_EPOCH`、`JOIN_EPOCH`、`BEGIN_TOKEN`、`OBJECT_HEADER`、
  `PUT_OBJECT_CHUNK`、`SEAL_TOKEN` 作为下一阶段完整协议名保留；当前代码使用更小的
  frame set：`BEGIN`、`OBJECT_CHUNK`、`SEAL_OBJECT`、`COMMIT_EPOCH`、`ABORT`。

因此，本阶段可以作为 receiver/protocol/publish 骨架和 fail-closed source boundary
交付；不能宣称已具备生产环境主机到备机的自动直传能力。生产直传闭环需要后续补齐
credential resolver、classic-protocol client、worker pool 和双 mysqld E2E。

## 1. 背景与当前代码边界

当前本机 preserve 路径在 `preserve_trx_kernel_preserve_attached_transaction()` 中完成
语义采集、prepare/detach、bundle 构建、本地 carrier 写入、内存 record 注册。关键
边界如下：

- `sql/preserve_trx.cc` 中 `preserve_trx_temp_table_build_preserve_manifest()` 生成 temp
  table manifest 和 sidecar 语义。
- `Preserved_trx_bundle_build_input` 汇集 `Preserve_snapshot_metadata`、binlog cache、
  record-lock external blob、temp manifest 等输入。
- `build_preserved_trx_bundle()` 形成 snapshot 语义 bundle。
- 随后当前代码调用 `store->write()`，由本机 carrier 写 external blobs 和 `.bin`
  snapshot。

直传备机场景的切入点应在 `build_preserved_trx_bundle()` 成功之后、`store->write()` 之前。
这里已经有完整 preserve 语义对象，但还没有被本机 carrier 绑定为本机 durable token。

当前 snapshot codec 会把 datadir fingerprint 和 server UUID 写入 snapshot，并用本地
carrier key 做认证。因此，源端 `.bin` 不能原样成为备机最终 `.bin`。源端必须发送
portable envelope，备机用自己的 codec context 重新编码成备机本地可读 snapshot。

## 2. 目标与非目标

### 2.1 目标

- 在源端 drain 阶段直接把每个 preserved token 的语义对象发送到备机。
- 备机在线接收并保存，不影响正在执行的只读查询逻辑。
- 备机最终文件位于自己的 `preserve_trx_default_dir()`，未来在线升主直接使用。
- 传输中的半成品不可被当成普通 preserved token。
- 保存完成的 token 有明确 `STANDBY_SAVED` 或 `PENDING_APPLY` 状态，区别于本机
  recovery 产生的 `PRESERVED` token。
- 当前阶段实现 receiver/protocol/publish 骨架和 test-injected sender boundary；生产
  source transport 与 source/receiver worker pool 是下一阶段目标。

### 2.2 非目标

- 不实现在线升主 apply/resume。
- 不实现备机 claim prepared transaction。
- 不导入 InnoDB read view、record locks、table locks、predicate locks。
- 不创建或恢复 MDL context。
- 不把 token 放入 `g_preserved_trx_records`，也不让普通 `RESUME PRESERVED
  TRANSACTION` 消费这些 token。
- 不设计 HA 控制器、旧主 fencing、业务连接迁移策略。
- 不在当前阶段实现生产 source transport、credential resolver、source/receiver worker
  pool、自适应调度、动态扩缩容或跨备机多目标复制。

## 3. 总体架构

直传保存由三层组成：

```text
Source drain preserve kernel
  |
  | builds Preserve_snapshot_metadata, TLVs, descriptors, object streams
  v
preserve_trx_transfer source exporter
  |
  | COM_PRESERVE_TRX_TRANSFER over configurable background sessions
  v
preserve_trx_transfer standby receiver
  |
  | staging write, digest check, target-side re-encode, final publish
  v
standby preserve_trx_default_dir()
```

设计上保留两种互不依赖的输出模式：

```text
LOCAL_CARRIER:
  当前本机 preserve/shutdown/recovery/resume 路径。
  build bundle -> store->write() -> register local record.

STANDBY_TRANSFER_SAVE:
  新增直传保存路径。
  build portable envelope -> send -> target re-encode/save.
  不要求源端先 store->write()。
```

未来可以支持同一次 drain 同时走本地 carrier 和远端 transfer，但这不是本轮默认目标。
本轮要求远端保存能力独立成立，并且源端处理/发送、备机接收/处理的并行框架已经具备，
默认并行度为 3，并允许通过参数降到 1 或提升到 N。

### 3.1 松耦合与复用原则

直传保存不是把网络代码塞进现有 preserve kernel，也不是新写一套独立的 preserve
serializer。实现边界必须同时满足两个要求：

```text
松耦合:
  transfer 模块独立拥有连接、协议、session、staging、receiver registry 和并发扩展。
  preserve kernel 不感知 classic-protocol frame 和对端连接细节。
  bundle codec 不感知网络传输和备机运行状态。

充分复用:
  preserve_trx 仍然是内核对象语义采集和序列化事实源。
  carrier 仍然是最终文件布局、snapshot publish、external blob 读写和 cleanup 事实源。
  bundle codec 仍然负责 snapshot TLV、descriptor、HMAC/CRC、metadata encode/decode。
```

因此，新增代码应围绕窄接口组合现有能力，而不是复制已有实现：

- 源端复用 `Preserve_snapshot_metadata`、`Preserved_trx_bundle_build_input`、
  `build_preserved_trx_bundle()` 和现有 binlog/lock/temp-table export 结果。
- 源端 portable envelope 只作为跨机传输 envelope；其中的语义字段来自现有 bundle 和
  object descriptors，不重新定义另一套 preserve 对象语义。
- 备机最终 `.bin` 使用现有 `encode_preserved_trx_bundle()`，但传入备机自己的
  `Preserved_trx_codec_context`。
- 备机后续读取、校验和未来在线 apply 应复用现有 carrier read/decode 能力，例如
  snapshot decode、external blob descriptor 校验和 payload read mode。
- final publish 应尽量复用 `Preserved_trx_carrier` / `Local_file_preserved_trx_carrier`
  的文件布局和原子写入语义；需要新增的只是 transfer staging、standby pending marker、
  以及“发布为 standby pending token”的小接口。
- temp table manifest、image sidecar、no-redo undo sidecar 不允许绕过现有
  `preserve_trx_temp_table_*` 和 temp-table carrier 语义另造文件名或 descriptor。

禁止的实现形态：

- 在 `preserve_trx_transfer` 中手写一套 snapshot TLV encoder/decoder。
- 在 receiver 中按字符串拼文件名，绕过 carrier 的 token safety、sharding、atomic write
  和 duplicate-token 保护。
- 在网络协议层重新解释 record locks、binlog cache、temp table manifest 的内部语义。
- 为了直传保存修改普通 `RESUME` 路径，使其认识未 apply 的 standby token。
- transfer disabled 时改变当前本机 preserve/shutdown/recovery/resume 行为。

### 3.2 非侵入式修改约束

本功能必须防止侵入式修改。实现时应优先增加窄接口、adapter 和 helper，避免把 transfer
状态、网络协议、receiver registry、备机保存状态散落进现有 preserve/resume 主流程。

硬性约束：

- 不为了 transfer 重写 preserve kernel 的对象采集、detach、prepare 或本机 token 发布语义。
- 不把 classic-protocol transfer frame 解析逻辑放进 `preserve_trx.cc` 的核心保存/恢复流程。
- 不让 transfer receiver 改造普通 `RESUME PRESERVED TRANSACTION`、`SHOW PRESERVED
  TRANSACTIONS`、startup preflight 或 recovery 的行为。
- 不通过大范围移动/重命名现有 preserve_trx 文件、类或函数来实现本功能。
- 不复制现有 carrier、bundle codec、temp-table sidecar 逻辑后再在 transfer 内维护第二套。
- transfer 关闭时，编译后行为、错误码、文件布局、观测输出和测试期望必须与当前本机
  preserve/shutdown/recovery/resume 路径保持等价。

允许的修改范围应集中在：

- 新增 `preserve_trx_transfer*` 模块。
- 在 preserve kernel 的 bundle 构建后增加一个 artifact sink 分流点。
- 给 carrier 增加最小 publish helper，用于发布 standby pending token。
- 给启动期路径增加物理主备 transfer 模式的隔离门禁，但不让启动期消费 transfer artifact。
- 增加参数、状态机、权限、测试和观测指标。

推荐的依赖方向：

```text
preserve kernel
  -> artifact sink interface
       -> local carrier sink
       -> transfer exporter

transfer receiver
  -> portable envelope validator
  -> existing bundle codec
  -> carrier publish/read helpers
  -> standby pending marker helpers
```

其中 `artifact sink interface` 是解耦点。preserve kernel 只交付“已经过当前 preserve
语义校验的 bundle 和对象 provider”，不直接发送网络包；transfer exporter 只消费这些
对象，不重新判断 preserve 事务语义。

## 4. 源端设计

### 4.1 Transfer exporter 接口

新增 `sql/preserve_trx_transfer.h` 和 `sql/preserve_trx_transfer.cc`，定义源端 exporter：

```text
Preserve_trx_transfer_exporter
  begin_epoch()
  begin_token()
  send_manifest()
  send_object()
  seal_object()
  seal_token()
  commit_epoch()
  abort_epoch()
```

`preserve_trx_kernel_preserve_attached_transaction()` 不应直接知道网络协议细节。kernel
只通过一个输出 sink 接口提交 bundle 和对象：

```text
Preserve_trx_artifact_sink
  publish_bundle_for_token(token, bundle, object_providers)
```

本轮先把 sink 做成两个实现，但接口按可并行 transfer 设计：

- `Local_carrier_artifact_sink`：包装现有 `store->write()`。
- `Transfer_artifact_sink`：把 portable envelope 交给 transfer exporter。

sink 接口必须是 preserve kernel 和 transfer 模块之间的唯一常规依赖。`Transfer_artifact_sink`
可以调用 portable envelope builder，但不能调用 SQL command dispatch、classic protocol
writer 或 receiver 代码；网络发送由 exporter 的后台 session 层负责。

### 4.2 源端对象来源

源端发送的是 portable envelope，不是本机最终 snapshot 文件。envelope 包含：

- token identity、owner、schema、timeout、created/expire 语义。
- binlog state、GTID、binlog cache metadata。
- read view payload bytes。
- record/predicate/table lock payload 或其 external object descriptor。
- MDL descriptor payload bytes。
- user variables、SQL/InnoDB savepoints。
- temp table manifest。
- external object descriptors：name、size、digest、object kind。
- 大对象 body stream：binlog cache、record-lock blob、temp image sidecar、temp undo
  sidecar。

源端允许使用现有 warmcopy 工作文件或临时 spill 文件作为发送对象来源，但这些文件
只是源端工作区，不是本地 durable preserved token。直传路径不得以“本地 `.bin` 已发布”
作为前置条件。

源端对象 provider 需要复用现有 provider/descriptor 体系：

- binlog cache 复用 `Mysql_binlog_preserve_snapshot` 和 `PrebuiltBinlogCacheBlob` 的
  metadata/descriptor。
- record-lock external body 复用 `PrebuiltRecordLocksBlob` 或
  `kPreservedTrxBlobRecordLocks` descriptor。
- inline record-lock payload externalize 时复用 `build_preserved_trx_bundle()` 的
  externalization 决策，不在 transfer 层重新决定同一 payload 是否外置。
- temp-table sidecar 复用 temp-table manifest 和 sidecar descriptor，不新增 transfer
  专用 temp-table manifest 格式。

### 4.3 与当前 preserve kernel 的关系

当前 preserve kernel 的 snapshot-write 阶段需要拆成两个概念：

```text
semantic bundle build:
  形成可 portable 传输的 preserve 语义对象。

artifact publication:
  LOCAL_CARRIER 写本地 carrier。
  STANDBY_TRANSFER_SAVE 发送给备机并等待 accepted-and-saved。
```

对 `STANDBY_TRANSFER_SAVE`，源端成功条件是备机返回 epoch commit ACK，表示备机已经
完成保存。若备机返回失败、连接中断、digest mismatch 或 commit timeout，本次直传 drain
失败，源端按 preserve kernel 当前失败路径收敛。

## 5. 协议设计

### 5.1 命令入口

新增 classic protocol 命令：

```text
COM_PRESERVE_TRX_TRANSFER
```

命令必须追加到 `enum_server_command` 末尾，并更新 `command_name[]` 和
`dispatch_command()`。本轮不走 SQL parser，不新增用户可直接执行的 SQL 语句。

备机只有在 receiver 参数显式打开时才接受该命令。命令处理入口先完成协议版本、source
server uuid、target server uuid、transfer epoch 和权限检查，再进入 receiver 状态机；检查
失败不得创建 staging 目录。

### 5.2 当前协议模型与后续参数化并行模型

后续生产直传协议按 control/data 分层设计：

- control session 管理 `HELLO`、`BEGIN_EPOCH`、token manifest、`SEAL_TOKEN`、
  `COMMIT_EPOCH` 和 `ABORT_EPOCH`。
- data session 发送 object chunks。`preserve_trx_transfer_data_sessions=1` 时，
  control 和 data 可以合用一个后台连接；参数大于 1 时，额外 data sessions 加入同一个
  epoch。
- 源端 sender workers 并行从 bundle/object providers 读取、切分和校验 object chunk。
- 备机 receiver workers 并行接收、写 staging、更新 object range map 和计算 digest。

完整生产协议计划使用以下 frame：

```text
HELLO
BEGIN_EPOCH
JOIN_EPOCH
BEGIN_TOKEN
OBJECT_HEADER
PUT_OBJECT_CHUNK
SEAL_OBJECT
...
SEAL_TOKEN
COMMIT_EPOCH
```

每个 frame 必须携带并行定位字段：

```text
protocol_version
epoch_id
source_server_uuid
target_server_uuid
token
object_id
object_kind
object_flags
chunk_offset
chunk_length
object_total_size
object_digest
frame_sequence
session_id
worker_id
```

`epoch_id + token + object_id + chunk_offset + chunk_length` 是 chunk 的幂等身份。重复
chunk 内容一致时可接受；同一 range 内容或 digest 不一致时必须拒绝整个 epoch。

当前代码先落地一个更小的 encoded-frame set：`BEGIN` 携带 token manifest，
`OBJECT_CHUNK` 携带 object range，`SEAL_OBJECT` 做 object 级 seal，`COMMIT_EPOCH`
做 epoch 级 publish，`ABORT` 清理 staging。这个 frame set 已经覆盖 receiver staging、
digest 校验、target-side re-encode 和 standby-pending publish；但还没有生产
classic-protocol source client、HELLO/JOIN_EPOCH 握手或真实 worker pool。

### 5.3 参数控制

本轮新增参数分为模式、连接、并行和限流四类。它们应同时控制主机并行处理/发送和备机
接收/处理：

```text
preserve_trx_transfer_enable
  源端是否启用直传保存能力。关闭时现有 LOCAL_CARRIER 路径行为不变。

preserve_trx_transfer_receiver_enable
  备机是否接受 COM_PRESERVE_TRX_TRANSFER。关闭时直接拒绝 transfer 命令。

preserve_trx_transfer_allowed_source_uuid
  备机允许写入 standby pending artifact 的源端 server UUID 列表。默认空列表应拒绝
  receiver 写入，测试环境可显式配置当前源端 UUID。

preserve_trx_transfer_artifact_mode
  源端 artifact 输出模式。初始支持 LOCAL_CARRIER 和 STANDBY_TRANSFER_SAVE；
  LOCAL_AND_STANDBY_TRANSFER_SAVE 可作为后续扩展，不作为本轮默认目标。

preserve_trx_transfer_target_host
preserve_trx_transfer_target_port
preserve_trx_transfer_target_socket
  源端后台 classic-protocol session 连接的目标 mysqld endpoint。TCP 和 Unix socket 二选一。

preserve_trx_transfer_target_server_uuid
  源端期望连接到的备机 server UUID。HELLO 返回值不匹配时必须中止 epoch，防止写错实例。

preserve_trx_transfer_target_user
preserve_trx_transfer_credential_name
  源端后台连接使用的受管 credential 引用。设计不要求把明文密码暴露成普通动态变量；
  credential 的落地方式应复用 MySQL 现有安全存储或启动配置约束。

preserve_trx_transfer_data_sessions
  源端到备机的后台 data session 数。默认 3；当前只影响 encoded-frame sink 的连接槽
  选择，真实 production source transport 接入前不代表 mysqld 已经会自动建立 3 条后台
  连接。设为 1 可验证同一状态机的单槽路径。

preserve_trx_transfer_sender_workers
  reserved，计划用于 source worker pool；当前 sender 同步构建 frame，不启动生产 worker。

preserve_trx_transfer_receiver_workers
  reserved，计划用于 receiver worker pool；当前 receiver 在 dispatch session 上处理
  单个 frame。

preserve_trx_transfer_chunk_bytes
  单个 PUT_OBJECT_CHUNK 的目标大小。

preserve_trx_transfer_max_inflight_bytes
  单个 epoch 在源端和备机允许的未 seal 数据上限，用于保护内存和磁盘压力。

preserve_trx_transfer_commit_timeout_ms
  源端等待备机 seal/commit 的超时。
```

默认配置把 `data_sessions/sender_workers/receiver_workers` 都设为 3，是为了固定未来
并行协议的配置面。当前只有 `data_sessions` 对 injected frame sink 有实际路由效果；
`sender_workers` 和 `receiver_workers` 在真实 worker pool 落地前不得作为生产并行能力
对外宣称。

### 5.4 并行状态机约束

- 锁粒度为 epoch、token、object，不允许全局大锁串行化所有 chunk。
- object range map 记录已写区间、累计大小和 rolling digest 状态。
- `SEAL_OBJECT` 是 object 级屏障，要求该 object 所有 range 到齐并通过 digest 校验。
- `SEAL_TOKEN` 是 token 级屏障，要求 token manifest 中全部 objects sealed。
- `COMMIT_EPOCH` 是 epoch 级屏障，只能由 control session 执行。
- 任一 data session 断开只使其负责的未 sealed range 进入 retry/abort 判断，不应破坏已
  sealed objects。
- receiver worker 只能写 staging 或最终 publish helper，不能直接注册 resumable record。

### 5.5 身份与权限

源端发起 drain 的权限和备机接收 artifact 的权限必须分离：

- 源端 `DRAIN TRANSACTIONS PRESERVE` 继续使用当前 drain 命令权限模型。
- 源端后台 classic-protocol sessions 使用 `preserve_trx_transfer_target_user` 指定的专用
  transfer 账户连接备机。
- 备机 receiver 命令建议新增 preserve 专用动态权限，例如
  `PRESERVE_TRX_TRANSFER_ADMIN`。如果为了兼容保留 `SUPER` 兜底，必须显式记录并覆盖测试。
- receiver 在创建 staging 之前校验 authenticated user、required privilege、
  `allowed_source_uuid`、`target_server_uuid` 和 epoch nonce。
- control session 和 data sessions 必须绑定同一个 authenticated source identity、epoch
  和 target UUID；data session 不能单独创建 token，也不能提升为 control session。
- credential 不应以明文普通变量形式暴露；性能视图、错误日志和诊断信息只输出 credential
  名称或 redacted 目标信息。

## 6. 备机接收与保存设计

### 6.1 最终目录

备机最终保存目录就是：

```text
preserve_trx_default_dir()
```

不设计长期独立 `preserve_trx_transfer_inbox/`。未来在线升主 apply 直接读取该目录里的
最终 carrier 兼容文件，不做目录间 copy。

### 6.2 传输 staging

传输中仍需要 staging，但它只是同目录下的临时写入命名空间，不是长期 inbox：

```text
preserve_trx_default_dir()/.transfer/<epoch_id>/<token>/<object_id>.part
```

staging 的作用：

- 防止半成品 object 被 list/read 路径误识别。
- 支持 receiver crash 后清理或继续审计。
- 支持本轮多 session range-level 幂等写入。
- 避免 `.bin` snapshot 在 marker 和 sidecars 尚未完成时可见。

`COMMIT_EPOCH` 后，receiver 只做同文件系统内 rename/publish，不做跨目录 copy。

### 6.3 Standby pending marker

最终文件在 `preserve_trx_default_dir()` 兼容布局下可见，但 token 不能被任何普通本机
preserve/resume 路径当作普通 token 消费。因此每个直传保存 token 必须有 pending marker：

```text
<token>.standby_pending
```

marker 语义：

- 表示该 token 是备机在线接收保存完成的远端 preserve artifact。
- 表示当前 mysqld 运行期不得自动 claim/apply/resume。
- 表示未来在线升主 apply 流程可以扫描和消费。
- marker 不参与 mysqld startup 流程；物理主备模式下启动阶段必须完全不处理 preserve/resume
  artifact。

发布顺序必须保证 `.bin` 不会在没有 marker 的情况下可见：

```text
1. write and fsync external blobs / temp sidecars in final or staged form
2. write and fsync <token>.standby_pending
3. atomically publish target-side encoded <token>.bin
4. fsync preserve_trx_default_dir()
5. write epoch commit marker
```

如果 crash 发生在 marker 已写但 `.bin` 未发布，清理路径可以删除 orphan marker。
如果 crash 发生在 `.bin` 发布后，该 artifact 在启动阶段仍必须保持惰性；后续只能由运行期
receiver 审计或未来在线升主 apply 流程处理。

### 6.4 目标端重新编码

备机不能保存源端 `.bin`。receiver 必须：

1. 校验 portable manifest 和 object digest。
2. 使用备机自己的 carrier key、datadir fingerprint、server UUID 构造
   `Preserved_trx_codec_context`。
3. 调用目标端 snapshot encoder 生成备机本地 `.bin`。
4. 写入 external blob 和 temp sidecar 的最终文件。
5. 发布 standby pending marker 和 `.bin`。

这保证未来在线升主使用目标端正常 carrier read path 时，snapshot 身份校验符合备机
本地上下文。

receiver 不应手写 snapshot bytes。正确实现是把 portable envelope 还原为
`Preserved_trx_bundle`，再调用现有 encoder。external blob 文件写入也应走 carrier
兼容 helper，避免直传路径和本地路径产生两套文件布局或两套 descriptor 校验逻辑。

### 6.5 Receiver 内存态

receiver 可以维护私有内存 registry，但不能写 `g_preserved_trx_records`：

```text
Transfer_receiver_registry
  epoch_id
  token
  state: RECEIVING | SAVED_ONLINE | CORRUPT | ABORTED
  metadata summary
  object descriptor table
  saved file paths
  last_error
```

内存中可展开：

- token metadata。
- owner、schema、timeout。
- binlog/GTID boundary。
- object descriptor table。
- lock payload count 和 digest summary。
- temp table manifest summary。

大对象保留在文件中：

- binlog cache body。
- record-lock external blob。
- 大型 predicate/table lock payload。
- temp table image sidecar。
- no-redo undo sidecar。

本产品路径不支持 receiver mysqld 重启后继续当前 transfer/promotion-ready epoch。
Receiver 重启后，进程内 registry、prewarm plan 和 native handle 均失效；旧 epoch 必须
fail closed，并由 source 在新的在线 epoch 中重新传输。不得从 spool、carrier 或其它
持久工件自动 replay/rebuild promotion-ready registry，也不得把该逻辑挂到 startup
阶段。正常路径是 receiver 全程在线，未来升主直接消费运行中的 registry。

## 7. 备机只读安全

备机接收保存期间必须满足以下约束：

- 不获取业务表 MDL。
- 不访问或修改 DD 对象。
- 不调用 `trx_preserve_claim_prepared()`。
- 不调用 `trx_preserve_import_read_view()`。
- 不调用 `trx_preserve_import_record_locks()` 或 `trx_preserve_import_table_locks()`。
- 不调用 `create_detached_mdl_context()`。
- 不向 `g_preserved_trx_records` 注册 record。
- 不让 `SHOW PRESERVED TRANSACTIONS` 展示这些 pending token。
- 不让 `RESUME PRESERVED TRANSACTION` 消费这些 pending token。

逻辑上，receiver 只写 preserve artifact 文件和私有内存状态。它可能带来磁盘 I/O 压力，
因此需要独立限速和后台线程配置，但不能改变备机当前只读查询的事务语义。

## 8. 与 mysqld 启动阶段的隔离关系

本文设计面向物理主备直传保存场景。该场景下，mysqld startup 阶段绝对不能执行任何
preserve/resume 处理：

- 不扫描直传保存 artifact。
- 不解析 `.standby_pending` marker。
- 不 decode transfer 保存的 snapshot。
- 不执行 preserve snapshot preflight。
- 不调用 `preserved_trx_recover_all()`。
- 不调用 `trx_preserve_claim_prepared()`。
- 不向 `g_preserved_trx_records` 注册 standby token。
- 不在启动日志中把 standby pending token 当作本机 preserved token 处理。

这不是把 marker 接入启动期保存/恢复流程的设计。正确边界是：物理主备 transfer 模式必须
在配置门禁处使启动期 preserve/resume 流程不进入，直传 artifact 对 startup 完全惰性。

未来在线升主 apply 流程负责把 `STANDBY_SAVED` token 转入可应用状态。该流程发生在 mysqld
已经在线运行之后，必须在升主 fencing、read_only/super_read_only 切换、prepared trx
claim 顺序等问题设计清楚后单独实现。它不能复用 mysqld 启动恢复路径作为触发点。

## 9. 文件发布与清理规则

### 9.1 写入顺序

receiver 对每个 token 按以下顺序处理：

```text
receive portable token manifest
receive object chunks into staging
seal every object by size and digest
target-side encode snapshot bytes
publish external blobs and temp sidecars
write standby_pending marker
publish snapshot .bin
fsync directory
mark token SAVED_ONLINE
```

外部大对象和 temp sidecar 可以先写入 staging，再 rename 到最终文件名。snapshot `.bin`
必须最后发布。

### 9.2 失败清理

- epoch 未 commit：只删除 `.transfer/<epoch_id>/` staging。
- object seal 失败：token 标记 `CORRUPT`，不发布 `.bin`。
- marker 写入失败：不发布 `.bin`，删除已发布 sidecar 或保留为 transfer cleanup-only。
- snapshot 发布后 fsync 失败：保留 marker 和 `.bin`，状态为 ambiguous saved，后续
  receiver 运行期审计或未来在线升主 apply 按 marker 审计；startup 不处理。
- token 已有普通 `.bin` 或 standby pending marker：拒绝重复 token，不能覆盖。

## 10. 当前验证范围与后续 E2E 范围

当前阶段目标是证明 receiver/protocol/publish 骨架成立，并明确 production source
transport fail-closed。已覆盖和必须保持的范围：

- portable manifest/frame codec。
- 一个 epoch 中支持多个 token，`COMMIT_EPOCH` 等待同 epoch 内所有 receiving token
  sealed 后再发布。
- 每个 object 按 chunk frame 发送，chunk 字段完整保留。
- 备机接收后保存到 `preserve_trx_default_dir()`，带 `.standby_pending` marker。
- 目标端 `.bin` 由备机 codec context 重新编码。
- 不注册普通 preserved record。
- 不触发 resume。
- 普通 startup recovery 和 temp bootstrap 不处理 standby-pending token。
- 默认 production source client 返回 `UNSUPPORTED`，避免把未接入 credential resolver 的
  配置误当成可用网络发送能力。

后续完整生产直传 E2E 还需要覆盖：

- 参数化后台 transfer sessions；`data_sessions=1` 和 `data_sessions>1` 走同一套状态机。
- source sender workers 并行读取/切分 object，receiver workers 并行写 staging。
- 双 mysqld 下源端 `DRAIN TRANSACTIONS PRESERVE` 真正通过 classic protocol 把 token
  发送到备机。
- transfer 期间备机并发只读查询持续成功。

当前可以采用保守限制：

- 不启用自动在线升主 apply。
- 不做自适应带宽调度；并发度只由静态参数控制。
- 不做动态 worker 扩缩容；参数变更只影响下一次 epoch。
- 若某类对象当前只能通过本地 writer 生成，允许源端使用临时工作文件作为发送来源，但不允许
  把本地 durable token publication 作为传输前置条件。

## 11. 后续并行执行细节

后续生产并行实现按以下模型落地：

- source side worker pool 按 token/object/range 分配 chunk。
- data session pool 从 source worker 输出队列取 chunk 并发送。
- target side receiver worker pool 并行写 staging range。
- 每个 object 使用 range map 记录已写区间。
- 重复 chunk 相同 digest 则幂等接受，不同内容拒绝 epoch。
- `SEAL_OBJECT` 校验全 object digest。
- `SEAL_TOKEN` 校验 token manifest 中全部对象 sealed。
- `COMMIT_EPOCH` 是唯一最终发布屏障。
- sender queue 和 receiver queue 都受 `preserve_trx_transfer_max_inflight_bytes` 约束。
- receiver publish 阶段仍按 token 顺序执行 final marker 和 `.bin` 发布，避免并行 publish
  破坏 carrier 可见性。

需要新增观测指标：

- epoch count、token count、object count。
- bytes sent、bytes received、bytes staged、bytes published。
- chunk retry count、digest mismatch count。
- receiver fsync time、snapshot encode time。
- current active sessions、configured data sessions、sender worker count、receiver worker count。
- source queue depth、receiver queue depth、max inflight bytes。

## 12. 测试计划

### 12.1 单元测试

- portable manifest encode/decode。
- command frame parser。
- chunk offset/length/digest 校验。
- duplicate chunk 幂等。
- corrupted chunk 拒绝。
- object seal size mismatch。
- parallel sessions 写入不同 object/range 后 seal 成功。
- same range duplicate chunk 内容一致时幂等接受，内容不一致时拒绝 epoch。
- `max_inflight_bytes` 到达上限时 source sender 反压。
- standby pending marker 发布顺序。
- target-side re-encode 后 carrier metadata 可读。
- pending token 不进入 `g_preserved_trx_records`。
- transfer receiver 不包含独立 snapshot TLV encoder/decoder。
- receiver 最终文件可由现有 carrier read/decode 路径读取。
- transfer disabled 时 `Local_carrier_artifact_sink` 与当前 `store->write()` 行为等价。

### 12.2 MTR 双 mysqld 测试

- 源端 drain 直传一个 token 到备机，分别覆盖 `data_sessions=1` 和默认 `data_sessions=3`。
- 多 token / 多 object 并行传输，验证每个 token 的 object seal 和 token seal 独立正确。
- 备机并发执行长时间只读 SELECT，transfer 期间查询持续成功。
- 备机 `preserve_trx_default_dir()` 出现最终 carrier 文件和 `.standby_pending` marker。
- `SHOW PRESERVED TRANSACTIONS` 不显示 standby pending token。
- `RESUME PRESERVED TRANSACTION` 对 pending token 返回不可用。
- 物理主备 transfer 模式下，备机启动阶段不执行 preserve preflight/recovery；带 pending
  marker 的 token 不被启动期 claim、decode 或注册。

### 12.3 故障注入

- source transfer session 断开。
- 某一个 data session 断开，其他 session 已 sealed object 保持可审计，epoch 按策略 retry
  或 abort。
- receiver 写 chunk 失败。
- object digest mismatch。
- marker 写入失败。
- snapshot publish 前 crash。
- snapshot publish 后、epoch commit 前 crash。
- 重复 token。
- 备机 `preserve_trx_default_dir()` 中已有普通本机 token。

### 12.4 回归

- transfer 功能关闭时，现有本机 preserve/shutdown/recovery/resume 行为不变。
- 当前 carrier duplicate-token 保护不被削弱。
- 当前 temp table sidecar cleanup 不删除 standby pending 的已保存 sidecar。
- 当前 warmcopy spill cleanup 不把 transfer staging 当成 orphan spill 误删。

## 13. 当前代码锚点

以下锚点来自当前源码，用于约束实现边界。实现前如果文件有移动，应按 symbol 复查，但设计
依赖的是这些边界而不是具体行号。

- 源端切入点在 `sql/preserve_trx.cc`：
  `preserve_trx_kernel_preserve_attached_transaction()` 当前在 8640 行附近；bundle input
  构造在 9968 行附近；`build_preserved_trx_bundle()` 调用在 9998 行附近；本地
  `store->write()` 调用在 10032 行附近。transfer sink 分流点应放在 bundle 成功构建之后、
  本地 carrier 发布之前。
- bundle 和 codec 复用 `sql/preserve_trx_bundle.h` / `.cc`：
  `Preserved_trx_codec_context` 在 header 402 行附近，明确包含 datadir fingerprint 和
  server UUID；`Preserved_trx_bundle_build_input` 在 429 行附近；`build_preserved_trx_bundle()`、
  `encode_preserved_trx_bundle()`、`decode_preserved_trx_snapshot_bytes()` 分别在 header
  457、461、467 行附近声明。
- carrier 复用 `sql/preserve_trx_carrier.h` / `sql/preserve_trx_carrier_file.cc`：
  `Preserved_trx_carrier` 接口在 header 161 行附近，已经定义 `codec_context()`、
  `write_external_blobs_new()`、`write_snapshot_new()`、`read_existing()`、`list_tokens()`
  和 `token_state()`；file carrier 的相关实现在 1580、1608、1680、1759、1919、2007
  行附近。receiver final publish 应扩展这些 helper，而不是绕开布局直接写文件名。
- temp-table sidecar 复用 `sql/preserve_trx_temp_table.h`：
  warmcopy participant、prebuilt sidecar 和 manifest builder 在 120、122、327、356 行
  附近；transfer 只能搬运/发布这些 descriptor 和 sidecar body，不能定义第二套 temp
  manifest。
- 可见性边界在 `sql/preserve_trx.cc`：
  `g_preserved_trx_records` 在 524 行附近；`SHOW PRESERVED TRANSACTIONS` 从
  `g_preserved_trx_records` 组装可见行，在 6493 行附近；普通记录数在 6529 行附近。备机
  receiver 私有 registry 不得写入该 vector。
- 需要隔离的现有启动期入口在 `sql/mysqld.cc`、`storage/innobase/handler/ha_innodb.cc` 和
  `sql/preserve_trx.cc`：
  mysqld 启动 preflight 调用在 `sql/mysqld.cc` 6287 行附近；
  `preserved_trx_preflight_recoverability()` 在 `sql/preserve_trx.cc` 8202 行附近；
  `preserved_trx_recover_all()` 在 8502 行附近；InnoDB 启动路径调用 recovery 在
  `ha_innodb.cc` 3738 和 3752 行附近；`trx_preserve_claim_prepared()` 调用在
  `sql/preserve_trx.cc` 8101 行附近。物理主备 transfer 模式必须在配置门禁处绕开这些入口，
  不能在这些入口里新增 standby pending marker 消费或跳过分支。
- preserve 专用动态权限的参考实现位于 `sql/auth/dynamic_privileges_impl.cc`：
  `RESUME_ANY_PRESERVED_TRANSACTION` 在 221 行附近注册。receiver 若新增
  `PRESERVE_TRX_TRANSFER_ADMIN`，应沿用同一注册机制。

## 14. 实施顺序建议与当前完成度

1. 已完成：定义 portable transfer manifest 和 frame codec。
2. 已完成：定义 transfer 参数、epoch/token/object/range 状态机和 inflight 上限。
3. 已完成：增加 standby-pending marker 过滤，确保普通 mysqld startup 不进入 standby
   pending token 的 local recovery/resume。
4. 已完成：实现 receiver staging、object seal、target-side snapshot re-encode、final publish。
5. 已完成：实现 `COM_PRESERVE_TRX_TRANSFER` receiver dispatch 和权限门禁。
6. 待完成：实现 production source classic-protocol client、credential-name resolver 和
   HELLO/endpoint 校验。
7. 待完成：实现 source sender worker pool 和 receiver worker pool；当前
   `sender_workers`/`receiver_workers` reserved。
8. 已完成：在 preserve kernel 中增加 artifact sink 分流点。
9. 待完成：增加双 mysqld MTR，覆盖显式并行度 1 和默认并行度 3。

原始顺序仍作为后续完整生产直传参考：

1. 定义 portable transfer manifest 和 frame codec。
2. 定义 transfer 参数、epoch/session/object/range 状态机和 inflight 反压。
3. 增加物理主备 transfer 模式启动期隔离门禁，确保 mysqld startup 不进入 preserve
   preflight/recovery；standby pending marker 仅供 receiver 运行期和未来在线升主 apply
   识别。
4. 实现 receiver staging、object seal、target-side snapshot re-encode、final publish。
5. 实现 `COM_PRESERVE_TRX_TRANSFER` control session 和参数化 data session pool。
6. 实现源端 sender worker pool 和备机 receiver worker pool。
7. 在 preserve kernel 中增加 artifact sink 分流点。
8. 增加双 mysqld MTR，覆盖显式并行度 1 和默认并行度 3。

## 15. 关键决策记录

- 直传路径不等待源端本地 durable token 发布。
- 备机最终使用 `preserve_trx_default_dir()`，不使用长期独立 inbox。
- staging 是同目录临时写入机制，不是未来 apply 的数据来源。
- 备机保存的是目标端 re-encoded carrier 文件，不是源端 `.bin` 原样复制。
- 新增代码必须通过 artifact sink、portable envelope、receiver publish helper 与现有
  preserve_trx 能力组合；不得重复实现 bundle codec、carrier 布局或 temp-table sidecar
  descriptor 语义。
- 当前 `data_sessions` 只为 encoded-frame sink 的连接槽预留并行形态；
  `sender_workers` reserved、`receiver_workers` reserved。生产 worker pool 不能在实现前
  作为已交付能力描述。
- 备机运行期不做 resume/activation，也不改变只读查询语义。
- 物理主备 transfer 模式下，mysqld startup 阶段不做任何 preserve/resume/preflight/recovery
  处理，也不解析 standby pending artifact。
- 后续在线升主 apply 是单独设计，不属于本文实现范围。
- 实现必须坚持非侵入式修改：新增窄接口和独立 transfer 模块，默认不改现有本机
  preserve/resume 行为，不做大范围重构。
