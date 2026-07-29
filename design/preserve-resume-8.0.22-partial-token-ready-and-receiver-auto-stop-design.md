# Preserve/Resume 8.0.22 部分 Token 快速恢复与 Receiver 自动停止设计

## 1. 文档状态

- 日期：2026-07-29
- 状态：设计审查稿，尚未实施
- 代码基线：`cf70f460409`
- 适用场景：在线物理备机接收 Preserve transfer，随后可能被 HA 选为新主
- 不适用场景：本地 `preserve -> shutdown -> startup -> resume`
- 审查修订：补齐 identity-first 调度前置条件、内部 READY/wire ACK 区分，
  并明确 `STANDBY_TRANSFER_SAVE` 成功后不关闭 source mysqld

> **正式方案选择：方案 2（收敛实现）。**
>
> 本文后续所有接口、状态、代码量和测试均以方案 2 为唯一实施基线：
> source 只允许精确列举的 pre-final token 排除；receiver 直接在 accepted
> epoch 内发布 READY/EXCLUDED 分类；只有 READY 注册 Resurrection
> candidate；worker 只复用现有线程池做 join-only 自动停止。不得在实施中
> 恢复独立 selection 对象、第二套 receiver runtime 状态机、通用 token-local
> 失败策略或新的 accepted-epoch 生命周期 enum。任何超出该边界的需求必须
> 停止编码并重新评审本文。

本文解决五个相互关联但必须保持边界清晰的问题：

1. source Phase2 中能够证明为 token-local 的 Preserve 失败，只排除失败 token，不能让它拖累已经成功的 token。
2. receiver 中部分 token 已经具备快速恢复条件，部分 token 尚未 READY 时，不能让一个 token 拖累其余大部分 token。
3. `preserved_trx_prepare_before_trx_sys_init_for_physical_promotion()` 必须在 `trx_sys_init_at_db_start()` 前等待并取得 receiver 已完成的 token 分类，但不能改变物理升主主流程。
4. receiver 完成接收、prewarm 和临时资源清理后，应当自动退出 worker 并停止运行；它的生命周期不能绑定到 HA 最终是否升主。
5. `STANDBY_TRANSFER_SAVE` 在 FINAL_ACK 后只完成 source 交权与安全的
   transfer 临时资源清理，DRAIN 返回结果但不调用 mysqld shutdown；source
   角色降级、后续停机或重建由 HA 负责。

本文只设计 Preserve 内核如何提供这些能力。redo apply、物理复制追平、主备角色裁决和 write-enable 仍由物理备机/HA 项目负责。

## 2. 结论

本轮推荐采用以下最小模型：

```text
一次物理升主流程不切换“模式”

source Phase2:
  declared tokens D
    -> 对 token-local 失败完成 source 恢复并逐 token ABORT
       - SOURCE_EXCLUDED Xs
    -> survivors S = D - Xs
    -> COMMIT_EPOCH fact.tokens = S
    -> FINAL_ACK

source FINAL_ACK 后:
  -> 只释放或移交 deferred cleanup 的 transport/warmcopy 临时资源
  -> 进入 TRANSFER_HANDOFF_COMPLETE，普通业务命令继续被拒绝
  -> 返回结构化 DRAIN 结果
  -> 不调用 shutdown()/kill_mysql()，不恢复或回滚 transferred trx
  -> HA 后续显式降级、停机或重建 source

receiver:
  accepted.tokens = fact.tokens = S
    -> per-token prewarm
    -> 冻结 token classification
       - READY candidates
       - RECEIVER_EXCLUDED Xr
    -> 发布 epoch READY selection
    -> 清理 receiver 临时资源
    -> worker 全部退出
    -> transfer handler 与 promotion/TTL 对象继续存在

physical promotion:
  redo apply/freeze 已完成
    -> prepare_before_trx_sys_init()
       - 只注册 READY candidates
       - 记录 RECEIVER_EXCLUDED tokens
    -> trx_sys_init_at_db_start()
       - READY candidate: 认证 Index 命中，跳过 Undo body
       - SOURCE_EXCLUDED: 未进入 accepted fact，走普通物理恢复
       - RECEIVER_EXCLUDED: 非 candidate，走原生 Undo body 扫描
    -> 将 RECEIVER_EXCLUDED 的 Preserve PREPARED trx
       交给原生 recovered rollback
    -> strict adopt READY survivors
    -> 原物理升主流程继续

SQL resume:
  READY survivor -> 使用现有 RESUME PRESERVED TRANSACTION
  SOURCE_EXCLUDED / RECEIVER_EXCLUDED -> 不可 resume
```

这里没有“整次普通升主降级”状态。物理升主编排始终保持原样；差异只发生在每个事务选择快速恢复还是传统恢复。

必须区分两类排除：

- `SOURCE_EXCLUDED`：source 在 `COMMIT_EPOCH` 前已经排除并取得单 token `ABORT` 的明确成功结果，不进入 accepted fact。
- `RECEIVER_EXCLUDED`：已经进入 accepted fact，但 receiver 在 FINAL_ACK 后的 prewarm/classification 中不满足快速恢复条件。

两类排除都不表示事务丢失，也不表示整次升主失败；它们不能执行 Preserve SQL RESUME，后续依靠物理页面、Undo 和原生恢复路径处理。只有 `RECEIVER_EXCLUDED` 属于 receiver promotion selection，`SOURCE_EXCLUDED` 不得被重新加入 selection。

“冻结 token classification”的耗时必须与其之前的等待分开统计：

- 等待阶段主要由 token prewarm 决定，从 FINAL_ACK 后持续到全部 token 进入终态或既有 deadline 到达。
- 冻结动作本身只读取 token 状态、构造 READY/RECEIVER_EXCLUDED 集合并发布不可变对象，不读取 payload，不执行 DD/锁物化，也不做文件 I/O。
- 冻结算法为 O(token count)。实现后记录该阶段耗时，但首版不新增未经
  runner 支撑的硬编码 10ms 发布门槛。

## 3. 场景与需求

### 3.1 正常全量 READY

一个 epoch 包含 1000 个 token，全部 token 在 receiver deadline 前完成 prewarm：

- 1000 个 token 都注册为 Resurrection candidate。
- `trx_sys_init_at_db_start()` 仍构建 1000 个 `trx_t`。
- 1000 个 token 都通过认证 Index 注入 exact table IDs，跳过 Undo body 扫描。
- strict gate adopt 这 1000 个事务。
- 后续 1000 个 token 均可使用现有 SQL RESUME。

这与当前 all-or-nothing 成功路径语义等价。

### 3.2 部分 READY

一个 epoch 包含 1000 个 token：

- 990 个 token 完成 prewarm。
- 1000 个 token 都先完成轻量 identity 认证。
- 10 个 token 因 PREWARM cutoff 前重型物化未完成，或在 identity 认证后命中
  首版明确不支持的 token 对象而未 READY。

目标行为：

- 990 个 READY token 进入 Preserve 快速恢复。
- 10 个 RECEIVER_EXCLUDED token 不注册 Resurrection candidate，执行传统 Undo body 扫描。
- 10 个 RECEIVER_EXCLUDED token 在 `trx_sys` 对象构建后被交给原生 recovered rollback。
- 990 个 survivor 仍可 strict adopt 和 SQL RESUME。
- 不因 10 个 token 的局部问题放弃其余 990 个 token。

### 3.3 没有 Token READY

如果 receiver 完成分类后 survivor 数为 0：

- `prepare_before_trx_sys_init()` 仍完成一次合法的空 selection 消费。
- 不注册任何 Preserve Resurrection candidate。
- `trx_sys_init_at_db_start()` 对这些事务全部执行传统 Undo 恢复。
- Preserve magic PREPARED 事务被转交原生 recovered rollback。
- strict adopt 成为 adopted count 为 0 的 no-op。
- 物理升主主流程不切换模式、不重启，也不因 Preserve 再走一套升主入口。

### 3.4 Receiver 完成后没有立即升主

receiver 已发布 selection 并完成临时资源清理，但 HA 暂时没有触发升主：

- receiver worker 应全部退出。
- worker count、active、queued、inflight 和 deferred 均应归零。
- READY selection、strict prepared token、解析后的 promotion 对象和 TTL 继续保留。
- 后续 promotion 直接消费这些进程内对象，不需要重新启动 receiver worker。
- TTL 到期由现有全局 Preserve reaper 销毁 promotion-owned 对象。

### 3.5 Source Phase2 部分失败

一个 epoch 声明了 1000 个 token：

- 995 个 token 完成 source Preserve、transfer 和 final metadata。
- 5 个 token 在 Phase2 发生能够隔离的 token-local 失败。

目标行为：

1. source 先把这 5 个事务恢复到明确的普通事务所有权。
2. source 对这 5 个 token 逐个发送既有 `ABORT` frame。
3. 只有 `ABORT` 明确成功的 token 才进入 `SOURCE_EXCLUDED`。
4. source 使用其余 995 个 token 构造 terminal fact 并发送 `COMMIT_EPOCH`。
5. receiver 的 accepted fact 只能包含这 995 个 survivor。
6. DRAIN 结构化结果返回 `SUCCESS_WITH_EXCLUSIONS`，分别列出 survivor 与 source exclusion reason。

如果单 token source 恢复失败、`ABORT` 失败或结果不明确，则不能伪装成部分成功，必须升级为既有整 epoch fail-closed。

### 3.6 Transfer 完成但 Source 进程继续存活

在 `STANDBY_TRANSFER_SAVE` 下，receiver 已返回 FINAL_ACK，DRAIN 控制连接已经
取得结构化结果，但 HA 尚未关闭或重建旧 source：

- source mysqld 进程继续运行，DRAIN 不调用 `shutdown()` 或 `kill_mysql()`。
- FINAL_ACK 仍是所有权交接点；source 上 transferred 事务不得恢复、回滚、
  commit、重新 attach 或再次提供 SQL RESUME。
- 原业务 THD 保持 `PRESERVED_DRAINED`，新的普通业务命令也由
  `TRANSFER_HANDOFF_COMPLETE` manager fence 拒绝。
- HA 专用控制连接仍可查询诊断并显式执行后续 `SHUTDOWN`；高权限控制连接
  不得承载业务 SQL。
- source 只允许清理不影响事务所有权的 frame sink、source epoch session、
  warmcopy 临时文件等资源；耗时项复用既有 deferred cleanup。事务本体、
  Undo 和锁不能在 source 上主动销毁，其最终释放依赖 HA 后续停机或节点
  重建。

因此“DRAIN 不关进程”不等于“原主立即恢复服务”。RESET 只在 FINAL_ACK 前
按既有合同生效；FINAL_ACK 后 source 必须保持 fenced，避免 receiver 已接管时
出现双重所有权。

这里的 `TRANSFER_HANDOFF_COMPLETE` 只保证**当前 source 进程生命周期内**
的内核命令 fence，不是跨 mysqld 重启的持久化 fence。允许
`STANDBY_TRANSFER_SAVE` 成功返回的外部前提是：HA 已经撤销旧 source 的
流量/写入资格，并保证该节点不能原地重启后重新作为主库使用；后续只能显式
停机、降级或重建。若物理备机项目不能提供这个进程外节点 fence，则必须另行
设计 durable handoff marker，本切片不得以进程内 manager 状态替代它。

## 4. 当前代码事实

### 4.0 Transfer 成功尾部仍会自动 shutdown

当前 HEAD 的 `STANDBY_TRANSFER_SAVE` 在 FINAL_ACK 后仍进入共用
`finish_with_shutdown()`，发布结构化结果后转入 `SHUTDOWN_REQUESTED` 并调用
`shutdown()`。本文后续的 `TRANSFER_HANDOFF_COMPLETE` 是待实现目标，不是
当前事实。`LOCAL_CARRIER` 的这条 shutdown 行为保持不变。

### 4.1 Epoch READY 当前是 all-or-nothing

`sql/preserve_trx_transfer.cc` 中的 READY 发布逻辑当前要求：

- epoch fact token 数与 accepted token 数完全一致；
- 每个 strict prepared token 都处于 `READY_FOR_GATE`；
- 所有 token 绑定完成后才调用 `mark_accepted_epoch_ready()`。

因此一个 token 未 READY 会阻止整个 accepted epoch 进入 `READY`。

### 4.2 Physical bootstrap 当前消费整个 Epoch

`preserved_trx_prepare_before_trx_sys_init_for_physical_promotion()` 当前：

- 立即尝试取得整个 accepted epoch promotion lease；
- 要求 fact token 数与 accepted token 数一致；
- 为 fact 中的全部 token 构造 gate key；
- 将全部 gate key 的数量传给
  `update_epoch_prepare_deadline()`，该接口同时要求 registry 中同 epoch
  entry 数量完全相等；
- 使用全部 gate key 调用 `pin_epoch_for_physical_promotion()`；
- 为全部 token 注册 Resurrection candidate；
- 任一 token 不满足条件即返回 `REGISTRY_NOT_READY`。

它当前没有等待 receiver READY，也没有 per-token survivor/excluded 分流。

现有 strict adopt executor 并不要求 `request.tokens` 等于 epoch fact 全集：
它按显式 `request.tokens` 逐项校验 prepared publication、构造 digest input、
执行 adopt 和 reversal。每个 token 仍绑定原始 `epoch_fact_digest`。因此方案 2
无需重写 strict executor；需要修改的是 bootstrap 的候选集构造：

```text
accepted.tokens == fact.tokens          // 仍做全集真实性校验
selection.ready + selection.excluded
    == fact.tokens                       // 仍做完整 partition 校验
gate_request.tokens == final_survivors  // 改为经过认证的 READY 子集
```

`selection.ready` 不是一份新的 epoch fact，也不能改变原始 fact digest。

### 4.3 原生 `trx_sys` 构建已经具备 Candidate/Fallback 分流

`storage/innobase/trx/trx0trx.cc` 中：

- `trx_preserve_startup_resurrection_is_candidate(trx_id)` 为真时，先延后 Undo body 扫描。
- candidate 认证成功时注入 exact table IDs，不扫描 Undo body。
- candidate 认证失败时回到 `trx_resurrect_table_ids()`。
- 非 candidate 直接调用 `trx_resurrect_table_ids()`，执行传统 Undo body 扫描。

因此不需要为 RECEIVER_EXCLUDED token 新写一套 trx list 构建器。

### 4.4 非 Candidate 的 Preserve PREPARED 不会自动回滚

Preserve freeze 将 Undo header 置为 `TRX_UNDO_PREPARED`，并使用 Preserve magic XID。

`trx_sys_init_at_db_start()` 即使对该事务执行了传统 Undo body 扫描，仍会把它构建为 `TRX_STATE_PREPARED`。原生 `trx_recovery_rollback_thread()` 只回滚 `TRX_STATE_ACTIVE` recovered transaction，不会回滚 `PREPARED/PRESERVED`。

所以“未注册 candidate”只解决传统信息重建，不足以完成传统事务回滚。必须增加一个很窄的 Preserve 内部交接动作：

- 精确找到 RECEIVER_EXCLUDED token 对应的 recovered Preserve PREPARED `trx_t`；
- 恢复为 native recovered ACTIVE/Undo ACTIVE；
- 之后交给现有 `trx_recovery_rollback_thread()`；
- 不在 promotion gate 中同步执行大事务全量回滚。

现有 `trx_preserve_reactivate_prepared_in_original_thd()` 和 `trx_preserve_activate_undo_state()` 已经提供状态及 Undo 激活逻辑，应抽取并复用，而不是复制第二套状态转换。

### 4.5 Worker 当前只在 mysqld Shutdown 时退出

receiver prewarm worker：

- 由 `ensure_receiver_prewarm_workers_locked()` 按需创建；
- 空闲后继续等待 condition variable；
- 只有 `g_receiver_prewarm_shutdown` 才退出；
- `preserve_trx_transfer_shutdown_receiver_prewarm_workers()` 当前只在 mysqld `clean_up()` 中调用。

现有完整 shutdown 还会清除 READY cache、strict prepared registry 等 promotion-owned 对象，不能直接用于运行时自动停止。

### 4.6 Promotion Completion 当前可能过早删除 Accepted Epoch

`complete_accepted_epoch_promotion_lease()` 当前直接从 `m_accepted_epochs` 删除 epoch。

如果 promotion 在 READY 后立即开始，而最后的 staging finalizer 或 cleanup retry 尚未结束，后续清理可能因找不到 accepted epoch 而失去所有权依据，留下 `RECEIVING`、staging 或 cleanup debt。

### 4.7 Source 已有窄范围单 Token 排除，但尚未普遍使用

当前 source/receiver 协议已经具备 survivor COMMIT 的基础能力：

- closing command timeout 且
  `preserve_trx_drain_command_timeout_fail_batch=OFF` 时，source 调用
  `abort_token(token, "closing_command_timeout")`。
- Phase1 target 被移除时，source 调用
  `abort_token(token, "source_phase1_target_removed")`。
- receiver 收到 `ABORT` 后只将对应 record 标记为 `ABORTED` 并清理其 staging。
- receiver 构造 COMMIT snapshot 时跳过 `ABORTED` record。
- source terminal fact 只由 finalized manifest 构造，因此不包含 aborted token。

但多数其他 Phase2 per-target 失败仍由 coordinator 调用
`abort_batch_transfer_epoch()`，即使其他 token 已经成功也会放弃整个 epoch。
这不是 wire 限制，而是当前 source coordinator 的保守策略。

现有回归已经覆盖：

- `batch_drain_closing_timeout_exclusion`：一个 token ABORT，survivor 继续 COMMIT。
- `batch_drain_closing_timeout_abort_failure`：单 token ABORT 无法确认时升级为整批失败。
- `batch_drain_closing_timeout_all_excluded`：没有 survivor 时不发送空 COMMIT。
- `TransferSourceEpochSessionAbortsRemovedTargetsBeforeCommit`：receiver accepted fact 不包含 source-aborted token。

当前 Python receiver E2E 仍要求 accepted fact 中全部 token READY，尚未覆盖
“source exclusion + receiver partial READY”的组合路径。

### 4.8 Identity 当前发生在重型 Prewarm 之后

当前 `run_receiver_staged_token_prewarm_job()` 的执行顺序是：

1. `preserve_trx_transfer_load_standby_bundle_from_staging()` 加载并解码 snapshot bundle。
2. 执行 record-lock ready-cache 或 binlog/record-lock prewarm。
3. 最后调用 `prepare_strict_bundle_for_receiver()`。
4. `prepare_strict_bundle_for_receiver()` 内才调用
   `resurrection_index_matches_receiver_bundle()` 完成 identity 认证。

此外，`receiver_staged_token_prewarm_job_runnable()` 会在 binlog prepare
pending 或 record-lock object proof 未 READY 时阻止 staged job 运行。有限
worker 下，队列深处的 token 可能在 PREWARM cutoff 前从未执行到 identity
认证。仅把 identity 记录语句移动到重型函数内部之前，不能解决尚未启动的
job。

因此部分 READY 的硬前置不是“已经开始的 job 尽早记录 identity”，而是：
**所有 accepted fact token 必须先完成不会被重型 prewarm 饿死的轻量
identity pass；只有 identity 完整后，重型 prewarm 未完成的 token 才能安全
进入 RECEIVER_EXCLUDED。**

### 4.9 Transfer 成功当前仍复用 Shutdown 收尾

当前 `sql/preserve_trx.cc` 在 FINAL_ACK 后虽然已经释放 source transfer
session、frame sink 和 warmcopy provider，最终仍无条件进入
`finish_with_shutdown()`：

1. ownership 进入 `SHUTDOWN_HANDOFF`。
2. manager 进入 `SHUTDOWN_REQUESTED`。
3. 调用 `shutdown(thd, SHUTDOWN_DEFAULT, ...)`。
4. `sql/sql_parse.cc::shutdown()` 最终调用 `kill_mysql()`。

`standby_transfer_streaming_enabled` 当前只影响结构化 DRAIN 结果和 transfer
资源清理，没有“不关闭 source”的成功分支。现有 E2E 也把 source 断连/退出
作为 DRAIN 后置事件。相对于“HA 管控 source 生命周期”的 transfer 合同，
这是本轮必须修正的确定性行为，不是低概率竞态。

## 5. 设计边界

### 5.1 本轮实现

- source Phase2 对可证明的 token-local 失败复用现有单 token
  `ABORT`，成功 token 继续形成 terminal fact。
- source exclusion 只有在事务所有权恢复和 receiver `ABORT` 均明确成功后才能发布。
- `STANDBY_TRANSFER_SAVE` 在 FINAL_ACK 后进入专用 transfer terminal，
  返回 DRAIN 结果但不关闭 source mysqld。
- receiver 在 token 全部对象 sealed、入重型 prewarm 队列前执行 bounded
  identity-only pass；identity 不经过 worker 队列。
- receiver 按 token 形成 READY candidate 与 RECEIVER_EXCLUDED 分类。
- epoch READY 表示 receiver 已完成并冻结分类，不再表示 accepted tokens 全部可 resume。
- physical bootstrap 等待分类完成，但物理升主主流程保持不变。
- 只为 READY candidate 注册 Resurrection Index。
- RECEIVER_EXCLUDED token 走传统 Undo body 扫描，并转交原生 recovered rollback。
- receiver 完成临时资源清理后自动退出 worker。
- 修复 accepted epoch 过早删除与 cleanup retry 残留。

### 5.2 本轮不实现

- 不修改 transfer protocol v1、frame、epoch fact 或 FINAL_ACK 格式。
- 不让 source 在 FINAL_ACK 后参与 receiver 生命周期。
- 不让 FINAL_ACK 后仍存活的 source 恢复、回滚或主动销毁 transferred
  事务，也不让其恢复业务写入。
- 不增加 receiver crash replay、fsync 或 startup spool replay。
- 不改变本地 shutdown/startup 的 durable carrier 语义。
- 不实现用户临时表的物理备机 transfer。
- 不改变原生 XA PREPARED 的恢复语义。
- 不修改 SQL `RESUME PRESERVED TRANSACTION` 语法。
- 不把 strict adopt 中的全局 fence/原子 reversal 改造成另一套 per-token gate。
- 不实现真实 physical apply/freeze provider 和 HA write-enable。
- 不实现 HA 角色切换、source 在线重新加入或免重建复用；这些属于外部
  物理备机项目。

### 5.3 方案比较与正式选择

本设计只比较三种能够直接落到当前代码结构的方案：

| 方案 | 优点 | 缺点 | 裁定 |
|---|---|---|---|
| 方案 0：保持 accepted epoch 全量 READY | 代码最少，保持现状 | 一个 token 未 READY 会放弃其余全部 token，不满足需求 | 不采用 |
| 方案 1：增加独立 selection/runtime/lifecycle 抽象 | 容易表达通用部分成功 | 状态和所有权重复，代码面过大 | 不采用 |
| **方案 2：accepted epoch 内联分类 + join-only stop** | 不改 wire；复用现有 registry、ABORT、startup 与 SQL RESUME | 需要补严格的 pre-final allowlist、批量 rollback handoff 和生命周期竞态 | **唯一采用** |

方案 2 不是通用的部分成功协议。source 只复用当前已经存在的 per-token
`ABORT` 能力，不新增 frame 或 epoch；receiver 分类严格限制在 FINAL_ACK
之后、accepted fact 内部。两层分别解决各自时序窗口的问题，不能互相代替。

## 6. 核心语义

### 6.1 两层排除模型

一次 transfer 的 token 集合按两个线性化边界分流：

```text
source declared D
  SOURCE_EXCLUDED Xs
  source survivors S = D - Xs

COMMIT_EPOCH / FINAL_ACK:
  accepted.tokens = fact.tokens = S

receiver classification:
  READY R
  RECEIVER_EXCLUDED Xr = S - R
```

必须满足：

```text
Xs ∩ fact.tokens = empty
accepted.tokens == fact.tokens
R ∩ Xr = empty
R ∪ Xr = fact.tokens
快速 SQL RESUME 集合 = R
传统恢复集合 = Xs ∪ Xr
```

`SOURCE_EXCLUDED` 不是 receiver selection 的输入。receiver 不知道也不需要恢复
source 已经 ABORT 的 transfer 对象；对应事务通过物理复制得到的 Undo 和页面进入普通恢复。

### 6.2 Epoch READY 的新定义

accepted epoch 的 `READY` 改为：

> receiver 已经对 fact 中的每个 token 给出不可逆的最终分类，并把该分类
> 原子发布到 accepted epoch。

`READY` 不再等价于“所有 token 都可 resume”。

不增加独立 selection 对象或 `shared_ptr`。直接扩展
`Preserve_trx_transfer_accepted_epoch`：

```cpp
std::vector<uint64_t> ready_tokens;
std::vector<Preserve_trx_receiver_excluded_token> excluded_tokens;
std::vector<Preserve_trx_resurrection_index_entry>
    authenticated_resurrection_entries;
uint64_t selection_binding_generation{0};
bool selection_published{false};
bool promotion_completed{false};
```

`Preserve_trx_receiver_excluded_token` 仅需要：

```cpp
struct Preserve_trx_receiver_excluded_token {
  uint64_t token;
  Preserve_trx_receiver_exclusion_reason reason;
};
```

首版 reason 收敛为：

- `PREWARM_DEADLINE_NOT_READY`
- `PREWARM_UNSUPPORTED_AFTER_IDENTITY`

`authenticated_resurrection_entries` 与分类使用同一个 accepted lease
生命周期，避免再维护第二份 transient identity registry。以上字段均为当前
进程内对象，不进入 wire、artifact 或 codec，不增加 fsync。

receiver 从现有 `resurrection_index_matches_receiver_bundle()` 抽取轻量
identity-only helper。在 `SEAL_OBJECT` 处理确认 token 全部对象 sealed 后、
调用 `enqueue_receiver_staged_token_prewarm()` 前同步认证 manifest-bound
identity，并记入现有 generation-local receiver ready state；fact 到达后再做
O(token count) 绑定，selection 发布时将 fact-bound identity 复制到 accepted
epoch 并清理临时副本。该顺序完全脱离 staged heavy worker 排队。

这里不改变现有 per-object prewarm：record-lock/binlog object 在各自 seal 后
仍可按当前逻辑提前入队和执行，其结果在 selection 发布前只是 provisional。
identity-first 只约束最终 staged-token job 和 READY publication，不能为了
identity 认证而串行化已有的对象级预热。

#### 6.2.1 Identity-first 是部分 READY 的硬前置

首版把 identity 认证前移到现有 sealed-token admission 边界，不增加第二套
worker pool、queue、registry 或 lifecycle：

```text
sealed snapshot + Resurrection Index + manifest
  -> SEAL_OBJECT/all_objects_sealed admission
       - 解码并认证 Index
       - 校验 epoch/token/XID、prepare LSN 和 snapshot digest
       - 校验 trx_id/Undo anchors/modified table ids 的结构合法性
       - 绑定完整 manifest digest 与 receiver process generation
       - 写入现有 generation-local receiver ready state
       - 成功后才 enqueue 原有 staged heavy prewarm

worker:
  -> 原有 heavy prewarm
       - 复用已认证 Index entry
       - 完成 modified-table count 等 bundle 交叉复验
       - binlog-cache/record-lock materialization
       - strict prepared publication

COMMIT_EPOCH fact 到达:
  -> O(token count) 将 manifest-bound identity 绑定到 exact fact token
  -> 缺失、重复或 manifest digest 不匹配立即 fail closed

selection publication
  -> 同时消费 fact-bound identity 与当时的 heavy prewarm terminal state
```

调度必须满足：

- identity-only helper 只在 strict token 全部对象 sealed 后调用；认证失败时
  不 enqueue staged heavy job，并按 token/epoch integrity 规则 fail closed。
- staged-token heavy job 只有在该 token 的 manifest-bound identity 已认证后
  才可入队；现有 per-object prewarm 不受此顺序约束，也不能据此提前发布
  READY。
- identity 不占用 prewarm worker，因此即使全部 worker 正在执行重型任务，
  后续 token 也能在各自 sealed admission 边界完成 identity。
- accepted fact 到达后只做 identity-to-fact 的有界 O(token count) 绑定，不
  重新读取 payload 或重复执行 heavy prewarm。
- identity-only pass 只读取 receiver registry 中已经 sealed 且有严格上限的
  Resurrection Index 内存 payload，不做 DD/锁物化、不加载 external blob、
  不执行网络 I/O 或大对象搬运。
- strict v1 路径必须直接读取已经由
  `Preserve_trx_transfer_receiver_registry::read_strict_v1_object()` 持有的
  内存 payload；非 strict manifest 在该 helper 中直接拒绝，禁止经通用
  accessor 回退到 staging file。
- identity-only pass 不声称独立验证 Index 内每个 trx_id/Undo anchor 的物理
  对象真实性；它们由已校验的 Index object digest 和随后 fact 中的完整
  manifest digest 约束。需要 snapshot bundle 才能完成的
  `modified_table_ids.size()` 与 `mod_tables_count` 等交叉检查，继续由原
  staged-token path 执行。
- staged-token prewarm 仍要复验 bundle metadata 与 Index 的一致性；
  identity 完成不等价于 token READY。

首版不采用“identity job 提高 worker 优先级”的实现：它无法抢占已经被重型
任务占满的全部 worker，仍可能在 cutoff 前饿死；同步的 sealed-token
admission 才能在不增加第二个线程池的前提下闭合该窗口。

cutoff 时的裁决固定为：

```text
identity 完整 + READY_FOR_GATE
  -> READY

identity 完整 + heavy prewarm 未完成/已知可排除不支持项
  -> RECEIVER_EXCLUDED

identity 缺失、重复、损坏或与 fact 不一致
  -> epoch-global fail closed
```

“通过部署扩大 PREWARM deadline 保证 identity 全部完成”不能替代上述
sealed admission 正确性。实现若需要专用 identity 线程池/队列、独立 identity
registry 或新的 accepted lifecycle，必须停止并重新评审，而不是扩大方案 2。

### 6.3 Receiver 分类不变量

selection 发布时必须满足：

```text
ready_tokens ∩ excluded_tokens = empty
ready_tokens ∪ excluded_tokens = accepted.tokens
每个 accepted token 恰好出现一次
accepted.fact_digest 保持不变
receiver_process_generation 匹配当前进程
每个 READY/EXCLUDED token 都有唯一的 authenticated resurrection identity
```

发布后：

- RECEIVER_EXCLUDED 不得重新变为 READY。
- READY 不得被 receiver worker 再降级。
- receiver worker 不得再修改 promotion-owned token object。
- 晚到的 job 只能被丢弃和清理，不能修改 selection。

### 6.4 Token-local 与 Epoch-global 失败

source 在 COMMIT 前只有满足以下条件的失败才能成为 `SOURCE_EXCLUDED`：

- 既有 closing command timeout，且现有参数明确允许排除。
- 首版唯一新增 production origin 是
  `standby_transfer_resurrection_facts_unsupported`：事务已经完成 Preserve
  prepare，但无法导出 strict transfer 所需 resurrection facts；现有失败路径
  必须先把事务完整恢复为原 THD 持有的 ACTIVE 事务，且尚未发布 final
  metadata。只有该精确出口可以显式设置 `PRE_FINAL_ISOLATED`。
- source 能把失败事务恢复到明确的普通事务所有权。
- receiver 对该 token 的 `ABORT` 明确返回成功。

现有 `preserve_trx_drain_command_timeout_fail_batch` 只裁决 CLOSING 旧 command
超时：`ON` 保持整批失败，`OFF` 才允许超时 token 排除。它不控制非超时的
`SOURCE_PRE_FINAL_TARGET_FAILURE`。后者是否可排除只由上述封闭 allowlist、
source 所有权恢复和单 token ABORT 的明确结果决定；不增加第三个参数。

`source_exclusion_eligible` 是封闭 allowlist，不从 `failure_reason` 字符串、
stage 大小关系或最终 owner 反推。首版只有已经用确定性测试证明没有修改
participant、fence、session 或其他 token 的失败出口才能设置它。
`finalize_token_manifest()` 已执行、final metadata 已排队或 token ACK 状态不明确
时，绝不允许单 token ABORT。

现有 `source_phase1_target_removed` 扫描允许 `abort_token()` 返回
`UNSUPPORTED`，只用于容忍同一个已移除 token 被 Phase1 重复扫描、此前已经
ABORT 的幂等清理；该状态也可能表示未声明、已 finalized 或 COMMIT 已开始，
因此不能作为新排除成功的所有权证明。既有 closing timeout 和本轮新增的
`SOURCE_PRE_FINAL_TARGET_FAILURE` 都必须取得本次 `ABORT` 的明确 `OK`，
这种不对称是有意保留，不能抽象成统一的宽容规则。

receiver 在 accepted fact 发布后，以下属于 token-local，可成为
`RECEIVER_EXCLUDED`：

- PREWARM deadline 到达时仍未 `READY_FOR_GATE`，但认证身份完整。
- 身份认证完成后才发现首版明确不支持的 token 对象。

以下仍属于 epoch-global，不能选择性继续：

- source 无法恢复失败事务的明确所有权。
- 单 token `ABORT` 失败、ACK 不明确或 receiver 状态无法查询确认。
- participant 共享 prepare/close/fence 失败。
- transfer session、sequence、frame 或 COMMIT_EPOCH 失败。
- epoch fact digest 不成立。
- accepted token 集合与 fact token 集合不一致。
- receiver process generation 不一致。
- source fence/physical fence 不成立。
- TRX_SYS durable store fact 不成立。
- 重复 token、重复 XID 或跨 token 身份冲突。
- 单 token 资源不足、语义校验失败或未知错误，除非后续另行评审并加入
  上述封闭 allowlist。

object/body digest、terminal fact digest 或 frame integrity 在 accepted fact 发布前失败，
仍属于 epoch-global；不能把 transport corruption 降级成 receiver token exclusion。

epoch-global 失败只表示 Preserve selection 不可信。物理备机项目仍按原有物理升主错误处理合同决定是否继续普通恢复，不由 receiver 创建第二套升主模式。

### 6.5 Source Phase2 最小实现

本轮不重写 Phase2 worker，也不把所有失败强行改成部分成功。只在现有
per-target 结果汇合点增加失败作用域裁决：

```text
target result = success
  -> 保留为 survivor

target result = token-local failure
  -> 恢复该 source transaction
  -> 发送既有 ABORT(token)
  -> ABORT 明确成功
       -> SOURCE_EXCLUDED
       -> 从 finalized/fact 输入集合移除
     否则
       -> abort_batch_transfer_epoch()

target result = epoch-global failure
  -> abort_batch_transfer_epoch()
```

source 恢复复用现有 batch reattach/reactivate/cleanup helper，不新增第二套事务状态转换。
receiver 继续复用现有 `ABORT` handler、`ABORTED` record 和 COMMIT snapshot
过滤逻辑，不修改 transfer protocol v1。

首版只推广上述已经能证明隔离、恢复和 ABORT 成功的
`standby_transfer_resurrection_facts_unsupported`。其它 validation、
binlog、lock、participant、fence、snapshot、identity、session 和 COMMIT
失败即使表面上也发生在 final metadata 前，仍保持整 epoch fail-closed；后续
扩展必须逐个 origin 评审，不能用 stage 或 failure string 泛化。
后续若要增加新的 token-local reason，必须先增加确定性故障用例证明它不污染
其他 token，不能按错误字符串逐项放宽。

### 6.6 内部 READY、Wire ACK 与 HA 可见性

必须区分三个不同合同：

1. accepted epoch 内部 lifecycle `READY`：表示 immutable
   READY/RECEIVER_EXCLUDED selection 已发布；允许 READY 子集为空。
2. strict token `READY_FOR_GATE`：只描述 selection 中的 READY token。
3. wire `QUERY_EPOCH_STATUS`：只处理 source 的 FINAL_ACK ambiguity，当前
   strict 路径对已接受 epoch 固定返回 `COMMITTED_NOT_READY`，不查询内部
   lifecycle 或 ready cache。

本设计不改变 `QUERY_EPOCH_STATUS`、`COMMITTED_READY`/
`COMMITTED_NOT_READY` wire 编码，也不让 source 在 FINAL_ACK 后轮询
selection。内部 lifecycle 已进入 READY 时，strict wire query 仍可以返回
`COMMITTED_NOT_READY`；这不是矛盾。

HA 不需要在升主前通过全局 STATUS 查询 READY 比例来保证正确性。物理升主
调用取得不可变 selection 后，由本设计扩展的现有 gate result 返回
candidate、excluded、fallback 和 adopted 计数；升主编排始终为同一条流程。若未来产品要在多个
候选节点之间按 READY 比例选主，必须另行设计 epoch-scoped 控制接口，不能
用非 epoch 作用域的全局计数代替。

发布实现和文档定稿前，以下受影响合同文档必须采用同一表述；本次修订只改
本文和对应实施计划，不直接改写这些文档：

- receiver TTL 的 `READY_DEADLINE_ACTIVE` 持有的是 immutable selection，
  不再隐含全部 token 为 `READY_FOR_GATE`。
- receiver UML/可观测性文档必须注明 strict status query 不返回内部 READY。
- 物理升主指南中的 all-or-nothing 只约束最终 READY survivor 子集，excluded
  token 走 native handoff/rollback。

本文与对应实施计划是本切片的实施权威。上述派生文档尚未同步不阻塞 Task 3
编码，但阻塞 release 文档定稿和对外 HA 合同签署；实施者不得反向采用派生
文档中的旧 all-ready 语义。

### 6.7 `STANDBY_TRANSFER_SAVE` 的 Source 终态

首版只在现有状态机中增加两个明确值，不增加第二套 ownership 状态机：

```text
Preserve_trx_drain_terminal::TRANSFER_HANDOFF
Preserve_trx_manager_state::TRANSFER_HANDOFF_COMPLETE
```

`acknowledge_commit()` 在 strict transfer FINAL_ACK 成功后进入
`TRANSFER_HANDOFF`；该状态与 `SHUTDOWN_HANDOFF` 一样禁止 source restore，
但不表示 mysqld shutdown 已经开始。`SHUTDOWN_HANDOFF` 继续只服务需要关闭
进程的本地路径。

source 成功收尾固定为：

```text
FINAL_ACK
  -> ownership = TRANSFER_HANDOFF
  -> 释放 source epoch session/frame sink/warmcopy 临时资源
  -> manager = TRANSFER_HANDOFF_COMPLETE, owner_thread_id = 0
  -> 发送结构化 DRAIN result
  -> DRAIN 返回成功
```

因此 HA 收到成功结果时，source 必须已经发布不可恢复且 default-deny 的稳定
终态。若结果发送失败，source 仍保持该终态；不能反向恢复 transferred
transaction。

该分支明确禁止调用：

```text
shutdown()
kill_mysql()
preserved_trx_defer_shutdown_signal()
source transaction restore/rollback/reactivate
manager -> IDLE
```

`TRANSFER_HANDOFF_COMPLETE` 复用 CLOSING 的 default-deny command gate：
普通协议和 SQL 命令继续返回既有 4020，`PRESERVED_DRAINED` 会话保持不可用；
只有现有 HA control connection 豁免，可执行诊断和显式 `SHUTDOWN`。如果
`COMMIT_UNKNOWN` 后由 HA proof 确认 receiver owns，也进入同一 manager
终态；确认 source owns 才可走既有 restore-to-IDLE。

空批次也必须有明确终态：

- 初始 `target_count == 0`：没有 token 所有权需要交接，但 planned switchover
  仍需关闭 source 写入窗口；在 abort 空 receiver epoch 后进入同一个
  `TRANSFER_HANDOFF_COMPLETE` manager fence 并返回空成功结果，不调用
  shutdown。
- 有候选但最终 survivor 为空：不发送空 `COMMIT_EPOCH`。先确认所有 token
  已恢复/ABORT；随后返回现有带 exclusions 的结果并进入
  `TRANSFER_HANDOFF_COMPLETE`。若任一恢复或 ABORT 不明确，则 DRAIN 失败并
  回到既有 fail-closed/restore 路径。
- 上述无 accepted epoch 的成功只表示 source 已完成 drain 并被当前进程
  fence；receiver 没有 Preserve READY token，升主后这些事务全部走原生物理
  recovery。外部 HA 节点 fence 前提仍然适用。

`LOCAL_CARRIER`、单机 shutdown/startup 和非 transfer DRAIN 继续执行原
`SHUTDOWN_REQUESTED -> shutdown()` 路径，不受该分流影响。receiver READY、
prewarm、TTL 和 promotion 不等待 source 后续停机。

## 7. Receiver 分类与自动停止

### 7.1 分类触发

receiver 在以下任一条件满足时尝试发布 selection：

1. fact 中全部 token identity 已认证，且全部 token 已经进入
   `READY_FOR_GATE` 或 token-local terminal state。
2. accepted epoch 的 PREWARMING deadline 到期。

deadline 到期时：

- 已经 `READY_FOR_GATE` 的 token 进入 `ready_tokens`。
- identity 完整但尚未 READY 的 token 进入 `excluded_tokens`。
- 任一 fact token identity 缺失、重复或不一致时，selection 不得发布，整个
  epoch fail closed。
- 分类在 accepted registry mutex 下原子冻结。

physical bootstrap 的 operation deadline 只限制该次调用等待 selection 的时间，
不能提前改变全局 PREWARM cutoff，也不能延长 READY TTL。bootstrap 等待超时
返回 Preserve 不可用状态，由外部 HA 保持原有 write-enable 栅栏；之后 receiver
仍可按全局 deadline 完成分类。

这里必须区分两个“超时”：

- receiver PREWARM cutoff 到达：立即冻结当时已经 READY 的 token，其余具备
  认证 identity 的 token 进入 RECEIVER_EXCLUDED；已 READY token 继续快速恢复。
- bootstrap operation deadline 先到：本次调用没有取得不可变 selection，不能
  临时读取一个仍在变化的 READY 子集；调用方不得启动 Preserve adopt。部署时
  应保证 operation deadline 覆盖预期 PREWARM cutoff，避免无意义地提前放弃。

同一 reaper pass 必须使用固定顺序：

```text
selection finalize
  -> selected prepared deadline publication
  -> accepted READY publication
  -> strict prepared expiry
  -> accepted TTL expiry
  -> cleanup retry
  -> worker idle-stop
```

不得保留“先 expire，再由普通晚调用者重新发布 READY”的分支。

### 7.2 READY Publication 是所有权切换点

READY 前：

- receiver worker 可以创建、替换和验证 prewarm 对象。

READY 后：

- READY token 的对象归 promotion registry。
- RECEIVER_EXCLUDED token 不再进入 prewarm。
- receiver worker 只允许清理 staging、raw payload、queue 和临时 proof。
- READY 后清理失败形成 cleanup debt，但不能撤销 selection。

因此 HA 可以在 READY 后开始 promotion，不需要等待 receiver 临时清理完成。

### 7.3 不增加第二套 Receiver Runtime 状态机

方案 2 复用当前已有的 `started/starting/stopping/shutdown` 与 worker queue。
只增加内部 `idle_stop_requested`，必要时再增加只由完整 mysqld shutdown 设置且
不回退的 `process_shutdown_started`。不新增
`IDLE_STOPPED/RUNNING/STOPPING` enum。

当前 receiver 没有一个需要常驻的独立“模块主线程”；transfer handler 随请求
进入，真正常驻的是 prewarm worker pool。因此“receiver 自动 stop”的精确定义
是：worker 全部 join、worker-owned queue/inflight/deferred/proof 清零，而
accepted classification、prepared promotion resources 和 TTL 转由现有 registry/
reaper 持有。不能为了让模块看起来停止而删除这些升主仍需使用的对象。

worker count 为 0 只表示线程池已经 join：

- transfer handler 仍可接收新的合法 epoch；
- 新 job 经现有 ensure 路径 lazy restart；
- accepted selection、prepared publication、promotion READY cache 和 TTL
  均不属于 worker runtime，不随 join 删除。

### 7.4 自动停止条件

只有同时满足以下条件才允许停止 worker：

```text
receiver staged-token queue == 0
receiver object queue == 0
receiver inflight/deferred job == 0
receiver active job == 0
正在进入 enqueue 的 admission refcount == 0
需要 worker 的 staging cleanup == 0
所有 worker 可能触达的 registry 均无未完成 worker-owned work
```

不能用 `m_online_epochs.size()` 代替该判断；online epoch 还承担 terminal
status retention，不等价于 receiver worker 工作。registry 应提供一个受自身
mutex 保护的 `has_unfinished_worker_owned_work()` 快照，不能维护无生命周期
保护的全局 raw-registry 列表。

cleanup debt 由全局 Preserve reaper 重试。如果 debt 的重试不需要 receiver
worker，则不应仅为了 debt 保留空闲 worker；但 join 前必须确认 debt 不再引用
worker-owned staging。

### 7.5 复用现有 Reaper 完成 Join

worker 不能安全地 `join()` 自己。

最小实现是：

1. 最后一个 receiver job/finalizer 设置 stop candidate。
2. 现有 `preserved_trx_expired_reaper_thread()` 每轮调用 receiver scan。
3. scan 判断自动停止条件。
4. 设置 `idle_stop_requested`，广播 receiver condition variable。
5. reaper 线程 `join()` 所有 receiver worker。
6. join 完成后只复位线程池启动状态；queue/inflight 在进入 stop 前必须已为空。
7. 保留 promotion selection、READY cache、strict prepared token 与 TTL。

worker 在持有 `g_receiver_prewarm_mutex` 后，必须先检查 shutdown/idle-stop，
再进入 paused、profile-limited 或普通空队列等待。所有带 predicate 的 wait
都必须观察 idle-stop；尤其 paused wait 不能只等待 shutdown/unpause。
profile 限流的 `wait_for(100ms)` 可继续复用，但 stop 的 `notify_all()` 唤醒后
必须回到顶部重新检查 idle-stop，不能再次进入限流等待。

不新增 stop thread 或 timer。

### 7.6 与完整 Mysqld Shutdown 分离

保留当前：

```cpp
preserve_trx_transfer_shutdown_receiver_prewarm_workers();
```

作为 mysqld shutdown 的完整销毁入口。

新增运行时内部入口，例如：

```cpp
bool stop_idle_receiver_prewarm_workers_if_possible();
```

该入口只做：

- 取得唯一 join owner；
- 停止并 join worker；
- 复位现有 started/starting/stopping 与线程计数。

它禁止清除：

- accepted epoch/fact；
- promotion selection；
- strict prepared token registry；
- promotion READY cache；
- TTL 和 process generation。

完整 mysqld shutdown 使用永久门闩阻止 lazy restart；idle-stop 不设置该门闩。

## 8. Physical Bootstrap 按 Token 分流

### 8.1 等待但不改变升主编排

`preserved_trx_prepare_before_trx_sys_init_for_physical_promotion()` 保持原调用位置：

```text
redo apply/freeze 完成
  -> prepare_before_trx_sys_init()
  -> trx_sys_init_at_db_start()
  -> resolve verified READY / RECEIVER_EXCLUDED
  -> RECEIVER_EXCLUDED batch handoff
  -> 必要的 dictionary recovered-transaction cleanup
  -> trx_sys_need_rollback() 一次性评估
  -> DD/MDL 就绪后 strict adopt READY survivors
  -> purge/write-enable
```

内部改为：

1. 将 `request.operation_deadline_us` 在入口处转换为一次绝对 monotonic deadline。
2. 等待 accepted epoch 发布 selection。
3. 使用 condition variable，不使用轮询或 sleep，也不引入新 sysvar。
4. 取得 selection 和 accepted epoch lease。
5. 只为 `ready_tokens` 构造 gate key。
6. 校验 accepted lease 中全部认证 entry 的 token/XID/trx_id/Undo anchors，
   并确认 `prepare_lsn` 非零且不超过 accepted source fence。
7. 只为 READY token 注册 Resurrection candidate；将 excluded token 的认证
   facts 保存到 bootstrap attempt，不注册 candidate。
8. 即使 `ready_tokens` 为空，也允许 bootstrap attempt 继续完成后续传统恢复交接。

没有“返回普通升主模式”的分支。

`ready_tokens` 为空时必须显式跳过 candidate registration 和 prepared-registry pin，不能把空集合传给要求非空 token 集的现有 helper。attempt 仍然有效，供 `trx_sys` 之后完成 RECEIVER_EXCLUDED 交接；strict adopt 返回成功的零 adopted no-op。

本仓库只实现并验证上述 Preserve 内核接口，不实现物理备机项目的生产调用点。
外部集成必须保证：

- receiver 进程内 accepted state 在 promotion 调用期间仍存在；若进程重启，
  按既有合同全部失效，不尝试重建 selection。
- redo apply/freeze 已完成后调用 pre-init 接口。
- `trx_sys_init_at_db_start()` 在该 promotion 生命周期中只执行一次。
- RECEIVER_EXCLUDED handoff 在原生 `trx_sys_need_rollback()` 一次性评估前完成。
- strict adopt 在 purge 和 write-enable 前完成；READY survivor 保持 PREPARED
  时不会被原生 rollback worker处理。
- 任一 Preserve handoff/adopt 失败时保持 write-enable 栅栏；本仓模拟器不能
  被描述为真实物理升主 E2E。

### 8.2 `trx_sys_init_at_db_start()` 的行为

READY token：

- 已注册 Resurrection candidate。
- 原生代码仍从 Undo header 构建 `trx_t` 和 rw trx list。
- 认证成功后注入 exact table IDs。
- 跳过 Undo body 扫描。

RECEIVER_EXCLUDED token：

- 没有注册 Resurrection candidate。
- 原生代码构建 `trx_t`。
- 执行 Undo body 扫描和传统 table-id 恢复。
- 此时仍是 Preserve magic `TRX_STATE_PREPARED`。

### 8.3 将 RECEIVER_EXCLUDED 交给原生 Recovered Rollback

不应在 promotion critical path 中调用 `trx_preserve_rollback_by_token()` 同步回滚大事务。正确做法是增加一个窄的 InnoDB 内部接口：

```cpp
dberr_t trx_preserve_release_recovered_prepared_batch_to_native_rollback(
    const std::vector<trx_preserve_resurrection_facts> &excluded);
```

该接口在 `trx_sys` mutex 下只扫描一次 `rw_trx_list`，构造 token/XID 到
`trx_t *` 的局部映射；不得为每个 excluded token 重新扫描链表。它复用现有
Preserve 状态转换和 Undo 激活逻辑，仅完成：

1. 根据 Preserve magic XID 找到每个 excluded token 的 recovered detached
   `trx_t`。
2. 批量验证 Preserve magic XID 和 trx_id 与 accepted lease 中的认证
   identity 一致。`prepare_lsn` 已在 pre-init 阶段验证为非零且不超过
   accepted source fence；非 candidate 的 recovered `trx_t` 不携带
   `preserve_prepare_lsn`，此处不得伪造第二次 actual-LSN 比较。
   Undo anchors 只用于 READY candidate 的快速认证；anchor mismatch 正是
   转入传统 Undo 扫描/rollback 的原因，不能在 handoff 中再次把它当成拒绝
   条件。
3. 要求状态为 `TRX_STATE_PREPARED`、`is_recovered=true`、
   `mysql_thd=nullptr` 且未 claim/adopt。
4. 显式拒绝 `ddl_operation`、字典事务和任何已经进入 DD recovery ownership
   的事务。
5. 全部验证成功后，在同一受控批次中递减 `trx_sys->n_prepared_trx` 并转为
   `TRX_STATE_ACTIVE`。
6. 在 Release 路径显式验证每个非空 Undo header 仍为
   `TRX_UNDO_PREPARED`，再复用 `trx_preserve_activate_undo_state()` 将
   redo/no-redo Undo 恢复为 ACTIVE，并保持 `is_recovered=true`。
7. 让既有 `trx_recovery_rollback_thread()` 按原生路径回滚。

该接口必须拒绝：

- 非 Preserve magic XID。
- 原生 XA PREPARED。
- 已 claim、已 adopt、已有 THD owner 的事务。
- token/XID 不匹配。

验证阶段必须全有或全无。当前 Undo activation helper 在前置状态合法时返回
`DB_SUCCESS`；首版按“预校验成功后无可恢复返回失败”使用，不为一个当前
不存在的可恢复失败分支增加 journal 或反向状态机。正确性不能依赖
Debug/Release 断言。该批次只提供当前进程内的无部分返回语义，不宣称 crash
原子性；状态转换期间进程 crash 后，receiver selection 也按现有合同整体
失效，外部 HA 不得据此 write-enable。若实现需要在首个事务发生状态修改后
加入真实可恢复失败分支，必须停止本切片并重新设计批量原子性，不能在部分
转换后继续执行。

这样既不复制 rollback kernel，也不让大事务回滚阻塞 promotion gate。

这一交接有严格调用时序：

```text
trx_sys_init_at_db_start()
  -> READY candidate verified/fallback 分区
  -> RECEIVER_EXCLUDED PREPARED -> recovered ACTIVE 状态交接
  -> srv_dict_recover_on_restart() 处理其余 dictionary recovery
  -> trx_sys_need_rollback() 一次性评估
  -> 必要时启动原生 recovery rollback worker
  -> DD/MDL 就绪后 strict adopt final survivors
  -> 允许 purge/write-enable
```

`trx_sys_need_rollback()` 只计算 `rw_trx_list` 中非 PREPARED 事务；纯 Preserve
PREPARED 集合会得到 false。通用 recovery rollback worker 只在
`srv_start_threads(false)` 中按该结果创建一次，因此“在 worker 启动前”还
不够，状态交接必须发生在这次判断之前。标准 startup 的首选可复用窗口是
`trx_sys_init_at_db_start()` 返回后、
`srv_dict_recover_on_restart()`/`trx_rollback_or_clean_recovered(FALSE)`
之前。这样被排除的普通 recovered 事务不会错过字典恢复阶段对 DDL 事务的
处理；batch helper 仍防御性拒绝 `ddl_operation`/字典事务。若物理备机项目
只能在 dictionary cleanup 之后调用，则必须先证明 accepted selection 不含
DDL/dictionary transaction，否则停止集成。

交接只改变事务及 Undo 的恢复状态，不在此处执行 Undo；真正的大事务回滚
仍由后续原生 recovery rollback worker 完成。READY survivor 继续保持
PREPARED，rollback worker 即使已经启动也不会处理它们，因此 strict adopt
仍可在 DD/MDL 就绪后执行。

首版物理备机集成必须提供上述调用窗口。如果其架构已经越过
`trx_sys_need_rollback()` 的一次性评估点，本计划不得重复调用整个
`srv_start_threads()`，也不得在 batch handoff helper 内偷偷启动线程；应停止
集成并单独评审一个窄的、幂等的 recovery-rollback-worker ensure 接口及其
线程所有权。

普通的 token 未 READY 是 token-local 分流；但若一个已确定 RECEIVER_EXCLUDED 的 Preserve PREPARED 事务无法安全交给原生 rollback，则属于恢复所有权不明确的正确性异常。此时不能继续把该 token 标成“传统恢复已接管”，也不能 write-enable。

### 8.4 Bootstrap 后置分流

`trx_sys_init_at_db_start()` 返回后，bootstrap attempt 做一次精确分区：

```text
READY candidate 且找到 verified trx_t
  -> final survivor

READY candidate 认证 fallback/找不到 verified trx_t
  -> 转入 RECEIVER_EXCLUDED
  -> 交给 native recovered rollback

预先 RECEIVER_EXCLUDED
  -> 交给 native recovered rollback
```

只有 final survivor 进入 strict adopt。

这意味着 Resurrection Index anchor 失配仍是 token-local fallback，不再因一个 verified pointer 缺失让全部 candidate 失败。

receiver selection 是 `trx_sys` 前的初始候选分类；它发布后保持不可变。anchor 验证发生在 `trx_sys` 构建期间，因此 bootstrap attempt 的 final survivor 集可以在 selection 基础上继续缩小，但不得扩大。新增 fallback token 只进入 attempt-local RECEIVER_EXCLUDED 集合，不回写 receiver selection。

### 8.5 Strict Adopt 保持现有原子边界

本轮只在 strict adopt 前形成 survivor 集合。

现有全量事实校验不能被删除：

```text
accepted.tokens == fact.tokens
selection.ready + selection.excluded == fact.tokens
final_survivors subset_of selection.ready
```

每个 survivor 的 prepared publication 继续携带原始 `epoch_fact_digest`、prepare/apply LSN、manifest/object digest 和 process generation；不能为 survivor 子集伪造一份新的 epoch fact。

现有 `update_epoch_prepare_deadline()` 会强制 registry 中同 epoch entry 数量等于传入数量，不能把 survivor count直接传给它。最小实现是在 prepared registry 增加一个 selection-key 版本，例如：

```cpp
Preserve_trx_prepared_status update_selected_prepare_deadline(
    const std::vector<Preserve_trx_prepared_token_key> &keys,
    uint64_t deadline_monotonic_us);
```

该接口只更新已由 selection 认证的 READY key，并继续校验同 epoch、无重复、publication identity一致和状态允许。现有 all-epoch helper 保持不变，避免影响本地 startup和当前全量 READY 路径。`pin_epoch_for_physical_promotion(keys)` 已按显式 key集合工作，可以直接复用；zero-survivor则跳过 update/pin。

进入 strict adopt 后：

- `gate_request.tokens` 与 `verified_transactions` 必须按相同顺序精确等于
  `final_survivors`，并证明其是不可变 `selection.ready` 的子集。
- strict executor 继续逐 token 对照原始 accepted epoch 的 fence、fact digest、
  process generation 和 prepared publication；不把子集改写成新的 epoch。
- physical fence、prepared pin、intent 和 reversal 仍按 survivor 集合保持原有严格原子性。
- 不在本轮把 strict adopt 的中途失败改成部分成功。
- adopt 成功后的 SQL RESUME 仍按 token 独立执行。

这是控制实现规模和正确性风险的关键边界。

## 9. Accepted Epoch 与 Cleanup 生命周期修正

### 9.1 不能过早 Erase

promotion 成功时不能立即删除 accepted epoch，但方案 2 不增加
`PROMOTION_COMPLETED_RETIRING` lifecycle enum。accepted epoch 只增加：

```cpp
bool promotion_completed{false};
```

`complete_accepted_epoch_promotion_lease()` 将该位设为 true。reaper 在 staging、
finalizer、late-worker cleanup 和 cleanup debt 全部 terminal 后调用
`retire_completed_epoch_if_clean()` 条件删除 accepted epoch。如果 promotion
开始前临时清理已经完成，completion 可以直接删除。

该布尔位只解决 cleanup ownership，不形成第二套 promotion 状态机。

### 9.2 Cleanup Retry 必须完整

cleanup retry 成功后必须同时清除同 epoch/token 的：

- receiver record；
- strict staging object；
- cleanup debt；
- frame/payload apply 临时状态；
- worker-owned prewarm proof。

不能只把 record 标成 `SAVED_ONLINE` 而保留 strict staging map。

### 9.3 Receiver Stop 与 Epoch TTL 解耦

receiver worker 停止后：

- READY selection 继续由 accepted epoch registry 持有。
- promotion lease 仍可取得。
- READY TTL 继续计时。
- TTL 到期由全局 reaper 清理 promotion-owned 对象。

因此 receiver stop 既不等待 promotion，也不影响 promotion。

## 10. 接口调整

### 10.1 Transfer/Receiver 内部接口

建议增加或调整：

```cpp
Preserve_trx_transfer_status
wait_and_acquire_receiver_promotion_selection(
    const std::string &root_dir,
    const std::string &epoch_id,
    uint64_t deadline_monotonic_us,
    Preserve_trx_transfer_accepted_epoch *accepted);

Preserve_trx_transfer_status
mark_accepted_epoch_ready_with_selection(
    const Preserve_trx_transfer_accepted_epoch &expected,
    uint64_t now_us,
    uint64_t ready_deadline_monotonic_us,
    const std::vector<uint64_t> &ready_tokens,
    const std::vector<Preserve_trx_receiver_excluded_token> &excluded_tokens,
    const std::vector<Preserve_trx_resurrection_index_entry>
        &authenticated_entries);

bool stop_idle_receiver_prewarm_workers_if_possible();
bool retire_completed_epoch_if_clean(
    const Preserve_trx_transfer_accepted_epoch &expected);
```

`expected` 至少绑定 root/epoch、fact digest、receiver process generation 和
现有 binding generation。分类字段直接存入 accepted epoch，不增加独立
selection 指针。

### 10.2 Promotion Bootstrap Attempt

attempt 内部保存：

```text
candidate token keys
excluded token ids/reasons
final verified survivor trx_t pointers
receiver accepted epoch lease
prepared token pin
```

不向 HA 暴露 `trx_t *`。

### 10.3 Gate Result

`Preserve_trx_physical_promotion_gate_result` 增加：

```cpp
uint64_t candidate_count;
uint64_t excluded_count;
uint64_t resurrection_fallback_count;
```

`adopted_count` 仍表示最终成功 adopt 的 survivor 数。

### 10.4 不新增的接口面

- 不新增 SQL。
- 不新增错误码。
- 不新增 wire frame。
- 不新增用户可见 sysvar。
- 不增加 `_for_unit_test` 生产接口。
- 不改变 strict `QUERY_EPOCH_STATUS` 的 ACK-only 语义。
- 不增加供 HA 升主前决策的全局 READY/EXCLUDED STATUS；现有 gate result
  返回当前 epoch 的计数。

## 11. 并发与锁顺序

### 11.1 Selection Publication

selection 复用现有 `binding/bound/binding_generation` 完成跨 registry
线性化，不增加完整 PUBLISHING 状态：

1. ready mutex 下将 `binding=true`，冻结 fact、READY 状态、认证 identity 和
   binding generation。
2. 释放 ready mutex，绑定 READY subset 并更新 selected prepared deadline。
3. accepted registry mutex 下按 expected fact digest、process generation 和
   binding generation 比较后，原子写入分类字段并设置 READY。
4. ready mutex 下将相同 generation 标为 `bound` 并 notify waiters。
5. 任一步失败只清理由相同 expected identity 创建的 derived state。

不能在持有 accepted registry mutex 时执行文件 I/O、DD open、lock-plan build 或 trx rollback。

### 11.2 Worker Stop

停止顺序：

1. receiver mutex 下取得唯一 stopping/join owner并设置
   `idle_stop_requested`。
2. 释放 mutex。
3. notify worker。
4. join worker。
5. receiver mutex 下复位现有启动状态和线程计数。

新 job 在 stopping 期间等待完成并重新检查完整 shutdown、registry retirement
和 dedupe；不能创建第二组并发 worker。

### 11.3 Promotion 与 Cleanup

READY publication 后：

- promotion 只读 immutable selection 和 promotion-owned object。
- cleanup 只操作 receiver-owned staging。
- 两者不得共享可变 payload。

这是 READY 后 promotion 不等待 cleanup 的基础。

## 12. 失败语义

| 场景 | Preserve 行为 | 物理升主行为 |
|---|---|---|
| source 单 token 失败且恢复、ABORT 均成功 | 仅该 token 进入 SOURCE_EXCLUDED，survivor 继续 COMMIT | 原流程继续 |
| source 单 token 恢复或 ABORT 无法确认 | 整 epoch fail-closed，不发送成功 FINAL_ACK | 按既有失败合同处理 |
| source epoch-global 证明失败 | 整 epoch fail-closed | 按既有失败合同处理 |
| transfer FINAL_ACK 成功 | source 进入 `TRANSFER_HANDOFF_COMPLETE` 并返回 DRAIN 结果；不 shutdown、不恢复 transferred trx | HA 继续角色降级、升主或显式停机 |
| FINAL_ACK 后 DRAIN 结果发送失败 | receiver ownership 不撤销；source 保持 fenced 并记录告警 | HA 通过 receiver/epoch 证据继续裁决，不能恢复 source 事务 |
| HA 后续显式关闭 source | 由 HA control connection 执行独立 `SHUTDOWN` | 不改变已经完成的 receiver ownership |
| 全部 token READY | 全部进入 survivor | 原流程继续 |
| accepted fact 中部分 token READY | READY 进入 survivor，其余 RECEIVER_EXCLUDED | 原流程继续 |
| survivor 为 0 | adopt no-op，全部传统恢复 | 原流程继续 |
| 单 token anchor fallback | 该 token 转 RECEIVER_EXCLUDED | 原流程继续 |
| RECEIVER_EXCLUDED rollback handoff 失败 | gate 失败并告警，不伪装已交接 | 不得 write-enable；由现有 HA 错误合同处理 |
| epoch fact 不可信 | 不生成 selection | 由物理备机项目按既有错误合同处理 |
| physical fence 不成立 | 不 adopt survivor | 由物理备机项目按既有错误合同处理 |
| receiver cleanup 失败 | READY 不撤销，形成 cleanup debt | promotion 不等待文件清理 |
| bootstrap operation deadline 先到 | 本次调用返回 selection 尚不可用，不改变全局 cutoff/TTL | 保持 write-enable 栅栏；调用方按既有升主合同处理 |
| selection 发布后晚到 worker 完成 | 丢弃其 READY 提升并清理同 identity 的 receiver-owned 派生资源 | 已冻结 survivor 集合不变 |
| receiver crash/restart | epoch/selection 全失效 | Preserve 不可用，现有 fail-closed 边界不变 |

## 13. 可观测性

首版不为方案 2 新建一组全局指标。优先复用：

- DRAIN 结构化结果中的 survivor/excluded 计数；
- promotion gate result 中的 candidate/excluded/fallback/adopted 计数；
- 现有 receiver worker count/active/queued、cleanup debt、accepted lifecycle
  和 prepared token 状态。

只增加两类无法从现有状态还原的聚合日志：

1. selection 发布日志：记录 accepted、READY 和 RECEIVER_EXCLUDED 数量。
2. selection/handoff 拒绝日志：记录阶段、状态和有界原因摘要。
3. source transfer handoff 完成日志：记录 epoch、survivor 数和
   `shutdown_requested=0`，用于证明 DRAIN 没有隐式关闭 source。

如果现有状态面无法统计 worker 自动停止次数，可增加一个内部累计计数；不得
为每种 exclusion reason、每个延迟阶段或第二套 receiver runtime state 增加
独立全局指标。source 侧继续复用 `SURVIVOR`、`EXCLUDED` 和
`SUCCESS_WITH_EXCLUSIONS`，不新增 SQL 列或错误码。

关键日志：

```text
PRESERVE: receiver promotion selection published
  epoch=...
  accepted=1000
  ready=990
  excluded=10

PRESERVE: physical promotion token classification consumed
  epoch=...
  indexed=988
  fallback=2
  native_rollback=12

PRESERVE: receiver prewarm workers stopped
  worker_count=0
  queued=0
  active=0

PRESERVE: standby transfer source handoff complete
  epoch=...
  survivors=995
  shutdown_requested=0
```

日志不得逐 token 打印正常成功项，避免大事务数下日志放大。

## 14. 精确修改范围与代码量

### 14.1 `sql/preserve_trx_transfer.h/.cc`、`sql/preserve_trx.cc`

修改：

- sealed-token identity admission、accepted epoch 内联 classification 与
  认证 identity；
- READY publication、condition-variable wait 和 reaper 固定顺序；
- `promotion_completed` 条件回收；
- worker-owned work 判断、join-only auto-stop 和 cleanup retry 收口；
- source pre-final allowlist 与 survivor terminal fact。
- transfer FINAL_ACK 后的 source terminal 分流和 command fence；本地
  shutdown 路径保持不变。

其中 transfer no-shutdown 分流预计净增 25 至 45 行；本节合计预计生产代码
255 至 400 行。

### 14.2 `sql/preserve_trx_promotion.h/.cc`、`sql/preserve_trx_promotion_prepared.h/.cc`

修改：

- bootstrap 保存 candidate/excluded；
- 只注册 READY candidates；
- `trx_sys` 后 verified/fallback 分流；
- 增加 selection-key deadline 更新，保留现有 all-epoch helper；
- survivor-only strict adopt；
- gate result 统计。

预计生产代码：75 至 115 行。

### 14.3 `storage/innobase/include/trx0preserve.h`

增加 production internal 接口声明：

```cpp
trx_preserve_release_recovered_prepared_batch_to_native_rollback()
```

预计生产代码：5 至 10 行。

### 14.4 `storage/innobase/trx/trx0preserve.cc`

修改：

- 抽取现有 PREPARED -> ACTIVE + Undo ACTIVE 共享逻辑；
- 实现一次 `rw_trx_list` 扫描的 batch recovered rollback handoff；
- 保持原生 XA 隔离。

预计生产代码：70 至 110 行。

### 14.5 最小日志与状态面

优先复用现有结果和 STATUS。只有确认现有状态无法表达 worker stop 次数时，
才修改 `sql/preserve_trx_resource.*`/`sql/mysqld.cc`，预计 10 至 20 行。

### 14.6 总量

预计生产代码净增目标约 **430 至 600 行**，**620 行是硬停止线**：

- source pre-final 精确排除：80 至 130 行；
- transfer terminal/no-shutdown 分流：25 至 45 行；
- sealed-token identity admission、receiver classification、reaper、cleanup
  与 auto-stop：
  150 至 230 行；
- promotion/prepared subset：75 至 115 行；
- InnoDB batch lookup/handoff：70 至 110 行；
- 必要日志：10 至 20 行。

identity-first 必须通过抽取现有 identity helper 并复用 sealed-token admission
落入 receiver 原预算；transfer no-shutdown 必须通过拆分现有成功收尾并复用
现有 command gate 落入 source 原预算。不能以这两项为由增加新协议、sysvar、
HA API 或 source cleanup 状态机。区间已按文件重叠扣减。外部 HA 调用点、
真实 redo/apply provider 和物理备机 E2E 不计入本仓工作量。600 至 620 行
只作为不可预见的并发/清理护栏余量；超过 620 行时必须停止扩展并重新评审
是否引入了方案 1 式的重复状态或接口。

## 15. 实施切片

### 强制切片审查门禁

S1、S1A 至 S6 每个可独立交付的生产代码切片完成后，必须先通过独立只读审查，
才能提交并进入下一切片。不能只在全部实现结束后做一次总审查，也不要求
每个函数或机械性小修改都单独启动审查。

审查开始前由主 session 冻结本切片证据：

```text
基线 commit
本切片精确文件清单
production diff/stat 与净增代码量
本切片解决的不变量和明确非目标
定向构建、GUnit、MTR 结果
受影响性能指标的同配置前后对比
git status --short -uall
```

审查期间不能继续修改工作树。sub-agent 只能读取源码、diff、测试日志和指标，
不得编辑文件、运行 record mode、执行会改变工作树的命令、提交或推送。

每个切片默认由两个相互独立的 reviewer 审查：

1. **正确性与异常路径 reviewer**
   - 审查所有权、状态迁移、publication 线性化点、锁顺序和幂等性。
   - 审查本切片相关的 late worker、TTL、cleanup debt、shutdown、zero READY、
     source ABORT 不明确、内存分配失败和 promotion 并发。
   - 确认失败后没有残留错误状态的 PREPARED、pin、staging、proof 或
     accepted epoch。
2. **收敛性、隔离与性能 reviewer**
   - 审查是否真正复用现有 registry、reaper、strict gate、rollback primitive
     和 SQL RESUME，是否存在可删除的重复代码或临时诊断。
   - 审查是否意外增加协议、sysvar、错误码、第二套状态机、全局 registry、
     仅供测试的生产接口或无必要 helper。
   - 审查 `preserve_trx_enable=OFF`、本地 startup 和普通 MySQL 热路径隔离。
   - 审查是否增加业务热路径锁、文件/网络 I/O、等待、长扫描或重型原子操作。
   - 对照同配置基线检查 Phase2、ACK-to-READY/prewarm、SQL RESUME、
     QPS/P99 和资源归零指标，禁止通过放宽门槛取得 PASS。

S3 至 S5 涉及 physical bootstrap、`trx_sys` 和 PREPARED -> ACTIVE 交接时，
再增加一个 InnoDB recovery 专项 reviewer，重点核查 Undo、`rw_trx_list`、
recovery rollback、purge 与 write-enable 的先后关系。

reviewer 结论只是证据输入，不能代替主 session 的源码核实和真实测试。主
session 必须形成逐项处置表：

```text
finding | severity | source evidence | disposition | verification
```

以下任一条件阻止进入下一切片：

- 未解决的 High/Medium 正确性、所有权或数据一致性问题。
- cleanup 责任、失败恢复或异常分支仍不明确。
- 修改超出本文文件范围或代码预算，且没有新的设计批准。
- OFF path、普通业务热路径或既有 startup 行为发生未解释变化。
- 相关性能指标出现未解释回退，或通过提高超时、扩大资源预算、放宽断言掩盖。
- reviewer 发现测试专用生产接口、重复状态/registry 或可以直接删除的废弃逻辑。

根据审查意见修改后，主 session 必须重跑受影响测试；若修改改变了审查过的
不变量、文件范围或性能路径，至少让对应 reviewer 复审新增 delta。S6 完成且
S7 全量验证通过后，再做一次覆盖 source、receiver、promotion/InnoDB 和
性能/侵入面的最终独立审查。

### S0：锁定方案 2 与现状合同

- 用 lint/文档审查锁定：不改 protocol v1、SQL、sysvar，不增加 selection
  对象、第二套 worker 状态机或 accepted lifecycle enum。
- 锁定 identity-first 不增加第二套 registry/线程池/队列，并锁定内部 READY 与
  strict wire `COMMITTED_NOT_READY` 相互独立。
- 登记 TTL、可观测性、receiver UML 和物理升主指南中的 READY 表述为发布前
  后续审计项；本次两文档修订不修改这些文件，它们的旧 all-ready 表述不能
  覆盖本文实施合同。
- GUnit 固化当前 all-ready 行为。
- 固化既有 source timeout exclusion、ABORT failure escalation 和
  all-excluded 行为。
- MTR 固化非 candidate 会执行 Undo body 扫描。
- GUnit 证明 Preserve magic PREPARED 不会被原生 rollback thread 自动处理。
- GUnit 固化当前 worker 空闲后仍常驻。

本切片不修改生产行为。

### S1：Source Phase2 Token-local Exclusion

- 在 per-target 结果汇合点裁决 failure scope。
- 复用现有 source transaction restore helper。
- 复用现有 `abort_token()` 和 survivor COMMIT。
- restore/ABORT 任一步不明确即升级整 epoch fail-closed。
- 结构化 DRAIN 结果准确区分 survivor 与 source exclusion。

不修改 receiver READY、wire 或 physical bootstrap。

### S1A：Transfer 交权后不关闭 Source

- 在现有 ownership/manager 状态机各增加一个 transfer terminal 值，不增加
  第二套状态机。
- 将公共 DRAIN 结果/participant 收尾与 mode-specific shutdown 分开。
- `STANDBY_TRANSFER_SAVE` 在 FINAL_ACK 后返回结果并进入
  `TRANSFER_HANDOFF_COMPLETE`，不调用 `shutdown()`/`kill_mysql()`。
- 普通 source command 保持 default-deny；HA control connection 可诊断和
  显式停机。
- 不恢复、回滚或销毁 transferred source transaction。
- `LOCAL_CARRIER` 和本地 shutdown/startup 行为不变。

不修改 receiver READY、promotion 或 wire。

### S2：Accepted Epoch 内联 Classification

- 从现有 bundle matcher 抽取 identity-only helper，在 sealed-token admission
  边界认证成功后才 enqueue 原有 staged heavy prewarm。
- 在 accepted epoch 内增加 immutable classification 和认证 identity。
- token 状态分类。
- all-ready、partial、zero-survivor 三种结果。
- sealed admission 使用 manifest digest/process generation 快照；identity
  decode 后、写入 transient identity 和 enqueue 前必须重新验证同一快照。
  ABORT、CORRUPT 和允许的 manifest replacement 只能清理匹配 identity。
- 使用现有 binding generation 完成跨 registry publication 并 notify。
- 同一 reaper pass 固定执行 classification、deadline publication、expiry、
  cleanup、idle-stop。
- promotion completion 只设置 `promotion_completed`；accepted epoch 在
  late-worker/cleanup/proof/dedupe 引用全部终止后才由 reaper erase。本项与
  classification 同一切片交付，避免 transient ready state 成为第二份
  selection authority。

只修改 receiver/ready/prepared registry，不接入 physical bootstrap。

### S3：Bootstrap 等待与 Candidate 分流

- `prepare_before_trx_sys_init()` 等待 selection。
- 只注册 READY candidates。
- attempt 保存 RECEIVER_EXCLUDED 的认证 facts，但不为其注册 candidate。
- zero-survivor 仍形成合法 attempt。

不修改 strict adopt 内核。

### S4：传统恢复交接

- 新增按认证 facts 一次扫描 `rw_trx_list` 的 recovered PREPARED -> native
  rollback batch handoff。
- `trx_sys` 后把 anchor fallback token并入 RECEIVER_EXCLUDED。
- 在 `trx_sys_need_rollback()` 一次性评估前完成全部 RECEIVER_EXCLUDED 状态
  交接；随后才按原生条件启动 recovery rollback worker。
- native recovery rollback thread处理 RECEIVER_EXCLUDED。
- 原生 XA 行为不变。

### S5：Survivor-only Strict Adopt

- gate 只使用 final survivor。
- 保持 survivor 集合内的 strict 原子性和 reversal。
- 结果返回 candidate/excluded/fallback/adopted 统计。

### S6：Receiver Cleanup 与 Auto-stop

- cleanup retry 清理完整。
- 运行时 stop 与 mysqld shutdown 分离。
- idle stop 使用可逆 request；完整 mysqld shutdown 另有不可逆
  `process_shutdown_started` latch，禁止 shutdown 后 lazy restart。
- reaper 自动 join worker。
- stop 判据使用 queue/inflight/admission/worker-owned cleanup，不使用
  `m_online_epochs.size()`。
- READY/promotion registry 不受影响。

### S7：真实回归与压力验证

- Debug/Release GUnit。
- Preserve MTR no-bin/log-bin。
- startup 路径全量回归。
- transfer receiver E2E。
- partial-ready physical promotion simulator。
- full-pressure 与 mixed-full，确认没有扩大 prewarm 或 Phase2 时延。

## 16. 测试计划

### 16.1 GUnit

必须覆盖：

1. source 单 token 失败且事务恢复、ABORT 均成功时，survivor 继续 COMMIT。
2. source-aborted token 不进入 terminal fact 或 receiver accepted tokens。
3. source transaction restore 失败时升级整 epoch abort。
4. 单 token ABORT 失败或 ACK 不明确时升级整 epoch abort。
5. 1000 accepted token 中 990 READY、10 RECEIVER_EXCLUDED，selection 集合严格完备。
6. token 数大于 worker 数且全部 heavy worker 被阻塞时，sealed-token
   identity admission 仍认证全部 token；cutoff 只排除 heavy 未完成 token。
7. identity 缺失、重复、损坏或与 fact 不一致时整 epoch拒绝，不能进入
   RECEIVER_EXCLUDED。
8. 内部 selection READY 后，strict commit-status query 仍返回
   `COMMITTED_NOT_READY`。
9. selection 发布后晚到 READY 不得重新加入。
10. zero-survivor selection 合法，且不更新 selected prepare deadline。
11. fact/token 集合不一致整 epoch拒绝。
12. bootstrap 只注册 READY candidate。
13. candidate anchor fallback 变为 RECEIVER_EXCLUDED。
14. RECEIVER_EXCLUDED Preserve PREPARED 转为 recovered ACTIVE。
15. 原生 XA PREPARED 不受影响。
16. native rollback thread可观察并回滚 RECEIVER_EXCLUDED。
17. rollback handoff后、一次性 worker 判定前
    `trx_sys_need_rollback()` 变为 true；handoff失败时不得启动 write-enable。
18. zero-survivor 不调用非空 prepared pin，strict adopt为成功 no-op。
19. promotion 与 staging finalizer 并发时 epoch 不被过早 erase。
20. cleanup retry 成功后无 strict staging 残留。
21. worker 在全部 receiver 工作完成后退出。
22. worker stop 后 READY selection 仍可取得 promotion lease。
23. worker join 后新 epoch 能通过现有 ensure 路径按需重新启动 worker。
24. paused worker 与 profile-limited worker 均能被 idle-stop 唤醒并完成 join。
25. operation deadline 到达只结束当前 bootstrap 等待，不改变全局 cutoff/TTL。
26. selection finalize 与 prepared/accepted expiry 同时到期时，固定顺序不会
    提前删除 READY survivor。
27. selection 发布后晚到 worker 同时处于 `binding` 或 `bound` 时，都不能扩
    大 READY 集合，且其 staging/proof/dedupe 最终清理。
28. promotion 完成后 accepted epoch 只有在 cleanup terminal 时才删除。
29. worker 已停止后 TTL 仍能清除未升主 epoch；完整 mysqld shutdown 与
    idle-stop 竞态只有一个 join owner。
30. strict transfer FINAL_ACK 将 ownership 置为 `TRANSFER_HANDOFF`，且
    `restore_allowed()` 为 false。
31. transfer 成功后 manager 为 `TRANSFER_HANDOFF_COMPLETE`，不会设置
    `SHUTDOWN_REQUESTED` 或触发 deferred shutdown。
32. `TRANSFER_HANDOFF_COMPLETE` 拒绝普通 command，但 HA control connection
    仍可执行诊断和显式 `SHUTDOWN`。
33. `LOCAL_CARRIER` 仍进入既有 `SHUTDOWN_REQUESTED` 路径。

### 16.2 MTR

新增 source/receiver 真实行为用例：

- `batch_drain_phase2_token_local_exclusion`
- `batch_drain_phase2_late_failure_aborts_epoch`
- `standby_transfer_drain_no_shutdown`
- `transfer_receiver_partial_ready_selection`
- `transfer_receiver_worker_auto_stop_restart`

MTR 必须验证：

- source exclusion 后 receiver accepted token 数只等于 source survivor 数；
- source-aborted token 在 receiver record 中为 `ABORTED`，且不进入 fact；
- source restore 或 ABORT 失败时没有 accepted epoch/FINAL_ACK；
- transfer FINAL_ACK 后 DRAIN 返回结构化结果且 source PID/连接监听仍存在；
- source 存活期间普通业务命令返回 4020，HA control connection 可查询并
  显式执行测试收尾 shutdown；
- local carrier 仍按既有路径关闭 mysqld；
- accepted classification 的 READY/RECEIVER_EXCLUDED 集合精确且冻结；
- receiver worker 数小于 token 数时，所有 token identity 在 heavy cutoff 前
  完成，且缺失 identity 仍整 epoch fail closed；
- receiver worker active/idle最终均为 0；
- promotion READY 统计在 worker stop 后仍存在。

物理 bootstrap、Undo body candidate/fallback、batch native handoff 和
zero-survivor no-op 通过 GUnit/promotion simulator 覆盖。当前仓库没有真实
physical promotion SQL/MTR 调用面，不新增伪 MTR 来声称真实升主行为。

### 16.3 仓库内 E2E 与外部验收

使用 standalone 双 mysqld/simulator：

1. source 声明 1000 个未提交事务。
2. 在 Phase2 对 5 个 token 注入可隔离的 per-target 失败。
3. 确认 source 对 5 个 token 分别取得 ABORT 成功，DRAIN 返回
   995 survivor 和 5 SOURCE_EXCLUDED。
4. 确认 source mysqld 仍存活、DRAIN 控制连接已收到结果，普通业务 SQL
   继续被 4020 拒绝。
5. 确认 receiver accepted fact 恰好包含 995 token，不包含 source exclusion。
6. receiver 再故意让其中 5 个 accepted token prewarm 超时。
7. 确认 selection 为 990 READY 和 5 RECEIVER_EXCLUDED。
8. 确认 source exclusion 从未重新进入 selection。
9. 确认 receiver worker自动退出。
10. 通过现有 simulator 调用 physical bootstrap/handoff/adopt。
11. 只为 READY 注册 candidate，并确认 excluded 走 batch native handoff。
12. adopt READY survivor。
13. 对 survivor 执行现有 SQL RESUME 并继续 DML。
14. 确认 excluded 均不能 SQL RESUME。
15. 测试需要关闭 source 时，由 HA control connection 在上述证据完成后显式
    执行 `SHUTDOWN`；不能把 DRAIN 断连当作成功条件。

还必须独立执行失败升级用例：让某个 token 的 receiver `ABORT` 返回失败或
ACK 不明确，确认 source 不发送成功 `COMMIT_EPOCH`/FINAL_ACK，不能为了保留
其他 token 而提交一个未能证明排除完整的 epoch。

真实物理复制尚未接入时，测试只能证明 shared kernel、分类和生命周期，不宣称
真实物理备机 promotion E2E。生产启用前，物理备机项目必须补充外部 E2E，
证明 redo freeze、唯一一次 `trx_sys_init_at_db_start()`、handoff/adopt、
recovery rollback/purge 和 write-enable 的真实调用顺序。

### 16.4 回归

- Preserve GUnit Debug 全量。
- Preserve GUnit Release 对称验证。
- Preserve MTR Debug no-bin/log-bin 全量。
- Preserve MTR Release no-bin/log-bin 全量。
- Python harness 全量单测。
- startup、transfer、promotion simulator E2E。
- full-pressure 与 mixed-full Release 回归。
- `preserve_trx_enable=OFF` native-path 回归。

## 17. 验收标准

功能：

- source token-local 失败在恢复和 ABORT 明确成功后，不再阻止其他 survivor COMMIT。
- source-aborted token 不得出现在 terminal fact、accepted tokens 或 receiver selection。
- source restore/ABORT 不明确以及 epoch-global 失败仍整批 fail-closed。
- `STANDBY_TRANSFER_SAVE` 的 DRAIN 在 FINAL_ACK 后返回结果但不关闭 source
  mysqld。
- source 存活期间 transferred 事务保持不可恢复、不可回滚、不可重新
  attach，普通业务 command 保持 4020/default-deny。
- 后续 source shutdown 只能由 HA 独立触发；`LOCAL_CARRIER` 的自动 shutdown
  行为不变。
- 一个 token 未 READY 不再阻止其余 READY token进入快速恢复。
- 所有 fact token 在分类前都有认证 identity；纯队列积压不会因 job 尚未启动
  而误伤整个 epoch。
- 升主编排始终为同一条流程，不存在全局“普通模式切换”。
- READY candidate跳过 Undo body。
- RECEIVER_EXCLUDED token执行 Undo body扫描并进入原生 recovered rollback。
- 原生 XA PREPARED 不被误回滚。
- survivor 使用现有 SQL RESUME，无第二套 promotion resume。
- zero-survivor 不进入非空 registry pin，strict adopt为成功 no-op。
- RECEIVER_EXCLUDED rollback handoff失败时不得伪装完成或继续 write-enable。

生命周期：

- READY 后 promotion 不等待 receiver临时清理。
- receiver临时清理完成后 worker全部退出。
- worker退出后 READY selection和promotion对象仍有效。
- receiver restart后 epoch失效的既有合同不变。
- promotion立即发生也不遗留 orphan staging或cleanup debt。

隔离：

- 本地 startup durable恢复行为不变。
- transfer no-shutdown 分支不进入 `SHUTDOWN_REQUESTED`、`shutdown()`、
  `kill_mysql()` 或 deferred shutdown signal。
- transfer protocol v1 和 artifact codec 不变。
- `preserve_trx_enable=OFF` 不进入新逻辑。
- 普通 binlog、InnoDB事务和锁热路径不增加未隔离逻辑。

性能：

- sealed-token identity admission 只读取现有 strict 内存 staging 中有上限的
  Resurrection Index，不读取文件、不获取 DD/lock lease、不执行网络 I/O。
- 现有 per-object prewarm 调度和并行度不变；identity admission 不得造成
  无法解释的 SEAL frame ACK、FINAL_ACK 或 ACK-to-READY 回退。
- selection 构建为 O(token count)，不读取 payload。
- 等待使用condition variable，不轮询。
- RECEIVER_EXCLUDED大事务回滚不在promotion gate同步完成。
- receiver auto-stop不新增线程。
- selection 构建耗时必须记录并用于回归比较，但首版不设置脱离 runner
  量测能力的固定 10ms 硬门槛。
- full-pressure现有Phase2和ACK-to-READY门槛不得放宽。

## 18. 风险与控制

### 18.1 PREPARED -> ACTIVE 转换失败

这是 RECEIVER_EXCLUDED 能否真正回到原生 rollback 的关键。

控制：

- 只接受精确 Preserve magic XID。
- prepare LSN、Undo anchors 和 object digest 已在 receiver identity/fact
  publication 前完成认证；batch handoff 不从 recovered `trx_t` 发明第二套
  actual-LSN/anchor 比较。
- 在 `trx_sys` mutex 下先对整批 XID、trx_id、状态、owner 和唯一性做全有或
  全无预校验。anchor mismatch 是 READY candidate 回退到 native Undo scan
  的合法原因，不得在 handoff 中再次拒绝。
- 任何预校验失败都发生在状态修改前，整批保持 PREPARED 并阻止
  write-enable。
- 当前 Undo activation helper 在合法前置状态下是非失败路径；首版不为
  假设中的可恢复失败增加 journal。实现中若需要在首个事务状态修改后加入
  任何可恢复失败操作，必须停止 Task 4 并重新设计批量原子性；不能依赖
  Debug/Release assert，也不能在部分转换后继续执行。

### 18.2 Selection 与 Prepared Registry 过期竞态

控制：

- 同一 reaper pass 固定为 selection finalize、selected deadline
  publication、accepted READY、prepared expiry、accepted TTL expiry、
  cleanup retry、worker idle-stop。
- selection 持有 accepted lease 内的 immutable publication，不引用会被
  reaper 删除的可变 record。

### 18.3 Receiver Stop 与新 Job 竞态

控制：

- 复用现有 `starting/stopping/shutdown` 布尔量，idle-stop 只增加 request
  标记。
- stopping 期间 enqueue 等待并在 join 后重检完整 shutdown、epoch
  retirement 和 dedupe。
- join 完成后才允许下一次 lazy start，不并存两代 worker pool。

### 18.4 Partial Ready 扩大 Strict Gate 复杂度

控制：

- 只在strict adopt前缩小survivor集合。
- strict adopt内部仍保持现有全量survivor原子性。
- 不在本轮实现adopt中途部分成功。

### 18.5 Source Failure Scope 误分类

风险：

- 把 participant、fence、identity 或 transfer-session 失败误判成 token-local，
  会让 survivor fact 建立在不可信的全局状态上。
- 失败事务虽然返回了 per-target error，但 source reattach/reactivate 或 receiver
  ABORT 没有完成，仍可能留下所有权不明确的 token。

控制：

- 首版使用封闭 allowlist，只接纳已经有确定性恢复和 ABORT 成功证据的
  token-local reason。
- 默认分支继续视为 epoch-global，不根据错误字符串自动降级。
- 只有 source transaction restore 和 receiver ABORT 都明确成功，才能把 token
  从 fact 输入集合移除并报告 `SUCCESS_WITH_EXCLUSIONS`。
- 任何 ACK ambiguity、cleanup debt 或 ownership uncertainty 都升级整 epoch
  fail-closed。

### 18.6 Identity 被重型 Prewarm 队列饿死

风险：

- 只在 staged heavy job 内提前记录 identity，仍无法覆盖尚未被 worker 取得的
  队列深处 token。
- 通过放大 PREWARM deadline 掩盖该问题，会使部分 READY 行为依赖部署时序。

控制：

- identity-only helper 在 `all_objects_sealed` admission 边界同步执行，不进入
  worker queue。
- staged-token heavy job 只有 identity 成功后才入队，不能反向阻塞 identity
  pass；现有 per-object prewarm 保持可提前执行。
- 使用 token 数大于 worker 数、heavy work 确定性暂停的 GUnit/MTR 证明队列
  积压时 identity 仍能完成。
- identity 缺失或损坏保持 epoch-global fail closed，不允许为了提高部分成功率
  放宽认证。

### 18.7 Source 存活导致双重所有权或资源误释放

风险：

- FINAL_ACK 后若 manager 回到 `IDLE` 或普通命令重新放行，旧 source 可能与
  receiver 新主同时写入。
- 若 source 为释放内存/锁而主动 rollback、reactivate 或销毁 transferred
  transaction，会在 source fence 之后产生新的事务语义并破坏交权。
- 若 HA 长期不关闭或重建 source，保留的 Undo、锁和 detached trx 会持续
  占用资源。

控制：

- FINAL_ACK 后固定进入 `TRANSFER_HANDOFF_COMPLETE`，复用 CLOSING
  default-deny；不得自动回 `IDLE`。
- 只清理 transport/warmcopy 临时资源，不清理 transferred trx、Undo 或锁。
- HA 必须移除业务流量，并负责后续显式停机或节点重建；本轮不增加 source
  TTL、后台 rollback 或在线重新加入能力。
- MTR/E2E 必须分别证明“source 进程仍存活”和“普通业务仍被拒绝”，不能只
  验证其中一个。

## 19. 最终裁定

本方案正式采用“方案 2”：accepted epoch 内联 classification、READY-only
candidate、excluded batch native handoff 和 join-only worker stop。它不增加
第二套物理升主模式，也不增加第二套事务恢复系统。

它只完成六个收敛动作：

1. source在COMMIT前只排除能够证明隔离、恢复和ABORT成功的失败token。
2. transfer FINAL_ACK后source保持fenced并返回DRAIN结果，不由内核关闭
   mysqld，也不恢复或回滚transferred事务。
3. receiver把accepted token分类为READY candidate和RECEIVER_EXCLUDED。
4. receiver在sealed-token admission边界完成不会被重型队列饿死的
   identity-only pass，再按重型prewarm结果分类。
5. 复用现有Resurrection Index/Undo扫描/原生recovery rollback，对每个事务选择正确路径。
6. receiver交付selection并清理临时资源后自动停止worker，promotion稍后独立消费READY对象。

这满足“source 少数 token 失败或 receiver 少数 token 未 READY 都不能拖累
大多数成功 token”，同时保持 startup 恢复、SQL RESUME、原生 rollback 和
物理升主编排为同一套内核逻辑。任何需要独立 selection registry、第二套
receiver 状态机、通用 token-local 失败策略或新 wire/SQL 的实现，都不属于
本方案。
