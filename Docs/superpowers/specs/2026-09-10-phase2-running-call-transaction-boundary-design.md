# 已执行 CALL 的事务换代：Phase2 最小支持设计

状态（2026-09-15 整理）：核心实现及下文所列定向验证完成于 2026-09-11，相关修改仍未提交。退出/T0 与跨事务 CALL 的旧代码 4013 已确定性复现；修改后 Phase2 no-bin、log-bin 各 31 个业务用例全部通过（各另有 shutdown_report 通过）。完整验证范围及追加的 ReadView 请求见第 9 节，不将定向通过等同于完整 HA 或性能验收。

核查基线：`9bf1d52f9ee1d5b3de7331de6aa2eeef25a76257`，加当前工作树已有的顶层多语句修复（`preserve_trx.cc`、`sql_parse.cc` 合计 +8/-1）。第 7 节保留实施前预算，第 9 节记录实际切片；两者均不包含这项既有修改，也不代表当前工作区全部内核差异。

文档关系：本文补充 [调度主设计](2026-08-26-preserve-trx-phase2-command-scheduler-design.md)。[purge 停止](2026-09-14-pre-t0-purge-pause-and-rollback-review.md) 和 [record 重试](../plans/2026-09-15-phase1-record-consistency-retry.md) 是后续独立捕获措施，不改变 CALL 的命令级调度规则。文中 `/private/tmp` 路径仅标识本机历史验证记录，不要求随设计提交。

## 1. 结论：管住整条 CALL，不追踪过程里的每个事务

支持目标不变：T0 时已进入执行体的外层 `CALL p()`，按原生逻辑继续执行；过程可以提交 T1、开启 T2，甚至继续换代。调度器不截断过程、不替用户提交，等整个 CALL 返回后，再接住它最终留下的事务。

更简单的实现是：

- **CALL 运行中：只登记命令，不为它内部的 T1、T2、T3 建立调度事务记录。** 它已经在执行，不需要再领放行票。
- **CALL 等锁时：照常扫描真实等待，给阻塞它的、需要放行的事务登记 support。** 不登记 CALL 的中间事务，不等于不检查它的锁等待。
- **CALL 返回时：用该连接线程的最终观察，一次性登记实际留下的事务。** 后续命令恢复原来的事务级准入规则。

实现再收敛一步：复用已有“命令尚未登记事务”的 pending 标记、连接上的 pending command key、默认 probe 输入和退出首次登记分支。不再新增 CALL 专用的连接索引、probe 初始化或事务退出函数。

这里的“登记”仅指 scheduler 自己的账本。原生事务、锁、binlog cache、Phase1 捕获和传输仍照常运行，不受上述简化影响。

范围仍限于 `DEPENDENCY_CONVERGENCE_V1 + STANDBY_TRANSFER_SAVE` 中已经执行的 CALL；不把尚未进入 BODY 的 CALL 加入白名单，不扩展 XA、临时表既有不支持状态、显式非事务级锁、RESET DRAIN 或物理 promotion。文本 CALL 与二进制 prepared CALL 在范围内；仅因 SQL `EXECUTE` 内部最终调用了过程，不自动把外层 EXECUTE 扩为本方案的 CALL。

## 2. 对原设计的审核

原设计的目标和两条底线是合理的：同一条 CALL 可以合法换事务；不同事务不能继承旧事务的放行资格。但它把“识别当前等待”进一步做成了“持续跟踪 CALL 内部每代事务”，超过了命令调度所需的信息量。

| 方案 | 审核结论 |
| --- | --- |
| 只删除退出时的事务不一致检查 | 不安全，也不完整。CALL 的 T2 等锁时仍可能使用 T1 的 version；另一个 waiter 等 T2 时，原 blocker 映射也可能失败。 |
| 原方案：动态换绑每代事务、分配 ordinal、维护未知隔离级别和 binding revision | 可以继续补全，但运行中的 CALL 根本不消费这些事务级许可。状态、过期规则和验证负担偏多，不再作为推荐方案。 |
| 本方案：运行中按命令处理，返回时首次登记最终事务 | 推荐。利用现有 support 的两端本来就不同这一事实，删除中间事务换代机制，保留精确锁证明和退出竞争保护。 |

实施前基线的源码依据如下（解释原缺口，不表示当前代码仍失败）：

- `Support_edge_key` 本来就是 **waiter 的 Command_key → blocker 的 Transaction_key**，不是“两端都必须有持久事务记录”（`scheduler.cc:77`）。
- `gate_command()` 对已处于 EXECUTING 的命令直接继续，不发新 permit（`:1751`）。
- 原失败由 `apply_command_exit_locked()` 检查退出事务与已封存 T1 不同而触发（`:1284`）。旧版 `routine_cross_tx.test` 用真实 waiter 将 T1 封存，随后让 CALL 留下 T2，复现 4013/`lineage_unknown=1`；当前用例已改为成功保存最终事务的断言。
- `find_native_transaction_locked()`、T0 collector、waiter scan 和 command exit 是现有四处事务登记入口（`:929 / :1583 / :2609 / :1248`）。前三处对合格 CALL 不登记，最后一处用于最终事务即可。`register_captured_command()` 不创建事务记录。

本节 `scheduler.cc` 均指 `sql/preserve_trx_standby_phase2_scheduler.cc`；行号对应上述基线，以函数名定位为准。

## 3. 什么样的 CALL 获得这项资格

资格属于某个固定的 command key，不属于整个连接，更不属于过程中每次出现的 `SQLCOM_CALL`。

CALL 识别沿用现有 capture / BODY CAS / T0 快照协议，只补一个外层 CALL 标记；退出与 T0 发布之间的遗漏窗口另按第 5.1 节补齐，不能假定现有 pending 通道已经覆盖所有退出：

1. capture 新命令时清零标记。
2. 第一次外层 SQL gate 取得真实 LEX 后，在发布 EXECUTING 之前，于既有 `LOCK_thd_data` 保护下核对 command key 并记录它是不是 CALL。标记写入并入 gate 现有的 command-key 校验临界区，不为它另加一次 THD 加锁。二进制 prepared CALL 使用 dispatch 已传入的 `stmt->lex`；不在后续扫描中读取会变化的 `thd->lex`。
3. T0 在同一次 THD 快照里读到 **EXECUTING 且外层 CALL 标记为真**，才把这个 key 登记为运行中的 CALL。内部 SQL、嵌套 CALL 不重写标记、不生成第二个资格。
4. T0 先拦住、以后才获得 permit 的命令，没有这个资格。不能用后来变为真的 `t0_member && entered_body` 反推。
5. CALL 若在 T0 注册完成前退出，退出交接必须把完整事实送入对应 attempt，`pending_exit_facts` 携带外层标记。注册未完成时新 permit 尚不能发出，因此该特殊路径的 `entered_body && outer_is_call` 可以认定为原已执行 CALL；注册完成后只能按登记的 key 判断。

T0 对合格 CALL 不调用事务 reserve，但应复用现有 pending 通道：

| 字段 | T0 登记值及含义 |
| --- | --- |
| `Connection_record.has_old_transaction` | `false`：scheduler 尚未给该 CALL 绑定事务。 |
| `Command_record.pending_t0_body_first_transaction` | `true`：等待首次调度登记；不是声称原生事务尚未开始。 |
| `Connection_record.pending_t0_body_first_command` | 当前 CALL key：直接复用现有索引，不新增第二份 key。 |
| `Command_record.t0_running_call`（新增） | 不可变的 T0 已执行 CALL 资格；普通 pending 命令不具备它。 |

前三项都是已有字段。`outer_is_call` 是命令类型事实，`t0_running_call` 是 T0 认定的资格，二者不能混用；request/exit 只传事实，不独立发放资格。CALL 的 `Command_class` 仍为 `DEFAULT_DENY`，不新增白名单枚举。

保留连接 pin 和既有 BODY 计时。正在执行的命令仍阻止正常 QUIESCENT；这项简化不会提前进入 HARD。

## 4. CALL 运行中：只处理需要调度的锁依赖

### 4.1 CALL 自己是 waiter

直接复用现有 probe 输入路径：`Probe_work` 的 cookie/version 默认是 0；CALL 没有 old transaction，便不会被填入 T1 身份；随后现有 `LOCK_thd_data` 临界区核对命令并 peek 当前 raw cookie（`scheduler.cc:2381–2428`）。无需额外的 CALL probe 标记、初始化分支或第二次 THD 快照。

这里的 0 表示“不预先指定旧版本”，不是“跳过身份验证”。仍须核对 command key、EXECUTING、pin、raw cookie、owner，以及原生完整等待快照；UNKNOWN、队列不完整或既有不支持的锁类型不能变成成功。

取得完整快照后：

- 对合格 CALL，跳过 waiter 的 reserve/seal 及 old transaction key 写入，保留 pending；快照中的事务身份只用于本次证明。
- 对其 blocker 依次完成下面的分类。
- 有效 support 继续记在 CALL 的 command key 上，复用原有更新、到期和退休规则。

CALL 中一次内部等待结束后，后续采样会更新或清空 support；两次采样之间仍受原有有限租期约束。它不是给 T2 继承 T1 的 blocker 许可，不增加新的长期授权。

有一处不能机械复用：CALL 分流后 `waiter_transaction` 可以为空，原 `scheduler.cc:2653–2655` 的自等待检查不能再解引用它。应先按本次 native snapshot 的精确 owner/连接身份拒绝 self，再处理普通或 running-CALL blocker；不为绕过空指针而创建虚假的 waiter 事务记录。

### 4.2 其它命令等着 CALL 持有的锁

完整 native 快照中的每个 blocker 都必须被识别，不能因为发现一个正在运行的 CALL 就停止检查其它 blocker。

- **精确映射到仍在执行的合格 CALL：** 标记为“已经运行，无需发票”。在既有 owner 映射中，通过 pending command key 找到带 `t0_running_call` 资格的当前 EXECUTING 命令；必须在 lazy reserve 和清 pending 之前分流，不为其 T1/T2 创建记录或 support。不能把未知 owner、退场连接、自等待或普通 pending 命令当成这一类。
- **普通 blocker：** 继续原有事务身份、隔离级别、锁释放类型与 support 检查；规则不放宽。
- **owner 已退场或采样过期：** 沿用 stale/retry；真正无法证明的身份仍走原有安全失败。

例如 W 同时等着运行中的 CALL A 和被冻结的事务 B：A 已经在跑，不发票；B 仍应拿到自己的 support。若所有 blocker 都已经运行，结果是“证明完整，但没有需要发票的事务”，不是 UNKNOWN。必须允许 `mapped_count=0`，并清掉 W 上一轮遗留的 InnoDB support；不能保留 `mapped_count == 0 → SAFETY_ABORT`。

“已经运行”的分类不要求提前知道 CALL 中间事务的隔离级别，因为根本不给它新增 permit。不能据此省掉对真正获准继续执行的普通 blocker 的隔离级别检查。

### 4.3 MDL 沿用现有机制

MDL 的 waiter demand 已按 command key 保存；被冻结的 owner 在自己的准入点核对事务级 MDL。CALL 等 MDL 时继续发布 demand；CALL 作为持锁者且仍在执行时，不需要额外的 owner-gate 放行。

不从 MDL key/type 推导事务身份，不建立 CALL 专用 MDL 状态机。事务级/显式锁、优先级等待者和 UNKNOWN 等既有边界不变。

## 5. CALL 返回：一次登记最终事务，再退休命令

### 5.1 先保证退出事实不会漏掉

基线 `finish_command()` 有一个与 CALL 换代无关、但本方案必须补齐的窗口：它在 `scheduler.cc:2073` 读到空 callback 后，owner 可以发布 attempt，collector 将尚为 EXECUTING 的命令登记进 T0；原退出路径随后在 `:2119–2120` 直接发布 IDLE，没有提交退出事实。`tick():2275–2279` 可因退出覆盖不完整而 SAFETY_ABORT。新方案还会因此漏掉 CALL 最终事务登记和退休计数。本轮已用锁外空 callback 屏障复现旧代码 4013/SAFETY_ABORT，并验证修复后完整退出覆盖及成功 handoff。

交接必须保证二选一：

- **退出先完成：** T0 看到的是 IDLE，不再把该 CALL 计为执行中的命令；若连接留下活动事务，collector 按既有规则登记它。
- **T0 已收录该命令：** 对应 attempt 必须收到同一份最终 observation 和 BODY exit；在注册完成后直接处理，或先进入 pending facts 再重放。CALL 最终事务登记、资格退休与退出覆盖不能丢失或重复。

最窄实现放在 `finish_command()` 现有 command-key 校验与 callback 选择的接缝：锁外取得的空 callback 只能作为 hint；在复用的 `LOCK_thd_data` 校验临界区内重验 key 和 `g_callback_active`。若仍无 callback，锁内发布 IDLE；若 callback 已出现，先释放 THD 锁，再获取 attempt 强引用并进入既有退出处理。重取仍为空时重新做受锁保护的选择，不能落回原来的裸 IDLE 分支。collector 若先取得快照，后来的退出必须看到已发布 callback；退出若先完成，collector 只能看到 IDLE。保持原有未进入 BODY 的错误、CUTOFF 和响应处理。

这不是仅给末尾 IDLE store 加锁，也不是在空 snapshot 后无锁重读一次。正常 BODY 退出移动/展开已有 key 校验临界区；无 attempt 快速路径不获取全局 route mutex，仍只进行一次 THD 加锁。未进入 BODY 的异常退出保留原 key 校验、CAS 和锁外 debug 屏障，因此在退休前再做一次受锁交接检查；不影响正常执行热路径。获取 route 强引用、等待注册、事务 seal 和 debug barrier 均不放在 THD 锁内；不新增 command stage、退出队列或第二份事务账本。该交接覆盖本功能捕获的普通命令和 CALL，不做 CALL 专用竞态补丁；OFF/LEGACY 仍不进入此捕获路径。

### 5.2 接住最终事务，复用原来的退出分支

外层 CALL 的本连接退出 observation 是最终依据，不是内部 COMMIT 的回调，也不是另一线程读取的临时 LEX。

第 5.1 节已确定退出归属且 T0 注册完成后，在既有 `finish_command()` 的同一 attempt mutex 临界区中完成；注册尚未完成则保存完整事实，按下面的重放规则处理：

1. 若连接仍有活动 SQL 事务，复用现有 reserve + seal 登记最终事务。若引擎尚未启动事务，保留既有 pending identity；不能把 NONE 冒充 EXACT，也不能因此强制要求引擎事务已经存在。
2. 若没有活动事务，走原有无活动事务处理。CALL 返回原生错误不等于事务结束，仍按 observation 决定。
3. 清掉该 CALL 的 waiter support 和 MDL demand，撤销运行资格，退休 command，发布 IDLE。
4. 增加下面的 CALL 退休计数，使正在借出的旧 probe 失效。

步骤 1、2 不另写 CALL 分支：`apply_command_exit_locked():1234–1259` 已会清 pending command key，并在 command pending、已进入 BODY、尚未绑定且最终 SQL 事务活动时 reserve + seal；无活动事务直接返回。步骤 3 复用既有 support/demand/command 退休代码，只补 CALL 资格对应的计数处理。

运行中没有建立 T1 的调度事务记录，所以通常只需复用原来首次登记的 ordinal=1，不再增加 ordinal 分配器或历史事务回收机制。退出事实可靠交接之后，T0 注册期间的重放复用原逻辑：无已登记事务则重建 pending；collector 若已经看到最终事务并建了未封存记录，就封存这份最终记录。重放只接纳退出事实，不重复执行退休计数；注册完成和正常 HARD 不得越过尚未处理的退出事实。

隔离级别使用最终当前事务的 `thd->tx_isolation`。实施前 observation 使用 session 默认 `variables.transaction_isolation`，当前已修正为前者，二者不能混用。复用现有 observation 字段即可，不引入“运行中隔离级别未知”的新状态。

CALL 返回后的最终事务如果仍阻塞其它执行中的命令，可由**新采样产生的 support**放行后续白名单命令，或按现有规则执行有效 no-CHAIN 的 COMMIT/ROLLBACK。没有 support 的普通命令继续 HELD；新 BEGIN、同包下一个顶层 SQL、下一 packet 均重新准入。HARD 后也不额外放行 COMMIT/ROLLBACK。

### 5.3 只保留一个必要的过期保护

假设 W 的 probe 采到了 CALL A 持有的锁，尚未合并时 A 返回。A 的最终事务此时已改为普通 blocker；不能把返回前的快照误套到返回后的身份上。

采用一个 attempt 内的 `call_retire_revision`：

- 借出任何 probe 时记录它；在合并 MDL 和 InnoDB 结果**之前**，于同一 attempt mutex 下重新核对。不一致则整份丢弃，下一轮重采，不部分发布。
- 仅合格 CALL 的外层退出或 teardown 撤销资格时增加；与最终登记/清理/退休原子完成。须在清 pending 索引或删除 command 前，从不可变资格取得判定，不能事后再查已删除记录。teardown 清理现有索引，不增加新退场状态；T0 未完成时先按第 5.1 节可靠交接，再通过 pending-fact 协议处理。
- CALL 内部每次 COMMIT/BEGIN 不增加它；普通事务封存、support 刷新也不增加它。最后一个 CALL 退出后不清零。
- 没有这类 CALL 的 attempt 中始终为 0，不引入额外线程、锁、全表扫描或由它触发的重扫。

该计数只保护“运行中 CALL → 已返回/退场”这个转换，不重新建立一套事务生命周期版本系统。现有 raw/version、连接 incarnation、command sequence 和锁快照租期各自继续负责原来的检查。

不继续删除这项保护：现有 merge 只核对 waiter W 是否仍在执行，不能发现 blocker CALL A 已返回。若 A 留下 T2，旧 T1 快照会触发版本不符；若最终事务尚未封存，还可能被旧快照错误地首次封存。也不改用普通 `attempt.revision`：它会随 support、HELD 等无关变化递增，导致范围更大的重采。保留一个仅随 CALL 退休变化的计数更直接。

## 6. 下游不改：保留现有捕获、替换和最终确认

| 对象 | 已核实的现有承接点 |
| --- | --- |
| record-lock、table lock、MDL 候选 | `phase1_owner.cc::same_binding()` 比较 owner/raw/version/epoch；final prepare 按当前 binding 重捕。`lock_warmcopy.cc::reconcile_bounded_final_record_candidates()` 清旧候选、保留连接壳。 |
| binlog cache | COMMIT/reset 提升 truncate generation，使旧镜像失效；final inline/rebuild/replacement 路径构造当前对象。仅同 generation 才可追加 prefix，T2 不拼到 T1 前缀上。 |
| 最终元数据 | ReadView、MDL、modified tables、savepoints 从最终 quiesced THD 导出。临时表成功 COMMIT/完整 ROLLBACK 清旧 participant，后续 DML 重建；既有不支持状态不放宽。 |

对应源码入口为 `sql/preserve_trx_phase1_owner.cc:415`、`sql/preserve_trx_lock_warmcopy.cc:2756`、`sql/preserve_trx.cc:9116 / 9431 / 16984`。Phase1 对象的 ACK 不等于最终 epoch 已提交；对象替换仍只能在 receiver 允许的 DECLARED/RECEIVING 阶段进行。

这些机制支持复用原流程的设计方向，不代表已经证明跨事务 CALL 的最终接纳正确。尤其 T1 已预传、T2 更短/等长/更长的直接成功测试还没有完成。若发现缺口，应单独报告，不把修复偷偷塞进 scheduler。

最终对象是否可保存、无 token 时如何处理，继续由既有 preserve 规则决定；“不再因 CALL 合法换代报 4013”不等于所有原本不支持的最终事务也自动受支持。事务提交的已发生效果不能因 DRAIN 取消而回滚。

## 7. 实施前预算（历史记录）

### 7.1 内核代码量

仍只涉及四个内核文件，主要逻辑在原独立 scheduler 中；不新增源码文件、通用适配层或配置项。下表包含退出/T0 交接补漏及必要的 debug 屏障，不包含既有多语句 +8/-1、MTR/Python 或文档。

| 文件 | 实际修改入口及职责 | 预计新增行 |
| --- | --- | ---: |
| `sql/sql_class.h` | captured-command 外层 CALL 标记 | 2–4 |
| `sql/preserve_trx_standby_phase2_scheduler.h` | request/exit 传递少量 CALL 元数据，复用 observation | 4–8 |
| `sql/preserve_trx.cc` | 薄分类/事实传递、修正当前事务隔离级别来源 | 20–30 |
| `sql/preserve_trx_standby_phase2_scheduler.cc` | capture/gate 类型固定、T0 pending、native owner 映射与 waiter merge 分流、self 检查、finish/teardown 退休计数；另在 finish 的 key 校验接缝补退出交接 | 130–215 |

**内核整体预计新增约 160–260 行，删除/改写原有 25–60 行。** 各文件区间端点合计 156–257 行，整体按约数报告；不是新增 260 行之外还要再加一份握手实现。

| 预算组成 | 新增行 | 删除/改写原有行 |
| --- | ---: | ---: |
| 原 CALL 收敛方案：三个登记入口分流，退出复用首次绑定，退休计数 | 约 130–200 | 15–35 |
| 本次 review 增量：退出/T0 交接及必要 debug 屏障 | 30–60 | 10–25 |
| 合计 | 约 160–260 | 25–60 |

这里按差异行估算：替换后的新行计入新增，被替换的旧行计入删除/改写；二者相加是审查改动量，不是净增长。以上为实施前工程预算；实际可编译切片及验证见第 9 节，不把预算当成当前待实施量或硬性裁剪目标。实际超过预算先检查是否重复实现了 pending、退出处理或 owner 扫描；必要的正确性保护不能为了压行数删除。

实现只在既有 owner 映射的 pending 分支识别合格 CALL，避免对普通 blocker 再遍历全部连接。除外层类型事实、T0 资格和退休计数外，不新增生命周期状态；已有 pending 索引与首次绑定逻辑继续承担它们原有的工作。

不改 `sql_parse.cc`、`sp_instr.cc`、`sql_call.cc`、InnoDB/MDL 锁热路径；不改 preserve/transfer/receiver 的进度、分批、ACK、epoch、READY、持久化及失败恢复语义。OFF/LEGACY 不进入新增分支。

### 7.2 本次 review 直接增加的测试工作量

MTR 与内核分开计算，复用现有 `phase2_multistatement_setup.inc` / `phase2_multistatement_cleanup.inc`，不新增测试框架或 Unit/GUnit：

| 工作 | 预计改动 |
| --- | --- |
| 两个既有 routine 用例：屏障迁移、cross_tx 成功语义与确定性断言 | 两份 `.test` 新增/替换约 30–70 行、删除/改写原有 20–45 行；对应 `.result` 新增/替换约 5–20 行 |
| 新增空 callback 与 T0 交错用例 | 一个主要事件序列的 `.test` 约 80–140 行；复用双实例配置的 `.cnf` 约 10 行，`.result` 约 5–15 行；补注册前后及退出先完成的顺序，另预留约 40–80 行测试文本 |

这仅是 review 新识别的测试增量，不是第 8 节完整 CALL 成功矩阵或压力回归的总代码量。预算时两个 `.test` 为 68/75 行；不能将“改几个屏障字符串”误报为整个成功验证的开发量。实现顺序为：先固定退出窗口和跨事务 CALL 的有意义 RED，再完成握手与 CALL 分流，最后迁移既有屏障并跑第 8 节验证；不靠修改预期隐藏失败。

## 8. 最小验证闭环

仅 MTR 与必要 Python E2E，不新增 Unit/GUnit，不扩展 RESET 竞态矩阵。新成功用例统一钉住 dependency + standby transfer；no-bin/log-bin 分别覆盖适用场景。

| 验证组 | 必须证明 |
| --- | --- |
| CALL 返回同一事务、新事务、无事务、活动 SQL 事务但引擎 NONE，以及原生错误 | 最终身份正确，原生结果不被覆盖；不因合法换代 4013，不把其它既有拒绝误报为支持。 |
| CALL 的 T2 等普通 blocker；W 同时等 CALL 与冻结 blocker；全部 blocker 为运行中 CALL | 等待能继续推进；不给运行中的 CALL 发票；其余可支持 blocker 不被遗漏；空 support 不是失败。 |
| CALL 内多次换代、嵌套 CALL、SET/条件表达式访问表、事务级 MDL 等待 | 不依赖内部 SQL gate；中间事务次数不增加 scheduler 事务记录；MDL 仍按原规则解锁。 |
| CALL 外层退出/teardown 发生在 probe 采样与合并之间；T0 注册中退出 | 旧快照整份丢弃；最终登记先于正常 HARD；pin、退出事实及资格不丢失。 |
| 退出曾读到空 callback 后遇到 T0 发布；反向次序由退出先完成 | 精确覆盖第 5.1 节窗口。需取得退出事实缺失/SAFETY_ABORT 的旧代码 RED，不能用超时代替；修复后要么 T0 不收录该 BODY，要么完整接收 exit，最终事务与退出覆盖均正确。 |
| 文本 CALL、二进制 prepared CALL、SQL EXECUTE 包装 CALL、尚未执行的 CALL、CALL 后同包/下包 SQL、当前隔离级别与 session 默认不同 | 外层识别准确，包装 EXECUTE 不获 CALL 资格；最终事务的放行依据真实隔离级别。共享 setup 关闭 PS 协议，prepared 用例须显式启用并确认实际走 COM_STMT_EXECUTE。 |
| T1 已预传后提交，T2 cache 更短/等长/更长/超过 inline 上限、最终不再需要原 record/binlog 对象，及既有可支持临时表 DML | 按现有规则替换或不再纳入最终对象集合；最终数据归属、binlog 和接纳一致，不复用 T1 对象。 |
| 同一跨事务 CALL 在 OFF、LEGACY 和非 standby 配置下的原生对照 | 不进入新增分支，原生事务结果、错误及结果集不变；不能只用不换事务的两个 SELECT 作为对照。 |

以下两个既有用例已完成同步屏障迁移（路径后行号为历史定位）：`mysql-test/suite/preserve_trx/t/standby_transfer_phase2_multistatement_routine.test:36–45` 和同目录的 `standby_transfer_phase2_multistatement_routine_cross_tx.test:39–48`。原夹具先等待“给运行中的 CALL 登记 support”，再释放 CALL；新方案不再产生这张票。当前两例已改用 `phase2_sched_after_probe_merged` 锁外屏障，保留同事务的结果集、COMMIT 结果和后续 SQL cutoff 断言；跨事务旧代码 4013 RED 单独保留，不靠改预期隐藏失败。

cross_tx 的成功用例优先验证 CALL 留下的 T2 被保存：内部 COMMIT 已释放旧 waiter，不能假定 CALL 后同包 COMMIT 一定抢在 HARD 前执行。若另测该 COMMIT 的放行，必须有真实执行中的命令维持调度窗口。新增退出竞争用例在“锁外空 callback hint 已取得”处暂停，再让 owner 发布并注册；修复后受锁保护的复核必须接住它。另覆盖退出先完成的反向顺序。debug barrier 不得持 THD 锁阻塞 collector；同步点未触发造成的超时不能当作根因证明。

本地 resume 的 COMMIT/ROLLBACK 数据断言与双独立 mysqld 的 transfer 接纳分开验证；后者不冒充真实物理备机 promotion。长 CALL、真实死锁及既有 unsupported wait 仍受原生处理和既有 deadline 约束；本设计不保证它们在两秒内结束。性能回归需同时观察无 CALL 的 sysbench/continuous 和 CALL 定向场景，不能用减少了状态推断 SLO 已经满足。

设计结论：采用“运行中按命令调度，退出时一次登记最终事务”。第 5.1 节退出/T0 交接与两个既有用例的屏障迁移已包含在实现中；实际验证见第 9 节，未覆盖的扩展矩阵仍保留，不能由定向通过推导完整验收。

## 9. 本轮实施与验证记录

CALL 核心修改保持在第 7 节四个文件中，扣除既有多语句 +8/-1 后，实际新增 **132 行、删除/改写 45 行**；其中 scheduler 为 +123/-44（包含原分支缩进调整和三个 MTR debug 同步点）。没有新增线程、外部参数、源文件或临时逐命令观测日志。CALL 修改本身不涉及 transfer/receiver 或锁热路径；用户追加批准的 ReadView 准入修改另记在第 9.1 节。均未提交。

- 旧代码 RED：`/private/tmp/call-support-red-20260911`。退出/T0、跨事务 CALL 两例均为 DRAIN 4013；日志分别证明退出覆盖丢失与旧事务身份封存后的换代失败，均非同步超时。
- no-bin Phase2 回归：`/private/tmp/call-support-reg-nobin-20260911`，31 个业务用例及 1 个 shutdown_report 全部通过。覆盖原多语句/调度专项，以及本轮新增退出交接、T0 pending replay、T2 waiter、混合两个 blocker、两个 running CALL blocker、退休 revision、真实二进制 prepared 嵌套 CALL。
- log-bin 同一 Phase2 回归：`/private/tmp/call-support-reg-logbin-20260911`，31 个业务用例及 1 个 shutdown_report 全部通过。两组均为 Debug、parallel=4、retry=0，不是全量 Preserve/Resume 或 Release 压测。
- 新 PS 用例使用原始 COM_STMT_PREPARE/COM_STMT_EXECUTE，返回后保持连接直至 DRAIN 接纳完成并验证下一命令 4020；不是 mysqltest 异步文本协议冒充 PS。
- CALL 同事务和跨事务两个既有用例已迁移到完整 InnoDB proof 合并后的锁外屏障；全为 running CALL 的零 support 合并也会触发。退休测试另明确命中 revision-discard 分支，不仅依赖总 stale 计数。
- 当前证据不是第 8 节全部扩展矩阵：完整物理 promotion MVCC、全部 binlog cache 尺寸换代、CALL 原生错误/最终 NONE 等组合、Release 压力及 SLO 尚未在本轮全部验证。

### 9.1 用户追加要求：standby transfer 支持合法 ReadView

跨事务 CALL 的普通 RR SELECT 已实际触发另一个既有拒绝：scheduler 正常 HARD、lineage/lock-proof 错误均为 0，但源端最终保存返回 `standby_transfer_strict_semantics_unsupported`。第一轮 CALL 成功验证暂用锁定读隔离此问题，不能把它当作 ReadView 已受支持。

用户要求解除这个限制。本轮已删除 `preserve_trx.cc::preserve_trx_resurrection_metadata_is_strict()` 与 `preserve_trx_transfer.cc::preserve_trx_transfer_validate_strict_eligibility()` 两处 presence-only 判断：**两个内核文件，新增 0 行、删除 4 行**。现有 bundle 结构/摘要校验、exact transaction identity、purge INIT/DISABLED 和事务号水位检查全部保留。不另建 importer，不改变调度或传输推进流程，不放宽其它引擎、GTID、predicate/waiting lock 限制。该追加修改与 CALL 四文件预算单列。

用户已确认按该最小范围实施。主会话执行以下清单，子会话仅只读审核，不提交：

- [x] 将 `standby_transfer_phase2_multistatement_routine_cross_tx.test` 恢复为普通 `SELECT`；新增 `standby_transfer_read_view_ready.test/.cnf/.result`，用 RR 先读旧值、其它事务提交新值、原事务保留自己的 UPDATE，再 DRAIN。已取得两例旧代码 DRAIN 4013 的 RED，非同步超时。
- [x] 删除 source 的 `!metadata.has_read_view && metadata.read_view_payload.empty() &&`，以及 receiver 中返回 `READ_VIEW_PRESENT` 的三行 `if`；不删除枚举或其它条件。现有 GUnit 的 presence-only 断言改用合法 `read_view_payload(100,90,95,80,{90,92})` 和 `rv_low_limit_no=80`，预期 eligibility 为 OK；只维护现有断言，不新增 GUnit。
- [x] `cmake --build build-debug --target mysqld preserve_trx-t -j8` 成功。定向 MTR 按 no-bin、log-bin 顺序完成，独立 vardir、parallel=4、retry=0。READY 用 TCP loopback 相同物理页 fixture 验证；现成 no-bin 重启 MVCC 用例独立证明恢复后快照语义，不冒充外部物理 HA。
- [x] 检查 ReadView 生产源码差异恰为两处四行删除；bundle、promotion、InnoDB ReadView/import 源码无 diff，`git diff --check` 通过，记录如下。

实际验证记录（2026-09-11）：

| 对照或回归 | 实际结果 | 本机证据 |
| --- | --- | --- |
| 两处门禁均保留 | cross_tx 与 ReadView READY 两例 DRAIN 4013；scheduler 正常 HARD，无 lineage/lock-proof 错误。cross_tx 日志明确为 `standby_transfer_strict_semantics_unsupported`。 | `/private/tmp/readview-red-20260911` |
| 只删除 source 一行，receiver 三行保留 | 同一 ReadView 用例 DRAIN SUCCESS、唯一 SURVIVOR、source `HAS_READ_VIEW=YES`；receiver ready=0、not-ready 非零，预期结果不匹配。不是超时。 | `/private/tmp/readview-source-only-20260911` |
| 两处均删除：no-bin | 9 个业务用例及 shutdown_report 全部通过。包括 cross_tx、ReadView READY、RR、RC、detach pin、export failure、import failure rollback、purge 可见性、RESUME 后首次读。 | `/private/tmp/readview-green-nobin-20260911` |
| 两处均删除：log-bin | 34 个业务用例及 shutdown_report 全部通过：31 个 Phase2 用例、ReadView READY、GTID strict READY、GTID mismatch。 | `/private/tmp/readview-green-logbin-20260911` |
| 现有 GUnit 定向回归 | `PreservedTrxTransfer.StrictEligibilityRejectsUnsupportedSemantics:*ReadView*`，8 个用例通过；没有新增 case。 | `/private/tmp/readview-gunit-20260911.log` |

最终 READY 用例在 no-bin/log-bin 均证明：RR 原事务先后读取均为 10，另一个事务已提交 20；DRAIN 保存一个含 ReadView 的 survivor，receiver ready=1、not-ready=0，accepted epoch=1，源连接后续命令返回 4020。源端 `HAS_READ_VIEW` 与 receiver READY 是两个独立断言，不把源端 P_S 行当成 receiver 缓存查询。receiver 保留 ReadView payload 的事实另由 semantic bundle 构建/安装源码确认。

上述结果是定向 Debug 回归，不是全量 MTR、Release 性能或外部物理备机 promotion 验收。现有 ReadView GUnit 验证合法/非法 payload 与 TLV 行为；尚未新增或运行 receiver 上“重算合法摘要但 ReadView 内容非法”的对抗解码用例，该解码保护本轮仅静态复核且未修改。只放开源端的中间 RED 未单独查询专属拒绝状态；归因依据为同 fixture 的两阶段对照及 receiver strict 调用链，不能表述为日志直接记录了 `READ_VIEW_PRESENT`。

跨事务 CALL 用例已改回普通 SELECT；receiver READY 与已有本地重启 MVCC 恢复的定向验证分别记录于上表。两项证据互补，但不冒充外部物理备机的完整 promotion 验收。普通在线 simulator 不满足 before-purge 契约；其导入后失败的反向清理另有既有局限，不在“删除准入限制”时顺手扩修。
