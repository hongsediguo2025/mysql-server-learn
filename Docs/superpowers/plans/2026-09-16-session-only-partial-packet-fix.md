# Session-only 接续名单收包竞态：最小修复与验证

> 2026-09-17 文档整理：当前实现为 5 个内核文件 `+70/-25`。以下按实施阶段保留历史记录，最新回归汇总见文末；本次仅整理文档，未重跑测试或提交代码。
>
> 历史证据边界：文中 `session-only-fix-1Rew02`、`preserve-none-rootcause-DaJdjd`、`session-packet-followup-J6sQBs`、`session-packet-completion-upOvxJ`、`session-proof-tIIDjh` 五个临时目录现已不存在。保留目录标识、日志名及当时记录，但它们不是当前可复读或随仓库提交的原始证据；后续 GREEN 也不能代替历史 RED 原始日志。

**目标：** 已确认无事务、且新业务执行已被 CLOSING 封住的连接，不能仅因下一条命令尚未收完整而丢失 RESUME 接续资格。

**设计：** 最终快照只处理原 planned IDs，复用资格函数已有的 `allow_unclassified_post_closing` 选项。追加修复将收包标记与初次目标扫描同步，并以连接线程维护的游标计数替代跨线程遍历。它们不改变 batch NONE 的含义，不等待网络包完成，不改变可执行命令类别或 HARD/CLOSING gate，不改 receiver、token、ACK 或传输逻辑。已识别 COM_QUIT、KILL、断连、真实事务/participant、临时表及不支持上下文仍被排除。包头被线性化归类为 CLOSING 之前的命令仍不获此豁免。

## 方案取舍

- 采用：最终快照复用已有的 post-CLOSING 资格选项，保持其余检查。
- 不采用：等待完整命令包；客户端/网络停顿会拖长 DRAIN。
- 不采用：延迟全部 NONE 清理或新增冻结状态；会扩大线程等待和清理协议。

尚未收完整的未来 QUIT 无法预知；最终快照时仍存活、未识别为退出的连接，与快照之后才断连的 idle 连接采用相同边界。已经识别的 COM_QUIT 仍须排除。

## 实施清单

- [x] 复核主会话留下的 Python 协议 E2E，由单个 MTR 启动执行。使用既有 DEBUG_SYNC，真实 INSERT 已提交；覆盖 DNT 清理后分包、新 INSERT/COMMIT/ROLLBACK 4020、原数据不变、RESUME 一次成功后重复拒绝。
- [x] 修改内核前运行新 MTR：`session-only-fix-1Rew02/red-no-bin.log`，因分包连接 RESUME 4023 失败；未出现用例启动或权限错误。
- [x] 修改 `sql/preserve_trx.cc` 最终快照调用为 `preserve_trx_session_only_candidate_is_eligible_locked(candidate, true, true)`，同步修正过时注释和 source-shape 约束；不新增内核字段、锁或扫描。
- [x] Debug 增量编译成功。新用例 no-bin/log-bin 串行通过；现有 control-only final snapshot、transaction final snapshot、mixed、claim race、unsent ACK restore、模式及 lint 用例通过。
- [x] 专项用例同时验证完整命令和分包连接的接续；真实 Phase2 COMMIT、OFF/LEGACY 原生多语句对照均通过。已审核 diff，未开启全量压测，不声称性能验收。

## 历史 RED 记录

同一个 Debug binary 上，no-bin 与 log-bin/ROW 均出现：旧 INSERT 已提交，新 INSERT 最终 4020，DRAIN 成功，但分包连接 RESUME 4023。只令同一命令在最终快照前收完整并返回 4020，RESUME 即成功。已有定向实验位于 `preserve-none-rootcause-DaJdjd`；新 MTR 用于保留可重复执行的回归覆盖。

## 首次修复验证结果（历史记录）

证据目录：`session-only-fix-1Rew02`。全部在本机 Debug 构建运行，MTR `--parallel=1 --retry=0`，各轮 vardir 独立。

| 阶段 | 结果 | 日志 |
| --- | --- | --- |
| 修复前 no-bin 专项 | 1 个业务用例失败，首因 RESUME 4023 | `red-no-bin.log` |
| 修复后 no-bin | 9 个业务用例 + 1 个 source-shape lint 通过 | `green-no-bin.log` |
| 修复后 no-bin 补充 | 2 个业务用例通过 | `green-extra-no-bin.log` |
| 修复后 log-bin | 12 个业务用例 + 1 个 source-shape lint 通过 | `green-log-bin.log` |

各轮 `shutdown_report` 另行通过，不计入上表。修复后合计 25 次定向检查通过，不是全量 MTR。

验证覆盖：分包时 DRAIN 无需等待 payload 收完；接续成功且只能消费一次；新 INSERT/COMMIT/ROLLBACK 仍为 4020，原数据不变；已分类 QUIT 与已断连连接不获接续授权；混合事务名单、接续 generation 竞争、未发送 ACK 恢复、实际 COMMIT 后的 session-only 转换及 DEPENDENCY/LEGACY/OFF 原生命令行为。

内核仅 `sql/preserve_trx.cc` 改动 `+9/-6` 行（含注释和换行），语义变化为最终快照调用增加已有布尔参数。源码 lint 为 `+3/-3` 行。专项 MTR 的 test/cnf/result 与 Python E2E 共 310 行，复用既有 DEBUG_SYNC，不增加内核测试钩子或临时诊断。

Debug binary SHA256：`88c45d9552e76345c8cd61918dd8c9b2080817a4ca38bba787307b9457df2fc8`。没有重编译 Release、运行全量 MTR 或大规模压测；没有提交或 push。

## 追加审核后的收敛实施

上述构建和结果是首次修复时的历史记录，不代表追加修改已验证。用户已批准继续识别并修正审核发现的问题；本轮仍不提交。

### 1. 跨 CLOSING 的收包归类

原代码先读取 CLOSING 状态，再发布 `before_closing`，两步之间可能被初次目标扫描穿过。最终核验即使允许未分类包，仍可能因旧的 `before_closing=true` 丢失接续项。

- [x] 在原算法的状态读取后、标记发布前增加锁外 `preserve_trx_header_after_closing_gate_sample` 同步点。
- [x] 新增 `standby_transfer_session_only_header_cross_closing` MTR：owner 停在 HARD；应用连接发送半包并停在上述同步点；owner 完成初次名单扫描；再释放收包线程。`red-header-no-bin.log` 记录旧算法在 receiver RESUME 得到 4023。
- [x] 取得 RED 后，在既有 `LOCK_thd_data` 内发布 packet marker，并核对锁外采样的 manager state/owner 是否仍有效；发生变化则解锁重采。首次归类后的 QUIESCED/ATTACHING 等待不重新归类。
- [x] 完整 gate 查询仍在锁外；锁内只做无锁 manager 快照核对。没有引入 THD mutex → active-attempt mutex 的反向锁序。
- [x] 原半包用例及新跨界用例均验证：旧 INSERT 已提交、新业务 4020、原连接仍在、首次 RESUME 成功、重复 RESUME 4023。no-bin/log-bin 顺序运行。

### 2. 测试同步必须失败关闭

`DEBUG_SYNC WAIT_FOR` 超时只产生 warning，ERR 响应也无法携带 warning count。因此不依赖解析 warning 来兜住同步失败。

- [x] 在安装同步动作前，为 source owner、HA controller、两个应用 THD 设置会话级 `debug_sync_abort_on_timeout`；仅限 Debug MTR，不修改全局配置或内核超时语义。
- [x] 隔离实例的 `sync-timeout-negative.log` 记录了启用守卫后，缺少 signal 导致 `debug_sync_execute` 中止服务器并使 MTR 明确失败。临时 `zz_session_*` 用例已移出源码目录；探针及日志当时保留在证据目录，不纳入提交；该临时目录现已不存在。
- [x] 对正常原半包与跨界用例重新取得 GREEN；未增大同步超时。

### 3. 游标检查的并发访问

- [x] 沿 `COM_STMT_CLOSE`、PREPARE/EXECUTE/FETCH/RESET、statement map 清理和资格检查核实 writer/read-side。删除跨线程遍历 statement map 的资格检查，改读 THD 原子游标计数；没有给整个 statement map 增加锁。
- [x] `header-green-cursor-red.log` 保留 CLOSE 交错下旧路径 RESUME 4023 的记录。新增 CLOSE 交错及游标生命周期 MTR，验证开着两个游标时拒绝 DRAIN、关掉一个仍拒绝、FETCH 耗尽、RESET、重新执行、重预编译成功/失败及 RESET_CONNECTION 清理。

计数由连接线程维护：打开之前先占一份，执行退出时按实际游标状态结清，FETCH/RESET/CLOSE/析构释放；重预编译交换 cursor 时同时交换计数归属。`THD::init()` 不提前清零计数，以免随后 statement 析构再次递减。普通无游标 PS 执行不更新计数；OFF 时辅助函数立即返回。`rds_preserve_trx_enable` 的实际开关变化被既有 startup-only 检查禁止，不存在运行中 OFF→ON 漏掉旧游标的合法入口。

### 构建和回归记录

- 证据目录：`session-packet-followup-J6sQBs`。
- 当前工作区已经是独立 worktree，保留现有脚本及文档修改。
- 先构建 Debug 取得 RED，再做最小修复及 GREEN；MTR 使用独立 vardir、`--retry=0`，no-bin/log-bin 不同时运行。
- 重点回归原半包、跨界、final snapshot、mixed、claim race、unsent restore、PS/cursor、命令收包、HARD cutoff、OFF 和 source-shape 用例。仅有 MTR 行为测试通过不代表性能 SLO 已达成。

## 2026-09-16 接续核验

主会话中断时，追加内核修复及专项用例已在工作区。接续复核没有扩大内核语义修改，只整理 include 顺序及测试说明；重新编译当前 Debug 并运行回归。新证据目录：`session-packet-completion-upOvxJ`。

| 当前构建验证 | 行为用例通过 | 源码约束通过 | 跳过 |
| --- | ---: | ---: | --- |
| no-bin 定向回归 | 18 | 4 | 0 |
| log-bin/ROW 定向回归 | 15 | 4 | 3 个既有用例要求关闭 binlog |
| 四个新专项及 PS/OFF 用例各重复 3 次，no-bin | 15 | 0 | 0 |

三轮 `shutdown_report` 分别通过，另计。合计 48 次行为用例、8 次源码约束检查通过；重复次数不计作新增独立用例。PS 生命周期用例分别在新模式、重启新模式和 OFF 下执行真实 EXECUTE、FETCH、CLOSE 及错误返回，不仅检查参数值。新增 standby 用例运行时断言 `DEPENDENCY_CONVERGENCE_V1 + BOUNDED_PIPELINE_V1 + STANDBY_TRANSFER_SAVE`。

当前修复的内核 diff 共 5 个文件，合计 `+67/-25`（含注释及供正式回归使用的 Debug 同步点）：`preserve_trx.cc`、`sql_class.cc/.h`、`sql_prepare.cc/.h`。`sql_parse.cc`、调度器、transfer/receiver 和 InnoDB 均无本次工作区改动。临时诊断未加入生产日志；相关 Debug 同步点由正式 MTR 使用，并非待清理的无用探针。

Debug binary SHA256：`f8323e5f91ac5c0d564de04f2342df89edcbef7492d9197e3ac89ff9f4292802`。本轮不重编译 Release、不运行全量 MTR 或压测，不宣称性能验收；不提交、不 push，其他既有脚本和文档修改保持不动。

另外，主会话的旧 binary/OFF 探针在同一 PS 先请求游标、随后改为无游标执行时发生原生异常，见 `native-cursor-old-off-final.log`；该探针未执行 DRAIN。此独立问题未纳入本次接续名单修复，不应把上述通过结果理解为已经修复所有原生游标组合。

## 2026-09-16 审核缺口的测试补强

本轮只补测试并细化 Debug 同步点，不改变生产游标计数算法、命令门禁或 transfer/receiver。证据目录：`session-proof-tIIDjh`。

- CLOSE 交错：同步点名称带被检查连接的 thread ID，owner 的等待精确绑定应用连接，其他候选不能消耗 `EXECUTE 1`。顺序为“目标进入 CLOSE → owner 检查该目标前暂停 → 目标完成 deallocate → owner 继续”。这个 GREEN 证明指定对象的交错正确，不单独宣称复现了未插桩旧代码的容器遍历 UAF。
- 新增 `standby_transfer_session_only_cursor_eof`：真实 SELECT 打开游标，一次 FETCH 取尽全部两行；断言 `LAST_ROW_SENT` 且无 `CURSOR_EXISTS`，随后目标不再发命令，直接 DRAIN。
- 新增 `standby_transfer_session_only_cursor_reset`：真实打开游标后执行 STMT_RESET，读取 OK 后目标不再发命令，直接 DRAIN。
- 两个新增用例都在 DRAIN 前后精确查询 connection ID＋statement ID，证明 PS 尚未析构；验证背景 INSERT 数据不变、首次 RESUME 成功、重复 RESUME 4023、源端新 COMMIT 4020。没有追加 FETCH/CLOSE/EXECUTE/RESET_CONNECTION 来掩盖计数遗漏。
- SELECT 执行必须收到结果集和 `CURSOR_EXISTS`，普通 OK 不算打开成功；旧双游标生命周期用例也使用此断言。

### 负向验证：证明新增用例能识别漏清理

以下是分别制造单一缺漏的测试变异，不是声称当前正常构建存在故障。两轮均无超时或崩溃，失败落在真实 DRAIN 返回 4013。

| 临时变异 | EOF 用例 | RESET 用例 | 日志 |
| --- | --- | --- | --- |
| 仅去掉 FETCH 结束时的计数更新 | DRAIN 4013 | 通过 | `negative-eof.log` |
| 恢复 FETCH，仅去掉 close_cursor 的计数更新 | 通过 | DRAIN 4013 | `negative-reset.log` |

两处变异均已恢复。`sql_prepare.cc` SHA256 恢复为本轮开始前的 `d96e1b56fab1fee3be3cd0e2bc065aca9e255b5ee9107ac6695f37840a883fa3`，再编译 Debug 并运行以下回归：

| 正常构建回归 | 行为通过 | 源码约束通过 | 跳过 |
| --- | ---: | ---: | --- |
| no-bin 定向 | 20 | 4 | 0 |
| log-bin/ROW 定向 | 17 | 4 | 3 个既有用例要求关闭 binlog |
| 六个 session-only 专项及 PS/OFF 用例各重复 3 次，no-bin | 21 | 0 | 0 |

合计 58 次行为、8 次源码约束检查通过，三轮 `shutdown_report` 另计；不是全量 MTR。SELECT 的普通 OK 拒绝断言在 log-bin 和最终 no-bin 重复轮均已执行。日志分别为 `regression-no-bin.log`、`regression-log-bin.log`、`repeat-three.log`。最终 Debug binary SHA256：`405a912a60a74d2d837c2b3367ddf6b166147e98ad30dfeca6f21960f16ebe64`。

当前内核累计 diff 为 5 文件 `+70/-25`；相对上一轮仅 Debug 同步点细化增加 3 行，无新的生产语义修改。没有新增 unit/GUnit、临时日志或故障注入开关，未提交或 push。本轮不证明 Release 性能，也不把顺序生命周期覆盖扩大为“打开游标过程中提前计数”或“仍被计数的游标跨 pre-execute reprepare 交换”的并发证明。


## 2026-09-17 后续回归汇总

以下核对已留存的运行报告和日志，不是本次文档整理重新执行测试。当前 Debug SHA256 仍为 `405a912a60a74d2d837c2b3367ddf6b166147e98ad30dfeca6f21960f16ebe64`。

| 执行范围 | suite 用例通过 | shutdown_report | 条件跳过 | 失败 |
| --- | ---: | ---: | ---: | ---: |
| 常规全量 no-bin，配置修正前，顶层显式客户端容量 400 | 433 | 1 | 151 | 0 |
| 配置修正后三项 no-bin，不补客户端容量参数 | 3 | 1 | 0 | 0 |
| 配置修正后三项 log-bin，不补客户端容量参数 | 3 | 1 | 0 | 0 |
| 常规全量 log-bin，配置修正后，不补客户端容量参数 | 413 | 1 | 171 | 0 |
| big-test 选集 no-bin | 17 | 1 | 1 | 0 |
| big-test 选集 log-bin | 2 | 1 | 16 | 0 |

常规全量两模式覆盖 566 个不同用例；big-test 两模式补齐 18 个不同用例。suite 通过数包含源码 lint，不全部等于业务测试。big-test 的 log-bin 轮中，`multi_session_100_resume` 自带配置强制关闭 binlog，不能将两项均称作开启 binlog 的验证。

完整 no-bin 是修配置前带 `--max-connections=400` 的结果；修配置后的 no-bin 仅重跑三个受影响用例，**没有声称修正后无补参重跑了双模式全量**。六个 session-only 新用例均在常规完整 no-bin/log-bin 中通过，且自身断言新调度＋有界流水线＋standby transfer 组合；既有 LEGACY/OFF 对照和 LOCAL_CARRIER big-test 保持原契约。配置修正及重跑命令见[高连接数说明](2026-09-17-mtr-high-connection-client-config.md)。

本地证据标识（以下路径相对仓库根目录，目录被 Git 忽略，不随普通提交携带）：

- 常规 MTR：`build-debug/preserve-final-evidence/mtr-full-20260917-8ileAp/`，含 `SUMMARY.md` 和原运行日志。
- big-test：`build-debug/preserve-final-evidence/mtr-big-20260917-75xuqsu2/`，含 `index.json`、`nobin.log`、`logbin.log`。

部分测试数据库副本已为后续压测清理，日志和报告仍保留；不把证据目录当成完整数据库备份。后续 Release 压测的通过项、超限和清理异常见[修复说明的回归边界](../reports/2026-09-16-session-only-race-fixes-wiki.md#5-后续-release-回归边界2026-09-16至17)。MTR 通过不等同于性能 SLO、物理备机晋升或所有并发组合完成验收。
