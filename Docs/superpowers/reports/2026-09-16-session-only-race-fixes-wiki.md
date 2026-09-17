# 接续名单与游标竞态修复说明

源码核查：2026-09-16；回归资料更新：2026-09-17。范围：相对基线提交 `6240edae50f7` 的 **5 个内核文件，新增 70 行、删除 25 行，增删合计 95 行，净增 45 行**。包含注释和 Debug 同步点，不包含测试、脚本和文档。

本文对照源码和测试实现编写，实施过程及最新 MTR 数字见[验证记录](../plans/2026-09-16-session-only-partial-packet-fix.md)。其中五个历史临时日志目录现已不存在，原 RED 和负向变异仅保留当时的文字记录；后续全量及压测另有现存报告，二者不可混为同一证据。本次只整理文档，没有重新编译或运行测试。下面区分“历史原问题复现”“受控交错验证”“人为缺漏验证”，不把六个用例都称为六个旧代码必现故障。

## 1. 这批代码解决什么

### 问题一：事务已经提交，却因下一条命令只收到半包而丢失接续资格

例如一个连接使用 autocommit INSERT，上一条 INSERT 已提交。DRAIN 已把它列入 session-only 名单——**没有待恢复的活动事务，但 receiver 仍需允许客户端按原连接 ID 接续一次**。

此时客户端发来下一条 INSERT，服务器只收到包头和部分内容。新业务执行已经被 CLOSING 拦住，但最终名单复查仍要求协议命令完成分类，于是把这个连接删掉。外部表现是：DRAIN 成功、新 INSERT 被 4020 拒绝，但 receiver 上按源连接 ID 执行 RESUME 得到 4023。

**修复：** 最终复查只在原候选名单内，允许“被归类为 CLOSING 之后、尚未收完整的包”保留资格；仍检查连接存活、无活动事务、无 participant、无临时表及无不支持上下文。已识别 QUIT、KILL、断连等仍排除，不等待客户端把包发完。

源码：[最终名单复查](../../../sql/preserve_trx.cc#L6085)、[资格条件](../../../sql/preserve_trx.cc#L5303)。对应测试：`standby_transfer_session_only_partial_packet`。

**这不是把 NONE 改成别的 token 状态。** 本批没有修改 `Preserve_batch_clear_target_generation` 的 NONE 清理；修正的是“哪些连接应保留在最终接续名单中”。已提交的 INSERT 本身不需要重新恢复。

### 问题二：收包线程拿着旧的阶段判断，跨过 CLOSING 后才登记

旧路径分两步：先读“是否进入 CLOSING”，再写该包属于 CLOSING 前还是后。两步之间，DRAIN 所在线程可能完成阶段切换和初次名单扫描。收包线程随后仍写入旧判断，即使连接已进入初次名单，最终复查也可能因“包属于 CLOSING 之前”再次将其排除。问题一的修复只接受 CLOSING 之后的半包，因此不能解决这个旧标记问题。

**修复：** 在已有 `LOCK_thd_data` 保护下，重新核对 manager 的状态和 owner 身份，再发布包的归类标记；若期间发生变化，解锁后重新采样。一个包只在首次成功登记时归类，后续等待不反复改归属。完整 gate 查询仍在锁外，没有增加新互斥锁。

源码：[包头登记](../../../sql/preserve_trx.cc#L11670)。对应测试：`standby_transfer_session_only_header_cross_closing`。

### 问题三：DRAIN 跨线程查看 PS/游标对象，与 CLOSE 释放对象交错

旧资格检查直接遍历另一条连接的 prepared-statement map，再访问 statement 和 cursor 指针。而该连接可以执行 `COM_STMT_CLOSE` 并释放 statement；这是容器和对象生命周期的并发访问风险，可能导致错误判定或无效内存访问。

**修复：** DRAIN 不再遍历别人的 statement map，改读连接上的原子游标计数。计数由连接自身维护：

- 打开游标前先计入，执行退出时按真实状态结清，避免打开过程被误判为零。
- FETCH 耗尽、STMT_RESET、CLOSE 和析构后更新计数；每个 statement 有独立标记，避免重复增减。
- 重预编译交换 cursor 时，同时交换计数归属；连接重置不提前清零，以免随后析构再次递减。

这里仍然**不支持带着打开的游标交接**，只是换成安全的检查方式。普通、从未使用游标的 PS 执行不做计数更新；Preserve 关闭时更新辅助函数直接返回。该改动涉及共享 PS 生命周期，并非只在 standby-transfer 专属函数内。

源码：[资格检查](../../../sql/preserve_trx.cc#L5282)、[FETCH 结清](../../../sql/sql_prepare.cc#L1963)、[关闭及计数更新](../../../sql/sql_prepare.cc#L2298)。对应测试：CLOSE 交错、游标生命周期、EOF、RESET 四个专项。

## 2. 补充了哪些 MTR，哪些属于“必现”

六个新增用例均位于 `mysql-test/suite/preserve_trx/`，各有 `.test/.cnf/.result`，通过[公共入口](../../../mysql-test/suite/preserve_trx/include/session_only_packet_case.inc)调用[真实协议 Python 驱动](../../../scripts/preserve_trx_session_only_packet_e2e.py)。MTR 管理本机两个 mysqld，驱动发送真实网络包，不是 mock。

运行时明确断言组合为 `DEPENDENCY_CONVERGENCE_V1 + BOUNDED_PIPELINE_V1 + STANDBY_TRANSFER_SAVE`，先执行并提交背景 INSERT。交错由 DEBUG_SYNC 安排；等待超时会使测试失败，不能靠超时后继续执行得到假通过。

| MTR 用例 | 场景与关键断言 | 证据性质 |
| --- | --- | --- |
| [standby_transfer_session_only_partial_packet](../../../mysql-test/suite/preserve_trx/t/standby_transfer_session_only_partial_packet.test) | HARD 后先拒绝一条命令并经过退出清理，再在最终名单复查时只发送下一条 INSERT 的部分包；DRAIN 不等剩余内容，连接仍能接续 | **原问题确定性交错复现**：历史修复前 RESUME 4023，修复后通过 |
| [standby_transfer_session_only_header_cross_closing](../../../mysql-test/suite/preserve_trx/t/standby_transfer_session_only_header_cross_closing.test) | 包头线程读完旧 gate 后暂停，DRAIN 跨过 CLOSING 并完成初次扫描，再放开包头线程 | **原问题确定性交错复现**：历史旧登记算法 RESUME 4023，修复后通过 |
| [standby_transfer_session_only_stmt_close](../../../mysql-test/suite/preserve_trx/t/standby_transfer_session_only_stmt_close.test) | 目标进入 CLOSE；owner 在检查这个特定连接前暂停；CLOSE 完成释放后 owner 继续；PS 已删除，连接接续成功 | **受控交错验证**。同步点绑定目标连接 ID；不能称为未插桩旧代码 UAF/crash 必现 |
| [standby_transfer_session_only_cursor_lifecycle](../../../mysql-test/suite/preserve_trx/t/standby_transfer_session_only_cursor_lifecycle.test) | 两个真实打开的游标使 DRAIN 被拒绝；关一个仍拒绝；覆盖 FETCH、RESET、重预编译成功/失败、普通 PS、无结果 DML、RESET_CONNECTION 及最终接续 | **生命周期回归**，不是一个独立的旧代码必现故障 |
| [standby_transfer_session_only_cursor_eof](../../../mysql-test/suite/preserve_trx/t/standby_transfer_session_only_cursor_eof.test) | 一次 FETCH 取尽两行，断言 LAST_ROW_SENT 且无 CURSOR_EXISTS；随后不再清理目标连接，直接 DRAIN；PS 仍存在而接续成功 | **人为缺漏的确定性负向验证**：仅去掉 FETCH 计数更新，DRAIN 4013；RESET 对照仍通过 |
| [standby_transfer_session_only_cursor_reset](../../../mysql-test/suite/preserve_trx/t/standby_transfer_session_only_cursor_reset.test) | 真实打开游标后 STMT_RESET；不再发送 FETCH/CLOSE/EXECUTE/RESET_CONNECTION，直接 DRAIN；PS 仍存在而接续成功 | **人为缺漏的确定性负向验证**：仅去掉 close_cursor 计数更新，DRAIN 4013；EOF 对照仍通过 |

EOF/RESET 的负向变异是在验证**新增计数维护不可遗漏**，不是证明旧遍历算法原来就有这两个计数问题。历史记录确认变异已撤回；当前源码也保留两处更新。

这些 session-only 成功用例中的 `NO_PRESERVABLE_TOKENS` 指没有事务 token，不代表没有接续名单。测试随后验证源连接 ID 首次 RESUME 成功、再次 RESUME 4023；分包用例还验证新 INSERT/COMMIT/ROLLBACK 仍被 4020 拦截，CLOSE 用例验证 COMMIT/ROLLBACK 4020，均核对已提交数据不变。

另调整了既有 [resume_required_token_set_lint](../../../mysql-test/suite/preserve_trx/t/resume_required_token_set_lint.test)，令其约束与新的最终名单规则一致。它是源码检查，**不能代替上述运行时复现**。

## 3. 历史验证结果与边界

以下为 2026-09-16 定向回归的历史记录，不是本次整理新跑的结果：

| 验证范围 | 行为用例通过 | 源码检查通过 | 跳过 |
| --- | ---: | ---: | --- |
| no-bin 定向回归 | 20 | 4 | 0 |
| log-bin/ROW 定向回归 | 17 | 4 | 3 个既有用例要求关闭 binlog |
| 六个新专项及既有 PS/OFF 用例，各重复三次 | 21 | 0 | 0 |

合计 **58 次行为检查、8 次源码检查通过**，不含另行通过的 shutdown_report；重复次数不是新增独立用例数。原问题失败及负向变异的记录均已在原文列出日志名称，但本次无法重新读取这些临时日志。

上述定向结果不能替代全量 MTR。2026-09-17 已补跑常规全量及 big-test，精确计数、跳过原因与客户端容量参数差异见[后续回归汇总](../plans/2026-09-16-session-only-partial-packet-fix.md#2026-09-17-后续回归汇总)。仍不能声称 Release 性能全部达标、真实物理备机切换已验证或所有游标并发组合已穷尽。尤其“打开过程中的提前计数”和“仍被计数的游标跨 pre-execute reprepare 交换”，不能只凭顺序生命周期测试宣称获得并发证明。

## 4. 95 行分布与未改动范围

| 内核文件 | 新增/删除 | 承载内容 |
| --- | ---: | --- |
| [sql/preserve_trx.cc](../../../sql/preserve_trx.cc) | +36/-12 | 最终名单资格、包头登记同步、读取游标计数、Debug 同步点 |
| [sql/sql_class.cc](../../../sql/sql_class.cc) | +0/-9 | 删除跨线程遍历 statement map 的辅助函数 |
| [sql/sql_class.h](../../../sql/sql_class.h) | +4/-4 | THD 原子游标计数、删除旧声明、修正包头标记注释 |
| [sql/sql_prepare.cc](../../../sql/sql_prepare.cc) | +28/-0 | PS 游标计数生命周期及 CLOSE 同步点 |
| [sql/sql_prepare.h](../../../sql/sql_prepare.h) | +2/-0 | 每个 PS 的计数归属标记及更新接口 |

本批没有改调度器、InnoDB、transfer/receiver、ACK/epoch/token 协议，也没有放开 HARD 后的新业务命令。它修正资格与收包登记，不通过扩大命令放行范围来保住名单。Debug 同步点服务于正式 MTR；EOF/RESET 的临时负向变异不保留在正常源码中。


## 5. 后续 Release 回归边界（2026-09-16至17）

以下均为已执行结果的摘要，不是本次文档修改重跑；使用同一 Release SHA256：`de3275f577e3c598efe023a4b13481e7667b5237a58d0495234037973fb9efdb`。

| 回归 | 已观察到的交接结果 | 不能省略的限制 |
| --- | --- | --- |
| sysbench OFF，1000 并发，300s | DRAIN 成功，124/124 READY，严格 Phase2 539.856ms | source 清理时因 SIGTERM 与单向停 purge 契约不兼容而断言退出，整轮为 cleanup_failed，不是 PASS |
| sysbench ON，1000 并发，300s | runner 通过；0 事务 token 的 session-only control commit，1000 原连接 HOLD | 事务 ACK→READY 不适用；未逐连接执行 RESUME；有业务可重试 1062，不能称零业务错误 |
| TPC-C，300 仓、1000 并发，300s | FUNCTIONAL_PASS，190/190 READY | 严格 Phase2 4.287776s，最后命令→ACK 675.261ms，未满足相应性能目标 |
| 128+16，三种提交型及一种不提交型，各一轮 | 四轮 DRAIN/READY 完整，三个 runner 通过 | range-10000 的最后命令→ACK 957.148ms 超限；smoke 不是正式规模 SLO 验收 |
| 剩余 full 六组，各一轮：1000+100 三形态、不提交、mixed、standby | 六轮 DRAIN 成功，survivor 全部 READY | 六个 runner 均未通过完整门限；mixed 有一次 connection-error 分支未定因；continuous 的 Phase1 TPS 影响因时钟映射误差为 INVALID |

这些是本机 source/receiver transfer/READY 测试，不代表物理复制及晋升后 SQL RESUME 全链路通过。后续执行采用已确认停止 purge 后 SIGKILL source 的测试退出方式，不抹去 sysbench OFF 的实际清理失败，也不表示原通用 runner 的所有退出路径已适配。

本地证据标识（仓库根目录相对路径，被 Git 忽略；本文保留结果摘要，原始报告不随普通提交携带）：

- `build-release/preserve-final-evidence/fullpressure-runs/session-race-sysbench-{off-20260916-pmsyLh,on-20260916-ruPDtt}-r1/RUN-SUMMARY.md`（花括号表示两个目录）。
- `build-release/diagnostics/tpcc-current-20260917/SUMMARY.md`。
- `build-release/preserve-final-evidence/scale128-both-20260917/SUMMARY.md`。
- `build-release/preserve-final-evidence/remaining-full-20260917/SUMMARY.md`。
