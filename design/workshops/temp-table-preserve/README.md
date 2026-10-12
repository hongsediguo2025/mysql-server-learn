# 临时表与待 FETCH 结果 Preserve/Resume

更新：2026-10-12。当前目录 `/Users/a1234/project/mysql-server-8022-preserve-port`，分支 `ha_preserve_trx`；本次Receiver TEMP／控制RESUME生命周期修复基于`75be6a9eab30`，实现与最新证据见下。此前连接管理交接基于`bc09a3e741ce`；旧记录按版本保留，不替代当前验收。未经用户指令不提交或push。

## 当前设计入口

2026-10-12 已完成 [Receiver I/O生命周期修复 R1–R6](receiver-io-lifecycle-fix-plan.md#9-实施复核补充2026-10-12)：独立控制请求先挂载target再等TEMP，错误响应返回后服务端KILL目标，目标原线程回滚、原reaper收尾；partial入选尾部继续，真实使用者退出后退休输入／prepared owner，调度分配异常成对清理。物理工程需在原独占接管／TLS路由中登记`Preserve_trx_control_resume_request`，不改变三个升主接点或cursor关联接口，不扩展同会话／local／RESET DRAIN。

最新全量Debug含big test：977个业务用例、18个big-test、21个新增用例均在适用模式通过，shutdown各通过。no-bin完整运行早于既有协议测试观察期限修改，最终版该例单独补跑；旧失败记录保留。同版Release的500连接两轮、TPC-C128、只写1000及M矩阵62/62本地功能通过；TPC-C／只写原验收通过，500缺EXACT样本，mixed-transfer strict25304.702ms／EXACT1738.488ms未达标，不能宣称性能全部闭合。还有10个原有配置未同版重跑，生产物理复制接线／升主／恢复后业务尚未验收。[最新MTR](../../../build-debug/preserve-receiver-lifecycle-20261012/README.md)、[任务跟踪E152](task-tracker.md#e152receiver-temp与控制-resume-生命周期修复2026-10-12)。

下面保留连接管理、E128及此前各轮的历史实施与证据。

2026-10-12 已实现 [连接管理跨主交接](connection-management-handoff-design.md)：standby transfer 下统一 QUIT／RESET／新连接最终准入与 CLOSING 的裁决，保留完整初始化及原生错误语义。新增 22 个 MTR；全量含 big test 加串行修正／补验后，956 个业务用例全部在适用模式通过，18 个 big test 全通过。原 no-bin 的 1 个 lint 失败及修正证据保留，见 [MTR 验证报告](../../../build-debug/preserve-connection-handoff-20261011/README.md)。随后同版 Release 的 500 连接持续 TEMP/FETCH、TPC-C128、sysbench 只写1000各运行一轮，本地交接功能均通过，strict Phase2／ACK→READY 均达标；500 模型 EXACT 无有效样本，原综合性能验收仍未通过。M1–M12 的62个配置各一轮，原场景验收62/62通过（含3个对照、3个预期拒绝）；56个成功迁移配置 strict／ACK→READY 均达标。真实 proxy／物理升主验收未覆盖。见 [Release 三模型](../../../build-release/preserve-connection-pressure-20261012/README.md)、[M 矩阵](../../../build-release/preserve-matrix-20261012/README.md)及[设计 §9.4](connection-management-handoff-design.md#94-release-压力回归)。

E128 已实现 [TEMP 后台完成与 session RESUME 屏障](async-temp-resume-barrier.md)：READY 后复用原 worker 完成最终写入、flush、close，SQL RESUME 只等待自己的资源。此前 E126 `seal_private` 已撤除。70个相关MTR在适用模式通过；Release原500连接持续模型五轮本地功能、strict及ACK→READY均通过，独立EXACT门槛和外部物理工程验收仍未关闭。详情见[本轮证据](../../../build-debug/preserve-temp-async-e128/README.md)。

新增正式图解入口：[用户临时表与待 FETCH 结果的跨主恢复（HTML）](../../html/preserve-resume-8.0.22-temp-table-fetch-guide.html)。2026-10-08 按当前源码与 E107／E108 核对，含背景知识、21 张分解图、控制 session 关联合同、性能口径和验证边界；本页下方 E63／E64 数据保留为历史记录。

2026-10-08 按用户确认调整部署默认值：`rds_preserve_trx_memory_budget_bytes=2147483648`（2 GiB）、`rds_preserve_trx_transfer_runtime_profile=PROMOTION_PREPARE`、`rds_preserve_trx_temp_id_namespace=ON`。生效时点与兼容条件见[详细设计 §1.2](detailed-design.md#12-当前部署默认值2026-10-08)，默认值定向验证见任务跟踪 E109，随后全量含 big test 的验证见 E110；历史性能数据不改写。E111 将结果捕获统一移到 DRAIN Phase1，并删除独立 `result_capture_enable` 参数；DRAIN 前不生成捕获工件，详见详细设计 §6。E114 进一步让 Phase1 新结果边生成边发送；存量和可选流回退继续复用安全借用扫描。结果接收不 fsync，OPEN 前缀不能 READY；当前功能与性能证据见任务跟踪 E114。 最终全量含big test双模式零失败；追加同版Release五轮功能均通过，但strict≤2.2s为4/5、ACK<500ms为0/5，性能未关闭。

本工程保存用户临时表和已物化的 Classic cursor 结果；物理复制工程已负责 session 上下文及 PS 回放。完整顺序为：

```mermaid
flowchart LR
    S[源端完整命令边界] --> T[传输 TEMP / 结果]
    T --> R[receiver 提前准备并 READY]
    R --> P[既有在线升主接点]
    P --> Q[SQL RESUME<br/>接管事务和资源 owner]
    Q --> X[外部 PS 回放]
    X --> A[显式关联源 PS 对应 cursor]
    A --> C[CLOSE 补发完成后放行业务]
```

SQL RESUME 不创建 PS，不恢复其 SQL/参数/历史依赖；客户端不改，也不重新 SELECT 生成旧结果。CLOSE 静默和生命周期规则保留，LONG_DATA 专属迁移/拒绝代码随 PS 本体删除。仅有普通 PS 的会话不产生 PS 工件。local startup、RESET DRAIN、X Protocol、打开中的 HANDLER 不扩展。

| 阅读目的 | 当前文档 |
| --- | --- |
| 用一张流程图理解功能 | [简明设计](design.md) |
| 需求背景、结果形态及责任边界 | [需求与范围](requirements-and-scope.md) |
| 捕获、传输、READY、升主、RESUME 和 attach 细则 | [详细设计](detailed-design.md) |
| CLOSING 期间 QUIT/RESET、新连接准入及恢复后执行（已实施并完成 MTR 回归） | [连接管理命令跨主交接设计](connection-management-handoff-design.md) |
| READY后TEMP I/O、partial尾部、控制RESUME失败及真实回滚／文件退休（R1–R6已实施） | [Receiver I/O生命周期修复设计](receiver-io-lifecycle-fix-plan.md) |
| 删除哪些代码、关联 API、身份与失败语义 | [PS transfer 删除与 cursor 关联](ps-transfer-removal-and-cursor-attach.md) |
| TEMP 增量与结果文件复用 | [增量捕获和传输](incremental-capture-and-transfer.md) |
| 当前真实文件和共享薄接点 | [源码分工](source-layout.md) |
| READY、RESUME、外部回放的成本边界 | [安装与关联开销](resume-install-optimization.md) |
| 实施状态、证据与待办 | [任务跟踪](task-tracker.md)、[剩余工作](remaining-work.md) |

## 历史验证状态（E62–E64）

- E62：删除 PS 本体迁移，接通独立结果清单、receiver 提前准备、RESUME owner 和回放后显式 attach。相对基线内核净减少 **9437 行**，包括 8 个新增 result 文件；不是新的剩余工作估算。
- E63：全量 `preserve_trx` MTR 含 big-test，no-bin **598 通过/285 跳过/0 失败**，log-bin **596/287/0**，shutdown 各另 1；两轮覆盖并集 883 项，18 个不同 big test 实际通过。通过数各包含 36 条 lint。
- E64：原有 11 个压力模型各一次，**3 通过、8 未通过**；临时资源 M1–M12 的 **62 个配置通过各自场景断言**。56 个 READY、3 个预期负例、3 个无 DRAIN；M11 两档限速 strict 超过 2 秒。不能宣布统一性能目标全部达标。
- 1000 并发 sysbench 读写本轮通过，strict Phase 2 **526.657 ms**，EXACT 尾 **330.834 ms**；不由一轮推广到其他模型。tiered 原指标通过，但精确尾部 **627.883 ms** 仍超 500 ms。
- E63 之后适配了五个压力驱动，内核和 MTR 用例文件未变；部分驱动也是 MTR 依赖，适配后未重跑整套，因此 E63 是当时快照的全量证据。外部物理升主、真实 PS 回放/关联、proxy CLOSE 和连续迁移仍需集成验收。

证据：[E63 全量 MTR](../../../build-debug/preserve-mtr-big-20261004-152746/report.txt)、[E64 原模型](../../../build-release/original-pressure-ps-removed-20261004/RESULTS.md)、[E64 资源明细](../../../build-release/all-pressure-20261004/RESULTS.md)。详细失败与指标边界在任务跟踪 E64，不以旧版本成绩代替当前验收。

## 历史资料如何使用

以下资料保留原日期、方案、源码快照和 RED/GREEN 结果。涉及 PS 定义、参数、factory/rebuild、依赖证明、PS BASE/DELTA 的章节已经撤下，不能照此继续实施。TEMP、结果、额度和清理方面的历史经验仍可查阅，但当前接口以本页导航为准。

| 历史类别 | 文档 |
| --- | --- |
| 删除前的完整设计与评审 | [旧详细设计](detailed-design-before-ps-removal.md)、[复审记录](detailed-design-review.md) |
| 实施过程及早期缺口 | [实施日志](implementation-plan.md)、[旧实现核查](existing-implementation-and-gaps.md) |
| 旧静态估算 | [代码量估算](code-volume-assessment.md)、[内核工作量](kernel-effort-assessment.md) |
| 已删除的 PS 接线 | [prepared](ps-prepared-integration.md)、[receiver](ps-receiver-integration.md)、[transfer](ps-transfer-integration.md) |
| 结果捕获问题 | [根因](cursor-root-cause-2026-09-28.md)、[方案](cursor-fix-plan-2026-09-28.md)、[修复](cursor-fix-2026-09-28.md) |
| 旧 PS 换代问题 | [根因](ps-churn-root-cause-2026-09-28.md)、[方案](ps-churn-fix-plan-2026-09-28.md)、[修复](ps-churn-fix-2026-09-28.md) |
| 旧 PS 性能优化 | [规模复测](ps-scale-retest-2026-10-01.md)、[重复工作裁剪](ps-work-pruning-2026-10-01.md) |
| 原始压力修复及后续汇总 | [回归修复](original-pressure-regression-fix-2026-09-28.md)、[完成情况](original-pressure-complete-2026-10-01.md)、[500 读写](sysbench-rw-500-2026-10-01.md) |
| 旧 Release 资源矩阵 | [基线](release-baseline-2026-09-28.md)、[最终矩阵](release-matrix-final-2026-09-28.md)、[流水线](release-pipeline-models-2026-09-28.md)、[混合压力](release-mixed-pressure-models-2026-09-28.md)、[矩阵复测](pressure-matrix-retest-2026-10-01.md) |
| 原版本清理与收敛 | [M11 清理](m11-capacity-cleanup-fix-2026-10-01.md)、[流水线](pipeline-convergence-2026-10-01.md)、[内核](kernel-convergence-2026-10-02.md) |

历史代码量不能与本次净删行数机械相减；历史“未完成”不直接转为当前待办。当前剩余项单列在 [remaining-work.md](remaining-work.md)。

最新全量 MTR（E110，2026-10-08）：no-bin 604 通过／295 条件跳过，log-bin 608 通过／291 条件跳过，均零失败；899 个不同用例及 18 个 big test 全部在适用模式通过。只补齐 19 个旧用例的配置／清理前提，内核未改，原失败和串行复验记录保留。见 [E110 报告](../../../build-debug/preserve-full-big-e110/README.md)及任务跟踪 E110。
