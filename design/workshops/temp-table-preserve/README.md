# 临时表与待 FETCH 结果 Preserve/Resume

更新：2026-10-04。当前目录 `/Users/a1234/project/mysql-server-8022-preserve-port`，分支 `ha_preserve_trx`；基线 `e6715a345881` 加未提交改动。当前范围与源码已按 PS transfer 删除重新核对，未经用户指令不提交或 push。

## 当前设计入口

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
| 删除哪些代码、关联 API、身份与失败语义 | [PS transfer 删除与 cursor 关联](ps-transfer-removal-and-cursor-attach.md) |
| TEMP 增量与结果文件复用 | [增量捕获和传输](incremental-capture-and-transfer.md) |
| 当前真实文件和共享薄接点 | [源码分工](source-layout.md) |
| READY、RESUME、外部回放的成本边界 | [安装与关联开销](resume-install-optimization.md) |
| 实施状态、证据与待办 | [任务跟踪](task-tracker.md)、[剩余工作](remaining-work.md) |

## 当前验证状态

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
