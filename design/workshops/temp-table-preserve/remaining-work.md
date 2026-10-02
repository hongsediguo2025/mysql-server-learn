# 当前剩余工作

更新：2026-10-04；依据 `e6715a345881` 加未提交代码、E62/E63/E64。此页替换早期 R1–R6 的缺口快照，不把已删除的 PS 本体能力重新列为开发任务。

| 项目 | 当前事实 | 下一步完成条件 |
| --- | --- | --- |
| TEMP 与独立结果主链 | 捕获、DATA/undo BASE＋DELTA、原 worker、receiver 提前准备及 final 复用已经接通 | 保持 M1–M12、OFF 和失败/取消回归，针对实际失败定位，不重复从零开发 |
| PS 本体和旧 W07/W08 | PS SQL/参数/历史解析/依赖/factory/rebuild 已删除；外部回放承接 | 不补回旧代码；验证真实回放后传入正确源记录/目标 PS |
| cursor 关联接口 | 已实现 ATTACHED/NO_CURSOR/ALREADY_ATTACHED/ERROR 和 owner 清理，本地测试通过 | 外部按 RESUME→回放→attach→CLOSE→业务接线，验证错 ID/错会话/旧生命周期、关闭/换代、失败不放行 |
| 全量 MTR | E63 当时快照含 big-test，883 项通过并集、18 个不同 big test 实际通过，无失败 | 后来五个压力驱动适配，部分为 MTR 依赖，适配后未重跑整套；内核和用例文件未变，不能说 E63 从未跑，也不能说所有当前输入已全量复验 |
| 原模型性能 | E64 原 11 模型 3 通过、8 未通过 | 分开定位精确尾部、T0 后命令排空、TPC-C 新增 1205、连续 Phase 1 TPS 时钟映射无效；不改原规模/预算/SLO |
| 临时资源性能 | 62 配置全部通过原场景断言；不等于统一性能通过 | M11 限速两档 strict 超 2秒；分析 READY 观测区间及 M10 准备重试/M2 fallback，按同钟指标复核瓶颈 |
| 物理工程集成 V04 | 三个既有在线升主接点保持，当前本地无法访问外部工程 | 验证物理 redo 下 ID/allocator/temp pool/undo 保活、已有临时表隔离、升主后原生 DML/ROLLBACK/FETCH |
| Proxy/外部回放 V05 | 现有 session/PS 回放能力为确认前提；本地 helper 不代替真实集成 | 前端不断、特殊错误码、原 ID 映射、CLOSE 留存与优先补发、RESUME 失败断前后端 |
| 连续迁移 V06 | 本地单次恢复/结果生命周期有覆盖 | 真实部署验证再次 preserve/迁移时无 pending 结果丢失，生命周期和 owner/额度正确 |

一次运行只证明本轮配置，不构成多轮统计验收。tiered 原 legacy 指标通过，但 EXACT 尾部 627.883 ms 仍超 500 ms；1000 sysbench 读写 strict 526.657 ms、精确尾 330.834 ms 均通过，其余模型不得借用该成绩。

历史 R1/R2/R3/R4 的 owner 路由、原生提前准备、DATA 增量、final 复用和源对象生命周期继续由现有 TEMP/结果实现维护；当前不再用早期“全部尚未完成”的表述。R5 的 PS 依赖证明已撤下；R6 的回放/关联、完整命令和外部责任按本页及[详细设计](detailed-design.md)处理。

证据入口：[任务跟踪 E63/E64](task-tracker.md)、[完整压力报告](../../../build-release/original-pressure-ps-removed-20261004/RESULTS.md)、[资源 62 项](../../../build-release/all-pressure-20261004/RESULTS.md)。外部环境不可访问是验收边界，不是缺少既有 session/PS 能力。本文不提出新的线程池、升主阶段、local startup 或 RESET DRAIN 工作。
