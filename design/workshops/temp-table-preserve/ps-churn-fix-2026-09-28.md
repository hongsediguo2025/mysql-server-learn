# PS 换代长尾：根因修复与验证（E38）

> **2026-10-04 版本说明：历史版本记录。** 文内“当前/尚未完成/通过”和源码路径均属于记录时点；旧 PS 定义/参数/重建方案不再适用，原失败与测量不改写为新版本成绩。 现行职责见[详细设计](detailed-design.md)和[PS 删除与 cursor 关联](ps-transfer-removal-and-cursor-attach.md)，最新状态见[任务跟踪 E63/E64](task-tracker.md)。下文保留历史事实，不作为现行实施清单。

本次只修复 [E37 已证实的两条成本路径](ps-churn-root-cause-2026-09-28.md)：中等结果在每次 EXECUTE 中发生捕获文件换代；receiver 对小 DELTA 重复写完整派生镜像。工作目录仍为 preserve-port / ha_preserve_trx，不提交或推送。没有新增线程池、外部升主阶段、RESET DRAIN 或通用 session 迁移。

## 1. 为什么这样修

E37 的实际结果工件为 84,571 B，超过原 64 KiB 缓冲。在一组有效探针中，四条约 1.44 秒 EXECUTE 对应业务线程中的 cursor close/mkstemp；累计文件调用占该波次命令耗时 99.60%。迁移前也出现同类尖峰，不能全部归因于 receiver 或锁竞争。这里只认定观察到的文件 API 阻塞，不推断 APFS 内部原因。

receiver 原生跨代少写发生在 source-image 合并之后；此前每代 DATA/undo DELTA 都先展开完整匿名文件。对于 10 MiB DATA，即使 PATCH 很小，仍会支付完整写入。旧阶段和节流计数遗漏了这部分派生写，不能用旧指标的零推断没有写。

## 2. C++ 改动

| 位置 | 本次行为 | 边界 |
|---|---|---|
| `preserve_trx_cursor` | 保留 64 KiB inline，按需扩展至 128/256/512/1024 KiB，封存后通过原只读句柄发送 | 分配前计旧＋新内存峰值；额度/OOM/超过 1 MiB 时 spill；首批写成功后释放动态缓冲，后续仍按 64 KiB 流式处理 |
| `preserve_trx_temp_delta`、`preserve_trx_file` | 完整顺序、边界与 SHA 校验后，发布 BASE/PATCH 只读组合视图，支持任意偏移跨块读取 | 未覆盖部分始终读固定 BASE；canonical shared_ptr 与索引 lease 活到最后读者；不能跳过 O(data) 完整校验 |
| `preserve_trx_temp_import`、`temp_receiver`、`receiver_candidates` | 索引资源不足时沿用派生文件回退；把成功派生写入纳入阶段指标和共享 worker 读写节流 | 目标 owner 换代时保留本代累计写入；失败批次已完成的读写仍计量；undo read 指标不混入 write |

结果编码、摘要、稀疏索引、PS ID、FETCH 位置、EOF 和传输格式均不变。已知一次追加超过上限的大 BLOB 直接 spill，避免先扩容再释放。全局额度和原 workload 的结果数量限制未放大。

组合视图减少的是派生文件创建、写入及关闭，不代表整个 receiver 准备变成 O(delta)。完整认证、页转换、字典/undo/统计核验仍保留；大结果和内存回退仍可能遇到文件系统长尾。

## 3. 功能证据与发现

证据目录：`build-debug/ps-churn-fix-20260928/`。`before/`、`this-fix.patch` 用于区分本次修改与已有未提交大需求。patch 仅覆盖有 before 快照的文件，不能单独重放；其余 header、MTR include、新测试及完整最终文件保存在 `final-source/`，范围说明见 `PATCH-SCOPE.md`。`final-inputs.json` 保存最终源文件及二进制摘要。

- 旧 Debug 的 storage 用例在 65,536 B 投影处实际创建 1 个捕获文件，新断言期望 0，稳定 RED。双实例 native cross 在下一代出现文件数 `[4,6]`，证明 receiver 仍生成派生文件。早先 loopback `[6,10]` 混入源端文件，不作为 receiver 单独证据。
- 新 storage 用例覆盖 84 KiB 宽结果、64 KiB/1 MiB 边界、索引 spill、重复 EXECUTE、逐批 FETCH/EOF 和释放归零。真实预算分别拒绝首次 128 KiB 扩容，以及只拒绝 128＋256 KiB 同时存在的峰值；均要求 spill 后内容正确、捕获失败不增加。
- DATA-only 与 undo 派生回退分别用内部 DBUG 分配拒绝验证，包含阶段写入计量。正常双实例路径要求换代不增加派生临时文件，source 阶段派生写入为零；DATA-only probe 避免仅 undo 写入造成伪阳性。没有使用 DEBUG_SYNC 或新增 UT。
- 首轮 storage_restore 的新预算子段把额外 Debug 恢复探针也限制住，导致断言。捕获的三个预算断言仍在低额度下执行；仅在内部 FETCH 恢复探针前恢复额度，之后验证内容和释放。保留失败日志与修正后通过日志。
- 扩展回归中，ps_pretransfer_abandon 曾在 181 批、144,769,024 B 持续处理后触发固定 10 秒 PREWARM_DEADLINE，未报告 CORRUPT。审核发现既有 PS-only 故障注入也会抛弃 TEMP candidate，已补 `!job.temp_candidate`，只影响 Debug。未延长内核或 Python 超时。初轮同时有并行 MTR 和构建，后续为独立串行；不能把复跑通过反推为超时只由探针误伤造成。

最终 **38 个不同 Debug 定向业务均有通过记录**，shutdown_report 单列；不是全量 Preserve/Resume 回归。扩展 log-bin 轮为29业务通过、1业务失败、3跳过；失败的 PS abandon 修正 probe 后独立串行3次通过。三项 no-bin 在显式 `--mysqld=--skip-log-bin` 后全部通过。最终 storage、storage_restore、DATA-only spill、undo spill 也分别复验通过。Release 另有 storage（含大结果及两档真实预算）、OFF、local-isolation 三业务通过。

所有失败/跳过及补测记录保留在 `mtr-outcomes.json` 与各轮日志中，不能将初轮日志改写为全绿。独占 vardir 已移除，日志/config 保存在 `retained-vardirs/`。

跨实例用例止于 READY；SQL RESUME/FETCH/回滚由本地桥接用例验证。它们不替代外部物理在线升主或真实 proxy 的验收。

## 4. Release 复测

原宽结果＋DML、4 owners、每 owner 4/16 PS、TEMP 4096×512 B、三段闭环业务窗口、无业务 ACK 人工暂停均保持不变；期间不并行构建或 MTR。两份 workload 及5个实际导入的公共脚本与 E37 完全相同；预算、结果限制和配置不变。98项冻结输入在运行后全部一致。

共15次启动尝试：**12轮有效**（新版9、旧版3），48/48业务事务READY，捕获失败0，业务命令与人工ACK暂停重叠0。初次复制旧二进制遗漏 `@loader_path` 的 protobuf 动态库，3次初始化即失败、未进入业务；保留并排除。补上指向同一依赖库的链接后，完成3组旧→新配对；另有最初新版 ps4/ps16 各3轮。所有15次独占数据目录均已清理。

| 同一本机/负载 | 有效轮数 | EXECUTE p99（ms） | 最大 EXECUTE（ms） | READY 观测尾（ms） |
|---|---:|---:|---:|---:|
| 旧版，16 PS/owner | 3 | 2.389–7.546 | 886.994 | 326.293–367.808 |
| 新版，16 PS/owner | 6 | 1.482–1.598 | 22.706 | 280.872–467.352 |
| 新版，4 PS/owner | 3 | 1.609–1.681 | 5.726 | 308.070–450.627 |

配对3轮的 p99 分别改善约38.0%/47.3%/80.2%；这里使用本轮旧版分母，不挑选 E37 较慢样本夸大收益。旧版本轮并非每轮都秒级卡顿，但仍复现887ms尖峰，说明旧文件路径的长尾具有波动性。新9轮未见这类尖峰，不代表大结果/低内存spill的OS长尾永远消失。

确认消除的工作量：

- 每份仍为84,571 B的结果工件。源端业务窗口全局文件创建/EXECUTE近似比，ps16从旧版约1.027降至新版0.0245–0.0252；余量含后台文件，不能把它当作cursor专属计数。直接“该结果不建捕获文件”的证据来自MTR。
- 新版全部样本的 receiver SOURCE/独立UNDO派生写入均为0，字段存在性也有断言。旧版该计数漏记，因此表中旧派生写为未知，不能对其做数值零对照。`image_delta_assembled` 仍表示完整逻辑内容校验完成次数；新路径不是全镜像写入次数。
- 结果、FETCH前缀、receiver既有只读TEMP及未来分配的断言通过。原协议及数据规模未缩窄；闭环变快意味着同一时长内执行更多业务，不是固定到达率压测。

**READY边界没有因这次修复被宣布完成。** 三组配对中，旧→新观测尾分别为367.808→467.352、344.627→413.183、326.293→341.316ms。前两组新版final候选精确复用为0、旧版为1，final逻辑处理量由约97.37MB增至129.83MB；第三组双方复用1。普通轮更快不保证最后一次DML后的候选精确命中，不能据此声明READY或整体迁移更快。此处READY尾是客户端观察READY时间减DRAIN返回时间；DRAIN含控制用命令窗口，均不是物理升主/SQL RESUME耗时。服务端final-spool-ACK到READY指标也保留于原始report。

内存代价也保留：ps16旧版源端峰值27.39–28.37MiB、新版42.28–43.61MiB；receiver旧版144.90–179.16MiB、新版196.43–205.29MiB。它们是新实例的high-water值，包含更高闭环吞吐与更多在途状态，并非单个缓冲大小；均受原1GiB全局预算约束，不用after-before冒充峰值。

原始证据：[逐轮汇总](../../../build-release/ps-churn-fix-20260928/ACCEPTANCE-SUMMARY.md)、[机器汇总](../../../build-release/ps-churn-fix-20260928/acceptance-summary.json)、[验收计算](../../../build-release/ps-churn-fix-20260928/analyze_acceptance.py)。逐轮metadata记录旧/新二进制SHA、服务器命令、workload SHA、退出和清理状态；初始化失败也在尝试表内。

## 5. 交付边界

这两条确认的成本路径已修复并完成本地功能和原负载验证；不会把共享MAC上的测量推广为商用SLO。全量 no-bin/log-bin/transfer-stby 回归尚未在此版本重跑，外部物理在线升主/真实proxy集成仍归V04–V06，READY精确复用和全体规模性能继续按V03跟踪。未提交或推送。
