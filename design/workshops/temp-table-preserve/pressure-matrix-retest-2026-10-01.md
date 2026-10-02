# 2026-10-01 压测矩阵复测

> **2026-10-04 版本说明：历史版本记录。** 文内“当前/尚未完成/通过”和源码路径均属于记录时点；旧 PS 定义/参数/重建方案不再适用，原失败与测量不改写为新版本成绩。 现行职责见[详细设计](detailed-design.md)和[PS 删除与 cursor 关联](ps-transfer-removal-and-cursor-attach.md)，最新状态见[任务跟踪 E63/E64](task-tracker.md)。下文保留历史事实，不作为现行实施清单。

本轮按用户“全部跑起来”的要求，在当前 `ha_preserve_trx` 未提交工作区执行。
本机可承载负载已结束：临时资源矩阵 158 轮中 152 轮通过原独立验收，两轮 500 连接容量重复均通过。
原 full 的 10 个模型（计划 38 轮）全部停在空间预检，未进入业务负载；TPC-C 原种子也因空间不足未恢复。

## 固定输入与执行方式

- Release mysqld SHA-256：`424e09ba3b8ee0aba637f5e5b71544e773daa282b966bb1f410248ae188ab994`。
- 本机 16 GiB 内存；一次只运行一个负载，独立双实例，保留日志后清理本轮数据目录。
- 临时资源矩阵冻结 235 项二进制、专用源码、驱动及 Python 依赖；这不是完整源码快照。
- 沿用历史矩阵的用例名称、顺序、规模、重复次数、预算和断言。三个实例驱动只改工作目录。
  协议观测 helper 使用当前兼容版本并纳入冻结，不能声称所有 helper 与历史版本逐字节相同。
- 不做栈采样，不并行构建或运行其他压力；不以改大预算、缩短业务窗口或放宽性能门槛取得通过。
- 普通功能／性能验收失败保留并继续下一独立用例；输入变化、磁盘保护或实例清理异常停止队列核查。

## 执行清单

| 组 | 内容 | 计划 |
| --- | --- | ---: |
| M1／M3／M4／M9 | OFF/ON 正常业务、会话数、16/64/128 MiB 密集更新、结果副本、FETCH 前缀与落盘 | 44 轮 |
| M7／M8／M5／M6／M10 | DATA/undo 增量、owner 路由、大小任务公平性、多对象、PS 换代；包括 6 轮无业务 ACK 暂停 | 60 轮 |
| M2／M11／M12 | 混合业务、CPU/IO/网络/额度/期限负例、receiver 原只读临时资源并存 | 54 轮 |
| 原全量 sysbench | write-only 事务／自动提交，以及 read-write；1000 连接、原 300 秒业务窗口 | 每模型原 5 轮 |
| 原混合事务 | `dependency-mixed-transfer` | 原 full 配置 |
| 原连续大事务 | range-10000／1000／100000，各自独立运行，另列 no-commit | 每模型原 5 轮 |
| 原 LOCKSET／分层负载 | `transfer-phase2`／`continuous-tiered-transfer` | 原 full 配置 |
| 500 连接容量重复 | 沿用上轮 4 GiB、60 秒业务及原 8 GiB 磁盘预检的容量配置，再跑两轮 | 2 轮；与标准 full 分开 |
| TPC-C | 300 仓／1000 连接 | 等待足够空间恢复原离线种子 |

原 full 配置的 20 GiB 磁盘预检、混合模型的 25 GiB 预检保留。调度器会逐模型保存实际预检结果；
预检失败不算负载运行，更不能计作性能通过。500 连接配置的磁盘门槛来自此前容量实验，
不用于替换原 full 门槛。

## 空间与外部边界

启动前剩余空间约 15 GiB。TPC-C 原种子缺少五张已经归档的大表，归档和种子 manifest 匹配；
恢复净需约 23.322 GiB，恢复后保留 full 的 20 GiB 预检余量，恢复前至少需要约 43.323 GiB，
并应另留运行增长空间。当前不展开不完整种子，也不减少仓数。已向用户询问可用本地磁盘／清理目录。

本任务旧观测日志、已结束 MTR 现场和历史二进制可无损压缩的总空间不足以弥补该缺口。
压力运行中不并行压缩，以免干扰 IO 和 CPU 指标；保留原 TPC-C 种子归档。

真实物理复制、在线升主及 proxy 联调工程当前不可访问。上述临时资源矩阵验证本地源端→传输→receiver READY；
不能将它写成物理升主／SQL RESUME／proxy 全链路验收。

## 证据与最终判定

证据总目录：`build-release/all-pressure-20261001/`。

- `queue.json`、`status.json`、`results.json`：158 轮临时资源计划、当前进度和运行结果。
- `original-queue.json`、`original-status.json`、`original-results.json`：原 full 模型与独立容量重复。
- `frozen-inputs.json`、`original-driver-inputs.json`：本轮输入身份。
- 三组原始数据分别在 `build-release/temp-{matrix,pipeline,pressure}-retest-20261001/`。

运行返回成功只是一层判断。44 轮还需执行 `verify_matrix.py`，核对精确配置、survivor、READY、
捕获失败、final 复用、FETCH 位置、落盘证据和观测完整性。60 轮另外检查 case 集合、观测器、
业务命令、undo 路由退休及无人工 ACK 暂停的窗口。54 轮须保留 `validation_errors`、逐 token
拒绝原因、RESOURCE_EXHAUSTED 和实际 IO 竞争证据，不沿用历史排除名单。

DRAIN 墙钟、严格 Phase 2、命令结束至 FINAL ACK、ACK／DRAIN 后至 READY、receiver 准备累计时间分别报告。
账本高水位增量不等于绝对峰值；进程 RSS 采样、CPU TIME 差值及实时 `pcpu` 也不混用。
本轮没有修改内核、提交或推送。

## 本轮结果

三组用例集合、参数、观测、清理和输入身份已独立核验；没有排除缺报或失败样本。
235 项矩阵输入与 21 项原压力／Lua／sysbench 输入未变。各用例的成功按原合同判断，
**不等于所有场景都达到统一的 2 秒 Phase 2 或瞬时 READY 目标。**

| 模型 | 实跑 | 原验收通过 | READY | 预期 NOT_READY |
| --- | ---: | ---: | ---: | ---: |
| M1 | 12 | 12 | 24 | 0 |
| M2 | 12 | 12 | 93 | 0 |
| M3 | 9 | 9 | 123 | 0 |
| M4 | 9 | 9 | 9 | 0 |
| M5 | 12 | 12 | 234 | 0 |
| M6 | 9 | 9 | 9 | 0 |
| M7 | 15 | 15 | 15 | 0 |
| M8 | 12 | 12 | 126 | 0 |
| M9 | 14 | 14 | 14 | 0 |
| M10 | 12 | 12 | 48 | 0 |
| M11 | 30 | 24 | 84 | 12 |
| M12 | 12 | 12 | 72 | 0 |

矩阵共观测到 851 个 READY、12 个预期 NOT_READY，cursor capture failure 增量为 0。
所有实例与本轮临时数据目录均已清理。原始逐轮结果：
[完整指标表](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/all-pressure-20261001/RESULTS.md)、
[结构化指标](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/all-pressure-20261001/summary.json)、
[流水线字节与阶段明细](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-pipeline-retest-20261001/RAW-SUMMARY.md)。

### 500 连接重复

两轮均为 500 个原连接、576500 条 PS、500 SURVIVOR、500 READY、0 NOT_READY，
没有重连，保持原连接至 READY 后才停止 sysbench。沿用 4 GiB／60 秒容量配置，不能替代 2 GiB／300 秒的原 full。

| 轮次 | Phase 1→purge 请求 s | strict Phase 2 ms | 末命令→ACK ms | ACK→READY ms | 稳态 TPS | DRAIN 活跃期 TPS 降幅 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| capacity500-repeat2 | 77.693 | 301.722 | 257.403 | 245.788 | 1719.16 | 28.57% |
| capacity500-repeat3 | 64.640 | 258.492 | 193.230 | 210.928 | 1797.69 | 26.76% |

这两轮满足该容量配置的严格 Phase 2 ≤2秒、末命令→ACK ≤500ms、ACK→READY ≤500ms。
但 Phase 1 仍需 64.64–77.69 秒，DRAIN 活跃期吞吐仍下降约27–29%；不能写成整体性能问题已经消失。
[容量计量明细](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/all-pressure-20261001/capacity-summary.json)包含账本、进程 RSS／footprint、CPU、换页及流水线计数。

### 仍未通过的功能与性能点

1. **M11 低内存额度：3/3 失败。** 原 owner 的 SELECT／ROLLBACK 成功，但清理观测期结束仍有
   2 个 inflight token、169654 B inflight 和 2460 B 计费内存。与历史 E39 的同槽 uncertain 帧阻挡 ABORT 缺口一致；
   active_epochs、queue 和 worker 为 0 不能证明 OPEN 上传 owner 已回收。
2. **M11 低 inflight 额度：3/3 失败。** receiver 数据资源已归零，源端仍为 COMMIT_UNKNOWN，
   原连接的后续 SELECT 收到 4020。4020 是正确隔离；不能把它当成 receiver 内存泄漏，或只放宽断言称通过。
   当前代码仍先 COMMIT_ADMITTED，再等待前序 apply；等待失败提前返回，ABANDON／reaper 不能推进到 NOT_COMMITTED_CLEAN。
   见 [准入和等待顺序](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:21727)、
   [源端保留隔离](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx.cc:21624)与
   [历史 E39](/Users/a1234/project/mysql-server-8022-preserve-port/design/workshops/temp-table-preserve/task-tracker.md:662)。不新增 RESET DRAIN 行为。
3. **带宽受限时仍超出 2 秒目标。** M11 的 8 MiB/s 三轮 strict Phase 2 为 8.554–8.897 秒，
   2 MiB/s 三轮为 30.946–31.010 秒。六轮通过原功能断言，但不能计作 2 秒性能通过。
4. **receiver 尾部仍有开销。** BUSINESS_FIRST 三轮 DRAIN→READY 为 3.988–4.509 秒；
   无人工业务 ACK 暂停的 M10 六轮尾部为 384.062–799.025ms，receiver final 墙钟为 515.028–851.350ms。
   冷态 128 MiB 密集更新等大对象场景也有秒级 READY 尾部，需与 source strict Phase 2 分开看。

两种额度失败是历史已登记缺口的重现，没有证据将它们认定为本轮 PS 裁剪引入的新回归。本轮未修改内核。

后续修复：上述两项容量收尾缺口已在E53完成内核修正，原六轮复测6/6通过，另有真实丢ACK
两轮和11项定向业务MTR通过；见[根因与最终证据](m11-capacity-cleanup-fix-2026-10-01.md)。
本报告保留原158轮的历史结果，后续定向修复不等于重新跑过全矩阵；以下性能缺口仍保留。

### 限速场景的耗时定位

对 `m11-net8-r1` 与 `m11-net2-r1` 结合服务端日志、传输帧与源码复核：

| 指标 | 8 MiB/s | 2 MiB/s |
| --- | ---: | ---: |
| strict Phase 2 | 8.896756 s | 31.009886 s |
| target worker 墙钟 | 8.873477 s | 30.971197 s |
| commit_epoch 调用 | 21.245 ms | 33.692 ms |
| relay 收到 COMMIT ACK→观察到 READY | 283.350 ms | 313.781 ms |

这些区间存在嵌套，不能相加。两轮都没有待结束的合格业务 body，`target_wait_us=0`；
`last_command_end_to_final_ack_us=0` 在这里是不可用值，不能当作零延迟。

初次 BEGIN 批次结束后、最终 BEGIN 之前，两轮都补传 **43,794,492 B CHUNK payload（41.7657 MiB）**，
共676个CHUNK：image 41,943,040 B、undo 1,508,676 B、cursor result 338,284 B、PS描述4,492 B。
数据来自逐帧求和，不是限速乘时间；也不等于完整线上报文字节。四个 owner 的对象组顺序发送，
观测区间没有交叠。最终 BEGIN 之后仅剩34,936 B snapshot payload、13个frame。
因此本轮主要长段是 final 捕获后的资源补传；提交和后续 READY 尾部不能解释8–31秒的严格 Phase 2。
补传时间包含读数据、限速、发送和ACK等待，现有证据不能将其全部认定为纯网络耗时。

源码与事件相符：closing 时[关闭 Phase 1 计量](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx.cc:21024)，
之后[逐个 stage candidate](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx.cc:22348)，
内部[同步发送 TEMP 与 PS 资源](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:14524)。
已有 `source_phase2_transfer_bulk_bytes=0` 是计量遗漏：finalize bundle 的计数路径
[跳过此前已 seal 的对象](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:13682)，
不能据此判定 Phase 2 没有大对象传输。后续须分别处理提前发送／final复用的缺口、关闭阶段顺序发送和计量覆盖；
本次未改内核，也未证明单独增加并行度就能满足限速下的2秒目标。

原始证据：两轮目录中的 `report.json`、`source-mysqld.err`，位于
`build-release/temp-pressure-retest-20261001/m11-net8-r1/` 和 `m11-net2-r1/`。

### 正常业务与 PS 换代观测

- M1 独立 steady 对照三轮：EXECUTE p99 OFF 为 1.310–1.362ms，ON 为 1.339–1.363ms，区间重叠。
  这是本机 8 会话模型，不能外推为全部规模的零开销。
- M10 无人工业务 ACK 暂停六轮：EXECUTE p99 为 1.333–1.578ms，单轮最大值为 3.908–8.174ms。
  本轮这些样本未重现此前的秒级 EXECUTE 尖峰；receiver final 尾部仍须单独优化。

### 尚未实际运行的模型与空间

原 full 的 10 个模型均有真实预检失败记录，要求 20 GiB（mixed 为25 GiB），实际约16.13 GiB；
它们没有进入负载，不能当作业务失败或已完成的38轮。TPC-C 300仓仍因恢复种子的空间不足未运行。
本轮无损压缩178个已结束实验文件，逐个验证解压SHA后才移除展开文件，释放约2.95 GiB；
保留原TPC-C归档、当前二进制和源码。恢复索引：
[空间整理清单](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/all-pressure-20261001/space-archive.json)。
压缩在两组负载之间完成，没有与性能测量并行。

本次没有提交或推送；V03 的完整规模性能验收与外部物理／proxy 验收保持开放。
