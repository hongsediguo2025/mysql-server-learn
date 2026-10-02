# 临时表与待 FETCH 结果：Release 基线压测（2026-09-28）

> **2026-10-04 版本说明：历史版本记录。** 文内“当前/尚未完成/通过”和源码路径均属于记录时点；旧 PS 定义/参数/重建方案不再适用，原失败与测量不改写为新版本成绩。 现行职责见[详细设计](detailed-design.md)和[PS 删除与 cursor 关联](ps-transfer-removal-and-cursor-attach.md)，最新状态见[任务跟踪 E63/E64](task-tracker.md)。下文保留历史事实，不作为现行实施清单。

> 本文保留修复前的历史基线与失败。后续已完成[内核修复](cursor-fix-2026-09-28.md)及[最终版本完整矩阵44/44复测](release-matrix-final-2026-09-28.md)；不要把下文21轮历史失败或当时待办读成当前未修复数量。

已完成 62 个正式及补充样本：41 通过，21 失败。另有 4 次预检，单独留档。

**本轮没有全部通过，也不构成商业性能验收。** 密集临时表修改触发当前逐行历史标记容量；正常业务开销和 READY 时延已取得可检查样本。未改内核、未提交。

## 试验身份与范围

- 当前 `ha_preserve_trx` 未提交源码，重新构建 `build-release/runtime_output_directory/mysqld`；HEAD、内核 diff、二进制和各轮脚本 SHA256 均归档。
- 16 GiB 内存、10 CPU；source/receiver 在同一 macOS 主机，loopback TCP。每轮专用全新 datadir，顺序运行；新 datadir 不等于 OS 冷缓存。
- 两端 BALANCED，GTID ON / ROW binlog ON；buffer pool 各 512 MiB，Preserve heap 各 1 GiB，transfer inflight 1 GiB；流水线 6 worker，receiver profile 准备 worker 3。
- 保留原 10 秒 receiver prewarm 期限。客户端 120 秒与编排 360 秒保护只限定测试等待，不延长内核期限。
- 用户表均为 InnoDB；仍以完整命令为边界。没有 DEBUG_SYNC、DBUG、RESET DRAIN、客户端重建结果、物理升主模拟。
- 只测正常业务和 standby transfer→READY；没有测真实物理 replay/升主、SQL RESUME、恢复后 FETCH 或 proxy 停顿。
- 生命周期只处理本轮实例。源端 DRAIN 的既有单向 purge fence不适合正常关库；测量结束后ON专用源统一SIGKILL回收（含未DRAIN的steady组），OFF源与receiver正常关闭，再删除本轮数据目录。该动作不是恢复/切换测试。

## 原定 37 个样本

| 模型 | 负载 | 结果 |
| --- | --- | --- |
| 1 | 8 会话，2048 行×256 B；UPDATE→128行游标EXECUTE→FETCH16；5秒预热+15秒采样；OFF/ON各3次 | 6组稳态命令均采得；OFF 3/3通过，ON后续DRAIN 0/3 |
| 3 | 1/8/32会话，每会话2表×4096行×512 B；每表2次全量UPDATE及保存点回滚；各3次 | 0/9，源端历史容量拒绝 |
| 4 | 1会话1表，16/64/128 MiB payload；全量UPDATE和部分DELETE回滚；无cursor；各3次 | 0/9，源端历史容量拒绝 |
| 9 | 固定4 MiB TEMP、结果复制1/8/32倍→4/32/128 MiB；各3次；另测未FETCH、半数FETCH、BLOB/MEMORY磁盘路径 | 13/13达到READY |

模型1吞吐统计单位是完整命令/秒，不是事务/秒；每个业务连接在整个测量阶段保持一个事务。ON未DRAIN也已执行结果文件捕获及行历史跟踪，不能称为“开启但没有捕获”。

## 补充对照：保持数据规模，限制行历史数量

这些样本保留独立名称，不替换原失败样本。模型3只把updates从2改为1；模型4保持16/64/128 MiB表体积，仅修改前1024行，DELETE/回滚也限定在此前缀。每档3次。

| 对照组 | 成功/样本 | DRAIN秒（全部重复范围） | 返回后READY观测尾段秒 | receiver FINAL→READY秒 |
| --- | --- | --- | --- | --- |
| m3-oneupdate-s1 | 3/3 | 0.324–0.340 | 0.098–0.103 | 0.091–0.092 |
| m3-oneupdate-s8 | 3/3 | 1.805–1.967 | 0.171–0.354 | 0.169–0.484 |
| m3-oneupdate-s32 | 3/3 | 6.124–11.896 | 0.104–0.259 | 0.160–0.875 |
| m4-sparse-16m | 3/3 | 0.396–0.421 | 0.169–0.176 | 0.162–0.162 |
| m4-sparse-64m | 3/3 | 0.990–1.126 | 0.505–0.514 | 0.502–0.503 |
| m4-sparse-128m | 3/3 | 1.833–1.942 | 0.931–0.948 | 0.925–0.938 |

另补 8192 B BLOB 行：512源行×8倍结果，32 MiB结果payload，1/1通过。BLOB+MEMORY与MEMORY低堆表额度的各组 `Created_tmp_disk_tables=1`；这是落盘观测，不能仅由累计计数推断每个内部对象的具体引擎。

## 大结果集原始重复

| 结果payload | 三次DRAIN秒 | 三次返回后READY观测尾段秒 | 三次receiver FINAL→READY秒 |
| --- | --- | --- | --- |
| 4 MiB | 0.351469, 0.335221, 0.347823 | 0.093591, 0.101313, 0.093604 | 0.088346, 0.087843, 0.086952 |
| 32 MiB | 2.424914, 0.602515, 0.638167 | 0.048879, 0.061717, 0.060181 | 0.040324, 0.047238, 0.047363 |
| 128 MiB | 6.404473, 1.505973, 1.513437 | 0.254807, 0.259340, 0.248996 | 0.240136, 0.243301, 0.242672 |

DRAIN墙钟与返回后的READY观测尾段相加，得到客户端从发起DRAIN到观测到全部READY的总时间。轮询sleep为10ms，但查询与调度会增加观测延迟，10ms不是严格误差上界。receiver FINAL→READY是另一节点的epoch本地墙钟，与前两段重叠，不能再相加。

## 正常业务 OFF/ON

初始15秒稳态重复存在明显波动，全部保留。后来补了3组独立配对：每轮5秒预热、30秒采样，不发DRAIN，按ON/OFF、OFF/ON、ON/OFF换序。两种模式其余配置相同，只改变顶层enable；namespace等开关保持一致。

| 样本 | 成功命令/秒 | DML p99 µs | EXECUTE p99 µs | FETCH p99 µs |
| --- | --- | --- | --- | --- |
| m1-off-r1 | 7203.8 | 642 | 1444 | 3460 |
| m1-off-r2 | 7430.1 | 537 | 1333 | 3267 |
| m1-off-r3 | 6717.5 | 628 | 1501 | 3617 |
| m1-on-r1 | 6403.6 | 511 | 20659 | 2874 |
| m1-on-r2 | 5579.1 | 509 | 31283 | 2859 |
| m1-on-r3 | 7881.7 | 571 | 1830 | 3050 |
| m1-steady-off-r1 | 7273.6 | 553 | 1339 | 3262 |
| m1-steady-off-r2 | 7270.3 | 535 | 1317 | 3245 |
| m1-steady-off-r3 | 7199.7 | 621 | 1446 | 3444 |
| m1-steady-on-r1 | 7828.6 | 605 | 3033 | 3017 |
| m1-steady-on-r2 | 7459.3 | 537 | 5651 | 2941 |
| m1-steady-on-r3 | 7437.4 | 555 | 3680 | 2952 |

| 独立steady样本 | 源mysqld采样CPU中位% | 客户端采样CPU中位% |
| --- | --- | --- |
| m1-steady-off-r1 | 19.8 | 302.4 |
| m1-steady-off-r2 | 19.6 | 303.2 |
| m1-steady-off-r3 | 19.8 | 300.2 |
| m1-steady-on-r1 | 97.8 | 261.2 |
| m1-steady-on-r2 | 95.5 | 263.9 |
| m1-steady-on-r3 | 94.2 | 263.6 |

CPU为1Hz ps进程采样值的中位数，100%表示约一个逻辑CPU；采样覆盖准备/预热/测量，不是严格单条命令CPU成本。结合源端CPU与EXECUTE尾延迟判断开销，不能只以混合吞吐宣称无开销。

p99在每轮每类完整成功命令的原始样本上按nearest-rank计算，排除warmup及跨测量边界的命令。未将多轮p99平均称为整体p99。延迟包含Python协议收发/解码与本机调度；外层循环及样本记录主要影响吞吐和发起节奏，不全在单命令计时区间。闭环下不同命令的节奏会互相影响，不能把某类命令变快直接解释成内核优化。

## 失败根因与未确认项

### 源端行历史容量阻挡密集更新

源码默认每participant最多16,384条journal、1MiB tail：`sql/preserve_trx_temp_table.h:302`。成功TEMP行UPDATE/DELETE逐行追加记录（`sql/handler.cc:7897`及`:7925`）；`append_journal()`超限后进入DEGRADED（`sql/preserve_trx_temp_table.cc:1439`）。业务DML可以继续成功，但后续phase1无法arm（同文件`:2758`）。

- 模型3每owner仅UPDATE就有2×4096×2=16384条，之后还有DELETE/INSERT/savepoint，必超过记录上限。
- 模型4最低16MiB档已有16384条UPDATE，之后继续DELETE，同样必超过记录上限。
- 原模型3/4日志为`phase1_pipeline_baseline_failed`、`temp-table phase1 job preparation failed`，`temp_steps=0`，未进入TEMP复制和receiver READY；不能标成READY超时。
- 低更新量配对通过，支持上述归因。当前日志没有输出最早的具体degraded_reason，不能声称已经直接观测到“先命中条数还是1MiB”；这不影响原负载必然越界的事实。
- 模型1 ON的业务在DRAIN阶段继续积累行历史；已观察到后台round执行，最终仍失败。不能据DRAIN发起后的任意命令样本宣称已测稳态active-capture p99。

### 高频结果换代下的捕获失败

模型1三轮ON在DRAIN前cursor_capture_failures均为0，结束时分别增加74,665 / 67,568 / 53,245；前台EXECUTE/FETCH仍成功。该计数对应额度/分配/文件/编码等捕获失败，不是正常cutoff计数。原62轮证据没有失败分原因与live峰值，单靠这些旧样本不能确定原因。

**后续已补11轮独立诊断，见[根因报告](cursor-root-cause-2026-09-28.md)。** 在相同诊断负载下，失败平台随count从256移动到512，确认预传输持有历史代导致live-result份数耗尽；单变量关闭结果捕获也消除了常态EXECUTE的大部分额外成本，采样进一步指向小文件创建/读写/关闭。该结论未把旧汇总计数逐笔归因；未改内核，V03仍未关闭。

### 环境与源端服务时间波动

原M1/M9二进制与负载脚本hash一致；同一模型、同一enable取值的重复配置差异只有实例身份/路径/GTID。OFF/ON对照另有声明的enable差异。磁盘可用空间从约11.5GiB变为约31.4GiB，发生于04:38:32–04:38:52 UTC；这是观测到的环境变化，不是已证明的性能原因。

- 128MiB结果的三轮source copy逻辑读量均13,631,488B，服务耗时却为4611.863 / 8.017 / 7.084ms。
- 32MiB结果的三轮undo写入量均4,540,991B，服务耗时为389.156 / 5.627 / 8.436ms。
- 差异在源端阶段可见，receiver READY观测尾没有同幅增加；缺少细粒度系统证据，不能归因为具体锁、fsync或网络ACK。累计服务时间存在嵌套，不能直接相加当DRAIN墙钟。

## 验收边界及后续

1. V03仍未关闭：密集更新容量拒绝、高频换代捕获失败和长尾波动均需继续处理/验证，不能以缩小更新量的通过替代。
2. 每个READY规模仅3次，不足以验收p99；用户未给具体规模/SLO，因此只报告测量值。
3. 1Hz CPU/RSS/目录占用为采样值，短于1秒样本可能漏掉峰值；不是严格峰值资源证明。目录占用采样不覆盖tmpdir中的匿名内部结果文件；另存的文件系统余额也不等于本轮磁盘用量。客户端和两实例共享主机，不能外推独立物理备机吞吐。
4. 本轮只修改现有benchmark的测量参数与报告；没有调整内核容量/超时、没有新增线程池。V04–V06外部验收状态不变。

## 可复核产物

- [原始汇总](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-baseline-20260928/RAW-SUMMARY.md)、[机器汇总](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-baseline-20260928/summary.json)、[实验声明](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-baseline-20260928/PLAN.md)、[环境](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-baseline-20260928/environment.json)。
- 每轮目录位于 `/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-baseline-20260928`，包含report.json（含原始命令样本/计数）、execution.json（命令/hash/资源采样/退出码）、两端variables和错误日志。
- 预检smoke保留额度冲突、测量SQL带入MDL、DRAIN后管理账号准入和专用源关库问题的证据；最终smoke-v4通过，预检不计入正式样本。
- build.log、head.txt、kernel-before.diff、git-status-before.txt及脚本快照保存了构建和实验身份。
