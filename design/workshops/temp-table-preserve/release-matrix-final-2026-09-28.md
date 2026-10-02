# 最终 Release：模型 1、3、4、9 完整矩阵复测（2026-09-28）

> **2026-10-04 版本说明：历史版本记录。** 文内“当前/尚未完成/通过”和源码路径均属于记录时点；旧 PS 定义/参数/重建方案不再适用，原失败与测量不改写为新版本成绩。 现行职责见[详细设计](detailed-design.md)和[PS 删除与 cursor 关联](ps-transfer-removal-and-cursor-attach.md)，最新状态见[任务跟踪 E63/E64](task-tracker.md)。下文保留历史事实，不作为现行实施清单。

**44/44 轮通过严格验收。** 已在 E34 最终修复二进制上完整重跑原定 37 轮，追加大 BLOB 1 轮及顶层 OFF/ON 常态对照 6 轮。此前“最终版本下原矩阵尚未补齐”这一缺口已关闭；本轮没有修改内核、调整额度或超时，也没有提交。

35 轮实际 standby transfer 共 **170/170 个业务事务 READY**；观测到的结果捕获失败为 0，final DATA 基线复用计数合计 170、全量回退 0。其余 9 轮是无需 DRAIN 的业务对照。

## 1. 样本身份与覆盖

- Release SHA256：`23e6212c81f2449db1bd18b89b72f2c879ba2376ea6631bfb1f0cc0e0d1e1753`，与 E34 完全一致。原 62 轮的旧二进制及其失败记录保持原样。
- 全部为本轮新跑样本；没有借用此前 capture-only 诊断代替顶层 OFF/ON，也没有用减少 UPDATE 或稀疏修改的对照替代密集负载。
- 每轮独立新建 source/receiver，顺序运行；同一台 macOS、loopback TCP，Python 3.9.6。所有实例都保持 GTID ON、ROW binlog、BALANCED、6 个流水线 worker、各 512 MiB buffer pool；Preserve heap 和 transfer inflight 各 1 GiB。
- result capture count=256、bytes=1 GiB，原 receiver 10 秒 prewarm 期限保持不变。顶层 OFF 组只关闭源 preserve enable，TEMP/capture 配置仍 ON，实际捕获次数核验为 0。
- 每轮前后重算固定二进制、E34 修复源文件、全部当前 scripts/*.py 依赖、负载及矩阵；61 项冻结输入均未变化。两端真实变量、运行命令和负载报告逐项验收。

| 模型 | 本轮完整覆盖 | 通过 |
| --- | --- | ---: |
| 1 | 8 会话×2048 行×256 B；UPDATE→128行游标 EXECUTE→FETCH16。5秒预热＋15秒测量，顶层 OFF/ON 各3轮，ON 后 DRAIN；另5秒＋30秒、无DRAIN的 OFF/ON 各3轮 | 12/12 |
| 3 | 1/8/32会话，各2表×4096行×512 B；每表完整UPDATE两次及原保存点、DELETE、INSERT、回滚；每档3轮 | 9/9 |
| 4 | 16/64/128 MiB表payload，各16384/65536/131072行；完整UPDATE及部分DELETE回滚，无cursor；每档3轮 | 9/9 |
| 9 | 4 MiB TEMP，结果复制1/8/32倍，4/32/128 MiB结果各3轮；未FETCH、半数FETCH、BLOB/MEMORY落盘各1轮；8192 B值的大BLOB另1轮 | 14/14 |

模型9检查实际FETCH返回行数及由总量减去已FETCH数计算的剩余行数，并检查落盘组 `Created_tmp_disk_tables > 0`；后者不唯一识别每个结果对象的存储引擎。READY模型同时验证 receiver 已有TEMP的数据、回滚和后续新建TEMP仍正常；本轮没有连接物理备机工程或执行恢复后的 FETCH。

## 2. DRAIN 与 READY 规模结果

以下是每档全部3轮的最小–最大值；较慢首轮没有被丢弃。DRAIN 是客户端墙钟，READY尾段从DRAIN返回开始计时。receiver FINAL→READY来自receiver的独立阶段墙钟，与前两段重叠，不能再相加。

| 场景 | 通过 | DRAIN 秒 | 返回后观测READY 秒 | receiver FINAL→READY 秒 |
| --- | ---: | ---: | ---: | ---: |
| 8会话连续业务 | 3/3 | 1.843–1.904 | 0.451–0.471 | 0.475–0.489 |
| 密集更新，1会话 | 3/3 | 0.334–0.339 | 0.100–0.127 | 0.095–0.124 |
| 密集更新，8会话 | 3/3 | 1.610–1.663 | 0.273–0.312 | 0.279–0.311 |
| 密集更新，32会话 | 3/3 | 6.127–9.522 | 0.143–0.442 | 0.267–0.947 |
| 密集更新，16 MiB | 3/3 | 0.568–0.602 | 0.198–0.202 | 0.185–0.192 |
| 密集更新，64 MiB | 3/3 | 1.799–5.890 | 0.580–1.079 | 0.582–1.082 |
| 密集更新，128 MiB | 3/3 | 3.340–6.756 | 1.117–1.347 | 1.118–1.344 |
| 保留结果，4 MiB | 3/3 | 0.337–0.360 | 0.089–0.101 | 0.087–0.087 |
| 保留结果，32 MiB | 3/3 | 0.600–0.615 | 0.053–0.058 | 0.047–0.048 |
| 保留结果，128 MiB | 3/3 | 1.511–2.230 | 0.014–0.250 | 0.002–0.242 |

模型9的未FETCH、半数FETCH及两种落盘路径，DRAIN为0.610–0.665秒，返回后READY为0.056–0.058秒；大BLOB为0.670秒＋0.168秒。每轮原值见[完整结果表](../../../build-release/temp-matrix-final-20260928/RAW-SUMMARY.md)。

128 MiB结果的第3轮DRAIN较长、返回后READY较短；READY工作在DRAIN返回前后如何分布会影响尾段，不能仅拿0.014秒尾段宣称完整准备成本降至0.014秒。该轮DRAIN至观测READY合计2.244秒。

源Preserve计费峰值最大278.961 MiB，receiver最大415.868 MiB，均未用扩容预算换取通过。这是Preserve账本峰值，**不是mysqld总RSS**；进程RSS另外按约1秒间隔采样并留档。final_reused底层按首次处理的source space计数，本矩阵每owner的用户TEMP表共享一个空间，因此170也等于业务owner数。

## 3. 常态业务：顶层 OFF/ON

以下6轮按 ON/OFF、OFF/ON、ON/OFF 顺序配对，每轮5秒预热、30秒采样、无需DRAIN；与 E34 的仅切 capture 对照分开。统计单位为完整成功命令/秒，每个会话在测量期间保持同一事务。

| 轮次 | 命令/秒 | DML p99 ms | EXECUTE p99 ms | FETCH p99 ms | 源CPU % |
| --- | ---: | ---: | ---: | ---: | ---: |
| OFF r1 | 7269.0 | 0.533 | 1.314 | 3.271 | 20.4 |
| ON r1 | 7263.3 | 0.561 | 1.382 | 3.302 | 27.6 |
| OFF r2 | 7162.8 | 0.542 | 1.339 | 3.297 | 20.1 |
| ON r2 | 7239.2 | 0.548 | 1.363 | 3.295 | 27.4 |
| OFF r3 | 7243.5 | 0.605 | 1.426 | 3.465 | 22.6 |
| ON r3 | 7311.7 | 0.550 | 1.381 | 3.317 | 27.9 |

ON EXECUTE p99为1.363–1.382 ms，OFF为1.314–1.426 ms；ON小结果每次EXECUTE的新建临时文件为0。ON源CPU为27.4–27.9%，OFF为20.1–22.6%，仍有完整扫描、编码和跟踪成本，不能说“零开销”。三组吞吐接近；样本较少且为本机闭环，不据差异宣称性能提升。

## 4. 验收方法与边界

- `verify_matrix.py`核对44个唯一用例、实际工作负载/命令参数、双方开关/额度/profile/worker/期限、原值捕获次数、采样期间失败计数、预期业务survivor与READY数量、final复用，以及进程退出和本轮数据清理；最终退出码0。
- 结果捕获失败为0，auto_prewarm_not_ready为0；这两个结论不等于所有名称带failed的内部诊断都为0。其他候选诊断按下节单列。
- 本轮负载脚本与E34相同；观察线程每50ms查询状态、约500ms查询handler，与最早无此线程的旧baseline整体观察开销不同。与旧版比较时只承诺相同业务参数，未声称整套观察负载字节相同。
- CPU以业务窗内首末进程累计CPU时间差计算，100%约一个逻辑CPU；实际窗口约28.4秒。macOS Python3.9的monotonic在不同进程有不同起点，故跨进程窗口按UNIX锚点对应，同runner分母使用monotonic差；采样中两钟偏移的最大变化约130.3微秒。
- p99为每轮单类完整成功命令的nearest-rank统计，包含协议解码和本机调度；没有平均多轮p99充当合并p99。READY轮询和1秒资源采样会带入调度误差或漏掉短峰值；business结束计数也不是所有worker退出瞬间的原子快照。
- 每档3轮能补齐既定矩阵，不能认证规模READY的p99或生产SLO。source/receiver同机运行，不能外推独立物理备机容量、在线升主或瞬时SQL RESUME。较慢首轮和运行中磁盘可用空间变化均保留，不无证据归因到锁、网络或fsync。
- 44轮的专用实例都已退出，数据/socket目录按所有权清理；原始日志/命令样本保留。源ON实例测量结束后SIGKILL用于销毁既有单向purge fence实例，不是崩溃恢复验收。实际测量时磁盘可用空间最低约8.59 GiB，未触发4 GiB保护。

### 4.1 不能混为业务失败的候选诊断

独立复核44份报告、8370条状态样本及308份err/stdout/benchmark日志，没有发现被success掩盖的ERROR、崩溃或断言；35条receiver FINAL日志全部READY。24条非成功业务命令均为DRAIN开始后的4020，符合原切换边界。以下非零诊断照实保留：

- 每轮传输的`receiver_failed_tokens=1`均对应token8，原因`source_phase1_target_removed`；业务token从10开始，token8不在业务survivor集合。其日志身份为观察用管理员连接。原source逻辑会ABORT被移出phase1目标集合的token，receiver的failed计数也包含ABORTED；它不表示本轮某个业务事务未READY。见[移除入口](../../../sql/preserve_trx.cc:20010)、[计数分类](../../../sql/preserve_trx_transfer.cc:11532)。
- M1 ON三轮的`temp_native_early_failed`为15/11/18，合计44。候选step失败、取消或槽位被新代替换均会增加该计数，不能将聚合值直接解释成最终恢复失败。见[候选收尾](../../../sql/preserve_trx_receiver_candidates.cc:298)。
- 同三轮`final_failed_batches`各8，合计24。它同时统计必需资源和可选候选的step失败；原始聚合未细分原因，**不能逐笔断言只是换代**。见[必需分支](../../../sql/preserve_trx_transfer.cc:18191)、[可选分支](../../../sql/preserve_trx_transfer.cc:18812)。相同诊断在E34原负载复测中已出现；本轮最终业务survivor全部READY，但候选失败原因细分及其性能成本仍需后续观察。

M1三轮的receiver `native_early_reused`均为0。因此本文的“final复用170”仅指source DATA预捕获基线复用，不能扩写成receiver提前准备的原生资源全部命中；高频结果换代时的目标端提前准备收益仍有独立评估空间。

## 5. 原始证据与后续

[实验声明](../../../build-release/temp-matrix-final-20260928/PLAN.md)、[44轮参数清单](../../../build-release/temp-matrix-final-20260928/matrix.json)、[验收日志](../../../build-release/temp-matrix-final-20260928/acceptance.log)、[机器汇总](../../../build-release/temp-matrix-final-20260928/summary.json)、[完整结果表](../../../build-release/temp-matrix-final-20260928/RAW-SUMMARY.md)位于`build-release/temp-matrix-final-20260928/`。每轮有report、execution、两端真实变量、status时间序列与错误日志；frozen-inputs.json保存61项输入哈希。

本次关闭的是V03中“原定模型1/3/4/9的最终版本完整矩阵复测”子项。超出该矩阵的规模/持续时间、商业长尾SLO仍需另行定义和验收；V01的E34后全量回归、V02精确边界、外部V04–V06状态均不由本次运行改变。
