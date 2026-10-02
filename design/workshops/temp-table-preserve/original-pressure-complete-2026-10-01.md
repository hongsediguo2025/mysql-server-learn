# 原有压力模型补齐执行记录

> **2026-10-04 版本说明：历史版本记录。** 文内“当前/尚未完成/通过”和源码路径均属于记录时点；旧 PS 定义/参数/重建方案不再适用，原失败与测量不改写为新版本成绩。 现行职责见[详细设计](detailed-design.md)和[PS 删除与 cursor 关联](ps-transfer-removal-and-cursor-attach.md)，最新状态见[任务跟踪 E63/E64](task-tracker.md)。下文保留历史事实，不作为现行实施清单。

本轮按用户要求补齐此前被空间预检挡住的原有压力模型，使用当前 `ha_preserve_trx` 未提交实现。
不缩小模型、缩短业务窗口、调高预算或放宽性能门槛。各轮结果及验收边界如下。

**最终：按执行中用户调整后的轮数，11个模型、32轮均实际运行完成。原验收8轮通过、24轮未通过；
27轮完成交接且应有survivor全部READY，5轮读写模型未完成交接。**
另1轮在准备阶段按用户要求取消，独立记录，不混入32轮及失败率。

| 模型 | 实跑 | 原验收通过 | 交接／READY完成 | 主要未达标项 |
|---|---:|---:|---:|---|
| TPC-C 300仓／1000连接 | 1 | 0 | 1 | strict 4.234秒 |
| sysbench写事务 | 5 | 4 | 5 | 1轮精确尾987.675ms |
| sysbench自动提交 | 5 | 4 | 5 | 1轮精确尾709.723ms |
| sysbench读写 | 5 | 0 | 0 | receiver共享2GiB预算下解码准入失败 |
| mixed | 1 | 0 | 1 | 原尾790.514ms |
| range-10000 | 5 | 0 | 5 | 尾部、命令响应、锁覆盖、TPS计量 |
| range-1000 | 2 | 0 | 2 | strict、尾部、命令响应、TPS计量 |
| range-100000 | 2 | 0 | 2 | 尾部、命令响应、锁覆盖、TPS计量 |
| 持续不提交 | 2 | 0 | 2 | 尾部、命令响应、锁覆盖、TPS计量 |
| LOCKSET | 2 | 0 | 2 | strict 9.517／9.603秒；各仅1个survivor |
| tiered | 2 | 0 | 2 | receiver读负载p99，另1轮QPS也超限 |

“交接／READY完成”只表示本地transfer结果，不表示全部性能通过或外部物理升主／SQL RESUME已验收。
TPS计量无效与真实吞吐下降分别记录；原脚本首先失败时，其他已采到指标仍按原合同独立核对。

## 范围与执行方式

原full共10模型、38轮：write-only事务／自动提交／read-write各5轮；mixed 1轮；
range-10000／1000／100000／no-commit各5轮；LOCKSET及tiered各1轮。
另补TPC-C 300仓、1000连接、300秒业务、1轮。业务保持运行直至DRAIN完成，继续核对原连接和READY。
一次只运行一个负载，不并行构建、压缩或执行其他压力。

**执行中按用户指令调整轮数**：用户要求后续每模型2轮。已完成的24轮保留：
TPC-C 1、三种sysbench各5、mixed 1、range-10000 5、range-1000 2。
range-1000第3轮在数据准备阶段取消，没有完成正式业务窗口或DRAIN；原始失败记录保留，
单独标记为用户取消，不混入验收失败。其专用实例及目录已清理，无残留进程。
剩余range-100000、no-commit、LOCKSET、tiered各2轮，调整后计划共32个正式轮次。
仅调整重复数；原工作负载、预算、时限和验收门槛不变。修订依据为`scope-revision.json`，
剩余执行入口和该修订另以`continuation-inputs.json`冻结，原301项输入继续校验。

证据目录：`build-release/original-pressure-complete-20261001/`。
Release SHA256：`f4b7082a4ae6a7ea08cce8033624d32b1361a12f14706e65f3acbb87d97b7d43`。
源代码、驱动、Lua及二进制共301项冻结输入；它们不是整个仓库的完整快照。

## 空间处理

- 133份旧MTR `std_data`可再生副本逐项校验文件SHA、尺寸、符号链接目标后删除，约30.94GiB；
  repo原始fixture保留。
- 已结束且归属本工程的17个worker datadir回收约2.70GiB，同级日志保留。
- Debug的2202个可重建`.o/.a`中间文件回收约3.99GiB；当前Debug／Release可执行、源码和构建配置保留。
  后续Debug增量构建需重新生成这些中间文件。
- 17个旧失败数据库现场打包压缩，逐文件解压SHA核验后删除展开副本；原始日志保持可直接阅读，
  恢复索引为`failure-data-archives.json`。
- 可用空间从约11.8GiB升至52.3GiB。TPC-C五张大表依既有流程解压并校验后，用完整离线种子
  替换压缩形态；恢复净增约23.32GiB。克隆前约29GiB，保留reuse入口原26GiB准入和8GiB运行保护。

## TPC-C入口与验收边界

采用已校验的原300仓离线种子，同卷APFS `cp -cR`生成独立双实例；原seed保持离线。
每端独立inode、receiver新UUID，保持表／索引物理身份。此入口复用种子，初始化方式与原full
的loader不同；业务仓数、连接数、RR及300秒窗口保持。source和receiver的Preserve预算、buffer pool
各2GiB，Phase1为60秒，既有worker配置不变，临时表及result capture开关打开。

通过本轮薄入口调用已有账号配置方法，随后使用`preserve_trx_tpcc_reuse_e2e.py --rounds=1`。
该脚本每5秒报告吞吐（原full为1秒），并额外严格拒绝DRAIN期间1205增量；这些差异单列。
原始report、源／目标日志和sysbench输出保留。

reuse退出码仅表示其功能判断，不能直接充当原full验收。另行使用原final/scheduler校验器核对
精确BODY证据、严格Phase2≤2秒、末命令→ACK<500ms、receiver同钟ACK→READY≤500ms；
核对完整业务窗口、原连接/HOLD、READY集合、业务错误分类及未决所有权。缺失指标不记为0或通过。

## TPC-C结果

实跑1/1，功能通过，性能未通过。独立从原始日志重跑原final／scheduler校验器：

| 指标 | 实测 | 原门槛／判断 |
|---|---:|---|
| 完整业务窗口 | 300.033秒 | ≥300秒，通过 |
| 平均TPS／QPS | 685.50／19459.37 | 60个5秒区间；均为正 |
| 严格Phase2 | 4234.099ms | ≤2000ms，失败 |
| 末命令→final ACK | 381.464ms | <500ms，通过 |
| receiver同钟ACK→READY | 1.534ms | ≤500ms，通过 |
| SUCCESS survivor／READY | 229／229 | NOT_READY、积压、tainted均0 |
| EXACT BODY | 16724 | coverage=1、missing=0、fallback=0 |
| 原连接／HOLD | 1000／1000 | ID前后一致，HOLD为0..999 |
| 1205 | 业务前段339次；DRAIN新增0 | 业务重试允许；不能宣称全轮错误0 |
| fatal／reconnect／未决所有权 | 均0 | 通过 |

严格区间中T0→HARD为3852.383ms，HARD→结束约381.716ms；这只是时间分解，
不能直接把前者全部归为网络、捕获或业务锁等待。最后业务body退出后317µs即发布HARD，
前段存在946个T0 executing、16724个eligible body及大量support/permit。coordinator在tick前仍会
执行active-binlog progress；本轮44批、约2.88MB inline数据、246次token progress且同步flush/ACK。
缺少该操作累计墙钟和最后退出连接700的完整依赖链，不能进一步判定业务锁链、调度证明与同步progress的占比。
每5秒、LIMIT64的现有采样不足以完成这项分离。源端Phase1有效配置为60000ms；
receiver的600000ms是其默认值，本轮不在receiver执行DRAIN。sysbench打印p95=0.00，
本轮不将其解释为真实零延迟。独立判定记录为`tpcc-independent-validation.json`。

两个本轮克隆实例已退出并清理；离线seed重新压缩，逐表解压SHA验证后回收展开副本。
可用空间恢复约52.3GiB，证据、归档和配置完整保留。

## 完成核对

原计划保留的24轮及后续8轮均完成。31份native结果＋TPC-C独立验收逐项核实，
原301项输入及新增两轮入口等3项冻结输入均未变化；native各轮observer正常停止，
无清理错误或剩余进程。专用临时数据库根目录为空，可用空间约51.66GiB（df约52GiB）。
证据索引为`completion-audit.json`，逐轮指标为`round-summary.json`；后者保留取消行，
统计时必须过滤`included_in_requested_matrix=false`。
本轮不修改内核，不提交；本地transfer/READY证据不代替外部物理在线升主和proxy验收。

## 写事务完整5轮

| 轮次 | 平均TPS | 严格Phase2 ms | 末命令→ACK ms | ACK→READY ms | READY | 原验收 |
|---|---:|---:|---:|---:|---:|---|
| r01 | 6613.87 | 715.786 | 251.309 | 10.789 | 1000 | 通过 |
| r02 | 7241.89 | 277.755 | 249.246 | 10.987 | 1000 | 通过 |
| r03 | 7277.42 | 506.408 | 259.121 | 7.978 | 1000 | 通过 |
| r04 | 7207.75 | 563.905 | 339.632 | 9.935 | 1000 | 通过 |
| r05 | 6926.26 | 1106.532 | 987.675 | 8.732 | 1000 | 尾部超500ms |

每轮完整1000连接、128×20000行、300秒业务、513000个PS，DRAIN与全部1000 READY完成。
第5轮性能未通过，不能将整体5轮记为通过。原业务报告间隔10秒；DRAIN吞吐下降作为报告项单列。
五轮DRAIN活动窗口平均TPS相对稳态下降37.19%、42.63%、38.03%、40.97%、43.00%，
这是原sysbench的REPORT_ONLY项，不是连续事务模型的Phase1吞吐门槛。

### 写事务第5轮尾部失败分析

总迁移资源token都是1000，不代表活跃事务工作量相同。r01–r05最终锁事务目标分别为
47、3、71、164、974；其余token主要只保留PS等资源。r04 authoritative transaction targets
为197，reconcile后164；r05为974，reconcile后仍974。

r05对r04的phase2_total增加647.077ms：互不重叠的survivor worker/staging增加280.004ms，
final candidate pass增加12.108ms，commit_epoch增加55.351ms，其余阶段增加299.614ms。
日志UTC与FINAL单调时钟粗对齐显示，这约300ms主要位于worker启动前的final record
prepare/submit/consume/reconcile barrier；CLOSING→reconcile约355ms对55ms。
这是带日志写入误差的区间定位，不是barrier内部每项操作的独立计时。

final capture累计198.670ms/974次，平均204µs，与通过轮每次198–248µs接近；没有capture
overrun。PS descriptor的1000份final状态均复用presealed。最后body→HARD仅209µs，
receiver ACK→READY为8.732ms，因此这两处不能解释主要增量。worker wall包含staging，
不能统称capture时间；receiver处理与staging有重叠，现有1秒采样不足以剥离贡献。

主机有持续压缩压力，但接近尾部的16.081秒VM样本仅Swapins增加4页、Swapouts增加0；
缺少包围988ms尾部两端的VM/fault/栈采样，不能判定为磁盘换页导致。当前证据将问题定位到
源端final barrier和worker/staging两个阶段，内部耗时归因尚未完成。本轮保持实现、预算及SLO不变。

## 自动提交完整5轮

| 轮次 | 平均事件/s | 严格Phase2 ms | 末命令→ACK ms | ACK→READY ms | READY | 原验收 |
|---|---:|---:|---:|---:|---:|---|
| r01 | 5691.70 | 309.177 | 242.444 | 9.229 | 1000 | 通过 |
| r02 | 5812.23 | 290.211 | 245.016 | 8.722 | 1000 | 通过 |
| r03 | 5251.70 | 316.740 | 304.158 | 14.233 | 1000 | 通过 |
| r04 | 4978.36 | 921.009 | 709.723 | 12.566 | 1000 | 尾部超500ms |
| r05 | 5104.16 | 429.885 | 356.640 | 10.793 | 1000 | 通过 |

每轮1000连接、完整300秒，报告间隔1秒。1000个会话token承载512000个原生PS，
全部READY；最终活动事务目标均0。
自动提交原模型允许1213/1020/1205/4020/1062，1062在每轮report按业务和DRAIN窗口独立统计。
DELETE/INSERT独立提交可产生并发主键冲突，不能将err/s统称迁移错误。
五轮DRAIN活动窗口平均事件率相对稳态下降41.13%、39.04%、49.93%、38.69%、39.89%，
均按原驱动列为REPORT_ONLY。1062服务端增量如下：

| 轮次 | 完整业务窗口 | DRAIN调用至HOLD |
|---|---:|---:|
| r01 | 12853 | 674 |
| r02 | 13297 | 628 |
| r03 | 11727 | 642 |
| r04 | 11255 | 716 |
| r05 | 11540 | 625 |

第4轮与写事务第5轮的失败阶段不同：末body→HARD为374.739ms（r03为69.136ms），
CLOSING→ACK为334.700ms（r03为234.903ms）；约75%的新增尾部在HARD之前。
readiness_samples=0、wait_us=634，按调用顺序与单调时钟推导，T0注册返回后计时起点在
T0+584.186–584.470ms，前3轮约1.4–3.7ms；至少374.105ms发生于最后body退出后且注册尚未返回。
因此窄点是T0注册路径，未进入progress/扫描循环，不能套用final record barrier归因。
内部THD遍历、pending-exit合并、锁、分配与调度的占比尚缺证据。VM采样未包围该374ms区间，
不能确认换页原因。READY并未变慢，元数据字节也未增长。

## 读写模型资源准入失败

5轮均真实完成300秒业务、1000连接、1153000个PS的准备，在DRAIN迁移阶段失败。
receiver拒绝589483B批次，status=RESOURCE_EXHAUSTED、stage=identity，源端将其映射为4013；
不能解释为业务SQL语法不支持。r01 early pipeline完成616/1000，目标212在external object stage失败。

identity批次解码先申请SNAPSHOT_CODEC_BUFFER，额度为count*sizeof(frame)+3*wire_bytes，
本包仅3倍wire下界即1768449B。r01两次拒绝之间采样receiver占用2146726935B，
2GiB预算仅余756713B；inflight352080986B，低于1GiB，queued=0。
因此证据指向全局Preserve内存不足，非inflight额度用满。日志没有区分lease拒绝与bad_alloc
子分支，也没有完整按kind占用快照，不能进一步证明哪类长期对象占满全部额度。

源码中SNAPSHOT_CODEC_BUFFER同时包含短寿命解码缓冲和常驻PS descriptor／Ready／
Prepared_statement factory lease，不能按该名称推断全是临时编解码内存。
`make_statement`对statement持有的factory memory计费，非compact对象在构造后缩至实际arena；
Ready继续由resource candidates持有，直到final接管、淘汰或epoch销毁。
本次没有kind／token／owner级字节快照，尚不能拆分常驻对象与瞬时批次各自的准确占比。

五轮失败后全局占用降至约157–243KiB，最后样本仍在递减，说明大部分额度已经释放；
停止采样时尚未归零，不能宣布清理绝对无泄漏。early-ready及runtime-restores为累计动作计数，
不随销毁递减；不能据失败后仍有累计计数认定Ready仍被持有。
inflight为传输准入保留量，不等同RAM lease，也不能从2GiB全局占用中直接相减推算PS内存。

60秒Phase1内的PS capture累计303744次，但完整pretransfer descriptors/results/bytes均0，
不是303744个独立PS的覆盖证明。closing补充准备中receiver early-ready达到598时接近总预算。
final record reconcile129/129 exact，pipeline没有invariant或publication错误；直接失败来自接收端准入。
本轮strict23.818075秒包含失败与清理，final_ack=0，tail=0不具有通过含义。

历史原full1000RW曾被磁盘预检阻断；500 owner通过实验用4GiB预算、600秒Phase1及60秒业务，
1000 owner历史4GiB实验虽DRAIN成功但strict98.786秒失败。均不能借用为本次2GiB原模型的通过证据。

### 读写5轮完整结果

| 轮次 | 原始前300s报告平均TPS | 原始平均QPS | 失败strict区间ms | 最终ACK／READY | 结果 |
|---|---:|---:|---:|---|---|
| r01 | 835.97 | 16778.01 | 23818.075 | 未产生 | receiver解码准入失败 |
| r02 | 843.27 | 16907.15 | 20047.238 | 未产生 | receiver解码准入失败 |
| r03 | 841.70 | 16865.41 | 15539.952 | 未产生 | receiver解码准入失败 |
| r04 | 988.45 | 19822.33 | 15053.179 | 未产生 | receiver解码准入失败 |
| r05 | 1036.39 | 20784.13 | 17238.195 | 未产生 | receiver解码准入失败 |

上表原始均值来自sysbench elapsed≤300s报告；失败report没有返回完整scenario，
缺少校准后的post-ready steady字段，不冒充与成功轮完全同口径的steady平均值。
其中r03包含29个elapsed 11..291秒样本，其余为30个10..300秒样本。
五轮原始报告reconnect/s均0；r01/r05的err/s峰值0.1，具体SQL码未记录，不能补写全轮业务错误0。
驱动已通过业务窗口校验并实际执行DRAIN，5轮均有1153000 PS计数及完整日志。
strict包含失败和收尾，不是成功迁移性能。tail原始0作为无ACK处理，未列为0ms通过。

## 混合事务完整1轮

1000会话、100表×300000行，事务大小10000/1000/100/10的连接数100/200/300/400。
正式业务60秒，DRAIN前93374条语句，最终118004条；856个保留事务全部READY，NOT_READY、
pending、tainted及未决所有权均0，两端在线。DRAIN拒绝1000次，unsupported=0。

| 指标 | 实测 | 验收解释 |
|---|---:|---|
| 原report尾部 | 790.514ms | 原500ms门槛失败 |
| 精确last BODY→ACK | 1163.648ms | 也超过500ms |
| 严格T0→Phase2结束 | 26821.142ms | 原mixed strict limit=0，不套用sysbench 2秒门槛 |
| CLOSING后的phase2_total | 1164.681ms | 原mixed 600秒门槛通过 |
| receiver ACK→READY | 8.685ms | 500ms门槛通过 |
| receiver读负载QPS，baseline→transfer | 822.681→5811.795 | 原劣化判据通过 |
| receiver读负载p99，baseline→transfer | 41854→6397µs | 原劣化判据通过 |
| receiver读线程／错误 | 8／0 | 通过 |

receiver两窗口分别153.118秒和54.229秒，阶段不同，不能推断迁移提高读性能。
原report尾部取自phase2_transfer_tail_us，起点是QUIESCED侧latest_command_boundary；
精确FINAL从scheduler.last_body_exit到ownership final_ack，二者不能混用。

精确区间主要为T0→最后body25.656162秒（包含长命令完成），body→HARD仅74µs；
CLOSING→ACK1.163523秒中worker/staging488.351ms、candidate24.284ms、commit189.903ms。
与写事务r05同处较大的closing阶段，但mixed为991候选→856 exact，final HWM约8.13MB，
不能推定内部根因相同。meta ACK26.352ms及READY8.685ms单独不足解释整段尾部。

## 连续大事务执行与首轮计量问题

所有continuous模型的命令记录器自worker创建起累计，业务窗口开始时不重置。
下文“命令max／超限次数”包含预热、正式业务及DRAIN，保持原验收口径；
它们不是独立300秒稳态窗口的max，也没有据此推导p99。

range-10000第1轮完整业务：显式窗口300.006818秒，start→DRAIN调用300.819049秒，
大事务语句增加179810，短事务提交增加1030957。1042个SUCCESS survivor全部READY，
1100个原连接（1000大事务＋100短事务）均4020 HOLD，RR覆盖完整，非4020／连接／重连／1205错误均0。

原验收失败：记录锁4389023<5000000；strict1403.065ms通过，但精确BODY→ACK1391.341ms
超500ms（legacy尾903.270ms也超限）；receiver ACK→READY11.110ms。命令最大2277.262ms，
1856次超1秒（large DML854、BEGIN3、预期4020响应999）。

另有独立计量无效项：Phase1实际4066.291ms，并非窗口太短；5519个50ms配置采样，
sampler无错误。测试端DRAIN调用区间9568.870ms减源Phase1→terminal5470.067ms，
映射宽度4098.803ms超过原1000ms上限，TPS计算提前返回。`large_statement_progress_in_phase1=false`
是缺失count后默认0的派生结果，`drop=None`表示未计算，均不能证明真实业务停滞。
控制调用区间包含建连、发送与结果取回，4.099秒未分项，不能统称网络ACK时间。
RANGE未持久化这些原始窗口进度样本，无法事后精算Phase1 TPS；本轮不放宽计量门槛。

### range-10000全部5轮

| 轮次 | READY | 记录锁 | strict ms | BODY→ACK ms | ACK→READY ms | 命令max ms | 吞吐计量 |
|---|---:|---:|---:|---:|---:|---:|---|
| r01 | 1042 | 4389023 | 1403.065 | 1391.341 | 11.110 | 2277.262 | INVALID |
| r02 | 1059 | 4528376 | 1626.849 | 1618.727 | 11.354 | 2501.335 | INVALID |
| r03 | 1045 | 3912151 | 1619.501 | 1534.029 | 11.678 | 2911.932 | INVALID |
| r04 | 1028 | 4462253 | 1340.322 | 1338.585 | 22.105 | 2879.108 | INVALID |
| r05 | 1000 | 4278555 | 1269.535 | 1255.976 | 10.748 | 3711.281 | INVALID |

5轮均迁移功能完成、strict≤2秒，但精确尾>500ms，命令响应存在>1秒，吞吐计量均INVALID；
这些项分别保留，不以READY成功代替正式性能验收。每轮完整300秒、1100原连接，详见各report。

## range-1000完整2轮及源端收尾分析

| 轮次 | READY | 记录锁 | strict ms | BODY→ACK ms | ACK→READY ms | 命令max ms | 吞吐计量 |
|---|---:|---:|---:|---:|---:|---:|---|
| r01 | 1000 | 42492770 | 3558.814 | 3544.834 | 10.917 | 3548.356 | INVALID |
| r02 | 1000 | 38257229 | 2988.850 | 2975.741 | 8.463 | 2903.368 | INVALID |

两轮均功能交接完成，但strict和尾部超限；业务窗口起点→DRAIN调用分别300.310、300.132秒。
原字段`continuous_business_window_actual_us`包含窗口结束至DRAIN调用的准备间隔；
计算快照前后业务速率时，使用明确的window_end−window_start（约300秒），不混用分母。

首轮1000/1000 READY、COMMITTED_HANDOFF，无非4020业务／连接／重连／锁等待超时错误。
strict为3558.814ms，精确尾3544.834ms，均失败；receiver ACK→READY为10.917ms。
严格区间拆分为T0→最后body 10.978ms、body→HARD 7.882ms、HARD→CLOSING 0.068ms、
CLOSING→ACK 3536.884ms、ACK→结束3.002ms。增加的时间主要在源端CLOSING。

模型名称1000不代表更小的现场：RANGE_1000每条UPDATE访问100行，RANGE_10000为10行。
本轮记录锁42492770，为前一档五轮的9.38–10.86倍；receiver累计接纳473370948B，
为前一档的4.28–4.88倍，接纳帧数23101与前一档接近。
worker wall为1870.583ms，前一档506.422–626.485ms；按日志近似配对，
CLOSING→final record reconcile约1.349秒，前一档0.393–0.470秒。
final store snapshot累计628.516ms、prepare累计785.072ms，均明显增加，
但它们是并行操作累计值，不能直接相加或当作墙钟占比。

final metadata仍约3.54MB/5001帧，其ACK约29.970ms；full-lock-scan与Phase2 materialized
payload计数均0，不能解释为新增全量扫描。receiver Preserve内存采样峰202.912MiB，
明显低于2GiB预算，无RESOURCE_EXHAUSTED。现有证据尚不能剥离worker内部CPU、staging与等待。

命令超过1秒共2644次，包括1100个预期4020响应及1544个成功命令，最大3548.356ms。
TPS证据因source↔harness映射宽6924.025ms超过1000ms而INVALID，
不能将无法计算的吞吐跌幅写成实测超过20%。后续轮次继续使用原配置和门槛。

## 连续模型的业务推进量和主机边界

业务快照表明，300秒内range-10000五轮的大事务语句分别推进179810、172019、108031、
157006、120212条，平均599.35、573.38、360.10、523.35、400.70条/秒。
range-1000两轮为168725、150089条，平均562.41、500.29条/秒。
range-100000首轮为113409条，平均378.02条/秒，短事务完成1084591次、平均3615.21次/秒。
以上是整个正式业务窗口的快照差值，不能替代未通过时钟校准的Phase1吞吐证据。

100M为预置行数及所有大事务各自完整一轮的总行访问规模，不是本轮DRAIN时已持有100M条锁。
各range模型每条UPDATE分别访问10、100、1行；在相近语句推进量下，实际锁规模明显不同。
覆盖不足必须单列，不能因为有100M种子数据就宣布达到了5M记录锁验收门槛。

按业务日志与host采样对齐并取累计CPU TIME差值，压测Python进程约220%–333% CPU，
source约87%–101%（100%相当于一核）；这支持负载发生端开销较高，但尚不能证明GIL、
客户端调度或服务端等待的因果占比。短事务占完成业务语句约96.6%–98.0%，也不能把发生端
CPU全部归于大事务。range-100000首轮约286秒VM采样区间仅换入72页、换出0页，
没有持续严重换页解释低推进量的证据。range-10000 r03有较多换页，仍只是主机总量，
不能沿用此前百万PS场景的内存结论解释所有range结果。

## range-100000完整2轮

| 轮次 | READY | 记录锁 | strict ms | BODY→ACK ms | ACK→READY ms | 命令max ms | 吞吐计量 |
|---|---:|---:|---:|---:|---:|---:|---|
| r01 | 1050 | 693525 | 1315.900 | 1311.711 | 14.364 | 2659.506 | INVALID |
| r02 | 1059 | 737002 | 1271.519 | 1255.192 | 9.672 | 2321.830 | INVALID |

两轮均完整执行300秒正式业务并完成交接，非4020业务／连接／重连／1205错误均0。
strict通过，但尾部>500ms、命令响应存在>1秒，实际记录锁低于5M，吞吐证据均因时钟映射过宽无效。
原验收因此均失败，没有通过减少每条UPDATE工作量或放宽覆盖条件改判。

## 持续不提交完整2轮

仍为1000个大事务＋100个短事务、300秒正式业务，采用原UPDATE_FOREVER模型。
每个大事务仅BEGIN一次、COMMIT和完成事务计数均0，每种UPDATE至少执行一次，
两轮`continuous_large_no_commit.verified=true`。最终累计UPDATE分别391126、433245条，
包括业务计时前的准备进展；不能将这些累计值直接除以300秒计算窗口吞吐。

| 轮次 | READY | 记录锁 | strict ms | BODY→ACK ms | ACK→READY ms | 命令max ms | 吞吐计量 |
|---|---:|---:|---:|---:|---:|---:|---|
| r01 | 1000 | 1174815 | 1420.287 | 1397.146 | 17.844 | 5762.967 | INVALID |
| r02 | 1000 | 1301426 | 1254.100 | 1233.401 | 11.253 | 3612.360 | INVALID |

两轮交接完成，非4020业务／连接／重连／1205错误均0。strict通过，但尾部、命令响应、
5M记录锁覆盖和吞吐计量未通过，因此原验收均失败。
该模型交替执行point increment、point assignment、range increment及range conditional，
每条实际访问1–10行，不能用累计UPDATE数×10推断锁数；完整范围遍历最小轮数为0，
各worker本次最多751／809条UPDATE，而完整范围遍历需40000条，因此两轮都没有worker
遍历完全部100000行。正式窗口UPDATE增量分别178880／179189，平均596.261／597.294条/秒；
未以持续不提交替代全范围遍历的证明。

## LOCKSET完整2轮：命令结束前的长区间

原配置为1000会话、100表、每事务一条覆盖100000行的范围UPDATE后COMMIT，
完整300秒业务，Preserve预算256MiB、buffer pool 2GiB、Phase1上限600秒。

| 轮次 | survivor／READY | 最终记录锁 | strict ms | BODY→ACK ms | ACK→READY ms | 原验收 |
|---|---:|---:|---:|---:|---:|---|
| r01 | 1／1 | 100228 | 9516.748 | 125.721 | 0.017 | strict超2秒 |
| r02 | 1／1 | 100228 | 9602.703 | 137.465 | 0.028 | strict超2秒 |

两轮均保持1000条原连接，非4020业务／连接／重连／1205错误均0。
第2轮成功BEGIN／DML各75077、COMMIT 75076，也仅剩1个未提交事务；
T0→末body为9464.708ms，失败区间与首轮一致。两轮精确尾及READY均满足500ms门槛。

第1轮1000条原连接均4020后保持，最终仅1个未提交事务、100228条记录锁，1/1 READY。
驱动全生命周期成功BEGIN与DML各74643次、COMMIT 74642次；final reconcile
targets=captured=exact=1，invalidated=failed=0。其余业务正常提交，不能写成999个token迁移失败。
也不能把1000并发连接直接当成1000个完整锁集合的迁移覆盖。

strict为9516.748ms，超过原2秒门槛；精确BODY→ACK为125.721ms，原legacy尾119.782ms，
receiver ACK→READY为0.017ms。strict拆分：T0→末body 9390.706ms、body→HARD 0.051ms、
HARD→CLOSING 0.158ms、CLOSING→ACK 125.512ms、ACK→结束0.321ms。
问题主要在命令结束前，和range的长收尾不同。

source在覆盖该等待区间的约9.01秒host样本中累计CPU增加79.97秒，平均约8.88核等效。
它证明源端确有重CPU工作，但无栈和逐命令CPU记录，不能将整段归于最后一条UPDATE、COMMIT、
scheduler扫描或binlog progress中的某一项。support_edge=0、permit_issued=690；
代码仅对TX_END提供无依赖许可，不能称为690条依赖DML。

记录器的命令max为187410.654ms，identity为sid834的首条UPDATE。
该命令已在全部worker就绪、300秒窗口开始前完成，且记录器没有窗口重置；
这是含预热的全生命周期最大值，不是正式300秒窗口max或p99。
本轮原驱动明确失败于strict，未把report-only计量项改造为新的失败门槛。

## tiered完整2轮：receiver并存业务退化

两轮都完成1000个事务、100M条记录锁的交接，1000/1000 READY，NOT_READY、pending、tainted、
source未决所有权及quarantine均0。保留原256MiB预算、2GiB buffer pool，以及30条三档背景连接。

| 指标 | r01 | r02 | 原验收口径 |
|---|---:|---:|---|
| 原source Phase2 ms | 477.046 | 479.861 | ≤500，通过 |
| 原legacy尾 ms | 318.814 | 322.677 | 仅记录；此分支未执行尾部断言 |
| native FINAL strict ms | 726.991 | 660.871 | 额外观测；原tiered未启用FINAL oracle |
| 精确BODY→ACK ms | 656.641 | 601.893 | 额外观测；不冒充legacy尾 |
| receiver ACK→READY ms | 7.680 | 8.392 | ≤500，通过 |
| receiver ACK→全部prewarm ms | 30.872 | 12.241 | ≤500，通过 |
| receiver QPS baseline→transfer | 16524.560→15883.690 | 17789.121→16647.648 | 下降3.878%／6.417%，r02失败 |
| receiver p99 µs baseline→transfer | 2244→2639 | 2103→2486 | 上升17.602%／18.212%，两轮失败 |

receiver baseline分别5.005／5.010秒、82707／89124个样本；transfer窗口18.531／18.078秒、
294346／300956个样本，读线程8、错误0。原驱动在读负载保护处先失败，外层完整validator未执行。
读延迟计量包含客户端execute与fetchall；transfer窗口覆盖DRAIN、READY及探测收尾，
不能把这些读负载退化指标解释成仅Phase2期间的退化。
逐字段独立复核其余原合同：100M锁、READY／全部prewarm、lock plan约91.96MB未超约161.06MB
子池、零冷取页／目标local redo／ibuf merge等约束，以及三档p50／采样均满足。
失败路径在累计语句数赋值前返回，`completed_stmt_total=0`是未赋值哨兵，不代表大事务没有执行。

| 背景档位 | r01 样本／p50 ms／p95 ms | r02 样本／p50 ms／p95 ms | 原p50范围ms |
|---|---|---|---|
| 10ms | 6103／17.696／120.913 | 6130／19.428／113.884 | 5–40 |
| 100ms | 2105／109.862／162.032 | 2091／110.076／155.432 | 60–160 |
| 200ms | 1049／218.072／287.014 | 1025／219.550／284.018 | 140–320 |

每轮30条背景连接均有样本并自然遇4020停止，错误／断连／客户端sleep均0。
表中p95仅作观测，原背景负载验收没有p95或最大延迟门槛。
receiver退化已由两轮实测确认，但现有分段窗口、主机聚合CPU／IO及延迟样本不能区分
接收解码、锁预准备、同机源端竞争和调度的因果占比；尚未完成内部根因归因，不写成已修复。
准确FINAL显示body→HARD仍有额外时间，故legacy Phase2／tail低于500ms不证明精确尾低于500ms。

## 后续需要关闭的事项

1. 读写模型2GiB下的准入失败：按owner和生命周期拆分常驻PS与瞬时解码额度，再决定削减占用或改进准入；
   本轮未以调大预算处理失败，未证明具体大块泄漏。
2. 源端时延：分别处理T0注册、final barrier／worker staging和LOCKSET命令结束前长区间，
   不把所有失败都归入receiver或网络ACK，也不以同机换页解释全部结果。
3. receiver并存读退化：保持原8线程读负载及门槛，进一步取得同窗口内部阶段证据。
4. 连续模型验收计量：保留精确业务进度和更窄的source↔harness时钟映射，解决TPS INVALID；
   锁覆盖不足单列。不能用缺失字段默认0或放宽门槛宣布性能通过。

这些是本轮证据支持的未闭环项；当前只完成所请求的全模型运行及审核，没有实施未经定位的内核修复。
