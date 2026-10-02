# 模型 2、11、12：混合事务与 receiver 压力验证

> **2026-10-04 版本说明：历史版本记录。** 文内“当前/尚未完成/通过”和源码路径均属于记录时点；旧 PS 定义/参数/重建方案不再适用，原失败与测量不改写为新版本成绩。 现行职责见[详细设计](detailed-design.md)和[PS 删除与 cursor 关联](ps-transfer-removal-and-cursor-attach.md)，最新状态见[任务跟踪 E63/E64](task-tracker.md)。下文保留历史事实，不作为现行实施清单。

状态：54轮正式负载及6轮独立长尾诊断已完成；正式48轮通过、6轮未闭环。**容量失败后的两条收尾路径尚未闭环，本轮没有修改内核。**

## 验证了什么

沿用 E38 的 Release mysqld，SHA256 为 `c4896fe57f05a76351bb2eb6dffee50562e12d56ad9c5ec4f210265fdd5a74ba`；67 项内核与 Release 二进制指纹与 E38 一致。新增 Python E2E 负载和观测工具，不新增 UT、DEBUG_SYNC、服务端线程池、RESET DRAIN 逻辑或外部升主阶段；不提交、不推送。

| 模型 | 同一轮内的负载 | 重复 |
| --- | --- | --- |
| 2 | 普通表、仅 TEMP、普通表＋TEMP 三类，各有64/8192行；小事务持续 COMMIT/BEGIN，大事务持续修改并保留结果；6/12连接，分别 no-bin/ROW log-bin | 每格3轮 |
| 11 | 4个迁移 owner，每个4096行×512B及保留结果；基线、8/2MiB/s传输限速、BUSINESS_FIRST目标策略、4个CPU工作连接、2个IO工作连接、64KiB内存预算、4MiB inflight预算、准备超时、READY后到期回收 | 每格3轮 |
| 12 | 4/12个迁移 owner；receiver同时有0/1/8组原有只读 TEMP＋建删 TEMP 连接；逐轮检查哨兵、内容摘要、ROLLBACK、READY后的新表分配 | 每格3轮 |

模型2在业务开始前将已提交的源 datadir 冷复制给目标，保留不同 UUID，核对永久表的 table/index/space ID。之后两端在线执行真实 transfer。没有模拟后续永久 DML 的物理 redo；READY验证锁元数据和临时资源准备，不证明永久数据已追平。

模型11的连续负载沿用普通命令和初始 SEAL 协调窗口，随后三轮业务变化期间没有人工 ACK 暂停。DRAIN包含这些受控窗口，不能当作纯静默切换时长。限速只作用于传输请求，所有连接共享总额度、16KiB发送分片；不改变产品协议。CPU压力是实际SQL计算，IO压力是两份各96MiB TEMP反复更新和扫描，receiver buffer pool128MiB；统计引擎页IO，不声称底层设备已经饱和。

模型12把长期只读事务与临时表 DDL 分开使用连接；MySQL允许前者修改已有 TEMP，但不允许在 READ ONLY事务内 CREATE/DROP。原有只读哨兵同时经历迁移压力，不能用另一个无关 mysqld 代替共存验证。

本地验收止于 **transfer → receiver READY／明确失败／资源回收**。真实物理 replay、固定的三个在线升主入口、SQL RESUME和proxy续接仍由外部物理复制工程验证；本轮未测它们的时延。

## 结果

**54个有效正式轮次，48轮通过；249个事务token READY，12个token按准备期限预期分类为NOT_READY。结果捕获失败为0。** 48轮通过包含45轮READY成功及3轮预期NOT_READY验收通过。两个容量负例各3轮未满足收尾合同，不能计为通过；另8次旧输入或测量干扰的尝试保留但排除。

| 测试格 | 通过／运行 | READY／NOT_READY | DRAIN秒：中位数（范围） | READY尾部秒：中位数（范围） |
| --- | ---: | ---: | ---: | ---: |
| m2-6-off | 3/3 | 17/0 | 1.351 (1.318-2.357) | 0.223 (0.212-0.326) |
| m2-6-on | 3/3 | 15/0 | 2.978 (1.699-4.115) | 0.288 (0.212-0.777) |
| m2-12-off | 3/3 | 30/0 | 4.429 (3.844-6.105) | 0.389 (0.199-0.878) |
| m2-12-on | 3/3 | 31/0 | 5.542 (4.135-9.636) | 0.430 (0.394-0.495) |
| m11-baseline | 3/3 | 12/0 | 5.851 (5.055-6.279) | 0.283 (0.181-0.376) |
| m11-net8 | 3/3 | 12/0 | 18.679 (18.600-18.731) | 0.286 (0.248-0.921) |
| m11-net2 | 3/3 | 12/0 | 56.891 (49.548-62.584) | 0.375 (0.247-0.494) |
| m11-profile | 3/3 | 12/0 | 9.857 (9.513-10.127) | 3.936 (3.931-4.274) |
| m11-cpu | 3/3 | 12/0 | 6.071 (4.855-6.332) | 0.233 (0.112-0.356) |
| m11-io | 3/3 | 12/0 | 6.530 (5.177-9.204) | 0.895 (0.218-1.523) |
| m11-memory | 0/3 | 0/0 | 0.115 (0.106-0.115) | - |
| m11-inflight | 0/3 | 0/0 | 0.648 (0.617-2.454) | - |
| m11-deadline | 3/3 | 0/12 | 1.144 (1.088-1.202) | - |
| m11-expiry | 3/3 | 12/0 | 2.352 (0.780-3.188) | 0.315 (0.211-0.598) |
| m12-4-0 | 3/3 | 12/0 | 1.907 (1.111-3.184) | 0.206 (0.031-0.231) |
| m12-4-1 | 3/3 | 12/0 | 1.159 (0.652-1.829) | 0.274 (0.036-1.139) |
| m12-4-8 | 3/3 | 12/0 | 1.712 (1.165-2.447) | 0.351 (0.301-1.391) |
| m12-12-8 | 3/3 | 36/0 | 6.228 (4.124-10.264) | 1.154 (0.338-1.473) |

表中模型2的off/on仅指binlog开关；Preserve、TEMP和结果捕获均开启。模型12的后两个数字是迁移owner数／receiver共存组数。memory／inflight行的DRAIN时长是失败返回时间，不是迁移成功时长；两者均未进入READY／NOT_READY分类。

模型2共108个业务连接，93个实际保留事务READY；另外15个普通小事务连接在截断时已提交且没有待保留引擎事务。它们按命令边界退出，不是遗漏15个迁移事务。模型2正式轮次的EXECUTE p99为0.596–2.037ms，COMMIT p99为0.605–5.918ms，但单次COMMIT最大908.984ms，不能用p99掩盖长尾。

模型11的准备超时格在解除暂停后，12个token均确认为PREWARM_DEADLINE，随后资源归零；到期格先确认12个READY，再确认到期回收。网络限速使DRAIN明显延长，但这些轮次仍全部READY。BUSINESS_FIRST格READY尾部3.931–4.274秒，与BALANCED基线0.181–0.376秒不同；策略同时把prewarm带宽从512降至32MiB/s、worker从3降至1，并调整其他预算，不能当作单独带宽变量的因果实验。CPU格及IO格均通过，receiver采样CPU最高468%；这里只证明施加了真实SQL与页IO压力，未证明整机或设备的容量上限。

模型12的72个迁移事务全部READY，原只读TEMP内容、ROLLBACK及后续分配均通过。receiver一轮建表→写入→查询→删表→只读哨兵验证的p99：1组为0.721–0.997ms，8组为4.665–4.924ms，12迁移＋8组为4.600–5.156ms；对应单轮最大87.361／303.369／827.685ms。它们是完整业务循环时长，不是单条SQL。

时延口径：`READY尾部 = 客户端观察到READY − DRAIN返回`，包含轮询和查询开销。每格3轮给范围和中位数，不计算epoch p99。SQL命令p99来自各轮原始命令样本。源端与receiver在同一台开发机，不能把跨格差异单独归因于某个内核锁，或作为商用容量上限。

## 缺口一：接收内存不足后，发生错误的上传 owner 没有收到 ABORT

64KiB预算格预期拒绝，验收目标是安全退出并收回资源。同一65,801B CHUNK发送两次（含一次重试），均被receiver以`RESOURCE_EXHAUSTED`拒绝后，源端DRAIN返回4013；4个源owner仍能读到正确值并ROLLBACK。然而receiver持续留下1个inflight owner、84,827B额度及1,227B已计费内存。

```mermaid
flowchart LR
    A[CHUNK收到原生错误包] --> B[客户端接口返回IO_ERROR]
    B --> C[重试后保存uncertain payload]
    C --> D[同槽ABORT内容不同]
    D --> E[发包前返回ACK_UNCERTAIN]
    E --> F[receiver的OPEN owner仍存活]
    F --> G[现有accepted epoch计时器无法回收它]
```

在校准`pilot11-memory-c`中，被拒绝的是token13；wire记录有ABORT11、12、14，独缺ABORT13。正式三轮重复得到相同残留大小。源端查询／回滚通过不能覆盖receiver收尾。

对应源码：

- [default_transfer_client_send](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:1940)把原生SQL错误统一映射成IO_ERROR；[发送重试](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:2781)保留不确定帧。
- [同通道限制](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:2635)在发送ABORT前返回ACK_UNCERTAIN；[abort_epoch_locked](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:14564)继续其他owner，但保留首次错误。
- [阶段一失败入口](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx.cc:20604)调用普通abort；[receiver reaper](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:17741)处理已接受epoch及显式回收债务，没有替未提交上传owner补发取消。`active_epochs=0`只说明没有accepted epoch。

这是仍持有资源的owner，不能改写为“垃圾文件等重启清理即可”。修复也不能一般性清空uncertainty，或把任意4013当作已认证的“确定未应用”；必须保留真实ACK丢失时的安全性。

## 缺口二：异步应用额度不足后，COMMIT登记状态无法收尾

4MiB inflight格的首次错误在异步DECLARE_OBJECT应用阶段，早于最终COMMIT。其admission ACK并不表示对象应用已成功；后续CHUNK因为声明未成功而失败。

```mermaid
flowchart LR
    A[异步DECLARE因额度不足失败] --> B[最终COMMIT先登记COMMIT_ADMITTED]
    B --> C[等待此前应用时读到失败并返回]
    C --> D[QUERY得到NOT_COMMITTED]
    D --> E[ABANDON遇到已登记COMMIT而不能接管]
    E --> F[缺少NOT_COMMITTED_CLEAN证明]
    F --> G[源端COMMIT_UNKNOWN并保持fencing]
```

[接收路径](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:21321)先做终态准入，再在[应用等待](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:21339)处提前返回；该返回绕过后面的状态收尾。[ABANDON](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:9383)遵守COMMIT单赢家约束，返回当前NOT_COMMITTED；[reaper](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:10429)又跳过COMMIT_ADMITTED。

正式三轮receiver的memory、inflight和queue均已归零，但终态在当前在线收尾路径中无法推进到NOT_COMMITTED_CLEAN。源端因此进入[COMMIT_UNKNOWN保护路径](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx.cc:24084)，保留4个隔离owner；后续业务收到4020是正确保护行为。

本格最初借用了“阶段一拒绝后可以继续SQL回滚”的断言，因而在4020处停止。这个断言不适用于已发送COMMIT的路径；不能把4020本身记成bug，也不能因此放宽业务fencing后宣称通过。真实缺口是：前序应用已经失败、COMMIT尚未执行的情况下，终态不能确定封存和清理。日志、QUERY/ABANDON帧及终态计数支持该定位。本轮保留为未闭环，不擅改COMMIT/ABANDON单赢家协议。

## COMMIT长尾诊断

校准`pilot2c`曾出现1.709489秒COMMIT，位于Phase1；同期TEMP-only COMMIT最大1.107ms。对应约1.870秒窗口内，TEMP copy累计增加38.240ms；整个Phase1的record latch累计持有131μs。这些已计量耗时不足以解释整个停顿。正式12轮模型2最大COMMIT为908.984ms，没有复现原来的≥1秒尾点，也不表示问题已经消失。

追加6轮独立诊断全部功能通过，全部排除在上述性能矩阵之外：

| 运行 | 观测方式 | 最大COMMIT ms |
| --- | --- | ---: |
| diag2-bin-a | 栈采样 | 34.189 |
| diag2-nobin | 栈采样 | 57.122 |
| diag2-bin-b | 栈采样 | 262.273 |
| io2-bin-a | 栈采样＋慢系统调用记录 | 382.950 |
| io2-nobin | 栈采样＋慢系统调用记录 | 11.328 |
| io2-bin-b | 栈采样＋慢系统调用记录 | 151.619 |

**已直接定位io2-bin-a这个尾点的主要等待：** mixed owner13／OS thread2664431的COMMIT区间为5172909–5555859μs，期间同线程对原生`mysql-bin.000002`执行fsync，耗时303.305ms，占该命令79.2%，距COMMIT完成仅168μs。见[实际系统调用第385行](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-pressure-models-20260928/io2-bin-a/source-slow-io.tsv:385)。同窗口redo fsync、redo pwrite及其他线程mirror pwrite分别停顿约303／302／295ms，几乎同时返回；能证明多个文件写入／同步调用共同停顿，不能进一步指定APFS内部或设备根因。

io2-bin-b的151.619ms COMMIT中，同线程binlog fsync仅7.949ms；并发redo fsync覆盖149.425ms，但覆盖不是因果分账。带探针和不带探针的栈均采到原生redo flush、group-commit排队及binlog sync等待，未显示TEMP worker join或mirror mutex占主导。栈为8秒聚合，不能冒充每次COMMIT的精确等待时间线。

io2-bin-a中，慢调用记录器本身在log flusher／writer上各占32个`record→write`采样点，有可见扰动；这些运行只用于定位。process-local warmcopy mirror的`defer_file_fsync=true`，不能把普通mirror flush写成fsync。**原校准1.709秒尾点仍未直接复现定位；当前证据支持继续查原生日志持久化等待，尚未证明是TEMP捕获导致。** 本轮不据此修改内核或宣称长尾修复。

## 证据与校准修正

原始产物在[实验目录](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-pressure-models-20260928/)，入口为[计划](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-pressure-models-20260928/PLAN.md)、[矩阵](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-pressure-models-20260928/matrix.json)、[冻结输入](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-pressure-models-20260928/formal-inputs-repaired.json)、[汇总](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-pressure-models-20260928/summary.json)。各轮保存命令、变量、二进制／负载SHA、原始命令时序、真实传输帧、source/receiver状态、CPU/RSS和错误日志。

- 初次READ ONLY DDL、永久表全扫描锁等待及relay错误包处理的校准失败，单列在[定位记录](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-pressure-models-20260928/confirmed-findings.md)。没有把这些夹具失败算作内核回归。
- 遗留CLI检查在业务前误拒绝模型2/12。它的修正使初始总控输入校验返回失败；修正前的轮次不计入最终验收，按同一修正后SHA补跑，原记录保留。
- 完成轮次的status归档采用gzip，验证解压后的SHA一致后才移除原始未压缩副本，见[归档清单](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-pressure-models-20260928/status-archive-manifest.json)。一次与归档IO重叠的基线另行排除、补跑，见[排除清单](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-pressure-models-20260928/measurement-exclusions.json)。
- 此机Python3.9的不同进程monotonic起点不同。跨进程观测使用各自UTC锚点对齐，单进程耗时继续使用monotonic；旧IO校准的空窗口判断已修正。引擎IO计数可能命中OS缓存，不作为设备IO测量。
- 每轮结束后保留日志并删除本任务已停止的datadir。源端已有单向purge fence，runner在测量结束后按既定方案对专属source执行SIGKILL，receiver正常SIGTERM退出；这是测试清场，不是本轮观察到的服务崩溃或崩溃恢复验收。没有清理其他会话工作区；尚存READY资源不会被提前要求归零，专门到期格再验证回收。
- [最终核验](/Users/a1234/project/mysql-server-8022-preserve-port/build-release/temp-pressure-models-20260928/final-verification.json)：67项内核／二进制指纹、10项冻结输入匹配；54轮生命周期记录及6轮诊断核对通过，专属pid／ibdata文件无残留。新增Python脚本语法检查与`git diff --check`通过；没有运行新的全量MTR，本轮内核未改动。
