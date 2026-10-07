# 双端不限速与 receiver READY 优化实施记录

> 主agent负责修改和测试；subagent只读审核。用户已授权实施，不提交或push。

2026-10-06：已在 `ha_preserve_trx` / `c9bc199a64d9` 及既有未提交收敛修改上完成以下实现和定向验证。完整输入指纹、失败留痕、最终指标见[验证报告](../../../build-release/receiver-ready-optimization-20261006/README.md)。

目标：删除receiver主动速率/固定睡眠限制；Phase1在原截止期内确认精确候选已准备；final仅一次完整资源认证，后续批次检查冻结身份。源端也取消发送限速（用户在实施中明确）；双方内存额度不变。

依据：[同版诊断](../../../build-release/ready-tail-diagnosis-20261005-235816/README.md)。主因是最后大候选提前窗口仅26.6ms、receiver按32/512MiB/s等待，以及每批在registry锁中复制历史record。32会话native尖峰未闭环，不猜测删除fsync。

实现边界：仅Preserve模块；没有native业务热点新增逻辑，没有新线程池/外部升主阶段，继续使用既有prepare/trx_lists/adopt和SQL RESUME。物理PS回放与session迁移职责不变。

- [x] 测试先行：已有temp_ready_workload MTR及Python ready benchmark增加两端无rate/yield睡眠、稳定TEMP候选final选择前early_ready断言。旧Release的128MiB稀疏场景两个断言均失败，ACK→READY为940.316ms；保存原report/日志。原M4/M5/M10/M11保留为性能比较基线。
- [x] 去双端限速：sql/preserve_trx_transfer.cc/.h删除saved/prewarm/source专用limiter、速率字段、固定yield及仅用于限速的统计遍历。保留原worker数、epoch credit、队列/内存/对象大小预算、显式pause及timeout。删除撤销逻辑专属的现有UT声明/旧断言，不新增UT。
- [x] final复用：sql/preserve_trx_receiver_prepare.h用needs_temp_selection()区分首次接管；首次完整lookup/authorize/matches后只保留已有不可变object_index的shared_ptr。registry短锁校验state、frozen、同index、同contract、未retired；每批原有epoch expiry/reject检查及发布前复核保留。可变早期候选继续完整校验。
- [x] Phase1确认：增加只读QUERY_RESOURCE_PREPARED，用现有认证连接/ACK绑定epoch/token/精确object-id（含digest）/nonce；不消费数据序号，不重用promotion control或terminal status。查询即时返回准备中/完成/不可用，丢ACK不新增数据不确定债；已有数据不确定债不能绕过。查询不能进入batch或普通apply，普通帧不能接受查询专用ACK状态。
- [x] source调度：TEMP job SEAL后照常安装sidecar，再由owner以小pending identity确认receiver；等待中的pending不占capture容量，不停驻worker或占用credit，实际查询仍由原worker执行。按not-before在既有pipeline调度，原Phase1 deadline到期后停止确认并保留final fallback。RESULT先发送有限批次全部文件再查询各精确generation；final finish不等待普通阶段查询。
- [x] MTR/E2E验证：12个不同用例通过，覆盖稳定TEMP及结果资源、候选换代、取消、缺BASE/错误身份、GC、OFF隔离及只读查询丢ACK后的真实本地SQL RESUME。不用DEBUG_SYNC，不新增GUnit；不是完整MTR或所有故障组合复验。
- [x] 同输入Release复测M4 sparse/dense128、M5大/小owner、M10 unheld16、M11 BUSINESS_FIRST及M3，各一次；同时输出Phase1、strict Phase2、ACK→READY、final处理量、早准备/复用、内存峰值。双方原1GiB Preserve预算和worker数不变。

风险控制：查询不阻塞SEAL或源session长等待；状态查询先取registry短锁，再放锁查询candidate，没有文件I/O。Classic网络超时按秒取整：查询不等待operation mutex，距截止不足1秒（需建连时2秒）直接转final；不声称毫秒级硬截止。冻结身份pin防止地址ABA，取消后必须停止发布。原始镜像/安装镜像及持久化暂不简化。测试/性能证据与尚未验证的外部HA接入分别记录。

## 最终证据与边界

六组Release原场景断言全部通过：strict Phase2为3.336–58.181ms，receiver同钟ACK→READY为0.082–30.734ms，分别低于原2s/500ms门槛。双端节流计时、cursor捕获失败增量均为0；final processed_bytes均为0，仍执行最终验证与绑定。M5的32个TEMP候选全部提前准备并被final复用；M3为32/32。六组DRAIN开始至观察到READY的总时间也均低于各自旧样本，不能只用尾部缩短代表整体吞吐提升。

实施中已修正并保留失败记录：显式pause返回UNAVAILABLE防止无意义等待；查询通信失败只退出可选等待，避免循环重连；观察relay仅对query接受新ACK状态。新增丢ACK用例证明其不污染数据序号或阻止最终交接。两个只读review完成并复核取消/最后使用者、冻结身份及协议边界。

本轮未重跑完整压力矩阵、全量MTR或长期重复统计，也未验证外部物理升主/proxy。M3旧native尖峰的存储与调度原因未证明消失；本次把准备工作前移并减少final重复处理，没有删fsync、放大预算或改变验收门槛。人工网络限速仍可由测试relay模拟，不限速样本不代表8/2MiB/s链路达标。
