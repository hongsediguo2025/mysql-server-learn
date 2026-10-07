# 500连接持续临时表与FETCH混合压测

**最新五轮（E93，直接核验R44版本）：** R50–R54保持原500连接/300秒/预算/不限速，功能5/5；strict为2589.884/4255.485/1874.739/4186.949/1769.103ms，ACK→READY为178.003/234.984/203.941/517.753/253.080ms。strict≤2000ms或≤2200ms与ACK<500ms共同门槛均2/5通过；ACK4/5。五轮有效EXACT均超500ms，完整脚本0/5。R44原始binary和10项脚本指纹与当前一致；同HEAD加R44批次输入patch重建核验46个内核文件全部逐字节一致，代码已恢复R44对应版本，差异仅文档/报告。五轮期间无代码/配置变化，**回撤正确但性能仍不稳定**。未提交/push。[五轮数据及回撤核验](../../../build-release/continuous-resource/r29-restored-r50-r54/README.md)。

**最新状态（E92已撤回，恢复R29）：** 对TEMP/RESULT实施过对象内data/ranges FD复用，保留pwrite、原重传/覆盖/SHA校验，使用既有FD额度与退休ticket，不新增线程池。原500连接/300秒/预算/不限速R48功能262 READY＋238 session-only通过，strict **3358.394ms** 未通过2.2秒，ACK→READY **496.767ms** 刚在500ms内，EXACT **3171.358ms** 未通过；source worker墙钟2941.140ms。未证明整体收益，不能仅凭open/close减少宣称优化，也不能用一轮数据证明全部退化由该改动引起。用户要求回退后已精确撤回本轮5个内核文件的增量及4个试验专用测试文件；R49在业务约85秒时中断、未进入DRAIN，不计验收。Release重建SHA恢复为`61b78ba69ad2ceea19c60fdaf2665da634901ede52fb643b80f922c6bbd3ec41`，与R29完全相同，benchmark及10个依赖SHA也相同。Debug/Release重建和回退后的cursor_final_transfer_chunks通过。R29五轮R40–R44仍是3/5双指标通过，回退不等于长尾已解决。未提交或push。[试验与回退证据](../../../build-release/continuous-resource/file-reuse-e92/README.md)。

**最新长尾诊断（E91）：** 对E90的source worker/staging窗口增加临时分段计时，按原500连接/300秒/预算/不限速完成R45–R47诊断，三轮功能通过。R45 worker3798.581ms中，协调器串行TEMP/RESULT/OTHER合计3747.862ms（98.665%），两处wave flush仅4.603ms；最慢source SEAL等447.659ms，对应同receiver后端前一CHUNK的ACK后apply447.635ms，而当前SEAL自身仅8.601ms。R46将长调用缩小到文件write/close；R47证明长close主要耗在OS调用，最慢848.919ms、MySQL登记处理仅3us。**R45已测worker窗口主要被最终当前代TEMP/RESULT的串行staging占据，并确认receiver文件处理反压下一请求；R47已测长close的文件登记开销不能解释其持续时间。** 不把不同轮次的细分耗时直接倒填给R41/R43。 OS内部具体I/O/调度原因尚未唯一归因；未实施修复。R45 final补传73份新结果代/164.2MB，不支持同代进度丢失后重复全传；仍需取证新代从产生到首次发送的时间线。三轮strict4013.580/2055.912/2033.606ms、ACK162.225/158.419/150.914ms；均带探针，R47另有低频栈采样，不并入八轮正式通过率。已撤回全部临时探针，强制重建Release与原R29 SHA完全相同；原EXACT失败及外部集成边界保留，未提交/push。[完整源码与逐帧证据](../../../build-release/continuous-resource/worker-window-e91/README.md)。

**最新五轮复测（E90）：** 同R29版本/500连接/300秒/原预算/不限速新增R40–R44五轮全部完成，功能5/5通过；strict≤2200ms与ACK→READY<500ms双指标**3/5通过**，原strict≤2000ms双指标同为3/5。R41/R43 strict **2963.767/2957.639ms** 超限，source target-worker墙钟 **2789.278/2778.728ms**；ACK五轮 **151.931–362.277ms** 全通过。加上E89共八轮双指标6/8通过，**R29基线仍未稳定达标，不能靠回退或前三轮绿色关闭长尾**。具体源端窗口内部原因尚未拆清，不归咎receiver READY、TEMP累计或fsync；本轮无内核/驱动修改。原EXACT超限/NA与脚本FAIL保留，R41首次端口预检失败另列并已补足五轮正式运行。[完整五轮及八轮报告](../../../build-release/continuous-resource/r29-repeat-r40-r44/README.md)。

**前次三轮复测（E89）：** 用户确认本轮 strict Phase2 **≤2200ms** 可接受，receiver 同钟 ACK→READY 仍须 **<500ms**；原≤2000ms与独立EXACT门槛保留。当前Release与R29逐字节相同，无需再回退。原500连接/300秒业务/预算/不限速连续三轮R37–R39：strict **1833.818 / 1694.442 / 1935.036ms**，ACK→READY **162.896 / 195.604 / 243.462ms**，两个关键指标按原2秒及本轮2.2秒均3/3通过；功能校验均通过。原脚本仍因R37/R38的EXACT超限、R39无合格EXACT证据而失败。保留R29基线、E85/E87继续撤回；历史同版本R30/R33长尾未据此关闭，三轮不代表长期稳定性或整体商用验收。[完整复测与指标](../../../build-release/continuous-resource/r29-repeat-r37-r39/README.md)。

**此前核定（E88）：** R35正式strict4169.042ms仍失败，ACK→READY77.144ms通过，E87已撤回。R36的500/120秒诊断为1920.659ms/149.974ms；最终39份待发送RESULT全为当前选中代、87.7MB，没有旧代补传。已删除临时日志，维持E84；不以诊断通过替代300秒验收，不修改退休协议、摘要或持久化边界。源端fallback与长调用仍需逐token取证。

**历史诊断与窄修改（E86/E87，未完成性能验收）：** R34的500/120秒分段诊断为strict2252.242ms、ACK→READY285.510ms；主要staging工作为RESULT1304.226ms、TEMP635.100ms，flush仅3.138ms。临时计时已删除。E87仅复用两处本次单帧decode，保留全部认证、路由和batch规则；Debug/Release构建及14个不同定向业务MTR通过，原500/300秒R35功能444 READY+56 session-only通过，但strict4169.042ms失败、ACK→READY77.144ms通过；E87未证明整体收益，已精确撤回并归档patch。E85仍撤回，不将局部复制减少称为整体收益。详见[实施记录](continuous-failure-fix-plan.md)。

**E84历史验收记录（本轮容忍口径见E89）：** 验收严格Phase2≤2000ms与receiver同钟ACK→READY<500ms必须同时通过；保持原预算、分组、300秒业务和不限速。R26两项1611.498ms/1532.279ms，R27为3267.422ms/293.310ms，均未同时通过，不称整体优化成功。R28同二进制诊断将当前热点定位到最终RESULT串行64KiB发送；E84仅在final采用现有协商分块、保留Phase1公平性和额度不足退路，10个不同业务MTR通过，正式R29两项1760.052ms/198.960ms同轮通过，独立EXACT尾部仍失败；R30同配置449 READY+51 session-only，strict2270.173ms失败、ACK→READY138.601ms通过，EXACT2269.235ms失败；两个关键指标尚未稳定共同达标。同二进制R31的120秒采样诊断未复现CLOSE/SEAL长耗时；E85单帧receiver直接执行试验R32虽功能通过，但strict4880.141ms失败、ACK28.911ms通过，未证明整体收益，已撤回并恢复E84R33与R29/R30二进制SHA相同，仍为strict4077.194ms失败、ACK18.347ms通过，故不能将R32全部归因于E85；继续保持撤回，R34仅补临时分段计时诊断。详见[实施记录](continuous-failure-fix-plan.md)。

**历史进展（E80）：** R14和R16均完成500连接完整归类、全部survivor READY、业务错误0。R14 strict889.592ms通过2s，R16 strict4133.502ms未通过；ACK→READY分别1277.689ms/1876.840ms，均超500ms，EXACT无合格样本。相同BASE复用的定向逻辑扫描从84,084,704B降到46,319,120B，9个业务MTR通过，原模型R17功能通过453READY+47session-only，strict7558.368ms/ACK→READY4388.992ms仍失败。整体性能仍未通过。详见[实施记录](continuous-failure-fix-plan.md)、[R16报告](../../../build-release/continuous-resource/mixed500-r16/README.md)。

**历史进展（E77）：** 在原预算内，稀疏BASE和最终raw回退的紧凑传输已消除r6/r7的容量拒绝；final候选可安全走既有代次复用。原500/300秒`mixed500-r7` DRAIN成功，413个survivor全部READY、业务异常0，但严格Phase2 **6,360.715ms**、receiver ACK→READY **3,411.420ms**仍超标；29个空BEGIN连接未分类，完整断言继续失败。定向单行final更新将receiver DATA写入从72MiB降至32KiB，相关7个业务用例通过。详见[r7完整报告](../../../build-release/continuous-resource/mixed500-r7/README.md)和[实施记录](continuous-failure-fix-plan.md)。以下首轮/r2/E76内容均为历史，不代表最新版本仍有相同失败。

2026-10-06。模型初版仅新增Python E2E与文档；后续E76/E77已包含上述内核修复，均未提交。复用现有Classic客户端、Release实例配置与初始化工具；不改变旧M2及其他模型。

## 固定业务合同

默认500个真实源连接：50 TEMP、50 FETCH、50 BOTH、350 RW；四组互斥。所有连接完成初始化并各完成至少一轮业务后，连续运行300秒再执行DRAIN。业务线程只因真实4020停止正常循环，socket一直保留到receiver READY及身份核验完成。没有ACK暂停、helper长命令、预停业务或限速。

- TEMP：每连接一张4096行InnoDB临时表，512B payload；轮换主键做单行UPDATE、点查核验，每16轮做全表COUNT/SUM及payload长度核验并提交一次短事务。保留临时表生命周期跨越所有提交。
- FETCH：查询共有永久只读数据表，4096行、512B payload；每批64行，按协议LAST_ROW_SENT判EOF，取完才COMMIT并重新BEGIN/EXECUTE。不是重复取前缀。整除时允许额外空FETCH确认EOF。
- BOTH：同样的用户临时表，每次修改下一批将读取的首行，再逐批FETCH既有结果；按EXECUTE时复制的行版本账本逐行验证，因此表已变更也不得改变旧结果。只在EOF后做全表聚合核验并提交，再产生下一代结果。
- RW：每连接独占一个永久表主键行，BEGIN→UPDATE→SELECT核验→COMMIT，消除跨连接人造热点；单独记录提交数，最终核对源端已提交数据。

只有真实成功命令才推进期望值和FETCH位置。各组报告成功命令数、提交/结果代数、FETCH批次/行数/EOF数、业务/Drain期间吞吐和延迟直方图。所有非4020异常均失败，4020只允许出现在DRAIN发起以后。验证500个原connection_id仍存在，不重连、不用新连接冒充。

## 准备与测量边界

两台本地Release mysqld共用本机，双方Preserve预算各1GiB、buffer pool各512MiB、BALANCED及既有worker配置。先创建已提交永久表/账号，源干净关闭后复制datadir给receiver，保证初始表/索引身份一致；之后不做物理redo复制。receiver同时保留已有只读事务中的用户临时表，READY后核验其值、ROLLBACK和未来临时表分配。

测量到standby transfer→receiver READY；不假装已完成外部在线升主、SQL RESUME或迁移后FETCH。性能从双方日志同钟字段提取：strict Phase2≤2000ms、最后命令→FINAL ACK≤500ms、receiver ACK→READY严格<500ms。保留功能结果与性能结果两个结论；超时、漏指标或错误不能当通过。

DRAIN survivor按实际结果对账；普通短事务及EOF间隙可能为session-only，不能要求READY=500。被动observer读取真实COMMIT的S集合，按成功提交ACK（OK/COMMITTED_READY/COMMITTED_NOT_READY）与后续READY共同确认，不能把500减survivor直接冒充S。带用户临时表、开放cursor或未提交DML的连接不能遗漏。保留每连接最终事务/结果位置及被拒命令，便于外部恢复验收。源端测量后按既有单向purge fence退役；这不是正常关机验收。

精确“最后命令→ACK”只有`last_body_exit_state=EXACT`且覆盖完整时才算有效。若`NO_ELIGIBLE_BODY`，报告保留原始日志并将该延迟标为null，不能把原生输出的0当500ms验收证据。strict Phase2、receiver同钟ACK→READY仍独立计算。

## 终态等待与诊断修正（2026-10-06）

首轮把业务socket、DRAIN响应和READY共同使用180秒期限，导致服务端尚在Phase1时驱动先退出。修正只涉及脚本，不调整服务端预算：

- `--timeout=180`保留业务命令超时；`--drain-target=180`独立记录DRAIN性能超标，超标仍继续收集。
- DRAIN终态收集期限从实际服务端Phase1/Phase2配置相加，再加`--terminal-grace=60`；本模型为600+30+60=690秒。DRAIN socket再留10秒余量，主线程仍在690秒处有界终止。690秒不是验收门槛。
- DRAIN返回后才开始独立`--ready-timeout=180`。DRAIN期间业务异常留在报告，不立即销毁服务端现场；它们仍使功能验收失败。
- 默认`--relay-mode=process`使用spawn独立转发器进程，避免与500业务线程共享Python解释器。`thread`仅用于单变量对照。协议、账号、TCP配置、包校验和业务内容保持相同。
- 转发器按请求（批次只计一次）累计有界耗时直方图，区分完整接收、解析、发送、后端响应等待、ACK观测和返回。后端响应等待包含receiver、网络及调度，不能称为receiver CPU或纯网络时间。`round_trip`从relay实际开始读取请求计时，不包含此前socket排队/调度及返回ACK后的直方图聚合。不声称仅记录COMMIT的observer能证明无重传；admission ACK字节也不代表语义处理成功的有效字节。
- 目标每10秒持久化`drain-diagnostics.jsonl`，记录业务进展、双端状态、转发器分段统计与进程CPU/RSS。采样依次执行，各端不能按同一瞬时状态解读；当前脚本另记录分项开始/结束时间。诊断使用独立总期限，读响应中断后关闭该连接，禁止复用半读协议流；relay控制超时同样作废通道。
- E76进一步使用两条专用诊断连接，超时关闭它们不会销毁控制连接或receiver背景TEMP。`--diagnostic-timeout`默认10秒，仅影响采样；20连接用1微秒期限验证两端诊断超时后仍保存业务账本和最终指标。末尾先保存owner账本和直方图，再尽力采集指标；指标错误不得掩盖原DRAIN错误，仍保留失败标记。
- 报告冻结主脚本及已加载脚本依赖SHA。失败DRAIN的原生FINAL单独记录，不依赖成功COMMIT；业务账本先于可能失败的relay快照保存。observer非正常退出、强制清理或指标丢失均不能判通过。即使功能断言失败，也保留独立性能结果。

修正验证顺序：先真实20连接，用极低的诊断门槛强制超标，确认仍收到DRAIN/READY并保留失败；随后按原500连接、300秒业务执行一轮。已有空BEGIN集合缺项继续保留失败，不改内核、不放宽断言。

## 实施与验证

- [x] 新增 `scripts/preserve_trx_continuous_resource_benchmark.py`：自有实例生命周期、四组负载、有界指标、结果核验和原始证据。原observer增加COMMIT集合解析/错误栈及默认关闭的分段计时钩子。
- [x] 先以20连接（2/2/2/14）短时运行，核验所有四组、真实EOF、旧结果快照、4020及原连接身份；严格交接集合断言发现空BEGIN边界，见下文，不标整体通过。
- [x] 以默认500连接（50/50/50/350）运行一轮，保存配置、输入指纹、时间序列、逐连接结果与两端日志；本轮DRAIN客户端180秒超时，未进入最终READY，不标通过。
- [x] 两个只读review后修复驱动问题，保留失败记录；清理本轮自有数据副本并保留报告，不运行新UT/GUnit或DEBUG_SYNC。
- [x] 修正等待与relay进程后，原配置500连接复测`mixed500-r2`：收到服务端4013，DRAIN 613.323秒；不是客户端超时，未达到READY。完整定位见下文。
- [x] 复测后补强诊断总期限/失败通道处理、失败终态保存与observer退出检查；最新脚本实跑`smoke20-r8`，成功收集DRAIN/READY及刻意设置的性能超标，保留原空BEGIN交接集合失败。未将小规模验证视为500连接通过。

## 使用方式

默认正式轮次（500连接、持续业务300秒、随后自然DRAIN）：

```bash
python3 -B -u scripts/preserve_trx_continuous_resource_benchmark.py --run-id mixed500-r3
```

20连接短验证（不代表500连接性能）：

```bash
python3 -B -u scripts/preserve_trx_continuous_resource_benchmark.py \
  --run-id smoke20-r1 --connections 20 \
  --temp-connections 2 --fetch-connections 2 --both-connections 2 \
  --business-seconds 8 --rows 256 --fetch-batch 32
```

每次使用新的run-id，拒绝覆盖旧目录。报告及双端日志在`build-release/continuous-resource/<run-id>/`，仅删除本次新建的临时实例目录；`--keep-data`可以保留故障数据。脚本退出码0要求功能和性能断言都满足；非0仍保留各项独立结果与原始日志。

## 已发现的边界

`smoke20-r6`中，20个原连接均收到4020并保持存在，源端已提交RW账本和receiver原有临时表/回滚/未来分配通过；DRAIN成功、资源READY，但3个普通RW连接刚成功BEGIN、首条UPDATE被4020拒绝，不在SURVIVOR或实际S集合中，模型保留失败。

源码解释：`sql/preserve_trx.cc`的`preserve_trx_has_explicit_active_transaction()`承认成功BEGIN，session-only资格拒绝显式活动事务；早期worker又以`strict_no_preservable_engine_state`把无引擎载荷、无资源的target从最终token集合移除。不能称它们已经进入S，也不能仅凭外部源码不可访问推断现网session上下文转移缺失。这是本工程严格连接交接分类的待核对边界；本次不改内核、不人为避开BEGIN窗口。

驱动开发失败均留痕：最初字典身份查询误用点分表名，随后修正为InnoDB目录的斜杠；observer最初硬编码错误magic长度，改用`len(magic)`；COMMIT ACK原本误限OK，改为识别真实已提交状态。这些失败不作为内核性能证据。

## 500连接首轮结果

默认配置 `mixed500-r1` 已实际跑完：全部500连接形成业务后持续300.008秒，业务期间发起DRAIN；客户端等待180.071秒后超时，现场仍是Phase1，未得到Phase2/READY指标。等待期间四组仍在运行，worker错误0、4020数0；不能标为DRAIN交接成功。

现场receiver已接收约172.6MB/3325帧，TEMP native early-ready=10、receiver worker idle=8；源调用栈仍在Phase1协调和初始候选完成等待。该采样不足以独立确定瓶颈根因。驱动超时180秒与原服务端Phase1上限600秒区分记录，没有为达标改变两者或预先停止业务。

[完整首轮报告](../../../build-release/continuous-resource/mixed500-r1/README.md)保留各组业务/Drain等待期间速率、输入指纹、完整账本、调用栈和日志。本轮测试实例已清理，未修改内核、未提交或push。

## 修正脚本后的500连接复测与定位

`mixed500-r2`保持原500连接、300秒业务、两端各1GiB Preserve内存及1GiB transfer inflight、原worker数和不限速配置。二进制SHA与首轮相同。**DRAIN 613.323秒后收到服务端4013；最终500个业务连接均到达4020，worker异常0。** Phase1到pre-closing为599.401秒，最终`COMMIT_UNKNOWN`，`final_ack_us=0`；失败路径strict区间12.911秒，不能当成功切换时延。没有有效ACK→READY样本。

已确认的错误链：receiver先回复admission ACK，后续`DECLARE_OBJECT`语义处理报`RESOURCE_EXHAUSTED`；源端因此仍登记对象并发送DATA。源码还显示，资源准备查询可将失败token映射成`RESOURCE_UNAVAILABLE`，源端TEMP/RESULT据此按可选预准备不可用清除等待，不向上升级失败；本轮没有逐次查询返回值，不能把每次查询都判成该分支。直到COMMIT检查此前apply失败，才进入提交错误/终态不确定路径。首错和后续错误必须分开：98次RESOURCE_EXHAUSTED、57次INVALID_ARGUMENT、662533次UNSUPPORTED、41次CORRUPT均为帧事件数，不能当失败事务数。

另有确定的重复工作：100个TEMP owner累计启动4904次baseline、复制51.422GB，源端prebuild安装事件16202次；启动数不代表全部COPY完成，安装数也不等于已发布receiver候选数。receiver目录保留同一token多代manifest/image。源码允许已初次完成owner在后续捕获后重新设置`receiver_pending`，再次阻塞全局初次完成条件。该反馈环静态可达，重复处理有运行证据；缺少每owner初次完成及pending代次账目，尚不能声称它是600秒等待的唯一原因。

容量判断也不能遗漏：近首错采样receiver inflight已达1,063,177,260B，距1GiB仅约10.1MiB，而Preserve内存峰值约200MiB。声明按对象完整大小加256B收费，历史对象与清理债务也占预算。按本轮每TEMP baseline 10MiB、每结果约2.145MiB估算，100份TEMP DATA加100份结果已约1.186GiB，尚不含UNDO/metadata。**这是模型尺度估算，不是首错瞬间精确账目；既不能把内存预算与在途账目混同，也不能断言仅清理历史代次就一定足够。** 本轮未提高预算或降低模型规模。

[复测完整报告](../../../build-release/continuous-resource/mixed500-r2/README.md)提供源码调用链、容量边界、流量口径与后续修复顺序；[原始报告](../../../build-release/continuous-resource/mixed500-r2/report.json)未覆盖改写。`inputs/`保留该轮实际加载脚本；随后诊断收尾补强对应`smoke20-r8`，没有冒称500连接已在最后脚本版本上再次通过。当前仅修正测试与定位，内核问题仍待修复。

R14原500/300秒复测：443READY+57session-only，未分类/业务错误均0，本地功能首次完整通过。strict889.592ms通过2s；ACK→READY1277.689ms仍超500ms，EXACT无合格样本而非通过。输入已冻结，无并行重任务。继续用同二进制R15短稳态诊断定位receiver尾部；不将其视作正式性能验收。详见[完整R14报告](../../../build-release/continuous-resource/mixed500-r14/README.md)。


R17原500/300秒结果：453READY+47session-only，业务/未分类错误0，功能通过；strict7558.368ms、ACK→READY4388.992ms仍失败，EXACT无合格样本。SOURCE累计扫描已降至2.218GB/1.061秒，但NATIVE累计18.953秒、IMAGE15.685秒；不同命令切点不可当等工作量对照。已暂停叠加并发修改，清理结束MTR的可再生std_data副本（保留证据）后可用空间恢复41GiB，以同二进制R18诊断定位等待。并未确认磁盘空间是因果，也不把局部扫描下降当整体闭环。详见[R17报告](../../../build-release/continuous-resource/mixed500-r17/README.md)。


### R19及诊断隔离核实（功能通过，性能仍开放）

原500/300秒R19完成439 READY+61 session-only，业务/未分类错误0。strict1959.247ms通过2s，仅40.753ms余量；ACK→READY2970.222ms未达500ms，EXACT无合格命令，整体仍失败。不能把无样本作为零延迟，也不以单轮strict通过关闭性能项。

诊断status原每端525行触发1074次socket recv，与500业务线程同进程形成可确认的调用放大；仅专用诊断wrapper加64KiB缓冲后，20连接实验降为2次、R19为1次，SQL/采样频率/绝对deadline不变。极短deadline独立实跑中两端在首个recv之前诊断失败，但19 READY+1 session-only、业务错误0，最终报告完整；此项不覆盖收到部分响应后超时；不归为内核功能失败。未证明该诊断放大就是ACK→READY根因。

R19最终prepare-only2966.036ms；整轮NATIVE13.105秒中11.457秒早在Phase1诊断已发生，不能归入最终窗口。后期IMAGE单批最大1981.933ms，正式staged job最大1981.938ms；源端/目标端时间必须用同阶段计量。SOURCE_FINAL现沿原RAII/TLS补TAIL、CLOSE、DIGEST、SEAL及其子项VALIDATE，非final不取时钟/更新计数；VALIDATE不能与SEAL重复相加。早期池实际16个worker，旧日志按普通路径误报10，现取成功join后的实际vector size，只修正报告，不修改建池行为。

这些新增计时经过6个业务MTR（含跨COMMIT四owner、final-tail、OFF、取消、physical metrics）验证；SOURCE_FINAL子计数不得在Phase1提前增加，每个owner final各计一次，已验证BASE复用时不再重新验全镜像摘要。Debug/Release构建成功。R20按500/120秒诊断（非300秒验收）进一步定位，不提高预算或盲目增加epoch并发。详见[R19报告](../../../build-release/continuous-resource/mixed500-r19/README.md)。


R21原500/300秒完成：440 READY+60 session-only、业务/未分类错误0，strict1825.504ms通过≤2s，但ACK→READY2076.796ms仍未通过严格<500ms，EXACT无合格样本。SOURCE_FINAL_CLOSE累计46.420ms、FINAL总656.398ms、ROUND2297.452ms；源端首次同步后移已缓解，receiver整体尾部尚未闭环。用户明确禁止以局部收益代替整体指标、不得越优化越劣化；保持规模/预算/业务不停/功能断言，无效或退化改动需复核撤回。当前暂停叠加内核修改，以同二进制R22（120秒诊断）继续定位receiver FINAL。详见[R21](../../../build-release/continuous-resource/mixed500-r21/README.md)。
