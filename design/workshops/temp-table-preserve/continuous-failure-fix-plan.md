# 持续TEMP/FETCH失败收口实施记录

## E93：回撤后五轮及R44源码核查（性能仍开放）

原500连接（50 TEMP/50 FETCH/50 BOTH/350 RW）、300秒持续业务、原预算/worker/不限速，R50–R54全部完成。五轮功能通过，原连接/源已提交账本/receiver既有TEMP隔离通过，worker异常与未分类均0。没有新增内核修改、探针或改变测试驱动。

| 轮次 | strict Phase2 ms | ACK→READY ms | EXACT→FINAL ACK ms | strict≤2200ms且ACK<500ms |
| --- | ---: | ---: | ---: | --- |
| R50 | 2589.884 | 178.003 | 2574.553 | 失败 |
| R51 | 4255.485 | 234.984 | 4254.597 | 失败 |
| R52 | 1874.739 | 203.941 | 1874.218 | 通过 |
| R53 | 4186.949 | 517.753 | 4186.461 | 失败 |
| R54 | 1769.103 | 253.080 | 1767.295 | 通过 |

原strict≤2000ms口径同为2/5；ACK4/5，R53超过门槛17.753ms。五轮EXACT均有效但超限，原脚本整体0/5。source worker/staging墙钟为2418.794/4055.323/1474.114/3995.320/1602.930ms，包含协调器staging和等待，不能当CPU。R51无TEMP回退仍长；其close累计4532.148ms不是独立墙钟根因，R53 close累计仅73.179ms但worker仍3995.320ms。继续保留长尾问题，不用重跑绿色关闭。

用户要求直接核查R44：R44原始报告的Release SHA与当前/R50–R54一致；10项脚本指纹（含主脚本）与配置除run_id外一致。以同HEAD `c9bc199a64d90b3881daec03bbdd4c5c4de14357`和R40–R44输入patch重建62个文件，46个内核文件逐字节一致，61/62文件一致，唯一tracked差异为task-tracker文档；E91探针前快照交叉验证一致。独立审核还确认历史51个未跟踪文件中49个一致，唯二差异为设计文档，E91/E92源码均已恢复原备份，4个E92试验测试已撤除。

结论为**运行内核及测试代码已恢复R44对应批次版本**。证据是批次源码输入快照和单轮二进制身份，不声称R44单轮保存了全源码hash，也不声称含文档及报告的整个目录相同。五轮期间tracked diff完全不变。此前R40–R44为3/5，本轮2/5，只能说明同版本仍有波动，不能归因残留试验代码。未测外部在线升主/SQL RESUME，未提交/push。[完整报告与逐文件hash](../../../build-release/continuous-resource/r29-restored-r50-r54/README.md)。

## E92：对象内FD复用试验已撤回，恢复R29

**最新状态（E92已撤回，恢复R29）：** 对TEMP/RESULT实施过对象内data/ranges FD复用，保留pwrite、原重传/覆盖/SHA校验，使用既有FD额度与退休ticket，不新增线程池。原500连接/300秒/预算/不限速R48功能262 READY＋238 session-only通过，strict **3358.394ms** 未通过2.2秒，ACK→READY **496.767ms** 刚在500ms内，EXACT **3171.358ms** 未通过；source worker墙钟2941.140ms。未证明整体收益，不能仅凭open/close减少宣称优化，也不能用一轮数据证明全部退化由该改动引起。用户要求回退后已精确撤回本轮5个内核文件的增量及4个试验专用测试文件；R49在业务约85秒时中断、未进入DRAIN，不计验收。Release重建SHA恢复为`61b78ba69ad2ceea19c60fdaf2665da634901ede52fb643b80f922c6bbd3ec41`，与R29完全相同，benchmark及10个依赖SHA也相同。Debug/Release重建和回退后的cursor_final_transfer_chunks通过。R29五轮R40–R44仍是3/5双指标通过，回退不等于长尾已解决。未提交或push。[试验与回退证据](../../../build-release/continuous-resource/file-reuse-e92/README.md)。

候选只改 `preserve_trx_file.*`、`preserve_trx_receiver_retired.cc` 与 `preserve_trx_transfer.*`，相对本轮备份共+126/-19行。writer由既有ticket持有，CHUNK复用句柄，SEAL同步关闭检查再coverage/hash；退役借用者归零后沿已有reaper锁外close并退账。FD预留失败回原逐帧路径，未修改速率、预算或worker。

不将pwrite换成write：CHUNK携带非零offset，重复/部分重叠校验会改变FD当前位置，直接write不能保持语义；本次诊断也在普通write上看到长调用。活跃writer的close错误必须在可发布成功之前结算，不能简单后台丢弃；终态异步清理由原有机制承担。

源码独立review补齐O_APPEND及可选lease分配异常回退。候选Debug/Release构建通过；6项log-bin与4项no-bin既有业务MTR、2项OFF用例通过（shutdown另列）。新增wire生命周期正常模式验证了multi-chunk/部分重叠后即时字节和长度、断线续传、range同inode truncate/unlink重建、SEAL后重传、冲突CORRUPT、未完成对象ABANDON、resident TEMP事务与回滚、额度及Open_files回落。增强实际FD断言后，allocation-fault模式失败：标志未作用于实际worker，不能声称bad_alloc运行覆盖。该夹具问题未用内核修改掩盖；候选与试验文件均已归档并从活动源码撤回。

Phase1静态核查：initial_pending与deferred_capture_targets会暂缓已完成owner的后续代；正常首次门槛结束后仍有stop_purge之后、T0之前的有限末采样窗口，但不能直接重调现有初次门槛或temp_owner.capture（会新建TEMP工作）。公平常刷会无限累积历史RESULT；一次全新100结果批约224934600B（214.514MiB，1GiB的20.949%），若后来再换代仍需final当前代额度。source没有现成receiver余量/末代预留事实，DECLARE拒绝属于fatal，不能按optional skip处理。因此本轮没有加入额外采样、调度或新生命周期机制。

当前回退仅恢复本轮开始的内核状态，保留其他既有未提交内容。候选、日志和R48/R49报告留在ignored build目录；不将它们作为交付代码或已通过性能证据。

## E91：worker/staging长尾分层定位（未实施新优化）

R43的2778.728ms不是纯worker CPU时间，计时范围还包含协调器逐token的TEMP/RESULT staging、wave flush及join。保持原500连接/300秒/双端1GiB/不限速和原脚本及10项依赖SHA，仅增加可撤回的计时探针；R45–R47全部功能通过：

| 诊断轮 | strict ms | ACK→READY ms | worker ms | 串行STAGE ms | final RESULT |
| --- | ---: | ---: | ---: | ---: | ---: |
| R45 | 4013.580 | 162.225 | 3798.581 | 3747.862 | 73份 / 164202309B |
| R46 | 2055.912 | 158.419 | 1873.204 | 1804.029 | 49份 / 110218101B |
| R47 | 2033.606 | 150.914 | 1927.497 | 1918.340 | 72份 / 161952954B |

R45全部STAGE在源端同一THD内串行无重叠，覆盖worker98.665%；TEMP1526.371ms、RESULT2218.988ms，wave两处flush共4.603ms。精确seq匹配证明：source SEAL12044 RPC447.659ms，receiver同THD的前CHUNK12043 ACK后apply447.635ms，其中file447.560ms；当前SEAL自身apply仅8.601ms。receiver在admission ACK后仍同步等待apply结束，该后端才能处理下一命令，反压沿同步请求返回source coordinator。不能把它标为纯网络/当前SEAL时间。

R46拆开目录、对象分片锁、open/stat/overlap/pwrite/close/range，≥20ms的26条长IO中dir最大27us、对象锁最大1us，长停顿在write/close/range.write；不扩大排除到所有短样本。R47进一步区分my_close文件名/注销登记/POSIX close：≥20ms的23条累计4747.926ms，其中syscall4747.591ms，最慢close848.919ms与登记3us。R47上述23条全部在worker开始前完成，是提前阶段的并发累计，不能倒填成Phase2墙钟。10ms间隔20秒receiver采样也落在close/write/pwrite，并看到prewarm flush的fsync，但没有逐事件因果证据证明某次fsync导致另一close；本机OS文件系统/写回/存储/调度层尚未唯一归因，不能直接删持久化或新增FD缓存机制。

R45 final73份全部为一份完整当前代（DECLARE+3CHUNK+SEAL），另外27份零发送；73个会话EOF都在DRAIN期间推进，42个从日志直接见到前后不同代次，剩余31个缺旧代日志。pin幂等与offset续传仍在，不支持同代进度丢失重复全传。early_reused=100也包含closing刚准备好的代，不能当成100份在Phase1完成；ordinary仅安全点采样、pending/awaiting期间不取新代。尚缺每代产生→安全点→pin→发送的完整时间线，不能断言这些新代均无法前移。

三轮仅诊断，R47还有采样，不加入E89/E90无探针正式通过率，不能将R46/R47处于2.2秒内当作已修复。没有改预算、worker、ACK、摘要或持久化。全部临时探针从备份精确还原（含mysys/my_open.cc），强制重建后的Release SHA与R29相同：`61b78ba69ad2ceea19c60fdaf2665da634901ede52fb643b80f922c6bbd3ec41`；对比本轮开始的tracked diff仅Git对象ID缩写长度不同，内容一致。原close诊断打印不作为失败errno传播验证，patch只作历史取证，未进入产品实现。未提交或push。

[完整诊断与源码链](../../../build-release/continuous-resource/worker-window-e91/README.md)、[可重算数据](../../../build-release/continuous-resource/worker-window-e91/report.json)、[还原核验](../../../build-release/continuous-resource/worker-window-e91/restoration-verification.json)。后续优先核定当前代进入closing前的欠账，以及如何降低/交错文件staging成本；在RED→GREEN证据前不新增并发机制或把关闭成本简单挪到SEAL。

## E90：R29同版本再跑五轮（长尾再次复现，未实施新优化）

保持R29二进制、主benchmark及10个依赖SHA、500连接、300秒持续业务、原1GiB预算与不限速，按顺序完成R40–R44。测试前后tracked diff相同，之后才更新文档；没有构建或并行负载，没有修改内核/驱动/门槛。

| 轮次 | READY/session-only | strict ms | ACK→READY ms | 原2秒/本轮2.2秒双指标 | 独立EXACT |
| --- | --- | ---: | ---: | --- | --- |
| R40 | 438/62 | 1389.380 | 151.931 | PASS/PASS | 1386.128ms，超限 |
| R41 | 412/88 | 2963.767 | 164.849 | FAIL/FAIL | 2961.671ms，超限 |
| R42 | 455/45 | 1889.680 | 192.163 | PASS/PASS | NA/NO_ELIGIBLE_BODY |
| R43 | 442/58 | 2957.639 | 182.981 | FAIL/FAIL | NA/NO_ELIGIBLE_BODY |
| R44 | 469/31 | 1694.605 | 362.277 | PASS/PASS | NA/NO_ELIGIBLE_BODY |

五轮功能、原500连接身份、已提交RW账本与receiver原TEMP隔离均通过；业务错误/未分类/采集错误均0，ACK五轮通过。双指标按原2秒及用户确认2.2秒均3/5通过。strict中位1889.680ms、最大2963.767ms；ACK中位182.981ms、最大362.277ms。合并E89后八轮双指标6/8通过，strict中位1861.749ms；样本通过数不等于长期稳定性概率，不以八轮计算稳定性p99。

**可确认的位置：** R41/R43的source target-worker墙钟分别2789.278/2778.728ms，COMMIT阶段仅78.808/72.813ms。R41比R40的strict多1574.387ms、target-worker窗口多1592.817ms，ACK→READY仍164.849ms；超限主要发生于包含并行准备、协调器staging及等待的源端窗口。R40/R41 TEMP回退均3次，R41 TEMP FINAL累计809.373ms还低于R40的905.652ms；R43为877.642ms。不能据累计timer或全DRAIN RESULT字节归因最终切片的fsync、发送、TEMP或调度，内部关键路径仍需后续独立取证。

**结论：R29仍有近3秒长尾，回退到它不能保证strict≤2.2秒。** 不以先前三轮绿色或本次ACK全通过关闭整体性能；保持版本冻结完成用户要求的复测，没有穿插未经证实的优化，V03仍开放。

R41首个attempt在receiver39112端口bind预检退出，未建history、未启动实例/业务；保留失败日志，外层runner仅在下一轮前等待原端口可用，补跑R41后共五轮正式运行。未观察到残留listener，不足以确证TIME_WAIT根因。五份原benchmark均退出1（EXACT超限或NA，R41/R43另有strict超限）；外层runner退出0只表示五轮执行完毕。source停机既有purge断言按用户范围排除，不冒称全部正常退出。未进行真实外部升主/SQL RESUME验收，未提交或push。

详见[五轮及合并八轮报告](../../../build-release/continuous-resource/r29-repeat-r40-r44/README.md)，包含逐轮原始JSON、阶段/业务/内存指标、预检失败记录、输入快照与汇总代码。

## E89：R29基线三轮正式复测（不新增内核修改）

用户接受本轮 strict Phase2≤2200ms，ACK→READY仍严格<500ms；原2000ms与独立EXACT≤500ms分别保留。当前Release SHA256 `61b78ba69ad2ceea19c60fdaf2665da634901ede52fb643b80f922c6bbd3ec41` 与R29/R30/R33相同，故不再做Git回退。冻结二进制、脚本及10个Python依赖、相同配置与预算后，按顺序运行三次500连接/300秒业务；业务在DRAIN中持续至实际4020，原连接保持到READY。无并行构建/MTR/重型采样。

| 轮次 | READY/session-only | strict ms | ACK→READY ms | 原2秒/本轮2.2秒双指标 | 独立EXACT与原脚本 |
| --- | --- | ---: | ---: | --- | --- |
| R37 | 446/54 | 1833.818 | 162.896 | PASS/PASS | 1832.567ms超500ms，原FAIL |
| R38 | 420/80 | 1694.442 | 195.604 | PASS/PASS | 1689.378ms超500ms，原FAIL |
| R39 | 483/17 | 1935.036 | 243.462 | PASS/PASS | NA/NO_ELIGIBLE_BODY，原FAIL |

三轮功能、500原连接、已提交RW账本及receiver原TEMP隔离均通过；业务错误/未分类均0，无采集/observer异常。strict中位1833.818ms、最大1935.036ms；ACK中位195.604ms、最大243.462ms。Phase1到请求停purge为14.327/13.700/13.307秒；TEMP最终复用98/99/96，回退2/1/4。计费内存预算未变、实际工作量随命令边界而变，不能当成完全相同的final切片；TEMP累计计时不是关键路径墙钟。source在测试结束SHUTDOWN时均触发用户已排除的既有purge停止断言，不记为所有进程正常退出。

**决定：继续保留R29/E84基线，E85/E87继续撤回，不叠加无收益证据的修改。** 本次三轮两个关键指标均通过，且不需要2–2.2秒容忍区间；同版本历史R30的2270.173ms、R33的4077.194ms仍是有效失败记录，不能宣称长尾根因已修复或全归于后来代码。原脚本FAIL、外部升主/回放/SQL RESUME未验收、R24独立binlog provider问题均不由此关闭。三次样本不计算稳定性p99。

完整阶段、工作量、业务延迟与内存见[复测报告](../../../build-release/continuous-resource/r29-repeat-r37-r39/README.md)，原始逐轮JSON、同钟复算、输入快照和汇总代码均保留。只更新文档与报告，未修改内核/驱动、未提交或push。

## E88：最终RESULT取证（未改协议，性能仍开放）

撤回E87后，以E84加临时日志运行R36（500/120秒，仅诊断）：448 READY+52 session-only、错误/未分类0；strict1920.659ms、ACK→READY149.974ms，EXACT无合格命令。最终选中100份RESULT，61份ordinary已发完，剩余39份均为选中的新代且未DECLARE，共87,724,611B；没有旧代补传。staging的TEMP575.556ms、RESULT617.025ms。不能据此修改累计RESULT或退休合同；对已声明旧代的本地跳过会违反现有source与receiver最终清单要求。

另核实writer checkpoint_result原已接受BASE/DELTA logical SHA，实际write/resize才失效；无写缓存命中只验文件身份，未发现可再省的一次完整hash。普通blob batch固定EXTERNAL_BLOB/整份输入，不能直接套RESULT/ACK offset；合并首末frame需新的session状态接入，最多省78次同步交换，缺乏收益证据，暂未实施。R35源端3次fallback的原因尚无token级证据，长CLOSE/DIGEST与fallback不能划等号。临时诊断已从内核删除，patch和报告保留；恢复后Release SHA256为61b78ba69ad2ceea19c60fdaf2665da634901ede52fb643b80f922c6bbd3ec41，与R29/R30/R33逐字节一致，维持E84代码，不叠加未经证实的优化。R35正式strict仍失败，不用R36诊断通过替代验收。

## E86定位与E87单次解码复用（未证明收益，已撤回）

R34保留500连接/分组/预算，业务120秒，仅增加临时分段计时，没有sample；属于诊断，不能替代300秒验收。439 READY+61 session-only、业务及未分类错误0，strict2252.242ms失败，receiver ACK→READY285.510ms通过，EXACT无合格命令。439个最终staging调用均在Phase2内：RESULT累计1304.226ms，TEMP累计635.100ms，二者以外2.654ms。四个wave的首末flush合计3.138ms；receiver COMMIT sequence、前序apply与admission前门槛合计4us。本轮主要工作在源端串行staging，不能用它解释或排除R32独立的COMMIT长等待。临时日志已从内核删除，原patch和日志保留于R34目录。

E87只在`connection_index_for_frame`和`verify_transfer_frame_ack`内复用本次单帧解码结果：每处由两次完整decode降为一次，减少重复摘要、分配和payload复制。batch原路径、控制帧/普通token路由、空输入错误类型、ACK完整原始帧SHA、CRC、epoch/sequence/nonce、版本、契约和retention检查全部保留。不复用跨调用对象，不增加缓存、锁、worker、协议或预算，也不减少同步RTT。独立只读review没有发现阻断问题。Debug/Release构建通过，14个不同定向业务MTR全部通过（log-bin5个、no-bin9个，shutdown两轮另列），涵盖大RESULT非零偏移丢ACK重放、换代、final DATA、查询ACK丢失、契约拒绝、语义拒绝和OFF；未运行新UT。原500/300秒R35完成444 READY+56 session-only、业务/未分类错误0；strict4169.042ms失败、ACK→READY77.144ms通过、EXACT无合格命令。target-worker墙钟3841.346ms，TEMP FINAL累计3046.562ms。E87未证明整体收益，已精确撤回，候选patch归档于R35目录。R33原E84也为4077.194ms，不能将整体长尾唯一归因于E87。E85单帧inline试验继续保持撤回。

## E85：单帧直接执行试验已撤回（未证明整体收益）

R31同E84二进制诊断438 READY+62 session-only，strict3876.214ms、ACK458.836ms，EXACT为NA；双端sample运行，不作正式性能验收。CLOSE59.125ms/SEAL37.736ms未复现R30的长调用，因此不改目录或数据持久化。staging341样本中TEMP206、RESULT135；receiver三个连接的join共209样本，包含实际apply等待，不全是线程调度。

源码证实worker_count>1且只有一帧时仍创建一个临时线程并join。仅扩展既有inline条件为worker_count<=1或单帧，保留callback、异常/首错、sequence TLS guard、admission ACK和COMMIT屏障；多帧和原生prewarm pool不变。不增加新计数/探针/线程池。既有固定负载R30是性能失败证据，采样及源码确认重复调度；尚无直接断言临时线程创建次数的MTR，不能以receiver_worker_active/count代替。Debug/Release构建通过。11个不同定向MTR业务用例通过（4个log-bin、7个no-bin），shutdown单列；首轮7个用例因binlog模式跳过，已用明确skip-log-bin配置逐一补跑，不将skip计通过。覆盖单帧RESULT丢ACK精确重放、TEMP、两token batch身份、认证语义失败、COMMIT拒绝与OFF隔离；原500/300秒R32功能430 READY+70 session-only通过，但strict4880.141ms失败、ACK→READY28.911ms通过；EXACT4878.842ms失败。target-worker墙钟4026.440ms，final metadata ACK740.317ms（R29/R30为36.102/35.427ms），source TEMP FINAL累计760.862ms，CLOSE/SEAL没有长调用。因此不能把更小ACK→READY当整体收益，也不能把全增长唯一归于此单行条件。已精确撤回新增的单帧条件及两行注释，保留其他既有改动；重建E84的SHA与R29/R30完全相同；R33原配置316 READY+184 session-only功能通过，strict4077.194ms仍失败、ACK18.347ms通过，EXACT4073.396ms失败。因此E85不能解释全部波动，但仍因未证明收益保持撤回。R34只补临时分段日志，诊断后删除，不叠加并发改动。没有引入内存DELTA或延迟目录fsync。

## E84：最终 RESULT 分块的窄修复（性能复验中）

同二进制 R28（500/120秒诊断，带采样）447 READY+53 session-only，strict2839.233ms失败、ACK→READY132.041ms通过，EXACT无合格样本。source coordinator的staging共292个样本，RESULT194、TEMP98；RESULT中108个落在CHUNK等待ACK，24个pread，另有摘要计算。final candidate仅7个样本，故没有实施新的candidate合批API。样本比例不是精确墙钟比例，不能据此将R27全部退化归于某一调用。

源码确认最终RESULT在finish和step双重限制64KiB，即使协商chunk为1MiB也逐64KiB同步请求。仅改finish使用现有协商量子，step尊重调用预算/协商量子/剩余长度三者最小值；Phase1调用显式保留64KiB。scratch先申请原小量子，再以既有lease.grow_to申请更大量子；不够时在read/wire前保留小量子。没有调整预算、线程、receiver并发、ACK或文件持久化协议。

新增cursor_final_transfer_chunks通过普通连接生成64行约4MiB结果，取前7行，真实relay丢掉非零偏移的成功ACK，验证完全相同的帧重发和后续offset连续；READY后本地SQL RESUME、外部PREPARE替身及attach、剩余FETCH逐字节比较和TEMP回滚。旧内核有效RED：上述功能断言通过，但65个CHUNK违反协商量子断言。第一次夹具误用本地bridge不支持的永久表事务，RESUME4013不计RED；改为临时表RC事务后的第二次才有效。该用例是FINAL内重连，不称为Phase1到FINAL断点覆盖；大scratch额度失败分支目前只有静态复核，未宣称故障覆盖。

Debug/Release构建通过。新用例GREEN将65个CHUNK降为5个（4,196,701B），在1MiB非零offset处丢ACK后精确重放再续传，全部功能断言通过。首轮GREEN仅因有意断连的MY-001158未抑制而被MTR标失败，采用原UNDO ACK重试测试相同的窄抑制后重跑通过；原UNDO重试也通过。加上8个相邻业务用例，10个不同MTR业务用例通过，shutdown单列。原500/300秒R29功能434 READY+66 session-only通过；strict1760.052ms、ACK→READY198.960ms同轮通过，原EXACT1756.549ms仍失败。全程RESULT约299.16MB/133代与R27几乎相同，所有CHUNK请求9852降至8278，source target-worker墙钟3044.310降至1608.081ms。单轮非配对数据，不宣布稳定性或全功能闭环；原配置R30已完成449 READY+51 session-only、业务和未分类错误0；strict2270.173ms失败、ACK→READY138.601ms通过，EXACT2269.235ms仍失败。两轮共同门槛1过1败，不能宣布稳定达标。R30全程RESULT字节更少，但source FINAL累计2068.471ms、CLOSE471.009ms、SEAL449.982ms，相比R29的712.392/51.016/41.548ms明显增加；累计timer不能直接当关键路径墙钟。暂停叠加改动，以同二进制R31采样区分文件/目录同步与传输等待。业务吞吐/延迟原始波动保留在R29报告，未改预算。

## E83：最终清单沿原 candidate 通道提前准备

E82 原500/300秒 R26 功能通过：453 READY+47 session-only，strict1611.498ms、ACK→READY1532.279ms，后者仍失败；Phase1到purge stop请求13.773322秒。source FINAL累计641.193ms、CLOSE45.375ms，receiver staged累计4608.069ms、最大active=3。R21两项分别1825.504ms/2076.796ms，单轮下降不能当稳定收益证明。

源码确认 final DATA/UNDO 先发送，但其 manifest 之前要等 deferred-finalize和最终metadata批才到，receiver无法仅凭页字节准备最终代。E83在原temp_transfer_stream完成依赖SEAL及metadata/TLV同步之后，通过既有.tempts.manifest通道发送同一份payload原文；没有新帧/队列/线程/外部阶段。Phase1与final共用一个分步发送函数，避免两份声明、进度、CHUNK、SEAL逻辑。最终授权仍在原take_temp的精确payload检查；working返回WAIT，partial work可由FINAL继续。

register遇到有效working或partial work时保留旧slot，放弃此次可选刷新，防止丢弃已经部分接管的donor。prepared(new_id)返回UNAVAILABLE而非无限等待。FINAL BEGIN虽不选择candidate工件，resource_candidates和input的shared owner仍保活文件与额度，最终释放沿原退休链。额外scratch申请失败发生在新candidate DECLARE前，安全保留原final路径；开始wire后任何错误继续fail。DATA传输chunk先释放，再使用同份scratch预算发送manifest，避免同时存在两份chunk未计费。

有效RED：旧内核的native_final_tail_cross完成DRAIN SUCCESS，但未出现final candidate。新流程在真实SEAL ACK被扣住时完成receiver准备，断言source final已执行，排除普通refresh；FINAL后native_reused恰好+1且DATA写入、stats读取、DD调用不再增加。cross验证传输/READY，配套loopback验证真实SQL RESUME和DML/ROLLBACK，不宣称外部物理集成。原SHA、BASE+DELTA、目标ID和数据断言保留。

11个业务MTR通过；scratch修正后追加cross小尾部、无baseline sparse final和COMMIT DATA三项通过；另新增resource-only final checkpoint及补强initial COMMIT两项通过，合计13个不同业务用例，shutdown单列。resource-only用例要求COMMIT后最终候选在FINAL metadata前准备完成，再SQL RESUME/新事务更新/ROLLBACK并核查资源归零。忙旧slot和scratch申请失败尚无专门运行故障覆盖，保留静态审核边界。Debug/Release构建通过。原500/300秒R27功能通过：436 READY+64 session-only，业务/未分类错误0。ACK→READY293.310ms通过，但strict3267.422ms与EXACT3266.589ms失败；Phase1至purge stop18.268515秒。target worker墙钟3044.310ms（R26为1453.053ms），解释大部分strict增长的位置；最终metadata ACK72.107ms、COMMIT阶段103.947ms。source TEMP FINAL累计863.345ms，不能与worker墙钟直接相加或相减。candidate每个小manifest新增3次同步请求已由源码证实，但当前全DRAIN汇总不能证明其为增长的唯一根因。停止叠加修改，用同二进制R28的120秒诊断采样核定；不把ACK达标当作整体优化成功。

## E82：首次 DATA 跨 COMMIT 与共同验收门槛

保持 strict Phase2 ≤2000ms、receiver 同钟 ACK→READY 严格 <500ms，两者共同验收；不调整预算、分组、稳态时长或停止业务方式。原 EXACT 末命令指标独立报告，NA 不作通过。正式负载不并行构建、MTR 或采样。

R22/R23 均为 500/120 秒诊断。R22 receiver 正式 prepare 的 889 个叶子样本中 fresh mkdir 408，并无 directory fsync 样本；R23 四个 fresh token 没有 take/failed 记录，不能归为 donor 换代丢失。R25 加 source 初次 gate 日志后，57/58/150/151 明确是成功回存 DATA、initial=0/candidate=0，却因统一 Install::STALE 完成门槛；60 属于其他 STALE，未扩大修复。R25 功能 422 READY+78 session-only 通过，strict 2336.085ms、ACK→READY 1502.648ms 仍失败。原始证据见 build-release/continuous-resource 内 R22–R25 报告及 R25 保存的诊断源码。

最小修复只区分 DONE 且至少一个有效连续 DATA 实际回存的 DATA_RETAINED。consume 在 install 移动 sidecar 前读取 initial_baseline_complete；首次 checkpoint 尚未尝试时不置完成，让下一个 idle 边界继续 DATA+当前 UNDO 捕获。已完成的初次门槛不重开；普通 STALE、OOM、quota、取消及 unsupported 保持旧退路。未增加锁、队列、worker 或外部阶段；全部 TEMP_DIAG 已移除。

新增 temp_capture_initial_commit 使用真实首 UNDO SEAL ACK、真实 User lock 和单个 COM_QUERY 内 COMMIT/BEGIN/UPDATE 固定时序；旧内核未发送首候选便完成 DRAIN SUCCESS，RED2 命中功能断言，不是超时。RED1 因测试未开启多语句未到 User lock，已纠正，不计有效 RED。修复后新用例及 COMMIT DATA、cross final tail、rollback、quota/cancel、sparse、metrics OFF 共 8 业务 MTR 通过，shutdown 单列。256 行全部值和 payload、SQL RESUME 后新事务更新及 ROLLBACK 回到 COMMIT 保存值均核验。之后补强锁期限与资源归零断言，待单独复验。Debug/Release 构建成功；原 500/300 秒 R26 正在复测，尚不能宣布性能完成。

R24 另在 Phase1 457.139ms 返回 4013，binlog_provider_prepare_failed，未到 HARD/CLOSING/FINAL ACK；TEMP 已做 6 个 source baseline，receiver native 尚未开始。静态确认历史 finish_initial_prefix 把 source reset 导致的 degraded 统一作 ERROR，而邻近 generation 检查返回 STALE；现场还缺失败 target/子阶段，不能宣布唯一命中。abort_after_sender_join 返回历史 m_failed，使 cleanup failed 文案不能证明新清理错误。相关代码来自旧提交，本轮未修，保留为独立待定位项。

R24后续只读审查补充：bounded begin不持目标THD命令锁，copy仅在单次source_copy_range持storage mutex，finish直接访问session共享字段。可确定构造的分支是copy释放storage锁后真实COMMIT/reset以storage→lease锁detach，worker随后获取同一lease锁调用write_at却因owner已空返回普通源写的OK，误推进prefix再在finish返回ERROR；该顺序有mutex同步，不依赖未定义的竞态读值。但原R24没有target/子阶段日志，仍不能把它当现场唯一根因。另有write返回后reset与finish共享字段并发的静态数据竞争风险。后续修复应复用lease锁覆盖worker专属owner检查、prefix写入及finish，共享字段不能仅改错误枚举；source_copy_range保持锁外以避免lease→storage逆序。纯source失效与IO/digest/budget/cleanup错误必须区分且hard error优先，不可一律ERROR→STALE。普通COMMIT的有界MTR应在copy后、lease写入前固定窗口，不使用DEBUG_SYNC。上述仅方案，当前性能复验没有叠加binlog修改。

下一项仍是提案：final DATA/UNDO 已 SEAL 且 compaction 同时更新 metadata/TLV 后，复用现有 manifest candidate 协议发送最终精确 payload，让现有 receiver worker 提前准备，FINAL 仍由原接口授权接管。不能延后 ACK 掩盖 READY 尾部。必须先保护尚在运行或部分完成的旧 slot，避免新候选导致重复创建；无实测及 RED/GREEN 前不实施并发扩张或跳过 DATA/UNDO 转换。

2026-10-06。承接 mixed500-r2 已验证的首错和源码链；保持500模型、服务端预算和既有worker，不新增RESET DRAIN、PS迁移或升主阶段。用户已授权继续实施；不提交。

## 顺序与验证

- [x] 新增普通MTR调用的Python线协议负例：真实声明大于receiver inflight预算的TEMP对象，确认admission先成功、后续进度查询/新数据/COMMIT能取得认证语义错误；相同请求重传与ABORT不被新门槛误伤，缺序号及时失败，ABANDON最后取得NOT_COMMITTED_CLEAN，原有TEMP/DML/ROLLBACK不受影响。已保留旧Debug RED、修复后GREEN。
- [x] 在 `sql/preserve_trx_transfer.cc/.h` 复用现有epoch最早apply失败记录和认证ACK；新增行为使用既有错误码1–5，不增加帧类型/线程。错误回复仍验证nonce/请求摘要，普通SQL错误或传输断线不冒充认证拒绝。新旧查询实现的混合版本兼容未验收，双端应同步移植。
- [x] `sql/preserve_trx_temp_prebuild.cc`、`sql/preserve_trx_result_pretransfer.cc` 区分候选可选不可用与致命失败。TEMP发送错误在BUSY/STALE安装检查前上报。COMMIT失败后沿已有查询/放弃路径取得可信清理证明，保留有界重试和fence；COMMIT前的早失败不混同所有权不确定。
- [x] Debug/Release构建通过，13个不同定向MTR业务用例实际通过，shutdown单列。真实source TEMP/RESULT用例固定32768B receiver负例预算，旧Release在closing阶段断言RED；新Debug在Phase1停止，原TEMP/FETCH位置、回滚与在途账目归零通过。这个负例参数不修改原500模型预算。
- [x] 首次RESULT完成单向；首次TEMP以一个bool保留该pending候选是否完成首次checkpoint，查询结束后才单向完成。后续pending归既有`complete()`排空，不重开首次门槛。既有持续捕获/游标换代/CLOSE/ACK丢失用例通过；原500性能仍单独验收。
- [ ] 当前集合/历史对象逐owner分项尚未完成。已精确确认r4首拒绝为1GiB inflight耗尽、cleanup debt=0，非RAM分配失败。当前唯一集合尺度估计约1.186GiB；旧文件仍被prepared/worker使用时不能减账，不能仅修历史累积便宣称容量通过。
- [x] Release复测原500模型r3/r4。r4在17.332秒Phase1返回4013，未进入closing/COMMIT/READY；没有有效strict/EXACT/ACK→READY成功样本，性能及功能仍失败。500账本、末尾指标及输入SHA完整，详见[r4完整报告](../../../build-release/continuous-resource/mixed500-r4/README.md)。

使用systematic-debugging、test-driven-development、writing-plans与verification-before-completion流程；本项目用MTR/Python E2E承载RED/GREEN，不新增UT/GUnit或DEBUG_SYNC。主agent修改和验证，subagent仅独立只读review。

## 当前集合容量修复与最终复用

原500/r4为不变预算的RED。另用相同4096行、512B值的20连接诊断取得四份完整BASE：每份10MiB，实际全零4KiB块占73.59–74.38%；按既有DELTA记录布局可缩至2,694,964–2,777,124B。证据在`build-release/continuous-resource/smoke20-image-profile/image-statistics.json`。该诊断不是500性能验收。

选择仅省略实际零字节的稀疏BASE，不按FSP空闲标记删页、不截短逻辑文件、不增加压缩库工作区。复用现有DELTA编解码、Overlay和worker预算；新增明确magic及manifest v13。wire BASE用稳定`.image.sparse`槽位，size/hash认证实际编码内容；logical image仍保留原长度/完整SHA256，源侧原始BASE FD继续用于后续DELTA比较。编码无收益时在DECLARE前回退raw。

receiver先完整验证稀疏BASE的有序记录、范围、终止符、逻辑长度及完整SHA，再发布缺块为零的不可变视图；若另有DELTA，继续验证其logical BASE身份和physical wire BASE身份。所有步骤在既有提前准备阶段完成，不向promotion/RESUME转移数据量工作。索引内存、实际文件、原生目标文件和旧引用均沿原有账目；不能以READY为由提前减账。

验证包括新编码确实使用、完整TEMP导入/数据与回滚、BASE+DELTA、更换/撤销、错误输入及原raw回退路径；随后原500模型再次验收。历史RESULT仍被累积保留，不能据稀疏比例宣称整个模型必然通过；若复测继续暴露历史活对象积压，再按最终选择与最后读者的证据处理。

原500的r5已越过Phase1配额失败，约14.598秒进入closing，但最终传输仍触顶。100个TEMP owner中35个复用、65个完整回退；仅提前编码不足以关闭容量问题，详见[r5记录](../../../build-release/continuous-resource/mixed500-r5/README.md)。已将同一编码延伸到既有最终sidecar发送入口：所有对象SEAL后，同时替换portable bundle的metadata与权威TEMP TLV；分配失败不得半更新，源原始文件和原逻辑描述符不变。编码同步核对原logical SHA，每个工作批次检查既有transport取消/期限事实。SKIP保留raw；编码开始后确定I/O/摘要错误失败收口，不继续发布。

新增`temp_capture_sparse_final`通过LEGACY_SERIAL明确绕过Phase1异步预传，验证最终raw回退路径。首次实跑已到READY，随后本地Debug SQL桥的原清单字节相等检查拒绝；桥已改为只归一化DATA wire BASE/DELTA引用，仍逐字节比较编码后的完整逻辑清单（包含size/hash、事务归属及undo），生产RESUME不放宽任何授权。最终复验结果单独记录。

### 最终准备复用：已取得定向 RED→GREEN

原500/r6已完成DRAIN与COMMITTED_HANDOFF，408个survivor全部READY；无工件预算拒绝。预算仍为1GiB，最终在途工件565,823,696B。严格Phase2为7,365.914ms，receiver ACK→READY为5,662.914ms，仍超标；43个空BEGIN连接未被交接集合分类，完整功能也未通过。详见[r6报告](../../../build-release/continuous-resource/mixed500-r6/README.md)。

源码确认`take_temp()`遇到final manifest不相同就丢弃提前准备的work，而Phase1换代已有严格的`begin_import(previous)`。现已接通final同一路径：锁内等待旧worker结束、标记final_claimed并独占移走slot；只提供ready/preprepared且未失败的donor，不授予final权限。最终输入仍独立认证；现有lineage、owner、undo前缀、结构及未发布检查不变。跨COMMIT不兼容仍fresh，提供donor本身不增加exact复用计数。

新`temp_capture_native_final_tail`使用真实SQL与既有SEAL ACK屏障：提前准备后单行UPDATE，再DRAIN/READY、RESUME、FETCH和ROLLBACK。旧代码有效RED将DATA完整重写75,497,472B；修复后仅写32,768B，目标space/table/index ID不变。变更undo后允许重新认证，兼容代次复用仍需要统计扫描和SQL定义解码，不能沿用静态exact候选“零stats/DD”预期。原用例断言保持原样。

Debug/Release构建通过；log-bin六个业务用例通过（final tail、exact early、cross、rollback、undo abandon、digest），需要no-bin的pipeline cancel另跑通过；shutdown单列。证据在`build-debug/sparse-fix/reports/mtr-final-refresh-{red3,green,cancel}`。该局部改善仍有全量源验证、页转换和比较，不能描述为只处理增量；原500/r7按相同配置复测，结果另记。

### 跨 COMMIT 的 DATA 保留（定向通过，原500复测中）

原普通成功COMMIT会销毁整个participant，而表仍存活；在途job也临时独占DATA sidecar并受事务cookie约束。新实现只在standby、native namespace、BOUNDED_PIPELINE及本轮TEMP capture epoch下保留纯DATA；完整回滚、隐式DDL提交、旧串行模式继续原清理路径。不是简单放宽现有trx cookie/version检查。

- 显式成功COMMIT保留当前DATA注册、capture floor、页版本及table ordinal；清空旧事务undo owner、undo sidecar、journal/DML历史，并递增transaction_generation和history fence。完整ROLLBACK、提交式ALTER/TRUNCATE/RENAME仍走原失效路径。
- 事务journal/DML摘要、DROP证明、undo owner/sidecar必须清理；history序号、table ordinal和页注册不可复位。resource-only→有事务→resource-only还需单调事务边界识别，不能只检查当前“无引擎”造成ABA。
- worker借用或回存DATA时必须证明同一owner/participant及data generation，且COPY/ROUND已经完整结束；取消的frozen round会使stream失效，不得留下可复用的半镜像。所有清理都等最后使用者退出。
- 在LOCK_thd_data内重新检查实际TEMP capture bit，再取得participant state锁；不能用batch generation OR放行取消后的COMMIT。移走的native/file owner在两锁退出后析构。旧串行prebuild不具备相同安装协议，故不启用该复用。
- 完成的跨COMMIT job仅能回存同owner/participant、data_generation、仍有效注册且不含undo的DATA；旧UNDO与候选不发布。transaction_generation明确识别resource-only ABA，不仅比较当前是否无引擎。此分支已静态独立review，专门ABA运行覆盖仍待补。
- 四owner真实undo SEAL ACK屏障内COMMIT A、开启B、执行DML，再DRAIN/READY/RESUME；验证B可见，后续ROLLBACK精确回到A而非A之前。旧代码有效RED为finalfallback=1，新代码0。最终读取13,140,512B、写754,208B（并行MTR中累计22,678us，不作性能验收）。Debug/Release构建成功，10个定向业务用例通过，shutdown单列；原500/r10已完成：DATA99复用/1回退，final写22,050,767B；strict4049.492ms、ACK→READY7783.881ms仍失败。receiver NATIVE累计12.451秒，继续做分项调用栈诊断，不宣称整体达标。

### receiver新文件全零页跳写（定向通过）

R9调用栈在receiver prepare_work::step分支采样中，796/1124个叶子落在双份pwrite；这是该分支样本比例，不是CPU或墙钟占比。稀疏wire展开后原逻辑仍双写全部页。现仅对previous_bytes=0的新私有文件，在完成原read/校验/重写/checksum之后跳过实际全零非末页；最后一页必写保持精确EOF。每页仍进完整SHA，import/统计/SEAL不变。donor仍比较并覆盖，包括原非零变零；不按allocated标志删页。

稀疏BASE用例从独立解码的source页计算可省写字节：旧代码写75,497,472B，逻辑37,748,736B中非末页全零11,403,264B，违反双份上限52,690,944B；修复后通过。跨代、spill、LOB、final小尾部、ROLLBACK、取消和OFF一并通过。文件租约仍按完整逻辑两份保守预留，洞不提前减账；后续填洞仍可能保留pending credit，这是保守预留而非超配，未为此增加记账机制。


## 先前定向验证与纠正

- `mtr-semantic-red` 的query/admission为有效RED；首次commit负例有缺少terminal身份的脚本错误，修正后的`mtr-semantic-red-commit`才是有效COMMIT RED。
- 旧source负例最初只要求15秒内4013、原事务可继续和在途账目归零，旧版也通过；日志证明已进入closing，不能据此宣称已测到修复。增加本轮receiver特定token的DECLARE RESOURCE_EXHAUSTED日志及重启后closing前后均0后，`mtr-semantic-source-red3`两例有效RED。
- 最终`mtr-semantic-gate-green2`七个业务用例全通过，含retry/gap/ABORT、真实DRAIN、原temp-id和epoch撤销；`mtr-semantic-gate-logbin`五个资源用例通过，binlog truncate的旧终态阶段断言失败。新行为在COMMIT前的early_target_pipeline_failed结束，原数据仍可读写且inflight bytes=0；按此准确阶段更新断言后，`mtr-semantic-binlog-green`通过。不是放松清理或数据一致性检查。
- `active_epochs=0`只证明无accepted epoch，不证明全部OPEN/sequence元数据删除；receiver已失败record可按既有寿命保留。此次未扩展abort、RESET DRAIN或元数据清理机制。
- MTR路径在`/private/tmp/`，收集后的永久证据放在`build-debug/semantic-fix/reports/`；这些定向结果不等于全量MTR。
- r3暴露日志格式未展开、诊断关闭控制socket后打断末尾报告。修正为预格式化有界日志、独立诊断连接、先保存业务账本；真实MTR和故意超时的20连接E2E验证，r4完整复测。保留原失败报告，不把诊断丢失当业务无异常的完整证据。

### r10结果与下一项定位

原500/300秒r10完成377/377 READY、业务错误0，47个空BEGIN未分类；strict4049.492ms、EXACT4048.488ms、ACK→READY7783.881ms均超过原门槛。DATA99复用/1回退、source FINAL写22,050,767B；源端优化有效，receiver尾部没有同步改善。NATIVE累计12,451,275us、单批最大1,010,135us，包含seal同步、fil/dict发布及ticket，不把它直接等同锁或fsync。

三个W11计量fixture旧“image写=2×逻辑读”已不适用稀疏目标：保留有效RED，改为在DRAIN后、RESUME前独立按源镜像真实页计算双份非零页+末页字节，仍严格相等；三个用例通过。第一次修订把计算放进仅continuous_capture经过的分支导致变量未赋值，已移到所有DRAIN成功路径并实跑纠正，不隐藏该测试脚本错误。COMMIT用例增强旧undo owner及时退出断言，亦通过。

新的500/120秒R11为调用栈诊断，非正式性能验收；保持原500分类、预算与业务操作，只缩短稳态期。待定位NATIVE具体同步路径后做窄修复，不用删除持久化要求或延长预算掩盖尾部。


### R11定位：DATA同步在final尾部；当前修复及验证

R11是500连接、120秒稳态的诊断轮，binary SHA与R10相同；不是300秒正式配对验收。receiver prepare分支1798个叶子样本中，seal_image有1368，DATA `my_sync`有1319（两份各664/655），调用点是writer close；未采到相应fil/dict锁等待。worker退休/reaper清理另有样本，但采样无逐样本时间戳，不能把它们全部计入ACK→READY。源端final staging采到64KiB逐对象同步发送/ACK等待，不能等同网络故障，也不能把worker wall全部归为捕获耗时。

修复保持既有worker和私有writer所有权：image、stats、DD完成后先flush两份DATA，成功才报告preprepared；final仍须身份、清单和SEAL授权。writer在实际write/truncate前置dirty，flush仅成功清除；不变的final close复用同步结果，内容改变则重新同步。目录发布/同步和完整摘要要求不删减。donor换代重置同步进度但保留writer自身dirty状态。另去掉warm名字已被durable seal移走后的重复目录同步；这不是R11的主要fsync样本来源。

有效RED为 `mtr-prepare-sync-red.log`：已准备候选之后仅receiver注入DATA同步错误，旧final close重复同步导致NOT_READY。新代码9个log-bin业务用例通过（early、cross、spill、final小尾部、rollback、LOB、COMMIT、sparse及W11）；3个no-bin writer所有权/摘要/清理用例通过。清理probe还验证重写、长度缩短/增长、同长truncate、连续同步失败和成功重试；shutdown单列。Debug/Release构建通过。定向通过不等于500连接性能通过；R12采用原500/300秒模型单独评估，输入patch及未跟踪文件已保存。

### 最终DATA增量缺口（已修复，原模型复测中）

`final_reused`表示本地连续sidecar复用，不等于wire checkpoint命中。最后DML改变logical SHA后，旧checkpoint select失配，final transfer会重新编码完整sparse BASE。旧immutable BASE owner仍在sidecar，当前bundle没有把它保留到发送阶段。

实施边界：仅在源端bundle持有预算内的小型BASE owner集合；不序列化、不入全局registry、metadata、长期record。冻结文件发送时先验证同token/space的BASE已经SEAL，然后复用现有DELTA builder，logical raw摘要与可能压缩的wire BASE摘要分别使用。可选未开始/SKIP先释放builder再回退原sparse；真实ERROR或传输开始后不通过回退掩盖失败。每批检查现有取消/截止状态，全部patch SEAL后原子替换manifest及其权威TLV。实际BEGIN/COMMIT路径与resource-only路径都要接线；receiver拒绝携带source-only owner的本地bundle。

该方案不承诺减少全量比较：manifest已做最终摘要，普通DELTA还读target与BASE；主要收益是少写和少传。禁止借用已disarm的dirty-mask证明。先增加“最后候选SEAL ACK被真实relay留住→最后DML→final产生新DELTA且摘要匹配→READY/RESUME/ROLLBACK”的RED/GREEN，再测原模型；不创建新缓存、线程池或外部阶段。


最终DELTA补丁已接入active与resource-only路径；只保留同空间的immutable BASE共享owner，不持有THD/participant/undo，不增线程。有效旧RED为最后DML后新patch集合为空；新代码最终tail、跨COMMIT到resource-only、cross/spill、rollback、LOB、sparse fallback、resource-only RESUME和W11共10业务用例通过，额外独立双实例final-tail也通过。跨COMMIT新增断言独立解析BASE/patch，检查完整最终SHA及RESUME后回滚，不只看aggregate计数。独立源码review未发现确定缺陷。Debug/Release已构建；原500/300秒R13已完成，输入快照已冻结：393READY/52session-only/55空BEGIN，业务错误0；strict2598.142ms、EXACT2593.758ms（5合格命令）、ACK→READY2819.190ms，仍未过原门槛。详见 mixed500-r13/README.md。

R12（只有提前同步、尚无final DELTA）完成313READY/59session-only/128空BEGIN，业务错误0；strict8857.465ms、ACK→READY3905.222ms、EXACT无合格命令，仍失败。receiver尾部下降但source尾部增加，不宣称整体优化成功。原始输入及结果见mixed500-r12-input和mixed500-r12；不能将不同命令切点的R10/R12当严格等工作量配对。receiver跨COMMIT DATA-only复用已实现并在定向验证：不移动旧undo，仍全页重新转换后比对，不放宽DDL/lineage/private owner约束。首次GREEN中仅最后owner保持ID，其他3个仍fresh；正在采集候选slot实际状态，区分donor交接丢失与复用条件拒绝，不能宣称该项闭环。

### 空 BEGIN 与跨 owner 收口（当前定向验证）

空显式BEGIN没有引擎更新，也没有TEMP/cursor，原early no-token和NONE payload的资源非空门槛共同排除了它。复用已有NONE恢复合同、SQL-active/explicit-BEGIN字段及RESUME状态恢复，不增加外部会话机制。仅在enable/temp/native namespace/Classic/standby路径放行，附着后必须再次证明NO_ENGINE；健康ACTIVE无更新read context维持旧no-token分类，有引擎更新保持原路径。独立源码review核对trx_mutex与原target pin/quiesce保护，未发现新的具体缺陷。

新empty-BEGIN MTR旧内核确定返回NO_PRESERVABLE_TOKENS；修复后已完成DRAIN/READY/真实SQL RESUME，旧端首个UPDATE返回4020，目标不重新BEGIN直接UPDATE仍未提交，ROLLBACK后目标状态及原值恢复。首次GREEN被沿用TEMP fixture的“必须有资源目录”断言拦住，改为明确无工件后通过，未放宽survivor/数据断言。该证据是本地SQL bridge，不是外部物理升主验收。普通session-only、read-context/implicit/empty/read-lock、TEMP readonly及OFF等定向回归另列；全量MTR尚未重跑。

receiver跨事务复用只接管private DATA身份和文件，新plan从新事务完整重建undo，并重新转换每个source页后与旧target比较。旧undo留原owner沿既有reaper退休。针对旧无undo→新active、旧active→新active、最终resource-only的四owner用例，除数据及回滚外，要求space/table/index ID不变。注册新候选或final接管时的pending donor交接尚在诊断，未添加新的缓存、线程池或外部阶段。

### pending donor 的实测根因与修复

有效运行trace中token 11/13/14在final take时均为 `ready=0 failed=0 work=0 previous=1`，随后分配全新DATA ID；token15已有work，成功复用。由此确认未启动候选的previous被final忽略，不是复用校验拒绝。register的连续替换也会丢同一份previous；源码检查证明这条路径同源，但本轮trace直接命中的是take。早先试图靠transfer目录的manifest文件代数证明候选更换不可行（最终已清理或内存承载），撤销该错误fixture断言，未把它当kernel RED。

修复仅在非working/failed/final-claimed且 `!work` 时转交已有preprepared previous；已启动work内部的previous可能经过partial MOVE，不能提取。take返回ABSENT，仍独立加载/认证最终输入，不能返回READY或授权旧manifest。只保留一个donor，无历史链、新线程池或新预算。临时诊断print在确认后删除。

修复后 `mtr-data-owner-empty-green3` 17个业务用例全部通过，shutdown单列；四owner ID、DATA+DELTA摘要、原值/新值/回滚、旧无undo→新active、最终resource-only均通过。另有空BEGIN独立GREEN，旧namespace-OFF no-bin分类单列验证。部分MOVE取消/失败和连续多次未启动slot替换尚无专门新运行覆盖，保留为静态审核边界。Debug/Release构建成功；原500/300秒R14输入快照已冻结，准备复测。

R14原500/300秒复测：443READY+57session-only，未分类/业务错误均0，本地功能首次完整通过。strict889.592ms通过2s；ACK→READY1277.689ms仍超500ms，EXACT无合格样本而非通过。输入已冻结，无并行重任务。继续用同二进制R15短稳态诊断定位receiver尾部；不将其视作正式性能验收。详见[完整R14报告](../../../build-release/continuous-resource/mixed500-r14/README.md)。


### R15/R16定位与不可变BASE复用（E80，整体性能仍未通过）

R15为R14同二进制500/120秒诊断，不是正式验收。receiver prepare的587个叶子样本中，SHA172、pread135（旧目标比较105）、pwrite65、索引解码60、link51、fsync9；不是墙钟占比。既有8线程受BALANCED每epoch3个active上限约束，不能仅凭CV样本断言错误串行。PROMOTION_PREPARE同时改变对象限额和批次；本轮没有切换该profile，也没有增加预算或新参数。

旧目标比较在每个step内以最多64KiB连续读取，保留逐页转换/比较/写入/摘要；跨step不缓存，不读超出旧文件或本批页面，缓冲增长仍走原memory lease，64KiB无法获得时退回页大小。指针以buffer+(offset-base)计算。有效RED为2304次16KiB读取；8业务MTR GREEN验证合并读与ID/数据/回滚，Debug/Release通过。原500/300秒R16功能通过442READY+58session-only，但strict4133.502ms、ACK→READY1876.840ms、EXACT无合格样本，性能未通过。NATIVE累计7.399秒/单批最大1.729秒，SOURCE FINAL累计6.061秒；不能将局部系统调用减少宣称整体达标，也不能未采样就把整个长批次等同fsync。详见[R16](../../../build-release/continuous-resource/mixed500-r16/README.md)。

进一步源码确认：每代重新解码同一物理不可变sparse BASE并验证完整logical SHA。新实现仅由当前exclusive previous、preprepared、同lineage且未final-authorized提供plan的当批只读提示；按序号与source_space_id取source_file。reader保留header/token/space/范围/计费，沿已有overlay查找zero_default节点，要求物理BASE对象指针相同及logical size/SHA相同后复用该节点。新DELTA仍完整验证并引用BASE，不保留旧DELTA根；UNDO、图验证、目标页转换及最终授权不跳过。无overlay、物理对象不同或derived-file退化都完整扫描。没有新缓存、历史链、文件或锁；shared_ptr保留原文件和索引租约至最后读者，cancel可以先退休donor。

数量型RED：commit fixture的SOURCE logical扫描84,084,704B，最终DATA46,137,344B。首次阈值2倍DATA旧代码也通过，不计RED；修正为最终DATA总长加held BASE逻辑长度9,437,184B后旧代码稳定失败。fixture要求四owner均有sparse BASE，held BASE独立解码取大小；SOURCE计数包含补零，不能称为同量物理磁盘读。新代码46,319,120B，减少44.9%；final DATA/BASE+DELTA SHA、空间/表/索引ID及COMMIT/ROLLBACK断言保持。9个业务MTR通过，覆盖跨代/跨COMMIT、小尾部、spill、rollback、undo摘要拒绝/取消、空BEGIN；shutdown单列。新增分支的物理owner更换和半途取消没有专门新故障注入，不夸大覆盖。独立源码review未发现阻断。Debug/Release构建通过。

原500/300秒R17已冻结全部输入，继续BALANCED、双端1GiB、不限速，运行期间无构建/MTR/采样；结果完成后记录。当前未提交或push。

R16时间线进一步核实：16.879秒诊断时source尚未进入closing，receiver单批1.729秒已发生，NATIVE累计6.591秒；不能把这段直接解释为ACK→READY。最终NATIVE差额0.808秒仍非精准ACK窗口。当前不因全局最大值或CV采样而删除同步或提高BALANCED并发。


R17原500/300秒结果：453READY+47session-only，业务/未分类错误0，功能通过；strict7558.368ms、ACK→READY4388.992ms仍失败，EXACT无合格样本。SOURCE累计扫描已降至2.218GB/1.061秒，但NATIVE累计18.953秒、IMAGE15.685秒；不同命令切点不可当等工作量对照。已暂停叠加并发修改，清理结束MTR的可再生std_data副本（保留证据）后可用空间恢复41GiB，以同二进制R18诊断定位等待。并未确认磁盘空间是因果，也不把局部扫描下降当整体闭环。详见[R17报告](../../../build-release/continuous-resource/mixed500-r17/README.md)。


### R19及诊断隔离核实（功能通过，性能仍开放）

原500/300秒R19完成439 READY+61 session-only，业务/未分类错误0。strict1959.247ms通过2s，仅40.753ms余量；ACK→READY2970.222ms未达500ms，EXACT无合格命令，整体仍失败。不能把无样本作为零延迟，也不以单轮strict通过关闭性能项。

诊断status原每端525行触发1074次socket recv，与500业务线程同进程形成可确认的调用放大；仅专用诊断wrapper加64KiB缓冲后，20连接实验降为2次、R19为1次，SQL/采样频率/绝对deadline不变。极短deadline独立实跑中两端在首个recv之前诊断失败，但19 READY+1 session-only、业务错误0，最终报告完整；此项不覆盖收到部分响应后超时；不归为内核功能失败。未证明该诊断放大就是ACK→READY根因。

R19最终prepare-only2966.036ms；整轮NATIVE13.105秒中11.457秒早在Phase1诊断已发生，不能归入最终窗口。后期IMAGE单批最大1981.933ms，正式staged job最大1981.938ms；源端/目标端时间必须用同阶段计量。SOURCE_FINAL现沿原RAII/TLS补TAIL、CLOSE、DIGEST、SEAL及其子项VALIDATE，非final不取时钟/更新计数；VALIDATE不能与SEAL重复相加。早期池实际16个worker，旧日志按普通路径误报10，现取成功join后的实际vector size，只修正报告，不修改建池行为。

这些新增计时经过6个业务MTR（含跨COMMIT四owner、final-tail、OFF、取消、physical metrics）验证；SOURCE_FINAL子计数不得在Phase1提前增加，每个owner final各计一次，已验证BASE复用时不再重新验全镜像摘要。Debug/Release构建成功。R20按500/120秒诊断（非300秒验收）进一步定位，不提高预算或盲目增加epoch并发。详见[R19报告](../../../build-release/continuous-resource/mixed500-r19/README.md)。


### E81：R20确诊首次同步后移及零页比较冗余

R20保持原500分组/预算/不限速，120秒为诊断而非正式验收；319 READY+181 session-only，strict3799.350ms、EXACT3692.706ms、ACK→READY1813.542ms。SOURCE_FINAL累计20.027932秒，CLOSE15.695548秒；16个worker采样同时等待DATA/UNDO文件fsync。连续DATA prebuild之前从未调用flush，checkpoint_result只缓存摘要，final close承担首次同步，根因已由源码与栈共同确认。SOURCE_FINAL剩余3.583626秒含UNDO同步，不称为metadata。

仅在既有ROUND无checkpoint完成和CHECKPOINT_EXTEND完成处调用writer.flush；后续write/实际resize自动置dirty，final仍同步最后变化，目录安装同步不删。沿原fail/retire处理同步错误。有效RED为mtr-source-flush-red：稳定checkpoint后注入writer sync failure，旧source返回4013；修复后同用例及cross-spill/final-tail/COMMIT/rollback/worker/sparse/OFF共8业务MTR通过。全部是本地行为验证，不宣称外部物理集成。

receiver只增加sealed_file::known_zero_range（无IO/无分配），用已验证sparse索引证明旧区间全零，任何patch重叠/普通FD都返回未知；当前页正常转换后也全零才省比较读。保留UNDO/分配/LOB验证、全SHA与原写/同步过程。Image保活本次donor source至checkpoint/取消；只一个旧代，不加cache/bitmap/worker，sizeof(Image)走原预算。有效RED为READY之后37,748,736B完整旧DATA比较，GREEN11,534,336B，减少69.4%。首次断言放在receiver仍paused时得到0，已纠正，不计RED。8个业务MTR覆盖跨COMMIT、final小尾部、spill、rollback、undo取消、sparse/OFF；shutdown单列。独立只读review未发现零页误跳写或owner泄漏。

新增SOURCE_FINAL的verify字段采用短名，避免MySQL 64字节变量名称缓冲截断最长unclassified_calls；语义是SEAL的全文件摘要验证子项。R21采用原500/300秒、相同1GiB预算、BALANCED，输入patch/未跟踪文件/SHA已冻结，运行期间不构建/MTR/采样。两项局部收益尚不能代替原性能验收。详见[R20](../../../build-release/continuous-resource/mixed500-profile-r20/README.md)。


R21原500/300秒完成：440 READY+60 session-only、业务/未分类错误0，strict1825.504ms通过≤2s，但ACK→READY2076.796ms仍未通过严格<500ms，EXACT无合格样本。SOURCE_FINAL_CLOSE累计46.420ms、FINAL总656.398ms、ROUND2297.452ms；源端首次同步后移已缓解，receiver整体尾部尚未闭环。用户明确禁止以局部收益代替整体指标、不得越优化越劣化；保持规模/预算/业务不停/功能断言，无效或退化改动需复核撤回。当前暂停叠加内核修改，以同二进制R22（120秒诊断）继续定位receiver FINAL。详见[R21](../../../build-release/continuous-resource/mixed500-r21/README.md)。
