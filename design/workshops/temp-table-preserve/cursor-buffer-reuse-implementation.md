# Cursor 缓冲复用：实施与验收记录

日期：2026-10-11。分支 `ha_preserve_trx`；差分基线 `48cab89fa3a6`。对应[设计](cursor-buffer-reuse-design.md)。缓冲复用、默认1024工作槽、按需扩批及源缓冲进程期保留已实施。后续已完成万级容量验证、E148全量MTR含big-test及五轮持续业务复测、E149–E150完整压力回归；E151清理后重新构建Debug／Release并通过26项定向MTR，尚未在清理后重跑全量或压测。仍有性能未通过项，详见本文末尾E147–E151记录及[任务跟踪](task-tracker.md)；没有提交或 push。

## 实现范围

| 处理位置 | 具体变化 | 保留的边界 |
| --- | --- | --- |
| 源端结果 owner | 身份登记与活动捕获状态分开；封存后释放私有捕获状态，内存 backing 移交 payload，固定 final 工作区归还执行者复用 | 旧代最后一个引用释放后才归还内容并减少存活统计；结果内容不会因借用结束而释放 |
| 捕获及发送 | 既有 worker 自带 workspace，跨结果、跨批次复用；partial 保留自己的写进度 | 执行者独占工作区；producer 借还只短锁保护空闲链表，无额外线程池或全批完成屏障 |
| final | Phase1 准入前按现有 final 并行上界预留；每执行者独占；join 后结束借用，源缓冲仍保留 | 预留失败走原失败出口，不减少 worker 数；归还 fixed home 不分配、不重新准入 |
| 内存账本 | 现有 lease 增加窄范围的 token 转移；校验目标聚合额度并原子转账 | 2 GiB 全局／1 GiB 单 token 上限不变；分配失败账本不漂移；实际 allocation 释放先于额度归还 |
| 新结果流 | worker 确认 producer DONE、ring 排空和行数一致后归还 producer 块供下一任务复用 | ABANDONED／取消标记本身不能证明 producer 已停止使用内存；远端 SEAL 条件不变 |
| receiver | 工作区由已有 worker 借用；长期 decoder 保留 schema、状态和文件，不保留每结果固定读取大缓冲 | 同一 candidate 的 working／final claim 排他、动态 proof、epoch 和取消检查不变 |
| 恢复后 FETCH | 每次 FETCH 按原 token 借用；返回前清 BLOB 指针、缓存范围和借用 | 不切换控制 THD 与业务 THD；EOF、错误及消费位置语义不变 |
| 源端退役 | 每次最多检查 64 个历史项；冻结有限轮次；变更清单合并发布，成功后两条队列各过滤一次 | final 仍以精确 selected 集合纠偏；发布失败不提前删队列或归还内容额度 |
| receiver 重复工作 | 冻结 manifest 的总字节数和特殊对象位置计算一次；BEGIN 使用已有对象索引；完整身份和 sealed owner 匹配才省略重复准备 | 不以 cursor ready 代替 TEMP／binlog／record-lock proof |

新增内核文件仅 `preserve_trx_cursor_workspace.h/.cc` 和 `preserve_trx_cursor_capture_state.h`。其他改动沿已有调用链传递工作区与调整生命周期；没有增加 wire 类型、PS 本体迁移、物理升主阶段或 RESET DRAIN 行为。共享 MySQL 路径仍为已有薄接点；`sql_cursor.cc` 的新增 workspace 是 Debug 校验路径，Release 原生 FETCH 不进入它。

```mermaid
flowchart LR
  A[登记结果身份] --> B[既有 worker 借用工作区]
  B --> C[partial 私有进度和内容]
  C --> D[封存 payload / 文件]
  D --> E[释放捕获状态]
  E --> F[工作区处理下一个结果]
  D --> G[已有发送和 receiver 准备]
```

`rds_preserve_trx_result_capture_work_slots` 替换旧数量参数，默认 **1,024**，只作工作槽上限，不预分配 1,024 份缓冲。已有 ordinary／final 执行者在 producer 发布前预留名额；producer 优先复用空闲块，无空闲按实际 ordinary worker 数 W 扩批，默认每批六块，尾批允许较少。忙、闲、在建块都占数量额度；归还只使块可再次借用，实际 allocation／lease 持续存在。sealed 和 partial 不占长期结果数量额度，`live_results` 仅统计，内容与内存预算保留。

用户确认无论 DRAIN 成功失败，旧主最终均被 kill。本次源工作缓冲因此统一保留到进程退出；只停止新借入，不在错误／取消路径销毁。取得执行者槽后，以进程期链持有纯工作内存 owner，不持有 THD、结果或事务。失败的实际分配不虚计费。只覆盖单次 DRAIN 内复用，不新增重复 DRAIN／RESET 或跨 epoch 池规则；receiver 仍按原生命周期清理。

普通组工作区在首次真实 cursor bind 时准备，bind 在读本地 lease 前先经过整组初始化发布，避免与另一 worker 成批初始化同时读写。TEMP-only 不触发 cursor scratch 申请。
## 已确认的证据

本机 Release ABI 下 `sizeof(Preserve_trx_cursor_result)=328` 字节。它不含 payload、sealed file、共享指针控制块或原生结果表，不能把 328 字节解释为每个完整结果的总成本。封存的小结果仍按持有的 payload capacity 计费；大结果保留文件。现有全局 memory peak 是 Preserve lease 累计高水位，不等于 malloc 实际 heap 或进程 RSS。

| 修订工作槽前已执行项目 | 实际结果 |
| --- | --- |
| 旧二进制负例 | 64 个小结果封存后的 lease 增量为 8,679,936 字节，超过新测试的 64×72 KiB 边界；保留 RED 日志 |
| Debug／Release 构建 | 通过；最终审查修复 final 初次 FAILED 未关闭 cursor 的错误出口后再次构建通过 |
| 定向 no-bin／log-bin | 22／41 个业务用例通过，22／3 个条件跳过；shutdown 各通过；覆盖原 OFF、小 count、增长／spill、流式、换代、故障及恢复路径 |
| 新增容量／final MTR | 两个业务用例通过，shutdown 通过；后者覆盖 129 个 DONE 待消费 producer、一个远端 OPEN、三段退役扫描及冷 cursor final 补捕获 |
| Debug 本地恢复桥 | 1,000×10 个真实结果全部恢复后再逐个消费，10,000 个结果剩余行、次序、位置和 EOF 全部通过 |
| 完整 no-bin（含 big test） | 924 个业务用例全部调度：606 通过、317 条件跳过、1 个源码 lint 失败；shutdown 通过。唯一失败串行复现后，仅修正该 lint 并串行通过；没有修改内核或二进制 |

129 项场景先宣告字典序最大的结果，再建立另外 128 项，CLOSE 后观察该远端 `.part` 退役。结合每片 64 项的源码路径验证跨片推进；这不是直接计量 129 次 registry erase，也不宣称覆盖所有取消竞态。冷结果覆盖空结果、精确尾部，以及恢复后实际读取剩余 70 KB BLOB 行；无需 DEBUG_SYNC。

新预算边界测试先测活动捕获固定成本，再分别拒绝第一次 128 KiB 扩容和旧 128 KiB＋新 256 KiB 并存峰值；不再错误使用封存成本替代活动成本。原 spill、内容与计数回基线要求保留。

完整 no-bin 的 `batch_drain_phase2_final_hwm_overlap_lint` 原正则要求调用名后 500 字符内出现 `pending_final_binlog_descriptor`，实际误匹配后面的清理语句；新增工作区参数使距离从 495 变为 517 字符。修订改为核对真实调用的 `pending_descriptor` 和工作区实参，不扩大字符阈值。descriptor 的来源、完整身份核对与 preseal 顺序保持原实现。原全量失败、串行 RED 和 GREEN 分别保留，不将原始全量日志改写为零失败。

## 历史万级 Release 容量（旧计数语义）

两轮均使用实际默认 count 16,384、双端 Preserve 2 GiB、内容 1 GiB、inflight 1 GiB、PROMOTION_PREPARE。原生配置满足 1,000 session、30,000 PS 和有效 32,768 FD。每结果 512 行，四列 INT/BIGINT/VARCHAR/BLOB；前缀 0／1／127／128／255。Phase1 在运行前固定为 60 秒。没有 TEMP 背景、没有 eligible BODY，因此 EXACT 独立尾部为 N/A。

| 分布 | strict Phase2 | receiver ACK→READY | source lease peak | receiver lease peak | 结果 |
| --- | ---: | ---: | ---: | ---: | --- |
| 1,000×10 | 291.968 ms | 46.718 ms | 685,100,398 B | 292,293,765 B | 10,000 捕获／提前准备／复用，1,000 READY |
| 500×1＋500×19 | 278.905 ms | 69.121 ms | 685,785,288 B | 292,082,173 B | 10,000 捕获／提前准备／复用，1,000 READY |

每轮源端实测内容 466,470,000 B；不是仅创建 PS 或空结果。内核最后一处 FAILED 出口修复发生在这两轮之后，只影响失败关闭语义；仍需最终 Release 的后续复测，不把旧二进制数据冒充新二进制验收。

这些数据只证明旧 16,384 总数上限下指定样本的容量与两项切换指标，不证明新 1,024 工作槽默认值已验收；不证明任意列宽、任意数据量或单 session 万级。不用本地恢复桥替代真实物理升主、SQL RESUME 和生产 PS 回放集成。

## 后续验收与记录

旧语义完整 log-bin MTR（含 big test）已完成：924 个业务用例，634 通过、290 条件跳过、零失败，shutdown 通过；两模式合计 924 个不同用例均至少通过一种模式，18 个 big test 全部通过适用模式。新工作槽语义需重新构建、定向及全量回归，再顺序运行最终 Release 的万级容量、持续混合业务和 TEMP／换代并存样本。失败、条件跳过、性能未达标或证据缺项分别记录，不以功能成功替代性能验收。实际 heap／FD 的未采样瞬时峰值以及设计中的细粒度工作量，不能由现有累计 lease 指标推导。

原始证据：[`build-release/cursor-buffer-reuse-e142`](../../../build-release/cursor-buffer-reuse-e142/)、[`build-debug/preserve-cursor-full-e142`](../../../build-debug/preserve-cursor-full-e142/)。测试数据目录只在本轮执行结束且日志保存后回收；原始失败证据保留。

## 2026-10-11 工作槽修订的定向验证

新名称 `rds_preserve_trx_result_capture_work_slots`、默认 1024 已进入内核及配置，旧内核／脚本／MTR 参数引用已清除。Debug 重建通过。定向 no-bin MTR 五个业务用例及 shutdown 全通过：cursor_result_capture、cursor_result_capture_off、cursor_result_buffer_reuse、cursor_result_storage_restore、code_review_resumable_trx_slices_lint。日志 `/private/tmp/mtr-cursor-work-slots.log`。小额度配置为 2，三个已完成结果均可继续持有和 FETCH，证明不再将工作槽解释为保留结果总数上限。

源端扩批、封顶、复借、任意结果下保留的代码已进入当前差分。新增 cursor_work_cache MTR 以 15 槽验证两槽执行者预留、6＋6＋1 个 producer 块、耗尽立即返回、申请／转账失败、同地址复借、停止后最后使用者归还和进程期留存；该 Debug 原语探针不代表真实迁移验收。第一轮定向七个业务用例及 shutdown 均通过，日志 `/private/tmp/mtr-cursor-pool-targeted.log`。随后静态审查发现 ordinary 组初始化的首次 lease 读取竞态并已修正；需用最终二进制复测，不能把此前结果算作该修复后的证据。

饱和的业务 EXECUTE 不等待缓冲，保留原生结果交给后台捕获。复核发现 ordinary cohort 封口后无 Loan 的新会话没有 producer owner 登记，可能只在 final 被发现；当时新增 cursor_phase1_work_slots MTR 验证此边界，后续 RED→GREEN 结果见下一节。该问题的修复与验收单列，不能把原生 fallback 等同于已经完成后台排队。

## 2026-10-11 槽满后的 Phase1 登记修复

真实执行型测试使用 19 个工作槽，启动 20 个在 SQL 用户锁处保持 ACTIVE 的 producer，再启动第 21 个不取锁的 EXECUTE。该命令必须在缓冲仍被占用时完成，并在释放旧 producer 前观察到 receiver SEAL，之后全部读取 128 行含 NULL／空值／嵌零 BLOB 的结果。普通 cohort 未封口的首次运行通过，不能据此排除后半段缺口。

随后以一个仅 Debug 的 DBUG 条件固定既有 `discovery_closed` 窗口（不是 DEBUG_SYNC）。旧行为串行复现 `slot-full cursor not captured during Phase1`；修改后同一用例及 shutdown 通过。日志分别为 `/private/tmp/mtr-cursor-pool-late-red.log`、`/private/tmp/mtr-cursor-pool-late-green.log`，原始失败现场保留。封口时点由 Debug 条件控制，其余 EXECUTE、网络搬迁和 FETCH 都走实际路径；不冒充物理复制工程的升主验收。

修复仅将既有登记入口改名 `register_producer` 并允许空 stream，借用失败时登记 owner/cookie 与 `sampled=false`。已有 builders 先处理，队列空后重新发现；复用原 declare／submit 和 NO_SLOT／NO_CREDIT 等待。没有新增线程、排队容器或业务等待条件，也没有放松真实 OOM／I/O 错误。旧 `close/retain` 成败分支和单扩容者 `m_growing` 门槛已移除。

## 2026-10-11 最终工作槽版 MTR 回归

最终 Debug 构建后，cursor 专项 no-bin 20 通过／22 条件跳过，log-bin 42 通过，shutdown 均通过。随后仅将重复 stop 调用收敛到统一收尾 guard，移除 RESET 路径新增调用并重建；完整 no-bin 含 big test 已结束：926 个业务用例，608 通过、318 需要 binlog 而跳过、零失败，shutdown 通过、退出 0。

独立审核补齐容量脚本的 FETCH 状态位断言：FETCH 0、已知精确数量的 FETCH 保持 CURSOR_EXISTS 且无 LAST_ROW_SENT；最终空读反向检查。该改动只加强测试，内核未变；no-bin cursor_capacity_restore 及 shutdown 已复测通过。之后完整 log-bin 含 big test：636 通过、290 要求关闭 binlog 而跳过、零失败，shutdown 通过、退出 0。

两种模式各完整调度 926 个业务用例，合计 1852 个业务结果；926 个不同用例及 18 个 big test 均至少在一种适用模式通过。保持 parallel=4、retry=0、force、max-test-fail=0 和原 check-testcases；没有遗漏或重复调度。测试前记录的 110 个 Preserve 文件及 Debug 二进制指纹未变化。

本轮日志及逐项库存：[`build-debug/preserve-cursor-pool-e142`](../../../build-debug/preserve-cursor-pool-e142/)。后续默认 1024 工作槽的结果见下一节；旧 16384 结果不能替代。

## 2026-10-11 默认 1024 工作槽的万级验收

最终 Release SHA256 为 `d34e2907fe63c6e672403676900ca21cc51d94930632959b7a68e482c1511d41`。三轮均使用实际默认 1024、1000 session、双端 Preserve 2 GiB、内容及 inflight 各 1 GiB、固定 Phase1 60 秒，没有提高预算。

| 分布／背景 | 捕获及提前准备 | final 复用／恢复 | strict Phase2 | receiver ACK→READY | 源／目标 lease 峰值（B） |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1000×10 | 10000 | 10000／10000 | 301.903 ms | 46.964 ms | 685582996／292297637 |
| 500×1＋500×19 | 10000 | 10000／10000 | 295.139 ms | 50.981 ms | 686368356／292085877 |
| 10000＋10 个源 TEMP＋100 次 CLOSE／新 PS | 10100 | 10000／10000 | 361.640 ms | 69.837 ms | 783484092／313259880 |

每轮均为 1000 token READY，执行驱动退出 0。并存样本独立检查 final 对象集合、源 PS ID 映射及 TEMP 归属，旧 100 个结果没有混入 final。它不代表目标端已有 TEMP 背景，也不是同 PS ID 的所有换代情况。三轮无 eligible BODY，EXACT 尾部为 N/A。

另用最终 Debug 同时保留 10000 个真实结果，经本地恢复桥逐个核对剩余内容、顺序、位置、CURSOR_EXISTS／LAST_ROW_SENT，最后 live results 为 0。该桥不执行物理升主或 SQL RESUME；性能不取自 Debug。证据在 [`cursor-buffer-reuse-e142`](../../../build-release/cursor-buffer-reuse-e142/)，本节目录均以 `capacity-workslots-` 开头。

当前结论限于上述内容规模在原预算内的全局万级结果，不保证任意行宽或任意内容总量。500 连接持续业务两轮及其余历史压力模型的最终记录见下节。

## 2026-10-11 同一 Release 的持续业务及其它压力模型

500连接持续TEMP50／FETCH50／BOTH50／RW350、300秒业务两轮本地功能均通过；strict1261.623／1971.511ms、ACK→READY85.992／40.925ms，两项关键指标达标。原综合验收仍0/2：第一轮有效EXACT1260.009ms超500ms，第二轮NO_ELIGIBLE_BODY且coverage完整，EXACT记N/A。证据：[两轮核验](../../../build-release/continuous-resource/e142-workslots-review/README.md)。

E143另将13项原有配置及M1–M12共62配置各跑一次，保持同一Release、原配置／预算／判据。75项均完成，原完整验收66/75：原有模型5/13，M矩阵61/62。三项sysbench1000连接、TPC-C64/128通过原验收；TPC-C1000在DRAIN前后取样窗口新增2次1205且strict超限，其余原模型的性能、覆盖和无效TPS证据逐项保留。

M矩阵56个预期成功迁移配置全部READY，合计334 token，捕获失败0；strict最大1377.425ms、ACK→READY最大22.756ms，均满足原2秒／500ms。56项EXACT均无eligible BODY，coverage完整，不能计0或通过。3个无DRAIN对照通过；memory、deadline预期拒绝通过。

唯一M矩阵失败是inflight不足负例的receiver清理：预期RESOURCE_EXHAUSTED／4013及4个源会话后续FETCH、rollback正确，但清理后内存账面值131076字节未回基线，空闲worker仍存活。源码核查显示workspace随worker保留，失败ABORT路径未请求既有idle-retirement，worker完成又仅在accepted epoch请求；131076与普通scratch及行缓冲尺寸强匹配，未做逐allocation探针。该问题未修复，源端缓冲进程期留存的要求不适用于receiver，不能删除回基线断言豁免。本轮只测试与取证，没有改内核或扩大判据。

完整指标、失败和边界见[E143报告](../../../build-release/preserve-pressure-e143/README.md)、[75项逐一清单](../../../build-release/preserve-pressure-e143/all-results.tsv)。本轮无实际物理升主／SQL RESUME／生产PS回放及恢复后FETCH验收，不替代外部工程集成。

E144用同一Release和原5秒断言再复现两次M11清理FAIL。第二轮在失败后空闲观察75.179秒仍为393228字节／8个idle worker；正确身份的首次ABANDON触发现有退休流程后，首个完成采样（ACK后3.452ms）中worker与内存均归零。结合源端四个ABORT成功即返回、receiver ABORT无退休请求、未COMMIT时worker guard不补请求，以及新workspace线程级持有链，确认是失败终态遗漏worker退休导致的缓冲留存。不是仅靠延长清理观察可解决，原负例仍FAIL，尚未改内核；[完整诊断](../../../build-release/preserve-pressure-e143/m11-inflight-triage.md)保留干预范围及首轮诊断身份错误。

## E145：2026-10-11 M11 receiver缓冲回收修复

上述E143／E144为修复前历史证据。本轮只在`sql/preserve_trx_transfer.cc`补齐既有回收触发，新增15行、删除5行，没有新增线程池或清理层：token成功ABORT后请求回收；worker忙碌或正在启动时保存请求；最后任务归还活动计数后唤醒reaper，仍由原完整空闲谓词及锁外join保证最后使用者退出。启动失败在join后清除请求，普通Phase1批次不会自行退休；忙碌期间不增加reaper扫描。源端缓存保留到进程退出的合同不变。

旧版原M11及两次诊断复现保持RED；新版原inflight负例连续3轮GREEN。实际cursor工作量、原脚本、4MiB inflight和5秒清理门槛均未变，receiver内存和worker在实例退出前归零；每轮四个原业务会话继续FETCH并正确ROLLBACK。没有增加ABANDON来协助通过。

Release M1–M12原62配置全部通过独立验收，含两次额外inflight复测共64/64；56个迁移正例333 READY、捕获失败0，strict最大1195.639ms、ACK→READY最大27.934ms。EXACT只有1个有效样本49.121ms，其余55个正例无eligible BODY，不能计作0或通过。

Debug相关102个MTR在no-bin／log-bin各调度一次：50通过＋52条件跳过／73通过＋29条件跳过，零失败，shutdown分别通过；102个不同用例均在适用模式通过。包含worker自动停止与重启及ABANDON清理；不是全套MTR重跑，也没有确定性穷举ABORT与worker启动／退出的所有交错。并发入口经过独立源码复核，复用既有锁和生命周期；本轮不增加UT或DEBUG_SYNC。

[修复、输入一致性与完整报告](../../../build-release/preserve-m11-fix-e145/README.md)、[定向MTR逐项结果](../../../build-debug/preserve-m11-fix-e145/results.tsv)。E143其它原压力模型的失败不在本次修复范围，也未改判。未提交或push。

## E146：2026-10-11 500连接持续业务五轮复测

E145最终Release保持原500连接TEMP50／FETCH50／BOTH50／RW350及300秒业务，连续完成五轮。原1GiB预算、BALANCED、6个worker、1024工作槽、不限速不变，脚本／依赖与E142一致，五轮期间源码及二进制一致。业务在DRAIN开始后仍完成84084至124211条命令，未提前停止。

strict为1430.475／1035.768／899.484／729.640／797.969ms；ACK→READY为8.965／20.867／21.064／91.960／65.651ms，两项原关键指标均5/5通过。功能4/5：r2遗漏已EOF且COMMIT收到4020的FETCH连接95，436＋63仅499；其余四轮全部500连接正确归类。no_token_target剔除后未补session-only是源码强候选，缺逐token运行状态，不宣称根因已闭环，尚未修复。

原综合验收0/5，原exit1全部保留：r4有效EXACT729.035ms超500，其余四轮无eligible BODY，EXACT为N/A。五轮receiver无ERROR或后台完成失败；r1源端一次通信读取超时后交接完成，根因未查清。r4 READY后后台完成观察间隔1663.324ms，不能等同真实I/O或RESUME延迟。仅本地DRAIN／transfer／READY及原业务核账，无外部物理升主／SQL RESUME证据。

[五轮完整报告](../../../build-release/continuous-resource/e146-five-rounds/README.md)、[逐项指标](../../../build-release/continuous-resource/e146-five-rounds/summary.json)、[r2失败核查](../../../build-release/continuous-resource/e146-five-rounds/r2-classification-triage.md)。没有修改内核／测试源码或提交/push。

## E147–E151：后续修复、回归与清理（2026-10-11）

上述E146为修复前历史记录，原失败与当时未定根因的结论保留。E147用同版固定命令边界复现了活动只读事务在cursor EOF后被遗漏的机制：没有更新undo被错误当作没有需保留的事务状态，且正式READ_CONTEXT恢复仍依赖临时资源存在。原E146连接95没有内部轨迹，不能倒填其执行路径。[定位证据](../../../build-release/preserve-eof-triage-e147/README.md)。

E148在standby＋namespace ON范围内修复两处分类，复用既有独占接管与恢复合同；动态关闭检查和失败恢复保留。固定9连接对照及5个正式MTR完成旧版RED→新版GREEN，另有3个失败路径MTR验证快照、保存点及重试恢复。全量Debug no-bin／log-bin各调度934个业务用例、包含18个big-test；两模式初次各有1个旧protocol预期失败，补充锁与无undo的实际状态断言后串行通过。合并证据934项均在适用模式通过，不改写初次失败日志。[修复与全量证据](../../../build-release/preserve-read-context-e148/README.md)。

E148的500连接持续业务五轮本地功能及独立核账全部通过；strict满足2秒及2.2秒各4/5，ACK→READY低于500ms为5/5，仍有3轮有效EXACT超过500ms，原综合验收失败保留。E149–E150继续完成13个原有配置及M1–M12的62个配置：原有配置本地交接终态证据13/13完整，但原综合验收仅3/13；M矩阵原场景验收及独立核对均62/62通过，包含预期拒绝场景。M的56个成功迁移配置strict和ACK→READY均达标，EXACT均因无eligible BODY而为N/A，不能替代持续业务的尾部指标。[五轮报告](../../../build-release/continuous-resource/e148-five-rounds/README.md)、[完整压力回归](../../../build-release/preserve-pressure-e150/README.md)。这些压力结果来自E151清理前的Release，不是清理后重新验收。

E151仅清理8个内核文件中的确定冗余，新增7行／删除25行；保留必要引用清理、身份校验、取消及最后使用者的生命周期约束。Debug／Release均重新构建通过，26项定向MTR在no-bin／log-bin分别18通过／8条件跳过和20通过／6条件跳过，零失败、shutdown各通过，26项均至少在一个适用模式通过。未改测试或预期结果；三个独立上下文审查未发现新增阻断问题，两处旧故障注入点因仍有失败验证用途而保留，当前精确覆盖缺口未补。本轮没有重跑全量MTR或压测。[清理与验证](../../../build-debug/preserve-cleanup-e151/README.md)。

以上本地证据不替代物理复制工程的在线升主、生产proxy PS回放及恢复后首次业务访问验收。最新状态以[任务跟踪](task-tracker.md)的对应轮次为准；历史失败、条件跳过、性能未通过项与集成边界均保留。
