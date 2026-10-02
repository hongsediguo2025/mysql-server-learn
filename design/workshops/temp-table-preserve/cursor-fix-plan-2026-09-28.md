# 结果捕获与历史代积压修复计划

> **2026-10-04 版本说明：历史版本记录。** 文内“当前/尚未完成/通过”和源码路径均属于记录时点；旧 PS 定义/参数/重建方案不再适用，原失败与测量不改写为新版本成绩。 现行职责见[详细设计](detailed-design.md)和[PS 删除与 cursor 关联](ps-transfer-removal-and-cursor-attach.md)，最新状态见[任务跟踪 E63/E64](task-tracker.md)。下文保留历史事实，不作为现行实施清单。

依据 `cursor-root-cause-2026-09-28.md` 及用户的修复指令。在当前工作目录直接实施；不提交，不改变限额、超时、线程池或外部升主阶段。

- [x] 保留同一旧 Release 的11轮根因实验作为 RED：两档份数额度均耗尽，正常结果捕获产生两个文件。
- [x] 增加无DEBUG_SYNC的MTR/Python存储边界用例：小结果无捕获文件、64KiB两侧、大结果/大索引spill、FETCH内容、重执行/CLOSE及资源归零。先在旧内核确认失败。
- [x] `preserve_trx_cursor.{cc,h}` 与 `preserve_trx_file.{cc,h}`：复用64KiB缓冲封存小结果，数据和4KiB索引缓冲溢出才建文件；SHA按缓冲块计算，行编码一次预留完整行额度。schema摘要与失败释放保持精确，DEBUG验证使用独立scratch。
- [x] `preserve_trx_ps_pretransfer.cc`：一个owner尚有待发送快照时不继续采下一批结果；已申报对象全部完成SEAL，final pin不受可选普通采样限制。这样保留连续提前发送，但待发量不随同一PS的EXECUTE次数积累。
- [x] `preserve_trx.cc`：先消费完成工作，再检查是否已经完成普通基线；分别记录队列排空与各owner首次基线完成。初次DATA copy必须继续到checkpoint尝试完成；全体首轮及record/binlog基线完成后停采、继续泵完已排队作业，再进入final。登记被容量挡住的owner并优先给未开始者机会。PS每TEMP捕获轮最多采一批，不随每个worker step补货。
- [x] `preserve_trx_receiver_candidates.cc`：同statement只保留最新可选decoder；旧worker有界退出、不覆盖新代；final精确匹配，必要时在READY前重新准备。已声明对象及认证账本保持不变。
- [x] Debug构建并运行新用例和现有cursor capture/decoder/restore、PS pretransfer supersede/close/large/abandon、TEMP连续增量及strict resume定向MTR。新增用例使用真实SQL和有界状态等待。
- [x] Release构建，重跑原常态OFF/ON与DRAIN、固定空闲复现，比较p99、区间CPU、文件数、live名额、捕获失败、receiver资源及READY。若仍有TEMP历史容量失败，独立沿代码定位处理，不把它标成已解决。
- [x] 主代理检查diff，独立只读review，更新原报告/任务跟踪，保留失败与修复后证据，清理本轮停止实例的数据目录。

不采用只发送每个PS第一代的永久latch：它会取消已有supersede普通阶段预准备行为。采用有界待发批次和完成后及时结束普通采样，兼顾既有多代提前准备和收敛。

- [x] 补充长事务空DML标记修复：38K+交替行修改用例在旧逻辑RED；standby以布尔摘要代替逐行空记录，所有sidecar新鲜度统一比较逻辑history_sequence。DDL/savepoint记录与预算保留；迁移后两种回滚及早期undo换代验收进行中。

- [x] 追加诊断发现仅限pin仍会不断制作TEMP checkpoint，旧600s ordinary窗口内历史文件持续膨胀。中止该诊断保留原始状态，追加首次基线完成判据，中间版本按旧15s/10s负载重跑均通过，最终构建继续精确复验。单owner多代探针需要有真实未完成基线窗口，不以热DML必须无限续轮为合同。

最终结果：v8 Debug/Release构建成功；定向22项不同业务通过，最终v8复验5项；13轮Release严格验收通过。见cursor-fix-2026-09-28.md和task-tracker.md E34。精确OOM/未admit owner退役故障仍只作源码审查边界，不宣称直接运行覆盖。
