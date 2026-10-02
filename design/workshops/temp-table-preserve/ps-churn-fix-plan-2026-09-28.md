# M10 长尾修复实施计划

> **2026-10-04 版本说明：历史版本记录。** 文内“当前/尚未完成/通过”和源码路径均属于记录时点；旧 PS 定义/参数/重建方案不再适用，原失败与测量不改写为新版本成绩。 现行职责见[详细设计](detailed-design.md)和[PS 删除与 cursor 关联](ps-transfer-removal-and-cursor-attach.md)，最新状态见[任务跟踪 E63/E64](task-tracker.md)。下文保留历史事实，不作为现行实施清单。

目标：按 E37 已确认根因消除中小结果每代文件生命周期及 receiver DELTA 的重复派生文件写，保持真实宽结果＋DML负载、预算、协议与固定升主入口。用户已授权实现，不提交/推送。

方案取舍：不只增大固定数组，不增加后台线程。cursor 保留64KiB基础缓冲，按需几何增长至每工件1MiB，先计新旧块峰值再分配；额度不足/分配失败/超过上限使用原spill，成功spill后退回基础缓冲。元数据已hash的前缀不重算，sealed后地址不再改变。修改仅在cursor专属文件。

receiver 在既有delta_reader中保留完整顺序校验与SHA，索引已校验PATCH块，完成后由sealed_file只读组合BASE/PATCH。缺项始终读固定BASE，不能读旧代派生结果。canonical shared_ptr与索引lease保留到最后读者。索引预算/分配不足时使用现有顺序派生文件回退，并把真实写入纳入receiver阶段与节流计量；不能以省略校验换性能。不新增worker/promotion/resume入口。

- [x] 先扩展 cursor_result_storage：84KiB、原64KiB边界、中间扩容、1MiB边界、索引spill、真实预算拒绝扩容、重复EXECUTE/FETCH/释放归零；用旧Debug观察新增断言RED。
- [x] 在已有native跨代 a→b→a E2E中，记录receiver每代Created_tmp_files，要求delta准备不新增派生临时文件；旧Debug观察RED。保留原ID/READY/SQL RESUME/回滚断言。
- [x] 实现cursor有界扩容；更新原写后故障专用查询保证仍超过spill阈值。
- [x] 实现已认证只读delta组合视图与低内存fallback，补足失败step读写记账。
- [x] 构建Debug，两个RED→GREEN；覆盖文件/索引/预算、DELTA digest/order/duplicate/missing/cancel/retry、native rollback/LOB、F03/F05/F06与OFF路径。MTR/Python，无新UT或DEBUG_SYNC。
- [x] 构建Release，冻结新输入；原宽结果M10 ps4/ps16各3轮，原预算、DML、无业务ACK暂停；保持READY、capture0及receiver原TEMP隔离。另验大结果spill/预算回退。
- [x] 独立只读审查代码和证据，修复发现的问题；更新根因/修复报告、任务跟踪；清理独占测试数据，保留RED、GREEN与性能证据。

证据目录：`build-debug/ps-churn-fix-20260928/`及`build-release/ps-churn-fix-20260928/`。E37旧源码和运行证据不覆盖。最终成功标准是对应开销路径已消除、原功能不变和原负载复测，不把所有大小结果的OS文件长尾都写成消失。

执行结果与失败记录见[修复报告](ps-churn-fix-2026-09-28.md)。实际新增三组旧→新确认，合计12有效样本；3次旧二进制加载失败保留。38项Debug定向业务及3项Release业务有GREEN，未进行全量回归。READY尾与内存代价独立报告，不能把成本修复等同全部NFR验收。
