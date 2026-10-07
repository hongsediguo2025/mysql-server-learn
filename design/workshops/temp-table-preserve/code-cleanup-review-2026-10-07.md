# 临时资源代码清理清单与复核结论

日期：2026-10-07。目录 `/Users/a1234/project/mysql-server-8022-preserve-port`，分支 `ha_preserve_trx`。复核对应最近两次提交：

- `c9bc199a64d90b3881daec03bbdd4c5c4de14357`：临时表与保留游标结果传输。
- `290e94a156c1511ea46a5dc57d70550ffec45b5b`：临时资源提前准备与 final 增量传输，也是本次 HEAD。

本文保存完整清理编号、源码依据、删除边界及最新纠正，供后续实施逐项核对。**21 项局部清理结论维持；旧日志的一项测试依赖此前漏判；receiver FINAL 删除及三组统计的 Debug-only 调整需要配套处理。不能将整份清单理解为可以直接执行的删除列表。**

本轮已按用户授权实施 A01–A21、B01–B06、C01/C03–C05，已完成补丁 review 和下文列明的运行验证；C02 与 D01–D06 保留。实施前已有七个其他未提交路径及本文（合计八个），其中 parser 清理不计入本清单；这些改动均予以保留。下文依据行号是清理前的审计定位，实施状态和新证据见文末。

## 结论总览

| 编号 | 内容 | 当前裁决 |
| --- | --- | --- |
| A01–A21 | 无用字段、重复判断和查找、薄包装、内部声明与旧注释 | 限定范围内可清理 |
| B01 | 已退役的 receiver decode 调试统计 | 可删除，真实 decode 复用不变 |
| B02 | TEMP prebuild 单处逐步打印 | 可选删除，仅减少 Debug 噪声 |
| B03 | receiver FINAL 活动、欠账、批次统计与细分日志 | 配套处理初始化、ACK 采样和测试后可删 |
| B04–B06 | SOURCE_FINAL 子阶段、cursor 行数、undo 页数统计 | 可选改为 Debug-only，不按无用代码全删 |
| C01 | Phase1 三个纯观察字段 | 可裁剪，保留相邻功能判断 |
| C02 | early token selection 日志 | 纠正为暂留，现行 MTR 使用其顺序标记 |
| C03–C05 | 其他旧计时日志 | 可选精简，无已测性能收益 |
| D01–D06 | 扫描、编码及校验的进一步合并 | 条件优化，不纳入确定删除项 |

“可清理”指源代码依赖和生命周期允许在指定边界内变更，不代表补丁已经完成运行验证。“纯观察”也不等于没有测试或运维价值。源码行数、宏生成的状态项数量和运行时收益分别统计。

## A 组局部清理

| 编号 | 清理内容与位置 | 正确性依据及必须保留的边界 |
| --- | --- | --- |
| A01 | [sql_class.cc:892](/Users/a1234/project/mysql-server-8022-preserve-port/sql/sql_class.cc:892) 的第二组结果 owner 释放和 pending count 清零 | `cleanup_connection()` 已无条件调用 `cleanup()`，后者完成同样操作；之后的 init 和 PS 析构不重新安装 owner。PS 析构更新 open cursor 数，不是 pending 数。保留 cleanup 中第一次释放和原有清理顺序。 |
| A02 | [preserve_trx.cc:25887](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx.cc:25887) strict RESUME 的局部 `temp_materialized` 别名及 false 赋值 | 该局部没有有效读取；失败回滚由实际 attach journal 负责。保留共享 runtime 的 `temp_tables_materialized`，它仍被准备和其他恢复路径使用。 |
| A03 | [preserve_trx.cc:23475](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx.cc:23475) 内层重复 `!execution.error` | 外层已检查；两次读取之间没有调用或写入。保留实际资源有效性及 lock fence 检查。 |
| A04 | [preserve_trx.cc:6264](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx.cc:6264) 锁内重复 owner 指针比较 | 6260 已提前排除同一指针，owner 构造后不变。保留 THD 锁及 killed、释放状态等可变条件；其他没有前置判断的扫描器不能照删。 |
| A05 | [sql_class.h:391](/Users/a1234/project/mysql-server-8022-preserve-port/sql/sql_class.h:391) map 的多余 friend | result restore 只调用 map 的 public `find()`。`Prepared_statement` 中用于访问私有 cursor 状态的 friend 必须保留。 |
| A06 | [sql_class.cc:83](/Users/a1234/project/mysql-server-8022-preserve-port/sql/sql_class.cc:83) 重复 cursor include | 前面的 sql_class 头已通过 result_restore 头包含同一文件。仅清除此处重复 include。 |
| A07 | [preserve_trx.cc:2178](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx.cc:2178) Debug 下空的匿名 namespace 关闭和重开 | 中间没有声明，不改变前后定义的命名空间和链接属性。 |
| A08 | [preserve_trx.cc:916](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx.cc:916) 与 [sql_class.h:1431](/Users/a1234/project/mysql-server-8022-preserve-port/sql/sql_class.h:1431) 的旧 PS 注释 | 原注释对应已经删除的 PS capture 字段。只删失去说明对象的注释，不删当前 TEMP 或 cursor 状态。 |
| A09 | [temp_prebuild.h:37](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_temp_prebuild.h:37) capture input 的空 `undo_memory` | 无赋值、申请或有效读取，默认空 lease 不持有资源。保留 final 路径同名的真实 lease。类型大小及按 sizeof 计费随之变化。 |
| A10 | [temp_prebuild.cc:705](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_temp_prebuild.cc:705) IMAGE_TRANSFER 的重复 prepare | 唯一入口要求 `copy_image()==READY`，所有 READY 返回均已置 prepared，后续 prepare 只返回完成。保留 UNDO_TRANSFER 的 prepare，它有实际编码工作。 |
| A11 | [temp_restore.h:26](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_temp_restore.h:26) 的 `manifest_index` 及赋值、恒等检查 | definitions 按 ordinal 建立，字段始终等于所在槽位。跨代时新 input 与 plan 配对换入，旧 sql_ready 退役，再为新 manifest 建 definitions。保留完整性、数组大小、原生表及字典检查。 |
| A12 | [temp_table_carrier.cc:2472](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_temp_table_carrier.cc:2472) encoder 的第二套 ordinal set | 前置 `history_valid()` 已对同一不可变 manifest 做相同唯一性认证。只删第二套 set 及对应插入检查；decoder 的早期检查保留。 |
| A13 | [temp_undo_prebuild.cc:17](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_temp_undo_prebuild.cc:17) ownership claims 的私有转发层 | 可将原实现体移入唯一公开入口，原包装无锁或异常转换。保持公开签名、局部 RAII 作用域和析构次序。 |
| A14 | [temp_prebuild.cc:170](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_temp_prebuild.cc:170) discard 包装 | 唯一动作是 `captures.clear()`。在原位置直接调用并删除包装，必须仍在借用者 reset 之后执行，不能推迟到成员自动析构。 |
| A15 | [resource.cc:87](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_resource.cc:87) 内部 key 的旧 `operator<` | 匿名类型的实际 map 使用显式 `Token_kind_less`，无默认比较器或关系运算消费者。保留真正的比较器。 |
| A16 | [transfer.cc:10482](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:10482) 第二次 sealed_files 查找 | 同一 mutex 内前面已找到，期间没有修改容器，可复用 iterator。保留 guard 前声明的 retired_file，确保最后一个旧文件 owner 在解锁后析构。 |
| A17 | [file.cc:59](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_file.cc:59) 后面的三处重复零长度判断 | 入口已处理 length 为零；overlay 分支改变 length 后直接返回。保留入口检查、范围校验和真实 I/O 错误处理。 |
| A18 | [cursor_file.cc:99](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_cursor_file.cc:99) 私有 header helper 的重复 layout 校验 | 唯一调用点前已校验同一 descriptor，中间不修改输入。保留首次 layout 校验、文件身份及真正的头尾内容检查。 |
| A19 | [cursor_file.h:116](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_cursor_file.h:116) 内部 Debug helper 的头导出 | helper 只在 debug 实现文件内部被调用。配套删除声明、将定义改为 static；保留函数体及原 MTR 探针入口。 |
| A20 | [trx0temp_preserve_undo.h:66](/Users/a1234/project/mysql-server-8022-preserve-port/storage/innobase/include/trx0temp_preserve_undo.h:66) `undo_field::end` 及四个写入 | 字段只有写入，没有解码、导入或编码消费者。保留 `value_end`、虚拟新值解析、LOB 后缀校验及完整原始尾部重编码。 |
| A21 | [temp_prebuild.cc:909](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_temp_prebuild.cc:909) waiting 条件重复 | 原表达式与已有 `Impl::waiting()` 完全相同，可在同一锁内复用。不能替换为 cached waiting_count，也不能合并条件不同的 initial_baselines_complete 扫描。 |

A09、A11、A20 删除字段后，按实际类型大小计费，不能人为保留旧 charge，也不能宣称所有额度拒绝位置完全相同。A12 会减少分配及其失败机会，这是删除重复工作的结果。

A01、A14、A16 的安全性依赖精确的生命周期位置。实施时不得借机统一析构、推迟释放或扩大锁范围。

## B 组观测代码

### B01 与 B02 调试残留

B01 是 [transfer.cc:7777](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:7777) 的 receiver decode 计数类型、TLS、两处递增，以及 21423 附近的局部 scope 和日志，共 36 行。其历史 decode_once 消费已经退役，当前没有对应消费者。真实 decoded_frames 复用、身份验证和解码内存 lease 必须保留。

B02 是 [temp_prebuild.cc:726](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_temp_prebuild.cc:726) 的 4 行逐步打印，只读取 stage、space 和 progress。可以减少此处 Debug 噪声，但没有证据把整个 preserve_temp_import 日志类别归为临时实验代码，其他探针和错误诊断不随之删除。

两项在正常 Release 配置下均不执行，不能算作 Release 性能优化收益。

### B03 receiver FINAL 可撤除的范围

FINAL 的活动区间、输入欠账、批次、重试、结束统计及 `PRESERVE_TEMP_FINAL_V1` 日志没有生产准入、认证或 READY 决策读取。可以整组撤除，不需要另建计数器、注册表或线程来替代。

范围包括：

- [temp_metrics.h:99](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_temp_metrics.h:99) 的 FINAL field、sample、timing 和日志接口及对应实现。
- epoch 的 temp_final 字段、note 和 activity guard，以及各 BEGIN、prepare、bind、step、retry、READY、PARTIAL、cancel、shutdown 观察调用。
- `resource_final_observed` 和 begin_receive 的 first_resource_final 观察输出。
- [receiver_prepare.cc:142](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_receiver_prepare.cc:142) 的一次性欠账快照和专用标志。
- 23 个 FINAL 状态项，及只消费这些状态或日志的测试部分。

此前逐行清点为六个源码文件中的 304 个现有行，加三处保留行签名改写。这是删除范围盘点，尚非实施补丁净行数。正常未完成资源批次的这些观察会额外获取 epoch mutex 四次；删除可去掉相应静态工作，不能据此推算实际尾延迟收益。

必须保留功能性的 epoch map 和 mutex、fact、token results、binding 和 selection 状态及其 guard、真实准备和预算、队列、取消、purge erase、shutdown swap。shutdown swap 保证退役 map 在锁外析构；不能概括为当前所有 purge 清理也都在锁外析构。receiver_prepare 的 m_scanned_bytes 仍供 RECEIVER_RESULT 阶段计时使用，m_batches 仍有 Debug 行为探针；删除 FINAL 后失去调用者的 scanned_bytes() getter 一并清理。

### B03 必须纠正的 ACK 采样结论

旧 FINAL BEGIN 和 activity guard 会通过 map 的 operator[] 顺带创建默认 epoch 记录。业务判定可以区分默认记录与已准备状态，但独立 ACK 采样使用 find，依赖记录存在。

| 路径 | 当前顺序 | 删除时的判断 |
| --- | --- | --- |
| 正常首次 strict COMMIT 且 cache 成功 | cache fact，然后 callback 发送 ACK | 不依赖 FINAL 的提前创建 |
| strict COMMIT 接受后 cache 发生异常 | accepted 已发布，cache 失败，可能由兜底路径发送已提交 ACK | 不能保证删除前后 per-epoch ACK 采样相同 |
| 重复 COMMIT | 先 callback 发送 ACK，再尝试 cache 和 bind | 正常重放通常已有记录；仍须核对失败与重试边界，不能只用首次成功路径证明 |
| legacy non-strict COMMIT | ACK 后才从 committed fact 建 cache | worker 尚未分类时，删除提前创建可能丢失 per-epoch ACK 时间 |

严格模式的静态条件反例为：旧 FINAL BEGIN 成功建立默认记录，worker 尚未分类；COMMIT 已发布 accepted，随后 cache 的查表 key 构造发生一次瞬时 bad_alloc。旧版记录仍在，纯删除版可能尚无记录。异常被捕获后，因 commit apply 已开始，不走 precommit purge；[兜底 COMMIT 查询](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:21679) 仍可发送已提交 ACK。[ACK 的 find](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_transfer.cc:21332) 因此可能旧版命中、删除版缺失。

这只证明“所有 strict 路径天然保持采样等价”的说法过强，**不是已经运行复现的数据或事务故障**。legacy 分支也尚未证明此类资源 envelope 最终能 READY。

实施 B03 时，应在既有路径中明确保留必要的 epoch 初始化和 ACK 采样语义，不能继续依赖观察代码的偶然副作用。任何 ACK 发送后的诊断分配失败都不能改变已经发送的业务结果。原生功能 map 不能删除，也不需要新增独立生命周期机制。

### B03 配套消费者和报告输出

删除 FINAL 状态及日志时，必须同步处理以下消费者。只删除内核定义，或只修改 READY benchmark，会遗漏现行硬断言。

| 入口 | 当前依赖及漏改后果 | 配套处理与保留边界 |
| --- | --- | --- |
| [temp_ready_benchmark.py:467](/Users/a1234/project/mysql-server-8022-preserve-port/scripts/preserve_trx_temp_ready_benchmark.py:467) | 直接断言 final_tokens、ready_epochs、staged_tokens、dropped 和 active；指标缺失将报覆盖不完整 | 退役 FINAL 专用断言；保留实际 READY、资源提前准备、目标端原有临时表隔离、数据和回滚检查。 |
| [temp_pressure_benchmark.py:585](/Users/a1234/project/mysql-server-8022-preserve-port/scripts/preserve_trx_temp_pressure_benchmark.py:585) 的 run_mixed | 当前 Release 压力入口直接索引 final_tokens，要求其等于非 ordinary owner 数；删除字段后会 KeyError，并在 590–593 行将结果标为失败，即使 DRAIN 和 READY 已成功 | 退役这条 FINAL 计数断言；保留 survivor 集合、实际 READY、业务线程结束、完整命令边界 4020、worker error 及业务结果断言。 |
| [temp_contract_e2e.py:1708](/Users/a1234/project/mysql-server-8022-preserve-port/scripts/preserve_trx_temp_contract_e2e.py:1708) 的负向 delta 路径 | 等待 final_active 归零，并比较 FINAL token、READY、PARTIAL 和 dropped 计数 | 退役观察等待、专用计数及其专用快照；保留负向 READY、损坏 delta 拒绝、依赖检查和目标端原有临时表 DML、回滚。final_active 只表示观测结束，不能替代 worker 或资源清理证据。 |
| [temp_contract_e2e.py:2282](/Users/a1234/project/mysql-server-8022-preserve-port/scripts/preserve_trx_temp_contract_e2e.py:2282) 的正向 stage_metrics 路径 | 断言 FINAL 欠账、批次和字节；要求恰有一个新增 epoch ID，并检查该 ID 最后保留的日志样本为 READY、起止时间与四段 wall 之和一致。同 epoch 的重复日志会被字典覆盖，当前不验证原始日志条数唯一 | 同步处理 630 行 final_samples 日志读取、643 行旧 epoch 快照、2282–2301 行专用断言和 2302 行输出说明；保留 SOURCE_FINAL 总计、receiver 准备、首次 DML/FETCH、物理阶段隔离等其他断言。 |
| [temp_stage_metrics_cancel.test:4](/Users/a1234/project/mysql-server-8022-preserve-port/mysql-test/suite/preserve_trx/t/temp_stage_metrics_cancel.test:4) 及其 [result](/Users/a1234/project/mysql-server-8022-preserve-port/mysql-test/suite/preserve_trx/r/temp_stage_metrics_cancel.result) | 查询 FINAL 状态，重启收尾后要求 CANCELLED 日志及 wall 分解一致 | 退役 FINAL 专用查询、日志断言和对应输出；不借此删除共享传输场景、真实清理逻辑或其他清理验证，也不把该日志当资源已释放的证明。 |
| [temp_stage_metrics_off.test:18](/Users/a1234/project/mysql-server-8022-preserve-port/mysql-test/suite/preserve_trx/t/temp_stage_metrics_off.test:18) 及其 [result](/Users/a1234/project/mysql-server-8022-preserve-port/mysql-test/suite/preserve_trx/r/temp_stage_metrics_off.result) | OFF 隔离查询包含 final_tokens 名称；其他计数仍可能使 SQL 聚合返回零，因此不能只看用例结果判断消费者已清完 | 从查询和输出中去掉退役名称，保留首次 DML、物理阶段计数及原有 OFF 业务、回滚和隔离检查。 |

正向 contract 的现行入口包括 [temp_stage_metrics.test](/Users/a1234/project/mysql-server-8022-preserve-port/mysql-test/suite/preserve_trx/t/temp_stage_metrics.test)、[temp_stage_metrics_ps.test](/Users/a1234/project/mysql-server-8022-preserve-port/mysql-test/suite/preserve_trx/t/temp_stage_metrics_ps.test)、[temp_stage_metrics_first_error.test](/Users/a1234/project/mysql-server-8022-preserve-port/mysql-test/suite/preserve_trx/t/temp_stage_metrics_first_error.test)。它们仍验证其他保留指标，不能整组删除；调整 Python 输出文字时，同步核对各自的 .result。

共享 [capture_metrics():224](/Users/a1234/project/mysql-server-8022-preserve-port/scripts/preserve_trx_temp_ready_benchmark.py:224) 还通过动态名称和 `.get(..., 0)` 汇总四个 FINAL wall 分量。若仅删除状态，它仍会输出 `source_final_wall_us=0` 和 `receiver_final_wall_us=0`，不会报告采样错误；这会把未采集误报为零耗时。必须同步退役这两个派生字段，以及 [counter_semantics:315](/Users/a1234/project/mysql-server-8022-preserve-port/scripts/preserve_trx_temp_ready_benchmark.py:315) 中 FINAL 专用说明，不保留恒零输出或新增替代计数。

该采集函数还被 [continuous_resource_benchmark](/Users/a1234/project/mysql-server-8022-preserve-port/scripts/preserve_trx_continuous_resource_benchmark.py:35)、[temp_pipeline_benchmark](/Users/a1234/project/mysql-server-8022-preserve-port/scripts/preserve_trx_temp_pipeline_benchmark.py:23) 和 [temp_pressure_benchmark](/Users/a1234/project/mysql-server-8022-preserve-port/scripts/preserve_trx_temp_pressure_benchmark.py:23) 导入，也用于 ready 脚本的业务、READY 和外部后续采集入口。实施后检查这些报告不再生成退役派生值；保留通用状态采集、其他阶段指标及采样失败记录。严格 Phase2 与 ACK→READY 使用独立日志端点，不能随 FINAL wall 报告一起删除。

### B04 至 B06 可选保留 Debug 统计

| 编号 | 统计范围 | 现有验证价值 | 本批必须保留的内容 |
| --- | --- | --- | --- |
| B04 | SOURCE_FINAL 五个子阶段，8 个 RAII 点、40 个 SHOW 字段 | Phase1 子阶段计数不增长；commit 模型的 final 四子阶段全局调用增量各等于 owner 数、verify 为零，不能单独证明每个 owner 恰好一次 | SOURCE_FINAL 总计与 TLS 字节累计、实际 tail、close、digest、seal 和 validation |
| B05 | [cursor_decode.cc:447](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_cursor_decode.cc:447) 的 preflight_rows 和 decoded_rows | 已准备结果不重扫；放弃后从头验证；实际恢复解码路径被执行 | 本地 row、offset、checked_rows、values_validated、文件验证、预算和位置 |
| B06 | [undo_capture.cc:157](/Users/a1234/project/mysql-server-8022-preserve-port/storage/innobase/trx/trx0temp_preserve_undo_capture.cc:157) 的 routed 和 used | 逐 owner 写入后捕获事件计数增长，扫描器消费过捕获批次中的页镜像 | owner、cookie、generation、watch、batch、lease、真实额度和关闭行为 |

routed 在已有页镜像被再次覆盖时也递增，不是新增页或去重页数量；used 在 take 成功取出批次中的页镜像时递增，也不是整个迁移过程的唯一页数。[owners E2E:351](/Users/a1234/project/mysql-server-8022-preserve-port/scripts/preserve_trx_temp_owners_e2e.py:351) 逐 owner UPDATE 后检查全局 routed 增长，再检查 used 增长；其验证价值是看护捕获和消费路径，不能扩展为逐页身份或新增页数量的证明。

三组全局统计不参与生产决策；若目标是减少 Release 统计开销，可将现有写入、声明、实现、getter、SHOW 注册统一限制在同一 Debug 编译条件中。不能只隐藏 getter，或保留返回恒零的状态项。

这属于可选调整，**没有实测证明它是当前性能瓶颈或能够达到多少收益**。调整后 44 个 Release SHOW 名称消失，是明确的状态接口变化；本仓搜索无法证明外部监控没有消费。

当前直接断言入口已经要求 Debug，Python 使用 MTR 提供的源和 receiver 端口，没有暗中启动 Release mysqld。因此保留 Debug 统计可保住现行精确工作量断言；它们本来就不是这些用例的 Release MTR 覆盖。Release 压测对这三组主要是泛收集，未找到依赖其值的等待、负载控制或硬门槛。

普通数据、FETCH 顺序、回滚断言不能等价证明“不重复工作”；SOURCE_FINAL 粗粒度 read_bytes 也不能直接替代。不要为了删统计而悄悄撤掉 Debug 工作量看护。

编译条件必须一致：当前 Debug 无 NDEBUG 和 DBUG_OFF，Release 两者都有，但 CMake 其他配置可以单独定义 DBUG_OFF。不能在 writer 用一种条件、声明或 SHOW 用另一种条件。只让 Release timer 选择 NONE 仍留下函数调用，不是完整退出。

## C 组可选诊断收敛

| 编号 | 内容与位置 | 最新裁决与边界 |
| --- | --- | --- |
| C01 | [phase1_pipeline.h:189](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_phase1_pipeline.h:189) 的 temp_steps、ordinary_temp_slow_operations、temp_credit_in_use_bytes 快照 | 可删除纯计数、快照、输出和仅为计数存在的局部变量。真实 family credit ledger、等待额度、operation budget 及 BINLOG 统计保留。 |
| C02 | [preserve_trx.cc:21732](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx.cc:21732) 的 early token selection 计时和日志 | 暂留。原“没有自动化消费者”的判断错误，MTR 用该 marker 验证 pipeline STOPPED 早于 Phase2 target work。 |
| C03 | [preserve_trx.cc:22074](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx.cc:22074) 的超过 100 ms target slow 日志及专用计时 | 可选精简，未找到现行自动断言；不能顺带删 execution.result 中其他用途的通用计时。 |
| C04 | [preserve_trx.cc:22617](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx.cc:22617) 的 survivor prune 计时和日志 | 可选精简；保留真正的 survivor 集合构造与批量清理。 |
| C05 | [preserve_trx.cc:23535](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx.cc:23535) 的 final candidate 累计、最大值和日志 | 可选精简，历史性能报告仍引用。保留相邻 final_fast_scan_us 和实际 validation、replacement 逻辑。 |

C01 尤其不能顺删 [ordinary TEMP_STEP overrun 分支](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_phase1_pipeline.cc:1346)。这里允许普通 TEMP_STEP 的慢同步 I/O 不直接触发取消；计数可以去掉，非致命处理条件必须留下。

C02 的现行消费者是 [standby_transfer_drain_no_shutdown.test:156](/Users/a1234/project/mysql-server-8022-preserve-port/mysql-test/suite/preserve_trx/t/standby_transfer_drain_no_shutdown.test:156)，171 附近断言 STOPPED 的日志顺序早于该 marker。属于事后顺序验证，不是 workload 控制。若以后改为仅 Debug marker，仍须完整保留此证明；不能只删除断言。C03–C05 来自更早的性能诊断，不能仅因当前搜索不到测试消费就认定无运维价值。

## D 组需要进一步证明的合并

| 编号 | 候选内容 | 允许研究的范围 | 不能忽略的条件 |
| --- | --- | --- | --- |
| D01 | [preserve_trx.cc:10731](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx.cc:10731) async open_phase1 的 idle 和 active 两次 THD 扫描 | 可考虑复用现有 filter_idle=false 收集一次；此处两组都只标记 capture epoch | 保留锁内资格检查和 pin、先全体收集再标记、先登记清理所有权再发布 epoch。两次扫描间 THD 集合和 idle 状态可变，不能宣称逐调度等价；不扩大到 non-async 或后续真实 capture。 |
| D02 | [receiver_retired.cc:442](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_receiver_retired.cc:442) 对整个 map 的两次 epoch 筛选 | 可在同一 epoch 的有界范围内保留两遍 | 必须先全部 pin 成功再 cancel。边 pin 边 cancel 会在后续分配失败时留下部分取消。lower_bound 的 string key 构造放在现有 try 内，iterator 继续受原 mutex 保护。 |
| D03 | [cursor.cc:289](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_cursor.cc:289) 行长计算和编码的两次 Field::pack | 研究同一行内复用已编码值 | 长度前缀及可变长度字段决定顺序；一遍 pack 可能引入整行 scratch 和更高峰值内存，不能只数调用次数。 |
| D04 | [result_transfer.cc:142](/Users/a1234/project/mysql-server-8022-preserve-port/sql/preserve_trx_result_transfer.cc:142) 全 N 工件验证后，对 S 个选中结果再次 describe | 研究单次调用内复用已验证 descriptor | 所有声明的 N 件仍须认证，不能只验 S 件。保持预算生命周期、文件保活、错误优先级和失败不发布部分 Snapshot。 |
| D05 | [trx0temp_preserve_input.cc:189](/Users/a1234/project/mysql-server-8022-preserve-port/storage/innobase/trx/trx0temp_preserve_input.cc:189) 重复读取 98 字节 header | 仅在已有结构能自然承载时局部复用 | 保留检查、读取计量和错误次序；收益小，不应为此增加适配层。 |
| D06 | [trx0temp_preserve.cc:4566](/Users/a1234/project/mysql-server-8022-preserve-port/storage/innobase/trx/trx0temp_preserve.cc:4566) 活动页版本和 map 重复查找 | 先证明同一 capture round 内的版本账本不变量 | 取消、换代和迟到 TLS 访问必须安全；保留 dirty_page_versions、capture_sequence 和相关探针，不能把时序检查当重复判断。 |

这六项都不增加跨轮缓存、独立线程池或新外部阶段。能够减少扫描或调用只是局部事实，是否值得实施还要结合内存、失败路径和性能对照。

## 保留项与实施边界

以下内容不纳入本批删除：

- 公开 cursor descriptor 的 version 常量，以及物理工程使用的 cursor attach 和既有升主接口。外部源码不可访问，不能以本仓无调用为由删除公开契约。
- 源端严格 Phase2 的 `PRESERVE_PHASE2_FINAL_V1`、receiver 的 `PRESERVE_RECEIVER_READY_V1` 与实际 ACK、READY 时间端点。保留原 2 秒指标和用户接受的 2.2 秒容忍说明，ACK→READY 仍要求小于 500 ms。
- SOURCE_FINAL 总计、首次 DML/FETCH、物理 prepare、resurrect、adopt 的必要计时和错误、资源指标。本批不删除整个 temp_metrics 模块。
- 认证、ID 隔离、额度、RAII、引用保活、异步退役、原始和安装镜像、graph 的 COUNT→RESERVE→BUILD、不同阶段和不同代次的独立校验。
- 生命周期和错误策略不同的 pin 或 capture 请求代码，不因外观相似强行增加统一 helper；读取动态开关的重复谓词也不能无证明地缓存。

功能范围继续限定为 standby transfer、物理备机在线升主及 SQL RESUME。保持 `preserved_trx_prepare_before_trx_sys_init_for_physical_promotion()`、在线升主也会调用的 `trx_lists_init_at_db_start()` 内 Preserve hook，以及 `preserved_trx_adopt_ready_epoch_for_physical_promotion()`；SQL RESUME 仍进入 `Sql_cmd_resume_preserved_transaction::execute()`，不增加外部升主阶段。PS 与 session 回放由物理复制工程承担；不恢复已删除的 PS transfer，也不扩展 RESET DRAIN 或 local startup。

## 实施顺序与验收

1. 先独立处理 A01–A21 和 B01；B02 可随同做小范围 Debug 噪声清理。保持补丁可逐项对应，不夹带 D 组优化。
2. B03 单独处理：同时核查初始化、strict 正常和异常 COMMIT、重试 ACK、适用的 legacy 路径，逐项处理上表的硬断言、日志读取、黄金输出、共享派生值和说明。核心采样和功能状态保持；既不能使成功业务因缺少退役指标报失败，也不能把未采集耗时显示为零。
3. B04–B06 仅在明确采用 Debug-only 时实施。调用、符号和注册一致退出 Release，保留原 Debug 工作量断言，并明确 44 个 Release 状态项变化。
4. C01 可单独裁剪；C02 暂留；C03–C05 按诊断价值选择。D01–D06 各自证明后再决定，不以减少行数代替正确性论证。

内核变更后使用既有 Debug 和 Release 构建，运行对应 MTR 和 Python E2E。关注连接清理、继续 FETCH、TEMP 换代、undo 虚拟列和 LOB、损坏 manifest、额度拒绝、取消和释放，以及 ACK 与 READY 的不同先后顺序。不新增 GUnit，不在新 MTR 中使用 DEBUG_SYNC。

涉及性能的调整使用同一负载、预算和阈值对照，分别记录严格 Phase2、ACK→READY、业务延迟和资源占用。不能将功能用例通过当作性能验收，也不能仅以删除锁次数或原子操作次数宣布目标已达成。

## 实施前审计证据（历史）

初稿结论经过三个独立上下文的只读复核及主审关键路径复查。文档创建时核验的 158 个内核文件内容、此前七个未提交路径和 diff 均保持不变，没有实施清理。随后三位审查者各自覆盖全部 38 项，确认需补齐 B03 的强制消费者和派生报告，并收窄 B06 的统计含义；这些修改已经纳入上文。当时只修订本文，内核、测试和脚本中的清理尚未实施；现已由文末 E101 记录更新实施状态。

完成上述修订后，再启动三个全新、未继承主会话历史的独立上下文，分别侧重逻辑与失败路径、并发与生命周期、测试消费者与编译条件。每位均完整核对全部 38 项，没有分摊条目，也没有实施清理。逻辑和生命周期复核没有新增待修正问题；消费者复核发现一处 P3：正向 contract 只验证新增 epoch ID 数为一，不能证明同 epoch 原始日志只输出一条。主审对照实际字典覆盖和断言代码修正了 B03 表述，并由发现者及生命周期审查者回读确认。

本轮没有剩余的已确认文档错误，A01–A21 的局部清理边界维持，B03 仍要求配套处理，C02 仍暂留，D01–D06 仍属于条件研究。这个结论限定于当前源码的静态复核，不保证尚未形成的清理补丁已经具备运行正确性或性能收益。实施时仍须按上文边界核对实际 diff 并完成验证。

此前 A01–A20 的隔离候选补丁为 17 个文件、增加 22 行、删除 74 行，净减 52 行；使用真实 Debug 和 Release 编译参数，对 20 个编译单元共做过 40 次成功的隔离语法检查。上述审计时补丁未应用，也未完成链接或运行测试。A21 不在该补丁和 40 次检查内。上述实施前新增复核本身没有构建或运行 MTR、E2E。

本地证据入口为 [局部清理复核](/Users/a1234/project/mysql-server-8022-preserve-port/build-debug/preserve-redundancy-e98/README.md)、[未应用的候选补丁](/Users/a1234/project/mysql-server-8022-preserve-port/build-debug/preserve-redundancy-e98/candidate.patch)、[观测清理分析](/Users/a1234/project/mysql-server-8022-preserve-port/build-debug/preserve-observation-e100/README.md)。这些 build 目录属于本地证据，可能不随 Git 分发；后续实施以本文的完整编号和源码为准。

旧观测报告中关于 C02 无自动消费者的说法，以及将正常 strict cache 顺序推广到所有异常和重试路径的说法，以本文纠正为准。2026-10-05 的[旧收敛审计](/Users/a1234/project/mysql-server-8022-preserve-port/design/workshops/temp-table-preserve/redundancy-and-consolidation-audit-2026-10-05.md)保留其已实施内容，不重复计算为本次待清理代码。

## 本轮实施记录（E101）

A01–A21、B01–B06、C01/C03–C05 已形成实际补丁。B03 只建立一次必要的默认 epoch：首次 `publish_accepted_epoch()` 成功后，在原 registry mutex 内、功能性异常回滚块之外初始化已有 ready map。重复 COMMIT 和 ACK 不补建；ACK 继续 `find`，诊断分配或日志异常不妨碍后续 terminal retention 登记。保留 READY binding、PARTIAL scope guard、purge erase 和 shutdown 的锁外退役。此处没有增加注册表或独立生命周期。

FINAL 的 23 个状态项和专用消费者退役；三个 Release 压测入口共享的 FINAL wall 派生值不再输出。删除 `temp_stage_metrics_cancel` 的 test/result/cnf，仅因它是 FINAL 观测专用包装；其引用的 `temp_source_transfer_cursor` 基础传输用例仍保留并纳入全量回归。SOURCE_FINAL 五个子阶段、cursor 两个行数和 undo 两个页数统计统一以 `NDEBUG` 限制为 Debug，共 44 个状态项退出 Release，原 Debug 工作量断言保留。

实施前文件与 SHA、原 dirty diff 保存在 `build-debug/preserve-cleanup-e101/`。三个只读审查者均覆盖完整实际补丁，没有剩余确认缺陷。相对本轮前置状态，31 个内核文件增加 115 行、删除 538 行，净减 423 行；不计原有 parser 清理。收尾还删除 TEMP 准备分支仅为已退役 getter 计算的两行读数，实际 TEMP/RESULT 计时保留。

全量 Debug MTR 使用 `--big-test --parallel=4 --retry=0 --force --max-test-fail=0`，两模式顺序执行：no-bin 603 通过／295 条件跳过／0 失败；log-bin 607 通过／291 条件跳过／0 失败，shutdown 两轮通过。898 项业务入口并集全部通过，18 个不同 big test 全部通过。

全量后补删上述两行无消费者计算；最终版本重新通过 Debug/Release 构建链接，并完成 14 项双模式定向回归：no-bin 6 通过／8 跳过，log-bin 9 通过／5 跳过，均零失败、shutdown 通过，14 项并集全部通过。两行的读数 getter 无副作用，TEMP 分支返回后下次 step 先清零，RESULT 不会消费它们。完整矩阵和最终收尾复测的版本边界分别记录，未宣称再跑了一轮全量。

二进制核验确认 44 项统计保留 Debug、退出 Release；23 项 FINAL 名称两端均退役，严格 Phase2 与 ACK→READY 端点保留。脚本语法及 diff 空白检查通过。没有新增 UT/GUnit、DEBUG_SYNC 或外部阶段，没有提交/push，不据此宣称压力性能改善。详见 [E101 验证记录](../../../build-debug/preserve-cleanup-e101/README.md)、[逐项结果](../../../build-debug/preserve-cleanup-e101/mtr-results.tsv)、[补丁 review](../../../build-debug/preserve-cleanup-e101/review.md)。
