# 临时资源代码冗余与合并审计

日期：2026-10-05。目录 `/Users/a1234/project/mysql-server-8022-preserve-port`，分支 `ha_preserve_trx`，源码基线 `c9bc199a64d90b3881daec03bbdd4c5c4de14357`。以下行号对应此基线。

本文记录临时表 ID 隔离、TEMP 导入和独立 cursor 结果链路的代码收敛结论及实施结果。**基线确认可清理的 24 行内核原文、3 处条件子句和 1 行重复测试声明已处理；无用名称复制和可保持现有契约的重复逻辑已收敛。** 前者包括替换唯一调用后删除包装函数，数量不代表补丁的净减行数。未发现可以可靠整块删除的生产模块；这也不是整个 MySQL 仓库的死代码清单。

下文审计位置和原始数量仍对应上述基线；实施进度及验证证据集中列在文末。按照“直接清理”“局部复用”“需要先确定预算及错误顺序的合并”分别处理，不把不同阶段的认证折叠为一次认证。N＋S 次文件头尾读取和预留计数 CAS 本轮保留，理由见实施记录。

## 确定可以直接删除的内容

| 位置 | 内容 | 确定性依据 | 删除边界 |
| --- | --- | --- | --- |
| [srv0tmp.cc](../../../storage/innobase/srv/srv0tmp.cc)，127–129 行 | 插入预留集合后 `if (!result.second)` 的 3 行计数回退 | `reserve_or_keep_preserved_space_id()` 自 105 行起持续持有同一个 reservation mutex，112–114 行已排除集合中存在该 ID。期间没有解锁或回调，向同一 `std::set<space_id_t>` 正常插入必然成功 | 只删不可达重复插入分支。保留 `created` 赋值、预留计数及 131–135 行的异常回退；内存分配失败仍可发生 |
| [preserve_trx_temp_table.cc](../../../sql/preserve_trx_temp_table.cc)，393 行 | `(void)descriptor;`，1 行 | 同一函数前面已读取 `descriptor.image_bytes` 和 `descriptor.image_digest`，该语句没有作用 | 不删参数或前面的大小、摘要赋值 |
| [preserve_trx_cursor_decode.cc](../../../sql/preserve_trx_cursor_decode.cc)，97–100、102 行 | `sent.length/charsetnr/flags/decimals/field` 的 5 个赋值 | 私有 `Column::sent` 后续只读取 `type` 和 `col_name`，不会整体传给 sender | 保留 88–96 行的 wire 字段读取、`sent_length` 范围、`sent_field<=1` 及其余拒绝检查 |
| [preserve_trx_receiver_candidates.cc](../../../sql/preserve_trx_receiver_candidates.cc)，149–150、211–214 行 | 两个不可达名称冲突判断及一个只供判断使用的局部变量，共 6 行 | map 以 statement ID 为 key；相同 generation 的对象名由二者唯一确定。`slot()` 是 id/generation 的唯一写入点，输入来自严格 canonical 解析或同一确定格式化函数 | 保留过时代次判断、descriptor 匹配、working/failed/final_claimed 状态。证明不要求 `take_result()` 的所有输入非零；零值格式化也仍唯一 |
| [preserve_trx_receiver_candidates.cc](../../../sql/preserve_trx_receiver_candidates.cc)，154 行 | `\|\| result->final_claimed` 条件子句 | 同一 mutex 范围内 147 行已排除旧 slot 的 final_claimed；新 slot 默认 false | 只简化该条件，不删字段或其他阶段的检查；不计为整行删除 |
| [preserve_trx_temp_transfer.cc](../../../sql/preserve_trx_temp_transfer.cc)，378、446 行 | 两次重复的 `file == record.sealed_files.end()` 条件子句 | 369–373、440–444 行已分别在同条件成立时立即返回，中间没有修改该 map | 保留前面的日志及返回、sealed 标记、非空指针、大小和摘要认证；不计为整行删除 |
| [preserve_trx_temp_table.cc](../../../sql/preserve_trx_temp_table.cc)，4239–4245 行 | 7 行 `preserve_trx_temp_table_resume_open_path()` 包装 | 丢弃 dir/token，仅调用已有的 `preserve_temp_dict_open_path(entry)`；仓库检索只有定义和同文件 4531 行调用，无头文件声明 | 先把唯一调用改为已有函数，再删包装；不删除 local materialize 或 staged-open 功能 |
| [preserve_trx_result_cursor.cc](../../../sql/preserve_trx_result_cursor.cc)，63、75 行 | `return_candidate` guard 及其 commit，共 2 行 | 回移目标是按值传入的 decoder，函数退出仍会销毁，无法返还调用方。失败时真正的 arena restore 先执行；cursor 的正常析构本就先销毁 decoder，再销毁不借用它的 sender | 保留 67、73–74 行的 THD arena 恢复。验证 sender factory OOM、初始化失败、worker 重用和额度释放，不能把所有 scope guard 都当冗余 |
| [preserve_trx_temp_table-t.cc](../../../unittest/gunit/preserve_trx_temp_table-t.cc)，96 行 | `is_preserved_space_id_reserved()` 的重复声明，1 行 | 与 89 行完全一致，同属 `namespace ibt`，中间没有条件编译 | 只删第二次声明，不删测试、函数或实现；不新增 GUnit |

内核整行合计为 `3+1+5+6+7+2=24`，另计 3 处条件子句和 1 行测试声明。以上是当前原文中的物理行数，不包含后续合并、调用换行或 include 调整，也不表示可以删除整个模块。死赋值和不可达条件主要减少维护噪声，不能宣称有可测时延收益。

## 可以合并的重复逻辑

### receiver 加载 bundle 时重复校验结果清单

位置：[preserve_trx_transfer.cc](../../../sql/preserve_trx_transfer.cc) 16608–16618 行，以及 [preserve_trx_result_transfer.cc](../../../sql/preserve_trx_result_transfer.cc) 100–162 行。

基线在线路径先调用 `preserve_trx_result_transfer_validate()`，随后调用 `preserve_trx_result_transfer_validate_files()`；后者内部又执行前一项校验。对同一组结果对象，清单解析、临时指针数组分配、排序、重复对象检查和最终选择匹配因此执行两次。下图展示基线问题：

```mermaid
flowchart LR
    A[解码 bundle] --> B[清单校验]
    B --> C[核对 receiver record 与 manifest]
    C --> D[文件校验入口]
    D --> E[再次清单校验]
    E --> F[检查封存文件]
```

本轮保留首次清单校验及其位置，将文件检查入口收敛为只检查文件，得到 `清单校验 → record 与 manifest 核对 → 文件校验`。`receiver_record_matches_manifest()` 在基线 7623 行比较 epoch、token、协议、freeze/commit 边界及完整对象描述符；这些核对未删除。无 registry 的路径仍保留自己的清单校验及结果数量约束。

源码可确认的收益是少一次扫描、排序和临时额度申请，未改变传输协议，也没有新增缓存。**合并范围限定在同一次调用及已核实一致的输入内**，没有因早期候选校验通过而取消 final 或换代后的认证。

实施时已核对错误顺序：没有前移 record 检查，且 `load()` 保留原 record.token 计费键。被删除的第二次额度申请不再产生它自身的资源不足错误，不能据此声称所有分配失败事件等价。

### 结果加载时重复解析清单和读取文件头尾

位置：[preserve_trx_result_transfer.cc](../../../sql/preserve_trx_result_transfer.cc) 142–162、187–222 行；实际读操作见 [preserve_trx_cursor_file.cc](../../../sql/preserve_trx_cursor_file.cc) 174–197 行。

`preserve_trx_result_transfer_load()` 先调用 `validate_files()`，它对所有 cursor 工件调用 `describe()`。随后 `load()` 再解析一次 manifest，并对最终选中的同一文件再次调用 `describe()`，填充 Snapshot。

设 record 中共有 N 个 cursor 工件，最终选择 S 个结果。当前单次成功加载调用 `describe()` 共 N＋S 次；每次包含一次头部和一次尾部 `read_at()`。在全部工件校验、预算和错误顺序约束均得到满足的实现中，目标为 N 次，即省去 S 次 `describe()` 和 2S 次 `read_at()` 调用。**这是候选方案的调用次数目标，不等于减少同样次数的物理磁盘 IO，也不是已测得的时延收益。**

可考虑在同次加载中复用解析后的 manifest view 和已检查的 descriptor，但**当前仅确认重复，不把“循环里直接填 Snapshot”视为已证明等价的实现方案**。现有 `validate()` 的 `N*sizeof(pointer)` 临时额度在返回时释放，全部 N 件文件校验结束后，`load()` 才申请 Snapshot 额度。提前申请 Snapshot 可能改变文件损坏与资源不足的错误优先级；若还保留排序 scratch，会增加两份额度同时占用的峰值。

实施前须写清排序 scratch、descriptor 和 Snapshot 各自何时申请、释放，比较峰值占用及拒绝行为。只使用本次调用的局部状态，不引入跨轮缓存、注册表或新生命周期；若无法用简明方案保持约束，先保留这 S 次读取，不能为消除小量重复增加预算或隐性状态。

必须保留以下语义：

- 所有声明的 cursor 工件都检查封存状态、文件存在性、大小、摘要以及对象名与文件身份的一致性；不能只验证最终选中的结果。
- Snapshot 仍按最终清单的 statement ID 递增顺序生成，检查 generation、大小、摘要、FETCH 位置和行数界限。`attach_cursor()` 在 [preserve_trx_result_restore.cc](../../../sql/preserve_trx_result_restore.cc) 201 行使用 `lower_bound`；record 工件顺序或对象名字典序不能替代此顺序。
- 文件继续由同一份已验证的 `shared_ptr<const Preserve_trx_sealed_file>` 保活，不重新按路径打开，也不推进 FETCH。
- 失败时不发布部分 Snapshot，临时内存仍计入预算并正确释放。
- 同次调用内复用校验结果，不跨阶段省略针对新清单、新 owner 或新代际的检查。

历史工件保留和选择早代均是现有行为：transfer 8759 行保留旧 CURSOR_RESULT，receiver candidates 215–216 行允许 final 选择更早的确切代次。因此“只检查最终 S 件”不能作为简化方案。

### 游标解码器不必保留未使用的名称和完整 Send_field

位置：[preserve_trx_cursor_decode.cc](../../../sql/preserve_trx_cursor_decode.cc) 26–33、80–102、289–301、330–342 行。

基线第二次解析为每列复制 db_name、table_name、org_table_name、col_name、org_col_name 五种名称，后续只有 col_name 传给 `make_field()`。sender 在 [sql_prepare.cc](../../../sql/sql_prepare.cc) 220–222 行只消费列数、类型和 result charset，恢复 FETCH 不重发原始列元数据。

本轮继续读取四个不用名称的长度并检查越界，但跳过 MEM_ROOT 复制；保留 col_name。私有 `Column::sent` 已缩为实际需要的 `col_name` 和 `sent_type`，不改公共 Send_field、持久文件格式或协议。所有 wire 字段仍按现有格式消费并保留拒绝检查；第一次无分配预解析承担形状校验和预算计算，仍予保留。

源码能证明的成功路径收益是每列少 4 次 MEM_ROOT 申请、4 次 memcpy 及四个名称 `Σ(length+1)` 的请求字节。MEM_ROOT 块数和 RSS 降幅需要测量。`rows_offset*4` 的保守计费不会因少复制而自动下降；Column 瘦身只使原有 `sizeof(Column)` 项减少，额度变化为 `列数×(sizeof(旧Column)−sizeof(新Column))`，具体 ABI 字节数待构建确认。不顺带降低其他预算项。

### 临时表描述符的基础构造重复

位置：[preserve_trx_temp_table.cc](../../../sql/preserve_trx_temp_table.cc) 363–395 行的 `image_descriptor_from_exported_metadata()` 与 1005–1025 行的 `binding_preflight_image_descriptor()`。

两者重复复制源空间、表身份、根页、页大小、表和空间 flags，以及全部索引的编号、根页、flags 和名称。

建议复用已有基础构造函数，正式导出再补 table ordinal、token 文件名、大小、摘要、操作序号及 format version。可移动已有函数定义到调用前，不需要新建文件或通用转换框架。

这项主要减少维护重复，不应宣称已经减少两次业务扫描。preflight 在 1100 行用于 binding 比对；正式构造在 2888 行还用于普通候选，此时 token 暂为空，后续 prebuild 补逻辑文件信息；3709 行用于 final。实施时要逐字段比较构造结果，特别保持 preflight 的默认值与正式导出的 `image_format_version=1`；不得删除阶段调用或提前填充尚未确定的封存信息。

### 两种临时文件封存流程的公共步骤重复

位置：[preserve_trx_temp_table_carrier.cc](../../../sql/preserve_trx_temp_table_carrier.cc) 2104–2142 行的 `seal_warm_image()` 与 2145–2181 行的 `seal_prevalidated_warm_image()`。

两者的 token/描述符检查、路径构造、目标存在检查、源文件大小检查、安装及目录 fsync 基本一致。核心差别是普通入口会重新校验整个文件摘要，预校验入口依赖调用方已完成摘要认证并关闭 writer。

快速入口有两类生产调用，均须保留：源端 final 在 temp_table 3724–3729 行按是否复用 Phase1 sidecar 选择；receiver 在 [preserve_trx_temp_receiver.cc](../../../sql/preserve_trx_temp_receiver.cc) 177–187 行完成转换后增量摘要和 checkpoint，204–206 行关闭两份 writer，再在 215 行分别封存 ORIGINAL 和 INSTALLATION。不能因合并让 receiver 重新整文件 hash，也不能改变 213–218 行 owned 位对部分安装失败的清理责任。

建议保留两个含义明确的现有入口，在现有 carrier 内共用封存主体；摘要是否需要重读由这两个入口明确选择，不向外增加可随意跳过验证的通用接口。

必须保持 `CORRUPT`、`ALREADY_EXISTS`、`NOT_FOUND`、摘要错误、安装错误及 fsync 错误的处理顺序和文件保留规则。普通路径不能漏掉摘要校验；预校验路径不能增加整文件重读，也不能先调用普通封存再补救。

尤其保留 `IO_ERROR_DURABLE_SNAPSHOT_MAY_EXIST`：目录 fsync 失败后的安装文件处理不同于普通 IO_ERROR，不能合并成同一状态。第二份副本失败时，第一份及可能已安装的第二份仍由原有 owner 清理。

这项主要减少重复代码。两个入口通常是择一执行，不能把代码合并本身解释为运行次数减半。

### 字典模板释放和按名称查找可复用已有函数

位置：[trx0temp_preserve_dict.cc](../../../storage/innobase/trx/trx0temp_preserve_dict.cc) 68–72、82–87、348–355 行。

私有字典 deleter 的五行虚拟列模板释放块，与同文件 `free_virtual_template()` 完全同义。两条销毁路径均先移除 indexes，再释放模板，最后释放 table。调整定义顺序后直接调用已有函数即可，五行换一行，无需新辅助层。

`cached_by_name()` 与 [dict0priv.ic](../../../storage/innobase/include/dict0priv.ic) 72–92 行的 `dict_table_check_if_in_cache_low()` 使用同一 hash、fold 和 strcmp；调用方均持 dict_sys mutex，原生 helper 不增加引用或移动 LRU。可以删除该八行副本，增加所需 include 并修改五处调用。净减量受 include 和换行影响；Debug 会增加原生 helper 的跟踪输出。不要把相邻 `cached_by_id()` 一起替换，原生按 ID open 接口有不同副作用。

### 退役和存活 undo record 共用初始化与发布尾部

位置：[trx0temp_preserve_graph.cc](../../../storage/innobase/trx/trx0temp_preserve_graph.cc) 157–234 行。

retired 与 live 分支重复 image/header/insert 初始化、address 登记、record push、header_next 和末尾位置推进。可以在本函数内共用这些步骤，只让 live 分支解析字段、LOB、隐藏 row ID；无需新 helper 或通用状态机。

退役表必须保持原生 rollback 的“先跳过已删除表，不再解析其行字段”行为，不能错误关联到同名重建表，也不申请 live 外部引用的增量额度。live 的 `graph_memory` 仍须在外部引用容器分配前增长（194–202 行）；仅 `surviving_insert/update` 与 `reference_bytes` 累计值在 `records.push_back()` 成功后更新（227–229 行）。公共尾部不得改变额度预留、地址登记、记录插入及计数提交的顺序。地址冲突、分配失败和取消路径均须验证。重复原文约 12 行不等于补丁净减量，此项排在直接复用之后。

### 低优先级的局部公共动作

| 候选 | 位置与收敛边界 |
| --- | --- |
| 完整 cursor descriptor 比较 | [preserve_trx_receiver_candidates.cc](../../../sql/preserve_trx_receiver_candidates.cc) 36–41 行与 [preserve_trx_result_cursor.cc](../../../sql/preserve_trx_result_cursor.cc) 47–50 行比较同一组字段，可在既有 descriptor 附近归并纯比较函数，但两处认证调用都保留，不减少阶段校验 |
| 单文件删除及目录 fsync | [preserve_trx_temp_table_carrier.cc](../../../sql/preserve_trx_temp_table_carrier.cc) 2243–2254、2398–2422 行的三个入口都删除文件及 .tmp，发生删除才 fsync。可在本文件共用相同动作；不得混入只删 warm 正式文件的 remove_warm_image，也不能把批量删除后一次 fsync 改为多次 |

这两项主要减少维护重复。若为少量行数引入更多参数、开关或跨文件依赖，优先保留现状。未使用 include 的候选尚未经过编译验证，不计入确定删除数量；[trx0temp_preserve_import.h](../../../storage/innobase/include/trx0temp_preserve_import.h) 497–506 行的单 anchor 注释实际描述 513 行的 collect_undo_pages，可仅调整注释位置，不能据此合并接口。

## 相似但不宜直接合并的内容

| 对象 | 必须保留的区别 |
| --- | --- |
| `reserve_preserved_space_id()` 与 `reserve_or_keep_preserved_space_id()` | 前者对重复 ID 返回 false；后者返回 true 且 `created=false`。生产 fil adopt 根据 `created` 决定失败时是否释放 reservation，不能误释放调用方原有预留。两者现有 OOM 处理也不同，直接替换并不等价 |
| 源字典与目标字典的构建、撤销 | 目标字典涉及发布、撤销、绑定表和原生所有权移交；源字典用于解析和映射。不能把两个状态机强行并成带大量开关的通用状态机 |
| 分配器防冲突、OPEN/ACK 契约、发布前重号检查 | 分别保证本地分配唯一、跨端策略一致、发布时对象仍正确，属于不同边界的校验；不能互相替代 |
| namespace 的独立启动 gate 与 Preserve 开关 | 本地临时 ID 策略在关闭 Preserve 或临时捕获后仍需保持，不能随业务开关退出，否则未来分配可能破坏隔离 |
| 按名称与按 ID 的原生字典查找 | `dict_table_check_if_in_cache_low()` 只查找；[dict0dd.ic](../../../storage/innobase/include/dict0dd.ic) 155–160 行的 `dd_table_open_on_id_in_mem()` 可能移动 LRU 并 acquire 引用，不能代替本地只观察的 cached_by_id |
| TEMP candidate、final 授权、后续 matches | candidate 允许 record 有其他代对象；final 加入精确对象集合、封存数量、resource-only snapshot 与 flags；后续匹配认证当前 record。不能用早期成功跳过 final 或后续检查 |
| cursor describe、open、行预检和 FETCH | describe 只提取基本布局，open 还验证列数/schema digest；预检与 FETCH 读取有各自作用。不能因头尾读取重复而删掉完整验证 |
| DATA 与 undo 的 delta 完成段 | DATA 需要 writer checkpoint；undo 在会分配的动作完成后才关闭完整文件替代 FD、移动 lease，保护 OOM 和 fallback。不能机械共用带开关的安装流程 |
| image 快速 seal 与 undo seal | undo 可通过 closed_writer 对确切 path/size/digest 认证，否则重读摘要，其认证依据和错误顺序不能随 image seal 一起折叠 |
| receiver checkpoint、final seal、publish | 普通轮保留可增量更新的私有 writer；final 认证后才封存、发布和移交原生所有权，不是三次可以互删的准备 |
| 只赋值但通过析构释放的 owner | 如 m_retired_table_memory 是 RAII 额度持有者，析构释放就是用途；不能按“显式读引用少”判为死字段 |
| Debug、MTR、既有 GUnit 和旧 local 路径使用的入口 | 线上主链不调用不等于零调用。本特性不扩展 local startup，不构成删除既有 local 恢复功能的授权；固定物理升主与外部 cursor attach 接口也不能按本地调用数删除 |

预留计数释放处的 CAS 重试还有一个低收益简化候选：[srv0tmp.cc](../../../storage/innobase/srv/srv0tmp.cc) 169–182 行已持 reservation mutex，当前所有计数写入也持有该锁，可考虑保留原子读写和零保护，省去 CAS 重试。这属于同步实现调整，非死代码；收益很小，不列入第一批必要改动。

## 实施规则与验证要求

1. **先做明确的小改动，再做有条件的合并。** 直接清理、无用名称复制和已有函数复用先行；随后处理清单重复校验。Snapshot 读取合并须先明确预算、排序和异常顺序，undo/封存合并单独验证。按调用关系确定补丁边界，不以凑删减行数为目标。
2. **保持现有流水线。** 复用已有 worker、预算及 owner；不增加线程池、全局缓存、额外传输阶段或新的外部接口。改动优先落在现有专用文件中，共享文件只保留必要编排。
3. **保持身份和所有权。** receiver 的原有只读临时表、未来分配、目标 space/table ID、undo、文件 pin 及跨代复用均不改变。READY 前完成数据量级准备，不把工作移到在线升主或 SQL RESUME。
4. **保留既定集成范围。** 物理升主只用三个已有接点：`preserved_trx_prepare_before_trx_sys_init_for_physical_promotion()`、`trx_lists_init_at_db_start()` 内 Preserve hook、`preserved_trx_adopt_ready_epoch_for_physical_promotion()`。SQL RESUME 入口仍为 `Sql_cmd_resume_preserved_transaction::execute()`。不新增 RESET DRAIN 行为，也不恢复 PS 本体 transfer。
5. **以 MTR 和 Python E2E 验证行为。** 不新增 UT/GUnit，不使用 DEBUG_SYNC；已有源码位置断言若受影响，只随真实源码组织调整，不能删掉业务或失败断言。

| 改动 | 必须验证的内容 |
| --- | --- |
| 小删除与条件简化 | canonical 名称和换代、working/final_claimed 边界；TEMP 缺失文件和 final 认证；包装替换后的 staged-open 失败清理；sender factory OOM/初始化失败、arena 恢复及后续 worker 重用 |
| 名称复制及 Column 瘦身 | 长名称、既有支持的全部列类型、wire 越界拒绝、继续 FETCH、重复 attach；计费变化与实际分配分开测量，保守预算项不顺带削减 |
| 结果清单与文件校验合并 | 正常传输和继续 FETCH；多代工件与最终子集、final 选择早代；缺失、重复、错误 kind/name、大小/摘要/代际不符、未封存文件；FETCH 越界；未选中历史工件损坏与预算不足组合；工件乱序而结果仍按 ID 排序；无 registry 分支；失败不发布部分状态 |
| 描述符构造复用 | 构造结果逐字段相同；多索引、隐藏聚簇键、生成列及现有支持的 TEMP 类型；OFF 路径不变 |
| 字典模板及原生名称查找复用 | 虚拟列、准备失败/取消、发布/撤销、重名重号、busy 及 native handoff；模板恰好释放一次、额度与 owner 正确 |
| undo record 公共尾部 | DROP 后同名重建、混合 retired/live、外部引用/隐藏 row ID、地址冲突、预算拒绝与取消；退役表不解析字段，计数只计存活记录 |
| 封存公共主体 | 普通摘要拒绝、源端和 receiver 预校验路径无整文件重读、目标已存在、源缺失、大小不符、安装和 fsync 失败；双副本第二份失败/OOM、重试与清理所有权不变 |
| 预留分支删除或计数简化 | 重复预留、保留已有预留、OOM 回退、取消释放及未来空间分配；已有 receiver 原表内容和 ROLLBACK 不受影响 |

可优先复用现有 `cursor_result_*`、`cursor_pretransfer_*`、`cursor_replay_*`、`temp_receiver_strict_ready_cursor`、`temp_table_id_namespace*`、`temp_receiver_fil_attach_failure`、`temp_image_writer_*` 等用例，再按实际缺口补行为覆盖；测试名存在不代表上述所有边界已经覆盖。

实施后运行相关构建、定向 MTR/E2E，并完成 Preserve Trx 全量 no-bin/log-bin MTR，包括 big-test。receiver 共存保护用 M12 检查原临时表内容、ROLLBACK 和后续创建；结果准备效率用含 cursor 的模型对比。明确区分清单校验次数、文件读调用次数、临时内存及 READY/Phase2 时间，不用普通无 cursor 的 PS 压测代替结果链路验收，不改变预算或性能门槛。

## 当前状态与相关文档

| 项目 | 状态 |
| --- | --- |
| 24 行内核原文、3 处条件及 1 行测试声明 | 已清理；不与合并候选的行数重复计数 |
| Column 瘦身和四个名称复制 | 已实现；仍校验五个名称的 wire 长度，保留 col_name 和 sent_type，不改格式和保守额度项 |
| descriptor 基础构造、虚拟列模板释放、字典名称查找 | 已复用已有函数；preflight 默认值、正式字段和按 ID 查找保留 |
| image seal、undo record 初始化与发布尾部 | 已合并；快速 seal 不新增整文件 hash，retired undo 不解码字段 |
| descriptor 比较和三个单文件清理尾部 | 已合并；认证调用位置、单文件和批量 fsync 次数不变 |
| 清单重复校验 | 已移除在线路径第二次清单扫描/排序/临时额度申请；错误顺序及调用前提见下文 |
| N＋S 次 describe | 保留；不为省 S 次头尾读取提前占用 Snapshot 额度或新增 descriptor 缓存 |
| 预留计数 CAS | 保留；属于同步实现调整，收益不足以扩大本轮修改范围 |
| 未使用 include 与注释 | source.cc 的四个未使用 include 已删除；undo claim/collect 接口注释归位 |
| 构建与运行验证 | Debug、Release 构建、定向用例、M9/M12 和全量 no-bin/log-bin（含 big-test）均已通过 |
| 提交 | 未提交、未 push |

### 实施细节

`preserve_trx_result_transfer_validate_files(record)` 现在仅验证文件。它的两个调用点均显式保证先校验相同的清单：在线 bundle 仍为 `validate → receiver record/manifest 核对 → validate_files`；`load()` 则显式执行 `validate(record.token) → validate_files → Snapshot 申请`。没有提前 record 核对，也没有提前 Snapshot 额度申请。所有 N 件工件仍检查；最终 S 件的再次 describe 暂保留。去掉第二次清单校验也去掉了该次申请可能产生的 RESOURCE_EXHAUSTED，这是明确减少重复资源申请，不能描述为每个分配失败事件完全等价。

普通与预校验 image seal 保留两个公共入口，通过 carrier 内私有重载共用主体。普通入口要求校验摘要，预校验入口跳过已完成的整文件 hash。两个入口的选择及公共主体安装/fsync 责任由原有源码约束用例继续检查。

undo live 分支仍先申请外部引用额度，再构建引用。`lob_diffs` 移交、external reference 按值复制及 hidden row ID 读取后，字段解析 scratch 即可释放；record 持有的页图像不依赖该 scratch。公共尾部保持地址登记、记录插入、存活计数提交的顺序。

现有 Python 行为用例补充两项：64 字符 db/table/column 名及 255 字符结果别名的普通结果与恢复 FETCH 比对；sender factory OOM 后额度归还、同一 THD 执行 SQL、重新导出/导入和继续 FETCH。后者通过已有内部桥及故障点验证 API 所有权，不能代替真实物理复制工程的回放联验。

### 本轮验证记录

证据目录：`build-debug/temp-resource-consolidation-20261005/`。内核补丁为 16 个文件、`+173/-263`、净减少 90 行；未增加内核文件、外部接口或线程池。另修改两个现有 Python 行为脚本、一个现有源码约束 MTR，并删除一行重复 GUnit 声明；没有新增或执行 GUnit。

- `baseline-build.log`、`baseline-mtr.log`：修改前 Debug 构建及 6 个定向行为用例通过，shutdown_report 单列通过。
- `after-build.log`、`release-build.log`：修改后 Debug 与 Release 的 mysqld 构建均通过。`hashes.txt`、`release-hash.txt` 和 `kernel.patch` 固定二进制及内核补丁证据。
- `focused-mtr.log`、`long-name-repro.log`、`focused-cursor-final.log`：初跑 8 项中有 2 项被新增测试的错误元数据假设阻断。串行复现确认原生物化将结果别名用作 org_col_name，失败发生在解码/FETCH 前。仅该新增场景改为比较跨批次二进制行，保留其他元数据断言；最终 cursor decode/restore/attach 三项通过，加上初跑已通过的五项，共 8 个不同定向用例通过（6 个行为、2 个 lint）。
- `full-no-bin.log`、`no-bin-summary.json`：完整 preserve_trx，`--big-test --parallel=4 --force --retry=0 --nocheck-testcases --mysqld=--skip-log-bin`，无用例筛选、自动失败重试或 golden 更新。598 通过（562 行为、36 lint）、285 因需要 binlog 跳过、0 失败，shutdown_report 单列通过，墙钟 1265 秒。883 项各出现一次，无遗漏/重复，17 个 big-test 实际通过。`--nocheck-testcases` 跳过 MTR 框架的通用 testcase 前后检查，用例内的业务、残留和故障断言仍执行。
- `full-log-bin.log`、`log-bin-summary.json`：上一轮退出后顺序运行，使用独立 vardir、相同选项并改为 `--mysqld=--log-bin=mysql-bin`。596 通过（560 行为、36 lint）、287 因要求关闭 binlog 跳过、0 失败，shutdown_report 单列通过，墙钟 1343 秒。883 项各出现一次，2 个 big-test 实际通过。
- `verification-summary.json`：两轮通过并集为全部 883 项，18 个不同 big-test 全部实际通过；没有因未开启 big-test 而跳过的项。`multi_session_100_resume` 自带 skip-log-bin，在两轮均通过，不能按轮次名推断该个案开启了 binlog。两个二进制及内核补丁指纹与构建后的记录一致。
- `m12-coexist-release/report.json`：4 个源 session、每表 4096 行、receiver 1 个并存后台；4 个 token READY，512 行 cursor 预检，NOT_READY 和捕获失败均为 0。原有临时表内容、ROLLBACK、READY 后及后台未来分配均通过；执行器校验和退出状态符合预期，已删除本次独占 datadir。
- `m9-cursor-release/report.json`：1 个源 session、4096 基础行、8 倍结果，receiver 预检 32768 行并 READY；捕获失败和 NOT_READY 均为 0。未测真实物理升主、外部 PS 回放或线上 SQL RESUME。

两项 E2E 使用本轮 Release 二进制，运行时另有全量 MTR，故只用于功能验收，不用于时延对比。M9 的 DRAIN/观测 READY 尾部为 455.082/92.064 ms；M12 为 545.515/235.721 ms。这不是 strict Phase2 或升主耗时，也不是优化前后收益。初次 M12 Debug 尝试被原脚本的 `Release required` 检查拒绝，未削弱检查，改用 Release 后通过。

全量结束后确认没有存活的 mysqld/mysqltest/MTR 进程，清理本轮生成的 18 个 MTR datadir；用例日志、初始失败日志及报告保留。清理清单见 `cleanup.json`。没有提交或 push。

新增 OOM 用例使用的故障点位于 sender factory 调用之前，证明该早退的 arena 恢复、decoder/额度释放及同连接重试；不据此宣称覆盖了 factory 内部真实初始化失败。长名称用例覆盖成功解析和跨批次 FETCH，没有新增逐名称截断负例；长度越界检查的保留另由源码核对确认。

审计版经两个全新、互不共享会话历史的 sub agent 分别全文阅读并对照源码审核，主会话再核对意见。实施补丁又分别进行结果链路、TEMP/undo/字典两路只读审核，未发现可行动的正确性问题；主会话负责修改和运行验证。静态审查不代替构建、MTR/E2E 或性能验收。

当前功能分工见[源码布局](source-layout.md)，功能约束见[详细设计](detailed-design.md)，外部 PS 回放与 cursor 关联见[接口文档](ps-transfer-removal-and-cursor-attach.md)。[任务跟踪](task-tracker.md)中的既有测试结果属于原实现，本次补丁使用下述独立验证记录；真实物理 replay 和在线升主的隔离联验仍按 V04 管理。
