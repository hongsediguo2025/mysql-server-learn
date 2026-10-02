# 当前源码分工与共享接点

2026-10-05，`ha_preserve_trx`。本文描述两批函数迁移后的布局；验证基线为本次 amend 前的 `9a439e9ac544` 加 E65/E67 迁移。只列实际存在的模块；原拟定文件名和 PS factory/参数导入方案不再作为开发清单。

## 1. 资源主线

| 文件组 | 当前职责 |
| --- | --- |
| `sql/preserve_trx_temp_table.*`、`temp_prebuild.*` | 用户表捕获/恢复及接入已有源 worker |
| `sql/preserve_trx_temp_transfer.*`、`temp_delta.*` | TEMP 工件、BASE/DELTA 与最终选择 |
| `sql/preserve_trx_temp_receiver.*`、`temp_undo_prebuild.*` | receiver 有界准备和候选复用 |
| `sql/preserve_trx_temp_import.*`、`temp_restore.*`、`temp_id_contract.*` | SQL/引擎桥接、安装和临时身份合同 |
| `storage/innobase/trx/trx0temp_preserve_*` | 原生空间/表/undo/行/LOB/字典/ID 的捕获、转换、准备和寿命 |
| `sql/preserve_trx_cursor.*`、`cursor_file.*`、`cursor_decode.*`、`result_cursor.*` | 结果物化、封存文件、值校验及恢复后的 FETCH |
| `sql/preserve_trx_result_manifest.*` | 独立最终结果清单，只保存结果身份和位置 |
| `sql/preserve_trx_result_pretransfer.*`、`result_transfer.*` | 现有 worker 内结果预传、final 捕获/发送/校验 |
| `sql/preserve_trx_result_restore.*` | receiver Ready、RESUME journal、THD owner、显式 cursor attach |

前缀相同表示一组实际 `.cc/.h`，详细入口见 [TEMP 源](../../../sql/preserve_trx_temp_table.cc)、[InnoDB 导入](../../../storage/innobase/trx/trx0temp_preserve_import.cc)、[结果传输](../../../sql/preserve_trx_result_transfer.cc)、[结果恢复接口](../../../sql/preserve_trx_result_restore.h)。

## 2. 复用的公共编排

| 位置 | 保留职责 |
| --- | --- |
| `preserve_trx.cc`、`preserve_trx_drain.*`、`preserve_trx_standby_phase2_scheduler.*` | 命令边界、分类、事务交接与 SQL RESUME |
| `preserve_trx_resource_session.*`、`preserve_trx_recovery_contract.*` | NONE 等资源会话与引擎恢复依据，不把普通 PS 当新工件 |
| `preserve_trx_bundle.*`、`preserve_trx_transfer.*`、`preserve_trx_transfer_index.*` | 认证合同、对象生命周期、已有队列/索引和 ACK |
| `preserve_trx_receiver_prepare.*`、`preserve_trx_receiver_candidates.*` | TEMP/结果准备、跨轮候选、最终认证和 READY |
| `preserve_trx_promotion.*`、`preserve_trx_promotion_prepared.*` | 既有在线采用接点、prepared owner 与 RESUME lease/journal |
| `preserve_trx_resource.*`、`preserve_trx_temp_metrics.*` | 共同预算/摘要及分段指标；不恢复旧 PS 专属指标 |

在线升主只通过 `preserved_trx_prepare_before_trx_sys_init_for_physical_promotion()`、`trx_lists_init_at_db_start()` 内 Preserve hook、`preserved_trx_adopt_ready_epoch_for_physical_promotion()`。SQL 入口保持 `Sql_cmd_resume_preserved_transaction::execute()`。

## 3. 原生路径上的薄接点

| 位置 | 当前必要接点 |
| --- | --- |
| `sql_prepare.*` | 将源 ID 传给结果创建；保留 cursor 成员接口和 FETCH/EOF/RESET/EXECUTE/CLOSE/析构生命周期；附着成员的定义归 `result_restore.cc` |
| `sql_cursor.*` | 在物化时生成封存结果，稳定 snapshot 不推进原 cursor |
| `sql_class.*` | open/pending 计数、待关联结果 owner，以及 reset/cleanup |
| `protocol_classic.*` | 保留原生协议与成员接口；恢复结果专用 metadata/bind 的定义归 `result_cursor.cc`；不为 CLOSE 发响应包 |
| InnoDB 原路径 | 原有捕获、ID/FSP/undo/锁接点保持 gate；大量逻辑继续放专属文件 |

无 cursor 时使用会话计数避免扫描普通 PS；有 cursor 时 final/预传仍可能遍历 PS map。删除迁移成本不等于删除源端正常 PS 内存或原生解析成本。

## 4. 外部唯一新增调用接口

```cpp
Preserve_cursor_attach_status preserve_trx_attach_cursor_after_ps_replay(
    THD *, uint32_t source_statement_id, Prepared_statement *target_ps);
```

外部在成功 SQL RESUME 后、该条 PS 回放后调用。保持原 ID、同一恢复 THD 和同一源生命周期记录；接口不创建 PS、不设置 SQL/参数、不证明 SQL 等价。返回 `ATTACHED/NO_CURSOR/ALREADY_ATTACHED/ERROR`，细则见[当前接口文档](ps-transfer-removal-and-cursor-attach.md)。

## 5. 已删除与不得误删

`preserve_trx_ps_*`、`preserve_trx_sp_bindings.*`、`preserve_trx_sp_expression.*`、`preserve_trx_metadata_watch.*` 及只服务它们的 Item/SP/parser 钩子、构建项、指标、Debug helper 和测试已删除。不保留恒定返回的兼容壳，也不新增通用 PS 参数或 LONG_DATA 迁移。

`ps_result_<id>_<generation>` 是仍在使用的结果对象名；`CURSOR_RESULT=6`、PS 指向 cursor 的原生关系、CLOSE 静默及命令边界不能误删。旧 PS 描述符 kind 5 和准备进度请求已退出协议。

## 6. 测试与代码量

新行为测试位于 `mysql-test/suite/preserve_trx`，包含 `cursor_pretransfer_*`、`cursor_replay_*`、`cursor_result_only_strict_resume`、`temp_cursor_replay_*`。Python 通用协议客户端位于 [classic_client](../../../scripts/preserve_trx_classic_client.py)，回放关联测试使用 [cursor_attach_e2e](../../../scripts/preserve_trx_cursor_attach_e2e.py) 及 [cursor_replay_test](../../../scripts/preserve_trx_cursor_replay_test.py)。不新增 GUnit 或 DEBUG_SYNC。

PS 删除阶段的历史工作区统计：相对当时的 `e6715a345881`，tracked +653/-11184 行，加 8 个新增 result 文件 1094 行，净减少 9437 行。该阶段已纳入 `9a439e9ac544`，不能将这些数字当作当前未提交改动。当前状态见[任务跟踪](task-tracker.md)。

## 7. 首批函数定义归位

本轮只将完整定义移入已存在、已参与构建的专用文件。原声明、调用位置、功能 gate、锁、异常处理和对象释放顺序均保留；没有新增接口、对象层或 worker。

| 原文件 | 目的文件 | 搬迁内容 | 主体物理行 |
| --- | --- | --- | ---: |
| `sql/preserve_trx_transfer.cc` | `sql/preserve_trx_receiver_retired.cc` | reservation/debt 账本、staging 清理状态、终态 owner 退休及退出前释放 | 272 |
| `sql/preserve_trx_temp_table.cc` | `sql/preserve_trx_temp_history.cc` | undo history 比较、native DROP 确认、retired history 导出与估算 | 40 |
| 同上 | `sql/preserve_trx_temp_prebuild.cc` | prebuilt sidecar 预留与取走所有权 | 18 |
| `storage/innobase/trx/trx0temp_preserve.cc` | `trx0temp_preserve_source.cc`（同目录） | 源 TABLE/字典元数据导出、virtual column 核对 | 186 |
| 同上 | `trx0temp_preserve_dict.cc`（同目录） | session handler 注册/核对/注销、已绑定字典摘要 | 160 |
| 同上 | `trx0temp_preserve_capture.cc`（同目录） | 同步、有界的最终 dirty tail 驱动 | 36 |
| `sql/protocol_classic.cc` | `sql/preserve_trx_result_cursor.cc` | `Protocol_binary` 的 preserved metadata 初始化和 packet 绑定 | 25 |
| `sql/sql_prepare.cc` | `sql/preserve_trx_result_restore.cc` | `Prepared_statement::attach_preserved_cursor()` 成员定义 | 9 |

合计 746 行是搬迁的完整主体，包含沿用的旧逻辑，不是净删减代码，也不代表性能收益。逐块保持原文，仅补目的文件直接使用的 include；既有源码位置断言随之更新，行为验收仍使用 MTR。

receiver 的最后引用继续在 registry 解锁后释放；TEMP handler 注册/注销仍验证当前 THD 和对象身份；metadata 导出保留原 autoinc 锁范围；cursor packet 先分配后切换 owner。未调整 SQL RESUME、三个在线升主接点及 native 生命周期。

Phase1 capture/job 构造、undo watch 页写热路径、依赖私有 helper 的 Debug 探针仍留原处。它们需要额外的依赖或性能论证，不包含在本次机械迁移中。

## 8. 第二批函数定义归位

2026-10-05，沿用现有声明和构建文件，将下面 12 个函数、185 行完整主体归入现有模块。前 62 行补齐 sidecar/undo 与 Debug cursor 分工，后 123 行归并既有恢复和导入代码；不按新增功能代码量计算。

| 原文件 | 目的文件 | 搬迁内容 | 主体物理行 |
| --- | --- | --- | ---: |
| `sql/preserve_trx_temp_table.cc` | `sql/preserve_trx_temp_undo_prebuild.cc` | undo ownership claims 的私有转换实现与既有入口 | 24 |
| 同上 | `sql/preserve_trx_temp_prebuild.cc` | sidecar 保存与两个查找重载，与 reserve/take 同处 | 30 |
| `sql/sql_prepare.cc` | `sql/preserve_trx_debug.cc` | Debug 专用 `Prepared_statement::install_preserved_cursor()` | 8 |
| `sql/preserve_trx_temp_table.cc` | `sql/preserve_trx_temp_restore.cc` | TABLE open、staged open、link 与 close | 94 |
| 同上 | `sql/preserve_trx_temp_import.cc` | manifest undo 身份应用及其唯一私有判定 helper | 29 |

全部函数主体逐字保持，两个私有 helper 仍在匿名 namespace；cursor 验证成员仍受相同 `NDEBUG` 条件保护。undo claim 的预算 lease 活到插入完成后，TABLE 的 DD 所有权交接、反向 link/cleanup 和部分链接失败回退不变。只同步三处既有 GUnit 源码定位读取，不新增 UT 或修改行为断言。

两批累计迁移 49 个函数、931 行完整主体，没有新增内核文件、接口、构建项或 worker；这不是净删减量或性能收益。Phase1、transfer 调度/终态认证、undo watch，以及需新增内部入口的 InnoDB 字典绑定组均未纳入本批。
