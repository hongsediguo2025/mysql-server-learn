# 需求背景与当前支持范围

2026-10-04，依据当前未提交代码同步。现行流程见[详细设计](detailed-design.md)，旧方案只作为[历史设计](detailed-design-before-ps-removal.md)保留。

## 1. 什么结果需要保存

| MySQL 形态 | 是否由本特性迁移 | 理由 |
| --- | --- | --- |
| Classic `COM_STMT_EXECUTE(CURSOR_TYPE_READ_ONLY)` 后的服务端 cursor | 是 | 后续 `COM_STMT_FETCH` 是新命令，结果仍留在服务端 |
| 该 cursor 使用的内部结果临时表，内存/磁盘或 spill | 以独立有序结果工件保存 | 不搬运各结果引擎的扫描指针，不重新执行 SELECT |
| 用户创建临时表保存结果，再按应用条件分页 SELECT | 按用户临时表保存 | 页码/条件来自应用，每次 SELECT 是新查询，不额外构造 cursor |
| 普通 SELECT 的 `mysql_fetch_row()` / 客户端缓存取行 | 不另造迁移对象 | 读取当前响应或本地缓存，不代表服务端独立 FETCH |
| CALL、多语句包、存储程序内部 cursor | 不迁移执行栈 | 等完整顶层命令按既有规则结束或超时 |
| X Protocol cursor | 维持现有限制 | 不扩展插件协议及其 ID/会话恢复 |
| 仍打开的 HANDLER | 维持现有限制 | 保存的是扫描句柄而非已物化完整结果，不顺带纳入 |

源查询可以来自永久表、用户临时表或两者；相同 SQL 的多个 PS 和同一 PS 的不同结果代次不得混淆。相关源码为 [sql_prepare](../../../sql/sql_prepare.cc)、[sql_cursor](../../../sql/sql_cursor.cc)、[result_transfer](../../../sql/preserve_trx_result_transfer.cc)。

## 2. 产品和集成前提

- 客户端到 proxy 的连接保持，客户端不改。后端重新连接后先 SQL RESUME，再外部回放 PS，再显式关联 cursor，处理 CLOSE 后放行业务。
- 物理复制工程已有 session 上下文和 PS 回放能力。本工程不另造通用 THD/字符集/PS 参数迁移；外部源码暂不可访问限制验证，不表示能力缺失。
- 用户表仅 InnoDB。内部结果物化/spill 仍按实际实现验证，不能把用户表引擎约束误用于结果存储。
- 源端只在完整命令边界 Preserve；尚未执行完的命令继续等待，除非按已有期限失败。
- 服务端已完成 FETCH 的位置与客户端应用消费位置分开。已经交付的前缀由前端链路保留，不增加逐行确认协议。
- 前端断开结束该逻辑会话；重连后重新 execute 的结果从新执行开始，不承诺旧结果续取。

## 3. 当前责任拆分

| 内容 | 当前范围 |
| --- | --- |
| 用户临时表、DATA/undo、DDL/保存点及原生身份 | 继续保留，跨主后能继续 DML/COMMIT/ROLLBACK |
| cursor 结果、列信息、顺序、代次、FETCH/EOF | 继续保留，READY 前准备文件/decoder/sender/位置 |
| PS SQL、参数、类型运行态、历史解析/依赖、factory/rebuild | 本工程删除；外部回放承接，不再作为 W07/W08 待开发 |
| 源 PS 对应关系 | 源结果记录 ID/代次；外部同一回放记录提供目标 PS，接口核查 THD/map/ID 和保留结果 |
| LONG_DATA 专属迁移和 pending 状态拒绝 | 已随 PS 本体迁移删除，不另列本特性参数限制；原生无响应保护保留，外部行为需集成验证 |
| CLOSE | proxy 转发前留存；RESUME→相关 PS 回放/关联→CLOSE 补发→新业务，记录按会话和 PS 生命周期消费 |

`NO_CURSOR` 表示成功恢复 owner 的最终集合无该 ID；owner 缺失、绑定失败或错对象应 ERROR，不能把错误当成无结果。generation 是结果代次，不是 PS 生命周期证明。接口不含 SQL 文本，无法自行识别同 ID 的错误回放对象。

## 4. 事务与资源组合

| 组合 | 必须保持的语义 |
| --- | --- |
| 永久表与临时表混合事务 | 原持久恢复依据、TEMP DATA/undo 和结果共同受最终合同认证 |
| TEMP_ONLY | 保持真实无 redo 临时事务和 undo，不伪造永久表恢复依据 |
| READ_CONTEXT | 复用已有读上下文恢复合同 |
| NONE：无需引擎事务恢复，有 TEMP/结果 | SQL 事务活动状态另行保存；无活动事务只是其中一种场景，不凭空开启业务事务 |
| 多 cursor、TEMP 与结果共存 | 每个结果独立的源 ID/代次/消费位置，一份恢复会话归属 |
| 仅普通 PS，无本特性资源 | 无 PS 工件，沿既有 session-only/control 分流 |

## 5. 不能改变的恢复边界

1. 仅 standby transfer，不新增 local startup 恢复或 RESET DRAIN 行为。
2. strict physical promotion 仍要求两端 `log_bin=ON`、`gtid_mode=ON`，TEMP_ONLY/NONE 也不豁免。只从三个已集成的在线升主接点扩展；`trx_lists_init_at_db_start()` 同样用于在线升主；SQL 入口为 `Sql_cmd_resume_preserved_transaction::execute()`。
3. receiver 原有只读会话的临时表和后续分配保持隔离；临时 ID/资源在物理重放中的稳定性需外部验证。
4. 数据复制、转换和值校验在 READY 前完成，不移到升主、SQL RESUME 或第一次 FETCH。
5. 原表已删除也不能重新 SELECT 恢复旧结果。外部回放处理历史 PS 依赖，本工程不恢复一套延迟 prepare 机制。
6. SQL RESUME 失败时 proxy 关闭前后端，不重试/补发业务/归还池。关联或 CLOSE 交付不确定时不放行业务。
7. 临时资源扩展使用专用文件和共享薄接点，复用已有 worker/预算/所有权；MTR/Python E2E，不新增 GUnit/UT 或 DEBUG_SYNC。

## 6. 验收如何判定

行为验证包括结果值/顺序/列、位置/EOF、换代/关闭、源/目标身份、SAVEPOINT/ROLLBACK、接收端共存、失败及 OFF 隔离。外部 PS 回放/关联与本地 Debug helper 证据分开。

性能需要同时看业务开销、捕获/网络/receiver 吞吐、strict Phase 2、精确尾部、同钟 ACK→READY，以及真实升主/RESUME/回放/首次业务。N/A、INVALID、跳过不算通过；一次矩阵不是重复轮次统计。当前结果与缺口见[任务跟踪 E63/E64](task-tracker.md)。
