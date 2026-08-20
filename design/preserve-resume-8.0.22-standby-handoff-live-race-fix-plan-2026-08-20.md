# Preserve/Resume Standby Handoff 竞态修复计划

状态：IMPLEMENTED_AND_VERIFIED

基线：`ha_preserve_trx` / `34b6a4fe54f35a025fe0d63ee5bcad7f18c45244`

## 1. 范围

- 仅处理 source 与 receiver 同一进程代均存活时的 COMMIT、RESET、RESUME 竞态与重试。
- 任一端崩溃或长期失联后由 HA 退役该轮，不实现跨重启续作。
- HA 控制账户不得承载业务事务，由管控与 Proxy 保证；内核不新增账户级检查。
- 不改 SQL grammar、普通 DML 热路径、THD 通用字段、InnoDB、handler、binlog 或 wire status。
- 不新增 lease、watchdog、后台线程、SHOW、sysvar 或测试专用 Release ABI。
- 所有 MTR steering 均在 `#ifndef DBUG_OFF` 内，Release 零符号、零字符串、零分支。

## 2. 必修项

| ID | 问题 | 修复 |
|---|---|---|
| H1 | COMMIT 发包前失败与 RESET 可同时恢复 source | ownership CAS：`RUNNING/FINAL_METADATA_ACCEPTED_LOCAL -> SOURCE_RESTORE_PENDING`；RESET 胜出则 DRAIN 让位 |
| H2 | transaction 的 `NOT_COMMITTED_CLEAN` 恢复后漏发 RESET barrier | control-only/transaction 共用 `restore -> complete -> barrier` 收尾 |
| H3 | RESUME 先见 handoff unavailable，新 S generation 随后发布，transaction 未 claim | record lookup 后，在一个 `m_session_only_mutex` 临界区完成当前 S 分类与 provisional claim |
| H4 | control-only publish 的瞬时 `RESOURCE_EXHAUSTED` 被升级为 terminal CORRUPT | mutation 前 OOM 原样返回；同一 COMMIT 可重放 |

## 3. 实现约束

生产代码仅修改：

- `sql/preserve_trx.h`
- `sql/preserve_trx.cc`
- `sql/preserve_trx_transfer.h`
- `sql/preserve_trx_transfer.cc`

关键接口：

```cpp
bool Preserve_trx_drain_ownership_state::
    claim_source_restore_before_commit_send();

Preserve_trx_transfer_resume_handoff
preserve_trx_transfer_begin_resume_handoff_for_token(
    const std::string &token, bool transaction_candidate);
```

H3 规则：

- `token in S`：返回 session-only，不建 transaction claim。
- 真实 transaction candidate 且 `token not in S`：登记 `{generation, token}`；无 active S 时 generation=0 也登记。
- 同一 token 已有任意 generation claim：fail closed。
- 非 transaction candidate 只读取，不分配。
- scope guard 覆盖校验、普通恢复、promotion attach 与所有错误出口。
- begin/release 只持 S 锁，不与 preserved-record、receiver registry、THD 或 InnoDB 锁嵌套。

禁止另加 `commit_sent` 状态、持锁跨 attach、全局 RESUME mutex 或通用恢复框架。

## 4. 测试

### GUnit

- ownership：source-restore claim 胜出、RESET 胜出、COMMIT send 后拒绝 claim。
- handoff：generation=0 claim、S 发布后 consume 被挡、release 后恰好成功一次、重复 claim fail closed。
- codec：空/非空 S 往返；拒绝 zero、duplicate、unsorted、oversize、坏 magic、截断和 trailing bytes。
- terminal：T 与 S 重叠在 COMMIT 前失败；control-only OOM 不 poison，exact replay 成功。
- transport：verified ACK loss once/twice 进入 exact replay/QUERY；pre-wire 双失败进入 QUERY/ABANDON。

### MTR

新增/重建：

- `standby_transfer_unsent_transaction_restore_reset_race`
- `standby_transfer_control_only_unsent_ack_restore`
- `reset_drain_not_committed_clean_barrier`
- `standby_transfer_resume_generation_claim_race`

增强：

- `standby_transfer_resume_required_lifecycle`：真实 ACK loss、并发 one-shot consume、最终数据。
- `standby_transfer_resume_required_mixed`：移除无关 ACK 注入，保留 T/S 分类并核对最终数据。
- `standby_transfer_resume_required_phase2_commit`
- `standby_transfer_resume_required_final_snapshot`
- `standby_transfer_resume_required_control_only_final_snapshot`

H4 只用 receiver registry GUnit：强造双机 OOM 会引入额外 HA 仲裁依赖，属于过度设计。

每个竞态 MTR 必须有真实 DML、最终数据和 live-process residue oracle；timeout 不作为正式 RED。

## 5. Debug steering

- `preserve_trx_transfer_fail_commit_before_send_once`
- `preserve_trx_transfer_drop_commit_send_twice`
- `preserve_trx_transfer_drop_commit_ack_once/twice`
- `preserve_trx_receiver_control_only_publish_resource_exhausted_once`
- `preserve_trx_unsent_failure_before_source_restore_claim`
- `preserve_trx_unsent_source_restore_claimed`
- `preserve_trx_not_committed_clean_source_restore_pending`
- `preserve_trx_resume_after_handoff_begin`
- `preserve_trx_session_only_after_consume_before_reply`

ACK loss 位于 client sink 已认证 COMMIT ACK 之后：once 为 COMMIT exact replay；twice 为 replay 后 QUERY。预算属于当前 sink，不使用 process-lifetime static。

## 6. 验证顺序

1. Debug build：`mysqld`、`preserve_trx-t`。
2. focused GUnit。
3. 上述 9 个 MTR，`parallel=1 --retry=0`，分别显式 no-bin 与 log-bin。
4. Debug `preserve_trx-t` 全量。
5. Release clean build；对 `.i/.o/libsql_main.a/mysqld/preserve_trx-t` 做 selector/helper 负扫描，并跑 Release GUnit。
6. `preserve_trx` 全量 no-bin/log-bin，独立 vardir，`parallel=8 --retry=0`。

最终证据（2026-08-20，均 `retry=0`）：

- focused GUnit：12/12；focused MTR：no-bin、log-bin 各 9 个业务用例 + `shutdown_report`，全部 first-pass PASS。
- Debug GUnit：`preserve_trx-t` 804/804、`preserve_trx_temp_table-t` 329/329；Python 625/625，source lint 40 rules/0 finding。
- Release：`preserve_trx-t` 742/742；6 个 OFF-path MTR + `shutdown_report` PASS；`.i/.o/libsql_main.a/mysqld/preserve_trx-t` steering 字符串与符号 0 命中。
- full MTR：no-bin 345/345（126 条条件 skip），log-bin 301/301（170 条条件 skip），全部 first-pass PASS。
- 首轮 full no-bin 捕获 opaque 本地 token 被误判为未 claim；收窄为仅 numeric transfer token 执行 fail-closed 检查后，全量重新通过。

## 7. 提交

最终只生成一个提交，详细说明 H1-H4、原子 handoff、ACK-loss harness、GUnit/MTR 数据 oracle 与实际回归结果。暂存时排除用户已有的 `sql/preserve_trx.cc` 空白行以及所有无关 untracked 文件。

完成条件：四项实现闭合；focused 与全量 first-pass GREEN；Release 测试引导零足迹；生产差异仅上述四文件；最终单提交可复核。
