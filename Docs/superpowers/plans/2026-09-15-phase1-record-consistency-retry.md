# Record 捕获一致性冲突：100ms 重试方案与实施记录

> 状态（2026-09-15）：窄化重试已实现，定向 MTR 和十轮 TPC-C 已执行；按下文原始结果分别记录，不代表完整功能或性能验收。本文是已实施方案记录，不要求再次执行旧 RED/回撤步骤。

**目标：** 活跃业务导致 record 捕获一致性冲突时，只让受影响 target 延后 100ms 再捕获；HARD 后新捕获仍不一致则失败。

**实现：** 复用 record owner 的 `RETRY_WAIT / next_retry_us / capture_generation`，结果与 credit 结清之后才重试。不增加线程、队列或阶段，不改 transfer/receiver。仅依赖调度 + bounded pipeline + standby transfer 的现有内部模式门禁开启新分类。

**关联：** [调度主设计](../specs/2026-08-26-preserve-trx-phase2-command-scheduler-design.md) 负责命令准入；[T0 前 purge 停止及锁候选边界](../specs/2026-09-14-pre-t0-purge-pause-and-rollback-review.md) 负责后台维护与严格最终校验。本文只补充普通 record 捕获的一致性重试。

## 边界和代码量

- 修改三个已有生产文件：`sql/preserve_trx_phase1_pipeline.h`、`sql/preserve_trx_phase1_record_adapter.cc`、`sql/preserve_trx_phase1_owner.cc`。
- 实际独立切片为 +74/−15，其中 16 行是 Debug 接缝及说明；不是当前工作区总代码量。
- 已撤掉 adapter 中 31 行临时 presence 日志，保留已验证的“近似 native count 不作为判空依据”修正。
- 当前普通 capture 在 T0 前关闭；此变更不在 T0→HARD 另开普通 capture。持续冲突可用完既有 Phase1 时间预算（默认 600000ms，TPC-C 配置另有显式值），到期停止新普通提交并进入原交接流程。
- 普通候选过期后从 store 或 native 补齐是正常路径，不改成失败。资源暂不足、分片/页/MDL 暂忙、epoch 关闭、事务换代仍走原语义；不将所有 RETRYABLE 改成一致性冲突。
- 不新增 unit/GUnit；不扩 RESET；不改 InnoDB 锁热路径、purge 生命周期、任何 ACK/epoch/READY 规则。

## 1. 已完成：固定旧行为 RED

- [x] 保存这三个文件的当前切片前像；单独删除临时诊断并与前像核对。
- [x] 在真实 RR UPDATE 捕获后的现有 presence 校验处添加 Debug-only 冲突注入。仅选中单 bit 快照，两个 bit 的另一业务事务作为正常对照；不修改原生锁。

```cpp
// Test seam: force the existing presence predicate to reject this snapshot.
bool presence_mismatch = record_locks_empty != (snapshot.exported_record_bits == 0);
// Debug flags select first two ordinary captures, all ordinary captures,
// or final captures. Only snapshots with one exported bit are selected.
```

- [x] 新增同名 .test/.result 的重复冲突、Phase1 到期、HARD 后冲突 MTR，公共 fixture 使用真实 UPDATE、独立 HA 连接、standby transfer，结尾 kill/restart 旧主。
- [x] Debug 增量编译并保留旧策略 RED：`var-cr-red2` 真实提交两个 capture，第一份结果触发 `PRODUCTION_RECORD_BASELINE_FAILED / record_capture_presence_mismatch`。首轮 `var-cr-red` 的 1s 预算不足以通过原生操作预留，未执行 capture，不计为有效 RED；测试改为 5s，未修改产品参数默认值。

## 2. 已完成：实现窄化策略

- [x] pipeline 内部结果枚举加入 `CONSISTENCY_CONFLICT`（无 wire/持久化含义）。
- [x] adapter 用现有 `omit_granted_insert_intentions` 模式门禁选新分类，旧模式保留调用点原状态：

```cpp
return preserve_trx_lock_warmcopy_current_options().omit_granted_insert_intentions
           ? Preserve_trx_phase1_pipeline_result_status::CONSISTENCY_CONFLICT
           : legacy_status;
```

- [x] 映射范围仅为 LOCK_SET_CHANGED、精确 presence mismatch、同 epoch 且同事务的 capture 前后 compare token 变化、native 新快照的 LOCK_FENCE_CHANGED。store 快照旧 fence 变化仍 STORE_FALLBACK；事务身份变化仍 IDENTITY_STALE。resolve 的 broad IDENTITY/NOT_FOUND 和含 epoch 关闭的 install CAS 拒绝不粗暴重分类。
- [x] owner 同时识别 worker result 和 install result 的新状态，沿用统一 settle/drop；最终新捕获冲突保留原 reason 并失败；普通冲突绕过原“一次 retry 后 defer”的上限，但仍受 Phase1 deadline 控制。

```cpp
if (consistency_conflict && entry->descriptor.final_generation) {
  fail(reason);
  return;
}
// ordinary_submissions_finished remains authoritative.
const uint64_t delay_us = consistency_conflict ? 100000 : k_record_retry_base_us;
```

## 3. 已完成的验证与清理

- [x] 重复冲突：两次 100ms；另一个事务先 published；原事务最终成功捕获，两 token READY。
- [x] 持续普通冲突：既有操作预留使 admission 在 5s 总预算前收口；40 次重试后 deferred，HARD 后 native 最终补齐成功、receiver READY。
- [x] HARD 后冲突：最终新捕获确实发生并因原 presence reason 失败，两个源事务的未提交值仍可读和回滚；credit/permit 最终清零。
- [x] 重跑真实 lock timeout、空 store/native fallback、final dirty store、等待授锁 fallback 及 pipeline mode，no-bin/log-bin 分别保留结果。
- [x] 两轮只读源码审核未发现本切片确定新增问题；已补强 MTR 实际间隔、健康事务进展、最终原生补齐与失败原因的断言。
- [x] Release 编译；复用当前 source/receiver 数据完成十轮 300 仓、1000 RR 并发、300 秒后 DRAIN。逐轮保留原始功能、全部 READY、阶段和业务错误指标；完成十轮执行不等于十轮功能或性能全绿，结果见下。

复核命令模板（本次文档整理未运行；每轮使用新 vardir，不覆盖旧证据）：

```sh
cmake --build build-debug --target mysqld -j8
cd build-debug/mysql-test
MTR_PORT_BASE=auto MTR_BUILD_THREAD=auto perl mysql-test-run.pl --suite=preserve_trx --do-test='^batch_drain_phase1_consistency_' --parallel=1 --retry=0 --force --mysqld=--skip-log-bin --vardir=var-consistency-check
# log-bin 使用另一个新 vardir，将 --skip-log-bin 替换为 --log-bin=mysql-bin。
```

## 本轮验证记录（2026-09-15）

本机历史证据目录：`build-release/diagnostics/presence-predicate-fix-20260915-P8p0F4/`。诊断路径不是版本库必备文件，历史数据库副本可能已按授权清理；下表只记录当时结果。

| 项目 | 当前结果 |
| --- | --- |
| 内核切片 | 3 个已有文件，合计 +74 / −15；其中 16 行为 Debug 接缝及说明，Release 不含注入/trace 字符串 |
| 临时诊断清理 | 另删除 31 行 `PRESERVE_RECORD_PRESENCE_DIAG`，与诊断前像逐字核对 |
| MTR 增量 | 3 个用例 + 公共 include + 3 个 result，共 193 行；无新增 unit/GUnit |
| 新场景 no-bin | 3/3，`retry-green-nb2-mtr.log` |
| 新场景与既有路径 log-bin | 8/8，`retry-green-lb-mtr.log` |
| 既有路径 no-bin | 5/5，`retry-existing-nb-mtr.log` |
| 重试实测（log-bin） | 连续冲突两例各 40 次，间隔 100.129–102.037ms；两次冲突例间隔 101.060ms |
| Debug / Release 编译 | 均成功；Release SHA256 `00fbc78b57c6520d6fc6c9c93535524efb37d4f5acf6b02a70e5b5ab884f70ce` |

上述 16 次均为业务用例执行次数，不含三份 `shutdown_report`。第一轮 GREEN 尝试存在新测试的场景标识传递/预期输出问题，已修正测试并以全新 vardir 重跑，未为此改内核。

这些结果证明重试策略及相关回退路径通过定向测试，不代表真实 TPC-C 十轮或性能 SLO 已通过；不使用之前 binary 的指标代替本次结果。未提交。

### 当前 Release 的 TPC-C 十轮复用回归

证据索引：`build-release/diagnostics/tpcc-record-retry-reuse10-20260915/race-index.json`；逐轮原始报告在 `attempt-NN/r01/report.json`。300 仓、1000 RR 并发、无 think/rate 限制；每轮双端重启，复用同一对 datadir，业务满 300 秒后 DRAIN。

十轮均为同一 Release SHA256，复用同一 source/receiver datadir 和 UUID，未重新装载或复制种子。每轮 1000 个原始业务连接收到 4020 后 HOLD、重连数为 0；source SIGKILL、receiver 正常退出，然后下一轮双端重新启动。以下时间单位均为微秒；Phase1 包含 T0 前的 purge 停止等待。

| 轮次 | DRAIN / READY | 原始整轮结果 / DRAIN 窗口 1205 | Phase1 | purge 停止 | 100ms 一致性重试 / 既有 1ms 暂忙重试 |
| --- | --- | --- | ---: | ---: | ---: |
| 1 | 成功 / 191/191 | FAIL / 8 | 2,004,416 | 5,114 | 0 / 0 |
| 2 | 成功 / 232/232 | FAIL / 1 | 2,418,600 | 15,713 | 0 / 0 |
| 3 | 成功 / 219/219 | FAIL / 5 | 2,569,159 | 166,181 | 3 / 0 |
| 4 | 成功 / 219/219 | FAIL / 2 | 2,552,408 | 28,977 | 1 / 0 |
| 5 | 成功 / 252/252 | FAIL / 4 | 2,710,151 | 4,385 | 0 / 0 |
| 6 | 成功 / 194/194 | FAIL / 6 | 2,332,119 | 2,109 | 1 / 0 |
| 7 | 成功 / 213/213 | FAIL / 2 | 2,183,539 | 52,552 | 10 / 2 |
| 8 | 成功 / 214/214 | FUNCTIONAL_PASS / 0 | 1,962,128 | 296 | 0 / 1 |
| 9 | 成功 / 208/208 | FAIL / 6 | 1,978,143 | 5,616 | 7 / 0 |
| 10 | 成功 / 198/198 | FAIL / 2 | 2,264,529 | 186,697 | 1 / 2 |

| 轮次 | T0→HARD | T0→Final ACK | T0→Phase2 结束 | 最后命令→Final ACK（EXACT） | receiver 本地 ACK→READY |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 3,326,981 | 4,015,547 | 4,019,814 | 689,002 | 3,104 |
| 2 | 2,688,016 | 3,486,242 | 3,490,066 | 799,676 | 5,101 |
| 3 | 4,447,355 | 5,110,536 | 5,114,388 | 663,535 | 2,326 |
| 4 | 4,487,162 | 5,119,280 | 5,121,897 | 632,496 | 4,902 |
| 5 | 2,326,204 | 2,940,841 | 2,944,619 | 614,952 | 4,450 |
| 6 | 2,589,350 | 3,018,060 | 3,019,586 | 429,091 | 2,808 |
| 7 | 3,852,852 | 4,460,289 | 4,462,021 | 607,844 | 3,191 |
| 8 | 3,216,385 | 3,685,369 | 3,686,374 | 469,397 | 3,193 |
| 9 | 3,743,031 | 4,286,726 | 4,289,234 | 543,976 | 4,425 |
| 10 | 2,594,250 | 3,074,111 | 3,075,746 | 480,230 | 2,760 |

| 轮次 | DRAIN 前最后 60 秒 TPS | 整段 DRAIN 采样 TPS | 对应降幅 |
| --- | ---: | ---: | ---: |
| 1 | 543.835 | 153.610 | 71.754% |
| 2 | 531.001 | 159.960 | 69.876% |
| 3 | 504.922 | 228.980 | 54.650% |
| 4 | 504.287 | 162.520 | 67.772% |
| 5 | 517.145 | 130.400 | 74.785% |
| 6 | 518.875 | 无可用采样值 | 不计算 |
| 7 | 552.255 | 98.430 | 82.177% |
| 8 | 557.317 | 139.950 | 74.889% |
| 9 | 493.782 | 143.190 | 71.001% |
| 10 | 522.254 | 209.450 | 59.895% |

整段 DRAIN 采样 TPS 包含命令收敛和 HOLD，且受采样粒度影响，不是精确 Phase1 TPS；十轮 `exact_phase1_tps` 均无可用值，不能用此表宣称 Phase1 业务影响指标已经验证。

本切片的真实压力证据：十轮未复现原 record 捕获失败，累计 2,140/2,140 个应接纳事务 READY；23 次 `record_install_native_fence_changed` 使用 100ms 重试、触发轮次最终均成功。另有 5 次 `record_lock_capture_shard_busy` 仍使用原来的 1ms 重试，未被粗暴改成一致性冲突。重试 telemetry 的延迟是逐 target 调度延迟之和，不等于 owner 的串行停顿。十次最终 record reconcile 均 `failed=0 / invalidated=0`；这些结果不是对所有潜在竞态不存在的证明。

完整验收仍有未达项：第 8 轮原始功能通过，其余九轮因 DRAIN 窗口 1205 保留 FAIL；十轮 T0→Phase2 结束均超过 2 秒。最后命令→Final ACK 为 429.091–799.676ms，其中三轮低于 500ms；receiver 本地 ACK→READY 为 2.326–5.101ms。未做同轮条件的旧/新 binary 性能 A/B，不能据此断言性能绝无退化。

36 次 DRAIN 窗口 1205 全部匹配到 DRAIN 前已有的同 SQL、trx、requested lock 等待；前述首次观测通常早于 DRAIN 约 45–51 秒。第 2 轮连接 531 的 `INSERT INTO new_orders1 ... VALUES (3184,10,211)` 例如在 DRAIN 前约 49.7 秒已等待，48 个采样的事务 ID、requested lock ID、wait_started 与 SQL 一致。锁边采样存在截断，不能据此认定完整阻塞链，也不足以证明无 DRAIN 时必然超时或 DRAIN 对剩余等待没有影响。逐轮 `error-observation-audit.json` 保留关联证据，未修改原始验收门槛。

### 历史运行说明

十轮运行当时使用 26GiB 启动空间门槛和 8GiB 运行保护线，未降低门槛。数据复用、历史 binlog 清理、证据保留及端口等待由本机测试脚本处理，不进入内核。历史目录是否仍存在不作为设计前提，也不把当时“datadir 未动”的记录写成当前数据仍保留。

第 8 轮另有一次业务启动前的 `Address already in use`，未启动业务、未执行 DRAIN，不计为正式一轮。监督程序成功退出只表示指定复测完成，不覆盖各轮原始功能 FAIL 或 SLO FAIL。
