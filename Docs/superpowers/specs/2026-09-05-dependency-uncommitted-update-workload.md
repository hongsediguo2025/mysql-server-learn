# Dependency uncommitted updates 压测模型

## 业务行为

这是原 `dependency continuous` 的独立变体，不替换原模型，也不修改内核代码。

- 1000 个大事务连接，隔离级别 RR。每个连接只执行一次 `START TRANSACTION`，此后一直执行 UPDATE，不主动 COMMIT/ROLLBACK，也不在走完一轮数据后重新 BEGIN。
- 使用 128 张表，每连接独占自己的 `sid` 数据范围，共 100,000 行。大事务之间不争用同一行；每条语句以主键定位，更新 1 行或最多 10 行。
- 每个十行范围依次执行：单行递增、单行算术赋值、十行递增、十行条件赋值。四类 UPDATE 都实际修改数据，不是 no-op。处理完全部范围后从头继续，事务始终不结束。
- 另外 100 个短事务连接沿用原模型，使用独立的 50 张表；每两个连接共享一张表，形成非死锁的锁等待关系，短事务照常提交。
- 所有业务连接就绪后，控制模块只计时，默认继续运行 300 秒后直接发起一次 `DRAIN TRANSACTIONS`。此前不暂停业务、不发 checkpoint、不查询锁等待，也不预先通知业务停止。
- 业务一直发命令，直到服务端返回 4020；收到后保留原连接，等待控制流程完成后统一清理。不以断连或重连代替 hold。

“100,000 行”是每连接可访问的数据范围，不是事务结束条件，也不是已实际修改的行数。累计 UPDATE 数及行修改事件会随运行时间增长；不能将这种模型的开销等同于原先每轮提交的模型。长时间不提交会积累 undo/binlog cache，必须保留磁盘预检和原有资源限额，不靠自动提交缩小事务。

## 入口与规模

在仓库根目录执行一轮正式规模、业务运行 300 秒后 drain：

```bash
python3 scripts/preserve_trx_uncommitted_large_tx_full_pressure_e2e.py \
  --profile full --business-run-before-drain 300
```

`--business-run-before-drain` 可调整业务运行秒数；显式指定此参数时 runner 执行一轮。省略它则继承原正式入口的连续五轮，每轮业务窗口默认 300 秒。先检查实际下发参数可加 `--check-only`。

两个诊断档位沿用相同业务逻辑：

| 档位 | 大事务 + 短事务 | 每大事务连接可访问行数 | 业务窗口 |
| --- | --- | --- | --- |
| `smoke` | 32 + 8 | 200 | 8 秒 |
| `scale-smoke` | 128 + 16 | 100,000 | 35 秒 |
| `full` | 1000 + 100 | 100,000 | 300 秒 |

小 smoke 特意缩小行范围，快速证明“多次走完数据仍不提交”；不是大规模性能验收。模式继续使用 `DEPENDENCY_CONVERGENCE_V1 + BOUNDED_PIPELINE_V1`，复用既有 preserve/transfer/receiver 流程。

## 报告与验收

模型身份为 `UPDATE_FOREVER`，提交策略为 `NEVER`；兼容字段 `RANGE_10000` 仅用于选择十行范围布局，不代表执行 10,000 条后提交。固定“每事务语句数/修改行数”等字段输出 `null`，不虚报事务大小。

`report.json` 的 `continuous_large_no_commit` 输出每连接 BEGIN 次数、COMMIT 次数、完成事务数、实际成功 UPDATE 数、四种 UPDATE 的执行次数，以及所有连接最少已完成几轮数据遍历。必须每连接 BEGIN=1、COMMIT=0、完成事务=0，且四种 UPDATE 均已执行。通过验收后，相同证据同时进入 `result.json` 的 `large_no_commit` 指标。

仍输出 Phase1、包含调度的 T0→Phase2 结束、最后 command→Final ACK、receiver 本地 Final ACK→READY、命令响应时延、业务 UPDATE 吞吐和短事务 TPS 影响。大事务提交 TPS 按定义为零，不能用它评估大事务业务影响。锁等待未命中和 receiver read-load 波动沿用 report-only 规则；功能成功与正式性能达标分开报告。

## 初次验证（2026-09-05）

最终脚本版本的 `20260905-uncommitted-update-scale-smoke-r01` 为 128+16、RR、35 秒业务窗口，runner 验收通过。128 个大事务均为 BEGIN=1、COMMIT=0，累计成功 UPDATE 822,843 条；144 个业务连接收到 4020 后保留原连接，1205、断连、重连和其他业务错误均为零。

| 指标 | 实测 |
| --- | --- |
| Phase1 | 3,665.006ms |
| T0→Phase2 结束 | 478.578ms |
| 最后 command→Final ACK | 464.783ms |
| receiver 本地 Final ACK→READY | 48.073ms |
| 大事务 UPDATE 最大响应时间 | 103.593ms |
| 全部业务命令最大响应时间（含 4020） | 473.740ms |
| Phase1 大事务 UPDATE 吞吐降幅 | 22.44% |
| Phase1 短事务 TPS 降幅 | 23.06% |

本轮不满足正式规模身份，业务降幅也超过正式 20% 目标，不能称为正式 SLO 达标。

后续 `20260905-uncommitted-updates-full-r01` 已实际执行 1000+100、301.022006 秒业务窗口；DRAIN 成功且 1060/1060 READY，但 runner 验收失败、`formal_evidence=false`。该轮 T0→Phase2 结束为 2,062,997us，最后 command→Final ACK 为 2,048,991us，另有业务命令响应超限等失败项。它证明正式规模已执行，不证明正式 SLO 达标；以上初次 smoke 数据也不能替代当前代码的验收。

同批次 32+8 小 smoke 的 r01 通过并证明每连接至少完成 70 次数据遍历仍不提交；r02、r03 均 DRAIN 成功，但 T0 恰好没有 BODY（`NO_ELIGIBLE_BODY`），缺少最后 command 的精确样本，原验收器据此拒绝。r03 另有 `receiver_worker_active=1` 门禁失败，未在本任务中修改其实现或放宽门禁。这些失败没有以重跑结果替代。

报告保存在 `build-release/preserve-final-evidence/fullpressure-runs/` 下同名 run-id 目录；本批测试临时 datadir 已由 runner 清理。另已执行既有 50 项脚本回归并通过；四项内存报告变异（重复 BEGIN、出现 COMMIT、完成事务数非零、无 UPDATE）均被校验器拒绝，未新增 unit test。
