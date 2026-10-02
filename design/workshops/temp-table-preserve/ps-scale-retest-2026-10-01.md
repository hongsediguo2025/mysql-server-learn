# PS 优化后 500／1000 连接复测（2026-10-01）

> **2026-10-04 版本说明：历史版本记录。** 文内“当前/尚未完成/通过”和源码路径均属于记录时点；旧 PS 定义/参数/重建方案不再适用，原失败与测量不改写为新版本成绩。 现行职责见[详细设计](detailed-design.md)和[PS 删除与 cursor 关联](ps-transfer-removal-and-cursor-attach.md)，最新状态见[任务跟踪 E63/E64](task-tracker.md)。下文保留历史事实，不作为现行实施清单。

本轮完成两档 Release 读写复测。500 档全部 READY，三个收尾时延门槛通过；1000 档全部 READY，但三个门槛均失败。两档 Phase1 均未达到 10 秒目标，不能宣布规模性能闭环。本轮没有修改内核代码，没有提交。

## 1. 固定输入与执行方法

工作目录为 `mysql-server-8022-preserve-port`，分支 `ha_preserve_trx`，HEAD 为 `324a8ec01101efa58977966fe4fba2aea40cca75`。测试使用包含此前未提交优化的 Release `mysqld`，SHA256：

```text
424e09ba3b8ee0aba637f5e5b71544e773daa282b966bb1f410248ae188ab994
```

复用 E46／E47 的容量诊断参数，500 与 1000 顺序执行，每档一个有效轮次：

| 参数 | 设置 |
|---|---|
| 业务 | `oltp_read_write`，`skip_trx=off`，原生 PS |
| 每连接 PS | 1153；总量分别为 576500／1153000 |
| 数据 | 128 张表，每表 20000 行 |
| 稳态 | 全部连接就绪后，60 份完整秒级报告 |
| Source／receiver Preserve 预算 | 各 4 GiB |
| Source／receiver buffer pool | 各 2 GiB |
| 原 Phase1 pipeline worker | 6；未新增线程池 |
| Phase1 配置超时 | 600000 ms；不代表实际执行一定精确止于 600 秒 |
| strict Phase2 门槛 | 2000000 μs |
| 最后命令结束至 Final ACK 门槛 | 500000 μs |
| Final ACK 至 READY 门槛 | 500000 μs |
| 隔离级别 | 两端实际变量均为 `REPEATABLE-READ` |

credit、reserve、chunk、result slot、transfer inflight 等配置与历史同档逐项相同。profile 中通用 `business_transaction_isolation=READ-COMMITTED` 字段不在该 sysbench 路径应用，不能据此写成 RC。

sysbench 使用无限时运行；业务持续至 DRAIN 完成，随后原连接保持 4020 HOLD，等待 receiver READY，再核对连接 ID 后受控停止。未在测量期间运行构建、MTR或调用栈采样。观测为每秒 SQL／主机状态、每 5 秒进程资源计数；沿用磁盘少于 4 GiB 的保护。

运行前后的 219 项源码、脚本和二进制校验值不变；另在 500 稳态期间、DRAIN 前记录的 14 项 sysbench／Lua 输入，结束时也一致。上一轮 42 项冻结输入亦全部一致。此处不把补充工作负载清单说成在 500 启动前采集。

本轮是 4 GiB／60 秒容量诊断，不替代原 2 GiB／300 秒 full profile，也不替代 64 连接、10 秒 Phase1 的配对结果。

## 2. 精确结果

| 指标 | 500：`scale500-after-r2` | 1000：`scale1000-after-r1` |
|---|---:|---:|
| DRAIN | SUCCESS | SUCCESS |
| SURVIVOR／READY／NOT_READY | 500／500／0 | 1000／1000／0 |
| Phase1 开始至请求停 purge | 90.784586 s | 610.648156 s |
| Phase1 开始至 pre-closing | 90.843604 s | 615.464864 s |
| **strict Phase2** | **357.357 ms，通过** | **98.785692 s，失败** |
| 最后命令结束至 Final ACK | 304.068 ms，通过 | 98.086278 s，失败 |
| Final ACK 至 READY | 472.315 ms，通过 | 1403.860 ms，失败 |
| DRAIN SQL 墙钟 | 91.428656 s | 716.574560 s |
| 稳态 TPS | 1384.6325 | 661.5713 |
| 稳态 QPS | 27685.6780 | 13465.2557 |
| DRAIN 期间平均 TPS，报告值 | 971.8920 | 190.3442 |
| 源 Preserve 记账峰值 | 1048180370 B | 1912997955 B |
| receiver Preserve 记账峰值 | 1835884122 B | 3688665368 B |
| 源 wire 复用／整份 fallback | 500／0 | 1000／0 |
| receiver descriptor 最终复用 | 500 | 1000 |

500 的 READY 余量仅 27.685 ms，单轮通过不构成稳定余量证明。`source_phase2_total_us` 属于旧 warmcopy 口径，不用于替换 strict interval。

两轮均核对了 DRAIN 前、DRAIN 后和停止 sysbench 前的原连接 ID 集合，并确认 `receiver_ready_before_sysbench_stop=true`。两端在 DRAIN 后存活；1000 runner 退出 1 的直接原因是 `FinalRecordError: strict_interval_us=98785692 exceeds 2000000`，不是服务端崩溃或功能结果缺失。失败后没有放宽 oracle。由于校验在写入 `validation.records` 前抛错，1000 的精确 FINAL 字段由归档的唯一 `PRESERVE_PHASE2_FINAL_V1` 原始日志提取，汇总明确标记来源。

sysbench 受控 SIGTERM 后没有常规总计输出，因此报告中的 `transactions=queries=0` 不能解读为没有业务；吞吐使用稳态 60 份报告。4020 HOLD 与业务错误分开统计。

500 的 152 份 err/reconnect 报告均为零，另有 500 个不同线程的预期 HOLD。1000 的稳态 60 份错误报告为零、全部 778 份 reconnect 为零、fatal 为零；整轮有三份非零 err/s：第 110 秒为 1.00（Phase1 期间，不能用 4020 解释，当前日志未给出具体 SQL 错误码），第 774／775 秒为 801.86／202.55，伴随 1000 个不同线程的 HOLD。因此不能宣称 1000 整轮业务错误为零，或将全部非零错误都归入切换通知。

## 3. 500 档：final 工作减少，Phase1 仍然慢

相对 pipeline STARTED 日志，秒级观测首次看到：

```text
41.27 s：初始捕获达到 576500 条；已封存描述仅 144 份
89.61～89.64 s：receiver early-ready 与源端已确认封存均达到 500 份
90.68 s：请求停 purge 的日志
```

这些是异步查询的首次观测时间，精确查询耗时保存在 `summary.json`，不是内核完成时间戳。正式 Phase1 口径还包含 pipeline STARTED 前约 108 ms。

初始捕获完成至全部描述确认仍有约 48 秒，包含源端编码、调度、proof refresh、发送确认与 receiver 工作，不能直接命名为网络或 ACK 等待。receiver early-ready 与源端封存数在采样上基本同步，不能由此证明单个环节没有成本。

对比 E46 的历史 500 轮，final 原生读取由 79470 次降至 110 次；final proof 累计墙钟由 14.591 秒降至 1.254 秒。此次 source target-worker 窗口为 111.254 ms，commit-epoch 为 92.628 ms。累计 proof 耗时与并行 worker 墙钟不能相加。

## 4. 1000 档：提前捕获不充分，将工作带入最终阶段

最终计数对齐为：

```text
Phase1 初始捕获               862820 条
最终补充捕获                 290180 条
合计                        1153000 条

Phase1 已确认封存／提前准备      98 份
最终 wire 复用                1000 份
其中 partial 复用              900 份
整份 fallback                    0 份
```

因此 `wire_reused=1000、fallback=0` 并不表示 PS 工作全部在 Phase1 完成。`ps_source_wire_final_capture_statements=290180` 与 `ps_source_wire_partial_reused=900` 直接证明最终阶段补齐了大量尚未完成的状态。

约 25.17% 的 PS 在最终阶段补齐。`sql/preserve_trx_ps_pretransfer.cc` 的 `try_reuse()` 会为未完成 snapshot 捕获缺失尾部，再编码和比较；partial 复用与整份 fallback 为零可以同时成立。receiver 第一份完整提前准备约在 pipeline STARTED 后 206.4 秒被观测到，全部 1000 份约在 710.236 秒才被观测到，不能用最终 `early_ready=1000` 代替 Phase1 完成数。

Source FINAL 的原生读取为 311687 次，fast 为 841313 次。final proof 累计墙钟 420.765 秒，其中 native 239.104 秒、match 167.972 秒；这些均是并行累计量。实际 target-worker 窗口为 **94.296 秒**，commit-epoch 为 **0.716 秒**。94.296 秒约占 strict interval 的 95.45%，说明本轮主要长段在源端 target-worker 窗口，不能将 98 秒称为纯网络 ACK 等待；该窗口本身仍包含多类工作与等待。

配置 Phase1 超时为 600 秒，实际请求停 purge 为 610.648 秒。日志有 `submit_deadline=1`；这 10.648 秒差异的组成尚未单独计时，不能把配置值当作精确实际阶段耗时。purge 请求到 pre-closing 另有约 4.817 秒。

计数含义已对照源码：

- `capture_statements` 在每条 PS 成功 capture/proof bind 后累计；每批至多 32 条。参见 `sql/preserve_trx_ps_restore.cc` 与 `ps_pretransfer.cc`。
- descriptor 计数在发送成功且 sealed 后增加；零值不意味着网络上没有在途字节。
- receiver `descriptor_early_ready` 在 sealed 文件解码与 native preparation 完成后增加，不是接收帧计数。参见 `sql/preserve_trx_receiver_candidates.cc`。
- `capture_parallel_max=6` 是历史并行高水位，不能用来证明整个阶段持续占满 6 个 worker；busy/no-slot 是累计次数，不是阻塞秒数。

本轮确认的剩余问题是 **Phase1 捕获和交付吞吐不足，最终阶段仍需处理大批 PS**。为什么此次 1000 档比历史更慢、各项优化与换页分别贡献多少，尚没有受控对照足以定量归因；不据此新增推测性内核修复。

## 5. 主机资源与历史比较边界

| 进程采样峰值 | 500 | 1000 |
|---|---:|---:|
| source footprint | 19.165 GB | 36.684 GB |
| source RSS | 5.235 GB | 7.574 GB |
| receiver footprint | 2.267 GB | 3.873 GB |
| receiver RSS | 0.509 GB | 1.618 GB |

上表为十进制 GB。footprint、RSS、Preserve 账本是不同口径，不可相加。主机物理内存为 16 GiB；1000 轮约 812.7 秒主机观测区间内，换入 139.61 GiB、换出 144.35 GiB，换页压力有直接证据，但全机流量不能全部归属本进程，也不等同缺页等待时间。

进程 CPU 原始计数按本机 `mach_timebase_info=125/3` 换算，并用相邻 `ps time` 核对。1000 source 业务进程约 813.8 秒观测窗内，用户 CPU 376.84 秒、系统 CPU 1066.26 秒；receiver 分别为 6.30／10.27 秒。这是进程全窗口 CPU，包含业务和迁移，不能当作 Phase2 CPU。原始值与换算记录保存在 `host-timebase.json`、`proc-usage.jsonl` 和 `summary.json`。

1000 轮 source SQL observer 在 Phase1 有 29 次超时，host 的 `ps` 有 8 次超时，秒级曲线存在空洞；不能把采样点间距当作精确阶段耗时。最终阶段时间以服务端 FINAL 记录为准。observer 最终退出且 `failure=null` 仅表示未触发保护，不表示每次采样成功。

| 历史同档与本轮 | E46 500 | 本轮 500 | E47 1000 | 本轮 1000 |
|---|---:|---:|---:|---:|
| Phase1 至 purge，s | 101.278 | 90.785 | 435.345 | 610.648 |
| strict Phase2，s | 1.350 | 0.357 | 26.940 | 98.786 |
| tail，s | 1.204 | 0.304 | 26.556 | 98.086 |
| ACK→READY，ms | 331.274 | 472.315 | 2718.869 | 1403.860 |

500 本轮收尾改善，1000 本轮明显更慢，不能只报告通过的档位。历史驱动在 DRAIN 后核对 HOLD 即停止 sysbench，本轮修正为 HOLD 至 receiver READY；又是本机单轮、跨运行比较，因此不能把所有差异归因某个内核改动。后续定位应保持该修正后的驱动与固定负载，先分清 Phase1 剩余捕获／描述交付的工作量、CPU 与等待，再做单变量验证；10 秒 Phase1、1000 档收尾门槛及 V03 继续开放。

## 6. 证据与清理

全部材料在 `build-release/ps-scale-retest-20261001/`：

- `run_scale.py`：冻结当前二进制的规模驱动；`runs/*/checklist.json` 保存完整参数与验收门槛。
- `runs/scale500-after-r2/`、`runs/scale1000-after-r1/`：原始 report/result、服务端日志与 sysbench 日志。
- `observation/`：两端状态、主机资源和进程资源时间序列。
- `summary.json`、`summarize.py`：可重新生成的汇总；保留失败轮的原始错误与 FINAL 来源。
- `frozen-inputs.json`、`workload-inputs.json`、`host-timebase.json`：输入身份与单位校验。

`scale500-after-r1` 仅执行了 check-only；随后误用相同 run-id 被驱动拒绝覆盖，未启动业务。这次启动前拒绝及日志保留，不计为有效性能轮次；正式 500 使用 r2。

两轮 observer 均正常退出，未触发磁盘／时间保护。两端在测量完成时均存活；之后由原驱动对独占 source 执行有记录的退休清理，对 receiver 正常停止，不属于运行中异常退出。两个测试 datadir、socket 和进程均已清理，日志保留，结束时磁盘约 14 GiB 可用。

本轮覆盖 DRAIN→receiver READY 与原连接 HOLD；不新增外部物理升主／SQL RESUME 的实测结论。未修改内核、未提交，完整回归矩阵状态不变。
