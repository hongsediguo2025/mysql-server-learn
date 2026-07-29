# Phase1 Readiness 超时进入 Phase2 实施计划

> **供实施代理使用：** 必须使用 `superpowers:subagent-driven-development`
>（推荐）或 `superpowers:executing-plans`，逐项执行本计划。每个步骤使用
> `- [ ]` 复选框跟踪状态。

**目标：** 复用 `preserve_trx_transfer_phase1_timeout_ms` 作为 Phase1
readiness 的绝对 deadline。deadline 已到时，立即结束 readiness 观察并进入
`WARMCOPY_CLOSING`。

**架构：** 保持 Phase1 participant、artifact、transfer 和 prewarm 工作完全
不变。只在 `preserve_trx_wait_for_phase1_readiness()` 中增加一个提前判断
deadline 的分支。现有调用方仍把 `DEADLINE` 当作非错误结果，并继续转换到
`WARMCOPY_CLOSING`。RESET、DRAIN owner 被 kill、Phase1 准备失败和传输失败
仍然按照现有逻辑失败或中止。

**技术栈：** MySQL 8.0.22 C++、DBUG/DEBUG_SYNC、Preserve/Resume MTR。

---

## 1. 行为合同

继续只使用 `preserve_trx_transfer_phase1_timeout_ms` 这一个参数。
两阶段 DRAIN 开始后，现有代码只创建一次绝对 deadline：

```cpp
const ulonglong phase1_readiness_started_us = preserve_trx_monotonic_us();
const ulonglong phase1_readiness_deadline_us =
    preserve_trx_monotonic_deadline_after_ms(
        phase1_readiness_started_us,
        preserve_trx_transfer_phase1_timeout_ms);
```

本次只修改 readiness 的行为：

1. readiness 全量扫描 THD 之前，优先检查 RESET 和 owner-kill。
2. 如果 Phase1 绝对 deadline 已到，立即返回 `DEADLINE`，不扫描 THD、不执行
   可选 progress callback，也不进入轮询休眠。
3. readiness 已经处于轮询时，每轮在再次扫描 offender 或执行可选 progress
   操作之前，先检查同一个绝对 deadline。
4. 现有调用方收到 `DEADLINE` 后，继续进入 `WARMCOPY_CLOSING`。

这个超时不会：

- 在 participant open、artifact 构建、source 发送或 sender flush 的单个操作
  执行到一半时强制中断；
- 把 Phase1 准备失败或传输失败变成成功；
- 改变 `preserve_trx_drain_closing_command_timeout_ms` 的语义；
- 改变 `preserve_trx_drain_command_timeout_fail_batch` 的语义；
- 保证 Phase2 或整个 DRAIN 最终成功。

## 2. 文件范围

只允许修改以下 4 个 tracked 文件：

- `sql/preserve_trx.cc`
  - 增加 deadline 提前返回，并调整轮询中的 deadline 检查顺序；
- `sql/sys_vars.cc`
  - 澄清现有参数的帮助文本；
- `mysql-test/suite/preserve_trx/t/batch_drain_phase1_readiness.test`
  - 增加“进入 readiness 前 deadline 已到”的运行时场景；
- `mysql-test/suite/preserve_trx/r/batch_drain_phase1_readiness.result`
  - 保存确定性的 MTR 预期输出。

不修改头文件、transfer、receiver、promotion、InnoDB、SQL 语法、
状态变量或构建系统。

预计生产代码新增约 **8 至 16 行**，调整约 **3 至 8 行**。
预计 MTR 增加约 **45 至 75 行**。

## 3. 实施任务

### 任务 1：先增加失败的运行时合同

**涉及文件：**

- 修改：
  `mysql-test/suite/preserve_trx/t/batch_drain_phase1_readiness.test`
- 修改：
  `mysql-test/suite/preserve_trx/r/batch_drain_phase1_readiness.result`

- [ ] **步骤 1：在现有 readiness MTR 中增加第二个场景**

复用现有 HA 账号和 DEBUG_SYNC 基础设施。设置较短的 Phase1 deadline，以及
较长的 CLOSING command deadline：

```sql
SET GLOBAL preserve_trx_transfer_phase1_timeout_ms=50;
SET GLOBAL preserve_trx_drain_closing_command_timeout_ms=5000;
```

在 `before_execute_sql_command` 阻塞一个普通 command。同时使用现有
`preserve_trx_phase1_readiness_before_wait` 调试点阻塞 DRAIN owner，直到
50 ms 的绝对 deadline 已经过期：

```sql
SET DEBUG_SYNC=
  'preserve_trx_phase1_readiness_before_wait
   SIGNAL phase1_deadline_tail
   WAIT_FOR phase1_deadline_continue';
SET DEBUG_SYNC=
  'preserve_trx_warmcopy_after_closing_state_before_targets
   SIGNAL phase1_deadline_closing';
```

控制连接等待 `phase1_deadline_tail`，等待 100 ms 后发送
`phase1_deadline_continue`。在释放被阻塞的 command 之前，控制连接必须先观察到
`phase1_deadline_closing`。这证明触发 CLOSING 转换的是 readiness deadline，
而不是旧 command 已经完成。

释放 command 并等待 DRAIN 返回后，检查：

```sql
SELECT VARIABLE_VALUE=0 AS expired_readiness_did_not_scan
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_phase1_readiness_samples';
SELECT VARIABLE_VALUE=0 AS expired_readiness_did_not_wait
  FROM performance_schema.global_status
 WHERE VARIABLE_NAME='Preserve_trx_phase1_readiness_wait_us';
```

测试清理阶段把两个全局参数恢复为默认值。

- [ ] **步骤 2：实现前运行测试，确认测试能够捕获当前缺口**

```bash
cd build-debug/mysql-test
perl mysql-test-run.pl \
  --suite=preserve_trx \
  --do-test=batch_drain_phase1_readiness \
  --mysqld=--log-bin=mysql-bin \
  --force
```

预期结果：`expired_readiness_did_not_scan` 断言失败。当前实现即使绝对 Phase1
deadline 已到，仍会先进行一次完整的 THD 采样。当前代码随后会在轮询阶段发现
deadline，因此仍可能到达 `phase1_deadline_closing`，但已经发生了不必要的
THD 采样和 offender 遍历。

### 任务 2：实现收敛的 readiness deadline 判断

**涉及文件：**

- 修改：`sql/preserve_trx.cc`
- 修改：`sql/sys_vars.cc`

- [ ] **步骤 1：增加进入 readiness 时的提前判断**

在 `preserve_trx_wait_for_phase1_readiness()` 开头、
`preserve_trx_phase1_readiness_before_wait` 之后，先保持控制操作的优先级，
再检查绝对 deadline：

```cpp
if (preserve_trx_active_drain_reset_requested(active_drain_attempt))
  return Preserve_trx_phase1_readiness_result::RESET_REQUESTED;
if (owner->killed)
  return Preserve_trx_phase1_readiness_result::OWNER_KILLED;

const ulonglong sampled_us = preserve_trx_monotonic_us();
if (preserve_trx_monotonic_deadline_expired_at(phase1_deadline_us,
                                               sampled_us)) {
  return Preserve_trx_phase1_readiness_result::DEADLINE;
}
```

不增加 helper、状态、指标、callback、sysvar 或测试专用生产接口。

- [ ] **步骤 2：让轮询阶段遵循相同的优先级**

在现有轮询中，RESET 和 owner-kill 仍保持最高优先级。把 monotonic deadline
检查移动到可选 active-binlog progress 和 offender identity 扫描之前：

```cpp
const ulonglong now_us = preserve_trx_monotonic_us();
if (preserve_trx_monotonic_deadline_expired_at(phase1_deadline_us,
                                               now_us)) {
  return Preserve_trx_phase1_readiness_result::DEADLINE;
}
if (progress && progress())
  return Preserve_trx_phase1_readiness_result::PROGRESS_FAILED;
```

这样 deadline 已经可观察时，就不会再多执行一次 progress/send 操作或
一次 O(N) offender 遍历。

- [ ] **步骤 3：澄清现有 sysvar 帮助文本**

保持参数名称、类型、范围、默认值和存储位置不变，只调整说明：

```cpp
"Classic-protocol operation timeout in milliseconds for source "
"standby-transfer phase 1. The same value is the absolute phase-1 "
"readiness deadline; after it expires readiness stops observing commands "
"and proceeds to WARMCOPY_CLOSING."
```

对应中文语义为：

> 该参数既是 source standby-transfer Phase1 的 classic protocol 操作超时，
> 也是 Phase1 readiness 的绝对 deadline。deadline 到期后，readiness 停止
> 观察 command，并继续进入 `WARMCOPY_CLOSING`。

### 任务 3：验证行为和隔离性

**涉及文件：** 不增加其他文件。

- [ ] **步骤 1：构建受影响的服务端目标**

```bash
cmake --build build-debug --target mysqld -j8
```

预期结果：构建成功。

- [ ] **步骤 2：运行 readiness MTR**

```bash
cd build-debug/mysql-test
perl mysql-test-run.pl \
  --suite=preserve_trx \
  --do-test=batch_drain_phase1_readiness \
  --mysqld=--log-bin=mysql-bin \
  --force
```

预期结果：PASS。

- [ ] **步骤 3：运行相邻的 CLOSING timeout 用例**

```bash
cd build-debug/mysql-test
perl mysql-test-run.pl \
  --suite=preserve_trx \
  --do-test='batch_drain_closing_command_timeout|batch_drain_closing_timeout_abort_failure|batch_drain_closing_timeout_all_excluded|batch_drain_closing_timeout_exclusion' \
  --mysqld=--log-bin=mysql-bin \
  --force
```

预期结果：所有选中的用例 PASS，证明 Phase2 参数 A/B 的语义没有变化。

- [ ] **步骤 4：检查补丁卫生**

```bash
git diff --check
git diff --stat
git status --short -uall
```

预期结果：没有空白格式错误。除已有的无关未跟踪文件外，只修改计划中的
4 个 tracked 文件。

## 4. 验收标准

- `preserve_trx_transfer_phase1_timeout_ms` 已到期时，readiness 直接返回
  `DEADLINE`，不再扫描 THD、不执行可选 progress callback，也不进入轮询休眠。
- 现有调用方收到 `DEADLINE` 后继续进入 `WARMCOPY_CLOSING`。
- RESET 和 owner kill 仍优先于 deadline。
- Phase1 participant、artifact 或 transfer 失败仍然中止 DRAIN。
- CLOSING 前已接受的 command 仍由
  `preserve_trx_drain_closing_command_timeout_ms` 和参数 A 管理。
- `preserve_trx_enable=OFF` 的行为不变，因为修改的 helper 只会从已经激活的
  两阶段 Preserve DRAIN 路径调用。
- 不增加测试专用生产接口，也不增加新的用户参数。

## 5. Git 纪律

执行本计划时不运行 `git add`、`git commit` 或 `git push`。4 个文件的补丁
保留在 working tree 中供审核。只有用户明确批准实现和测试证据后，才考虑提交。
