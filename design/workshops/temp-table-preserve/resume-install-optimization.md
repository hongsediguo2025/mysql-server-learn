# READY、SQL RESUME 与 cursor 关联的成本边界

2026-10-04。旧 PS factory、参数安装、statement map 扩容和 PS quota 优化已随本工程 PS transfer 删除退出当前方案；其历史成本不能继续当成当前 RESUME 必做工作。

## 1. 把数据量相关的工作提前完成

```mermaid
flowchart LR
    A[Phase 1 接收工件] --> B[READY 前<br/>TEMP 原生资源 / 值校验 / decoder / sender / 位置]
    B --> C[既有在线升主接管]
    C --> D[SQL RESUME<br/>journal 移交资源 owner]
    D --> E[外部 PS 回放]
    E --> F[逐条 cursor attach]
    F --> G[CLOSE 处理后继续业务]
```

receiver 沿原 worker 和候选缓存提前准备 TEMP 与结果，不能只把文件接收完成当作 READY。全结果扫描、目标 DATA/undo 转换和相关元数据准备不延迟至 SQL RESUME、attach 或第一次 FETCH。

## 2. 当前 RESUME 做什么

[主流程](../../../sql/preserve_trx.cc) 通过已有 lease 和 journal 接管事务、临时表和 [result Ready owner](../../../sql/preserve_trx_result_restore.h)。结果 token/清单摘要须与本次 prepared 资源一致。

既有 activation 不可回退边界前失败可按原协议归还同一个 Ready owner；`begin_activation()` 成功后，即使结果 journal 尚未 commit，也不能归还 lease 重试，按实际安装阶段回滚、终止或 taint。成功 `finish()` 将 owner 放入 THD，保留 pending 数量。

此处不创建 PS，不安装参数，不更新 PS map/ID/quota，不重建解析树。外部 PS 回放在 RESUME 成功之后进行，其自身成本必须单独测量，不能因为移出本工程就当作端到端耗时为零。

## 3. attach 的实际成本和并发条件

`preserve_trx_attach_cursor_after_ps_replay()` 在目标 owner 线程执行，核对真实 THD、PS map 对象和源 ID；对按 ID 排序的结果集合二分查找。结果已准备好，首次挂接不全文件散列/扫描、不执行 SELECT。

bind 仍可能为协议 bitmap 分配内存，存在失败路径；成功后才移动 cursor。不能承诺零分配、零锁或固定微秒。已经关联且仍开放的同一 imported cursor 可返回 ALREADY_ATTACHED；关闭或替换后不得复活。

跨 session 并行调用也必须遵守各自 THD 的独占执行边界；不能为了提高并发而共享裸 PS/cursor 指针。当前没有新的 PS 共享缓存或工作池。

pending_count 为零并不使 Ready owner 自动清空；描述记录保留用于重复调用校验。EOF 释放 decoder/file，但 PS/cursor/sender 的小对象可能继续存活至原生生命周期清理。

## 4. 需要测量的独立区间

| 阶段 | 应计入什么 |
| --- | --- |
| receiver READY | 排队、输入/值验证、目标资源准备、final 未完成计划；服务时间与墙钟分开 |
| 三个物理升主入口 | 各自调用及收尾，不向外部添加新阶段 |
| SQL RESUME | 事务/TEMP/owner 安装、journal、失败及清理 |
| 外部 PS 回放 | 对象创建、参数/上下文及原 ID 恢复；本地不能假设为零 |
| cursor attach | 查找、对象核对、bind、成功/重复/失败分开 |
| 首次及后续业务 | FETCH/DML 的真实往返、冷/热读取及 EOF/错误路径 |

原有 2 秒 strict 和独立 500 ms 尾部等门槛不放宽。控制器观察到 READY 与 receiver 同钟 ACK→READY 不可互换；无 body 或时钟未校准写 N/A/INVALID。

## 5. 当前证据边界

E63 已完成当时快照的本地 MTR（含 big-test）；随后五个压力驱动适配，部分同时为 MTR 依赖，适配后未重新跑整套。E64 已完成本轮原预算压力复测，仍有性能失败。1000 读写通过不能推导所有资源规模或大量外部 PS 回放都满足切换目标。外部实际回放/关联和在线升主性能尚未测到，详见[当前剩余工作](remaining-work.md)。
