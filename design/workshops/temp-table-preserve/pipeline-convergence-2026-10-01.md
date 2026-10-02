# PS 流水线第二轮收敛实施与验证

> **2026-10-04 版本说明：历史版本记录。** 文内“当前/尚未完成/通过”和源码路径均属于记录时点；旧 PS 定义/参数/重建方案不再适用，原失败与测量不改写为新版本成绩。 现行职责见[详细设计](detailed-design.md)和[PS 删除与 cursor 关联](ps-transfer-removal-and-cursor-attach.md)，最新状态见[任务跟踪 E63/E64](task-tracker.md)。下文保留历史事实，不作为现行实施清单。

用户已批准本轮方案。工作目录、分支不变，不提交；主 agent 实施，subagent 只读审查。
沿用已批准的设计与执行计划流程，验证只用 MTR/Python E2E，无新增 UT/DEBUG_SYNC。

目标：裁剪 receiver 重复解码、源 owner 重复扫描与投递偏置、compact runtime 临时对象。
保持完整命令边界、原序号/ACK/认证语义、PS ID/代次/参数/结果状态、原预算和线程池。
此轮不修改全局 transport 锁、TDC 锁、TEMP 安装流程或 descriptor 磁盘协议。

## 1. receiver 一次解码

- [x] 在既有 transfer codec 内复用一次完整 batch/frame 认证的结果。公共 encoded API 保持行为，
  在线 dispatch 用本次请求拥有的 decoded frames 完成身份核验及准入/应用；worker join 后才释放。
- [x] 内部 batch decoder 可选择输出 encoded 或 decoded frames，复用同一验证循环。
  handler 的 decoded 核心仍执行原序号、准入、apply、失败收敛。保留 canonical encode+SHA 摘要语义。
- [x] Debug 仅观察每个真实 dispatch 的 batch/frame 解码次数。先用旧逻辑跑同一真实多帧传输，
  断言 batch=1、frame=原帧数得到 RED；修后 GREEN。覆盖 lost-ACK 重试和输入损坏拒绝。
- [x] 修改 `sql/preserve_trx_transfer.cc`，用例位于既有 `ps_phase1` MTR/Python 驱动。

## 2. source owner 调度

- [x] 在 `sql/preserve_trx_temp_prebuild.cc` 内将 compact 刷新与 admission 分开，保留有限 token
  轮转位置；NO_SLOT/NO_CREDIT 不再阻止后部 completed 状态传播。
- [x] 活跃/initial pending 统计在一次刷新或状态失效后重建，capture 对单个 Entry 作差量更新，
  消除每个目标一次全表统计。每 token single-flight、incarnation/generation 和家族保留额度不变。
- [x] 用前序大 descriptor 与后序小 descriptor 验证小任务在大任务 seal 前得到传输机会，
  配合真实 ACK gate 和紧 slot，保留旧行为 RED、最终全部 READY。补生命周期与公平性回归。

## 3. compact runtime

- [x] 在现有 `sql/preserve_trx_ps_runtime.{h,cc}` 提供严格无值格式的无分配校验；普通完整格式不变。
  复用它检查实际类型、转换、charset、flags、position、空字符串/view、LONG_DATA 和 arena，
  完整成功之后才移交/覆盖现有 image，不以长度代替认证。
- [x] `preserve_trx_ps_restore.cc` 与 `preserve_trx_ps_runtime_delta.cc` 的 compact 分支直接校验字节、
  保留 image 和 arena，避免分配完整 Value 数组再销毁。非空参数/cursor 保留完整解码。
- [x] Debug 观察通用 runtime 解码次数，旧逻辑在普通 compact 场景得到 RED，修后不增加；
  再覆盖 first-execute/actual type/unsigned/reset/error、冷/热delta和SQL RESUME。

## 验证与结果

- [x] 保存修改前源码、构建包含观察的旧逻辑，运行上述 RED。
- [x] `cmake --build build-debug --target mysqld -j8`；独立 vardir、log-bin 定向 MTR。
- [x] 独立审查内存额度、decoded 帧所有权、坏包/重试、轮转与状态失效后复验。
- [x] Release 构建与同参数读写复测，报告已测范围，不由小规模推导500/1000验收。
- [x] 更新本记录及 task-tracker；保留所有有效失败证据，不通过放宽门槛转绿。

修改前源码与日志根目录：`build-debug/ps-pipeline-convergence-20261001/`。

## 已实现的边界与证据

```mermaid
flowchart LR
    A[命令边界捕获] --> B[owner统计差量更新]
    B --> C[刷新全体完成状态]
    C --> D[按token轮转投递]
    D --> E[既有发送线程与额度]
    E --> F[receiver一次完整解码]
    F --> G[原顺序身份校验与原准入]
    G --> H[空runtime字节校验并复用image]
    H --> I[既有READY候选]
```

- receiver 的 decoded frames 仅由本次 dispatch 持有，内存 lease 到 apply workers 全部 join
  后才释放。外层 payload 用原报文视图；3倍报文字节及 frame 容器额度覆盖解码峰值。
  通用 encoded API 保持逐帧消费，不能为复用校验而另外积攒一份整批对象。
- 认证、原始顺序的 epoch/nonce/连续序号、canonical 摘要、原报文 ACK 和重试语义不变。
  复用发生在校验成功之后，重传的新请求仍重新完整校验。
- source 的统计只由原 drain owner 修改；worker 写入的字段不影响统计谓词。
  capture 在锁内作单 Entry 差量更新；consume/abandon/收尾使缓存失效，下一次重建。
  submit 先刷新状态再准入，NO_SLOT/NO_CREDIT 保留当前 token，下一次从它继续。
- compact 路径校验完整 header、SQL摘要、参数位置、类型、转换、状态、flags、精度、charset、
  空字符串/view 和 EOF，然后移动或覆盖原 image。非空参数、开放 cursor 保持完整解码。
  NULL/空字符串等非空 state 回退前不新增一次 SQL SHA。冷热 delta 都在修改前校验。

### 正确性验收

- 三条同观察条件的旧实现 RED：batch=2且frame=4N；compact出现full runtime decode；
  单slot时小owner的data序号66晚于大owner的seal序号65。环境日志路径错误不计RED。
- 修后 `ps_phase1_decode_once`、`ps_phase1_compact_decode`、`ps_phase1_round_robin` 全过；
  最终构建再加 `ps_phase1_runtime_cold` 四条全过。Debug/Release最终构建退出码均0。
- 59条不同业务回归全过，覆盖PS换代/关闭/断连/延期捕获、冷热delta、ACK丢失与损坏、
  SQL RESUME后的原ID和无重绑EXECUTE、unsigned、RESET、失败命令、游标及OFF；shutdown另计。
- `temp_id_contract` 在显式skip-log-bin下通过：外层合法但内层CRC/摘要错误、epoch/nonce/
  序号gap错误都拒绝；之后同epoch正常序号及重放ACK仍成功。首次默认binlog导致跳过，不计通过。
- 新 `ps_compact_validation` 是MTR驱动的内部资源桥验证：8条ordinary PS及游标，14种损坏
  runtime先重算外层摘要，再到实际Preparation；13种直接被新validator拒绝，string长度错误
  回退full decoder后拒绝。失败同时核对preparation错误文本，额度和游标位置保持，随后正常迁移/执行/RESET/FETCH/CLOSE。
  该用例不算512 PS容量、合法非空参数、恶意delta或真实物理升主验收；512档由既有compact用例覆盖。
- 新增4个MTR用例；没有UT/GUnit或DEBUG_SYNC。独立source与receiver审查已完成，发现的C++14
  兼容、整批副本、解码峰值计费问题已修正并复审。

关键日志位于 `build-debug/ps-pipeline-convergence-20261001/`：
`red.log`、`red2.log`、`green.log`、`regression.log`、`input-nobin.log`、`final.log`、
`compact-validation-final.log`。合计61个不同业务用例通过，清单为`passed-business-tests.json`。
已结束的本轮MTR数据目录已按需清理；日志与有效RED证据保留，清理清单为`cleaned-data.json`。

### Release同参数对照

执行顺序为旧→新→新→旧。均为64连接、每连接1153个PS（合计73792）、128表×20000行，
60秒稳态读写，原2GiB Preserve额度、6 worker、10秒Phase1上限；业务持续到DRAIN完成，
原连接HOLD到receiver READY后才关闭sysbench。没有增大预算、放宽门槛或增加线程池。
四轮均64 SURVIVOR/READY/HOLD、0 NOT_READY、源/目标复用64、fallback0；描述payload均
37,544,000字节。42项冻结输入前后核验一致，两个Release二进制SHA分别为：

- 旧：`e3352aceb11152b228c21694dcd1d4e19fdd225e6fbc9d885a5de61708153732`。
- 新：`424e09ba3b8ee0aba637f5e5b71544e773daa282b966bb1f410248ae188ab994`。

| 轮次 | Phase1 秒 | strict Phase2 毫秒 | 末命令→ACK 毫秒 | ACK→READY 毫秒 | 源额度峰值 MiB | receiver额度峰值 MiB | 稳态 TPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| pipe64-before-r1 | 2.000876 | 45.889 | 20.232 | 0.913 | 140.849 | 219.090 | 2995.319 |
| pipe64-after-r1 | 0.750396 | 28.274 | 24.610 | 0.898 | 133.196 | 219.189 | 3486.591 |
| pipe64-after-r2 | 0.918799 | 22.133 | 20.944 | 5.353 | 140.766 | 220.110 | 3000.280 |
| pipe64-before-r3 | 1.623190 | 30.860 | 30.104 | 0.400 | 140.769 | 219.611 | 3309.565 |

本机这两组对照均观察到Phase1缩短，配对降幅约62.5%、43.4%；strict也较短。
这不是500/1000连接验收，不能由此承诺大规模2秒。原始eligible BODY数各不相同
（旧1=3、新1=41、新2=8、旧2=4）；末命令→ACK是完整收尾区间，不是纯网络ACK。
READY尾部未稳定改善（新版第二轮5.353ms），整体内存峰值也未证明稳定下降。
owner capture累计耗时并未下降，不应将Phase1变化解释为所有环节都变快；本轮没有拆分
三项优化各自的耗时贡献。稳态TPS存在波动，不将其单独归因此补丁。

`pipe64-before-r2`在启动前被端口占用预检拒绝，未开始业务，不计性能样本；独立端口的
`before-r3`补齐第二组。检查时无监听进程，未进一步归因具体套接字状态，也未停止无关进程。
全部有效轮由既有驱动在指标采集完成后清理实例和datadir；source的记录内SIGKILL属于
这一步结束测试，不是在业务期间异常退出。失败与清理证据均保留。

完整报告、逐秒状态和对照JSON在 `build-release/ps-pipeline-convergence-20261001/`。
本轮三项优化已落地；TDC依赖检查、TEMP安装BUSY路径、descriptor文件读取与全局transport锁
仍是分开的后续分析项。原500/1000规模、完整回归矩阵及V03/外部集成验收继续开放。未提交。
