# Standby Transfer 简化机会评审（源码复核修正版）

- 日期：2026-07-27
- 原始审查基线：`preserve_trx_alt` @ `75e3053c4f6`
- 修订复核基线：`preserve_trx_alt` @ `2acd8b9ed05c9fa8fa3a0c31cd8cf3ff2bb2b7c7`
- 范围：`sql/preserve_trx_transfer.{cc,h}` 为主，交叉 `sql/preserve_trx.cc`、`sql/preserve_trx_promotion.{cc,h}`、`sql/preserve_trx_promotion_prepared.{cc,h}`、`sql/preserve_trx_resource.cc`、`sql/sys_vars.cc`、MTR lint 与 `unittest/gunit/preserve_trx-t.cc`
- 原始基线文件规模：`sql/preserve_trx_transfer.cc` 17,325 行，`sql/preserve_trx_transfer.h` 1,734 行。原文将头文件误记为 1,441 行。
- 方法：先验证代码片段和调用点是否真实存在，再分别审核“问题定性”“性能优先级”“建议是否保持并发、所有权、资源和 fail-closed 合同”。代码事实存在，不等于删除或合并建议安全。
- 行号说明：原稿部分行号相对其声明基线系统性偏低约 5–7 行；当前 HEAD 又有后续变更。现有证据不能证明偏差来自未提交工作树、生成脚本或其它原因，本文不对成因作猜测。保留行号仅用于定位上下文，**函数名、字段名和 commit 才是权威定位依据，实施时不得按裸行号修改**。
- 总体裁决：本文包含多项真实简化机会，但原稿“17 条完全确认、0 条误报”混淆了代码事实与实施结论。经三路独立源码复核和主审交叉验证，F1、F3、F5、F6/F22、F9、F10、F13、F14、F15、F17、F18、F19、F20、F21 的原建议均需实质修正。本文是修正后的评审基线，**不是可直接执行的一次性实施计划**；每个切片仍须按末尾门禁单独设计、测试和审批。

## 结论速览

| # | 发现 | 复核 verdict | 优先级 |
|---|---|---|---|
| F1 | 同一 payload 存在多次 decode/hash/re-encode | PARTIAL：浪费成立；必须按 single/batch、连接数和 ACK 路径分别计数，优先级尚缺 profiling | P1，先测量 |
| F2 | 死函数 `preserve_trx_transfer_send_epoch_declare_object_frame` | CONFIRMED_INTERNAL_DEAD：无仓内调用、无公开声明；删除仍需编译验证 | 低优先级清理 |
| F3 | COMMIT_EPOCH 两次 sealed 检查 | 原“第二次恒 false”被推翻；须扩展 credential/protocol 设计 §4.4 的唯一 terminal CAS，不能另建状态机 | P0 正确性设计 |
| F4 | 平行自由函数发送栈；nullptr-session fallback 当前生产不可达 | PARTIAL：是当前控制流不变量，不是 API 层不可达证明 | P2 |
| F5 | receiver 本地 projection 死链 | PARTIAL：内部 projection helper/publish mutex 可删；staging-finalize mutex 必须保留；GLOBAL STATUS 另行处理 | P1 |
| F6 | committed-epoch 冷 prewarm 死链 | PARTIAL：自动 `COMMITTED_EPOCH` job 是死码；公开 promotion-prewarm API 仍是潜在外部集成面 | P1 |
| F7 | 发送骨架复制和 payload digest 重验 | PARTIAL：仅对实际携带 materialized payload 的对象成立；保留一个发送边界校验 | P1 |
| F8 | commit_epoch 重编码、双排序、digest 重算 | CONFIRMED_FACT；只能通过私有 canonical 类型/API 去重 | P1 |
| F9 | manifest 重复 validate | PARTIAL：部分调用点重复；公共 encoder 的 release 校验不得降为 `DBUG_ASSERT` | P2 |
| F10 | decode lease key 对 encoded batch 做 SHA256 | PARTIAL：开销成立；当前是 byte-operation key，不等于 semantic token；替换必须重定义聚合与 lease 生命周期 | P1/P2 |
| F11 | 编解码存在可避免拷贝 | PARTIAL：view 只能用于内部短生命周期，跨 worker/registry 边界仍需 owning value | P2 |
| F12 | Reaper 文件探测 | DOWNGRADED：正常路径先命中 registry；ABANDONING/EXPIRED 仅存在一轮并发窗口或异常残留，不是已证实长期热点 | P3 |
| F13 | registry 重复扫描与深拷贝 | PARTIAL：state-only query/第一次 COMMIT snapshot 复用可独立处理，但不能替代 F3 terminal barrier | P1 |
| F14 | frame/batch/ACK 完整性校验重叠 | 原删除 CRC 建议被推翻：CRC 与 SHA 覆盖区间不同；batch SHA 仅列为待证明候选 | P3 研究项 |
| F15 | 线/盘格式冗余 | PARTIAL：`frame_sequence` 当前仍被 public/legacy helper 读取，不能单独删；`epoch_fact.objects` 是 strict READY 输入；协议保持唯一 v1 | P3 |
| F16 | `Preserve_trx_transfer_client_ops` 仅一个生产实现 | NOT_AN_ISSUE：它是明确的测试依赖注入接缝 | 不处理 |
| F17 | `data_sessions`/`sender_workers` 与 pause 状态 | REFUTED：连接通道和发送线程是不同资源维度；sysvar→atomic 是控制面发布 | 不处理 |
| F18 | bundle 拷贝和 ready 状态分散 | PARTIAL：复制浪费成立且 `clear()` 可能保留 capacity；共享完整 bundle 方案被拒绝 | P2 |
| F19 | abort 空扫与逐 token ABORT | PARTIAL：只能在 `abort_epoch()` 已 clear 的专用上下文跳过空扫；协议合并另案 | P1/P3 |
| F20 | 多种 deadline | PARTIAL：存在两个 gate-timeout 借用点；prepare/READY/gate/resume/retention 生命周期不能合并 | P2 |
| F21 | phase1 采样 vector 与查询 sort | PARTIAL：reset 位于 drain 独占准入前，先修指标正确性，再评估统计结构性能 | P1/P3 |
| F22 | 每 token 重读同一 epoch.fact | PARTIAL：自动 job 不可达，但公开 plural promotion-prewarm API 仍可能产生 N 次读取 | P2，独立于 F6 |
| ~~F23~~ | ~~限速参数每 chunk 重算~~ | PARTIALLY → **撤销**（纳秒级、动态调速语义所需） | — |

---

## P1-1（F1）帧 payload 重复解码/哈希：真实浪费，优先级待测量

### 代码事实：源端次数依发送形态而变

下列清点描述的是启用 batch 且 `data_sessions > 1`、随后执行 ACK 校验的较重路径；单连接、单帧和 terminal frame 不会经过完全相同的次数。

**第 1 次（encode 期哈希，必要）**：`preserve_trx_encode_frame`（preserve_trx_transfer.cc:6777-6824）计算 payload SHA256（6791）+ control CRC32（6817-6819）。

**第 2 次（batch 组包自检，冗余）**：`encode_frame_batch_with_limit`（6920-6966）对**本进程刚编码的每帧**再完整 decode 进 `ignored`：

```cpp
// preserve_trx_transfer.cc:6933-6937
for (const std::string &encoded_frame : encoded_frames) {
  Preserve_trx_transfer_frame ignored;
  const Preserve_trx_transfer_status frame_status =
      preserve_trx_transfer_decode_frame(encoded_frame, &ignored);
  if (frame_status != Preserve_trx_transfer_status::OK) return frame_status;
```

每次 `decode_frame` 内含 CRC32 校验（6881-6884）、payload SHA256 校验（6895）、`validate_frame_components`（6905）。

**第 3、4 次（连接路由，冗余）**：`Preserve_trx_transfer_client_frame_sink::connection_index_for_frame`（2642-2674）在 `data_sessions>1` 时，先 `decode_frame_batch`（其内部 7100-7103 对每帧完整 decode 一遍），再逐帧 decode 第二遍——全部只为取 `first_token % m_connections.size()`：

```cpp
// preserve_trx_transfer.cc:2649-2660
} else if (preserve_trx_transfer_decode_frame_batch(encoded_frame,
                                                    &encoded_frames) != ...) {
  return 0;
}
uint64_t first_token = 0;
for (const std::string &frame_bytes : encoded_frames) {
  Preserve_trx_transfer_frame frame;
  if (preserve_trx_transfer_decode_frame(frame_bytes, &frame) != ...)
    return 0;
```

**第 5、6 次（ack 校验，冗余）**：`verify_frame_ack`（7273-7350）→ `transfer_payload_identity`（7167-7203）对 batch 先 `decode_frame_batch`（7177，内部每帧 decode）+ 7185-7188 循环再逐帧 decode；7328 再 `sha256_digest(encoded_payload)` 整段哈希：

```cpp
// preserve_trx_transfer.cc:7176-7189（transfer_payload_identity）
if (transfer_frame_batch_magic_matches(encoded_payload)) {
  const Preserve_trx_transfer_status status =
      preserve_trx_transfer_decode_frame_batch(encoded_payload, &encoded_frames);
  ...
}
for (const std::string &encoded_frame : encoded_frames) {
  Preserve_trx_transfer_frame frame;
  const Preserve_trx_transfer_status status =
      preserve_trx_transfer_decode_frame(encoded_frame, &frame);
```

**为什么存在优化机会**：decode 出的 epoch_id/sequence/token 在编码期全部已知——发送侧 `frames` 结构体就在 `send_token_objects_batch` 作用域内，但 `send_encoded_frame(std::string)` 接口只带字节，把结构化信息丢弃后又在连接路由和 ACK 校验中反解出来。需要注意：连接路由的额外 decode 只在 `data_sessions > 1` 时发生，单帧、batch 和 terminal frame 的次数也不同，因此不能用一个固定“每字节 6 次 SHA256”概括所有生产请求。

### 代码事实：接收端存在路径相关的重复 decode 与 re-encode

原稿把 batch-only decode 混入“单帧逐次清点”，不能用固定次数描述接收端。正确拆分如下：

- 公共 dispatch/identity 层会先解出 epoch/sequence/type；`validate_online_payload_identity()` 和 ACK 构造又调用 `transfer_payload_identity()`，因此 single 与 batch 都存在重复 identity decode；
- 普通单帧进入 `preserve_trx_transfer_handle_receiver_payload_batch()` 时，只复制进 `expanded_encoded_frames`，随后在 16468-16473 做一次 frame decode，**不会**调用 `decode_frame_batch()`；
- 只有命中 batch magic 时，16456-16459 才调用 `decode_frame_batch()`；该 decoder 在 7100-7103 已校验每个 inner frame，返回后 16468-16473 又逐帧 decode，因此 batch 路径额外存在一轮 inner-frame decode；
- ACK 构造和 ACK 验证都会再计算 payload identity，并对完整 encoded payload 求 request digest。

因此性能证据必须按 `single/batch × data_sessions=1/>1 × terminal/non-terminal` 输出 decode 次数、SHA bytes 与 copy bytes，不能再用“接收端每帧固定第 1..N 次”作为事实。

**最典型的一处**：`pre_admit_receiver_batch_sequence`（16211-16277）对刚 decode 完的帧 **re-encode 再 hash**，而原始编码字节 `expanded_encoded_frames`（16449-16464）就在上游调用方作用域内：

```cpp
// preserve_trx_transfer.cc:16238-16246
std::string encoded_frame;
Preserve_trx_transfer_status status =
    preserve_trx_transfer_encode_frame(frame, &encoded_frame);
if (status != Preserve_trx_transfer_status::OK) return status;
...
status = registry->admit_frame_sequence(
    frame.epoch_id, frame.sequence, sha256_digest(encoded_frame),
    &admission);
```

复核确认：re-encode 是 frame 字段的纯函数，decode→encode 往返字节一致，digest 等价；唯一注意点是 16475 对 `frames` 做了 `stable_sort` 而 `expanded_encoded_frames` 未同步重排，需在 16468-16474 解码循环里顺手算 digest 并随帧携带。

### 建议与风险

- **建议**：先增加 source/receiver decode 次数、hash bytes 和 copy bytes 指标或 microbenchmark，再决定是否列为 P0。实现可引入内部 owning `Decoded_frame` 三元组 `(encoded bytes, decoded frame, encoded digest)`，沿 identity/sequence/apply 路径复用；原始编码 bytes 的所有权必须覆盖 worker join。
- **边界**：公共 decoder 和发送边界仍需 fail-closed。`encode_frame_batch_with_limit` 如需跳过对本进程刚编码帧的再次 decode，应增加只接受 validated frame 的私有 helper，而不是把公共检查简单改成 release 无效的断言。
- **验收**：线格式、错误码、sequence admission digest、ACK identity 和错误日志保持不变；full-pressure A/B 必须证明 CPU 或尾延迟有可观测收益。

---

## Low（F2）内部死函数：`preserve_trx_transfer_send_epoch_declare_object_frame`

- 位置：preserve_trx_transfer.cc:9966-9992；头文件无声明（h:1417-1419 只声明 declare_token 版）。
- 代码事实：`grep -rn send_epoch_declare_object_frame sql/ unittest/ mysql-test/` 全仓唯一命中即定义行本身（9967），**连 gunit 都无调用**。
- 建议：可作为独立死码切片删除，并同步确认没有只按函数名约束其存在的 lint。风险低，但仍需编译和 targeted transfer GUnit 证明没有宏展开、函数指针或测试接缝依赖。

## P0（F3）COMMIT_EPOCH sealed 检查存在 TOCTOU，不能作为死码删除

`preserve_trx_transfer_apply_receiver_frame_internal()` 中有两次
`all_receiving_tokens_sealed()`。单线程、无 mutation 的执行中，第一次为 true
时第二次也为 true；但该结论不是并发不变量。每次检查只在 registry
mutex 下读取一次，两个调用之间没有持锁：

```cpp
// preserve_trx_transfer.cc:15757-15769
if (frame.type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH &&
    !registry->all_receiving_tokens_sealed(frame.epoch_id)) {
  ...
  return mark_status == Preserve_trx_transfer_status::OK
             ? Preserve_trx_transfer_status::CORRUPT : mark_status;
}
```

`declare_token()` 只在写 `m_records` 时持同一 mutex，没有检查 epoch 是否已经
进入 COMMIT/frozen。batch handler 虽然保证 COMMIT 等待此前 sequence apply 完成，
但 `payload_apply_tokens` 明确排除了 COMMIT frame，payload sequence gate 又在
semantic apply 前释放；不同 token 的更高 sequence mutation 仍可能并发进入。
COMMIT 只等待已经 admission 的更低 sequence apply 完成，并不自动阻止更高
sequence 的 DECLARE/BEGIN/CHUNK/SEAL。因此第二次检查不是严格不可达块。

`publish_accepted_epoch()` 在最终发布前会重新检查 token set、record state 和 sealed
状态，这能关闭一部分窗口，但还不足以作为 terminal barrier：它没有逐项比较当前
manifest descriptor、LSN 和 flags 是否仍与构造 epoch fact 时的 snapshot 相同。
同 token 的 BEGIN replacement 还可能保留 descriptor 匹配的 sealed object，不能
只靠“仍 sealed”推导 fact snapshot 未变化。现有 COMMITTED terminal status 也是在
`publish_accepted_epoch()` 后段才登记，而 ABANDON 可以更早进入清理，因此现有
terminal history 不能反向充当 COMMIT admission barrier。

- **禁止的简化**：不得直接删除 switch 内第二次检查。
- **临时最小化**：如果仅删除一次检查，应保留更靠近 records snapshot 的第二次检查；但这仍未关闭第二次检查之后的窗口，不作为最终方案。
- **正确方案**：把 COMMIT admission 与 epoch terminal 状态机放在同一 registry
  临界区。权威终态合同必须复用
  `preserve-resume-8.0.22-standby-transfer-runtime-credential-protocol-consolidation-design.md`
  §4.4 的单 epoch terminal CAS 表，以及现有 receiver registry/
  terminal tombstone；不得新增第二套 terminal registry、CAS 表或状态真值。
  `COMMIT_ADMITTED/COMMITTING` 如实现，只能是该状态机内部的瞬态或
  operation lease，用来完成
  `FINAL_METADATA_ACCEPTED -> COMMITTED` 之前的 sequence 收敛和 immutable
  snapshot 建立。ABANDON 继续走同一 §4.4 CAS 到
  `ABANDONING -> NOT_COMMITTED_CLEAN`。COMMIT admission 必须：
  1. 固定本次 terminal sequence，并拒绝随后到达的 mutating frame；
  2. 允许已经 admission、sequence 更低的 apply 收敛完成；
  3. 在同一临界区校验 token set、manifest descriptor/LSN/flags、sealed objects，
     并返回 authoritative immutable snapshot；
  4. COMMIT retry 只能复用同一 terminal sequence 和同一 snapshot；失败、过期、
     ABANDON 或 epoch retire 必须解除/终结 terminal 状态，不能留下永久 frozen
     epoch。
- **边界**：terminal barrier 只阻断 wire data-plane mutation；不得阻止
  `mark_saved_online()` 等 receiver 内部完成态推进。对外查询和幂等 retry 仍只观察
  §4.4 冻结的稳定终态；内部瞬态不得形成新的可外部观察协议状态。
- **测试**：至少覆盖 COMMIT 后更高 sequence mutation、同 token manifest
  replacement、COMMIT/ABANDON 竞争、COMMIT retry、batch 内 COMMIT 后帧，以及
  failure/expiry/retirement cleanup。测试必须证明不会漏 token、不会部分 commit、
  不会复活已终结 epoch。

---

## P2（F4）平行"自由函数"发送栈 ~700 行，当前生产控制流不进入 fallback

### 代码事实：逐函数调用点（复核后全量 grep，排除 build-*）

| 函数（transfer.cc） | 生产调用点 | gunit 调用点 |
|---|---|---|
| `send_bundle_frames`（基线实际从 12809 开始） | 编译调用点：17160（`artifact_sink::publish_bundle` 内） | 22085, 22553, 22585, 25640, 25660, 25706 |
| `send_epoch_bundles`（基线实际从 12915 开始） | 无 | 17428, 22629, 22711, 22775 |
| `build_encoded_frame_sequence`（12476-12510） | 无 | 20198 仅 1 处 |
| `send_epoch_begin/object_frames` 与单数 `send_epoch_commit_frame`（基线 9913-10074） | 无 | 21600, 22827-22873, 24436-24442 |
| `send_epoch_declare_token_frame`（9947） | 无 | 24400 仅 1 处 |
| `build_frame_sequence`（9823-9906） | 仅被 send_bundle_frames(12852) 与 gunit-only 函数调用 | 18648, 18784, 20248, 22124 |
| `send_token_bundle`（11153-11178） | 无 | ~20 处 |
| `send_token_objects`（10932-10947） | 无 | 16633, 22913, 26864 |

注意：`send_token_objects_batch`（10949-11151）是**生产真用**（session sink 17245；12105 ← `finalize_deferred_candidate` ← preserve_trx.cc:19952），不在删除范围。

### 代码事实：唯一生产入口的劣化与不可达性

`Preserve_trx_transfer_artifact_sink::publish_bundle`（基线实际从 17124 开始，sink 类在 transfer.cc）经 `send_bundle_frames` 发送，对 DECLARE+每帧各调一次 `sink->send_encoded_frame`，生产 sink 的 `send_frame` = `default_transfer_client_send` → `simple_command(COM_PRESERVE_TRX_TRANSFER)`——**每帧一次独立 COM 往返**，无 batching、无 phase1/phase2、无节流。

复核进一步确认该 fallback **当前生产控制流不可达**：`artifact_decision==STANDBY_TRANSFER_SAVE` 只在 `BATCH_MANAGER_DELIVERY` 模式下可能（1429-1438，单语句 preserve 得 UNSUPPORTED）；batch drain 在 phase1 开头就 `open_batch_transfer_source_epoch()`，失败即 abort 整个 drain（preserve_trx.cc:17989-17991），之后每个 target 都带 session。该路径仅由 lint 契约（code_review_resumable_trx_slices_lint.test:422-425、1951-1968）强制保留并被 gunit（25846、26930、26979）覆盖。

- **裁决**：这是“编译可达、当前生产运行控制流不进入”，不是 API 层天然不可达。删除前应先把 factory 合同改为 STANDBY_TRANSFER_SAVE 必须持有 source epoch session，并以 source-shape test 锁定；随后再迁移依赖该 fallback 的 GUnit/lint。由于当前单事务 PRESERVE 不作为上线能力，不应为其新造单-token epoch 生产路径。

## P1（F5）receiver 本地 projection 内部死链；可观察接口另行处理

### 代码事实

```cpp
// preserve_trx_transfer.cc:401-405
static constexpr size_t kReceiverProjectionLockShardCount = 4096;
static std::array<std::mutex, kReceiverProjectionLockShardCount>
    g_receiver_standby_publish_mutexes;
```

`publish_receiver_standby_bundle_projection()`（6633-6689）全仓（含 gunit）**零调用**——唯一命中是定义与 lint 引用。其 helper 族（`receiver_standby_projection_exists` 6554、`receiver_standby_projection_mutex` 6567、`ensure_receiver_standby_projection_key_ready` 6590、`mark_receiver_projection_external_blobs_prebuilt` 6609、`receiver_standby_projection_write_options` 6543、`note_receiver_projection_publish_us` 5314）的全部调用点都在 6640-6687 即该死函数内部，**整族连带死亡**。

lint 的自相矛盾（code_review_resumable_trx_slices_lint.test）：

```perl
# lint.test:375-376 —— 禁止 staged 路径调用它
die "online standby receiver staged-token READY must not publish local carrier projection\n"
  if $receiver_staged_prewarm_job =~ /publish_receiver_standby_bundle_projection/s;
# lint.test:393-399 —— 又强制它必须存在且用 4096 分片锁+lock-wait 指标
die "standby receiver projection must use token-sharded locks and lock-wait metrics\n"
  ...
```

- **可删除**：`publish_receiver_standby_bundle_projection()`、仅被它调用的内部
  helper、`g_receiver_standby_publish_mutexes` 及只约束该死链实现形状的 lint。
- **不能在同一切片删除**：projection 计数器已经注册为 GLOBAL STATUS，并被 E2E
  脚本/测试读取。即使当前长期为零，它们仍是可观察接口；应选择“保留零值兼容”
  或另开 coordinated interface-removal 切片，同步 SHOW、脚本、测试和文档。
- **必须保留**：`g_receiver_staging_finalize_mutexes` 和 `receiver_staging_finalize_mutex()`。它们仍在 `finalize_receiver_ready_token_staging()` 中序列化 staging cleanup 与 `mark_saved_online()`，并不是 projection 死链的一部分。
- **边界**：当前产品不支持 receiver 重启后恢复本 epoch，允许删除 process-local projection fallback；不得顺手删除 staging/finalize、commit marker 或 strict registry 的在线生命周期逻辑。

## P1（F6）死链：committed-epoch 冷 prewarm（~400 行）

### 代码事实

`enqueue_receiver_committed_epoch_prewarm`（15185-15196）全仓唯一入队点即自身定义，`Receiver_prewarm_job_kind::COMMITTED_EPOCH` 的 `job.kind=` 赋值全仓只此一处（15191），故 dispatch 分支（14773-14774/14796）、去重检查（15037）、`run_receiver_committed_epoch_prewarm_job`（14585-14660）全部不可达：

```cpp
// preserve_trx_transfer.cc:15185-15196（无任何调用者，头文件亦无声明）
Preserve_trx_transfer_status enqueue_receiver_committed_epoch_prewarm(
    const std::string &root_dir,
    const std::vector<Preserve_trx_transfer_manifest> &manifests) {
  if (manifests.empty()) return Preserve_trx_transfer_status::OK;
  g_receiver_committed_epoch_fallback_count.fetch_add(1);
  Receiver_prewarm_job job;
  job.kind = Receiver_prewarm_job_kind::COMMITTED_EPOCH;
  ...
```

自动 job 到 promotion helper 的当前仓库内部调用链也随之不可达；但
`preserved_trx_promotion_prewarm_standby_pending_token(s)` 仍声明在公开
`preserve_trx_promotion.h` 中。它们可能是物理备机外部工程的集成接口，不能仅凭
本仓库生产调用为零就按内部死码删除。必须先完成外部 API inventory 和调用方审批。

**F22 保持独立**：复数 promotion-prewarm API 逐 token 调用单 token helper时，可能
对同一 epoch.fact 做 N 次读取；当前 staged 产品路径传
`wait_for_final_epoch_fact=true`，不会命中该读取。但公开 API 仍存在，因此不能写成
“只在 GUnit 发生”或随自动 job 一并删除。若外部调用方存在，应在复数入口一次读取
fact 并向下传递 immutable snapshot。

- **建议**：在已确认“online receiver 重启后当前 epoch fail closed、由 source
  重传”的产品边界下，只删除自动 `COMMITTED_EPOCH` enqueue/job-kind/run 分支及
  只约束该自动分支的 lint/GUnit。公开 promotion-prewarm API、其行为测试和
  ready-cache/prewarm helper 保留到外部接口盘点完成。
- **风险**：不得把本项扩大成 local startup recovery 或 legacy local preserve 路径清理。

## P1（F7）发送骨架复制 5 份，materialized payload 存在 digest 重验

### 代码事实

骨架 "validate_manifest_components → encode_manifest/BEGIN → 逐 object 六项校验 → chunk 循环 → SEAL" 复制于：

1. `send_token_objects_locked`（10847-10930），digest 重验在 10896；
2. `send_token_objects_batch`（10949-11151），digest 重验在 11062；
3. `build_frame_sequence`（9823-9906），digest 重验在 9867；
4. `send_epoch_object_frames`（9994-10054），digest 重验在 10022；
5. `send_epoch_bundles`（13031）——**复核时发现的第 5 处，原报告漏计**。

对实际进入 `built_objects`、携带 materialized payload 并走发送骨架的对象，`sha256_digest(object_payload->payload) != descriptor.digest` 会重复验证构造阶段已经计算的 digest。prebuilt external blob 只把 descriptor 放入 `built_manifest.objects`，因 `continue` 不进入 `built_objects` payload vector，因此不属于这类重复 SHA。当前调用链中 build 后没有修改 payload，但 `const&` 只表达被调函数不修改，不能从类型上阻止持有 mutable 副本的上游或未来并发写入。

另外 "presealed 判定"（declared/sealed/written 三重 map 查找）也复制了 4 份：`object_presealed_for_token`（10773-10796）、`object_is_presealed` lambda（11007-11031）、`already_presealed` 块（11506-11527）、`prefix_presealed` 块（11535-11555）。

- **建议**：随 F4 只保留一个生产发送骨架；presealed 判定收口为私有 helper。digest 校验至少保留在 payload ownership 交给 sender 的边界，并让构造结果成为 immutable/owning object；只有同一对象在同一边界内的第二次校验才可删除。
- **风险**：不得依赖注释式“调用方不会修改”。需要类型/所有权合同和 mutation-negative GUnit。

## P1（F8）commit_epoch 重编码/双排序/digest 重算

### 代码事实

**(a) 重编码取 digest**：`build_epoch_fact_from_manifests`（6390-6446）对每个 manifest 重新 `encode_manifest` + `sha256_digest` 只为填 `token.manifest_digest`——而同样的编码字节在 BEGIN 帧发出时已生成过（10462/10871/11037），digest 未留存：

```cpp
// preserve_trx_transfer.cc:6421-6429
std::string encoded_manifest;
const Preserve_trx_transfer_status encode_status =
    preserve_trx_transfer_encode_manifest(manifest, &encoded_manifest);
if (encode_status != Preserve_trx_transfer_status::OK) return encode_status;
Preserve_trx_transfer_epoch_fact_token token;
token.token = manifest.token;
...
token.manifest_digest = sha256_digest(encoded_manifest);
```

**(b) 双排序**：调用方 6433-6437 刚对 `built.tokens` 排过序，`encode_epoch_fact` 内部又整 vector copy + sort：

```cpp
// preserve_trx_transfer.cc:6158-6163
std::vector<Preserve_trx_transfer_epoch_fact_token> tokens = fact.tokens;
std::sort(tokens.begin(), tokens.end(),
          [](const Preserve_trx_transfer_epoch_fact_token &left,
             const Preserve_trx_transfer_epoch_fact_token &right) {
            return left.token < right.token;
          });
```

**(c) digest 算出又重算**：`encode_epoch_fact` 内部 6237-6239 已算出 `sha256_digest(body)` 并以 hex 追加进 body，但不返回；调用方只能截取再整体 SHA256 一遍，算出的正是同一个 digest：

```cpp
// preserve_trx_transfer.cc:6442-6443
built.fact_digest = sha256_digest(
    encoded_fact.substr(0, encoded_fact.rfind("digest=")));
```

- **建议**：引入仅模块内部可构造的 canonical manifest/fact 类型，携带 validated、sorted、encoded bytes 与 digest；公共 `encode_epoch_fact` 继续接受非 canonical 输入并负责排序校验。只缓存 final descriptor generation，不能把 phase1 provisional manifest digest 复用于 final fact。
- **风险**：epoch_fact 的线格式与 digest 语义是接收端校验锚点，改动必须保持字节级一致，并补 canonical byte golden/round-trip 测试。

## P2（F9）manifest 重复 validate

### 代码事实

`preserve_trx_transfer_encode_manifest` 开头固定校验（5930-5933）：

```cpp
// preserve_trx_transfer.cc:5930-5933
const Preserve_trx_transfer_status validation_status =
    validate_manifest_components(manifest, false);
if (validation_status != Preserve_trx_transfer_status::OK)
  return validation_status;
```

而几乎每个调用点紧邻上一行已做过同一校验（中间无修改）：10444+10462、10863+10871、10979+11037、9833+9838、10555+10559、10621+10624、6414+6423。`send_bundle_frames` 路径同一 manifest 被校验 4 次（build 9795 → encode 12828 → build_frame_sequence 9833 → 其内 encode 9838）。校验是纯函数、无副作用，同一未变对象重复校验不可能得出新结论。

- **修正结论**：重复校验在部分紧邻调用点成立，但 `preserve_trx_transfer_encode_manifest()` 是公共 fail-closed API，且并非所有调用方都在编码前执行完全相同的 `validate_manifest_components()`。例如 strict preparation 做的是 eligibility/facts 检查，不等于 encoder 的完整 wire contract 校验。
- **建议**：保留公共 encoder 的 release validation。只有在显式持有 internal validated/canonical 类型时，才允许调用私有 `encode_validated_manifest()` 跳过重复校验。
- **风险**：把公共检查降为 `DBUG_ASSERT` 会使 release 构建接受非法 manifest，风险不是“极低”。

## P2（F10）decode lease identity 对整 batch 做 SHA256

### 代码事实

```cpp
// preserve_trx_transfer.cc:5793-5800
Preserve_memory_lease acquire_transfer_decode_memory_lease(
    const std::string &encoded, uint64_t bytes) {
  const auto digest = sha256_digest(encoded);
  return preserve_trx_acquire_memory_lease(
      "transfer-decode-" + bytes_to_lower_hex(digest.data(), digest.size()),
      Preserve_trx_memory_kind::SNAPSHOT_CODEC_BUFFER,
      std::max<uint64_t>(1, bytes));
}
```

调用点：6021（整 manifest）、7083（整 batch）、16444（只哈希
`encoded_frames.front()` 但按全量记账）。三处 operation identity 的构造语义并不
一致。追踪 `preserve_trx_acquire_memory_lease`
（preserve_trx_resource.cc:1183-1189）→ `acquire/release_memory_locked`
（192-249）：lease token 除记账配平外，还作为 `m_by_token`/
`m_by_token_kind` 的聚合 key；即使日志不消费该字符串，它也不是无语义名字。

- **修正结论**：整段 SHA 作为 lease key 的成本真实，但 lease token 并非纯日志
  名字。resource manager 按该字符串 key 聚合占用并执行 per-key cap。当前 key 是
  encoded bytes 的 identity，不是 semantic token：同一 token 的不同 batch 会拆成
  多个 key；receiver 外层又存在“只哈希第一帧、却按整个 batch 计费”的路径，使相同
  首帧、不同 tail 的操作可能意外聚合。
- **禁止的替换**：每次唯一的原子计数器会把同一 byte operation 的并发/嵌套 lease
  拆散，弱化 per-key 上限；指针地址还存在复用、ABA 和生命周期问题。也不能在取得
  decode lease 前先完整 decode 大 payload 来构造 semantic key。
- **建议**：先把该合同准确命名为 byte-operation identity。若 profiling 证明 SHA
  成本值得处理，再定义可从有界 inner-frame control header 无分配读取的稳定
  operation key，例如 `(receiver process generation, epoch, sequence range,
  operation kind)`；外层 batch lease 与内层 frame lease 必须使用可证明一致的
  namespace。lease 生命周期必须覆盖 owning decode 输出及 worker apply，不能在
  `string_view` 或临时 buffer 仍被引用时提前释放。无法同时满足这些条件时保留现有
  digest。

## P2（F11）帧/batch 编解码多次可避免的全量拷贝

### 代码事实

**encode_frame**（6785-6822）：先拼临时 `payload`（`append_string` 把 manifest_payload/chunk_payload/reason 各拷贝一次），算 digest 后 `out.append(payload)` 把整段再拷贝一次——payload 字节共 2 次拷贝。digest 先于 payload 落盘是格式约束，但可增量哈希避免物化临时串。

**decode_frame**（6826-6912）：整段物化 payload 后再用 `Manifest_reader::read_string`（assign 语义）把三个子串各再拷贝一次：

```cpp
// preserve_trx_transfer.cc:6891,6898-6901
const std::string payload(payload_bytes, static_cast<size_t>(payload_length));
...
Manifest_reader payload_reader(payload);
if (payload_reader.read_string(&parsed.manifest_payload) ||
    payload_reader.read_string(&parsed.chunk_payload) ||
    payload_reader.read_string(&parsed.reason) || !payload_reader.eof()) {
```

**decode_frame_batch**（7023-7113）：7068 同样整段拷贝 batch payload，7099 又把每帧各拷贝一次——同一模式。1MB 默认 chunk 下是热路径 MB 级 memcpy。

- **建议**：encode 可用预估长度 `reserve`、增量 hash 和逐段 append。decode 只能在同步、短生命周期内部使用 `Decoded_frame_view`；进入 worker queue、registry、diagnostic retention 或任何可能越过输入 buffer 生命周期的边界前必须 materialize owning value。
- **风险**：当前 batch worker 会在排序后跨线程消费 frame。把公共 frame 字段直接改成 `string_view` 可能产生悬空引用，不能定性为低风险“纯内部实现”。需 ASAN/TSAN、temporary-buffer 和 worker-lifetime 测试。

## P3（F12）Reaper 磁盘 fallback：存在，但不是正常 online bound 热路径

### 代码事实

周期链：preserve_trx.cc:11944 `wait_for(lock, std::chrono::seconds(1))` → 11947 `preserved_trx_expired_reaper_scan_once()` → 11911 → `receiver_reaper_scan_once`（transfer.cc:14002-14030）对每个 bound epoch 的每条 sealed RECEIVING record 调 `finalize_receiver_ready_token_staging`（13918-13973）。当 `query_accepted_epoch()` 不在本进程 registry（13923-13927，注意此调用点 out 参数为 nullptr，**没有**整结构拷贝——复核修正了原报告的此条描述）时，落到 `preserve_trx_transfer_epoch_committed()`：

```cpp
// preserve_trx_transfer.cc:15444-15460 —— 每次探测最多 3 次文件操作
if (!file_exists(commit_path)) return false;                    // IO #1
std::string payload;
if (read_whole_file(commit_path, &payload) != ...) return false; // IO #2
if (payload != "PTRXFER_COMMIT_V1\n" + epoch_id + "\n") return false;
Preserve_trx_transfer_epoch_fact fact;
return preserve_trx_transfer_read_epoch_fact(root_dir, epoch_id, &fact) == ...
       && fact.epoch_id == epoch_id && !fact.tokens.empty();     // IO #3：读整个 epoch fact 文件
```

正常 online bound epoch 已在进程内 accepted registry 中，`query_accepted_epoch()` 会直接命中，不进入磁盘 fallback；未 commit 的 epoch 也不在 `bound_receiver_epochs()` 的正常 reaper 集合中。最多 3 次文件操作只出现在 process-local accepted 状态缺失、但仍有兼容/残留工件的异常边界，不能描述为正常“每秒×每 sealed token”成本。

`ABANDONING/EXPIRED` 值得单独说明：`query_accepted_epoch()` 对这两个 lifecycle
返回 `IO_ERROR`，但当前 reaper 在遍历 `bound_receiver_epochs()` 前，先通过
`expire_accepted_epochs_once()` 收集它们，再调用
`preserve_trx_transfer_destroy_receiver_epoch_process_local()`；后者首先执行
`purge_receiver_epoch_derived_state()`，删除
`g_receiver_ready_epoch_state` 中的 bound entry。terminal ABANDON 路径也在
`try_begin_epoch_abandon()` 成功后、清理 staging 前立即 purge。因此一个稳定停留在
`ABANDONING` 的 epoch 不会按“每秒 × 每 token”长期重复 fallback。

仍有一个窄并发窗口：epoch 可能在本轮 `expire_accepted_epochs_once()` 已完成后、
`bound_receiver_epochs()` 扫描前转为 `ABANDONING/EXPIRED`，从而在这一轮触发
fallback；下一轮会先 purge。异常中断或未来改变 purge 顺序也可能扩大窗口，但当前
源码不能证明它是长期 COMMIT_UNKNOWN 热点。

- **建议**：先增加 fallback hit count、file-read bytes，并按 normal、
  `ABANDONING/EXPIRED` transition 和 residual-artifact 分类；故障注入应覆盖状态在
  reaper 两阶段之间切换。若当前 online/no-restart 产品边界证明长期命中为零，再删除
  或隔离该兼容 fallback；否则只缓存 per-epoch 结论，不要按 token 重读。
- **风险**：该路径承担异常清理/旧工件兼容语义，不能仅凭静态 IO 次数删除。

## P1（F13）registry 重复全表扫描与深拷贝

### 代码事实（复核逐点确认）

- `sealed_receiving_records_for_epoch()` 在 COMMIT_EPOCH 路径调两次：15858（构建 fact）与 16047（finalize 循环）；实现 9373-9395 每次全表扫 `m_records` + `records.push_back(record)` 逐条深拷贝整条 record（含 objects vector、sealed_objects set）。16047 完全可以复用 15858 的结果。
- `registry->lookup()`（9397-9406）`*record = found->second` 深拷贝整条 record；调用方 `receiver_prewarm_job_cancelled`（14282-14285）与 `receiver_epoch_expired_or_removed`（14296-14299，单个 staged job 内调 3 次：14133/14181/14197）都只读 `record.state`。
- `status_counts()`（9431-9469 全表扫描 + cleanup_debts 扫描）被 6 个 status 函数（5843-5865）各自独立触发一次，只取一个字段。
- `begin_receive`（7539-7552）每次调用都对 `m_records` 全表求 epoch reserved_bytes；`declare_object` 的全表扫描（7639-7652）**只在 replacement 分支**（同 object_id 但 descriptor 变化，7611 进入）执行——复核限定：普通新 object declare 不扫全表。
- 附带：`begin_receive` 在已有 record 时把 `receiver_record_manifest()` 重建两遍（7512 与 7564）。

- **建议**：state-only query 是独立、低风险的小切片。当前一次 COMMIT 调用内，
  15858 得到的 first snapshot 也可直接复用于 16047 的 finalize 循环；只要明确它
  表达“本次 COMMIT 构造使用的 snapshot”，该去重本身不依赖 F3 terminal barrier，
  也不改变当前语义。
- **边界补充**：复用 first snapshot 只消除一次扫描/深拷贝，**不能解决 F3**。最终
  正确性仍要求 terminal admission 后产生 authoritative snapshot，并阻止后续 wire
  mutation；不能把“复用了旧 snapshot”误写成 epoch 已冻结。
- **边界**：`staging_finalize_mutex` 是 per-token cleanup 锁，不能证明全 epoch 在两次扫描之间没有 DECLARE/BEGIN mutation。增量 counters 需要所有 terminal/cleanup/retry 路径共同维护，并以全表重算 GUnit 做差分校验。

---

## 不处理（F16）`Preserve_trx_transfer_client_ops` 是测试依赖注入接缝

- 代码事实：定义 transfer.h:1232-1244（connect / send_frame / set_operation_timeout / interrupt / disconnect）；生产唯一实例 `kDefault_transfer_client_ops`（transfer.cc:1932-1936）；`configured_transfer_client_ops()`（1943-1946）返回 `unit_transfer_client_ops()` 或默认；unit ops 只能经 `preserve_trx_transfer_set_client_ops_for_unit_test`（12689-12691）设置，全部调用点在 gunit（6506/6510、6600/6604）。
- **裁决**：一个生产实现并不能证明该抽象冗余。当前 vtable 明确隔离 connect/send/timeout/interrupt/disconnect，支持确定性模拟重连、ACK uncertain 和 transport failure。改成“普通函数 + override”仍需要同等数量的间接接缝，代码不会实质减少，还可能把测试状态散落成多个全局 hook。保留现状，仅在注释中明确 test seam。

## 不处理（F17）连接通道、发送线程与 pause publication 语义不同

### 代码事实

```cpp
// preserve_trx_transfer.cc:12286-12289 与 12352-12355（两处同构）
const size_t desired_payloads = std::min<size_t>(
    std::min<uint>(std::max<uint>(1, preserve_trx_transfer_sender_workers),
                   std::max<uint>(1, preserve_trx_transfer_data_sessions)),
    final_token_count);
```

`data_sessions` 在 sink 构造时决定真实 classic-protocol 连接数量，并参与 token-affine 路由；`sender_workers` 决定 phase2 payload 的发送线程数。两者在 worker 数计算处取 `min()` 是“线程不能超过可用连接通道”的容量约束，不代表两个参数语义等价。

`prewarm_paused` 的 sysvar backing value 服务 SET/SHOW，update callback 将状态发布到 worker 可无锁读取的 atomic。它们是控制面值与运行时 publication，不是两个可独立写入的事实源。

- **裁决**：本轮不合并参数、不删除 backing value。若未来产品只允许固定 `sender_workers == data_sessions`，应作为明确配置策略另案，而不是以“死参数”名义删除。

## P2（F18）同一 bundle 内存存两份；ready 信号散落 5 处

### 代码事实

同一来源：`run_receiver_staged_token_prewarm_job` 在 14145-14148 加载 `staged_bundle` 一次，随后：

1. 经 `prewarm_loaded_bundle_into_ready_cache` → promotion.cc:1813-1814 `entry.ready_bundle = bundle;`——ready cache 存**完整**拷贝（含 binlog payload、external blobs）；
2. 14191-14192 `prepare_strict_bundle_for_receiver(..., staged_bundle, ...)`：

```cpp
// preserve_trx_transfer.cc:4841-4843 —— 先全量拷贝构造，再裁剪
auto semantic_bundle = std::make_unique<Preserved_trx_bundle>(bundle);
semantic_bundle->metadata.binlog_cache_payload.clear();
semantic_bundle->external_blobs.clear();
```

"ready" 信号实际散落在 5 处独立维护的结构：receiver registry `record.state`、`g_receiver_seal_prewarm_state`（5370-5375）、`g_receiver_ready_epoch_state.ready_tokens`（5451-5461）、promotion ready cache `entry.state`（promotion.cc:1807-1811）、strict prepared registry token state（5386-5398）。`purge_receiver_epoch_derived_state()`（13723-13786）被迫逐一清 **9 个**全局结构（prewarm 队列 + seal_prewarm_state + ready_epoch_state + object_prewarm_proofs + record_lock_prepared + strict_record_lock_facts + strict_binlog_facts + ready cache + prepared registry）——状态碎片化的直接症状。

- **修正结论**：先全量复制再 clear 大字段不仅产生瞬时复制；标准
  `std::string::clear()` 不保证释放 capacity，因此大 binlog payload 的 capacity 还
  可能随 `semantic_bundle` 留在 prepared registry 中。让 ready cache 和 strict
  registry 共享完整 `Preserved_trx_bundle` 又会延长 binlog payload/external blobs
  的真实生命周期，破坏轻量化和资源预算。
- **建议**：第一刀只在当前模块直接构造一个轻量 `Preserved_trx_bundle`，逐项复制
  strict 路径必需字段，从一开始就不复制 binlog payload 和 external blobs。先测量
  peak/retained bytes，再决定是否值得引入跨模块 immutable metadata 类型；不要为
  这个局部浪费先扩大公共数据模型。五处 ready 状态对应 legacy/strict、
  object/token/epoch 的不同阶段，合并状态机属于独立迁移，不纳入本轮精简。

## P1/P3（F19）abort 空扫描与逐 token ABORT 是两个不同问题

### 代码事实

`abort_epoch`（12188-12225）先整体 clear pending 帧，随后对每个 declared token 调 `abort_token_locked`（12120-12178），后者又对同一 vector 做 `remove_if`：

```cpp
// preserve_trx_transfer.cc:12206-12210（abort_epoch 先 clear）
if (first_pending_sequence != 0) {
  m_pending_final_metadata_frames.clear();
  m_final_metadata_tokens.clear();
  m_next_sequence = first_pending_sequence;
}

// preserve_trx_transfer.cc:12163-12169（abort_token_locked 之后又逐 token 扫）
m_pending_final_metadata_frames.erase(
    std::remove_if(m_pending_final_metadata_frames.begin(),
                   m_pending_final_metadata_frames.end(),
                   [&](const Preserve_trx_transfer_frame &frame) {
                     return frame.token == transfer_token;
                   }),
```

`abort_epoch()` 的当前调用顺序可证明 clear 后的逐 token `remove_if` 扫空；但
`abort_token_locked()` 是共享 helper，单 token abort/部分失败路径调用它时，
pending vector 未必为空。不能从 `abort_epoch()` 的局部前提推导全局删除安全。
`m_finalized_manifests` 的 remove_if（12155-12161）则是真实全量扫，T 个 token × M
个 manifest 为 O(T×M)。另外 `abort_token_locked` 每 token 发一个独立 ABORT 帧
（12142-12151，一次网络往返）。

- **可直接处理**：只在 `abort_epoch()` 已 clear 的专用上下文，通过专用 helper、
  明确 flag 或拆分函数跳过 pending-frame scan；共享
  `abort_token_locked()` 的默认行为必须保留。`m_finalized_manifests` 可在不改变
  partial abort/发送失败语义的前提下做一次集合过滤。
- **另案处理**：逐 token ABORT 合并只能复用现有 frame-batch 容器，且 receiver 仍逐 token apply；不要新增 epoch-level ABORT 语义。网络收益必须先由 abort-heavy 故障指标证明。

## P1/P3（F21）phase1 指标 reset 时序错误；采样结构性能另行测量

### 代码事实

```cpp
// preserve_trx_transfer.cc:620-629 —— 每次 p50/p95 查询都 copy+全排序
static uint64_t phase1_sample_percentile(const std::vector<uint64_t> &values,
                                         uint percentile) {
  if (values.empty()) return 0;
  std::vector<uint64_t> sorted(values);
  std::sort(sorted.begin(), sorted.end());
  ...
```

4 个采样 vector（207-210：batch_bytes / batch_tokens / record_batch_tokens /
batch_linger_us）只 push_back（640-641、646、656），唯一清空点
`preserve_trx_transfer_reset_source_phase1_metrics()`（659-671）。reset 确实在每次
batch drain 开头调用，但当前调用发生在 active-drain 独占准入之前：第二个并发 DRAIN
可以先把第一个正在运行的 DRAIN 指标清零，随后才因已有 active drain 被拒绝。这不是
单纯性能问题，而是指标语义错误；vector mutex 只能保证无 C++ data race，不能保护
“本次 drain”的归属。

- **P1 正确性修复**：把 reset 移到独占 admission 成功之后、任何本次 drain 指标
  emission 之前。增加并发测试：运行中的 DRAIN 已产生 sample，第二个 DRAIN 被拒绝，
  第一个 DRAIN 的 sample/count 不得被清零。
- **P3 性能评估**：修复 reset 时序后，再报告单次 full-pressure drain 的 sample count
  与 SHOW 查询频率。只有样本数量或查询成本达到可观测量级时，才改为有界直方图或
  reservoir；max 可在线维护。release evidence 若依赖精确 percentile，必须明确近似
  误差合同。

---

## P3 协议与 deadline 研究项

### F14 完整性校验覆盖不同字节，禁止直接删除 CRC

- 帧级：payload SHA256（encode 6791 / verify 6895）+ control CRC32（6817-6819 / 6881-6884，`my_checksum`）；
- batch 级：payload SHA256（6952 / 7072）+ control CRC32（6960-6962 / 7057-7061）；
- ack 级：`sha256_digest(encoded_payload)` 整段（7245 / 7328）+ ack body CRC32（7265-7267 / 7313-7316）。

原稿“SHA256 严格强于 CRC32，因此 CRC 不提供额外保证”是错误结论：

- frame SHA 覆盖三个 length-prefixed payload string：
  `manifest_payload/chunk_payload/reason`；
- frame CRC 覆盖 protocol/type/sequence/epoch/nonce/token/object/offset/LSN
  facts/retention/fact digest/payload length/payload digest 等 control bytes；
- batch SHA 覆盖 inner-frame length table 与全部 inner-frame bytes；batch CRC 覆盖
  batch control header；
- ACK request digest 覆盖完整原始 request bytes；ACK CRC 覆盖 ACK body 自身的
  epoch/nonce/sequence/status/retention 等字段。

删除 frame/batch CRC 会失去 receiver admission 前专门覆盖 control bytes 的
checksum。结构解析只能发现格式错误，不能替代字段完整性检查；ACK request digest
发生在 request 已 admission 之后，且 apply 可能继续推进，也不能回溯替代
pre-admission 检查。batch SHA 可以列为待证明候选：只有在证明
count/length/EOF、每个 inner frame 校验与 end-to-end ACK digest 足以唯一绑定
batch，并补 mutation/property/fuzz 测试和性能数据后，才可在唯一 v1 格式中移除。
当前不批准删除 frame CRC、batch CRC、ACK CRC 或 ACK request digest。

### F15 线/盘格式冗余（四点）

1. **`manifest.frame_sequence` 不是可独立删除的死字段**（transfer.h:516；
   encode 5938；decode 5989）：当前主生产构造路径没有显式赋非零值，但
   `preserve_trx_transfer_build_frame_sequence()` 会读取该字段，并被
   `send_bundle_frames()` 这一编译可达的 public/legacy helper 链使用。它与 frame
   header `sequence` 的语义看似重叠，但字段删除必须和 helper/API contract 的删除或
   重定义一起完成，不能只改 wire struct。
2. **`epoch_fact_token.objects` 体积较大但当前不可删除**（transfer.h:523-529；encode 6199-6234）：`manifest_digest` 可以证明某个已知 manifest 未变化，却不能从 digest 反向恢复 object descriptor。strict READY/fact bind 当前直接用 `objects` 判断 record-lock/binlog 对象、校验 proof、重建 strict manifest 并执行 token readiness。除非先建立以 digest/generation 为键、生命周期覆盖 final fact bind 的权威 manifest store，否则删除 objects 会使 receiver 无法完成 strict READY。
3. **平铺 frame 恒零字段**（transfer.h:545-568；encode 6799-6813）：`trx_id_store`（24B，仅 COMMIT_EPOCH）、`terminal_fact_digest`（32B；复核修正：COMMIT_EPOCH 也允许携带，4021-4024 要求 nonce 非空时必填，非"仅 QUERY/ABANDON"）、`requested_terminal_status_retention_us`（8B，仅 OPEN_EPOCH）、`receiver_process_nonce`（每帧重复）对每帧都编解码；`validate_frame_components`（3901-3943）用 10+ 个分支强制其他类型下这些字段必须为零/空——证明它们不构成真正的扩展位。
4. **epoch_fact 文本格式 vs 其余全二进制**：文本编解码 6146-6388 约 240 行（`split_pipe_fields`/`parse_uint64_strict`/`line_has_prefix`），与二进制 manifest 编解码（5925-6078）承载同类数据，双格式无双倍收益。

本项目尚未产品化，online wire、manifest、fact 等格式继续统一称为唯一 **v1**，
不引入 v2 兼容层。允许在 source/receiver 同步升级时原位收敛 v1，但每次线格式修改
必须原子更新 encoder/decoder/golden/fuzz/MTR，旧布局与新布局确定性互拒。当前
**不批准删除任何 F15 字段**：`frame_sequence` 等待 helper/API contract 收敛，
`objects`、terminal facts 和 strict contract 字段保持。retention 语义保持：低于
60s 向上钳制，高于 300s 直接 `INVALID_ARGUMENT`。

### F20 deadline 各自保护不同生命周期，只修正两个明确误用点

- `receiver_prewarm_timeout_ms` 的 PREWARMING deadline 在 COMMIT/fact 被接受后创建，
  保护 token/object 准备窗口；READY 发布时再切换到
  `receiver_ready_timeout_ms`，保护已准备资源驻留期。这是两个状态的 deadline，
  不是同一值的重复存储；
- `preserve_trx_promotion_gate_timeout_ms` 被两个 receiver prewarm 路径误借用：
  promotion ready-cache 路径的 record-lock residency wait
  （promotion.cc:1909）以及 receiver object-prewarm 路径
  （transfer.cc:14467、14484）；
- `epoch_prepare_deadline_us` 控制 gate 前 strict prepared 资源有效期；
  `client_resume_deadline_us` 当前在 fact load/构造 entry 时按约 300s 预先计算，随后
  由 ADOPTED_LOCKED cleanup 消费；它并非在状态刚进入 ADOPTED_LOCKED 时才开始。
  terminal status retention 则是 source/receiver ownership 仲裁窗口。三者不能合并。
  retention 是下限 60s 钳制、上限 300s 直接拒绝，不是双向硬钳；
- prewarm 队列状态簿记 6 件套：inflight/done/deferred 三个 set + object inflight + `g_receiver_record_plan_deferred` + `g_receiver_record_plan_attempted_generation`（13616-13626），配合 `kReceiverRecordLockObjectProofRetryLimit=16`（413）重试循环。

**批准的最小修正方向**：建立 deadline inventory，逐项写明 start event、consumer、
适用 state 与 clock domain。两个 record-lock wait 不应借用
`preserve_trx_promotion_gate_timeout_ms`，应接收本状态已有 deadline 的
**remaining budget** 或内部 operation deadline；绝不能把一个进程/时钟域的 absolute
monotonic deadline 原样传到另一个时钟域。不得为此新增 public sysvar。
retry/deferred 多容器合并只有在能写出完整状态转移表、幂等键和 purge 合同时另案
处理。本轮不合并 PREWARMING、READY、gate、client resume 或 terminal retention
deadline。

---

## 已撤销 / 已降级项（复核结论）

| 项 | 原描述 | 复核结论 |
|---|---|---|
| ~~限速参数每 chunk 重算~~ | `throttle_source_transfer_io`（568-574）每 chunk 调 `preserve_trx_transfer_current_runtime_limits()`（444-471） | **撤销**：只是 8 个全局变量的无锁读 + 几次 switch/min/max，纳秒级；且逐 chunk 重读是"sysvar 改了立即生效"的动态调速语义所需，非实质性能问题 |
| F15 `manifest.frame_sequence` | "生产无读取点，可以直接删除" | **撤销删除结论**：public/legacy `build_frame_sequence()` 仍读取；只能随 helper/API contract 收敛一并处理 |
| F21 采样 vector | "长跑实例无界内存增长" | **改写**：长期泄漏定性不成立，但 reset 位于 active-drain admission 前，存在并发指标清零的正确性问题；修复后再评估单 drain 内存/排序成本 |
| F22 epoch.fact 重读 | "只在 GUnit 中发生，随 F6 删除" | **保持独立**：当前 staged 产品路径不读，但公开 plural promotion-prewarm API 仍可逐 token 重读；需先盘点外部调用 |

## 复核确认"不是问题"的点（避免误改）

- `Preserve_trx_transfer_phase1_batch_sender` 的 pimpl（756-997）：隐藏 `<thread>`/队列细节，语义完整，不算过度；
- `m_pending_final_metadata_frames` 延迟到 commit 才编码（10187-10190、12316-12334）：是 phase2 设计语义（final metadata 与 COMMIT 绑定），非冗余；
- `commit_epoch` 的多 worker 发送：worker 数被 `min(sender_workers, data_sessions, payload 数)` 收敛，payload 为空时零开销；这里体现线程数受连接数约束，不代表两个配置重复；
- `Preserve_trx_transfer_encoded_frame_sink` 的默认空实现 virtual（h:1013-1044）：仅服务测试 mock，可接受的测试接缝；
- 原始基线有 25 个 transfer sysvar；当前 HEAD 删除 `preserve_trx_transfer_receiver_enable` 后为 24 个。未发现完全无读取点的现存 transfer sysvar，但“有读取点”不等于产品配置面已经最优；
- epoch_fact 文本解码有 `token_count > 1000000`（6296）与 `kMaxTransferManifestObjects` 等限额，已做基本防 DoS，精简时不应削弱。

## 物理备机升主接口硬边界

物理备机工程会按固定时序调用以下两个生产接口，它们不是可清理的 test seam 或
仓内无调用 API：

1. `preserved_trx_prepare_before_trx_sys_init_for_physical_promotion()`：
   在 `trx_sys_init_at_db_start()` 前绑定当前进程 READY epoch，校验 accepted
   epoch/final fact，持有 receiver promotion lease 与 prepared-token pin，并注册
   Resurrection Index candidates。它不 claim 事务、不安装锁、不 attach THD。
2. `preserved_trx_adopt_prepared_epoch_for_physical_promotion()`：
   在 `trx_sys` 初始化完成、DD/MDL 可用且 purge/rollback/write-enable 尚未开始时，
   消费同一个 `Preserve_trx_physical_promotion_bootstrap_attempt`，解析 exact verified
   `trx_t *`，执行 strict all-or-nothing adopt；成功后才完成 receiver promotion
   lease、释放 token pin 并清理 resurrection candidates，失败则通过 attempt
   `abort()` fail closed。

所有 F1-F22 精简切片必须保持：

- 两个函数的公开声明、参数、返回状态和调用先后关系不变；
- `Preserve_trx_physical_promotion_bootstrap_attempt` 的 move-disabled RAII
  ownership、单 active attempt、abort/complete 语义不变；
- accepted epoch、final fact、source fence LSN、receiver process generation、
  strict prepared registry、token pin 和 exact verified transaction 的绑定关系不变；
- prepare 前不得提前 claim/import，adopt 成功前不得释放 lease/pin、启动
  purge/rollback 或开放写服务；
- S0 “仓内零调用”证明不得用于删除这两个接口或其传递依赖。任何公开 promotion
  API、fact 字段、registry 状态或 lifecycle cleanup 的删除，都必须先证明不在这两条
  生产调用链上，并完成物理备机工程的编译与集成验证。

最低回归门禁包括：

- `preserve_trx-t` 中 `*PhysicalBootstrap*`、`*StrictPhysicalGate*` 和
  `*PhysicalPromotion*` targeted GUnit；
- bootstrap 缺失、错误 clock domain、deadline、重复 active attempt、adopt
  success、partial failure rollback、cleanup taint 与 attempt 析构 abort；
- source-shape/ABI 检查确保两个生产 symbol 及 bootstrap-attempt 签名仍存在；
- 触及 header、accepted epoch、strict registry、fact 或 promotion lease 时，除仓内
  GUnit/MTR 外还必须由物理备机工程重新编译其调用点。

## 修订后的执行顺序与门禁

| 批次 | 内容 | 改动性质 |
|---|---|---|
| S0 内部 housekeeping | F2；F5 仅内部 projection helper/publish mutex；F6 仅自动 `COMMITTED_EPOCH` job；F19 仅 `abort_epoch()` 专用空扫 | 不删除 GLOBAL STATUS、物理升主接口及传递依赖、公开 promotion API、staging-finalize、startup/legacy 或共享 abort 行为 |
| S1 正确性 | F3 在 §4.4 唯一 terminal CAS 内增加 COMMIT-aware admission；F21 reset 移到 active-drain admission 成功后 | 两项分别先写并发 RED；F3 不新增 registry/外部状态，F21 不能包装成“性能精简” |
| S2 测量与接口盘点 | F1 路径矩阵 profiling；F12 normal/ABANDONING/EXPIRED fallback hit 与 file bytes；F21 性能阶段 sample count；F22 外部 API/read-count 盘点 | 本门禁仅约束 F1/F12、F21 后续性能改造及 F22 删除，不阻塞 S3 |
| S3 局部去重 | F7 单一发送边界、F8 private canonical 类型、F9 private validated encoder、F11 内部 view、F13 state-only query + first snapshot 复用 | 可独立实施；门禁是公共 fail-closed API、owning lifetime 和 byte-identical wire 不变，且 F13 不替代 F3 |
| S4 内存 | F18 直接构造轻量 `Preserved_trx_bundle` | 先测 retained capacity；不先引入跨模块类型，不共享完整 bundle，不合并 strict/legacy 状态机 |
| S5 协议研究 | F14 仅研究 batch SHA；F15 先收敛 helper/API contract，再评估字段/layout | 唯一 v1 原子变更；property/fuzz/golden 前不得删 CRC、ACK digest、`frame_sequence` 或 live fact objects |
| S6 deadline 修正 | F20 两个 gate-timeout 借用点改用本状态 remaining budget | 先冻结 start/consumer/state/clock-domain 表；不新增 sysvar、不合并各生命周期 |
| 不实施 | F16、F17；跨生命周期 deadline 合并；公开 API/STATUS 未盘点前的删除 | 当前证据不支持 |

### 每个切片的统一审核要求

1. 修改前重新按函数名和当前 commit 定位，不使用本文旧行号。
2. 明确列出 touched files、OFF-path 隔离、并发/所有权/内存预算影响。
3. 先完成外部接口和 GLOBAL STATUS inventory；本仓库零调用不等于外部物理备机
   工程零调用。
4. 先写能复现当前问题的 targeted RED；纯内部死码需以全仓零调用、无公开声明和
   source-shape 证明替代。
5. F3 必须以 credential/protocol 设计 §4.4 为唯一终态合同，覆盖
   COMMIT/ABANDON、retry、higher-sequence、replacement 和 cleanup；F21 必须覆盖
   并发 rejected DRAIN 不清零 live drain 指标。
6. 删除 process-local fallback 后，仍需覆盖 receiver 在线正常链路、receiver 重启后
   当前 epoch 明确 fail closed、OFF command/startup、promotion take/restore；lint
   不能替代运行时行为。
7. 修改后重新审查：是否真正减少调用/拷贝/内存，是否改变 wire bytes、错误码、
   sequence、ACK、cleanup、READY 或 observable STATUS 合同。
8. 跑对应 GUnit/MTR；触及 online transfer 热路径时补 scaled E2E，触及
   full-pressure 指标时连续三轮 release 验证。
9. 触及 physical bootstrap/adopt 的任何传递依赖时，必须运行物理升主 targeted
   GUnit，并由外部物理备机工程重新编译调用点；仓内无直接调用不能替代该验证。
10. 本文不是“一次性大重构”授权。任何切片收益未证明或需要扩大到原生 MySQL
   8.0.22 路径时，应停止并重新设计。
