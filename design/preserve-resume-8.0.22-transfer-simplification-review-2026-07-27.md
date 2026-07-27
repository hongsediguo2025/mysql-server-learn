# Standby Transfer 简化机会评审（含代码事实与二次复核）

- 日期：2026-07-27
- 分支/HEAD：`preserve_trx_alt` @ `75e3053c4f6`
- 范围：`sql/preserve_trx_transfer.{cc,h}`（17,325 / 1,441 行）为主，交叉 `sql/preserve_trx.cc`、`sql/preserve_trx_promotion.{cc,h}`、`sql/preserve_trx_promotion_prepared.h`、`sql/sys_vars.cc`、`mysql-test/suite/preserve_trx/t/code_review_resumable_trx_slices_lint.test`、`unittest/gunit/preserve_trx-t.cc`
- 方法：三个独立方向的只读审查（源端 / 接收端 / 协议与配置面）→ 21 项合并发现 → 再由三个独立复核代理逐条 confirm/refute + 根代理对硬断言（死代码调用点、lint 断言、字段写入点、`min()` 使用点、不可达分支）亲自 grep/读码核验。本文所有行号基于上述 HEAD，所有代码片段为逐字摘录。
- 复核统计：**17 条完全确认，4 条部分成立（修正/降级），0 条误报**；复核另发现 2 处原报告漏计（骨架复制实为 5 份；abort 路径空扫）。

## 结论速览

| # | 发现 | 复核 verdict | 优先级 |
|---|---|---|---|
| F1 | 同一帧 payload 在源端被完整 decode 5 次 / 接收端 5–7 次 + re-encode 1 次，每次含 SHA256+CRC32 | CONFIRMED（原报告低估次数） | **P0** |
| F2 | 死函数 `preserve_trx_transfer_send_epoch_declare_object_frame`（全仓零调用） | CONFIRMED | **P0** |
| F3 | COMMIT_EPOCH sealed 检查第二遍恒 false（不可达块） | CONFIRMED | **P0** |
| F4 | 平行"自由函数"发送栈 ~700 行；nullptr-session fallback 生产不可达 | PARTIALLY（结论更强：fallback 不可达） | P1 |
| F5 | 死链：receiver 本地 projection 发布（~250 行 + 4096 个静态 mutex），lint 自相矛盾 | CONFIRMED | P1 |
| F6 | 死链：committed-epoch 冷 prewarm（~400 行，lint 禁止生产使用） | CONFIRMED | P1 |
| F7 | "validate→encode→逐对象重哈希→chunk→seal"骨架复制 5 份，每份含冗余全量 SHA256 | CONFIRMED（原报告漏计第 5 份） | P1 |
| F8 | commit_epoch 重编码 manifest 取 digest、双排序、fact_digest 重算 | CONFIRMED | P1 |
| F9 | manifest 重复 validate（encode 内必检 + 调用点已检） | CONFIRMED | P1 |
| F10 | lease 起名对整 batch 做 SHA256 | CONFIRMED | P1 |
| F11 | 帧/batch 编解码多次可避免的全量拷贝 | CONFIRMED | P1 |
| F12 | Reaper 每秒×每 sealed token 最多 3 次文件操作 | CONFIRMED（比原描述更重） | P1 |
| F13 | registry 重复全表扫描与深拷贝 | CONFIRMED（一处限定） | P1 |
| F14 | 三层完整性校验叠加（帧/batch/ack 均 SHA256+CRC32） | CONFIRMED | P3（协议改版） |
| F15 | 线/盘格式冗余：死字段、fact 重复携带 objects、平铺 frame 恒零字段、双格式编解码 | CONFIRMED（两处细节纠偏） | P3（协议改版） |
| F16 | `Preserve_trx_transfer_client_ops` 五函数指针抽象仅一个生产实现 | CONFIRMED | P2 |
| F17 | `data_sessions`/`sender_workers` 双 knob 恒取 min；`prewarm_paused` 双份事实源 | CONFIRMED | P2 |
| F18 | 同一 bundle 内存存两份；ready 信号散落 5 处、purge 清 9 个结构 | CONFIRMED | P2 |
| F19 | abort 路径：clear 后逐 token `remove_if` 必然扫空 + 逐 token 独立 ABORT 帧 | CONFIRMED | P2 |
| F20 | 超时/租约参数层层叠加（4 处期限概念） | CONFIRMED | P3（接口变更） |
| F21 | phase1 采样 vector 无界增长 + 每次查询全量 sort | PARTIALLY（每次 drain 开头重置，降级） | P2（降级） |
| F22 | 每 token 重读同一 epoch.fact | PARTIALLY（生产不可达，并入 F6） | 并入 F6 |
| ~~F23~~ | ~~限速参数每 chunk 重算~~ | PARTIALLY → **撤销**（纳秒级、动态调速语义所需） | — |

---

## P0-1（F1）帧 payload 重复解码/哈希：热路径最大纯浪费

### 代码事实：源端 5 次完整 decode + 6 次 SHA256

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

**为什么是无收益**：decode 出的 epoch_id/sequence/token 在编码期全部已知——发送侧 `frames` 结构体就在 `send_token_objects_batch` 作用域内（11110-11116），但 `send_encoded_frame(std::string)` 接口（2210）只带字节，把结构化信息丢弃后又在 2642/7167 反解出来。按 chunk_bytes=1MB 计，每字节在源端被 SHA256 约 6 次、memcpy 10+ 次。

### 代码事实：接收端 5–7 次 decode + 1 次 re-encode

单帧路径逐次清点（行号为 transfer.cc）：

1. dispatch 16836 `decode_frame`（identity_frame）；
2. `validate_online_payload_identity`（16658-16706）→ 16668 `transfer_payload_identity` 内 decode（第 2 次）→ 16682-16685 循环再 decode（第 3 次）；
3. dispatch 17005-17006 又 decode 一次只为读 `type`；
4. `handle_receiver_payload_batch`（16410-）16457-16459 `decode_frame_batch`（内部每帧 decode）+ 16470-16471 逐帧再 decode（第 5、6 次）；
5. ack 路径 `send_receiver_authenticated_ack`（16724）与 `build_frame_ack`（7240）各再调一次 `transfer_payload_identity`。

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

- **建议**：入口 decode 一次，把 `(encoded_frame, decoded_frame, digest(encoded_frame))` 三元组沿调用链下传；identity/nonce/sequence 校验、sequence admission、apply 全部复用；`encode_frame_batch_with_limit` 的自检降级为 `#ifndef NDEBUG` 断言。
- **风险**：去掉对"编码器自身 bug"的线上自检，由既有 GUnit round-trip 测试兜底；错误路径日志分支（16989/17046）行为需保持不变。

---

## P0-2（F2）死函数：`preserve_trx_transfer_send_epoch_declare_object_frame`

- 位置：preserve_trx_transfer.cc:9966-9992；头文件无声明（h:1417-1419 只声明 declare_token 版）。
- 代码事实：`grep -rn send_epoch_declare_object_frame sql/ unittest/ mysql-test/` 全仓唯一命中即定义行本身（9967），**连 gunit 都无调用**。
- 建议：直接删除。风险：无。

## P0-3（F3）COMMIT_EPOCH sealed 检查第二遍恒 false

`preserve_trx_transfer_apply_receiver_frame_internal()` 中，第一处检查失败即 mark corrupt + 清理并 `return`：

```cpp
// preserve_trx_transfer.cc:15757-15769
if (frame.type == Preserve_trx_transfer_frame_type::COMMIT_EPOCH &&
    !registry->all_receiving_tokens_sealed(frame.epoch_id)) {
  ...
  return mark_status == Preserve_trx_transfer_status::OK
             ? Preserve_trx_transfer_status::CORRUPT : mark_status;
}
```

switch 内第二处（15844-15856）是完全相同的检查+失败处理块；第一处不通过时已 return，第二处条件**恒为 false**，是不可达代码。且 `all_receiving_tokens_sealed()` 本身是对 `m_records` 的全表扫描（9336-9355），同一帧扫了两次。

- 建议：删除 switch 内重复块。风险：无（逻辑恒等）。

---

## P1-1（F4）平行"自由函数"发送栈 ~700 行，生产 fallback 实际不可达

### 代码事实：逐函数调用点（复核后全量 grep，排除 build-*）

| 函数（transfer.cc） | 生产调用点 | gunit 调用点 |
|---|---|---|
| `send_bundle_frames`（12804-12908） | 有：17153（artifact_sink::publish_bundle） | 22085, 22553, 22585, 25640, 25660, 25706 |
| `send_epoch_bundles`（12910-13093） | 无 | 17428, 22629, 22711, 22775 |
| `build_encoded_frame_sequence`（12476-12510） | 无 | 20198 仅 1 处 |
| `send_epoch_begin/object/commit_frames`（9908-10074） | 无 | 21600, 22827-22873, 24436-24442 |
| `send_epoch_declare_token_frame`（9947） | 无 | 24400 仅 1 处 |
| `build_frame_sequence`（9823-9906） | 仅被 send_bundle_frames(12852) 与 gunit-only 函数调用 | 18648, 18784, 20248, 22124 |
| `send_token_bundle`（11153-11178） | 无 | ~20 处 |
| `send_token_objects`（10932-10947） | 无 | 16633, 22913, 26864 |

注意：`send_token_objects_batch`（10949-11151）是**生产真用**（session sink 17245；12105 ← `finalize_deferred_candidate` ← preserve_trx.cc:19952），不在删除范围。

### 代码事实：唯一生产入口的劣化与不可达性

`Preserve_trx_transfer_artifact_sink::publish_bundle`（17117-17168，注意：sink 类在 transfer.cc，不在 preserve_trx.cc）经 `send_bundle_frames` 发送，对 DECLARE+每帧各调一次 `sink->send_encoded_frame`（12873/12891），生产 sink 的 `send_frame` = `default_transfer_client_send` → `simple_command(COM_PRESERVE_TRX_TRANSFER)`（1859-1871）——**每帧一次独立 COM 往返**，无 batching、无 phase1/phase2、无节流。

复核进一步确认该 fallback **当前生产控制流不可达**：`artifact_decision==STANDBY_TRANSFER_SAVE` 只在 `BATCH_MANAGER_DELIVERY` 模式下可能（1429-1438，单语句 preserve 得 UNSUPPORTED）；batch drain 在 phase1 开头就 `open_batch_transfer_source_epoch()`，失败即 abort 整个 drain（preserve_trx.cc:17989-17991），之后每个 target 都带 session。该路径仅由 lint 契约（code_review_resumable_trx_slices_lint.test:422-425、1951-1968）强制保留并被 gunit（25846、26930、26979）覆盖。

- 建议：非 batch drain 的 artifact 发布收口到 `Preserve_trx_transfer_source_epoch_session`（单 token epoch），随后删除整套自由函数发送 API、`Preserve_trx_transfer_artifact_sink` 及对应 lint/gunit 段落。
- 风险：合并时需保持单 token 非 drain 场景的协议序列兼容；lint 契约需同步删除。

## P1-2（F5）死链：receiver 本地 projection 发布（~250 行 + 4096 个静态 mutex）

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

- 建议：删除该函数族与对应 lint 段落（含 4096×2 个静态 mutex 与 projection 指标）。
- 风险：若未来重启用本地 projection 需从 git 历史恢复。

## P1-3（F6）死链：committed-epoch 冷 prewarm（~400 行）

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

连带 promotion.cc 的 `preserved_trx_promotion_prewarm_standby_pending_tokens`（2200-2341）生产唯一调用点是 transfer.cc:14620（死 job 内）；单 token 版（2003-2056）唯一生产调用者是复数版内部（2246）。两者另有 gunit 直接调用（9251、9364、10985、11038、11253、11272、19892），属"生产死、测试直接养"。lint.test:323-324 明确禁止生产路径使用；lint.test:2732-2746 又强制这对函数及其多 worker 结构必须存在。

**F22 并入**：该冷 prewarm 路径内 `prewarm_loaded_bundle_into_ready_cache()`（promotion.cc:1954-1958）对 epoch 内 N 个 token 各读一次同一 epoch.fact 文件（复数入口 2245-2247 逐 token 调用）。但生产 staged 路径传 `wait_for_final_epoch_fact=true`（promotion.cc:2069-2070）跳过读文件——即该重复磁盘 IO **只在 gunit 中发生**，随死链一并删除即可，不单独列性能项。

- 建议：删除 enqueue/job-kind/run 函数 + `prewarm_standby_pending_token(s)` 降级为 test-only 或删除；同步 lint.test:323-324、2732-2746 与 gunit。
- 风险：设计文档将其当"未来 fallback"提及，删除前确认无重启复用计划。

## P1-4（F7）发送骨架复制 5 份，每份含冗余全量 SHA256 重验

### 代码事实

骨架 "validate_manifest_components → encode_manifest/BEGIN → 逐 object 六项校验 → chunk 循环 → SEAL" 复制于：

1. `send_token_objects_locked`（10847-10930），digest 重验在 10896；
2. `send_token_objects_batch`（10949-11151），digest 重验在 11062；
3. `build_frame_sequence`（9823-9906），digest 重验在 9867；
4. `send_epoch_object_frames`（9994-10054），digest 重验在 10022；
5. `send_epoch_bundles`（13031）——**复核时发现的第 5 处，原报告漏计**。

每处的 `sha256_digest(object_payload->payload) != descriptor.digest` 都是冗余重验：digest 生成点与 payload 是同一份字节、此后以 const& 传递无修改窗口——snapshot 对象 9719（`sha256_digest(portable_snapshot)` 后立刻 `payload = std::move(portable_snapshot)`）、resurrection index 9758、external blob 经 `transfer_external_blob_descriptor`（3620-3621）。每个 token 的 snapshot payload 因此被多做一次全量 SHA256。

另外 "presealed 判定"（declared/sealed/written 三重 map 查找）也复制了 4 份：`object_presealed_for_token`（10773-10796）、`object_is_presealed` lambda（11007-11031）、`already_presealed` 块（11506-11527）、`prefix_presealed` 块（11535-11555）。

- 建议：随 F4 只保留 `send_token_objects_batch` 一份，其余改薄封装或删除；presealed 判定抽成一个私有 helper；digest 信任构建期结果（合并点保留一次校验）。
- 风险：删除发送时复核后，未来若在 build 与 send 之间引入 payload 变更路径将失去最后一道防线。

## P1-5（F8）commit_epoch 重编码/双排序/digest 重算

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

- 建议：BEGIN 帧编码时缓存 manifest 编码/摘要；`encode_epoch_fact` 契约改为"输入已排序"并返回 body_digest。
- 风险：epoch_fact 的线格式与 digest 语义是接收端校验锚点，改动必须保持字节级一致。

## P1-6（F9）manifest 重复 validate

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

- 建议：约定"编码前由调用点校验一次"，encode 内校验降为 `DBUG_ASSERT`；decode 路径（6071）的校验必须保留。
- 风险：极低。

## P1-7（F10）lease 起名对整 batch 做 SHA256

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

调用点：6021（整 manifest）、7083（整 batch）、16444（只哈希 `encoded_frames.front()` 但按全量记账——三处语义本就不一致，佐证名字无意义）。追踪 `preserve_trx_acquire_memory_lease`（preserve_trx_resource.cc:1183-1189）→ `acquire/release_memory_locked`（192-249）：lease token 只作为 `m_by_token`/`m_by_token_kind` 的 map key 做记账配平，无任何日志/诊断消费该名字。

- 建议：改用原子计数器或指针地址做 lease 名。
- 风险：无。

## P1-8（F11）帧/batch 编解码多次可避免的全量拷贝

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

- 建议：encode 用增量哈希+逐段写出；decode 用偏移量引用原 buffer 子串（string_view 语义）；batch 一次性 reserve 后逐帧 append。
- 风险：低，纯内部实现，线格式不变；需 round-trip 测试覆盖。

## P1-9（F12）Reaper 每秒×每 sealed token 最多 3 次文件操作

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

频率：epoch 未 commit 期间，每秒 × 每 sealed token 一次上述探测（复核确认比原描述"2 次"更重，是 3 次）。

- 建议：finalize 入口先用 registry 内存态快速失败；reaper 的 sibling-finalize 与 staged-job 完成时的 sibling-finalize（14221-14226）合并为一处。
- 风险：低；仅影响失败/边界时序的清理延迟。

## P1-10（F13）registry 重复全表扫描与深拷贝

### 代码事实（复核逐点确认）

- `sealed_receiving_records_for_epoch()` 在 COMMIT_EPOCH 路径调两次：15858（构建 fact）与 16047（finalize 循环）；实现 9373-9395 每次全表扫 `m_records` + `records.push_back(record)` 逐条深拷贝整条 record（含 objects vector、sealed_objects set）。16047 完全可以复用 15858 的结果。
- `registry->lookup()`（9397-9406）`*record = found->second` 深拷贝整条 record；调用方 `receiver_prewarm_job_cancelled`（14282-14285）与 `receiver_epoch_expired_or_removed`（14296-14299，单个 staged job 内调 3 次：14133/14181/14197）都只读 `record.state`。
- `status_counts()`（9431-9469 全表扫描 + cleanup_debts 扫描）被 6 个 status 函数（5843-5865）各自独立触发一次，只取一个字段。
- `begin_receive`（7539-7552）每次调用都对 `m_records` 全表求 epoch reserved_bytes；`declare_object` 的全表扫描（7639-7652）**只在 replacement 分支**（同 object_id 但 descriptor 变化，7611 进入）执行——复核限定：普通新 object declare 不扫全表。
- 附带：`begin_receive` 在已有 record 时把 `receiver_record_manifest()` 重建两遍（7512 与 7564）。

- 建议：COMMIT_EPOCH 分支复用同一份 records；registry 增加只读 state 查询（返回 enum 而非拷贝）；inflight bytes/tokens 改增量计数器随 `mark_terminal_locked` 统一收口维护。
- 风险：低；复用 records 时需确认 finalize 前无并发 mark（有 staging_finalize_mutex 保护）。

---

## P2-1（F16）`Preserve_trx_transfer_client_ops` 五函数指针抽象仅一个生产实现

- 代码事实：定义 transfer.h:1232-1244（connect / send_frame / set_operation_timeout / interrupt / disconnect）；生产唯一实例 `kDefault_transfer_client_ops`（transfer.cc:1932-1936）；`configured_transfer_client_ops()`（1943-1946）返回 `unit_transfer_client_ops()` 或默认；unit ops 只能经 `preserve_trx_transfer_set_client_ops_for_unit_test`（12689-12691）设置，全部调用点在 gunit（6506/6510、6600/6604）。
- 建议：收敛为普通函数 + 仅测试可见的 override 钩子，或在设计文档中明确这只是 test seam。
- 风险：低；若近期确有第二传输实现计划则保留。

## P2-2（F17）双 knob 恒取 min / `prewarm_paused` 双份事实源

### 代码事实

```cpp
// preserve_trx_transfer.cc:12286-12289 与 12352-12355（两处同构）
const size_t desired_payloads = std::min<size_t>(
    std::min<uint>(std::max<uint>(1, preserve_trx_transfer_sender_workers),
                   std::max<uint>(1, preserve_trx_transfer_data_sessions)),
    final_token_count);
```

`data_sessions` 另在 2195 仅做 `max(1, …)` 快照。两个独立 sysvar（sys_vars.cc:1519-1535）在所有真实决策点都取 min，等价于一个"有效并发度"knob。

`prewarm_paused`：sysvar 全局 `preserve_trx_transfer_prewarm_paused`（transfer.cc:95）在**服务器逻辑中零读者**（唯一读取是 sysvar 框架 SHOW 绑定 sys_vars.cc:1515）；真实状态在 `g_receiver_prewarm_paused` 原子（worker 主循环 14699/14703 读取），由 `preserve_trx_transfer_set_prewarm_paused`（13788-13790）双写同步。

- 建议：保留一个 sysvar，另一个标记废弃/别名；prewarm_paused 单一事实源用原子。
- 风险：低；现有部署与 MTR 引用需迁移。

## P2-3（F18）同一 bundle 内存存两份；ready 信号散落 5 处

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

- 建议：内存层面两处共享 `shared_ptr<const Preserved_trx_bundle>`；中期把 ready cache entry 与 prepared token entry 合并为单一 per-token 就绪记录。
- 风险：两条 adopt 路径（经典 vs strict physical）消费字段略有差异，合并需保持 lint 契约与 digest 绑定语义。

## P2-4（F19）abort 路径：clear 后 `remove_if` 必然扫空 + 逐 token 独立 ABORT 帧

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

复核确认"必然扫空"：clear 与 remove_if 之间唯一的帧发送是 `send_encoded_transfer_frame`（5781，自由函数，只 encode+发送，不回填 pending）；pending 的唯一填充方是 `emit_frame_locked`，abort 路径不经过它；若 pending 本为空（跳过 clear），remove_if 同样扫空表。两种情形 remove_if 都必然遍历空 vector。`m_finalized_manifests` 的 remove_if（12155-12161）则是真实全量扫，T 个 token × M 个 manifest 为 O(T×M)。另外 `abort_token_locked` 每 token 发一个独立 ABORT 帧（12142-12151，一次网络往返）。

- 建议：`abort_epoch` 批量清理容器（finalized 按集合差一次重构）；ABORT 帧可合并发送（batch 编码或 epoch 级 abort）。
- 风险：接收端目前按 per-token ABORT 记账，合并帧需协议两侧同步。

## P2-5（F21，降级）phase1 采样 vector：单 drain 内无界增长 + 每次查询全量 sort

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

4 个采样 vector（207-210：batch_bytes / batch_tokens / record_batch_tokens / batch_linger_us）只 push_back（640-641、646、656），唯一清空点 `preserve_trx_transfer_reset_source_phase1_metrics()`（659-671）。**复核修正**：reset 并非测试专用——每次 batch drain 命令开头都会调用（preserve_trx.cc:17446），故实际语义是"单次 drain 内无界增长、每次 drain 重置"，不是长跑永久泄漏，严重性降级；但单 drain batch 很多时无界 + 每次查询 O(n log n) 全量拷贝仍成立。max 也是扫整个 vector（697-700、715-718、738-741）。

- 建议：固定容量 reservoir/环形采样（或 count/sum/max + 有界直方图），max 在线维护。
- 风险：采样语义变化影响既有 MTR 对 p50/p95 精确值的断言，需同步期望。

---

## P3（协议/接口改版，统一一次做）

### F14 三层完整性校验叠加

- 帧级：payload SHA256（encode 6791 / verify 6895）+ control CRC32（6817-6819 / 6881-6884，`my_checksum`）；
- batch 级：payload SHA256（6952 / 7072）+ control CRC32（6960-6962 / 7057-7061）；
- ack 级：`sha256_digest(encoded_payload)` 整段（7245 / 7328）+ ack body CRC32（7265-7267 / 7313-7316）。

同一份字节 3 层 4 次哈希；SHA256 严格强于 CRC32，CRC 不提供额外保证。建议每层只保留 SHA256，去掉两处 CRC，并考虑去掉 batch 级 digest。风险：改线格式，`transfer_protocol_version_is_decodable` 目前只认 v1（1484-1486），需 bump 版本（本仓库 source/receiver 同版本部署，可控）。

### F15 线/盘格式冗余（四点）

1. **死字段 `manifest.frame_sequence`**（transfer.h:516；encode 5938；decode 5989）：生产代码无 `.frame_sequence =` 写入点、无读取点（复核补充：gunit 的 5253/5286/5313/5326/5361 有赋值，但仅用于 round-trip 测试）。序列号实际由 frame header 的 `sequence` 承载。
2. **`epoch_fact_token` 重复携带完整 objects 列表**（transfer.h:523-529；encode 6199-6234）：`manifest_digest` 已是"包含这些 objects 的 manifest"的 SHA256，fact 又把每个 object 全字段（object_id|kind|flags|total_size|digest|lock_plan 6 个子字段，文本 hex 后约 200B/object）再抄一遍；`epoch_fact_tokens_equal`（6474-6482）同时比 digest 和 objects——digest 相等蕴含 objects 相等。
3. **平铺 frame 恒零字段**（transfer.h:545-568；encode 6799-6813）：`trx_id_store`（24B，仅 COMMIT_EPOCH）、`terminal_fact_digest`（32B；复核修正：COMMIT_EPOCH 也允许携带，4021-4024 要求 nonce 非空时必填，非"仅 QUERY/ABANDON"）、`requested_terminal_status_retention_us`（8B，仅 OPEN_EPOCH）、`receiver_process_nonce`（每帧重复）对每帧都编解码；`validate_frame_components`（3901-3943）用 10+ 个分支强制其他类型下这些字段必须为零/空——证明它们不构成真正的扩展位。
4. **epoch_fact 文本格式 vs 其余全二进制**：文本编解码 6146-6388 约 240 行（`split_pipe_fields`/`parse_uint64_strict`/`line_has_prefix`），与二进制 manifest 编解码（5925-6078）承载同类数据，双格式无双倍收益。

建议合并为一次 protocol v2 改版统一处理，避免多次格式 churn。另注意 retention 协商（7369-7380）高于 300s 是**直接拒绝** INVALID_ARGUMENT（7375）而非钳制（复核修正），改版时语义需保留。

### F20 超时/租约参数叠加

- `receiver_prewarm_timeout_ms` 的 PREWARMING deadline 在 READY 化时被 `receiver_ready_timeout_ms` 整体替换（`synchronize_receiver_epoch_ready_deadline` 5422-5426），前者只在"token 永远收不齐"时兜底，语义可被后者覆盖；
- `preserve_trx_promotion_gate_timeout_ms` 被 prewarm 路径借用来等 record-lock 驻留（promotion.cc:1909-1911）；
- 硬编码 `kClientResumeWindowUs = 300s`（transfer.cc:4951，写入 5026）+ promotion_prepared.h:295-296 双 deadline（均被实际使用）+ open_online_epoch retention 协商硬钳 [60s,300s]（7369-7380）——四处期限概念层层叠加；
- prewarm 队列状态簿记 6 件套：inflight/done/deferred 三个 set + object inflight + `g_receiver_record_plan_deferred` + `g_receiver_record_plan_attempted_generation`（13616-13626），配合 `kReceiverRecordLockObjectProofRetryLimit=16`（413）重试循环。

建议：PREWARMING/READY 两个 deadline 合并为单一 residency deadline；record-lock 驻留等待改用 prewarm 专用参数；retry/deferred 三 set 合并为带状态字段的单 map。风险：sysvar 是已发布接口，合并需保留旧变量别名；部分 deadline 区分了"传输期"与"就绪保持期"，合并前需确认 NFR 测试覆盖。

---

## 已撤销 / 已降级项（复核结论）

| 项 | 原描述 | 复核结论 |
|---|---|---|
| ~~限速参数每 chunk 重算~~ | `throttle_source_transfer_io`（568-574）每 chunk 调 `preserve_trx_transfer_current_runtime_limits()`（444-471） | **撤销**：只是 8 个全局变量的无锁读 + 几次 switch/min/max，纳秒级；且逐 chunk 重读是"sysvar 改了立即生效"的动态调速语义所需，非实质性能问题 |
| F21 采样 vector | "长跑实例无界内存增长" | **降级**：每次 batch drain 开头重置（preserve_trx.cc:17446），实为单 drain 内无界，见 P2-5 |
| F22 epoch.fact 重读 | "prewarm 每 token 重读同一文件（冗余 IO）" | **降级并入 F6**：生产 staged 路径传 `wait_for_final_epoch_fact=true`（promotion.cc:2069-2070）跳过读文件；冷路径生产不可达，重复读只在 gunit 发生 |

## 复核确认"不是问题"的点（避免误改）

- `Preserve_trx_transfer_phase1_batch_sender` 的 pimpl（756-997）：隐藏 `<thread>`/队列细节，语义完整，不算过度；
- `m_pending_final_metadata_frames` 延迟到 commit 才编码（10187-10190、12316-12334）：是 phase2 设计语义（final metadata 与 COMMIT 绑定），非冗余；
- `commit_epoch` 的多 worker 发送（12351-12395）：worker 数被 `min(sender_workers, data_sessions, payload 数)` 收敛，payload 为空时零开销，合理；
- `Preserve_trx_transfer_encoded_frame_sink` 的默认空实现 virtual（h:1013-1044）：仅服务测试 mock，可接受的测试接缝；
- 25 个 transfer sysvar 均有真实读取点，无完全死配置；
- epoch_fact 文本解码有 `token_count > 1000000`（6296）与 `kMaxTransferManifestObjects` 等限额，已做基本防 DoS，精简时不应削弱。

## 建议执行顺序

| 批次 | 内容 | 改动性质 |
|---|---|---|
| P0 | F1 解码/哈希去重（源端+接收端）；F2、F3 立删 | 纯内部重构，不动格式，CPU 收益最大 |
| P1 | F4 平行栈收口（保留 `send_token_objects_batch`）；F5+F6 死链删除（~1100+ 行，同步 lint.test:323-407/422-425/1951-1968/2732-2746 与 gunit）；F7-F13 单点去重 | 各自独立、低风险 |
| P2 | F16-F19、F21 | 内存/语义行为变化，需调 MTR 期望 |
| P3 | F14+F15 协议 v2 一次改版；F20 配置面合并（留别名） | 线/盘格式与已发布接口变更 |
