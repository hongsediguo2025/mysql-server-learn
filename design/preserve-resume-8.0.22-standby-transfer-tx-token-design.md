# MySQL 8.0.22 Preserve/Resume 备机直传 Token 取值方案

| 字段 | 值 |
|---|---|
| 分支 | `codex/user-temp-table-phase1-drain-resume` |
| 源码核查 HEAD | `1b6f680d9890` |
| 实现状态 | token selection、`uint64_t` manifest token、receiver `{epoch_id, token}` registry 已落地 |

本文只讨论备机直传场景中的 token 取值和相关最小代码边界。本文不设计发送协议、
receiver worker、目标端保存流程、在线升主 apply/resume、peerid 映射执行逻辑或
startup 行为。

本方案记录当前已落地的 transfer token 选择边界：transfer path 使用源端连接 id，
本地 preserve/resume 继续使用随机 token。跨实例协议身份和 endpoint 校验必须同时结合
source UUID、epoch 和 generation，不能把连接 id 当作跨生命周期全局唯一身份。

## 1. 结论

备机直传路径的 token 取值：

```text
token = source target_thd->thread_id()
```

这里的 `target_thd` 是源端正在被 `DRAIN TRANSACTIONS PRESERVE` 处理的业务连接 THD。
该 `thread_id()` 就是 proxy peerid 里的源端 Tx。

transfer manifest 中的 token 使用 `uint64_t`，值就是 Tx 本身。只有进入现有
preserve kernel 的 XID、metadata、文件名等字符串接口时，才把该 `uint64_t` token
格式化成十进制字符串。

本地 preserve/resume 路径的 token 取值不变：

```text
token = generate_preserve_trx_token()
```

也就是说：

- `LOCAL_CARRIER` 继续使用随机 32 hex token。
- `STANDBY_TRANSFER_SAVE` 使用源端 Tx 本身作为 `uint64_t` token。
- 不修改 `generate_preserve_trx_token()` 语义。
- 不把 Tx token 用到普通本地 preserve/resume 用例中。
- `server_uuid` 不参与 token 数值计算，但 source/target UUID 必须保留在 manifest/fact 中，
  用于跨实例协议身份和 endpoint 校验。
- transfer token 只服务后续物理备机在线升主时通过 `peerid(Tx, Ty)` 找到目标端 Ty。
- 本轮不实现 B 端 Tx 到 Ty 映射，只把 Tx 作为 transfer token 持久化。

## 2. 为什么 token 取 Tx

proxy 面向用户是一个连接，但对节点 A 和节点 B 建立两个后端连接。`peerid(Tx, Ty)` 记录
的是：

```text
Tx = 节点 A 后端连接的 THD::thread_id()
Ty = 节点 B 后端连接的 THD::thread_id()
```

事务 T 从 A 转移到 B 时，源端发送的 preserve 对象必须携带 Tx。未来 B 在线升主 apply
时，可以用 Tx 在 peerid 中找到 Ty，并由 Ty 承接事务 T。

因此，transfer token 直接取 Tx 最自然：

- token 本身就是源端连接身份。
- B 端保存 artifact 时文件名、manifest、registry key 都围绕 Tx。
- 未来 apply 不需要额外从随机 token 再查 Tx。

## 3. THD::thread_id() 语义

当前源码事实：

- `THD::thread_id()` 是 MySQL client protocol 使用的 32-bit connection id。
- 它不是 pthread id。
- `THD::set_new_thread_id()` 通过 `Global_THD_manager::get_new_thread_id()` 分配。
- `Global_THD_manager` 维护 active id 集合，连接存活期间不会复用同一个 id。
- `THD::release_resources()` 会释放 id；释放后未来连接可能复用。

设计含义：

- transfer 期间，源端连接必须仍然有效。
- 源端连接在 transfer accepted-and-saved 之前断开，peerid 失效，本次 transfer 失败。
- Tx token 不承诺跨源端重启全局唯一。
- Tx token 不承诺跨连接生命周期永久唯一。
- Tx token 只在当前计划内切换窗口和当前 peerid 集合内有意义。

## 4. 实现前问题与当前落点

原始 token 生成时机在 `preserve_trx_kernel_preserve_attached_transaction()` 中靠前：

```text
generate_preserve_trx_token()
  -> preserve_trx_token_to_xid(token)
  -> prepare/detach
  -> build metadata/bundle
  -> artifact decision
  -> artifact sink publish
```

这个顺序对本地路径没问题，但不支持 transfer token = Tx。原因：

- XID 已经包含 token。
- `Preserve_snapshot_metadata::token` 已经包含 token。
- bundle metadata 已经包含 token。
- transfer manifest 当前从 bundle metadata 取 token。

所以不能只在 receiver 或 manifest 层替换 token。当前实现已在 XID 构建之前调用
`preserve_trx_select_token_for_request()`，并把同一选择结果传给 metadata、bundle 和
transfer manifest。

## 5. 新 token selection 边界

当前 request-aware token selection helper 位于 `sql/preserve_trx.cc` 内部，现有随机 token
helper 保持不变。

当前数据结构：

```c++
struct Preserve_trx_token_selection {
  std::string preserve_token_string;
  uint64_t transfer_token{0};
  bool is_transfer_token{false};
  const char *failure_reason{nullptr};
};
```

`preserve_token_string` 是为了复用现有 XID、metadata、文件名和本地 token 校验接口；
`transfer_token` 才是 transfer manifest 里的 token。local path 下 `transfer_token` 为 0。

当前 helper：

```c++
bool preserve_trx_select_token_for_request(
    THD *target_thd,
    Preserve_trx_transfer_artifact_decision artifact_decision,
    Preserve_trx_token_selection *selection);
```

返回值语义沿用现有代码习惯：

- `false` 表示成功。
- `true` 表示失败，`selection->failure_reason` 给出原因。

## 6. 选择规则

### 6.1 LOCAL_CARRIER

当 artifact decision 是 `LOCAL_CARRIER`：

```text
selection.preserve_token_string = generate_preserve_trx_token()
selection.transfer_token = 0
selection.is_transfer_token = false
```

要求：

- 继续使用现有随机 32 hex。
- 继续检查本地 memory record collision 和本地 artifact file collision。
- 不改变任何现有本地 preserve/resume 测试期望。

### 6.2 STANDBY_TRANSFER_SAVE

当 artifact decision 是 `STANDBY_TRANSFER_SAVE`：

```text
selection.transfer_token = target_thd->thread_id()
selection.preserve_token_string = std::to_string(selection.transfer_token)
selection.is_transfer_token = true
```

要求：

- `target_thd != nullptr`。
- `target_thd->thread_id() != 0`。
- `delivery_mode == BATCH_MANAGER_DELIVERY`。
- `preserve_token_string` 必须通过现有 token filename-safe 校验。
- 源端本地已有同名 preserved record 或同名 artifact 时，返回失败。
- 失败时不得 fallback 到随机 token。

失败原因建议：

```text
standby_transfer_requires_batch_drain
standby_transfer_missing_target_thd
standby_transfer_invalid_token
standby_transfer_token_collision
standby_transfer_token_to_xid_failed
```

### 6.3 UNSUPPORTED

当 artifact decision 是 `UNSUPPORTED`：

- 不生成 token。
- 不进入 XID 构建。
- 不进入 prepare/detach。
- 直接按现有 preserve reject/failure path 返回。

## 7. artifact decision 时机

当前 `preserve_trx_transfer_artifact_decision()` 发生在 bundle 构建后。为了 token 取值，
需要在 token 生成前得到 request-aware decision。

建议新增：

```c++
Preserve_trx_transfer_artifact_decision
preserve_trx_transfer_artifact_decision_for_request(
    Preserve_trx_delivery_mode delivery_mode);
```

规则：

- transfer disabled 或 `artifact_mode=LOCAL_CARRIER` 返回 `LOCAL_CARRIER`。
- `artifact_mode=STANDBY_TRANSFER_SAVE` 时要求 transfer enable 和 endpoint 配置完整。
- `STANDBY_TRANSFER_SAVE` 只允许 batch drain delivery。
- 普通 single preserve 不进入 transfer path。

调整后的主流程：

```text
artifact_decision_for_request(delivery_mode)
  -> preserve_trx_select_token_for_request(...)
  -> result->token = selection.preserve_token_string
  -> preserve_trx_token_to_xid(selection.preserve_token_string)
  -> prepare/detach
  -> build metadata/bundle using selection.preserve_token_string
  -> publish using artifact sink
```

实现时可以在后续 artifact sink 创建处复用前面算出的 `artifact_decision`，不要再次用全局
sysvar 重新判断导致前后不一致。

## 8. manifest 中的 token 字段

transfer manifest 不应同时保存 `token` 和 `source_connection_id` 两个重复身份字段。
token 取值方案下的最小 manifest 只要求 `token` 字段本身就是 Tx，类型使用 `uint64_t`：

```c++
struct Preserve_trx_transfer_manifest {
  uint16_t protocol_version;
  std::string epoch_id;
  std::string source_server_uuid;
  std::string target_server_uuid;
  uint64_t token;
  uint64_t frame_sequence;
  std::vector<Preserve_trx_transfer_object_descriptor> objects;
};
```

manifest 必须携带 `source_server_uuid` 和 `target_server_uuid`。UUID 不参与 token 数值计算，
但它们是 endpoint validation 和跨实例 identity 的组成部分。当前 receiver transfer registry
使用 `{epoch_id, token}` 作为本机 key，并分别校验 source/target UUID；strict prepared registry
进一步使用 `{source_uuid, epoch_id, token, generation}`。任何路径都不能使用进程全局 flat
token 作为跨实例身份。

receiver manifest 校验至少覆盖：

```text
token != 0
epoch_id/source_server_uuid/target_server_uuid 合法
target_server_uuid 与目标实例配置匹配
```

这一步只保证 Tx 被随 artifact 一起保存。B 端 peerid 查找和 Ty 承接不在本轮范围。

## 9. XID 与 token 安全性

Tx token 进入现有 preserve kernel 字符串接口时使用十进制格式。该适配字符串满足现有
token 约束：

- 只包含数字。
- 可作为 filename-safe token。
- 可传给 `preserve_trx_token_to_xid()`。
- 长度远小于 XID bqual 可容纳上限。

实现时仍要走现有校验：

```text
token_is_filename_safe(token)
preserve_trx_token_to_xid(token, &xid)
```

不要为 Tx token 新开一套 XID 编码。

## 10. 与现有用例的隔离

必须保持：

- transfer 默认关闭时，现有所有本地 token 仍是随机 32 hex。
- `generate_preserve_trx_token()` 不改。
- 普通 single preserve 不使用 Tx token。
- 普通 local carrier 不使用 Tx token。
- 普通 resume 不因为 Tx token 方案改变行为。
- GUnit 和 MTR 中依赖随机 token 的本地用例不需要改期望。

允许新增：

- transfer-specific unit tests。
- manifest uint64_t token 校验测试。
- batch drain transfer token 选择测试。

## 11. 源端 collision 语义

Tx token 不像随机 token 那样全局稀疏，因此必须明确 collision 语义。

源端 token selection 时检查：

```text
preserved_trx_record_exists_locked(token)
preserved_trx_file_exists_for_token(token)
```

发现 collision：

- 返回失败。
- 不 fallback 到随机 token。
- 不覆盖已有 token。
- 不继续 prepare/detach。

目标端 duplicate token 由 receiver 保存阶段拒绝。那部分不在本文展开，但 token 取值方案
要求 receiver 不允许覆盖已有同名 standby artifact。

## 12. 已落地实现点

### 12.1 Request-aware artifact decision

文件：

- `sql/preserve_trx_transfer.h`
- `sql/preserve_trx_transfer.cc`

当前实现：

- 已增加 `preserve_trx_transfer_artifact_decision_for_request(delivery_mode)`。
- 保留现有 `preserve_trx_transfer_artifact_decision()`，供旧测试或非 request 场景使用。
- 新 helper 对 single preserve 的 `STANDBY_TRANSFER_SAVE` 返回 `UNSUPPORTED`。

### 12.2 Token selection helper

文件：

- `sql/preserve_trx.cc`

当前实现：

- 已增加 `Preserve_trx_token_selection`。
- 已增加 `preserve_trx_select_token_for_request()`。
- local path 内部继续调用 `generate_preserve_trx_token()`。
- transfer path 使用 `target_thd->thread_id()` 作为 `uint64_t transfer_token`，
  并生成 decimal `preserve_token_string` 适配现有内核接口。

### 12.3 Token 选择时机

文件：

- `sql/preserve_trx.cc`

当前实现：

- 在 `preserve_trx_kernel_preserve_attached_transaction()` 中，把 artifact decision 提前到
  token 生成前。
- 用 `selection.preserve_token_string` 构造 XID。
- 后续 metadata、result、bundle、artifact sink 均使用 `preserve_token_string`。
- transfer manifest 使用 `selection.transfer_token`，不要再从 bundle metadata 的 string
  token 反推一个重复身份字段。
- bundle 构建后的 artifact sink 创建复用前面确定的 decision。

### 12.4 Manifest token 使用 uint64_t Tx

文件：

- `sql/preserve_trx_transfer.h`
- `sql/preserve_trx_transfer.cc`
- `unittest/gunit/preserve_trx-t.cc`

当前实现：

- manifest 的 `token` 字段改为 `uint64_t`。
- build portable objects 时从 token selection 传入 Tx。
- encode/decode 更新。
- validate receiver manifest 时校验 `token != 0`。

### 12.5 回归保护

文件：

- `unittest/gunit/preserve_trx-t.cc`

现有测试应持续保护：

- `LOCAL_CARRIER` 仍调用随机 token 逻辑。
- `STANDBY_TRANSFER_SAVE` transfer token 等于 `target_thd->thread_id()`。
- transfer token collision 返回失败，不 fallback 随机 token。
- single preserve 不进入 transfer token path。
- manifest roundtrip 保留 uint64_t token。
- receiver 拒绝 token 为 0 的 manifest。

## 13. 验收标准

当前实现及后续变更必须满足：

- 源端 batch drain transfer token 等于源端 Tx。
- 本地 preserve/resume token 行为不变。
- XID、metadata、bundle 使用 Tx 的 decimal string 适配形式；manifest 使用 uint64_t Tx。
- manifest 中只持久化 `uint64_t token`，不再重复保存 `source_connection_id`。
- receiver 能拒绝 token 为 0 的 manifest。
- transfer token collision fail-closed，不生成随机 token 兜底。
- 代码改动集中在 token selection 和 manifest identity，避免侵入 preserve/resume 主逻辑。
