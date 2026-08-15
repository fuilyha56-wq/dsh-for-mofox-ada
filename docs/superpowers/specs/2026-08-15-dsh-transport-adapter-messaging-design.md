# DSH Transport Adapter 消息回传设计

## 状态

- 日期：2026-08-15
- 目标插件：`plugins/dsh_adapter`
- 计划版本：`1.4.0`
- 设计状态：已确认，待实现计划
- DSH 契约基线：`@deepseek-ai/dsh@0.1.0-rc.6`

## 背景

当前 `dsh_adapter` 已能通过 Service、Router、Tool、Action 和 Command 调用 DSH，
并支持一元 RPC、同源 HTTP、CLI、长期进程、SSE 事件流与 `DSH_HOME` 数据。
这些能力仍属于控制与桥接接口，插件尚未作为 Neo-MoFox 的原生消息平台注册。

目标形态与 `onebot_adapter` 同级：新增 `BaseAdapter` 组件，把 DSH 会话作为
`platform="dsh"` 的私聊会话接入核心传输链。每个 DSH session 对应一个稳定的
Neo-MoFox stream。DSH 发出的提问、选项、审批、反馈、完成状态与错误通过标准
`MessageEnvelope` 进入核心；MoFox 对该流的出站消息由 Adapter 转回 DSH。

## 已验证的外部契约

1. DSH 一元 RPC 使用 `POST /api/<method>`。
2. DSH 下行事件使用 SSE，而不是 WebSocket：
   - `GET /api/events.mux`
   - `GET /api/events.host`
3. 可回答事件是完整 `server-request` 信封，`rpcId` 必须在响应时原样回显。
4. 交互响应使用 `POST /api/respond`，body 为 `client-response` 信封。
5. 普通会话输入使用 `session.prompt`。
6. `headless` profile 只等待最终 Assistant 文本并退出，不提供问答或审批事件通道。
   需要完整交互的任务必须使用 Web session。

## 目标

- 将 `dsh_adapter:adapter:dsh_adapter` 注册为原生 Adapter 组件。
- 使用核心提供的 CoreSink、MessageReceiver、MessageDistributor 和 MessageSender，
  不直接依赖某个 Chatter 实现。
- 保证一条 DSH session 稳定对应一条 MoFox 私聊流。
- 让 DSH 问题、选项、审批和任务反馈进入正常 Chatter 决策流程。
- 使用结构化 Action 或 Owner 命令回答所有问题和审批。
- 在没有未决交互时，把 MoFox 发往 DSH 流的普通文本作为 `session.prompt`。
- 保留现有 Service、Router、Tool、Action、Command 和原始事件查询能力。
- 对 SSE 重连重放、重复事件和响应失败提供幂等恢复。

## 非目标

- 不把 DSH 事件回传到最初发起任务的 QQ、Telegram 或其他平台流。
- 不把 DSH session 与外部平台 stream 合并；DSH session 自身就是独立流。
- 不让普通自然语言隐式批准工具执行。
- 不为 DSH 的 52 个 RPC 分别创建 Action。
- 不绕过 DSH 对 preset、凭据、审批或会话状态的原生约束。
- 不宣称 headless 任务支持中途问答。

## 方案比较

### 方案 A：原生 Transport Adapter

新增 `BaseAdapter` 子类，使用标准入站与出站链路。DSH session 以平台用户身份
进入 Neo-MoFox。该方案与 OneBot 的组件层级一致，Chatter 无关，核心生命周期和
消息持久化均可复用。

这是选定方案。

### 方案 B：向发起流注入恢复事件

通过 `ChatterManager.resume_chatter()` 把 DSH 信息注入原 QQ 等流。实现较快，但依赖
具体 Chatter 对恢复提示的消费行为，也不满足“一条 DSH session 就是一条流”。

不采用。

### 方案 C：仅保留 Tool/Action 轮询

模型主动读取事件并调用 `respond`。底层可用，但 DSH 不是原生消息平台，异步事件
无法自然进入核心分发链。

不采用。

## 总体架构

```text
DSH Web
  ├─ SSE events.mux/events.host
  ├─ POST /api/respond
  └─ POST /api/session.prompt
          │
          ▼
DshBridgeRuntime
  ├─ 协议连接、重连、原始事件缓冲
  └─ 发布已解码事件给订阅者
          │
          ▼
DshTransportAdapter (platform=dsh)
  ├─ DSH event -> MessageEnvelope
  ├─ pending request registry
  └─ MessageEnvelope -> respond/session.prompt
          │
          ▼
CoreSink -> MessageReceiver -> MessageDistributor -> Chatter
          │
          ▼
MessageSender -> DshTransportAdapter
```

## 组件边界

### DshBridgeRuntime

职责：

- 维护 DSH HTTP client 和 SSE 重连循环。
- 保留当前有界事件缓冲、sequence 和 cursor 行为。
- 在每个已解码事件写入缓冲后，通知已注册的异步监听器。
- 不构造 `MessageEnvelope`，不调用 CoreSink，不理解 MoFox stream。

新增接口应支持注册和注销监听器，并保证单个监听器失败不会终止 SSE 流。监听器
收到的对象必须包含流名、桥 sequence、接收时间和完整 DSH `server-request` 信封。

### DshTransportAdapter

职责：

- 继承 `BaseAdapter`。
- 组件名为 `dsh_adapter`，平台标识为 `dsh`。
- 订阅 Runtime 的事件出口，并把可见事件转换为入站 `MessageEnvelope`。
- 维护并持久化需要回答的 pending request。
- 把 MoFox 出站消息转换为 DSH `session.prompt`。
- 执行结构化问题和审批响应。
- 提供基于 `host.describe` 与 SSE 状态的健康检查。

Adapter 停止时只注销监听器和停止自身工作，不关闭共享 Runtime。插件最终卸载时仍由
`DshAdapterPlugin.on_plugin_unloaded()` 关闭 Runtime、HTTP client 和受管进程。

### DshInteractionRegistry

独立模块，负责 pending request 的状态、去重和持久化，不依赖 CoreSink 或 Chatter。

每条记录至少包含：

```text
rpc_id
session_id
request_type          question | approval
payload               完整且经过 JSON 序列化的 DSH payload
stream_name           mux
first_seen_at
last_seen_at
state                 pending | responding | resolved | stale
```

存储键以 `rpcId` 为权威标识。SSE 重连重放相同 `rpcId` 时更新 `last_seen_at`，不得再次
生成入站消息。状态通过 `storage_api.save_json/load_json` 持久化到插件数据目录。

### DshInteractionResponseAction

新增 `dsh_respond` Action，专门回答 pending 问题或审批。Action 从当前
`self.chat_stream` 推导 DSH session，禁止跨 session 回答。

支持三类结构化请求：

- `answer`：答案数组，每项包含 `id`、`selected[]` 和可选 `custom`。
- `cancel_question`：向 `/api/respond` 发送 `ok=false`、`error.code="cancelled"`。
- `approval`：只允许 `allowed-once` 或 `rejected`。

同等能力通过 `/dsh respond` Owner 命令和 Service API 暴露，便于人工处理和插件集成。

## 流身份

所有入站 DSH session 消息使用：

```text
message_info.platform = "dsh"
message_info.user_info.user_id = <DSH sessionId>
message_info.user_info.user_nickname = "DSH <short-session-id>"
```

核心根据 `platform + user_id` 生成稳定 stream ID。因此：

- 同一 DSH session 的所有事件进入同一 MoFox stream。
- 不同 DSH session 永不共享 stream。
- 不需要在插件中维护 `DSH sessionId -> MoFox stream_id` 映射。
- DSH 流为 private chat，不创建 group_info。

## 入站消息契约

### 通用 Envelope

```text
direction = "incoming"
message_info.platform = "dsh"
message_info.message_type = "message"
message_info.message_id = <稳定事件 ID>
message_info.extra = <DSH 关联元数据>
message_segment = [{"type": "text", "data": <安全渲染文本>}]
raw_message = <完整 DSH server-request>
```

稳定事件 ID 使用 `dsh:<stream>:<rpcId>`；对于同一 `rpcId` 内可区分的推送事件，追加
事件类型和 DSH session event seq。`message_info.extra` 至少保存：

```text
dsh_rpc_id
dsh_session_id
dsh_frame_type
dsh_event_stream
dsh_bridge_sequence
dsh_requires_response
```

### 立即投递事件

以下事件立即转成标准入站消息：

- `question/requested`
- `approval/requested`
- `question/resolved`
- `approval/resolved`
- `host/agent-error`
- 明确表示 turn 完成、失败或取消的 `session/event`

问题文本必须包含 question id、header、question、detail、options、option description、
multiSelect 和 plan-review intent。审批文本必须包含 approval id、toolName、callId 和
reason，并明确说明只能通过 `dsh_respond` 或 Owner 命令处理。

### 聚合投递事件

普通 `session/event`、tool call/result、queue、jobs、projection 和状态变化进入短窗口
聚合器。聚合器按 session 隔离，在配置的窗口结束或收到 turn 结束事件时投递一条摘要。
原始事件不因聚合而丢失，仍保存在 Runtime 的 `event_read` 缓冲中。

聚合摘要不得包含：

- 凭据值、API key 或 secret 设置内容。
- 未裁剪的 Base64 附件。
- 超过配置长度的原始 JSON。
- 高频 `assistant/chunk` 的逐 token 副本。

### 仅缓冲事件

连接控制、subscription baseline、桥解码错误与连接重试默认只进入原始事件缓冲和日志，
不创建用户可见消息。无法归属 session 的 Host 事件同样只缓冲，除非它是 agent error。

## 出站消息契约

### 无 pending 交互

MoFox 向 `platform=dsh` 流发送普通文本时，Adapter 从目标 `user_id` 取得 sessionId，
调用：

```json
{
  "method": "session.prompt",
  "payload": {
    "sessionId": "<session-id>",
    "mode": "queue",
    "content": [{"type": "text", "text": "<message>"}]
  }
}
```

Adapter 只有在 RPC 业务结果 `ok=true` 时返回 `PlatformSendResult(success=true)`。

### 存在 pending 交互

只要目标 session 存在任何 `question/requested` 或 `approval/requested` pending，普通文本
出站就必须失败。失败信息应明确要求使用 `dsh_respond` Action 或 `/dsh respond`，不得：

- 把自然语言猜成问题答案。
- 把自然语言猜成审批结果。
- 在 pending 期间另发 `session.prompt`。

这一约束避免 Chatter 的普通回复被误解释为 DSH 交互响应，也避免审批旁路。

## 问题响应

成功答案的 `/api/respond` value 必须为：

```json
{
  "sessionId": "<session-id>",
  "answer": {
    "answers": [
      {
        "id": "<question-id>",
        "selected": ["<option-label>"],
        "custom": "<optional-free-text>"
      }
    ]
  }
}
```

规则：

- 每个原始问题 id 必须恰好出现一次。
- `selected` 始终为数组；无预定义选项而使用自由文本时可为空数组并提供 `custom`。
- 非多选问题最多选择一个 label。
- 所有 label 必须来自原始 options。
- 取消问题使用 `ok=false` 且 `error.code="cancelled"`。
- DSH 回执 `accepted=true` 后才将 registry 记录标为 resolved。

## 审批响应与权限

配置项 `interaction.approval_policy` 支持：

- `ask`：默认值。Bot 可以分析和说明，但只有 Owner 命令可以发送 `allowed-once`；
  Bot 通过 Action 只能拒绝。
- `autonomous`：Bot 可对每一条审批请求调用 `dsh_respond`，持续逐次选择
  `allowed-once` 或 `rejected`。DSH 协议本身只有单次批准，不存在永久批准值。
- `reject`：Adapter 收到审批请求后立即发送 `rejected`，并把结果作为入站反馈投递。

所有模式都必须保留审批请求和结果审计信息，但不得记录凭据值。`allowed-once` 必须精确
关联当前 session、approvalId 和 rpcId；不得把一次批准复用于后续请求。

## 配置

新增 `interaction` section：

```toml
[interaction]
enabled = true
approval_policy = "ask"
progress_delivery = "aggregate"
progress_window_seconds = 2.0
max_event_text_characters = 12000
persist_pending_requests = true
```

约束：

- `approval_policy` 仅允许 `ask`、`autonomous`、`reject`。
- `progress_delivery` 首期仅允许 `aggregate`、`critical_only`。
- 禁用 `interaction.enabled` 时 Adapter 不注册，现有桥接能力仍可使用。
- `bridge.start_event_streams` 为 Adapter 启用时的必要条件；配置校验应避免启用 Adapter
  却关闭事件流。

## 错误处理与恢复

- SSE 监听器异常：记录错误，继续其他监听器与 SSE 流。
- 重连重放：按 rpcId 和稳定事件 ID 去重。
- `/api/respond` 网络错误：pending 保持不变，允许重试。
- `/api/respond` 返回 `accepted=false, reason="not-pending"`：标记 stale，并投递过期反馈。
- `/api/respond` 返回 `bad-response`：保持 pending，返回结构校验错误，不自动改写答案。
- `session.prompt` 业务失败：平台发送失败，保留 DSH error code/message。
- DSH session 已删除：相关 pending 标记 stale；后续出站明确失败。
- 插件卸载：先停止 Adapter 事件消费和聚合任务，再关闭共享 Runtime。

## 安全边界

- 所有审批默认采用 `ask`。
- Secret 不得通过事件摘要、日志、Action 返回值或持久化 registry 回显。
- `credentials.set` 和 `llm.discoverModels.apiKey` 继续不建议由聊天模型传入。
- 原始事件保留受现有事件缓冲与本机 Router 权限控制；不新增公网暴露。
- Adapter 只接受目标 platform 为 `dsh` 的出站消息。
- 结构化响应必须校验当前 Action 所在 DSH stream 与 pending sessionId 一致。

## 与现有能力的兼容

以下组件继续保留：

- `dsh_adapter:service:dsh_adapter`
- `dsh_adapter:command:dsh`
- `dsh_adapter:router:dsh_adapter`
- `dsh_adapter:tool:dsh_query`
- `dsh_headless`
- `dsh_model_switch`
- `dsh_preset_switch`
- `dsh_rpc`
- `dsh_operate`

新增：

- `dsh_adapter:adapter:dsh_adapter`
- `dsh_adapter:action:dsh_respond`

现有通用 `respond` operation 继续存在，作为 Service、Router 和运维接口。模型处理交互时
应优先使用 `dsh_respond`，因为它执行 session 归属、payload 和权限校验。

当前事件传输已使用 HTTP SSE，因此实现时删除不再使用的 `websockets` 插件依赖，并把
README、API 和 RPC 非一元能力目录中的旧 WebSocket 描述改为 SSE。

## 测试策略

实现使用测试驱动开发，每项行为先看到预期失败，再写最小实现。

### Runtime

- SSE 完整保留 `server-request` 信封。
- 事件监听器收到 stream、sequence、时间与信封。
- 一个监听器失败不影响缓冲和其他监听器。
- 监听器可安全注销。

### Adapter 入站

- `question/requested` 转成标准 incoming envelope。
- 问题、选项、多选、detail 和 intent 均出现在安全文本与 raw payload 中。
- `approval/requested` 生成 pending 并立即投递。
- 相同 rpcId 重放不重复投递。
- 两个 DSH session 生成不同 MoFox stream，同一 session 保持一致。
- `host/agent-error` 正确归属 session 并投递。
- 高频进度在窗口内聚合，turn 结束时立即 flush。

### Adapter 出站

- 无 pending 时文本调用 `session.prompt`。
- pending 存在时普通文本发送失败且不调用任何 DSH RPC。
- 非 dsh platform 不被 Adapter 接受。
- `session.prompt` 业务失败映射为发送失败。

### 结构化应答

- 单题、多题、单选、多选和 custom 答案编码精确。
- 漏题、重复 id、非法 label 和多选约束失败时不发送。
- 取消问题生成 DSH 要求的 error 分支。
- `ask` 模式下 Bot 不能批准，Owner 可以批准。
- `autonomous` 模式下 Bot 可逐次批准。
- `reject` 模式自动拒绝且幂等。
- 回执成功才消费 pending；网络失败和 bad-response 保留 pending。
- Action 不能回答其他 DSH session 的请求。

### 集成

- Manifest 和 PluginManager 注册 Adapter 与 Action。
- AdapterManager 能启动和停止 DSH Adapter，无残留监听器或任务。
- 真实 DSH `question/requested` 能进入对应 `platform=dsh` stream。
- 真实 `/api/respond` 被 DSH 接受并产生 resolved 事件。
- 真实空闲 DSH 流普通消息触发 `session.prompt`。
- 现有 RPC、模型、preset、CLI、HTTP、数据与事件测试全部继续通过。

## 验收标准

1. 组件签名 `dsh_adapter:adapter:dsh_adapter` 在全局注册表可见并由 AdapterManager 启动。
2. 任意 DSH session 的问题、审批、错误和完成反馈能进入唯一对应的 MoFox 私聊流。
3. 同一 session 重连后不产生重复问题消息。
4. 所有问题和审批只能通过结构化 Action、Owner 命令或 Service API 回答。
5. 未决交互期间普通文本不会被错误发送为新 prompt 或审批。
6. 无未决交互时，MoFox 向 DSH 流发送文本可继续驱动该 DSH session。
7. 默认审批策略不会允许 Bot 自主批准；切换为 autonomous 后允许逐次持续审批。
8. 原始事件、RPC、CLI、HTTP、进程和数据能力保持可用。
9. 插件完整测试与 Ruff 通过，真实 DSH 交互往返通过。

## 实施顺序约束

1. 先扩展 Runtime 的事件监听出口，并用测试证明 SSE 信封与隔离行为。
2. 再实现 registry 与纯事件渲染/codec，避免 Adapter 类承担协议细节。
3. 实现 Adapter 入站后立刻做 CoreSink 聚焦集成测试。
4. 实现结构化应答和审批策略。
5. 最后接出站 `session.prompt`、PluginManager 注册、文档和真实 DSH 验收。

不得先把事件直接注入 Chatter，再回头补 Adapter；Transport Adapter 是本功能的控制路径。