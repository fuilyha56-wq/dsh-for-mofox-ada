# DSH Transport Adapter Messaging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 DSH 注册为与 OneBot 同级的 Neo-MoFox Transport Adapter，使每个 DSH session 成为独立消息流，并完整支持反馈、结构化问答、审批和普通会话回复。

**Architecture:** 保留 `DshBridgeRuntime` 作为 DSH HTTP/SSE/CLI 共享运行时，通过异步事件监听出口连接新的 `DshTransportAdapter`。Adapter 只负责 DSH 事件与 `MessageEnvelope` 的双向转换；独立的 interaction registry、response codec 和 event renderer 负责持久化、验证、去重、权限与文本摘要，避免网络、协议和核心消息层耦合。

**Tech Stack:** Python 3.11、Neo-MoFox `BaseAdapter`/CoreSink/MessageEnvelope、httpx SSE、`storage_api` JSONStore、pytest/pytest-asyncio、Ruff、uv。

## Global Constraints

- DSH 契约基线固定为 `@deepseek-ai/dsh@0.1.0-rc.6`。
- 事件传输必须使用 HTTP SSE：`GET /api/events.mux` 与 `GET /api/events.host`，不得重新引入 WebSocket。
- 一条 DSH session 必须稳定对应一条 `platform="dsh"` 的 Neo-MoFox private stream。
- 所有 `question/requested` 与 `approval/requested` 只能通过结构化 `dsh_respond`、Owner 命令或 Service API 回答。
- 目标 session 存在 pending 交互时，普通文本出站必须失败，不得调用 `session.prompt` 或猜测答案。
- 默认 `interaction.approval_policy="ask"`；只有 `autonomous` 允许 Bot 发送 `allowed-once`，`reject` 自动拒绝。
- `headless` 保持一次性最终结果接口，不宣称支持中途交互。
- 保留现有 Service、Router、Tool、Action、Command、RPC、HTTP、CLI、进程、事件和数据能力。
- 所有新增 Python 文件、类、函数和方法必须有类型注解与文档字符串。
- 异步后台工作必须使用现有 `task_manager`，不得直接调用 `asyncio.create_task()`。
- 不自动创建 Git commit；每个任务以聚焦测试、诊断和差异审查作为检查点。

## File Structure

- Create `plugins/dsh_adapter/interactions.py`: pending request 数据模型、JSON 持久化、幂等状态机与 session 查询。
- Create `plugins/dsh_adapter/interaction_codec.py`: DSH question/approval payload 的纯验证与 `/api/respond` result 编码。
- Create `plugins/dsh_adapter/event_messages.py`: DSH frame 分类、安全文本渲染、稳定消息 ID 与进度聚合。
- Create `plugins/dsh_adapter/adapter.py`: `DshTransportAdapter`，连接 Runtime、CoreSink、registry 和 DSH RPC client。
- Modify `plugins/dsh_adapter/runtime.py`: 增加隔离的异步事件监听出口；继续拥有 SSE 与原始缓冲。
- Modify `plugins/dsh_adapter/config.py`: 新增 `interaction` section。
- Modify `plugins/dsh_adapter/components.py`: 新增 `DshInteractionResponseAction` 与 `/dsh pending|respond`。
- Modify `plugins/dsh_adapter/service.py`: 新增 pending 查询和结构化 response API。
- Modify `plugins/dsh_adapter/plugin.py`: 构造共享 registry，注册 Adapter/Action，理顺生命周期。
- Modify `plugins/dsh_adapter/manifest.json`: 注册 Adapter/Action，版本升至 `1.4.0`，移除 `websockets` 依赖。
- Modify `plugins/dsh_adapter/rpc_catalog.py`: 把非 RPC 事件 transport 描述改为 SSE。
- Modify `plugins/dsh_adapter/README.md`, `plugins/dsh_adapter/API.md`, `plugins/dsh_adapter/examples/dsh_adapter_service.py`: 更新用户契约与示例。
- Create tests under `plugins/dsh_adapter/tests/`: runtime listener、registry/codec、event messages、adapter、components、PluginManager integration。

---

### Task 1: Runtime Event Listener Outlet

**Files:**
- Modify: `plugins/dsh_adapter/runtime.py`
- Test: `plugins/dsh_adapter/tests/test_events.py`

**Interfaces:**
- Produces: `DshRuntimeEvent(stream: str, sequence: int, received_at: str, message: dict[str, Any])`
- Produces: `DshBridgeRuntime.add_event_listener(listener: DshEventListener) -> str`
- Produces: `DshBridgeRuntime.remove_event_listener(listener_id: str) -> bool`
- Consumes: 既有 `_append_event(state, message)` 和 TaskManager 管理的 SSE 循环。

- [ ] **Step 1: 添加监听器收到完整事件的失败测试**

在现有本地 SSE server 测试中注册异步监听器，把收到的 `DshRuntimeEvent` 放入测试队列。手写断言：

```python
assert event.stream == "mux"
assert event.sequence == 1
assert event.message == {
    "type": "server-request",
    "rpcId": "question-1",
    "method": "events.mux",
    "payload": {
        "type": "question/requested",
        "sessionId": "session-1",
        "questions": [],
    },
}
```

该测试捕获的错误：Runtime 只写内部 deque，Adapter 无法收到 SSE 事件。

- [ ] **Step 2: 运行测试确认 RED**

Run:

```powershell
$env:PYTHONPATH = (Get-Location).Path
uv run pytest plugins/dsh_adapter/tests/test_events.py::test_event_listener_receives_complete_runtime_event -q -p no:cacheprovider --no-cov
```

Expected: FAIL，因为 `add_event_listener` 或 `DshRuntimeEvent` 尚不存在。

- [ ] **Step 3: 实现最小事件值对象与监听注册表**

在 `runtime.py` 增加：

```python
@dataclass(frozen=True, slots=True)
class DshRuntimeEvent:
    """表示 Runtime 已缓冲的一条 DSH 下行事件。"""

    stream: str
    sequence: int
    received_at: str
    message: dict[str, Any]


DshEventListener = Callable[[DshRuntimeEvent], Awaitable[None]]
```

Runtime 保存 `dict[str, DshEventListener]`，注册时返回 `uuid4().hex`。`_append_event()` 先写有界缓冲并通知等待者，再逐个调用监听器。

- [ ] **Step 4: 添加监听器隔离和注销失败测试**

注册一个抛出异常的监听器和一个记录监听器；断言记录监听器仍收到事件、SSE 缓冲仍保留消息。注销记录监听器后追加第二个事件，断言它不再收到。

该测试捕获的错误：一个插件回调异常杀死整个 SSE 流，或卸载后监听器泄漏。

- [ ] **Step 5: 运行隔离测试确认 RED**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_events.py::test_event_listeners_are_isolated_and_removable -q -p no:cacheprovider --no-cov
```

Expected: FAIL，直到 Runtime 捕获每个 listener 的异常并正确移除 listener。

- [ ] **Step 6: 实现隔离通知并验证 GREEN**

监听器调用使用顺序 await；每个 listener 独立 `try/except` 并记录异常，不向 SSE 循环抛出。运行：

```powershell
uv run pytest plugins/dsh_adapter/tests/test_events.py -q -p no:cacheprovider --no-cov
```

Expected: 全部 PASS。

- [ ] **Step 7: 静态验证**

Run:

```powershell
uv run ruff check plugins/dsh_adapter/runtime.py plugins/dsh_adapter/tests/test_events.py
```

Expected: PASS。

### Task 2: Pending Interaction Registry

**Files:**
- Create: `plugins/dsh_adapter/interactions.py`
- Create: `plugins/dsh_adapter/tests/test_interactions.py`

**Interfaces:**
- Produces: `InteractionKind = Literal["question", "approval"]`
- Produces: `InteractionState = Literal["pending", "responding", "resolved", "stale"]`
- Produces: `DshPendingInteraction.from_runtime_event(event: DshRuntimeEvent) -> DshPendingInteraction | None`
- Produces: `DshInteractionRegistry.load() -> None`
- Produces: `DshInteractionRegistry.upsert(interaction: DshPendingInteraction) -> bool`
- Produces: `DshInteractionRegistry.list_pending(session_id: str | None = None) -> list[DshPendingInteraction]`
- Produces: `DshInteractionRegistry.get_pending(rpc_id: str) -> DshPendingInteraction`
- Produces: `DshInteractionRegistry.mark_responding(rpc_id: str) -> None`
- Produces: `DshInteractionRegistry.mark_pending(rpc_id: str) -> None`
- Produces: `DshInteractionRegistry.mark_resolved(rpc_id: str) -> None`
- Produces: `DshInteractionRegistry.mark_stale(rpc_id: str) -> None`
- Produces: `DshInteractionRegistry.resolve_question(question_rpc_id: str) -> bool`
- Produces: `DshInteractionRegistry.resolve_approval(approval_id: str) -> bool`
- Produces: `DshInteractionRegistry.mark_session_stale(session_id: str) -> int`
- Consumes: `storage_api.save_json("dsh_adapter", "pending_interactions", data)` 与
    `storage_api.load_json("dsh_adapter", "pending_interactions")`。

- [ ] **Step 1: 编写重放去重与 session 隔离失败测试**

使用真实 registry 和内存 persistence double，依次 upsert：

```python
question = DshPendingInteraction(
    rpc_id="rpc-1",
    session_id="session-a",
    kind="question",
    payload={"type": "question/requested", "questions": []},
    stream="mux",
    first_seen_at="2026-08-15T00:00:00+00:00",
    last_seen_at="2026-08-15T00:00:00+00:00",
)
```

断言第一次 `upsert()` 返回 `True`，相同 rpcId 重放返回 `False`，只更新
`last_seen_at`；`list_pending("session-b")` 不得看到 session-a 的记录。

该测试捕获的错误：SSE 重连把同一问题重复投递，或跨 session 混用 pending。

- [ ] **Step 2: 运行确认 RED**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_interactions.py::test_registry_deduplicates_replayed_rpc_id_per_session -q -p no:cacheprovider --no-cov
```

Expected: FAIL，因为 registry 尚不存在。

- [ ] **Step 3: 实现数据模型、锁和幂等 upsert**

`DshInteractionRegistry` 使用一个 `asyncio.Lock` 串行化读改写；构造函数注入
`load_func`/`save_func`，默认使用 `storage_api`，测试使用真实 dict persistence double。
持久化格式：

```json
{
  "version": 1,
  "items": {
    "rpc-1": {
      "rpc_id": "rpc-1",
      "session_id": "session-a",
      "kind": "question",
      "payload": {},
      "stream": "mux",
    "first_seen_at": "2026-08-15T00:00:00+00:00",
    "last_seen_at": "2026-08-15T00:00:00+00:00",
      "state": "pending"
    }
  }
}
```

- [ ] **Step 4: 编写状态回滚与持久化恢复失败测试**

测试 `pending -> responding -> pending` 网络失败回滚、`resolved/stale` 不出现在
`list_pending()`、重新构造 registry 并 `load()` 后状态保持。再覆盖：

- `question/resolved.questionRpcId` 精确结束原 question rpcId。
- `approval/resolved.approvalId` 只结束匹配 approvalId 的记录。
- `host/session-removed` 把该 session 的所有 pending 标为 stale，不影响其他 session。

该测试捕获的错误：失败响应提前消费 pending，或插件重启丢失待答请求。

- [ ] **Step 5: 实现状态转换并验证 GREEN**

`DshPendingInteraction.from_runtime_event()` 只接受完整 `server-request` 中的
`question/requested` 与 `approval/requested`，并从 payload 提取 sessionId、approvalId。
非法 transition 抛 `ValueError`；所有成功 mutation 完成后持久化。运行：

```powershell
uv run pytest plugins/dsh_adapter/tests/test_interactions.py -q -p no:cacheprovider --no-cov
uv run ruff check plugins/dsh_adapter/interactions.py plugins/dsh_adapter/tests/test_interactions.py
```

Expected: 全部 PASS。

### Task 3: Question and Approval Response Codec

**Files:**
- Create: `plugins/dsh_adapter/interaction_codec.py`
- Create: `plugins/dsh_adapter/tests/test_interaction_codec.py`

**Interfaces:**
- Produces: `build_question_response(interaction, answers: list[dict[str, Any]]) -> dict[str, Any]`
- Produces: `build_question_cancellation(interaction) -> dict[str, Any]`
- Produces: `build_approval_response(interaction, outcome: str) -> dict[str, Any]`
- Consumes: Task 2 的 `DshPendingInteraction`。

- [ ] **Step 1: 编写单题、多题、单选、多选和 custom 的表驱动失败测试**

使用手写 DSH payload fixture：

```python
{
    "type": "question/requested",
    "sessionId": "session-1",
    "questions": [
        {
            "id": "language",
            "question": "选择语言",
            "options": [{"label": "Python"}, {"label": "Rust"}],
        },
        {
            "id": "features",
            "question": "选择功能",
            "options": [{"label": "Tests"}, {"label": "Docs"}],
            "multiSelect": True,
        },
        {"id": "note", "question": "补充说明"},
    ],
}
```

断言成功 value 精确等于 DSH schema：`sessionId + answer.answers[]`，不包含 Adapter
内部字段。

该测试捕获的错误：问题答案 shape 不被 DSH `questionResponsePayloadSchema` 接受。

- [ ] **Step 2: 运行确认 RED**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_interaction_codec.py::test_build_question_response_matches_dsh_schema -q -p no:cacheprovider --no-cov
```

Expected: FAIL，因为 codec 尚不存在。

- [ ] **Step 3: 实现最小 question codec**

验证每个原始 id 恰好出现一次；有 options 时 selected label 必须来自 options；
`multiSelect != True` 时最多选择一项；无 options 时允许空 selected 加非空 custom。

- [ ] **Step 4: 编写非法答案失败测试**

分别覆盖漏题、重复 id、未知 id、非法 option、单选传多个、空 custom。断言均抛
`ValueError`，且错误文本包含具体 question id。

- [ ] **Step 5: 编写取消与审批 schema 失败测试**

断言：

```python
build_question_cancellation(question) == {
    "ok": False,
    "error": {"code": "cancelled", "message": "MoFox cancelled the DSH question"},
}
```

审批 value 必须精确为：

```python
{
    "ok": True,
    "value": {
        "sessionId": "session-1",
        "approvalId": "approval-1",
        "outcome": "allowed-once",
    },
}
```

任何非 `allowed-once/rejected` outcome 必须失败。

- [ ] **Step 6: 实现 approval/cancel codec 并验证 GREEN**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_interaction_codec.py -q -p no:cacheprovider --no-cov
uv run ruff check plugins/dsh_adapter/interaction_codec.py plugins/dsh_adapter/tests/test_interaction_codec.py
```

Expected: 全部 PASS。

### Task 4: DSH Event Rendering and Aggregation

**Files:**
- Create: `plugins/dsh_adapter/event_messages.py`
- Create: `plugins/dsh_adapter/tests/test_event_messages.py`

**Interfaces:**
- Produces: `RenderedDshEvent(session_id: str, message_id: str, received_at: str, text: str, requires_response: bool, immediate: bool, extra: dict[str, Any], raw_message: dict[str, Any])`
- Produces: `render_dsh_event(event: DshRuntimeEvent, max_characters: int) -> RenderedDshEvent | None`
- Produces: `DshProgressAggregator.add(rendered: RenderedDshEvent) -> list[RenderedDshEvent]`
- Produces: `DshProgressAggregator.flush_due(now: float) -> list[RenderedDshEvent]`
- Produces: `DshProgressAggregator.flush_session(session_id: str) -> RenderedDshEvent | None`
- Consumes: Task 1 的 `DshRuntimeEvent`。

- [ ] **Step 1: 编写 question/approval 安全渲染失败测试**

对 `question/requested` 断言文本包含 header、detail、每个 option label/description、
`multiSelect=true`、plan-review intent 和 rpcId；对 `approval/requested` 断言包含 toolName、
callId、reason、approvalId，且提示使用 `dsh_respond`。

断言 `message_id` 使用稳定字面值：`dsh:mux:<rpcId>`，同一重放得到相同 ID。

该测试捕获的错误：模型看不到选项/约束，或重连生成不同消息 ID。

- [ ] **Step 2: 运行确认 RED**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_event_messages.py::test_render_question_and_approval_preserves_decision_context -q -p no:cacheprovider --no-cov
```

Expected: FAIL，因为 renderer 尚不存在。

- [ ] **Step 3: 实现立即事件分类与长度边界**

立即事件集合：`question/requested`、`approval/requested`、`question/resolved`、
`approval/resolved`、`host/agent-error`；`session/event` 内 `event.type == "turn/end"`
也立即 flush 当前 session 聚合并投递完成原因。

渲染时递归删除 key 名包含 `secret`、`apiKey`、`credential` 的值；超过
`max_characters` 时添加明确截断标记。Base64 大字段只显示字节/字符长度。

- [ ] **Step 4: 编写聚合与敏感字段失败测试**

连续加入 `assistant/chunk`、`tool/call`、`tool/result`、`assistant/message`，断言窗口内
不立即产出；到期或 `turn/end` 后只产出一条摘要，且不包含逐 token chunk、
`apiKey="secret-value"` 或长 Base64 原文。

该测试捕获的错误：高频事件逐条唤醒 Chatter或 secret 进入聊天上下文。

- [ ] **Step 5: 实现 session 隔离聚合器并验证 GREEN**

聚合器只保存必要的类型、工具名、错误和完成摘要；按 session 使用独立 bucket。
`progress_delivery="critical_only"` 时普通事件不进入 bucket，立即事件仍正常返回；增加
一个测试证明该分支不会延迟产生摘要。
运行：

```powershell
uv run pytest plugins/dsh_adapter/tests/test_event_messages.py -q -p no:cacheprovider --no-cov
uv run ruff check plugins/dsh_adapter/event_messages.py plugins/dsh_adapter/tests/test_event_messages.py
```

Expected: 全部 PASS。

### Task 5: Native DSH Transport Adapter Inbound Path

**Files:**
- Create: `plugins/dsh_adapter/adapter.py`
- Create: `plugins/dsh_adapter/tests/test_adapter.py`
- Modify: `plugins/dsh_adapter/runtime.py`

**Interfaces:**
- Produces: `DshTransportAdapter(BaseAdapter)` with `name="dsh_adapter"`, `platform="dsh"`, `adapter_version="1.4.0"`
- Produces: `DshTransportAdapter.from_platform_message(raw: RenderedDshEvent) -> MessageEnvelope | None`
- Produces: `DshTransportAdapter._handle_runtime_event(event: DshRuntimeEvent) -> None`
- Produces: `DshTransportAdapter.on_adapter_loaded() -> None`
- Produces: `DshTransportAdapter.on_adapter_unloaded() -> None`
- Produces: `DshTransportAdapter.health_check() -> bool`
- Produces: `DshTransportAdapter.reconnect() -> None`
- Produces: `DshTransportAdapter.get_bot_info() -> dict[str, Any]`
- Consumes: Tasks 1-4、插件共享 `runtime`/`interaction_registry`、核心注入的 CoreSink。

- [ ] **Step 1: 编写真实 CoreSink 入站失败测试**

使用测试 CoreSink 保存 Adapter `core_sink.send()` 收到的真实 envelope。向 Adapter
传入一条 `question/requested` Runtime event，断言：

```python
assert envelope["direction"] == "incoming"
assert envelope["message_info"]["platform"] == "dsh"
assert envelope["message_info"]["user_info"]["user_id"] == "session-1"
assert envelope["message_info"]["message_type"] == "message"
assert envelope["message_info"]["extra"]["dsh_rpc_id"] == "rpc-1"
assert envelope["raw_message"]["rpcId"] == "rpc-1"
```

再用核心 `extract_stream_id()` 对两个 envelope 做字面断言：同 session 相等，不同 session
不等。

该测试捕获的错误：DSH 仍未进入标准 Adapter/CoreSink 链，或一 session 一流失效。

- [ ] **Step 2: 运行确认 RED**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_adapter.py::test_question_event_enters_core_as_dsh_private_message -q -p no:cacheprovider --no-cov
```

Expected: FAIL，因为 Adapter 尚不存在。

- [ ] **Step 3: 实现最小 Adapter 与 envelope builder**

`__init__(core_sink, plugin, **kwargs)` 不配置自动 transport，调用
`super().__init__(core_sink, plugin=plugin, **kwargs)`。`from_platform_message()` 构造：

```python
{
    "direction": "incoming",
    "message_info": {
        "platform": "dsh",
        "message_id": rendered.message_id,
        "time": received_timestamp,
        "message_type": "message",
        "user_info": {
            "platform": "dsh",
            "role": UserRole.MEMBER,
            "user_id": rendered.session_id,
            "user_nickname": f"DSH {rendered.session_id[:8]}",
        },
        "extra": rendered.extra,
    },
    "message_segment": [{"type": "text", "data": rendered.text}],
    "raw_message": event.message,
}
```

由于 mofox-wire 的 TypedDict 尚未声明 `message_type/extra`，只在构造边界做局部
`cast(MessageEnvelope, envelope_dict)`，不得散布 `type: ignore`。

- [ ] **Step 4: 编写 lifecycle/去重失败测试**

`on_adapter_loaded()` 必须先注册 Runtime listener，再依次启动 `mux` 与 `host` SSE；断言
调用顺序。第一次请求写 registry 并发送 envelope；相同 rpcId 重放不发送第二条。
`on_adapter_unloaded()` 必须先停止两条 SSE，再移除 listener；共享 Runtime 与 HTTP client
不得关闭，仍可调用 `host.describe` fake。

该顺序捕获的错误：Plugin 在 Adapter 注册前打开 SSE，DSH 的 pending baseline 已重放但
没有进入 CoreSink。

- [ ] **Step 5: 实现 listener lifecycle 和入站状态机**

顺序：更新 resolved/stale 状态 -> render -> pending `upsert()` -> 若新 pending 或非 pending
事件则调用 `await self.on_platform_message(rendered)`，由 BaseAdapter 执行
`from_platform_message()` 和 CoreSink send。
收到 `question/resolved`、`approval/resolved`、`host/session-removed` 时调用 Task 2 的状态
方法；其中 resolved 事件可见，session-removed 只更新 stale 状态和原始缓冲。Adapter 持有
listener ID；卸载时使用 TaskManager 取消聚合 flush 任务、停止两条 SSE、按配置处理未
flush 摘要并移除 listener。

- [ ] **Step 6: 编写聚合 flush 入站测试**

通过 Adapter 的真实事件监听入口加入多个普通 event，推进 fake clock 或直接调用
`flush_due()`，断言只发送一条摘要 envelope。

- [ ] **Step 7: 编写健康检查和重连失败测试**

`health_check()` 仅在 `host.describe.ok` 且 mux/host 两条流都 running 时返回 True。
`reconnect()` 不调用 `BaseAdapter.stop()` 或关闭 Runtime，只对未运行的 SSE 调
`start_event_stream()`；断言现有 listener ID 保持不变。`get_bot_info()` 精确返回：

```python
{"bot_id": "mofox", "bot_name": "Neo-MoFox", "platform": "dsh"}
```

该测试捕获的错误：无自动 transport 的 BaseAdapter 默认健康检查永远失败并每 30 秒
重启，或抽象 `get_bot_info()` 未实现导致 Adapter 无法实例化。

- [ ] **Step 8: 实现健康检查、重连和 bot identity**

`host.describe` 传输异常返回 False；业务 `ok=false` 返回 False。只恢复断开的事件流，
不得关闭共享 client 或受管 CLI 进程。

- [ ] **Step 9: 验证 GREEN**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_adapter.py plugins/dsh_adapter/tests/test_events.py -q -p no:cacheprovider --no-cov
uv run ruff check plugins/dsh_adapter/adapter.py plugins/dsh_adapter/runtime.py plugins/dsh_adapter/tests/test_adapter.py
```

Expected: 全部 PASS。

### Task 6: Native Adapter Outbound Session Prompt

**Files:**
- Modify: `plugins/dsh_adapter/adapter.py`
- Test: `plugins/dsh_adapter/tests/test_adapter.py`

**Interfaces:**
- Produces: `DshTransportAdapter._send_platform_message(envelope: MessageEnvelope) -> PlatformSendResult`
- Produces: `_extract_target_session_id(envelope: MessageEnvelope) -> str`
- Consumes: `runtime.client.call_async("session.prompt", payload)` 与 registry 的 session pending 查询。

- [ ] **Step 1: 编写空闲流普通文本发送失败测试**

构造真实 outgoing envelope，target user_id 为 `session-1`，segments 包含单个 text。Fake
client 返回完整成功 `DshRpcResult`。断言 `PlatformSendResult.success is True`，且 payload
精确为：

```python
{
    "sessionId": "session-1",
    "mode": "queue",
    "content": [{"type": "text", "text": "继续执行并汇报结果"}],
}
```

该测试捕获的错误：MoFox 对 DSH 流的正常回复没有进入 `session.prompt`。

- [ ] **Step 2: 运行确认 RED**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_adapter.py::test_outgoing_text_prompts_idle_dsh_session -q -p no:cacheprovider --no-cov
```

Expected: FAIL，因为 `_send_platform_message` 尚未实现。

- [ ] **Step 3: 实现文本提取与 session.prompt**

只接受 `platform=dsh`、private target 和 text segments；忽略 reply/at 段但拒绝媒体段。
DSH RPC `ok=false` 映射为
`PlatformSendResult(success=False, error="<error-code>: <error-message>", response=rpc_result.to_dict())`；
缺少 code 或 message 时分别使用 `unknown-error` 和 `DSH session.prompt failed`。

- [ ] **Step 4: 编写 pending 阻断失败测试**

给 `session-1` 写入一个 pending question，调用相同 outgoing envelope。断言：

```python
assert result.success is False
assert "dsh_respond" in (result.error or "")
assert client.calls == []
```

同时覆盖多个 pending、缺少 target session、错误 platform、媒体消息和 DSH 业务失败。

该测试捕获的错误：普通自然语言绕过结构化问题/审批协议。

- [ ] **Step 5: 实现阻断与错误映射并验证 GREEN**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_adapter.py -q -p no:cacheprovider --no-cov
uv run ruff check plugins/dsh_adapter/adapter.py plugins/dsh_adapter/tests/test_adapter.py
```

Expected: 全部 PASS。

### Task 7: Structured Interaction Response Service

**Files:**
- Modify: `plugins/dsh_adapter/adapter.py`
- Modify: `plugins/dsh_adapter/interactions.py`
- Create: `plugins/dsh_adapter/tests/test_interaction_service.py`

**Interfaces:**
- Produces: `DshInteractionResponder.respond_question(rpc_id: str, answers: list[dict[str, Any]]) -> dict[str, Any]`
- Produces: `DshInteractionResponder.cancel_question(rpc_id: str) -> dict[str, Any]`
- Produces: `DshInteractionResponder.respond_approval(rpc_id: str, outcome: str, actor: Literal["bot", "owner", "service"]) -> dict[str, Any]`
- Consumes: registry、Task 3 codec、`runtime.client.respond(rpc_id, result)`、`approval_policy`。

- [ ] **Step 1: 编写成功回执后才消费 pending 的失败测试**

使用真实 registry/codec/responder，fake client 第一次抛 `DshTransportError`，第二次返回
`{"accepted": True}`。断言第一次后 state 回到 `pending`，第二次后 state 为 resolved。

该测试捕获的错误：网络失败导致不可恢复地丢失 DSH 问题。

- [ ] **Step 2: 运行确认 RED**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_interaction_service.py::test_response_consumes_pending_only_after_dsh_accepts -q -p no:cacheprovider --no-cov
```

Expected: FAIL，因为 responder 尚不存在。

- [ ] **Step 3: 实现 responding transaction**

流程固定为：get pending -> validate kind -> mark responding -> build result ->
`client.respond()` -> accepted true 标 resolved；异常/bad-response 标回 pending；not-pending
标 stale。返回结构包含 `rpc_id/session_id/kind/accepted/state/receipt`。

- [ ] **Step 4: 编写审批策略失败测试**

表驱动覆盖：

| policy | actor | outcome | expected |
| --- | --- | --- | --- |
| ask | bot | allowed-once | PermissionError |
| ask | bot | rejected | accepted |
| ask | owner | allowed-once | accepted |
| ask | service | allowed-once | PermissionError |
| autonomous | bot | allowed-once | accepted |
| autonomous | service | allowed-once | accepted |
| reject | bot | allowed-once | PermissionError |

另测 `reject` 自动处理路径调用一次 `rejected`，重放相同 rpcId 不重复响应。

- [ ] **Step 5: 实现审批 policy 并验证 GREEN**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_interaction_service.py -q -p no:cacheprovider --no-cov
uv run ruff check plugins/dsh_adapter/adapter.py plugins/dsh_adapter/interactions.py plugins/dsh_adapter/tests/test_interaction_service.py
```

Expected: 全部 PASS。

### Task 8: LLM Action, Owner Command, and Service API

**Files:**
- Modify: `plugins/dsh_adapter/components.py`
- Modify: `plugins/dsh_adapter/service.py`
- Modify: `plugins/dsh_adapter/plugin.py`
- Modify: `plugins/dsh_adapter/tests/test_components.py`
- Create: `plugins/dsh_adapter/tests/test_service.py`

**Interfaces:**
- Produces: `DshInteractionResponseAction` with `name="dsh_respond"`
- Produces: `DshAdapterService.list_pending(session_id: str | None = None) -> list[dict[str, Any]]`
- Produces: `DshAdapterService.answer_question(rpc_id: str, answers: list[dict[str, Any]]) -> dict[str, Any]`
- Produces: `DshAdapterService.cancel_question(rpc_id: str) -> dict[str, Any]`
- Produces: `DshAdapterService.respond_approval(rpc_id: str, outcome: str, *, actor: str = "service") -> dict[str, Any]`
- Consumes: Plugin 的 `interaction_registry`、`interaction_responder` 和当前 Action `chat_stream`。

- [ ] **Step 1: 编写 Action session 归属失败测试**

构造 `ChatStream(platform="dsh")`，给 context 放入最近一条真实 DSH 入站 `Message`，其
`sender_id="session-1"`。Action 回答 `session-1` 的 rpcId 成功；尝试回答 session-2 的
rpcId 返回 False，且 responder 未发送。

该测试捕获的错误：模型从一个 DSH 流回答另一个会话的问题。

- [ ] **Step 2: 运行确认 RED**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_components.py::test_dsh_respond_action_cannot_cross_dsh_sessions -q -p no:cacheprovider --no-cov
```

Expected: FAIL，因为 Action 尚不存在。

- [ ] **Step 3: 实现 Action schema 和当前 session 解析**

Action 签名：

```python
async def execute(
    self,
    rpc_id: str,
    response_type: str,
    response_json: str = "{}",
) -> tuple[bool, str]:
```

`response_type` 只允许 `answer/cancel/approve/reject`。当前 session 优先取
`chat_stream.context.current_message.sender_id`，并要求 `chat_stream.platform == "dsh"`；
没有可靠 session 时失败，不查全局最近请求。

- [ ] **Step 4: 编写 Owner 与 bot 审批身份测试**

Action 调 responder 时 actor 固定为 `bot`；`/dsh respond approval <rpc_id> allow|reject`
调用 actor=`owner`。断言 ask policy 下 Bot approve 失败，Owner approve 成功。

- [ ] **Step 5: 实现命令与 Service API**

新增命令：

```text
/dsh pending [session_id]
/dsh respond answer <rpc_id> '<answers JSON array>'
/dsh respond cancel <rpc_id>
/dsh respond approval <rpc_id> <allow|reject>
```

Service 直接接收结构化 Python list/dict，不解析 JSON 字符串。Service actor 固定为
`service`，不得让调用方伪造 `owner`；因此 ask policy 下 Service 可拒绝但不能批准，
autonomous policy 下可批准。

- [ ] **Step 6: 验证 GREEN**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_components.py plugins/dsh_adapter/tests/test_service.py -q -p no:cacheprovider --no-cov
uv run ruff check plugins/dsh_adapter/components.py plugins/dsh_adapter/service.py plugins/dsh_adapter/tests/test_components.py plugins/dsh_adapter/tests/test_service.py
```

Expected: 全部 PASS。

### Task 9: Configuration and Component Registration

**Files:**
- Modify: `plugins/dsh_adapter/config.py`
- Modify: `plugins/dsh_adapter/plugin.py`
- Modify: `plugins/dsh_adapter/manifest.json`
- Modify: `plugins/dsh_adapter/rpc_catalog.py`
- Create: `plugins/dsh_adapter/tests/test_plugin_registration.py`
- Modify: `plugins/dsh_adapter/tests/test_operations.py`

**Interfaces:**
- Produces: `DshBridgeConfig.InteractionSection`
- Produces: Plugin shared attributes `interaction_registry`, `interaction_responder`
- Produces: manifest entries `adapter:dsh_adapter` and `action:dsh_respond`
- Consumes: Tasks 1-8。

- [ ] **Step 1: 编写配置验证失败测试**

断言默认值：

```python
assert config.interaction.enabled is True
assert config.interaction.approval_policy == "ask"
assert config.interaction.progress_delivery == "aggregate"
assert config.interaction.progress_window_seconds == 2.0
assert config.interaction.max_event_text_characters == 12000
assert config.interaction.persist_pending_requests is True
```

非法 policy/delivery 必须由配置模型拒绝；`interaction.enabled=True` 且
`bridge.start_event_streams=False` 时不注册 Adapter 与 `dsh_respond`，其他桥能力保持可用。
`persist_pending_requests=False` 时 registry 只在内存保存，不调用 `storage_api`；分别用测试
固定这两个分支。

- [ ] **Step 2: 运行确认 RED**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_plugin_registration.py::test_interaction_config_defaults_and_validation -q -p no:cacheprovider --no-cov
```

Expected: FAIL，因为 section 尚不存在。

- [ ] **Step 3: 实现配置和共享对象装配**

使用 `Literal["ask", "autonomous", "reject"]` 与
`Literal["aggregate", "critical_only"]`。Plugin 构造时创建 registry/responder；
`on_plugin_loaded()` 只负责 `await registry.load()` 和确保 Web 可用，不得在 Adapter 启用时
启动 SSE。初次 SSE 的所有权属于 Adapter：它必须先注册 listener，再启动两条流。
当 interaction 未启用但 `bridge.start_event_streams=True` 时，Plugin 才按旧行为启动原始
事件缓冲流。

- [ ] **Step 4: 编写 manifest/组件集合失败测试**

解析真实 manifest 并实例化 Plugin，断言版本 `1.4.0`、12 个 include（原 10 + adapter +
respond action）、11 个运行组件（config 不运行）、`websockets` 不在 dependencies，
`httpx` 保留。禁用 interaction 时 Adapter 和 dsh_respond 不返回，其他组件仍在。

该测试捕获的错误：文件存在但 PluginManager 不会发现或启动新 Adapter。

- [ ] **Step 5: 更新注册、版本和 SSE 目录描述**

在 `rpc_catalog.NON_RPC_CAPABILITIES` 中把 transport 改为：

```text
SSE GET /api/events.mux 与 /api/events.host
```

删除 manifest 的 `websockets>=16,<17`。

- [ ] **Step 6: 验证 GREEN**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_plugin_registration.py plugins/dsh_adapter/tests/test_operations.py -q -p no:cacheprovider --no-cov
uv run ruff check plugins/dsh_adapter/config.py plugins/dsh_adapter/plugin.py plugins/dsh_adapter/rpc_catalog.py plugins/dsh_adapter/tests/test_plugin_registration.py
```

Expected: 全部 PASS。

### Task 10: Core Receiver and AdapterManager Integration

**Files:**
- Create: `plugins/dsh_adapter/tests/test_transport_integration.py`
- Verify: `plugins/dsh_adapter/adapter.py`
- Verify: `plugins/dsh_adapter/interactions.py`

**Interfaces:**
- Consumes: 真实 `InProcessCoreSinkImpl`、`MessageReceiver.receive_envelope()`、
  `MessageConverter.envelope_to_message()`、`AdapterManager`、Task 9 注册。
- Produces: 可复现的 Adapter 到核心消息对象、核心发送到 DSH RPC 的集成证明。

- [ ] **Step 1: 编写真实转换链失败测试**

使用真实 `MessageConverter` 转换 Task 5 生成的 question envelope，断言：

```python
assert message.platform == "dsh"
assert message.chat_type == "private"
assert message.sender_id == "session-1"
assert message.extra["dsh_rpc_id"] == "rpc-1"
assert "选择语言" in (message.processed_plain_text or "")
```

该测试捕获的错误：Adapter 自测 envelope 看似正确，但核心转换器丢失关键关联字段。

- [ ] **Step 2: 运行确认 RED 或兼容性事实**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_transport_integration.py::test_dsh_envelope_survives_core_message_conversion -q -p no:cacheprovider --no-cov
```

Expected: 初次应因 Adapter 未完成而 FAIL；实现完成后 PASS。若失败原因是
mofox-wire TypedDict 与核心扩展字段不一致，只修 Adapter 构造边界，不修改核心。

- [ ] **Step 3: 编写 AdapterManager 生命周期集成测试**

用真实 registry/plugin instance/sink manager 测试启动 `dsh_adapter:adapter:dsh_adapter`，
断言 active adapter、Runtime listener、两条 SSE 与 CoreSink 均存在；stop 后 listener 和
SSE 清理且 shared Runtime/HTTP client 直到 Plugin unload 才关闭。

- [ ] **Step 4: 编写 MessageSender 出站集成测试**

构造 `Message(platform="dsh", chat_type="private", stream_id=<真实生成 stream>)`，让真实
`MessageConverter.message_to_envelope()` 与 Adapter 发送。只 fake DSH 外部 HTTP client，
断言 `session.prompt` payload 和 `send_message() is True`；pending 时返回 False 且不写发送历史。

- [ ] **Step 5: 验证集成 GREEN**

Run:

```powershell
uv run pytest plugins/dsh_adapter/tests/test_transport_integration.py -q -p no:cacheprovider --no-cov
```

Expected: 全部 PASS，无后台任务残留警告。

### Task 11: Documentation and Service Example

**Files:**
- Modify: `plugins/dsh_adapter/README.md`
- Modify: `plugins/dsh_adapter/API.md`
- Modify: `plugins/dsh_adapter/examples/dsh_adapter_service.py`

**Interfaces:**
- Consumes: 最终 `DshTransportAdapter`、`dsh_respond`、Interaction config 和 Service API。
- Produces: 用户可执行的配置、命令和插件调用说明。

- [ ] **Step 1: 更新 README 的组件与消息模型**

明确：

- Adapter 签名为 `dsh_adapter:adapter:dsh_adapter`。
- 每个 DSH session 是一个 `platform=dsh` 私聊流。
- Web session 支持问答/审批/反馈；headless 只返回最终结果。
- 问题/审批只能使用 `dsh_respond` 或 Owner 命令。
- `ask/autonomous/reject` 的权限差异。
- 事件 transport 是 SSE，不是 WebSocket。

- [ ] **Step 2: 更新 API 的结构化 payload 示例**

加入单选、多选、custom、cancel、approval、pending 查询和普通 `session.prompt` 示例；明确
DSH `accepted=true` 才消费 pending。

- [ ] **Step 3: 更新 Service 示例**

示例流程：调用 `await service.list_pending()`；question 使用
`await service.answer_question(rpc_id, answers)`，approval 使用
`await service.respond_approval(rpc_id, "rejected")`；打印回执。不得在示例中硬编码 secret。

- [ ] **Step 4: 文档一致性检查**

Run:

```powershell
rg -n "WebSocket|1\.3\.0|dsh_respond|platform=dsh|approval_policy" plugins/dsh_adapter
git diff --check -- plugins/dsh_adapter/README.md plugins/dsh_adapter/API.md plugins/dsh_adapter/examples/dsh_adapter_service.py
```

Expected: 事件文档不再称为 WebSocket；版本和新接口一致；diff check 无输出。

### Task 12: Full Regression and Real DSH Acceptance

**Files:**
- Verify: `plugins/dsh_adapter/**`
- Verify: `plugins/dsh_adapter/examples/dsh_adapter_service.py`

**Interfaces:**
- Consumes: Tasks 1-11 的全部产物。
- Produces: 单元、静态、PluginManager 与真实 DSH 往返证据。

- [ ] **Step 1: 运行插件完整测试**

Run:

```powershell
$env:PYTHONPATH = (Get-Location).Path
uv run pytest plugins/dsh_adapter/tests -q -p no:cacheprovider --no-cov
```

Expected: 全部 PASS；除上游已知 deprecation 外无新增 warning。

- [ ] **Step 2: 运行 Ruff 和编辑器诊断**

Run:

```powershell
uv run ruff check plugins/dsh_adapter
```

然后对所有新增/修改 Python 文件运行 Pylance syntax/diagnostic；Expected: 无新增错误。

- [ ] **Step 3: 真实 PluginManager 验收**

加载真实插件并断言：

```text
plugin_version = 1.4.0
manifest includes = 12
runtime components = 11
adapter signature = dsh_adapter:adapter:dsh_adapter
action schema = action-dsh_respond
service version = 1.4.0
```

启动和停止 Adapter 后检查 registry、AdapterManager、Runtime listeners 和 Service/Action
无残留。不得停止用户现有的 `127.0.0.1:18948` DSH Web。

- [ ] **Step 4: 真实空闲 session.prompt 验收**

选择或创建一个专用测试 DSH session，通过 DSH Adapter 的 `_send_platform_message()` 发送
唯一测试文本，确认 `session.history` 中出现该 user message。测试完成后取消或归档测试
session，不修改用户已有会话模型/preset。

- [ ] **Step 5: 真实 question/respond 往返验收**

在专用测试 session 中触发一个可控 `question/requested`：

1. SSE 收到完整 server-request。
2. Adapter 生成 `platform=dsh` 入站 Message。
3. registry 出现 pending rpcId。
4. 使用结构化 responder 发送合法答案或 cancel。
5. DSH `/api/respond` 返回 `accepted=true`。
6. SSE 收到 `question/resolved`，registry 变 resolved。

如果当前 DSH preset 无法稳定触发问题，使用 DSH 自带测试/开发入口或本地协议 fixture 做
端到端 HTTP/SSE 验收，并在最终报告中明确真实 Agent 未触发的环境限制；不得伪造成功。

- [ ] **Step 6: 审批策略真实或协议验收**

优先在专用 session 触发低风险审批并验证 ask policy 阻止 bot approve、Owner approve 被
接受。若无法安全触发真实审批，使用真实 `/api/respond` handler 的协议 fixture 覆盖
allowed-once/rejected，并明确未执行真实副作用工具。

- [ ] **Step 7: 最终差异审查**

Run:

```powershell
git diff --check
git diff --stat
git status --short
```

确认未修改 DSH 凭据、用户已有 session 数据、核心 Chatter 私有实现或无关工作树文件。