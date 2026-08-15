"""DSH 原生 Transport Adapter 入站行为测试。"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from plugins.dsh_adapter.adapter import DshInteractionResponder, DshTransportAdapter
from plugins.dsh_adapter.client import DshRpcResult
from plugins.dsh_adapter.event_messages import RenderedDshEvent
from plugins.dsh_adapter.interactions import DshInteractionRegistry
from plugins.dsh_adapter.runtime import DshRuntimeEvent
from src.core.transport.message_receive.utils import extract_stream_id


async def _load_empty(_store: str, _name: str) -> dict[str, Any] | None:
    """返回空的测试持久化数据。"""

    return None


async def _save_memory(
    _store: str,
    _name: str,
    _data: dict[str, Any],
) -> None:
    """接受 registry 测试持久化写入。"""


class _RuntimeStub:
    """提供首轮入站测试所需的最小 Runtime 接口。"""


class _LifecycleRuntime:
    """记录 Adapter 生命周期调用并提供健康检查 double。"""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.listener: Any | None = None
        self.fail_on_start: str | None = None
        self.statuses = {
            "mux": {"name": "mux", "running": True},
            "host": {"name": "host", "running": True},
        }
        self.client = AsyncMock()
        self.client.call_async.return_value = DshRpcResult(
            rpc_id="host-rpc",
            ok=True,
            value={"version": "0.1.0-rc.6"},
        )

    def add_event_listener(self, listener: Any) -> str:
        """记录 listener 注册。"""

        self.calls.append("listener:add")
        self.listener = listener
        return "listener-1"

    def remove_event_listener(self, listener_id: str) -> bool:
        """记录 listener 移除。"""

        self.calls.append(f"listener:remove:{listener_id}")
        self.listener = None
        return True

    async def start_event_stream(self, name: str) -> dict[str, Any]:
        """记录 SSE 启动。"""

        self.calls.append(f"stream:start:{name}")
        if name == self.fail_on_start:
            raise RuntimeError(f"cannot start {name}")
        self.statuses[name]["running"] = True
        return self.statuses[name]

    async def stop_event_stream(self, name: str) -> dict[str, Any]:
        """记录 SSE 停止。"""

        self.calls.append(f"stream:stop:{name}")
        self.statuses[name]["running"] = False
        return self.statuses[name]

    def event_stream_status(self, name: str) -> dict[str, Any]:
        """返回 fake SSE 状态。"""

        return dict(self.statuses[name])


def _make_event(
    *,
    rpc_id: str,
    payload_type: str,
    session_id: str = "session-1",
    sequence: int = 1,
    **payload_fields: Any,
) -> DshRuntimeEvent:
    """构造一条完整的 DSH server-request 测试事件。"""

    return DshRuntimeEvent(
        stream="mux",
        sequence=sequence,
        received_at="2026-08-15T00:00:00+00:00",
        message={
            "type": "server-request",
            "rpcId": rpc_id,
            "payload": {
                "type": payload_type,
                "sessionId": session_id,
                **payload_fields,
            },
        },
    )


def _make_adapter(sink: AsyncMock) -> tuple[DshTransportAdapter, DshInteractionRegistry]:
    """构造使用内存持久化的测试 Adapter。"""

    registry = DshInteractionRegistry(
        load_func=_load_empty,
        save_func=_save_memory,
    )
    return (
        DshTransportAdapter(
            core_sink=sink,
            runtime=_RuntimeStub(),
            interaction_registry=registry,
        ),
        registry,
    )


@pytest.mark.asyncio
async def test_reject_policy_auto_rejects_new_approval_once() -> None:
    """Adapter 仅对首次出现的 approval 请求自动发送一次 rejected。"""

    sink = AsyncMock()
    runtime = _LifecycleRuntime()
    runtime.client.respond = AsyncMock(return_value={"accepted": True})
    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)
    responder = DshInteractionResponder(
        runtime, registry, approval_policy="reject"
    )
    adapter = DshTransportAdapter(
        core_sink=sink,
        runtime=runtime,
        interaction_registry=registry,
        interaction_responder=responder,
    )
    event = _make_event(
        rpc_id="approval-rpc",
        payload_type="approval/requested",
        approvalId="approval-1",
        toolName="shell_exec",
    )

    await adapter._handle_runtime_event(event)
    await adapter._handle_runtime_event(event)

    runtime.client.respond.assert_awaited_once_with(
        "approval-rpc",
        {
            "ok": True,
            "value": {
                "sessionId": "session-1",
                "approvalId": "approval-1",
                "outcome": "rejected",
            },
        },
    )
    assert await registry.list_pending("session-1") == []


def _make_progress_event(
    *,
    session_id: str = "session-1",
    sequence: int = 1,
    event_type: str = "assistant/chunk",
) -> DshRuntimeEvent:
    """构造一条普通 session/event 进度帧。"""

    return _make_event(
        rpc_id=f"progress-{sequence}",
        payload_type="session/event",
        session_id=session_id,
        sequence=sequence,
        event={"type": event_type, "seq": sequence, "data": {"text": "token"}},
    )


def _make_outgoing_envelope(
    *,
    session_id: str = "session-1",
    platform: str = "dsh",
    segments: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """构造发往一个 DSH 私聊 session 的标准 outgoing envelope。"""

    return {
        "direction": "outgoing",
        "message_info": {
            "platform": platform,
            "message_id": "outgoing-1",
            "user_info": {
                "platform": platform,
                "user_id": session_id,
                "role": None,
            },
        },
        "message_segment": segments
        if segments is not None
        else [{"type": "text", "data": "继续执行并汇报结果"}],
    }


@pytest.mark.asyncio
async def test_question_event_enters_core_as_dsh_private_message() -> None:
    """question/requested 应沿标准 Adapter/CoreSink 链进入 DSH 私聊流。"""

    sink = AsyncMock()
    registry = DshInteractionRegistry(
        load_func=_load_empty,
        save_func=_save_memory,
    )
    adapter = DshTransportAdapter(
        core_sink=sink,
        runtime=_RuntimeStub(),
        interaction_registry=registry,
    )
    event = DshRuntimeEvent(
        stream="mux",
        sequence=7,
        received_at="2026-08-15T00:00:00+00:00",
        message={
            "type": "server-request",
            "rpcId": "rpc-1",
            "payload": {
                "type": "question/requested",
                "sessionId": "session-1",
                "questions": [
                    {"id": "q-1", "question": "继续吗？", "apiKey": "secret"}
                ],
            },
        },
    )

    await adapter._handle_runtime_event(event)

    sink.send.assert_awaited_once()
    envelope = sink.send.await_args.args[0]
    assert envelope["direction"] == "incoming"
    assert envelope["message_info"]["platform"] == "dsh"
    assert envelope["message_info"]["user_info"]["user_id"] == "session-1"
    assert envelope["message_info"]["message_type"] == "message"
    assert envelope["message_info"]["extra"]["dsh_rpc_id"] == "rpc-1"
    assert envelope["raw_message"]["rpcId"] == "rpc-1"
    assert envelope["raw_message"]["payload"]["questions"][0]["apiKey"] == "[REDACTED]"


def test_constructor_tolerates_core_sink_injection_after_construction() -> None:
    """AdapterManager 应能先以 core_sink=None 实例化，再注入真实 Sink。"""

    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)

    adapter = DshTransportAdapter(
        core_sink=None,
        runtime=_RuntimeStub(),
        interaction_registry=registry,
    )

    assert adapter.core_sink is None


@pytest.mark.asyncio
async def test_from_platform_message_rejects_timestamp_without_timezone() -> None:
    """畸形 received_at 应明确失败，不能静默伪造消息时间。"""

    sink = AsyncMock()
    adapter, _registry = _make_adapter(sink)
    rendered = RenderedDshEvent(
        session_id="session-1",
        message_id="dsh:mux:rpc-1",
        received_at="2026-08-15T00:00:00",
        text="event",
        requires_response=False,
        immediate=True,
        extra={"dsh_session_id": "session-1"},
        raw_message={"type": "server-request", "rpcId": "rpc-1"},
    )

    with pytest.raises(ValueError, match="received_at 必须包含时区"):
        await adapter.from_platform_message(rendered)


@pytest.mark.asyncio
async def test_pending_replay_is_deduplicated_and_sessions_are_isolated() -> None:
    """同 rpcId 重放只投递一次，流 ID 按 DSH session 稳定隔离。"""

    sink = AsyncMock()
    adapter, registry = _make_adapter(sink)
    first = _make_event(
        rpc_id="rpc-1",
        payload_type="question/requested",
        questions=[],
    )
    second_session = _make_event(
        rpc_id="rpc-3",
        payload_type="question/requested",
        session_id="session-2",
        sequence=3,
        questions=[],
    )
    same_session = _make_event(
        rpc_id="rpc-2",
        payload_type="question/requested",
        sequence=2,
        questions=[],
    )

    await adapter._handle_runtime_event(first)
    await adapter._handle_runtime_event(first)
    await adapter._handle_runtime_event(same_session)
    await adapter._handle_runtime_event(second_session)

    assert sink.send.await_count == 3
    first_envelope = sink.send.await_args_list[0].args[0]
    same_session_envelope = sink.send.await_args_list[1].args[0]
    second_envelope = sink.send.await_args_list[2].args[0]
    assert extract_stream_id(first_envelope["message_info"]) == extract_stream_id(
        same_session_envelope["message_info"]
    )
    assert extract_stream_id(first_envelope["message_info"]) != extract_stream_id(
        second_envelope["message_info"]
    )
    assert {item.rpc_id for item in await registry.list_pending()} == {
        "rpc-1",
        "rpc-2",
        "rpc-3",
    }


@pytest.mark.asyncio
async def test_resolved_events_update_registry_and_remain_visible() -> None:
    """resolved 帧应先结束精确 pending，再作为普通入站消息可见。"""

    sink = AsyncMock()
    adapter, registry = _make_adapter(sink)
    await adapter._handle_runtime_event(
        _make_event(
            rpc_id="question-rpc",
            payload_type="question/requested",
            questions=[],
        )
    )
    await adapter._handle_runtime_event(
        _make_event(
            rpc_id="question-resolved-frame",
            payload_type="question/resolved",
            sequence=2,
            questionRpcId="question-rpc",
        )
    )
    await adapter._handle_runtime_event(
        _make_event(
            rpc_id="approval-rpc",
            payload_type="approval/requested",
            sequence=3,
            approvalId="approval-1",
        )
    )
    await adapter._handle_runtime_event(
        _make_event(
            rpc_id="approval-resolved-frame",
            payload_type="approval/resolved",
            sequence=4,
            approvalId="approval-1",
        )
    )

    assert await registry.list_pending() == []
    assert sink.send.await_count == 4
    assert (
        sink.send.await_args_list[1].args[0]["message_info"]["extra"][
            "dsh_session_event_type"
        ]
        == "question/resolved"
    )
    assert (
        sink.send.await_args_list[3].args[0]["message_info"]["extra"][
            "dsh_session_event_type"
        ]
        == "approval/resolved"
    )


@pytest.mark.asyncio
async def test_session_removed_marks_pending_stale_without_core_delivery() -> None:
    """session-removed 只更新 registry，不应作为模型可见消息投递。"""

    sink = AsyncMock()
    adapter, registry = _make_adapter(sink)
    await adapter._handle_runtime_event(
        _make_event(
            rpc_id="rpc-1",
            payload_type="question/requested",
            questions=[],
        )
    )

    await adapter._handle_runtime_event(
        _make_event(
            rpc_id="removed-frame",
            payload_type="host/session-removed",
            sequence=2,
        )
    )

    assert await registry.list_pending() == []
    sink.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_adapter_lifecycle_registers_before_streams_and_unloads_in_order() -> None:
    """Adapter 应先注册 listener，卸载时停流后再移除 listener。"""

    sink = AsyncMock()
    runtime = _LifecycleRuntime()
    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)
    adapter = DshTransportAdapter(
        core_sink=sink,
        runtime=runtime,
        interaction_registry=registry,
        flush_interval_seconds=60.0,
    )

    await adapter.on_adapter_loaded()
    assert runtime.calls[:3] == [
        "listener:add",
        "stream:start:mux",
        "stream:start:host",
    ]
    assert adapter.listener_id == "listener-1"

    await adapter.on_adapter_unloaded()
    assert runtime.calls[-3:] == [
        "stream:stop:mux",
        "stream:stop:host",
        "listener:remove:listener-1",
    ]
    assert runtime.client.aclose.await_count == 0
    probe = await runtime.client.call_async("host.describe", {})
    assert probe.ok is True


@pytest.mark.asyncio
async def test_adapter_load_failure_rolls_back_stream_and_listener() -> None:
    """任一 SSE 启动失败时，不得遗留 listener 或已启动的另一条流。"""

    sink = AsyncMock()
    runtime = _LifecycleRuntime()
    runtime.statuses["mux"]["running"] = False
    runtime.statuses["host"]["running"] = False
    runtime.fail_on_start = "host"
    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)
    adapter = DshTransportAdapter(
        core_sink=sink,
        runtime=runtime,
        interaction_registry=registry,
    )

    with pytest.raises(RuntimeError, match="cannot start host"):
        await adapter.on_adapter_loaded()

    assert runtime.calls == [
        "listener:add",
        "stream:start:mux",
        "stream:start:host",
        "stream:stop:mux",
        "listener:remove:listener-1",
    ]
    assert adapter.listener_id is None
    assert runtime.statuses["mux"]["running"] is False


@pytest.mark.asyncio
async def test_progress_events_are_aggregated_until_due_and_sent_as_one_summary() -> None:
    """普通进度帧在窗口内不逐条投递，到期只发送一条安全摘要。"""

    sink = AsyncMock()
    clock_value = [0.0]
    adapter, _registry = _make_adapter(sink)
    adapter = DshTransportAdapter(
        core_sink=sink,
        runtime=_RuntimeStub(),
        interaction_registry=adapter.interaction_registry,
        aggregation_window_seconds=2.0,
        clock=lambda: clock_value[0],
    )

    await adapter._handle_runtime_event(_make_progress_event(sequence=1))
    await adapter._handle_runtime_event(
        _make_progress_event(sequence=2, event_type="tool/call")
    )
    assert sink.send.await_count == 0

    clock_value[0] = 2.0
    assert await adapter.flush_due() == 1
    assert sink.send.await_count == 1
    envelope = sink.send.await_args.args[0]
    assert "token" not in envelope["message_segment"][0]["data"]
    assert "DSH 进度摘要" in envelope["message_segment"][0]["data"]


@pytest.mark.asyncio
async def test_turn_end_delivers_summary_before_completion() -> None:
    """turn/end 应先投递待聚合摘要，再投递回合结束消息。"""

    sink = AsyncMock()
    adapter, _registry = _make_adapter(sink)
    await adapter._handle_runtime_event(_make_progress_event(sequence=1))
    await adapter._handle_runtime_event(
        _make_event(
            rpc_id="turn-end-frame",
            payload_type="session/event",
            sequence=2,
            event={
                "type": "turn/end",
                "seq": 2,
                "data": {"reason": {"kind": "completed"}},
            },
        )
    )

    assert sink.send.await_count == 2
    summary = sink.send.await_args_list[0].args[0]
    completion = sink.send.await_args_list[1].args[0]
    assert summary["raw_message"]["type"] == "dsh-progress-summary"
    assert (
        completion["message_info"]["extra"]["dsh_session_event_type"]
        == "turn/end"
    )
    assert await adapter.flush_due(force=True) == 0


@pytest.mark.asyncio
async def test_health_check_and_reconnect_only_restart_missing_stream() -> None:
    """健康检查要求 host 与两流同时正常，重连只启动断开的流。"""

    sink = AsyncMock()
    runtime = _LifecycleRuntime()
    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)
    adapter = DshTransportAdapter(
        core_sink=sink,
        runtime=runtime,
        interaction_registry=registry,
        flush_interval_seconds=60.0,
    )
    adapter._listener_id = "listener-keep"

    assert await adapter.health_check() is True
    runtime.statuses["mux"]["running"] = False
    assert await adapter.health_check() is False

    await adapter.reconnect()
    assert runtime.calls == ["stream:start:mux"]
    assert adapter.listener_id == "listener-keep"
    assert runtime.client.aclose.await_count == 0
    assert await adapter.get_bot_info() == {
        "bot_id": "mofox",
        "bot_name": "Neo-MoFox",
        "platform": "dsh",
    }


@pytest.mark.asyncio
async def test_health_check_rejects_business_and_transport_failures() -> None:
    """host.describe 业务失败或传输异常均不得被当作健康状态。"""

    sink = AsyncMock()
    runtime = _LifecycleRuntime()
    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)
    adapter = DshTransportAdapter(
        core_sink=sink,
        runtime=runtime,
        interaction_registry=registry,
    )
    runtime.client.call_async.return_value = DshRpcResult(
        rpc_id="host-rpc",
        ok=False,
        error={"code": "unavailable"},
    )
    assert await adapter.health_check() is False

    runtime.client.call_async.side_effect = RuntimeError("connection lost")
    assert await adapter.health_check() is False


@pytest.mark.asyncio
async def test_regular_outgoing_message_reports_explicit_failure() -> None:
    """Task 6 前普通文本出站必须失败，不能伪造平台发送成功。"""

    sink = AsyncMock()
    adapter, _registry = _make_adapter(sink)

    result = await adapter._send_platform_message({"message_info": {}})

    assert result.success is False
    assert result.error is not None


@pytest.mark.asyncio
async def test_outgoing_text_prompts_idle_dsh_session() -> None:
    """空闲 DSH 私聊流的文本回复应转成 session.prompt。"""

    sink = AsyncMock()
    runtime = _LifecycleRuntime()
    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)
    adapter = DshTransportAdapter(
        core_sink=sink,
        runtime=runtime,
        interaction_registry=registry,
    )
    runtime.client.call_async.return_value = DshRpcResult(
        rpc_id="prompt-rpc",
        ok=True,
        value={"queued": True},
    )

    result = await adapter._send_platform_message(_make_outgoing_envelope())

    assert result.success is True
    runtime.client.call_async.assert_awaited_once_with(
        "session.prompt",
        {
            "sessionId": "session-1",
            "mode": "queue",
            "content": [{"type": "text", "text": "继续执行并汇报结果"}],
        },
    )


@pytest.mark.asyncio
async def test_outgoing_text_is_blocked_when_session_has_pending_interaction() -> None:
    """存在 pending question 时，普通文本不得绕过 dsh_respond。"""

    sink = AsyncMock()
    runtime = _LifecycleRuntime()
    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)
    adapter = DshTransportAdapter(
        core_sink=sink,
        runtime=runtime,
        interaction_registry=registry,
    )
    await adapter._handle_runtime_event(
        _make_event(
            rpc_id="question-rpc",
            payload_type="question/requested",
            questions=[],
        )
    )
    runtime.client.call_async.reset_mock()

    result = await adapter._send_platform_message(_make_outgoing_envelope())

    assert result.success is False
    assert "dsh_respond" in (result.error or "")
    runtime.client.call_async.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("envelope", "expected_error"),
    [
        (_make_outgoing_envelope(platform="qq"), "platform 必须为 dsh"),
        (
            {
                "direction": "outgoing",
                "message_info": {"platform": "dsh", "message_id": "outgoing-1"},
                "message_segment": [{"type": "text", "data": "hello"}],
            },
            "缺少私聊 user_info",
        ),
        (
            _make_outgoing_envelope(segments=[{"type": "image", "data": "base64"}]),
            "不支持 segment",
        ),
        (
            _make_outgoing_envelope(segments=[{"type": "reply", "data": "old"}]),
            "至少一个 text segment",
        ),
    ],
)
async def test_outgoing_text_rejects_invalid_target_or_segments(
    envelope: dict[str, Any],
    expected_error: str,
) -> None:
    """非 DSH 私聊、缺目标和媒体/空文本段必须在调用 RPC 前拒绝。"""

    sink = AsyncMock()
    runtime = _LifecycleRuntime()
    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)
    adapter = DshTransportAdapter(
        core_sink=sink,
        runtime=runtime,
        interaction_registry=registry,
    )

    result = await adapter._send_platform_message(envelope)

    assert result.success is False
    assert expected_error in (result.error or "")
    runtime.client.call_async.assert_not_awaited()


@pytest.mark.asyncio
async def test_outgoing_text_maps_dsh_business_failure_to_send_result() -> None:
    """DSH RPC 业务错误必须保留回执并使用稳定的 code/message 错误文本。"""

    sink = AsyncMock()
    runtime = _LifecycleRuntime()
    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)
    adapter = DshTransportAdapter(
        core_sink=sink,
        runtime=runtime,
        interaction_registry=registry,
    )
    runtime.client.call_async.return_value = DshRpcResult(
        rpc_id="prompt-rpc",
        ok=False,
        error={"code": "session-busy", "message": "waiting for tool"},
    )

    result = await adapter._send_platform_message(_make_outgoing_envelope())

    assert result.success is False
    assert result.error == "session-busy: waiting for tool"
    assert result.response == {
        "rpc_id": "prompt-rpc",
        "ok": False,
        "error": {"code": "session-busy", "message": "waiting for tool"},
    }


@pytest.mark.asyncio
async def test_outgoing_text_uses_stable_defaults_for_incomplete_dsh_error() -> None:
    """DSH 省略 code/message 时应使用约定的稳定错误默认值。"""

    sink = AsyncMock()
    runtime = _LifecycleRuntime()
    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)
    adapter = DshTransportAdapter(
        core_sink=sink,
        runtime=runtime,
        interaction_registry=registry,
    )
    runtime.client.call_async.return_value = DshRpcResult(
        rpc_id="prompt-rpc",
        ok=False,
        error={},
    )

    result = await adapter._send_platform_message(_make_outgoing_envelope())

    assert result.success is False
    assert result.error == "unknown-error: DSH session.prompt failed"


@pytest.mark.asyncio
async def test_outgoing_text_maps_transport_exception_to_failure() -> None:
    """session.prompt 传输异常必须被转成明确失败，不得向核心抛出。"""

    sink = AsyncMock()
    runtime = _LifecycleRuntime()
    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)
    adapter = DshTransportAdapter(
        core_sink=sink,
        runtime=runtime,
        interaction_registry=registry,
    )
    runtime.client.call_async.side_effect = RuntimeError("network down")

    result = await adapter._send_platform_message(_make_outgoing_envelope())

    assert result.success is False
    assert "transport failure: network down" in (result.error or "")