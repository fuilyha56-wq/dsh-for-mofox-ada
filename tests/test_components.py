"""测试 DSH Adapter 的 LLM 组件。"""

from __future__ import annotations

import json
from typing import Any

import pytest

from plugins.dsh_adapter.components import (
    DshAdapterCommand,
    DshInteractionResponseAction,
    DshModelSwitchAction,
    DshOperateAction,
    DshPresetSwitchAction,
    DshQueryTool,
    DshRpcAction,
)
from plugins.dsh_adapter.config import DshBridgeConfig
from plugins.dsh_adapter.interactions import DshPendingInteraction


class RecordingDispatcher:
    """记录组件发出的统一操作调用。"""

    def __init__(self) -> None:
        """初始化调用记录。"""

        self.calls: list[tuple[str, dict[str, Any] | None]] = []

    async def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """记录并回显操作。"""

        self.calls.append((operation, parameters))
        if operation in {"model_switch", "preset_switch", "rpc_call"}:
            return {"operation": operation, "result": {"ok": True, "value": parameters}}
        return {"operation": operation, "result": parameters}


class FakePlugin:
    """提供组件测试需要的最小插件状态。"""

    def __init__(self) -> None:
        """初始化配置与记录分派器。"""

        self.config = DshBridgeConfig()
        self.dispatcher = RecordingDispatcher()


class RecordingResponder:
    """记录结构化交互 responder 调用。"""

    def __init__(self) -> None:
        """初始化调用记录。"""

        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    async def respond_question(
        self, rpc_id: str, answers: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """记录问题回答。"""

        self.calls.append(("question", (rpc_id, answers), {}))
        return {"accepted": True, "rpc_id": rpc_id}

    async def cancel_question(self, rpc_id: str) -> dict[str, Any]:
        """记录问题取消。"""

        self.calls.append(("cancel", (rpc_id,), {}))
        return {"accepted": True, "rpc_id": rpc_id}

    async def respond_approval(
        self, rpc_id: str, outcome: str, *, actor: str
    ) -> dict[str, Any]:
        """记录审批响应及固定 actor。"""

        self.calls.append(("approval", (rpc_id, outcome), {"actor": actor}))
        return {"accepted": True, "rpc_id": rpc_id}


class RecordingRegistry:
    """按 rpc ID 返回测试 pending 交互。"""

    def __init__(self, interactions: dict[str, DshPendingInteraction]) -> None:
        """保存测试 pending 交互。"""

        self.interactions = interactions

    async def get_pending(self, rpc_id: str) -> DshPendingInteraction:
        """取得指定 pending 交互。"""

        return self.interactions[rpc_id]


class DshInteractionPlugin(FakePlugin):
    """为 dsh_respond 提供 responder 与 registry 的测试插件。"""

    def __init__(self, interactions: dict[str, DshPendingInteraction]) -> None:
        """初始化交互相关测试依赖。"""

        super().__init__()
        self.interaction_registry = RecordingRegistry(interactions)
        self.interaction_responder = RecordingResponder()


class DshChatStream:
    """模拟带最新 DSH 入站消息的 ChatStream。"""

    def __init__(self, session_id: str) -> None:
        """构造 private dsh 流。"""

        self.platform = "dsh"
        self.context = type(
            "Context", (), {"current_message": type("Message", (), {"sender_id": session_id})()}
        )()


def _pending_question(rpc_id: str, session_id: str) -> DshPendingInteraction:
    """构造 Action 测试使用的 pending question。"""

    return DshPendingInteraction(
        rpc_id=rpc_id,
        session_id=session_id,
        kind="question",
        payload={"type": "question/requested", "sessionId": session_id, "questions": []},
        stream="mux",
        first_seen_at="2026-08-15T00:00:00+00:00",
        last_seen_at="2026-08-15T00:00:00+00:00",
    )


@pytest.mark.asyncio
async def test_query_tool_rejects_side_effecting_operation() -> None:
    """只读 Tool 不得成为执行 CLI 或写入进程的旁路。"""

    plugin = FakePlugin()
    tool = DshQueryTool(plugin)  # type: ignore[arg-type]
    success, result = await tool.execute(
        "cli_run",
        '{"arguments":["--version"]}',
    )

    assert success is False
    assert "不允许" in str(result)
    assert plugin.dispatcher.calls == []


@pytest.mark.asyncio
async def test_operate_action_preserves_arbitrary_operation_json() -> None:
    """完整 Action 应将任意受支持操作的 JSON 参数原样传给分派器。"""

    plugin = FakePlugin()
    action = DshOperateAction(object(), plugin)  # type: ignore[arg-type]
    parameters = {
        "method": "session.selectModel",
        "payload": {
            "sessionId": "session-1",
            "provider": "deepseek-official",
            "model": "deepseek-v4-pro",
        },
    }
    success, rendered = await action.execute(
        "rpc_call",
        json.dumps(parameters),
    )

    assert success is True
    assert plugin.dispatcher.calls == [("rpc_call", parameters)]
    assert "session.selectModel" in rendered


@pytest.mark.asyncio
async def test_model_switch_action_uses_dedicated_operation() -> None:
    """专用模型 Action 应使用结构化 model_switch，而不是让模型拼 RPC。"""

    plugin = FakePlugin()
    action = DshModelSwitchAction(object(), plugin)  # type: ignore[arg-type]
    success, rendered = await action.execute(
        "session-1",
        "deepseek-v4-flash",
        reasoning_effort="high",
    )

    assert success is True
    assert plugin.dispatcher.calls == [
        (
            "model_switch",
            {
                "session_id": "session-1",
                "model": "deepseek-v4-flash",
                "reasoning_effort": "high",
            },
        )
    ]
    assert "deepseek-v4-flash" in rendered


@pytest.mark.asyncio
async def test_preset_switch_action_uses_dedicated_operation() -> None:
    """专用模式 Action 应使用结构化 preset_switch。"""

    plugin = FakePlugin()
    action = DshPresetSwitchAction(object(), plugin)  # type: ignore[arg-type]
    success, rendered = await action.execute("session-1", "PTC 模式")

    assert success is True
    assert plugin.dispatcher.calls == [
        (
            "preset_switch",
            {"session_id": "session-1", "preset": "PTC 模式"},
        )
    ]
    assert "PTC 模式" in rendered


@pytest.mark.asyncio
async def test_rpc_action_preserves_any_method_and_payload() -> None:
    """专用 RPC Action 不应硬编码方法白名单或改写业务 payload。"""

    plugin = FakePlugin()
    action = DshRpcAction(object(), plugin)  # type: ignore[arg-type]
    success, rendered = await action.execute(
        "future.domainAction",
        '{"nested":{"enabled":true}}',
    )

    assert success is True
    assert plugin.dispatcher.calls == [
        (
            "rpc_call",
            {
                "method": "future.domainAction",
                "payload": {"nested": {"enabled": True}},
            },
        )
    ]
    assert "future.domainAction" in rendered


@pytest.mark.asyncio
async def test_dsh_respond_action_cannot_cross_dsh_sessions() -> None:
    """Action 只能回应当前 DSH 私聊流所属 session 的 pending rpcId。"""

    plugin = DshInteractionPlugin(
        {
            "rpc-1": _pending_question("rpc-1", "session-1"),
            "rpc-2": _pending_question("rpc-2", "session-2"),
        }
    )
    action = DshInteractionResponseAction(DshChatStream("session-1"), plugin)  # type: ignore[arg-type]

    success, _ = await action.execute(
        "rpc-1", "answer", '[{"id":"language","selected":["Python"]}]'
    )
    cross_success, cross_result = await action.execute("rpc-2", "cancel")

    assert success is True
    assert cross_success is False
    assert "当前 DSH session" in cross_result
    assert plugin.interaction_responder.calls == [
        ("question", ("rpc-1", [{"id": "language", "selected": ["Python"]}]), {})
    ]


@pytest.mark.asyncio
async def test_dsh_respond_action_uses_bot_actor_for_approval() -> None:
    """LLM Action 的审批调用者必须固定为 bot。"""

    interaction = DshPendingInteraction(
        rpc_id="approval-rpc",
        session_id="session-1",
        kind="approval",
        payload={
            "type": "approval/requested",
            "sessionId": "session-1",
            "approvalId": "approval-1",
        },
        stream="mux",
        first_seen_at="2026-08-15T00:00:00+00:00",
        last_seen_at="2026-08-15T00:00:00+00:00",
        approval_id="approval-1",
    )
    plugin = DshInteractionPlugin({"approval-rpc": interaction})
    action = DshInteractionResponseAction(DshChatStream("session-1"), plugin)  # type: ignore[arg-type]

    success, _ = await action.execute("approval-rpc", "approve")

    assert success is True
    assert plugin.interaction_responder.calls == [
        ("approval", ("approval-rpc", "allowed-once"), {"actor": "bot"})
    ]


@pytest.mark.asyncio
async def test_owner_command_uses_owner_actor_for_approval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Owner 命令的 allow 映射必须固定使用 owner，而不能伪装成 bot。"""

    sent: list[str] = []

    async def _send_text(text: str, **_kwargs: Any) -> bool:
        """记录命令回包。"""

        sent.append(text)
        return True

    monkeypatch.setattr("plugins.dsh_adapter.components.send_text", _send_text)
    plugin = DshInteractionPlugin({})
    command = DshAdapterCommand(plugin, "owner-stream")  # type: ignore[arg-type]

    success, _ = await command.handle_respond_approval("approval-rpc", "allow")

    assert success is True
    assert plugin.interaction_responder.calls == [
        ("approval", ("approval-rpc", "allowed-once"), {"actor": "owner"})
    ]
    assert len(sent) == 1
