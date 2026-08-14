"""测试 DSH Adapter 的 LLM 组件。"""

from __future__ import annotations

import json
from typing import Any

import pytest

from plugins.dsh_adapter.components import (
    DshModelSwitchAction,
    DshOperateAction,
    DshPresetSwitchAction,
    DshQueryTool,
    DshRpcAction,
)
from plugins.dsh_adapter.config import DshBridgeConfig


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
