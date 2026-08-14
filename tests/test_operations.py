"""测试 DSH Adapter 统一操作分派器。"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

from plugins.dsh_adapter.operations import DshOperationDispatcher
from plugins.dsh_adapter.runtime import DshBridgeRuntime, DshRuntimeOptions


class FakeRpcResult:
    """提供模型操作测试需要的最小 RPC 结果。"""

    def __init__(self, value: Any) -> None:
        """保存一个成功的 RPC 值。"""

        self.ok = True
        self.value = value
        self.error = None

    def to_dict(self) -> dict[str, Any]:
        """返回与真实 DSH RPC 结果一致的结构。"""

        return {"rpc_id": "test-rpc", "ok": self.ok, "value": self.value}


class ModelRpcClient:
    """记录模型与模式目录及切换 RPC 调用。"""

    def __init__(self) -> None:
        """初始化模型目录与调用记录。"""

        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.catalog = {
            "current": {
                "provider": "deepseek-official",
                "model": "deepseek-v4-pro",
            },
            "groups": [
                {
                    "id": "deepseek-official",
                    "models": [
                        {
                            "id": "deepseek-v4-flash",
                            "name": "DeepSeek-V4-Flash",
                            "reasoning": {"efforts": [{"id": "off"}, {"id": "high"}]},
                        }
                    ],
                }
            ],
        }
        self.presets = {
            "presets": [
                {
                    "id": "standard",
                    "trust": "system",
                    "isDefault": True,
                    "name": "标准模式",
                },
                {
                    "id": "code",
                    "trust": "system",
                    "isDefault": False,
                    "name": "PTC 模式",
                },
            ],
            "authorable": True,
            "hasDocument": True,
        }

    async def call_async(self, method: str, payload: dict[str, Any]) -> FakeRpcResult:
        """返回模型目录或模拟模型切换结果。"""

        self.calls.append((method, payload))
        if method in {"llm.models", "session.models"}:
            return FakeRpcResult(self.catalog)
        if method == "session.selectModel":
            return FakeRpcResult({"selected": payload})
        if method == "agentPreset.list":
            return FakeRpcResult(self.presets)
        if method == "agentPreset.select":
            return FakeRpcResult({"selected": payload})
        raise AssertionError(f"unexpected method: {method}")

    async def close(self) -> None:
        """模拟关闭 RPC 客户端。"""


def make_dispatcher(tmp_path: Path) -> DshOperationDispatcher:
    """创建以当前 Python 解释器模拟 DSH 的分派器。"""

    dsh_home = tmp_path / "dsh-home"
    dsh_home.mkdir()
    runtime = DshBridgeRuntime(
        DshRuntimeOptions(
            dsh_command=sys.executable,
            dsh_home=dsh_home,
            default_workspace=tmp_path,
            web_base_url="http://127.0.0.1:1",
            default_timeout=5.0,
            max_timeout=10.0,
        )
    )
    return DshOperationDispatcher(runtime)


@pytest.mark.asyncio
async def test_model_list_uses_host_catalog_without_session_id(tmp_path: Path) -> None:
    """不提供会话 ID 时应查询 DSH 主机级模型目录。"""

    dispatcher = make_dispatcher(tmp_path)
    client = ModelRpcClient()
    dispatcher.runtime.client = client  # type: ignore[assignment]

    response = await dispatcher.execute("model_list")
    await dispatcher.runtime.close()

    assert response["result"]["value"]["groups"][0]["models"][0]["id"] == (
        "deepseek-v4-flash"
    )
    assert client.calls == [("llm.models", {})]


@pytest.mark.asyncio
async def test_rpc_catalog_covers_every_current_dsh_method(tmp_path: Path) -> None:
    """RPC 目录必须完整覆盖当前 DSH RpcMethodMap 的 52 个方法。"""

    dispatcher = make_dispatcher(tmp_path)
    response = await dispatcher.execute("rpc_catalog")
    details = await dispatcher.execute(
        "rpc_catalog",
        {"method": "credentials.set"},
    )
    await dispatcher.runtime.close()

    expected = {
        "session.list",
        "session.search",
        "session.create",
        "session.history",
        "session.models",
        "session.selectModel",
        "session.rename",
        "session.fork",
        "session.prompt",
        "session.attachment",
        "session.updateQueue",
        "session.cancel",
        "subagent.list",
        "subagent.history",
        "subagent.prompt",
        "subagent.interrupt",
        "host.describe",
        "host.pickDirectory",
        "host.listDirectory",
        "host.createDirectory",
        "host.openPath",
        "workspace.list",
        "workspace.create",
        "workspace.rename",
        "workspace.delete",
        "workspace.insertBefore",
        "workspace.insertSessionBefore",
        "workspace.archiveSession",
        "skill.list",
        "agentPreset.list",
        "agentPreset.select",
        "agentPreset.read",
        "agentPreset.copy",
        "agentPreset.openDocument",
        "agentPreset.remove",
        "goal.create",
        "goal.edit",
        "goal.pause",
        "goal.resume",
        "goal.complete",
        "goal.clear",
        "settings.describe",
        "settings.openDocument",
        "settings.update",
        "settings.replace",
        "settings.mutate",
        "credentials.describe",
        "credentials.set",
        "credentials.unset",
        "llm.providers",
        "llm.models",
        "llm.discoverModels",
    }
    actual = {
        method
        for methods in response["result"]["domains"].values()
        for method in methods
    }
    assert response["result"]["method_count"] == 52
    assert actual == expected
    assert details["result"]["method"]["risk"] == "secret"
    assert "value" in details["result"]["method"]["payload_schema"]


@pytest.mark.asyncio
async def test_model_switch_resolves_provider_and_validates_reasoning(
    tmp_path: Path,
) -> None:
    """模型切换应自动解析 provider 并拒绝目录未声明的推理等级。"""

    dispatcher = make_dispatcher(tmp_path)
    client = ModelRpcClient()
    dispatcher.runtime.client = client  # type: ignore[assignment]

    response = await dispatcher.execute(
        "model_switch",
        {
            "session_id": "session-1",
            "model": "DeepSeek-V4-Flash",
            "reasoning_effort": " high ",
        },
    )
    assert response["result"]["value"]["selected"] == {
        "sessionId": "session-1",
        "provider": "deepseek-official",
        "model": "deepseek-v4-flash",
        "reasoningEffort": "high",
    }

    with pytest.raises(ValueError, match="不支持推理等级 impossible"):
        await dispatcher.execute(
            "model_switch",
            {
                "session_id": "session-1",
                "model": "deepseek-v4-flash",
                "reasoning_effort": "impossible",
            },
        )
    await dispatcher.runtime.close()


@pytest.mark.asyncio
async def test_preset_switch_resolves_display_name_and_uses_agent_preset_rpc(
    tmp_path: Path,
) -> None:
    """模式切换应按实时目录解析显示名并发送准确的 Agent preset ID。"""

    dispatcher = make_dispatcher(tmp_path)
    client = ModelRpcClient()
    dispatcher.runtime.client = client  # type: ignore[assignment]

    listed = await dispatcher.execute("preset_list")
    switched = await dispatcher.execute(
        "preset_switch",
        {"session_id": "session-1", "preset": "PTC 模式"},
    )
    await dispatcher.runtime.close()

    assert listed["result"]["value"]["presets"][1]["id"] == "code"
    assert switched["result"]["value"]["selected"] == {
        "sessionId": "session-1",
        "agentPreset": "code",
    }
    assert client.calls == [
        ("agentPreset.list", {}),
        ("agentPreset.list", {}),
        (
            "agentPreset.select",
            {"sessionId": "session-1", "agentPreset": "code"},
        ),
    ]


@pytest.mark.asyncio
async def test_cli_run_dispatches_complete_structured_parameters(tmp_path: Path) -> None:
    """cli_run 应保持参数、标准输入与环境变量。"""

    dispatcher = make_dispatcher(tmp_path)
    code = "import os,sys; print(sys.argv[1], os.environ['X'], sys.stdin.read())"
    response = await dispatcher.execute(
        "cli_run",
        {
            "arguments": ["-c", code, "argument value"],
            "stdin": "stdin value",
            "environment": {"X": "environment value"},
        },
    )
    await dispatcher.runtime.close()

    result = response["result"]
    assert result["exit_code"] == 0
    assert "argument value environment value stdin value" in result["stdout"]


@pytest.mark.asyncio
async def test_dispatcher_rejects_unknown_operations_and_parameters(
    tmp_path: Path,
) -> None:
    """分派器应对未知操作和拼错参数失败，而不是静默忽略。"""

    dispatcher = make_dispatcher(tmp_path)
    with pytest.raises(ValueError, match="不支持的操作"):
        await dispatcher.execute("shell", {})
    with pytest.raises(ValueError, match="未知参数"):
        await dispatcher.execute("process_list", {"limti": 1})
    await dispatcher.runtime.close()


@pytest.mark.asyncio
async def test_data_operations_preserve_binary_and_paging_fields(tmp_path: Path) -> None:
    """数据操作应返回原始 Base64 和分页字段。"""

    dispatcher = make_dispatcher(tmp_path)
    target = dispatcher.runtime.options.dsh_home / "sessions" / "one.bin"
    target.parent.mkdir()
    target.write_bytes(b"\x00\x01\x02")
    response = await dispatcher.execute(
        "data_read",
        {"path": "sessions/one.bin", "offset": 1, "limit": 2},
    )
    await dispatcher.runtime.close()

    assert response["result"]["body_base64"] == "AQI="
    assert response["result"]["next_offset"] == 3