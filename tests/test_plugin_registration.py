"""测试 DSH Adapter 的 interaction 配置与组件注册。"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from plugins.dsh_adapter.adapter import DshTransportAdapter
from plugins.dsh_adapter.components import DshInteractionResponseAction
from plugins.dsh_adapter.config import DshBridgeConfig
from plugins.dsh_adapter.plugin import DshAdapterPlugin


def test_interaction_config_has_safe_defaults_and_rejects_invalid_modes() -> None:
    """interaction 默认值和枚举约束必须符合传输桥协议。"""

    config = DshBridgeConfig()

    assert config.interaction.enabled is True
    assert config.interaction.approval_policy == "ask"
    assert config.interaction.progress_delivery == "aggregate"
    assert config.interaction.progress_window_seconds == 2.0
    assert config.interaction.max_event_text_characters == 12000
    assert config.interaction.persist_pending_requests is True
    with pytest.raises(ValidationError):
        DshBridgeConfig(interaction={"approval_policy": "always-allow"})
    with pytest.raises(ValidationError):
        DshBridgeConfig(interaction={"progress_delivery": "raw"})


def test_interaction_registration_requires_event_stream_ownership() -> None:
    """交互关闭或禁用 SSE 时不得注册 Adapter 和 dsh_respond。"""

    enabled = DshAdapterPlugin(DshBridgeConfig())
    enabled_components = enabled.get_components()
    assert DshTransportAdapter in enabled_components
    assert DshInteractionResponseAction in enabled_components

    disabled = DshBridgeConfig()
    disabled.interaction.enabled = False
    disabled_components = DshAdapterPlugin(disabled).get_components()
    assert DshTransportAdapter not in disabled_components
    assert DshInteractionResponseAction not in disabled_components

    no_streams = DshBridgeConfig()
    no_streams.bridge.start_event_streams = False
    no_stream_components = DshAdapterPlugin(no_streams).get_components()
    assert DshTransportAdapter not in no_stream_components
    assert DshInteractionResponseAction not in no_stream_components


def test_manifest_registers_adapter_and_response_action() -> None:
    """发布清单必须发现原生 Adapter 和受限回应 Action。"""

    manifest_path = Path(__file__).resolve().parents[1] / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    includes = {
        (entry["component_type"], entry["component_name"])
        for entry in manifest["include"]
    }

    assert manifest["version"] == "1.0.0"
    assert len(manifest["include"]) == 12
    assert ("adapter", "dsh_adapter") in includes
    assert ("action", "dsh_respond") in includes
    assert all("websockets" not in item for item in manifest["python_dependencies"])


def test_dsh_bundle_metadata_registers_the_package_patch() -> None:
    """DSH Marketplace 所需的 bundle 清单必须指向可装配的同名 package。"""

    root = Path(__file__).resolve().parents[1]
    package = json.loads((root / "package.json").read_text(encoding="utf-8"))
    patch = (root / "cordis.patch.yml").read_text(encoding="utf-8")

    assert package["name"] == "dsh-for-mofox-ada"
    assert package["version"] == "1.0.0"
    assert package["type"] == "module"
    assert package["main"] == "index.js"
    assert package["dsh"]["bundle"] == {"patch": "./cordis.patch.yml"}
    assert {"index.js", "cordis.patch.yml"}.issubset(package["files"])
    assert "name: dsh-for-mofox-ada" in patch


@pytest.mark.asyncio
async def test_plugin_unload_stops_native_adapter_before_runtime_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PluginManager 卸载时必须先停 Adapter，避免关闭共享 Runtime 后遗留监听器。"""

    plugin = DshAdapterPlugin(DshBridgeConfig())
    calls: list[str] = []

    async def _stop_adapter(signature: str) -> bool:
        """记录 Adapter 停止调用。"""

        calls.append(f"stop:{signature}")
        return True

    async def _close_runtime() -> None:
        """记录 Runtime 关闭调用。"""

        calls.append("runtime:close")

    monkeypatch.setattr("plugins.dsh_adapter.plugin.stop_adapter", _stop_adapter)
    plugin.runtime.close = AsyncMock(side_effect=_close_runtime)

    await plugin.on_plugin_unloaded()

    assert calls == [
        "stop:dsh_adapter:adapter:dsh_adapter",
        "runtime:close",
    ]