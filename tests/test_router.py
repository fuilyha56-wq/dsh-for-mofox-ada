"""测试 DSH Adapter 的 HTTP Router。"""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from plugins.dsh_adapter.config import DshBridgeConfig
from plugins.dsh_adapter.router import DshAdapterRouter


class RecordingDispatcher:
    """记录 Router 转发的操作。"""

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
        return {"operation": operation, "result": parameters}


class FakePlugin:
    """提供 Router 测试需要的最小插件状态。"""

    def __init__(self, token: str = "") -> None:
        """初始化配置与记录分派器。"""

        self.config = DshBridgeConfig()
        self.config.router.shared_token = token
        self.dispatcher = RecordingDispatcher()


def test_router_requires_configured_token_even_for_loopback_client() -> None:
    """显式配置令牌后，本地或代理请求也必须携带正确令牌。"""

    plugin = FakePlugin(token="bridge-secret")
    client = TestClient(DshAdapterRouter(plugin).app)  # type: ignore[arg-type]

    assert client.get("/operations").status_code == 403
    assert (
        client.get(
            "/operations",
            headers={"X-DSH-Bridge-Token": "wrong"},
        ).status_code
        == 403
    )
    response = client.get(
        "/operations",
        headers={"X-DSH-Bridge-Token": "bridge-secret"},
    )
    assert response.status_code == 200
    assert "rpc_call" in response.json()["operations"]


def test_execute_endpoint_forwards_structured_operation() -> None:
    """通用 execute 端点应原样转发操作和参数对象。"""

    plugin = FakePlugin()
    client = TestClient(DshAdapterRouter(plugin).app)  # type: ignore[arg-type]
    payload = {
        "operation": "process_start",
        "parameters": {
            "process_id": "web",
            "arguments": ["--profile", "web", "--port", "0"],
        },
    }
    response = client.post("/execute", json=payload)

    assert response.status_code == 200
    assert plugin.dispatcher.calls == [
        ("process_start", payload["parameters"])
    ]
