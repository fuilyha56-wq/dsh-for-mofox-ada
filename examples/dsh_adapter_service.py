"""通过 Neo-MoFox Service API 调用 DSH Adapter。"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any, Protocol, cast

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PLUGIN_ROOT.parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.app.plugin_system.api import service_api  # noqa: E402
from src.core.components.loader import load_all_plugins  # noqa: E402
from src.core.config import init_core_config  # noqa: E402


class DshAdapterServiceProtocol(Protocol):
    """示例所需的 DSH Adapter Service 最小接口。"""

    async def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """执行一个统一桥接操作。"""

    async def list_sessions(self) -> dict[str, Any]:
        """列出 DSH 会话。"""

    async def switch_model(
        self,
        session_id: str,
        model: str,
        *,
        reasoning_effort: str | None = None,
        provider: str | None = None,
    ) -> dict[str, Any]:
        """切换指定 DSH 会话模型。"""

    async def list_presets(self) -> dict[str, Any]:
        """列出 DSH Agent preset 模式。"""

    async def switch_preset(
        self,
        session_id: str,
        preset: str,
    ) -> dict[str, Any]:
        """切换指定空白 DSH 会话模式。"""


async def main() -> None:
    """加载插件并调用真实 DSH host.describe。"""

    init_core_config(str(REPO_ROOT / "config" / "core.toml"))
    await load_all_plugins(str(REPO_ROOT / "plugins"))
    raw_service = service_api.get_service("dsh_adapter:service:dsh_adapter")
    if raw_service is None:
        raise RuntimeError("dsh_adapter Service 未注册")
    service = cast(DshAdapterServiceProtocol, raw_service)
    sessions = await service.list_sessions()
    items = sessions["result"]["value"]["items"]
    if not items:
        raise RuntimeError("DSH 当前没有可切换的会话")
    result = await service.switch_model(
        items[0]["sessionId"],
        "deepseek-v4-flash",
        reasoning_effort="high",
    )
    presets = await service.list_presets()
    print({"model": result, "presets": presets})


if __name__ == "__main__":
    asyncio.run(main())