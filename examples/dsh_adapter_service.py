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

    async def list_pending(
        self, session_id: str | None = None
    ) -> list[dict[str, Any]]:
        """列出待处理的 DSH Web 交互。"""

    async def answer_question(
        self,
        rpc_id: str,
        answers: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """提交结构化问题答案。"""

    async def cancel_question(self, rpc_id: str) -> dict[str, Any]:
        """取消待处理问题。"""

    async def respond_approval(
        self,
        rpc_id: str,
        outcome: str,
    ) -> dict[str, Any]:
        """以 Service 身份回应一次审批。"""


async def main() -> None:
    """加载插件、列出会话和仅读取 pending 交互。"""

    init_core_config(str(REPO_ROOT / "config" / "core.toml"))
    await load_all_plugins(str(REPO_ROOT / "plugins"))
    raw_service = service_api.get_service("dsh_adapter:service:dsh_adapter")
    if raw_service is None:
        raise RuntimeError("dsh_adapter Service 未注册")
    service = cast(DshAdapterServiceProtocol, raw_service)
    sessions = await service.list_sessions()
    pending = await service.list_pending()
    print({"sessions": sessions, "pending": pending})

    # 仅在调用方已向用户展示题面、获得明确答案并确认 rpc_id 对应目标 session 后调用：
    # result = await service.answer_question(
    #     "QUESTION_RPC_ID", [{"id": "language", "selected": ["Python"]}]
    # )
    # approval = await service.respond_approval("APPROVAL_RPC_ID", "rejected")


if __name__ == "__main__":
    asyncio.run(main())