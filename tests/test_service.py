"""测试 DSH Adapter 的交互 Service API。"""

from __future__ import annotations

from typing import Any

import pytest

from plugins.dsh_adapter.interactions import DshPendingInteraction
from plugins.dsh_adapter.service import DshAdapterService


class _Registry:
    """返回静态 pending 交互列表的测试 registry。"""

    def __init__(self, items: list[DshPendingInteraction]) -> None:
        """初始化记录。"""

        self.items = items
        self.session_filters: list[str | None] = []

    async def list_pending(self, session_id: str | None = None) -> list[DshPendingInteraction]:
        """记录过滤条件并返回交互。"""

        self.session_filters.append(session_id)
        return self.items


class _Responder:
    """记录 Service 发出的结构化交互响应。"""

    def __init__(self) -> None:
        """初始化调用记录。"""

        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    async def respond_question(
        self, rpc_id: str, answers: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """记录问题回答。"""

        self.calls.append(("answer", (rpc_id, answers), {}))
        return {"accepted": True}

    async def cancel_question(self, rpc_id: str) -> dict[str, Any]:
        """记录问题取消。"""

        self.calls.append(("cancel", (rpc_id,), {}))
        return {"accepted": True}

    async def respond_approval(
        self, rpc_id: str, outcome: str, *, actor: str
    ) -> dict[str, Any]:
        """记录审批 actor。"""

        self.calls.append(("approval", (rpc_id, outcome), {"actor": actor}))
        return {"accepted": True}


class _Plugin:
    """提供 Service 所需的最小插件属性。"""

    def __init__(self, registry: _Registry, responder: _Responder) -> None:
        """初始化交互依赖。"""

        self.interaction_registry = registry
        self.interaction_responder = responder


def _pending() -> DshPendingInteraction:
    """构造不含敏感字段的 pending 记录。"""

    return DshPendingInteraction(
        rpc_id="question-rpc",
        session_id="session-1",
        kind="question",
        payload={"type": "question/requested", "sessionId": "session-1"},
        stream="mux",
        first_seen_at="2026-08-15T00:00:00+00:00",
        last_seen_at="2026-08-15T00:00:00+00:00",
    )


@pytest.mark.asyncio
async def test_service_lists_safe_pending_records_and_fixes_service_actor() -> None:
    """Service 应返回审计字段，并固定审批 actor 为 service。"""

    registry = _Registry([_pending()])
    responder = _Responder()
    service = DshAdapterService(_Plugin(registry, responder))  # type: ignore[arg-type]

    pending = await service.list_pending("session-1")
    result = await service.answer_question(
        "question-rpc", [{"id": "language", "selected": ["Python"]}]
    )
    approval = await service.respond_approval("approval-rpc", "rejected")

    assert pending == [
        {
            "rpc_id": "question-rpc",
            "session_id": "session-1",
            "kind": "question",
            "state": "pending",
            "approval_id": None,
        }
    ]
    assert registry.session_filters == ["session-1"]
    assert result == {"accepted": True}
    assert approval == {"accepted": True}
    assert responder.calls == [
        ("answer", ("question-rpc", [{"id": "language", "selected": ["Python"]}]), {}),
        ("approval", ("approval-rpc", "rejected"), {"actor": "service"}),
    ]