"""DSH 结构化交互响应事务与审批策略测试。"""

from __future__ import annotations

from typing import Any

import pytest

from plugins.dsh_adapter.adapter import DshInteractionResponder
from plugins.dsh_adapter.interactions import DshInteractionRegistry, DshPendingInteraction


async def _load_empty(_store: str, _name: str) -> dict[str, Any] | None:
    """返回空的测试持久化内容。"""

    return None


async def _save_memory(
    _store: str,
    _name: str,
    _data: dict[str, Any],
) -> None:
    """接受测试 registry 的持久化写入。"""


class _RespondClient:
    """记录 respond 调用并按队列返回或抛出预设结果。"""

    def __init__(self, outcomes: list[dict[str, Any] | Exception]) -> None:
        self.outcomes = outcomes
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def respond(self, rpc_id: str, result: dict[str, Any]) -> dict[str, Any]:
        """记录一条 response 并消费下一个预设结果。"""

        self.calls.append((rpc_id, result))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class _RuntimeStub:
    """向 responder 提供最小 client 容器。"""

    def __init__(self, client: _RespondClient) -> None:
        self.client = client


def _make_question() -> DshPendingInteraction:
    """构造一条具备单选题面的 pending question。"""

    return DshPendingInteraction(
        rpc_id="question-rpc",
        session_id="session-1",
        kind="question",
        payload={
            "type": "question/requested",
            "sessionId": "session-1",
            "questions": [
                {
                    "id": "language",
                    "question": "选择语言",
                    "options": [{"label": "Python"}],
                }
            ],
        },
        stream="mux",
        first_seen_at="2026-08-15T00:00:00+00:00",
        last_seen_at="2026-08-15T00:00:00+00:00",
    )


def _make_approval() -> DshPendingInteraction:
    """构造一条 approval pending 记录。"""

    return DshPendingInteraction(
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


@pytest.mark.asyncio
async def test_response_consumes_pending_only_after_dsh_accepts() -> None:
    """网络失败回滚 pending；仅 accepted=true 后将 question 标为 resolved。"""

    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)
    await registry.upsert(_make_question())
    client = _RespondClient([RuntimeError("network down"), {"accepted": True}])
    responder = DshInteractionResponder(_RuntimeStub(client), registry)
    answers = [{"id": "language", "selected": ["Python"]}]

    with pytest.raises(RuntimeError, match="network down"):
        await responder.respond_question("question-rpc", answers)
    assert (await registry.get_pending("question-rpc")).state == "pending"

    result = await responder.respond_question("question-rpc", answers)

    assert result == {
        "rpc_id": "question-rpc",
        "session_id": "session-1",
        "kind": "question",
        "accepted": True,
        "state": "resolved",
        "receipt": {"accepted": True},
    }
    with pytest.raises(KeyError, match="question-rpc"):
        await registry.get_pending("question-rpc")


@pytest.mark.asyncio
async def test_rejected_receipt_retries_or_marks_stale_by_reason() -> None:
    """bad response 回滚 pending；not-pending 则标记 stale。"""

    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)
    await registry.upsert(_make_question())
    client = _RespondClient(
        [{"accepted": False, "reason": "bad-response"}, {"accepted": False, "reason": "not-pending"}]
    )
    responder = DshInteractionResponder(_RuntimeStub(client), registry)
    answers = [{"id": "language", "selected": ["Python"]}]

    with pytest.raises(ValueError, match="未接受"):
        await responder.respond_question("question-rpc", answers)
    assert (await registry.get_pending("question-rpc")).state == "pending"

    result = await responder.respond_question("question-rpc", answers)
    assert result["accepted"] is False
    assert result["state"] == "stale"
    with pytest.raises(KeyError, match="question-rpc"):
        await registry.get_pending("question-rpc")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("policy", "actor", "outcome", "allowed"),
    [
        ("ask", "bot", "allowed-once", False),
        ("ask", "bot", "rejected", True),
        ("ask", "owner", "allowed-once", True),
        ("ask", "service", "allowed-once", False),
        ("autonomous", "bot", "allowed-once", True),
        ("autonomous", "service", "allowed-once", True),
        ("reject", "bot", "allowed-once", False),
    ],
)
async def test_approval_policy_controls_allowed_once(
    policy: str,
    actor: str,
    outcome: str,
    allowed: bool,
) -> None:
    """ask/autonomous/reject 的 actor 授权必须符合审批策略表。"""

    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)
    await registry.upsert(_make_approval())
    client = _RespondClient([{"accepted": True}])
    responder = DshInteractionResponder(
        _RuntimeStub(client),
        registry,
        approval_policy=policy,  # type: ignore[arg-type]
    )

    if not allowed:
        with pytest.raises(PermissionError):
            await responder.respond_approval(
                "approval-rpc",
                outcome,
                actor=actor,  # type: ignore[arg-type]
            )
        assert client.calls == []
        return

    result = await responder.respond_approval(
        "approval-rpc",
        outcome,
        actor=actor,  # type: ignore[arg-type]
    )
    assert result["accepted"] is True
    assert client.calls[0][1]["value"]["outcome"] == outcome


@pytest.mark.asyncio
async def test_reject_policy_auto_rejects_once_without_replay() -> None:
    """reject 策略应自动拒绝一次，重放同 rpcId 不得再次发送。"""

    registry = DshInteractionRegistry(load_func=_load_empty, save_func=_save_memory)
    await registry.upsert(_make_approval())
    client = _RespondClient([{"accepted": True}])
    responder = DshInteractionResponder(
        _RuntimeStub(client), registry, approval_policy="reject"
    )

    first = await responder.auto_reject_approval("approval-rpc")
    second = await responder.auto_reject_approval("approval-rpc")

    assert first is not None
    assert first["accepted"] is True
    assert second is None
    assert len(client.calls) == 1
    assert client.calls[0][1]["value"]["outcome"] == "rejected"