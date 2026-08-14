"""测试 DSH Adapter 的 pending interaction registry（Task 2，第一轮：重放去重与 session 隔离）。"""

from __future__ import annotations

import json
from typing import Any

import pytest

from plugins.dsh_adapter.interactions import (
    DshInteractionRegistry,
    DshPendingInteraction,
)
from plugins.dsh_adapter.runtime import DshRuntimeEvent

RECEIVED_AT = "2026-08-15T00:00:00+00:00"


class MemoryPersistence:
    """模拟 storage_api 的内存持久化 double：读写均经过 JSON 序列化。

    数据以 ``json.loads(json.dumps(...))`` 往返，保证与真实 JSONStore 一样只接受
    JSON 可序列化内容，并让 load 返回独立深拷贝。
    """

    def __init__(self) -> None:
        self.data: dict[str, Any] | None = None
        self.fail_saves = False

    async def load(self, store_name: str, name: str) -> dict[str, Any] | None:
        """读取持久化数据；不存在时返回 None（与 storage_api.load_json 一致）。"""

        if self.data is None:
            return None
        return json.loads(json.dumps(self.data))

    async def save(self, store_name: str, name: str, data: dict[str, Any]) -> None:
        """写入持久化数据；``fail_saves`` 为真时模拟存储写入失败。"""

        if self.fail_saves:
            raise RuntimeError("模拟存储写入失败")
        self.data = json.loads(json.dumps(data))


def make_registry(persistence: MemoryPersistence) -> DshInteractionRegistry:
    """构造注入内存持久化 double 的真实 registry。"""

    return DshInteractionRegistry(load_func=persistence.load, save_func=persistence.save)


def make_question(**overrides: Any) -> DshPendingInteraction:
    """构造手写的 question 记录，支持按字段覆盖。"""

    fields: dict[str, Any] = {
        "rpc_id": "rpc-1",
        "session_id": "session-a",
        "kind": "question",
        "payload": {"type": "question/requested", "questions": []},
        "stream": "mux",
        "first_seen_at": RECEIVED_AT,
        "last_seen_at": RECEIVED_AT,
    }
    fields.update(overrides)
    return DshPendingInteraction(**fields)


def make_approval(**overrides: Any) -> DshPendingInteraction:
    """构造手写的 approval 记录，支持按字段覆盖。"""

    fields: dict[str, Any] = {
        "rpc_id": "rpc-2",
        "session_id": "session-a",
        "kind": "approval",
        "payload": {
            "type": "approval/requested",
            "sessionId": "session-a",
            "approvalId": "approval-1",
            "toolName": "bash",
            "callId": "call-1",
            "reason": "执行删除操作",
        },
        "stream": "mux",
        "first_seen_at": RECEIVED_AT,
        "last_seen_at": RECEIVED_AT,
        "approval_id": "approval-1",
    }
    fields.update(overrides)
    return DshPendingInteraction(**fields)


async def test_registry_deduplicates_replayed_rpc_id_per_session() -> None:
    """相同 rpcId 重放返回 False 且只更新 last_seen_at；不同 session 互不可见。"""

    persistence = MemoryPersistence()
    registry = make_registry(persistence)
    await registry.load()

    question = make_question()
    assert await registry.upsert(question) is True
    replay = make_question(last_seen_at="2026-08-15T00:01:00+00:00")
    assert await registry.upsert(replay) is False

    stored = await registry.get_pending("rpc-1")
    assert stored.first_seen_at == RECEIVED_AT
    assert stored.last_seen_at == "2026-08-15T00:01:00+00:00"
    assert stored.session_id == "session-a"
    assert stored.kind == "question"
    assert stored.state == "pending"
    assert stored.payload == question.payload

    assert await registry.list_pending("session-b") == []
    assert [item.rpc_id for item in await registry.list_pending("session-a")] == ["rpc-1"]


async def test_upsert_rejects_replay_that_changes_session_or_kind() -> None:
    """同 rpcId 重放改变 session 或 kind 必须拒绝，不能静默串会话。"""

    persistence = MemoryPersistence()
    registry = make_registry(persistence)
    await registry.load()
    await registry.upsert(make_question())

    with pytest.raises(ValueError, match="session"):
        await registry.upsert(make_question(session_id="session-b"))
    with pytest.raises(ValueError, match="kind"):
        await registry.upsert(make_question(kind="approval"))

    stored = await registry.get_pending("rpc-1")
    assert stored.session_id == "session-a"
    assert stored.kind == "question"
    assert await registry.list_pending("session-b") == []


async def test_load_initializes_empty_registry_when_no_data() -> None:
    """load 对不存在的数据初始化为空 registry。"""

    persistence = MemoryPersistence()
    registry = make_registry(persistence)
    await registry.load()
    assert await registry.list_pending() == []
    with pytest.raises(KeyError, match="rpc-1"):
        await registry.get_pending("rpc-1")


async def test_upsert_new_interaction_is_always_pending() -> None:
    """新记录无论传入什么 state 都以 pending 落库，保证状态机从 pending 出发。"""

    persistence = MemoryPersistence()
    registry = make_registry(persistence)
    await registry.load()
    await registry.upsert(make_question(state="responding"))
    stored = await registry.get_pending("rpc-1")
    assert stored.state == "pending"


# ===========================================================================
# 第二轮：状态转换、持久化恢复、resolved/stale 与事件提取
# ===========================================================================


async def test_save_failure_rolls_back_state_change() -> None:
    """pending -> responding -> pending 回滚：保存失败不留下已变更内存状态。"""

    persistence = MemoryPersistence()
    registry = make_registry(persistence)
    await registry.load()
    await registry.upsert(make_question())
    await registry.mark_responding("rpc-1")

    persistence.fail_saves = True
    with pytest.raises(RuntimeError, match="存储"):
        await registry.mark_pending("rpc-1")
    persistence.fail_saves = False

    # 内存状态回滚：rpc-1 仍是 responding（get_pending 因非 pending 抛 KeyError）
    with pytest.raises(KeyError, match="不是 pending"):
        await registry.get_pending("rpc-1")
    # 最近一次成功保存仍是 responding，未被失败的保存污染
    assert persistence.data is not None
    assert persistence.data["items"]["rpc-1"]["state"] == "responding"
    # 失败后可重试成功
    await registry.mark_pending("rpc-1")
    stored = await registry.get_pending("rpc-1")
    assert stored.state == "pending"


async def test_upsert_save_failure_leaves_no_in_memory_state() -> None:
    """首次 upsert 保存失败不得留下内存残留，且之后可正常重试。"""

    persistence = MemoryPersistence()
    registry = make_registry(persistence)
    await registry.load()

    persistence.fail_saves = True
    with pytest.raises(RuntimeError, match="存储"):
        await registry.upsert(make_question())
    persistence.fail_saves = False

    with pytest.raises(KeyError, match="rpc-1"):
        await registry.get_pending("rpc-1")
    assert persistence.data is None
    assert await registry.upsert(make_question()) is True


async def test_resolved_and_stale_are_excluded_from_pending_views() -> None:
    """resolved/stale 不出现在 list_pending，get_pending 对它们抛 KeyError。"""

    persistence = MemoryPersistence()
    registry = make_registry(persistence)
    await registry.load()
    await registry.upsert(make_question())  # rpc-1 -> resolved
    await registry.upsert(make_question(rpc_id="rpc-2"))  # rpc-2 -> stale
    await registry.upsert(make_question(rpc_id="rpc-3", session_id="session-b"))
    await registry.mark_resolved("rpc-1")
    await registry.mark_stale("rpc-2")

    assert {item.rpc_id for item in await registry.list_pending()} == {"rpc-3"}
    assert await registry.list_pending("session-a") == []
    with pytest.raises(KeyError, match="不是 pending"):
        await registry.get_pending("rpc-1")
    with pytest.raises(KeyError, match="不是 pending"):
        await registry.get_pending("rpc-2")


async def test_persistence_round_trip_preserves_state_and_approval_id() -> None:
    """重新构造 registry 并 load() 后状态保持，approval_id 不丢失。"""

    persistence = MemoryPersistence()
    registry = make_registry(persistence)
    await registry.load()
    await registry.upsert(make_question())
    await registry.upsert(make_approval())
    await registry.mark_responding("rpc-2")

    assert persistence.data is not None
    assert persistence.data["version"] == 1
    assert persistence.data["items"]["rpc-2"]["state"] == "responding"
    assert persistence.data["items"]["rpc-2"]["approval_id"] == "approval-1"

    restored = make_registry(persistence)
    await restored.load()
    # responding 状态在重启后保持，可回滚为 pending 再读取
    await restored.mark_pending("rpc-2")
    stored = await restored.get_pending("rpc-2")
    assert stored.kind == "approval"
    assert stored.approval_id == "approval-1"
    assert stored.session_id == "session-a"
    assert stored.stream == "mux"
    assert stored.first_seen_at == RECEIVED_AT
    assert stored.payload["approvalId"] == "approval-1"
    question = await restored.get_pending("rpc-1")
    assert question.kind == "question"
    assert question.first_seen_at == RECEIVED_AT
    assert question.payload == {"type": "question/requested", "questions": []}


async def test_resolve_question_ends_exact_question_rpc_id() -> None:
    """question/resolved 只精确结束原 question rpcId，且幂等。"""

    persistence = MemoryPersistence()
    registry = make_registry(persistence)
    await registry.load()
    await registry.upsert(make_question())  # rpc-1
    await registry.upsert(make_question(rpc_id="rpc-5"))  # rpc-5
    await registry.upsert(make_approval())  # rpc-2

    assert await registry.resolve_question("rpc-1") is True
    assert {item.rpc_id for item in await registry.list_pending()} == {"rpc-5", "rpc-2"}
    # 已 resolved 的重放返回 True（幂等，不再落盘）
    assert await registry.resolve_question("rpc-1") is True
    # 未知 rpcId 返回 False
    assert await registry.resolve_question("missing") is False
    # approval 记录不能被 resolve_question 结束
    assert await registry.resolve_question("rpc-2") is False
    assert {item.rpc_id for item in await registry.list_pending()} == {"rpc-5", "rpc-2"}


async def test_resolve_approval_matches_only_approval_id() -> None:
    """approval/resolved 只结束匹配 approvalId 的记录，不影响其他 session。"""

    persistence = MemoryPersistence()
    registry = make_registry(persistence)
    await registry.load()
    await registry.upsert(make_question())  # rpc-1
    await registry.upsert(make_approval())  # rpc-2, approval-1
    await registry.upsert(
        make_approval(
            rpc_id="rpc-6",
            session_id="session-b",
            approval_id="approval-2",
            payload={
                "type": "approval/requested",
                "sessionId": "session-b",
                "approvalId": "approval-2",
                "toolName": "bash",
                "callId": "call-2",
                "reason": "执行其他操作",
            },
        )
    )

    assert await registry.resolve_approval("approval-1") is True
    assert {item.rpc_id for item in await registry.list_pending()} == {"rpc-1", "rpc-6"}
    # 幂等重放
    assert await registry.resolve_approval("approval-1") is True
    # 未知 approvalId 返回 False；question 的 rpcId 不能当作 approvalId 使用
    assert await registry.resolve_approval("approval-missing") is False
    assert await registry.resolve_approval("rpc-1") is False
    assert {item.rpc_id for item in await registry.list_pending()} == {"rpc-1", "rpc-6"}


async def test_mark_session_stale_affects_only_that_session() -> None:
    """host/session-removed 把该 session 的 pending 标为 stale，不影响其他 session。"""

    persistence = MemoryPersistence()
    registry = make_registry(persistence)
    await registry.load()
    await registry.upsert(make_question())  # rpc-1 session-a pending
    await registry.upsert(make_question(rpc_id="rpc-2"))  # rpc-2 session-a -> responding
    await registry.upsert(make_question(rpc_id="rpc-3", session_id="session-b"))
    await registry.upsert(make_approval(rpc_id="rpc-4"))  # rpc-4 session-a approval
    await registry.mark_responding("rpc-2")

    count = await registry.mark_session_stale("session-a")
    assert count == 2  # rpc-1 与 rpc-4；responding 的 rpc-2 不受影响

    assert {item.rpc_id for item in await registry.list_pending()} == {"rpc-3"}
    with pytest.raises(KeyError, match="rpc-1"):
        await registry.get_pending("rpc-1")
    with pytest.raises(KeyError, match="rpc-4"):
        await registry.get_pending("rpc-4")
    # responding 记录未被标为 stale：仍可回滚到 pending
    await registry.mark_pending("rpc-2")
    assert {item.rpc_id for item in await registry.list_pending()} == {"rpc-2", "rpc-3"}
    # 回滚后的 rpc-2 重新成为 pending，再次标记会命中它
    assert await registry.mark_session_stale("session-a") == 1
    assert {item.rpc_id for item in await registry.list_pending()} == {"rpc-3"}
    # 无 pending 时返回 0；session-b 的 pending 也被标记
    assert await registry.mark_session_stale("session-a") == 0
    assert await registry.mark_session_stale("session-b") == 1
    assert await registry.list_pending() == []


async def test_state_transitions_are_explicit_and_illegal_transitions_raise() -> None:
    """合法状态转换明确；非法转换抛 ValueError，未知 rpcId 抛 KeyError。"""

    persistence = MemoryPersistence()
    registry = make_registry(persistence)
    await registry.load()
    await registry.upsert(make_question())  # rpc-1
    await registry.upsert(make_approval())  # rpc-2

    # pending -> responding
    await registry.mark_responding("rpc-1")
    with pytest.raises(ValueError, match="responding"):
        await registry.mark_responding("rpc-1")
    # responding -> pending（失败回滚）
    await registry.mark_pending("rpc-1")
    with pytest.raises(ValueError, match="pending"):
        await registry.mark_pending("rpc-1")
    # pending -> resolved；resolved 是终态
    await registry.mark_resolved("rpc-1")
    with pytest.raises(ValueError, match="resolved"):
        await registry.mark_resolved("rpc-1")
    with pytest.raises(ValueError, match="resolved"):
        await registry.mark_responding("rpc-1")
    with pytest.raises(ValueError, match="resolved"):
        await registry.mark_pending("rpc-1")
    with pytest.raises(ValueError, match="resolved"):
        await registry.mark_stale("rpc-1")
    # approval: responding -> stale；stale 是终态
    await registry.mark_responding("rpc-2")
    await registry.mark_stale("rpc-2")
    with pytest.raises(ValueError, match="stale"):
        await registry.mark_responding("rpc-2")
    with pytest.raises(ValueError, match="stale"):
        await registry.mark_resolved("rpc-2")
    # 未知 rpcId 抛 KeyError
    with pytest.raises(KeyError, match="missing"):
        await registry.mark_responding("missing")
    with pytest.raises(KeyError, match="missing"):
        await registry.mark_resolved("missing")


def test_from_runtime_event_accepts_complete_question_and_approval() -> None:
    """完整 server-request 的 question/requested 与 approval/requested 被接受。"""

    question_event = DshRuntimeEvent(
        stream="mux",
        sequence=1,
        received_at=RECEIVED_AT,
        message={
            "type": "server-request",
            "rpcId": "question-1",
            "method": "events.mux",
            "payload": {"type": "question/requested", "sessionId": "session-1", "questions": []},
        },
    )
    interaction = DshPendingInteraction.from_runtime_event(question_event)
    assert interaction is not None
    assert interaction.rpc_id == "question-1"
    assert interaction.session_id == "session-1"
    assert interaction.kind == "question"
    assert interaction.state == "pending"
    assert interaction.stream == "mux"
    assert interaction.first_seen_at == RECEIVED_AT
    assert interaction.last_seen_at == RECEIVED_AT
    assert interaction.approval_id is None
    assert interaction.payload == question_event.message["payload"]

    approval_event = DshRuntimeEvent(
        stream="mux",
        sequence=2,
        received_at=RECEIVED_AT,
        message={
            "type": "server-request",
            "rpcId": "approval-1",
            "method": "events.mux",
            "payload": {
                "type": "approval/requested",
                "sessionId": "session-1",
                "approvalId": "approval-1",
                "toolName": "bash",
                "callId": "call-1",
                "reason": "执行删除操作",
            },
        },
    )
    interaction = DshPendingInteraction.from_runtime_event(approval_event)
    assert interaction is not None
    assert interaction.rpc_id == "approval-1"
    assert interaction.session_id == "session-1"
    assert interaction.kind == "approval"
    assert interaction.approval_id == "approval-1"
    assert interaction.payload == approval_event.message["payload"]


def test_from_runtime_event_rejects_malformed_and_non_pending_events() -> None:
    """非 server-request 或缺 rpcId/sessionId/approvalId 的畸形事件安全拒绝为 None。"""

    base = {
        "type": "server-request",
        "rpcId": "question-1",
        "method": "events.mux",
        "payload": {"type": "question/requested", "sessionId": "session-1", "questions": []},
    }
    cases: list[dict[str, Any]] = [
        {},  # 空消息
        {"type": "session/event", "payload": {"type": "turn/end"}},  # 非 server-request
        {**base, "rpcId": None},
        {**base, "rpcId": ""},
        {**base, "rpcId": 123},
        {**base, "payload": None},
        {**base, "payload": []},
        {**base, "payload": {"type": "question/requested"}},  # 缺 sessionId
        {**base, "payload": {"type": "question/requested", "sessionId": ""}},
        {**base, "payload": {"type": "question/requested", "sessionId": 7}},
        {**base, "payload": {"type": "approval/requested", "sessionId": "session-1"}},  # 缺 approvalId
        {**base, "payload": {"type": "approval/requested", "sessionId": "session-1", "approvalId": ""}},
        {**base, "payload": {"type": "mystery/requested", "sessionId": "session-1"}},  # 未知类型
    ]
    for message in cases:
        event = DshRuntimeEvent(
            stream="mux", sequence=1, received_at=RECEIVED_AT, message=message
        )
        assert DshPendingInteraction.from_runtime_event(event) is None, message


@pytest.mark.parametrize(
    "corrupt",
    [
        {"version": 2, "items": {}},  # 版本不匹配
        {"items": {}},  # 缺 version
        {"version": 1, "items": "not-a-dict"},  # items 不是对象
        {"version": 1, "items": {"rpc-1": "not-a-dict"}},  # 记录不是对象
        {"version": 1, "items": {"rpc-1": {"rpc_id": "rpc-1", "session_id": "session-a", "kind": "bogus", "payload": {}, "stream": "mux", "first_seen_at": "t", "last_seen_at": "t", "state": "pending"}}},  # kind 枚举损坏
        {"version": 1, "items": {"rpc-1": {"rpc_id": "rpc-1", "session_id": "session-a", "kind": "question", "payload": {}, "stream": "mux", "first_seen_at": "t", "last_seen_at": "t", "state": "bogus"}}},  # state 枚举损坏
        {"version": 1, "items": {"rpc-1": {"rpc_id": "rpc-1", "session_id": "session-a", "kind": "question", "payload": [], "stream": "mux", "first_seen_at": "t", "last_seen_at": "t", "state": "pending"}}},  # payload 不是对象
        {"version": 1, "items": {"rpc-1": {"rpc_id": "other", "session_id": "session-a", "kind": "question", "payload": {}, "stream": "mux", "first_seen_at": "t", "last_seen_at": "t", "state": "pending"}}},  # 键与 rpc_id 不一致
        {"version": 1, "items": {"rpc-1": {"rpc_id": "rpc-1", "session_id": "", "kind": "question", "payload": {}, "stream": "mux", "first_seen_at": "t", "last_seen_at": "t", "state": "pending"}}},  # session 为空
        {"version": 1, "items": {"rpc-1": {"rpc_id": "rpc-1", "session_id": "session-a", "kind": "approval", "payload": {}, "stream": "mux", "first_seen_at": "t", "last_seen_at": "t", "state": "pending"}}},  # approval 缺 approval_id
    ],
)
async def test_load_rejects_corrupted_persisted_data(corrupt: dict[str, Any]) -> None:
    """version/shape/枚举损坏必须明确 ValueError，不能静默丢数据。"""

    persistence = MemoryPersistence()
    persistence.data = corrupt
    registry = make_registry(persistence)
    with pytest.raises(ValueError):
        await registry.load()


async def test_failed_load_keeps_current_in_memory_state() -> None:
    """load 校验失败时不覆盖当前内存状态。"""

    persistence = MemoryPersistence()
    registry = make_registry(persistence)
    await registry.load()
    await registry.upsert(make_question())

    persistence.data = {"version": 999, "items": {}}
    with pytest.raises(ValueError):
        await registry.load()

    stored = await registry.get_pending("rpc-1")
    assert stored.rpc_id == "rpc-1"
