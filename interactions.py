"""DSH pending interaction registry：状态机、去重与 JSON 持久化。

负责 pending request 的状态、去重与持久化，不依赖 CoreSink 或 Chatter。存储键以
``rpcId`` 为权威标识；SSE 重连重放相同 ``rpcId`` 时只更新 ``last_seen_at``。
所有读改写操作由一把 ``asyncio.Lock`` 串行化，成功 mutation 在锁内完成持久化后才
返回；持久化失败回滚内存状态，保证“响应失败可重试”的承重契约。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from typing import Any, Literal

from src.app.plugin_system.api import storage_api

from .runtime import DshRuntimeEvent

InteractionKind = Literal["question", "approval"]
InteractionState = Literal["pending", "responding", "resolved", "stale"]

STORE_NAME = "dsh_adapter"
DATA_NAME = "pending_interactions"
PERSISTED_VERSION = 1

_KINDS: frozenset[str] = frozenset({"question", "approval"})
_STATES: frozenset[str] = frozenset({"pending", "responding", "resolved", "stale"})

LoadFunc = Callable[[str, str], Awaitable[dict[str, Any] | None]]
SaveFunc = Callable[[str, str, dict[str, Any]], Awaitable[None]]


def _require_non_empty_str(raw: dict[str, Any], field: str, key: str) -> str:
    """从持久化记录中取非空字符串字段，缺失或类型错误时抛 ValueError。"""

    value = raw.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"交互 {key!r} 的 {field!r} 字段缺失或不是非空字符串")
    return value


@dataclass(frozen=True, slots=True)
class DshPendingInteraction:
    """表示一条待处理（pending）的 DSH 交互请求。

    ``approval_id`` 只在 ``kind == "approval"`` 时存在，供 :meth:`resolve_approval`
    按 approvalId 精确匹配；持久化 round-trip 不得丢失。
    """

    rpc_id: str
    session_id: str
    kind: InteractionKind
    payload: dict[str, Any]
    stream: str
    first_seen_at: str
    last_seen_at: str
    state: InteractionState = "pending"
    approval_id: str | None = None

    @classmethod
    def from_runtime_event(cls, event: DshRuntimeEvent) -> DshPendingInteraction | None:
        """从运行时事件提取完整 pending 交互；不适用或畸形事件安全返回 None。

        只接受完整 ``server-request`` 信封中 ``payload.type`` 为
        ``question/requested`` 或 ``approval/requested`` 的事件；缺 rpcId、
        sessionId 或 approval 缺 approvalId 的畸形事件一律返回 None，绝不抛异常。
        """

        message = event.message
        if message.get("type") != "server-request":
            return None
        rpc_id = message.get("rpcId")
        payload = message.get("payload")
        if not isinstance(rpc_id, str) or not rpc_id:
            return None
        if not isinstance(payload, dict):
            return None
        session_id = payload.get("sessionId")
        if not isinstance(session_id, str) or not session_id:
            return None
        payload_type = payload.get("type")
        if payload_type == "question/requested":
            kind: InteractionKind = "question"
            approval_id: str | None = None
        elif payload_type == "approval/requested":
            approval_id = payload.get("approvalId")
            if not isinstance(approval_id, str) or not approval_id:
                return None
            kind = "approval"
        else:
            return None
        return cls(
            rpc_id=rpc_id,
            session_id=session_id,
            kind=kind,
            payload=payload,
            stream=event.stream,
            first_seen_at=event.received_at,
            last_seen_at=event.received_at,
            approval_id=approval_id,
        )

    def to_persisted_dict(self) -> dict[str, Any]:
        """返回可 JSON 序列化的持久化表示。"""

        return {
            "rpc_id": self.rpc_id,
            "session_id": self.session_id,
            "kind": self.kind,
            "payload": self.payload,
            "stream": self.stream,
            "first_seen_at": self.first_seen_at,
            "last_seen_at": self.last_seen_at,
            "state": self.state,
            "approval_id": self.approval_id,
        }

    @classmethod
    def from_persisted_dict(cls, raw: dict[str, Any], *, key: str) -> DshPendingInteraction:
        """从持久化字典恢复记录；version/shape/枚举损坏时明确抛 ValueError。"""

        rpc_id = _require_non_empty_str(raw, "rpc_id", key)
        if rpc_id != key:
            raise ValueError(f"存储键 {key!r} 与记录 rpc_id {rpc_id!r} 不一致")
        session_id = _require_non_empty_str(raw, "session_id", key)
        stream = _require_non_empty_str(raw, "stream", key)
        first_seen_at = _require_non_empty_str(raw, "first_seen_at", key)
        last_seen_at = _require_non_empty_str(raw, "last_seen_at", key)
        kind = raw.get("kind")
        if kind not in _KINDS:
            raise ValueError(f"交互 {key!r} 的 kind 非法: {kind!r}")
        state = raw.get("state")
        if state not in _STATES:
            raise ValueError(f"交互 {key!r} 的 state 非法: {state!r}")
        payload = raw.get("payload")
        if not isinstance(payload, dict):
            raise ValueError(f"交互 {key!r} 的 payload 不是 JSON 对象")
        approval_id = raw.get("approval_id")
        if approval_id is not None and not isinstance(approval_id, str):
            raise ValueError(f"交互 {key!r} 的 approval_id 不是字符串")
        if kind == "approval" and not approval_id:
            raise ValueError(f"approval 交互 {key!r} 缺少 approval_id")
        return cls(
            rpc_id=rpc_id,
            session_id=session_id,
            kind=kind,
            payload=payload,
            stream=stream,
            first_seen_at=first_seen_at,
            last_seen_at=last_seen_at,
            state=state,
            approval_id=approval_id,
        )


class DshInteractionRegistry:
    """管理 pending 交互的状态、去重与持久化的注册表。

    所有公共方法在 ``asyncio.Lock`` 内完成读改写；任何成功 mutation 都必须先
    持久化成功才返回，保存失败时回滚内存状态并向外抛出原始异常。``load_func`` /
    ``save_func`` 默认使用 ``storage_api.load_json`` / ``storage_api.save_json``。
    """

    def __init__(
        self,
        *,
        load_func: LoadFunc | None = None,
        save_func: SaveFunc | None = None,
    ) -> None:
        """初始化 registry；测试可注入内存持久化 double。"""

        self._load_func: LoadFunc = load_func if load_func is not None else storage_api.load_json
        self._save_func: SaveFunc = save_func if save_func is not None else storage_api.save_json
        self._lock = asyncio.Lock()
        self._items: dict[str, DshPendingInteraction] = {}

    async def load(self) -> None:
        """从持久化数据恢复全部交互；数据不存在时初始化为空。

        先完整校验再整体替换内存状态：损坏数据抛 ``ValueError`` 且不影响当前
        内存状态。
        """

        async with self._lock:
            raw = await self._load_func(STORE_NAME, DATA_NAME)
            if raw is None:
                self._items = {}
                return
            self._items = self._validate_persisted(raw)

    async def upsert(self, interaction: DshPendingInteraction) -> bool:
        """插入或重放一条交互；返回 True 表示首次插入。

        新记录一律以 ``pending`` 落库。同 rpcId 重放只更新 ``last_seen_at`` 并
        返回 False；重放改变 session 或 kind 时抛 ``ValueError`` 拒绝串会话。
        """

        async with self._lock:
            existing = self._items.get(interaction.rpc_id)
            if existing is not None:
                if existing.session_id != interaction.session_id:
                    raise ValueError(
                        f"rpc_id={interaction.rpc_id!r} 重放改变 session "
                        f"({existing.session_id!r} -> {interaction.session_id!r})，拒绝跨 session 串用"
                    )
                if existing.kind != interaction.kind:
                    raise ValueError(
                        f"rpc_id={interaction.rpc_id!r} 重放改变 kind "
                        f"({existing.kind!r} -> {interaction.kind!r})，拒绝串用"
                    )
                snapshot = dict(self._items)
                self._items[interaction.rpc_id] = replace(
                    existing, last_seen_at=interaction.last_seen_at
                )
                await self._persist_or_rollback(snapshot)
                return False
            snapshot = dict(self._items)
            self._items[interaction.rpc_id] = replace(interaction, state="pending")
            await self._persist_or_rollback(snapshot)
            return True

    async def list_pending(self, session_id: str | None = None) -> list[DshPendingInteraction]:
        """列出 pending 交互；可按 session 过滤，按插入顺序返回。"""

        async with self._lock:
            items = [item for item in self._items.values() if item.state == "pending"]
            if session_id is not None:
                items = [item for item in items if item.session_id == session_id]
            return items

    async def get_pending(self, rpc_id: str) -> DshPendingInteraction:
        """返回指定 rpcId 的 pending 交互；不存在或非 pending 时抛 ``KeyError``。"""

        async with self._lock:
            item = self._items.get(rpc_id)
            if item is None:
                raise KeyError(f"没有 pending 交互: rpc_id={rpc_id!r}")
            if item.state != "pending":
                raise KeyError(f"rpc_id={rpc_id!r} 不是 pending 状态（当前 state={item.state!r}）")
            return item

    async def mark_responding(self, rpc_id: str) -> None:
        """pending -> responding：开始发送结构化响应。"""

        async with self._lock:
            snapshot = dict(self._items)
            updated = self._transition(
                rpc_id,
                allowed=("pending",),
                target="responding",
                label="标记为 responding",
            )
            self._items[rpc_id] = updated
            await self._persist_or_rollback(snapshot)

    async def mark_pending(self, rpc_id: str) -> None:
        """responding -> pending：响应失败后回滚，等待重试。"""

        async with self._lock:
            snapshot = dict(self._items)
            updated = self._transition(
                rpc_id,
                allowed=("responding",),
                target="pending",
                label="回滚为 pending",
            )
            self._items[rpc_id] = updated
            await self._persist_or_rollback(snapshot)

    async def mark_resolved(self, rpc_id: str) -> None:
        """pending/responding -> resolved：响应成功，交互结束。"""

        async with self._lock:
            snapshot = dict(self._items)
            updated = self._transition(
                rpc_id,
                allowed=("pending", "responding"),
                target="resolved",
                label="标记为 resolved",
            )
            self._items[rpc_id] = updated
            await self._persist_or_rollback(snapshot)

    async def mark_stale(self, rpc_id: str) -> None:
        """pending/responding -> stale：交互作废（如 session 移除）。"""

        async with self._lock:
            snapshot = dict(self._items)
            updated = self._transition(
                rpc_id,
                allowed=("pending", "responding"),
                target="stale",
                label="标记为 stale",
            )
            self._items[rpc_id] = updated
            await self._persist_or_rollback(snapshot)

    async def resolve_question(self, question_rpc_id: str) -> bool:
        """按原 question rpcId 精确结束 question 交互；返回是否已处于 resolved。

        记录不存在、kind 不是 question 或已是 stale 终态时返回 False；已 resolved
        时幂等返回 True（不再落盘）。
        """

        async with self._lock:
            item = self._items.get(question_rpc_id)
            if item is None or item.kind != "question" or item.state == "stale":
                return False
            if item.state == "resolved":
                return True
            snapshot = dict(self._items)
            self._items[question_rpc_id] = replace(item, state="resolved")
            await self._persist_or_rollback(snapshot)
            return True

    async def resolve_approval(self, approval_id: str) -> bool:
        """按 approvalId 精确结束 approval 交互；返回是否已处于 resolved。

        没有匹配 approvalId 的 approval 记录时返回 False；全部匹配记录已 resolved
        时幂等返回 True（不再落盘）；stale 终态记录不会被复活。
        """

        async with self._lock:
            candidates = [
                item
                for item in self._items.values()
                if item.kind == "approval" and item.approval_id == approval_id
            ]
            if not candidates:
                return False
            actionable = [item for item in candidates if item.state in ("pending", "responding")]
            already_resolved = any(item.state == "resolved" for item in candidates)
            if not actionable:
                return already_resolved
            snapshot = dict(self._items)
            for item in actionable:
                self._items[item.rpc_id] = replace(item, state="resolved")
            await self._persist_or_rollback(snapshot)
            return True

    async def mark_session_stale(self, session_id: str) -> int:
        """把指定 session 的所有 pending 交互标为 stale；返回标记数量。

        responding 中的交互刻意不动，等待在途响应自然结束。其他 session 不受影响。
        """

        async with self._lock:
            targets = [
                item
                for item in self._items.values()
                if item.session_id == session_id and item.state == "pending"
            ]
            if not targets:
                return 0
            snapshot = dict(self._items)
            for item in targets:
                self._items[item.rpc_id] = replace(item, state="stale")
            await self._persist_or_rollback(snapshot)
            return len(targets)

    def _require_item(self, rpc_id: str) -> DshPendingInteraction:
        """按 rpcId 取记录；不存在时抛 KeyError。"""

        item = self._items.get(rpc_id)
        if item is None:
            raise KeyError(f"没有交互: rpc_id={rpc_id!r}")
        return item

    def _transition(
        self,
        rpc_id: str,
        *,
        allowed: tuple[InteractionState, ...],
        target: InteractionState,
        label: str,
    ) -> DshPendingInteraction:
        """校验并构造状态转换后的新记录；非法转换抛 ValueError。"""

        item = self._require_item(rpc_id)
        if item.state not in allowed:
            raise ValueError(
                f"非法状态转换: rpc_id={rpc_id!r} 当前 state={item.state!r}，不能 {label}"
            )
        return replace(item, state=target)

    def _to_persisted(self) -> dict[str, Any]:
        """序列化全部交互为持久化格式。"""

        return {
            "version": PERSISTED_VERSION,
            "items": {rpc_id: item.to_persisted_dict() for rpc_id, item in self._items.items()},
        }

    @staticmethod
    def _validate_persisted(raw: dict[str, Any]) -> dict[str, DshPendingInteraction]:
        """校验并反序列化持久化数据；任何损坏都明确抛 ValueError。"""

        if not isinstance(raw, dict):
            raise ValueError("pending_interactions 数据不是 JSON 对象")
        version = raw.get("version")
        if version != PERSISTED_VERSION:
            raise ValueError(f"不支持的持久化版本: {version!r}（期望 {PERSISTED_VERSION}）")
        raw_items = raw.get("items")
        if not isinstance(raw_items, dict):
            raise ValueError("pending_interactions 的 items 字段不是 JSON 对象")
        items: dict[str, DshPendingInteraction] = {}
        for rpc_id, raw_item in raw_items.items():
            if not isinstance(raw_item, dict):
                raise ValueError(f"交互 {rpc_id!r} 不是 JSON 对象")
            items[rpc_id] = DshPendingInteraction.from_persisted_dict(raw_item, key=rpc_id)
        return items

    async def _persist_or_rollback(
        self, snapshot: dict[str, DshPendingInteraction]
    ) -> None:
        """在锁内持久化当前状态；失败时回滚内存到快照并重抛异常。"""

        try:
            await self._save_func(STORE_NAME, DATA_NAME, self._to_persisted())
        except Exception:
            self._items = snapshot
            raise
