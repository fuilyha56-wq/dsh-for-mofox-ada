"""DSH 事件安全渲染与进度聚合（Task 4）。

纯函数渲染层：把 Task 1 的 ``DshRuntimeEvent`` 转成可供 MoFox 入站的
``RenderedDshEvent``；``DshProgressAggregator`` 按 session 短窗口聚合普通进度
事件。本模块不访问网络、不依赖 CoreSink 或 Chatter，只按 ``payload.type`` 分类
（绝不依赖 ``method``），并对 text/extra/raw_message 做递归脱敏。

分类规则（已验证协议事实）：

- 真实 DSH ServerRequest 信封为 ``{type, rpcId, method, payload}``，分类只看
  ``payload.type``。
- ``sessionId`` 存在于全部 mux 帧与 ``host/session-*``、``host/agent-error``；
  无 sessionId 的 ``host/workspace-*``、``remote-event``、``stream/error`` 仅
  缓冲，渲染返回 ``None``。
- 立即事件：``question/requested``、``approval/requested``、``question/resolved``、
  ``approval/resolved``、``host/agent-error``，以及 ``session/event`` 内嵌套
  ``event.type == "turn/end"``。
- requested 帧的 rpcId 可稳定重放：``message_id = dsh:<stream>:<rpcId>``；
  ``session/event`` push 的 rpcId 不稳定，message_id 追加嵌套 ``event.type`` 与
  ``event.seq``。
- ``turn/end`` 完成原因位于嵌套 ``event.data.reason.kind``。
- 跨任务安全契约：``RenderedDshEvent.raw_message`` 是唯一允许进入
  MessageEnvelope 的脱敏副本，Task 5 及以后任何消费方都不得退回 Runtime 原
  ``event.message``。
"""

from __future__ import annotations

import copy
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Literal

from .runtime import DshRuntimeEvent

IMMEDIATE_PAYLOAD_TYPES: frozenset[str] = frozenset(
    {
        "question/requested",
        "approval/requested",
        "question/resolved",
        "approval/resolved",
        "host/agent-error",
    }
)
RESPONSE_PAYLOAD_TYPES: frozenset[str] = frozenset(
    {"question/requested", "approval/requested"}
)
TURN_END_EVENT_TYPE = "turn/end"
EXTRA_WHITELIST: frozenset[str] = frozenset(
    {
        "dsh_rpc_id",
        "dsh_session_id",
        "dsh_frame_type",
        "dsh_event_stream",
        "dsh_bridge_sequence",
        "dsh_requires_response",
        "dsh_session_event_type",
    }
)
_TOOL_NAME_KEYS: tuple[str, ...] = ("name", "tool", "toolName")
REDACTED_VALUE = "[REDACTED]"
TRUNCATION_MARKER = "\n...[内容过长已截断]"
SUMMARY_RAW_TYPE = "dsh-progress-summary"
_SECRET_TERMS: tuple[str, ...] = ("secret", "apikey", "credential")
_BASE64_RE = re.compile(r"^[A-Za-z0-9+/]*={0,2}$")
_MIN_BASE64_CHARS = 64
_TOOL_NAME_LIMIT = 64
_SUMMARY_MAX_CHARACTERS = 2000


def _normalize_key(key: str) -> str:
    """归一化键名：小写并去掉所有非字母数字字符，使分隔符差异不影响匹配。"""

    return "".join(ch for ch in key.lower() if ch.isalnum())


def _is_secret_key(key: str) -> bool:
    """判断键名是否命中脱敏词（secret/apiKey/api_key/credential 等变体）。"""

    normalized = _normalize_key(key)
    return any(term in normalized for term in _SECRET_TERMS)


def _is_large_base64(value: str) -> bool:
    """判断字符串是否为长度可观（>=64 字符）的合法 Base64 内容。"""

    if len(value) < _MIN_BASE64_CHARS:
        return False
    if len(value) % 4 == 1:
        return False
    if not _BASE64_RE.fullmatch(value):
        return False
    return value.strip("=") != ""


def _sanitize(value: Any) -> Any:
    """递归脱敏：secret 类键的值替换为 REDACTED_VALUE，大段 Base64 只显示长度。"""

    if isinstance(value, dict):
        return {
            key: REDACTED_VALUE if _is_secret_key(key) else _sanitize(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    if isinstance(value, str):
        if _is_large_base64(value):
            return f"[base64 {len(value)} chars]"
        return value
    return value


def _truncate(text: str, max_characters: int) -> str:
    """超过 max_characters 时按稳定截断标记截断，不泄露尾部内容。"""

    if len(text) <= max_characters:
        return text
    return text[:max_characters] + TRUNCATION_MARKER


@dataclass(frozen=True, slots=True)
class RenderedDshEvent:
    """表示一条已渲染、可供 MoFox 入站的 DSH 事件。

    ``raw_message`` 是唯一允许进入 MessageEnvelope 的脱敏副本：它是完整
    server-request 信封的递归脱敏深拷贝，保留路由所需标识（type/rpcId/
    payload.sessionId），已剔除 secret/apiKey/credential 类键与大段 Base64
    原文。Task 5 及以后任何消费方都必须使用 ``raw_message`` 构建
    MessageEnvelope，不得退回 Runtime 原 ``event.message``。
    """

    session_id: str
    message_id: str
    received_at: str
    text: str
    requires_response: bool
    immediate: bool
    extra: dict[str, Any]
    # 唯一允许进入 MessageEnvelope 的脱敏信封副本（见类 docstring）
    raw_message: dict[str, Any]

    def __post_init__(self) -> None:
        """深拷贝 extra 与 raw_message，建立与外部对象的不可变边界。

        frozen dataclass 只保证字段引用不可变；这里统一 ``deepcopy``，使
        ``raw_message`` 与 Runtime 原 ``event.message`` 彻底分离，即使调用方
        事后改写原事件也不影响已渲染结果。
        """

        object.__setattr__(self, "extra", copy.deepcopy(self.extra))
        object.__setattr__(self, "raw_message", copy.deepcopy(self.raw_message))


def _find_tool_name(payload: dict[str, Any]) -> str | None:
    """从 session/event 嵌套 payload 中提取工具名；缺失时返回 None。"""

    event = payload.get("event")
    if not isinstance(event, dict):
        return None
    data = event.get("data")
    if not isinstance(data, dict):
        return None
    for key in _TOOL_NAME_KEYS:
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _render_question(payload: dict[str, Any], rpc_id: str, session_id: str) -> str:
    """渲染 question/requested：id/header/question/detail/options/multiSelect/intent。"""

    lines: list[str] = [f"DSH 问题请求 (rpcId: {rpc_id})", f"sessionId: {session_id}"]
    questions = payload.get("questions")
    if not isinstance(questions, list):
        return "\n".join(lines)
    for index, item in enumerate(questions, start=1):
        if not isinstance(item, dict):
            continue
        lines.append(f"[问题 {index}]")
        question_id = item.get("id")
        if isinstance(question_id, str) and question_id:
            lines.append(f"id: {question_id}")
        header = item.get("header")
        if isinstance(header, str) and header:
            lines.append(f"header: {header}")
        question = item.get("question")
        if isinstance(question, str) and question:
            lines.append(f"question: {question}")
        detail = item.get("detail")
        if isinstance(detail, str) and detail:
            lines.append(f"detail: {detail}")
        multi_select = item.get("multiSelect", False)
        if isinstance(multi_select, bool):
            lines.append(f"multiSelect={str(multi_select).lower()}")
        intent = item.get("intent")
        if isinstance(intent, dict):
            intent_kind = intent.get("kind")
            if isinstance(intent_kind, str) and intent_kind:
                lines.append(f"intent: {intent_kind}")
            approve = intent.get("approve")
            if isinstance(approve, bool):
                lines.append(f"approve: {str(approve).lower()}")
        options = item.get("options")
        if isinstance(options, list):
            for option in options:
                if not isinstance(option, dict):
                    continue
                label = option.get("label")
                if not isinstance(label, str):
                    continue
                description = option.get("description")
                if isinstance(description, str) and description:
                    lines.append(f"- {label}: {description}")
                else:
                    lines.append(f"- {label}")
    return "\n".join(lines)


def _render_approval(payload: dict[str, Any], rpc_id: str, session_id: str) -> str:
    """渲染 approval/requested：approvalId/toolName/callId/reason 与处理入口提示。"""

    lines: list[str] = [f"DSH 审批请求 (rpcId: {rpc_id})", f"sessionId: {session_id}"]
    for field_name in ("approvalId", "toolName", "callId", "reason"):
        value = payload.get(field_name)
        if isinstance(value, str) and value:
            lines.append(f"{field_name}: {value}")
    lines.append("请使用 dsh_respond Action 或 /dsh respond 命令处理该审批。")
    return "\n".join(lines)


def _render_resolved(
    payload: dict[str, Any], rpc_id: str, session_id: str, label: str
) -> str:
    """渲染 question/resolved 与 approval/resolved 的通用完成文本。"""

    lines: list[str] = [f"DSH {label} (rpcId: {rpc_id})", f"sessionId: {session_id}"]
    approval_id = payload.get("approvalId")
    if isinstance(approval_id, str) and approval_id:
        lines.append(f"approvalId: {approval_id}")
    return "\n".join(lines)


def _render_agent_error(payload: dict[str, Any], rpc_id: str, session_id: str) -> str:
    """渲染 host/agent-error：携带 sessionId 与错误 message。"""

    lines: list[str] = [f"DSH 代理错误 (rpcId: {rpc_id})", f"sessionId: {session_id}"]
    message = payload.get("message")
    if isinstance(message, str) and message:
        lines.append(f"message: {message}")
    return "\n".join(lines)


def _render_turn_end(
    nested_event: dict[str, Any], rpc_id: str, session_id: str
) -> str:
    """渲染 turn/end：完成原因取自嵌套 event.data.reason.kind。"""

    lines: list[str] = [f"DSH 回合结束 (rpcId: {rpc_id})", f"sessionId: {session_id}"]
    data = nested_event.get("data")
    reason = data.get("reason") if isinstance(data, dict) else None
    reason_kind = reason.get("kind") if isinstance(reason, dict) else None
    if isinstance(reason_kind, str) and reason_kind:
        lines.append(f"reason: {reason_kind}")
    return "\n".join(lines)


def _render_progress(nested_event: dict[str, Any]) -> str:
    """渲染普通进度事件的安全摘要文本，绝不携带逐 token 内容。"""

    event_type = nested_event.get("type")
    if not isinstance(event_type, str):
        return "DSH 进度：event"
    data = nested_event.get("data")
    if event_type == "assistant/chunk":
        return "DSH 进度：assistant 输出中"
    if event_type == "assistant/message":
        return "DSH 进度：assistant 消息"
    if event_type == "tool/call":
        tool_name = _find_tool_name({"event": nested_event})
        if tool_name is not None:
            return f"DSH 进度：工具调用 {tool_name}"
        return "DSH 进度：工具调用"
    if event_type == "tool/result":
        failed = isinstance(data, dict) and bool(data.get("error"))
        return "DSH 进度：工具结果 失败" if failed else "DSH 进度：工具结果 成功"
    return f"DSH 进度：{event_type}"


def render_dsh_event(
    event: DshRuntimeEvent, max_characters: int
) -> RenderedDshEvent | None:
    """把一条运行时事件渲染为入站消息；不适用或畸形事件安全返回 None。

    只接受完整 ``server-request`` 信封且 ``payload.sessionId`` 为非空字符串；
    分类只看 ``payload.type`` 与嵌套 ``event.type``，绝不依赖 ``method``。
    无 sessionId 的 host/workspace-*、remote-event、stream/error 仅缓冲，
    返回 None。

    ``raw_message`` 为信封的递归脱敏深拷贝，是唯一允许进入 MessageEnvelope
    的脱敏副本；消费方不得退回 Runtime 原 ``event.message``（跨任务安全
    契约，Task 5 必须遵守）。
    """

    message = event.message
    if not isinstance(message, dict):
        return None
    if message.get("type") != "server-request":
        return None
    rpc_id = message.get("rpcId")
    payload = message.get("payload")
    if not isinstance(rpc_id, str) or not rpc_id:
        return None
    if not isinstance(payload, dict):
        return None
    payload_type = payload.get("type")
    if not isinstance(payload_type, str) or not payload_type:
        return None
    session_id = payload.get("sessionId")
    if not isinstance(session_id, str) or not session_id:
        return None

    nested_event: dict[str, Any] | None = None
    session_event_type: str = payload_type
    if payload_type == "session/event":
        raw_event = payload.get("event")
        if not isinstance(raw_event, dict):
            return None
        event_type = raw_event.get("type")
        if not isinstance(event_type, str) or not event_type:
            return None
        nested_event = raw_event
        session_event_type = event_type
        seq = raw_event.get("seq", 0)
        message_id = f"dsh:{event.stream}:{rpc_id}:{event_type}:{seq}"
    else:
        message_id = f"dsh:{event.stream}:{rpc_id}"

    if nested_event is not None and session_event_type == TURN_END_EVENT_TYPE:
        text = _render_turn_end(nested_event, rpc_id, session_id)
        immediate = True
    elif payload_type in IMMEDIATE_PAYLOAD_TYPES:
        if payload_type == "question/requested":
            text = _render_question(payload, rpc_id, session_id)
        elif payload_type == "approval/requested":
            text = _render_approval(payload, rpc_id, session_id)
        elif payload_type == "question/resolved":
            text = _render_resolved(payload, rpc_id, session_id, "问题已解决")
        elif payload_type == "approval/resolved":
            text = _render_resolved(payload, rpc_id, session_id, "审批已解决")
        else:
            text = _render_agent_error(payload, rpc_id, session_id)
        immediate = True
    elif nested_event is not None:
        text = _render_progress(nested_event)
        immediate = False
    else:
        text = f"DSH 进度：{payload_type}"
        immediate = False

    requires_response = payload_type in RESPONSE_PAYLOAD_TYPES
    text = _truncate(text, max_characters)
    raw_message = copy.deepcopy(_sanitize(message))
    extra: dict[str, Any] = {
        "dsh_rpc_id": rpc_id,
        "dsh_session_id": session_id,
        "dsh_frame_type": "server-request",
        "dsh_event_stream": event.stream,
        "dsh_bridge_sequence": event.sequence,
        "dsh_requires_response": requires_response,
        "dsh_session_event_type": session_event_type,
    }
    return RenderedDshEvent(
        session_id=session_id,
        message_id=message_id,
        received_at=event.received_at,
        text=text,
        requires_response=requires_response,
        immediate=immediate,
        extra=extra,
        raw_message=raw_message,
    )


ProgressDeliveryMode = Literal["aggregate", "critical_only"]


@dataclass(slots=True)
class _ProgressBucket:
    """保存一个 DSH session 在聚合窗口内的进度状态（只存计数与必要元数据）。"""

    session_id: str
    stream: str
    deadline: float
    counts: dict[str, int] = field(default_factory=dict)
    tools: list[str] = field(default_factory=list)
    tool_results: tuple[int, int] = (0, 0)
    last_received_at: str = ""

    def record(self, rendered: RenderedDshEvent) -> None:
        """记录一条进度事件：计数、工具名与工具结果成败概况。"""

        event_type = rendered.extra.get("dsh_session_event_type")
        if not isinstance(event_type, str) or not event_type:
            event_type = "event"
        self.counts[event_type] = self.counts.get(event_type, 0) + 1
        self.last_received_at = rendered.received_at
        if event_type == "tool/call":
            payload = rendered.raw_message.get("payload")
            tool_name = _find_tool_name(payload) if isinstance(payload, dict) else None
            if tool_name is not None:
                bounded = _truncate(tool_name, _TOOL_NAME_LIMIT)
                if bounded not in self.tools:
                    self.tools.append(bounded)
        elif event_type == "tool/result":
            ok_count, failed_count = self.tool_results
            if _tool_result_failed(rendered.raw_message):
                self.tool_results = (ok_count, failed_count + 1)
            else:
                self.tool_results = (ok_count + 1, failed_count)


def _tool_result_failed(raw_message: dict[str, Any]) -> bool:
    """判断 tool/result 是否携带 error 字段（失败概况）。"""

    payload = raw_message.get("payload")
    event = payload.get("event") if isinstance(payload, dict) else None
    data = event.get("data") if isinstance(event, dict) else None
    return isinstance(data, dict) and bool(data.get("error"))


class DshProgressAggregator:
    """按 session 隔离的短窗口进度聚合器；立即事件直接放行。

    ``add(immediate)`` 立即返回该事件；``add(turn/end)`` 若同 session 存在待
    聚合桶，立即返回 ``[摘要, turn_end]``（摘要在前）并清桶，无桶时返回
    ``[turn_end]``；普通进度事件入桶并返回 ``[]``。``flush_due(now)`` 用调用方
    传入的 now 判断窗口（deadline 取自首个进度事件的注入时钟 +
    ``window_seconds``，后续事件不延长窗口）；``flush_session`` 立即取出指定
    session 的摘要。``delivery_mode="critical_only"`` 时普通事件直接丢弃且
    永不形成摘要，立即事件（含 turn/end）仍正常返回。
    """

    def __init__(
        self,
        *,
        delivery_mode: ProgressDeliveryMode = "aggregate",
        window_seconds: float = 2.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """初始化聚合器；delivery_mode 与 window_seconds 非法时抛 ValueError。"""

        if delivery_mode not in ("aggregate", "critical_only"):
            raise ValueError(
                f"delivery_mode 必须为 aggregate 或 critical_only: {delivery_mode!r}"
            )
        if (
            isinstance(window_seconds, bool)
            or not isinstance(window_seconds, (int, float))
            or window_seconds <= 0
        ):
            raise ValueError(f"window_seconds 必须大于 0: {window_seconds!r}")
        self._delivery_mode: ProgressDeliveryMode = delivery_mode
        self._window_seconds: float = float(window_seconds)
        self._clock: Callable[[], float] = clock
        self._buckets: dict[str, _ProgressBucket] = {}

    def add(self, rendered: RenderedDshEvent) -> list[RenderedDshEvent]:
        """加入一条已渲染事件。

        立即事件原样返回；``turn/end`` 若同 session 存在待聚合桶，立即返回
        ``[摘要, turn_end]``（摘要在前）并清桶，无桶时返回 ``[turn_end]``。
        普通进度事件入桶后返回 ``[]``。两种 delivery_mode 都不吞立即事件；
        ``critical_only`` 恒无桶，``turn/end`` 同样原样放行。
        """

        if rendered.immediate:
            if (
                rendered.extra.get("dsh_session_event_type") == TURN_END_EVENT_TYPE
            ):
                bucket = self._buckets.pop(rendered.session_id, None)
                if bucket is not None:
                    return [self._make_summary(bucket), rendered]
            return [rendered]
        if self._delivery_mode == "critical_only":
            return []
        bucket = self._buckets.get(rendered.session_id)
        if bucket is None:
            bucket = _ProgressBucket(
                session_id=rendered.session_id,
                stream=str(rendered.extra.get("dsh_event_stream", "")),
                deadline=self._clock() + self._window_seconds,
            )
            self._buckets[rendered.session_id] = bucket
        bucket.record(rendered)
        return []

    def flush_due(self, now: float) -> list[RenderedDshEvent]:
        """flush 所有窗口已到期（deadline <= now）的 session 摘要。"""

        if self._delivery_mode == "critical_only":
            return []
        due_session_ids = [
            session_id
            for session_id in sorted(self._buckets)
            if self._buckets[session_id].deadline <= now
        ]
        return [
            self._make_summary(self._buckets.pop(session_id))
            for session_id in due_session_ids
        ]

    def flush_session(self, session_id: str) -> RenderedDshEvent | None:
        """立即取出指定 session 的聚合摘要；无桶或 critical_only 时返回 None。"""

        if self._delivery_mode == "critical_only":
            return None
        bucket = self._buckets.pop(session_id, None)
        if bucket is None:
            return None
        return self._make_summary(bucket)

    def _make_summary(self, bucket: _ProgressBucket) -> RenderedDshEvent:
        """把桶内计数汇总为一条摘要事件；不携带任何逐 token 内容或工具参数。"""

        total = sum(bucket.counts.values())
        lines: list[str] = [f"DSH 进度摘要 ({bucket.session_id})：共 {total} 条事件。"]
        counts_line = "、".join(
            f"{event_type} × {count}" for event_type, count in bucket.counts.items()
        )
        if counts_line:
            lines.append(counts_line)
        if bucket.tools:
            lines.append("工具调用：" + "、".join(bucket.tools))
        ok_count, failed_count = bucket.tool_results
        if ok_count or failed_count:
            lines.append(f"工具结果：{ok_count} 成功、{failed_count} 失败")
        raw_message: dict[str, Any] = {
            "type": SUMMARY_RAW_TYPE,
            "sessionId": bucket.session_id,
            "eventCounts": dict(bucket.counts),
            "tools": list(bucket.tools),
            "toolResults": {"ok": ok_count, "error": failed_count},
        }
        extra: dict[str, Any] = {
            "dsh_session_id": bucket.session_id,
            "dsh_event_stream": bucket.stream,
            "dsh_frame_type": "progress",
            "dsh_requires_response": False,
        }
        return RenderedDshEvent(
            session_id=bucket.session_id,
            message_id=f"dsh:{bucket.stream}:progress:{bucket.session_id}",
            received_at=bucket.last_received_at,
            text=_truncate("\n".join(lines), _SUMMARY_MAX_CHARACTERS),
            requires_response=False,
            immediate=False,
            extra=extra,
            raw_message=raw_message,
        )
