"""测试 DSH 事件安全渲染与进度聚合（Task 4）。"""

from __future__ import annotations

import base64
import copy
import json
from typing import Any

import pytest

from plugins.dsh_adapter.event_messages import DshProgressAggregator, render_dsh_event
from plugins.dsh_adapter.runtime import DshRuntimeEvent

RECEIVED_AT = "2026-08-15T01:00:00+00:00"

QUESTION_MESSAGE: dict[str, Any] = {
    "type": "server-request",
    "rpcId": "q-1",
    "method": "session/questions",
    "payload": {
        "type": "question/requested",
        "sessionId": "session-1",
        "questions": [
            {
                "id": "language",
                "header": "选择开发语言",
                "question": "请选择开发语言",
                "detail": "该选项将决定项目模板",
                "options": [
                    {"label": "Python", "description": "推荐，生态丰富"},
                    {"label": "Rust", "description": "高性能，编译较慢"},
                ],
                "multiSelect": True,
                "intent": {"kind": "plan-review", "approve": False},
            }
        ],
    },
}

APPROVAL_MESSAGE: dict[str, Any] = {
    "type": "server-request",
    "rpcId": "a-1",
    "method": "session/approve",
    "payload": {
        "type": "approval/requested",
        "sessionId": "session-1",
        "approvalId": "approval-1",
        "toolName": "shell_exec",
        "callId": "call-9",
        "reason": "需要在远程主机执行命令",
    },
}

TURN_END_MESSAGE: dict[str, Any] = {
    "type": "server-request",
    "rpcId": "push-abc",
    "method": "session/event",
    "payload": {
        "type": "session/event",
        "sessionId": "session-1",
        "event": {
            "type": "turn/end",
            "seq": 7,
            "data": {"reason": {"kind": "completed"}},
        },
    },
}


def make_event(
    message: dict[str, Any],
    *,
    stream: str = "mux",
    sequence: int = 1,
) -> DshRuntimeEvent:
    """构造手写 DshRuntimeEvent fixture；message 深拷贝防止跨测试污染。"""

    return DshRuntimeEvent(
        stream=stream,
        sequence=sequence,
        received_at=RECEIVED_AT,
        message=copy.deepcopy(message),
    )


class FakeClock:
    """可手动推进的单调时钟 double；测试不依赖真实 sleep。"""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


BLOB = base64.b64encode(b"attachment-bytes-" * 16).decode()


def make_session_event(
    event_type: str,
    seq: int,
    *,
    session_id: str = "session-1",
    rpc_id: str | None = None,
    data: dict[str, Any] | None = None,
    bridge_sequence: int = 1,
) -> DshRuntimeEvent:
    """构造一条 session/event push 类型的运行时事件。"""

    return make_event(
        {
            "type": "server-request",
            "rpcId": rpc_id or f"push-{seq}",
            "method": "session/event",
            "payload": {
                "type": "session/event",
                "sessionId": session_id,
                "event": {"type": event_type, "seq": seq, "data": data or {}},
            },
        },
        sequence=bridge_sequence,
    )


def test_render_question_and_approval_preserves_decision_context() -> None:
    """question/requested 与 approval/requested 必须保留完整决策上下文并立即投递。"""

    question = render_dsh_event(make_event(QUESTION_MESSAGE), max_characters=12000)
    assert question is not None
    assert question.session_id == "session-1"
    assert question.message_id == "dsh:mux:q-1"
    assert question.received_at == RECEIVED_AT
    assert question.requires_response is True
    assert question.immediate is True
    assert question.text == (
        "DSH 问题请求 (rpcId: q-1)\n"
        "sessionId: session-1\n"
        "[问题 1]\n"
        "id: language\n"
        "header: 选择开发语言\n"
        "question: 请选择开发语言\n"
        "detail: 该选项将决定项目模板\n"
        "multiSelect=true\n"
        "intent: plan-review\n"
        "approve: false\n"
        "- Python: 推荐，生态丰富\n"
        "- Rust: 高性能，编译较慢"
    )
    assert question.extra == {
        "dsh_rpc_id": "q-1",
        "session_id": "session-1",
        "frame_type": "server-request",
        "event_stream": "mux",
        "bridge_sequence": 1,
        "requires_response": True,
        "session_event_type": "question/requested",
    }
    assert question.raw_message == QUESTION_MESSAGE

    approval = render_dsh_event(make_event(APPROVAL_MESSAGE), max_characters=12000)
    assert approval is not None
    assert approval.session_id == "session-1"
    assert approval.message_id == "dsh:mux:a-1"
    assert approval.requires_response is True
    assert approval.immediate is True
    assert approval.text == (
        "DSH 审批请求 (rpcId: a-1)\n"
        "sessionId: session-1\n"
        "approvalId: approval-1\n"
        "toolName: shell_exec\n"
        "callId: call-9\n"
        "reason: 需要在远程主机执行命令\n"
        "请使用 dsh_respond Action 或 /dsh respond 命令处理该审批。"
    )


def test_replayed_requested_frame_renders_stable_message_id() -> None:
    """同一 rpcId 重放（不同 bridge_sequence）必须生成相同 message_id。"""

    first = render_dsh_event(make_event(QUESTION_MESSAGE, sequence=1), max_characters=12000)
    replayed = render_dsh_event(make_event(QUESTION_MESSAGE, sequence=2), max_characters=12000)
    assert first is not None
    assert replayed is not None
    assert first.message_id == replayed.message_id
    assert first.message_id == "dsh:mux:q-1"


def test_resolved_events_render_immediately_without_response_requirement() -> None:
    """question/resolved 与 approval/resolved 立即投递，且不要求结构化响应。"""

    resolved = render_dsh_event(
        make_event(
            {
                "type": "server-request",
                "rpcId": "q-1",
                "method": "session/questions",
                "payload": {"type": "question/resolved", "sessionId": "session-1"},
            }
        ),
        max_characters=12000,
    )
    assert resolved is not None
    assert resolved.immediate is True
    assert resolved.requires_response is False
    assert resolved.message_id == "dsh:mux:q-1"
    assert resolved.text == "DSH 问题已解决 (rpcId: q-1)\nsessionId: session-1"

    approval_resolved = render_dsh_event(
        make_event(
            {
                "type": "server-request",
                "rpcId": "a-1",
                "method": "session/approve",
                "payload": {
                    "type": "approval/resolved",
                    "sessionId": "session-1",
                    "approvalId": "approval-1",
                },
            }
        ),
        max_characters=12000,
    )
    assert approval_resolved is not None
    assert approval_resolved.immediate is True
    assert approval_resolved.requires_response is False
    assert approval_resolved.text == (
        "DSH 审批已解决 (rpcId: a-1)\nsessionId: session-1\napprovalId: approval-1"
    )


def test_turn_end_renders_immediately_with_reason_kind() -> None:
    """session/event 内 event.type == turn/end 立即投递，并携带完成原因与稳定 ID。"""

    rendered = render_dsh_event(make_event(TURN_END_MESSAGE), max_characters=12000)
    assert rendered is not None
    assert rendered.immediate is True
    assert rendered.requires_response is False
    assert rendered.message_id == "dsh:mux:push-abc:turn/end:7"
    assert rendered.text == "DSH 回合结束 (rpcId: push-abc)\nsessionId: session-1\nreason: completed"
    assert rendered.extra["session_event_type"] == "turn/end"


def test_host_agent_error_renders_immediately_with_message() -> None:
    """host/agent-error 归属 session 并立即投递错误信息。"""

    rendered = render_dsh_event(
        make_event(
            {
                "type": "server-request",
                "rpcId": "err-1",
                "method": "host/agent-error",
                "payload": {
                    "type": "host/agent-error",
                    "sessionId": "session-1",
                    "message": "模型调用超时",
                },
            }
        ),
        max_characters=12000,
    )
    assert rendered is not None
    assert rendered.immediate is True
    assert rendered.requires_response is False
    assert rendered.message_id == "dsh:mux:err-1"
    assert rendered.text == (
        "DSH 代理错误 (rpcId: err-1)\nsessionId: session-1\nmessage: 模型调用超时"
    )


@pytest.mark.parametrize(
    "payload_type",
    ["host/workspace-created", "host/workspace-removed", "remote-event", "stream/error"],
)
def test_events_without_session_id_render_none(payload_type: str) -> None:
    """无 sessionId 的 host/workspace-*、remote-event、stream/error 仅缓冲，渲染返回 None。"""

    rendered = render_dsh_event(
        make_event(
            {
                "type": "server-request",
                "rpcId": "r-1",
                "method": payload_type,
                "payload": {"type": payload_type},
            }
        ),
        max_characters=12000,
    )
    assert rendered is None


def test_malformed_envelopes_render_none_safely() -> None:
    """畸形信封（非 server-request、缺 rpcId、payload 非对象、缺 sessionId）一律返回 None。"""

    assert render_dsh_event(make_event({"type": "event/stream"}), 12000) is None
    assert render_dsh_event(make_event({"type": "server-request"}), 12000) is None
    assert (
        render_dsh_event(
            make_event({"type": "server-request", "rpcId": "x", "payload": []}),
            12000,
        )
        is None
    )
    assert (
        render_dsh_event(
            make_event(
                {
                    "type": "server-request",
                    "rpcId": "x",
                    "payload": {"type": "question/requested"},
                }
            ),
            12000,
        )
        is None
    )


def test_renderer_classifies_by_payload_type_not_method() -> None:
    """同一 payload 仅 method 不同时，渲染结果必须完全一致（分类不依赖 method）。"""

    method_a = dict(QUESTION_MESSAGE)
    method_b = dict(QUESTION_MESSAGE)
    method_a["method"] = "session/questions"
    method_b["method"] = "events.mux"
    first = render_dsh_event(make_event(method_a), max_characters=12000)
    second = render_dsh_event(make_event(method_b), max_characters=12000)
    assert first is not None
    assert second is not None
    assert first.text == second.text
    assert first.message_id == second.message_id
    assert first.immediate == second.immediate
    assert first.requires_response == second.requires_response


def test_session_event_push_message_id_appends_event_type_and_seq() -> None:
    """session/event push 的 rpcId 不稳定，message_id 必须追加嵌套 event.type 与 seq。"""

    rendered = render_dsh_event(
        make_session_event("assistant/chunk", 5, rpc_id="push-9"),
        max_characters=12000,
    )
    assert rendered is not None
    assert rendered.immediate is False
    assert rendered.message_id == "dsh:mux:push-9:assistant/chunk:5"
    assert rendered.extra["session_event_type"] == "assistant/chunk"
    assert rendered.text == "DSH 进度：assistant 输出中"


def test_raw_message_does_not_share_reference_with_runtime_event() -> None:
    """raw_message 是独立深拷贝：改写 Runtime 原 event.message 不影响已渲染结果。"""

    event = make_event(QUESTION_MESSAGE)
    rendered = render_dsh_event(event, max_characters=12000)
    assert rendered is not None
    assert rendered.raw_message is not event.message
    event.message["payload"]["questions"][0]["id"] = "mutated"
    assert rendered.raw_message["payload"]["questions"][0]["id"] == "language"


def test_secret_keys_redacted_across_raw_message_text_and_extra() -> None:
    """secret/apiKey/credential 键的值必须从 text、extra、raw_message 全部消失。"""

    message: dict[str, Any] = {
        "type": "server-request",
        "rpcId": "q-2",
        "method": "session/questions",
        "payload": {
            "type": "question/requested",
            "sessionId": "session-1",
            "apiKey": "secret-value",
            "credentials": {"token": "t-1"},
            "questions": [{"id": "a", "question": "选择", "options": [{"label": "X"}]}],
        },
    }
    rendered = render_dsh_event(make_event(message), max_characters=12000)
    assert rendered is not None
    assert "secret-value" not in rendered.text
    assert "secret-value" not in json.dumps(rendered.extra, ensure_ascii=False)
    serialized_raw = json.dumps(rendered.raw_message, ensure_ascii=False)
    assert "secret-value" not in serialized_raw
    assert "t-1" not in serialized_raw
    assert rendered.raw_message["payload"]["apiKey"] == "[REDACTED]"
    assert rendered.raw_message["payload"]["credentials"] == "[REDACTED]"
    assert rendered.raw_message["type"] == "server-request"
    assert rendered.raw_message["rpcId"] == "q-2"
    assert rendered.raw_message["payload"]["sessionId"] == "session-1"
    assert rendered.raw_message["payload"]["questions"][0]["id"] == "a"


@pytest.mark.parametrize(
    "secret_key",
    ["apiKey", "API_KEY", "Api-Key", "api key", "mySecret", "credentialValue"],
)
def test_secret_key_detection_normalizes_case_and_separators(secret_key: str) -> None:
    """脱敏键匹配必须做大小写/分隔符归一，至少覆盖 secret/apiKey/credential。"""

    payload: dict[str, Any] = {
        "type": "question/requested",
        "sessionId": "session-1",
        secret_key: "sensitive-1",
        "questions": [],
    }
    rendered = render_dsh_event(
        make_event(
            {
                "type": "server-request",
                "rpcId": "q-3",
                "method": "session/questions",
                "payload": payload,
            }
        ),
        max_characters=12000,
    )
    assert rendered is not None
    assert rendered.raw_message["payload"][secret_key] == "[REDACTED]"


def test_large_base64_values_are_replaced_with_length_only() -> None:
    """大段合法 Base64 只显示字符长度，原文不得进入文本或 raw_message。"""

    rendered = render_dsh_event(
        make_session_event(
            "tool/call",
            4,
            data={"name": "read_file", "arguments": {"attachment": BLOB, "size": 272}},
        ),
        max_characters=12000,
    )
    assert rendered is not None
    arguments = rendered.raw_message["payload"]["event"]["data"]["arguments"]
    assert arguments["attachment"] == "[base64 364 chars]"
    assert arguments["size"] == 272
    assert BLOB not in rendered.text
    assert BLOB not in json.dumps(rendered.raw_message, ensure_ascii=False)


def test_short_base64_like_values_are_kept() -> None:
    """短字符串即使形似 Base64 也保持原样，只有大段内容才显示长度。"""

    payload: dict[str, Any] = {
        "type": "question/requested",
        "sessionId": "session-1",
        "questions": [{"id": "a", "question": "输入", "detail": "dG9rZW4="}],
    }
    rendered = render_dsh_event(
        make_event(
            {
                "type": "server-request",
                "rpcId": "q-4",
                "method": "session/questions",
                "payload": payload,
            }
        ),
        max_characters=12000,
    )
    assert rendered is not None
    assert "dG9rZW4=" in rendered.text


def test_text_over_max_characters_is_truncated_with_stable_marker() -> None:
    """文本超 max_characters 时使用稳定明确截断标记，且不泄露尾部内容。"""

    payload: dict[str, Any] = {
        "type": "question/requested",
        "sessionId": "session-1",
        "questions": [{"id": "a", "header": "H", "question": "Q", "detail": "Z" * 200}],
    }
    rendered = render_dsh_event(
        make_event(
            {
                "type": "server-request",
                "rpcId": "q-5",
                "method": "session/questions",
                "payload": payload,
            }
        ),
        max_characters=90,
    )
    assert rendered is not None
    assert rendered.text.startswith(
        "DSH 问题请求 (rpcId: q-5)\nsessionId: session-1\n[问题 1]\n"
        "id: a\nheader: H\nquestion: Q\ndetail: "
    )
    assert rendered.text.endswith("\n...[内容过长已截断]")
    assert len(rendered.text) == 90 + len("\n...[内容过长已截断]")
    assert rendered.text.count("Z") == 4


def test_progress_events_aggregate_until_window_expiry_then_single_summary() -> None:
    """高频进度在窗口内不产出，到期后只产出一条不携带敏感内容的摘要。"""

    clock = FakeClock(1000.0)
    aggregator = DshProgressAggregator(window_seconds=2.0, clock=clock)
    events = [
        make_session_event("assistant/chunk", 1, data={"text": "这是逐 token 的增量内容"}),
        make_session_event("assistant/chunk", 2, data={"text": "这是逐 token 的增量内容"}),
        make_session_event("assistant/chunk", 3, data={"text": "这是逐 token 的增量内容"}),
        make_session_event(
            "tool/call",
            4,
            data={
                "name": "web_search",
                "arguments": {"apiKey": "secret-value", "attachment": BLOB},
            },
        ),
        make_session_event("tool/call", 5, data={"name": "file_edit", "arguments": {"path": "/tmp/x.txt"}}),
        make_session_event("tool/result", 6, data={}),
        make_session_event("tool/result", 7, data={"error": "执行失败"}),
        make_session_event("assistant/message", 8, data={"content": "完整消息"}),
    ]
    for event in events[:7]:
        rendered = render_dsh_event(event, max_characters=12000)
        assert rendered is not None
        assert rendered.immediate is False
        assert aggregator.add(rendered) == []
    clock.now = 1001.0
    last = render_dsh_event(events[7], max_characters=12000)
    assert last is not None
    assert aggregator.add(last) == []
    clock.now = 1001.9
    assert aggregator.flush_due(clock.now) == []
    clock.now = 1002.0
    due = aggregator.flush_due(clock.now)
    assert len(due) == 1
    summary = due[0]
    assert summary.session_id == "session-1"
    assert summary.message_id == "dsh:mux:progress:session-1"
    assert summary.received_at == RECEIVED_AT
    assert summary.immediate is False
    assert summary.requires_response is False
    assert summary.text == (
        "DSH 进度摘要 (session-1)：共 8 条事件。\n"
        "assistant/chunk × 3、tool/call × 2、tool/result × 2、assistant/message × 1\n"
        "工具调用：web_search、file_edit\n"
        "工具结果：1 成功、1 失败"
    )
    assert summary.extra == {
        "session_id": "session-1",
        "event_stream": "mux",
        "frame_type": "progress",
        "requires_response": False,
    }
    assert summary.raw_message == {
        "type": "dsh-progress-summary",
        "sessionId": "session-1",
        "eventCounts": {
            "assistant/chunk": 3,
            "tool/call": 2,
            "tool/result": 2,
            "assistant/message": 1,
        },
        "tools": ["web_search", "file_edit"],
        "toolResults": {"ok": 1, "error": 1},
    }
    assert "逐 token" not in summary.text
    serialized_summary = json.dumps(summary.raw_message, ensure_ascii=False)
    assert "secret-value" not in serialized_summary
    assert BLOB not in serialized_summary
    assert "arguments" not in serialized_summary
    assert aggregator.flush_due(clock.now) == []
    assert aggregator.flush_session("session-1") is None


def test_aggregation_is_isolated_per_session() -> None:
    """不同 session 使用独立桶：flush_session 只影响目标 session。"""

    clock = FakeClock(1000.0)
    aggregator = DshProgressAggregator(window_seconds=2.0, clock=clock)
    chunk_a = render_dsh_event(
        make_session_event("assistant/chunk", 1, session_id="session-a"), max_characters=12000
    )
    chunk_b = render_dsh_event(
        make_session_event("assistant/chunk", 1, session_id="session-b"), max_characters=12000
    )
    chunk_a_2 = render_dsh_event(
        make_session_event("assistant/chunk", 2, session_id="session-a"), max_characters=12000
    )
    assert chunk_a is not None
    assert chunk_b is not None
    assert chunk_a_2 is not None
    assert aggregator.add(chunk_a) == []
    assert aggregator.add(chunk_b) == []
    assert aggregator.add(chunk_a_2) == []
    summary_a = aggregator.flush_session("session-a")
    assert summary_a is not None
    assert summary_a.text == "DSH 进度摘要 (session-a)：共 2 条事件。\nassistant/chunk × 2"
    assert "session-b" not in summary_a.text
    clock.now = 1003.0
    due = aggregator.flush_due(clock.now)
    assert len(due) == 1
    assert due[0].session_id == "session-b"
    assert due[0].text == "DSH 进度摘要 (session-b)：共 1 条事件。\nassistant/chunk × 1"


def test_turn_end_passes_immediately_and_adapter_flushes_session_before_delivery() -> None:
    """turn/end 作为立即事件放行；Adapter 随后 flush_session 取出前桶摘要，不重复投递。"""

    clock = FakeClock(1000.0)
    aggregator = DshProgressAggregator(window_seconds=2.0, clock=clock)
    chunk = render_dsh_event(make_session_event("assistant/chunk", 1), max_characters=12000)
    assert chunk is not None
    assert aggregator.add(chunk) == []
    turn_end = render_dsh_event(make_event(TURN_END_MESSAGE), max_characters=12000)
    assert turn_end is not None
    assert aggregator.add(turn_end) == [turn_end]
    summary = aggregator.flush_session("session-1")
    assert summary is not None
    assert summary.text == "DSH 进度摘要 (session-1)：共 1 条事件。\nassistant/chunk × 1"
    assert aggregator.flush_session("session-1") is None
    clock.now = 2000.0
    assert aggregator.flush_due(clock.now) == []


def test_critical_only_mode_drops_progress_without_delay() -> None:
    """critical_only 下普通事件直接丢弃且永不形成摘要，立即事件仍正常返回。"""

    clock = FakeClock(1000.0)
    aggregator = DshProgressAggregator(
        delivery_mode="critical_only", window_seconds=2.0, clock=clock
    )
    chunk = render_dsh_event(make_session_event("assistant/chunk", 1), max_characters=12000)
    assert chunk is not None
    assert aggregator.add(chunk) == []
    clock.now = 2000.0
    assert aggregator.flush_due(clock.now) == []
    assert aggregator.flush_session("session-1") is None
    question = render_dsh_event(make_event(QUESTION_MESSAGE), max_characters=12000)
    assert question is not None
    assert aggregator.add(question) == [question]


def test_aggregator_rejects_invalid_delivery_mode_and_window() -> None:
    """delivery_mode 与 window_seconds 非法时立即抛 ValueError。"""

    with pytest.raises(ValueError):
        DshProgressAggregator(delivery_mode="instant")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        DshProgressAggregator(window_seconds=0)
    with pytest.raises(ValueError):
        DshProgressAggregator(window_seconds=-1.5)
