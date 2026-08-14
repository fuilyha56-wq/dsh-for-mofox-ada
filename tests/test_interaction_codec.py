"""测试 DSH 交互响应 codec（Task 3）：question/approval 响应的 schema 与校验规则。

覆盖成功路径（表驱动精确匹配 DSH 响应 schema）、非法答案、畸形
interaction/payload/kind、取消与审批字面量，以及纯函数不修改 interaction
的契约。所有非法输入断言 ``ValueError`` 且错误文本包含具体 question id。
"""

from __future__ import annotations

import copy
from typing import Any

import pytest

from plugins.dsh_adapter.interaction_codec import (
    build_approval_response,
    build_question_cancellation,
    build_question_response,
)
from plugins.dsh_adapter.interactions import DshPendingInteraction

RECEIVED_AT = "2026-08-15T00:00:00+00:00"

QUESTION_PAYLOAD: dict[str, Any] = {
    "type": "question/requested",
    "sessionId": "session-1",
    "questions": [
        {
            "id": "language",
            "question": "选择语言",
            "options": [{"label": "Python"}, {"label": "Rust"}],
        },
        {
            "id": "features",
            "question": "选择功能",
            "options": [{"label": "Tests"}, {"label": "Docs"}],
            "multiSelect": True,
        },
        {"id": "note", "question": "补充说明"},
    ],
}

APPROVAL_PAYLOAD: dict[str, Any] = {
    "type": "approval/requested",
    "sessionId": "session-1",
    "approvalId": "approval-1",
    "toolName": "bash",
    "callId": "call-1",
    "reason": "执行删除操作",
}


def make_question(**overrides: Any) -> DshPendingInteraction:
    """构造手写的 question 交互，支持按字段覆盖。"""

    fields: dict[str, Any] = {
        "rpc_id": "rpc-1",
        "session_id": "session-1",
        "kind": "question",
        "payload": copy.deepcopy(QUESTION_PAYLOAD),
        "stream": "mux",
        "first_seen_at": RECEIVED_AT,
        "last_seen_at": RECEIVED_AT,
    }
    fields.update(overrides)
    return DshPendingInteraction(**fields)


def make_approval(**overrides: Any) -> DshPendingInteraction:
    """构造手写的 approval 交互，支持按字段覆盖。"""

    fields: dict[str, Any] = {
        "rpc_id": "rpc-2",
        "session_id": "session-1",
        "kind": "approval",
        "payload": copy.deepcopy(APPROVAL_PAYLOAD),
        "stream": "mux",
        "first_seen_at": RECEIVED_AT,
        "last_seen_at": RECEIVED_AT,
        "approval_id": "approval-1",
    }
    fields.update(overrides)
    return DshPendingInteraction(**fields)


def full_answers() -> list[dict[str, Any]]:
    """构造覆盖全部三道题的合法答案。"""

    return [
        {"id": "language", "selected": ["Python"]},
        {"id": "features", "selected": ["Tests", "Docs"]},
        {"id": "note", "selected": [], "custom": "加个注释"},
    ]


@pytest.mark.parametrize(
    ("answers", "expected"),
    [
        # 单选/多选/无 options + custom 全量作答
        (
            full_answers(),
            [
                {"id": "language", "selected": ["Python"]},
                {"id": "features", "selected": ["Tests", "Docs"]},
                {"id": "note", "selected": [], "custom": "加个注释"},
            ],
        ),
        # 单选最多一项；custom 未提供时输出项不含 custom 键
        (
            [
                {"id": "language", "selected": ["Rust"]},
                {"id": "features", "selected": ["Tests"]},
                {"id": "note", "selected": [], "custom": "说明"},
            ],
            [
                {"id": "language", "selected": ["Rust"]},
                {"id": "features", "selected": ["Tests"]},
                {"id": "note", "selected": [], "custom": "说明"},
            ],
        ),
        # 单选/多选允许 0 selected，但必须带非空 custom
        (
            [
                {"id": "language", "selected": [], "custom": "都不要"},
                {"id": "features", "selected": [], "custom": "都不选"},
                {"id": "note", "selected": [], "custom": "自由文本"},
            ],
            [
                {"id": "language", "selected": [], "custom": "都不要"},
                {"id": "features", "selected": [], "custom": "都不选"},
                {"id": "note", "selected": [], "custom": "自由文本"},
            ],
        ),
        # custom 输出 trim 首尾空白（固定 trim 语义）
        (
            [
                {"id": "language", "selected": ["Python"], "custom": "  hello  "},
                {"id": "features", "selected": ["Docs"], "custom": "\tworld\n"},
                {"id": "note", "selected": [], "custom": "  note  "},
            ],
            [
                {"id": "language", "selected": ["Python"], "custom": "hello"},
                {"id": "features", "selected": ["Docs"], "custom": "world"},
                {"id": "note", "selected": [], "custom": "note"},
            ],
        ),
    ],
)
def test_build_question_response_matches_dsh_schema(
    answers: list[dict[str, Any]], expected: list[dict[str, Any]]
) -> None:
    """成功答案精确等于 DSH questionResponse schema，不包含内部字段。"""

    result = build_question_response(make_question(), answers)
    assert result == {
        "ok": True,
        "value": {"sessionId": "session-1", "answer": {"answers": expected}},
    }
    for item in result["value"]["answer"]["answers"]:
        assert set(item) <= {"id", "selected", "custom"}


@pytest.mark.parametrize(
    ("answers", "id_hint"),
    [
        # answers 本身不是数组
        ({"id": "language", "selected": []}, None),
        # 答案项不是对象
        (["not-a-dict"], None),
        # 答案缺少 id
        ([{"selected": ["Python"]}, *full_answers()[1:]], None),
        # 漏题：note 未作答
        (full_answers()[:2], "note"),
        # 重复作答同一题
        ([*full_answers()[:1], *full_answers()[:1], full_answers()[2]], "language"),
        # 未知 id
        ([{"id": "ghost", "selected": []}, *full_answers()[1:]], "ghost"),
        # selected 不是数组
        ([{"id": "language", "selected": "Python"}, *full_answers()[1:]], "language"),
        # selected 项为数字
        ([{"id": "language", "selected": [1]}, *full_answers()[1:]], "language"),
        # selected 项为布尔
        ([{"id": "language", "selected": [True]}, *full_answers()[1:]], "language"),
        # selected 重复项
        ([{"id": "language", "selected": ["Python", "Python"]}, *full_answers()[1:]], "language"),
        # 单选传多个
        ([{"id": "language", "selected": ["Python", "Rust"]}, *full_answers()[1:]], "language"),
        # label 不在 options 中
        ([{"id": "language", "selected": ["Go"]}, *full_answers()[1:]], "language"),
        # 无 options 却给了 selected
        ([full_answers()[0], full_answers()[1], {"id": "note", "selected": ["x"], "custom": "c"}], "note"),
        # 无 options 缺 custom
        ([full_answers()[0], full_answers()[1], {"id": "note", "selected": []}], "note"),
        # 无 options 空白 custom
        (
            [full_answers()[0], full_answers()[1], {"id": "note", "selected": [], "custom": "   "}],
            "note",
        ),
        # 有 options 的题 selected 为空且 custom 缺失
        ([{"id": "language", "selected": []}, *full_answers()[1:]], "language"),
        # custom 不是字符串
        ([{"id": "features", "selected": [], "custom": 123}, *full_answers()[::2]], "features"),
    ],
)
def test_build_question_response_rejects_invalid_answers(
    answers: list[dict[str, Any]], id_hint: str | None
) -> None:
    """非法答案一律 ValueError，且错误信息包含具体 question id。"""

    with pytest.raises(ValueError) as excinfo:
        build_question_response(make_question(), answers)
    if id_hint is not None:
        assert id_hint in str(excinfo.value)


@pytest.mark.parametrize(
    "interaction",
    [
        None,
        {"rpc_id": "rpc-1"},
        make_approval(),
        make_question(kind="approval"),
        make_question(payload={"type": "approval/requested"}),
        make_question(payload=123),
        make_question(payload={"type": "question/requested"}),
        make_question(payload={"type": "question/requested", "questions": "oops"}),
        make_question(
            payload={"type": "question/requested", "questions": [{"question": "缺 id"}]}
        ),
        make_question(
            payload={"type": "question/requested", "questions": [{"id": ""}]}
        ),
        make_question(
            payload={"type": "question/requested", "questions": [{"id": "a"}, {"id": "a"}]}
        ),
        make_question(
            payload={
                "type": "question/requested",
                "questions": [{"id": "a", "multiSelect": "yes"}],
            }
        ),
        make_question(
            payload={
                "type": "question/requested",
                "questions": [{"id": "a", "options": "nope"}],
            }
        ),
        make_question(
            payload={
                "type": "question/requested",
                "questions": [{"id": "a", "options": [{"x": 1}]}],
            }
        ),
    ],
)
def test_build_question_response_rejects_malformed_interaction(
    interaction: object,
) -> None:
    """畸形 interaction/payload/kind 必须 ValueError，不得泄漏 KeyError/TypeError。"""

    with pytest.raises(ValueError):
        build_question_response(interaction, full_answers())


@pytest.mark.parametrize(
    "interaction",
    [
        None,
        make_approval(),
        make_question(payload={"type": "approval/requested"}),
        make_question(payload=123),
        make_question(
            payload={"type": "question/requested", "questions": "oops"}
        ),
    ],
)
def test_build_question_cancellation_rejects_malformed(interaction: object) -> None:
    """取消只接受合法 question 交互，其余一律 ValueError。"""

    with pytest.raises(ValueError):
        build_question_cancellation(interaction)


def test_build_question_cancellation_matches_dsh_schema() -> None:
    """取消响应精确等于 DSH 字面对象。"""

    result = build_question_cancellation(make_question())
    assert result == {
        "ok": False,
        "error": {"code": "cancelled", "message": "MoFox cancelled the DSH question"},
    }


@pytest.mark.parametrize("outcome", ["allowed-once", "rejected"])
def test_build_approval_response_matches_dsh_schema(outcome: str) -> None:
    """审批 value 精确等于 DSH 字面对象。"""

    result = build_approval_response(make_approval(), outcome)
    assert result == {
        "ok": True,
        "value": {
            "sessionId": "session-1",
            "approvalId": "approval-1",
            "outcome": outcome,
        },
    }


@pytest.mark.parametrize(
    "outcome",
    ["allow", "ALLOWED_ONCE", "permanent", "allowed", "", None, 1],
)
def test_build_approval_response_rejects_unknown_outcome(outcome: object) -> None:
    """非 allowed-once/rejected 的 outcome 必须失败。"""

    with pytest.raises(ValueError) as excinfo:
        build_approval_response(make_approval(), outcome)  # type: ignore[arg-type]
    assert "allowed-once" in str(excinfo.value)


@pytest.mark.parametrize(
    "interaction",
    [
        None,
        make_question(),
        make_approval(kind="question"),
        make_approval(approval_id=None),
        make_approval(approval_id=""),
        make_approval(payload={"type": "question/requested"}),
        make_approval(payload=123),
    ],
)
def test_build_approval_response_rejects_malformed(interaction: object) -> None:
    """审批只接受带 approval_id 的 approval 交互，其余一律 ValueError。"""

    with pytest.raises(ValueError):
        build_approval_response(interaction, "allowed-once")


def test_codecs_are_pure_and_do_not_mutate_interaction() -> None:
    """纯函数契约：调用前后 interaction 及其 payload 逐字节不变。"""

    question = make_question()
    question_payload_before = copy.deepcopy(question.payload)
    build_question_response(question, full_answers())
    build_question_cancellation(question)
    assert question.payload == question_payload_before
    assert question.state == "pending"

    approval = make_approval()
    approval_payload_before = copy.deepcopy(approval.payload)
    build_approval_response(approval, "rejected")
    assert approval.payload == approval_payload_before
    assert approval.state == "pending"
