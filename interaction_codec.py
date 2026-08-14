"""DSH 交互响应编解码：把 MoFox 侧答案/审批转成 DSH ``/api/respond`` 载荷。

纯函数模块：只做校验与形状转换，不修改传入 interaction，不访问网络、registry
或策略权限。question codec 仅接受 ``kind="question"`` 的交互，approval codec
仅接受 ``kind="approval"`` 且带 ``approval_id`` 的交互；一切畸形输入以
``ValueError``（错误信息含具体 question id）表达，绝不泄漏 KeyError/TypeError。
"""

from __future__ import annotations

from typing import Any

from .interactions import DshPendingInteraction

QUESTION_PAYLOAD_TYPE = "question/requested"
APPROVAL_PAYLOAD_TYPE = "approval/requested"

APPROVAL_OUTCOMES: frozenset[str] = frozenset({"allowed-once", "rejected"})

CANCELLED_CODE = "cancelled"
CANCELLED_MESSAGE = "MoFox cancelled the DSH question"


def _require_question_payload(interaction: DshPendingInteraction) -> dict[str, Any]:
    """校验交互为合法 question 交互并返回其 payload。

    非 ``DshPendingInteraction``、kind 非 ``question``、payload 非 JSON 对象或
    payload type 非 ``question/requested`` 时抛 ``ValueError``。
    """

    if not isinstance(interaction, DshPendingInteraction):
        raise ValueError(
            f"期望 DshPendingInteraction，实际得到 {type(interaction).__name__}"
        )
    if interaction.kind != "question":
        raise ValueError(
            f"交互 {interaction.rpc_id!r} 的 kind 为 {interaction.kind!r}，"
            "question codec 仅接受 kind='question'"
        )
    payload = interaction.payload
    if not isinstance(payload, dict):
        raise ValueError(f"交互 {interaction.rpc_id!r} 的 payload 不是 JSON 对象")
    if payload.get("type") != QUESTION_PAYLOAD_TYPE:
        raise ValueError(
            f"交互 {interaction.rpc_id!r} 的 payload.type 不是 {QUESTION_PAYLOAD_TYPE!r}"
        )
    return payload


def _validate_questions(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """校验 payload.questions 形状并提取作答所需的题面规格。

    要求 questions 为数组，每题是对象、id 非空字符串且全局唯一；options 若存在
    必须是数组且每项含非空 label；multiSelect 若存在必须是布尔值。
    """

    raw_questions = payload.get("questions")
    if not isinstance(raw_questions, list):
        raise ValueError("payload.questions 必须是数组")
    questions: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for item in raw_questions:
        if not isinstance(item, dict):
            raise ValueError("payload.questions 的每一项必须是对象")
        question_id = item.get("id")
        if not isinstance(question_id, str) or not question_id:
            raise ValueError("question 的 id 必须是非空字符串")
        if question_id in seen_ids:
            raise ValueError(f"question id 重复: {question_id!r}")
        seen_ids.add(question_id)
        labels: list[str] = []
        options = item.get("options")
        if options is not None:
            if not isinstance(options, list):
                raise ValueError(f"question {question_id!r} 的 options 必须是数组")
            for option in options:
                if not isinstance(option, dict):
                    raise ValueError(
                        f"question {question_id!r} 的 options 项必须是对象"
                    )
                label = option.get("label")
                if not isinstance(label, str) or not label:
                    raise ValueError(
                        f"question {question_id!r} 的 option 缺少非空 label"
                    )
                labels.append(label)
        multi_select = item.get("multiSelect", False)
        if not isinstance(multi_select, bool):
            raise ValueError(
                f"question {question_id!r} 的 multiSelect 必须是布尔值"
            )
        questions.append(
            {"id": question_id, "labels": labels, "multi_select": multi_select}
        )
    return questions


def _validate_answer(
    answer: dict[str, Any], question: dict[str, Any]
) -> dict[str, Any]:
    """校验单个答案并返回仅含 id/selected/custom 的输出项。

    要求 selected 为字符串数组且不重复、label 必须来自题面 options；单选最多
    一项；无 options 的题 selected 必须为空且 custom 非空；任何题都不能
    selected 为空且 custom 缺失/空白。custom 输出 trim 首尾空白，仅在调用方
    实际提供时出现。所有错误信息包含 question id。
    """

    question_id = question["id"]
    selected = answer.get("selected", [])
    if not isinstance(selected, list):
        raise ValueError(f"question {question_id!r} 的 selected 必须是数组")
    selected_out: list[str] = []
    for label in selected:
        if not isinstance(label, str):
            raise ValueError(
                f"question {question_id!r} 的 selected 项必须是字符串"
            )
        if label in selected_out:
            raise ValueError(
                f"question {question_id!r} 的 selected 包含重复项: {label!r}"
            )
        selected_out.append(label)

    labels = question["labels"]
    if labels:
        for label in selected_out:
            if label not in labels:
                raise ValueError(
                    f"question {question_id!r} 的 selected 标签 {label!r} "
                    "不在 options 中"
                )
        if not question["multi_select"] and len(selected_out) > 1:
            raise ValueError(
                f"question {question_id!r} 是单选，最多选择一个标签"
            )
    elif selected_out:
        raise ValueError(
            f"question {question_id!r} 没有 options，selected 必须为空"
        )

    custom_value: str | None = None
    if "custom" in answer:
        custom = answer["custom"]
        if not isinstance(custom, str):
            raise ValueError(f"question {question_id!r} 的 custom 必须是字符串")
        stripped = custom.strip()
        if not stripped:
            raise ValueError(f"question {question_id!r} 的 custom 为空白")
        custom_value = stripped
    if not labels and custom_value is None:
        raise ValueError(
            f"question {question_id!r} 没有 options，必须提供非空 custom"
        )
    if not selected_out and custom_value is None:
        raise ValueError(
            f"question {question_id!r} 的答案既无 selected 也无 custom"
        )

    item: dict[str, Any] = {"id": question_id, "selected": selected_out}
    if custom_value is not None:
        item["custom"] = custom_value
    return item


def build_question_response(
    interaction: DshPendingInteraction, answers: list[dict[str, Any]]
) -> dict[str, Any]:
    """把答案列表转成 DSH 问题响应 result。

    每个原始 question id 必须恰好作答一次；输出 value 为
    ``{"sessionId", "answer": {"answers": [...]}}``，答案项仅含 id/selected
    （custom 仅在调用方提供时出现），不泄露 rpc_id/stream/state 等内部字段。
    纯函数，不修改 interaction。
    """

    payload = _require_question_payload(interaction)
    questions = _validate_questions(payload)
    if not isinstance(answers, list):
        raise ValueError("answers 必须是数组")
    by_id = {q["id"]: q for q in questions}
    answered: set[str] = set()
    output_answers: list[dict[str, Any]] = []
    for answer in answers:
        if not isinstance(answer, dict):
            raise ValueError("answers 的每一项必须是对象")
        question_id = answer.get("id")
        if not isinstance(question_id, str) or not question_id:
            raise ValueError("answers 每一项的 id 必须是非空字符串")
        if question_id not in by_id:
            raise ValueError(f"未知 question id: {question_id!r}")
        if question_id in answered:
            raise ValueError(f"question {question_id!r} 被重复作答")
        answered.add(question_id)
        output_answers.append(_validate_answer(answer, by_id[question_id]))
    missing = sorted(set(by_id) - answered)
    if missing:
        raise ValueError(f"以下 question 未作答: {missing!r}")
    return {
        "ok": True,
        "value": {
            "sessionId": interaction.session_id,
            "answer": {"answers": output_answers},
        },
    }


def build_question_cancellation(
    interaction: DshPendingInteraction,
) -> dict[str, Any]:
    """构造取消问题的 DSH 响应，精确返回字面对象。

    只接受合法 question 交互；畸形交互、payload 或 kind 一律 ValueError。
    """

    payload = _require_question_payload(interaction)
    _validate_questions(payload)
    return {
        "ok": False,
        "error": {"code": CANCELLED_CODE, "message": CANCELLED_MESSAGE},
    }


def build_approval_response(
    interaction: DshPendingInteraction, outcome: str
) -> dict[str, Any]:
    """把审批结果转成 DSH 审批响应 result。

    只接受带非空 approval_id 的 approval 交互；outcome 仅允许
    ``allowed-once`` / ``rejected``。输出 value 精确为
    ``{"sessionId", "approvalId", "outcome"}``。
    """

    if not isinstance(interaction, DshPendingInteraction):
        raise ValueError(
            f"期望 DshPendingInteraction，实际得到 {type(interaction).__name__}"
        )
    if interaction.kind != "approval":
        raise ValueError(
            f"交互 {interaction.rpc_id!r} 的 kind 为 {interaction.kind!r}，"
            "approval codec 仅接受 kind='approval'"
        )
    payload = interaction.payload
    if not isinstance(payload, dict):
        raise ValueError(f"交互 {interaction.rpc_id!r} 的 payload 不是 JSON 对象")
    if payload.get("type") != APPROVAL_PAYLOAD_TYPE:
        raise ValueError(
            f"交互 {interaction.rpc_id!r} 的 payload.type 不是 {APPROVAL_PAYLOAD_TYPE!r}"
        )
    approval_id = interaction.approval_id
    if not isinstance(approval_id, str) or not approval_id:
        raise ValueError(f"交互 {interaction.rpc_id!r} 缺少非空 approval_id")
    if not isinstance(outcome, str) or outcome not in APPROVAL_OUTCOMES:
        raise ValueError(
            f"审批 outcome 必须为 'allowed-once' 或 'rejected'，实际 {outcome!r}"
        )
    return {
        "ok": True,
        "value": {
            "sessionId": interaction.session_id,
            "approvalId": approval_id,
            "outcome": outcome,
        },
    }
