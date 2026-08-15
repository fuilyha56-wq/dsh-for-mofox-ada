"""向其他 Neo-MoFox 插件提供 DSH Adapter 公共服务。"""

from __future__ import annotations

from typing import Any, Protocol, cast

from src.app.plugin_system.base import BaseService

from .adapter import DshInteractionResponder
from .interactions import DshInteractionRegistry, DshPendingInteraction
from .operations import DshOperationDispatcher


class _DshAdapterPluginProtocol(Protocol):
    """公共服务依赖的最小插件接口。"""

    dispatcher: DshOperationDispatcher
    interaction_registry: DshInteractionRegistry
    interaction_responder: DshInteractionResponder


def _interaction_audit_record(interaction: DshPendingInteraction) -> dict[str, Any]:
    """返回不含原始题面或凭据的 pending 交互审计字段。"""

    return {
        "rpc_id": interaction.rpc_id,
        "session_id": interaction.session_id,
        "kind": interaction.kind,
        "state": interaction.state,
        "approval_id": interaction.approval_id,
    }


class DshAdapterService(BaseService):
    """允许其他插件通过组件签名访问全部 DSH 能力。"""

    name = "dsh_adapter"
    description = "调用 DSH 模型、RPC、HTTP、CLI、进程、事件流和数据能力"
    version = "1.0.0"

    @property
    def dispatcher(self) -> DshOperationDispatcher:
        """返回插件共享的操作分派器。"""

        return cast(_DshAdapterPluginProtocol, self.plugin).dispatcher

    @property
    def interaction_registry(self) -> DshInteractionRegistry:
        """返回插件共享的 pending interaction registry。"""

        return cast(_DshAdapterPluginProtocol, self.plugin).interaction_registry

    @property
    def interaction_responder(self) -> DshInteractionResponder:
        """返回插件共享的结构化 interaction responder。"""

        return cast(_DshAdapterPluginProtocol, self.plugin).interaction_responder

    async def list_pending(
        self, session_id: str | None = None
    ) -> list[dict[str, Any]]:
        """列出 pending 交互的无题面审计记录。"""

        records = await self.interaction_registry.list_pending(session_id)
        return [_interaction_audit_record(record) for record in records]

    async def answer_question(
        self,
        rpc_id: str,
        answers: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """以 Service 身份提交一条 question 的结构化答案。"""

        return await self.interaction_responder.respond_question(rpc_id, answers)

    async def cancel_question(self, rpc_id: str) -> dict[str, Any]:
        """以 Service 身份取消一条 pending question。"""

        return await self.interaction_responder.cancel_question(rpc_id)

    async def respond_approval(
        self,
        rpc_id: str,
        outcome: str,
    ) -> dict[str, Any]:
        """以固定 Service 身份回应一条 pending approval。"""

        return await self.interaction_responder.respond_approval(
            rpc_id, outcome, actor="service"
        )

    async def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """执行任意桥接操作。"""

        return await self.dispatcher.execute(operation, parameters)

    async def rpc_call(
        self,
        method: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """调用任意 DSH 一元 RPC。"""

        return await self.execute(
            "rpc_call",
            {"method": method, "payload": payload or {}},
        )

    async def rpc_catalog(
        self,
        *,
        method: str | None = None,
        domain: str | None = None,
        risk: str | None = None,
        include_details: bool = False,
    ) -> dict[str, Any]:
        """查询 DSH RPC 方法、payload、约束与风险目录。"""

        parameters: dict[str, Any] = {"include_details": include_details}
        for key, value in {
            "method": method,
            "domain": domain,
            "risk": risk,
        }.items():
            if value is not None:
                parameters[key] = value
        return await self.execute("rpc_catalog", parameters)

    async def list_sessions(self) -> dict[str, Any]:
        """列出 DSH 会话及其会话 ID。"""

        return await self.execute("session_list")

    async def list_models(self, session_id: str | None = None) -> dict[str, Any]:
        """列出 DSH 主机或指定会话的实时模型目录。"""

        parameters = {"session_id": session_id} if session_id else None
        return await self.execute("model_list", parameters)

    async def switch_model(
        self,
        session_id: str,
        model: str,
        *,
        reasoning_effort: str | None = None,
        provider: str | None = None,
    ) -> dict[str, Any]:
        """按实时目录解析并切换指定 DSH 会话模型。"""

        parameters = {"session_id": session_id, "model": model}
        if reasoning_effort:
            parameters["reasoning_effort"] = reasoning_effort
        if provider:
            parameters["provider"] = provider
        return await self.execute("model_switch", parameters)

    async def list_presets(self) -> dict[str, Any]:
        """列出 DSH 实时提供的 Agent preset 模式。"""

        return await self.execute("preset_list")

    async def switch_preset(
        self,
        session_id: str,
        preset: str,
    ) -> dict[str, Any]:
        """切换指定空白 DSH 会话的 Agent preset。"""

        return await self.execute(
            "preset_switch",
            {"session_id": session_id, "preset": preset},
        )

    async def run_headless(
        self,
        task: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """运行一次 DSH headless 任务。"""

        parameters: dict[str, Any] = {"task": task}
        if cwd is not None:
            parameters["cwd"] = cwd
        if timeout is not None:
            parameters["timeout"] = timeout
        return await self.execute("headless", parameters)

    async def run_cli(
        self,
        arguments: list[str],
        *,
        cwd: str | None = None,
        stdin: str | None = None,
        environment: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """执行任意 DSH CLI 参数。"""

        parameters: dict[str, Any] = {"arguments": arguments}
        for key, value in {
            "cwd": cwd,
            "stdin": stdin,
            "environment": environment,
            "timeout": timeout,
        }.items():
            if value is not None:
                parameters[key] = value
        return await self.execute("cli_run", parameters)
