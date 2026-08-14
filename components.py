"""DSH Adapter 的 LLM Tool、Action 与聊天命令组件。"""

from __future__ import annotations

import json
from typing import Any, Protocol, cast

from src.app.plugin_system.api.send_api import send_text
from src.app.plugin_system.base import BaseAction, BaseCommand, BaseTool, cmd_route
from src.app.plugin_system.types import PermissionLevel

from .config import DshBridgeConfig
from .operations import DshOperationDispatcher, READ_ONLY_OPERATIONS, SUPPORTED_OPERATIONS

_USAGE = """/dsh 用法：
  /dsh status
    /dsh methods [domain|method]
    /dsh sessions
    /dsh models [session_id]
    /dsh model <session_id> <model> [reasoning_effort] [provider]
    /dsh presets
    /dsh preset <session_id> <preset>
  /dsh headless "任务文本" [工作目录]
  /dsh rpc <方法> '<payload JSON>'
  /dsh exec <操作> '<parameters JSON>'
  /dsh cli '<arguments JSON数组>' [工作目录]
  /dsh processes
  /dsh output <process_id> [after_sequence]
  /dsh stop <process_id>
  /dsh events <mux|host> [after_sequence]
  /dsh data <相对DSH_HOME路径>
  /dsh help

完整操作：""" + ", ".join(SUPPORTED_OPERATIONS)


class _DshAdapterPluginProtocol(Protocol):
    """LLM 与命令组件依赖的最小插件接口。"""

    config: DshBridgeConfig
    dispatcher: DshOperationDispatcher


def _plugin(component: BaseTool | BaseAction | BaseCommand) -> _DshAdapterPluginProtocol:
    """返回带具体类型的 DSH Adapter 插件实例。"""

    return cast(_DshAdapterPluginProtocol, component.plugin)


def _parse_object(raw: str, name: str) -> dict[str, Any]:
    """解析 JSON 对象参数。"""

    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} 不是有效 JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{name} 必须是 JSON 对象")
    return value


def _parse_string_list(raw: str, name: str) -> list[str]:
    """解析 JSON 字符串数组参数。"""

    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} 不是有效 JSON: {exc}") from exc
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{name} 必须是 JSON 字符串数组")
    return value


def _render(value: dict[str, Any], limit: int) -> str:
    """将结构化结果渲染为有长度上限的 JSON。"""

    text = json.dumps(value, ensure_ascii=False, indent=2, default=str)
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n... [已截断 {len(text) - limit} 个字符]"


class DshQueryTool(BaseTool):
    """向 LLM 暴露 DSH 状态、输出、事件和数据查询。"""

    name = "dsh_query"
    description = (
        "查询 DeepSeek Harness 完整 RPC 能力目录、状态、会话、真实可用模型、"
        "Agent preset 模式、"
        "长期进程输出、事件流或 DSH_HOME 数据。operation 仅支持 "
        "status/rpc_catalog/session_list/model_list/preset_list/"
        "process_output/process_list/event_status/event_read/data_list/data_read；"
        "调用任意 DSH RPC 前先用 rpc_catalog 查询 method 或 domain，可获得 payload "
        "schema、约束和风险级别；"
        "model_list 可传可选 session_id；preset_list 返回标准、PTC、极简、创造等"
        "实时模式；parameters_json 是对应参数的 JSON 对象。"
    )

    async def execute(
        self,
        operation: str,
        parameters_json: str = "{}",
    ) -> tuple[bool, str | dict]:
        """执行只读 DSH 查询。

        Args:
            operation: 只读操作名称。
            parameters_json: 操作参数 JSON 对象字符串。
        """

        if operation not in READ_ONLY_OPERATIONS:
            return False, f"dsh_query 不允许有副作用的操作: {operation}"
        plugin = _plugin(self)
        try:
            result = await plugin.dispatcher.execute(
                operation,
                _parse_object(parameters_json, "parameters_json"),
            )
        except Exception as exc:
            return False, f"DSH 查询失败: {exc}"
        return True, result


class DshHeadlessAction(BaseAction):
    """让 LLM 直接委派一个任务给 DSH headless Agent。"""

    name = "dsh_headless"
    description = (
        "让 DeepSeek Harness headless Agent 在指定工作目录完成一个任务并返回最终输出。"
        "适合代码分析、修改、测试和其他 DSH 能完成的任务。"
    )
    primary_action = False
    associated_types = ["text"]

    async def execute(
        self,
        task: str,
        cwd: str = "",
        timeout_seconds: float = 300.0,
    ) -> tuple[bool, str]:
        """执行一次 DSH headless 任务。

        Args:
            task: 交给 DSH Agent 的完整任务描述。
            cwd: DSH 工作目录；留空使用插件配置默认值。
            timeout_seconds: 最长等待秒数，受插件配置上限约束。
        """

        parameters: dict[str, Any] = {"task": task, "timeout": timeout_seconds}
        if cwd.strip():
            parameters["cwd"] = cwd
        plugin = _plugin(self)
        try:
            result = await plugin.dispatcher.execute("headless", parameters)
        except Exception as exc:
            return False, f"DSH headless 调用失败: {exc}"
        command_result = result["result"]
        return (
            command_result.get("exit_code") == 0,
            _render(result, plugin.config.llm.max_result_characters),
        )


class DshModelSwitchAction(BaseAction):
    """让 LLM 按 DSH 实时目录切换指定会话模型。"""

    name = "dsh_model_switch"
    description = (
        "切换指定 DeepSeek Harness 会话的模型。必须使用 dsh_query 的 session_list "
        "取得 session_id；模型名称应先通过 model_list 查询。provider 可留空自动从"
        "实时目录解析，例如 model=deepseek-v4-flash。"
    )
    primary_action = False
    associated_types = ["text"]

    async def execute(
        self,
        session_id: str,
        model: str,
        reasoning_effort: str = "",
        provider: str = "",
    ) -> tuple[bool, str]:
        """切换 DSH 会话模型。

        Args:
            session_id: 由 session_list 返回的 DSH 会话 ID。
            model: 由 model_list 返回的模型 ID 或显示名。
            reasoning_effort: 可选推理等级，例如 off、high 或 max。
            provider: 可选 provider ID；留空时从模型目录唯一解析。
        """

        parameters = {"session_id": session_id, "model": model}
        if reasoning_effort.strip():
            parameters["reasoning_effort"] = reasoning_effort
        if provider.strip():
            parameters["provider"] = provider
        plugin = _plugin(self)
        try:
            result = await plugin.dispatcher.execute("model_switch", parameters)
        except Exception as exc:
            return False, f"DSH 模型切换失败: {exc}"
        rpc_result = result.get("result", {})
        return (
            isinstance(rpc_result, dict) and rpc_result.get("ok") is True,
            _render(result, plugin.config.llm.max_result_characters),
        )


class DshPresetSwitchAction(BaseAction):
    """让 LLM 按 DSH 实时目录切换指定空白会话模式。"""

    name = "dsh_preset_switch"
    description = (
        "切换指定 DeepSeek Harness 空白会话的 Agent preset 模式。先使用 dsh_query "
        "的 session_list 选择 blank=true 的会话，再用 preset_list 查询模式。支持"
        "目录 ID 或显示名，例如 standard/标准模式、code/PTC 模式、minimal/极简模式、"
        "cordis/创造模式。已有对话的非空白会话无法切换模式。"
    )
    primary_action = False
    associated_types = ["text"]

    async def execute(
        self,
        session_id: str,
        preset: str,
    ) -> tuple[bool, str]:
        """切换 DSH 空白会话的 Agent preset。

        Args:
            session_id: 由 session_list 返回且 blank=true 的 DSH 会话 ID。
            preset: 由 preset_list 返回的模式 ID 或显示名。
        """

        plugin = _plugin(self)
        try:
            result = await plugin.dispatcher.execute(
                "preset_switch",
                {"session_id": session_id, "preset": preset},
            )
        except Exception as exc:
            return False, f"DSH 模式切换失败: {exc}"
        rpc_result = result.get("result", {})
        return (
            isinstance(rpc_result, dict) and rpc_result.get("ok") is True,
            _render(result, plugin.config.llm.max_result_characters),
        )


class DshRpcAction(BaseAction):
    """让 LLM 调用任意当前或未来 DSH 一元 RPC。"""

    name = "dsh_rpc"
    description = (
        "调用任意 DeepSeek Harness 一元 RPC，覆盖 session、subagent、host、workspace、"
        "skill、agentPreset、goal、settings、credentials 和 llm 全部方法。先使用 "
        "dsh_query 的 rpc_catalog 查询精确 method、payload_schema、约束和风险级别，再将"
        "业务 payload 作为 parameters_json 传入。方法目录当前包含 52 项，但本 Action "
        "不设方法白名单，因此兼容 DSH 后续新增 RPC。privileged/write 动作会修改 DSH "
        "状态；不要通过模型传入 credentials.set 或 llm.discoverModels 的 secret 值。"
    )
    primary_action = False
    associated_types = ["text"]

    async def execute(
        self,
        method: str,
        payload_json: str = "{}",
    ) -> tuple[bool, str]:
        """调用任意 DSH RPC。

        Args:
            method: rpc_catalog 返回的完整 RPC 方法名。
            payload_json: 该方法的业务 payload JSON 对象字符串。
        """

        plugin = _plugin(self)
        try:
            result = await plugin.dispatcher.execute(
                "rpc_call",
                {
                    "method": method,
                    "payload": _parse_object(payload_json, "payload_json"),
                },
            )
        except Exception as exc:
            return False, f"DSH RPC 调用失败: {exc}"
        rpc_result = result.get("result", {})
        return (
            isinstance(rpc_result, dict) and rpc_result.get("ok") is True,
            _render(result, plugin.config.llm.max_result_characters),
        )


class DshOperateAction(BaseAction):
    """向 LLM 暴露全部 DSH 有副作用操作。"""

    name = "dsh_operate"
    description = (
        "执行任意 DeepSeek Harness 操作，包括 RPC、交互响应、HTTP、CLI、模式切换、"
        "长期进程、事件订阅和数据访问。operation 可选："
        + ", ".join(SUPPORTED_OPERATIONS)
        + "。parameters_json 必须是对应参数的 JSON 对象。"
    )
    primary_action = False
    associated_types = ["text"]

    async def execute(
        self,
        operation: str,
        parameters_json: str = "{}",
    ) -> tuple[bool, str]:
        """执行任意 DSH 桥接操作。

        Args:
            operation: DSH Adapter 操作名。
            parameters_json: 操作参数 JSON 对象字符串。
        """

        plugin = _plugin(self)
        try:
            result = await plugin.dispatcher.execute(
                operation,
                _parse_object(parameters_json, "parameters_json"),
            )
        except Exception as exc:
            return False, f"DSH 操作失败: {exc}"
        return True, _render(result, plugin.config.llm.max_result_characters)


class DshAdapterCommand(BaseCommand):
    """通过 Owner 级聊天命令管理并调用 DSH。"""

    name = "dsh"
    description = "调用和管理 DeepSeek Harness"
    permission_level = PermissionLevel.OWNER

    async def _reply_result(self, result: dict[str, Any]) -> tuple[bool, str]:
        """渲染并发送结构化命令结果。"""

        plugin = _plugin(self)
        rendered = _render(result, plugin.config.llm.max_result_characters)
        await send_text(rendered, stream_id=self.stream_id, reply_to=self.message_id or None)
        return True, rendered

    async def _reply_error(self, exc: Exception) -> tuple[bool, str]:
        """发送命令错误。"""

        message = f"DSH 操作失败: {exc}"
        await send_text(message, stream_id=self.stream_id, reply_to=self.message_id or None)
        return False, message

    @cmd_route()
    async def handle_default(self) -> tuple[bool, str]:
        """显示 DSH Adapter 帮助。"""

        await send_text(_USAGE, stream_id=self.stream_id, reply_to=self.message_id or None)
        return True, _USAGE

    @cmd_route("help")
    async def handle_help(self) -> tuple[bool, str]:
        """显示 DSH Adapter 帮助。"""

        return await self.handle_default()

    @cmd_route("status")
    async def handle_status(self) -> tuple[bool, str]:
        """查看 DSH 与桥接状态。"""

        try:
            return await self._reply_result(await _plugin(self).dispatcher.execute("status"))
        except Exception as exc:
            return await self._reply_error(exc)

    @cmd_route("methods")
    async def handle_methods(self, query: str = "") -> tuple[bool, str]:
        """列出全部 RPC 方法，或查询一个 domain/精确方法。"""

        parameters: dict[str, Any] = {}
        if query:
            parameters["method" if "." in query else "domain"] = query
        try:
            return await self._reply_result(
                await _plugin(self).dispatcher.execute("rpc_catalog", parameters)
            )
        except Exception as exc:
            return await self._reply_error(exc)

    @cmd_route("sessions")
    async def handle_sessions(self) -> tuple[bool, str]:
        """列出 DSH 会话及其 ID。"""

        try:
            return await self._reply_result(
                await _plugin(self).dispatcher.execute("session_list")
            )
        except Exception as exc:
            return await self._reply_error(exc)

    @cmd_route("models")
    async def handle_models(self, session_id: str = "") -> tuple[bool, str]:
        """列出 DSH 主机或指定会话的真实模型目录。"""

        parameters = {"session_id": session_id} if session_id else {}
        try:
            return await self._reply_result(
                await _plugin(self).dispatcher.execute("model_list", parameters)
            )
        except Exception as exc:
            return await self._reply_error(exc)

    @cmd_route("model")
    async def handle_model(
        self,
        session_id: str,
        model: str,
        reasoning_effort: str = "",
        provider: str = "",
    ) -> tuple[bool, str]:
        """切换指定 DSH 会话模型。"""

        parameters = {"session_id": session_id, "model": model}
        if reasoning_effort:
            parameters["reasoning_effort"] = reasoning_effort
        if provider:
            parameters["provider"] = provider
        try:
            return await self._reply_result(
                await _plugin(self).dispatcher.execute("model_switch", parameters)
            )
        except Exception as exc:
            return await self._reply_error(exc)

    @cmd_route("presets")
    async def handle_presets(self) -> tuple[bool, str]:
        """列出 DSH 实时提供的 Agent preset 模式。"""

        try:
            return await self._reply_result(
                await _plugin(self).dispatcher.execute("preset_list")
            )
        except Exception as exc:
            return await self._reply_error(exc)

    @cmd_route("preset")
    async def handle_preset(
        self,
        session_id: str,
        preset: str,
    ) -> tuple[bool, str]:
        """切换指定空白 DSH 会话的 Agent preset。"""

        try:
            return await self._reply_result(
                await _plugin(self).dispatcher.execute(
                    "preset_switch",
                    {"session_id": session_id, "preset": preset},
                )
            )
        except Exception as exc:
            return await self._reply_error(exc)

    @cmd_route("headless")
    async def handle_headless(self, task: str, cwd: str = "") -> tuple[bool, str]:
        """运行一次 DSH headless 任务；含空格参数需使用引号。"""

        parameters: dict[str, Any] = {"task": task}
        if cwd:
            parameters["cwd"] = cwd
        try:
            return await self._reply_result(
                await _plugin(self).dispatcher.execute("headless", parameters)
            )
        except Exception as exc:
            return await self._reply_error(exc)

    @cmd_route("rpc")
    async def handle_rpc(
        self,
        method: str,
        payload_json: str = "{}",
    ) -> tuple[bool, str]:
        """调用任意 DSH RPC；payload 必须作为一个引号包裹的 JSON 参数。"""

        try:
            return await self._reply_result(
                await _plugin(self).dispatcher.execute(
                    "rpc_call",
                    {"method": method, "payload": _parse_object(payload_json, "payload_json")},
                )
            )
        except Exception as exc:
            return await self._reply_error(exc)

    @cmd_route("exec")
    async def handle_execute(
        self,
        operation: str,
        parameters_json: str = "{}",
    ) -> tuple[bool, str]:
        """执行任意桥操作；parameters 必须作为一个引号包裹的 JSON 参数。"""

        try:
            return await self._reply_result(
                await _plugin(self).dispatcher.execute(
                    operation,
                    _parse_object(parameters_json, "parameters_json"),
                )
            )
        except Exception as exc:
            return await self._reply_error(exc)

    @cmd_route("cli")
    async def handle_cli(
        self,
        arguments_json: str,
        cwd: str = "",
    ) -> tuple[bool, str]:
        """执行任意 DSH CLI 参数数组。"""

        parameters: dict[str, Any] = {
            "arguments": _parse_string_list(arguments_json, "arguments_json")
        }
        if cwd:
            parameters["cwd"] = cwd
        try:
            return await self._reply_result(
                await _plugin(self).dispatcher.execute("cli_run", parameters)
            )
        except Exception as exc:
            return await self._reply_error(exc)

    @cmd_route("processes")
    async def handle_processes(self) -> tuple[bool, str]:
        """列出桥管理的 DSH 长期进程。"""

        try:
            return await self._reply_result(
                await _plugin(self).dispatcher.execute("process_list")
            )
        except Exception as exc:
            return await self._reply_error(exc)

    @cmd_route("output")
    async def handle_output(
        self,
        process_id: str,
        after_sequence: int = 0,
    ) -> tuple[bool, str]:
        """读取长期 DSH 进程的增量输出。"""

        try:
            return await self._reply_result(
                await _plugin(self).dispatcher.execute(
                    "process_output",
                    {"process_id": process_id, "after_sequence": after_sequence},
                )
            )
        except Exception as exc:
            return await self._reply_error(exc)

    @cmd_route("stop")
    async def handle_stop(self, process_id: str) -> tuple[bool, str]:
        """停止一个长期 DSH 进程。"""

        try:
            return await self._reply_result(
                await _plugin(self).dispatcher.execute(
                    "process_stop", {"process_id": process_id}
                )
            )
        except Exception as exc:
            return await self._reply_error(exc)

    @cmd_route("events")
    async def handle_events(
        self,
        stream: str,
        after_sequence: int = 0,
    ) -> tuple[bool, str]:
        """读取 mux 或 host 事件流。"""

        try:
            return await self._reply_result(
                await _plugin(self).dispatcher.execute(
                    "event_read",
                    {"stream": stream, "after_sequence": after_sequence},
                )
            )
        except Exception as exc:
            return await self._reply_error(exc)

    @cmd_route("data")
    async def handle_data(self, path: str = "") -> tuple[bool, str]:
        """列出 DSH_HOME 中的目录。"""

        try:
            return await self._reply_result(
                await _plugin(self).dispatcher.execute("data_list", {"path": path})
            )
        except Exception as exc:
            return await self._reply_error(exc)
