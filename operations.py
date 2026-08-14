"""DSH Adapter 的统一操作分派器。"""

from __future__ import annotations

import base64
from typing import Any

from .rpc_catalog import query_rpc_catalog
from .runtime import DshBridgeRuntime

SUPPORTED_OPERATIONS: tuple[str, ...] = (
    "status",
    "rpc_catalog",
    "session_list",
    "model_list",
    "model_switch",
    "preset_list",
    "preset_switch",
    "ensure_web",
    "headless",
    "rpc_call",
    "respond",
    "http_request",
    "cli_run",
    "process_start",
    "process_write",
    "process_output",
    "process_list",
    "process_stop",
    "event_start",
    "event_stop",
    "event_status",
    "event_read",
    "data_list",
    "data_read",
)

READ_ONLY_OPERATIONS: frozenset[str] = frozenset(
    {
        "status",
        "rpc_catalog",
        "session_list",
        "model_list",
        "preset_list",
        "process_output",
        "process_list",
        "event_status",
        "event_read",
        "data_list",
        "data_read",
    }
)


class DshOperationDispatcher:
    """将结构化操作映射到共享 DSH 运行时。"""

    def __init__(self, runtime: DshBridgeRuntime) -> None:
        """初始化分派器。"""

        self.runtime = runtime

    async def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """执行一个受支持的 DSH 桥接操作。

        Args:
            operation: ``SUPPORTED_OPERATIONS`` 中的操作名。
            parameters: 操作参数 JSON 对象。

        Returns:
            包含操作名和结构化结果的字典。
        """

        name = operation.strip()
        if name not in SUPPORTED_OPERATIONS:
            raise ValueError(
                f"不支持的操作: {name or '<empty>'}; "
                f"可用操作: {', '.join(SUPPORTED_OPERATIONS)}"
            )
        params = parameters or {}
        if not isinstance(params, dict):
            raise TypeError("parameters 必须是 JSON 对象")

        handler = getattr(self, f"_execute_{name}")
        result = await handler(params)
        return {"operation": name, "result": result}

    async def _execute_status(self, params: dict[str, Any]) -> dict[str, Any]:
        """返回桥、DSH Web、进程和事件流状态。"""

        self._reject_unknown(params, set())
        web: dict[str, Any]
        try:
            web = (await self.runtime.client.call_async("host.describe", {})).to_dict()
        except Exception as exc:
            web = {"ok": False, "transport_error": str(exc)}
        return {
            "dsh_executable": str(self.runtime.resolve_executable()),
            "dsh_home": str(self.runtime.options.dsh_home.expanduser().resolve()),
            "web_base_url": self.runtime.options.web_base_url,
            "web": web,
            "processes": self.runtime.list_processes(),
            "event_streams": {
                name: self.runtime.event_stream_status(name)
                for name in ("mux", "host")
            },
            "supported_operations": list(SUPPORTED_OPERATIONS),
        }

    async def _execute_rpc_catalog(self, params: dict[str, Any]) -> dict[str, Any]:
        """查询当前 DSH 版本的完整 RPC 与非 RPC 能力目录。"""

        self._reject_unknown(
            params,
            {"method", "domain", "risk", "include_details"},
        )
        return query_rpc_catalog(
            method=self._optional_str(params, "method"),
            domain=self._optional_str(params, "domain"),
            risk=self._optional_str(params, "risk"),
            include_details=self._bool(params, "include_details", False),
        )

    async def _execute_model_list(self, params: dict[str, Any]) -> dict[str, Any]:
        """返回主机模型目录，或指定会话的当前选择与模型目录。"""

        self._reject_unknown(params, {"session_id"})
        session_id = self._optional_str(params, "session_id")
        if session_id is None or not session_id.strip():
            result = await self.runtime.client.call_async("llm.models", {})
            return result.to_dict()
        result = await self.runtime.client.call_async(
            "session.models",
            {"sessionId": session_id},
        )
        return result.to_dict()

    async def _execute_session_list(self, params: dict[str, Any]) -> dict[str, Any]:
        """列出 DSH 会话，供会话级操作选择准确目标。"""

        self._reject_unknown(params, set())
        result = await self.runtime.client.call_async("session.list", {})
        return result.to_dict()

    async def _execute_preset_list(self, params: dict[str, Any]) -> dict[str, Any]:
        """列出 DSH 实时提供的 Agent preset 模式。"""

        self._reject_unknown(params, set())
        result = await self.runtime.client.call_async("agentPreset.list", {})
        return result.to_dict()

    async def _execute_preset_switch(self, params: dict[str, Any]) -> dict[str, Any]:
        """按实时目录解析并切换指定空白会话的 Agent preset。"""

        self._reject_unknown(params, {"session_id", "preset"})
        session_id = self._required_str(params, "session_id")
        requested_preset = self._required_str(params, "preset")
        catalog = await self.runtime.client.call_async("agentPreset.list", {})
        if not catalog.ok or not isinstance(catalog.value, dict):
            raise RuntimeError(f"无法读取 DSH 模式目录: {catalog.to_dict()}")

        requested = requested_preset.strip().casefold()
        matches: list[dict[str, Any]] = []
        available: list[str] = []
        presets = catalog.value.get("presets", [])
        if isinstance(presets, list):
            for entry in presets:
                if not isinstance(entry, dict) or not isinstance(entry.get("id"), str):
                    continue
                preset_id = entry["id"]
                display_name = entry.get("name")
                available.append(
                    f"{preset_id} ({display_name})"
                    if isinstance(display_name, str)
                    else preset_id
                )
                names = {preset_id.casefold()}
                if isinstance(display_name, str):
                    names.add(display_name.casefold())
                if requested in names:
                    matches.append(entry)

        if not matches:
            choices = ", ".join(available) or "<empty>"
            raise ValueError(f"DSH 没有模式 {requested_preset}; 可用模式: {choices}")
        if len(matches) > 1:
            choices = ", ".join(entry["id"] for entry in matches)
            raise ValueError(f"模式名称 {requested_preset} 不唯一，请使用 ID: {choices}")
        selected = matches[0]
        if isinstance(selected.get("broken"), str):
            raise ValueError(
                f"DSH 模式 {selected['id']} 当前不可用: {selected['broken']}"
            )
        result = await self.runtime.client.call_async(
            "agentPreset.select",
            {"sessionId": session_id, "agentPreset": selected["id"]},
        )
        return result.to_dict()

    async def _execute_model_switch(self, params: dict[str, Any]) -> dict[str, Any]:
        """按实时模型目录解析 provider 并切换指定 DSH 会话模型。"""

        self._reject_unknown(
            params,
            {"session_id", "model", "provider", "reasoning_effort"},
        )
        session_id = self._required_str(params, "session_id")
        requested_model = self._required_str(params, "model")
        requested_provider = self._optional_str(params, "provider")
        reasoning_effort = self._optional_str(params, "reasoning_effort")
        if reasoning_effort is not None:
            reasoning_effort = reasoning_effort.strip() or None
        provider, model = await self._resolve_model_selection(
            session_id,
            requested_model,
            requested_provider,
            reasoning_effort,
        )

        payload = {
            "sessionId": session_id,
            "provider": provider,
            "model": model,
        }
        if reasoning_effort is not None and reasoning_effort.strip():
            payload["reasoningEffort"] = reasoning_effort
        result = await self.runtime.client.call_async("session.selectModel", payload)
        return result.to_dict()

    async def _resolve_model_selection(
        self,
        session_id: str,
        model: str,
        provider: str | None,
        reasoning_effort: str | None,
    ) -> tuple[str, str]:
        """从 DSH 会话目录中解析并校验 provider、模型与推理等级。"""

        catalog = await self.runtime.client.call_async(
            "session.models",
            {"sessionId": session_id},
        )
        if not catalog.ok or not isinstance(catalog.value, dict):
            raise RuntimeError(f"无法读取 DSH 模型目录: {catalog.to_dict()}")

        requested = model.strip().casefold()
        requested_provider = (
            provider.strip().casefold() if provider and provider.strip() else None
        )
        matches: list[tuple[str, str, dict[str, Any]]] = []
        available: list[str] = []
        groups = catalog.value.get("groups", [])
        if isinstance(groups, list):
            for group in groups:
                if not isinstance(group, dict) or not isinstance(group.get("id"), str):
                    continue
                group_id = group["id"]
                models = group.get("models", [])
                if not isinstance(models, list):
                    continue
                for entry in models:
                    if not isinstance(entry, dict) or not isinstance(entry.get("id"), str):
                        continue
                    model_id = entry["id"]
                    available.append(f"{group_id}/{model_id}")
                    if (
                        requested_provider is not None
                        and group_id.casefold() != requested_provider
                    ):
                        continue
                    display_name = entry.get("name")
                    names = {model_id.casefold()}
                    if isinstance(display_name, str):
                        names.add(display_name.casefold())
                    if requested in names:
                        matches.append((group_id, model_id, entry))

        if not matches:
            choices = ", ".join(available) or "<empty>"
            raise ValueError(f"DSH 会话没有模型 {model}; 可用模型: {choices}")
        providers = {item[0] for item in matches}
        if len(providers) != 1:
            choices = ", ".join(f"{item[0]}/{item[1]}" for item in matches)
            raise ValueError(f"模型 {model} 属于多个 provider，请明确指定: {choices}")
        selected_provider, selected_model, metadata = matches[0]
        if reasoning_effort is not None and reasoning_effort.strip():
            supported_efforts: list[str] = []
            reasoning = metadata.get("reasoning")
            if isinstance(reasoning, dict) and isinstance(reasoning.get("efforts"), list):
                supported_efforts = [
                    effort["id"]
                    for effort in reasoning["efforts"]
                    if isinstance(effort, dict) and isinstance(effort.get("id"), str)
                ]
            if supported_efforts and reasoning_effort not in supported_efforts:
                raise ValueError(
                    f"模型 {selected_provider}/{selected_model} 不支持推理等级 "
                    f"{reasoning_effort}; 可用值: {', '.join(supported_efforts)}"
                )
        return selected_provider, selected_model

    async def _execute_ensure_web(self, params: dict[str, Any]) -> dict[str, Any]:
        """确保 DSH Web profile 已经启动。"""

        self._reject_unknown(params, {"start_timeout"})
        return await self.runtime.ensure_web(
            start_timeout=self._optional_float(params, "start_timeout")
            or self.runtime.options.default_timeout
        )

    async def _execute_headless(self, params: dict[str, Any]) -> dict[str, Any]:
        """让 DSH headless profile 完成一个任务。"""

        self._reject_unknown(
            params,
            {"task", "cwd", "environment", "timeout", "patches"},
        )
        task = self._required_str(params, "task")
        patches = self._string_list(params.get("patches", []), "patches")
        arguments = ["--profile", "headless"]
        for patch in patches:
            arguments.extend(["--patch", patch])
        arguments.append(task)
        result = await self.runtime.run_cli(
            arguments,
            cwd=self._optional_str(params, "cwd"),
            environment=self._string_dict(params.get("environment"), "environment"),
            timeout=self._optional_float(params, "timeout"),
        )
        return result.to_dict()

    async def _execute_rpc_call(self, params: dict[str, Any]) -> dict[str, Any]:
        """调用任意 DSH 一元 RPC。"""

        self._reject_unknown(params, {"method", "payload"})
        payload = params.get("payload", {})
        if not isinstance(payload, dict):
            raise TypeError("payload 必须是 JSON 对象")
        result = await self.runtime.client.call_async(
            self._required_str(params, "method"),
            payload,
        )
        return result.to_dict()

    async def _execute_respond(self, params: dict[str, Any]) -> dict[str, Any]:
        """响应事件流中的 DSH ServerRequest。"""

        self._reject_unknown(params, {"rpc_id", "result"})
        result = params.get("result")
        if not isinstance(result, dict):
            raise TypeError("result 必须是 JSON 对象")
        return await self.runtime.client.respond(
            self._required_str(params, "rpc_id"),
            result,
        )

    async def _execute_http_request(self, params: dict[str, Any]) -> dict[str, Any]:
        """向 DSH Web origin 发送任意 HTTP 请求。"""

        self._reject_unknown(
            params,
            {
                "method",
                "path",
                "query",
                "headers",
                "json_body",
                "body_base64",
            },
        )
        encoded_body = params.get("body_base64")
        body: bytes | None = None
        if encoded_body is not None:
            if not isinstance(encoded_body, str):
                raise TypeError("body_base64 必须是字符串")
            try:
                body = base64.b64decode(encoded_body, validate=True)
            except ValueError as exc:
                raise ValueError("body_base64 不是有效 Base64") from exc
        response = await self.runtime.client.request(
            method=self._required_str(params, "method"),
            path=self._required_str(params, "path"),
            query=self._string_dict(params.get("query"), "query"),
            headers=self._string_dict(params.get("headers"), "headers"),
            json_body=params.get("json_body"),
            body=body,
        )
        return response.to_dict()

    async def _execute_cli_run(self, params: dict[str, Any]) -> dict[str, Any]:
        """执行任意 DSH CLI 参数并等待结束。"""

        self._reject_unknown(
            params,
            {"arguments", "cwd", "stdin", "environment", "timeout"},
        )
        result = await self.runtime.run_cli(
            self._string_list(params.get("arguments"), "arguments"),
            cwd=self._optional_str(params, "cwd"),
            stdin=self._optional_str(params, "stdin"),
            environment=self._string_dict(params.get("environment"), "environment"),
            timeout=self._optional_float(params, "timeout"),
        )
        return result.to_dict()

    async def _execute_process_start(self, params: dict[str, Any]) -> dict[str, Any]:
        """启动一个长期 DSH 进程。"""

        self._reject_unknown(params, {"process_id", "arguments", "cwd", "environment"})
        return await self.runtime.start_process(
            self._required_str(params, "process_id"),
            self._string_list(params.get("arguments"), "arguments"),
            cwd=self._optional_str(params, "cwd"),
            environment=self._string_dict(params.get("environment"), "environment"),
        )

    async def _execute_process_write(self, params: dict[str, Any]) -> dict[str, Any]:
        """向长期 DSH 进程写入标准输入。"""

        self._reject_unknown(params, {"process_id", "text"})
        return await self.runtime.write_process(
            self._required_str(params, "process_id"),
            self._required_str(params, "text", allow_empty=True),
        )

    async def _execute_process_output(self, params: dict[str, Any]) -> dict[str, Any]:
        """读取长期 DSH 进程输出。"""

        self._reject_unknown(
            params,
            {"process_id", "after_sequence", "limit", "wait_seconds"},
        )
        return await self.runtime.read_process_output(
            self._required_str(params, "process_id"),
            after_sequence=self._int(params, "after_sequence", 0),
            limit=self._int(params, "limit", 200),
            wait_seconds=self._float(params, "wait_seconds", 0.0),
        )

    async def _execute_process_list(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        """列出桥管理的长期 DSH 进程。"""

        self._reject_unknown(params, set())
        return self.runtime.list_processes()

    async def _execute_process_stop(self, params: dict[str, Any]) -> dict[str, Any]:
        """停止一个长期 DSH 进程。"""

        self._reject_unknown(params, {"process_id"})
        return await self.runtime.stop_process(self._required_str(params, "process_id"))

    async def _execute_event_start(self, params: dict[str, Any]) -> dict[str, Any]:
        """启动一条 DSH 事件流。"""

        self._reject_unknown(params, {"stream"})
        return await self.runtime.start_event_stream(self._required_str(params, "stream"))

    async def _execute_event_stop(self, params: dict[str, Any]) -> dict[str, Any]:
        """停止一条 DSH 事件流。"""

        self._reject_unknown(params, {"stream"})
        return await self.runtime.stop_event_stream(self._required_str(params, "stream"))

    async def _execute_event_status(self, params: dict[str, Any]) -> dict[str, Any]:
        """读取一条 DSH 事件流状态。"""

        self._reject_unknown(params, {"stream"})
        return self.runtime.event_stream_status(self._required_str(params, "stream"))

    async def _execute_event_read(self, params: dict[str, Any]) -> dict[str, Any]:
        """读取一条 DSH 事件流的缓冲消息。"""

        self._reject_unknown(
            params,
            {"stream", "after_sequence", "limit", "wait_seconds"},
        )
        return await self.runtime.read_events(
            self._required_str(params, "stream"),
            after_sequence=self._int(params, "after_sequence", 0),
            limit=self._int(params, "limit", 200),
            wait_seconds=self._float(params, "wait_seconds", 0.0),
        )

    async def _execute_data_list(self, params: dict[str, Any]) -> dict[str, Any]:
        """列出 DSH_HOME 中的数据。"""

        self._reject_unknown(params, {"path", "recursive", "pattern", "limit"})
        return self.runtime.list_data(
            path=self._optional_str(params, "path") or "",
            recursive=self._bool(params, "recursive", False),
            pattern=self._optional_str(params, "pattern") or "*",
            limit=self._int(params, "limit", 500),
        )

    async def _execute_data_read(self, params: dict[str, Any]) -> dict[str, Any]:
        """读取 DSH 数据文件。"""

        self._reject_unknown(params, {"path", "offset", "limit"})
        limit = params.get("limit")
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int)):
            raise TypeError("limit 必须是整数")
        return await self.runtime.read_data(
            path=self._required_str(params, "path"),
            offset=self._int(params, "offset", 0),
            limit=limit,
        )

    @staticmethod
    def _reject_unknown(params: dict[str, Any], allowed: set[str]) -> None:
        """拒绝拼写错误或当前操作不支持的参数。"""

        unknown = sorted(set(params) - allowed)
        if unknown:
            raise ValueError(f"未知参数: {', '.join(unknown)}")

    @staticmethod
    def _required_str(
        params: dict[str, Any],
        key: str,
        *,
        allow_empty: bool = False,
    ) -> str:
        """读取必需字符串。"""

        value = params.get(key)
        if not isinstance(value, str) or (not allow_empty and not value.strip()):
            raise ValueError(f"{key} 必须是非空字符串")
        return value

    @staticmethod
    def _optional_str(params: dict[str, Any], key: str) -> str | None:
        """读取可选字符串。"""

        value = params.get(key)
        if value is None:
            return None
        if not isinstance(value, str):
            raise TypeError(f"{key} 必须是字符串")
        return value

    @staticmethod
    def _string_list(value: Any, key: str) -> list[str]:
        """校验字符串数组。"""

        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise TypeError(f"{key} 必须是字符串数组")
        return value

    @staticmethod
    def _string_dict(value: Any, key: str) -> dict[str, str] | None:
        """校验可选字符串字典。"""

        if value is None:
            return None
        if not isinstance(value, dict) or not all(
            isinstance(item_key, str) and isinstance(item_value, str)
            for item_key, item_value in value.items()
        ):
            raise TypeError(f"{key} 必须是字符串到字符串的对象")
        return value

    @staticmethod
    def _optional_float(params: dict[str, Any], key: str) -> float | None:
        """读取可选数值。"""

        value = params.get(key)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{key} 必须是数值")
        return float(value)

    @classmethod
    def _float(cls, params: dict[str, Any], key: str, default: float) -> float:
        """读取带默认值的数值。"""

        value = cls._optional_float(params, key)
        return default if value is None else value

    @staticmethod
    def _int(params: dict[str, Any], key: str, default: int) -> int:
        """读取带默认值的整数。"""

        value = params.get(key, default)
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{key} 必须是整数")
        return value

    @staticmethod
    def _bool(params: dict[str, Any], key: str, default: bool) -> bool:
        """读取带默认值的布尔值。"""

        value = params.get(key, default)
        if not isinstance(value, bool):
            raise TypeError(f"{key} 必须是布尔值")
        return value
