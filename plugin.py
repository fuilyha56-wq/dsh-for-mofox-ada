"""DSH Adapter 插件入口。"""

from __future__ import annotations

import os
from pathlib import Path
from typing import cast

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BasePlugin, register_plugin

from .components import (
    DshAdapterCommand,
    DshHeadlessAction,
    DshModelSwitchAction,
    DshOperateAction,
    DshPresetSwitchAction,
    DshQueryTool,
    DshRpcAction,
)
from .config import DshBridgeConfig
from .operations import DshOperationDispatcher
from .router import DshAdapterRouter
from .runtime import DshBridgeRuntime, DshRuntimeOptions
from .service import DshAdapterService

logger = get_logger("dsh_adapter", display="DSH Adapter")


@register_plugin
class DshAdapterPlugin(BasePlugin):
    """让 Neo-MoFox 完整调用和管理 DeepSeek Harness。"""

    plugin_name = "dsh_adapter"
    plugin_description = "DeepSeek Harness 全能力适配器"
    plugin_version = "1.3.0"
    configs = [DshBridgeConfig]

    def __init__(self, config: DshBridgeConfig | None = None) -> None:
        """初始化共享 DSH 运行时与操作分派器。"""

        resolved_config = config or DshBridgeConfig()
        super().__init__(resolved_config)
        bridge = resolved_config.bridge
        self.runtime = DshBridgeRuntime(
            DshRuntimeOptions(
                dsh_command=bridge.dsh_command,
                dsh_home=self._resolve_path(bridge.dsh_home),
                default_workspace=self._resolve_path(bridge.default_workspace),
                web_base_url=bridge.web_base_url,
                default_timeout=bridge.default_timeout_seconds,
                max_timeout=bridge.max_timeout_seconds,
                max_response_bytes=bridge.max_response_bytes,
                process_output_bytes=bridge.process_output_bytes,
                event_buffer_size=bridge.event_buffer_size,
                allow_arbitrary_data_paths=bridge.allow_arbitrary_data_paths,
                allow_sensitive_data=bridge.allow_sensitive_data,
            )
        )
        self.dispatcher = DshOperationDispatcher(self.runtime)

    @staticmethod
    def _resolve_path(raw_path: str) -> Path:
        """展开环境变量和用户目录并返回绝对路径。"""

        return Path(os.path.expandvars(os.path.expanduser(raw_path))).resolve()

    def get_components(self) -> list[type]:
        """按配置返回 Service、命令、Router 与 LLM 组件。"""

        config = cast(DshBridgeConfig, self.config)
        if not config.bridge.enabled:
            return []
        components: list[type] = [DshAdapterService, DshAdapterCommand]
        if config.router.enabled:
            components.append(DshAdapterRouter)
        if config.llm.expose_tools:
            components.append(DshQueryTool)
        if config.llm.expose_actions:
            components.extend(
                [
                    DshHeadlessAction,
                    DshModelSwitchAction,
                    DshPresetSwitchAction,
                    DshRpcAction,
                    DshOperateAction,
                ]
            )
        return components

    async def on_plugin_loaded(self) -> None:
        """可选启动 DSH Web 并订阅两条下行事件流。"""

        config = cast(DshBridgeConfig, self.config)
        if not config.bridge.enabled:
            logger.info("DSH Adapter 已在配置中关闭")
            return
        try:
            if config.bridge.auto_start_web:
                result = await self.runtime.ensure_web(
                    start_timeout=config.bridge.web_start_timeout_seconds
                )
                logger.info(f"DSH Web 已就绪: {result['host']}")
            else:
                result = await self.runtime.client.call_async("host.describe", {})
                logger.info(f"DSH Web 探测结果: {result.to_dict()}")
        except Exception as exc:
            logger.warning(f"DSH Web 当前不可用，CLI 与数据通道仍可使用: {exc}")
            return

        if config.bridge.start_event_streams:
            for stream_name in ("mux", "host"):
                await self.runtime.start_event_stream(stream_name)
            logger.info("DSH mux 与 host 事件流订阅已启动")

    async def on_plugin_unloaded(self) -> None:
        """关闭事件流、HTTP 客户端和桥启动的 DSH 子进程。"""

        await self.runtime.close()
        logger.info("DSH Adapter 运行时已关闭")


__all__ = ["DshAdapterPlugin"]
