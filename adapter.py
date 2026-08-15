"""Neo-MoFox 原生 DSH Transport Adapter。"""

from __future__ import annotations

import asyncio
import copy
from datetime import datetime
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from mofox_wire import CoreSink, MessageEnvelope
from mofox_wire.types import UserRole

from src.app.plugin_system.api.log_api import get_logger
from src.core.components import BaseAdapter
from src.core.components.types import PlatformSendResult
from src.kernel.concurrency import get_task_manager

from .event_messages import (
    DshProgressAggregator,
    ProgressDeliveryMode,
    RenderedDshEvent,
    render_dsh_event,
)
from .interactions import DshInteractionRegistry, DshPendingInteraction
from .runtime import DshBridgeRuntime, DshRuntimeEvent

if TYPE_CHECKING:
    from src.core.components.base.plugin import BasePlugin

_logger = get_logger("dsh_adapter.adapter", display="DSH Transport Adapter")
_EVENT_STREAMS: tuple[str, str] = ("mux", "host")


class DshTransportAdapter(BaseAdapter):
    """把每个 DSH session 映射为一条 Neo-MoFox 私聊流。"""

    name = "dsh_adapter"
    adapter_name = name
    adapter_version = "1.4.0"
    description = "DeepSeek Harness 原生传输适配器"
    platform = "dsh"

    def __init__(
        self,
        core_sink: CoreSink | None,
        plugin: BasePlugin | None = None,
        *,
        runtime: DshBridgeRuntime | None = None,
        interaction_registry: DshInteractionRegistry | None = None,
        max_event_characters: int | None = None,
        progress_delivery: ProgressDeliveryMode | None = None,
        aggregation_window_seconds: float | None = None,
        flush_interval_seconds: float = 0.5,
        flush_on_unload: bool = True,
        start_event_streams: bool | None = None,
        clock: Callable[[], float] = time.monotonic,
        **kwargs: Any,
    ) -> None:
        """初始化共享 Runtime、pending registry 与事件渲染状态。"""

        super().__init__(cast(CoreSink, core_sink), plugin=plugin, **kwargs)
        resolved_runtime = runtime or getattr(plugin, "runtime", None)
        resolved_registry = interaction_registry or getattr(
            plugin, "interaction_registry", None
        )
        if resolved_runtime is None:
            raise ValueError("DshTransportAdapter 需要共享 runtime")
        if resolved_registry is None:
            raise ValueError("DshTransportAdapter 需要 interaction_registry")
        plugin_config = getattr(plugin, "config", None)
        interaction_config = getattr(plugin_config, "interaction", None)
        bridge_config = getattr(plugin_config, "bridge", None)
        resolved_max_characters = (
            max_event_characters
            if max_event_characters is not None
            else getattr(interaction_config, "max_event_text_characters", 12000)
        )
        resolved_delivery = (
            progress_delivery
            if progress_delivery is not None
            else getattr(interaction_config, "progress_delivery", "aggregate")
        )
        resolved_window = (
            aggregation_window_seconds
            if aggregation_window_seconds is not None
            else getattr(interaction_config, "progress_window_seconds", 2.0)
        )
        resolved_start_streams = (
            start_event_streams
            if start_event_streams is not None
            else getattr(bridge_config, "start_event_streams", True)
        )
        if resolved_max_characters <= 0:
            raise ValueError("max_event_characters 必须大于 0")
        self.runtime = resolved_runtime
        self.interaction_registry = resolved_registry
        self._max_event_characters = resolved_max_characters
        if flush_interval_seconds <= 0:
            raise ValueError("flush_interval_seconds 必须大于 0")
        self._progress_delivery = resolved_delivery
        self._aggregation_window_seconds = resolved_window
        self._flush_interval_seconds = flush_interval_seconds
        self._flush_on_unload = flush_on_unload
        self._start_event_streams = resolved_start_streams
        self._clock = clock
        self._aggregator = DshProgressAggregator(
            delivery_mode=resolved_delivery,
            window_seconds=resolved_window,
            clock=clock,
        )
        self._progress_session_ids: set[str] = set()
        self._listener_id: str | None = None
        self._flush_task_info: Any | None = None

    async def on_adapter_loaded(self) -> None:
        """注册 Runtime 监听器后按 mux、host 顺序启动两条 SSE 流。"""

        listener_added = False
        started_streams: list[str] = []
        if self._listener_id is None:
            self._listener_id = self.runtime.add_event_listener(
                self._handle_runtime_event
            )
            listener_added = True
        try:
            if self._start_event_streams:
                for stream_name in _EVENT_STREAMS:
                    was_running = bool(
                        self.runtime.event_stream_status(stream_name).get("running")
                    )
                    await self.runtime.start_event_stream(stream_name)
                    if not was_running:
                        started_streams.append(stream_name)
            if self._flush_task_info is None:
                self._flush_task_info = get_task_manager().create_task(
                    self._flush_loop(),
                    name="dsh-adapter-progress-flush",
                    daemon=True,
                )
        except BaseException:
            for stream_name in reversed(started_streams):
                await self.runtime.stop_event_stream(stream_name)
            if listener_added and self._listener_id is not None:
                self.runtime.remove_event_listener(self._listener_id)
                self._listener_id = None
            raise

    async def on_adapter_unloaded(self) -> None:
        """停止事件流并移除监听器，但不关闭共享 Runtime 或 HTTP 客户端。"""

        await self._cancel_flush_task()
        try:
            await self.runtime.stop_event_stream("mux")
        finally:
            try:
                await self.runtime.stop_event_stream("host")
            finally:
                try:
                    if self._flush_on_unload:
                        await self.flush_due(force=True)
                finally:
                    if self._listener_id is not None:
                        self.runtime.remove_event_listener(self._listener_id)
                        self._listener_id = None

    async def health_check(self) -> bool:
        """确认 host.describe 成功且 mux、host 两条流均处于运行状态。"""

        try:
            result = await self.runtime.client.call_async("host.describe", {})
            if result.ok is not True:
                return False
            return all(
                bool(self.runtime.event_stream_status(name).get("running"))
                for name in _EVENT_STREAMS
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _logger.warning(f"DSH Adapter 健康检查失败: {exc}")
            return False

    async def reconnect(self) -> None:
        """只恢复未运行的事件流，保持现有 Runtime 监听器不变。"""

        if not self._start_event_streams:
            return
        for name in _EVENT_STREAMS:
            status = self.runtime.event_stream_status(name)
            if not status.get("running"):
                await self.runtime.start_event_stream(name)

    async def flush_due(self, *, now: float | None = None, force: bool = False) -> int:
        """发送到期或强制取出的进度摘要，并返回发送条数。"""

        rendered_events: list[RenderedDshEvent]
        if force:
            rendered_events = []
            for session_id in tuple(self._progress_session_ids):
                summary = self._aggregator.flush_session(session_id)
                if summary is not None:
                    rendered_events.append(summary)
                self._progress_session_ids.discard(session_id)
        else:
            rendered_events = self._aggregator.flush_due(
                self._clock() if now is None else now
            )
            for rendered in rendered_events:
                self._progress_session_ids.discard(rendered.session_id)
        for rendered in rendered_events:
            await self.on_platform_message(rendered)
        return len(rendered_events)

    @property
    def listener_id(self) -> str | None:
        """返回当前 Runtime 监听器 ID，供生命周期检查使用。"""

        return self._listener_id

    async def from_platform_message(
        self,
        raw: RenderedDshEvent,
    ) -> MessageEnvelope:
        """把已脱敏的 DSH 事件转换为标准 incoming envelope。"""

        received_at = datetime.fromisoformat(raw.received_at.replace("Z", "+00:00"))
        if received_at.tzinfo is None:
            raise ValueError("received_at 必须包含时区")
        envelope: dict[str, Any] = {
            "direction": "incoming",
            "message_info": {
                "platform": self.platform,
                "message_id": raw.message_id,
                "time": received_at.timestamp(),
                "message_type": "message",
                "user_info": {
                    "platform": self.platform,
                    "role": UserRole.MEMBER,
                    "user_id": raw.session_id,
                    "user_nickname": f"DSH {raw.session_id[:8]}",
                },
                "extra": copy.deepcopy(raw.extra),
            },
            "message_segment": [{"type": "text", "data": raw.text}],
            "raw_message": copy.deepcopy(raw.raw_message),
        }
        return cast(MessageEnvelope, envelope)

    async def _handle_runtime_event(self, event: DshRuntimeEvent) -> None:
        """渲染 Runtime 事件，注册交互并投递首次出现的入站消息。"""

        payload = event.message.get("payload")
        payload_type = payload.get("type") if isinstance(payload, dict) else None
        if payload_type == "host/session-removed":
            session_id = payload.get("sessionId") if isinstance(payload, dict) else None
            if isinstance(session_id, str) and session_id:
                await self.interaction_registry.mark_session_stale(session_id)
            return
        if payload_type == "question/resolved":
            question_rpc_id = (
                payload.get("questionRpcId") if isinstance(payload, dict) else None
            )
            if isinstance(question_rpc_id, str) and question_rpc_id:
                await self.interaction_registry.resolve_question(question_rpc_id)
        elif payload_type == "approval/resolved":
            approval_id = payload.get("approvalId") if isinstance(payload, dict) else None
            if isinstance(approval_id, str) and approval_id:
                await self.interaction_registry.resolve_approval(approval_id)

        rendered = render_dsh_event(event, self._max_event_characters)
        if rendered is None:
            return
        event_type = rendered.extra.get("dsh_session_event_type")
        if event_type == "turn/end":
            self._progress_session_ids.discard(rendered.session_id)
        elif not rendered.immediate and self._progress_delivery == "aggregate":
            self._progress_session_ids.add(rendered.session_id)
        if rendered.requires_response:
            interaction = DshPendingInteraction.from_runtime_event(event)
            if interaction is None:
                return
            if not await self.interaction_registry.upsert(interaction):
                return
        for outbound in self._aggregator.add(rendered):
            await self.on_platform_message(outbound)

    async def _flush_loop(self) -> None:
        """按固定周期发送已到期的进度摘要。"""

        try:
            while True:
                await asyncio.sleep(self._flush_interval_seconds)
                await self.flush_due()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _logger.error(f"DSH 进度摘要刷新任务退出: {exc}", exc_info=True)

    async def _cancel_flush_task(self) -> None:
        """取消并等待由 TaskManager 管理的进度刷新任务。"""

        task_info = self._flush_task_info
        self._flush_task_info = None
        if task_info is None:
            return
        task_manager = get_task_manager()
        task_manager.cancel_task(task_info.task_id)
        if task_info.task is not None:
            await asyncio.gather(task_info.task, return_exceptions=True)

    async def _send_platform_message(
        self,
        envelope: MessageEnvelope,
    ) -> PlatformSendResult:
        """明确拒绝尚未实现的 DSH 普通文本出站路径。"""

        del envelope
        return PlatformSendResult(
            success=False,
            error="DSH Transport Adapter 的普通文本出站将在 Task 6 实现",
        )

    async def get_bot_info(self) -> dict[str, Any]:
        """返回 DSH 平台上的 Neo-MoFox 身份。"""

        return {"bot_id": "mofox", "bot_name": "Neo-MoFox", "platform": "dsh"}


__all__ = ["DshTransportAdapter"]