"""DSH Adapter 的 FastAPI HTTP 接口。"""

from __future__ import annotations

import hmac
from typing import Any, Protocol, cast

from fastapi import Depends, Header, HTTPException, Request
from pydantic import BaseModel, Field

from src.app.plugin_system.base import BaseRouter

from .config import DshBridgeConfig
from .operations import DshOperationDispatcher, SUPPORTED_OPERATIONS


class _DshAdapterPluginProtocol(Protocol):
    """Router 依赖的最小插件接口。"""

    config: DshBridgeConfig
    dispatcher: DshOperationDispatcher


class OperationRequest(BaseModel):
    """通用 DSH 操作请求。"""

    operation: str = Field(description="DSH Adapter 操作名")
    parameters: dict[str, Any] = Field(default_factory=dict)


class RpcRequest(BaseModel):
    """任意 DSH RPC 请求。"""

    method: str
    payload: dict[str, Any] = Field(default_factory=dict)


class HeadlessRequest(BaseModel):
    """DSH headless 任务请求。"""

    task: str
    cwd: str | None = None
    timeout: float | None = None


class DshAdapterRouter(BaseRouter):
    """通过 Neo-MoFox HTTP 服务器暴露 DSH Adapter。"""

    name = "dsh_adapter"
    description = "DeepSeek Harness 完整桥接 HTTP API"
    custom_route_path = "/api/dsh-adapter"

    @property
    def adapter_plugin(self) -> _DshAdapterPluginProtocol:
        """返回带具体类型的插件实例。"""

        return cast(_DshAdapterPluginProtocol, self.plugin)

    async def _authorize(
        self,
        request: Request,
        token: str | None = Header(default=None, alias="X-DSH-Bridge-Token"),
    ) -> None:
        """允许环回请求，远程请求必须满足显式配置策略。"""

        config = self.adapter_plugin.config.router
        host = request.client.host if request.client is not None else ""
        expected = config.shared_token
        if expected:
            if token is not None and hmac.compare_digest(token, expected):
                return
            raise HTTPException(status_code=403, detail="DSH Adapter 访问令牌无效")
        if host in {"127.0.0.1", "::1", "localhost", "testclient"}:
            return
        if config.allow_remote_without_token:
            return
        raise HTTPException(status_code=403, detail="DSH Adapter 远程访问被拒绝")

    def register_endpoints(self) -> None:
        """注册状态、通用操作、RPC 与 headless 端点。"""

        authorize = self._authorize

        @self.app.get("/operations")
        async def operations(_: None = Depends(authorize)) -> dict[str, Any]:
            """列出支持的完整操作集合。"""

            return {"operations": list(SUPPORTED_OPERATIONS)}

        @self.app.get("/status")
        async def status(_: None = Depends(authorize)) -> dict[str, Any]:
            """返回 DSH 与桥接状态。"""

            return await self.adapter_plugin.dispatcher.execute("status")

        @self.app.post("/execute")
        async def execute(
            payload: OperationRequest,
            _: None = Depends(authorize),
        ) -> dict[str, Any]:
            """执行任意桥接操作。"""

            try:
                return await self.adapter_plugin.dispatcher.execute(
                    payload.operation,
                    payload.parameters,
                )
            except (KeyError, TypeError, ValueError, PermissionError) as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except FileNotFoundError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc

        @self.app.post("/rpc")
        async def rpc(
            payload: RpcRequest,
            _: None = Depends(authorize),
        ) -> dict[str, Any]:
            """调用任意 DSH 一元 RPC。"""

            return await self.adapter_plugin.dispatcher.execute(
                "rpc_call",
                {"method": payload.method, "payload": payload.payload},
            )

        @self.app.post("/headless")
        async def headless(
            payload: HeadlessRequest,
            _: None = Depends(authorize),
        ) -> dict[str, Any]:
            """运行一次 DSH headless 任务。"""

            parameters: dict[str, Any] = {"task": payload.task}
            if payload.cwd is not None:
                parameters["cwd"] = payload.cwd
            if payload.timeout is not None:
                parameters["timeout"] = payload.timeout
            return await self.adapter_plugin.dispatcher.execute("headless", parameters)