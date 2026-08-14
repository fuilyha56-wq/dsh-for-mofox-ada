"""DeepSeek Harness HTTP RPC 客户端。"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, AsyncIterator
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin, urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4

import httpx


class DshTransportError(RuntimeError):
    """表示 DSH HTTP 传输或协议层失败。"""


@dataclass(frozen=True, slots=True)
class DshRpcResult:
    """表示一次 DSH RPC 的完整业务结果。"""

    rpc_id: str
    ok: bool
    value: Any = None
    error: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """返回可序列化的 RPC 结果。"""

        result: dict[str, Any] = {"rpc_id": self.rpc_id, "ok": self.ok}
        if self.ok:
            result["value"] = self.value
        else:
            result["error"] = self.error
        return result


@dataclass(frozen=True, slots=True)
class DshHttpResult:
    """表示一次 DSH HTTP 请求的完整响应。"""

    status_code: int
    headers: dict[str, str]
    body: bytes

    def to_dict(self) -> dict[str, Any]:
        """返回可序列化的 HTTP 响应。"""

        content_type = self.headers.get("content-type", "")
        result: dict[str, Any] = {
            "status_code": self.status_code,
            "headers": self.headers,
            "body_base64": base64.b64encode(self.body).decode("ascii"),
        }
        if content_type.startswith(("text/", "application/json")):
            result["text"] = self.body.decode("utf-8", errors="replace")
        return result


class DshRpcClient:
    """通过 DSH Web profile 的 HTTP carrier 调用任意 RPC。"""

    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        max_response_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        """初始化客户端。

        Args:
            base_url: DSH Web 服务根地址。
            timeout: 单次 HTTP 请求超时秒数。
            max_response_bytes: 允许读取的最大响应体字节数。
        """

        normalized = base_url.strip().rstrip("/") + "/"
        if not normalized.startswith(("http://", "https://")):
            raise ValueError("base_url 必须使用 http:// 或 https://")
        if timeout <= 0:
            raise ValueError("timeout 必须大于 0")
        if max_response_bytes <= 0:
            raise ValueError("max_response_bytes 必须大于 0")
        self.base_url = normalized
        self.timeout = timeout
        self.max_response_bytes = max_response_bytes
        self._async_client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout,
            follow_redirects=False,
        )

    def call(self, method: str, payload: dict[str, Any]) -> DshRpcResult:
        """调用任意 DSH 一元 RPC。

        Args:
            method: DSH RPC 方法名，例如 ``session.list``。
            payload: 该方法要求的 JSON 对象。

        Returns:
            包含业务成功或失败分支的完整结果。
        """

        normalized_method = method.strip()
        if not normalized_method or "/" in normalized_method:
            raise ValueError("method 必须是非空且不含斜杠的 RPC 方法名")
        if not isinstance(payload, dict):
            raise TypeError("payload 必须是 JSON 对象")

        rpc_id, body = self._make_request(method=normalized_method, payload=payload)
        response = self._request_json(
            path=f"api/{quote(normalized_method, safe='._-:')}",
            body=body,
        )
        return self._parse_rpc_response(rpc_id=rpc_id, response=response)

    async def call_async(
        self,
        method: str,
        payload: dict[str, Any],
    ) -> DshRpcResult:
        """异步调用任意 DSH 一元 RPC。"""

        normalized_method = method.strip()
        if not normalized_method or "/" in normalized_method:
            raise ValueError("method 必须是非空且不含斜杠的 RPC 方法名")
        if not isinstance(payload, dict):
            raise TypeError("payload 必须是 JSON 对象")
        rpc_id, body = self._make_request(method=normalized_method, payload=payload)
        response = await self.request(
            method="POST",
            path=f"/api/{quote(normalized_method, safe='._-:')}",
            json_body=body,
        )
        decoded = self._decode_json(response.body)
        return self._parse_rpc_response(rpc_id=rpc_id, response=decoded)

    async def respond(
        self,
        rpc_id: str,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        """响应 DSH 通过事件流发起的问题或审批请求。"""

        if not rpc_id.strip():
            raise ValueError("rpc_id 不能为空")
        response = await self.request(
            method="POST",
            path="/api/respond",
            json_body={
                "type": "client-response",
                "rpcId": rpc_id,
                "result": result,
            },
        )
        return self._decode_json(response.body)

    async def request(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        json_body: Any = None,
        body: bytes | None = None,
    ) -> DshHttpResult:
        """向配置的 DSH 服务发送任意同源 HTTP 请求。"""

        parsed = urlsplit(path)
        if parsed.scheme or parsed.netloc or not parsed.path.startswith("/"):
            raise ValueError("path 必须是以 / 开头的同源相对路径")
        if json_body is not None and body is not None:
            raise ValueError("json_body 与 body 不能同时提供")
        try:
            async with self._async_client.stream(
                method=method.upper(),
                url=path,
                params=query,
                headers=headers,
                json=json_body,
                content=body,
            ) as response:
                chunks: list[bytes] = []
                total = 0
                async for chunk in response.aiter_bytes():
                    total += len(chunk)
                    if total > self.max_response_bytes:
                        raise DshTransportError("DSH 响应超过大小限制")
                    chunks.append(chunk)
        except httpx.HTTPError as exc:
            raise DshTransportError(f"无法连接 DSH: {exc}") from exc
        return DshHttpResult(
            status_code=response.status_code,
            headers={key.lower(): value for key, value in response.headers.items()},
            body=b"".join(chunks),
        )

    async def stream_sse(self, path: str) -> AsyncIterator[str]:
        """逐帧读取一个 DSH 同源 SSE 事件流。"""

        parsed = urlsplit(path)
        if parsed.scheme or parsed.netloc or not parsed.path.startswith("/"):
            raise ValueError("path 必须是以 / 开头的同源相对路径")
        stream_timeout = httpx.Timeout(
            connect=self.timeout,
            read=None,
            write=self.timeout,
            pool=self.timeout,
        )
        try:
            async with self._async_client.stream(
                method="GET",
                url=path,
                headers={"accept": "text/event-stream"},
                timeout=stream_timeout,
            ) as response:
                if not response.is_success:
                    raise DshTransportError(
                        f"DSH SSE 请求失败: HTTP {response.status_code}"
                    )
                data_lines: list[str] = []
                event_bytes = 0
                async for line in response.aiter_lines():
                    if line == "":
                        if data_lines:
                            yield "".join(data_lines)
                            data_lines = []
                            event_bytes = 0
                        continue
                    if line.startswith("data: "):
                        data = line[6:]
                    elif line == "data:":
                        data = ""
                    else:
                        continue
                    event_bytes += len(data.encode("utf-8"))
                    if event_bytes > self.max_response_bytes:
                        raise DshTransportError("DSH SSE 事件超过大小限制")
                    data_lines.append(data)
        except DshTransportError:
            raise
        except httpx.HTTPError as exc:
            raise DshTransportError(f"无法连接 DSH SSE: {exc}") from exc

    async def close(self) -> None:
        """关闭异步 HTTP 连接池。"""

        await self._async_client.aclose()

    @staticmethod
    def _make_request(
        method: str,
        payload: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        """构造 DSH client-request 信封。"""

        rpc_id = str(uuid4())
        return rpc_id, {
            "type": "client-request",
            "rpcId": rpc_id,
            "method": method,
            "payload": payload,
        }

    @staticmethod
    def _parse_rpc_response(
        rpc_id: str,
        response: dict[str, Any],
    ) -> DshRpcResult:
        """校验并解析 DSH server-response 信封。"""

        if response.get("type") != "server-response":
            raise DshTransportError("DSH 返回了无效的响应类型")
        if response.get("rpcId") != rpc_id:
            raise DshTransportError("DSH 响应 rpcId 与请求不匹配")

        result = response.get("result")
        if not isinstance(result, dict) or not isinstance(result.get("ok"), bool):
            raise DshTransportError("DSH 返回了无效的 result")
        if result["ok"]:
            return DshRpcResult(rpc_id=rpc_id, ok=True, value=result.get("value"))

        error = result.get("error")
        if not isinstance(error, dict):
            raise DshTransportError("DSH 返回了无效的业务错误")
        return DshRpcResult(rpc_id=rpc_id, ok=False, error=error)

    @staticmethod
    def _decode_json(raw: bytes) -> dict[str, Any]:
        """将 UTF-8 JSON 响应解析为对象。"""

        try:
            decoded = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DshTransportError("DSH 返回的响应不是有效 JSON") from exc
        if not isinstance(decoded, dict):
            raise DshTransportError("DSH 返回的 JSON 顶层不是对象")
        return decoded

    def _request_json(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        """发送 JSON POST 并读取受大小限制的 JSON 对象。"""

        request = Request(
            url=urljoin(self.base_url, path),
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                raw = response.read(self.max_response_bytes + 1)
        except HTTPError as exc:
            raise DshTransportError(f"DSH HTTP 请求失败: {exc.code}") from exc
        except URLError as exc:
            raise DshTransportError(f"无法连接 DSH: {exc.reason}") from exc
        if len(raw) > self.max_response_bytes:
            raise DshTransportError("DSH 响应超过大小限制")
        return self._decode_json(raw)
