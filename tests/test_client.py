"""测试 DeepSeek Harness HTTP RPC 客户端。"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from plugins.dsh_adapter.client import DshRpcClient, DshTransportError


@contextmanager
def rpc_server(
    response_factory: Any,
) -> Iterator[tuple[str, list[dict[str, Any]]]]:
    """启动一个记录请求的本地 DSH RPC 假服务。"""

    requests: list[dict[str, Any]] = []

    class Handler(BaseHTTPRequestHandler):
        """处理测试 RPC 请求。"""

        def do_POST(self) -> None:
            """记录请求并返回测试响应。"""

            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length))
            requests.append({"path": self.path, "body": body})
            response = response_factory(body)
            encoded = json.dumps(response).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:
            """关闭测试 HTTP 日志。"""

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}", requests
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def test_call_preserves_arbitrary_method_payload_and_value() -> None:
    """客户端应原样承载任意方法、参数和成功值。"""

    def response(body: dict[str, Any]) -> dict[str, Any]:
        return {
            "type": "server-response",
            "rpcId": body["rpcId"],
            "result": {"ok": True, "value": {"items": [1, 2]}},
        }

    with rpc_server(response) as (base_url, requests):
        result = DshRpcClient(base_url).call("session.list", {"cursor": "next"})

    assert result.ok is True
    assert result.value == {"items": [1, 2]}
    assert requests[0]["path"] == "/api/session.list"
    assert requests[0]["body"] == {
        "type": "client-request",
        "rpcId": result.rpc_id,
        "method": "session.list",
        "payload": {"cursor": "next"},
    }


def test_call_preserves_business_error() -> None:
    """HTTP 200 中的 DSH 业务错误应作为结果返回。"""

    def response(body: dict[str, Any]) -> dict[str, Any]:
        return {
            "type": "server-response",
            "rpcId": body["rpcId"],
            "result": {
                "ok": False,
                "error": {
                    "code": "session-not-found",
                    "message": "missing",
                    "details": {"sessionId": "nope"},
                },
            },
        }

    with rpc_server(response) as (base_url, _):
        result = DshRpcClient(base_url).call(
            "session.history", {"sessionId": "nope"}
        )

    assert result.ok is False
    assert result.error == {
        "code": "session-not-found",
        "message": "missing",
        "details": {"sessionId": "nope"},
    }


def test_call_rejects_mismatched_rpc_id() -> None:
    """客户端必须拒绝无法关联到请求的响应。"""

    def response(body: dict[str, Any]) -> dict[str, Any]:
        return {
            "type": "server-response",
            "rpcId": "another-request",
            "result": {"ok": True, "value": None},
        }

    with rpc_server(response) as (base_url, _):
        with pytest.raises(DshTransportError, match="rpcId"):
            DshRpcClient(base_url).call("host.describe", {})


@pytest.mark.asyncio
async def test_async_call_and_respond_use_wire_envelopes() -> None:
    """异步 RPC 与 respond 应使用各自的完整线协议信封。"""

    def response(body: dict[str, Any]) -> dict[str, Any]:
        if body["type"] == "client-response":
            return {"accepted": True}
        return {
            "type": "server-response",
            "rpcId": body["rpcId"],
            "result": {"ok": True, "value": {"version": "test"}},
        }

    with rpc_server(response) as (base_url, requests):
        client = DshRpcClient(base_url)
        result = await client.call_async("host.describe", {})
        receipt = await client.respond("question-id", {"ok": True, "value": {}})
        await client.close()

    assert result.value == {"version": "test"}
    assert receipt == {"accepted": True}
    assert requests[1] == {
        "path": "/api/respond",
        "body": {
            "type": "client-response",
            "rpcId": "question-id",
            "result": {"ok": True, "value": {}},
        },
    }


@pytest.mark.asyncio
async def test_request_rejects_cross_origin_path() -> None:
    """通用 HTTP 通道不得跳出配置的 DSH origin。"""

    client = DshRpcClient("http://127.0.0.1:1")
    with pytest.raises(ValueError, match="同源"):
        await client.request("GET", "https://example.com/api")
    await client.close()