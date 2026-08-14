"""测试 DSH Adapter 的 SSE 事件流。"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from plugins.dsh_adapter.runtime import DshBridgeRuntime, DshRuntimeOptions


@pytest.mark.asyncio
async def test_event_stream_preserves_complete_server_request(tmp_path: Path) -> None:
    """事件订阅应完整保留 DSH ServerRequest 信封并提供桥游标。"""

    expected = (
        '{"type":"server-request","rpcId":"question-1",'
        '"method":"session/questions","payload":{"items":[{"id":"one"}]}}'
    )

    requested_paths: list[str] = []

    async def handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        request_line = (await reader.readline()).decode("ascii")
        requested_paths.append(request_line.split()[1])
        while await reader.readline() not in {b"\r\n", b"\n", b""}:
            pass
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/event-stream\r\n"
            b"Cache-Control: no-cache\r\n"
            b"Connection: close\r\n\r\n"
            + f"data: {expected}\n\n".encode()
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    try:
        port = server.sockets[0].getsockname()[1]
        runtime = DshBridgeRuntime(
            DshRuntimeOptions(
                dsh_command="missing-dsh",
                dsh_home=tmp_path,
                default_workspace=tmp_path,
                web_base_url=f"http://127.0.0.1:{port}",
                event_buffer_size=10,
            )
        )
        await runtime.start_event_stream("mux")
        result = await runtime.read_events("mux", wait_seconds=2.0)
        await runtime.close()
    finally:
        server.close()
        await server.wait_closed()

    assert requested_paths[0] == "/api/events.mux"
    assert result["cursor"] == 1
    assert result["messages"][0]["message"] == {
        "type": "server-request",
        "rpcId": "question-1",
        "method": "session/questions",
        "payload": {"items": [{"id": "one"}]},
    }