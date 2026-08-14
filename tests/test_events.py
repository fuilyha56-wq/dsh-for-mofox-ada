"""测试 DSH Adapter 的 SSE 事件流与运行时监听器出口。"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from plugins.dsh_adapter.runtime import (
    DshBridgeRuntime,
    DshRuntimeEvent,
    DshRuntimeOptions,
)


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


@pytest.mark.asyncio
async def test_event_listener_receives_complete_runtime_event(
    tmp_path: Path,
) -> None:
    """注册的异步监听器应收到包含完整消息与桥游标的 DshRuntimeEvent。"""

    payload = {
        "type": "server-request",
        "rpcId": "question-1",
        "method": "events.mux",
        "payload": {
            "type": "question/requested",
            "sessionId": "session-1",
            "questions": [],
        },
    }
    raw = json.dumps(payload)

    async def handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        await reader.readline()
        while await reader.readline() not in {b"\r\n", b"\n", b""}:
            pass
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/event-stream\r\n"
            b"Cache-Control: no-cache\r\n"
            b"Connection: close\r\n\r\n"
            + f"data: {raw}\n\n".encode()
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    received: asyncio.Queue[DshRuntimeEvent] = asyncio.Queue()

    async def record_listener(event: DshRuntimeEvent) -> None:
        await received.put(event)

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
        listener_id = runtime.add_event_listener(record_listener)
        try:
            event = await asyncio.wait_for(received.get(), timeout=2.0)
        finally:
            await runtime.close()
    finally:
        server.close()
        await server.wait_closed()

    assert listener_id
    assert event.stream == "mux"
    assert event.sequence == 1
    assert event.received_at
    assert event.message == {
        "type": "server-request",
        "rpcId": "question-1",
        "method": "events.mux",
        "payload": {
            "type": "question/requested",
            "sessionId": "session-1",
            "questions": [],
        },
    }


@pytest.mark.asyncio
async def test_event_listeners_are_isolated_and_removable(tmp_path: Path) -> None:
    """抛错监听器不应拖垮其他监听器或 SSE 流；注销后监听器不再收到事件。"""

    payload_1 = {
        "type": "server-request",
        "rpcId": "question-1",
        "method": "events.mux",
        "payload": {
            "type": "question/requested",
            "sessionId": "session-1",
            "questions": [],
        },
    }
    payload_2 = {
        "type": "server-request",
        "rpcId": "question-2",
        "method": "events.mux",
        "payload": {"type": "question/answered", "sessionId": "session-1"},
    }
    release_second = asyncio.Event()

    async def handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        await reader.readline()
        while await reader.readline() not in {b"\r\n", b"\n", b""}:
            pass
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/event-stream\r\n"
            b"Cache-Control: no-cache\r\n"
            b"Connection: close\r\n\r\n"
            + f"data: {json.dumps(payload_1)}\n\n".encode()
        )
        await writer.drain()
        await release_second.wait()
        writer.write(f"data: {json.dumps(payload_2)}\n\n".encode())
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    received: asyncio.Queue[DshRuntimeEvent] = asyncio.Queue()

    async def throwing_listener(event: DshRuntimeEvent) -> None:
        raise RuntimeError(f"listener boom on {event.sequence}")

    async def record_listener(event: DshRuntimeEvent) -> None:
        await received.put(event)

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
        runtime.add_event_listener(throwing_listener)
        record_id = runtime.add_event_listener(record_listener)
        try:
            first = await asyncio.wait_for(received.get(), timeout=2.0)
            assert first.sequence == 1
            assert first.message == payload_1
            retained = await runtime.read_events("mux")
            assert retained["messages"][0]["message"] == payload_1
            assert runtime.remove_event_listener(record_id) is True
            assert runtime.remove_event_listener(record_id) is False
            release_second.set()
            second = await runtime.read_events(
                "mux", after_sequence=1, wait_seconds=2.0
            )
            assert second["cursor"] == 2
            assert second["messages"][0]["message"] == payload_2
            assert received.empty()
        finally:
            await runtime.close()
    finally:
        release_second.set()
        server.close()
        await server.wait_closed()