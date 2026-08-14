"""DeepSeek Harness 进程、数据和事件流共享运行时。"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import shutil
import subprocess
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import aiofiles

from src.kernel.concurrency import get_task_manager
from src.kernel.concurrency.task_info import TaskInfo

from .client import DshRpcClient, DshTransportError

_PROCESS_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DshRuntimeOptions:
    """保存不依赖 MoFox 配置模型的运行时选项。"""

    dsh_command: str = "dsh"
    dsh_home: Path = Path.home() / ".dsh"
    default_workspace: Path = Path.cwd()
    web_base_url: str = "http://127.0.0.1:18948"
    default_timeout: float = 300.0
    max_timeout: float = 3600.0
    max_response_bytes: int = 8 * 1024 * 1024
    process_output_bytes: int = 4 * 1024 * 1024
    event_buffer_size: int = 2000
    allow_arbitrary_data_paths: bool = False
    allow_sensitive_data: bool = False


@dataclass(frozen=True, slots=True)
class DshCommandResult:
    """表示一次前台 DSH CLI 执行结果。"""

    argv: list[str]
    cwd: str
    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool
    output_truncated: bool
    duration_seconds: float

    def to_dict(self) -> dict[str, Any]:
        """返回可序列化的命令结果。"""

        return {
            "argv": self.argv,
            "cwd": self.cwd,
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "timed_out": self.timed_out,
            "output_truncated": self.output_truncated,
            "duration_seconds": self.duration_seconds,
        }


@dataclass(frozen=True, slots=True)
class OutputEntry:
    """表示长期进程的一段标准输出或错误输出。"""

    sequence: int
    stream: str
    text: str
    time: str
    byte_count: int

    def to_dict(self) -> dict[str, Any]:
        """返回可序列化的输出段。"""

        return {
            "sequence": self.sequence,
            "stream": self.stream,
            "text": self.text,
            "time": self.time,
            "byte_count": self.byte_count,
        }


@dataclass(slots=True)
class ManagedProcess:
    """保存一个长期 DSH 子进程及其有界输出。"""

    process_id: str
    argv: list[str]
    cwd: str
    process: asyncio.subprocess.Process
    started_at: str
    output_limit: int
    output: deque[OutputEntry] = field(default_factory=deque)
    output_bytes: int = 0
    next_sequence: int = 1
    dropped_through: int = 0
    reader_tasks: list[TaskInfo] = field(default_factory=list)
    monitor_task: TaskInfo | None = None
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)

    def status(self) -> dict[str, Any]:
        """返回当前进程状态。"""

        return {
            "process_id": self.process_id,
            "pid": self.process.pid,
            "argv": self.argv,
            "cwd": self.cwd,
            "started_at": self.started_at,
            "running": self.process.returncode is None,
            "exit_code": self.process.returncode,
            "next_sequence": self.next_sequence,
            "dropped_through": self.dropped_through,
            "retained_output_bytes": self.output_bytes,
        }


@dataclass(slots=True)
class EventStreamState:
    """保存一条 DSH SSE 下行流的状态。"""

    name: str
    messages: deque[dict[str, Any]]
    next_sequence: int = 1
    dropped_through: int = 0
    task: TaskInfo | None = None
    stopping: bool = False
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)


@dataclass(frozen=True, slots=True)
class DshRuntimeEvent:
    """表示 Runtime 已缓冲的一条 DSH 下行事件。"""

    stream: str
    sequence: int
    received_at: str
    message: dict[str, Any]


DshEventListener = Callable[[DshRuntimeEvent], Awaitable[None]]


class DshBridgeRuntime:
    """统一管理所有 DSH 调用通道与生命周期。"""

    def __init__(self, options: DshRuntimeOptions) -> None:
        """初始化共享运行时。"""

        self.options = options
        self.client = DshRpcClient(
            base_url=options.web_base_url,
            timeout=options.default_timeout,
            max_response_bytes=options.max_response_bytes,
        )
        self._task_manager = get_task_manager()
        self._processes: dict[str, ManagedProcess] = {}
        self._event_streams: dict[str, EventStreamState] = {}
        self._event_listeners: dict[str, DshEventListener] = {}
        self._closed = False

    def resolve_timeout(self, requested: float | None) -> float:
        """解析外部超时并强制应用配置上限。"""

        value = self.options.default_timeout if requested is None else requested
        if value <= 0:
            raise ValueError("timeout 必须大于 0")
        return min(value, self.options.max_timeout)

    def resolve_executable(self) -> Path:
        """定位配置的 DSH 命令。"""

        expanded = Path(os.path.expandvars(os.path.expanduser(self.options.dsh_command)))
        if expanded.is_file():
            return expanded.resolve()
        resolved = shutil.which(self.options.dsh_command)
        if resolved is None:
            raise FileNotFoundError(f"找不到 DSH 命令: {self.options.dsh_command}")
        return Path(resolved).resolve()

    def build_argv(self, arguments: list[str]) -> list[str]:
        """构造可直接交给 CreateProcess 的 DSH 参数列表。"""

        executable = self.resolve_executable()
        suffix = executable.suffix.lower()
        if suffix in {".cmd", ".bat"}:
            node_entry = (
                executable.parent
                / "node_modules"
                / "@deepseek-ai"
                / "dsh"
                / "lib"
                / "bin.js"
            )
            node = shutil.which("node")
            if node is not None and node_entry.is_file():
                return [str(Path(node).resolve()), str(node_entry), *arguments]
            comspec = os.environ.get("COMSPEC", "cmd.exe")
            command_line = subprocess.list2cmdline([str(executable), *arguments])
            return [comspec, "/d", "/s", "/c", command_line]
        if suffix == ".ps1":
            shell = shutil.which("pwsh") or shutil.which("powershell")
            if shell is None:
                raise FileNotFoundError("无法执行 DSH PowerShell 启动脚本")
            return [shell, "-NoProfile", "-File", str(executable), *arguments]
        return [str(executable), *arguments]

    async def run_cli(
        self,
        arguments: list[str],
        *,
        cwd: str | None = None,
        stdin: str | None = None,
        environment: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> DshCommandResult:
        """执行任意 DSH CLI 参数并等待退出。"""

        self._ensure_open()
        argv = self.build_argv(self._normalize_arguments(arguments))
        working_directory = self._resolve_workspace(cwd)
        process = await self._spawn(argv, working_directory, environment)
        started = time.monotonic()
        stdout_task = self._task_manager.create_task(
            self._capture_stream(process.stdout),
            name="dsh-adapter-cli-stdout",
        )
        stderr_task = self._task_manager.create_task(
            self._capture_stream(process.stderr),
            name="dsh-adapter-cli-stderr",
        )
        if process.stdin is not None:
            if stdin is not None:
                process.stdin.write(stdin.encode("utf-8"))
                await process.stdin.drain()
            process.stdin.close()

        timed_out = False
        try:
            await asyncio.wait_for(process.wait(), timeout=self.resolve_timeout(timeout))
        except TimeoutError:
            timed_out = True
            await self._terminate(process)
        stdout, stdout_truncated = await self._task_result(stdout_task)
        stderr, stderr_truncated = await self._task_result(stderr_task)
        return DshCommandResult(
            argv=argv,
            cwd=str(working_directory),
            exit_code=process.returncode if process.returncode is not None else -1,
            stdout=stdout,
            stderr=stderr,
            timed_out=timed_out,
            output_truncated=stdout_truncated or stderr_truncated,
            duration_seconds=round(time.monotonic() - started, 3),
        )

    async def start_process(
        self,
        process_id: str,
        arguments: list[str],
        *,
        cwd: str | None = None,
        environment: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """启动一个可持续交互的 DSH profile 或命令。"""

        self._ensure_open()
        self._validate_process_id(process_id)
        existing = self._processes.get(process_id)
        if existing is not None and existing.process.returncode is None:
            raise ValueError(f"进程已存在: {process_id}")
        if existing is not None:
            self._processes.pop(process_id)

        argv = self.build_argv(self._normalize_arguments(arguments))
        working_directory = self._resolve_workspace(cwd)
        process = await self._spawn(argv, working_directory, environment)
        record = ManagedProcess(
            process_id=process_id,
            argv=argv,
            cwd=str(working_directory),
            process=process,
            started_at=datetime.now(UTC).isoformat(),
            output_limit=self.options.process_output_bytes,
        )
        self._processes[process_id] = record
        if process.stdout is not None:
            record.reader_tasks.append(
                self._task_manager.create_task(
                    self._read_process_stream(record, "stdout", process.stdout),
                    name=f"dsh-adapter-{process_id}-stdout",
                    daemon=True,
                )
            )
        if process.stderr is not None:
            record.reader_tasks.append(
                self._task_manager.create_task(
                    self._read_process_stream(record, "stderr", process.stderr),
                    name=f"dsh-adapter-{process_id}-stderr",
                    daemon=True,
                )
            )
        record.monitor_task = self._task_manager.create_task(
            self._monitor_process(record),
            name=f"dsh-adapter-{process_id}-monitor",
            daemon=True,
        )
        return record.status()

    async def write_process(self, process_id: str, text: str) -> dict[str, Any]:
        """向长期 DSH 进程的标准输入写入文本。"""

        record = self._get_process(process_id)
        if record.process.returncode is not None or record.process.stdin is None:
            raise RuntimeError(f"进程不可写: {process_id}")
        record.process.stdin.write(text.encode("utf-8"))
        await record.process.stdin.drain()
        return record.status()

    async def read_process_output(
        self,
        process_id: str,
        *,
        after_sequence: int = 0,
        limit: int = 200,
        wait_seconds: float = 0.0,
    ) -> dict[str, Any]:
        """按游标读取长期进程的增量输出。"""

        record = self._get_process(process_id)
        bounded_limit = max(1, min(limit, 2000))
        if wait_seconds > 0 and not any(
            item.sequence > after_sequence for item in record.output
        ):
            try:
                async with record.condition:
                    await asyncio.wait_for(
                        record.condition.wait_for(
                            lambda: any(
                                item.sequence > after_sequence for item in record.output
                            )
                            or record.process.returncode is not None
                        ),
                        timeout=min(wait_seconds, 30.0),
                    )
            except TimeoutError:
                pass
        entries = [
            item.to_dict()
            for item in record.output
            if item.sequence > after_sequence
        ][:bounded_limit]
        return {
            **record.status(),
            "entries": entries,
            "cursor": entries[-1]["sequence"] if entries else after_sequence,
            "cursor_was_dropped": after_sequence < record.dropped_through,
        }

    def list_processes(self) -> list[dict[str, Any]]:
        """列出所有由桥管理的 DSH 进程。"""

        return [record.status() for record in self._processes.values()]

    async def stop_process(self, process_id: str) -> dict[str, Any]:
        """终止一个由桥管理的 DSH 进程。"""

        record = self._get_process(process_id)
        await self._terminate(record.process)
        if record.monitor_task is not None and record.monitor_task.task is not None:
            await asyncio.gather(record.monitor_task.task, return_exceptions=True)
        return record.status()

    async def ensure_web(self, start_timeout: float = 30.0) -> dict[str, Any]:
        """确保配置的 DSH Web profile 正在运行。"""

        try:
            probe = await self.client.call_async("host.describe", {})
            if probe.ok:
                return {"started": False, "host": probe.to_dict()}
        except DshTransportError:
            pass

        parsed = urlsplit(self.options.web_base_url)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise RuntimeError("自动启动仅支持本机 http DSH Web 地址")
        port = parsed.port or 80
        await self.start_process(
            "dsh-web",
            [
                "--profile",
                "web",
                "--host",
                parsed.hostname,
                "--port",
                str(port),
            ],
        )
        deadline = time.monotonic() + start_timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                probe = await self.client.call_async("host.describe", {})
                if probe.ok:
                    return {"started": True, "host": probe.to_dict()}
            except DshTransportError as exc:
                last_error = exc
            record = self._get_process("dsh-web")
            if record.process.returncode is not None:
                output = await self.read_process_output("dsh-web")
                raise RuntimeError(f"DSH Web 提前退出: {output}")
            await asyncio.sleep(0.2)
        await self.stop_process("dsh-web")
        raise TimeoutError(f"等待 DSH Web 就绪超时: {last_error}")

    async def start_event_stream(self, name: str) -> dict[str, Any]:
        """启动 ``mux`` 或 ``host`` SSE 事件订阅。"""

        if name not in {"mux", "host"}:
            raise ValueError("事件流名称必须是 mux 或 host")
        state = self._event_streams.get(name)
        if state is None:
            state = EventStreamState(
                name=name,
                messages=deque(maxlen=self.options.event_buffer_size),
            )
            self._event_streams[name] = state
        if state.task is not None and not state.task.is_done():
            return self.event_stream_status(name)
        state.stopping = False
        state.task = self._task_manager.create_task(
            self._event_stream_loop(state),
            name=f"dsh-adapter-events-{name}",
            daemon=True,
        )
        return self.event_stream_status(name)

    async def stop_event_stream(self, name: str) -> dict[str, Any]:
        """停止一条 DSH SSE 事件订阅。"""

        state = self._event_streams.get(name)
        if state is None:
            return {"name": name, "running": False, "message_count": 0}
        state.stopping = True
        if state.task is not None:
            state.task.cancel()
            if state.task.task is not None:
                await asyncio.gather(state.task.task, return_exceptions=True)
        return self.event_stream_status(name)

    async def read_events(
        self,
        name: str,
        *,
        after_sequence: int = 0,
        limit: int = 200,
        wait_seconds: float = 0.0,
    ) -> dict[str, Any]:
        """按游标读取事件流缓冲区。"""

        state = self._event_streams.get(name)
        if state is None:
            raise KeyError(f"事件流尚未启动: {name}")
        if wait_seconds > 0 and not any(
            item["sequence"] > after_sequence for item in state.messages
        ):
            try:
                async with state.condition:
                    await asyncio.wait_for(
                        state.condition.wait_for(
                            lambda: any(
                                item["sequence"] > after_sequence
                                for item in state.messages
                            )
                            or state.stopping
                        ),
                        timeout=min(wait_seconds, 30.0),
                    )
            except TimeoutError:
                pass
        messages = [
            item for item in state.messages if item["sequence"] > after_sequence
        ][: max(1, min(limit, 2000))]
        return {
            **self.event_stream_status(name),
            "messages": messages,
            "cursor": messages[-1]["sequence"] if messages else after_sequence,
            "cursor_was_dropped": after_sequence < state.dropped_through,
        }

    def event_stream_status(self, name: str) -> dict[str, Any]:
        """返回事件流订阅状态。"""

        state = self._event_streams.get(name)
        if state is None:
            return {"name": name, "running": False, "message_count": 0}
        return {
            "name": name,
            "running": state.task is not None and not state.task.is_done(),
            "message_count": len(state.messages),
            "next_sequence": state.next_sequence,
            "dropped_through": state.dropped_through,
        }

    def add_event_listener(self, listener: DshEventListener) -> str:
        """注册一个异步事件监听器并返回其唯一标识。"""

        listener_id = uuid4().hex
        self._event_listeners[listener_id] = listener
        return listener_id

    def remove_event_listener(self, listener_id: str) -> bool:
        """按唯一标识注销事件监听器。"""

        return self._event_listeners.pop(listener_id, None) is not None

    def resolve_data_path(self, path: str) -> Path:
        """解析一个直接数据访问路径并执行 DSH_HOME 边界检查。"""

        root = self.options.dsh_home.expanduser().resolve()
        candidate = Path(os.path.expandvars(os.path.expanduser(path)))
        resolved = candidate.resolve() if candidate.is_absolute() else (root / candidate).resolve()
        try:
            relative = resolved.relative_to(root)
        except ValueError:
            relative = None
        if not self.options.allow_arbitrary_data_paths and relative is None:
            raise PermissionError("直接数据访问仅限 DSH_HOME")
        if (
            not self.options.allow_sensitive_data
            and relative is not None
            and relative.parts
            and relative.parts[0].lower() in {".credentials.yaml", "credentials.yaml"}
        ):
            raise PermissionError("读取 DSH 凭据存储需要 allow_sensitive_data")
        return resolved

    async def read_data(
        self,
        path: str,
        *,
        offset: int = 0,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """读取 DSH 数据文件的一段原始内容。"""

        target = self.resolve_data_path(path)
        if not target.is_file():
            raise FileNotFoundError(f"DSH 数据文件不存在: {target}")
        if offset < 0:
            raise ValueError("offset 不能小于 0")
        read_limit = self.options.max_response_bytes if limit is None else limit
        if read_limit <= 0 or read_limit > self.options.max_response_bytes:
            raise ValueError("limit 超出允许范围")
        async with aiofiles.open(target, "rb") as handle:
            await handle.seek(offset)
            raw = await handle.read(read_limit + 1)
        truncated = len(raw) > read_limit
        raw = raw[:read_limit]
        result: dict[str, Any] = {
            "path": str(target),
            "offset": offset,
            "byte_count": len(raw),
            "next_offset": offset + len(raw),
            "truncated": truncated,
            "body_base64": base64.b64encode(raw).decode("ascii"),
        }
        try:
            result["text"] = raw.decode("utf-8")
        except UnicodeDecodeError:
            pass
        return result

    def list_data(
        self,
        path: str = "",
        *,
        recursive: bool = False,
        pattern: str = "*",
        limit: int = 500,
    ) -> dict[str, Any]:
        """列出 DSH 数据目录内容。"""

        target = self.resolve_data_path(path)
        if not target.is_dir():
            raise NotADirectoryError(f"DSH 数据目录不存在: {target}")
        pattern_path = Path(pattern)
        if pattern_path.is_absolute() or ".." in pattern_path.parts:
            raise ValueError("pattern 不能是绝对路径或包含 ..")
        bounded_limit = max(1, min(limit, 5000))
        iterator = target.rglob(pattern) if recursive else target.glob(pattern)
        entries: list[dict[str, Any]] = []
        truncated = False
        for item in iterator:
            if len(entries) >= bounded_limit:
                truncated = True
                break
            stat = item.stat()
            entries.append(
                {
                    "path": str(item),
                    "relative_path": item.relative_to(target).as_posix(),
                    "type": "directory" if item.is_dir() else "file",
                    "size": stat.st_size,
                    "modified_at": datetime.fromtimestamp(
                        stat.st_mtime, tz=UTC
                    ).isoformat(),
                }
            )
        return {"path": str(target), "entries": entries, "truncated": truncated}

    async def close(self) -> None:
        """停止所有事件流和长期进程并关闭 HTTP 连接。"""

        if self._closed:
            return
        self._closed = True
        for name in list(self._event_streams):
            await self.stop_event_stream(name)
        for process_id, record in list(self._processes.items()):
            if record.process.returncode is None:
                await self.stop_process(process_id)
        await self.client.close()

    async def _spawn(
        self,
        argv: list[str],
        cwd: Path,
        environment: dict[str, str] | None,
    ) -> asyncio.subprocess.Process:
        """创建带管道和独立 Windows 进程组的子进程。"""

        env = os.environ.copy()
        env["DSH_HOME"] = str(self.options.dsh_home.expanduser().resolve())
        if environment:
            env.update({str(key): str(value) for key, value in environment.items()})
        creationflags = (
            subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        )
        return await asyncio.create_subprocess_exec(
            *argv,
            cwd=str(cwd),
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=creationflags,
        )

    async def _capture_stream(
        self,
        stream: asyncio.StreamReader | None,
    ) -> tuple[str, bool]:
        """持续排空一条输出流并仅保留配置上限内的字节。"""

        if stream is None:
            return "", False
        chunks: list[bytes] = []
        retained = 0
        truncated = False
        while chunk := await stream.read(65536):
            available = self.options.max_response_bytes - retained
            if available > 0:
                chunks.append(chunk[:available])
                retained += min(len(chunk), available)
            if len(chunk) > available:
                truncated = True
        return b"".join(chunks).decode("utf-8", errors="replace"), truncated

    async def _read_process_stream(
        self,
        record: ManagedProcess,
        stream_name: str,
        stream: asyncio.StreamReader,
    ) -> None:
        """读取长期进程流并写入有界输出缓冲。"""

        while chunk := await stream.read(65536):
            await self._append_process_output(record, stream_name, chunk)

    async def _append_process_output(
        self,
        record: ManagedProcess,
        stream_name: str,
        raw: bytes,
    ) -> None:
        """向长期进程缓冲追加一段输出。"""

        entry = OutputEntry(
            sequence=record.next_sequence,
            stream=stream_name,
            text=raw.decode("utf-8", errors="replace"),
            time=datetime.now(UTC).isoformat(),
            byte_count=len(raw),
        )
        record.next_sequence += 1
        record.output.append(entry)
        record.output_bytes += entry.byte_count
        while record.output and record.output_bytes > record.output_limit:
            dropped = record.output.popleft()
            record.output_bytes -= dropped.byte_count
            record.dropped_through = dropped.sequence
        async with record.condition:
            record.condition.notify_all()

    async def _monitor_process(self, record: ManagedProcess) -> None:
        """等待长期进程退出并唤醒输出等待者。"""

        await record.process.wait()
        async with record.condition:
            record.condition.notify_all()

    async def _event_stream_loop(self, state: EventStreamState) -> None:
        """连接并自动重连 DSH 下行 SSE。"""

        path = f"/api/events.{state.name}"
        while not state.stopping:
            try:
                async for raw in self.client.stream_sse(path):
                    if state.stopping:
                        return
                    try:
                        message = json.loads(raw)
                        if not isinstance(message, dict):
                            raise ValueError("事件顶层不是对象")
                    except (json.JSONDecodeError, ValueError) as exc:
                        message = {
                            "type": "bridge/decode-error",
                            "error": str(exc),
                            "raw": raw,
                        }
                    await self._append_event(state, message)
                if not state.stopping:
                    await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._append_event(
                    state,
                    {"type": "bridge/connection-error", "error": str(exc)},
                )
                await asyncio.sleep(1.0)

    async def _append_event(
        self,
        state: EventStreamState,
        message: dict[str, Any],
    ) -> None:
        """为事件添加桥游标、写入有界缓冲并逐个通知监听器。"""

        sequence = state.next_sequence
        received_at = datetime.now(UTC).isoformat()
        if len(state.messages) == state.messages.maxlen and state.messages:
            state.dropped_through = state.messages[0]["sequence"]
        state.messages.append(
            {
                "sequence": sequence,
                "received_at": received_at,
                "message": message,
            }
        )
        state.next_sequence += 1
        async with state.condition:
            state.condition.notify_all()
        event = DshRuntimeEvent(
            stream=state.name,
            sequence=sequence,
            received_at=received_at,
            message=message,
        )
        for listener_id, listener in list(self._event_listeners.items()):
            try:
                await listener(event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _logger.exception(
                    "事件监听器 %s 处理 %s#%d 时发生异常: %s",
                    listener_id,
                    state.name,
                    sequence,
                    exc,
                )

    async def _terminate(self, process: asyncio.subprocess.Process) -> None:
        """先温和终止，超时后强制结束子进程。"""

        if process.returncode is not None:
            return
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5.0)
        except TimeoutError:
            process.kill()
            await process.wait()

    async def _task_result(self, task_info: TaskInfo) -> tuple[str, bool]:
        """等待任务管理器中的捕获任务并返回结果。"""

        if task_info.task is None:
            raise RuntimeError("任务管理器未创建底层任务")
        result = await task_info.task
        if not isinstance(result, tuple):
            raise RuntimeError("输出捕获任务返回类型无效")
        return result

    def _resolve_workspace(self, cwd: str | None) -> Path:
        """解析并校验 DSH 工作目录。"""

        raw = cwd if cwd is not None and cwd.strip() else str(self.options.default_workspace)
        path = Path(os.path.expandvars(os.path.expanduser(raw))).resolve()
        if not path.is_dir():
            raise NotADirectoryError(f"DSH 工作目录不存在: {path}")
        return path

    @staticmethod
    def _normalize_arguments(arguments: list[str]) -> list[str]:
        """将外部 CLI 参数规范化为字符串列表。"""

        if not isinstance(arguments, list) or not all(
            isinstance(argument, str) for argument in arguments
        ):
            raise TypeError("arguments 必须是字符串数组")
        return arguments

    @staticmethod
    def _validate_process_id(process_id: str) -> None:
        """校验调用方提供的进程标识符。"""

        if not _PROCESS_ID_RE.fullmatch(process_id):
            raise ValueError("process_id 只能包含字母、数字、点、下划线和连字符")

    def _get_process(self, process_id: str) -> ManagedProcess:
        """取得已登记进程。"""

        try:
            return self._processes[process_id]
        except KeyError as exc:
            raise KeyError(f"进程不存在: {process_id}") from exc

    def _ensure_open(self) -> None:
        """拒绝在运行时关闭后创建新任务。"""

        if self._closed:
            raise RuntimeError("DSH 运行时已关闭")
