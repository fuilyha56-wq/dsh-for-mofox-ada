"""测试 DeepSeek Harness 共享运行时。"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from plugins.dsh_adapter.runtime import DshBridgeRuntime, DshRuntimeOptions


def make_runtime(tmp_path: Path, **overrides: object) -> DshBridgeRuntime:
    """创建使用当前 Python 解释器作为测试命令的运行时。"""

    values: dict[str, object] = {
        "dsh_command": sys.executable,
        "dsh_home": tmp_path / "dsh-home",
        "default_workspace": tmp_path,
        "web_base_url": "http://127.0.0.1:1",
        "default_timeout": 5.0,
        "max_timeout": 10.0,
        "max_response_bytes": 1024 * 1024,
        "process_output_bytes": 1024 * 1024,
        "event_buffer_size": 10,
    }
    values.update(overrides)
    options = DshRuntimeOptions(**values)  # type: ignore[arg-type]
    options.dsh_home.mkdir(parents=True)
    return DshBridgeRuntime(options)


@pytest.mark.asyncio
async def test_run_cli_preserves_arguments_stdin_and_environment(tmp_path: Path) -> None:
    """前台执行应完整承载参数、stdin 和环境变量。"""

    runtime = make_runtime(tmp_path)
    code = (
        "import os,sys;"
        "print(sys.argv[1]);"
        "print(os.environ['BRIDGE_TEST']);"
        "print(sys.stdin.read())"
    )
    result = await runtime.run_cli(
        ["-c", code, "hello world"],
        stdin="input text",
        environment={"BRIDGE_TEST": "env value"},
    )
    await runtime.close()

    assert result.exit_code == 0
    assert result.stdout.splitlines() == ["hello world", "env value", "input text"]
    assert result.timed_out is False


@pytest.mark.asyncio
async def test_managed_process_supports_input_incremental_output_and_exit(
    tmp_path: Path,
) -> None:
    """长期进程应支持输入、增量输出与退出状态读取。"""

    runtime = make_runtime(tmp_path)
    code = "import sys; print('ready', flush=True); print(sys.stdin.readline(), end='')"
    await runtime.start_process("interactive", ["-u", "-c", code])
    first = await runtime.read_process_output("interactive", wait_seconds=2.0)
    await runtime.write_process("interactive", "answer\n")
    second = await runtime.read_process_output(
        "interactive",
        after_sequence=first["cursor"],
        wait_seconds=2.0,
    )
    await runtime.stop_process("interactive")
    await runtime.close()

    assert "ready" in "".join(item["text"] for item in first["entries"])
    assert "answer" in "".join(item["text"] for item in second["entries"])


@pytest.mark.asyncio
async def test_data_access_is_bounded_to_dsh_home_by_default(tmp_path: Path) -> None:
    """默认直接文件接口只能读取 DSH_HOME 内的数据。"""

    runtime = make_runtime(tmp_path)
    data_file = runtime.options.dsh_home / "sessions" / "one.jsonl"
    data_file.parent.mkdir()
    data_file.write_bytes(b'{"type":"user/message"}\n')
    result = await runtime.read_data("sessions/one.jsonl")

    with pytest.raises(PermissionError, match="DSH_HOME"):
        runtime.resolve_data_path(str(tmp_path / "outside.txt"))
    (runtime.options.dsh_home / ".credentials.yaml").write_text(
        "secret: value", encoding="utf-8"
    )
    with pytest.raises(PermissionError, match="allow_sensitive_data"):
        await runtime.read_data(".credentials.yaml")
    with pytest.raises(ValueError, match=r"\.\."):
        runtime.list_data("sessions", pattern="../*")
    await runtime.close()

    assert result["text"] == '{"type":"user/message"}\n'
    assert runtime.list_data("sessions")["entries"][0]["relative_path"] == "one.jsonl"