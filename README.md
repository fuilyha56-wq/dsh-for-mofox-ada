# DSH Adapter

[![dshfind](https://dshfind.com/api/badge/fuilyha56-wq/dsh-for-mofox-ada?lang=zh)](https://dshfind.com/zh/plugins/fuilyha56-wq/dsh-for-mofox-ada?ref=badge)

`dsh_adapter` 让 Neo-MoFox 调用和管理本机 DeepSeek Harness。插件不会复刻 DSH
内部能力，而是完整保留 DSH 官方边界：任意一元 RPC、同源 HTTP、CLI 参数、
profile、长期进程、HTTP SSE 下行事件和 `DSH_HOME` 数据。

当前发布版本：`1.0.0`。

## 安装

仓库根目录提供 DSH bundle，可先将其登记到 DSH Web profile：

```sh
dsh plugin --profile web add github:fuilyha56-wq/dsh-for-mofox-ada
```

本地开发或已克隆仓库时：

```sh
dsh plugin --profile web add .
```

该 bundle 的作用是让 DSH profile 记录此集成包；它不在 DSH 的 Node/Cordis 进程中
执行 Neo-MoFox 的 Python 代码。完整桥接运行时仍必须作为 Neo-MoFox 插件部署：将本目录
放入 Neo-MoFox 的 `plugins/`，重启或重新加载 Neo-MoFox，然后确认
`dsh_adapter:adapter:dsh_adapter` 已注册。DSH Web 与 Neo-MoFox 可以运行在同一台主机，
默认通过 `http://127.0.0.1:18948` 通信。

未发布到 npm 前，建议固定可信提交安装，例如：

```sh
dsh plugin --profile web add github:fuilyha56-wq/dsh-for-mofox-ada#COMMIT_SHA
```

DSH 使用 pnpm 安装 git 依赖；若它提示需要在 profile 的 `pnpm-workspace.yaml` 中授权
`allowBuilds`，仅在审阅并信任该固定提交后再授予权限。本包没有构建步骤或安装脚本。

## 组件

| 签名 | 用途 |
| --- | --- |
| `dsh_adapter:service:dsh_adapter` | 供其他插件调用全部桥接操作 |
| `dsh_adapter:command:dsh` | Owner 级 `/dsh` 管理命令 |
| `dsh_adapter:adapter:dsh_adapter` | 将每个 DSH Web session 映射为 Neo-MoFox 私聊流 |
| `dsh_adapter:router:dsh_adapter` | `/api/dsh-adapter` HTTP API |
| `dsh_adapter:tool:dsh_query` | LLM 只读查询 Tool |
| `dsh_adapter:action:dsh_headless` | LLM 委派任务给 DSH headless Agent |
| `dsh_adapter:action:dsh_model_switch` | LLM 按实时目录切换 DSH 会话模型 |
| `dsh_adapter:action:dsh_preset_switch` | LLM 切换空白 DSH 会话的 Agent preset 模式 |
| `dsh_adapter:action:dsh_operate` | LLM 完整 DSH 操作 Action |
| `dsh_adapter:action:dsh_respond` | LLM 在当前 DSH 私聊流中结构化回答问题或审批 |

## 启动

当前机器已经全局安装 `@deepseek-ai/dsh`。默认配置使用 `dsh` 命令和
`~/.dsh`，并在 `http://127.0.0.1:18948` 自动启动 Web profile。

Neo-MoFox 首次加载插件时会生成：

```text
config/plugins/dsh_adapter/config.toml
```

常用配置：

```toml
[bridge]
enabled = true
dsh_command = "dsh"
dsh_home = "~/.dsh"
default_workspace = "."
web_base_url = "http://127.0.0.1:18948"
auto_start_web = true
start_event_streams = true

[router]
enabled = true
shared_token = ""
allow_remote_without_token = false

[llm]
expose_tools = true
expose_actions = true

[interaction]
enabled = true
approval_policy = "ask"
progress_delivery = "aggregate"
progress_window_seconds = 2.0
max_event_text_characters = 12000
persist_pending_requests = true
```

`web_base_url` 若指向已运行的 DSH Web 服务，插件会直接复用；否则
`auto_start_web = true` 时由插件启动。插件卸载时只清理由插件自己管理的进程。

`interaction.enabled = true` 且 `bridge.start_event_streams = true` 时，原生 Adapter
负责先注册 Runtime listener、再订阅 `GET /api/events.mux` 与
`GET /api/events.host` 两条 HTTP SSE 流。每个 DSH `sessionId` 都是一个
`platform=dsh` 的独立 Neo-MoFox 私聊流；关闭 interaction 不影响现有 CLI、RPC、Router
或 Service 能力。

## 聊天命令

```text
/dsh status
/dsh sessions
/dsh models
/dsh models session-e8664bf4-1f7e-479e-bd6c-9a04ff87f3e1
/dsh model session-e8664bf4-1f7e-479e-bd6c-9a04ff87f3e1 deepseek-v4-flash high
/dsh presets
/dsh preset session-e8664bf4-1f7e-479e-bd6c-9a04ff87f3e1 "PTC 模式"
/dsh headless "检查当前项目并运行测试" "E:\project"
/dsh rpc host.describe "{}"
/dsh rpc session.list "{}"
/dsh cli "[\"--version\"]"
/dsh exec process_start "{\"process_id\":\"web2\",\"arguments\":[\"--profile\",\"web\",\"--port\",\"19000\"]}"
/dsh processes
/dsh output web2 0
/dsh stop web2
/dsh events mux 0
/dsh data sessions
/dsh pending
/dsh pending session-e8664bf4-1f7e-479e-bd6c-9a04ff87f3e1
/dsh respond answer QUESTION_RPC_ID '[{"id":"language","selected":["Python"]}]'
/dsh respond cancel QUESTION_RPC_ID
/dsh respond approval APPROVAL_RPC_ID allow
/dsh respond approval APPROVAL_RPC_ID reject
```

命令权限为 `OWNER`。带空格或 JSON 的参数必须整体加引号，解析规则与 Neo-MoFox
其他命令一致。

## LLM 调用

- `dsh_query`：可用 `session_list` 查会话 ID，使用 `model_list` 查模型，使用 `preset_list` 查 Agent preset 模式，也可查询状态、输出、事件和数据。
- `dsh_headless`：让 DSH Agent 在指定工作目录完成任务，任务可能修改文件。
- `dsh_model_switch`：切换指定会话模型；provider 留空时按实时目录自动解析并校验推理等级。
- `dsh_preset_switch`：切换 `blank=true` 空白会话的模式；支持 preset ID 或显示名。
- `dsh_operate`：使用 `operation` 与 `parameters_json` 调用全部操作。
- `dsh_respond`：只处理当前 `platform=dsh` 私聊流的 pending 请求，不能跨 session
  回答。`answer` 传入问题答案 JSON 数组，`cancel` 取消问题，`approve`/`reject` 回答审批。

例如切换到当前 DSH 原生提供的 DeepSeek-V4-Flash：

```json
{
  "session_id": "SESSION_ID",
  "model": "deepseek-v4-flash",
  "reasoning_effort": "high"
}
```

`model_switch` 会先调用 `session.models` 校验目录，并把显示名规范化为真实模型 ID；
调用方不需要猜测 provider。成功的会话选择由 DSH 自身持久化为默认模型设置。

DSH 当前内置模式：

| ID | 显示名 | 用途 |
| --- | --- | --- |
| `standard` | 标准模式 | 完整编码 Agent，包含编辑、Shell、检索、Skills、计划、目标和子代理 |
| `code` | PTC 模式 | 标准模式能力加 Code Mode SDK，由 TypeScript 程序组合多步操作 |
| `minimal` | 极简模式 | 仅提供持久 bash 与 `str_replace_editor` |
| `cordis` | 创造模式 | 创建和实验自定义 Agent preset |

DSH 只允许尚未开始对话的空白会话切换模式。模型应先调用 `session_list`，选择
`blank=true` 的会话，再调用 `preset_list` 和 `dsh_preset_switch`；非空白会话会由 DSH
返回 `agent-preset-locked`，适配器不会绕过这一安全约束。

## HTTP API

Router 挂载在 `/api/dsh-adapter`：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/operations` | 列出统一操作 |
| `GET` | `/status` | 查询 DSH 与桥状态 |
| `POST` | `/execute` | 执行任意统一操作 |
| `POST` | `/rpc` | 调用任意 DSH RPC |
| `POST` | `/headless` | 执行 headless 任务 |

通用调用示例：

```json
POST /api/dsh-adapter/execute
{
  "operation": "rpc_call",
  "parameters": {
    "method": "session.list",
    "payload": {}
  }
}
```

未设置 `shared_token` 时仅允许环回请求。设置后，所有请求都必须携带：

```text
X-DSH-Bridge-Token: <shared_token>
```

DSH Web 自身没有远程认证层。不要将 DSH Web 端口或本 Router 无保护地暴露到公网。

## Service API

其他插件通过公开 Service API 获取实例：

```python
from typing import Any, Protocol, cast

from src.app.plugin_system.api import service_api


class DshAdapterServiceProtocol(Protocol):
    """DSH Adapter Service 的最小公共形状。"""

    async def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """执行一个统一桥接操作。"""

    async def switch_model(
      self,
      session_id: str,
      model: str,
      *,
      reasoning_effort: str | None = None,
      provider: str | None = None,
    ) -> dict[str, Any]:
      """切换指定 DSH 会话模型。"""


service = cast(
    DshAdapterServiceProtocol,
    service_api.get_service("dsh_adapter:service:dsh_adapter"),
)
result = await service.switch_model(
  "SESSION_ID",
  "deepseek-v4-flash",
  reasoning_effort="high",
)
```

完整参数见 [API.md](API.md)。

## DSH Web 会话交互

普通 DSH Web 会话可在执行过程中发出 `question/requested` 和
`approval/requested`。它们进入对应 session 的私聊流，普通文本不会被猜测为答案；只要
该 session 有 pending 交互，普通 `session.prompt` 出站会失败，必须使用
`dsh_respond`、Owner `/dsh respond` 命令或 Service API。

问题答案是对象数组，例如：

```json
[
  {"id": "language", "selected": ["Python"]},
  {"id": "style", "selected": ["custom"], "custom": "保持 PEP 8"}
]
```

审批策略由 `interaction.approval_policy` 控制：

| 策略 | `dsh_respond` Bot | Owner 命令 | Service |
| --- | --- | --- | --- |
| `ask` | 仅可 `reject` | 可 `allow` 或 `reject` | 仅可 `reject` |
| `autonomous` | 可 `approve` 或 `reject` | 可 `allow` 或 `reject` | 可 `allowed-once` 或 `rejected` |
| `reject` | 新审批由 Adapter 自动 `rejected` | 仅可 `reject` | 仅可 `rejected` |

每次 `allowed-once` 都精确绑定当前 `rpcId`、`approvalId` 和 session，不能复用。只有
DSH 回执 `{"accepted": true}` 后 pending 才会消费；网络失败或其他拒绝会保留为可重试，
`reason="not-pending"` 则标记为 stale。

## 数据与安全边界

- CLI 通道只能执行配置的 DSH 可执行文件，但参数、profile、patch、stdin 和环境变量可透传。
- 通用 HTTP 通道只能请求 `web_base_url` 的同源相对路径，不能转为任意 URL 请求器。
- 直接文件读取默认限制在 `DSH_HOME`，并拒绝 `.credentials.yaml`。
- `allow_arbitrary_data_paths = true` 可扩大直接文件读取范围。
- `allow_sensitive_data = true` 才允许读取 DSH 凭据存储；不要向 LLM 开启此项。
- 输出、响应、文件和事件缓冲均有配置上限，避免无界内存增长。
- `respond` 可回答 DSH 事件流发起的问题；调用方必须使用事件中的原始 `rpcId`。

## 验证

```powershell
uv run pytest plugins/dsh_adapter/tests -q -p no:cacheprovider --no-cov
```
