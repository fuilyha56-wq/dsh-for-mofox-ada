# DSH Adapter 操作参考

所有入口最终调用同一个接口：

```python
await service.execute(operation, parameters)
```

HTTP 使用 `POST /api/dsh-adapter/execute`，LLM Action 使用 `operation` 和
`parameters_json`。未知操作或未知参数会明确失败，不会静默忽略。

## 操作表

| 操作 | 参数 | 说明 |
| --- | --- | --- |
| `status` | 无 | DSH 路径、Web、进程、事件流和操作列表 |
| `session_list` | 无 | 列出 DSH 会话及会话 ID |
| `model_list` | `session_id?` | 查询主机模型目录，或指定会话的当前选择和目录 |
| `model_switch` | `session_id`, `model`, `provider?`, `reasoning_effort?` | 按实时目录校验并切换会话模型 |
| `preset_list` | 无 | 查询 DSH 实时提供的 Agent preset 模式 |
| `preset_switch` | `session_id`, `preset` | 按 ID 或显示名切换指定空白会话模式 |
| `ensure_web` | `start_timeout?` | 探测或启动配置的 Web profile |
| `headless` | `task`, `cwd?`, `environment?`, `timeout?`, `patches?` | 运行一次 headless Agent 任务 |
| `rpc_call` | `method`, `payload?` | 调用任意 `/api/<method>` 一元 RPC |
| `respond` | `rpc_id`, `result` | POST `/api/respond`，回答问题或交互请求 |
| `http_request` | `method`, `path`, `query?`, `headers?`, `json_body?`, `body_base64?` | 请求 DSH 同源任意 HTTP 路径 |
| `cli_run` | `arguments`, `cwd?`, `stdin?`, `environment?`, `timeout?` | 执行任意 DSH CLI 参数并等待退出 |
| `process_start` | `process_id`, `arguments`, `cwd?`, `environment?` | 启动长期 DSH profile/命令 |
| `process_write` | `process_id`, `text` | 写入长期进程 stdin |
| `process_output` | `process_id`, `after_sequence?`, `limit?`, `wait_seconds?` | 增量读取 stdout/stderr |
| `process_list` | 无 | 列出长期进程 |
| `process_stop` | `process_id` | 停止长期进程 |
| `event_start` | `stream` | 启动 `mux` 或 `host` WebSocket 下行流 |
| `event_stop` | `stream` | 停止事件流 |
| `event_status` | `stream` | 查询事件流状态 |
| `event_read` | `stream`, `after_sequence?`, `limit?`, `wait_seconds?` | 增量读取完整 ServerRequest 信封 |
| `data_list` | `path?`, `recursive?`, `pattern?`, `limit?` | 列出 DSH 数据目录 |
| `data_read` | `path`, `offset?`, `limit?` | 分页读取原始文件，返回 Base64 和可选 UTF-8 文本 |

带 `?` 的参数可省略。`arguments`、`patches` 是字符串数组；`environment`、
`query`、`headers` 是字符串键值对象。

## 会话与模型

先用 `session_list` 取得目标会话 ID，再用 `model_list` 查询 DSH 实际提供的模型：

```json
{
  "operation": "model_list",
  "parameters": {
    "session_id": "SESSION_ID"
  }
}
```

切换模型时可以省略 provider，适配器会在该会话的实时目录中唯一解析，并校验可选
推理等级：

```json
{
  "operation": "model_switch",
  "parameters": {
    "session_id": "SESSION_ID",
    "model": "deepseek-v4-flash",
    "reasoning_effort": "high"
  }
}
```

模型 ID 或显示名均可匹配；真正提交给 `session.selectModel` 的是目录中的规范 ID。
不存在的模型会返回可用的 `provider/model` 列表，不会把 MoFox 模型配置误当作 DSH 目录。

## Agent preset 模式

先用 `session_list` 选择 `blank=true` 的空白会话，再查询实时模式目录：

```json
{
  "operation": "preset_list",
  "parameters": {}
}
```

切换时可传目录 ID，也可传显示名：

```json
{
  "operation": "preset_switch",
  "parameters": {
    "session_id": "SESSION_ID",
    "preset": "PTC 模式"
  }
}
```

当前内置映射为 `standard`/标准模式、`code`/PTC 模式、`minimal`/极简模式、
`cordis`/创造模式。适配器先调用 `agentPreset.list` 解析真实 ID，再调用
`agentPreset.select`。DSH 仅允许空白会话切换；已有对话的会话会返回
`agent-preset-locked`。

## 任意 RPC

`rpc_call` 不维护硬编码方法白名单，因此可承载当前和后续 DSH 一元 RPC。是否支持
某个方法及其 payload schema 由运行中的 DSH 决定。当前安装可见的域包括：

- `session.*`：list、search、create、history、models、selectModel、rename、fork、prompt、attachment、updateQueue、cancel
- `subagent.*`：list、history、prompt、interrupt
- `workspace.*`：list、create、rename、delete、排序、归档
- `host.*`：describe、目录选择/浏览/创建、openPath
- `agentPreset.*`、`goal.*`、`skill.list`
- `settings.*`、`credentials.*`、`llm.*`

示例：

```json
{
  "operation": "rpc_call",
  "parameters": {
    "method": "session.history",
    "payload": {
      "sessionId": "SESSION_ID",
      "maxMessages": 100
    }
  }
}
```

业务失败仍是 HTTP 200，并保留在结果的 `ok: false` 与 `error` 中；传输或协议错误才
由桥抛出异常。

## 任意 CLI 与 Profile

`cli_run` 和 `process_start` 直接将字符串数组交给配置的 DSH 入口，不经过 shell
拼接。由此可使用任意已安装 profile、patch 和 plugin 子命令：

```json
{
  "operation": "cli_run",
  "parameters": {
    "arguments": ["--profile", "web", "--dump-config"]
  }
}
```

```json
{
  "operation": "process_start",
  "parameters": {
    "process_id": "custom-profile",
    "arguments": ["--profile", "custom", "--patch", "E:\\patch.yml"],
    "cwd": "E:\\workspace"
  }
}
```

需要持续运行、持续读取输出或写入 stdin 时使用 `process_*`；单次命令使用
`cli_run`。`process_id` 允许字母、数字、点、下划线和连字符，长度最多 64。

## 事件与交互

Web profile 提供两条只下行 WebSocket：`events.mux` 与 `events.host`。桥为每条消息
添加独立递增 `sequence`，但 `message` 内仍是 DSH 原始完整信封。

收到需要回答的 ServerRequest 后，取其 `rpcId`，再调用：

```json
{
  "operation": "respond",
  "parameters": {
    "rpc_id": "SERVER_REQUEST_RPC_ID",
    "result": {
      "ok": true,
      "value": {}
    }
  }
}
```

具体 `value` 结构由该 ServerRequest 的方法定义；错误结构同样可通过 `result`
原样发送。

## HTTP 下载与非 RPC 接口

`http_request` 覆盖不属于一元 RPC 的 DSH 路径，例如会话日志导出：

```json
{
  "operation": "http_request",
  "parameters": {
    "method": "GET",
    "path": "/api/session.export",
    "query": {
      "sessionId": "SESSION_ID",
      "includeDescendants": "true"
    }
  }
}
```

二进制响应始终返回 `body_base64`。文本或 JSON 响应还会返回 `text`。
