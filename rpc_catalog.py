"""DeepSeek Harness RPC 方法与非 RPC 传输能力目录。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

RpcRisk = Literal["read", "write", "privileged", "secret"]

CATALOG_DSH_VERSION = "0.1.0-rc.6"
RPC_RISKS: tuple[RpcRisk, ...] = ("read", "write", "privileged", "secret")


@dataclass(frozen=True, slots=True)
class DshRpcMethodSpec:
    """描述一个 DSH 一元 RPC 方法的模型调用契约。"""

    method: str
    payload_schema: str
    summary: str
    risk: RpcRisk = "read"
    constraints: tuple[str, ...] = ()

    @property
    def domain(self) -> str:
        """返回 RPC 方法的顶级域名。"""

        return self.method.split(".", 1)[0]

    def to_dict(self) -> dict[str, Any]:
        """返回可序列化的方法描述。"""

        return {
            "method": self.method,
            "domain": self.domain,
            "payload_schema": self.payload_schema,
            "summary": self.summary,
            "risk": self.risk,
            "constraints": list(self.constraints),
        }


def _method(
    method: str,
    payload_schema: str,
    summary: str,
    *,
    risk: RpcRisk = "read",
    constraints: tuple[str, ...] = (),
) -> DshRpcMethodSpec:
    """创建一个简洁的 RPC 方法描述。"""

    return DshRpcMethodSpec(
        method=method,
        payload_schema=payload_schema,
        summary=summary,
        risk=risk,
        constraints=constraints,
    )


RPC_METHOD_SPECS: tuple[DshRpcMethodSpec, ...] = (
    # session.*
    _method(
        "session.list",
        "{ cursor?: string }",
        "列出全部持久化会话，按更新时间倒序返回。",
        constraints=("cursor 当前为保留字段。",),
    ),
    _method(
        "session.search",
        "{ query: string }",
        "跨会话搜索当前用户、助手和 steering 消息表面。",
        constraints=("最多返回 20 个会话；hasMore 表示需要收窄查询。",),
    ),
    _method(
        "session.create",
        "{ workspaceId?: string, cwd?: string, sessionId?: string, "
        "agentPreset?: string }",
        "创建真实会话并启动空闲 Agent。",
        risk="write",
        constraints=(
            "workspaceId 与 cwd 最多提供一个。",
            "agentPreset 必须来自 agentPreset.list。",
        ),
    ),
    _method(
        "session.history",
        "{ sessionId: string, beforeSeq?: number, maxMessages?: number }",
        "按完整消息边界分页读取会话原始事件和投影。",
    ),
    _method(
        "session.models",
        "{ sessionId: string }",
        "读取会话当前模型选择、可路由状态和实时模型目录。",
    ),
    _method(
        "session.selectModel",
        "{ sessionId: string, provider: string, model: string, "
        "reasoningEffort?: string }",
        "切换指定会话的完整模型路由。",
        risk="write",
        constraints=("reasoningEffort 必须匹配该精确模型目录。",),
    ),
    _method(
        "session.rename",
        "{ sessionId: string, title: string }",
        "设置并固定会话标题。",
        risk="write",
    ),
    _method(
        "session.fork",
        "{ sessionId: string, atSeq?: number }",
        "从一个已完成回合边界派生新会话。",
        risk="write",
        constraints=("开放回合内的锚点会返回 fork-unavailable。",),
    ),
    _method(
        "session.prompt",
        "{ sessionId: string, mode: 'queue' | 'steer', content: "
        "Array<{ type: 'text', text: string } | { type: 'image', "
        "mediaType: string, data: string, name?: string }>, "
        "clientTimeZone?: string }",
        "向普通会话发送文本或临时图片；斜杠开头文本可执行命令。",
        risk="write",
    ),
    _method(
        "session.attachment",
        "{ sessionId: string, attachmentId: string }",
        "读取会话日志已引用的耐久图片附件及 Base64 数据。",
    ),
    _method(
        "session.updateQueue",
        "{ sessionId: string, itemId: string, action: "
        "{ kind: 'edit', content: ContentBlock[] } | "
        "{ kind: 'remove' } | { kind: 'steer' } }",
        "编辑、删除或 steering 一个仍在等待的队列项。",
        risk="write",
    ),
    _method(
        "session.cancel",
        "{ sessionId: string }",
        "停止普通会话的当前活动回合并保留后续队列。",
        risk="write",
    ),
    # subagent.*
    _method(
        "subagent.list",
        "{ parentSessionId: string }",
        "列出一个会话的直接 session-backed 子代理目录。",
    ),
    _method(
        "subagent.history",
        "{ parentSessionId: string, childSessionId: string, "
        "mode: 'one-shot' | 'continuable', beforeSeq?: number, "
        "maxMessages?: number }",
        "读取一个健康直接子代理的消息对齐历史。",
    ),
    _method(
        "subagent.prompt",
        "{ parentSessionId: string, childSessionId: string, "
        "mode: 'continuable', content: ContentBlock[], "
        "clientTimeZone?: string }",
        "向可继续的直接子代理投递人类消息。",
        risk="write",
    ),
    _method(
        "subagent.interrupt",
        "{ parentSessionId: string, childSessionId: string, "
        "mode: 'continuable' }",
        "中断可继续子代理的当前回合。",
        risk="write",
    ),
    # host.*
    _method("host.describe", "{}", "读取 Host 版本、目录、默认模型和会话状态。"),
    _method(
        "host.pickDirectory",
        "{}",
        "打开操作系统目录选择器。",
        risk="privileged",
        constraints=("只在 native capability 下可用。",),
    ),
    _method(
        "host.listDirectory",
        "{ path?: string }",
        "列出 Host 文件系统中的一级目录和面包屑。",
        risk="privileged",
        constraints=("只在 browse capability 下可用。",),
    ),
    _method(
        "host.createDirectory",
        "{ path: string, name: string }",
        "在现有 Host 父目录下创建一个子目录。",
        risk="privileged",
    ),
    _method(
        "host.openPath",
        "{ path: string }",
        "使用操作系统默认应用打开 Host 文件系统路径。",
        risk="privileged",
    ),
    # workspace.*
    _method("workspace.list", "{}", "列出工作区及全局归档会话集合。"),
    _method(
        "workspace.create",
        "{ path: string }",
        "为现有目录创建或幂等取得工作区记录。",
        risk="write",
        constraints=("不会创建文件系统目录。",),
    ),
    _method(
        "workspace.rename",
        "{ workspaceId: string, title: string }",
        "修改工作区显示标题。",
        risk="write",
    ),
    _method(
        "workspace.delete",
        "{ workspaceId: string }",
        "删除工作区注册记录。",
        risk="write",
        constraints=("不会删除目录、用户文件或会话日志。",),
    ),
    _method(
        "workspace.insertBefore",
        "{ workspaceId: string, beforeWorkspaceId?: string }",
        "调整工作区显示顺序；省略锚点时追加。",
        risk="write",
    ),
    _method(
        "workspace.insertSessionBefore",
        "{ workspaceId: string, sessionId: string, beforeSessionId?: string }",
        "调整工作区内会话的手动顺序。",
        risk="write",
    ),
    _method(
        "workspace.archiveSession",
        "{ sessionId: string }",
        "把会话加入全局归档集合。",
        risk="write",
        constraints=("保留日志和工作区中的原有位置。",),
    ),
    # skill.*
    _method(
        "skill.list",
        "{ sessionId: string }",
        "列出会话项目中用户可调用的 Skill。",
        constraints=("调用 Skill 使用 session.prompt 发送 /name，而非独立 RPC。",),
    ),
    # agentPreset.*
    _method("agentPreset.list", "{}", "列出全部 Agent preset、信任级别和默认项。"),
    _method(
        "agentPreset.select",
        "{ sessionId: string, agentPreset: string }",
        "为指定空白会话切换 Agent preset。",
        risk="write",
        constraints=("只允许 blank=true 会话；否则返回 agent-preset-locked。",),
    ),
    _method(
        "agentPreset.read",
        "{ agentPreset: string }",
        "读取一个 preset 的完整 composition 文本。",
        risk="privileged",
    ),
    _method(
        "agentPreset.copy",
        "{ from: string, agentPreset: string, name?: string }",
        "复制现有 preset，创建本地可编辑 preset。",
        risk="privileged",
    ),
    _method(
        "agentPreset.openDocument",
        "{ agentPreset: string }",
        "用平台打开器打开本地用户 preset 目录。",
        risk="privileged",
        constraints=("随包 preset 不允许编辑。",),
    ),
    _method(
        "agentPreset.remove",
        "{ agentPreset: string }",
        "删除本地用户 preset。",
        risk="privileged",
        constraints=("随包 preset 不允许删除。",),
    ),
    # goal.*
    _method(
        "goal.create",
        "{ sessionId: string, objective: string, maxGoalRounds?: number }",
        "创建并启动会话目标。",
        risk="write",
    ),
    _method(
        "goal.edit",
        "{ sessionId: string, ref: { id: string, revision: number }, "
        "objective?: string, maxGoalRounds?: number }",
        "使用 CAS ref 编辑目标内容或回合上限。",
        risk="write",
    ),
    _method(
        "goal.pause",
        "{ sessionId: string, ref: { id: string, revision: number } }",
        "暂停当前目标。",
        risk="write",
    ),
    _method(
        "goal.resume",
        "{ sessionId: string, ref: { id: string, revision: number } }",
        "恢复并重新启动已停止目标。",
        risk="write",
    ),
    _method(
        "goal.complete",
        "{ sessionId: string, ref: { id: string, revision: number } }",
        "将当前目标标记为完成。",
        risk="write",
    ),
    _method(
        "goal.clear",
        "{ sessionId: string, ref: { id: string, revision: number } }",
        "清除当前目标并保留耐久 tombstone。",
        risk="write",
    ),
    # settings.*
    _method(
        "settings.describe",
        "{}",
        "读取全部设置 namespace、schema、脱敏值和 revision。",
        risk="privileged",
        constraints=("secret 字段只返回是否已设置，永不返回值。",),
    ),
    _method(
        "settings.openDocument",
        "{}",
        "创建并用平台文本编辑器打开本地设置文档。",
        risk="privileged",
    ),
    _method(
        "settings.update",
        "{ ns: string, patch: object, expectedRevision?: number }",
        "合并更新一个设置 namespace 的用户层。",
        risk="privileged",
        constraints=("建议始终携带 settings.describe 返回的 expectedRevision。",),
    ),
    _method(
        "settings.replace",
        "{ ns: string, section: object, expectedRevision?: number }",
        "整体替换一个设置 namespace 的用户 section。",
        risk="privileged",
        constraints=("未提供的键会被删除，包含 secret。",),
    ),
    _method(
        "settings.mutate",
        "{ ns: string, ops: Array<{ op: 'set', path: string[], value: unknown } "
        "| { op: 'unset', path: string[] }>, expectedRevision?: number }",
        "按路径原子设置或移除 namespace 字段。",
        risk="privileged",
        constraints=("建议始终携带 settings.describe 返回的 expectedRevision。",),
    ),
    # credentials.*
    _method(
        "credentials.describe",
        "{ refs: string[] }",
        "批量读取凭据引用的 configured、source 和 writable 状态。",
        risk="privileged",
        constraints=("响应永不包含凭据值。",),
    ),
    _method(
        "credentials.set",
        "{ ref: string, value: string }",
        "写入一个凭据引用。",
        risk="secret",
        constraints=("不要从聊天中索取或回显 secret；工具参数可能进入调用日志。",),
    ),
    _method(
        "credentials.unset",
        "{ ref: string }",
        "移除一个可写层凭据引用。",
        risk="secret",
    ),
    # llm.*
    _method("llm.providers", "{}", "列出可配置 LLM provider 及活动状态。"),
    _method("llm.models", "{}", "列出 Host 范围内全部实时模型目录和失败项。"),
    _method(
        "llm.discoverModels",
        "{ settingsNs: string, provider?: string, baseURL?: string, "
        "api?: string, apiKey?: string }",
        "探测尚在编辑中的 provider endpoint 可用模型。",
        risk="secret",
        constraints=(
            "不会写入设置。",
            "apiKey 只发送给目标 endpoint，不存储也不返回；工具参数可能进入日志。",
        ),
    ),
)

RPC_METHODS_BY_NAME: dict[str, DshRpcMethodSpec] = {
    spec.method: spec for spec in RPC_METHOD_SPECS
}

NON_RPC_CAPABILITIES: tuple[dict[str, Any], ...] = (
    {
        "name": "server_request_response",
        "operation": "respond",
        "transport": "POST /api/respond",
        "summary": "回答 events.mux/events.host 发起的问题、审批和交互请求。",
    },
    {
        "name": "same_origin_http",
        "operation": "http_request",
        "transport": "HTTP",
        "summary": "请求 DSH Web 同源任意路径，包括 /api/session.export。",
    },
    {
        "name": "event_streams",
        "operation": "event_start/event_read/event_stop",
        "transport": "WebSocket /api/events.mux 与 /api/events.host",
        "summary": "订阅、缓冲并读取完整下行 ServerRequest 与 Host 事件。",
    },
    {
        "name": "cli_and_profiles",
        "operation": "cli_run/headless/ensure_web",
        "transport": "DSH CLI",
        "summary": "运行任意 CLI 参数、profile、patch、headless 和 Web profile。",
    },
    {
        "name": "managed_processes",
        "operation": "process_start/process_write/process_output/process_stop",
        "transport": "stdin/stdout/stderr",
        "summary": "管理可持续交互的任意 DSH CLI/profile 进程。",
    },
    {
        "name": "dsh_home_data",
        "operation": "data_list/data_read",
        "transport": "filesystem",
        "summary": "在配置的路径与敏感数据边界内列举、分页读取 DSH_HOME。",
    },
)


def query_rpc_catalog(
    *,
    method: str | None = None,
    domain: str | None = None,
    risk: str | None = None,
    include_details: bool = False,
) -> dict[str, Any]:
    """查询完整 RPC 目录或一个精确方法契约。"""

    normalized_method = method.strip() if method is not None else ""
    normalized_domain = domain.strip() if domain is not None else ""
    normalized_risk = risk.strip() if risk is not None else ""
    if normalized_method and normalized_domain:
        raise ValueError("method 与 domain 不能同时提供")
    if normalized_risk and normalized_risk not in RPC_RISKS:
        raise ValueError(f"risk 必须是: {', '.join(RPC_RISKS)}")
    if normalized_method:
        spec = RPC_METHODS_BY_NAME.get(normalized_method)
        if spec is None:
            raise ValueError(f"当前目录没有 RPC 方法: {normalized_method}")
        return {
            "dsh_version": CATALOG_DSH_VERSION,
            "method_count": len(RPC_METHOD_SPECS),
            "method": spec.to_dict(),
            "forward_compatible": "rpc_call 和 dsh_rpc 不限制目录方法名",
            "non_rpc_capabilities": [dict(item) for item in NON_RPC_CAPABILITIES],
        }

    selected = [
        spec
        for spec in RPC_METHOD_SPECS
        if (not normalized_domain or spec.domain == normalized_domain)
        and (not normalized_risk or spec.risk == normalized_risk)
    ]
    if normalized_domain and not selected:
        domains = sorted({spec.domain for spec in RPC_METHOD_SPECS})
        raise ValueError(
            f"当前目录没有 RPC 域: {normalized_domain}; 可用域: {', '.join(domains)}"
        )

    result: dict[str, Any] = {
        "dsh_version": CATALOG_DSH_VERSION,
        "method_count": len(RPC_METHOD_SPECS),
        "selected_count": len(selected),
        "risks": list(RPC_RISKS),
        "forward_compatible": "rpc_call 和 dsh_rpc 不限制目录方法名",
        "non_rpc_capabilities": [dict(item) for item in NON_RPC_CAPABILITIES],
    }
    if include_details or normalized_domain or normalized_risk:
        result["methods"] = [spec.to_dict() for spec in selected]
    else:
        domains: dict[str, list[str]] = {}
        for spec in selected:
            domains.setdefault(spec.domain, []).append(spec.method)
        result["domains"] = domains
    return result