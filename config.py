"""DSH Adapter 插件配置。"""

from __future__ import annotations

from typing import ClassVar

from src.app.plugin_system.base import BaseConfig, Field, SectionBase, config_section


class DshBridgeConfig(BaseConfig):
    """定义 DSH 桥接、运行时与 HTTP 暴露策略。"""

    name: ClassVar[str] = "config"
    description: ClassVar[str] = "Neo-MoFox 与 DeepSeek Harness 桥接配置"

    @config_section("bridge")
    class BridgeSection(SectionBase):
        """DSH 进程与协议配置。"""

        enabled: bool = Field(default=True, description="是否启用 DSH 桥接")
        dsh_command: str = Field(default="dsh", description="DSH 可执行命令或绝对路径")
        dsh_home: str = Field(default="~/.dsh", description="DSH_HOME 数据目录")
        default_workspace: str = Field(default=".", description="DSH 默认工作目录")
        web_base_url: str = Field(
            default="http://127.0.0.1:18948",
            description="DSH Web profile 的根地址",
        )
        auto_start_web: bool = Field(
            default=True,
            description="连接失败时是否自动启动 Web profile",
        )
        web_start_timeout_seconds: float = Field(
            default=30.0,
            gt=0,
            description="等待 Web profile 就绪的秒数",
        )
        start_event_streams: bool = Field(
            default=True,
            description="是否自动订阅 mux 与 host 事件流",
        )
        default_timeout_seconds: float = Field(
            default=300.0,
            gt=0,
            description="CLI 与 HTTP 操作的默认超时",
        )
        max_timeout_seconds: float = Field(
            default=3600.0,
            gt=0,
            description="外部调用允许指定的最大超时",
        )
        max_response_bytes: int = Field(
            default=8 * 1024 * 1024,
            gt=0,
            description="单次 HTTP、CLI 或文件读取最大保留字节数",
        )
        process_output_bytes: int = Field(
            default=4 * 1024 * 1024,
            gt=0,
            description="每个长期进程保留的输出字节数",
        )
        event_buffer_size: int = Field(
            default=2000,
            gt=0,
            description="每条 DSH 事件流保留的消息数量",
        )
        allow_arbitrary_data_paths: bool = Field(
            default=False,
            description="是否允许直接读取 DSH_HOME 之外的文件",
        )
        allow_sensitive_data: bool = Field(
            default=False,
            description="是否允许直接读取 DSH 凭据存储文件",
        )

    @config_section("router")
    class RouterSection(SectionBase):
        """MoFox HTTP Router 的访问策略。"""

        enabled: bool = Field(default=True, description="是否暴露桥接 HTTP API")
        shared_token: str = Field(
            default="",
            description="远程请求必须提供的 X-DSH-Bridge-Token",
        )
        allow_remote_without_token: bool = Field(
            default=False,
            description="是否允许无令牌远程访问高权限桥接接口",
        )

    @config_section("llm")
    class LlmSection(SectionBase):
        """LLM 可见组件配置。"""

        expose_tools: bool = Field(default=True, description="是否向 LLM 注册查询工具")
        expose_actions: bool = Field(default=True, description="是否向 LLM 注册操作动作")
        max_result_characters: int = Field(
            default=30000,
            gt=0,
            description="Tool、Action 与 Command 返回文本最大字符数",
        )

    bridge: BridgeSection = Field(default_factory=BridgeSection)
    router: RouterSection = Field(default_factory=RouterSection)
    llm: LlmSection = Field(default_factory=LlmSection)
