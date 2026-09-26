"""taskmq 异常层次。

约定：
- 所有异常都继承 `TaskMQError`，便于调用方一次性捕获。
- 配置错误在 `App()` 构造期抛出（`ConfigError`），不留到运行时。
- 协议错误（编解码/版本/大小）在生产者侧或消费侧尽早抛出，不静默降级。
"""
from __future__ import annotations


class TaskMQError(Exception):
    """所有 taskmq 异常的基类。"""


class ConfigError(TaskMQError):
    """配置非法。在 `App()` 构造时抛出。"""


# --------------------------------------------------------------------- 协议
class ProtocolError(TaskMQError):
    """消息协议层错误。"""


class EncodeError(ProtocolError):
    """编码失败（未知类型、非有限浮点数等）。"""


class MessageTooLarge(EncodeError):
    """编码后超过 `max_message_bytes`。"""


class DecodeError(ProtocolError):
    """解码失败或协议版本不兼容。"""


class UnsupportedCodec(ProtocolError):
    """序列化器不可用（未安装或名字未知）。"""


# ----------------------------------------------------------------- transport
class TransportError(TaskMQError):
    """transport 层错误。"""


class MessageNotFound(TransportError):
    """引用的消息不存在。"""


class LeaseLost(TransportError):
    """当前 worker 已不持有该消息的租约（重复 ack / 租约过期后 ack）。

    这是 at-least-once 的正常现象：调用方应丢弃本地结果并记录，
    **不要**重试 ack，也不要把别人的投递状态改掉。
    """


# ---------------------------------------------------------------------- 任务
class TaskError(TaskMQError):
    """任务执行层错误。"""


class RemoteError(TaskError):
    """任务在 worker 侧失败；保留远端 traceback 文本。"""

    def __init__(self, message: str, *, error: str | None = None, traceback: str | None = None) -> None:
        super().__init__(message)
        self.error = error
        self.traceback = traceback


class RetryRequest(TaskMQError):
    """任务显式要求重试：`raise ctx.retry(reason="...")`。"""

    def __init__(self, reason: str = "", *, delay: float | None = None) -> None:
        super().__init__(reason or "retry requested")
        self.reason = reason
        self.delay = delay


class Reject(TaskMQError):
    """任务显式拒绝：立即进 DLQ，不重试。`raise ctx.reject(reason="...")`。"""

    def __init__(self, reason: str = "") -> None:
        super().__init__(reason or "rejected")
        self.reason = reason


class TaskTimeout(TaskError):
    """协作式超时（软超时）。"""


def error_text(exc: BaseException) -> str:
    """`类型: 消息` 形式的简短错误文本。"""
    text = str(exc)
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__
