"""taskmq：零外部服务、投递语义可预测、配置显式的 Python 分布式任务队列。

公开 API 只暴露这些名字；其余都是内部实现。
"""
from __future__ import annotations

from importlib.metadata import PackageNotFoundError as _PackageNotFound
from importlib.metadata import version as _package_version

from .app import App
from .config import Config, QueueConfig
from .errors import (
    ConfigError,
    DecodeError,
    EncodeError,
    LeaseLost,
    MessageNotFound,
    MessageTooLarge,
    ProtocolError,
    Reject,
    RemoteError,
    RetryRequest,
    TaskError,
    TaskMQError,
    TaskTimeout,
    TransportError,
    UnsupportedCodec,
)
from .plugins import (
    TransportOptions,
    register_codec,
    register_pool,
    register_sink,
    register_transport,
)
from .priority import PRIORITY_MAX, PRIORITY_MIN, Priority, validate_priority
from .protocol import PROTOCOL_VERSION, Envelope
from .task import Retry, Task, TaskContext, TaskHandle, current_app, current_task
from .transport.base import (
    DeadLetter,
    Delivery,
    JobRecord,
    JobState,
    MessageState,
    QueueStat,
    Transport,
    WorkerInfo,
)
from .transport.memory import MemoryTransport
from .transport.postgres import PostgresTransport
from .transport.redis import RedisTransport
from .transport.sqlite import SqliteTransport
from .worker.runner import Worker

try:                       # 版本以打包元数据为准（pyproject 是唯一来源，避免两处漂移）
    __version__ = _package_version("taskmq")
except _PackageNotFound:   # pragma: no cover - 未安装、直接源码 import
    __version__ = "0.0.0+unknown"

__all__ = [
    "App",
    "Config",
    "QueueConfig",
    "Retry",
    "Task",
    "TaskHandle",
    "TaskContext",
    "Worker",
    "current_task",
    "current_app",
    "Priority",
    "TransportOptions",
    "register_codec",
    "register_pool",
    "register_sink",
    "register_transport",
    "PRIORITY_MIN",
    "PRIORITY_MAX",
    "validate_priority",
    "Envelope",
    "PROTOCOL_VERSION",
    "Transport",
    "MemoryTransport",
    "SqliteTransport",
    "RedisTransport",
    "PostgresTransport",
    "Delivery",
    "JobRecord",
    "JobState",
    "MessageState",
    "QueueStat",
    "DeadLetter",
    "WorkerInfo",
    "TaskMQError",
    "ConfigError",
    "ProtocolError",
    "EncodeError",
    "DecodeError",
    "MessageTooLarge",
    "UnsupportedCodec",
    "TransportError",
    "MessageNotFound",
    "LeaseLost",
    "TaskError",
    "RemoteError",
    "RetryRequest",
    "Reject",
    "TaskTimeout",
    "__version__",
]
