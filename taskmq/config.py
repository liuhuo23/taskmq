"""配置模型：frozen dataclass + 构造即校验 + 显式环境变量映射。

优先级相关字段对应 docs/design/priority.md v1.0 的 P1–P19：
`default_priority` / `priority_min` / `priority_max` / `yield_enabled` / `yield_min_delta` /
`yield_delay` / `max_yields` / `normal_reserved_slots` / `retry_priority`。
"""
from __future__ import annotations

import dataclasses
import os
from collections.abc import Callable, Mapping
from typing import Any

from ._compat import _SLOTS
from .errors import ConfigError, UnsupportedCodec
from .priority import PRIORITY_MAX, PRIORITY_MIN, validate_priority
from .protocol import get_codec

SUPPORTED_POOLS = ("solo", "threads", "processes", "asyncio")
SUPPORTED_LOG_FORMATS = ("json", "pretty")
SUPPORTED_RETRY_PRIORITY = ("keep", "lower")


@dataclasses.dataclass(frozen=True, **_SLOTS)
class QueueConfig:
    """单个逻辑队列的配置。

    `weight` 只在**同优先级档位内**决定取件比例（P3）；`priority` 只作为「任务/提交都没写
    优先级时的 fallback」，不参与跨档位比较。
    """

    weight: int = 1
    priority: int = 0
    max_size: int | None = None
    ttl: float | None = None


@dataclasses.dataclass(frozen=True, **_SLOTS)
class Config:
    transport: Any = "memory://"
    result: str | None = None
    result_ttl: float | None = None
    default_queue: str = "default"
    queues: Mapping[str, QueueConfig] = dataclasses.field(default_factory=dict)
    concurrency: int = 4
    pool: str = "threads"
    prefetch: int | None = None
    lease: float = 60.0
    heartbeat_interval: float = 10.0
    shutdown_timeout: float = 30.0
    serializer: str = "msgspec"
    max_message_bytes: int = 256 * 1024
    timezone: str = "UTC"
    log_format: str = "json"
    events: str = "stdout"          # "stdout" | "null"（结构化事件，§14.1）
    poll_interval: float = 0.05
    max_poll_interval: float = 0.5
    eager: bool = False
    # ---- 优先级（§20-9）----
    default_priority: int = 0
    priority_min: int = PRIORITY_MIN
    priority_max: int = PRIORITY_MAX
    yield_enabled: bool = True
    yield_min_delta: int = 1
    yield_delay: float = 0.1
    max_yields: int = 100
    normal_reserved_slots: int = 0
    priority_aging: float | None = None  # Phase 2
    retry_priority: str = "keep"
    # ---- 其它 ----
    max_deliveries: int = 5
    idempotency_ttl: float = 86400.0

    # ---------------------------------------------------------------- 校验
    def validate(self) -> None:
        if isinstance(self.transport, str):
            if "://" not in self.transport:
                raise ConfigError(f"transport URL 非法：{self.transport!r}（需要 scheme://...）")
        elif not hasattr(self.transport, "enqueue"):
            raise ConfigError("transport 必须是 URL 字符串或 Transport 实例")

        from .plugins import known_pools, pool_factory

        if self.pool not in SUPPORTED_POOLS and pool_factory(self.pool) is None:
            known = tuple(SUPPORTED_POOLS) + tuple(sorted(set(known_pools()) - set(SUPPORTED_POOLS)))
            raise ConfigError(f"未知 pool={self.pool!r}，可选 {known}")
        if self.concurrency < 1:
            raise ConfigError(f"concurrency 必须 >= 1，收到 {self.concurrency}")
        if self.prefetch is not None and self.prefetch < 1:
            raise ConfigError(f"prefetch 必须 >= 1（None = concurrency），收到 {self.prefetch}")
        if self.lease <= 0:
            raise ConfigError(f"lease 必须 > 0，收到 {self.lease}")
        if self.max_message_bytes < 1024:
            raise ConfigError(f"max_message_bytes 太小：{self.max_message_bytes}")
        if self.log_format not in SUPPORTED_LOG_FORMATS:
            raise ConfigError(f"未知 log_format={self.log_format!r}")
        if not isinstance(self.events, str) or not self.events:
            # 具体名字（含插件注册的 sink）由 events.build_sink 在 App 构造时解析并报错 → fail fast
            raise ConfigError(
                f'events 必须是 "stdout" | "null" | "otel" 或插件注册的 sink 名，收到 {self.events!r}'
            )
        if self.result and not self.result_ttl:
            raise ConfigError("配置了 result 就必须给 result_ttl（无 TTL 会被拒绝）")
        if self.result_ttl is not None and self.result_ttl <= 0:
            raise ConfigError(f"result_ttl 必须 > 0，收到 {self.result_ttl}")

        validate_priority(self.default_priority, where="Config.default_priority")
        if self.priority_min != PRIORITY_MIN or self.priority_max != PRIORITY_MAX:
            raise ConfigError(
                f"priority_min/max 固定为 {PRIORITY_MIN}..{PRIORITY_MAX}（P1 决策），收到 "
                f"{self.priority_min}..{self.priority_max}"
            )
        if self.yield_min_delta < 1:
            raise ConfigError(f"yield_min_delta 必须 >= 1，收到 {self.yield_min_delta}")
        if self.yield_delay < 0:
            raise ConfigError(f"yield_delay 必须 >= 0，收到 {self.yield_delay}")
        if self.max_yields < 1:
            raise ConfigError(f"max_yields 必须 >= 1，收到 {self.max_yields}")
        if not 0 <= self.normal_reserved_slots < self.concurrency:
            raise ConfigError(
                f"normal_reserved_slots 必须满足 0 <= n < concurrency({self.concurrency})，"
                f"收到 {self.normal_reserved_slots}"
            )
        if self.retry_priority not in SUPPORTED_RETRY_PRIORITY:
            raise ConfigError(f"retry_priority 可选 {SUPPORTED_RETRY_PRIORITY}，收到 {self.retry_priority!r}")
        if self.max_deliveries < 1:
            raise ConfigError(f"max_deliveries 必须 >= 1，收到 {self.max_deliveries}")

        for name, queue in self.queues.items():
            if not isinstance(queue, QueueConfig):
                raise ConfigError(f"queues[{name!r}] 必须是 QueueConfig，收到 {type(queue).__name__}")
            if queue.weight < 1:
                raise ConfigError(f"queues[{name!r}].weight 必须 >= 1，收到 {queue.weight}")
            validate_priority(queue.priority, where=f"queues[{name!r}].priority")

        try:
            get_codec(self.serializer)
        except UnsupportedCodec as exc:
            raise ConfigError(str(exc)) from exc

    # ------------------------------------------------------------ 环境变量
    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None, **overrides: Any) -> Config:
        """显式映射 `TASKMQ_<UPPER_FIELD>`；不做通配魔法（P2）。"""
        env = os.environ if environ is None else environ
        parsers: dict[str, Callable[[str], Any]] = {
            "transport": str,
            "result": str,
            "result_ttl": float,
            "default_queue": str,
            "concurrency": int,
            "pool": str,
            "prefetch": int,
            "lease": float,
            "heartbeat_interval": float,
            "shutdown_timeout": float,
            "serializer": str,
            "max_message_bytes": int,
            "timezone": str,
            "log_format": str,
            "poll_interval": float,
            "max_poll_interval": float,
            "eager": _parse_bool,
            "default_priority": int,
            "yield_enabled": _parse_bool,
            "yield_min_delta": int,
            "yield_delay": float,
            "max_yields": int,
            "normal_reserved_slots": int,
            "retry_priority": str,
            "max_deliveries": int,
            "idempotency_ttl": float,
        }
        values: dict[str, Any] = {}
        for field_name, parser in parsers.items():
            key = f"TASKMQ_{field_name.upper()}"
            raw = env.get(key)
            if raw is None or raw == "":
                continue
            try:
                values[field_name] = parser(raw)
            except ValueError as exc:
                raise ConfigError(f"环境变量 {key}={raw!r} 解析失败：{exc}") from exc
        values.update(overrides)
        return cls(**values)

    # ------------------------------------------------------------ 派生属性
    @property
    def effective_prefetch(self) -> int:
        return self.prefetch if self.prefetch is not None else self.concurrency

    def queue_weight(self, queue: str) -> int:
        queue_config = self.queues.get(queue)
        return queue_config.weight if queue_config else 1


def _parse_bool(raw: str) -> bool:
    lowered = raw.strip().lower()
    if lowered in ("1", "true", "yes", "on"):
        return True
    if lowered in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"不是合法布尔值：{raw!r}")


__all__ = ["Config", "QueueConfig", "SUPPORTED_POOLS"]
