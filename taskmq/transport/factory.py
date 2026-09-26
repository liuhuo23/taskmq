"""内建 transport 的构造工厂（App 与「状态侧车」共用）。

为什么要单独抽出来：`amqp://` 需要一个**状态侧车**（job 状态 / 命名租约 / worker 表 / job 枚举），
而侧车本身不是 App；这里是**唯一**的 scheme 解析点，避免 URL 解析在 App 和插件里各写一份。

插件 scheme 不走这里（由 `App` 查注册表 + entry point 懒发现）。
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import unquote

from ..errors import ConfigError
from ..protocol import Codec, CodecRegistry, JSONCodec
from .base import Transport
from .memory import MemoryTransport
from .postgres import PostgresTransport, parse_postgres_url
from .redis import RedisTransport
from .sqlite import SqliteTransport

__all__ = ["build_transport", "FACTORY_SCHEMES"]

#: 这个工厂认识的 scheme（App 只用它对内建 scheme 分派）
FACTORY_SCHEMES = ("memory", "sqlite", "redis", "postgres", "postgresql", "amqp")


def _parse_query(url: str) -> tuple[str, dict[str, str]]:
    if "?" not in url:
        return url, {}
    base, _, query = url.partition("?")
    params: dict[str, str] = {}
    for item in query.split("&"):
        if not item:
            continue
        name, _, value = item.partition("=")
        params[name] = unquote(value)
    return base, params


def _parse_bool(raw: str | None) -> bool | None:
    if raw is None or raw == "":
        return None
    return raw.strip().lower() not in ("0", "off", "false", "no")


def build_transport(
    url: str,
    *,
    codec: Codec | None = None,
    registry: CodecRegistry | None = None,
    idempotency_ttl: float = 86400.0,
    max_message_bytes: int | None = None,
    queue_weights: Mapping[str, int] | None = None,
    clock: Any | None = None,
) -> Transport:
    """按 URL 造一个内建 transport；scheme 未知时抛 `ConfigError`。

    `amqp://` 需要 `?state=<另一个 transport URL>` 作为状态侧车（AMQP 是消息代理，没有 KV 存储）。
    """
    scheme = url.split("://", 1)[0] if "://" in url else ""
    if codec is None:                      # 内建 transport 都要真 codec；工厂统一兜底
        codec = JSONCodec()
    if scheme == "memory":
        transport = MemoryTransport(clock=clock, idempotency_ttl=idempotency_ttl)
        if queue_weights:
            transport.set_queue_weights(dict(queue_weights))
        return transport
    if scheme == "sqlite":
        path = url[len("sqlite://") :]
        if path.startswith("/"):          # sqlite:///rel.db -> rel.db；sqlite:////abs.db -> /abs.db
            path = path[1:]
        if not path:
            raise ConfigError("sqlite:// 需要文件路径，例如 sqlite:///./taskmq.db")
        return SqliteTransport(
            path,
            codec=codec,
            registry=registry,
            clock=clock,
            idempotency_ttl=idempotency_ttl,
            max_message_bytes=max_message_bytes,
            queue_weights=queue_weights,
        )
    if scheme in ("postgres", "postgresql"):
        libpq_url, prefix, params = parse_postgres_url(url)
        return PostgresTransport(
            libpq_url,
            codec=codec,
            registry=registry,
            prefix=prefix,
            clock=clock,
            connect_timeout=float(params.get("connect_timeout", 10.0)),
            idempotency_ttl=idempotency_ttl,
            max_message_bytes=max_message_bytes,
        )
    if scheme == "redis":
        base, params = _parse_query(url)
        redis_transport = RedisTransport(
            base,
            codec=codec,
            registry=registry,
            prefix=params.get("prefix") or "taskmq:",
            clock=clock,
            idempotency_ttl=idempotency_ttl,
            max_message_bytes=max_message_bytes,
            lua=_parse_bool(params.get("lua")),
            cluster=bool(_parse_bool(params.get("cluster"))),
        )
        if queue_weights:
            redis_transport.set_queue_weights(dict(queue_weights))
        return redis_transport
    if scheme == "amqp":
        from .amqp import AmqpTransport

        base, params = _parse_query(url)
        state_url = params.get("state", "")
        if not state_url:
            raise ConfigError(
                "amqp:// 需要状态侧车：AMQP 是消息代理，没有 job 状态/DLQ 索引/命名租约的存储。"
                "请写成 amqp://user:pass@host:5672/vhost?state=sqlite:///./taskmq.db"
                "（或 state=postgresql://… / state=redis://…）"
            )
        state = build_transport(
            state_url,
            codec=codec,
            registry=registry,
            idempotency_ttl=idempotency_ttl,
            max_message_bytes=max_message_bytes,
            clock=clock,
        )
        return AmqpTransport(
            base,
            codec=codec,
            registry=registry,
            state=state,
            prefix=params.get("prefix") or "taskmq.",
            clock=clock,
            idempotency_ttl=idempotency_ttl,
            max_message_bytes=max_message_bytes,
        )
    raise ConfigError(f"未知的 transport scheme：{scheme or url!r}")
