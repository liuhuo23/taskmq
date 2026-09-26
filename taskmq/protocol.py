"""消息协议 v1：Envelope、ULID、编解码与大小限制。

设计约束（详见 docs/design/protocol.md）：
- `v` 是协议版本（整数）。遇到不认识的大版本直接拒绝并告警，不猜测。
- `args`/`kwargs` 只允许 JSON 可编码的值，以及显式注册的自定义类型；未知类型在**编码期**报错。
- 时间统一为 UTC 时间戳（float 秒）；展示层再转时区。
- 消息体默认上限 256 KiB；超限在**生产者侧**抛 `MessageTooLarge`。
- **优先级**：`priority` 为整数，越大越先被 claim；transport 的排序键是
  `(priority DESC, id ASC)`，同优先级 FIFO。默认值来自 Config/Task，提交时可覆盖。
"""
from __future__ import annotations

import dataclasses
import json
import math
import os
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any

from .errors import (
    DecodeError,
    EncodeError,
    MessageTooLarge,
    ProtocolError,
    UnsupportedCodec,
)

PROTOCOL_VERSION = 1
MAX_PROTOCOL_VERSION = PROTOCOL_VERSION
DEFAULT_MAX_MESSAGE_BYTES = 256 * 1024

ACK_ON_RECEIPT = "on_receipt"
ACK_ON_SUCCESS = "on_success"
ACK_ON_COMPLETION = "on_completion"
ACK_STRATEGIES = (ACK_ON_RECEIPT, ACK_ON_SUCCESS, ACK_ON_COMPLETION)

_TAG_KEY = "__taskmq__"

__all__ = [
    "PROTOCOL_VERSION",
    "MAX_PROTOCOL_VERSION",
    "DEFAULT_MAX_MESSAGE_BYTES",
    "ACK_ON_RECEIPT",
    "ACK_ON_SUCCESS",
    "ACK_ON_COMPLETION",
    "ACK_STRATEGIES",
    "Envelope",
    "new_ulid",
    "ulid_timestamp",
    "TypeCodec",
    "CodecRegistry",
    "Codec",
    "JSONCodec",
    "MsgspecCodec",
    "get_codec",
    "encode_payload",
    "decode_payload",
    "encode_value",
    "decode_value",
]


# --------------------------------------------------------------------------- ULID
_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_ulid_lock = threading.Lock()
_ulid_last_ms = -1
_ulid_last_rand = 0


def _encode_ulid(ms: int, rand: int) -> str:
    out = ["0"] * 26
    for i in range(25, 9, -1):
        out[i] = _CROCKFORD[rand & 0x1F]
        rand >>= 5
    for i in range(9, -1, -1):
        out[i] = _CROCKFORD[ms & 0x1F]
        ms >>= 5
    return "".join(out)


def new_ulid(now: float | None = None) -> str:
    """生成按时间可排序的 ULID（26 字符 Crockford Base32），同毫秒内单调递增。"""
    global _ulid_last_ms, _ulid_last_rand
    ms = int((time.time() if now is None else now) * 1000)
    with _ulid_lock:
        if ms > _ulid_last_ms:
            _ulid_last_ms = ms
            _ulid_last_rand = int.from_bytes(os.urandom(10), "big")
        else:
            ms = _ulid_last_ms
            _ulid_last_rand += 1
        rand = _ulid_last_rand
    return _encode_ulid(ms, rand)


def ulid_timestamp(ulid: str) -> float:
    """从 ULID 解出创建时间（UTC 时间戳，秒）。"""
    if not isinstance(ulid, str) or len(ulid) != 26:
        raise ValueError(f"不是合法的 ULID: {ulid!r}")
    ms = 0
    for ch in ulid[:10]:
        idx = _CROCKFORD.find(ch)
        if idx < 0:
            raise ValueError(f"不是合法的 ULID: {ulid!r}")
        ms = (ms << 5) | idx
    return ms / 1000.0


# ----------------------------------------------------------------------- Envelope
@dataclasses.dataclass(frozen=True, slots=True)
class Envelope:
    """transport 中流转的消息体。

    `attempt` 是**当前投递的尝试序号**（从 1 开始，重试时 +1）；
    transport 另外维护单调递增的 `deliveries`（累计投递次数，用于毒丸保护）。
    """

    task: str
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    queue: str = "default"
    priority: int = 0
    id: str = dataclasses.field(default_factory=new_ulid)
    v: int = PROTOCOL_VERSION
    eta: float | None = None
    expires_at: float | None = None
    deadline: float | None = None
    attempt: int = 1
    max_attempts: int = 5
    ack: str = ACK_ON_SUCCESS
    key: str | None = None
    concurrency_key: str | None = None
    trace: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    enqueued_at: float = dataclasses.field(default_factory=time.time)
    headers: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    # ------------------------------------------------------------- 序列化
    def to_dict(self) -> dict[str, Any]:
        return {
            "v": self.v,
            "id": self.id,
            "task": self.task,
            "args": list(self.args),
            "kwargs": dict(self.kwargs),
            "queue": self.queue,
            "priority": self.priority,
            "eta": self.eta,
            "expires_at": self.expires_at,
            "deadline": self.deadline,
            "attempt": self.attempt,
            "max_attempts": self.max_attempts,
            "ack": self.ack,
            "key": self.key,
            "concurrency_key": self.concurrency_key,
            "trace": dict(self.trace),
            "enqueued_at": self.enqueued_at,
            "headers": dict(self.headers),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Envelope:
        if not isinstance(data, Mapping):
            raise DecodeError(f"envelope 必须是对象，收到 {type(data).__name__}")

        task = data.get("task")
        if not isinstance(task, str) or not task:
            raise DecodeError("envelope.task 必须是非空字符串")

        job_id = data.get("id")
        if not isinstance(job_id, str) or not job_id:
            raise DecodeError("envelope.id 必须是非空字符串")

        raw_args = data.get("args") or []
        if not isinstance(raw_args, (list, tuple)):
            raise DecodeError("envelope.args 必须是数组")
        raw_kwargs = data.get("kwargs") or {}
        if not isinstance(raw_kwargs, Mapping):
            raise DecodeError("envelope.kwargs 必须是对象")

        priority = data.get("priority", 0)
        if not isinstance(priority, int) or isinstance(priority, bool):
            raise DecodeError("envelope.priority 必须是整数")

        ack = data.get("ack", ACK_ON_SUCCESS)
        if ack not in ACK_STRATEGIES:
            raise DecodeError(f"envelope.ack 非法：{ack!r}，可选 {ACK_STRATEGIES}")

        def _int(name: str, default: int, minimum: int = 0) -> int:
            value = data.get(name, default)
            if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
                raise DecodeError(f"envelope.{name} 必须是不小于 {minimum} 的整数")
            return value

        def _time(name: str) -> float | None:
            value = data.get(name)
            if value is None:
                return None
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise DecodeError(f"envelope.{name} 必须是时间戳或 null")
            return float(value)

        key = data.get("key")
        if key is not None and not isinstance(key, str):
            raise DecodeError("envelope.key 必须是字符串或 null")
        concurrency_key = data.get("concurrency_key")
        if concurrency_key is not None and not isinstance(concurrency_key, str):
            raise DecodeError("envelope.concurrency_key 必须是字符串或 null")

        trace = data.get("trace") or {}
        headers = data.get("headers") or {}
        if not isinstance(trace, Mapping) or not isinstance(headers, Mapping):
            raise DecodeError("envelope.trace / envelope.headers 必须是对象")

        return cls(
            task=task,
            args=tuple(raw_args),
            kwargs=dict(raw_kwargs),
            queue=str(data.get("queue") or "default"),
            priority=priority,
            id=job_id,
            v=_int("v", PROTOCOL_VERSION, minimum=1),
            eta=_time("eta"),
            expires_at=_time("expires_at"),
            deadline=_time("deadline"),
            attempt=_int("attempt", 1, minimum=1),
            max_attempts=_int("max_attempts", 5, minimum=1),
            ack=ack,
            key=key,
            concurrency_key=concurrency_key,
            trace=dict(trace),
            enqueued_at=_time("enqueued_at") or time.time(),
            headers=dict(headers),
        )

    # ------------------------------------------------------------- 便捷方法
    def next_attempt(self, **changes: Any) -> Envelope:
        """返回 `attempt + 1` 的新 envelope（重试用）；可覆盖任意字段（如 priority）。"""
        defaults: dict[str, Any] = {"attempt": self.attempt + 1, "enqueued_at": time.time()}
        defaults.update(changes)
        return dataclasses.replace(self, **defaults)

    def with_routing(self, queue: str, priority: int | None = None) -> Envelope:
        changes: dict[str, Any] = {"queue": queue}
        if priority is not None:
            changes["priority"] = priority
        return dataclasses.replace(self, **changes)


# ------------------------------------------------------------------ 自定义类型
@dataclasses.dataclass(frozen=True, slots=True)
class TypeCodec:
    """自定义类型的编解码对。`encode` 必须返回 JSON 可编码的值。"""

    tag: str
    type: type
    encode: Callable[[Any], Any]
    decode: Callable[[Any], Any]


class CodecRegistry:
    """自定义类型注册表（`app.register_codec()` 的底层实现）。"""

    def __init__(self) -> None:
        self._by_type: dict[type, TypeCodec] = {}
        self._by_tag: dict[str, TypeCodec] = {}

    def register(
        self,
        type_: type,
        encode: Callable[[Any], Any],
        decode: Callable[[Any], Any],
        *,
        tag: str | None = None,
    ) -> TypeCodec:
        if not isinstance(type_, type):
            raise ProtocolError(f"register_codec 的第一个参数必须是类型，收到 {type_!r}")
        resolved = tag or f"{type_.__module__}.{type_.__qualname__}"
        existing = self._by_tag.get(resolved)
        if existing is not None and existing.type is not type_:
            raise ProtocolError(f"tag {resolved!r} 已被 {existing.type!r} 占用")
        codec = TypeCodec(tag=resolved, type=type_, encode=encode, decode=decode)
        self._by_type[type_] = codec
        self._by_tag[resolved] = codec
        return codec

    def get_by_type(self, type_: type) -> TypeCodec | None:
        codec = self._by_type.get(type_)
        if codec is not None:
            return codec
        for base in type_.__mro__[1:]:
            codec = self._by_type.get(base)
            if codec is not None:
                return codec
        return None

    def get_by_tag(self, tag: str) -> TypeCodec | None:
        return self._by_tag.get(tag)

    def __len__(self) -> int:
        return len(self._by_type)


def _encode_value(value: Any, registry: CodecRegistry, path: str) -> Any:
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise EncodeError(f"{path}: 非有限浮点数不可入队（{value!r}）")
        return value
    if isinstance(value, (list, tuple)):
        return [_encode_value(item, registry, f"{path}[{i}]") for i, item in enumerate(value)]
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise EncodeError(f"{path}: 对象的 key 必须是 str，收到 {type(key).__name__}")
            out[key] = _encode_value(item, registry, f"{path}.{key}")
        return out
    codec = registry.get_by_type(type(value))
    if codec is None:
        raise EncodeError(
            f"{path}: 类型 {type(value).__module__}.{type(value).__qualname__} 未注册；"
            "请先用 app.register_codec(Type, encode=..., decode=...) 注册"
        )
    return {_TAG_KEY: codec.tag, "v": _encode_value(codec.encode(value), registry, path)}


def _decode_value(value: Any, registry: CodecRegistry, path: str) -> Any:
    if isinstance(value, list):
        return [_decode_value(item, registry, f"{path}[{i}]") for i, item in enumerate(value)]
    if isinstance(value, dict):
        if set(value.keys()) == {_TAG_KEY, "v"}:
            tag = value[_TAG_KEY]
            codec = registry.get_by_tag(tag) if isinstance(tag, str) else None
            if codec is None:
                raise DecodeError(f"{path}: 未知的自定义类型 tag {tag!r}（未在本进程注册）")
            return codec.decode(_decode_value(value["v"], registry, path))
        return {key: _decode_value(item, registry, f"{path}.{key}") for key, item in value.items()}
    return value


def encode_value(value: Any, registry: CodecRegistry | None = None, *, path: str = "value") -> Any:
    """任意值 -> 纯 JSON 结构（自定义类型装箱）；未注册类型在编码期抛 `EncodeError`。

    transport 用它持久化 job 结果与 meta（跨进程、跨版本都只走白名单类型）。
    """
    return _encode_value(value, registry or CodecRegistry(), path)


def decode_value(value: Any, registry: CodecRegistry | None = None, *, path: str = "value") -> Any:
    """纯 JSON 结构 -> 任意值（`encode_value` 的逆操作）。"""
    return _decode_value(value, registry or CodecRegistry(), path)


def encode_payload(env: Envelope, registry: CodecRegistry | None = None) -> dict[str, Any]:
    """Envelope -> 纯 JSON 结构（自定义类型已装箱）。"""
    payload = _encode_value(env.to_dict(), registry or CodecRegistry(), "envelope")
    assert isinstance(payload, dict)
    return payload


def decode_payload(payload: Any, registry: CodecRegistry | None = None) -> Envelope:
    """纯 JSON 结构 -> Envelope，含协议版本检查与自定义类型还原。"""
    if not isinstance(payload, dict):
        raise DecodeError(f"消息体必须是 JSON 对象，收到 {type(payload).__name__}")
    version = payload.get("v")
    if not isinstance(version, int) or isinstance(version, bool):
        raise DecodeError("消息体缺少整数协议版本字段 v")
    if version > MAX_PROTOCOL_VERSION:
        raise DecodeError(
            f"不支持的协议版本 v={version}（本进程最高支持 v={MAX_PROTOCOL_VERSION}），拒绝猜测"
        )
    if version < 1:
        raise DecodeError(f"协议版本非法：v={version}")
    plain = _decode_value(payload, registry or CodecRegistry(), "envelope")
    return Envelope.from_dict(plain)


# ----------------------------------------------------------------------- Codec
def _check_size(data: bytes, max_bytes: int | None) -> bytes:
    if max_bytes is not None and len(data) > max_bytes:
        raise MessageTooLarge(
            f"编码后 {len(data)} 字节，超过 max_message_bytes={max_bytes}；请在生产者侧裁剪载荷"
        )
    return data


class Codec:
    """编解码器基类：`encode` 产出 bytes，`decode` 还原 Envelope。"""

    name = "base"

    def __init__(self, registry: CodecRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CodecRegistry()

    def encode(self, env: Envelope, *, max_bytes: int | None = DEFAULT_MAX_MESSAGE_BYTES) -> bytes:
        raise NotImplementedError

    def decode(self, data: bytes | str) -> Envelope:
        raise NotImplementedError


class JSONCodec(Codec):
    """标准库 json：零第三方依赖，兼容性最好。"""

    name = "json"

    def encode(self, env: Envelope, *, max_bytes: int | None = DEFAULT_MAX_MESSAGE_BYTES) -> bytes:
        payload = encode_payload(env, self.registry)
        try:
            data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise EncodeError(f"json 编码失败：{exc}") from exc
        return _check_size(data.encode("utf-8"), max_bytes)

    def decode(self, data: bytes | str) -> Envelope:
        if isinstance(data, bytes):
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise DecodeError(f"消息体不是合法 UTF-8：{exc}") from exc
        else:
            text = data
        try:
            payload = json.loads(text)
        except ValueError as exc:
            raise DecodeError(f"json 解码失败：{exc}") from exc
        return decode_payload(payload, self.registry)


class MsgspecCodec(Codec):
    """msgspec.json：默认编解码器（更快、更严格）。"""

    name = "msgspec"

    def __init__(self, registry: CodecRegistry | None = None) -> None:
        super().__init__(registry)
        try:
            import msgspec  # noqa: PLC0415
        except ImportError as exc:  # pragma: no cover - 取决于环境
            raise UnsupportedCodec(
                "serializer='msgspec' 需要安装 msgspec（pip install msgspec）；"
                "或显式使用 serializer='json' 走纯标准库路径"
            ) from exc
        self._msgspec = msgspec

    def encode(self, env: Envelope, *, max_bytes: int | None = DEFAULT_MAX_MESSAGE_BYTES) -> bytes:
        payload = encode_payload(env, self.registry)
        try:
            data = self._msgspec.json.encode(payload)
        except Exception as exc:  # msgspec 的异常类型随版本变化
            raise EncodeError(f"msgspec 编码失败：{exc}") from exc
        return _check_size(data, max_bytes)

    def decode(self, data: bytes | str) -> Envelope:
        if isinstance(data, str):
            data = data.encode("utf-8")
        try:
            payload = self._msgspec.json.decode(data)
        except Exception as exc:
            raise DecodeError(f"msgspec 解码失败：{exc}") from exc
        return decode_payload(payload, self.registry)


_CODEC_FACTORIES: dict[str, type[Codec]] = {"json": JSONCodec, "msgspec": MsgspecCodec}


def get_codec(name: str, registry: CodecRegistry | None = None) -> Codec:
    """按名字取编解码器；名字未知或依赖缺失时抛 `UnsupportedCodec`。"""
    try:
        factory = _CODEC_FACTORIES[name]
    except KeyError:
        raise UnsupportedCodec(
            f"未知的序列化器 {name!r}，可选：{', '.join(sorted(_CODEC_FACTORIES))}"
        ) from None
    return factory(registry)
