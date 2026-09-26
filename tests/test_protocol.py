"""协议层测试：Envelope 往返、协议版本、大小限制、自定义类型。"""
from __future__ import annotations

import json
import time

import pytest

from taskmq.errors import DecodeError, EncodeError, MessageTooLarge
from taskmq.protocol import (
    PROTOCOL_VERSION,
    CodecRegistry,
    Envelope,
    JSONCodec,
    MsgspecCodec,
    new_ulid,
    ulid_timestamp,
)


def test_ulid_is_sortable_and_monotonic():
    first = new_ulid()
    second = new_ulid()
    assert len(first) == 26
    assert first <= second                                  # 同毫秒内单调
    assert abs(ulid_timestamp(first) - time.time()) < 5     # 近似当前时间


def test_envelope_roundtrip_both_codecs():
    env = Envelope(
        task="app.add",
        args=(1, 2),
        kwargs={"k": [1, 2, 3]},
        queue="math",
        priority=5,
        key="k1",
        headers={"a": None},
    )
    for codec in (JSONCodec(), MsgspecCodec()):
        restored = codec.decode(codec.encode(env, max_bytes=10_000))
        assert restored == env
        assert restored.args == (1, 2)                      # tuple 还原
        assert restored.kwargs == {"k": [1, 2, 3]}
        assert restored.priority == 5


def test_unknown_protocol_version_rejected():
    codec = JSONCodec()
    payload = json.loads(codec.encode(Envelope(task="t")))
    payload["v"] = PROTOCOL_VERSION + 1
    with pytest.raises(DecodeError):
        codec.decode(json.dumps(payload).encode())


def test_unknown_type_fails_at_encode_time():
    with pytest.raises(EncodeError):
        JSONCodec().encode(Envelope(task="t", args=(object(),)))


def test_custom_codec_roundtrip():
    registry = CodecRegistry()

    class Money:
        def __init__(self, cents: int) -> None:
            self.cents = cents

        def __eq__(self, other: object) -> bool:
            return isinstance(other, Money) and other.cents == self.cents

    registry.register(Money, encode=lambda m: {"cents": m.cents}, decode=lambda d: Money(d["cents"]))
    codec = JSONCodec(registry)
    restored = codec.decode(codec.encode(Envelope(task="t", args=(Money(42),))))
    assert restored.args == (Money(42),)


def test_message_too_large_is_rejected_at_producer_side():
    with pytest.raises(MessageTooLarge):
        JSONCodec().encode(Envelope(task="t", args=("x" * 5000,)), max_bytes=1024)


def test_non_finite_float_rejected():
    with pytest.raises(EncodeError):
        JSONCodec().encode(Envelope(task="t", args=(float("nan"),)))
