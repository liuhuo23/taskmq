"""用同一套一致性套件跑三家内建 transport（契约必须是"我们自己也在跑的"）。

见 docs/design/plugins.md §6 / D5：第三方后端 `transport_conformance(factory)` 一行自证，
内建实现跑同一套件 —— 避免"给别人的契约"和"自己跑的契约"脱节。
"""
from __future__ import annotations

import itertools
import os
import uuid

import pytest

from taskmq.protocol import JSONCodec
from taskmq.redis_client import RedisClient
from taskmq.testing import CONFORMANCE_SCENARIOS, transport_conformance
from taskmq.transport.memory import MemoryTransport
from taskmq.transport.redis import RedisTransport
from taskmq.transport.sqlite import SqliteTransport

REDIS_URL = os.environ.get("TASKMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379/15")


def _redis_available() -> bool:
    try:
        return RedisClient(REDIS_URL, timeout=1.5).ping()
    except Exception:
        return False


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_builtin_transport_conformance(backend, tmp_path):
    counter = itertools.count()

    def factory():
        if backend == "memory":
            return MemoryTransport()
        return SqliteTransport(str(tmp_path / f"conf{next(counter)}.db"), codec=JSONCodec())

    executed = transport_conformance(factory)
    assert executed == [name for name in CONFORMANCE_SCENARIOS if name != "capability_honesty"] or (
        "capability_honesty" in executed
    )
    assert len(executed) == len(CONFORMANCE_SCENARIOS)


@pytest.mark.skipif(not _redis_available(), reason=f"没有可用的 Redis（{REDIS_URL}）")
@pytest.mark.parametrize("lua", [True, False], ids=["lua", "no-lua"])
def test_redis_transport_conformance(lua):
    def factory():
        return RedisTransport(
            REDIS_URL, codec=JSONCodec(), prefix=f"taskmq:conf:{uuid.uuid4().hex}:", lua=lua
        )

    def cleanup(transport):
        transport.flush_prefix()
        transport.close()

    executed = transport_conformance(factory, cleanup=cleanup)
    # redis 支持跨队列全局严格优先 → global_priority 场景要真的跑（不是被声明跳过）
    assert {"priority_fifo", "global_priority", "atomic_claim", "lease_recovery", "dlq_replay"} <= set(
        executed
    )
    # Cluster 是显式开关（?cluster=1）：单机模式一行声明都没有；降级挂在 cluster_limitations
    assert "cluster" not in RedisTransport.limitations
    assert "global_priority" in RedisTransport.cluster_limitations


def test_conformance_rejects_unknown_scenarios():
    with pytest.raises(ValueError, match="未知的一致性场景"):
        transport_conformance(lambda: MemoryTransport(), scenarios=["nope"])


def test_conformance_honours_declared_limitations():
    class Limited(MemoryTransport):
        limitations = {"atomic_claim": "本后端不保证原子 claim"}

    executed = transport_conformance(lambda: Limited())
    assert "atomic_claim" not in executed
    assert "priority_fifo" in executed


def test_conformance_detects_dishonest_capabilities():
    class Dishonest(MemoryTransport):
        supports_leases = False          # 声明不支持……

        def acquire_lease(self, name: str, owner: str, ttl: float) -> bool:
            return True                  # ……却静默成功（谎报能力）

    with pytest.raises(AssertionError, match="capability_honesty"):
        transport_conformance(lambda: Dishonest(), scenarios=["capability_honesty"])


def test_conformance_can_use_a_fake_clock():
    class Clock:
        now = 1_800_000_000.0

        def __call__(self) -> float:
            return self.now

    clock = Clock()
    executed = transport_conformance(
        lambda: MemoryTransport(clock=clock),
        now=clock,
        advance=lambda seconds: setattr(clock, "now", clock.now + seconds),
        scenarios=["lease_recovery", "visibility", "expires"],
    )
    assert executed == ["lease_recovery", "visibility", "expires"]
