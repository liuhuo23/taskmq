"""transport 不变量 + 优先级/让位语义（决策 §20-9）。"""
from __future__ import annotations

import pytest

from taskmq.errors import ConfigError, LeaseLost
from taskmq.priority import validate_priority
from taskmq.protocol import Envelope
from taskmq.transport.memory import MemoryTransport


class FakeClock:
    def __init__(self, start: float = 100.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _envelope(task: str, priority: int = 0, **kwargs) -> Envelope:
    return Envelope(task=task, priority=priority, **kwargs)


def test_priority_range_validation():
    assert validate_priority(-9) == -9
    assert validate_priority(9) == 9
    for bad in (10, -10, "5", 1.5, True, None):
        with pytest.raises(ConfigError):
            validate_priority(bad)


def test_claim_order_is_priority_then_fifo():
    transport = MemoryTransport()
    for task, priority in (("low1", 0), ("high", 5), ("mid", 1), ("low2", 0)):
        transport.enqueue(_envelope(task, priority), queue="q")
    granted = transport.reserve(["q"], worker_id="w", lease=30, limit=10)
    assert [d.envelope.task for d in granted] == ["high", "mid", "low1", "low2"]
    assert [d.priority for d in granted] == [5, 1, 0, 0]
    assert [d.deliveries for d in granted] == [1, 1, 1, 1]


def test_band_fairness_is_weighted_round_robin():
    transport = MemoryTransport()
    transport.set_queue_weights({"a": 1, "b": 2})
    for index in range(4):
        transport.enqueue(_envelope(f"a{index}"), queue="a")
        transport.enqueue(_envelope(f"b{index}"), queue="b")
    tasks = [d.envelope.task for d in transport.reserve(["a", "b"], worker_id="w", lease=30, limit=6)]
    assert tasks == ["a0", "b0", "b1", "a1", "b2", "b3"]     # b 的权重是 a 的两倍


def test_reserve_is_exclusive_and_reap_redelivers():
    clock = FakeClock()
    transport = MemoryTransport(clock=clock)
    transport.enqueue(_envelope("t"), queue="q")

    first = transport.reserve(["q"], worker_id="w1", lease=10, limit=1)[0]
    assert transport.reserve(["q"], worker_id="w2", lease=10, limit=1) == []

    clock.advance(11)
    assert transport.reap_expired_leases() == 1
    second = transport.reserve(["q"], worker_id="w2", lease=10, limit=1)[0]
    assert second.deliveries == 2

    with pytest.raises(LeaseLost):
        transport.ack(first)                                # 迟到 ack 不能误删别人的投递
    transport.ack(second)
    transport.ack(second)                                   # 幂等
    assert transport.queue_stats(["q"])[0].pending == 0


def test_yield_reservation_counts_separately_from_deliveries():
    clock = FakeClock()
    transport = MemoryTransport(clock=clock)
    transport.enqueue(_envelope("low", 0), queue="q")
    delivery = transport.reserve(["q"], worker_id="w1", lease=30, limit=1)[0]
    assert transport.peek_max_priority(["q"]) is None       # 已被 reserve，没有可见消息

    transport.enqueue(_envelope("urgent", 9), queue="q")
    assert transport.peek_max_priority(["q"]) == 9

    assert transport.yield_reservation(delivery, delay=0.05, max_yields=1) is True
    assert transport.peek_max_priority(["q"]) == 9          # low 让位后不可见（防抖）

    granted = transport.reserve(["q"], worker_id="w2", lease=30, limit=1)
    assert [d.envelope.task for d in granted] == ["urgent"]

    clock.advance(0.05)
    again = transport.reserve(["q"], worker_id="w2", lease=30, limit=1)[0]
    assert again.envelope.task == "low"
    assert again.deliveries == 2                            # 第二次投递
    assert again.yields == 1                                # 让位次数独立计数（P13）
    assert transport.yield_reservation(again, delay=0.0, max_yields=1) is False  # 到顶后不再让位


def test_expired_before_execution_is_marked_expired():
    clock = FakeClock()
    transport = MemoryTransport(clock=clock)
    job_id = transport.enqueue(_envelope("t", expires_at=clock.now + 5), queue="q")

    clock.advance(6)
    assert transport.reap_expired_jobs() == 1
    assert transport.reserve(["q"], worker_id="w", lease=5, limit=1) == []
    record = transport.get_state(job_id)
    assert record is not None and record.state == "EXPIRED"


def test_idempotency_key_dedupes_within_window():
    transport = MemoryTransport()
    first = transport.enqueue(_envelope("t", key="k"), queue="q")
    second = transport.enqueue(_envelope("other", key="k"), queue="q")
    assert first == second
    assert transport.queue_stats(["q"])[0].pending == 1
    assert transport.next_visible_at(["q"]) is not None
