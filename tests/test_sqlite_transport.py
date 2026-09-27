"""SqliteTransport：不变量、优先级/让位、持久化、多连接并发（与 memory 同语义）。"""
from __future__ import annotations

import threading

import pytest

from taskmq.errors import LeaseLost
from taskmq.protocol import JSONCodec
from taskmq.transport.sqlite import SqliteTransport


def _transport(path, **kwargs) -> SqliteTransport:
    return SqliteTransport(str(path), codec=JSONCodec(), **kwargs)


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_claim_order_is_priority_then_fifo(tmp_path):
    transport = _transport(tmp_path / "t.db")
    for task, priority in (("low1", 0), ("high", 5), ("mid", 1), ("low2", 0)):
        transport.enqueue(__import__("taskmq").Envelope(task=task, priority=priority), queue="q")
    granted = transport.reserve(["q"], worker_id="w", lease=30, limit=10)
    assert [d.envelope.task for d in granted] == ["high", "mid", "low1", "low2"]
    assert [d.priority for d in granted] == [5, 1, 0, 0]
    assert [d.deliveries for d in granted] == [1, 1, 1, 1]


def test_band_fairness_is_weighted_round_robin(tmp_path):
    transport = _transport(tmp_path / "t.db")
    transport.set_queue_weights({"a": 1, "b": 2})
    for index in range(4):
        transport.enqueue(__import__("taskmq").Envelope(task=f"a{index}"), queue="a")
        transport.enqueue(__import__("taskmq").Envelope(task=f"b{index}"), queue="b")
    tasks = [d.envelope.task for d in transport.reserve(["a", "b"], worker_id="w", lease=30, limit=6)]
    assert tasks == ["a0", "b0", "b1", "a1", "b2", "b3"]


def test_lease_reap_and_late_ack(tmp_path):
    clock = FakeClock()
    transport = _transport(tmp_path / "t.db", clock=clock)
    transport.enqueue(__import__("taskmq").Envelope(task="t"), queue="q")

    first = transport.reserve(["q"], worker_id="w1", lease=10, limit=1)[0]
    assert transport.reserve(["q"], worker_id="w2", lease=10, limit=1) == []

    clock.advance(11)
    assert transport.reap_expired_leases() == 1
    second = transport.reserve(["q"], worker_id="w2", lease=10, limit=1)[0]
    assert second.deliveries == 2

    with pytest.raises(LeaseLost):
        transport.ack(first)
    transport.ack(second)
    transport.ack(second)                                   # 幂等
    assert transport.queue_stats(["q"])[0].pending == 0


def test_yield_counts_separately_and_respects_max_yields(tmp_path):
    clock = FakeClock()
    transport = _transport(tmp_path / "t.db", clock=clock)
    envelope = __import__("taskmq").Envelope
    transport.enqueue(envelope(task="low"), queue="q")
    delivery = transport.reserve(["q"], worker_id="w1", lease=30, limit=1)[0]

    transport.enqueue(envelope(task="urgent", priority=9), queue="q")
    assert transport.peek_max_priority(["q"]) == 9
    assert transport.yield_reservation(delivery, delay=0.05, max_yields=1) is True
    assert transport.peek_max_priority(["q"]) == 9          # low 不可见（防抖）
    assert [d.envelope.task for d in transport.reserve(["q"], worker_id="w2", lease=30, limit=1)] == [
        "urgent"
    ]

    clock.advance(0.05)
    again = transport.reserve(["q"], worker_id="w2", lease=30, limit=1)[0]
    assert again.envelope.task == "low"
    assert again.deliveries == 2
    assert again.yields == 1
    assert transport.yield_reservation(again, delay=0.0, max_yields=1) is False


def test_expires_and_idempotency(tmp_path):
    envelope = __import__("taskmq").Envelope
    clock = FakeClock()
    transport = _transport(tmp_path / "t.db", clock=clock)
    job_id = transport.enqueue(envelope(task="t", expires_at=clock.now + 5), queue="q")
    first = transport.enqueue(envelope(task="t", key="k"), queue="q")
    second = transport.enqueue(envelope(task="t2", key="k"), queue="q")
    assert first == second

    clock.advance(6)
    assert transport.reap_expired_jobs() == 1
    record = transport.get_state(job_id)
    assert record is not None and record.state == "EXPIRED"


def test_reap_expired_jobs_does_not_touch_other_jobs(tmp_path):
    """回归：一条消息过期不能把别的 QUEUED/RETRYING job 也标成 EXPIRED。

    之前 `_expire_overdue_locked` 的 UPDATE jobs 少了 job_id 过滤，于是延迟/退避中的任务
    （`delay=3600` 这种）会被误判成"过期"，`handle.state` 直接变 EXPIRED。
    """
    envelope = __import__("taskmq").Envelope
    clock = FakeClock()
    transport = _transport(tmp_path / "t.db", clock=clock)

    expiring = transport.enqueue(envelope(task="expiring", expires_at=clock.now + 5), queue="q")
    delayed = transport.enqueue(envelope(task="delayed"), queue="later", delay=3600)
    queued = transport.enqueue(envelope(task="queued"), queue="q")

    clock.advance(6)
    assert transport.reap_expired_jobs() == 1

    assert transport.get_state(expiring).state == "EXPIRED"
    for job_id, task in ((delayed, "delayed"), (queued, "queued")):
        record = transport.get_state(job_id)
        assert record is not None and record.state == "QUEUED", f"{task} 被误判：{record.state}"


def test_dlq_and_replay_with_priority_override(tmp_path):
    envelope = __import__("taskmq").Envelope
    transport = _transport(tmp_path / "t.db")
    transport.enqueue(envelope(task="t", priority=0), queue="q")
    delivery = transport.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
    transport.dead_letter(delivery, "boom")

    dead = transport.dead_letters()
    assert len(dead) == 1 and dead[0].reason == "boom"
    assert transport.replay_dead(dead[0].message_id, priority=7) is True
    assert transport.priority_stats(["q"]) == {7: 1}


def test_state_round_trip_with_result_and_meta(tmp_path):
    transport = _transport(tmp_path / "t.db")
    transport.set_state("job-1", "RUNNING", task="t", attempt=1, stage="half")
    transport.set_state("job-1", "SUCCEEDED", result={"answer": [1, 2, 3]}, percent=100)
    record = transport.get_state("job-1")
    assert record is not None
    assert record.result == {"answer": [1, 2, 3]}
    assert record.has_result is True
    assert record.meta["stage"] == "half" and record.meta["percent"] == 100


def test_data_survives_reopen(tmp_path):
    envelope_cls = __import__("taskmq").Envelope
    path = tmp_path / "persist.db"
    first = _transport(path)
    job_id = first.enqueue(envelope_cls(task="survive", args=(1, 2), priority=3), queue="q")
    first.set_state(job_id, "QUEUED")
    first.close()

    second = _transport(path)
    delivery = second.reserve(["q"], worker_id="w", lease=5, limit=1)[0]
    assert delivery.envelope.task == "survive"
    assert delivery.envelope.args == (1, 2)
    assert delivery.priority == 3
    record = second.get_state(job_id)
    assert record is not None and record.state == "RUNNING"
    second.close()


def test_two_connections_never_claim_the_same_message(tmp_path):
    envelope_cls = __import__("taskmq").Envelope
    path = tmp_path / "concurrent.db"
    writer = _transport(path)
    for index in range(200):
        writer.enqueue(envelope_cls(task=f"t{index}"), queue="q")

    claimed: list[int] = []
    lock = threading.Lock()

    def drain(worker_id: str) -> None:
        transport = _transport(path)
        try:
            while True:
                batch = transport.reserve(["q"], worker_id=worker_id, lease=30, limit=10)
                if not batch:
                    return
                with lock:
                    claimed.extend(d.message_id for d in batch)
                for delivery in batch:
                    transport.ack(delivery)
        finally:
            transport.close()

    threads = [threading.Thread(target=drain, args=(f"w{i}",)) for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(claimed) == 200
    assert len(set(claimed)) == 200                          # 同一条消息只会被一个人拿到
    assert writer.queue_stats(["q"])[0].pending == 0
    writer.close()
