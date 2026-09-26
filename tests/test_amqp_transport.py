"""AMQP（RabbitMQ）transport：投递走 broker、状态走侧车 + 一致性套件 + DAG。

- 没装 pika 或连不上 broker → 整个文件 skip（`TASKMQ_TEST_AMQP_URL` 可覆盖）。
- 每个用例用独立队列前缀 + 独立 sqlite 侧车，跑完删掉自己的队列（绝不动别人的 vhost）。
- 声明了 `global_priority` 降级（AMQP 是 per-queue 有序）→ 该场景按声明跳过。
"""
from __future__ import annotations

import importlib.util
import os
import socket
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

import pytest

from taskmq import App, Config, Envelope
from taskmq.testing import CONFORMANCE_SCENARIOS, run_until_idle, transport_conformance
from taskmq.transport.base import JobState
from taskmq.transport.factory import build_transport

BROKER = os.environ.get("TASKMQ_TEST_AMQP_BROKER", "amqp://taskmq:taskmq@127.0.0.1:55672/%2F")


def _available() -> bool:
    if importlib.util.find_spec("pika") is None:
        return False
    parsed = urlparse(BROKER)
    try:
        sock = socket.create_connection(
            (parsed.hostname or "127.0.0.1", parsed.port or 5672), timeout=1.5
        )
        sock.close()
        return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _available(), reason=f"没有可用的 RabbitMQ（{BROKER}）或未装 pika")


def _url(prefix: str, tmp_path: Path) -> str:
    return f"{BROKER}?state=sqlite:///{tmp_path / 'state.db'}&prefix={prefix}"


@pytest.fixture
def prefix() -> str:
    return f"t{uuid.uuid4().hex[:8]}."


@pytest.fixture
def amp(prefix, tmp_path):
    transport = build_transport(_url(prefix, tmp_path))
    try:
        yield transport
    finally:
        for queue in ("q", "exp", "q2"):
            transport._drop_queues(queue)
        transport.close()


def _env(task: str = "t", priority: int = 0, **kwargs: object) -> Envelope:
    return Envelope(task=task, priority=priority, **kwargs)


def test_requires_state_sidecar():
    from taskmq.errors import ConfigError

    with pytest.raises(ConfigError, match="状态侧车"):
        build_transport(f"{BROKER}?prefix=x.")


def test_priority_within_queue_and_stats(amp):
    for task, priority in (("low", 0), ("mid", 1), ("high", 5)):
        amp.enqueue(_env(task, priority), queue="q")
    time.sleep(0.2)
    granted = amp.reserve(["q"], worker_id="w", lease=30, limit=5)
    assert [d.envelope.task for d in granted] == ["high", "mid", "low"]   # broker 侧排序
    assert amp.queue_stats(["q"])[0].inflight == 3
    amp.ack(granted[0])
    amp.ack(granted[0])                                                   # 幂等
    assert amp.queue_stats(["q"])[0].inflight == 2


def test_defer_does_not_consume_deliveries(amp):
    amp.enqueue(_env("job"), queue="q")
    time.sleep(0.1)
    first = amp.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
    assert first.deliveries == 1
    amp.defer(first, delay=0.4)                                           # 放进 delay 队列（TTL 回投）
    assert amp.reserve(["q"], worker_id="w", lease=30, limit=1) == []
    assert amp.next_visible_at(["q"]) is not None
    time.sleep(0.9)
    again = amp.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
    assert again.deliveries == 1                                          # 没有消耗投递次数


def test_yield_reservation_counts(amp):
    amp.enqueue(_env("long"), queue="q")
    time.sleep(0.1)
    delivery = amp.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
    assert amp.yield_reservation(delivery, delay=0.0, max_yields=1) is True
    again = amp.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
    assert again.yields == 1
    assert amp.yield_reservation(again, delay=0.0, max_yields=1) is False


def test_lease_reap_requeues_and_redelivers(amp):
    amp.enqueue(_env("job"), queue="q")
    time.sleep(0.1)
    first = amp.reserve(["q"], worker_id="w", lease=0.05, limit=1)[0]
    time.sleep(0.1)
    assert amp.reap_expired_leases() == 1
    second = amp.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
    assert second.message_id == first.message_id
    assert second.deliveries == 2


def test_expired_message_is_not_delivered(amp, tmp_path):
    job_id = amp.enqueue(_env("old", expires_at=time.time() - 1), queue="exp")
    time.sleep(0.1)
    assert amp.reserve(["exp"], worker_id="w", lease=5, limit=1) == []
    record = amp.get_state(job_id)
    assert record is not None and record.state == JobState.EXPIRED


def test_dlq_scan_and_replay(amp):
    amp.enqueue(_env("doomed", priority=0), queue="q")
    time.sleep(0.1)
    delivery = amp.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
    amp.dead_letter(delivery, "boom")

    dead = amp.dead_letters()
    assert [(d.task, d.reason) for d in dead] == [("doomed", "boom")]
    assert len(amp.dead_letters()) == 1                    # 非破坏性扫描（读两遍都还在）
    assert amp.queue_stats(["q"])[0].dead == 1

    assert amp.replay_dead(dead[0].message_id, priority=7) is True
    assert amp.queue_stats(["q"])[0].pending == 1
    assert amp.dead_letters() == []


def test_state_leases_and_workers_go_through_sidecar(amp):
    amp.set_state("j1", "RUNNING", task="t", stage="half")
    amp.set_state("j1", "SUCCEEDED", result={"n": 1}, percent=100)
    record = amp.get_state("j1")
    assert record is not None and record.result == {"n": 1}
    assert record.meta["stage"] == "half" and record.meta["percent"] == 100

    assert amp.acquire_lease("ck", "w1", 5.0) is True
    assert amp.acquire_lease("ck", "w2", 5.0) is False
    amp.release_lease("ck", "w1")
    assert amp.acquire_lease("ck", "w2", 5.0) is True

    amp.register_worker("w1", queues=("q",), pool="threads", concurrency=2)
    assert [w.worker_id for w in amp.list_workers()] == ["w1"]
    amp.deregister_worker("w1")
    assert [r.job_id for r in amp.list_jobs(prefix="j")] == ["j1"]


def test_multiple_consumers_do_not_duplicate(prefix, tmp_path):
    """两个 transport 实例（两条连接）抢同一批消息 → 不重复（broker 保证）。"""
    writer = build_transport(_url(prefix, tmp_path))
    for index in range(30):
        writer.enqueue(_env(f"t{index}"), queue="q")
    time.sleep(0.3)

    claimed: list[int] = []
    lock = threading.Lock()

    def drain(name: str) -> None:
        transport = build_transport(_url(prefix, tmp_path))
        try:
            for _ in range(40):
                batch = transport.reserve(["q"], worker_id=name, lease=30, limit=1)
                if not batch:
                    time.sleep(0.05)
                    if not transport.reserve(["q"], worker_id=name, lease=30, limit=1):
                        return
                    continue
                with lock:
                    claimed.append(batch[0].message_id)
                transport.ack(batch[0])
        finally:
            transport.close()

    threads = [threading.Thread(target=drain, args=(f"w{i}",)) for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(claimed) == 30 and len(set(claimed)) == 30
    for queue in ("q",):
        writer._drop_queues(queue)
    writer.close()


def test_conformance_suite(prefix, tmp_path):
    def factory():
        return build_transport(_url(f"c{uuid.uuid4().hex[:8]}.", tmp_path))

    def cleanup(transport):
        for queue in ("q", "conformance", "conformance-alt", "exp"):
            transport._drop_queues(queue)
        transport.close()

    executed = transport_conformance(factory, cleanup=cleanup)
    skipped = [name for name in CONFORMANCE_SCENARIOS if name not in executed]
    assert skipped == ["global_priority"]                  # 按声明降级跳过
    assert {"priority_fifo", "atomic_claim", "lease_recovery", "dlq_replay", "job_listing"} <= set(
        executed
    )


def test_app_end_to_end_and_dag(prefix, tmp_path):
    app = App(Config(transport=_url(prefix, tmp_path), serializer="json", events="null", concurrency=2))
    calls: list[str] = []

    @app.task(name="amq.extract", queue="amq")
    def extract(source: str) -> list[int]:
        calls.append("extract")
        return [1, 2, 3]

    @app.task(name="amq.clean", queue="amq")
    def clean(rows: list[int]) -> list[int]:
        calls.append("clean")
        return [row * 3 for row in rows]

    @app.task(name="amq.report", queue="amq")
    def report(tables: list) -> int:
        calls.append("report")
        return sum(sum(table) for table in tables)

    @app.workflow("amq")
    def amq(wf, source: str):
        raw = wf.step("extract", "amq.extract", args=(source,))
        cleaned = wf.step("clean", "amq.clean", deps={"rows": raw})
        return wf.join("report", "amq.report", deps=[cleaned], collect="tables")

    handle = app.submit_workflow("amq", {"source": "s3://x"})
    run_until_idle(app, queues=["amq"], timeout=60)

    assert handle.status().state == JobState.SUCCEEDED
    assert handle.result() == 18                          # sum([3,6,9])
    assert calls == ["extract", "clean", "report"]
    for queue in ("amq",):
        app.transport._drop_queues(queue)
    app.close()
