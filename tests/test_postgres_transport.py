"""Postgres transport：与 memory/sqlite/redis 同语义 + 一致性套件 + DAG。

- 没装 psycopg 或连不上 PG → 整个文件 skip（`TASKMQ_TEST_POSTGRES_URL` 可覆盖地址）。
- 每个用例用**独立表前缀**，跑完 DROP 掉自己的表（绝不动别人的库）。
"""
from __future__ import annotations

import importlib.util
import os
import socket
import threading
import uuid
from urllib.parse import urlparse

import pytest

from taskmq import App, Config, Envelope
from taskmq.errors import LeaseLost
from taskmq.protocol import JSONCodec
from taskmq.testing import CONFORMANCE_SCENARIOS, run_until_idle, transport_conformance
from taskmq.transport.base import JobState
from taskmq.transport.postgres import PostgresTransport

URL = os.environ.get(
    "TASKMQ_TEST_POSTGRES_URL", "postgresql://taskmq:taskmq@127.0.0.1:55432/taskmq"
)


def _available() -> bool:
    if importlib.util.find_spec("psycopg") is None:
        return False
    parsed = urlparse(URL)
    try:
        sock = socket.create_connection(
            (parsed.hostname or "127.0.0.1", parsed.port or 5432), timeout=1.5
        )
        sock.close()
        return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _available(), reason=f"没有可用的 PostgreSQL（{URL}）或未安装 psycopg"
)


class FakeClock:
    def __init__(self, start: float = 1_800_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def prefix() -> str:
    return f"t{uuid.uuid4().hex[:10]}_"


@pytest.fixture
def pg(prefix):
    transport = PostgresTransport(URL, codec=JSONCodec(), prefix=prefix)
    try:
        yield transport
    finally:
        transport._drop_schema()
        transport.close()


def _env(task: str = "t", priority: int = 0, **kwargs: object) -> Envelope:
    return Envelope(task=task, priority=priority, **kwargs)


# ------------------------------------------------------------------ 基础语义
def test_priority_order_and_fifo(pg):
    for task, priority in (("low1", 0), ("high", 5), ("mid", 1), ("low2", 0)):
        pg.enqueue(_env(task, priority), queue="q")
    granted = pg.reserve(["q"], worker_id="w", lease=30, limit=10)
    assert [d.envelope.task for d in granted] == ["high", "mid", "low1", "low2"]
    assert [d.deliveries for d in granted] == [1, 1, 1, 1]
    assert pg.peek_max_priority(["q"]) is None


def test_lease_reap_late_ack_and_idempotent_ack(prefix):
    clock = FakeClock()
    transport = PostgresTransport(URL, codec=JSONCodec(), prefix=prefix, clock=clock)
    try:
        transport.enqueue(_env(), queue="q")
        first = transport.reserve(["q"], worker_id="w1", lease=10, limit=1)[0]
        assert transport.reserve(["q"], worker_id="w2", lease=10, limit=1) == []

        clock.advance(11)
        assert transport.reap_expired_leases() == 1
        second = transport.reserve(["q"], worker_id="w2", lease=10, limit=1)[0]
        assert second.deliveries == 2

        with pytest.raises(LeaseLost):
            transport.ack(first)
        transport.ack(second)
        transport.ack(second)                       # 幂等
        assert transport.queue_stats(["q"])[0].inflight == 0
    finally:
        transport._drop_schema()
        transport.close()


def test_defer_and_yield_semantics(prefix):
    clock = FakeClock()
    transport = PostgresTransport(URL, codec=JSONCodec(), prefix=prefix, clock=clock)
    try:
        transport.enqueue(_env("low"), queue="q")
        delivery = transport.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
        transport.defer(delivery, delay=0.0)        # 不消耗投递次数
        again = transport.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
        assert again.deliveries == 1

        transport.enqueue(_env("urgent", 9), queue="q")
        assert transport.yield_reservation(again, delay=0.0, max_yields=1) is True
        assert [d.envelope.task for d in transport.reserve(["q"], worker_id="w", lease=30, limit=1)] == [
            "urgent"
        ]
        third = transport.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
        assert third.yields == 1 and third.envelope.task == "low"
        assert transport.yield_reservation(third, delay=0.0, max_yields=1) is False
    finally:
        transport._drop_schema()
        transport.close()


def test_expires_and_idempotency_key(prefix):
    clock = FakeClock()
    transport = PostgresTransport(URL, codec=JSONCodec(), prefix=prefix, clock=clock)
    try:
        job_id = transport.enqueue(_env(expires_at=clock.now + 5), queue="exp")
        first = transport.enqueue(_env("t", key="k"), queue="q")
        assert transport.enqueue(_env("t2", key="k"), queue="q") == first
        assert transport.queue_stats(["q"])[0].pending == 1

        clock.advance(6)
        assert transport.reserve(["exp"], worker_id="w", lease=5, limit=1) == []
        record = transport.get_state(job_id)
        assert record is not None and record.state == JobState.EXPIRED
    finally:
        transport._drop_schema()
        transport.close()


def test_dlq_and_replay(pg):
    pg.enqueue(_env(priority=0), queue="q")
    delivery = pg.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
    pg.dead_letter(delivery, "boom")
    dead = pg.dead_letters()
    assert len(dead) == 1 and dead[0].reason == "boom"
    assert pg.queue_stats(["q"])[0].dead == 1
    assert pg.replay_dead(dead[0].message_id, priority=7) is True
    assert pg.priority_stats(["q"]) == {7: 1}
    assert pg.dead_letters() == []
    assert pg.get_state(delivery.job_id).state == JobState.QUEUED


def test_job_state_round_trip(pg):
    pg.set_state("j1", "RUNNING", task="t", attempt=1, stage="half")
    pg.set_state("j1", "SUCCEEDED", result={"answer": [1, 2, 3]}, percent=100)
    record = pg.get_state("j1")
    assert record is not None
    assert record.result == {"answer": [1, 2, 3]} and record.has_result is True
    assert record.meta["stage"] == "half" and record.meta["percent"] == 100
    assert pg.get_state("missing") is None


def test_named_leases_and_worker_registry(pg):
    assert pg.acquire_lease("ck", "w1", 5.0) is True
    assert pg.acquire_lease("ck", "w2", 5.0) is False
    assert pg.renew_lease("ck", "w2", 5.0) is False
    assert pg.renew_lease("ck", "w1", 5.0) is True
    pg.release_lease("ck", "w1")
    assert pg.acquire_lease("ck", "w2", 5.0) is True

    pg.register_worker("w1", queues=("q",), pool="threads", concurrency=4, meta={"pid": 7})
    info = pg.list_workers()[0]
    assert info.worker_id == "w1" and info.queues == ("q",) and info.meta["pid"] == 7
    pg.heartbeat_worker("w1")
    pg.deregister_worker("w1")
    assert pg.list_workers() == []


def test_list_jobs_filters(pg):
    pg.set_state("wf-1", JobState.RUNNING, task="workflow:demo")
    pg.set_state("wf-1::a", JobState.SUCCEEDED, task="t")
    pg.set_state("other", JobState.SUCCEEDED)
    assert {r.job_id for r in pg.list_jobs(prefix="wf-")} == {"wf-1", "wf-1::a"}
    assert [r.job_id for r in pg.list_jobs(prefix="wf-", states=[JobState.RUNNING])] == ["wf-1"]
    assert len(pg.list_jobs(limit=1)) == 1


# --------------------------------------------------- SKIP LOCKED 多连接并发
def test_concurrent_claims_across_connections_have_no_duplicates(prefix):
    """PG 的看家本领：两个连接（相当于两台 worker）+ 4 线程并发 claim，零重复、零阻塞。"""
    writer = PostgresTransport(URL, codec=JSONCodec(), prefix=prefix)
    total = 200
    for index in range(total):
        writer.enqueue(_env(f"t{index}"), queue="q")

    claimed: list[int] = []
    lock = threading.Lock()

    def drain(worker: str) -> None:
        transport = PostgresTransport(URL, codec=JSONCodec(), prefix=prefix)
        try:
            while True:
                batch = transport.reserve(["q"], worker_id=worker, lease=30, limit=5)
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

    assert len(claimed) == total and len(set(claimed)) == total
    assert writer.queue_stats(["q"])[0].pending == 0
    writer._drop_schema()
    writer.close()


# ------------------------------------------------------------ 一致性 & DAG
def test_conformance_suite(prefix):
    def factory():
        return PostgresTransport(URL, codec=JSONCodec(), prefix=f"c{uuid.uuid4().hex[:8]}_")

    def cleanup(transport):
        transport._drop_schema()
        transport.close()

    executed = transport_conformance(factory, cleanup=cleanup)
    assert executed == list(CONFORMANCE_SCENARIOS)          # PG 不做任何降级声明


def test_app_end_to_end_with_priority(prefix):
    app = App(
        Config(
            transport=f"{URL}?prefix={prefix}",
            serializer="json",
            events="null",
            concurrency=4,
        )
    )
    order: list[str] = []

    @app.task(name="pg.job", queue="pgq")
    def job(name: str) -> str:
        order.append(name)
        return name

    handles = [app.submit("pg.job", (f"low{i}",)) for i in range(10)]
    urgent = app.submit("pg.job", ("urgent",), priority=9)
    run_until_idle(app, queues=["pgq"], timeout=60)

    assert all(handle.successful() for handle in handles)
    assert urgent.get(timeout=5) == "urgent"
    assert order[0] == "urgent"
    assert app.transport.queue_stats(["pgq"])[0].pending == 0
    app.transport._drop_schema()
    app.close()


def test_dag_workflow_over_postgres(prefix):
    app = App(Config(transport=f"{URL}?prefix={prefix}", serializer="json", events="null"))
    calls: list[str] = []

    @app.task(name="pgd.extract", queue="pgd")
    def extract(source: str) -> list[int]:
        calls.append("extract")
        return [1, 2, 3]

    @app.task(name="pgd.clean", queue="pgd")
    def clean(rows: list[int]) -> list[int]:
        calls.append("clean")
        return [row * 2 for row in rows]

    @app.task(name="pgd.report", queue="pgd")
    def report(tables: list) -> int:
        calls.append("report")
        return sum(sum(table) for table in tables)

    @app.workflow("pgd")
    def pgd(wf, source: str):
        raw = wf.step("extract", "pgd.extract", args=(source,))
        cleaned = wf.step("clean", "pgd.clean", deps={"rows": raw})
        return wf.join("report", "pgd.report", deps=[cleaned], collect="tables")

    handle = app.submit_workflow("pgd", {"source": "s3://x"})
    run_until_idle(app, queues=["pgd"], timeout=60)
    assert handle.status().state == JobState.SUCCEEDED
    assert handle.result() == 12
    assert calls == ["extract", "clean", "report"]
    app.transport._drop_schema()
    app.close()
