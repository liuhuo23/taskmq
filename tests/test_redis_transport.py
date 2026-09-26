"""RedisTransport：与 memory/sqlite 同语义的不变量（本地有 Redis 才跑）。

隔离：每个测试用独立键前缀 + DB 15，跑完只删自己的前缀（绝不 FLUSH 别人的库）。
可用 `TASKMQ_TEST_REDIS_URL` 指定别的地址。
"""
from __future__ import annotations

import os
import threading
import uuid

import pytest

from taskmq import App, Config, Envelope
from taskmq.errors import LeaseLost
from taskmq.protocol import JSONCodec
from taskmq.redis_client import RedisClient
from taskmq.transport.redis import RedisTransport
from taskmq.worker.runner import Worker

REDIS_URL = os.environ.get("TASKMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379/15")


def _redis_available() -> bool:
    try:
        return RedisClient(REDIS_URL, timeout=1.5).ping()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _redis_available(), reason=f"没有可用的 Redis（{REDIS_URL}）")


class FakeClock:
    def __init__(self, start: float = 1_800_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def prefix():
    """独立前缀；**无论测试成功还是失败**都在 teardown 删掉自己的键。"""
    value = f"taskmq:test:{uuid.uuid4().hex}:"
    yield value
    client = RedisClient(REDIS_URL, timeout=3)
    try:
        client.delete_prefix(value.rstrip(":"))
    finally:
        client.close()


@pytest.fixture(params=[True, False], ids=["lua", "no-lua"])
def lua_mode(request):
    """Lua 快路径与 WATCH/MULTI 回退路径**各跑一遍全部用例**。"""
    return bool(request.param)


@pytest.fixture
def rt(prefix, lua_mode):
    transport = RedisTransport(REDIS_URL, codec=JSONCodec(), prefix=prefix, lua=lua_mode)
    try:
        yield transport
    finally:
        transport.close()


def _env(task: str = "t", priority: int = 0, **kwargs) -> Envelope:
    return Envelope(task=task, priority=priority, **kwargs)


def test_auto_detect_falls_back_when_lua_is_forbidden(monkeypatch, prefix):
    """托管 Redis 常见配置：EVAL 被禁（rename-command EVAL ""）→ 自动走 WATCH/MULTI。"""
    from taskmq import redis_client
    from taskmq.errors import TransportError

    original = redis_client.RedisClient.execute

    def guarded(self, *args):
        if args and str(args[0]).upper() == "EVAL":
            raise redis_client.RedisError("ERR unknown command 'EVAL'")
        return original(self, *args)

    monkeypatch.setattr(redis_client.RedisClient, "execute", guarded)

    transport = RedisTransport(REDIS_URL, codec=JSONCodec(), prefix=prefix)   # lua=None → 探测
    try:
        assert transport.lua_enabled is False
        transport.enqueue(Envelope(task="t"), queue="q")
        assert [d.envelope.task for d in transport.reserve(["q"], worker_id="w", lease=5, limit=1)] == ["t"]
    finally:
        transport.close()

    with pytest.raises(TransportError):                                        # 强制要求 Lua → 报错
        RedisTransport(REDIS_URL, codec=JSONCodec(), prefix=prefix, lua=True)


def test_lua_off_matches_lua_on_semantics(prefix):
    """同一组操作在两种模式下结果一致（手工对拍）。"""
    results = {}
    for mode in (True, False):
        transport = RedisTransport(
            REDIS_URL, codec=JSONCodec(), prefix=f"{prefix}{mode}:", lua=mode
        )
        try:
            for task, priority in (("a", 0), ("b", 5), ("c", 0)):
                transport.enqueue(Envelope(task=task, priority=priority), queue="q")
            granted = transport.reserve(["q"], worker_id="w", lease=10, limit=3)
            order = [d.envelope.task for d in granted]
            transport.dead_letter(granted[0], "boom")
            results[mode] = (order, [d.reason for d in transport.dead_letters()])
            transport.flush_prefix()
        finally:
            transport.close()
    assert results[True] == results[False] == (["b", "a", "c"], ["boom"])


def test_priority_order_and_fifo(rt):
    for task, priority in (("low1", 0), ("high", 5), ("mid", 1), ("low2", 0)):
        rt.enqueue(_env(task, priority), queue="q")
    granted = rt.reserve(["q"], worker_id="w", lease=30, limit=10)
    assert [d.envelope.task for d in granted] == ["high", "mid", "low1", "low2"]
    assert [d.priority for d in granted] == [5, 1, 0, 0]
    assert [d.deliveries for d in granted] == [1, 1, 1, 1]
    assert rt.peek_max_priority(["q"]) is None            # 都被领走了


def test_lease_reap_and_late_ack(prefix, lua_mode):
    clock = FakeClock()
    transport = RedisTransport(
        REDIS_URL, codec=JSONCodec(), prefix=prefix, clock=clock, lua=lua_mode
    )
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
        transport.ack(second)                              # 幂等
        assert transport.queue_stats(["q"])[0].inflight == 0
    finally:
        transport.close()


def test_defer_and_yield_semantics(prefix, lua_mode):
    clock = FakeClock()
    transport = RedisTransport(
        REDIS_URL, codec=JSONCodec(), prefix=prefix, clock=clock, lua=lua_mode
    )
    try:
        transport.enqueue(_env("low"), queue="q")
        delivery = transport.reserve(["q"], worker_id="w1", lease=30, limit=1)[0]
        assert delivery.deliveries == 1

        transport.defer(delivery, delay=0.0)               # 推迟不算一次投递
        again = transport.reserve(["q"], worker_id="w1", lease=30, limit=1)[0]
        assert again.deliveries == 1

        transport.enqueue(_env("urgent", 9), queue="q")
        assert transport.peek_max_priority(["q"]) == 9
        assert transport.yield_reservation(again, delay=0.05, max_yields=1) is True
        granted = transport.reserve(["q"], worker_id="w1", lease=30, limit=1)
        assert [d.envelope.task for d in granted] == ["urgent"]

        clock.advance(0.05)
        third = transport.reserve(["q"], worker_id="w1", lease=30, limit=1)[0]
        assert third.envelope.task == "low" and third.yields == 1
        assert transport.yield_reservation(third, delay=0.0, max_yields=1) is False
    finally:
        transport.close()


def test_expires_and_idempotency(prefix, lua_mode):
    clock = FakeClock()
    transport = RedisTransport(
        REDIS_URL, codec=JSONCodec(), prefix=prefix, clock=clock, lua=lua_mode
    )
    try:
        job_id = transport.enqueue(_env(expires_at=clock.now + 5), queue="exp")
        first = transport.enqueue(_env("t", key="k"), queue="q")
        second = transport.enqueue(_env("t2", key="k"), queue="q")
        assert first == second                              # 幂等键
        assert transport.queue_stats(["q"])[0].pending == 1

        clock.advance(6)
        assert transport.reserve(["exp"], worker_id="w", lease=5, limit=1) == []   # 过期的不投递
        record = transport.get_state(job_id)
        assert record is not None and record.state == "EXPIRED"
    finally:
        transport.close()


def test_dlq_and_replay(rt):
    rt.enqueue(_env(priority=0), queue="q")
    delivery = rt.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
    rt.dead_letter(delivery, "boom")

    dead = rt.dead_letters()
    assert len(dead) == 1 and dead[0].reason == "boom"
    assert rt.queue_stats(["q"])[0].dead == 1
    assert rt.replay_dead(dead[0].message_id, priority=7) is True
    assert rt.priority_stats(["q"]) == {7: 1}
    assert rt.dead_letters() == []


def test_state_round_trip(rt):
    rt.set_state("job-1", "RUNNING", task="t", attempt=1, stage="half")
    rt.set_state("job-1", "SUCCEEDED", result={"answer": [1, 2, 3]}, percent=100)
    record = rt.get_state("job-1")
    assert record is not None
    assert record.result == {"answer": [1, 2, 3]} and record.has_result is True
    assert record.meta["stage"] == "half" and record.meta["percent"] == 100
    assert rt.get_state("missing") is None


def test_named_leases(rt):
    assert rt.acquire_lease("ck:u1", "w1", 5.0) is True
    assert rt.acquire_lease("ck:u1", "w2", 5.0) is False
    assert rt.renew_lease("ck:u1", "w2", 5.0) is False
    rt.release_lease("ck:u1", "w2")                        # 非持有者无效
    assert rt.acquire_lease("ck:u1", "w2", 5.0) is False
    rt.release_lease("ck:u1", "w1")
    assert rt.acquire_lease("ck:u1", "w2", 5.0) is True


def test_peek_visible_and_stats(prefix, lua_mode):
    clock = FakeClock()
    transport = RedisTransport(
        REDIS_URL, codec=JSONCodec(), prefix=prefix, clock=clock, lua=lua_mode
    )
    try:
        transport.enqueue(_env("later", 3), queue="q", delay=100)
        assert transport.peek_max_priority(["q"]) is None
        assert transport.next_visible_at(["q"]) == pytest.approx(clock.now + 100)
        clock.advance(101)
        assert transport.peek_max_priority(["q"]) == 3
        stats = transport.queue_stats(["q"])[0]
        assert stats.pending == 1 and stats.inflight == 0
    finally:
        transport.close()


def test_no_double_claim_across_connections(prefix, lua_mode):
    writer = RedisTransport(REDIS_URL, codec=JSONCodec(), prefix=prefix, lua=lua_mode)
    for index in range(200):
        writer.enqueue(_env(f"t{index}"), queue="q")

    claimed: list[int] = []
    lock = threading.Lock()

    def drain(worker_id: str) -> None:
        transport = RedisTransport(REDIS_URL, codec=JSONCodec(), prefix=prefix, lua=lua_mode)
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

    assert len(claimed) == 200 and len(set(claimed)) == 200
    assert writer.queue_stats(["q"])[0].pending == 0
    writer.close()


def test_app_end_to_end_over_redis(prefix, lua_mode):
    url = f"{REDIS_URL}?prefix={prefix}&lua={'on' if lua_mode else 'off'}"
    app = App(Config(transport=url, serializer="json", events="null", concurrency=4))
    order: list[str] = []
    lock = threading.Lock()

    @app.task(name="redis.job", queue="redis")
    def job(name: str, priority: int = 0) -> str:
        with lock:
            order.append(name)
        return name

    handles = [app.submit("redis.job", (f"low{i}",)) for i in range(40)]
    urgent = app.submit("redis.job", ("urgent",), priority=9)

    with Worker(app, queues=["redis"]) as worker:
        worker.run_until_idle(timeout=60)

    assert all(handle.successful() for handle in handles)
    assert urgent.get(timeout=5) == "urgent"
    assert order[0] == "urgent"                            # 全局严格优先
    assert app.transport.queue_stats(["redis"])[0].pending == 0
    app.close()
