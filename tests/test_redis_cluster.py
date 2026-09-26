"""Redis Cluster（`?cluster=1`）：hash tag 分槽、Lua 显式 KEYS、跨队列降级取件。

跑法：`make redis-cluster-up` 起 3 主容器（默认 redis://127.0.0.1:7380/0），
没有集群时整个文件 skip；地址可用 `TASKMQ_TEST_REDIS_CLUSTER_URL` 覆盖。
隔离：每个测试一个随机 prefix，清理走跨主节点 SCAN（`flush_prefix`），不碰别人的键。
"""
from __future__ import annotations

import os
import threading
import uuid

import pytest

from taskmq import App, Config, Envelope
from taskmq.errors import ConfigError, LeaseLost, TransportError
from taskmq.protocol import JSONCodec
from taskmq.redis_client import RedisClient, RedisClusterClient, hash_slot
from taskmq.testing import CONFORMANCE_SCENARIOS, transport_conformance
from taskmq.transport.base import JobState
from taskmq.transport.redis import RedisTransport
from taskmq.worker.runner import Worker

CLUSTER_URL = os.environ.get("TASKMQ_TEST_REDIS_CLUSTER_URL", "redis://127.0.0.1:7380/0")


def _cluster_available() -> bool:
    try:
        client = RedisClusterClient(CLUSTER_URL, timeout=1.5)
    except Exception:
        return False
    try:
        info = RedisClient.decode(client.execute("INFO", "cluster")) or ""
        return "cluster_enabled:1" in info
    except Exception:
        return False
    finally:
        client.close()


pytestmark = pytest.mark.skipif(
    not _cluster_available(), reason=f"没有可用的 Redis Cluster（{CLUSTER_URL}）"
)


class FakeClock:
    def __init__(self, start: float = 1_800_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def prefix():
    """独立前缀（集群里跨 3 个节点）：跑完把本前缀下的键全删掉。"""
    value = f"taskmq:test:{uuid.uuid4().hex}:"
    yield value
    client = RedisClusterClient(CLUSTER_URL, timeout=3)
    try:
        client.delete_prefix(value.rstrip(":"))
    finally:
        client.close()


@pytest.fixture(params=[True, False], ids=["lua", "no-lua"])
def lua_mode(request):
    """Lua 快路径与 WATCH/MULTI 回退路径各跑一遍（Cluster 下后者也不许跨槽）。"""
    return bool(request.param)


@pytest.fixture
def rt(prefix, lua_mode):
    transport = RedisTransport(
        CLUSTER_URL, codec=JSONCodec(), prefix=prefix, lua=lua_mode, cluster=True
    )
    try:
        yield transport
    finally:
        transport.close()


def _env(task: str = "t", priority: int = 0, **kwargs) -> Envelope:
    return Envelope(task=task, priority=priority, **kwargs)


# ------------------------------------------------------------------ 键命名 / 路由
def test_hash_tag_puts_queues_on_different_nodes(prefix):
    """每个队列一个 hash tag：不同队列的键真的落在不同主节点上（否则测不出跨槽）。"""
    client = RedisClusterClient(CLUSTER_URL, timeout=3)
    transport = RedisTransport(CLUSTER_URL, codec=JSONCodec(), prefix=prefix, cluster=True)
    try:
        assert transport._qkey("ready", "q1") == f"{prefix}{{q1}}:ready"
        assert transport._mkey("q1", 7) == f"{prefix}{{q1}}:msg:7"
        first, second = f"{prefix}{{q1}}:ready", f"{prefix}{{q2}}:ready"
        assert client.node_for(first) != client.node_for(second), "两个队列应在不同节点"
        # 同一队列的结构（ready/delayed/leases/prio/msg）必须同槽
        for key in (f"{prefix}{{q1}}:delayed", f"{prefix}{{q1}}:leases", f"{prefix}{{q1}}:msg:7"):
            assert client.node_for(key) == client.node_for(first)
    finally:
        transport.close()
        client.close()


def test_cluster_client_refuses_cross_slot_command(prefix):
    """跨槽命令必须在**发出前**被拦下（Redis 7 的 Lua 会放行，静默写错节点）。"""
    client = RedisClusterClient(CLUSTER_URL, timeout=3)
    try:
        first, second = f"{prefix}{{q1}}:ready", f"{prefix}{{q2}}:ready"
        assert client.node_for(first) != client.node_for(second)
        with pytest.raises(TransportError, match="同一 slot"):
            client.execute("MGET", first, second)
    finally:
        client.close()


def test_client_follows_moved_when_topology_is_stale(prefix):
    """拓扑过期时靠 MOVED 自我纠正（真实的 slot 迁移就是这个路径）。"""
    client = RedisClusterClient(CLUSTER_URL, timeout=3)
    try:
        key = f"{prefix}{{q1}}:ready"
        slot = hash_slot(key)
        right = client.node_for(key)
        wrong = [node for node in client.masters if node != right][0]
        client._slot_nodes[slot] = wrong                  # 人为把映射指到错节点
        assert client.execute("SET", key, "v") == "OK"    # MOVED → 就地修正 + 重试
        client._slot_nodes[slot] = wrong                  # 再错一次（验证是"跟随"而不是"撞对"）
        assert client.execute("GET", key) == b"v"
        client.execute("DEL", key)
    finally:
        client.close()


def test_cluster_transport_rejects_bad_config():
    with pytest.raises(ConfigError, match="花括号"):
        RedisTransport(CLUSTER_URL, codec=JSONCodec(), prefix="taskmq:{x}:", cluster=True)
    with pytest.raises(ConfigError, match="db 0"):
        RedisTransport("redis://127.0.0.1:7380/15", codec=JSONCodec(), cluster=True)


# ------------------------------------------------------------------ 基本语义
def test_reserve_and_ack_across_slots(rt):
    for index in range(3):
        rt.enqueue(_env(f"a{index}"), queue="q1")
        rt.enqueue(_env(f"b{index}"), queue="q2")
    granted = rt.reserve(["q1", "q2"], worker_id="w", lease=30, limit=6)
    assert len(granted) == 6
    assert {delivery.queue for delivery in granted} == {"q1", "q2"}
    for delivery in granted:
        rt.ack(delivery)
    assert rt.queue_stats(["q1"])[0].pending == 0
    assert rt.get_state(granted[0].job_id).state == JobState.RUNNING   # 取件即 RUNNING（与单机一致）


def test_weighted_fairness_within_band(rt):
    """档内加权轮询在 Cluster 下同样成立（3:1 → 取 40 条约 30:10）。"""
    rt.set_queue_weights({"q1": 3, "q2": 1})
    for _ in range(100):
        rt.enqueue(_env(), queue="q1")
        rt.enqueue(_env(), queue="q2")

    taken = {"q1": 0, "q2": 0}
    for _ in range(40):
        batch = rt.reserve(["q1", "q2"], worker_id="w", lease=30, limit=1)
        assert batch, "不该取空"
        taken[batch[0].queue] += 1
        rt.ack(batch[0])

    assert abs(taken["q1"] - 30) <= 2, taken
    assert abs(taken["q2"] - 10) <= 2, taken


def test_priority_still_beats_weights(rt):
    rt.set_queue_weights({"q1": 9, "q2": 1})
    for _ in range(5):
        rt.enqueue(_env("low"), queue="q1")
    rt.enqueue(_env("urgent", 9), queue="q2")
    granted = rt.reserve(["q1", "q2"], worker_id="w", lease=30, limit=1)
    assert granted and granted[0].envelope.task == "urgent"


def test_no_double_claim_across_connections(prefix, lua_mode):
    writer = RedisTransport(
        CLUSTER_URL, codec=JSONCodec(), prefix=prefix, lua=lua_mode, cluster=True
    )
    for index in range(200):
        writer.enqueue(_env(f"t{index}"), queue="q1")

    claimed: list[int] = []
    lock = threading.Lock()

    def drain(worker_id: str) -> None:
        transport = RedisTransport(
            CLUSTER_URL, codec=JSONCodec(), prefix=prefix, lua=lua_mode, cluster=True
        )
        try:
            while True:
                batch = transport.reserve(["q1"], worker_id=worker_id, lease=30, limit=10)
                if not batch:
                    return
                with lock:
                    claimed.extend(delivery.message_id for delivery in batch)
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
    assert writer.queue_stats(["q1"])[0].pending == 0
    writer.close()


def test_lease_recovery(prefix, lua_mode):
    clock = FakeClock()
    transport = RedisTransport(
        CLUSTER_URL, codec=JSONCodec(), prefix=prefix, clock=clock, lua=lua_mode, cluster=True
    )
    try:
        transport.enqueue(_env(), queue="q1")
        first = transport.reserve(["q1"], worker_id="w1", lease=10, limit=1)[0]
        assert transport.reserve(["q1"], worker_id="w2", lease=10, limit=1) == []

        clock.advance(11)
        assert transport.reap_expired_leases() == 1              # 逐队列回收（跨槽拆多次）
        second = transport.reserve(["q1"], worker_id="w2", lease=10, limit=1)[0]
        assert second.deliveries == 2
        with pytest.raises(LeaseLost):
            transport.ack(first)
        transport.ack(second)
    finally:
        transport.close()


def test_replay_dead_moves_message_between_slots(rt):
    """换队列 = 换 slot：重放要把消息整份搬到目标队列的槽里，且搬完还能正常 ack。"""
    rt.enqueue(_env("t"), queue="q1")
    delivery = rt.reserve(["q1"], worker_id="w", lease=30, limit=1)[0]
    rt.dead_letter(delivery, "boom")
    message_id = delivery.message_id
    old_key = f"{rt.prefix}{{q1}}:msg:{message_id}"
    new_key = f"{rt.prefix}{{q2}}:msg:{message_id}"

    client = RedisClusterClient(CLUSTER_URL, timeout=3)
    try:
        assert client.execute("EXISTS", old_key) == 1
        assert rt.replay_dead(message_id, queue="q2", priority=7) is True
        assert client.execute("EXISTS", old_key) == 0
        assert client.execute("EXISTS", new_key) == 1
        assert client.execute("HGET", f"{rt.prefix}msgq", str(message_id)) == b"q2"
    finally:
        client.close()

    again = rt.reserve(["q2"], worker_id="w", lease=30, limit=1)[0]
    assert again.message_id == message_id
    assert again.queue == "q2" and again.priority == 7
    rt.ack(again)                                     # seq 字段没丢才 ack 得掉
    assert rt.get_state(again.job_id).state == JobState.RUNNING
    assert rt.dead_letters() == []
    assert rt.priority_stats(["q2"]) == {}             # ack 后计数归零，空桶不返回


def test_dead_letters_and_stats(rt):
    rt.enqueue(_env(), queue="q1")
    delivery = rt.reserve(["q1"], worker_id="w", lease=30, limit=1)[0]
    rt.dead_letter(delivery, "boom")
    dead = rt.dead_letters()
    assert len(dead) == 1 and dead[0].reason == "boom" and dead[0].queue == "q1"
    assert rt.queue_stats(["q1"])[0].dead == 1
    assert rt.list_jobs(states=[JobState.QUEUED], limit=10) == []


# ------------------------------------------------------------------ 一致性套件
def test_conformance_cluster(lua_mode):
    def factory():
        return RedisTransport(
            CLUSTER_URL, codec=JSONCodec(), prefix=f"taskmq:conf:{uuid.uuid4().hex}:",
            lua=lua_mode, cluster=True,
        )

    def cleanup(transport):
        transport.flush_prefix()
        transport.close()

    executed = transport_conformance(factory, cleanup=cleanup)
    skipped = [name for name in CONFORMANCE_SCENARIOS if name not in executed]
    assert skipped == ["global_priority"]            # 按 cluster_limitations 声明跳过
    assert {"priority_fifo", "atomic_claim", "lease_recovery", "dlq_replay", "job_listing"} <= set(
        executed
    )


# ------------------------------------------------------------------ 端到端
def test_app_end_to_end_over_cluster(prefix):
    url = f"{CLUSTER_URL}?prefix={prefix}&cluster=1"
    app = App(Config(transport=url, serializer="json", events="null", concurrency=4))
    order: list[str] = []
    lock = threading.Lock()

    @app.task(name="cluster.job", queue="cq")
    def job(name: str) -> str:
        with lock:
            order.append(name)
        return name

    handles = [app.submit("cluster.job", (f"t{index}",)) for index in range(20)]
    urgent = app.submit("cluster.job", ("urgent",), priority=9)

    with Worker(app, queues=["cq"]) as worker:
        worker.run_until_idle(timeout=60)

    assert all(handle.successful() for handle in handles)
    assert urgent.get(timeout=5) == "urgent"
    assert order[0] == "urgent"
    assert app.transport.queue_stats(["cq"])[0].pending == 0
    app.close()
