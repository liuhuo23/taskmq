"""Redis 档内加权轮询：权重 3:1 的两个同级队列，取件比例应接近 3:1（Lua / 无 Lua 两种模式）。"""
from __future__ import annotations

import os
import uuid

import pytest

from taskmq import Envelope
from taskmq.protocol import JSONCodec
from taskmq.redis_client import RedisClient
from taskmq.transport.redis import RedisTransport

REDIS_URL = os.environ.get("TASKMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379/15")


def _redis_available() -> bool:
    try:
        return RedisClient(REDIS_URL, timeout=1.5).ping()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _redis_available(), reason=f"没有可用的 Redis（{REDIS_URL}）")


@pytest.mark.parametrize("lua", [True, False], ids=["lua", "no-lua"])
@pytest.mark.parametrize(
    "weights, expected", [({"q1": 3, "q2": 1}, (30, 10)), ({"q1": 1, "q2": 1}, (20, 20))]
)
def test_weighted_round_robin_within_priority_band(lua, weights, expected):
    prefix = f"taskmq:test:{uuid.uuid4().hex}:"
    transport = RedisTransport(REDIS_URL, codec=JSONCodec(), prefix=prefix, lua=lua)
    try:
        transport.set_queue_weights(weights)
        for _ in range(100):                        # 两个队列都灌满同级任务
            transport.enqueue(Envelope(task="t", priority=0), queue="q1")
            transport.enqueue(Envelope(task="t", priority=0), queue="q2")

        taken = {"q1": 0, "q2": 0}
        for _ in range(40):
            batch = transport.reserve(["q1", "q2"], worker_id="w", lease=30, limit=1)
            assert batch, "不该取空"
            taken[batch[0].queue] += 1
            transport.ack(batch[0])

        assert abs(taken["q1"] - expected[0]) <= 2, taken
        assert abs(taken["q2"] - expected[1]) <= 2, taken
    finally:
        transport.flush_prefix()
        transport.close()


def test_priority_still_beats_weights():
    """插队优先于公平性：低权重队列里的高优先级任务仍然先出。"""
    prefix = f"taskmq:test:{uuid.uuid4().hex}:"
    transport = RedisTransport(REDIS_URL, codec=JSONCodec(), prefix=prefix, lua=True)
    try:
        transport.set_queue_weights({"q1": 9, "q2": 1})
        for _ in range(5):
            transport.enqueue(Envelope(task="low", priority=0), queue="q1")
        transport.enqueue(Envelope(task="urgent", priority=9), queue="q2")
        granted = transport.reserve(["q1", "q2"], worker_id="w", lease=30, limit=1)
        assert granted and granted[0].envelope.task == "urgent"
    finally:
        transport.flush_prefix()
        transport.close()
