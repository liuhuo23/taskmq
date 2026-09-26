"""Redis job 索引：`list_jobs` 走 `{prefix}jobs` ZSET（Redis Cluster 下 SCAN 只扫一个节点，不可用）。"""
from __future__ import annotations

import os
import uuid

import pytest

from taskmq import Envelope
from taskmq.protocol import JSONCodec
from taskmq.redis_client import RedisClient
from taskmq.transport.base import JobState
from taskmq.transport.redis import RedisTransport

REDIS_URL = os.environ.get("TASKMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379/15")


def _redis_available() -> bool:
    try:
        return RedisClient(REDIS_URL, timeout=1.5).ping()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _redis_available(), reason=f"没有可用的 Redis（{REDIS_URL}）")


@pytest.fixture
def prefix() -> str:
    return f"taskmq:test:{uuid.uuid4().hex}:"


@pytest.fixture
def rt(prefix):
    transport = RedisTransport(REDIS_URL, codec=JSONCodec(), prefix=prefix, lua=True)
    try:
        yield transport
    finally:
        transport.flush_prefix()
        transport.close()


def test_enqueued_jobs_are_indexed(rt):
    job_id = rt.enqueue(Envelope(task="t"), queue="q")
    record = rt.get_state(job_id)
    assert record is not None
    listed = rt.list_jobs()
    assert [r.job_id for r in listed] == [job_id]
    assert listed[0].state == JobState.QUEUED


def test_list_jobs_orders_by_updated_at_desc(rt):
    first = rt.enqueue(Envelope(task="t"), queue="q")
    second = rt.enqueue(Envelope(task="t"), queue="q")
    third = rt.enqueue(Envelope(task="t"), queue="q")
    rt.set_state(first, JobState.SUCCEEDED, task="t")          # 更新 → 排到最前
    listed = [record.job_id for record in rt.list_jobs()]
    assert listed[0] == first
    assert set(listed) == {first, second, third}


def test_list_jobs_filters_by_prefix_and_state(rt):
    rt.set_state("wf-1", JobState.RUNNING, task="workflow:demo")
    rt.set_state("wf-1::a", JobState.SUCCEEDED, task="t")
    rt.set_state("other", JobState.QUEUED, task="t")

    prefixed = {record.job_id for record in rt.list_jobs(prefix="wf-")}
    assert prefixed == {"wf-1", "wf-1::a"}
    running = [record.job_id for record in rt.list_jobs(prefix="wf-", states=[JobState.RUNNING])]
    assert running == ["wf-1"]
    assert len(rt.list_jobs(limit=1)) == 1


def test_list_jobs_is_empty_for_fresh_prefix(rt):
    assert rt.list_jobs() == []
    assert rt.list_jobs(prefix="wf-") == []
