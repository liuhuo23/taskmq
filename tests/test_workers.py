"""worker 心跳注册表：memory / sqlite / redis + Worker 生命周期 + CLI status。"""
from __future__ import annotations

import importlib
import os
import uuid
from pathlib import Path

import pytest

from taskmq import App, Config
from taskmq.protocol import JSONCodec
from taskmq.redis_client import RedisClient
from taskmq.transport.memory import MemoryTransport
from taskmq.transport.redis import RedisTransport
from taskmq.transport.sqlite import SqliteTransport
from taskmq.worker.runner import Worker

REDIS_URL = os.environ.get("TASKMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379/15")


class FakeClock:
    def __init__(self, start: float = 1_800_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_worker_registry_lifecycle(backend, tmp_path):
    clock = FakeClock()
    transport = (
        MemoryTransport(clock=clock)
        if backend == "memory"
        else SqliteTransport(str(tmp_path / "w.db"), codec=JSONCodec(), clock=clock)
    )
    try:
        assert transport.supports_workers is True
        assert transport.list_workers() == []

        transport.register_worker(
            "w1", queues=("a", "b"), pool="threads", concurrency=4, meta={"pid": 1}
        )
        info = transport.list_workers()[0]
        assert info.worker_id == "w1" and info.queues == ("a", "b")
        assert info.pool == "threads" and info.concurrency == 4 and info.meta["pid"] == 1
        assert info.alive(now=clock.now, stale_after=30) is True

        clock.advance(60)
        assert info.alive(now=clock.now, stale_after=30) is False            # 心跳过期

        transport.heartbeat_worker("w1", meta={"jobs": 3})
        refreshed = transport.list_workers()[0]
        assert refreshed.heartbeat_at == clock.now
        assert refreshed.meta["jobs"] == 3 and refreshed.meta["pid"] == 1    # meta 合并
        assert refreshed.alive(now=clock.now, stale_after=30) is True

        transport.deregister_worker("w1")
        assert transport.list_workers() == []

        transport.heartbeat_worker("w2")                                     # 未知 worker 自动登记
        assert [item.worker_id for item in transport.list_workers()] == ["w2"]
    finally:
        transport.close()


def test_worker_registers_on_poll_and_deregisters_on_close():
    app = App(Config(transport="memory://", serializer="json", events="null", concurrency=2))

    @app.task(name="w.job", queue="q")
    def job() -> str:
        return "ok"

    with Worker(app, queues=["q"]) as worker:
        worker.poll()
        workers = app.transport.list_workers()
        assert [item.worker_id for item in workers] == [worker.worker_id]
        assert workers[0].queues == ("q",)
        assert workers[0].pool == "threads"
        assert workers[0].concurrency == 2
        assert workers[0].meta["pid"] == os.getpid()

    assert app.transport.list_workers() == []                                # 优雅退出注销


def test_worker_heartbeat_events_are_emitted():
    from taskmq.events import CollectingSink

    app = App(Config(transport="memory://", serializer="json", events="null"))
    sink = CollectingSink()
    app.add_sink(sink)

    @app.task(name="w.job2", queue="q")
    def job() -> str:
        return "ok"

    with Worker(app, queues=["q"]) as worker:
        worker.poll()
    names = sink.names()
    assert "worker.started" in names and "worker.stopped" in names
    assert sink.of("worker.started")[0]["worker"] == worker.worker_id


# ------------------------------------------------------------------ Redis
def _redis_available() -> bool:
    try:
        return RedisClient(REDIS_URL, timeout=1.5).ping()
    except Exception:
        return False


@pytest.mark.skipif(not _redis_available(), reason=f"没有可用的 Redis（{REDIS_URL}）")
def test_redis_worker_registry():
    prefix = f"taskmq:test:{uuid.uuid4().hex}:"
    transport = RedisTransport(REDIS_URL, codec=JSONCodec(), prefix=prefix, lua=False)
    try:
        transport.register_worker("rw1", queues=("q",), pool="threads", concurrency=8)
        info = transport.list_workers()[0]
        assert info.worker_id == "rw1" and info.queues == ("q",) and info.concurrency == 8

        transport.heartbeat_worker("rw1")
        transport.deregister_worker("rw1")
        assert transport.list_workers() == []
    finally:
        client = RedisClient(REDIS_URL, timeout=3)
        client.delete_prefix(prefix.rstrip(":"))
        client.close()
        transport.close()


# ------------------------------------------------------------------ CLI
def test_cli_status_lists_workers(tmp_path: Path, monkeypatch, capsys):
    module_path = tmp_path / "worker_cli_app.py"
    module_path.write_text(
        "from taskmq import App, Config\n"
        "\n"
        "app = App(Config(transport='memory://', serializer='json', events='null', concurrency=3))\n"
        "\n"
        "\n"
        "@app.task(name='w.status_job', queue='q')\n"
        "def job() -> str:\n"
        "    return 'ok'\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    module = importlib.import_module("worker_cli_app")

    from taskmq.cli import main

    with Worker(module.app, queues=["q"]) as worker:
        worker.poll()
        assert main(["--app", "worker_cli_app:app", "status", "-Q", "q"]) == 0
        out = capsys.readouterr().out
        assert "WORKERS" in out
        assert worker.worker_id in out
        assert "concurrency=3" in out
