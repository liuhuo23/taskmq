"""执行池：asyncio（单事件循环）与 processes（真隔离 + 可强杀硬超时）。"""
from __future__ import annotations

import asyncio
import importlib
import time
from pathlib import Path

import pytest

from taskmq import App, Config
from taskmq.errors import ConfigError
from taskmq.testing import run_until_idle
from taskmq.worker.pool import ProcessPool, make_pool
from taskmq.worker.runner import Worker

SAMPLE = '''
from pathlib import Path

from taskmq import App, Config, Task

LOG = Path(r"{tmp}") / "pool_child.log"
DB = Path(r"{tmp}") / "pool.db"

app = App(Config(transport=f"sqlite:///{DB}", serializer="json", events="null", concurrency=1))


class CpuTask(Task):
    name = "pool.cpu"
    queue = "cpu"

    def before_start(self, ctx):
        with LOG.open("a", encoding="utf-8") as fh:
            fh.write("before " + ctx.id + " ")

    def run(self, n: int) -> int:
        return n * n


app.register(CpuTask)


@app.task(name="pool.hang", queue="cpu", hard_timeout=0.5)
def hang(seconds: float) -> str:
    import time

    time.sleep(seconds)
    return "finished"


@app.task(name="pool.boom", queue="cpu")
def boom() -> int:
    raise RuntimeError("child boom")
'''


def _sample_module(tmp_path: Path, monkeypatch, name: str):
    module_path = tmp_path / f"{name}.py"
    module_path.write_text(SAMPLE.replace("{tmp}", str(tmp_path)), encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    module = importlib.import_module(name)
    return module, f"{name}:app"


def _app(**overrides) -> App:
    config = {"transport": "memory://", "serializer": "json", "events": "null"}
    config.update(overrides)
    return App(Config(**config))


# ------------------------------------------------------------------ asyncio 池
def test_asyncio_pool_runs_async_tasks():
    app = _app(pool="asyncio", concurrency=2)

    @app.task(name="pool.async_add", queue="async")
    async def async_add(a: int, b: int) -> int:
        await asyncio.sleep(0.01)
        return a + b

    handles = [app.submit("pool.async_add", (index, 1)) for index in range(4)]
    run_until_idle(app, queues=["async"], timeout=30)

    assert [handle.get(timeout=2) for handle in handles] == [1, 2, 3, 4]
    assert all(handle.state == "SUCCEEDED" for handle in handles)


def test_asyncio_pool_also_runs_sync_tasks():
    app = _app(pool="asyncio", concurrency=2)

    @app.task(name="pool.sync_add", queue="async")
    def sync_add(a: int, b: int) -> int:
        return a + b

    handle = app.submit("pool.sync_add", (2, 3))
    run_until_idle(app, queues=["async"], timeout=30)
    assert handle.get(timeout=2) == 5


def test_async_task_in_threads_pool_fails_fast():
    app = _app(pool="threads")

    @app.task(name="pool.bad_async", queue="q")
    async def bad_async() -> int:
        return 1

    app.submit("pool.bad_async")
    with pytest.raises(ConfigError), Worker(app, queues=["q"]) as worker:
        worker.poll()


def test_hard_timeout_warns_on_non_process_pool():
    app = _app(pool="threads")

    @app.task(name="pool.hard_here", queue="q", hard_timeout=1.0)
    def hard_here() -> str:
        return "ok"

    with Worker(app, queues=["q"]) as worker:
        assert worker.poll() == 0          # 只是警告，不报错


# ------------------------------------------------------------------ processes 池
def test_process_pool_runs_task_in_child_with_hooks(tmp_path, monkeypatch):
    module, spec = _sample_module(tmp_path, monkeypatch, "pool_sample_cpu")
    pool = ProcessPool(concurrency=1, app_spec=spec)
    try:
        with Worker(module.app, queues=["cpu"], pool=pool, app_spec=spec) as worker:
            handle = module.app.submit("pool.cpu", (7,))
            worker.run_until_idle(timeout=60)
        assert handle.get(timeout=5) == 49
        log = (tmp_path / "pool_child.log").read_text(encoding="utf-8")
        assert "before" in log                 # 钩子确实在子进程里执行了
    finally:
        pool.shutdown()


def test_process_pool_propagates_child_exception(tmp_path, monkeypatch):
    module, spec = _sample_module(tmp_path, monkeypatch, "pool_sample_boom")
    pool = ProcessPool(concurrency=1, app_spec=spec)
    try:
        with Worker(module.app, queues=["cpu"], pool=pool, app_spec=spec) as worker:
            handle = module.app.submit("pool.boom")
            worker.run_until_idle(timeout=60)
        assert handle.state == "FAILED"
        dead = module.app.transport.dead_letters()
        assert len(dead) == 1 and "child boom" in dead[0].reason
    finally:
        pool.shutdown()


def test_process_pool_hard_timeout_kills_child(tmp_path, monkeypatch):
    module, spec = _sample_module(tmp_path, monkeypatch, "pool_sample_hang")
    pool = ProcessPool(concurrency=1, app_spec=spec)
    try:
        started = time.monotonic()
        with Worker(module.app, queues=["cpu"], pool=pool, app_spec=spec) as worker:
            handle = module.app.submit("pool.hang", (10.0,))
            worker.run_until_idle(timeout=60)
        elapsed = time.monotonic() - started

        assert elapsed < 5.0                    # 10s 的任务被 0.5s 硬超时 kill 掉
        assert handle.state == "FAILED"
        dead = module.app.transport.dead_letters()
        assert len(dead) == 1 and "hard_timeout" in dead[0].reason
        assert "HardTimeout" in (handle.info.get("error") or "")
    finally:
        pool.shutdown()


def test_process_pool_requires_app_spec():
    with pytest.raises(ConfigError):
        ProcessPool(concurrency=1, app_spec=None)


def test_make_pool_factories():
    assert make_pool("threads", 2).name == "threads"
    assert make_pool("asyncio", 2).name == "asyncio"
    assert isinstance(make_pool("solo", 1), object)
    with pytest.raises(ValueError):
        make_pool("nope", 1)
