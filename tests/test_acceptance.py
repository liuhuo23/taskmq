"""Phase 0 验收（docs/design.md §19）：1000 任务 / 真·进程死亡重投 / DLQ 重放 / 插队。"""
from __future__ import annotations

import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

from taskmq import App, Config, Retry, current_task
from taskmq.protocol import JSONCodec
from taskmq.testing import run_until_idle
from taskmq.transport.sqlite import SqliteTransport
from taskmq.worker.runner import Worker

ROOT = Path(__file__).resolve().parents[1]


def _app(db_path, **overrides) -> App:
    config = {"transport": f"sqlite:///{db_path}", "serializer": "json"}
    config.update(overrides)
    return App(Config(**config))


def test_acceptance_1000_tasks_two_workers_over_sqlite(tmp_path):
    """无 Redis 环境下 1000 个任务并发跑完，且每个任务只执行一次。"""
    db = tmp_path / "bench.db"
    app_a = _app(db, concurrency=4)
    app_b = _app(db, concurrency=4)

    seen: list[int] = []
    lock = threading.Lock()

    def work(index: int) -> int:
        with lock:
            seen.append(index)
        return index * 2

    for app in (app_a, app_b):
        app.task(name="acceptance.bench", queue="bench")(work)

    handles = [app_a.submit("acceptance.bench", (index,)) for index in range(1000)]
    errors: list[BaseException] = []

    def drain(app: App) -> None:
        try:
            with Worker(app, queues=["bench"]) as worker:
                worker.run_until_idle(timeout=180)
        except BaseException as exc:  # noqa: BLE001 - 线程里收集错误
            errors.append(exc)

    threads = [threading.Thread(target=drain, args=(app,)) for app in (app_a, app_b)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(seen) == 1000
    assert len(set(seen)) == 1000                       # 没有重复执行
    assert all(handle.successful() for handle in handles)
    for index in (0, 1, 500, 999):
        assert handles[index].get(timeout=5) == index * 2
    assert app_a.transport.queue_stats(["bench"])[0].pending == 0


def test_acceptance_killed_process_leases_are_recovered(tmp_path):
    """worker 进程被 SIGKILL（os._exit）后，租约到期消息必须重新可见并被重新执行。"""
    db = tmp_path / "killed.db"
    script = textwrap.dedent(
        f"""
        import os, sys
        sys.path.insert(0, {str(ROOT)!r})
        from taskmq import App, Config

        app = App(Config(transport="sqlite:///{db}", serializer="json"))

        @app.task(name="acceptance.kill", queue="kill")
        def doomed():
            return "never"

        doomed.delay()
        claimed = app.transport.reserve(["kill"], worker_id="w-doomed", lease=0.2, limit=1)
        assert claimed, "reserve 失败"
        print("RESERVED", claimed[0].message_id, flush=True)
        os._exit(9)                  # 模拟 SIGKILL：不 ack、不清理、不写状态
        """
    )
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=120)
    assert proc.returncode == 9, proc.stderr
    assert "RESERVED" in proc.stdout

    # 父进程用「时间快进 61 秒」的时钟（死进程的租约只有 0.2s），不靠 sleep 碰运气
    ahead = time.time() + 61
    transport = SqliteTransport(str(db), codec=JSONCodec(), clock=lambda: ahead)
    app = App(Config(transport=transport, serializer="json", concurrency=1))
    deliveries: list[int] = []

    @app.task(name="acceptance.kill", queue="kill")
    def recovered():
        deliveries.append(current_task().deliveries)
        return "ok"

    run_until_idle(app, queues=["kill"], timeout=30)

    assert deliveries == [2]                       # 第二次投递（第一次被死掉的进程占着）
    assert app.transport.dead_letters() == []
    assert app.transport.queue_stats(["kill"])[0].pending == 0


def test_long_running_task_lease_is_renewed():
    """跑得比 lease 还久的任务不会被判孤儿：worker 在途续租（否则会重复执行）。"""
    app = App(
        Config(transport="memory://", serializer="json", events="null", concurrency=1, lease=0.3)
    )
    runs: list[int] = []

    @app.task(name="acceptance.long", queue="long")
    def long_task() -> str:
        runs.append(1)
        time.sleep(1.0)
        return "ok"

    handle = app.submit("acceptance.long")
    with Worker(app, queues=["long"], lease=0.3) as worker:
        worker.run_until_idle(timeout=30)

    assert handle.get(timeout=2) == "ok"
    assert len(runs) == 1                       # 没有被重新投递


def test_acceptance_dlq_then_replay(tmp_path):
    """超过重试上限 → DLQ；重放 → attempt 归 1 重新开始。"""
    db = tmp_path / "dlq.db"
    app = _app(db, concurrency=1)
    calls: list[int] = []

    @app.task(
        name="acceptance.flaky",
        queue="q",
        retry=Retry(max_attempts=2, base=0.0, jitter=False),
    )
    def flaky():
        calls.append(1)
        raise RuntimeError("always fails")

    handle = app.submit("acceptance.flaky")
    run_until_idle(app, queues=["q"], timeout=30)

    assert handle.state == "FAILED"
    assert len(calls) == 2                        # max_attempts=2
    dead = app.transport.dead_letters()
    assert len(dead) == 1
    assert handle.info["attempt"] == 2            # job 级尝试次数（重试是新的消息行）

    assert app.transport.replay_dead(dead[0].message_id) is True
    run_until_idle(app, queues=["q"], timeout=30)

    assert len(calls) == 4                        # 重放后又完整跑了两轮
    assert len(app.transport.dead_letters()) == 1


def test_acceptance_urgent_jumps_the_queue_over_sqlite(tmp_path):
    """跨进程 transport 上，claim 时高优先级先出（G1 的 transport 侧验收）。"""
    db = tmp_path / "jump.db"
    app = _app(db, concurrency=1)
    order: list[str] = []

    @app.task(name="acceptance.job", queue="jump")
    def job(name: str) -> str:
        order.append(name)
        return name

    for index in range(5):
        app.submit("acceptance.job", (f"low{index}",))
    app.submit("acceptance.job", ("urgent",), priority=9)

    run_until_idle(app, queues=["jump"], timeout=30)

    assert order[0] == "urgent"
    assert sorted(order[1:]) == [f"low{index}" for index in range(5)]
