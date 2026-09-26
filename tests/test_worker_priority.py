"""插队语义验收：G1 空闲即最高 / G2 未开始让位 / G3 不打断运行中（§20-9）。"""
from __future__ import annotations

import threading
import time

from taskmq import App, Config, Priority, QueueConfig
from taskmq.testing import worker_for


def _wait_until(predicate, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def _app(**overrides):
    config = {
        "transport": "memory://",
        "serializer": "json",
        "pool": "threads",
        "concurrency": 1,
        "poll_interval": 0.005,
    }
    config.update(overrides)
    return App(Config(**config))


def _short_name(delivery) -> str:
    return delivery.envelope.task.rsplit(".", 1)[-1]


def test_g1_next_free_slot_runs_highest_priority():
    app = _app()
    order: list[str] = []
    gate = threading.Event()

    @app.task
    def slow(name):
        order.append(name)
        gate.wait(5)
        return name

    @app.task
    def quick(name):
        order.append(name)
        return name

    slow.delay("slow")
    quick.delay("normal")

    with worker_for(app, concurrency=1) as worker:
        worker.poll()
        assert _wait_until(lambda: order == ["slow"])        # 唯一槽位被 slow 占住

        urgent = quick.apply_async(("urgent",), priority=Priority.CRITICAL)
        worker.poll()
        assert "urgent" not in order                         # G3：不打断正在执行的 slow

        gate.set()
        worker.run_until_idle(timeout=10)

    assert order == ["slow", "urgent", "normal"]             # G1：下一个空槽位给最高优先级
    assert urgent.get(timeout=1) == "urgent"


def test_g2_prefetch_hold_ahead_yields_unstarted_reservations():
    app = _app(yield_delay=0.02)
    order: list[str] = []
    gate = threading.Event()

    @app.task
    def blocker():
        order.append("blocker")
        gate.wait(5)
        return "blocked"

    @app.task
    def normal(name):
        order.append(name)
        return name

    @app.task
    def urgent():
        order.append("urgent")
        return "urgent"

    blocker.delay()
    for index in range(3):
        normal.delay(f"n{index}")

    with worker_for(app, concurrency=1, prefetch=4) as worker:
        worker.poll()
        assert _wait_until(lambda: order == ["blocker"])
        assert len(worker.pending_deliveries()) == 3         # 预取 4 条、只启动 1 条

        urgent_handle = urgent.apply_async((), priority=Priority.CRITICAL)
        worker.poll()
        assert worker.yielded == 3                           # 未开始的低优全部让位（G2/P12）
        assert [_short_name(d) for d in worker.pending_deliveries()] == ["urgent"]

        gate.set()
        worker.run_until_idle(timeout=10)

    assert order[0] == "blocker"
    assert order[1] == "urgent"                              # G1 + G2：紧急任务第二个跑
    assert sorted(order[2:]) == ["n0", "n1", "n2"]           # 被让位的消息之后正常跑完
    assert app.transport.dead_letters() == []                # P13：让位不会把人送进 DLQ
    assert urgent_handle.get(timeout=1) == "urgent"


def test_submit_priority_overrides_task_and_queue_and_config():
    app = _app(default_priority=1, queues={"q": QueueConfig(priority=2)})

    @app.task(queue="q")
    def plain():
        return "plain"

    @app.task(queue="q", priority=4)
    def important():
        return "important"

    assert app.resolve_priority(plain, queue="q") == 2                     # queue fallback
    assert app.resolve_priority(important, queue="q") == 4                 # task
    assert app.resolve_priority(important, queue="q", priority=7) == 7     # submit 覆盖

    @app.task(queue="other")
    def elsewhere():
        return "elsewhere"

    assert app.resolve_priority(elsewhere, queue="other") == 1             # config 默认

    handle = important.delay()
    record = app.transport.get_state(handle.id)
    assert record is not None and record.meta["priority"] == 4

    urgent = important.apply_async((), priority=Priority.MAX)
    urgent_record = app.transport.get_state(urgent.id)
    assert urgent_record is not None and urgent_record.meta["priority"] == 9
