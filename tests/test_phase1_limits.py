"""Phase 1：限流 / concurrency_key 互斥 / defer / 结构化事件。"""
from __future__ import annotations

import threading
import time

import pytest

from taskmq import App, Config
from taskmq.errors import ConfigError
from taskmq.events import CollectingSink
from taskmq.protocol import JSONCodec
from taskmq.ratelimit import RateLimit, TokenBucket
from taskmq.testing import run_until_idle
from taskmq.transport.sqlite import SqliteTransport
from taskmq.worker.runner import Worker


def _app(**overrides) -> App:
    config = {"transport": "memory://", "serializer": "json", "events": "null", "concurrency": 4}
    config.update(overrides)
    return App(Config(**config))


# --------------------------------------------------------------------- 限流
def test_rate_limit_parsing_and_errors():
    assert RateLimit.parse("100/m") == RateLimit(limit=100, window=60.0)
    assert RateLimit.parse("10/s").rate == 10.0
    assert RateLimit.parse("1000/h").window == 3600.0
    for bad in ("100", "100/x", "0/s", "abc", 5):
        with pytest.raises(ConfigError):
            RateLimit.parse(bad)  # type: ignore[arg-type]


def test_token_bucket_allows_burst_then_throttles():
    clock = {"now": 0.0}
    bucket = TokenBucket(RateLimit.parse("2/s"), clock=lambda: clock["now"])
    assert bucket.acquire() is None                 # 突发：桶是满的
    assert bucket.acquire() is None
    wait = bucket.acquire()
    assert wait is not None and wait == pytest.approx(0.5, abs=0.01)
    clock["now"] += 0.5
    assert bucket.acquire() is None


def test_worker_rate_limit_defers_instead_of_failing():
    app = _app(concurrency=1, poll_interval=0.01)
    runs: list[float] = []

    @app.task(name="phase1.limited", rate_limit="2/s", queue="limited")
    def limited(index: int) -> int:
        runs.append(time.monotonic())
        return index

    handles = [app.submit("phase1.limited", (index,)) for index in range(4)]
    started = time.monotonic()
    with Worker(app, queues=["limited"]) as worker:
        worker.run_until_idle(timeout=30)
        deferred = worker.deferred

    assert all(handle.successful() for handle in handles)
    assert deferred >= 2                                    # 后两个被限流推后
    assert len(runs) == 4
    assert runs[-1] - started >= 0.8                        # 2/s：第 3、4 个必须等到下一轮令牌
    assert handles[0].info["attempt"] == 1                  # 被限流不算重试


# ------------------------------------------------------- concurrency_key 互斥
def test_concurrency_key_serialises_same_key_in_one_worker():
    app = _app(concurrency=4)
    state = {"active": 0, "max": 0}
    lock = threading.Lock()

    @app.task(name="phase1.serial", concurrency_key="user:{user}", queue="serial")
    def serial(user: str) -> str:
        with lock:
            state["active"] += 1
            state["max"] = max(state["max"], state["active"])
        time.sleep(0.05)
        with lock:
            state["active"] -= 1
        return user

    handles = [app.submit("phase1.serial", (), {"user": "u1"}) for _ in range(4)]
    with Worker(app, queues=["serial"]) as worker:
        worker.run_until_idle(timeout=30)

    assert all(handle.successful() for handle in handles)
    assert state["max"] == 1                                # 同 key 内绝不并发


def test_concurrency_key_requires_lease_capable_transport():
    app = _app(concurrency=1)

    @app.task(name="phase1.keyed", concurrency_key="k", queue="q")
    def keyed() -> str:
        return "ok"

    app.submit("phase1.keyed")
    app.transport.supports_leases = False                   # type: ignore[attr-defined]
    with pytest.raises(ConfigError), Worker(app, queues=["q"]) as worker:
        worker.poll()


def test_concurrency_key_template_error_is_fail_fast():
    app = _app()

    @app.task(name="phase1.needs_user", concurrency_key="user:{user}", queue="q")
    def needs_user(name: str) -> str:
        return name

    with pytest.raises(ConfigError):
        app.submit("phase1.needs_user", ("no-kwargs",))


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_named_lease_semantics(backend, tmp_path):
    if backend == "memory":
        transport = App(Config(transport="memory://", serializer="json", events="null")).transport
    else:
        transport = SqliteTransport(str(tmp_path / "lease.db"), codec=JSONCodec())

    assert transport.supports_leases is True
    assert transport.acquire_lease("ck:u1", "w1", 10.0) is True
    assert transport.acquire_lease("ck:u1", "w2", 10.0) is False   # 别人持有
    assert transport.acquire_lease("ck:u1", "w1", 10.0) is True    # 自己可续
    assert transport.renew_lease("ck:u1", "w2", 10.0) is False
    transport.release_lease("ck:u1", "w2")                          # 非持有者释放无效
    assert transport.acquire_lease("ck:u1", "w2", 10.0) is False
    transport.release_lease("ck:u1", "w1")
    assert transport.acquire_lease("ck:u1", "w2", 10.0) is True
    transport.close()


def test_defer_does_not_consume_a_delivery(tmp_path):
    transport = SqliteTransport(str(tmp_path / "defer.db"), codec=JSONCodec())
    from taskmq import Envelope

    transport.enqueue(Envelope(task="t", queue="q"))
    delivery = transport.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
    assert delivery.deliveries == 1

    transport.defer(delivery, delay=0.0)
    again = transport.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
    assert again.deliveries == 1                            # 推迟不算一次投递
    transport.close()


def test_concurrency_key_across_two_workers_over_sqlite(tmp_path):
    """两个 App（各自连接）共享同一个 key：仍然串行（命名租约跨连接互斥）。"""
    db = tmp_path / "cross.db"
    apps = [
        App(Config(transport=f"sqlite:///{db}", serializer="json", events="null", concurrency=2))
        for _ in range(2)
    ]
    state = {"active": 0, "max": 0}
    lock = threading.Lock()

    def body(user: str) -> str:
        with lock:
            state["active"] += 1
            state["max"] = max(state["max"], state["active"])
        time.sleep(0.05)
        with lock:
            state["active"] -= 1
        return user

    for app in apps:
        app.task(name="phase1.cross", queue="cross", concurrency_key="user:{user}")(body)

    handles = [apps[0].submit("phase1.cross", (), {"user": "u1"}) for _ in range(4)]
    workers = [Worker(apps[0], queues=["cross"]), Worker(apps[1], queues=["cross"])]
    errors: list[BaseException] = []

    def drain(worker: Worker) -> None:
        try:
            worker.run_until_idle(timeout=60)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=drain, args=(worker,)) for worker in workers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert all(handle.successful() for handle in handles)
    assert state["max"] == 1


# --------------------------------------------------------------------- 事件
def test_events_sequence_for_success_and_failure():
    app = _app(concurrency=1)
    sink = CollectingSink()
    app.add_sink(sink)

    @app.task(name="phase1.ok", queue="events")
    def ok() -> str:
        return "fine"

    @app.task(name="phase1.bad", queue="events", retry=None)
    def bad() -> None:
        raise RuntimeError("nope")

    good = app.submit("phase1.ok")
    failed = app.submit("phase1.bad")
    run_until_idle(app, queues=["events"], timeout=30)

    assert good.successful() and failed.state == "FAILED"
    names = sink.names()
    assert names.count("task.submitted") == 2
    assert names.count("task.started") == 2
    assert names.count("task.succeeded") == 1
    assert names.count("task.failed") == 1
    succeeded = sink.of("task.succeeded")[0]
    assert succeeded["job_id"] == good.id and succeeded["attempt"] == 1
    assert sink.of("task.failed")[0]["job_id"] == failed.id
    assert sink.of("task.submitted")[0]["queue"] == "events"


def test_events_include_defer_reason():
    app = _app(concurrency=1, poll_interval=0.01)
    sink = CollectingSink()
    app.add_sink(sink)

    @app.task(name="phase1.slow_rate", rate_limit="1/s", queue="ev")
    def slow_rate() -> str:
        return "ok"

    for _ in range(3):
        app.submit("phase1.slow_rate")
    with Worker(app, queues=["ev"]) as worker:
        worker.run_until_idle(timeout=30)

    deferred = sink.of("task.deferred")
    assert deferred and deferred[0]["reason"] == "rate_limit"
    assert deferred[0]["delay"] > 0


def test_events_config_validation():
    """值必须是非空字符串；名字合法性由 events.build_sink 在 App 构造时判定（插件可注册 sink）。"""
    with pytest.raises(ConfigError):
        Config(events="").validate()
    with pytest.raises(ConfigError):
        Config(events=123).validate()      # 类型不对 → 直接报错
    with pytest.raises(ConfigError):            # 未知 sink → App 构造即失败（fail fast）
        App(Config(transport="memory://", events="kafka"))
