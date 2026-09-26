"""针对性单元测试：补齐分支覆盖（不是行为主线的重复）。"""
from __future__ import annotations

import concurrent.futures
import dataclasses
import importlib
import json
import logging
import multiprocessing
import sys
import threading
import time
from pathlib import Path

import pytest

from taskmq import App, Config, Envelope
from taskmq.config import QueueConfig
from taskmq.errors import (
    ConfigError,
    DecodeError,
    EncodeError,
    MessageNotFound,
    ProtocolError,
    TaskError,
    TaskMQError,
    TaskTimeout,
    TransportError,
)
from taskmq.events import CollectingSink
from taskmq.protocol import (
    CodecRegistry,
    JSONCodec,
    decode_payload,
    encode_value,
    get_codec,
    new_ulid,
    ulid_timestamp,
)
from taskmq.task import Retry, RetryRequest, Task, TaskContext, current_app, current_task, run_hook
from taskmq.transport.base import UNSET, Delivery, JobRecord, JobState, Transport
from taskmq.transport.memory import MemoryTransport

if sys.version_info >= (3, 10):     # 3.10+ 用标准库
    from typing import ParamSpec
else:                               # pragma: no cover - 3.9
    from typing_extensions import ParamSpec

#: 测试里 Task 的参数位用 ParamSpec 透传，别用 ... —— Task[_P, str] 在 3.9 上会 TypeError
_P = ParamSpec("_P")


def _write_module(tmp_path: Path, monkeypatch, name: str = "unit_app") -> str:
    path = tmp_path / f"{name}.py"
    path.write_text(
        "from taskmq import App, Config\n"
        "app = App(Config(transport='memory://', serializer='json', events='null'))\n"
        "\n"
        "\n"
        "@app.task(name='mod.job', queue='q')\n"
        "def job() -> str:\n"
        "    return 'ok'\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    return f"{name}:app"


# ==================================================================== config
@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"transport": "nope"}, "scheme"),
        ({"transport": 123}, "URL 字符串"),
        ({"prefetch": 0}, "prefetch"),
        ({"lease": 0.0}, "lease"),
        ({"max_message_bytes": 16}, "max_message_bytes"),
        ({"log_format": "xml"}, "log_format"),
        ({"result": "sqlite:///r.db"}, "result_ttl"),
        ({"result_ttl": 0.0}, "result_ttl"),
        ({"yield_min_delta": 0}, "yield_min_delta"),
        ({"yield_delay": -1.0}, "yield_delay"),
        ({"max_yields": 0}, "max_yields"),
        ({"retry_priority": "bogus"}, "retry_priority"),
        ({"max_deliveries": 0}, "max_deliveries"),
        ({"concurrency": 0}, "concurrency"),
    ],
)
def test_config_rejects_bad_values(kwargs, match):
    with pytest.raises(ConfigError, match=match):
        Config(**kwargs).validate()


def test_queue_weight_and_queue_config_validation():
    config = Config(transport="memory://", queues={"q": QueueConfig(weight=4)})
    config.validate()
    assert config.queue_weight("q") == 4
    assert config.queue_weight("missing") == 1
    with pytest.raises(ConfigError, match="QueueConfig"):
        Config(transport="memory://", queues={"q": "nope"}).validate()
    with pytest.raises(ConfigError, match="weight"):
        Config(transport="memory://", queues={"q": QueueConfig(weight=0)}).validate()


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("1", True),
        ("TRUE", True),
        ("yes", True),
        ("on", True),
        ("0", False),
        ("false", False),
        ("no", False),
        ("off", False),
    ],
)
def test_from_env_parses_bools(raw, expected):
    assert Config.from_env({"TASKMQ_EAGER": raw}).eager is expected


def test_from_env_errors_and_empty_values():
    with pytest.raises(ConfigError, match="TASKMQ_CONCURRENCY"):
        Config.from_env({"TASKMQ_CONCURRENCY": "abc"})
    with pytest.raises(ConfigError, match="TASKMQ_EAGER"):
        Config.from_env({"TASKMQ_EAGER": "maybe"})
    assert Config.from_env({"TASKMQ_CONCURRENCY": ""}).concurrency == 4
    assert Config.from_env({"TASKMQ_CONCURRENCY": "8", "TASKMQ_TRANSPORT": "memory://"}).concurrency == 8


# =================================================================== protocol
@pytest.mark.parametrize("value", ["", "not-a-ulid", "01H"])
def test_ulid_timestamp_rejects(value):
    with pytest.raises(ValueError, match="ULID"):
        ulid_timestamp(value)


@pytest.mark.parametrize(
    "extra, match",
    [
        ({"priority": 1.5}, "priority"),
        ({"ack": "nope"}, "ack"),
        ({"attempt": 0}, "attempt"),
        ({"max_attempts": 0}, "max_attempts"),
        ({"eta": "soon"}, "eta"),
        ({"expires_at": "soon"}, "expires_at"),
        ({"key": 1}, "key"),
        ({"concurrency_key": 1}, "concurrency_key"),
        ({"trace": [1]}, "trace"),
        ({"headers": ["x"]}, "headers"),
        ({"args": "not-a-list"}, "args"),
        ({"kwargs": "not-a-dict"}, "kwargs"),
    ],
)
def test_decode_payload_rejects_invalid_fields(extra, match):
    payload = {"v": 1, "task": "t", "id": new_ulid(), "args": [], "kwargs": {}, **extra}
    with pytest.raises(DecodeError, match=match):
        decode_payload(payload)


@pytest.mark.parametrize(
    "payload, match",
    [
        ([], "必须是对象"),
        ({"task": ""}, "task"),
        ({"task": "t"}, "id"),
        ({"task": "t", "id": ""}, "id"),
    ],
)
def test_envelope_from_dict_rejects_invalid_envelope(payload, match):
    with pytest.raises(DecodeError, match=match):
        Envelope.from_dict(payload)


@pytest.mark.parametrize(
    "payload, match",
    [
        ({}, "协议版本"),
        ({"v": 999, "task": "t", "id": new_ulid()}, "协议版本"),
    ],
)
def test_decode_payload_version_guard(payload, match):
    with pytest.raises(DecodeError, match=match):
        decode_payload(payload)


def test_codec_registry_errors_and_mro_lookup():
    registry = CodecRegistry()
    with pytest.raises(ProtocolError, match="必须是类型"):
        registry.register("not-a-type", lambda v: v, lambda v: v)  # type: ignore[arg-type]

    class Base:
        pass

    class Child(Base):
        pass

    registry.register(Base, lambda v: "base", lambda v: Base(), tag="unit.Base")
    with pytest.raises(ProtocolError, match="已被"):
        registry.register(Child, lambda v: "x", lambda v: Child(), tag="unit.Base")
    assert len(registry) == 1
    assert registry.get_by_type(Base) is not None
    assert registry.get_by_type(Child) is not None          # 走 MRO
    assert registry.get_by_type(int) is None
    assert registry.get_by_tag("unit.Base") is not None
    assert registry.get_by_tag("nope") is None


def test_encode_value_rejects_non_str_key():
    with pytest.raises(EncodeError, match="key 必须是 str"):
        encode_value({1: "x"})


def test_codec_size_limit_and_unknown_codec():
    with pytest.raises(EncodeError, match="max_message_bytes"):
        JSONCodec().encode(Envelope(task="t"), max_bytes=8)
    with pytest.raises(TaskMQError, match="序列化器"):
        get_codec("bogus")
    assert get_codec("json") is not None and get_codec("msgspec") is not None


def test_decode_payload_requires_mapping():
    with pytest.raises(DecodeError, match="对象"):
        decode_payload("nope")  # type: ignore[arg-type]


# ======================================================================= task
def test_retry_policy_validation_and_branches():
    with pytest.raises(ValueError, match="max_attempts"):
        Retry(max_attempts=0)
    with pytest.raises(ValueError, match="backoff"):
        Retry(backoff="bogus")
    with pytest.raises(ValueError, match="不能为负"):
        Retry(base=-1)
    with pytest.raises(ValueError, match="retry_on"):
        Retry(retry_on=())

    from taskmq.task import Reject

    policy = Retry(backoff="fixed", base=2.0, jitter=False)
    assert policy.should_retry(RuntimeError("x")) is True
    assert policy.should_retry(Reject("nope")) is False
    assert policy.delay_for(3) == 2.0                                   # fixed
    assert Retry(backoff="linear", base=1.0, jitter=False).delay_for(3) == 3.0
    assert Retry(backoff="exp", base=1.0, factor=2.0, jitter=False).delay_for(4) == 8.0
    capped = Retry(backoff="exp", base=1.0, factor=10.0, max_delay=5.0, jitter=False)
    assert capped.delay_for(4) == 5.0                                   # 上限
    jittered = Retry(base=4.0, jitter=True).delay_for(1)
    assert 2.0 <= jittered <= 6.0                                       # uniform(0.5, 1.5)


def test_run_hook_swallows_hook_errors():
    app = App(Config(transport="memory://", events="null"))

    class Boom(Task[_P, str]):
        name = "unit.boom"

        def on_success(self, ctx: TaskContext, retval: object) -> None:
            raise RuntimeError("hook exploded")

        def run(self, *args: object, **kwargs: object) -> str:
            return "ok"

    task = Boom(app=app)
    ctx = _ctx(app)
    run_hook(task, "on_success", ctx, "value")                          # 不抛
    assert run_hook(task, "missing_hook", ctx) is None


def test_current_task_and_app_outside_context():
    with pytest.raises(TaskError, match="不在任务执行上下文"):
        current_task()
    with pytest.raises(TaskError, match="不在任务执行上下文"):
        current_app()


def _ctx(app: App, **overrides: object) -> TaskContext:
    values: dict[str, object] = {
        "app": app,
        "envelope": Envelope(task="unit.ctx", deadline=time.time() - 1),
        "worker_id": "w",
        "attempt": 1,
        "deliveries": 1,
        "priority": 0,
        "queue": "q",
        "deadline": time.time() - 1,
        "log": logging.getLogger("unit"),
    }
    values.update(overrides)
    return TaskContext(**values)  # type: ignore[arg-type]


def test_task_context_deadline_and_cancellation_checks():
    app = App(Config(transport="memory://", events="null"))
    ctx = _ctx(app)
    assert ctx.remaining() == 0 or ctx.remaining() < 0
    with pytest.raises(TaskTimeout):
        ctx.check_timeout()

    fresh = _ctx(app, deadline=None)
    assert fresh.remaining() is None
    fresh.check_timeout()                                               # 无 deadline 不抛

    cancelled = threading.Event()
    cancelled.set()
    with pytest.raises(TaskError, match="取消"):
        _ctx(app, cancelled=cancelled).check_cancelled()


# ================================================================== execution
def test_body_outcome_timeout_helper():
    from taskmq.worker.execution import BodyOutcome

    outcome = BodyOutcome.timeout(1.5)
    assert outcome.ok is False and outcome.state == JobState.FAILED
    assert "1.5" in outcome.error


def test_safe_hook_reports_error_text():
    from taskmq.worker.execution import _safe_hook

    app = App(Config(transport="memory://", events="null"))

    class Boom(Task[_P, str]):
        name = "unit.hook"

        def on_failure(self, ctx: TaskContext, exc: BaseException) -> None:
            raise RuntimeError("bad hook")

        def run(self, *args: object, **kwargs: object) -> str:
            return "ok"

    task = Boom(app=app)
    ctx = _ctx(app)
    assert _safe_hook(task, "on_failure", ctx, RuntimeError("x")).startswith("on_failure:")
    assert _safe_hook(task, "no_such_hook", ctx) == ""


@pytest.mark.parametrize(
    "exc, ack_mode, attempt, deliveries, expected",
    [
        (RetryRequest(), "on_success", 1, 1, 0.0),
        (RetryRequest(delay=3.0), "on_success", 1, 1, 3.0),
        (RetryRequest(), "on_completion", 1, 1, None),
        (RetryRequest(), "on_success", 1, 99, None),          # deliveries 超限
        (RuntimeError("x"), "on_success", 1, 1, 0.0),         # 走策略
    ],
)
def test_decide_retry_branches(exc, ack_mode, attempt, deliveries, expected):
    from taskmq.worker.execution import decide_retry

    app = App(Config(transport="memory://", events="null"))

    class Job(Task[_P, str]):
        name = "unit.retry"
        retry_policy = Retry(backoff="fixed", base=0.0, jitter=False)

        def run(self, *args: object, **kwargs: object) -> str:
            return "ok"

    task = Job(app=app)
    delay = decide_retry(task, attempt, deliveries, exc, ack_mode=ack_mode)
    assert delay is None if expected is None else delay == pytest.approx(expected)


def test_decide_retry_without_policy_and_exhausted():
    from taskmq.worker.execution import decide_retry

    app = App(Config(transport="memory://", events="null"))

    class Plain(Task[_P, str]):
        name = "unit.plain"
        retry_policy = None

        def run(self, *args: object, **kwargs: object) -> str:
            return "ok"

    assert decide_retry(Plain(app=app), 1, 1, RuntimeError("x"), ack_mode="on_success") is None

    class Limited(Task[_P, str]):
        name = "unit.limited"
        retry_policy = Retry(max_attempts=2, jitter=False)

        def run(self, *args: object, **kwargs: object) -> str:
            return "ok"

    assert decide_retry(Limited(app=app), 5, 1, RuntimeError("x"), ack_mode="on_success") is None


def test_run_child_task_executes_and_reports_unregistered(tmp_path, monkeypatch):
    from taskmq.worker import execution as execution_mod
    from taskmq.worker.execution import ChildTask

    monkeypatch.setattr(execution_mod, "_CHILD_APP", None, raising=False)
    spec = _write_module(tmp_path, monkeypatch)
    payload = ChildTask(
        app_spec=spec, envelope=Envelope(task="mod.job"), worker_id="w", attempt=1, deliveries=1
    )
    outcome = execution_mod.run_child_task(payload)
    assert outcome.ok is True and outcome.result == "ok"

    unknown = dataclasses.replace(payload, envelope=Envelope(task="mod.missing"))
    failed = execution_mod.run_child_task(unknown)
    assert failed.ok is False and failed.error_type == "TaskNotRegistered"

    execution_mod.init_child(spec)                                       # 幂等且已初始化
    assert execution_mod.run_child_task(payload).ok is True


# ====================================================================== pool
def test_pool_edge_paths(tmp_path, monkeypatch):
    from taskmq.worker.execution import ChildTask
    from taskmq.worker.pool import ProcessPool, SoloPool, _child_main, _InProcessPool

    assert SoloPool().call_body(lambda x: x + 1, (1,), {}) == 2
    SoloPool().submit(lambda: None)                                      # 立即执行
    SoloPool().shutdown()

    payload = ChildTask(
        app_spec="mod:app", envelope=Envelope(task="mod.job"), worker_id="w", attempt=1, deliveries=1
    )
    with pytest.raises(ConfigError, match="不支持子进程"):
        _InProcessPool().run_remote(payload)
    with pytest.raises(ConfigError, match="不支持子进程"):
        SoloPool().run_remote(payload)

    spec = _write_module(tmp_path, monkeypatch, name="pool_app")
    real = dataclasses.replace(payload, app_spec=spec)
    parent, child = multiprocessing.get_context("spawn").Pipe(duplex=False)
    _child_main(real, child)
    outcome = parent.recv()
    assert outcome.ok is True and outcome.result == "ok"

    from taskmq.worker import execution as execution_mod

    monkeypatch.setattr(execution_mod, "_CHILD_APP", None, raising=False)
    pool = ProcessPool(1, app_spec=spec)
    try:
        monkeypatch.setattr(
            pool._executor,
            "submit",
            lambda *a, **k: (_ for _ in ()).throw(
                concurrent.futures.process.BrokenProcessPool("broken")
            ),
        )
        broken = pool.run_remote(real)
        assert broken.ok is False and broken.error_type == "BrokenProcessPool"

        monkeypatch.setattr(
            pool._executor,
            "submit",
            lambda *a, **k: (_ for _ in ()).throw(TypeError("cannot pickle")),
        )
        unpicklable = pool.run_remote(real)
        assert unpicklable.ok is False and unpicklable.error_type == "TypeError"
    finally:
        pool.shutdown()


def test_process_pool_requires_app_spec():
    from taskmq.worker.pool import ProcessPool

    with pytest.raises(ConfigError, match="app_spec"):
        ProcessPool(2, app_spec=None)


# ============================================================ transport base
class _MiniTransport(Transport):
    """最小实现：只用来触发 base 的默认方法。"""

    def __init__(self) -> None:
        self.nacked: list[tuple[int, bool, float]] = []
        self.closed = False

    def enqueue(self, env, *, queue=None, delay=0.0, priority=None):  # type: ignore[no-untyped-def]
        return env.id

    def reserve(self, queues, *, worker_id, lease, limit):  # type: ignore[no-untyped-def]
        return []

    def ack(self, delivery) -> None:  # type: ignore[no-untyped-def]
        return None

    def nack(self, delivery, *, requeue=True, delay=0.0):  # type: ignore[no-untyped-def]
        self.nacked.append((delivery.message_id, requeue, delay))

    def dead_letter(self, delivery, reason) -> None:  # type: ignore[no-untyped-def]
        return None

    def extend_lease(self, delivery, seconds) -> None:  # type: ignore[no-untyped-def]
        return None

    def set_state(self, job_id, state, **kwargs):  # type: ignore[no-untyped-def]
        raise NotImplementedError

    def get_state(self, job_id):  # type: ignore[no-untyped-def]
        return None

    def queue_stats(self, queues=None):  # type: ignore[no-untyped-def]
        return []

    def reap_expired_leases(self, now=None) -> int:  # type: ignore[no-untyped-def]
        return 0

    def reap_expired_jobs(self, now=None) -> int:  # type: ignore[no-untyped-def]
        return 0

    def close(self) -> None:
        self.closed = True


def test_transport_base_defaults():
    assert bool(UNSET) is False
    done = JobRecord(job_id="j", task="t", state=JobState.SUCCEEDED, attempt=1)
    assert done.terminal is True
    assert dataclasses.replace(done, state=JobState.QUEUED).terminal is False

    transport = _MiniTransport()
    assert transport.supports_leases is False
    assert transport.supports_workers is False
    assert transport.peek_max_priority(["q"]) is None
    assert transport.yield_reservation(_delivery()) is False
    assert transport.next_visible_at(["q"]) is None
    assert transport.dead_letters() == []
    assert transport.replay_dead(1) is False
    assert transport.priority_stats() == {}
    assert transport.list_workers() == []

    for call in (
        lambda: transport.acquire_lease("n", "o", 1.0),
        lambda: transport.release_lease("n", "o"),
        lambda: transport.renew_lease("n", "o", 1.0),
        lambda: transport.register_worker("w"),
        lambda: transport.heartbeat_worker("w"),
        lambda: transport.deregister_worker("w"),
    ):
        with pytest.raises(TransportError, match="不支持"):
            call()

    transport.defer(_delivery(), delay=2.0)                              # 默认退化为 nack(requeue=True)
    assert transport.nacked == [(1, True, 2.0)]

    with transport as ctx:
        assert ctx is transport
    assert transport.closed is True


def _delivery(**overrides: object) -> Delivery:
    values: dict[str, object] = {
        "job_id": "j1",
        "message_id": 1,
        "queue": "q",
        "envelope": Envelope(task="t"),
        "worker_id": "w",
        "attempt": 1,
        "deliveries": 1,
        "lease_until": 0.0,
        "reserved_at": 0.0,
    }
    values.update(overrides)
    return Delivery(**values)  # type: ignore[arg-type]


# ==================================================================== memory
def test_memory_idempotency_purge_and_eta():
    class Clock:
        now = 1_800_000_000.0

        def __call__(self) -> float:
            return self.now

    clock = Clock()
    transport = MemoryTransport(clock=clock, idempotency_ttl=10.0)
    first = transport.enqueue(Envelope(task="t", key="k"), queue="q")
    assert transport.enqueue(Envelope(task="t", key="k"), queue="q") == first
    assert transport.queue_stats(["q"])[0].pending == 1

    clock.now += 11
    transport.reap_expired_jobs()                                        # 顺带清幂等键
    assert transport.enqueue(Envelope(task="t", key="k"), queue="q") != first

    eta_transport = MemoryTransport(clock=clock)
    eta_transport.enqueue(Envelope(task="t", eta=clock.now + 50), queue="q")
    assert eta_transport.reserve(["q"], worker_id="w", lease=5, limit=1) == []
    assert eta_transport.next_visible_at(["q"]) == pytest.approx(clock.now + 50)
    eta_transport.close()
    transport.close()


def test_memory_error_paths_and_requeue_delay():
    class Clock:
        now = 1_800_000_000.0

        def __call__(self) -> float:
            return self.now

    clock = Clock()
    transport = MemoryTransport(clock=clock)
    transport.enqueue(Envelope(task="t"), queue="q")
    delivery = transport.reserve(["q"], worker_id="w", lease=30, limit=1)[0]

    transport.nack(delivery, requeue=True, delay=5.0)                    # 退避重排
    assert transport.next_visible_at(["q"]) == pytest.approx(clock.now + 5.0)
    assert transport.reserve(["q"], worker_id="w", lease=30, limit=1) == []

    clock.now += 5
    again = transport.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
    assert again.deliveries == 2

    with pytest.raises(MessageNotFound):
        transport.nack(dataclasses.replace(again, message_id=999))
    with pytest.raises(MessageNotFound):
        transport.ack(dataclasses.replace(again, message_id=999))

    assert transport.reserve([], worker_id="w", lease=5, limit=1) == []
    assert transport.reserve(["q"], worker_id="w", lease=5, limit=0) == []

    transport.ack(again)
    transport.close()
    with pytest.raises(TransportError, match="已关闭"):
        transport.enqueue(Envelope(task="t"), queue="q")


# ======================================================================= beat
def test_beat_corrupt_state_no_lease_and_error_event(tmp_path, monkeypatch):
    from taskmq.schedule import every
    from taskmq.worker.beat import Beat

    app = App(Config(transport="memory://", events="null"))

    @app.task(name="unit.beat", queue="q")
    def job() -> str:
        return "ok"

    state = tmp_path / "beat.json"
    state.write_text("{ not json", encoding="utf-8")
    entry = every("unit.beat", seconds=60, misfire="run_once")
    beat = Beat(app, [entry], state_path=state)
    assert beat.tick(1_800_000_000.0) == []                              # 坏状态文件被忽略
    assert json.loads(state.read_text(encoding="utf-8"))["entries"]

    monkeypatch.setattr(app.transport, "supports_leases", False)
    assert beat.tick(1_800_000_000.0 + 61) == [entry.entry_name()]        # 无租约 → 单副本

    sink = CollectingSink()
    app.add_sink(sink)

    def boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("submit failed")

    monkeypatch.setattr(app, "submit", boom)
    entry2 = every("unit.beat", seconds=1, misfire="run_once", name="failing")
    beat2 = Beat(app, [entry2], state_path=tmp_path / "beat2.json")
    beat2.tick(1_800_000_000.0)
    beat2.tick(1_800_000_000.0 + 2)
    assert sink.of("beat.error")[0]["entry"] == entry2.entry_name()


def test_beat_run_forever_and_context_manager(tmp_path, monkeypatch):
    from taskmq.schedule import every
    from taskmq.worker.beat import Beat

    app = App(Config(transport="memory://", events="null"))

    @app.task(name="unit.beat2", queue="q")
    def job() -> str:
        return "ok"

    entry = every("unit.beat2", seconds=60)
    beat = Beat(app, [entry], state_path=tmp_path / "b.json")

    def stop_after_first(now: float | None = None) -> list[str]:
        beat._stopping.set()
        return []

    monkeypatch.setattr(Beat, "tick", stop_after_first)
    beat.run_forever(poll=0.01)                                          # 循环体 + 退出

    with Beat(app, [entry], state_path=tmp_path / "c.json") as managed:
        assert isinstance(managed, Beat)


# ============================================================== Redis Cluster 槽位
def test_crc16_and_hash_slot_match_redis_cluster_rules():
    """slot 算法必须与 Redis 一致（值取自 `CLUSTER KEYSLOT` 实测）。"""
    from taskmq.redis_client import crc16, hash_slot

    assert crc16(b"123456789") == 0x31C3                     # CRC16-XMODEM 标准校验值
    expected = {
        "foo": 12182,
        "bar": 5061,
        "abc": 7638,
        "{user1000}.following": 3443,
        "{user1000}.followers": 3443,
        "taskmq:{q1}:ready": 7450,
        "a{b}c": 3300,
    }
    for key, slot in expected.items():
        assert hash_slot(key) == slot, key
    # hash tag：`{...}` 里的内容决定 slot（空 `{}` 不算）
    assert hash_slot("{user1000}.following") == hash_slot("{user1000}.followers")
    assert hash_slot("taskmq:{q1}:ready") == hash_slot("taskmq:{q1}:msg:42")
    assert hash_slot("x{}y") != hash_slot("{}")              # 空 tag → 整个键参与计算


def test_cluster_client_validates_db_and_slot_shape():
    from taskmq.errors import ConfigError
    from taskmq.redis_client import RedisClusterClient

    with pytest.raises(ConfigError, match="db 0"):
        RedisClusterClient("redis://127.0.0.1:7380/15", timeout=1.0)
    with pytest.raises(ConfigError, match="scheme"):
        RedisClusterClient("redis+unix:///tmp/redis.sock", timeout=1.0)


def test_package_version_matches_pyproject():
    """版本只有一个来源：`pyproject.toml` 的 version（打包元数据）——CLI / 文档 / Release 都引用它。"""
    import re
    from pathlib import Path

    import taskmq

    if taskmq.__version__ == "0.0.0+unknown":
        pytest.skip("taskmq 未安装（源码目录直接 import），无法与打包元数据比对")
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    match = re.search(r'^version = "([^"]+)"', pyproject.read_text(encoding="utf-8"), re.MULTILINE)
    assert match is not None, "pyproject.toml 里找不到 version"
    assert taskmq.__version__ == match.group(1)
