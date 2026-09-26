"""端到端：memory transport + threads pool 的最小闭环（Phase 0）。"""
from __future__ import annotations

import pytest

from taskmq import App, Config, Priority, RemoteError, Retry
from taskmq.errors import ConfigError
from taskmq.priority import validate_priority
from taskmq.testing import eager_app, run_until_idle


def _app(**overrides):
    config = {"transport": "memory://", "serializer": "json", "concurrency": 2}
    config.update(overrides)
    return App(Config(**config))


def test_end_to_end_result_roundtrip():
    app = _app()

    @app.task(queue="math", priority=Priority.NORMAL)
    def add(a: int, b: int) -> int:
        return a + b

    handles = [add.delay(index, index) for index in range(10)]
    run_until_idle(app, queues=["math"], timeout=10)
    assert [handle.get(timeout=1) for handle in handles] == [index * 2 for index in range(10)]
    assert all(handle.state == "SUCCEEDED" for handle in handles)
    assert app.transport.queue_stats(["math"])[0].pending == 0


def test_retry_then_success_keeps_priority():
    app = _app(concurrency=1)
    attempts: list[int] = []

    @app.task(priority=Priority.HIGH, retry=Retry(max_attempts=3, backoff="fixed", base=0.0, jitter=False))
    def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise ValueError("boom")
        return "ok"

    handle = flaky.delay()
    run_until_idle(app, timeout=10)
    assert handle.state == "SUCCEEDED"
    assert handle.get(timeout=1) == "ok"
    assert len(attempts) == 3
    assert handle.info["attempt"] == 3
    assert handle.info["priority"] == Priority.HIGH       # P5：重试保持原优先级


def test_retry_exhausted_goes_to_dlq_and_can_be_replayed():
    app = _app(concurrency=1)

    calls: list[int] = []

    @app.task(retry=Retry(max_attempts=1, base=0.0, jitter=False))
    def always_fail():
        calls.append(1)
        raise RuntimeError("nope")

    handle = always_fail.delay()
    run_until_idle(app, timeout=10)
    assert handle.state == "FAILED"
    with pytest.raises(RemoteError):
        handle.get(timeout=1)
    assert len(calls) == 1

    dead = app.transport.dead_letters()
    assert len(dead) == 1
    assert "nope" in dead[0].reason

    # 重放复用同一条消息行（deliveries 重置），因此 DLQ 仍只有 1 条；用调用次数证明"真的又跑了一次"
    assert app.transport.replay_dead(dead[0].message_id) is True
    run_until_idle(app, timeout=10)
    assert len(calls) == 2
    assert len(app.transport.dead_letters()) == 1


def test_eager_mode_executes_inline():
    app = eager_app(serializer="json")

    @app.task
    def add(a: int, b: int) -> int:
        return a + b

    handle = add.delay(2, 3)
    assert handle.state == "SUCCEEDED"
    assert handle.get() == 5


def test_eager_mode_propagates_exception_and_records_failure():
    app = eager_app(serializer="json")

    @app.task
    def boom():
        raise ZeroDivisionError("division by zero")

    with pytest.raises(ZeroDivisionError):
        boom.delay()


def test_many_tasks_on_threads_pool_with_default_serializer():
    app = App(Config(transport="memory://", concurrency=8))     # 默认 serializer=msgspec

    @app.task
    def work(index: int) -> int:
        return index * 2

    handles = [work.delay(index) for index in range(200)]
    run_until_idle(app, timeout=30)
    assert all(handle.successful() for handle in handles)
    assert [handle.get(timeout=1) for handle in handles[:5]] == [0, 2, 4, 6, 8]


def test_apply_async_separates_task_kwargs_from_options():
    app = _app()

    @app.task
    def echo(**kwargs):
        return kwargs

    handle = echo.apply_async((), {"a": 1, "priority": "task-arg"}, priority=Priority.HIGH)
    run_until_idle(app, timeout=10)
    assert handle.get(timeout=1) == {"a": 1, "priority": "task-arg"}


def test_config_validation_rejects_bad_values():
    with pytest.raises(ConfigError):
        Config(concurrency=0).validate()
    with pytest.raises(ConfigError):
        Config(default_priority=10).validate()
    with pytest.raises(ConfigError):
        Config(pool="quantum").validate()
    with pytest.raises(ConfigError):
        Config(serializer="cbor").validate()
    with pytest.raises(ConfigError):
        Config(result="memory://", result_ttl=None).validate()
    with pytest.raises(ConfigError):
        Config(normal_reserved_slots=8, concurrency=4).validate()


def test_config_from_env_is_explicit():
    config = Config.from_env(
        {
            "TASKMQ_CONCURRENCY": "8",
            "TASKMQ_DEFAULT_PRIORITY": "5",
            "TASKMQ_YIELD_ENABLED": "false",
            "TASKMQ_UNKNOWN_FIELD": "ignored",
        }
    )
    assert config.concurrency == 8
    assert config.default_priority == 5
    assert config.yield_enabled is False
    with pytest.raises(ConfigError):
        Config.from_env({"TASKMQ_CONCURRENCY": "abc"})


def test_unknown_transport_scheme_raises():
    # redis:// 已实现（Phase 1）；amqp:// 是 Phase 2，仍未实现
    app = App(Config(transport="amqp://guest@localhost//", serializer="json"))
    with pytest.raises(ConfigError):
        _ = app.transport


def test_validate_priority_helper_is_exported():
    assert validate_priority(Priority.MAX) == 9
