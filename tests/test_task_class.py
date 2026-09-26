"""类式任务、`bind=True` 与生命周期钩子（docs/design/tasks.md 的验收）。"""
from __future__ import annotations

import pytest

from taskmq import App, Config, Retry, Task, TaskError
from taskmq.errors import ConfigError
from taskmq.testing import run_until_idle


def _app(**overrides):
    config = {"transport": "memory://", "serializer": "json", "concurrency": 2}
    config.update(overrides)
    return App(Config(**config))


def test_class_based_task_with_hooks_and_process_resources():
    app = _app()
    events: list[str] = []

    class EmailTask(Task):
        queue = "email"
        priority = 5
        retry_policy = Retry(max_attempts=2, base=0.0, jitter=False)

        def __init__(self, app):
            super().__init__(app)
            self.client = "smtp-client"                     # 进程级资源

        def before_start(self, ctx):
            events.append(f"before:{ctx.args[0]}")

        def run(self, to: str, subject: str) -> str:
            assert self.client == "smtp-client"             # 进程级资源共享
            assert self.request.args == (to, subject)       # 请求级信息走 request
            assert self.request.queue == "email"
            assert self.request.priority == 5
            events.append(f"run:{to}")
            return f"sent:{to}"

        def on_success(self, ctx, result, runtime):
            events.append(f"success:{result}")

        def after_return(self, ctx, state, result=None, exc=None):
            events.append(f"after:{state}")

    task = app.register(EmailTask)
    handle = task.delay("a@b.com", "hi")
    run_until_idle(app, queues=["email"], timeout=10)

    assert handle.get(timeout=1) == "sent:a@b.com"
    assert events == [
        "before:a@b.com",
        "run:a@b.com",
        "success:sent:a@b.com",
        "after:SUCCEEDED",
    ]


def test_bind_true_injects_self_and_request():
    app = _app()
    seen: dict[str, object] = {}

    @app.task(bind=True)
    def whoami(self, value: int, flag: bool = False) -> str:
        seen["is_task"] = isinstance(self, Task)
        seen["id"] = self.request.id
        seen["retries"] = self.request.retries
        seen["args"] = self.request.args
        seen["kwargs"] = dict(self.request.kwargs)
        seen["hostname"] = self.request.hostname
        return "ok"

    handle = whoami.apply_async((7,), {"flag": True})
    run_until_idle(app, timeout=10)

    assert handle.get(timeout=1) == "ok"
    assert seen["is_task"] is True
    assert seen["id"] == handle.id
    assert seen["retries"] == 0
    assert seen["args"] == (7,)
    assert seen["kwargs"] == {"flag": True}
    assert seen["hostname"]


def test_self_request_outside_execution_raises():
    app = _app()

    @app.task(bind=True)
    def nothing(self) -> int:
        return 1

    with pytest.raises(TaskError):
        _ = nothing.request


def test_base_mixin_applies_hooks_to_function_task():
    app = _app()
    events: list[str] = []

    class TenantTask(Task):
        def before_start(self, ctx):
            events.append(f"tenant:{ctx.kwargs.get('tenant')}")

    @app.task(base=TenantTask)
    def build_report(month: str, tenant: str = "") -> str:
        events.append(f"run:{month}")
        return month

    handle = build_report.apply_async(("2025-01",), {"tenant": "acme"})
    run_until_idle(app, timeout=10)

    assert handle.get(timeout=1) == "2025-01"
    assert events == ["tenant:acme", "run:2025-01"]


def test_hook_order_on_retry_then_success():
    app = _app(concurrency=1)
    events: list[str] = []

    class FlakyTask(Task):
        retry_policy = Retry(max_attempts=2, base=0.0, jitter=False)

        def before_start(self, ctx):
            events.append("before")

        def run(self):
            events.append("run")
            if self.request.attempt == 1:
                raise ValueError("boom")
            return "ok"

        def on_retry(self, ctx, exc, delay):
            events.append(f"retry:{delay}")

        def on_success(self, ctx, result, runtime):
            events.append("success")

        def on_failure(self, ctx, exc):
            events.append("failure")

        def after_return(self, ctx, state, result=None, exc=None):
            events.append(f"after:{state}")

    task = app.register(FlakyTask)
    handle = task.delay()
    run_until_idle(app, timeout=10)

    assert handle.get(timeout=1) == "ok"
    assert events == [
        "before",
        "run",
        "retry:0.0",
        "after:RETRYING",
        "before",
        "run",
        "success",
        "after:SUCCEEDED",
    ]


def test_failure_hook_order_and_dlq():
    app = _app(concurrency=1)
    events: list[str] = []

    class AlwaysFail(Task):
        retry_policy = Retry(max_attempts=1, base=0.0, jitter=False)

        def run(self):
            raise RuntimeError("nope")

        def on_failure(self, ctx, exc):
            events.append("failure")

        def after_return(self, ctx, state, result=None, exc=None):
            events.append(f"after:{state}")

    task = app.register(AlwaysFail)
    handle = task.delay()
    run_until_idle(app, timeout=10)

    assert handle.state == "FAILED"
    assert events == ["failure", "after:FAILED"]
    assert len(app.transport.dead_letters()) == 1


def test_hook_exception_does_not_change_delivery_semantics():
    app = _app(concurrency=1)

    class NoisyTask(Task):
        retry_policy = Retry(max_attempts=1, base=0.0, jitter=False)

        def run(self):
            raise RuntimeError("nope")

        def on_failure(self, ctx, exc):
            raise RuntimeError("hook exploded")

    task = app.register(NoisyTask)
    handle = task.delay()
    run_until_idle(app, timeout=10)

    assert handle.state == "FAILED"                          # 钩子炸了也仍然是 FAILED → DLQ
    assert len(app.transport.dead_letters()) == 1
    assert "hook_error" in handle.info


def test_before_start_exception_fails_the_task():
    app = _app(concurrency=1)

    class BrokenTask(Task):
        retry_policy = Retry(max_attempts=1, base=0.0, jitter=False)

        def before_start(self, ctx):
            raise RuntimeError("resource down")

        def run(self):
            return "never"

    task = app.register(BrokenTask)
    handle = task.delay()
    run_until_idle(app, timeout=10)

    assert handle.state == "FAILED"
    assert "resource down" in (handle.info.get("error") or "")


def test_self_retry_countdown_is_honoured():
    app = _app(concurrency=1)
    delays: list[float] = []
    attempts: list[int] = []

    class FlakyTask(Task):
        retry_policy = Retry(max_attempts=3, base=100.0, jitter=False)

        def run(self):
            attempts.append(self.request.attempt)
            if self.request.attempt < 2:
                raise self.retry(countdown=0.01, reason="later")
            return "ok"

        def on_retry(self, ctx, exc, delay):
            delays.append(delay)

    task = app.register(FlakyTask)
    handle = task.delay()
    run_until_idle(app, timeout=10)

    assert handle.get(timeout=1) == "ok"
    assert delays == [0.01]                                  # countdown 覆盖策略退避
    assert attempts == [1, 2]


def test_update_meta_reports_progress():
    app = _app()

    class ProgressTask(Task):
        def run(self):
            self.request.update_meta(percent=50, stage="half")
            return "done"

    task = app.register(ProgressTask)
    handle = task.delay()
    run_until_idle(app, timeout=10)

    assert handle.get(timeout=1) == "done"
    assert handle.info["percent"] == 50
    assert handle.info["stage"] == "half"


def test_register_requires_run_implementation():
    app = _app()

    class NoRunTask(Task):
        pass

    with pytest.raises(ConfigError):
        app.register(NoRunTask)


def test_decorator_level_hooks():
    app = _app()
    events: list[str] = []

    @app.task(on_success=lambda ctx, result, runtime: events.append(f"ok:{result}"))
    def add(a: int, b: int) -> int:
        return a + b

    handle = add.delay(1, 2)
    run_until_idle(app, timeout=10)

    assert handle.get(timeout=1) == 3
    assert events == ["ok:3"]
