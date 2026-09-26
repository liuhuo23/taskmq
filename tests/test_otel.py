"""OTel 适配：用鸭子类型 stub tracer 测（不需要安装 opentelemetry）。"""
from __future__ import annotations

import importlib.util

import pytest

from taskmq import App, Config
from taskmq.errors import ConfigError
from taskmq.events import build_sink
from taskmq.otel import OtelEventSink, otel_sink
from taskmq.testing import run_until_idle


def _status_code(status: object) -> str:
    """兼容真实 OTel Status 与缺依赖时的降级 dict。"""
    code = getattr(status, "status_code", None)
    if code is None:
        return str(dict(status)["status_code"])  # type: ignore[call-overload]
    return str(getattr(code, "name", code))


class StubSpan:
    def __init__(self, name: str, attributes: dict) -> None:
        self.name = name
        self.attributes = dict(attributes)
        self.events: list[tuple[str, dict]] = []
        self.status = None
        self.ended = False

    def set_attribute(self, key: str, value: object) -> None:
        self.attributes[key] = value

    def add_event(self, name: str, attributes: dict | None = None) -> None:
        self.events.append((name, attributes or {}))

    def set_status(self, status: object) -> None:
        self.status = status

    def end(self) -> None:
        self.ended = True


class StubTracer:
    def __init__(self) -> None:
        self.spans: list[StubSpan] = []

    def start_span(self, name: str, attributes: dict | None = None) -> StubSpan:
        span = StubSpan(name, attributes or {})
        self.spans.append(span)
        return span


def _sink() -> tuple[OtelEventSink, StubTracer]:
    tracer = StubTracer()
    return OtelEventSink(tracer), tracer


def test_started_creates_span_with_messaging_attributes():
    sink, tracer = _sink()
    sink.emit(
        {
            "event": "task.started",
            "job_id": "j1",
            "task": "app.send",
            "queue": "email",
            "attempt": 2,
            "priority": 5,
            "worker": "w1",
        }
    )
    span = tracer.spans[0]
    assert span.name == "task app.send"
    assert span.attributes["messaging.system"] == "taskmq"
    assert span.attributes["messaging.destination.name"] == "email"
    assert span.attributes["messaging.message.id"] == "j1"
    assert span.attributes["taskmq.attempt"] == 2
    assert span.attributes["taskmq.priority"] == 5
    assert span.ended is False


def test_succeeded_finishes_span_with_ok_status():
    sink, tracer = _sink()
    sink.emit({"event": "task.started", "job_id": "j1", "task": "t"})
    sink.emit({"event": "task.succeeded", "job_id": "j1", "task": "t", "runtime": 0.25})
    span = tracer.spans[0]
    assert span.ended is True
    assert _status_code(span.status) == "OK"


def test_failed_and_retrying_mark_error():
    sink, tracer = _sink()
    sink.emit({"event": "task.started", "job_id": "j1", "task": "t"})
    sink.emit({"event": "task.failed", "job_id": "j1", "task": "t", "error": "boom"})
    failed = tracer.spans[0]
    assert failed.ended is True
    assert _status_code(failed.status) == "ERROR"
    assert failed.attributes["taskmq.error"] == "boom"

    sink.emit({"event": "task.started", "job_id": "j2", "task": "t"})
    sink.emit(
        {"event": "task.retrying", "job_id": "j2", "task": "t", "error": "again", "delay": 3.0}
    )
    retrying = tracer.spans[1]
    assert _status_code(retrying.status) == "ERROR"
    assert retrying.attributes["taskmq.delay"] == 3.0


def test_deferred_annotates_current_span_and_unknown_is_ignored():
    sink, tracer = _sink()
    sink.emit({"event": "task.deferred", "job_id": "ghost", "reason": "rate_limit"})   # 没 started：忽略
    assert tracer.spans == []

    sink.emit({"event": "task.started", "job_id": "j1", "task": "t"})
    sink.emit({"event": "task.deferred", "job_id": "j1", "reason": "rate_limit", "delay": 1.5})
    events = tracer.spans[0].events
    assert events[0][0] == "task.deferred"
    assert events[0][1]["reason"] == "rate_limit"


def test_end_to_end_traces_task_runs():
    app = App(Config(transport="memory://", serializer="json", events="null", concurrency=2))
    tracer = StubTracer()
    app.add_sink(OtelEventSink(tracer))

    @app.task(name="otel.ok", queue="q")
    def ok() -> str:
        return "fine"

    @app.task(name="otel.bad", queue="q", retry=None)
    def bad() -> None:
        raise RuntimeError("nope")

    app.submit("otel.ok")
    app.submit("otel.bad")
    run_until_idle(app, queues=["q"], timeout=30)

    names = [span.name for span in tracer.spans]
    assert "task otel.ok" in names and "task otel.bad" in names
    assert all(span.ended for span in tracer.spans)
    assert any(_status_code(span.status) == "ERROR" for span in tracer.spans)


def test_otel_sink_without_dependency_has_clear_error():
    if importlib.util.find_spec("opentelemetry") is not None:
        pytest.skip("本环境装了 opentelemetry，跳过缺依赖分支")
    with pytest.raises(ConfigError) as excinfo:
        build_sink("otel")
    assert "opentelemetry" in str(excinfo.value)


def test_status_uses_real_otel_api_when_installed():
    """装了 opentelemetry 时 `_status` 必须返回真正的 Status（不是降级 dict）。"""
    from taskmq.otel import _status

    if importlib.util.find_spec("opentelemetry") is None:
        assert _status(False, "boom") == {"status_code": "ERROR", "description": "boom"}
        return
    from opentelemetry.trace import StatusCode

    assert _status(True).status_code == StatusCode.OK
    failed = _status(False, "boom")
    assert failed.status_code == StatusCode.ERROR and failed.description == "boom"


def test_otel_sink_accepts_injected_tracer():
    tracer = StubTracer()
    assert isinstance(otel_sink(tracer=tracer), OtelEventSink)


def test_config_accepts_otel_events_value():
    """值本身合法（依赖是否装好由 App 构造时 `build_sink` 判定，fail fast）。"""
    Config(events="otel").validate()
    if importlib.util.find_spec("opentelemetry") is None:
        with pytest.raises(ConfigError):
            App(Config(events="otel", serializer="json"))
    else:  # pragma: no cover - 装了额外依赖时
        App(Config(events="otel", serializer="json")).close()
