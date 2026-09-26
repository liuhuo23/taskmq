"""OpenTelemetry 适配器（可选依赖 `taskmq-py[otel]`）。

事件 → OTel span 的映射：

| taskmq 事件 | OTel |
|---|---|
| `task.started` | `start_span("task <name>")`，带标准 messaging 语义约定 + `taskmq.*` 属性 |
| `task.succeeded` / `task.failed` / `task.retrying` | 结束 span 并设状态（retrying 记 ERROR） |
| 其它（`task.deferred` / `beat.fired` / `worker.*`） | 作为 span event 挂到当前任务 span 上（没有就忽略） |

依赖是可选的：为了可测试，sink 接受注入的 `tracer`（鸭子类型，只要有 `start_span`）；
不注入时才 import opentelemetry，没装就抛 `ConfigError` 并给出安装提示。
"""
from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from .errors import ConfigError

__all__ = ["OtelEventSink", "otel_sink"]


def _status(ok: bool, description: str = "") -> Any:
    try:
        from opentelemetry.trace import Status, StatusCode
    except ImportError:  # 没装额外依赖：给个可读替代（测试与降级都用它）
        return {"status_code": "OK" if ok else "ERROR", "description": description}
    return Status(StatusCode.OK if ok else StatusCode.ERROR, description)


def _is_attribute(value: Any) -> bool:
    return isinstance(value, (str, int, float, bool))


class OtelEventSink:
    """把结构化事件转成 OTel span。`tracer` 只需实现 `start_span(name, attributes=…)`。"""

    _END_EVENTS = ("task.succeeded", "task.failed", "task.retrying")

    def __init__(
        self,
        tracer: Any,
        *,
        span_name: Callable[[dict[str, Any]], str] | None = None,
    ) -> None:
        self._tracer = tracer
        self._span_name = span_name or (lambda event: f"task {event.get('task', '?')}")
        self._spans: dict[str, Any] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ 入口
    def emit(self, event: dict[str, Any]) -> None:
        name = str(event.get("event", ""))
        if name == "task.started":
            self._start(event)
        elif name in self._END_EVENTS:
            self._finish(event, ok=name == "task.succeeded")
        else:
            self._annotate(event)

    # ------------------------------------------------------------------ 内部
    def _start(self, event: dict[str, Any]) -> None:
        span = self._tracer.start_span(self._span_name(event), attributes=self._attributes(event))
        with self._lock:
            self._spans[str(event.get("job_id", ""))] = span

    def _finish(self, event: dict[str, Any], *, ok: bool) -> None:
        job_id = str(event.get("job_id", ""))
        with self._lock:
            span = self._spans.pop(job_id, None)
        if span is None:                                   # 没有对应的 started，忽略
            return
        for key, value in self._attributes(event).items():
            if hasattr(span, "set_attribute"):
                span.set_attribute(key, value)
        if event.get("error") and hasattr(span, "set_attribute"):
            span.set_attribute("taskmq.error", str(event["error"])[:500])
        if event.get("will_retry") and hasattr(span, "set_attribute"):
            span.set_attribute("taskmq.will_retry", True)
        if hasattr(span, "set_status"):
            span.set_status(_status(ok, str(event.get("error", ""))))
        if hasattr(span, "end"):
            span.end()

    def _annotate(self, event: dict[str, Any]) -> None:
        job_id = str(event.get("job_id", ""))
        with self._lock:
            span = self._spans.get(job_id) if job_id else None
        if span is None or not hasattr(span, "add_event"):
            return
        attributes = {
            key: value
            for key, value in event.items()
            if key != "event" and _is_attribute(value)
        }
        span.add_event(str(event.get("event", "taskmq")), attributes=attributes)

    @staticmethod
    def _attributes(event: dict[str, Any]) -> dict[str, Any]:
        attributes: dict[str, Any] = {
            "messaging.system": "taskmq",
            "messaging.operation": "process",
        }
        if event.get("queue"):
            attributes["messaging.destination.name"] = str(event["queue"])
        if event.get("job_id"):
            attributes["messaging.message.id"] = str(event["job_id"])
        if event.get("task"):
            attributes["messaging.message.type"] = str(event["task"])
        if event.get("attempt") is not None:
            attributes["taskmq.attempt"] = int(event["attempt"])
        if event.get("priority") is not None:
            attributes["taskmq.priority"] = int(event["priority"])
        if event.get("worker"):
            attributes["taskmq.worker"] = str(event["worker"])
        if event.get("delay") is not None:
            attributes["taskmq.delay"] = float(event["delay"])
        if event.get("reason"):
            attributes["taskmq.reason"] = str(event["reason"])
        return attributes


def otel_sink(*, tracer: Any = None, tracer_name: str = "taskmq") -> OtelEventSink:
    """不传 tracer 时取 OTel 全局 tracer；没装 opentelemetry 就抛 `ConfigError`。"""
    if tracer is None:
        try:
            from opentelemetry import trace
        except ImportError as exc:  # pragma: no cover - 取决于环境
            raise ConfigError(
                "events='otel' 需要 opentelemetry-api：pip install 'taskmq-py[otel]'"
            ) from exc
        tracer = trace.get_tracer(tracer_name)
    return OtelEventSink(tracer)
