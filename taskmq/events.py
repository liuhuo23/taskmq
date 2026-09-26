"""结构化事件（docs/design.md §14.1）。

默认把 JSON 行写到 stdout（`Config.events="stdout"`）；测试或嵌入时可挂 `CollectingSink`，
OTel 适配器（Phase 1 后续）只需要实现同一个 `EventSink` 协议。
"""
from __future__ import annotations

import json
import sys
import threading
import time
from typing import Any, Protocol, TextIO, runtime_checkable


@runtime_checkable
class EventSink(Protocol):
    def emit(self, event: dict[str, Any]) -> None:
        """接收一条已带 `ts` / `event` 的事件。实现不应阻塞、不应抛异常。"""


class JsonStdoutSink:
    """一行一个 JSON 对象（便于 grep / 采集）。"""

    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream if stream is not None else sys.stdout
        self._lock = threading.Lock()

    def emit(self, event: dict[str, Any]) -> None:
        line = json.dumps(event, ensure_ascii=False, default=str)
        with self._lock:
            self._stream.write(line + "\n")
            self._stream.flush()


class CollectingSink:
    """测试用：把事件收进列表。"""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def emit(self, event: dict[str, Any]) -> None:
        self.events.append(dict(event))

    def names(self) -> list[str]:
        return [str(event.get("event")) for event in self.events]

    def of(self, name: str) -> list[dict[str, Any]]:
        return [event for event in self.events if event.get("event") == name]


class NullSink:
    def emit(self, event: dict[str, Any]) -> None:
        return None


def build_sink(kind: str) -> EventSink:
    if kind == "stdout":
        return JsonStdoutSink()
    if kind == "null":
        return NullSink()
    from .errors import ConfigError

    raise ConfigError(f'events 可选 "stdout" | "null"，收到 {kind!r}')


__all__ = ["EventSink", "JsonStdoutSink", "CollectingSink", "NullSink", "build_sink"]


def now() -> float:
    return time.time()
