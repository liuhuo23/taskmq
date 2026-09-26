"""最小可跑示例：SQLite transport + 插队 + DLQ。

启动 worker：

~~~bash
PYTHONPATH=. uv run taskmq --app examples.demo_app:app worker -Q demo -c 2
~~~

投递任务：

~~~bash
PYTHONPATH=. uv run python -c "from examples.demo_app import app; app.submit('demo.add', (1, 2))"
~~~
"""
from __future__ import annotations

import time
from pathlib import Path

from taskmq import App, Config, Retry

LOG = Path("demo_run.log")

app = App(
    Config(
        transport="sqlite:///./demo.db",
        serializer="json",
        concurrency=4,
        poll_interval=0.02,
    )
)


def _mark(name: str) -> None:
    with LOG.open("a", encoding="utf-8") as handle:
        handle.write(f"{time.time():.3f} {name}\n")


@app.task(queue="demo", name="demo.add")
def add(a: int, b: int) -> int:
    _mark(f"add({a},{b})")
    return a + b


@app.task(queue="demo", name="demo.slow")
def slow(seconds: float) -> str:
    _mark(f"slow({seconds}) start")
    time.sleep(seconds)
    _mark(f"slow({seconds}) done")
    return f"slept {seconds}"


@app.task(queue="demo", name="demo.flaky", retry=Retry(max_attempts=2, base=0.05, jitter=False))
def flaky(ok: bool = True) -> str:
    _mark(f"flaky(ok={ok})")
    if not ok:
        raise RuntimeError("demo: always fails")
    return "fine"
