"""测试与本地开发辅助（docs/design.md §16）。"""
from __future__ import annotations

import contextlib
from collections.abc import Iterator, Sequence
from typing import Any

from .app import App
from .config import Config
from .worker.pool import Pool
from .worker.runner import Worker

__all__ = ["worker_for", "run_until_idle", "eager_app"]


@contextlib.contextmanager
def worker_for(
    app: App,
    *,
    queues: Sequence[str] | None = None,
    concurrency: int | None = None,
    prefetch: int | None = None,
    worker_id: str | None = None,
    pool: Pool | None = None,
) -> Iterator[Worker]:
    """在**当前进程内**跑完整链路（transport → pool → ack），可断点调试。

    默认 `prefetch == concurrency`：不会有「预留了但没开始」的消息，即 reserve–start 耦合（P16）。
    要测让位（G2）就显式把 `prefetch` 调大。
    """
    worker = Worker(
        app,
        queues=queues,
        concurrency=concurrency,
        prefetch=prefetch,
        worker_id=worker_id,
        pool=pool,
    )
    try:
        yield worker
    finally:
        worker.close()


def run_until_idle(
    app: App,
    *,
    queues: Sequence[str] | None = None,
    concurrency: int | None = None,
    prefetch: int | None = None,
    timeout: float = 30.0,
) -> int:
    """一次性把队列跑空的语法糖。"""
    with worker_for(
        app, queues=queues, concurrency=concurrency, prefetch=prefetch
    ) as worker:
        return worker.run_until_idle(timeout=timeout)


def eager_app(**config_kwargs: Any) -> App:
    """`eager=True` 的 App：`delay()` 同步执行，用于纯逻辑单测。"""
    config_kwargs.setdefault("eager", True)
    return App(Config(**config_kwargs))
