"""taskmq 冒烟脚本：确认装好的 taskmq 能跑通最小闭环。

    python skills/taskmq/scripts/smoke.py
    # 或（仓库内）uv run python skills/taskmq/scripts/smoke.py

做三件事：sqlite transport 入队 → 优先级插队 → 同进程跑 worker → 取结果。
退出码 0 = 环境可用；非 0 会打印失败原因。
"""
from __future__ import annotations

import tempfile
from pathlib import Path

from taskmq import App, Config, Priority
from taskmq.testing import run_until_idle


def main() -> int:
    import taskmq

    db = Path(tempfile.mkdtemp()) / "smoke.db"
    app = App(Config(transport=f"sqlite:///{db}", events="null", concurrency=2))

    @app.task(queue="smoke")
    def add(a: int, b: int) -> int:
        return a + b

    @app.task(queue="smoke", priority=Priority.CRITICAL)
    def urgent() -> str:
        return "urgent"

    slow = add.delay(1, 1)
    fast = urgent.delay()
    run_until_idle(app, queues=["smoke"], timeout=30)

    assert slow.get(timeout=5) == 2, "普通任务结果不对"
    assert fast.get(timeout=5) == "urgent", "高优先级任务结果不对"

    stats = app.transport.queue_stats(["smoke"])[0]
    assert stats.pending == 0 and stats.inflight == 0, f"队列没跑干净：{stats}"
    print(f"OK · taskmq {taskmq.__version__} · python {__import__('sys').version.split()[0]} · 队列已跑空")
    app.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
