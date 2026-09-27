#!/usr/bin/env python3
"""taskmq 压测：入队吞吐、消费吞吐、端到端延迟分位（可换后端）。

    python scripts/bench.py                                     # memory:// 2000 条
    python scripts/bench.py -n 10000 -c 8
    python scripts/bench.py -t "sqlite:///./bench.db" -n 5000
    python scripts/bench.py -t "redis://127.0.0.1:6379/15?prefix=bench:" -n 2000
    python scripts/bench.py -t "redis://127.0.0.1:6379/15?prefix=b:&lua=off"
    python scripts/bench.py -q 8 --payload 4096                  # 8 队列轮投、每条约 4KB

口径（都在这台机器上单进程实测，网络后端请把 redis 换成本机地址）：
- **入队**：单线程连续 `apply_async` 的墙钟吞吐；
- **消费**：同进程 `run_until_idle`（reserve → 执行 → ack 全链路）跑空；
- **延迟**：从提交到任务真正被执行的墙钟时间（含排队），报 p50 / p95 / max。

参考量级（M 系列 Mac、单进程、redis 在 loopback）：
memory:// ≈ 1.3 万/s（小积压）→ 1.2 千/s（1 万积压，受全表扫描影响）；
sqlite:// ≈ 3–5 千/s；redis（Lua）≈ 800/s；redis（无 Lua 回退）≈ 300/s。
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import time
from pathlib import Path

from taskmq import App, Config
from taskmq.testing import run_until_idle


def _percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round(ratio * (len(ordered) - 1))))
    return ordered[index]


def bench(
    transport: str,
    *,
    count: int,
    concurrency: int,
    queue_count: int,
    payload: int,
) -> dict:
    if transport.startswith("sqlite:///"):
        path = Path(transport.split("///", 1)[1])
        path.unlink(missing_ok=True)
    queues = [f"bench{i}" for i in range(queue_count)] if queue_count > 1 else ["bench"]
    app = App(Config(
        transport=transport,
        events="null",
        concurrency=concurrency,
        poll_interval=0.05,
    ))

    submitted: dict[int, float] = {}
    executed: list[float] = []

    @app.task(queue=queues[0], name="bench.noop")
    def noop(i: int, blob: str) -> int:
        executed.append(time.perf_counter() - submitted[i])
        return i

    blob = "x" * max(0, payload)
    started = time.perf_counter()
    for i in range(count):
        submitted[i] = time.perf_counter()
        noop.apply_async((i, blob), queue=queues[i % len(queues)])
    enqueue_seconds = time.perf_counter() - started

    started = time.perf_counter()
    run_until_idle(app, queues=queues, timeout=3600)
    consume_seconds = time.perf_counter() - started
    app.close()

    return {
        "transport": transport,
        "count": count,
        "concurrency": concurrency,
        "queues": queue_count,
        "payload": payload,
        "enqueue_per_s": count / enqueue_seconds if enqueue_seconds else 0.0,
        "consume_per_s": count / consume_seconds if consume_seconds else 0.0,
        "consume_seconds": consume_seconds,
        "latency_ms": {
            "p50": _percentile(executed, 0.50) * 1000,
            "p95": _percentile(executed, 0.95) * 1000,
            "max": (max(executed) if executed else 0.0) * 1000,
            "mean": (statistics.fmean(executed) if executed else 0.0) * 1000,
        },
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="taskmq 压测（吞吐 + 延迟分位）")
    parser.add_argument("-t", "--transport", default="memory://")
    parser.add_argument("-n", "--count", type=int, default=2000)
    parser.add_argument("-c", "--concurrency", type=int, default=8)
    parser.add_argument("-q", "--queues", type=int, default=1, dest="queue_count")
    parser.add_argument("--payload", type=int, default=0, help="每条消息 payload 字节数")
    parser.add_argument("--json", action="store_true", help="只输出 JSON（便于脚本对比）")
    args = parser.parse_args(argv[1:])

    result = bench(
        args.transport,
        count=args.count,
        concurrency=args.concurrency,
        queue_count=args.queue_count,
        payload=args.payload,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False))
        return 0

    lat = result["latency_ms"]
    print(f"transport      : {result['transport']}")
    print(f"规模           : {result['count']} 条 · 并发 {result['concurrency']} · 队列 {result['queues']} "
          f"· payload {result['payload']}B")
    print(f"入队吞吐       : {result['enqueue_per_s']:>10.0f} /s")
    print(f"消费吞吐       : {result['consume_per_s']:>10.0f} /s（{result['consume_seconds']:.2f}s 跑空）")
    print(f"端到端延迟     : p50 {lat['p50']:.1f}ms · p95 {lat['p95']:.1f}ms · max {lat['max']:.1f}ms "
          f"· mean {lat['mean']:.1f}ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
