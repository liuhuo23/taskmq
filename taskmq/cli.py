"""`taskmq` 命令行：worker / status / dlq / call。

用法（docs/design.md §6.3）：

~~~bash
export TASKMQ_APP=myapp.tasks:app          # 或 --app myapp.tasks:app
taskmq worker -Q email,default -c 8
taskmq worker --once                       # 跑空就退出（调试/CI）
taskmq status --by-priority
taskmq dlq list -Q email
taskmq dlq replay --all -Q email --priority 0
taskmq call myapp.tasks.send_email --args '["a@b.com","hi"]'   # 同步执行一次
~~~
"""
from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import signal
import time
from collections.abc import Sequence
from typing import Any

from . import __version__
from .app import App
from .errors import TaskMQError, error_text
from .priority import validate_priority
from .worker.runner import Worker

logger = logging.getLogger("taskmq.cli")


def _load_app(spec: str) -> App:
    """`module:attr` -> App 实例。"""
    if ":" not in spec:
        raise SystemExit(f"--app 需要 module:attr 形式，收到 {spec!r}")
    module_name, _, attr = spec.partition(":")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise SystemExit(f"无法导入 {module_name}：{exc}") from exc
    app = getattr(module, attr, None)
    if not isinstance(app, App):
        raise SystemExit(f"{spec} 不是 App 实例（拿到 {type(app).__name__}）")
    return app


def _queues(raw: str) -> list[str] | None:
    items = [item.strip() for item in raw.split(",") if item.strip()]
    return items or None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="taskmq", description="零外部服务的 Python 任务队列")
    parser.add_argument("--version", action="version", version=f"taskmq {__version__}")
    parser.add_argument(
        "--app",
        default=os.environ.get("TASKMQ_APP"),
        help="App 位置：module:attr（也可用环境变量 TASKMQ_APP）",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    worker = sub.add_parser("worker", help="消费队列")
    worker.add_argument("-Q", "--queues", default="", help="逗号分隔；默认 config.default_queue")
    worker.add_argument("-c", "--concurrency", type=int, default=None)
    worker.add_argument("--prefetch", type=int, default=None)
    worker.add_argument("--once", action="store_true", help="跑空即退出（调试/CI 用）")

    status = sub.add_parser("status", help="队列深度 / 优先级分布")
    status.add_argument("-Q", "--queues", default="")
    status.add_argument("--by-priority", action="store_true", help="按优先级分桶显示 pending")

    dlq = sub.add_parser("dlq", help="死信队列")
    dlq_sub = dlq.add_subparsers(dest="dlq_command", required=True)
    dlq_list = dlq_sub.add_parser("list", help="列出 DLQ")
    dlq_list.add_argument("-Q", "--queue", default=None)
    dlq_replay = dlq_sub.add_parser("replay", help="重放（attempt 归 1）")
    dlq_replay.add_argument("--all", action="store_true")
    dlq_replay.add_argument("--id", type=int, default=None)
    dlq_replay.add_argument("-Q", "--queue", default=None)
    dlq_replay.add_argument("--priority", type=int, default=None)

    beat = sub.add_parser("beat", help="定时调度器（lease 选主）")
    beat.add_argument(
        "--schedule",
        default=os.environ.get("TASKMQ_SCHEDULE"),
        help="module:attr（App 或 Schedule 列表）；默认用 App 里 app.schedule(...) 注册的",
    )
    beat.add_argument(
        "--state", default=os.environ.get("TASKMQ_BEAT_STATE", "taskmq.beat.json"), help="状态文件"
    )
    beat.add_argument("--poll", type=float, default=1.0, help="tick 间隔秒")
    beat.add_argument("--once", action="store_true", help="只推进一轮（测试/外部 cron 驱动）")

    dev = sub.add_parser("dev", help="本地开发：worker + beat 同一进程")
    dev.add_argument("-Q", "--queues", default="", help="逗号分隔；默认 config.default_queue")
    dev.add_argument("-c", "--concurrency", type=int, default=None)
    dev.add_argument("--schedule", default=os.environ.get("TASKMQ_SCHEDULE"))
    dev.add_argument("--state", default=os.environ.get("TASKMQ_BEAT_STATE", "taskmq.beat.json"))

    call = sub.add_parser("call", help="在本进程同步执行一次（调试）")
    call.add_argument("task", help="任务名或 module.qualname")
    call.add_argument("--args", default="[]", help="JSON 数组")
    call.add_argument("--kwargs", default="{}", help="JSON 对象")
    call.add_argument("-Q", "--queue", default=None)
    call.add_argument("--priority", type=int, default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not args.app:
        parser.error("需要 --app module:attr 或环境变量 TASKMQ_APP")

    app = _load_app(args.app)
    if args.command == "worker":
        return _cmd_worker(app, args)
    if args.command == "status":
        return _cmd_status(app, args)
    if args.command == "dlq":
        return _cmd_dlq(app, args)
    if args.command == "call":
        return _cmd_call(app, args)
    if args.command == "beat":
        return _cmd_beat(app, args)
    if args.command == "dev":
        return _cmd_dev(app, args)
    parser.error(f"未知命令：{args.command}")
    return 2


def _cmd_worker(app: App, args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s" if app.config.log_format == "pretty"
        else '{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
    )
    worker = Worker(
        app,
        queues=_queues(args.queues),
        concurrency=args.concurrency,
        prefetch=args.prefetch,
        app_spec=args.app,
    )
    stopping = {"flag": False}

    def _request_stop(signum: int, frame: Any) -> None:
        stopping["flag"] = True
        logger.info("收到信号 %s，优雅退出（停止拉取，跑完在途任务）", signum)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _request_stop)

    try:
        if args.once:
            worker.run_until_idle(timeout=app.config.shutdown_timeout)
            return 0
        while not stopping["flag"]:
            worker.poll()
            time.sleep(app.config.poll_interval)
    except KeyboardInterrupt:  # pragma: no cover - 交互式
        pass
    finally:
        worker.close()
    return 0


def _cmd_status(app: App, args: argparse.Namespace) -> int:
    queues = _queues(args.queues)
    stats = app.transport.queue_stats(queues)
    if not stats:
        print("(没有队列)")
    else:
        print("QUEUES")
        for stat in stats:
            print(
                f"  {stat.queue:<16} pending={stat.pending:<6}"
                f" inflight={stat.inflight:<4} dead={stat.dead}"
            )
    if args.by_priority:
        buckets = app.transport.priority_stats(queues)
        print("BY-PRIORITY (pending)")
        if not buckets:
            print("  (空)")
        for priority, count in buckets.items():
            print(f"  P{priority:<4} {count}")
    return 0


def _cmd_dlq(app: App, args: argparse.Namespace) -> int:
    transport = app.transport
    if args.dlq_command == "list":
        entries = transport.dead_letters(queue=args.queue)
        if not entries:
            print("(DLQ 为空)")
            return 0
        for entry in entries:
            reason = entry.reason.splitlines()[0][:80] if entry.reason else ""
            print(
                f"  id={entry.message_id:<6} job={entry.job_id} queue={entry.queue:<12} "
                f"task={entry.task} deliveries={entry.deliveries} reason={reason}"
            )
        return 0

    priority = args.priority
    if priority is not None:
        validate_priority(priority, where="dlq replay --priority")
    if args.id is not None:
        targets = [args.id]
    elif args.all:
        targets = [entry.message_id for entry in transport.dead_letters(queue=args.queue)]
    else:
        raise SystemExit("dlq replay 需要 --all 或 --id N")

    replayed = sum(
        1 for message_id in targets if transport.replay_dead(message_id, queue=args.queue, priority=priority)
    )
    print(f"replayed {replayed}/{len(targets)}")
    return 0 if replayed else 1


def _cmd_call(app: App, args: argparse.Namespace) -> int:
    try:
        call_args = json.loads(args.args)
        call_kwargs = json.loads(args.kwargs)
    except ValueError as exc:
        raise SystemExit(f"--args/--kwargs 必须是合法 JSON：{exc}") from exc
    if not isinstance(call_args, list) or not isinstance(call_kwargs, dict):
        raise SystemExit("--args 需要 JSON 数组，--kwargs 需要 JSON 对象")
    try:
        result = app.call(
            args.task, call_args, call_kwargs, queue=args.queue, priority=args.priority
        )
    except TaskMQError as exc:
        print(f"FAILED: {error_text(exc)}", file=__import__("sys").stderr)
        return 1
    print(json.dumps(result, default=str, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
