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
import json
import logging
import os
import signal
import sys
import threading
import time
from collections.abc import Sequence
from typing import Any

from . import __version__
from .app import App
from .errors import ConfigError, TaskMQError, error_text
from .loader import load_app, load_attr
from .plugins import PLUGINS_ENV, load_plugins
from .priority import validate_priority
from .schedule import Schedule
from .transport.base import JobState
from .worker.beat import Beat
from .worker.runner import Worker
from .workflow import RUN_META_WORKFLOW, RUN_PREFIX

logger = logging.getLogger("taskmq.cli")


def _load_app(spec: str) -> App:
    """`module:attr` -> App 实例（复用 `loader`，错误转成 CLI 友好的 SystemExit）。"""
    try:
        return load_app(spec)
    except ConfigError as exc:
        raise SystemExit(str(exc)) from exc


def _load_object(spec: str) -> Any:
    """任意 `module:attr` 加载（`--schedule` 用）。"""
    try:
        return load_attr(spec)
    except ConfigError as exc:
        raise SystemExit(str(exc)) from exc


def _load_schedules(app: App, spec: str | None) -> list[Schedule]:
    """调度来源：默认用 App 里注册的；也可用 `module:attr` 指向 App 或 Schedule 列表。"""
    if not spec:
        return app.schedules
    obj = _load_object(spec)
    if isinstance(obj, App):
        return obj.schedules
    if isinstance(obj, (list, tuple)) and all(isinstance(item, Schedule) for item in obj):
        return list(obj)
    raise SystemExit(f"--schedule {spec} 需要 App 或 Schedule 列表，拿到 {type(obj).__name__}")


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
    parser.add_argument(
        "--plugins",
        default="",
        help=f"逗号分隔的插件模块（也读环境变量 {PLUGINS_ENV}）",
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

    workflow = sub.add_parser("workflow", help="DAG 工作流：列表 / 状态 / 补偿推进")
    workflow_sub = workflow.add_subparsers(dest="workflow_command", required=True)
    workflow_sub.add_parser("list", help="运行中的工作流")
    workflow_status = workflow_sub.add_parser("status", help="某次运行的每节点状态")
    workflow_status.add_argument("run", help="运行 id（wf-...）")
    workflow_resume = workflow_sub.add_parser("resume", help="补偿推进（幂等）")
    workflow_resume.add_argument("run", help="运行 id（wf-...）")

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

    if args.plugins:
        try:                                             # 先加载插件，App 才会看到注册的 scheme/pool/sink
            modules = [item.strip() for item in args.plugins.split(",") if item.strip()]
            load_plugins(modules, entry_points=False)
        except ConfigError as exc:
            raise SystemExit(str(exc)) from exc

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
    if args.command == "workflow":
        return _cmd_workflow(app, args)
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


def _cmd_beat(app: App, args: argparse.Namespace) -> int:
    schedules = _load_schedules(app, args.schedule)
    if not schedules:
        raise SystemExit("没有调度：用 app.schedule(...) 注册，或 --schedule module:attr 指定")
    runner = Beat(app, schedules, state_path=args.state, poll_interval=args.poll)

    if args.once:
        fired = runner.tick()
        summary = f"fired {len(fired)}" + (f": {', '.join(fired)}" if fired else "")
        print(summary)
        runner.close()
        return 0

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stopped = {"flag": False}

    def _request_stop(signum: int, frame: Any) -> None:
        stopped["flag"] = True
        runner.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _request_stop)
    try:
        while not stopped["flag"]:
            runner.tick()
            time.sleep(max(0.05, args.poll))
    except KeyboardInterrupt:  # pragma: no cover - 交互式
        pass
    finally:
        runner.close()
    print(f"beat 退出：fired={runner.fired} skipped={runner.skipped} standby={runner.standby}")
    return 0


def _cmd_dev(app: App, args: argparse.Namespace) -> int:
    """本地开发：worker + beat 同进程（生产请分开跑，§13）。"""
    schedules = _load_schedules(app, args.schedule)
    runner = Beat(app, schedules, state_path=args.state) if schedules else None
    worker = Worker(
        app, queues=_queues(args.queues), concurrency=args.concurrency, app_spec=args.app
    )
    stopped = threading.Event()

    def _request_stop(signum: int, frame: Any) -> None:
        stopped.set()
        worker.stop()
        if runner is not None:
            runner.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _request_stop)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if runner is not None:
        threading.Thread(target=runner.run_forever, name="taskmq-beat", daemon=True).start()
    try:
        while not stopped.is_set():
            worker.poll()
            time.sleep(app.config.poll_interval)
    except KeyboardInterrupt:  # pragma: no cover - 交互式
        pass
    finally:
        if runner is not None:
            runner.close()
        worker.close()
    return 0


def _cmd_workflow(app: App, args: argparse.Namespace) -> int:
    """DAG 工作流的 list / status / resume（docs/design/workflows.md §5）。"""
    if not app.workflows:
        raise SystemExit("没有注册工作流：用 @app.workflow(\"...\") 定义")

    if args.workflow_command == "list":
        if not getattr(app.transport, "supports_job_listing", False):
            print("(该 transport 不支持 job 枚举，无法列出运行)")
            return 0
        runs = app.transport.list_jobs(
            prefix=RUN_PREFIX, states=[JobState.RUNNING], limit=50
        )
        if not runs:
            print("(没有运行中的工作流)")
            return 0
        print("RUNS")
        for record in runs:
            name = record.meta.get(RUN_META_WORKFLOW, "?")
            print(f"  {record.job_id}  workflow={name}  state={record.state}")
        return 0

    if args.workflow_command == "status":
        handle = app.handle_workflow(args.run)
        status = handle.status()
        print(f"RUN {status.run_id}  workflow={status.workflow}  state={status.state}")
        for name, node in status.nodes.items():
            deps = ",".join(handle.plan.by_name[name].upstreams) or "-"
            print(
                f"  {name:<16} state={node.state:<10} attempt={node.attempt}"
                f" deps={deps} task={node.task}"
            )
        return 1 if status.state == JobState.FAILED else 0

    state = app.resume_workflow(args.run)
    print(f"resumed {args.run} -> {state}")
    return 1 if state == JobState.FAILED else 0


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
    limitations = getattr(app.transport, "limitations", None) or {}
    if limitations:
        print("LIMITATIONS（transport 主动声明的语义降级）")
        for key, text in limitations.items():
            print(f"  {key}: {text}")
    if app.workflows:
        pending = app.pending_workflows(limit=50)
        print("WORKFLOWS")
        print(f"  registered={len(app.workflows)} running={len(pending)}")
    workers = app.transport.list_workers()
    if workers:
        now = time.time()
        stale_after = max(3 * app.config.heartbeat_interval, 30.0)
        print("WORKERS")
        for worker in workers:
            age = now - worker.heartbeat_at if worker.heartbeat_at else float("inf")
            flag = "" if worker.alive(now=now, stale_after=stale_after) else "  [stale]"
            print(
                f"  {worker.worker_id:<30} queues={','.join(worker.queues) or '-'}"
                f" pool={worker.pool or '-'} concurrency={worker.concurrency}"
                f" heartbeat={age:.0f}s ago{flag}"
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
        try:
            validate_priority(priority, where="dlq replay --priority")
        except ConfigError as exc:                     # CLI 给友好提示，而不是 backtrack
            raise SystemExit(str(exc)) from exc
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
        print(f"FAILED: {error_text(exc)}", file=sys.stderr)
        return 1
    except Exception as exc:  # eager 调用：任务体异常原样抛出，翻译成可读结果
        print(f"FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, default=str, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
