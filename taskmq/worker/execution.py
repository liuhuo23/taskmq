"""任务执行体：钩子 + run + 重试决策，结果可序列化（进程池的子进程也能跑全套）。

抽出来的原因：`processes` 池要在**子进程**里执行 `before_start → run → on_success/on_retry/on_failure
→ after_return`（钩子管理进程内资源是常见用法），而状态写入/ack/入队必须留在父进程。
所以这里只做「纯执行」，返回一个可 pickle 的 `BodyOutcome`；父进程据此决定 ack / 重试 / DLQ。
"""
from __future__ import annotations

import dataclasses
import logging
import time
import traceback
from collections.abc import Callable
from typing import Any

from .._compat import _SLOTS
from ..errors import Reject, RetryRequest, error_text
from ..protocol import ACK_ON_COMPLETION, ACK_ON_SUCCESS, Envelope
from ..task import Task, TaskContext
from ..transport.base import JobState

__all__ = [
    "BodyOutcome",
    "ChildTask",
    "execute_task",
    "decide_retry",
    "call_body_sync",
    "init_child",
    "run_child_task",
]

logger = logging.getLogger("taskmq.child")
_CHILD_APP: Any = None


@dataclasses.dataclass(frozen=True, **_SLOTS)
class BodyOutcome:
    """一次执行的结果（可 pickle，能跨进程回传）。"""

    ok: bool
    state: str
    result: Any = None
    retry_delay: float | None = None
    error_type: str = ""
    error: str = ""
    detail: str = ""
    runtime: float = 0.0
    hook_error: str = ""

    @staticmethod
    def timeout(seconds: float) -> BodyOutcome:
        """硬超时被 kill：没有钩子可跑（进程已经没了），重试决策交给父进程。"""
        return BodyOutcome(
            ok=False,
            state=JobState.FAILED,
            error_type="HardTimeout",
            error=f"任务超过 hard_timeout={seconds}s，子进程已被 kill",
            detail=f"HardTimeout: 超过 hard_timeout={seconds}s，子进程已被 kill",
        )


@dataclasses.dataclass(frozen=True, **_SLOTS)
class ChildTask:
    """发给子进程的执行请求（必须可 pickle）。"""

    app_spec: str
    envelope: Envelope
    worker_id: str
    attempt: int
    deliveries: int
    ack_mode: str = ACK_ON_SUCCESS
    hard_timeout: float | None = None
    plugins: tuple[str, ...] = ()          # 子进程要重建同样的插件注册表（docs/design/plugins.md §5）


def init_child(app_spec: str) -> None:
    """子进程初始化：导入 App（注册表/transport 都在子进程内重新建立）。"""
    global _CHILD_APP
    from ..loader import load_app

    _CHILD_APP = load_app(app_spec)


def run_child_task(payload: ChildTask) -> BodyOutcome:
    """子进程侧入口：跑完整套钩子 + 任务体，返回可 pickle 的结果。"""
    if payload.plugins:
        from ..plugins import load_plugins

        load_plugins(list(payload.plugins), entry_points=False)
    app = _CHILD_APP
    if app is None:  # 独立进程模式（可强杀路径）
        init_child(payload.app_spec)
        app = _CHILD_APP
    task = app.task_for(payload.envelope.task)
    if task is None:
        return BodyOutcome(
            ok=False,
            state=JobState.FAILED,
            error_type="TaskNotRegistered",
            error=f"子进程没有注册任务 {payload.envelope.task}",
            detail=f"TaskNotRegistered: {payload.envelope.task}",
        )
    ctx = TaskContext(
        app=app,
        envelope=payload.envelope,
        worker_id=payload.worker_id,
        attempt=payload.attempt,
        deliveries=payload.deliveries,
        priority=payload.envelope.priority,
        queue=payload.envelope.queue,
        deadline=payload.envelope.deadline,
        log=logging.getLogger("taskmq.task").getChild(payload.envelope.task),
    )
    return execute_task(task, ctx, call_body_sync, ack_mode=payload.ack_mode)


def call_body_sync(fn: Callable[..., Any], args: tuple, kwargs: dict) -> Any:
    """在同步池里调用任务体。`async def` 任务不允许走到这里（启动即校验）。"""
    return fn(*args, **kwargs)


def _safe_hook(task: Task, name: str, ctx: TaskContext, *args: Any) -> str:
    """调用钩子；异常只记录并返回错误文本（不改投递语义，docs/design/tasks.md §5）。"""
    hook = getattr(task, name, None)
    if hook is None:
        return ""
    try:
        hook(ctx, *args)
    except BaseException as exc:  # noqa: BLE001 - 钩子绝不能影响投递语义
        return f"{name}: {type(exc).__name__}: {exc}"
    return ""


def decide_retry(
    task: Task,
    attempt: int,
    deliveries: int,
    exc: BaseException,
    *,
    ack_mode: str,
) -> float | None:
    """纯函数：返回退避秒数；`None` 表示不重试（进 DLQ）。"""
    if isinstance(exc, Reject):
        return None
    if ack_mode == ACK_ON_COMPLETION:
        return None
    policy = task.retry_policy
    if isinstance(exc, RetryRequest):
        if deliveries >= task.max_deliveries:
            return None
        if policy is not None and attempt >= policy.max_attempts:
            return None
        if exc.delay is not None:
            return max(0.0, float(exc.delay))
        return policy.delay_for(attempt) if policy is not None else 0.0
    if policy is None or not policy.should_retry(exc):
        return None
    if attempt >= policy.max_attempts:
        return None
    if deliveries >= task.max_deliveries:
        return None
    return policy.delay_for(attempt)


def execute_task(
    task: Task,
    ctx: TaskContext,
    call_body: Callable[[Callable[..., Any], tuple, dict], Any] = call_body_sync,
    *,
    ack_mode: str,
) -> BodyOutcome:
    """执行一次投递：钩子 + 任务体 + 重试决策。

    - `before_start` 抛异常 = 任务失败（走正常失败路径，可能重试/DLQ）；
    - `on_success` / `on_retry` / `on_failure` / `after_return` 的异常只记进
      `BodyOutcome.hook_error`，**不改投递语义**。
    """
    started = time.monotonic()
    state = JobState.FAILED
    result: Any = None
    failure: BaseException | None = None
    hook_error = ""
    retry_delay: float | None = None
    detail = ""
    error_type = ""
    error = ""

    try:
        # before_start 是前置条件：它的异常要当任务失败，所以不走 _safe_hook
        task.before_start(ctx)
        result = call_body(task.run, ctx.envelope.args, dict(ctx.envelope.kwargs))
    except BaseException as exc:
        failure = exc
        error_type = type(exc).__name__
        error = error_text(exc)
        detail = f"{error}\n{traceback.format_exc()}"
        retry_delay = decide_retry(task, ctx.attempt, ctx.deliveries, exc, ack_mode=ack_mode)
        if retry_delay is None:
            state = JobState.FAILED
            hook_error += _safe_hook(task, "on_failure", ctx, exc)
        else:
            state = JobState.RETRYING
            hook_error += _safe_hook(task, "on_retry", ctx, exc, retry_delay)
    else:
        state = JobState.SUCCEEDED
        runtime = round(time.monotonic() - started, 6)
        hook_error += _safe_hook(task, "on_success", ctx, result, runtime)

    runtime = round(time.monotonic() - started, 6)
    hook_error += _safe_hook(task, "after_return", ctx, state, result, failure)

    return BodyOutcome(
        ok=state == JobState.SUCCEEDED,
        state=state,
        result=result,
        retry_delay=retry_delay,
        error_type=error_type,
        error=error,
        detail=detail,
        runtime=runtime,
        hook_error=hook_error,
    )
