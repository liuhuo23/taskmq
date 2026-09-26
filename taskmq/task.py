"""任务定义、句柄与运行时上下文。

两种等价写法（docs/design/tasks.md）：

~~~python
# 类式：可继承、可 mixin、可覆写生命周期钩子
class EmailTask(Task):
    queue = "email"
    retry_policy = Retry(max_attempts=5)     # 注意：策略叫 retry_policy，retry 留给 self.retry()

    def __init__(self, app):        # 每进程一次：进程级资源
        super().__init__(app)
        self.smtp = SmtpClient()

    def before_start(self, ctx): self.smtp.acquire()
    def run(self, to: str) -> str: return self.smtp.send(to)
    def after_return(self, ctx, state, result=None, exc=None): self.smtp.release()

app.register(EmailTask)

# 函数式：`@app.task` 是糖；`bind=True` 时首参注入任务实例
@app.task(queue="email", bind=True)
def send_email(self, to: str) -> str:
    self.request.log.info("sending", attempt=self.request.attempt)
    return smtp_send(to)
~~~

约定：
- `self` 是**每进程一个**的任务实例，只放进程级资源；请求级状态放 `self.request`（= `TaskContext`）。
- `self.request` 就是当前 `TaskContext`，执行期外访问抛 `TaskError`（不做 Celery 那种隐式代理）。
- 钩子不能改 args/kwargs，也不能改 ack/重试/DLQ 的投递语义。
"""
from __future__ import annotations

import contextvars
import dataclasses
import functools
import logging
import random
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Generic, ParamSpec, TypeVar, cast

from .errors import Reject, RemoteError, RetryRequest, TaskError, TaskTimeout
from .protocol import ACK_ON_SUCCESS, Envelope
from .ratelimit import RateLimit
from .transport.base import Delivery, JobRecord, JobState

logger = logging.getLogger("taskmq.task")

P = ParamSpec("P")
R = TypeVar("R")


# --------------------------------------------------------------------- Retry
@dataclasses.dataclass(frozen=True, slots=True)
class Retry:
    """声明式重试策略（§11.2）。`retry_on` 是白名单；不在白名单的异常不重试。"""

    max_attempts: int = 5
    backoff: str = "exp"
    base: float = 1.0
    factor: float = 2.0
    max_delay: float = 600.0
    jitter: bool = True
    retry_on: tuple[type[BaseException], ...] = (Exception,)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts 必须 >= 1，收到 {self.max_attempts}")
        if self.backoff not in ("fixed", "linear", "exp"):
            raise ValueError(f'backoff 可选 "fixed" | "linear" | "exp"，收到 {self.backoff!r}')
        if self.base < 0 or self.max_delay < 0:
            raise ValueError("base / max_delay 不能为负")
        if not self.retry_on:
            raise ValueError("retry_on 不能为空；不想重试就不要给 retry 策略")

    def should_retry(self, exc: BaseException) -> bool:
        if isinstance(exc, Reject):
            return False
        return isinstance(exc, self.retry_on)

    def delay_for(self, attempt: int) -> float:
        """退避计算：`min(max_delay, base * factor**(attempt-1)) * uniform(0.5, 1.5)`。

        传入的是**当前尝试序号**（第 1 次失败后传 1）。
        """
        step = max(1, int(attempt))
        if self.backoff == "fixed":
            delay = self.base
        elif self.backoff == "linear":
            delay = self.base * step
        else:
            delay = self.base * (self.factor ** (step - 1))
        delay = min(float(self.max_delay), delay)
        if self.jitter:
            delay = delay * random.uniform(0.5, 1.5)
        return max(0.0, delay)


# ----------------------------------------------------------------- 上下文变量
_current: contextvars.ContextVar[TaskContext | None] = contextvars.ContextVar(
    "taskmq_current_task", default=None
)


def current_task() -> TaskContext:
    ctx = _current.get()
    if ctx is None:
        raise TaskError("不在任务执行上下文中：current_task() 只能在任务函数内调用")
    return ctx


def current_app() -> Any:
    return current_task().app


def run_hook(task: Task, name: str, ctx: TaskContext, *args: Any) -> None:
    """调用生命周期钩子；钩子异常**只记录、不改投递语义**（docs/design/tasks.md §5）。

    `before_start` 不走这里——它是任务前置条件，抛异常即任务失败。
    """
    hook = getattr(task, name, None)
    if hook is None:
        return
    try:
        hook(ctx, *args)
    except BaseException:  # noqa: BLE001 - 钩子绝不能影响投递语义
        logger.exception("钩子 %s 抛异常（不影响投递语义）job=%s", name, ctx.id)


def _set_current(ctx: TaskContext) -> contextvars.Token:
    return _current.set(ctx)


def _reset_current(token: contextvars.Token) -> None:
    _current.reset(token)


@dataclasses.dataclass
class TaskContext:
    """任务运行时的显式上下文（§7.5）；同时就是 `self.request`。"""

    app: Any
    envelope: Envelope
    worker_id: str
    attempt: int
    deliveries: int
    priority: int
    queue: str
    delivery: Delivery | None = None
    deadline: float | None = None
    cancelled: threading.Event | None = None
    log: logging.Logger = dataclasses.field(default_factory=lambda: logger)

    @property
    def id(self) -> str:
        return self.envelope.id

    @property
    def task(self) -> str:
        return self.envelope.task

    @property
    def args(self) -> tuple[Any, ...]:
        """本次投递的任务参数（只读快照）。"""
        return self.envelope.args

    @property
    def kwargs(self) -> Mapping[str, Any]:
        """本次投递的任务关键字参数（只读快照）。"""
        return self.envelope.kwargs

    @property
    def retries(self) -> int:
        """Celery 语义：已经重试过几次（首次执行 = 0）。"""
        return max(0, self.attempt - 1)

    @property
    def hostname(self) -> str:
        """执行本任务的 worker id（Celery 的 `request.hostname`）。"""
        return self.worker_id

    @property
    def eta(self) -> float | None:
        return self.envelope.eta

    def remaining(self) -> float | None:
        """软超时剩余秒数；没配 timeout 时返回 None。"""
        if self.deadline is None:
            return None
        return max(0.0, self.deadline - time.time())

    def check_timeout(self) -> None:
        remaining = self.remaining()
        if remaining is not None and remaining <= 0:
            raise TaskTimeout(f"任务 {self.task} 超过软超时（协作式，仅在被调用处生效）")

    def check_cancelled(self) -> None:
        """协作式取消检查点（P19）：只有显式 `terminate` 才会置位。"""
        if self.cancelled is not None and self.cancelled.is_set():
            raise TaskError(f"任务 {self.task} 已被取消（协作式检查点）")

    def heartbeat(self) -> None:
        """长任务手动续租，避免租约到期被判定为孤儿。"""
        if self.delivery is None:
            return
        transport = getattr(self.app, "transport", None)
        if transport is None:
            return
        transport.extend_lease(self.delivery, getattr(self.app.config, "lease", 60.0))

    def update_meta(self, **meta: Any) -> None:
        """进度/阶段上报（Celery `self.update_state` 的等价物），写进 JobRecord.meta。"""
        transport = getattr(self.app, "transport", None)
        if transport is None:
            return
        record = transport.get_state(self.id)
        state = record.state if record is not None else JobState.RUNNING
        transport.set_state(self.id, state, task=self.task, attempt=self.attempt, **meta)

    def retry(self, reason: str = "", *, delay: float | None = None) -> RetryRequest:
        """返回要抛出的异常：`raise ctx.retry("boom")`。"""
        return RetryRequest(reason, delay=delay)

    def reject(self, reason: str = "") -> Reject:
        """返回要抛出的异常：立即进 DLQ，不重试。"""
        return Reject(reason)

    def publish(self, target: Any, *args: Any, **kwargs: Any) -> TaskHandle[Any]:
        """在任务里派发子任务；**默认继承父任务优先级**（P6），可显式覆盖。"""
        priority = kwargs.pop("priority", None)
        if priority is None:
            priority = self.priority
        task = target if isinstance(target, Task) else self.app.task_for(target)
        if task is None:
            raise TaskError(f"未注册的任务：{target!r}")
        return self.app.submit(task, args, kwargs, priority=priority)


# --------------------------------------------------------------------- 句柄
class TaskHandle(Generic[R]):
    """任务句柄（§7.4）：轻量、不阻塞、所有状态来自 transport。"""

    def __init__(self, app: Any, job_id: str) -> None:
        self._app = app
        self.id = job_id

    def __repr__(self) -> str:
        return f"<TaskHandle {self.id} state={self.state}>"

    def _record(self) -> JobRecord | None:
        return self._app.transport.get_state(self.id)

    @property
    def state(self) -> str:
        record = self._record()
        return record.state if record is not None else JobState.PENDING

    @property
    def info(self) -> dict[str, Any]:
        record = self._record()
        if record is None:
            return {}
        info = dict(record.meta)
        info.update(
            {
                "job_id": record.job_id,
                "task": record.task,
                "state": record.state,
                "attempt": record.attempt,
                "updated_at": record.updated_at,
            }
        )
        if record.error:
            info["error"] = record.error
        return info

    def ready(self) -> bool:
        return self.state in JobState.TERMINAL

    def successful(self) -> bool:
        return self.state == JobState.SUCCEEDED

    def failed(self) -> bool:
        return self.state in (JobState.FAILED, JobState.EXPIRED, JobState.REVOKED)

    def wait(self, timeout: float | None = None, *, poll: float | None = None) -> str:
        """只等状态，不取结果；超时抛 `TimeoutError`。"""
        interval: float = (
            poll if poll is not None else float(getattr(self._app.config, "poll_interval", 0.05))
        )
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            state = self.state
            if state in JobState.TERMINAL:
                return state
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"等待 {self.id} 状态超时（当前 {state}）")
            time.sleep(interval)

    def get(self, timeout: float | None = None, *, poll: float | None = None) -> R:
        """阻塞取结果；任务失败抛 `RemoteError`（含远端 traceback 文本）。"""
        state = self.wait(timeout, poll=poll)
        record = self._record()
        if state == JobState.SUCCEEDED:
            value: Any = record.result if record is not None and record.has_result else None
            return cast(R, value)
        error = record.error if record is not None else None
        raise RemoteError(f"任务 {self.id} 状态为 {state}", error=error, traceback=error)

    def forget(self) -> None:
        """删除 job 状态（结果清理）。"""
        self._app.transport.set_state(self.id, JobState.REVOKED, error="forgotten by caller")


# --------------------------------------------------------------------- Task
class Task(Generic[P, R]):
    """任务基类：既可被 `@app.task` 用来包装函数，也可被继承。

    静态策略（`queue/priority/retry/...`）既可以写成**类属性**，也可以通过装饰器参数传入；
    参数优先。`run` 是唯一必须实现的方法。
    """

    # 类级默认值（子类可覆盖）
    name: str = ""
    queue: str | None = None
    priority: int | None = None
    retry_policy: Retry | None = None
    timeout: float | None = None
    hard_timeout: float | None = None   # 硬超时：仅 processes 池能强杀（§10.6）
    ack: str | None = None
    expires: float | None = None
    max_deliveries: int = 0  # 0 = 未设置，按 config / Retry 推导
    rate_limit: str | RateLimit | None = None   # "100/m" 或 RateLimit；Worker 侧建令牌桶
    concurrency_key: str | None = None
    serializer: str | None = None
    store_result: bool = True

    def __init__(
        self,
        app: Any,
        func: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        queue: str | None = None,
        priority: int | None = None,
        retry: Retry | None = None,
        timeout: float | None = None,
        hard_timeout: float | None = None,
        ack: str | None = None,
        expires: float | None = None,
        max_deliveries: int | None = None,
        rate_limit: str | RateLimit | None = None,
        concurrency_key: str | None = None,
        serializer: str | None = None,
        store_result: bool | None = None,
        bind: bool = False,
    ) -> None:
        cls = type(self)
        if func is not None and not callable(func):
            raise TaskError(f"@task 只能装饰可调用对象，收到 {func!r}")

        self.app = app
        self._callable = func
        self._bind = bool(bind)
        self.name = name if name is not None else (
            cls.name or self._default_name(func) or f"{cls.__module__}.{cls.__qualname__}"
        )
        self.queue = queue if queue is not None else cls.queue
        self.priority = priority if priority is not None else cls.priority
        self.retry_policy = retry if retry is not None else cls.retry_policy
        self.timeout = timeout if timeout is not None else cls.timeout
        self.hard_timeout = hard_timeout if hard_timeout is not None else cls.hard_timeout
        if self.hard_timeout is not None and self.hard_timeout <= 0:
            raise ValueError(f"hard_timeout 必须 > 0，收到 {self.hard_timeout}")
        self.ack = ack if ack is not None else (cls.ack or ACK_ON_SUCCESS)
        self.expires = expires if expires is not None else cls.expires
        resolved_rate_limit = rate_limit if rate_limit is not None else cls.rate_limit
        if resolved_rate_limit is not None:
            RateLimit.parse(resolved_rate_limit)   # 构造即校验（fail fast）
        self.rate_limit = resolved_rate_limit
        self.concurrency_key = concurrency_key if concurrency_key is not None else cls.concurrency_key
        self.serializer = serializer if serializer is not None else cls.serializer
        self.store_result = store_result if store_result is not None else cls.store_result

        configured_max = int(getattr(getattr(app, "config", None), "max_deliveries", 5))
        ceiling = self.retry_policy.max_attempts if self.retry_policy is not None else 0
        explicit_max = max_deliveries if max_deliveries is not None else (cls.max_deliveries or None)
        self.max_deliveries = explicit_max if explicit_max is not None else max(configured_max, ceiling)

        if func is not None:
            functools.update_wrapper(self, func)

    # ------------------------------------------------------------- 基础
    @staticmethod
    def _default_name(func: Callable[..., Any] | None) -> str:
        if func is None:
            return ""
        module = getattr(func, "__module__", "") or ""
        qualname = getattr(func, "__qualname__", None) or getattr(func, "__name__", "task")
        return f"{module}.{qualname}"

    def __repr__(self) -> str:
        return f"<Task {self.name} queue={self.queue or 'default'} priority={self.priority}>"

    @property
    def is_async(self) -> bool:
        """是不是 `async def` 任务（只能跑在 asyncio 池，决策见 §10.2）。"""
        import inspect

        if inspect.iscoroutinefunction(self.run):
            return True
        callee = self._callable
        return callee is not None and inspect.iscoroutinefunction(callee)

    @property
    def request(self) -> TaskContext:
        """当前请求上下文（= `ctx`）；执行期外访问抛 `TaskError`。"""
        return current_task()

    def run(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """任务体。类式任务覆写本方法；函数式任务由装饰器注入。"""
        if self._callable is None:
            raise NotImplementedError(f"{type(self).__name__} 必须实现 run()")
        if self._bind:
            return self._callable(self, *args, **kwargs)
        return self._callable(*args, **kwargs)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.run(*args, **kwargs)

    def retry(
        self,
        *,
        reason: str = "",
        countdown: float | None = None,
        delay: float | None = None,
        exc: BaseException | None = None,
        max_retries: int | None = None,
    ) -> RetryRequest:
        """返回要抛出的 `RetryRequest`（需 `raise`）。

        `countdown` 是 Celery 叫法，等价于 `delay`；`exc` 只用于生成原因文本；
        `max_retries` 仅作兼容占位——重试次数由 `Retry` 策略与 `max_deliveries` 决定。
        """
        wait = delay if delay is not None else countdown
        note = reason or (f"{type(exc).__name__}: {exc}" if exc is not None else "")
        return RetryRequest(note, delay=wait)

    # ------------------------------------------------------------- 钩子
    def before_start(self, ctx: TaskContext) -> None:
        """`run` 之前（资源获取/上下文设置）。抛异常 = 任务失败。"""

    def on_success(self, ctx: TaskContext, result: Any, runtime: float) -> None:
        """成功后。抛异常不改投递语义。"""

    def on_retry(self, ctx: TaskContext, exc: BaseException, delay: float) -> None:
        """已决定重试、新消息入队之后。抛异常不改投递语义。"""

    def on_failure(self, ctx: TaskContext, exc: BaseException) -> None:
        """已决定进 DLQ 之后。抛异常不改投递语义。"""

    def after_return(
        self,
        ctx: TaskContext,
        state: str,
        result: Any = None,
        exc: BaseException | None = None,
    ) -> None:
        """`finally` 语义，成功/重试/失败都会执行。不依赖它做关键清理（硬超时被 kill 时不会跑）。"""

    # ------------------------------------------------------------- 提交
    def delay(self, *args: P.args, **kwargs: P.kwargs) -> TaskHandle[R]:
        """语法糖：**只传任务参数**（参数类型有静态检查）。

        需要 `queue/priority/eta/key/...` 这些调度选项时用 `apply_async()` 或 `submit()`，
        这样保留选项名不会和任务 kwargs 混淆。
        """
        return cast(TaskHandle[R], self.app.submit(self, args, kwargs))

    def submit(self, *args: Any, **options: Any) -> TaskHandle[R]:
        """全功能入口：`queue/priority/eta/expires/timeout/key/headers/ack` 是保留选项名，

        其余关键字参数进任务 kwargs（撞名时用 `apply_async()`）。
        """
        return cast(TaskHandle[R], self.app.submit(self, args, {}, **options))

    def apply_async(
        self,
        args: Sequence[Any] = (),
        kwargs: Mapping[str, Any] | None = None,
        **options: Any,
    ) -> TaskHandle[R]:
        """任务参数（`args`/`kwargs`）与调度选项完全分离，不会撞名。"""
        return cast(TaskHandle[R], self.app.submit(self, args, kwargs or {}, **options))


__all__ = [
    "Retry",
    "Task",
    "TaskHandle",
    "TaskContext",
    "current_task",
    "current_app",
    "run_hook",
]
