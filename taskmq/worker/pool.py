"""执行池：`solo` / `threads` / `asyncio` / `processes`。

Worker 依赖的契约：

- `submit(fn)`：把一段**同步**调用（建 ctx、写状态、ack/nack）放到池的槽位上跑。
  `SoloPool` 同步执行并返回 `None`；其余返回 Future（`None` 表示「已提交给执行池」= 已开始）。
- `call_body(fn, args, kwargs)`：调用任务体。`asyncio` 池遇到 awaitable 会丢到事件循环上跑。
- `runs_in_child=True`（`processes`）：`before_start → run → 钩子` 全部在子进程里执行，
  Worker 改调 `run_remote(payload)`，结果（`BodyOutcome`，可 pickle）回传父进程。
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import multiprocessing
import threading
import traceback
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

from ..errors import ConfigError
from ..transport.base import JobState
from .execution import BodyOutcome, ChildTask, init_child, run_child_task


@runtime_checkable
class Pool(Protocol):
    name: str
    runs_in_child: bool

    def submit(self, fn: Callable[[], Any]) -> Any: ...

    def call_body(self, fn: Callable[..., Any], args: tuple, kwargs: dict) -> Any: ...

    def run_remote(self, payload: ChildTask) -> BodyOutcome: ...

    def shutdown(self, wait: bool = True) -> None: ...


class _InProcessPool:
    """本进程内执行的池的公共部分。"""

    name = "base"
    runs_in_child = False

    def call_body(self, fn: Callable[..., Any], args: tuple, kwargs: dict) -> Any:
        return fn(*args, **kwargs)

    def run_remote(self, payload: ChildTask) -> BodyOutcome:
        raise ConfigError(f"{self.name} 池不支持子进程执行")


class SoloPool(_InProcessPool):
    """调试用：同步顺序执行，异常直接冒到调用方。"""

    name = "solo"

    def submit(self, fn: Callable[[], Any]) -> None:
        fn()
        return None

    def shutdown(self, wait: bool = True) -> None:
        return None


class ThreadPool(_InProcessPool):
    """IO 密集默认池（决策 §20-3）。无法强杀，超时只能协作式。"""

    name = "threads"

    def __init__(self, concurrency: int) -> None:
        self.concurrency = max(1, int(concurrency))
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.concurrency, thread_name_prefix="taskmq-pool"
        )

    def submit(self, fn: Callable[[], Any]) -> concurrent.futures.Future[Any]:
        return self._executor.submit(fn)

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)


async def _await_it(value: Any) -> Any:
    return await value


class AsyncPool(ThreadPool):
    """单事件循环 + 线程槽位（§10.2）。

    - `async def` 任务体在事件循环上 await；
    - 同步任务体直接在池线程里跑（不会阻塞 loop）；
    - 状态写入 / ack / 钩子仍在线程里，不占用事件循环。
    """

    name = "asyncio"

    def __init__(self, concurrency: int) -> None:
        super().__init__(concurrency)
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._run_loop, name="taskmq-asyncio", daemon=True
        )
        self._loop_thread.start()

    def _run_loop(self) -> None:  # pragma: no cover - 线程内
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def call_body(self, fn: Callable[..., Any], args: tuple, kwargs: dict) -> Any:
        result = fn(*args, **kwargs)
        if inspect.isawaitable(result):
            future = asyncio.run_coroutine_threadsafe(_await_it(result), self._loop)
            return future.result()
        return result

    def shutdown(self, wait: bool = True) -> None:
        super().shutdown(wait=wait)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._loop_thread.join(timeout=5)


def _child_main(payload: ChildTask, conn: Any) -> None:
    """可强杀路径的子进程入口：跑完把结果写回管道。"""
    try:
        init_child(payload.app_spec)
        outcome = run_child_task(payload)
    except BaseException as exc:  # noqa: BLE001 - 子进程要尽力回传失败原因
        outcome = BodyOutcome(
            ok=False,
            state=JobState.FAILED,
            error_type=type(exc).__name__,
            error=str(exc),
            detail=traceback.format_exc(),
        )
    try:
        conn.send(outcome)
    finally:
        conn.close()


class ProcessPool:
    """进程池：真隔离 + 可强杀硬超时（§10.2、§10.6）。

    - 声明了 `hard_timeout` 的任务：**每次投递一个独立子进程**，超时直接 `terminate/kill`；
    - 其余任务：走 `ProcessPoolExecutor`（子进程复用，避免每次付进程启动成本）；
    - 子进程需要 `app_spec`（`module:attr`）重建 App 与任务注册表 —— CLI 用 `--app` 传入。

    代价（写清楚）：硬超时任务每次投递多付一次 spawn（macOS 上约 0.2–0.4s），
    因此 `processes` 池适合 CPU 密集的**长任务**；海量小任务用 `threads`。
    """

    name = "processes"
    runs_in_child = True

    def __init__(self, concurrency: int, *, app_spec: str | None) -> None:
        if not app_spec:
            raise ConfigError(
                "processes 池需要 app_spec：用 --app module:attr 或环境变量 TASKMQ_APP 指定"
            )
        self.concurrency = max(1, int(concurrency))
        self.app_spec = app_spec
        self._ctx = multiprocessing.get_context("spawn")
        # 父进程侧槽位：Worker._handle 在这些线程里同步等待子进程结果
        self._slots = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.concurrency, thread_name_prefix="taskmq-proc"
        )
        self._executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=self.concurrency,
            mp_context=self._ctx,
            initializer=init_child,
            initargs=(app_spec,),
        )

    def submit(self, fn: Callable[[], Any]) -> concurrent.futures.Future[Any]:
        """父进程侧只占一个线程槽位；真正的任务体在子进程里跑。"""
        return self._slots.submit(fn)

    def call_body(self, fn: Callable[..., Any], args: tuple, kwargs: dict) -> Any:  # pragma: no cover
        raise ConfigError("processes 池在子进程里调用任务体")

    def run_remote(self, payload: ChildTask) -> BodyOutcome:
        if payload.hard_timeout:
            return self._run_killable(payload)
        try:
            return self._executor.submit(run_child_task, payload).result()
        except concurrent.futures.process.BrokenProcessPool as exc:
            return BodyOutcome(
                ok=False,
                state=JobState.FAILED,
                error_type="BrokenProcessPool",
                error=f"子进程池已损坏（致命）：{exc}",
                detail=traceback.format_exc(),
            )
        except Exception as exc:  # 含 args/kwargs 不可 pickle 的情形
            return BodyOutcome(
                ok=False,
                state=JobState.FAILED,
                error_type=type(exc).__name__,
                error=f"投递到子进程失败：{exc}",
                detail=traceback.format_exc(),
            )

    def _run_killable(self, payload: ChildTask) -> BodyOutcome:
        parent, child = self._ctx.Pipe(duplex=False)
        process = self._ctx.Process(target=_child_main, args=(payload, child), daemon=True)
        process.start()
        child.close()
        try:
            if parent.poll(float(payload.hard_timeout or 0.0)):
                try:
                    return parent.recv()
                except EOFError:
                    return BodyOutcome(
                        ok=False,
                        state=JobState.FAILED,
                        error_type="ChildProcessBroken",
                        error="子进程未回传结果就退出了",
                    )
            process.terminate()
            process.join(2.0)
            return BodyOutcome.timeout(float(payload.hard_timeout or 0.0))
        finally:
            parent.close()
            if process.is_alive():
                process.kill()
                process.join(1.0)

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=True)
        self._slots.shutdown(wait=wait)


def make_pool(name: str, concurrency: int, *, app_spec: str | None = None) -> Pool:
    if name == "solo":
        return SoloPool()
    if name == "threads":
        return ThreadPool(concurrency)
    if name == "asyncio":
        return AsyncPool(concurrency)
    if name == "processes":
        return ProcessPool(concurrency, app_spec=app_spec)
    raise ValueError(f"未知 pool：{name!r}")


__all__ = ["Pool", "SoloPool", "ThreadPool", "AsyncPool", "ProcessPool", "make_pool"]
