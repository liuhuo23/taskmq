"""Worker 运行时：拉取 → 执行 → ack/nack/重试/DLQ，并实现决策 §20-9 的插队语义。

关键点：
- **reserve–start 耦合（P16）**：只在 `prefetch - 已持有` 有空间时 reserve；
  默认 `prefetch == concurrency`，因此正常情况下不会有「预留了但没开始」的消息。
- **让位（G2/P12–P14）**：`prefetch > concurrency` 时本地会持有未开始的预留；
  一旦探测到更高优先级的可见消息，就从最低优先级开始让位（`yields` 独立计数）。
- **运行中不打断（G3/P19）**：`_handle` 一旦提交给执行池就算「开始」，不再让位。
"""
from __future__ import annotations

import concurrent.futures
import dataclasses
import functools
import logging
import os
import random
import secrets
import threading
import time
from collections.abc import Sequence
from typing import Any

from ..errors import ConfigError, LeaseLost, TaskTimeout
from ..protocol import ACK_ON_RECEIPT, ACK_ON_SUCCESS
from ..ratelimit import RateLimit, TokenBucket
from ..task import Task, TaskContext, _reset_current, _set_current
from ..transport.base import Delivery, JobState
from .execution import BodyOutcome, ChildTask, decide_retry, execute_task
from .pool import Pool, make_pool

logger = logging.getLogger("taskmq.worker")


class Worker:
    """一个进程内的 worker：持有 N 个并发槽位，订阅若干队列。"""

    def __init__(
        self,
        app: Any,
        *,
        queues: Sequence[str] | None = None,
        worker_id: str | None = None,
        pool: Pool | None = None,
        concurrency: int | None = None,
        prefetch: int | None = None,
        lease: float | None = None,
        app_spec: str | None = None,
    ) -> None:
        self.app = app
        self.app_spec = app_spec or os.environ.get("TASKMQ_APP") or ""
        self.config = app.config
        self.transport = app.transport
        self.queues = tuple(queues) if queues else (self.config.default_queue,)
        self.worker_id = worker_id or f"w-{os.getpid()}-{secrets.token_hex(3)}"
        self.concurrency = max(1, int(concurrency if concurrency is not None else self.config.concurrency))
        self.prefetch = max(
            1, int(prefetch if prefetch is not None else self.config.effective_prefetch)
        )
        self.lease = float(lease if lease is not None else self.config.lease)
        self._pool = (
            pool
            if pool is not None
            else make_pool(self.config.pool, self.concurrency, app_spec=self.app_spec or None)
        )

        self._lock = threading.RLock()
        self._pending: dict[int, Delivery] = {}
        self._inflight: dict[int, Delivery] = {}
        self._futures: set[concurrent.futures.Future] = set()
        self._stopped = False
        self._last_reap = 0.0

        self.processed = 0
        self.reserved = 0
        self.yielded = 0
        self.deferred = 0

        # Phase 1：限流 / concurrency_key 互斥 / 能力校验
        self._buckets: dict[str, TokenBucket] = {}
        self._concurrency_leases: dict[int, str] = {}
        self._held_keys: set[str] = set()          # 本 worker 正在跑的 concurrency_key
        self._capabilities_checked = False

    # ------------------------------------------------------------------ 观测
    def __repr__(self) -> str:
        return (
            f"<Worker {self.worker_id} queues={','.join(self.queues)} "
            f"concurrency={self.concurrency} prefetch={self.prefetch} "
            f"inflight={len(self._inflight)} pending={len(self._pending)}>"
        )

    def __enter__(self) -> Worker:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    def held(self) -> int:
        with self._lock:
            return len(self._pending) + len(self._inflight)

    def pending_deliveries(self) -> list[Delivery]:
        with self._lock:
            return list(self._pending.values())

    # ------------------------------------------------------------------ 主循环
    def poll(self) -> int:
        """一轮：回收 → 让位 → reserve → 启动。返回本轮启动的任务数。"""
        if self._stopped:
            return 0
        self._validate_capabilities()
        self._reap()
        self._yield_for_priority()
        self._reserve()
        return self._start_pending()

    def _reap(self) -> None:
        now = time.monotonic()
        interval = max(0.05, min(float(self.config.heartbeat_interval), self.lease / 3.0))
        if now - self._last_reap < interval:
            return
        self._last_reap = now
        try:
            self.transport.reap_expired_leases()
            self.transport.reap_expired_jobs()
        except Exception:  # pragma: no cover - transport 故障不应打死 worker
            logger.exception("reap 失败（transport 异常）")
        self._renew_inflight_leases()

    def _renew_inflight_leases(self) -> None:
        """在途任务续租（§10.4）。

        没有这一步，跑得比 `lease` 还久的任务会被自家 reap 判成孤儿并重投 —— 在
        at-least-once 下就是**重复执行**。续租周期取 `min(heartbeat_interval, lease/3)`，
        所以 `lease` 调小也不会「还没续上就过期」。
        """
        with self._lock:
            deliveries = list(self._inflight.values())
        for delivery in deliveries:
            try:
                self.transport.extend_lease(delivery, self.lease)
            except LeaseLost:
                logger.warning(
                    "续租时租约已易主，任务结果会被丢弃：job=%s", delivery.job_id,
                    extra={"worker": self.worker_id},
                )
            except Exception:  # pragma: no cover - 续租失败只记录
                logger.debug("续租失败 job=%s", delivery.job_id, exc_info=True)

    def _yield_delay(self) -> float:
        base = float(self.config.yield_delay)
        if base <= 0:
            return 0.0
        return base * random.uniform(0.8, 1.2)

    def _yield_for_priority(self) -> int:
        """G2：未开始的预留让给更高优先级（P12–P14）。"""
        if not self.config.yield_enabled:
            return 0
        with self._lock:
            pending = sorted(self._pending.values(), key=lambda item: (item.priority, item.message_id))
        if not pending:
            return 0
        yielded = 0
        for delivery in pending:
            top = self.transport.peek_max_priority(self.queues)
            if top is None or top - delivery.priority < self.config.yield_min_delta:
                break
            try:
                ok = self.transport.yield_reservation(
                    delivery, delay=self._yield_delay(), max_yields=self.config.max_yields
                )
            except LeaseLost:
                ok = False
            if ok:
                with self._lock:
                    self._pending.pop(delivery.message_id, None)
                yielded += 1
        if yielded:
            self.yielded += yielded
            logger.info("让位 %d 条未开始的预留给更高优先级", yielded, extra={"worker": self.worker_id})
        return yielded

    def _reserve(self) -> int:
        with self._lock:
            room = self.prefetch - (len(self._pending) + len(self._inflight))
        if room <= 0:
            return 0
        deliveries = self.transport.reserve(
            self.queues, worker_id=self.worker_id, lease=self.lease, limit=room
        )
        with self._lock:
            for delivery in deliveries:
                self._pending[delivery.message_id] = delivery
        self.reserved += len(deliveries)
        return len(deliveries)

    def _start_pending(self) -> int:
        with self._lock:
            free = max(0, self.concurrency - len(self._inflight))
            chosen = sorted(self._pending.values(), key=lambda item: (-item.priority, item.message_id))[
                :free
            ]
            for delivery in chosen:
                self._pending.pop(delivery.message_id, None)
                self._inflight[delivery.message_id] = delivery
        for delivery in chosen:
            self._submit(delivery)
        return len(chosen)

    def _submit(self, delivery: Delivery) -> None:
        future = self._pool.submit(functools.partial(self._handle, delivery))
        if future is not None:
            with self._lock:
                self._futures.add(future)
            future.add_done_callback(self._on_future_done)

    def _on_future_done(self, future: concurrent.futures.Future) -> None:
        with self._lock:
            self._futures.discard(future)

    # ------------------------------------------------------------------ 执行
    def _handle(self, delivery: Delivery) -> None:
        try:
            self._process(delivery)
        except Exception:  # pragma: no cover - 兜底：不让 worker 线程死掉
            logger.exception("处理投递时发生意外错误 job=%s", delivery.job_id)
        finally:
            with self._lock:
                self._inflight.pop(delivery.message_id, None)
                self.processed += 1

    # ------------------------------------------------- 能力校验 / 限流 / 并发键
    def _validate_capabilities(self) -> None:
        """启动即校验：用了 `concurrency_key` 就必须有支持命名租约的 transport。"""
        if self._capabilities_checked:
            return
        self._capabilities_checked = True
        if not self.transport.supports_leases:
            keyed = [task.name for task in self.app.tasks.values() if task.concurrency_key]
            if keyed:
                raise ConfigError(
                    f"任务 {keyed} 使用了 concurrency_key，但 transport "
                    f"{type(self.transport).__name__} 不支持命名租约"
                )

        # async def 任务只能跑在 asyncio 池（§10.2：不做隐式 asyncio.run()）
        if self._pool.name != "asyncio":
            async_tasks = [task.name for task in self.app.tasks.values() if task.is_async]
            if async_tasks:
                raise ConfigError(
                    f"async 任务 {async_tasks} 只能跑在 asyncio 池（当前 pool={self._pool.name}）"
                )

        # hard_timeout 只有 processes 池能兑现（threads 无法强杀，§10.6）
        if self._pool.name != "processes":
            hard = [task.name for task in self.app.tasks.values() if task.hard_timeout is not None]
            if hard:
                logger.warning(
                    "任务 %s 声明了 hard_timeout，但 pool=%s 无法强杀进程：硬超时不会生效",
                    hard,
                    self._pool.name,
                )

    def _concurrency_ttl(self, task: Task) -> float:
        return max(self.lease, float(task.timeout or 0.0) + 30.0)

    def _defer_reason(self, task: Task, delivery: Delivery) -> tuple[float, str] | None:
        """返回 `(等待秒数, 原因)`；`None` 表示可以立即执行。"""
        key = delivery.envelope.concurrency_key
        if key:
            lease_name = f"ck:{key}"
            wait_for_key = max(self.config.poll_interval * 2, 0.05)
            # 「自查 + 获取」必须原子：租约 owner 是 worker_id，否则同 worker 的多个
            # 线程会互相「自认持有」，同 key 并发跑起来。
            with self._lock:
                if lease_name in self._held_keys:
                    return (wait_for_key, "concurrency_key")
                acquired = self.transport.acquire_lease(
                    lease_name, self.worker_id, self._concurrency_ttl(task)
                )
                if acquired:
                    self._held_keys.add(lease_name)
                    self._concurrency_leases[delivery.message_id] = lease_name
            if not acquired:
                return (wait_for_key, "concurrency_key")

        spec = task.rate_limit
        if spec is not None:
            bucket = self._buckets.get(task.name)
            if bucket is None:
                bucket = TokenBucket(RateLimit.parse(spec))
                self._buckets[task.name] = bucket
            wait = bucket.acquire()
            if wait is not None:
                self._release_concurrency(delivery)      # 本轮没执行，不占锁
                return (max(wait, 0.001), "rate_limit")
        return None

    def _release_concurrency(self, delivery: Delivery) -> None:
        with self._lock:
            lease_name = self._concurrency_leases.pop(delivery.message_id, None)
            if lease_name is None:
                return
            self._held_keys.discard(lease_name)
        try:
            self.transport.release_lease(lease_name, self.worker_id)
        except Exception:  # pragma: no cover - 释放失败不该影响执行结果
            logger.debug("释放 concurrency 租约失败：%s", lease_name, exc_info=True)

    def _process(self, delivery: Delivery) -> None:
        env = delivery.envelope
        task = self.app.task_for(env.task)
        if task is None:
            reason = f"未注册的任务 {env.task}（部署不一致，直接 DLQ）"
            self.transport.set_state(
                env.id, JobState.FAILED, task=env.task, attempt=delivery.attempt, error=reason,
                worker=self.worker_id, priority=delivery.priority,
            )
            self._safe_dead_letter(delivery, reason)
            return

        # 限流 / concurrency_key：还没真正执行，用 defer 放回（不消耗投递次数）
        deferral = self._defer_reason(task, delivery)
        if deferral is not None:
            wait, reason = deferral
            self.deferred += 1
            self.transport.defer(delivery, delay=wait)
            self.transport.set_state(
                env.id,
                JobState.QUEUED,
                task=env.task,
                attempt=delivery.attempt,
                deferred=reason,
                deferred_for=round(wait, 3),
            )
            self.app.emit(
                "task.deferred",
                job_id=env.id,
                task=env.task,
                queue=delivery.queue,
                worker=self.worker_id,
                reason=reason,
                delay=round(wait, 3),
            )
            return

        ack_mode = env.ack or ACK_ON_SUCCESS
        if ack_mode == ACK_ON_RECEIPT:
            self._safe_ack(delivery)

        self.app.emit(
            "task.started",
            job_id=env.id,
            task=env.task,
            queue=delivery.queue,
            worker=self.worker_id,
            attempt=delivery.attempt,
            priority=delivery.priority,
        )
        ctx = TaskContext(
            app=self.app,
            envelope=env,
            worker_id=self.worker_id,
            attempt=delivery.attempt,
            deliveries=delivery.deliveries,
            priority=delivery.priority,
            queue=delivery.queue,
            delivery=delivery,
            deadline=env.deadline,
            log=logger.getChild(env.task),
        )
        token = _set_current(ctx)
        try:
            if self._pool.runs_in_child:
                # processes 池：钩子 + run 都在子进程里跑，结果（可 pickle）回传父进程
                outcome = self._run_in_child(task, delivery, ctx, ack_mode)
            else:
                outcome = execute_task(task, ctx, self._pool.call_body, ack_mode=ack_mode)
        finally:
            self._release_concurrency(delivery)
            _reset_current(token)
        self._apply_outcome(task, delivery, ctx, outcome, ack_mode)

    def _apply_outcome(
        self,
        task: Task,
        delivery: Delivery,
        ctx: TaskContext,
        outcome: BodyOutcome,
        ack_mode: str,
    ) -> None:
        """把执行结果落成状态 + ack / 重试 / DLQ。

        钩子在 `execute_task`（本进程）或子进程里已经跑过，这里只做投递语义。
        """
        env = delivery.envelope
        extra = {"hook_error": outcome.hook_error} if outcome.hook_error else {}

        if outcome.ok:
            self.transport.set_state(
                env.id,
                JobState.SUCCEEDED,
                task=env.task,
                attempt=delivery.attempt,
                result=outcome.result,
                worker=self.worker_id,
                runtime=outcome.runtime,
                queue=delivery.queue,
                priority=delivery.priority,
                yields=delivery.yields,
                **extra,
            )
            self.app.emit(
                "task.succeeded",
                job_id=env.id,
                task=env.task,
                queue=delivery.queue,
                worker=self.worker_id,
                attempt=delivery.attempt,
                runtime=outcome.runtime,
            )
            if ack_mode != ACK_ON_RECEIPT:
                self._safe_ack(delivery)
            return

        detail = outcome.detail or outcome.error or outcome.error_type or "任务失败"
        if outcome.retry_delay is not None:
            self.transport.set_state(
                env.id,
                JobState.RETRYING,
                task=env.task,
                attempt=delivery.attempt + 1,
                error=outcome.error,
                will_retry=True,
                worker=self.worker_id,
                priority=delivery.priority,
                **extra,
            )
            # P5：重试**保持原优先级**；P9：退避通过重新入队 + visible_at 实现。
            self.transport.enqueue(
                env.next_attempt(),
                queue=delivery.queue,
                delay=outcome.retry_delay,
                priority=delivery.priority,
            )
            if ack_mode != ACK_ON_RECEIPT:
                self._safe_ack(delivery)
            self.app.emit(
                "task.retrying",
                job_id=env.id,
                task=env.task,
                queue=delivery.queue,
                worker=self.worker_id,
                attempt=delivery.attempt + 1,
                delay=round(outcome.retry_delay, 3),
            )
            return

        self.transport.set_state(
            env.id,
            JobState.FAILED,
            task=env.task,
            attempt=delivery.attempt,
            error=detail,
            worker=self.worker_id,
            priority=delivery.priority,
            **extra,
        )
        self._safe_dead_letter(delivery, detail)
        self.app.emit(
            "task.failed",
            job_id=env.id,
            task=env.task,
            queue=delivery.queue,
            worker=self.worker_id,
            attempt=delivery.attempt,
            error=outcome.error or detail,
        )

    def _run_in_child(
        self,
        task: Task,
        delivery: Delivery,
        ctx: TaskContext,
        ack_mode: str,
    ) -> BodyOutcome:
        """`processes` 池：`before_start → run → 钩子` 都在子进程里跑，结果回传。"""
        payload = ChildTask(
            app_spec=self.app_spec,
            envelope=delivery.envelope,
            worker_id=self.worker_id,
            attempt=delivery.attempt,
            deliveries=delivery.deliveries,
            ack_mode=ack_mode,
            hard_timeout=task.hard_timeout,
        )
        outcome = self._pool.run_remote(payload)
        if outcome.error_type == "HardTimeout":
            # 子进程已被 kill：没有钩子可跑（进程没了），重试决策由父进程按策略做
            timed_out = TaskTimeout(outcome.error)
            outcome = dataclasses.replace(
                outcome,
                retry_delay=decide_retry(
                    task, delivery.attempt, delivery.deliveries, timed_out, ack_mode=ack_mode
                ),
            )
        return outcome

    def _safe_ack(self, delivery: Delivery) -> None:
        try:
            self.transport.ack(delivery)
        except LeaseLost:
            logger.warning(
                "ack 时租约已易主（结果被丢弃，消息会重投）：job=%s", delivery.job_id,
                extra={"worker": self.worker_id},
            )

    def _safe_dead_letter(self, delivery: Delivery, reason: str) -> None:
        try:
            self.transport.dead_letter(delivery, reason)
        except LeaseLost:
            logger.warning(
                "dead_letter 时租约已易主：job=%s", delivery.job_id, extra={"worker": self.worker_id}
            )

    # ------------------------------------------------------------------ 空闲
    def is_idle(self) -> bool:
        with self._lock:
            if self._pending or self._inflight:
                return False
        return self.transport.next_visible_at(self.queues) is None

    def run_until_idle(self, *, timeout: float = 30.0, poll: float | None = None) -> int:
        """跑到「没有可见消息 + 没有持有」为止；超时抛 `TimeoutError`。"""
        interval = float(poll if poll is not None else self.config.poll_interval)
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            if self.is_idle():
                return self.processed
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"run_until_idle 超时（processed={self.processed}, held={self.held()}）"
                )
            wait = interval
            next_at = self.transport.next_visible_at(self.queues)
            if next_at is not None:
                delta = next_at - time.time()
                if delta > 0:
                    wait = min(interval, max(0.001, delta))
            time.sleep(max(0.0, wait))

    def drain(self, *, timeout: float = 30.0) -> None:
        """等待在途任务跑完（不拉新消息）。"""
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                futures = list(self._futures)
                idle = not self._pending and not self._inflight
            if idle and not futures:
                return
            if futures:
                concurrent.futures.wait(futures, timeout=0.05)
            else:
                time.sleep(0.005)
            if time.monotonic() >= deadline:
                raise TimeoutError(f"drain 超时（held={self.held()}）")

    def stop(self) -> None:
        """停止拉取，并把**未开始**的预留 nack 回队（优雅退出的第一步）。"""
        self._stopped = True
        with self._lock:
            pending = list(self._pending.values())
            self._pending.clear()
        for delivery in pending:
            try:
                self.transport.nack(delivery, requeue=True)
            except LeaseLost:
                continue

    def close(self) -> None:
        self.stop()
        try:
            self.drain()
        finally:
            self._pool.shutdown(wait=True)


__all__ = ["Worker"]
