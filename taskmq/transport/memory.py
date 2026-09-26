"""内存 transport：单进程、线程安全，用于单测 / eager / 本地试跑。

语义与 SQLite transport 对齐（同一套不变量，见 `base.py`），并实现决策 §20-9 的
**方案 D**：全局严格优先 + 让位 + 平级队列轮询。

- claim：所有订阅队列统一按 `priority DESC` 定档；**同一档位内**按 `weight` 轮询（P3）。
- 让位：`peek_max_priority` + `yield_reservation`，`yields` 独立计数（P13），
  到 `max_yields` 后 `yieldable=false`（P14）。
- 租约/过期/幂等/DLQ 语义与之前一致。
"""
from __future__ import annotations

import dataclasses
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from ..errors import LeaseLost, MessageNotFound, TransportError
from ..protocol import Envelope
from .base import (
    UNSET,
    DeadLetter,
    Delivery,
    JobRecord,
    JobState,
    MessageState,
    QueueStat,
    Transport,
)

__all__ = ["MemoryTransport"]


@dataclasses.dataclass
class _Message:
    id: int
    job_id: str
    queue: str
    envelope: Envelope
    priority: int
    state: str
    visible_at: float
    expires_at: float | None
    enqueued_at: float
    deliveries: int = 0
    yields: int = 0
    yieldable: bool = True
    claimed_by: str | None = None
    claimed_at: float | None = None
    lease_until: float | None = None
    last_error: str | None = None
    dead_reason: str | None = None


class MemoryTransport(Transport):
    """进程内 transport。`clock` 可注入（测试里用假时钟推进租约/退避）。"""

    supports_leases = True

    def __init__(
        self,
        *,
        clock: Callable[[], float] | None = None,
        idempotency_ttl: float = 86400.0,
    ) -> None:
        self._clock = clock or time.time
        self._leases: dict[str, tuple[str, float]] = {}
        self._lock = threading.RLock()
        self._messages: dict[int, _Message] = {}
        self._jobs: dict[str, JobRecord] = {}
        self._idempotency: dict[str, tuple[str, float]] = {}
        self._weights: dict[str, int] = {}
        self._served: dict[str, int] = {}
        self._next_id = 1
        self._idempotency_ttl = idempotency_ttl
        self._closed = False

    # ---------------------------------------------------------------- 配置
    def set_queue_weights(self, weights: Mapping[str, int]) -> None:
        """设置**同优先级档位内**的队列取件权重（P3）；未配置的队列权重为 1。"""
        with self._lock:
            self._weights = {name: max(1, int(weight)) for name, weight in weights.items()}

    def _weight(self, queue: str) -> int:
        return max(1, self._weights.get(queue, 1))

    # ---------------------------------------------------------------- helpers
    def _now(self) -> float:
        return float(self._clock())

    def _purge_idempotency_locked(self, now: float) -> None:
        expired = [key for key, (_, until) in self._idempotency.items() if until <= now]
        for key in expired:
            del self._idempotency[key]

    def _requeue_expired_locked(self, now: float) -> int:
        count = 0
        for message in self._messages.values():
            if (
                message.state == MessageState.RESERVED
                and message.lease_until is not None
                and message.lease_until <= now
            ):
                message.state = MessageState.QUEUED
                message.visible_at = now
                message.claimed_by = None
                message.claimed_at = None
                message.lease_until = None
                job = self._jobs.get(message.job_id)
                if job is not None and job.state in (JobState.RUNNING, JobState.QUEUED):
                    self._jobs[message.job_id] = dataclasses.replace(
                        job, state=JobState.RETRYING, updated_at=now
                    )
                count += 1
        return count

    def _expire_overdue_locked(self, now: float) -> int:
        count = 0
        for message in self._messages.values():
            if (
                message.state == MessageState.QUEUED
                and message.expires_at is not None
                and message.expires_at <= now
            ):
                message.state = MessageState.EXPIRED
                message.dead_reason = "expired"
                job = self._jobs.get(message.job_id)
                if job is not None:
                    self._jobs[message.job_id] = dataclasses.replace(
                        job,
                        state=JobState.EXPIRED,
                        error="expired before execution",
                        updated_at=now,
                    )
                count += 1
        return count

    def _owned_reserved(self, delivery: Delivery) -> _Message:
        message = self._messages.get(delivery.message_id)
        if message is None:
            raise MessageNotFound(f"消息 {delivery.message_id} 不存在")
        if message.state == MessageState.RESERVED and message.claimed_by == delivery.worker_id:
            return message
        raise LeaseLost(
            f"消息 {delivery.message_id} 当前 state={message.state!r} holder={message.claimed_by!r}，"
            f"不是 worker {delivery.worker_id!r} 持有"
        )

    def _select_locked(self, candidates: list[_Message], limit: int) -> list[_Message]:
        """方案 D：全局按优先级定档；档内按 weight 轮询（P3）。"""
        selected: list[_Message] = []
        pool = list(candidates)
        while pool and len(selected) < limit:
            top = max(message.priority for message in pool)
            band = [message for message in pool if message.priority == top]
            by_queue: dict[str, list[_Message]] = {}
            for message in band:
                by_queue.setdefault(message.queue, []).append(message)
            chosen_queue = min(
                by_queue,
                key=lambda queue: (self._served.get(queue, 0) / self._weight(queue), queue),
            )
            message = min(by_queue[chosen_queue], key=lambda item: item.id)
            pool.remove(message)
            selected.append(message)
            self._served[chosen_queue] = self._served.get(chosen_queue, 0) + 1
        return selected

    # ------------------------------------------------------------------ 入队
    def enqueue(
        self,
        env: Envelope,
        *,
        queue: str | None = None,
        delay: float = 0.0,
        priority: int | None = None,
    ) -> str:
        if self._closed:
            raise TransportError("transport 已关闭")
        now = self._now()
        target = queue or env.queue or "default"
        prio = env.priority if priority is None else int(priority)
        with self._lock:
            if env.key:
                self._purge_idempotency_locked(now)
                existing = self._idempotency.get(env.key)
                if existing is not None:
                    return existing[0]

            visible_at = now + max(0.0, float(delay))
            if env.eta is not None:
                visible_at = max(visible_at, float(env.eta))

            effective = (
                env if (env.queue == target and env.priority == prio) else env.with_routing(target, prio)
            )
            message = _Message(
                id=self._next_id,
                job_id=env.id,
                queue=target,
                envelope=effective,
                priority=prio,
                state=MessageState.QUEUED,
                visible_at=visible_at,
                expires_at=env.expires_at,
                enqueued_at=now,
            )
            self._next_id += 1
            self._messages[message.id] = message

            if env.key:
                self._idempotency[env.key] = (env.id, now + self._idempotency_ttl)

            built = self._jobs.get(env.id)
            if built is None:
                self._jobs[env.id] = JobRecord(
                    job_id=env.id,
                    task=env.task,
                    state=JobState.QUEUED,
                    attempt=env.attempt,
                    meta={"queue": target, "priority": prio},
                    created_at=now,
                    updated_at=now,
                )
            else:
                meta = dict(built.meta)
                meta.update({"queue": target, "priority": prio})
                self._jobs[env.id] = dataclasses.replace(
                    built, state=JobState.QUEUED, attempt=env.attempt, meta=meta, updated_at=now
                )
            return env.id

    # ------------------------------------------------------------------ 投递
    def reserve(
        self,
        queues: Sequence[str],
        *,
        worker_id: str,
        lease: float,
        limit: int,
    ) -> list[Delivery]:
        if limit <= 0:
            return []
        now = self._now()
        wanted = set(queues)
        with self._lock:
            self._requeue_expired_locked(now)
            self._expire_overdue_locked(now)
            candidates = [
                message
                for message in self._messages.values()
                if message.state == MessageState.QUEUED
                and message.queue in wanted
                and message.visible_at <= now
            ]
            deliveries: list[Delivery] = []
            for message in self._select_locked(candidates, limit):
                message.state = MessageState.RESERVED
                message.claimed_by = worker_id
                message.claimed_at = now
                message.lease_until = now + max(0.0, float(lease))
                message.deliveries += 1
                deliveries.append(
                    Delivery(
                        job_id=message.job_id,
                        message_id=message.id,
                        queue=message.queue,
                        envelope=message.envelope,
                        worker_id=worker_id,
                        attempt=message.envelope.attempt,
                        deliveries=message.deliveries,
                        lease_until=message.lease_until,
                        reserved_at=now,
                        priority=message.priority,
                        yields=message.yields,
                    )
                )
                job = self._jobs.get(message.job_id)
                if job is not None and job.state in (JobState.QUEUED, JobState.RETRYING):
                    self._jobs[message.job_id] = dataclasses.replace(
                        job, state=JobState.RUNNING, attempt=message.envelope.attempt, updated_at=now
                    )
            return deliveries

    def ack(self, delivery: Delivery) -> None:
        now = self._now()
        with self._lock:
            message = self._messages.get(delivery.message_id)
            if message is None:
                raise MessageNotFound(f"消息 {delivery.message_id} 不存在")
            if message.state == MessageState.ACKED:
                return
            self._owned_reserved(delivery)
            message.state = MessageState.ACKED
            message.claimed_by = None
            message.claimed_at = None
            message.lease_until = None
            job = self._jobs.get(message.job_id)
            if job is not None and job.state == JobState.RUNNING:
                self._jobs[message.job_id] = dataclasses.replace(job, updated_at=now)

    def nack(self, delivery: Delivery, *, requeue: bool = True, delay: float = 0.0) -> None:
        now = self._now()
        with self._lock:
            message = self._messages.get(delivery.message_id)
            if message is None:
                raise MessageNotFound(f"消息 {delivery.message_id} 不存在")
            if message.state == MessageState.QUEUED and message.claimed_by is None:
                return
            self._owned_reserved(delivery)
            if requeue:
                message.state = MessageState.QUEUED
                message.claimed_by = None
                message.claimed_at = None
                message.lease_until = None
                message.visible_at = now + max(0.0, float(delay))
            else:
                message.state = MessageState.DEAD
                message.dead_reason = message.dead_reason or "nacked without requeue"
                message.claimed_by = None
                message.claimed_at = None
                message.lease_until = None

    def dead_letter(self, delivery: Delivery, reason: str) -> None:
        now = self._now()
        with self._lock:
            message = self._messages.get(delivery.message_id)
            if message is None:
                raise MessageNotFound(f"消息 {delivery.message_id} 不存在")
            if message.state == MessageState.DEAD:
                return
            self._owned_reserved(delivery)
            message.state = MessageState.DEAD
            message.dead_reason = reason
            message.last_error = reason
            message.claimed_by = None
            message.claimed_at = None
            message.lease_until = None
            job = self._jobs.get(message.job_id)
            if job is not None and job.state not in JobState.TERMINAL:
                self._jobs[message.job_id] = dataclasses.replace(job, updated_at=now)

    def extend_lease(self, delivery: Delivery, seconds: float) -> None:
        now = self._now()
        with self._lock:
            message = self._owned_reserved(delivery)
            message.lease_until = now + max(0.0, float(seconds))

    def defer(self, delivery: Delivery, *, delay: float = 0.0) -> None:
        now = self._now()
        with self._lock:
            message = self._owned_reserved(delivery)
            message.state = MessageState.QUEUED
            message.visible_at = now + max(0.0, float(delay))
            message.deliveries = max(0, message.deliveries - 1)   # 推迟不算一次投递
            message.claimed_by = None
            message.claimed_at = None
            message.lease_until = None
            job = self._jobs.get(message.job_id)
            if job is not None and job.state in (JobState.RUNNING, JobState.RETRYING):
                self._jobs[message.job_id] = dataclasses.replace(
                    job, state=JobState.QUEUED, updated_at=now
                )

    # ------------------------------------------------------------------ 命名租约
    def acquire_lease(self, name: str, owner: str, ttl: float) -> bool:
        now = self._now()
        with self._lock:
            entry = self._leases.get(name)
            if entry is None or entry[1] <= now or entry[0] == owner:
                self._leases[name] = (owner, now + max(0.0, float(ttl)))
                return True
            return False

    def release_lease(self, name: str, owner: str) -> None:
        with self._lock:
            entry = self._leases.get(name)
            if entry is not None and entry[0] == owner:
                del self._leases[name]

    def renew_lease(self, name: str, owner: str, ttl: float) -> bool:
        now = self._now()
        with self._lock:
            entry = self._leases.get(name)
            if entry is None or entry[0] != owner or entry[1] <= now:
                return False
            self._leases[name] = (owner, now + max(0.0, float(ttl)))
            return True

    # ------------------------------------------------------------------ 插队
    def peek_max_priority(self, queues: Sequence[str], *, now: float | None = None) -> int | None:
        moment = self._now() if now is None else float(now)
        wanted = set(queues)
        with self._lock:
            best: int | None = None
            for message in self._messages.values():
                visible = (
                    message.state == MessageState.QUEUED
                    and message.queue in wanted
                    and message.visible_at <= moment
                    and (message.expires_at is None or message.expires_at > moment)
                )
                if visible and (best is None or message.priority > best):
                    best = message.priority
            return best

    def yield_reservation(
        self,
        delivery: Delivery,
        *,
        delay: float = 0.0,
        max_yields: int = 100,
    ) -> bool:
        now = self._now()
        with self._lock:
            message = self._messages.get(delivery.message_id)
            if message is None:
                raise MessageNotFound(f"消息 {delivery.message_id} 不存在")
            self._owned_reserved(delivery)
            if not message.yieldable:
                return False
            message.state = MessageState.QUEUED
            message.visible_at = now + max(0.0, float(delay))
            message.yields += 1
            message.claimed_by = None
            message.claimed_at = None
            message.lease_until = None
            if message.yields >= max(1, int(max_yields)):
                message.yieldable = False
            job = self._jobs.get(message.job_id)
            if job is not None and job.state in (JobState.RUNNING, JobState.QUEUED, JobState.RETRYING):
                meta = dict(job.meta)
                meta["yields"] = message.yields
                self._jobs[message.job_id] = dataclasses.replace(
                    job, state=JobState.QUEUED, meta=meta, updated_at=now
                )
            return True

    def next_visible_at(self, queues: Sequence[str], *, now: float | None = None) -> float | None:
        wanted = set(queues)
        with self._lock:
            times = [
                message.visible_at
                for message in self._messages.values()
                if message.state == MessageState.QUEUED and message.queue in wanted
            ]
            return min(times) if times else None

    # -------------------------------------------------------------- job 状态
    def set_state(
        self,
        job_id: str,
        state: str,
        *,
        task: str | None = None,
        attempt: int | None = None,
        result: Any = UNSET,
        error: Any = UNSET,
        **meta: Any,
    ) -> JobRecord:
        now = self._now()
        with self._lock:
            existing = self._jobs.get(job_id)
            if existing is None:
                existing = JobRecord(
                    job_id=job_id, task=task or "", state=JobState.PENDING, created_at=now, updated_at=now
                )
            merged = dict(existing.meta)
            merged.update(meta)
            record = dataclasses.replace(
                existing,
                task=task or existing.task,
                state=state,
                attempt=existing.attempt if attempt is None else attempt,
                result=existing.result if result is UNSET else result,
                has_result=existing.has_result if result is UNSET else True,
                error=existing.error if error is UNSET else (None if error is None else str(error)),
                meta=merged,
                updated_at=now,
            )
            self._jobs[job_id] = record
            return record

    def get_state(self, job_id: str) -> JobRecord | None:
        with self._lock:
            return self._jobs.get(job_id)

    def queue_stats(self, queues: Sequence[str] | None = None) -> list[QueueStat]:
        wanted = set(queues) if queues is not None else None
        with self._lock:
            counts: dict[str, list[int]] = {}
            if wanted:
                for queue in wanted:
                    counts[queue] = [0, 0, 0]
            for message in self._messages.values():
                if wanted is not None and message.queue not in wanted:
                    continue
                bucket = counts.setdefault(message.queue, [0, 0, 0])
                if message.state == MessageState.QUEUED:
                    bucket[0] += 1
                elif message.state == MessageState.RESERVED:
                    bucket[1] += 1
                elif message.state == MessageState.DEAD:
                    bucket[2] += 1
            return [
                QueueStat(queue=queue, pending=pending, inflight=inflight, dead=dead)
                for queue, (pending, inflight, dead) in sorted(counts.items())
            ]

    # ------------------------------------------------------------------ 回收
    def reap_expired_leases(self, now: float | None = None) -> int:
        moment = self._now() if now is None else float(now)
        with self._lock:
            return self._requeue_expired_locked(moment)

    def reap_expired_jobs(self, now: float | None = None) -> int:
        moment = self._now() if now is None else float(now)
        with self._lock:
            return self._expire_overdue_locked(moment)

    # ------------------------------------------------------------------ DLQ
    def dead_letters(self, *, queue: str | None = None) -> list[DeadLetter]:
        with self._lock:
            return [
                DeadLetter(
                    message_id=message.id,
                    job_id=message.job_id,
                    queue=message.queue,
                    task=message.envelope.task,
                    envelope=message.envelope,
                    reason=message.dead_reason or "",
                    deliveries=message.deliveries,
                    failed_at=message.claimed_at or message.enqueued_at,
                )
                for message in sorted(self._messages.values(), key=lambda item: item.id)
                if message.state == MessageState.DEAD and (queue is None or message.queue == queue)
            ]

    def priority_stats(self, queues: Sequence[str] | None = None) -> dict[int, int]:
        wanted = set(queues) if queues is not None else None
        buckets: dict[int, int] = {}
        with self._lock:
            for message in self._messages.values():
                if message.state != MessageState.QUEUED:
                    continue
                if wanted is not None and message.queue not in wanted:
                    continue
                buckets[message.priority] = buckets.get(message.priority, 0) + 1
        return dict(sorted(buckets.items()))

    def replay_dead(
        self, message_id: int, *, queue: str | None = None, priority: int | None = None
    ) -> bool:
        now = self._now()
        with self._lock:
            message = self._messages.get(message_id)
            if message is None or message.state != MessageState.DEAD:
                return False
            if queue is not None:
                message.queue = queue
            if priority is not None:
                message.priority = int(priority)
            message.envelope = dataclasses.replace(
                message.envelope.with_routing(message.queue, message.priority), attempt=1
            )
            message.state = MessageState.QUEUED
            message.visible_at = now
            message.deliveries = 0
            message.yields = 0
            message.yieldable = True
            message.last_error = None
            message.dead_reason = None
            message.claimed_by = None
            message.claimed_at = None
            message.lease_until = None
            job = self._jobs.get(message.job_id)
            if job is not None:
                self._jobs[message.job_id] = dataclasses.replace(
                    job, state=JobState.QUEUED, error=None, updated_at=now
                )
            return True

    # ------------------------------------------------------------------ 关闭
    def close(self) -> None:
        self._closed = True
