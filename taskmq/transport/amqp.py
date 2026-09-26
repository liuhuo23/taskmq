"""AMQP transport（RabbitMQ）：把投递交给 broker，状态交给侧车（Phase 2）。

## 为什么需要"状态侧车"

AMQP 是**消息代理**，不是存储：它没有 job 状态、DAG 需要的 job 枚举、命名租约、幂等键的 KV 空间。
硬凑会得到一堆"假装支持"的语义。所以本 transport 明确分工：

| 职责 | 归谁 |
|---|---|
| 消息投递、优先级排序、延迟、死信、未确认重投 | **RabbitMQ**（`x-max-priority`、per-message TTL + DLX） |
| job 状态/结果、`list_jobs`（DAG 补偿推进用）、命名租约（beat 选主）、worker 注册表、幂等键 |
  **状态侧车**（`?state=sqlite:///…` 等任意 taskmq transport） |

URL：`amqp://user:pass@host:5672/vhost?state=sqlite:///./taskmq.db&prefix=taskmq.`
没有 `state=` 直接报错（fail fast，而不是运行时才发现 `handle.get()` 拿不到结果）。

## 语义与降级（`limitations` 声明，一致性套件据此跳过）

- **同队列内**优先级由 broker 的 `x-max-priority` 排序（RabbitMQ 文档说明是 best-effort）；
  **跨队列不做全局严格优先** → 声明 `global_priority` 降级（套件会跳过该场景）。
- 过期在 `reserve` 时判定（AMQP 无法按属性检索消息）→ `reap_expired_jobs()` 是 no-op。
- `priority_stats()` 无法聚合（AMQP 没有按优先级计数的接口）→ 返回空。
- 延迟用 per-message TTL + DLX：RabbitMQ 只在**队头附近**过期，短延迟排长延迟后面会被顶住
  → 声明 `delay_precision` 降级（对退避/让位这类小延迟够用）。

## 连接与线程

pika 的 `BlockingConnection` 不是线程安全的。Worker 的真实用法是「主线程 reserve、线程池 ack」，
所以这里用**一条连接 + 一把锁**串行化所有 broker 操作（简单且不会踩 pika 的雷）；
通道损坏/连接断开时自动重连一次。吞吐受单通道限制，是真·多机的阶段 2 后续优化点。
"""
from __future__ import annotations

import contextlib
import os
import threading
import time
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import parse_qs, urlparse

from ..errors import ConfigError, LeaseLost, TransportError
from ..protocol import Codec, CodecRegistry, Envelope, JSONCodec
from .base import (
    DeadLetter,
    Delivery,
    JobRecord,
    JobState,
    QueueStat,
    Transport,
    WorkerInfo,
)

__all__ = ["AmqpTransport", "parse_amqp_url"]

_DRIVER_HINT = "AMQP transport 需要 pika：pip install 'taskmq-py[amqp]'"

#: 我们自己的优先级域 -9..9 → AMQP 0..18
_PRIORITY_OFFSET = 9
_MAX_PRIORITY = 20

_H_ID = "x-taskmq-id"
_H_QUEUE = "x-taskmq-queue"
_H_PRIORITY = "x-taskmq-priority"
_H_DELIVERIES = "x-taskmq-deliveries"
_H_YIELDS = "x-taskmq-yields"
_H_YIELDABLE = "x-taskmq-yieldable"
_H_EXPIRES = "x-taskmq-expires"


def _import_driver() -> Any:
    try:
        import pika
    except ImportError as exc:  # pragma: no cover - 取决于环境
        raise ConfigError(_DRIVER_HINT) from exc
    return pika


class AmqpTransport(Transport):
    """`amqp://…?state=<transport url>`。"""

    #: 支持的资源由侧车决定（命名租约/worker 表/job 枚举都委托给它）
    limitations = {
        "global_priority": "跨队列不做全局严格优先（AMQP 是 per-queue 有序）；同队列内用 x-max-priority 排序",
        "delay_precision": "延迟用 per-message TTL + DLX，RabbitMQ 只在队头附近过期（小延迟够用）",
        "priority_stats": "AMQP 无按优先级聚合的接口 → priority_stats() 返回空",
        "reap_expired_jobs": "过期在 reserve 时判定；reap_expired_jobs() 是 no-op",
    }

    def __init__(
        self,
        url: str,
        *,
        state: Transport,
        codec: Codec | None = None,
        registry: CodecRegistry | None = None,
        prefix: str = "taskmq.",
        clock: Any | None = None,
        heartbeat: int = 60,
        idempotency_ttl: float = 86400.0,
        max_message_bytes: int | None = None,
    ) -> None:
        self._pika = _import_driver()
        self._url = url
        self._state = state
        self._codec: Codec = codec if codec is not None else JSONCodec()
        self._registry = registry if registry is not None else CodecRegistry()
        self._prefix = prefix if prefix.endswith(".") else prefix + "."
        self._clock = clock or time.time
        self._heartbeat = int(heartbeat)
        self._idempotency_ttl = float(idempotency_ttl)
        self._max_message_bytes = max_message_bytes
        self._lock = threading.RLock()
        self._conn: Any = None
        self._channel: Any = None
        self._declared: set[str] = set()
        #: 未确认投递：message_id -> (delivery_tag, 逻辑队列, lease_until, Delivery)
        self._unacked: dict[int, tuple[int, str, float, Delivery]] = {}
        self._delayed: dict[str, list[float]] = {}
        self._rr: dict[tuple[str, ...], int] = {}
        #: 被本进程回收过的 message_id（区分"已 ack 过（幂等）"与"租约被回收（LeaseLost）"）
        self._reaped: dict[int, None] = {}
        self._next_id = int.from_bytes(os.urandom(6), "big")
        # 能力跟随侧车（侧车不支持的能力，这里也不谎报）
        self.supports_leases = bool(getattr(state, "supports_leases", False))
        self.supports_workers = bool(getattr(state, "supports_workers", False))
        self.supports_job_listing = bool(getattr(state, "supports_job_listing", False))
        self._connect()

    # ------------------------------------------------------------------ 连接
    def _connect(self) -> None:
        params = self._pika.URLParameters(self._url)
        params.heartbeat = self._heartbeat
        try:
            self._conn = self._pika.BlockingConnection(params)
            self._channel = self._conn.channel()
            self._channel.confirm_delivery()          # 生产者侧确认：发布失败要看得见
        except Exception as exc:
            raise TransportError(f"连不上 RabbitMQ（{self._url.split('@')[-1]}）：{exc}") from exc
        self._declared.clear()

    def _broker(self) -> Any:
        """拿一个可用的 channel；坏了重连一次。"""
        with self._lock:
            if self._channel is None or not getattr(self._channel, "is_open", False):
                self._connect()
            return self._channel

    def _retry(self, fn: Any) -> Any:
        with self._lock:
            try:
                return fn(self._broker())
            except (self._pika.exceptions.AMQPConnectionError, self._pika.exceptions.ChannelWrongStateError):
                self._connect()
                return fn(self._channel)

    def close(self) -> None:
        with self._lock:
            with contextlib.suppress(Exception):
                if self._conn is not None and self._conn.is_open:
                    self._conn.close()
            self._conn = None
            self._channel = None
        with contextlib.suppress(Exception):
            self._state.close()

    # ------------------------------------------------------------------ 命名
    def _work(self, queue: str) -> str:
        return f"{self._prefix}{queue}"

    def _delay(self, queue: str) -> str:
        return f"{self._prefix}{queue}.delay"

    def _dlq(self, queue: str) -> str:
        return f"{self._prefix}{queue}.dlq"

    def _work_args(self, queue: str) -> dict[str, Any]:
        """work 队列参数：优先级 + 死信到 dlq。"""
        return {
            "x-max-priority": _MAX_PRIORITY,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": self._dlq(queue),
        }

    def _delay_args(self, queue: str) -> dict[str, Any]:
        """delay 队列参数：TTL 到期后死信回 work（经典"延迟队列"套路）。"""
        return {"x-dead-letter-exchange": "", "x-dead-letter-routing-key": self._work(queue)}

    def _declare(self, channel: Any, queue: str) -> None:
        """声明三条队列：work（优先级 + DLX）、delay（TTL 回投 work）、dlq。

        注意：RabbitMQ 对同名队列的 **arguments 必须完全一致**，否则 406 PRECONDITION_FAILED；
        所以参数统一由 `_work_args/_delay_args/_dlq_args` 生成，任何地方都不要手写。
        """
        if queue in self._declared:
            return
        channel.queue_declare(queue=self._work(queue), durable=True, arguments=self._work_args(queue))
        channel.queue_declare(queue=self._delay(queue), durable=True, arguments=self._delay_args(queue))
        channel.queue_declare(queue=self._dlq(queue), durable=True)
        self._declared.add(queue)

    def _drop_queues(self, queue: str) -> None:
        """删除本前缀下某逻辑队列的三条 AMQP 队列（测试清理用）。"""
        for name in (self._work(queue), self._delay(queue), self._dlq(queue)):
            with contextlib.suppress(Exception):
                self._retry(lambda ch, name=name: ch.queue_delete(queue=name))
        self._declared.discard(queue)

    # ------------------------------------------------------------------ 工具
    def _now(self) -> float:
        return float(self._clock())

    def _properties(self, envelope: Envelope, *, message_id: int, priority: int, delay: float | None) -> Any:
        headers = {
            _H_ID: message_id,
            _H_QUEUE: envelope.queue,
            _H_PRIORITY: int(priority),
            _H_DELIVERIES: 0,
            _H_YIELDS: 0,
            _H_YIELDABLE: True,
            # AMQP 0-9-1 的 field table **没有 float 类型** → 时间戳一律存字符串
            _H_EXPIRES: None if envelope.expires_at is None else repr(float(envelope.expires_at)),
        }
        return self._pika.BasicProperties(
            delivery_mode=2,                              # 持久化
            priority=max(0, min(_MAX_PRIORITY - 1, int(priority) + _PRIORITY_OFFSET)),
            expiration=None if delay is None else str(max(1, int(delay * 1000))),
            headers=headers,
            content_type="application/octet-stream",
        )

    def _publish(
        self,
        *,
        queue: str,
        envelope: Envelope,
        message_id: int,
        headers: Mapping[str, Any] | None = None,
        delay: float | None = None,
    ) -> None:
        priority = int(headers.get(_H_PRIORITY, envelope.priority) if headers else envelope.priority)
        target = self._delay(queue) if delay and delay > 0 else self._work(queue)
        blob = self._codec.encode(envelope, max_bytes=self._max_message_bytes)
        props = self._properties(envelope, message_id=message_id, priority=priority, delay=delay)

        def publish(channel: Any) -> None:
            self._declare(channel, queue)
            if headers:
                merged = dict(props.headers)
                merged.update(headers)
                props.headers = merged
            channel.basic_publish("", target, blob, properties=props)

        self._retry(publish)
        if delay and delay > 0:
            self._delayed.setdefault(queue, []).append(self._now() + delay)

    # ------------------------------------------------------------------ 入队
    def enqueue(
        self,
        env: Envelope,
        *,
        queue: str | None = None,
        delay: float = 0.0,
        priority: int | None = None,
    ) -> str:
        target = queue or env.queue or "default"
        prio = env.priority if priority is None else int(priority)
        effective = env if (env.queue == target and env.priority == prio) else env.with_routing(target, prio)
        # 幂等键：委托侧车（AMQP 没有 KV；侧车怎么做由它的语义决定）
        if env.key:
            existing = self._state.get_state(f"key:{env.key}")
            if existing is not None and existing.meta.get("for_job"):
                return str(existing.meta["for_job"])
        self._next_id += 1
        message_id = self._next_id
        head = {
            _H_ID: message_id,
            _H_QUEUE: target,
            _H_PRIORITY: prio,
            _H_DELIVERIES: 0,
            _H_YIELDS: 0,
            _H_YIELDABLE: True,
        }
        self._publish(
            queue=target,
            envelope=effective,
            message_id=message_id,
            headers=head,
            delay=delay if delay > 0 else None,
        )
        visible_at = self._now() + max(0.0, float(delay))
        if env.eta is not None:
            visible_at = max(visible_at, float(env.eta))
        delivered = visible_at + max(0.0, float(delay))
        self._state.set_state(
            env.id,
            JobState.QUEUED,
            task=env.task,
            attempt=env.attempt,
            queue=target,
            priority=prio,
            message_id=message_id,
            enqueued_at=delivered,
        )
        if env.key:
            self._state.set_state(
                f"key:{env.key}", JobState.QUEUED, task="idempotency", for_job=env.id,
                until=self._now() + self._idempotency_ttl,
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
        wanted = list(dict.fromkeys(queues))
        if limit <= 0 or not wanted:
            return []
        now = self._now()
        deliveries: list[Delivery] = []
        key = tuple(wanted)
        start = self._rr.get(key, 0)
        for step in range(len(wanted) * max(1, limit)):
            if len(deliveries) >= limit:
                break
            queue = wanted[(start + step) % len(wanted)]
            got = self._reserve_one(queue, worker_id=worker_id, lease=lease, now=now)
            if got is not None:
                deliveries.append(got)
        self._rr[key] = (start + max(1, len(deliveries))) % len(wanted)
        return deliveries

    def _reserve_one(
        self, queue: str, *, worker_id: str, lease: float, now: float
    ) -> Delivery | None:
        def fetch(channel: Any) -> Any:
            self._declare(channel, queue)
            return channel.basic_get(self._work(queue), auto_ack=False)

        method, props, body = self._retry(fetch)
        if method is None:
            return None
        headers = dict(props.headers or {})
        envelope = self._codec.decode(bytes(body))
        message_id = int(headers.get(_H_ID, method.delivery_tag))
        expires = headers.get(_H_EXPIRES)
        # 故意写成 if/else 而不是三元表达式：三元形式下 mypy 1.x 不做收窄，
        # 会报 float(Any | None)（headers 是 dict[Any, Any]）
        if expires is None or expires == "":  # noqa: SIM108
            expires_at = envelope.expires_at
        else:
            expires_at = float(expires)
        if expires_at is not None and float(expires_at) <= now:
            self._retry(lambda ch: ch.basic_ack(method.delivery_tag))
            self._state.set_state(
                envelope.id, JobState.EXPIRED, task=envelope.task,
                error="expired before execution", queue=queue,
            )
            return None
        lease_until = now + float(lease)
        delivery = Delivery(
            job_id=envelope.id,
            message_id=message_id,
            queue=queue,
            envelope=envelope,
            worker_id=worker_id,
            attempt=envelope.attempt,
            deliveries=int(headers.get(_H_DELIVERIES, 0)) + 1,
            lease_until=lease_until,
            reserved_at=now,
            priority=int(headers.get(_H_PRIORITY, envelope.priority)),
            yields=int(headers.get(_H_YIELDS, 0)),
        )
        with self._lock:
            self._unacked[message_id] = (method.delivery_tag, queue, lease_until, delivery)
        # 注意：不要因为"又投递了一次"就把 message_id 从 _reaped 里删掉——
        # 旧投递对象是**失效**的，它的迟到 ack 必须继续报 LeaseLost（幂等只针对同一投递）。
        return delivery

    def _entry_for(self, delivery: Delivery) -> tuple[int, str, float, Delivery] | None:
        """只认**同一代**投递：重投后同一个 message_id 会换一个 reserved_at，
        旧投递对象不得操作新投递（否则过期 ack 会把新投递悄悄 ack 掉）。"""
        with self._lock:
            entry = self._unacked.get(delivery.message_id)
            if entry is None:
                return None
            return entry if entry[3].reserved_at == delivery.reserved_at else None

    def _take(self, delivery: Delivery) -> tuple[int, str, float, Delivery] | None:
        with self._lock:
            entry = self._entry_for(delivery)
            if entry is None:
                return None
            self._unacked.pop(delivery.message_id, None)
            return entry

    def _headers_of(self, delivery: Delivery) -> dict[str, Any]:
        return {
            _H_ID: delivery.message_id,
            _H_QUEUE: delivery.queue,
            _H_PRIORITY: int(delivery.priority),
            _H_DELIVERIES: int(delivery.deliveries),
            _H_YIELDS: int(delivery.yields),
            _H_YIELDABLE: True,
            _H_EXPIRES: (
                None if delivery.envelope.expires_at is None else repr(float(delivery.envelope.expires_at))
            ),
        }

    def ack(self, delivery: Delivery) -> None:
        entry = self._take(delivery)
        if entry is None:
            if delivery.message_id in self._reaped:
                raise LeaseLost(f"消息 {delivery.message_id} 已被回收，ack 被拒绝")
            return                                        # 幂等：已经 ack 过了
        tag, _queue, _until, _saved = entry
        self._retry(lambda ch: ch.basic_ack(tag))

    def nack(self, delivery: Delivery, *, requeue: bool = True, delay: float = 0.0) -> None:
        if not requeue:
            self.dead_letter(delivery, "nacked without requeue")
            return
        self._requeue(delivery, delay=delay, deliveries=int(delivery.deliveries))

    def defer(self, delivery: Delivery, *, delay: float = 0.0) -> None:
        """放回队列且不消耗投递次数（限流等待 / concurrency_key 抢不到锁）。"""
        self._requeue(delivery, delay=delay, deliveries=max(0, int(delivery.deliveries) - 1))

    def yield_reservation(
        self, delivery: Delivery, *, delay: float = 0.0, max_yields: int = 100
    ) -> bool:
        yields = int(delivery.yields)
        if yields >= max_yields:
            return False
        self._requeue(delivery, delay=delay, deliveries=int(delivery.deliveries), yields=yields + 1)
        return True

    def _requeue(
        self,
        delivery: Delivery,
        *,
        delay: float,
        deliveries: int,
        yields: int | None = None,
    ) -> None:
        entry = self._take(delivery)
        if entry is None:
            raise LeaseLost(f"消息 {delivery.message_id} 已被回收")
        tag, queue, _until, _saved = entry
        self._retry(lambda ch: ch.basic_ack(tag))         # 先确认原消息，再重投一份
        headers = self._headers_of(delivery)
        headers[_H_DELIVERIES] = int(deliveries)
        headers[_H_QUEUE] = delivery.queue
        if yields is not None:
            headers[_H_YIELDS] = int(yields)
            headers[_H_YIELDABLE] = yields < 100
        self._publish(
            queue=queue,
            envelope=delivery.envelope,
            message_id=delivery.message_id,
            headers=headers,
            delay=delay if delay > 0 else None,
        )

    def dead_letter(self, delivery: Delivery, reason: str) -> None:
        entry = self._take(delivery)
        if entry is None:
            if delivery.message_id in self._reaped:
                raise LeaseLost(f"消息 {delivery.message_id} 已被回收")
            return
        tag, queue, _until, _saved = entry
        self._retry(lambda ch: ch.basic_ack(tag))
        blob = self._codec.encode(delivery.envelope, max_bytes=self._max_message_bytes)
        headers = self._headers_of(delivery)
        headers["x-taskmq-reason"] = reason

        def publish(channel: Any) -> None:
            self._declare(channel, queue)
            channel.basic_publish(
                "",
                self._dlq(queue),
                blob,
                properties=self._pika.BasicProperties(
                    delivery_mode=2, headers=headers, priority=0
                ),
            )

        self._retry(publish)
        self._state.set_state(
            delivery.job_id, JobState.FAILED, task=delivery.envelope.task,
            attempt=delivery.attempt, error=reason, queue=queue,
        )

    def extend_lease(self, delivery: Delivery, seconds: float) -> None:
        with self._lock:
            entry = self._entry_for(delivery)
            if entry is None:
                raise LeaseLost(f"消息 {delivery.message_id} 已被回收")
            tag, queue, _until, saved = entry
            self._unacked[delivery.message_id] = (
                tag, queue, self._now() + float(seconds), saved,
            )

    # ------------------------------------------------------------------ 插队
    def peek_max_priority(self, queues: Sequence[str], *, now: float | None = None) -> int | None:
        """AMQP 不能"偷看"队列头 → 取一条读优先级再 `nack(requeue=True)` 放回（会保持优先级）。"""
        best: int | None = None
        for queue in dict.fromkeys(queues):
            def peek(channel: Any, queue: str = queue) -> Any:
                self._declare(channel, queue)
                return channel.basic_get(self._work(queue), auto_ack=False)

            method, props, _body = self._retry(peek)
            if method is None:
                continue
            headers = dict(props.headers or {})
            current = int(headers.get(_H_PRIORITY, 0))
            best = current if best is None else max(best, current)
            self._retry(lambda ch, tag=method.delivery_tag: ch.basic_nack(tag, requeue=True))
        return best

    def next_visible_at(self, queues: Sequence[str], *, now: float | None = None) -> float | None:
        """只反映**本进程**发起的延迟（defer/让位/带 delay 的入队）；跨进程的延迟在 broker 里不可见。"""
        moment = self._now() if now is None else float(now)
        upcoming: list[float] = []
        for queue in dict.fromkeys(queues):
            pending = [due for due in self._delayed.get(queue, []) if due > moment]
            self._delayed[queue] = pending
            upcoming.extend(pending)
        return min(upcoming) if upcoming else None

    # -------------------------------------------------------------- job 状态（侧车）
    @property
    def state_transport(self) -> Transport:
        return self._state

    def set_state(self, job_id: str, state: str, **kwargs: Any) -> JobRecord:
        return self._state.set_state(job_id, state, **kwargs)

    def get_state(self, job_id: str) -> JobRecord | None:
        return self._state.get_state(job_id)

    def list_jobs(self, **kwargs: Any) -> list[JobRecord]:
        return self._state.list_jobs(**kwargs)

    def register_worker(self, worker_id: str, **kwargs: Any) -> None:
        self._state.register_worker(worker_id, **kwargs)

    def heartbeat_worker(self, worker_id: str, **kwargs: Any) -> None:
        self._state.heartbeat_worker(worker_id, **kwargs)

    def deregister_worker(self, worker_id: str) -> None:
        self._state.deregister_worker(worker_id)

    def list_workers(self, **kwargs: Any) -> list[WorkerInfo]:
        return self._state.list_workers(**kwargs)

    def acquire_lease(self, name: str, owner: str, ttl: float) -> bool:
        return self._state.acquire_lease(name, owner, ttl)

    def release_lease(self, name: str, owner: str) -> None:
        self._state.release_lease(name, owner)

    def renew_lease(self, name: str, owner: str, ttl: float) -> bool:
        return self._state.renew_lease(name, owner, ttl)

    # ------------------------------------------------------------------ 统计
    def queue_stats(self, queues: Sequence[str] | None = None) -> list[QueueStat]:
        targets = list(dict.fromkeys(queues)) if queues else list(self._declared)
        stats = []
        for queue in targets:
            counts = self._queue_counts(queue)
            inflight = sum(1 for _tag, name, _until, _d in list(self._unacked.values()) if name == queue)
            stats.append(
                QueueStat(
                    queue=queue,
                    pending=counts["pending"],
                    inflight=inflight,
                    dead=counts["dead"],
                )
            )
        return stats

    def _queue_counts(self, queue: str) -> dict[str, int]:
        def counts(channel: Any) -> dict[str, int]:
            # 用与 _declare 完全一致的 arguments 再声明一次，顺便拿到 message_count
            work = channel.queue_declare(
                queue=self._work(queue), durable=True, arguments=self._work_args(queue)
            ).method.message_count
            delay = channel.queue_declare(
                queue=self._delay(queue), durable=True, arguments=self._delay_args(queue)
            ).method.message_count
            dead = channel.queue_declare(queue=self._dlq(queue), durable=True).method.message_count
            self._declared.add(queue)
            return {"pending": int(work) + int(delay), "dead": int(dead)}

        return self._retry(counts)

    def priority_stats(self, queues: Sequence[str] | None = None) -> dict[int, int]:
        return {}                                     # AMQP 没有按优先级聚合的接口（已声明降级）

    # ------------------------------------------------------------------ 回收
    def reap_expired_leases(self, now: float | None = None) -> int:
        """把本进程持有、租约已过的投递 `nack(requeue=True)` 交回 broker。

        进程整个挂掉时不需要这步：未确认消息由 broker 自动重投。
        """
        moment = self._now() if now is None else float(now)
        reaped = 0
        for message_id, (tag, queue, lease_until, delivery) in list(self._unacked.items()):
            if lease_until > moment:
                continue
            with self._lock:
                self._unacked.pop(message_id, None)
                self._reaped[message_id] = None
                while len(self._reaped) > 4096:         # 有界，避免长跑进程里无限增长
                    self._reaped.pop(next(iter(self._reaped)))
            # 用 ack + 重投而不是 nack：nack 会原样重投（头字段里的 deliveries 不会递增），
            # 而我们要让下一次投递看到 deliveries=2（与其它 transport 语义一致）。
            self._retry(lambda ch, tag=tag: ch.basic_ack(tag))
            headers = self._headers_of(delivery)
            headers[_H_DELIVERIES] = int(delivery.deliveries)
            self._publish(
                queue=queue,
                envelope=delivery.envelope,
                message_id=message_id,
                headers=headers,
            )
            reaped += 1
        return reaped

    def reap_expired_jobs(self, now: float | None = None) -> int:
        return 0                                       # 过期在 reserve 时判定（已声明降级）

    # ------------------------------------------------------------------ DLQ
    def dead_letters(self, *, queue: str | None = None) -> list[DeadLetter]:
        """非破坏性扫描：先把 DLQ 拉空（不 ack），再逐条 `nack(requeue=True)` 放回。

        **不能在循环里边取边 requeue**：同一条消息会被立刻再次取到 → 死循环。
        """
        targets = [queue] if queue else sorted(self._declared)
        entries: list[DeadLetter] = []
        for name in targets:
            taken: list[tuple[Any, Any, bytes]] = []
            while True:
                def fetch(channel: Any, name: str = name) -> Any:
                    self._declare(channel, name)
                    return channel.basic_get(self._dlq(name), auto_ack=False)

                method, props, body = self._retry(fetch)
                if method is None:
                    break
                taken.append((method, props, bytes(body)))
            for method, props, body in taken:            # 读完再统一放回
                headers = dict(props.headers or {})
                envelope = self._codec.decode(body)
                entries.append(
                    DeadLetter(
                        message_id=int(headers.get(_H_ID, method.delivery_tag)),
                        job_id=envelope.id,
                        queue=str(headers.get(_H_QUEUE, name)),
                        task=envelope.task,
                        envelope=envelope,
                        reason=str(headers.get("x-taskmq-reason", "")),
                        deliveries=int(headers.get(_H_DELIVERIES, 0)),
                        failed_at=self._now(),
                    )
                )
                self._retry(lambda ch, tag=method.delivery_tag: ch.basic_nack(tag, requeue=True))
        return entries

    def replay_dead(
        self, message_id: int, *, queue: str | None = None, priority: int | None = None
    ) -> bool:
        """从 DLQ 里找到这条、重投回工作队列（attempt 归 1，可覆盖优先级）。"""
        import dataclasses

        targets = [queue] if queue else sorted(self._declared)
        for name in targets:
            scanned: list[tuple[Any, Any, bytes]] = []
            found: tuple[Any, Any, bytes] | None = None
            while True:
                def fetch(channel: Any, name: str = name) -> Any:
                    self._declare(channel, name)
                    return channel.basic_get(self._dlq(name), auto_ack=False)

                method, props, body = self._retry(fetch)
                if method is None:
                    break
                headers = dict(props.headers or {})
                if int(headers.get(_H_ID, method.delivery_tag)) == int(message_id):
                    found = (method, props, bytes(body))
                    break
                scanned.append((method, props, bytes(body)))
            for method, _props, _body in scanned:      # 扫描过的放回
                self._retry(lambda ch, tag=method.delivery_tag: ch.basic_nack(tag, requeue=True))
            if found is None:
                continue
            method, props, body = found
            headers = dict(props.headers or {})
            envelope = dataclasses.replace(self._codec.decode(body), attempt=1)
            if priority is not None:
                envelope = dataclasses.replace(envelope, priority=int(priority))
            target_queue = queue or str(headers.get(_H_QUEUE, "default"))
            self._retry(lambda ch, tag=method.delivery_tag: ch.basic_ack(tag))
            new_headers = {
                _H_ID: int(message_id),
                _H_QUEUE: target_queue,
                _H_PRIORITY: int(headers.get(_H_PRIORITY, envelope.priority))
                if priority is None
                else int(priority),
                _H_DELIVERIES: 0,
                _H_YIELDS: 0,
                _H_YIELDABLE: True,
                _H_EXPIRES: None if envelope.expires_at is None else float(envelope.expires_at),
            }
            self._publish(
                queue=target_queue,
                envelope=envelope.with_routing(target_queue, int(str(new_headers[_H_PRIORITY]))),
                message_id=int(message_id),
                headers=new_headers,
            )
            self._state.set_state(
                envelope.id, JobState.QUEUED, task=envelope.task, attempt=1, queue=target_queue
            )
            return True
        return False


def parse_amqp_url(url: str) -> tuple[str, dict[str, str]]:
    """拆出 `(pika 用的 URL, taskmq 自己的参数)`；`state`/`prefix` 是我们自己的。"""
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    params = {key: values[0] for key, values in query.items()}
    return parsed._replace(query="").geturl(), params
