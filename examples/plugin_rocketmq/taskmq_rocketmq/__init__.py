"""示例插件：把一台「消息队列」接成 taskmq transport（本文件不 import 任何 taskmq 内部结构）。

这是一个**假实现**：用进程内存储模拟 RocketMQ 风格的消息队列，目的是演示
docs/design/plugins.md 里「第三方后端怎么接进来」——

- 只依赖公开契约：`taskmq.transport.base` 的数据类型与 `Transport` 协议 +
  `taskmq.protocol.Envelope` + `taskmq.plugins`（注册表）；
- **如实声明能力**：MQ 没有 CAS/租约原语，所以 `supports_leases = False`；
  不支持把「未开始的预留」交回队列，所以在 `limitations` 里显式声明 `yield` 降级；
- 这样框架会在启动时校验（`concurrency_key`/beat 需要租约 → 直接报错），
  一致性套件也会**跳过**被声明的场景，而不是假装一致。

真实接入阿里云 RocketMQ 时，把 `_FakeMQClient` 换成官方 SDK 即可，
其余代码结构不变（见同目录 README.md）。
"""
from __future__ import annotations

import dataclasses
import threading
import time
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import parse_qs, urlparse

from taskmq import TransportOptions, register_transport
from taskmq.errors import LeaseLost, MessageNotFound
from taskmq.plugins import transport_factory
from taskmq.protocol import Codec, JSONCodec
from taskmq.transport.base import (
    UNSET,
    DeadLetter,
    Delivery,
    JobRecord,
    JobState,
    QueueStat,
    Transport,
)

__all__ = ["RocketMQTransport", "register"]

_PRIORITY_BASE = 1_000_000_000          # 高优先级排前面（演示用，真实 MQ 一般按 topic 分档）


@dataclasses.dataclass
class _Message:
    message_id: int
    job_id: str
    queue: str
    envelope: Any
    priority: int
    state: str = "queued"               # queued / reserved / acked / dead / expired
    visible_at: float = 0.0
    claimed_by: str = ""
    claimed_at: float = 0.0
    lease_until: float = 0.0
    deliveries: int = 0
    reason: str = ""


class _FakeMQClient:
    """假 MQ：一台最小的「按优先级 + 延迟投递」的队列服务。真实实现换成 SDK。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._messages: dict[int, _Message] = {}
        self._jobs: dict[str, JobRecord] = {}
        self._keys: dict[str, str] = {}
        self._seq = 0

    # ------------------------------------------------------------------ 基础
    def now(self) -> float:
        return time.time()

    def send(self, message: _Message, *, delay: float, key: str | None) -> str:
        with self._lock:
            if key:
                existing = self._keys.get(key)
                if existing is not None:
                    return existing
            self._seq += 1
            message.message_id = self._seq
            message.visible_at = self.now() + max(0.0, delay)
            self._messages[message.message_id] = message
            if key:
                self._keys[key] = message.job_id
            return message.job_id

    def expire(self, message: _Message) -> None:
        """消息过期 → 同时把 job 置 EXPIRED（契约要求，一致性套件会验）。"""
        message.state = "expired"
        self.set_state(message.job_id, JobState.EXPIRED, error="expired before execution")

    def _visible(self, message: _Message, now: float) -> bool:
        return message.state == "queued" and message.visible_at <= now

    def receive(self, queues: Sequence[str], *, limit: int, worker: str, lease: float) -> list[_Message]:
        now = self.now()
        granted: list[_Message] = []
        with self._lock:
            for message in self._messages.values():                 # 过期先出清
                expires_at = message.envelope.expires_at
                if (
                    message.state == "queued"
                    and expires_at is not None
                    and expires_at <= now
                ):
                    self.expire(message)
            candidates = sorted(
                (m for m in self._messages.values() if m.queue in queues and self._visible(m, now)),
                key=lambda m: (-m.priority, m.message_id),
            )
            for message in candidates[: max(0, limit)]:
                message.state = "reserved"
                message.claimed_by = worker
                message.claimed_at = now
                message.lease_until = now + lease
                message.deliveries += 1
                granted.append(message)
        return granted

    def mutate(self, message_id: int, worker: str, action: str, **kwargs: Any) -> str:
        """状态转换；返回 ok / missing / lost（与内建 transport 的判定一致）。"""
        now = self.now()
        with self._lock:
            message = self._messages.get(message_id)
            if message is None:
                return "missing"
            if action == "ack" and message.state == "acked":
                return "ok"
            if action == "dead" and message.state == "dead":
                return "ok"
            if message.state != "reserved" or message.claimed_by != worker:
                return "lost"
            if action == "ack":
                message.state = "acked"
            elif action == "dead":
                message.state = "dead"
                message.reason = str(kwargs.get("reason", ""))
            elif action in ("requeue", "defer"):
                message.state = "queued"
                message.visible_at = now + float(kwargs.get("delay", 0.0))
                if action == "defer":
                    message.deliveries = max(0, message.deliveries - 1)   # 推迟不算一次投递
            elif action == "extend":
                message.lease_until = now + float(kwargs.get("seconds", 0.0))
                return "ok"
            message.claimed_by = ""
            message.claimed_at = 0.0
            message.lease_until = 0.0
        return "ok"

    def reap(self, now: float) -> int:
        count = 0
        with self._lock:
            for message in self._messages.values():
                if message.state == "reserved" and message.lease_until <= now:
                    message.state = "queued"
                    message.visible_at = now
                    message.claimed_by = ""
                    message.lease_until = 0.0
                    count += 1
        return count

    def set_state(self, job_id: str, state: str, **fields: Any) -> JobRecord:
        with self._lock:
            current = self._jobs.get(job_id)
            meta = dict(current.meta) if current else {}
            meta.update(fields.pop("meta", {}))
            record = JobRecord(
                job_id=job_id,
                task=fields.get("task") or (current.task if current else ""),
                state=state,
                attempt=int(fields.get("attempt") or (current.attempt if current else 0)),
                result=fields.get("result", current.result if current else None),
                has_result=bool(fields.get("has_result", current.has_result if current else False)),
                error=fields.get("error", current.error if current else None),
                meta=meta,
                created_at=current.created_at if current else self.now(),
                updated_at=self.now(),
            )
            self._jobs[job_id] = record
            return record

    def messages(self) -> list[_Message]:
        with self._lock:
            return list(self._messages.values())


class RocketMQTransport(Transport):
    """`rocketmq://endpoints/topic?group=workers` → taskmq transport（示例）。"""

    #: MQ 没有 CAS / 租约原语 → `concurrency_key` 与 beat 选主在这个后端上直接启动报错
    supports_leases = False
    #: 也不维护 worker 心跳表（`taskmq status` 会显示"(不支持)"）
    supports_workers = False
    #: 显式声明语义降级：让位（G2）在 MQ 上只能退化成"延迟重投"
    limitations = {
        "yield": "MQ 无「未开始预留交回队列」原语；让位退化为延迟重投（yields 计数不维护）",
    }

    def __init__(
        self,
        *,
        endpoints: str,
        topic: str,
        group: str = "taskmq",
        codec: Codec | None = None,
        client: _FakeMQClient | None = None,
    ) -> None:
        self.endpoints = endpoints
        self.topic = topic
        self.group = group
        self._codec = codec or JSONCodec()
        self._client = client or _FakeMQClient()

    # ------------------------------------------------------------------ 写入
    def enqueue(
        self,
        env: Any,
        *,
        queue: str | None = None,
        delay: float = 0.0,
        priority: int | None = None,
    ) -> str:
        target = queue or env.queue or self.topic
        prio = env.priority if priority is None else int(priority)
        routed = env if (env.queue == target and env.priority == prio) else env.with_routing(target, prio)
        self._codec.encode(routed)                     # 生产侧先编码，坏载荷这里就炸（不拖到消费侧）
        return self._client.send(
            _Message(
                message_id=0,
                job_id=routed.id,
                queue=target,
                envelope=routed,
                priority=prio,
            ),
            delay=delay,
            key=routed.key,
        )

    def reserve(
        self, queues: Sequence[str], *, worker_id: str, lease: float, limit: int
    ) -> list[Delivery]:
        if limit <= 0 or not queues:
            return []
        return [
            Delivery(
                job_id=message.job_id,
                message_id=message.message_id,
                queue=message.queue,
                envelope=message.envelope,
                worker_id=worker_id,
                attempt=message.envelope.attempt,
                deliveries=message.deliveries,
                lease_until=message.lease_until,
                reserved_at=message.claimed_at,
                priority=message.priority,
            )
            for message in self._client.receive(
                list(dict.fromkeys(queues)), limit=limit, worker=worker_id, lease=lease
            )
        ]

    # ------------------------------------------------------------------ 状态
    def _transition(self, delivery: Delivery, action: str, *, required: bool = False, **kwargs: Any) -> str:
        status = self._client.mutate(delivery.message_id, delivery.worker_id, action, **kwargs)
        if status == "missing" and required:
            raise MessageNotFound(f"消息 {delivery.message_id} 不存在")
        if status == "lost" and required:
            raise LeaseLost(f"消息 {delivery.message_id} 已被回收")
        return status

    def ack(self, delivery: Delivery) -> None:
        self._transition(delivery, "ack", required=True)

    def nack(self, delivery: Delivery, *, requeue: bool = True, delay: float = 0.0) -> None:
        if requeue:
            self._transition(delivery, "requeue", required=True, delay=delay)
        else:
            self._transition(delivery, "dead", reason="nacked without requeue")

    def dead_letter(self, delivery: Delivery, reason: str) -> None:
        self._transition(delivery, "dead", required=True, reason=reason)

    def extend_lease(self, delivery: Delivery, seconds: float) -> None:
        self._transition(delivery, "extend", seconds=seconds)

    def defer(self, delivery: Delivery, *, delay: float = 0.0) -> None:
        self._transition(delivery, "defer", delay=delay)

    def yield_reservation(self, delivery: Delivery, *, delay: float = 0.0, max_yields: int = 100) -> bool:
        """MQ 没有"交回未开始的预留"原语 → 直接声明不支持（`limitations` 里写了）。"""
        return False

    # ------------------------------------------------------------------ 查询
    def peek_max_priority(self, queues: Sequence[str], *, now: float | None = None) -> int | None:
        moment = self._client.now() if now is None else float(now)
        visible = [
            m
            for m in self._client.messages()
            if m.queue in queues and m.state == "queued" and m.visible_at <= moment
        ]
        return max((m.priority for m in visible), default=None)

    def next_visible_at(self, queues: Sequence[str], *, now: float | None = None) -> float | None:
        pending = [m for m in self._client.messages() if m.queue in queues and m.state == "queued"]
        if not pending:
            return None
        return min(m.visible_at for m in pending)

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
        fields: dict[str, Any] = {"meta": meta}
        if task is not None:
            fields["task"] = task
        if attempt is not None:
            fields["attempt"] = attempt
        if result is not UNSET:
            fields["result"] = result
            fields["has_result"] = True
        if error is not UNSET:
            fields["error"] = "" if error is None else str(error)
        return self._client.set_state(job_id, state, **fields)

    def get_state(self, job_id: str) -> JobRecord | None:
        return self._client._jobs.get(job_id)

    def queue_stats(self, queues: Sequence[str] | None = None) -> list[QueueStat]:
        targets = (
            list(dict.fromkeys(queues))
            if queues
            else sorted({m.queue for m in self._client.messages()})
        )
        stats = []
        for queue in targets:
            messages = [m for m in self._client.messages() if m.queue == queue]
            stats.append(
                QueueStat(
                    queue=queue,
                    pending=sum(1 for m in messages if m.state in ("queued", "expired")),
                    inflight=sum(1 for m in messages if m.state == "reserved"),
                    dead=sum(1 for m in messages if m.state == "dead"),
                )
            )
        return stats

    # ------------------------------------------------------------------ 回收
    def reap_expired_leases(self, now: float | None = None) -> int:
        return self._client.reap(self._client.now() if now is None else float(now))

    def reap_expired_jobs(self, now: float | None = None) -> int:
        moment = self._client.now() if now is None else float(now)
        expired = 0
        for message in self._client.messages():
            if (
                message.state == "queued"
                and message.envelope.expires_at is not None
                and message.envelope.expires_at <= moment
            ):
                self._client.expire(message)
                expired += 1
        return expired

    # ------------------------------------------------------------------ DLQ
    def dead_letters(self, *, queue: str | None = None) -> list[DeadLetter]:
        return [
            DeadLetter(
                message_id=message.message_id,
                job_id=message.job_id,
                queue=message.queue,
                task=message.envelope.task,
                envelope=message.envelope,
                reason=message.reason,
                deliveries=message.deliveries,
                failed_at=message.claimed_at,
            )
            for message in self._client.messages()
            if message.state == "dead" and (queue is None or message.queue == queue)
        ]

    def replay_dead(
        self, message_id: int, *, queue: str | None = None, priority: int | None = None
    ) -> bool:
        for message in self._client.messages():
            if message.message_id == message_id and message.state == "dead":
                message.state = "queued"
                message.reason = ""
                message.deliveries = 0
                message.visible_at = self._client.now()
                if queue is not None:
                    message.queue = queue
                if priority is not None:
                    message.priority = int(priority)
                return True
        return False

    def priority_stats(self, queues: Sequence[str] | None = None) -> dict[int, int]:
        buckets: dict[int, int] = {}
        for message in self._client.messages():
            if message.state == "queued" and (queues is None or message.queue in queues):
                buckets[message.priority] = buckets.get(message.priority, 0) + 1
        return dict(sorted(buckets.items()))

    def close(self) -> None:
        return None


def _factory(options: TransportOptions) -> RocketMQTransport:
    """`rocketmq://host:port/topic?group=g&endpoints=...` → transport（URL 语义由插件自己定）。"""
    parsed = urlparse(options.url)
    query: Mapping[str, list[str]] = parse_qs(parsed.query)
    return RocketMQTransport(
        endpoints=parsed.netloc or (query.get("endpoints") or [""])[0],
        topic=parsed.path.lstrip("/") or "taskmq",
        group=(query.get("group") or ["taskmq"])[0],
        codec=options.codec,
    )


def register() -> None:
    """注册到 taskmq（模块导入即调用；entry point 也可以指到本模块）。**幂等**。"""
    if transport_factory("rocketmq") is None:          # 重复 import / 重复调用都安全
        register_transport("rocketmq", _factory)


register()
