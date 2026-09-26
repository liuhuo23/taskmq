"""Transport 抽象与必须成立的不变量。

Transport 是消息的可靠投递层，只做五件事：**enqueue / reserve / ack / nack / dead_letter**，
外加租约续期、轻量 job 状态、队列统计和孤儿回收。

必须成立的不变量（所有实现都要满足，并在测试里覆盖）：
1. 一条消息在任意时刻**至多被一个 worker 持有**（租约保证）。
2. 持有者崩溃且租约到期后，消息**必须**重新可见（除非已 ack）。
3. `ack` 之后消息不可再被任何 worker 看到。
4. `reserve` 返回的消息，其 `deliveries` 计数已自增。
5. 队列顺序：同优先级下 FIFO；有 `priority` 时高优先级先出
   （排序键 `(priority DESC, id ASC)`；跨队列的严格公平调度在 Phase 2）。
6. 所有状态转换都是**幂等**的（重复 ack 不报错）。
7. `reap_*` 可由任意进程并发调用而不产生重复投递。
8. 任何错误都不允许「假装成功」——宁可重投。

优先级语义（决策 §20-9）：
- 消息的 `priority` 是整数，越大越先被 claim。
- 解析顺序：`submit(priority=...)` > `@task(priority=...)` > `QueueConfig.priority` >
  `Config.default_priority`。
- 严格优先级可能导致低优先级饥饿；Phase 0/1 不做 aging/公平调度（Phase 2 补）。
"""
from __future__ import annotations

import abc
import dataclasses
from collections.abc import Mapping, Sequence
from typing import Any

from .._compat import _SLOTS
from ..errors import TransportError
from ..protocol import Envelope


class JobState:
    """Job 级状态机（docs/design.md §7.3）。"""

    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    REVOKED = "REVOKED"
    EXPIRED = "EXPIRED"
    #: DAG 专用：上游最终失败导致本节点不会执行（`docs/design/workflows.md`）
    SKIPPED = "SKIPPED"

    ALL = (PENDING, QUEUED, RUNNING, RETRYING, SUCCEEDED, FAILED, REVOKED, EXPIRED, SKIPPED)
    TERMINAL = (SUCCEEDED, FAILED, REVOKED, EXPIRED, SKIPPED)


class MessageState:
    """消息级状态（transport 内部）：一条消息在队列里的可见性状态。"""

    QUEUED = "queued"
    RESERVED = "reserved"
    ACKED = "acked"
    DEAD = "dead"
    EXPIRED = "expired"


class _Unset:
    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        return "UNSET"

    def __bool__(self) -> bool:
        return False


UNSET = _Unset()


@dataclasses.dataclass(frozen=True, **_SLOTS)
class Delivery:
    """一次 reserve 的结果：消息 + 当前投递的上下文。"""

    job_id: str
    message_id: int
    queue: str
    envelope: Envelope
    worker_id: str
    attempt: int
    deliveries: int
    lease_until: float
    reserved_at: float
    priority: int = 0
    yields: int = 0


@dataclasses.dataclass(frozen=True, **_SLOTS)
class JobRecord:
    """轻量 job 状态（`h.state` / `h.info` 的数据来源）。"""

    job_id: str
    task: str
    state: str
    attempt: int = 0
    result: Any = None
    has_result: bool = False
    error: str | None = None
    meta: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    created_at: float = 0.0
    updated_at: float = 0.0

    @property
    def terminal(self) -> bool:
        return self.state in JobState.TERMINAL


@dataclasses.dataclass(frozen=True, **_SLOTS)
class QueueStat:
    """队列深度快照。"""

    queue: str
    pending: int = 0
    inflight: int = 0
    dead: int = 0
    priority: int = 0


@dataclasses.dataclass(frozen=True, **_SLOTS)
class DeadLetter:
    """DLQ 条目：可查询、可重放。"""

    message_id: int
    job_id: str
    queue: str
    task: str
    envelope: Envelope
    reason: str
    deliveries: int
    failed_at: float


@dataclasses.dataclass(frozen=True, **_SLOTS)
class WorkerInfo:
    """一个 worker 进程的心跳快照（`taskmq status` 用它显示 worker 列表）。"""

    worker_id: str
    queues: tuple[str, ...] = ()
    pool: str = ""
    concurrency: int = 0
    started_at: float = 0.0
    heartbeat_at: float = 0.0
    meta: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def alive(self, *, now: float, stale_after: float = 60.0) -> bool:
        """心跳是否还新鲜（超过 `stale_after` 视为掉线）。"""
        return self.heartbeat_at > 0 and (now - self.heartbeat_at) <= stale_after


class Transport(abc.ABC):
    """消息投递层抽象。实现见 `memory.py`（Phase 0）与 `sqlite.py`（Phase 0 下一步）。"""

    # ------------------------------------------------------------------ 写入
    @abc.abstractmethod
    def enqueue(
        self,
        env: Envelope,
        *,
        queue: str | None = None,
        delay: float = 0.0,
        priority: int | None = None,
    ) -> str:
        """入队一条消息。返回 job id（即 `env.id`）。

        `delay` 是相对秒数；`env.eta` 是绝对时间戳，两者取较晚者。
        带 `env.key` 时按幂等键去重：窗口内已存在则**不重复入队**，返回已存在 job id。
        """

    # ------------------------------------------------------------------ 投递
    @abc.abstractmethod
    def reserve(
        self,
        queues: Sequence[str],
        *,
        worker_id: str,
        lease: float,
        limit: int,
    ) -> list[Delivery]:
        """原子地领取至多 `limit` 条可见消息，按 `(priority DESC, id ASC)` 排序。"""

    @abc.abstractmethod
    def ack(self, delivery: Delivery) -> None:
        """确认成功。幂等；若租约已被回收（`LeaseLost`）则抛出，调用方丢弃本地结果。"""

    @abc.abstractmethod
    def nack(self, delivery: Delivery, *, requeue: bool = True, delay: float = 0.0) -> None:
        """拒绝当前投递。`requeue=True` 时在 `delay` 秒后重新可见（重试走这条路）。"""

    @abc.abstractmethod
    def dead_letter(self, delivery: Delivery, reason: str) -> None:
        """投递进 DLQ（不可重试 / 超过 max_deliveries）。"""

    @abc.abstractmethod
    def extend_lease(self, delivery: Delivery, seconds: float) -> None:
        """续租（长任务心跳）。"""

    # ---------------------------------------------------------------- 插队
    def peek_max_priority(self, queues: Sequence[str], *, now: float | None = None) -> int | None:
        """当前**可见**消息里的最高优先级；没有可见消息返回 `None`（决策 §20-9 / G2）。

        注意：`yield_delay` 让位后的消息不可见，因此不会立刻被再次探测到（防抖）。
        """
        return None

    def yield_reservation(
        self,
        delivery: Delivery,
        *,
        delay: float = 0.0,
        max_yields: int = 100,
    ) -> bool:
        """把**未开始**的预留交回队列（让位，G2）。返回是否真的让位。

        - `yields` 独立递增，**不计入** `deliveries`（P13）；
        - 达到 `max_yields` 后该消息不再让位（`yieldable=false`，防抖/防活锁，P14）。
        """
        return False

    def next_visible_at(self, queues: Sequence[str], *, now: float | None = None) -> float | None:
        """最早的可见时间，用于 `run_until_idle` 等待退避/让位延迟；没有待处理消息返回 `None`。"""
        return None

    # -------------------------------------------------------------- job 状态
    @abc.abstractmethod
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
        """写入/合并 job 状态。`meta` 合并进已有 meta，不整体覆盖。"""

    @abc.abstractmethod
    def get_state(self, job_id: str) -> JobRecord | None:
        """读取 job 状态；不存在返回 `None`（不返回 PENDING 占位）。"""

    @abc.abstractmethod
    def queue_stats(self, queues: Sequence[str] | None = None) -> list[QueueStat]:
        """队列深度快照（pending / inflight / dead）。"""

    # ------------------------------------------------------------------ 回收
    @abc.abstractmethod
    def reap_expired_leases(self, now: float | None = None) -> int:
        """把租约到期的 reserved 消息重新置为可见，返回回收条数。幂等。"""

    @abc.abstractmethod
    def reap_expired_jobs(self, now: float | None = None) -> int:
        """把超过 `expires_at` 仍未开始的消息标记为 EXPIRED，返回条数。"""

    # ------------------------------------------------------------------ DLQ
    def dead_letters(self, *, queue: str | None = None) -> list[DeadLetter]:
        """列出 DLQ 条目（实现可选；CLI `taskmq dlq list` 用它）。"""
        return []

    def replay_dead(
        self, message_id: int, *, queue: str | None = None, priority: int | None = None
    ) -> bool:
        """把一条 DLQ 消息重新入队（重置 deliveries/yields）。`priority` 可覆盖（P7）。"""
        return False

    def priority_stats(self, queues: Sequence[str] | None = None) -> dict[int, int]:
        """按优先级分桶的 pending 深度（`status --by-priority` 用它）。"""
        return {}

    # -------------------------------------------------------------- 重排/租约
    def defer(self, delivery: Delivery, *, delay: float = 0.0) -> None:
        """把消息放回队列，且**不消耗投递次数**。

        用于「还没真正执行」的重排：限流等待、`concurrency_key` 抢不到锁。
        默认退化为 `nack(requeue=True)`；内建 transport 会把 `deliveries` 减回去，
        避免被反复推迟的消息被毒丸保护误送 DLQ。
        """
        self.nack(delivery, requeue=True, delay=delay)

    #: 该 transport 是否支持命名租约（`concurrency_key` / beat 选主需要）
    supports_leases: bool = False

    def acquire_lease(self, name: str, owner: str, ttl: float) -> bool:
        """尝试获取命名租约；拿到（或本来就是我持有）返回 True。"""
        raise TransportError(f"{type(self).__name__} 不支持命名租约（concurrency_key / beat 选主需要）")

    def release_lease(self, name: str, owner: str) -> None:
        """释放自己持有的命名租约（幂等）。"""
        raise TransportError(f"{type(self).__name__} 不支持命名租约")

    def renew_lease(self, name: str, owner: str, ttl: float) -> bool:
        """续租；不是自己持有或已过期返回 False。"""
        raise TransportError(f"{type(self).__name__} 不支持命名租约")

    # -------------------------------------------------------------- worker 心跳
    #: 该 transport 是否支持 worker 注册表（`taskmq status` 的 worker 列表）
    supports_workers: bool = False

    #: 是否支持枚举 job（DAG 工作流的补偿推进 / `taskmq workflow list` 需要）。
    #: 不支持时 workflow 功能**启动即报错**，而不是静默不推进（docs/design/workflows.md D8）。
    supports_job_listing: bool = False

    def list_jobs(
        self,
        *,
        prefix: str | None = None,
        states: Sequence[str] | None = None,
        limit: int = 100,
    ) -> list[JobRecord]:
        """按 job id 前缀 / 状态列出 job（新的在前）。不支持时抛 `TransportError`。"""
        raise TransportError(f"{type(self).__name__} 不支持 job 枚举（supports_job_listing=False）")

    #: 主动声明的**语义降级**（键=能力/场景名，值=说明）。`taskmq status` 会打印；
    #: 一致性测试套件（`taskmq.testing.transport_conformance`）据此跳过对应场景——
    #: 让"能力不对等"显式可见，而不是假装一致（docs/design/plugins.md D7）。
    limitations: Mapping[str, str] = {}

    def register_worker(
        self,
        worker_id: str,
        *,
        queues: Sequence[str] = (),
        pool: str = "",
        concurrency: int = 0,
        meta: Mapping[str, Any] | None = None,
        now: float | None = None,
    ) -> None:
        """登记一个 worker 并写首次心跳。"""
        raise TransportError(f"{type(self).__name__} 不支持 worker 注册表")

    def heartbeat_worker(
        self,
        worker_id: str,
        *,
        meta: Mapping[str, Any] | None = None,
        now: float | None = None,
    ) -> None:
        """刷新心跳（worker 每 `heartbeat_interval` 调一次）。"""
        raise TransportError(f"{type(self).__name__} 不支持 worker 注册表")

    def deregister_worker(self, worker_id: str) -> None:
        """优雅退出时注销（幂等）。"""
        raise TransportError(f"{type(self).__name__} 不支持 worker 注册表")

    def list_workers(
        self, *, stale_after: float = 60.0, now: float | None = None
    ) -> list[WorkerInfo]:
        """列出已知 worker（含心跳时间；不支持时返回空列表）。"""
        return []

    # ------------------------------------------------------------------ 生命周期
    def close(self) -> None:
        """释放资源（幂等）。"""
        return None

    def __enter__(self) -> Transport:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()
