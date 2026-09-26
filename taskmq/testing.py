"""测试与本地开发辅助（docs/design.md §16、插件契约 docs/design/plugins.md §6）。"""
from __future__ import annotations

import contextlib
import dataclasses
import logging
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any

from .app import App
from .config import Config
from .errors import LeaseLost, TransportError
from .protocol import Envelope
from .transport.base import JobState, Transport
from .worker.pool import Pool
from .worker.runner import Worker

__all__ = [
    "worker_for",
    "run_until_idle",
    "eager_app",
    "transport_conformance",
    "CONFORMANCE_SCENARIOS",
]

logger = logging.getLogger("taskmq.testing")

_CONF_QUEUE = "conformance"
_CONF_ALT_QUEUE = "conformance-alt"

#: 一致性套件覆盖的场景名（可用 `scenarios=` 只跑其中一部分）
CONFORMANCE_SCENARIOS: tuple[str, ...] = (
    "priority_fifo",
    "global_priority",
    "atomic_claim",
    "ack_semantics",
    "lease_recovery",
    "defer",
    "yield",
    "expires",
    "idempotency_key",
    "dlq_replay",
    "job_state",
    "visibility",
    "named_leases",
    "worker_registry",
    "job_listing",
    "capability_honesty",
)


@contextlib.contextmanager
def worker_for(
    app: App,
    *,
    queues: Sequence[str] | None = None,
    concurrency: int | None = None,
    prefetch: int | None = None,
    worker_id: str | None = None,
    pool: Pool | None = None,
) -> Iterator[Worker]:
    """在**当前进程内**跑完整链路（transport → pool → ack），可断点调试。

    默认 `prefetch == concurrency`：不会有「预留了但没开始」的消息，即 reserve–start 耦合（P16）。
    要测让位（G2）就显式把 `prefetch` 调大。
    """
    worker = Worker(
        app,
        queues=queues,
        concurrency=concurrency,
        prefetch=prefetch,
        worker_id=worker_id,
        pool=pool,
    )
    try:
        yield worker
    finally:
        worker.close()


def run_until_idle(
    app: App,
    *,
    queues: Sequence[str] | None = None,
    concurrency: int | None = None,
    prefetch: int | None = None,
    timeout: float = 30.0,
) -> int:
    """一次性把队列跑空的语法糖。"""
    with worker_for(
        app, queues=queues, concurrency=concurrency, prefetch=prefetch
    ) as worker:
        return worker.run_until_idle(timeout=timeout)


def eager_app(**config_kwargs: Any) -> App:
    """`eager=True` 的 App：`delay()` 同步执行，用于纯逻辑单测。"""
    config_kwargs.setdefault("eager", True)
    return App(Config(**config_kwargs))


# =============================================================== 一致性套件
def transport_conformance(
    factory: Callable[[], Transport],
    *,
    supports: Mapping[str, bool] | None = None,
    cleanup: Callable[[Transport], None] | None = None,
    scenarios: Sequence[str] | None = None,
    now: Callable[[], float] | None = None,
    advance: Callable[[float], None] | None = None,
) -> list[str]:
    """对任意 `Transport` 实现跑一遍契约（第三方后端的质量闸门）。

    用法::

        def test_my_backend():
            transport_conformance(
                lambda: MyTransport(...),
                supports={"leases": True, "workers": False},
                cleanup=lambda t: t.close(),
            )

    约定与语义：

    - `factory()` 每次都返回一个**全新**的 transport（第三方后端请用唯一前缀/独立库，避免互相干扰）；
    - `cleanup(t)` 负责收尾（清键/关连接）；不给就用 `t.close()`；
    - 返回**实际执行**的场景名列表。被 `t.limitations` 声明降级的场景会自动跳过——
      也就是说「能力不对等」必须是**显式声明**出来的，而不是让测试静默失效；
    - 并发场景在**同一个实例**上跑多线程（Worker 的真实用法：reserve 在主线程、ack 在线程池里），
      因此 transport 必须线程安全；跨连接共享存储由 `factory` 自己决定；
    - `supports` 覆盖 transport 声明的能力（默认取 `t.supports_leases` / `t.supports_workers`）。
    """
    t = _Time(clock=now or time.time, advance=advance)
    selected = list(scenarios) if scenarios is not None else list(CONFORMANCE_SCENARIOS)
    unknown = [name for name in selected if name not in CONFORMANCE_SCENARIOS]
    if unknown:
        raise ValueError(f"未知的一致性场景：{unknown}；可选 {list(CONFORMANCE_SCENARIOS)}")

    executed: list[str] = []
    for name in selected:
        transport = factory()
        try:
            limitations = getattr(transport, "limitations", None) or {}
            if name in limitations:
                logger.info("一致性场景 %s 被 transport 声明降级，跳过：%s", name, limitations[name])
                continue
            if name == "named_leases" and not _capability(transport, supports, "leases"):
                continue
            if name == "worker_registry" and not _capability(transport, supports, "workers"):
                continue
            if name == "job_listing" and not _capability(transport, supports, "job_listing"):
                continue
            try:
                _RUNNERS[name](transport, t)
            except AssertionError as exc:
                raise AssertionError(f"[transport_conformance/{name}] {exc}") from exc
            executed.append(name)
        finally:
            if cleanup is not None:
                cleanup(transport)
            else:
                transport.close()
    return executed


def _capability(transport: Transport, supports: Mapping[str, bool] | None, name: str) -> bool:
    if supports is not None and name in supports:
        return bool(supports[name])
    return bool(getattr(transport, f"supports_{name}", False))


@dataclasses.dataclass(frozen=True, slots=True)
class _Time:
    """套件内部的时间视图：真实时钟用 sleep，假时钟用 advance（都只推进"刚好过期"）。"""

    clock: Callable[[], float]
    advance: Callable[[float], None] | None = None

    def now(self) -> float:
        return float(self.clock())

    def expire(self, seconds: float) -> None:
        """把时间推进到租约过期之后。"""
        if self.advance is not None:
            self.advance(seconds + 0.01)
        else:
            time.sleep(min(0.05, seconds / 2 + 0.01))


def _check(condition: Any, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _enqueue(
    transport: Transport,
    task: str = "conf.task",
    *,
    priority: int = 0,
    queue: str = _CONF_QUEUE,
    delay: float = 0.0,
    **options: Any,
) -> str:
    """`delay` 走 transport，其余关键字进 Envelope（`eta`/`expires_at`/`key` …）。"""
    return transport.enqueue(
        Envelope(task=task, priority=priority, **options), queue=queue, delay=delay
    )


def _scenario_priority_fifo(transport: Transport, t: _Time) -> None:
    for task, priority in (("low1", 0), ("high", 5), ("mid", 1), ("low2", 0)):
        _enqueue(transport, task, priority=priority)
    granted = transport.reserve([_CONF_QUEUE], worker_id="conf", lease=30, limit=10)
    order = [d.envelope.task for d in granted]
    _check(order == ["high", "mid", "low1", "low2"], f"优先级/FIFO 顺序错误：{order}")


def _scenario_global_priority(transport: Transport, t: _Time) -> None:
    _enqueue(transport, "low", priority=0)
    _enqueue(transport, "high", priority=7, queue=_CONF_ALT_QUEUE)
    granted = transport.reserve([_CONF_QUEUE, _CONF_ALT_QUEUE], worker_id="conf", lease=30, limit=1)
    tasks = [d.envelope.task for d in granted]
    _check(tasks == ["high"], f"跨队列未按全局优先级取件：{tasks}")


def _scenario_atomic_claim(transport: Transport, t: _Time) -> None:
    total = 40
    for index in range(total):
        _enqueue(transport, f"t{index}")
    claimed: list[int] = []
    lock = threading.Lock()

    def drain(worker: str) -> None:
        while True:
            batch = transport.reserve([_CONF_QUEUE], worker_id=worker, lease=30, limit=1)
            if not batch:
                return
            with lock:
                claimed.append(batch[0].message_id)
            transport.ack(batch[0])

    threads = [threading.Thread(target=drain, args=(f"w{index}",)) for index in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    _check(len(claimed) == total, f"投递数量不对：{len(claimed)} != {total}")
    _check(len(set(claimed)) == total, "出现重复投递（claim 不是原子的）")


def _scenario_ack_semantics(transport: Transport, t: _Time) -> None:
    _enqueue(transport)
    delivery = transport.reserve([_CONF_QUEUE], worker_id="conf", lease=30, limit=1)[0]
    transport.ack(delivery)
    transport.ack(delivery)                                  # 幂等
    stats = transport.queue_stats([_CONF_QUEUE])[0]
    _check(stats.inflight == 0, f"ack 后 inflight 应为 0，实际 {stats.inflight}")


def _scenario_lease_recovery(transport: Transport, t: _Time) -> None:
    _enqueue(transport)
    first = transport.reserve([_CONF_QUEUE], worker_id="w1", lease=0.02, limit=1)[0]
    t.expire(0.02)                                   # 只推进到"刚过期"，避免消息在未来才可见
    transport.reap_expired_leases(now=t.now())
    second = transport.reserve([_CONF_QUEUE], worker_id="w2", lease=30, limit=1)
    _check(second, "租约过期后消息应重新可见")
    _check(second[0].deliveries == 2, f"重投后 deliveries 应为 2，实际 {second[0].deliveries}")
    try:
        transport.ack(first)
    except LeaseLost:
        pass
    else:
        _check(False, "租约已被回收，迟到 ack 应抛 LeaseLost")


def _scenario_defer(transport: Transport, t: _Time) -> None:
    _enqueue(transport)
    first = transport.reserve([_CONF_QUEUE], worker_id="conf", lease=30, limit=1)[0]
    transport.defer(first, delay=0.0)
    second = transport.reserve([_CONF_QUEUE], worker_id="conf", lease=30, limit=1)
    _check(second, "defer 后消息应立刻重新可见")
    _check(
        second[0].deliveries == first.deliveries,
        f"defer 不应消耗投递次数：{first.deliveries} -> {second[0].deliveries}",
    )


def _scenario_yield(transport: Transport, t: _Time) -> None:
    _enqueue(transport, "long")
    delivery = transport.reserve([_CONF_QUEUE], worker_id="conf", lease=30, limit=1)[0]
    _check(transport.yield_reservation(delivery, delay=0.0, max_yields=1) is True, "首次让位应成功")
    again = transport.reserve([_CONF_QUEUE], worker_id="conf", lease=30, limit=1)
    _check(again, "让位后消息应重新可见")
    _check(again[0].yields == 1, f"让位计数应为 1，实际 {again[0].yields}")
    _check(
        transport.yield_reservation(again[0], delay=0.0, max_yields=1) is False,
        "达到 max_yields 后不应再让位",
    )


def _scenario_expires(transport: Transport, t: _Time) -> None:
    job_id = _enqueue(transport, expires_at=t.now() - 1)
    _check(
        transport.reserve([_CONF_QUEUE], worker_id="conf", lease=30, limit=1) == [],
        "过期消息不应被投递",
    )
    transport.reap_expired_jobs()
    record = transport.get_state(job_id)
    _check(
        record is not None and record.state == "EXPIRED",
        f"过期 job 状态应为 EXPIRED，实际 {record and record.state}",
    )


def _scenario_idempotency_key(transport: Transport, t: _Time) -> None:
    first = transport.enqueue(Envelope(task="conf.task", key="conf-key"), queue=_CONF_QUEUE)
    second = transport.enqueue(Envelope(task="conf.task", key="conf-key"), queue=_CONF_QUEUE)
    _check(first == second, f"同幂等键应返回同一个 job id：{first} != {second}")
    stats = transport.queue_stats([_CONF_QUEUE])[0]
    _check(stats.pending == 1, f"同幂等键不应重复入队，pending={stats.pending}")


def _scenario_dlq_replay(transport: Transport, t: _Time) -> None:
    _enqueue(transport)
    delivery = transport.reserve([_CONF_QUEUE], worker_id="conf", lease=30, limit=1)[0]
    transport.dead_letter(delivery, "conformance")
    entries = [item for item in transport.dead_letters() if item.reason == "conformance"]
    _check(entries, "DLQ 里找不到刚投递的消息")
    _check(
        transport.queue_stats([_CONF_QUEUE])[0].dead >= 1,
        "queue_stats.dead 没有算上 DLQ",
    )
    _check(transport.replay_dead(entries[0].message_id) is True, "replay_dead 应返回 True")
    _check(
        not any(item.message_id == entries[0].message_id for item in transport.dead_letters()),
        "重放后不应还留在 DLQ",
    )


def _scenario_job_state(transport: Transport, t: _Time) -> None:
    transport.set_state("conf-job", "RUNNING", task="conf.task", attempt=1, stage="half")
    transport.set_state("conf-job", "SUCCEEDED", result={"n": 1}, percent=100)
    record = transport.get_state("conf-job")
    if record is None:
        raise AssertionError("get_state 读不到刚写的 job")
    _check(record.result == {"n": 1}, f"result 往返不一致：{record.result}")
    _check(
        record.meta.get("stage") == "half" and record.meta.get("percent") == 100,
        f"meta 必须合并而不是覆盖：{record.meta}",
    )
    _check(transport.get_state("conf-missing") is None, "不存在的 job 应返回 None")


def _scenario_visibility(transport: Transport, t: _Time) -> None:
    _enqueue(transport, delay=3600)
    _check(
        transport.reserve([_CONF_QUEUE], worker_id="conf", lease=30, limit=1) == [],
        "延迟消息不应立即可见",
    )
    visible_at = transport.next_visible_at([_CONF_QUEUE])
    _check(
        visible_at is None or visible_at >= t.now() - 1,
        f"next_visible_at 不该是过去时间：{visible_at}",
    )
    _check(transport.peek_max_priority([_CONF_QUEUE]) is None, "不可见的消息不该被 peek 到")


def _scenario_named_leases(transport: Transport, t: _Time) -> None:
    _check(transport.acquire_lease("conf-lock", "w1", 30.0) is True, "首次抢租约应成功")
    _check(transport.acquire_lease("conf-lock", "w2", 30.0) is False, "别人持有时应抢不到")
    _check(transport.renew_lease("conf-lock", "w2", 30.0) is False, "非持有者续租应失败")
    transport.release_lease("conf-lock", "w1")
    _check(transport.acquire_lease("conf-lock", "w2", 30.0) is True, "释放后应能被别人抢到")


def _scenario_worker_registry(transport: Transport, t: _Time) -> None:
    transport.register_worker("conf-w", queues=(_CONF_QUEUE,), pool="threads", concurrency=2)
    workers = transport.list_workers()
    _check(any(item.worker_id == "conf-w" for item in workers), "注册后 list_workers 里找不到")
    transport.heartbeat_worker("conf-w")
    transport.deregister_worker("conf-w")
    _check(transport.list_workers() == [], "注销后 worker 列表应为空")


def _scenario_job_listing(transport: Transport, t: _Time) -> None:
    """枚举 job（DAG 补偿推进依赖；不支持的后端在提交工作流时会被拒绝）。"""
    transport.set_state("conf-job-a", JobState.SUCCEEDED, task="conf.task", result=1)
    transport.set_state("conf-job-b", JobState.RUNNING, task="conf.task")
    listed = {record.job_id for record in transport.list_jobs()}
    _check({"conf-job-a", "conf-job-b"} <= listed, f"list_jobs 少了刚写的 job：{sorted(listed)}")
    running = [record.job_id for record in transport.list_jobs(states=[JobState.RUNNING])]
    _check(running == ["conf-job-b"], f"按状态过滤不对：{running}")
    prefixed = [record.job_id for record in transport.list_jobs(prefix="conf-job-a")]
    _check(prefixed == ["conf-job-a"], f"按前缀过滤不对：{prefixed}")
    limited = transport.list_jobs(limit=1)
    _check(len(limited) == 1, f"limit 没生效：{len(limited)}")
    newest = transport.list_jobs()[0].job_id
    _check(newest == "conf-job-b", f"应按 updated_at 倒序（新的在前）：{newest}")


def _scenario_capability_honesty(transport: Transport, t: _Time) -> None:
    lease_calls = {
        "acquire_lease": lambda: transport.acquire_lease("conf-honest", "o", 1.0),
        "release_lease": lambda: transport.release_lease("conf-honest", "o"),
        "renew_lease": lambda: transport.renew_lease("conf-honest", "o", 1.0),
    }
    worker_calls = {
        "register_worker": lambda: transport.register_worker("conf-honest"),
        "heartbeat_worker": lambda: transport.heartbeat_worker("conf-honest"),
        "deregister_worker": lambda: transport.deregister_worker("conf-honest"),
    }
    if not transport.supports_leases:
        _assert_unsupported(lease_calls, "命名租约")
    if not transport.supports_workers:
        _assert_unsupported(worker_calls, "worker 注册表")


def _assert_unsupported(calls: Mapping[str, Callable[[], Any]], label: str) -> None:
    for name, call in calls.items():
        try:
            call()
        except TransportError:
            continue
        except Exception as exc:  # noqa: BLE001 - 必须是 TransportError 才算诚实
            _check(False, f"声明不支持{label}时 {name} 应抛 TransportError，实际 {type(exc).__name__}")
        else:
            _check(False, f"声明不支持{label}，但 {name} 静默成功了（谎报能力）")


_RUNNERS: dict[str, Callable[[Transport, _Time], None]] = {
    "priority_fifo": _scenario_priority_fifo,
    "global_priority": _scenario_global_priority,
    "atomic_claim": _scenario_atomic_claim,
    "ack_semantics": _scenario_ack_semantics,
    "lease_recovery": _scenario_lease_recovery,
    "defer": _scenario_defer,
    "yield": _scenario_yield,
    "expires": _scenario_expires,
    "idempotency_key": _scenario_idempotency_key,
    "dlq_replay": _scenario_dlq_replay,
    "job_state": _scenario_job_state,
    "visibility": _scenario_visibility,
    "named_leases": _scenario_named_leases,
    "worker_registry": _scenario_worker_registry,
    "job_listing": _scenario_job_listing,
    "capability_honesty": _scenario_capability_honesty,
}
