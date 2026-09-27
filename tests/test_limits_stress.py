"""上限与压测：边界值、规模下的正确性、吞吐回归。

规模都压在秒级，所以默认 `make test` 也会跑（CI 同样跑）；
单独压这一组：`make stress`（= `uv run pytest -m stress`）。
压吞吐/延迟分位用 `python scripts/bench.py`（可换后端、可调积压规模）。
"""
from __future__ import annotations

import threading
import time

import pytest

from taskmq import App, Config, Envelope, MessageTooLarge, Priority, QueueConfig, Reject, Retry
from taskmq.errors import ConfigError
from taskmq.testing import run_until_idle
from taskmq.transport.memory import MemoryTransport

pytestmark = pytest.mark.stress


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# ------------------------------------------------------------------ 边界值
def test_message_size_limit_is_enforced_at_the_boundary():
    """`max_message_bytes` 是硬边界：刚好塞得进的能入队，超一点就 MessageTooLarge。"""
    app = App(Config(transport="memory://", events="null", max_message_bytes=4096))

    @app.task(queue="big")
    def take(blob: str) -> int:
        return len(blob)

    handle = take.delay("x" * 3000)                 # 编码后仍在上限内
    assert handle.id
    with pytest.raises(MessageTooLarge):
        take.delay("x" * 100_000)                   # 远超上限
    app.close()


def test_max_attempts_boundary_then_single_dead_letter():
    """重试上限：正好尝试 `max_attempts` 次，之后进 DLQ，且只有一条死信。

    两个计数不是一回事：`attempt` 是任务级（跨重试累加），`deliveries` 是**消息级**
    （每次重试都是新消息，从 1 重新数）——所以死信上的 deliveries 是 1，不是 3。
    """
    app = App(Config(transport="memory://", events="null", concurrency=1, max_deliveries=3))
    attempts: list[int] = []

    @app.task(queue="poison", retry=Retry(max_attempts=3, backoff="fixed", base=0.0))
    def boom(i: int) -> int:
        attempts.append(i)
        raise RuntimeError("boom")

    handle = boom.delay(1)
    run_until_idle(app, queues=["poison"], timeout=30)

    assert len(attempts) == 3, f"应该正好尝试 3 次，实际 {len(attempts)}"
    assert handle.state == "FAILED"
    record = app.transport.get_state(handle.id)
    assert record is not None and record.attempt == 3, "任务级 attempt 应该累加到 3"
    dead = app.transport.dead_letters(queue="poison")
    assert len(dead) == 1 and dead[0].deliveries == 1
    app.close()


def test_message_deliveries_counter_grows_on_lease_loss():
    """毒丸保护的依据：同一条消息反复租约超时，`deliveries` 必须逐次累加。"""
    clock = FakeClock()
    transport = MemoryTransport(clock=clock)
    transport.enqueue(Envelope(task="t"), queue="q")

    seen: list[int] = []
    for _ in range(3):
        granted = transport.reserve(["q"], worker_id="w", lease=5, limit=1)
        assert len(granted) == 1
        seen.append(granted[0].deliveries)
        clock.advance(6)
        assert transport.reap_expired_leases() == 1
    assert seen == [1, 2, 3]
    transport.close()


def test_priority_extremes_are_ordered_strictly_across_bands():
    """19 个档位（-9..9）全部用上：跨档严格优先，档内 FIFO。"""
    transport = MemoryTransport()
    total = 0
    for priority in range(-9, 10):
        for seq in range(40):
            transport.enqueue(
                Envelope(task="t", args=(priority, seq), priority=priority), queue="q"
            )
            total += 1

    granted = transport.reserve(["q"], worker_id="w", lease=30, limit=total)
    assert len(granted) == total
    bands = [d.priority for d in granted]
    assert bands == sorted(bands, reverse=True), "跨档必须严格递减"
    for priority in range(-9, 10):
        seqs = [d.envelope.args[1] for d in granted if d.priority == priority]
        assert seqs == sorted(seqs), f"档内应 FIFO：{priority}"
    transport.close()

    # 越界优先级在**提交侧**就报错（不 clamp）：-9..9 是硬区间
    app = App(Config(transport="memory://", events="null"))

    @app.task(queue="q")
    def noop() -> None: ...

    for out_of_range in (10, -10, 100):
        with pytest.raises(ConfigError):
            noop.apply_async(priority=out_of_range)
    app.close()


# ------------------------------------------------------------------ 规模下的正确性
def test_deep_queue_priority_jump_and_exactly_once():
    """深队列（3000 条低优先级）里插一条最高优先级：第一条就得是它，且一条不丢不重。"""
    count = 3000
    app = App(Config(transport="memory://", events="null", concurrency=8))
    seen: list[int] = []

    @app.task(queue="deep")
    def tick(i: int) -> int:
        seen.append(i)
        return i

    for i in range(count):
        tick.apply_async((i,), priority=Priority.LOW)
    vip = count
    tick.apply_async((vip,), priority=Priority.CRITICAL)

    run_until_idle(app, queues=["deep"], timeout=180)
    assert seen[0] == vip, "最高优先级应该插到队首"
    assert sorted(seen) == list(range(count + 1)), "有丢件或重复"
    app.close()


def test_concurrent_producers_do_not_lose_or_duplicate(tmp_path):
    """4 个线程并发投递 + 8 并发消费：每条任务恰好执行一次。"""
    app = App(Config(transport=f"sqlite:///{tmp_path / 'load.db'}", events="null", concurrency=8))
    executed: dict[int, int] = {}
    lock = threading.Lock()

    @app.task(queue="u")
    def once(i: int) -> int:
        with lock:
            executed[i] = executed.get(i, 0) + 1
        return i

    def produce(base: int) -> None:
        for k in range(250):
            once.delay(base + k)

    threads = [threading.Thread(target=produce, args=(base,)) for base in (0, 250, 500, 750)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    run_until_idle(app, queues=["u"], timeout=180)
    assert len(executed) == 1000, f"只跑了 {len(executed)} 条"
    assert set(executed.values()) == {1}, "有任务被执行了多次"
    app.close()


def test_lease_recovery_at_scale_requeues_exactly_once():
    """500 条租约同时过期：全部回收，重新领取时不丢不重。"""
    clock = FakeClock()
    transport = MemoryTransport(clock=clock)
    for i in range(500):
        transport.enqueue(Envelope(task="t", args=(i,)), queue="q")

    first = transport.reserve(["q"], worker_id="w1", lease=5, limit=500)
    assert len(first) == 500
    assert transport.reserve(["q"], worker_id="w2", lease=5, limit=500) == []

    clock.advance(6)
    assert transport.reap_expired_leases() == 500

    again = transport.reserve(["q"], worker_id="w2", lease=5, limit=500)
    assert {d.message_id for d in again} == {d.message_id for d in first}
    assert all(d.deliveries == 2 for d in again)          # 重投算第二次投递
    transport.close()


def test_idempotency_keys_at_scale_dedupe():
    """1000 个幂等键各提交两次：只入队 1000 条，且返回同一个 job id。"""
    app = App(Config(transport="memory://", events="null"))

    @app.task(queue="i")
    def t(i: int) -> int:
        return i

    first = {i: t.apply_async((i,), key=f"k{i}") for i in range(1000)}
    second = {i: t.apply_async((i,), key=f"k{i}") for i in range(1000)}

    assert all(second[i].id == first[i].id for i in range(1000))
    stats = app.transport.queue_stats(["i"])[0]
    assert stats.pending == 1000, f"幂等键没去重：pending={stats.pending}"
    app.close()


def test_many_queues_all_drained_and_interleaved():
    """20 个队列 × 100 条：全部消费到，且不是"一个队列跑完再跑下一个"。"""
    queue_count, per_queue = 20, 100
    app = App(Config(
        transport="memory://",
        events="null",
        concurrency=4,
        queues={f"q{i}": QueueConfig(weight=(i % 3) + 1) for i in range(queue_count)},
    ))
    order: list[str] = []
    lock = threading.Lock()

    @app.task(queue="q0", name="many.mark")
    def mark(queue: str) -> str:
        with lock:
            order.append(queue)
        return queue

    for i in range(queue_count):
        for _ in range(per_queue):
            mark.apply_async((f"q{i}",), queue=f"q{i}")

    run_until_idle(app, queues=[f"q{i}" for i in range(queue_count)], timeout=180)
    assert len(order) == queue_count * per_queue
    for i in range(queue_count):
        assert order.count(f"q{i}") == per_queue, f"q{i} 条数不对"
    assert len(set(order[:queue_count])) > 1, "前 20 条只来自一个队列 → 没有轮询"
    app.close()


def test_dlq_at_scale_keeps_every_message():
    """200 条毒丸：全部进 DLQ，message_id 不重复。"""
    app = App(Config(transport="memory://", events="null", concurrency=4, max_deliveries=1))

    @app.task(queue="d")
    def bad(i: int) -> int:
        raise Reject("nope")

    for i in range(200):
        bad.delay(i)
    run_until_idle(app, queues=["d"], timeout=120)

    dead = app.transport.dead_letters(queue="d")
    assert len(dead) == 200
    assert len({d.message_id for d in dead}) == 200
    assert app.transport.queue_stats(["d"])[0].dead == 200
    app.close()


def test_queue_stats_on_deep_queue_is_not_slow():
    """深队列统计不该是秒级（曾经的 O(n²) 隐患护栏）。"""
    transport = MemoryTransport()
    for _ in range(5000):
        transport.enqueue(Envelope(task="t"), queue="q")
    started = time.perf_counter()
    stats = transport.queue_stats(["q"])[0]
    elapsed = time.perf_counter() - started
    assert stats.pending == 5000
    assert elapsed < 2.0, f"单次 queue_stats 用了 {elapsed:.2f}s"
    transport.close()


# ------------------------------------------------------------------ 吞吐回归
def test_throughput_is_not_capped_by_poll_interval():
    """回归：worker 主循环曾每轮 poll 后无条件 sleep(poll_interval)。

    那样吞吐上限就是 `concurrency / poll_interval`：c=8、0.05s → 约 160 条/秒，
    2400 条要 15 秒；修复后是几千条/秒（实测 ~0.5s）。这里留 12 倍余量，
    CI 慢机器也稳，但旧行为（15s+）必然失败。
    """
    count = 2400
    app = App(Config(transport="memory://", events="null", concurrency=8, poll_interval=0.05))

    @app.task(queue="t")
    def t(i: int) -> int:
        return i

    for i in range(count):
        t.delay(i)

    started = time.perf_counter()
    run_until_idle(app, queues=["t"], timeout=120)
    elapsed = time.perf_counter() - started
    assert elapsed < 6.0, f"{count} 条用了 {elapsed:.1f}s（被 poll_interval 卡住的旧行为约需 15s）"
    app.close()


def test_saturated_pool_keeps_making_progress():
    """并发槽位占满时也必须持续推进（旧实现在 poll_interval=0 时会被自旋饿死）。"""
    count = 500
    app = App(Config(transport="memory://", events="null", concurrency=4, poll_interval=0.0))
    done: list[int] = []

    @app.task(queue="sat")
    def slow(i: int) -> int:
        time.sleep(0.001)                     # 让槽位真的被占满
        done.append(i)
        return i

    for i in range(count):
        slow.delay(i)

    started = time.perf_counter()
    run_until_idle(app, queues=["sat"], timeout=60)
    elapsed = time.perf_counter() - started
    assert len(done) == count
    assert elapsed < 10.0, f"槽位占满时推进太慢：{elapsed:.1f}s"
    app.close()
