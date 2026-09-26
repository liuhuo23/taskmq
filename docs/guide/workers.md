# 运行 worker

worker 的职责只有三件：**reserve（领消息）→ 执行 → ack/nack**，顺带做租约续期、过期回收和工作流补偿推进。

## 三种跑法

=== "生产：独立进程（CLI）"

    ```bash
    export TASKMQ_APP=myapp.tasks:app
    taskmq worker -Q email,default -c 8        # 多队列、并发 8
    taskmq worker -Q email --once              # 跑到队列空就退出（CI / 批处理）
    ```

=== "嵌进自己的程序"

    ```python
    from taskmq import Worker

    with Worker(app, queues=["email"], concurrency=8) as worker:
        worker.run_until_idle(timeout=30)     # 或 worker.run_forever()
    ```

=== "测试 / 调试：同进程跑空"

    ```python
    from taskmq.testing import run_until_idle, worker_for

    run_until_idle(app, queues=["email"], timeout=10)     # 一次性跑空

    with worker_for(app, queues=["email"], prefetch=4) as worker:   # 断点调试友好
        worker.run_once()
    ```

## 池（执行模型）

```python
Config(pool="threads")     # 默认：IO 密集（网络、DB）
Config(pool="processes")   # CPU 密集；hard_timeout 能真杀
Config(pool="asyncio")     # async def 任务：单事件循环 + 线程槽位
Config(pool="solo")        # 同线程顺序执行（测试用，无并发）
```

- `async def` 任务跑在非 `asyncio` 池 → **启动即 `ConfigError`**（不做隐式 `asyncio.run()`，避免事件循环惊喜）；
- `processes` 池会**重建 `App`**：所以 worker 必须能通过 `--app module:attr` / `TASKMQ_APP` 找到它；
- `hard_timeout` 只在 `processes` 池能强杀；其他池里只告警。被 kill 的任务不会跑 `on_failure` / `after_return`。

## 并发与预取

```python
Config(concurrency=8, prefetch=8)   # prefetch 默认 = concurrency
```

- `concurrency` = 同时执行的任务数；
- `prefetch` = 最多同时「预留但还没开始」的消息数。默认与 `concurrency` 相等，
  即 **reserve–start 耦合**：不会有消息被领了却排不到队（这直接影响优先级公平性）；
- 把它调大才能观察让位（G2）：预留多了，低优先级才有机会被让位掉。

## 租约、心跳与回收

```python
Config(lease=60, heartbeat_interval=10, shutdown_timeout=30)
```

- worker 每隔 `min(heartbeat_interval, lease/3)` 给在跑的消息**续租**；
- 同时定期 `reap_expired_leases()`：进程崩了、机器断电，租约到期后消息自动回到队列（`deliveries+1`）；
- worker 注册心跳进 transport（`supports_workers` 为真时），`taskmq status` 的 `WORKERS` 段能看到
  `queues / pool / concurrency / heartbeat` 年龄；
- 退出时优雅收尾：先停止领新任务，等在跑的收尾，最长等 `shutdown_timeout`；超时则记 `task.lost_lease` 之类事件后退出。

```bash
taskmq status            # 队列深度 / 优先级分布 / WORKERS / LIMITATIONS
```

## 限流、串行、让位在 worker 侧怎么发生

| 机制 | 触发时机 | 动作 |
|---|---|---|
| `rate_limit` | 令牌桶没额度（**还没执行**） | `defer()` 放回，不消耗投递次数 |
| `concurrency_key` | 抢不到命名租约（**还没执行**） | `defer()` 放回，租约到期自动释放 |
| 优先级插队 | 有更高优先级的可见消息，且当前预留**还没开始** | `yield_reservation()` 让位 |
| 正在执行 | 任何情况 | **不打断**（G3） |

## 部署形态建议

- **多机**：`redis://` 或 `postgresql://`，worker 无状态，想扩就多起几个进程；
- **单机**：`sqlite://` 足够（WAL + `BEGIN IMMEDIATE`），多进程安全；
- **同一台机器多进程**：每个进程一个 worker 即可，不要共享 App 实例（线程池不是进程安全的执行体）；
- **任务必须幂等**：at-least-once 下崩溃/租约过期会重投，用幂等键或业务侧去重兜住。

## 调优旋钮

| 场景 | 调整 |
|---|---|
| 空队列时 CPU 空转 | 调大 `poll_interval` / `max_poll_interval`（默认 0.05 / 0.5，指数退避） |
| 任务很长（> lease） | 调大 `lease@@（worker 会自动续租，但机器假死时回收会变慢） |
| 机器假死要快速重投 | 调小 `lease@@；同时保证任务幂等 |
| 优先级公平性被破坏 | 保持 `prefetch == concurrency`（默认），别为了吞吐调大 |
| CPU 密集 | `pool="processes"` + `hard_timeout` |

## 下一步

- [定时调度（beat）](scheduling.md) ｜ [CLI 与运维](cli.md) ｜ [核心概念](concepts.md)
