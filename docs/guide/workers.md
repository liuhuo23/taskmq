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
    import time
    from taskmq import Worker

    with Worker(app, queues=["email"], concurrency=8) as worker:
        worker.run_until_idle(timeout=30)     # 跑空即退出（批处理 / 测试）

    # 常驻循环（CLI 做的就是这件事）：有活连续取，空转才按 poll_interval 让出 CPU
    while running:
        if worker.poll() == 0 and not worker.wait_for_slot(timeout=app.config.poll_interval):
            time.sleep(app.config.poll_interval)
    ```

=== "测试 / 调试：同进程跑空"

    ```python
    from taskmq.testing import run_until_idle, worker_for

    run_until_idle(app, queues=["email"], timeout=10)     # 一次性跑空

    with worker_for(app, queues=["email"], prefetch=4) as worker:   # 断点调试友好
        worker.poll()                                                 # 单步：领一次 + 启动到并发上限
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
| 任务很长（> lease） | 调大 `lease`（worker 会自动续租，但机器假死时回收会变慢） |
| 机器假死要快速重投 | 调小 `lease`；同时保证任务幂等 |
| 优先级公平性被破坏 | 保持 `prefetch == concurrency`（默认），别为了吞吐调大 |
| CPU 密集 | `pool="processes"` + `hard_timeout` |
| 吞吐上不去 | 先 `make bench` 定位是排队还是执行：换后端 / 调大 `concurrency` / 把 sqlite 放本地盘 |

## 吞吐与上限（实测）

取件节奏：**有活就连续取；槽位占满就等任意一个任务完成；真没活才按 `poll_interval` 睡**。
吞吐不该被 `poll_interval` 卡住 —— 早期实现每轮 `poll()` 后无条件 sleep，
上限恰好是 `concurrency / poll_interval`（并发 8、0.05s → 约 160 条/秒）；修好后同一配置从 117 条/秒提到约 2500 条/秒。

实测（M 系列 Mac，单进程，2000 条，并发 8；`make bench` 可复现）：

| 后端 | 入队 | 消费 | 说明 |
|---|---|---|---|
| `memory://` | ~65k/s | ~13k/s（500 条）/ ~5k/s（2000 条） | 进程内；**积压越大越慢**（每次取件全表扫描，10k 积压掉到 ~1.2k/s） |
| `sqlite://` | ~12k/s | ~2.7k/s | 每条一次提交，fsync 主导：本地盘 ~2.7k/s，外置/网络卷只有 ~0.8k/s |
| `redis://` | ~1.2k/s | ~0.8k/s | 每消息多趟往返、无流水线；关掉 Lua（`lua=off`）再慢约 2.5 倍 |
| `postgresql://` | 视网络 | 视网络 | `FOR UPDATE SKIP LOCKED`，适合已有 PG 的团队 |

几条硬边界：

- `max_message_bytes`（默认 256KB）是硬上限，超了直接 `MessageTooLarge`，不截断；
- 优先级只有 `-9..9`，越界在**提交侧**就 `ConfigError`（不 clamp）；
- `max_attempts` 是任务级（跨重试累加）；`deliveries` 是**消息级**计数，毒丸保护靠它（同一条消息反复崩就累加）；
- 深队列里优先级仍然严格：跨档严格优先、档内 FIFO，插队只发生在"还没开始执行"的预留上。

压自己的场景：

```bash
make bench                                    # 默认 memory:// 2000 条
make bench ARGS='-t "sqlite:///./b.db" -n 20000 -c 16 --payload 4096'
python scripts/bench.py -t "redis://127.0.0.1:6379/15?prefix=b:" -n 5000
make stress                                   # 边界/规模正确性用例（tests/test_limits_stress.py）
```

## 下一步

- [定时调度（beat）](scheduling.md) ｜ [CLI 与运维](cli.md) ｜ [核心概念](concepts.md)
