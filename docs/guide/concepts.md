# 核心概念

## 术语

| 词 | 含义 |
|---|---|
| **任务（task）** | 一段可被投递执行的代码，用 `@app.task` / `Task` 子类声明 |
| **队列（queue）** | 逻辑通道；worker 显式订阅若干个队列 |
| **消息 / 投递（delivery）** | 一次具体的「把这条任务交给某个 worker」 |
| **envelope** | 消息里被序列化的东西：任务名、参数、优先级、attempt、`eta`、`expires_at`、幂等键 |
| **job** | 任务的持久状态记录（`job:<id>`）：`QUEUED` / `RUNNING` / `SUCCEEDED` … |
| **租约（lease）** | 「这条消息归我，`lease` 秒内别人别动」；到期后自动回收重投 |
| **让位（yield）** | 把**还没开始执行**的预留交回队列，让更高优先级先跑 |

## 一条消息的一生

```text
enqueue ──► [ready] ──reserve──► [reserved + lease] ──► ack ──► 结束
                ▲                     │
                │                     ├─ nack(requeue) / defer / yield ──► [delayed] ──► 回 ready
                │                     ├─ 租约到期（进程崩了）──► 回收重投（deliveries+1）
                └── replay ── [dlq] ◄──┴─ 超过 max_deliveries / reject / dead_letter
```

- **at-least-once**：只有 `ack` 之后才算成功；worker 崩溃 → 租约到期 → 重投 → 任务可能被执行两次。
  所以任务应当**幂等**，或者用[幂等键](#幂等键)去重。
- **不丢**：消息先落 transport 再返回；worker 拿在手里没 ack 也不算丢（租约会把它捡回来）。
- **不重复**：同一时刻只有一条投递持有租约，不会两个 worker 同时跑同一条消息（除非租约过期）。

## 优先级

数值越大越优先，合法范围 `-9..9`（越界直接 `ConfigError`，**不 clamp**）：

```python
from taskmq import Priority

Priority.LOW       # -5
Priority.NORMAL    #  0
Priority.HIGH      #  5
Priority.CRITICAL  #  9
```

取件规则（也就是「方案 D」）：

1. **跨档严格优先**：所有队头里优先级最高的那个档位先出；
2. **档内平级加权轮询**：同一档位内按 `served/weight` 取（`QueueConfig.weight`），平手按队列名；
3. **同级 FIFO**：同队列同优先级按入队顺序；
4. **不打断正在执行的**：抢占只发生在「还没开始执行」的预留上（让位）。

```python
app = App(Config(
    transport="sqlite:///./taskmq.db",
    queues={"email": QueueConfig(weight=3), "sms": QueueConfig(weight=1)},
    max_yields=100,          # 让位上限（超过就不再让位，避免饿死）
))
```

## ack / nack / defer / yield

| 调用 | 语义 | 消耗 `deliveries`？ | 典型场景 |
|---|---|---|---|
| `ack` | 成功完成，出队 | — | 正常路径 |
| `nack(requeue=True, delay=)` | 失败重投（延迟可见） | 是 | 可重试的失败 |
| `nack(requeue=False)` / `dead_letter` | 直接进 DLQ | — | 明确不可重试 |
| `defer(delay=)` | **还没开始**就放回去 | **否** | 限流、`concurrency_key` 冲突 |
| `yield_reservation()` | 让位给更高优先级 | **否** | 插队（G2） |
| `extend_lease` | 续租 | — | 长任务心跳 |

`defer` / `yield` 不消耗投递次数，所以不会被毒丸保护误送进 DLQ。

## 失败与 DLQ

- 任务抛异常 → worker 按 `Retry` 策略决定重试或失败；重试也用 `deliveries` 计数；
- `max_deliveries`（默认 5）是**毒丸保护**：同一条消息投递次数超限直接进 DLQ，
  避免一条坏消息把 worker 卡死；
- DLQ 是每个队列一个列表：`taskmq dlq list -Q email` / `dlq replay --all`；
- 代码里：`transport.dead_letters()` / `transport.replay_dead(id)`。

## 幂等键

```python
send_email.apply_async(("a@b.com", "hi"), key="welcome:a@b.com")
```

同一个 `key` 在 `idempotency_ttl`（默认 1 天）内只会入队一次，返回同一个 job id——
重复提交（用户连点、上游重试）不会产生第二条消息。

## 可见性：`eta` / `delay` / `expires_at`

```python
send_email.apply_async(("a@b.com", "hi"), delay=60)              # 60 秒后才可见
send_email.apply_async(("a@b.com", "hi"), eta=time.time() + 60)   # 绝对时间
send_email.apply_async(("a@b.com", "hi"), expires_at=time.time() + 3600)  # 过期就不执行了
```

过期的消息在 promote/claim 时被判定为 `EXPIRED`，不会执行；对应 job 状态也会变成 `EXPIRED`。

## 下一步

- [定义任务](tasks.md) ｜ [运行 worker](workers.md) ｜ [选择 transport](transports.md)
