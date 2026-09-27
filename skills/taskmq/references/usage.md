# taskmq 完整用法

> 入口是 `SKILL.md`；这里是细节。所有代码片段都按仓库当前版本（0.1.x）验证过。

## 1. 安装

```bash
pip install taskmq-py                      # PyPI（分发名 taskmq-py，import 仍是 taskmq）
pip install "taskmq-py[postgres,amqp,otel]" # 需要哪个后端带哪个 extra
pip install -e ".[dev]"                     # 从源码开发（仓库内）
```

- 运行时依赖只有 `msgspec`（`serializer="json"` 可退回纯标准库）；
- Python **3.9+**（仓库开发与类型检查按 3.10）；
- 命令：`taskmq ...` 或 `python -m taskmq ...`（等价）。

## 2. App 与 Config

```python
from taskmq import App, Config, QueueConfig

app = App(Config(
    transport="sqlite:///./taskmq.db",
    default_queue="default",
    concurrency=8,                 # 同时执行的任务数
    pool="threads",                # solo | threads | processes | asyncio
    prefetch=None,                 # 默认 = concurrency（reserve–start 耦合，别乱调大）
    lease=60.0,                    # 可见性租约（秒），worker 自动续租
    heartbeat_interval=10.0,       # 心跳 + 续租周期（实际取 min(它, lease/3)）
    shutdown_timeout=30.0,         # 优雅退出等待上限
    serializer="msgspec",          # msgspec | json
    max_message_bytes=256 * 1024,
    events="stdout",               # stdout | null | otel | 插件注册的 sink
    queues={"email": QueueConfig(weight=3), "sms": QueueConfig(weight=1)},
    default_priority=0,
    max_deliveries=5,              # 毒丸保护：投递次数上限（超了进 DLQ）
    idempotency_ttl=86400.0,       # 幂等键有效期
    eager=False,                   # True：delay() 当场同步执行（纯逻辑单测）
))
```

其它可调项：`yield_enabled` / `yield_delay` / `max_yields`（让位）、
`normal_reserved_slots`（给高优先级留槽）、`retry_priority`（`"keep"`|`"lower"`）、
`poll_interval` / `max_poll_interval`（空队列时的轮询退避）、`timezone`、`log_format`、
`result` / `result_ttl`（独立结果后端，配了就必须给 TTL）。

配置可以显式从环境变量来（一对一映射，不做通配魔法）：`Config.from_env()` 读 `TASKMQ_TRANSPORT` /
`TASKMQ_CONCURRENCY` / `TASKMQ_LEASE` …

## 3. 定义任务

三种写法能力等价，选顺手的：

```python
# ① 函数式（最常用）
@app.task(queue="email", retry=Retry(max_attempts=5, backoff="exp"))
def send_email(to: str, subject: str) -> str: ...

# ② 类式：要继承 / mixin / 进程级资源
class EmailTask(Task):
    queue = "email"
    retry_policy = Retry(max_attempts=5)      # 注意：策略叫 retry_policy，retry 留给 self.retry()
    def __init__(self, app):                  # 每进程一次
        super().__init__(app); self.client = SmtpClient()
    def before_start(self, ctx): self.client.acquire()
    def run(self, to: str, subject: str) -> str:
        self.request.update_meta(stage="sending")
        return self.client.send(to, subject)
    def on_failure(self, ctx, exc): alert(f"{ctx.id} failed: {exc!r}")
    def after_return(self, ctx, state, result=None, exc=None): self.client.release()
app.register(EmailTask)

# ③ bind=True：函数式也能拿到任务实例
@app.task(bind=True, retry=Retry(max_attempts=3))
def bound(self, to: str) -> str:
    if busy(): raise self.retry(countdown=30, reason="busy")
    return send(to)
```

任务选项（装饰器参数或 `Task` 子类属性，装饰器优先）：

| 选项 | 说明 |
|---|---|
| `name` | 任务名（默认 模块.函数名；跨进程/跨版本必须稳定） |
| `queue` | 投到哪个队列（worker 必须订阅它） |
| `priority` | `-9..9`，越大越优先 |
| `retry` / `retry_policy` | `Retry(max_attempts, backoff="fixed|linear|exp", base, factor, max_delay, jitter, retry_on)` |
| `timeout` | **软超时**（入队时算 deadline，超时抛 `TaskTimeout`） |
| `hard_timeout` | **硬超时**，只有 `processes` 池能强杀 |
| `rate_limit` | `"100/m"` / `"10/s"` / `"1000/h"`，worker 内令牌桶 |
| `concurrency_key` | 模板如 `"user:{user}"`，同 key 跨 worker 串行（需要命名租约） |
| `expires` | 秒；超过仍未开始执行就作废（`EXPIRED`） |
| `ack` | `on_receipt` / `on_success`（默认）/ `on_completion` |
| `max_deliveries` | 覆盖全局毒丸上限 |

钩子顺序：`before_start → run → on_success / on_retry / on_failure → after_return`。
钩子异常只记录（不改投递语义）；例外是 `before_start`，它抛异常即任务失败。
`self` 是每进程一个的实例（放连接池），请求级状态放 `self.request`（= `ctx`）。

## 4. 投递

```python
send_email.delay("a@b.com", "hi")            # 只传任务参数（有静态类型检查）

send_email.apply_async(
    ("a@b.com", "hi"),
    queue="email",                           # 覆盖队列
    priority=Priority.HIGH,                  # -9..9
    delay=30,                                # 相对延迟（秒）：30 秒后才可见
    eta=None,                                # 绝对时间用 datetime；给数字则按**相对秒数**（等同 delay）
    expires=3600,                            # 秒：没在期限内开始执行就作废
    key="welcome:a@b.com",                   # 幂等键（同 key 在 idempotency_ttl 内只入队一次）
    timeout=120,                             # 覆盖软超时
)
```

返回 `TaskHandle`：`.id` / `.state` / `.info` / `.ready()` / `.successful()` /
`.failed()` / `.wait(timeout)` / `.get(timeout)`（失败抛 `RemoteError`，带远端 traceback 文本）/ `.forget()`。

## 5. 语义细节

- **重试**：worker 侧执行，按退避放回 `delayed`，`deliveries` 累加；超过 `max_deliveries` 进 DLQ。
  想直接进 DLQ：`raise Reject("原因")`；想显式重试：`raise self.retry(countdown=..., reason=...)`。
- **defer**（`ctx.defer()` / transport 层）：把**还没开始执行**的预留放回，不消耗投递次数。
- **让位**：有更高优先级可见消息且当前预留未开始执行时，worker 会自动 `yield_reservation()`；正在执行的不打断。
- **幂等键**：`key=` 在 `idempotency_ttl`（默认 1 天）内只入队一次，返回同一个 job id。
- **过期**：`expires_at` 到点仍未开始 → 消息与 job 都变 `EXPIRED`，不执行。
- **异常语义**：`LeaseLost` = 租约已被回收（迟到 ack），丢弃本地结果、别重试 ack；`MessageNotFound` = 消息不存在（通常是被重放/清理过）。

## 6. 选择 transport

| URL | 适用 | 说明 |
|---|---|---|
| `memory://` | 单测 / eager | 进程内，跨进程不共享 |
| `sqlite:///./taskmq.db` | 单机多进程 / 共享盘 | WAL + `BEGIN IMMEDIATE` 原子 claim |
| `redis://host:6379/1?prefix=app:` | 多机高吞吐 | 零依赖 RESP2 客户端 + Lua 原子取件；`&lua=off` 强制走 WATCH/MULTI |
| `redis://host:7380/0?prefix=app:&cluster=1` | Redis Cluster | **db 只能 0**；键按队列打 hash tag；跨队列取件降级（`status` 会声明） |
| `postgresql://user:pass@host:5432/db?prefix=app_` | 多机强一致 | `FOR UPDATE SKIP LOCKED`；需要 `psycopg` |
| `amqp://user:pass@host:5672/%2F?state=sqlite:///./state.db` | 已有 RabbitMQ | `state=` 侧车**必填**（job 状态/租约/worker 表/DAG 索引存在那里） |

内建后端都支持 `supports_leases / supports_workers / supports_job_listing`；每个后端在 `limitations` 里
如实声明降级（如 AMQP/Cluster 的 `global_priority`）。`taskmq status` 会打印 `LIMITATIONS`，检查能力前先看它。

## 7. 运行 worker

```bash
export TASKMQ_APP=myapp.tasks:app
taskmq worker -Q email,default -c 8      # 多队列、并发 8
taskmq worker -Q email --once            # 跑空即退出（CI / 批处理）
```

```python
from taskmq import Worker

with Worker(app, queues=["email"], concurrency=8) as worker:
    worker.run_until_idle(timeout=30)          # 跑空即退出（测试/批处理）

# 常驻循环就是 CLI 干的事：poll() 返回本轮启动数
while running:
    if worker.poll() == 0 and not worker.wait_for_slot(timeout=app.config.poll_interval):
        time.sleep(app.config.poll_interval)   # 只有真闲着才睡
```

- **池**：`threads`（IO 默认）/ `processes`（CPU，hard_timeout 可强杀，需要 `module:attr` 才能重建 App）/ `asyncio`（`async def` 任务）/ `solo`（测试）；
- **prefetch = concurrency**（默认）保证「不会有预留了却排不到队」的公平性，除非明确要测让位否则别调大；
- worker 会周期性续租 + `reap_expired_leases()`，并（有工作流时）做补偿推进；
- 退出：先停领新任务，等在跑的收尾，最长等 `shutdown_timeout`；
- 多机部署：worker 无状态，多起几个进程即可；任务必须幂等；
- **取件节奏**：有活连续取、槽位占满等任意任务完成、真没活才睡 —— 所以吞吐**不**等于 `concurrency / poll_interval`。
  想量自己的场景：`make bench`（`python scripts/bench.py`，可换后端/规模/payload），
  边界与规模正确性：`make stress`。参考量级：memory ~5k/s、sqlite ~2.7k/s、redis（Lua）~0.8k/s（2000 条、并发 8）。

## 8. DAG 工作流

```python
from taskmq.workflow import WorkflowBuilder

@app.workflow("etl")
def etl(wf: WorkflowBuilder, source: str):
    extract = wf.step("extract", extract_task, args=(source,))
    clean   = wf.step("clean", clean_task, deps={"rows": extract})   # 上游结果按参数名注入
    stats   = wf.step("stats", stats_task, deps={"rows": clean})
    return wf.join("report", report_task, deps=[clean, stats], collect="tables")

handle = app.submit_workflow("etl", {"source": "s3://bucket/2026-03-08"})
handle.status()            # 每节点 state/attempt/job_id + 总体状态
handle.get(timeout=60)     # 等汇节点结果
```

- `wf.step(name, task, args=, kwargs=, deps=, bind=, collect=, queue=, priority=, on_failure="fail"|"continue")`；
- 节点就是普通任务（自己的重试/DLQ/超时/队列）；解析顺序 `step > 任务自带 > 运行级 > Config`；
- 幂等推进（job id = `{run}::{node}`）、崩溃后 worker 自动补偿推进，也可 `app.resume_workflow(run_id)`；
- CLI：`taskmq workflow list|status <run>|resume <run>`。

## 9. 定时调度（beat）

```python
from taskmq.schedule import cron, every

app.schedule(
    cron("send_report", "0 9 * * *", tz="Asia/Shanghai"),   # 标准 5 段 cron，zoneinfo 时区
    every("cleanup", minutes=5, misfire="run_once"),         # 错过补一次
)
```

```bash
taskmq --app myapp.tasks:app beat            # 常驻；多副本靠 __beat__ 租约选主
taskmq --app myapp.tasks:app beat --once     # 只推进一轮
taskmq --app myapp.tasks:app dev             # 本地：worker + beat 同进程
```

`misfire`：`"skip"`（默认）/ `"run_once"`；状态文件 `taskmq.beat.json`（`--state` 可改），只有 leader 写；首次部署只记基准不补跑。

## 10. CLI 速查

```bash
taskmq --app myapp.tasks:app worker -Q email -c 8 [--once]
taskmq --app myapp.tasks:app status [--by-priority]
taskmq --app myapp.tasks:app dlq list -Q email
taskmq --app myapp.tasks:app dlq replay --all -Q email --priority 0
taskmq --app myapp.tasks:app beat [--once] [--state path]
taskmq --app myapp.tasks:app workflow list|status <run>|resume <run>
taskmq --app myapp.tasks:app call myapp.tasks.send_email --args '["a@b.com","hi"]'
taskmq --version
```

`status` 输出里：`QUEUES`（pending = ready+delayed，inflight = 持有租约，dead = DLQ 长度）、
`PRIORITY`（`--by-priority`）、`WORKERS`（心跳年龄）、`LIMITATIONS`（后端声明的降级）。

## 11. 测试工具（`taskmq.testing`）

```python
from taskmq.testing import run_until_idle, worker_for, eager_app, transport_conformance

run_until_idle(app, queues=["email"], timeout=10)      # 同进程把队列跑空（超时抛 TimeoutError）
with worker_for(app, queues=["email"], prefetch=4) as w:   # 断点调试友好
    w.poll()                                                # 单步：领一次 + 启动到并发上限

eager_app(transport="memory://")                        # Config(eager=True)：delay() 当场执行

# 自定义 transport 后端自证语义（16 个场景，按 limitations 自动跳过）
def test_my_backend():
    transport_conformance(lambda: MyTransport(...), supports={"leases": True, "workers": False})
```

`run_until_idle` 的语义是「等到没有未完成的消息」——**还没到可见时间的延迟消息也算未完成**，
所以队列里如果有 `delay=3600` 这种消息，它会一直等到 `timeout` 然后抛 `TimeoutError`。
测试时把长延迟消息放**另一个队列**，或改用 `worker.poll()` 单步推进。``

## 12. 排查

| 症状 | 先查 |
|---|---|
| 任务不执行 | worker 的 `-Q` 与任务队列是否一致（`status` 看 pending 在哪个队列）；`memory://` 是否跨进程用了 |
| 任务重复执行 | at-least-once 的正常现象：worker 崩溃/租约过期会重投 → 任务加幂等（`key=` 或业务去重） |
| 一直卡在某条消息 | 该队列 `dead` 是否有毒丸；`max_deliveries` 与 `Retry` 策略 |
| `LeaseLost` 频繁 | 任务跑得比 `lease` 久（worker 会续租，但机器假死会误判）→ 调大 `lease` / 让任务更快返回 |
| 限流任务反复重试 | 用 `rate_limit`（超限走 defer，不消耗投递次数），不要自己 `retry` |
| 优先级没生效 | `status --by-priority` 看档位；跨队列严格优先只在同一 worker 可见范围内成立（Cluster/AMQP 有声明降级） |
| 想让某类任务绝不并发 | `concurrency_key`（需要后端支持命名租约） |
| CPU 打满/卡死 | 换 `pool="processes"` + `hard_timeout` |

事件与 tracing：`Config(events="stdout")`（一行一个 JSON：`task.submitted/started/succeeded/retrying/failed/deferred`、
`worker.started/stopped`、`beat.fired/standby/error`、`workflow.started/advanced/succeeded`）；
OTel 用 `Config(events="otel")`（需 `taskmq-py[otel]`）或 `app.add_sink(OtelEventSink(tracer))`。
