# 定义任务

## 三种写法（能力等价）

=== "函数式（最常用）"

    ```python
    from taskmq import App, Config, Retry

    app = App(Config(transport="memory://"))

    @app.task(queue="email", retry=Retry(max_attempts=5, backoff="exp"))
    def send_email(to: str, subject: str) -> str:
        return smtp_send(to, subject)
    ```

=== "类式（要继承 / 要进程级资源）"

    ```python
    from taskmq import Task

    class EmailTask(Task):
        queue = "email"
        retry_policy = Retry(max_attempts=5, backoff="exp")   # 注意策略叫 retry_policy

        def __init__(self, app):        # 每进程一次：连接池、客户端都放这里
            super().__init__(app)
            self.client = SmtpClient()

        def before_start(self, ctx):
            self.client.acquire()

        def run(self, to: str, subject: str) -> str:
            self.request.update_meta(stage="sending")         # 进度上报（写进 job.meta）
            return self.client.send(to, subject)

        def on_failure(self, ctx, exc):
            alert(f"{ctx.id} failed: {exc!r}")

        def after_return(self, ctx, state, result=None, exc=None):
            self.client.release()

    app.register(EmailTask)
    ```

=== "bind=True（函数式也能拿到实例）"

    ```python
    @app.task(bind=True, queue="email", retry=Retry(max_attempts=3))
    def send_email(self, to: str, subject: str) -> str:
        self.request.log.info("sending", attempt=self.request.attempt)
        if rate_limited():
            raise self.retry(countdown=30, reason="rate limited")
        return smtp_send(to, subject)
    ```

约定：

- `self` 是**每进程一个**的实例，只放进程级资源；请求级状态放 `self.request`（就是 `ctx`）；
- 钩子顺序：`before_start → run → on_success / on_retry / on_failure → after_return`；
- 钩子异常**不改投递语义**（只记 `hook_error`）；例外是 `before_start`，它抛异常即任务失败。

## 任务级选项

写在装饰器参数里，或写成 `Task` 子类的类属性（装饰器参数优先）：

| 选项 | 默认 | 说明 |
|---|---|---|
| `name` | 模块.函数名 | 任务名（跨进程/跨版本必须稳定） |
| `queue` | `Config.default_queue` | 投到哪个队列（worker 必须订阅它） |
| `priority` | `Config.default_priority` | `-9..9`，越大越优先 |
| `retry` / `retry_policy` | 无 | `Retry(...)` 策略，见下 |
| `timeout` | 无 | **软超时**：入队时算 deadline，超时抛 `TaskTimeout`（协作式） |
| `hard_timeout` | 无 | **硬超时**：只有 `processes` 池能强杀子进程 |
| `rate_limit` | 无 | `"100/m"` / `"10/s"` / `"1000/h"`，worker 内令牌桶，超限 `defer` |
| `concurrency_key` | 无 | 模板（如 `"user:{user}"`），同 key 在集群内串行 |
| `expires` | 无 | 秒；超过这个时间还没开始执行就作废（`EXPIRED`） |
| `ack` | `on_success` | `on_receipt`（收到即 ack）/ `on_success` / `on_completion` |
| `max_deliveries` | 5（或 `Retry.max_attempts`） | 毒丸保护：投递次数上限，超过进 DLQ |
| `store_result` | `True` | 是否把返回值写进 job 记录；`False` 时 `handle.get()` 返回 `None` |

> **返回值存在哪**：没有独立结果后端时，返回值就写在 transport 的 job 记录里（sqlite/PG/Redis 的那张 job 表），
> **没有大小上限**——几 MB 的返回值会直接把库撑大。大结果或敏感结果用 `@app.task(store_result=False)` 关掉，
> 需要结果就自己落到对象存储/数据库。
>
> `Config(result=..., result_ttl=...)`（独立结果后端）目前**只做校验、尚未实现**，配置它会打一条告警；
> 别把它当成"结果不占队列库"的手段。

## 提交选项

```python
send_email.delay("a@b.com", "hi")                       # 只传任务参数

send_email.apply_async(                                  # 带投递选项
    ("a@b.com", "hi"),
    queue="email",
    priority=Priority.HIGH,
    delay=30,                     # 相对延迟（秒）：30 秒后才可见
    eta=None,                     # 绝对可见时间用 datetime；给数字则按相对秒数（等同 delay）
    expires=3600,                 # 秒：1 小时内没开始执行就作废（EXPIRED）
    key="welcome:a@b.com",        # 幂等键（窗口内只入队一次，且返回同一个 job id）
    timeout=120,                  # 覆盖任务的软超时
)
```

- `delay()` 只接任务参数（有静态类型检查，IDE 能补全）；
- `apply_async()` 才接队列/优先级/可见性等投递选项；
- 两者都返回 `TaskHandle`：`.id`、`.state`、`.info`、`.successful()`、`.wait(timeout)`、
  `.get(timeout)`（失败抛 `RemoteError`，带远端 traceback 文本）、`.forget()`。

## 重试策略

```python
from taskmq import Retry

Retry(
    max_attempts=5,        # 总尝试次数
    backoff="exp",         # "fixed" | "linear" | "exp"
    base=1.0,              # 基准延迟（秒）
    factor=2.0,            # exp/linear 的倍数
    max_delay=600.0,       # 延迟上限
    jitter=True,           # ±50% 抖动，避免惊群
    retry_on=(Exception,), # 只有这些异常才重试
)
```

- 重试由 worker 侧执行：失败后按退避把消息放回 `delayed`，`deliveries` 继续累加；
- 想**不重试**直接进 DLQ：抛 `Reject("原因")`；想显式重试：`raise ctx.retry(reason=..., delay=...)`；
- 重试次数用尽 → job 变 `FAILED`，消息进 DLQ。

## 限流与按 key 串行

```python
@app.task(queue="email", rate_limit="100/m")
def send_email(to: str) -> str: ...

@app.task(queue="report", concurrency_key="user:{user}")
def build_report(user: str) -> str: ...

build_report.delay(user="u1")     # 模板用关键字参数渲染；位置参数写成 "user:{}"
```

两者都在**还没真正执行**时用 `transport.defer()` 放回队列，**不消耗 `deliveries`**，
所以不会被毒丸保护误送 DLQ。`concurrency_key` 用 transport 的**命名租约**做跨 worker 互斥，租约到期自动释放
（需要 transport 支持 `supports_leases`；不支持的后端会在启动时报错）。

## 进度上报与日志

```python
@app.task(queue="etl")
def extract(path: str) -> int:
    ctx = current_task()                      # 或 bind=True 的 self.request
    ctx.update_meta(stage="reading", rows=0)  # 写进 job.meta，status/info 可见
    ctx.log.info("reading", path=path)        # 结构化日志（带 job/task/attempt 字段）
    ...
```

`handle.info` 能看到 `meta`；`taskmq status` / 工作流状态页也读同一份数据。

## 下一步

- [运行 worker](workers.md)：这些选项在 worker 侧是怎么生效的
- [核心概念](concepts.md)：重试与租约的交互
