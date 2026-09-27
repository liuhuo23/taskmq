# 配方（复制就能用）

> 全部基于 `taskmq-py 0.1.x`；入口见 `SKILL.md`，语义细节见 `usage.md`。

## 1. 幂等提交（用户连点 / 上游重试）

```python
send_email.apply_async(("a@b.com", "hi"), key="welcome:a@b.com")
```

同 key 在 `idempotency_ttl`（默认 1 天）内只入队一次，返回同一个 job id。
**注意**：幂等键去的是「重复提交」，不是「重复执行」——执行侧仍然要幂等。

## 2. 延迟一次 / 定时一次性

```python
send_email.apply_async(("a@b.com", "hi"), delay=3600)      # 相对延迟：1 小时后可见
send_email.apply_async(("a@b.com", "hi"), eta=when_due)    # 绝对时间：when_due 必须是 datetime
send_email.apply_async(("a@b.com", "hi"), expires=7200)    # 秒：2 小时内没开始执行就作废
```

周期任务不要用 delay 循环，用 [beat](#9-定时调度用-beat)。

## 3. 重试与退避

```python
from taskmq import Reject, Retry

@app.task(retry=Retry(max_attempts=5, backoff="exp", base=1.0, factor=2.0, max_delay=600, jitter=True,
                      retry_on=(ConnectionError, TimeoutError)))
def fetch(url: str) -> str: ...

@app.task(bind=True)
def sometimes(self, arg):
    if permanently_broken(arg):
        raise Reject("坏数据，别重试")          # 直接进 DLQ
    if temporarily_down():
        raise self.retry(countdown=30, reason="上游抖动")   # 显式重试
```

## 4. 限流（超限不算重试）

```python
@app.task(queue="email", rate_limit="100/m")    # 100/s 用 "100/s"，每小时用 "1000/h"
def send_email(to: str) -> str: ...
```

worker 侧令牌桶没额度时会 `defer` 放回，**不消耗 `deliveries`**，所以不会意外进 DLQ。

## 5. 按 key 串行（同一用户/同一资源全局互斥）

```python
@app.task(queue="report", concurrency_key="user:{user}")
def build_report(user: str) -> str: ...
```

用 transport 的命名租约实现跨 worker 互斥，抢不到就 defer；租约到期自动释放。
需要后端支持 `supports_leases`（内建后端都支持；不支持的后端启动即报错）。

## 6. 优先级插队 + 平级公平

```python
from taskmq import App, Config, Priority, QueueConfig

app = App(Config(
    transport="redis://127.0.0.1:6379/1?prefix=myapp:",
    queues={"email": QueueConfig(weight=3), "sms": QueueConfig(weight=1)},
))

@app.task(queue="email")           # 默认优先级
def bulk(to: str) -> str: ...

@app.task(queue="email", priority=Priority.CRITICAL)
def vip(to: str) -> str: ...
```

规则：跨档严格优先 → 档内按 `served/weight` 轮询 → 同级 FIFO；插队只发生在未开始执行的预留上。

## 7. 长任务（别被误判为死掉）

```python
@app.task(timeout=1800, queue="etl")        # 软超时（协作式，超时抛 TaskTimeout）
def long_job(path: str) -> int:
    ctx = current_task()
    for i, chunk in enumerate(chunks(path)):
        process(chunk)
        ctx.update_meta(progress=f"{i}/{n}")   # 进度写进 job.meta（handle.info 可见）
    return n
```

worker 会自动按 `min(heartbeat_interval, lease/3)` 续租；但**机器假死**仍会在 `lease` 后被回收重投 ——
所以长任务要么幂等，要么把 `Config(lease=...)` 调大。

## 8. CPU 密集 + 硬超时

```python
app = App(Config(transport="redis://127.0.0.1:6379/1?prefix=app:", pool="processes", concurrency=4))

@app.task(hard_timeout=30)
def crunch(data: list[int]) -> int: ...
```

```bash
# processes 池要在子进程里重建 App，所以必须能按 module:attr 导入
TASKMQ_APP=myapp.tasks:app taskmq worker -Q cpu -c 4
```

被强杀的任务不会跑 `on_failure` / `after_return`。

## 9. DLQ：查看与重放

```bash
taskmq --app myapp.tasks:app dlq list -Q email
taskmq --app myapp.tasks:app dlq replay --all -Q email --priority 0
```

```python
dead = app.transport.dead_letters(queue="email")     # DeadLetter(message_id, job_id, task, reason, deliveries, ...)
app.transport.replay_dead(dead[0].message_id, priority=0)   # attempt 归 1，重新入队
```

## 10. 优雅停机

```python
import signal
import time
from taskmq import Worker

with Worker(app, queues=["email"], concurrency=8) as worker:
    stop = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))   # 或 Ctrl-C 走 KeyboardInterrupt
    while not stop["flag"]:
        worker.poll()
        time.sleep(app.config.poll_interval)
# close() 会先停领新任务，等在跑的收尾（最长 Config.shutdown_timeout）
```

CLI 的 `taskmq worker` 已经把 SIGTERM/SIGINT 接好了，容器里正常 `docker stop` 即可。

## 11. Web 服务里提交任务（FastAPI 例子）

```python
from fastapi import FastAPI
from myapp.tasks import app as taskmq_app, send_email

api = FastAPI()

@api.post("/welcome")
def welcome(email: str):
    handle = send_email.apply_async((email, "hi"), key=f"welcome:{email}")
    return {"job_id": handle.id, "state": handle.state}   # 异步：状态来自 transport
```

```bash
# 任务执行在另一个进程（或另一台机器）
TASKMQ_APP=myapp.tasks:app taskmq worker -Q email -c 8
```

不要在同一进程里既 serve HTTP 又跑 worker（除非明确知道要共享 GIL/连接池）；用框架的 lifespan 起 Worker 时要保证退出时 close。

## 12. 单测模板

```python
import pytest
from taskmq import App, Config
from taskmq.testing import run_until_idle

@pytest.fixture
def app(tmp_path):
    app = App(Config(transport=f"sqlite:///{tmp_path}/t.db", events="null", concurrency=2))
    yield app
    app.close()

def test_add(app):
    @app.task(queue="q")
    def add(a, b):
        return a + b

    handle = add.delay(1, 2)
    run_until_idle(app, queues=["q"])
    assert handle.get(timeout=5) == 3
```

- `memory://` + `run_until_idle` 跑得快；要测「跨进程」「租约回收」就换 `sqlite://` / `redis://`；
- 纯逻辑单测可以用 `eager_app()`（`delay()` 当场同步执行，不经过队列）；
- 断言失败语义：`handle.get()` 失败抛 `RemoteError`；`handle.failed()` / `handle.state` 可查；
- 队列里有**长延迟**消息（`delay=3600`）时别用 `run_until_idle` 排它——它会等满 timeout；
  把延迟消息放另一个队列，或 `worker.poll()` 单步。

## 13. 上线检查单

- [ ] transport 选对了：跨机器用 `redis://` / `postgresql://`（不是 `memory://` / 共享盘 sqlite）；
- [ ] worker 的 `-Q` 覆盖了所有任务队列，且有进程守护/编排（k8s deployment / systemd）；
- [ ] 任务**幂等**（或提交带 `key=`）；
- [ ] `lease` > 最长任务耗时（或用 `hard_timeout` / 续租）；
- [ ] 失败有去处：`max_deliveries` + DLQ 有监控（`status` 的 `dead` / 事件 `task.failed`）；
- [ ] `status` 的 `LIMITATIONS` 看过了（例如 Cluster/AMQP 的全局优先级降级）；
- [ ] beat 只在一个 leader 上跑（`taskmq beat` 多副本即可，租约自动选主）；
- [ ] 序列化选型确认（`msgspec` 默认；跨语言/无依赖场景换 `json`）。
