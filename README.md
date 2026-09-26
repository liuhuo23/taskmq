# taskmq

零外部服务就能跑起来、投递语义可预测、配置显式、调试不用猜的 Python 分布式任务队列。

- 不需要 Redis / RabbitMQ / 外部数据库：`memory://`（Phase 0 已实现）、`sqlite://`（Phase 0 下一步）
- 默认 **at-least-once**（成功才 ack）+ 可见性租约 + DLQ
- **优先级插队**：全局严格优先 + **未开始预留让位**（`yields` 独立计数）+ 平级队列轮询
- 默认执行池 **threads**，CPU 密集显式切 `processes`
- 默认序列化 **msgspec**，`serializer="json"` 可退回纯标准库
- 最低 Python **3.10**（在 3.10.21 上真机验证）

## 开发环境（uv）

工具链统一走 [uv](https://docs.astral.sh/uv/)（>= 0.12）。`.python-version` 固定 **3.10**（最低支持版本）。

```bash
uv sync                 # 安装依赖（含 dev group：pytest / ruff / mypy）
uv run pytest           # 跑测试
uv run ruff check taskmq tests
uv run mypy             # 类型检查（CI 口径）
uv run pyright          # 类型检查（Pylance / 编辑器口径）
make check              # = lint + typecheck(mypy + pyright) + test
```

说明：本仓库的 `uv.toml` 把 uv 的 cache 放在工程内（`.uv-cache/`），`Makefile` 通过
`UV_PYTHON_INSTALL_DIR` 把托管 Python 也放在工程内（`.uv-python/`），因此在 HOME 不可写的
受限沙箱里也能直接用；普通开发机不受影响。新增依赖用 `uv add <pkg>`，不要手改 virtualenv。

## 当前状态（Phase 0 进行中）

已实现：

| 模块 | 内容 |
|---|---|
| `taskmq/protocol.py` | Envelope v1、ULID、msgspec/json 编解码、自定义类型注册、大小限制 |
| `taskmq/transport/base.py` | Transport 抽象 + 不变量 + 插队原语（`peek_max_priority` / `yield_reservation` / `next_visible_at`） |
| `taskmq/transport/memory.py` | 内存 transport：优先级定档 + 档内加权轮询、租约重投、DLQ、幂等键、让位 |
| `taskmq/transport/sqlite.py` | **SQLite transport**：WAL + `BEGIN IMMEDIATE` 原子 claim、让位、DLQ、幂等键、结果/meta 持久化 |
| `taskmq/cli.py` | CLI：`worker` / `status --by-priority` / `dlq list\|replay` / `call`（入口 `taskmq`） |
| `taskmq/app.py` `task.py` | `App` / `@task` / `TaskHandle` / `TaskContext` / `Retry`、优先级解析、eager；**可继承的 `Task` 基类 + `bind=True` + 生命周期钩子** |
| `taskmq/worker/` | `solo` / `threads` / **`asyncio`** / **`processes`** 池、reserve–start 耦合、让位（G2）、不打断运行中（G3）；限流 / `concurrency_key` 串行；`hard_timeout` 可强杀 |
| `taskmq/ratelimit.py` | `"100/m"` token bucket（worker 内），超限走 `defer`（不消耗投递次数） |
| `taskmq/events.py` | 结构化事件：`Config.events="stdout"`、`EventSink` 协议、`CollectingSink`（测试） |
| `taskmq/testing.py` | `worker_for` / `run_until_idle` / `eager_app` |

> 类型检查跑**两个引擎**：`mypy`（CI 口径）与 `pyright`（Pylance/编辑器口径）。
> 两者对 `Any` 的推断规则不同，只跑一个会出现「本地绿、编辑器红」（见 `TaskHandle.wait` 的 `float | None` 案例）。

**Phase 0 验收已达成**（`tests/test_acceptance.py`）：1000 任务 × 2 worker 无重复执行、worker 进程被 `os._exit(9)`
真杀掉后租约回收重投、超限进 DLQ 并重放。下一步 Phase 1：`processes`/`asyncio` 池、Redis transport、beat、结构化事件。

## CLI

```bash
export TASKMQ_APP=myapp.tasks:app        # 或 --app myapp.tasks:app
uv run taskmq worker -Q email,default -c 8
uv run taskmq worker --once             # 跑空即退出（CI/调试）
uv run taskmq status --by-priority
uv run taskmq dlq list -Q email
uv run taskmq dlq replay --all -Q email --priority 0
uv run taskmq call myapp.tasks.send_email --args '["a@b.com","hi"]'
```

## 快速开始

```python
from taskmq import App, Config, Priority, Retry
from taskmq.testing import run_until_idle

app = App(Config(transport="memory://", concurrency=8))

@app.task(queue="email", retry=Retry(max_attempts=5, backoff="exp"), priority=Priority.NORMAL)
def send_email(to: str, subject: str) -> str:
    return f"sent:{to}"

normal = send_email.delay("a@b.com", "hi")
urgent = send_email.apply_async(("vip@b.com", "now"), priority=Priority.CRITICAL)

run_until_idle(app, queues=["email"], timeout=10)   # worker 订阅的队列要显式指定

assert normal.get(timeout=5) == "sent:a@b.com"
assert urgent.get(timeout=5) == "sent:vip@b.com"
```

插队语义（决策 §20-9）在 `tests/test_worker_priority.py` 里有三个可执行验收：
**G1** 空闲槽位一定给当前可见的最高优先级；**G2** 未开始的低优预留必须让位；
**G3** 正在执行的任务绝不打断。

## 执行池与硬超时

```python
Config(pool="threads")     # 默认：IO 密集
Config(pool="asyncio")     # async def 任务（单事件循环 + 线程槽位）
Config(pool="processes")   # CPU 密集；需要 --app module:attr 或 TASKMQ_APP（子进程重建 App）

@app.task(hard_timeout=30)   # 在 processes 池里：超时直接 kill 子进程，再按策略重试/进 DLQ
def crunch(data: list[int]) -> int: ...
```

- `async def` 任务跑在非 `asyncio` 池 → **启动即 `ConfigError`**（不做隐式 `asyncio.run()`）
- `hard_timeout` 在 `threads`/`solo`/`asyncio` 池只告警（无法强杀）；被 kill 的任务不会跑 `on_failure`/`after_return`

## 限流与按 key 串行

```python
@app.task(queue="email", rate_limit="100/m")            # worker 内限速，超限自动推后（不算重试）
def send_email(to: str) -> str: ...

@app.task(queue="report", concurrency_key="user:{user}")  # 同 user 在集群内串行
def build_report(user: str) -> str: ...
```

两者都在「还没真正执行」时用 `transport.defer()` 放回队列，**不消耗 `deliveries`**，
因此不会被毒丸保护误送 DLQ；`concurrency_key` 用 transport 命名租约（SQLite 走 `leases` 表）跨
worker 互斥，租约到期自动释放。

## 任务定义的三种写法（能力等价）

```python
from taskmq import App, Config, Retry, Task

app = App(Config(transport="memory://"))

# 1) 类式：可继承、可 mixin、可覆写生命周期钩子
class EmailTask(Task):
    queue = "email"
    retry_policy = Retry(max_attempts=5, backoff="exp")   # 策略叫 retry_policy，retry 留给 self.retry()

    def __init__(self, app):        # 每进程一次：进程级资源（连接池、客户端）
        super().__init__(app)
        self.client = SmtpClient()

    def before_start(self, ctx): self.client.acquire()
    def run(self, to: str, subject: str) -> str:
        self.request.update_meta(stage="sending")          # 进度上报
        return self.client.send(to, subject)
    def on_failure(self, ctx, exc): alert(f"{ctx.id} failed: {exc!r}")
    def after_return(self, ctx, state, result=None, exc=None): self.client.release()

app.register(EmailTask)

# 2) 函数式（糖）
@app.task(queue="email")
def send_email(to: str, subject: str) -> str: ...

# 3) bind=True：函数式也能拿到任务实例（Celery 手感）
@app.task(bind=True, queue="email", retry=Retry(max_attempts=3))
def send_email_bound(self, to: str, subject: str) -> str:
    self.request.log.info("sending", attempt=self.request.attempt)
    if rate_limited():
        raise self.retry(countdown=30, reason="rate limited")
    return smtp_send(to, subject)
```

约定（决策见 [tasks.md](docs/design/tasks.md)）：

- `self` 是**每进程一个**的实例，只放进程级资源；请求级状态放 `self.request`（= `ctx`）。
- 钩子顺序：`before_start → run → on_success / on_retry / on_failure → after_return(finally)`。
- 钩子异常**不改投递语义**（只记 `hook_error`）；例外是 `before_start`，它抛异常即任务失败。
- `delay()` 只收任务参数（有静态类型检查）；`queue/priority/eta/key/...` 走 `apply_async()`。

## 决策

| # | 决策 | 结论 |
|---|---|---|
| 1 | 包名 | `taskmq` |
| 2 | 最低 Python | 3.10+ |
| 3 | 默认池 | `threads` |
| 4 | 投递语义 | at-least-once（成功才 ack） |
| 5 | 序列化 | msgspec（`json` 可切） |
| 6 | SQLite 跨主机 | 同机 + 共享盘；跨主机走 Redis/PG |
| 7 | 工作流 | 🟡 暂定 DAG 放 Phase 2 |
| 8 | Celery 兼容层 | Phase 2 可选包 `taskmq-celery` |
| 9 | 优先级 | **方案 D**：全局严格优先 + 让位 + 平级队列轮询（P1–P19 见 [priority.md](docs/design/priority.md)） |

完整设计：[docs/design.md](docs/design.md) 与分册 [docs/design/](docs/design/)。
