# taskmq

[![ci](https://github.com/liuhuo23/taskmq/actions/workflows/ci.yml/badge.svg)](https://github.com/liuhuo23/taskmq/actions/workflows/ci.yml)
[![pypi](https://img.shields.io/pypi/v/taskmq-py?label=pypi)](https://pypi.org/project/taskmq-py/)
[![python](https://img.shields.io/pypi/pyversions/taskmq-py)](https://pypi.org/project/taskmq-py/)
[![docs](https://github.com/liuhuo23/taskmq/actions/workflows/docs.yml/badge.svg)](https://liuhuo23.github.io/taskmq/)

📖 **使用文档：<https://liuhuo23.github.io/taskmq/>**（[安装与快速开始](https://liuhuo23.github.io/taskmq/guide/install/) ·
[核心概念](https://liuhuo23.github.io/taskmq/guide/concepts/) ·
[选择 transport](https://liuhuo23.github.io/taskmq/guide/transports/)）

零外部服务就能跑起来、投递语义可预测、配置显式、调试不用猜的 Python 分布式任务队列。

- 不需要 Redis / RabbitMQ / 外部数据库：`memory://`、`sqlite://`、`redis://`（都可用；redis 自带零依赖 RESP 客户端）
- 默认 **at-least-once**（成功才 ack）+ 可见性租约 + DLQ
- **优先级插队**：全局严格优先 + **未开始预留让位**（`yields` 独立计数）+ 平级队列轮询
- 默认执行池 **threads**，CPU 密集显式切 `processes`
- 默认序列化 **msgspec**，`serializer="json"` 可退回纯标准库
- 最低 Python **3.9**（CI 在 3.9 与 3.10 上各跑一遍全量测试；开发按 3.10）

## 安装（使用方）

**不需要任何特殊工具，更不需要 uv**：Python ≥ 3.9 + pip 就够。

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate

pip install taskmq-py                                  # PyPI（分发名 taskmq-py，import 仍是 taskmq）
pip install "taskmq-py[postgres,amqp,otel]"            # 需要哪个后端就带哪个 extra

# 也可以从源码 / Release 装：
#   pip install "taskmq-py @ git+https://github.com/liuhuo23/taskmq"
#   pip install https://github.com/liuhuo23/taskmq/releases/download/v0.1.0/taskmq_py-0.1.0-py3-none-any.whl

taskmq --version              # console script（装包后就有）
python -m taskmq --version    # 没进 PATH / 源码目录临时跑，等价
```

起 worker：

```bash
export TASKMQ_APP=myapp.tasks:app
taskmq worker -Q email,default -c 8
```

📖 完整使用文档：**<https://liuhuo23.github.io/taskmq/>**

## 开发环境

开发用 [uv](https://docs.astral.sh/uv/)（推荐）：解释器、虚拟环境、锁文件它一起管。

```bash
uv sync                     # 按 uv.lock 装依赖（dev group 含 pytest / ruff / mypy / pyright / …）
make test                   # = uv run pytest
make test-py39              # 在最低支持版本 3.9 上跑一遍（独立环境 .venv39，不动 .venv）
make check                  # = lint + typecheck(mypy + pyright) + test
```

**3.9 / 3.10 的分工**：`.python-version` 固定 **3.10**（日常开发与类型检查的语言级别），
3.9 作为**运行时下限**由 CI 用真 3.9 跑全量测试来守。想在代码里用 3.10 特性（`match`、`slots=True`、
运行时 `X | Y`…）没问题，但要么放进 `if sys.version_info >= (3, 10)` 分支，要么走
[`taskmq/_compat.py`](taskmq/_compat.py)（3.10+ 用原生、3.9 自动退化）——3.9 上跑一次测试就会告诉你有没有漏。

不想装 uv 也行（使用方/CI 的 pip 路径，Makefile 会自动回落）：

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
make test                   # 没有 uv 时用 .venv/bin/python -m pytest
```

说明：`Makefile` 默认用 `.venv/bin/python`，没有就用 `$PYTHON`（默认 `python3`），所以
`make test` / `make lint` / `make typecheck` 都不需要 uv。`uv.toml` 把 uv 的 cache 放在工程内
（`.uv-cache/`），走 uv 时 `UV_PYTHON_INSTALL_DIR` 把托管 Python 也放工程内（`.uv-python/`）——
只为在 HOME 不可写的受限沙箱里也能用，普通开发机不受影响。依赖声明有两处、必须一致：
`[dependency-groups].dev`（uv）与 `[project.optional-dependencies].dev`（pip），CI 两条路径都会跑。

## 当前状态（Phase 1–2 完成：306 个测试全绿，含真 Redis / Redis Cluster / PostgreSQL / RabbitMQ）

已实现：

| 模块 | 内容 |
|---|---|
| `taskmq/protocol.py` | Envelope v1、ULID、msgspec/json 编解码、自定义类型注册、大小限制 |
| `taskmq/transport/base.py` | Transport 抽象 + 不变量 + 插队原语（`peek_max_priority` / `yield_reservation` / `next_visible_at`） |
| `taskmq/transport/memory.py` | 内存 transport：优先级定档 + 档内加权轮询、租约重投、DLQ、幂等键、让位 |
| `taskmq/transport/sqlite.py` | **SQLite transport**：WAL + `BEGIN IMMEDIATE` 原子 claim、让位、DLQ、幂等键、结果/meta 持久化 |
| `taskmq/cli.py` | CLI：`worker` / `status --by-priority`（含 `WORKERS` 段）/ `dlq list\|replay` / `beat` / `dev` / `call`（入口 `taskmq`） |
| `taskmq/app.py` `task.py` | `App` / `@task` / `TaskHandle` / `TaskContext` / `Retry`、优先级解析、eager；**可继承的 `Task` 基类 + `bind=True` + 生命周期钩子** |
| `taskmq/worker/` | `solo` / `threads` / **`asyncio`** / **`processes`** 池、reserve–start 耦合、让位（G2）、不打断运行中（G3）；限流 / `concurrency_key` 串行；`hard_timeout` 可强杀 |
| `taskmq/ratelimit.py` | `"100/m"` token bucket（worker 内），超限走 `defer`（不消耗投递次数） |
| \`taskmq/events.py\` | 结构化事件：\`Config.events="stdout"|\"null\"|\"otel\"\`、\`EventSink\` 协议、\`CollectingSink\`（测试） |
| \`taskmq/otel.py\` | **OTel 适配器**（可选依赖 \`taskmq-py[otel]\`）：任务 span + 标准 messaging 语义约定 |
| \`taskmq/transport/redis.py\` \`redis_client.py\` | **Redis transport**：零依赖 RESP2 客户端 + Lua 原子操作；禁用 Lua 时自动回退 \`WATCH/MULTI/EXEC\`；\`?cluster=1\` 走 slot 路由（hash tag 分槽 + MOVED/ASK） |
| \`taskmq/transport/postgres.py\` | **PostgreSQL transport**：\`FOR UPDATE SKIP LOCKED\` 原子 claim（多机零重复、零阻塞）、表前缀隔离、一致性套件 16/16 |
| \`taskmq/schedule.py\` \`worker/beat.py\` | **beat**：cron/interval（zoneinfo、DST 覆盖）、\`__beat__\` 租约选主、misfire、JSON 状态文件 |
| `taskmq/transport/amqp.py` | **AMQP transport**：`x-max-priority` 排序 + TTL/DLX 延迟 + DLX 死信；状态走 `?state=` 侧车 |
| `taskmq/workflow.py` | **原生 DAG 工作流**：依赖声明在代码里、事件驱动推进（不轮询）、幂等补偿推进 |
| `taskmq/plugins.py` | 插件注册表：transport / codec / sink / pool 扩展点 + `taskmq.plugins` entry point 懒发现 |
| \`taskmq/worker/runner.py\` | worker 心跳注册表（memory/sqlite/redis 三家），\`status\` 可看 worker 列表与心跳年龄 |
| \`taskmq/testing.py\` | \`worker_for\` / \`run_until_idle\` / \`eager_app\` |

> 类型检查跑**两个引擎**：`mypy`（CI 口径）与 `pyright`（Pylance/编辑器口径）。
> 两者对 `Any` 的推断规则不同，只跑一个会出现「本地绿、编辑器红」（见 `TaskHandle.wait` 的 `float | None` 案例）。

**验收**：Phase 0（`tests/test_acceptance.py`）1000 任务 × 2 worker 无重复执行、worker 被 `os._exit(9)`
真杀掉后租约回收重投、超限进 DLQ 并重放；Phase 1 加上了池/硬超时、限流、`concurrency_key`、
beat 选主、Redis transport（Lua 与无 Lua 两种模式各跑一遍全部用例）、worker 心跳、OTel 适配。
Phase 2 已完成：**原生 DAG 工作流**、**PostgreSQL transport**、**`amqp://`**、**Redis 档内加权轮询与 Cluster（`?cluster=1`）**。下一步：Celery 兼容 shim（`taskmq-celery`）、独立 `result=` 后端。

## transport 选择

| URL | 适用 | 说明 |
|---|---|---|
| `memory://` | 单测 / eager | 进程内，零依赖 |
| `sqlite:///./taskmq.db` | 单机生产 / 共享盘 | WAL + `BEGIN IMMEDIATE` 原子 claim；同机多进程一等公民 |
| `redis://127.0.0.1:6379/1?prefix=app1` | 高吞吐 / 多机 | 零依赖 RESP 客户端 + Lua 原子操作；`prefix` 隔离键空间；`?cluster=1` 支持 Redis Cluster |
| `postgresql://user:pass@host:5432/db?prefix=app1_` | 多机 / 强一致 | `FOR UPDATE SKIP LOCKED` 原子 claim + `ON CONFLICT` CAS 租约；需要 `taskmq-py[postgres]` |
| `amqp://user:pass@host:5672/vhost?state=sqlite:///./taskmq.db` | 已有 RabbitMQ / 需要路由 | `x-max-priority` 排序 + TTL/DLX 延迟 + DLX 死信；**状态（job/租约/worker/DAG 索引）必须给 `state=` 侧车**；需要 `taskmq-py[amqp]` |

```python
app = App(Config(transport="redis://127.0.0.1:6379/1?prefix=myapp:"))
```

Redis 上：score = `-priority * 2**40 + seq`，取件规则 = 先定所有队头里的最高优先级档位，
档内按 `served/weight` 加权轮询（权重来自 `Config.queues[name].weight`）、同级 FIFO；延迟/退避/让位
统一进 delayed ZSET、过期在 promote/claim 时判定。

Postgres 上用 `SELECT … FOR UPDATE SKIP LOCKED` 做原子 claim：**多台 worker 并发领取互不阻塞、不会重复**；
命名租约是 `INSERT … ON CONFLICT DO UPDATE … WHERE` 的单条 CAS；表名带 `?prefix=` 前缀，多环境共库不打架。
本地跑测试：`make pg-up && make test-postgres`（起一个 55432 端口的专用容器，`make pg-down` 删掉）。

**Lua 被禁也能用**：启动探测 `EVAL`，被禁用时自动回退 `WATCH/MULTI/EXEC` 乐观事务（语义相同，
争抢时多几个往返）。`?lua=off` 强制回退、`?lua=on` 要求必须有 Lua。

**Redis Cluster（`?cluster=1`）**：键按逻辑队列打 hash tag（`taskmq:{q}:ready`），单队列的
promote/claim 仍是一次原子 Lua（显式 KEYS，同槽）；跨队列取件跨 slot、没有单次原子可言，降级为
「每队列各取一次 + Python 侧按 band/权重选」，一致性套件据此声明跳过 `global_priority`。
客户端自带 CRC16 slot 计算 + `CLUSTER SLOTS` 拓扑 + `MOVED`/`ASK` 跟随，跨槽命令在发出前就
被拦下；只支持 db 0。本地跑测试：`make redis-cluster-up && make test-redis-cluster`。

## 扩展：接自己的后端（插件）

不改 taskmq 源码，三条路径（能力等价，细节见 [docs/design/plugins.md](docs/design/plugins.md)）：

```python
# ① 打包 + entry point：插件包声明 [project.entry-points."taskmq.plugins"]，用户只写一行
app = App(Config(transport="rocketmq://rmq.aliyuncs.com:8080/taskmq?group=workers"))

# ② 私有环境不打包：显式加载
app = App(Config(transport="rocketmq://..."))
app.load_plugins(["mycompany.mq_adapters.rocketmq"])      # 或 TASKMQ_PLUGINS=... / CLI --plugins ...
```

```bash
TASKMQ_PLUGINS=mycompany.mq taskmq --app myapp:app worker -Q email --once
taskmq --app myapp:app --plugins mycompany.mq status       # 顺带打印 transport 声明的 LIMITATIONS
```

```python
# ③ 自己管生命周期：直接给实例
app = App(Config(transport=RocketMQTransport(...)))
```

插件作者只依赖公开契约（`taskmq.transport` / `taskmq.protocol` / `taskmq.plugins`），
并且**如实声明能力**：`supports_leases` / `supports_workers` 决定框架会不会调用对应方法，
语义对不齐的写进 `limitations`（`status` 会打印）。用一致性套件自证语义：

```python
from taskmq.testing import transport_conformance

def test_my_backend():                     # 15 个场景：优先级/FIFO、原子 claim、租约回收、
    transport_conformance(                 # defer/让位/过期/幂等键/DLQ/状态往返/能力诚实性…
        lambda: MyTransport(...),
        supports={"leases": True, "workers": False},
    )
```

完整可运行示例：[examples/plugin_rocketmq](examples/plugin_rocketmq)（假 MQ 实现 + entry point + README；
跑套件 **12 个场景通过、4 个按声明跳过**——包括"不支持 job 枚举 ⇒ 该后端不能跑 DAG 工作流"）。

## DAG 工作流

依赖**声明在代码里**（可 review / 可测试 / 可画图），推进由节点完成事件驱动，**不轮询、无中心协调**：

```python
from taskmq.workflow import WorkflowBuilder

@app.workflow("etl")
def etl(wf: WorkflowBuilder, source: str):            # 提交时传参数（按关键字）
    extract = wf.step("extract", extract_task, args=(source,))
    clean   = wf.step("clean", clean_task, deps={"rows": extract})      # 上游结果按参数名注入
    stats   = wf.step("stats", stats_task, deps={"rows": clean})
    return wf.join("report", report_task, deps=[clean, stats], collect="tables")   # 汇合点按序收集

handle = app.submit_workflow("etl", {"source": "s3://bucket/2026-03-08"})
handle.status()        # 每节点 state/attempt/job_id + 总体状态
handle.get(timeout=60) # 等汇节点结果（客户端本地等待）
```

```bash
taskmq --app myapp:app workflow list              # 运行中的工作流
taskmq --app myapp:app workflow status wf-01H...  # 每节点状态 + 依赖
taskmq --app myapp:app workflow resume wf-01H...  # 补偿推进（幂等，崩溃恢复用）
```

语义要点：

- 节点就是**普通任务**：自己的重试、DLQ、超时、优先级、队列（解析顺序 `step > 任务自带 > 运行级 > config 默认`）；
- **幂等推进**：节点 job id 确定性（`{run}::{node}`）+ 幂等键 → at-least-once 下重复推进不会重复执行；
- **崩溃恢复**：节点状态是事实来源，worker 维护期对未完成的运行做补偿推进（≥1s 节流），也可以手工 `resume`；
- **失败语义**：默认 fail-fast（下游标 `SKIPPED`，其他分支照跑）；`on_failure="continue"` 表示该节点失败不阻断整条运行；
- 需要 transport 支持 `supports_job_listing`（memory/sqlite/redis 都支持；插件不实现则在提交工作流时**直接报错**）。

设计与决策（D1–D9）：[docs/design/workflows.md](docs/design/workflows.md)。

## 定时调度（beat）

```python
from taskmq.schedule import cron, every

app.schedule(
    cron("send_report", "0 9 * * *", tz="Asia/Shanghai"),   # 每天 9 点（zoneinfo 本地时区）
    every("cleanup", minutes=5, misfire="run_once"),        # 每 5 分钟，错过补一次
)
```

```bash
taskmq --app myapp:app beat                        # 独立进程；多副本靠 __beat__ 租约选主
taskmq --app myapp:app beat --once                 # 只推进一轮（测试/外部 cron 驱动）
taskmq --app myapp:app dev                         # 本地开发：worker + beat 同进程
```

misfire：`skip`（默认，错过就跳过）/ `run_once`（补一次）；状态落 `taskmq.beat.json`
（`--state` 可改），只有 leader 写；**首次部署只记基准不补跑**。

## 可观测性

结构化事件默认输出 JSON 到 stdout（`Config(events="stdout")`）；接 OTel 只需换一行：

```python
app = App(Config(events="otel"))            # 需要 pip install 'taskmq-py[otel]'
# 或自己注入 tracer（便于测试 / 自定义 exporter）：
from taskmq.otel import OtelEventSink
app.add_sink(OtelEventSink(tracer))
```

每个任务执行是一个 span（`messaging.system=taskmq`、`messaging.destination.name`、
`messaging.message.id`、`taskmq.attempt/priority/worker`），失败/重试记 ERROR 状态；
`task.deferred` 之类的事件挂成 span event。**OTel 不是 core 依赖**。

`taskmq status` 会列出在线 worker：

```
WORKERS
  worker-1.local-8123-9f2c        queues=email,default pool=threads concurrency=8 heartbeat=2s ago
```

## CLI

```bash
export TASKMQ_APP=myapp.tasks:app        # 或 --app myapp.tasks:app
taskmq worker -Q email,default -c 8
taskmq worker --once                    # 跑空即退出（CI/调试）
taskmq status --by-priority
taskmq dlq list -Q email
taskmq dlq replay --all -Q email --priority 0
taskmq call myapp.tasks.send_email --args '["a@b.com","hi"]'
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
| 2 | 最低 Python | 3.9+（开发/类型检查按 3.10） |
| 3 | 默认池 | `threads` |
| 4 | 投递语义 | at-least-once（成功才 ack） |
| 5 | 序列化 | msgspec（`json` 可切） |
| 6 | SQLite 跨主机 | 同机 + 共享盘；跨主机走 Redis/PG |
| 7 | 工作流 | 🟡 暂定 DAG 放 Phase 2 |
| 8 | Celery 兼容层 | Phase 2 可选包 `taskmq-celery` |
| 9 | 优先级 | **方案 D**：全局严格优先 + 让位 + 平级队列轮询（P1–P19 见 [priority.md](docs/design/priority.md)） |

完整设计：[docs/design.md](docs/design.md) 与分册 [docs/design/](docs/design/)。

## 发版

CI 在 `main` 上全绿后，[release.yml](.github/workflows/release.yml) 会自动把 `pyproject.toml` 的版本
打成 tag + GitHub Release（`0.1.0.dev0` → `v0.1.0`，同名 tag 已存在就跳过，幂等）。
发下一个版本：改 `version`，合进 `main` 即可；CI 跑 ruff + mypy + pyright + 全量 pytest
（Redis / Redis Cluster / PostgreSQL / RabbitMQ 都是真服务，见 [ci.yml](.github/workflows/ci.yml)）。

## License

[MIT](LICENSE) © 2026 liuhuo
