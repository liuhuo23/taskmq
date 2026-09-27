---
name: taskmq
description: 用 taskmq（Python 分布式任务队列）写代码、排查问题，或修改 taskmq 仓库本身时使用。触发场景：需要异步/后台任务、任务队列、Celery 替代方案、优先级队列与插队、延迟/定时任务、cron/interval 调度、DAG 工作流、重试与退避、限流、按 key 串行、死信 DLQ、worker 进程与并发、多机消费、以及把 SQLite / Redis / Redis Cluster / PostgreSQL / RabbitMQ 当作队列后端；也包括「安装或升级 taskmq-py」「taskmq 命令行（worker/status/dlq/beat/workflow）」「跑 taskmq 的测试」「给 taskmq 发版」。即使用户没有直接说出 taskmq 这个词，只要在讨论 Python 任务队列、后台作业、异步执行，也应先读本 skill。
---

# taskmq

零外部服务就能跑起来、投递语义可预测、配置显式、调试不用猜的 Python 分布式任务队列。
安装名（PyPI）是 `taskmq-py`，import 名和命令都是 `taskmq`。

## 先选路径

| 你要做的事 | 读这个 |
|---|---|
| 用 taskmq 写业务代码（定义任务、投递、跑 worker） | `references/usage.md` |
| 想要「某某需求怎么写」的现成代码 | `references/recipes.md` |
| 排查问题（任务不执行、重复执行、卡住、进 DLQ） | `references/usage.md` 的「排查」一节 |
| 改 taskmq 仓库本身（加功能、修 bug、发版） | `references/contributing.md` |
| 只想确认环境能跑通 | 执行 `scripts/smoke.py` |

## 心智模型（30 秒）

- **at-least-once**：只有 `ack` 之后才算成功。worker 崩溃 → 租约到期 → 消息重投 → **任务可能执行两次**，所以任务要幂等（或给提交加幂等键）。
- **可见性租约**：reserve 到的消息默认锁定 `lease` 秒并由 worker 自动续租；进程被 kill 后租约到期自动回滚。
- **优先级**：整数 `-9..9`，越大越优先；取件规则是「跨档严格优先 → 档内按队列权重轮询 → 同级 FIFO」。插队只发生在**还没开始执行**的预留上，正在跑的不会被中断。
- **defer ≠ 重试**：限流、`concurrency_key` 抢不到锁时用 `defer` 放回，**不消耗投递次数**，因此不会触发毒丸保护。
- 消息最终失败进 **DLQ**（`taskmq dlq list` / `replay`）。

## 最小可用（复制即可跑）

```python
# myapp/tasks.py
from taskmq import App, Config, Priority, Retry

app = App(Config(
    transport="sqlite:///./taskmq.db",   # 换成 redis:// / postgresql:// 即可多机
    concurrency=8,
))

@app.task(queue="email", retry=Retry(max_attempts=5, backoff="exp"))
def send_email(to: str, subject: str) -> str:
    return f"sent:{to}"
```

投递与消费：

```python
from myapp.tasks import send_email

send_email.delay("a@b.com", "hi")                                       # 只传任务参数
send_email.apply_async(("vip@b.com", "now"), priority=Priority.CRITICAL) # 带投递选项
```

```bash
export TASKMQ_APP=myapp.tasks:app
taskmq worker -Q email -c 8          # 生产：独立进程消费（-Q 必须列出订阅的队列）
taskmq status --by-priority          # 队列深度 / 优先级分布 / worker / LIMITATIONS
```

单测或调试时在**同进程**里跑（`memory://` 只在同进程可见）：

```python
from taskmq.testing import run_until_idle
run_until_idle(app, queues=["email"], timeout=10)
```

## 最容易踩的坑

1. **worker 的 `-Q` 和任务的队列必须对上**。任务投到 q1，worker 只订阅 q2 → 永远不会执行。先用 `taskmq status` 看 `pending` 落在哪个队列。
2. **`memory://` 是进程内的**：跨进程/跨机器必须换 `sqlite://`（单机共享盘）、`redis://` 或 `postgresql://`。
3. **任务要幂等**：at-least-once 下重投是常态。用 `apply_async(..., key="...")` 做提交去重，或在业务侧去重。
4. **`delay()` 只收任务参数**；队列/优先级/eta/key 等投递选项走 `apply_async()`。
5. **优先级只有 `-9..9`**，越界直接 `ConfigError`（不 clamp）。
6. **`async def` 任务必须跑 `pool="asyncio"`**，否则启动即报错（不做隐式 `asyncio.run()`）。
7. **`processes` 池会重建 App**：必须能通过 `--app module:attr` / `TASKMQ_APP` 找到它；`hard_timeout` 也只有该池能真正强杀。
8. **`LeaseLost` / `MessageNotFound` 是正常现象**：租约过期后迟到 `ack` 会被拒。不要重试 ack，丢弃本地结果并记录即可。

## 关键事实（别记错）

- 安装：`pip install taskmq-py`（extras：`[postgres,amqp,otel]`）；Python **3.9+**（开发/类型检查按 3.10）。运行时依赖只有 `msgspec`。
- 命令行两种等价入口：`taskmq ...`（console script）与 `python -m taskmq ...`；App 用 `--app module:attr` 或 `TASKMQ_APP` 指定。
- transport 是 URL：`memory://`、`sqlite:///./db`、`redis://host:port/db?prefix=app:`（`&cluster=1` 开 Cluster，db 只能 0）、`postgresql://...?prefix=app_`、`amqp://...?state=<侧车URL>`。
- 每个后端都会**如实声明** `supports_leases / supports_workers / supports_job_listing` 与 `limitations`；`taskmq status` 会打印 `LIMITATIONS`。
- DAG 工作流需要 transport 支持 `supports_job_listing`（内建后端都支持）。
- 文档站：<https://liuhuo23.github.io/taskmq/>；源码：<https://github.com/liuhuo23/taskmq>。

## 参考文件

- `references/usage.md` —— 完整用法：任务定义与全部选项、transport 选择、worker、工作流、beat、CLI、测试工具、排查表
- `references/recipes.md` —— 可直接抄的配方（幂等、延迟、限流、串行、DLQ 重放、优雅停机、DAG、beat、单测、上线检查单）
- `references/contributing.md` —— 在 taskmq 仓库里改代码的规矩（uv 开发流、3.9 兼容写法、测试与容器、发版到 PyPI 的全自动链路）
- `scripts/smoke.py` —— 冒烟脚本：`python scripts/smoke.py` 验证安装可用（sqlite + 优先级 + 取结果）
