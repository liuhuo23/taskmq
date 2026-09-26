# 安装与快速开始

## 环境要求

- **Python 3.10+**（CI 在 3.10 上跑全量测试）
- 运行时依赖只有 `msgspec`；PostgreSQL / AMQP / OTel 都是可选 extras
- 各后端需要的外部服务见[选择 transport](transports.md)

## 安装

还**没有发布到 PyPI**，三种装法：

=== "源码（开发）"

    ```bash
    git clone https://github.com/liuhuo23/taskmq && cd taskmq
    uv sync                 # .venv（Python 3.10）+ dev 依赖
    uv run pytest           # 全量测试（带服务时会连真 Redis/PG/RabbitMQ）
    ```

=== "Releases 里的 wheel"

    ```bash
    pip install https://github.com/liuhuo23/taskmq/releases/download/v0.1.0/taskmq-0.1.0-py3-none-any.whl
    ```

=== "直接从 git"

    ```bash
    pip install "taskmq @ git+https://github.com/liuhuo23/taskmq"
    # 需要哪个后端就带哪个 extra：
    pip install "taskmq[postgres,amqp,otel] @ git+https://github.com/liuhuo23/taskmq"
    ```

## 冒烟测试

```bash
uv run taskmq --version          # 版本号来自打包元数据
```

```python
from taskmq import App, Config
from taskmq.testing import run_until_idle

app = App(Config(transport="memory://", events="null"))

@app.task(queue="demo")
def add(a: int, b: int) -> int:
    return a + b

handle = add.delay(1, 2)
run_until_idle(app, queues=["demo"])     # 同进程跑 worker，把队列跑空
assert handle.get(timeout=5) == 3
```

`memory://` 是**进程内**的：worker 必须在同一个进程里（`run_until_idle` 就是干这个的）。
跨进程/跨机器要换成真的后端。

## 第一个真正的部署

### 1) 定义 App（`myapp/tasks.py`）

```python
# myapp/tasks.py
from pathlib import Path

from taskmq import App, Config, Priority, Retry

app = App(
    Config(
        transport="sqlite:///./taskmq.db",   # 换成 redis://... / postgresql://... 即可多机
        concurrency=8,
        events="stdout",
        serializer="json",                   # 不装 msgspec 也能跑
    )
)

@app.task(queue="email", retry=Retry(max_attempts=5, backoff="exp"), priority=Priority.NORMAL)
def send_email(to: str, subject: str) -> str:
    return f"sent:{to}"
```

### 2) 起 worker 进程

```bash
export TASKMQ_APP=myapp.tasks:app
uv run taskmq worker -Q email -c 8        # -Q 队列（逗号分隔），-c 并发
uv run taskmq worker -Q email --once      # 跑空即退出：CI / 调试用
```

!!! warning "worker 必须显式声明订阅的队列"
    没有 `-Q` 时用 `Config.default_queue`（默认 `default`）；任务投到哪个队列，
    就得有 worker 订阅哪个队列（队列名解析顺序：提交参数 > 任务定义 > `Config.default_queue`）。

### 3) 投递任务

```python
from myapp.tasks import send_email

send_email.delay("a@b.com", "hi")                          # 只传任务参数
send_email.apply_async(("vip@b.com", "now"), priority=Priority.CRITICAL)   # 带投递选项
```

```bash
uv run taskmq --app myapp.tasks:app call myapp.tasks.send_email --args '["a@b.com","hi"]'
uv run taskmq --app myapp.tasks:app status           # 队列深度 / 优先级分布 / worker
```

### 4) 换后端只改一行

```python
Config(transport="memory://")                                    # 单测 / eager
Config(transport="sqlite:///./taskmq.db")                        # 单机、多进程、共享盘
Config(transport="redis://127.0.0.1:6379/1?prefix=myapp:")       # 多机高吞吐
Config(transport="redis://127.0.0.1:7380/0?prefix=myapp:&cluster=1")   # Redis Cluster（db 固定 0）
Config(transport="postgresql://taskmq:taskmq@db:5432/taskmq?prefix=myapp_")  # 强一致、多机
Config(transport="amqp://user:pass@mq:5672/%2F?state=sqlite:///./state.db")  # 已有 RabbitMQ
```

## 环境变量

配置可以显式从环境变量来（**只做名字一对一的映射，不做通配魔法**）：

```python
Config.from_env()          # TASKMQ_TRANSPORT / TASKMQ_CONCURRENCY / TASKMQ_LEASE / ...
```

```bash
export TASKMQ_APP=myapp.tasks:app        # CLI 用它找 App
export TASKMQ_PLUGINS=mycompany.mq       # 插件模块（逗号分隔）
```

## 下一步

- [核心概念](concepts.md)：先把语义看懂，再谈调参
- [选择 transport](transports.md)：哪个后端适合你的场景
- [运行 worker](workers.md)：池、并发、优雅退出
