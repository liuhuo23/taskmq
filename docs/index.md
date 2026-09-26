# taskmq

[![ci](https://github.com/liuhuo23/taskmq/actions/workflows/ci.yml/badge.svg)](https://github.com/liuhuo23/taskmq/actions/workflows/ci.yml)
[![release](https://img.shields.io/github/v/release/liuhuo23/taskmq?label=release)](https://github.com/liuhuo23/taskmq/releases)
[![license](https://img.shields.io/github/license/liuhuo23/taskmq)](https://github.com/liuhuo23/taskmq/blob/main/LICENSE)
![python](https://img.shields.io/badge/python-3.10%2B-blue)

**零外部服务就能跑起来、投递语义可预测、配置显式、调试不用猜**的 Python 分布式任务队列。

## 30 秒上手

```python
from taskmq import App, Config
from taskmq.testing import run_until_idle

app = App(Config(transport="memory://"))

@app.task(queue="email")
def send_email(to: str) -> str:
    return f"sent:{to}"

send_email.delay("a@b.com")             # 投递（异步）
run_until_idle(app, queues=["email"])   # 同进程就地跑 worker：断点调试友好
```

生产上把 `transport` 换成真正的后端，worker 单独进程跑：

```bash
export TASKMQ_APP=myapp.tasks:app       # 你的 App 实例（module:attr）
taskmq worker -Q email -c 8      # 消费 email 队列，并发 8
```

## 特性一览

| | |
|---|---|
| **投递语义** | 默认 **at-least-once**（成功才 ack）+ 可见性租约 + DLQ / 重放 |
| **优先级** | `-9..9`，数值越大越优先；跨档严格优先 + **档内平级加权轮询** + 未开始预留让位 |
| **transport** | `memory://`、`sqlite://`、`redis://`（含 `?cluster=1`）、`postgresql://`、`amqp://`；插件可接自有 MQ |
| **执行池** | `threads`（默认）、`processes`（硬超时可强杀）、`asyncio`、`solo` |
| **可靠性** | 幂等键、`concurrency_key` 跨 worker 串行、限流、`max_deliveries` 毒丸保护 |
| **编排** | 原生 **DAG 工作流**（依赖写在代码里，事件驱动、不轮询）+ **beat** 定时（cron/interval + 租约选主） |
| **可观测** | 结构化事件（JSON stdout）、OTel span、`taskmq status` 看队列 / worker / DLQ |
| **依赖** | 运行时只有 `msgspec`；PostgreSQL / AMQP / OTel 都是可选 extras |

## 从这里开始

- [安装与快速开始](guide/install.md)：装好、跑通第一个任务
- [核心概念](guide/concepts.md)：租约、ack、让位、优先级、DLQ 到底是什么
- [定义任务](guide/tasks.md)：三种写法与全部可调项
- [选择 transport](guide/transports.md)：五个后端怎么选、能力差在哪
- [运行 worker](guide/workers.md)：池、并发、限流、优雅退出
- [定时调度（beat）](guide/scheduling.md)
- [DAG 工作流](guide/workflows.md)
- [CLI 与运维](guide/cli.md)
- [接入新后端（插件）](guide/plugins.md)
- [常见问题](guide/faq.md)

设计取向与决策记录在[设计文档](design.md)：[优先级](design/priority.md)、[任务模型](design/tasks.md)、
[工作流](design/workflows.md)、[插件与一致性套件](design/plugins.md)、
[Redis 公平调度与 Cluster](design/redis-cluster.md)。

## 安装

**只需要 Python 3.10+ 和 pip**（不需要 uv）。三种装法见[安装与快速开始](guide/install.md)：
`pip install "taskmq-py @ git+https://github.com/liuhuo23/taskmq"`、从
[Releases](https://github.com/liuhuo23/taskmq/releases) 下载 wheel，或 clone 源码 `pip install -e ".[dev]"`。

装完就有 `taskmq` 命令；没进 PATH 时用 `python -m taskmq`（等价）。

## License

[MIT](https://github.com/liuhuo23/taskmq/blob/main/LICENSE) © 2026 liuhuo
