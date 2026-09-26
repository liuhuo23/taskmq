# CLI 与运维

## 命令一览

```bash
taskmq --app myapp.tasks:app [--plugins mycompany.mq] <命令> [选项]
```

| 命令 | 作用 |
|---|---|
| `worker -Q a,b -c 8 [--once]` | 消费队列（`--once`：跑空即退出） |
| `status [--by-priority]` | 队列深度 / 优先级分布 / 在线 worker / LIMITATIONS |
| `dlq list [-Q q]` | 列出死信 |
| `dlq replay --all [-Q q] [--priority n]` | 重放死信（attempt 归 1） |
| `beat [--once] [--state path]` | 定时调度器（租约选主） |
| `workflow list\|status\|resume` | DAG 工作流：运行中列表 / 逐节点状态 / 补偿推进 |
| `dev` | 本地开发：worker + beat 同进程 |
| `call <task> --args '[...]'` | 在本进程同步执行一次（调试，不走 reserve/ack） |
| `--version` | 版本（来自打包元数据） |

`--app` 或 `TASKMQ_APP=module:attr` 指向你的 `App` 实例；`--plugins` / `TASKMQ_PLUGINS` 加载插件模块。

```bash
export TASKMQ_APP=myapp.tasks:app
taskmq worker -Q email,default -c 8
taskmq worker --once
taskmq status --by-priority
taskmq dlq list -Q email
taskmq dlq replay --all -Q email --priority 0
taskmq call myapp.tasks.send_email --args '["a@b.com","hi"]'
```

## status 看什么

```text
QUEUES
  email        pending=12 inflight=3 dead=1
  default      pending=0  inflight=0 dead=0
PRIORITY                      # --by-priority
  9: 1   5: 4   0: 7
WORKERS
  w-8123-9f2c  queues=email,default pool=threads concurrency=8 heartbeat=2s ago
LIMITATIONS
  reap_expired_jobs  过期在 reserve 的 promote/claim 阶段判定
```

- `pending` = ready + delayed；`inflight` = 持有租约（正在跑）；`dead` = DLQ 长度；
- `LIMITATIONS` 是 transport **如实声明**的降级（没声明的能力问题就是 bug）。

## 可观测性

### 结构化事件

```python
Config(events="stdout")     # 默认：一行一个 JSON
Config(events="null")       # 关掉（测试常用）
```

事件名：`task.submitted` / `task.started` / `task.succeeded` / `task.retrying` /
`task.failed` / `task.deferred` / `worker.started` / `worker.stopped` / `beat.fired` /
`beat.standby` / `beat.error` / `workflow.started` / `workflow.advanced` / `workflow.succeeded`。

### OTel

```python
Config(events="otel")                  # 需要 pip install "taskmq-py[otel]"
# 或自己注入 tracer：
from taskmq.otel import OtelEventSink
app.add_sink(OtelEventSink(tracer))
```

每次任务执行是一个 span（`messaging.system=taskmq`、`messaging.destination.name`、
`messaging.message.id`、`taskmq.attempt/priority/worker`），失败/重试记 ERROR 状态，
`task.deferred` 之类的事件挂成 span event。**OTel 不是 core 依赖**。

### 自定义 sink

```python
from taskmq.plugins import register_sink

register_sink("my-exporter", lambda **kw: MyExporter(**kw))
app = App(Config(events="my-exporter"))
```

## 常见运维动作

| 想做的事 | 怎么做 |
|---|---|
| 看队列积压 | `taskmq status --by-priority` |
| 看 worker 是否还活着 | `status` 的 `WORKERS` 段（heartbeat 年龄） |
| 处理毒丸消息 | `dlq list` 看原因 → 修数据 → `dlq replay --all` |
| worker 崩溃后消息卡住 | 不用手工干预：租约到期后 worker 维护期自动回收重投 |
| 改优先级/队列后重新投递 | `dlq replay --priority 9 -Q email` |
| 手工触发一次调度 | `taskmq beat --once` |
| 工作流卡住 | `taskmq workflow status <run>` → `workflow resume <run>` |

## 下一步

- [接入新后端（插件）](plugins.md) ｜ [常见问题](faq.md)
