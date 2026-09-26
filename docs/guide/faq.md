# 常见问题

## 和 Celery 比怎么样？

同一条路（生产者投递 → worker 消费 → 结果/状态），但取向不同：**零外部服务起步**（`memory://` /
`sqlite://` 就是完整功能，不是玩具）、配置显式（不做通配魔法）、语义可预测（每种后端都要
如实声明 `limitations` 并跑同一套一致性套件）。Celery 兼容 shim（`taskmq-celery`）规划在后续
Phase，见 [README 决策表](https://github.com/liuhuo23/taskmq#决策)。

## 消息会丢吗？会重复执行吗？

- **不会丢**：消息先落 transport 才返回；worker 拿在手里没 ack 也不算丢——租约过期后会被回收重投。
- **会重复**：默认 at-least-once。worker 崩溃、租约超时、`nack(requeue=True)` 都会重投，所以任务应当**幂等**；
  实现手段：幂等键（`key=`）、`concurrency_key`、业务侧去重。
- 想要「最多一次」？把 `ack="on_receipt"` 会更快但更可能丢——不推荐，除非任务天然可丢。

## 优先级范围为什么只有 -9..9？

够用且能强制想清楚（P1）：越界不 clamp、直接报错；同优先级档位内用队列权重做公平。档位少的好处是
「跨档严格优先」不会退化成事实上的 FIFO。

## worker 起来了但任务不执行？

最常见的原因：**队列没对上**。worker 只订阅 `-Q` 列出的队列；任务投到哪个队列按
「提交参数 > 任务定义 > `Config.default_queue`」。先 `taskmq status` 看 `pending` 在哪个队列。

其次是 `memory://` 用在了两个进程里——它是进程内的，跨进程必须换后端。

## sqlite 能上生产吗？

单机（或共享盘）可以：WAL + `BEGIN IMMEDIATE` 保证原子 claim，多进程安全。
跨主机不要用共享盘 sqlite，换 `redis://` 或 `postgresql://`。

## Redis 集群（Cluster）能用吗？

能，加 `?cluster=1`（只支持 db 0）。注意三点：prefix/队列名不能含花括号；跨队列取件不再是
单次原子（降级为每队列各取一次 + Python 侧选择，`status` 会声明 `global_priority` 降级）；
需要真实集群做测试：`make redis-cluster-up && make test-redis-cluster`。

## 长任务被别的 worker 抢走 / 重复执行？

租约（`Config(lease=60)`）默认 60 秒，worker 会按 `min(heartbeat_interval, lease/3)` 自动续租。
但如果进程被 `SIGKILL` / 机器断电，租约到期后消息会重投——这是设计如此。任务越久，越要幂等；
也可以调大 `lease@@ 降低误判。

## 怎么控制 CPU 密集任务的并发？

`Config(pool="processes", concurrency=4)` + 可选 `@app.task(hard_timeout=30)`。
`processes` 池通过 `--app module:attr` / `TASKMQ_APP` 重建 App，所以入口必须是可导入的字符串。

## 任务能拿到自己的日志/进度吗？

可以。`bind=True` 或 `current_task()` 拿到 `ctx`：`ctx.log.info(...)`（结构化日志）、
`ctx.update_meta(...)`（写进 job.meta，`handle.info` / 工作流状态可见）、`ctx.retry(...)`。

## 怎么在不改代码的情况下加日志/tracing/审计？

自定义 EventSink（`register_sink` + `Config(events="...")`；OTel 就是内建的一个 sink），
或者直接用 CLI 的 `--plugins` 加载你的 sink 模块。

## 我没装 psycopg / pika 会怎样？

只有用到对应后端时才需要：`pip install "taskmq[postgres]"` / `"taskmq[amqp]"` / `"taskmq[otel]"`。
没装就构造那个 transport 会立刻报错（fail fast），不影响其他后端。

## 有生产版本了吗？

有：见 [Releases](https://github.com/liuhuo23/taskmq/releases)。发版流程是「`main` 上 CI 全绿 → 自动打 tag +
建 Release」（tag = `pyproject.toml` 的版本去掉 `.devN`），还没发布到 PyPI。

## 下一步

- 没找到答案？[开 issue](https://github.com/liuhuo23/taskmq/issues) 或先看[设计文档](../design.md)
