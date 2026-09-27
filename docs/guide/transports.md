# 选择 transport

transport 是 URL：`build_transport("redis://...")` 造实例，`Config(transport="...")` 直接用。
**换后端只改这一行**，任务代码不动。

## 能力矩阵

| | `memory://` | `sqlite://` | `redis://` | `postgresql://` | `amqp://` |
|---|---|---|---|---|---|
| 适用 | 单测 / eager | 单机多进程 / 共享盘 | 多机高吞吐 | 多机强一致 | 已有 RabbitMQ |
| 跨主机 | ❌ | ⚠️ 共享盘 | ✅ | ✅ | ✅ |
| 原子 claim | ✅ 内存锁 | ✅ `BEGIN IMMEDIATE` | ✅ Lua / `WATCH` | ✅ `FOR UPDATE SKIP LOCKED` | ✅ broker 语义 |
| 可见性租约 | ✅ | ✅ | ✅ | ✅ | ✅（+ `state=` 侧车） |
| worker 注册表 | ✅ | ✅ | ✅ | ✅ | ✅（侧车） |
| job 枚举（DAG 需要） | ✅ | ✅ | ✅ | ✅ | ✅（侧车） |
| 命名租约（`concurrency_key` / beat 选主） | ✅ | ✅ | ✅ | ✅ | 取决于侧车 |
| 外部依赖 | 无 | 无 | Redis | PostgreSQL + `psycopg` | RabbitMQ + `pika` + `state=` |

`memory://` 是**进程内**的：跨进程不共享，只适合单测和 `eager`。
`sqlite://` 支持同机多进程（WAL），跨主机要放共享盘（NFS 之类的锁语义请自测）。

## URL 参数

### `memory://`

进程内，无参数。

### `sqlite:///./taskmq.db`

`sqlite:///相对路径` / `sqlite:////绝对路径`。第一次使用自动建表；`*.db-wal`、`*.db-shm` 记得别提交进 git。

### `redis://host:port/db?prefix=app1:&lua=on&cluster=1`

| 参数 | 说明 |
|---|---|
| `prefix` | 键前缀（默认 `taskmq:`），多套环境共库不打架 |
| `lua` | `on` 必须有 Lua；`off` 强制走 `WATCH/MULTI/EXEC`；不写=启动探测 |
| `cluster` | `1` 开启 Redis Cluster 模式（见下） |

```bash
redis://127.0.0.1:6379/1?prefix=myapp:               # 单机 / 主从
redis://127.0.0.1:7380/0?prefix=myapp:&cluster=1     # Cluster（db 只能是 0）
```

Redis 侧语义：score = `-priority * 2**40 + seq`，取件 = 「最高优先级档位 → 档内 `served/weight` 加权轮询 → 同级 FIFO」；
延迟/退避/让位统一进 delayed ZSET；过期在 promote/claim 时判定（所以 `reap_expired_jobs()` 在 Redis 上是 no-op）。

!!! warning "Cluster 模式的三个前提"
    1. **只支持 db 0**（Cluster 不支持 `SELECT`），URL 写别的 db 直接报错；
    2. **`prefix` 和队列名不能含 `{` / `}`**——键按逻辑队列打 hash tag（`taskmq:{q}:ready`），花括号会破坏分槽；
    3. **跨队列取件不再是一次原子操作**：跨 slot 无法原子，降级成「每队列各取一次 + Python 侧按 band/权重选」，
       `status` 的 `LIMITATIONS` 里会如实声明 `global_priority` 降级。

    客户端自带 CRC16 槽位计算 + `CLUSTER SLOTS` 拓扑 + `MOVED`/`ASK` 跟随，跨槽命令在发出前就被拦下。

### `postgresql://user:pass@host:5432/db?prefix=app1_`

需要 `pip install "taskmq-py[postgres]"`。`prefix` 是表名前缀（多环境共库）。

原子 claim 用 `SELECT … FOR UPDATE SKIP LOCKED`：多台 worker 并发领取不阻塞、不重复；命名租约是
`INSERT … ON CONFLICT DO UPDATE … WHERE` 的单条 CAS。

### `amqp://user:pass@host:5672/vhost?state=sqlite:///./state.db&prefix=taskmq.`

需要 `pip install "taskmq-py[amqp]"`。**`state=` 是必填**：AMQP 是消息代理，没有 KV 存储，
job 状态 / 命名租约 / worker 表 / job 枚举都放在侧车（可以是 sqlite / postgres / redis）。

AMQP 是 per-queue 有序，**跨队列不做全局严格优先** → 声明 `global_priority` 降级（用 `x-max-priority` 排同队列）。

**吞吐开关**：默认每条发布都等 broker 的持久化确认（`delivery_mode=2` + publisher confirm），
每次都要等一次落盘 fsync —— 可靠但慢（本机 RabbitMQ 实测 58 条/秒）。加 `&confirms=off` 就不等回执，
本机实测 6.4k/s（约 100 倍），代价是**发布失败不再报错**（消息丢了你看不见）。要可靠性保持默认，
要高吞吐考虑 `redis://`。详见 [吞吐与上限（实测）](workers.md#吞吐与上限实测)。

## 声明式降级与一致性套件

每个后端都要**如实声明**能力：

```python
transport.supports_leases        # False → concurrency_key / beat 选主直接启动报错
transport.supports_workers       # False → 不注册心跳，status 里没有 WORKERS
transport.supports_job_listing   # False → 提交 DAG 工作流时直接报错
transport.limitations            # {"global_priority": "跨队列不做全局严格优先"}
```

`taskmq status` 会打印 `LIMITATIONS` 段，一眼看到你在哪个后端上少了什么能力。
内建后端跑同一套**一致性套件**自证语义（第三方后端也建议在自家 CI 里跑）：

```python
from taskmq.testing import transport_conformance

def test_my_backend():
    transport_conformance(lambda: MyTransport(...), supports={"leases": True, "workers": False})
```

套件覆盖 16 个场景：优先级/FIFO、跨队列优先、原子 claim、ack 语义、租约回收、defer、让位、过期、
幂等键、DLQ 重放、job 状态、可见性、命名租约、worker 注册表、job 枚举、能力诚实性；
被 `limitations` 声明降级的场景会**显式跳过**（而不是静默失效）。

## 本地起测试服务

```bash
make pg-up    && make test-postgres        # PostgreSQL 16（55432）
make mq-up    && make test-amqp            # RabbitMQ 3.12（55672）
make redis-cluster-up && make test-redis-cluster   # 3 主 Redis Cluster（7380-7382）
```

## 下一步

- [接入新后端（插件）](plugins.md)：自己的 MQ 怎么接
- [Redis 公平调度与 Cluster 设计](../design/redis-cluster.md)：键布局与降级细节
