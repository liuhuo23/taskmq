# Redis：平级加权轮询 + Cluster 支持（v1.0，已落地）

> Phase 2 第四刀：**① 平级公平调度** ✅ 与 **② Cluster 支持** ✅（§5 的 C1–C4 按「建议」列拍板后实现）。
> 实现位置：`taskmq/transport/redis.py`、`taskmq/redis_client.py`、`tests/test_redis_cluster.py`。

## 1. 问题

原取件规则是「全局最小 score」：`score = -priority * 2**40 + seq` —— 跨队列严格优先级、同级 FIFO。
这满足插队，但**同级队列之间没有公平性**：q1 堆 10 万条时 q2 可能饿死。memory/sqlite 早有档内加权轮询，
Redis 之前如实声明了 `limitations["queue_weights"]` —— 本刀实现掉。

## 2. 算法（与 memory/sqlite 一致）

```
每个队列取队头（ZRANGE ready:{q} 0 0 WITHSCORES）
band = 所有队头的最高优先级
候选 = 队头处于该 band 的队列
选 served/weight 最小者（平手按队列名，保证确定性）
INCR served:{chosen}          -- 计数放 Redis，跨进程仍公平
```

- **原子性不变**：单机整段在 `_RESERVE_LUA` 一次往返里（权重读 `{prefix}weights`、计数走 `served:{queue}`）；
- **两种模式一致**：无 Lua 回退路径（`_peek_candidate`）实现同一规则 + 同样 tie-break，一致性套件两种模式都跑；
- 权重来源：`Config.queues[name].weight` → `Transport.set_queue_weights()`；
- Cluster 下同一规则仍成立，只是「选择」挪到 Python 侧（§5.3）。

## 3. 权衡

- 每次取件多 2 条 Redis 命令（HGET weights / GET served），单机仍在一个 Lua 往返内；
- 公平性只在**同一优先级档位**内生效，不同优先级仍严格按优先级（方案 D）；
- 档内轮询让「同级队列」不再互相饿死，但也不再是全局 FIFO —— 这是取舍点，不是 bug。

## 4. 验收（✅ 实测）

- 2 个同级队列、权重 3:1、各灌 100 条 → 取 40 条比例 30:10（±2）✅（Lua / 无 Lua / Cluster 三种跑法）；
- 插队不受影响：低权重队列里的高优先级任务仍然先出 ✅；
- `limitations` 里的 `queue_weights` 已移除（不再声明降级）。

---

## 5. Cluster 支持（✅ 已落地）

**为什么之前 cluster-safe 不了**：Lua 里用前缀拼键（`prefix .. "ready:" .. queue`）。Cluster 要求一次
命令/Lua 用到的键**在同一 slot**；而且 Redis 7 **不会**在脚本里拦截跨槽访问——它会照写，于是数据静默
落在错误的节点上（比 `CROSSSLOT` 报错更危险）。所以键命名和校验都得自己兜住。

### 5.1 键布局：按逻辑队列分槽（C1：按逻辑队列 ✅）

```
结构            单机                         Cluster                      说明
ready           taskmq:ready:q               taskmq:{q}:ready             队头/取件
delayed         taskmq:delayed:q             taskmq:{q}:delayed           可见性（score = visible_at）
msg             taskmq:msg:<seq>             taskmq:{q}:msg:<seq>         消息 hash（Lua 里按 id 拼，必须同槽）
leases          taskmq:leases:q              taskmq:{q}:leases            租约 ZSET
prio            taskmq:prio:q                taskmq:{q}:prio              优先级计数
served          taskmq:served:q              taskmq:{q}:served            档内公平计数
weight          taskmq:weights (HASH)        taskmq:{q}:weight            每队列权重（Cluster 下拆成单键）
dlq             taskmq:dlq:q                 taskmq:{q}:dlq               死信列表
```

全局单键（不参与同槽约束，按 slot 路由即可）：`seq` / `queues`(SET) / `jobs`(ZSET) /
`workers`(ZSET) / `worker:<id>` / `job:<id>` / `key:<idem>` / `nlease:<name>` /
`msgq`（**Cluster 专有**：`msg_id -> queue` 反查索引，消息键带 hash tag 后没法只凭 id 定位）。

- `?cluster=1` 时才加 hash tag；默认单机键名**一个字节都没变**（老数据/老客户端不受影响）；
- prefix 与队列名在 Cluster 下不能含 `{`/`}`（会抢走或破坏 hash tag）→ 构造时报 `ConfigError`；
- 只支持 db 0（Cluster 不支持 `SELECT`），URL 写别的 db 直接报错。

### 5.2 Lua：显式 KEYS

单队列脚本全部改成显式 `KEYS`（Cluster 也能用，单机共用一份）：

| 脚本 | KEYS | 用途 |
|---|---|---|
| `_MUTATE_LUA` | msg, leases, delayed, prio, dlq | ack/nack/dead/defer/extend/yield（单机与 Cluster 共用） |
| `_PEEK_QUEUE_LUA` | delayed, ready, served, weight | Cluster：promote 到期 + 返回队头/计数（不改状态） |
| `_CLAIM_QUEUE_LUA` | ready, leases, prio, served | Cluster：单队列原子取件（队头变了就 lost） |
| `_REAP_QUEUE_LUA` | leases, delayed, prio | Cluster：逐队列租约回收 |

单机跨队列脚本 `_RESERVE_LUA` / `_REAP_LUA` 保留原样（一次往返扫多队列）；它们**只在非 Cluster 模式**使用，
所以允许按前缀拼 msg 键——Cluster 模式永远不会走到这里。

### 5.3 跨队列 reserve：降到 Python 侧（C2 ✅）

跨队列 = 跨 slot，没有单次原子可言。Cluster 下的 `reserve([q1, q2, …])` 变成：

```
for 每个队列: PEEK（1 次 Lua：promote 到期 + 返回队头 + served/weight）
band = max(队头优先级)
候选 = band 内的队列；按 served/weight 升序、平手按队列名
for 候选: CLAIM（1 次 Lua：仍是原子的）→ ok 收下 / expired 清掉 / lost 换下一个
候选全 lost → 重新 PEEK（最多 3 轮，避免空转）
```

代价：一次取件是 `1 + N` 次往返（N = 队列数），且跨队列选择期间可能出现**瞬时优先级倒挂**
（先 peeking 的队列还没被 promote 到时，另一个队列的高优先级消息可能刚好到期）。
因此 `cluster_limitations` 里显式声明 `global_priority` 降级，一致性套件据此跳过该场景
（与 AMQP 同样处理）——但**同一档位内的加权轮询与跨档严格优先在单进程视角下仍然成立**，
另有 `test_priority_still_beats_weights` 等用例守着这个行为。

**job 状态**：`job:<id>` 是全局键（与队列不同槽），Lua 里碰不到 → 取件后由 Python 补 `RUNNING`、
过期由 Python 补 `EXPIRED`。崩溃窗口内 job 可能停在 `QUEUED`，租约回收后会重投（at-least-once 语义内）。

### 5.4 worker 表 / job 枚举（C3 ✅ 每 worker 一个键 + 维护期枚举）

`worker:<id>` 本来就是每 worker 一个键，指标挂在全局 `workers` ZSET；`job:<id>` + 全局
`jobs` ZSET 索引做 `list_jobs`（**不用 SCAN** —— Cluster 的 SCAN 只扫一个节点，会漏键）。
这些键都是单键操作，按 slot 路由即可，不需要维护期全量枚举。

### 5.5 开关（C4 ✅ `?cluster=1` 显式开启）

不自动探测 `CLUSTER INFO`：单机行为保持不变、测试/生产行为不因环境漂移而变。写 `cluster=1` 才启用。

```
redis://127.0.0.1:7380/0?cluster=1&prefix=myapp:
```

### 5.6 客户端：slot 路由（`RedisClusterClient`）

`taskmq/redis_client.py` 里与 `RedisClient` 同接口的集群实现：

- CRC16-XMODEM 算 slot + hash tag 规则（与 `CLUSTER KEYSLOT` 实测对齐）；
- `CLUSTER SLOTS` 建拓扑（5s 缓存），单键命令按 slot 路由到对应主节点；
- `MOVED` 就地更新映射并重试、`ASK` 走一次性 `ASKING`、`CLUSTERDOWN/TRYAGAIN/LOADING` 有界重试；
- **发命令前做同槽校验**（一命令多键 / 一次 Lua 的 KEYS）：不同槽直接报 `TransportError`，不让它变成"写错节点"；
- `flush_prefix` 跨所有主节点 SCAN（单节点 SCAN 会漏），DEL 按槽分组（`CROSSSLOT` 与"谁持有"无关）；
- 无 Lua 回退（`WATCH/MULTI/EXEC`）在 Cluster 下同样可用：事务里的键全部同槽，跨槽的 `job:<id>`
  挪到事务提交后写；`MULTI` 里报错会 `DISCARD`（否则这条连接后面只会拿到 `QUEUED`）。

### 5.7 影响面（与 PG/AMQP 两刀相当）

RedisTransport 键命名加 hash tag（`?cluster=1`）、Lua 改显式 KEYS、reserve 降级路径 + 声明
`global_priority` 降级；一致性套件在 cluster 模式下按声明跳过 `global_priority`。

## 6. 测试环境与验收（✅）

`make redis-cluster-up` 起一个 3 主容器（7380-7382，`--cluster-announce-ip 127.0.0.1` 让 MOVED
返回宿主机可达地址），`make test-redis-cluster` 跑 `tests/test_redis_cluster.py`（21 个用例）：

- 槽位算法（对 `CLUSTER KEYSLOT` 实测值）、hash tag 分槽、两个队列确实落在不同主节点；
- 跨槽取件 / ack / 租约回收 / DLQ 重放（含**换队列搬槽**：整份 hash 搬到目标槽 + `msgq` 索引 + 搬完可 ack）；
- 3:1 加权公平、优先级压过权重、4 线程 200 条无重复投递；
- 一致性套件 `16` 个场景全跑，只有 `global_priority` 按声明跳过（**Lua / 无 Lua 两种模式各一遍**）；
- `App` 端到端（`?prefix=…&cluster=1`）：20 个普通任务 + 1 个插队任务跑完。

## 7. 落地顺序

1. ✅ 平级加权轮询（Lua / 无 Lua / Cluster 三路一致）；
2. ✅ Cluster：C1–C4 拍板 → 键命名 + Lua 显式 KEYS + 降级路径 + 客户端 slot 路由 + 集群容器测试。

## 8. 明确不做（留给后续）

- **自动探测** `CLUSTER INFO`（C4 备选）：显式开关更可预测；
- **从节点读**（`READONLY` + `replicas` 参数）：当前只用主节点；
- 多 DB / `SELECT`：Cluster 不支持，直接拒绝；
- 跨队列取件的服务端原子化：除非 Redis 支持跨 slot 脚本，否则只能在客户端侧补偿。
