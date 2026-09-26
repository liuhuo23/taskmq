# 优先级设计（草案 v0.1）

> 分册：[../design.md](../design.md) 只解决「优先级」这一件事。
> 状态：**v1.0 已定稿**（P1–P19 全部 ✅，见 §10）。核心需求：**紧急任务插队**（§3）。
> 正文中的 ❓ 保留为决策过程记录，**结论以 §10 为准**；实现按 §10 对齐。

## 0. 一句话

优先级决定「**下一次 claim 先取哪条消息**」，并且保证：

- **G1 有空闲槽位时，当前可见的最高优先级消息先跑**；
- **G2 未开始的低优预留必须让位**给更高优先级；
- **G3 已开始执行的任务永不打断**（非抢占）。

它不承诺执行顺序、不承诺完成顺序，也不能跨越 `concurrency_key` 与速率限制。

## 1. 目标 / 非目标

目标：

- **可预测**：给定队列内容与配置，claim 顺序可复算（排序键固定、平局规则固定）。
- **不饿死**：持续高优负载下，低优先级消息仍有有界的最坏等待时间——需要**显式策略**，不能靠运气。
- **可观测**：`status` / 事件流能按优先级看到积压。
- **零魔法**：优先级来源唯一确定，不做隐式提升、不做静默 clamp。

非目标（Phase 0/1 明确不做）：

- 不做**运行中抢占**：已开始执行的任务不会被高优任务打断（但**做让位式插队**，见 §3.2）。
- 全局严格优先现在是**默认**（方案 D，为插队服务）；队列间公平只作为**平级**的 tie-break。
- 不做优先级反转治理 / 预算 / shuffle sharding（Phase 2+）。
- 不承诺 ETA（延迟任务）到点的相对顺序（见 §6）。

## 2. 数值语义

| 项 | 结论 |
|---|---|
| 方向 | **数值越大越优先**（与 AMQP / Celery / 常识一致） |
| 默认值 | `0` |
| 合法范围 | ❓ 建议 `-9..9`；越界在 `App()` 构造期抛 `ConfigError`（不 clamp） |
| 平局规则 | `id ASC`（ULID，等价于入队序 FIFO） |
| 排序键 | `(priority DESC, visible_at ASC, id ASC)`（`visible_at` 对已可见消息恒等于过去，不影响平局） |
| 便捷常量 | ❓ 是否提供 `Priority.LOW / NORMAL / HIGH / CRITICAL`（建议提供，纯常量，不引入行为） |

## 3. 插队：让位式抢占（核心需求）

需求原话：**「很多时候需要插队。突然来一个高优先级任务，一旦有资源就应该立刻执行它。」**

### 3.1 形式化保证（要写进实现与测试）

- **G1 空闲即最高**：任意 worker 出现空闲槽位时，它领取的必须是**当时可见的最高优先级消息**。
- **G2 未开始必须让位**：worker 持有的「已 reserve 但尚未开始执行」的消息，当更高优先级消息可见时**必须交回队列**（受防抖与上限保护）。
- **G3 执行中不打断**：已开始执行的任务**永不**被高优任务打断。❓ 若这条不符合预期，见 P19。

「立刻」的边界 = `poll_interval`（默认 50ms，可调小）+ 一次让位探测往返。
SQLite 只能轮询；Redis（Phase 1）可用 `BZPOPMIN` 做到接近推送。

### 3.2 插队的三个层次（缺一不可）

| 层次 | 做什么 | 解决什么 |
|---|---|---|
| ① claim 排序 | 领取时**全局**按 `priority DESC` 选，不被队列边界挡住 | 「我提交时正好有别人在抢」 |
| ② 让位（yield） | 交回**未开始**的低优预留，腾出槽位 | 「槽位被低优预取占了但还没跑」 |
| ③ 运行中抢占 | 打断正在跑的低优任务 | ❓ 默认**不做**（P19） |

② 的必要性来自 prefetch：只要允许「预取超过当前空闲槽位」，就必然出现「槽位占着但活没开始」的窗口。两条对策一起上：

- **reserve–start 耦合**：只在真有**空闲槽位**时 reserve；`prefetch` 默认 = `concurrency`；
  执行池的内部队列也必须**有界**（容量 = 可用槽位），不允许无限缓冲。
- **让位兜底**：任何「未开始」的预留都可被更高优先级抢走（`prefetch > concurrency`、进程池/事件循环池的缓冲场景）。

### 3.3 让位的实现

transport 新增两个原语：

~~~python
def peek_max_priority(self, queues) -> int | None: ...
    # 当前可见消息里的最高优先级（一次 SELECT MAX，比给每条候选排序便宜）

def yield_reservation(self, delivery, *, delay: float) -> None: ...
    # 未开始的预留放回队列：state -> queued，visible_at = now + delay
    # yields += 1；**不计入 deliveries**（否则让位会把普通消息送进 DLQ）
~~~

worker 每轮 poll：

1. `peek_max_priority(queues)`；
2. 若它 > 「我持有的未开始预留中的最低优先级」，且差值 ≥ `yield_min_delta`（默认 1），
   对这些预留调用 `yield_reservation`（从最低优先级开始，够腾出槽位即止），然后重新 reserve；
3. 没有任何未开始预留时，这一步近乎零成本。

**防抖 / 防活锁**（必须有，否则高优洪峰会变成 reserve→yield 抖动）：

- `yield_delay`（默认 0.1s + 抖动）：让位后延迟可见，避免立刻又被抢回去。
- `max_yields`（默认 100）：单条消息让位次数上限；到顶后 `yieldable=false`，只能按正常顺序等。
- `yield_min_delta`：同优先级不触发让位。
- 让位只发生在 **reserved 且未开始** 的窗口；一旦提交给执行池就算「开始」。

### 3.4 比较范围（❓ 关键决策）

三层来源，解析顺序（右侧不能覆盖左侧）：

`submit(priority=…)` > `@app.task(priority=…)` > `QueueConfig.priority` > `Config.default_priority`

| 方案 | 语义 | 优点 | 代价 |
|---|---|---|---|
| **D（推荐）** | **全局**按优先级排序；队列 `weight` 只在**同优先级档位内**做轮询 | 满足「跨队列插队」；平级不偏袒 | 持续高优会饿死低优，需要可见性与兜底（§5） |
| A | 只在同一 queue 内比较，队列之间加权轮询 | 不饿死、实现简单 | **跨队列插队做不到**（高优要等自己队列的轮次） |
| B | 全局严格优先，平级不给公平 | 最简单 | 同优先级下会被某个队列刷屏 |
| C | 全局优先 + aging | 兼顾防饥饿 | 多算一层，调参敏感 |

❓ **选哪个？** 按插队需求建议 **D**（全局严格优先 + 让位 + 平级队列轮询）。

## 4. 队列权重（只在同优先级档位内生效）

- `QueueConfig.weight: int = 1`：**只决定同一优先级档位内**的取件比例；**不参与跨档位比较**（否则就不叫插队了）。
- 取件流程：① 找到当前可见的最高优先级档位 → ② 在该档位内按 weight 轮询 → ③ 取满 `limit`。
- 档位内配额按 `weight / Σweight × limit` 分配，**至少 1 条**（否则小权重队列永远取不到）。
- 队列级 `priority` 仅作为「任务/提交都没写优先级时的 fallback」，不参与跨档位比较。
- ❓ 默认所有队列等权（`weight=1`）？建议是。

## 5. 防饥饿（插队的必然代价）

- 插队语义**天然会饿死低优消息**：高优持续到达时，低优可能长期等待。这点必须写进文档，不假装不存在。
- Phase 0/1 默认 **不做 aging**，但必须提供可见性：
  1. `queue.depth` 事件与 `status --by-priority` 能看见「谁在饿」；
  2. 可选兜底：`Config.normal_reserved_slots: int = 0`（给普通优先级留 N 个槽位，>0 时高优最多占用 `concurrency - N`）。
- Phase 2 可选 aging：`effective = base + min(cap, wait / interval)`，**内存计算不写库**。
- ❓ 兜底选哪个？是否 Phase 0 就要 `normal_reserved_slots`？（建议：默认 0，只做可见性 + 告警，Phase 2 再上兜底）

## 6. 与其它机制的关系（边界必须写清楚）

| 机制 | 规则 | 状态 |
|---|---|---|
| 重试 | **保持原优先级**；❓ 是否提供 `retry_priority="keep"\|"lower"`（降级防雪崩） | ❓ |
| 子任务 `ctx.publish()` | **默认继承父任务优先级**（显式传参可覆盖） | ❓ |
| DLQ 重放 | **保留原优先级**；❓ CLI 是否允许 `--priority` 覆盖 | ❓ |
| beat / 定时任务 | 用任务级 `priority`，到点入队后与普通消息同池竞争 | 建议固定 |
| `concurrency_key` | 同 key 内**不保证**优先级顺序（拿不到锁就重投，重投后重新竞争） | 建议固定 |
| 速率限制 | 令牌**先到先得**，高优不插队；❓ 是否优先给高优 | ❓ 建议先到先得 |
| prefetch / 预留 | **reserve–start 耦合**：只在真有空闲槽位时 reserve；`prefetch` 默认 = `concurrency`；池内队列**有界** | 建议固定（§3.2） |
| **让位（yield）** | **未开始**的预留必须让给更高优先级；`yields` 单独计数，**不计入** `deliveries`/`max_deliveries` | ❓ 默认开启？建议是（P12） |
| 运行中抢占 | **不打断**正在执行的任务；❓ 是否需要硬抢占 / 协作式取消点 | ❓ 默认不做（P19） |
| ETA / 延迟任务 | 到点后与普通消息同池按优先级竞争；❓ 是否给到期 eta 隐式加权 | ❓ 建议否 |
| 超时 / 硬超时 / 取消 | 与优先级无关（`ctx.check_cancelled()` 是 P19 的协作式钩子） | — |

## 7. 数据模型与 claim 实现（SQLite）

~~~sql
priority   INTEGER NOT NULL DEFAULT 0,
yields     INTEGER NOT NULL DEFAULT 0,
yieldable  INTEGER NOT NULL DEFAULT 1,
CREATE INDEX idx_claim       ON messages(state, visible_at, priority DESC, id);
CREATE INDEX idx_claim_queue ON messages(state, queue, visible_at, priority DESC, id);
~~~

**全局 claim（方案 D：跨队列插队）**：

~~~sql
UPDATE messages
   SET state='reserved', claimed_by=?, claimed_at=?, lease_until=?, deliveries=deliveries+1
 WHERE id = (SELECT id FROM messages
              WHERE state='queued' AND queue IN (...) AND visible_at<=?
                AND (expires_at IS NULL OR expires_at > ?)
              ORDER BY priority DESC, visible_at ASC, id
              LIMIT 1)
RETURNING *;
~~~

**让位探测**（每个 poll 轮一次，命中 `idx_claim`）：

~~~sql
SELECT MAX(priority) FROM messages
 WHERE state='queued' AND queue IN (...) AND visible_at <= ? AND yieldable = 1;
~~~

**让位写入**（`yields` 独立计数；到顶后 `yieldable=0`）：

~~~sql
UPDATE messages
   SET state='queued', claimed_by=NULL, claimed_at=NULL, lease_until=NULL,
       visible_at = :now + :yield_delay, yields = yields + 1,
       yieldable = CASE WHEN yields + 1 >= :max_yields THEN 0 ELSE 1 END
 WHERE id = ? AND state='reserved' AND claimed_by = ?;
~~~

**平级公平**（同一优先级档位内的队列轮询）两种实现：

1. 先 `SELECT MAX(priority)` 定档，再在档内「每队列 `LIMIT 1`、按 weight 循环」，直到凑满 `limit`。
2. 单条 SQL + 窗口函数 `ROW_NUMBER() OVER (PARTITION BY priority, queue ORDER BY id)`。

❓ 选哪种？建议先 (1)，Phase 1 压测后再看要不要合成 (2)。
吞吐边界：全局 claim 是 1 次往返；「定档 + 档内轮询」是 2 次往返。队列数 < 10 时可接受，文档写明这条边界。

## 8. API / CLI 形态

~~~python
from taskmq import App, Config, Priority          # ❓ 是否暴露 Priority 常量

app = App(Config(
    default_priority=0,
    queues={"email": QueueConfig(weight=3)},      # 队列间配额；priority 只作 fallback
))

@app.task(queue="email", priority=Priority.HIGH)
def send_email(to: str) -> str: ...

send_email.delay("a@b.com")                       # 用任务默认
send_email.delay("a@b.com", priority=Priority.CRITICAL)   # 提交时覆盖
send_email.apply_async(("a@b.com",), priority=-1)

ctx.publish(other_task, priority=...)             # 显式覆盖继承
~~~

~~~bash
taskmq call myapp.tasks.send_email --args '["a@b.com"]' --priority 5
taskmq dlq replay --all --queue email --priority 0
taskmq status --by-priority                       # email: P9=3 P5=12 P0=104
~~~

## 9. 测试清单（写实现之前先立这些用例）

1. 同队列：高优先先出；同优先严格 FIFO（按 id）。
2. 数值边界：越界/非整数在 `App()` 构造期报错。
3. 解析顺序：submit > task > queue > config，四层各覆盖一次。
4. **插队 G1（空闲即最高）**：灌入 1000 条 P0 后提交 1 条 P9，**下一个空闲槽位必须执行 P9**。
5. **插队 G2（让位）**：worker 预取 8 条 P0 且全部未开始（故障注入让执行池阻塞），提交 1 条 P9 →
   最少必要数量的 P0 被让位、P9 立即执行；被让位的 P0 之后正常跑完。
6. **G3（不打断）**：正在执行的 P0 不被 P9 中断，P9 等下一个槽位。
7. **让位隔离**：反复让位的消息不会被误送 DLQ（`yields` 不计入 `deliveries`）；`max_yields` 到顶后不再让位。
8. **平级公平**：两个队列同为 P0，各灌 100 条，轮询取件比例符合 weight。
9. 重试保持优先级；子任务继承；DLQ 重放保留。
10. 并发 claim 不重复（复用 transport 不变量测试）。
11. `status --by-priority` 输出正确。
12. 若开 aging / `normal_reserved_slots`：低优在最坏等待时间内能被取到。

## 10. 决策清单（✅ 全部已定）

| # | 问题 | 建议 |
|---|---|---|
| P1 | 数值范围 | ✅ `-9..9`；越界/非整数在 `App()` 构造期抛 `ConfigError`，不 clamp |
| P2 | 比较范围 | ✅ **方案 D**：全局严格优先 + 让位 + 平级队列轮询 |
| P3 | 队列间配额 | ✅ 仅在**同优先级档位内**按 `weight`，至少 1 条；默认等权（`weight=1`） |
| P4 | aging | ✅ Phase 0/1 关闭，Phase 2 再评估 |
| P5 | 重试优先级 | ✅ 保持原优先级（`retry_priority="keep"` 默认；`"lower"` 预留为 Phase 1 可选值） |
| P6 | 子任务继承 | ✅ 默认继承父任务优先级，可显式覆盖 |
| P7 | DLQ 重放 | ✅ 保留原优先级；CLI `--priority` 可覆盖 |
| P8 | 速率限制 | ✅ 令牌先到先得，高优不插队 |
| P9 | ETA | ✅ 到点后与普通消息同池竞争，不做加权 |
| P10 | claim 实现 | ✅ 先「定档 + 档内每队列 `LIMIT 1` 循环」；Phase 1 压测后再看窗口函数版 |
| P11 | 便捷常量 | ✅ 提供 `Priority.LOW / NORMAL / HIGH / CRITICAL` 纯常量 |
| P12 | 让位默认开启 | ✅ 是；`yield_min_delta=1`（差值 ≥1 即让位） |
| P13 | 让位计数 | ✅ 独立 `yields`，**不计入** `deliveries` / `max_deliveries` |
| P14 | 让位防抖 | ✅ `yield_delay=0.1s`（+抖动）、`max_yields=100`，到顶后 `yieldable=false` |
| P15 | 发现高优的延迟 | ✅ Phase 0 只靠 poll（`poll_interval=0.05s`）；Phase 1 Redis 用 `BZPOPMIN` 接近推送 |
| P16 | reserve–start 耦合 | ✅ 只在真有空闲槽位时 reserve；`prefetch` 默认 = `concurrency`；池内队列有界 |
| P17 | 防饿死兜底 | ✅ 默认 `normal_reserved_slots=0`（只做可见性 + 告警）；Phase 2 再评估 |
| P18 | 让位与限流/串行键 | ✅ 让位不消耗速率令牌、不影响 `concurrency_key` 锁 |
| P19 | 运行中抢占 | ✅ 默认**不做**；提供协作式 `ctx.check_cancelled()` 作为 opt-in（Phase 1），硬抢占仅在 `processes` 池评估 |

> 定稿依据：用户确认「**D + 其余按建议**」。实现按本表对齐；正文 ❓ 不再作为实现依据。
