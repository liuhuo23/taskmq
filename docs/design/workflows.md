# 原生 DAG 工作流设计（v1.0）

> Phase 2 第一刀：把 [design.md:45](design.md) 里那句「用**原生 DAG 工作流**替代 chain/group/chord，
> 调度器直接感知依赖，不走轮询」落地。
>
> **状态：v1.0 已落地**（D1–D9 全部取推荐项）；实现纪要、踩到的坑与语义澄清见 §8。

---

## 1. 为什么不做 Celery 式 canvas

| 维度 | Celery canvas | 原生 DAG（本设计） |
|---|---|---|
| 依赖在哪 | 藏在消息里，逐步传递（chain 字段） | **声明在 App 里**（可 review、可测试、可画图） |
| chord 怎么等 | 计数 + chord_unlock 任务 + 轮询 result backend | 依赖满足即入队，**无需计数原语、无需轮询** |
| 失败可见性 | 得拼消息链推断 | 每个节点一个 job 状态，直接查 |
| 崩溃恢复 | 消息丢了就断了 | 节点状态是事实来源，**重算 ready 节点**即可续跑（幂等） |
| 与 at-least-once | 靠 canvas 自身语义兜 | 每步都是普通任务，推进用**幂等键**去重 |

结论：DAG 做**核心原语**；Celery 的 `chain/group/chord` 以后在 `taskmq-celery` 里映射成 DAG（§19 已定：兼容层是可选包）。

---

## 2. 定义方式（声明式、可测）

```python
from taskmq.workflow import Workflow

@app.workflow("etl", queue="etl")            # 注册一个 DAG 模板
def etl(wf, source: str):                     # 提交时传参数，构建本次运行的图
    extract = wf.step("extract", extract_task, args=(source,))
    clean   = wf.step("clean",  clean_task,  deps={"rows": extract})
    stats   = wf.join("stats",  stats_task,  deps=[clean], collect="tables")
    report  = wf.step("report", report_task, deps={"stats": stats, "rows": clean})
    return report                              # 返回值 = 这次运行的"汇"（sink）
```

- `wf.step(name, task, args=..., kwargs=..., deps={参数名: 上游}, queue=..., priority=..., on_failure="fail"|"continue")`
  —— `deps` 把上游**结果**按参数名注入下游；
- `wf.join(name, task, deps=[...], collect="参数名")` —— 汇合点，按声明顺序收集成一个 list（chord 的 body 等价物）；
- 定义期就报错：重名、自依赖、环、依赖不存在的节点、`deps` 参数名与任务签名冲突（尽力而为）。

---

## 3. 运行与推进（无轮询、幂等、可恢复）

**存储布局**（复用现有 job 状态，不新增原语）：

| 东西 | 键 | 说明 |
|---|---|---|
| 运行 | @@BT@{run_id}` | 一个 job：`state=RUNNING/SUCCEEDED/FAILED`，meta 里是节点摘要 + 定义名 + 参数 |
| 节点 | @@BT@{run_id}::{node}` | 一个 job：自己的 state/result/attempt（**事实来源**） |

节点 job id 是**确定性**的，入队时带幂等键 `f"{run}::{node}"` —— 重复推进不会重复执行（at-least-once 安全）。

**推进算法**（任一 worker 都能算，无中心协调）：

```
advance(run):
  nodes = 读所有节点 job 状态
  for step in 定义:
      若 step 已有终态 → skip
      若任一依赖 FAILED  → 标记 SKIPPED（fail-fast，除 on_failure=continue）
      若所有依赖 SUCCEEDED → **入队**（幂等键 = run::node，注入依赖结果）
  总体状态 = 全 SUCCEEDED → SUCCEEDED / 有 FAILED → FAILED / 否则 RUNNING
  写回运行 job（派生缓存，节点状态才是事实来源）
```

- **触发点**：① 节点任务成功/最终失败时（worker 侧推进，延迟最低）；② worker 维护周期对 `RUNNING` 运行做**补偿推进**（幂等，修复"节点完成但没来得及推进"的窗口）；③ CLI `taskmq workflow resume <run>`。
- 需要枚举 `RUNNING` 运行 → transport 增加**可选能力** `supports_job_listing` + `list_jobs()`（memory/sqlite/redis 都实现；插件不实现则该能力位为 False，框架**启动即报错**而不是静默不推进，与现有能力校验一致）。

---

## 4. 失败与重试语义

- 节点 = 普通任务：**自己的重试策略、自己的 DLQ、自己的超时**；重试期间节点处于 `RETRYING`，下游继续等（不算终态失败）。
- 节点**最终失败**（重试耗尽 / DLQ）→ 运行 `FAILED`；其下游标记 `SKIPPED`（不执行、可查）；其余独立分支继续跑完（不做全局熔断）。
- `on_failure="continue"`：该节点失败不阻断运行，只跳过它的下游。
- 结果：`WorkflowHandle.get()` 返回 sink 节点的结果；`status()` 返回每节点 state/attempt/job_id + 总体状态。

---

## 5. 可观测性

```bash
taskmq --app myapp:app workflow list               # 运行中/最近的工作流
taskmq --app myapp:app workflow status <run_id>    # 每节点状态 + 依赖关系
taskmq --app myapp:app workflow resume <run_id>    # 补偿推进（幂等）
```

`taskmq status` 增加 `WORKFLOWS` 段（RUNNING 数量）；事件新增 `workflow.started/advanced/succeeded/failed`（沿用同一 EventSink，OTel 自动可用）。

---

## 6. ✅ 决策点（按建议定稿）

| # | 问题 | 结论 |
|---|---|---|
| D1 | 核心原语：Celery canvas vs 原生 DAG | ✅ **原生 DAG**；canvas 留给 `taskmq-celery` 映射 |
| D2 | 定义方式 | ✅ 声明式 `@app.workflow` + 构建器（`wf.step/join`），提交时构建本次图 |
| D3 | 上游结果怎么给下游 | ✅ `deps={参数名: 节点}` 按名注入；汇合用 `join(collect="参数名")` 按序收集成 list |
| D4 | 推进由谁做 | ✅ **worker 侧事件推进** + 维护期补偿 + CLI resume；**不引入独立调度进程、不轮询** |
| D5 | 去重/幂等 | ✅ 节点 job id 确定性 + 入队用幂等键 `run::node`；节点状态是事实来源 |
| D6 | 失败语义 | ✅ 默认 fail-fast（下游 SKIPPED、其他分支继续）；`on_failure="continue"` 可选 |
| D7 | 是否需要计数原语（chord） | ✅ **不需要**：join 节点按依赖满足即入队，天然解决 chord |
| D8 | 运行枚举 | ✅ transport 增加**可选** `supports_job_listing`/`list_jobs`；不支持时 workflow 功能启动即报错 |
| D9 | 动态扇出（运行时才知道的 list） | ⏳ 本轮不做（需要 `wf.map` 与运行期展开），列 Phase 2 后续 |

---

## 7. 核心改动清单

| 文件 | 改动 | 风险 |
|---|---|---|
| `taskmq/workflow.py` | **新增**：`Step/Workflow/WorkflowBuilder/WorkflowHandle`、校验（环/重名/依赖）、ready 计算、节点 payload 编解码 | 中（新模块，独立） |
| `taskmq/app.py` | `@app.workflow` 注册表、`submit_workflow`、`advance_workflow`/`resume_workflow` | 中 |
| `taskmq/transport/base.py` + 三家实现 | 可选 `supports_job_listing` / `list_jobs()` | 低（默认不支持） |
| `taskmq/worker/runner.py` | 完成/最终失败后推进；维护期补偿推进 | 中（要保证不影响单任务路径） |
| `taskmq/cli.py` | `workflow list/status/resume`；`status` 增加 WORKFLOWS 段 | 低 |
| `taskmq/testing.py` | 一致性套件加 `job_listing` 场景（声明支持才跑） | 低 |
| `tests/test_workflow*.py`、文档 | 端到端 + 幂等 + 崩溃恢复 + 失败传播 | — |

**兼容性**：单任务路径零变化（没有 workflow 头的消息走原逻辑）；新方法都有默认实现；节点就是普通任务，DLQ/重试/超时语义全部复用。

---

## 8. 实现纪要（v1.0 已落地）

**代码**

| 位置 | 内容 |
|---|---|
| `taskmq/workflow.py` | Step/Workflow/WorkflowBuilder/WorkflowPlan/PlanEvaluation/WorkflowHandle + 节点头编解码；纯求值、不做副作用 |
| `App` | `@app.workflow` / `submit_workflow` / `advance_workflow` / `resume_workflow` / `pending_workflows` / `handle_workflow` |
| transport | 可选能力 `supports_job_listing` + `list_jobs(prefix, states, limit)`（memory/sqlite/redis 已实现；插件不实现则该位为 False，**提交工作流时直接报错**） |
| worker | 节点结束后 `_advance_workflow()` 事件推进；维护期 `_reconcile_workflows()` 补偿推进（≥1s 节流） |
| CLI | `taskmq workflow list / status / resume`；`taskmq status` 增加 `WORKFLOWS` 段 |
| 一致性套件 | 新增 `job_listing` 场景（共 16 个），三家内建 transport 一起跑 |

**测试**：`tests/test_workflow.py` 16 例——线性链、扇出 + 汇合顺序、失败传播（下游 SKIPPED）、
`on_failure="continue"`、节点重试阻塞下游、推进幂等（重复推进不重复执行）、崩溃恢复（推进前崩）、
维护期补偿、定义校验 8 类、事件、跨 memory + sqlite + redis、CLI list/status/resume。

**实测抓到的两个真问题**

1. **推进时忽略了任务自带的 queue**：第一版用 `config.default_queue` 兜底 → 消息进了 default 队列，
   worker 永远看不到（节点卡在 QUEUED）。现在与单任务口径一致：
   `step.queue > 任务自带 queue > 运行级 queue > config.default_queue`；优先级同样走 `resolve_priority()`（P2）。
2. **重试被幂等键吞掉**（跨模块的真实 bug）：重试的实现是「同一个 envelope 再入队一次」
   （`enqueue(env.next_attempt())`），而 DAG 节点用**确定性幂等键**去重 → SETNX 命中旧键，
   重试消息被静默丢弃、节点永远停在第一次失败。修法：`Envelope.next_attempt()` 让幂等键**按尝试次数分代**
   （`key#n`）——生产者去重照旧，重试不再被顶掉。

**语义澄清（写给运维）**：事件推进是**即时**的；补偿推进是**周期性**的（默认 ≥1s 节流），
所以只有「推进前进程崩了」才会等下一个维护周期（≤1s 量级）——这正是它要修的问题。
`WorkflowHandle.get()` 是**客户端本地**等待（轮询状态），不是调度轮询，不影响 worker。
