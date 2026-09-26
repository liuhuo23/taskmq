# DAG 工作流

依赖**写在代码里**（可 review、可测试、可画图），推进由**节点完成事件**驱动——不轮询、没有中心协调器。

```python
from taskmq.workflow import WorkflowBuilder

`app.workflow("etl")
def etl(wf: WorkflowBuilder, source: str):                 # 参数按关键字传入
    extract = wf.step("extract", extract_task, args=(source,))
    clean   = wf.step("clean", clean_task, deps={"rows": extract})      # 上游结果按参数名注入
    stats   = wf.step("stats", stats_task, deps={"rows": clean})
    return wf.join("report", report_task, deps=[clean, stats], collect="tables")   # 汇合点

handle = app.submit_workflow("etl", {"source": "s3://bucket/2026-03-08"})
handle.status()                      # 每个节点的 state / attempt / job_id + 总体状态
handle.get(timeout=60)               # 等汇节点结果
```

## 节点怎么写

```python
wf.step(
    "clean",                       # 节点名（同一运行内唯一）
    clean_task,                    # 任务（@app.task 注册过的，或 Task 子类）
    args=(), kwargs={},            # 静态参数
    deps={"rows": extract},        # dict：上游结果注入到同名参数；list：只表示依赖顺序
    bind={"x": "extract"},         # 只注入不建依赖（少用）
    collect="tables",              # join 用：按 deps 顺序把上游结果收成 list
    queue="etl", priority=5,       # 覆盖节点级队列/优先级
    on_failure="fail",             # "fail"（默认，阻断下游）| "continue"（不阻断整条运行）
)
```

- 节点就是**普通任务**：自己的重试、DLQ、超时、优先级、队列都生效；
  解析顺序 `step > 任务自带 > 运行级 > Config 默认`；
- `wf.join(name, task, deps=[...], collect="参数名")` 是 chord 的等价物：按声明顺序收集上游结果成一个 list；
- 构建函数**每次提交都会重新执行**，所以可以按参数动态搭图；重名节点直接 `ConfigError`。

## 语义要点

| 关注点 | 行为 |
|---|---|
| **幂等推进** | 节点 job id 确定性（`{run}::{node}`）+ 幂等键 → at-least-once 下重复推进不会重复执行 |
| **失败语义** | 默认 fail-fast：节点失败 → 下游标 `SKIPPED`，其他分支照跑；`on_failure="continue"` 表示该节点失败不阻断整条运行 |
| **崩溃恢复** | 节点状态是事实来源；worker 维护期自动补偿推进未完成的运行（≥1s 节流），也可手工 `resume` |
| **运行状态** | `RUNNING` / `SUCCEEDED` / `FAILED`；`handle.status().nodes` 逐节点可见 |
| **前提** | transport 必须支持 `supports_job_listing`（memory/sqlite/redis/postgres 都支持；不支持的后端在提交时直接报错） |

## 查询与恢复

```python
status = handle.status()
status.state                                   # 总体状态
{name: node.state for name, node in status.nodes.items()}

app.resume_workflow(handle.id)                 # 补偿推进（幂等，可重复调用）
```

```bash
taskmq --app myapp.tasks:app workflow list              # 未完成的运行
taskmq --app myapp.tasks:app workflow status wf-01H...  # 逐节点状态 + 依赖
taskmq --app myapp.tasks:app workflow resume wf-01H...  # 补偿推进
```

## 完整示例

```python
from taskmq import App, Config, Retry

app = App(Config(transport="sqlite:///./taskmq.db"))

`app.task(name="etl.extract", queue="etl", retry=Retry(max_attempts=3))
def extract(source: str) -> list[dict]:
    return read_csv(source)

`app.task(name="etl.clean", queue="etl")
def clean(rows: list[dict]) -> list[dict]:
    return [normalize(r) for r in rows]

`app.task(name="etl.stats", queue="etl")
def stats(rows: list[dict]) -> dict:
    return summarize(rows)

`app.task(name="etl.report", queue="etl")
def report(tables: list) -> str:
    return render(tables)
```

设计与决策（D1–D9）见[工作流设计](../design/workflows.md)。

## 下一步

- [CLI 与运维](cli.md) ｜ [运行 worker](workers.md)
