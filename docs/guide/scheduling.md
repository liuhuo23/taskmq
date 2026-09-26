# 定时调度（beat）

beat 是**独立进程**里的调度器：到点就把任务投出去。多副本靠一个命名租约（`__beat__`）选主，
只有 leader 会触发，所以可以放心多起几个。

## 声明调度

```python
from taskmq.schedule import cron, every

app.schedule(
    cron("send_report", "0 9 * * *", tz="Asia/Shanghai"),  # 每天 9:00（zoneinfo 本地时区，DST 正确）
    every("cleanup", minutes=5, misfire="run_once"),       # 每 5 分钟；错过就补一次
    every("ping", seconds=30),
)
```

- `cron(name, expr, tz=)`：标准 5 段 cron（分 时 日 月 周），时区走 `zoneinfo`；
- `every(name, seconds= / minutes= / hours=, misfire=)`：固定间隔；
- `misfire`：`"skip"`（默认，错过就跳过）/ `"run_once"`（错过补一次）；
- **首次部署只记基准不补跑**，避免上线瞬间把所有历史窗口一次性打出去。

## 起 beat

```bash
taskmq --app myapp.tasks:app beat              # 常驻
taskmq --app myapp.tasks:app beat --once       # 只推进一轮（测试 / 外部 cron 驱动）
taskmq --app myapp.tasks:app dev               # 本地开发：worker + beat 同进程
```

```python
from taskmq import App, Config
from taskmq.worker.beat import Beat

beat = Beat(app, app.schedule_entries, state_path="taskmq.beat.json")
beat.run_forever(poll=1.0)        # 或 beat.tick() 单步
```

## 状态与选主

- 调度状态（每个 entry 的 `last_run` / `next_run`）写在 `taskmq.beat.json`（`--state` 可改），**只有 leader 写**；
- leader 通过 transport 的**命名租约** `__beat__` 选出，租约过期自动换主；
- 所以 beat 需要 transport 支持 `supports_leases`（`memory/sqlite/redis/postgres` 都支持；不支持的后端启动即报错）。

## 时区

```python
Config(timezone="Asia/Shanghai")     # App 默认时区
cron("send_report", "0 9 * * *")     # 用 App 时区
cron("send_report", "0 9 * * *", tz="UTC")   # 或者逐条覆盖
```

## 下一步

- [DAG 工作流](workflows.md) ｜ [CLI 与运维](cli.md)
