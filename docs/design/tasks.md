# 任务定义（Task 抽象）设计（草案 v0.1）

> 分册：[../design.md](../design.md)。本文只解决「任务怎么写、能自定义什么」。
> 状态：**v1.0 已定稿并落地**（T1–T11 全部 ✅，见 §11）。触发：应像 Celery 那样支持**继承 Task 类**与
> **`bind=True`**，让用户自定义行为、拿到任务实例，从而获得更大灵活性。
> 实现：`taskmq/task.py` + `taskmq/app.py` + `taskmq/worker/runner.py`；验收：`tests/test_task_class.py`（13 例）。

## 0. 现状与问题

现在只有函数式：

~~~python
@app.task(queue="email", retry=Retry(max_attempts=5))
def send_email(to: str) -> str: ...
~~~

能配的都在装饰器参数里（§7.2），**没有任何「执行前后/失败时做点什么」的扩展点**：

- 无法在任务前后成对管理资源（DB session、租约、trace span、租户上下文、缓存预热）；
- 无法对「这一类任务」统一加行为（只能包装函数或写装饰器，而包装函数会绕开框架的注册/重试语义）；
- 无法自定义失败/重试时的动作（告警、打标、写审计）；
- 用户想复用一套「基类」（如 `TenantAwareTask`、`DBTask`）时无处安放。

## 1. 目标 / 非目标

目标：

- **可继承**：`Task` 是公开基类，用户可 subclass / 用 mixin 组合出项目自己的任务基类。
- **生命周期钩子**：在明确的位置、以明确的顺序、用类型化签名暴露扩展点。
- **两种写法等价**：`@app.task`（函数式）是糖，内部就是「生成一个 Task 子类并注册」；两种写法能力一致。
- **不牺牲可预测性**：钩子不允许改变投递语义（ack/重试/DLQ 的决策只由 `Retry`/`ack`/`max_deliveries` 决定）。
- **类型友好**（P8）：`Task` 泛型化，`run` 的签名与返回值可被 mypy/pyright 推断。

非目标：

- **`bind=True` 采纳**（见 §3.5）：函数式任务的首参注入任务实例 `self`；但 `self.request` **不做**
  Celery 那种无类型代理，它直接返回**类型化的 `TaskContext`**（`ctx` 的别名），执行期外访问立刻抛
  `TaskError`，不猜、不返回 `None`。
- 不做「钩子里改 args/kwargs」这种隐式改写（Celery 也没有，容易写出难查的 bug）。
- 不做 `self.retry()` 的隐式抛错（Celery 默认自己 raise）：统一「返回异常对象、由调用方 `raise`」（T9）。
- 不做 Celery 的 `Task.replace()` 之类工作流原语（Phase 2 DAG 负责）。

## 2. Celery 的 Task 类给了什么：逐条对照

| Celery 能力 | 作用 | taskmq 采纳 | 落地形式 |
|---|---|---|---|
| `class MyTask(Task)` + `run()` | 类式定义任务 | ✅ | `Task.run(self, *args, **kwargs)` |
| `@app.task(base=MyTask)` | 用自定义基类装饰函数 | ✅ | 生成 `type(name, (base,), {"run": ...})` |
| mixin 组合（`class T(TenantMixin, Task)`） | 共享横切行为 | ✅ | 普通 Python 多继承 |
| `before_start` / `on_success` / `on_retry` / `on_failure` / `after_return` | 生命周期钩子 | ✅ | 见 §4（签名统一带 `ctx`） |
| `bind=True` + `self` | 函数式任务拿到任务实例 | ✅ | `@app.task(bind=True)`，首参注入 `self`（§3.5） |
| `self.request`（id/retries/args/…） | 请求上下文 | ✅ | `self.request` → 类型化的 `TaskContext`（`ctx.id/attempt/retries/args/kwargs`） |
| `self.retry()` | 手动重试（异常控制流） | ✅ 已有 | `raise ctx.retry(reason=...)` |
| `autoretry_for` | 白名单重试 | ✅ 已有 | `Retry(retry_on=(...))` |
| `self.update_state(state, meta)` | 进度上报 | ✅ | `ctx.update_meta(**kw)` |
| `acks_late` / `task_acks_on_failure_or_timeout` | ack 时机 | ✅ 已有 | `@task(ack="on_success" \| "on_receipt" \| "on_completion")` |
| `max_retries` / `default_retry_delay` / `retry_backoff` | 重试策略 | ✅ 已有 | `Retry(...)` |
| `ignore_result` | 结果开关 | ✅ 已有 | `@task(store_result=False)` |
| `rate_limit` / `priority` / `queue` / `expires` | 静态策略 | ✅ 已有 | 装饰器参数 / 类属性 |
| `Task.replace()`、chord/chain | 工作流 | ❌ | Phase 2 原生 DAG |
| 实例是**进程内单例** | `__init__` 放进程级资源（连接池） | ✅（并写清风险） | 见 §6 |

## 3. 两种写法（等价）

~~~python
from taskmq import App, Task, Retry, Priority

app = App(...)

# 写法一：类式（可继承、可放进程级资源、可覆写钩子）
class EmailTask(Task):
    queue = "email"
    priority = Priority.HIGH
    retry_policy = Retry(max_attempts=5, backoff="exp")   # 策略叫 retry_policy；retry 留给 self.retry()

    def __init__(self, app):          # 每进程一次：连接池、客户端等
        super().__init__(app)
        self.smtp = SmtpClient()

    def before_start(self, ctx):
        self.smtp.acquire()

    def run(self, to: str, subject: str) -> str:
        return self.smtp.send(to, subject)

    def on_failure(self, ctx, exc):
        alert(f"发送失败 {ctx.id}: {exc!r}")

    def after_return(self, ctx, state, result=None, exc=None):
        self.smtp.release()

app.register(EmailTask)               # name 默认 "module.EmailTask"

# 写法二：函数式（糖 → 内部生成 Task 子类并注册）
@app.task(queue="email", retry="default")
def send_email(to: str, subject: str) -> str:
    return smtp_send(to, subject)

# 写法二 + 自定义基类：函数体只写业务，横切行为来自基类
class TenantAwareTask(Task):
    def before_start(self, ctx):
        ctx.tenant = ctx.kwargs.get("tenant")     # 请求级状态放 ctx，不放在 self
        set_current_tenant(ctx.tenant)

@app.task(base=TenantAwareTask, queue="report")
def build_report(month: str) -> str: ...

# 写法三：bind=True —— 函数式任务也能拿到任务实例（Celery 手感）
@app.task(bind=True, queue="email", retry="default")
def send_email(self, to: str, subject: str) -> str:
    self.request.log.info("sending", to=to, attempt=self.request.attempt)
    if rate_limited():
        raise self.retry(countdown=30, reason="rate limited")   # 返回异常 → raise（T9）
    return smtp_send(to, subject)
~~~

**类属性 ↔ 装饰器参数对应关系**（同名同义，后者覆盖前者）：

| 类属性 | 装饰器参数 | 说明 |
|---|---|---|
| `name` / `queue` / `priority` / `timeout` / `ack` / `expires` / `max_deliveries` / `rate_limit` / `concurrency_key` / `serializer` / `store_result` | 同名 | 与 §7.2 完全一致，不引入第二套命名 |
| **`retry_policy`** | `retry=` | 唯一不同名的一处：`retry` 这个名字留给方法 `self.retry()`（Celery 手感），策略对象叫 `retry_policy` |

> `delay()` 只接受**任务参数**（因此参数有静态检查）；`queue/priority/eta/key/...` 这些调度选项走
> `apply_async()`（显式分离，避免保留选项名和任务 kwargs 撞车）。`submit()` 保留为「两者混用」的逃生口。

### 3.5 `bind=True` 语义（核心能力）

- `bind=True` 时，用户函数的**第一个位置参数是任务实例 `self`**；生产者侧 `delay/submit/apply_async`
  的 args **不含** `self`（`send_email.delay("a@b.com")` → `def send_email(self, to)`）。
- `self` 就是注册表里**每进程一个**的实例（T6 方案 A）：**只放进程级资源**（连接池、HTTP 客户端、
  编译好的模板）；请求级状态一律放 `self.request` / `ctx`，绝不放在 `self` 上。
- `self.request` **就是当前执行的 `TaskContext`**（别名，不是代理、不会是 `None`）；执行期外访问抛
  `TaskError`。为对齐 Celery 手感，`TaskContext` 增补：
  `ctx.args` / `ctx.kwargs`（只读）、`ctx.retries`（= `attempt - 1`）、`ctx.hostname`（worker id）、
  `ctx.update_meta(**kw)`（进度/阶段上报）。
- `self.retry(...)`：**返回** `RetryRequest`，需要 `raise`；与 `ctx.retry()` 同义。接受 Celery 味道的别名：
  `countdown=秒`（= `delay`）、`exc=异常`（仅记录）、`reason=文本`、`max_retries=次数`（❓ T9）。
- 调用约定对照：

  | 生产者 | 任务实现 |
  |---|---|
  | `send_email.delay("a@b.com")` | `def send_email(self, to: str) -> str`（`bind=True`） |
  | `send_email.apply_async(("a@b.com",), {"x": 1})` | `def send_email(self, to: str, x: int = 0)` |
  | 类式 `app.register(EmailTask)` | `def run(self, to: str) -> str`（`self` 天然存在，不需要 bind） |

- 类型：`App.task` 用重载把 `self` 从 `delay()` 的签名里排除（`Concatenate[Task, P]`，§8），
  mypy 与 pyright 都验证过。

## 4. 生命周期与钩子（顺序固定，写进文档 + 测试）

| 顺序 | 钩子 | 签名 | 何时 | 线程 |
|---|---|---|---|---|
| 1 | `before_start` | `(self, ctx) -> None` | reserve + 状态置 RUNNING 之后、`run` 之前 | 池线程 |
| 2 | `run` | `(self, *args, **kwargs) -> T` | 唯一必须实现的方法 | 池线程 |
| 3a | `on_success` | `(self, ctx, result, runtime) -> None` | 成功、状态置 SUCCEEDED 之后 | 池线程 |
| 3b | `on_failure` | `(self, ctx, exc) -> None` | 失败且**已决定**进 DLQ 时 | 池线程 |
| 3c | `on_retry` | `(self, ctx, exc, delay) -> None` | 失败且**已决定**重试、新消息入队之后 | 池线程 |
| 4 | `after_return` | `(self, ctx, state, result=None, exc=None) -> None` | `finally` 语义，**必然会跑** | 池线程 |

- `ctx` 新增 `ctx.args` / `ctx.kwargs`（只读）/`ctx.update_meta(**kw)`（写 JobRecord.meta，进度用）。
- 钩子**不能**改 args/kwargs、不能改 ack 决策、不能决定重试（重试只由 `Retry`/`ctx.retry()` 决定）。
- 钩子与 `run` 在**同一线程**执行，保证 `before_start` 里 set 的 contextvar / 资源在 `run` 与 `after_return` 中可见。

## 5. 钩子抛异常怎么办（必须显式，不能含糊）

| 钩子 | 抛异常的行为 | 理由 |
|---|---|---|
| `before_start` | 视为**任务执行失败**（走正常失败路径：可能重试 / DLQ） | 资源没准备好，`run` 不应执行；这是「业务前置条件」 |
| `on_success` / `on_failure` / `on_retry` | **不改投递语义**：只记 `hook_error` 事件 + 在 meta 打标 | 投递语义（P3）不能被通知类钩子改变，否则「失败已被判定」又被搅乱 |
| `after_return` | 只记日志/事件 | 它在 `finally` 里，抛异常无处可去 |

❓ T5：是否希望提供「严格模式」（钩子异常 → 任务失败）？建议默认宽松，配置项 `hook_errors_fatal=False`。

## 6. 实例生命周期与状态边界（❓ 关键决策）

Celery 的 Task 实例是**进程内单例**：好处是 `__init__` 可以放连接池；坏处是大家把请求级状态放到 `self`，
在多线程/重试下互相串（Celery 官方文档反复警告）。

两个选项：

| 方案 | 语义 | 优点 | 代价 |
|---|---|---|---|
| **A（推荐，贴近 Celery）** | 每个进程/注册表一个实例，`__init__(app)` 只放**进程级资源** | 连接池/客户端只建一次；兼容层映射直接 | `self` 上放请求态会串；文档 + 代码审查约束 |
| B | 每次投递新建实例 | 无共享状态，天然线程安全 | 每次调用重建资源；`__init__` 放连接池就失效，灵活性反而下降 |

建议 A + 硬约束：**请求级状态一律放 `ctx`**（并提供 `ctx.tenant = ...` 这样的自由属性；`bind=True` 的
`self` 同样只放进程级资源），
`Task.__init__` 的文档字符串里写明「不要在这里放请求状态」；测试里加一个并发用例验证 `self` 不被请求态污染。

## 7. 与其它机制的关系

| 机制 | 规则 |
|---|---|
| ack 策略 | 钩子不参与；`on_receipt` 时 ack 发生在 `before_start` 之前（与现在一致） |
| 重试 | `on_retry` 在新消息入队**之后**调用；重试保持原优先级（P5）不变 |
| `ctx.retry()` / `Reject` | 分别触发 `on_retry` / `on_failure` |
| 幂等键 | 幂等命中时**不执行任何钩子**（没有真正投递） |
| `ctx.publish()` | 子任务默认继承父优先级（P6）不变 |
| 硬超时/取消（Phase 1） | `processes` 池被 kill 时 `after_return` **不会**执行 → 文档必须写明「不要依赖 `after_return` 做关键清理」 |
| 中间件（Phase 2） | 全局/跨任务的横切用中间件，per-task 的用钩子；两者顺序：中间件包裹钩子+run |

## 8. 类型友好

~~~python
class Task(Generic[P, R]):
    def run(self, *args: P.args, **kwargs: P.kwargs) -> R: ...

class EmailTask(Task[[str, str], str]):     # 写法可选，不强制
    def run(self, to: str, subject: str) -> str: ...
~~~

```python
class App:
    @overload
    def task(self, func: Callable[Concatenate[Task[P, R], P], R], *, bind: Literal[True], **opts) -> Task[P, R]: ...
    @overload
    def task(self, func: Callable[P, R], *, bind: Literal[False] = False, **opts) -> Task[P, R]: ...
```

- 所以 `@app.task(bind=True)` 下，`send_email.delay(...)` 的参数列表里**没有** `self`，IDE 补全正确；
- `@app.task` 生成的类保持原函数签名 → `h.get()` 可推断为 `R`；
- `app.register(EmailTask)` 时校验 `run` 是否可调用、`name` 是否冲突；
- `mypy` + `pyright` 双引擎保持 0 error（与现状一致）。

## 9. Celery 兼容层（Phase 2）受益

核心支持类式任务后，`taskmq-celery` 的映射变薄：

| Celery | taskmq |
|---|---|
| `class T(Task)` + `run` | 同名基类；`base=taskmq.Task` |
| `bind=True`，函数首参 `self` | **1:1 直接支持**（核心已做） |
| `self.request.id/retries/args` | **1:1**（`self.request` = `TaskContext`） |
| `self.retry(exc=..., countdown=...)` | **1:1**（核心 `self.retry` 接受 `countdown`/`exc`） |
| `self.update_state` | `self.request.update_meta(...)` |
| `on_failure(self, exc, task_id, args, kwargs, einfo)` | 适配器把 5 参映射成 `on_failure(self, ctx, exc)` |

## 10. 测试清单（写实现前先立）

1. 类式任务：`app.register` + `delay` + 结果往返。
2. `base=` 混入：`@app.task(base=TenantAwareTask)` 的 `before_start` 在 `run` 之前生效，`ctx.tenant` 可读。
3. 钩子顺序：用 recorder 断言 `before_start → run → on_success → after_return`；失败时 `before_start → run → on_retry/on_failure → after_return`。
4. `after_return` 必然执行：`run` 抛异常 / 重试 / 成功三种路径都跑。
5. 钩子异常策略：`on_failure` 抛异常 → 任务仍是 FAILED 且进 DLQ（不被钩子改写）；`before_start` 抛异常 → 走重试/DLQ。
6. 实例状态边界：并发 8 + 100 个任务，`self` 上的进程级资源不串；请求态只在 `ctx`。
7. `@app.task` 生成的类与手写类能力等价（同一套钩子/属性）。
8. 类型：`mypy` + `pyright` 对 `run` 签名与 `h.get()` 推断正确。
9. 幂等命中不触发钩子；`on_receipt` 时 ack 早于 `before_start`。
10. `bind=True`：`delay` 的 args 不含 `self`；`self.request.id/retries/args/kwargs` 正确；执行期外访问抛 `TaskError`。
11. `bind=True` + `base=` 组合：基类钩子生效，`self` 上的进程级资源被复用。
12. `raise self.retry(countdown=30)` 走重试，且实际退避时间 ≈ 30s；`on_retry` 收到该 delay。
13. 类型：`bind=True` 下 mypy/pyright 不把 `self` 算进 `delay()` 参数。

## 11. 决策清单（✅ 全部已定，且已实现）

| # | 决策 | 结论（已实现） |
|---|---|---|
| T1 | 类式任务进核心 | ✅ `Task` 是可继承基类；`app.register(cls)` 注册；`@app.task` 是糖，两者能力等价 |
| T2 | `bind=True` / `self.request` | ✅ 首参注入任务实例 `self`；`self.request` = 当前 `TaskContext`；执行期外抛 `TaskError` |
| T3 | 钩子集合 | ✅ `before_start / on_success / on_retry / on_failure / after_return`（顺序与签名见 §4） |
| T4 | 钩子能否改参数/决策 | ✅ 不能：args/kwargs 只读，ack/重试/DLQ 决策不参与 |
| T5 | 钩子异常 | ✅ 默认宽松（只记 `hook_error` 到 meta，不改投递语义）；`before_start` 例外 = 任务失败 |
| T6 | 实例生命周期 | ✅ 方案 A：每进程一个实例；`__init__(app)` 放进程级资源，请求态只放 `ctx` / `self.request` |
| T7 | `ctx.update_meta` | ✅ 提供（写 JobRecord.meta，进度/阶段上报） |
| T8 | 落地阶段 | ✅ Phase 0 已落地 |
| T9 | `self.retry()` 语义 | ✅ 返回 `RetryRequest`（需 `raise`），接受 `countdown`/`exc`/`reason`/`max_retries` 别名 |
| T10 | 装饰器直接挂钩子 | ✅ `@app.task(on_success=fn, on_failure=fn, ...)` 可用（内部生成带钩子的动态基类） |
| T11 | `self.request` 返回什么 | ✅ 直接返回 `TaskContext`，不做代理对象 |

**验证证据**：`tests/test_task_class.py` 13 个用例（钩子顺序、`after_return` 必然执行、`bind` 注入、
`base=` 混入、钩子异常不改语义、`self.retry(countdown=…)`、`update_meta`、未实现 `run` 报错）；
`mypy` + `pyright` 双引擎对 `@app.task` / `@app.task(...)` / `bind=True` 的 `delay()` 参数与返回类型
推断为 0 error（`bind=True` 时 `self` 不出现在 `delay()` 参数里）。
