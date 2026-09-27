"""App：任务注册表、传输/编解码装配、`submit` 入口。

设计约束（docs/design.md §7.1）：
- 构造即校验；未知配置项直接抛 `ConfigError`。
- 无全局单例：可以有多个 App；`current_app()` 只在任务执行上下文中有效。
- 导入期零副作用：`include=[...]` 只做 import。

任务定义支持两种等价写法（docs/design/tasks.md）：
`@app.task(...)`（函数式，`bind=True` 时首参注入 `self`）与 `app.register(TaskSubclass)`（类式）。
"""
from __future__ import annotations

import dataclasses
import logging
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any, Literal, TypeVar, cast, overload

if sys.version_info >= (3, 10):     # 3.10+ 用标准库
    from typing import Concatenate, ParamSpec
else:                               # pragma: no cover - 3.9（typing_extensions 是 <3.10 的条件依赖）
    from typing_extensions import Concatenate, ParamSpec

from .config import Config
from .errors import ConfigError, TaskError, WorkflowError, error_text
from .events import EventSink, build_sink
from .plugins import (
    BUILTIN_TRANSPORTS,
    TransportOptions,
    env_plugins,
    load_plugin,
    load_plugins,
    transport_factory,
    unknown_scheme_message,
)
from .priority import validate_priority
from .protocol import ACK_ON_SUCCESS, ACK_STRATEGIES, Codec, CodecRegistry, Envelope, get_codec, new_ulid
from .schedule import Schedule
from .task import Task, TaskContext, TaskHandle, _reset_current, _set_current, run_hook
from .transport.base import JobState, Transport
from .transport.factory import build_transport
from .workflow import (
    RUN_META_WORKFLOW,
    RUN_PREFIX,
    Workflow,
    WorkflowHandle,
    WorkflowPlan,
    encode_node_header,
    run_meta,
)

P = ParamSpec("P")
R = TypeVar("R")

logger = logging.getLogger("taskmq.app")

_HOOK_NAMES = ("before_start", "on_success", "on_retry", "on_failure", "after_return")


class _TaskFactory:
    """`@app.task(...)` 的装饰器对象。

    `__call__` 是**泛型方法**，所以 `@app.task(queue="x")` 也能把函数的参数类型与返回类型
    推断进 `Task[P, R]`（`delay()` 因此有类型）。
    """

    def __init__(self, app: App, options: dict[str, Any], *, bind: bool = False) -> None:
        self._app = app
        self._options = options
        self._bind = bind

    def __call__(self, func: Callable[P, R]) -> Task[P, R]:
        return cast("Task[P, R]", self._app._register(func, bind=self._bind, **self._options))


class _BoundTaskFactory(_TaskFactory):
    """`bind=True` 版本：函数首参会被注入任务实例 `self`，因此从 `delay()` 的参数里排除。"""

    def __call__(self, func: Callable[Concatenate[Task, P], R]) -> Task[P, R]:  # type: ignore[override]
        return cast("Task[P, R]", self._app._register(func, bind=True, **self._options))


def _make_hook(name: str, handler: Callable[..., Any]) -> Callable[..., Any]:
    if not callable(handler):
        raise ConfigError(f"{name} 钩子必须是可调用对象，收到 {handler!r}")

    def hook(self: Task, *args: Any, **kwargs: Any) -> Any:
        return handler(*args, **kwargs)

    hook.__name__ = name
    return hook


class App:
    """应用容器。一个进程里可以有多个（测试友好）。"""

    def __init__(self, config: Config | None = None, *, include: Sequence[str] | None = None) -> None:
        self.config = config if config is not None else Config.from_env()
        if not isinstance(self.config, Config):
            raise ConfigError(f"config 必须是 Config，收到 {type(self.config).__name__}")
        self._plugins: list[str] = []
        # 显式声明的插件（include= 与 TASKMQ_PLUGINS=）先加载：它们是用户明确要求的，
        # 而且可能注册了 events / pool / codec，必须在 config.validate() / build_sink() 之前生效。
        self._load_declared_plugins(include)
        self.config.validate()
        self._codec_registry = CodecRegistry()
        self._tasks: dict[str, Task[Any, Any]] = {}
        self._transport: Transport | None = None
        self._codec: Codec | None = None
        # 懒加载属性要防并发首用：Web 服务里多线程同时第一次 delay() 时，
        # 没有这把锁会各自造一个 transport（sqlite 下直接 "database is locked"）。
        # **必须是可重入锁**：transport 的懒加载内部会取 self.codec（_make_transport），
        # 用非重入 Lock 会在同一线程里自己卡死。
        self._lazy_lock = threading.RLock()
        self._closed = False
        self._sinks: list[EventSink] = [build_sink(self.config.events)]
        self._schedules: list[Schedule] = []
        self._workflows: dict[str, Workflow] = {}

    # -------------------------------------------------------------- 注册表
    @property
    def tasks(self) -> Mapping[str, Task[Any, Any]]:
        return dict(self._tasks)

    def task_for(self, name: str) -> Task[Any, Any] | None:
        return self._tasks.get(name)

    # ---------------------------------------------------------------- 定义
    @overload
    def task(self, func: Callable[P, R], **options: Any) -> Task[P, R]: ...

    @overload
    def task(
        self, func: None = None, *, bind: Literal[True], **options: Any
    ) -> _BoundTaskFactory: ...

    @overload
    def task(self, func: None = None, *, bind: bool = False, **options: Any) -> _TaskFactory: ...

    def task(self, func: Any = None, **options: Any) -> Any:
        """`@app.task` / `@app.task(queue=..., bind=True, base=..., on_failure=...)`。"""
        bind = bool(options.pop("bind", False))
        if func is None:
            factory = _BoundTaskFactory if bind else _TaskFactory
            return factory(self, options, bind=bind)
        return self._register(func, bind=bind, **options)

    def register(self, target: type[Task] | Task, **options: Any) -> Task[Any, Any]:
        """注册**类式**任务：`app.register(EmailTask)`（或已构造好的实例）。"""
        if isinstance(target, Task):
            task: Task[Any, Any] = target
        elif isinstance(target, type) and issubclass(target, Task):
            if target.run is Task.run:
                raise ConfigError(f"{target.__name__} 必须实现 run()")
            task = target(self, **options)
        else:
            raise ConfigError(f"register() 需要 Task 子类或 Task 实例，收到 {target!r}")
        return self._add(task)

    def _register(self, func: Callable[..., Any], *, bind: bool = False, **options: Any) -> Task[Any, Any]:
        base = options.pop("base", None)
        hooks = {name: options.pop(name) for name in _HOOK_NAMES if name in options}
        if base is not None and not (isinstance(base, type) and issubclass(base, Task)):
            raise ConfigError(f"base 必须是 Task 子类，收到 {base!r}")

        task: Task[Any, Any]
        if base is None and not hooks:
            task = Task(self, func, bind=bind, **options)
        else:
            task_cls = self._build_task_class(base or Task, func, bind=bind, hooks=hooks)
            task = task_cls(self, **options)
        return self._add(task)

    @staticmethod
    def _build_task_class(
        base: type[Task],
        func: Callable[..., Any] | None,
        *,
        bind: bool,
        hooks: Mapping[str, Callable[..., Any]],
    ) -> type[Task]:
        namespace: dict[str, Any] = {}
        class_name = base.__name__
        if func is not None:
            if bind:
                def run(self: Task, *args: Any, **kwargs: Any) -> Any:
                    return func(self, *args, **kwargs)
            else:
                def run(self: Task, *args: Any, **kwargs: Any) -> Any:
                    return func(*args, **kwargs)

            namespace["run"] = run
            class_name = getattr(func, "__name__", base.__name__)
            namespace["__module__"] = getattr(func, "__module__", base.__module__)
            namespace["__qualname__"] = getattr(func, "__qualname__", class_name)
        for hook_name, handler in hooks.items():
            namespace[hook_name] = _make_hook(hook_name, handler)
        return type(class_name, (base,), namespace)

    def _add(self, task: Task[Any, Any]) -> Task[Any, Any]:
        if not task.name:
            raise ConfigError("任务必须有稳定的 name（默认 module.qualname，或显式 name=…）")
        if task.priority is not None:
            validate_priority(task.priority, where=f"task {task.name} priority")
        existing = self._tasks.get(task.name)
        if existing is not None and existing is not task:
            raise ConfigError(f"任务名重复：{task.name}")
        self._tasks[task.name] = task
        return task

    def schedule(self, *entries: Schedule) -> None:
        """注册定时调度（`taskmq beat` / `taskmq dev` 用，§13）。"""
        for entry in entries:
            if not isinstance(entry, Schedule):
                raise ConfigError(f"schedule() 需要 Schedule 对象（用 cron()/every() 构造），收到 {entry!r}")
            self._schedules.append(entry)

    @property
    def schedules(self) -> list[Schedule]:
        return list(self._schedules)

    # ---------------------------------------------------------------- 插件
    @property
    def plugins(self) -> tuple[str, ...]:
        """已显式加载的插件模块（进程池子进程要靠它重建同样的注册表）。"""
        return tuple(dict.fromkeys(self._plugins))

    def load_plugins(self, modules: Sequence[str] | str) -> list[str]:
        """显式加载插件模块（支持 "a,b" 形式）。见 docs/design/plugins.md。"""
        if isinstance(modules, str):
            items = [item.strip() for item in modules.split(",") if item.strip()]
        else:
            items = list(modules)
        loaded = load_plugins(items, entry_points=False)
        for module in loaded:
            if module not in self._plugins:
                self._plugins.append(module)
        return loaded

    def _load_declared_plugins(self, include: Sequence[str] | None) -> None:
        for module in list(include or ()) + env_plugins():
            loaded = load_plugin(module)
            if loaded not in self._plugins:
                self._plugins.append(loaded)

    # -------------------------------------------------------------- DAG 工作流
    @property
    def workflows(self) -> Mapping[str, Workflow]:
        return dict(self._workflows)

    def workflow(
        self,
        name: str,
        *,
        queue: str | None = None,
        priority: int | None = None,
        description: str = "",
    ) -> Callable[[Callable[[Any, Mapping[str, Any]], Any]], Workflow]:
        """注册一个 DAG 模板（docs/design/workflows.md）。

        ```python
        @app.workflow("etl")
        def etl(wf, source):
            extract = wf.step("extract", extract_task, args=(source,))
            clean = wf.step("clean", clean_task, deps={"rows": extract})
            return wf.join("report", report_task, deps=[clean], collect="tables")
        ```
        """

        def decorator(build: Callable[[Any, Mapping[str, Any]], Any]) -> Workflow:
            if name in self._workflows:
                raise ConfigError(f"工作流名重复：{name}")
            definition = Workflow(
                name=name,
                build=build,
                queue=queue,
                priority=priority,
                description=description,
            )
            self._workflows[name] = definition
            return definition

        return decorator

    def submit_workflow(
        self,
        name: str,
        params: Mapping[str, Any] | None = None,
        *,
        queue: str | None = None,
        priority: int | None = None,
    ) -> WorkflowHandle:
        """提交一次运行：写运行记录 → 幂等推进（入队所有 ready 节点）。"""
        definition = self._workflows.get(name)
        if definition is None:
            raise ConfigError(f"未注册的工作流 {name!r}；已注册 {sorted(self._workflows)}")
        if not getattr(self.transport, "supports_job_listing", False):
            raise ConfigError(
                f"{type(self.transport).__name__} 不支持 job 枚举（supports_job_listing=False），"
                "DAG 工作流的补偿推进依赖它；请用 memory/sqlite/redis 或让插件实现 list_jobs()"
            )
        plan = definition.instantiate(params or {})
        if queue is not None or priority is not None:        # 运行级默认（step/任务都没指定时生效）
            plan = dataclasses.replace(
                plan,
                queue=queue if queue is not None else plan.queue,
                priority=priority if priority is not None else plan.priority,
            )
        run_id = f"{RUN_PREFIX}{new_ulid()}"
        self.transport.set_state(run_id, JobState.RUNNING, task=f"workflow:{name}", **run_meta(plan))
        self.emit(
            "workflow.started", workflow=name, run=run_id, nodes=[step.name for step in plan.steps]
        )
        self.advance_workflow(run_id, plan=plan)
        return WorkflowHandle(self.transport, run_id, plan)

    def handle_workflow(self, run_id: str) -> WorkflowHandle:
        """按运行 id 拿回句柄（重新实例化 plan）。"""
        return WorkflowHandle(self.transport, run_id, self._load_plan(run_id))

    def resume_workflow(self, run_id: str) -> str:
        """补偿推进（幂等）：崩溃恢复 / 手工续跑用。返回运行状态。"""
        return self.advance_workflow(run_id)

    def advance_workflow(self, run_id: str, *, plan: WorkflowPlan | None = None) -> str:
        """读节点状态 → 入队 ready → 标记 skipped → 写回运行状态。**幂等**。"""
        if plan is None:
            plan = self._load_plan(run_id)
        states, results = self._observe_workflow(run_id, plan)
        evaluation = plan.evaluate(states)
        queued: list[str] = []
        for step in evaluation.ready:
            node_job_id = plan.node_job_id(run_id, step.name)
            args, kwargs = plan.payload(step, results)
            node_task = self.task_for(step.task)
            # 队列/优先级优先级：step 显式 > 任务自带 > 运行级默认 > config 默认
            target_queue = (
                step.queue
                or (node_task.queue if node_task is not None else None)
                or plan.queue
                or self.config.default_queue
            )
            node_priority = step.priority
            if node_priority is None and node_task is not None:
                node_priority = node_task.priority
            if node_priority is None:
                node_priority = plan.priority
            # 解析顺序同单任务（P2）：step > 任务 > 运行级 > QueueConfig > Config 默认
            resolved_priority = self.resolve_priority(
                node_task, queue=target_queue, priority=node_priority
            )
            envelope = Envelope(
                id=node_job_id,
                task=step.task,
                args=args,
                kwargs=kwargs,
                queue=target_queue,
                priority=resolved_priority,
                key=node_job_id,                     # 幂等键：重复推进不会重复执行
                headers=encode_node_header(run_id, plan.workflow, step.name),
            )
            self.transport.enqueue(envelope, queue=target_queue)
            queued.append(step.name)
        for name in evaluation.skipped:
            self.transport.set_state(
                plan.node_job_id(run_id, name),
                JobState.SKIPPED,
                task=plan.by_name[name].task,
                error="上游节点最终失败（fail-fast），本节点不会执行",
            )
        self.transport.set_state(
            run_id, evaluation.state, task=f"workflow:{plan.workflow}", **run_meta(plan)
        )
        if queued:
            self.emit(
                "workflow.advanced",
                workflow=plan.workflow,
                run=run_id,
                nodes=queued,
                state=evaluation.state,
            )
        if evaluation.terminal:
            self.emit(
                "workflow.succeeded"
                if evaluation.state == JobState.SUCCEEDED
                else "workflow.failed",
                workflow=plan.workflow,
                run=run_id,
                state=evaluation.state,
            )
        return evaluation.state

    def pending_workflows(self, *, limit: int = 20) -> list[str]:
        """还没结束的运行 id（worker 维护期补偿推进 / CLI 列表用）。"""
        if not getattr(self.transport, "supports_job_listing", False):
            return []
        records = self.transport.list_jobs(
            prefix=RUN_PREFIX, states=[JobState.RUNNING], limit=limit
        )
        return [record.job_id for record in records]

    def _observe_workflow(
        self, run_id: str, plan: WorkflowPlan
    ) -> tuple[dict[str, str], dict[str, Any]]:
        states: dict[str, str] = {}
        results: dict[str, Any] = {}
        for step in plan.steps:
            record = self.transport.get_state(plan.node_job_id(run_id, step.name))
            if record is None:
                continue
            states[step.name] = record.state
            if record.has_result:
                results[step.name] = record.result
        return states, results

    def _load_plan(self, run_id: str) -> WorkflowPlan:
        record = self.transport.get_state(run_id)
        if record is None:
            raise WorkflowError(f"找不到工作流运行 {run_id}")
        name = str(record.meta.get(RUN_META_WORKFLOW, ""))
        definition = self._workflows.get(name)
        if definition is None:
            raise ConfigError(f"运行 {run_id} 引用的工作流 {name!r} 未注册")
        return definition.instantiate(record.meta.get("params") or {})

    def add_sink(self, sink: EventSink) -> None:
        """挂事件接收器（测试用 `CollectingSink`；OTel 适配器实现同一协议）。"""
        self._sinks.append(sink)

    def emit(self, event: str, **fields: Any) -> None:
        """发一条结构化事件；sink 抛异常不影响主流程。"""
        if not self._sinks:
            return
        payload: dict[str, Any] = {"ts": time.time(), "event": event, **fields}
        for sink in self._sinks:
            try:
                sink.emit(payload)
            except Exception:  # pragma: no cover - 事件不能影响业务
                logger.exception("事件 sink 抛异常（已忽略）：%s", event)

    def register_codec(
        self,
        type_: type,
        encode: Callable[[Any], Any],
        decode: Callable[[Any], Any],
        *,
        tag: str | None = None,
    ) -> None:
        """注册自定义类型的编解码；未注册的类型在**编码期**报错。"""
        self._codec_registry.register(type_, encode, decode, tag=tag)

    # ------------------------------------------------------- codec / transport
    @property
    def codec(self) -> Codec:
        if self._codec is None:
            with self._lazy_lock:
                if self._codec is None:
                    self._codec = get_codec(self.config.serializer, self._codec_registry)
        return self._codec

    @property
    def transport(self) -> Transport:
        if self._transport is None:
            with self._lazy_lock:      # 双重检查：并发首次提交只能装配出一个 transport
                if self._transport is None:
                    self._transport = self._make_transport()
        return self._transport

    def _transport_options(self, url: str) -> TransportOptions:
        return TransportOptions(
            url=url,
            codec=self.codec,
            codec_registry=self._codec_registry,
            max_message_bytes=self.config.max_message_bytes,
            idempotency_ttl=self.config.idempotency_ttl,
            config=self.config,
        )

    def _make_transport(self) -> Transport:
        url = self.config.transport
        if not isinstance(url, str):
            return url
        scheme = url.split("://", 1)[0]
        # 插件显式覆盖内建（override=True）时优先；否则内建优先（默认行为不变）
        if scheme in BUILTIN_TRANSPORTS:
            overriding = transport_factory(scheme)
            if overriding is not None:
                return overriding(self._transport_options(url))
            # 内建 scheme 统一走工厂（App 与 AMQP 的状态侧车共用同一套 URL 解析）
            return build_transport(
                url,
                codec=self.codec,
                registry=self._codec_registry,
                idempotency_ttl=self.config.idempotency_ttl,
                max_message_bytes=self.config.max_message_bytes,
                queue_weights={name: q.weight for name, q in self.config.queues.items()},
            )
        # 非内建：查注册表；仍未命中则做一次 entry point 懒发现（标准部署不 import 第三方包）
        options = TransportOptions(
            url=url,
            codec=self.codec,
            codec_registry=self._codec_registry,
            max_message_bytes=self.config.max_message_bytes,
            idempotency_ttl=self.config.idempotency_ttl,
            config=self.config,
        )
        factory = transport_factory(scheme)
        if factory is None:
            load_plugins(entry_points=True)
            factory = transport_factory(scheme)
        if factory is None:
            raise ConfigError(unknown_scheme_message(scheme))
        return factory(options)

    def close(self) -> None:
        self._closed = True
        if self._transport is not None:
            self._transport.close()
            self._transport = None

    # ------------------------------------------------------------ 优先级解析
    def resolve_priority(
        self,
        task: Task[Any, Any] | None = None,
        *,
        queue: str | None = None,
        priority: int | None = None,
    ) -> int:
        """解析顺序（P2）：submit > task > QueueConfig.priority > Config.default_priority。"""
        if priority is not None:
            return validate_priority(priority, where="submit(priority=…)")
        if task is not None and task.priority is not None:
            return validate_priority(task.priority, where=f"task {task.name} priority")
        queue_name = queue or (task.queue if task is not None else None) or self.config.default_queue
        queue_config = self.config.queues.get(queue_name)
        if queue_config is not None:
            return validate_priority(queue_config.priority, where=f"queue {queue_name} priority")
        return validate_priority(self.config.default_priority, where="Config.default_priority")

    # ---------------------------------------------------------------- submit
    def submit(
        self,
        task: Task[Any, Any] | str,
        args: Sequence[Any] = (),
        kwargs: Mapping[str, Any] | None = None,
        *,
        queue: str | None = None,
        priority: int | None = None,
        eta: Any = None,
        delay: float | None = None,
        expires: float | None = None,
        key: str | None = None,
        headers: Mapping[str, Any] | None = None,
        timeout: float | None = None,
        ack: str | None = None,
    ) -> TaskHandle[Any]:
        if self._closed:
            raise TaskError("App 已关闭")
        if isinstance(task, str):
            resolved = self.task_for(task)
            if resolved is None:
                raise TaskError(f"未注册的任务：{task!r}")
            task = resolved

        env = self._build_envelope(
            task,
            args,
            kwargs,
            queue=queue,
            priority=priority,
            eta=eta,
            delay=delay,
            expires=expires,
            key=key,
            headers=headers,
            timeout=timeout,
            ack=ack,
        )
        # 生产者侧校验：类型白名单 + 大小上限（§8），不让 broker 才炸
        self.codec.encode(env, max_bytes=self.config.max_message_bytes)

        if self.config.eager:
            self.emit(
                "task.submitted",
                job_id=env.id,
                task=env.task,
                queue=env.queue,
                priority=env.priority,
                key=env.key,
            )
            self._run_eager(task, env)
            return TaskHandle(self, env.id)

        delay = max(0.0, env.eta - time.time()) if env.eta is not None else 0.0
        # 幂等键命中时 transport **不重复入队**，并返回已存在的 job id（见 Transport.enqueue 契约）：
        # 句柄必须跟着它，否则调用方拿到的是一个没有 job 记录的 id（handle.get() 会一直等到超时）。
        job_id = self.transport.enqueue(env, queue=env.queue, delay=delay, priority=env.priority)
        self.emit(
            "task.submitted",
            job_id=job_id,
            task=env.task,
            queue=env.queue,
            priority=env.priority,
            key=env.key,
            deduplicated=job_id != env.id,
        )
        return TaskHandle(self, job_id or env.id)

    def call(
        self,
        task: Task[Any, Any] | str,
        args: Sequence[Any] = (),
        kwargs: Mapping[str, Any] | None = None,
        **options: Any,
    ) -> Any:
        """同步在**本进程**执行一次（CLI `taskmq call` / 调试用），不经过 reserve/ack。"""
        resolved = self.task_for(task) if isinstance(task, str) else task
        if resolved is None:
            raise TaskError(f"未注册的任务：{task!r}")
        env = self._build_envelope(resolved, args, kwargs, **options)
        self.codec.encode(env, max_bytes=self.config.max_message_bytes)
        return self._run_eager(resolved, env)

    def _build_envelope(
        self,
        task: Task[Any, Any],
        args: Sequence[Any],
        kwargs: Mapping[str, Any] | None,
        *,
        queue: str | None = None,
        priority: int | None = None,
        eta: Any = None,
        delay: float | None = None,
        expires: float | None = None,
        key: str | None = None,
        headers: Mapping[str, Any] | None = None,
        timeout: float | None = None,
        ack: str | None = None,
    ) -> Envelope:
        queue_name = queue or task.queue or self.config.default_queue
        resolved_priority = self.resolve_priority(task, queue=queue_name, priority=priority)

        eta_absolute = _normalize_eta(eta)          # 数字=相对秒数，datetime=绝对时间
        if delay is not None:
            # delay 是明确的「相对秒数」（与 transport.enqueue 的 delay 同义）；和 eta 同时给时取较晚的
            delayed = time.time() + max(0.0, float(delay))
            eta_absolute = delayed if eta_absolute is None else max(eta_absolute, delayed)
        expires_value = expires if expires is not None else task.expires
        expires_at = time.time() + float(expires_value) if expires_value is not None else None
        timeout_value = timeout if timeout is not None else task.timeout
        # 软超时：deadline 在入队时计算（排队也算预算）；Phase 1 再提供「执行时计算」的选项。
        deadline = time.time() + float(timeout_value) if timeout_value is not None else None
        ack_value = ack or task.ack or ACK_ON_SUCCESS
        if ack_value not in ACK_STRATEGIES:
            raise ConfigError(f"ack 可选 {ACK_STRATEGIES}，收到 {ack_value!r}")

        concurrency_key: str | None = None
        if task.concurrency_key:
            try:
                concurrency_key = task.concurrency_key.format(*args, **(kwargs or {}))
            except (KeyError, IndexError) as exc:
                raise ConfigError(
                    f"concurrency_key 模板 {task.concurrency_key!r} 无法用本次参数渲染：{exc}。"
                    f"模板用关键字参数渲染（如 {task.name}.delay(user=...)）；"
                    "位置参数要用空占位，例如 concurrency_key='user:{}'"
                ) from exc

        return Envelope(
            task=task.name,
            args=tuple(args),
            kwargs=dict(kwargs or {}),
            queue=queue_name,
            priority=resolved_priority,
            eta=eta_absolute,
            expires_at=expires_at,
            deadline=deadline,
            attempt=1,
            max_attempts=(
                task.retry_policy.max_attempts if task.retry_policy is not None else task.max_deliveries
            ),
            ack=ack_value,
            key=key,
            concurrency_key=concurrency_key,
            headers=dict(headers or {}),
        )

    # ------------------------------------------------------------------ eager
    def _run_eager(self, task: Task[Any, Any], env: Envelope) -> Any:
        """`eager=True`：跳过 transport 的 reserve/ack，直接执行并写 job 状态（异常向外抛）。"""
        transport = self.transport
        transport.set_state(
            env.id,
            JobState.RUNNING,
            task=env.task,
            attempt=env.attempt,
            queue=env.queue,
            priority=env.priority,
            worker="eager",
        )
        ctx = TaskContext(
            app=self,
            envelope=env,
            worker_id="eager",
            attempt=env.attempt,
            deliveries=1,
            priority=env.priority,
            queue=env.queue,
        )
        token = _set_current(ctx)
        started = time.monotonic()
        state = JobState.FAILED
        result: Any = None
        failure: BaseException | None = None
        try:
            task.before_start(ctx)
            result = task.run(*env.args, **env.kwargs)
        except BaseException as exc:
            failure = exc
            transport.set_state(
                env.id,
                JobState.FAILED,
                task=env.task,
                attempt=env.attempt,
                error=error_text(exc),
                worker="eager",
                runtime=round(time.monotonic() - started, 6),
            )
            run_hook(task, "on_failure", ctx, exc)
            raise
        else:
            runtime = round(time.monotonic() - started, 6)
            state = JobState.SUCCEEDED
            transport.set_state(
                env.id,
                JobState.SUCCEEDED,
                task=env.task,
                attempt=env.attempt,
                result=result,
                worker="eager",
                runtime=runtime,
            )
            run_hook(task, "on_success", ctx, result, runtime)
            return result
        finally:
            run_hook(task, "after_return", ctx, state, result, failure)
            _reset_current(token)


def _normalize_eta(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"eta 必须是秒数、datetime 或 None，收到 {value!r}")
    return time.time() + float(value)


__all__ = ["App"]
