"""原生 DAG 工作流（docs/design/workflows.md v1.0）。

三条不变量（也是这个模块存在的理由）：

1. **依赖声明在 App 里**：可 review、可测试、可画图，不藏在消息链里；
2. **推进是纯计算**：读节点状态 → 算 ready/skipped → 返回要入队的节点。任何 worker 都能算，
   无中心协调、无轮询；
3. **节点状态是事实来源**，运行记录只是派生缓存 —— 所以并发推进不会互相踩，
   节点 job id 又是确定性的（额外用幂等键去重），at-least-once 下重复推进不会重复执行。

本模块**不做副作用**（不 import App、不碰 transport 的写接口），便于单测；
真正的入队/写状态在 `App.advance_workflow()` 里。
"""
from __future__ import annotations

import dataclasses
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from ._compat import _SLOTS
from .errors import ConfigError, WorkflowError
from .transport.base import JobState, Transport

__all__ = [
    "WORKFLOW_HEADER",
    "RUN_PREFIX",
    "Step",
    "Workflow",
    "WorkflowBuilder",
    "WorkflowPlan",
    "PlanEvaluation",
    "NodeRef",
    "NodeStatus",
    "WorkflowStatus",
    "WorkflowHandle",
    "encode_node_header",
    "decode_node_header",
    "run_meta",
]

#: Envelope.headers 里挂工作流上下文的键（协议 v1 兼容：只是多一个 header）
WORKFLOW_HEADER = "taskmq.workflow"
#: 运行 id 前缀（便于 `list_jobs(prefix="wf-")` 枚举未完成的运行）
RUN_PREFIX = "wf-"
#: 运行 job meta 里记录定义名/参数/汇节点的键
RUN_META_WORKFLOW = "workflow"

_FAILURE_MODES = ("fail", "continue")


@dataclasses.dataclass(frozen=True, **_SLOTS)
class Step:
    """DAG 里的一个节点：一个普通任务 + 依赖声明。

    - `bind={参数名: 上游节点}`：把上游**结果**注入该参数；
    - `collect="参数名"`：把 `deps` 里列的上游结果**按声明顺序**收集成 list 注入；
    - `on_failure="continue"`：本节点最终失败不阻断整条运行（只跳过它的下游）。
    """

    name: str
    task: str
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    deps: tuple[str, ...] = ()
    bind: Mapping[str, str] = dataclasses.field(default_factory=dict)
    collect: str | None = None
    queue: str | None = None
    priority: int | None = None
    on_failure: str = "fail"

    def __post_init__(self) -> None:
        if not self.name:
            raise ConfigError("workflow step 必须有 name")
        if not self.task:
            raise ConfigError(f"workflow step {self.name!r} 必须有 task")
        if self.on_failure not in _FAILURE_MODES:
            raise ConfigError(
                f"step {self.name!r} 的 on_failure 可选 {_FAILURE_MODES}，收到 {self.on_failure!r}"
            )
        if self.collect and not self.deps:
            raise ConfigError(f"step {self.name!r} 用了 collect 但没有 deps")
        if self.collect and self.collect in self.kwargs:
            raise ConfigError(f"step {self.name!r}: 参数 {self.collect!r} 同时出现在 kwargs 与 collect")
        for param in self.bind:
            if param in self.kwargs:
                raise ConfigError(f"step {self.name!r}: 参数 {param!r} 同时出现在 kwargs 与 deps")
            if param == self.collect:
                raise ConfigError(f"step {self.name!r}: 参数 {param!r} 同时出现在 bind 与 collect")

    @property
    def upstreams(self) -> tuple[str, ...]:
        """所有上游节点（去重、保序）：先 deps，再 bind 的值。"""
        names = list(self.deps)
        for name in self.bind.values():
            if name not in names:
                names.append(name)
        return tuple(names)

    @property
    def blocks_dependents(self) -> bool:
        """本节点失败时是否阻断下游（fail-fast）。"""
        return self.on_failure != "continue"


def _upstream_name(value: Any) -> str:
    """上游引用：`wf.step(...)` 返回的 Step 用它的 name；字符串原样用。"""
    return str(getattr(value, "name", None) or value)


class WorkflowBuilder:
    """在 `@app.workflow` 的函数体里搭图（每次提交都会重新构建，所以可以按参数变化）。"""

    def __init__(self, workflow: str) -> None:
        self._workflow = workflow
        self._steps: list[Step] = []

    def step(
        self,
        name: str,
        task: Any,
        *,
        args: Sequence[Any] = (),
        kwargs: Mapping[str, Any] | None = None,
        deps: Mapping[str, str] | Sequence[str] | None = None,
        bind: Mapping[str, str] | None = None,
        collect: str | None = None,
        queue: str | None = None,
        priority: int | None = None,
        on_failure: str = "fail",
    ) -> Step:
        """加一个节点。`deps` 传 dict 表示「参数名 → 上游」，传序列表示「纯依赖顺序」。"""
        dependency_names: tuple[str, ...] = ()
        bound: dict[str, str] = {
            str(key): _upstream_name(value) for key, value in (bind or {}).items()
        }
        if isinstance(deps, Mapping):
            bound.update({str(key): _upstream_name(value) for key, value in deps.items()})
        elif deps is not None:
            dependency_names = tuple(_upstream_name(item) for item in deps)

        step = Step(
            name=str(name),
            task=getattr(task, "name", None) or str(task),
            args=tuple(args),
            kwargs=dict(kwargs or {}),
            deps=dependency_names,
            bind=bound,
            collect=collect,
            queue=queue,
            priority=priority,
            on_failure=on_failure,
        )
        if any(existing.name == step.name for existing in self._steps):
            raise ConfigError(f"workflow {self._workflow!r} 里有重名节点：{step.name!r}")
        self._steps.append(step)
        return step

    def join(
        self,
        name: str,
        task: Any,
        *,
        deps: Sequence[str],
        collect: str,
        **options: Any,
    ) -> Step:
        """汇合点（chord 的 body 等价物）：按 `deps` 声明顺序收集结果成一个 list。"""
        return self.step(name, task, deps=deps, collect=collect, **options)

    @property
    def steps(self) -> tuple[Step, ...]:
        return tuple(self._steps)


@dataclasses.dataclass(frozen=True, **_SLOTS)
class Workflow:
    """注册在 App 上的 DAG 模板。"""

    name: str
    #: 构建函数：`build(wf, **params)` —— 参数按**关键字**传入，所以 arity 不固定
    build: Callable[..., Step | None]
    queue: str | None = None
    priority: int | None = None
    description: str = ""

    def instantiate(self, params: Mapping[str, Any] | None = None) -> WorkflowPlan:
        """用本次运行的参数构建具体图（并做校验）。"""
        builder = WorkflowBuilder(self.name)
        # 参数按**关键字**传给构建函数（键必须能当参数名）：def etl(wf, source) ← {"source": ...}
        sink = self.build(builder, **dict(params or {}))
        return WorkflowPlan(
            workflow=self.name,
            steps=builder.steps,
            params=dict(params or {}),
            sink=getattr(sink, "name", None),
            queue=self.queue,
            priority=self.priority,
        )


@dataclasses.dataclass(frozen=True, **_SLOTS)
class PlanEvaluation:
    """一次推进评估的结果（纯数据，便于日志/事件/测试）。

    `ready` 只包含**尚未入队**的节点（依赖已满足）——这正是 `advance()` 要入队的集合；
    已经在队列里/在跑/重试中的节点归入 `inflight`。
    """

    ready: tuple[Step, ...] = ()
    skipped: tuple[str, ...] = ()
    inflight: tuple[str, ...] = ()
    state: str = JobState.RUNNING

    @property
    def terminal(self) -> bool:
        return self.state in JobState.TERMINAL


@dataclasses.dataclass(frozen=True, **_SLOTS)
class WorkflowPlan:
    """一次运行的具体图：拓扑、求值、参数注入。"""

    workflow: str
    steps: tuple[Step, ...]
    params: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    sink: str | None = None
    queue: str | None = None          # 运行级默认队列（step / 任务都没指定时用）
    priority: int | None = None       # 运行级默认优先级

    def __post_init__(self) -> None:
        if not self.steps:
            raise ConfigError(f"workflow {self.workflow!r} 没有任何 step")
        names = [step.name for step in self.steps]
        duplicates = {name for name in names if names.count(name) > 1}
        if duplicates:
            raise ConfigError(f"workflow {self.workflow!r} 节点重名：{sorted(duplicates)}")
        known = set(names)
        for step in self.steps:
            for upstream in step.upstreams:
                if upstream == step.name:
                    raise ConfigError(f"workflow {self.workflow!r}: 节点 {step.name!r} 依赖自己")
                if upstream not in known:
                    raise ConfigError(
                        f"workflow {self.workflow!r}: 节点 {step.name!r} 依赖不存在的节点 {upstream!r}"
                    )
        if self.sink is not None and self.sink not in known:
            raise ConfigError(f"workflow {self.workflow!r}: 汇节点 {self.sink!r} 不在图里")
        self.order()                      # 顺带做环检测

    # ------------------------------------------------------------------ 结构
    @property
    def by_name(self) -> dict[str, Step]:
        return {step.name: step for step in self.steps}

    def order(self) -> tuple[str, ...]:
        """拓扑序（Kahn）；有环直接抛 `ConfigError`。"""
        by_name = self.by_name
        pending = {name: set(step.upstreams) for name, step in by_name.items()}
        ordered: list[str] = []
        while pending:
            ready = sorted(name for name, ups in pending.items() if not ups - set(ordered))
            if not ready:
                raise ConfigError(
                    f"workflow {self.workflow!r} 存在环：{sorted(pending)}"
                )
            for name in ready:
                pending.pop(name)
                ordered.append(name)
        return tuple(ordered)

    def node_job_id(self, run_id: str, node: str) -> str:
        return f"{run_id}::{node}"

    def run_id(self, value: str) -> str:
        return value

    # ------------------------------------------------------------------ 求值
    def evaluate(self, states: Mapping[str, str]) -> PlanEvaluation:
        """`states`: 节点名 -> 已观测状态（含 `JobState.SKIPPED`）。"""
        by_name = self.by_name
        ready: list[Step] = []
        skipped: list[str] = []
        inflight: list[str] = []
        for name in self.order():
            step = by_name[name]
            observed = states.get(name)
            if observed in JobState.TERMINAL or observed == JobState.SKIPPED:
                continue
            if any(
                states.get(up) == JobState.FAILED and by_name[up].blocks_dependents
                for up in step.upstreams
            ):
                skipped.append(name)
                continue
            if all(states.get(up) == JobState.SUCCEEDED for up in step.upstreams):
                if observed is None or observed == JobState.PENDING:
                    ready.append(step)
                else:
                    inflight.append(name)
        return PlanEvaluation(
            ready=tuple(ready),
            skipped=tuple(skipped),
            inflight=tuple(inflight),
            state=self.state_after(states, skipped),
        )

    def state_after(self, states: Mapping[str, str], skipped: Sequence[str] = ()) -> str:
        observed = dict(states)
        for name in skipped:
            observed.setdefault(name, JobState.SKIPPED)

        def settled(name: str) -> bool:
            step = self.by_name[name]
            state = observed.get(name)
            if state in (JobState.SUCCEEDED, JobState.SKIPPED):
                return True
            # on_failure="continue" 的节点：失败也算"完结"，只是不阻断运行
            return state == JobState.FAILED and not step.blocks_dependents

        for name, step in self.by_name.items():
            if observed.get(name) == JobState.FAILED and step.blocks_dependents:
                return JobState.FAILED
        if all(settled(name) for name in self.by_name):
            if self.sink is None or observed.get(self.sink) == JobState.SUCCEEDED:
                return JobState.SUCCEEDED
            return JobState.FAILED
        return JobState.RUNNING

    def payload(self, step: Step, results: Mapping[str, Any]) -> tuple[tuple[Any, ...], dict[str, Any]]:
        """把上游结果注入本节点的参数（bind 按名、collect 按序）。"""
        args = tuple(step.args)
        kwargs = dict(step.kwargs)
        for param, upstream in step.bind.items():
            kwargs[param] = results.get(upstream)
        if step.collect:
            kwargs[step.collect] = [results.get(name) for name in step.deps]
        return args, kwargs

    def summary(self, states: Mapping[str, str]) -> list[dict[str, Any]]:
        return [
            {
                "node": step.name,
                "task": step.task,
                "state": states.get(step.name, JobState.PENDING),
                "on_failure": step.on_failure,
                "deps": list(step.upstreams),
            }
            for step in self.steps
        ]


# ------------------------------------------------------------------ 运行上下文
@dataclasses.dataclass(frozen=True, **_SLOTS)
class NodeRef:
    """消息头里的工作流上下文（worker 靠它知道该推进哪个运行）。"""

    run: str
    workflow: str
    node: str


def encode_node_header(run_id: str, workflow: str, node: str) -> dict[str, Any]:
    return {WORKFLOW_HEADER: {"run": run_id, "workflow": workflow, "node": node}}


def decode_node_header(headers: Mapping[str, Any] | None) -> NodeRef | None:
    raw = (headers or {}).get(WORKFLOW_HEADER)
    if not isinstance(raw, Mapping):
        return None
    run = str(raw.get("run", "") or "")
    node = str(raw.get("node", "") or "")
    if not run or not node:
        return None
    return NodeRef(run=run, workflow=str(raw.get("workflow", "") or ""), node=node)


def run_meta(plan: WorkflowPlan) -> dict[str, Any]:
    """运行 job 的 meta（用于重建 plan + CLI 展示）。"""
    return {
        RUN_META_WORKFLOW: plan.workflow,
        "params": dict(plan.params),
        "sink": plan.sink,
        "nodes": [step.name for step in plan.steps],
    }


# ------------------------------------------------------------------ 查询句柄
@dataclasses.dataclass(frozen=True, **_SLOTS)
class NodeStatus:
    name: str
    state: str
    task: str = ""
    attempt: int = 0
    job_id: str = ""
    error: str | None = None


@dataclasses.dataclass(frozen=True, **_SLOTS)
class WorkflowStatus:
    run_id: str
    workflow: str
    state: str
    nodes: Mapping[str, NodeStatus] = dataclasses.field(default_factory=dict)
    error: str | None = None

    @property
    def terminal(self) -> bool:
        return self.state in JobState.TERMINAL

    @property
    def successful(self) -> bool:
        return self.state == JobState.SUCCEEDED


class WorkflowHandle:
    """一次运行的句柄：查状态/等结果（`get()` 是**客户端本地**等待，不是调度轮询）。"""

    def __init__(
        self,
        transport: Transport,
        run_id: str,
        plan: WorkflowPlan,
        *,
        poll_interval: float = 0.05,
    ) -> None:
        self._transport = transport
        self.id = run_id
        self.plan = plan
        self.workflow = plan.workflow
        self._poll = poll_interval

    # ------------------------------------------------------------------ 状态
    def node_states(self) -> dict[str, str]:
        states: dict[str, str] = {}
        for step in self.plan.steps:
            record = self._transport.get_state(self.plan.node_job_id(self.id, step.name))
            states[step.name] = record.state if record is not None else JobState.PENDING
        return states

    def status(self) -> WorkflowStatus:
        states = self.node_states()
        nodes: dict[str, NodeStatus] = {}
        for step in self.plan.steps:
            record = self._transport.get_state(self.plan.node_job_id(self.id, step.name))
            nodes[step.name] = NodeStatus(
                name=step.name,
                state=states[step.name],
                task=step.task,
                attempt=record.attempt if record is not None else 0,
                job_id=self.plan.node_job_id(self.id, step.name),
                error=(record.error if record is not None else None),
            )
        return WorkflowStatus(
            run_id=self.id,
            workflow=self.plan.workflow,
            state=self.plan.state_after(states),
            nodes=nodes,
        )

    def results(self) -> dict[str, Any]:
        results: dict[str, Any] = {}
        for step in self.plan.steps:
            record = self._transport.get_state(self.plan.node_job_id(self.id, step.name))
            if record is not None and record.has_result:
                results[step.name] = record.result
        return results

    def result(self) -> Any:
        """汇节点的结果（没有汇节点则返回每个节点结果的 dict）。"""
        results = self.results()
        if self.plan.sink is not None:
            return results.get(self.plan.sink)
        return results

    def ready(self) -> bool:
        return self.status().terminal

    def successful(self) -> bool:
        return self.status().state == JobState.SUCCEEDED

    def get(self, timeout: float | None = None, *, poll: float | None = None) -> Any:
        """等运行结束并返回汇结果；超时抛 `WorkflowError`（带每节点状态，便于排查）。"""
        interval = poll if poll is not None else self._poll
        deadline = None if timeout is None else time.monotonic() + float(timeout)
        while True:
            status = self.status()
            if status.terminal:
                if status.state != JobState.SUCCEEDED:
                    states = ", ".join(f"{name}={item.state}" for name, item in status.nodes.items())
                    raise WorkflowError(f"工作流 {self.id} 未成功（{status.state}）：{states}")
                return self.result()
            if deadline is not None and time.monotonic() >= deadline:
                states = ", ".join(f"{name}={item.state}" for name, item in status.nodes.items())
                raise WorkflowError(f"等待工作流 {self.id} 超时（{status.state}）：{states}")
            time.sleep(max(0.005, interval))
