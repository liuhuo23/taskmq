"""原生 DAG 工作流：定义校验、依赖推进、失败传播、幂等、崩溃恢复、跨 transport、CLI。

设计见 docs/design/workflows.md v1.0。
"""
from __future__ import annotations

import dataclasses
import importlib
import os
import textwrap
import uuid
from pathlib import Path

import pytest

from taskmq import App, Config
from taskmq.errors import ConfigError, WorkflowError
from taskmq.events import CollectingSink
from taskmq.redis_client import RedisClient
from taskmq.task import Retry
from taskmq.testing import run_until_idle
from taskmq.transport.base import JobState
from taskmq.worker.runner import Worker
from taskmq.workflow import WorkflowBuilder, WorkflowPlan

REDIS_URL = os.environ.get("TASKMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379/15")


def _redis_available() -> bool:
    try:
        return RedisClient(REDIS_URL, timeout=1.5).ping()
    except Exception:
        return False


def _app(transport: str = "memory://", **config: object) -> App:
    return App(Config(transport=transport, serializer="json", events="null", **config))


def _register_basic(app: App, calls: list[str]) -> None:
    @app.task(name="wf.extract", queue="q")
    def extract(source: str) -> list[int]:
        calls.append("extract")
        return [1, 2, 3]

    @app.task(name="wf.clean", queue="q")
    def clean(rows: list[int]) -> list[int]:
        calls.append("clean")
        return [row * 2 for row in rows]

    @app.task(name="wf.tally", queue="q")
    def tally(left: list[int], right: list[int]) -> int:
        calls.append("tally")
        return sum(left) + sum(right)

    @app.task(name="wf.report", queue="q")
    def report(tables: list) -> int:
        calls.append("report")
        return sum(sum(table) if isinstance(table, list) else table for table in tables)


def _linear_workflow(app: App) -> None:
    @app.workflow("linear")
    def linear(wf: WorkflowBuilder, source: str):
        extract = wf.step("extract", "wf.extract", args=(source,))
        clean = wf.step("clean", "wf.clean", deps={"rows": extract})
        return wf.join("report", "wf.report", deps=[clean], collect="tables")


# ------------------------------------------------------------------ 基本链路
def test_linear_dag_runs_and_returns_sink_result():
    app = _app()
    calls: list[str] = []
    _register_basic(app, calls)
    _linear_workflow(app)

    handle = app.submit_workflow("linear", {"source": "s3://bucket"})
    run_until_idle(app, queues=["q"], timeout=30)

    status = handle.status()
    assert status.state == JobState.SUCCEEDED
    assert [node.state for node in status.nodes.values()] == [JobState.SUCCEEDED] * 3
    assert calls == ["extract", "clean", "report"]
    assert handle.result() == 12                      # sum([2,4,6])
    assert handle.successful() is True
    assert handle.get(timeout=5) == 12
    assert app.pending_workflows() == []
    app.close()


def test_fan_out_and_join_collects_in_declared_order():
    app = _app()
    calls: list[str] = []
    _register_basic(app, calls)

    @app.workflow("fan")
    def fan(wf: WorkflowBuilder):
        extract = wf.step("extract", "wf.extract", args=("x",))
        left = wf.step("left", "wf.clean", deps={"rows": extract})
        right = wf.step("right", "wf.clean", deps={"rows": extract})
        total = wf.step("total", "wf.tally", deps={"left": left, "right": right})
        # report 收的是 [left, total]：一个 list、一个 int，顺序 = deps 声明顺序
        return wf.join("report", "wf.report", deps=[left, total], collect="tables")

    handle = app.submit_workflow("fan")
    run_until_idle(app, queues=["q"], timeout=30)

    results = handle.results()
    assert handle.status().state == JobState.SUCCEEDED
    assert results["left"] == [2, 4, 6] and results["right"] == [2, 4, 6]
    assert results["total"] == 24                     # sum([2,4,6]) 两次
    assert results["report"] == sum(results["left"]) + results["total"]
    app.close()


def test_report_node_receives_join_list_in_order():
    app = _app()
    seen: list[list[int]] = []

    @app.task(name="wf.pair", queue="q")
    def pair(value: int) -> int:
        return value

    @app.task(name="wf.sum", queue="q")
    def total(values: list[int]) -> int:
        seen.append(list(values))
        return sum(values)

    @app.workflow("ordered")
    def ordered(wf: WorkflowBuilder):
        first = wf.step("first", "wf.pair", args=(1,))
        second = wf.step("second", "wf.pair", args=(2,))
        return wf.join("sum", "wf.sum", deps=[second, first], collect="values")

    handle = app.submit_workflow("ordered")
    run_until_idle(app, queues=["q"], timeout=30)
    assert seen == [[2, 1]]                            # 按 deps 声明顺序（second, first）
    assert handle.result() == 3
    app.close()


# ------------------------------------------------------------------ 失败传播
def test_fail_fast_skips_downstream_and_marks_run_failed():
    app = _app()
    calls: list[str] = []

    @app.task(name="wf.boom", queue="q", retry=None)
    def boom() -> int:
        calls.append("boom")
        raise RuntimeError("炸了")

    @app.task(name="wf.after", queue="q")
    def after(value: int) -> int:
        calls.append("after")
        return value

    @app.workflow("failing")
    def failing(wf: WorkflowBuilder):
        first = wf.step("boom", "wf.boom")
        return wf.step("after", "wf.after", deps={"value": first})

    handle = app.submit_workflow("failing")
    run_until_idle(app, queues=["q"], timeout=30)

    status = handle.status()
    assert status.state == JobState.FAILED
    assert status.nodes["boom"].state == JobState.FAILED
    assert status.nodes["after"].state == JobState.SKIPPED
    assert calls == ["boom"]
    with pytest.raises(WorkflowError, match="未成功"):
        handle.get(timeout=1)
    app.close()


def test_on_failure_continue_does_not_block_siblings():
    app = _app()
    calls: list[str] = []

    @app.task(name="wf.bad", queue="q", retry=None)
    def bad() -> int:
        calls.append("bad")
        raise RuntimeError("允许失败")

    @app.task(name="wf.good", queue="q")
    def good() -> int:
        calls.append("good")
        return 7

    @app.task(name="wf.merge", queue="q")
    def merge(left: int, right: int) -> int:
        calls.append("merge")
        return left + right

    @app.workflow("resilient")
    def resilient(wf: WorkflowBuilder):
        wf.step("bad", "wf.bad", on_failure="continue")        # 允许失败
        healthy = wf.step("good", "wf.good")
        return wf.step("merge", "wf.merge", deps={"left": healthy, "right": healthy})

    handle = app.submit_workflow("resilient")
    run_until_idle(app, queues=["q"], timeout=30)
    status = handle.status()
    assert calls.count("bad") == 1 and calls.count("good") == 1
    assert status.nodes["bad"].state == JobState.FAILED
    assert status.nodes["good"].state == JobState.SUCCEEDED
    assert status.nodes["merge"].state == JobState.SUCCEEDED
    assert status.state == JobState.SUCCEEDED          # continue 的失败不算运行失败
    assert handle.result() == 14
    app.close()


def test_node_retry_blocks_downstream_until_success():
    app = _app()
    attempts: list[int] = []

    @app.task(
        name="wf.flaky",
        queue="q",
        retry=Retry(backoff="fixed", base=0.0, jitter=False, max_attempts=3),
    )
    def flaky() -> int:
        attempts.append(len(attempts) + 1)
        if len(attempts) == 1:
            raise RuntimeError("第一次失败")
        return 42

    @app.task(name="wf.consume", queue="q")
    def consume(value: int) -> int:
        return value * 2

    @app.workflow("retrying")
    def retrying(wf: WorkflowBuilder):
        first = wf.step("flaky", "wf.flaky")
        return wf.step("consume", "wf.consume", deps={"value": first})

    handle = app.submit_workflow("retrying")
    run_until_idle(app, queues=["q"], timeout=30)

    assert len(attempts) == 2
    assert handle.status().nodes["flaky"].attempt == 2
    assert handle.status().state == JobState.SUCCEEDED
    assert handle.result() == 84
    app.close()


# ------------------------------------------------------------------ 幂等/恢复
def test_advance_is_idempotent():
    app = _app()
    executions: list[str] = []
    _register_basic(app, executions)
    _linear_workflow(app)

    handle = app.submit_workflow("linear", {"source": "x"})
    app.advance_workflow(handle.id)                   # 重复推进（a 已 QUEUED）
    app.advance_workflow(handle.id)
    assert app.transport.queue_stats(["q"])[0].pending == 1

    run_until_idle(app, queues=["q"], timeout=30)
    app.advance_workflow(handle.id)                   # 结束后再推一次
    app.resume_workflow(handle.id)
    assert executions == ["extract", "clean", "report"]
    app.close()


def test_crash_between_completion_and_advance_is_recovered():
    app = _app()
    calls: list[str] = []
    _register_basic(app, calls)
    _linear_workflow(app)

    handle = app.submit_workflow("linear", {"source": "x"})
    monkeypatch_target = Worker._advance_workflow
    Worker._advance_workflow = lambda self, delivery: None      # 模拟「推进前崩了」
    try:
        run_until_idle(app, queues=["q"], timeout=30)
    finally:
        Worker._advance_workflow = monkeypatch_target

    assert app.transport.get_state(f"{handle.id}::extract").state == JobState.SUCCEEDED
    assert app.transport.get_state(f"{handle.id}::clean") is None      # 崩溃 → 下游还没入队
    assert handle.status().nodes["clean"].state == JobState.PENDING
    assert handle.status().state == JobState.RUNNING

    assert app.resume_workflow(handle.id) == JobState.RUNNING    # 补偿推进（幂等）
    run_until_idle(app, queues=["q"], timeout=30)
    assert handle.status().state == JobState.SUCCEEDED
    assert handle.result() == 12
    assert calls == ["extract", "clean", "report"]               # 节点没有重复执行
    app.close()


def test_worker_maintenance_reconciles_pending_runs():
    app = _app()
    calls: list[str] = []
    _register_basic(app, calls)
    _linear_workflow(app)

    handle = app.submit_workflow("linear", {"source": "x"})
    original = Worker._advance_workflow
    Worker._advance_workflow = lambda self, delivery: None
    try:
        with Worker(app, queues=["q"]) as worker:
            worker.run_until_idle(timeout=30)
            assert handle.status().state == JobState.RUNNING
            # 补偿推进是**周期性**的（默认 ≥1s 节流）：模拟后续几个维护周期
            for _ in range(5):
                worker._last_reconcile = 0.0
                worker.run_until_idle(timeout=30)
                if handle.status().terminal:
                    break
            assert handle.status().terminal is True
    finally:
        Worker._advance_workflow = original
    assert handle.status().state == JobState.SUCCEEDED
    app.close()


# ------------------------------------------------------------------ 定义校验
def _plan(steps, **kwargs) -> WorkflowPlan:
    return WorkflowPlan(workflow=kwargs.pop("workflow", "w"), steps=steps, **kwargs)


def test_definition_validation_errors():
    builder = WorkflowBuilder("v")
    first = builder.step("a", "t.a")
    second = builder.step("b", "t.b", deps=[first])

    with pytest.raises(ConfigError, match="环"):
        _plan((dataclasses.replace(first, deps=("b",)), second))
    with pytest.raises(ConfigError, match="不存在"):
        _plan((builder.step("c", "t.c", deps=["ghost"]),))
    with pytest.raises(ConfigError, match="依赖自己"):
        _plan((builder.step("d", "t.d", deps=["d"]),))
    with pytest.raises(ConfigError, match="重名"):
        _plan((first, first))
    with pytest.raises(ConfigError, match="没有任何 step"):
        _plan(())
    with pytest.raises(ConfigError, match="collect"):
        builder.step("e", "t.e", collect="items")
    with pytest.raises(ConfigError, match="同时出现在"):
        builder.step("f", "t.f", kwargs={"rows": []}, deps={"rows": first})
    with pytest.raises(ConfigError, match="汇节点"):
        _plan((first,), sink="missing")


def test_workflow_registration_and_submit_errors():
    app = _app()

    @app.workflow("dup")
    def dup(wf: WorkflowBuilder):
        return wf.step("only", "wf.extract")

    with pytest.raises(ConfigError, match="重复"):
        app.workflow("dup")(lambda wf, **params: None)
    with pytest.raises(ConfigError, match="未注册的工作流"):
        app.submit_workflow("nope")

    app.transport.supports_job_listing = False                     # 缺能力 → 启动即报错
    with pytest.raises(ConfigError, match="supports_job_listing"):
        app.submit_workflow("dup")
    app.close()


def test_events_are_emitted():
    app = _app()
    sink = CollectingSink()
    app.add_sink(sink)
    calls: list[str] = []
    _register_basic(app, calls)
    _linear_workflow(app)

    handle = app.submit_workflow("linear", {"source": "x"})
    run_until_idle(app, queues=["q"], timeout=30)
    names = sink.names()
    assert names.count("workflow.started") == 1
    assert names.count("workflow.advanced") >= 2
    assert names.count("workflow.succeeded") == 1
    assert sink.of("workflow.started")[0]["run"] == handle.id
    app.close()


# ------------------------------------------------------------ 跨 transport
@pytest.mark.parametrize("backend", ["sqlite", "memory"])
def test_workflow_over_other_transports(backend, tmp_path):
    transport = (
        f"sqlite:///{tmp_path / 'wf.db'}" if backend == "sqlite" else "memory://"
    )
    app = _app(transport)
    calls: list[str] = []
    _register_basic(app, calls)
    _linear_workflow(app)

    handle = app.submit_workflow("linear", {"source": "x"})
    run_until_idle(app, queues=["q"], timeout=30)
    assert handle.status().state == JobState.SUCCEEDED
    assert handle.result() == 12
    app.close()


@pytest.mark.skipif(not _redis_available(), reason=f"没有可用的 Redis（{REDIS_URL}）")
def test_workflow_over_redis():
    prefix = f"taskmq:test:{uuid.uuid4().hex}:"
    app = _app(f"redis://127.0.0.1:6379/15?prefix={prefix}", concurrency=2)
    calls: list[str] = []
    _register_basic(app, calls)
    _linear_workflow(app)
    try:
        handle = app.submit_workflow("linear", {"source": "x"})
        run_until_idle(app, queues=["q"], timeout=30)
        assert handle.status().state == JobState.SUCCEEDED
        assert handle.result() == 12
        runs = app.transport.list_jobs(prefix="wf-")
        assert runs and runs[0].job_id == handle.id
        assert runs[0].state == JobState.SUCCEEDED
    finally:
        app.transport.flush_prefix()
        app.close()


# ---------------------------------------------------------------------- CLI
def test_cli_workflow_list_status_resume(tmp_path: Path, monkeypatch, capsys):
    module_path = tmp_path / "wf_cli_app.py"
    module_path.write_text(
        textwrap.dedent(
            """
            from taskmq import App, Config
            from taskmq.workflow import WorkflowBuilder

            app = App(Config(transport="memory://", serializer="json", events="null"))


            @app.task(name="cli.step", queue="q")
            def step(value: int) -> int:
                return value + 1


            @app.workflow("cli")
            def cli(wf):
                first = wf.step("a", "cli.step", args=(1,))
                return wf.step("b", "cli.step", deps={"value": first})
            """
        ),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    module = importlib.import_module("wf_cli_app")
    app = module.app

    handle = app.submit_workflow("cli")
    from taskmq.cli import main

    assert main(["--app", "wf_cli_app:app", "workflow", "list"]) == 0
    assert "RUNS" in capsys.readouterr().out

    assert main(["--app", "wf_cli_app:app", "workflow", "status", handle.id]) == 0
    out = capsys.readouterr().out
    assert "RUN " in out and "a " in out and "deps=" in out

    assert main(["--app", "wf_cli_app:app", "workflow", "resume", handle.id]) == 0
    assert "resumed" in capsys.readouterr().out

    assert main(["--app", "wf_cli_app:app", "status", "-Q", "q"]) == 0
    assert "WORKFLOWS" in capsys.readouterr().out

    run_until_idle(app, queues=["q"], timeout=30)
    assert app.handle_workflow(handle.id).result() == 3
    app.close()
