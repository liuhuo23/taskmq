"""示例插件（examples/plugin_rocketmq）端到端：不改 taskmq 核心也能接一个后端。

对应 docs/design/plugins.md §4 的三条接入路径与 §6 的一致性套件。
"""
from __future__ import annotations

import importlib
import textwrap
import uuid
from pathlib import Path

import pytest

from taskmq import App, Config
from taskmq import plugins as plugins_mod
from taskmq.cli import main
from taskmq.errors import ConfigError
from taskmq.testing import CONFORMANCE_SCENARIOS, transport_conformance
from taskmq.worker.runner import Worker

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "plugin_rocketmq"


@pytest.fixture(autouse=True)
def _example_on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(EXAMPLE))
    importlib.invalidate_caches()
    plugins_mod.reset_plugins()
    yield
    plugins_mod.reset_plugins()


@pytest.fixture
def plugin():
    """模拟「已安装/已加载」：import 一次并幂等注册（生产进程里 import 只会发生一次）。"""
    module = importlib.import_module("taskmq_rocketmq")
    module.register()
    return module


def _url() -> str:
    return "rocketmq://fake-mq:8080/taskmq?group=conf"


def _write_app_module(tmp_path: Path, monkeypatch, name: str, transport: str) -> str:
    (tmp_path / f"{name}.py").write_text(
        textwrap.dedent(
            f"""
            from taskmq import App, Config

            app = App(Config(transport="{transport}", serializer="json", events="null"))


            @app.task(name="rocket.job", queue="rocket")
            def job() -> str:
                return "ok"
            """
        ),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    return f"{name}:app"


# ------------------------------------------------------------ 一致性 & 解析
def test_example_passes_the_conformance_suite(plugin):
    executed = transport_conformance(
        lambda: plugin.RocketMQTransport(endpoints="fake-mq:8080", topic="taskmq")
    )
    skipped = [name for name in CONFORMANCE_SCENARIOS if name not in executed]
    # 声明了 supports_leases/workers/job_listing=False + limitations["yield"]
    assert skipped == ["yield", "named_leases", "worker_registry", "job_listing"]
    assert {"priority_fifo", "atomic_claim", "lease_recovery", "dlq_replay", "job_state"} <= set(
        executed
    )


def test_url_is_parsed_by_the_plugin(plugin):
    app = App(Config(transport=_url(), serializer="json", events="null"))
    transport = app.transport
    assert isinstance(transport, plugin.RocketMQTransport)
    assert transport.endpoints == "fake-mq:8080"
    assert transport.topic == "taskmq"
    assert transport.group == "conf"
    app.close()


# ------------------------------------------------------------------ 端到端
def test_end_to_end_task_run_over_plugin_transport(plugin):
    app = App(Config(transport=_url(), serializer="json", events="null", concurrency=2))
    seen: list[int] = []

    @app.task(name="plugin.add", queue="rocket")
    def add(a: int, b: int) -> int:
        seen.append(a)
        return a + b

    handle = app.submit("plugin.add", (2, 3))
    with Worker(app, queues=["rocket"]) as worker:
        worker.run_until_idle(timeout=10)

    assert handle.get(timeout=5) == 5
    assert seen == [2]
    assert app.transport.queue_stats(["rocket"])[0].pending == 0
    app.close()


def test_unsupported_capability_fails_fast_on_startup(plugin):
    """插件声明 supports_leases=False → 用 concurrency_key 的任务启动即报错，不拖到运行时。"""
    app = App(Config(transport=_url(), serializer="json", events="null"))

    @app.task(name="plugin.locked", queue="rocket", concurrency_key="user:{uid}")
    def locked(uid: int) -> int:
        return uid

    worker = Worker(app, queues=["rocket"])
    try:
        with pytest.raises(ConfigError):        # 注册了 concurrency_key 任务 → 启动即校验
            worker.poll()
    finally:
        worker.close()
    app.close()


# ---------------------------------------------------------------------- CLI
def test_cli_worker_with_plugins_flag(tmp_path, monkeypatch, capsys):
    """路径②：不打包，靠 CLI --plugins 显式加载（用一个全新模块，避免 import 缓存干扰）。"""
    module_name = f"flag_plugin_{uuid.uuid4().hex[:8]}"
    (tmp_path / f"{module_name}.py").write_text(
        "from taskmq.plugins import register_transport\n"
        "from taskmq.transport.memory import MemoryTransport\n"
        "\n"
        "register_transport('rocketmq', lambda options: MemoryTransport())\n",
        encoding="utf-8",
    )
    spec = _write_app_module(tmp_path, monkeypatch, f"flag_app_{uuid.uuid4().hex[:8]}", _url())
    module = importlib.import_module(spec.split(":")[0])

    assert (
        main(["--app", spec, "--plugins", module_name, "status", "-Q", "rocket"]) == 0
    )  # 插件先加载，App 才看得到 rocketmq://
    assert "QUEUES" in capsys.readouterr().out

    module.app.submit("rocket.job")
    assert (
        main(["--app", spec, "--plugins", module_name, "worker", "--once", "-Q", "rocket"]) == 0
    )
    capsys.readouterr()
    assert module.app.transport.queue_stats(["rocket"])[0].pending == 0


def test_unknown_scheme_without_plugins_is_actionable():
    with pytest.raises(ConfigError) as excinfo:
        _ = App(Config(transport=_url(), serializer="json", events="null")).transport
    message = str(excinfo.value)
    assert "未知 transport scheme" in message
    assert "load_plugins" in message and "pip install" in message
    assert plugins_mod.ENTRY_POINT_GROUP in message


def test_example_declares_the_entry_point_group():
    pyproject = (EXAMPLE / "pyproject.toml").read_text(encoding="utf-8")
    assert 'entry-points."taskmq.plugins"' in pyproject
    assert 'rocketmq = "taskmq_rocketmq"' in pyproject
