"""插件机制：注册表、显式加载、entry point 懒发现、codec/sink/pool 扩展、子进程透传。

见 docs/design/plugins.md（D1–D7 已拍板）。
"""
from __future__ import annotations

import argparse
import importlib
import textwrap
from pathlib import Path
from typing import Any

import pytest

from taskmq import App, Config, TransportOptions
from taskmq import plugins as plugins_mod
from taskmq.errors import ConfigError
from taskmq.protocol import JSONCodec, get_codec
from taskmq.transport.base import Transport
from taskmq.transport.memory import MemoryTransport
from taskmq.worker.execution import BodyOutcome
from taskmq.worker.pool import make_pool
from taskmq.worker.runner import Worker


@pytest.fixture(autouse=True)
def _isolated_registry():
    """每个用例前后都把插件注册表恢复原状，避免用例互相污染。"""
    plugins_mod.reset_plugins()
    yield
    plugins_mod.reset_plugins()


def _memory_factory(seen: list[TransportOptions] | None = None):
    def factory(options: TransportOptions) -> Transport:
        if seen is not None:
            seen.append(options)
        return MemoryTransport(idempotency_ttl=options.idempotency_ttl)

    return factory


def _write_module(tmp_path: Path, monkeypatch, body: str, name: str) -> str:
    (tmp_path / f"{name}.py").write_text(textwrap.dedent(body), encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    return name


# ------------------------------------------------------------------ 注册表
def test_register_custom_transport_and_options():
    seen: list[TransportOptions] = []
    plugins_mod.register_transport("demo", _memory_factory(seen))

    app = App(Config(transport="demo://whatever?x=1", serializer="json", events="null"))
    assert isinstance(app.transport, MemoryTransport)
    assert len(seen) == 1
    options = seen[0]
    assert options.url == "demo://whatever?x=1"          # URL 原样交给插件
    assert isinstance(options.codec, JSONCodec)
    assert options.config.transport == "demo://whatever?x=1"
    app.close()


def test_builtin_schemes_are_protected_and_overridable():
    with pytest.raises(ConfigError, match="内建"):
        plugins_mod.register_transport("sqlite", _memory_factory())

    plugins_mod.register_transport("sqlite", _memory_factory(), override=True)
    app = App(Config(transport="sqlite:///whatever.db", serializer="json", events="null"))
    assert isinstance(app.transport, MemoryTransport)     # 显式 override 才覆盖内建
    app.close()


def test_duplicate_registration_requires_override():
    plugins_mod.register_transport("dup", _memory_factory())
    with pytest.raises(ConfigError, match="已被"):
        plugins_mod.register_transport("dup", _memory_factory())
    plugins_mod.register_transport("dup", _memory_factory(), override=True)     # 显式覆盖 OK


def test_unknown_scheme_error_is_actionable():
    plugins_mod.register_transport("known", _memory_factory())
    with pytest.raises(ConfigError) as excinfo:
        app = App(Config(transport="rocketmq://rmq:8080/taskmq", serializer="json", events="null"))
        _ = app.transport                                  # transport 是懒建的 → 这里才解析 scheme
    message = str(excinfo.value)
    assert "rocketmq" in message
    assert "memory" in message and "sqlite" in message and "redis" in message
    assert "known" in message                             # 列出已注册插件
    assert "pip install" in message and "load_plugins" in message
    assert plugins_mod.ENTRY_POINT_GROUP in message


def test_registry_helpers():
    plugins_mod.register_transport("demo", _memory_factory())
    assert "demo" in plugins_mod.known_transports()
    assert plugins_mod.known_transports()["demo"].endswith("test_plugins")
    assert plugins_mod.transport_factory("nope") is None
    assert plugins_mod.transport_factory("demo") is not None


# ------------------------------------------------------------------ 显式加载
_PLUGIN_BODY = """
    from taskmq import TransportOptions
    from taskmq.events import CollectingSink
    from taskmq.plugins import register_codec, register_sink, register_transport
    from taskmq.protocol import JSONCodec
    from taskmq.transport.memory import MemoryTransport

    SINK = CollectingSink()


    def transport_factory(options: TransportOptions):
        return MemoryTransport(idempotency_ttl=options.idempotency_ttl)


    register_transport("plug", transport_factory)
    register_codec("plugjson", lambda registry: JSONCodec(registry))
    register_sink("plugsink", lambda: SINK)
"""


def test_explicit_load_plugins_registers_everything(tmp_path, monkeypatch):
    module = _write_module(tmp_path, monkeypatch, _PLUGIN_BODY, "explicit_plugin")
    plugin = importlib.import_module(module)

    # ① 模块级显式加载：之后 Config 里的自定义 serializer / sink 才认得
    assert plugins_mod.load_plugins([module]) == [module]
    app = App(Config(transport="memory://", serializer="plugjson", events="plugsink"))
    assert app.plugins == ()                     # 不是这个 App 加载的
    app.emit("task.started", task="t")
    assert plugin.SINK.names() == ["task.started"]

    # ② App.load_plugins（在 validate 之前生效）+ include= 路径
    app2 = App(Config(transport="memory://", serializer="plugjson", events="plugsink"))
    assert app2.load_plugins([module]) == [module]   # 幂等：重复加载不会重复注册
    app3 = App(
        Config(transport="plug://x", serializer="plugjson", events="plugsink"), include=[module]
    )
    assert app3.plugins == (module,)
    assert isinstance(app3.transport, MemoryTransport)   # scheme 只有加载插件后才可用

    app.close()
    app2.close()
    app3.close()


def test_load_plugins_accepts_comma_string_and_reports_failures(tmp_path, monkeypatch):
    module = _write_module(tmp_path, monkeypatch, "VALUE = 1\n", "tiny_plugin")
    app = App(Config(transport="memory://", events="null"))
    assert app.load_plugins(f" {module} , ") == [module]
    with pytest.raises(ConfigError, match="no_such_plugin_xyz"):
        app.load_plugins(["no_such_plugin_xyz"])
    app.close()


def test_env_plugins_loaded_at_app_construction(tmp_path, monkeypatch):
    module = _write_module(tmp_path, monkeypatch, _PLUGIN_BODY, "env_plugin")
    monkeypatch.setenv(plugins_mod.PLUGINS_ENV, f"{module}, ")
    app = App(Config(transport="plug://x", serializer="json", events="null"))
    assert app.plugins == (module,)
    assert isinstance(app.transport, MemoryTransport)
    app.close()


def test_include_parameter_registers_plugins(tmp_path, monkeypatch):
    module = _write_module(tmp_path, monkeypatch, _PLUGIN_BODY, "include_plugin")
    app = App(Config(transport="plug://x", events="null"), include=[module])
    assert app.plugins == (module,)
    assert isinstance(app.transport, MemoryTransport)
    app.close()


# --------------------------------------------------------------- entry point
def test_entry_point_discovery_is_lazy_and_does_not_import(tmp_path, monkeypatch):
    module = _write_module(tmp_path, monkeypatch, _PLUGIN_BODY, "ep_plugin")

    class _EntryPoint:
        value = module

    calls: list[str] = []

    def fake_entry_points(group: str = plugins_mod.ENTRY_POINT_GROUP):
        calls.append(group)
        return [_EntryPoint()]

    monkeypatch.setattr(plugins_mod, "_entry_points", fake_entry_points)

    assert plugins_mod.discover() == [module]            # 只读元数据
    assert calls and module not in plugins_mod.loaded_plugins()   # discover 不 import

    app = App(Config(transport="plug://x", events="null"))        # 未知 scheme → 懒发现
    assert isinstance(app.transport, MemoryTransport)             # 这里才触发（懒）
    assert module in plugins_mod.loaded_plugins()
    app.close()


def test_entry_point_with_attr_hook(tmp_path, monkeypatch):
    module = _write_module(
        tmp_path,
        monkeypatch,
        """
        from taskmq import TransportOptions
        from taskmq.plugins import register_transport
        from taskmq.transport.memory import MemoryTransport

        called = []


        def setup():
            called.append(True)
            register_transport("attrplug", lambda options: MemoryTransport())
        """,
        "ep_attr_plugin",
    )

    class _EntryPoint:
        value = f"{module}:setup"

    monkeypatch.setattr(
        plugins_mod, "_entry_points", lambda group=plugins_mod.ENTRY_POINT_GROUP: [_EntryPoint()]
    )
    app = App(Config(transport="attrplug://x", events="null"))
    assert isinstance(app.transport, MemoryTransport)              # 懒解析 → 触发 entry point
    assert importlib.import_module(module).called == [True]         # module:attr 的 attr 被调用
    app.close()


# -------------------------------------------------------------- codec / sink
def test_custom_codec_is_used_for_the_wire(tmp_path, monkeypatch):
    module = _write_module(tmp_path, monkeypatch, _PLUGIN_BODY, "codec_plugin")
    plugins_mod.load_plugins([module])

    app = App(Config(transport="memory://", serializer="plugjson", events="null"))

    @app.task(name="p.echo", queue="q")
    def echo(value: int) -> int:
        return value

    app.submit("p.echo", (3,))
    with Worker(app, queues=["q"]) as worker:
        worker.run_until_idle(timeout=10)
    assert app.transport.queue_stats(["q"])[0].pending == 0
    app.close()


def test_unknown_codec_and_sink_errors_list_options():
    with pytest.raises(Exception, match="register_codec"):
        get_codec("nope")
    with pytest.raises(ConfigError, match="register_sink"):
        App(Config(transport="memory://", events="carrot"))


def test_registered_pool_is_usable():
    class FakePool:
        name = "mypool"
        runs_in_child = False

        def __init__(self, options: Any) -> None:
            self.options = options

        def submit(self, fn: Any) -> None:
            fn()

        def call_body(self, fn: Any, args: tuple, kwargs: dict) -> Any:
            return fn(*args, **kwargs)

        def run_remote(self, payload: Any) -> BodyOutcome:
            raise ConfigError("本进程池不支持子进程")

        def shutdown(self, wait: bool = True) -> None:
            return None

    plugins_mod.register_pool("mypool", FakePool)
    pool = make_pool("mypool", 3)
    assert isinstance(pool, FakePool)
    assert pool.options.concurrency == 3
    with pytest.raises(ValueError, match="未知 pool"):
        make_pool("nope", 1)


# ------------------------------------------------------------- 子进程透传
def test_child_payload_carries_plugins(tmp_path, monkeypatch):
    module = _write_module(tmp_path, monkeypatch, "VALUE = 1\n", "child_plugin")
    app = App(Config(transport="memory://", events="null"), include=[module])

    @app.task(name="p.child", queue="q")
    def child() -> str:
        return "ok"

    captured: list[Any] = []

    class RecordingPool:
        name = "processes"
        runs_in_child = True

        def submit(self, fn: Any) -> None:
            fn()

        def call_body(self, fn: Any, args: tuple, kwargs: dict) -> Any:
            return fn(*args, **kwargs)

        def run_remote(self, payload: Any) -> BodyOutcome:
            captured.append(payload)
            return BodyOutcome(ok=True, state="SUCCEEDED", result="ok", runtime=0.0)

        def shutdown(self, wait: bool = True) -> None:
            return None

    app.submit("p.child")
    with Worker(app, queues=["q"], pool=RecordingPool()) as worker:   # type: ignore[arg-type]
        worker.run_until_idle(timeout=10)

    assert captured and captured[0].plugins == (module,)
    app.close()


def test_run_child_task_loads_plugins_from_payload(tmp_path, monkeypatch):
    module = _write_module(tmp_path, monkeypatch, "VALUE = 1\n", "payload_plugin")
    from taskmq.worker import execution as execution_mod

    child_app = App(Config(transport="memory://", events="null"))
    monkeypatch.setattr(execution_mod, "_CHILD_APP", child_app, raising=False)
    from taskmq import Envelope as _Envelope

    payload = execution_mod.ChildTask(
        app_spec="",
        envelope=_Envelope(task="absent"),
        worker_id="w",
        attempt=1,
        deliveries=1,
        plugins=(module,),
    )
    plugins_mod.reset_plugins(clear_registrations=False)     # 只清「已加载」记录，保留注册
    outcome = execution_mod.run_child_task(payload)
    assert outcome.ok is False and outcome.error_type == "TaskNotRegistered"
    assert module in plugins_mod.loaded_plugins()


def test_child_main_loads_plugins(tmp_path, monkeypatch):
    import multiprocessing

    from taskmq import Envelope
    from taskmq.worker import pool as pool_mod

    module = _write_module(tmp_path, monkeypatch, _PLUGIN_BODY, "main_plugin")
    spec = _write_module(
        tmp_path,
        monkeypatch,
        """
        from taskmq import App, Config

        app = App(Config(transport="memory://", serializer="json", events="null"))


        @app.task(name="p.job", queue="q")
        def job() -> str:
            return "ok"
        """,
        "child_main_app",
    )
    payload = pool_mod.ChildTask(
        app_spec=f"{spec}:app",
        envelope=Envelope(task="p.job"),
        worker_id="w",
        attempt=1,
        deliveries=1,
        plugins=(module,),
    )
    parent, child = multiprocessing.get_context("spawn").Pipe(duplex=False)
    pool_mod._child_main(payload, child)
    assert parent.recv().ok is True
    assert module in plugins_mod.loaded_plugins()


# --------------------------------------------------------------- limitations
def test_limitations_are_declared_and_shown_in_status(capsys):
    from taskmq.cli import _cmd_status
    from taskmq.transport.redis import RedisTransport

    assert MemoryTransport.limitations == {}
    assert "reap_expired_jobs" in RedisTransport.limitations
    assert "global_priority" in RedisTransport.cluster_limitations   # ?cluster=1 才生效的降级

    app = App(Config(transport="memory://", events="null"))
    app.transport.limitations = {"global_priority": "无平级加权轮询"}   # type: ignore[attr-defined]
    assert _cmd_status(app, argparse.Namespace(queues="", by_priority=False)) == 0
    out = capsys.readouterr().out
    assert "LIMITATIONS" in out and "无平级加权轮询" in out
    app.close()
