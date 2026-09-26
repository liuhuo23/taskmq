"""CLI 分支覆盖：加载错误、dlq/call、status、beat/dev/worker 循环。"""
from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest

from taskmq import Envelope
from taskmq.cli import main

_TEMPLATE = """from taskmq import App, Config
from taskmq.schedule import every

app = App(Config(transport={transport!r}, serializer="json", events="null"))


@app.task(name="cli.job", queue="q")
def job(x: int = 0) -> int:
    return x + 1


@app.task(name="cli.boom", queue="q")
def boom() -> None:
    raise RuntimeError("cli boom")

{schedules}"""


def _write(tmp_path: Path, monkeypatch, body: str, name: str) -> str:
    (tmp_path / f"{name}.py").write_text(body, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    return f"{name}:app"


def _app_module(
    tmp_path: Path, monkeypatch, *, name: str = "cli_more_app", schedules: str = "", transport: str = ""
) -> str:
    transport = transport or f"sqlite:///{tmp_path / (name + '.db')}"
    return _write(tmp_path, monkeypatch, _TEMPLATE.format(transport=transport, schedules=schedules), name)


def _drive_signal_loop(monkeypatch, cli_module) -> None:
    """把 signal 处理函数记下来，并在第一次 sleep 时触发（模拟 Ctrl-C）。"""
    handlers: dict[int, object] = {}

    def fake_signal(sig, handler):
        handlers[sig] = handler
        return handler

    def fake_sleep(seconds: float) -> None:
        for handler in list(handlers.values()):
            if callable(handler):
                handler(15, None)

    monkeypatch.setattr(cli_module.signal, "signal", fake_signal)
    monkeypatch.setattr(cli_module.time, "sleep", fake_sleep)


# ------------------------------------------------------------------ 加载错误
def test_cli_load_errors(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match="无法导入"):
        main(["--app", "no_such_module_xyz:app", "status"])
    with pytest.raises(SystemExit, match="不是 App 实例"):
        main(["--app", "taskmq:App", "status"])
    monkeypatch.delenv("TASKMQ_APP", raising=False)
    with pytest.raises(SystemExit):
        main(["status"])


# ------------------------------------------------------------------ call
def test_cli_call_errors_and_success(tmp_path, monkeypatch, capsys):
    spec = _app_module(tmp_path, monkeypatch)
    with pytest.raises(SystemExit, match="合法 JSON"):
        main(["--app", spec, "call", "cli.job", "--args", "{not json"])
    with pytest.raises(SystemExit, match="JSON 数组"):
        main(["--app", spec, "call", "cli.job", "--args", "{}"])

    assert main(["--app", spec, "call", "cli.job", "--args", "[1]"]) == 0
    assert json.loads(capsys.readouterr().out.strip()) == 2
    assert main(["--app", spec, "call", "cli.job", "--args", "[]", "--kwargs", '{"x": 4}']) == 0
    assert json.loads(capsys.readouterr().out.strip()) == 5

    assert main(["--app", spec, "call", "cli.boom"]) == 1
    assert "FAILED" in capsys.readouterr().err
    assert main(["--app", spec, "call", "no.such.task"]) == 1
    assert "FAILED" in capsys.readouterr().err


# ------------------------------------------------------------------ status
def test_cli_status_empty_queue_and_by_priority(tmp_path, monkeypatch, capsys):
    empty = _app_module(tmp_path, monkeypatch, name="cli_empty_app", transport="memory://")
    assert main(["--app", empty, "status"]) == 0
    assert "(没有队列)" in capsys.readouterr().out

    spec = _app_module(tmp_path, monkeypatch, name="cli_status_app")
    module = importlib.import_module("cli_status_app")
    module.app.transport.enqueue(Envelope(task="cli.job", priority=3), queue="q")
    assert main(["--app", spec, "status", "-Q", "q", "--by-priority"]) == 0
    out = capsys.readouterr().out
    assert "QUEUES" in out and "BY-PRIORITY" in out and "P3" in out


# ------------------------------------------------------------------ dlq
def test_cli_dlq_list_and_replay(tmp_path, monkeypatch, capsys):
    spec = _app_module(tmp_path, monkeypatch, name="cli_dlq_app")
    module = importlib.import_module("cli_dlq_app")
    transport = module.app.transport
    transport.enqueue(Envelope(task="cli.job"), queue="q")
    delivery = transport.reserve(["q"], worker_id="w", lease=30, limit=1)[0]
    transport.dead_letter(delivery, "boom\nsecond line")

    assert main(["--app", spec, "dlq", "list", "-Q", "q"]) == 0
    out = capsys.readouterr().out
    assert f"id={delivery.message_id}" in out and "reason=boom" in out

    with pytest.raises(SystemExit, match="--all 或 --id"):
        main(["--app", spec, "dlq", "replay"])
    with pytest.raises(SystemExit, match="超出合法范围"):
        main(["--app", spec, "dlq", "replay", "--priority", "999", "--all"])

    assert main(["--app", spec, "dlq", "replay", "--id", str(delivery.message_id)]) == 0
    assert "replayed 1/1" in capsys.readouterr().out
    assert main(["--app", spec, "dlq", "replay", "--all"]) == 1
    assert "replayed 0/0" in capsys.readouterr().out
    assert transport.queue_stats(["q"])[0].pending == 1


# ------------------------------------------------------------ worker/dev/beat
def test_cli_worker_and_dev_loops(tmp_path, monkeypatch, capsys):
    import taskmq.cli as cli_module

    spec = _app_module(tmp_path, monkeypatch, name="cli_loop_app")
    _drive_signal_loop(monkeypatch, cli_module)
    assert main(["--app", spec, "worker", "-Q", "q", "-c", "2", "--prefetch", "2"]) == 0
    capsys.readouterr()
    assert main(["--app", spec, "dev", "-Q", "q"]) == 0
    capsys.readouterr()


def test_cli_beat_errors_and_loops(tmp_path, monkeypatch, capsys):
    import taskmq.cli as cli_module

    no_sched = _app_module(tmp_path, monkeypatch, name="cli_nosched_app")
    with pytest.raises(SystemExit, match="没有调度"):
        main(["--app", no_sched, "beat", "--once"])

    _write(
        tmp_path,
        monkeypatch,
        "from taskmq.schedule import every\n"
        "schedules = [every('cli.job', seconds=60, misfire='run_once')]\n",
        "cli_sched_list",
    )
    with pytest.raises(SystemExit, match="module:attr"):
        main(["--app", no_sched, "beat", "--once", "--schedule", "bogus"])
    with pytest.raises(SystemExit, match="需要 App 或 Schedule 列表"):
        main(["--app", no_sched, "beat", "--once", "--schedule", "cli_sched_list:nope"])

    state = tmp_path / "beat_once.json"
    assert (
        main(
            [
                "--app",
                no_sched,
                "beat",
                "--once",
                "--schedule",
                "cli_sched_list:schedules",
                "--state",
                str(state),
            ]
        )
        == 0
    )
    assert capsys.readouterr().out.startswith("fired 0")            # 首次只记基准

    scheduled = _app_module(
        tmp_path,
        monkeypatch,
        name="cli_beat_app",
        schedules="\napp.schedule(every('cli.job', seconds=1, misfire='run_once'))\n",
    )
    _drive_signal_loop(monkeypatch, cli_module)
    assert (
        main(
            [
                "--app",
                scheduled,
                "beat",
                "--poll",
                "0.01",
                "--state",
                str(tmp_path / "beat_loop.json"),
            ]
        )
        == 0
    )
    assert "beat 退出" in capsys.readouterr().out

    assert main(["--app", scheduled, "dev", "-Q", "q", "--state", str(tmp_path / "dev.json")]) == 0
    capsys.readouterr()
