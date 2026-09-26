"""CLI 冒烟测试：call / worker --once / status / dlq list+replay。"""
from __future__ import annotations

import importlib
from pathlib import Path

from taskmq.cli import main


def _prepare(tmp_path: Path, monkeypatch) -> str:
    db = tmp_path / "cli.db"
    module = tmp_path / "cli_sample_app.py"
    module.write_text(
        "from taskmq import App, Config, Retry\n"
        "\n"
        f"app = App(Config(transport='sqlite:///{db}', serializer='json', concurrency=1))\n"
        "\n"
        "\n"
        "@app.task(name='cli.add', queue='q')\n"
        "def add(a: int, b: int) -> int:\n"
        "    return a + b\n"
        "\n"
        "\n"
        "@app.task(name='cli.boom', queue='q', retry=Retry(max_attempts=1, base=0.0, jitter=False))\n"
        "def boom() -> None:\n"
        "    raise RuntimeError('boom')\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    monkeypatch.chdir(tmp_path)
    return "cli_sample_app:app"


def test_cli_call_runs_inline(tmp_path, monkeypatch, capsys):
    spec = _prepare(tmp_path, monkeypatch)
    assert main(["--app", spec, "call", "cli.add", "--args", "[1,2]"]) == 0
    assert capsys.readouterr().out.strip() == "3"


def test_cli_worker_once_status_and_dlq(tmp_path, monkeypatch, capsys):
    spec = _prepare(tmp_path, monkeypatch)
    module = importlib.import_module("cli_sample_app")
    app = module.app

    ok = app.submit("cli.add", (2, 3))
    bad = app.submit("cli.boom")

    assert main(["--app", spec, "worker", "--once", "-Q", "q"]) == 0
    capsys.readouterr()

    assert ok.successful()
    assert ok.get(timeout=5) == 5
    assert bad.state == "FAILED"

    assert main(["--app", spec, "status", "-Q", "q", "--by-priority"]) == 0
    out = capsys.readouterr().out
    assert "QUEUES" in out and "dead=1" in out

    assert main(["--app", spec, "dlq", "list", "-Q", "q"]) == 0
    listing = capsys.readouterr().out
    assert "cli.boom" in listing

    message_id = int(listing.split("id=")[1].split()[0])
    assert main(["--app", spec, "dlq", "replay", "--id", str(message_id), "--priority", "3"]) == 0
    assert "replayed 1/1" in capsys.readouterr().out
    assert app.transport.priority_stats(["q"]) == {3: 1}
