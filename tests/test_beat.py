"""beat：cron/interval、DST、misfire、lease 选主、状态文件、CLI。"""
from __future__ import annotations

import importlib
import json
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from taskmq import App, Config
from taskmq.errors import ConfigError
from taskmq.schedule import cron, every, parse_cron
from taskmq.worker.beat import Beat


def _app(**overrides) -> App:
    config = {"transport": "memory://", "serializer": "json", "events": "null"}
    config.update(overrides)
    return App(Config(**config))


def _task_app(name: str = "beat.job") -> App:
    app = _app()

    @app.task(name=name, queue="beat")
    def job() -> str:
        return "ok"

    return app


# ------------------------------------------------------------------ cron 解析
def test_cron_parsing_variants_and_errors():
    assert parse_cron("*/15 * * * *").minutes == frozenset({0, 15, 30, 45})
    assert parse_cron("1,2,3 * * * *").minutes == frozenset({1, 2, 3})
    assert parse_cron("0 9-11 * * *").hours == frozenset({9, 10, 11})
    assert parse_cron("0 0 * * 7").weekdays == frozenset({0})       # 7 = 周日
    assert parse_cron("@daily").hours == frozenset({0})
    for bad in ("0 9 * *", "60 * * * *", "0 24 * * *", "0 0 0 * *", "0 0 * 13 *", "@nope", "x"):
        with pytest.raises(ConfigError):
            parse_cron(bad)


def test_cron_next_fire_daily_and_weekday():
    zone = ZoneInfo("Asia/Shanghai")
    daily = parse_cron("0 9 * * *")
    start = datetime(2026, 3, 7, 10, 0, tzinfo=zone)                # 周六 10:00
    assert daily.next_fire(start) == datetime(2026, 3, 8, 9, 0, tzinfo=zone)

    weekday = parse_cron("30 8 * * 1-5")
    assert weekday.next_fire(start) == datetime(2026, 3, 9, 8, 30, tzinfo=zone)   # 跳到周一


def test_cron_day_of_month_or_weekday_vixie_rule():
    zone = ZoneInfo("UTC")
    expr = parse_cron("0 0 1 * 1")                                   # 每月 1 号 **或** 周一
    assert expr.next_fire(datetime(2026, 3, 2, 0, 0, tzinfo=zone)) == datetime(
        2026, 3, 9, 0, 0, tzinfo=zone
    )


def test_cron_dst_boundary_keeps_local_hour():
    zone = ZoneInfo("America/New_York")
    entry = cron("beat.dst", "0 9 * * *", tz="America/New_York")      # 2026-03-08 是美东夏令时切换日
    cursor = datetime(2026, 3, 7, 0, 0, tzinfo=zone).timestamp()
    fires = []
    for _ in range(3):
        cursor = entry.next_fire(cursor)
        fires.append(cursor)

    local = [datetime.fromtimestamp(ts, tz=zone) for ts in fires]
    assert [dt.hour for dt in local] == [9, 9, 9]                     # 本地时间始终 9 点
    assert [dt.strftime("%Y-%m-%d") for dt in local] == ["2026-03-07", "2026-03-08", "2026-03-09"]
    assert fires[1] - fires[0] == pytest.approx(23 * 3600)            # 春季拨快 1 小时
    assert fires[2] - fires[1] == pytest.approx(24 * 3600)


def test_interval_is_epoch_aligned():
    entry = every("beat.job", seconds=60)
    anchor = 1790435249.914
    nxt = entry.next_fire(anchor)
    assert nxt % 60 == 0 and nxt > anchor
    assert every("beat.job", minutes=5).seconds == 300
    with pytest.raises(ConfigError):
        every("beat.job")                                             # 必须给一个间隔
    with pytest.raises(ConfigError):
        every("beat.job", seconds=1, minutes=1)
    with pytest.raises(ConfigError):
        cron("beat.job", "0 9 * * *", tz="Mars/Olympus")


# ------------------------------------------------------------------ beat 行为
def test_beat_first_tick_records_baseline_then_fires(tmp_path):
    app = _task_app()
    entry = every("beat.job", seconds=60)
    beat = Beat(app, [entry], state_path=tmp_path / "state.json")
    start = 1_800_000_000.0

    assert beat.tick(start) == []                                     # 首次只记基准，不补跑
    assert (tmp_path / "state.json").exists()
    assert beat.tick(start + 61) == [entry.entry_name()]              # 到点触发
    assert beat.tick(start + 61) == []                                # 同一时刻不会重复触发
    assert beat.fired == 1


def test_misfire_skip_vs_run_once(tmp_path):
    start = 1_800_000_000.0
    old = start - 3600                                                # 错过 60 次
    for policy, expected_fires in (("skip", 0), ("run_once", 1)):
        app = _task_app()
        entry = every("beat.job", seconds=60, misfire=policy)
        state_path = tmp_path / f"{policy}.json"
        state_path.write_text(json.dumps({"entries": {entry.entry_name(): old}}), encoding="utf-8")
        beat = Beat(app, [entry], state_path=state_path)
        fired = beat.tick(start)
        assert len(fired) == expected_fires
        assert beat.skipped == (1 if policy == "skip" else 0)


def test_lease_election_only_leader_fires(tmp_path):
    app = _task_app()
    entry = every("beat.job", seconds=1, misfire="run_once")
    start = 1_800_000_000.0

    first = Beat(app, [entry], state_path=tmp_path / "a.json", owner="beat-a")
    second = Beat(app, [entry], state_path=tmp_path / "b.json", owner="beat-b")

    first.tick(start)
    second.tick(start)                                                # 抢不到租约 → 待命
    assert first.is_leader is True and second.is_leader is False
    assert second.standby >= 1

    assert first.tick(start + 2) == [entry.entry_name()]
    assert second.tick(start + 2) == []                               # 非 leader 绝不触发
    assert second.fired == 0

    first.close()                                                     # 释放后 follower 才能上位
    third = Beat(app, [entry], state_path=tmp_path / "c.json", owner="beat-c")
    third.tick(start)
    assert third.is_leader is True


def test_state_file_survives_restart(tmp_path):
    app = _task_app()
    entry = every("beat.job", seconds=60)
    state_path = tmp_path / "state.json"
    start = 1_800_000_000.0

    first = Beat(app, [entry], state_path=state_path)
    first.tick(start)
    assert first.tick(start + 61) == [entry.entry_name()]
    first.close()

    second = Beat(app, [entry], state_path=state_path)                # 重启：同一时刻不重复触发
    assert second.tick(start + 61) == []


def test_beat_rejects_unknown_task(tmp_path):
    app = _app()
    with pytest.raises(ValueError):
        Beat(app, [every("missing.task", seconds=5)], state_path=tmp_path / "s.json")


def test_app_schedule_registration():
    app = _task_app()
    entry = every("beat.job", seconds=5)
    app.schedule(entry)
    assert app.schedules == [entry]
    with pytest.raises(ConfigError):
        app.schedule("not-a-schedule")  # type: ignore[arg-type]


# ------------------------------------------------------------------ CLI
def test_cli_beat_once(tmp_path, monkeypatch, capsys):
    spec = _write_schedule_module(tmp_path, monkeypatch)
    module = importlib.import_module("beat_sample_app")
    entry = module.app.schedules[0]
    state = tmp_path / "cli_state.json"
    state.write_text(
        json.dumps({"entries": {entry.entry_name(): time.time() - 300}}), encoding="utf-8"
    )

    from taskmq.cli import main

    assert main(["--app", spec, "beat", "--once", "--state", str(state)]) == 0
    out = capsys.readouterr().out
    assert out.startswith("fired 1")
    handle = next(iter(module.app.transport.queue_stats([entry.queue or "beat"])))
    assert handle.pending == 1                                        # 任务已入队


def _write_schedule_module(tmp_path: Path, monkeypatch) -> str:
    module_path = tmp_path / "beat_sample_app.py"
    module_path.write_text(
        "from taskmq import App, Config\n"
        "from taskmq.schedule import every\n"
        "\n"
        "app = App(Config(transport='memory://', serializer='json', events='null'))\n"
        "\n"
        "\n"
        "@app.task(name='beat.sample', queue='beat')\n"
        "def sample() -> str:\n"
        "    return 'ok'\n"
        "\n"
        "\n"
        "app.schedule(every('beat.sample', seconds=60, misfire='run_once'))\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    return "beat_sample_app:app"
