"""定时调度：`cron()` / `every()`（docs/design.md §13）。

只依赖标准库：cron 解析自己实现（5 字段 + `@alias`），时区用 `zoneinfo`。
`next_fire(after)` 返回**绝对 epoch 秒**，beat 用它决定「到点了吗」。

语义写清楚：
- cron 字段：`*  a  a-b  a,b  */n  a-b/n`；weekday 0=周日（也接受 7=周日）；
- day-of-month 与 day-of-week **同时限定**时按 Vixie 规则取**或**；
- `every(seconds=…)` 以 epoch 对齐（默认锚点 0），所以 300s 就是 :00/:05/:10…
"""
from __future__ import annotations

import dataclasses
import math
import re
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ._compat import _SLOTS
from .errors import ConfigError

__all__ = ["Schedule", "CronExpr", "cron", "every", "parse_cron"]

_RANGES: dict[str, tuple[int, int]] = {
    "minute": (0, 59),
    "hour": (0, 23),
    "day": (1, 31),
    "month": (1, 12),
    "weekday": (0, 6),
}
_ALIASES = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}
_MISFIRE = ("skip", "run_once")
_PART = re.compile(r"^(?P<base>\*|\d+(?:-\d+)?)(?:/(?P<step>\d+))?$")


def _parse_field(text: str, field: str) -> tuple[frozenset[int], bool]:
    """返回 (取值集合, 是否 `*`)；越界/格式错抛 ConfigError。"""
    low, high = _RANGES[field]
    allowed_high = 7 if field == "weekday" else high   # cron 允许 7 = 周日
    values: set[int] = set()
    starred = False
    for raw in text.split(","):
        part = raw.strip()
        if not part:
            raise ConfigError(f"cron {field} 字段有空项：{text!r}")
        matched = _PART.match(part)
        if matched is None:
            raise ConfigError(f"cron {field} 字段非法：{part!r}")
        base = matched.group("base")
        step = int(matched.group("step") or 1)
        if step < 1:
            raise ConfigError(f"cron {field} 步长必须 >= 1：{part!r}")
        if base == "*":
            start, end = low, high
            starred = True
        elif "-" in base:
            start_text, _, end_text = base.partition("-")
            start, end = int(start_text), int(end_text)
        else:
            start = end = int(base)
            if step > 1:
                end = high
        if start < low or end > allowed_high or start > end:
            raise ConfigError(f"cron {field} 字段越界（{low}-{allowed_high}）：{part!r}")
        values.update(range(start, end + 1, step))
    if field == "weekday" and 7 in values:
        values.discard(7)
        values.add(0)
    return frozenset(values), starred


@dataclasses.dataclass(frozen=True, **_SLOTS)
class CronExpr:
    """5 字段 cron 表达式。"""

    expr: str
    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]
    day_star: bool
    weekday_star: bool

    def _day_matches(self, moment: datetime) -> bool:
        day_ok = moment.day in self.days
        weekday_ok = ((moment.weekday() + 1) % 7) in self.weekdays   # 周一=0 → 周日=0
        if self.day_star and self.weekday_star:
            return True
        if self.day_star:
            return weekday_ok
        if self.weekday_star:
            return day_ok
        return day_ok or weekday_ok                                  # Vixie：两者都限定取“或”

    def matches(self, moment: datetime) -> bool:
        return (
            moment.minute in self.minutes
            and moment.hour in self.hours
            and moment.month in self.months
            and self._day_matches(moment)
        )

    def _first_time_on(self, moment: datetime) -> datetime | None:
        """这一天里第一个 >= `moment` 的 (时, 分) 组合。"""
        for hour in sorted(self.hours):
            for minute in sorted(self.minutes):
                candidate = moment.replace(hour=hour, minute=minute)
                if candidate >= moment:
                    return candidate
        return None

    def next_fire(self, after: datetime) -> datetime:
        """严格晚于 `after` 的下一个匹配时刻（按分钟对齐）。"""
        probe = (after + timedelta(minutes=1)).replace(second=0, microsecond=0)
        for _ in range(366 * 4):
            if probe.month in self.months and self._day_matches(probe):
                candidate = self._first_time_on(probe)
                if candidate is not None:
                    return candidate
            probe = (probe + timedelta(days=1)).replace(hour=0, minute=0)
        raise ConfigError(f"cron 表达式 {self.expr!r} 在 4 年内没有匹配时间")


def parse_cron(expr: str) -> CronExpr:
    text = expr.strip()
    if not text:
        raise ConfigError("cron 表达式不能为空")
    if text.startswith("@"):
        alias = _ALIASES.get(text.lower())
        if alias is None:
            raise ConfigError(f"未知的 cron 别名 {text!r}，可选 {sorted(_ALIASES)}")
        text = alias
    fields = text.split()
    if len(fields) != 5:
        raise ConfigError(f"cron 需要 5 个字段（分 时 日 月 周），收到 {expr!r}")
    minutes, _ = _parse_field(fields[0], "minute")
    hours, _ = _parse_field(fields[1], "hour")
    days, day_star = _parse_field(fields[2], "day")
    months, _ = _parse_field(fields[3], "month")
    weekdays, weekday_star = _parse_field(fields[4], "weekday")
    return CronExpr(
        expr=expr.strip(),
        minutes=minutes,
        hours=hours,
        days=days,
        months=months,
        weekdays=weekdays,
        day_star=day_star,
        weekday_star=weekday_star,
    )


@dataclasses.dataclass(frozen=True, **_SLOTS)
class Schedule:
    """一条调度：`cron` 与 `every` 共用一个结构（便于序列化/落状态）。"""

    task: str
    cron: CronExpr | None = None
    seconds: float | None = None
    anchor: float = 0.0
    tz: str = "UTC"
    name: str = ""
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    queue: str | None = None
    priority: int | None = None
    misfire: str = "skip"

    def entry_name(self) -> str:
        if self.name:
            return self.name
        what = self.cron.expr if self.cron is not None else f"every {self.seconds:g}s"
        return f"{self.task} [{what}]"

    def next_fire(self, after: float) -> float:
        """`after`（epoch 秒）之后的下一次触发时间。"""
        if self.cron is not None:
            zone = self._zone()
            moment = datetime.fromtimestamp(after, tz=zone)
            return self.cron.next_fire(moment).timestamp()
        assert self.seconds is not None
        step = float(self.seconds)
        slots = math.floor((after - self.anchor) / step) + 1
        return self.anchor + slots * step

    def _zone(self) -> ZoneInfo:
        try:
            return ZoneInfo(self.tz)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ConfigError(f"未知时区 {self.tz!r}：{exc}") from exc


def _validate_common(misfire: str, tz: str) -> None:
    if misfire not in _MISFIRE:
        raise ConfigError(f'misfire 可选 {_MISFIRE}，收到 {misfire!r}')
    try:
        ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ConfigError(f"未知时区 {tz!r}：{exc}") from exc


def cron(
    task: str,
    expr: str,
    *,
    tz: str = "UTC",
    name: str = "",
    args: tuple[Any, ...] = (),
    kwargs: Mapping[str, Any] | None = None,
    queue: str | None = None,
    priority: int | None = None,
    misfire: str = "skip",
) -> Schedule:
    """`cron("send_report", "0 9 * * *", tz="Asia/Shanghai")`。"""
    _validate_common(misfire, tz)
    if not task:
        raise ConfigError("cron 需要任务名（或 `name=…` 时用 task 指定）")
    return Schedule(
        task=task,
        cron=parse_cron(expr),
        tz=tz,
        name=name,
        args=tuple(args),
        kwargs=dict(kwargs or {}),
        queue=queue,
        priority=priority,
        misfire=misfire,
    )


def every(
    task: str,
    *,
    seconds: float | None = None,
    minutes: float | None = None,
    hours: float | None = None,
    name: str = "",
    args: tuple[Any, ...] = (),
    kwargs: Mapping[str, Any] | None = None,
    queue: str | None = None,
    priority: int | None = None,
    misfire: str = "skip",
    anchor: float = 0.0,
) -> Schedule:
    """`every("cleanup", minutes=5)`：固定间隔（epoch 对齐）。"""
    given = [value for value in (seconds, minutes, hours) if value is not None]
    if len(given) != 1:
        raise ConfigError("every() 需要且只需要一个间隔（seconds / minutes / hours）")
    step = float(given[0])
    if step <= 0:
        raise ConfigError(f"间隔必须 > 0，收到 {step}")
    if seconds is not None:
        total = step
    elif minutes is not None:
        total = step * 60.0
    else:
        total = step * 3600.0
    return Schedule(
        task=task,
        seconds=total,
        anchor=float(anchor),
        name=name,
        args=tuple(args),
        kwargs=dict(kwargs or {}),
        queue=queue,
        priority=priority,
        misfire=misfire,
    )
