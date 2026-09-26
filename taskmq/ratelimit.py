"""限流：token bucket（worker 内生效，§10.5）。

跨 worker 的分布式限流属 Phase 2；这里只保证「单个 worker 内不超过配置速率」，并在
超限时用 `transport.defer()` 把消息**不消耗投递次数**地推后。
"""
from __future__ import annotations

import dataclasses
import re
import time
from collections.abc import Callable

from .errors import ConfigError

_UNITS = {
    "s": 1.0, "sec": 1.0, "second": 1.0, "seconds": 1.0,
    "m": 60.0, "min": 60.0, "minute": 60.0, "minutes": 60.0,
    "h": 3600.0, "hour": 3600.0, "hours": 3600.0,
}
_PATTERN = re.compile(r"^\s*(\d+)\s*/\s*([A-Za-z]+)\s*$")


@dataclasses.dataclass(frozen=True, slots=True)
class RateLimit:
    """`"100/m"` 这类速率声明。"""

    limit: int
    window: float

    def __post_init__(self) -> None:
        if self.limit < 1:
            raise ConfigError(f"rate_limit 的额度必须 >= 1，收到 {self.limit}")
        if self.window <= 0:
            raise ConfigError(f"rate_limit 的窗口必须 > 0，收到 {self.window}")

    @property
    def rate(self) -> float:
        """每秒允许的次数。"""
        return self.limit / self.window

    @classmethod
    def parse(cls, spec: str | RateLimit) -> RateLimit:
        if isinstance(spec, RateLimit):
            return spec
        if not isinstance(spec, str):
            raise ConfigError(f'rate_limit 需要 "100/m" 这样的字符串，收到 {spec!r}')
        matched = _PATTERN.match(spec)
        if matched is None:
            raise ConfigError(f'rate_limit 格式应为 "100/m" / "10/s" / "1000/h"，收到 {spec!r}')
        limit = int(matched.group(1))
        unit = matched.group(2).lower()
        if unit not in _UNITS:
            raise ConfigError(f"未知的 rate_limit 单位 {unit!r}，可选 {sorted(set(_UNITS))}")
        return cls(limit=limit, window=_UNITS[unit])

    def __str__(self) -> str:
        return f"{self.limit}/{self.window:g}s"


class TokenBucket:
    """初始满桶（允许一次性突发 = limit），之后按速率连续补充。"""

    def __init__(self, limit: RateLimit, *, clock: Callable[[], float] | None = None) -> None:
        self._limit = limit
        self._clock = clock or time.monotonic
        self._tokens = float(limit.limit)
        self._updated = self._clock()

    @property
    def limit(self) -> RateLimit:
        return self._limit

    def acquire(self) -> float | None:
        """拿到令牌返回 `None`；否则返回需要等待的秒数（不消耗令牌）。"""
        now = self._clock()
        elapsed = max(0.0, now - self._updated)
        self._updated = now
        self._tokens = min(float(self._limit.limit), self._tokens + elapsed * self._limit.rate)
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return None
        return (1.0 - self._tokens) / self._limit.rate


__all__ = ["RateLimit", "TokenBucket"]
