"""优先级常量与校验（决策 §20-9 / docs/design/priority.md v1.0）。

约定：
- **数值越大越优先**，合法范围 `-9..9`（P1）。
- 越界/非整数在 `App()` 构造期或 submit 时抛 `ConfigError`，**不 clamp**。
- 排序键 `(priority DESC, visible_at ASC, id ASC)`；队列权重只在**同优先级档位内**生效（P3）。
"""
from __future__ import annotations

from .errors import ConfigError

PRIORITY_MIN = -9
PRIORITY_MAX = 9


class Priority:
    """便捷常量（P11）。纯常量，不引入任何隐式行为。"""

    LOW = -5
    NORMAL = 0
    HIGH = 5
    CRITICAL = 9
    MIN = PRIORITY_MIN
    MAX = PRIORITY_MAX


def validate_priority(value: object, *, where: str = "priority") -> int:
    """校验并返回优先级整数；非法值抛 `ConfigError`。"""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{where} 必须是整数（{PRIORITY_MIN}..{PRIORITY_MAX}），收到 {value!r}")
    if not PRIORITY_MIN <= value <= PRIORITY_MAX:
        raise ConfigError(
            f"{where}={value} 超出合法范围 {PRIORITY_MIN}..{PRIORITY_MAX}（不 clamp，请显式修正）"
        )
    return value


__all__ = ["PRIORITY_MIN", "PRIORITY_MAX", "Priority", "validate_priority"]
