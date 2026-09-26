"""Python 3.9 / 3.10+ 的差异集中在这里（只放语法与标准库层面的）。

代码里到处是 `from __future__ import annotations`，所以注解里的 PEP 604（`X | None`）在 3.9 上没问题
（注解不求值）。真正会炸的只有两处：

- `dataclasses.dataclass(slots=True)`：3.10 才有的参数 → 3.9 展开成空 dict，语义完全不变，只是没有
  `__slots__`（省内存的优化拿不到）；
- `typing.ParamSpec` / `typing.Concatenate`：3.10 才有 → 3.9 从 `typing_extensions` 拿（条件依赖，
  见 pyproject 的 `dependencies`）。

用的时候保持"能被类型检查器看见"的写法（mypy 的 dataclass 插件与 pyright 都能正确展开 `**`）：

```python
@dataclasses.dataclass(frozen=True, **_SLOTS)
class Delivery: ...
```
"""
from __future__ import annotations

import sys
from typing import Any

#: 3.10+ 传 `slots=True`；3.9 传空（`dataclasses.dataclass` 在 3.9 上不认这个参数）
_SLOTS: dict[str, Any] = {"slots": True} if sys.version_info >= (3, 10) else {}

__all__ = ["_SLOTS"]
