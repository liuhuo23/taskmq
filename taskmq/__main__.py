"""`python -m taskmq` 入口。

装包后正常用 console script（`taskmq ...`）；没装进 PATH、或者在源码目录里临时跑，就用
`python -m taskmq ...` —— 行为完全一样。
"""
from __future__ import annotations

from .cli import main

if __name__ == "__main__":  # pragma: no cover - 由解释器入口触发
    raise SystemExit(main())
