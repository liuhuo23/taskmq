"""测试通用夹具：确保仓库根在 sys.path 上（也让 `uv run pytest` 之外的调用方式可用）。"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
