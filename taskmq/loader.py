"""`module:attr` -> App 的加载器（CLI 与进程池子进程共用）。"""
from __future__ import annotations

import importlib
from typing import Any

from .errors import ConfigError

__all__ = ["load_app"]


def load_app(spec: str) -> Any:
    """按 `module:attr` 加载 App；失败抛 `ConfigError`（调用方决定怎么呈现）。"""
    if not spec or ":" not in spec:
        raise ConfigError(f"需要 module:attr 形式的 App 位置，收到 {spec!r}")
    module_name, _, attr = spec.partition(":")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ConfigError(f"无法导入 {module_name}：{exc}") from exc
    app = getattr(module, attr, None)
    from .app import App

    if not isinstance(app, App):
        raise ConfigError(f"{spec} 不是 App 实例（拿到 {type(app).__name__}）")
    return app
