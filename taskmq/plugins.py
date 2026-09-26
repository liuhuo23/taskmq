"""插件注册表：让第三方后端/组件**不改核心**就接进来（docs/design/plugins.md）。

三条接入路径（能力等价）：

1. **打包 + entry point**：插件包声明
   `[project.entry-points."taskmq.plugins"] rocketmq = "taskmq_rocketmq"`，用户只写
   `Config(transport="rocketmq://...")`；
2. **显式加载**：`app.load_plugins(["mycompany.mq"])` / `TASKMQ_PLUGINS=` / CLI `--plugins`；
3. **直接给实例**：`Config(transport=<Transport 实例>)`（框架不干预生命周期）。

约束（都是刻意的）：

- **内建优先**：插件要覆盖内建名字必须显式 `override=True`；
- **懒发现**：只有出现未知 scheme 时才读 entry points，标准部署不 import 任何第三方包；
- **不做** `sys.path` 目录扫描、不做动态下载（供应链风险）；
- 插件即代码，信任边界 = 已安装的包 + 显式配置；加载失败**立刻报错**（不静默降级）。
"""
from __future__ import annotations

import dataclasses
import importlib
import importlib.metadata as importlib_metadata
import logging
import os
import threading
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, cast

from ._compat import _SLOTS
from .errors import ConfigError

__all__ = [
    "ENTRY_POINT_GROUP",
    "PLUGINS_ENV",
    "BUILTIN_TRANSPORTS",
    "BUILTIN_CODECS",
    "BUILTIN_SINKS",
    "BUILTIN_POOLS",
    "TransportOptions",
    "PoolOptions",
    "register_transport",
    "register_codec",
    "register_sink",
    "register_pool",
    "transport_factory",
    "codec_factory",
    "sink_factory",
    "pool_factory",
    "known_transports",
    "known_codecs",
    "known_sinks",
    "known_pools",
    "discover",
    "env_plugins",
    "load_plugin",
    "load_plugins",
    "loaded_plugins",
    "reset_plugins",
]

logger = logging.getLogger("taskmq.plugins")

#: entry point 组名（插件包在 pyproject 里声明这个组）
ENTRY_POINT_GROUP = "taskmq.plugins"
#: 显式插件模块的环境变量（逗号分隔）
PLUGINS_ENV = "TASKMQ_PLUGINS"

BUILTIN_TRANSPORTS = ("memory", "sqlite", "redis", "postgres", "postgresql", "amqp")
BUILTIN_CODECS = ("json", "msgspec")
BUILTIN_SINKS = ("stdout", "null", "otel")
BUILTIN_POOLS = ("solo", "threads", "processes", "asyncio")


@dataclasses.dataclass(frozen=True, **_SLOTS)
class TransportOptions:
    """框架交给 transport 工厂的全部上下文（插件自己解析 URL 里自己的参数）。

    插件应当**只依赖**这里给的东西 + `taskmq.transport` / `taskmq.protocol` 的公开名字，
    不要 import `taskmq.worker.*` 或任何下划线开头的内部结构（docs/design/plugins.md §7）。
    """

    url: str
    codec: Any
    codec_registry: Any
    max_message_bytes: int
    idempotency_ttl: float
    config: Any


@dataclasses.dataclass(frozen=True, **_SLOTS)
class PoolOptions:
    """池工厂的上下文（预留扩展点，未承诺稳定性）。"""

    name: str
    concurrency: int
    app_spec: str | None = None


class _Registry:
    """一个扩展点的注册表：名字 -> (工厂, 来源)。"""

    def __init__(self, kind: str, builtins: Sequence[str]) -> None:
        self.kind = kind
        self.builtins = tuple(builtins)
        self._items: dict[str, tuple[Any, str]] = {}
        self._lock = threading.RLock()

    def register(self, name: str, factory: Any, *, override: bool = False) -> None:
        if not callable(factory):
            raise ConfigError(f"{self.kind} 工厂必须可调用，收到 {factory!r}")
        key = str(name).strip()
        if not key:
            raise ConfigError(f"{self.kind} 名字不能为空")
        with self._lock:
            if key in self.builtins and not override:
                raise ConfigError(f"{key!r} 是内建 {self.kind}；要覆盖请显式 override=True")
            existing = self._items.get(key)
            if existing is not None and not override:
                raise ConfigError(
                    f"{self.kind} {key!r} 已被 {existing[1]} 注册；要覆盖请显式 override=True"
                )
            self._items[key] = (factory, getattr(factory, "__module__", "?"))

    def get(self, name: str) -> Any | None:
        with self._lock:
            item = self._items.get(name)
            return None if item is None else item[0]

    def sources(self) -> dict[str, str]:
        with self._lock:
            return {key: origin for key, (_, origin) in self._items.items()}

    def __contains__(self, name: str) -> bool:
        with self._lock:
            return name in self._items


_transports = _Registry("transport", BUILTIN_TRANSPORTS)
_codecs = _Registry("codec", BUILTIN_CODECS)
_sinks = _Registry("sink", BUILTIN_SINKS)
_pools = _Registry("pool", BUILTIN_POOLS)

_loaded: list[str] = []
_load_lock = threading.RLock()
_entry_points_loaded = False


# --------------------------------------------------------------------- 注册
def register_transport(
    scheme: str, factory: Callable[[TransportOptions], Any], *, override: bool = False
) -> None:
    """注册一个 transport scheme（`factory(options) -> Transport`）。"""
    _transports.register(scheme, factory, override=override)


def register_codec(name: str, factory: Callable[[Any], Any], *, override: bool = False) -> None:
    """注册一个序列化器名（`factory(codec_registry) -> Codec`）。"""
    _codecs.register(name, factory, override=override)


def register_sink(name: str, factory: Callable[[], Any], *, override: bool = False) -> None:
    """注册一个事件 sink 名（`factory() -> EventSink`）。"""
    _sinks.register(name, factory, override=override)


def register_pool(name: str, factory: Callable[[PoolOptions], Any], *, override: bool = False) -> None:
    """注册一个执行池名（**预留**扩展点：接口稳定但暂未承诺语义）。"""
    _pools.register(name, factory, override=override)


def transport_factory(scheme: str) -> Any | None:
    return _transports.get(scheme)


def codec_factory(name: str) -> Any | None:
    return _codecs.get(name)


def sink_factory(name: str) -> Any | None:
    return _sinks.get(name)


def pool_factory(name: str) -> Any | None:
    return _pools.get(name)


def known_transports() -> dict[str, str]:
    """`scheme -> 来源`（内建或注册工厂所属模块）。"""
    return _transports.sources()


def known_codecs() -> dict[str, str]:
    return _codecs.sources()


def known_sinks() -> dict[str, str]:
    return _sinks.sources()


def known_pools() -> dict[str, str]:
    return _pools.sources()


# --------------------------------------------------------------------- 发现
def _entry_points(group: str) -> list[Any]:
    """兼容 3.10（dict 形态）与 3.12+（`select`）两套 API。"""
    try:
        found = importlib_metadata.entry_points()
    except Exception as exc:  # pragma: no cover - 元数据坏了
        logger.warning("读取 entry points 失败（忽略）：%s", exc)
        return []
    if hasattr(found, "select"):
        return list(found.select(group=group))
    grouped = getattr(found, "get", None)          # Python 3.10 的 dict 形态
    if not callable(grouped):
        return []
    lookup = cast("Callable[[str, list[Any]], list[Any]]", grouped)
    return list(lookup(group, []))


def discover(*, group: str = ENTRY_POINT_GROUP) -> list[str]:
    """列出 entry point 组里的 `module[:attr]` 目标（只读元数据，**不 import**）。"""
    targets: list[str] = []
    for entry in _entry_points(group):
        value = str(getattr(entry, "value", "")).strip()
        if value and value not in targets:
            targets.append(value)
    return targets


def env_plugins(environ: Mapping[str, str] | None = None) -> list[str]:
    """读 `TASKMQ_PLUGINS`（逗号分隔）。"""
    env = os.environ if environ is None else environ
    raw = env.get(PLUGINS_ENV, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def load_plugin(module: str, attr: str | None = None) -> str:
    """import 一个插件模块（导入副作用即注册）。幂等；失败抛 `ConfigError`。

    `attr` 可选：entry point 写成 `pkg:setup` 时导入后调用它（便于插件做参数化注册）。
    """
    if not isinstance(module, str) or not module.strip():
        raise ConfigError(f"插件模块名必须是非空字符串，收到 {module!r}")
    target = module.strip()
    with _load_lock:
        if target in _loaded and not attr:
            return target
        try:
            imported = importlib.import_module(target)
        except Exception as exc:  # noqa: BLE001 - 插件任何导入错误都要变成可读的配置错误
            raise ConfigError(f"插件 {target!r} 加载失败：{type(exc).__name__}: {exc}") from exc
        if attr:
            hook = getattr(imported, attr, None)
            if not callable(hook):
                raise ConfigError(f"插件 {target!r} 没有可调用的入口 {attr!r}")
            hook()
        if target not in _loaded:
            _loaded.append(target)
        logger.debug("插件已加载：%s", target)
        return target


def load_plugins(
    modules: Iterable[str] | None = None,
    *,
    entry_points: bool = True,
    strict: bool = True,
) -> list[str]:
    """加载插件：`modules`（给定则用它，否则读 `TASKMQ_PLUGINS`）+ entry points。

    返回本次实际加载（或已加载过）的模块名；`strict=False` 时单个插件失败只记 warning。
    """
    global _entry_points_loaded
    targets: list[str] = []
    for item in (list(modules) if modules is not None else env_plugins()):
        if item not in targets:
            targets.append(item)

    with _load_lock:
        if entry_points and not _entry_points_loaded:
            _entry_points_loaded = True
            for value in discover():
                if value not in targets:
                    targets.append(value)

    loaded: list[str] = []
    for item in targets:
        module, _, attr = str(item).partition(":")
        try:
            loaded.append(load_plugin(module, attr.strip() or None))
        except ConfigError:
            if strict:
                raise
            logger.warning("插件加载失败（已忽略）：%s", item)
    return loaded


def unknown_scheme_message(scheme: str) -> str:
    """未知 scheme 的错误信息（必须可操作：列出已注册 + 三条接入路径）。"""
    builtin = ", ".join(sorted(BUILTIN_TRANSPORTS))
    extra = {
        name: origin
        for name, origin in known_transports().items()
        if name not in BUILTIN_TRANSPORTS
    }
    plugin_line = (
        "\n已注册插件 transport："
        + ", ".join(f"{name}（{origin}）" for name, origin in sorted(extra.items()))
        if extra
        else ""
    )
    return (
        f"未知 transport scheme：{scheme}://\n"
        f"已注册：{builtin}（内建）{plugin_line}\n"
        f"想接自己的后端？pip install 你的包（entry point 组 {ENTRY_POINT_GROUP}），"
        f"或 app.load_plugins([\"mycompany.mq\"]) / {PLUGINS_ENV}=... / taskmq --plugins ...\n"
        "细节见 docs/design/plugins.md"
    )


def loaded_plugins() -> list[str]:
    with _load_lock:
        return list(_loaded)


def reset_plugins(*, clear_registrations: bool = True) -> None:
    """重置插件状态（测试隔离用）：清空加载记录，默认连注册表一起清。"""
    global _entry_points_loaded
    with _load_lock:
        _loaded.clear()
        _entry_points_loaded = False
        if clear_registrations:
            for registry in (_transports, _codecs, _sinks, _pools):
                with registry._lock:
                    registry._items.clear()
