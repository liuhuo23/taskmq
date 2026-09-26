"""极简 RESP2 客户端（标准库 socket，零第三方依赖）。

两个实现，接口一致（transport 不区分单机/集群）：

- `RedisClient`：**单节点**连接（一把锁串行化），常见回复解析、`EVAL`（Lua）、乐观事务；
- `RedisClusterClient`：**按 slot 路由**的集群客户端 —— CRC16 + `CLUSTER SLOTS` 拓扑 +
  `MOVED`/`ASK` 重定向，并把「同一条命令的键必须同槽」在客户端就拦下来（否则 Redis 7 会
  静默写到错误的节点上）。

不支持：TLS、pipeline、订阅。够用且可读优先。
"""
from __future__ import annotations

import contextlib
import re
import socket
import threading
import time
from collections.abc import Callable, Sequence
from typing import Any
from urllib.parse import unquote, urlparse

from .errors import ConfigError, TransportError

__all__ = ["RedisClient", "RedisClusterClient", "RedisError", "crc16", "hash_slot"]

SLOT_COUNT = 16384


class RedisError(TransportError):
    """Redis 返回 `-ERR …`（服务端错误回复；连接本身没坏）。"""


# ------------------------------------------------------------------ Cluster 槽位
def _build_crc16_table() -> tuple[int, ...]:
    table = []
    for index in range(256):
        crc = index << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
        table.append(crc)
    return tuple(table)


_CRC16_TABLE = _build_crc16_table()


def crc16(data: bytes) -> int:
    """CRC16-XMODEM —— Redis Cluster 算槽位用的那一套。"""
    crc = 0
    for byte in data:
        crc = ((crc << 8) & 0xFFFF) ^ _CRC16_TABLE[((crc >> 8) ^ byte) & 0xFF]
    return crc


def hash_slot(key: str | bytes) -> int:
    """键的槽位：取第一个**非空** `{...}` 子串算 CRC16（Redis Cluster 的 hash tag 规则）。"""
    text = key.decode("utf-8", "replace") if isinstance(key, bytes) else key
    start = text.find("{")
    if start >= 0:
        end = text.find("}", start + 1)
        if end > start + 1:
            text = text[start + 1 : end]
    return crc16(text.encode("utf-8")) % SLOT_COUNT


#: `MOVED 1234 127.0.0.1:7001` / `ASK 1234 127.0.0.1:7001`
_REDIRECT_RE = re.compile(r"^(MOVED|ASK)\s+(\d+)\s+([^\s:]+):(\d+)")

#: 键在参数 1 的命令
_KEY_AT_1 = frozenset(
    [
        "GET", "SET", "SETNX", "SETEX", "PSETEX", "GETSET", "GETDEL", "APPEND", "STRLEN",
        "GETRANGE", "SETRANGE", "INCR", "DECR", "INCRBY", "DECRBY",
        "HGET", "HSET", "HSETNX", "HGETALL", "HDEL", "HEXISTS", "HINCRBY", "HINCRBYFLOAT",
        "HLEN", "HKEYS", "HVALS", "HMGET",
        "ZADD", "ZREM", "ZCARD", "ZSCORE", "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE",
        "ZREVRANGEBYSCORE", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK", "ZCOUNT", "ZINCRBY",
        "ZPOPMIN", "ZPOPMAX",
        "LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE", "LREM", "LTRIM",
        "SADD", "SREM", "SMEMBERS", "SCARD", "SISMEMBER", "SPOP", "SRANDMEMBER",
        "EXPIRE", "PEXPIRE", "TTL", "PTTL", "PERSIST", "TYPE",
    ]
)

#: 所有参数都是键的命令
_ALL_ARGS_ARE_KEYS = frozenset({"DEL", "UNLINK", "TOUCH", "WATCH", "MGET", "EXISTS"})

#: 不带键的命令（发给任意节点）
_KEYLESS = frozenset(
    [
        "PING", "INFO", "SCAN", "CLUSTER", "COMMAND", "CONFIG", "SCRIPT", "MULTI", "EXEC",
        "DISCARD", "UNWATCH", "ASKING", "SELECT", "AUTH", "HELLO", "FLUSHDB", "FLUSHALL",
        "DBSIZE", "RANDOMKEY", "WAIT", "DEBUG",
    ]
)


def _text(value: Any) -> str:
    return value.decode("utf-8", "replace") if isinstance(value, bytes) else str(value)


def _command_keys(command: str, args: Sequence[Any]) -> list[str]:
    """一条命令涉及哪些键（Cluster 路由 + 同槽校验用）。

    只覆盖 transport 真正用到的命令族；未知命令保守按「参数 1 是键」处理。
    """
    if command in ("EVAL", "EVALSHA"):
        try:
            count = int(args[2])
        except (IndexError, TypeError, ValueError):  # pragma: no cover - 调用方保证格式
            return []
        return [_text(key) for key in args[3 : 3 + max(0, count)]]
    if command in _ALL_ARGS_ARE_KEYS:
        return [_text(key) for key in args[1:]]
    if command in _KEY_AT_1:
        return [_text(args[1])] if len(args) > 1 else []
    if command in _KEYLESS:
        return []
    return [_text(args[1])] if len(args) > 1 else []


class RedisClient:
    """单连接、线程安全的 RESP2 客户端（简单够用；高并发请上连接池）。"""

    cluster_enabled = False

    def __init__(self, url: str, *, timeout: float = 5.0) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in ("redis", "redis+unix"):
            raise ConfigError(f"不支持的 Redis scheme：{parsed.scheme!r}")
        if parsed.scheme == "redis+unix":
            raise ConfigError("redis+unix:// 尚未支持，请用 redis://host:port/db")
        self.url = url
        self.host = parsed.hostname or "127.0.0.1"
        self.port = int(parsed.port or 6379)
        self.db = int((parsed.path or "/0").lstrip("/") or 0)
        self.password = unquote(parsed.password) if parsed.password else None
        self.username = unquote(parsed.username) if parsed.username else None
        self.timeout = timeout
        self._lock = threading.RLock()
        self._sock: socket.socket | None = None

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    # ------------------------------------------------------------------ 连接
    def _connect(self) -> socket.socket:
        try:
            sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        except OSError as exc:
            raise TransportError(f"连不上 Redis {self.host}:{self.port}：{exc}") from exc
        sock.settimeout(self.timeout)
        self._sock = sock
        if self.password:
            auth = ("AUTH", self.username, self.password) if self.username else ("AUTH", self.password)
            self._send(auth)
            self._read_reply()
        if self.db:
            self._send(("SELECT", str(self.db)))
            self._read_reply()
        return sock

    def close(self) -> None:
        with self._lock:
            if self._sock is not None:
                try:
                    self._sock.close()
                finally:
                    self._sock = None

    # ------------------------------------------------------------------ 协议
    @staticmethod
    def _encode(args: tuple[Any, ...]) -> bytes:
        parts = [b"*" + str(len(args)).encode() + b"\r\n"]
        for arg in args:
            raw = arg if isinstance(arg, bytes) else str(arg).encode()
            parts.append(b"$" + str(len(raw)).encode() + b"\r\n" + raw + b"\r\n")
        return b"".join(parts)

    def _send(self, args: tuple[Any, ...]) -> None:
        assert self._sock is not None
        self._sock.sendall(self._encode(args))

    def _read_line(self) -> bytes:
        assert self._sock is not None
        buffer = bytearray()
        while not buffer.endswith(b"\r\n"):
            chunk = self._sock.recv(1)
            if not chunk:
                raise TransportError("Redis 连接被关闭")
            buffer += chunk
        return bytes(buffer[:-2])

    def _read_exact(self, size: int) -> bytes:
        assert self._sock is not None
        data = bytearray()
        while len(data) < size:
            chunk = self._sock.recv(size - len(data))
            if not chunk:
                raise TransportError("Redis 连接提前关闭")
            data += chunk
        return bytes(data)

    def _read_reply(self) -> Any:
        line = self._read_line()
        kind, payload = line[:1], line[1:]
        if kind == b"+":
            return payload.decode("utf-8", "replace")
        if kind == b"-":
            raise RedisError(payload.decode("utf-8", "replace"))
        if kind == b":":
            return int(payload)
        if kind == b"$":
            size = int(payload)
            if size == -1:
                return None
            data = self._read_exact(size)
            self._read_exact(2)                      # 丢弃结尾 CRLF
            return data
        if kind == b"*":
            count = int(payload)
            if count == -1:
                return None
            return [self._read_reply() for _ in range(count)]
        raise TransportError(f"无法解析的 RESP 回复：{line[:40]!r}")

    # ------------------------------------------------------------------ 命令
    def execute(self, *args: Any) -> Any:
        """发一条命令并返回解析后的回复；连接断了自动重连一次。

        服务端错误回复（`-ERR/-WRONGTYPE/-MOVED` …）**原样抛 `RedisError`**：连接是好的，
        重连重试没有意义，而且集群路由要靠上层看 `MOVED`/`ASK`。
        """
        with self._lock:
            for attempt in (1, 2):
                try:
                    if self._sock is None:
                        self._connect()
                    self._send(args)
                    return self._read_reply()
                except RedisError:
                    raise
                except (OSError, TransportError) as exc:
                    self.close()
                    if attempt == 2:
                        raise TransportError(f"Redis 命令失败 {args[:1]}：{exc}") from exc
        raise TransportError("unreachable")  # pragma: no cover

    def eval(self, script: str, keys: tuple[str, ...] = (), args: tuple[Any, ...] = ()) -> Any:
        """`EVAL script numkeys key… arg…`。"""
        return self.execute("EVAL", script, str(len(keys)), *keys, *args)

    def watch_transaction(
        self,
        keys: tuple[str, ...],
        verify: Callable[[], bool],
        commands: Callable[[], list[tuple[Any, ...]]],
    ) -> list[Any] | None:
        """乐观事务（**无 Lua 环境**的回退实现）。

        `WATCH keys` → `verify()`（拿着锁读状态，返回 False 表示放弃这轮）→ `MULTI` +
        `commands()` 生成的写命令 → `EXEC`。

        - `EXEC` 返回 `None`：期间这些键被别的客户端改过（被抢），调用方重试即可；
        - `verify()` 返回 False：调用方按自己的语义处置（不重试）；
        - `commands()` **只能构造命令元组，不能读**（MULTI 里的读只会拿到 QUEUED）。
        """
        with self._lock:
            self.execute("WATCH", *keys)
            try:
                if not verify():
                    self.execute("UNWATCH")
                    return None
                self.execute("MULTI")
                for command in commands():
                    self.execute(*command)
                return self.execute("EXEC")
            except BaseException:
                # MULTI 里报错（如 -MOVED）会留下未结束的事务：先 DISCARD 再 UNWATCH，
                # 否则这条连接后面所有命令都只会拿到 QUEUED。
                with contextlib.suppress(Exception):
                    self.execute("DISCARD")
                with contextlib.suppress(Exception):
                    self.execute("UNWATCH")
                raise

    # ------------------------------------------------------------------ 便捷
    def ping(self) -> bool:
        try:
            return self.execute("PING") == "PONG"
        except TransportError:
            return False

    def delete_prefix(self, prefix: str) -> int:
        """删除 `prefix*` 的键（用 SCAN，不用 KEYS）——测试清理用。"""
        removed = 0
        cursor = "0"
        while True:
            raw_cursor, keys = self.execute("SCAN", cursor, "MATCH", prefix + "*", "COUNT", 200)
            cursor = str(self.decode(raw_cursor))          # RESP 回来是 bytes，别拿 bytes 比 "0"
            if keys:
                removed += int(self.execute("DEL", *keys))
            if cursor == "0":
                return removed

    @staticmethod
    def decode(value: Any) -> Any:
        return value.decode("utf-8", "replace") if isinstance(value, bytes) else value

    @classmethod
    def to_hash(cls, flat: list[Any]) -> dict[Any, Any]:
        """`HGETALL` 的扁平数组 -> dict（键值都保持 Redis 原始类型，调用方自己解码）。"""
        return {flat[index]: flat[index + 1] for index in range(0, len(flat) - 1, 2)}


class RedisClusterClient:
    """按 slot 路由的集群客户端：`CLUSTER SLOTS` 建拓扑 + `MOVED`/`ASK` 跟随。

    - 与 `RedisClient` 同接口（`execute` / `eval` / `watch_transaction` / `ping` / `close`），
      transport 不需要知道自己在集群里；
    - **同槽校验**：一条命令的多个键、一次 Lua 的 KEYS 必须落同一个 slot —— 客户端先算 CRC16
      再发，避免 Redis 7 在脚本里放行跨槽访问而把数据写到错的节点上（见本文件顶部注释）；
    - `SELECT`/多 DB 在 Cluster 下不可用：只允许 db 0（构造时就报错）。
    """

    cluster_enabled = True

    def __init__(self, url: str, *, timeout: float = 5.0, refresh_interval: float = 5.0) -> None:
        parsed = urlparse(url)
        if parsed.scheme != "redis":
            raise ConfigError(f"Cluster 只支持 redis:// scheme：{parsed.scheme!r}")
        self.url = url
        self.timeout = timeout
        self.db = int((parsed.path or "/0").lstrip("/") or 0)
        if self.db != 0:
            raise ConfigError("Redis Cluster 只有 db 0（SELECT 不可用）；请用 redis://host:port/0")
        self._seed = (parsed.hostname or "127.0.0.1", int(parsed.port or 6379))
        self._userinfo = ""
        if parsed.username:
            self._userinfo = unquote(parsed.username)
            if parsed.password:
                self._userinfo += ":" + unquote(parsed.password)
            self._userinfo += "@"
        self._lock = threading.RLock()
        self._conns: dict[tuple[str, int], RedisClient] = {}
        self._slot_nodes: list[tuple[str, int] | None] = [None] * SLOT_COUNT
        self._masters: list[tuple[str, int]] = []
        self._refreshed_at = 0.0
        self._refresh_interval = refresh_interval
        self._refresh(force=True)

    # ------------------------------------------------------------------ 拓扑
    def _url_for(self, host: str, port: int) -> str:
        return f"redis://{self._userinfo}{host}:{port}/0"

    def _conn(self, host: str, port: int) -> RedisClient:
        with self._lock:
            conn = self._conns.get((host, port))
            if conn is None:
                conn = RedisClient(self._url_for(host, port), timeout=self.timeout)
                self._conns[(host, port)] = conn
            return conn

    def _refresh(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and self._masters and now - self._refreshed_at < self._refresh_interval:
            return
        raw = self._conn(*self._seed).execute("CLUSTER", "SLOTS")
        slots: list[tuple[str, int] | None] = [None] * SLOT_COUNT
        masters: list[tuple[str, int]] = []
        for entry in raw or []:
            start, end = int(entry[0]), int(entry[1])
            host, port = _text(entry[2][0]), int(entry[2][1])
            for slot in range(start, end + 1):
                slots[slot] = (host, port)
            if (host, port) not in masters:
                masters.append((host, port))
        if not masters:
            raise TransportError(f"CLUSTER SLOTS 为空（{self._seed[0]}:{self._seed[1]} 不是集群节点？）")
        with self._lock:
            self._slot_nodes = slots
            self._masters = masters
            self._refreshed_at = now

    @property
    def masters(self) -> list[tuple[str, int]]:
        """当前拓扑里的主节点地址（诊断/测试用，会先按需刷新）。"""
        self._refresh()
        return list(self._masters)

    def node_for(self, key: str) -> tuple[str, int]:
        """键最终落到哪个主节点（诊断/测试用）。"""
        self._refresh()
        with self._lock:
            return self._slot_nodes[hash_slot(key)] or self._seed

    # ------------------------------------------------------------------ 路由
    def _route(self, keys: Sequence[str]) -> RedisClient:
        if not keys:
            self._refresh()
            return self._conn(*self._seed)
        slot = hash_slot(keys[0])
        for key in keys[1:]:
            if hash_slot(key) != slot:
                raise TransportError(
                    f"Redis Cluster：一条命令的键必须落同一 slot（{keys[0]!r} / {key!r} 分别是 "
                    f"{slot} / {hash_slot(key)}）；跨队列操作请拆成多条命令"
                )
        self._refresh()
        with self._lock:
            node = self._slot_nodes[slot]
        return self._conn(*(node or self._seed))

    def execute(self, *args: Any) -> Any:
        command = str(args[0]).upper() if args else ""
        keys = _command_keys(command, args)
        return self._call(self._route(keys), args)

    def _call(self, conn: RedisClient, args: Sequence[Any]) -> Any:
        """发命令，跟随 MOVED/ASK（最多跳 5 次）；CLUSTERDOWN/TRYAGAIN/LOADING 有界重试。"""
        for _ in range(5):
            try:
                return conn.execute(*args)
            except RedisError as exc:
                text = str(exc)
                redirect = _REDIRECT_RE.match(text)
                if redirect is None:
                    if text.startswith(("CLUSTERDOWN", "TRYAGAIN", "LOADING")):
                        time.sleep(0.05)
                        continue
                    raise
                kind, slot = redirect.group(1), int(redirect.group(2))
                host, port = redirect.group(3), int(redirect.group(4))
                if kind == "MOVED":
                    with self._lock:
                        self._slot_nodes[slot] = (host, port)
                    conn = self._conn(host, port)
                    continue
                target = self._conn(host, port)                  # ASK：一次性转向，不改拓扑
                with target._lock:                               # ASKING 只对下一条命令生效
                    target.execute("ASKING")
                    return target.execute(*args)
        raise TransportError(f"Redis Cluster 重定向次数过多：{str(args[:1])}")

    def eval(self, script: str, keys: tuple[str, ...] = (), args: tuple[Any, ...] = ()) -> Any:
        conn = self._route(list(keys))
        return self._call(conn, ("EVAL", script, str(len(keys)), *keys, *args))

    def watch_transaction(
        self,
        keys: tuple[str, ...],
        verify: Callable[[], bool],
        commands: Callable[[], list[tuple[Any, ...]]],
    ) -> list[Any] | None:
        """乐观事务：WATCH/MULTI/EXEC 都钉在**同一个节点**（键同槽，天然同节点）。"""
        for _ in range(2):
            conn = self._route(list(keys))
            try:
                return conn.watch_transaction(keys, verify, commands)
            except RedisError as exc:
                if not str(exc).startswith("MOVED"):           # 拓扑过期 → 刷新后重试一次
                    raise
                self._refresh(force=True)
        raise TransportError("Redis Cluster：WATCH 重定向后仍失败")

    # ------------------------------------------------------------------ 便捷
    def ping(self) -> bool:
        try:
            return self._conn(*self._seed).execute("PING") == "PONG"
        except TransportError:
            return False

    def delete_prefix(self, prefix: str) -> int:
        """跨所有主节点 SCAN + DEL（单节点的 SCAN 只扫一个节点，会漏键）。"""
        removed = 0
        for host, port in self.masters:
            conn = self._conn(host, port)
            cursor = "0"
            while True:
                raw_cursor, keys = conn.execute("SCAN", cursor, "MATCH", prefix + "*", "COUNT", 200)
                cursor = str(RedisClient.decode(raw_cursor))
                if keys:
                    # 一次 DEL 的键必须同槽（集群的 CROSSSLOT 校验跟"谁持有"无关）→ 按槽分组
                    groups: dict[int, list[Any]] = {}
                    for key in keys:
                        groups.setdefault(hash_slot(key), []).append(key)
                    for group in groups.values():
                        removed += int(conn.execute("DEL", *group))
                if cursor == "0":
                    break
        return removed

    def close(self) -> None:
        with self._lock:
            conns, self._conns = list(self._conns.values()), {}
        for conn in conns:
            conn.close()
