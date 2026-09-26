"""Redis transport：高吞吐 + 原生原子操作（Phase 1）。

设计取舍（docs/design.md §9.4、priority.md §7）：

- **键前缀**：默认 `taskmq:`，可用 `redis://host:port/db?prefix=app1` 隔离多套环境；
  `enqueue` 等操作**只碰自己的前缀**，不会 FLUSH 别人的库。
- **优先级**：ready ZSET 的 score = `-priority * 2**40 + seq`，`ZPOPMIN`/最小 score 就是
  「最高优先级 + 同级 FIFO」。跨队列取全局最小 score = 方案 D 的全局严格优先；
  平级队列的加权轮询（memory/sqlite 有）在 Redis 上暂不实现（Phase 2，写进文档）。
- **可见性**：延迟/退避/让位/重投统一进 delayed ZSET（score = visible_at），
  reserve 时先 promote 到期的；**过期在 promote/claim 时判定**（不额外全表扫描）。
- **原子性**：claim、状态转换、孤儿回收、命名租约都用 Lua（EVAL）一次往返完成。
- **Cluster（`?cluster=1`）**：键按「逻辑队列」打 hash tag（`taskmq:{q}:ready`），
  单队列的 promote/claim 走**显式 KEYS** 的 Lua，仍是原子的一次往返；跨队列取件在 Cluster 下没有
  单次原子可言（跨 slot），降级为「每队列各取一次 + Python 侧按 band/权重选」
  （docs/design/redis-cluster.md）。
"""
from __future__ import annotations

import json
import logging
import math
import time
from collections.abc import Mapping, Sequence
from typing import Any, NamedTuple

from ..errors import ConfigError, DecodeError, LeaseLost, MessageNotFound, TransportError
from ..protocol import Codec, CodecRegistry, Envelope, decode_value, encode_value
from ..redis_client import RedisClient, RedisClusterClient, RedisError
from .base import (
    UNSET,
    DeadLetter,
    Delivery,
    JobRecord,
    JobState,
    QueueStat,
    Transport,
    WorkerInfo,
)

__all__ = ["RedisTransport"]

logger = logging.getLogger("taskmq.transport.redis")

SEQ_BITS = 40
SEQ_MOD = 1 << SEQ_BITS


class _QueueHead(NamedTuple):
    """Cluster 取件时每个队列的「队头快照」（`_PEEK_QUEUE_LUA` 的返回值）。"""

    queue: str
    message_id: str
    score: float
    priority: int
    served: int
    weight: int
    expired_jobs: tuple[str, ...] = ()


def _score(priority: int, seq: int) -> float:
    return -int(priority) * SEQ_MOD + (int(seq) % SEQ_MOD)


def _priority_of(score: float) -> int:
    return -math.floor(score / SEQ_MOD)


_RESERVE_LUA = """
local prefix = ARGV[1]
local now = tonumber(ARGV[2])
local lease = tonumber(ARGV[3])
local worker = ARGV[4]
local limit = tonumber(ARGV[5])
local nq = tonumber(ARGV[6])

local function expire_msg(id, mkey)
  redis.call("HSET", mkey, "state", "expired", "dead_reason", "expired")
  local jid = redis.call("HGET", mkey, "job_id")
  if jid then redis.call("HSET", prefix .. "job:" .. jid, "state", "EXPIRED",
                         "error", "expired before execution", "updated_at", now) end
end

for i = 1, nq do
  local queue = ARGV[6 + i]
  local dkey = prefix .. "delayed:" .. queue
  local due = redis.call("ZRANGEBYSCORE", dkey, "-inf", now, "LIMIT", 0, 500)
  for _, id in ipairs(due) do
    redis.call("ZREM", dkey, id)
    local mkey = prefix .. "msg:" .. id
    local expires = redis.call("HGET", mkey, "expires_at")
    if expires and expires ~= "" and tonumber(expires) <= now then
      expire_msg(id, mkey)
    else
      local score = redis.call("HGET", mkey, "score")
      if score then redis.call("ZADD", prefix .. "ready:" .. queue, score, id) end
    end
  end
end

local SEQ_MOD = 1099511627776              -- 2^40：score = -priority * 2^40 + seq
local out = {}
while #out < limit do
  -- 取各队列队头，先定「最高优先级档位」
  local heads = {}
  local band = false
  for i = 1, nq do
    local queue = ARGV[6 + i]
    local top = redis.call("ZRANGE", prefix .. "ready:" .. queue, 0, 0, "WITHSCORES")
    if top[1] then
      local sc = tonumber(top[2])
      local prio = -math.floor(sc / SEQ_MOD)
      heads[#heads + 1] = {queue = queue, score = sc, id = top[1], prio = prio}
      if band == false or prio > band then band = prio end
    end
  end
  if band == false then break end
  -- 档内按 served/weight 最小选（平手按队列名，保证确定性）
  local best = false
  local best_ratio = false
  for _, head in ipairs(heads) do
    if head.prio == band then
      local weight = tonumber(redis.call("HGET", prefix .. "weights", head.queue) or "1")
      if weight < 1 then weight = 1 end
      local served = tonumber(redis.call("GET", prefix .. "served:" .. head.queue) or "0")
      local ratio = served / weight
      if best == false or ratio < best_ratio or (ratio == best_ratio and head.queue < best.queue) then
        best, best_ratio = head, ratio
      end
    end
  end
  redis.call("INCR", prefix .. "served:" .. best.queue)
  local best_q, best_score, best_id = best.queue, best.score, best.id
  if best_id == false then break end
  redis.call("ZREM", prefix .. "ready:" .. best_q, best_id)
  local mkey = prefix .. "msg:" .. best_id
  local expires = redis.call("HGET", mkey, "expires_at")
  if expires and expires ~= "" and tonumber(expires) <= now then
    expire_msg(best_id, mkey)
  else
    redis.call("HSET", mkey, "state", "reserved", "claimed_by", worker,
               "claimed_at", now, "lease_until", now + lease)
    redis.call("HINCRBY", mkey, "deliveries", 1)
    redis.call("ZADD", prefix .. "leases:" .. best_q, now + lease, best_id)
    local prio = redis.call("HGET", mkey, "priority")
    if prio then redis.call("HINCRBY", prefix .. "prio:" .. best_q, prio, -1) end
    local jid = redis.call("HGET", mkey, "job_id")
    if jid then redis.call("HSET", prefix .. "job:" .. jid, "state", "RUNNING",
                           "updated_at", now) end
    table.insert(out, best_id)
  end
end
return out
"""

_MUTATE_LUA = """
-- 显式 KEYS：单机与 Cluster 共用（Cluster 下这 4 个键都带同一个 hash tag，天然同槽）
local mkey, leases, delayed = KEYS[1], KEYS[2], KEYS[3]
local prio_key, dlq = KEYS[4], KEYS[5]
local worker, action, id = ARGV[1], ARGV[2], ARGV[3]
local now, extra = tonumber(ARGV[4]), tonumber(ARGV[5])
local reason, max_yields = ARGV[6], tonumber(ARGV[7])
local state = redis.call("HGET", mkey, "state")
if not state then return "missing" end
if state == "acked" and action == "ack" then return "ok" end
if state == "dead" and action == "dead" then return "ok" end
if state ~= "reserved" or redis.call("HGET", mkey, "claimed_by") ~= worker then return "lost" end
local prio = redis.call("HGET", mkey, "priority")

local function requeue()
  redis.call("HSET", mkey, "state", "queued", "visible_at", now + extra,
             "claimed_by", "", "claimed_at", "", "lease_until", "")
  redis.call("ZREM", leases, id)
  redis.call("ZADD", delayed, now + extra, id)
  if prio then redis.call("HINCRBY", prio_key, prio, 1) end
end

if action == "ack" then
  redis.call("HSET", mkey, "state", "acked", "claimed_by", "", "claimed_at", "", "lease_until", "")
  redis.call("ZREM", leases, id)
elseif action == "dead" then
  redis.call("HSET", mkey, "state", "dead", "dead_reason", reason, "last_error", reason,
             "claimed_by", "", "claimed_at", "", "lease_until", "")
  redis.call("ZREM", leases, id)
  redis.call("RPUSH", dlq, id)
elseif action == "requeue" then
  requeue()
elseif action == "defer" then
  local deliveries = tonumber(redis.call("HGET", mkey, "deliveries") or "0")
  redis.call("HSET", mkey, "deliveries", math.max(0, deliveries - 1))
  requeue()
elseif action == "extend" then
  redis.call("HSET", mkey, "lease_until", now + extra)
  redis.call("ZADD", leases, now + extra, id)
elseif action == "yield" then
  if tonumber(redis.call("HGET", mkey, "yieldable") or "1") == 0 then return "not_yieldable" end
  local yields = tonumber(redis.call("HGET", mkey, "yields") or "0") + 1
  redis.call("HSET", mkey, "yields", yields, "visible_at", now + extra)
  redis.call("HSET", mkey, "yieldable", (yields >= max_yields) and 0 or 1)
  requeue()
  redis.call("HSET", mkey, "yields", yields, "yieldable", (yields >= max_yields) and 0 or 1)
else
  return "bad_action"
end
return "ok"
"""

_REAP_LUA = """
local prefix = ARGV[1]
local now = tonumber(ARGV[2])
local nq = tonumber(ARGV[3])
local total = 0
for i = 1, nq do
  local queue = ARGV[3 + i]
  local lkey = prefix .. "leases:" .. queue
  local expired = redis.call("ZRANGEBYSCORE", lkey, "-inf", now, "LIMIT", 0, 500)
  for _, id in ipairs(expired) do
    redis.call("ZREM", lkey, id)
    local mkey = prefix .. "msg:" .. id
    if redis.call("HGET", mkey, "state") == "reserved" then
      redis.call("HSET", mkey, "state", "queued", "visible_at", now,
                 "claimed_by", "", "claimed_at", "", "lease_until", "")
      redis.call("ZADD", prefix .. "delayed:" .. queue, now, id)
      local prio = redis.call("HGET", mkey, "priority")
      if prio then redis.call("HINCRBY", prefix .. "prio:" .. queue, prio, 1) end
      total = total + 1
    end
  end
end
return total
"""

# ------------------------------------------------------------------ Cluster 脚本
# 下面三个脚本只在 `?cluster=1` 下用：KEY 全部显式传入且带同一个 `{queue}` hash tag，
# 脚本里按 `msg_prefix` 拼出来的 msg 键也在同一槽（键命名保证，见 `_msg_prefix`）。
_PEEK_QUEUE_LUA = """
-- 单队列：promote 到期消息 + 返回队头（不取走）+ 该队列已服务数/权重。
local delayed, ready, served_key, weight_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local now = tonumber(ARGV[1])
local msg_prefix = ARGV[2]

local expired_jobs = {}
local due = redis.call("ZRANGEBYSCORE", delayed, "-inf", now, "LIMIT", 0, 500)
for _, id in ipairs(due) do
  redis.call("ZREM", delayed, id)
  local mkey = msg_prefix .. id
  local expires = redis.call("HGET", mkey, "expires_at")
  if expires and expires ~= "" and tonumber(expires) <= now then
    redis.call("HSET", mkey, "state", "expired", "dead_reason", "expired")
    expired_jobs[#expired_jobs + 1] = redis.call("HGET", mkey, "job_id") or ""
  else
    local score = redis.call("HGET", mkey, "score")
    if score then redis.call("ZADD", ready, score, id) end
  end
end

local top = redis.call("ZRANGE", ready, 0, 0, "WITHSCORES")
local out = {"", "", "0", redis.call("GET", served_key) or "0", redis.call("GET", weight_key) or "1"}
if top[1] then
  local score = tonumber(top[2])
  out[1] = top[1]
  out[2] = top[2]
  out[3] = tostring(-math.floor(score / 1099511627776))
end
for _, jid in ipairs(expired_jobs) do out[#out + 1] = jid end
return out
"""

_CLAIM_QUEUE_LUA = """
-- 单队列原子取件：队头仍是 expected 才取，否则 lost（调用方换候选/重试）。
local ready, leases, prio_key, served_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local id, worker = ARGV[1], ARGV[2]
local now, lease = tonumber(ARGV[3]), tonumber(ARGV[4])
local msg_prefix, expected_score = ARGV[5], ARGV[6]
local top = redis.call("ZRANGE", ready, 0, 0)
if not top[1] or top[1] ~= id then return {"lost"} end
local mkey = msg_prefix .. id
local expires = redis.call("HGET", mkey, "expires_at")
if expires and expires ~= "" and tonumber(expires) <= now then
  redis.call("ZREM", ready, id)
  redis.call("HSET", mkey, "state", "expired", "dead_reason", "expired")
  return {"expired", redis.call("HGET", mkey, "job_id") or ""}
end
local score = redis.call("HGET", mkey, "score")
if (not score) or tonumber(score) ~= tonumber(expected_score) then return {"lost"} end
redis.call("ZREM", ready, id)
redis.call("HSET", mkey, "state", "reserved", "claimed_by", worker,
           "claimed_at", now, "lease_until", now + lease)
redis.call("HINCRBY", mkey, "deliveries", 1)
redis.call("ZADD", leases, now + lease, id)
local prio = redis.call("HGET", mkey, "priority")
if prio then redis.call("HINCRBY", prio_key, prio, -1) end
redis.call("INCR", served_key)
return {"ok", id}
"""

_REAP_QUEUE_LUA = """
-- 单队列租约回收（跨队列要分多次调用：不同队列不同槽）。
local leases, delayed, prio_key = KEYS[1], KEYS[2], KEYS[3]
local now = tonumber(ARGV[1])
local msg_prefix = ARGV[2]
local total = 0
local expired = redis.call("ZRANGEBYSCORE", leases, "-inf", now, "LIMIT", 0, 500)
for _, id in ipairs(expired) do
  redis.call("ZREM", leases, id)
  local mkey = msg_prefix .. id
  if redis.call("HGET", mkey, "state") == "reserved" then
    redis.call("HSET", mkey, "state", "queued", "visible_at", now,
               "claimed_by", "", "claimed_at", "", "lease_until", "")
    redis.call("ZADD", delayed, now, id)
    local prio = redis.call("HGET", mkey, "priority")
    if prio then redis.call("HINCRBY", prio_key, prio, 1) end
    total = total + 1
  end
end
return total
"""

_ACQUIRE_LEASE_LUA = """
local key, owner, ttl_ms = KEYS[1], ARGV[1], ARGV[2]
local current = redis.call("GET", key)
if (not current) or current == owner then
  redis.call("SET", key, owner, "PX", ttl_ms)
  return 1
end
return 0
"""

_RELEASE_LEASE_LUA = """
local key, owner = KEYS[1], ARGV[1]
if redis.call("GET", key) == owner then
  redis.call("DEL", key)
  return 1
end
return 0
"""


class RedisTransport(Transport):
    """Redis 版 transport。`prefix` 决定键空间，测试可给随机前缀互不干扰。"""

    supports_leases = True
    supports_workers = True
    supports_job_listing = True
    limitations = {
        "reap_expired_jobs": "过期在 reserve 的 promote/claim 阶段判定，reap_expired_jobs() 为 no-op",
    }

    #: Cluster 下额外的降级说明（信息型；`global_priority` 会被一致性套件按声明跳过）
    cluster_limitations = {
        "global_priority": (
            "Cluster 下跨队列取件无法在一次原子调用里完成（跨 slot）：降级为「每队列各取一次 + "
            "Python 侧按 band/权重选」，peek 窗口内可能出现瞬时优先级倒挂"
        ),
        "cross_queue_atomicity": "reserve([q1, q2, …]) 不再是一条原子命令，而是 1 + N 次往返",
    }

    def __init__(
        self,
        url: str,
        *,
        codec: Codec,
        registry: CodecRegistry | None = None,
        prefix: str = "taskmq:",
        clock: Any | None = None,
        timeout: float = 5.0,
        idempotency_ttl: float = 86400.0,
        max_message_bytes: int | None = None,
        lua: bool | None = None,
        cluster: bool = False,
    ) -> None:
        self.url = url
        self.cluster = bool(cluster)
        self.prefix = prefix.rstrip(":") + ":"
        if self.cluster and ("{" in self.prefix or "}" in self.prefix):
            raise ConfigError(f"Cluster 模式下 prefix 不能含花括号（会抢走 hash tag）：{prefix!r}")
        limits: dict[str, str] = dict(type(self).limitations)
        if self.cluster:
            limits.update(type(self).cluster_limitations)
        self.limitations = limits
        self._client: RedisClient | RedisClusterClient = (
            RedisClusterClient(url, timeout=timeout)
            if self.cluster
            else RedisClient(url, timeout=timeout)
        )
        self._codec = codec
        self._registry = registry if registry is not None else CodecRegistry()
        self._clock = clock or time.time
        self._idempotency_ttl = idempotency_ttl
        self._max_message_bytes = max_message_bytes
        self._weights: dict[str, int] = {}          # 档内加权轮询权重（P3）
        if not self._client.ping():
            raise TransportError(f"Redis 不可用：{url}")
        self.lua_enabled = self._resolve_lua(lua)

    # ------------------------------------------------------------ Lua 能力探测
    #: Redis 禁用/限制 EVAL 时的典型报错关键字
    _LUA_FORBIDDEN_HINTS = (
        "unknown command",
        "disabled",
        "noperm",
        "not allowed",
        "no permissions",
        "unknown subcommand",
    )

    @classmethod
    def _lua_forbidden(cls, exc: BaseException) -> bool:
        text = str(exc).lower()
        return any(hint in text for hint in cls._LUA_FORBIDDEN_HINTS)

    def _resolve_lua(self, requested: bool | None) -> bool:
        """`None`=探测（默认）；`True`=必须有 Lua（没有就报错）；`False`=一律不用 Lua。

        有些托管 Redis 会禁用 EVAL（或 `rename-command EVAL ""`）——这种情况下自动回退到
        `WATCH/MULTI/EXEC` 乐观事务，语义不变，只是多几个往返。
        """
        if requested is False:
            logger.info("Redis transport：按配置禁用 Lua，使用 WATCH/MULTI/EXEC 回退")
            return False
        try:
            self._client.execute("EVAL", "return 1", "0")
        except RedisError as exc:
            if requested is True or not self._lua_forbidden(exc):
                raise TransportError(f"Redis 无法执行 Lua（EVAL）：{exc}") from exc
            logger.warning(
                "Redis 禁用了 Lua（EVAL）：%s —— 回退到 WATCH/MULTI/EXEC（多几个往返，语义相同）", exc
            )
            return False
        return True

    # ------------------------------------------------------------------ 工具
    def _now(self) -> float:
        return float(self._clock())

    def _key(self, *parts: str) -> str:
        return self.prefix + ":".join(parts)

    @staticmethod
    def _check_tag(queue: str) -> str:
        if "{" in queue or "}" in queue:
            raise ConfigError(f"Cluster 模式下队列名不能含花括号（hash tag）：{queue!r}")
        return queue

    def _qkey(self, name: str, queue: str) -> str:
        """队列私有结构的键：`{q}:ready` / `{q}:leases` …（Cluster 下带 hash tag 保证同槽）。"""
        if not self.cluster:
            return self.prefix + name + ":" + queue
        return f"{self.prefix}{{{self._check_tag(queue)}}}:{name}"

    def _msg_prefix(self, queue: str) -> str:
        """消息键前缀。Cluster 下必须与队列同槽（Lua 里按 id 拼出来的键也在同一脚本内访问）。"""
        if not self.cluster:
            return self.prefix + "msg:"
        return f"{self.prefix}{{{self._check_tag(queue)}}}:msg:"

    def _mkey(self, queue: str, message_id: Any) -> str:
        return self._msg_prefix(queue) + str(message_id)

    @staticmethod
    def _s(value: Any, default: str = "") -> str:
        if value is None:
            return default
        return value.decode("utf-8", "replace") if isinstance(value, bytes) else str(value)

    def _hash(self, key: str) -> dict[bytes, bytes]:
        flat = self._client.execute("HGETALL", key)
        return RedisClient.to_hash(flat)

    def _queues(self, queues: Sequence[str] | None) -> list[str]:
        if queues is not None:
            return list(dict.fromkeys(queues))
        members = self._client.execute("SMEMBERS", self._key("queues")) or []
        return sorted(self._s(item) for item in members)

    def _encode(self, env: Envelope) -> bytes:
        return self._codec.encode(env, max_bytes=self._max_message_bytes)

    def _decode(self, blob: bytes) -> Envelope:
        return self._codec.decode(blob)

    def _to_delivery(self, msg: Mapping[bytes, bytes], worker_id: str) -> Delivery:
        envelope = self._decode(msg[b"envelope"])
        return Delivery(
            job_id=self._s(msg.get(b"job_id")),
            message_id=int(self._s(msg.get(b"seq"), "0")),
            queue=self._s(msg.get(b"queue")),
            envelope=envelope,
            worker_id=worker_id,
            attempt=envelope.attempt,
            deliveries=int(self._s(msg.get(b"deliveries"), "0")),
            lease_until=float(self._s(msg.get(b"lease_until"), "0") or 0),
            reserved_at=float(self._s(msg.get(b"claimed_at"), "0") or 0),
            priority=int(self._s(msg.get(b"priority"), "0")),
            yields=int(self._s(msg.get(b"yields"), "0")),
        )

    def set_queue_weights(self, weights: Mapping[str, int]) -> None:
        """档内加权轮询的权重（P3），语义与 memory/sqlite 一致。"""
        self._weights = {name: max(1, int(weight)) for name, weight in weights.items()}
        if not self._weights:
            return
        if self.cluster:            # 权重跟队列同槽（Lua 的 KEYS 里要读它）
            for name, weight in self._weights.items():
                self._client.execute("SET", self._qkey("weight", name), str(weight))
            return
        flat: list[Any] = []
        for name, weight in self._weights.items():
            flat.extend([name, weight])
        self._client.execute("HSET", self._key("weights"), *flat)

    # ------------------------------------------------------------------ 入队
    def enqueue(
        self,
        env: Envelope,
        *,
        queue: str | None = None,
        delay: float = 0.0,
        priority: int | None = None,
    ) -> str:
        now = self._now()
        target = queue or env.queue or "default"
        prio = env.priority if priority is None else int(priority)
        effective = env if (env.queue == target and env.priority == prio) else env.with_routing(target, prio)
        blob = self._encode(effective)

        if env.key:
            key = self._key("key", env.key)
            claimed = self._client.execute(
                "SET", key, env.id, "NX", "EX", str(int(self._idempotency_ttl))
            )
            if claimed is None:                       # 已有同 key：返回已存在 job
                existing = self._client.execute("GET", key)
                if existing is not None:
                    return self._s(existing)

        visible_at = now + max(0.0, float(delay))
        if env.eta is not None:
            visible_at = max(visible_at, float(env.eta))

        seq = int(self._client.execute("INCR", self._key("seq")))
        msg_key = self._mkey(target, seq)
        if self.cluster:
            # 消息 -> 队列 反查索引：Cluster 下 msg 键带队列 hash tag，没有它就无法按 id 找键
            self._client.execute("HSET", self._key("msgq"), str(seq), target)
        self._client.execute(
            "HSET",
            msg_key,
            "seq", str(seq),
            "job_id", env.id,
            "queue", target,
            "task", env.task,
            "envelope", blob,
            "priority", str(prio),
            "score", repr(_score(prio, seq)),
            "state", "queued",
            "visible_at", repr(visible_at),
            "expires_at", "" if env.expires_at is None else repr(float(env.expires_at)),
            "claimed_by", "",
            "claimed_at", "",
            "lease_until", "",
            "deliveries", "0",
            "yields", "0",
            "yieldable", "1",
            "dead_reason", "",
            "last_error", "",
        )
        self._client.execute("SADD", self._key("queues"), target)
        self._client.execute("HINCRBY", self._qkey("prio", target), str(prio), 1)
        score = _score(prio, seq)
        if visible_at > now:
            self._client.execute("ZADD", self._qkey("delayed", target), repr(visible_at), str(seq))
        else:
            self._client.execute("ZADD", self._qkey("ready", target), repr(score), str(seq))

        job_key = self._key("job", env.id)
        self._client.execute(
            "HSET",
            job_key,
            "task", env.task,
            "state", JobState.QUEUED,
            "attempt", str(env.attempt),
            "updated_at", repr(now),
            "error", "",
        )
        self._client.execute("HSETNX", job_key, "created_at", repr(now))
        self._client.execute(
            "HSETNX", job_key, "meta", json.dumps({"queue": target, "priority": prio})
        )
        self._client.execute("HSETNX", job_key, "has_result", "0")
        # 入队也要进 job 索引（list_jobs 靠它；否则新建的 job 查不到）
        self._client.execute("ZADD", self._key("jobs"), repr(now), env.id)
        return env.id

    # ------------------------------------------------------------------ 投递
    def reserve(
        self,
        queues: Sequence[str],
        *,
        worker_id: str,
        lease: float,
        limit: int,
    ) -> list[Delivery]:
        wanted = list(dict.fromkeys(queues))
        if limit <= 0 or not wanted:
            return []
        now = self._now()
        if not self.lua_enabled:
            return self._reserve_fallback(
                wanted, worker_id=worker_id, lease=lease, limit=limit, now=now
            )
        if self.cluster:
            return self._reserve_cluster(
                wanted, worker_id=worker_id, lease=lease, limit=limit, now=now
            )
        ids = (
            self._client.eval(
                _RESERVE_LUA,
                (),
                (
                    self.prefix,
                    repr(now),
                    repr(lease),
                    worker_id,
                    str(limit),
                    str(len(wanted)),
                    *wanted,
                ),
            )
            or []
        )
        deliveries: list[Delivery] = []
        for raw_id in ids:
            msg = self._hash(self._key("msg", self._s(raw_id)))
            if msg:
                deliveries.append(self._to_delivery(msg, worker_id))
        return deliveries

    # ------------------------------------------------- Cluster 跨队列取件（C2 降级）
    def _reserve_cluster(
        self, wanted: list[str], *, worker_id: str, lease: float, limit: int, now: float
    ) -> list[Delivery]:
        """Cluster 下跨队列取件：每队列各取一次 + Python 侧按 band/权重选。

        选择规则与单机 Lua **完全一致**（先定最高优先级档位；档内 served/weight 最小者优先，
        平手按队列名）；单队列的 promote + claim 仍是一次原子 Lua。代价：一次 reserve 是
        1 + N 次往返，且跨队列不再是单次原子（见 `cluster_limitations`）。
        """
        claimed: list[Delivery] = []
        stalls = 0
        while len(claimed) < limit and stalls < 3:
            heads: list[_QueueHead] = []
            for queue in wanted:
                head = self._peek_queue(queue, now)
                if head is None:
                    continue
                for job_id in head.expired_jobs:
                    self._expire_job(job_id, now)
                if head.message_id:
                    heads.append(head)
            if not heads:
                break
            band = max(head.priority for head in heads)
            candidates = sorted(
                (head for head in heads if head.priority == band),
                key=lambda head: (head.served / head.weight, head.queue),
            )
            for head in candidates:
                outcome, payload = self._claim_queue(
                    head, worker_id=worker_id, lease=lease, now=now
                )
                if outcome == "lost":                     # 被别的 worker 抢走 → 试下一个候选
                    continue
                if outcome == "expired":
                    self._expire_job(payload, now)
                else:
                    message = self._hash(self._mkey(head.queue, head.message_id))
                    if message:
                        delivery = self._to_delivery(message, worker_id)
                        self._mark_job_running(delivery.job_id, now)     # job 键跨槽，Lua 里碰不到
                        claimed.append(delivery)
                stalls = 0
                break
            else:                                          # 候选全被抢：重新 peek 一轮
                stalls += 1
        return claimed

    def _peek_queue(self, queue: str, now: float) -> _QueueHead | None:
        raw = self._client.eval(
            _PEEK_QUEUE_LUA,
            (
                self._qkey("delayed", queue),
                self._qkey("ready", queue),
                self._qkey("served", queue),
                self._qkey("weight", queue),
            ),
            (repr(now), self._msg_prefix(queue)),
        )
        if not raw:
            return None
        values = [self._s(item) for item in raw]
        return _QueueHead(
            queue=queue,
            message_id=values[0],
            score=float(values[1] or 0.0),
            priority=int(values[2] or 0),
            served=int(values[3] or 0),
            weight=max(1, int(values[4] or 1)),
            expired_jobs=tuple(job for job in values[5:] if job),
        )

    def _claim_queue(
        self, head: _QueueHead, *, worker_id: str, lease: float, now: float
    ) -> tuple[str, str]:
        raw = self._client.eval(
            _CLAIM_QUEUE_LUA,
            (
                self._qkey("ready", head.queue),
                self._qkey("leases", head.queue),
                self._qkey("prio", head.queue),
                self._qkey("served", head.queue),
            ),
            (
                head.message_id,
                worker_id,
                repr(now),
                repr(lease),
                self._msg_prefix(head.queue),
                repr(head.score),
            ),
        )
        values = [self._s(item) for item in (raw or [])]
        if not values:
            return "lost", ""
        return values[0], (values[1] if len(values) > 1 else "")

    def _mark_job_running(self, job_id: str, now: float) -> None:
        """与单机 Lua 里的 `job -> RUNNING` 对齐（Cluster 下 job 键与队列不同槽）。"""
        if not job_id:
            return
        self._client.execute(
            "HSET", self._key("job", job_id), "state", JobState.RUNNING, "updated_at", repr(now)
        )

    def _expire_job(self, job_id: str, now: float) -> None:
        """Cluster 下 job 键是全局的（跟队列不同槽），Lua 里碰不到 → 由 Python 补一刀。"""
        if not job_id:
            return
        self._client.execute(
            "HSET",
            self._key("job", job_id),
            "state",
            JobState.EXPIRED,
            "error",
            "expired before execution",
            "updated_at",
            repr(now),
        )

    def _mutate(
        self,
        delivery: Delivery,
        action: str,
        *,
        extra: float = 0.0,
        reason: str = "",
        max_yields: int = 100,
    ) -> str:
        if not self.lua_enabled:
            return self._mutate_fallback(
                delivery, action, extra=extra, reason=reason, max_yields=max_yields
            )
        queue = delivery.queue
        result = self._client.eval(
            _MUTATE_LUA,
            (
                self._mkey(queue, delivery.message_id),
                self._qkey("leases", queue),
                self._qkey("delayed", queue),
                self._qkey("prio", queue),
                self._qkey("dlq", queue),
            ),
            (
                delivery.worker_id,
                action,
                str(delivery.message_id),
                repr(self._now()),
                repr(float(extra)),
                reason,
                str(int(max_yields)),
            ),
        )
        return self._s(result)

    def ack(self, delivery: Delivery) -> None:
        status = self._mutate(delivery, "ack")
        if status == "missing":
            raise MessageNotFound(f"消息 {delivery.message_id} 不存在")
        if status == "lost":
            raise LeaseLost(f"消息 {delivery.message_id} 已被回收，ack 被拒绝")

    def nack(self, delivery: Delivery, *, requeue: bool = True, delay: float = 0.0) -> None:
        if requeue:
            status = self._mutate(delivery, "requeue", extra=delay)
            if status == "missing":
                raise MessageNotFound(f"消息 {delivery.message_id} 不存在")
            if status == "lost":
                raise LeaseLost(f"消息 {delivery.message_id} 已被回收")
            return
        status = self._mutate(delivery, "dead", reason="nacked without requeue")
        if status == "lost":
            raise LeaseLost(f"消息 {delivery.message_id} 已被回收")

    def dead_letter(self, delivery: Delivery, reason: str) -> None:
        status = self._mutate(delivery, "dead", reason=reason)
        if status == "missing":
            raise MessageNotFound(f"消息 {delivery.message_id} 不存在")
        if status == "lost":
            raise LeaseLost(f"消息 {delivery.message_id} 已被回收")

    def extend_lease(self, delivery: Delivery, seconds: float) -> None:
        status = self._mutate(delivery, "extend", extra=seconds)
        if status == "lost":
            raise LeaseLost(f"消息 {delivery.message_id} 已被回收")

    def defer(self, delivery: Delivery, *, delay: float = 0.0) -> None:
        status = self._mutate(delivery, "defer", extra=delay)
        if status == "lost":
            raise LeaseLost(f"消息 {delivery.message_id} 已被回收")

    def yield_reservation(
        self, delivery: Delivery, *, delay: float = 0.0, max_yields: int = 100
    ) -> bool:
        status = self._mutate(delivery, "yield", extra=delay, max_yields=max_yields)
        if status == "lost":
            raise LeaseLost(f"消息 {delivery.message_id} 已被回收")
        return status == "ok"

    # ------------------------------------------------------------------ 插队
    def peek_max_priority(self, queues: Sequence[str], *, now: float | None = None) -> int | None:
        moment = self._now() if now is None else float(now)
        best: float | None = None
        for queue in dict.fromkeys(queues):
            ready = self._client.execute("ZRANGE", self._qkey("ready", queue), 0, 0, "WITHSCORES")
            if ready:
                score = float(ready[1])
                best = score if best is None else min(best, score)
            delayed = self._client.execute(
                "ZRANGEBYSCORE", self._qkey("delayed", queue), "-inf", repr(moment), "LIMIT", 0, 1
            )
            if delayed:
                msg = self._hash(self._mkey(queue, self._s(delayed[0])))
                if msg:
                    score = float(self._s(msg.get(b"score"), "0"))
                    best = score if best is None else min(best, score)
        return None if best is None else _priority_of(best)

    def next_visible_at(self, queues: Sequence[str], *, now: float | None = None) -> float | None:
        earliest: float | None = None
        for queue in dict.fromkeys(queues):
            if self._client.execute("ZCARD", self._qkey("ready", queue)):
                return self._now()                     # 已有可见消息
            top = self._client.execute("ZRANGE", self._qkey("delayed", queue), 0, 0, "WITHSCORES")
            if top:
                score = float(top[1])
                earliest = score if earliest is None else min(earliest, score)
        return earliest

    # -------------------------------------------------------------- job 状态
    def set_state(
        self,
        job_id: str,
        state: str,
        *,
        task: str | None = None,
        attempt: int | None = None,
        result: Any = UNSET,
        error: Any = UNSET,
        **meta: Any,
    ) -> JobRecord:
        now = self._now()
        key = self._key("job", job_id)
        current = self._hash(key)
        merged_meta = self._load_json(current.get(b"meta")) if current else {}
        if not isinstance(merged_meta, dict):
            merged_meta = {}
        merged_meta.update(meta)

        fields: dict[str, Any] = {
            "task": task or self._s(current.get(b"task")),
            "state": state,
            "attempt": str(attempt if attempt is not None else int(self._s(current.get(b"attempt"), "0"))),
            "updated_at": repr(now),
            "meta": json.dumps(
                encode_value(merged_meta, self._registry, path="jobs.meta"), separators=(",", ":")
            ),
        }
        if result is not UNSET:
            fields["result"] = json.dumps(
                encode_value(result, self._registry, path="jobs.result"), separators=(",", ":")
            )
            fields["has_result"] = "1"
        if error is not UNSET:
            fields["error"] = "" if error is None else str(error)
        if not current:
            fields["created_at"] = repr(now)
            fields.setdefault("has_result", "0")
        flat: list[Any] = []
        for name, value in fields.items():
            flat.extend([name, value])
        self._client.execute("HSET", key, *flat)
        # job 索引（updated_at 倒序）：list_jobs 用它，cluster 下也必须存在（SCAN 不可用）
        self._client.execute("ZADD", self._key("jobs"), repr(now), job_id)

        record = self.get_state(job_id)
        assert record is not None
        return record

    def _load_json(self, raw: bytes | None) -> Any:
        if raw is None or raw == b"":
            return {}
        try:
            return decode_value(json.loads(raw), self._registry, path="jobs.meta")
        except ValueError as exc:  # pragma: no cover - 数据损坏
            raise DecodeError(f"job 字段不是合法 JSON：{exc}") from exc

    def get_state(self, job_id: str) -> JobRecord | None:
        key = self._key("job", job_id)
        if not self._client.execute("EXISTS", key):
            return None
        row = self._hash(key)
        meta = self._load_json(row.get(b"meta"))
        if not isinstance(meta, dict):
            meta = {}
        has_result = self._s(row.get(b"has_result"), "0") == "1"
        result = None
        if has_result and row.get(b"result"):
            result = decode_value(json.loads(row[b"result"]), self._registry, path="jobs.result")
        return JobRecord(
            job_id=job_id,
            task=self._s(row.get(b"task")),
            state=self._s(row.get(b"state")),
            attempt=int(self._s(row.get(b"attempt"), "0")),
            result=result,
            has_result=has_result,
            error=self._s(row.get(b"error")) or None,
            meta=meta,
            created_at=float(self._s(row.get(b"created_at"), "0") or 0),
            updated_at=float(self._s(row.get(b"updated_at"), "0") or 0),
        )

    def queue_stats(self, queues: Sequence[str] | None = None) -> list[QueueStat]:
        stats = []
        for queue in self._queues(queues):
            pending = int(self._client.execute("ZCARD", self._qkey("ready", queue))) + int(
                self._client.execute("ZCARD", self._qkey("delayed", queue))
            )
            inflight = int(self._client.execute("ZCARD", self._qkey("leases", queue)))  # 按队列的租约 ZSET
            dead = int(self._client.execute("LLEN", self._qkey("dlq", queue)))
            stats.append(QueueStat(queue=queue, pending=pending, inflight=inflight, dead=dead))
        return stats

    def priority_stats(self, queues: Sequence[str] | None = None) -> dict[int, int]:
        buckets: dict[int, int] = {}
        for queue in self._queues(queues):
            flat = self._client.execute("HGETALL", self._qkey("prio", queue))
            for priority, count in RedisClient.to_hash(flat).items():
                value = int(self._s(count, "0"))
                if value > 0:
                    buckets[int(priority)] = buckets.get(int(priority), 0) + value
        return dict(sorted(buckets.items()))

    # ------------------------------------------------------------------ 回收
    def reap_expired_leases(self, now: float | None = None) -> int:
        moment = self._now() if now is None else float(now)
        if not self.lua_enabled:
            return self._reap_fallback(moment)
        queues = self._queues(None)
        if not queues:
            return 0
        if self.cluster:                     # 每个队列一次（不同队列不同槽，无法一条 Lua 扫完）
            total = 0
            for queue in queues:
                total += int(
                    self._client.eval(
                        _REAP_QUEUE_LUA,
                        (
                            self._qkey("leases", queue),
                            self._qkey("delayed", queue),
                            self._qkey("prio", queue),
                        ),
                        (repr(moment), self._msg_prefix(queue)),
                    )
                    or 0
                )
            return total
        return int(
            self._client.eval(
                _REAP_LUA, (), (self.prefix, repr(moment), str(len(queues)), *queues)
            )
            or 0
        )

    def reap_expired_jobs(self, now: float | None = None) -> int:
        """Redis 上过期在 reserve 的 promote/claim 阶段判定，这里不额外全表扫描。"""
        return 0

    # ------------------------------------------------------------------ DLQ
    def dead_letters(self, *, queue: str | None = None) -> list[DeadLetter]:
        entries: list[DeadLetter] = []
        for name in self._queues([queue] if queue else None):
            ids = self._client.execute("LRANGE", self._qkey("dlq", name), 0, -1) or []
            for raw_id in ids:
                msg = self._hash(self._mkey(name, self._s(raw_id)))
                if not msg:
                    continue
                envelope = self._decode(msg[b"envelope"])
                entries.append(
                    DeadLetter(
                        message_id=int(self._s(msg.get(b"seq"), "0")),
                        job_id=self._s(msg.get(b"job_id")),
                        queue=self._s(msg.get(b"queue")),
                        task=envelope.task,
                        envelope=envelope,
                        reason=self._s(msg.get(b"dead_reason")),
                        deliveries=int(self._s(msg.get(b"deliveries"), "0")),
                        failed_at=float(self._s(msg.get(b"claimed_at"), "0") or 0),
                    )
                )
        return entries

    def replay_dead(
        self, message_id: int, *, queue: str | None = None, priority: int | None = None
    ) -> bool:
        source_queue = self._locate_msg(message_id, queue)
        key = self._mkey(source_queue, message_id)
        msg = self._hash(key)
        if not msg or self._s(msg.get(b"state")) != "dead":
            return False
        now = self._now()
        msg_queue = self._s(msg.get(b"queue"))
        target_queue = queue or msg_queue
        target_priority = int(self._s(msg.get(b"priority"), "0")) if priority is None else int(priority)
        envelope = self._decode(msg[b"envelope"]).with_routing(target_queue, target_priority)
        import dataclasses

        envelope = dataclasses.replace(envelope, attempt=1)
        seq = message_id
        fields: list[Any] = [
            "seq", str(seq),                       # 少这个字段，重放后的投递 message_id 会变 0（ack 找不到）
            "state", "queued",
            "queue", target_queue,
            "priority", str(target_priority),
            "envelope", self._encode(envelope),
            "score", repr(_score(target_priority, seq)),
            "visible_at", repr(now),
            "deliveries", "0",
            "yields", "0",
            "yieldable", "1",
            "dead_reason", "",
            "last_error", "",
        ]
        if self.cluster and target_queue != msg_queue:
            # 换队列 = 换 slot：把消息**整份**搬到目标队列的槽里（否则目标队列的 Lua 碰不到它，
            # 而且 job_id/task/expires_at 这些没进 fields 的字段会丢）
            merged: dict[bytes, bytes] = dict(msg)
            for index in range(0, len(fields), 2):
                name = str(fields[index]).encode()
                value = fields[index + 1]
                merged[name] = value if isinstance(value, bytes) else str(value).encode()
            flat: list[Any] = [item for pair in merged.items() for item in pair]
            self._client.execute("HSET", self._mkey(target_queue, seq), *flat)
            self._client.execute("DEL", key)
            self._client.execute("HSET", self._key("msgq"), str(seq), target_queue)
        else:
            self._client.execute("HSET", key, *fields)
        self._client.execute("ZADD", self._qkey("delayed", target_queue), repr(now), str(seq))
        self._client.execute("LREM", self._qkey("dlq", msg_queue), 0, str(seq))
        self._client.execute("SADD", self._key("queues"), target_queue)
        self._client.execute("HINCRBY", self._qkey("prio", target_queue), str(target_priority), 1)
        job_key = self._key("job", self._s(msg.get(b"job_id")))
        self._client.execute("HSET", job_key, "state", JobState.QUEUED, "error", "", "updated_at", repr(now))
        return True

    def _locate_msg(self, message_id: int, hint: str | None = None) -> str:
        """消息当前所在队列：Cluster 下 msg 键带队列 hash tag，必须先知道队列才能定位。

        单机下 msg 键与队列无关，直接返回空串（`_msg_prefix` 忽略它）。
        """
        if not self.cluster:
            return ""
        found = self._s(self._client.execute("HGET", self._key("msgq"), str(message_id)))
        return found or (hint or "")

    # ------------------------------------------------------------------ 租约
    def acquire_lease(self, name: str, owner: str, ttl: float) -> bool:
        key = self._key("nlease", name)
        ttl_ms = str(int(max(0.0, ttl) * 1000))
        if not self.lua_enabled:
            return self._acquire_lease_fallback(key, owner, ttl_ms)
        result = self._client.eval(_ACQUIRE_LEASE_LUA, (key,), (owner, ttl_ms))
        return bool(result)

    def release_lease(self, name: str, owner: str) -> None:
        key = self._key("nlease", name)
        if not self.lua_enabled:
            self._release_lease_fallback(key, owner)
            return
        self._client.eval(_RELEASE_LEASE_LUA, (key,), (owner,))

    def renew_lease(self, name: str, owner: str, ttl: float) -> bool:
        return self.acquire_lease(name, owner, ttl)

    # ------------------------------------------------- 无 Lua 回退（WATCH/MULTI/EXEC）
    def _reserve_fallback(
        self, wanted: list[str], *, worker_id: str, lease: float, limit: int, now: float
    ) -> list[Delivery]:
        """没有 Lua 时的 claim：先 promote 到期消息，再逐个用乐观事务抢。"""
        self._promote_due_fallback(wanted, now)
        claimed: list[Delivery] = []
        budget = limit * 5 + 10
        while len(claimed) < limit and budget > 0:
            budget -= 1
            candidate = self._peek_candidate(wanted)
            if candidate is None:
                break
            score, queue, msg_id = candidate
            if self._claim_transaction(queue, msg_id, score, worker_id, lease, now) != "claimed":
                continue
            msg = self._hash(self._mkey(queue, msg_id))
            if msg:
                claimed.append(self._to_delivery(msg, worker_id))
        return claimed

    def _peek_candidate(self, wanted: Sequence[str]) -> tuple[float, str, str] | None:
        """档内加权轮询（与 Lua 路径同一套规则 + 同样 tie-break）。"""
        heads: list[tuple[str, float, str]] = []
        for queue in wanted:
            top = self._client.execute("ZRANGE", self._qkey("ready", queue), 0, 0, "WITHSCORES")
            if top:
                heads.append((queue, float(top[1]), self._s(top[0])))
        if not heads:
            return None
        band = max(_priority_of(score) for _q, score, _i in heads)
        best: tuple[float, str, str] | None = None
        best_ratio: float | None = None
        for queue, score, msg_id in sorted(heads):        # 队列名升序 → tie-break 与 Lua 一致
            if _priority_of(score) != band:
                continue
            weight = max(1, self._weights.get(queue, 1))
            served = int(self._client.execute("GET", self._qkey("served", queue)) or 0)
            ratio = served / weight
            if best_ratio is None or ratio < best_ratio:
                best, best_ratio = (score, queue, msg_id), ratio
        if best is None:  # pragma: no cover - heads 非空必有候选
            return None
        self._client.execute("INCR", self._qkey("served", best[1]))
        return best

    def _claim_transaction(
        self, queue: str, msg_id: str, score: float, worker_id: str, lease: float, now: float
    ) -> str:
        """乐观事务抢一条消息：被抢就返回 lost，调用方换下一条。"""
        msg_key = self._mkey(queue, msg_id)
        ready_key = self._qkey("ready", queue)
        info: dict[str, Any] = {}

        def verify() -> bool:
            current = self._client.execute("ZSCORE", ready_key, msg_id)
            if current is None or float(current) != score:
                info["abort"] = "lost"
                return False
            row = self._hash(msg_key)
            if self._s(row.get(b"state")) != "queued":
                info["abort"] = "lost"
                return False
            info["priority"] = self._s(row.get(b"priority"), "0")
            info["job_id"] = self._s(row.get(b"job_id"))
            expires = self._s(row.get(b"expires_at"))
            info["outcome"] = "expired" if (expires and float(expires) <= now) else "claimed"
            return True

        def commands() -> list[tuple[Any, ...]]:
            if info["outcome"] == "expired":
                return [
                    ("ZREM", ready_key, msg_id),
                    ("HSET", msg_key, "state", "expired", "dead_reason", "expired"),
                    *self._job_state_command(info, JobState.EXPIRED, now),
                ]
            return [
                ("ZREM", ready_key, msg_id),
                (
                    "HSET",
                    msg_key,
                    "state",
                    "reserved",
                    "claimed_by",
                    worker_id,
                    "claimed_at",
                    repr(now),
                    "lease_until",
                    repr(now + lease),
                ),
                ("HINCRBY", msg_key, "deliveries", 1),
                ("ZADD", self._qkey("leases", queue), repr(now + lease), msg_id),
                ("HINCRBY", self._qkey("prio", queue), info.get("priority", "0"), -1),
                *self._job_state_command(info, JobState.RUNNING, now),
            ]

        result = self._client.watch_transaction((ready_key, msg_key), verify, commands)
        if result is None:
            # 两种情况：verify() 主动放弃（abort 有原因）或 EXEC 被抢（没写任何东西）——
            # 后者绝不能当成 claimed，否则会凭空多出一个「抢到」的投递。
            return str(info.get("abort") or "lost")
        outcome = str(info.get("outcome") or "claimed")
        if self.cluster and info.get("job_id"):
            # Cluster 下 job 键与队列不同槽，事务里不能碰 → 提交后由 Python 补
            if outcome == "expired":
                self._expire_job(info["job_id"], now)
            else:
                self._mark_job_running(info["job_id"], now)
        return outcome

    def _job_state_command(
        self, info: Mapping[str, Any], state: str, now: float
    ) -> list[tuple[Any, ...]]:
        """事务内的 job 状态写命令；Cluster 下返回空（跨槽的键不能进事务）。"""
        if self.cluster or not info.get("job_id"):
            return []
        key = self._key("job", info["job_id"])
        if state == JobState.EXPIRED:
            return [
                ("HSET", key, "state", state, "error", "expired before execution", "updated_at", repr(now))
            ]
        return [("HSET", key, "state", state, "updated_at", repr(now))]

    def _promote_due_fallback(self, wanted: Sequence[str], now: float) -> int:
        promoted = 0
        for queue in wanted:
            dkey = self._qkey("delayed", queue)
            ids = (
                self._client.execute("ZRANGEBYSCORE", dkey, "-inf", repr(now), "LIMIT", 0, 200) or []
            )
            for raw_id in ids:
                if self._promote_one_fallback(queue, self._s(raw_id), now):
                    promoted += 1
        return promoted

    def _promote_one_fallback(self, queue: str, msg_id: str, now: float) -> bool:
        dkey = self._qkey("delayed", queue)
        msg_key = self._mkey(queue, msg_id)
        info: dict[str, Any] = {}

        def verify() -> bool:
            score = self._client.execute("ZSCORE", dkey, msg_id)
            if score is None or float(score) > now:
                return False
            row = self._hash(msg_key)
            if not row or self._s(row.get(b"state")) != "queued":
                return False
            info["score"] = self._s(row.get(b"score"), "0")
            info["job_id"] = self._s(row.get(b"job_id"))
            expires = self._s(row.get(b"expires_at"))
            info["outcome"] = "expired" if (expires and float(expires) <= now) else "ready"
            return True

        def commands() -> list[tuple[Any, ...]]:
            if info["outcome"] == "expired":
                return [
                    ("ZREM", dkey, msg_id),
                    ("HSET", msg_key, "state", "expired", "dead_reason", "expired"),
                    *self._job_state_command(info, JobState.EXPIRED, now),
                ]
            return [("ZREM", dkey, msg_id), ("ZADD", self._qkey("ready", queue), info["score"], msg_id)]

        result = self._client.watch_transaction((dkey, msg_key), verify, commands)
        if result is not None and self.cluster and info.get("outcome") == "expired":
            self._expire_job(info.get("job_id", ""), now)     # 跨槽的 job 键：提交后补
        return result is not None

    def _mutate_fallback(
        self, delivery: Delivery, action: str, *, extra: float, reason: str, max_yields: int
    ) -> str:
        """无 Lua 的状态转换：同一套判定（幂等 ack / 租约易主 / 让位上限），乐观事务写。"""
        msg_id = str(delivery.message_id)
        msg_key = self._mkey(delivery.queue, msg_id)
        info: dict[str, Any] = {}

        def verify() -> bool:
            row = self._hash(msg_key)
            if not row:
                info["outcome"] = "missing"
                return False
            state = self._s(row.get(b"state"))
            if action == "ack" and state == "acked":
                info["outcome"] = "ok"
                return False
            if action == "dead" and state == "dead":
                info["outcome"] = "ok"
                return False
            if state != "reserved" or self._s(row.get(b"claimed_by")) != delivery.worker_id:
                info["outcome"] = "lost"
                return False
            if action == "yield" and self._s(row.get(b"yieldable"), "1") == "0":
                info["outcome"] = "not_yieldable"
                return False
            info["queue"] = self._s(row.get(b"queue"))
            info["priority"] = self._s(row.get(b"priority"), "0")
            info["deliveries"] = int(self._s(row.get(b"deliveries"), "0"))
            info["yields"] = int(self._s(row.get(b"yields"), "0"))
            return True

        def commands() -> list[tuple[Any, ...]]:
            queue = info.get("queue", "")
            leases_key = self._qkey("leases", queue)
            if action == "ack":
                return [
                    (
                        "HSET",
                        msg_key,
                        "state",
                        "acked",
                        "claimed_by",
                        "",
                        "claimed_at",
                        "",
                        "lease_until",
                        "",
                    ),
                    ("ZREM", leases_key, msg_id),
                ]
            if action == "dead":
                return [
                    (
                        "HSET",
                        msg_key,
                        "state",
                        "dead",
                        "dead_reason",
                        reason,
                        "last_error",
                        reason,
                        "claimed_by",
                        "",
                        "claimed_at",
                        "",
                        "lease_until",
                        "",
                    ),
                    ("ZREM", leases_key, msg_id),
                    ("RPUSH", self._qkey("dlq", queue), msg_id),
                ]
            if action == "extend":
                until = repr(self._now() + extra)
                return [("HSET", msg_key, "lease_until", until), ("ZADD", leases_key, until, msg_id)]
            now = self._now()
            writes: list[tuple[Any, ...]] = []
            if action == "defer":
                writes.append(
                    ("HSET", msg_key, "deliveries", str(max(0, int(info.get("deliveries", 1)) - 1)))
                )
            if action == "yield":
                yields = int(info.get("yields", 0)) + 1
                writes.append(("HSET", msg_key, "yields", str(yields)))
                writes.append(("HSET", msg_key, "yieldable", "0" if yields >= max_yields else "1"))
            writes.extend(
                [
                    (
                        "HSET",
                        msg_key,
                        "state",
                        "queued",
                        "visible_at",
                        repr(now + extra),
                        "claimed_by",
                        "",
                        "claimed_at",
                        "",
                        "lease_until",
                        "",
                    ),
                    ("ZREM", leases_key, msg_id),
                    ("ZADD", self._qkey("delayed", queue), repr(now + extra), msg_id),
                    ("HINCRBY", self._qkey("prio", queue), info.get("priority", "0"), 1),
                ]
            )
            return writes

        for _ in range(3):
            result = self._client.watch_transaction((msg_key,), verify, commands)
            if result is not None:
                return "ok"
            if info.get("outcome"):
                return str(info["outcome"])
        return "lost"

    def _requeue_reserved_fallback(self, queue: str, msg_id: str, now: float) -> bool:
        msg_key = self._mkey(queue, msg_id)
        leases_key = self._qkey("leases", queue)
        info: dict[str, Any] = {}

        def verify() -> bool:
            if self._s(self._client.execute("HGET", msg_key, "state")) != "reserved":
                return False
            info["priority"] = self._s(self._client.execute("HGET", msg_key, "priority"), "0")
            return True

        def commands() -> list[tuple[Any, ...]]:
            return [
                ("ZREM", leases_key, msg_id),
                (
                    "HSET",
                    msg_key,
                    "state",
                    "queued",
                    "visible_at",
                    repr(now),
                    "claimed_by",
                    "",
                    "claimed_at",
                    "",
                    "lease_until",
                    "",
                ),
                ("ZADD", self._qkey("delayed", queue), repr(now), msg_id),
                ("HINCRBY", self._qkey("prio", queue), info.get("priority", "0"), 1),
            ]

        return (
            self._client.watch_transaction((msg_key, leases_key), verify, commands) is not None
        )

    def _reap_fallback(self, now: float) -> int:
        total = 0
        for queue in self._queues(None):
            lkey = self._qkey("leases", queue)
            ids = (
                self._client.execute("ZRANGEBYSCORE", lkey, "-inf", repr(now), "LIMIT", 0, 200) or []
            )
            for raw_id in ids:
                if self._requeue_reserved_fallback(queue, self._s(raw_id), now):
                    total += 1
        return total

    def _acquire_lease_fallback(self, key: str, owner: str, ttl_ms: str) -> bool:
        if self._client.execute("SET", key, owner, "NX", "PX", ttl_ms) is not None:
            return True
        result = self._client.watch_transaction(
            (key,),
            lambda: self._s(self._client.execute("GET", key)) == owner,
            lambda: [("SET", key, owner, "PX", ttl_ms)],
        )
        return result is not None

    def _release_lease_fallback(self, key: str, owner: str) -> None:
        self._client.watch_transaction(
            (key,),
            lambda: self._s(self._client.execute("GET", key)) == owner,
            lambda: [("DEL", key)],
        )

    # -------------------------------------------------------------- worker 心跳
    def register_worker(
        self,
        worker_id: str,
        *,
        queues: Sequence[str] = (),
        pool: str = "",
        concurrency: int = 0,
        meta: Mapping[str, Any] | None = None,
        now: float | None = None,
    ) -> None:
        moment = self._now() if now is None else float(now)
        self._client.execute(
            "HSET",
            self._key("worker", worker_id),
            "queues", ",".join(queues),
            "pool", pool,
            "concurrency", str(int(concurrency)),
            "started_at", repr(moment),
            "heartbeat_at", repr(moment),
            "meta", json.dumps({"queues": list(queues), **(meta or {})}),
        )
        self._client.execute("ZADD", self._key("workers"), repr(moment), worker_id)

    def heartbeat_worker(
        self,
        worker_id: str,
        *,
        meta: Mapping[str, Any] | None = None,
        now: float | None = None,
    ) -> None:
        moment = self._now() if now is None else float(now)
        if not self._client.execute("EXISTS", self._key("worker", worker_id)):
            self.register_worker(worker_id, meta=meta, now=moment)
            return
        self._client.execute("HSET", self._key("worker", worker_id), "heartbeat_at", repr(moment))
        self._client.execute("ZADD", self._key("workers"), repr(moment), worker_id)

    def deregister_worker(self, worker_id: str) -> None:
        self._client.execute("DEL", self._key("worker", worker_id))
        self._client.execute("ZREM", self._key("workers"), worker_id)

    def list_workers(
        self, *, stale_after: float = 60.0, now: float | None = None
    ) -> list[WorkerInfo]:
        ids = self._client.execute("ZRANGE", self._key("workers"), 0, -1) or []
        workers: list[WorkerInfo] = []
        for raw_id in ids:
            worker_id = self._s(raw_id)
            row = self._hash(self._key("worker", worker_id))
            if not row:
                continue
            meta = self._load_json(row.get(b"meta"))
            workers.append(
                WorkerInfo(
                    worker_id=worker_id,
                    queues=tuple(filter(None, self._s(row.get(b"queues")).split(","))),
                    pool=self._s(row.get(b"pool")),
                    concurrency=int(self._s(row.get(b"concurrency"), "0")),
                    started_at=float(self._s(row.get(b"started_at"), "0") or 0),
                    heartbeat_at=float(self._s(row.get(b"heartbeat_at"), "0") or 0),
                    meta=meta if isinstance(meta, dict) else {},
                )
            )
        return workers

    def list_jobs(
        self,
        *,
        prefix: str | None = None,
        states: Sequence[str] | None = None,
        limit: int = 100,
    ) -> list[JobRecord]:
        """走 `{prefix}jobs` ZSET 索引（updated_at 倒序）。

        为什么不用 SCAN：Redis Cluster 的 SCAN 只扫**单个节点**，会漏键；
        ZSET 索引是单键结构（cluster 里也只落一个槽），既对又更快。
        索引从本版本开始维护（老数据没有索引项，属预期）。
        """
        wanted = set(states) if states is not None else None
        max_items = max(1, min(5000, max(limit, 1) * 20))
        ids = self._client.execute("ZREVRANGE", self._key("jobs"), 0, max_items - 1) or []
        found: list[JobRecord] = []
        for raw_id in ids:
            job_id = self._s(raw_id)
            if prefix and not job_id.startswith(prefix):
                continue
            record = self.get_state(job_id)
            if record is None:
                continue
            if wanted is not None and record.state not in wanted:
                continue
            found.append(record)
            if len(found) >= max(0, limit):
                break
        return found[: max(0, limit)]

    def flush_prefix(self) -> int:
        """删除本实例前缀下的所有键（测试清理用；绝不影响别人的键空间）。"""
        return self._client.delete_prefix(self.prefix.rstrip(":"))

    def close(self) -> None:
        self._client.close()
