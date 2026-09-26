"""SQLite transport：零外部服务的关键（WAL + 原子 claim）。

设计依据：docs/design.md §9.3、docs/design/priority.md §7。

- **原子 claim**：`BEGIN IMMEDIATE` 包住「定档 → 档内轮询 → 置 reserved」，
  多进程共享同一个 db 文件时也只有一个赢家；
- **方案 D**：全局 `SELECT MAX(priority)` 定档；同一档位内按 `weight` 每队列 `LIMIT 1` 轮询；
- **让位**：`yields` / `yieldable` 独立于 `deliveries`，让位不会把人送进 DLQ；
- **吞吐边界**（SSD + WAL 单文件）：enqueue 约 3k–10k msg/s，claim 约 1k–3k msg/s；
  超过这个量级请上 `redis://`（Phase 1）。不做「什么都能扛」的承诺。
"""
from __future__ import annotations

import contextlib
import dataclasses
import json
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any

from ..errors import DecodeError, LeaseLost, MessageNotFound, TransportError
from ..protocol import Codec, CodecRegistry, Envelope, decode_value, encode_value
from .base import (
    UNSET,
    DeadLetter,
    Delivery,
    JobRecord,
    JobState,
    MessageState,
    QueueStat,
    Transport,
)

__all__ = ["SqliteTransport"]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id       TEXT    NOT NULL,
  queue        TEXT    NOT NULL,
  task         TEXT    NOT NULL,
  envelope     BLOB    NOT NULL,
  priority     INTEGER NOT NULL DEFAULT 0,
  state        TEXT    NOT NULL DEFAULT 'queued',
  visible_at   REAL    NOT NULL,
  expires_at   REAL,
  claimed_by   TEXT,
  claimed_at   REAL,
  lease_until  REAL,
  deliveries   INTEGER NOT NULL DEFAULT 0,
  yields       INTEGER NOT NULL DEFAULT 0,
  yieldable    INTEGER NOT NULL DEFAULT 1,
  dead_reason  TEXT,
  last_error   TEXT
);
CREATE INDEX IF NOT EXISTS idx_claim       ON messages(state, visible_at, priority DESC, id);
CREATE INDEX IF NOT EXISTS idx_claim_queue ON messages(state, queue, visible_at, priority DESC, id);
CREATE INDEX IF NOT EXISTS idx_job         ON messages(job_id);

CREATE TABLE IF NOT EXISTS jobs (
  job_id     TEXT PRIMARY KEY,
  task       TEXT NOT NULL,
  state      TEXT NOT NULL,
  attempt    INTEGER NOT NULL DEFAULT 0,
  result     TEXT,
  has_result INTEGER NOT NULL DEFAULT 0,
  error      TEXT,
  meta       TEXT NOT NULL DEFAULT '{}',
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS idempotency (
  key        TEXT PRIMARY KEY,
  job_id     TEXT NOT NULL,
  created_at REAL NOT NULL,
  expires_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (   -- concurrency_key 互斥 / beat 选主
  name       TEXT PRIMARY KEY,
  owner      TEXT NOT NULL,
  expires_at REAL NOT NULL
);
"""

_MESSAGE_COLUMNS = (
    "id, job_id, queue, envelope, priority, state, visible_at, expires_at, "
    "claimed_by, claimed_at, lease_until, deliveries, yields, yieldable, dead_reason"
)


class SqliteTransport(Transport):
    """单文件 SQLite transport。`clock` 可注入以便测试租约/退避。"""

    supports_leases = True

    def __init__(
        self,
        path: str,
        *,
        codec: Codec,
        registry: CodecRegistry | None = None,
        clock: Callable[[], float] | None = None,
        busy_timeout: float = 5.0,
        idempotency_ttl: float = 86400.0,
        queue_weights: Mapping[str, int] | None = None,
        max_message_bytes: int | None = None,
    ) -> None:
        if not path:
            raise TransportError("sqlite:// 需要文件路径（例如 sqlite:///./taskmq.db）")
        self.path = path
        self._codec = codec
        self._registry = registry if registry is not None else CodecRegistry()
        self._clock = clock or time.time
        self._lock = threading.RLock()
        self._served: dict[str, int] = {}
        self._weights = {name: max(1, int(weight)) for name, weight in (queue_weights or {}).items()}
        self._idempotency_ttl = idempotency_ttl
        self._max_message_bytes = max_message_bytes
        self._closed = False
        self._conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(f"PRAGMA busy_timeout={int(busy_timeout * 1000)}")
        with self._lock:
            self._conn.executescript(_SCHEMA)

    # ------------------------------------------------------------------ 基础设施
    def _now(self) -> float:
        return float(self._clock())

    @contextlib.contextmanager
    def _tx(self) -> Iterator[None]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        else:
            self._conn.execute("COMMIT")

    def set_queue_weights(self, weights: Mapping[str, int]) -> None:
        with self._lock:
            self._weights = {name: max(1, int(weight)) for name, weight in weights.items()}

    def _weight(self, queue: str) -> int:
        return max(1, self._weights.get(queue, 1))

    def _encode(self, env: Envelope) -> bytes:
        return self._codec.encode(env, max_bytes=self._max_message_bytes)

    def _decode(self, blob: bytes) -> Envelope:
        return self._codec.decode(blob)

    @staticmethod
    def _placeholders(count: int) -> str:
        return ",".join("?" * count)

    def _to_delivery(self, row: sqlite3.Row, worker_id: str) -> Delivery:
        envelope = self._decode(row["envelope"])
        return Delivery(
            job_id=row["job_id"],
            message_id=int(row["id"]),
            queue=row["queue"],
            envelope=envelope,
            worker_id=worker_id,
            attempt=int(envelope.attempt),
            deliveries=int(row["deliveries"]),
            lease_until=float(row["lease_until"] or 0.0),
            reserved_at=float(row["claimed_at"] or 0.0),
            priority=int(row["priority"]),
            yields=int(row["yields"]),
        )

    def _message(self, message_id: int) -> sqlite3.Row | None:
        return self._conn.execute(
            f"SELECT {_MESSAGE_COLUMNS} FROM messages WHERE id=?", (message_id,)
        ).fetchone()

    def _require_owner(self, delivery: Delivery) -> sqlite3.Row:
        row = self._message(delivery.message_id)
        if row is None:
            raise MessageNotFound(f"消息 {delivery.message_id} 不存在")
        if row["state"] == MessageState.RESERVED and row["claimed_by"] == delivery.worker_id:
            return row
        raise LeaseLost(
            f"消息 {delivery.message_id} 当前 state={row['state']!r} holder={row['claimed_by']!r}，"
            f"不是 worker {delivery.worker_id!r} 持有"
        )

    # ------------------------------------------------------------------ 入队
    def enqueue(
        self,
        env: Envelope,
        *,
        queue: str | None = None,
        delay: float = 0.0,
        priority: int | None = None,
    ) -> str:
        if self._closed:
            raise TransportError("transport 已关闭")
        now = self._now()
        target = queue or env.queue or "default"
        prio = env.priority if priority is None else int(priority)
        effective = env if (env.queue == target and env.priority == prio) else env.with_routing(target, prio)
        blob = self._encode(effective)
        with self._lock, self._tx():
            if env.key:
                self._conn.execute("DELETE FROM idempotency WHERE expires_at<=?", (now,))
                row = self._conn.execute(
                    "SELECT job_id FROM idempotency WHERE key=?", (env.key,)
                ).fetchone()
                if row is not None:
                    return str(row["job_id"])
            visible_at = now + max(0.0, float(delay))
            if env.eta is not None:
                visible_at = max(visible_at, float(env.eta))
            self._conn.execute(
                "INSERT INTO messages(job_id, queue, task, envelope, priority, state, visible_at, expires_at)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (env.id, target, env.task, blob, prio, MessageState.QUEUED, visible_at, env.expires_at),
            )
            if env.key:
                self._conn.execute(
                    "INSERT OR REPLACE INTO idempotency(key, job_id, created_at, expires_at)"
                    " VALUES (?,?,?,?)",
                    (env.key, env.id, now, now + self._idempotency_ttl),
                )
            self._conn.execute(
                "INSERT INTO jobs(job_id, task, state, attempt, meta, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?)"
                " ON CONFLICT(job_id) DO UPDATE SET state='QUEUED', attempt=excluded.attempt,"
                " updated_at=excluded.updated_at",
                (env.id, env.task, JobState.QUEUED, env.attempt,
                 json.dumps({"queue": target, "priority": prio}), now, now),
            )
            return env.id

    # ------------------------------------------------------------------ 投递
    def _max_priority(self, queues: Sequence[str], now: float) -> int | None:
        row = self._conn.execute(
            f"SELECT MAX(priority) AS top FROM messages WHERE state='queued'"
            f" AND queue IN ({self._placeholders(len(queues))}) AND visible_at<=?"
            f" AND (expires_at IS NULL OR expires_at>?)",
            (*queues, now, now),
        ).fetchone()
        return None if row is None or row["top"] is None else int(row["top"])

    def reserve(
        self,
        queues: Sequence[str],
        *,
        worker_id: str,
        lease: float,
        limit: int,
    ) -> list[Delivery]:
        if limit <= 0:
            return []
        wanted = list(dict.fromkeys(queues))
        if not wanted:
            return []
        now = self._now()
        with self._lock, self._tx():
            self._requeue_expired_locked(now)
            self._expire_overdue_locked(now)
            picked: list[sqlite3.Row] = []
            served = dict(self._served)
            while len(picked) < limit:
                top = self._max_priority(wanted, now)
                if top is None:
                    break
                per_queue: dict[str, sqlite3.Row] = {}
                for queue in wanted:
                    row = self._conn.execute(
                        f"SELECT {_MESSAGE_COLUMNS} FROM messages WHERE state='queued' AND queue=?"
                        " AND priority=? AND visible_at<=? AND (expires_at IS NULL OR expires_at>?)"
                        " ORDER BY id LIMIT 1",
                        (queue, top, now, now),
                    ).fetchone()
                    if row is not None:
                        per_queue[queue] = row
                if not per_queue:
                    break
                chosen = min(
                    per_queue, key=lambda queue: (served.get(queue, 0) / self._weight(queue), queue)
                )
                row = per_queue[chosen]
                cursor = self._conn.execute(
                    "UPDATE messages SET state='reserved', claimed_by=?, claimed_at=?,"
                    " lease_until=?, deliveries=deliveries+1"
                    " WHERE id=? AND state='queued'",
                    (worker_id, now, now + max(0.0, float(lease)), row["id"]),
                )
                if cursor.rowcount == 0:  # pragma: no cover - 同一事务内不该发生
                    continue
                served[chosen] = served.get(chosen, 0) + 1
                updated = self._message(int(row["id"]))
                if updated is not None:
                    picked.append(updated)
            self._served = served
            deliveries = [self._to_delivery(row, worker_id) for row in picked]
            for delivery in deliveries:
                self._conn.execute(
                    "UPDATE jobs SET state=?, attempt=?, updated_at=? WHERE job_id=? AND state IN (?,?)",
                    (JobState.RUNNING, delivery.attempt, now, delivery.job_id,
                     JobState.QUEUED, JobState.RETRYING),
                )
            return deliveries

    def ack(self, delivery: Delivery) -> None:
        now = self._now()
        with self._lock, self._tx():
            cursor = self._conn.execute(
                "UPDATE messages SET state='acked', claimed_by=NULL, claimed_at=NULL, lease_until=NULL"
                " WHERE id=? AND state='reserved' AND claimed_by=?",
                (delivery.message_id, delivery.worker_id),
            )
            if cursor.rowcount:
                self._conn.execute(
                    "UPDATE jobs SET updated_at=? WHERE job_id=? AND state=?",
                    (now, delivery.job_id, JobState.RUNNING),
                )
                return
            row = self._message(delivery.message_id)
            if row is None:
                raise MessageNotFound(f"消息 {delivery.message_id} 不存在")
            if row["state"] == MessageState.ACKED:
                return
            raise LeaseLost(
                f"消息 {delivery.message_id} 已被回收（state={row['state']!r}），ack 被拒绝"
            )

    def nack(self, delivery: Delivery, *, requeue: bool = True, delay: float = 0.0) -> None:
        now = self._now()
        with self._lock, self._tx():
            row = self._message(delivery.message_id)
            if row is None:
                raise MessageNotFound(f"消息 {delivery.message_id} 不存在")
            if row["state"] == MessageState.QUEUED and row["claimed_by"] is None:
                return
            self._require_owner(delivery)
            if requeue:
                self._conn.execute(
                    "UPDATE messages SET state='queued', claimed_by=NULL, claimed_at=NULL,"
                    " lease_until=NULL, visible_at=? WHERE id=?",
                    (now + max(0.0, float(delay)), delivery.message_id),
                )
            else:
                self._conn.execute(
                    "UPDATE messages SET state='dead', claimed_by=NULL, claimed_at=NULL,"
                    " lease_until=NULL, dead_reason=COALESCE(dead_reason,'nacked without requeue')"
                    " WHERE id=?",
                    (delivery.message_id,),
                )

    def dead_letter(self, delivery: Delivery, reason: str) -> None:
        now = self._now()
        with self._lock, self._tx():
            row = self._message(delivery.message_id)
            if row is None:
                raise MessageNotFound(f"消息 {delivery.message_id} 不存在")
            if row["state"] == MessageState.DEAD:
                return
            self._require_owner(delivery)
            self._conn.execute(
                "UPDATE messages SET state='dead', claimed_by=NULL, claimed_at=NULL,"
                " lease_until=NULL, dead_reason=?, last_error=? WHERE id=?",
                (reason, reason, delivery.message_id),
            )
            self._conn.execute(
                "UPDATE jobs SET updated_at=? WHERE job_id=? AND state NOT IN (?,?,?,?)",
                (now, delivery.job_id, JobState.SUCCEEDED, JobState.FAILED,
                 JobState.EXPIRED, JobState.REVOKED),
            )

    def extend_lease(self, delivery: Delivery, seconds: float) -> None:
        now = self._now()
        with self._lock, self._tx():
            self._require_owner(delivery)
            self._conn.execute(
                "UPDATE messages SET lease_until=? WHERE id=?",
                (now + max(0.0, float(seconds)), delivery.message_id),
            )

    def defer(self, delivery: Delivery, *, delay: float = 0.0) -> None:
        now = self._now()
        with self._lock, self._tx():
            row = self._require_owner(delivery)
            self._conn.execute(
                "UPDATE messages SET state='queued', visible_at=?,"
                " deliveries=MAX(0, deliveries-1), claimed_by=NULL, claimed_at=NULL,"
                " lease_until=NULL WHERE id=?",
                (now + max(0.0, float(delay)), delivery.message_id),
            )
            self._conn.execute(
                "UPDATE jobs SET state=?, updated_at=? WHERE job_id=? AND state IN (?,?)",
                (JobState.QUEUED, now, row["job_id"], JobState.RUNNING, JobState.RETRYING),
            )

    # ------------------------------------------------------------------ 命名租约
    def acquire_lease(self, name: str, owner: str, ttl: float) -> bool:
        now = self._now()
        with self._lock, self._tx():
            self._conn.execute("DELETE FROM leases WHERE expires_at<=?", (now,))
            row = self._conn.execute("SELECT owner FROM leases WHERE name=?", (name,)).fetchone()
            expires = now + max(0.0, float(ttl))
            if row is None:
                self._conn.execute(
                    "INSERT INTO leases(name, owner, expires_at) VALUES (?,?,?)",
                    (name, owner, expires),
                )
                return True
            if row["owner"] == owner:
                self._conn.execute("UPDATE leases SET expires_at=? WHERE name=?", (expires, name))
                return True
            return False

    def release_lease(self, name: str, owner: str) -> None:
        with self._lock, self._tx():
            self._conn.execute("DELETE FROM leases WHERE name=? AND owner=?", (name, owner))

    def renew_lease(self, name: str, owner: str, ttl: float) -> bool:
        now = self._now()
        with self._lock, self._tx():
            cursor = self._conn.execute(
                "UPDATE leases SET expires_at=? WHERE name=? AND owner=? AND expires_at>?",
                (now + max(0.0, float(ttl)), name, owner, now),
            )
            return bool(cursor.rowcount)

    # ------------------------------------------------------------------ 插队
    def peek_max_priority(self, queues: Sequence[str], *, now: float | None = None) -> int | None:
        wanted = list(dict.fromkeys(queues))
        if not wanted:
            return None
        moment = self._now() if now is None else float(now)
        with self._lock:
            return self._max_priority(wanted, moment)

    def yield_reservation(
        self,
        delivery: Delivery,
        *,
        delay: float = 0.0,
        max_yields: int = 100,
    ) -> bool:
        now = self._now()
        with self._lock, self._tx():
            row = self._require_owner(delivery)
            if not int(row["yieldable"]):
                return False
            yields = int(row["yields"]) + 1
            yieldable = 0 if yields >= max(1, int(max_yields)) else 1
            self._conn.execute(
                "UPDATE messages SET state='queued', visible_at=?, yields=?, yieldable=?,"
                " claimed_by=NULL, claimed_at=NULL, lease_until=NULL WHERE id=?",
                (now + max(0.0, float(delay)), yields, yieldable, delivery.message_id),
            )
            job = self._conn.execute(
                "SELECT meta FROM jobs WHERE job_id=?", (delivery.job_id,)
            ).fetchone()
            if job is not None:
                meta = self._load_meta(job["meta"])
                meta["yields"] = yields
                self._conn.execute(
                    "UPDATE jobs SET state=?, meta=?, updated_at=? WHERE job_id=?"
                    " AND state IN (?,?,?)",
                    (JobState.QUEUED, self._dump_meta(meta), now, delivery.job_id,
                     JobState.RUNNING, JobState.QUEUED, JobState.RETRYING),
                )
            return True

    def next_visible_at(self, queues: Sequence[str], *, now: float | None = None) -> float | None:
        wanted = list(dict.fromkeys(queues))
        if not wanted:
            return None
        with self._lock:
            row = self._conn.execute(
                f"SELECT MIN(visible_at) AS next FROM messages WHERE state='queued'"
                f" AND queue IN ({self._placeholders(len(wanted))})",
                tuple(wanted),
            ).fetchone()
            return None if row is None or row["next"] is None else float(row["next"])

    # -------------------------------------------------------------- job 状态
    def _load_meta(self, raw: Any) -> dict[str, Any]:
        if not raw:
            return {}
        try:
            decoded = json.loads(raw)
        except ValueError as exc:  # pragma: no cover - 数据损坏
            raise DecodeError(f"jobs.meta 不是合法 JSON：{exc}") from exc
        return dict(decode_value(decoded, self._registry, path="jobs.meta"))

    def _dump_meta(self, meta: Mapping[str, Any]) -> str:
        return json.dumps(encode_value(dict(meta), self._registry, path="jobs.meta"), separators=(",", ":"))

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
        with self._lock, self._tx():
            row = self._conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                merged: dict[str, Any] = {}
                created = now
                current_task = task or ""
                current_attempt = attempt or 0
                current_result = None
                has_result = 0
                current_error = None
            else:
                merged = self._load_meta(row["meta"])
                created = float(row["created_at"])
                current_task = task or str(row["task"])
                current_attempt = int(row["attempt"]) if attempt is None else attempt
                current_result = (
                    decode_value(json.loads(row["result"]), self._registry, path="jobs.result")
                    if row["has_result"] and row["result"] is not None
                    else None
                )
                has_result = int(row["has_result"])
                current_error = row["error"]
            merged.update(meta)
            result_json: str | None
            if result is not UNSET:
                current_result = result
                has_result = 1
                result_json = json.dumps(
                    encode_value(result, self._registry, path="jobs.result"), separators=(",", ":")
                )
            else:
                result_json = row["result"] if row is not None else None
            if error is not UNSET:
                current_error = None if error is None else str(error)
            self._conn.execute(
                "INSERT INTO jobs(job_id, task, state, attempt, result, has_result, error, meta,"
                " created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(job_id) DO UPDATE SET task=excluded.task, state=excluded.state,"
                " attempt=excluded.attempt, result=excluded.result, has_result=excluded.has_result,"
                " error=excluded.error, meta=excluded.meta, updated_at=excluded.updated_at",
                (job_id, current_task, state, current_attempt, result_json, has_result,
                 current_error, self._dump_meta(merged), created, now),
            )
            return JobRecord(
                job_id=job_id,
                task=current_task,
                state=state,
                attempt=current_attempt,
                result=current_result,
                has_result=bool(has_result),
                error=current_error,
                meta=merged,
                created_at=created,
                updated_at=now,
            )

    def get_state(self, job_id: str) -> JobRecord | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                return None
            return JobRecord(
                job_id=str(row["job_id"]),
                task=str(row["task"]),
                state=str(row["state"]),
                attempt=int(row["attempt"]),
                result=(
                    decode_value(json.loads(row["result"]), self._registry, path="jobs.result")
                    if row["has_result"] and row["result"] is not None
                    else None
                ),
                has_result=bool(row["has_result"]),
                error=row["error"],
                meta=self._load_meta(row["meta"]),
                created_at=float(row["created_at"]),
                updated_at=float(row["updated_at"]),
            )

    def queue_stats(self, queues: Sequence[str] | None = None) -> list[QueueStat]:
        wanted = list(dict.fromkeys(queues)) if queues is not None else None
        sql = "SELECT queue, state, COUNT(*) AS n FROM messages"
        params: tuple[Any, ...] = ()
        if wanted is not None:
            sql += f" WHERE queue IN ({self._placeholders(len(wanted))})"
            params = tuple(wanted)
        sql += " GROUP BY queue, state"
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        stats: dict[str, list[int]] = {}
        if wanted is not None:
            for queue in wanted:
                stats[queue] = [0, 0, 0]
        for row in rows:
            bucket = stats.setdefault(str(row["queue"]), [0, 0, 0])
            if row["state"] == MessageState.QUEUED:
                bucket[0] += int(row["n"])
            elif row["state"] == MessageState.RESERVED:
                bucket[1] += int(row["n"])
            elif row["state"] == MessageState.DEAD:
                bucket[2] += int(row["n"])
        return [
            QueueStat(queue=queue, pending=pending, inflight=inflight, dead=dead)
            for queue, (pending, inflight, dead) in sorted(stats.items())
        ]

    def priority_stats(self, queues: Sequence[str] | None = None) -> dict[int, int]:
        wanted = list(dict.fromkeys(queues)) if queues is not None else None
        sql = "SELECT priority, COUNT(*) AS n FROM messages WHERE state='queued'"
        params: tuple[Any, ...] = ()
        if wanted is not None:
            sql += f" AND queue IN ({self._placeholders(len(wanted))})"
            params = tuple(wanted)
        sql += " GROUP BY priority ORDER BY priority"
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return {int(row["priority"]): int(row["n"]) for row in rows}

    # ------------------------------------------------------------------ 回收
    def _requeue_expired_locked(self, now: float) -> int:
        cursor = self._conn.execute(
            "UPDATE messages SET state='queued', visible_at=?, claimed_by=NULL, claimed_at=NULL,"
            " lease_until=NULL WHERE state='reserved' AND lease_until IS NOT NULL AND lease_until<=?",
            (now, now),
        )
        if cursor.rowcount:
            self._conn.execute(
                "UPDATE jobs SET state=?, updated_at=? WHERE state=?",
                (JobState.RETRYING, now, JobState.RUNNING),
            )
        return int(cursor.rowcount)

    def _expire_overdue_locked(self, now: float) -> int:
        cursor = self._conn.execute(
            "UPDATE messages SET state='expired', dead_reason='expired' WHERE state='queued'"
            " AND expires_at IS NOT NULL AND expires_at<=?",
            (now,),
        )
        if cursor.rowcount:
            self._conn.execute(
                "UPDATE jobs SET state=?, error='expired before execution', updated_at=?"
                " WHERE state IN (?,?)",
                (JobState.EXPIRED, now, JobState.QUEUED, JobState.RETRYING),
            )
        return int(cursor.rowcount)

    def reap_expired_leases(self, now: float | None = None) -> int:
        moment = self._now() if now is None else float(now)
        with self._lock, self._tx():
            return self._requeue_expired_locked(moment)

    def reap_expired_jobs(self, now: float | None = None) -> int:
        moment = self._now() if now is None else float(now)
        with self._lock, self._tx():
            return self._expire_overdue_locked(moment)

    # ------------------------------------------------------------------ DLQ
    def dead_letters(self, *, queue: str | None = None) -> list[DeadLetter]:
        sql = f"SELECT {_MESSAGE_COLUMNS} FROM messages WHERE state='dead'"
        params: tuple[Any, ...] = ()
        if queue is not None:
            sql += " AND queue=?"
            params = (queue,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [
            DeadLetter(
                message_id=int(row["id"]),
                job_id=str(row["job_id"]),
                queue=str(row["queue"]),
                task=str(self._decode(row["envelope"]).task),
                envelope=self._decode(row["envelope"]),
                reason=str(row["dead_reason"] or ""),
                deliveries=int(row["deliveries"]),
                failed_at=float(row["claimed_at"] or 0.0),
            )
            for row in rows
        ]

    def replay_dead(
        self, message_id: int, *, queue: str | None = None, priority: int | None = None
    ) -> bool:
        now = self._now()
        with self._lock, self._tx():
            row = self._message(message_id)
            if row is None or row["state"] != MessageState.DEAD:
                return False
            target_queue = queue or str(row["queue"])
            target_priority = int(row["priority"]) if priority is None else int(priority)
            envelope = dataclasses.replace(
                self._decode(row["envelope"]).with_routing(target_queue, target_priority), attempt=1
            )
            self._conn.execute(
                "UPDATE messages SET state='queued', queue=?, priority=?, envelope=?, visible_at=?,"
                " deliveries=0, yields=0, yieldable=1, dead_reason=NULL, last_error=NULL,"
                " claimed_by=NULL, claimed_at=NULL, lease_until=NULL WHERE id=?",
                (target_queue, target_priority, self._encode(envelope), now, message_id),
            )
            self._conn.execute(
                "UPDATE jobs SET state=?, error=NULL, updated_at=? WHERE job_id=?",
                (JobState.QUEUED, now, row["job_id"]),
            )
            return True

    # ------------------------------------------------------------------ 关闭
    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                self._conn.close()
