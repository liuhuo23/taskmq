"""PostgreSQL transport：多机 + 强一致（Phase 2）。

为什么用 Postgres（以及和 sqlite/redis 的分工）：

- `SELECT … FOR UPDATE SKIP LOCKED` 是 Postgres 队列的经典原语：**多 worker 并发 claim 不互相阻塞、
  不重复投递**，不需要把整个队列串行化（sqlite 的 `BEGIN IMMEDIATE` 只能单机）；
- 命名租约用 `INSERT … ON CONFLICT DO UPDATE … WHERE` 直接做 **CAS**，比"WATCH 重试"干净；
- 事务 + 唯一约束让幂等键/job 状态天然跨机一致；
- 代价：需要一个 Postgres 服务（`pip install taskmq-py[postgres]` + `postgresql://…`），
  小规模单机仍然建议 sqlite。

语义与其它 transport **完全一致**（同一套 `transport_conformance` 场景，见 docs/design/plugins.md §6）。
隔离：URL 用 `?prefix=app1_` 指定表前缀（多环境共用一个库时互不干扰）。
"""
from __future__ import annotations

import contextlib
import dataclasses
import json
import threading
import time
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from ..errors import ConfigError, DecodeError, LeaseLost, MessageNotFound, TransportError
from ..protocol import Codec, CodecRegistry, Envelope, JSONCodec, decode_value, encode_value
from .base import (
    UNSET,
    DeadLetter,
    Delivery,
    JobRecord,
    JobState,
    MessageState,
    QueueStat,
    Transport,
    WorkerInfo,
)

__all__ = ["PostgresTransport"]

_DRIVER_HINT = "PostgreSQL transport 需要 psycopg：pip install 'taskmq-py[postgres]'"


def _import_driver() -> Any:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover - 取决于环境
        raise ConfigError(_DRIVER_HINT) from exc
    return psycopg, dict_row


class PostgresTransport(Transport):
    """`postgresql://user:pass@host:port/db?prefix=taskmq_&sslmode=require`。"""

    supports_leases = True
    supports_workers = True
    supports_job_listing = True
    limitations = {
        "queue_weights": "同优先级档位内暂无加权轮询（全局严格优先级 + FIFO，Phase 2 后续）",
    }

    def __init__(
        self,
        url: str,
        *,
        codec: Codec | None = None,
        registry: CodecRegistry | None = None,
        prefix: str = "taskmq_",
        clock: Any | None = None,
        connect_timeout: float = 10.0,
        idempotency_ttl: float = 86400.0,
        max_message_bytes: int | None = None,
    ) -> None:
        self._psycopg, dict_row = _import_driver()
        self._url = url
        self._prefix = prefix
        self._codec: Codec = codec if codec is not None else JSONCodec()
        self._registry = registry if registry is not None else CodecRegistry()
        self._clock = clock or time.time
        self._timeout = float(connect_timeout)
        self._idempotency_ttl = float(idempotency_ttl)
        self._max_message_bytes = max_message_bytes
        self._dict_row = dict_row
        self._lock = threading.RLock()
        self._local = threading.local()
        self._connections: list[Any] = []
        self._schema_ready = False
        self._probe()                      # 连不上/建表失败 → 构造期就报错（fail fast）

    # ------------------------------------------------------------------ 连接
    @property
    def _t(self) -> str:
        """表前缀（`taskmq_`）。"""
        return self._prefix

    def _probe(self) -> None:
        try:
            with self._conn().cursor() as cur:
                cur.execute("SELECT 1")
        except ConfigError:
            raise
        except Exception as exc:
            raise TransportError(f"连不上 PostgreSQL（{self._url.split('@')[-1]}）：{exc}") from exc

    def _conn(self) -> Any:
        conn = getattr(self._local, "conn", None)
        if conn is not None and not conn.closed:
            return conn
        try:
            conn = self._psycopg.connect(
                self._url,
                connect_timeout=self._timeout,
                autocommit=True,
                row_factory=self._dict_row,
            )
        except Exception as exc:
            raise TransportError(f"连接 PostgreSQL 失败：{exc}") from exc
        self._local.conn = conn
        with self._lock:
            self._connections.append(conn)
            ready = self._schema_ready
        if not ready:
            self._create_schema(conn)
        return conn

    def _create_schema(self, conn: Any) -> None:
        t = self._t
        ddl = [
            f"""CREATE TABLE IF NOT EXISTS {t}messages (
                message_id BIGSERIAL PRIMARY KEY,
                job_id     TEXT NOT NULL,
                queue      TEXT NOT NULL,
                envelope   BYTEA NOT NULL,
                priority   INTEGER NOT NULL DEFAULT 0,
                state      TEXT NOT NULL DEFAULT 'queued',
                visible_at DOUBLE PRECISION NOT NULL DEFAULT 0,
                expires_at DOUBLE PRECISION,
                claimed_by TEXT NOT NULL DEFAULT '',
                claimed_at DOUBLE PRECISION NOT NULL DEFAULT 0,
                lease_until DOUBLE PRECISION NOT NULL DEFAULT 0,
                deliveries INTEGER NOT NULL DEFAULT 0,
                yields     INTEGER NOT NULL DEFAULT 0,
                yieldable  BOOLEAN NOT NULL DEFAULT TRUE,
                dead_reason TEXT NOT NULL DEFAULT '',
                last_error  TEXT NOT NULL DEFAULT ''
            )""",
            f"""CREATE INDEX IF NOT EXISTS {t}idx_msg_claim
                ON {t}messages (queue, state, visible_at, priority DESC, message_id)""",
            f"""CREATE INDEX IF NOT EXISTS {t}idx_msg_lease
                ON {t}messages (state, lease_until)""",
            f"""CREATE INDEX IF NOT EXISTS {t}idx_msg_job ON {t}messages (job_id)""",
            f"""CREATE TABLE IF NOT EXISTS {t}jobs (
                job_id     TEXT PRIMARY KEY,
                task       TEXT NOT NULL DEFAULT '',
                state      TEXT NOT NULL,
                attempt    INTEGER NOT NULL DEFAULT 0,
                result     TEXT,
                has_result BOOLEAN NOT NULL DEFAULT FALSE,
                error      TEXT,
                meta       TEXT NOT NULL DEFAULT '{{}}',
                created_at DOUBLE PRECISION NOT NULL,
                updated_at DOUBLE PRECISION NOT NULL
            )""",
            f"""CREATE INDEX IF NOT EXISTS {t}idx_jobs_state ON {t}jobs (state, updated_at DESC)""",
            f"""CREATE TABLE IF NOT EXISTS {t}idempotency (
                key    TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                until  DOUBLE PRECISION NOT NULL
            )""",
            f"""CREATE TABLE IF NOT EXISTS {t}leases (
                name       TEXT PRIMARY KEY,
                owner      TEXT NOT NULL,
                expires_at DOUBLE PRECISION NOT NULL
            )""",
            f"""CREATE TABLE IF NOT EXISTS {t}workers (
                id           TEXT PRIMARY KEY,
                queues       TEXT NOT NULL DEFAULT '',
                pool         TEXT NOT NULL DEFAULT '',
                concurrency  INTEGER NOT NULL DEFAULT 0,
                started_at   DOUBLE PRECISION NOT NULL,
                heartbeat_at DOUBLE PRECISION NOT NULL,
                meta         TEXT NOT NULL DEFAULT '{{}}'
            )""",
        ]
        with conn.transaction(), conn.cursor() as cur:
            for statement in ddl:
                cur.execute(statement)
        with self._lock:
            self._schema_ready = True

    def _drop_schema(self) -> None:
        """删掉本前缀的表（测试清理用；只碰自己的前缀）。"""
        tables = ("messages", "jobs", "idempotency", "leases", "workers")
        with self._conn().transaction(), self._conn().cursor() as cur:
            for table in tables:
                cur.execute(f"DROP TABLE IF EXISTS {self._t}{table}")

    def close(self) -> None:
        with self._lock:
            connections, self._connections = self._connections, []
        for conn in connections:
            with contextlib.suppress(Exception):
                conn.close()
        self._local = threading.local()

    # ------------------------------------------------------------------ 工具
    def _now(self) -> float:
        return float(self._clock())

    def _encode(self, env: Envelope) -> bytes:
        return self._codec.encode(env, max_bytes=self._max_message_bytes)

    @staticmethod
    def _load_json(raw: Any, registry: CodecRegistry, path: str) -> Any:
        if raw is None or raw == "":
            return {}
        try:
            return decode_value(json.loads(raw), registry, path=path)
        except ValueError as exc:  # pragma: no cover - 数据损坏
            raise DecodeError(f"{path} 不是合法 JSON：{exc}") from exc

    def _row_to_record(self, row: Mapping[str, Any]) -> JobRecord:
        return JobRecord(
            job_id=str(row["job_id"]),
            task=str(row["task"]),
            state=str(row["state"]),
            attempt=int(row["attempt"]),
            result=(
                self._load_json(row["result"], self._registry, "jobs.result")
                if row["has_result"] and row["result"] is not None
                else None
            ),
            has_result=bool(row["has_result"]),
            error=row["error"],
            meta=self._load_json(row["meta"], self._registry, "jobs.meta") or {},
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
        )

    def _delivery(self, row: Mapping[str, Any], worker_id: str) -> Delivery:
        envelope = self._codec.decode(bytes(row["envelope"]))
        return Delivery(
            job_id=str(row["job_id"]),
            message_id=int(row["message_id"]),
            queue=str(row["queue"]),
            envelope=envelope,
            worker_id=worker_id,
            attempt=envelope.attempt,
            deliveries=int(row["deliveries"]),
            lease_until=float(row["lease_until"]),
            reserved_at=float(row["claimed_at"]),
            priority=int(row["priority"]),
            yields=int(row["yields"]),
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
        now = self._now()
        target = queue or env.queue or "default"
        prio = env.priority if priority is None else int(priority)
        effective = env if (env.queue == target and env.priority == prio) else env.with_routing(target, prio)
        blob = self._encode(effective)
        visible_at = now + max(0.0, float(delay))
        if env.eta is not None:
            visible_at = max(visible_at, float(env.eta))
        expires_at = None if env.expires_at is None else float(env.expires_at)

        conn = self._conn()
        with conn.transaction(), conn.cursor() as cur:
            if env.key:
                cur.execute(
                    f"""INSERT INTO {self._t}idempotency (key, job_id, until)
                            VALUES (%s, %s, %s) ON CONFLICT (key) DO NOTHING RETURNING job_id""",
                    (env.key, env.id, now + self._idempotency_ttl),
                )
                row = cur.fetchone()
                if row is None:                       # 已有同 key → 返回已存在 job
                    cur.execute(
                        f"SELECT job_id FROM {self._t}idempotency WHERE key = %s", (env.key,)
                    )
                    existing = cur.fetchone()
                    if existing is not None:
                        return str(existing["job_id"])
            cur.execute(
                f"""INSERT INTO {self._t}messages
                        (job_id, queue, envelope, priority, state, visible_at, expires_at)
                        VALUES (%s, %s, %s, %s, 'queued', %s, %s)""",
                (env.id, target, blob, prio, visible_at, expires_at),
            )
            meta = json.dumps({"queue": target, "priority": prio}, separators=(",", ":"))
            cur.execute(
                f"""INSERT INTO {self._t}jobs
                        (job_id, task, state, attempt, meta, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (job_id) DO UPDATE
                        SET task = EXCLUDED.task, state = EXCLUDED.state,
                            attempt = EXCLUDED.attempt, updated_at = EXCLUDED.updated_at""",
                (env.id, env.task, JobState.QUEUED, env.attempt, meta, now, now),
            )
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
        t = self._t
        conn = self._conn()
        with conn.transaction(), conn.cursor() as cur:
            # 过期：先标记消息，再把对应 job 置 EXPIRED（同一事务内）
            cur.execute(
                f"""UPDATE {t}messages SET state = 'expired', dead_reason = 'expired'
                        WHERE state = 'queued' AND queue = ANY(%s)
                          AND expires_at IS NOT NULL AND expires_at <= %s
                        RETURNING job_id""",
                (wanted, now),
            )
            expired_jobs = [str(row["job_id"]) for row in cur.fetchall()]
            if expired_jobs:
                cur.execute(
                    f"""UPDATE {t}jobs SET state = %s, error = 'expired before execution',
                              updated_at = %s WHERE job_id = ANY(%s)""",
                    (JobState.EXPIRED, now, expired_jobs),
                )
            # 原子 claim：FOR UPDATE SKIP LOCKED → 多 worker 并发不阻塞、不重复
            cur.execute(
                f"""WITH candidate AS (
                            SELECT message_id FROM {t}messages
                            WHERE queue = ANY(%s) AND state = 'queued' AND visible_at <= %s
                            ORDER BY priority DESC, message_id
                            LIMIT %s
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE {t}messages m
                        SET state = 'reserved', claimed_by = %s, claimed_at = %s,
                            lease_until = %s, deliveries = m.deliveries + 1
                        FROM candidate c
                        WHERE m.message_id = c.message_id
                        RETURNING m.*""",
                (wanted, now, limit, worker_id, now, now + lease),
            )
            rows = cur.fetchall()
        return [self._delivery(row, worker_id) for row in rows]

    def _owned(self, cur: Any, delivery: Delivery) -> Mapping[str, Any] | None:
        cur.execute(
            f"SELECT * FROM {self._t}messages WHERE message_id = %s FOR UPDATE",
            (delivery.message_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise MessageNotFound(f"消息 {delivery.message_id} 不存在")
        if row["state"] == MessageState.RESERVED and row["claimed_by"] == delivery.worker_id:
            return row
        return None

    def ack(self, delivery: Delivery) -> None:
        conn = self._conn()
        with conn.transaction(), conn.cursor() as cur:
            row = self._owned(cur, delivery)
            if row is None:
                if self._is_acked(delivery):
                    return                              # 幂等
                raise LeaseLost(f"消息 {delivery.message_id} 已被回收，ack 被拒绝")
            cur.execute(
                f"""UPDATE {self._t}messages SET state = 'acked', claimed_by = '',
                        claimed_at = 0, lease_until = 0 WHERE message_id = %s""",
                (delivery.message_id,),
            )

    def _is_acked(self, delivery: Delivery) -> bool:
        with self._conn().cursor() as cur:
            cur.execute(
                f"SELECT state FROM {self._t}messages WHERE message_id = %s",
                (delivery.message_id,),
            )
            row = cur.fetchone()
        return row is not None and row["state"] == MessageState.ACKED

    def nack(self, delivery: Delivery, *, requeue: bool = True, delay: float = 0.0) -> None:
        if not requeue:
            conn = self._conn()
            with conn.transaction(), conn.cursor() as cur:
                row = self._owned(cur, delivery)
                if row is None:
                    if self._is_dead(delivery):
                        return
                    raise LeaseLost(f"消息 {delivery.message_id} 已被回收")
                self._mark_dead(cur, delivery, "nacked without requeue")
            return
        self._requeue(delivery, delay=delay, consume_delivery=False)

    def defer(self, delivery: Delivery, *, delay: float = 0.0) -> None:
        """放回队列且**不消耗投递次数**（限流等待 / concurrency_key 抢不到锁）。"""
        self._requeue(delivery, delay=delay, consume_delivery=True)

    def _requeue(self, delivery: Delivery, *, delay: float, consume_delivery: bool) -> None:
        now = self._now()
        conn = self._conn()
        with conn.transaction(), conn.cursor() as cur:
            row = self._owned(cur, delivery)
            if row is None:
                raise LeaseLost(f"消息 {delivery.message_id} 已被回收")
            deliveries = int(row["deliveries"])
            if consume_delivery:
                deliveries = max(0, deliveries - 1)
            cur.execute(
                f"""UPDATE {self._t}messages SET state = 'queued', visible_at = %s,
                        deliveries = %s, claimed_by = '', claimed_at = 0, lease_until = 0
                        WHERE message_id = %s""",
                (now + max(0.0, float(delay)), deliveries, delivery.message_id),
            )

    def _mark_dead(self, cur: Any, delivery: Delivery, reason: str) -> None:
        cur.execute(
            f"""UPDATE {self._t}messages SET state = 'dead', dead_reason = %s, last_error = %s,
                claimed_by = '', claimed_at = 0, lease_until = 0 WHERE message_id = %s""",
            (reason, reason, delivery.message_id),
        )

    def _is_dead(self, delivery: Delivery) -> bool:
        with self._conn().cursor() as cur:
            cur.execute(
                f"SELECT state FROM {self._t}messages WHERE message_id = %s",
                (delivery.message_id,),
            )
            row = cur.fetchone()
        return row is not None and row["state"] == "dead"

    def dead_letter(self, delivery: Delivery, reason: str) -> None:
        conn = self._conn()
        with conn.transaction(), conn.cursor() as cur:
            row = self._owned(cur, delivery)
            if row is None:
                if self._is_dead(delivery):
                    return
                raise LeaseLost(f"消息 {delivery.message_id} 已被回收")
            self._mark_dead(cur, delivery, reason)

    def extend_lease(self, delivery: Delivery, seconds: float) -> None:
        conn = self._conn()
        with conn.transaction(), conn.cursor() as cur:
            row = self._owned(cur, delivery)
            if row is None:
                raise LeaseLost(f"消息 {delivery.message_id} 已被回收")
            cur.execute(
                f"UPDATE {self._t}messages SET lease_until = %s WHERE message_id = %s",
                (self._now() + float(seconds), delivery.message_id),
            )

    def yield_reservation(
        self, delivery: Delivery, *, delay: float = 0.0, max_yields: int = 100
    ) -> bool:
        now = self._now()
        conn = self._conn()
        with conn.transaction(), conn.cursor() as cur:
            row = self._owned(cur, delivery)
            if row is None:
                raise LeaseLost(f"消息 {delivery.message_id} 已被回收")
            if not row["yieldable"]:
                return False
            yields = int(row["yields"]) + 1
            cur.execute(
                f"""UPDATE {self._t}messages SET state = 'queued', visible_at = %s,
                        yields = %s, yieldable = %s, claimed_by = '', claimed_at = 0,
                        lease_until = 0 WHERE message_id = %s""",
                (now + max(0.0, float(delay)), yields, yields < max_yields, delivery.message_id),
            )
        return True

    # ------------------------------------------------------------------ 插队
    def peek_max_priority(self, queues: Sequence[str], *, now: float | None = None) -> int | None:
        moment = self._now() if now is None else float(now)
        with self._conn().cursor() as cur:
            cur.execute(
                f"""SELECT MAX(priority) AS top FROM {self._t}messages
                    WHERE queue = ANY(%s) AND state = 'queued' AND visible_at <= %s""",
                (list(dict.fromkeys(queues)), moment),
            )
            row = cur.fetchone()
        return None if row is None or row["top"] is None else int(row["top"])

    def next_visible_at(self, queues: Sequence[str], *, now: float | None = None) -> float | None:
        with self._conn().cursor() as cur:
            cur.execute(
                f"""SELECT MIN(visible_at) AS at FROM {self._t}messages
                    WHERE queue = ANY(%s) AND state = 'queued'""",
                (list(dict.fromkeys(queues)),),
            )
            row = cur.fetchone()
        return None if row is None or row["at"] is None else float(row["at"])

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
        conn = self._conn()
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {self._t}jobs WHERE job_id = %s FOR UPDATE", (job_id,)
            )
            current = cur.fetchone()
            merged = self._load_json(current["meta"], self._registry, "jobs.meta") if current else {}
            if not isinstance(merged, dict):
                merged = {}
            merged.update(meta)
            meta_json = json.dumps(
                encode_value(merged, self._registry, path="jobs.meta"), separators=(",", ":")
            )
            current_task = task or (str(current["task"]) if current else "")
            current_attempt = (
                int(attempt)
                if attempt is not None
                else (int(current["attempt"]) if current else 0)
            )
            result_json: Any
            if result is not UNSET:
                result_json = json.dumps(
                    encode_value(result, self._registry, path="jobs.result"),
                    separators=(",", ":"),
                )
                has_result = True
            else:
                result_json = current["result"] if current else None
                has_result = bool(current["has_result"]) if current else False
            current_error: str | None = (
                (None if error is None else str(error))
                if error is not UNSET
                else (current["error"] if current else None)
            )
            created = float(current["created_at"]) if current else now
            cur.execute(
                f"""INSERT INTO {self._t}jobs
                        (job_id, task, state, attempt, result, has_result, error, meta,
                         created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (job_id) DO UPDATE SET
                            task = EXCLUDED.task, state = EXCLUDED.state,
                            attempt = EXCLUDED.attempt, result = EXCLUDED.result,
                            has_result = EXCLUDED.has_result, error = EXCLUDED.error,
                            meta = EXCLUDED.meta, updated_at = EXCLUDED.updated_at""",
                (
                    job_id,
                    current_task,
                    state,
                    current_attempt,
                    result_json,
                    has_result,
                    current_error,
                    meta_json,
                    created,
                    now,
                ),
            )
        record = self.get_state(job_id)
        assert record is not None
        return record

    def get_state(self, job_id: str) -> JobRecord | None:
        with self._conn().cursor() as cur:
            cur.execute(f"SELECT * FROM {self._t}jobs WHERE job_id = %s", (job_id,))
            row = cur.fetchone()
        return None if row is None else self._row_to_record(row)

    def list_jobs(
        self,
        *,
        prefix: str | None = None,
        states: Sequence[str] | None = None,
        limit: int = 100,
    ) -> list[JobRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        if prefix:
            clauses.append("job_id LIKE %s")
            params.append(f"{prefix}%")
        if states:
            clauses.append("state = ANY(%s)")
            params.append(list(states))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(0, int(limit)))
        with self._conn().cursor() as cur:
            cur.execute(
                f"SELECT * FROM {self._t}jobs{where} ORDER BY updated_at DESC LIMIT %s", params
            )
            rows = cur.fetchall()
        return [self._row_to_record(row) for row in rows]

    def queue_stats(self, queues: Sequence[str] | None = None) -> list[QueueStat]:
        targets = list(dict.fromkeys(queues)) if queues else self._known_queues()
        stats = []
        with self._conn().cursor() as cur:
            for queue in targets:
                cur.execute(
                    f"""SELECT state, COUNT(*) AS total FROM {self._t}messages
                        WHERE queue = %s GROUP BY state""",
                    (queue,),
                )
                counts = {str(row["state"]): int(row["total"]) for row in cur.fetchall()}
                stats.append(
                    QueueStat(
                        queue=queue,
                        pending=counts.get("queued", 0),
                        inflight=counts.get("reserved", 0),
                        dead=counts.get("dead", 0),
                    )
                )
        return stats

    def priority_stats(self, queues: Sequence[str] | None = None) -> dict[int, int]:
        targets = list(dict.fromkeys(queues)) if queues else self._known_queues()
        with self._conn().cursor() as cur:
            cur.execute(
                f"""SELECT priority, COUNT(*) AS total FROM {self._t}messages
                    WHERE state = 'queued' AND queue = ANY(%s)
                    GROUP BY priority ORDER BY priority""",
                (targets,),
            )
            rows = cur.fetchall()
        return {int(row["priority"]): int(row["total"]) for row in rows}

    def _known_queues(self) -> list[str]:
        with self._conn().cursor() as cur:
            cur.execute(f"SELECT DISTINCT queue FROM {self._t}messages ORDER BY queue")
            return [str(row["queue"]) for row in cur.fetchall()]

    # ------------------------------------------------------------------ 回收
    def reap_expired_leases(self, now: float | None = None) -> int:
        moment = self._now() if now is None else float(now)
        conn = self._conn()
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                f"""UPDATE {self._t}messages SET state = 'queued', visible_at = %s,
                        claimed_by = '', claimed_at = 0, lease_until = 0
                        WHERE state = 'reserved' AND lease_until <= %s""",
                (moment, moment),
            )
            return cur.rowcount

    def reap_expired_jobs(self, now: float | None = None) -> int:
        moment = self._now() if now is None else float(now)
        conn = self._conn()
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                f"""UPDATE {self._t}messages SET state = 'expired', dead_reason = 'expired'
                        WHERE state = 'queued' AND expires_at IS NOT NULL AND expires_at <= %s
                        RETURNING job_id""",
                (moment,),
            )
            job_ids = [str(row["job_id"]) for row in cur.fetchall()]
            if job_ids:
                cur.execute(
                    f"""UPDATE {self._t}jobs SET state = %s,
                            error = 'expired before execution', updated_at = %s
                            WHERE job_id = ANY(%s)""",
                    (JobState.EXPIRED, moment, job_ids),
                )
        return len(job_ids)

    # ------------------------------------------------------------------ DLQ
    def dead_letters(self, *, queue: str | None = None) -> list[DeadLetter]:
        clauses = ["state = 'dead'"]
        params: list[Any] = []
        if queue:
            clauses.append("queue = %s")
            params.append(queue)
        with self._conn().cursor() as cur:
            cur.execute(
                f"SELECT * FROM {self._t}messages WHERE {' AND '.join(clauses)}"
                " ORDER BY message_id",
                params,
            )
            rows = cur.fetchall()
        entries = []
        for row in rows:
            envelope = self._codec.decode(bytes(row["envelope"]))
            entries.append(
                DeadLetter(
                    message_id=int(row["message_id"]),
                    job_id=str(row["job_id"]),
                    queue=str(row["queue"]),
                    task=envelope.task,
                    envelope=envelope,
                    reason=str(row["dead_reason"]),
                    deliveries=int(row["deliveries"]),
                    failed_at=float(row["claimed_at"]),
                )
            )
        return entries

    def replay_dead(
        self, message_id: int, *, queue: str | None = None, priority: int | None = None
    ) -> bool:
        now = self._now()
        conn = self._conn()
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {self._t}messages WHERE message_id = %s FOR UPDATE",
                (message_id,),
            )
            row = cur.fetchone()
            if row is None or row["state"] != "dead":
                return False
            envelope = self._codec.decode(bytes(row["envelope"]))
            target_queue = queue or str(row["queue"])
            target_priority = int(row["priority"]) if priority is None else int(priority)
            updated = dataclasses.replace(
                envelope.with_routing(target_queue, target_priority), attempt=1
            )
            cur.execute(
                f"""UPDATE {self._t}messages SET state = 'queued', queue = %s, priority = %s,
                        envelope = %s, visible_at = %s, deliveries = 0, yields = 0,
                        yieldable = TRUE, dead_reason = '', last_error = '',
                        claimed_by = '', claimed_at = 0, lease_until = 0
                        WHERE message_id = %s""",
                (target_queue, target_priority, self._encode(updated), now, message_id),
            )
            cur.execute(
                f"""UPDATE {self._t}jobs SET state = %s, error = NULL, updated_at = %s
                        WHERE job_id = %s""",
                (JobState.QUEUED, now, str(row["job_id"])),
            )
        return True

    # ------------------------------------------------------------------ 租约
    def acquire_lease(self, name: str, owner: str, ttl: float) -> bool:
        now = self._now()
        expires = now + float(ttl)
        with self._conn().cursor() as cur:
            # 一行搞定 CAS：过期或本来就是自己 → 抢占/续期
            cur.execute(
                f"""INSERT INTO {self._t}leases (name, owner, expires_at) VALUES (%s, %s, %s)
                    ON CONFLICT (name) DO UPDATE SET owner = EXCLUDED.owner,
                        expires_at = EXCLUDED.expires_at
                    WHERE {self._t}leases.expires_at <= %s OR {self._t}leases.owner = EXCLUDED.owner
                    RETURNING owner""",
                (name, owner, expires, now),
            )
            row = cur.fetchone()
        return row is not None and str(row["owner"]) == owner

    def release_lease(self, name: str, owner: str) -> None:
        with self._conn().cursor() as cur:
            cur.execute(
                f"DELETE FROM {self._t}leases WHERE name = %s AND owner = %s", (name, owner)
            )

    def renew_lease(self, name: str, owner: str, ttl: float) -> bool:
        now = self._now()
        with self._conn().cursor() as cur:
            cur.execute(
                f"""UPDATE {self._t}leases SET expires_at = %s
                    WHERE name = %s AND owner = %s AND expires_at > %s RETURNING owner""",
                (now + float(ttl), name, owner, now),
            )
            return cur.fetchone() is not None

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
        payload = json.dumps({"queues": list(queues), **(meta or {})}, separators=(",", ":"))
        with self._conn().cursor() as cur:
            cur.execute(
                f"""INSERT INTO {self._t}workers
                    (id, queues, pool, concurrency, started_at, heartbeat_at, meta)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET queues = EXCLUDED.queues,
                        pool = EXCLUDED.pool, concurrency = EXCLUDED.concurrency,
                        heartbeat_at = EXCLUDED.heartbeat_at""",
                (worker_id, ",".join(queues), pool, int(concurrency), moment, moment, payload),
            )

    def heartbeat_worker(
        self,
        worker_id: str,
        *,
        meta: Mapping[str, Any] | None = None,
        now: float | None = None,
    ) -> None:
        moment = self._now() if now is None else float(now)
        with self._conn().cursor() as cur:
            cur.execute(
                f"UPDATE {self._t}workers SET heartbeat_at = %s WHERE id = %s",
                (moment, worker_id),
            )
            if cur.rowcount == 0:
                self.register_worker(worker_id, meta=meta, now=moment)
                return
            if meta:
                cur.execute(f"SELECT meta FROM {self._t}workers WHERE id = %s", (worker_id,))
                row = cur.fetchone()
                merged = self._load_json(row["meta"], self._registry, "workers.meta") if row else {}
                if not isinstance(merged, dict):
                    merged = {}
                merged.update(meta)
                cur.execute(
                    f"UPDATE {self._t}workers SET meta = %s WHERE id = %s",
                    (json.dumps(merged, separators=(",", ":")), worker_id),
                )

    def deregister_worker(self, worker_id: str) -> None:
        with self._conn().cursor() as cur:
            cur.execute(f"DELETE FROM {self._t}workers WHERE id = %s", (worker_id,))

    def list_workers(
        self, *, stale_after: float = 60.0, now: float | None = None
    ) -> list[WorkerInfo]:
        with self._conn().cursor() as cur:
            cur.execute(f"SELECT * FROM {self._t}workers ORDER BY heartbeat_at DESC, id")
            rows = cur.fetchall()
        workers = []
        for row in rows:
            meta = self._load_json(row["meta"], self._registry, "workers.meta")
            workers.append(
                WorkerInfo(
                    worker_id=str(row["id"]),
                    queues=tuple(filter(None, str(row["queues"]).split(","))),
                    pool=str(row["pool"]),
                    concurrency=int(row["concurrency"]),
                    started_at=float(row["started_at"]),
                    heartbeat_at=float(row["heartbeat_at"]),
                    meta=meta if isinstance(meta, dict) else {},
                )
            )
        return workers


def parse_postgres_url(url: str) -> tuple[str, str, dict[str, str]]:
    """拆出 `(libpq_url, table_prefix, extra_params)`；`prefix` 是 taskmq 自己的参数。"""
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    prefix = (query.pop("prefix", ["taskmq_"])[0]) or "taskmq_"
    sslmode = query.pop("sslmode", [None])[0]
    timeout = query.pop("connect_timeout", [None])[0]
    params: dict[str, str] = {}
    if sslmode:
        params["sslmode"] = unquote(sslmode)
    if timeout:
        params["connect_timeout"] = unquote(timeout)
    libpq = parsed._replace(query="").geturl()
    return libpq, prefix, params
