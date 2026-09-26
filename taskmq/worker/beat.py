"""beat：定时调度器（docs/design.md §13）。

- **lease 选主**：所有 beat 副本抢 transport 的 `__beat__` 命名租约，抢不到就待命；
  多副本部署不会重复触发（Celery beat 的经典坑）。transport 不支持命名租约时降级为单副本并告警。
- **misfire**：`skip`（默认，错过就跳过）/ `run_once`（补一次）。
- **状态**：每条 schedule 的「上次观测时间」落在 JSON 文件（`--state`，默认 `taskmq.beat.json`）；
  只有 leader 写，所以多副本共享同一文件也没问题（同机/共享盘）。
- **首次部署不补跑**：第一次见到某条 schedule 只记录基准时间，一次 interval/cron 槽位之后才触发，
  避免刚上线就刷一堆任务。
"""
from __future__ import annotations

import dataclasses
import json
import logging
import os
import secrets
import socket
import threading
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ..errors import TransportError
from ..schedule import Schedule

logger = logging.getLogger("taskmq.beat")

DEFAULT_STATE_FILE = "taskmq.beat.json"


@dataclasses.dataclass
class Beat:
    """定时调度器。`tick()` 是纯函数式的「推进一轮」，方便确定性测试。"""

    app: Any
    schedules: Sequence[Schedule]
    state_path: str | os.PathLike[str] | None = DEFAULT_STATE_FILE
    lease_name: str = "__beat__"
    lease_ttl: float = 30.0
    owner: str = ""
    poll_interval: float = 1.0

    def __post_init__(self) -> None:
        self.schedules = tuple(self.schedules)
        self.owner = self.owner or f"beat-{socket.gethostname()}-{os.getpid()}-{secrets.token_hex(3)}"
        self.transport = self.app.transport
        self.is_leader = False
        self.fired = 0
        self.skipped = 0
        self.standby = 0
        self._state: dict[str, float] = {}
        self._lock = threading.RLock()
        self._stopping = threading.Event()
        self._warned_no_lease = False
        self._load_state()
        self._validate_tasks()

    # ------------------------------------------------------------------ 状态
    def _state_file(self) -> Path | None:
        return None if self.state_path is None else Path(self.state_path)

    def _load_state(self) -> None:
        path = self._state_file()
        if path is None or not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            entries = payload.get("entries", {})
            self._state = {str(name): float(value) for name, value in entries.items()}
        except (ValueError, OSError) as exc:  # 状态文件坏了不该拖垮 beat
            logger.warning("beat 状态文件读取失败（忽略）：%s", exc)

    def _save_state(self) -> None:
        path = self._state_file()
        if path is None:
            return
        payload = {"entries": self._state, "updated_at": time.time(), "owner": self.owner}
        tmp = path.with_suffix(path.suffix + ".tmp")
        try:
            tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            tmp.replace(path)
        except OSError as exc:  # pragma: no cover - 磁盘问题
            logger.warning("beat 状态文件写入失败：%s", exc)

    def _validate_tasks(self) -> None:
        missing = [entry.task for entry in self.schedules if self.app.task_for(entry.task) is None]
        if missing:
            raise ValueError(f"beat 里引用了未注册的任务：{missing}")

    # ------------------------------------------------------------------ 选主
    def _ensure_leadership(self, now: float) -> bool:
        if not getattr(self.transport, "supports_leases", False):
            if not self._warned_no_lease:
                self._warned_no_lease = True
                logger.warning(
                    "transport %s 不支持命名租约：beat 以单副本模式运行",
                    type(self.transport).__name__,
                )
            self.is_leader = True
            return True
        try:
            self.is_leader = bool(
                self.transport.acquire_lease(self.lease_name, self.owner, self.lease_ttl)
            )
        except TransportError as exc:  # pragma: no cover - 能力位与实现不一致
            logger.warning("beat 选主失败（按单副本继续）：%s", exc)
            self.is_leader = True
        return self.is_leader

    # ------------------------------------------------------------------ 主循环
    def tick(self, now: float | None = None) -> list[str]:
        """推进一轮；返回本轮触发的 schedule 名字。"""
        moment = time.time() if now is None else float(now)
        with self._lock:
            if not self._ensure_leadership(moment):
                self.standby += 1
                self.app.emit("beat.standby", owner=self.owner)
                return []

            fired: list[str] = []
            changed = False
            for entry in self.schedules:
                name = entry.entry_name()
                last = self._state.get(name)
                if last is None:
                    # 首次见到：只记录基准，不立刻补跑
                    self._state[name] = moment
                    changed = True
                    continue
                due = entry.next_fire(last)
                if due > moment:
                    continue
                missed_more = entry.next_fire(due) <= moment
                if missed_more and entry.misfire == "skip":
                    self._state[name] = moment
                    self.skipped += 1
                    changed = True
                    self.app.emit(
                        "beat.misfire_skipped", entry=name, task=entry.task, misfire=entry.misfire
                    )
                    continue
                self._fire(entry, name)
                self._state[name] = moment
                fired.append(name)
                changed = True
            if changed:
                self._save_state()
            return fired

    def _fire(self, entry: Schedule, name: str) -> None:
        try:
            handle = self.app.submit(
                entry.task,
                entry.args,
                entry.kwargs,
                queue=entry.queue,
                priority=entry.priority,
            )
        except Exception as exc:  # 单条调度失败不该打死 beat
            logger.exception("beat 触发失败：%s", name)
            self.app.emit("beat.error", entry=name, task=entry.task, error=str(exc))
            return
        self.fired += 1
        self.app.emit("beat.fired", entry=name, task=entry.task, job_id=handle.id)

    def run_forever(self, *, poll: float | None = None) -> None:
        interval = float(poll if poll is not None else self.poll_interval)
        while not self._stopping.is_set():
            self.tick()
            self._stopping.wait(max(0.01, interval))

    def stop(self) -> None:
        self._stopping.set()

    def close(self) -> None:
        """优雅退出：停 tick、存状态、**主动释放选主租约**（follower 可立刻接手）。"""
        self.stop()
        self._save_state()
        if self.is_leader and getattr(self.transport, "supports_leases", False):
            try:
                self.transport.release_lease(self.lease_name, self.owner)
            except TransportError:  # pragma: no cover - 能力位与实现不一致
                logger.debug("释放 beat 租约失败", exc_info=True)
            self.is_leader = False

    def __enter__(self) -> Beat:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()


__all__ = ["Beat", "DEFAULT_STATE_FILE"]
