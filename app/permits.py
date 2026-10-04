"""Cross-process in-flight permit store backed by SQLite.

Permits enforce capacity boundaries (global and per-target) consistently
across worker processes: acquisition happens inside a ``BEGIN IMMEDIATE``
transaction (delete expired rows, count live rows for the scope, insert one
if below the limit), so concurrent processes can never oversubscribe a
scope -- the SQLite write lock serializes decisions exactly like the token
bucket does for rate limiting.

Each permit row carries a TTL (``expires_at``); expired rows are
garbage-collected on every acquisition, so a crashed worker cannot leak
capacity forever. In the normal path permits are released explicitly in
``finally`` blocks -- success, failure, timeout and cancellation alike --
so capacity is recycled promptly and behavior does not drift across waves.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass

from .state import open_db


@dataclass(frozen=True)
class Permit:
    """A held capacity slot in one scope; must be released exactly once."""

    permit_id: int
    scope: str


class PermitStore:
    def __init__(
        self,
        db_path: str,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._db_path = db_path
        # Wall clock: expiry must be comparable across processes.
        self._clock = clock
        self._owner = f"{os.getpid()}:{uuid.uuid4().hex[:12]}"
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.Lock()

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = open_db(self._db_path)
        return self._conn

    def acquire(self, scope: str, limit: int, ttl_sec: float) -> Permit | None:
        """Take one permit for ``scope`` or return None when it is full."""
        if limit <= 0:
            return None
        with self._lock:
            conn = self._connect()
            while True:
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    now = self._clock()
                    # Reap permits orphaned by crashed/timed-out holders.
                    conn.execute(
                        "DELETE FROM inflight_permit WHERE expires_at < ?", (now,)
                    )
                    (count,) = conn.execute(
                        "SELECT COUNT(*) FROM inflight_permit WHERE scope = ?",
                        (scope,),
                    ).fetchone()
                    if int(count) >= limit:
                        conn.execute("COMMIT")
                        return None
                    cur = conn.execute(
                        "INSERT INTO inflight_permit(scope, owner, expires_at) "
                        "VALUES (?, ?, ?)",
                        (scope, self._owner, now + ttl_sec),
                    )
                    conn.execute("COMMIT")
                    return Permit(permit_id=int(cur.lastrowid), scope=scope)
                except sqlite3.OperationalError as exc:
                    try:
                        conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    if "locked" in str(exc).lower():
                        time.sleep(0.002)
                        continue
                    raise

    def release(self, permit: Permit) -> None:
        """Return a permit; idempotent and safe to call after expiry."""
        with self._lock:
            conn = self._connect()
            while True:
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute(
                        "DELETE FROM inflight_permit WHERE id = ?",
                        (permit.permit_id,),
                    )
                    conn.execute("COMMIT")
                    return
                except sqlite3.OperationalError as exc:
                    try:
                        conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    if "locked" in str(exc).lower():
                        time.sleep(0.002)
                        continue
                    raise

    def in_flight(self, scope: str) -> int:
        """Current number of live permits for ``scope`` (observability)."""
        with self._lock:
            conn = self._connect()
            now = self._clock()
            (count,) = conn.execute(
                "SELECT COUNT(*) FROM inflight_permit "
                "WHERE scope = ? AND expires_at >= ?",
                (scope, now),
            ).fetchone()
            return int(count)

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
