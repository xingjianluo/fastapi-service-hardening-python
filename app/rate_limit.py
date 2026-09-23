"""Cross-process token-bucket admission control backed by SQLite.

Admission is decided inside a ``BEGIN IMMEDIATE`` transaction:
``SELECT`` old bucket state, lazily refill in Python, then ``UPDATE``. The
immediate write lock serializes decisions across worker processes (other
writers hit SQLITE_BUSY and retry via ``busy_timeout``), and an in-process
lock serializes threads within a worker. Thus concurrent callers can never
double-spend a token:

* eligible requests always succeed (burst up to ``capacity``), so normal
  traffic is never rejected while the bucket is non-empty;
* over-capacity requests are deterministically rejected; they consume no token
  and receive a distinct 429 response.

The row is seeded full (``tokens=capacity, updated_at=now``) under
``INSERT OR IGNORE``, so the bucket starts exactly full regardless of which
process handles the first request.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Callable

from .state import open_db

DEFAULT_KEY = "inbound:default"
# Floating-point slop at the one-token boundary (1e-9 s of refill).
_EPS = 1e-9


class RateLimiter:
    def __init__(
        self,
        db_path: str,
        capacity: float,
        refill_per_sec: float,
        key: str = DEFAULT_KEY,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._db_path = db_path
        self._capacity = float(capacity)
        self._refill = float(refill_per_sec)
        self._key = key
        self._clock = clock
        self._conn: sqlite3.Connection | None = None
        self._seeded = False
        self._lock = threading.Lock()

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = open_db(self._db_path)
        return self._conn

    def _ensure_seed(self, conn: sqlite3.Connection) -> None:
        if self._seeded:
            return
        # Seed a full bucket; IGNORE keeps this safe under concurrency.
        conn.execute(
            "INSERT OR IGNORE INTO token_bucket(key, tokens, updated_at) VALUES (?, ?, ?)",
            (self._key, self._capacity, self._clock()),
        )
        self._seeded = True

    def allow(self) -> bool:
        """Return True when one token was consumed, False when rejected."""
        with self._lock:
            conn = self._connect()
            self._ensure_seed(conn)
            while True:
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    tokens, updated_at = conn.execute(
                        "SELECT tokens, updated_at FROM token_bucket WHERE key = ?",
                        (self._key,),
                    ).fetchone()
                    now = self._clock()
                    avail = min(
                        self._capacity,
                        float(tokens) + (now - float(updated_at)) * self._refill,
                    )
                    allowed = avail >= 1.0 - _EPS
                    if allowed:
                        conn.execute(
                            "UPDATE token_bucket SET tokens = ?, updated_at = ? WHERE key = ?",
                            (avail - 1.0, now, self._key),
                        )
                    conn.execute("COMMIT")
                    return allowed
                except sqlite3.OperationalError as exc:
                    try:
                        conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    if "locked" in str(exc).lower():
                        time.sleep(0.002)
                        continue
                    raise

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
