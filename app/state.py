"""Shared SQLite-backed state (schema + connection helpers).

SQLite in WAL mode is used so that multiple uvicorn worker processes share the
same rate-limit state and metrics aggregates. Connections are opened with
``check_same_thread=False``; callers MUST serialize access (the rate limiter
and metrics recorder take a lock / short transactions).
"""

from __future__ import annotations

import os
import sqlite3
import time

# Token bucket state (single row per logical bucket).
_SCHEMA_TOKENS = """
CREATE TABLE IF NOT EXISTS token_bucket (
    key        TEXT PRIMARY KEY,
    tokens     REAL NOT NULL,
    updated_at REAL NOT NULL
)
"""

# Aggregated metrics across processes. Each process periodically upserts the
# delta observed since its last flush.
_SCHEMA_METRICS = """
CREATE TABLE IF NOT EXISTS metrics (
    outcome       TEXT PRIMARY KEY,
    count         INTEGER NOT NULL,
    latency_sum   REAL NOT NULL,
    latency_sumsq REAL NOT NULL,
    latency_min   REAL NOT NULL,
    latency_max   REAL NOT NULL
)
"""


# Log-spaced latency histogram (seconds), additive across processes, used to
# derive approximate p50/p90/p99 without per-request rows.
_SCHEMA_METRICS_HIST = """
CREATE TABLE IF NOT EXISTS metrics_hist (
    outcome    TEXT NOT NULL,
    bucket_idx INTEGER NOT NULL,
    count      INTEGER NOT NULL,
    PRIMARY KEY (outcome, bucket_idx)
)
"""

# Cross-process in-flight bulkhead permits. One row per held permit, keyed by
# scope ("global" or "target:<host:port>") plus a unique owner id. Each row
# carries a lease expiry so permits leaked by a crashed worker are reclaimed
# lazily instead of stranding capacity forever.
_SCHEMA_INFLIGHT = """
CREATE TABLE IF NOT EXISTS inflight_permits (
    scope      TEXT NOT NULL,
    owner      TEXT NOT NULL,
    expires_at REAL NOT NULL,
    PRIMARY KEY (scope, owner)
)
"""

# Per-target outcome/latency aggregates (same additive-delta scheme as
# ``metrics`` but with a target dimension).
_SCHEMA_METRICS_TARGET = """
CREATE TABLE IF NOT EXISTS metrics_target (
    target      TEXT NOT NULL,
    outcome     TEXT NOT NULL,
    count       INTEGER NOT NULL,
    latency_sum REAL NOT NULL,
    latency_min REAL NOT NULL,
    latency_max REAL NOT NULL,
    PRIMARY KEY (target, outcome)
)
"""


def open_db(db_path: str) -> sqlite3.Connection:
    """Open (creating schema/directories as needed) a shared SQLite connection.

    Retries briefly on ``database is locked``: when several worker processes
    start at once they all run the PRAGMA/schema bootstrap concurrently, and
    the journal-mode switch can lose that race even with a busy timeout. The
    whole bootstrap is idempotent, so retrying is safe.
    """
    directory = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(directory, exist_ok=True)
    last_exc: sqlite3.OperationalError | None = None
    for _ in range(50):  # up to ~5s of contention at startup
        conn = sqlite3.connect(
            db_path, timeout=5.0, isolation_level=None, check_same_thread=False
        )
        try:
            # Persistent DB-wide; cheap to (re)set on every connection.
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute(_SCHEMA_TOKENS)
            conn.execute(_SCHEMA_METRICS)
            conn.execute(_SCHEMA_METRICS_HIST)
            conn.execute(_SCHEMA_INFLIGHT)
            conn.execute(_SCHEMA_METRICS_TARGET)
            return conn
        except sqlite3.OperationalError as exc:
            conn.close()
            if "locked" in str(exc).lower():
                last_exc = exc
                time.sleep(0.1)
                continue
            raise
    raise last_exc  # type: ignore[misc]


def init_state(db_path: str) -> None:
    """Pre-create the database file and schema, then release the connection."""
    conn = open_db(db_path)
    conn.close()
