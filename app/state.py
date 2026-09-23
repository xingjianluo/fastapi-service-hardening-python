"""Shared SQLite-backed state (schema + connection helpers).

SQLite in WAL mode is used so that multiple uvicorn worker processes share the
same rate-limit state and metrics aggregates. Connections are opened with
``check_same_thread=False``; callers MUST serialize access (the rate limiter
and metrics recorder take a lock / short transactions).
"""

from __future__ import annotations

import os
import sqlite3

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


def open_db(db_path: str) -> sqlite3.Connection:
    """Open (creating schema/directories as needed) a shared SQLite connection."""
    directory = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(
        db_path, timeout=5.0, isolation_level=None, check_same_thread=False
    )
    # Persistent DB-wide; cheap to (re)set on every connection.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute(_SCHEMA_TOKENS)
    conn.execute(_SCHEMA_METRICS)
    conn.execute(_SCHEMA_METRICS_HIST)
    return conn


def init_state(db_path: str) -> None:
    """Pre-create the database file and schema, then release the connection."""
    conn = open_db(db_path)
    conn.close()
