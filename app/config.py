"""Service configuration (environment-overridable)."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # Token-bucket admission control shared across worker processes.
    rate_limit_capacity: float
    rate_limit_refill_per_sec: float
    # Downstream isolation: per-request timeout and max concurrent in-flight calls.
    downstream_timeout_sec: float
    downstream_max_inflight: int
    # SQLite file used for cross-process rate limiting and metrics aggregation.
    db_path: str
    # How often in-process metrics counters are flushed to the shared DB.
    metrics_flush_interval_sec: float


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def get_settings() -> Settings:
    return Settings(
        rate_limit_capacity=_float_env("RATE_LIMIT_CAPACITY", 20.0),
        rate_limit_refill_per_sec=_float_env("RATE_LIMIT_REFILL_PER_SEC", 10.0),
        downstream_timeout_sec=_float_env("DOWNSTREAM_TIMEOUT_SEC", 1.0),
        downstream_max_inflight=_int_env("DOWNSTREAM_MAX_INFLIGHT", 20),
        db_path=os.environ.get("SERVICE_DB_PATH", ".state/service.db"),
        metrics_flush_interval_sec=_float_env("METRICS_FLUSH_INTERVAL_SEC", 1.0),
    )
