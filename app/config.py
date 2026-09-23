"""Runtime configuration loaded from environment variables.

All knobs have safe defaults so the service behaves the same on cold
start and after warm-up; tests override the environment explicitly.
"""

import os
import tempfile
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # Rate limiting (fixed window, shared across worker processes via a
    # file lock). Normal traffic must stay below this; only excess load
    # is rejected.
    rate_limit_per_second: int
    # Directory that holds the cross-process window counter file.
    rate_limit_state_dir: str

    # Bulkhead: max number of in-flight downstream calls *per process*.
    max_concurrent_calls: int

    # Hard timeout for a single downstream call.
    upstream_timeout_seconds: float

    # Shared HTTP client pool. Defaults (0) mean "derive the lane count
    # from the bulkhead", i.e. one client lane per concurrency slot.
    client_pool_size: int
    per_client_connections: int
    upstream_connect_timeout_seconds: float

    # Period between aggregate metrics log lines (0 disables the
    # background emitter); metrics remain available via /metrics.
    metrics_emit_interval_seconds: float


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


def get_settings() -> Settings:
    return Settings(
        rate_limit_per_second=_env_int("RATE_LIMIT_PER_SECOND", 10),
        rate_limit_state_dir=os.environ.get(
            "RATE_LIMIT_STATE_DIR",
            os.path.join(tempfile.gettempdir(), "fastapi-hardening-ratelimit"),
        ),
        max_concurrent_calls=_env_int("MAX_CONCURRENT_CALLS", 20),
        upstream_timeout_seconds=_env_float("UPSTREAM_TIMEOUT_SECONDS", 1.0),
        client_pool_size=_env_int("CLIENT_POOL_SIZE", 0),
        per_client_connections=_env_int("PER_CLIENT_CONNECTIONS", 8),
        upstream_connect_timeout_seconds=_env_float(
            "UPSTREAM_CONNECT_TIMEOUT_SECONDS", 0.5
        ),
        metrics_emit_interval_seconds=_env_float(
            "METRICS_EMIT_INTERVAL_SECONDS", 5.0
        ),
    )
