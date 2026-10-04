"""Service configuration (environment-overridable)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class TargetRule:
    """Per-downstream-target overrides; ``None`` means "use the default"."""

    max_inflight: int | None = None
    timeout_sec: float | None = None


@dataclass(frozen=True)
class Settings:
    # Token-bucket admission control shared across worker processes.
    rate_limit_capacity: float
    rate_limit_refill_per_sec: float
    # Downstream isolation: per-request timeout and max concurrent in-flight calls.
    # These remain the *global* gate (final circuit breaker across all targets)
    # and the fallback for targets without an explicit rule.
    downstream_timeout_sec: float
    downstream_max_inflight: int
    # Per-target defaults (fall back to the global values when unset).
    target_default_max_inflight: int
    target_default_timeout_sec: float
    # SQLite file used for cross-process rate limiting and metrics aggregation.
    db_path: str
    # How often in-process metrics counters are flushed to the shared DB.
    metrics_flush_interval_sec: float
    # Explicit per-target overrides keyed by "host:port".
    target_overrides: dict[str, TargetRule] = field(default_factory=dict)


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _target_overrides_env(name: str) -> dict[str, TargetRule]:
    """Parse ``TARGET_OVERRIDES`` JSON: ``{"host:port": {"max_inflight": 4,
    "timeout_sec": 0.5}, ...}``. Unknown/absent fields fall back to defaults."""
    raw = os.environ.get(name)
    if raw in (None, ""):
        return {}
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise TypeError(f"{name} must be a JSON object keyed by host:port")
    rules: dict[str, TargetRule] = {}
    for key, spec in data.items():
        spec = spec or {}
        rules[str(key)] = TargetRule(
            max_inflight=(
                int(spec["max_inflight"]) if spec.get("max_inflight") is not None else None
            ),
            timeout_sec=(
                float(spec["timeout_sec"]) if spec.get("timeout_sec") is not None else None
            ),
        )
    return rules


def get_settings() -> Settings:
    timeout = _float_env("DOWNSTREAM_TIMEOUT_SEC", 1.0)
    max_inflight = _int_env("DOWNSTREAM_MAX_INFLIGHT", 20)
    return Settings(
        rate_limit_capacity=_float_env("RATE_LIMIT_CAPACITY", 20.0),
        rate_limit_refill_per_sec=_float_env("RATE_LIMIT_REFILL_PER_SEC", 10.0),
        downstream_timeout_sec=timeout,
        downstream_max_inflight=max_inflight,
        # Per-target defaults fall back to the global values so existing
        # deployments keep their exact previous behavior.
        target_default_max_inflight=_int_env(
            "TARGET_DEFAULT_MAX_INFLIGHT", max_inflight
        ),
        target_default_timeout_sec=_float_env(
            "TARGET_DEFAULT_TIMEOUT_SEC", timeout
        ),
        target_overrides=_target_overrides_env("TARGET_OVERRIDES"),
        db_path=os.environ.get("SERVICE_DB_PATH", ".state/service.db"),
        metrics_flush_interval_sec=_float_env("METRICS_FLUSH_INTERVAL_SEC", 1.0),
    )
