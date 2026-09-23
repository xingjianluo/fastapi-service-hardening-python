"""In-process observability.

Every served /call request is recorded with exactly one outcome:

* ``ok``            - downstream call succeeded
* ``timeout``       - downstream exceeded its hard deadline
* ``rate_limited``  - rejected by the cross-process rate limiter (429)
* ``bulkhead_full`` - rejected by the in-process bulkhead (503)
* ``upstream_error``- downstream failed for another reason (502)
* ``internal_error``- unexpected server-side failure (500)

For each outcome the registry keeps a request count and a latency
histogram (bucket upper bounds in seconds). Snapshot semantics are
identical during cold start and after warm-up: an empty registry simply
reports zeros, and in-flight is decremented on every code path.

Metrics are exposed via ``GET /metrics`` (pull) and, when configured,
logged periodically as a single structured JSON line (push, easy to
aggregate across processes from log pipelines).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import threading
import time
from dataclasses import dataclass, field

OUTCOMES: tuple[str, ...] = (
    "ok",
    "timeout",
    "rate_limited",
    "bulkhead_full",
    "upstream_error",
    "internal_error",
)

# Latency bucket upper bounds, in seconds. Finer resolution below 1s
# keeps deadline-adjacent percentiles (e.g. a 0.4s timeout) meaningful.
BUCKET_BOUNDS: tuple[float, ...] = (
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.3,
    0.35, 0.4, 0.45, 0.5, 0.6, 0.75, 1.0, 1.5, 2.5, 5.0, 10.0,
)

logger = logging.getLogger("app.metrics")


@dataclass
class LatencyStats:
    count: int = 0
    sum_seconds: float = 0.0
    avg_seconds: float = 0.0
    max_seconds: float = 0.0
    p50_seconds: float = 0.0
    p95_seconds: float = 0.0
    p99_seconds: float = 0.0
    buckets: dict[str, int] = field(default_factory=dict)


@dataclass
class Snapshot:
    started_at: float
    captured_at: float
    uptime_seconds: float
    in_flight: int
    completed: int
    accepted: int
    rejected: int
    counts: dict[str, int]
    latencies: dict[str, LatencyStats]

    def to_dict(self) -> dict:
        return {
            "started_at": self.started_at,
            "captured_at": self.captured_at,
            "uptime_seconds": round(self.uptime_seconds, 6),
            "in_flight": self.in_flight,
            "completed": self.completed,
            "accepted": self.accepted,
            "rejected": self.rejected,
            "counts": dict(self.counts),
            "latencies": {
                outcome: {
                    "count": s.count,
                    "sum_seconds": round(s.sum_seconds, 6),
                    "avg_seconds": round(s.avg_seconds, 6),
                    "max_seconds": round(s.max_seconds, 6),
                    "p50_seconds": round(s.p50_seconds, 6),
                    "p95_seconds": round(s.p95_seconds, 6),
                    "p99_seconds": round(s.p99_seconds, 6),
                    "buckets": dict(s.buckets),
                }
                for outcome, s in self.latencies.items()
            },
        }


def _percentile(buckets: list[int], total: int, q: float) -> float:
    target = total * q
    cumulative = 0
    for i, value in enumerate(buckets):
        cumulative += value
        if cumulative >= target:
            return BUCKET_BOUNDS[i]
    return BUCKET_BOUNDS[-1]


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started_at = time.time()
        self._counts: dict[str, int] = {outcome: 0 for outcome in OUTCOMES}
        # Outcome -> per-bucket counts; an extra implicit "+inf" value
        # landing in the last bucket covers latency above the max bound.
        self._buckets: dict[str, list[int]] = {
            outcome: [0] * len(BUCKET_BOUNDS) for outcome in OUTCOMES
        }
        self._sums: dict[str, float] = {outcome: 0.0 for outcome in OUTCOMES}
        self._max: dict[str, float] = {outcome: 0.0 for outcome in OUTCOMES}
        self._in_flight = 0

    def enter_request(self) -> None:
        with self._lock:
            self._in_flight += 1

    def leave_request(self) -> None:
        with self._lock:
            self._in_flight = max(0, self._in_flight - 1)

    def record(self, outcome: str, latency_seconds: float) -> None:
        if outcome not in OUTCOMES:
            raise ValueError(f"unknown outcome: {outcome}")
        with self._lock:
            self._counts[outcome] += 1
            self._sums[outcome] += latency_seconds
            self._max[outcome] = max(self._max[outcome], latency_seconds)
            index = 0
            for i, bound in enumerate(BUCKET_BOUNDS):
                if latency_seconds <= bound:
                    index = i
                    break
            else:
                index = len(BUCKET_BOUNDS) - 1
            self._buckets[outcome][index] += 1

    def snapshot(self) -> Snapshot:
        with self._lock:
            now = time.time()
            latencies: dict[str, LatencyStats] = {}
            for outcome in OUTCOMES:
                total = self._counts[outcome]
                buckets = list(self._buckets[outcome])
                sum_seconds = self._sums[outcome]
                stats = LatencyStats(
                    count=total,
                    sum_seconds=sum_seconds,
                    avg_seconds=(sum_seconds / total) if total else 0.0,
                    max_seconds=self._max[outcome],
                    p50_seconds=_percentile(buckets, total, 0.50) if total else 0.0,
                    p95_seconds=_percentile(buckets, total, 0.95) if total else 0.0,
                    p99_seconds=_percentile(buckets, total, 0.99) if total else 0.0,
                    buckets={
                        f"le_{bound:g}": value
                        for bound, value in zip(BUCKET_BOUNDS, buckets)
                    },
                )
                latencies[outcome] = stats
            counts = dict(self._counts)
            accepted = counts["ok"] + counts["timeout"] + counts[
                "upstream_error"
            ] + counts["internal_error"]
            rejected = counts["rate_limited"] + counts["bulkhead_full"]
            return Snapshot(
                started_at=self._started_at,
                captured_at=now,
                uptime_seconds=now - self._started_at,
                in_flight=self._in_flight,
                completed=sum(counts.values()),
                accepted=accepted,
                rejected=rejected,
                counts=counts,
                latencies=latencies,
            )


def snapshot_to_json(snapshot: Snapshot) -> str:
    return json.dumps(snapshot.to_dict(), separators=(",", ":"), sort_keys=True)


class PeriodicMetricsEmitter:
    """Background task emitting one aggregate JSON log line per interval."""

    def __init__(self, registry: MetricsRegistry, interval_seconds: float) -> None:
        self._registry = registry
        self._interval = interval_seconds
        self._task = None

    async def start(self) -> None:
        if self._interval <= 0:
            return
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            try:
                logger.info(
                    "metrics_snapshot %s",
                    snapshot_to_json(self._registry.snapshot()),
                )
            except Exception:  # pragma: no cover - logging must never crash the app
                logger.exception("metrics emission failed")

