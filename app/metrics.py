"""Request outcome/latency metrics, cross-process aggregated via SQLite.

Every request is classified into exactly one outcome -- ``success``,
``timeout``, ``rate_limited``, ``bulkhead_rejected`` or ``internal_error`` --
and its wall latency recorded.

Storage
-------
* ``metrics``      -- per-outcome counts and running sum / sum-of-squares /
  min / max (updated with additive deltas).
* ``metrics_hist`` -- a log-spaced latency histogram, additive across
  processes, used to derive approximate p50/p90/p99 without per-request rows.

Counters accumulate in memory (lock-protected) and are periodically flushed
to the shared SQLite database as deltas (``ON CONFLICT ... + excluded``), so
multiple worker processes aggregate without double counting.

Cold start semantics: counters begin at zero. A snapshot always contains every
outcome with a full set of zeroed fields, so metric names and shape are
identical before and after warm-up.
"""

from __future__ import annotations

import math
import sqlite3
import threading
from collections import defaultdict

from .state import open_db

OUTCOMES: tuple[str, ...] = (
    "success",
    "timeout",
    "rate_limited",
    "bulkhead_rejected",
    "target_rejected",
    "internal_error",
)

# Upper edges (seconds) for log-spaced buckets; idx 0 is underflow, idx N
# (== len(BOUNDS)) is overflow. Chosen to cover sub-millisecond to tens of
# seconds with roughly 2-5x resolution.
BOUNDS: tuple[float, ...] = (
    0.001, 0.002, 0.005,
    0.01, 0.02, 0.05,
    0.1, 0.2, 0.5,
    1.0, 2.0, 5.0,
    10.0, 30.0,
)
_N_BUCKETS = len(BOUNDS) + 1


def bucket_index(latency_sec: float) -> int:
    for i, edge in enumerate(BOUNDS):
        if latency_sec < edge:
            return i
    return len(BOUNDS)


def bucket_edges(idx: int) -> tuple[float, float]:
    if idx == 0:
        return 0.0, BOUNDS[0]
    if idx >= len(BOUNDS):
        return BOUNDS[-1], BOUNDS[-1] * 2.0
    return BOUNDS[idx - 1], BOUNDS[idx]


def percentile_from_hist(hist: list[int], q: float) -> float:
    total = sum(hist)
    if total <= 0:
        return 0.0
    target = q * (total - 1)  # nearest-rank style, 0-based fractional rank
    cumulative = 0
    for idx, count in enumerate(hist):
        if count == 0:
            continue
        if target <= cumulative + count - 1:
            low, high = bucket_edges(idx)
            if count == 1 or high == low:
                return high
            frac = (target - cumulative) / (count - 1)
            return low + frac * (high - low)
        cumulative += count
    low, _ = bucket_edges(len(hist) - 1)
    return low


class MetricsRecorder:
    def __init__(self, db_path: str, flush_interval_sec: float = 1.0) -> None:
        self._db_path = db_path
        self._flush_interval = float(flush_interval_sec)
        self._counts: dict[str, int] = defaultdict(int)
        self._sums: dict[str, float] = defaultdict(float)
        self._sumsq: dict[str, float] = defaultdict(float)
        self._mins: dict[str, float] = {}
        self._maxs: dict[str, float] = {}
        self._hists: dict[str, list[int]] = defaultdict(
            lambda: [0] * _N_BUCKETS
        )
        # Per-target aggregates: (target, outcome) -> [count, sum, min, max].
        self._target_stats: dict[tuple[str, str], list[float]] = {}
        self._lock = threading.Lock()
        self._db_lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # -- in-process recording ------------------------------------------------

    def observe(
        self, outcome: str, latency_sec: float, target: str | None = None
    ) -> None:
        with self._lock:
            self._counts[outcome] += 1
            self._sums[outcome] += latency_sec
            self._sumsq[outcome] += latency_sec * latency_sec
            cur_min = self._mins.get(outcome)
            self._mins[outcome] = (
                latency_sec if cur_min is None else min(cur_min, latency_sec)
            )
            cur_max = self._maxs.get(outcome)
            self._maxs[outcome] = (
                latency_sec if cur_max is None else max(cur_max, latency_sec)
            )
            self._hists[outcome][bucket_index(latency_sec)] += 1
            if target is not None:
                key = (target, outcome)
                stats = self._target_stats.get(key)
                if stats is None:
                    # [count, sum, min, max]
                    self._target_stats[key] = [1, latency_sec, latency_sec, latency_sec]
                else:
                    stats[0] += 1
                    stats[1] += latency_sec
                    stats[2] = min(stats[2], latency_sec)
                    stats[3] = max(stats[3], latency_sec)

    # -- cross-process flush -------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = open_db(self._db_path)
        return self._conn

    def flush(self) -> None:
        """Atomically move in-process deltas into the shared database."""
        with self._lock:
            if not self._counts:
                return
            metric_rows = []
            for outcome, count in self._counts.items():
                if count <= 0:
                    continue
                metric_rows.append((
                    outcome, count,
                    self._sums.get(outcome, 0.0),
                    self._sumsq.get(outcome, 0.0),
                    self._mins.get(outcome, 0.0),
                    self._maxs.get(outcome, 0.0),
                ))
            hist_rows = []
            for outcome, hist in self._hists.items():
                for idx, count in enumerate(hist):
                    if count:
                        hist_rows.append((outcome, idx, count))
            target_rows = [
                (target, outcome, int(stats[0]), stats[1], stats[2], stats[3])
                for (target, outcome), stats in self._target_stats.items()
                if stats[0] > 0
            ]
            self._counts.clear()
            self._sums.clear()
            self._sumsq.clear()
            self._mins.clear()
            self._maxs.clear()
            self._hists.clear()
            self._target_stats.clear()
        if not metric_rows and not target_rows:
            return
        # Serialize all transactions/reads on this connection: the periodic
        # flush thread and request threads serving /metrics otherwise overlap.
        with self._db_lock:
            conn = self._connect()
            while True:
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.executemany(
                        """
                        INSERT INTO metrics(outcome, count, latency_sum, latency_sumsq,
                                           latency_min, latency_max)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(outcome) DO UPDATE SET
                            count = count + excluded.count,
                            latency_sum = latency_sum + excluded.latency_sum,
                            latency_sumsq = latency_sumsq + excluded.latency_sumsq,
                            latency_min = MIN(latency_min, excluded.latency_min),
                            latency_max = MAX(latency_max, excluded.latency_max)
                        """,
                        metric_rows,
                    )
                    conn.executemany(
                        """
                        INSERT INTO metrics_hist(outcome, bucket_idx, count)
                        VALUES (?, ?, ?)
                        ON CONFLICT(outcome, bucket_idx) DO UPDATE SET
                            count = count + excluded.count
                        """,
                        hist_rows,
                    )
                    conn.executemany(
                        """
                        INSERT INTO metrics_target(target, outcome, count,
                                                   latency_sum, latency_min, latency_max)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(target, outcome) DO UPDATE SET
                            count = count + excluded.count,
                            latency_sum = latency_sum + excluded.latency_sum,
                            latency_min = MIN(latency_min, excluded.latency_min),
                            latency_max = MAX(latency_max, excluded.latency_max)
                        """,
                        target_rows,
                    )
                    conn.execute("COMMIT")
                    return
                except sqlite3.OperationalError as exc:
                    try:
                        conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    if "locked" in str(exc).lower():
                        continue
                    raise

    def _run(self) -> None:
        while not self._stop.wait(self._flush_interval):
            try:
                self.flush()
            except Exception:  # noqa: BLE001 - metrics must never kill a worker
                pass

    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._run, name="metrics-flush", daemon=True
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        self.flush()

    # -- read ---------------------------------------------------------------

    def snapshot(self) -> dict:
        """Aggregate persisted rows plus this process' unflushed deltas.

        Note: this flushes the *current* process; other processes are included
        once their periodic flush runs (``METRICS_FLUSH_INTERVAL_SEC``).
        """
        self.flush()
        with self._db_lock:
            conn = self._connect()
            persisted = {
                row[0]: row[1:]
                for row in conn.execute(
                    "SELECT outcome, count, latency_sum, latency_sumsq, "
                    "latency_min, latency_max FROM metrics"
                )
            }
            hists: dict[str, list[int]] = {
                outcome: [0] * _N_BUCKETS for outcome in OUTCOMES
            }
            for outcome, idx, count in conn.execute(
                "SELECT outcome, bucket_idx, count FROM metrics_hist"
            ):
                if outcome in hists:
                    hists[outcome][idx] += int(count)
            target_rows = conn.execute(
                "SELECT target, outcome, count, latency_sum, latency_min, "
                "latency_max FROM metrics_target"
            ).fetchall()

        result: dict[str, dict] = {}
        total = 0
        for outcome in OUTCOMES:
            count, ssum, ssq, smin, smax = persisted.get(
                outcome, (0, 0.0, 0.0, 0.0, 0.0)
            )
            count = int(count)
            mean = ssum / count if count else 0.0
            var = max(0.0, ssq / count - mean * mean) if count else 0.0
            hist = hists[outcome]
            result[outcome] = {
                "count": count,
                "latency_mean_sec": mean,
                "latency_stddev_sec": math.sqrt(var),
                "latency_min_sec": smin if count else 0.0,
                "latency_max_sec": smax if count else 0.0,
                "latency_p50_sec": percentile_from_hist(hist, 0.50),
                "latency_p90_sec": percentile_from_hist(hist, 0.90),
                "latency_p99_sec": percentile_from_hist(hist, 0.99),
            }
            total += count
        result["total"] = total

        # Per-target breakdown (only targets with observations appear).
        targets: dict[str, dict] = {}
        for target, outcome, count, lsum, lmin, lmax in target_rows:
            count = int(count)
            if count <= 0:
                continue
            targets.setdefault(target, {})[outcome] = {
                "count": count,
                "latency_mean_sec": lsum / count,
                "latency_min_sec": lmin,
                "latency_max_sec": lmax,
            }
        result["targets"] = targets
        return result
