"""High-concurrency against a SLOW dependency: hardened vs. legacy path.

Asserts:
* a reproducible throughput improvement (hardened wall-clock bounded by the
  timeout; legacy is bounded by the dependency's real latency x threadpool
  batches);
* distinct, consistent failure reasons (504 timeout vs 503 bulkhead).
"""

from __future__ import annotations

import urllib.parse

from conftest import flood

CONCURRENCY = 64
SLOW_DELAY = 2.0
TIMEOUT = 0.5
MAX_INFLIGHT = 32


def _slow_target(base_url: str) -> str:
    dep = f"{base_url}/internal/downstream?delay={SLOW_DELAY}"
    return f"{base_url}/data?target=" + urllib.parse.quote(dep, safe="")


def _legacy_target(base_url: str) -> str:
    dep = f"{base_url}/internal/downstream?delay={SLOW_DELAY}"
    return f"{base_url}/upstream-legacy?target=" + urllib.parse.quote(dep, safe="")


def test_hardened_path_bounds_latency_and_classifies_failures(server_factory):
    server = server_factory(
        "throughput-hardened",
        env_extra={
            "DOWNSTREAM_TIMEOUT_SEC": str(TIMEOUT),
            "DOWNSTREAM_MAX_INFLIGHT": str(MAX_INFLIGHT),
            # Rate limiter must NOT interfere in this scenario.
            "RATE_LIMIT_CAPACITY": "1000000",
            "RATE_LIMIT_REFILL_PER_SEC": "1000000",
        },
    )

    result = flood(
        "hardened/slow-dep", _slow_target(server.base_url), CONCURRENCY,
        timeout=10.0,
    )

    # Wall clock must be close to the timeout, not the dependency delay.
    assert result.wall_sec < 1.5, result.report()
    # Every request is decided within ~timeout (+ scheduling slack).
    assert result.percentile(0.99) < 1.0, result.report()
    assert -1 not in result.status_counts, "no client-side errors/resource leaks"

    # Exactly the bulkhead permits wait (and time out); the rest fail fast.
    timeout_count = result.status_counts.get(504, 0)
    rejected_count = result.status_counts.get(503, 0)
    assert timeout_count + rejected_count == CONCURRENCY, result.report()
    assert abs(timeout_count - MAX_INFLIGHT) <= 2, result.report()
    assert abs(rejected_count - (CONCURRENCY - MAX_INFLIGHT)) <= 2, result.report()
    assert result.outcomes.get("timeout") == timeout_count
    assert result.outcomes.get("bulkhead_rejected") == rejected_count

    # Repeat: resources (semaphore permits / connections) must be recycled.
    second = flood(
        "hardened/slow-dep-again", _slow_target(server.base_url), CONCURRENCY,
        timeout=10.0,
    )
    assert second.wall_sec < 1.5, second.report()
    assert second.status_counts.get(504, 0) + second.status_counts.get(503, 0) == CONCURRENCY
    # Same distribution across waves -> consistent, no leaked permits.
    assert abs(
        second.status_counts.get(503, 0) - rejected_count
    ) <= 2, second.report()


def test_legacy_path_is_bottlenecked_and_hardened_improves_throughput(server_factory):
    server = server_factory(
        "throughput-compare",
        env_extra={
            "DOWNSTREAM_TIMEOUT_SEC": str(TIMEOUT),
            "DOWNSTREAM_MAX_INFLIGHT": str(MAX_INFLIGHT),
            "RATE_LIMIT_CAPACITY": "1000000",
            "RATE_LIMIT_REFILL_PER_SEC": "1000000",
        },
    )

    legacy = flood(
        "legacy/slow-dep", _legacy_target(server.base_url), CONCURRENCY,
        timeout=30.0,
    )
    hardened = flood(
        "hardened/slow-dep", _slow_target(server.base_url), CONCURRENCY,
        timeout=10.0,
    )

    # Legacy: blocking calls each take the full dependency latency.
    assert all(code == 200 for code in legacy.status_counts), legacy.report()
    assert legacy.percentile(0.50) >= SLOW_DELAY * 0.9, legacy.report()
    # Hardened: everything decided near the timeout.
    assert hardened.wall_sec < 1.5, hardened.report()

    speedup = legacy.wall_sec / hardened.wall_sec
    print(f"\nTHROUGHPUT SPEEDUP (wall-clock): {speedup:.2f}x "
          f"(legacy {legacy.throughput_per_sec:.1f} req/s vs "
          f"hardened {hardened.throughput_per_sec:.1f} req/s)")
    assert speedup >= 3.0, "hardened path must reproducibly improve throughput"
