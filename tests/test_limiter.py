"""Cross-process admission-control semantics.

The fixed-window limiter is exercised from multiple processes to prove
the counter is shared (not per-process), and an end-to-end run with two
uvicorn workers proves the same over real HTTP.
"""

from __future__ import annotations

import multiprocessing
import time

import httpx
import pytest

from app.limiter import FixedWindowRateLimiter, RateLimitExceeded


def _worker_acquire(state_dir: str, limit: int, attempts: int, barrier, queue) -> None:
    limiter = FixedWindowRateLimiter(limit, state_dir)
    barrier.wait()
    started = time.perf_counter()
    accepted = 0
    for _ in range(attempts):
        try:
            limiter.acquire()
            accepted += 1
        except RateLimitExceeded:
            pass
    queue.put((accepted, time.perf_counter() - started))


def test_fixed_window_is_shared_across_processes(tmp_path):
    state_dir = tmp_path / "rl"
    limit = 50
    workers = 4
    attempts = 40  # 160 total attempts, only 50 may pass in the window

    # fork lets every child inherit the same Barrier object.
    ctx = multiprocessing.get_context("fork")
    barrier = ctx.Barrier(workers + 1)  # +1 for the coordinating parent
    queue = ctx.Queue()
    processes = [
        ctx.Process(
            target=_worker_acquire,
            args=(str(state_dir), limit, attempts, barrier, queue),
        )
        for _ in range(workers)
    ]
    for proc in processes:
        proc.start()
    # Release every child right after a fresh window edge so the whole
    # fan-out is judged against a single fixed-window counter.
    now = time.time()
    time.sleep(1.03 - (now - int(now)))
    barrier.wait()
    accepted_per_worker = [queue.get() for _ in processes]
    for proc in processes:
        proc.join()
        assert proc.exitcode == 0

    total_accepted = sum(item[0] for item in accepted_per_worker)
    elapsed = max(item[1] for item in accepted_per_worker)
    print(
        f"\ncross-process limiter: admitted per worker="
        f"{[item[0] for item in accepted_per_worker]} slowest worker elapsed="
        f"{elapsed * 1000:.1f}ms (must stay inside one 1s window)"
    )
    assert elapsed < 0.9, "workload spilled across window boundaries"
    assert total_accepted == limit, (
        f"exactly {limit} requests must be admitted across all processes, "
        f"got {total_accepted} (per worker: {accepted_per_worker})"
    )


def test_window_rolls_over(tmp_path):
    limiter = FixedWindowRateLimiter(2, str(tmp_path / "rl"))
    limiter.acquire()
    limiter.acquire()
    with pytest.raises(RateLimitExceeded):
        limiter.acquire()
    time.sleep(1.05)
    # New window: budget replenished.
    limiter.acquire()
    limiter.acquire()
    with pytest.raises(RateLimitExceeded):
        limiter.acquire()


def test_two_uvicorn_workers_share_one_limit(tmp_path):
    from tests.conftest import start_server

    state_dir = tmp_path / "rl"
    limit = 30
    server = start_server(
        "main:app",
        workers=2,
        env_overrides={
            "RATE_LIMIT_PER_SECOND": str(limit),
            "MAX_CONCURRENT_CALLS": "100",
            "UPSTREAM_TIMEOUT_SECONDS": "1.0",
        },
        state_dir=state_dir,
    )
    try:
        base = server.base_url
        from tests.conftest import print_report, run_load

        # Start just after a window edge and saturate for two full
        # windows. With a shared counter the accepted throughput is
        # bounded by ~limit/s regardless of worker count; two
        # independent per-worker counters would double it.
        now = time.time()
        time.sleep(1.05 - (now - int(now)))

        duration = 2.0
        report = run_load(
            base,
            f"{base}/mock/delay?seconds=0",
            concurrency=16,
            duration_seconds=duration,
            client_timeout=5.0,
        )
        print_report("two workers, one shared 30/s budget", report)

        statuses = report.status_counts()
        accepted = statuses.get(200, 0)
        rejected = statuses.get(429, 0)
        assert set(statuses) <= {200, 429}, statuses
        assert rejected > 0, "surplus requests must be rejected with 429"
        assert accepted >= int(limit * duration * 0.7), (
            f"limiter too aggressive / windows not replenishing: {accepted}"
        )
        # 2.0s run spans at most parts of 4 fixed windows; allow a
        # generous slack but stay far below the 2x per-process figure.
        assert accepted <= int(limit * (duration + 1.6)), (
            f"counter not shared across workers: {accepted} accepted"
        )
        assert accepted < int(2 * limit * duration * 0.85), (
            f"each worker appears to own a private limit: {accepted}"
        )
        # Every rejection carries the distinct, stable error envelope.
        assert dict(report.error_counts()) == {"rate_limited": rejected}
    finally:
        server.stop()


def test_rate_limited_requests_consume_no_bulkhead_slot(tmp_path):
    """Rejected-at-the-door traffic must never reach the isolation layer."""
    from tests.conftest import start_server

    state_dir = tmp_path / "rl"
    server = start_server(
        "main:app",
        env_overrides={
            "RATE_LIMIT_PER_SECOND": "3",
            "MAX_CONCURRENT_CALLS": "50",
            "UPSTREAM_TIMEOUT_SECONDS": "1.0",
        },
        state_dir=state_dir,
    )
    try:
        base = server.base_url
        now = time.time()
        time.sleep(1.02 - (now - int(now)))
        statuses = []
        for _ in range(6):
            statuses.append(
                httpx.get(
                    f"{base}/call",
                    params={"url": f"{base}/mock/delay?seconds=0.001"},
                    timeout=5.0,
                ).status_code
            )
        assert statuses.count(200) <= 3
        assert 429 in statuses
    finally:
        server.stop()
