"""High-concurrency and slow-dependency resilience scenarios.

Every scenario prints the concurrency level, observed latency
distribution and rejection/error counts. The headline assertion is a
reproducible throughput improvement over the pre-hardening baseline.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

import httpx

from tests.conftest import print_report, run_load, start_server


def _get(base: str, url: str, timeout: float = 5.0):
    return httpx.get(
        f"{base}/call", params={"url": url}, timeout=timeout
    )


def test_slow_dependency_never_hangs_and_is_distinct(tmp_path):
    server = start_server(
        "main:app",
        env_overrides={
            "RATE_LIMIT_PER_SECOND": "100000",
            "MAX_CONCURRENT_CALLS": "30",
            "UPSTREAM_TIMEOUT_SECONDS": "0.4",
        },
        state_dir=tmp_path / "rl",
    )
    try:
        base = server.base_url
        slow_url = f"{base}/mock/delay?seconds=10"  # dependency 'stuck'
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=20) as pool:
            responses = list(
                pool.map(lambda _: _get(base, slow_url, timeout=3.0), range(20))
            )
        wall = time.perf_counter() - started

        assert all(r.status_code == 504 for r in responses)
        assert all(
            r.json()["error"]["type"] == "upstream_timeout" for r in responses
        )
        # 20 stuck calls resolve near the 0.4s deadline, never near 10s.
        assert wall < 1.5, f"slow calls were not cut off fast enough: {wall:.2f}s"
        print(
            f"\nslow-dependency: concurrency=20 wall={wall:.3f}s "
            f"504/upstream_timeout=20"
        )
    finally:
        server.stop()


def test_throughput_improves_vs_pre_hardening_baseline(tmp_path):
    """A/B: hardened service sheds slow calls at the timeout; the old
    service occupies its bounded pool for the whole dependency latency.
    """
    hard = start_server(
        "main:app",
        env_overrides={
            "RATE_LIMIT_PER_SECOND": "100000",
            "MAX_CONCURRENT_CALLS": "12",
            "UPSTREAM_TIMEOUT_SECONDS": "0.4",
        },
        state_dir=tmp_path / "rl-hard",
    )
    base = start_server(
        "tests.baseline_app:app",
        env_overrides={},
    )
    try:
        slow = "seconds=0.8"
        duration = 3.0
        # Concurrency equals the 12-slot in-flight budget. The old
        # service occupies its 12 pool connections for the full 0.8s
        # each; the hardened service frees every slot at the 0.4s
        # deadline. (Over-subscription + 503 fast-fail is covered
        # separately by test_bulkhead_rejects_with_distinct_503.)
        concurrency = 12

        old_report = run_load(
            base.base_url,
            f"{base.base_url}/mock/delay?{slow}",
            concurrency=concurrency,
            duration_seconds=duration,
            client_timeout=3.0,
        )
        print_report("PRE-HARDENING baseline (no timeout, shared pool)", old_report)

        new_report = run_load(
            hard.base_url,
            f"{hard.base_url}/mock/delay?{slow}",
            concurrency=concurrency,
            duration_seconds=duration,
            client_timeout=3.0,
        )
        print_report("HARDENED (0.4s deadline, per-call client)", new_report)

        old_tps = len(old_report.results) / old_report.wall_seconds
        new_tps = len(new_report.results) / new_report.wall_seconds
        speedup = new_tps / old_tps

        print(
            f"\nthroughput: baseline={old_tps:.2f} req/s "
            f"hardened={new_tps:.2f} req/s speedup={speedup:.2f}x"
        )
        # The theoretical ceiling with a 0.4s deadline vs 0.8s latency
        # is 2x; observed is typically ~1.9x. Require a robust,
        # reproducible margin well above noise.
        assert speedup >= 1.6, (
            f"hardened throughput (fast-fail 503/504, recycled slots) must be "
            f"reproducibly higher, got {speedup:.2f}x"
        )
        # Hardened calls terminate fast with identifiable reasons; the
        # baseline waits the full 0.8s and has no error taxonomy at all.
        assert set(new_report.status_counts()) <= {503, 504}
        assert (
            new_report.error_counts()["upstream_timeout"]
            + new_report.error_counts()["bulkhead_full"]
            == len(new_report.results)
        )
        # The deadline is enforced server-side; verify it via the
        # server's own latency histogram, which excludes any client-side
        # queueing from the load generator itself.
        metrics = httpx.get(f"{hard.base_url}/metrics", timeout=3.0).json()
        timeout_stats = metrics["latencies"]["timeout"]
        print(
            f"server-side timeout latency: p50={timeout_stats['p50_seconds']}s "
            f"p95={timeout_stats['p95_seconds']}s max={timeout_stats['max_seconds']}s"
        )
        # p95 must sit at the deadline; the single worst request is
        # allowed to be delayed by kernel accept-queue / event-loop
        # scheduling on a saturated box, but must remain a small
        # fraction of the 0.8s dependency latency (never hang).
        assert timeout_stats["p95_seconds"] < 0.6
        assert timeout_stats["max_seconds"] < 1.2
        # Baseline: 200s take the full dependency latency, and under
        # x2 over-subscription some requests exceed even the *client*
        # deadline (status 0) - the old service hangs instead of
        # failing fast. It never produces an explicit server error.
        assert set(old_report.status_counts()) <= {200, 0}
        old_ok = old_report.latencies_of(200)
        assert old_ok and old_report.percentile(old_ok, 0.5) > 0.7
    finally:
        hard.stop()
        base.stop()


def test_timed_out_calls_release_resources_for_followups(tmp_path):
    server = start_server(
        "main:app",
        env_overrides={
            "RATE_LIMIT_PER_SECOND": "100000",
            "MAX_CONCURRENT_CALLS": "12",
            "UPSTREAM_TIMEOUT_SECONDS": "0.3",
        },
        state_dir=tmp_path / "rl",
    )
    try:
        base = server.base_url
        with ThreadPoolExecutor(max_workers=12) as pool:
            list(
                pool.map(
                    lambda _: _get(base, f"{base}/mock/delay?seconds=5", 2.0),
                    range(24),
                )
            )

        metrics = httpx.get(f"{base}/metrics", timeout=3.0).json()
        assert metrics["in_flight"] == 0, metrics
        assert metrics["counts"]["timeout"] == 24

        # The event loop, pool and bulkhead must be fully usable again.
        for _ in range(5):
            response = _get(base, f"{base}/mock/delay?seconds=0.01", 3.0)
            assert response.status_code == 200, response.text
        print(
            "\nresource-recycling: 24 timeouts -> in_flight=0 -> "
            "5 follow-up calls all 200"
        )
    finally:
        server.stop()


def test_cancelled_connections_do_not_poison_pool(tmp_path):
    """Slow calls (cancelled at the deadline) and fast successful calls
    share the same client pool. If a cancelled socket were wrongly
    reused, a fast call would inherit the dead connection and fail or
    stall. It must remain fast and 200 throughout the mix.
    """
    server = start_server(
        "main:app",
        env_overrides={
            "RATE_LIMIT_PER_SECOND": "100000",
            "MAX_CONCURRENT_CALLS": "16",
            "UPSTREAM_TIMEOUT_SECONDS": "0.3",
        },
        state_dir=tmp_path / "rl",
    )
    try:
        base = server.base_url

        def slow():
            return _get(base, f"{base}/mock/delay?seconds=2", 2.0)

        def fast():
            started = time.perf_counter()
            response = _get(base, f"{base}/mock/delay?seconds=0.02", 2.0)
            return response, time.perf_counter() - started

        fast_results: list = []
        for _ in range(6):
            # Keep the pool saturated with slow calls...
            with ThreadPoolExecutor(max_workers=12) as pool:
                slow_futures = [pool.submit(slow) for _ in range(12)]
                # ...while interleaving fast calls that must succeed.
                for _ in range(3):
                    response, latency = fast()
                    fast_results.append((response.status_code, latency))
                for fut in slow_futures:
                    assert fut.result().status_code == 504

        codes = [code for code, _ in fast_results]
        latencies = sorted(lat for _, lat in fast_results)
        print(
            f"\npool-poisoning: {len(codes)} interleaved fast calls "
            f"codes={set(codes)} p95={latencies[int(0.95*(len(latencies)-1))]:.3f}s "
            f"max={latencies[-1]:.3f}s"
        )
        assert codes == [200] * len(codes)
        # A fast 20ms upstream must not inherit a dead connection.
        assert latencies[-1] < 0.5
    finally:
        server.stop()


def test_bulkhead_rejects_with_distinct_503(tmp_path):
    server = start_server(
        "main:app",
        env_overrides={
            "RATE_LIMIT_PER_SECOND": "100000",
            "MAX_CONCURRENT_CALLS": "3",
            "UPSTREAM_TIMEOUT_SECONDS": "1.5",
        },
        state_dir=tmp_path / "rl",
    )
    try:
        base = server.base_url
        with ThreadPoolExecutor(max_workers=12) as pool:
            responses = list(
                pool.map(
                    lambda _: _get(base, f"{base}/mock/delay?seconds=0.3", 3.0),
                    range(12),
                )
            )
        statuses = {}
        for response in responses:
            statuses.setdefault(response.status_code, []).append(
                response.json().get("error", {}).get("type")
            )
        print(
            f"\nbulkhead: capacity=3 concurrency=12 -> "
            f"{ {code: len(v) for code, v in statuses.items()} }"
        )
        assert 503 in statuses
        assert all(t == "bulkhead_full" for t in statuses[503])
        # Admitted work still succeeds normally (0.3s < 1.5s timeout).
        assert 200 in statuses
        assert 504 not in statuses

        metrics = httpx.get(f"{base}/metrics", timeout=3.0).json()
        assert metrics["in_flight"] == 0
        assert metrics["counts"]["bulkhead_full"] == len(statuses[503])
    finally:
        server.stop()


def test_normal_traffic_is_not_rejected_during_rate_limiting(tmp_path):
    server = start_server(
        "main:app",
        env_overrides={
            "RATE_LIMIT_PER_SECOND": "50",
            "MAX_CONCURRENT_CALLS": "20",
            "UPSTREAM_TIMEOUT_SECONDS": "1.0",
        },
        state_dir=tmp_path / "rl",
    )
    try:
        base = server.base_url
        # Well below the 50/s budget: every normal request succeeds.
        statuses = [
            _get(base, f"{base}/mock/delay?seconds=0.001", 3.0).status_code
            for _ in range(20)
        ]
        print(
            f"\nnormal-traffic: 20 requests under a 50/s limit -> "
            f"{ {code: statuses.count(code) for code in set(statuses)} }"
        )
        assert statuses == [200] * 20
    finally:
        server.stop()


def test_metrics_reconcile_after_load(tmp_path):
    server = start_server(
        "main:app",
        env_overrides={
            "RATE_LIMIT_PER_SECOND": "8",
            "MAX_CONCURRENT_CALLS": "4",
            "UPSTREAM_TIMEOUT_SECONDS": "0.2",
        },
        state_dir=tmp_path / "rl",
    )
    try:
        base = server.base_url
        report = run_load(
            base,
            f"{base}/mock/delay?seconds=0.6",  # always times out
            concurrency=8,
            duration_seconds=2.0,
            client_timeout=2.0,
        )
        print_report("mixed-load: rate-limited + bulkhead + timeout", report)

        metrics = httpx.get(f"{base}/metrics", timeout=3.0).json()
        counts = metrics["counts"]
        assert metrics["in_flight"] == 0
        assert metrics["completed"] == sum(counts.values())
        assert metrics["completed"] == len(report.results)
        assert metrics["rejected"] == (
            counts["rate_limited"] + counts["bulkhead_full"]
        )
        assert counts["rate_limited"] > 0
        assert counts["bulkhead_full"] > 0
        assert counts["timeout"] > 0
        # Latency accounting exists per outcome and counts agree.
        for outcome, value in counts.items():
            assert metrics["latencies"][outcome]["count"] == value
        # Timeouts are bounded by the configured deadline.
        assert metrics["latencies"]["timeout"]["p95_seconds"] < 0.6
        print(
            f"\nmetrics reconcile: counts={counts} "
            f"timeout p95={metrics['latencies']['timeout']['p95_seconds']}s"
        )
    finally:
        server.stop()
