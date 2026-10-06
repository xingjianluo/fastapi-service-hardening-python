"""Observability: outcome taxonomy, latency aggregates and cold-start semantics."""

from __future__ import annotations

import urllib.parse

import httpx


def _data_url(base_url: str, **params) -> str:
    query = "&".join(f"{k}={v}" for k, v in params.items())
    dep = f"{base_url}/internal/downstream?{query}" if query else f"{base_url}/internal/downstream"
    return f"{base_url}/data?target=" + urllib.parse.quote(dep, safe="")


def _metrics(base_url: str) -> dict:
    return httpx.get(f"{base_url}/metrics", timeout=5.0).json()


def test_cold_start_metrics_have_stable_zero_shape(server_factory):
    server = server_factory("metrics-cold")
    snap = _metrics(server.base_url)
    print("\n[cold-start metrics keys]", sorted(snap.keys()))
    for outcome in ("success", "timeout", "rate_limited", "bulkhead_rejected",
                    "target_rejected", "connect_failed", "downstream_error",
                    "internal_error"):
        assert outcome in snap, outcome
        assert snap[outcome]["count"] == 0
        assert snap[outcome]["latency_mean_sec"] == 0.0
        assert snap[outcome]["latency_p50_sec"] == snap[outcome]["latency_p99_sec"] == 0.0
    assert snap["total"] == 0


def test_each_outcome_is_distinctly_recorded(server_factory):
    server = server_factory(
        "metrics-outcomes",
        env_extra={
            "RATE_LIMIT_CAPACITY": "2",
            "RATE_LIMIT_REFILL_PER_SEC": "0.0",
            "DOWNSTREAM_TIMEOUT_SEC": "0.3",
            "DOWNSTREAM_MAX_INFLIGHT": "1",
        },
    )
    base = server.base_url
    with httpx.Client(timeout=10.0) as client:
        def get(url: str) -> int:
            return client.get(url).status_code

        # 2 successes (consume the two tokens)
        assert get(_data_url(base, delay=0)) == 200
        assert get(_data_url(base, delay=0)) == 200
        # next request -> rate limited (no token), distinct 429
        assert get(_data_url(base, delay=0)) == 429
        assert get(_data_url(base, delay=0)) == 429
        # downstream internal error: cannot trigger now (bucket empty), verify
        # taxonomy on a fresh instance below instead.

    snap = _metrics(base)
    print("\n[outcomes] success=%s rate_limited=%s" % (
        snap["success"]["count"], snap["rate_limited"]["count"]))
    assert snap["success"]["count"] == 2
    assert snap["rate_limited"]["count"] == 2
    assert snap["timeout"]["count"] == 0
    assert snap["total"] == 4
    # latency stats present and non-negative for observed outcomes
    s = snap["success"]
    assert s["latency_max_sec"] >= s["latency_min_sec"] >= 0
    assert s["latency_p99_sec"] >= s["latency_p50_sec"] >= 0


def test_timeout_bulkhead_and_error_outcomes_are_separable(server_factory):
    server = server_factory(
        "metrics-failures",
        env_extra={
            "RATE_LIMIT_CAPACITY": "1000",
            "RATE_LIMIT_REFILL_PER_SEC": "1000",
            "DOWNSTREAM_TIMEOUT_SEC": "0.3",
            "DOWNSTREAM_MAX_INFLIGHT": "1",
        },
    )
    base = server.base_url
    with httpx.Client(timeout=10.0) as client:
        # downstream timeout: 504
        r = client.get(_data_url(base, delay=2))
        assert r.status_code == 504 and r.json()["outcome"] == "timeout"
        # downstream actively returns an error status: 502 downstream_error
        r = client.get(_data_url(base, fail="true"))
        assert r.status_code == 502 and r.json()["outcome"] == "downstream_error"
        assert r.json()["error"] == "downstream_error"
        assert r.json()["downstream_status_code"] == 500
        # transport failure (closed port): 502 connect_failed, fast, and
        # clearly distinct from the downstream's own error status above
        r = client.get(f"{base}/data?target=" + urllib.parse.quote(
            "http://127.0.0.1:1/nope", safe=""))
        assert r.status_code == 502 and r.json()["outcome"] == "connect_failed"
        assert r.json()["error"] == "downstream_unreachable"

    snap = _metrics(base)
    print("\n[failure outcomes] timeout=%s downstream_error=%s connect_failed=%s" % (
        snap["timeout"]["count"], snap["downstream_error"]["count"],
        snap["connect_failed"]["count"]))
    assert snap["timeout"]["count"] == 1
    assert snap["downstream_error"]["count"] == 1
    assert snap["connect_failed"]["count"] == 1
    assert snap["internal_error"]["count"] == 0
    assert snap["bulkhead_rejected"]["count"] == 0
    # timeout latency is bounded at the deadline, not the dependency delay
    assert snap["timeout"]["latency_max_sec"] < 0.6


def test_metrics_aggregate_across_processes(server_factory):
    import time

    server = server_factory(
        "metrics-multiprocess",
        workers=2,
        env_extra={
            "RATE_LIMIT_CAPACITY": "1000",
            "RATE_LIMIT_REFILL_PER_SEC": "1000",
            "DOWNSTREAM_MAX_INFLIGHT": "100",
            "METRICS_FLUSH_INTERVAL_SEC": "0.2",
        },
    )
    with httpx.Client(timeout=10.0) as client:
        for _ in range(10):
            assert client.get(_data_url(server.base_url, delay=0)).status_code == 200
    # Wait long enough for every worker process to flush its deltas.
    time.sleep(0.8)
    snap = _metrics(server.base_url)
    print("\n[multiprocess metrics] success count:", snap["success"]["count"])
    assert snap["success"]["count"] == 10
