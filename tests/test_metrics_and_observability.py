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
                    "target_rejected", "downstream_error",
                    "downstream_unreachable", "internal_error"):
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


def test_timeout_downstream_error_and_unreachable_are_separable(server_factory):
    server = server_factory(
        "metrics-failures",
        env_extra={
            "RATE_LIMIT_CAPACITY": "1000",
            "RATE_LIMIT_REFILL_PER_SEC": "1000",
            "DOWNSTREAM_TIMEOUT_SEC": "0.3",
            "DOWNSTREAM_MAX_INFLIGHT": "100",
        },
    )
    base = server.base_url
    with httpx.Client(timeout=10.0) as client:
        # downstream timeout: 504
        r = client.get(_data_url(base, delay=2))
        assert r.status_code == 504 and r.json()["outcome"] == "timeout"
        assert r.json()["error"] == "downstream_timeout"
        # downstream actively returns an error status -> downstream_error (502)
        r = client.get(_data_url(base, fail="true"))
        body = r.json()
        assert r.status_code == 502 and body["outcome"] == "downstream_error"
        assert body["error"] == "downstream_error"
        assert body["error_kind"] == "http_error_status"
        assert body["downstream_status_code"] == 500
        # connection refused (nothing listening): a DISTINCT 502 outcome
        r = client.get(f"{base}/data?target=" + urllib.parse.quote(
            "http://127.0.0.1:1/nope", safe=""))
        body = r.json()
        assert r.status_code == 502
        assert body["outcome"] == "downstream_unreachable"
        assert body["error"] == "downstream_unreachable"
        assert body["error_kind"] == "connection_refused"

    snap = _metrics(base)
    print("\n[failure outcomes] timeout=%s downstream_error=%s "
          "downstream_unreachable=%s" % (
              snap["timeout"]["count"],
              snap["downstream_error"]["count"],
              snap["downstream_unreachable"]["count"]))
    assert snap["timeout"]["count"] == 1
    assert snap["downstream_error"]["count"] == 1
    assert snap["downstream_unreachable"]["count"] == 1
    assert snap["internal_error"]["count"] == 0
    assert snap["bulkhead_rejected"]["count"] == 0
    # timeout latency is bounded at the deadline, not the dependency delay
    assert snap["timeout"]["latency_max_sec"] < 0.6

    # Per-target attribution: the two 502 kinds land under DIFFERENT targets.
    targets = snap["targets"]
    dep_target = f"{base.replace('http://', 'http://')}/internal/downstream"
    assert targets[dep_target]["downstream_error"]["count"] == 1
    assert targets["http://127.0.0.1:1/nope"][
        "downstream_unreachable"]["count"] == 1
    # every target carries the full zero-filled taxonomy
    for bucket in targets.values():
        assert set(bucket) == {
            "success", "timeout", "rate_limited", "bulkhead_rejected",
            "target_rejected", "downstream_error",
            "downstream_unreachable", "internal_error",
        }


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
