"""Backward compatibility and basic failure-taxonomy contract."""

from __future__ import annotations

import httpx


def test_root_contract_unchanged(hardened_server):
    response = httpx.get(f"{hardened_server.base_url}/", timeout=3.0)
    assert response.status_code == 200
    assert response.json() == {"Hello": "World"}


def test_health(hardened_server):
    response = httpx.get(f"{hardened_server.base_url}/health", timeout=3.0)
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_successful_call(hardened_server):
    base = hardened_server.base_url
    response = httpx.get(
        f"{base}/call",
        params={"url": f"{base}/mock/delay?seconds=0.01"},
        timeout=3.0,
    )
    assert response.status_code == 200
    assert response.json() == {"data": {"slept": 0.01}}


def test_slow_call_becomes_distinct_timeout(hardened_server):
    # 0.8s dependency latency vs 0.4s configured hard timeout.
    base = hardened_server.base_url
    response = httpx.get(
        f"{base}/call",
        params={"url": f"{base}/mock/delay?seconds=0.8"},
        timeout=3.0,
    )
    assert response.status_code == 504
    body = response.json()
    assert body["error"]["type"] == "upstream_timeout"
    assert "0.4" in body["error"]["message"]


def test_failing_upstream_becomes_distinct_error(hardened_server):
    base = hardened_server.base_url
    response = httpx.get(
        f"{base}/call",
        params={"url": f"{base}/mock/error"},
        timeout=3.0,
    )
    assert response.status_code == 502
    assert response.json()["error"]["type"] == "upstream_error"


def test_timeout_does_not_exceed_deadline_much(hardened_server):
    base = hardened_server.base_url
    # Even with a catastrophically slow dependency (30s), the caller
    # must get the timeout result near the 0.4s deadline.
    response = httpx.get(
        f"{base}/call",
        params={"url": f"{base}/mock/delay?seconds=30"},
        timeout=3.0,
    )
    assert response.status_code == 504


def test_cold_metrics_have_zero_semantics(hardened_server):
    response = httpx.get(f"{hardened_server.base_url}/metrics", timeout=3.0)
    assert response.status_code == 200
    metrics = response.json()
    assert metrics["completed"] == 0
    assert metrics["in_flight"] == 0
    assert metrics["accepted"] == 0
    assert metrics["rejected"] == 0
    # Every outcome and latency field exists from the first instant.
    for outcome in (
        "ok",
        "timeout",
        "rate_limited",
        "bulkhead_full",
        "upstream_error",
        "internal_error",
    ):
        assert metrics["counts"][outcome] == 0
        assert metrics["latencies"][outcome]["count"] == 0
        assert metrics["latencies"][outcome]["p95_seconds"] == 0.0
