"""Admission-control scenarios.

* burst over capacity -> exactly `capacity` admits, rest 429 (distinct);
* normal below-rate traffic is never rejected (no collateral damage);
* tokens refill over time so capacity recovers;
* rate-limit state is shared across worker processes (SQLite).
"""

from __future__ import annotations

import asyncio
import time
import urllib.parse

import httpx

from conftest import flood


def _data_url(base_url: str, delay: float = 0.0) -> str:
    dep = f"{base_url}/internal/downstream?delay={delay}"
    return f"{base_url}/data?target=" + urllib.parse.quote(dep, safe="")


def test_burst_is_admitted_up_to_capacity_then_distinctly_rejected(server_factory):
    capacity = 10
    server = server_factory(
        "ratelimit-burst",
        env_extra={
            "RATE_LIMIT_CAPACITY": str(capacity),
            "RATE_LIMIT_REFILL_PER_SEC": "0.01",
            "DOWNSTREAM_MAX_INFLIGHT": "100",
        },
    )
    total = 20
    result = flood("ratelimit/burst", _data_url(server.base_url), total)

    assert result.status_counts.get(200) == capacity, result.report()
    assert result.status_counts.get(429) == total - capacity, result.report()
    assert result.outcomes.get("success") == capacity
    assert result.outcomes.get("rate_limited") == total - capacity


def test_sustained_below_rate_traffic_is_never_rejected(server_factory):
    server = server_factory(
        "ratelimit-sustained",
        env_extra={
            "RATE_LIMIT_CAPACITY": "5",
            "RATE_LIMIT_REFILL_PER_SEC": "20",  # > send rate of 10/s
            "DOWNSTREAM_MAX_INFLIGHT": "100",
        },
    )

    statuses: list[int] = []
    started = time.perf_counter()
    with httpx.Client(timeout=5.0) as client:
        for _ in range(20):
            statuses.append(client.get(_data_url(server.base_url)).status_code)
            time.sleep(0.1)  # 10 req/s, well under the refill rate
    wall = time.perf_counter() - started
    rejected = statuses.count(429)
    admitted = statuses.count(200)
    print(f"\n[sustained] concurrency=1, requests=20 over {wall:.1f}s "
          f"(10 req/s < 20 tokens/s) -> 200={admitted} 429={rejected}")
    assert rejected == 0, "normal traffic must not be rate limited"
    assert admitted == 20


def test_bucket_refills_and_recovers_capacity(server_factory):
    capacity = 10
    refill = 5.0
    server = server_factory(
        "ratelimit-refill",
        env_extra={
            "RATE_LIMIT_CAPACITY": str(capacity),
            "RATE_LIMIT_REFILL_PER_SEC": str(refill),
            "DOWNSTREAM_MAX_INFLIGHT": "100",
        },
    )
    first = flood("ratelimit/exhaust", _data_url(server.base_url), capacity + 5)
    assert first.status_counts.get(200) == capacity

    time.sleep(1.05)  # ~5 tokens refilled
    second = flood("ratelimit/refilled", _data_url(server.base_url), 6)
    print(f"[refill] after 1.05s @ {refill}/s -> {second.status_counts}")
    assert second.status_counts.get(200) == 5, second.report()
    assert second.status_counts.get(429) == 1, second.report()


def test_rate_limit_is_consistent_across_processes(server_factory):
    capacity = 12
    server = server_factory(
        "ratelimit-multiprocess",
        workers=2,
        env_extra={
            "RATE_LIMIT_CAPACITY": str(capacity),
            "RATE_LIMIT_REFILL_PER_SEC": "0.01",
            "DOWNSTREAM_MAX_INFLIGHT": "100",
        },
    )

    async def hit():
        limits = httpx.Limits(max_connections=40, max_keepalive_connections=40)
        async with httpx.AsyncClient(timeout=10.0, limits=limits) as client:
            return await asyncio.gather(*(
                client.get(_data_url(server.base_url)) for _ in range(2 * capacity)
            ))

    responses = asyncio.run(hit())
    codes = sorted(r.status_code for r in responses)
    admitted = codes.count(200)
    rejected = codes.count(429)
    print(f"\n[multiprocess workers=2] total=24 capacity=12 -> "
          f"200={admitted} 429={rejected}")
    assert admitted == capacity, "shared bucket across processes must not over-admit"
    assert rejected == capacity
