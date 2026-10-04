"""Per-target isolation: one slow downstream must not drag a healthy one down.

Topology: two independent dependency servers (distinct origins = distinct
targets) plus the hardened service under test. The "slow" target answers in
2s (beyond its 0.5s deadline); the "fast" target answers in ~20ms.

Asserts:
* the slow target only consumes its own (small) permit share; the fast target
  keeps its success rate and latency while the slow one is saturated;
* target-level rejection is a distinct, stable result, separable from target
  timeout / downstream error / connection failure / internal error;
* permits are recycled across repeated waves (no distribution drift);
* target + global limits hold across multiple worker processes;
* /metrics exposes a per-target breakdown (counts + latency stats).
"""

from __future__ import annotations

import asyncio
import json
import time
import urllib.parse

import httpx
from conftest import FloodResult, flood

SLOW_DELAY = 2.0
SLOW_TIMEOUT = 0.5
SLOW_LIMIT = 4
FAST_DELAY = 0.02
GLOBAL_LIMIT = 32


def _data_url(base_url: str, dep_url: str) -> str:
    return f"{base_url}/data?target=" + urllib.parse.quote(dep_url, safe="")


def _flood_mixed(title: str, groups: list[tuple[str, str, int]], timeout: float = 15.0):
    """Fire several (label, url, count) groups concurrently; report per label."""

    async def run():
        per_label: dict[str, dict] = {
            label: {"lat": [], "status": {}, "outcomes": {}}
            for label, _, _ in groups
        }

        async def one(client: httpx.AsyncClient, label: str, url: str) -> None:
            started = time.perf_counter()
            try:
                resp = await client.get(url)
                dt = time.perf_counter() - started
                code = resp.status_code
                try:
                    outcome = resp.json().get("outcome", "no_outcome")
                except ValueError:
                    outcome = "non_json"
            except Exception as exc:  # noqa: BLE001
                dt = time.perf_counter() - started
                code = -1
                outcome = f"client_error:{type(exc).__name__}"
            bucket = per_label[label]
            bucket["lat"].append(dt)
            bucket["status"][code] = bucket["status"].get(code, 0) + 1
            bucket["outcomes"][outcome] = bucket["outcomes"].get(outcome, 0) + 1

        total = sum(count for _, _, count in groups)
        limits = httpx.Limits(max_connections=total, max_keepalive_connections=total)
        async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
            await asyncio.gather(*(
                one(client, label, url)
                for label, url, count in groups
                for _ in range(count)
            ))
        return per_label

    started = time.perf_counter()
    per_label = asyncio.run(run())
    wall = time.perf_counter() - started
    results = {}
    for label, data in per_label.items():
        res = FloodResult(
            f"{title}/{label}", len(data["lat"]), wall,
            data["status"], data["outcomes"], data["lat"],
        )
        res.report()
        results[label] = res
    return results


def _spawn_topology(server_factory, name, *, workers=1, slow_limit=SLOW_LIMIT,
                    global_limit=GLOBAL_LIMIT):
    dep_slow = server_factory(f"{name}-dep-slow")
    dep_fast = server_factory(f"{name}-dep-fast")
    overrides = {
        f"127.0.0.1:{dep_slow.port}": {
            "max_inflight": slow_limit,
            "timeout_sec": SLOW_TIMEOUT,
        }
    }
    main = server_factory(
        f"{name}-main",
        workers=workers,
        env_extra={
            "DOWNSTREAM_MAX_INFLIGHT": str(global_limit),
            "DOWNSTREAM_TIMEOUT_SEC": "1.0",
            "TARGET_OVERRIDES": json.dumps(overrides),
            "RATE_LIMIT_CAPACITY": "1000000",
            "RATE_LIMIT_REFILL_PER_SEC": "1000000",
        },
    )
    slow_url = _data_url(
        main.base_url, f"{dep_slow.base_url}/internal/downstream?delay={SLOW_DELAY}"
    )
    fast_url = _data_url(
        main.base_url, f"{dep_fast.base_url}/internal/downstream?delay={FAST_DELAY}"
    )
    return main, dep_slow, dep_fast, slow_url, fast_url


def test_slow_target_does_not_starve_fast_target(server_factory):
    main, dep_slow, dep_fast, slow_url, fast_url = _spawn_topology(
        server_factory, "isolation", global_limit=64
    )
    n_slow, n_fast = 32, 32
    results = _flood_mixed(
        "isolation", [("slow", slow_url, n_slow), ("fast", fast_url, n_fast)]
    )
    slow, fast = results["slow"], results["fast"]

    # Slow target: only its own share (SLOW_LIMIT) waits and times out; the
    # rest fail fast with the DISTINCT target-level rejection.
    slow_timeouts = slow.status_counts.get(504, 0)
    slow_rejected = slow.outcomes.get("target_rejected", 0)
    assert abs(slow_timeouts - SLOW_LIMIT) <= 1, slow.report()
    assert abs(slow_rejected - (n_slow - SLOW_LIMIT)) <= 1, slow.report()
    assert slow.outcomes.get("timeout") == slow_timeouts
    # Target rejection must not be misclassified as global bulkhead / 429.
    assert slow.outcomes.get("bulkhead_rejected", 0) == 0
    assert slow.status_counts.get(429, 0) == 0

    # Fast target: essentially untouched by the slow target's saturation.
    assert fast.status_counts.get(200, 0) == n_fast, fast.report()
    assert fast.outcomes.get("success") == n_fast
    assert fast.percentile(0.99) < SLOW_TIMEOUT, fast.report()
    assert -1 not in fast.status_counts and -1 not in slow.status_counts

    # Per-target metrics: each target's concurrency outcome mix is visible.
    snap = httpx.get(f"{main.base_url}/metrics", timeout=5.0).json()
    slow_key = f"127.0.0.1:{dep_slow.port}"
    fast_key = f"127.0.0.1:{dep_fast.port}"
    print("\n[metrics.targets]", json.dumps(snap["targets"], indent=2, sort_keys=True))
    assert snap["targets"][slow_key]["timeout"]["count"] == slow_timeouts
    assert snap["targets"][slow_key]["target_rejected"]["count"] == slow_rejected
    assert snap["targets"][fast_key]["success"]["count"] == n_fast
    # Latency stats per target: slow timeouts pinned near the deadline, fast
    # target near its real dependency latency.
    assert snap["targets"][slow_key]["timeout"]["latency_max_sec"] < 1.0
    assert snap["targets"][fast_key]["success"]["latency_mean_sec"] < 0.5


def test_target_results_are_distinguishable(server_factory):
    main, dep_slow, dep_fast, slow_url, fast_url = _spawn_topology(
        server_factory, "distinct", global_limit=64
    )
    base = main.base_url
    with httpx.Client(timeout=10.0) as client:
        # success on the fast target
        r = client.get(fast_url)
        assert r.status_code == 200 and r.json()["outcome"] == "success"
        # target's own timeout (slow target, deadline 0.5s < delay 2s)
        r = client.get(slow_url)
        assert r.status_code == 504 and r.json()["outcome"] == "timeout"
        # downstream actively returns an error -> downstream_error (502)
        r = client.get(_data_url(
            base, f"{dep_fast.base_url}/internal/downstream?fail=true"))
        assert r.status_code == 502 and r.json()["outcome"] == "internal_error"
        assert r.json()["error"] == "downstream_error"
        # connection failure (closed port) -> downstream_error (502), fast
        r = client.get(_data_url(base, "http://127.0.0.1:1/nope"))
        assert r.status_code == 502 and r.json()["error"] == "downstream_error"

    # target-level rejection: saturate the slow target, then one more.
    async def saturate():
        limits = httpx.Limits(max_connections=8, max_keepalive_connections=8)
        async with httpx.AsyncClient(timeout=10.0, limits=limits) as client:
            return await asyncio.gather(*(
                client.get(slow_url) for _ in range(SLOW_LIMIT + 3)
            ))

    responses = asyncio.run(saturate())
    bodies = [r.json() for r in responses]
    rejected = [b for b in bodies if b["outcome"] == "target_rejected"]
    assert len(rejected) >= 2, bodies
    for body in rejected:
        assert body["error"] == "target_bulkhead_rejected"
        assert body["target"] == f"127.0.0.1:{dep_slow.port}"
    # ...and it differs from timeout / downstream_error / rate_limited bodies.
    assert {b["outcome"] for b in bodies} == {"timeout", "target_rejected"}


def test_capacity_is_recycled_across_waves(server_factory):
    main, _ds, _df, slow_url, fast_url = _spawn_topology(
        server_factory, "recycle", global_limit=64
    )
    waves = []
    for wave in range(3):
        results = _flood_mixed(
            f"recycle-wave{wave}",
            [("slow", slow_url, 16), ("fast", fast_url, 16)],
        )
        waves.append(results)
        # Between waves every permit must be back: inflight fully drained.
        snap = httpx.get(f"{main.base_url}/metrics", timeout=5.0).json()
        assert snap["total"] >= 32 * (wave + 1)

    # No distribution drift: each wave shows the same split and no client-side
    # errors (which would indicate leaked connections/permits).
    for wave, results in enumerate(waves):
        slow, fast = results["slow"], results["fast"]
        assert abs(slow.status_counts.get(504, 0) - SLOW_LIMIT) <= 1, slow.report()
        assert abs(
            slow.outcomes.get("target_rejected", 0) - (16 - SLOW_LIMIT)
        ) <= 1, slow.report()
        assert fast.status_counts.get(200, 0) == 16, fast.report()
        assert -1 not in slow.status_counts and -1 not in fast.status_counts


def test_limits_are_consistent_across_worker_processes(server_factory):
    main, _ds, dep_fast, slow_url, _fast_url = _spawn_topology(
        server_factory, "multiproc", workers=2, slow_limit=6
    )
    # Per-target limit (6) is shared by both workers: 24 concurrent requests
    # to the slow target -> ~6 timeouts, ~18 target rejections, no over-admit.
    result = flood("multiproc/slow-target", slow_url, 24, timeout=15.0)
    timeouts = result.status_counts.get(504, 0)
    rejected = result.outcomes.get("target_rejected", 0)
    assert abs(timeouts - 6) <= 1, result.report()
    assert abs(rejected - 18) <= 1, result.report()
    assert result.outcomes.get("bulkhead_rejected", 0) == 0

    # Global gate is the final breaker across workers: the fast target's
    # default share equals the global limit (32); make it slow too and fire
    # 40 -> ~32 admitted (timeout at the default 1s deadline), ~8 rejected
    # by the GLOBAL gate (distinct outcome).
    slow_fast_url = _data_url(
        main.base_url, f"{dep_fast.base_url}/internal/downstream?delay={SLOW_DELAY}"
    )
    second = flood("multiproc/global-gate", slow_fast_url, 40, timeout=15.0)
    g_timeouts = second.status_counts.get(504, 0)
    g_rejected = second.outcomes.get("bulkhead_rejected", 0)
    assert abs(g_timeouts - GLOBAL_LIMIT) <= 2, second.report()
    assert abs(g_rejected - (40 - GLOBAL_LIMIT)) <= 2, second.report()
    assert second.outcomes.get("target_rejected", 0) == 0
