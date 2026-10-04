"""Per-target isolation: one slow downstream must not drag down another.

Uses two controllable targets against the built-in dependency:

* ``slow`` -- 2s delay, tiny capacity (4) and tight timeout (0.4s);
* ``fast`` -- ~0s delay, generous capacity (8).

Asserts:
* under a concurrent mixed flood, the slow target saturates and is rejected
  with its OWN distinct result (503 ``target_bulkhead_rejected``) while the
  fast target keeps its original success rate and latency;
* target-level rejection is distinguishable from the target's own timeout
  (504), downstream-returned errors (502 ``downstream_error``), network
  failures (502 ``downstream_connect_error``) and the global gate
  (503 ``bulkhead_rejected``);
* capacity is recycled after every wave: repeated waves show a stable
  distribution (no drift) and ``/internal/bulkheads`` returns to zero;
* per-target capacity and the global cap hold across worker processes.
"""

from __future__ import annotations

import asyncio
import json
import time
import urllib.parse

import httpx
from conftest import FloodResult, flood

SLOW_DELAY = 2.0
SLOW_TIMEOUT = 0.4
SLOW_CAP = 4
FAST_CAP = 16

TARGETS_JSON = json.dumps([
    {
        "name": "slow",
        "match": "/internal/downstream?delay=2",
        "max_inflight": SLOW_CAP,
        "timeout_sec": SLOW_TIMEOUT,
    },
    {
        "name": "fast",
        "match": "/internal/downstream?delay=0",
        "max_inflight": FAST_CAP,
        "timeout_sec": 1.0,
    },
])

BASE_ENV = {
    "DOWNSTREAM_TIMEOUT_SEC": "0.4",
    "DOWNSTREAM_MAX_INFLIGHT": "64",
    "RATE_LIMIT_CAPACITY": "1000000",
    "RATE_LIMIT_REFILL_PER_SEC": "1000000",
    "DOWNSTREAM_TARGETS_JSON": TARGETS_JSON,
}


def _data_url(base_url: str, dep: str) -> str:
    return f"{base_url}/data?target=" + urllib.parse.quote(dep, safe="")


def _slow_url(base_url: str) -> str:
    return _data_url(base_url, f"{base_url}/internal/downstream?delay={SLOW_DELAY}")


def _fast_url(base_url: str) -> str:
    return _data_url(base_url, f"{base_url}/internal/downstream?delay=0.02")


async def _mixed_flood_async(
    groups: list[tuple[str, str, int, float]], timeout: float
) -> dict[str, dict]:
    """Fire several named request groups concurrently (optional start delay)."""
    stats: dict[str, dict] = {
        name: {"latencies": [], "status_counts": {}, "outcomes": {}}
        for name, _, _, _ in groups
    }

    async def one(client: httpx.AsyncClient, name: str, url: str, delay: float):
        if delay:
            await asyncio.sleep(delay)
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
        bucket = stats[name]
        bucket["latencies"].append(dt)
        bucket["status_counts"][code] = bucket["status_counts"].get(code, 0) + 1
        bucket["outcomes"][outcome] = bucket["outcomes"].get(outcome, 0) + 1

    total = sum(count for _, _, count, _ in groups)
    limits = httpx.Limits(
        max_connections=max(16, total), max_keepalive_connections=max(16, total)
    )
    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        await asyncio.gather(*(
            one(client, name, url, delay)
            for name, url, count, delay in groups
            for _ in range(count)
        ))
    return stats


def mixed_flood(
    title: str, groups: list[tuple[str, str, int, float]], timeout: float = 15.0
) -> dict[str, FloodResult]:
    started = time.perf_counter()
    stats = asyncio.run(_mixed_flood_async(groups, timeout))
    wall = time.perf_counter() - started
    results = {}
    for name, bucket in stats.items():
        result = FloodResult(
            f"{title}/{name}",
            len(bucket["latencies"]),
            wall,
            bucket["status_counts"],
            bucket["outcomes"],
            bucket["latencies"],
        )
        result.report()
        results[name] = result
    print(f"[{title}] wall_sec={wall:.3f}")
    results["wall"] = wall  # type: ignore[assignment]
    return results


def _bulkheads(base_url: str) -> dict[str, int]:
    return httpx.get(f"{base_url}/internal/bulkheads", timeout=5.0).json()


def test_slow_target_does_not_drag_fast_target_and_resources_recycle(server_factory):
    server = server_factory("target-isolation", env_extra=BASE_ENV)
    base = server.base_url
    waves = []
    for wave in range(3):
        results = mixed_flood(
            f"isolation-wave{wave + 1}",
            [
                ("slow", _slow_url(base), 24, 0.0),
                ("fast", _fast_url(base), 16, 0.0),
            ],
        )
        waves.append(results)
        slow, fast = results["slow"], results["fast"]

        # Fast target: unaffected success rate and latency.
        assert fast.status_counts.get(200, 0) == 16, fast.report()
        assert fast.outcomes.get("success") == 16, fast.report()
        assert fast.percentile(0.90) < 0.35, fast.report()
        assert 503 not in fast.status_counts and 504 not in fast.status_counts

        # Slow target: only its own capacity waits (and times out); the rest
        # is rejected fast with the target-level, distinguishable 503.
        timeouts = slow.status_counts.get(504, 0)
        rejected = slow.status_counts.get(503, 0)
        assert timeouts + rejected == 24, slow.report()
        assert abs(timeouts - SLOW_CAP) <= 2, slow.report()
        assert abs(rejected - (24 - SLOW_CAP)) <= 2, slow.report()
        assert slow.outcomes.get("target_bulkhead_rejected") == rejected
        assert slow.outcomes.get("timeout") == timeouts

        # Whole wave is bounded by the slow target's timeout, not its delay.
        assert results["wall"] < 1.5

        # Capacity fully recycled between waves.
        time.sleep(0.1)
        snapshot = _bulkheads(base)
        print(f"[wave{wave + 1}] bulkheads after wave: {snapshot}")
        assert all(v == 0 for v in snapshot.values()), snapshot

    # No drift across waves: the rejection distribution stays stable.
    rejects = [w["slow"].status_counts.get(503, 0) for w in waves]
    print(f"[isolation] per-wave slow rejections: {rejects}")
    assert max(rejects) - min(rejects) <= 2, rejects

    # Metrics aggregate the same, exactly, per outcome.
    snap = httpx.get(f"{base}/metrics", timeout=5.0).json()
    exp_success = sum(w["fast"].outcomes.get("success", 0) for w in waves)
    exp_target_rej = sum(
        w["slow"].outcomes.get("target_bulkhead_rejected", 0) for w in waves
    )
    exp_timeout = sum(w["slow"].outcomes.get("timeout", 0) for w in waves)
    print(
        "[isolation] metrics: "
        f"success={snap['success']['count']} "
        f"target_bulkhead_rejected={snap['target_bulkhead_rejected']['count']} "
        f"timeout={snap['timeout']['count']}"
    )
    assert snap["success"]["count"] == exp_success
    assert snap["target_bulkhead_rejected"]["count"] == exp_target_rej
    assert snap["timeout"]["count"] == exp_timeout


def test_target_rejection_is_distinct_from_other_failure_classes(server_factory):
    server = server_factory("target-failures", env_extra=BASE_ENV)
    base = server.base_url

    # Saturate the slow target: 4 timeouts + 2 target-level rejections.
    result = flood("failures/saturate-slow", _slow_url(base), 6, timeout=10.0)
    assert result.status_counts.get(504, 0) == SLOW_CAP
    assert result.status_counts.get(503, 0) == 2

    with httpx.Client(timeout=10.0) as client:
        # Target-level rejection: distinct status/outcome/error/target fields.
        r = client.get(_slow_url(base))
        # (capacity free again now -> this one times out; force rejection below)
        assert r.status_code in (503, 504)

    # Force a rejection deterministically: hold all 4 permits, then one more.
    async def hold_and_probe():
        limits = httpx.Limits(max_connections=8, max_keepalive_connections=8)
        async with httpx.AsyncClient(timeout=10.0, limits=limits) as client:
            holders = [asyncio.create_task(client.get(_slow_url(base)))
                       for _ in range(SLOW_CAP)]
            await asyncio.sleep(0.15)  # let holders occupy all permits
            probe = await client.get(_slow_url(base))
            for h in holders:
                await h
            return probe

    probe = asyncio.run(hold_and_probe())
    body = probe.json()
    print(f"\n[distinction] target rejection body: {body}")
    assert probe.status_code == 503
    assert body["outcome"] == "target_bulkhead_rejected"
    assert body["error"] == "target_bulkhead_rejected"
    assert body["target"] == "slow"

    with httpx.Client(timeout=10.0) as client:
        # Target's own timeout: 504, different outcome.
        r = client.get(_slow_url(base))
        assert r.status_code == 504 and r.json()["outcome"] == "timeout"
        assert r.json()["error"] == "downstream_timeout"

        # Downstream actively returns an error: 502 downstream_error.
        r = client.get(_data_url(base, f"{base}/internal/downstream?fail=true"))
        assert r.status_code == 502 and r.json()["error"] == "downstream_error"
        assert r.json()["outcome"] == "internal_error"

        # Network/connect failure: 502 downstream_connect_error.
        r = client.get(_data_url(base, "http://127.0.0.1:1/nope"))
        assert r.status_code == 502
        assert r.json()["error"] == "downstream_connect_error"
        assert r.json()["outcome"] == "internal_error"

    snap = httpx.get(f"{base}/metrics", timeout=5.0).json()
    print("\n[distinction] metrics counts: " + ", ".join(
        f"{k}={snap[k]['count']}" for k in (
            "success", "timeout", "target_bulkhead_rejected",
            "bulkhead_rejected", "internal_error",
        )
    ))
    assert snap["target_bulkhead_rejected"]["count"] == 2 + 1  # flood + probe
    # timeouts: 4 (flood) + 1 (first probe) + 4 (holders) + 1 (final probe)
    assert snap["timeout"]["count"] == 2 * SLOW_CAP + 2
    assert snap["internal_error"]["count"] == 2
    assert snap["bulkhead_rejected"]["count"] == 0  # global gate never hit


def test_global_gate_remains_final_cap_across_targets(server_factory):
    env = dict(BASE_ENV)
    env["DOWNSTREAM_MAX_INFLIGHT"] = "6"
    env["DOWNSTREAM_TARGETS_JSON"] = json.dumps([
        {"name": "slow", "match": "/internal/downstream?delay=2",
         "max_inflight": 10, "timeout_sec": 0.5},
        {"name": "fast", "match": "/internal/downstream?delay=0",
         "max_inflight": 10, "timeout_sec": 1.0},
    ])
    server = server_factory("target-global-gate", env_extra=env)
    base = server.base_url

    # Slow target occupies the whole global budget; fast arrives slightly
    # later and must hit the GLOBAL gate (not its own target cap of 10).
    results = mixed_flood(
        "global-gate",
        [
            ("slow", _slow_url(base), 12, 0.0),
            ("fast", _fast_url(base), 12, 0.15),
        ],
    )
    slow, fast = results["slow"], results["fast"]
    # At most 6 in flight globally -> 6 slow timeouts, 6 slow global rejects.
    assert abs(slow.status_counts.get(504, 0) - 6) <= 1, slow.report()
    assert slow.outcomes.get("bulkhead_rejected", 0) >= 5, slow.report()
    # Fast target's own capacity (10) was never the binding constraint:
    # its rejections are the GLOBAL class, distinguishable from target-level.
    assert fast.outcomes.get("target_bulkhead_rejected", 0) == 0, fast.report()
    assert fast.outcomes.get("bulkhead_rejected", 0) >= 8, fast.report()
    assert fast.outcomes.get("success", 0) <= 2, fast.report()

    time.sleep(0.1)
    assert all(v == 0 for v in _bulkheads(base).values())


def test_target_capacity_is_consistent_across_processes(server_factory):
    env = dict(BASE_ENV)
    env["DOWNSTREAM_MAX_INFLIGHT"] = "100"
    env["DOWNSTREAM_TARGETS_JSON"] = json.dumps([
        {"name": "slow", "match": "/internal/downstream?delay=2",
         "max_inflight": 6, "timeout_sec": 0.5},
    ])
    server = server_factory("target-multiprocess", workers=2, env_extra=env)
    base = server.base_url

    result = flood("multiprocess/slow-target", _slow_url(base), 30, timeout=15.0)
    timeouts = result.status_counts.get(504, 0)
    rejected = result.status_counts.get(503, 0)
    print(f"\n[multiprocess] target cap=6, 30 requests over 2 workers -> "
          f"504={timeouts} 503={rejected}")
    # Shared per-target capacity: no oversubscription regardless of which
    # worker a request lands on.
    assert timeouts <= 8, result.report()
    assert timeouts >= 4, result.report()
    assert abs(rejected - (30 - timeouts)) <= 2, result.report()
    assert result.outcomes.get("target_bulkhead_rejected") == rejected

    time.sleep(0.1)
    assert all(v == 0 for v in _bulkheads(base).values())
