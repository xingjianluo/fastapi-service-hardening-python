"""Logical downstreams: same origin, different endpoints + failure taxonomy.

Topology is fully local/offline: ONE dependency process exposes two logical
downstreams that share the same host:port and differ only by path --

* slow endpoint: ``/internal/downstream?delay=2`` (answers in 2s, deadline 0.4s)
* fast endpoint: ``/`` (answers immediately)

plus a guaranteed-refused port (127.0.0.1:1) for transport-level failures.

Asserts:
* endpoints on the SAME origin are isolated: the slow endpoint only consumes
  its own (small) permit share; the fast endpoint keeps its success rate and
  latency while the slow one is saturated;
* transport failure (connect_failed) is stably distinguishable from the
  downstream actively returning an error status (downstream_error), and both
  from timeout / target_rejected / internal_error;
* per-target metrics attribute every outcome to its own logical target;
* permits are recycled across repeated waves of mixed outcomes (success,
  timeout, rejection, connect failure) with no distribution drift.
"""

from __future__ import annotations

import asyncio
import json
import urllib.parse

import httpx
from conftest import flood_mixed

SLOW_DELAY = 2.0
SLOW_TIMEOUT = 0.4
SLOW_LIMIT = 2
FAST_LIMIT = 16
GLOBAL_LIMIT = 64
# Port 1 (tcpmux) requires root to bind; nothing listens there in tests, so
# connecting fails fast and deterministically with ECONNREFUSED -- offline.
UNREACHABLE_TARGET = "http://127.0.0.1:1/nope"


def _data_url(base_url: str, dep_url: str) -> str:
    return f"{base_url}/data?target=" + urllib.parse.quote(dep_url, safe="")


def _spawn_same_origin_topology(server_factory, name: str):
    """One dependency, two logical downstreams (paths) + the hardened service."""
    dep = server_factory(f"{name}-dep")
    overrides = {
        # Endpoint-precise rule: only the slow path gets the tiny share.
        f"127.0.0.1:{dep.port}/internal/downstream": {
            "max_inflight": SLOW_LIMIT,
            "timeout_sec": SLOW_TIMEOUT,
        }
    }
    main = server_factory(
        f"{name}-main",
        env_extra={
            "DOWNSTREAM_MAX_INFLIGHT": str(GLOBAL_LIMIT),
            "DOWNSTREAM_TIMEOUT_SEC": "1.0",
            "TARGET_DEFAULT_MAX_INFLIGHT": str(FAST_LIMIT),
            "TARGET_OVERRIDES": json.dumps(overrides),
            "RATE_LIMIT_CAPACITY": "1000000",
            "RATE_LIMIT_REFILL_PER_SEC": "1000000",
        },
    )
    slow_url = _data_url(
        main.base_url, f"{dep.base_url}/internal/downstream?delay={SLOW_DELAY}"
    )
    # Same origin, different endpoint: the dependency's root path answers at
    # once. Its target key is the bare authority (path "/" is normalized away).
    fast_url = _data_url(main.base_url, f"{dep.base_url}/")
    broken_url = _data_url(main.base_url, UNREACHABLE_TARGET)
    keys = {
        "slow": f"127.0.0.1:{dep.port}/internal/downstream",
        "fast": f"127.0.0.1:{dep.port}",
        "broken": "127.0.0.1:1/nope",
    }
    return main, slow_url, fast_url, broken_url, keys


def _print_targets(main, title: str) -> dict:
    snap = httpx.get(f"{main.base_url}/metrics", timeout=5.0).json()
    print(f"\n[metrics.targets/{title}]",
          json.dumps(snap["targets"], indent=2, sort_keys=True))
    return snap


def test_same_origin_endpoints_have_independent_capacity(server_factory):
    main, slow_url, fast_url, _broken, keys = _spawn_same_origin_topology(
        server_factory, "sameorigin"
    )
    n_slow, n_fast = 16, 16
    results = flood_mixed(
        "sameorigin", [("slow", slow_url, n_slow), ("fast", fast_url, n_fast)]
    )
    slow, fast = results["slow"], results["fast"]

    # Slow endpoint: only its own tiny share waits and times out; the rest
    # fail fast with the DISTINCT target-level rejection.
    slow_timeouts = slow.status_counts.get(504, 0)
    slow_rejected = slow.outcomes.get("target_rejected", 0)
    assert abs(slow_timeouts - SLOW_LIMIT) <= 1, slow.report()
    assert abs(slow_rejected - (n_slow - SLOW_LIMIT)) <= 1, slow.report()
    assert slow.outcomes.get("bulkhead_rejected", 0) == 0

    # Fast endpoint on the SAME host:port: untouched by the slow endpoint.
    assert fast.status_counts.get(200, 0) == n_fast, fast.report()
    assert fast.outcomes.get("success") == n_fast
    assert fast.percentile(0.99) < SLOW_TIMEOUT, fast.report()
    assert -1 not in fast.status_counts and -1 not in slow.status_counts

    # Per-target metrics: the two endpoints on one origin are accounted
    # separately, each under its own authority+path key.
    snap = _print_targets(main, "sameorigin")
    assert snap["targets"][keys["slow"]]["timeout"]["count"] == slow_timeouts
    assert snap["targets"][keys["slow"]]["target_rejected"]["count"] == slow_rejected
    assert snap["targets"][keys["fast"]]["success"]["count"] == n_fast
    # The fast endpoint's key carries no path (root normalized away) and is
    # NOT merged into the slow endpoint's key.
    assert keys["fast"] != keys["slow"]


def test_network_failure_vs_downstream_error_are_distinguishable(server_factory):
    main, slow_url, fast_url, broken_url, keys = _spawn_same_origin_topology(
        server_factory, "taxonomy"
    )
    base = main.base_url
    seen: dict[str, dict] = {}
    with httpx.Client(timeout=10.0) as client:
        r = client.get(fast_url)
        assert r.status_code == 200
        seen["success"] = r.json()
        r = client.get(slow_url)
        assert r.status_code == 504
        seen["timeout"] = r.json()
        # The downstream IS reachable and actively says no (HTTP 500).
        r = client.get(_data_url(
            base, f"{main.base_url}/internal/downstream?fail=true"))
        assert r.status_code == 502
        seen["downstream_error"] = r.json()
        # Transport-level failure: nothing listens on the port.
        r = client.get(broken_url)
        assert r.status_code == 502
        seen["connect_failed"] = r.json()

    # The two 502 classes carry different, stable machine-readable identities.
    assert seen["connect_failed"]["outcome"] == "connect_failed"
    assert seen["connect_failed"]["error"] == "downstream_unreachable"
    assert seen["downstream_error"]["outcome"] == "downstream_error"
    assert seen["downstream_error"]["error"] == "downstream_error"
    assert seen["downstream_error"]["downstream_status_code"] == 500
    # ...and every class is distinct from every other class.
    outcomes = {k: b["outcome"] for k, b in seen.items()}
    assert len(set(outcomes.values())) == len(outcomes), outcomes
    errors = {b["error"] for b in seen.values() if "error" in b}
    assert len(errors) == 3, errors  # timeout/unreachable/downstream_error

    # target_rejected: saturate the slow endpoint, then one more wave.
    async def saturate():
        limits = httpx.Limits(max_connections=8, max_keepalive_connections=8)
        async with httpx.AsyncClient(timeout=10.0, limits=limits) as client:
            return await asyncio.gather(*(
                client.get(slow_url) for _ in range(SLOW_LIMIT + 3)
            ))

    bodies = [r.json() for r in asyncio.run(saturate())]
    rejected = [b for b in bodies if b["outcome"] == "target_rejected"]
    assert len(rejected) >= 2, bodies
    assert all(b["error"] == "target_bulkhead_rejected" for b in rejected)
    assert all(b["target"] == keys["slow"] for b in rejected)

    # Per-target attribution: each failure class lands on ITS OWN target key.
    snap = _print_targets(main, "taxonomy")
    assert snap["targets"][keys["broken"]]["connect_failed"]["count"] == 1
    downstream_error_key = f"127.0.0.1:{main.port}/internal/downstream"
    assert snap["targets"][downstream_error_key]["downstream_error"]["count"] == 1
    assert snap["targets"][keys["slow"]]["timeout"]["count"] >= 1
    assert snap["targets"][keys["slow"]]["target_rejected"]["count"] == len(rejected)
    assert snap["targets"][keys["fast"]]["success"]["count"] == 1
    # No internal_error anywhere: every failure above is a classified one.
    assert snap["internal_error"]["count"] == 0


def test_capacity_recycled_across_waves_with_mixed_failures(server_factory):
    main, slow_url, fast_url, broken_url, keys = _spawn_same_origin_topology(
        server_factory, "recycle-mixed"
    )
    per_wave = {"slow": 8, "fast": 8, "broken": 4}
    waves = []
    for wave in range(3):
        results = flood_mixed(
            f"recycle-mixed-wave{wave}",
            [("slow", slow_url, per_wave["slow"]),
             ("fast", fast_url, per_wave["fast"]),
             ("broken", broken_url, per_wave["broken"])],
        )
        waves.append(results)

    # No distribution drift across waves: identical split every time, and no
    # client-side errors (which would indicate leaked permits/connections).
    for wave, results in enumerate(waves):
        slow, fast, broken = results["slow"], results["fast"], results["broken"]
        assert abs(slow.status_counts.get(504, 0) - SLOW_LIMIT) <= 1, slow.report()
        assert abs(
            slow.outcomes.get("target_rejected", 0) - (per_wave["slow"] - SLOW_LIMIT)
        ) <= 1, slow.report()
        assert fast.status_counts.get(200, 0) == per_wave["fast"], fast.report()
        assert broken.outcomes.get("connect_failed") == per_wave["broken"], \
            broken.report()
        for res in (slow, fast, broken):
            assert -1 not in res.status_counts, res.report()

    # Cumulative per-target accounting matches exactly 3 waves of traffic;
    # the global total is the sum over all targets (final safety net intact).
    snap = _print_targets(main, "recycle-mixed")
    n_waves = len(waves)
    slow_target = snap["targets"][keys["slow"]]
    assert abs(slow_target["timeout"]["count"] - n_waves * SLOW_LIMIT) <= n_waves
    assert abs(
        slow_target["target_rejected"]["count"]
        - n_waves * (per_wave["slow"] - SLOW_LIMIT)
    ) <= n_waves
    assert snap["targets"][keys["fast"]]["success"]["count"] == \
        n_waves * per_wave["fast"]
    assert snap["targets"][keys["broken"]]["connect_failed"]["count"] == \
        n_waves * per_wave["broken"]
    per_target_total = sum(
        stats["count"]
        for target in snap["targets"].values()
        for stats in target.values()
    )
    assert per_target_total == snap["total"]
