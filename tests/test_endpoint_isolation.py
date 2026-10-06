"""Offline-stable regressions for the three hardening refinements.

1. **Endpoint-level isolation**: two logical downstreams sharing the SAME
   domain and port but exposed at different paths
   (``/internal/deps/slow`` vs ``/internal/deps/fast``) get independent
   capacity and deadlines. Saturating the slow path must never slow down or
   reject calls to the fast path on the same origin.

2. **Network failure vs downstream business failure**: a refused connection
   (``downstream_unreachable``) and a live downstream's non-2xx reply
   (``downstream_error``) are distinct, machine-readable results -- and both
   stay separate from the target's timeout, its capacity rejections and the
   service's own internal errors.

3. **Capacity recycle across waves and cancellation**: permits return to the
   pool after every wave (verified directly against the cross-process permit
   table), distributions do not drift, and a request cancelled mid-flight
   releases its permit immediately.

Nothing here depends on thread scheduling timing or on the public network:
"unreachable" is a request to a closed local port, which fails immediately and
deterministically on every platform. Short awaits only let in-flight work
settle; every assertion is on exact counts, never on a scheduling race.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import urllib.parse

import httpx
from conftest import flood, free_port
from test_target_isolation import _flood_mixed

SLOW_DELAY = 2.0
SLOW_TIMEOUT = 0.4
SLOW_LIMIT = 4
FAST_DELAY = 0.02
GLOBAL_LIMIT = 64


def _data_url(base_url: str, dep_url: str) -> str:
    return f"{base_url}/data?target=" + urllib.parse.quote(dep_url, safe="")


def _spawn_endpoint_configured(server_factory, name, *, workers=1,
                               default_share=None, default_timeout=None):
    """Reserve a port first, then spawn with an endpoint-keyed slow override.

    The override key is the FULL logical endpoint
    ``http://127.0.0.1:<port>/internal/deps/slow`` (path included), so only
    that one path on the origin gets the tight capacity/deadline; the sibling
    ``.../fast`` path keeps the generous default and must stay fully healthy.
    """
    port = free_port()
    env_extra = {
        "DOWNSTREAM_MAX_INFLIGHT": str(GLOBAL_LIMIT),
        "DOWNSTREAM_TIMEOUT_SEC": "1.0",
        "TARGET_OVERRIDES": json.dumps({
            f"http://127.0.0.1:{port}/internal/deps/slow": {
                "max_inflight": SLOW_LIMIT,
                "timeout_sec": SLOW_TIMEOUT,
            },
        }),
        "RATE_LIMIT_CAPACITY": "1000000",
        "RATE_LIMIT_REFILL_PER_SEC": "1000000",
    }
    if default_share is not None:
        env_extra["TARGET_DEFAULT_MAX_INFLIGHT"] = str(default_share)
    if default_timeout is not None:
        env_extra["TARGET_DEFAULT_TIMEOUT_SEC"] = str(default_timeout)
    return server_factory(name, workers=workers, env_extra=env_extra, port=port)


def _permit_target_scopes(db_path) -> dict[str, int]:
    """Current permit counts per target scope, via a read-only connection.

    Read-only connections still see committed WAL pages and never block or
    disturb the service's writer connection.
    """
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    try:
        rows = conn.execute(
            "SELECT scope, COUNT(*) FROM inflight_permits GROUP BY scope"
        ).fetchall()
        return {scope: n for scope, n in rows if scope != "global"}
    finally:
        conn.close()


def _flood_two(title, slow_url, fast_url, n_slow, n_fast, *, timeout=15.0):
    return _flood_mixed(
        title,
        [("slow", slow_url, n_slow), ("fast", fast_url, n_fast)],
        timeout=timeout,
    )


def test_same_origin_different_paths_are_isolated(server_factory):
    main = _spawn_endpoint_configured(server_factory, "ep-isolation")
    port = main.port
    slow_url = _data_url(
        main.base_url, f"{main.base_url}/internal/deps/slow?delay={SLOW_DELAY}")
    fast_url = _data_url(
        main.base_url, f"{main.base_url}/internal/deps/fast?delay={FAST_DELAY}")

    results = _flood_two("ep-isolation", slow_url, fast_url, 32, 32)
    slow, fast = results["slow"], results["fast"]

    # The slow PATH burns only its own tiny share: 4 wait+timeout, 28 rejected
    # by THAT path's gate -- none of which touch the sibling fast path.
    slow_timeouts = slow.status_counts.get(504, 0)
    slow_rejected = slow.outcomes.get("target_rejected", 0)
    assert abs(slow_timeouts - SLOW_LIMIT) <= 1, slow.report()
    assert abs(slow_rejected - (32 - SLOW_LIMIT)) <= 1, slow.report()
    assert slow.outcomes.get("bulkhead_rejected", 0) == 0
    assert slow.status_counts.get(429, 0) == 0

    # The fast PATH on the SAME host:port is completely unaffected.
    assert fast.status_counts.get(200, 0) == 32, fast.report()
    assert fast.outcomes.get("success") == 32
    assert fast.outcomes.get("target_rejected", 0) == 0
    assert fast.percentile(0.99) < SLOW_TIMEOUT, fast.report()
    assert -1 not in slow.status_counts and -1 not in fast.status_counts

    # Metrics attribute the two mixes to two DIFFERENT target keys that share
    # an origin.
    snap = httpx.get(f"{main.base_url}/metrics", timeout=5).json()
    slow_key = f"http://127.0.0.1:{port}/internal/deps/slow"
    fast_key = f"http://127.0.0.1:{port}/internal/deps/fast"
    print("\n[same-origin endpoints]", json.dumps(
        {k: {o: v["count"] for o, v in snap["targets"][k].items() if v["count"]}
         for k in (slow_key, fast_key)}, indent=2, sort_keys=True))
    assert snap["targets"][slow_key]["timeout"]["count"] == slow_timeouts
    assert snap["targets"][slow_key]["target_rejected"]["count"] == slow_rejected
    assert snap["targets"][slow_key]["success"]["count"] == 0
    assert snap["targets"][fast_key]["success"]["count"] == 32
    assert snap["targets"][fast_key]["target_rejected"]["count"] == 0
    assert snap["targets"][fast_key]["timeout"]["count"] == 0


def test_unreachable_vs_downstream_error_are_distinct(server_factory):
    main = _spawn_endpoint_configured(server_factory, "ep-failures")
    base = main.base_url
    with httpx.Client(timeout=10.0) as client:
        # live downstream actively answers 500
        r = client.get(_data_url(base, f"{base}/internal/deps/fast?fail=true"))
        biz = r.json()
        assert r.status_code == 502
        assert biz["outcome"] == "downstream_error"
        assert biz["error"] == "downstream_error"
        assert biz["error_kind"] == "http_error_status"
        assert biz["downstream_status_code"] == 500

        # closed local port: network-level failure, immediate
        r = client.get(_data_url(base, "http://127.0.0.1:1/dead"))
        net = r.json()
        assert r.status_code == 502
        assert net["outcome"] == "downstream_unreachable"
        assert net["error"] == "downstream_unreachable"
        assert net["error_kind"] == "connection_refused"
        assert net["latency_sec"] < 1.0

        # timeout stays its own class
        r = client.get(_data_url(base, f"{base}/internal/deps/slow?delay=2"))
        tob = r.json()
        assert r.status_code == 504 and tob["outcome"] == "timeout"

        # an internal-error trigger (invalid target URL) stays 500
        r = client.get(_data_url(base, "not-a-url"))
        iob = r.json()
        assert r.status_code == 500 and iob["outcome"] == "internal_error"

    # The classes differ in machine-readable outcome; the two 502s also differ
    # in error/error_kind.
    assert {biz["outcome"], net["outcome"], tob["outcome"], iob["outcome"]} == {
        "downstream_error", "downstream_unreachable", "timeout",
        "internal_error",
    }

    snap = httpx.get(f"{base}/metrics", timeout=5).json()
    assert snap["downstream_error"]["count"] >= 1
    assert snap["downstream_unreachable"]["count"] >= 1
    assert snap["timeout"]["count"] >= 1
    assert snap["internal_error"]["count"] >= 1
    # per-target counters never cross wires
    assert snap["targets"]["http://127.0.0.1:1/dead"][
        "downstream_unreachable"]["count"] == 1
    assert snap["targets"][f"{base}/internal/deps/fast"][
        "downstream_error"]["count"] >= 1


def test_capacity_is_recycled_across_repeated_waves(server_factory, tmp_path):
    main = _spawn_endpoint_configured(server_factory, "ep-recycle")
    db = tmp_path / "ep-recycle.db"
    slow_url = _data_url(
        main.base_url, f"{main.base_url}/internal/deps/slow?delay={SLOW_DELAY}")
    fast_url = _data_url(
        main.base_url, f"{main.base_url}/internal/deps/fast?delay={FAST_DELAY}")

    waves = []
    for wave in range(4):
        results = _flood_two(
            f"ep-wave{wave}", slow_url, fast_url, 24, 24)
        waves.append(results)
        # Every response is already back when the flood returns, so every
        # permit must be released -- verified directly in the permit table.
        leaked = {s: n for s, n in _permit_target_scopes(db).items() if n > 0}
        assert leaked == {}, f"wave {wave} leaked permits: {leaked}"

    # No drift wave-over-wave: identical split, zero client-side errors.
    for wave, results in enumerate(waves):
        slow, fast = results["slow"], results["fast"]
        assert abs(slow.status_counts.get(504, 0) - SLOW_LIMIT) <= 1, slow.report()
        assert abs(slow.outcomes.get("target_rejected", 0)
                   - (24 - SLOW_LIMIT)) <= 1, slow.report()
        assert fast.status_counts.get(200, 0) == 24, fast.report()
        assert -1 not in slow.status_counts and -1 not in fast.status_counts


def test_cancelled_request_releases_its_permit(server_factory, tmp_path):
    main = _spawn_endpoint_configured(server_factory, "ep-cancel")
    db = tmp_path / "ep-cancel.db"
    url = _data_url(
        main.base_url, f"{main.base_url}/internal/deps/slow?delay={SLOW_DELAY}")

    async def hold_then_cancel():
        limits = httpx.Limits(max_connections=16, max_keepalive_connections=16)
        async with httpx.AsyncClient(timeout=10.0, limits=limits) as client:
            tasks = [asyncio.create_task(client.get(url))
                     for _ in range(SLOW_LIMIT)]
            # Let them all acquire permits and enter the in-flight downstream
            # call (0.15s << 2s dependency delay), then cancel every one.
            await asyncio.sleep(0.15)
            for t in tasks:
                t.cancel()
            for t in tasks:
                try:
                    await t
                except BaseException:  # noqa: BLE001,S110 - cancellation expected
                    pass

    asyncio.run(hold_then_cancel())

    # The disconnect must propagate to the server and run its explicit
    # ``finally`` release quickly. Poll briefly (generous, but FAR below the
    # 2s dependency delay and the 10s+ lease-recovery margin) so this verifies
    # prompt explicit release rather than lazy lease expiry.
    async def drain():
        for _ in range(100):
            if not _permit_target_scopes(db):
                return True
            await asyncio.sleep(0.02)
        return False

    assert asyncio.run(drain()), _permit_target_scopes(db)

    # A follow-up wave must admit the FULL share again, not one fewer.
    result = flood("ep-cancel/post-wave", url, SLOW_LIMIT + 8, timeout=15.0)
    assert abs(result.status_counts.get(504, 0) - SLOW_LIMIT) <= 1, result.report()
    assert abs(result.outcomes.get("target_rejected", 0) - 8) <= 1, result.report()


def test_endpoint_isolation_holds_across_worker_processes(server_factory):
    # Tight DEFAULT share for every endpoint, 2 workers. Two paths on ONE
    # origin own INDEPENDENT shares: SLOW_LIMIT slow calls occupy only the
    # slow scope, so the same number of fast calls are still all admitted --
    # were paths merged, the fast calls would be rejected.
    main = _spawn_endpoint_configured(
        server_factory, "ep-multiproc", workers=2,
        default_share=SLOW_LIMIT, default_timeout=SLOW_TIMEOUT)
    slow_url = _data_url(
        main.base_url, f"{main.base_url}/internal/deps/slow?delay={SLOW_DELAY}")
    fast_url = _data_url(
        main.base_url, f"{main.base_url}/internal/deps/fast?delay={FAST_DELAY}")

    async def run():
        limits = httpx.Limits(max_connections=32, max_keepalive_connections=32)
        async with httpx.AsyncClient(timeout=10.0, limits=limits) as client:
            slow_tasks = [asyncio.create_task(client.get(slow_url))
                          for _ in range(SLOW_LIMIT)]
            await asyncio.sleep(0.25)  # slow calls admitted + in flight
            fast_resps = await asyncio.gather(*(
                client.get(fast_url) for _ in range(SLOW_LIMIT)))
            slow_resps = await asyncio.gather(*slow_tasks, return_exceptions=True)
            return fast_resps, slow_resps

    fast_resps, slow_resps = asyncio.run(run())
    assert all(r.status_code == 200 for r in fast_resps), [
        getattr(r, "status_code", r) for r in fast_resps]
    assert sorted(getattr(r, "status_code", -1) for r in slow_resps) == \
        [504] * SLOW_LIMIT
