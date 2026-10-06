"""Fast in-process unit tests for the cross-process two-level bulkhead gate."""

from __future__ import annotations

import threading

from app.bulkhead import (
    GLOBAL_SCOPE,
    BulkheadGate,
    RejectReason,
    TargetPolicy,
    origin_keys,
    resolve_policy,
    target_key,
)
from app.config import Settings, TargetRule


def _policy(key: str, limit: int, timeout: float = 1.0) -> TargetPolicy:
    return TargetPolicy(key=key, max_inflight=limit, timeout_sec=timeout)


def test_target_key_is_the_logical_endpoint():
    # scheme + authority + path; query/fragment excluded
    assert target_key("http://127.0.0.1:9001/a?b=c") == "http://127.0.0.1:9001/a"
    assert target_key("https://example.com/x") == "https://example.com/x"
    assert target_key("http://example.com") == "http://example.com/"
    assert target_key("http://example.com/a#frag") == "http://example.com/a"
    assert target_key("not a url") == "unknown"


def test_same_origin_different_paths_are_distinct_targets():
    slow = target_key("http://shared.example:8080/api/slow?x=1")
    fast = target_key("http://shared.example:8080/api/fast")
    other = target_key("https://shared.example:8080/api/slow")
    assert slow == "http://shared.example:8080/api/slow"
    assert fast == "http://shared.example:8080/api/fast"
    assert slow != fast  # the core fix: independent logical downstreams
    assert slow != other  # scheme also part of identity
    # query-only differences do NOT create a new target
    assert target_key("http://h:1/p?a=1") == target_key("http://h:1/p?a=2")


def test_origin_keys_offer_scheme_qualified_and_legacy_lookup():
    assert origin_keys("http://127.0.0.1:9001/a/b") == (
        "http://127.0.0.1:9001", "127.0.0.1:9001",
    )
    assert origin_keys("garbage") == ()


def test_resolve_policy_prefers_override_then_default():
    settings = Settings(
        rate_limit_capacity=1, rate_limit_refill_per_sec=1,
        downstream_timeout_sec=2.0, downstream_max_inflight=10,
        target_default_max_inflight=7, target_default_timeout_sec=1.5,
        db_path=":memory:", metrics_flush_interval_sec=1.0,
        target_overrides={
            "http://a:1/x": TargetRule(max_inflight=3, timeout_sec=0.25),
            "b:2": TargetRule(max_inflight=None, timeout_sec=0.5),
        },
    )
    pa = resolve_policy(settings, "http://a:1/x")
    assert (pa.key, pa.max_inflight, pa.timeout_sec) == (
        "http://a:1/x", 3, 0.25)
    pb = resolve_policy(settings, "http://b:2/y")
    assert (pb.key, pb.max_inflight, pb.timeout_sec) == (
        "http://b:2/y", 7, 0.5)  # partial legacy-authority override
    pc = resolve_policy(settings, "http://c:3/z")
    assert (pc.max_inflight, pc.timeout_sec) == (7, 1.5)  # pure default


def test_resolve_policy_supports_origin_wide_and_exact_endpoint_overrides():
    settings = Settings(
        rate_limit_capacity=1, rate_limit_refill_per_sec=1,
        downstream_timeout_sec=2.0, downstream_max_inflight=10,
        target_default_max_inflight=7, target_default_timeout_sec=1.5,
        db_path=":memory:", metrics_flush_interval_sec=1.0,
        target_overrides={
            # applies to every path on this origin
            "http://shared:8080": TargetRule(max_inflight=5, timeout_sec=1.0),
            # exact endpoint wins over the origin-wide rule
            "http://shared:8080/api/special": TargetRule(max_inflight=2),
        },
    )
    p = resolve_policy(settings, "http://shared:8080/api/a")
    assert (p.max_inflight, p.timeout_sec) == (5, 1.0)
    p = resolve_policy(settings, "http://shared:8080/api/special")
    assert (p.max_inflight, p.timeout_sec) == (2, 1.0)  # exact key, inherits timeout


def test_slow_target_cannot_consume_other_targets_share(tmp_path):
    gate = BulkheadGate(str(tmp_path / "g.db"), global_limit=10)
    slow = _policy("slow:1", 2)
    fast = _policy("fast:1", 2)
    held = [gate.acquire(slow), gate.acquire(slow)]
    assert all(not isinstance(p, str) for p in held)
    # Slow target's own share is exhausted -> distinct TARGET rejection...
    assert gate.acquire(slow) == RejectReason.TARGET
    # ...while the fast target still admits (isolation).
    p = gate.acquire(fast)
    assert not isinstance(p, str)
    p.release()
    for permit in held:
        permit.release()


def test_same_origin_paths_have_independent_capacity(tmp_path):
    gate = BulkheadGate(str(tmp_path / "g.db"), global_limit=10)
    slow = _policy("http://shared:1/api/slow", 2)
    fast = _policy("http://shared:1/api/fast", 2)
    held = [gate.acquire(slow), gate.acquire(slow)]
    assert all(not isinstance(p, str) for p in held)
    # The slow PATH's own share is exhausted...
    assert gate.acquire(slow) == RejectReason.TARGET
    # ...but a different path on the SAME host:port keeps its full share.
    fp = gate.acquire(fast)
    assert not isinstance(fp, str)
    fp2 = gate.acquire(fast)
    assert not isinstance(fp2, str)
    assert gate.acquire(fast) == RejectReason.TARGET
    fp.release()
    fp2.release()
    for permit in held:
        permit.release()
    assert gate.inflight("target:http://shared:1/api/slow") == 0
    assert gate.inflight("target:http://shared:1/api/fast") == 0


def test_global_gate_is_the_final_breaker(tmp_path):
    gate = BulkheadGate(str(tmp_path / "g.db"), global_limit=3)
    held = [gate.acquire(_policy(f"t{i}:1", 2)) for i in range(3)]
    assert all(not isinstance(p, str) for p in held)
    # A target with free capacity is still stopped by the global gate.
    assert gate.acquire(_policy("other:1", 5)) == RejectReason.GLOBAL
    for permit in held:
        permit.release()
    assert gate.inflight(GLOBAL_SCOPE) == 0


def test_release_recycles_capacity_for_next_waves(tmp_path):
    gate = BulkheadGate(str(tmp_path / "g.db"), global_limit=4)
    slow = _policy("slow:1", 2)
    for wave in range(5):
        held = [gate.acquire(slow) for _ in range(2)]
        assert all(not isinstance(p, str) for p in held), wave
        assert gate.acquire(slow) == RejectReason.TARGET
        for permit in held:
            permit.release()
        assert gate.inflight(GLOBAL_SCOPE) == 0, wave
        assert gate.inflight("target:slow:1") == 0, wave


def test_concurrent_threads_never_exceed_limits(tmp_path):
    gate = BulkheadGate(str(tmp_path / "g.db"), global_limit=8)
    slow = _policy("slow:1", 4)
    results = {"permit": 0, "target": 0, "global": 0}
    lock = threading.Lock()

    def worker() -> None:
        acquired = gate.acquire(slow)
        with lock:
            if isinstance(acquired, str):
                results[acquired] += 1
            else:
                results["permit"] += 1
                acquired.release()

    threads = [threading.Thread(target=worker) for _ in range(40)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # Exactly the target share was handed out; the rest saw the TARGET reason.
    assert results["permit"] == 4
    assert results["target"] == 36
    assert results["global"] == 0
    assert gate.inflight(GLOBAL_SCOPE) == 0


def test_crashed_holder_permits_are_reclaimed_after_lease(tmp_path):
    db = str(tmp_path / "g.db")
    now = [1000.0]
    gate = BulkheadGate(db, global_limit=5, clock=lambda: now[0])
    crashed = gate.acquire(_policy("x:1", 1, timeout=0.1))
    assert not isinstance(crashed, str)
    # Holder "crashes" (never releases). Before the lease expires the gate
    # stays saturated; afterwards capacity is reclaimed automatically.
    assert gate.acquire(_policy("x:1", 1, timeout=0.1)) == RejectReason.TARGET
    now[0] += 60.0
    reclaimed = gate.acquire(_policy("x:1", 1, timeout=0.1))
    assert not isinstance(reclaimed, str)
    reclaimed.release()
