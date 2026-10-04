"""Fast in-process unit tests for the cross-process two-level bulkhead gate."""

from __future__ import annotations

import threading

from app.bulkhead import (
    GLOBAL_SCOPE,
    BulkheadGate,
    RejectReason,
    TargetPolicy,
    resolve_policy,
    target_key,
)
from app.config import Settings, TargetRule


def _policy(key: str, limit: int, timeout: float = 1.0) -> TargetPolicy:
    return TargetPolicy(key=key, max_inflight=limit, timeout_sec=timeout)


def test_target_key_is_url_authority():
    assert target_key("http://127.0.0.1:9001/a?b=c") == "127.0.0.1:9001"
    assert target_key("https://example.com/x") == "example.com"
    assert target_key("not a url") == "unknown"


def test_resolve_policy_prefers_override_then_default():
    settings = Settings(
        rate_limit_capacity=1, rate_limit_refill_per_sec=1,
        downstream_timeout_sec=2.0, downstream_max_inflight=10,
        target_default_max_inflight=7, target_default_timeout_sec=1.5,
        db_path=":memory:", metrics_flush_interval_sec=1.0,
        target_overrides={
            "a:1": TargetRule(max_inflight=3, timeout_sec=0.25),
            "b:2": TargetRule(max_inflight=None, timeout_sec=0.5),
        },
    )
    pa = resolve_policy(settings, "http://a:1/x")
    assert (pa.key, pa.max_inflight, pa.timeout_sec) == ("a:1", 3, 0.25)
    pb = resolve_policy(settings, "http://b:2/y")
    assert (pb.max_inflight, pb.timeout_sec) == (7, 0.5)  # partial override
    pc = resolve_policy(settings, "http://c:3/z")
    assert (pc.max_inflight, pc.timeout_sec) == (7, 1.5)  # pure default


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
