"""Fast in-process unit tests for the cross-process token bucket."""

from __future__ import annotations

import threading
import time

from app.rate_limit import RateLimiter


def test_exact_burst_capacity_no_over_admit(tmp_path):
    db = tmp_path / "b.db"
    for capacity in (1, 2, 5, 20):
        rl = RateLimiter(str(db) + f".{capacity}", capacity=float(capacity),
                         refill_per_sec=0.0)
        results = [rl.allow() for _ in range(capacity + 3)]
        assert sum(results) == capacity, (capacity, results)
        assert results[:capacity] == [True] * capacity


def test_refill_restores_tokens(tmp_path):
    db = str(tmp_path / "r.db")
    rl = RateLimiter(db, capacity=10.0, refill_per_sec=0.0)
    assert sum(rl.allow() for _ in range(10)) == 10
    assert rl.allow() is False
    # New connection instance, same DB, configured to refill 50/s.
    rl2 = RateLimiter(db, capacity=10.0, refill_per_sec=50.0)
    time.sleep(0.105)  # ~5 tokens
    assert sum(rl2.allow() for _ in range(6)) == 5


def test_concurrent_threads_admit_exactly_capacity(tmp_path):
    rl = RateLimiter(str(tmp_path / "t.db"), capacity=50.0, refill_per_sec=0.0)
    results: list[bool] = []
    lock = threading.Lock()

    def worker() -> None:
        allowed = rl.allow()
        with lock:
            results.append(allowed)

    threads = [threading.Thread(target=worker) for _ in range(120)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(results) == 50
    assert len(results) == 120


def test_rejections_consume_no_token(tmp_path):
    rl = RateLimiter(str(tmp_path / "n.db"), capacity=1.0, refill_per_sec=0.0)
    assert rl.allow() is True
    # Many rejections while empty...
    for _ in range(5):
        assert rl.allow() is False
    # ...must not drive the bucket negative: with refill 10/s for 0.15s,
    # only ~1 token should become available.
    rl2 = RateLimiter(str(tmp_path / "n.db"), capacity=1.0, refill_per_sec=10.0)
    time.sleep(0.16)
    assert rl2.allow() is True
    assert rl2.allow() is False
