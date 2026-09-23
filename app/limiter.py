"""Admission control.

Two independently distinguishable layers:

* ``FixedWindowRateLimiter``: a fixed 1-second window counter kept in a
  file protected by an exclusive ``flock``. Every worker process on the
  same host coordinates through it, so the effective request rate never
  exceeds ``limit_per_second`` regardless of the process/worker count.
  Only excess requests are rejected; requests that fit the window are
  always admitted.
* ``Bulkhead``: an in-process, non-blocking semaphore bounding the
  number of concurrently in-flight downstream calls. A slow dependency
  can consume at most ``max_concurrent`` slots per process, so it can
  never exhaust the whole event loop.

The two failure modes raise distinct exception types and map to
distinct HTTP status codes (429 vs 503).
"""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import json
import logging
import os
import time

logger = logging.getLogger("app.limiter")


class RateLimitExceeded(Exception):
    """The cross-process fixed-window rate limit was exceeded (-> HTTP 429)."""


class BulkheadFull(Exception):
    """The in-process concurrency budget is exhausted (-> HTTP 503)."""


class FixedWindowRateLimiter:
    WINDOW_SECONDS = 1

    def __init__(self, limit_per_second: int, state_dir: str) -> None:
        self._limit = limit_per_second
        self._state_dir = state_dir
        self._path = os.path.join(state_dir, "window.json")
        os.makedirs(state_dir, exist_ok=True)

    @contextlib.contextmanager
    def _locked_state(self):
        # Open with a+ so it works on the very first call; the lock
        # serializes every worker on the host.
        with open(self._path, "a+") as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            try:
                fh.seek(0)
                raw = fh.read()
                try:
                    state = json.loads(raw) if raw.strip() else {}
                except json.JSONDecodeError:
                    state = {}
                yield state

                def write_state(value: object) -> None:
                    fh.seek(0)
                    fh.truncate()
                    fh.write(json.dumps(value, separators=(",", ":")))
                    # flush() makes the new counter visible to every
                    # other process (shared page cache under the flock).
                    # fsync is deliberately omitted: a hard crash may
                    # lose at most one window of counter state, which is
                    # an acceptable price for admission staying cheap.
                    fh.flush()

                write_state(state)
            finally:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    def acquire(self) -> None:
        """Consume one token. Raises RateLimitExceeded if the window is full.

        Fails *open* on storage errors: an unavailable state file must
        not turn into an outage for otherwise healthy traffic.
        """
        try:
            window = int(time.time()) // self.WINDOW_SECONDS
            with self._locked_state() as state:
                if state.get("window") != window:
                    state.clear()
                    state["window"] = window
                    state["count"] = 0
                count = int(state.get("count", 0))
                if count >= self._limit:
                    raise RateLimitExceeded(
                        f"rate limit {self._limit}/{self.WINDOW_SECONDS}s exceeded"
                    )
                state["count"] = count + 1
        except RateLimitExceeded:
            raise
        except OSError:
            logger.exception("rate limiter state unavailable; failing open")


class Bulkhead:
    """Non-blocking, per-process concurrency limiter."""

    def __init__(self, max_concurrent: int) -> None:
        self._max = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._in_flight = 0

    @property
    def in_flight(self) -> int:
        return self._in_flight

    @property
    def capacity(self) -> int:
        return self._max

    @contextlib.asynccontextmanager
    async def slot(self):
        # Non-blocking acquisition: a full bulkhead is an explicit
        # rejection, never a queue that hides overload.
        if self._semaphore.locked() and self._in_flight >= self._max:
            raise BulkheadFull(f"bulkhead full ({self._max} concurrent calls)")
        await self._semaphore.acquire()
        self._in_flight += 1
        try:
            yield
        finally:
            self._in_flight -= 1
            self._semaphore.release()
