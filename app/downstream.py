"""Downstream HTTP calls with timeout and bounded-concurrency isolation.

Two protections keep a slow downstream from collapsing the service:

* **Timeout** -- every call carries an explicit ``asyncio`` deadline on top of
  the client's own timeouts. A slow call is returned as ``CallStatus.TIMEOUT``
  (mapped to HTTP 504) instead of hanging or being silently swallowed.
* **Bulkhead** -- an ``asyncio.Semaphore`` caps the number of in-flight calls.
  Once saturated, further requests fail fast with ``CallStatus.REJECTED``
  (mapped to HTTP 503) rather than queueing without bound, so connections,
  memory and event-loop capacity stay bounded. The semaphore is released in a
  ``finally`` block (including on timeout/cancellation) and ``httpx`` closes
  the response, so resources are recycled for subsequent requests.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum

import httpx


class CallStatus(str, Enum):
    OK = "ok"
    TIMEOUT = "timeout"
    REJECTED = "rejected"
    ERROR = "error"


@dataclass
class CallResult:
    status: CallStatus
    status_code: int | None = None
    payload: dict | None = None
    detail: str | None = None


class DownstreamClient:
    def __init__(self, timeout_sec: float, max_inflight: int) -> None:
        self._timeout_sec = float(timeout_sec)
        self._max_inflight = int(max_inflight)
        self._semaphore = asyncio.Semaphore(self._max_inflight)
        # Makes the "is the bulkhead full?" check and permit acquisition
        # atomic, eliminating a check-then-acquire race under concurrency.
        self._admission_lock = asyncio.Lock()
        # Bounded pool: at most max_inflight connections in use so a slow
        # dependency cannot exhaust sockets/worker threads.
        limits = httpx.Limits(
            max_connections=self._max_inflight,
            max_keepalive_connections=self._max_inflight,
        )
        self._client = httpx.AsyncClient(
            # Give httpx a slightly larger budget so the asyncio deadline below
            # is the single, authoritative source of timeout classification.
            timeout=httpx.Timeout(self._timeout_sec + 0.25),
            limits=limits,
        )
        self._closed = False

    @property
    def max_inflight(self) -> int:
        return self._max_inflight

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            await self._client.aclose()

    async def call(self, url: str) -> CallResult:
        # Atomically decide admit vs. fail-fast. Because the check and acquire
        # are uninterrupted, exactly max_inflight calls are ever in flight.
        async with self._admission_lock:
            if self._semaphore.locked():
                return CallResult(
                    status=CallStatus.REJECTED,
                    detail="downstream bulkhead saturated",
                )
            await self._semaphore.acquire()
        try:
            async with asyncio.timeout(self._timeout_sec):
                response = await self._client.get(url)
                try:
                    payload: dict | None = None
                    try:
                        payload = response.json()
                    except ValueError:
                        payload = None
                    if response.status_code < 400:
                        return CallResult(
                            status=CallStatus.OK,
                            status_code=response.status_code,
                            payload=payload,
                        )
                    return CallResult(
                        status=CallStatus.ERROR,
                        status_code=response.status_code,
                        payload=payload,
                        detail=f"downstream HTTP {response.status_code}",
                    )
                finally:
                    # Release the connection back to the pool promptly.
                    await response.aclose()
        except TimeoutError:
            return CallResult(
                status=CallStatus.TIMEOUT,
                detail=f"downstream call exceeded {self._timeout_sec}s",
            )
        except asyncio.CancelledError:
            # Never strand a bulkhead permit on cancellation.
            raise
        except httpx.HTTPError as exc:
            return CallResult(status=CallStatus.ERROR, detail=str(exc))
        finally:
            self._semaphore.release()
