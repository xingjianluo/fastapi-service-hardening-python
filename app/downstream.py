"""Downstream HTTP calls with a hard per-call timeout.

The timeout is applied per call (``asyncio.timeout`` on top of the httpx
client's own, slightly larger, budget), so different downstream targets can
carry different deadlines. A slow call returns ``CallStatus.TIMEOUT`` (mapped
to HTTP 504) instead of hanging or being silently swallowed.

Concurrency isolation itself lives in :mod:`app.bulkhead` (cross-process
global + per-target permits); this module only owns the HTTP exchange. The
httpx pool is bounded by the global in-flight limit so a slow dependency
cannot exhaust sockets, and responses are explicitly closed so connections
are recycled for subsequent requests.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum

import httpx


class CallStatus(str, Enum):
    OK = "ok"
    TIMEOUT = "timeout"
    REJECTED = "rejected"  # kept for taxonomy compatibility
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
        # Bounded pool: at most max_inflight connections in use so a slow
        # dependency cannot exhaust sockets/worker threads.
        limits = httpx.Limits(
            max_connections=self._max_inflight,
            max_keepalive_connections=self._max_inflight,
        )
        self._client = httpx.AsyncClient(limits=limits)
        self._closed = False

    @property
    def max_inflight(self) -> int:
        return self._max_inflight

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            await self._client.aclose()

    async def call(self, url: str, timeout_sec: float | None = None) -> CallResult:
        deadline = self._timeout_sec if timeout_sec is None else float(timeout_sec)
        try:
            async with asyncio.timeout(deadline):
                # Give httpx a slightly larger budget so the asyncio deadline
                # is the single, authoritative source of timeout classification.
                response = await self._client.get(
                    url, timeout=httpx.Timeout(deadline + 0.25)
                )
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
                detail=f"downstream call exceeded {deadline}s",
            )
        except asyncio.CancelledError:
            # Propagate cancellation; the caller's ``finally`` releases permits.
            raise
        except httpx.HTTPError as exc:
            return CallResult(status=CallStatus.ERROR, detail=str(exc))
