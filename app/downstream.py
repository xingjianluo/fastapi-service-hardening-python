"""Downstream HTTP calls with a hard per-call timeout and stable failure taxonomy.

Every call ends in exactly one of:

* :data:`CallStatus.OK`                 -- a complete HTTP response with a 2xx;
* :data:`CallStatus.TIMEOUT`            -- the deadline elapsed (connect, read
  or pool wait);
* :data:`CallStatus.DOWNSTREAM_ERROR`   -- the downstream WAS reached and
  answered with a non-2xx HTTP status (its *business/protocol* failure);
* :data:`CallStatus.UNREACHABLE`        -- no HTTP response was ever obtained:
  connection refused, network unreachable, DNS resolution failure, connection
  reset / remote protocol break, ...;
* :data:`CallStatus.ERROR`              -- unexpected client-side failure
  (e.g. malformed target URL), mapped to the service's own internal error.

``DOWNSTREAM_ERROR`` vs ``UNREACHABLE`` is the key distinction callers and
metrics rely on: a 500 from a live dependency and a refused TCP connection are
not the same incident. Each failure additionally carries a stable machine-
readable ``error_kind`` (``connection_refused`` / ``dns_failure`` /
``network_unreachable`` / ``connection_reset`` / ``connect_error`` /
``protocol_error`` / ...), derived by walking the exception cause chain so the
real socket-level reason is reported even when wrapped by httpx/httpcore.

The timeout is applied per call (``asyncio.timeout`` on top of the httpx
client's own, slightly larger, budget), so different downstream targets can
carry different deadlines.

Concurrency isolation itself lives in :mod:`app.bulkhead` (cross-process
global + per-target permits); this module only owns the HTTP exchange. The
httpx pool is bounded by the global in-flight limit so a slow dependency
cannot exhaust sockets, and responses are explicitly closed so connections
are recycled for subsequent requests.
"""

from __future__ import annotations

import asyncio
import errno
import socket
from dataclasses import dataclass
from enum import Enum

import httpx


class CallStatus(str, Enum):
    OK = "ok"
    TIMEOUT = "timeout"
    DOWNSTREAM_ERROR = "downstream_error"  # reached the downstream, non-2xx reply
    UNREACHABLE = "downstream_unreachable"  # no HTTP response obtainable
    ERROR = "error"  # unexpected client-side failure -> service internal error


@dataclass
class CallResult:
    status: CallStatus
    status_code: int | None = None
    payload: dict | None = None
    detail: str | None = None
    error_kind: str | None = None


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
            # Give httpx a slightly larger budget so the asyncio deadline is the
            # single, authoritative source of timeout classification.
            async with asyncio.timeout(deadline):
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
                # We DID reach the downstream: an error *status* is the
                # downstream's own failure, never confused with a network fault.
                return CallResult(
                    status=CallStatus.DOWNSTREAM_ERROR,
                    status_code=response.status_code,
                    payload=payload,
                    detail=f"downstream HTTP {response.status_code}",
                    error_kind="http_error_status",
                )
            finally:
                # Release the connection back to the pool promptly.
                await response.aclose()
        except asyncio.CancelledError:
            # Propagate cancellation; the caller's ``finally`` releases permits.
            raise
        except TimeoutError:
            return CallResult(
                status=CallStatus.TIMEOUT,
                detail=f"downstream call exceeded {deadline}s",
                error_kind="deadline_exceeded",
            )
        except Exception as exc:  # noqa: BLE001 - classified exhaustively below
            return classify_transport_error(exc)


# errno names that unambiguously mean "the network path to the downstream is
# broken" -- used when walking the exception cause chain.
_NETWORK_OS_ERRNOS: dict[int, str] = {
    errno.ECONNREFUSED: "connection_refused",
    errno.ECONNRESET: "connection_reset",
    errno.ENETUNREACH: "network_unreachable",
    errno.EHOSTUNREACH: "network_unreachable",
    errno.EHOSTDOWN: "network_unreachable",
    errno.ECONNABORTED: "connection_reset",
    errno.EPIPE: "connection_reset",
    errno.ETIMEDOUT: "io_timeout",
}


def _iter_cause_chain(exc: BaseException):
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        yield cur
        cur = cur.__cause__ or cur.__context__


def classify_transport_error(exc: Exception) -> CallResult:
    """Map a raised transport exception to a stable, machine-readable result.

    Walks the exception cause chain (httpx wraps httpcore wraps socket-level
    errors) so the concrete reason -- refused port, bad DNS, unreachable
    network, reset connection -- is surfaced instead of a generic string.
    """
    # Timeouts first (httpx timeouts are transport errors too, and
    # ConnectTimeout is *also* a ConnectError; the asyncio deadline is a
    # TimeoutError handled by the caller, this covers httpx's own budget).
    for e in _iter_cause_chain(exc):
        if isinstance(e, (httpx.TimeoutException, asyncio.TimeoutError)):
            return CallResult(
                status=CallStatus.TIMEOUT,
                detail=f"downstream timeout: {exc}",
                error_kind="transport_timeout",
            )

    # Socket/OS-level evidence along the cause chain is the most precise.
    for e in _iter_cause_chain(exc):
        if isinstance(e, (ConnectionRefusedError,)):
            return CallResult(
                status=CallStatus.UNREACHABLE, detail=str(exc),
                error_kind="connection_refused",
            )
        if isinstance(e, socket.gaierror):
            return CallResult(
                status=CallStatus.UNREACHABLE, detail=str(exc),
                error_kind="dns_failure",
            )
        if isinstance(e, OSError):
            kind = _NETWORK_OS_ERRNOS.get(getattr(e, "errno", -1))
            if kind is not None:
                return CallResult(
                    status=CallStatus.UNREACHABLE, detail=str(exc), error_kind=kind
                )

    # httpx-level categories.
    if isinstance(exc, (httpx.ConnectError, httpx.NetworkError)):
        return CallResult(
            status=CallStatus.UNREACHABLE, detail=str(exc),
            error_kind="connect_error",
        )
    if isinstance(exc, httpx.RemoteProtocolError):
        # Connected but the peer broke the conversation before a usable
        # response: the dependency is effectively unreachable for this call.
        return CallResult(
            status=CallStatus.UNREACHABLE, detail=str(exc),
            error_kind="protocol_error",
        )
    if isinstance(exc, (httpx.InvalidURL, httpx.UnsupportedProtocol)):
        return CallResult(
            status=CallStatus.ERROR, detail=str(exc),
            error_kind="invalid_target_url",
        )
    if isinstance(exc, httpx.HTTPError):
        return CallResult(
            status=CallStatus.ERROR, detail=str(exc),
            error_kind="client_error",
        )
    return CallResult(
        status=CallStatus.ERROR, detail=str(exc), error_kind="unexpected_error"
    )
