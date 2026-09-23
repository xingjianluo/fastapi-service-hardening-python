"""Downstream HTTP calls with a hard timeout and guaranteed cleanup.

Calls go through a shared, bounded :class:`~app.clients.ClientPool`.
Each call runs inside an ``anyio.fail_after`` cancellation scope: when
the deadline expires the request is aborted, the affected socket is
closed by httpx/anyio rather than returned to the keepalive set, and
the pool slot is released for the next request. A slow dependency can
thus hold at most the pool/bulkhead budget and never hangs a worker.

Timeout and other failures are distinct exceptions, so callers and
metrics can tell them apart.
"""

from __future__ import annotations

import httpx

from app.clients import ClientPool


class UpstreamTimeout(Exception):
    """The downstream dependency did not respond within the deadline (-> 504)."""


class UpstreamError(Exception):
    """The downstream call failed for any other reason (-> 502)."""


async def call_upstream(pool: ClientPool, url: str, timeout_seconds: float) -> dict:
    try:
        response, body = await pool.get_json(
            url, total_timeout=timeout_seconds
        )
    except TimeoutError as exc:
        # anyio.fail_after raises TimeoutError (a subclass in 3.11+);
        # also map httpx connect timeouts to the same outcome.
        raise UpstreamTimeout(
            f"upstream timeout after {timeout_seconds}s"
        ) from exc
    except httpx.TimeoutException as exc:
        raise UpstreamTimeout(
            f"upstream timeout after {timeout_seconds}s"
        ) from exc
    except httpx.HTTPError as exc:
        raise UpstreamError(f"upstream transport error: {exc}") from exc

    if response.status_code >= 400:
        raise UpstreamError(f"upstream returned HTTP {response.status_code}")
    if body is None:
        raise UpstreamError("upstream returned a non-JSON body")
    return body
