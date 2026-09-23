"""Bounded HTTP client pool with hard deadlines and connection quarantine.

Design
------
* A small number of long-lived :class:`httpx.AsyncClient` objects share
  warm keepalive connections, avoiding per-request TCP setup.
* Every call runs under ``anyio.fail_after(total_timeout)``. This is a
  true cancellation scope: on deadline the in-flight request is aborted.
* The cancellation tears down the response stream; httpx then closes
  that socket instead of returning it to the keepalive set, so a
  poisoned / half-dead connection can never be handed to a later
  request. (Verified end-to-end by
  ``test_cancelled_connections_do_not_poison_pool``.) Successful
  responses keep their warm connection, which is what restores the
  throughput lost to per-call TCP setup.
* A semaphore bounds the number of concurrent in-flight calls; the
  bulkhead bounds it again one layer up. A slow dependency can hold at
  most that many sockets.
* All clients are closed on application shutdown, releasing every
  socket and worker thread.
"""

from __future__ import annotations

import asyncio
import itertools

import anyio
import httpx


class ClientPool:
    def __init__(
        self,
        *,
        pool_size: int,
        per_client_connections: int,
        connect_timeout: float,
    ) -> None:
        self._size = pool_size
        self._semaphore = asyncio.Semaphore(pool_size)
        self._counter = itertools.count()
        limits = httpx.Limits(
            max_connections=per_client_connections,
            max_keepalive_connections=max(1, per_client_connections // 2),
            keepalive_expiry=5.0,
        )
        # httpx bounds only connection establishment/pool waits; the
        # end-to-end deadline is imposed per call by the fail_after
        # scope below.
        timeout = httpx.Timeout(
            connect=connect_timeout, read=None, write=None, pool=None
        )
        self._clients = [
            httpx.AsyncClient(timeout=timeout, limits=limits)
            for _ in range(pool_size)
        ]

    async def get_json(
        self, url: str, *, total_timeout: float
    ) -> tuple[httpx.Response, dict | None]:
        """GET ``url`` with a hard total deadline.

        Returns ``(response, parsed_json_or_None)``. Raises
        ``TimeoutError`` when the deadline expires; any other transport
        failure propagates as an ``httpx.HTTPError``.
        """
        async with self._semaphore:
            client = self._clients[next(self._counter) % self._size]
            with anyio.fail_after(total_timeout):
                response = await client.get(url)
            try:
                return response, response.json()
            except ValueError:
                return response, None

    async def aclose(self) -> None:
        await asyncio.gather(
            *(client.aclose() for client in self._clients),
            return_exceptions=True,
        )
