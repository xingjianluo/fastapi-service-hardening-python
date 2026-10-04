"""Downstream HTTP calls with per-target isolation, timeout and a global gate.

Three layered protections keep one slow downstream from collapsing the rest:

* **Per-target bulkhead** -- each resolved target has its own in-flight
  capacity (a named scope in the shared :class:`PermitStore`). Once a target
  saturates, further calls to *that target* fail fast with
  ``CallStatus.TARGET_REJECTED`` (HTTP 503 ``target_bulkhead_rejected``),
  while calls to other targets are unaffected.
* **Global bulkhead** -- a final cross-target cap (``global`` scope). When
  even the total budget is exhausted, calls fail with ``CallStatus.REJECTED``
  (HTTP 503 ``bulkhead_rejected``), unchanged from before.
* **Per-target timeout** -- every call carries an ``asyncio`` deadline sized
  by its target's policy; a slow call returns ``CallStatus.TIMEOUT``
  (HTTP 504) instead of hanging.

Both permit scopes are rows in the shared SQLite database, so capacity
semantics hold across worker processes: requests landing on different
instances share the same counters and cannot oversubscribe a target or the
global budget. Permits carry a TTL (reaped on acquire) so a crashed worker
cannot leak capacity; in the normal path they are released in ``finally``
blocks -- success, error, timeout and cancellation alike -- so resources are
recycled and repeated waves behave identically.

Failure classes stay distinct: target-capacity rejection (503
``target_bulkhead_rejected``) is separate from the target's own timeout
(504), from the downstream actively returning an error status (502
``downstream_error``), from network/connect failures (502
``downstream_connect_error``) and from unexpected server-side exceptions
(500 ``internal_error``).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum

import httpx

from .permits import Permit, PermitStore
from .targets import DEFAULT_TARGET_NAME, TargetPolicy, resolve_target

GLOBAL_SCOPE = "global"
# Permits outlive their call by this margin; only relevant if the holder
# crashes before releasing (normal releases happen in `finally`).
_PERMIT_TTL_MARGIN_SEC = 10.0


class CallStatus(str, Enum):
    OK = "ok"
    TIMEOUT = "timeout"
    REJECTED = "rejected"  # global bulkhead saturated
    TARGET_REJECTED = "target_rejected"  # per-target bulkhead saturated
    ERROR = "error"  # downstream answered with an error status
    CONNECT_ERROR = "connect_error"  # network/connection failure


@dataclass
class CallResult:
    status: CallStatus
    target: str = DEFAULT_TARGET_NAME
    status_code: int | None = None
    payload: dict | None = None
    detail: str | None = None


class DownstreamClient:
    def __init__(
        self,
        db_path: str,
        default_timeout_sec: float,
        default_max_inflight: int,
        policies: tuple[TargetPolicy, ...] = (),
    ) -> None:
        self._default_timeout_sec = float(default_timeout_sec)
        self._max_inflight = int(default_max_inflight)
        self._policies = tuple(policies)
        self._permits = PermitStore(db_path)
        # Bounded pool: at most max_inflight connections in use so a slow
        # dependency cannot exhaust sockets/worker threads.
        limits = httpx.Limits(
            max_connections=self._max_inflight,
            max_keepalive_connections=self._max_inflight,
        )
        self._client = httpx.AsyncClient(
            # Slightly larger than any policy deadline so the asyncio timeout
            # below stays the single authoritative timeout classification.
            timeout=httpx.Timeout(self._default_timeout_sec + 0.25),
            limits=limits,
        )
        self._closed = False

    @property
    def max_inflight(self) -> int:
        return self._max_inflight

    def policy_for(self, url: str) -> TargetPolicy:
        return resolve_target(
            self._policies,
            url,
            default_max_inflight=self._max_inflight,
            default_timeout_sec=self._default_timeout_sec,
        )

    def in_flight_snapshot(self) -> dict[str, int]:
        """Live permits per scope (global + each configured target)."""
        snapshot = {GLOBAL_SCOPE: self._permits.in_flight(GLOBAL_SCOPE)}
        for policy in self._policies:
            snapshot[f"target:{policy.name}"] = self._permits.in_flight(
                f"target:{policy.name}"
            )
        return snapshot

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            await self._client.aclose()
            self._permits.close()

    async def call(self, url: str) -> CallResult:
        policy = self.policy_for(url)
        ttl = policy.timeout_sec + _PERMIT_TTL_MARGIN_SEC

        # 1) Global gate first (final cap across all targets).
        global_permit = self._permits.acquire(
            GLOBAL_SCOPE, self._max_inflight, ttl
        )
        if global_permit is None:
            return CallResult(
                status=CallStatus.REJECTED,
                target=policy.name,
                detail="downstream bulkhead saturated",
            )
        # 2) Per-target gate. The default target's capacity IS the global
        #    budget, so a second permit would be redundant.
        target_permit: Permit | None = None
        if policy.name != DEFAULT_TARGET_NAME:
            target_permit = self._permits.acquire(
                f"target:{policy.name}", policy.max_inflight, ttl
            )
            if target_permit is None:
                self._permits.release(global_permit)
                return CallResult(
                    status=CallStatus.TARGET_REJECTED,
                    target=policy.name,
                    detail=(
                        f"target {policy.name!r} bulkhead saturated "
                        f"(max_inflight={policy.max_inflight})"
                    ),
                )
        try:
            return await self._call_bounded(url, policy)
        finally:
            # Always recycle capacity, on success/failure/timeout/cancel.
            if target_permit is not None:
                self._permits.release(target_permit)
            self._permits.release(global_permit)

    async def _call_bounded(self, url: str, policy: TargetPolicy) -> CallResult:
        try:
            async with asyncio.timeout(policy.timeout_sec):
                response = await self._client.get(
                    url, timeout=policy.timeout_sec + 0.25
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
                            target=policy.name,
                            status_code=response.status_code,
                            payload=payload,
                        )
                    return CallResult(
                        status=CallStatus.ERROR,
                        target=policy.name,
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
                target=policy.name,
                detail=(
                    f"target {policy.name!r} call exceeded "
                    f"{policy.timeout_sec}s"
                ),
            )
        except asyncio.CancelledError:
            # Never strand a bulkhead permit on cancellation.
            raise
        except httpx.TransportError as exc:
            # Connect refused / DNS / reset etc.: distinct from a downstream
            # that answered with an error status.
            return CallResult(
                status=CallStatus.CONNECT_ERROR,
                target=policy.name,
                detail=str(exc),
            )
        except httpx.HTTPError as exc:
            return CallResult(
                status=CallStatus.ERROR, target=policy.name, detail=str(exc)
            )
