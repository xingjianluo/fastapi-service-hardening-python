"""FastAPI application wiring and HTTP endpoints.

Endpoints
---------
* ``GET /``                -- unchanged baseline response (backward compatible).
* ``GET /upstream-legacy`` -- the *unhardened* behavior: no timeout, no
  isolation, no admission control. Kept so tests can quantify the improvement.
* ``GET /data``            -- hardened path: rate limit -> bulkhead -> timeout
  bounded downstream call, with distinct status codes per failure class.
* ``GET /internal/downstream`` -- a built-in slow/flaky dependency used for
  local verification and tests (``delay`` / ``fail`` query params).
* ``GET /metrics``         -- aggregated outcome/latency snapshot.

Failure semantics (distinct and machine-readable)
-------------------------------------------------
* 200 success
* 429 ``rate_limited``            -- rejected by the cross-process token bucket
* 503 ``bulkhead_rejected``       -- global in-flight gate saturated (all targets)
* 503 ``target_rejected``         -- this target's own capacity share exhausted
* 504 ``timeout``                 -- downstream did not answer within the deadline
* 502 ``downstream_error``        -- reached the downstream, it replied non-2xx
* 502 ``downstream_unreachable``  -- no HTTP response obtainable (connection
                                     refused / DNS failure / network down / reset)
* 500 ``internal_error``          -- unexpected server-side error

The two 502s share a status code by HTTP convention but are stable, distinct
results: they differ in both ``outcome`` and ``error`` fields, plus a
fine-grained ``error_kind`` (``connection_refused`` / ``dns_failure`` /
``network_unreachable`` / ``connection_reset`` / ``protocol_error`` /
``http_error_status`` / ...), so callers can branch on them mechanically.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request

from .bulkhead import BulkheadGate, RejectReason, resolve_policy
from .config import Settings, get_settings
from .downstream import CallStatus, DownstreamClient
from .metrics import MetricsRecorder
from .outcomes import Outcome
from .rate_limit import RateLimiter
from .state import init_state


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings: Settings = app.state.settings
    init_state(settings.db_path)
    app.state.limiter = RateLimiter(
        settings.db_path,
        settings.rate_limit_capacity,
        settings.rate_limit_refill_per_sec,
    )
    app.state.metrics = MetricsRecorder(
        settings.db_path, settings.metrics_flush_interval_sec
    )
    app.state.metrics.start()
    app.state.downstream = DownstreamClient(
        settings.downstream_timeout_sec, settings.downstream_max_inflight
    )
    app.state.bulkhead = BulkheadGate(
        settings.db_path, settings.downstream_max_inflight
    )
    try:
        yield
    finally:
        await app.state.downstream.aclose()
        app.state.metrics.stop()
        app.state.limiter.close()
        app.state.bulkhead.close()


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    app = FastAPI(title="hardened-fastapi-service", lifespan=lifespan)
    app.state.settings = settings

    @app.get("/")
    def read_root():
        # Unchanged baseline entrypoint; hardening is layered on /data.
        return {"Hello": "World"}

    @app.get("/internal/downstream")
    async def internal_downstream(
        delay: float = Query(0.0, ge=0.0, le=30.0),
        fail: bool = False,
    ):
        """Built-in controllable slow dependency for local verification."""
        if delay:
            await asyncio.sleep(delay)
        if fail:
            from fastapi.responses import JSONResponse

            return JSONResponse(status_code=500, content={"error": "forced"})
        return {"slept": delay, "ts": time.time()}

    @app.get("/internal/deps/{name}")
    async def internal_named_dependency(
        name: str,
        delay: float = Query(0.0, ge=0.0, le=30.0),
        fail: bool = False,
        status_code: int = Query(200, ge=200, le=599),
    ):
        """Path-addressed built-in dependency for same-origin isolation tests.

        Several *logical* downstreams can thus share one host/port while living
        on different paths (``/internal/deps/slow`` vs
        ``/internal/deps/fast``); the path is part of the target identity, so
        they get independent bulkheads and deadlines. The query string only
        steers behavior and is deliberately NOT part of the identity.
        """
        if delay:
            await asyncio.sleep(delay)
        if fail:
            from fastapi.responses import JSONResponse

            return JSONResponse(
                status_code=status_code if status_code >= 400 else 500,
                content={"error": "forced", "name": name, "slept": delay},
            )
        return {"name": name, "slept": delay, "ts": time.time()}

    def _call_naive(target: str) -> dict:
        # Pre-hardening behavior: a *blocking* call with no timeout and no
        # concurrency bound. It runs on a threadpool worker (sync endpoint),
        # so a slow dependency pins worker threads for its full duration.
        import httpx

        with httpx.Client(timeout=httpx.Timeout(30.0)) as client:
            response = client.get(target)
            return {
                "status_code": response.status_code,
                "data": _safe_json(response),
            }

    @app.get("/upstream-legacy")
    def upstream_legacy(request: Request, target: str | None = None):
        """Unhardened reference path: blocks on a slow dependency."""
        target = target or _default_target(request)
        return _call_naive(target)

    @app.get("/data")
    async def upstream_data(
        request: Request,
        target: str | None = None,
    ):
        from fastapi.responses import JSONResponse

        started = time.perf_counter()
        # Stash timing/attribution on the request so the global exception
        # handler can attribute an unexpected error to the same target.
        request.state.started = started
        target = target or _default_target(request)
        # Resolve the per-target policy (capacity + timeout) up front so every
        # outcome of this request is attributable to its downstream target.
        policy = resolve_policy(request.app.state.settings, target)
        request.state.target_key = policy.key

        def finish(outcome: Outcome, status_code: int, body: dict) -> JSONResponse:
            latency = time.perf_counter() - started
            request.app.state.metrics.observe(outcome.value, latency, policy.key)
            return JSONResponse(
                status_code=status_code,
                content={
                    **body,
                    "outcome": outcome.value,
                    "target": policy.key,
                    "latency_sec": latency,
                },
            )

        # 1) Admission control: over-capacity requests are rejected distinctly,
        #    normal requests are unaffected while tokens remain.
        if not request.app.state.limiter.allow():
            return finish(
                Outcome.RATE_LIMITED,
                429,
                {"error": "rate limit exceeded", "retry_after_hint_sec": 1.0},
            )

        # 2) Two-level bulkhead (cross-process): the global gate is the final
        #    circuit breaker; the per-target gate confines a slow target to its
        #    own share so healthy targets keep their capacity.
        gate: BulkheadGate = request.app.state.bulkhead
        acquired = gate.acquire(policy)
        if acquired == RejectReason.GLOBAL:
            return finish(
                Outcome.BULKHEAD_REJECTED,
                503,
                {"error": "bulkhead_rejected", "detail": "global in-flight limit reached"},
            )
        if acquired == RejectReason.TARGET:
            return finish(
                Outcome.TARGET_REJECTED,
                503,
                {
                    "error": "target_bulkhead_rejected",
                    "detail": f"target {policy.key} in-flight limit "
                    f"({policy.max_inflight}) reached",
                },
            )

        # 3) Isolated downstream call with the target's own deadline. The
        #    permit is released on every exit path (success, timeout, network
        #    failure, downstream error, cancellation), so capacity is recycled
        #    for the next request even under mid-flight client disconnects.
        client: DownstreamClient = request.app.state.downstream
        try:
            result = await client.call(target, timeout_sec=policy.timeout_sec)
        finally:
            acquired.release()

        if result.status == CallStatus.OK:
            return finish(Outcome.SUCCESS, 200, {"data": result.payload})
        if result.status == CallStatus.TIMEOUT:
            return finish(
                Outcome.TIMEOUT,
                504,
                {
                    "error": "downstream_timeout",
                    "detail": result.detail,
                    "error_kind": result.error_kind,
                },
            )
        if result.status == CallStatus.DOWNSTREAM_ERROR:
            # Reached the downstream; its non-2xx reply is its own failure.
            return finish(
                Outcome.DOWNSTREAM_ERROR,
                502,
                {
                    "error": "downstream_error",
                    "detail": result.detail,
                    "error_kind": result.error_kind,
                    "downstream_status_code": result.status_code,
                },
            )
        if result.status == CallStatus.UNREACHABLE:
            # No HTTP response obtainable: refused port, DNS failure, network
            # down, reset, ... -- deliberately distinct from a downstream
            # error status and from the service's own internal errors.
            return finish(
                Outcome.DOWNSTREAM_UNREACHABLE,
                502,
                {
                    "error": "downstream_unreachable",
                    "detail": result.detail,
                    "error_kind": result.error_kind,
                },
            )
        return finish(
            Outcome.INTERNAL_ERROR,
            500,
            {
                "error": "internal_error",
                "detail": result.detail,
                "error_kind": result.error_kind,
            },
        )

    @app.get("/metrics")
    async def metrics(request: Request):
        return request.app.state.metrics.snapshot()

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        from fastapi.responses import JSONResponse

        # Internal errors are observable through the same outcome taxonomy and
        # are attributed to their target whenever the request got far enough to
        # resolve one.
        try:
            started = getattr(request.state, "started", None)
            target_key = getattr(request.state, "target_key", None)
            latency = time.perf_counter() - started if started else 0.0
            request.app.state.metrics.observe(
                Outcome.INTERNAL_ERROR.value, latency, target_key
            )
        except Exception:  # noqa: BLE001
            pass
        return JSONResponse(
            status_code=500,
            content={
                "error": "internal_error",
                "detail": str(exc),
                "outcome": Outcome.INTERNAL_ERROR.value,
                "target": getattr(getattr(request, "state", None), "target_key", None),
            },
        )

    return app


def _safe_json(response) -> dict | None:
    try:
        return response.json()
    except ValueError:
        return None


def _default_target(request: Request) -> str:
    settings: Settings = request.app.state.settings
    # Prefer the in-process dependency so /data works out of the box.
    return str(request.base_url).rstrip("/") + "/internal/downstream?delay=0.05"
