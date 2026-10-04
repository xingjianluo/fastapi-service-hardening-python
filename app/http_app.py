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
* 429 ``rate_limited``              -- rejected by the cross-process token bucket
* 503 ``bulkhead_rejected``         -- global in-flight budget exhausted
* 503 ``target_bulkhead_rejected``  -- this target's own capacity exhausted
* 504 ``downstream_timeout``        -- downstream did not respond within the deadline
* 502 ``downstream_error``          -- downstream answered with an error status
* 502 ``downstream_connect_error``  -- network/connection failure
* 500 ``internal_error``            -- unexpected server-side error
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request

from .config import Settings, get_settings
from .downstream import CallStatus, DownstreamClient
from .metrics import MetricsRecorder
from .outcomes import Outcome
from .rate_limit import RateLimiter
from .state import init_state
from .targets import parse_targets


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
        settings.db_path,
        settings.downstream_timeout_sec,
        settings.downstream_max_inflight,
        parse_targets(settings.downstream_targets_json),
    )
    try:
        yield
    finally:
        await app.state.downstream.aclose()
        app.state.metrics.stop()
        app.state.limiter.close()


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

        def finish(outcome: Outcome, status_code: int, body: dict) -> JSONResponse:
            latency = time.perf_counter() - started
            request.app.state.metrics.observe(outcome.value, latency)
            return JSONResponse(
                status_code=status_code,
                content={**body, "outcome": outcome.value, "latency_sec": latency},
            )

        # 1) Admission control: over-capacity requests are rejected distinctly,
        #    normal requests are unaffected while tokens remain.
        if not request.app.state.limiter.allow():
            return finish(
                Outcome.RATE_LIMITED,
                429,
                {"error": "rate limit exceeded", "retry_after_hint_sec": 1.0},
            )

        target = target or _default_target(request)

        # 2) Isolated downstream call: bounded concurrency + hard timeout.
        client: DownstreamClient = request.app.state.downstream
        result = await client.call(target)

        if result.status == CallStatus.OK:
            return finish(
                Outcome.SUCCESS,
                200,
                {"data": result.payload, "target": result.target},
            )
        if result.status == CallStatus.TIMEOUT:
            return finish(
                Outcome.TIMEOUT,
                504,
                {
                    "error": "downstream_timeout",
                    "target": result.target,
                    "detail": result.detail,
                },
            )
        if result.status == CallStatus.REJECTED:
            return finish(
                Outcome.BULKHEAD_REJECTED,
                503,
                {
                    "error": "bulkhead_rejected",
                    "target": result.target,
                    "detail": result.detail,
                },
            )
        if result.status == CallStatus.TARGET_REJECTED:
            return finish(
                Outcome.TARGET_BULKHEAD_REJECTED,
                503,
                {
                    "error": "target_bulkhead_rejected",
                    "target": result.target,
                    "detail": result.detail,
                },
            )
        if result.status == CallStatus.CONNECT_ERROR:
            return finish(
                Outcome.INTERNAL_ERROR,
                502,
                {"error": "downstream_connect_error", "detail": result.detail},
            )
        return finish(
            Outcome.INTERNAL_ERROR,
            502,
            {
                "error": "downstream_error",
                "detail": result.detail,
                "downstream_status_code": result.status_code,
            },
        )

    @app.get("/metrics")
    async def metrics(request: Request):
        return request.app.state.metrics.snapshot()

    @app.get("/internal/bulkheads")
    async def bulkheads(request: Request):
        """Live in-flight permits per scope (global + per target)."""
        client: DownstreamClient = request.app.state.downstream
        return client.in_flight_snapshot()

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        from fastapi.responses import JSONResponse

        # Internal errors are observable through the same outcome taxonomy.
        try:
            started = getattr(request.state, "started", None)
            latency = time.perf_counter() - started if started else 0.0
            request.app.state.metrics.observe(Outcome.INTERNAL_ERROR.value, latency)
        except Exception:  # noqa: BLE001
            pass
        return JSONResponse(
            status_code=500,
            content={"error": "internal_error", "detail": str(exc)},
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
