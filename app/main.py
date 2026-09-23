"""Hardened FastAPI application.

Routes
------
GET /                 - unchanged legacy response (backward compatible)
GET /health           - liveness probe
GET /call?url=URL     - calls a downstream dependency with admission
                        control, hard timeout and full observability
GET /mock/delay       - built-in slow dependency for local verification
GET /mock/error       - built-in failing dependency for local verification
GET /metrics          - aggregate counters / latency distribution (JSON)

Every /call response uses the envelope
``{"error": {"type": ..., "message": ...}}`` on failure and maps to a
distinct status code so callers can distinguish rejection reasons:

    200 ok                429 rate_limited      502 upstream_error
    503 bulkhead_full     504 upstream_timeout  500 internal_error
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.clients import ClientPool
from app.config import get_settings
from app.downstream import UpstreamError, UpstreamTimeout, call_upstream
from app.limiter import (
    Bulkhead,
    BulkheadFull,
    FixedWindowRateLimiter,
    RateLimitExceeded,
)
from app.logging_setup import configure_logging
from app.metrics import MetricsRegistry, PeriodicMetricsEmitter, snapshot_to_json

logger = logging.getLogger("app.service")


def error_response(status_code: int, error_type: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"type": error_type, "message": message}},
    )


def create_app() -> FastAPI:
    configure_logging()
    settings = get_settings()
    rate_limiter = FixedWindowRateLimiter(
        limit_per_second=settings.rate_limit_per_second,
        state_dir=settings.rate_limit_state_dir,
    )
    bulkhead = Bulkhead(settings.max_concurrent_calls)
    pool_size = settings.client_pool_size or settings.max_concurrent_calls
    pool = ClientPool(
        pool_size=pool_size,
        per_client_connections=settings.per_client_connections,
        connect_timeout=settings.upstream_connect_timeout_seconds,
    )
    metrics = MetricsRegistry()
    emitter = PeriodicMetricsEmitter(metrics, settings.metrics_emit_interval_seconds)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await emitter.start()
        logger.info(
            "service_started %s",
            json.dumps(
                {
                    "rate_limit_per_second": settings.rate_limit_per_second,
                    "max_concurrent_calls": settings.max_concurrent_calls,
                    "upstream_timeout_seconds": settings.upstream_timeout_seconds,
                }
            ),
        )
        try:
            yield
        finally:
            await emitter.stop()
            await pool.aclose()

    app = FastAPI(lifespan=lifespan)
    app.state.settings = settings
    app.state.rate_limiter = rate_limiter
    app.state.bulkhead = bulkhead
    app.state.metrics = metrics

    @app.get("/")
    async def read_root():
        # Legacy contract: byte-for-byte unchanged.
        return {"Hello": "World"}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/mock/delay")
    async def mock_delay(seconds: float = 0.5):
        # A deliberately slow dependency, used for local verification.
        await asyncio.sleep(max(0.0, seconds))
        return {"slept": seconds}

    @app.get("/mock/error")
    async def mock_error():
        return JSONResponse(status_code=500, content={"boom": True})

    @app.get("/metrics")
    async def get_metrics():
        return metrics.snapshot().to_dict()

    @app.get("/call")
    async def call(url: str):
        started = time.perf_counter()
        metrics.enter_request()
        outcome = "internal_error"
        try:
            # Layer 1: cross-process rate limiting. The flock plus a
            # tiny state-file read/write is blocking I/O, so it runs in
            # a worker thread instead of stalling the event loop (the
            # lock itself still serializes every worker process). It is
            # checked first so rejected traffic never consumes a
            # bulkhead slot or opens an upstream connection.
            try:
                await asyncio.to_thread(rate_limiter.acquire)
            except RateLimitExceeded as exc:
                outcome = "rate_limited"
                return error_response(429, "rate_limited", str(exc))

            # Layer 2: in-process concurrency isolation (bulkhead).
            try:
                async with bulkhead.slot():
                    result = await call_upstream(
                        pool, url, settings.upstream_timeout_seconds
                    )
            except BulkheadFull as exc:
                outcome = "bulkhead_full"
                return error_response(503, "bulkhead_full", str(exc))
            except UpstreamTimeout as exc:
                # The hard deadline fired: an explicit, identifiable
                # failure - never a hang or a silent fallback. The
                # cancellation scope has already torn down the in-flight
                # request and quarantined its connection.
                outcome = "timeout"
                return error_response(504, "upstream_timeout", str(exc))
            except UpstreamError as exc:
                outcome = "upstream_error"
                return error_response(502, "upstream_error", str(exc))

            outcome = "ok"
            return JSONResponse(status_code=200, content={"data": result})
        except Exception:
            logger.exception("internal error while serving /call")
            outcome = "internal_error"
            return error_response(
                500, "internal_error", "unexpected internal error"
            )
        finally:
            latency = time.perf_counter() - started
            metrics.record(outcome, latency)
            metrics.leave_request()
            logger.info(
                "request_done %s",
                json.dumps(
                    {
                        "outcome": outcome,
                        "latency_seconds": round(latency, 6),
                        "in_flight": bulkhead.in_flight,
                        "url": url,
                    }
                ),
            )

    logger.info(
        "app_created %s",
        snapshot_to_json(metrics.snapshot()),
    )
    return app


app = create_app()
