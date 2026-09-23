"""The *pre-hardening* implementation, kept solely for A/B comparison.

Characteristics of the old service under a slow dependency:

* no timeout at all - a slow call occupies its connection for the full
  dependency latency;
* one process-wide httpx client with a bounded pool, so a slow
  dependency ties up pool connections and caps effective throughput;
* no admission control, no failure taxonomy, no metrics.
"""

from __future__ import annotations

import asyncio

import httpx
from fastapi import FastAPI

# Same pool size the hardened service uses for its per-process
# bulkhead, so the A/B comparison differs only in timeout/isolation.
POOL_SIZE = 12

app = FastAPI()
_client: httpx.AsyncClient | None = None


@app.on_event("startup")
async def _startup() -> None:
    global _client
    _client = httpx.AsyncClient(
        timeout=None,
        limits=httpx.Limits(
            max_connections=POOL_SIZE,
            max_keepalive_connections=POOL_SIZE,
            keepalive_expiry=5.0,
        ),
    )


@app.on_event("shutdown")
async def _shutdown() -> None:
    if _client is not None:
        await _client.aclose()


@app.get("/mock/delay")
async def mock_delay(seconds: float = 0.5):
    await asyncio.sleep(max(0.0, seconds))
    return {"slept": seconds}


@app.get("/call")
async def call(url: str):
    # Naive behaviour: wait as long as the dependency takes, holding a
    # pooled connection the whole time.
    response = await _client.get(url)
    return {"data": response.json()}
