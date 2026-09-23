"""Shared test helpers: real uvicorning servers, flooding, latency reporting.

Tests intentionally exercise the service over real HTTP (uvicorn subprocesses)
rather than only the ASGI test transport, so cross-process rate limiting,
connection/thread recycling and end-to-end latency are covered.
"""

from __future__ import annotations

import asyncio
import math
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@dataclass
class ServerHandle:
    process: subprocess.Popen
    port: int
    log_path: Path | None

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)


def spawn_server(
    db_path: Path | str,
    *,
    workers: int = 1,
    env_extra: dict | None = None,
    port: int | None = None,
    log_path: Path | None = None,
    ready_timeout: float = 20.0,
) -> ServerHandle:
    port = port or free_port()
    env = os.environ.copy()
    env["SERVICE_DB_PATH"] = str(db_path)
    env.setdefault("RATE_LIMIT_CAPACITY", "100000")
    env.setdefault("RATE_LIMIT_REFILL_PER_SEC", "100000")
    if env_extra:
        env.update(env_extra)

    log_file = open(log_path, "wb") if log_path else subprocess.DEVNULL
    cmd = [
        sys.executable, "-m", "uvicorn", "main:app",
        "--host", "127.0.0.1", "--port", str(port),
    ]
    if workers > 1:
        cmd += ["--workers", str(workers)]
    proc = subprocess.Popen(
        cmd, cwd=PROJECT_ROOT, env=env,
        stdout=log_file, stderr=subprocess.STDOUT,
    )
    handle = ServerHandle(proc, port, log_path)

    deadline = time.monotonic() + ready_timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            tail = ""
            if log_path and log_path.exists():
                tail = log_path.read_text(errors="replace")[-2000:]
            raise RuntimeError(f"server exited early (code {proc.returncode})\n{tail}")
        try:
            resp = httpx.get(f"http://127.0.0.1:{port}/", timeout=1.0)
            if resp.status_code == 200:
                return handle
        except httpx.HTTPError:
            time.sleep(0.1)
    handle.stop()
    raise TimeoutError(f"server on port {port} did not become ready")


@dataclass
class FloodResult:
    title: str
    total: int
    wall_sec: float
    status_counts: dict[int, int]
    outcomes: dict[str, int]
    latencies: list[float]

    @property
    def throughput_per_sec(self) -> float:
        return self.total / self.wall_sec if self.wall_sec else math.inf

    def percentile(self, q: float) -> float:
        if not self.latencies:
            return 0.0
        vals = sorted(self.latencies)
        idx = min(len(vals) - 1, max(0, int(round(q * (len(vals) - 1)))))
        return vals[idx]

    def report(self) -> str:
        lines = [
            f"[{self.title}] total_requests={self.total} wall_sec={self.wall_sec:.3f} "
            f"throughput={self.throughput_per_sec:.1f} req/s",
            f"  status_counts={dict(sorted(self.status_counts.items()))}",
            f"  outcomes={dict(sorted(self.outcomes.items()))}",
            f"  latency sec: min={min(self.latencies):.3f} "
            f"p50={self.percentile(0.50):.3f} p90={self.percentile(0.90):.3f} "
            f"p99={self.percentile(0.99):.3f} max={max(self.latencies):.3f} "
            f"mean={sum(self.latencies)/len(self.latencies):.3f}",
        ]
        text = "\n".join(lines)
        print("\n" + text)
        return text


async def _flood_async(
    url: str, total: int, *, timeout: float
) -> tuple[list[float], dict[int, int], dict[str, int]]:
    latencies: list[float] = []
    status_counts: dict[int, int] = {}
    outcomes: dict[str, int] = {}

    async def one(client: httpx.AsyncClient) -> None:
        started = time.perf_counter()
        try:
            resp = await client.get(url)
            dt = time.perf_counter() - started
            code = resp.status_code
            try:
                outcome = resp.json().get("outcome", "no_outcome")
            except ValueError:
                outcome = "non_json"
        except Exception as exc:  # noqa: BLE001 - record client-side failure
            dt = time.perf_counter() - started
            code = -1
            outcome = f"client_error:{type(exc).__name__}"
        latencies.append(dt)
        status_counts[code] = status_counts.get(code, 0) + 1
        outcomes[outcome] = outcomes.get(outcome, 0) + 1

    limits = httpx.Limits(max_connections=max(16, total), max_keepalive_connections=max(16, total))
    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        # Fire all requests concurrently.
        await asyncio.gather(*(one(client) for _ in range(total)))
    return latencies, status_counts, outcomes


def flood(
    title: str, url: str, total: int, *, timeout: float = 10.0
) -> FloodResult:
    started = time.perf_counter()
    latencies, status_counts, outcomes = asyncio.run(
        _flood_async(url, total, timeout=timeout)
    )
    wall = time.perf_counter() - started
    result = FloodResult(title, total, wall, status_counts, outcomes, latencies)
    result.report()
    return result


@pytest.fixture
def server_factory(tmp_path: Path):
    handles: list[ServerHandle] = []
    logs_dir = PROJECT_ROOT / "logs"
    logs_dir.mkdir(exist_ok=True)

    def _factory(name: str, *, workers: int = 1, env_extra: dict | None = None):
        db = tmp_path / f"{name}.db"
        log = logs_dir / f"test-{name}-{os.getpid()}.log"
        handle = spawn_server(
            db, workers=workers, env_extra=env_extra, log_path=log
        )
        handles.append(handle)
        return handle

    yield _factory

    for handle in handles:
        handle.stop()
