"""Test infrastructure: real HTTP servers and load-generation helpers.

Servers are launched as real uvicorn subprocesses on loopback ports
because only real sockets reproduce pool exhaustion, timeouts and the
cross-process file lock.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_ready(base_url: str, probe_path: str = "/health", timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            if httpx.get(f"{base_url}{probe_path}", timeout=1.0).status_code < 500:
                return
        except Exception as exc:  # server not up yet
            last_error = exc
        time.sleep(0.1)
    raise RuntimeError(f"server at {base_url} did not become ready: {last_error}")


@dataclass
class ServerHandle:
    base_url: str
    env: dict
    process: subprocess.Popen
    workers: int
    log_path: Path

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)


def start_server(
    app_module: str,
    *,
    workers: int = 1,
    env_overrides: dict | None = None,
    state_dir: Path | None = None,
) -> ServerHandle:
    port = free_port()
    env = os.environ.copy()
    env.pop("VIRTUAL_ENV", None)  # avoid uv warning noise in subprocesses
    env.setdefault("METRICS_EMIT_INTERVAL_SECONDS", "0")
    env.setdefault("PYTHONUNBUFFERED", "1")
    if state_dir is not None:
        env["RATE_LIMIT_STATE_DIR"] = str(state_dir)
    if env_overrides:
        env.update(env_overrides)

    log_path = Path("/tmp") / f"uvicorn-test-{port}.log"
    log_fh = open(log_path, "w")
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        app_module,
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--log-level",
        "warning",
    ]
    if workers > 1:
        cmd.extend(["--workers", str(workers)])
    process = subprocess.Popen(
        cmd,
        cwd=PROJECT_ROOT,
        env=env,
        stdout=log_fh,
        stderr=subprocess.STDOUT,
    )
    handle = ServerHandle(
        base_url=f"http://127.0.0.1:{port}",
        env=env,
        process=process,
        workers=workers,
        log_path=log_path,
    )
    probe = "/health" if app_module in ("main:app",) else "/mock/delay?seconds=0"
    try:
        wait_ready(handle.base_url, probe_path=probe)
    except Exception:
        handle.stop()
        raise
    return handle


@pytest.fixture
def hardened_server(tmp_path):
    handle = start_server(
        "main:app",
        env_overrides={
            "RATE_LIMIT_PER_SECOND": "100000",  # effectively off by default
            "MAX_CONCURRENT_CALLS": "12",
            "UPSTREAM_TIMEOUT_SECONDS": "0.4",
        },
        state_dir=tmp_path / "rl",
    )
    try:
        yield handle
    finally:
        handle.stop()


@dataclass
class CallResult:
    status_code: int
    error_type: str | None
    latency: float
    body: dict | None = None


@dataclass
class LoadReport:
    concurrency: int
    started_at: float = 0.0
    finished_at: float = 0.0
    results: list[CallResult] = field(default_factory=list)

    @property
    def wall_seconds(self) -> float:
        return self.finished_at - self.started_at

    def status_counts(self) -> Counter:
        return Counter(r.status_code for r in self.results)

    def error_counts(self) -> Counter:
        return Counter(
            r.error_type for r in self.results if r.error_type is not None
        )

    def latencies_of(self, status_code: int) -> list[float]:
        return sorted(
            r.latency for r in self.results if r.status_code == status_code
        )

    @staticmethod
    def percentile(sorted_values: list[float], q: float) -> float:
        if not sorted_values:
            return 0.0
        index = min(len(sorted_values) - 1, round(q * (len(sorted_values) - 1)))
        return sorted_values[index]


def _one_call(base_url: str, url_param: str, timeout: float) -> CallResult:
    started = time.perf_counter()
    try:
        response = httpx.get(
            f"{base_url}/call",
            params={"url": url_param},
            timeout=timeout,
        )
        latency = time.perf_counter() - started
        body = response.json()
        error_type = body.get("error", {}).get("type") if isinstance(body, dict) else None
        return CallResult(response.status_code, error_type, latency, body)
    except Exception as exc:  # client-side timeout/connection failure
        latency = time.perf_counter() - started
        return CallResult(0, f"client_error:{type(exc).__name__}", latency)


def run_load(
    base_url: str,
    url_param: str,
    *,
    concurrency: int,
    duration_seconds: float,
    client_timeout: float = 10.0,
) -> LoadReport:
    """Fire /call requests at fixed concurrency for a duration."""
    report = LoadReport(concurrency=concurrency, started_at=time.perf_counter())
    deadline = report.started_at + duration_seconds

    def worker() -> None:
        while time.perf_counter() < deadline:
            result = _one_call(base_url, url_param, client_timeout)
            report.results.append(result)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker) for _ in range(concurrency)]
        for future in futures:
            future.result()
    report.finished_at = time.perf_counter()
    return report


def print_report(title: str, report: LoadReport) -> None:
    print(f"\n===== {title} =====")
    print(f"concurrency        : {report.concurrency}")
    print(f"wall time (s)      : {report.wall_seconds:.3f}")
    print(f"total requests     : {len(report.results)}")
    print(f"throughput (req/s) : {len(report.results) / report.wall_seconds:.2f}")
    print(f"status counts      : {dict(sorted(report.status_counts().items()))}")
    print(f"rejection counts   : {dict(sorted(report.error_counts().items()))}")
    for code in sorted({r.status_code for r in report.results}):
        values = report.latencies_of(code)
        if not values:
            continue
        print(
            f"  HTTP {code}: n={len(values):4d} "
            f"p50={report.percentile(values, 0.50) * 1000:7.1f}ms "
            f"p95={report.percentile(values, 0.95) * 1000:7.1f}ms "
            f"p99={report.percentile(values, 0.99) * 1000:7.1f}ms "
            f"max={values[-1] * 1000:7.1f}ms"
        )
