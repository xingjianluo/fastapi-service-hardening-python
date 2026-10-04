"""Per-target + global downstream bulkheads, consistent across processes.

Isolation model
---------------
Every downstream call must hold **two** permits for its whole lifetime:

* a **global** permit (scope ``global``) -- the final circuit breaker capping
  total in-flight downstream calls (``DOWNSTREAM_MAX_INFLIGHT``);
* a **target** permit (scope ``target:<host:port>``) -- the per-downstream
  bulkhead, so one slow target can only consume its own share and can never
  starve healthy targets.

Permits are rows in the shared SQLite ``inflight_permits`` table. Acquisition
happens inside a single ``BEGIN IMMEDIATE`` transaction (expired-lease sweep
-> count -> insert), so concurrent workers can never over-admit: a target's
in-flight count never exceeds its configured limit, and the global count never
exceeds the global limit, no matter which worker process serves the request.

Crash safety: each permit carries a lease (``expires_at``). Holders release
explicitly in a ``finally`` (success, timeout, error *and* cancellation); a
worker that dies anyway has its permits reclaimed lazily once the lease
expires, so capacity cannot be stranded permanently.

Target identity: the ``host:port`` authority of the downstream URL. Limits and
timeouts resolve from ``Settings.target_overrides`` with
``target_default_*`` (which themselves default to the global values) as
fallback -- so without any target configuration, behavior is exactly the
previous single global bulkhead.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Self
from urllib.parse import urlsplit

from .config import Settings
from .state import open_db

GLOBAL_SCOPE = "global"
# Extra lease headroom beyond the call deadline; only relevant if the holder
# crashes without releasing (normal release is immediate).
_LEASE_MARGIN_SEC = 10.0


def target_key(url: str) -> str:
    """Stable identity of a downstream target: the URL authority (host:port)."""
    try:
        netloc = urlsplit(url).netloc
    except ValueError:
        netloc = ""
    return netloc or "unknown"


@dataclass(frozen=True)
class TargetPolicy:
    """Resolved capacity/timeout for one downstream target."""

    key: str
    max_inflight: int
    timeout_sec: float


def resolve_policy(settings: Settings, url: str) -> TargetPolicy:
    key = target_key(url)
    rule = settings.target_overrides.get(key)
    max_inflight = settings.target_default_max_inflight
    timeout_sec = settings.target_default_timeout_sec
    if rule is not None:
        if rule.max_inflight is not None:
            max_inflight = rule.max_inflight
        if rule.timeout_sec is not None:
            timeout_sec = rule.timeout_sec
    return TargetPolicy(key=key, max_inflight=max_inflight, timeout_sec=timeout_sec)


class RejectReason:
    GLOBAL = "global"  # global gate saturated -> bulkhead_rejected
    TARGET = "target"  # this target's own share exhausted -> target_rejected


class Permit:
    """A held pair of (global, target) permits; ``release`` is idempotent."""

    def __init__(self, gate: BulkheadGate, owner: str, target_scope: str) -> None:
        self._gate = gate
        self._owner = owner
        self._target_scope = target_scope
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._gate._release(self._owner, self._target_scope)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc) -> None:
        self.release()


class BulkheadGate:
    """Cross-process, two-level (global + per-target) in-flight gate."""

    def __init__(
        self,
        db_path: str,
        global_limit: int,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._db_path = db_path
        self._global_limit = int(global_limit)
        # Wall clock: permits/leases are compared across processes.
        self._clock = clock
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.Lock()

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = open_db(self._db_path)
        return self._conn

    @staticmethod
    def target_scope(key: str) -> str:
        return f"target:{key}"

    def acquire(self, policy: TargetPolicy) -> Permit | str:
        """Try to take one global + one target permit atomically.

        Returns a :class:`Permit` on success, else ``RejectReason.GLOBAL`` or
        ``RejectReason.TARGET`` so the caller can reject with a distinct,
        stable classification of *which* gate was saturated.
        """
        owner = f"{os.getpid()}:{uuid.uuid4().hex}"
        target_scope = self.target_scope(policy.key)
        expires = self._clock() + policy.timeout_sec + _LEASE_MARGIN_SEC
        with self._lock:
            conn = self._connect()
            while True:
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    now = self._clock()
                    # Lazily reclaim permits leaked by crashed holders.
                    conn.execute(
                        "DELETE FROM inflight_permits WHERE expires_at < ?", (now,)
                    )
                    (global_count,) = conn.execute(
                        "SELECT COUNT(*) FROM inflight_permits WHERE scope = ?",
                        (GLOBAL_SCOPE,),
                    ).fetchone()
                    if global_count >= self._global_limit:
                        conn.execute("ROLLBACK")
                        return RejectReason.GLOBAL
                    (target_count,) = conn.execute(
                        "SELECT COUNT(*) FROM inflight_permits WHERE scope = ?",
                        (target_scope,),
                    ).fetchone()
                    if target_count >= policy.max_inflight:
                        conn.execute("ROLLBACK")
                        return RejectReason.TARGET
                    conn.execute(
                        "INSERT INTO inflight_permits(scope, owner, expires_at) "
                        "VALUES (?, ?, ?)",
                        (GLOBAL_SCOPE, owner, expires),
                    )
                    conn.execute(
                        "INSERT INTO inflight_permits(scope, owner, expires_at) "
                        "VALUES (?, ?, ?)",
                        (target_scope, owner, expires),
                    )
                    conn.execute("COMMIT")
                    return Permit(self, owner, target_scope)
                except sqlite3.OperationalError as exc:
                    try:
                        conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    if "locked" in str(exc).lower():
                        time.sleep(0.002)
                        continue
                    raise

    def _release(self, owner: str, target_scope: str) -> None:
        with self._lock:
            conn = self._connect()
            while True:
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute(
                        "DELETE FROM inflight_permits WHERE owner = ? AND scope IN (?, ?)",
                        (owner, GLOBAL_SCOPE, target_scope),
                    )
                    conn.execute("COMMIT")
                    return
                except sqlite3.OperationalError as exc:
                    try:
                        conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    if "locked" in str(exc).lower():
                        time.sleep(0.002)
                        continue
                    raise

    def inflight(self, scope: str) -> int:
        """Current non-expired permit count for a scope (observability/tests)."""
        with self._lock:
            conn = self._connect()
            (count,) = conn.execute(
                "SELECT COUNT(*) FROM inflight_permits WHERE scope = ? AND expires_at >= ?",
                (scope, self._clock()),
            ).fetchone()
            return int(count)

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
