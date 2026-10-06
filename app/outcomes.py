"""Outcome classification shared by handlers, metrics and tests."""

from __future__ import annotations

from enum import Enum


class Outcome(str, Enum):
    SUCCESS = "success"
    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"
    BULKHEAD_REJECTED = "bulkhead_rejected"  # global in-flight gate saturated
    TARGET_REJECTED = "target_rejected"  # one target's own share exhausted
    # Transport-level failure: nothing usable came back from the downstream
    # (connection refused, DNS failure, network unreachable, reset, TLS...).
    CONNECT_FAILED = "connect_failed"
    # The downstream WAS reached and actively returned an error status (>=400).
    DOWNSTREAM_ERROR = "downstream_error"
    INTERNAL_ERROR = "internal_error"  # unexpected server-side exception
