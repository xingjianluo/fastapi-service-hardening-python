"""Outcome classification shared by handlers, metrics and tests."""

from __future__ import annotations

from enum import Enum


class Outcome(str, Enum):
    SUCCESS = "success"
    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"
    BULKHEAD_REJECTED = "bulkhead_rejected"
    TARGET_BULKHEAD_REJECTED = "target_bulkhead_rejected"
    INTERNAL_ERROR = "internal_error"
