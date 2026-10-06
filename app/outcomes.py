"""Outcome classification shared by handlers, metrics and tests."""

from __future__ import annotations

from enum import Enum


class Outcome(str, Enum):
    SUCCESS = "success"
    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"
    BULKHEAD_REJECTED = "bulkhead_rejected"  # global in-flight gate saturated
    TARGET_REJECTED = "target_rejected"  # one target's own share exhausted
    # The downstream was reached and answered with a non-2xx status: a
    # business/protocol failure *of the downstream*, distinct from not being
    # able to establish a conversation with it at all.
    DOWNSTREAM_ERROR = "downstream_error"
    # No HTTP response was ever obtained: connection refused, network
    # unreachable, DNS resolution failure, transport reset, ... Distinct from a
    # downstream error status, a timeout, capacity rejections and internal bugs.
    DOWNSTREAM_UNREACHABLE = "downstream_unreachable"
    INTERNAL_ERROR = "internal_error"
