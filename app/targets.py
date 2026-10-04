"""Per-downstream-target policies: capacity and timeout boundaries.

A *target* is a named downstream destination identified by a URL prefix.
Each target gets its own capacity (max in-flight calls) and timeout, so a
slow target can only consume the capacity assigned to it and cannot starve
other targets. Targets are configured at runtime via the
``DOWNSTREAM_TARGETS_JSON`` environment variable, e.g.::

    [
      {"name": "slow", "match": "http://a/internal/downstream?slow",
       "max_inflight": 4, "timeout_sec": 0.3},
      {"name": "fast", "match": "http://a/internal/downstream",
       "max_inflight": 16, "timeout_sec": 1.0}
    ]

Resolution is longest-prefix match. A ``match`` may be a full URL prefix
(``http://host/dep?slow``) or a path(+query) prefix (``/dep?slow``); the
latter keeps the same config usable across hosts/ports and worker
instances. Calls matching no configured target fall back to the
service-wide defaults (``DOWNSTREAM_MAX_INFLIGHT`` /
``DOWNSTREAM_TIMEOUT_SEC``) under the reserved name ``default``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.parse import urlsplit

DEFAULT_TARGET_NAME = "default"


@dataclass(frozen=True)
class TargetPolicy:
    """Resolved capacity/timeout boundary for one downstream target."""

    name: str
    match: str  # URL prefix that routes a call to this target ("" = default)
    max_inflight: int
    timeout_sec: float


def parse_targets(raw: str | None) -> tuple[TargetPolicy, ...]:
    """Parse the ``DOWNSTREAM_TARGETS_JSON`` config value into policies."""
    if not raw or not raw.strip():
        return ()
    data = json.loads(raw)
    if not isinstance(data, list):
        raise TypeError("DOWNSTREAM_TARGETS_JSON must be a JSON list")
    policies: list[TargetPolicy] = []
    seen: set[str] = set()
    for item in data:
        name = str(item["name"]).strip()
        match = str(item["match"]).strip()
        if not name or not match:
            raise ValueError("each target needs non-empty 'name' and 'match'")
        if name == DEFAULT_TARGET_NAME or name in seen:
            raise ValueError(f"duplicate or reserved target name: {name!r}")
        seen.add(name)
        max_inflight = int(item["max_inflight"])
        timeout_sec = float(item["timeout_sec"])
        if max_inflight <= 0 or timeout_sec <= 0:
            raise ValueError(f"target {name!r}: limits must be positive")
        policies.append(
            TargetPolicy(
                name=name,
                match=match,
                max_inflight=max_inflight,
                timeout_sec=timeout_sec,
            )
        )
    # Longest prefix first so resolution is deterministic.
    return tuple(sorted(policies, key=lambda p: len(p.match), reverse=True))


def _path_query(url: str) -> str:
    parts = urlsplit(url)
    return parts.path + (f"?{parts.query}" if parts.query else "")


def resolve_target(
    policies: tuple[TargetPolicy, ...],
    url: str,
    *,
    default_max_inflight: int,
    default_timeout_sec: float,
) -> TargetPolicy:
    """Resolve the policy for ``url`` (longest prefix wins; else default)."""
    path_query = _path_query(url)
    for policy in policies:  # already sorted longest-prefix-first
        if url.startswith(policy.match) or path_query.startswith(policy.match):
            return policy
    return TargetPolicy(
        name=DEFAULT_TARGET_NAME,
        match="",
        max_inflight=int(default_max_inflight),
        timeout_sec=float(default_timeout_sec),
    )
