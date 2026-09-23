"""Minimal logging wiring.

The service emits structured JSON log lines (request outcomes, periodic
metrics snapshots) on the ``app.*`` loggers. Uvicorn's default root
logger sits at WARNING, which would swallow our INFO lines, so we attach
a stream handler at INFO to the ``app`` namespace and keep propagation
enabled. Configuration is intentionally tiny and side-effect free until
``configure_logging()`` is called.
"""

from __future__ import annotations

import logging
import sys

_CONFIGURED = False


def configure_logging(level: int = logging.INFO) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger("app")
    root.setLevel(level)
    root.addHandler(handler)
    root.propagate = False  # avoid duplicate lines via uvicorn's handlers
    _CONFIGURED = True
