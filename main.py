"""Backward-compatible entrypoint.

The hardened implementation lives in the ``app`` package; this module
simply re-exports the ASGI application so ``uvicorn main:app`` (and any
pre-existing import of ``main.app``) keeps working.
"""

from app.main import app

__all__ = ["app"]
