"""Pure offline tests for the transport-failure classifier.

These never open a socket: they build the same exception shapes httpx/httpcore
produce (including wrapped socket-level causes) and assert a stable,
machine-readable classification. They pin the key requirement that
"downstream unreachable" and "downstream answered with an error" can never be
confused, and both stay separate from timeouts and internal errors.
"""

from __future__ import annotations

import errno
import socket

import httpx

from app.downstream import CallStatus, classify_transport_error


def _check(exc, status, kind=None):
    result = classify_transport_error(exc)
    assert result.status == status, (exc, result)
    if kind is not None:
        assert result.error_kind == kind, (exc, result)
    return result


def _wrap(outer, cause):
    outer.__cause__ = cause
    return outer


def test_connection_refused_is_unreachable():
    # httpx wraps the real OSError as ConnectError -> cause chain must surface
    # the precise socket reason.
    exc = _wrap(
        httpx.ConnectError("[Errno 111] Connection refused"),
        ConnectionRefusedError(errno.ECONNREFUSED, "Connection refused"),
    )
    _check(exc, CallStatus.UNREACHABLE, "connection_refused")


def test_dns_resolution_failure_is_unreachable():
    # gaierror(-2, "Name or service not known") as raised by getaddrinfo.
    gai = socket.gaierror(-2, "Name or service not known")
    exc = _wrap(httpx.ConnectError("[Errno -2]"), gai)
    _check(exc, CallStatus.UNREACHABLE, "dns_failure")


def test_network_unreachable_is_unreachable():
    _check(OSError(errno.ENETUNREACH, "Network is unreachable"),
           CallStatus.UNREACHABLE, "network_unreachable")
    _check(OSError(errno.EHOSTUNREACH, "No route to host"),
           CallStatus.UNREACHABLE, "network_unreachable")


def test_connection_reset_is_unreachable():
    _check(ConnectionResetError(errno.ECONNRESET, "Connection reset by peer"),
           CallStatus.UNREACHABLE, "connection_reset")
    _check(BrokenPipeError(errno.EPIPE, "Broken pipe"),
           CallStatus.UNREACHABLE, "connection_reset")


def test_bare_connect_and_protocol_errors_are_unreachable():
    _check(httpx.ConnectError("generic connect failure"),
           CallStatus.UNREACHABLE, "connect_error")
    _check(httpx.NetworkError("generic network failure"),
           CallStatus.UNREACHABLE, "connect_error")
    _check(httpx.RemoteProtocolError("peer closed without response"),
           CallStatus.UNREACHABLE, "protocol_error")


def test_timeouts_are_never_unreachable():
    # ConnectTimeout is BOTH ConnectError and TimeoutException; timeout wins.
    _check(httpx.ConnectTimeout("timed out connecting"),
           CallStatus.TIMEOUT, "transport_timeout")
    _check(httpx.ReadTimeout("timed out reading"),
           CallStatus.TIMEOUT, "transport_timeout")
    _check(httpx.PoolTimeout("timed out waiting for a connection"),
           CallStatus.TIMEOUT, "transport_timeout")
    # Even when nested deep inside a connect-style wrapper.
    exc = _wrap(httpx.ConnectError("outer"), httpx.ReadTimeout("inner"))
    _check(exc, CallStatus.TIMEOUT, "transport_timeout")


def test_invalid_target_url_is_internal_error_not_unreachable():
    _check(httpx.InvalidURL("not a url at all"),
           CallStatus.ERROR, "invalid_target_url")
    _check(httpx.UnsupportedProtocol("ftp://x"),
           CallStatus.ERROR, "invalid_target_url")


def test_unexpected_errors_are_internal_errors():
    _check(httpx.HTTPError("mystery"), CallStatus.ERROR, "client_error")
    _check(RuntimeError("boom"), CallStatus.ERROR, "unexpected_error")


def test_result_set_is_pairwise_distinct():
    kinds = {
        classify_transport_error(
            _wrap(httpx.ConnectError("x"),
                  ConnectionRefusedError(errno.ECONNREFUSED, "x"))
        ).status,
        classify_transport_error(httpx.ReadTimeout("x")).status,
        classify_transport_error(httpx.InvalidURL("x")).status,
    }
    assert kinds == {CallStatus.UNREACHABLE, CallStatus.TIMEOUT, CallStatus.ERROR}
