from __future__ import annotations

"""Small thread-safe HTTP/HTTPS connection pool for buffered requests."""

from collections import deque
from dataclasses import dataclass, field
import http.client
import os
import socket
import threading
import time
from typing import Deque, Dict, Mapping, Optional, Tuple
from urllib.parse import urlparse

from pycloud_parallel.controlplane.bounded_http_server import DEFAULT_KEEPALIVE_IDLE_TIMEOUT_SEC


DEFAULT_MAX_CONNECTIONS_PER_ORIGIN = 32
DEFAULT_MAX_IDLE_CONNECTIONS_PER_ORIGIN = 8
DEFAULT_IDLE_CONNECTION_TTL_SEC = 0.25
MAX_SHARED_IDLE_CONNECTION_TTL_SEC = DEFAULT_KEEPALIVE_IDLE_TIMEOUT_SEC * 0.8


@dataclass(frozen=True)
class BufferedHttpResponse:
    status: int
    reason: str
    headers: Mapping[str, str]
    body: bytes


@dataclass
class _OriginState:
    connection_count: int = 0
    idle: Deque[Tuple[http.client.HTTPConnection, float]] = field(default_factory=deque)


class HttpConnectionPool:
    def __init__(
        self,
        *,
        max_connections_per_origin: int = DEFAULT_MAX_CONNECTIONS_PER_ORIGIN,
        max_idle_connections_per_origin: int = DEFAULT_MAX_IDLE_CONNECTIONS_PER_ORIGIN,
        idle_ttl_sec: float = DEFAULT_IDLE_CONNECTION_TTL_SEC,
    ) -> None:
        limit = int(max_connections_per_origin)
        if limit <= 0:
            raise ValueError("max_connections_per_origin must be greater than zero")
        idle_limit = int(max_idle_connections_per_origin)
        if idle_limit < 0:
            raise ValueError("max_idle_connections_per_origin must be non-negative")
        self.max_connections_per_origin = limit
        self.max_idle_connections_per_origin = min(limit, idle_limit)
        self.idle_ttl_sec = max(0.1, float(idle_ttl_sec))
        self._condition = threading.Condition()
        self._origins: Dict[Tuple[str, str, int], _OriginState] = {}
        self._closed = False

    def request(
        self,
        *,
        url: str,
        method: str,
        timeout_sec: float,
        headers: Optional[Mapping[str, str]] = None,
        body: Optional[bytes] = None,
        max_response_bytes: Optional[int] = None,
        retry_connection_error: Optional[bool] = None,
    ) -> BufferedHttpResponse:
        parsed = urlparse(url)
        origin = self._origin(parsed)
        target = self._request_target(parsed)
        timeout = max(0.1, float(timeout_sec))
        retry_allowed = (
            method.upper() in {"GET", "HEAD", "OPTIONS"}
            if retry_connection_error is None
            else bool(retry_connection_error)
        )
        attempts = 2 if retry_allowed else 1
        last_error: Optional[BaseException] = None
        for attempt in range(attempts):
            connection = self._acquire(origin, timeout_sec=timeout, require_fresh=attempt > 0)
            reusable = False
            try:
                self._set_timeout(connection, timeout)
                connection.request(method.upper(), target, body=body, headers=dict(headers or {}))
                response = connection.getresponse()
                if max_response_bytes is None:
                    response_body = response.read()
                else:
                    limit = max(0, int(max_response_bytes))
                    response_body = response.read(limit + 1)
                reusable = (
                    len(response_body) <= int(max_response_bytes)
                    if max_response_bytes is not None
                    else True
                )
                reusable = (
                    reusable
                    and int(response.status) < 400
                    and not response.will_close
                    and str(response.getheader("Connection", "")).lower() != "close"
                )
                return BufferedHttpResponse(
                    status=int(response.status),
                    reason=str(response.reason or ""),
                    headers={str(key): str(value) for key, value in response.getheaders()},
                    body=bytes(response_body),
                )
            except (OSError, http.client.HTTPException) as exc:
                last_error = exc
                if attempt + 1 >= attempts:
                    raise
            finally:
                self._release(origin, connection, reusable=reusable)
        assert last_error is not None
        raise last_error

    def close(self) -> None:
        with self._condition:
            if self._closed:
                return
            self._closed = True
            connections = [connection for state in self._origins.values() for connection, _created_at in state.idle]
            self._origins.clear()
            self._condition.notify_all()
        for connection in connections:
            connection.close()

    def _acquire(
        self,
        origin: Tuple[str, str, int],
        *,
        timeout_sec: float,
        require_fresh: bool,
    ) -> http.client.HTTPConnection:
        deadline = time.monotonic() + timeout_sec
        stale = []
        with self._condition:
            while True:
                if self._closed:
                    raise RuntimeError("HTTP connection pool is closed")
                state = self._origins.setdefault(origin, _OriginState())
                now = time.monotonic()
                while state.idle and now - state.idle[0][1] >= self.idle_ttl_sec:
                    connection, _idle_at = state.idle.popleft()
                    state.connection_count -= 1
                    stale.append(connection)
                if require_fresh and state.idle:
                    connection, _idle_at = state.idle.popleft()
                    state.connection_count -= 1
                    stale.append(connection)
                if not require_fresh and state.idle:
                    connection, _idle_at = state.idle.pop()
                    break
                if state.connection_count < self.max_connections_per_origin:
                    state.connection_count += 1
                    connection = self._new_connection(origin, timeout_sec)
                    break
                remaining = deadline - now
                if remaining <= 0:
                    raise socket.timeout(f"timed out waiting for pooled HTTP connection to {origin[1]}:{origin[2]}")
                self._condition.wait(timeout=remaining)
        for stale_connection in stale:
            stale_connection.close()
        return connection

    def _release(
        self,
        origin: Tuple[str, str, int],
        connection: http.client.HTTPConnection,
        *,
        reusable: bool,
    ) -> None:
        close_connection = not reusable
        with self._condition:
            state = self._origins.get(origin)
            if state is None:
                close_connection = True
            elif self._closed or not reusable:
                state.connection_count -= 1
                if state.connection_count <= 0 and not state.idle:
                    self._origins.pop(origin, None)
            elif len(state.idle) < self.max_idle_connections_per_origin:
                state.idle.append((connection, time.monotonic()))
            else:
                state.connection_count -= 1
                close_connection = True
                if state.connection_count <= 0 and not state.idle:
                    self._origins.pop(origin, None)
            self._condition.notify()
        if close_connection:
            connection.close()

    @staticmethod
    def _new_connection(origin: Tuple[str, str, int], timeout_sec: float) -> http.client.HTTPConnection:
        scheme, host, port = origin
        connection_type = http.client.HTTPSConnection if scheme == "https" else http.client.HTTPConnection
        return connection_type(host, port=port, timeout=timeout_sec)

    @staticmethod
    def _set_timeout(connection: http.client.HTTPConnection, timeout_sec: float) -> None:
        connection.timeout = timeout_sec
        if connection.sock is not None:
            connection.sock.settimeout(timeout_sec)

    @staticmethod
    def _origin(parsed) -> Tuple[str, str, int]:
        scheme = str(parsed.scheme or "").lower()
        if scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("url must use http or https and include a host")
        port = int(parsed.port or (443 if scheme == "https" else 80))
        return scheme, str(parsed.hostname), port

    @staticmethod
    def _request_target(parsed) -> str:
        path = parsed.path or "/"
        return f"{path}?{parsed.query}" if parsed.query else path


def _default_pool_size() -> int:
    raw = str(os.getenv("PYCLOUD_HTTP_MAX_CONNECTIONS_PER_ORIGIN", "") or "").strip()
    if not raw:
        return DEFAULT_MAX_CONNECTIONS_PER_ORIGIN
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_MAX_CONNECTIONS_PER_ORIGIN


def _default_idle_ttl_sec() -> float:
    raw = str(os.getenv("PYCLOUD_HTTP_IDLE_CONNECTION_TTL_SEC", "") or "").strip()
    if not raw:
        return DEFAULT_IDLE_CONNECTION_TTL_SEC
    try:
        return min(MAX_SHARED_IDLE_CONNECTION_TTL_SEC, max(0.1, float(raw)))
    except ValueError:
        return DEFAULT_IDLE_CONNECTION_TTL_SEC


shared_http_connection_pool = HttpConnectionPool(
    max_connections_per_origin=_default_pool_size(),
    idle_ttl_sec=_default_idle_ttl_sec(),
)


def pooled_http_request(**kwargs) -> BufferedHttpResponse:
    return shared_http_connection_pool.request(**kwargs)


__all__ = [
    "BufferedHttpResponse",
    "HttpConnectionPool",
    "MAX_SHARED_IDLE_CONNECTION_TTL_SEC",
    "pooled_http_request",
    "shared_http_connection_pool",
]
