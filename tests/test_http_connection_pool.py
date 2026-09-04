from __future__ import annotations

from http.server import BaseHTTPRequestHandler
import http.client
import threading
import time

from pycloud_parallel.controlplane.bounded_http_server import (
    BoundedThreadPoolHTTPServer,
    DEFAULT_KEEPALIVE_IDLE_TIMEOUT_SEC,
)
from pycloud_parallel.controlplane.http_connection_pool import (
    DEFAULT_IDLE_CONNECTION_TTL_SEC,
    MAX_SHARED_IDLE_CONNECTION_TTL_SEC,
    HttpConnectionPool,
    _default_idle_ttl_sec,
)


def _start_server(handler):
    server = BoundedThreadPoolHTTPServer(("127.0.0.1", 0), handler, max_workers=4, queue_capacity=4)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _stop_server(server, thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=2.0)


def test_client_idle_ttl_precedes_server_keepalive_timeout() -> None:
    assert DEFAULT_IDLE_CONNECTION_TTL_SEC < DEFAULT_KEEPALIVE_IDLE_TIMEOUT_SEC


def test_shared_client_idle_ttl_env_is_clamped_below_server_timeout(monkeypatch) -> None:
    monkeypatch.setenv("PYCLOUD_HTTP_IDLE_CONNECTION_TTL_SEC", "30")

    assert _default_idle_ttl_sec() == MAX_SHARED_IDLE_CONNECTION_TTL_SEC
    assert _default_idle_ttl_sec() < DEFAULT_KEEPALIVE_IDLE_TIMEOUT_SEC


def test_pool_reuses_connection_for_same_origin() -> None:
    client_ports = []

    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):  # noqa: N802
            client_ports.append(self.client_address[1])
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, _format, *args):  # noqa: A002
            return

    server, thread = _start_server(_Handler)
    pool = HttpConnectionPool(max_connections_per_origin=2)
    url = f"http://127.0.0.1:{server.server_address[1]}/health"
    try:
        assert pool.request(url=url, method="GET", timeout_sec=1.0).body == b"ok"
        assert pool.request(url=url, method="GET", timeout_sec=1.0).body == b"ok"
        assert len(set(client_ports)) == 1
    finally:
        pool.close()
        _stop_server(server, thread)


def test_pool_discards_stale_connection_and_retries_once() -> None:
    client_ports = []

    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):  # noqa: N802
            client_ports.append(self.client_address[1])
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")
            if len(client_ports) == 1:
                self.close_connection = True

        def log_message(self, _format, *args):  # noqa: A002
            return

    server, thread = _start_server(_Handler)
    pool = HttpConnectionPool(max_connections_per_origin=2)
    url = f"http://127.0.0.1:{server.server_address[1]}/health"
    try:
        assert pool.request(url=url, method="GET", timeout_sec=1.0).body == b"ok"
        assert pool.request(url=url, method="GET", timeout_sec=1.0).body == b"ok"
        assert len(client_ports) == 2
        assert len(set(client_ports)) == 2
    finally:
        pool.close()
        _stop_server(server, thread)


def test_pool_does_not_retry_post_after_ambiguous_disconnect() -> None:
    calls = []

    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):  # noqa: N802
            calls.append(self.path)
            self.close_connection = True
            self.connection.shutdown(2)
            self.connection.close()

        def log_message(self, _format, *args):  # noqa: A002
            return

    server, thread = _start_server(_Handler)
    pool = HttpConnectionPool(max_connections_per_origin=2)
    url = f"http://127.0.0.1:{server.server_address[1]}/side-effect"
    try:
        try:
            pool.request(url=url, method="POST", body=b"work", timeout_sec=1.0)
        except (OSError, http.client.HTTPException):
            pass
        else:
            raise AssertionError("ambiguous POST disconnect must be surfaced")
        assert calls == ["/side-effect"]
    finally:
        pool.close()
        _stop_server(server, thread)


def test_pool_explicitly_retries_idempotent_post_after_lost_response() -> None:
    calls = []
    applied_keys = set()
    side_effect_count = 0

    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):  # noqa: N802
            nonlocal side_effect_count
            length = int(self.headers.get("Content-Length", "0") or 0)
            self.rfile.read(length)
            request_id = str(self.headers.get("X-Create-Request-Id", "") or "")
            calls.append(request_id)
            if request_id not in applied_keys:
                applied_keys.add(request_id)
                side_effect_count += 1
            if len(calls) == 1:
                self.close_connection = True
                self.connection.shutdown(2)
                self.connection.close()
                return
            body = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format, *args):  # noqa: A002
            return

    server, thread = _start_server(_Handler)
    pool = HttpConnectionPool(max_connections_per_origin=2)
    url = f"http://127.0.0.1:{server.server_address[1]}/create"
    try:
        response = pool.request(
            url=url,
            method="POST",
            body=b"create",
            headers={"X-Create-Request-Id": "create-1"},
            timeout_sec=1.0,
            retry_connection_error=True,
        )
        assert response.body == b'{"ok":true}'
        assert calls == ["create-1", "create-1"]
        assert applied_keys == {"create-1"}
        assert side_effect_count == 1
    finally:
        pool.close()
        _stop_server(server, thread)


def test_pool_does_not_reuse_http_error_connection() -> None:
    client_ports = []

    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):  # noqa: N802
            client_ports.append(self.client_address[1])
            status = 401 if len(client_ports) == 1 else 200
            body = b'{"ok":false}' if status == 401 else b'{"ok":true}'
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format, *args):  # noqa: A002
            return

    server, thread = _start_server(_Handler)
    pool = HttpConnectionPool(max_connections_per_origin=2)
    url = f"http://127.0.0.1:{server.server_address[1]}/auth"
    try:
        assert pool.request(url=url, method="POST", body=b"unread", timeout_sec=1.0).status == 401
        assert pool.request(url=url, method="POST", body=b"next", timeout_sec=1.0).status == 200
        assert len(set(client_ports)) == 2
    finally:
        pool.close()
        _stop_server(server, thread)


def test_pool_bounds_concurrent_connections_per_origin() -> None:
    release = threading.Event()
    first_started = threading.Event()
    active = 0
    max_active = 0
    lock = threading.Lock()

    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):  # noqa: N802
            nonlocal active, max_active
            with lock:
                active += 1
                max_active = max(max_active, active)
                first_started.set()
            release.wait(timeout=2.0)
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")
            with lock:
                active -= 1

        def log_message(self, _format, *args):  # noqa: A002
            return

    server, server_thread = _start_server(_Handler)
    pool = HttpConnectionPool(max_connections_per_origin=1)
    url = f"http://127.0.0.1:{server.server_address[1]}/work"
    results = []

    def _request() -> None:
        results.append(pool.request(url=url, method="GET", timeout_sec=2.0).body)

    first = threading.Thread(target=_request)
    second = threading.Thread(target=_request)
    try:
        first.start()
        assert first_started.wait(timeout=1.0)
        second.start()
        time.sleep(0.1)
        assert max_active == 1
        release.set()
        first.join(timeout=2.0)
        second.join(timeout=2.0)
        assert results == [b"ok", b"ok"]
        assert max_active == 1
    finally:
        release.set()
        first.join(timeout=2.0)
        second.join(timeout=2.0)
        pool.close()
        _stop_server(server, server_thread)


def test_pool_bounds_idle_connections_per_origin() -> None:
    release = threading.Event()
    started = threading.Barrier(3)

    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):  # noqa: N802
            started.wait(timeout=2.0)
            release.wait(timeout=2.0)
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, _format, *args):  # noqa: A002
            return

    server, server_thread = _start_server(_Handler)
    pool = HttpConnectionPool(max_connections_per_origin=2, max_idle_connections_per_origin=1)
    url = f"http://127.0.0.1:{server.server_address[1]}/work"
    requests = [threading.Thread(target=lambda: pool.request(url=url, method="GET", timeout_sec=2.0)) for _ in range(2)]
    try:
        for request in requests:
            request.start()
        started.wait(timeout=2.0)
        release.set()
        for request in requests:
            request.join(timeout=2.0)
        origin_state = next(iter(pool._origins.values()))
        assert origin_state.connection_count == 1
        assert len(origin_state.idle) == 1
    finally:
        release.set()
        for request in requests:
            request.join(timeout=2.0)
        pool.close()
        _stop_server(server, server_thread)
