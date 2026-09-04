from __future__ import annotations

import http.client
from http.server import BaseHTTPRequestHandler
import socket
import threading
import time

from pycloud_parallel.controlplane.bounded_http_server import BoundedThreadPoolHTTPServer
from pycloud_parallel.controlplane.client_transport import _encode_http_json_body
from pycloud_parallel.controlplane.http_gateway import ServiceHttpGateway, StreamingHttpResponse
from pycloud_parallel.controlplane.server import build_infocenter_server, build_nodecontrol_server


def _wait_until(predicate, *, timeout_sec: float = 3.0) -> None:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met before timeout")


def _open_request(port: int) -> socket.socket:
    request = socket.create_connection(("127.0.0.1", port), timeout=2.0)
    request.settimeout(2.0)
    request.sendall(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
    return request


def test_bounded_http_server_caps_workers_and_rejects_overload() -> None:
    release = threading.Event()
    started = 0
    started_lock = threading.Lock()

    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):  # noqa: N802
            nonlocal started
            with started_lock:
                started += 1
            release.wait(timeout=3.0)
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, _format, *args):  # noqa: A002
            return

    server = BoundedThreadPoolHTTPServer(
        ("127.0.0.1", 0),
        _Handler,
        max_workers=2,
        queue_capacity=1,
        thread_name_prefix="bounded-http-test",
    )
    serve_thread = threading.Thread(target=server.serve_forever, daemon=True)
    serve_thread.start()
    requests: list[socket.socket] = []
    try:
        requests.append(_open_request(server.server_address[1]))
        _wait_until(lambda: started == 1)
        requests.append(_open_request(server.server_address[1]))
        _wait_until(lambda: started == 2)
        requests.append(_open_request(server.server_address[1]))
        _wait_until(lambda: len(server._active_requests) == 3)
        rejected = _open_request(server.server_address[1])
        requests.append(rejected)
        _wait_until(lambda: server.rejected_requests == 1)

        worker_threads = [thread for thread in threading.enumerate() if thread.name.startswith("bounded-http-test")]
        assert len(worker_threads) == 2
        assert started == 2
        try:
            assert rejected.recv(1) == b""
        except (ConnectionResetError, OSError):
            pass
    finally:
        release.set()
        for request in requests:
            request.close()
        server.shutdown()
        server.server_close()
        serve_thread.join(timeout=2.0)

    assert not serve_thread.is_alive()
    _wait_until(
        lambda: not [
            thread
            for thread in threading.enumerate()
            if thread.name.startswith("bounded-http-test")
        ]
    )


def test_server_close_terminates_idle_http11_keepalive_worker() -> None:
    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, _format, *args):  # noqa: A002
            return

    server = BoundedThreadPoolHTTPServer(
        ("127.0.0.1", 0),
        _Handler,
        max_workers=1,
        queue_capacity=0,
        thread_name_prefix="keepalive-http-test",
    )
    serve_thread = threading.Thread(target=server.serve_forever, daemon=True)
    serve_thread.start()
    connection = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=2.0)
    connection.request("GET", "/")
    response = connection.getresponse()
    assert response.read() == b"ok"

    server.shutdown()
    server.server_close()
    serve_thread.join(timeout=2.0)
    connection.close()

    assert not serve_thread.is_alive()
    _wait_until(
        lambda: not [
            thread
            for thread in threading.enumerate()
            if thread.name.startswith("keepalive-http-test")
        ]
    )


def test_server_close_does_not_wait_forever_for_stuck_handler() -> None:
    started = threading.Event()
    release = threading.Event()

    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):  # noqa: N802
            started.set()
            release.wait(timeout=5.0)

        def log_message(self, _format, *args):  # noqa: A002
            return

    server = BoundedThreadPoolHTTPServer(
        ("127.0.0.1", 0),
        _Handler,
        max_workers=1,
        queue_capacity=0,
        thread_name_prefix="stuck-close-http-test",
    )
    serve_thread = threading.Thread(target=server.serve_forever, daemon=True)
    serve_thread.start()
    request = _open_request(server.server_address[1])
    try:
        assert started.wait(timeout=1.0)
        server.shutdown()
        started_at = time.monotonic()
        server.server_close()
        assert time.monotonic() - started_at < 0.5
        assert all(worker.daemon for worker in server._worker_threads)
    finally:
        release.set()
        request.close()
        serve_thread.join(timeout=2.0)
        _wait_until(
            lambda: not [
                thread
                for thread in threading.enumerate()
                if thread.name.startswith("stuck-close-http-test")
            ]
        )


def test_http11_keepalive_idle_timeout_releases_worker() -> None:
    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, _format, *args):  # noqa: A002
            return

    server = BoundedThreadPoolHTTPServer(
        ("127.0.0.1", 0),
        _Handler,
        max_workers=1,
        queue_capacity=0,
        keepalive_idle_timeout_sec=0.1,
        thread_name_prefix="idle-timeout-http-test",
    )
    serve_thread = threading.Thread(target=server.serve_forever, daemon=True)
    serve_thread.start()
    connection = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=2.0)
    try:
        connection.request("GET", "/")
        response = connection.getresponse()
        assert response.read() == b"ok"
        _wait_until(lambda: len(server._active_requests) == 0)
        assert connection.sock is not None
        connection.sock.settimeout(0.5)
        assert connection.sock.recv(1) == b""
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        serve_thread.join(timeout=2.0)


def test_idle_keepalive_connections_do_not_starve_queued_request() -> None:
    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, _format, *args):  # noqa: A002
            return

    server = BoundedThreadPoolHTTPServer(
        ("127.0.0.1", 0),
        _Handler,
        max_workers=2,
        queue_capacity=2,
        thread_name_prefix="keepalive-fairness-test",
    )
    serve_thread = threading.Thread(target=server.serve_forever, daemon=True)
    serve_thread.start()
    connections = []
    try:
        for _index in range(2):
            connection = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=2.0)
            connection.request("GET", "/")
            assert connection.getresponse().read() == b"ok"
            connections.append(connection)

        queued = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=1.5)
        connections.append(queued)
        started_at = time.monotonic()
        queued.request("GET", "/")
        assert queued.getresponse().read() == b"ok"
        assert time.monotonic() - started_at < 1.2
    finally:
        for connection in connections:
            connection.close()
        server.shutdown()
        server.server_close()
        serve_thread.join(timeout=2.0)


def test_nodecontrol_builder_applies_max_workers(tmp_path) -> None:
    server, state = build_nodecontrol_server(
        "127.0.0.1:0",
        node_id="bounded-node",
        artifact_dir=str(tmp_path / "bounded_node"),
        service_http_bind="",
        max_workers=3,
    )
    try:
        server.start()
        assert server.max_workers == 3
        assert server._server is not None
        assert server._server.max_workers == 3
        assert server._server.RequestHandlerClass.protocol_version == "HTTP/1.1"
    finally:
        server.stop()
        state.close()


def test_infocenter_responses_close_connections_to_preserve_worker_capacity() -> None:
    server = build_infocenter_server("127.0.0.1:0", max_workers=2)
    server.start()
    connection = http.client.HTTPConnection(server.base_url.split("://", 1)[-1], timeout=2.0)
    try:
        connection.request("GET", "/nodes?limit=1")
        response = connection.getresponse()
        response.read()
        assert response.version == 11
        assert response.getheader("Connection") == "close"
        assert response.will_close is True
    finally:
        connection.close()
        server.stop()


def test_service_gateway_http11_response_headers() -> None:
    def _invoke(*_args):
        return StreamingHttpResponse(status_code=200, body_iter=[b'{"value":1}\n'])

    gateway = ServiceHttpGateway(
        bind="127.0.0.1:0",
        invoke_handler=_invoke,
        status_handler=lambda service_id: (200, {"ok": True, "service_id": service_id}),
        max_workers=2,
        queue_capacity=1,
    )
    gateway.start()
    try:
        connection = http.client.HTTPConnection(gateway.base_url.split("://", 1)[-1], timeout=2.0)
        connection.request("GET", "/")
        response = connection.getresponse()
        assert response.version == 11
        assert int(response.getheader("Content-Length", "-1")) >= 0
        response.read()
        connection.close()

        connection = http.client.HTTPConnection(gateway.base_url.split("://", 1)[-1], timeout=2.0)
        body = _encode_http_json_body({})
        connection.request(
            "POST",
            "/svc/svc-1/call/run?stream=1",
            body=body,
            headers={"Content-Type": "application/json; charset=utf-8", "Content-Length": str(len(body))},
        )
        response = connection.getresponse()
        assert response.version == 11
        assert response.getheader("Connection") == "close"
        assert response.read() == b'{"value":1}\n'
        connection.close()
    finally:
        gateway.stop()
