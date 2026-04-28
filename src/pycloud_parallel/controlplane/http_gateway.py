from __future__ import annotations

"""Lightweight HTTP gateway for service-session mode."""

import errno
import json
import threading
import traceback
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Union
from urllib.parse import parse_qs, urlparse

from .client_transport import (
    _decode_http_request_body_with_mode,
    _decode_http_transport_request_body_with_mode,
    _encode_http_json_body,
    _encode_http_transport_response_body,
    _is_http_transport_content_type,
)
from pycloud_parallel.controlplane.netutil import resolve_public_host
from pycloud_parallel.controlplane.serialization import serialize_arrow_compatible


@dataclass
class StreamingHttpResponse:
    status_code: int
    body_iter: Iterable[bytes]
    content_type: str = "application/x-ndjson; charset=utf-8"
    extra_headers: Dict[str, str] = field(default_factory=dict)


InvokeHandler = Callable[
    [str, str, dict, str, float, str, bool, bool],
    Union[Tuple[int, Dict[str, object]], StreamingHttpResponse],
]
StatusHandler = Callable[[str], Tuple[int, Dict[str, object]]]
MethodsHandler = Callable[[str, bool], Tuple[int, Dict[str, object]]]
ExtraGetHandler = Callable[[str, list[str], Dict[str, list[str]]], Optional[Tuple[Any, ...]]]


def _is_client_disconnect_error(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
        return True
    if isinstance(exc, OSError):
        return getattr(exc, "errno", None) in (errno.EPIPE, errno.ECONNRESET, errno.ECONNABORTED)
    return False


MAX_BODY_BYTES = 64 * 1024 * 1024
SERVICE_HTTP_REQUEST_QUEUE_SIZE = 1024


class _ServiceThreadingHTTPServer(ThreadingHTTPServer):
    # The stdlib default backlog is 5, which is too small for bursty async
    # service calls that fan out many HTTP connections to a single route.
    request_queue_size = SERVICE_HTTP_REQUEST_QUEUE_SIZE


def _split_host_port(bind: str) -> Tuple[str, int]:
    if ":" not in bind:
        raise ValueError("bind must be host:port")
    host, port = bind.rsplit(":", 1)
    return host.strip(), int(port)


class ServiceHttpGateway:
    def __init__(
        self,
        *,
        bind: str,
        invoke_handler: InvokeHandler,
        status_handler: StatusHandler,
        methods_handler: Optional[MethodsHandler] = None,
        extra_get_handler: Optional[ExtraGetHandler] = None,
    ) -> None:
        self._bind = bind
        self._invoke_handler = invoke_handler
        self._status_handler = status_handler
        self._methods_handler = methods_handler
        self._extra_get_handler = extra_get_handler
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._start_lock = threading.Lock()
        self.base_url = ""

    def start(self) -> None:
        with self._start_lock:
            if self._server is not None:
                return
            host, port = _split_host_port(self._bind)

        invoke_handler = self._invoke_handler
        status_handler = self._status_handler
        methods_handler = self._methods_handler
        extra_get_handler = self._extra_get_handler

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                try:
                    parsed = urlparse(self.path)
                    parts = [x for x in parsed.path.split("/") if x]
                    if len(parts) == 4 and parts[0] == "svc" and parts[2] == "call":
                        service_id = parts[1]
                        method = parts[3]
                        timeout_sec = 60.0
                        qs = parse_qs(parsed.query)
                        if "timeout_sec" in qs:
                            try:
                                timeout_sec = max(0.1, float(qs["timeout_sec"][0]))
                            except Exception:
                                timeout_sec = 60.0
                        try:
                            length = int(self.headers.get("Content-Length", "0") or 0)
                        except Exception:
                            length = 0
                        if length > MAX_BODY_BYTES:
                            self._send_json(413, {"ok": False, "error": "payload too large"})
                            return
                        body = self.rfile.read(max(0, length))
                        try:
                            if _is_http_transport_content_type(str(self.headers.get("Content-Type", "") or "")):
                                payload, serialization_mode = _decode_http_transport_request_body_with_mode(
                                    body,
                                    headers=self.headers,
                                    context="service_internal",
                                )
                            else:
                                payload, serialization_mode = _decode_http_request_body_with_mode(
                                    body,
                                    context="service_internal",
                                )
                        except ValueError as exc:
                            self._send_json(400, {"ok": False, "error": str(exc)})
                            return
                        token = self._extract_token()
                        wants_transport_response = _is_http_transport_content_type(
                            str(self.headers.get("Content-Type", "") or "")
                        )
                        stream_response = str((qs.get("stream", ["false"]) or ["false"])[0]).lower() in ("1", "true", "yes")
                        handled = invoke_handler(
                            service_id,
                            method,
                            payload,
                            token,
                            timeout_sec,
                            serialization_mode,
                            wants_transport_response,
                            stream_response,
                        )
                        if isinstance(handled, StreamingHttpResponse):
                            self._send_stream(handled)
                            return
                        code, resp = handled
                        if (
                            wants_transport_response
                            and int(code or 0) < 400
                            and isinstance(resp, dict)
                            and bool(resp.get("ok", False))
                            and "data" in resp
                        ):
                            raw, response_headers = _encode_http_transport_response_body(
                                resp.get("data"),
                                context="service_result",
                                mode=serialization_mode,
                            )
                            self._send_body(
                                code,
                                raw,
                                content_type=response_headers.pop("Content-Type", "application/octet-stream"),
                                extra_headers=response_headers,
                            )
                        else:
                            self._send_json(code, resp)
                        return

                    self._send_json(404, {"ok": False, "error": "not found"})
                except Exception as exc:
                    if _is_client_disconnect_error(exc):
                        return
                    self._send_json(
                        500,
                        {
                            "ok": False,
                            "error": f"service http handler failed: {exc}",
                            "error_type": type(exc).__name__,
                            "traceback": traceback.format_exc(limit=20),
                        },
                    )

            def do_GET(self):  # noqa: N802
                try:
                    parsed = urlparse(self.path)
                    parts = [x for x in parsed.path.split("/") if x]
                    qs = parse_qs(parsed.query)
                    if len(parts) == 3 and parts[0] == "svc" and parts[2] == "methods":
                        if methods_handler is None:
                            self._send_json(404, {"ok": False, "error": "methods unavailable"})
                            return
                        service_id = parts[1]
                        include_docs = str((qs.get("include_docs", ["false"]) or ["false"])[0]).lower() in ("1", "true", "yes")
                        code, resp = methods_handler(service_id, include_docs)
                        self._send_json(code, resp)
                        return
                    if len(parts) == 3 and parts[0] == "svc" and parts[2] == "status":
                        service_id = parts[1]
                        code, resp = status_handler(service_id)
                        self._send_json(code, resp)
                        return
                    if len(parts) >= 3 and parts[0] == "svc" and extra_get_handler is not None:
                        service_id = parts[1]
                        handled = extra_get_handler(service_id, parts[2:], qs)
                        if handled is not None:
                            if len(handled) == 3:
                                code, resp, content_type = handled
                                self._send_body(code, resp, content_type=str(content_type or "text/plain; charset=utf-8"))
                            else:
                                code, resp = handled
                                self._send_json(code, resp)
                            return
                    self._send_json(404, {"ok": False, "error": "not found"})
                except Exception as exc:
                    if _is_client_disconnect_error(exc):
                        return
                    self._send_json(
                        500,
                        {
                            "ok": False,
                            "error": f"service http handler failed: {exc}",
                            "error_type": type(exc).__name__,
                            "traceback": traceback.format_exc(limit=20),
                        },
                    )

            def log_message(self, fmt, *args):  # noqa: A003
                # Keep gateway logs quiet by default.
                return

            def _extract_token(self) -> str:
                x_token = self.headers.get("X-Service-Token", "").strip()
                if x_token:
                    return x_token
                auth = self.headers.get("Authorization", "").strip()
                low = auth.lower()
                if low.startswith("bearer "):
                    return auth[7:].strip()
                return ""

            def _send_json(self, status_code: int, data: Dict[str, object]) -> None:
                raw = _encode_http_json_body(data)
                self._send_body(status_code, raw, content_type="application/json; charset=utf-8")

            def _send_body(
                self,
                status_code: int,
                body: Any,
                *,
                content_type: str,
                extra_headers: Optional[Dict[str, str]] = None,
            ) -> None:
                raw = body if isinstance(body, (bytes, bytearray)) else str(body).encode("utf-8")
                try:
                    self.send_response(status_code)
                    self.send_header("Content-Type", str(content_type or "application/octet-stream"))
                    for key, value in dict(extra_headers or {}).items():
                        self.send_header(str(key), str(value))
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                except Exception as exc:
                    if not _is_client_disconnect_error(exc):
                        raise

            def _send_stream(self, response: StreamingHttpResponse) -> None:
                try:
                    self.send_response(int(response.status_code or 200))
                    self.send_header("Content-Type", str(response.content_type or "application/x-ndjson; charset=utf-8"))
                    self.send_header("Cache-Control", "no-cache")
                    self.send_header("Connection", "close")
                    for key, value in dict(response.extra_headers or {}).items():
                        self.send_header(str(key), str(value))
                    self.end_headers()
                    for chunk in response.body_iter:
                        if not chunk:
                            continue
                        raw = chunk if isinstance(chunk, (bytes, bytearray)) else str(chunk).encode("utf-8")
                        self.wfile.write(raw)
                        self.wfile.flush()
                except Exception as exc:
                    if not _is_client_disconnect_error(exc):
                        raise

        with self._start_lock:
            if self._server is not None:
                return
            self._server = _ServiceThreadingHTTPServer((host, port), _Handler)
            self._thread = threading.Thread(target=self._server.serve_forever, name="service-http-gateway", daemon=True)
            self._thread.start()

        public_host = resolve_public_host(host)
        actual_port = int(self._server.server_address[1])
        self.base_url = f"http://{public_host}:{actual_port}"

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None
