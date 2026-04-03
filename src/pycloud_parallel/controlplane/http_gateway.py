from __future__ import annotations

"""Lightweight HTTP gateway for service-session mode."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Dict, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from pycloud_parallel.controlplane.serialization import serialize_arrow_compatible, serialize_inline_payload


InvokeHandler = Callable[[str, str, dict, str, float], Tuple[int, Dict[str, object]]]
StatusHandler = Callable[[str], Tuple[int, Dict[str, object]]]


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
    ) -> None:
        self._bind = bind
        self._invoke_handler = invoke_handler
        self._status_handler = status_handler
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self.base_url = ""

    def start(self) -> None:
        if self._server is not None:
            return
        host, port = _split_host_port(self._bind)

        invoke_handler = self._invoke_handler
        status_handler = self._status_handler

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
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
                    body = self.rfile.read(int(self.headers.get("Content-Length", "0") or 0))
                    try:
                        payload = json.loads(body.decode("utf-8") if body else "{}")
                    except Exception:
                        self._send_json(400, {"ok": False, "error": "invalid json body"})
                        return
                    if not isinstance(payload, dict):
                        self._send_json(400, {"ok": False, "error": "json body must be object"})
                        return
                    try:
                        payload, _, _ = serialize_inline_payload(payload, context="service call payload")
                    except ValueError as exc:
                        self._send_json(400, {"ok": False, "error": str(exc)})
                        return
                    token = self._extract_token()
                    code, resp = invoke_handler(service_id, method, payload, token, timeout_sec)
                    self._send_json(code, resp)
                    return

                self._send_json(404, {"ok": False, "error": "not found"})

            def do_GET(self):  # noqa: N802
                parsed = urlparse(self.path)
                parts = [x for x in parsed.path.split("/") if x]
                if len(parts) == 3 and parts[0] == "svc" and parts[2] == "status":
                    service_id = parts[1]
                    code, resp = status_handler(service_id)
                    self._send_json(code, resp)
                    return
                self._send_json(404, {"ok": False, "error": "not found"})

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
                raw = json.dumps(serialize_arrow_compatible(data), ensure_ascii=False).encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self._server = ThreadingHTTPServer((host, port), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, name="service-http-gateway", daemon=True)
        self._thread.start()

        public_host = "127.0.0.1" if host in ("0.0.0.0", "", "::") else host
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
