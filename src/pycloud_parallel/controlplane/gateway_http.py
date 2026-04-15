from __future__ import annotations

"""HTTP gateway for service-mode callers."""

import errno
import ipaddress
import json
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Dict, Optional, Sequence, Tuple
from urllib.error import HTTPError
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen

from pycloud_parallel.controlplane.data_ref import maybe_data_ref, with_data_ref_control_addr, with_data_ref_locator
from pycloud_parallel.controlplane.client import (
    InfoCenterServiceRoute,
    NodeControlClient,
    _decode_http_request_body,
    _decode_http_response_body,
    _encode_http_json_body,
    _serialize_http_call_payload,
)
from pycloud_parallel.controlplane.gateway_cache import GatewayRouteCache
from pycloud_parallel.controlplane.netutil import resolve_public_host
from pycloud_parallel.controlplane.serialization import serialize_arrow_compatible


def _is_client_disconnect_error(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
        return True
    if isinstance(exc, OSError):
        return getattr(exc, "errno", None) in (errno.EPIPE, errno.ECONNRESET, errno.ECONNABORTED)
    return False


MAX_BODY_BYTES = 64 * 1024 * 1024


def _split_host_port(bind: str) -> Tuple[str, int]:
    if ":" not in bind:
        raise ValueError("bind must be host:port")
    host, port = bind.rsplit(":", 1)
    return host.strip(), int(port)


@dataclass
class GatewayCallError(Exception):
    status_code: int
    data: Dict[str, object]

    def __str__(self) -> str:
        return str(self.data.get("error", f"http {self.status_code}"))


class GatewayHttpApp:
    def __init__(
        self,
        *,
        route_cache: GatewayRouteCache,
        timeout_sec: float = 10.0,
        allow_private_addrs: bool = True,
        register_data_ref: Optional[Callable[..., object]] = None,
        controlplane_target: str = "",
    ) -> None:
        self.route_cache = route_cache
        self.timeout_sec = max(0.1, float(timeout_sec))
        self._stopped = False
        self.allow_private_addrs = bool(allow_private_addrs)
        self.register_data_ref = register_data_ref
        self.controlplane_target = str(controlplane_target or "").strip()

    def start(self) -> None:
        self._stopped = False
        self.route_cache.start()

    def stop(self) -> None:
        self._stopped = True
        self.route_cache.stop()

    def handle_post(self, *, path: str, headers, body: bytes) -> Optional[Tuple[int, Dict[str, object]]]:
        if self._stopped:
            return 503, {"ok": False, "error": "gateway stopping"}
        parsed = urlparse(path)
        parts = [x for x in parsed.path.split("/") if x]
        if len(parts) != 4 or parts[0] != "svc" or parts[2] != "call":
            return None

        service_name = parts[1]
        method = parts[3]
        try:
            payload = _decode_http_request_body(body, context="service call payload")
        except ValueError as exc:
            return 400, {"ok": False, "error": str(exc)}

        qs = parse_qs(parsed.query)
        timeout_sec = self.timeout_sec
        if "timeout_sec" in qs:
            try:
                timeout_sec = max(0.1, float(qs["timeout_sec"][0]))
            except Exception:
                timeout_sec = self.timeout_sec
        service_token = self._extract_token(headers)

        tried = set()
        try:
            route = self.route_cache.select_route(service_name)
            tried.add(route.service_id)
            resp = self._invoke_route(route, method=method, payload=payload, timeout_sec=timeout_sec, service_token=service_token)
            self.route_cache.mark_success(route)
            return 200, resp
        except GatewayCallError as exc:
            if not self._is_route_failure(exc):
                return exc.status_code, exc.data
            self.route_cache.mark_failure(route, str(exc))
            try:
                self.route_cache.refresh(service_name, force=True)
                retry_route = self.route_cache.select_route(service_name, exclude_service_ids=tried)
                resp = self._invoke_route(retry_route, method=method, payload=payload, timeout_sec=timeout_sec, service_token=service_token)
                self.route_cache.mark_success(retry_route)
                return 200, resp
            except GatewayCallError as retry_exc:
                if self._is_route_failure(retry_exc):
                    self.route_cache.mark_failure(retry_route, str(retry_exc))
                return retry_exc.status_code, retry_exc.data
            except Exception as retry_exc:
                return 502, {"ok": False, "error": f"gateway retry failed: {retry_exc}"}
        except Exception as exc:
            return 502, {"ok": False, "error": f"gateway call failed: {exc}"}

    def handle_get(self, *, path: str, headers) -> Optional[Tuple[int, Dict[str, object]]]:
        if self._stopped:
            return 503, {"ok": False, "error": "gateway stopping"}
        del headers
        parsed = urlparse(path)
        parts = [x for x in parsed.path.split("/") if x]
        if len(parts) == 3 and parts[0] == "svc" and parts[2] == "methods":
            service_name = parts[1]
            qs = parse_qs(parsed.query)
            include_docs = str((qs.get("include_docs", ["false"]) or ["false"])[0]).lower() in ("1", "true", "yes")
            return self._methods(service_name, include_docs=include_docs)
        if len(parts) == 3 and parts[0] == "svc" and parts[2] == "status":
            service_name = parts[1]
            return self._status(service_name)
        return None

    def _methods(self, service_name: str, *, include_docs: bool) -> Tuple[int, Dict[str, object]]:
        tried = set()
        try:
            route = self.route_cache.select_route(service_name)
            tried.add(route.service_id)
            resp = self._list_methods(route, include_docs=include_docs)
            self.route_cache.mark_success(route)
            return 200, resp
        except Exception as exc:
            if tried:
                self.route_cache.mark_failure(route, str(exc))
            try:
                self.route_cache.refresh(service_name, force=True)
                retry_route = self.route_cache.select_route(service_name, exclude_service_ids=tried)
                resp = self._list_methods(retry_route, include_docs=include_docs)
                self.route_cache.mark_success(retry_route)
                return 200, resp
            except Exception as exc:
                if "retry_route" in locals():
                    self.route_cache.mark_failure(retry_route, str(exc))
                return 502, {"ok": False, "error": f"gateway list methods failed: {exc}"}

    def _status(self, service_name: str) -> Tuple[int, Dict[str, object]]:
        try:
            info = self.route_cache.snapshot_info(service_name)
        except Exception as exc:
            return 502, {"ok": False, "error": f"gateway status failed: {exc}"}
        routes: Sequence[InfoCenterServiceRoute] = info["routes"]
        if not routes:
            return 404, {"ok": False, "error": f"service not found: {service_name}"}
        serialized = []
        for route in routes:
            serialized.append(
                {
                    "service_name": route.service_name,
                    "service_id": route.service_id,
                    "node_instance_id": route.node_instance_id,
                    "node_id": route.node_id,
                    "control_addr": route.control_addr,
                    "node_healthy": route.node_healthy,
                    "worker_count": route.worker_count,
                    "alive_workers": route.alive_workers,
                    "in_flight": route.in_flight,
                    "reported_in_flight": route.reported_in_flight,
                    "received_count": route.received_count,
                    "returned_count": route.returned_count,
                    "ema_child_invoke_ms": route.ema_child_invoke_ms,
                    "ema_samples": route.ema_samples,
                    "predicted_busy": route.predicted_busy,
                    "http_base_url": route.http_base_url,
                    "status": int(route.status),
                    "lease_expire_at": route.lease_expire_at.isoformat(),
                }
            )
        return 200, {
            "ok": True,
            "service_name": service_name,
            "refreshed_at": info["refreshed_at"],
            "route_count": info["route_count"],
            "routes": serialized,
        }

    def _list_methods(self, route: InfoCenterServiceRoute, *, include_docs: bool) -> Dict[str, object]:
        if not str(route.control_addr or "").strip():
            base_url = self._validate_route_url(route.http_base_url)
            if not base_url:
                raise GatewayCallError(status_code=502, data={"ok": False, "error": "invalid route http_base_url"})
            url = f"{base_url}/methods?include_docs={'true' if include_docs else 'false'}"
            req = Request(url, method="GET")
            try:
                with urlopen(req, timeout=max(2.0, self.timeout_sec + 1.0)) as resp:
                    data = json.loads(resp.read().decode("utf-8") or "{}")
            except HTTPError as exc:
                try:
                    data = json.loads((exc.read() or b"{}").decode("utf-8") or "{}")
                except Exception:
                    data = {"ok": False, "error": exc.reason}
                raise GatewayCallError(status_code=exc.code, data=data) from exc
            except Exception as exc:
                raise GatewayCallError(status_code=502, data={"ok": False, "error": repr(exc)}) from exc
            if not isinstance(data, dict):
                raise GatewayCallError(status_code=502, data={"ok": False, "error": "invalid json response"})
            if not data.get("ok", False):
                raise GatewayCallError(status_code=200, data=data)
            return data
        with NodeControlClient(route.control_addr, timeout_sec=self.timeout_sec) as client:
            methods = client.list_service_methods(service_id=route.service_id, include_docs=include_docs)
        return {
            "ok": True,
            "service_name": route.service_name,
            "service_id": route.service_id,
            "methods": [
                {
                    "method": item.method,
                    "qualified_name": item.qualified_name,
                    "doc": item.doc,
                }
                for item in methods
            ],
        }

    def _invoke_route(
        self,
        route: InfoCenterServiceRoute,
        *,
        method: str,
        payload: Dict[str, object],
        timeout_sec: float,
        service_token: str,
    ) -> Dict[str, object]:
        base_url = self._validate_route_url(route.http_base_url)
        if not base_url:
            raise GatewayCallError(status_code=502, data={"ok": False, "error": "invalid route http_base_url"})
        url = f"{base_url}/call/{quote(method, safe='')}?timeout_sec={max(0.1, timeout_sec):.3f}"
        headers = {"Content-Type": "application/json"}
        if service_token:
            headers["X-Service-Token"] = service_token
        req = Request(
            url=url,
            method="POST",
            headers=headers,
            data=_encode_http_json_body(_serialize_http_call_payload(payload, context="service call payload")),
        )
        try:
            with urlopen(req, timeout=max(2.0, timeout_sec + 1.0)) as resp:
                raw = resp.read(MAX_BODY_BYTES + 1)
                if len(raw) > MAX_BODY_BYTES:
                    raise GatewayCallError(status_code=502, data={"ok": False, "error": "response too large"})
                data = _decode_http_response_body(raw, control_addr=route.control_addr)
        except HTTPError as exc:
            try:
                raw = exc.read() or b"{}"
                if len(raw) > MAX_BODY_BYTES:
                    data = {"ok": False, "error": "response too large"}
                else:
                    data = _decode_http_response_body(raw)
            except Exception:
                data = {"ok": False, "error": exc.reason}
            raise GatewayCallError(status_code=exc.code, data=data) from exc
        except Exception as exc:
            raise GatewayCallError(status_code=502, data={"ok": False, "error": repr(exc)}) from exc
        if not isinstance(data, dict):
            raise GatewayCallError(status_code=502, data={"ok": False, "error": "invalid json response"})
        if not data.get("ok", False):
            raise GatewayCallError(status_code=200, data=data)
        return self._attach_controlplane_locator(
            _attach_result_ref_control_addr(data, control_addr=route.control_addr),
            route=route,
        )

    def _extract_token(self, headers) -> str:
        x_token = str(headers.get("X-Service-Token", "") or "").strip()
        if x_token:
            return x_token
        auth = str(headers.get("Authorization", "") or "").strip()
        low = auth.lower()
        if low.startswith("bearer "):
            return auth[7:].strip()
        return ""

    def _is_route_failure(self, exc: GatewayCallError) -> bool:
        if exc.status_code == 502:
            return True
        if exc.status_code == 200:
            return False
        if exc.status_code not in (404, 409, 500):
            return False
        msg = str(exc.data.get("error", "") or "").lower()
        return any(text in msg for text in ("service not found", "service not running", "service executor stopped", "artifact missing"))

    def _validate_route_url(self, http_base_url: str) -> str:
        raw = str(http_base_url or "").strip()
        if not raw:
            return ""
        parsed = urlparse(raw)
        if parsed.scheme not in ("http", "https"):
            return ""
        if not parsed.netloc:
            return ""
        host = parsed.hostname or ""
        if not host:
            return ""
        if host in ("localhost",):
            return ""
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            return raw.rstrip("/")
        if ip.is_multicast or ip.is_unspecified:
            return ""
        if not self.allow_private_addrs and (ip.is_private or ip.is_loopback or ip.is_link_local):
            return ""
        if str(ip) == "169.254.169.254":
            return ""
        return raw.rstrip("/")

    def _attach_controlplane_locator(self, data: Dict[str, object], *, route: InfoCenterServiceRoute) -> Dict[str, object]:
        if not isinstance(data, dict) or "data" not in data:
            return data
        updated = with_data_ref_locator(
            data.get("data"),
            locator_kind="controlplane" if self.controlplane_target else "node_control",
            locator_token=self.controlplane_target or str(route.control_addr or ""),
            node_id=str(route.node_id or ""),
            node_instance_id=str(route.node_instance_id or ""),
        )
        if updated is data.get("data"):
            return data
        ref = maybe_data_ref(updated)
        if ref is not None and callable(self.register_data_ref):
            try:
                self.register_data_ref(
                    ref=updated,
                    node_id=str(route.node_id or ""),
                    node_instance_id=str(route.node_instance_id or ""),
                    control_addr=str(route.control_addr or ""),
                    locator_kind="node_control",
                    locator_token=str(route.control_addr or ""),
                )
            except Exception:
                pass
        body = dict(data)
        body["data"] = updated
        return body


class GatewayHttpServer:
    def __init__(self, *, bind: str, app: GatewayHttpApp) -> None:
        self._bind = bind
        self.app = app
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._start_lock = threading.Lock()
        self.base_url = ""

    def start(self) -> None:
        with self._start_lock:
            if self._server is not None:
                return
            host, port = _split_host_port(self._bind)
            app = self.app
            app.start()

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                try:
                    length = int(self.headers.get("Content-Length", "0") or 0)
                except Exception:
                    length = 0
                if length > MAX_BODY_BYTES:
                    self._send_json(413, {"ok": False, "error": "payload too large"})
                    return
                body = self.rfile.read(max(0, length))
                handled = app.handle_post(path=self.path, headers=self.headers, body=body)
                if handled is None:
                    self._send_json(404, {"ok": False, "error": "not found"})
                    return
                code, resp = handled
                self._send_json(code, resp)

            def do_GET(self):  # noqa: N802
                handled = app.handle_get(path=self.path, headers=self.headers)
                if handled is None:
                    self._send_json(404, {"ok": False, "error": "not found"})
                    return
                code, resp = handled
                self._send_json(code, resp)

            def log_message(self, fmt, *args):  # noqa: A003
                return

            def _send_json(self, status_code: int, data: Dict[str, object]) -> None:
                raw = _encode_http_json_body(data)
                try:
                    self.send_response(status_code)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                except Exception as exc:
                    if not _is_client_disconnect_error(exc):
                        raise

        with self._start_lock:
            if self._server is not None:
                return
            self._server = ThreadingHTTPServer((host, port), _Handler)
            self._thread = threading.Thread(target=self._server.serve_forever, name="gateway-http", daemon=True)
            self._thread.start()
        public_host = resolve_public_host(host)
        actual_port = int(self._server.server_address[1])
        self.base_url = f"http://{public_host}:{actual_port}"

    def wait_for_termination(self) -> None:
        thread = self._thread
        if thread is not None:
            thread.join()

    def stop(self, grace: int = 0) -> None:
        del grace
        self.app.stop()
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None


def _attach_result_ref_control_addr(data: Dict[str, object], *, control_addr: str) -> Dict[str, object]:
    if not isinstance(data, dict):
        return data
    result = data.get("data")
    updated = with_data_ref_control_addr(result, control_addr=control_addr)
    if updated is not result:
        data = dict(data)
        data["data"] = updated
    return data
