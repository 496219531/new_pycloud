from __future__ import annotations

"""HTTP gateway for service-mode transport requests."""

import errno
import contextlib
import ipaddress
import json
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, BinaryIO, Callable, Dict, Iterable, Optional, Sequence, Tuple
from urllib.error import HTTPError
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen

from pycloud_parallel.controlplane.config import (
    GATEWAY_MAX_UPLOAD_FILE_BYTES,
    GATEWAY_MAX_UPLOAD_TOTAL_BYTES,
)
from .client_transport import (
    _decode_http_request_body,
    _decode_http_request_body_with_mode,
    _decode_http_transport_request_body_with_mode,
    _decode_http_response_with_headers,
    _decode_http_response_body,
    _encode_http_json_body,
    _encode_http_transport_body,
    _encode_http_transport_response_body,
    _is_http_transport_content_type,
    _iter_route_http_stream,
    _serialize_http_call_payload,
)
from pycloud_parallel.controlplane.data_ref import maybe_data_ref, with_data_ref_control_addr, with_data_ref_locator
from pycloud_parallel.controlplane.gateway_cache import GatewayRouteCache
from pycloud_parallel.controlplane.http_gateway import StreamingHttpResponse
from pycloud_parallel.controlplane.gateway_stage import GatewayStageManager
from pycloud_parallel.controlplane.gateway_upload import (
    collect_used_upload_slots,
    GatewayUploadError,
    is_gateway_upload_call_path,
    parse_gateway_upload_call,
    release_uploaded_refs_on_route,
    rewrite_payload_with_uploaded_refs,
    upload_staged_files_to_route,
)
from pycloud_parallel.controlplane.infocenter_client import InfoCenterServiceRoute
from pycloud_parallel.controlplane.netutil import resolve_public_host
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.controlplane.serialization import (
    INLINE_TRANSPORT_CARRIER_SENTINEL,
    _adapt_blob_for_json_transport,
    is_inline_transport_carrier,
    serialize_arrow_compatible,
)


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


def _stream_json_safe(value: Any) -> Any:
    if is_inline_transport_carrier(value):
        meta = dict(value.get(INLINE_TRANSPORT_CARRIER_SENTINEL) or {})
        meta["content_bytes"] = _adapt_blob_for_json_transport(meta.get("content_bytes", b""))
        return {INLINE_TRANSPORT_CARRIER_SENTINEL: meta}
    if isinstance(value, dict):
        return {key: _stream_json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_stream_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_stream_json_safe(item) for item in value]
    return value


def _encode_stream_line(event: Dict[str, object]) -> bytes:
    return json.dumps(serialize_arrow_compatible(_stream_json_safe(event)), ensure_ascii=False).encode("utf-8") + b"\n"


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
        stage_manager: Optional[GatewayStageManager] = None,
        max_upload_file_bytes: int = GATEWAY_MAX_UPLOAD_FILE_BYTES,
        max_upload_total_bytes: int = GATEWAY_MAX_UPLOAD_TOTAL_BYTES,
    ) -> None:
        self.route_cache = route_cache
        self.timeout_sec = max(0.1, float(timeout_sec))
        self._stopped = False
        self.allow_private_addrs = bool(allow_private_addrs)
        self.register_data_ref = register_data_ref
        self.controlplane_target = str(controlplane_target or "").strip()
        self.stage_manager = stage_manager or GatewayStageManager()
        self.max_upload_file_bytes = max(1, int(max_upload_file_bytes or GATEWAY_MAX_UPLOAD_FILE_BYTES))
        self.max_upload_total_bytes = max(self.max_upload_file_bytes, int(max_upload_total_bytes or GATEWAY_MAX_UPLOAD_TOTAL_BYTES))

    def start(self) -> None:
        self._stopped = False
        self.route_cache.start()
        self.stage_manager.start()

    def stop(self) -> None:
        self._stopped = True
        self.route_cache.stop()
        self.stage_manager.stop()

    def handle_post_stream(
        self,
        *,
        path: str,
        headers,
        stream: BinaryIO,
        content_length: int,
    ) -> Optional[Tuple[Any, ...]]:
        if self._stopped:
            return 503, {"ok": False, "error": "gateway stopping"}
        parsed = urlparse(path)
        if not is_gateway_upload_call_path(parsed.path):
            return None

        parts = [x for x in parsed.path.split("/") if x]
        service_name = parts[1]
        method = parts[3]
        qs = parse_qs(parsed.query)
        timeout_sec = self.timeout_sec
        if "timeout_sec" in qs:
            try:
                timeout_sec = max(0.1, float(qs["timeout_sec"][0]))
            except Exception:
                timeout_sec = self.timeout_sec
        service_token = self._extract_token(headers)
        try:
            parsed_upload = parse_gateway_upload_call(
                headers=headers,
                stream=stream,
                content_length=max(0, int(content_length or 0)),
                service_name=service_name,
                method=method,
                stage_manager=self.stage_manager,
                max_total_bytes=self.max_upload_total_bytes,
                max_file_bytes=self.max_upload_file_bytes,
            )
        except GatewayUploadError as exc:
            message = str(exc)
            status_code = 413 if "limit" in message.lower() or "too large" in message.lower() else 400
            return status_code, {"ok": False, "error": message}
        except Exception as exc:
            return 400, {"ok": False, "error": f"invalid upload-call request: {exc}"}

        request = parsed_upload.request
        try:
            parsed_upload.used_slots = tuple(
                collect_used_upload_slots(
                    payload=parsed_upload.payload,
                    file_slots=tuple(parsed_upload.files.keys()),
                    file_map=parsed_upload.file_map,
                )
            )
        except GatewayUploadError as exc:
            self.stage_manager.cleanup(request)
            return 400, {"ok": False, "error": str(exc)}
        tried = set()
        attempt_count = 0
        failed_count = 0
        last_failed_route_id = ""
        last_error: Exception | None = None

        def _record_observation(*, selected_route_id: str = "") -> None:
            recorder = getattr(self.route_cache, "record_call_observation", None)
            if callable(recorder):
                recorder(
                    service_name,
                    route_attempt_count=attempt_count,
                    failed_route_count=failed_count,
                    last_failed_route_id=last_failed_route_id,
                    selected_route_id=selected_route_id,
                )

        while True:
            try:
                route = self.route_cache.select_route(service_name, exclude_service_ids=tried)
            except Exception as exc:
                _record_observation()
                message = str(last_error or exc)
                self.stage_manager.preserve_failure(request, status="failed")
                return 502, {"ok": False, "error": f"gateway upload-call failed: {message}"}
            route_id = str(route.service_id or "")
            if route_id in tried:
                self.route_cache.release_route(route)
                _record_observation()
                message = str(last_error or f"no untried route for service_name={service_name}")
                self.stage_manager.preserve_failure(request, status="failed")
                return 502, {"ok": False, "error": f"gateway upload-call failed: {message}"}
            tried.add(route_id)
            attempt_count += 1
            try:
                resp = self._invoke_uploaded_route(
                    route,
                    method=method,
                    parsed_upload=parsed_upload,
                    timeout_sec=timeout_sec,
                    service_token=service_token,
                )
                self.route_cache.mark_success(route)
                _record_observation(selected_route_id=route_id)
                self.stage_manager.cleanup(request)
                return 200, resp
            except GatewayCallError as exc:
                last_error = exc
                self.stage_manager.preserve_failure(request, status="call_failed")
                if not self._is_route_failure(exc):
                    self.route_cache.release_route(route)
                    _record_observation()
                    return exc.status_code, exc.data
                failed_count += 1
                last_failed_route_id = route_id
                self.route_cache.mark_failure(route, str(exc))
                with contextlib.suppress(Exception):
                    self.route_cache.refresh(service_name, force=True)
                continue
            except Exception as exc:
                last_error = exc
                self.route_cache.release_route(route)
                _record_observation()
                self.stage_manager.preserve_failure(request, status="failed")
                return 502, {"ok": False, "error": f"gateway upload-call failed: {exc}"}

    def handle_post(self, *, path: str, headers, body: bytes) -> Optional[Tuple[Any, ...]]:
        if self._stopped:
            return 503, {"ok": False, "error": "gateway stopping"}
        parsed = urlparse(path)
        parts = [x for x in parsed.path.split("/") if x]
        if len(parts) != 4 or parts[0] != "svc" or parts[2] != "call":
            return None

        service_name = parts[1]
        method = parts[3]
        try:
            if _is_http_transport_content_type(str(headers.get("Content-Type", "") or "")):
                payload, serialization_mode = _decode_http_transport_request_body_with_mode(
                    body,
                    headers=headers,
                    context="gateway_public",
                )
            else:
                payload, serialization_mode = _decode_http_request_body_with_mode(
                    body,
                    context="gateway_public",
                )
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
        stream_response = str((qs.get("stream", ["false"]) or ["false"])[0]).lower() in ("1", "true", "yes")
        if stream_response:
            return self._handle_stream_call(
                service_name=service_name,
                method=method,
                payload=payload,
                timeout_sec=timeout_sec,
                service_token=service_token,
                serialization_mode=serialization_mode,
            )

        tried = set()
        attempt_count = 0
        failed_count = 0
        last_failed_route_id = ""
        last_error: Exception | None = None

        def _route_success_response(resp: Dict[str, object]):
            if (
                _is_http_transport_content_type(str(headers.get("Content-Type", "") or ""))
                and isinstance(resp, dict)
                and bool(resp.get("ok", False))
                and "data" in resp
            ):
                raw, response_headers = _encode_http_transport_response_body(
                    resp.get("data"),
                    context="service_result",
                    mode=serialization_mode,
                )
                return 200, raw, response_headers.pop("Content-Type", "application/octet-stream"), response_headers
            return 200, resp

        def _record_observation(*, selected_route_id: str = "") -> None:
            recorder = getattr(self.route_cache, "record_call_observation", None)
            if callable(recorder):
                recorder(
                    service_name,
                    route_attempt_count=attempt_count,
                    failed_route_count=failed_count,
                    last_failed_route_id=last_failed_route_id,
                    selected_route_id=selected_route_id,
                )

        while True:
            try:
                route = self.route_cache.select_route(service_name, exclude_service_ids=tried)
            except Exception as exc:
                _record_observation()
                message = str(last_error or exc)
                return 502, {"ok": False, "error": f"gateway call failed: {message}"}
            route_id = str(route.service_id or "")
            if route_id in tried:
                self.route_cache.release_route(route)
                _record_observation()
                message = str(last_error or f"no untried route for service_name={service_name}")
                return 502, {"ok": False, "error": f"gateway call failed: {message}"}
            tried.add(route_id)
            attempt_count += 1
            try:
                resp = self._invoke_route(
                    route,
                    method=method,
                    payload=payload,
                    timeout_sec=timeout_sec,
                    service_token=service_token,
                    serialization_mode=serialization_mode,
                )
                self.route_cache.mark_success(route)
                _record_observation(selected_route_id=route_id)
                return _route_success_response(resp)
            except GatewayCallError as exc:
                last_error = exc
                if not self._is_route_failure(exc):
                    self.route_cache.release_route(route)
                    _record_observation()
                    return exc.status_code, exc.data
                failed_count += 1
                last_failed_route_id = route_id
                self.route_cache.mark_failure(route, str(exc))
                with contextlib.suppress(Exception):
                    self.route_cache.refresh(service_name, force=True)
                continue
            except Exception as exc:
                last_error = exc
                self.route_cache.release_route(route)
                _record_observation()
                return 502, {"ok": False, "error": f"gateway call failed: {exc}"}

    def _handle_stream_call(
        self,
        *,
        service_name: str,
        method: str,
        payload: Dict[str, object],
        timeout_sec: float,
        service_token: str,
        serialization_mode: str = "",
    ) -> Tuple[Any, ...]:
        tried = set()
        attempt_count = 0
        failed_count = 0
        last_failed_route_id = ""
        last_error: Exception | None = None

        def _record_observation(*, selected_route_id: str = "") -> None:
            recorder = getattr(self.route_cache, "record_call_observation", None)
            if callable(recorder):
                recorder(
                    service_name,
                    route_attempt_count=attempt_count,
                    failed_route_count=failed_count,
                    last_failed_route_id=last_failed_route_id,
                    selected_route_id=selected_route_id,
                )

        while True:
            try:
                route = self.route_cache.select_route(service_name, exclude_service_ids=tried)
            except Exception as exc:
                _record_observation()
                message = str(last_error or exc)
                return 502, {"ok": False, "error": f"gateway stream failed: {message}"}
            route_id = str(route.service_id or "")
            if route_id in tried:
                self.route_cache.release_route(route)
                _record_observation()
                message = str(last_error or f"no untried route for service_name={service_name}")
                return 502, {"ok": False, "error": f"gateway stream failed: {message}"}
            tried.add(route_id)
            attempt_count += 1
            try:
                upstream = self._invoke_route_stream(
                    route,
                    method=method,
                    payload=payload,
                    timeout_sec=timeout_sec,
                    service_token=service_token,
                    serialization_mode=serialization_mode,
                )
            except GatewayCallError as exc:
                last_error = exc
                if not self._is_route_failure(exc):
                    self.route_cache.release_route(route)
                    _record_observation()
                    return exc.status_code, exc.data
                failed_count += 1
                last_failed_route_id = route_id
                self.route_cache.mark_failure(route, str(exc))
                with contextlib.suppress(Exception):
                    self.route_cache.refresh(service_name, force=True)
                continue
            except Exception as exc:
                last_error = exc
                self.route_cache.release_route(route)
                _record_observation()
                return 502, {"ok": False, "error": f"gateway stream failed: {exc}"}

            def _iter_stream():
                saw_done = False
                try:
                    for event in upstream:
                        if not isinstance(event, dict):
                            continue
                        event_name = str(event.get("event", "") or "")
                        if event_name == "item":
                            event = self._attach_stream_event_locator(event, route=route)
                            yield _encode_stream_line(event)
                            continue
                        if event_name == "done":
                            saw_done = True
                            if bool(event.get("ok", False)):
                                self.route_cache.mark_success(route)
                                _record_observation(selected_route_id=route_id)
                            else:
                                status_code = 400 if "user" in str(event.get("error_type", "") or "").lower() else 502
                                failure = GatewayCallError(status_code=status_code, data=dict(event))
                                if self._is_route_failure(failure):
                                    self.route_cache.mark_failure(route, str(failure))
                                else:
                                    self.route_cache.release_route(route)
                                _record_observation()
                            yield _encode_stream_line(event)
                            return
                    if not saw_done:
                        self.route_cache.mark_failure(route, "service stream ended without final response")
                        _record_observation()
                        yield _encode_stream_line(
                            {
                                "event": "done",
                                "ok": False,
                                "error_type": "StreamClosed",
                                "error": "service stream ended without final response",
                            }
                        )
                except Exception as exc:
                    self.route_cache.mark_failure(route, str(exc))
                    _record_observation()
                    yield _encode_stream_line(
                        {
                            "event": "done",
                            "ok": False,
                            "error_type": exc.__class__.__name__,
                            "error": f"gateway stream failed: {exc}",
                        }
                    )

            return (StreamingHttpResponse(status_code=200, body_iter=_iter_stream()),)

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
                    "policy_id": str(getattr(route, "policy_id", "") or "default_safe"),
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
                    "capability": route.capability.to_dict(),
                }
            )
        return 200, {
            "ok": True,
            "service_name": service_name,
            "refreshed_at": info["refreshed_at"],
            "route_count": info["route_count"],
            "last_call": dict(info.get("last_call") or {}),
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
        serialization_mode: str = "",
    ) -> Dict[str, object]:
        base_url = self._validate_route_url(route.http_base_url)
        if not base_url:
            raise GatewayCallError(status_code=502, data={"ok": False, "error": "invalid route http_base_url"})
        url = f"{base_url}/call/{quote(method, safe='')}?timeout_sec={max(0.1, timeout_sec):.3f}"
        headers = {"Content-Type": "application/json"}
        if service_token:
            headers["X-Service-Token"] = service_token
        if str(serialization_mode or "").strip().lower() == "pickle_stable_v1":
            request_body, transport_headers, _codec = _encode_http_transport_body(
                payload,
                context="service_internal",
                mode=serialization_mode,
            )
            headers.update(transport_headers)
        else:
            request_body = _encode_http_json_body(
                _serialize_http_call_payload(
                    payload,
                    context="service call payload",
                    mode=serialization_mode,
                )
            )
        req = Request(
            url=url,
            method="POST",
            headers=headers,
            data=request_body,
        )
        try:
            with urlopen(req, timeout=max(2.0, timeout_sec + 1.0)) as resp:
                raw = resp.read(MAX_BODY_BYTES + 1)
                if len(raw) > MAX_BODY_BYTES:
                    raise GatewayCallError(status_code=502, data={"ok": False, "error": "response too large"})
                data = _decode_http_response_with_headers(raw, headers=resp.headers, control_addr=route.control_addr)
        except HTTPError as exc:
            try:
                raw = exc.read() or b"{}"
                if len(raw) > MAX_BODY_BYTES:
                    data = {"ok": False, "error": "response too large"}
                else:
                    data = _decode_http_response_with_headers(raw, headers=getattr(exc, "headers", {}) or {})
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

    def _invoke_route_stream(
        self,
        route: InfoCenterServiceRoute,
        *,
        method: str,
        payload: Dict[str, object],
        timeout_sec: float,
        service_token: str,
        serialization_mode: str = "",
    ):
        base_url = self._validate_route_url(route.http_base_url)
        if not base_url:
            raise GatewayCallError(status_code=502, data={"ok": False, "error": "invalid route http_base_url"})
        try:
            return _iter_route_http_stream(
                route,
                method=method,
                payload=payload,
                timeout_sec=timeout_sec,
                service_token=service_token,
                serialization_mode=serialization_mode,
            )
        except Exception as exc:
            data = getattr(exc, "data", None)
            if not isinstance(data, dict):
                data = {"ok": False, "error": str(exc)}
            raise GatewayCallError(
                status_code=int(getattr(exc, "status_code", 502) or 502),
                data=data,
            ) from exc

    def _attach_stream_event_locator(
        self,
        event: Dict[str, object],
        *,
        route: InfoCenterServiceRoute,
    ) -> Dict[str, object]:
        if str(event.get("event", "") or "") != "item":
            return event
        response = self._attach_controlplane_locator({"data": event.get("data")}, route=route)
        if response.get("data") is event.get("data"):
            return event
        updated = dict(event)
        updated["data"] = response.get("data")
        return updated

    def _invoke_uploaded_route(
        self,
        route: InfoCenterServiceRoute,
        *,
        method: str,
        parsed_upload,
        timeout_sec: float,
        service_token: str,
    ) -> Dict[str, object]:
        request = parsed_upload.request
        self.stage_manager.mark_status(request, status="uploading")
        self.stage_manager.record_route(
            request,
            route={
                "service_id": str(route.service_id or ""),
                "node_id": str(route.node_id or ""),
                "node_instance_id": str(route.node_instance_id or ""),
                "control_addr": str(route.control_addr or ""),
                "http_base_url": str(route.http_base_url or ""),
            },
        )
        used_slots = tuple(parsed_upload.used_slots or ())
        upload_files = {
            slot: stage_file
            for slot, stage_file in parsed_upload.files.items()
            if not used_slots or slot in set(used_slots)
        }
        refs_by_slot: Dict[str, object] = {}
        try:
            refs_by_slot = upload_staged_files_to_route(
                request=request,
                route=route,
                files=upload_files,
                timeout_sec=timeout_sec,
            )
            self.stage_manager.record_resolved_refs(
                request,
                refs_by_slot={
                    slot: serialize_arrow_compatible(ref)
                    for slot, ref in refs_by_slot.items()
                },
            )
            rewritten_payload, _used_slots = rewrite_payload_with_uploaded_refs(
                payload=parsed_upload.payload,
                refs_by_slot=refs_by_slot,
                file_map=parsed_upload.file_map,
            )
            self.stage_manager.mark_status(request, status="calling")
            invoke_kwargs = {
                "method": method,
                "payload": rewritten_payload,
                "timeout_sec": timeout_sec,
                "service_token": service_token,
            }
            normalized_mode = str(parsed_upload.serialization_mode or "").strip().lower()
            if normalized_mode and normalized_mode != "legacy_v1":
                invoke_kwargs["serialization_mode"] = normalized_mode
            return self._invoke_route(
                route,
                **invoke_kwargs,
            )
        finally:
            release_uploaded_refs_on_route(
                route=route,
                refs_by_slot=refs_by_slot,
                timeout_sec=timeout_sec,
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
        msg = str(exc.data.get("error", "") or "").lower()
        error_type = str(exc.data.get("error_type", "") or "").lower()
        if any(text in f"{error_type} {msg}" for text in ("usererror", "failed_user", "user error")):
            return False
        if exc.status_code in (502, 503, 504):
            return True
        if exc.status_code == 200:
            return False
        if exc.status_code not in (404, 409, 500):
            return False
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
        route_control_addr = str(route.control_addr or "").strip()
        route_http_base_url = str(route.http_base_url or "").strip()
        if not route_control_addr and route_http_base_url:
            updated = with_data_ref_locator(
                data.get("data"),
                locator_kind="service_http",
                locator_token=route_http_base_url,
                node_id=str(route.node_id or ""),
                node_instance_id=str(route.node_instance_id or ""),
            )
            if updated is data.get("data"):
                return data
            body = dict(data)
            body["data"] = updated
            return body
        updated = with_data_ref_locator(
            data.get("data"),
            locator_kind="node_control" if route_control_addr else "controlplane",
            locator_token=route_control_addr or self.controlplane_target,
            node_id=str(route.node_id or ""),
            node_instance_id=str(route.node_instance_id or ""),
            control_addr=route_control_addr,
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
                    control_addr=route_control_addr,
                    locator_kind="node_control",
                    locator_token=route_control_addr,
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
                handled = app.handle_post_stream(
                    path=self.path,
                    headers=self.headers,
                    stream=self.rfile,
                    content_length=length,
                )
                if handled is not None:
                    if len(handled) == 1 and isinstance(handled[0], StreamingHttpResponse):
                        self._send_stream(handled[0])
                    elif len(handled) == 4:
                        code, resp, content_type, extra_headers = handled
                        self._send_body(code, resp, content_type=content_type, extra_headers=extra_headers)
                    else:
                        code, resp = handled
                        self._send_json(code, resp)
                    return
                if length > MAX_BODY_BYTES:
                    self._send_json(413, {"ok": False, "error": "payload too large"})
                    return
                body = self.rfile.read(max(0, length))
                handled = app.handle_post(path=self.path, headers=self.headers, body=body)
                if handled is None:
                    self._send_json(404, {"ok": False, "error": "not found"})
                    return
                if len(handled) == 4:
                    code, resp, content_type, extra_headers = handled
                    self._send_body(code, resp, content_type=content_type, extra_headers=extra_headers)
                elif len(handled) == 1 and isinstance(handled[0], StreamingHttpResponse):
                    self._send_stream(handled[0])
                else:
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

            def _send_json(self, status_code: int, data: Dict[str, object]) -> None:
                raw = _encode_http_json_body(data)
                self._send_body(status_code, raw, content_type="application/json; charset=utf-8")

            def _send_body(
                self,
                status_code: int,
                body: object,
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
