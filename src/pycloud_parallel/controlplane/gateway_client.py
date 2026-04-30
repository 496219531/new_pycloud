from __future__ import annotations

"""Gateway HTTP low-level transport client."""

import contextlib
import http.client
import json
from pathlib import Path
from types import SimpleNamespace
import uuid
from typing import Dict, List, Optional, Sequence, Tuple, Union
from urllib.parse import quote, urlencode, urlparse

from pycloud_parallel.controlplane.effective_policy import EffectivePolicy, resolve_effective_policy
from pycloud_parallel.controlplane.policy_profile import (
    get_default_mode_for_binding,
    get_default_policy_id_for_binding,
    get_policy_profile,
)
from .client_transport import (
    _call_route_http,
    _decode_http_response_body,
    _iter_route_http_stream,
    _prefers_http_bytes_transport,
    _serialize_http_call_payload,
)
from pycloud_parallel.data.ref import maybe_data_ref, with_data_ref_locator
from pycloud_parallel.controlplane.data_registry import DataRegistryClient, resolve_data_ref
from pycloud_parallel.controlplane.http_client import http_json_request, target_to_base_url
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.controlplane.remote_payload import prepare_remote_call_payload
from pycloud_parallel.controlplane.replica_client import _extract_result_ref
from pycloud_parallel.controlplane.serialization import INLINE_PAYLOAD_SOFT_LIMIT_BYTES, serialize_arrow_compatible
from pycloud_parallel.execution.failover import STATUS_LOOKUP_FAILED, should_degrade
client_mod = SimpleNamespace(
    _target_to_base_url=target_to_base_url,
    NodeControlClient=NodeControlClient,
    _prepare_remote_call_payload=prepare_remote_call_payload,
    INLINE_PAYLOAD_SOFT_LIMIT_BYTES=INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    _call_route_http=_call_route_http,
    _iter_route_http_stream=_iter_route_http_stream,
    _prefers_http_bytes_transport=_prefers_http_bytes_transport,
    _serialize_http_call_payload=_serialize_http_call_payload,
    _http_json_request=http_json_request,
    _extract_result_ref=_extract_result_ref,
    serialize_arrow_compatible=serialize_arrow_compatible,
    _decode_http_response_body=_decode_http_response_body,
)


_JOBQUEUE_BINDING_ID = "jobqueue_controlplane_transport"


def _resolve_gateway_service_policy(
    service_name: str,
    *,
    serialization_mode: str = "",
    effective_policy: Optional[EffectivePolicy] = None,
) -> Tuple[str, Optional[EffectivePolicy]]:
    mode = str(serialization_mode or "").strip()
    if effective_policy is not None or mode:
        return mode, effective_policy
    if str(service_name or "").strip() != "job-orchestrator":
        return mode, effective_policy
    default_mode = get_default_mode_for_binding(_JOBQUEUE_BINDING_ID)
    policy = resolve_effective_policy(
        get_policy_profile(get_default_policy_id_for_binding(_JOBQUEUE_BINDING_ID)),
        requested_mode=default_mode,
        context="jobqueue_session",
    )
    return str(policy.resolved_mode or default_mode).strip() or default_mode, policy


class GatewayServiceClient:
    """Low-level HTTP transport client for Gateway-routed service calls."""

    def __init__(self, target: str, *, timeout_sec: float = 10.0, service_token: str = "") -> None:
        self.target = target
        self.base_url = client_mod._target_to_base_url(target)
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.service_token = str(service_token or "").strip()
        self._last_routes_by_service: Dict[str, List[Dict[str, object]]] = {}

    def close(self) -> None:
        return None

    def __enter__(self) -> "GatewayServiceClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def call(
        self,
        *,
        service_name: str,
        method: str,
        payload: Optional[Dict[str, object]] = None,
        timeout_sec: float = 60.0,
        service_token: Optional[str] = None,
        serialization_mode: str = "",
        effective_policy: Optional[EffectivePolicy] = None,
    ) -> Dict[str, object]:
        name = str(service_name or "").strip()
        method_name = str(method or "").strip()
        if not name:
            raise ValueError("service_name is required")
        if not method_name:
            raise ValueError("method is required")
        serialization_mode, effective_policy = _resolve_gateway_service_policy(
            name,
            serialization_mode=serialization_mode,
            effective_policy=effective_policy,
        )
        token = self.service_token if service_token is None else str(service_token or "").strip()
        headers: Dict[str, str] = {}
        if token:
            headers["X-Service-Token"] = token
        params = urlencode({"timeout_sec": f"{max(0.1, float(timeout_sec)):.3f}"})
        routes: List[Dict[str, object]] = []
        status_error: Optional[Exception] = None
        try:
            status = self.get_status(service_name=name)
            routes = list(status.get("routes", [])) if isinstance(status, dict) else []
            if routes:
                self._last_routes_by_service[name] = [dict(item) for item in routes if isinstance(item, dict)]
        except Exception as exc:
            status_error = exc
            routes = list(self._last_routes_by_service.get(name, []))
        if status_error is not None and not should_degrade(
            STATUS_LOOKUP_FAILED,
            has_cached_candidate=bool(routes),
            requires_route_aware_staging=False,
        ):
            raise RuntimeError(f"gateway status lookup failed for service_name={name!r}: {status_error}") from status_error
        clients: List[object] = []
        prepared_payload = payload or {}
        try:
            for item in routes:
                if not isinstance(item, dict):
                    continue
                control_addr = str(item.get("control_addr", "") or "").strip()
                if not control_addr:
                    continue
                clients.append(client_mod.NodeControlClient(control_addr, timeout_sec=self.timeout_sec))
            if clients:
                prepare_kwargs = {
                    "object_threshold_bytes": client_mod.INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
                }
                if str(serialization_mode or "").strip() and str(serialization_mode).strip().lower() != "legacy_v1":
                    prepare_kwargs["serialization_mode"] = serialization_mode
                if effective_policy is not None:
                    prepare_kwargs["effective_policy"] = effective_policy
                prepared_payload = client_mod._prepare_remote_call_payload(
                    clients,
                    payload,
                    **prepare_kwargs,
                )
            else:
                try:
                    prepared_payload = payload or {}
                    serialized_payload = client_mod._serialize_http_call_payload(
                        prepared_payload,
                        context="service call payload",
                        mode=serialization_mode,
                        effective_policy=effective_policy,
                    )
                except ValueError as exc:
                    if status_error is not None and not should_degrade(
                        STATUS_LOOKUP_FAILED,
                        has_cached_candidate=bool(routes),
                        requires_route_aware_staging=True,
                    ):
                        raise RuntimeError(
                            "Gateway call requires route-aware staging but gateway status lookup failed "
                            "and no cached routes are available"
                        ) from exc
                    raise
            if clients:
                serialized_payload = client_mod._serialize_http_call_payload(
                    prepared_payload,
                    context="service call payload",
                    mode=serialization_mode,
                    effective_policy=effective_policy,
                )
        finally:
            for client in clients:
                with contextlib.suppress(Exception):
                    client.close()
        if client_mod._prefers_http_bytes_transport(serialization_mode, effective_policy=effective_policy):
            response = client_mod._call_route_http(
                SimpleNamespace(
                    http_base_url=f"{self.base_url}/svc/{quote(name, safe='')}",
                    control_addr="",
                ),
                method=method_name,
                payload=prepared_payload,
                timeout_sec=max(0.1, float(timeout_sec)),
                service_token=token,
                serialization_mode=serialization_mode,
                **({"effective_policy": effective_policy} if effective_policy is not None else {}),
            )
        else:
            response = client_mod._http_json_request(
                base_url=self.base_url,
                path=f"/svc/{quote(name, safe='')}/call/{quote(method_name, safe='')}?{params}",
                method="POST",
                timeout_sec=max(self.timeout_sec, max(0.1, float(timeout_sec)) + 1.0),
                payload=serialized_payload,
                headers=headers,
            )
        return self._attach_controlplane_locator(response)

    def stream_call(
        self,
        *,
        service_name: str,
        method: str,
        payload: Optional[Dict[str, object]] = None,
        timeout_sec: float = 60.0,
        service_token: Optional[str] = None,
        serialization_mode: str = "",
        effective_policy: Optional[EffectivePolicy] = None,
    ):
        name = str(service_name or "").strip()
        method_name = str(method or "").strip()
        if not name:
            raise ValueError("service_name is required")
        if not method_name:
            raise ValueError("method is required")
        serialization_mode, effective_policy = _resolve_gateway_service_policy(
            name,
            serialization_mode=serialization_mode,
            effective_policy=effective_policy,
        )
        token = self.service_token if service_token is None else str(service_token or "").strip()
        routes: List[Dict[str, object]] = []
        status_error: Optional[Exception] = None
        try:
            status = self.get_status(service_name=name)
            routes = list(status.get("routes", [])) if isinstance(status, dict) else []
            if routes:
                self._last_routes_by_service[name] = [dict(item) for item in routes if isinstance(item, dict)]
        except Exception as exc:
            status_error = exc
            routes = list(self._last_routes_by_service.get(name, []))
        if status_error is not None and not should_degrade(
            STATUS_LOOKUP_FAILED,
            has_cached_candidate=bool(routes),
            requires_route_aware_staging=False,
        ):
            raise RuntimeError(f"gateway status lookup failed for service_name={name!r}: {status_error}") from status_error
        clients: List[object] = []
        prepared_payload = payload or {}
        try:
            for item in routes:
                if not isinstance(item, dict):
                    continue
                control_addr = str(item.get("control_addr", "") or "").strip()
                if not control_addr:
                    continue
                clients.append(client_mod.NodeControlClient(control_addr, timeout_sec=self.timeout_sec))
            if clients:
                prepare_kwargs = {
                    "object_threshold_bytes": client_mod.INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
                }
                if str(serialization_mode or "").strip() and str(serialization_mode).strip().lower() != "legacy_v1":
                    prepare_kwargs["serialization_mode"] = serialization_mode
                if effective_policy is not None:
                    prepare_kwargs["effective_policy"] = effective_policy
                prepared_payload = client_mod._prepare_remote_call_payload(
                    clients,
                    payload,
                    **prepare_kwargs,
                )
            else:
                try:
                    prepared_payload = payload or {}
                    client_mod._serialize_http_call_payload(
                        prepared_payload,
                        context="service call payload",
                        mode=serialization_mode,
                        effective_policy=effective_policy,
                    )
                except ValueError as exc:
                    if status_error is not None and not should_degrade(
                        STATUS_LOOKUP_FAILED,
                        has_cached_candidate=bool(routes),
                        requires_route_aware_staging=True,
                    ):
                        raise RuntimeError(
                            "Gateway stream requires route-aware staging but gateway status lookup failed "
                            "and no cached routes are available"
                        ) from exc
                    raise
        finally:
            for client in clients:
                with contextlib.suppress(Exception):
                    client.close()
        return client_mod._iter_route_http_stream(
            SimpleNamespace(
                http_base_url=f"{self.base_url}/svc/{quote(name, safe='')}",
                control_addr="",
            ),
            method=method_name,
            payload=prepared_payload,
            timeout_sec=max(0.1, float(timeout_sec)),
            service_token=token,
            serialization_mode=serialization_mode,
            **({"effective_policy": effective_policy} if effective_policy is not None else {}),
        )

    def upload_call(
        self,
        *,
        service_name: str,
        method: str,
        payload: Dict[str, object],
        files: Dict[str, Union[str, Path, bytes, bytearray, memoryview]],
        file_map: Optional[Dict[str, str]] = None,
        timeout_sec: float = 60.0,
        service_token: Optional[str] = None,
    ) -> Dict[str, object]:
        name = str(service_name or "").strip()
        method_name = str(method or "").strip()
        if not name:
            raise ValueError("service_name is required")
        if not method_name:
            raise ValueError("method is required")
        if not isinstance(payload, dict):
            raise ValueError("payload must be object")
        if not files:
            raise ValueError("files are required")
        token = self.service_token if service_token is None else str(service_token or "").strip()
        params = urlencode({"timeout_sec": f"{max(0.1, float(timeout_sec)):.3f}"})
        path = f"/svc/{quote(name, safe='')}/upload-call/{quote(method_name, safe='')}?{params}"
        response = _http_multipart_request(
            base_url=self.base_url,
            path=path,
            payload=client_mod._serialize_http_call_payload(payload, context="gateway upload-call payload"),
            files=files,
            file_map=file_map or {},
            timeout_sec=max(self.timeout_sec, max(0.1, float(timeout_sec)) + 1.0),
            headers={"X-Service-Token": token} if token else {},
        )
        return self._attach_controlplane_locator(response)

    def list_methods(self, *, service_name: str, include_docs: bool = False) -> Sequence[Dict[str, object]]:
        name = str(service_name or "").strip()
        if not name:
            raise ValueError("service_name is required")
        params = urlencode({"include_docs": "true" if include_docs else "false"})
        resp = client_mod._http_json_request(
            base_url=self.base_url,
            path=f"/svc/{quote(name, safe='')}/methods?{params}",
            method="GET",
            timeout_sec=self.timeout_sec,
        )
        methods = resp.get("methods", [])
        if not isinstance(methods, list):
            raise RuntimeError("invalid methods response")
        return [item for item in methods if isinstance(item, dict)]

    def get_status(self, *, service_name: str) -> Dict[str, object]:
        name = str(service_name or "").strip()
        if not name:
            raise ValueError("service_name is required")
        return client_mod._http_json_request(
            base_url=self.base_url,
            path=f"/svc/{quote(name, safe='')}/status",
            method="GET",
            timeout_sec=self.timeout_sec,
        )

    def download_result_to_file(self, response_or_data: object, *, target_path: str) -> Path:
        ref = client_mod._extract_result_ref(response_or_data)
        if ref is None:
            raise ValueError("service result is inline data; no download needed")
        self._touch_data_ref(ref)
        resolved = resolve_data_ref(ref, target=self.target, timeout_sec=self.timeout_sec)
        try:
            with client_mod.NodeControlClient(resolved.control_addr, timeout_sec=self.timeout_sec) as client:
                return client.download_result_to_file(ref, target_path=target_path)
        finally:
            self._release_data_ref_if_consumed(ref)

    def fetch_result_data(self, response_or_data: object, *, target_path: str = ""):
        ref = client_mod._extract_result_ref(response_or_data)
        if ref is None:
            if isinstance(response_or_data, dict) and "data" in response_or_data:
                return response_or_data["data"]
            return response_or_data
        self._touch_data_ref(ref)
        resolved = resolve_data_ref(ref, target=self.target, timeout_sec=self.timeout_sec)
        try:
            with client_mod.NodeControlClient(resolved.control_addr, timeout_sec=self.timeout_sec) as client:
                return client.fetch_result_ref_data(ref, target_path=target_path)
        finally:
            self._release_data_ref_if_consumed(ref)

    def _attach_controlplane_locator(self, response: Dict[str, object]) -> Dict[str, object]:
        if not isinstance(response, dict) or "data" not in response:
            return response
        updated = with_data_ref_locator(
            response.get("data"),
            locator_kind="controlplane",
            locator_token=self.target,
        )
        if updated is response.get("data"):
            return response
        body = dict(response)
        body["data"] = updated
        return body

    def _touch_data_ref(self, ref: object) -> None:
        data_ref = maybe_data_ref(ref)
        if data_ref is None or str(data_ref.locator_kind or "").strip().lower() != "controlplane":
            return
        target = str(data_ref.locator_token or self.target or "").strip()
        if not target:
            return
        try:
            DataRegistryClient(target, timeout_sec=self.timeout_sec).touch(data_ref.ref_id)
        except Exception:
            pass

    def _release_data_ref_if_consumed(self, ref: object) -> None:
        data_ref = maybe_data_ref(ref)
        if data_ref is None or not bool(data_ref.consume_on_read):
            return
        target = str(data_ref.locator_token or self.target or "").strip()
        if not target:
            return
        try:
            DataRegistryClient(target, timeout_sec=self.timeout_sec).release(data_ref.ref_id)
        except Exception:
            pass


__all__ = ["GatewayServiceClient"]


def _multipart_part_header(*, boundary: str, name: str, filename: str = "", content_type: str = "") -> bytes:
    lines = [f"--{boundary}\r\n"]
    disposition = f'Content-Disposition: form-data; name="{name}"'
    if filename:
        disposition += f'; filename="{filename}"'
    lines.append(disposition + "\r\n")
    if content_type:
        lines.append(f"Content-Type: {content_type}\r\n")
    lines.append("\r\n")
    return "".join(lines).encode("utf-8")


def _coerce_upload_file_entry(
    value: Union[str, Path, bytes, bytearray, memoryview],
) -> Tuple[str, Optional[Path], bytes]:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "upload.bin", None, bytes(value)
    path = Path(value).expanduser()
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"upload file not found: {value}")
    return path.name, path, b""


def _multipart_content_length(
    *,
    boundary: str,
    payload_bytes: bytes,
    file_map_bytes: bytes,
    files: Dict[str, Union[str, Path, bytes, bytearray, memoryview]],
) -> int:
    total = 0
    total += len(_multipart_part_header(boundary=boundary, name="payload", content_type="application/json"))
    total += len(payload_bytes) + 2
    if file_map_bytes:
        total += len(_multipart_part_header(boundary=boundary, name="file_map", content_type="application/json"))
        total += len(file_map_bytes) + 2
    for slot, value in files.items():
        filename, path, inline = _coerce_upload_file_entry(value)
        total += len(
            _multipart_part_header(
                boundary=boundary,
                name=f"files[{slot}]",
                filename=filename,
                content_type="application/octet-stream",
            )
        )
        total += (path.stat().st_size if path is not None else len(inline)) + 2
    total += len(f"--{boundary}--\r\n".encode("utf-8"))
    return total


def _http_multipart_request(
    *,
    base_url: str,
    path: str,
    payload: Dict[str, object],
    files: Dict[str, Union[str, Path, bytes, bytearray, memoryview]],
    file_map: Dict[str, str],
    timeout_sec: float,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, object]:
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"}:
        raise RuntimeError(f"unsupported gateway scheme: {parsed.scheme!r}")
    boundary = f"pycloud-{uuid.uuid4().hex}"
    payload_bytes = json.dumps(client_mod.serialize_arrow_compatible(payload), ensure_ascii=False).encode("utf-8")
    file_map_bytes = (
        json.dumps({str(path): str(slot) for path, slot in dict(file_map or {}).items()}, ensure_ascii=False).encode("utf-8")
        if file_map
        else b""
    )
    request_headers = dict(headers or {})
    request_headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    request_headers["Content-Length"] = str(
        _multipart_content_length(
            boundary=boundary,
            payload_bytes=payload_bytes,
            file_map_bytes=file_map_bytes,
            files=files,
        )
    )
    connection_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    connection = connection_cls(parsed.hostname, parsed.port, timeout=max(0.1, float(timeout_sec)))
    try:
        connection.putrequest("POST", path)
        for name, value in request_headers.items():
            if str(value or "").strip():
                connection.putheader(str(name), str(value))
        connection.endheaders()
        connection.send(_multipart_part_header(boundary=boundary, name="payload", content_type="application/json"))
        connection.send(payload_bytes)
        connection.send(b"\r\n")
        if file_map_bytes:
            connection.send(_multipart_part_header(boundary=boundary, name="file_map", content_type="application/json"))
            connection.send(file_map_bytes)
            connection.send(b"\r\n")
        for slot, value in files.items():
            filename, path_obj, inline = _coerce_upload_file_entry(value)
            connection.send(
                _multipart_part_header(
                    boundary=boundary,
                    name=f"files[{slot}]",
                    filename=filename,
                    content_type="application/octet-stream",
                )
            )
            if path_obj is not None:
                with path_obj.open("rb") as fh:
                    while True:
                        chunk = fh.read(256 * 1024)
                        if not chunk:
                            break
                        connection.send(chunk)
            else:
                connection.send(inline)
            connection.send(b"\r\n")
        connection.send(f"--{boundary}--\r\n".encode("utf-8"))
        response = connection.getresponse()
        raw = response.read()
        if 200 <= int(response.status) < 300:
            return client_mod._decode_http_response_body(raw)
        data = client_mod._decode_http_response_body(raw) if raw else {"ok": False, "error": response.reason}
        raise RuntimeError(str(data.get("error", response.reason)))
    finally:
        connection.close()
