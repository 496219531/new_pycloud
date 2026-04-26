from __future__ import annotations

"""Communication-layer helpers extracted from controlplane client."""

from dataclasses import dataclass
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from pycloud_parallel.controlplane.config import PayloadPolicy, get_payload_policy
from pycloud_parallel.controlplane.data_ref import (
    DataRef,
    maybe_data_ref,
    resolve_data_ref_materialize_as,
    with_data_ref_control_addr,
)
from pycloud_parallel.controlplane.payload_transport import (
    decode_payload_from_transport,
    decode_result_from_transport,
    encode_payload_for_transport,
)
from pycloud_parallel.controlplane.serialization import (
    decode_transport_payload_bytes,
    encode_transport_payload_bytes,
    detect_transport_mode,
    deserialize_dataframe_bundle,
    deserialize_series_bundle,
    deserialize_by_mode,
    log_payload_flow,
    serialize_arrow_compatible,
)
from pycloud_parallel.controlplane.serialization_mode import resolve_received_transport_mode

if TYPE_CHECKING:
    from pycloud_parallel.controlplane.effective_policy import EffectivePolicy


HTTP_TRANSPORT_CONTENT_TYPE = "application/x-pycloud-transport"
HTTP_CODEC_HEADER = "X-Pycloud-Codec"
HTTP_TRANSPORT_VERSION_HEADER = "X-Pycloud-Transport-Version"


def _normalize_content_type(value: str) -> str:
    return str(value or "").split(";", 1)[0].strip().lower()


def _header_get(headers: Any, name: str) -> str:
    if headers is None:
        return ""
    direct = getattr(headers, "get", None)
    if callable(direct):
        value = direct(name, None)
        if value is not None:
            return str(value)
        value = direct(name.lower(), None)
        if value is not None:
            return str(value)
        value = direct(name.title(), None)
        if value is not None:
            return str(value)
    items = getattr(headers, "items", None)
    if callable(items):
        lowered = str(name or "").strip().lower()
        for key, value in items():
            if str(key or "").strip().lower() == lowered:
                return str(value)
    return ""


def _prefers_http_bytes_transport(mode: str = "", effective_policy: Optional["EffectivePolicy"] = None) -> bool:
    return _should_use_http_bytes_transport(mode=mode, effective_policy=effective_policy)


def _should_use_http_bytes_transport(
    *,
    mode: str = "",
    effective_policy: Optional["EffectivePolicy"] = None,
) -> bool:
    from pycloud_parallel.controlplane.effective_policy import should_use_http_bytes_transport

    return should_use_http_bytes_transport(
        mode=mode,
        effective_policy=effective_policy,
    )

def _is_http_transport_content_type(value: str) -> bool:
    return _normalize_content_type(value) == HTTP_TRANSPORT_CONTENT_TYPE


def _resolve_http_call_payload_policy(
    *,
    payload_policy: Optional[PayloadPolicy] = None,
    effective_policy: Optional["EffectivePolicy"] = None,
) -> PayloadPolicy:
    if payload_policy is not None:
        return payload_policy
    from pycloud_parallel.controlplane.effective_policy import payload_policy_from_effective_policy

    return payload_policy_from_effective_policy("http_call", effective_policy)


def _serialize_http_call_payload(
    payload: Optional[Dict[str, object]],
    *,
    context: str,
    mode: str = "",
    payload_policy: Optional[PayloadPolicy] = None,
    effective_policy: Optional["EffectivePolicy"] = None,
) -> Dict[str, object]:
    return encode_payload_for_transport(
        payload,
        policy=_resolve_http_call_payload_policy(
            payload_policy=payload_policy,
            effective_policy=effective_policy,
        ),
        context=context,
        mode=mode,
    )


def _encode_http_transport_body(
    payload: Optional[Dict[str, object]],
    *,
    context: str,
    mode: str = "",
    payload_policy: Optional[PayloadPolicy] = None,
    effective_policy: Optional["EffectivePolicy"] = None,
) -> tuple[bytes, Dict[str, str], str]:
    policy = _resolve_http_call_payload_policy(
        payload_policy=payload_policy,
        effective_policy=effective_policy,
    )
    transport = encode_transport_payload_bytes(
        payload or {},
        mode=mode,
        context=context,
        limit_bytes=policy.inline_payload_hard_limit_bytes,
    )
    headers = {
        "Content-Type": HTTP_TRANSPORT_CONTENT_TYPE,
        HTTP_CODEC_HEADER: str(transport.codec or ""),
        HTTP_TRANSPORT_VERSION_HEADER: str(int(transport.version or 0)),
    }
    return (transport.payload or b""), headers, str(transport.codec or "")


def _decode_http_request_body_with_mode(body: bytes, *, context: str) -> tuple[Dict[str, object], str]:
    try:
        payload = json.loads(body.decode("utf-8") if body else "{}")
    except Exception as exc:
        raise ValueError("invalid json body") from exc
    if not isinstance(payload, dict):
        raise ValueError("json body must be object")
    transport_mode = detect_transport_mode(payload)
    decoded = decode_payload_from_transport(
        payload,
        policy=get_payload_policy("http_call"),
        mode=transport_mode,
        context=context,
    )
    if not isinstance(decoded, dict):
        raise ValueError("json body must decode to object")
    return decoded, transport_mode


def _decode_http_transport_request_body_with_mode(
    body: bytes,
    *,
    headers,
    context: str,
) -> tuple[Dict[str, object], str]:
    codec = _header_get(headers, HTTP_CODEC_HEADER).strip().lower()
    if not codec:
        raise ValueError("transport request is missing X-Pycloud-Codec")
    raw_version = _header_get(headers, HTTP_TRANSPORT_VERSION_HEADER).strip()
    try:
        version = int(raw_version or 0)
    except ValueError as exc:
        raise ValueError("transport request has invalid X-Pycloud-Transport-Version") from exc
    effective_mode = resolve_received_transport_mode(
        declared_mode=codec,
        default_mode="legacy_v1",
        context=context,
    )
    decoded = decode_transport_payload_bytes(
        codec,
        version,
        body,
        context=context,
    )
    if not isinstance(decoded, dict):
        raise ValueError("transport body must decode to object")
    return decoded, effective_mode


def _decode_http_request_body(body: bytes, *, context: str) -> Dict[str, object]:
    decoded, _mode = _decode_http_request_body_with_mode(body, context=context)
    return decoded


def _encode_http_json_body(data: Dict[str, object]) -> bytes:
    return json.dumps(serialize_arrow_compatible(data), ensure_ascii=False).encode("utf-8")


def _decode_http_response_body(body: bytes, *, control_addr: str = "") -> Dict[str, object]:
    try:
        parsed = json.loads(body.decode("utf-8") if body else "{}")
    except Exception as exc:
        raise RuntimeError("invalid json response") from exc
    return _normalize_http_response_body(parsed, control_addr=control_addr)


def _encode_http_transport_response_body(
    value: Any,
    *,
    context: str,
    mode: str,
) -> tuple[bytes, Dict[str, str]]:
    transport = encode_transport_payload_bytes(
        value,
        mode=mode,
        context=context,
    )
    headers = {
        "Content-Type": HTTP_TRANSPORT_CONTENT_TYPE,
        HTTP_CODEC_HEADER: str(transport.codec or ""),
        HTTP_TRANSPORT_VERSION_HEADER: str(int(transport.version or 0)),
    }
    return (transport.payload or b""), headers


def _decode_http_response_with_headers(body: bytes, *, headers, control_addr: str = "") -> Dict[str, object]:
    content_type = _normalize_content_type(_header_get(headers, "Content-Type"))
    if _is_http_transport_content_type(content_type):
        codec = _header_get(headers, HTTP_CODEC_HEADER).strip().lower()
        if not codec:
            raise RuntimeError("transport response is missing X-Pycloud-Codec")
        raw_version = _header_get(headers, HTTP_TRANSPORT_VERSION_HEADER).strip()
        try:
            version = int(raw_version or 0)
        except ValueError as exc:
            raise RuntimeError("transport response has invalid X-Pycloud-Transport-Version") from exc
        decoded = decode_transport_payload_bytes(
            codec,
            version,
            body,
            context="service_result",
        )
        decoded = _inject_result_ref_control_addr(decoded, control_addr=control_addr)
        return {"ok": True, "data": decoded}
    return _decode_http_response_body(body, control_addr=control_addr)


def _is_bundle_format(fmt: str, *, expected: str) -> bool:
    return str(fmt or "").strip().lower() == expected


def _materialize_downloaded_result(path: Path, *, result_ref: object):
    data_ref = maybe_data_ref(result_ref)
    if data_ref is None:
        raise TypeError("result_ref must be a DataRef-compatible value")
    materialized = resolve_data_ref_materialize_as(data_ref, default="path")
    log_payload_flow(
        "result_materialize",
        materialize_as=materialized,
        format=data_ref.format,
        path=str(path),
    )
    if materialized == "path":
        return path
    normalized_format = str(data_ref.format or "").strip().lower()
    if normalized_format in {"structured_v1", "pickle_stable_v1"}:
        return deserialize_by_mode(path.read_bytes(), mode=normalized_format)
    if materialized == "bytes":
        return path.read_bytes()
    if materialized == "text":
        return path.read_text(encoding="utf-8")
    if materialized == "json":
        return json.loads(path.read_text(encoding="utf-8"))
    if materialized == "ndarray":
        import numpy as np

        return np.load(path, allow_pickle=False)
    if materialized == "dataframe":
        import pandas as pd

        if _is_bundle_format(data_ref.format, expected="dfbundle"):
            import zipfile

            with zipfile.ZipFile(path) as zf:
                if {"data.parquet", "meta.json"}.issubset(set(zf.namelist())):
                    with zf.open("data.parquet") as fh:
                        frame = pd.read_parquet(fh)
                    with zf.open("meta.json") as fh:
                        meta = json.load(fh)
                    return deserialize_dataframe_bundle(meta, frame)
        return pd.read_parquet(path)
    if materialized == "series":
        import pandas as pd

        if _is_bundle_format(data_ref.format, expected="seriesbundle"):
            import zipfile

            with zipfile.ZipFile(path) as zf:
                if {"data.parquet", "meta.json"}.issubset(set(zf.namelist())):
                    with zf.open("data.parquet") as fh:
                        frame = pd.read_parquet(fh)
                    with zf.open("meta.json") as fh:
                        meta = json.load(fh)
                    if len(frame.columns) != 1:
                        raise ValueError("series bundle parquet must contain exactly one column")
                    return deserialize_series_bundle(meta, frame.iloc[:, 0])
        frame = pd.read_parquet(path)
        if len(frame.columns) != 1:
            raise ValueError("series parquet must contain exactly one column")
        return frame.iloc[:, 0]
    raise ValueError(f"unsupported result materialize_as: {data_ref.materialize_as!r}")


def _inject_result_ref_control_addr(value: object, *, control_addr: str) -> object:
    return with_data_ref_control_addr(value, control_addr=control_addr)


def _normalize_http_response_body(body: object, *, control_addr: str = "") -> Dict[str, object]:
    if not isinstance(body, dict):
        raise RuntimeError("invalid json response")
    converted = dict(body)
    if "data" in converted:
        converted["data"] = decode_result_from_transport(
            converted.get("data"),
            policy=get_payload_policy("result"),
            context="service_result",
        )
        converted["data"] = _inject_result_ref_control_addr(converted.get("data"), control_addr=control_addr)
    return converted


@dataclass
class DiscoveryCallError(Exception):
    status_code: int
    data: Dict[str, object]

    def __str__(self) -> str:
        message = str(self.data.get("error", f"http {self.status_code}") or f"http {self.status_code}")
        error_type = str(self.data.get("error_type", "") or "").strip()
        if error_type and error_type not in message:
            message = f"{message} ({error_type})"
        traceback_text = str(self.data.get("traceback", "") or "").strip()
        if traceback_text:
            message = f"{message}\n{traceback_text}"
        return message


def _serialize_route(route: Any) -> Dict[str, object]:
    return {
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
        "capability": getattr(getattr(route, "capability", None), "to_dict", lambda: {})(),
    }


def _call_route_http(
    route: Any,
    *,
    method: str,
    payload: Dict[str, object],
    timeout_sec: float,
    service_token: str,
    serialization_mode: str = "",
    payload_policy: Optional[PayloadPolicy] = None,
    effective_policy: Optional["EffectivePolicy"] = None,
) -> Dict[str, object]:
    url = f"{route.http_base_url}/call/{quote(method, safe='')}?timeout_sec={max(0.1, timeout_sec):.3f}"
    headers: Dict[str, str] = {}
    if service_token:
        headers["X-Service-Token"] = service_token
    if _should_use_http_bytes_transport(
        mode=serialization_mode,
        effective_policy=effective_policy,
    ):
        request_body, transport_headers, _codec = _encode_http_transport_body(
            payload,
            context="service_internal",
            mode=serialization_mode,
            payload_policy=payload_policy,
            effective_policy=effective_policy,
        )
        headers.update(transport_headers)
    else:
        headers["Content-Type"] = "application/json"
        serialized_payload = _serialize_http_call_payload(
            payload,
            context="service call payload",
            mode=serialization_mode,
            payload_policy=payload_policy,
            effective_policy=effective_policy,
        )
        request_body = json.dumps(serialized_payload).encode("utf-8")
    req = Request(
        url=url,
        method="POST",
        headers=headers,
        data=request_body,
    )
    try:
        with urlopen(req, timeout=max(2.0, timeout_sec + 1.0)) as resp:
            raw = resp.read()
            data = _decode_http_response_with_headers(
                raw,
                headers=resp.headers,
                control_addr=route.control_addr,
            )
    except HTTPError as exc:
        try:
            raw = exc.read() or b"{}"
            data = _decode_http_response_with_headers(
                raw,
                headers=getattr(exc, "headers", {}) or {},
            )
        except Exception:
            data = {"ok": False, "error": exc.reason}
        raise DiscoveryCallError(status_code=exc.code, data=data) from exc
    except Exception as exc:
        raise DiscoveryCallError(status_code=502, data={"ok": False, "error": repr(exc)}) from exc
    if not data.get("ok", False):
        raise DiscoveryCallError(status_code=502, data=data)
    return data


def _list_route_methods_http(
    route: Any,
    *,
    include_docs: bool,
    timeout_sec: float,
) -> List[Dict[str, object]]:
    params = urlencode({"include_docs": "true" if include_docs else "false"})
    url = f"{route.http_base_url}/methods?{params}"
    req = Request(url, method="GET")
    try:
        with urlopen(req, timeout=max(2.0, timeout_sec + 1.0)) as resp:
            data = json.loads(resp.read().decode("utf-8") or "{}")
    except HTTPError as exc:
        try:
            data = json.loads((exc.read() or b"{}").decode("utf-8") or "{}")
        except Exception:
            data = {"ok": False, "error": exc.reason}
        raise DiscoveryCallError(status_code=exc.code, data=data) from exc
    except Exception as exc:
        raise DiscoveryCallError(status_code=502, data={"ok": False, "error": repr(exc)}) from exc
    if not isinstance(data, dict):
        raise DiscoveryCallError(status_code=502, data={"ok": False, "error": "invalid methods response"})
    if not data.get("ok", False):
        raise DiscoveryCallError(status_code=502, data=data)
    methods = data.get("methods", [])
    if not isinstance(methods, list):
        raise DiscoveryCallError(status_code=502, data={"ok": False, "error": "invalid methods payload"})
    return [item for item in methods if isinstance(item, dict)]


def _is_route_failure(exc: DiscoveryCallError) -> bool:
    msg = str(exc.data.get("error", "") or "").lower()
    error_type = str(exc.data.get("error_type", "") or "").lower()
    if any(text in f"{error_type} {msg}" for text in ("usererror", "failed_user", "user error")):
        return False
    if exc.status_code in (502, 503, 504):
        return True
    if exc.status_code not in (404, 409, 500):
        return False
    return any(text in msg for text in ("service not found", "service not running", "service executor stopped", "artifact missing"))
