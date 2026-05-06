from __future__ import annotations

"""Thin runtime HTTP transport helpers shared by service/taskpool clients.

These helpers intentionally stop at transport concerns: request framing,
sidecar packing, timeout handling, and error normalization. Service call and
task-pool submit/results keep their own business protocols on top.
"""

import json
import socket
from dataclasses import dataclass, field
from http.client import RemoteDisconnected
from typing import Dict, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from pycloud_parallel.controlplane.client_transport import _normalize_http_response_body
from pycloud_parallel.controlplane.serialization import serialize_arrow_compatible


_BINARY_META_LEN_BYTES = 8


@dataclass(frozen=True)
class RuntimeTransportRequest:
    path: str
    mode: str
    payload: Optional[Dict[str, object]] = None
    chunks: Sequence[bytes] = field(default_factory=tuple)
    timeout_sec: float = 10.0
    method: str = "POST"
    headers: Dict[str, str] = field(default_factory=dict)


def _json_bytes(data: Dict[str, object]) -> bytes:
    return json.dumps(serialize_arrow_compatible(data), ensure_ascii=False).encode("utf-8")


def pack_binary_sidecar(meta: Dict[str, object], chunks: Sequence[bytes] = ()) -> bytes:
    meta_raw = _json_bytes(meta)
    return len(meta_raw).to_bytes(_BINARY_META_LEN_BYTES, "big") + meta_raw + b"".join(bytes(item or b"") for item in chunks)


def unpack_binary_sidecar(body: bytes) -> Tuple[Dict[str, object], bytes]:
    raw = bytes(body or b"")
    if len(raw) < _BINARY_META_LEN_BYTES:
        raise ValueError("binary sidecar body is too short")
    meta_len = int.from_bytes(raw[:_BINARY_META_LEN_BYTES], "big")
    if meta_len < 0 or len(raw) < _BINARY_META_LEN_BYTES + meta_len:
        raise ValueError("invalid binary sidecar meta length")
    meta = json.loads(raw[_BINARY_META_LEN_BYTES : _BINARY_META_LEN_BYTES + meta_len].decode("utf-8") or "{}")
    if not isinstance(meta, dict):
        raise ValueError("binary sidecar meta must be object")
    return meta, raw[_BINARY_META_LEN_BYTES + meta_len :]


def _friendly_runtime_http_error(*, url: str, exc: BaseException) -> RuntimeError:
    parsed = urlparse(url)
    endpoint = parsed.netloc or parsed.path or url
    reason = getattr(exc, "reason", exc)
    if isinstance(reason, ConnectionRefusedError):
        return RuntimeError(
            f"cannot connect to {endpoint}; check that the target is correct and the service is listening on that address"
        )
    if isinstance(reason, socket.timeout):
        return RuntimeError(
            f"request to {endpoint} timed out; check that the target is reachable and the service is responding"
        )
    if isinstance(exc, RemoteDisconnected):
        return RuntimeError(
            f"connection to {endpoint} was closed by the remote service; check that the target points to a healthy compatible server"
        )
    if isinstance(exc, URLError):
        return RuntimeError(f"http request to {endpoint} failed: {reason}")
    return RuntimeError(f"http request to {endpoint} failed: {exc}")


def runtime_http_request(
    *,
    base_url: str,
    request: RuntimeTransportRequest,
    control_addr: str = "",
) -> Dict[str, object]:
    headers = dict(request.headers or {})
    if request.mode == "json":
        raw = None if request.payload is None else _json_bytes(request.payload)
        if request.payload is not None:
            headers.setdefault("Content-Type", "application/json; charset=utf-8")
    elif request.mode == "binary_sidecar":
        raw = pack_binary_sidecar(request.payload or {}, request.chunks)
        headers.setdefault("Content-Type", "application/octet-stream")
        headers.setdefault("Accept", "application/json")
    else:
        raise ValueError("request.mode must be json or binary_sidecar")

    url = f"{base_url.rstrip('/')}{request.path}"
    req = Request(
        url,
        method=request.method.upper(),
        data=raw,
        headers=headers,
    )
    try:
        with urlopen(req, timeout=max(0.1, float(request.timeout_sec))) as resp:
            parsed = json.loads((resp.read() or b"{}").decode("utf-8") or "{}")
    except HTTPError as exc:
        try:
            parsed = json.loads((exc.read() or b"{}").decode("utf-8") or "{}")
        except Exception:
            parsed = {"ok": False, "error": exc.reason}
        raise RuntimeError(str(parsed.get("error", exc.reason))) from exc
    except URLError as exc:
        raise _friendly_runtime_http_error(url=url, exc=exc) from exc
    except RemoteDisconnected as exc:
        raise _friendly_runtime_http_error(url=url, exc=exc) from exc
    normalized = _normalize_http_response_body(parsed, control_addr=control_addr)
    if normalized.get("ok", False) is False:
        raise RuntimeError(str(normalized.get("error", "request failed")))
    return normalized


def runtime_http_request_for_binary_sidecar_response(
    *,
    base_url: str,
    request: RuntimeTransportRequest,
    control_addr: str = "",
) -> Tuple[Dict[str, object], bytes]:
    if request.mode != "json":
        raise ValueError("binary sidecar response requests must use json request mode")
    raw = None if request.payload is None else _json_bytes(request.payload)
    headers = dict(request.headers or {})
    if request.payload is not None:
        headers.setdefault("Content-Type", "application/json; charset=utf-8")
    headers.setdefault("Accept", "application/octet-stream")
    url = f"{base_url.rstrip('/')}{request.path}"
    req = Request(
        url,
        method=request.method.upper(),
        data=raw,
        headers=headers,
    )
    try:
        with urlopen(req, timeout=max(0.1, float(request.timeout_sec))) as resp:
            meta, body = unpack_binary_sidecar(resp.read() or b"")
    except HTTPError as exc:
        try:
            parsed = json.loads((exc.read() or b"{}").decode("utf-8") or "{}")
        except Exception:
            parsed = {"ok": False, "error": exc.reason}
        raise RuntimeError(str(parsed.get("error", exc.reason))) from exc
    except URLError as exc:
        raise _friendly_runtime_http_error(url=url, exc=exc) from exc
    except RemoteDisconnected as exc:
        raise _friendly_runtime_http_error(url=url, exc=exc) from exc
    normalized_meta = _normalize_http_response_body(meta, control_addr=control_addr)
    if normalized_meta.get("ok", False) is False:
        raise RuntimeError(str(normalized_meta.get("error", "request failed")))
    return normalized_meta, body


__all__ = [
    "RuntimeTransportRequest",
    "pack_binary_sidecar",
    "runtime_http_request",
    "runtime_http_request_for_binary_sidecar_response",
    "unpack_binary_sidecar",
]
