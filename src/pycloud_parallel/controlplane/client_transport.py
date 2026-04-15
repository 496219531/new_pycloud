from __future__ import annotations

"""Communication-layer helpers extracted from controlplane client."""

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from pycloud_parallel.controlplane.config import get_payload_policy
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
    deserialize_dataframe_bundle,
    deserialize_series_bundle,
    log_payload_flow,
    serialize_arrow_compatible,
)


def _serialize_http_call_payload(payload: Optional[Dict[str, object]], *, context: str) -> Dict[str, object]:
    return encode_payload_for_transport(
        payload,
        policy=get_payload_policy("http_call"),
        context=context,
    )


def _decode_http_request_body(body: bytes, *, context: str) -> Dict[str, object]:
    try:
        payload = json.loads(body.decode("utf-8") if body else "{}")
    except Exception as exc:
        raise ValueError("invalid json body") from exc
    if not isinstance(payload, dict):
        raise ValueError("json body must be object")
    serialized = _serialize_http_call_payload(payload, context=context)
    decoded = decode_payload_from_transport(
        serialized,
        policy=get_payload_policy("http_call"),
    )
    if not isinstance(decoded, dict):
        raise ValueError("json body must decode to object")
    return decoded


def _encode_http_json_body(data: Dict[str, object]) -> bytes:
    return json.dumps(serialize_arrow_compatible(data), ensure_ascii=False).encode("utf-8")


def _decode_http_response_body(body: bytes, *, control_addr: str = "") -> Dict[str, object]:
    try:
        parsed = json.loads(body.decode("utf-8") if body else "{}")
    except Exception as exc:
        raise RuntimeError("invalid json response") from exc
    return _normalize_http_response_body(parsed, control_addr=control_addr)


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
    converted = decode_result_from_transport(
        body,
        policy=get_payload_policy("result"),
    )
    if not isinstance(converted, dict):
        raise RuntimeError("invalid json response")
    if "data" in converted:
        converted = dict(converted)
        converted["data"] = _inject_result_ref_control_addr(converted.get("data"), control_addr=control_addr)
    return converted


@dataclass
class DiscoveryCallError(Exception):
    status_code: int
    data: Dict[str, object]

    def __str__(self) -> str:
        return str(self.data.get("error", f"http {self.status_code}"))


def _serialize_route(route: Any) -> Dict[str, object]:
    return {
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


def _call_route_http(
    route: Any,
    *,
    method: str,
    payload: Dict[str, object],
    timeout_sec: float,
    service_token: str,
) -> Dict[str, object]:
    url = f"{route.http_base_url}/call/{quote(method, safe='')}?timeout_sec={max(0.1, timeout_sec):.3f}"
    headers = {"Content-Type": "application/json"}
    if service_token:
        headers["X-Service-Token"] = service_token
    serialized_payload = _serialize_http_call_payload(payload, context="service call payload")
    req = Request(
        url=url,
        method="POST",
        headers=headers,
        data=json.dumps(serialized_payload).encode("utf-8"),
    )
    try:
        with urlopen(req, timeout=max(2.0, timeout_sec + 1.0)) as resp:
            data = _normalize_http_response_body(
                json.loads(resp.read().decode("utf-8") or "{}"),
                control_addr=route.control_addr,
            )
    except HTTPError as exc:
        try:
            data = _normalize_http_response_body(json.loads((exc.read() or b"{}").decode("utf-8") or "{}"))
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
    if exc.status_code == 502:
        return True
    if exc.status_code not in (404, 409, 500):
        return False
    msg = str(exc.data.get("error", "") or "").lower()
    return any(text in msg for text in ("service not found", "service not running", "service executor stopped", "artifact missing"))
