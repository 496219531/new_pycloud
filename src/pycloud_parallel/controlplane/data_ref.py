from __future__ import annotations

"""Unified public data reference helpers."""

from dataclasses import dataclass, replace
from typing import Any, Dict, Optional

from pycloud_parallel.controlplane.object_ref import normalize_object_format, normalize_object_id

DATA_REF_SENTINEL = "__pycloud_data_ref__"
_MATERIALIZE_PREFS = {"auto", "path", "dataframe", "series", "ndarray", "json", "bytes", "text"}


def normalize_data_ref_id(ref_id: str) -> str:
    text = str(ref_id or "").strip()
    if not text:
        raise ValueError("ref_id must not be empty")
    return text


def normalize_data_ref_materialize_as(value: str = "", *, default: str = "auto") -> str:
    text = str(value or "").strip().lower()
    if not text:
        text = str(default or "auto").strip().lower() or "auto"
    if text not in _MATERIALIZE_PREFS:
        raise ValueError(f"unsupported data ref materialize_as: {value!r}")
    return text


def _normalize_storage_id(storage_id: str, *, fallback: str) -> str:
    text = str(storage_id or fallback or "").strip()
    if not text:
        raise ValueError("storage_id must not be empty")
    if text.startswith("sha256:"):
        return normalize_object_id(text)
    return text


def _infer_logical_type(*, logical_type: str, fmt: str, materialize_as: str) -> str:
    normalized = str(logical_type or "").strip().lower()
    if normalized:
        return normalized
    normalized_format = normalize_object_format(fmt, default="bin")
    normalized_materialize = normalize_data_ref_materialize_as(materialize_as, default="auto")
    if normalized_materialize != "auto":
        if normalized_materialize == "path":
            if normalized_format in {"zip", "tar.gz", "whl"}:
                return "archive"
            return "file"
        if normalized_materialize == "text":
            return "text"
        return normalized_materialize
    if normalized_format == "dfbundle":
        return "dataframe"
    if normalized_format == "seriesbundle":
        return "series"
    if normalized_format == "npy":
        return "ndarray"
    if normalized_format == "json":
        return "json"
    if normalized_format in {"txt", "text", "log", "md", "sql"}:
        return "text"
    if normalized_format in {"zip", "tar.gz", "whl"}:
        return "archive"
    return "bytes"


@dataclass(frozen=True)
class DataRef:
    ref_id: str
    storage_id: str = ""
    logical_type: str = "bytes"
    format: str = "bin"
    size_bytes: int = 0
    materialize_as: str = "auto"
    locator_kind: str = "controlplane"
    locator_token: str = ""
    consume_on_read: bool = False
    node_id: str = ""
    node_instance_id: str = ""
    control_addr: str = ""

    def __post_init__(self) -> None:
        normalized_ref_id = normalize_data_ref_id(self.ref_id or self.storage_id)
        normalized_storage_id = _normalize_storage_id(self.storage_id, fallback=normalized_ref_id)
        normalized_format = normalize_object_format(self.format, default="bin")
        normalized_materialize = normalize_data_ref_materialize_as(self.materialize_as, default="auto")
        normalized_locator_kind = str(self.locator_kind or "controlplane").strip().lower() or "controlplane"
        normalized_control_addr = str(self.control_addr or "").strip()
        normalized_locator_token = str(self.locator_token or normalized_control_addr).strip()
        if normalized_control_addr and normalized_locator_kind == "controlplane":
            normalized_locator_kind = "node_control"
        logical_type = _infer_logical_type(
            logical_type=self.logical_type,
            fmt=normalized_format,
            materialize_as=normalized_materialize,
        )
        object.__setattr__(self, "ref_id", normalized_ref_id)
        object.__setattr__(self, "storage_id", normalized_storage_id)
        object.__setattr__(self, "logical_type", logical_type)
        object.__setattr__(self, "format", normalized_format)
        object.__setattr__(self, "size_bytes", max(0, int(self.size_bytes or 0)))
        object.__setattr__(self, "materialize_as", normalized_materialize)
        object.__setattr__(self, "locator_kind", normalized_locator_kind)
        object.__setattr__(self, "locator_token", normalized_locator_token)
        object.__setattr__(self, "consume_on_read", bool(self.consume_on_read))
        object.__setattr__(self, "node_id", str(self.node_id or "").strip())
        object.__setattr__(self, "node_instance_id", str(self.node_instance_id or "").strip())
        object.__setattr__(self, "control_addr", normalized_control_addr or normalized_locator_token if normalized_locator_kind == "node_control" else normalized_control_addr)

    @property
    def object_id(self) -> str:
        return _normalize_storage_id(self.storage_id, fallback=self.ref_id)

    def to_payload(self) -> Dict[str, Dict[str, object]]:
        return {
            DATA_REF_SENTINEL: {
                "ref_id": self.ref_id,
                "storage_id": self.storage_id,
                "logical_type": self.logical_type,
                "format": self.format,
                "size_bytes": self.size_bytes,
                "materialize_as": self.materialize_as,
                "locator_kind": self.locator_kind,
                "locator_token": self.locator_token,
                "consume_on_read": self.consume_on_read,
                "node_id": self.node_id,
                "node_instance_id": self.node_instance_id,
                "control_addr": self.control_addr,
            }
        }


def is_data_ref_payload(data: Any) -> bool:
    return (
        isinstance(data, dict)
        and set(data.keys()) == {DATA_REF_SENTINEL}
        and isinstance(data.get(DATA_REF_SENTINEL), dict)
    )


def data_ref_from_payload(data: Dict[str, object]) -> DataRef:
    if not is_data_ref_payload(data):
        raise ValueError("payload is not a DataRef sentinel")
    payload = dict(data[DATA_REF_SENTINEL] or {})
    return DataRef(
        ref_id=str(payload.get("ref_id", "") or payload.get("storage_id", "") or ""),
        storage_id=str(payload.get("storage_id", "") or payload.get("ref_id", "") or ""),
        logical_type=str(payload.get("logical_type", "") or ""),
        format=str(payload.get("format", "") or ""),
        size_bytes=int(payload.get("size_bytes", 0) or 0),
        materialize_as=str(payload.get("materialize_as", "") or "auto"),
        locator_kind=str(payload.get("locator_kind", "") or "controlplane"),
        locator_token=str(payload.get("locator_token", "") or ""),
        consume_on_read=bool(payload.get("consume_on_read", False)),
        node_id=str(payload.get("node_id", "") or ""),
        node_instance_id=str(payload.get("node_instance_id", "") or ""),
        control_addr=str(payload.get("control_addr", "") or ""),
    )


def data_ref_to_payload(ref: DataRef) -> Dict[str, Dict[str, object]]:
    return ref.to_payload()


def data_ref_from_object_ref(ref: Any) -> DataRef:
    return DataRef(
        ref_id=str(ref.object_id or ""),
        storage_id=str(ref.object_id or ""),
        logical_type=_infer_logical_type(logical_type="", fmt=str(ref.format or ""), materialize_as=str(ref.materialize_as or "")),
        format=str(ref.format or "bin"),
        size_bytes=int(ref.size_bytes or 0),
        materialize_as=str(ref.materialize_as or "path"),
        locator_kind="node_local",
        locator_token="",
        consume_on_read=bool(getattr(ref, "consume_on_read", False)),
    )


def data_ref_from_result_ref(ref: Any) -> DataRef:
    control_addr = str(getattr(ref, "control_addr", "") or "").strip()
    return DataRef(
        ref_id=str(ref.object_id or ""),
        storage_id=str(ref.object_id or ""),
        logical_type=_infer_logical_type(logical_type="", fmt=str(ref.format or ""), materialize_as=str(ref.materialize_as or "")),
        format=str(ref.format or "bin"),
        size_bytes=int(ref.size_bytes or 0),
        materialize_as=str(ref.materialize_as or "path"),
        locator_kind="node_control" if control_addr else "node_local",
        locator_token=control_addr,
        consume_on_read=bool(getattr(ref, "consume_on_read", False)),
        node_id=str(getattr(ref, "node_id", "") or ""),
        node_instance_id=str(getattr(ref, "node_instance_id", "") or ""),
        control_addr=control_addr,
    )


def coerce_data_ref(value: Any) -> DataRef:
    if isinstance(value, DataRef):
        return value

    from pycloud_parallel.controlplane.object_ref import ObjectRef, is_object_ref_payload, object_ref_from_payload
    from pycloud_parallel.controlplane.result_ref import ResultRef, is_result_ref_payload, result_ref_from_payload

    if isinstance(value, ObjectRef):
        return data_ref_from_object_ref(value)
    if isinstance(value, ResultRef):
        return data_ref_from_result_ref(value)
    if isinstance(value, dict):
        if is_data_ref_payload(value):
            return data_ref_from_payload(value)
        if is_object_ref_payload(value):
            return data_ref_from_object_ref(object_ref_from_payload(value))
        if is_result_ref_payload(value):
            return data_ref_from_result_ref(result_ref_from_payload(value))
    raise TypeError(f"value is not a supported data ref: {type(value).__name__}")


def maybe_data_ref(value: Any) -> Optional[DataRef]:
    try:
        return coerce_data_ref(value)
    except Exception:
        return None


def resolve_data_ref_materialize_as(ref: DataRef, *, default: str = "path") -> str:
    preference = normalize_data_ref_materialize_as(ref.materialize_as, default="auto")
    if preference != "auto":
        return preference
    logical_type = str(ref.logical_type or "").strip().lower()
    if logical_type == "dataframe":
        return "dataframe"
    if logical_type == "series":
        return "series"
    if logical_type == "ndarray":
        return "ndarray"
    if logical_type == "json":
        return "json"
    if logical_type == "text":
        return "text"
    if logical_type in {"file", "archive"}:
        return "path"
    if logical_type == "bytes":
        return "bytes"
    return normalize_data_ref_materialize_as(default, default="path")


def with_data_ref_control_addr(value: Any, *, control_addr: str) -> Any:
    normalized_control_addr = str(control_addr or "").strip()
    if not normalized_control_addr:
        return value
    from pycloud_parallel.controlplane.result_ref import ResultRef

    if isinstance(value, ResultRef) and not value.control_addr:
        return replace(value, control_addr=normalized_control_addr)
    data_ref = maybe_data_ref(value)
    if data_ref is None or data_ref.control_addr:
        return value
    return replace(
        data_ref,
        locator_kind="node_control",
        locator_token=normalized_control_addr,
        control_addr="",
    )


def with_data_ref_locator(
    value: Any,
    *,
    locator_kind: str,
    locator_token: str,
    node_id: str = "",
    node_instance_id: str = "",
    control_addr: str = "",
) -> Any:
    normalized_kind = str(locator_kind or "").strip().lower()
    normalized_token = str(locator_token or "").strip()
    if not normalized_kind or not normalized_token:
        return value
    data_ref = maybe_data_ref(value)
    if data_ref is None:
        return value
    return replace(
        data_ref,
        locator_kind=normalized_kind,
        locator_token=normalized_token,
        node_id=str(node_id or data_ref.node_id or "").strip(),
        node_instance_id=str(node_instance_id or data_ref.node_instance_id or "").strip(),
        control_addr=(
            ""
            if normalized_kind == "controlplane" and not str(control_addr or "").strip()
            else str(control_addr or data_ref.control_addr or "").strip()
        ),
    )


__all__ = [
    "DATA_REF_SENTINEL",
    "DataRef",
    "coerce_data_ref",
    "data_ref_from_object_ref",
    "data_ref_from_payload",
    "data_ref_from_result_ref",
    "data_ref_to_payload",
    "is_data_ref_payload",
    "maybe_data_ref",
    "normalize_data_ref_id",
    "normalize_data_ref_materialize_as",
    "resolve_data_ref_materialize_as",
    "with_data_ref_locator",
    "with_data_ref_control_addr",
]
