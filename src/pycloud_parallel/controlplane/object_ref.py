from __future__ import annotations

"""Shared ObjectRef helpers for node-local large object caching."""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

OBJECT_REF_SENTINEL = "__pycloud_object_ref__"
_OBJECT_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_OBJECT_FORMAT_RE = re.compile(r"[^A-Za-z0-9._-]+")
_MATERIALIZE_AS = {"path", "dataframe", "series", "ndarray", "json", "bytes", "text"}


def normalize_object_id(object_id: str) -> str:
    text = str(object_id or "").strip().lower()
    if not text:
        raise ValueError("object_id must not be empty")
    if not _OBJECT_ID_RE.match(text):
        raise ValueError(f"invalid object_id: {object_id!r}")
    return text


def object_id_from_sha256_hex(digest: str) -> str:
    hex_text = str(digest or "").strip().lower()
    if len(hex_text) != 64 or any(ch not in "0123456789abcdef" for ch in hex_text):
        raise ValueError(f"invalid sha256 digest: {digest!r}")
    return f"sha256:{hex_text}"


def normalize_object_format(fmt: str = "", *, source_name: str = "", default: str = "bin") -> str:
    text = str(fmt or "").strip().lower().lstrip(".")
    if text:
        return _OBJECT_FORMAT_RE.sub("_", text).strip("._") or str(default or "bin")
    suffixes = [part.lstrip(".").lower() for part in Path(str(source_name or "")).suffixes if part]
    if suffixes:
        joined = ".".join(suffixes)
        return _OBJECT_FORMAT_RE.sub("_", joined).strip("._") or str(default or "bin")
    return str(default or "bin").strip().lower() or "bin"


def object_format_suffix(fmt: str) -> str:
    normalized = normalize_object_format(fmt)
    return f".{normalized}" if normalized else ""


def object_storage_path(base_dir: Path, *, object_id: str, fmt: str) -> Path:
    normalized_id = normalize_object_id(object_id)
    digest = normalized_id.replace("sha256:", "", 1)
    suffix = object_format_suffix(fmt)
    prefix = digest[:2]
    rest = digest[2:]
    return Path(base_dir) / prefix / f"{rest}{suffix}"


def normalize_materialize_as(value: str = "", *, default: str = "path") -> str:
    text = str(value or "").strip().lower()
    if not text:
        text = str(default or "path").strip().lower() or "path"
    if text not in _MATERIALIZE_AS:
        raise ValueError(f"unsupported materialize_as: {value!r}")
    return text


@dataclass(frozen=True)
class ObjectRef:
    object_id: str
    format: str = "bin"
    size_bytes: int = 0
    materialize_as: str = "path"
    consume_on_read: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "object_id", normalize_object_id(self.object_id))
        object.__setattr__(self, "format", normalize_object_format(self.format, default="bin"))
        object.__setattr__(self, "size_bytes", max(0, int(self.size_bytes or 0)))
        object.__setattr__(self, "materialize_as", normalize_materialize_as(self.materialize_as, default="path"))
        object.__setattr__(self, "consume_on_read", bool(self.consume_on_read))

    def to_payload(self) -> Dict[str, Dict[str, object]]:
        return {
            OBJECT_REF_SENTINEL: {
                "object_id": self.object_id,
                "format": self.format,
                "size_bytes": self.size_bytes,
                "materialize_as": self.materialize_as,
                "consume_on_read": self.consume_on_read,
            }
        }

    def to_data_ref(self):
        from pycloud_parallel.controlplane.data_ref import data_ref_from_object_ref

        return data_ref_from_object_ref(self)


def is_object_ref_payload(data: Any) -> bool:
    return (
        isinstance(data, dict)
        and set(data.keys()) == {OBJECT_REF_SENTINEL}
        and isinstance(data.get(OBJECT_REF_SENTINEL), dict)
    )


def object_ref_from_payload(data: Dict[str, object]) -> ObjectRef:
    if not is_object_ref_payload(data):
        raise ValueError("payload is not an ObjectRef sentinel")
    payload = dict(data[OBJECT_REF_SENTINEL] or {})
    return ObjectRef(
        object_id=str(payload.get("object_id", "") or ""),
        format=str(payload.get("format", "") or ""),
        size_bytes=int(payload.get("size_bytes", 0) or 0),
        materialize_as=str(payload.get("materialize_as", "") or "path"),
        consume_on_read=bool(payload.get("consume_on_read", False)),
    )


def object_ref_to_payload(ref: ObjectRef) -> Dict[str, Dict[str, object]]:
    return ref.to_payload()


def object_ref_from_data_ref(ref: object) -> ObjectRef:
    from pycloud_parallel.controlplane.data_ref import coerce_data_ref

    data_ref = coerce_data_ref(ref)
    return ObjectRef(
        object_id=data_ref.object_id,
        format=data_ref.format,
        size_bytes=data_ref.size_bytes,
        materialize_as=data_ref.materialize_as if data_ref.materialize_as != "auto" else "path",
        consume_on_read=bool(data_ref.consume_on_read),
    )
