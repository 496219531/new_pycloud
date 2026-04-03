from __future__ import annotations

"""Shared ResultRef helpers for task result indirection."""

from dataclasses import dataclass
from typing import Any, Dict

from pycloud_parallel.controlplane.object_ref import normalize_materialize_as, normalize_object_format, normalize_object_id

RESULT_REF_SENTINEL = "__pycloud_result_ref__"


@dataclass(frozen=True)
class ResultRef:
    object_id: str
    node_id: str
    control_addr: str = ""
    format: str = "bin"
    size_bytes: int = 0
    materialize_as: str = "path"

    def __post_init__(self) -> None:
        object.__setattr__(self, "object_id", normalize_object_id(self.object_id))
        object.__setattr__(self, "node_id", str(self.node_id or "").strip())
        object.__setattr__(self, "control_addr", str(self.control_addr or "").strip())
        object.__setattr__(self, "format", normalize_object_format(self.format, default="bin"))
        object.__setattr__(self, "size_bytes", max(0, int(self.size_bytes or 0)))
        object.__setattr__(self, "materialize_as", normalize_materialize_as(self.materialize_as, default="path"))

    def to_payload(self) -> Dict[str, Dict[str, object]]:
        return {
            RESULT_REF_SENTINEL: {
                "object_id": self.object_id,
                "node_id": self.node_id,
                "control_addr": self.control_addr,
                "format": self.format,
                "size_bytes": self.size_bytes,
                "materialize_as": self.materialize_as,
            }
        }


def is_result_ref_payload(data: Any) -> bool:
    return (
        isinstance(data, dict)
        and set(data.keys()) == {RESULT_REF_SENTINEL}
        and isinstance(data.get(RESULT_REF_SENTINEL), dict)
    )


def result_ref_from_payload(data: Dict[str, object]) -> ResultRef:
    if not is_result_ref_payload(data):
        raise ValueError("payload is not a ResultRef sentinel")
    payload = dict(data[RESULT_REF_SENTINEL] or {})
    return ResultRef(
        object_id=str(payload.get("object_id", "") or ""),
        node_id=str(payload.get("node_id", "") or ""),
        control_addr=str(payload.get("control_addr", "") or ""),
        format=str(payload.get("format", "") or ""),
        size_bytes=int(payload.get("size_bytes", 0) or 0),
        materialize_as=str(payload.get("materialize_as", "") or "path"),
    )


def result_ref_to_payload(ref: ResultRef) -> Dict[str, Dict[str, object]]:
    return ref.to_payload()
