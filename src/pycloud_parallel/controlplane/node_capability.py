from __future__ import annotations

"""Node capability models and local capability discovery helpers."""

from dataclasses import dataclass
from math import inf
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from pycloud_parallel.controlplane.config import (
    CONTROL_HTTP_MAX_RECEIVE_BYTES,
    CONTROL_HTTP_MAX_SEND_BYTES,
    GATEWAY_MAX_UPLOAD_FILE_BYTES,
    GATEWAY_MAX_UPLOAD_TOTAL_BYTES,
    SERVICE_HTTP_BODY_MAX_BYTES,
)
from pycloud_parallel.controlplane.serialization_mode import SUPPORTED_SERIALIZATION_MODES, normalize_serialization_mode


def _normalize_modes(values: Sequence[str] | None) -> Tuple[str, ...]:
    out = []
    seen = set()
    for item in values or ():
        normalized = normalize_serialization_mode(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return tuple(out)


@dataclass(frozen=True)
class NodeCapability:
    supported_modes: Tuple[str, ...] = ()
    supports_raw_bytes_payload: bool = False
    supports_http_raw_bytes_body: bool = False
    supports_http_control: bool = False
    control_base_url: str = ""
    max_control_send_bytes: int = 0
    max_control_recv_bytes: int = 0
    max_http_body_bytes: int = 0
    max_upload_file_bytes: int = 0
    max_upload_total_bytes: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "supported_modes", _normalize_modes(self.supported_modes))
        object.__setattr__(self, "supports_raw_bytes_payload", bool(self.supports_raw_bytes_payload))
        object.__setattr__(self, "supports_http_raw_bytes_body", bool(self.supports_http_raw_bytes_body))
        object.__setattr__(self, "supports_http_control", bool(self.supports_http_control))
        object.__setattr__(self, "control_base_url", str(self.control_base_url or "").strip())
        object.__setattr__(self, "max_control_send_bytes", max(0, int(self.max_control_send_bytes or 0)))
        object.__setattr__(self, "max_control_recv_bytes", max(0, int(self.max_control_recv_bytes or 0)))
        object.__setattr__(self, "max_http_body_bytes", max(0, int(self.max_http_body_bytes or 0)))
        object.__setattr__(self, "max_upload_file_bytes", max(0, int(self.max_upload_file_bytes or 0)))
        object.__setattr__(self, "max_upload_total_bytes", max(0, int(self.max_upload_total_bytes or 0)))

    @property
    def supports_http_nodecontrol(self) -> bool:
        return bool(self.supports_http_control)

    @property
    def supports_transport_payload_bytes(self) -> bool:
        return bool(self.supports_raw_bytes_payload)

    @property
    def supports_http_bytes_transport(self) -> bool:
        return bool(self.supports_http_raw_bytes_body)

    @property
    def node_http_base_url(self) -> str:
        return str(self.control_base_url or "")

    def to_dict(self) -> Dict[str, object]:
        return {
            "supported_modes": list(self.supported_modes),
            "supports_raw_bytes_payload": bool(self.supports_raw_bytes_payload),
            "supports_http_raw_bytes_body": bool(self.supports_http_raw_bytes_body),
            "supports_http_control": bool(self.supports_http_control),
            "control_base_url": str(self.control_base_url or ""),
            "max_control_send_bytes": int(self.max_control_send_bytes),
            "max_control_recv_bytes": int(self.max_control_recv_bytes),
            "max_http_body_bytes": int(self.max_http_body_bytes),
            "max_upload_file_bytes": int(self.max_upload_file_bytes),
            "max_upload_total_bytes": int(self.max_upload_total_bytes),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any] | None) -> "NodeCapability":
        if not isinstance(payload, Mapping):
            return cls()
        return cls(
            supported_modes=tuple(payload.get("supported_modes") or ()),
            supports_raw_bytes_payload=bool(
                payload.get("supports_raw_bytes_payload", payload.get("supports_transport_payload_bytes", False))
            ),
            supports_http_raw_bytes_body=bool(
                payload.get("supports_http_raw_bytes_body", payload.get("supports_http_bytes_transport", False))
            ),
            supports_http_control=bool(
                payload.get("supports_http_control", payload.get("supports_http_nodecontrol", False))
            ),
            control_base_url=str(payload.get("control_base_url", payload.get("node_http_base_url", "")) or ""),
            max_control_send_bytes=int(payload.get("max_control_send_bytes", 0) or 0),
            max_control_recv_bytes=int(payload.get("max_control_recv_bytes", 0) or 0),
            max_http_body_bytes=int(payload.get("max_http_body_bytes", 0) or 0),
            max_upload_file_bytes=int(payload.get("max_upload_file_bytes", 0) or 0),
            max_upload_total_bytes=int(payload.get("max_upload_total_bytes", 0) or 0),
        )

    def control_payload_limit_bytes(self) -> float:
        limits = [value for value in (self.max_control_send_bytes, self.max_control_recv_bytes) if int(value or 0) > 0]
        if not limits:
            return inf
        return float(min(limits))

    def http_payload_limit_bytes(self) -> float:
        if int(self.max_http_body_bytes or 0) <= 0:
            return inf
        return float(self.max_http_body_bytes)

    def is_empty(self) -> bool:
        return not any(
            (
                self.supported_modes,
                bool(self.supports_raw_bytes_payload),
                bool(self.supports_http_raw_bytes_body),
                bool(self.supports_http_control),
                bool(str(self.control_base_url or "").strip()),
                int(self.max_control_send_bytes or 0) > 0,
                int(self.max_control_recv_bytes or 0) > 0,
                int(self.max_http_body_bytes or 0) > 0,
                int(self.max_upload_file_bytes or 0) > 0,
                int(self.max_upload_total_bytes or 0) > 0,
            )
        )


def detect_local_node_capability(
    *,
    supported_modes: Sequence[str] | None = None,
    supports_raw_bytes_payload: Optional[bool] = None,
    supports_http_raw_bytes_body: Optional[bool] = None,
    supports_http_control: Optional[bool] = None,
    control_base_url: str = "",
    max_control_send_bytes: Optional[int] = None,
    max_control_recv_bytes: Optional[int] = None,
    max_http_body_bytes: Optional[int] = None,
    max_upload_file_bytes: Optional[int] = None,
    max_upload_total_bytes: Optional[int] = None,
) -> NodeCapability:
    return NodeCapability(
        supported_modes=tuple(supported_modes or SUPPORTED_SERIALIZATION_MODES),
        supports_raw_bytes_payload=True if supports_raw_bytes_payload is None else bool(supports_raw_bytes_payload),
        supports_http_raw_bytes_body=True if supports_http_raw_bytes_body is None else bool(supports_http_raw_bytes_body),
        supports_http_control=True if supports_http_control is None else bool(supports_http_control),
        control_base_url=str(control_base_url or "").strip(),
        max_control_send_bytes=int(
            CONTROL_HTTP_MAX_SEND_BYTES if max_control_send_bytes is None else max_control_send_bytes
        ),
        max_control_recv_bytes=int(
            CONTROL_HTTP_MAX_RECEIVE_BYTES if max_control_recv_bytes is None else max_control_recv_bytes
        ),
        max_http_body_bytes=int(SERVICE_HTTP_BODY_MAX_BYTES if max_http_body_bytes is None else max_http_body_bytes),
        max_upload_file_bytes=int(
            GATEWAY_MAX_UPLOAD_FILE_BYTES if max_upload_file_bytes is None else max_upload_file_bytes
        ),
        max_upload_total_bytes=int(
            GATEWAY_MAX_UPLOAD_TOTAL_BYTES if max_upload_total_bytes is None else max_upload_total_bytes
        ),
    )


def control_base_url_from_capability(value: object) -> str:
    capability = capability_from_candidate(value)
    if capability is None:
        capability = getattr(value, "capability", None)
    if isinstance(capability, NodeCapability):
        return str(capability.control_base_url or "").strip()
    if isinstance(capability, Mapping):
        return str(capability.get("control_base_url", capability.get("node_http_base_url", "")) or "").strip()
    return str(getattr(capability, "control_base_url", getattr(capability, "node_http_base_url", "")) or "").strip()


def capability_from_candidate(value: object) -> Optional[NodeCapability]:
    if isinstance(value, NodeCapability):
        return None if value.is_empty() else value
    if isinstance(value, Mapping):
        direct_keys = {
            "supported_modes",
            "supports_raw_bytes_payload",
            "supports_http_raw_bytes_body",
            "supports_transport_payload_bytes",
            "supports_http_bytes_transport",
            "supports_http_control",
            "control_base_url",
            "supports_http_nodecontrol",
            "node_http_base_url",
            "max_control_send_bytes",
            "max_control_recv_bytes",
            "max_http_body_bytes",
            "max_upload_file_bytes",
            "max_upload_total_bytes",
        }
        if direct_keys & set(value.keys()):
            capability = NodeCapability.from_dict(value)
            return None if capability.is_empty() else capability
        nested = value.get("capability")
        if isinstance(nested, NodeCapability):
            return None if nested.is_empty() else nested
        if isinstance(nested, Mapping):
            capability = NodeCapability.from_dict(nested)
            return None if capability.is_empty() else capability
        return None
    capability = getattr(value, "capability", None)
    if isinstance(capability, NodeCapability):
        return None if capability.is_empty() else capability
    if isinstance(capability, Mapping):
        parsed = NodeCapability.from_dict(capability)
        return None if parsed.is_empty() else parsed
    return None


__all__ = [
    "capability_from_candidate",
    "control_base_url_from_capability",
    "NodeCapability",
    "detect_local_node_capability",
]
