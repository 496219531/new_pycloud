from __future__ import annotations

"""Structured JSON-compatible serialization mode for explicit non-pickle transport."""

import base64
import json
from typing import Any


_STRUCTURED_V1_SENTINEL = "__pycloud_structured_v1__"
_STRUCTURED_V1_BYTES_SENTINEL = "__pycloud_structured_v1_bytes__"


def _encode_value(value: Any) -> Any:
    from pycloud_parallel.controlplane.serialization import serialize_arrow_compatible

    if isinstance(value, (bytes, bytearray, memoryview)):
        return {
            _STRUCTURED_V1_BYTES_SENTINEL: {
                "encoding": "base64",
                "data": base64.b64encode(bytes(value)).decode("ascii"),
            }
        }
    if isinstance(value, dict):
        return {str(key): _encode_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_encode_value(item) for item in value]
    if isinstance(value, tuple):
        return [_encode_value(item) for item in value]
    return serialize_arrow_compatible(value)


def _decode_value(value: Any) -> Any:
    from pycloud_parallel.controlplane.serialization import convert_dict_to_arrow

    if isinstance(value, dict) and _STRUCTURED_V1_BYTES_SENTINEL in value:
        payload = dict(value.get(_STRUCTURED_V1_BYTES_SENTINEL) or {})
        if str(payload.get("encoding", "") or "").strip().lower() != "base64":
            raise ValueError("unsupported structured_v1 bytes encoding")
        return base64.b64decode(str(payload.get("data", "") or "").encode("ascii"))
    if isinstance(value, dict):
        if value.get("__type__") or "__pycloud_data_ref__" in value:
            return convert_dict_to_arrow(value)
        return {str(key): _decode_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_decode_value(item) for item in value]
    return convert_dict_to_arrow(value)


def structured_dumps(value: Any, *, ensure_ascii: bool = False) -> bytes:
    payload = {
        _STRUCTURED_V1_SENTINEL: {
            "version": 1,
            "payload": _encode_value(value),
        }
    }
    return json.dumps(payload, ensure_ascii=ensure_ascii, separators=(",", ":")).encode("utf-8")


def structured_loads(blob: bytes | bytearray | memoryview | str) -> Any:
    raw = blob.decode("utf-8") if isinstance(blob, (bytes, bytearray, memoryview)) else str(blob or "")
    payload = json.loads(raw or "{}")
    if not isinstance(payload, dict) or _STRUCTURED_V1_SENTINEL not in payload:
        raise ValueError("invalid structured_v1 payload")
    envelope = dict(payload.get(_STRUCTURED_V1_SENTINEL) or {})
    version = int(envelope.get("version", 0) or 0)
    if version != 1:
        raise ValueError(f"unsupported structured_v1 version: {version}")
    return _decode_value(envelope.get("payload"))


__all__ = [
    "structured_dumps",
    "structured_loads",
]
