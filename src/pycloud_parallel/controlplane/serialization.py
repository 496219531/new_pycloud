from __future__ import annotations

"""Shared helpers for Arrow-compatible payload/result serialization."""

import base64
from datetime import date, datetime, time, timedelta
import hashlib
import logging
import json
import pickle
from typing import Any, Optional, Sequence

from google.protobuf import struct_pb2

from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.controlplane.config import (
    get_payload_policy,
    get_inline_transport_checksum,
)
from pycloud_parallel.data.ref import (
    DataRef,
    coerce_data_ref,
    data_ref_from_payload,
    data_ref_to_payload,
    is_data_ref_payload,
    maybe_data_ref,
)
from pycloud_parallel.controlplane.pickle_stable_v1 import (
    stable_pickle_dumps,
    stable_pickle_load_file,
    stable_pickle_loads,
)
from pycloud_parallel.controlplane.serialization_mode import (
    PICKLE_SERIALIZATION_MODES,
    resolve_declared_transport_mode,
    resolve_effective_serialization_mode,
    resolve_received_transport_mode,
)
from pycloud_parallel.controlplane.structured_v1 import structured_dumps, structured_loads

payload_flow_logger = logging.getLogger("pycloud_parallel.payload_flow")
MAX_ARROW_RECURSION_DEPTH = 200
TRANSPORT_ENVELOPE_SENTINEL = "__pycloud_transport__"
INLINE_TRANSPORT_CARRIER_SENTINEL = "__pycloud_inline_transport__"
TRANSPORT_PAYLOAD_VERSION = 1
INTERNAL_PICKLE_NATIVE_V1 = "pickle_native_v1"
LOCAL_IPC_SERIALIZATION_MODE = INTERNAL_PICKLE_NATIVE_V1
PICKLE_RAW_BYTES_MODES = frozenset(PICKLE_SERIALIZATION_MODES)
_INTERNAL_PICKLE_NATIVE_CONTEXTS = {
    "service_internal",
    "service_owner",
    "taskpool_session",
    "service_result",
    "local_ipc",
}


def _format_payload_bytes(size_bytes: int) -> str:
    size = max(0, int(size_bytes or 0))
    if size >= 1024 * 1024:
        return f"{size / (1024 * 1024):.2f} MiB"
    if size >= 1024:
        return f"{size / 1024:.2f} KiB"
    return f"{size} B"


def summarize_payload_flow_value(value: Any) -> str:
    if value is None:
        return "None"
    data_ref = maybe_data_ref(value)
    if data_ref is not None:
        return (
            f"DataRef(logical_type={data_ref.logical_type}, format={data_ref.format}, "
            f"size_bytes={data_ref.size_bytes}, materialize_as={data_ref.materialize_as})"
        )
    if isinstance(value, dict):
        keys = list(value.keys())
        preview = keys[:5]
        suffix = "..." if len(keys) > 5 else ""
        return f"dict(len={len(value)}, keys={preview}{suffix})"
    if isinstance(value, (list, tuple)):
        return f"{type(value).__name__}(len={len(value)})"
    if isinstance(value, datetime):
        return f"datetime({value.isoformat()})"
    if isinstance(value, date) and not isinstance(value, datetime):
        return f"date({value.isoformat()})"
    if isinstance(value, time):
        return f"time({value.isoformat()})"
    if isinstance(value, timedelta):
        return f"timedelta(seconds={value.total_seconds()})"

    try:
        import pandas as pd

        if isinstance(value, pd.DataFrame):
            return (
                f"DataFrame(shape={value.shape}, "
                f"index={type(value.index).__name__}, columns={type(value.columns).__name__})"
            )
        if isinstance(value, pd.Series):
            return (
                f"Series(len={len(value)}, index={type(value.index).__name__}, "
                f"name={value.name!r})"
            )
        if isinstance(value, pd.Index):
            return f"{type(value).__name__}(len={len(value)}, name={value.name!r})"
        if isinstance(value, pd.Timestamp):
            return f"Timestamp({value.isoformat()})"
        if isinstance(value, pd.Timedelta):
            return f"Timedelta({value.isoformat()})"
    except ImportError:
        pass

    try:
        import numpy as np

        if isinstance(value, np.ndarray):
            return f"ndarray(shape={value.shape}, dtype={value.dtype})"
    except ImportError:
        pass

    if isinstance(value, (str, int, float, bool)):
        return f"{type(value).__name__}({value!r})"
    return type(value).__name__


def log_payload_flow(event: str, /, **fields: Any) -> None:
    if not payload_flow_logger.isEnabledFor(logging.DEBUG):
        return
    pieces = [f"event={event}"]
    for key, value in fields.items():
        pieces.append(f"{key}={value}")
    payload_flow_logger.debug(" ".join(pieces))


def _inline_payload_limit_hint() -> str:
    return (
        "Use put_data()/put_dataframe()/put_ndarray()/put_json()/"
        "put_object_from_file()/put_object_from_bytes() and pass DataRef instead."
    )


def inline_payload_limit_error(size_bytes: int, *, limit_bytes: int, context: str) -> ValueError:
    return ValueError(
        f"{context} serialized to {_format_payload_bytes(size_bytes)}, "
        f"which exceeds the inline limit {_format_payload_bytes(limit_bytes)}. "
        f"{_inline_payload_limit_hint()}"
    )


def _default_payload_hard_limit_bytes() -> int:
    return max(1, int(get_payload_policy("http_call").inline_payload_hard_limit_bytes))


def _default_result_hard_limit_bytes() -> int:
    return max(1, int(get_payload_policy("result").inline_result_hard_limit_bytes))


def _effective_limit_bytes(value: int, *, default: int) -> int:
    return max(1, int(value or default))


def validate_inline_payload_size(size_bytes: int, *, limit_bytes: int = 0, context: str = "payload") -> int:
    normalized = max(0, int(size_bytes or 0))
    effective_limit = _effective_limit_bytes(limit_bytes, default=_default_payload_hard_limit_bytes())
    if normalized > effective_limit:
        raise inline_payload_limit_error(normalized, limit_bytes=effective_limit, context=context)
    return normalized


INLINE_STRUCT_WIRE_OVERHEAD_RATIO = 0.1
INLINE_STRUCT_WIRE_OVERHEAD_MIN_BYTES = 512


def _struct_wire_size_with_overhead(data: struct_pb2.Struct) -> int:
    size = int(data.ByteSize())
    overhead = max(INLINE_STRUCT_WIRE_OVERHEAD_MIN_BYTES, int(size * INLINE_STRUCT_WIRE_OVERHEAD_RATIO))
    return size + overhead


def validate_inline_payload_struct(data: struct_pb2.Struct, *, limit_bytes: int = 0, context: str = "payload") -> int:
    return validate_inline_payload_size(
        _struct_wire_size_with_overhead(data),
        limit_bytes=limit_bytes,
        context=context,
    )


def validate_inline_request_size(size_bytes: int, *, limit_bytes: int = 0, context: str = "payload request") -> int:
    return validate_inline_payload_size(
        size_bytes,
        limit_bytes=_effective_limit_bytes(limit_bytes, default=_default_payload_hard_limit_bytes()),
        context=context,
    )


def inline_result_limit_error(size_bytes: int, *, limit_bytes: int, context: str) -> ValueError:
    return ValueError(
        f"{context} serialized to {_format_payload_bytes(size_bytes)}, "
        f"which exceeds the inline result limit {_format_payload_bytes(limit_bytes)}. "
        "For large results, write to a node-local file or stream chunks and return a lightweight handle instead."
    )


def validate_inline_result_size(size_bytes: int, *, limit_bytes: int = 0, context: str = "result") -> int:
    normalized = max(0, int(size_bytes or 0))
    effective_limit = _effective_limit_bytes(limit_bytes, default=_default_result_hard_limit_bytes())
    if normalized > effective_limit:
        raise inline_result_limit_error(normalized, limit_bytes=effective_limit, context=context)
    return normalized


def validate_inline_result_struct(data: struct_pb2.Struct, *, limit_bytes: int = 0, context: str = "result") -> int:
    return validate_inline_result_size(
        _struct_wire_size_with_overhead(data),
        limit_bytes=limit_bytes,
        context=context,
    )


def serialize_inline_result(
    data: Optional[dict],
    *,
    context: str = "result",
    limit_bytes: int = 0,
    mode: str = "",
) -> tuple[dict, struct_pb2.Struct, int]:
    normalized_data = {} if data is None else data
    transport_value = (
        data
        if (isinstance(data, dict) and TRANSPORT_ENVELOPE_SENTINEL in data)
        else encode_transport_value(normalized_data, mode=mode, context=context)
    )
    serialized = serialize_arrow_compatible(transport_value)
    out = struct_pb2.Struct()
    out.update(serialized)
    size_bytes = validate_inline_result_struct(out, limit_bytes=limit_bytes, context=context)
    log_payload_flow(
        "inline_result_encode",
        context=context,
        size_bytes=size_bytes,
        summary=summarize_payload_flow_value(normalized_data),
    )
    return serialized, out, size_bytes


def serialize_by_mode(value: Any, *, mode: str = "") -> Any:
    normalized = resolve_effective_serialization_mode(
        request_mode=mode,
        context="transport_encode",
    )
    if normalized == "structured_v1":
        return structured_dumps(value)
    if normalized == "pickle_stable_v1":
        return stable_pickle_dumps(value)
    if normalized == INTERNAL_PICKLE_NATIVE_V1:
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    return serialize_arrow_compatible(value)


def deserialize_by_mode(value: Any, *, mode: str = "") -> Any:
    normalized = resolve_effective_serialization_mode(
        request_mode=mode,
        context="transport_decode",
    )
    if normalized == "structured_v1":
        return structured_loads(value)
    if normalized == "pickle_stable_v1":
        return stable_pickle_loads(value)
    if normalized == INTERNAL_PICKLE_NATIVE_V1:
        return pickle.loads(bytes(value or b""))
    return convert_dict_to_arrow(value)


def detect_transport_mode(value: Any, *, default: str = "") -> str:
    normalized_default = resolve_declared_transport_mode(default_mode=default)
    if isinstance(value, dict) and TRANSPORT_ENVELOPE_SENTINEL in value:
        envelope = dict(value.get(TRANSPORT_ENVELOPE_SENTINEL) or {})
        codec = str(envelope.get("codec", "") or "").strip().lower()
        if codec:
            return resolve_declared_transport_mode(declared_mode=codec)
    return normalized_default


def _adapt_blob_for_json_transport(blob: bytes | bytearray | memoryview) -> dict[str, str]:
    """Adapt raw bytes for JSON/Struct transport containers.

    This helper lives in the transport layer on purpose: codecs such as
    ``pickle_stable_v1`` produce raw bytes and do not pre-encode them for
    text-only containers.
    """
    raw = bytes(blob)
    return {
        "encoding": "base64",
        "data": base64.b64encode(raw).decode("ascii"),
    }


def _restore_blob_from_json_transport(payload: Any) -> bytes:
    normalized = dict(payload or {})
    if str(normalized.get("encoding", "") or "").strip().lower() != "base64":
        raise ValueError("unsupported transport byte encoding")
    return base64.b64decode(str(normalized.get("data", "") or "").encode("ascii"))


def encode_transport_value(value: Any, *, mode: str = "", context: str = "payload") -> Any:
    normalized = resolve_effective_serialization_mode(
        request_mode=mode,
        context="transport_encode",
    )
    if normalized == "legacy_v1":
        encoded = serialize_arrow_compatible(value)
        log_payload_flow("transport_encode", context=context, codec=normalized, summary=summarize_payload_flow_value(value))
        return encoded
    if normalized == "structured_v1":
        payload = json.loads(structured_dumps(value).decode("utf-8"))
        log_payload_flow("transport_encode", context=context, codec=normalized, summary=summarize_payload_flow_value(value))
        return {
            TRANSPORT_ENVELOPE_SENTINEL: {
                "codec": normalized,
                "version": 1,
                "payload": payload,
            }
        }
    if normalized in PICKLE_RAW_BYTES_MODES:
        blob = (
            stable_pickle_dumps(value)
            if normalized == "pickle_stable_v1"
            else pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        )
        log_payload_flow("transport_encode", context=context, codec=normalized, summary=summarize_payload_flow_value(value))
        return {
            TRANSPORT_ENVELOPE_SENTINEL: {
                "codec": normalized,
                "version": 1,
                "payload": _adapt_blob_for_json_transport(blob),
            }
        }
    raise ValueError(f"unsupported serialization mode: {normalized!r}")


def prefers_raw_bytes_payload(mode: str = "") -> bool:
    normalized = resolve_effective_serialization_mode(
        request_mode=mode,
        context="transport_encode",
    )
    return normalized in {"structured_v1", *PICKLE_RAW_BYTES_MODES}


def encode_transport_payload_bytes(
    value: Any,
    *,
    mode: str = "",
    context: str = "payload",
    limit_bytes: int = 0,
) -> pb2.TransportPayload:
    normalized = resolve_effective_serialization_mode(
        request_mode=mode,
        context=context,
    )
    if normalized == "legacy_v1":
        payload = json.dumps(
            serialize_arrow_compatible(value),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    elif normalized == "pickle_stable_v1":
        payload = stable_pickle_dumps(value)
    elif normalized == INTERNAL_PICKLE_NATIVE_V1:
        payload = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    elif normalized == "structured_v1":
        payload = structured_dumps(value)
    else:
        raise ValueError(f"{normalized!r} does not use TransportPayload adapter payloads")
    if int(limit_bytes or 0) > 0:
        validate_inline_payload_size(
            len(payload),
            limit_bytes=max(1, int(limit_bytes)),
            context=context,
        )
    log_payload_flow("transport_payload_encode", context=context, codec=normalized, summary=summarize_payload_flow_value(value))
    return pb2.TransportPayload(
        codec=normalized,
        version=TRANSPORT_PAYLOAD_VERSION,
        payload=payload,
    )


def decode_transport_payload_bytes(
    codec: str,
    version: int,
    payload: bytes,
    *,
    context: str = "payload",
    trust_mode: str = "",
    limit_bytes: int = 0,
) -> Any:
    normalized, raw, _size = validate_transport_payload_bytes(
        codec,
        version,
        payload,
        context=context,
        trust_mode=trust_mode,
        limit_bytes=limit_bytes,
    )
    if normalized == "legacy_v1":
        decoded = convert_dict_to_arrow(json.loads(raw.decode("utf-8") if raw else "null"))
    elif normalized == INTERNAL_PICKLE_NATIVE_V1:
        decoded = pickle.loads(raw)
    elif normalized == "pickle_stable_v1":
        decoded = stable_pickle_loads(raw)
    elif normalized == "structured_v1":
        decoded = structured_loads(raw)
    else:
        raise ValueError(f"{normalized!r} is not supported by the TransportPayload adapter")
    log_payload_flow("transport_payload_decode", context=context, codec=normalized, summary=summarize_payload_flow_value(decoded))
    return decoded


def decode_transport_value(
    value: Any,
    *,
    mode: str = "",
    context: str = "payload",
    trust_mode: str = "",
) -> Any:
    normalized = resolve_received_transport_mode(
        default_mode=mode,
        context=context,
        trust_mode=trust_mode,
    )
    if isinstance(value, dict) and TRANSPORT_ENVELOPE_SENTINEL in value:
        envelope = dict(value.get(TRANSPORT_ENVELOPE_SENTINEL) or {})
        declared_codec = str(envelope.get("codec", "") or "").strip().lower()
        if not declared_codec:
            raise ValueError("transport envelope is missing codec")
        codec = resolve_received_transport_mode(
            declared_mode=declared_codec,
            default_mode=mode,
            context=context,
            trust_mode=trust_mode,
        )
        if codec == "legacy_v1":
            decoded = convert_dict_to_arrow(envelope.get("payload"))
        elif codec == "structured_v1":
            decoded = structured_loads(json.dumps(envelope.get("payload"), ensure_ascii=False).encode("utf-8"))
        elif codec in PICKLE_RAW_BYTES_MODES:
            raw = _restore_blob_from_json_transport(envelope.get("payload"))
            decoded = stable_pickle_loads(raw) if codec == "pickle_stable_v1" else pickle.loads(raw)
        else:
            raise ValueError(f"unsupported transport codec: {codec!r}")
        log_payload_flow("transport_decode", context=context, codec=codec, summary=summarize_payload_flow_value(decoded))
        return decoded
    if normalized != "legacy_v1":
        raise ValueError(f"{context} missing transport envelope for serialization mode {normalized!r}")
    decoded = convert_dict_to_arrow(value)
    log_payload_flow("transport_decode", context=context, codec="legacy_v1", summary=summarize_payload_flow_value(decoded))
    return decoded


def validate_inline_payload_structs(
    payloads: Sequence[struct_pb2.Struct],
    *,
    item_context: str = "payload",
    item_limit_bytes: int = 0,
    request_context: str = "payload request",
    total_limit_bytes: int = 0,
) -> int:
    total_size = 0
    total_count = len(payloads)
    for index, payload in enumerate(payloads):
        context = item_context if total_count == 1 else f"{item_context}[{index}]"
        size_bytes = validate_inline_payload_struct(
            payload,
            limit_bytes=item_limit_bytes,
            context=context,
        )
        total_size += size_bytes
    return validate_inline_request_size(
        total_size,
        limit_bytes=total_limit_bytes,
        context=request_context,
    )


def _coerce_payload_bytes(payload: bytes) -> bytes:
    return payload if isinstance(payload, bytes) else bytes(payload or b"")


def _transport_payload_checksum(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(_coerce_payload_bytes(payload)).hexdigest()


def _maybe_transport_payload_checksum(payload: bytes) -> str:
    return _transport_payload_checksum(payload) if get_inline_transport_checksum() else ""


def validate_transport_payload_bytes(
    codec: str,
    version: int,
    payload: bytes,
    *,
    context: str = "payload",
    trust_mode: str = "",
    limit_bytes: int = 0,
) -> tuple[str, bytes, int]:
    declared_codec = str(codec or "").strip().lower()
    if declared_codec == INTERNAL_PICKLE_NATIVE_V1:
        normalized_context = str(context or "").strip().lower()
        if normalized_context not in _INTERNAL_PICKLE_NATIVE_CONTEXTS:
            raise ValueError(
                f"{INTERNAL_PICKLE_NATIVE_V1} is only allowed on trusted internal transport; "
                f"context={normalized_context or 'unknown'}"
            )
        normalized = INTERNAL_PICKLE_NATIVE_V1
    else:
        normalized = resolve_received_transport_mode(
            declared_mode=codec,
            default_mode="legacy_v1",
            context=context,
            trust_mode=trust_mode,
        )
    if int(version or 0) != TRANSPORT_PAYLOAD_VERSION:
        raise ValueError(f"unsupported transport payload version: {version!r}")
    raw = _coerce_payload_bytes(payload)
    if int(limit_bytes or 0) > 0:
        size = validate_inline_payload_size(
            len(raw),
            limit_bytes=max(1, int(limit_bytes)),
            context=context,
        )
    else:
        size = len(raw)
    return normalized, raw, size


def make_inline_transport_carrier(
    *,
    codec: str,
    version: int,
    payload: bytes,
    payload_mode: str = "",
    context: str = "payload",
    trust_mode: str = "",
    limit_bytes: int = 0,
) -> dict[str, Any]:
    normalized, raw, size = validate_transport_payload_bytes(
        codec,
        version,
        payload,
        context=context,
        trust_mode=trust_mode,
        limit_bytes=limit_bytes,
    )
    return make_validated_inline_transport_carrier(
        codec=normalized,
        payload=raw,
        content_size=size,
        payload_mode=payload_mode,
        context=context,
    )


def make_validated_inline_transport_carrier(
    *,
    codec: str,
    payload: bytes,
    content_size: int,
    payload_mode: str = "",
    context: str = "payload",
) -> dict[str, Any]:
    return {
        INLINE_TRANSPORT_CARRIER_SENTINEL: {
            "carrier": "inline_bytes",
            "codec": str(codec or "").strip().lower(),
            "version": TRANSPORT_PAYLOAD_VERSION,
            "payload_mode": str(payload_mode or context or ""),
            "content_size": int(content_size or 0),
            "checksum": _maybe_transport_payload_checksum(payload),
            "content_bytes": payload,
        }
    }


def is_inline_transport_carrier(value: Any) -> bool:
    return isinstance(value, dict) and INLINE_TRANSPORT_CARRIER_SENTINEL in value


def _inline_transport_carrier_meta(value: Any) -> dict[str, Any]:
    if not is_inline_transport_carrier(value):
        raise TypeError("payload is not an inline transport carrier")
    meta = value.get(INLINE_TRANSPORT_CARRIER_SENTINEL)
    if not isinstance(meta, dict):
        raise ValueError("inline transport carrier metadata must be a dict")
    if str(meta.get("carrier", "") or "").strip().lower() != "inline_bytes":
        raise ValueError("unsupported inline transport carrier")
    return meta


def validate_inline_transport_carrier(
    value: Any,
    *,
    context: str = "payload",
    trust_mode: str = "",
    limit_bytes: int = 0,
) -> tuple[str, int, bytes]:
    meta = _inline_transport_carrier_meta(value)
    payload = meta.get("content_bytes", b"")
    raw = _coerce_payload_bytes(payload)
    normalized, raw, size = validate_transport_payload_bytes(
        str(meta.get("codec", "") or ""),
        int(meta.get("version", 0) or 0),
        raw,
        context=context,
        trust_mode=trust_mode,
        limit_bytes=limit_bytes,
    )
    declared_size = int(meta.get("content_size", size) or 0)
    if declared_size != size:
        raise ValueError(f"inline transport content_size mismatch: declared={declared_size} actual={size}")
    checksum = str(meta.get("checksum", "") or "").strip().lower()
    if checksum and checksum != _transport_payload_checksum(raw):
        raise ValueError("inline transport checksum mismatch")
    return normalized, TRANSPORT_PAYLOAD_VERSION, raw


def transport_payload_to_inline_carrier(
    transport: pb2.TransportPayload,
    *,
    payload_mode: str = "",
    context: str = "payload",
    trust_mode: str = "",
    limit_bytes: int = 0,
) -> dict[str, Any]:
    return make_inline_transport_carrier(
        codec=str(transport.codec or ""),
        version=int(transport.version or 0),
        payload=_coerce_payload_bytes(transport.payload),
        payload_mode=payload_mode,
        context=context,
        trust_mode=trust_mode,
        limit_bytes=limit_bytes,
    )


def inline_carrier_to_transport_payload(
    value: Any,
    *,
    context: str = "payload",
    trust_mode: str = "",
    limit_bytes: int = 0,
) -> pb2.TransportPayload:
    codec, version, raw = validate_inline_transport_carrier(
        value,
        context=context,
        trust_mode=trust_mode,
        limit_bytes=limit_bytes,
    )
    return pb2.TransportPayload(codec=codec, version=version, payload=raw)


def decode_inline_transport_carrier(
    value: Any,
    *,
    context: str = "payload",
    trust_mode: str = "",
    limit_bytes: int = 0,
) -> Any:
    codec, version, raw = validate_inline_transport_carrier(
        value,
        context=context,
        trust_mode=trust_mode,
        limit_bytes=limit_bytes,
    )
    return decode_transport_payload_bytes(
        codec,
        version,
        raw,
        context=context,
        trust_mode=trust_mode,
    )


def value_to_transport_payload(
    value: Any,
    *,
    mode: str = "",
    context: str = "payload",
    carrier_context: str = "",
    limit_bytes: int = 0,
    reject_transport_envelope: bool = False,
) -> pb2.TransportPayload:
    if is_inline_transport_carrier(value):
        return inline_carrier_to_transport_payload(
            value,
            context=carrier_context or context,
            limit_bytes=limit_bytes,
        )
    if reject_transport_envelope and isinstance(value, dict) and TRANSPORT_ENVELOPE_SENTINEL in value:
        raise RuntimeError("transport bytes lane received already-encoded result")
    return encode_transport_payload_bytes(
        value,
        mode=mode,
        context=context,
        limit_bytes=limit_bytes,
    )


def serialize_inline_payload(
    data: Optional[dict],
    *,
    context: str = "payload",
    limit_bytes: int = 0,
    mode: str = "",
) -> tuple[dict, struct_pb2.Struct, int]:
    normalized_data = {} if data is None else data
    transport_value = (
        data
        if (isinstance(data, dict) and TRANSPORT_ENVELOPE_SENTINEL in data)
        else encode_transport_value(normalized_data, mode=mode, context=context)
    )
    serialized = serialize_arrow_compatible(transport_value)
    out = struct_pb2.Struct()
    out.update(serialized)
    size_bytes = validate_inline_payload_struct(out, limit_bytes=limit_bytes, context=context)
    log_payload_flow(
        "inline_payload_encode",
        context=context,
        size_bytes=size_bytes,
        summary=summarize_payload_flow_value(normalized_data),
    )
    return serialized, out, size_bytes


def serialize_arrow_compatible(obj: Any) -> Any:
    """Recursively convert Arrow-compatible objects into JSON/Struct-safe data."""
    return _serialize_arrow_compatible(obj, path="payload", depth=0)


def _child_path(path: str, key: Any) -> str:
    if isinstance(key, int):
        return f"{path}[{key}]"
    return f"{path}.{key}"


def _normalize_mapping_key(key: Any, *, path: str, existing_keys: set[str]) -> str:
    if isinstance(key, str):
        normalized = key
    elif key is None or isinstance(key, (int, float, bool)):
        normalized = str(key)
    else:
        raise TypeError(
            f"{path} contains dict key {key!r} of type {type(key).__name__}; "
            "only string keys or scalar keys convertible to strings are supported"
        )

    if normalized in existing_keys:
        raise TypeError(
            f"{path} contains multiple dict keys that normalize to {normalized!r}; "
            "please use unique string keys"
        )
    return normalized


def _serialize_pandas_label(value: Any, *, path: str) -> Any:
    if value is None:
        return {"__type__": "pd.label", "kind": "none"}
    if isinstance(value, str):
        return {"__type__": "pd.label", "kind": "str", "value": value}
    if isinstance(value, bool):
        return {"__type__": "pd.label", "kind": "bool", "value": value}
    if isinstance(value, int):
        return {"__type__": "pd.label", "kind": "int", "value": str(value)}
    if isinstance(value, float):
        return {"__type__": "pd.label", "kind": "float", "value": repr(value)}
    if isinstance(value, tuple):
        return {
            "__type__": "pd.label",
            "kind": "tuple",
            "items": [_serialize_pandas_label(item, path=_child_path(path, idx)) for idx, item in enumerate(value)],
        }

    serialized = _serialize_arrow_compatible(value, path=path, depth=0)
    return {"__type__": "pd.label", "kind": "object", "value": serialized}


def _deserialize_pandas_label(value: Any) -> Any:
    if not isinstance(value, dict) or value.get("__type__") != "pd.label":
        return convert_dict_to_arrow(value)

    kind = str(value.get("kind", "") or "").strip().lower()
    if kind == "none":
        return None
    if kind == "str":
        return str(value.get("value", ""))
    if kind == "bool":
        return bool(value.get("value"))
    if kind == "int":
        return int(str(value.get("value", "0")))
    if kind == "float":
        return float(str(value.get("value", "0.0")))
    if kind == "tuple":
        return tuple(_deserialize_pandas_label(item) for item in value.get("items", []))
    if kind == "object":
        return convert_dict_to_arrow(value.get("value"))
    raise TypeError(f"unsupported pandas label kind: {kind!r}")


def _serialize_pandas_index(index: Any, *, path: str) -> dict[str, Any]:
    import pandas as pd

    if isinstance(index, pd.MultiIndex):
        tuples = [list(item) for item in index.tolist()]
        return {
            "kind": "multi",
            "values": [
                [_serialize_pandas_label(item, path=_child_path(f"{path}.values[{row_idx}]", col_idx)) for col_idx, item in enumerate(row)]
                for row_idx, row in enumerate(tuples)
            ],
            "names": [_serialize_pandas_label(name, path=_child_path(f"{path}.names", idx)) for idx, name in enumerate(index.names)],
        }
    if isinstance(index, pd.RangeIndex):
        return {
            "kind": "range",
            "start": int(index.start),
            "stop": int(index.stop),
            "step": int(index.step),
            "name": _serialize_pandas_label(index.name, path=f"{path}.name"),
        }
    return {
        "kind": "index",
        "values": [_serialize_pandas_label(item, path=_child_path(f"{path}.values", idx)) for idx, item in enumerate(index.tolist())],
        "name": _serialize_pandas_label(index.name, path=f"{path}.name"),
    }


def _deserialize_pandas_index(spec: Any):
    import pandas as pd

    if not isinstance(spec, dict):
        return pd.Index(convert_dict_to_arrow(spec))

    kind = str(spec.get("kind", "index") or "index").strip().lower()
    if kind == "multi":
        values = [[_deserialize_pandas_label(item) for item in row] for row in spec.get("values", [])]
        names = [_deserialize_pandas_label(item) for item in spec.get("names", [])]
        tuples = [tuple(item) for item in values]
        return pd.MultiIndex.from_tuples(tuples, names=list(names) if names is not None else None)
    if kind == "range":
        return pd.RangeIndex(
            start=int(spec.get("start", 0) or 0),
            stop=int(spec.get("stop", 0) or 0),
            step=int(spec.get("step", 1)),
            name=_deserialize_pandas_label(spec.get("name")),
        )
    return pd.Index(
        [_deserialize_pandas_label(item) for item in spec.get("values", [])],
        name=_deserialize_pandas_label(spec.get("name")),
    )


def serialize_dataframe_bundle(frame: Any) -> dict[str, Any]:
    import pandas as pd

    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"serialize_dataframe_bundle expects DataFrame, got {type(frame).__name__}")
    return {
        "version": 1,
        "index": _serialize_pandas_index(frame.index, path="payload.index"),
        "columns": _serialize_pandas_index(frame.columns, path="payload.columns"),
    }


def dataframe_bundle_parquet_frame(frame: Any):
    import pandas as pd

    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"dataframe_bundle_parquet_frame expects DataFrame, got {type(frame).__name__}")
    safe = frame.copy(deep=False)
    safe.columns = [f"c{idx}" for idx in range(len(frame.columns))]
    return safe


def deserialize_dataframe_bundle(meta: Any, frame: Any):
    import pandas as pd

    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"deserialize_dataframe_bundle expects DataFrame, got {type(frame).__name__}")
    if not isinstance(meta, dict):
        raise TypeError("dataframe bundle meta must be dict")
    version = int(meta.get("version", 0) or 0)
    if version != 1:
        raise ValueError(f"unsupported dataframe bundle meta version: {version}")
    frame.index = _deserialize_pandas_index(meta.get("index"))
    frame.columns = _deserialize_pandas_index(meta.get("columns"))
    return frame


def serialize_series_bundle(series: Any) -> dict[str, Any]:
    import pandas as pd

    if not isinstance(series, pd.Series):
        raise TypeError(f"serialize_series_bundle expects Series, got {type(series).__name__}")
    return {
        "version": 1,
        "index": _serialize_pandas_index(series.index, path="payload.index"),
        "name": _serialize_arrow_compatible(series.name, path="payload.name", depth=0),
    }


def deserialize_series_bundle(meta: Any, series: Any):
    import pandas as pd

    if not isinstance(series, pd.Series):
        raise TypeError(f"deserialize_series_bundle expects Series, got {type(series).__name__}")
    if not isinstance(meta, dict):
        raise TypeError("series bundle meta must be dict")
    version = int(meta.get("version", 0) or 0)
    if version != 1:
        raise ValueError(f"unsupported series bundle meta version: {version}")
    series.index = _deserialize_pandas_index(meta.get("index"))
    series.name = convert_dict_to_arrow(meta.get("name"))
    return series


def _serialize_arrow_compatible(obj: Any, *, path: str, depth: int) -> Any:
    """Internal recursive serializer with better error locations."""
    if depth > MAX_ARROW_RECURSION_DEPTH:
        raise RecursionError(f"{path} exceeds max serialization depth {MAX_ARROW_RECURSION_DEPTH}")
    if obj is None or isinstance(obj, (str, float, bool)):
        return obj
    if isinstance(obj, int):
        if abs(obj) > 2**53:
            return {"__type__": "int64", "value": str(obj)}
        return obj
    if isinstance(obj, datetime):
        return {"__type__": "datetime", "value": obj.isoformat()}
    if isinstance(obj, date):
        return {"__type__": "date", "value": obj.isoformat()}
    if isinstance(obj, time):
        return {"__type__": "time", "value": obj.isoformat()}
    if isinstance(obj, timedelta):
        return {"__type__": "timedelta", "seconds": obj.total_seconds()}
    if maybe_data_ref(obj) is not None:
        return data_ref_to_payload(coerce_data_ref(obj))
    if isinstance(obj, dict):
        out = {}
        seen_keys: set[str] = set()
        for key, value in obj.items():
            normalized_key = _normalize_mapping_key(key, path=path, existing_keys=seen_keys)
            seen_keys.add(normalized_key)
            out[normalized_key] = _serialize_arrow_compatible(value, path=_child_path(path, key), depth=depth + 1)
        return out
    if isinstance(obj, (list, tuple)):
        return [_serialize_arrow_compatible(item, path=_child_path(path, idx), depth=depth + 1) for idx, item in enumerate(obj)]

    try:
        import numpy as np

        if isinstance(obj, (np.integer, np.floating, np.bool_)):
            return obj.item()
    except ImportError:
        pass

    try:
        import pandas as pd

        if isinstance(obj, pd.Timestamp):
            return {"__type__": "pd.Timestamp", "value": obj.isoformat()}
        if isinstance(obj, pd.Timedelta):
            return {"__type__": "pd.Timedelta", "value": obj.isoformat()}
        if isinstance(obj, pd.DataFrame):
            rows = [list(row) for row in obj.itertuples(index=False, name=None)]
            return {
                "__type__": "DataFrame",
                "data": _serialize_arrow_compatible(rows, path=f"{path}.data", depth=depth + 1),
                "index": _serialize_pandas_index(obj.index, path=f"{path}.index"),
                "columns": _serialize_pandas_index(obj.columns, path=f"{path}.columns"),
                "column_dtypes": [str(dtype) for dtype in obj.dtypes],
            }
        if isinstance(obj, pd.Series):
            return {
                "__type__": "Series",
                "data": _serialize_arrow_compatible(obj.tolist(), path=f"{path}.data", depth=depth + 1),
                "index": _serialize_pandas_index(obj.index, path=f"{path}.index"),
                "name": _serialize_arrow_compatible(obj.name, path=f"{path}.name", depth=depth + 1),
            }
    except ImportError:
        pass

    try:
        import numpy as np

        if isinstance(obj, np.ndarray):
            if obj.dtype.kind not in ("i", "u", "f", "b", "U", "S"):
                raise TypeError(
                    f"{path} uses numpy.ndarray dtype {obj.dtype}, "
                    "only simple numeric/bool/string dtypes are supported"
                )
            return {
                "__type__": "ndarray",
                "data": _serialize_arrow_compatible(obj.tolist(), path=f"{path}.data", depth=depth + 1),
                "dtype": str(obj.dtype),
            }
    except ImportError:
        pass

    raise TypeError(
        f"{path} has unsupported type {type(obj).__name__}; "
        "supported values are JSON scalars, list/tuple, dict, "
        "datetime/date/time/timedelta, pandas.DataFrame, pandas.Series, numpy.ndarray, and DataRef"
    )


def is_arrow_compatible(obj: Any) -> bool:
    """Check whether obj is one supported Arrow-compatible leaf type."""
    try:
        import pandas as pd

        if isinstance(obj, (pd.DataFrame, pd.Series)):
            return True
    except ImportError:
        pass

    try:
        import numpy as np

        if isinstance(obj, np.ndarray):
            return obj.dtype.kind in ("i", "u", "f", "b", "U", "S")
    except ImportError:
        pass

    return False


def convert_arrow_to_dict(obj: Any) -> dict:
    """Convert one Arrow-compatible leaf object into tagged dict form."""
    converted = serialize_arrow_compatible(obj)
    if not isinstance(converted, dict) or "__type__" not in converted:
        raise TypeError(f"Cannot convert {type(obj)} to dict: not Arrow compatible")
    return converted


def convert_dict_to_arrow(data: Any) -> Any:
    """Restore tagged dict/list payloads back into pandas/numpy objects."""
    return _convert_dict_to_arrow(data, depth=0)


def _convert_dict_to_arrow(data: Any, *, depth: int) -> Any:
    if depth > MAX_ARROW_RECURSION_DEPTH:
        raise RecursionError(f"payload exceeds max deserialization depth {MAX_ARROW_RECURSION_DEPTH}")
    if isinstance(data, dict):
        if is_data_ref_payload(data):
            return data_ref_from_payload(data)
        obj_type = data.get("__type__")
        if obj_type == "datetime":
            return datetime.fromisoformat(str(data["value"]))
        if obj_type == "date":
            return date.fromisoformat(str(data["value"]))
        if obj_type == "time":
            return time.fromisoformat(str(data["value"]))
        if obj_type == "timedelta":
            return timedelta(seconds=float(data["seconds"]))
        if obj_type == "int64":
            return int(str(data.get("value", "0")))
        if obj_type == "pd.Timestamp":
            try:
                import pandas as pd

                return pd.Timestamp(data["value"])
            except ImportError as exc:
                raise RuntimeError("pandas not available, cannot deserialize Timestamp") from exc
        if obj_type == "pd.Timedelta":
            try:
                import pandas as pd

                return pd.Timedelta(data["value"])
            except ImportError as exc:
                raise RuntimeError("pandas not available, cannot deserialize Timedelta") from exc
        if obj_type == "DataFrame":
            try:
                import pandas as pd

                frame = pd.DataFrame(_convert_dict_to_arrow(data["data"], depth=depth + 1))
                frame.index = _deserialize_pandas_index(data.get("index"))
                frame.columns = _deserialize_pandas_index(data.get("columns"))
                dtypes = data.get("column_dtypes")
                if isinstance(dtypes, list) and len(dtypes) == len(frame.columns):
                    for col, dtype in zip(frame.columns, dtypes):
                        try:
                            frame[col] = frame[col].astype(dtype)
                        except Exception:
                            continue
                return frame
            except ImportError as exc:
                raise RuntimeError("pandas not available, cannot deserialize DataFrame") from exc
        if obj_type == "Series":
            try:
                import pandas as pd

                return pd.Series(
                    _convert_dict_to_arrow(data["data"], depth=depth + 1),
                    index=_deserialize_pandas_index(data.get("index")),
                    name=_convert_dict_to_arrow(data.get("name"), depth=depth + 1),
                )
            except ImportError as exc:
                raise RuntimeError("pandas not available, cannot deserialize Series") from exc
        if obj_type == "ndarray":
            try:
                import numpy as np

                return np.array(data["data"], dtype=data.get("dtype"))
            except ImportError as exc:
                raise RuntimeError("numpy not available, cannot deserialize ndarray") from exc
        if obj_type is not None:
            raise TypeError(f"unsupported payload __type__: {obj_type!r}")
        return {k: _convert_dict_to_arrow(v, depth=depth + 1) for k, v in data.items()}
    if isinstance(data, list):
        return [_convert_dict_to_arrow(item, depth=depth + 1) for item in data]
    return data


def dict_to_struct(data: Optional[dict], *, mode: str = "legacy_v1") -> struct_pb2.Struct:
    """Convert nested payload data into protobuf Struct."""
    out = struct_pb2.Struct()
    if data is not None:
        transport_value = data if (isinstance(data, dict) and TRANSPORT_ENVELOPE_SENTINEL in data) else encode_transport_value(data, mode=mode, context="protobuf payload")
        out.update(serialize_arrow_compatible(transport_value))
    return out


def _value_to_python(value: struct_pb2.Value) -> Any:
    kind = value.WhichOneof("kind")
    if kind == "null_value":
        return None
    if kind == "number_value":
        return float(value.number_value)
    if kind == "string_value":
        return str(value.string_value)
    if kind == "bool_value":
        return bool(value.bool_value)
    if kind == "struct_value":
        return {key: _value_to_python(item) for key, item in value.struct_value.fields.items()}
    if kind == "list_value":
        return [_value_to_python(item) for item in value.list_value.values]
    return None


def struct_to_python(data: struct_pb2.Struct) -> dict:
    """Convert protobuf Struct into nested Python objects without carrier decode."""
    return {key: _value_to_python(item) for key, item in data.fields.items()}


def struct_to_dict(data: struct_pb2.Struct, *, mode: str = "legacy_v1") -> dict:
    """Convert protobuf Struct into nested Python objects and decode carrier envelopes."""
    result = struct_to_python(data)
    decoded = decode_transport_value(result, mode=mode, context="protobuf payload")
    if isinstance(decoded, dict):
        return decoded
    return {"value": decoded}
