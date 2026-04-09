from __future__ import annotations

"""Shared helpers for Arrow-compatible payload/result serialization."""

from datetime import date, datetime, time, timedelta
import logging
from typing import Any, Optional, Sequence

from google.protobuf import json_format
from google.protobuf import struct_pb2

from pycloud_parallel.controlplane.config import (
    INLINE_PAYLOAD_HARD_LIMIT_BYTES,
    INLINE_PAYLOAD_REQUEST_LIMIT_BYTES,
    INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    INLINE_RESULT_HARD_LIMIT_BYTES,
    INLINE_RESULT_SOFT_LIMIT_BYTES,
)
from pycloud_parallel.controlplane.object_ref import (
    ObjectRef,
    is_object_ref_payload,
    object_ref_from_payload,
    object_ref_to_payload,
)
from pycloud_parallel.controlplane.result_ref import (
    ResultRef,
    is_result_ref_payload,
    result_ref_from_payload,
    result_ref_to_payload,
)

payload_flow_logger = logging.getLogger("pycloud_parallel.payload_flow")
MAX_ARROW_RECURSION_DEPTH = 200


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
    if isinstance(value, ObjectRef):
        return (
            f"ObjectRef(format={value.format}, size_bytes={value.size_bytes}, "
            f"materialize_as={value.materialize_as})"
        )
    if isinstance(value, ResultRef):
        return (
            f"ResultRef(format={value.format}, size_bytes={value.size_bytes}, "
            f"materialize_as={value.materialize_as}, node_id={value.node_id})"
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
        "put_object_from_file()/put_object_from_bytes() and pass ObjectRef instead."
    )


def inline_payload_limit_error(size_bytes: int, *, limit_bytes: int, context: str) -> ValueError:
    return ValueError(
        f"{context} serialized to {_format_payload_bytes(size_bytes)}, "
        f"which exceeds the inline limit {_format_payload_bytes(limit_bytes)}. "
        f"{_inline_payload_limit_hint()}"
    )


def validate_inline_payload_size(size_bytes: int, *, limit_bytes: int = INLINE_PAYLOAD_HARD_LIMIT_BYTES, context: str = "payload") -> int:
    normalized = max(0, int(size_bytes or 0))
    if normalized > max(1, int(limit_bytes)):
        raise inline_payload_limit_error(normalized, limit_bytes=max(1, int(limit_bytes)), context=context)
    return normalized


INLINE_STRUCT_WIRE_OVERHEAD_RATIO = 0.1
INLINE_STRUCT_WIRE_OVERHEAD_MIN_BYTES = 512


def _struct_wire_size_with_overhead(data: struct_pb2.Struct) -> int:
    size = int(data.ByteSize())
    overhead = max(INLINE_STRUCT_WIRE_OVERHEAD_MIN_BYTES, int(size * INLINE_STRUCT_WIRE_OVERHEAD_RATIO))
    return size + overhead


def validate_inline_payload_struct(data: struct_pb2.Struct, *, limit_bytes: int = INLINE_PAYLOAD_HARD_LIMIT_BYTES, context: str = "payload") -> int:
    return validate_inline_payload_size(
        _struct_wire_size_with_overhead(data),
        limit_bytes=limit_bytes,
        context=context,
    )


def validate_inline_request_size(size_bytes: int, *, limit_bytes: int = INLINE_PAYLOAD_REQUEST_LIMIT_BYTES, context: str = "payload request") -> int:
    return validate_inline_payload_size(
        size_bytes,
        limit_bytes=limit_bytes,
        context=context,
    )


def inline_result_limit_error(size_bytes: int, *, limit_bytes: int, context: str) -> ValueError:
    return ValueError(
        f"{context} serialized to {_format_payload_bytes(size_bytes)}, "
        f"which exceeds the inline result limit {_format_payload_bytes(limit_bytes)}. "
        "For large results, write to a node-local file or stream chunks and return a lightweight handle instead."
    )


def validate_inline_result_size(size_bytes: int, *, limit_bytes: int = INLINE_RESULT_HARD_LIMIT_BYTES, context: str = "result") -> int:
    normalized = max(0, int(size_bytes or 0))
    if normalized > max(1, int(limit_bytes)):
        raise inline_result_limit_error(normalized, limit_bytes=max(1, int(limit_bytes)), context=context)
    return normalized


def validate_inline_result_struct(data: struct_pb2.Struct, *, limit_bytes: int = INLINE_RESULT_HARD_LIMIT_BYTES, context: str = "result") -> int:
    return validate_inline_result_size(
        _struct_wire_size_with_overhead(data),
        limit_bytes=limit_bytes,
        context=context,
    )


def serialize_inline_result(
    data: Optional[dict],
    *,
    context: str = "result",
    limit_bytes: int = INLINE_RESULT_HARD_LIMIT_BYTES,
) -> tuple[dict, struct_pb2.Struct, int]:
    serialized = serialize_arrow_compatible(data or {})
    out = struct_pb2.Struct()
    out.update(serialized)
    size_bytes = validate_inline_result_struct(out, limit_bytes=limit_bytes, context=context)
    log_payload_flow(
        "inline_result_encode",
        context=context,
        size_bytes=size_bytes,
        summary=summarize_payload_flow_value(data or {}),
    )
    return serialized, out, size_bytes


def validate_inline_payload_structs(
    payloads: Sequence[struct_pb2.Struct],
    *,
    item_context: str = "payload",
    item_limit_bytes: int = INLINE_PAYLOAD_HARD_LIMIT_BYTES,
    request_context: str = "payload request",
    request_limit_bytes: int = INLINE_PAYLOAD_REQUEST_LIMIT_BYTES,
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
        limit_bytes=request_limit_bytes,
        context=request_context,
    )


def serialize_inline_payload(
    data: Optional[dict],
    *,
    context: str = "payload",
    limit_bytes: int = INLINE_PAYLOAD_HARD_LIMIT_BYTES,
) -> tuple[dict, struct_pb2.Struct, int]:
    serialized = serialize_arrow_compatible(data or {})
    out = struct_pb2.Struct()
    out.update(serialized)
    size_bytes = validate_inline_payload_struct(out, limit_bytes=limit_bytes, context=context)
    log_payload_flow(
        "inline_payload_encode",
        context=context,
        size_bytes=size_bytes,
        summary=summarize_payload_flow_value(data or {}),
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
    if isinstance(obj, ObjectRef):
        return object_ref_to_payload(obj)
    if isinstance(obj, ResultRef):
        return result_ref_to_payload(obj)

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

    raise TypeError(
        f"{path} has unsupported type {type(obj).__name__}; "
        "supported values are JSON scalars, list/tuple, dict, "
        "datetime/date/time/timedelta, pandas.DataFrame, pandas.Series, numpy.ndarray, and ObjectRef"
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
        if is_object_ref_payload(data):
            return object_ref_from_payload(data)
        if is_result_ref_payload(data):
            return result_ref_from_payload(data)
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


def dict_to_struct(data: Optional[dict]) -> struct_pb2.Struct:
    """Convert nested payload data into protobuf Struct."""
    out = struct_pb2.Struct()
    if data is not None:
        out.update(serialize_arrow_compatible(data))
    return out


def struct_to_dict(data: struct_pb2.Struct) -> dict:
    """Convert protobuf Struct into nested Python objects."""
    result = json_format.MessageToDict(data, preserving_proto_field_name=True)
    return convert_dict_to_arrow(result)
