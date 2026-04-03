from __future__ import annotations

"""Shared helpers for Arrow-compatible payload/result serialization."""

from typing import Any, Optional

from google.protobuf import json_format
from google.protobuf import struct_pb2

from pycloud_parallel.controlplane.object_ref import (
    ObjectRef,
    is_object_ref_payload,
    object_ref_from_payload,
    object_ref_to_payload,
)


def serialize_arrow_compatible(obj: Any) -> Any:
    """Recursively convert Arrow-compatible objects into JSON/Struct-safe data."""
    return _serialize_arrow_compatible(obj, path="payload")


def _child_path(path: str, key: Any) -> str:
    if isinstance(key, int):
        return f"{path}[{key}]"
    return f"{path}.{key}"


def _serialize_arrow_compatible(obj: Any, *, path: str) -> Any:
    """Internal recursive serializer with better error locations."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, ObjectRef):
        return object_ref_to_payload(obj)

    try:
        import numpy as np

        if isinstance(obj, (np.integer, np.floating, np.bool_)):
            return obj.item()
    except ImportError:
        pass

    try:
        import pandas as pd

        if isinstance(obj, pd.DataFrame):
            return {
                "__type__": "DataFrame",
                "data": _serialize_arrow_compatible(obj.to_dict(orient="records"), path=f"{path}.data"),
            }
        if isinstance(obj, pd.Series):
            return {
                "__type__": "Series",
                "data": _serialize_arrow_compatible(obj.tolist(), path=f"{path}.data"),
                "index": _serialize_arrow_compatible(list(obj.index), path=f"{path}.index"),
                "name": obj.name,
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
                "data": _serialize_arrow_compatible(obj.tolist(), path=f"{path}.data"),
                "dtype": str(obj.dtype),
            }
    except ImportError:
        pass

    if isinstance(obj, dict):
        out = {}
        for key, value in obj.items():
            if not isinstance(key, str):
                raise TypeError(
                    f"{path} contains dict key {key!r} of type {type(key).__name__}; "
                    "only string keys are supported"
                )
            out[key] = _serialize_arrow_compatible(value, path=_child_path(path, key))
        return out
    if isinstance(obj, (list, tuple)):
        return [_serialize_arrow_compatible(item, path=_child_path(path, idx)) for idx, item in enumerate(obj)]

    raise TypeError(
        f"{path} has unsupported type {type(obj).__name__}; "
        "supported values are JSON scalars, list/tuple, dict, "
        "pandas.DataFrame, pandas.Series, numpy.ndarray, and ObjectRef"
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
    if isinstance(data, dict):
        if is_object_ref_payload(data):
            return object_ref_from_payload(data)
        obj_type = data.get("__type__")
        if obj_type == "DataFrame":
            try:
                import pandas as pd

                return pd.DataFrame(data["data"])
            except ImportError as exc:
                raise RuntimeError("pandas not available, cannot deserialize DataFrame") from exc
        if obj_type == "Series":
            try:
                import pandas as pd

                return pd.Series(data["data"], index=data.get("index"), name=data.get("name"))
            except ImportError as exc:
                raise RuntimeError("pandas not available, cannot deserialize Series") from exc
        if obj_type == "ndarray":
            try:
                import numpy as np

                return np.array(data["data"], dtype=data.get("dtype"))
            except ImportError as exc:
                raise RuntimeError("numpy not available, cannot deserialize ndarray") from exc
        return {k: convert_dict_to_arrow(v) for k, v in data.items()}
    if isinstance(data, list):
        return [convert_dict_to_arrow(item) for item in data]
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
