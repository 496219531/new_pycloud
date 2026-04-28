from __future__ import annotations

"""Object-codec layer for ``pickle_stable_v1``.

This module is intentionally transport-agnostic.

It defines stable object schemas for pandas / numpy containers and returns raw
Python objects plus raw ``bytes`` where appropriate. In particular, ndarray
payloads retain raw bytes in the schema itself:

```
{"__codec__": "np.ndarray.v1", ..., "data": b"..."}
```

Whether those bytes can travel through JSON / Struct / protobuf is a transport
container concern handled by the caller. This module does not pre-encode raw
bytes as base64 just to satisfy a future container.

Backward compatibility:
- decoding still accepts legacy ``data_b64`` ndarray payloads
- encoding only emits raw ``data`` bytes
"""

import base64  # backward compatibility for legacy data_b64 ndarray payloads
import pickle
from typing import Any


def _lazy_serialization_helpers():
    from pycloud_parallel.controlplane.serialization import (
        _deserialize_pandas_index,
        _deserialize_pandas_label,
        _serialize_pandas_index,
        _serialize_pandas_label,
    )

    return (
        _serialize_pandas_index,
        _deserialize_pandas_index,
        _serialize_pandas_label,
        _deserialize_pandas_label,
    )


def _is_supported_ndarray_dtype(arr: Any) -> bool:
    try:
        kind = str(arr.dtype.kind)
    except Exception:
        return False
    return kind != "O"


def _encode_ndarray_v1(arr):
    import numpy as np

    array = np.asarray(arr)
    if not _is_supported_ndarray_dtype(array):
        raise TypeError("pickle_stable_v1 does not support ndarray dtype=object")
    order = "F" if array.flags.f_contiguous and not array.flags.c_contiguous else "C"
    contiguous = np.asfortranarray(array) if order == "F" else np.ascontiguousarray(array)
    return {
        "__codec__": "np.ndarray.v1",
        "dtype": str(contiguous.dtype),
        "shape": [int(dim) for dim in contiguous.shape],
        "order": order,
        "data": contiguous.tobytes(order=order),
    }


def _decode_ndarray_v1(payload):
    import numpy as np

    dtype = np.dtype(str(payload.get("dtype", "") or "float64"))
    if dtype.kind == "O":
        raise TypeError("pickle_stable_v1 does not support ndarray dtype=object")
    shape = tuple(int(dim) for dim in list(payload.get("shape") or ()))
    order = str(payload.get("order", "C") or "C").strip().upper() or "C"
    if "data" in payload:
        raw_data = payload.get("data")
        if isinstance(raw_data, memoryview):
            blob = raw_data.tobytes()
        elif isinstance(raw_data, (bytes, bytearray)):
            blob = bytes(raw_data)
        else:
            raise TypeError("np.ndarray.v1 data must be bytes-like")
    elif "data_b64" in payload:
        blob = base64.b64decode(str(payload.get("data_b64", "") or "").encode("ascii"))
    else:
        raise ValueError("np.ndarray.v1 payload must include data or data_b64")
    array = np.frombuffer(blob, dtype=dtype)
    if shape:
        array = array.reshape(shape, order=order)
    return array.copy()


def _encode_pandas_index_v1(index, *, path: str):
    import numpy as np
    import pandas as pd

    (
        _serialize_pandas_index,
        _deserialize_pandas_index,
        _serialize_pandas_label,
        _deserialize_pandas_label,
    ) = _lazy_serialization_helpers()
    del _deserialize_pandas_index, _deserialize_pandas_label
    if isinstance(index, pd.DatetimeIndex):
        return {
            "__codec__": "pd.index.datetime64.v1",
            "values": _encode_ndarray_v1(np.asarray(index.asi8, dtype=np.int64)),
            "tz": str(index.tz or ""),
            "name": _serialize_pandas_label(index.name, path=f"{path}.name"),
        }
    values = np.asarray(index.to_numpy(copy=False))
    if _is_supported_ndarray_dtype(values):
        return {
            "__codec__": "pd.index.ndarray.v1",
            "values": _encode_ndarray_v1(values),
            "name": _serialize_pandas_label(index.name, path=f"{path}.name"),
        }
    return _serialize_pandas_index(index, path=path)


def _decode_pandas_index_v1(payload):
    import pandas as pd

    (
        _serialize_pandas_index,
        _deserialize_pandas_index,
        _serialize_pandas_label,
        _deserialize_pandas_label,
    ) = _lazy_serialization_helpers()
    del _serialize_pandas_index, _serialize_pandas_label
    if isinstance(payload, dict):
        codec = str(payload.get("__codec__", "") or "").strip()
        if codec == "pd.index.datetime64.v1":
            values = _decode_ndarray_v1(dict(payload.get("values") or {}))
            tz = str(payload.get("tz", "") or "").strip()
            name = _deserialize_pandas_label(payload.get("name"))
            if tz:
                index = pd.to_datetime(values, unit="ns", utc=True).tz_convert(tz)
            else:
                index = pd.to_datetime(values, unit="ns")
            return pd.DatetimeIndex(index, name=name)
        if codec == "pd.index.ndarray.v1":
            values = _decode_ndarray_v1(dict(payload.get("values") or {}))
            name = _deserialize_pandas_label(payload.get("name"))
            return pd.Index(values, name=name)
    return _deserialize_pandas_index(payload)


def _encode_series_v1(series):
    import numpy as np
    import pandas as pd

    if not isinstance(series, pd.Series):
        raise TypeError(f"_encode_series_v1 expects Series, got {type(series).__name__}")
    values = np.asarray(series.to_numpy(copy=False))
    if not _is_supported_ndarray_dtype(values):
        raise TypeError("pickle_stable_v1 does not support Series with dtype=object")
    (_, _, _serialize_pandas_label, _) = _lazy_serialization_helpers()
    return {
        "__codec__": "pd.series.v1",
        "index": _encode_pandas_index_v1(series.index, path="pickle_stable_v1.series.index"),
        "name": _serialize_pandas_label(series.name, path="pickle_stable_v1.series.name"),
        "dtype": str(series.dtype),
        "values": _encode_ndarray_v1(values),
    }


def _decode_series_v1(payload):
    import pandas as pd

    (_, _, _, _deserialize_pandas_label) = _lazy_serialization_helpers()
    values = _decode_ndarray_v1(dict(payload.get("values") or {}))
    index = _decode_pandas_index_v1(payload.get("index"))
    name = _deserialize_pandas_label(payload.get("name"))
    return pd.Series(values, index=index, name=name)


def _encode_dataframe_v1(df):
    import numpy as np
    import pandas as pd

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"_encode_dataframe_v1 expects DataFrame, got {type(df).__name__}")
    (_, _, _serialize_pandas_label, _) = _lazy_serialization_helpers()
    columns = []
    for column in list(df.columns):
        series = df[column]
        values = np.asarray(series.to_numpy(copy=False))
        if not _is_supported_ndarray_dtype(values):
            raise TypeError(
                f"pickle_stable_v1 does not support DataFrame object dtype column: {column!r}"
            )
        columns.append(
            {
                "name": _serialize_pandas_label(column, path="pickle_stable_v1.dataframe.columns"),
                "dtype": str(series.dtype),
                "values": _encode_ndarray_v1(values),
            }
        )
    return {
        "__codec__": "pd.dataframe.v1",
        "index": _encode_pandas_index_v1(df.index, path="pickle_stable_v1.dataframe.index"),
        "columns": _encode_pandas_index_v1(df.columns, path="pickle_stable_v1.dataframe.columns"),
        "column_dtypes": [str(dtype) for dtype in df.dtypes],
        "data": columns,
    }


def _decode_dataframe_v1(payload):
    import pandas as pd

    (_, _, _, _deserialize_pandas_label) = _lazy_serialization_helpers()
    rows = {}
    for item in list(payload.get("data") or ()):
        normalized = dict(item or {})
        name = _deserialize_pandas_label(normalized.get("name"))
        rows[name] = _decode_ndarray_v1(dict(normalized.get("values") or {}))
    frame = pd.DataFrame(rows)
    frame.index = _decode_pandas_index_v1(payload.get("index"))
    frame.columns = _decode_pandas_index_v1(payload.get("columns"))
    return frame


def normalize_for_pickle_stable(obj: Any) -> Any:
    try:
        import numpy as np
        import pandas as pd
    except Exception:  # pragma: no cover - import errors simply fall through
        np = None
        pd = None

    if pd is not None:
        if isinstance(obj, pd.DataFrame):
            return _encode_dataframe_v1(obj)
        if isinstance(obj, pd.Series):
            return _encode_series_v1(obj)
        if isinstance(obj, pd.Index):
            return {
                "__codec__": "pd.index.v1",
                "payload": _encode_pandas_index_v1(obj, path="pickle_stable_v1.index"),
            }
    if np is not None and isinstance(obj, np.ndarray):
        return _encode_ndarray_v1(obj)
    if isinstance(obj, dict):
        return {key: normalize_for_pickle_stable(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [normalize_for_pickle_stable(value) for value in obj]
    if isinstance(obj, tuple):
        return tuple(normalize_for_pickle_stable(value) for value in obj)
    return obj


def restore_from_pickle_stable(obj: Any) -> Any:
    if isinstance(obj, dict):
        codec = str(obj.get("__codec__", "") or "").strip()
        if codec == "np.ndarray.v1":
            return _decode_ndarray_v1(obj)
        if codec == "pd.series.v1":
            return _decode_series_v1(obj)
        if codec == "pd.dataframe.v1":
            return _decode_dataframe_v1(obj)
        if codec == "pd.index.v1":
            return _decode_pandas_index_v1(obj.get("payload"))
        return {key: restore_from_pickle_stable(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [restore_from_pickle_stable(value) for value in obj]
    if isinstance(obj, tuple):
        return tuple(restore_from_pickle_stable(value) for value in obj)
    return obj


def stable_pickle_dumps(obj: Any) -> bytes:
    """Serialize an object-codec payload as raw pickle bytes.

    The returned bytes are transport-neutral. Container-specific adaptation
    such as base64-for-JSON belongs to the transport layer, not here.
    """
    normalized = normalize_for_pickle_stable(obj)
    return pickle.dumps(normalized, protocol=pickle.HIGHEST_PROTOCOL)


def stable_pickle_loads(blob: bytes | bytearray | memoryview) -> Any:
    raw = blob if isinstance(blob, bytes) else bytes(blob)
    loaded = pickle.loads(raw)
    return restore_from_pickle_stable(loaded)


__all__ = [
    "_decode_dataframe_v1",
    "_decode_ndarray_v1",
    "_decode_series_v1",
    "_encode_dataframe_v1",
    "_encode_ndarray_v1",
    "_encode_series_v1",
    "normalize_for_pickle_stable",
    "restore_from_pickle_stable",
    "stable_pickle_dumps",
    "stable_pickle_loads",
]
