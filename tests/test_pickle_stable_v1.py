from __future__ import annotations

import numpy as np
import pandas as pd

from pycloud_parallel.controlplane.pickle_stable_v1 import (
    normalize_for_pickle_stable,
    stable_pickle_dumps,
    stable_pickle_loads,
)


def test_pickle_stable_v1_roundtrips_dataframe_series_and_ndarray():
    frame = pd.DataFrame({"a": [1, 2], "b": [3.5, 4.5]}, index=pd.Index(["x", "y"], name="idx"))
    series = pd.Series([10, 11], index=pd.Index(["u", "v"], name="sidx"), name="score")
    array = np.array([[1, 2], [3, 4]], dtype=np.int64)

    payload = {
        "frame": frame,
        "series": series,
        "array": array,
    }

    restored = stable_pickle_loads(stable_pickle_dumps(payload))

    assert restored["frame"].equals(frame)
    assert restored["series"].equals(series)
    assert np.array_equal(restored["array"], array)


def test_pickle_stable_v1_ndarray_schema_uses_raw_bytes():
    array = np.array([[1, 2], [3, 4]], dtype=np.int64)

    normalized = normalize_for_pickle_stable(array)

    assert normalized["__codec__"] == "np.ndarray.v1"
    assert normalized["dtype"] == "int64"
    assert normalized["shape"] == [2, 2]
    assert normalized["order"] == "C"
    assert "data" in normalized
    assert isinstance(normalized["data"], bytes)
    assert "data_b64" not in normalized


def test_pickle_stable_v1_dataframe_series_and_index_keep_structural_schema():
    frame = pd.DataFrame(
        {"a": np.array([1, 2], dtype=np.int64), "b": np.array([3.5, 4.5], dtype=np.float64)},
        index=pd.Index(["x", "y"], name="row"),
    )
    series = pd.Series(np.array([10, 11], dtype=np.int64), index=pd.Index(["u", "v"], name="sid"), name="score")
    normalized = normalize_for_pickle_stable(
        {
            "frame": frame,
            "series": series,
            "index": frame.index,
        }
    )

    frame_schema = normalized["frame"]
    assert frame_schema["__codec__"] == "pd.dataframe.v1"
    assert "index" in frame_schema
    assert "columns" in frame_schema
    assert "column_dtypes" in frame_schema
    assert "data" in frame_schema
    assert isinstance(frame_schema["data"][0]["values"]["data"], bytes)
    assert "data_b64" not in frame_schema["data"][0]["values"]

    series_schema = normalized["series"]
    assert series_schema["__codec__"] == "pd.series.v1"
    assert "index" in series_schema
    assert "name" in series_schema
    assert "dtype" in series_schema
    assert isinstance(series_schema["values"]["data"], bytes)
    assert "data_b64" not in series_schema["values"]

    index_schema = normalized["index"]
    assert index_schema["__codec__"] == "pd.index.v1"
    assert "payload" in index_schema


def test_pickle_stable_v1_uses_compact_datetime_index_schema():
    index = pd.date_range("2024-01-01", periods=3, name="day")
    series = pd.Series(np.array([1.0, 2.0, 3.0]), index=index, name="nav")

    normalized = normalize_for_pickle_stable(series)
    normalized_index = normalize_for_pickle_stable(index)
    restored = stable_pickle_loads(stable_pickle_dumps(series))
    restored_index = stable_pickle_loads(stable_pickle_dumps(index))

    assert normalized["index"]["__codec__"] == "pd.index.datetime64.v1"
    assert normalized["index"]["values"]["__codec__"] == "np.ndarray.v1"
    assert normalized_index["__codec__"] == "pd.index.v1"
    assert normalized_index["payload"]["__codec__"] == "pd.index.datetime64.v1"
    assert normalized_index["payload"]["values"]["__codec__"] == "np.ndarray.v1"
    assert restored.equals(series)
    assert restored_index.equals(index)


def test_pickle_stable_v1_roundtrips_dataframe_object_dtype():
    frame = pd.DataFrame(
        {
            "param": ["参数", "strategy"],
            "value": [{"window": 20}, ["a", "b"]],
            "score": [1.5, 2.5],
        }
    )

    normalized = normalize_for_pickle_stable(frame)
    restored = stable_pickle_loads(stable_pickle_dumps(frame))

    assert normalized["data"][0]["values"]["__codec__"] == "np.ndarray.object.v1"
    assert restored.equals(frame)


def test_pickle_stable_v1_roundtrips_dataframe_with_duplicate_columns():
    frame = pd.concat(
        [
            pd.Series([1, 2], name="dup", dtype=np.int64),
            pd.Series([3.5, 4.5], name="dup", dtype=np.float64),
            pd.Series(["x", "y"], name="label"),
        ],
        axis=1,
    )

    normalized = normalize_for_pickle_stable(frame)
    restored = stable_pickle_loads(stable_pickle_dumps(frame))

    assert [item["name"] for item in normalized["data"]] == [
        {"__type__": "pd.label", "kind": "str", "value": "dup"},
        {"__type__": "pd.label", "kind": "str", "value": "dup"},
        {"__type__": "pd.label", "kind": "str", "value": "label"},
    ]
    assert normalized["data"][0]["dtype"] == "int64"
    assert normalized["data"][1]["dtype"] == "float64"
    assert list(restored.columns) == ["dup", "dup", "label"]
    assert list(restored.dtypes.astype(str)) == list(frame.dtypes.astype(str))
    assert restored.equals(frame)


def test_pickle_stable_v1_roundtrips_series_object_dtype():
    series = pd.Series(["中文", {"x": 1}, ["a", "b"]], name="value")

    normalized = normalize_for_pickle_stable(series)
    restored = stable_pickle_loads(stable_pickle_dumps(series))

    assert normalized["values"]["__codec__"] == "np.ndarray.object.v1"
    assert restored.equals(series)


def test_pickle_stable_v1_roundtrips_ndarray_object_dtype():
    array = np.array([{"x": 1}], dtype=object)
    normalized = normalize_for_pickle_stable(array)
    restored = stable_pickle_loads(stable_pickle_dumps(array))

    assert normalized["__codec__"] == "np.ndarray.object.v1"
    assert np.array_equal(restored, array)
