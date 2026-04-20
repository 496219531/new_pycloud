from __future__ import annotations

import numpy as np
import pandas as pd

from pycloud_parallel.controlplane.structured_v1 import structured_dumps, structured_loads


def test_structured_v1_roundtrips_dataframe_series_ndarray_and_bytes():
    frame = pd.DataFrame({"a": [1, 2], "b": [3.5, 4.5]}, index=pd.Index(["x", "y"], name="idx"))
    series = pd.Series([10, 11], index=pd.Index(["u", "v"], name="sidx"), name="score")
    array = np.array([[1, 2], [3, 4]], dtype=np.int64)
    blob = b"hello-world"

    payload = {
        "frame": frame,
        "series": series,
        "array": array,
        "blob": blob,
    }

    restored = structured_loads(structured_dumps(payload))

    assert restored["frame"].equals(frame)
    assert restored["series"].equals(series)
    assert np.array_equal(restored["array"], array)
    assert restored["blob"] == blob
