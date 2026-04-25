from __future__ import annotations

import pytest

from pycloud_parallel.controlplane.node.execution import (
    _invoke_local_user_callable,
    _invoke_user_callable,
)
from pycloud_parallel.controlplane.serialization import serialize_arrow_compatible


def test_invoke_local_user_callable_normalizes_tagged_dataframe_payload() -> None:
    pd = pytest.importorskip("pandas")

    frame = pd.DataFrame([{"x": 1}, {"x": 2}])
    payload = {"frame": serialize_arrow_compatible(frame)}

    def _fn(frame):
        assert isinstance(frame, pd.DataFrame)
        return int(frame["x"].sum())

    assert _invoke_local_user_callable(_fn, payload) == 3


def test_invoke_user_callable_worker_path_does_not_renormalize_payload() -> None:
    pd = pytest.importorskip("pandas")

    frame = pd.DataFrame([{"x": 1}, {"x": 2}])
    serialized = serialize_arrow_compatible(frame)

    def _fn(frame):
        assert isinstance(frame, dict)
        assert frame == serialized
        return "ok"

    assert _invoke_user_callable(_fn, {"frame": serialized}) == "ok"
