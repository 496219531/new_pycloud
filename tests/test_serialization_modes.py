from __future__ import annotations

import numpy as np
import pandas as pd

from pycloud_parallel.controlplane.data_ref import DataRef
from pycloud_parallel.controlplane.node.results import _materialize_object_bytes
from pycloud_parallel.controlplane.serialization import deserialize_by_mode, serialize_by_mode
from pycloud_parallel.execution.support import _put_data_via_clients


def test_legacy_v1_mode_roundtrips_structured_values():
    payload = {"value": 1, "items": [1, 2, 3]}
    encoded = serialize_by_mode(payload, mode="legacy_v1")
    restored = deserialize_by_mode(encoded, mode="legacy_v1")
    assert restored == payload


def test_structured_v1_mode_roundtrips_payload():
    payload = {
        "frame": pd.DataFrame({"a": [1, 2]}),
        "array": np.array([1, 2, 3], dtype=np.int64),
        "blob": b"abc",
    }
    encoded = serialize_by_mode(payload, mode="structured_v1")
    restored = deserialize_by_mode(encoded, mode="structured_v1")
    assert restored["frame"].equals(payload["frame"])
    assert np.array_equal(restored["array"], payload["array"])
    assert restored["blob"] == payload["blob"]


def test_pickle_stable_v1_mode_roundtrips_payload():
    payload = {
        "frame": pd.DataFrame({"a": [1, 2]}),
        "array": np.array([1, 2, 3], dtype=np.int64),
    }
    encoded = serialize_by_mode(payload, mode="pickle_stable_v1")
    restored = deserialize_by_mode(encoded, mode="pickle_stable_v1")
    assert restored["frame"].equals(payload["frame"])
    assert np.array_equal(restored["array"], payload["array"])


class _FakeUploadClient:
    def __init__(self) -> None:
        self.calls = []

    def upload_object_from_bytes(self, *, blob: bytes, format: str, chunk_size: int):
        self.calls.append({"blob": blob, "format": format, "chunk_size": chunk_size})
        return DataRef(
            ref_id="sha256:" + "a" * 64,
            storage_id="sha256:" + "a" * 64,
            format=format,
            size_bytes=len(blob),
            locator_kind="node_local",
            locator_token="",
        )


def test_put_data_via_clients_supports_explicit_structured_mode():
    client = _FakeUploadClient()
    frame = pd.DataFrame({"a": [1, 2]})

    ref = _put_data_via_clients([client], frame, serialization_mode="structured_v1")

    assert ref.format == "structured_v1"
    assert client.calls[0]["format"] == "structured_v1"


def test_node_results_materializes_new_mode_payloads():
    payload = {"frame": pd.DataFrame({"a": [1, 2]})}
    structured_blob = serialize_by_mode(payload, mode="structured_v1")
    pickle_blob = serialize_by_mode(payload, mode="pickle_stable_v1")

    structured_restored = _materialize_object_bytes(blob=structured_blob, fmt="structured_v1", materialize_as="auto")
    pickle_restored = _materialize_object_bytes(blob=pickle_blob, fmt="pickle_stable_v1", materialize_as="auto")

    assert structured_restored["frame"].equals(payload["frame"])
    assert pickle_restored["frame"].equals(payload["frame"])
