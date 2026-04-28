from __future__ import annotations

import numpy as np
import pandas as pd

from pycloud_parallel.controlplane.data_ref import DataRef
from pycloud_parallel.controlplane.node.results import _materialize_object_bytes
from pycloud_parallel.controlplane.serialization import deserialize_by_mode, serialize_by_mode
from pycloud_parallel.execution.support import _put_data_via_clients, _replicas_for_uploaded_ref


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
    def __init__(self, target: str = "") -> None:
        self.calls = []
        self.target = target

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


def test_put_data_via_clients_defaults_to_fanout(monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod

    monkeypatch.delenv("PYCLOUD_DATAREF_UPLOAD_STRATEGY", raising=False)
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    clients = [_FakeUploadClient("node-a:50061"), _FakeUploadClient("node-b:50062")]
    ref = _put_data_via_clients(clients, b"hello", serialization_mode="legacy_v1")

    assert ref.locator_kind == "node_local"
    assert len(clients[0].calls) == 1
    assert len(clients[1].calls) == 1


def test_put_data_via_clients_upload_once_sets_locator(monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod

    monkeypatch.setenv("PYCLOUD_DATAREF_UPLOAD_STRATEGY", "upload_once")
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    clients = [_FakeUploadClient("node-a:50061"), _FakeUploadClient("node-b:50062")]
    ref = _put_data_via_clients(clients, b"hello", serialization_mode="legacy_v1")

    assert ref.locator_kind == "node_control"
    assert ref.locator_token == "node-a:50061"
    assert ref.control_addr == "node-a:50061"
    assert len(clients[0].calls) == 1
    assert clients[1].calls == []


def test_upload_once_replica_registration_keeps_only_upload_target(monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod

    monkeypatch.setenv("PYCLOUD_DATAREF_UPLOAD_STRATEGY", "upload_once")
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    ref = DataRef(
        ref_id="sha256:" + "a" * 64,
        storage_id="sha256:" + "a" * 64,
        format="bin",
        size_bytes=5,
        locator_kind="node_control",
        locator_token="node-a:50061",
        control_addr="node-a:50061",
    )
    replicas = [
        {"control_addr": "node-a:50061", "node_id": "node-a", "node_instance_id": "inst-a"},
        {"control_addr": "node-b:50062", "node_id": "node-b", "node_instance_id": "inst-b"},
    ]

    assert list(_replicas_for_uploaded_ref(ref, replicas)) == [replicas[0]]


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
