from __future__ import annotations

import hashlib
from types import SimpleNamespace

from pycloud_parallel.data.ref import DataRef
from pycloud_parallel.controlplane.data_registry import ResolvedDataRef
from pycloud_parallel.controlplane.gateway_upload import relay_data_ref_v1
from pycloud_parallel.controlplane.job_queue import _resolve_payload_data_refs
from pycloud_parallel.controlplane.node.results import _resolve_single_data_ref
from pycloud_parallel.execution.support import _put_data_via_clients


def _object_id(blob: bytes) -> str:
    return "sha256:" + hashlib.sha256(blob).hexdigest()


def test_upload_once_ref_can_be_resolved_by_worker_remote_fetch(tmp_path, monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod
    import pycloud_parallel.controlplane.node_control_client as node_control_client_mod

    monkeypatch.delenv("PYCLOUD_DATAREF_UPLOAD_STRATEGY", raising=False)
    monkeypatch.delenv("PYCLOUD_DATAREF_RESOLUTION", raising=False)
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    blobs = {}
    remote_calls = []

    class FakeUploadClient:
        def __init__(self, target):
            self.target = target
            self.calls = []

        def upload_object_from_bytes(self, *, blob, format, chunk_size):
            del chunk_size
            self.calls.append((blob, format))
            object_id = _object_id(blob)
            blobs[(self.target, object_id)] = bytes(blob)
            return DataRef(ref_id=object_id, storage_id=object_id, format=format, size_bytes=len(blob))

    class FakeNodeControlClient:
        def __init__(self, target, *args, **kwargs):
            self.target = target

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def download_object_bytes(self, *, object_id):
            remote_calls.append((self.target, object_id))
            return blobs[(self.target, object_id)]

    clients = [FakeUploadClient("node-a:50061"), FakeUploadClient("node-b:50062")]
    ref = _put_data_via_clients(clients, b"hello once", serialization_mode="legacy_v1")

    assert ref.control_addr == "node-a:50061"
    assert len(clients[0].calls) == 1
    assert clients[1].calls == []

    monkeypatch.setattr(node_control_client_mod, "NodeControlClient", FakeNodeControlClient)
    assert _resolve_single_data_ref(ref, object_dir=str(tmp_path)) == b"hello once"
    assert _resolve_single_data_ref(ref, object_dir=str(tmp_path)) == b"hello once"
    assert remote_calls == [("node-a:50061", ref.object_id)]


def test_gateway_lazy_ref_can_be_resolved_by_worker_remote_fetch(tmp_path, monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod
    import pycloud_parallel.controlplane.node_control_client as node_control_client_mod

    monkeypatch.setenv("PYCLOUD_GATEWAY_DATAREF_RELAY", "lazy")
    monkeypatch.delenv("PYCLOUD_DATAREF_RESOLUTION", raising=False)
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    blob = b"gateway lazy payload"
    object_id = _object_id(blob)
    original = DataRef(
        ref_id=object_id,
        storage_id=object_id,
        format="bin",
        size_bytes=len(blob),
        materialize_as="bytes",
        locator_kind="controlplane",
        locator_token="infocenter:50051",
    )
    route = SimpleNamespace(control_addr="node-b:50062", node_id="node-b", node_instance_id="node-b-inst")
    remote_calls = []

    class ForbiddenGatewayNodeControlClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("lazy gateway relay must not copy object bytes")

    class FakeNodeControlClient:
        def __init__(self, target, *args, **kwargs):
            self.target = target

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def download_object_bytes(self, *, object_id):
            remote_calls.append((self.target, object_id))
            return blob

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.gateway_upload.resolve_data_ref",
        lambda ref, **_kwargs: ResolvedDataRef(
            ref=ref,
            control_addr="node-a:50061",
            node_id="node-a",
            node_instance_id="node-a-inst",
            locator_kind="node_control",
            locator_token="node-a:50061",
            replicas=({"control_addr": "node-a:50061", "node_id": "node-a", "node_instance_id": "node-a-inst"},),
        ),
    )
    monkeypatch.setattr("pycloud_parallel.controlplane.gateway_upload.NodeControlClient", ForbiddenGatewayNodeControlClient)
    monkeypatch.setattr(node_control_client_mod, "NodeControlClient", FakeNodeControlClient)

    relayed = relay_data_ref_v1(route=route, data_ref=original, registry_target="infocenter:50051", timeout_sec=1.0)

    assert relayed.control_addr == "node-a:50061"
    assert _resolve_single_data_ref(relayed, object_dir=str(tmp_path)) == blob
    assert remote_calls == [("node-a:50061", object_id)]


def test_jobqueue_deferred_ref_can_be_resolved_by_worker_remote_fetch(tmp_path, monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod
    import pycloud_parallel.controlplane.node_control_client as node_control_client_mod

    monkeypatch.delenv("PYCLOUD_JOBQUEUE_RESOLVE_REFS", raising=False)
    monkeypatch.delenv("PYCLOUD_DATAREF_RESOLUTION", raising=False)
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    blob = b"jobqueue deferred payload"
    object_id = _object_id(blob)
    ref = DataRef(
        ref_id=object_id,
        storage_id=object_id,
        format="bin",
        size_bytes=len(blob),
        materialize_as="bytes",
        locator_kind="node_control",
        locator_token="node-a:50061",
        control_addr="node-a:50061",
    )

    class ForbiddenJobQueueNodeControlClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("defer_to_worker must not fetch in job-orch")

    monkeypatch.setattr("pycloud_parallel.controlplane.job_queue.NodeControlClient", ForbiddenJobQueueNodeControlClient)
    payload = _resolve_payload_data_refs(
        {"job_payload": {"blob_ref": ref}},
        registry_target="infocenter:50051",
        timeout_sec=1.0,
    )

    class FakeNodeControlClient:
        def __init__(self, target, *args, **kwargs):
            self.target = target

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def download_object_bytes(self, *, object_id):
            assert self.target == "node-a:50061"
            assert object_id == ref.object_id
            return blob

    monkeypatch.setattr(node_control_client_mod, "NodeControlClient", FakeNodeControlClient)
    assert payload["job_payload"]["blob_ref"] == ref
    assert _resolve_single_data_ref(payload["job_payload"]["blob_ref"], object_dir=str(tmp_path)) == blob
