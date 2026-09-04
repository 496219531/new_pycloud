from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from pycloud_parallel.data.ref import DataRef
from pycloud_parallel.controlplane.data_registry import ResolvedDataRef
from pycloud_parallel.controlplane.gateway_upload import (
    ensure_data_ref_on_route,
    relay_data_ref_v1,
    relay_payload_data_refs_v1,
)


def test_ensure_data_ref_on_route_reuses_same_route_ref():
    route = SimpleNamespace(control_addr="127.0.0.1:50061", node_id="node-1", node_instance_id="node-1-inst")
    ref = DataRef(
        ref_id="sha256:" + "a" * 64,
        storage_id="sha256:" + "a" * 64,
        format="bin",
        locator_kind="node_control",
        locator_token="127.0.0.1:50061",
        control_addr="127.0.0.1:50061",
    )

    assert ensure_data_ref_on_route(route=route, value=ref, registry_target="127.0.0.1:50051", timeout_sec=5.0) == ref


def test_relay_payload_data_refs_v1_rewrites_nested_refs():
    route = SimpleNamespace(control_addr="127.0.0.1:50062", node_id="node-2", node_instance_id="node-2-inst")
    original = DataRef(
        ref_id="sha256:" + "b" * 64,
        storage_id="sha256:" + "b" * 64,
        format="bin",
        locator_kind="controlplane",
        locator_token="127.0.0.1:50051",
    )
    relayed = DataRef(
        ref_id="sha256:" + "c" * 64,
        storage_id="sha256:" + "c" * 64,
        format="bin",
        locator_kind="node_control",
        locator_token="127.0.0.1:50062",
        control_addr="127.0.0.1:50062",
    )

    with patch(
        "pycloud_parallel.controlplane.gateway_upload.ensure_data_ref_on_route",
        return_value=relayed,
    ) as mocked:
        rewritten = relay_payload_data_refs_v1(
            route=route,
            payload={"a": original, "nested": [original]},
            registry_target="127.0.0.1:50051",
            timeout_sec=5.0,
        )

    assert rewritten["a"] == relayed
    assert rewritten["nested"][0] == relayed
    assert mocked.call_count == 2


def test_relay_data_ref_v1_defaults_to_lazy_locator(monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod

    monkeypatch.delenv("PYCLOUD_GATEWAY_DATAREF_RELAY", raising=False)
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    route = SimpleNamespace(control_addr="127.0.0.1:50062", node_id="node-2", node_instance_id="node-2-inst")
    original = DataRef(
        ref_id="sha256:" + "b" * 64,
        storage_id="sha256:" + "b" * 64,
        format="bin",
        size_bytes=5,
        locator_kind="node_control",
        locator_token="127.0.0.1:50061",
        control_addr="127.0.0.1:50061",
    )
    calls = []

    class FakeNodeControlClient:
        def __init__(self, target, *args, **kwargs):
            self.target = target

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def download_object_bytes(self, *, object_id):
            calls.append(("download", self.target, object_id))
            return b"hello"

        def upload_object_from_bytes(self, *, blob, format):
            calls.append(("upload", self.target, blob, format))
            return DataRef(
                ref_id="sha256:" + "c" * 64,
                storage_id="sha256:" + "c" * 64,
                format=format,
                size_bytes=len(blob),
            )

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.gateway_upload.resolve_data_ref",
        lambda ref, **_kwargs: ResolvedDataRef(ref=ref, control_addr="127.0.0.1:50061"),
    )
    monkeypatch.setattr("pycloud_parallel.controlplane.gateway_upload.NodeControlClient", FakeNodeControlClient)

    relayed = relay_data_ref_v1(route=route, data_ref=original, registry_target="127.0.0.1:50051", timeout_sec=5.0)

    assert relayed.control_addr == "127.0.0.1:50061"
    assert relayed.storage_id == original.storage_id
    assert calls == []


def test_relay_data_ref_v1_lazy_keeps_source_locator_without_copy(monkeypatch, request):
    from pycloud_parallel.controlplane import config as config_mod

    monkeypatch.setenv("PYCLOUD_GATEWAY_DATAREF_RELAY", "lazy")
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    route = SimpleNamespace(control_addr="127.0.0.1:50062", node_id="node-2", node_instance_id="node-2-inst")
    original = DataRef(
        ref_id="sha256:" + "b" * 64,
        storage_id="sha256:" + "b" * 64,
        format="bin",
        size_bytes=5,
        locator_kind="controlplane",
        locator_token="127.0.0.1:50051",
    )
    registered = []

    class ForbiddenNodeControlClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("lazy relay must not download or upload object bytes")

    class FakeDataRegistryClient:
        def __init__(self, target, *args, **kwargs):
            self.target = target

        def register(self, ref, **kwargs):
            registered.append((self.target, ref, kwargs))
            return {}

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.gateway_upload.resolve_data_ref",
        lambda ref, **_kwargs: ResolvedDataRef(
            ref=ref,
            control_addr="127.0.0.1:50061",
            node_id="node-1",
            node_instance_id="node-1-inst",
            locator_kind="node_control",
            locator_token="127.0.0.1:50061",
            via_registry=True,
            replicas=({"control_addr": "127.0.0.1:50061", "node_id": "node-1", "node_instance_id": "node-1-inst"},),
        ),
    )
    monkeypatch.setattr("pycloud_parallel.controlplane.gateway_upload.NodeControlClient", ForbiddenNodeControlClient)
    monkeypatch.setattr("pycloud_parallel.controlplane.gateway_upload.DataRegistryClient", FakeDataRegistryClient)

    relayed = relay_data_ref_v1(route=route, data_ref=original, registry_target="127.0.0.1:50051", timeout_sec=5.0)

    assert relayed.storage_id == original.object_id
    assert relayed.locator_kind == "node_control"
    assert relayed.locator_token == "127.0.0.1:50061"
    assert relayed.control_addr == "127.0.0.1:50061"
    assert registered
    assert registered[0][0] == "127.0.0.1:50051"
