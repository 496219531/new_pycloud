from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from pycloud_parallel.controlplane.data_ref import DataRef
from pycloud_parallel.controlplane.gateway_upload import (
    ensure_data_ref_on_route,
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
