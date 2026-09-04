from __future__ import annotations

import json
from types import SimpleNamespace

from pycloud_parallel.controlplane.client_transport import _call_route_http
from pycloud_parallel.controlplane.http_connection_pool import BufferedHttpResponse
from pycloud_parallel.controlplane.effective_policy import (
    EffectivePolicy,
    should_use_http_raw_bytes_body,
    should_use_raw_bytes_payload,
)
from pycloud_parallel.controlplane.infocenter_client import InfoCenterNode
from pycloud_parallel.controlplane.node_capability import NodeCapability
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.execution.task_pool import _node_control_target_for_node as _taskpool_nodecontrol_target
from pycloud_parallel.execution.service_session import _node_control_target_for_node as _service_nodecontrol_target
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


def _policy(
    *,
    resolved_mode: str,
    allowed_modes: tuple[str, ...],
    use_raw_bytes_payload: bool,
    use_http_raw_bytes_body: bool,
) -> EffectivePolicy:
    return EffectivePolicy(
        policy_id="trusted_internal",
        version=1,
        resolved_mode=resolved_mode,
        allowed_modes=allowed_modes,
        inline_payload_threshold_bytes=256,
        inline_payload_hard_limit_bytes=1024,
        inline_result_threshold_bytes=1024,
        inline_result_hard_limit_bytes=1024,
        use_raw_bytes_payload=use_raw_bytes_payload,
        use_http_raw_bytes_body=use_http_raw_bytes_body,
        allow_pickle_stable="pickle_stable_v1" in allowed_modes,
    )


def test_transport_lane_follows_effective_policy_before_mode():
    assert should_use_raw_bytes_payload(mode="pickle_stable_v1") is True
    assert should_use_raw_bytes_payload(mode="pickle_native_v1") is True
    assert should_use_raw_bytes_payload(mode="legacy_v1") is False
    assert (
        should_use_raw_bytes_payload(
            mode="pickle_stable_v1",
            effective_policy=_policy(
                resolved_mode="pickle_stable_v1",
                allowed_modes=("pickle_stable_v1", "structured_v1"),
                use_raw_bytes_payload=False,
                use_http_raw_bytes_body=False,
            ),
        )
        is False
    )


def test_http_lane_follows_effective_policy_before_mode():
    assert should_use_http_raw_bytes_body(mode="pickle_stable_v1") is True
    assert should_use_http_raw_bytes_body(mode="pickle_native_v1") is True
    assert (
        should_use_http_raw_bytes_body(
            mode="pickle_stable_v1",
            effective_policy=_policy(
                resolved_mode="pickle_stable_v1",
                allowed_modes=("pickle_stable_v1", "structured_v1"),
                use_raw_bytes_payload=False,
                use_http_raw_bytes_body=False,
            ),
        )
        is False
    )


def test_call_route_http_uses_json_when_effective_policy_disables_http_bytes(monkeypatch):
    captured = {}

    def _fake_request(**kwargs):
        captured["headers"] = dict(kwargs.get("headers") or {})
        captured["body"] = bytes(kwargs.get("body") or b"")
        body = json.dumps({"ok": True, "data": {"value": 1}}).encode("utf-8")
        return BufferedHttpResponse(200, "OK", {"Content-Type": "application/json"}, body)

    monkeypatch.setattr("pycloud_parallel.controlplane.client_transport.pooled_http_request", _fake_request)

    _call_route_http(
        SimpleNamespace(http_base_url="http://127.0.0.1:18080/svc/demo", control_addr=""),
        method="run",
        payload={"value": 1},
        timeout_sec=5.0,
        service_token="",
        serialization_mode="pickle_stable_v1",
        effective_policy=_policy(
            resolved_mode="pickle_stable_v1",
            allowed_modes=("pickle_stable_v1", "structured_v1"),
            use_raw_bytes_payload=False,
            use_http_raw_bytes_body=False,
        ),
    )

    header_map = {key.lower(): value for key, value in captured["headers"].items()}
    assert header_map["content-type"].startswith("application/json")
    assert "x-pycloud-codec" not in header_map


def test_call_route_http_can_use_bytes_for_structured_when_policy_enables(monkeypatch):
    captured = {}

    def _fake_request(**kwargs):
        captured["headers"] = dict(kwargs.get("headers") or {})
        captured["body"] = bytes(kwargs.get("body") or b"")
        body = json.dumps({"ok": True, "data": {"value": 1}}).encode("utf-8")
        return BufferedHttpResponse(200, "OK", {"Content-Type": "application/json"}, body)

    monkeypatch.setattr("pycloud_parallel.controlplane.client_transport.pooled_http_request", _fake_request)

    _call_route_http(
        SimpleNamespace(http_base_url="http://127.0.0.1:18080/svc/demo", control_addr=""),
        method="run",
        payload={"value": 1},
        timeout_sec=5.0,
        service_token="",
        serialization_mode="structured_v1",
        effective_policy=_policy(
            resolved_mode="structured_v1",
            allowed_modes=("structured_v1", "legacy_v1"),
            use_raw_bytes_payload=True,
            use_http_raw_bytes_body=True,
        ),
    )

    header_map = {key.lower(): value for key, value in captured["headers"].items()}
    assert header_map["content-type"] == "application/x-pycloud-transport"
    assert header_map["x-pycloud-codec"] == "structured_v1"


def test_node_control_client_uses_struct_payload_when_transport_lane_disabled():
    captured = {}

    def _fake_request(**kwargs):
        captured["url"] = kwargs["url"]
        captured["body"] = json.loads((kwargs.get("body") or b"{}").decode("utf-8"))
        body = json.dumps({"ok": True, "data": {"value": 1}}).encode("utf-8")
        return BufferedHttpResponse(200, "OK", {"Content-Type": "application/json"}, body)

    from pycloud_parallel.controlplane import client_transport_runtime

    original_request = client_transport_runtime.pooled_http_request
    client_transport_runtime.pooled_http_request = _fake_request
    try:
        client = NodeControlClient("http://127.0.0.1:18061", timeout_sec=5.0)
        NodeControlClient.call_service(
            client,
            service_id="svc-1",
            method="run",
            payload={"value": 1},
            serialization_mode="pickle_stable_v1",
            effective_policy=_policy(
                resolved_mode="pickle_stable_v1",
                allowed_modes=("pickle_stable_v1", "structured_v1"),
                use_raw_bytes_payload=False,
                use_http_raw_bytes_body=False,
            ),
        )
    finally:
        client_transport_runtime.pooled_http_request = original_request

    assert captured["url"].endswith("/services/svc-1/call/run")
    assert captured["body"]["payload"]["__pycloud_transport__"]["codec"] == "pickle_stable_v1"


def test_node_control_client_can_use_transport_lane_for_structured_mode():
    captured = {}

    def _fake_request(**kwargs):
        captured["body"] = json.loads((kwargs.get("body") or b"{}").decode("utf-8"))
        body = json.dumps({"ok": True, "data": {"value": 1}}).encode("utf-8")
        return BufferedHttpResponse(200, "OK", {"Content-Type": "application/json"}, body)

    from pycloud_parallel.controlplane import client_transport_runtime

    original_request = client_transport_runtime.pooled_http_request
    client_transport_runtime.pooled_http_request = _fake_request
    try:
        client = NodeControlClient("http://127.0.0.1:18061", timeout_sec=5.0)
        NodeControlClient.call_service(
            client,
            service_id="svc-1",
            method="run",
            payload={"value": 1},
            serialization_mode="structured_v1",
            effective_policy=_policy(
                resolved_mode="structured_v1",
                allowed_modes=("structured_v1", "legacy_v1"),
                use_raw_bytes_payload=True,
                use_http_raw_bytes_body=True,
            ),
        )
    finally:
        client_transport_runtime.pooled_http_request = original_request

    assert captured["body"]["payload"]["__pycloud_transport__"]["codec"] == "structured_v1"


def test_nodecontrol_target_uses_http_capability():
    node = InfoCenterNode(
        node_instance_id="node-inst",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        healthy=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
        capability=NodeCapability(
            supports_http_control=True,
            control_base_url="http://127.0.0.1:18061",
        ),
    )

    assert _taskpool_nodecontrol_target(node) == "http://127.0.0.1:18061"
    assert _service_nodecontrol_target(node) == "http://127.0.0.1:18061"
    assert node.capability.to_dict()["supports_http_control"] is True


def test_nodecontrol_target_falls_back_to_control_addr_when_http_missing():
    node = InfoCenterNode(
        node_instance_id="node-inst",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        healthy=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )

    assert _taskpool_nodecontrol_target(node) == "127.0.0.1:50061"
    assert _service_nodecontrol_target(node) == "127.0.0.1:50061"
