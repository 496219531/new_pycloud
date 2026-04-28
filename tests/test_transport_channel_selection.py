from __future__ import annotations

import json
from types import SimpleNamespace

from pycloud_parallel.controlplane.client_transport import _call_route_http
from pycloud_parallel.controlplane.effective_policy import (
    EffectivePolicy,
    should_use_http_bytes_transport,
    should_use_transport_payload_bytes,
)
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def _policy(
    *,
    resolved_mode: str,
    allowed_modes: tuple[str, ...],
    use_transport_payload_bytes: bool,
    use_http_bytes_transport: bool,
) -> EffectivePolicy:
    return EffectivePolicy(
        policy_id="trusted_internal",
        version=1,
        resolved_mode=resolved_mode,
        allowed_modes=allowed_modes,
        inline_payload_soft_limit_bytes=256,
        inline_payload_hard_limit_bytes=1024,
        inline_result_hard_limit_bytes=1024,
        use_transport_payload_bytes=use_transport_payload_bytes,
        use_http_bytes_transport=use_http_bytes_transport,
        allow_pickle_stable="pickle_stable_v1" in allowed_modes,
    )


class _FakeHttpResponse:
    def __init__(self, body: bytes, headers: dict[str, str]) -> None:
        self._body = body
        self.headers = headers

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_FakeHttpResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        return None


def test_transport_lane_follows_effective_policy_before_mode():
    assert should_use_transport_payload_bytes(mode="pickle_stable_v1") is True
    assert should_use_transport_payload_bytes(mode="legacy_v1") is False
    assert (
        should_use_transport_payload_bytes(
            mode="pickle_stable_v1",
            effective_policy=_policy(
                resolved_mode="pickle_stable_v1",
                allowed_modes=("pickle_stable_v1", "structured_v1"),
                use_transport_payload_bytes=False,
                use_http_bytes_transport=False,
            ),
        )
        is False
    )


def test_http_lane_follows_effective_policy_before_mode():
    assert should_use_http_bytes_transport(mode="pickle_stable_v1") is True
    assert (
        should_use_http_bytes_transport(
            mode="pickle_stable_v1",
            effective_policy=_policy(
                resolved_mode="pickle_stable_v1",
                allowed_modes=("pickle_stable_v1", "structured_v1"),
                use_transport_payload_bytes=False,
                use_http_bytes_transport=False,
            ),
        )
        is False
    )


def test_call_route_http_uses_json_when_effective_policy_disables_http_bytes(monkeypatch):
    captured = {}

    def _fake_urlopen(req, timeout):  # noqa: ARG001
        captured["headers"] = dict(req.header_items())
        captured["body"] = bytes(req.data or b"")
        body = json.dumps({"ok": True, "data": {"value": 1}}).encode("utf-8")
        return _FakeHttpResponse(body, {"Content-Type": "application/json"})

    monkeypatch.setattr("pycloud_parallel.controlplane.client_transport.urlopen", _fake_urlopen)

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
            use_transport_payload_bytes=False,
            use_http_bytes_transport=False,
        ),
    )

    header_map = {key.lower(): value for key, value in captured["headers"].items()}
    assert header_map["content-type"].startswith("application/json")
    assert "x-pycloud-codec" not in header_map


def test_call_route_http_can_use_bytes_for_structured_when_policy_enables(monkeypatch):
    captured = {}

    def _fake_urlopen(req, timeout):  # noqa: ARG001
        captured["headers"] = dict(req.header_items())
        captured["body"] = bytes(req.data or b"")
        body = json.dumps({"ok": True, "data": {"value": 1}}).encode("utf-8")
        return _FakeHttpResponse(body, {"Content-Type": "application/json"})

    monkeypatch.setattr("pycloud_parallel.controlplane.client_transport.urlopen", _fake_urlopen)

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
            use_transport_payload_bytes=True,
            use_http_bytes_transport=True,
        ),
    )

    header_map = {key.lower(): value for key, value in captured["headers"].items()}
    assert header_map["content-type"] == "application/x-pycloud-transport"
    assert header_map["x-pycloud-codec"] == "structured_v1"


def test_node_control_client_uses_struct_payload_when_transport_lane_disabled():
    client = NodeControlClient.__new__(NodeControlClient)
    client.timeout_sec = 5.0
    captured = {}

    def _fake_call_service(request, timeout):  # noqa: ARG001
        captured["request"] = request
        return pb2.CallServiceResponse(ok=True, service_id="svc-1", method="run")

    client.stub = SimpleNamespace(CallService=_fake_call_service)

    NodeControlClient.call_service(
        client,
        service_id="svc-1",
        method="run",
        payload={"value": 1},
        serialization_mode="pickle_stable_v1",
        effective_policy=_policy(
            resolved_mode="pickle_stable_v1",
            allowed_modes=("pickle_stable_v1", "structured_v1"),
            use_transport_payload_bytes=False,
            use_http_bytes_transport=False,
        ),
    )

    request = captured["request"]
    assert not request.HasField("transport_payload")
    assert request.payload.fields


def test_node_control_client_can_use_transport_lane_for_structured_mode():
    client = NodeControlClient.__new__(NodeControlClient)
    client.timeout_sec = 5.0
    captured = {}

    def _fake_call_service(request, timeout):  # noqa: ARG001
        captured["request"] = request
        return pb2.CallServiceResponse(ok=True, service_id="svc-1", method="run")

    client.stub = SimpleNamespace(CallService=_fake_call_service)

    NodeControlClient.call_service(
        client,
        service_id="svc-1",
        method="run",
        payload={"value": 1},
        serialization_mode="structured_v1",
        effective_policy=_policy(
            resolved_mode="structured_v1",
            allowed_modes=("structured_v1", "legacy_v1"),
            use_transport_payload_bytes=True,
            use_http_bytes_transport=True,
        ),
    )

    request = captured["request"]
    assert request.HasField("transport_payload")
    assert request.transport_payload.codec == "structured_v1"
