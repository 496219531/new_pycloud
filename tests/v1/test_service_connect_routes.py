from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from pycloud_parallel.api import Service
from pycloud_parallel.controlplane.client_transport import _serialize_route
from pycloud_parallel.controlplane.discovery_client import DiscoveryServiceClient
from pycloud_parallel.controlplane.infocenter_client import InfoCenterNode, InfoCenterNodeService
from pycloud_parallel.controlplane.infocenter_client import InfoCenterServiceRoute
from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient
from pycloud_parallel.controlplane.node_capability import NodeCapability
from pycloud_parallel.execution.call_proxy import _CallProxy
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


def test_service_connect_discovery_returns_unified_service_object():
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        route="discovery",
        validate_on_init=False,
    )
    try:
        assert isinstance(client._transport_client, DiscoveryServiceClient)
        with (
            patch.object(type(client), "list_methods", return_value=[{"method": "square"}]) as mocked_methods,
            patch.object(type(client), "call_balanced", return_value=("node-1", {"ok": True, "data": {"y": 49}})),
            patch.object(DiscoveryServiceClient, "get_status", return_value={"ok": True, "route_count": 1}) as mocked_status,
        ):
            assert client.methods == ["square"]
            assert isinstance(client.square, _CallProxy)
            assert client.square.sync(x=7) == {"y": 49}
            assert client.status() == {"ok": True, "route_count": 1}
        mocked_methods.assert_called_once_with(include_docs=True)
        mocked_status.assert_called_once_with(service_name="svc-demo")
    finally:
        client.close()


def test_service_connect_gateway_returns_unified_service_object():
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        route="gateway",
        validate_on_init=False,
    )
    try:
        assert isinstance(client._transport_client, GatewayServiceClient)
        with (
            patch.object(type(client), "list_methods", return_value=[{"method": "square"}]) as mocked_methods,
            patch.object(type(client), "call_balanced", return_value=("gateway", {"ok": True, "data": {"y": 64}})),
            patch.object(GatewayServiceClient, "get_status", return_value={"ok": True, "route_count": 1}) as mocked_status,
        ):
            assert client.methods == ["square"]
            assert isinstance(client.square, _CallProxy)
            assert client.square.sync(x=8) == {"y": 64}
            assert client.status() == {"ok": True, "route_count": 1}
        mocked_methods.assert_called_once_with(include_docs=True)
        mocked_status.assert_called_once_with(service_name="svc-demo")
    finally:
        client.close()


def test_service_connect_gateway_stream_proxy_yields_items():
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        route="gateway",
        validate_on_init=False,
    )
    try:
        with (
            patch.object(type(client), "list_methods", return_value=[{"method": "count"}]),
            patch.object(
                GatewayServiceClient,
                "stream_call",
                return_value=iter(
                    [
                        {"event": "item", "index": 0, "data": 1},
                        {"event": "item", "index": 1, "data": 2},
                        {"event": "done", "ok": True, "item_count": 2},
                    ]
                ),
            ) as mocked_stream,
        ):
            assert list(client.count.stream()) == [1, 2]
        mocked_stream.assert_called_once()
    finally:
        client.close()


def test_service_connect_discovery_retries_briefly_when_routes_are_not_ready():
    with (
        patch.object(DiscoveryServiceClient, "refresh_routes", return_value=[]) as mocked_refresh,
        patch.object(
            DiscoveryServiceClient,
            "get_status",
            side_effect=[
                {"ok": True, "route_count": 0},
                {"ok": True, "route_count": 1},
            ],
        ) as mocked_status,
        patch("pycloud_parallel.execution.service_session.time.sleep", return_value=None) as mocked_sleep,
    ):
        client = Service.connect(
            target="127.0.0.1:50051",
            service_name="svc-demo",
            route="discovery",
            timeout_sec=1.0,
            validate_on_init=True,
        )
    try:
        assert isinstance(client._transport_client, DiscoveryServiceClient)
        assert mocked_status.call_count == 2
        assert mocked_refresh.call_count >= 2
        assert mocked_refresh.call_args.kwargs == {"service_name": "svc-demo", "force": True}
        mocked_sleep.assert_called()
    finally:
        client.close()


def test_service_connect_inherits_deploy_bound_policy_from_routes():
    capability = NodeCapability(
        supported_modes=("legacy_v1", "structured_v1", "pickle_stable_v1", "pickle_native_v1"),
        supports_raw_bytes_payload=True,
        supports_http_raw_bytes_body=True,
        max_control_send_bytes=4 * 1024 * 1024,
        max_control_recv_bytes=4 * 1024 * 1024,
        max_http_body_bytes=4 * 1024 * 1024,
        max_upload_file_bytes=64 * 1024 * 1024,
        max_upload_total_bytes=128 * 1024 * 1024,
    )
    route = InfoCenterServiceRoute(
        service_name="svc-demo",
        service_id="svc-id-1",
        status=pb2.SERVICE_STATUS_RUNNING,
        node_instance_id="node-1-inst",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        node_healthy=True,
        worker_count=2,
        alive_workers=2,
        in_flight=0,
        lease_expire_at=datetime.now(timezone.utc),
        http_base_url="http://127.0.0.1:18081/svc/svc-id-1",
        capability=capability,
        policy_id="pickle_internal_heavy",
    )
    route_status = {"ok": True, "route_count": 1, "routes": [_serialize_route(route)]}
    with (
        patch.object(DiscoveryServiceClient, "refresh_routes", return_value=[route]),
        patch.object(DiscoveryServiceClient, "list_routes", return_value=[route]),
        patch.object(DiscoveryServiceClient, "get_status", return_value=route_status),
    ):
        client = Service.connect(
            target="127.0.0.1:50051",
            service_name="svc-demo",
            route="discovery",
            serialization_mode="pickle_stable_v1",
            validate_on_init=True,
        )
    try:
        assert client.effective_policy is not None
        assert client.effective_policy.policy_id == "pickle_internal_heavy"
        assert client.effective_policy.resolved_mode == "pickle_stable_v1"
        assert client.serialization_mode == "pickle_stable_v1"
    finally:
        client.close()


def test_service_connect_rejects_mixed_route_policy_metadata():
    route_a = {
        "service_name": "svc-demo",
        "service_id": "svc-id-1",
        "policy_id": "trusted_internal",
        "node_instance_id": "node-1-inst",
        "node_id": "node-1",
        "control_addr": "127.0.0.1:50061",
        "node_healthy": True,
        "worker_count": 1,
        "alive_workers": 1,
        "in_flight": 0,
        "http_base_url": "http://127.0.0.1:18081/svc/svc-id-1",
        "status": pb2.SERVICE_STATUS_RUNNING,
        "lease_expire_at": datetime.now(timezone.utc).isoformat(),
        "capability": NodeCapability(
            supported_modes=("legacy_v1", "structured_v1"),
            supports_raw_bytes_payload=False,
            supports_http_raw_bytes_body=False,
            max_control_send_bytes=4 * 1024 * 1024,
            max_control_recv_bytes=4 * 1024 * 1024,
            max_http_body_bytes=4 * 1024 * 1024,
            max_upload_file_bytes=64 * 1024 * 1024,
            max_upload_total_bytes=128 * 1024 * 1024,
        ).to_dict(),
    }
    route_b = dict(route_a)
    route_b["service_id"] = "svc-id-2"
    route_b["node_instance_id"] = "node-2-inst"
    route_b["node_id"] = "node-2"
    route_b["policy_id"] = "pickle_internal_heavy"
    with (
        patch.object(DiscoveryServiceClient, "refresh_routes", return_value=[]),
        patch.object(DiscoveryServiceClient, "get_status", return_value={"ok": True, "route_count": 2, "routes": [route_a, route_b]}),
    ):
        try:
            Service.connect(
                target="127.0.0.1:50051",
                service_name="svc-demo",
                route="discovery",
                validate_on_init=True,
            )
        except RuntimeError as exc:
            assert "inconsistent deploy-bound policy_id" in str(exc)
        else:
            raise AssertionError("expected Service.connect() to reject mixed route policy metadata")
