from __future__ import annotations

from unittest.mock import patch

from pycloud_parallel.api import Service
from pycloud_parallel.controlplane.discovery_client import DiscoveryServiceClient
from pycloud_parallel.controlplane.infocenter_client import InfoCenterNode, InfoCenterNodeService
from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient
from pycloud_parallel.execution.call_proxy import _CallProxy


def test_service_connect_discovery_returns_unified_service_object():
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        transport="discovery",
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
        transport="gateway",
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
            transport="discovery",
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
