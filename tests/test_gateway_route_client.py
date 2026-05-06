"""Tests for GatewayServiceClient."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pycloud_parallel.data.ref import DataRef


class TestGatewayServiceClient:
    def test_call_sync_like_usage(self):
        from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

        client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=9.0)
        with patch(
            "pycloud_parallel.controlplane.gateway_client.client_mod._http_json_request",
            return_value={"ok": True, "data": {"y": 49}},
        ) as mocked:
            result = client.call(service_name="svc-demo", method="square", payload={"x": 7}, timeout_sec=9.0)

        assert result == {"ok": True, "data": {"y": 49}}
        assert mocked.call_count == 2

    def test_list_methods(self):
        from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

        client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0)
        with patch(
            "pycloud_parallel.controlplane.gateway_client.client_mod._http_json_request",
            return_value={
                "ok": True,
                "methods": [{"method": "square"}, {"method": "fibonacci"}],
            },
        ) as mocked:
            assert client.list_methods(service_name="svc-demo", include_docs=True) == [
                {"method": "square"},
                {"method": "fibonacci"},
            ]
            mocked.assert_called_once()

    def test_status(self):
        from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

        client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0)
        with patch(
            "pycloud_parallel.controlplane.gateway_client.client_mod._http_json_request",
            return_value={"ok": True, "route_count": 1},
        ) as mocked:
            result = client.get_status(service_name="svc-demo")

        assert result == {"ok": True, "route_count": 1}
        mocked.assert_called_once()


def test_gateway_service_client_rejects_oversized_inline_payload_before_http():
    from pycloud_parallel.controlplane.config import get_binding_payload_thresholds
    from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

    soft_limit_bytes, _hard_limit_bytes, _result_limit_bytes = get_binding_payload_thresholds(
        "gateway_public",
        requested_mode="structured_v1",
        context="gateway_public",
    )
    payload = {"blob": "x" * (soft_limit_bytes + 1024)}
    client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0)
    with patch("pycloud_parallel.controlplane.gateway_client.client_mod._http_json_request") as mocked:
        with pytest.raises(ValueError, match="size_bytes=.*soft_limit_bytes="):
            client.call(
                service_name="svc-demo",
                method="run",
                payload=payload,
                timeout_sec=5.0,
                serialization_mode="structured_v1",
            )
    assert mocked.call_count == 1
    assert mocked.call_args.kwargs["path"] == "/svc/svc-demo/status"


def test_gateway_service_client_does_not_use_route_aware_staging_when_status_succeeds(monkeypatch):
    from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

    routes = [{"control_addr": "127.0.0.1:50061"}]

    client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0)
    with (
        patch.object(client, "get_status", return_value={"ok": True, "route_count": 1, "routes": routes}),
        patch(
            "pycloud_parallel.controlplane.gateway_client.client_mod._http_json_request",
            return_value={"ok": True, "data": {"y": 49}},
        ) as mocked_http,
    ):
        result = client.call(service_name="svc-demo", method="run", payload={"blob": "x" * 2048}, timeout_sec=5.0)

    assert result == {"ok": True, "data": {"y": 49}}
    assert mocked_http.call_count == 1


def test_gateway_service_client_does_not_use_cached_routes_for_payload_staging(monkeypatch):
    from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

    routes = [{"control_addr": "127.0.0.1:50061"}]

    client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0)
    with (
        patch.object(client, "get_status", side_effect=[
            {"ok": True, "route_count": 1, "routes": routes},
            RuntimeError("status boom"),
        ]),
        patch(
            "pycloud_parallel.controlplane.gateway_client.client_mod._http_json_request",
            return_value={"ok": True, "data": {"y": 81}},
        ),
    ):
        client.call(service_name="svc-demo", method="run", payload={"blob": "x" * 2048}, timeout_sec=5.0)
        client.call(service_name="svc-demo", method="run", payload={"blob": "y" * 2048}, timeout_sec=5.0)


def test_gateway_service_client_status_failure_without_cache_allows_small_payload():
    from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

    client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0)
    with (
        patch.object(client, "get_status", side_effect=RuntimeError("status boom")),
        patch(
            "pycloud_parallel.controlplane.gateway_client.client_mod._http_json_request",
            return_value={"ok": True, "data": {"y": 16}},
        ) as mocked_http,
    ):
        result = client.call(service_name="svc-demo", method="run", payload={"x": 4}, timeout_sec=5.0)

    assert result == {"ok": True, "data": {"y": 16}}
    assert mocked_http.call_count == 1


def test_gateway_service_client_status_failure_without_cache_rejects_large_payload():
    from pycloud_parallel.controlplane.config import get_binding_payload_thresholds
    from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

    soft_limit_bytes, _hard_limit_bytes, _result_limit_bytes = get_binding_payload_thresholds(
        "gateway_public",
        requested_mode="structured_v1",
        context="gateway_public",
    )
    payload = {"blob": "x" * (soft_limit_bytes + 1024)}
    client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0)
    with (
        patch.object(client, "get_status", side_effect=RuntimeError("status boom")),
        patch("pycloud_parallel.controlplane.gateway_client.client_mod._http_json_request") as mocked_http,
    ):
        with pytest.raises(ValueError, match="gateway payload is too large"):
            client.call(
                service_name="svc-demo",
                method="run",
                payload=payload,
                timeout_sec=5.0,
                serialization_mode="structured_v1",
            )
    mocked_http.assert_not_called()
