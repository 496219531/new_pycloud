"""Tests for GatewayServiceClient."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pycloud_parallel.controlplane.data_ref import DataRef
from pycloud_parallel.controlplane.serialization import INLINE_PAYLOAD_HARD_LIMIT_BYTES


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
    from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

    payload = {"blob": "x" * (INLINE_PAYLOAD_HARD_LIMIT_BYTES + 1024)}
    client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0)
    with patch("pycloud_parallel.controlplane.gateway_client.client_mod._http_json_request") as mocked:
        with pytest.raises(ValueError, match="DataRef"):
            client.call(service_name="svc-demo", method="run", payload=payload, timeout_sec=5.0)
    assert mocked.call_count == 1
    assert mocked.call_args.kwargs["path"] == "/svc/svc-demo/status"


def test_gateway_service_client_uses_route_aware_staging_when_status_succeeds(monkeypatch):
    from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

    routes = [{"control_addr": "127.0.0.1:50061"}]
    uploads = []

    class _FakeNodeControlClient:
        def __init__(self, target: str, timeout_sec: float = 10.0) -> None:
            del timeout_sec
            self.target = target

        def close(self) -> None:
            return None

    def _fake_prepare(clients, payload, *, object_threshold_bytes):
        del object_threshold_bytes
        uploads.append([client.target for client in clients])
        return {"blob": {"__pycloud_data_ref__": {"ref_id": "sha256:" + ("a" * 64)}}}

    client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0)
    monkeypatch.setattr("pycloud_parallel.controlplane.gateway_client.client_mod.NodeControlClient", _FakeNodeControlClient)
    monkeypatch.setattr("pycloud_parallel.controlplane.gateway_client.client_mod._prepare_remote_call_payload", _fake_prepare)
    with (
        patch.object(client, "get_status", return_value={"ok": True, "route_count": 1, "routes": routes}),
        patch(
            "pycloud_parallel.controlplane.gateway_client.client_mod._http_json_request",
            return_value={"ok": True, "data": {"y": 49}},
        ) as mocked_http,
    ):
        result = client.call(service_name="svc-demo", method="run", payload={"blob": "x" * 2048}, timeout_sec=5.0)

    assert result == {"ok": True, "data": {"y": 49}}
    assert uploads == [["127.0.0.1:50061"]]
    assert mocked_http.call_count == 1


def test_gateway_service_client_uses_cached_routes_when_status_lookup_fails(monkeypatch):
    from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

    routes = [{"control_addr": "127.0.0.1:50061"}]
    uploads = []

    class _FakeNodeControlClient:
        def __init__(self, target: str, timeout_sec: float = 10.0) -> None:
            del timeout_sec
            self.target = target

        def close(self) -> None:
            return None

    def _fake_prepare(clients, payload, *, object_threshold_bytes):
        del object_threshold_bytes
        uploads.append([client.target for client in clients])
        return {"blob": {"__pycloud_data_ref__": {"ref_id": "sha256:" + ("b" * 64)}}}

    client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0)
    monkeypatch.setattr("pycloud_parallel.controlplane.gateway_client.client_mod.NodeControlClient", _FakeNodeControlClient)
    monkeypatch.setattr("pycloud_parallel.controlplane.gateway_client.client_mod._prepare_remote_call_payload", _fake_prepare)
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

    assert uploads == [["127.0.0.1:50061"], ["127.0.0.1:50061"]]


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
    from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

    payload = {"blob": "x" * (INLINE_PAYLOAD_HARD_LIMIT_BYTES + 1024)}
    client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0)
    with (
        patch.object(client, "get_status", side_effect=RuntimeError("status boom")),
        patch("pycloud_parallel.controlplane.gateway_client.client_mod._http_json_request") as mocked_http,
    ):
        with pytest.raises(RuntimeError, match="route-aware staging"):
            client.call(service_name="svc-demo", method="run", payload=payload, timeout_sec=5.0)
    mocked_http.assert_not_called()
