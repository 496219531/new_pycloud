"""Tests for GatewayServiceClient."""

from unittest.mock import patch

import pytest

from pycloud_parallel.controlplane.serialization import INLINE_PAYLOAD_HARD_LIMIT_BYTES


class TestGatewayServiceClient:
    def test_call_sync_like_usage(self):
        from pycloud_parallel.controlplane.client import GatewayServiceClient

        client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=9.0)
        with patch(
            "pycloud_parallel.controlplane.gateway_client.client_mod._http_json_request",
            return_value={"ok": True, "data": {"y": 49}},
        ) as mocked:
            result = client.call(service_name="svc-demo", method="square", payload={"x": 7}, timeout_sec=9.0)

        assert result == {"ok": True, "data": {"y": 49}}
        assert mocked.call_count == 2

    def test_list_methods(self):
        from pycloud_parallel.controlplane.client import GatewayServiceClient

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
        from pycloud_parallel.controlplane.client import GatewayServiceClient

        client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0)
        with patch(
            "pycloud_parallel.controlplane.gateway_client.client_mod._http_json_request",
            return_value={"ok": True, "route_count": 1},
        ) as mocked:
            result = client.get_status(service_name="svc-demo")

        assert result == {"ok": True, "route_count": 1}
        mocked.assert_called_once()


def test_gateway_service_client_rejects_oversized_inline_payload_before_http():
    from pycloud_parallel.controlplane.client import GatewayServiceClient

    payload = {"blob": "x" * (INLINE_PAYLOAD_HARD_LIMIT_BYTES + 1024)}
    client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0)
    with patch("pycloud_parallel.controlplane.gateway_client.client_mod._http_json_request") as mocked:
        with pytest.raises(ValueError, match="DataRef"):
            client.call(service_name="svc-demo", method="run", payload=payload, timeout_sec=5.0)
    assert mocked.call_count == 1
    assert mocked.call_args.kwargs["path"] == "/svc/svc-demo/status"
