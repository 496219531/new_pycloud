"""Tests for GatewayConnect."""

import asyncio
from unittest.mock import patch

import pytest

from pycloud_parallel.controlplane.serialization import INLINE_PAYLOAD_HARD_LIMIT_BYTES


class TestGatewayConnect:
    def test_getattr_creates_proxy(self):
        from pycloud_parallel.controlplane.client import GatewayConnect, _CallProxy

        client = GatewayConnect("127.0.0.1:50051", service_name="svc-demo", validate_on_init=False)
        client._discovered_methods = ["square", "fibonacci"]

        proxy = client.square

        assert isinstance(proxy, _CallProxy)
        assert proxy._method == "square"

    def test_unknown_method_raises(self):
        from pycloud_parallel.controlplane.client import GatewayConnect

        client = GatewayConnect("127.0.0.1:50051", service_name="svc-demo", validate_on_init=False)
        client._discovered_methods = ["square"]

        with pytest.raises(AttributeError, match="has no method 'unknown'"):
            _ = client.unknown

    def test_methods_property_uses_gateway_list_methods(self):
        from pycloud_parallel.controlplane.client import GatewayConnect

        client = GatewayConnect("127.0.0.1:50051", service_name="svc-demo", validate_on_init=False)
        with patch(
            "pycloud_parallel.controlplane.client.GatewayServiceClient.list_methods",
            return_value=[
                {"method": "square"},
                {"method": "fibonacci"},
            ],
        ) as mocked:
            assert client.methods == ["square", "fibonacci"]
            assert client.methods == ["square", "fibonacci"]
            mocked.assert_called_once_with(service_name="svc-demo", include_docs=True)

    def test_call_sync(self):
        from pycloud_parallel.controlplane.client import GatewayConnect

        client = GatewayConnect("127.0.0.1:50051", service_name="svc-demo", timeout_sec=9.0, validate_on_init=False)
        with patch(
            "pycloud_parallel.controlplane.client.GatewayServiceClient.call",
            return_value={"ok": True, "data": {"y": 49}},
        ) as mocked:
            result = client.call_sync("square", x=7)

        assert result == {"y": 49}
        mocked.assert_called_once_with(
            service_name="svc-demo",
            method="square",
            payload={"x": 7},
            timeout_sec=9.0,
        )

    def test_async_proxy_call(self):
        from pycloud_parallel.controlplane.client import GatewayConnect

        client = GatewayConnect("127.0.0.1:50051", service_name="svc-demo", timeout_sec=8.0, validate_on_init=False)
        client._discovered_methods = ["square"]
        with patch(
            "pycloud_parallel.controlplane.client.GatewayServiceClient.call",
            return_value={"ok": True, "data": {"y": 64}},
        ) as mocked:
            async def _run():
                return await client.square(x=8)

            result = asyncio.run(_run())

        assert result == {"y": 64}
        mocked.assert_called_once_with(
            service_name="svc-demo",
            method="square",
            payload={"x": 8},
            timeout_sec=8.0,
        )

    def test_status(self):
        from pycloud_parallel.controlplane.client import GatewayConnect

        client = GatewayConnect("127.0.0.1:50051", service_name="svc-demo", validate_on_init=False)
        with patch(
            "pycloud_parallel.controlplane.client.GatewayServiceClient.get_status",
            return_value={"ok": True, "route_count": 1},
        ) as mocked:
            result = client.status()

        assert result == {"ok": True, "route_count": 1}
        mocked.assert_called_once_with(service_name="svc-demo")

    def test_broadcast_is_not_supported(self):
        from pycloud_parallel.controlplane.client import GatewayConnect

        client = GatewayConnect("127.0.0.1:50051", service_name="svc-demo", validate_on_init=False)
        client._discovered_methods = ["square"]

        async def _run():
            return await client.square.broadcast(x=7)

        with pytest.raises(NotImplementedError, match="does not support broadcast"):
            asyncio.run(_run())

    def test_init_raises_when_no_available_route(self):
        from pycloud_parallel.controlplane.client import GatewayConnect

        with patch(
            "pycloud_parallel.controlplane.client.GatewayServiceClient.get_status",
            return_value={"ok": True, "route_count": 0},
        ):
            with pytest.raises(RuntimeError, match="no available route"):
                GatewayConnect("127.0.0.1:50051", service_name="svc-demo")

    def test_methods_raise_clear_error_when_service_has_no_exported_methods(self):
        from pycloud_parallel.controlplane.client import GatewayConnect

        with patch(
            "pycloud_parallel.controlplane.client.GatewayServiceClient.get_status",
            return_value={"ok": True, "route_count": 1},
        ):
            client = GatewayConnect("127.0.0.1:50051", service_name="svc-demo")

        with patch(
            "pycloud_parallel.controlplane.client.GatewayServiceClient.list_methods",
            return_value=[],
        ):
            with patch(
                "pycloud_parallel.controlplane.client.GatewayServiceClient.get_status",
                return_value={"ok": True, "route_count": 1},
            ):
                with pytest.raises(RuntimeError, match="no exported methods"):
                    _ = client.methods


def test_gateway_service_client_rejects_oversized_inline_payload_before_http():
    from pycloud_parallel.controlplane.client import GatewayServiceClient

    payload = {"blob": "x" * (INLINE_PAYLOAD_HARD_LIMIT_BYTES + 1024)}
    client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0)
    with patch("pycloud_parallel.controlplane.client._http_json_request") as mocked:
        with pytest.raises(ValueError, match="ObjectRef"):
            client.call(service_name="svc-demo", method="run", payload=payload, timeout_sec=5.0)
    assert mocked.call_count == 1
    assert mocked.call_args.kwargs["path"] == "/svc/svc-demo/status"
