"""Tests for GatewayConnect."""

import asyncio
from unittest.mock import patch

import pytest


class TestGatewayConnect:
    def test_getattr_creates_proxy(self):
        from pycloud_parallel.controlplane.client import GatewayConnect, _CallProxy

        client = GatewayConnect("127.0.0.1:50051", service_name="svc-demo")
        client._discovered_methods = ["square", "fibonacci"]

        proxy = client.square

        assert isinstance(proxy, _CallProxy)
        assert proxy._method == "square"

    def test_unknown_method_raises(self):
        from pycloud_parallel.controlplane.client import GatewayConnect

        client = GatewayConnect("127.0.0.1:50051", service_name="svc-demo")
        client._discovered_methods = ["square"]

        with pytest.raises(AttributeError, match="has no method 'unknown'"):
            _ = client.unknown

    def test_methods_property_uses_gateway_list_methods(self):
        from pycloud_parallel.controlplane.client import GatewayConnect

        client = GatewayConnect("127.0.0.1:50051", service_name="svc-demo")
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

        client = GatewayConnect("127.0.0.1:50051", service_name="svc-demo", timeout_sec=9.0)
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

        client = GatewayConnect("127.0.0.1:50051", service_name="svc-demo", timeout_sec=8.0)
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

        client = GatewayConnect("127.0.0.1:50051", service_name="svc-demo")
        with patch(
            "pycloud_parallel.controlplane.client.GatewayServiceClient.get_status",
            return_value={"ok": True, "route_count": 1},
        ) as mocked:
            result = client.status()

        assert result == {"ok": True, "route_count": 1}
        mocked.assert_called_once_with(service_name="svc-demo")

    def test_broadcast_is_not_supported(self):
        from pycloud_parallel.controlplane.client import GatewayConnect

        client = GatewayConnect("127.0.0.1:50051", service_name="svc-demo")
        client._discovered_methods = ["square"]

        async def _run():
            return await client.square.broadcast(x=7)

        with pytest.raises(NotImplementedError, match="does not support broadcast"):
            asyncio.run(_run())
