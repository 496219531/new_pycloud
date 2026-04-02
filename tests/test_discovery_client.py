from __future__ import annotations

import asyncio
import time
from concurrent import futures
from datetime import datetime, timezone
from typing import Tuple
from unittest.mock import patch

import grpc
import pytest

from pycloud_parallel.controlplane.client import (
    DiscoveryModuleClient,
    DiscoveryServiceClient,
    InfoCenterClient,
    InfoCenterServiceRoute,
    NodeControlClient,
    _CallProxy,
)
from pycloud_parallel.controlplane.server import build_controlplane_server
from pycloud_parallel.controlplane.services import NodeControlService
from pycloud_parallel.controlplane.state import NodeControlState
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc


def _wait_until(predicate, timeout_sec: float = 5.0, interval_sec: float = 0.1) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_sec)
    return False


def _start_nodecontrol_server(node_id: str, artifact_dir: str) -> Tuple[grpc.Server, str, NodeControlState]:
    state = NodeControlState(
        node_id=node_id,
        queue_capacity=32,
        worker_capacity=4,
        artifact_dir=artifact_dir,
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
        monitor_interval_sec=1,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=24))
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, f"127.0.0.1:{port}", state


def _create_exported_service(target: str, service_name: str) -> str:
    blob = (
        b"def pycloud_export(fn):\n"
        b"    fn.__pycloud_export__ = True\n"
        b"    return fn\n\n"
        b"@pycloud_export\n"
        b"def add(value=0, **_kwargs):\n"
        b"    value = int(value)\n"
        b"    return {'value': value, 'plus_one': value + 1}\n\n"
        b"@pycloud_export\n"
        b"def mul(value=0, **_kwargs):\n"
        b"    value = int(value)\n"
        b"    return {'value': value, 'square': value * value}\n"
    )
    with NodeControlClient(target, timeout_sec=10.0) as client:
        session = client.create_service_from_bytes(
            owner_client_id=f"owner-{service_name}",
            service_name=service_name,
            filename=f"{service_name}.py",
            blob=blob,
            runtime="py3",
            entry_module=service_name,
            entry_callable="add",
            worker_count=2,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=True,
        )
    return session.service_id


def _register_node_with_services(
    info_target: str,
    *,
    node_id: str,
    control_addr: str,
    state: NodeControlState,
) -> None:
    with InfoCenterClient(info_target, timeout_sec=10.0) as infocenter:
        infocenter.register_node(
            node_id=node_id,
            control_addr=control_addr,
            capacity=8,
            queue_capacity=64,
            tags=["compute"],
            services=state.service_reports(),
            service_worker_capacity=state.worker_capacity,
            service_worker_used=state.service_worker_used(),
        )


def _demo_route(service_name: str = "svc-demo") -> InfoCenterServiceRoute:
    return InfoCenterServiceRoute(
        service_name=service_name,
        service_id="svc-id-1",
        status=pb2.SERVICE_STATUS_RUNNING,
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        node_healthy=True,
        worker_count=2,
        alive_workers=2,
        in_flight=0,
        lease_expire_at=datetime.now(timezone.utc),
        http_base_url="http://127.0.0.1:18081/svc/svc-id-1",
    )


class TestDiscoveryModuleClient:
    def test_getattr_creates_proxy(self):
        client = DiscoveryModuleClient("127.0.0.1:50051", service_name="svc-demo")
        try:
            client._discovered_methods = ["square", "fibonacci"]
            proxy = client.square
            assert isinstance(proxy, _CallProxy)
            assert proxy._method == "square"
        finally:
            client.close()

    def test_unknown_method_raises(self):
        client = DiscoveryModuleClient("127.0.0.1:50051", service_name="svc-demo")
        try:
            client._discovered_methods = ["square"]
            with pytest.raises(AttributeError, match="has no method 'unknown'"):
                _ = client.unknown
        finally:
            client.close()

    def test_methods_property_uses_discovery_list_methods(self):
        client = DiscoveryModuleClient("127.0.0.1:50051", service_name="svc-demo")
        try:
            with patch.object(
                DiscoveryModuleClient,
                "list_methods",
                return_value=[{"method": "square"}, {"method": "fibonacci"}],
            ) as mocked:
                assert client.methods == ["square", "fibonacci"]
                assert client.methods == ["square", "fibonacci"]
                mocked.assert_called_once_with(include_docs=True)
        finally:
            client.close()

    def test_call_sync(self):
        route = _demo_route()
        client = DiscoveryModuleClient("127.0.0.1:50051", service_name="svc-demo", timeout_sec=9.0)
        try:
            with patch.object(client._route_cache, "select_route", return_value=route), patch(
                "pycloud_parallel.controlplane.client._call_route_http",
                return_value={"ok": True, "data": {"y": 49}},
            ) as mocked:
                result = client.call_sync("square", x=7)
            assert result == {"y": 49}
            mocked.assert_called_once()
        finally:
            client.close()

    def test_async_proxy_call(self):
        route = _demo_route()
        client = DiscoveryModuleClient("127.0.0.1:50051", service_name="svc-demo", timeout_sec=8.0)
        try:
            client._discovered_methods = ["square"]
            with patch.object(client._route_cache, "select_route", return_value=route), patch(
                "pycloud_parallel.controlplane.client._call_route_http",
                return_value={"ok": True, "data": {"y": 64}},
            ):
                async def _run():
                    return await client.square(x=8)

                result = asyncio.run(_run())
            assert result == {"y": 64}
        finally:
            client.close()

    def test_status(self):
        client = DiscoveryModuleClient("127.0.0.1:50051", service_name="svc-demo")
        try:
            with patch.object(
                DiscoveryServiceClient,
                "get_status",
                return_value={"ok": True, "route_count": 1},
            ) as mocked:
                result = client.status()
            assert result == {"ok": True, "route_count": 1}
            mocked.assert_called_once_with(service_name="svc-demo")
        finally:
            client.close()

    def test_broadcast_is_not_supported(self):
        client = DiscoveryModuleClient("127.0.0.1:50051", service_name="svc-demo")
        try:
            client._discovered_methods = ["square"]

            async def _run():
                return await client.square.broadcast(x=7)

            with pytest.raises(NotImplementedError, match="does not support broadcast"):
                asyncio.run(_run())
        finally:
            client.close()


def test_discovery_client_direct_call_roundtrip(tmp_path):
    controlplane = build_controlplane_server("127.0.0.1:0")
    controlplane.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-discovery-01", str(tmp_path / "node_discovery_01"))

    try:
        service_id = _create_exported_service(node_target, "svc_discovery")
        _register_node_with_services(
            controlplane.base_url,
            node_id="node-discovery-01",
            control_addr=node_target,
            state=node_state,
        )

        assert _wait_until(
            lambda: len(
                InfoCenterClient(controlplane.base_url, timeout_sec=5.0).list_service_routes(
                    service_name="svc_discovery",
                    healthy_only=True,
                    limit=20,
                )
            )
            == 1
        )

        with DiscoveryServiceClient(controlplane.base_url, timeout_sec=5.0) as client:
            methods = client.list_methods(service_name="svc_discovery", include_docs=False)
            assert sorted(item["method"] for item in methods) == ["add", "mul"]

            body = client.call(service_name="svc_discovery", method="mul", payload={"value": 6}, timeout_sec=5.0)
            assert body["data"]["square"] == 36

            status = client.get_status(service_name="svc_discovery")
            assert status["route_count"] == 1
            assert status["routes"][0]["service_id"] == service_id

        module_client = DiscoveryModuleClient(controlplane.base_url, service_name="svc_discovery", timeout_sec=5.0)
        try:
            assert module_client.methods == ["add", "mul"]
            assert module_client.call_sync("add", value=10) == {"value": 10, "plus_one": 11}

            async def _call_discovery_module():
                return await module_client.mul(value=8)

            assert asyncio.run(_call_discovery_module()) == {"value": 8, "square": 64}
        finally:
            module_client.close()
    finally:
        node_server.stop(grace=0)
        node_state.close()
        controlplane.stop()


def test_discovery_client_retries_second_route_when_first_route_is_broken(tmp_path):
    controlplane = build_controlplane_server("127.0.0.1:0", gateway_failure_threshold=1, gateway_open_sec=1.0)
    controlplane.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-discovery-good", str(tmp_path / "node_discovery_good"))

    try:
        service_id = _create_exported_service(node_target, "svc_discovery_failover")
        _register_node_with_services(
            controlplane.base_url,
            node_id="node-discovery-good",
            control_addr=node_target,
            state=node_state,
        )

        bad_route = pb2.ServiceRouteReport(
            service_name="svc_discovery_failover",
            service_id="svc-discovery-bad",
            status=pb2.SERVICE_STATUS_RUNNING,
            worker_count=4,
            alive_workers=4,
            in_flight=0,
            http_base_url="http://127.0.0.1:1/svc/svc-discovery-bad",
        )
        with InfoCenterClient(controlplane.base_url, timeout_sec=10.0) as infocenter:
            infocenter.register_node(
                node_id="node-aaa-discovery-bad",
                control_addr="127.0.0.1:1",
                capacity=8,
                queue_capacity=64,
                tags=["compute"],
                services=[bad_route],
                service_worker_capacity=8,
                service_worker_used=4,
            )

        assert _wait_until(
            lambda: len(
                InfoCenterClient(controlplane.base_url, timeout_sec=5.0).list_service_routes(
                    service_name="svc_discovery_failover",
                    healthy_only=True,
                    limit=20,
                )
            )
            == 2
        )

        with DiscoveryServiceClient(controlplane.base_url, timeout_sec=5.0, failure_threshold=1, open_sec=1.0) as client:
            body = client.call(
                service_name="svc_discovery_failover",
                method="mul",
                payload={"value": 11},
                timeout_sec=5.0,
            )
            assert body["data"]["square"] == 121
            status = client.get_status(service_name="svc_discovery_failover")
            route_map = {item["service_id"]: item for item in status["routes"]}
            assert service_id in route_map
            assert "svc-discovery-bad" in route_map
    finally:
        node_server.stop(grace=0)
        node_state.close()
        controlplane.stop()
