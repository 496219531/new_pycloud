from __future__ import annotations

import asyncio
import shutil
import time
from concurrent import futures
from dataclasses import replace
from datetime import datetime, timezone
from typing import Tuple
from unittest.mock import patch

import grpc
import pytest

from pycloud_parallel import Service
from pycloud_parallel.controlplane import client_transport as client_transport_mod
from pycloud_parallel.controlplane.discovery_client import DiscoveryServiceClient
from pycloud_parallel.controlplane.discovery_route_cache import _DiscoveryRouteCache, _ServiceRouteSnapshot
from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient, InfoCenterServiceRoute
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.execution.call_proxy import _CallProxy
from pycloud_parallel.controlplane.data_ref import DataRef
from pycloud_parallel.controlplane.server import build_controlplane_server
from pycloud_parallel.controlplane.services import NodeControlService
from pycloud_parallel.controlplane.node.state import NodeControlState
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc

DiscoveryCallError = client_transport_mod.DiscoveryCallError


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
            services=state.service_report_payloads(),
            service_worker_capacity=state.worker_capacity,
            service_worker_used=state.service_worker_used(),
        )


def _demo_route(service_name: str = "svc-demo") -> InfoCenterServiceRoute:
    return InfoCenterServiceRoute(
        service_name=service_name,
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
    )


def _connect_discovery_service(
    target: str = "127.0.0.1:50051",
    *,
    service_name: str = "svc-demo",
    timeout_sec: float = 10.0,
    validate_on_init: bool = True,
):
    return Service.connect(
        target=target,
        service_name=service_name,
        timeout_sec=timeout_sec,
        transport="discovery",
        validate_on_init=validate_on_init,
    )


def test_upload_object_recreates_missing_object_dir(tmp_path):
    server, target, state = _start_nodecontrol_server("node-object-01", str(tmp_path / "node_object_01"))
    try:
        shutil.rmtree(state.object_dir, ignore_errors=True)
        assert not state.object_dir.exists()

        with NodeControlClient(target, timeout_sec=10.0) as client:
            ref = client.upload_object_from_bytes(blob=b"hello object", format="bin")
            restored = client.download_object_bytes(object_id=ref.object_id)

        assert restored == b"hello object"
        assert state.object_dir.exists()
    finally:
        server.stop(grace=0)
        state.close()


def test_upload_object_from_file_uses_trusted_precheck_by_default(tmp_path):
    server, target, state = _start_nodecontrol_server("node-object-precheck-01", str(tmp_path / "node_object_precheck_01"))
    try:
        upload_path = tmp_path / "dup.bin"
        upload_path.write_bytes(b"duplicate object payload")
        with NodeControlClient(target, timeout_sec=10.0) as client:
            first = client.upload_object_from_file(file_path=str(upload_path), format="bin")
            meta = client.get_object_meta(object_id=first.object_id)
            assert meta.exists is True
            with patch.object(client.stub, "UploadObject", wraps=client.stub.UploadObject) as mocked_upload:
                second = client.upload_object_from_file(file_path=str(upload_path), format="bin")
            assert second.object_id == first.object_id
            assert second.size_bytes == first.size_bytes
            mocked_upload.assert_not_called()
    finally:
        server.stop(grace=0)
        state.close()


def test_upload_object_from_file_can_disable_trusted_precheck(tmp_path):
    server, target, state = _start_nodecontrol_server("node-object-precheck-02", str(tmp_path / "node_object_precheck_02"))
    try:
        upload_path = tmp_path / "dup-legacy.bin"
        upload_path.write_bytes(b"duplicate object payload legacy")
        with NodeControlClient(target, timeout_sec=10.0) as client:
            first = client.upload_object_from_file(file_path=str(upload_path), format="bin")
            assert client.has_object(object_id=first.object_id) is True
            with (
                patch.object(client.stub, "GetObjectMeta", wraps=client.stub.GetObjectMeta) as mocked_meta,
                patch.object(client.stub, "UploadObject", wraps=client.stub.UploadObject) as mocked_upload,
            ):
                second = client.upload_object_from_file(
                    file_path=str(upload_path),
                    format="bin",
                    trusted_precheck=False,
                )
            assert second.object_id == first.object_id
            mocked_meta.assert_not_called()
            assert mocked_upload.call_count == 1
    finally:
        server.stop(grace=0)
        state.close()


class TestDiscoveryConnectedService:
    def test_getattr_creates_proxy(self):
        client = _connect_discovery_service(validate_on_init=False)
        try:
            client._discovered_methods = ["square", "fibonacci"]
            proxy = client.square
            assert isinstance(proxy, _CallProxy)
            assert proxy._method == "square"
            assert proxy._strategy == "predicted_busy"
        finally:
            client.close()

    def test_unknown_method_raises(self):
        client = _connect_discovery_service(validate_on_init=False)
        try:
            client._discovered_methods = ["square"]
            with pytest.raises(AttributeError, match="has no method 'unknown'"):
                _ = client.unknown
        finally:
            client.close()

    def test_methods_property_uses_discovery_list_methods(self):
        client = _connect_discovery_service(validate_on_init=False)
        try:
            with patch.object(
                type(client),
                "list_methods",
                return_value=[{"method": "square"}, {"method": "fibonacci"}],
            ) as mocked:
                assert client.methods == ["square", "fibonacci"]
                assert client.methods == ["square", "fibonacci"]
                mocked.assert_called_once_with(include_docs=True)
        finally:
            client.close()

    def test_methods_fail_over_to_retry_route_when_primary_route_is_stale(self):
        primary = _demo_route()
        retry = replace(
            primary,
            service_id="svc-id-2",
            node_instance_id="node-2-inst",
            node_id="node-2",
            control_addr="127.0.0.1:50062",
            http_base_url="http://127.0.0.1:18082/svc/svc-id-2",
        )
        client = _connect_discovery_service(validate_on_init=False)
        try:
            with (
                patch.object(client._route_cache, "select_route", side_effect=[primary, retry]) as mocked_select,
                patch.object(client._route_cache, "refresh", return_value=[retry]) as mocked_refresh,
                patch.object(type(client), "_list_methods_via_route", side_effect=[RuntimeError("stale route"), [{"method": "square"}]]),
            ):
                assert client.methods == ["square"]
            assert mocked_select.call_count == 2
            mocked_refresh.assert_called_once_with("svc-demo", force=True)
        finally:
            client.close()

    def test_call_sync(self):
        route = _demo_route()
        client = _connect_discovery_service(timeout_sec=9.0, validate_on_init=False)
        try:
            with patch.object(client._route_cache, "select_route", return_value=route), patch(
                "pycloud_parallel.controlplane.discovery_client.client_mod._call_route_http",
                return_value={"ok": True, "data": {"y": 49}},
            ) as mocked:
                result = client.call_sync("square", x=7)
            assert result == {"y": 49}
            mocked.assert_called_once()
        finally:
            client.close()

    def test_async_proxy_call(self):
        route = _demo_route()
        client = _connect_discovery_service(timeout_sec=8.0, validate_on_init=False)
        try:
            client._discovered_methods = ["square"]
            with patch.object(client._route_cache, "select_route", return_value=route), patch(
                "pycloud_parallel.controlplane.discovery_client.client_mod._call_route_http",
                return_value={"ok": True, "data": {"y": 64}},
            ):
                async def _run():
                    return await client.square(x=8)

                result = asyncio.run(_run())
            assert result == {"y": 64}
        finally:
            client.close()

    def test_large_payload_upload_targets_selected_route_only(self, monkeypatch):
        primary = _demo_route()

        uploads = []

        def fake_estimate(value):
            return 600000 if isinstance(value, str) else 16

        def fake_put(clients, data, *, format="", chunk_size=0):
            uploads.append([client.target for client in clients])
            return DataRef(
                ref_id="sha256:" + ("a" * 64),
                storage_id="sha256:" + ("a" * 64),
                logical_type="bytes",
                format=format or "bin",
                size_bytes=2048,
                materialize_as="bytes",
                locator_kind="node_local",
                locator_token="",
            )

        client = _connect_discovery_service(timeout_sec=8.0, validate_on_init=False)
        try:
            monkeypatch.setattr("pycloud_parallel.controlplane.remote_payload._estimate_managed_global_inline_size", fake_estimate)
            monkeypatch.setattr("pycloud_parallel.controlplane.remote_payload._put_data_via_clients", fake_put)
            with patch.object(client._route_cache, "select_route", return_value=primary), patch(
                "pycloud_parallel.controlplane.discovery_client.client_mod._call_route_http",
                return_value={"ok": True, "data": {"y": 81}},
            ):
                result = client.call_sync("square", blob="x" * 2048)
            assert result == {"y": 81}
            assert uploads == [[primary.control_addr]]
        finally:
            client.close()

    def test_large_payload_retry_uploads_only_to_retry_route(self, monkeypatch):
        primary = _demo_route()
        retry = replace(
            primary,
            service_id="svc-id-2",
            node_instance_id="node-2-inst",
            node_id="node-2",
            control_addr="127.0.0.1:50062",
            http_base_url="http://127.0.0.1:18082/svc/svc-id-2",
        )

        uploads = []

        def fake_estimate(value):
            return 600000 if isinstance(value, str) else 16

        def fake_put(clients, data, *, format="", chunk_size=0):
            uploads.append([client.target for client in clients])
            return DataRef(
                ref_id="sha256:" + ("b" * 64),
                storage_id="sha256:" + ("b" * 64),
                logical_type="bytes",
                format=format or "bin",
                size_bytes=2048,
                materialize_as="bytes",
                locator_kind="node_local",
                locator_token="",
            )

        def fake_call(route, *, method, payload, timeout_sec, service_token):
            if route.service_id == primary.service_id:
                raise DiscoveryCallError(status_code=502, data={"ok": False, "error": "primary failed"})
            return {"ok": True, "data": {"y": 100}}

        client = _connect_discovery_service(timeout_sec=8.0, validate_on_init=False)
        try:
            monkeypatch.setattr("pycloud_parallel.controlplane.remote_payload._estimate_managed_global_inline_size", fake_estimate)
            monkeypatch.setattr("pycloud_parallel.controlplane.remote_payload._put_data_via_clients", fake_put)
            with patch.object(client._route_cache, "select_route", side_effect=[primary, retry]), patch.object(
                client._route_cache,
                "refresh",
                return_value=[retry],
            ), patch(
                "pycloud_parallel.controlplane.discovery_client.client_mod._call_route_http",
                side_effect=fake_call,
            ):
                result = client.call_sync("square", blob="x" * 2048)
            assert result == {"y": 100}
            assert uploads == [[primary.control_addr], [retry.control_addr]]
        finally:
            client.close()

    def test_status(self):
        client = _connect_discovery_service(validate_on_init=False)
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
        client = _connect_discovery_service(validate_on_init=False)
        try:
            client._discovered_methods = ["square"]

            async def _run():
                return await client.square.broadcast(x=7)

            with pytest.raises(NotImplementedError, match="does not support broadcast"):
                asyncio.run(_run())
        finally:
            client.close()

    def test_init_raises_when_no_available_route(self):
        with patch.object(DiscoveryServiceClient, "refresh_routes", return_value=[]), patch.object(
            DiscoveryServiceClient,
            "get_status",
            return_value={"ok": True, "route_count": 0},
        ):
            with pytest.raises(RuntimeError, match="no available route"):
                _connect_discovery_service()

    def test_methods_raise_clear_error_when_service_has_no_exported_methods(self):
        with patch.object(DiscoveryServiceClient, "refresh_routes", return_value=[object()]), patch.object(
            DiscoveryServiceClient,
            "get_status",
            return_value={"ok": True, "route_count": 1},
        ):
            client = _connect_discovery_service()
        try:
            with patch.object(
                type(client),
                "list_methods",
                return_value=[],
            ):
                with patch.object(DiscoveryServiceClient, "refresh_routes", return_value=[object()]), patch.object(
                    DiscoveryServiceClient,
                    "get_status",
                    return_value={"ok": True, "route_count": 1},
                ):
                    with pytest.raises(RuntimeError, match="no exported methods"):
                        _ = client.methods
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
            assert "predicted_busy" in status["routes"][0]

        module_client = _connect_discovery_service(
            controlplane.base_url,
            service_name="svc_discovery",
            timeout_sec=5.0,
        )
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


def test_discovery_client_fetches_large_dataframe_result(tmp_path):
    pytest.importorskip("pyarrow")
    pd = pytest.importorskip("pandas")

    controlplane = build_controlplane_server("127.0.0.1:0")
    controlplane.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-discovery-large-01", str(tmp_path / "node_discovery_large_01"))

    try:
        blob = (
            b"import pandas as pd\n\n"
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    rows = 100000\n"
            b"    return pd.DataFrame({'x': list(range(value, value + rows)), 'tag': ['x' * 128] * rows})\n"
        )
        with NodeControlClient(node_target, timeout_sec=10.0) as client:
            session = client.create_service_from_bytes(
                owner_client_id="owner-svc-discovery-large",
                service_name="svc_discovery_large_result",
                blob=blob,
                runtime="py3",
                entry_module="svc_discovery_large_result",
                entry_callable="run",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=True,
            )
            assert session.service_id
        _register_node_with_services(
            controlplane.base_url,
            node_id="node-discovery-large-01",
            control_addr=node_target,
            state=node_state,
        )

        assert _wait_until(
            lambda: len(
                InfoCenterClient(controlplane.base_url, timeout_sec=5.0).list_service_routes(
                    service_name="svc_discovery_large_result",
                    healthy_only=True,
                    limit=20,
                )
            )
            == 1
        )

        with DiscoveryServiceClient(controlplane.base_url, timeout_sec=5.0) as client:
            body = client.call(
                service_name="svc_discovery_large_result",
                method="run",
                payload={"value": 4},
                timeout_sec=5.0,
            )
            assert body["ok"] is True
            assert isinstance(body["data"], DataRef)
            assert body["data"].node_id == "node-discovery-large-01"
            assert body["data"].locator_kind == "controlplane"
            assert body["data"].locator_token == controlplane.base_url
            assert body["data"].control_addr == ""
            frame = client.fetch_result_data(body)
            assert isinstance(frame, pd.DataFrame)
            assert list(frame["x"].head(3)) == [4, 5, 6]
            assert frame["tag"].iloc[0] == "x" * 128
            assert len(frame) == 100000
    finally:
        node_server.stop(grace=0)
        node_state.close()
        controlplane.stop()


def test_discovery_route_cache_defaults_to_predicted_busy():
    cache = _DiscoveryRouteCache(infocenter_target="127.0.0.1:50051", timeout_sec=5.0)
    try:
        cache._snapshots["svc-demo"] = _ServiceRouteSnapshot(  # noqa: SLF001
            service_name="svc-demo",
            routes=[
                InfoCenterServiceRoute(
                    service_name="svc-demo",
                    service_id="svc-low-inflight",
                    status=pb2.SERVICE_STATUS_RUNNING,
                    node_instance_id="node-1-inst",
                    node_id="node-1",
                    control_addr="127.0.0.1:50061",
                    node_healthy=True,
                    worker_count=2,
                    alive_workers=2,
                    in_flight=1,
                    lease_expire_at=datetime.now(timezone.utc),
                    http_base_url="http://127.0.0.1:18081/svc/svc-low-inflight",
                    predicted_busy=40.0,
                ),
                InfoCenterServiceRoute(
                    service_name="svc-demo",
                    service_id="svc-low-predicted",
                    status=pb2.SERVICE_STATUS_RUNNING,
                    node_instance_id="node-2-inst",
                    node_id="node-2",
                    control_addr="127.0.0.1:50062",
                    node_healthy=True,
                    worker_count=2,
                    alive_workers=2,
                    in_flight=3,
                    lease_expire_at=datetime.now(timezone.utc),
                    http_base_url="http://127.0.0.1:18082/svc/svc-low-predicted",
                    predicted_busy=12.0,
                ),
            ],
        )
        assert cache.select_route("svc-demo").service_id == "svc-low-predicted"
        assert cache.select_route("svc-demo", strategy="least_inflight").service_id == "svc-low-inflight"
    finally:
        cache.stop()


def test_discovery_service_client_call_uses_http_payload_policy(monkeypatch):
    route = _demo_route()
    captured = {}

    class _FakeNodeControlClient:
        def __init__(self, target: str, timeout_sec: float = 10.0) -> None:
            del timeout_sec
            self.target = target

        def close(self) -> None:
            return None

    def _fake_prepare(payload, *, put_data, estimate_inline_size, policy):
        del put_data, estimate_inline_size
        captured["mode"] = policy.mode
        captured["preserve_args_kwargs_container"] = policy.preserve_args_kwargs_container
        return dict(payload or {})

    monkeypatch.setattr("pycloud_parallel.controlplane.discovery_client.client_mod.NodeControlClient", _FakeNodeControlClient)
    monkeypatch.setattr("pycloud_parallel.controlplane.remote_payload.prepare_outbound_payload", _fake_prepare)
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.discovery_client.client_mod._call_route_http",
        lambda route, *, method, payload, timeout_sec, service_token: {"ok": True, "data": payload},
    )

    client = DiscoveryServiceClient("127.0.0.1:50051", timeout_sec=8.0)
    try:
        monkeypatch.setattr(client._route_cache, "select_route", lambda *args, **kwargs: route)
        monkeypatch.setattr(client._route_cache, "get_routes", lambda *args, **kwargs: [route])
        monkeypatch.setattr(client._route_cache, "mark_success", lambda *args, **kwargs: None)

        result = client.call(service_name="svc-demo", method="square", payload={"args": [7], "kwargs": {"x": 8}})
        assert result == {"ok": True, "data": {"args": [7], "kwargs": {"x": 8}}}
        assert captured["mode"] == "http_call"
        assert captured["preserve_args_kwargs_container"] is True
    finally:
        client.close()
