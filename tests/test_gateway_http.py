from __future__ import annotations

from concurrent import futures
from datetime import datetime, timezone
import json
import time
from typing import Tuple
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import grpc
import pytest

from pycloud_parallel.controlplane.client import GatewayConnect, GatewayServiceClient, InfoCenterClient, InfoCenterServiceRoute, NodeControlClient
from pycloud_parallel.controlplane.gateway_cache import GatewayRouteCache
from pycloud_parallel.controlplane.data_ref import DataRef
from pycloud_parallel.controlplane.server import (
    build_controlplane_server,
    build_gateway_server,
    build_infocenter_server,
    build_job_orchestrator_server,
)
from pycloud_parallel.controlplane.services import NodeControlService
from pycloud_parallel.controlplane.state import NodeControlState
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc


def _http_json(method: str, url: str, payload: dict | None = None) -> tuple[int, dict]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    req = Request(
        url=url,
        method=method,
        headers={"Content-Type": "application/json"},
        data=body,
    )
    with urlopen(req, timeout=10.0) as resp:
        return int(resp.status), json.loads(resp.read().decode("utf-8") or "{}")


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


def test_controlplane_embeds_gateway_for_service_calls(tmp_path):
    controlplane = build_controlplane_server("127.0.0.1:0")
    controlplane.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-gw-01", str(tmp_path / "node_gw_01"))

    try:
        service_id = _create_exported_service(node_target, "svc_gateway_controlplane")
        _register_node_with_services(
            controlplane.base_url,
            node_id="node-gw-01",
            control_addr=node_target,
            state=node_state,
        )

        assert _wait_until(
            lambda: len(
                InfoCenterClient(controlplane.base_url, timeout_sec=5.0).list_service_routes(
                    service_name="svc_gateway_controlplane",
                    healthy_only=True,
                    limit=20,
                )
            )
            == 1
        )

        code, call_resp = _http_json(
            "POST",
            f"{controlplane.base_url}/svc/svc_gateway_controlplane/call/add",
            {"value": 7},
        )
        assert code == 200
        assert call_resp["ok"] is True
        assert call_resp["data"]["plus_one"] == 8

        code, methods_resp = _http_json(
            "GET",
            f"{controlplane.base_url}/svc/svc_gateway_controlplane/methods?include_docs=false",
        )
        assert code == 200
        assert sorted(item["method"] for item in methods_resp["methods"]) == ["add", "mul"]
        assert methods_resp["service_id"] == service_id

        code, status_resp = _http_json(
            "GET",
            f"{controlplane.base_url}/svc/svc_gateway_controlplane/status",
        )
        assert code == 200
        assert status_resp["ok"] is True
        assert status_resp["service_name"] == "svc_gateway_controlplane"
        assert status_resp["route_count"] == 1
        assert status_resp["routes"][0]["service_id"] == service_id
        assert "predicted_busy" in status_resp["routes"][0]

        with GatewayServiceClient(controlplane.base_url, timeout_sec=5.0) as gateway:
            methods = gateway.list_methods(service_name="svc_gateway_controlplane", include_docs=False)
            assert sorted(item["method"] for item in methods) == ["add", "mul"]
            body = gateway.call(service_name="svc_gateway_controlplane", method="mul", payload={"value": 6}, timeout_sec=5.0)
            assert body["data"]["square"] == 36

        module_client = GatewayConnect(
            controlplane.base_url,
            service_name="svc_gateway_controlplane",
            timeout_sec=5.0,
        )
        assert module_client.methods == ["add", "mul"]
        assert module_client.call_sync("add", value=10) == {"value": 10, "plus_one": 11}

        async def _call_gateway_module():
            return await module_client.mul(value=8)

        import asyncio

        assert asyncio.run(_call_gateway_module()) == {"value": 8, "square": 64}
    finally:
        node_server.stop(grace=0)
        node_state.close()
        controlplane.stop()


def test_gateway_route_cache_defaults_to_predicted_busy():
    class _StaticSource:
        def __init__(self, routes):
            self._routes = list(routes)

        def list_service_routes(self, *, service_name: str, healthy_only: bool, limit: int):
            del service_name, healthy_only, limit
            return list(self._routes)

    routes = [
        InfoCenterServiceRoute(
            service_name="svc-gateway-cache",
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
            predicted_busy=24.0,
        ),
        InfoCenterServiceRoute(
            service_name="svc-gateway-cache",
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
            predicted_busy=8.0,
        ),
    ]
    cache = GatewayRouteCache(source=_StaticSource(routes), refresh_interval_sec=60.0)
    try:
        assert cache.select_route("svc-gateway-cache", force_refresh=True).service_id == "svc-low-predicted"
    finally:
        cache.stop()


def test_standalone_gateway_reads_routes_from_infocenter(tmp_path):
    infocenter = build_infocenter_server("127.0.0.1:0")
    infocenter.start()
    gateway = build_gateway_server("127.0.0.1:0", infocenter_addr=infocenter.base_url)
    gateway.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-gw-02", str(tmp_path / "node_gw_02"))

    try:
        service_id = _create_exported_service(node_target, "svc_gateway_remote")
        _register_node_with_services(
            infocenter.base_url,
            node_id="node-gw-02",
            control_addr=node_target,
            state=node_state,
        )

        assert _wait_until(
            lambda: len(
                InfoCenterClient(infocenter.base_url, timeout_sec=5.0).list_service_routes(
                    service_name="svc_gateway_remote",
                    healthy_only=True,
                    limit=20,
                )
            )
            == 1
        )

        code, call_resp = _http_json(
            "POST",
            f"{gateway.base_url}/svc/svc_gateway_remote/call/mul",
            {"value": 9},
        )
        assert code == 200
        assert call_resp["ok"] is True
        assert call_resp["data"]["square"] == 81

        code, methods_resp = _http_json(
            "GET",
            f"{gateway.base_url}/svc/svc_gateway_remote/methods",
        )
        assert code == 200
        assert sorted(item["method"] for item in methods_resp["methods"]) == ["add", "mul"]
        assert methods_resp["service_id"] == service_id
    finally:
        node_server.stop(grace=0)
        node_state.close()
        gateway.stop()
        infocenter.stop()


def test_gateway_service_client_fetches_large_dataframe_result(tmp_path):
    pytest.importorskip("pyarrow")
    pd = pytest.importorskip("pandas")

    controlplane = build_controlplane_server("127.0.0.1:0")
    controlplane.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-gw-large-01", str(tmp_path / "node_gw_large_01"))

    try:
        blob = (
            b"import pandas as pd\n\n"
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return pd.DataFrame([{'x': value}, {'x': value + 1}])\n"
        )
        with NodeControlClient(node_target, timeout_sec=10.0) as client:
            session = client.create_service_from_bytes(
                owner_client_id="owner-svc-gateway-large",
                service_name="svc_gateway_large_result",
                blob=blob,
                runtime="py3",
                entry_module="svc_gateway_large_result",
                entry_callable="run",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=True,
            )
            service_id = session.service_id
        _register_node_with_services(
            controlplane.base_url,
            node_id="node-gw-large-01",
            control_addr=node_target,
            state=node_state,
        )

        assert _wait_until(
            lambda: len(
                InfoCenterClient(controlplane.base_url, timeout_sec=5.0).list_service_routes(
                    service_name="svc_gateway_large_result",
                    healthy_only=True,
                    limit=20,
                )
            )
            == 1
        )

        with GatewayServiceClient(controlplane.base_url, timeout_sec=5.0) as gateway:
            body = gateway.call(
                service_name="svc_gateway_large_result",
                method="run",
                payload={"value": 11},
                timeout_sec=5.0,
            )
            assert body["ok"] is True
            if isinstance(body["data"], DataRef):
                assert body["data"].node_id == "node-gw-large-01"
                assert body["data"].locator_kind == "controlplane"
                assert body["data"].locator_token == controlplane.base_url
                assert body["data"].control_addr == ""
            else:
                assert isinstance(body["data"], pd.DataFrame)
            frame = gateway.fetch_result_data(body)
            assert isinstance(frame, pd.DataFrame)
            assert list(frame["x"]) == [11, 12]

        assert service_id
    finally:
        node_server.stop(grace=0)
        node_state.close()
        controlplane.stop()


def test_gateway_retries_second_route_when_first_route_is_broken(tmp_path):
    controlplane = build_controlplane_server("127.0.0.1:0", gateway_failure_threshold=1, gateway_open_sec=1.0)
    controlplane.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-good", str(tmp_path / "node_good"))

    try:
        service_id = _create_exported_service(node_target, "svc_gateway_failover")
        _register_node_with_services(
            controlplane.base_url,
            node_id="node-good",
            control_addr=node_target,
            state=node_state,
        )

        bad_route = pb2.ServiceRouteReport(
            service_name="svc_gateway_failover",
            service_id="svc-bad-route",
            status=pb2.SERVICE_STATUS_RUNNING,
            worker_count=4,
            alive_workers=4,
            in_flight=0,
            http_base_url="http://127.0.0.1:1/svc/svc-bad-route",
        )
        with InfoCenterClient(controlplane.base_url, timeout_sec=10.0) as infocenter:
            infocenter.register_node(
                node_id="node-aaa-bad",
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
                    service_name="svc_gateway_failover",
                    healthy_only=True,
                    limit=20,
                )
            )
            == 2
        )

        code, call_resp = _http_json(
            "POST",
            f"{controlplane.base_url}/svc/svc_gateway_failover/call/mul",
            {"value": 11},
        )
        assert code == 200
        assert call_resp["ok"] is True
        assert call_resp["data"]["square"] == 121

        code, status_resp = _http_json(
            "GET",
            f"{controlplane.base_url}/svc/svc_gateway_failover/status",
        )
        assert code == 200
        route_map = {item["service_id"]: item for item in status_resp["routes"]}
        assert service_id in route_map
        assert "svc-bad-route" in route_map
    finally:
        node_server.stop(grace=0)
        node_state.close()
        controlplane.stop()


def test_gateway_supports_http_only_job_orchestrator_service():
    infocenter = build_infocenter_server("127.0.0.1:0")
    infocenter.start()
    gateway = build_gateway_server("127.0.0.1:0", infocenter_addr=infocenter.base_url)
    gateway.start()
    job_orchestrator = build_job_orchestrator_server(
        "127.0.0.1:0",
        infocenter_addr=infocenter.base_url,
        node_id="job-orchestrator-test",
    )
    job_orchestrator.start()
    with job_orchestrator.job_queue._cv:  # noqa: SLF001
        job_orchestrator.job_queue._running_job_id = "__test_blocked__"  # noqa: SLF001

    try:
        assert _wait_until(
            lambda: len(
                InfoCenterClient(infocenter.base_url, timeout_sec=5.0).list_service_routes(
                    service_name="job-orchestrator",
                    healthy_only=True,
                    limit=10,
                )
            )
            == 1
        )

        with GatewayServiceClient(gateway.base_url, timeout_sec=5.0) as client:
            methods = client.list_methods(service_name="job-orchestrator", include_docs=False)
            assert sorted(item["method"] for item in methods) == ["cancel_job", "get_job_status", "reorder_job", "submit_job"]

        with GatewayServiceClient(gateway.base_url, timeout_sec=5.0, service_token="job-owner-token") as owner_client:
            submit = owner_client.call(
                service_name="job-orchestrator",
                method="submit_job",
                payload={
                    "client_id": "gw-job-test",
                    "subtasks": [{"value": 1}],
                    "entry_module": "task_demo",
                    "code_version": "sha256:test",
                },
                timeout_sec=5.0,
            )
            assert submit["ok"] is True
            job_id = str(submit["job"]["job_id"])
            assert job_id
            second = owner_client.call(
                service_name="job-orchestrator",
                method="submit_job",
                payload={
                    "client_id": "gw-job-test",
                    "subtasks": [{"value": 2}],
                    "entry_module": "task_demo",
                    "code_version": "sha256:test",
                },
                timeout_sec=5.0,
            )
            second_job_id = str(second["job"]["job_id"])
            third = owner_client.call(
                service_name="job-orchestrator",
                method="submit_job",
                payload={
                    "client_id": "gw-job-test",
                    "subtasks": [{"value": 3}],
                    "entry_module": "task_demo",
                    "code_version": "sha256:test",
                },
                timeout_sec=5.0,
            )
            third_job_id = str(third["job"]["job_id"])
            reorder = owner_client.call(
                service_name="job-orchestrator",
                method="reorder_job",
                payload={"job_id": third_job_id, "direction": "up"},
                timeout_sec=5.0,
            )
            assert reorder["ok"] is True
            waiting_ids = [item["job_id"] for item in reorder["queue"]["waiting_jobs"]]
            assert second_job_id in waiting_ids and third_job_id in waiting_ids
            assert waiting_ids.index(third_job_id) < waiting_ids.index(second_job_id)

            with GatewayServiceClient(gateway.base_url, timeout_sec=5.0, service_token="job-other-token") as other_client:
                with pytest.raises(RuntimeError, match="cancel auth failed"):
                    other_client.call(
                        service_name="job-orchestrator",
                        method="cancel_job",
                        payload={"job_id": job_id},
                        timeout_sec=5.0,
                    )

            cancelled = owner_client.call(
                service_name="job-orchestrator",
                method="cancel_job",
                payload={"job_id": second_job_id},
                timeout_sec=5.0,
            )
            assert cancelled["ok"] is True
            assert cancelled["job"]["status"] == "CANCELLED"

        job_state = job_orchestrator.job_queue.get_job(job_id)
        assert job_state is not None
        job_state.status = "FAILED"
        job_state.final_result = {"processed": 2}
        job_state.results = [
            {
                "task_id": "task-ok-1",
                "status": int(pb2.TASK_STATUS_SUCCEEDED),
                "status_text": "SUCCEEDED",
                "attempt": 1,
                "result": {"value": 1, "square": 1},
            },
            {
                "task_id": "task-fail-2",
                "status": int(pb2.TASK_STATUS_FAILED_USER),
                "status_text": "FAILED_USER",
                "attempt": 1,
                "error": {"type": "UserError", "message": "boom"},
            },
        ]

        with urlopen(f"{job_orchestrator.base_url}/svc/{job_orchestrator.service_id}/jobs/{job_id}", timeout=5.0) as resp:
            detail = json.loads(resp.read().decode("utf-8") or "{}")
        assert detail["ok"] is True
        assert detail["job"]["job_id"] == job_id

        with urlopen(f"{job_orchestrator.base_url}/svc/{job_orchestrator.service_id}/jobs/{job_id}?view=html", timeout=5.0) as resp:
            html_detail = resp.read().decode("utf-8")
        assert "Job Detail" in html_detail
        assert "auto_refresh_sec=10" in html_detail
        assert "http-equiv='refresh' content='10'" in html_detail
        assert "white-space:pre-wrap" in html_detail
        assert "Payload" in html_detail
        assert "Checkpoint" in html_detail
        assert "Final Result" in html_detail
        assert "Results" in html_detail
        assert "task-filter" in html_detail
        assert "filterJobResults()" in html_detail
        assert "details" in html_detail
        assert "result-row-failed" in html_detail
        assert "task-ok-1" in html_detail
        assert "task-fail-2" in html_detail
        assert job_id in html_detail

        with pytest.raises(HTTPError):
            urlopen(f"{job_orchestrator.base_url}/svc/{job_orchestrator.service_id}/jobs/not-found", timeout=5.0)
    finally:
        job_orchestrator.stop()
        gateway.stop()
        infocenter.stop()


def test_gateway_service_client_call_uses_http_payload_policy(monkeypatch) -> None:
    captured = {}

    class _FakeNodeControlClient:
        def __init__(self, target: str, timeout_sec: float = 10.0) -> None:
            del timeout_sec
            self.target = target

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.client.NodeControlClient",
        _FakeNodeControlClient,
    )
    monkeypatch.setattr(
        GatewayServiceClient,
        "get_status",
        lambda self, *, service_name: {"routes": [{"control_addr": "127.0.0.1:50061"}]},
    )

    def _fake_prepare(payload, *, put_data, estimate_inline_size, policy):
        del put_data, estimate_inline_size
        captured["mode"] = policy.mode
        captured["preserve_args_kwargs_container"] = policy.preserve_args_kwargs_container
        return dict(payload or {})

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.client.prepare_outbound_payload",
        _fake_prepare,
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.client._http_json_request",
        lambda **kwargs: {"ok": True, "data": kwargs.get("payload", {})},
    )

    with GatewayServiceClient("127.0.0.1:50051", timeout_sec=5.0) as gateway:
        resp = gateway.call(service_name="svc-demo", method="run", payload={"args": [1], "kwargs": {"x": 2}})

    assert resp["ok"] is True
    assert captured["mode"] == "http_call"
    assert captured["preserve_args_kwargs_container"] is True
