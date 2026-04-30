from __future__ import annotations

from concurrent import futures
import time
from typing import Tuple
from unittest.mock import patch

import grpc

from pycloud_parallel import JobQueue
from pycloud_parallel.artifact import Artifact, ArtifactExports
from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient
from pycloud_parallel.controlplane.server import (
    build_gateway_server,
    build_infocenter_server,
    build_job_orchestrator_server,
)
from pycloud_parallel.controlplane.services import NodeControlService
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState
from pycloud_parallel.execution.service_session import Service
from pycloud_parallel.execution.task_pool import TaskPool
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
        enable_internal_executor=True,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
        monitor_interval_sec=1,
        executor_poll_interval_sec=0.02,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=24))
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, f"127.0.0.1:{port}", state


def _register_node(info_target: str, *, node_id: str, control_addr: str, state: NodeControlState) -> None:
    with InfoCenterClient(info_target, timeout_sec=10.0) as infocenter:
        infocenter.register_node(
            node_id=node_id,
            control_addr=control_addr,
            capacity=8,
            queue_capacity=64,
            tags=["compute"],
            services=state.service_reports(),
            service_worker_capacity=state.service_worker_capacity,
            service_worker_used=state.service_worker_used(),
        )


def test_service_task_pool_and_job_queue_smoke(tmp_path):
    infocenter = build_infocenter_server("127.0.0.1:0")
    infocenter.start()
    gateway = build_gateway_server("127.0.0.1:0", infocenter_addr=infocenter.base_url)
    gateway.start()
    job_orchestrator = build_job_orchestrator_server(
        "127.0.0.1:0",
        infocenter_addr=infocenter.base_url,
        node_id="job-orchestrator-smoke-01",
    )
    job_orchestrator.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-smoke-01", str(tmp_path / "node_smoke_01"))

    try:
        _register_node(infocenter.base_url, node_id="node-smoke-01", control_addr=node_target, state=node_state)
        assert _wait_until(lambda: len(InfoCenterClient(infocenter.base_url, timeout_sec=5.0).list_nodes(limit=10)) >= 2)
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

        pool_blob = (
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        )
        with TaskPool.open(
            target=infocenter.base_url,
            job_id="v1-smoke-pool",
            artifact=Artifact.from_bytes(
                pool_blob,
                package_format="py",
                entry_module="v1_smoke_pool_demo",
                entry_callable="run",
            ),
            worker_count=2,
            node_count=1,
            tags=["compute"],
        ) as pool:
            submit = pool.submit_payloads([{"value": 2}, {"value": 3}])
            values = pool.wait_for_data(expected_count=len(submit.accepted), timeout_sec=10.0)
            assert sorted(item["square"] for item in values) == [4, 9]
            assert pool.status().kind == "task_pool"

        service_blob = (
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def mul(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        )
        service = Service.deploy(
            target=infocenter.base_url,
            owner_client_id="v1-smoke-owner",
            service_name="v1-smoke-service",
            artifact=Artifact.from_bytes(
                service_blob,
                package_format="py",
                entry_module="v1_smoke_service_demo",
                entry_callable="mul",
                exports=ArtifactExports.use_decorator(),
            ),
            worker_count=1,
            node_count=1,
            tags=["compute"],
            session_cache_dir=str(tmp_path / "service_cache"),
        )
        try:
            _node_id, response = service.call_balanced("mul", {"value": 4}, timeout_sec=10.0)
            assert response["data"]["square"] == 16
            assert service.status().kind == "service"
        finally:
            service.close()

        job_blob = (
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n\n"
            b"def task_generator(value=0, count=2, **_kwargs):\n"
            b"    return [{'value': value + i} for i in range(count)]\n\n"
            b"def handle_result(task_id, result, state=None, **_kwargs):\n"
            b"    state.setdefault('items', []).append(result)\n\n"
            b"def finalize(state=None, **_kwargs):\n"
            b"    return {'count': len(state.get('items', []))}\n"
        )
        with patch(
            "pycloud_parallel.controlplane.job_queue._create_job_task_pool",
            wraps=TaskPool._from_infocenter,
        ) as mocked_create_pool:
            client = JobQueue.connect(infocenter.base_url, client_id="v1-smoke-job-client", timeout_sec=10.0)
            try:
                submit = client.submit(
                    source=job_blob,
                    job_payload={"value": 6, "count": 2},
                    runtime="py3",
                    entry_module="v1_smoke_job_demo",
                )
                final = client.wait_for_terminal(submit["job"]["job_id"], timeout_sec=15.0, poll_interval_sec=0.2)["job"]
            finally:
                client.close()

        assert final["status"] == "SUCCEEDED"
        assert final["final_result"] == {"count": 2}
        assert mocked_create_pool.call_count >= 1
    finally:
        node_server.stop(grace=0)
        node_state.close()
        job_orchestrator.stop()
        gateway.stop()
        infocenter.stop()
