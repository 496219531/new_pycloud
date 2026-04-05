from __future__ import annotations

from concurrent import futures
import time
from typing import Tuple

import grpc

from pycloud_parallel.controlplane.client import InfoCenterClient, JobQueueClient, NodeControlClient, TaskPoolSession
from pycloud_parallel.controlplane.server import build_controlplane_server
from pycloud_parallel.controlplane.services import NodeControlService
from pycloud_parallel.controlplane.state import NodeControlState
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


def test_native_task_pool_session_end_to_end(tmp_path):
    controlplane = build_controlplane_server("127.0.0.1:0")
    controlplane.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-pool-01", str(tmp_path / "node_pool_01"))

    try:
        _register_node(controlplane.base_url, node_id="node-pool-01", control_addr=node_target, state=node_state)
        assert _wait_until(lambda: len(InfoCenterClient(controlplane.base_url, timeout_sec=5.0).list_nodes(limit=10)) == 1)

        blob = (
            b"def run(value=0, sleep_ms=0, **_kwargs):\n"
            b"    import time\n"
            b"    sleep_ms = int(sleep_ms)\n"
            b"    if sleep_ms > 0:\n"
            b"        time.sleep(sleep_ms / 1000.0)\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        )

        with TaskPoolSession.from_infocenter(
            infocenter_target=controlplane.base_url,
            job_id="job-pool-e2e",
            blob=blob,
            runtime="py3",
            entry_module="task_pool_demo",
            entry_callable="run",
            worker_count=2,
            node_count=1,
            tags=["compute"],
        ) as pool:
            status_map = pool.status_map()
            assert "node-pool-01" in status_map
            assert status_map["node-pool-01"].status == "RUNNING"

            submit = pool.submit_payloads([{"value": 2}, {"value": 3}, {"value": 4}])
            assert len(submit.accepted) == 3

            results = pool.wait_for_data(expected_count=3, timeout_sec=10.0)
            assert sorted(item["square"] for item in results) == [4, 9, 16]

            cancel_resp = pool.cancel_job(job_id="job-pool-e2e", reason="post-finish-cancel")
            assert cancel_resp.already_done >= 0
    finally:
        node_server.stop(grace=0)
        node_state.close()
        controlplane.stop()


def test_job_queue_uses_native_task_pool_end_to_end(tmp_path):
    controlplane = build_controlplane_server("127.0.0.1:0")
    controlplane.start()
    node_server, node_target, node_state = _start_nodecontrol_server("node-jobq-01", str(tmp_path / "node_jobq_01"))

    try:
        _register_node(controlplane.base_url, node_id="node-jobq-01", control_addr=node_target, state=node_state)
        assert _wait_until(lambda: len(InfoCenterClient(controlplane.base_url, timeout_sec=5.0).list_nodes(limit=10)) == 1)

        driver_blob = (
            b"def build(value=0, count=3, **_kwargs):\n"
            b"    return [{'value': value + i} for i in range(count)]\n"
        )
        task_blob = (
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        )
        client = JobQueueClient(controlplane.base_url, timeout_sec=10.0)
        try:
            submit = client.submit_job_from_bytes(
                blob=driver_blob,
                driver_entry_module="job_driver_demo",
                driver_entry_callable="build",
                driver_payload={"value": 5, "count": 3},
                client_id="jobq-client",
                runtime="py3",
                task_blob=task_blob,
                task_entry_module="task_pool_demo",
                task_entry_callable="run",
                task_package_format="py",
                tags=["compute"],
                pool_worker_count=2,
                pool_node_count=1,
                wait_timeout_sec=10.0,
            )
            job_id = submit["job"]["job_id"]

            deadline = time.time() + 15.0
            final = None
            while time.time() < deadline:
                status = client.get_job_status(job_id)
                if status["job"]["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                    final = status["job"]
                    break
                time.sleep(0.2)

            assert final is not None
            assert final["status"] == "SUCCEEDED"
            assert len(final["results"]) == 3
        finally:
            client.close()
    finally:
        node_server.stop(grace=0)
        node_state.close()
        controlplane.stop()
