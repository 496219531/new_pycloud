"""Integration tests for NodeControl service-session client helpers."""

from concurrent import futures

import grpc
import pytest

from pycloud_parallel.controlplane.client import InfoCenterClient, NodeControlClient, TaskBatchClient
from pycloud_parallel.controlplane.infocenter_http import InfoCenterHttpServer
from pycloud_parallel.controlplane.services import NodeControlService
from pycloud_parallel.controlplane.state import InfoCenterState, NodeControlState
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc


def test_service_session_client_roundtrip(tmp_path):
    state = NodeControlState(
        node_id="node-client-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
        monitor_interval_sec=1,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    target = f"127.0.0.1:{port}"

    try:
        blob = (
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(payload):\n"
            b"    v = int(payload.get('value', 0))\n"
            b"    return {'value': v, 'square': v * v}\n"
        )
        with NodeControlClient(target, timeout_sec=10.0) as client:
            session = client.create_service_from_bytes(
                owner_client_id="owner-client",
                service_name="svc-demo",
                filename="svc_demo.py",
                blob=blob,
                runtime="py3.11",
                entry_module="svc_demo",
                entry_callable="run",
                worker_count=2,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=True,
            )
            assert session.status == pb2.SERVICE_STATUS_RUNNING

            hb = client.heartbeat_service(
                owner_client_id=session.owner_client_id,
                service_id=session.service_id,
                service_token=session.service_token,
                seq=1,
            )
            assert hb.accepted is True
            assert hb.status == pb2.SERVICE_STATUS_RUNNING

            methods = session.list_methods(include_docs=False)
            assert [m.method for m in methods] == ["run"]

            resp = session.call("run", {"value": 7}, timeout_sec=10.0)
            assert resp["ok"] is True
            assert resp["data"]["square"] == 49

            info = session.get_status()
            assert info.service_id == session.service_id
            assert info.status == pb2.SERVICE_STATUS_RUNNING

            end_resp = session.end("test done")
            assert end_resp.ok is True
            assert end_resp.accepted is True
            assert end_resp.status == pb2.SERVICE_STATUS_STOPPED

            with pytest.raises(RuntimeError):
                session.call("run", {"value": 1}, timeout_sec=2.0)
    finally:
        server.stop(grace=0)
        state.close()
def test_nodecontrol_client_task_helpers_roundtrip(tmp_path):
    state = NodeControlState(
        node_id="node-client-task-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=False,
        monitor_interval_sec=1,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    target = f"127.0.0.1:{port}"

    try:
        blob = (
            b"def run(payload):\n"
            b"    value = int(payload.get('value', 0))\n"
            b"    return {'value': value, 'square': value * value}\n"
        )
        with NodeControlClient(target, timeout_sec=10.0) as client:
            upload = client.upload_code_from_bytes(
                client_id="task-client",
                filename="task_demo.py",
                blob=blob,
                runtime="py3.11",
                entry_module="task_demo",
                entry_callable="run",
            )
            assert upload.ok is True
            assert upload.code_version

            submit = client.submit_tasks(
                client_id="task-client",
                code_version=upload.code_version,
                job_id="job-demo",
                tasks=[
                    pb2.TaskSubmitItem(task_id="task-1", payload={"value": 2}, priority=1),
                    pb2.TaskSubmitItem(task_id="task-2", payload={"value": 3}, priority=1),
                ],
            )
            assert submit.ok is True
            assert [item.task_id for item in submit.accepted] == ["task-1", "task-2"]

            cancel = client.cancel_job(
                client_id="task-client",
                job_id="job-demo",
                reason="stop batch",
            )
            assert cancel.ok is True
            assert cancel.queued_cancelled == 2
            assert cancel.running_marked == 0
            assert cancel.already_done == 0
            assert cancel.not_found == 0

            pulled = client.pull_results(client_id="task-client", limit=10, wait_ms=0, cursor="")
            assert pulled.ok is True
            assert sorted(item.task_id for item in pulled.results) == ["task-1", "task-2"]
            assert {item.job_id for item in pulled.results} == {"job-demo"}

            metrics = client.get_metrics()
            assert metrics.ok is True
            assert metrics.node_id == "node-client-task-01"
    finally:
        server.stop(grace=0)
        state.close()


def test_task_batch_client_from_infocenter_roundtrip(tmp_path):
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()

    state_a = NodeControlState(
        node_id="node-client-task-batch-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_a"),
        enable_internal_executor=False,
        enable_service_session=False,
        monitor_interval_sec=1,
    )
    server_a = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state_a), server_a)
    port_a = server_a.add_insecure_port("127.0.0.1:0")
    server_a.start()
    target_a = f"127.0.0.1:{port_a}"

    state_b = NodeControlState(
        node_id="node-client-task-batch-02",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_b"),
        enable_internal_executor=False,
        enable_service_session=False,
        monitor_interval_sec=1,
    )
    server_b = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state_b), server_b)
    port_b = server_b.add_insecure_port("127.0.0.1:0")
    server_b.start()
    target_b = f"127.0.0.1:{port_b}"

    try:
        with InfoCenterClient(info_server.base_url, timeout_sec=5.0) as infocenter:
            infocenter.register_node(
                node_id="node-client-task-batch-01",
                control_addr=target_a,
                capacity=2,
                queue_capacity=16,
                tags=["compute"],
            )
            infocenter.register_node(
                node_id="node-client-task-batch-02",
                control_addr=target_b,
                capacity=2,
                queue_capacity=16,
                tags=["compute"],
            )
            infocenter.heartbeat_node(
                node_id="node-client-task-batch-01",
                healthy=True,
                metrics={"queued": 0, "inflight": 0, "running": 0, "credit": 8},
            )
            infocenter.heartbeat_node(
                node_id="node-client-task-batch-02",
                healthy=True,
                metrics={"queued": 0, "inflight": 0, "running": 0, "credit": 6},
            )

        blob = (
            b"def run(payload):\n"
            b"    value = int(payload.get('value', 0))\n"
            b"    return {'value': value, 'square': value * value}\n"
        )
        with TaskBatchClient.from_infocenter(
            infocenter_target=info_server.base_url,
            client_id="task-batch-client",
            job_id="job-batch",
            blob=blob,
            filename="task_batch_demo.py",
            runtime="py3.11",
            entry_module="task_batch_demo",
            entry_callable="run",
            tags=["compute"],
            timeout_sec=10.0,
        ) as batch:
            assert set(batch.node_ids) == {
                "node-client-task-batch-01",
                "node-client-task-batch-02",
            }
            assert batch.code_version.startswith("sha256:")
            metrics = batch.get_metrics()
            assert set(metrics.keys()) == {
                "node-client-task-batch-01",
                "node-client-task-batch-02",
            }

            submit = batch.submit_payloads(
                [
                    {"value": 5},
                    {"value": 6},
                ],
                job_id="job-batch-cancel",
            )
            assert [item.task_id for item in submit.accepted] == [
                "job-batch-cancel-task-0001",
                "job-batch-cancel-task-0002",
            ]
            assert list(batch.submitted_task_ids(job_id="job-batch-cancel")) == [
                "job-batch-cancel-task-0001",
                "job-batch-cancel-task-0002",
            ]

            cancel = batch.cancel_job(reason="cancel demo", job_id="job-batch-cancel")
            assert cancel.queued_cancelled == 2
            assert cancel.running_marked == 0

            results = batch.wait_for_results(
                job_id="job-batch-cancel",
                expected_count=2,
                timeout_sec=2.0,
                wait_ms=0,
                limit=10,
            )
            assert len(results) == 2
            assert {item.job_id for item in results} == {"job-batch-cancel"}
            assert {item.status for item in results} == {pb2.TASK_STATUS_CANCELLED}
    finally:
        server_a.stop(grace=0)
        state_a.close()
        server_b.stop(grace=0)
        state_b.close()
        info_server.stop()
