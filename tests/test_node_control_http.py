from __future__ import annotations

import time

from pycloud_parallel.controlplane.node_control_http import HttpNodeControlClient, NodeControlHttpServer
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState
from pycloud_parallel.controlplane.serialization import encode_transport_payload_bytes
from pycloud_parallel.controlplane.serialization import dict_to_struct, struct_to_dict
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


def _start_http_node(tmp_path):
    state = NodeControlState(
        node_id="node-http-control",
        queue_capacity=32,
        worker_capacity=4,
        artifact_dir=str(tmp_path / "node_http_control"),
        service_http_bind="127.0.0.1:0",
    )
    server = NodeControlHttpServer(bind="127.0.0.1:0", state=state)
    server.start()
    return server, state


def test_http_create_service_call_heartbeat_status_close(tmp_path):
    server, state = _start_http_node(tmp_path)
    blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value) + 1}\n"
    try:
        with HttpNodeControlClient(server.base_url, timeout_sec=10.0) as client:
            session = client.create_service_from_bytes(
                owner_client_id="owner-http",
                service_name="svc-http",
                blob=blob,
                runtime="py3",
                entry_module="svc_http_demo",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                expose_http=False,
            )
            methods = client.list_service_methods(service_id=session.service_id)
            assert [item.method for item in methods] == ["run"]
            response = client.call_service(
                service_id=session.service_id,
                method="run",
                payload={"value": 2},
                service_token=session.service_token,
                timeout_sec=10.0,
            )
            assert struct_to_dict(response.data) == {"value": 3}
            assert client.heartbeat_service(
                owner_client_id="owner-http",
                service_id=session.service_id,
                service_token=session.service_token,
            ).accepted is True
            assert client.get_service_status(service_id=session.service_id).service_id == session.service_id
            assert client.end_service(
                owner_client_id="owner-http",
                service_id=session.service_id,
                service_token=session.service_token,
            ).accepted is True
    finally:
        server.stop()
        state.close()


def test_http_create_taskpool_submit_pull_heartbeat_close(tmp_path):
    server, state = _start_http_node(tmp_path)
    blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value) * 2}\n"
    try:
        with HttpNodeControlClient(server.base_url, timeout_sec=10.0) as client:
            pool = client.create_task_pool_from_bytes(
                owner_client_id="owner-http",
                pool_name="pool-http",
                blob=blob,
                runtime="py3",
                entry_module="pool_http_demo",
                entry_callable="run",
                package_format="py",
                worker_count=1,
            )
            task = pb2.TaskSubmitItem(
                task_id="task-http-1",
                payload=dict_to_struct({"value": 4}),
                timeout_hint_sec=10,
            )
            submitted = pool.submit_tasks([task], job_id="job-http")
            assert len(submitted.accepted) == 1
            deadline = time.time() + 10.0
            result = None
            while time.time() < deadline:
                pulled = pool.pull_results(limit=10, wait_ms=100)
                if pulled.results:
                    result = pulled.results[0]
                    break
            assert result is not None
            assert result.status == pb2.TASK_STATUS_SUCCEEDED
            assert struct_to_dict(result.result) == {"value": 8}
            assert pool.heartbeat().accepted is True
            assert pool.get_status().pool_id == pool.pool_id
            assert pool.close().accepted is True
    finally:
        server.stop()
        state.close()


def test_http_taskpool_uses_transport_payload_adapter(tmp_path):
    server, state = _start_http_node(tmp_path)
    blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value) * 3}\n"
    try:
        with HttpNodeControlClient(server.base_url, timeout_sec=10.0) as client:
            pool = client.create_task_pool_from_bytes(
                owner_client_id="owner-http-bytes",
                pool_name="pool-http-bytes",
                blob=blob,
                runtime="py3",
                entry_module="pool_http_bytes_demo",
                entry_callable="run",
                package_format="py",
                worker_count=1,
            )
            task = pb2.TaskSubmitItem(
                task_id="task-http-bytes-1",
                transport_payload=encode_transport_payload_bytes(
                    {"value": 7},
                    mode="pickle_stable_v1",
                    context="taskpool_session",
                ),
                timeout_hint_sec=10,
            )
            submitted = pool.submit_tasks([task], job_id="job-http-bytes")
            assert len(submitted.accepted) == 1
            deadline = time.time() + 10.0
            result = None
            while time.time() < deadline:
                pulled = pool.pull_results(limit=10, wait_ms=100)
                if pulled.results:
                    result = pulled.results[0]
                    break
            assert result is not None
            assert result.status == pb2.TASK_STATUS_SUCCEEDED
            assert result.HasField("transport_result")
            assert result.transport_result.codec == "pickle_stable_v1"
            assert client.fetch_result_data(result) == {"value": 21}
            assert pool.close().accepted is True
    finally:
        server.stop()
        state.close()
