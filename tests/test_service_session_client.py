"""Integration tests for NodeControl service-session client helpers."""

from concurrent import futures

import grpc
import pytest

from pycloud_parallel.controlplane.client import NodeControlClient
from pycloud_parallel.controlplane.services import NodeControlService, WorkerInternalService
from pycloud_parallel.controlplane.state import NodeControlState
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
    pb2_grpc.add_WorkerInternalServiceServicer_to_server(WorkerInternalService(state), server)
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

            session.start_keepalive(interval_sec=1.0)
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
