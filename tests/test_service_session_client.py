"""Integration tests for NodeControl service-session client helpers."""

from concurrent import futures
import hashlib
import json
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import grpc
import pytest

from pycloud_parallel.controlplane.client import InfoCenterClient, NodeControlClient, TaskBatchClient
from pycloud_parallel.controlplane.infocenter_http import InfoCenterHttpServer
from pycloud_parallel.controlplane.result_ref import ResultRef
from pycloud_parallel.controlplane.serialization import INLINE_PAYLOAD_HARD_LIMIT_BYTES, dict_to_struct
from pycloud_parallel.controlplane.services import NodeControlService
from pycloud_parallel.controlplane.state import InfoCenterState, NodeControlState, struct_to_dict
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc


def _make_local_dependency_package(base_dir: Path, *, folder_name: str = "dep_pkg") -> Path:
    pkg_root = base_dir / folder_name
    module_dir = pkg_root / "pycloud_local_dep"
    module_dir.mkdir(parents=True, exist_ok=True)
    (pkg_root / "setup.py").write_text(
        "from setuptools import setup\n"
        "setup(name='pycloud-local-dep', version='0.0.1', packages=['pycloud_local_dep'])\n",
        encoding="utf-8",
    )
    (module_dir / "__init__.py").write_text(
        "def multiply(value):\n"
        "    return int(value) * 10\n",
        encoding="utf-8",
    )
    return pkg_root


def _oversized_inline_payload() -> dict:
    return {"blob": "x" * (INLINE_PAYLOAD_HARD_LIMIT_BYTES + 1024)}


def test_upload_code_preflight_rejects_bad_import(tmp_path):
    state = NodeControlState(
        node_id="node-client-upload-preflight-01",
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

    blob = (
        b"import pycloud_missing_dep_for_test_case\n\n"
        b"def run(**_kwargs):\n"
        b"    return {'ok': True}\n"
    )
    digest = hashlib.sha256(blob).hexdigest()
    code_version = f"sha256:{digest}"
    try:
        with NodeControlClient(target, timeout_sec=10.0) as client:
            with pytest.raises(grpc.RpcError) as excinfo:
                client.upload_code_from_bytes(
                    client_id="upload-client",
                    blob=blob,
                    runtime="py3",
                    entry_module="bad_upload",
                    entry_callable="run",
                )
        assert excinfo.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert "artifact validation failed while loading" in excinfo.value.details()
        assert "dependency_allowlist" in excinfo.value.details()
        assert state.has_code_version(code_version) is False
    finally:
        server.stop(grace=0)
        state.close()


def test_create_service_surfaces_user_import_error(tmp_path):
    state = NodeControlState(
        node_id="node-client-create-service-err-01",
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

    blob = (
        b"import pycloud_missing_dep_for_service_case\n\n"
        b"def run(**_kwargs):\n"
        b"    return {'ok': True}\n"
    )
    try:
        with NodeControlClient(target, timeout_sec=10.0) as client:
            with pytest.raises(grpc.RpcError) as excinfo:
                client.create_service_from_bytes(
                    owner_client_id="owner-client",
                    service_name="svc-bad",
                    blob=blob,
                    runtime="py3",
                    entry_module="svc_bad",
                    entry_callable="run",
                    worker_count=1,
                    heartbeat_timeout_sec=30,
                    idle_ttl_sec=0,
                    expose_http=False,
                )
        assert excinfo.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert "artifact validation failed while loading" in excinfo.value.details()
    finally:
        server.stop(grace=0)
        state.close()


def test_upload_code_allowlist_installs_missing_dep(tmp_path):
    state = NodeControlState(
        node_id="node-client-upload-dep-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=True,
        enable_service_session=False,
        monitor_interval_sec=1,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    target = f"127.0.0.1:{port}"

    dep_pkg = _make_local_dependency_package(tmp_path, folder_name="dep_pkg_a")
    blob = (
        b"from pycloud_local_dep import multiply\n\n"
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': multiply(value)}\n"
    )
    try:
        with NodeControlClient(target, timeout_sec=30.0) as client:
            upload = client.upload_code_from_bytes(
                client_id="upload-client",
                blob=blob,
                runtime="py3",
                entry_module="dep_upload",
                entry_callable="run",
                dependency_allowlist=[str(dep_pkg)],
            )
            assert upload.ok is True

            submit = client.submit_tasks(
                client_id="upload-client",
                code_version=upload.code_version,
                job_id="job-upload-dep",
                tasks=[pb2.TaskSubmitItem(task_id="task-dep-1", payload={"value": 7}, priority=1)],
            )
            assert [item.task_id for item in submit.accepted] == ["task-dep-1"]

            results = client.pull_results(client_id="upload-client", limit=10, wait_ms=3000, cursor="")
            assert results.ok is True
            assert len(results.results) == 1
            assert results.results[0].status == pb2.TASK_STATUS_SUCCEEDED
            assert results.results[0].result["value"] == 70
    finally:
        server.stop(grace=0)
        state.close()


def test_create_service_allowlist_installs_missing_dep(tmp_path):
    state = NodeControlState(
        node_id="node-client-create-service-dep-01",
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

    dep_pkg = _make_local_dependency_package(tmp_path, folder_name="dep_pkg_service")
    blob = (
        b"from pycloud_local_dep import multiply\n\n"
        b"def pycloud_export(fn):\n"
        b"    fn.__pycloud_export__ = True\n"
        b"    return fn\n\n"
        b"@pycloud_export\n"
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': multiply(value)}\n"
    )
    try:
        with NodeControlClient(target, timeout_sec=30.0) as client:
            session = client.create_service_from_bytes(
                owner_client_id="owner-client",
                service_name="svc-dep",
                blob=blob,
                runtime="py3",
                entry_module="svc_dep",
                entry_callable="run",
                dependency_allowlist=[str(dep_pkg)],
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=True,
            )
            resp = session.call("run", {"value": 9}, timeout_sec=10.0)
            assert resp["ok"] is True
            assert resp["data"]["value"] == 90
    finally:
        server.stop(grace=0)
        state.close()


def test_upload_code_cached_version_rejects_different_dependency_allowlist(tmp_path):
    state = NodeControlState(
        node_id="node-client-upload-dep-mismatch-01",
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

    dep_pkg_a = _make_local_dependency_package(tmp_path, folder_name="dep_pkg_first")
    dep_pkg_b = _make_local_dependency_package(tmp_path, folder_name="dep_pkg_second")
    blob = (
        b"from pycloud_local_dep import multiply\n\n"
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': multiply(value)}\n"
    )
    try:
        with NodeControlClient(target, timeout_sec=30.0) as client:
            first = client.upload_code_from_bytes(
                client_id="upload-client",
                blob=blob,
                runtime="py3",
                entry_module="dep_upload_same",
                entry_callable="run",
                dependency_allowlist=[str(dep_pkg_a)],
            )
            assert first.ok is True

            with pytest.raises(grpc.RpcError) as excinfo:
                client.upload_code_from_bytes(
                    client_id="upload-client",
                    blob=blob,
                    runtime="py3",
                    entry_module="dep_upload_same",
                    entry_callable="run",
                    dependency_allowlist=[str(dep_pkg_b)],
                )
        assert excinfo.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert "different dependency_allowlist" in excinfo.value.details()
    finally:
        server.stop(grace=0)
        state.close()


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
            b"def run(value=0, **_kwargs):\n"
            b"    v = int(value)\n"
            b"    return {'value': v, 'square': v * v}\n"
        )
        with NodeControlClient(target, timeout_sec=10.0) as client:
            session = client.create_service_from_bytes(
                owner_client_id="owner-client",
                service_name="svc-demo",
                blob=blob,
                runtime="py3",
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


def test_service_session_call_dataframe_result_returns_result_ref_and_fetches(tmp_path):
    pytest.importorskip("pyarrow")
    pd = pytest.importorskip("pandas")

    state = NodeControlState(
        node_id="node-client-session-result-ref-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_session_result_ref"),
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
            b"import pandas as pd\n\n"
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return pd.DataFrame([{'x': value}, {'x': value + 1}, {'x': value + 2}])\n"
        )
        with NodeControlClient(target, timeout_sec=10.0) as client:
            session = client.create_service_from_bytes(
                owner_client_id="owner-session-result-ref",
                service_name="svc-session-result-ref",
                blob=blob,
                runtime="py3",
                entry_module="svc_session_result_ref",
                entry_callable="run",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=True,
            )
            resp = session.call("run", {"value": 7}, timeout_sec=10.0)
            assert resp["ok"] is True
            assert isinstance(resp["data"], ResultRef)
            assert resp["data"].control_addr == target
            frame = session.fetch_result_data(resp)
            assert isinstance(frame, pd.DataFrame)
            assert list(frame["x"]) == [7, 8, 9]
    finally:
        server.stop(grace=0)
        state.close()


def test_nodecontrol_call_service_dataframe_result_returns_result_ref_and_fetches(tmp_path):
    pytest.importorskip("pyarrow")
    pd = pytest.importorskip("pandas")

    state = NodeControlState(
        node_id="node-client-call-service-result-ref-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_call_service_result_ref"),
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
            b"import pandas as pd\n\n"
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return pd.DataFrame([{'x': value}, {'x': value + 1}])\n"
        )
        with NodeControlClient(target, timeout_sec=10.0) as client:
            session = client.create_service_from_bytes(
                owner_client_id="owner-call-service-result-ref",
                service_name="svc-call-service-result-ref",
                blob=blob,
                runtime="py3",
                entry_module="svc_call_service_result_ref",
                entry_callable="run",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=True,
            )
            resp = client.call_service(
                service_id=session.service_id,
                method="run",
                payload={"value": 5},
                timeout_sec=10.0,
                service_token=session.service_token,
            )
            result_value = struct_to_dict(resp.data)
            assert isinstance(result_value, ResultRef)
            assert result_value.node_id == "node-client-call-service-result-ref-01"
            frame = client.fetch_service_result_data(resp)
            assert isinstance(frame, pd.DataFrame)
            assert list(frame["x"]) == [5, 6]
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
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        )
        with NodeControlClient(target, timeout_sec=10.0) as client:
            upload = client.upload_code_from_bytes(
                client_id="task-client",
                blob=blob,
                runtime="py3",
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


def test_nodecontrol_client_task_stream_roundtrip(tmp_path):
    state = NodeControlState(
        node_id="node-client-task-stream-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_stream"),
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
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        )
        with NodeControlClient(target, timeout_sec=10.0) as client:
            upload = client.upload_code_from_bytes(
                client_id="task-stream-client",
                blob=blob,
                runtime="py3",
                entry_module="task_stream_demo",
                entry_callable="run",
            )
            with client.open_task_stream(
                client_id="task-stream-client",
                code_version=upload.code_version,
                result_limit=10,
                result_wait_ms=100,
            ) as stream:
                submit = stream.submit_tasks(
                    [
                        pb2.TaskSubmitItem(task_id="stream-task-1", payload={"value": 2}, priority=1, runtime_key="demo-runtime"),
                        pb2.TaskSubmitItem(task_id="stream-task-2", payload={"value": 3}, priority=1, runtime_key="demo-runtime"),
                    ],
                    job_id="job-stream-demo",
                )
                assert [item.task_id for item in submit.accepted] == ["stream-task-1", "stream-task-2"]

                cancel = stream.cancel_job(job_id="job-stream-demo", reason="stream cancel")
                assert cancel.queued_cancelled == 2
                assert cancel.running_marked == 0

                pulled = stream.pull_results(limit=10, wait_ms=500)
                assert sorted(item.task_id for item in pulled.results) == ["stream-task-1", "stream-task-2"]
                assert {item.job_id for item in pulled.results} == {"job-stream-demo"}
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
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        )
        with TaskBatchClient.from_infocenter(
            infocenter_target=info_server.base_url,
            client_id="task-batch-client",
            job_id="job-batch",
            blob=blob,
            runtime="py3",
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


def test_service_session_http_supports_nested_dataframe_series_ndarray(tmp_path):
    pd = pytest.importorskip("pandas")
    np = pytest.importorskip("numpy")

    state = NodeControlState(
        node_id="node-client-http-serialize-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_http_nested"),
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
            b"def run(bundle=None, **_kwargs):\n"
            b"    return {\n"
            b"        'df_rows': int(bundle['df'].shape[0]),\n"
            b"        'series_name': str(bundle['series'].name),\n"
            b"        'arr_sum': int(bundle['arr'].sum()),\n"
            b"    }\n"
        )
        with NodeControlClient(target, timeout_sec=10.0) as client:
            session = client.create_service_from_bytes(
                owner_client_id="owner-http-client",
                service_name="svc-http-nested",
                blob=blob,
                runtime="py3",
                entry_module="svc_http_nested",
                entry_callable="run",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=True,
            )
            resp = session.call(
                "run",
                {
                    "bundle": {
                        "df": pd.DataFrame([{"x": 1}, {"x": 2}]),
                        "series": pd.Series([10, 20], name="alpha"),
                        "arr": np.array([3, 4, 5], dtype=np.int64),
                    }
                },
                timeout_sec=10.0,
            )
        assert resp["ok"] is True
        assert resp["data"] == {"df_rows": 2, "series_name": "alpha", "arr_sum": 12}
    finally:
        server.stop(grace=0)
        state.close()


def test_service_session_call_auto_resolves_object_ref_to_local_path(tmp_path):
    state = NodeControlState(
        node_id="node-client-http-object-ref-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_http_object_ref"),
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

    data_file = tmp_path / "shared.txt"
    data_file.write_text("hello-object-ref\n", encoding="utf-8")

    try:
        blob = (
            b"from pathlib import Path\n\n"
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(dataset=None, **_kwargs):\n"
            b"    return {\n"
            b"        'cls': dataset.__class__.__name__,\n"
            b"        'exists': bool(dataset.exists()),\n"
            b"        'name': dataset.name,\n"
            b"        'content': dataset.read_text(encoding='utf-8').strip(),\n"
            b"    }\n"
        )
        with NodeControlClient(target, timeout_sec=10.0) as client:
            ref = client.upload_object_from_file(file_path=str(data_file), format="txt")
            session = client.create_service_from_bytes(
                owner_client_id="owner-object-ref",
                service_name="svc-object-ref",
                blob=blob,
                runtime="py3",
                entry_module="svc_object_ref",
                entry_callable="run",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=True,
            )
            resp = session.call("run", {"dataset": ref}, timeout_sec=10.0)
        assert resp["ok"] is True
        assert resp["data"]["cls"] == "PosixPath"
        assert resp["data"]["exists"] is True
        assert resp["data"]["name"].endswith(".txt")
        assert resp["data"]["content"] == "hello-object-ref"
    finally:
        server.stop(grace=0)
        state.close()


def test_task_batch_grpc_supports_nested_dataframe_series_ndarray(tmp_path):
    pd = pytest.importorskip("pandas")
    np = pytest.importorskip("numpy")

    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()

    state = NodeControlState(
        node_id="node-client-grpc-serialize-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_grpc_nested"),
        enable_internal_executor=True,
        enable_service_session=False,
        monitor_interval_sec=1,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    target = f"127.0.0.1:{port}"

    try:
        with InfoCenterClient(info_server.base_url, timeout_sec=5.0) as infocenter:
            infocenter.register_node(
                node_id="node-client-grpc-serialize-01",
                control_addr=target,
                capacity=8,
                queue_capacity=16,
                tags=["compute"],
                services=[],
                service_worker_capacity=0,
                service_worker_used=0,
            )

        blob = (
            b"def run(bundle=None, **_kwargs):\n"
            b"    return {\n"
            b"        'df_rows': int(bundle['df'].shape[0]),\n"
            b"        'series_name': str(bundle['series'].name),\n"
            b"        'arr_sum': int(bundle['arr'].sum()),\n"
            b"    }\n"
        )
        with TaskBatchClient.from_infocenter(
            infocenter_target=info_server.base_url,
            client_id="grpc-serialize-client",
            job_id="job-grpc-serialize",
            blob=blob,
            runtime="py3",
            entry_module="task_grpc_nested",
            entry_callable="run",
            timeout_sec=10.0,
        ) as batch:
            submit = batch.submit_payloads(
                [
                    {
                        "bundle": {
                            "df": pd.DataFrame([{"x": 1}, {"x": 2}]),
                            "series": pd.Series([10, 20], name="alpha"),
                            "arr": np.array([3, 4, 5], dtype=np.int64),
                        }
                    }
                ]
            )
            assert len(submit.accepted) == 1

            pulled = batch.pull_results(limit=10, wait_ms=3000)
            assert len(pulled.results) == 1
            assert pulled.results[0].status == pb2.TASK_STATUS_SUCCEEDED
            assert dict(pulled.results[0].result) == {
                "df_rows": 2,
                "series_name": "alpha",
                "arr_sum": 12,
            }
    finally:
        server.stop(grace=0)
        state.close()
        info_server.stop()


def test_task_batch_auto_resolves_object_ref_to_local_path(tmp_path):
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()

    state = NodeControlState(
        node_id="node-client-grpc-object-ref-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_grpc_object_ref"),
        enable_internal_executor=True,
        enable_service_session=False,
        monitor_interval_sec=1,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    target = f"127.0.0.1:{port}"

    data_file = tmp_path / "shared-task.txt"
    data_file.write_text("hello-task-object-ref\n", encoding="utf-8")

    try:
        with InfoCenterClient(info_server.base_url, timeout_sec=5.0) as infocenter:
            infocenter.register_node(
                node_id="node-client-grpc-object-ref-01",
                control_addr=target,
                capacity=8,
                queue_capacity=16,
                tags=["compute"],
                services=[],
                service_worker_capacity=0,
                service_worker_used=0,
            )

        blob = (
            b"def run(dataset=None, **_kwargs):\n"
            b"    return {\n"
            b"        'cls': dataset.__class__.__name__,\n"
            b"        'exists': bool(dataset.exists()),\n"
            b"        'name': dataset.name,\n"
            b"        'content': dataset.read_text(encoding='utf-8').strip(),\n"
            b"    }\n"
        )
        with TaskBatchClient.from_infocenter(
            infocenter_target=info_server.base_url,
            client_id="grpc-object-ref-client",
            job_id="job-grpc-object-ref",
            blob=blob,
            runtime="py3",
            entry_module="task_grpc_object_ref",
            entry_callable="run",
            timeout_sec=10.0,
        ) as batch:
            ref = batch.put_object_from_file(str(data_file), format="txt")
            submit = batch.submit_payloads([{"dataset": ref}])
            assert len(submit.accepted) == 1

            pulled = batch.pull_results(limit=10, wait_ms=3000)
            assert len(pulled.results) == 1
            assert pulled.results[0].status == pb2.TASK_STATUS_SUCCEEDED
            result = dict(pulled.results[0].result)
            assert result["cls"] == "PosixPath"
            assert result["exists"] is True
            assert result["name"].endswith(".txt")
            assert result["content"] == "hello-task-object-ref"
    finally:
        server.stop(grace=0)
        state.close()
        info_server.stop()


def test_task_batch_put_data_json_auto_resolves_to_dict(tmp_path):
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()

    state = NodeControlState(
        node_id="node-client-grpc-object-json-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_grpc_object_json"),
        enable_internal_executor=True,
        enable_service_session=False,
        monitor_interval_sec=1,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    target = f"127.0.0.1:{port}"

    try:
        with InfoCenterClient(info_server.base_url, timeout_sec=5.0) as infocenter:
            infocenter.register_node(
                node_id="node-client-grpc-object-json-01",
                control_addr=target,
                capacity=8,
                queue_capacity=16,
                tags=["compute"],
                services=[],
                service_worker_capacity=0,
                service_worker_used=0,
            )

        blob = (
            b"def run(config=None, **_kwargs):\n"
            b"    return {\n"
            b"        'cls': config.__class__.__name__,\n"
            b"        'count': int(config['count']),\n"
            b"        'name': str(config['name']),\n"
            b"    }\n"
        )
        with TaskBatchClient.from_infocenter(
            infocenter_target=info_server.base_url,
            client_id="grpc-object-json-client",
            job_id="job-grpc-object-json",
            blob=blob,
            runtime="py3",
            entry_module="task_grpc_object_json",
            entry_callable="run",
            timeout_sec=10.0,
        ) as batch:
            ref = batch.put_data({"count": 3, "name": "demo"})
            submit = batch.submit_payloads([{"config": ref}])
            assert len(submit.accepted) == 1

            pulled = batch.pull_results(limit=10, wait_ms=3000)
            assert len(pulled.results) == 1
            assert pulled.results[0].status == pb2.TASK_STATUS_SUCCEEDED
            assert dict(pulled.results[0].result) == {
                "cls": "dict",
                "count": 3,
                "name": "demo",
            }
    finally:
        server.stop(grace=0)
        state.close()
        info_server.stop()


def test_task_batch_put_dataframe_auto_resolves_to_dataframe(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()

    state = NodeControlState(
        node_id="node-client-grpc-object-df-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_grpc_object_df"),
        enable_internal_executor=True,
        enable_service_session=False,
        monitor_interval_sec=1,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    target = f"127.0.0.1:{port}"

    try:
        with InfoCenterClient(info_server.base_url, timeout_sec=5.0) as infocenter:
            infocenter.register_node(
                node_id="node-client-grpc-object-df-01",
                control_addr=target,
                capacity=8,
                queue_capacity=16,
                tags=["compute"],
                services=[],
                service_worker_capacity=0,
                service_worker_used=0,
            )

        blob = (
            b"def run(dataset=None, **_kwargs):\n"
            b"    return {\n"
            b"        'cls': dataset.__class__.__name__,\n"
            b"        'rows': int(dataset.shape[0]),\n"
            b"        'cols': int(dataset.shape[1]),\n"
            b"    }\n"
        )
        with TaskBatchClient.from_infocenter(
            infocenter_target=info_server.base_url,
            client_id="grpc-object-df-client",
            job_id="job-grpc-object-df",
            blob=blob,
            runtime="py3",
            entry_module="task_grpc_object_df",
            entry_callable="run",
            timeout_sec=10.0,
        ) as batch:
            ref = batch.put_data(pd.DataFrame([{"x": 1}, {"x": 2}, {"x": 3}]))
            submit = batch.submit_payloads([{"dataset": ref}])
            assert len(submit.accepted) == 1

            pulled = batch.pull_results(limit=10, wait_ms=3000)
            assert len(pulled.results) == 1
            assert pulled.results[0].status == pb2.TASK_STATUS_SUCCEEDED
            assert dict(pulled.results[0].result) == {
                "cls": "DataFrame",
                "rows": 3,
                "cols": 1,
            }
    finally:
        server.stop(grace=0)
        state.close()
        info_server.stop()


def test_task_batch_submit_payloads_rejects_oversized_inline_payload(tmp_path):
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()

    state = NodeControlState(
        node_id="node-client-inline-limit-task-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_inline_limit_task"),
        enable_internal_executor=True,
        enable_service_session=False,
        monitor_interval_sec=1,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    target = f"127.0.0.1:{port}"

    try:
        with InfoCenterClient(info_server.base_url, timeout_sec=5.0) as infocenter:
            infocenter.register_node(
                node_id="node-client-inline-limit-task-01",
                control_addr=target,
                capacity=8,
                queue_capacity=16,
                tags=["compute"],
                services=[],
                service_worker_capacity=0,
                service_worker_used=0,
            )

        blob = (
            b"def run(blob=None, **_kwargs):\n"
            b"    return {'size': len(str(blob or ''))}\n"
        )
        with TaskBatchClient.from_infocenter(
            infocenter_target=info_server.base_url,
            client_id="inline-limit-task-client",
            job_id="job-inline-limit-task",
            blob=blob,
            runtime="py3",
            entry_module="task_inline_limit",
            entry_callable="run",
            timeout_sec=10.0,
        ) as batch:
            with pytest.raises(ValueError, match="ObjectRef"):
                batch.submit_payloads([_oversized_inline_payload()])
    finally:
        server.stop(grace=0)
        state.close()
        info_server.stop()


def test_submit_tasks_grpc_rejects_oversized_inline_payload_server_side(tmp_path):
    state = NodeControlState(
        node_id="node-client-inline-limit-submit-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_inline_limit_submit"),
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
        with NodeControlClient(target, timeout_sec=10.0) as client:
            with pytest.raises(grpc.RpcError) as excinfo:
                client.stub.SubmitTasks(
                    pb2.SubmitTasksRequest(
                        client_id="inline-limit-submit-client",
                        code_version="sha256:test-inline-limit",
                        tasks=[
                            pb2.TaskSubmitItem(
                                task_id="task-inline-limit-0001",
                                payload=dict_to_struct(_oversized_inline_payload()),
                            )
                        ],
                    ),
                    timeout=5.0,
                )
        assert excinfo.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert "task payload" in excinfo.value.details()
        assert "ObjectRef" in excinfo.value.details()
    finally:
        server.stop(grace=0)
        state.close()


def test_service_session_call_rejects_oversized_inline_payload_before_http(tmp_path):
    state = NodeControlState(
        node_id="node-client-inline-limit-service-http-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_inline_limit_service_http"),
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
            b"def run(blob=None, **_kwargs):\n"
            b"    return {'size': len(str(blob or ''))}\n"
        )
        with NodeControlClient(target, timeout_sec=10.0) as client:
            session = client.create_service_from_bytes(
                owner_client_id="owner-inline-limit-http",
                service_name="svc-inline-limit-http",
                blob=blob,
                runtime="py3",
                entry_module="svc_inline_limit_http",
                entry_callable="run",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=True,
            )
            with pytest.raises(ValueError, match="ObjectRef"):
                session.call("run", _oversized_inline_payload(), timeout_sec=10.0)
    finally:
        server.stop(grace=0)
        state.close()


def test_call_service_grpc_rejects_oversized_inline_payload_server_side(tmp_path):
    state = NodeControlState(
        node_id="node-client-inline-limit-call-grpc-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_inline_limit_call_grpc"),
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
            b"def run(blob=None, **_kwargs):\n"
            b"    return {'size': len(str(blob or ''))}\n"
        )
        with NodeControlClient(target, timeout_sec=10.0) as client:
            session = client.create_service_from_bytes(
                owner_client_id="owner-inline-limit-grpc",
                service_name="svc-inline-limit-grpc",
                blob=blob,
                runtime="py3",
                entry_module="svc_inline_limit_grpc",
                entry_callable="run",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=True,
            )
            with pytest.raises(grpc.RpcError) as excinfo:
                client.stub.CallService(
                    pb2.CallServiceRequest(
                        service_id=session.service_id,
                        method="run",
                        payload=dict_to_struct(_oversized_inline_payload()),
                        timeout_sec=5.0,
                        service_token=session.service_token,
                    ),
                    timeout=5.0,
                )
        assert excinfo.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert "service call payload" in excinfo.value.details()
        assert "ObjectRef" in excinfo.value.details()
    finally:
        server.stop(grace=0)
        state.close()


def test_service_http_gateway_rejects_oversized_inline_payload_server_side(tmp_path):
    state = NodeControlState(
        node_id="node-client-inline-limit-call-http-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_inline_limit_call_http"),
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
            b"def run(blob=None, **_kwargs):\n"
            b"    return {'size': len(str(blob or ''))}\n"
        )
        with NodeControlClient(target, timeout_sec=10.0) as client:
            session = client.create_service_from_bytes(
                owner_client_id="owner-inline-limit-http-server",
                service_name="svc-inline-limit-http-server",
                blob=blob,
                runtime="py3",
                entry_module="svc_inline_limit_http_server",
                entry_callable="run",
                worker_count=1,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=True,
            )

            req = Request(
                f"{session.http_base_url}/call/run",
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "X-Service-Token": session.service_token,
                },
                data=json.dumps(_oversized_inline_payload()).encode("utf-8"),
            )
            with pytest.raises(HTTPError) as excinfo:
                urlopen(req, timeout=5.0)
        assert excinfo.value.code == 400
        body = json.loads(excinfo.value.read().decode("utf-8") or "{}")
        assert body["ok"] is False
        assert "ObjectRef" in body["error"]
    finally:
        server.stop(grace=0)
        state.close()


def test_task_batch_dataframe_result_returns_result_ref_and_fetches(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()

    state = NodeControlState(
        node_id="node-client-result-ref-df-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_result_ref_df"),
        enable_internal_executor=True,
        enable_service_session=False,
        monitor_interval_sec=1,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    target = f"127.0.0.1:{port}"

    try:
        with InfoCenterClient(info_server.base_url, timeout_sec=5.0) as infocenter:
            infocenter.register_node(
                node_id="node-client-result-ref-df-01",
                control_addr=target,
                capacity=8,
                queue_capacity=16,
                tags=["compute"],
                services=[],
                service_worker_capacity=0,
                service_worker_used=0,
            )

        blob = (
            b"import pandas as pd\n"
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return pd.DataFrame([{'x': value}, {'x': value + 1}, {'x': value + 2}])\n"
        )
        with TaskBatchClient.from_infocenter(
            infocenter_target=info_server.base_url,
            client_id="result-ref-df-client",
            job_id="job-result-ref-df",
            blob=blob,
            runtime="py3",
            entry_module="task_result_ref_df",
            entry_callable="run",
            timeout_sec=10.0,
        ) as batch:
            submit = batch.submit_payloads([{"value": 7}])
            assert len(submit.accepted) == 1

            pulled = batch.pull_results(limit=10, wait_ms=3000)
            assert len(pulled.results) == 1
            assert pulled.results[0].status == pb2.TASK_STATUS_SUCCEEDED
            result_value = struct_to_dict(pulled.results[0].result)
            assert isinstance(result_value, ResultRef)
            assert result_value.node_id == "node-client-result-ref-df-01"
            frame = batch.fetch_result_data(pulled.results[0])
            assert list(frame["x"]) == [7, 8, 9]
    finally:
        server.stop(grace=0)
        state.close()
        info_server.stop()


def test_task_batch_large_inline_result_fails_with_file_guidance(tmp_path):
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()

    state = NodeControlState(
        node_id="node-client-result-inline-fail-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_result_inline_fail"),
        enable_internal_executor=True,
        enable_service_session=False,
        monitor_interval_sec=1,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    target = f"127.0.0.1:{port}"

    try:
        with InfoCenterClient(info_server.base_url, timeout_sec=5.0) as infocenter:
            infocenter.register_node(
                node_id="node-client-result-inline-fail-01",
                control_addr=target,
                capacity=8,
                queue_capacity=16,
                tags=["compute"],
                services=[],
                service_worker_capacity=0,
                service_worker_used=0,
            )

        blob = (
            b"def run(**_kwargs):\n"
            b"    return {'blob': 'x' * (1024 * 1024 + 1024)}\n"
        )
        with TaskBatchClient.from_infocenter(
            infocenter_target=info_server.base_url,
            client_id="result-inline-fail-client",
            job_id="job-result-inline-fail",
            blob=blob,
            runtime="py3",
            entry_module="task_result_inline_fail",
            entry_callable="run",
            timeout_sec=10.0,
        ) as batch:
            submit = batch.submit_payloads([{}])
            assert len(submit.accepted) == 1

            pulled = batch.pull_results(limit=10, wait_ms=3000)
            assert len(pulled.results) == 1
            assert pulled.results[0].status == pb2.TASK_STATUS_FAILED_USER
            assert "node-local file" in pulled.results[0].error.message
            assert "stream chunks" in pulled.results[0].error.message
    finally:
        server.stop(grace=0)
        state.close()
        info_server.stop()
