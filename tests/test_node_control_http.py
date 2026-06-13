from __future__ import annotations

import base64
import json
import threading
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pycloud_parallel.controlplane.node_control_http import HttpNodeControlClient, NodeControlHttpServer, _message_to_dict
from pycloud_parallel.controlplane.nodecontrol_state import CreateRequestStillCreating, NodeControlState
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


def _start_http_node_with_api_token(tmp_path, token: str):
    state = NodeControlState(
        node_id="node-http-control-auth",
        queue_capacity=32,
        worker_capacity=4,
        artifact_dir=str(tmp_path / "node_http_control_auth"),
        service_http_bind="127.0.0.1:0",
    )
    server = NodeControlHttpServer(bind="127.0.0.1:0", state=state, api_token=token)
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
            assert client.last_heartbeat_resource["alive_workers"] == session.worker_count
            assert client.last_heartbeat_resource["method_failures"] == {}
            assert client.get_service_status(service_id=session.service_id).service_id == session.service_id
            assert client.end_service(
                owner_client_id="owner-http",
                service_id=session.service_id,
                service_token=session.service_token,
            ).accepted is True
    finally:
        server.stop()
        state.close()


def test_http_handler_unexpected_exception_returns_json_500(tmp_path):
    server, state = _start_http_node(tmp_path)

    def _raise_handler_error(*_args, **_kwargs):
        raise RuntimeError("boom during create")

    server.app.handle_post = _raise_handler_error
    try:
        req = Request(
            f"{server.base_url}/services",
            method="POST",
            headers={"Content-Type": "application/json; charset=utf-8"},
            data=b"{}",
        )
        status_code = 0
        try:
            urlopen(req, timeout=5.0)
        except HTTPError as exc:
            status_code = int(exc.code)
            body = json.loads(exc.read().decode("utf-8"))
        else:
            raise AssertionError("expected HTTP 500")

        assert status_code == 500
        assert body["ok"] is False
        assert "NodeControl internal error: RuntimeError: boom during create" in body["error"]
    finally:
        server.stop()
        state.close()


def test_http_end_service_requires_owner_token(tmp_path):
    server, state = _start_http_node(tmp_path)
    blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value) + 1}\n"
    try:
        with HttpNodeControlClient(server.base_url, timeout_sec=10.0) as client:
            session = client.create_service_from_bytes(
                owner_client_id="owner-http",
                service_name="svc-http-owner-token",
                blob=blob,
                runtime="py3",
                entry_module="svc_http_owner_token",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                expose_http=False,
            )
            try:
                client.end_service(
                    owner_client_id="owner-http",
                    service_id=session.service_id,
                    service_token="",
                )
            except RuntimeError as exc:
                assert "service_token mismatch" in str(exc)
            else:
                raise AssertionError("end_service without service_token should fail")

            try:
                client.end_service(
                    owner_client_id="not-owner",
                    service_id=session.service_id,
                    service_token=session.service_token,
                )
            except RuntimeError as exc:
                assert "owner_client_id mismatch" in str(exc)
            else:
                raise AssertionError("end_service with wrong owner_client_id should fail")

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
            assert client.last_heartbeat_resource["alive_workers"] == pool.worker_count
            assert client.last_heartbeat_resource["method_failures"] == {}
            assert pool.get_status().pool_id == pool.pool_id
            assert pool.close().accepted is True
    finally:
        server.stop()
        state.close()


def test_http_resource_signals_and_progress_endpoints(tmp_path):
    server, state = _start_http_node(tmp_path)
    service_blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
    pool_blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
    try:
        with HttpNodeControlClient(server.base_url, timeout_sec=10.0) as client:
            service = client.create_service_from_bytes(
                owner_client_id="owner-http-signals",
                service_name="svc-http-signals",
                blob=service_blob,
                runtime="py3",
                entry_module="svc_http_signals",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                expose_http=False,
            )
            pool = client.create_task_pool_from_bytes(
                owner_client_id="owner-http-signals",
                pool_name="pool-http-signals",
                blob=pool_blob,
                runtime="py3",
                entry_module="pool_http_signals",
                entry_callable="run",
                package_format="py",
                worker_count=1,
            )
            service.heartbeat()
            pool.heartbeat()

            service_progress = client.get_service_progress(service_id=service.service_id)
            pool_progress = client.get_task_pool_progress(pool_id=pool.pool_id)
            signals = client.list_resource_signals(since=0, limit=50)

            assert service_progress["readiness"] == "ready"
            assert service_progress["create_stage"] in {"ready", "create_succeeded"}
            assert service_progress["status"] == pb2.SERVICE_STATUS_RUNNING
            assert service_progress["latest_signal_seq"] > 0
            assert pool_progress["readiness"] == "ready"
            assert pool_progress["create_stage"] in {"ready", "create_succeeded"}
            assert pool_progress["status"] == "RUNNING"
            assert pool_progress["latest_signal_seq"] > 0
            assert any(item["resource_id"] == service.service_id for item in signals["signals"])
            assert any(item["resource_id"] == pool.pool_id for item in signals["signals"])
    finally:
        server.stop()
        state.close()


def test_http_create_wait_ready_false_returns_progress_before_ready(tmp_path, monkeypatch):
    server, state = _start_http_node(tmp_path)
    artifact_ready_started = threading.Event()
    release_artifact_ready = threading.Event()
    original_ensure_artifact_ready = state._ensure_artifact_ready  # noqa: SLF001

    def _slow_ensure_artifact_ready(*args, **kwargs):
        artifact_ready_started.set()
        release_artifact_ready.wait(timeout=5.0)
        return original_ensure_artifact_ready(*args, **kwargs)

    monkeypatch.setattr(state, "_ensure_artifact_ready", _slow_ensure_artifact_ready)
    service_blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
    pool_blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
    try:
        with HttpNodeControlClient(server.base_url, timeout_sec=10.0) as client:
            service = client.create_service_from_bytes(
                owner_client_id="owner-http-async",
                service_name="svc-http-async",
                blob=service_blob,
                runtime="py3",
                entry_module="svc_http_async",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                expose_http=False,
                create_request_id="http-service-async-1",
                wait_ready=False,
            )
            assert service.readiness == "initializing"
            assert service.create_stage in {"accepted", "artifact_prepare"}
            assert service.operation_id == "http-service-async-1"
            assert artifact_ready_started.wait(timeout=2.0)
            service.heartbeat()
            service_progress = client.get_service_progress(service_id=service.service_id)
            assert service_progress["readiness"] == "initializing"
            assert service_progress["operation_id"] == "http-service-async-1"
            assert service_progress["operation_updated_at"]

            artifact_ready_started.clear()
            pool = client.create_task_pool_from_bytes(
                owner_client_id="owner-http-async",
                pool_name="pool-http-async",
                blob=pool_blob,
                runtime="py3",
                entry_module="pool_http_async",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                create_request_id="http-pool-async-1",
                wait_ready=False,
            )
            assert pool.readiness == "initializing"
            assert pool.create_stage in {"accepted", "artifact_prepare"}
            assert pool.operation_id == "http-pool-async-1"
            assert artifact_ready_started.wait(timeout=2.0)
            pool.heartbeat()
            pool_progress = client.get_task_pool_progress(pool_id=pool.pool_id)
            assert pool_progress["readiness"] == "initializing"
            assert pool_progress["operation_id"] == "http-pool-async-1"
            assert pool_progress["operation_updated_at"]
    finally:
        release_artifact_ready.set()
        server.stop()
        state.close()


def test_http_create_service_request_id_is_idempotent(tmp_path):
    server, state = _start_http_node(tmp_path)
    blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value) + 1}\n"
    try:
        with HttpNodeControlClient(server.base_url, timeout_sec=10.0) as client:
            first = client.create_service_from_bytes(
                owner_client_id="owner-http-idem",
                service_name="svc-http-idem",
                blob=blob,
                runtime="py3",
                entry_module="svc_http_idem",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                expose_http=False,
                create_request_id="http-service-create-idem-1",
            )
            second = client.create_service_from_bytes(
                owner_client_id="owner-http-idem",
                service_name="svc-http-idem",
                blob=blob,
                runtime="py3",
                entry_module="svc_http_idem",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                expose_http=False,
                create_request_id="http-service-create-idem-1",
            )

            assert second.service_id == first.service_id
            assert second.service_token == first.service_token
            assert len(state.service_reports()) == 1
    finally:
        server.stop()
        state.close()


def test_http_create_taskpool_request_id_is_idempotent(tmp_path):
    server, state = _start_http_node(tmp_path)
    blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value) * 2}\n"
    try:
        with HttpNodeControlClient(server.base_url, timeout_sec=10.0) as client:
            first = client.create_task_pool_from_bytes(
                owner_client_id="owner-http-idem",
                pool_name="pool-http-idem",
                blob=blob,
                runtime="py3",
                entry_module="pool_http_idem",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                create_request_id="http-taskpool-create-idem-1",
            )
            second = client.create_task_pool_from_bytes(
                owner_client_id="owner-http-idem",
                pool_name="pool-http-idem",
                blob=blob,
                runtime="py3",
                entry_module="pool_http_idem",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                create_request_id="http-taskpool-create-idem-1",
            )

            assert second.pool_id == first.pool_id
            assert second.pool_token == first.pool_token
            assert len(state.task_pool_reports()) == 1
    finally:
        server.stop()
        state.close()


def test_http_create_service_still_creating_returns_409(tmp_path):
    server, state = _start_http_node(tmp_path)

    def _still_creating(**_kwargs):
        raise CreateRequestStillCreating("service create_request_id still creating")

    state.create_service = _still_creating
    meta = pb2.CreateServiceMeta(
        owner_client_id="owner-http-create-wait",
        service_name="svc-http-create-wait",
        sha256="sha256:abc",
        runtime="py3",
        entry_module="svc_http_create_wait",
        entry_callable="run",
        worker_count=1,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        expose_http=False,
        package_format="py",
    )
    payload = {
        "meta": _message_to_dict(meta),
        "code_b64": base64.b64encode(b"def run(**_kwargs):\n    return {'ok': True}\n").decode("ascii"),
        "create_request_id": "http-service-still-creating-1",
    }
    try:
        req = Request(
            f"{server.base_url}/services",
            method="POST",
            headers={"Content-Type": "application/json; charset=utf-8"},
            data=json.dumps(payload).encode("utf-8"),
        )
        try:
            urlopen(req, timeout=5.0)
        except HTTPError as exc:
            body = json.loads(exc.read().decode("utf-8"))
            assert exc.code == 409
            assert body["ok"] is False
            assert "still creating" in body["error"]
        else:
            raise AssertionError("expected HTTP 409")
    finally:
        server.stop()
        state.close()


def test_http_create_taskpool_still_creating_returns_409(tmp_path):
    server, state = _start_http_node(tmp_path)

    def _still_creating(**_kwargs):
        raise CreateRequestStillCreating("task_pool create_request_id still creating")

    state.create_task_pool = _still_creating
    meta = pb2.CreateTaskPoolMeta(
        owner_client_id="owner-http-create-wait",
        pool_name="pool-http-create-wait",
        sha256="sha256:abc",
        runtime="py3",
        entry_module="pool_http_create_wait",
        entry_callable="run",
        worker_count=1,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        package_format="py",
    )
    payload = {
        "meta": _message_to_dict(meta),
        "code_b64": base64.b64encode(b"def run(**_kwargs):\n    return {'ok': True}\n").decode("ascii"),
        "create_request_id": "http-taskpool-still-creating-1",
    }
    try:
        req = Request(
            f"{server.base_url}/taskpools",
            method="POST",
            headers={"Content-Type": "application/json; charset=utf-8"},
            data=json.dumps(payload).encode("utf-8"),
        )
        try:
            urlopen(req, timeout=5.0)
        except HTTPError as exc:
            body = json.loads(exc.read().decode("utf-8"))
            assert exc.code == 409
            assert body["ok"] is False
            assert "still creating" in body["error"]
        else:
            raise AssertionError("expected HTTP 409")
    finally:
        server.stop()
        state.close()


def test_http_taskpool_submit_closed_pool_returns_structured_error(tmp_path):
    server, state = _start_http_node(tmp_path)
    blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value) * 2}\n"
    try:
        with HttpNodeControlClient(server.base_url, timeout_sec=10.0) as client:
            pool = client.create_task_pool_from_bytes(
                owner_client_id="owner-http-closed-pool",
                pool_name="pool-http-closed-pool",
                blob=blob,
                runtime="py3",
                entry_module="pool_http_closed_pool",
                entry_callable="run",
                package_format="py",
                worker_count=1,
            )
            assert pool.close().accepted is True
            task = pb2.TaskSubmitItem(
                task_id="task-http-closed-pool",
                payload=dict_to_struct({"value": 4}),
                timeout_hint_sec=10,
            )
            try:
                pool.submit_tasks([task], job_id="job-http-closed-pool")
            except RuntimeError as exc:
                assert "task pool not running" in str(exc)
            else:
                raise AssertionError("submit to closed task pool should fail")
    finally:
        server.stop()
        state.close()


def test_http_create_service_initial_globals_visible_before_first_call(tmp_path):
    server, state = _start_http_node(tmp_path)
    blob = (
        b"cfg = {}\n\n"
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value) + int(cfg.get('offset', 0))}\n"
    )
    try:
        with HttpNodeControlClient(server.base_url, timeout_sec=10.0) as client:
            session = client.create_service_from_bytes(
                owner_client_id="owner-http",
                service_name="svc-http-initial-globals",
                blob=blob,
                runtime="py3",
                entry_module="svc_http_initial_globals",
                entry_callable="run",
                package_format="py",
                managed_global_names=["cfg"],
                initial_globals={"cfg": {"offset": 5}},
                worker_count=1,
                expose_http=False,
            )
            response = client.call_service(
                service_id=session.service_id,
                method="run",
                payload={"value": 2},
                service_token=session.service_token,
                timeout_sec=10.0,
            )
            assert struct_to_dict(response.data) == {"value": 7}
    finally:
        server.stop()
        state.close()


def test_http_create_taskpool_initial_globals_visible_before_submit(tmp_path):
    server, state = _start_http_node(tmp_path)
    blob = (
        b"cfg = {}\n\n"
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value) + int(cfg.get('offset', 0))}\n"
    )
    try:
        with HttpNodeControlClient(server.base_url, timeout_sec=10.0) as client:
            pool = client.create_task_pool_from_bytes(
                owner_client_id="owner-http",
                pool_name="pool-http-initial-globals",
                blob=blob,
                runtime="py3",
                entry_module="pool_http_initial_globals",
                entry_callable="run",
                package_format="py",
                initial_globals={"cfg": {"offset": 6}},
                worker_count=1,
            )
            task = pb2.TaskSubmitItem(
                task_id="task-http-initial-globals",
                payload=dict_to_struct({"value": 4}),
                timeout_hint_sec=10,
            )
            submitted = pool.submit_tasks([task], job_id="job-http-initial-globals")
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
            assert struct_to_dict(result.result) == {"value": 10}
    finally:
        server.stop()
        state.close()


def test_http_create_service_requires_owner_api_token(tmp_path):
    server, state = _start_http_node_with_api_token(tmp_path, "owner-secret")
    blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value) + 1}\n"
    try:
        with HttpNodeControlClient(server.base_url, timeout_sec=10.0) as client:
            try:
                client.create_service_from_bytes(
                    owner_client_id="owner-http",
                    service_name="svc-auth",
                    blob=blob,
                    runtime="py3",
                    entry_module="svc_auth_demo",
                    entry_callable="run",
                    package_format="py",
                    worker_count=1,
                    expose_http=False,
                )
                assert False, "expected missing api token to fail"
            except RuntimeError as exc:
                assert "owner api token is required" in str(exc)

            session = client.create_service_from_bytes(
                owner_client_id="owner-http",
                service_name="svc-auth",
                blob=blob,
                runtime="py3",
                entry_module="svc_auth_demo",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                expose_http=False,
                api_token="owner-secret",
            )
            assert session.service_id
            response = client.call_service(
                service_id=session.service_id,
                method="run",
                payload={"value": 2},
                service_token=session.service_token,
                timeout_sec=10.0,
            )
            assert struct_to_dict(response.data) == {"value": 3}
            assert client.end_service(
                owner_client_id="owner-http",
                service_id=session.service_id,
                service_token=session.service_token,
            ).accepted is True
    finally:
        server.stop()
        state.close()


def test_http_create_taskpool_requires_owner_api_token(tmp_path):
    server, state = _start_http_node_with_api_token(tmp_path, "owner-secret")
    blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value) * 2}\n"
    try:
        with HttpNodeControlClient(server.base_url, timeout_sec=10.0) as client:
            try:
                client.create_task_pool_from_bytes(
                    owner_client_id="owner-http",
                    pool_name="pool-auth",
                    blob=blob,
                    runtime="py3",
                    entry_module="pool_auth_demo",
                    entry_callable="run",
                    package_format="py",
                    worker_count=1,
                )
                assert False, "expected missing api token to fail"
            except RuntimeError as exc:
                assert "owner api token is required" in str(exc)

            pool = client.create_task_pool_from_bytes(
                owner_client_id="owner-http",
                pool_name="pool-auth",
                blob=blob,
                runtime="py3",
                entry_module="pool_auth_demo",
                entry_callable="run",
                package_format="py",
                worker_count=1,
                api_token="owner-secret",
            )
            assert pool.pool_id
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
