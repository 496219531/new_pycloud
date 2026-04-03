"""中文说明：验证 gRPC 控制面的核心状态流转（内存后端）。"""

import hashlib
import json
import sys
import time
from datetime import timedelta
from urllib.request import Request, urlopen

import pytest

from pycloud_parallel.controlplane.state import NodeControlState, dict_to_struct, struct_to_dict, utc_now
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def _seed_code(state: NodeControlState) -> str:
    blob = (
        b"def run(value=0, should_fail=False, **_kwargs):\n"
        b"    value = int(value)\n"
        b"    if should_fail:\n"
        b"        raise ValueError(f'intentional failure value={value}')\n"
        b"    return {'input': value, 'output': value * value}\n"
    )
    digest = hashlib.sha256(blob).hexdigest()
    artifact, _ = state.put_code(
        sha256=f"sha256:{digest}",
        runtime="py3",
        entry_module="demo",
        entry_callable="run",
        package_format="py",
        chunks=[blob],
    )
    return artifact.code_version


def test_submit_poll_report_and_pull_results(tmp_path):
    state = NodeControlState(
        node_id="node-test-01",
        queue_capacity=16,
        worker_capacity=4,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        code_version = _seed_code(state)
        submit_req = pb2.SubmitTasksRequest(
            client_id="client-a",
            code_version=code_version,
            execution_mode=pb2.EXECUTION_MODE_PERSISTENT,
            job_id="job-alpha",
            tasks=[
                pb2.TaskSubmitItem(task_id="task-1", payload={"x": 1}, timeout_hint_sec=10, priority=1),
            ],
        )
        accepted, rejected, _credit = state.submit_tasks(submit_req)
        assert [x.task_id for x in accepted] == ["task-1"]
        assert rejected == []

        envelope = state.poll_task(worker_id="worker-1")
        assert envelope is not None
        assert envelope.task_id == "task-1"

        ok = state.report_result(
            pb2.ReportResultRequest(
                worker_id="worker-1",
                task_id="task-1",
                attempt=1,
                status=pb2.TASK_STATUS_SUCCEEDED,
                result={"value": 2},
            )
        )
        assert ok is True

        results, next_cursor = state.pull_results(
            pb2.PullResultsRequest(client_id="client-a", limit=10, wait_ms=0, cursor="")
        )
        assert len(results) == 1
        assert results[0].task_id == "task-1"
        assert results[0].job_id == "job-alpha"
        assert results[0].status == pb2.TASK_STATUS_SUCCEEDED
        assert int(next_cursor) >= 1
    finally:
        state.close()


def test_nested_arrow_payload_roundtrip():
    pd = pytest.importorskip("pandas")
    np = pytest.importorskip("numpy")

    payload = {
        "bundle": {
            "df": pd.DataFrame([{"x": 1}, {"x": 2}]),
            "series": pd.Series([10, 20], name="s"),
            "arr": np.array([3, 4, 5], dtype=np.int64),
        },
        "plain": [1, True, None],
    }

    restored = struct_to_dict(dict_to_struct(payload))

    assert list(restored["bundle"]["df"]["x"]) == [1, 2]
    assert restored["bundle"]["series"].name == "s"
    assert restored["bundle"]["series"].tolist() == [10, 20]
    assert restored["bundle"]["arr"].tolist() == [3, 4, 5]
    assert restored["plain"] == [1, True, None]


def test_dict_to_struct_rejects_unsupported_object_with_clear_path():
    class DemoObject:
        pass

    with pytest.raises(TypeError, match=r"payload\.bundle\.bad has unsupported type DemoObject"):
        dict_to_struct({"bundle": {"bad": DemoObject()}})


def test_dict_to_struct_rejects_complex_ndarray_dtype():
    np = pytest.importorskip("numpy")

    with pytest.raises(TypeError, match=r"payload\.arr uses numpy\.ndarray dtype object"):
        dict_to_struct({"arr": np.array([{"x": 1}], dtype=object)})


def test_infra_timeout_requeue_then_retry(tmp_path):
    state = NodeControlState(
        node_id="node-test-02",
        queue_capacity=16,
        worker_capacity=4,
        heartbeat_timeout_sec=1,
        max_retries=2,
        monitor_interval_sec=60,  # disable periodic checks in test
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        code_version = _seed_code(state)
        submit_req = pb2.SubmitTasksRequest(
            client_id="client-b",
            code_version=code_version,
            execution_mode=pb2.EXECUTION_MODE_EPHEMERAL,
            tasks=[pb2.TaskSubmitItem(task_id="task-retry", payload={"x": 3}, priority=1)],
        )
        accepted, rejected, _ = state.submit_tasks(submit_req)
        assert len(accepted) == 1
        assert not rejected

        first = state.poll_task(worker_id="worker-2")
        assert first is not None
        assert first.attempt == 1

        # 模拟心跳超时，触发基础设施重试。
        task = state._tasks["task-retry"]  # noqa: SLF001
        task.last_heartbeat_at = utc_now() - timedelta(seconds=5)
        state._handle_timeouts()  # noqa: SLF001

        second = state.poll_task(worker_id="worker-3")
        assert second is not None
        assert second.attempt == 2
    finally:
        state.close()


def test_internal_executor_runs_tasks_without_external_worker(tmp_path):
    state = NodeControlState(
        node_id="node-test-03",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=True,
        enable_service_session=False,
    )
    try:
        code_version = _seed_code(state)
        submit_req = pb2.SubmitTasksRequest(
            client_id="client-c",
            code_version=code_version,
            execution_mode=pb2.EXECUTION_MODE_PERSISTENT,
            tasks=[
                pb2.TaskSubmitItem(task_id="task-auto-1", payload={"value": 9, "sleep_ms": 20}, priority=1),
                pb2.TaskSubmitItem(task_id="task-auto-2", payload={"value": 5, "sleep_ms": 20, "should_fail": True}, priority=1),
            ],
        )
        accepted, rejected, _ = state.submit_tasks(submit_req)
        assert len(accepted) == 2
        assert not rejected

        deadline = time.time() + 10
        results = []
        cursor = ""
        while time.time() < deadline and len(results) < 2:
            batch, cursor = state.pull_results(
                pb2.PullResultsRequest(client_id="client-c", limit=10, wait_ms=200, cursor=cursor)
            )
            results.extend(batch)

        assert len(results) == 2
        statuses = sorted(item.status for item in results)
        assert statuses == [pb2.TASK_STATUS_SUCCEEDED, pb2.TASK_STATUS_FAILED_USER]
    finally:
        state.close()


def test_internal_executor_runtime_slots_queue_and_reclaim(tmp_path):
    state = NodeControlState(
        node_id="node-test-runtime-slot-01",
        queue_capacity=16,
        worker_capacity=1,
        runtime_slot_capacity=1,
        runtime_slot_idle_ttl_sec=1,
        artifact_dir=str(tmp_path / "code_cache_runtime_slot"),
        enable_internal_executor=True,
        enable_service_session=False,
        executor_poll_interval_sec=0.02,
    )
    try:
        blob = (
            b"import time\n"
            b"def run(value=0, sleep_ms=0, **_kwargs):\n"
            b"    sleep_ms = int(sleep_ms)\n"
            b"    if sleep_ms > 0:\n"
            b"        time.sleep(sleep_ms / 1000.0)\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        artifact, _ = state.put_code(
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="runtime_slot_demo",
            entry_callable="run",
            package_format="py",
            chunks=[blob],
        )
        code_version = artifact.code_version
        accepted, rejected, _ = state.submit_tasks(
            pb2.SubmitTasksRequest(
                client_id="client-slot",
                code_version=code_version,
                execution_mode=pb2.EXECUTION_MODE_PERSISTENT,
                tasks=[
                    pb2.TaskSubmitItem(task_id="slot-a-1", payload={"value": 2, "sleep_ms": 40}, priority=1, runtime_key="rt-a"),
                    pb2.TaskSubmitItem(task_id="slot-a-2", payload={"value": 3, "sleep_ms": 40}, priority=1, runtime_key="rt-a"),
                    pb2.TaskSubmitItem(task_id="slot-b-1", payload={"value": 4, "sleep_ms": 40}, priority=1, runtime_key="rt-b"),
                ],
            )
        )
        assert len(accepted) == 3
        assert not rejected

        time.sleep(0.1)
        with state._lock:  # noqa: SLF001
            assert "rt-a" in state._runtime_slots  # noqa: SLF001
            assert "rt-b" in state._runtime_slots  # noqa: SLF001
            assert state._runtime_slots["rt-a"].executor is not None  # noqa: SLF001
            assert state._runtime_slots["rt-b"].executor is None  # noqa: SLF001

        deadline = time.time() + 10
        cursor = ""
        results = []
        while time.time() < deadline and len(results) < 3:
            batch, cursor = state.pull_results(
                pb2.PullResultsRequest(client_id="client-slot", limit=10, wait_ms=200, cursor=cursor)
            )
            results.extend(batch)

        assert len(results) == 3
        assert {item.status for item in results} == {pb2.TASK_STATUS_SUCCEEDED}

        time.sleep(1.3)
        with state._lock:  # noqa: SLF001
            active_slots = [slot for slot in state._runtime_slots.values() if slot.executor is not None]  # noqa: SLF001
        assert active_slots == []
    finally:
        state.close()


def test_internal_executor_timeout_recycles_runtime_slot_and_allows_retry(tmp_path):
    state = NodeControlState(
        node_id="node-test-runtime-timeout-01",
        queue_capacity=16,
        worker_capacity=1,
        runtime_slot_capacity=1,
        heartbeat_timeout_sec=1,
        max_retries=2,
        monitor_interval_sec=60,
        artifact_dir=str(tmp_path / "code_cache_runtime_timeout"),
        enable_internal_executor=True,
        enable_service_session=False,
        executor_poll_interval_sec=0.02,
    )
    try:
        blob = (
            b"import time\n"
            b"def run(value=0, sleep_ms=0, **_kwargs):\n"
            b"    sleep_ms = int(sleep_ms)\n"
            b"    if sleep_ms > 0:\n"
            b"        time.sleep(sleep_ms / 1000.0)\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        artifact, _ = state.put_code(
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="runtime_timeout_demo",
            entry_callable="run",
            package_format="py",
            chunks=[blob],
        )
        accepted, rejected, _ = state.submit_tasks(
            pb2.SubmitTasksRequest(
                client_id="client-timeout",
                code_version=artifact.code_version,
                execution_mode=pb2.EXECUTION_MODE_PERSISTENT,
                tasks=[
                    pb2.TaskSubmitItem(
                        task_id="timeout-task-1",
                        payload={"value": 6, "sleep_ms": 5000},
                        priority=1,
                        runtime_key="rt-timeout",
                    ),
                ],
            )
        )
        assert len(accepted) == 1
        assert not rejected

        deadline = time.time() + 10
        while time.time() < deadline:
            with state._lock:  # noqa: SLF001
                task = state._tasks["timeout-task-1"]  # noqa: SLF001
                slot = state._runtime_slots["rt-timeout"]  # noqa: SLF001
                if task.status == pb2.TASK_STATUS_RUNNING and slot.current_task_id == "timeout-task-1":
                    break
            time.sleep(0.05)
        else:
            raise AssertionError("task never entered running state")

        with state._lock:  # noqa: SLF001
            task = state._tasks["timeout-task-1"]  # noqa: SLF001
            task.payload["sleep_ms"] = 0
            task.last_heartbeat_at = utc_now() - timedelta(seconds=5)

        state._handle_timeouts()  # noqa: SLF001

        deadline = time.time() + 10
        results = []
        cursor = ""
        while time.time() < deadline and not results:
            batch, cursor = state.pull_results(
                pb2.PullResultsRequest(client_id="client-timeout", limit=10, wait_ms=200, cursor=cursor)
            )
            results.extend(batch)

        assert len(results) == 1
        assert results[0].status == pb2.TASK_STATUS_SUCCEEDED
        assert results[0].attempt == 2
        assert dict(results[0].result) == {"value": 6, "square": 36}
        with state._lock:  # noqa: SLF001
            slot = state._runtime_slots["rt-timeout"]  # noqa: SLF001
            assert slot.current_task_id == ""
    finally:
        state.close()


def test_cancel_job_marks_matching_tasks(tmp_path):
    state = NodeControlState(
        node_id="node-test-cancel-job",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        code_version = _seed_code(state)
        accepted, rejected, _ = state.submit_tasks(
            pb2.SubmitTasksRequest(
                client_id="client-job",
                code_version=code_version,
                execution_mode=pb2.EXECUTION_MODE_PERSISTENT,
                job_id="job-42",
                tasks=[
                    pb2.TaskSubmitItem(task_id="task-running", payload={"value": 2}, priority=1),
                    pb2.TaskSubmitItem(task_id="task-queued", payload={"value": 3}, priority=1),
                ],
            )
        )
        assert len(accepted) == 2
        assert not rejected

        envelope = state.poll_task(worker_id="worker-job")
        assert envelope is not None
        assert envelope.task_id == "task-running"

        queued_cancelled, running_marked, already_done, not_found = state.cancel_job(
            pb2.CancelJobRequest(
                client_id="client-job",
                job_id="job-42",
                reason="debug stop",
            )
        )
        assert queued_cancelled == 1
        assert running_marked == 1
        assert already_done == 0
        assert not_found == 0

        assert state._tasks["task-running"].cancel_requested is True  # noqa: SLF001
        assert state._tasks["task-queued"].status == pb2.TASK_STATUS_CANCELLED  # noqa: SLF001

        results, _ = state.pull_results(
            pb2.PullResultsRequest(client_id="client-job", limit=10, wait_ms=0, cursor="")
        )
        assert len(results) == 1
        assert results[0].task_id == "task-queued"
        assert results[0].job_id == "job-42"
        assert results[0].status == pb2.TASK_STATUS_CANCELLED
    finally:
        state.close()


def test_cancel_job_returns_not_found_for_unknown_job(tmp_path):
    state = NodeControlState(
        node_id="node-test-cancel-job-miss",
        queue_capacity=8,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        queued_cancelled, running_marked, already_done, not_found = state.cancel_job(
            pb2.CancelJobRequest(client_id="client-miss", job_id="job-missing", reason="noop")
        )
        assert queued_cancelled == 0
        assert running_marked == 0
        assert already_done == 0
        assert not_found == 1
    finally:
        state.close()


def test_service_session_http_call_and_end(tmp_path):
    state = NodeControlState(
        node_id="node-svc-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    try:
        blob = (
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(value=0, **_kwargs):\n"
            b"    v = int(value)\n"
            b"    return {'v': v, 'square': v * v}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        session = state.create_service(
            owner_client_id="owner-a",
            service_name="svc-a",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_entry",
            entry_callable="run",
            package_format="py",
            worker_count=2,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=True,
            chunks=[blob],
        )
        assert session.status == pb2.SERVICE_STATUS_RUNNING
        assert session.http_base_url.startswith("http://")

        req = Request(
            url=f"{session.http_base_url}/call/run",
            method="POST",
            data=json.dumps({"value": 8}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urlopen(req, timeout=5) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        assert body["ok"] is True
        assert body["data"]["square"] == 64

        hb = state.heartbeat_service(
            owner_client_id="owner-a",
            service_id=session.service_id,
            service_token=session.service_token,
        )
        assert hb.status == pb2.SERVICE_STATUS_RUNNING

        ended = state.end_service(
            owner_client_id="owner-a",
            service_id=session.service_id,
            service_token=session.service_token,
            reason="done",
        )
        assert ended.status == pb2.SERVICE_STATUS_STOPPED
    finally:
        state.close()


def test_service_session_management_requires_token(tmp_path):
    state = NodeControlState(
        node_id="node-svc-auth-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    try:
        blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
        digest = hashlib.sha256(blob).hexdigest()
        session = state.create_service(
            owner_client_id="owner-auth",
            service_name="svc-auth",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_auth",
            entry_callable="run",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=False,
            chunks=[blob],
        )

        try:
            state.heartbeat_service(
                owner_client_id="owner-auth",
                service_id=session.service_id,
                service_token="",
            )
            assert False, "expected missing token to be rejected"
        except PermissionError as exc:
            assert "service_token mismatch" in str(exc)

        try:
            state.end_service(
                owner_client_id="owner-auth",
                service_id=session.service_id,
                service_token="bad-token",
                reason="should fail",
            )
            assert False, "expected bad token to be rejected"
        except PermissionError as exc:
            assert "service_token mismatch" in str(exc)
    finally:
        state.close()


def test_service_session_heartbeat_timeout_recycles(tmp_path):
    state = NodeControlState(
        node_id="node-svc-02",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
        monitor_interval_sec=60,
    )
    try:
        blob = (
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(**_kwargs):\n"
            b"    return {'ok': True}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        session = state.create_service(
            owner_client_id="owner-b",
            service_name="svc-b",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_entry",
            entry_callable="run",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=1,
            idle_ttl_sec=0,
            expose_http=True,
            chunks=[blob],
        )
        assert session.status == pb2.SERVICE_STATUS_RUNNING
        session.last_heartbeat_at = utc_now() - timedelta(seconds=5)
        session.lease_expire_at = utc_now() - timedelta(seconds=1)
        state._handle_service_timeouts()  # noqa: SLF001
        info = state.service_status_info(session.service_id)
        assert info["status"] == pb2.SERVICE_STATUS_STOPPED
    finally:
        state.close()


def test_service_call_recovers_after_executor_host_restart(tmp_path):
    state = NodeControlState(
        node_id="node-svc-host-restart-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
        monitor_interval_sec=1,
    )
    try:
        blob = (
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(value=0, **_kwargs):\n"
            b"    v = int(value)\n"
            b"    return {'v': v, 'square': v * v}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        session = state.create_service(
            owner_client_id="owner-host-restart",
            service_name="svc-host-restart",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_host_restart",
            entry_callable="run",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=False,
            chunks=[blob],
        )

        assert state._executor_host is not None  # noqa: SLF001
        state._executor_host._process.terminate()  # noqa: SLF001
        state._executor_host._process.join(timeout=5.0)  # noqa: SLF001

        code, body = state.call_service(
            service_id=session.service_id,
            method="run",
            payload={"value": 8},
            service_token=session.service_token,
            timeout_sec=5.0,
        )
        assert code == 200
        assert body["ok"] is True
        assert body["data"] == {"v": 8, "square": 64}
    finally:
        state.close()


def test_service_create_does_not_keep_package_module_in_parent(tmp_path):
    state = NodeControlState(
        node_id="node-svc-03",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    try:
        blob = (
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(**_kwargs):\n"
            b"    return {'ok': True}\n"
        )

        import io
        import zipfile

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("compute_service/__init__.py", "")
            zf.writestr("compute_service/main.py", blob)
        archive = buf.getvalue()
        digest = hashlib.sha256(archive).hexdigest()

        session = state.create_service(
            owner_client_id="owner-c",
            service_name="svc-c",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="compute_service.main",
            entry_callable="run",
            package_format="zip",
            export_mode="decorator",
            export_methods=(),
            export_decorator="pycloud_export",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=True,
            chunks=[archive],
        )

        assert session.status == pb2.SERVICE_STATUS_RUNNING
        assert "compute_service" not in sys.modules
        assert "compute_service.main" not in sys.modules
    finally:
        state.close()


def test_internal_executor_recovers_after_executor_host_restart(tmp_path):
    state = NodeControlState(
        node_id="node-runtime-host-restart-01",
        queue_capacity=16,
        worker_capacity=1,
        runtime_slot_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_runtime_host_restart"),
        heartbeat_timeout_sec=30,
        max_retries=2,
        enable_internal_executor=True,
        enable_service_session=False,
        executor_poll_interval_sec=0.02,
        monitor_interval_sec=60,
    )
    try:
        blob = (
            b"import time\n"
            b"def run(value=0, sleep_ms=0, **_kwargs):\n"
            b"    sleep_ms = int(sleep_ms)\n"
            b"    if sleep_ms > 0:\n"
            b"        time.sleep(sleep_ms / 1000.0)\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        artifact, _ = state.put_code(
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="runtime_host_restart_demo",
            entry_callable="run",
            package_format="py",
            chunks=[blob],
        )
        accepted, rejected, _ = state.submit_tasks(
            pb2.SubmitTasksRequest(
                client_id="client-host-restart",
                code_version=artifact.code_version,
                execution_mode=pb2.EXECUTION_MODE_PERSISTENT,
                tasks=[
                    pb2.TaskSubmitItem(
                        task_id="restart-task-1",
                        payload={"value": 7, "sleep_ms": 5000},
                        priority=1,
                        runtime_key="rt-host-restart",
                    ),
                ],
            )
        )
        assert len(accepted) == 1
        assert not rejected

        deadline = time.time() + 10
        while time.time() < deadline:
            with state._lock:  # noqa: SLF001
                task = state._tasks["restart-task-1"]  # noqa: SLF001
                slot = state._runtime_slots["rt-host-restart"]  # noqa: SLF001
                if task.status == pb2.TASK_STATUS_RUNNING and slot.current_task_id == "restart-task-1":
                    task.payload["sleep_ms"] = 0
                    break
            time.sleep(0.05)
        else:
            raise AssertionError("task never entered running state")

        assert state._executor_host is not None  # noqa: SLF001
        state._executor_host._process.terminate()  # noqa: SLF001
        state._executor_host._process.join(timeout=5.0)  # noqa: SLF001

        deadline = time.time() + 15
        results = []
        cursor = ""
        while time.time() < deadline and not results:
            batch, cursor = state.pull_results(
                pb2.PullResultsRequest(client_id="client-host-restart", limit=10, wait_ms=200, cursor=cursor)
            )
            results.extend(batch)

        assert len(results) == 1
        assert results[0].status == pb2.TASK_STATUS_SUCCEEDED
        assert results[0].attempt == 2
        assert dict(results[0].result) == {"value": 7, "square": 49}
    finally:
        state.close()
