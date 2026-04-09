"""中文说明：验证 gRPC 控制面的核心状态流转（内存后端）。"""

import hashlib
import io
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen

import pytest

from pycloud_parallel.controlplane.client import ServiceGroup, ServiceSessionClient
from pycloud_parallel.controlplane.state import (
    NodeControlState,
    _build_execute_spec,
    _execute_payload_in_subprocess,
    dict_to_struct,
    struct_to_dict,
    utc_now,
)
from pycloud_parallel.controlplane.state import InfoCenterState, NodeServiceState
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

        results2, next_cursor2 = state.pull_results(
            pb2.PullResultsRequest(client_id="client-a", limit=10, wait_ms=0, cursor=next_cursor)
        )
        assert results2 == []
        assert next_cursor2 == "0"
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


def test_dataframe_round_trips_int_columns_and_multiindex():
    pd = pytest.importorskip("pandas")

    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-02"), "stock"),
            (pd.Timestamp("2024-01-03"), "bond"),
        ],
        names=["trade_date", "asset_type"],
    )
    columns = pd.Index([10006, 10007], name="fund_id")
    frame = pd.DataFrame([[0.1, 0.2], [0.3, 0.4]], index=index, columns=columns)

    restored = struct_to_dict(dict_to_struct({"frame": frame}))

    pd.testing.assert_frame_equal(restored["frame"], frame)


def test_dict_to_struct_rejects_unsupported_object_with_clear_path():
    class DemoObject:
        pass

    with pytest.raises(TypeError, match=r"payload\.bundle\.bad has unsupported type DemoObject"):
        dict_to_struct({"bundle": {"bad": DemoObject()}})


def test_dict_to_struct_rejects_complex_ndarray_dtype():
    np = pytest.importorskip("numpy")

    with pytest.raises(TypeError, match=r"payload\.arr uses numpy\.ndarray dtype object"):
        dict_to_struct({"arr": np.array([{"x": 1}], dtype=object)})


def test_dict_to_struct_stringifies_scalar_dict_keys():
    restored = struct_to_dict(
        dict_to_struct(
            {
                "payload": [
                    {
                        10006: {"value": 1},
                        True: "flag",
                    }
                ]
            }
        )
    )

    assert restored["payload"][0]["10006"]["value"] == 1
    assert restored["payload"][0]["True"] == "flag"


def test_dict_to_struct_rejects_colliding_normalized_dict_keys():
    with pytest.raises(TypeError, match=r"normalize to '1'"):
        dict_to_struct({"payload": {1: "int-key", "1": "string-key"}})


def test_dict_to_struct_round_trips_temporal_scalars_and_series_index():
    pd = pytest.importorskip("pandas")

    ts = pd.Timestamp("2024-01-02T03:04:05+08:00")
    payload = {
        "when": datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        "series": pd.Series([10, 20], index=[ts, ts + pd.Timedelta(days=1)], name="nav"),
    }

    restored = struct_to_dict(dict_to_struct(payload))

    assert restored["when"] == payload["when"]
    assert restored["series"].name == "nav"
    assert list(restored["series"]) == [10, 20]
    assert restored["series"].index[0] == ts
    assert restored["series"].index[1] == ts + pd.Timedelta(days=1)


def test_dataframe_object_upload_parquet_preserves_index_and_int_columns():
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    from pycloud_parallel.controlplane.client import _serialize_data_for_object_ref
    from pycloud_parallel.controlplane.client import _materialize_downloaded_result
    from pycloud_parallel.controlplane.result_ref import ResultRef

    index = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2024-01-02"), "a"), (pd.Timestamp("2024-01-03"), "b")],
        names=["trade_date", "bucket"],
    )
    frame = pd.DataFrame([[1, 2], [3, 4]], index=index, columns=[10006, 10007])

    kind, fmt, blob = _serialize_data_for_object_ref(frame, format="parquet")
    import tempfile
    from pathlib import Path

    fd, tmp_name = tempfile.mkstemp(prefix="pycloud-dfbundle-", suffix=".dfbundle")
    import os
    os.close(fd)
    Path(tmp_name).write_bytes(blob)
    try:
        import io
        import zipfile

        with zipfile.ZipFile(Path(tmp_name)) as zf:
            with zf.open("data.parquet") as fh:
                stored_frame = pd.read_parquet(io.BytesIO(fh.read()))
        assert list(stored_frame.columns) == ["c0", "c1"]

        restored = _materialize_downloaded_result(
            Path(tmp_name),
            result_ref=ResultRef(
                object_id="sha256:" + "b" * 64,
                node_id="node-1",
                format=fmt,
                size_bytes=len(blob),
                materialize_as="dataframe",
            ),
        )

        assert kind == "dataframe"
        assert fmt == "dfbundle"
        pd.testing.assert_frame_equal(restored, frame)
    finally:
        Path(tmp_name).unlink(missing_ok=True)


def test_series_object_upload_preserves_index_and_name():
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    from pycloud_parallel.controlplane.client import _materialize_downloaded_result
    from pycloud_parallel.controlplane.client import _serialize_data_for_object_ref
    from pycloud_parallel.controlplane.result_ref import ResultRef

    series = pd.Series(
        [1.1, 2.2],
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-01-02"), "a"), (pd.Timestamp("2024-01-03"), "b")],
            names=["trade_date", "bucket"],
        ),
        name=10006,
    )

    kind, fmt, blob = _serialize_data_for_object_ref(series)

    import os
    import tempfile
    from pathlib import Path

    fd, tmp_name = tempfile.mkstemp(prefix="pycloud-seriesbundle-", suffix=".seriesbundle")
    os.close(fd)
    Path(tmp_name).write_bytes(blob)
    try:
        restored = _materialize_downloaded_result(
            Path(tmp_name),
            result_ref=ResultRef(
                object_id="sha256:" + "c" * 64,
                node_id="node-1",
                format=fmt,
                size_bytes=len(blob),
                materialize_as="series",
            ),
        )

        assert kind == "series"
        assert fmt == "seriesbundle"
        pd.testing.assert_series_equal(restored, series)
    finally:
        Path(tmp_name).unlink(missing_ok=True)


def test_object_ref_resolution_restores_dataframe_bundle_on_node(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    from pycloud_parallel.controlplane.client import _serialize_data_for_object_ref
    from pycloud_parallel.controlplane.object_ref import ObjectRef
    from pycloud_parallel.controlplane.object_ref import object_storage_path
    from pycloud_parallel.controlplane.state import _resolve_object_refs_in_payload

    frame = pd.DataFrame(
        [[1, 2], [3, 4]],
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-01-02"), "a"), (pd.Timestamp("2024-01-03"), "b")],
            names=["trade_date", "bucket"],
        ),
        columns=[10006, 10007],
    )
    _kind, fmt, blob = _serialize_data_for_object_ref(frame)
    object_id = "sha256:" + "d" * 64
    path = object_storage_path(tmp_path, object_id=object_id, fmt=fmt)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(blob)

    payload = {
        "frame": ObjectRef(
            object_id=object_id,
            format=fmt,
            size_bytes=len(blob),
            materialize_as="dataframe",
        )
    }

    restored = _resolve_object_refs_in_payload(payload, object_dir=str(tmp_path))
    pd.testing.assert_frame_equal(restored["frame"], frame)


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
            running = [task.task_id for task in state._tasks.values() if task.status == pb2.TASK_STATUS_RUNNING]  # noqa: SLF001
            queued = [task.task_id for task in state._tasks.values() if task.status == pb2.TASK_STATUS_QUEUED]  # noqa: SLF001
            assert len(running) == 1
            assert len(queued) == 2

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
            assert state._inflight_count_locked() == 0  # noqa: SLF001
            assert state._queued_count_locked() == 0  # noqa: SLF001
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
                if task.status == pb2.TASK_STATUS_RUNNING:
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
            task = state._tasks["timeout-task-1"]  # noqa: SLF001
            assert task.status == pb2.TASK_STATUS_SUCCEEDED
            assert state._inflight_count_locked() == 0  # noqa: SLF001
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
        info = state.service_status_info(session.service_id)
        timing = dict(info.get("timing_metrics") or {})
        assert int(timing.get("call_count", 0) or 0) >= 1
        assert float(timing.get("last_total_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_build_execute_spec_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_child_decode_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_child_invoke_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_child_encode_ms", 0.0) or 0.0) >= 0.0
        assert float(timing.get("last_invoke_ms", 0.0) or 0.0) == float(timing.get("last_child_invoke_ms", 0.0) or 0.0)

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


def test_service_sessions_with_same_blob_and_different_managed_globals_can_coexist(tmp_path):
    state = NodeControlState(
        node_id="node-svc-managed-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_managed"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    try:
        blob = (
            b"A = 1\n"
            b"B = 2\n"
            b"def pycloud_export(fn):\n"
            b"    fn.__pycloud_export__ = True\n"
            b"    return fn\n\n"
            b"@pycloud_export\n"
            b"def run(**_kwargs):\n"
            b"    return {'A': A, 'B': B}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        session_a = state.create_service(
            owner_client_id="owner-a",
            service_name="svc-a",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_managed",
            entry_callable="run",
            package_format="py",
            managed_global_names=["A"],
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=False,
            chunks=[blob],
        )
        session_b = state.create_service(
            owner_client_id="owner-b",
            service_name="svc-b",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_managed",
            entry_callable="run",
            package_format="py",
            managed_global_names=["B"],
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=False,
            chunks=[blob],
        )

        digest_a, updated_a = state.update_service_globals(
            owner_client_id="owner-a",
            service_id=session_a.service_id,
            service_token=session_a.service_token,
            values={"A": 10},
        )
        digest_b, updated_b = state.update_service_globals(
            owner_client_id="owner-b",
            service_id=session_b.service_id,
            service_token=session_b.service_token,
            values={"B": 20},
        )

        assert updated_a == ["A"]
        assert updated_b == ["B"]
        assert digest_a
        assert digest_b
        assert session_a.managed_global_names == ("A",)
        assert session_b.managed_global_names == ("B",)
    finally:
        state.close()


def test_task_pool_keeps_instance_managed_global_names(tmp_path):
    state = NodeControlState(
        node_id="node-pool-managed-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_pool_managed"),
        enable_internal_executor=True,
        enable_service_session=False,
    )
    try:
        blob = b"STATE = 1\ndef run(**_kwargs):\n    return {'ok': True}\n"
        digest = hashlib.sha256(blob).hexdigest()
        pool = state.create_task_pool(
            owner_client_id="owner-pool",
            pool_name="pool-managed",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="pool_managed",
            entry_callable="run",
            package_format="py",
            managed_global_names=["STATE"],
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )
        assert pool.managed_global_names == ("STATE",)
    finally:
        state.close()


def test_same_blob_with_different_export_specs_can_coexist(tmp_path):
    state = NodeControlState(
        node_id="node-code-cache-variants-01",
        queue_capacity=16,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_variants"),
        enable_internal_executor=False,
        enable_service_session=False,
    )
    try:
        blob = (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': int(value)}\n\n"
            b"def alt(value=0, **_kwargs):\n"
            b"    return {'value': int(value) + 1}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        explicit_artifact, explicit_cached = state.put_code(
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="cache_variant_demo",
            entry_callable="run",
            package_format="py",
            export_mode="explicit",
            export_methods=["alt"],
            chunks=[blob],
        )
        single_artifact, single_cached = state.put_code(
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="cache_variant_demo",
            entry_callable="run",
            package_format="py",
            export_mode="single",
            export_methods=["run"],
            chunks=[blob],
        )

        assert explicit_cached is False
        assert single_cached is False
        assert explicit_artifact.code_version != single_artifact.code_version
        assert explicit_artifact.export_mode == "explicit"
        assert explicit_artifact.export_methods == ("alt",)
        assert single_artifact.export_mode == "single"
        assert single_artifact.export_methods == ("run",)

        failed_status, _failed_result, failed_type, failed_message, _ = _execute_payload_in_subprocess(
            **_build_execute_spec(
                explicit_artifact,
                object_dir=tmp_path / "objects",
                method_name="run",
                payload={"value": 5},
            )
        )
        assert failed_status == "FAILED_USER"
        assert failed_type == "RuntimeError"
        assert "method `run` not exported" in failed_message

        ok_status, ok_result, ok_type, ok_message, _ = _execute_payload_in_subprocess(
            **_build_execute_spec(
                single_artifact,
                object_dir=tmp_path / "objects",
                method_name="run",
                payload={"value": 5},
            )
        )
        assert ok_status == "SUCCEEDED"
        assert ok_result == {"value": 5}
        assert ok_type == ""
        assert ok_message == ""
    finally:
        state.close()


def test_infocenter_stale_node_degrades_service_route_status():
    state = InfoCenterState(lease_ttl_sec=1, heartbeat_interval_sec=1)
    state.register_node_record(
        node_id="node-stale",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=16,
        services={
            "svc-1": NodeServiceState(
                service_name="svc-stale",
                service_id="svc-1",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=2,
                alive_workers=2,
                in_flight=1,
            )
        },
    )
    state._nodes["node-stale"].last_seen_at = utc_now() - timedelta(seconds=5)  # noqa: SLF001

    routes = state.list_service_routes(service_name="svc-stale", healthy_only=False, limit=10)
    assert len(routes) == 1
    assert routes[0]["node_healthy"] is False
    assert routes[0]["stale"] is True
    assert routes[0]["status"] == pb2.SERVICE_STATUS_UNSPECIFIED
    assert routes[0]["status_text"] == "LOST"
    assert routes[0]["alive_workers"] == 0
    assert routes[0]["in_flight"] == 0
    assert state.list_service_routes(service_name="svc-stale", healthy_only=True, limit=10) == []


def test_service_session_keepalive_fails_fast_after_consecutive_errors():
    class _FailingClient:
        def heartbeat_service(self, **_kwargs):
            raise RuntimeError("heartbeat unavailable")

    session = ServiceSessionClient(
        _client=_FailingClient(),
        owner_client_id="owner-x",
        service_id="svc-x",
        service_token="token-x",
        http_base_url="",
        heartbeat_timeout_sec=1,
        worker_count=1,
        status=pb2.SERVICE_STATUS_RUNNING,
        heartbeat_failure_threshold=2,
    )
    group = ServiceGroup(
        owner_client_id="owner-x",
        service_name="svc-x",
        sessions={"node-1": session},
        nodes={},
    )

    group._start_keepalive(interval_sec=0.05)
    start = time.monotonic()
    group.join(poll_interval_sec=0.05)
    elapsed = time.monotonic() - start

    assert elapsed < 2.0
    assert session.failed is True
    assert session.status == pb2.SERVICE_STATUS_STOPPED
    assert "heartbeat unavailable" in session.last_error
    assert "node-1" in group.failures


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
                if task.status == pb2.TASK_STATUS_RUNNING:
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
