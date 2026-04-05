from __future__ import annotations

import base64
from unittest.mock import patch

from pycloud_parallel.controlplane.job_queue import JobQueueManager


def test_submit_and_cancel_waiting_job() -> None:
    queue = JobQueueManager()
    job = queue.submit_job(
        {
            "job_id": "job-waiting-1",
            "client_id": "client-a",
            "priority": 2,
            "code_version": "sha256:test",
            "entry_module": "task_demo",
            "subtasks": [{"value": 1}],
        }
    )
    assert job.status == "WAITING"

    cancelled = queue.cancel_job("job-waiting-1")
    assert cancelled is not None
    assert cancelled.status == "CANCELLED"
    assert cancelled.cancel_requested is True


def test_pick_next_job_prefers_priority_then_submission_order() -> None:
    queue = JobQueueManager()
    low = queue.submit_job(
        {
            "job_id": "job-low",
            "client_id": "client-a",
            "priority": 1,
            "code_version": "sha256:test",
            "entry_module": "task_demo",
            "subtasks": [{"value": 1}],
        }
    )
    high = queue.submit_job(
        {
            "job_id": "job-high",
            "client_id": "client-b",
            "priority": 9,
            "code_version": "sha256:test",
            "entry_module": "task_demo",
            "subtasks": [{"value": 2}],
        }
    )
    with queue._lock:  # noqa: SLF001
        selected = queue._pick_next_job_locked()  # noqa: SLF001
    assert selected is not None
    assert selected.job_id == high.job_id
    assert selected.job_id != low.job_id


def test_expand_subtasks_from_driver_blob() -> None:
    queue = JobQueueManager()
    blob = (
        b"def build(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n"
    )
    subtasks = queue._expand_subtasks(  # noqa: SLF001
        {
            "driver_blob_b64": base64.b64encode(blob).decode("utf-8"),
            "driver_entry_module": "job_driver_demo",
            "driver_entry_callable": "build",
            "driver_payload": {"value": 10, "count": 3},
            "driver_package_format": "py",
        }
    )
    assert subtasks == [{"value": 10}, {"value": 11}, {"value": 12}]


def test_run_job_prefers_task_pool_session() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-pool-1",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(b"def run(value=0, **_kwargs):\n    return {'value': value}\n").decode("utf-8"),
            "entry_module": "task_demo",
            "entry_callable": "run",
            "subtasks": [{"value": 1}, {"value": 2}],
            "use_task_pool": True,
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-pool-1"
            self.submitted = []

        def submit_payloads(self, payloads, **kwargs):
            self.submitted.append((list(payloads), dict(kwargs)))
            from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[
                    pb2.TaskAccepted(task_id="t-1", status=pb2.TASK_STATUS_QUEUED),
                    pb2.TaskAccepted(task_id="t-2", status=pb2.TASK_STATUS_QUEUED),
                ],
            )

        def wait_for_results(self, **kwargs):
            from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
            from pycloud_parallel.controlplane.serialization import dict_to_struct

            return [
                pb2.TaskResult(task_id="t-1", job_id="job-pool-1", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1})),
                pb2.TaskResult(task_id="t-2", job_id="job-pool-1", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 2})),
            ]

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue.TaskPoolSession.from_infocenter", return_value=fake_pool) as mocked:
        queue._run_job("job-pool-1")  # noqa: SLF001

    mocked.assert_called_once()
    job = queue.get_job("job-pool-1")
    assert job is not None
    assert job.status == "SUCCEEDED"
    assert len(job.results) == 2


def test_job_queue_client_submit_job_from_bytes_includes_pool_policy() -> None:
    from pycloud_parallel.controlplane.client import JobQueueClient

    client = JobQueueClient("127.0.0.1:50051")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]
    resp = client.submit_job_from_bytes(
        blob=b"def build(**_kwargs):\n    return []\n",
        driver_entry_module="driver_demo",
        client_id="client-a",
        pool_name="pool-a",
        pool_worker_count=3,
        pool_node_count=2,
        pool_heartbeat_timeout_sec=20,
    )
    assert resp == {"ok": True}
    assert captured["pool_name"] == "pool-a"
    assert captured["pool_worker_count"] == 3
    assert captured["pool_node_count"] == 2
    assert captured["pool_heartbeat_timeout_sec"] == 20


def test_job_queue_client_submit_job_from_func_builds_payloads() -> None:
    from pycloud_parallel.controlplane.client import JobQueueClient

    client = JobQueueClient("127.0.0.1:50051")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]

    def driver(value=0, **_kwargs):
        return [{"value": value}]

    resp = client.submit_job_from_func(
        func=driver,
        client_id="client-func",
        pool_worker_count=2,
    )
    assert resp == {"ok": True}
    assert captured["driver_entry_module"]
    assert captured["driver_entry_callable"] == "driver"
    assert captured["entry_module"]
    assert captured["entry_callable"] == "run"
    assert captured["pool_worker_count"] == 2
    assert captured["blob_b64"]
    assert captured["driver_blob_b64"]


def test_job_queue_client_wait_for_terminal_polls_until_done() -> None:
    from pycloud_parallel.controlplane.client import JobQueueClient

    client = JobQueueClient("127.0.0.1:50051")
    states = [
        {"ok": True, "job": {"job_id": "job-1", "status": "WAITING"}},
        {"ok": True, "job": {"job_id": "job-1", "status": "RUNNING"}},
        {"ok": True, "job": {"job_id": "job-1", "status": "SUCCEEDED"}},
    ]

    def _fake_status(job_id):
        assert job_id == "job-1"
        return states.pop(0)

    client.get_job_status = _fake_status  # type: ignore[method-assign]
    result = client.wait_for_terminal("job-1", timeout_sec=2.0, poll_interval_sec=0.01)
    assert result["job"]["status"] == "SUCCEEDED"
