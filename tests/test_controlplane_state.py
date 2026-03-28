"""中文说明：验证 gRPC 控制面的核心状态流转（内存后端）。"""

import hashlib
from datetime import timedelta

from pycloud_parallel.controlplane.state import NodeControlState, utc_now
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def _seed_code(state: NodeControlState) -> str:
    blob = b"print('hello grpc')\n"
    digest = hashlib.sha256(blob).hexdigest()
    artifact, _ = state.put_code(
        sha256=f"sha256:{digest}",
        filename="demo.py",
        chunks=[blob],
    )
    return artifact.code_version


def test_submit_poll_report_and_pull_results(tmp_path):
    state = NodeControlState(
        node_id="node-test-01",
        queue_capacity=16,
        worker_capacity=4,
        artifact_dir=str(tmp_path / "code_cache"),
    )
    try:
        code_version = _seed_code(state)
        submit_req = pb2.SubmitTasksRequest(
            client_id="client-a",
            code_version=code_version,
            execution_mode=pb2.EXECUTION_MODE_PERSISTENT,
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
        assert results[0].status == pb2.TASK_STATUS_SUCCEEDED
        assert int(next_cursor) >= 1
    finally:
        state.close()


def test_infra_timeout_requeue_then_retry(tmp_path):
    state = NodeControlState(
        node_id="node-test-02",
        queue_capacity=16,
        worker_capacity=4,
        heartbeat_timeout_sec=1,
        max_retries=2,
        monitor_interval_sec=60,  # disable periodic checks in test
        artifact_dir=str(tmp_path / "code_cache"),
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
