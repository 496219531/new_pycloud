from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.controlplane.serialization import dict_to_struct


def test_native_task_pool_session_submit_and_wait() -> None:
    from pycloud_parallel.controlplane.client import TaskPoolSession

    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(
            ok=True,
            accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
            rejected=[],
        ),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(
            ok=True,
            results=[
                pb2.TaskResult(
                    task_id="pool-task-0001",
                    job_id="job-native",
                    status=pb2.TASK_STATUS_SUCCEEDED,
                    result=dict_to_struct({"value": 1}),
                )
            ],
            next_cursor="",
        ),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )

    with patch("pycloud_parallel.controlplane.client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.client.NodeControlClient.create_task_pool_from_bytes",
        return_value=fake_pool_client,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPoolSession.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native",
            blob=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
            entry_module="task_demo",
            entry_callable="run",
            worker_count=2,
            node_count=1,
        )

    try:
        resp = session.submit_payloads([{"value": 1}])
        assert len(resp.accepted) == 1
        assert session.job_id == "job-native"
        assert session.node_ids == ["node-1"]
        assert session.methods == ["run"]
    finally:
        session.close()


def test_native_task_pool_session_cancel_job_aggregates_pool_responses() -> None:
    from pycloud_parallel.controlplane.client import TaskPoolSession

    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True, queued_cancelled=1, running_marked=2, already_done=3, not_found=0),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    with patch("pycloud_parallel.controlplane.client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.client.NodeControlClient.create_task_pool_from_bytes",
        return_value=fake_pool_client,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPoolSession.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-cancel",
            blob=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
            entry_module="task_demo",
            entry_callable="run",
            worker_count=2,
            node_count=1,
        )
    try:
        resp = session.cancel_job(job_id="job-native-cancel", reason="test cancel")
        assert resp.queued_cancelled == 1
        assert resp.running_marked == 2
        assert resp.already_done == 3
    finally:
        session.close()


def test_native_task_pool_session_status_map() -> None:
    from pycloud_parallel.controlplane.client import TaskPoolSession

    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_status = SimpleNamespace(status="RUNNING", worker_count=2, task_count=0)
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True),
        get_status=lambda: fake_status,
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    with patch("pycloud_parallel.controlplane.client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.client.NodeControlClient.create_task_pool_from_bytes",
        return_value=fake_pool_client,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPoolSession.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-status",
            blob=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
            entry_module="task_demo",
            entry_callable="run",
            worker_count=2,
            node_count=1,
        )
    try:
        status_map = session.status_map()
        assert status_map["node-1"].status == "RUNNING"
    finally:
        session.close()


def test_native_task_pool_session_submit_values_delegates() -> None:
    from pycloud_parallel.controlplane.client import TaskPoolSession

    session = TaskPoolSession(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-values",
    )
    captured = {}

    def _fake_submit(payloads, **kwargs):
        captured["payloads"] = payloads
        captured["kwargs"] = kwargs
        return pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[])

    session.submit_payloads = _fake_submit  # type: ignore[method-assign]
    session.submit_values([1, 2, 3], arg_name="x", extra=9)
    assert captured["payloads"] == [{"x": 1, "extra": 9}, {"x": 2, "extra": 9}, {"x": 3, "extra": 9}]
