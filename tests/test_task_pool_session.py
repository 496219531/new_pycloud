from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import patch

from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def _fake_group():
    calls = []
    update_calls = []

    def call_balanced(method, payload, *, timeout_sec=60.0, **_kwargs):
        calls.append((method, dict(payload), float(timeout_sec)))
        return "node-1", {"data": {"echo": payload}}

    def update_globals(values):
        update_calls.append(dict(values))
        return "sha256:group-digest"

    group = SimpleNamespace(
        owner_client_id="owner-demo",
        service_name="task-pool-demo",
        sessions={"node-1": SimpleNamespace(worker_count=2)},
        _artifact_code_version="sha256:test",
        call_balanced=call_balanced,
        update_globals=update_globals,
        close=lambda end_services=True, reason="": None,
    )
    return group, calls, update_calls


def test_task_pool_session_submit_and_wait() -> None:
    from pycloud_parallel.controlplane.client import DedicatedTaskServiceSession

    group, calls, _update_calls = _fake_group()
    with patch(
        "pycloud_parallel.controlplane.client.ServiceGroup.deploy_from_infocenter",
        return_value=group,
    ):
        session = DedicatedTaskServiceSession.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            blob=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
            entry_module="task_demo",
            entry_callable="run",
            worker_count=2,
            node_count=1,
        )

    try:
        resp = session.submit_payloads([{"value": 1}, {"value": 2}])
        assert len(resp.accepted) == 2
        results = list(session.wait_for_results(expected_count=2, timeout_sec=5.0))
        assert len(results) == 2
        assert {item.status for item in results} == {pb2.TASK_STATUS_SUCCEEDED}
        assert len(calls) == 2
        assert all(method == "run" for method, _payload, _timeout in calls)
        assert session.methods == ["run"]
        assert session.run.sync(value=3)["echo"] == {"value": 3}
        assert session.call_sync("run", value=6)["echo"] == {"value": 6}
        mapped = session.map([4, 5], timeout_sec=5.0)
        assert len(mapped) == 2
    finally:
        session.close()


def test_dedicated_task_pool_proxy_submit_returns_task_id() -> None:
    from pycloud_parallel.controlplane.client import DedicatedTaskServiceSession

    group, _calls, _update_calls = _fake_group()
    session = DedicatedTaskServiceSession(group=group, task_method="run", job_id="job-dedicated-submit")
    try:
        task_id = session.run.submit(value=7)
        assert str(task_id).startswith("job-dedicated-submit-task-")
    finally:
        session.close()


def test_dedicated_task_pool_update_globals_delegates_to_group() -> None:
    from pycloud_parallel.controlplane.client import DedicatedTaskServiceSession

    group, _calls, update_calls = _fake_group()
    session = DedicatedTaskServiceSession(group=group, task_method="run", job_id="job-dedicated-update")
    try:
        digest = session.update_globals({"cfg": {"k": "v"}})
        assert digest == "sha256:group-digest"
        assert update_calls == [{"cfg": {"k": "v"}}]
    finally:
        session.close()


def test_dedicated_task_pool_status_map_delegates_to_group() -> None:
    from pycloud_parallel.controlplane.client import DedicatedTaskServiceSession

    status = SimpleNamespace(status=pb2.SERVICE_STATUS_RUNNING)
    group, _calls, _update_calls = _fake_group()
    group.status_map = lambda: {"node-1": status}
    session = DedicatedTaskServiceSession(group=group, task_method="run", job_id="job-dedicated-status")
    try:
        status_map = session.status_map()
        assert status_map == {"node-1": status}
    finally:
        session.close()


def test_dedicated_task_pool_imap_unordered_waits_for_delayed_results() -> None:
    from pycloud_parallel.controlplane.client import DedicatedTaskServiceSession

    def call_balanced(method, payload, *, timeout_sec=60.0, **_kwargs):
        del method, timeout_sec
        time.sleep(0.2)
        return "node-1", {"data": {"echo": payload}}

    group = SimpleNamespace(
        owner_client_id="owner-demo",
        service_name="task-pool-demo",
        sessions={"node-1": SimpleNamespace(worker_count=2)},
        _artifact_code_version="sha256:test",
        call_balanced=call_balanced,
        update_globals=lambda values: "sha256:group-digest",
        status_map=lambda: {"node-1": SimpleNamespace(status=pb2.SERVICE_STATUS_RUNNING)},
        close=lambda end_services=True, reason="": None,
    )
    session = DedicatedTaskServiceSession(group=group, task_method="run", job_id="job-dedicated-imap")
    try:
        items = list(
            session.imap_unordered(
                [{"value": 1}, {"value": 2}],
                max_in_flight=2,
                receive_batch=1,
                submit_timeout_sec=5.0,
                result_timeout_sec=1.0,
            )
        )
        assert len(items) == 2
        assert {task_id for task_id, _data in items} == {
            "job-dedicated-imap-task-0001",
            "job-dedicated-imap-task-0002",
        }
    finally:
        session.close()
