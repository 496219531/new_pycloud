from __future__ import annotations

from types import SimpleNamespace

import numpy as np

from pycloud_parallel.controlplane.node.models import TaskState
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.controlplane.serialization import (
    dict_to_struct,
    encode_transport_payload_bytes,
)
from pycloud_parallel.controlplane.services import NodeControlService
from pycloud_parallel.execution.task_pool import _NativePoolResultAdapter, TaskPool
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


class _FakeContext:
    def __init__(self) -> None:
        self.code = None
        self.details = ""

    def set_code(self, code) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details

    def peer(self) -> str:
        return "test-peer"


def test_node_control_client_call_service_uses_transport_payload_for_pickle():
    client = NodeControlClient.__new__(NodeControlClient)
    client.timeout_sec = 5.0
    captured = {}

    def _fake_call_service(request, timeout):  # noqa: ARG001
        captured["request"] = request
        return pb2.CallServiceResponse(ok=True, service_id="svc-1", method="run")

    client.stub = SimpleNamespace(CallService=_fake_call_service)

    resp = NodeControlClient.call_service(
        client,
        service_id="svc-1",
        method="run",
        payload={"array": np.array([1, 2, 3], dtype=np.int64)},
        serialization_mode="pickle_stable_v1",
    )

    request = captured["request"]
    assert resp.ok is True
    assert request.HasField("transport_payload")
    assert request.transport_payload.codec == "pickle_stable_v1"
    assert bytes(request.transport_payload.payload)
    assert not request.payload.fields


def test_node_control_client_update_runtime_globals_uses_transport_values_for_pickle():
    client = NodeControlClient.__new__(NodeControlClient)
    client.timeout_sec = 5.0
    captured = {}

    def _fake_update(request, timeout):  # noqa: ARG001
        captured["request"] = request
        return pb2.UpdateRuntimeGlobalsResponse(ok=True, code_version="cv", runtime_key="rk")

    client.stub = SimpleNamespace(UpdateRuntimeGlobals=_fake_update)

    resp = NodeControlClient.update_runtime_globals_prepared(
        client,
        client_id="client-1",
        code_version="cv",
        runtime_key="rk",
        code_token="tok",
        prepared_values={"array": np.array([1, 2, 3], dtype=np.int64)},
        serialization_mode="pickle_stable_v1",
    )

    request = captured["request"]
    assert resp.ok is True
    assert request.HasField("transport_values")
    assert request.transport_values.codec == "pickle_stable_v1"
    assert not request.values.fields


def test_node_control_service_prefers_transport_payload_for_call_service():
    captured = {}

    class _State:
        def call_service(self, **kwargs):
            captured.update(kwargs)
            return 200, {"ok": True, "data": {"value": 7}}

    service = NodeControlService(_State())
    context = _FakeContext()
    request = pb2.CallServiceRequest(
        service_id="svc-1",
        method="run",
        payload=dict_to_struct({"value": 1}),
        transport_payload=encode_transport_payload_bytes(
            {"value": 7},
            mode="pickle_stable_v1",
            context="service_owner",
        ),
    )

    response = service.CallService(request, context)

    assert context.code is None
    assert captured["payload"]["value"] == 7
    assert response.ok is True
    assert response.HasField("transport_data")
    assert response.transport_data.codec == "pickle_stable_v1"


def test_task_pool_pickle_submit_uses_transport_payload():
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
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    session = TaskPool(
        pools={"node-1": fake_pool_client},
        nodes={},
        task_method="run",
        serialization_mode="pickle_stable_v1",
    )
    try:
        captured = {}

        def _capture_submit(tasks, job_id=""):  # noqa: ARG001
            captured["task"] = tasks[0]
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=tasks[0].task_id, status=pb2.TASK_STATUS_QUEUED)],
                rejected=[],
            )

        fake_pool_client.submit_tasks = _capture_submit
        session.submit_payloads([{"array": np.array([1, 2, 3], dtype=np.int64)}])

        task = captured["task"]
        assert task.HasField("transport_payload")
        assert task.transport_payload.codec == "pickle_stable_v1"
        assert not task.payload.fields
    finally:
        session.close()


def test_task_result_pickle_uses_transport_result_and_adapter_reads_it():
    state = TaskState(
        task_id="task-1",
        client_id="client-1",
        job_id="job-1",
        code_version="cv",
        runtime_key="rk",
        execution_mode=pb2.EXECUTION_MODE_PERSISTENT,
        payload={},
        timeout_hint_sec=0,
        priority=1,
        status=pb2.TASK_STATUS_SUCCEEDED,
        result={"array": np.array([1, 2, 3], dtype=np.int64)},
        serialization_mode="pickle_stable_v1",
    )

    task_result = state.as_result()

    assert task_result.HasField("transport_result")
    assert task_result.transport_result.codec == "pickle_stable_v1"

    restored = _NativePoolResultAdapter(serialization_mode="pickle_stable_v1").fetch_result_data(task_result)
    assert np.array_equal(restored["array"], np.array([1, 2, 3], dtype=np.int64))
