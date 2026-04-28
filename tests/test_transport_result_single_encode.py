from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pycloud_parallel.controlplane.node.models import TaskState
from pycloud_parallel.controlplane.serialization import (
    TRANSPORT_ENVELOPE_SENTINEL,
    encode_transport_payload_bytes,
    transport_payload_to_inline_carrier,
)
from pycloud_parallel.controlplane.services import NodeControlService
from pycloud_parallel.execution.task_pool import _NativePoolResultAdapter
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


def test_task_result_pickle_decodes_directly_to_dataframe():
    frame = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
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
        result=frame,
        serialization_mode="pickle_stable_v1",
    )

    task_result = state.as_result()
    restored = _NativePoolResultAdapter(serialization_mode="pickle_stable_v1").fetch_result_data(task_result)

    assert task_result.HasField("transport_result")
    assert restored.equals(frame)


def test_task_result_guard_rejects_already_transport_wrapped_result():
    state = TaskState(
        task_id="task-2",
        client_id="client-1",
        job_id="job-1",
        code_version="cv",
        runtime_key="rk",
        execution_mode=pb2.EXECUTION_MODE_PERSISTENT,
        payload={},
        timeout_hint_sec=0,
        priority=1,
        status=pb2.TASK_STATUS_SUCCEEDED,
        result={
            TRANSPORT_ENVELOPE_SENTINEL: {
                "codec": "pickle_stable_v1",
                "version": 1,
                "payload": {"encoding": "base64", "data": "AAAA"},
            }
        },
        serialization_mode="pickle_stable_v1",
    )

    with pytest.raises(RuntimeError, match="already-encoded result"):
        state.as_result()


def test_task_result_passes_worker_transport_carrier_without_reencoding():
    transport = encode_transport_payload_bytes(
        {"value": 7},
        mode="pickle_stable_v1",
        context="service_result",
    )
    state = TaskState(
        task_id="task-carrier-1",
        client_id="client-1",
        job_id="job-1",
        code_version="cv",
        runtime_key="rk",
        execution_mode=pb2.EXECUTION_MODE_PERSISTENT,
        payload={},
        timeout_hint_sec=0,
        priority=1,
        status=pb2.TASK_STATUS_SUCCEEDED,
        result=transport_payload_to_inline_carrier(
            transport,
            payload_mode="result",
            context="service_result",
        ),
        serialization_mode="pickle_stable_v1",
        use_transport_result=True,
    )

    task_result = state.as_result()

    assert task_result.transport_result.codec == transport.codec
    assert bytes(task_result.transport_result.payload) == bytes(transport.payload)


def test_service_bytes_response_decodes_directly_to_ndarray():
    array = np.array([[1, 2], [3, 4]], dtype=np.int64)

    class _State:
        def call_service(self, **kwargs):
            del kwargs
            return 200, {"ok": True, "data": array}

    service = NodeControlService(_State())
    context = _FakeContext()
    request = pb2.CallServiceRequest(
        service_id="svc-1",
        method="run",
        transport_payload=encode_transport_payload_bytes(
            {"value": 1},
            mode="pickle_stable_v1",
            context="service_owner",
        ),
    )

    response = service.CallService(request, context)

    restored = _NativePoolResultAdapter(serialization_mode="pickle_stable_v1").fetch_result_data(
        pb2.TaskResult(
            task_id="task-service-1",
            status=pb2.TASK_STATUS_SUCCEEDED,
            transport_result=response.transport_data,
        )
    )
    assert np.array_equal(restored, array)


def test_service_bytes_response_passes_worker_transport_carrier_without_reencoding():
    transport = encode_transport_payload_bytes(
        {"value": 9},
        mode="pickle_stable_v1",
        context="service_result",
    )

    class _State:
        def call_service(self, **kwargs):
            del kwargs
            return 200, {
                "ok": True,
                "data": transport_payload_to_inline_carrier(
                    transport,
                    payload_mode="result",
                    context="service_result",
                ),
            }

    service = NodeControlService(_State())
    context = _FakeContext()
    request = pb2.CallServiceRequest(
        service_id="svc-1",
        method="run",
        transport_payload=encode_transport_payload_bytes(
            {"value": 1},
            mode="pickle_stable_v1",
            context="service_owner",
        ),
    )

    response = service.CallService(request, context)

    assert response.transport_data.codec == transport.codec
    assert bytes(response.transport_data.payload) == bytes(transport.payload)
