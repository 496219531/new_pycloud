from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd

from pycloud_parallel.api import TaskPool
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.controlplane.config import get_payload_policy
from pycloud_parallel.controlplane.payload_transport import (
    decode_payload_from_transport,
    encode_result_for_transport,
)
from pycloud_parallel.controlplane.serialization import (
    decode_transport_payload_bytes,
    detect_transport_mode,
    dict_to_struct,
    encode_transport_payload_bytes,
    struct_to_python,
)
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def _assert_roundtrip(actual: object, expected: object) -> None:
    if isinstance(expected, pd.DataFrame):
        assert isinstance(actual, pd.DataFrame)
        assert actual.equals(expected)
        return
    if isinstance(expected, np.ndarray):
        assert isinstance(actual, np.ndarray)
        assert np.array_equal(actual, expected)
        return
    assert actual == expected


def _payload_for_mode(mode: str) -> dict[str, object]:
    payload: dict[str, object] = {
        "frame": pd.DataFrame({"a": [1, 2]}),
        "array": np.array([1, 2, 3], dtype=np.int64),
    }
    if mode != "legacy_v1":
        payload["blob"] = b"abc"
    return payload


def _result_for_mode(mode: str) -> dict[str, object]:
    result: dict[str, object] = {
        "frame": pd.DataFrame({"b": [3, 4]}),
        "array": np.array([[5, 6], [7, 8]], dtype=np.int64),
    }
    if mode != "legacy_v1":
        result["blob"] = b"xyz"
    return result


def test_task_pool_submit_payload_roundtrips_transport_modes():
    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")

    for mode in ("legacy_v1", "structured_v1", "pickle_stable_v1"):
        captured_tasks = []
        payload = _payload_for_mode(mode)

        fake_pool_client = SimpleNamespace(
            owner_client_id="owner-demo",
            pool_id="pool-1",
            pool_token="token-1",
            code_version="sha256:test",
            worker_count=2,
            heartbeat_timeout_sec=30,
            submit_tasks=lambda tasks, job_id="": (
                captured_tasks.extend(tasks),
                pb2.SubmitTasksResponse(
                    ok=True,
                    accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                    rejected=[],
                ),
            )[1],
            pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
            heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
            close=lambda reason="": None,
            _client=SimpleNamespace(close=lambda: None),
        )

        with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
            "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
            return_value=fake_pool_client,
        ):
            mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
            session = TaskPool._from_infocenter(
                infocenter_target="127.0.0.1:50051",
                job_id=f"job-{mode}",
                source=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
                entry_module="task_demo",
                entry_callable="run",
                worker_count=2,
                node_count=1,
                serialization_mode=mode,
            )
        try:
            session.submit_payloads([payload])
            if captured_tasks[0].HasField("transport_payload") and str(captured_tasks[0].transport_payload.codec or "").strip():
                decoded = decode_transport_payload_bytes(
                    captured_tasks[0].transport_payload.codec,
                    captured_tasks[0].transport_payload.version,
                    captured_tasks[0].transport_payload.payload,
                    context="taskpool_session",
                )
            else:
                raw_payload = struct_to_python(captured_tasks[0].payload)
                decoded = decode_payload_from_transport(
                    raw_payload,
                    policy=get_payload_policy("task_submit"),
                    mode=detect_transport_mode(raw_payload, default=mode),
                    context="taskpool_session",
                )
            _assert_roundtrip(decoded["frame"], payload["frame"])
            _assert_roundtrip(decoded["array"], payload["array"])
            if mode != "legacy_v1":
                assert decoded["blob"] == payload["blob"]
        finally:
            session.close()


def test_task_pool_wait_for_data_roundtrips_transport_modes():
    for mode in ("legacy_v1", "structured_v1", "pickle_stable_v1"):
        result = _result_for_mode(mode)
        encoded = encode_result_for_transport(
            result,
            policy=get_payload_policy("result"),
            mode=mode,
        )
        result_kwargs = {
            "task_id": "pool-task-0001",
            "job_id": "job-native",
            "status": pb2.TASK_STATUS_SUCCEEDED,
        }
        if mode == "pickle_stable_v1":
            result_kwargs["transport_result"] = encode_transport_payload_bytes(
                result,
                mode=mode,
                context="taskpool_session",
            )
        else:
            result_kwargs["result"] = dict_to_struct(encoded)
        task_result = pb2.TaskResult(**result_kwargs)
        restored = NodeControlClient.__new__(NodeControlClient).fetch_result_data(task_result)
        _assert_roundtrip(restored["frame"], result["frame"])
        _assert_roundtrip(restored["array"], result["array"])
        if mode != "legacy_v1":
            assert restored["blob"] == result["blob"]
