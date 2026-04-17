from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pycloud_parallel.controlplane.job_queue import JobQueueManager
from pycloud_parallel.data.ref import DataRef, data_ref_to_payload


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


def test_cancel_job_rejects_auth_token_mismatch() -> None:
    queue = JobQueueManager()
    queue.submit_job(
        {
            "job_id": "job-auth-1",
            "client_id": "client-a",
            "entry_module": "task_demo",
            "subtasks": [{"value": 1}],
        },
        auth_token="token-a",
    )

    with pytest.raises(PermissionError, match="cancel auth failed"):
        queue.cancel_job("job-auth-1", auth_token="token-b")

    cancelled = queue.cancel_job("job-auth-1", auth_token="token-a")
    assert cancelled is not None
    assert cancelled.status == "CANCELLED"


def test_cancel_job_rejects_expired_auth_token() -> None:
    queue = JobQueueManager()
    job = queue.submit_job(
        {
            "job_id": "job-auth-expired",
            "client_id": "client-a",
            "entry_module": "task_demo",
            "subtasks": [{"value": 1}],
        },
        auth_token="token-a",
    )
    job.owner_token_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)

    with pytest.raises(PermissionError, match="cancel auth expired"):
        queue.cancel_job("job-auth-expired", auth_token="token-a")


def test_submit_job_rejects_unresolvable_object_ref_payloads() -> None:
    queue = JobQueueManager()
    with pytest.raises(ValueError, match="resolvable locator"):
        queue.submit_job(
            {
                "job_id": "job-ref-1",
                "client_id": "client-a",
                "entry_module": "task_demo",
                "subtasks": [
                    {
                        "blob": data_ref_to_payload(
                            DataRef(
                                ref_id="sha256:" + ("c" * 64),
                                storage_id="sha256:" + ("c" * 64),
                                logical_type="json",
                                format="json",
                                size_bytes=12,
                                materialize_as="json",
                                locator_kind="node_local",
                                locator_token="",
                            )
                        )
                    }
                ],
            }
        )


def test_job_queue_manager_submit_job_uses_unified_inbound_normalizer(monkeypatch) -> None:
    queue = JobQueueManager()
    captured = {}

    def _fake_normalize(payload, *, object_dir, policy, resolve_object_refs=None):
        del resolve_object_refs
        captured["payload"] = dict(payload)
        captured["object_dir"] = object_dir
        captured["mode"] = policy.mode
        return dict(payload)

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.job_queue.normalize_inbound_payload",
        _fake_normalize,
    )

    job = queue.submit_job(
        {
            "job_id": "job-policy-1",
            "client_id": "client-a",
            "entry_module": "task_demo",
            "subtasks": [{"value": 1}],
        }
    )

    assert job.job_id == "job-policy-1"
    assert captured["mode"] == "job_submit"
    assert captured["payload"]["subtasks"] == [{"value": 1}]


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


def test_reorder_waiting_job_updates_waiting_order() -> None:
    queue = JobQueueManager()
    queue.submit_job({"job_id": "job-1", "client_id": "client-a", "priority": 1, "entry_module": "task_demo", "subtasks": [{"value": 1}]})
    queue.submit_job({"job_id": "job-2", "client_id": "client-a", "priority": 1, "entry_module": "task_demo", "subtasks": [{"value": 2}]})
    queue.submit_job({"job_id": "job-3", "client_id": "client-a", "priority": 1, "entry_module": "task_demo", "subtasks": [{"value": 3}]})

    moved = queue.reorder_job("job-3", direction="up")
    assert moved is not None
    moved = queue.reorder_job("job-3", direction="up")
    assert moved is not None

    summary = queue.summary()
    assert [item["job_id"] for item in summary["waiting_jobs"]] == ["job-3", "job-1", "job-2"]


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


def test_expand_subtasks_purges_loaded_driver_modules() -> None:
    queue = JobQueueManager()
    blob = (
        b"def build(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n"
    )

    with (
        patch("pycloud_parallel.controlplane.job_queue._purge_loaded_artifact_modules") as mocked,
        patch("pycloud_parallel.controlplane.job_queue.gc.collect") as mocked_gc,
    ):
        subtasks = queue._expand_subtasks(  # noqa: SLF001
            {
                "driver_blob_b64": base64.b64encode(blob).decode("utf-8"),
                "driver_entry_module": "job_driver_demo",
                "driver_entry_callable": "build",
                "driver_payload": {"value": 1, "count": 2},
                "driver_package_format": "py",
            }
        )

    assert subtasks == [{"value": 1}, {"value": 2}]
    mocked.assert_called_once()
    mocked_gc.assert_called_once()
    assert mocked.call_args.kwargs["entry_module"] == "job_driver_demo"
    assert mocked.call_args.kwargs["package_format"] == "py"


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
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-pool-1")  # noqa: SLF001

    mocked.assert_called_once()
    job = queue.get_job("job-pool-1")
    assert job is not None
    assert job.status == "SUCCEEDED"
    assert len(job.results) == 2


def test_run_job_with_hooks_uses_generator_handler_and_finalize() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    value = int(value)\n"
        b"    return {'value': value, 'square': value * value}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    for i in range(count):\n"
        b"        yield {'value': value + i}\n\n"
        b"def handle_result(task_id, result, state=None, **_kwargs):\n"
        b"    state.setdefault('squares', []).append(result['square'])\n\n"
        b"def finalize(state=None, **_kwargs):\n"
        b"    return {'sum_square': sum(state.get('squares', []))}\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-1",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "handle_result_callable": "handle_result",
            "finalize_callable": "finalize",
            "job_payload": {"value": 2, "count": 3},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-1"
            self.updated_globals = []

        def unordered(self, payloads, **kwargs):
            assert kwargs["max_in_flight"] >= 1
            items = list(payloads)
            assert items == [{"value": 2}, {"value": 3}, {"value": 4}]
            for idx, item in enumerate(items, start=1):
                value = int(item["value"])
                yield f"t-{idx}", {"value": value, "square": value * value}

        def update_globals(self, values):
            self.updated_globals.append(dict(values))

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-hooks-1")  # noqa: SLF001

    mocked.assert_called_once()
    job = queue.get_job("job-hooks-1")
    assert job is not None
    assert job.status == "SUCCEEDED"
    assert [item["result"]["square"] for item in job.results] == [4, 9, 16]
    assert job.final_result == {"sum_square": 29}


def test_run_job_with_hooks_accepts_update_globals_callable_name() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value)}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n\n"
        b"def build_globals(value=0, count=1, **_kwargs):\n"
        b"    return {'cfg': {'base': int(value), 'count': int(count)}}\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-globals-name",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_globals_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "job_payload": {"value": 2, "count": 3},
            "update_globals": "build_globals",
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-globals-name"
            self.updated_globals = []

        def unordered(self, payloads, **kwargs):
            del kwargs
            for idx, item in enumerate(list(payloads), start=1):
                yield f"t-{idx}", {"value": int(item["value"])}

        def update_globals(self, values):
            self.updated_globals.append(dict(values))

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-hooks-globals-name")  # noqa: SLF001

    assert fake_pool.updated_globals == [{"cfg": {"base": 2, "count": 3}}]
    assert mocked.call_args.kwargs["managed_global_names"] == ["cfg"]
    job = queue.get_job("job-hooks-globals-name")
    assert job is not None
    assert job.status == "SUCCEEDED"


def test_run_job_with_hooks_uses_entryfunc_for_taskpool() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value)}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-entryfunc",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_entryfunc_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "job_payload": {"value": 2, "count": 3},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-entryfunc"

        def unordered(self, payloads, **kwargs):
            del kwargs
            for idx, item in enumerate(list(payloads), start=1):
                yield f"t-{idx}", {"value": int(item["value"])}

        def update_globals(self, values):
            return values

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-hooks-entryfunc")  # noqa: SLF001

    call_kwargs = mocked.call_args.kwargs
    assert callable(call_kwargs["entry_func"])
    assert call_kwargs["entry_func"].__name__ == "run"
    assert "func" not in call_kwargs
    assert "blob" not in call_kwargs
    assert "entry_module" not in call_kwargs


def test_run_job_with_hooks_accepts_direct_payload_list_task_generator() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value)}\n"
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-direct-payloads",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_direct_payloads_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": [{"value": 7}, {"value": 8}],
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-direct-payloads"

        def unordered(self, payloads, **kwargs):
            del kwargs
            items = list(payloads)
            assert items == [{"value": 7}, {"value": 8}]
            for idx, item in enumerate(items, start=1):
                yield f"t-{idx}", {"value": int(item["value"])}

        def update_globals(self, values):
            return values

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool):
        queue._run_job("job-hooks-direct-payloads")  # noqa: SLF001

    job = queue.get_job("job-hooks-direct-payloads")
    assert job is not None
    assert job.status == "SUCCEEDED"


def test_run_job_with_hooks_accepts_update_globals_dict() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value)}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-globals-dict",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_globals_dict_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "job_payload": {"value": 1, "count": 2},
            "update_globals": {"cfg": {"mode": "fast"}},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-globals-dict"
            self.updated_globals = []

        def unordered(self, payloads, **kwargs):
            del kwargs
            for idx, item in enumerate(list(payloads), start=1):
                yield f"t-{idx}", {"value": int(item["value"])}

        def update_globals(self, values):
            self.updated_globals.append(dict(values))

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-hooks-globals-dict")  # noqa: SLF001

    assert fake_pool.updated_globals == [{"cfg": {"mode": "fast"}}]
    assert mocked.call_args.kwargs["managed_global_names"] == ["cfg"]
    job = queue.get_job("job-hooks-globals-dict")
    assert job is not None
    assert job.status == "SUCCEEDED"


def test_run_job_with_hooks_accepts_blob_ref_payload() -> None:
    from pycloud_parallel.data.ref import DataRef, data_ref_to_payload

    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    value = int(value)\n"
        b"    return {'value': value, 'square': value * value}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    for i in range(count):\n"
        b"        yield {'value': value + i}\n\n"
        b"def handle_result(task_id, result, state=None, **_kwargs):\n"
        b"    state.setdefault('items', []).append(result)\n\n"
        b"def finalize(state=None, **_kwargs):\n"
        b"    return {'count': len(state.get('items', []))}\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-ref-1",
            "client_id": "client-a",
            "priority": 5,
            "blob_ref": data_ref_to_payload(
                DataRef(
                    ref_id="sha256:" + ("b" * 64),
                    storage_id="sha256:" + ("b" * 64),
                    logical_type="bytes",
                    format="py",
                    size_bytes=len(module_blob),
                    materialize_as="bytes",
                    locator_kind="node_local",
                    locator_token="",
                )
            ),
            "blob_control_addr": "127.0.0.1:50061",
            "entry_module": "job_hooks_ref_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "handle_result_callable": "handle_result",
            "finalize_callable": "finalize",
            "job_payload": {"value": 2, "count": 3},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-ref-1"

        def unordered(self, payloads, **kwargs):
            del kwargs
            items = list(payloads)
            assert items == [{"value": 2}, {"value": 3}, {"value": 4}]
            for idx, item in enumerate(items, start=1):
                value = int(item["value"])
                yield f"t-{idx}", {"value": value, "square": value * value}

        def update_globals(self, values):
            return values

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with (
        patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool),
        patch(
            "pycloud_parallel.controlplane.job_queue.NodeControlClient.download_object_bytes",
            return_value=module_blob,
        ),
    ):
        queue._run_job("job-hooks-ref-1")  # noqa: SLF001

    job = queue.get_job("job-hooks-ref-1")
    assert job is not None
    assert job.status == "SUCCEEDED"
    assert job.final_result == {"count": 3}


def test_job_queue_manager_submit_job_tracks_controlplane_data_ref_in_nested_business_blob_ref() -> None:
    from pycloud_parallel.data.ref import DataRef

    queue = JobQueueManager()

    data_ref = DataRef(
        ref_id="sha256:" + ("b" * 64),
        storage_id="sha256:" + ("b" * 64),
        format="bin",
        size_bytes=16,
        locator_kind="controlplane",
        locator_token="http://127.0.0.1:50051",
    )
    job = queue.submit_job(
        {
            "job_id": "job-invalid-job-payload-ref",
            "client_id": "client-a",
            "entry_module": "job_demo",
            "job_mode": "hooks",
            "blob_b64": base64.b64encode(
                b"def run(**_kwargs):\n    return {}\n\ndef task_generator(**_kwargs):\n    return []\n"
            ).decode("utf-8"),
            "package_format": "py",
            "job_payload": {"blob_ref": data_ref},
        }
    )

    assert job.staged_ref_ids == [data_ref.ref_id]
    assert job.payload_schema_version == 2
    assert job.payload["job_payload"]["blob_ref"] == data_ref


def test_resolve_payload_data_refs_falls_back_across_replicas(monkeypatch) -> None:
    from pycloud_parallel.data.ref import DataRef
    from pycloud_parallel.controlplane.data_registry import ResolvedDataRef
    from pycloud_parallel.controlplane.job_queue import _resolve_payload_data_refs

    data_ref = DataRef(
        ref_id="sha256:" + ("c" * 64),
        storage_id="sha256:" + ("c" * 64),
        format="json",
        size_bytes=32,
        logical_type="json",
        materialize_as="json",
        locator_kind="controlplane",
        locator_token="http://127.0.0.1:50051",
    )
    attempts = []

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.job_queue.DataRegistryClient.resolve",
        lambda self, ref: ResolvedDataRef(
            ref=data_ref,
            control_addr="127.0.0.1:50062",
            locator_kind="node_control",
            locator_token="127.0.0.1:50062",
            via_registry=True,
            replicas=(
                {
                    "control_addr": "127.0.0.1:50061",
                    "node_id": "node-1",
                    "node_instance_id": "node-1-inst",
                },
                {
                    "control_addr": "127.0.0.1:50062",
                    "node_id": "node-2",
                    "node_instance_id": "node-2-inst",
                },
            ),
        ),
    )

    class _FakeNodeClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            del timeout_sec
            self.target = target

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def fetch_result_ref_data(self, result_ref):
            del result_ref
            attempts.append(self.target)
            if self.target.endswith(":50061"):
                raise RuntimeError("replica unavailable")
            return {"value": 1}

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.job_queue.NodeControlClient",
        _FakeNodeClient,
    )

    resolved = _resolve_payload_data_refs(
        {"job_payload": {"blob_ref": data_ref}},
        registry_target="http://127.0.0.1:50051",
        timeout_sec=1.0,
    )

    assert resolved["job_payload"]["blob_ref"] == {"value": 1}
    assert attempts == ["127.0.0.1:50061", "127.0.0.1:50062"]


def test_submit_job_rejects_nested_unresolvable_business_blob_ref() -> None:
    queue = JobQueueManager()
    with pytest.raises(ValueError, match="resolvable locator"):
        queue.submit_job(
            {
                "job_id": "job-nested-ref-1",
                "client_id": "client-a",
                "entry_module": "task_demo",
                "job_mode": "hooks",
                "blob_b64": base64.b64encode(
                    b"def run(**_kwargs):\n    return {}\n\ndef task_generator(**_kwargs):\n    return []\n"
                ).decode("utf-8"),
                "package_format": "py",
                "job_payload": {
                    "blob_ref": data_ref_to_payload(
                        DataRef(
                            ref_id="sha256:" + ("e" * 64),
                            storage_id="sha256:" + ("e" * 64),
                            logical_type="json",
                            format="json",
                            size_bytes=12,
                            materialize_as="json",
                            locator_kind="node_local",
                            locator_token="",
                        )
                    )
                },
            }
        )


def test_job_queue_manager_close_releases_staged_refs(monkeypatch) -> None:
    from pycloud_parallel.data.ref import DataRef

    released = []
    queue = JobQueueManager()
    queue._controlplane_target = "http://127.0.0.1:50051"  # noqa: SLF001
    data_ref = DataRef(
        ref_id="sha256:" + ("d" * 64),
        storage_id="sha256:" + ("d" * 64),
        format="json",
        size_bytes=24,
        logical_type="json",
        materialize_as="json",
        locator_kind="controlplane",
        locator_token="http://127.0.0.1:50051",
    )
    job = queue.submit_job(
        {
            "job_id": "job-close-release",
            "client_id": "client-a",
            "entry_module": "job_demo",
            "subtasks": [{"cfg": data_ref}],
        }
    )

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.job_queue.DataRegistryClient.release",
        lambda self, ref_id: released.append(ref_id) or {"ok": True},
    )

    queue.close()

    assert released == [data_ref.ref_id]
    current = queue.get_job(job.job_id)
    assert current is not None
    assert current.staged_ref_ids == []


def test_run_job_with_hooks_purges_loaded_modules() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value)}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-purge-1",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_purge_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "job_payload": {"value": 1, "count": 2},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-purge-1"

        def unordered(self, payloads, **kwargs):
            del kwargs
            for idx, item in enumerate(list(payloads), start=1):
                yield f"t-{idx}", {"value": int(item["value"])}

        def update_globals(self, values):
            return values

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with (
        patch("pycloud_parallel.controlplane.job_queue._purge_loaded_artifact_modules") as mocked,
        patch("pycloud_parallel.controlplane.job_queue.gc.collect") as mocked_gc,
        patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool),
    ):
        queue._run_job("job-hooks-purge-1")  # noqa: SLF001

    job = queue.get_job("job-hooks-purge-1")
    assert job is not None
    assert job.status == "SUCCEEDED"
    mocked.assert_called_once()
    mocked_gc.assert_called_once()
    assert mocked.call_args.kwargs["entry_module"] == "job_hooks_purge_demo"
    assert mocked.call_args.kwargs["package_format"] == "py"


def test_job_queue_client_submit_job_from_bytes_uses_minimal_payload() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]
    resp = client.submit_job_from_bytes(
        blob=b"def run(**_kwargs):\n    return {}\n\ndef task_generator(**_kwargs):\n    return []\n",
        entry_module="job_demo",
    )
    assert resp == {"ok": True}
    assert captured["job_mode"] == "hooks"
    assert captured["client_id"] == "client-a"
    assert captured["entry_module"] == "job_demo"
    assert captured["entry_callable"] == "run"
    assert captured["task_generator_callable"] == "task_generator"
    assert "handle_result_callable" not in captured
    assert "finalize_callable" not in captured
    assert "pool_worker_count" not in captured
    assert "priority" not in captured


def test_job_queue_client_submit_uses_source_bytes_as_default_product_path() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]
    resp = client.submit(
        source=b"def run(**_kwargs):\n    return {}\n\ndef task_generator(**_kwargs):\n    return []\n",
        entry_module="job_demo",
    )
    assert resp == {"ok": True}
    assert captured["entry_module"] == "job_demo"
    assert captured["entry_callable"] == "run"
    assert captured["task_generator_callable"] == "task_generator"
    assert captured["package_format"] == "py"


def test_job_queue_client_submit_accepts_advanced_artifact() -> None:
    from pycloud_parallel import JobQueue
    from pycloud_parallel.artifact import Artifact, ArtifactDeps

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]
    resp = client.submit(
        artifact=Artifact.from_bytes(
            b"def run(**_kwargs):\n    return {}\n\ndef task_generator(**_kwargs):\n    return []\n",
            package_format="py",
            entry_module="job_demo",
            deps=ArtifactDeps.allow_install(["orjson==3.10.18"]),
        ),
    )
    assert resp == {"ok": True}
    assert captured["entry_module"] == "job_demo"
    assert captured["dependency_allowlist"] == ["orjson==3.10.18"]


def test_job_queue_client_submit_accepts_module_source_via_unified_artifact_path() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]

    import types

    module = types.ModuleType("job_module_demo")
    exec(
        (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': value}\n\n"
            b"def task_generator(value=0, **_kwargs):\n"
            b"    return [{'value': value}]\n"
        ).decode("utf-8"),
        module.__dict__,
    )

    with patch(
        "pycloud_parallel.controlplane.artifact._prepare_artifact_blob",
        return_value=(b"blob", "job_module_demo.tar.gz"),
    ):
        resp = client.submit(source=module)

    assert resp == {"ok": True}
    assert captured["entry_module"] == "job_module_demo"
    assert captured["task_generator_callable"] == "task_generator"
    assert captured["package_format"] == "tar.gz"


def test_job_queue_client_submit_rejects_callable_source() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")

    def _job_func(**_kwargs):
        return {}

    with pytest.raises(ValueError, match="JobQueue.submit\\(source=callable\\) is not supported"):
        client.submit(source=_job_func)


def test_job_queue_client_submit_job_from_bytes_auto_binds_update_globals() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]
    resp = client.submit_job_from_bytes(
        blob=(
            b"def run(**_kwargs):\n    return {}\n\n"
            b"def task_generator(**_kwargs):\n    return []\n\n"
            b"def update_globals(**_kwargs):\n    return {'cfg': {'k': 'v'}}\n"
        ),
        entry_module="job_demo",
    )
    assert resp == {"ok": True}
    assert captured["update_globals"] == "update_globals"


def test_job_queue_client_submit_job_from_bytes_auto_binds_handle_data_alias() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]
    resp = client.submit_job_from_bytes(
        blob=(
            b"def run(**_kwargs):\n    return {}\n\n"
            b"def task_generator(**_kwargs):\n    return []\n\n"
            b"def handle_data(task_id, result, state=None, **_kwargs):\n    return state\n"
        ),
        entry_module="job_demo",
    )
    assert resp == {"ok": True}
    assert captured["handle_result_callable"] == "handle_data"
    assert "finalize_callable" not in captured


def test_job_queue_client_submit_job_from_bytes_auto_binds_finalize_when_present() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]
    resp = client.submit_job_from_bytes(
        blob=(
            b"def run(**_kwargs):\n    return {}\n\n"
            b"def task_generator(**_kwargs):\n    return []\n\n"
            b"def finalize(state=None, **_kwargs):\n    return state\n"
        ),
        entry_module="job_demo",
    )
    assert resp == {"ok": True}
    assert captured["finalize_callable"] == "finalize"


def test_prepare_job_blob_submit_fields_uses_object_ref_for_large_blob(monkeypatch) -> None:
    from pycloud_parallel.execution.support import _prepare_job_blob_submit_fields
    from pycloud_parallel.data.ref import DataRef

    monkeypatch.setattr(
        "pycloud_parallel.execution.support._job_blob_requires_object_ref",
        lambda blob: True,
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.infocenter_client.InfoCenterClient.select_task_nodes",
        lambda self, **kwargs: [SimpleNamespace(control_addr="127.0.0.1:50061")],
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.upload_object_from_bytes",
        lambda self, **kwargs: DataRef(
            ref_id="sha256:" + ("a" * 64),
            storage_id="sha256:" + ("a" * 64),
            logical_type="archive",
            format="tar.gz",
            size_bytes=4096,
            materialize_as="path",
            locator_kind="node_local",
            locator_token="",
        ),
    )

    fields = _prepare_job_blob_submit_fields(
        target="127.0.0.1:50051",
        blob=b"x" * (3 * 1024 * 1024),
        package_format="tar.gz",
        runtime="py3",
        timeout_sec=10.0,
    )
    assert "blob_b64" not in fields
    assert fields["blob_control_addr"] == "127.0.0.1:50061"
    assert fields["blob_ref"].object_id == "sha256:" + ("a" * 64)


def test_prepare_job_submit_payload_for_call_preserves_staged_update_globals(monkeypatch) -> None:
    from pycloud_parallel.data.ref import DataRef
    from pycloud_parallel.execution.support import _prepare_job_submit_payload_for_call

    class _FakeUploadClient:
        def close(self) -> None:
            return None

    staged_ref = DataRef(
        ref_id="sha256:" + ("9" * 64),
        storage_id="sha256:" + ("9" * 64),
        format="json",
        size_bytes=32,
        logical_type="json",
        materialize_as="json",
        locator_kind="controlplane",
        locator_token="http://127.0.0.1:50051",
    )

    monkeypatch.setattr(
        "pycloud_parallel.execution.support._job_submit_upload_clients",
        lambda **kwargs: [_FakeUploadClient()],
    )
    monkeypatch.setattr(
        "pycloud_parallel.execution.support.prepare_outbound_payload",
        lambda payload, **kwargs: {**dict(payload or {}), "artifact_path": "prepared-object-ref"},
    )

    prepared = _prepare_job_submit_payload_for_call(
        target="127.0.0.1:50051",
        payload={
            "entry_module": "job_demo",
            "artifact_path": "demo.py",
            "update_globals": {"cfg": staged_ref},
        },
        timeout_sec=10.0,
    )

    assert prepared["artifact_path"] == "prepared-object-ref"
    assert prepared["update_globals"]["cfg"] == staged_ref


def test_job_queue_client_submit_job_accepts_controlplane_data_ref_in_job_payload(monkeypatch) -> None:
    from pycloud_parallel import DataRef, JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    data_ref = DataRef(
        ref_id="sha256:" + ("a" * 64),
        storage_id="sha256:" + ("a" * 64),
        format="bin",
        size_bytes=12,
        locator_kind="controlplane",
        locator_token="http://127.0.0.1:50051",
    )
    captured = {}

    monkeypatch.setattr(
        "pycloud_parallel.execution.support._job_submit_upload_clients",
        lambda **kwargs: [],
    )

    def _fake_call(*, service_name, method, payload=None, timeout_sec=60.0, service_token=None):
        del service_name, method, timeout_sec, service_token
        captured["payload"] = dict(payload or {})
        return {"ok": True, "job": {"job_id": "job-1", "status": "WAITING"}}

    client._service_client.call = _fake_call  # type: ignore[method-assign]
    resp = client.submit_job(
        {
            "entry_module": "job_demo",
            "job_payload": {"cfg": data_ref},
        }
    )

    assert resp["job"]["job_id"] == "job-1"
    assert captured["payload"]["job_payload"]["cfg"] == data_ref


def test_job_queue_client_submit_job_stages_oversized_job_payload(monkeypatch) -> None:
    from pycloud_parallel import DataRef, JobQueue
    import pycloud_parallel.execution.support as client_mod

    monkeypatch.setattr("pycloud_parallel.execution.support.INLINE_PAYLOAD_SOFT_LIMIT_BYTES", 32)
    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}
    registered = {}
    replicas = [
        {
            "node_id": "node-1",
            "node_instance_id": "node-1-inst",
            "control_addr": "127.0.0.1:50061",
        },
        {
            "node_id": "node-2",
            "node_instance_id": "node-2-inst",
            "control_addr": "127.0.0.1:50062",
        },
    ]

    class _FakeStageClient:
        def __init__(self, target: str) -> None:
            self.target = target

        def upload_object_from_bytes(self, *, blob, format="", chunk_size=0):
            del blob, chunk_size
            return DataRef(
                ref_id="sha256:" + ("d" * 64),
                storage_id="sha256:" + ("d" * 64),
                logical_type="text",
                format=format or "txt",
                size_bytes=256,
                materialize_as="text",
                locator_kind="node_local",
                locator_token="",
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "pycloud_parallel.execution.support._select_job_staging_clients",
        lambda **kwargs: (
            [_FakeStageClient("127.0.0.1:50061"), _FakeStageClient("127.0.0.1:50062")],
            list(replicas),
        ),
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.data_registry.DataRegistryClient.register",
        lambda self, ref, **kwargs: registered.update({"ref": ref, **kwargs}) or {"ok": True},
    )
    monkeypatch.setattr(
        "pycloud_parallel.execution.support._job_submit_upload_clients",
        lambda **kwargs: [],
    )

    def _fake_call(*, service_name, method, payload=None, timeout_sec=60.0, service_token=None):
        del service_name, method, timeout_sec, service_token
        captured["payload"] = dict(payload or {})
        return {"ok": True, "job": {"job_id": "job-1", "status": "WAITING"}}

    client._service_client.call = _fake_call  # type: ignore[method-assign]
    resp = client.submit_job(
        {
            "entry_module": "job_demo",
            "runtime": "py3",
            "job_payload": {"text": "x" * 256},
        }
    )

    assert resp["job"]["job_id"] == "job-1"
    assert isinstance(captured["payload"]["job_payload"], DataRef)
    assert captured["payload"]["job_payload"].locator_kind == "controlplane"
    assert registered["replicas"] == replicas


def test_job_queue_client_submit_job_keeps_directory_root_dir_inline(monkeypatch, tmp_path) -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fail_if_staged(**kwargs):
        raise AssertionError(f"directory payload should not be staged: {kwargs}")

    monkeypatch.setattr(
        "pycloud_parallel.execution.support._select_job_staging_clients",
        _fail_if_staged,
    )
    monkeypatch.setattr(
        "pycloud_parallel.execution.support._job_submit_upload_clients",
        lambda **kwargs: [],
    )

    def _fake_call(*, service_name, method, payload=None, timeout_sec=60.0, service_token=None):
        del service_name, method, timeout_sec, service_token
        captured["payload"] = dict(payload or {})
        return {"ok": True, "job": {"job_id": "job-1", "status": "WAITING"}}

    client._service_client.call = _fake_call  # type: ignore[method-assign]
    resp = client.submit_job(
        {
            "entry_module": "job_demo",
            "runtime": "py3",
            "job_payload": {"root_dir": str(tmp_path)},
        }
    )

    assert resp["job"]["job_id"] == "job-1"
    assert captured["payload"]["job_payload"]["root_dir"] == str(tmp_path)


def test_job_queue_client_submit_job_uses_unified_outbound_policy(monkeypatch) -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    class _FakeUploadClient:
        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "pycloud_parallel.execution.support._job_submit_upload_clients",
        lambda **kwargs: [_FakeUploadClient()],
    )

    def _fake_prepare(payload, *, put_data, estimate_inline_size, policy):
        del put_data, estimate_inline_size
        captured["payload"] = dict(payload or {})
        captured["mode"] = policy.mode
        captured["managed_global_field_names"] = policy.managed_global_field_names
        return dict(payload or {})

    monkeypatch.setattr(
        "pycloud_parallel.execution.support.prepare_outbound_payload",
        _fake_prepare,
    )

    client._service_client.call = lambda **kwargs: {"ok": True, "job": {"job_id": "job-1", "status": "WAITING"}}  # type: ignore[method-assign]
    resp = client.submit_job({"entry_module": "job_demo", "update_globals": {"cfg": {"k": "v"}}})

    assert resp["job"]["job_id"] == "job-1"
    assert captured["mode"] == "job_submit"
    assert captured["managed_global_field_names"] == ("update_globals",)


def test_job_queue_client_restores_cached_session(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("PYCLOUD_JOB_CLIENT_SESSION_DIR", str(tmp_path))

    from pycloud_parallel import JobQueue

    first = JobQueue("127.0.0.1:50051")
    second = JobQueue("127.0.0.1:50051")

    assert first.client_id == second.client_id
    assert first.auth_token == second.auth_token


def test_job_queue_client_rotates_expired_cached_session(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("PYCLOUD_JOB_CLIENT_SESSION_DIR", str(tmp_path))

    from pycloud_parallel import JobQueue
    from pycloud_parallel.execution.support import _job_client_session_cache_file

    first = JobQueue("127.0.0.1:50051", client_id="client-cache")
    cache_path = _job_client_session_cache_file(
        target="127.0.0.1:50051",
        service_name="job-orchestrator",
        client_scope="client-cache",
    )
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    payload["expires_at"] = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    cache_path.write_text(json.dumps(payload), encoding="utf-8")

    second = JobQueue("127.0.0.1:50051", client_id="client-cache")

    assert second.client_id == "client-cache"
    assert second.auth_token != first.auth_token


def test_job_queue_client_recent_job_ids_tracks_and_restores(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("PYCLOUD_JOB_CLIENT_SESSION_DIR", str(tmp_path))

    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-recent")
    job_ids = iter(["job-1", "job-2", "job-1"])

    def _fake_call(*, service_name, method, payload=None, timeout_sec=60.0, service_token=None):
        del service_name, payload, timeout_sec, service_token
        if method == "submit_job":
            return {"ok": True, "job": {"job_id": next(job_ids)}}
        raise AssertionError(f"unexpected method: {method}")

    client._service_client.call = _fake_call  # type: ignore[method-assign]
    client.submit_job({"entry_module": "job_demo"})
    client.submit_job({"entry_module": "job_demo"})
    client.submit_job({"entry_module": "job_demo"})

    assert client.recent_job_ids() == ["job-1", "job-2"]

    restored = JobQueue("127.0.0.1:50051", client_id="client-recent")
    assert restored.recent_job_ids() == ["job-1", "job-2"]


def test_job_queue_client_discovers_job_orchestrator_via_infocenter(monkeypatch) -> None:
    from pycloud_parallel import JobQueue
    from pycloud_parallel.controlplane.infocenter_client import InfoCenterServiceRoute
    from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

    route = InfoCenterServiceRoute(
        service_name="job-orchestrator",
        service_id="job-orch-1",
        status=pb2.SERVICE_STATUS_RUNNING,
        node_instance_id="job-orch-1-inst",
        node_id="job-orch-1",
        control_addr="",
        node_healthy=True,
        worker_count=1,
        alive_workers=1,
        in_flight=0,
        lease_expire_at=datetime.now(timezone.utc),
        http_base_url="http://127.0.0.1:18080/svc/job-orch-1",
    )
    captured = {}

    def _fake_list_service_routes(self, *, service_name="", healthy_only=True, limit=500):
        captured["service_name"] = service_name
        captured["healthy_only"] = healthy_only
        captured["limit"] = limit
        return [route]

    def _fake_call_route_http(route_arg, *, method, payload, timeout_sec, service_token):
        captured["route"] = route_arg
        captured["method"] = method
        captured["payload"] = dict(payload or {})
        captured["timeout_sec"] = timeout_sec
        captured["service_token"] = service_token
        return {"ok": True, "job": {"job_id": "job-1", "status": "WAITING"}}

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.infocenter_client.InfoCenterClient.list_service_routes",
        _fake_list_service_routes,
    )
    monkeypatch.setattr(
        "pycloud_parallel.execution.queue._call_route_http",
        _fake_call_route_http,
    )

    client = JobQueue("127.0.0.1:50051", client_id="client-discovery", auth_token="token-discovery")
    try:
        resp = client.get_job_status("job-1")
    finally:
        client.close()

    assert resp["job"]["job_id"] == "job-1"
    assert captured["service_name"] == "job-orchestrator"
    assert captured["method"] == "get_job_status"
    assert captured["payload"] == {"job_id": "job-1"}
    assert captured["service_token"] == "token-discovery"
    assert captured["route"].http_base_url == "http://127.0.0.1:18080/svc/job-orch-1"


def test_job_queue_client_submit_job_from_module_builds_payloads() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-module")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]

    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': value}\n\n"
        b"def task_generator(value=0, **_kwargs):\n"
        b"    return [{'value': value}]\n\n"
        b"def handle_result(task_id, result, state=None, **_kwargs):\n"
        b"    state.setdefault('items', []).append((task_id, result))\n"
    )
    module_name = "job_module_demo"

    import types

    module = types.ModuleType(module_name)
    exec(module_blob.decode("utf-8"), module.__dict__)

    with patch("pycloud_parallel.execution.support._prepare_code_blob", return_value=(module_blob, f"{module_name}.tar.gz")):
        resp = client.submit_job_from_module(module=module)
    assert resp == {"ok": True}
    assert captured["client_id"] == "client-module"
    assert captured["entry_module"] == module_name
    assert captured["entry_callable"] == "run"
    assert captured["task_generator_callable"] == "task_generator"
    assert captured["handle_result_callable"] == "handle_result"
    assert "finalize_callable" not in captured
    assert captured["package_format"] == "tar.gz"
    assert captured["blob_b64"]


def test_job_queue_client_submit_job_from_module_auto_binds_update_globals() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-module")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]

    import types

    module = types.ModuleType("job_module_demo_with_globals")
    exec(
        (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': value}\n\n"
            b"def task_generator(value=0, **_kwargs):\n"
            b"    return [{'value': value}]\n\n"
            b"def handle_result(task_id, result, state=None, **_kwargs):\n"
            b"    state.setdefault('items', []).append((task_id, result))\n\n"
            b"def update_globals(**_kwargs):\n"
            b"    return {'cfg': {'mode': 'auto'}}\n"
        ).decode("utf-8"),
        module.__dict__,
    )

    with patch(
        "pycloud_parallel.execution.support._prepare_code_blob",
        return_value=(b"blob", "job_module_demo_with_globals.tar.gz"),
    ):
        resp = client.submit_job_from_module(module=module)
    assert resp == {"ok": True}
    assert captured["update_globals"] == "update_globals"


def test_job_queue_client_submit_job_from_module_auto_binds_handle_data_alias() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-module")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]

    import types

    module = types.ModuleType("job_module_demo_with_handle_data")
    exec(
        (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': value}\n\n"
            b"def task_generator(value=0, **_kwargs):\n"
            b"    return [{'value': value}]\n\n"
            b"def handle_data(task_id, result, state=None, **_kwargs):\n"
            b"    state.setdefault('items', []).append((task_id, result))\n"
        ).decode("utf-8"),
        module.__dict__,
    )

    with patch(
        "pycloud_parallel.execution.support._prepare_code_blob",
        return_value=(b"blob", "job_module_demo_with_handle_data.tar.gz"),
    ):
        resp = client.submit_job_from_module(module=module)
    assert resp == {"ok": True}
    assert captured["handle_result_callable"] == "handle_data"
    assert "finalize_callable" not in captured


def test_job_queue_client_submit_job_from_module_auto_binds_finalize_when_present() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-module")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]

    import types

    module = types.ModuleType("job_module_demo_with_finalize")
    exec(
        (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': value}\n\n"
            b"def task_generator(value=0, **_kwargs):\n"
            b"    return [{'value': value}]\n\n"
            b"def finalize(state=None, **_kwargs):\n"
            b"    return state\n"
        ).decode("utf-8"),
        module.__dict__,
    )

    with patch(
        "pycloud_parallel.execution.support._prepare_code_blob",
        return_value=(b"blob", "job_module_demo_with_finalize.tar.gz"),
    ):
        resp = client.submit_job_from_module(module=module)
    assert resp == {"ok": True}
    assert captured["finalize_callable"] == "finalize"


def test_job_queue_client_submit_job_from_module_accepts_update_globals_dict() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-module")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]

    import types

    module = types.ModuleType("job_module_demo_with_explicit_globals")
    exec(
        (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': value}\n\n"
            b"def task_generator(value=0, **_kwargs):\n"
            b"    return [{'value': value}]\n"
        ).decode("utf-8"),
        module.__dict__,
    )

    with patch(
        "pycloud_parallel.execution.support._prepare_code_blob",
        return_value=(b"blob", "job_module_demo_with_explicit_globals.tar.gz"),
    ):
        resp = client.submit_job_from_module(module=module, update_globals={"cfg": {"mode": "manual"}})
    assert resp == {"ok": True}
    assert captured["update_globals"] == {"cfg": {"mode": "manual"}}


def test_run_job_with_hooks_uses_larger_default_worker_node_and_inflight(monkeypatch) -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value)}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-defaults",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_defaults_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "job_payload": {"value": 2, "count": 3},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        job_id = "job-hooks-defaults"

        def unordered(self, payloads, **kwargs):
            assert kwargs["max_in_flight"] == 100
            assert kwargs["receive_batch"] == 10
            for idx, item in enumerate(list(payloads), start=1):
                yield f"t-{idx}", {"value": int(item["value"])}

        def update_globals(self, values):
            return values

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    monkeypatch.setattr("pycloud_parallel.controlplane.job_queue.os.cpu_count", lambda: 8)
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.job_queue.InfoCenterClient.select_task_nodes",
        lambda self, **kwargs: [SimpleNamespace(node_id="n1"), SimpleNamespace(node_id="n2")],
    )

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-hooks-defaults")  # noqa: SLF001

    assert mocked.call_args.kwargs["worker_count"] == 4
    assert mocked.call_args.kwargs["node_count"] == 2


def test_run_job_non_hook_uses_larger_default_worker_and_all_nodes(monkeypatch) -> None:
    from pycloud_parallel.controlplane.serialization import dict_to_struct
    from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-defaults-non-hook",
            "client_id": "client-a",
            "priority": 5,
            "code_version": "sha256:test",
            "entry_module": "task_demo",
            "entry_callable": "run",
            "subtasks": [{"value": 1}, {"value": 2}],
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        job_id = "job-defaults-non-hook"

        def submit_payloads(self, subtasks, **kwargs):
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[
                    pb2.TaskAccepted(task_id=f"t-{idx}", status=pb2.TASK_STATUS_QUEUED)
                    for idx, _ in enumerate(subtasks, start=1)
                ],
                rejected=[],
            )

        def iter_results(self, **kwargs):
            yield pb2.TaskResult(task_id="t-1", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1}))
            yield pb2.TaskResult(task_id="t-2", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 2}))

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    monkeypatch.setattr("pycloud_parallel.controlplane.job_queue.os.cpu_count", lambda: 6)
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.job_queue.InfoCenterClient.select_task_nodes",
        lambda self, **kwargs: [SimpleNamespace(node_id="n1"), SimpleNamespace(node_id="n2"), SimpleNamespace(node_id="n3")],
    )

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-defaults-non-hook")  # noqa: SLF001

    assert mocked.call_args.kwargs["worker_count"] == 3
    assert mocked.call_args.kwargs["node_count"] == 3


def test_job_queue_client_wait_for_terminal_polls_until_done() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051")
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
