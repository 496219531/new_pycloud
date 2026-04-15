from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pycloud_parallel.controlplane.job_queue import JobQueueManager
from pycloud_parallel.controlplane.object_ref import ObjectRef, object_ref_to_payload


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


def test_submit_job_localizes_object_ref_payloads(monkeypatch) -> None:
    queue = JobQueueManager()

    def _fake_resolve(payload, *, object_dir):
        assert object_dir
        payload = dict(payload)
        payload["subtasks"] = [{"blob": {"x": 1}}]
        return payload

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.job_queue._resolve_object_refs_in_payload",
        _fake_resolve,
    )

    job = queue.submit_job(
        {
            "job_id": "job-ref-1",
            "client_id": "client-a",
            "entry_module": "task_demo",
            "subtasks": [
                {
                    "blob": object_ref_to_payload(
                        ObjectRef(
                            object_id="sha256:" + ("c" * 64),
                            format="json",
                            size_bytes=12,
                            materialize_as="json",
                        )
                    )
                }
            ],
        }
    )

    payload_item = job.payload["subtasks"][0]["blob"]
    assert payload_item == {"x": 1}


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
    with patch("pycloud_parallel.controlplane.job_queue.TaskPoolSession.from_infocenter", return_value=fake_pool) as mocked:
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
    with patch("pycloud_parallel.controlplane.job_queue.TaskPoolSession.from_infocenter", return_value=fake_pool) as mocked:
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
    with patch("pycloud_parallel.controlplane.job_queue.TaskPoolSession.from_infocenter", return_value=fake_pool) as mocked:
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
    with patch("pycloud_parallel.controlplane.job_queue.TaskPoolSession.from_infocenter", return_value=fake_pool) as mocked:
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
    with patch("pycloud_parallel.controlplane.job_queue.TaskPoolSession.from_infocenter", return_value=fake_pool):
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
    with patch("pycloud_parallel.controlplane.job_queue.TaskPoolSession.from_infocenter", return_value=fake_pool) as mocked:
        queue._run_job("job-hooks-globals-dict")  # noqa: SLF001

    assert fake_pool.updated_globals == [{"cfg": {"mode": "fast"}}]
    assert mocked.call_args.kwargs["managed_global_names"] == ["cfg"]
    job = queue.get_job("job-hooks-globals-dict")
    assert job is not None
    assert job.status == "SUCCEEDED"


def test_run_job_with_hooks_accepts_blob_ref_payload() -> None:
    from pycloud_parallel.controlplane.object_ref import ObjectRef, object_ref_to_payload

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
    with patch(
        "pycloud_parallel.controlplane.job_queue._resolve_object_refs_in_payload",
        return_value={
            "job_id": "job-hooks-ref-1",
            "client_id": "client-a",
            "priority": 5,
            "blob_ref": module_blob,
            "blob_control_addr": "127.0.0.1:50061",
            "entry_module": "job_hooks_ref_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "handle_result_callable": "handle_result",
            "finalize_callable": "finalize",
            "job_payload": {"value": 2, "count": 3},
        },
    ):
        state = queue.submit_job(  # noqa: SLF001
            {
                "job_id": "job-hooks-ref-1",
                "client_id": "client-a",
                "priority": 5,
                "blob_ref": object_ref_to_payload(
                    ObjectRef(
                        object_id="sha256:" + ("b" * 64),
                        format="py",
                        size_bytes=len(module_blob),
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
    with patch("pycloud_parallel.controlplane.job_queue.TaskPoolSession.from_infocenter", return_value=fake_pool):
        queue._run_job("job-hooks-ref-1")  # noqa: SLF001

    job = queue.get_job("job-hooks-ref-1")
    assert job is not None
    assert job.status == "SUCCEEDED"
    assert job.final_result == {"count": 3}


def test_job_queue_manager_submit_job_rejects_data_ref_in_job_payload() -> None:
    from pycloud_parallel.controlplane.client import DataRef

    queue = JobQueueManager()

    with pytest.raises(ValueError, match="must not contain ObjectRef/DataRef/ResultRef"):
        queue.submit_job(
            {
                "job_id": "job-invalid-job-payload-ref",
                "client_id": "client-a",
                "entry_module": "job_demo",
                "job_mode": "hooks",
                "blob_b64": base64.b64encode(
                    b"def run(**_kwargs):\n    return {}\n\ndef task_generator(**_kwargs):\n    return []\n"
                ).decode("utf-8"),
                "package_format": "py",
                "job_payload": {
                    "cfg": DataRef(
                        ref_id="sha256:" + ("b" * 64),
                        storage_id="sha256:" + ("b" * 64),
                        format="bin",
                        size_bytes=16,
                    )
                },
            }
        )


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
        patch("pycloud_parallel.controlplane.job_queue.TaskPoolSession.from_infocenter", return_value=fake_pool),
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
    from pycloud_parallel.controlplane.client import JobQueueClient

    client = JobQueueClient("127.0.0.1:50051", client_id="client-a")
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


def test_job_queue_client_submit_job_from_bytes_auto_binds_update_globals() -> None:
    from pycloud_parallel.controlplane.client import JobQueueClient

    client = JobQueueClient("127.0.0.1:50051", client_id="client-a")
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
    from pycloud_parallel.controlplane.client import JobQueueClient

    client = JobQueueClient("127.0.0.1:50051", client_id="client-a")
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
    from pycloud_parallel.controlplane.client import JobQueueClient

    client = JobQueueClient("127.0.0.1:50051", client_id="client-a")
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
    from pycloud_parallel.controlplane.client import _prepare_job_blob_submit_fields
    from pycloud_parallel.controlplane.object_ref import ObjectRef

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.client._job_blob_requires_object_ref",
        lambda blob: True,
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.client.InfoCenterClient.select_task_nodes",
        lambda self, **kwargs: [SimpleNamespace(control_addr="127.0.0.1:50061")],
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.client.NodeControlClient.upload_object_from_bytes",
        lambda self, **kwargs: ObjectRef(
            object_id="sha256:" + ("a" * 64),
            format="tar.gz",
            size_bytes=4096,
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


def test_job_queue_client_submit_job_reuses_update_globals_objectify(monkeypatch) -> None:
    from pycloud_parallel.controlplane.client import JobQueueClient

    client = JobQueueClient("127.0.0.1:50051", client_id="client-a")
    captured = {}

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.client._prepare_job_submit_payload_for_call",
        lambda **kwargs: {
            **dict(kwargs["payload"]),
            "update_globals": {"cfg": "prepared-object-ref"},
        },
    )

    def _fake_call(*, service_name, method, payload=None, timeout_sec=60.0, service_token=None):
        captured["service_name"] = service_name
        captured["method"] = method
        captured["payload"] = dict(payload or {})
        captured["timeout_sec"] = timeout_sec
        captured["service_token"] = service_token
        return {"ok": True, "job": {"job_id": "job-1", "status": "WAITING"}}

    client._service_client.call = _fake_call  # type: ignore[method-assign]
    resp = client.submit_job(
        {
            "entry_module": "job_demo",
            "update_globals": {"cfg": {"big": "value"}},
        }
    )

    assert resp["job"]["job_id"] == "job-1"
    assert captured["payload"]["update_globals"] == {"cfg": "prepared-object-ref"}


def test_job_queue_client_submit_job_from_bytes_rejects_data_ref_in_job_payload() -> None:
    from pycloud_parallel.controlplane.client import DataRef, JobQueueClient

    client = JobQueueClient("127.0.0.1:50051", client_id="client-a")

    with pytest.raises(ValueError, match="must not contain ObjectRef/DataRef/ResultRef"):
        client.submit_job_from_bytes(
            blob=b"def run(**_kwargs):\n    return {}\n\ndef task_generator(**_kwargs):\n    return []\n",
            entry_module="job_demo",
            job_payload={
                "cfg": DataRef(
                    ref_id="sha256:" + ("a" * 64),
                    storage_id="sha256:" + ("a" * 64),
                    format="bin",
                    size_bytes=12,
                )
            },
        )


def test_job_queue_client_submit_job_from_bytes_rejects_oversized_job_payload(monkeypatch) -> None:
    from pycloud_parallel.controlplane.client import JobQueueClient
    import pycloud_parallel.controlplane.client as client_mod

    monkeypatch.setattr(client_mod, "JOB_PAYLOAD_MAX_BYTES", 32)
    client = JobQueueClient("127.0.0.1:50051", client_id="client-a")

    with pytest.raises(ValueError, match="exceeds the job payload limit"):
        client.submit_job_from_bytes(
            blob=b"def run(**_kwargs):\n    return {}\n\ndef task_generator(**_kwargs):\n    return []\n",
            entry_module="job_demo",
            job_payload={"text": "x" * 256},
        )


def test_job_queue_client_submit_job_uses_unified_outbound_policy(monkeypatch) -> None:
    from pycloud_parallel.controlplane.client import JobQueueClient

    client = JobQueueClient("127.0.0.1:50051", client_id="client-a")
    captured = {}

    class _FakeUploadClient:
        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.client._job_submit_upload_clients",
        lambda **kwargs: [_FakeUploadClient()],
    )

    def _fake_prepare(payload, *, put_data, estimate_inline_size, policy):
        del put_data, estimate_inline_size
        captured["payload"] = dict(payload or {})
        captured["mode"] = policy.mode
        captured["managed_global_field_names"] = policy.managed_global_field_names
        return dict(payload or {})

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.client.prepare_outbound_payload",
        _fake_prepare,
    )

    client._service_client.call = lambda **kwargs: {"ok": True, "job": {"job_id": "job-1", "status": "WAITING"}}  # type: ignore[method-assign]
    resp = client.submit_job({"entry_module": "job_demo", "update_globals": {"cfg": {"k": "v"}}})

    assert resp["job"]["job_id"] == "job-1"
    assert captured["mode"] == "job_submit"
    assert captured["managed_global_field_names"] == ("update_globals",)


def test_job_queue_client_restores_cached_session(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("PYCLOUD_JOB_CLIENT_SESSION_DIR", str(tmp_path))

    from pycloud_parallel.controlplane.client import JobQueueClient

    first = JobQueueClient("127.0.0.1:50051")
    second = JobQueueClient("127.0.0.1:50051")

    assert first.client_id == second.client_id
    assert first.auth_token == second.auth_token


def test_job_queue_client_rotates_expired_cached_session(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("PYCLOUD_JOB_CLIENT_SESSION_DIR", str(tmp_path))

    from pycloud_parallel.controlplane.client import JobQueueClient, _job_client_session_cache_file

    first = JobQueueClient("127.0.0.1:50051", client_id="client-cache")
    cache_path = _job_client_session_cache_file(
        target="127.0.0.1:50051",
        service_name="job-orchestrator",
        client_scope="client-cache",
    )
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    payload["expires_at"] = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    cache_path.write_text(json.dumps(payload), encoding="utf-8")

    second = JobQueueClient("127.0.0.1:50051", client_id="client-cache")

    assert second.client_id == "client-cache"
    assert second.auth_token != first.auth_token


def test_job_queue_client_recent_job_ids_tracks_and_restores(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("PYCLOUD_JOB_CLIENT_SESSION_DIR", str(tmp_path))

    from pycloud_parallel.controlplane.client import JobQueueClient

    client = JobQueueClient("127.0.0.1:50051", client_id="client-recent")
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

    restored = JobQueueClient("127.0.0.1:50051", client_id="client-recent")
    assert restored.recent_job_ids() == ["job-1", "job-2"]


def test_job_queue_client_discovers_job_orchestrator_via_infocenter(monkeypatch) -> None:
    from pycloud_parallel.controlplane.client import InfoCenterServiceRoute, JobQueueClient
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
        "pycloud_parallel.controlplane.client.InfoCenterClient.list_service_routes",
        _fake_list_service_routes,
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.client._call_route_http",
        _fake_call_route_http,
    )

    client = JobQueueClient("127.0.0.1:50051", client_id="client-discovery", auth_token="token-discovery")
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
    from pycloud_parallel.controlplane.client import JobQueueClient

    client = JobQueueClient("127.0.0.1:50051", client_id="client-module")
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

    with patch("pycloud_parallel.controlplane.client._prepare_code_blob", return_value=(module_blob, f"{module_name}.tar.gz")):
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
    from pycloud_parallel.controlplane.client import JobQueueClient

    client = JobQueueClient("127.0.0.1:50051", client_id="client-module")
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
        "pycloud_parallel.controlplane.client._prepare_code_blob",
        return_value=(b"blob", "job_module_demo_with_globals.tar.gz"),
    ):
        resp = client.submit_job_from_module(module=module)
    assert resp == {"ok": True}
    assert captured["update_globals"] == "update_globals"


def test_job_queue_client_submit_job_from_module_auto_binds_handle_data_alias() -> None:
    from pycloud_parallel.controlplane.client import JobQueueClient

    client = JobQueueClient("127.0.0.1:50051", client_id="client-module")
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
        "pycloud_parallel.controlplane.client._prepare_code_blob",
        return_value=(b"blob", "job_module_demo_with_handle_data.tar.gz"),
    ):
        resp = client.submit_job_from_module(module=module)
    assert resp == {"ok": True}
    assert captured["handle_result_callable"] == "handle_data"
    assert "finalize_callable" not in captured


def test_job_queue_client_submit_job_from_module_auto_binds_finalize_when_present() -> None:
    from pycloud_parallel.controlplane.client import JobQueueClient

    client = JobQueueClient("127.0.0.1:50051", client_id="client-module")
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
        "pycloud_parallel.controlplane.client._prepare_code_blob",
        return_value=(b"blob", "job_module_demo_with_finalize.tar.gz"),
    ):
        resp = client.submit_job_from_module(module=module)
    assert resp == {"ok": True}
    assert captured["finalize_callable"] == "finalize"


def test_job_queue_client_submit_job_from_module_accepts_update_globals_dict() -> None:
    from pycloud_parallel.controlplane.client import JobQueueClient

    client = JobQueueClient("127.0.0.1:50051", client_id="client-module")
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
        "pycloud_parallel.controlplane.client._prepare_code_blob",
        return_value=(b"blob", "job_module_demo_with_explicit_globals.tar.gz"),
    ):
        resp = client.submit_job_from_module(module=module, update_globals={"cfg": {"mode": "manual"}})
    assert resp == {"ok": True}
    assert captured["update_globals"] == {"cfg": {"mode": "manual"}}


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
