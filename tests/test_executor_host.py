from __future__ import annotations

import hashlib
import time

from pycloud_parallel.controlplane.executor_host import ExecutorHostClient
from pycloud_parallel.controlplane.state import NodeControlState, _build_execute_spec


def _seed_artifact(tmp_path, *, blob: bytes, entry_module: str, entry_callable: str = "run"):
    state = NodeControlState(
        node_id=f"seed-{entry_module}",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / f"artifact_{entry_module}"),
        enable_internal_executor=False,
        enable_service_session=False,
        monitor_interval_sec=1,
    )
    try:
        digest = hashlib.sha256(blob).hexdigest()
        artifact, _ = state.put_code(
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format="py",
            chunks=[blob],
        )
        return state, artifact
    except Exception:
        state.close()
        raise


def _wait_for_runtime_event(host: ExecutorHostClient, *, timeout_sec: float = 8.0):
    deadline = time.monotonic() + max(0.1, float(timeout_sec))
    while time.monotonic() < deadline:
        for item in host.drain_events():
            if item.get("kind") == "runtime_task_done":
                return item
        time.sleep(0.05)
    raise AssertionError("timed out waiting for runtime_task_done event")


def test_executor_host_service_call_roundtrip(tmp_path):
    state, artifact = _seed_artifact(
        tmp_path,
        blob=(
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        ),
        entry_module="executor_host_service_roundtrip",
    )
    host = ExecutorHostClient()
    try:
        host.create_service(service_id="svc-host-roundtrip", worker_count=1)
        resp = host.call_service(
            service_id="svc-host-roundtrip",
            timeout_sec=5.0,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={"value": 7},
            ),
        )
        assert resp["ok"] is True
        assert resp["status_text"] == "SUCCEEDED"
        assert resp["result"] == {"value": 7, "square": 49}
        host.stop_service(service_id="svc-host-roundtrip")
    finally:
        host.close()
        state.close()


def test_executor_host_runtime_slot_emits_done_event(tmp_path):
    state, artifact = _seed_artifact(
        tmp_path,
        blob=(
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'plus_one': value + 1}\n"
        ),
        entry_module="executor_host_runtime_event",
    )
    host = ExecutorHostClient()
    try:
        host.start_runtime_slot(runtime_key="rt-host-event")
        host.submit_runtime_task(
            runtime_key="rt-host-event",
            task_id="task-host-1",
            attempt=1,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={"value": 9},
            ),
        )
        event = _wait_for_runtime_event(host)
        assert event["runtime_key"] == "rt-host-event"
        assert event["task_id"] == "task-host-1"
        assert event["attempt"] == 1
        assert event["status_text"] == "SUCCEEDED"
        assert event["result"] == {"value": 9, "plus_one": 10}
        host.stop_runtime_slot(runtime_key="rt-host-event")
    finally:
        host.close()
        state.close()


def test_executor_host_close_cleans_active_runtime_worker(tmp_path):
    state, artifact = _seed_artifact(
        tmp_path,
        blob=(
            b"import time\n"
            b"def run(value=0, sleep_ms=0, **_kwargs):\n"
            b"    time.sleep(max(0, int(sleep_ms)) / 1000.0)\n"
            b"    value = int(value)\n"
            b"    return {'value': value}\n"
        ),
        entry_module="executor_host_close_cleanup",
    )
    host = ExecutorHostClient()
    try:
        host.start_runtime_slot(runtime_key="rt-host-close")
        host.submit_runtime_task(
            runtime_key="rt-host-close",
            task_id="task-host-close-1",
            attempt=1,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={"value": 3, "sleep_ms": 5000},
            ),
        )
        started = time.monotonic()
        host.close()
        elapsed = time.monotonic() - started
        assert elapsed < 20.0
        assert host._process.is_alive() is False  # noqa: SLF001
    finally:
        state.close()
