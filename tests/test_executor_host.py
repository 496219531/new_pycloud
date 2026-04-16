from __future__ import annotations

import hashlib
import time

from pycloud_parallel.controlplane.executor_host import ExecutorHostClient
from pycloud_parallel.controlplane.node.execution import _build_execute_spec
from pycloud_parallel.controlplane.node.state import NodeControlState


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
        assert float((resp.get("timings") or {}).get("decode_ms", 0.0) or 0.0) >= 0.0
        assert float((resp.get("timings") or {}).get("invoke_ms", 0.0) or 0.0) >= 0.0
        assert float((resp.get("timings") or {}).get("encode_ms", 0.0) or 0.0) >= 0.0
        host.stop_service(service_id="svc-host-roundtrip")
    finally:
        host.close()
        state.close()


def test_executor_host_runtime_task_emits_done_event(tmp_path):
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
    finally:
        host.close()
        state.close()


def test_executor_host_warmup_pool_does_not_kill_host_process(tmp_path):
    state, artifact = _seed_artifact(
        tmp_path,
        blob=(
            b"def run(**_kwargs):\n"
            b"    return {'ok': True}\n"
        ),
        entry_module="executor_host_pool_warmup",
    )
    host = ExecutorHostClient()
    try:
        host.create_task_pool(pool_id="pool-host-warmup", worker_count=2)
        submitted = host.warmup_pool(
            pool_id="pool-host-warmup",
            fanout=4,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={},
                warmup_only=True,
            ),
        )
        assert submitted == 4

        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if not host.drain_events():
                break
            time.sleep(0.05)

        host.submit_pool_task(
            pool_id="pool-host-warmup",
            task_id="pool-task-1",
            attempt=1,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={},
            ),
        )

        deadline = time.monotonic() + 8.0
        done_event = None
        while time.monotonic() < deadline:
            for item in host.drain_events():
                if item.get("kind") == "pool_task_done":
                    done_event = item
                    break
            if done_event is not None:
                break
            time.sleep(0.05)

        assert done_event is not None
        assert done_event["pool_id"] == "pool-host-warmup"
        assert done_event["task_id"] == "pool-task-1"
        assert done_event["status_text"] == "SUCCEEDED"
        host.stop_task_pool(pool_id="pool-host-warmup")
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


def test_executor_host_recycles_service_executor_after_timeout(tmp_path):
    state, artifact = _seed_artifact(
        tmp_path,
        blob=(
            b"import time\n"
            b"def run(value=0, sleep_ms=0, **_kwargs):\n"
            b"    time.sleep(max(0, int(sleep_ms)) / 1000.0)\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        ),
        entry_module="executor_host_service_timeout_recycle",
    )
    host = ExecutorHostClient()
    try:
        host.create_service(service_id="svc-host-timeout", worker_count=1)
        resp = host.call_service(
            service_id="svc-host-timeout",
            timeout_sec=0.2,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={"value": 2, "sleep_ms": 3000},
            ),
        )
        assert resp["ok"] is False
        assert resp["timeout"] is True

        resp = host.call_service(
            service_id="svc-host-timeout",
            timeout_sec=5.0,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={"value": 4, "sleep_ms": 0},
            ),
        )
        assert resp["ok"] is True
        assert resp["status_text"] == "SUCCEEDED"
        assert resp["result"] == {"value": 4, "square": 16}
        host.stop_service(service_id="svc-host-timeout")
    finally:
        host.close()
        state.close()
