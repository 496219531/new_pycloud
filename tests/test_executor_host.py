from __future__ import annotations

import hashlib
import inspect
import os
import time

import pytest

from pycloud_parallel.controlplane.executor_backend import (
    SubprocessExecutorBackend,
    create_executor_backend,
)
from pycloud_parallel.controlplane import executor_core as executor_core_mod
from pycloud_parallel.controlplane.executor_core import ExecutorCore
from pycloud_parallel.controlplane.executor_host import ExecutorHostClient
from pycloud_parallel.controlplane.node.execution import _build_execute_spec
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState


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


def _wait_for_backend_event(backend, kind: str, *, timeout_sec: float = 8.0):
    deadline = time.monotonic() + max(0.1, float(timeout_sec))
    while time.monotonic() < deadline:
        for item in backend.drain_events():
            if item.get("kind") == kind:
                return item
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for backend event: {kind}")


class _NeverDoneFuture:
    def __init__(self):
        self.cancelled = False

    def add_done_callback(self, _callback):
        return None

    def cancel(self):
        self.cancelled = True
        return True


def test_executor_core_stop_service_fails_inflight_call_and_stream():
    responses = []
    core = ExecutorCore(
        task_worker_capacity=1,
        emit_response=lambda item: responses.append(dict(item)),
        emit_event=lambda _item: None,
    )
    call_future = _NeverDoneFuture()
    stream_future = _NeverDoneFuture()
    stream_manager = type("_Manager", (), {"shutdown": lambda self: None})()
    core._service_workers["svc-stop"] = 1  # noqa: SLF001
    core._service_executors["svc-stop"] = None  # noqa: SLF001
    core._track_inflight(  # noqa: SLF001
        call_future,
        {
            "kind": "service",
            "service_id": "svc-stop",
            "request_id": "req-call",
            "streaming": False,
        },
    )
    core._track_inflight(  # noqa: SLF001
        stream_future,
        {
            "kind": "service",
            "service_id": "svc-stop",
            "request_id": "req-stream",
            "streaming": True,
            "stream_manager": stream_manager,
        },
    )

    core.handle_request(
        "req-stop",
        "stop_service",
        {"service_id": "svc-stop", "reason": "owner heartbeat timeout"},
    )

    by_request = {item["request_id"]: item for item in responses}
    assert by_request["req-call"]["status_text"] == "FAILED_INFRA"
    assert by_request["req-call"]["err_message"] == "owner heartbeat timeout"
    assert by_request["req-stream"]["status_text"] == "FAILED_INFRA"
    assert by_request["req-stream"]["err_message"] == "owner heartbeat timeout"
    assert by_request["req-stop"]["ok"] is True
    assert by_request["req-stop"]["failed_inflight"] == 2
    assert call_future.cancelled is True
    assert stream_future.cancelled is True
    assert core._inflight == {}  # noqa: SLF001
    assert core._stream_state == {}  # noqa: SLF001


def test_create_executor_backend_defaults_to_subprocess_host():
    backend = create_executor_backend(task_worker_capacity=1)
    try:
        assert backend.backend_name == "subprocess_host"
    finally:
        backend.close()


def test_subprocess_backend_stop_service_accepts_reason_and_reports_liveness():
    calls = []

    class _FakeClient:
        def __init__(self, liveness):
            self._liveness = dict(liveness)

        def is_alive(self):
            return True

        def stop_service(self, **kwargs):
            calls.append(("stop_service", kwargs))

        def service_worker_liveness(self):
            return dict(self._liveness)

        def close(self, **kwargs):
            calls.append(("close", kwargs))

    backend = SubprocessExecutorBackend(task_worker_capacity=1)
    backend._service_clients["svc-a"] = _FakeClient({"svc-a": 2})  # noqa: SLF001
    backend._service_clients["svc-b"] = _FakeClient({"svc-b": 0})  # noqa: SLF001

    assert backend.service_worker_liveness() == {"svc-a": 2, "svc-b": 0}
    backend.stop_service(service_id="svc-a", reason="owner heartbeat timeout")

    assert ("stop_service", {"service_id": "svc-a", "reason": "owner heartbeat timeout"}) in calls
    assert any(name == "close" for name, _kwargs in calls)
    assert "svc-a" not in backend._service_clients  # noqa: SLF001
    assert backend.service_worker_liveness() == {"svc-b": 0}


def test_executor_backend_interface_exposes_service_stop_reason_and_liveness():
    stop_signature = inspect.signature(SubprocessExecutorBackend.stop_service)
    assert "reason" in stop_signature.parameters
    assert stop_signature.parameters["reason"].default == ""
    assert hasattr(SubprocessExecutorBackend, "service_worker_liveness")


def test_create_executor_backend_rejects_embedded():
    with pytest.raises(ValueError, match="subprocess_host"):
        create_executor_backend(executor_backend="embedded", task_worker_capacity=1)


def test_create_executor_backend_rejects_old_aliases():
    for value in ("host", "executor_host", "subprocess", "subprocesshost"):
        with pytest.raises(ValueError, match="subprocess_host"):
            create_executor_backend(executor_backend=value, task_worker_capacity=1)


def test_executor_host_process_died_message_includes_action_pid_exitcode(monkeypatch):
    class _FakeProcess:
        pid = 12345
        exitcode = 1

        @staticmethod
        def is_alive():
            return False

    client = object.__new__(ExecutorHostClient)
    client._process = _FakeProcess()  # noqa: SLF001
    message = client._format_process_died_message("create_service")  # noqa: SLF001
    assert "action=create_service" in message
    assert "pid=12345" in message
    assert "exitcode=1" in message
    if os.name == "nt":
        assert "if __name__ == '__main__'" in message


def test_subprocess_backend_uses_distinct_hosts_per_session(tmp_path):
    state, artifact = _seed_artifact(
        tmp_path,
        blob=b"def run(value=0, **_kwargs):\n    return {'value': int(value)}\n",
        entry_module="executor_backend_distinct_hosts",
    )
    backend = SubprocessExecutorBackend(task_worker_capacity=1)
    try:
        spec = _build_execute_spec(
            artifact,
            object_dir=state.object_dir,
            work_dir=str(tmp_path),
            method_name="run",
            payload={},
            payload_mode="task_submit",
            warmup_only=True,
        )
        assert backend.prepare_artifact(artifact_spec=spec, scope="pool", key="pool-a").get("ok") is True
        assert backend.prepare_artifact(artifact_spec=spec, scope="pool", key="pool-b").get("ok") is True
        assert set(backend._pool_clients) == {"pool-a", "pool-b"}  # noqa: SLF001
        assert backend._pool_clients["pool-a"] is not backend._pool_clients["pool-b"]  # noqa: SLF001
    finally:
        backend.close()
        state.close()


def test_subprocess_backend_reports_dead_session_host():
    backend = SubprocessExecutorBackend(task_worker_capacity=1)

    class _FakeClient:
        def __init__(self, alive):
            self._alive = alive

        def is_alive(self):
            return self._alive

    try:
        assert backend.is_alive()
        backend._pool_clients["pool-dead"] = _FakeClient(False)  # noqa: SLF001
        assert not backend.is_alive()
        with pytest.raises(RuntimeError, match="task pool executor host died"):
            backend.submit_pool_task(pool_id="pool-dead", task_id="task-1", attempt=1, execute_spec={})
    finally:
        backend._pool_clients.clear()  # noqa: SLF001
        backend.close()


def test_executor_core_defaults_to_fork_inside_host_on_posix(monkeypatch):
    monkeypatch.setenv("PYCLOUD_EXECUTOR_PARENT_KIND", "executor_host")
    monkeypatch.delenv("PYCLOUD_WORKER_START_METHOD", raising=False)
    monkeypatch.setattr(os, "name", "posix")
    monkeypatch.setattr(executor_core_mod.mp, "get_all_start_methods", lambda: ["fork", "spawn"])

    contexts = []

    class _FakeContext:
        def __init__(self, name):
            self.name = name

    monkeypatch.setattr(executor_core_mod.mp, "get_context", lambda name: contexts.append(name) or _FakeContext(name))

    assert ExecutorCore._ensure_mp_context().name == "fork"  # noqa: SLF001
    assert contexts == ["fork"]


def test_executor_core_defaults_to_spawn_on_windows(monkeypatch):
    monkeypatch.setenv("PYCLOUD_EXECUTOR_PARENT_KIND", "executor_host")
    monkeypatch.delenv("PYCLOUD_WORKER_START_METHOD", raising=False)
    monkeypatch.setattr(os, "name", "nt")
    monkeypatch.setattr(executor_core_mod.mp, "get_all_start_methods", lambda: ["spawn"])

    contexts = []

    class _FakeContext:
        def __init__(self, name):
            self.name = name

    monkeypatch.setattr(executor_core_mod.mp, "get_context", lambda name: contexts.append(name) or _FakeContext(name))

    assert ExecutorCore._ensure_mp_context().name == "spawn"  # noqa: SLF001
    assert contexts == ["spawn"]


@pytest.mark.parametrize("requested_start_method", ["", "fork"])
def test_executor_core_falls_back_to_spawn_when_fork_submit_fails(monkeypatch, requested_start_method):
    monkeypatch.setenv("PYCLOUD_EXECUTOR_PARENT_KIND", "executor_host")
    if requested_start_method:
        monkeypatch.setenv("PYCLOUD_WORKER_START_METHOD", requested_start_method)
    else:
        monkeypatch.delenv("PYCLOUD_WORKER_START_METHOD", raising=False)
    monkeypatch.setattr(os, "name", "posix")
    monkeypatch.setattr(executor_core_mod.mp, "get_all_start_methods", lambda: ["fork", "spawn"])

    contexts = []

    class _FakeContext:
        def __init__(self, name):
            self.name = name

    monkeypatch.setattr(executor_core_mod.mp, "get_context", lambda name: contexts.append(name) or _FakeContext(name))

    class _FakeExecutor:
        def __init__(self, max_workers, mp_context):
            self.max_workers = max_workers
            self.mp_context = mp_context

        def shutdown(self, **_kwargs):
            pass

    monkeypatch.setattr(executor_core_mod, "ProcessPoolExecutor", _FakeExecutor)
    calls = []

    class _FakeFuture:
        def add_done_callback(self, _callback):
            pass

    def _fake_submit(executor, _payload):
        calls.append(executor.mp_context.name)
        if executor.mp_context.name == "fork":
            raise RuntimeError("fork failed")
        return _FakeFuture()

    monkeypatch.setattr(executor_core_mod, "submit_callable_to_worker", _fake_submit)

    core = ExecutorCore(task_worker_capacity=1)
    try:
        assert core.handle_request("create", "create_task_pool", {"pool_id": "pool-fallback", "worker_count": 1})
        assert core.handle_request(
            "submit",
            "submit_pool_task",
            {"pool_id": "pool-fallback", "task_id": "task-1", "attempt": 1},
        )
        assert calls == ["fork", "spawn"]
        assert contexts == ["fork", "spawn"]
    finally:
        core.close()


def test_executor_core_background_submit_falls_back_to_spawn_when_fork_fails(monkeypatch):
    monkeypatch.setenv("PYCLOUD_EXECUTOR_PARENT_KIND", "executor_host")
    monkeypatch.setenv("PYCLOUD_WORKER_START_METHOD", "fork")
    monkeypatch.setattr(os, "name", "posix")
    monkeypatch.setattr(executor_core_mod.mp, "get_all_start_methods", lambda: ["fork", "spawn"])

    contexts = []

    class _FakeContext:
        def __init__(self, name):
            self.name = name

    monkeypatch.setattr(executor_core_mod.mp, "get_context", lambda name: contexts.append(name) or _FakeContext(name))

    class _FakeExecutor:
        def __init__(self, max_workers, mp_context):
            self.max_workers = max_workers
            self.mp_context = mp_context

        def shutdown(self, **_kwargs):
            pass

    monkeypatch.setattr(executor_core_mod, "ProcessPoolExecutor", _FakeExecutor)
    calls = []

    class _FakeFuture:
        def add_done_callback(self, callback):
            self.callback = callback

    def _fake_submit(executor, _payload):
        calls.append(executor.mp_context.name)
        if executor.mp_context.name == "fork":
            raise RuntimeError("fork failed")
        return _FakeFuture()

    monkeypatch.setattr(executor_core_mod, "submit_callable_to_worker", _fake_submit)

    core = ExecutorCore(task_worker_capacity=1)
    try:
        assert core.handle_request("create", "create_task_pool", {"pool_id": "pool-warmup-fallback", "worker_count": 1})
        assert core.handle_request(
            "warmup",
            "warmup_pool",
            {"pool_id": "pool-warmup-fallback", "fanout": 1},
        )
        assert calls == ["fork", "spawn"]
        assert contexts == ["fork", "spawn"]
    finally:
        core.close()


@pytest.mark.parametrize("backend_cls", [SubprocessExecutorBackend])
def test_executor_backend_service_call_roundtrip(tmp_path, backend_cls):
    state, artifact = _seed_artifact(
        tmp_path,
        blob=(
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        ),
        entry_module=f"executor_backend_service_{backend_cls.backend_name}",
    )
    backend = backend_cls(task_worker_capacity=1)
    try:
        backend.create_service(service_id=f"svc-{backend_cls.backend_name}", worker_count=1)
        resp = backend.call_service(
            service_id=f"svc-{backend_cls.backend_name}",
            timeout_sec=5.0,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={"value": 6},
            ),
        )
        assert resp["ok"] is True
        assert resp["status_text"] == "SUCCEEDED"
        assert resp["result"] == {"value": 6, "square": 36}
    finally:
        backend.close()
        state.close()


@pytest.mark.parametrize("backend_cls", [SubprocessExecutorBackend])
def test_executor_backend_warmup_and_preload_paths_keep_backend_usable(tmp_path, backend_cls):
    state, artifact = _seed_artifact(
        tmp_path,
        blob=(
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'ok': True}\n"
        ),
        entry_module=f"executor_backend_warmup_{backend_cls.backend_name}",
    )
    backend = backend_cls(task_worker_capacity=1)
    try:
        backend.create_service(service_id=f"svc-warm-{backend_cls.backend_name}", worker_count=1)
        assert backend.preload_service(
            service_id=f"svc-warm-{backend_cls.backend_name}",
            fanout=1,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={},
                warmup_only=True,
            ),
        ) == 1
        assert backend.warmup_service(
            service_id=f"svc-warm-{backend_cls.backend_name}",
            fanout=1,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={},
                warmup_only=True,
            ),
        ) == 1

        backend.create_task_pool(pool_id=f"pool-warm-{backend_cls.backend_name}", worker_count=1)
        assert backend.preload_pool(
            pool_id=f"pool-warm-{backend_cls.backend_name}",
            fanout=1,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={},
                warmup_only=True,
            ),
        ) == 1
        assert backend.warmup_pool(
            pool_id=f"pool-warm-{backend_cls.backend_name}",
            fanout=1,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={},
                warmup_only=True,
            ),
        ) == 1

        resp = backend.call_service(
            service_id=f"svc-warm-{backend_cls.backend_name}",
            timeout_sec=5.0,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={"value": 2},
            ),
        )
        assert resp["ok"] is True
        assert resp["status_text"] == "SUCCEEDED"
        assert resp["result"] == {"value": 2, "ok": True}
    finally:
        backend.close()
        state.close()


@pytest.mark.parametrize("backend_cls", [SubprocessExecutorBackend])
def test_executor_backend_task_pool_submit_emits_done_event(tmp_path, backend_cls):
    state, artifact = _seed_artifact(
        tmp_path,
        blob=(
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'plus_one': value + 1}\n"
        ),
        entry_module=f"executor_backend_pool_{backend_cls.backend_name}",
    )
    backend = backend_cls(task_worker_capacity=1)
    try:
        backend.create_task_pool(pool_id=f"pool-{backend_cls.backend_name}", worker_count=1)
        backend.submit_pool_task(
            pool_id=f"pool-{backend_cls.backend_name}",
            task_id=f"task-{backend_cls.backend_name}",
            attempt=1,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={"value": 10},
            ),
        )
        deadline = time.monotonic() + 8.0
        done_event = None
        while time.monotonic() < deadline:
            for item in backend.drain_events():
                if item.get("kind") == "pool_task_done":
                    done_event = item
                    break
            if done_event is not None:
                break
            time.sleep(0.05)
        assert done_event is not None
        assert done_event["status_text"] == "SUCCEEDED"
        assert done_event["result"] == {"value": 10, "plus_one": 11}
    finally:
        backend.close()
        state.close()


@pytest.mark.parametrize("backend_cls", [SubprocessExecutorBackend])
def test_executor_backend_service_timeout_recycles_executor(tmp_path, backend_cls):
    state, artifact = _seed_artifact(
        tmp_path,
        blob=(
            b"import time\n"
            b"def run(value=0, sleep_ms=0, **_kwargs):\n"
            b"    time.sleep(max(0, int(sleep_ms)) / 1000.0)\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        ),
        entry_module=f"executor_backend_timeout_{backend_cls.backend_name}",
    )
    backend = backend_cls(task_worker_capacity=1)
    try:
        service_id = f"svc-timeout-{backend_cls.backend_name}"
        backend.create_service(service_id=service_id, worker_count=1)
        resp = backend.call_service(
            service_id=service_id,
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

        resp = backend.call_service(
            service_id=service_id,
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
    finally:
        backend.close()
        state.close()


@pytest.mark.parametrize("backend_cls", [SubprocessExecutorBackend])
def test_executor_backend_close_cleans_active_runtime_worker(tmp_path, backend_cls):
    state, artifact = _seed_artifact(
        tmp_path,
        blob=(
            b"import time\n"
            b"def run(value=0, sleep_ms=0, **_kwargs):\n"
            b"    time.sleep(max(0, int(sleep_ms)) / 1000.0)\n"
            b"    return {'value': int(value)}\n"
        ),
        entry_module=f"executor_backend_close_{backend_cls.backend_name}",
    )
    backend = backend_cls(task_worker_capacity=1)
    try:
        backend.submit_runtime_task(
            runtime_key=f"rt-close-{backend_cls.backend_name}",
            task_id=f"task-close-{backend_cls.backend_name}",
            attempt=1,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={"value": 3, "sleep_ms": 5000},
            ),
        )
        started = time.monotonic()
        backend.close()
        assert time.monotonic() - started < 20.0
        assert backend.is_alive() is False
    finally:
        backend.close()
        state.close()


@pytest.mark.parametrize("backend_cls", [SubprocessExecutorBackend])
def test_executor_backend_pool_recovers_after_broken_worker(tmp_path, backend_cls):
    state, artifact = _seed_artifact(
        tmp_path,
        blob=(
            b"import os\n"
            b"def run(value=0, crash=False, **_kwargs):\n"
            b"    if crash:\n"
            b"        os._exit(7)\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        ),
        entry_module=f"executor_backend_rebuild_{backend_cls.backend_name}",
    )
    backend = backend_cls(task_worker_capacity=1)
    try:
        pool_id = f"pool-rebuild-{backend_cls.backend_name}"
        backend.create_task_pool(pool_id=pool_id, worker_count=1)
        backend.submit_pool_task(
            pool_id=pool_id,
            task_id=f"task-crash-{backend_cls.backend_name}",
            attempt=1,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={"value": 2, "crash": True},
            ),
        )
        rebuilt = _wait_for_backend_event(backend, "pool_executor_rebuilt", timeout_sec=10.0)
        assert rebuilt["pool_id"] == pool_id

        failed = _wait_for_backend_event(backend, "pool_task_done", timeout_sec=10.0)
        assert failed["task_id"] == f"task-crash-{backend_cls.backend_name}"
        assert failed["status_text"] == "FAILED_INFRA"

        backend.submit_pool_task(
            pool_id=pool_id,
            task_id=f"task-after-rebuild-{backend_cls.backend_name}",
            attempt=1,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=state.object_dir,
                method_name="run",
                payload={"value": 5, "crash": False},
            ),
        )
        done = _wait_for_backend_event(backend, "pool_task_done", timeout_sec=10.0)
        assert done["task_id"] == f"task-after-rebuild-{backend_cls.backend_name}"
        assert done["status_text"] == "SUCCEEDED"
        assert done["result"] == {"value": 5, "square": 25}
    finally:
        backend.close()
        state.close()


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
        assert float((resp.get("timings") or {}).get("invoke_wrapper_ms", 0.0) or 0.0) >= 0.0
        assert float((resp.get("timings") or {}).get("user_fn_ms", 0.0) or 0.0) >= 0.0
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


def test_executor_host_worker_pid_tracking_replaces_scope_key_set(monkeypatch):
    host = object.__new__(ExecutorHostClient)
    host._reader_stop = type("_NeverSet", (), {"is_set": lambda self: False})()  # noqa: SLF001
    host._event_q = object()  # noqa: SLF001
    host._worker_pids = set()  # noqa: SLF001
    host._worker_pid_sets = {}  # noqa: SLF001
    host._async_events = []  # noqa: SLF001
    host._responses = {}  # noqa: SLF001
    host._stream_events = {}  # noqa: SLF001
    host._expired_requests = set()  # noqa: SLF001
    host._cv = type(
        "_NoopCondition",
        (),
        {
            "__enter__": lambda self: self,
            "__exit__": lambda self, *_args: False,
            "notify_all": lambda self: None,
        },
    )()  # noqa: SLF001
    events = iter(
        (
            {"kind": "executor_worker_pids", "scope": "service", "key": "svc", "worker_pids": [101, 102]},
            {"kind": "executor_worker_pids", "scope": "service", "key": "svc", "worker_pids": [103]},
        )
    )

    def _next_event(_queue, *, timeout=0.0):  # noqa: ARG001
        try:
            return next(events)
        except StopIteration:
            host._reader_stop.is_set = lambda: True  # noqa: SLF001
            return None

    monkeypatch.setattr("pycloud_parallel.controlplane.executor_host._simple_queue_get_if_ready", _next_event)

    host._reader_loop()  # noqa: SLF001

    assert host._worker_pids == {103}  # noqa: SLF001
    assert host._worker_pid_sets == {"service:svc": {103}}  # noqa: SLF001


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
