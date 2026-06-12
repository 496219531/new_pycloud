from __future__ import annotations

import inspect
import time
from pathlib import Path
from types import SimpleNamespace

from pycloud_parallel.controlplane.node_control_http import HttpNodeControlClient
from pycloud_parallel.data.ref import DataRef
from pycloud_parallel.execution import support
from pycloud_parallel.execution.service_session import Service
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


def _node(*, instance_id: str, node_id: str = "node-1", control_addr: str = "127.0.0.1:50061"):
    return SimpleNamespace(
        node_id=node_id,
        node_instance_id=instance_id,
        control_addr=control_addr,
        healthy=True,
        schedulable=True,
        drain=False,
        accept_service_deploy=True,
        service_worker_available=1,
        capacity=1,
        queued=0,
        python_version="py3.11",
    )


def _service_compensation_spec() -> dict[str, object]:
    return {
        "infocenter_target": "127.0.0.1:50051",
        "blob": b"def run(**kwargs):\n    return kwargs\n",
        "runtime": "py3",
        "entry_module": "demo_service",
        "entry_callable": "run",
        "package_format": "py",
        "export_mode": "all",
        "export_methods": [],
        "managed_global_names": [],
        "initial_globals": {},
        "policy_id": "default_safe",
        "worker_count": 1,
        "heartbeat_timeout_sec": 30,
        "idle_ttl_sec": 0,
        "expose_http": True,
        "node_count": 1,
        "node_limit": 10,
        "timeout_sec": 1.0,
        "api_token": "",
    }


def _task_pool_compensation_spec() -> dict[str, object]:
    return {
        "infocenter_target": "127.0.0.1:50051",
        "owner_client_id": "owner-1",
        "pool_name": "pool-demo",
        "blob": b"def run(**kwargs):\n    return kwargs\n",
        "runtime": "py3",
        "entry_module": "demo_task",
        "entry_callable": "run",
        "package_format": "py",
        "deps": None,
        "managed_global_names": [],
        "initial_globals": {},
        "worker_count": 1,
        "heartbeat_timeout_sec": 30,
        "idle_ttl_sec": 0,
        "chunk_size": 1024,
        "healthy_only": True,
        "tags": [],
        "node_ids": [],
        "node_instance_ids": [],
        "node_count": 1,
        "node_limit": 10,
        "timeout_sec": 1.0,
        "api_token": "",
    }


def test_service_compensation_create_fences_expected_node_instance(monkeypatch) -> None:
    node = _node(instance_id="node-inst-1")
    captured: list[dict[str, object]] = []

    class _InfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def list_nodes(self, **_kwargs):
            return [node]

    class _NodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_service_from_bytes(self, **kwargs):
            captured.append(dict(kwargs))
            return SimpleNamespace(
                kind="service",
                service_id="svc-new",
                service_token="token-new",
                http_base_url=f"http://{self.target}/svc/svc-new",
                worker_count=1,
                heartbeat_timeout_sec=30,
                status=pb2.SERVICE_STATUS_RUNNING,
                heartbeat=lambda: pb2.HeartbeatServiceResponse(ok=True, accepted=True),
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr("pycloud_parallel.execution.service_session._infocenter_client", lambda *args, **kwargs: _InfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.service_session._new_node_control_client", _NodeControlClient)

    service = Service(
        owner_client_id="owner-1",
        service_name="svc-demo",
        sessions={},
        nodes={},
    )
    service._configure_dynamic_compensation(_service_compensation_spec())  # noqa: SLF001

    assert service.try_compensate_replicas() == 1
    assert captured[0]["expected_node_instance_id"] == "node-inst-1"
    assert captured[0]["create_request_id"]
    assert "node-inst-1" in str(captured[0]["create_request_id"])


def test_service_compensation_identity_mismatch_marks_node_lost(monkeypatch) -> None:
    node = _node(instance_id="node-inst-1")
    marked_lost: list[tuple[str, str]] = []

    class _InfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def list_nodes(self, **_kwargs):
            return [node]

        def mark_node_lost(self, node_instance_id, *, reason):
            marked_lost.append((node_instance_id, reason))

    class _NodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_service_from_bytes(self, **_kwargs):
            raise RuntimeError("node control_addr instance mismatch")

        def close(self) -> None:
            return None

    monkeypatch.setattr("pycloud_parallel.execution.service_session._infocenter_client", lambda *args, **kwargs: _InfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.service_session._new_node_control_client", _NodeControlClient)

    service = Service(
        owner_client_id="owner-1",
        service_name="svc-demo",
        sessions={},
        nodes={},
    )
    service._configure_dynamic_compensation(_service_compensation_spec())  # noqa: SLF001

    assert service.try_compensate_replicas() == 0
    assert marked_lost
    assert marked_lost[0][0] == "node-inst-1"
    assert "service compensation identity mismatch" in marked_lost[0][1]


def test_service_compensation_defers_while_retry_probe_pending(monkeypatch) -> None:
    from pycloud_parallel import Service

    retry_node = _node(instance_id="node-inst-retry", node_id="node-2", control_addr="127.0.0.1:50062")
    active_node = _node(instance_id="node-inst-active", node_id="node-1", control_addr="127.0.0.1:50061")
    captured: list[dict[str, object]] = []

    class _InfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def list_nodes(self, **_kwargs):
            return [retry_node, active_node]

    class _NodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_service_from_bytes(self, **kwargs):
            captured.append(dict(kwargs))
            return SimpleNamespace(
                kind="service",
                service_id="svc-new",
                service_token="token-new",
                http_base_url=f"http://{self.target}/svc/svc-new",
                worker_count=1,
                heartbeat_timeout_sec=30,
                status=pb2.SERVICE_STATUS_RUNNING,
                heartbeat=lambda: pb2.HeartbeatServiceResponse(ok=True, accepted=True),
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr("pycloud_parallel.execution.service_session._infocenter_client", lambda *args, **kwargs: _InfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.service_session._new_node_control_client", _NodeControlClient)

    service = Service(
        owner_client_id="owner-1",
        service_name="svc-demo",
        sessions={
            "node-inst-retry": SimpleNamespace(kind="service", failed=True, last_error="timeout"),
            "node-inst-active": SimpleNamespace(kind="service", failed=False, last_error=""),
        },
        nodes={
            "node-inst-retry": retry_node,
            "node-inst-active": active_node,
        },
    )
    spec = _service_compensation_spec()
    spec["node_count"] = 2
    service._configure_dynamic_compensation(spec)  # noqa: SLF001
    service._discard_active_replica("node-inst-retry")  # noqa: SLF001
    service._mark_retry_probe_replica("node-inst-retry")  # noqa: SLF001

    assert service.try_compensate_replicas() == 0
    assert captured == []


def test_service_compensation_does_not_defer_retry_probe_when_no_active(monkeypatch) -> None:
    from pycloud_parallel import Service

    retry_node = _node(instance_id="node-inst-retry")
    captured: list[dict[str, object]] = []

    class _InfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def list_nodes(self, **_kwargs):
            return [retry_node]

    class _NodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_service_from_bytes(self, **kwargs):
            captured.append(dict(kwargs))
            return SimpleNamespace(
                kind="service",
                service_id="svc-new",
                service_token="token-new",
                http_base_url=f"http://{self.target}/svc/svc-new",
                worker_count=1,
                heartbeat_timeout_sec=30,
                status=pb2.SERVICE_STATUS_RUNNING,
                heartbeat=lambda: pb2.HeartbeatServiceResponse(ok=True, accepted=True),
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr("pycloud_parallel.execution.service_session._infocenter_client", lambda *args, **kwargs: _InfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.service_session._new_node_control_client", _NodeControlClient)

    service = Service(
        owner_client_id="owner-1",
        service_name="svc-demo",
        sessions={"node-inst-retry": SimpleNamespace(kind="service", failed=True, last_error="timeout")},
        nodes={"node-inst-retry": retry_node},
    )
    service._configure_dynamic_compensation(_service_compensation_spec())  # noqa: SLF001
    service._discard_active_replica("node-inst-retry")  # noqa: SLF001
    service._mark_retry_probe_replica("node-inst-retry")  # noqa: SLF001

    assert service.try_compensate_replicas() == 1
    assert captured[0]["expected_node_instance_id"] == "node-inst-retry"


def test_service_compensation_prunes_stale_retry_probe_owner_replica(monkeypatch) -> None:
    from pycloud_parallel import Service

    node = _node(instance_id="node-inst-new")
    captured: list[dict[str, object]] = []
    closed: list[str] = []

    class _InfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def list_nodes(self, **_kwargs):
            return [node]

    class _OldClient:
        target = "127.0.0.1:50061"

        def close(self) -> None:
            closed.append("old")

    class _NodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_service_from_bytes(self, **kwargs):
            captured.append(dict(kwargs))
            return SimpleNamespace(
                kind="service",
                service_id="svc-new",
                service_token="token-new",
                http_base_url=f"http://{self.target}/svc/svc-new",
                worker_count=1,
                heartbeat_timeout_sec=30,
                status=pb2.SERVICE_STATUS_RUNNING,
                heartbeat=lambda: pb2.HeartbeatServiceResponse(ok=True, accepted=True),
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr("pycloud_parallel.execution.service_session._infocenter_client", lambda *args, **kwargs: _InfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.service_session._new_node_control_client", _NodeControlClient)

    service = Service(
        owner_client_id="owner-1",
        service_name="svc-demo",
        sessions={"node-inst-old": SimpleNamespace(kind="service", failed=True, last_error="cannot connect")},
        nodes={"node-inst-old": _node(instance_id="node-inst-old")},
        _clients={"node-inst-old": _OldClient()},
    )
    service._configure_dynamic_compensation(_service_compensation_spec())  # noqa: SLF001
    service._discard_active_replica("node-inst-old")  # noqa: SLF001
    service._mark_retry_probe_replica("node-inst-old")  # noqa: SLF001

    assert service.try_compensate_replicas() == 1
    assert set(service.sessions) == {"node-inst-new"}
    assert [route["node_instance_id"] for route in service.route_summary()] == ["node-inst-new"]
    assert captured[0]["expected_node_instance_id"] == "node-inst-new"
    assert closed == ["old"]


def test_service_compensation_redeploys_same_node_after_service_terminal(monkeypatch) -> None:
    from pycloud_parallel import Service

    node = _node(instance_id="node-inst-1")
    captured: list[dict[str, object]] = []
    closed: list[str] = []

    class _InfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def list_nodes(self, **_kwargs):
            return [node]

    class _OldClient:
        target = "127.0.0.1:50061"

        def close(self) -> None:
            closed.append("old")

    class _NodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_service_from_bytes(self, **kwargs):
            captured.append(dict(kwargs))
            return SimpleNamespace(
                kind="service",
                service_id="svc-new",
                service_token="token-new",
                http_base_url=f"http://{self.target}/svc/svc-new",
                worker_count=1,
                heartbeat_timeout_sec=30,
                status=pb2.SERVICE_STATUS_RUNNING,
                heartbeat=lambda: pb2.HeartbeatServiceResponse(ok=True, accepted=True),
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr("pycloud_parallel.execution.service_session._infocenter_client", lambda *args, **kwargs: _InfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.service_session._new_node_control_client", _NodeControlClient)

    old_session = SimpleNamespace(kind="service", failed=False, last_error="", status=pb2.SERVICE_STATUS_RUNNING)
    service = Service(
        owner_client_id="owner-1",
        service_name="svc-demo",
        sessions={"node-inst-1": old_session},
        nodes={"node-inst-1": node},
        _clients={"node-inst-1": _OldClient()},
    )
    service._configure_dynamic_compensation(_service_compensation_spec())  # noqa: SLF001
    service._record_terminal_heartbeat_failure("node-inst-1", old_session, RuntimeError("service is stopped"))  # noqa: SLF001

    assert "node-inst-1" not in service.failures
    assert service.try_compensate_replicas() == 1
    assert set(service.sessions) == {"node-inst-1"}
    assert captured[0]["expected_node_instance_id"] == "node-inst-1"
    assert closed == ["old"]


def test_taskpool_compensation_create_fences_expected_node_instance(monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    node = _node(instance_id="node-inst-1")
    captured: list[dict[str, object]] = []

    class _InfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            return [node]

    class _NodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_task_pool_from_bytes(self, **kwargs):
            captured.append(dict(kwargs))
            pool = SimpleNamespace(
                kind="task_pool",
                owner_client_id=kwargs["owner_client_id"],
                pool_id="pool-new",
                pool_name=kwargs["pool_name"],
                pool_token="token-new",
                code_version="sha256:test",
                worker_count=1,
                heartbeat_timeout_sec=30,
                heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True),
                _client=self,
            )
            return pool

        def close(self) -> None:
            return None

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _InfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _NodeControlClient)

    session = TaskPool(pools={}, nodes={}, task_method="run", job_id="job-contract")
    session._configure_dynamic_compensation(_task_pool_compensation_spec())  # noqa: SLF001

    try:
        assert session.try_compensate_replicas() == 1
        assert captured[0]["expected_node_instance_id"] == "node-inst-1"
    finally:
        session.close()


def test_taskpool_compensation_identity_mismatch_marks_node_lost(monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    node = _node(instance_id="node-inst-1")
    marked_lost: list[tuple[str, str]] = []

    class _InfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            return [node]

        def mark_node_lost(self, node_instance_id, *, reason):
            marked_lost.append((node_instance_id, reason))

    class _NodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec
            self.closed = False

        def create_task_pool_from_bytes(self, **_kwargs):
            raise RuntimeError("expected_node_instance_id mismatch")

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _InfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _NodeControlClient)

    session = TaskPool(pools={}, nodes={}, task_method="run", job_id="job-contract")
    session._configure_dynamic_compensation(_task_pool_compensation_spec())  # noqa: SLF001

    try:
        assert session.try_compensate_replicas() == 0
        assert marked_lost
        assert marked_lost[0][0] == "node-inst-1"
        assert "task pool compensation identity mismatch" in marked_lost[0][1]
    finally:
        session.close()


def test_terminal_heartbeat_error_is_not_retried_even_with_retry_forever() -> None:
    calls = {"count": 0}

    class _StoppedService:
        kind = "service"
        heartbeat_timeout_sec = 1
        heartbeat_failure_threshold = 1
        failed = False
        last_error = ""
        status = pb2.SERVICE_STATUS_RUNNING

        def heartbeat(self):
            calls["count"] += 1
            raise RuntimeError("service not found")

    service = Service(
        owner_client_id="owner-1",
        service_name="svc-terminal",
        sessions={"node-inst-1": _StoppedService()},
        nodes={},
    )

    service._start_keepalive(interval_sec=0.02)  # noqa: SLF001
    try:
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline and not service._is_terminal_replica("node-inst-1"):  # noqa: SLF001
            time.sleep(0.02)

        first_count = calls["count"]
        time.sleep(0.15)
        assert first_count == 1
        assert calls["count"] == first_count
        assert "node-inst-1" not in service._active_replica_ids  # noqa: SLF001
    finally:
        service.close()


def test_user_file_path_put_data_upload_does_not_cleanup_source(tmp_path) -> None:
    source = tmp_path / "user-input.bin"
    source.write_bytes(b"user-owned payload")

    class _Client:
        control_addr = "node-a:50061"
        node_id = "node-a"
        node_instance_id = "node-a-inst"

        def upload_object_from_file(self, *, file_path, format, chunk_size):
            assert Path(file_path) == source
            assert Path(file_path).exists()
            return DataRef(
                ref_id="sha256:" + ("1" * 64),
                storage_id="sha256:" + ("1" * 64),
                format=format,
                size_bytes=Path(file_path).stat().st_size,
                materialize_as="path",
                locator_kind="node_control",
                locator_token=self.control_addr,
                control_addr=self.control_addr,
            )

        def upload_object_from_bytes(self, **_kwargs):
            raise AssertionError("file path put_data must not use byte upload")

    ref = support._put_data_via_clients([_Client()], source, format="bin")

    assert ref.object_id == "sha256:" + ("1" * 64)
    assert source.read_bytes() == b"user-owned payload"


def test_recovery_public_api_signature_snapshot() -> None:
    taskpool_params = inspect.signature(HttpNodeControlClient.create_task_pool_from_bytes).parameters
    service_params = inspect.signature(HttpNodeControlClient.create_service_from_bytes).parameters

    assert "expected_node_instance_id" in taskpool_params
    assert taskpool_params["expected_node_instance_id"].default == ""
    assert "create_request_id" in taskpool_params
    assert taskpool_params["create_request_id"].default == ""
    assert "expected_node_instance_id" in service_params
    assert service_params["expected_node_instance_id"].default == ""
    assert "create_request_id" in service_params
    assert service_params["create_request_id"].default == ""
