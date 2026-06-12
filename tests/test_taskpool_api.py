from __future__ import annotations

"""Tests for the V1 task-pool-facing API helpers."""

from pathlib import Path
import threading
import time
from types import SimpleNamespace

from pycloud_parallel.controlplane.infocenter_client import InfoCenterNode
from pycloud_parallel.controlplane.artifact import Artifact
from pycloud_parallel.execution.task_pool import TaskPool


ROOT = Path(__file__).resolve().parents[1]


def test_taskpool_route_summary_reports_fixed_routes():
    node = InfoCenterNode(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="10.0.0.1:50061",
        healthy=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )
    pool = SimpleNamespace(
        pool_id="pool-1",
        pool_name="calc-pool",
        owner_client_id="owner-1",
        node_id="node-1",
    )
    session = TaskPool(
        pools={"node-inst-1": pool},
        nodes={"node-inst-1": node},
        task_method="run",
    )

    assert session.routes() == [
        {
            "node_instance_id": "node-inst-1",
            "node_id": "node-1",
            "control_addr": "10.0.0.1:50061",
            "pool_id": "pool-1",
            "pool_name": "calc-pool",
            "owner_client_id": "owner-1",
        }
    ]


def test_taskpool_after_keepalive_tick_submits_compensation_async() -> None:
    session = TaskPool(pools={}, nodes={}, task_method="run")
    session._configure_dynamic_compensation(  # noqa: SLF001
        {
            "node_count": 1,
            "check_interval_sec": 0.05,
            "infocenter_target": "127.0.0.1:1",
        }
    )
    compensation_started = threading.Event()
    release_compensation = threading.Event()

    def _slow_compensation() -> int:
        compensation_started.set()
        release_compensation.wait(0.5)
        return 0

    session.try_compensate_replicas = _slow_compensation  # type: ignore[method-assign]

    started = time.monotonic()
    try:
        session._after_keepalive_tick()  # noqa: SLF001
        elapsed = time.monotonic() - started
        assert elapsed < 0.2
        assert compensation_started.wait(1.0)
    finally:
        release_compensation.set()
        session._stop_keepalive()  # noqa: SLF001


def test_taskpool_after_keepalive_tick_uses_recovery_intervals() -> None:
    session = TaskPool(pools={}, nodes={}, task_method="run")
    session._configure_dynamic_compensation(  # noqa: SLF001
        {
            "node_count": 2,
            "check_interval_sec": 15.0,
            "infocenter_target": "127.0.0.1:1",
        }
    )
    submitted = []
    session._submit_compensation_attempt = lambda **kwargs: submitted.append(kwargs) or True  # type: ignore[method-assign]  # noqa: SLF001

    original_monotonic = time.monotonic
    try:
        time.monotonic = lambda: 100.0  # type: ignore[assignment]
        session._last_compensation_attempt_at = 99.2  # noqa: SLF001
        session._after_keepalive_tick()  # noqa: SLF001
        assert submitted == []

        time.monotonic = lambda: 100.3  # type: ignore[assignment]
        session._after_keepalive_tick()  # noqa: SLF001
        assert len(submitted) == 1

        session._active_replica_ids = {"node-1"}  # noqa: SLF001
        session._last_compensation_attempt_at = 196.0  # noqa: SLF001
        time.monotonic = lambda: 200.0  # type: ignore[assignment]
        session._after_keepalive_tick()  # noqa: SLF001
        assert len(submitted) == 1

        time.monotonic = lambda: 255.9  # type: ignore[assignment]
        session._after_keepalive_tick()  # noqa: SLF001
        assert len(submitted) == 1

        time.monotonic = lambda: 256.1  # type: ignore[assignment]
        session._after_keepalive_tick()  # noqa: SLF001
        assert len(submitted) == 2

        session._active_replica_ids = {"node-1", "node-2"}  # noqa: SLF001
        session._last_compensation_attempt_at = 0.0  # noqa: SLF001
        time.monotonic = lambda: 300.0  # type: ignore[assignment]
        session._after_keepalive_tick()  # noqa: SLF001
        assert len(submitted) == 2
    finally:
        time.monotonic = original_monotonic  # type: ignore[assignment]
        session._stop_keepalive()  # noqa: SLF001


def test_taskpool_runtime_boundary_does_not_use_service_discovery_surface() -> None:
    text = (ROOT / "src/pycloud_parallel/execution/task_pool.py").read_text(encoding="utf-8")
    assert "service_name=" not in text
    assert "list_service_routes(" not in text
    assert ".call_service(" not in text
    assert "/services/" not in text


def test_taskpool_runtime_boundary_keeps_taskpool_protocol_and_capacity_account() -> None:
    taskpool_text = (ROOT / "src/pycloud_parallel/execution/task_pool.py").read_text(encoding="utf-8")
    node_text = (ROOT / "src/pycloud_parallel/controlplane/nodecontrol_state.py").read_text(encoding="utf-8")
    assert "submit_pool_tasks" in taskpool_text
    assert "pull_pool_results" in taskpool_text
    assert "task_pool_worker_capacity" in node_text
    assert "service_worker_capacity" in node_text


def test_taskpool_open_rejects_main_module_callable_source() -> None:
    import pytest
    from pycloud_parallel.execution.deployment_create_helper import prepare_deployment_artifact

    def run(value=0):
        return value

    run.__module__ = "__main__"

    with pytest.raises(ValueError, match="defined in __main__"):
        prepare_deployment_artifact(
            consumer_kind="task",
            source=run,
            artifact=None,
            deps=None,
            runtime="py3",
            entry_module="",
            entry_callable="run",
            package_format="",
            managed_global_names=None,
        )


def test_taskpool_open_uses_configured_default_heartbeat_timeout(monkeypatch) -> None:
    import pycloud_parallel.execution.task_pool as task_pool_mod
    from pycloud_parallel.controlplane import config as config_mod

    monkeypatch.setenv("PYCLOUD_TASKPOOL_HEARTBEAT_TIMEOUT_SEC", "456")
    config_mod.reload_config()
    monkeypatch.setattr(task_pool_mod, "get_taskpool_heartbeat_timeout_sec", config_mod.get_taskpool_heartbeat_timeout_sec)
    created: list[dict[str, object]] = []

    node = InfoCenterNode(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        healthy=True,
        schedulable=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            return [node]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_task_pool_from_bytes(self, **kwargs):
            created.append(dict(kwargs))
            return SimpleNamespace(
                owner_client_id=kwargs["owner_client_id"],
                pool_id="pool-1",
                pool_name=kwargs["pool_name"],
                pool_token="token",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                heartbeat=lambda **_kwargs: SimpleNamespace(ok=True, accepted=True),
                close=lambda reason="": None,
                _client=SimpleNamespace(close=lambda: None),
            )

        def close(self):
            return None

    monkeypatch.setattr(task_pool_mod, "_infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr(task_pool_mod, "_new_node_control_client", _FakeNodeControlClient)

    try:
        pool = TaskPool.open(
            target="127.0.0.1:50051",
            artifact=Artifact.from_bytes(
                b"def run(**_kwargs): return {'ok': True}\n",
                package_format="py",
                entry_module="demo_task",
                entry_callable="run",
            ),
            worker_count=1,
        )
        pool.close()
        assert created[0]["heartbeat_timeout_sec"] == 456

        created.clear()
        pool = TaskPool.open(
            target="127.0.0.1:50051",
            artifact=Artifact.from_bytes(
                b"def run(**_kwargs): return {'ok': True}\n",
                package_format="py",
                entry_module="demo_task",
                entry_callable="run",
            ),
            worker_count=1,
            heartbeat_timeout_sec=12,
        )
        pool.close()
        assert created[0]["heartbeat_timeout_sec"] == 12
    finally:
        monkeypatch.delenv("PYCLOUD_TASKPOOL_HEARTBEAT_TIMEOUT_SEC", raising=False)
        config_mod.reload_config()


def test_taskpool_try_compensate_replicas_adds_newly_available_node(monkeypatch) -> None:
    node_1 = InfoCenterNode(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        healthy=True,
        schedulable=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )
    node_2 = InfoCenterNode(
        node_instance_id="node-inst-2",
        node_id="node-2",
        control_addr="127.0.0.1:50062",
        healthy=True,
        schedulable=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )
    created = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            return [node_1, node_2]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_task_pool_from_bytes(self, **kwargs):
            created.append((self.target, dict(kwargs)))
            return SimpleNamespace(
                owner_client_id=kwargs["owner_client_id"],
                pool_id=f"pool-{self.target.rsplit(':', 1)[-1]}",
                pool_name=kwargs["pool_name"],
                pool_token="token",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                heartbeat=lambda **_kwargs: SimpleNamespace(ok=True, accepted=True),
                close=lambda reason="": None,
                _client=SimpleNamespace(close=lambda: None),
            )

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    existing_pool = SimpleNamespace(
        owner_client_id="owner-1",
        pool_id="pool-existing",
        pool_name="pool-demo",
        pool_token="token",
        code_version="sha256:test",
        worker_count=1,
        heartbeat_timeout_sec=30,
        node_id="node-1",
        _client=SimpleNamespace(close=lambda: None),
    )
    session = TaskPool(
        pools={"node-inst-1": existing_pool},
        nodes={"node-inst-1": node_1},
        task_method="run",
    )
    session._configure_dynamic_compensation(  # noqa: SLF001
        {
            "infocenter_target": "127.0.0.1:50051",
            "owner_client_id": "owner-1",
            "pool_name": "pool-demo",
            "blob": b"def run(**_kwargs): return {'ok': True}\n",
            "runtime": "py3",
            "entry_module": "demo_task",
            "entry_callable": "run",
            "package_format": "py",
            "managed_global_names": [],
            "worker_count": 1,
            "heartbeat_timeout_sec": 30,
            "idle_ttl_sec": 0,
            "node_count": 2,
            "node_limit": 10,
            "timeout_sec": 1.0,
        }
    )

    added = session.try_compensate_replicas()

    assert added == 1
    assert set(session.node_instance_ids) == {"node-inst-1", "node-inst-2"}
    assert created[0][0] == "127.0.0.1:50062"
    assert created[0][1]["pool_name"] == "pool-demo"


def test_taskpool_compensation_skips_when_no_new_node_is_available(monkeypatch) -> None:
    node_1 = InfoCenterNode(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        healthy=True,
        schedulable=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )
    node_2 = InfoCenterNode(
        node_instance_id="node-inst-2",
        node_id="node-2",
        control_addr="127.0.0.1:50062",
        healthy=True,
        schedulable=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )
    created = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            return [node_1, node_2]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            del timeout_sec
            self.target = target

        def create_task_pool_from_bytes(self, **kwargs):
            created.append((self.target, dict(kwargs)))
            raise AssertionError("compensation should not create on active nodes")

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    session = TaskPool(
        pools={
            "node-inst-1": SimpleNamespace(
                owner_client_id="owner-1",
                pool_id="pool-1",
                pool_name="pool-demo",
                pool_token="token",
                worker_count=1,
                heartbeat_timeout_sec=30,
                node_id="node-1",
                _client=SimpleNamespace(close=lambda: None),
            ),
            "node-inst-2": SimpleNamespace(
                owner_client_id="owner-1",
                pool_id="pool-2",
                pool_name="pool-demo",
                pool_token="token",
                worker_count=1,
                heartbeat_timeout_sec=30,
                node_id="node-2",
                _client=SimpleNamespace(close=lambda: None),
            ),
        },
        nodes={"node-inst-1": node_1, "node-inst-2": node_2},
        task_method="run",
    )
    session._configure_dynamic_compensation(  # noqa: SLF001
        {
            "infocenter_target": "127.0.0.1:50051",
            "owner_client_id": "owner-1",
            "pool_name": "pool-demo",
            "blob": b"def run(**_kwargs): return {'ok': True}\n",
            "runtime": "py3",
            "entry_module": "demo_task",
            "entry_callable": "run",
            "package_format": "py",
            "managed_global_names": [],
            "worker_count": 1,
            "heartbeat_timeout_sec": 30,
            "idle_ttl_sec": 0,
            "node_count": 3,
            "node_limit": 10,
            "timeout_sec": 1.0,
        }
    )

    assert session.try_compensate_replicas() == 0
    assert created == []


def test_taskpool_compensation_attaches_each_replica_before_next_create_finishes(monkeypatch) -> None:
    node_1 = InfoCenterNode(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        healthy=True,
        schedulable=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )
    node_2 = InfoCenterNode(
        node_instance_id="node-inst-2",
        node_id="node-2",
        control_addr="127.0.0.1:50062",
        healthy=True,
        schedulable=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )
    created = []
    heartbeats = []
    second_create_started = threading.Event()
    release_second_create = threading.Event()

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            return [node_1, node_2]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_task_pool_from_bytes(self, **kwargs):
            created.append((self.target, dict(kwargs)))
            node_instance_id = str(kwargs["expected_node_instance_id"])
            if node_instance_id == "node-inst-2":
                second_create_started.set()
                assert release_second_create.wait(2.0)

            def _heartbeat(**_kwargs):
                heartbeats.append(node_instance_id)
                return SimpleNamespace(ok=True, accepted=True)

            return SimpleNamespace(
                owner_client_id=kwargs["owner_client_id"],
                pool_id=f"pool-{node_instance_id}",
                pool_name=kwargs["pool_name"],
                pool_token="token",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                heartbeat=_heartbeat,
                close=lambda reason="": None,
                _client=SimpleNamespace(close=lambda: None),
            )

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    session = TaskPool(pools={}, nodes={}, task_method="run")
    session._configure_dynamic_compensation(  # noqa: SLF001
        {
            "infocenter_target": "127.0.0.1:50051",
            "owner_client_id": "owner-1",
            "pool_name": "pool-demo",
            "blob": b"def run(**_kwargs): return {'ok': True}\n",
            "runtime": "py3",
            "entry_module": "demo_task",
            "entry_callable": "run",
            "package_format": "py",
            "managed_global_names": [],
            "worker_count": 1,
            "heartbeat_timeout_sec": 30,
            "idle_ttl_sec": 0,
            "node_count": 2,
            "node_limit": 10,
            "timeout_sec": 1.0,
        }
    )

    result = {}
    worker = threading.Thread(target=lambda: result.setdefault("added", session.try_compensate_replicas()), daemon=True)
    worker.start()
    try:
        assert second_create_started.wait(1.0)
        assert "node-inst-1" in session.node_instance_ids
        assert "node-inst-1" in session._active_replica_snapshot()  # noqa: SLF001
        assert heartbeats == ["node-inst-1"]
    finally:
        release_second_create.set()
        worker.join(2.0)
    assert not worker.is_alive()
    assert result["added"] == 2
    assert set(session.node_instance_ids) == {"node-inst-1", "node-inst-2"}
    assert heartbeats == ["node-inst-1", "node-inst-2"]
    assert [item[0] for item in created] == ["127.0.0.1:50061", "127.0.0.1:50062"]


def test_taskpool_compensation_submits_async_globals_without_blocking(monkeypatch) -> None:
    node = InfoCenterNode(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        healthy=True,
        schedulable=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )
    async_updates = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            return [node]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target

        def create_task_pool_from_bytes(self, **kwargs):
            return SimpleNamespace(
                owner_client_id=kwargs["owner_client_id"],
                pool_id="pool-new",
                pool_name=kwargs["pool_name"],
                pool_token="token",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                heartbeat=lambda **_kwargs: SimpleNamespace(ok=True, accepted=True),
                close=lambda reason="": None,
                _client=SimpleNamespace(close=lambda: None),
            )

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    session = TaskPool(pools={}, nodes={}, task_method="run")
    session._last_managed_globals = {"value": 1}  # noqa: SLF001
    monkeypatch.setattr(session, "update_globals", lambda _values: (_ for _ in ()).throw(AssertionError("must be async")))
    monkeypatch.setattr(
        session,
        "_submit_async_update_globals",
        lambda values, *, reason="": async_updates.append((dict(values), reason)) or True,
    )
    session._configure_dynamic_compensation(  # noqa: SLF001
        {
            "infocenter_target": "127.0.0.1:50051",
            "owner_client_id": "owner-1",
            "pool_name": "pool-demo",
            "blob": b"def run(**_kwargs): return {'ok': True}\n",
            "runtime": "py3",
            "entry_module": "demo_task",
            "entry_callable": "run",
            "package_format": "py",
            "managed_global_names": [],
            "worker_count": 1,
            "heartbeat_timeout_sec": 30,
            "idle_ttl_sec": 0,
            "node_count": 1,
            "node_limit": 10,
            "timeout_sec": 1.0,
        }
    )

    assert session.try_compensate_replicas() == 1
    assert async_updates == [({"value": 1}, "compensation")]
    assert "node-inst-1" in session.node_instance_ids


def test_taskpool_async_update_globals_keeps_latest_pending(monkeypatch) -> None:
    session = TaskPool(pools={}, nodes={}, task_method="run")
    started = threading.Event()
    release = threading.Event()
    calls = []

    def _update(values):
        calls.append(dict(values))
        if values.get("value") == 1:
            started.set()
            release.wait(timeout=5.0)
        return "digest"

    monkeypatch.setattr(session, "update_globals", _update)
    try:
        assert session._submit_async_update_globals({"value": 1}, reason="first") is True  # noqa: SLF001
        assert started.wait(timeout=2.0)
        assert session._submit_async_update_globals({"value": 2}, reason="middle") is True  # noqa: SLF001
        assert session._submit_async_update_globals({"value": 3}, reason="latest") is True  # noqa: SLF001
        release.set()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and len(calls) < 2:
            time.sleep(0.01)
        assert calls == [{"value": 1}, {"value": 3}]
    finally:
        release.set()
        session.close()


def test_taskpool_compensation_uses_active_count_and_skips_failed_node(monkeypatch) -> None:
    node_1 = InfoCenterNode(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        healthy=True,
        schedulable=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )
    node_2 = InfoCenterNode(
        node_instance_id="node-inst-2",
        node_id="node-2",
        control_addr="127.0.0.1:50062",
        healthy=True,
        schedulable=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )
    created = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            return [node_1, node_2]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_task_pool_from_bytes(self, **kwargs):
            created.append((self.target, dict(kwargs)))
            return SimpleNamespace(
                owner_client_id=kwargs["owner_client_id"],
                pool_id="pool-new",
                pool_name=kwargs["pool_name"],
                pool_token="token",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                heartbeat=lambda **_kwargs: SimpleNamespace(ok=True, accepted=True),
                close=lambda reason="": None,
                _client=SimpleNamespace(close=lambda: None),
            )

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    existing_pool = SimpleNamespace(
        owner_client_id="owner-1",
        pool_id="pool-existing",
        pool_name="pool-demo",
        pool_token="token",
        code_version="sha256:test",
        worker_count=1,
        heartbeat_timeout_sec=30,
        node_id="node-1",
        failed=True,
        last_error="ModuleNotFoundError: missing_pkg",
        _client=SimpleNamespace(close=lambda: None),
    )
    session = TaskPool(
        pools={"node-inst-1": existing_pool},
        nodes={"node-inst-1": node_1},
        task_method="run",
    )
    session._active_replica_ids.discard("node-inst-1")  # noqa: SLF001
    session.failures["node-inst-1"] = "ModuleNotFoundError: missing_pkg"
    session._configure_dynamic_compensation(  # noqa: SLF001
        {
            "infocenter_target": "127.0.0.1:50051",
            "owner_client_id": "owner-1",
            "pool_name": "pool-demo",
            "blob": b"def run(**_kwargs): return {'ok': True}\n",
            "runtime": "py3",
            "entry_module": "demo_task",
            "entry_callable": "run",
            "package_format": "py",
            "managed_global_names": [],
            "worker_count": 1,
            "heartbeat_timeout_sec": 30,
            "idle_ttl_sec": 0,
            "node_count": 1,
            "node_limit": 10,
            "timeout_sec": 1.0,
        }
    )

    added = session.try_compensate_replicas()

    assert added == 1
    assert created[0][0] == "127.0.0.1:50062"
    assert "node-inst-1" in session.failures


def test_taskpool_compensation_allows_restarted_node_with_new_instance_id(monkeypatch) -> None:
    old_node = InfoCenterNode(
        node_instance_id="node-inst-old",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        healthy=False,
        schedulable=False,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=0,
    )
    restarted_node = InfoCenterNode(
        node_instance_id="node-inst-new",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        healthy=True,
        schedulable=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )
    created = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            return [restarted_node]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_task_pool_from_bytes(self, **kwargs):
            created.append((self.target, dict(kwargs)))
            return SimpleNamespace(
                owner_client_id=kwargs["owner_client_id"],
                pool_id="pool-restarted",
                pool_name=kwargs["pool_name"],
                pool_token="token",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                heartbeat=lambda **_kwargs: SimpleNamespace(ok=True, accepted=True),
                close=lambda reason="": None,
                _client=SimpleNamespace(close=lambda: None),
            )

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    existing_pool = SimpleNamespace(
        owner_client_id="owner-1",
        pool_id="pool-old",
        pool_name="pool-demo",
        pool_token="token",
        code_version="sha256:test",
        worker_count=1,
        heartbeat_timeout_sec=30,
        node_id="node-1",
        failed=True,
        last_error="ModuleNotFoundError: missing_pkg",
        _client=SimpleNamespace(close=lambda: None),
    )
    session = TaskPool(
        pools={"node-inst-old": existing_pool},
        nodes={"node-inst-old": old_node},
        task_method="run",
    )
    session._active_replica_ids.discard("node-inst-old")  # noqa: SLF001
    session.failures["node-inst-old"] = "ModuleNotFoundError: missing_pkg"
    session._configure_dynamic_compensation(  # noqa: SLF001
        {
            "infocenter_target": "127.0.0.1:50051",
            "owner_client_id": "owner-1",
            "pool_name": "pool-demo",
            "blob": b"def run(**_kwargs): return {'ok': True}\n",
            "runtime": "py3",
            "entry_module": "demo_task",
            "entry_callable": "run",
            "package_format": "py",
            "managed_global_names": [],
            "worker_count": 1,
            "heartbeat_timeout_sec": 30,
            "idle_ttl_sec": 0,
            "node_ids": ["node-1"],
            "node_count": 1,
            "node_limit": 10,
            "timeout_sec": 1.0,
        }
    )

    added = session.try_compensate_replicas()

    assert added == 1
    assert created[0][0] == "127.0.0.1:50061"
    assert "node-inst-new" in session.node_instance_ids
    assert "node-inst-old" in session.failures


def test_taskpool_compensation_allows_retry_probe_when_no_active_replicas(monkeypatch) -> None:
    node = InfoCenterNode(
        node_instance_id="node-inst-1",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        healthy=True,
        schedulable=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )
    created = []
    heartbeats = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            return [node]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_task_pool_from_bytes(self, **kwargs):
            created.append((self.target, dict(kwargs)))
            return SimpleNamespace(
                owner_client_id=kwargs["owner_client_id"],
                pool_id="pool-recovered",
                pool_name=kwargs["pool_name"],
                pool_token="token-new",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                heartbeat=lambda **kwargs: heartbeats.append(dict(kwargs)) or SimpleNamespace(ok=True, accepted=True),
                close=lambda reason="": None,
                _client=SimpleNamespace(close=lambda: None),
            )

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    existing_pool = SimpleNamespace(
        owner_client_id="owner-1",
        pool_id="pool-old",
        pool_name="pool-demo",
        pool_token="token-old",
        code_version="sha256:test",
        worker_count=1,
        heartbeat_timeout_sec=30,
        node_id="node-1",
        failed=True,
        last_error="ConnectionResetError(10054, 'connection reset')",
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    session = TaskPool(
        pools={"node-inst-1": existing_pool},
        nodes={"node-inst-1": node},
        task_method="run",
    )
    session._discard_active_replica("node-inst-1")  # noqa: SLF001
    session._mark_retry_probe_replica("node-inst-1")  # noqa: SLF001
    session.failures["node-inst-1"] = "ConnectionResetError(10054, 'connection reset')"
    session._configure_dynamic_compensation(  # noqa: SLF001
        {
            "infocenter_target": "127.0.0.1:50051",
            "owner_client_id": "owner-1",
            "pool_name": "pool-demo",
            "blob": b"def run(**_kwargs): return {'ok': True}\n",
            "runtime": "py3",
            "entry_module": "demo_task",
            "entry_callable": "run",
            "package_format": "py",
            "managed_global_names": [],
            "worker_count": 1,
            "heartbeat_timeout_sec": 30,
            "idle_ttl_sec": 0,
            "node_count": 1,
            "node_limit": 10,
            "timeout_sec": 1.0,
        }
    )

    added = session.try_compensate_replicas()

    assert added == 1
    assert created[0][1]["expected_node_instance_id"] == "node-inst-1"
    assert heartbeats
    assert session._pools["node-inst-1"].pool_id == "pool-recovered"  # noqa: SLF001
    assert "node-inst-1" in session._active_replica_ids  # noqa: SLF001
    assert "node-inst-1" not in session.failures


def test_taskpool_compensation_does_not_defer_stale_retry_probe_with_active_replica(monkeypatch) -> None:
    active_node = InfoCenterNode(
        node_instance_id="node-2-current",
        node_id="node-2",
        control_addr="127.0.0.1:50062",
        healthy=True,
        schedulable=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )
    new_node = InfoCenterNode(
        node_instance_id="node-1-new",
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        healthy=True,
        schedulable=True,
        capacity=4,
        queue_capacity=32,
        queued=0,
        inflight=0,
        credit=32,
    )
    created = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def select_task_nodes(self, **_kwargs):
            return [active_node, new_node]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_task_pool_from_bytes(self, **kwargs):
            created.append((self.target, dict(kwargs)))
            return SimpleNamespace(
                owner_client_id=kwargs["owner_client_id"],
                pool_id="pool-new",
                pool_name=kwargs["pool_name"],
                pool_token="token-new",
                code_version="sha256:test",
                worker_count=kwargs["worker_count"],
                heartbeat_timeout_sec=kwargs["heartbeat_timeout_sec"],
                heartbeat=lambda **_kwargs: SimpleNamespace(ok=True, accepted=True),
                close=lambda reason="": None,
                _client=SimpleNamespace(close=lambda: None),
            )

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._new_node_control_client", _FakeNodeControlClient)

    session = TaskPool(
        pools={
            "node-2-current": SimpleNamespace(
                owner_client_id="owner-1",
                pool_id="pool-active",
                pool_name="pool-demo",
                pool_token="token-active",
                code_version="sha256:test",
                worker_count=1,
                heartbeat_timeout_sec=30,
                _client=SimpleNamespace(close=lambda: None),
            ),
            "node-1-old": SimpleNamespace(
                owner_client_id="owner-1",
                pool_id="pool-old",
                pool_name="pool-demo",
                pool_token="token-old",
                code_version="sha256:test",
                worker_count=1,
                heartbeat_timeout_sec=30,
                failed=True,
                _client=SimpleNamespace(close=lambda: None),
            ),
        },
        nodes={
            "node-2-current": active_node,
            "node-1-old": SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061"),
        },
        task_method="run",
    )
    session._discard_active_replica("node-1-old")  # noqa: SLF001
    session._mark_retry_probe_replica("node-1-old")  # noqa: SLF001
    session.failures["node-1-old"] = "ConnectionResetError(10054, 'connection reset')"
    session._configure_dynamic_compensation(  # noqa: SLF001
        {
            "infocenter_target": "127.0.0.1:50051",
            "owner_client_id": "owner-1",
            "pool_name": "pool-demo",
            "blob": b"def run(**_kwargs): return {'ok': True}\n",
            "runtime": "py3",
            "entry_module": "demo_task",
            "entry_callable": "run",
            "package_format": "py",
            "managed_global_names": [],
            "worker_count": 1,
            "heartbeat_timeout_sec": 30,
            "idle_ttl_sec": 0,
            "node_count": 2,
            "node_limit": 10,
            "timeout_sec": 1.0,
        }
    )

    added = session.try_compensate_replicas()

    assert added == 1
    assert created[0][1]["expected_node_instance_id"] == "node-1-new"
    assert "node-1-old" not in session._retry_probe_replica_snapshot()  # noqa: SLF001


def test_prepare_task_payload_for_submit_uses_task_submit_policy(monkeypatch) -> None:
    from pycloud_parallel.execution.support import _prepare_task_payload_for_submit

    captured = {}

    def _fake_prepare(payload, *, put_data, estimate_inline_size, policy, managed_global_policy=None):
        del put_data, estimate_inline_size
        del managed_global_policy
        captured["payload"] = dict(payload or {})
        captured["mode"] = policy.mode
        captured["consume_on_read"] = policy.consume_on_read
        return dict(payload or {})

    monkeypatch.setattr(
        "pycloud_parallel.execution.support.prepare_outbound_payload",
        _fake_prepare,
    )

    client = SimpleNamespace(target="127.0.0.1:50061")
    prepared = _prepare_task_payload_for_submit(client, {"value": 7})

    assert prepared == {"value": 7}
    assert captured["payload"] == {"value": 7}
    assert captured["mode"] == "task_submit"
    assert captured["consume_on_read"] is True
