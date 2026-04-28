from __future__ import annotations

"""Tests for the V1 task-pool-facing API helpers."""

from types import SimpleNamespace

from pycloud_parallel.controlplane.infocenter_client import InfoCenterNode
from pycloud_parallel.execution.task_pool import TaskPool


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
                _client=SimpleNamespace(close=lambda: None),
            )

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._node_control_client", _FakeNodeControlClient)

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
                _client=SimpleNamespace(close=lambda: None),
            )

    monkeypatch.setattr("pycloud_parallel.execution.task_pool._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.task_pool._node_control_client", _FakeNodeControlClient)

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
