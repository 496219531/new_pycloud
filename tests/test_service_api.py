"""Tests for the V1 service-facing API surface."""

import asyncio
import contextlib
import importlib
import io
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from pycloud_parallel.controlplane.infocenter_client import InfoCenterNode
from pycloud_parallel.execution.service_session import Service


def _build_service_entry_module(tmp_path, monkeypatch):
    package_name = "demo_service_pkg_entry"
    sys.modules.pop(package_name, None)
    sys.modules.pop(f"{package_name}.worker", None)
    sys.modules.pop(f"{package_name}.helper", None)
    package_dir = tmp_path / package_name
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "helper.py").write_text(
        "def normalize(value):\n"
        "    return int(value)\n",
        encoding="utf-8",
    )
    (package_dir / "ignored.csv").write_text("value\n1\n", encoding="utf-8")
    (package_dir / "worker.py").write_text(
        "from .helper import normalize\n\n"
        "def run(value=0, **_kwargs):\n"
        "    return {'value': normalize(value)}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    return importlib.import_module(f"{package_name}.worker")


def _build_service_entry_module_with_resource(tmp_path, monkeypatch):
    worker_module = _build_service_entry_module(tmp_path, monkeypatch)
    package_dir = tmp_path / worker_module.__package__
    (package_dir / "data.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    return worker_module


def test_service_route_summary_reports_fixed_routes():
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
    session = SimpleNamespace(
        service_id="svc-1",
        http_base_url="http://10.0.0.1:18081/svc/svc-1",
    )
    group = Service(
        owner_client_id="owner-1",
        service_name="calc",
        sessions={"node-inst-1": session},
        nodes={"node-inst-1": node},
    )

    assert group.routes() == [
        {
            "node_instance_id": "node-inst-1",
            "node_id": "node-1",
            "control_addr": "10.0.0.1:50061",
            "service_name": "calc",
            "service_id": "svc-1",
            "http_base_url": "http://10.0.0.1:18081/svc/svc-1",
        }
    ]


def test_service_try_compensate_replicas_adds_newly_available_node(monkeypatch):
    from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

    node_1 = SimpleNamespace(
        node_id="node-1",
        node_instance_id="node-inst-1",
        control_addr="127.0.0.1:50061",
        healthy=True,
        schedulable=True,
        drain=False,
        accept_service_deploy=True,
        service_worker_available=2,
        capacity=2,
        queued=0,
        python_version="py3.11",
    )
    node_2 = SimpleNamespace(
        node_id="node-2",
        node_instance_id="node-inst-2",
        control_addr="127.0.0.1:50062",
        healthy=True,
        schedulable=True,
        drain=False,
        accept_service_deploy=True,
        service_worker_available=2,
        capacity=2,
        queued=0,
        python_version="py3.11",
    )
    created = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def list_nodes(self, **_kwargs):
            return [node_1, node_2]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_service_from_bytes(self, **kwargs):
            created.append((self.target, dict(kwargs)))
            return SimpleNamespace(
                service_id=f"svc-{self.target.rsplit(':', 1)[-1]}",
                service_token="token",
                http_base_url=f"http://{self.target}/svc/demo",
                heartbeat_timeout_sec=30,
                worker_count=1,
                status=pb2.SERVICE_STATUS_RUNNING,
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr("pycloud_parallel.execution.service_session._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.service_session._node_control_client", _FakeNodeControlClient)

    group = Service(
        owner_client_id="owner-1",
        service_name="svc-demo",
        sessions={
            "node-inst-1": SimpleNamespace(
                service_id="svc-existing",
                service_token="token",
                http_base_url="http://127.0.0.1:50061/svc/demo",
                heartbeat_timeout_sec=30,
                worker_count=1,
            )
        },
        nodes={"node-inst-1": node_1},
    )
    group._configure_dynamic_compensation(  # noqa: SLF001
        {
            "infocenter_target": "127.0.0.1:50051",
            "blob": b"def run(**_kwargs): return {'ok': True}\n",
            "runtime": "py3",
            "entry_module": "demo_service",
            "entry_callable": "run",
            "package_format": "py",
            "export_mode": "all",
            "export_methods": [],
            "managed_global_names": [],
            "policy_id": "default_safe",
            "worker_count": 1,
            "heartbeat_timeout_sec": 30,
            "idle_ttl_sec": 0,
            "expose_http": True,
            "node_count": 2,
            "node_limit": 10,
            "timeout_sec": 1.0,
        }
    )

    added = group.try_compensate_replicas()

    assert added == 1
    assert set(group.sessions) == {"node-inst-1", "node-inst-2"}
    assert created[0][0] == "127.0.0.1:50062"
    assert created[0][1]["service_name"] == "svc-demo"


def test_service_compensation_uses_active_count_and_skips_failed_node(monkeypatch):
    from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

    node_1 = SimpleNamespace(
        node_id="node-1",
        node_instance_id="node-inst-1",
        control_addr="127.0.0.1:50061",
        healthy=True,
        schedulable=True,
        drain=False,
        accept_service_deploy=True,
        service_worker_available=2,
        capacity=2,
        queued=0,
        python_version="py3.11",
    )
    node_2 = SimpleNamespace(
        node_id="node-2",
        node_instance_id="node-inst-2",
        control_addr="127.0.0.1:50062",
        healthy=True,
        schedulable=True,
        drain=False,
        accept_service_deploy=True,
        service_worker_available=2,
        capacity=2,
        queued=0,
        python_version="py3.11",
    )
    created = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def list_nodes(self, **_kwargs):
            return [node_1, node_2]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_service_from_bytes(self, **kwargs):
            created.append((self.target, dict(kwargs)))
            return SimpleNamespace(
                service_id="svc-new",
                service_token="token",
                http_base_url=f"http://{self.target}/svc/demo",
                heartbeat_timeout_sec=30,
                worker_count=1,
                status=pb2.SERVICE_STATUS_RUNNING,
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr("pycloud_parallel.execution.service_session._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.service_session._node_control_client", _FakeNodeControlClient)

    group = Service(
        owner_client_id="owner-1",
        service_name="svc-demo",
        sessions={
            "node-inst-1": SimpleNamespace(
                service_id="svc-existing",
                service_token="token",
                http_base_url="http://127.0.0.1:50061/svc/demo",
                heartbeat_timeout_sec=30,
                worker_count=1,
                failed=True,
                last_error="ModuleNotFoundError: missing_pkg",
            )
        },
        nodes={"node-inst-1": node_1},
    )
    group._active_replica_ids.discard("node-inst-1")  # noqa: SLF001
    group.failures["node-inst-1"] = "ModuleNotFoundError: missing_pkg"
    group._configure_dynamic_compensation(  # noqa: SLF001
        {
            "infocenter_target": "127.0.0.1:50051",
            "blob": b"def run(**_kwargs): return {'ok': True}\n",
            "runtime": "py3",
            "entry_module": "demo_service",
            "entry_callable": "run",
            "package_format": "py",
            "export_mode": "all",
            "export_methods": [],
            "managed_global_names": [],
            "policy_id": "default_safe",
            "worker_count": 1,
            "heartbeat_timeout_sec": 30,
            "idle_ttl_sec": 0,
            "expose_http": True,
            "node_count": 1,
            "node_limit": 10,
            "timeout_sec": 1.0,
        }
    )

    added = group.try_compensate_replicas()

    assert added == 1
    assert created[0][0] == "127.0.0.1:50062"
    assert "node-inst-1" in group.failures


def test_service_compensation_allows_restarted_node_with_new_instance_id(monkeypatch):
    from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

    old_node = SimpleNamespace(
        node_id="node-1",
        node_instance_id="node-inst-old",
        control_addr="127.0.0.1:50061",
        healthy=False,
        schedulable=False,
        drain=False,
        accept_service_deploy=True,
        service_worker_available=0,
        capacity=2,
        queued=0,
        python_version="py3.11",
    )
    restarted_node = SimpleNamespace(
        node_id="node-1",
        node_instance_id="node-inst-new",
        control_addr="127.0.0.1:50061",
        healthy=True,
        schedulable=True,
        drain=False,
        accept_service_deploy=True,
        service_worker_available=2,
        capacity=2,
        queued=0,
        python_version="py3.11",
    )
    created = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def list_nodes(self, **_kwargs):
            return [restarted_node]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_service_from_bytes(self, **kwargs):
            created.append((self.target, dict(kwargs)))
            return SimpleNamespace(
                service_id="svc-restarted",
                service_token="token",
                http_base_url=f"http://{self.target}/svc/demo",
                heartbeat_timeout_sec=30,
                worker_count=1,
                status=pb2.SERVICE_STATUS_RUNNING,
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr("pycloud_parallel.execution.service_session._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.service_session._node_control_client", _FakeNodeControlClient)

    group = Service(
        owner_client_id="owner-1",
        service_name="svc-demo",
        sessions={
            "node-inst-old": SimpleNamespace(
                service_id="svc-old",
                service_token="token",
                http_base_url="http://127.0.0.1:50061/svc/demo",
                heartbeat_timeout_sec=30,
                worker_count=1,
                failed=True,
                last_error="ModuleNotFoundError: missing_pkg",
            )
        },
        nodes={"node-inst-old": old_node},
    )
    group._active_replica_ids.discard("node-inst-old")  # noqa: SLF001
    group.failures["node-inst-old"] = "ModuleNotFoundError: missing_pkg"
    group._configure_dynamic_compensation(  # noqa: SLF001
        {
            "infocenter_target": "127.0.0.1:50051",
            "blob": b"def run(**_kwargs): return {'ok': True}\n",
            "runtime": "py3",
            "entry_module": "demo_service",
            "entry_callable": "run",
            "package_format": "py",
            "export_mode": "all",
            "export_methods": [],
            "managed_global_names": [],
            "policy_id": "default_safe",
            "worker_count": 1,
            "heartbeat_timeout_sec": 30,
            "idle_ttl_sec": 0,
            "expose_http": True,
            "node_ids": ["node-1"],
            "node_count": 1,
            "node_limit": 10,
            "timeout_sec": 1.0,
        }
    )

    added = group.try_compensate_replicas()

    assert added == 1
    assert created[0][0] == "127.0.0.1:50061"
    assert "node-inst-new" in group.sessions
    assert "node-inst-old" in group.failures


def test_service_compensation_rejects_requested_cordon_or_drain_nodes(monkeypatch):
    node_cordon = SimpleNamespace(
        node_id="node-1",
        node_instance_id="node-inst-cordon",
        control_addr="127.0.0.1:50061",
        healthy=True,
        schedulable=False,
        drain=False,
        accept_service_deploy=True,
        service_worker_available=2,
        capacity=2,
        queued=0,
        python_version="py3.11",
    )
    node_drain = SimpleNamespace(
        node_id="node-2",
        node_instance_id="node-inst-drain",
        control_addr="127.0.0.1:50062",
        healthy=True,
        schedulable=True,
        drain=True,
        accept_service_deploy=True,
        service_worker_available=2,
        capacity=2,
        queued=0,
        python_version="py3.11",
    )
    created = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def list_nodes(self, **_kwargs):
            return [node_cordon, node_drain]

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_service_from_bytes(self, **kwargs):
            created.append((self.target, dict(kwargs)))
            raise AssertionError("cordon/drain nodes must not receive compensation deploy")

        def close(self) -> None:
            return None

    monkeypatch.setattr("pycloud_parallel.execution.service_session._infocenter_client", lambda *args, **kwargs: _FakeInfoCenter())
    monkeypatch.setattr("pycloud_parallel.execution.service_session._node_control_client", _FakeNodeControlClient)

    group = Service(
        owner_client_id="owner-1",
        service_name="svc-demo",
        sessions={},
        nodes={},
        failures={},
    )
    group._configure_dynamic_compensation(  # noqa: SLF001
        {
            "infocenter_target": "127.0.0.1:50051",
            "blob": b"def run(**_kwargs): return {'ok': True}\n",
            "runtime": "py3",
            "entry_module": "demo_service",
            "entry_callable": "run",
            "package_format": "py",
            "export_mode": "all",
            "export_methods": [],
            "managed_global_names": [],
            "policy_id": "default_safe",
            "worker_count": 1,
            "heartbeat_timeout_sec": 30,
            "idle_ttl_sec": 0,
            "expose_http": True,
            "node_ids": ["node-1", "node-2"],
            "node_count": 1,
            "node_limit": 10,
            "timeout_sec": 1.0,
        }
    )

    assert group.try_compensate_replicas() == 0
    assert created == []


def test_service_deploy_from_infocenter_creates_node_services_concurrently(tmp_path):
    from pycloud_parallel.execution.service_session import Service
    from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

    nodes = [
        SimpleNamespace(
            node_id="node-1",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        ),
        SimpleNamespace(
            node_id="node-2",
            control_addr="127.0.0.1:50062",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        ),
    ]
    condition = threading.Condition()
    started = []

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_service_from_bytes(self, **kwargs):
            with condition:
                started.append(self.target)
                condition.notify_all()
                if len(started) < 2:
                    assert condition.wait_for(lambda: len(started) >= 2, timeout=1.0)
            return SimpleNamespace(
                service_id=f"svc-{self.target.rsplit(':', 1)[-1]}",
                service_token="token",
                http_base_url=f"http://{self.target}/svc/demo",
                heartbeat_timeout_sec=30,
                worker_count=int(kwargs["worker_count"]),
                status=pb2.SERVICE_STATUS_RUNNING,
            )

        def close(self) -> None:
            return None

    with patch(
        "pycloud_parallel.execution.service_session._retry_infocenter_request",
        return_value=((), nodes),
    ), patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
        _FakeNodeControlClient,
    ), patch.object(
        Service,
        "_persist_session_cache",
        lambda self: None,
    ), patch.object(
        Service,
        "_start_keepalive",
        lambda self, interval_sec=None: None,
    ):
        group = Service._deploy_from_infocenter(
            infocenter_target="127.0.0.1:50051",
            owner_client_id="owner-demo",
            service_name="svc-concurrent",
            source=b"def run(**_kwargs):\n    return {'ok': True}\n",
            entry_module="svc_concurrent",
            entry_callable="run",
            worker_count=2,
            node_count=2,
            min_success_nodes=2,
            allow_partial=False,
            session_cache_dir=str(tmp_path),
        )

    try:
        assert started == ["127.0.0.1:50061", "127.0.0.1:50062"]
        assert group.node_instance_ids() == ["node-1", "node-2"]
    finally:
        group.close(end_services=False)


def test_service_update_globals_fans_out_to_nodes_concurrently(monkeypatch):
    from pycloud_parallel.execution import service_session as service_session_mod
    from pycloud_parallel.execution.service_session import Service

    condition = threading.Condition()
    started = []
    encoded_calls = []

    class _FakeServiceSession:
        failed = False

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self.service_id = f"svc-{node_id}"
            self.http_base_url = ""

        def update_globals_encoded(self, *, prepared_keys, values=None, transport_values=None):
            assert prepared_keys == ["cfg"]
            assert values is not None or transport_values is not None
            with condition:
                started.append(self.node_id)
                condition.notify_all()
                if len(started) < 2:
                    assert condition.wait_for(lambda: len(started) >= 2, timeout=1.0)
            return SimpleNamespace(globals_digest=f"digest-{self.node_id}")

        def update_globals_prepared(self, *_args, **_kwargs):
            raise AssertionError("service update should use pre-encoded globals")

    monkeypatch.setattr(
        service_session_mod,
        "_prepare_managed_globals_batches_for_upload",
        lambda _clients, values, **_kwargs: ([dict(values)], {
            "globals_batch_count": 1,
            "batch_keys": [["cfg"]],
            "batch_bytes": [0],
            "staged_keys": [],
            "inline_keys": ["cfg"],
        }),
    )
    def _fake_encode_batches(prepared_batches, **_kwargs):
        encoded = []
        for value in prepared_batches:
            encoded_calls.append(dict(value))
            encoded.append((dict(value), {"encoded": dict(value)}, None))
        return encoded

    monkeypatch.setattr(service_session_mod, "_encode_managed_globals_batches", _fake_encode_batches)

    group = Service(
        owner_client_id="owner-demo",
        service_name="svc-update",
        sessions={
            "node-1": _FakeServiceSession("node-1"),
            "node-2": _FakeServiceSession("node-2"),
        },
        nodes={
            "node-1": SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061"),
            "node-2": SimpleNamespace(node_id="node-2", control_addr="127.0.0.1:50062"),
        },
        _clients={"node-1": object(), "node-2": object()},
    )

    digest = group.update_globals({"cfg": {"mode": "fast"}})

    assert digest in {"digest-node-1", "digest-node-2"}
    assert sorted(started) == ["node-1", "node-2"]
    assert len(encoded_calls) == 1


def test_service_startup_uses_nodecontrol_executor_service(tmp_path, monkeypatch):
    module_path = tmp_path / "startup_calc_service.py"
    module_path.write_text(
        "def add(x=0, y=0):\n"
        "    return {'value': int(x) + int(y)}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    node = Service.startup(
        service_name="startup-calc",
        entry_module="startup_calc_service",
        export_methods=("add",),
        bind="",
        worker_count=2,
        start=False,
    )
    try:
        from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState

        assert isinstance(node, NodeControlState)
        assert node.accept_service_deploy is False
        session = next(iter(node._services.values()))  # noqa: SLF001
        assert session.worker_count == 2
        assert session.node_managed is True
        assert node.methods == ["add"]
        assert node.list_methods(include_docs=True)[0]["method"] == "add"

        call_service_kwargs = []
        original_call_service = node.call_service

        def _record_call_service(**kwargs):
            call_service_kwargs.append(dict(kwargs))
            return original_call_service(**kwargs)

        monkeypatch.setattr(node, "call_service", _record_call_service)
        assert node.add.sync(x=1, y=2) == {"value": 3}
        assert call_service_kwargs
        assert call_service_kwargs[0]["service_id"] == session.service_id
        assert call_service_kwargs[0]["service_token"] == session.service_token

        code, body = node.call_service(
            service_id=session.service_id,
            method="add",
            payload={"args": [4], "kwargs": {"y": 5}},
            service_token=session.service_token,
            timeout_sec=5.0,
        )
        assert code == 200
        assert body["data"] == {"value": 9}

        from pycloud_parallel.controlplane.state_time import utc_now
        from datetime import timedelta
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        session.lease_expire_at = utc_now() - timedelta(seconds=1)
        node._handle_service_timeouts()  # noqa: SLF001
        assert session.status == pb2.SERVICE_STATUS_RUNNING
        assert node.service_report_payloads()[0]["service_name"] == "startup-calc"
    finally:
        node.close()


def test_service_startup_local_proxy_calls_executor_without_http(tmp_path, monkeypatch):
    module_path = tmp_path / "startup_local_proxy_service.py"
    module_path.write_text(
        "def add(x=0, y=0):\n"
        "    return {'value': int(x) + int(y)}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    node = Service.startup(
        target="local",
        service_name="startup-local-proxy",
        entry_module="startup_local_proxy_service",
        export_methods=("add",),
        worker_count=1,
    )
    try:
        assert node.service_name == "startup-local-proxy"
        assert node.service_id
        assert node.policy_id
        assert node.service_http_bind == ""
        assert node.add.sync(x=4, y=6) == {"value": 10}
        assert asyncio.run(node.add.broadcast(x=2, y=3))[0][1] == {"value": 5}
    finally:
        node.close()


def test_service_startup_local_proxy_streams_from_executor(tmp_path, monkeypatch):
    module_path = tmp_path / "startup_local_stream_service.py"
    module_path.write_text(
        "def count(limit=3):\n"
        "    for value in range(1, int(limit) + 1):\n"
        "        yield value\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    node = Service.startup(
        target="local",
        service_name="startup-local-stream",
        entry_module="startup_local_stream_service",
        export_methods=("count",),
        worker_count=1,
    )
    try:
        assert list(node.count.stream(limit=3)) == [1, 2, 3]
    finally:
        node.close()


def test_service_deploy_local_returns_direct_proxy(tmp_path, monkeypatch):
    worker_module = _build_service_entry_module(tmp_path, monkeypatch)

    service = Service.deploy(
        target="local",
        service_name="deploy-local-proxy",
        source=worker_module,
        worker_count=1,
    )
    try:
        assert service.service_name == "deploy-local-proxy"
        assert service.service_id
        assert service.owner_client_id.startswith("local-client-")
        assert service.code_version
        assert service.route_summary()[0]["service_name"] == "deploy-local-proxy"
        assert list(service.sessions.keys()) == service.node_instance_ids()
        assert service.node_ids() == ["deploy-local-proxy-local"]
        assert service.failures == {}
        assert service.service_http_bind == ""
        assert service.run.sync(value=7) == {"value": 7}
    finally:
        service.close()


def test_service_connect_local_streams_via_ipc(tmp_path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_LOCAL_IPC_DIR", str(tmp_path / "local-ipc"))
    module_path = tmp_path / "connect_local_stream_service.py"
    module_path.write_text(
        "def run(limit=3):\n"
        "    for value in range(1, int(limit) + 1):\n"
        "        yield value\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    service = Service.deploy(
        target="local",
        service_name="connect-local-stream",
        source=importlib.import_module("connect_local_stream_service"),
        worker_count=1,
    )
    try:
        client = Service.connect(target="local", service_name="connect-local-stream", timeout_sec=5.0)
        try:
            assert list(client.run.stream(limit=4)) == [1, 2, 3, 4]
        finally:
            client.close()
    finally:
        service.close()


def test_service_connect_local_fetches_large_result_dataref_via_ipc(tmp_path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_LOCAL_IPC_DIR", str(tmp_path / "local-ipc"))
    module_path = tmp_path / "connect_local_large_result_service.py"
    module_path.write_text(
        "from pathlib import Path\n"
        "import tempfile\n\n"
        "def run(size=1048576):\n"
        "    path = Path(tempfile.gettempdir()) / 'pycloud-local-large-result.bin'\n"
        "    path.write_bytes(b'x' * int(size))\n"
        "    return path\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    service = Service.deploy(
        target="local",
        service_name="connect-local-large-result",
        source=importlib.import_module("connect_local_large_result_service"),
        worker_count=1,
    )
    try:
        client = Service.connect(target="local", service_name="connect-local-large-result", timeout_sec=5.0)
        try:
            result_path = client.run.sync(size=1024 * 1024 + 17)
            assert isinstance(result_path, Path)
            assert result_path.read_bytes() == b"x" * (1024 * 1024 + 17)
        finally:
            client.close()
    finally:
        service.close()


def test_service_connect_local_handles_many_concurrent_ipc_calls(tmp_path, monkeypatch):
    monkeypatch.setenv("PYCLOUD_LOCAL_IPC_DIR", str(tmp_path / "local-ipc"))
    module_path = tmp_path / "connect_local_many_calls_service.py"
    module_path.write_text(
        "import time\n\n"
        "def run(value=0):\n"
        "    time.sleep(0.01)\n"
        "    return {'value': int(value)}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    service = Service.deploy(
        target="local",
        service_name="connect-local-many-calls",
        source=importlib.import_module("connect_local_many_calls_service"),
        worker_count=1,
    )

    async def _run_many():
        client = Service.connect(target="local", service_name="connect-local-many-calls", timeout_sec=10.0)
        try:
            tasks = [client.run(value=index) for index in range(80)]
            return await asyncio.gather(*tasks)
        finally:
            client.close()

    try:
        assert asyncio.run(_run_many()) == [{"value": index} for index in range(80)]
    finally:
        service.close()


def test_service_connect_local_uses_ipc_registry(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane.local_ipc import local_service_metadata_path

    monkeypatch.setenv("PYCLOUD_LOCAL_IPC_DIR", str(tmp_path / "local-ipc"))
    worker_module = _build_service_entry_module(tmp_path, monkeypatch)

    service = Service.deploy(
        target="local",
        service_name="deploy-local-ipc",
        source=worker_module,
        worker_count=1,
    )
    try:
        assert local_service_metadata_path("deploy-local-ipc").exists()
        client = Service.connect(
            target="local",
            service_name="deploy-local-ipc",
            transport="gateway",
            validate_on_init=True,
        )
        try:
            assert client.run.sync(value=11) == {"value": 11}
            assert asyncio.run(client.run.broadcast(value=12))[0][1] == {"value": 12}
            assert client.route_summary()[0]["control_addr"] == "local"
        finally:
            client.close()
    finally:
        service.close()
    assert not local_service_metadata_path("deploy-local-ipc").exists()


def test_service_connect_local_discards_stale_ipc_registry(tmp_path, monkeypatch):
    import base64
    import json

    from pycloud_parallel.controlplane.local_ipc import LocalServiceClient, local_service_metadata_path

    monkeypatch.setenv("PYCLOUD_LOCAL_IPC_DIR", str(tmp_path / "local-ipc"))
    service_name = "deploy-local-stale-ipc-client"
    socket_path = tmp_path / "stale.sock"
    metadata_path = local_service_metadata_path(service_name)
    metadata_path.write_text(
        json.dumps(
            {
                "version": 1,
                "service_name": service_name,
                "pid": 999999,
                "address": str(socket_path),
                "family": "AF_UNIX",
                "authkey": base64.b64encode(b"0" * 32).decode("ascii"),
                "ipc_token": "stale-token",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="not accepting connections"):
        LocalServiceClient(service_name=service_name, timeout_sec=0.1)
    assert not metadata_path.exists()


def test_service_local_ipc_client_reuses_thread_connection(monkeypatch):
    from pycloud_parallel.controlplane import local_ipc as local_ipc_mod
    from pycloud_parallel.controlplane.local_ipc import LocalServiceClient

    connect_count = 0

    class _FakeConn:
        def __init__(self):
            self.request = {}
            self.closed = False

        def send(self, request):
            self.request = dict(request or {})

        def poll(self, timeout):
            del timeout
            return True

        def recv(self):
            action = self.request.get("action")
            if action == "ping":
                return {"ok": True}
            if action == "list_methods":
                return {"ok": True, "methods": []}
            if action == "get_status":
                return {"ok": True, "routes": []}
            return {"ok": False, "error": f"unexpected action: {action}"}

        def close(self):
            self.closed = True

    def _fake_read_metadata(service_name):
        return {
            "version": 1,
            "service_name": service_name,
            "address": "fake",
            "family": "AF_UNIX",
            "ipc_token": "token-1",
            "authkey": "",
        }

    def _fake_connect(meta):
        nonlocal connect_count
        assert meta["ipc_token"] == "token-1"
        connect_count += 1
        return _FakeConn()

    monkeypatch.setattr(local_ipc_mod, "_read_metadata", _fake_read_metadata)
    monkeypatch.setattr(local_ipc_mod, "_connect_local_service", _fake_connect)

    client = LocalServiceClient(service_name="reuse-test", timeout_sec=1.0)
    try:
        client.list_methods()
        client.get_status()
    finally:
        client.close()

    assert connect_count == 2


def test_service_local_ipc_sends_payload_as_inline_pickle_transport(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane.local_ipc import LocalServiceClient, start_local_service_ipc
    from pycloud_parallel.controlplane.serialization import (
        INTERNAL_PICKLE_NATIVE_V1,
        decode_inline_transport_carrier,
        is_inline_transport_carrier,
    )

    monkeypatch.setenv("PYCLOUD_LOCAL_IPC_DIR", str(tmp_path / "local-ipc"))
    captured = {}

    class _FakeNode:
        node_id = "fake-local-node"
        node_instance_id = "fake-local-node-inst"
        object_dir = tmp_path / "objects"
        methods = ["run"]

        def call_balanced(self, method, payload, **kwargs):
            captured["method"] = method
            captured["payload"] = payload
            captured["kwargs"] = dict(kwargs)
            return "fake-local-node-inst", {"ok": True, "data": {"value": 3}}

    server = start_local_service_ipc(node=_FakeNode(), service_name="local-pickle-transport")
    try:
        client = LocalServiceClient(service_name="local-pickle-transport", timeout_sec=5.0)
        assert client.call(method="run", payload={"x": 1, "y": 2}, serialization_mode="pickle_stable_v1")["data"] == {"value": 3}
    finally:
        server.close()

    assert captured["method"] == "run"
    assert is_inline_transport_carrier(captured["payload"])
    assert captured["payload"]["__pycloud_inline_transport__"]["codec"] == INTERNAL_PICKLE_NATIVE_V1
    assert decode_inline_transport_carrier(captured["payload"], context="service_owner") == {"x": 1, "y": 2}
    with pytest.raises(ValueError, match="trusted internal"):
        decode_inline_transport_carrier(captured["payload"], context="gateway_public")
    assert captured["kwargs"]["serialization_mode"] == "pickle_stable_v1"


def test_service_local_ipc_uses_local_payload_thresholds_for_dataref(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane import config
    from pycloud_parallel.controlplane.local_ipc import LocalServiceClient, start_local_service_ipc
    from pycloud_parallel.controlplane.serialization import decode_inline_transport_carrier
    from pycloud_parallel.data.ref import DataRef

    monkeypatch.setenv("PYCLOUD_LOCAL_IPC_DIR", str(tmp_path / "local-ipc"))
    monkeypatch.setenv("PYCLOUD_LOCAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES", "64")
    monkeypatch.setenv("PYCLOUD_LOCAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES", "16384")
    config.reload_config()
    captured = {}

    class _FakeNode:
        node_id = "fake-local-node"
        node_instance_id = "fake-local-node-inst"
        object_dir = tmp_path / "objects"
        methods = ["run"]

        def call_balanced(self, method, payload, **kwargs):
            captured["payload"] = payload
            return "fake-local-node-inst", {"ok": True, "data": {"value": 3}}

    server = start_local_service_ipc(node=_FakeNode(), service_name="local-dataref-threshold")
    try:
        client = LocalServiceClient(service_name="local-dataref-threshold", timeout_sec=5.0)
        assert client.call(method="run", payload={"blob": b"x" * 4096})["data"] == {"value": 3}
    finally:
        server.close()
        monkeypatch.delenv("PYCLOUD_LOCAL_INLINE_PAYLOAD_SOFT_LIMIT_BYTES", raising=False)
        monkeypatch.delenv("PYCLOUD_LOCAL_INLINE_PAYLOAD_HARD_LIMIT_BYTES", raising=False)
        config.reload_config()

    decoded = decode_inline_transport_carrier(captured["payload"], context="service_owner")
    assert isinstance(decoded["blob"], DataRef)
    assert decoded["blob"].locator_kind == "node_local"
    assert decoded["blob"].node_instance_id == "fake-local-node-inst"


def test_service_local_ipc_fetch_result_data_materializes_dataref_directly(tmp_path, monkeypatch):
    from pycloud_parallel.controlplane import local_ipc
    from pycloud_parallel.controlplane.local_ipc import LocalServiceClient
    from pycloud_parallel.data.ref import DataRef

    client = object.__new__(LocalServiceClient)
    client.service_name = "local-direct-dataref"
    client.timeout_sec = 5.0
    client._meta = {"object_dir": str(tmp_path / "objects")}  # noqa: SLF001

    def _fail_request(*args, **kwargs):
        raise AssertionError("fetch_result_data should not call back through IPC")

    monkeypatch.setattr(client, "_request", _fail_request)
    monkeypatch.setattr(
        local_ipc,
        "_resolve_single_data_ref",
        lambda ref, *, object_dir: {"object_id": ref.object_id, "object_dir": object_dir},
    )
    ref = DataRef(
        ref_id="sha256:" + "a" * 64,
        storage_id="sha256:" + "a" * 64,
        format="bin",
        materialize_as="bytes",
        locator_kind="node_local",
    )

    assert client.fetch_result_data({"data": ref}) == {
        "object_id": "sha256:" + "a" * 64,
        "object_dir": str(tmp_path / "objects"),
    }


def test_service_local_duplicate_service_name_rejected(tmp_path, monkeypatch):
    worker_module = _build_service_entry_module(tmp_path, monkeypatch)
    first = Service.deploy(
        target="local",
        service_name="deploy-local-duplicate",
        source=worker_module,
        worker_count=1,
    )
    try:
        with pytest.raises(RuntimeError, match="local service_name already exists"):
            Service.deploy(
                target="local",
                service_name="deploy-local-duplicate",
                source=worker_module,
                worker_count=1,
            )
    finally:
        first.close()


def test_service_connect_local_across_processes(tmp_path):
    service_name = "deploy-local-cross-process"
    module_path = tmp_path / "local_cross_process_worker.py"
    module_path.write_text(
        "def run(value=0, **_kwargs):\n"
        "    return {'value': int(value) + 100}\n",
        encoding="utf-8",
    )
    server_script = tmp_path / "local_cross_process_server.py"
    server_script.write_text(
        "import importlib, sys\n\n"
        "from pycloud_parallel import Service\n"
        "\n"
        "def main():\n"
        f"    sys.path.insert(0, {str(tmp_path)!r})\n"
        "    importlib.invalidate_caches()\n"
        "    worker = importlib.import_module('local_cross_process_worker')\n"
        f"    svc = Service.deploy(target='local', service_name={service_name!r}, source=worker, worker_count=1)\n"
        "    print('READY', flush=True)\n"
        "    try:\n"
        "        sys.stdin.readline()\n"
        "    finally:\n"
        "        svc.close()\n"
        "\n"
        "if __name__ == '__main__':\n"
        "    main()\n",
        encoding="utf-8",
    )
    env = {
        **os.environ,
        "PYTHONPATH": str((Path(__file__).resolve().parents[1] / "src")),
        "PYCLOUD_LOCAL_IPC_DIR": str(tmp_path / "local-ipc"),
    }
    old_ipc_dir = os.environ.get("PYCLOUD_LOCAL_IPC_DIR")
    os.environ["PYCLOUD_LOCAL_IPC_DIR"] = env["PYCLOUD_LOCAL_IPC_DIR"]
    proc = subprocess.Popen(
        [sys.executable, str(server_script)],
        cwd=str(tmp_path),
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        ready = proc.stdout.readline().strip() if proc.stdout is not None else ""
        if ready != "READY":
            stderr = proc.stderr.read() if proc.stderr is not None else ""
            raise AssertionError(f"local service server did not become ready: stdout={ready!r} stderr={stderr}")
        client = Service.connect(target="local", service_name=service_name, timeout_sec=5.0)
        try:
            assert client.run.sync(value=23) == {"value": 123}
        finally:
            client.close()
    finally:
        if proc.stdin is not None:
            with contextlib.suppress(Exception):
                proc.stdin.write("STOP\n")
                proc.stdin.flush()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        if old_ipc_dir is None:
            os.environ.pop("PYCLOUD_LOCAL_IPC_DIR", None)
        else:
            os.environ["PYCLOUD_LOCAL_IPC_DIR"] = old_ipc_dir


def test_service_local_stale_registry_allows_same_name_restart_after_crash(tmp_path):
    from pycloud_parallel.controlplane.local_ipc import local_service_metadata_path

    service_name = "deploy-local-stale-restart"
    module_path = tmp_path / "local_stale_worker.py"
    module_path.write_text(
        "def run(value=0, **_kwargs):\n"
        "    return {'value': int(value) + 1}\n",
        encoding="utf-8",
    )
    server_script = tmp_path / "local_stale_server.py"
    server_script.write_text(
        "import importlib, sys, time\n\n"
        "from pycloud_parallel import Service\n"
        "\n"
        "def main():\n"
        f"    sys.path.insert(0, {str(tmp_path)!r})\n"
        "    importlib.invalidate_caches()\n"
        "    worker = importlib.import_module('local_stale_worker')\n"
        f"    svc = Service.deploy(target='local', service_name={service_name!r}, source=worker, worker_count=1)\n"
        "    print('READY', flush=True)\n"
        "    while True:\n"
        "        time.sleep(1.0)\n"
        "\n"
        "if __name__ == '__main__':\n"
        "    main()\n",
        encoding="utf-8",
    )
    env = {
        **os.environ,
        "PYTHONPATH": str((Path(__file__).resolve().parents[1] / "src")),
        "PYCLOUD_LOCAL_IPC_DIR": str(tmp_path / "local-ipc"),
    }
    old_ipc_dir = os.environ.get("PYCLOUD_LOCAL_IPC_DIR")
    os.environ["PYCLOUD_LOCAL_IPC_DIR"] = env["PYCLOUD_LOCAL_IPC_DIR"]
    proc = subprocess.Popen(
        [sys.executable, str(server_script)],
        cwd=str(tmp_path),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        ready = proc.stdout.readline().strip() if proc.stdout is not None else ""
        if ready != "READY":
            stderr = proc.stderr.read() if proc.stderr is not None else ""
            raise AssertionError(f"local stale server did not become ready: stdout={ready!r} stderr={stderr}")
        assert local_service_metadata_path(service_name).exists()
        proc.kill()
        proc.wait(timeout=10)
        assert local_service_metadata_path(service_name).exists()

        monkeypatch = pytest.MonkeyPatch()
        try:
            monkeypatch.syspath_prepend(str(tmp_path))
            importlib.invalidate_caches()
            service = Service.deploy(
                target="local",
                service_name=service_name,
                source=importlib.import_module("local_stale_worker"),
                worker_count=1,
            )
            try:
                assert service.run.sync(value=10) == {"value": 11}
            finally:
                service.close()
        finally:
            monkeypatch.undo()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
        if old_ipc_dir is None:
            os.environ.pop("PYCLOUD_LOCAL_IPC_DIR", None)
        else:
            os.environ["PYCLOUD_LOCAL_IPC_DIR"] = old_ipc_dir


def test_service_startup_defaults_to_dynamic_http_bind(tmp_path, monkeypatch):
    module_path = tmp_path / "startup_dynamic_bind_service.py"
    module_path.write_text("def ping():\n    return {'ok': True}\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    node = Service.startup(
        service_name="startup-dynamic",
        entry_module="startup_dynamic_bind_service",
        start=False,
    )

    try:
        assert node.service_http_bind == "0.0.0.0:0"
    finally:
        node.close()


def test_service_startup_installs_process_interrupt_shutdown(tmp_path, monkeypatch):
    module_path = tmp_path / "startup_interrupt_service.py"
    module_path.write_text("def ping():\n    return {'ok': True}\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    calls = []

    def _fake_install(self):
        calls.append(self.node_id)

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.node_runtime_base.NodeRuntimeBase.install_interrupt_shutdown_handlers",
        _fake_install,
    )

    node = Service.startup(
        service_name="startup-interrupt",
        entry_module="startup_interrupt_service",
        node_id="startup-interrupt-node",
        start=False,
    )
    try:
        assert calls == ["startup-interrupt-node"]
    finally:
        node.close()


def test_service_startup_same_endpoint_binds_before_infocenter_register(tmp_path, monkeypatch):
    from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

    module_path = tmp_path / "startup_same_endpoint_service.py"
    module_path.write_text("def ping():\n    return {'ok': True}\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    events = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def list_service_routes(self, **kwargs):
            assert kwargs["service_name"] == "startup-same-endpoint"
            return [
                SimpleNamespace(
                    service_name="startup-same-endpoint",
                    service_id="svc-old",
                    status=pb2.SERVICE_STATUS_RUNNING,
                    node_healthy=True,
                    node_id="startup-old",
                    node_instance_id="startup-old",
                    control_addr="",
                    http_base_url="http://127.0.0.1:18081/svc/svc-old",
                )
            ]

    def _fake_infocenter(*_args, **_kwargs):
        return _FakeInfoCenter()

    def _fake_start_gateway(self):
        if not self.service_http_bind:
            return
        events.append("bind")
        self.service_http_base_url = "http://127.0.0.1:18081"

    def _fake_start_registration(self, **_kwargs):
        events.append("register")

    monkeypatch.setattr("pycloud_parallel.execution.service_session._infocenter_client", _fake_infocenter)
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.nodecontrol_state.NodeControlState.start_node_service_gateway",
        _fake_start_gateway,
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.nodecontrol_state.NodeControlState.start_infocenter_registration",
        _fake_start_registration,
    )

    node = Service.startup(
        target="127.0.0.1:50051",
        service_name="startup-same-endpoint",
        entry_module="startup_same_endpoint_service",
        bind="127.0.0.1:18081",
    )
    try:
        assert events == ["bind", "register"]
    finally:
        node.close()


def test_service_startup_different_endpoint_fails_before_bind(tmp_path, monkeypatch):
    from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

    module_path = tmp_path / "startup_other_endpoint_service.py"
    module_path.write_text("def ping():\n    return {'ok': True}\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    events = []

    class _FakeInfoCenter:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def list_service_routes(self, **_kwargs):
            return [
                SimpleNamespace(
                    service_name="startup-other-endpoint",
                    service_id="svc-old",
                    status=pb2.SERVICE_STATUS_RUNNING,
                    node_healthy=True,
                    node_id="startup-old",
                    node_instance_id="startup-old",
                    control_addr="",
                    http_base_url="http://127.0.0.1:18082/svc/svc-old",
                )
            ]

    monkeypatch.setattr("pycloud_parallel.execution.service_session._infocenter_client", lambda *_args, **_kwargs: _FakeInfoCenter())
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.nodecontrol_state.NodeControlState.start_node_service_gateway",
        lambda self: events.append("bind") if self.service_http_bind else None,
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.nodecontrol_state.NodeControlState.start_infocenter_registration",
        lambda self, **_kwargs: events.append("register"),
    )

    with pytest.raises(RuntimeError, match="different endpoint"):
        Service.startup(
            target="127.0.0.1:50051",
            service_name="startup-other-endpoint",
            entry_module="startup_other_endpoint_service",
            bind="127.0.0.1:18081",
        )

    assert events == []


def test_service_startup_http_gateway_uses_large_accept_backlog(tmp_path, monkeypatch):
    module_path = tmp_path / "startup_backlog_service.py"
    module_path.write_text("def ping():\n    return {'ok': True}\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    node = Service.startup(
        service_name="startup-backlog",
        entry_module="startup_backlog_service",
        bind="127.0.0.1:0",
    )
    try:
        server = node._service_http_gateway._server  # noqa: SLF001
        assert server is not None
        assert server.request_queue_size >= 1024
    finally:
        node.close()


def test_service_startup_http_gateway_serves_data_refs(tmp_path, monkeypatch):
    module_path = tmp_path / "startup_http_dataref_service.py"
    module_path.write_text("def ping():\n    return {'ok': True}\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    node = Service.startup(
        service_name="startup-http-dataref",
        entry_module="startup_http_dataref_service",
        bind="127.0.0.1:0",
    )
    try:
        from pycloud_parallel.data.ref import DataRef
        from pycloud_parallel.controlplane.discovery_client import DiscoveryServiceClient

        session = next(iter(node._services.values()))  # noqa: SLF001
        source = tmp_path / "payload.txt"
        source.write_text("startup-http-result", encoding="utf-8")
        artifact = node.data_store.store_path(source)
        ref = DataRef(
            ref_id=artifact.object_id,
            storage_id=artifact.object_id,
            logical_type="text",
            format=artifact.format,
            size_bytes=artifact.size_bytes,
            materialize_as="text",
            locator_kind="service_http",
            locator_token=session.http_base_url,
            node_id=node.node_id,
        )

        with DiscoveryServiceClient("127.0.0.1:1", timeout_sec=1.0) as client:
            assert client.fetch_result_data({"data": ref}) == "startup-http-result"
    finally:
        node.close()


def test_service_startup_registers_infocenter_when_target_is_set(tmp_path, monkeypatch):
    module_path = tmp_path / "startup_registered_service.py"
    module_path.write_text("def ping():\n    return {'ok': True}\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    calls = []

    def _fake_start_infocenter_registration(self, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.nodecontrol_state.NodeControlState.start_infocenter_registration",
        _fake_start_infocenter_registration,
    )

    node = Service.startup(
        target="127.0.0.1:50051",
        service_name="startup-registered",
        entry_module="startup_registered_service",
        worker_count=3,
        policy_id="trusted_internal",
        start=False,
    )

    try:
        assert node.service_worker_capacity == 3
        session = next(iter(node._services.values()))  # noqa: SLF001
        assert session.policy_id == "trusted_internal"
        assert session.node_managed is True
        assert calls == [
                {
                    "infocenter_target": "127.0.0.1:50051",
                    "control_addr": "",
                    "queue_capacity": 0,
                    "tags": None,
                    "version": "",
                    "metadata": {
                        "service_name": "startup-registered",
                        "entry_module": "startup_registered_service",
                },
                "heartbeat_sec": 10,
                "rpc_timeout_sec": 5.0,
            }
        ]
        assert node.close_on_registration_lost is True
    finally:
        node.close()


def test_service_startup_update_globals_uses_service_executor_path(tmp_path, monkeypatch):
    module_path = tmp_path / "startup_globals_service.py"
    module_path.write_text(
        "cfg = None\n"
        "def read_cfg():\n"
        "    return cfg\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    node = Service.startup(
        service_name="startup-globals",
        entry_module="startup_globals_service",
        export_methods=("read_cfg",),
        managed_global_names=("cfg",),
        start=False,
    )

    try:
        session = next(iter(node._services.values()))  # noqa: SLF001
        first_digest = node.update_globals({"cfg": {"value": 42}})
        code, body = node.call_service(
            service_id=session.service_id,
            method="read_cfg",
            payload={},
            service_token=session.service_token,
            timeout_sec=5.0,
        )
        assert code == 200
        assert body["data"] == {"value": 42}
        assert first_digest
        assert node.globals_digests == {session.service_id: first_digest}
    finally:
        node.close()


def test_service_startup_update_globals_recreates_missing_service_executor(tmp_path, monkeypatch):
    module_path = tmp_path / "startup_globals_recreate_service.py"
    module_path.write_text(
        "cfg = None\n"
        "def read_cfg():\n"
        "    return cfg\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    node = Service.startup(
        service_name="startup-globals-recreate",
        entry_module="startup_globals_recreate_service",
        export_methods=("read_cfg",),
        managed_global_names=("cfg",),
        start=False,
    )

    try:
        session = next(iter(node._services.values()))  # noqa: SLF001
        node._executor_host.stop_service(service_id=session.service_id)  # noqa: SLF001

        digest = node.update_globals({"cfg": {"value": 43}})
        code, body = node.call_service(
            service_id=session.service_id,
            method="read_cfg",
            payload={},
            service_token=session.service_token,
            timeout_sec=5.0,
        )

        assert digest
        assert code == 200
        assert body["data"] == {"value": 43}
    finally:
        node.close()


def test_connected_service_async_calls_are_gated():
    from pycloud_parallel.execution.service_session import _ConnectedService

    service = _ConnectedService.__new__(_ConnectedService)
    service._async_call_gate = None
    service._async_call_gate_loop = None
    service._async_call_gate_capacity = 0
    service._default_max_in_flight = lambda: 2
    active = 0
    max_active = 0
    lock = threading.Lock()

    def _call_balanced(method, payload, **kwargs):
        nonlocal active, max_active
        del method, payload, kwargs
        with lock:
            active += 1
            max_active = max(max_active, active)
        try:
            time.sleep(0.05)
            return "node-1", {"ok": True}
        finally:
            with lock:
                active -= 1

    service.call_balanced = _call_balanced

    async def _run():
        await asyncio.gather(
            *[
                service.acall_balanced("run", {}, timeout_sec=1.0, refresh_status=False)
                for _ in range(8)
            ]
        )

    asyncio.run(_run())
    assert max_active == 2


def test_connected_service_does_not_expose_owner_update_globals():
    from pycloud_parallel.execution.service_session import _ConnectedService

    service = _ConnectedService.__new__(_ConnectedService)
    service._discovered_methods = ["update_globals"]

    with pytest.raises(AttributeError, match="only available on owner service handles"):
        _ = service.update_globals


class TestCallProxy:
    """测试 _CallProxy 类。"""

    def test_repr(self):
        """测试 __repr__ 方法。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group)

        assert "square" in repr(proxy)

    def test_method_property(self):
        """测试 method 属性。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("fibonacci", mock_group)

        assert proxy.method == "fibonacci"

    def test_sync_property(self):
        """测试 sync 属性。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy, _SyncCallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group)

        sync_proxy = proxy.sync

        assert isinstance(sync_proxy, _SyncCallProxy)
        assert sync_proxy._method == "square"

    def test_broadcast_property(self):
        """测试 broadcast 属性。"""
        from pycloud_parallel.execution.call_proxy import _BroadcastProxy, _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group)

        broadcast_proxy = proxy.broadcast

        assert isinstance(broadcast_proxy, _BroadcastProxy)
        assert broadcast_proxy._method == "square"

    def test_with_options(self):
        """测试 with_options 方法。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group, timeout_sec=60.0)

        new_proxy = proxy.with_options(timeout_sec=30.0, strategy="round_robin")

        assert new_proxy._timeout_sec == 30.0
        assert new_proxy._strategy == "round_robin"
        assert new_proxy._method == "square"

    def test_with_options_accepts_service_latency_profile(self):
        """测试 with_options 支持显式 service profile 名称。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group, timeout_sec=60.0)

        new_proxy = proxy.with_options(strategy="service_latency_first")

        assert new_proxy._strategy == "service_latency_first"

    def test_map_delegates_to_group_batch_map(self):
        """测试 map 会委托给 group.map_calls。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        mock_group.map_calls = MagicMock(return_value=[{"value": 1}, {"value": 4}])
        proxy = _CallProxy("square", mock_group)

        result = proxy.map([1, 2], arg_name="x")

        assert result == [{"value": 1}, {"value": 4}]
        mock_group.map_calls.assert_called_once()

    def test_amap_delegates_to_group_async_batch_map(self):
        """测试 amap 会委托给 group.amap_calls。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = AsyncMock()
        mock_group.amap_calls = AsyncMock(return_value=[{"value": 1}, {"value": 4}])
        proxy = _CallProxy("square", mock_group)

        async def _run():
            return await proxy.amap([1, 2], arg_name="x")

        result = asyncio.run(_run())

        assert result == [{"value": 1}, {"value": 4}]
        mock_group.amap_calls.assert_awaited_once()

    def test_unordered_returns_stream_object(self):
        """测试 unordered 返回同步可迭代流对象。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group)

        stream = proxy.unordered([{"x": 1}, {"x": 2}], max_in_flight=2)

        assert hasattr(stream, "__iter__")
        assert not hasattr(stream, "__aiter__")

    def test_aunordered_returns_async_iterable_stream_object(self):
        """测试 aunordered 返回异步可迭代流对象。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group)

        stream = proxy.aunordered([{"x": 1}, {"x": 2}], max_in_flight=2)

        assert hasattr(stream, "__aiter__")
        assert not hasattr(stream, "__iter__")

    def test_iter_items_returns_sync_iterable_stream_object(self):
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group)

        stream = proxy.iter_items([{"x": 1}, {"x": 2}], max_in_flight=2)

        assert hasattr(stream, "__iter__")
        assert not hasattr(stream, "__aiter__")

    def test_aiter_items_returns_async_iterable_stream_object(self):
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        proxy = _CallProxy("square", mock_group)

        stream = proxy.aiter_items([{"x": 1}, {"x": 2}], max_in_flight=2)

        assert hasattr(stream, "__aiter__")
        assert not hasattr(stream, "__iter__")

    def test_collect_items_delegates_to_group_collect_item_calls(self):
        from pycloud_parallel.execution.base import ExecutionItem
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        mock_group.collect_item_calls = MagicMock(return_value=[ExecutionItem(index=0, ok=True, result={"value": 1})])
        proxy = _CallProxy("square", mock_group)

        result = proxy.collect_items([{"x": 1}])

        assert len(result) == 1
        assert result[0].result == {"value": 1}
        mock_group.collect_item_calls.assert_called_once()

    def test_map_uses_dynamic_default_max_in_flight_when_not_explicit(self):
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = MagicMock()
        mock_group.map_calls = MagicMock(return_value=[{"value": 1}, {"value": 4}])
        proxy = _CallProxy("square", mock_group)

        result = proxy.map([1, 2], arg_name="x")

        assert result == [{"value": 1}, {"value": 4}]
        assert mock_group.map_calls.call_args.kwargs["max_in_flight"] is None

    def test_acollect_items_delegates_to_group_acollect_item_calls(self):
        from pycloud_parallel.execution.base import ExecutionItem
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = AsyncMock()
        mock_group.acollect_item_calls = AsyncMock(return_value=[ExecutionItem(index=0, ok=True, result={"value": 1})])
        proxy = _CallProxy("square", mock_group)

        async def _run():
            return await proxy.acollect_items([{"x": 1}])

        result = asyncio.run(_run())

        assert len(result) == 1
        assert result[0].result == {"value": 1}
        mock_group.acollect_item_calls.assert_awaited_once()


def test_service_iter_item_calls_uses_group_dynamic_default_max_in_flight():
    from pycloud_parallel.execution.service_session import _service_iter_item_calls

    class _Group:
        def _default_max_in_flight(self):
            return 3

        def call_balanced(self, method, payload, *, timeout_sec, strategy, refresh_status):  # noqa: ARG002
            return "node-1", {"data": payload}

    items = list(
        _service_iter_item_calls(
            _Group(),
            method="square",
            payloads=[{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}],
            timeout_sec=30.0,
            strategy="predicted_busy",
            refresh_status=True,
            max_in_flight=None,
        )
    )

    assert len(items) == 4


def test_service_iter_item_calls_submits_streaming_window_only():
    from pycloud_parallel.execution.service_session import _service_iter_item_calls

    produced = []
    started = []
    release = threading.Event()

    def _payloads():
        for idx in range(5):
            produced.append(idx)
            yield {"x": idx}

    class _Group:
        def call_balanced(self, method, payload, *, timeout_sec, strategy, refresh_status):  # noqa: ARG002
            started.append(int(payload["x"]))
            release.wait(timeout=5.0)
            return "node-1", {"data": payload}

    iterator = _service_iter_item_calls(
        _Group(),
        method="square",
        payloads=_payloads(),
        timeout_sec=30.0,
        strategy="predicted_busy",
        refresh_status=True,
        max_in_flight=2,
    )
    results = []
    thread = threading.Thread(target=lambda: results.append(next(iterator)), daemon=True)
    thread.start()
    deadline = time.time() + 2.0
    while len(started) < 2 and time.time() < deadline:
        time.sleep(0.01)

    assert produced == [0, 1]
    release.set()
    thread.join(timeout=2.0)
    assert len(results) == 1
    assert produced == [0, 1]

    def test_async_call(self):
        """测试异步调用。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = AsyncMock()
        mock_group.acall_balanced = AsyncMock(return_value=("node1", {"data": {"result": 49}}))
        proxy = _CallProxy("square", mock_group)

        async def test():
            result = await proxy(x=7)
            # resp.get("data", resp) 返回 {"result": 49}
            assert result == {"result": 49}
            mock_group.acall_balanced.assert_called_once_with(
                "square",
                {"x": 7},
                timeout_sec=60.0,
                strategy="predicted_busy",
                refresh_status=True,
            )

        asyncio.run(test())

    def test_await_syntax(self):
        """测试 await 语法。"""
        from pycloud_parallel.execution.call_proxy import _CallProxy

        mock_group = AsyncMock()
        mock_group.acall_balanced = AsyncMock(return_value=("node1", {"data": {"y": 100}}))
        proxy = _CallProxy("square", mock_group)

        async def test():
            result = await proxy(x=10)
            assert result == {"y": 100}

        asyncio.run(test())


class TestSyncCallProxy:
    """测试 _SyncCallProxy 类。"""

    def test_repr(self):
        """测试 __repr__ 方法。"""
        from pycloud_parallel.execution.call_proxy import _SyncCallProxy

        mock_group = MagicMock()
        proxy = _SyncCallProxy("square", mock_group)

        assert "square" in repr(proxy)

    def test_sync_call(self):
        """测试同步调用。"""
        from pycloud_parallel.execution.call_proxy import _SyncCallProxy

        mock_group = MagicMock()
        mock_group.call_balanced = MagicMock(return_value=("node1", {"data": {"result": 64}}))
        proxy = _SyncCallProxy("square", mock_group)

        result = proxy(x=8)

        assert result == {"result": 64}
        mock_group.call_balanced.assert_called_once()


class TestBroadcastProxy:
    """测试 _BroadcastProxy 类。"""

    def test_repr(self):
        """测试 __repr__ 方法。"""
        from pycloud_parallel.execution.call_proxy import _BroadcastProxy

        mock_group = MagicMock()
        proxy = _BroadcastProxy("square", mock_group)

        assert "square" in repr(proxy)

    def test_async_broadcast(self):
        """测试异步广播调用。"""
        from pycloud_parallel.execution.call_proxy import _BroadcastProxy

        mock_group = AsyncMock()
        mock_results = [
            ("node1", {"data": {"result": 49}}, None),
            ("node2", {"data": {"result": 49}}, None),
        ]
        mock_group.acall_all = AsyncMock(return_value=mock_results)
        proxy = _BroadcastProxy("square", mock_group)

        async def test():
            results = await proxy(x=7)
            assert len(results) == 2
            assert results[0][1] == {"result": 49}

        asyncio.run(test())


class TestOwnerServiceFacade:
    """测试 V1 owner service facade。"""

    def test_legacy_deploy_from_bytes_facade_removed(self):
        from pycloud_parallel import Service as OwnerServiceFacade

        assert not hasattr(OwnerServiceFacade, "deploy_from_bytes")

    def test_deploy_forwards_replace_changed_code_option(self):
        from pycloud_parallel import Service as OwnerServiceFacade

        sentinel = object()
        with patch("pycloud_parallel.execution.service_session.Service._deploy_from_infocenter", return_value=sentinel) as mocked:
            result = OwnerServiceFacade.deploy(
                target="127.0.0.1:50051",
                source=b"def run(**_kwargs):\n    return {'ok': True}\n",
                service_name="demo-service",
                replace_existing_if_code_changed=False,
            )

        assert result is sentinel
        assert mocked.call_args.kwargs["replace_existing_if_code_changed"] is False

    def test_service_session_cache_lock_rejects_second_local_owner(self, tmp_path):
        """测试同一个 session cache 文件不能被第二个本地 deploy 进程持有。"""
        from pycloud_parallel.execution.service_session import _ServiceSessionFileLock

        path = tmp_path / "owner" / "svc.json"
        first = _ServiceSessionFileLock(path).acquire()
        try:
            with pytest.raises(RuntimeError, match="already holds cache lock|already active"):
                _ServiceSessionFileLock(path).acquire()
        finally:
            first.close()

        second = _ServiceSessionFileLock(path).acquire()
        second.close()

    def test_getattr_creates_proxy(self):
        """测试 __getattr__ 创建代理。"""
        from pycloud_parallel import Service as OwnerServiceFacade
        from pycloud_parallel.execution.call_proxy import _CallProxy
        from unittest.mock import MagicMock

        # 模拟有方法的 session
        mock_session = MagicMock()
        mock_method_info = MagicMock()
        mock_method_info.method = "square"
        mock_session.list_methods.return_value = [mock_method_info]

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={"node1": mock_session},
            nodes={"node1": MagicMock()},
        )
        group._discovered_methods = None

        proxy = group.square

        assert isinstance(proxy, _CallProxy)
        assert proxy._method == "square"
        assert proxy._strategy == "predicted_busy"

    def test_getattr_with_empty_methods_raises(self):
        """测试当方法列表为空时，访问任何方法都应该报错。"""
        from pycloud_parallel import Service as OwnerServiceFacade
        from unittest.mock import MagicMock

        # 模拟返回空方法列表的 session
        mock_session = MagicMock()
        mock_session.list_methods.return_value = []

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={"node1": mock_session},
            nodes={"node1": MagicMock()},
        )
        group._discovered_methods = None

        # 当列表为空时，访问任何方法都应该报错
        with pytest.raises(AttributeError, match="has no method 'square'"):
            _ = group.square

    def test_getattr_with_discovered_methods(self):
        """测试已发现方法时的 __getattr__。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={},
            nodes={},
        )
        group._discovered_methods = ["square", "fibonacci"]

        proxy = group.square
        assert proxy._method == "square"

    def test_getattr_unknown_method_raises(self):
        """测试访问已知列表中不存在的方法时抛出异常。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={},
            nodes={},
        )
        # 设置已知的非空方法列表
        group._discovered_methods = ["square", "fibonacci"]

        # 当列表非空且包含已知方法时，访问未知方法应该报错
        with pytest.raises(AttributeError, match="has no method 'unknown'"):
            _ = group.unknown

    def test_getattr_private_raises(self):
        """测试访问私有属性时抛出异常。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={},
            nodes={},
        )

        with pytest.raises(AttributeError):
            _ = group._private

    def test_methods_property(self):
        """测试 methods 属性。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={},
            nodes={},
        )
        group._discovered_methods = ["square", "fibonacci"]

        assert group.methods == ["square", "fibonacci"]

    def test_repr(self):
        """测试 __repr__ 方法。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="compute-service",
            sessions={"node1": MagicMock()},
            nodes={"node1": MagicMock()},
        )
        group._discovered_methods = ["square", "fibonacci"]

        repr_str = repr(group)

        assert "compute-service" in repr_str
        assert "square" in repr_str

    def test_repr_not_discovered(self):
        """测试未发现方法时的 __repr__。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="compute-service",
            sessions={},
            nodes={},
        )
        group._discovered_methods = None

        repr_str = repr(group)

        assert "compute-service" in repr_str

    def test_async_call_interface(self):
        """测试异步 call 接口。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={},
            nodes={},
        )
        group.acall_balanced = AsyncMock(return_value=("node1", {"data": {"result": 100}}))

        async def test():
            result = await group.call("square", x=10)
            assert result == {"result": 100}

        asyncio.run(test())

    def test_sync_call_interface(self):
        """测试同步 call_sync 接口。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={},
            nodes={},
        )
        group.call_balanced = MagicMock(return_value=("node1", {"data": {"result": 100}}))

        result = group.call_sync("square", x=10)
        assert result == {"result": 100}

    def test_deploy_from_infocenter_emits_message_when_no_nodes(self, capsys):
        from pycloud_parallel.execution.service_session import Service

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=((), []),
        ):
            with pytest.raises(RuntimeError, match="no available nodes from InfoCenter"):
                Service._deploy_from_infocenter(
                    infocenter_target="127.0.0.1:50051",
                    owner_client_id="owner-demo",
                    service_name="demo-service",
                    source=b"def run(**_kwargs):\n    return {'ok': True}\n",
                    entry_module="demo_service",
                    entry_callable="run",
                )

        err = capsys.readouterr().err
        assert "[Service] deploy start" in err
        assert "[Service] deploy failed: no available nodes" in err
        assert "127.0.0.1:50051" in err

    def test_deploy_from_infocenter_emits_success_message(self, tmp_path, capsys):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        fake_node = SimpleNamespace(
            node_id="node-1",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **_kwargs):
                return SimpleNamespace(
                    service_id="svc-1",
                    service_token="token-1",
                    http_base_url="http://127.0.0.1:18081/svc/svc-1",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=((), [fake_node]),
        ), patch(
            "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_persist_session_cache",
            lambda self: None,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service._deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-service",
                source=b"def run(**_kwargs):\n    return {'ok': True}\n",
                entry_module="demo_service",
                entry_callable="run",
                session_cache_dir=str(tmp_path),
            )

        err = capsys.readouterr().err
        assert "[Service] deploy start" in err
        assert "[Service] deploy success service_name=demo-service routes=" in err
        assert "node-1/node-1@127.0.0.1:50061(service_id=svc-1, http=http://127.0.0.1:18081/svc/svc-1)" in err
        for client in group._clients.values():  # noqa: SLF001
            client.close()

    def test_deploy_from_infocenter_filters_nodes_that_do_not_accept_service_deploy(self, tmp_path):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        deploy_node = SimpleNamespace(
            node_id="node-1",
            node_instance_id="node-1-inst",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            accept_service_deploy=True,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )
        job_node = SimpleNamespace(
            node_id="job-orchestrator-01",
            node_instance_id="job-orchestrator-01-inst",
            control_addr="",
            healthy=True,
            schedulable=True,
            drain=False,
            accept_service_deploy=False,
            service_worker_available=0,
            capacity=1,
            queued=0,
            python_version="py3.11",
        )
        targets = []

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec
                targets.append(target)

            def create_service_from_bytes(self, **_kwargs):
                return SimpleNamespace(
                    service_id="svc-1",
                    service_token="token-1",
                    http_base_url="http://127.0.0.1:18081/svc/svc-1",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=((), [deploy_node, job_node]),
        ), patch(
            "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_persist_session_cache",
            lambda self: None,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service._deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-service",
                source=b"def run(**_kwargs):\n    return {'ok': True}\n",
                entry_module="demo_service",
                entry_callable="run",
                node_count=2,
                session_cache_dir=str(tmp_path),
            )

        assert targets == ["127.0.0.1:50061"]
        assert list(group.nodes.keys()) == ["node-1-inst"]
        for client in group._clients.values():  # noqa: SLF001
            client.close()

    def test_deploy_from_infocenter_auto_keeps_duplicate_node_ids_by_instance(self, tmp_path):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        nodes = [
            SimpleNamespace(
                node_id="node-1",
                node_instance_id="node-1-a",
                control_addr="127.0.0.1:50061",
                healthy=True,
                schedulable=True,
                drain=False,
                accept_service_deploy=True,
                service_worker_available=2,
                capacity=2,
                queued=0,
                python_version="py3.11",
            ),
            SimpleNamespace(
                node_id="node-1",
                node_instance_id="node-1-b",
                control_addr="127.0.0.1:50062",
                healthy=True,
                schedulable=True,
                drain=False,
                accept_service_deploy=True,
                service_worker_available=2,
                capacity=2,
                queued=0,
                python_version="py3.11",
            ),
        ]
        targets = []

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec
                targets.append(target)

            def create_service_from_bytes(self, **_kwargs):
                return SimpleNamespace(
                    service_id=f"svc-{self.target.rsplit(':', 1)[-1]}",
                    service_token="token",
                    http_base_url=f"http://{self.target}/svc/demo",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=((), nodes),
        ), patch(
            "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_persist_session_cache",
            lambda self: None,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service._deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-service",
                source=b"def run(**_kwargs):\n    return {'ok': True}\n",
                entry_module="demo_service",
                entry_callable="run",
                node_count=2,
                min_success_nodes=2,
                allow_partial=False,
                session_cache_dir=str(tmp_path),
            )

        assert targets == ["127.0.0.1:50061", "127.0.0.1:50062"]
        assert list(group.nodes.keys()) == ["node-1-a", "node-1-b"]
        for client in group._clients.values():  # noqa: SLF001
            client.close()

    def test_deploy_from_infocenter_retries_briefly_until_nodes_register(self, tmp_path):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        fake_node = SimpleNamespace(
            node_id="node-1",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )
        discovery_calls = {"count": 0}

        class _FakeInfoCenter:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return None

            def list_service_routes(self, **_kwargs):
                return []

            def list_nodes(self, **_kwargs):
                discovery_calls["count"] += 1
                if discovery_calls["count"] == 1:
                    return []
                return [fake_node]

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **_kwargs):
                return SimpleNamespace(
                    service_id="svc-1",
                    service_token="token-1",
                    http_base_url="http://127.0.0.1:18081/svc/svc-1",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._infocenter_client",
            return_value=_FakeInfoCenter(),
        ), patch(
            "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_persist_session_cache",
            lambda self: None,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ), patch(
            "pycloud_parallel.execution.service_session.time.sleep",
            return_value=None,
        ) as mocked_sleep:
            group = Service._deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-retry-service",
                source=b"def run(**_kwargs):\n    return {'ok': True}\n",
                entry_module="demo_service",
                entry_callable="run",
                timeout_sec=1.0,
                session_cache_dir=str(tmp_path),
            )

        try:
            assert discovery_calls["count"] == 2
            mocked_sleep.assert_called()
            assert list(group.sessions.keys()) == ["node-1"]
        finally:
            for client in group._clients.values():  # noqa: SLF001
                client.close()

    def test_deploy_from_infocenter_packages_module_object_entry_module(self, tmp_path, monkeypatch):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        worker_module = _build_service_entry_module(tmp_path, monkeypatch)
        fake_node = SimpleNamespace(
            node_id="node-1",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )
        create_calls = []

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **kwargs):
                create_calls.append(dict(kwargs))
                return SimpleNamespace(
                    service_id="svc-1",
                    service_token="token-1",
                    http_base_url="http://127.0.0.1:18081/svc/svc-1",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=((), [fake_node]),
        ), patch(
            "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_persist_session_cache",
            lambda self: None,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service._deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-module-service",
                source=worker_module,
                session_cache_dir=str(tmp_path),
            )

        try:
            assert len(create_calls) == 1
            create_call = create_calls[0]
            assert create_call["entry_module"] == worker_module.__name__
            assert create_call["package_format"] == "tar.gz"
            with tarfile.open(fileobj=io.BytesIO(create_call["blob"]), mode="r:gz") as tar:
                names = set(tar.getnames())
            assert f"{worker_module.__package__}/__init__.py" in names
            assert f"{worker_module.__package__}/worker.py" in names
            assert f"{worker_module.__package__}/helper.py" in names
            assert f"{worker_module.__package__}/ignored.csv" not in names
        finally:
            for client in group._clients.values():  # noqa: SLF001
                client.close()

    def test_deploy_from_infocenter_includes_only_explicit_resource_paths(self, tmp_path, monkeypatch):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        worker_module = _build_service_entry_module_with_resource(tmp_path, monkeypatch)
        fake_node = SimpleNamespace(
            node_id="node-1",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )
        create_calls = []

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **kwargs):
                create_calls.append(dict(kwargs))
                return SimpleNamespace(
                    service_id="svc-1",
                    service_token="token-1",
                    http_base_url="http://127.0.0.1:18081/svc/svc-1",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=((), [fake_node]),
        ), patch(
            "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_persist_session_cache",
            lambda self: None,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service._deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-module-service-resource",
                source=worker_module,
                resource_paths=["data.csv"],
                session_cache_dir=str(tmp_path),
            )

        try:
            create_call = create_calls[0]
            with tarfile.open(fileobj=io.BytesIO(create_call["blob"]), mode="r:gz") as tar:
                names = set(tar.getnames())
            assert f"{worker_module.__package__}/data.csv" in names
        finally:
            for client in group._clients.values():  # noqa: SLF001
                client.close()

    def test_deploy_from_infocenter_packages_callable_object_entry_callable(self, tmp_path, monkeypatch):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        worker_module = _build_service_entry_module(tmp_path, monkeypatch)
        fake_node = SimpleNamespace(
            node_id="node-1",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )
        create_calls = []

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **kwargs):
                create_calls.append(dict(kwargs))
                return SimpleNamespace(
                    service_id="svc-1",
                    service_token="token-1",
                    http_base_url="http://127.0.0.1:18081/svc/svc-1",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=((), [fake_node]),
        ), patch(
            "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_persist_session_cache",
            lambda self: None,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service._deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-callable-service",
                source=worker_module.run,
                session_cache_dir=str(tmp_path),
            )

        try:
            assert len(create_calls) == 1
            create_call = create_calls[0]
            assert create_call["entry_module"] == worker_module.__name__
            assert create_call["entry_callable"] == "run"
            assert create_call["package_format"] == "tar.gz"
            with tarfile.open(fileobj=io.BytesIO(create_call["blob"]), mode="r:gz") as tar:
                names = set(tar.getnames())
            assert f"{worker_module.__package__}/__init__.py" in names
            assert f"{worker_module.__package__}/worker.py" in names
            assert f"{worker_module.__package__}/helper.py" in names
            assert f"{worker_module.__package__}/ignored.csv" not in names
        finally:
            for client in group._clients.values():  # noqa: SLF001
                client.close()

    def test_join_emits_failure_summary(self, capsys):
        from pycloud_parallel.execution.service_session import Service

        failed_session = SimpleNamespace(
            failed=True,
            last_error="RuntimeError('heartbeat unavailable')",
            _hb_lock=threading.Lock(),
            _hb_thread=None,
        )
        group = Service(
            owner_client_id="owner-demo",
            service_name="demo-service",
            sessions={"node-1": failed_session},
            nodes={},
        )

        group.join(poll_interval_sec=0.01)
        err = capsys.readouterr().err
        assert "[Service] owner keepalive stopped service_name=demo-service" in err
        assert "node-1" in err

    def test_service_group_update_globals_prepares_values_once_for_all_nodes(self):
        from pycloud_parallel.execution.service_session import Service

        session_a = SimpleNamespace(failed=False, last_error="")
        session_a.update_globals_prepared = MagicMock(return_value=SimpleNamespace(globals_digest="sha256:same"))
        session_b = SimpleNamespace(failed=False, last_error="")
        session_b.update_globals_prepared = MagicMock(return_value=SimpleNamespace(globals_digest="sha256:same"))

        client_a = MagicMock()
        client_b = MagicMock()
        group = Service(
            owner_client_id="owner-demo",
            service_name="svc-demo",
            sessions={"node-a": session_a, "node-b": session_b},
            nodes={},
            _clients={"node-a": client_a, "node-b": client_b},
        )

        with patch(
            "pycloud_parallel.execution.service_session._prepare_managed_globals_batches_for_upload",
            return_value=(
                [{"cfg": {"k": "v"}}],
                {
                    "globals_batch_count": 1,
                    "batch_keys": [["cfg"]],
                    "batch_bytes": [1],
                    "staged_keys": [],
                    "inline_keys": ["cfg"],
                },
            ),
        ) as mocked_prepare:
            digest = group.update_globals({"cfg": {"k": "v"}})

        assert digest == "sha256:same"
        assert group.globals_digests == {"node-a": "sha256:same", "node-b": "sha256:same"}
        mocked_prepare.assert_called_once()
        prepare_args, prepare_kwargs = mocked_prepare.call_args
        assert prepare_args == ([client_a, client_b], {"cfg": {"k": "v"}})
        assert prepare_kwargs["effective_policy"] == group.effective_policy
        session_a.update_globals_prepared.assert_called_once_with(
            {"cfg": {"k": "v"}},
            serialization_mode=group.serialization_mode,
            effective_policy=group.effective_policy,
        )
        session_b.update_globals_prepared.assert_called_once_with(
            {"cfg": {"k": "v"}},
            serialization_mode=group.serialization_mode,
            effective_policy=group.effective_policy,
        )

    def test_service_group_update_globals_prunes_failed_nodes(self):
        from pycloud_parallel.execution.service_session import Service

        session_a = SimpleNamespace(failed=False, last_error="")
        session_a.update_globals_prepared = MagicMock(return_value=SimpleNamespace(globals_digest="sha256:same"))
        session_b = SimpleNamespace(failed=False, last_error="")
        session_b.update_globals_prepared = MagicMock(side_effect=RuntimeError("node-b unavailable"))

        client_a = MagicMock()
        client_b = MagicMock()
        group = Service(
            owner_client_id="owner-demo",
            service_name="svc-demo",
            sessions={"node-a": session_a, "node-b": session_b},
            nodes={"node-a": MagicMock(), "node-b": MagicMock()},
            _clients={"node-a": client_a, "node-b": client_b},
        )

        with patch(
            "pycloud_parallel.execution.service_session._prepare_managed_globals_batches_for_upload",
            return_value=(
                [{"cfg": {"k": "v"}}],
                {
                    "globals_batch_count": 1,
                    "batch_keys": [["cfg"]],
                    "batch_bytes": [1],
                    "staged_keys": [],
                    "inline_keys": ["cfg"],
                },
            ),
        ):
            digest = group.update_globals({"cfg": {"k": "v"}})

        assert digest == "sha256:same"
        assert set(group.sessions.keys()) == {"node-a"}
        assert set(group._clients.keys()) == {"node-a"}  # noqa: SLF001
        assert "node-b" in group.failures
        assert group.globals_digests == {"node-a": "sha256:same"}
        client_b.close.assert_called_once()

    def test_service_group_update_globals_allows_per_node_digests(self):
        from pycloud_parallel.execution.service_session import Service

        session_a = SimpleNamespace(failed=False, last_error="")
        session_a.update_globals_prepared = MagicMock(return_value=SimpleNamespace(globals_digest="sha256:a"))
        session_b = SimpleNamespace(failed=False, last_error="")
        session_b.update_globals_prepared = MagicMock(return_value=SimpleNamespace(globals_digest="sha256:b"))
        client_a = MagicMock()
        client_b = MagicMock()
        group = Service(
            owner_client_id="owner-demo",
            service_name="svc-demo",
            sessions={"node-a": session_a, "node-b": session_b},
            nodes={},
            _clients={"node-a": client_a, "node-b": client_b},
        )

        with patch(
            "pycloud_parallel.execution.service_session._prepare_managed_globals_batches_for_upload",
            return_value=(
                [{"cfg": {"k": "v"}}],
                {
                    "globals_batch_count": 1,
                    "batch_keys": [["cfg"]],
                    "batch_bytes": [1],
                    "staged_keys": [],
                    "inline_keys": ["cfg"],
                },
            ),
        ):
            digest = group.update_globals({"cfg": {"k": "v"}})

        assert digest in {"sha256:a", "sha256:b"}
        assert group.globals_digests == {"node-a": "sha256:a", "node-b": "sha256:b"}

    def test_service_group_update_globals_fails_when_all_nodes_fail(self):
        from pycloud_parallel.execution.service_session import Service

        session_a = SimpleNamespace(failed=False, last_error="")
        session_a.update_globals_prepared = MagicMock(side_effect=RuntimeError("node-a unavailable"))
        client_a = MagicMock()
        group = Service(
            owner_client_id="owner-demo",
            service_name="svc-demo",
            sessions={"node-a": session_a},
            nodes={"node-a": MagicMock()},
            _clients={"node-a": client_a},
        )

        with patch(
            "pycloud_parallel.execution.service_session._prepare_managed_globals_batches_for_upload",
            return_value=(
                [{"cfg": {"k": "v"}}],
                {
                    "globals_batch_count": 1,
                    "batch_keys": [["cfg"]],
                    "batch_bytes": [1],
                    "staged_keys": [],
                    "inline_keys": ["cfg"],
                },
            ),
        ):
            with pytest.raises(RuntimeError, match="update_globals failed on all nodes"):
                group.update_globals({"cfg": {"k": "v"}})

    def test_deploy_from_infocenter_clamps_worker_count_per_node_capacity(self, tmp_path):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        node_a = SimpleNamespace(
            node_id="node-a",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=10,
            capacity=10,
            queued=0,
            python_version="py3.11",
        )
        node_b = SimpleNamespace(
            node_id="node-b",
            control_addr="127.0.0.1:50062",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=7,
            capacity=7,
            queued=0,
            python_version="py3.11",
        )
        create_calls = []

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **kwargs):
                create_calls.append((self.target, dict(kwargs)))
                return SimpleNamespace(
                    service_id=f"svc-{self.target.rsplit(':', 1)[-1]}",
                    service_token="token",
                    http_base_url=f"http://{self.target}/svc/demo",
                    heartbeat_timeout_sec=30,
                    worker_count=int(kwargs["worker_count"]),
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=((), [node_a, node_b]),
        ), patch(
            "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_persist_session_cache",
            lambda self: None,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service._deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="svc-clamp",
                source=b"def run(**_kwargs):\n    return {'ok': True}\n",
                entry_module="svc_clamp",
                entry_callable="run",
                worker_count=8,
                node_count=2,
                min_success_nodes=2,
                allow_partial=False,
                session_cache_dir=str(tmp_path),
            )

        try:
            assert len(create_calls) == 2
            call_map = {target: kwargs for target, kwargs in create_calls}
            assert call_map["127.0.0.1:50061"]["worker_count"] == 8
            assert call_map["127.0.0.1:50062"]["worker_count"] == 7
        finally:
            for client in group._clients.values():  # noqa: SLF001
                client.close()

    def test_deploy_from_infocenter_ignores_inspected_stopped_routes(self, tmp_path):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        fake_node = SimpleNamespace(
            node_id="node-1",
            node_instance_id="node-1-inst",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )
        fake_route = SimpleNamespace(
            service_name="demo-stopped-service",
            service_id="svc-old",
            status=pb2.SERVICE_STATUS_RUNNING,
            node_id="node-1",
            node_instance_id="node-1-inst",
            control_addr="127.0.0.1:50061",
            http_base_url="http://127.0.0.1:18081/svc/svc-old",
        )
        stopped_info = SimpleNamespace(
            owner_client_id="owner-demo",
            code_version="sha256:old",
            status=pb2.SERVICE_STATUS_STOPPED,
            service_name="demo-stopped-service",
            http_base_url=fake_route.http_base_url,
            worker_count=1,
            created_at=None,
            last_heartbeat_at=None,
            lease_expire_at=None,
        )
        create_calls = []

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def create_service_from_bytes(self, **kwargs):
                create_calls.append(dict(kwargs))
                return SimpleNamespace(
                    service_id="svc-new",
                    service_token="token-new",
                    http_base_url="http://127.0.0.1:18081/svc/svc-new",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=([fake_route], [fake_node]),
        ), patch.object(
            Service,
            "_inspect_existing_routes",
            return_value=[(fake_route, stopped_info)],
        ), patch(
            "pycloud_parallel.execution.service_session._node_control_client",
            _FakeNodeControlClient,
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service._deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-stopped-service",
                source=b"def run(**_kwargs):\n    return {'ok': True}\n",
                runtime="py3",
                entry_module="demo_service",
                entry_callable="run",
                session_cache_dir=str(tmp_path),
            )

        try:
            assert len(create_calls) == 1
            assert create_calls[0]["service_name"] == "demo-stopped-service"
        finally:
            group.close(end_services=False)

    def test_deploy_from_infocenter_redeploys_when_reuse_heartbeat_hits_stopped_service(self, tmp_path):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.execution.support import _artifact_code_version
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
        effective_code_version = _artifact_code_version(
            blob=blob,
            runtime="py3",
            entry_module="demo_service",
            entry_callable="run",
            package_format="py",
            export_mode="decorator",
        )
        fake_node = SimpleNamespace(
            node_id="node-1",
            node_instance_id="node-1-inst",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )
        fake_route = SimpleNamespace(
            service_name="demo-race-service",
            service_id="svc-old",
            status=pb2.SERVICE_STATUS_RUNNING,
            node_id="node-1",
            node_instance_id="node-1-inst",
            control_addr="127.0.0.1:50061",
            http_base_url="http://127.0.0.1:18081/svc/svc-old",
        )
        running_info = SimpleNamespace(
            owner_client_id="owner-demo",
            code_version=effective_code_version,
            status=pb2.SERVICE_STATUS_RUNNING,
            service_name="demo-race-service",
            http_base_url=fake_route.http_base_url,
            worker_count=1,
            created_at=None,
            last_heartbeat_at=None,
            lease_expire_at=None,
        )
        create_calls = []

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def heartbeat_service(self, **kwargs):
                del kwargs
                raise RuntimeError("service is stopped")

            def create_service_from_bytes(self, **kwargs):
                create_calls.append(dict(kwargs))
                return SimpleNamespace(
                    service_id="svc-new",
                    service_token="token-new",
                    http_base_url="http://127.0.0.1:18081/svc/svc-new",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=([fake_route], [fake_node]),
        ), patch.object(
            Service,
            "_inspect_existing_routes",
            return_value=[(fake_route, running_info)],
        ), patch(
            "pycloud_parallel.execution.service_session._node_control_client",
            _FakeNodeControlClient,
        ), patch(
            "pycloud_parallel.execution.service_session._load_service_session_cache",
            return_value={
                "artifact_code_version": effective_code_version,
                "nodes": {
                    "node-1-inst": {
                        "service_id": "svc-old",
                        "service_token": "token-old",
                        "http_base_url": fake_route.http_base_url,
                        "worker_count": 1,
                        "heartbeat_timeout_sec": 30,
                    }
                },
            },
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service._deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-race-service",
                source=blob,
                runtime="py3",
                entry_module="demo_service",
                entry_callable="run",
                session_cache_dir=str(tmp_path),
            )

        try:
            assert len(create_calls) == 1
            assert create_calls[0]["service_name"] == "demo-race-service"
        finally:
            group.close(end_services=False)

    def test_deploy_from_infocenter_replaces_different_code_using_cached_token(self, tmp_path):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        fake_node = SimpleNamespace(
            node_id="node-1",
            node_instance_id="node-1-inst",
            control_addr="127.0.0.1:50061",
            healthy=True,
            schedulable=True,
            drain=False,
            service_worker_available=2,
            capacity=2,
            queued=0,
            python_version="py3.11",
        )
        fake_route = SimpleNamespace(
            service_name="demo-replace-service",
            service_id="svc-old",
            status=pb2.SERVICE_STATUS_RUNNING,
            node_id="node-1",
            node_instance_id="node-1-inst",
            control_addr="127.0.0.1:50061",
            http_base_url="http://127.0.0.1:18081/svc/svc-old",
        )
        running_info = SimpleNamespace(
            owner_client_id="owner-demo",
            code_version="sha256:old-code",
            status=pb2.SERVICE_STATUS_RUNNING,
            service_name="demo-replace-service",
            http_base_url=fake_route.http_base_url,
            worker_count=1,
            created_at=None,
            last_heartbeat_at=None,
            lease_expire_at=None,
        )
        operations = []

        class _FakeNodeControlClient:
            def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
                self.target = target
                self.timeout_sec = timeout_sec

            def end_service(self, **kwargs):
                operations.append(("end", self.target, dict(kwargs)))

            def create_service_from_bytes(self, **kwargs):
                operations.append(("create", self.target, dict(kwargs)))
                return SimpleNamespace(
                    service_id="svc-new",
                    service_token="token-new",
                    http_base_url="http://127.0.0.1:18081/svc/svc-new",
                    heartbeat_timeout_sec=30,
                    worker_count=1,
                    status=pb2.SERVICE_STATUS_RUNNING,
                )

            def close(self) -> None:
                return None

        with patch(
            "pycloud_parallel.execution.service_session._retry_infocenter_request",
            return_value=([fake_route], [fake_node]),
        ), patch.object(
            Service,
            "_inspect_existing_routes",
            return_value=[(fake_route, running_info)],
        ), patch(
            "pycloud_parallel.execution.service_session._node_control_client",
            _FakeNodeControlClient,
        ), patch(
            "pycloud_parallel.execution.service_session._load_service_session_cache",
            return_value={
                "artifact_code_version": "sha256:old-code",
                "nodes": {
                    "node-1-inst": {
                        "service_id": "svc-old",
                        "service_token": "token-old",
                        "http_base_url": fake_route.http_base_url,
                        "worker_count": 1,
                        "heartbeat_timeout_sec": 30,
                    }
                },
            },
        ), patch.object(
            Service,
            "_start_keepalive",
            lambda self, interval_sec=None: None,
        ):
            group = Service._deploy_from_infocenter(
                infocenter_target="127.0.0.1:50051",
                owner_client_id="owner-demo",
                service_name="demo-replace-service",
                source=b"def run(**_kwargs):\n    return {'ok': True}\n",
                runtime="py3",
                entry_module="demo_service",
                entry_callable="run",
                session_cache_dir=str(tmp_path),
            )

        try:
            assert [item[0] for item in operations] == ["end", "create"]
            assert operations[0][2]["service_id"] == "svc-old"
            assert operations[0][2]["service_token"] == "token-old"
            assert operations[1][2]["service_name"] == "demo-replace-service"
        finally:
            group.close(end_services=False)

    def test_inspect_existing_routes_rejects_startup_http_only_route(self):
        from pycloud_parallel.execution.service_session import Service
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        route = SimpleNamespace(
            service_name="calc_asset_ratio",
            service_id="calc-asset-ratio-startup-1",
            status=pb2.SERVICE_STATUS_RUNNING,
            node_id="startup-node",
            node_instance_id="startup-node-inst",
            control_addr="",
            http_base_url="http://127.0.0.1:18080/svc/calc-asset-ratio-startup-1",
        )

        with patch("pycloud_parallel.execution.service_session._node_control_client") as mocked_client:
            with pytest.raises(RuntimeError, match="startup/http-only service route"):
                Service._inspect_existing_routes(active_routes=[route], timeout_sec=1.0)

        mocked_client.assert_not_called()

    def test_async_call_all_interface(self):
        """测试异步 call_all 接口。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="test-service",
            sessions={},
            nodes={},
        )
        mock_results = [("node1", {"result": 49}, None)]
        group.acall_all = AsyncMock(return_value=mock_results)

        async def test():
            results = await group.call_all("square", x=7)
            assert len(results) == 1
            assert results[0][1] == {"result": 49}

        asyncio.run(test())


class TestIntegration:
    """集成测试，测试完整的调用流程。"""

    def test_full_async_flow(self):
        """测试完整的异步调用流程。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        # 模拟 session
        mock_session = MagicMock()
        mock_method_info = MagicMock()
        mock_method_info.method = "square"
        mock_session.list_methods.return_value = [mock_method_info]

        # 模拟 group
        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="compute-service",
            sessions={"node1": mock_session, "node2": MagicMock()},
            nodes={"node1": MagicMock(), "node2": MagicMock()},
        )

        # 模拟 acall_balanced
        async def mock_acall(method, payload, **kwargs):
            if method == "square":
                x = payload.get("x", 0)
                return ("node1", {"data": {"x": x, "y": x * x}})
            raise ValueError(f"Unknown method: {method}")

        group.acall_balanced = mock_acall

        async def run_test():
            # 调用远程方法，就像本地函数一样
            result1 = await group.square(x=7)
            assert result1 == {"x": 7, "y": 49}

            result2 = await group.square(x=10)
            assert result2 == {"x": 10, "y": 100}

        asyncio.run(run_test())

    def test_full_sync_flow(self):
        """测试完整的同步调用流程。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        # 模拟 session
        mock_session = MagicMock()
        mock_method_info = MagicMock()
        mock_method_info.method = "square"
        mock_session.list_methods.return_value = [mock_method_info]

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="compute-service",
            sessions={"node1": mock_session},
            nodes={"node1": MagicMock()},
        )

        def mock_call(method, payload, **kwargs):
            if method == "square":
                x = payload.get("x", 0)
                return ("node1", {"data": {"x": x, "y": x * x}})
            raise ValueError(f"Unknown method: {method}")

        group.call_balanced = mock_call

        # 同步调用
        result = group.square.sync(x=5)
        assert result == {"x": 5, "y": 25}

    def test_full_broadcast_flow(self):
        """测试完整的广播调用流程。"""
        from pycloud_parallel import Service as OwnerServiceFacade

        # 模拟 session
        mock_session = MagicMock()
        mock_method_info = MagicMock()
        mock_method_info.method = "square"
        mock_session.list_methods.return_value = [mock_method_info]

        group = OwnerServiceFacade(
            owner_client_id="test",
            service_name="compute-service",
            sessions={"node1": mock_session, "node2": MagicMock()},
            nodes={"node1": MagicMock(), "node2": MagicMock()},
        )

        async def mock_acall_all(method, payload, **kwargs):
            return [
                ("node1", {"data": {"x": 7, "y": 49}}, None),
                ("node2", {"data": {"x": 7, "y": 49}}, None),
            ]

        group.acall_all = mock_acall_all

        async def run_test():
            results = await group.square.broadcast(x=7)

            assert len(results) == 2
            for node_id, result, error in results:
                assert error is None
                assert result == {"x": 7, "y": 49}

        asyncio.run(run_test())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
