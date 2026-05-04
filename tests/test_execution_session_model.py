from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


def _utc_now():
    return datetime.now(timezone.utc)


def test_service_replica_state_snapshot_views() -> None:
    from pycloud_parallel.controlplane.node.models import ServiceReplicaState

    now = _utc_now()
    state = ServiceReplicaState(
        service_id="svc-1",
        owner_client_id="owner-a",
        service_name="svc-demo",
        code_version="sha256:" + ("a" * 64),
        worker_count=2,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=15,
        expose_http=True,
        service_token="token-a",
        http_base_url="http://127.0.0.1:18080/svc/svc-1",
        status=pb2.SERVICE_STATUS_RUNNING,
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
        executor_ready=True,
        managed_global_names=("cfg",),
        managed_globals_scope_dir="/tmp/globals",
        managed_globals_digest="sha256:digest",
    )

    assert state.identity().session_name == "svc-demo"
    assert state.lease().idle_ttl_sec == 15
    assert state.binding().executor_ready is True
    resource = state.resource_snapshot()
    assert resource.worker_count == 2
    assert resource.alive_workers == 0
    assert resource.in_flight == 0
    assert resource.received_count == 0
    assert resource.returned_count == 0
    snap = state.snapshot(node_instance_id="node-inst-1", node_id="node-1")
    assert snap.kind == "service"
    assert snap.alive is True
    assert snap.session_id == "svc-1"


def test_task_pool_replica_state_snapshot_views() -> None:
    from pycloud_parallel.controlplane.node.models import TaskPoolReplicaState

    now = _utc_now()
    state = TaskPoolReplicaState(
        pool_id="pool-1",
        owner_client_id="owner-a",
        pool_name="pool-demo",
        code_version="sha256:" + ("b" * 64),
        task_method="run",
        worker_count=3,
        heartbeat_timeout_sec=20,
        idle_ttl_sec=10,
        pool_token="pool-token",
        status="RUNNING",
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=20),
        executor_ready=True,
        alive_workers=3,
        managed_global_names=("cfg",),
        managed_globals_scope_dir="/tmp/pool-globals",
        managed_globals_digest="sha256:pool-digest",
    )

    assert state.identity().session_token == "pool-token"
    assert state.binding().managed_globals_digest == "sha256:pool-digest"
    resource = state.resource_snapshot()
    assert resource.worker_count == 3
    assert resource.alive_workers == 3
    assert resource.in_flight == 0
    assert resource.received_count == 0
    assert resource.returned_count == 0
    snap = state.snapshot(node_instance_id="node-inst-1", node_id="node-1")
    assert snap.kind == "task_pool"
    assert snap.alive is True
    assert snap.session_name == "pool-demo"


def test_service_session_client_identity_and_snapshot() -> None:
    from pycloud_parallel.controlplane.replica_client import ServiceSessionClient

    now = _utc_now()
    client = ServiceSessionClient(
        _client=MagicMock(),
        owner_client_id="owner-a",
        service_id="svc-1",
        service_token="token-a",
        code_version="sha256:" + ("c" * 64),
        http_base_url="http://127.0.0.1:18080/svc/svc-1",
        heartbeat_timeout_sec=30,
        worker_count=2,
        status=pb2.SERVICE_STATUS_RUNNING,
        service_name="svc-demo",
        node_instance_id="node-inst-1",
        node_id="node-1",
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
    )

    assert client.identity().session_id == "svc-1"
    assert client.binding().code_version.endswith("c" * 64)
    snap = client.snapshot()
    assert snap.kind == "service"
    assert snap.node_instance_id == "node-inst-1"
    assert snap.alive is True


def test_native_task_pool_client_update_globals_prepared_uses_pool_identity() -> None:
    from pycloud_parallel.controlplane.replica_client import NativeTaskPoolClient

    control_client = MagicMock()
    control_client.update_runtime_globals_prepared.return_value = SimpleNamespace(globals_digest="sha256:digest")
    pool = NativeTaskPoolClient(
        _client=control_client,
        owner_client_id="owner-a",
        pool_id="pool-1",
        pool_token="pool-token",
        code_version="sha256:" + ("d" * 64),
        worker_count=2,
        pool_name="pool-demo",
    )

    resp = pool.update_globals_prepared({"cfg": {"k": "v"}})

    assert resp.globals_digest == "sha256:digest"
    control_client.update_runtime_globals_prepared.assert_called_once_with(
        client_id="pool-1",
        code_version="sha256:" + ("d" * 64),
        runtime_key="pool-1",
        code_token="pool-token",
        prepared_values={"cfg": {"k": "v"}},
    )


def test_service_session_client_update_globals_prepared_uses_prepared_rpc() -> None:
    from pycloud_parallel.controlplane.replica_client import ServiceSessionClient

    control_client = MagicMock()
    control_client.update_service_globals_prepared.return_value = SimpleNamespace(globals_digest="sha256:digest")
    service = ServiceSessionClient(
        _client=control_client,
        owner_client_id="owner-a",
        service_id="svc-1",
        service_token="svc-token",
        code_version="sha256:" + ("e" * 64),
        http_base_url="http://127.0.0.1:18080/svc/svc-1",
        heartbeat_timeout_sec=30,
        worker_count=2,
        status=pb2.SERVICE_STATUS_RUNNING,
        service_name="svc-demo",
    )

    resp = service.update_globals_prepared({"cfg": {"k": "v"}})

    assert resp.globals_digest == "sha256:digest"
    control_client.update_service_globals_prepared.assert_called_once_with(
        owner_client_id="owner-a",
        service_id="svc-1",
        service_token="svc-token",
        prepared_values={"cfg": {"k": "v"}},
    )
    control_client.update_service_globals.assert_not_called()


def test_service_group_exposes_replicas_and_snapshot() -> None:
    from pycloud_parallel.controlplane.replica_client import ServiceSessionClient
    from pycloud_parallel.execution.service_session import Service

    now = _utc_now()
    replica = ServiceSessionClient(
        _client=MagicMock(),
        owner_client_id="owner-a",
        service_id="svc-1",
        service_token="token-a",
        code_version="sha256:" + ("e" * 64),
        http_base_url="http://127.0.0.1:18080/svc/svc-1",
        heartbeat_timeout_sec=30,
        worker_count=2,
        status=pb2.SERVICE_STATUS_RUNNING,
        service_name="svc-demo",
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
    )
    group = Service(
        owner_client_id="owner-a",
        service_name="svc-demo",
        sessions={"node-inst-1": replica},
        nodes={"node-inst-1": SimpleNamespace(node_id="node-1")},
    )

    assert group.replicas["node-inst-1"] is replica
    snap = group.snapshot()["node-inst-1"]
    assert snap.node_id == "node-1"
    assert group.is_alive() is True
    status = group.status()
    assert status.kind == "service"
    assert status.replica_count == 1
    assert status.alive_replica_count == 1
    assert status.failed is False
    assert status.alive is True
    assert status.last_heartbeat_at == now
    assert status.lease_expire_at == now + timedelta(seconds=30)


def test_task_pool_session_exposes_replicas_and_snapshot() -> None:
    from pycloud_parallel import TaskPool
    from pycloud_parallel.controlplane.replica_client import NativeTaskPoolClient

    now = _utc_now()
    pool = NativeTaskPoolClient(
        _client=MagicMock(),
        owner_client_id="owner-a",
        pool_id="pool-1",
        pool_token="pool-token",
        code_version="sha256:" + ("f" * 64),
        worker_count=2,
        heartbeat_timeout_sec=30,
        pool_name="pool-demo",
        status="RUNNING",
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
    )
    session = TaskPool(
        pools={"node-inst-1": pool},
        nodes={"node-inst-1": SimpleNamespace(node_id="node-1")},
        task_method="run",
        job_id="job-1",
    )

    assert session.replicas["node-inst-1"] is pool
    snap = session.snapshot()["node-inst-1"]
    assert snap.session_name == "pool-demo"
    assert snap.node_id == "node-1"
    status = session.status()
    assert status.kind == "task_pool"
    assert status.replica_count == 1
    assert status.alive_replica_count == 1
    assert status.failed is False
    assert status.alive is True
    assert status.last_heartbeat_at == now
    assert status.lease_expire_at == now + timedelta(seconds=30)


def test_task_pool_session_put_data_uses_shared_client_upload_path(monkeypatch) -> None:
    from pycloud_parallel import TaskPool
    from pycloud_parallel.controlplane.replica_client import NativeTaskPoolClient
    from pycloud_parallel.data.ref import DataRef
    from pycloud_parallel.execution import task_pool as task_pool_module

    now = _utc_now()
    pool_client = MagicMock()
    pool = NativeTaskPoolClient(
        _client=pool_client,
        owner_client_id="owner-a",
        pool_id="pool-1",
        pool_token="pool-token",
        code_version="sha256:" + ("f" * 64),
        worker_count=2,
        heartbeat_timeout_sec=30,
        pool_name="pool-demo",
        status="RUNNING",
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
    )
    session = TaskPool(
        pools={"node-inst-1": pool},
        nodes={"node-inst-1": SimpleNamespace(node_id="node-1")},
        task_method="run",
        job_id="job-1",
    )
    captured = {}

    monkeypatch.setattr(
        task_pool_module,
        "_put_data_via_clients",
        lambda clients, data, *, format="", chunk_size=0, serialization_mode="": captured.update(
            {
                "clients": list(clients),
                "data": data,
                "format": format,
                "chunk_size": chunk_size,
                "serialization_mode": serialization_mode,
            }
        )
        or DataRef(
            ref_id="sha256:" + ("1" * 64),
            storage_id="sha256:" + ("1" * 64),
            format=str(format or "json"),
            locator_token="127.0.0.1:50051",
        ),
    )

    ref = session.put_json({"ok": True})

    assert ref.format == "json"
    assert captured["clients"] == [pool_client]
    assert captured["data"] == {"ok": True}
    assert captured["format"] == "json"
    assert captured["serialization_mode"] == session.serialization_mode
