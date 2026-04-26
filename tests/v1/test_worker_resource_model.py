from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from pycloud_parallel.controlplane.node.models import ServiceSession, TaskPoolState
from pycloud_parallel.controlplane.node.session_views import (
    build_service_report_payload,
    build_service_status_info,
    build_task_pool_info,
    build_task_pool_status_info,
    execute_warmup,
)
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def test_shared_service_and_task_pool_view_builders_use_resource_snapshot():
    now = _utc_now()
    service = ServiceSession(
        service_id="svc-1",
        owner_client_id="owner-a",
        service_name="svc-demo",
        code_version="sha256:" + ("a" * 64),
        worker_count=3,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        expose_http=True,
        service_token="token-a",
        http_base_url="http://127.0.0.1:18080/svc/svc-1",
        status=pb2.SERVICE_STATUS_RUNNING,
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
        alive_workers=2,
        request_count=7,
        returned_count=4,
    )
    pool = TaskPoolState(
        pool_id="pool-1",
        owner_client_id="owner-a",
        pool_name="pool-demo",
        code_version="sha256:" + ("b" * 64),
        task_method="run",
        worker_count=4,
        heartbeat_timeout_sec=30,
        idle_ttl_sec=0,
        pool_token="pool-token",
        status="RUNNING",
        created_at=now,
        last_heartbeat_at=now,
        lease_expire_at=now + timedelta(seconds=30),
        alive_workers=3,
        task_count=9,
        returned_count=5,
    )

    service_status = build_service_status_info(service, in_flight=3)
    service_report = build_service_report_payload(service, in_flight=3)
    pool_info = build_task_pool_info(pool, in_flight=4)
    pool_status = build_task_pool_status_info(pool, in_flight=4)

    assert service_status["worker_count"] == 3
    assert service_status["alive_workers"] == 2
    assert service_status["received_count"] == 7
    assert service_status["returned_count"] == 4
    assert service_status["in_flight"] == 3
    assert service_report["received_count"] == 7
    assert service_report["returned_count"] == 4

    assert pool_info.worker_count == 4
    assert pool_info.alive_workers == 3
    assert pool_info.received_count == 9
    assert pool_info.returned_count == 5
    assert pool_info.inflight == 4
    assert pool_status["received_count"] == 9
    assert pool_status["returned_count"] == 5
    assert pool_status["inflight"] == 4


def test_execute_warmup_dispatches_scope_and_normalizes_result():
    executor_host = SimpleNamespace(
        warmup_service=lambda *, service_id, fanout, execute_spec: (fanout - 1, [101, 102]),
        warmup_pool=lambda *, pool_id, fanout, execute_spec: [201, 202],
        warmup_runtime=lambda *, runtime_key, fanout, execute_spec: fanout,
    )

    submitted, worker_pids = execute_warmup(
        executor_host,
        scope="service",
        key="svc-1",
        worker_count=2,
        execute_spec={"warmup_only": True},
    )
    assert submitted == 1
    assert worker_pids == [101, 102]

    submitted, worker_pids = execute_warmup(
        executor_host,
        scope="pool",
        key="pool-1",
        worker_count=2,
        execute_spec={"warmup_only": True},
    )
    assert submitted == 2
    assert worker_pids == [201, 202]

    submitted, worker_pids = execute_warmup(
        executor_host,
        scope="runtime",
        key="runtime-1",
        worker_count=2,
        execute_spec={"warmup_only": True},
    )
    assert submitted == 2
    assert worker_pids == []
