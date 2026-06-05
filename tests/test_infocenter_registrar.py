from __future__ import annotations

"""Integration tests for NodeControl -> InfoCenter registrar sync."""

import hashlib
import json
import time
from datetime import datetime, timezone
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pytest

from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient
from pycloud_parallel.controlplane.infocenter_http import InfoCenterHttpServer, _render_ops_page, _render_ops_snapshot, _reorder_job_via_http
from pycloud_parallel.controlplane.infocenter.models import NodeServiceState, NodeState, NodeTaskPoolInfo
from pycloud_parallel.controlplane.node_control_http import NodeControlHttpServer
from pycloud_parallel.controlplane.registrar import NodeInfoCenterRegistrar
from pycloud_parallel.controlplane.runtime_spec import matches_python_runtime, normalize_python_runtime_spec
from pycloud_parallel.controlplane.server import build_job_orchestrator_server
from pycloud_parallel.controlplane.infocenter_state import InfoCenterState, normalize_node_profile_key
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


def _wait_until(predicate, timeout_sec: float = 5.0, interval_sec: float = 0.1) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_sec)
    return False


def test_node_registrar_syncs_service_routes(tmp_path):
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    node_state = NodeControlState(
        node_id="node-reg-01",
        queue_capacity=32,
        worker_capacity=4,
        artifact_dir=str(tmp_path / "code_cache"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
        monitor_interval_sec=1,
    )
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr=info_target,
        node_id="node-reg-01",
        control_addr="127.0.0.1:50061",
        state=node_state,
        capacity=4,
        queue_capacity=32,
        tags=["compute", "test"],
        version="test-v1",
        fallback_heartbeat_sec=1,
    )

    try:
        registrar.start()

        with InfoCenterClient(info_target, timeout_sec=5.0) as infocenter:
            assert _wait_until(lambda: len(infocenter.list_nodes(healthy_only=True, tags=["compute"], limit=20)) >= 1)

            blob = (
                b"def run(value=0, **_kwargs):\n"
                b"    value = int(value)\n"
                b"    return {'value': value, 'square': value * value}\n"
            )
            digest = hashlib.sha256(blob).hexdigest()
            session = node_state.create_service(
                owner_client_id="owner-reg",
                service_name="svc-reg-sync",
                sha256=f"sha256:{digest}",
                runtime="py3",
                entry_module="svc_reg",
                entry_callable="run",
                package_format="py",
                worker_count=2,
                heartbeat_timeout_sec=30,
                idle_ttl_sec=0,
                expose_http=True,
                chunks=[blob],
            )

            def _route_ready() -> bool:
                routes = infocenter.list_service_routes(service_name="svc-reg-sync", healthy_only=True, limit=20)
                return any(r.service_id == session.service_id and r.status == pb2.SERVICE_STATUS_RUNNING for r in routes)

            assert _wait_until(_route_ready, timeout_sec=6.0)

            nodes = infocenter.list_nodes(healthy_only=True, tags=["compute"], limit=20)
            assert len(nodes) == 1
            node = nodes[0]
            assert node.loaded_services == ("svc-reg-sync",)
            assert len(node.services) == 1
            assert node.services[0].service_name == "svc-reg-sync"
            assert node.services[0].worker_count == 2
            assert node.services[0].alive_workers == 2

            with urlopen(f"{info_target}/ops", timeout=5.0) as resp:
                raw = resp.read().decode("utf-8")
            assert "Service Instances" in raw
            assert "svc-reg-sync" in raw
            assert ">2</td><td>2</td>" in raw
            assert "controlplane_version=" in raw
            for header in (
                "<th>node_id</th>",
                "<th>instance_id</th>",
                "<th>control_addr</th>",
                "<th>healthy</th>",
                "<th>schedulable</th>",
                "<th>node quota</th>",
                "<th>proc quota</th>",
                "<th>accept deploy</th>",
                "<th>drain</th>",
                "<th>enabled</th>",
                "<th>pycloud</th>",
            ):
                assert header in raw
            assert "task 0/4" in raw
            assert "free 4" in raw
            assert "queue 0/32" in raw
            assert "credit 32" in raw
            assert "<th>effective tags</th><th>managed tags</th><th>capability tags</th><th>legacy node tags</th>" in raw
            assert "avg_total_ms" in raw
            assert "avg_child_decode_ms" in raw
            assert "avg_child_invoke_ms" in raw
            assert "avg_child_encode_ms" in raw
            assert "last_total_ms" not in raw
            assert "last_child_decode_ms" not in raw
            assert "last_build_execute_spec_ms" not in raw

            node_state.end_service(
                owner_client_id="owner-reg",
                service_id=session.service_id,
                service_token=session.service_token,
                reason="done",
            )

            def _route_cleared() -> bool:
                routes = infocenter.list_service_routes(service_name="svc-reg-sync", healthy_only=True, limit=20)
                return len(routes) == 0

            assert _wait_until(_route_cleared, timeout_sec=6.0)
    finally:
        registrar.close()
        node_state.close()
        info_server.stop()


def test_node_registrar_advertises_http_control_capability(tmp_path):
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    node_state = NodeControlState(
        node_id="node-http-cap",
        queue_capacity=32,
        worker_capacity=4,
        artifact_dir=str(tmp_path / "code_cache_http_cap"),
        enable_internal_executor=False,
        enable_service_session=False,
        control_base_url="http://127.0.0.1:18061",
    )
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr=info_server.base_url,
        node_id="node-http-cap",
        control_addr="127.0.0.1:50061",
        state=node_state,
        capacity=4,
        queue_capacity=32,
        tags=["compute"],
        fallback_heartbeat_sec=1,
    )

    try:
        assert registrar.sync_now() is True
        with InfoCenterClient(info_server.base_url, timeout_sec=5.0) as infocenter:
            nodes = infocenter.list_nodes(healthy_only=True, tags=["compute"], limit=20)
        assert len(nodes) == 1
        assert nodes[0].capability.supports_http_control is True
        assert nodes[0].capability.control_base_url == "http://127.0.0.1:18061"
    finally:
        registrar.close()
        node_state.close()
        info_server.stop()


def test_node_registrar_marks_fenced_node_unhealthy_and_not_deployable(tmp_path):
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    node_state = NodeControlState(
        node_id="node-fenced-reg",
        queue_capacity=32,
        worker_capacity=4,
        artifact_dir=str(tmp_path / "code_cache_fenced_reg"),
        enable_internal_executor=False,
        enable_service_session=True,
        control_base_url="http://127.0.0.1:18062",
    )
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr=info_server.base_url,
        node_id="node-fenced-reg",
        control_addr="127.0.0.1:50062",
        state=node_state,
        capacity=4,
        queue_capacity=32,
        tags=["compute"],
        fallback_heartbeat_sec=1,
    )

    try:
        assert registrar.sync_now() is True
        node_state.reset_execution_state(reason="test fence")
        assert registrar.sync_now() is True

        with InfoCenterClient(info_server.base_url, timeout_sec=5.0) as infocenter:
            healthy_nodes = infocenter.list_nodes(healthy_only=True, tags=["compute"], limit=20)
            all_nodes = infocenter.list_nodes(healthy_only=False, tags=["compute"], limit=20)

        assert healthy_nodes == []
        assert len(all_nodes) == 1
        assert all_nodes[0].healthy is False
        assert all_nodes[0].accept_service_deploy is False
    finally:
        registrar.close()
        node_state.close()
        info_server.stop()


def test_ops_page_merges_duplicate_services_with_same_endpoint():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_state.register_node_record(
        node_instance_id="node-dup-1",
        node_id="node-dup",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=32,
        tags=["compute"],
        services={
            "svc-a": NodeServiceState(
                service_name="calc_asset_ratio",
                service_id="svc-a",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=2,
                alive_workers=2,
                in_flight=1,
                http_base_url="http://127.0.0.1:18081/svc/svc-a",
            ),
            "svc-b": NodeServiceState(
                service_name="calc_asset_ratio",
                service_id="svc-b",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=2,
                alive_workers=2,
                in_flight=1,
                http_base_url="http://127.0.0.1:18081/svc/svc-b",
            ),
        },
    )

    raw = _render_ops_page(info_state)

    assert raw.count("calc_asset_ratio") >= 1
    assert "svc-a (+1)" in raw or "svc-b (+1)" in raw
    assert "merged×2" not in raw


def test_ops_snapshot_returns_partial_table_fragments():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_state.register_node_record(
        node_instance_id="node-snapshot-inst",
        node_id="node-snapshot",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=32,
        tags=["compute"],
        services={
            "svc-snapshot": NodeServiceState(
                service_name="svc-snapshot",
                service_id="svc-snapshot",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=2,
                alive_workers=2,
                in_flight=1,
                http_base_url="http://127.0.0.1:18081/svc/svc-snapshot",
            )
        },
    )

    snapshot = _render_ops_snapshot(info_state)

    assert snapshot["ok"] is True
    fragments = snapshot["fragments"]
    assert "node-snapshot" in fragments["ops-nodes-body"]
    assert "svc 0/0" in fragments["ops-nodes-body"]
    assert "svc-snapshot" not in fragments["ops-nodes-body"]
    assert "svc-snapshot" in fragments["ops-services-body"]
    assert "<tbody" not in fragments["ops-nodes-body"]


def test_list_service_routes_uses_service_name_index_for_filtered_lookup():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_state.register_node_record(
        node_instance_id="node-a-inst",
        node_id="node-a",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=32,
        tags=["compute"],
        services={
            "svc-a": NodeServiceState(
                service_name="svc-a",
                service_id="svc-a",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=1,
                alive_workers=1,
                http_base_url="http://127.0.0.1:18081/svc/svc-a",
            )
        },
    )
    info_state.register_node_record(
        node_instance_id="node-b-inst",
        node_id="node-b",
        control_addr="127.0.0.1:50062",
        capacity=4,
        queue_capacity=32,
        tags=["compute"],
        services={
            "svc-b": NodeServiceState(
                service_name="svc-b",
                service_id="svc-b",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=1,
                alive_workers=1,
                http_base_url="http://127.0.0.1:18082/svc/svc-b",
            )
        },
    )

    original = info_state._fence_if_stale_locked  # noqa: SLF001
    visited: list[str] = []

    def _tracking_fence(state, *, now=None):  # noqa: ANN001
        visited.append(str(state.node_instance_id))
        return original(state, now=now)

    info_state._fence_if_stale_locked = _tracking_fence  # type: ignore[method-assign]  # noqa: SLF001
    try:
        routes = info_state.list_service_routes(
            service_name="svc-a",
            healthy_only=True,
            limit=10,
        )
    finally:
        info_state._fence_if_stale_locked = original  # type: ignore[method-assign]  # noqa: SLF001

    assert [route["service_name"] for route in routes] == ["svc-a"]
    assert visited == ["node-a-inst"]


def test_ops_page_merges_duplicate_services_across_node_records_with_same_endpoint():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    for idx, instance_id in enumerate(("node-dup-a", "node-dup-b"), start=1):
        service_id = f"svc-{idx}"
        info_state.register_node_record(
            node_instance_id=instance_id,
            node_id="node-dup",
            control_addr=f"127.0.0.1:5006{idx}",
            capacity=4,
            queue_capacity=32,
            tags=["compute"],
            services={
                service_id: NodeServiceState(
                    service_name="calc_asset_ratio",
                    service_id=service_id,
                    status=pb2.SERVICE_STATUS_RUNNING,
                    worker_count=2,
                    alive_workers=2,
                    in_flight=1,
                    http_base_url=f"http://127.0.0.1:18081/svc/{service_id}",
                )
            },
        )

    raw = _render_ops_page(info_state)

    assert "node-dup-a, node-dup-b" in raw
    assert "svc-1 (+1)" in raw or "svc-2 (+1)" in raw
    assert raw.count("<td>calc_asset_ratio</td>") == 1


def test_ops_page_merges_duplicate_startup_nodes_with_same_control_addr():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_state._nodes["startup-old"] = NodeState(  # noqa: SLF001
        node_instance_id="startup-old",
        node_id="calc_asset_ratio-startup",
        control_addr="127.0.0.1:18081",
        capacity=1,
        queue_capacity=1,
        metadata={"startup_service": "true"},
        services={
            "svc-old": NodeServiceState(
                service_name="calc_asset_ratio",
                service_id="svc-old",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=1,
                alive_workers=1,
                http_base_url="http://127.0.0.1:18081/svc/svc-old",
            )
        },
    )
    info_state._nodes["startup-new"] = NodeState(  # noqa: SLF001
        node_instance_id="startup-new",
        node_id="calc_asset_ratio-startup",
        control_addr="127.0.0.1:18081",
        capacity=1,
        queue_capacity=1,
        metadata={"startup_service": "true"},
        services={
            "svc-new": NodeServiceState(
                service_name="calc_asset_ratio",
                service_id="svc-new",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=1,
                alive_workers=1,
                http_base_url="http://127.0.0.1:18081/svc/svc-new",
            )
        },
    )

    raw = _render_ops_page(info_state)

    assert raw.count("127.0.0.1:18081") == 2
    assert "startup-old" in raw
    assert "startup-new" in raw
    assert "merged_nodes=2" in raw
    assert raw.count("<td>calc_asset_ratio</td>") == 1


def test_ops_page_shows_service_and_taskpool_failure_reasons():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    failure_at = datetime(2026, 6, 1, 7, 30, 0, tzinfo=timezone.utc)
    info_state.register_node_record(
        node_instance_id="node-failed-1",
        node_id="node-failed",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=32,
        tags=["compute"],
        services={
            "svc-failed": NodeServiceState(
                service_name="calc_asset_ratio",
                service_id="svc-failed",
                status=pb2.SERVICE_STATUS_STOPPED,
                worker_count=2,
                alive_workers=0,
                stop_reason="ModuleNotFoundError: missing_pkg",
                failure_at=failure_at,
            )
        },
        task_pools={
            "pool-failed": NodeTaskPoolInfo(
                pool_id="pool-failed",
                owner_client_id="owner-1",
                pool_name="calc-pool",
                code_version="sha256:test",
                status="STOPPED",
                worker_count=2,
                failure_reason="executor host restart failed: missing_pkg",
                failure_at=failure_at,
            )
        },
    )

    raw = _render_ops_page(info_state)

    assert "failure_reason" in raw
    assert "2026-06-01T07:30:00+00:00" in raw
    assert "ModuleNotFoundError: missing_pkg" in raw
    assert "executor host restart failed: missing_pkg" in raw


def test_infocenter_preserves_failure_timestamp_across_heartbeats():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_state.register_node_record(
        node_instance_id="node-failed-1",
        node_id="node-failed",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=32,
        services={
            "svc-failed": NodeServiceState(
                service_name="calc_asset_ratio",
                service_id="svc-failed",
                status=pb2.SERVICE_STATUS_STOPPED,
                stop_reason="ModuleNotFoundError: missing_pkg",
            )
        },
        task_pools={
            "pool-failed": NodeTaskPoolInfo(
                pool_id="pool-failed",
                owner_client_id="owner-1",
                pool_name="calc-pool",
                code_version="sha256:test",
                status="STOPPED",
                failure_reason="executor host restart failed: missing_pkg",
            )
        },
    )
    first = info_state.list_nodes(healthy_only=False, tags=(), limit=10)[0]
    service_failure_at = first.services["svc-failed"].failure_at
    pool_failure_at = first.task_pools["pool-failed"].failure_at

    time.sleep(0.01)
    info_state.heartbeat_record(
        node_instance_id="node-failed-1",
        node_id="node-failed",
        healthy=True,
        services={
            "svc-failed": NodeServiceState(
                service_name="calc_asset_ratio",
                service_id="svc-failed",
                status=pb2.SERVICE_STATUS_STOPPED,
                stop_reason="ModuleNotFoundError: missing_pkg",
            )
        },
        task_pools={
            "pool-failed": NodeTaskPoolInfo(
                pool_id="pool-failed",
                owner_client_id="owner-1",
                pool_name="calc-pool",
                code_version="sha256:test",
                status="STOPPED",
                failure_reason="executor host restart failed: missing_pkg",
            )
        },
    )
    second = info_state.list_nodes(healthy_only=False, tags=(), limit=10)[0]

    assert service_failure_at is not None
    assert pool_failure_at is not None
    assert second.services["svc-failed"].failure_at == service_failure_at
    assert second.task_pools["pool-failed"].failure_at == pool_failure_at


def test_startup_service_registration_rejects_duplicate_service_name():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_state.register_node_record(
        node_instance_id="startup-a",
        node_id="startup-a",
        control_addr="127.0.0.1:18081",
        capacity=1,
        queue_capacity=1,
        metadata={"startup_service": "true"},
        services={
            "svc-a": NodeServiceState(
                service_name="calc_asset_ratio",
                service_id="svc-a",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=1,
                alive_workers=1,
            )
        },
    )

    with pytest.raises(ValueError, match="startup service_name already exists"):
        info_state.register_node_record(
            node_instance_id="startup-b",
            node_id="startup-b",
            control_addr="127.0.0.1:18082",
            capacity=1,
            queue_capacity=1,
            metadata={"startup_service": "true"},
            services={
                "svc-b": NodeServiceState(
                    service_name="calc_asset_ratio",
                    service_id="svc-b",
                    status=pb2.SERVICE_STATUS_RUNNING,
                    worker_count=1,
                    alive_workers=1,
                )
            },
        )


def test_startup_service_registration_replaces_same_endpoint_duplicate():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_state.register_node_record(
        node_instance_id="startup-old",
        node_id="startup-old",
        control_addr="",
        capacity=1,
        queue_capacity=1,
        metadata={"startup_service": "true"},
        services={
            "svc-old": NodeServiceState(
                service_name="calc_asset_ratio",
                service_id="svc-old",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=1,
                alive_workers=1,
                http_base_url="http://127.0.0.1:18081/svc/svc-old",
            )
        },
    )

    info_state.register_node_record(
        node_instance_id="startup-new",
        node_id="startup-new",
        control_addr="",
        capacity=1,
        queue_capacity=1,
        metadata={"startup_service": "true"},
        services={
            "svc-new": NodeServiceState(
                service_name="calc_asset_ratio",
                service_id="svc-new",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=1,
                alive_workers=1,
                http_base_url="http://127.0.0.1:18081/svc/svc-new",
            )
        },
    )

    nodes = info_state.list_nodes(healthy_only=True, tags=(), limit=10)
    routes = info_state.list_service_routes(service_name="calc_asset_ratio", healthy_only=True, limit=10)

    assert [node.node_instance_id for node in nodes] == ["startup-new"]
    assert [route["service_id"] for route in routes] == ["svc-new"]


def test_startup_service_registration_ignores_stopped_duplicate_service_name():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_state.register_node_record(
        node_instance_id="worker-a",
        node_id="worker-a",
        control_addr="127.0.0.1:50061",
        capacity=1,
        queue_capacity=1,
        metadata={},
        services={
            "svc-old": NodeServiceState(
                service_name="calc_asset_ratio",
                service_id="svc-old",
                status=pb2.SERVICE_STATUS_STOPPED,
                worker_count=1,
                alive_workers=0,
                http_base_url="http://127.0.0.1:18081/svc/svc-old",
            )
        },
    )

    info_state.register_node_record(
        node_instance_id="startup-new",
        node_id="startup-new",
        control_addr="",
        capacity=1,
        queue_capacity=1,
        metadata={"startup_service": "true"},
        services={
            "svc-new": NodeServiceState(
                service_name="calc_asset_ratio",
                service_id="svc-new",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=1,
                alive_workers=1,
                http_base_url="http://127.0.0.1:18886/svc/svc-new",
            )
        },
    )

    routes = info_state.list_service_routes(service_name="calc_asset_ratio", healthy_only=True, limit=10)

    assert [route["service_id"] for route in routes] == ["svc-new"]


def test_infocenter_replaces_existing_node_with_same_control_addr():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_state.register_node_record(
        node_instance_id="node-old-instance",
        node_id="node-old",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=32,
        tags=["compute"],
        version="old",
        services={
            "svc-old": NodeServiceState(
                service_name="calc_asset_ratio",
                service_id="svc-old",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=2,
                alive_workers=2,
                http_base_url="http://127.0.0.1:18081/svc/svc-old",
            ),
        },
    )

    info_state.register_node_record(
        node_instance_id="node-new-instance",
        node_id="node-new",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=32,
        tags=["compute"],
        version="new",
        services={
            "svc-new": NodeServiceState(
                service_name="calc_asset_ratio",
                service_id="svc-new",
                status=pb2.SERVICE_STATUS_RUNNING,
                worker_count=2,
                alive_workers=2,
                http_base_url="http://127.0.0.1:18081/svc/svc-new",
            ),
        },
    )

    nodes = info_state.list_nodes(healthy_only=True, tags=["compute"], limit=20)
    routes = info_state.list_service_routes(service_name="calc_asset_ratio", healthy_only=True, limit=20)

    assert [node.node_instance_id for node in nodes] == ["node-new-instance"]
    assert [route["service_id"] for route in routes] == ["svc-new"]


def test_infocenter_http_rejects_new_instance_when_control_addr_serves_old_instance(tmp_path):
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    old_state = NodeControlState(
        node_id="node-same-port",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "old_code_cache"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    old_server = NodeControlHttpServer(bind="127.0.0.1:0", state=old_state)
    info_server.start()
    old_server.start()
    try:
        with InfoCenterClient(info_server.base_url, timeout_sec=5.0) as client:
            first = client.register_node(
                node_id="node-same-port",
                node_instance_id=old_state.node_instance_id,
                control_addr=old_server.base_url,
                capacity=1,
                queue_capacity=4,
                tags=["compute"],
            )
            assert first["accepted"] is True
            second = client.register_node(
                node_id="node-same-port",
                node_instance_id="node-same-port-new",
                control_addr=old_server.base_url,
                capacity=1,
                queue_capacity=4,
                tags=["compute"],
            )

        assert second["accepted"] is False
        assert second["reset_required"] is True
        assert "actual_node_instance_id=" + old_state.node_instance_id in second["reason"]
        nodes = info_state.list_nodes(healthy_only=True, tags=["compute"], limit=20)
        assert [node.node_instance_id for node in nodes] == [old_state.node_instance_id]
    finally:
        old_server.stop()
        old_state.close()
        info_server.stop()


def test_infocenter_http_version_prefers_runtime_package_version(monkeypatch):
    import pycloud_parallel
    from pycloud_parallel.controlplane import infocenter_http

    monkeypatch.setattr(pycloud_parallel, "__version__", "runtime-ops-version", raising=False)
    monkeypatch.setattr(
        infocenter_http.importlib_metadata,
        "version",
        lambda _dist_name: "dist-metadata-version",
    )

    assert infocenter_http._pycloud_version() == "runtime-ops-version"


def test_registrar_version_prefers_runtime_package_version(monkeypatch):
    import pycloud_parallel
    from pycloud_parallel.controlplane import registrar

    monkeypatch.setattr(pycloud_parallel, "__version__", "runtime-registrar-version", raising=False)
    monkeypatch.setattr(
        registrar.importlib_metadata,
        "version",
        lambda _dist_name: "dist-metadata-version",
    )

    assert registrar._pycloud_version() == "runtime-registrar-version"


def test_ops_page_marks_lost_service_instances(tmp_path):
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    node_state = NodeControlState(
        node_id="node-ops-01",
        queue_capacity=32,
        worker_capacity=2,
        artifact_dir=str(tmp_path / "code_cache_ops"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
        monitor_interval_sec=1,
    )
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr=info_target,
        node_id="node-ops-01",
        control_addr="127.0.0.1:50071",
        state=node_state,
        capacity=2,
        queue_capacity=32,
        tags=["compute"],
        version="test-v1",
        fallback_heartbeat_sec=1,
    )

    try:
        registrar.start()
        with InfoCenterClient(info_target, timeout_sec=5.0) as infocenter:
            assert _wait_until(lambda: len(infocenter.list_nodes(healthy_only=True, tags=["compute"], limit=20)) == 1)

        blob = b"def run(**_kwargs):\n    return {'ok': True}\n"
        digest = hashlib.sha256(blob).hexdigest()
        node_state.create_service(
            owner_client_id="owner-ops",
            service_name="svc-ops",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="svc_ops",
            entry_callable="run",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            expose_http=True,
            chunks=[blob],
        )
        def _service_ready() -> bool:
            with InfoCenterClient(info_target, timeout_sec=5.0) as infocenter:
                return len(infocenter.list_service_routes(service_name="svc-ops", healthy_only=True, limit=20)) == 1

        assert _wait_until(_service_ready)

        with InfoCenterClient(info_target, timeout_sec=5.0) as infocenter:
            instance_id = infocenter.list_nodes(healthy_only=False, tags=["compute"], limit=20)[0].node_instance_id
        req = Request(f"{info_target}/ops/nodes/{instance_id}/mark-lost", method="POST", data=b"")
        with urlopen(req, timeout=5.0) as resp:
            assert resp.status == 200
        with InfoCenterClient(info_target, timeout_sec=5.0) as infocenter:
            routes = infocenter.list_service_routes(service_name="svc-ops", healthy_only=False, limit=20)
        assert len(routes) == 1
        assert routes[0].node_healthy is False
        assert routes[0].status == pb2.SERVICE_STATUS_UNSPECIFIED
        with urlopen(f"{info_target}/ops", timeout=5.0) as resp:
            raw = resp.read().decode("utf-8")
        assert "node_healthy" in raw
        assert "LOST" in raw
        assert "stale-row" in raw
    finally:
        registrar.close()
        node_state.close()
        info_server.stop()


def test_infocenter_http_rejects_fenced_instance_with_reset_required():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    try:
        with InfoCenterClient(info_target, timeout_sec=5.0) as client:
            client.register_node(
                node_id="node-fenced-http",
                node_instance_id="node-fenced-http-inst",
                control_addr="127.0.0.1:50061",
                capacity=2,
                queue_capacity=16,
                tags=["compute"],
            )
        info_state.mark_node_lost("node-fenced-http-inst", reason="test lost")

        payload = json.dumps(
            {
                "node_id": "node-fenced-http",
                "node_instance_id": "node-fenced-http-inst",
                "healthy": True,
            }
        ).encode("utf-8")
        req = Request(
            f"{info_target}/nodes/heartbeat",
            method="POST",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urlopen(req, timeout=5.0) as resp:
            body = json.loads(resp.read().decode("utf-8"))

        assert body["accepted"] is False
        assert body["reset_required"] is True
        assert body["new_instance_required"] is True
        assert body["reason"] == "test lost"
        assert body["error"] == "node_instance_id fenced"
    finally:
        info_server.stop()


def test_infocenter_client_returns_fenced_reset_response_for_node_sync():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    try:
        with InfoCenterClient(info_target, timeout_sec=5.0) as client:
            client.register_node(
                node_id="node-fenced-client",
                node_instance_id="node-fenced-client-inst",
                control_addr="127.0.0.1:50061",
                capacity=2,
                queue_capacity=16,
                tags=["compute"],
            )
            info_state.mark_node_lost("node-fenced-client-inst", reason="test lost")

            register_resp = client.register_node(
                node_id="node-fenced-client",
                node_instance_id="node-fenced-client-inst",
                control_addr="127.0.0.1:50061",
                capacity=2,
                queue_capacity=16,
                tags=["compute"],
            )
            heartbeat_resp = client.heartbeat_node(
                node_id="node-fenced-client",
                node_instance_id="node-fenced-client-inst",
                healthy=True,
            )

        for resp in (register_resp, heartbeat_resp):
            assert resp["accepted"] is False
            assert resp["reset_required"] is True
            assert resp["new_instance_required"] is True
            assert resp["reason"] == "test lost"
            assert resp["error"] == "node_instance_id fenced"
    finally:
        info_server.stop()


def test_node_registrar_handles_fenced_register_response(tmp_path):
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    node_state = NodeControlState(
        node_id="node-fenced-reg",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_fenced_reg"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr=info_target,
        node_id="node-fenced-reg",
        control_addr="127.0.0.1:50061",
        state=node_state,
        capacity=1,
        queue_capacity=4,
        tags=["compute"],
        fallback_heartbeat_sec=1,
        rpc_timeout_sec=5.0,
        exit_on_fence=False,
    )

    try:
        assert registrar.sync_now() is True
        info_state.mark_node_lost(node_state.node_instance_id, reason="test lost")
        registrar._registered = False  # noqa: SLF001

        assert registrar.sync_now() is False
        assert registrar._stop_event.is_set() is True  # noqa: SLF001
        assert node_state.service_report_payloads(include_stopped=True) == []
        assert node_state.task_pool_reports() == {}
    finally:
        registrar.close()
        node_state.close()
        info_server.stop()


def test_node_registrar_exits_host_after_fenced_register_response(tmp_path):
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    node_state = NodeControlState(
        node_id="node-fenced-reg-restart",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_fenced_reg_restart"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    exit_calls = []
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr=info_target,
        node_id="node-fenced-reg-restart",
        control_addr="127.0.0.1:50061",
        state=node_state,
        capacity=1,
        queue_capacity=4,
        tags=["compute"],
        fallback_heartbeat_sec=1,
        rpc_timeout_sec=5.0,
        exit_delay_sec=1.5,
        exit_on_fence=True,
        exit_callback=lambda delay_sec: exit_calls.append(delay_sec),
    )

    try:
        assert registrar.sync_now() is True
        info_state.mark_node_lost(node_state.node_instance_id, reason="test lost")
        registrar._registered = False  # noqa: SLF001

        assert registrar.sync_now() is False
        assert registrar._stop_event.is_set() is True  # noqa: SLF001
        assert node_state.execution_fenced is True
        assert exit_calls == [1.5]
    finally:
        registrar.close()
        node_state.close()
        info_server.stop()


def test_node_registrar_exits_host_after_fenced_heartbeat_response(tmp_path):
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    node_state = NodeControlState(
        node_id="node-fenced-heartbeat-restart",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_fenced_heartbeat_restart"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    exit_calls = []
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr=info_target,
        node_id="node-fenced-heartbeat-restart",
        control_addr="127.0.0.1:50061",
        state=node_state,
        capacity=1,
        queue_capacity=4,
        tags=["compute"],
        fallback_heartbeat_sec=1,
        rpc_timeout_sec=5.0,
        exit_delay_sec=1.5,
        exit_on_fence=True,
        exit_callback=lambda delay_sec: exit_calls.append(delay_sec),
    )

    try:
        assert registrar.sync_now() is True
        info_state.mark_node_lost(node_state.node_instance_id, reason="test lost")
        registrar._registered = True  # noqa: SLF001

        assert registrar.sync_now() is False
        assert registrar._stop_event.is_set() is True  # noqa: SLF001
        assert node_state.execution_fenced is True
        assert exit_calls == [1.5]
    finally:
        registrar.close()
        node_state.close()
        info_server.stop()


def test_node_registrar_exits_only_after_fence_cleanup():
    events = []

    class _FakeState:
        node_instance_id = "node-fence-order-inst"
        close_on_registration_lost = False

        def reset_execution_state(self, *, reason):
            events.append(("reset", reason))

        def close(self):
            events.append(("close", "state"))

    registrar = NodeInfoCenterRegistrar(
        infocenter_addr="http://127.0.0.1:9",
        node_id="node-fence-order",
        control_addr="127.0.0.1:50061",
        state=_FakeState(),
        capacity=1,
        queue_capacity=1,
        exit_on_fence=True,
        exit_delay_sec=1.25,
        exit_callback=lambda delay_sec: events.append(("exit", delay_sec)),
    )

    try:
        registrar._reset_state_after_fence("test fence", restart=True)  # noqa: SLF001

        assert events == [("reset", "test fence"), ("exit", 1.25)]
        assert registrar._stop_event.is_set() is True  # noqa: SLF001
    finally:
        registrar.close(mark_lost=False)


def test_node_registrar_exits_when_replaced_by_confirmed_new_instance(tmp_path, monkeypatch):
    import pycloud_parallel.controlplane.infocenter_http as infocenter_http

    class _ConfirmedReplacementNodeControlClient:
        def __init__(self, *_args, **_kwargs):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def node_status(self):
            return {
                "node_id": "node-replaced-heartbeat",
                "node_instance_id": "node-replaced-heartbeat-new",
            }

    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url
    monkeypatch.setattr(infocenter_http, "NodeControlClient", _ConfirmedReplacementNodeControlClient)

    node_state = NodeControlState(
        node_id="node-replaced-heartbeat",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_replaced_heartbeat"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    exit_calls = []
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr=info_target,
        node_id="node-replaced-heartbeat",
        control_addr="127.0.0.1:50061",
        state=node_state,
        capacity=1,
        queue_capacity=4,
        tags=["compute"],
        fallback_heartbeat_sec=1,
        rpc_timeout_sec=5.0,
        exit_delay_sec=1.5,
        exit_on_fence=True,
        exit_callback=lambda delay_sec: exit_calls.append(delay_sec),
    )

    try:
        assert registrar.sync_now() is True
        with InfoCenterClient(info_target, timeout_sec=5.0) as client:
            client.register_node(
                node_id="node-replaced-heartbeat",
                node_instance_id="node-replaced-heartbeat-new",
                control_addr="127.0.0.1:50061",
                capacity=1,
                queue_capacity=4,
                tags=["compute"],
            )

        registrar._registered = True  # noqa: SLF001
        assert registrar.sync_now() is False
        assert registrar._stop_event.is_set() is True  # noqa: SLF001
        assert node_state.execution_fenced is True
        assert exit_calls == [1.5]
    finally:
        registrar.close()
        node_state.close()
        info_server.stop()


def test_node_registrar_self_fences_after_local_lease_expires(tmp_path):
    node_state = NodeControlState(
        node_id="node-self-fence",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_self_fence"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr="http://127.0.0.1:9",
        node_id="node-self-fence",
        control_addr="127.0.0.1:50061",
        state=node_state,
        capacity=1,
        queue_capacity=4,
        fallback_heartbeat_sec=1,
        rpc_timeout_sec=0.5,
        exit_on_fence=False,
    )

    try:
        now = time.monotonic()
        registrar._registered = True  # noqa: SLF001
        registrar._last_successful_sync_at = now - 10.0  # noqa: SLF001
        registrar._lease_ttl_sec = 1  # noqa: SLF001

        assert registrar.sync_now() is False
        assert registrar._stop_event.is_set() is True  # noqa: SLF001
        assert node_state.service_report_payloads(include_stopped=True) == []
        assert node_state.task_pool_reports() == {}
    finally:
        registrar.close()
        node_state.close()


def test_node_registrar_transient_disconnect_does_not_close_before_lease_expires(tmp_path, monkeypatch):
    node_state = NodeControlState(
        node_id="node-transient-disconnect",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_transient_disconnect"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    node_state.close_on_registration_lost = True
    closed = []
    monkeypatch.setattr(node_state, "close", lambda: closed.append(True))
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr="http://127.0.0.1:9",
        node_id="node-transient-disconnect",
        control_addr="127.0.0.1:50061",
        state=node_state,
        capacity=1,
        queue_capacity=4,
        fallback_heartbeat_sec=1,
        rpc_timeout_sec=0.5,
        exit_on_fence=False,
    )

    def _raise_transient():
        raise URLError(ConnectionRefusedError("temporarily unavailable"))

    monkeypatch.setattr(registrar, "_heartbeat_once", _raise_transient)
    try:
        now = time.monotonic()
        registrar._registered = True  # noqa: SLF001
        registrar._last_successful_sync_at = now  # noqa: SLF001
        registrar._lease_ttl_sec = 30  # noqa: SLF001

        assert registrar.sync_now() is False
        assert closed == []
        assert registrar._stop_event.is_set() is False  # noqa: SLF001
        assert node_state.execution_fenced is False
    finally:
        registrar.close(mark_lost=False)
        node_state.close()


def test_node_registrar_wrapped_transient_disconnect_does_not_close_before_lease_expires(tmp_path, monkeypatch):
    node_state = NodeControlState(
        node_id="node-wrapped-transient-disconnect",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_wrapped_transient_disconnect"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    node_state.close_on_registration_lost = True
    closed = []
    monkeypatch.setattr(node_state, "close", lambda: closed.append(True))
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr="http://127.0.0.1:9",
        node_id="node-wrapped-transient-disconnect",
        control_addr="127.0.0.1:50061",
        state=node_state,
        capacity=1,
        queue_capacity=4,
        fallback_heartbeat_sec=1,
        rpc_timeout_sec=0.5,
        exit_on_fence=False,
    )

    def _raise_wrapped_transient():
        raise RuntimeError("http request to 127.0.0.1:9 failed: [WinError 10061] connection refused")

    monkeypatch.setattr(registrar, "_heartbeat_once", _raise_wrapped_transient)
    try:
        now = time.monotonic()
        registrar._registered = True  # noqa: SLF001
        registrar._last_successful_sync_at = now  # noqa: SLF001
        registrar._lease_ttl_sec = 30  # noqa: SLF001

        assert registrar.sync_now() is False
        assert closed == []
        assert registrar._stop_event.is_set() is False  # noqa: SLF001
        assert node_state.execution_fenced is False
    finally:
        registrar.close(mark_lost=False)
        node_state.close()


def test_node_registrar_closes_close_on_lost_after_wrapped_transient_lease_expires(tmp_path, monkeypatch):
    node_state = NodeControlState(
        node_id="node-wrapped-transient-expired",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_wrapped_transient_expired"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    node_state.close_on_registration_lost = True
    closed = []
    monkeypatch.setattr(node_state, "close", lambda: closed.append(True))
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr="http://127.0.0.1:9",
        node_id="node-wrapped-transient-expired",
        control_addr="127.0.0.1:50061",
        state=node_state,
        capacity=1,
        queue_capacity=4,
        fallback_heartbeat_sec=1,
        rpc_timeout_sec=0.5,
        exit_on_fence=False,
    )

    def _raise_wrapped_transient():
        raise RuntimeError("connection to 127.0.0.1:9 was closed by the remote service")

    monkeypatch.setattr(registrar, "_heartbeat_once", _raise_wrapped_transient)
    try:
        now = time.monotonic()
        registrar._registered = True  # noqa: SLF001
        registrar._last_successful_sync_at = now - 10.0  # noqa: SLF001
        registrar._lease_ttl_sec = 1  # noqa: SLF001

        assert registrar.sync_now() is False
        assert closed == [True]
        assert registrar._stop_event.is_set() is True  # noqa: SLF001
        assert node_state.execution_fenced is True
    finally:
        registrar.close(mark_lost=False)
        node_state.close()


def test_node_registrar_does_not_exit_after_self_fence_by_default(tmp_path):
    node_state = NodeControlState(
        node_id="node-self-fence-restart",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_self_fence_restart"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    exit_calls = []
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr="http://127.0.0.1:9",
        node_id="node-self-fence-restart",
        control_addr="127.0.0.1:50061",
        state=node_state,
        capacity=1,
        queue_capacity=4,
        fallback_heartbeat_sec=1,
        rpc_timeout_sec=0.5,
        exit_delay_sec=2.0,
        exit_callback=lambda delay_sec: exit_calls.append(delay_sec),
    )

    try:
        now = time.monotonic()
        registrar._registered = True  # noqa: SLF001
        registrar._last_successful_sync_at = now - 10.0  # noqa: SLF001
        registrar._lease_ttl_sec = 1  # noqa: SLF001

        assert registrar.sync_now() is False
        assert registrar._stop_event.is_set() is True  # noqa: SLF001
        assert exit_calls == []
        assert node_state.execution_fenced is True
    finally:
        registrar.close()
        node_state.close()


def test_node_registrar_exits_after_self_fence_when_enabled(tmp_path):
    node_state = NodeControlState(
        node_id="node-self-fence-restart-enabled",
        queue_capacity=4,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_self_fence_restart_enabled"),
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
    )
    exit_calls = []
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr="http://127.0.0.1:9",
        node_id="node-self-fence-restart-enabled",
        control_addr="127.0.0.1:50061",
        state=node_state,
        capacity=1,
        queue_capacity=4,
        fallback_heartbeat_sec=1,
        rpc_timeout_sec=0.5,
        exit_delay_sec=2.0,
        exit_on_fence=True,
        exit_callback=lambda delay_sec: exit_calls.append(delay_sec),
    )

    try:
        now = time.monotonic()
        registrar._registered = True  # noqa: SLF001
        registrar._last_successful_sync_at = now - 10.0  # noqa: SLF001
        registrar._lease_ttl_sec = 1  # noqa: SLF001

        assert registrar.sync_now() is False
        assert registrar._stop_event.is_set() is True  # noqa: SLF001
        assert exit_calls == [2.0]
        assert node_state.execution_fenced is True
    finally:
        registrar.close()
        node_state.close()


def test_infocenter_tracks_nodes_that_do_not_accept_service_deploy():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    try:
        with InfoCenterClient(info_target, timeout_sec=5.0) as infocenter:
            infocenter.register_node(
                node_id="job-orchestrator-01",
                node_instance_id="job-orchestrator-01-inst",
                control_addr="",
                capacity=1,
                queue_capacity=4000,
                tags=["job"],
                metadata={"component": "job-orchestrator", "accept_service_deploy": "false"},
                accept_service_deploy=False,
            )

            nodes = infocenter.list_nodes(healthy_only=True, limit=20)

        assert len(nodes) == 1
        assert nodes[0].node_id == "job-orchestrator-01"
        assert nodes[0].accept_service_deploy is False
    finally:
        info_server.stop()


def test_ops_page_shows_job_queue_status_section():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    try:
        with InfoCenterClient(info_target, timeout_sec=5.0) as infocenter:
            infocenter.register_node(
                node_id="job-orchestrator-01",
                node_instance_id="job-orchestrator-01-inst",
                control_addr="",
                capacity=1,
                queue_capacity=4000,
                tags=["job"],
                metadata={
                    "component": "job-orchestrator",
                    "pycloud_version": "test-version",
                    "current_job_id": "job-123",
                    "current_job_status": "RUNNING",
                    "job_waiting": "4",
                    "job_running": "1",
                    "job_terminal": "7",
                    "job_recent": (
                        '[{"job_id":"job-123","status":"RUNNING","submitted_at":"2026-01-01T00:00:00+00:00","finished_at":"","final_result_preview":"","error_preview":""},'
                        '{"job_id":"job-122","status":"SUCCEEDED","submitted_at":"2025-12-31T23:59:00+00:00","finished_at":"2026-01-01T00:00:10+00:00","final_result_preview":"{\\"count\\":3}","error_preview":""}]'
                    ),
                    "job_waiting_list": (
                        '[{"job_id":"job-200","priority":5,"submitted_at":"2026-01-01T00:01:00+00:00","position":1},'
                        '{"job_id":"job-201","priority":4,"submitted_at":"2026-01-01T00:02:00+00:00","position":2}]'
                    ),
                },
                services=[
                    {
                        "service_name": "job-orchestrator",
                        "service_id": "svc-job-1",
                        "status": int(pb2.SERVICE_STATUS_RUNNING),
                        "worker_count": 1,
                        "alive_workers": 1,
                        "in_flight": 1,
                        "http_base_url": "http://127.0.0.1:50053/svc/svc-job-1",
                    }
                ],
                service_worker_capacity=1,
                service_worker_used=1,
            )

        with urlopen(f"{info_target}/ops", timeout=5.0) as resp:
            raw = resp.read().decode("utf-8")
        assert "Job Queue" in raw
        assert "Recent Jobs" in raw
        assert "Waiting Jobs" in raw
        assert "job-123" in raw
        assert "job-122" in raw
        assert "job-200" in raw
        assert "job-201" in raw
        assert "href='http://127.0.0.1:50053/svc/svc-job-1/jobs/job-123?view=html'" in raw
        assert "href='http://127.0.0.1:50053/svc/svc-job-1/jobs/job-122?view=html'" in raw
        assert "/ops/job-queues/job-orchestrator-01-inst/jobs/job-200/move-up" in raw
        assert "/ops/job-queues/job-orchestrator-01-inst/jobs/job-201/move-down" in raw
        assert "RUNNING" in raw
        assert ">4</td>" in raw
        assert ">1</td>" in raw
        assert ">7</td>" in raw
        assert "count" in raw
        assert "http://127.0.0.1:50053/svc/svc-job-1" in raw
        assert "overflow-wrap:anywhere" in raw
    finally:
        info_server.stop()


def test_ops_job_queue_reorder_proxies_to_job_orchestrator():
    admin_token = "ops-admin-token"
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url
    orchestrator = build_job_orchestrator_server(
        "127.0.0.1:0",
        infocenter_addr=info_target,
        node_id="job-orchestrator-proxy",
        admin_token=admin_token,
    )
    orchestrator.start()
    with orchestrator.job_queue._cv:  # noqa: SLF001
        orchestrator.job_queue._stop = True  # noqa: SLF001

    try:
        orchestrator.job_queue.submit_job({"job_id": "job-a", "client_id": "c", "entry_module": "m", "task_generator_callable": [{"value": 1}]})
        orchestrator.job_queue.submit_job({"job_id": "job-b", "client_id": "c", "entry_module": "m", "task_generator_callable": [{"value": 2}]})
        orchestrator.job_queue.submit_job({"job_id": "job-c", "client_id": "c", "entry_module": "m", "task_generator_callable": [{"value": 3}]})
        service_http_base = f"{orchestrator.base_url}/svc/{orchestrator.service_id}"

        with pytest.raises(RuntimeError, match="admin auth required"):
            _reorder_job_via_http(service_http_base, "job-c", direction="up")

        resp = _reorder_job_via_http(service_http_base, "job-c", direction="up", auth_token=admin_token)
        waiting_ids = [item["job_id"] for item in resp["queue"]["waiting_jobs"]]
        assert waiting_ids == ["job-a", "job-c", "job-b"]
    finally:
        orchestrator.stop()
        info_server.stop()


def test_infocenter_client_select_task_nodes_prefers_credit():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    try:
        with InfoCenterClient(info_target, timeout_sec=5.0) as client:
            client.register_node(
                node_id="node-low",
                control_addr="127.0.0.1:50061",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
            )
            client.register_node(
                node_id="node-high",
                control_addr="127.0.0.1:50062",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
            )
            client.register_node(
                node_id="node-drain",
                control_addr="127.0.0.1:50063",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
            )

            client.heartbeat_node(
                node_id="node-low",
                healthy=True,
                metrics={"queued": 3, "inflight": 2, "running": 2, "credit": 4},
            )
            client.heartbeat_node(
                node_id="node-high",
                healthy=True,
                metrics={"queued": 1, "inflight": 1, "running": 1, "credit": 9},
            )
            info_state.update_node_schedule_state("node-drain", drain=True)
            client.heartbeat_node(
                node_id="node-drain",
                healthy=True,
                metrics={"queued": 0, "inflight": 0, "running": 0, "credit": 15},
            )

            selected = list(
                client.select_task_nodes(
                    healthy_only=True,
                    tags=["compute"],
                    node_count=2,
                    limit=10,
                    require_credit=True,
                )
            )
            assert [node.node_id for node in selected] == ["node-high", "node-low"]
    finally:
        info_server.stop()


def test_infocenter_client_select_task_nodes_prefers_hot_runtime():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    try:
        with InfoCenterClient(info_target, timeout_sec=5.0) as client:
            client.register_node(
                node_id="node-cold-high-credit",
                control_addr="127.0.0.1:50071",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
            )
            client.register_node(
                node_id="node-hot-lower-credit",
                control_addr="127.0.0.1:50072",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
            )

            client.heartbeat_node(
                node_id="node-cold-high-credit",
                healthy=True,
                metrics={"queued": 0, "inflight": 0, "running": 0, "credit": 10},
                active_runtimes=["other-runtime"],
            )
            client.heartbeat_node(
                node_id="node-hot-lower-credit",
                healthy=True,
                metrics={"queued": 1, "inflight": 1, "running": 1, "credit": 6},
                active_runtimes=["runtime-hot", "other-runtime"],
            )

            selected = list(
                client.select_task_nodes(
                    healthy_only=True,
                    tags=["compute"],
                    node_count=2,
                    limit=10,
                    require_credit=True,
                    preferred_runtime_key="runtime-hot",
                )
            )
            assert [node.node_id for node in selected] == ["node-hot-lower-credit", "node-cold-high-credit"]
            assert selected[0].active_runtimes == ("runtime-hot", "other-runtime")
    finally:
        info_server.stop()


def test_infocenter_client_select_task_nodes_accepts_explicit_node_ids():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    try:
        with InfoCenterClient(info_target, timeout_sec=5.0) as client:
            client.register_node(
                node_id="node-a",
                control_addr="127.0.0.1:50061",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
            )
            client.register_node(
                node_id="node-b",
                control_addr="127.0.0.1:50062",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
            )

            selected = list(
                client.select_task_nodes(
                    healthy_only=True,
                    node_ids=["node-b"],
                    limit=10,
                )
            )
            assert [node.node_id for node in selected] == ["node-b"]
    finally:
        info_server.stop()


def test_infocenter_client_select_task_nodes_rejects_explicit_cordon_or_drain_nodes():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    try:
        with InfoCenterClient(info_target, timeout_sec=5.0) as client:
            client.register_node(
                node_id="node-cordon",
                node_instance_id="node-cordon-inst",
                control_addr="127.0.0.1:50061",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
            )
            client.register_node(
                node_id="node-drain-explicit",
                node_instance_id="node-drain-inst",
                control_addr="127.0.0.1:50062",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
            )
            info_state.update_node_schedule_state("node-cordon-inst", schedulable=False)
            info_state.update_node_schedule_state("node-drain-inst", drain=True)

            with pytest.raises(RuntimeError, match="not deployable.*cordon"):
                list(client.select_task_nodes(healthy_only=True, node_ids=["node-cordon"], limit=10))
            with pytest.raises(RuntimeError, match="not deployable.*drain"):
                list(client.select_task_nodes(healthy_only=True, node_instance_ids=["node-drain-inst"], limit=10))
    finally:
        info_server.stop()


def test_infocenter_managed_tags_are_endpoint_profiles(tmp_path):
    profiles_path = tmp_path / "profiles.json"
    state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5, profiles_path=profiles_path)
    profile_key = normalize_node_profile_key("http://127.0.0.1:50061/")
    assert profile_key == "127.0.0.1:50061"

    state.update_node_profile("http://127.0.0.1:50061/", managed_tags=["gpu", "manual"], notes="ops note")
    node = state.register_node_record(
        node_id="node-profile-a",
        node_instance_id="node-profile-a-inst",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=20,
        tags=["legacy"],
        python_version="3.11.9",
    )

    assert node.profile_key == profile_key
    assert node.managed_tags == ["gpu", "manual"]
    assert node.legacy_node_tags == ["legacy"]
    assert "python:3.x" in node.capability_tags
    assert "role:compute" in node.capability_tags
    assert set(node.tags) >= {"gpu", "manual", "legacy", "runtime:py3", "role:compute"}
    assert state.list_nodes(healthy_only=True, tags=["legacy"], limit=10)[0].node_id == "node-profile-a"
    assert state.list_nodes(healthy_only=True, tags=["manual"], limit=10)[0].node_id == "node-profile-a"

    raw = json.loads(profiles_path.read_text(encoding="utf-8"))
    saved = raw["profiles"][profile_key]
    assert saved == {
        "profile_key": profile_key,
        "managed_tags": ["gpu", "manual"],
        "enabled": True,
        "drain": False,
        "notes": "ops note",
    }
    assert "capability_tags" not in json.dumps(raw)
    assert "legacy_node_tags" not in json.dumps(raw)

    restarted = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5, profiles_path=profiles_path)
    restored = restarted.register_node_record(
        node_id="node-profile-a",
        node_instance_id="node-profile-new-inst",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=20,
        tags=["legacy2"],
    )

    assert restored.node_instance_id == "node-profile-new-inst"
    assert restored.managed_tags == ["gpu", "manual"]
    assert "manual" in restored.tags
    assert "legacy2" in restored.tags
    assert restarted.list_nodes(healthy_only=True, tags=["manual"], limit=10)[0].node_instance_id == "node-profile-new-inst"


def test_infocenter_tag_profile_boundaries_survive_register_and_heartbeat(tmp_path):
    profiles_path = tmp_path / "profiles.json"
    state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5, profiles_path=profiles_path)
    state.update_node_profile("127.0.0.1:50061", managed_tags=["manual", "shared"], notes="kept")

    node = state.register_node_record(
        node_id="node-boundary",
        node_instance_id="node-boundary-inst",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=20,
        tags=["legacy-a", "shared"],
        metadata={"component": "compute"},
        python_version="3.12.1",
    )
    expected_tags = sorted(set(node.managed_tags + node.capability_tags + node.legacy_node_tags))
    assert node.tags == expected_tags
    assert node.managed_tags == ["manual", "shared"]
    assert node.legacy_node_tags == ["legacy-a", "shared"]
    assert state.list_nodes(healthy_only=True, tags=["manual"], limit=10)[0].node_id == "node-boundary"
    assert state.list_nodes(healthy_only=True, tags=["legacy-a"], limit=10)[0].node_id == "node-boundary"
    assert state.list_nodes(healthy_only=True, tags=["role:compute"], limit=10)[0].node_id == "node-boundary"

    rereregistered = state.register_node_record(
        node_id="node-boundary",
        node_instance_id="node-boundary-inst",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=20,
        tags=["legacy-b"],
        metadata={"component": "job-orchestrator"},
        accept_service_deploy=False,
        python_version="3.12.1",
    )
    assert rereregistered.managed_tags == ["manual", "shared"]
    assert rereregistered.legacy_node_tags == ["legacy-b"]
    assert "role:job" in rereregistered.capability_tags
    assert "role:compute" not in rereregistered.capability_tags
    assert rereregistered.tags == sorted(set(rereregistered.managed_tags + rereregistered.capability_tags + rereregistered.legacy_node_tags))

    heartbeated = state.heartbeat_record(
        node_id="node-boundary",
        node_instance_id="node-boundary-inst",
        healthy=True,
        metadata={"component": "compute"},
        python_version="3.11.8",
        accept_service_deploy=True,
    )
    assert heartbeated is not None
    assert heartbeated.managed_tags == ["manual", "shared"]
    assert "role:compute" in heartbeated.capability_tags
    assert "role:job" not in heartbeated.capability_tags
    assert heartbeated.tags == sorted(set(heartbeated.managed_tags + heartbeated.capability_tags + heartbeated.legacy_node_tags))

    raw = json.loads(profiles_path.read_text(encoding="utf-8"))
    allowed = {"profile_key", "managed_tags", "enabled", "drain", "notes"}
    for item in raw["profiles"].values():
        assert set(item) == allowed
    serialized = json.dumps(raw)
    assert "capability_tags" not in serialized
    assert "legacy_node_tags" not in serialized
    assert '"tags"' not in serialized


def test_infocenter_profile_enabled_and_drain_block_task_selection(tmp_path):
    state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5, profiles_path=tmp_path / "profiles.json")
    server = InfoCenterHttpServer(bind="127.0.0.1:0", state=state)
    server.start()

    try:
        with InfoCenterClient(server.base_url, timeout_sec=5.0) as client:
            client.register_node(
                node_id="node-profile-blocked",
                node_instance_id="node-profile-blocked-inst",
                control_addr="127.0.0.1:50061",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
            )
            state.update_node_profile("127.0.0.1:50061", enabled=False)
            blocked = client.list_nodes(healthy_only=True, tags=["compute"], limit=10)[0]
            assert blocked.profile_enabled is False
            assert blocked.schedulable is False
            with pytest.raises(RuntimeError, match="no schedulable task nodes"):
                list(client.select_task_nodes(healthy_only=True, tags=["compute"], limit=10))

            state.update_node_profile("127.0.0.1:50061", enabled=True, drain=True)
            drained = client.list_nodes(healthy_only=True, tags=["compute"], limit=10)[0]
            assert drained.profile_enabled is True
            assert drained.drain is True
            with pytest.raises(RuntimeError, match="no schedulable task nodes"):
                list(client.select_task_nodes(healthy_only=True, tags=["compute"], limit=10))
    finally:
        server.stop()


def test_ops_managed_tag_form_updates_endpoint_profile(tmp_path, monkeypatch):
    import pycloud_parallel.controlplane.infocenter_http as infocenter_http

    class _ConfirmedReplacementNodeControlClient:
        def __init__(self, *_args, **_kwargs):
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def node_status(self):
            return {
                "node_id": "node-ops-profile",
                "node_instance_id": "node-ops-profile-new-inst",
            }

    state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5, profiles_path=tmp_path / "profiles.json")
    server = InfoCenterHttpServer(bind="127.0.0.1:0", state=state)
    server.start()
    monkeypatch.setattr(infocenter_http, "NodeControlClient", _ConfirmedReplacementNodeControlClient)

    try:
        with InfoCenterClient(server.base_url, timeout_sec=5.0) as client:
            client.register_node(
                node_id="node-ops-profile",
                node_instance_id="node-ops-profile-inst",
                control_addr="127.0.0.1:50061",
                capacity=4,
                queue_capacity=20,
                tags=["legacy"],
            )

        body = urlencode({"tag": "manual-ops", "op": "add"}).encode("utf-8")
        req = Request(
            f"{server.base_url}/ops/nodes/node-ops-profile-inst/managed-tags",
            method="POST",
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urlopen(req, timeout=5.0) as resp:
            assert resp.status == 200

        node = state.list_nodes(healthy_only=True, tags=["manual-ops"], limit=10)[0]
        assert node.managed_tags == ["manual-ops"]
        assert "manual-ops" in node.tags

        with InfoCenterClient(server.base_url, timeout_sec=5.0) as client:
            client.register_node(
                node_id="node-ops-profile",
                node_instance_id="node-ops-profile-new-inst",
                control_addr="http://127.0.0.1:50061",
                capacity=4,
                queue_capacity=20,
                tags=[],
            )
            restored = client.list_nodes(healthy_only=True, tags=["manual-ops"], limit=10)[0]
        assert restored.node_instance_id == "node-ops-profile-new-inst"
        assert restored.managed_tags == ("manual-ops",)
    finally:
        server.stop()


def test_infocenter_client_select_task_nodes_detects_duplicate_node_ids_and_supports_instance_ids():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    try:
        with InfoCenterClient(info_target, timeout_sec=5.0) as client:
            client.register_node(
                node_id="node-dup",
                node_instance_id="node-dup-a",
                control_addr="127.0.0.1:50061",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
            )
            client.register_node(
                node_id="node-dup",
                node_instance_id="node-dup-b",
                control_addr="127.0.0.1:50062",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
            )

            with pytest.raises(RuntimeError, match="ambiguous"):
                list(client.select_task_nodes(healthy_only=True, node_ids=["node-dup"], limit=10))

            selected = list(
                client.select_task_nodes(
                    healthy_only=True,
                    node_instance_ids=["node-dup-b"],
                    limit=10,
                )
            )
            assert [node.node_instance_id for node in selected] == ["node-dup-b"]
            assert [node.control_addr for node in selected] == ["127.0.0.1:50062"]
    finally:
        info_server.stop()


def test_registering_restarted_node_prunes_stale_same_addr_instance():
    state = InfoCenterState(lease_ttl_sec=5, heartbeat_interval_sec=2)
    first = state.register_node_record(
        node_instance_id="node-a-old",
        node_id="node-a",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=20,
        tags=["compute"],
    )
    assert first.node_instance_id == "node-a-old"

    state.mark_node_lost("node-a-old", reason="restart")

    second = state.register_node_record(
        node_instance_id="node-a-new",
        node_id="node-a",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=20,
        tags=["compute"],
    )
    assert second.node_instance_id == "node-a-new"

    nodes = state.list_nodes(healthy_only=False, tags=[], limit=20)
    assert [node.node_instance_id for node in nodes] == ["node-a-new"]


def test_infocenter_rejects_same_control_addr_when_status_probe_unconfirmed(monkeypatch):
    import pycloud_parallel.controlplane.infocenter_http as infocenter_http

    class _ProbeFailsNodeControlClient:
        def __init__(self, *_args, **_kwargs):
            return None

        def __enter__(self):
            raise TimeoutError("probe timed out")

        def __exit__(self, exc_type, exc, tb):
            return None

    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url
    monkeypatch.setattr(infocenter_http, "NodeControlClient", _ProbeFailsNodeControlClient)

    try:
        with InfoCenterClient(info_target, timeout_sec=5.0) as client:
            first = client.register_node(
                node_id="node-a",
                node_instance_id="node-a-old",
                control_addr="127.0.0.1:50061",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
            )
            assert first["accepted"] is True

            second = client.register_node(
                node_id="node-a",
                node_instance_id="node-a-new",
                control_addr="127.0.0.1:50061",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
            )

        assert second["accepted"] is False
        assert second["retryable"] is True
        assert second["error"] == "node control_addr replacement not confirmed"
        nodes = info_state.list_nodes(healthy_only=False, tags=[], limit=20)
        assert [node.node_instance_id for node in nodes] == ["node-a-old"]
        assert info_state.is_instance_fenced("node-a-old") is False
    finally:
        info_server.stop()


def test_infocenter_client_mark_node_lost_preserves_reason():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()

    try:
        with InfoCenterClient(info_server.base_url, timeout_sec=5.0) as client:
            client.register_node(
                node_id="node-lost-reason",
                node_instance_id="node-lost-reason-inst",
                control_addr="127.0.0.1:50061",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
            )
            response = client.mark_node_lost(
                "node-lost-reason-inst",
                reason="task pool create identity mismatch",
            )

        assert response["ok"] is True
        nodes = info_state.list_nodes(healthy_only=False, tags=[], limit=20)
        assert nodes[0].reason == "task pool create identity mismatch"
        assert info_state.fenced_instance_reason("node-lost-reason-inst") == "task pool create identity mismatch"
    finally:
        info_server.stop()


def test_runtime_spec_helpers_support_exact_major_and_comparators():
    assert normalize_python_runtime_spec("3.11") == "py3.11"
    assert normalize_python_runtime_spec(">=3.11") == ">=py3.11"
    assert matches_python_runtime("py3.13", "py3") is True
    assert matches_python_runtime("py3.13", "py3.11") is False
    assert matches_python_runtime("py3.13", ">=py3.11") is True
    assert matches_python_runtime("py3.10", ">=py3.11") is False


def test_infocenter_client_select_task_nodes_filters_by_python_runtime():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    try:
        with InfoCenterClient(info_target, timeout_sec=5.0) as client:
            client.register_node(
                node_id="node-py310",
                control_addr="127.0.0.1:50101",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
                python_version="py3.10",
            )
            client.register_node(
                node_id="node-py311",
                control_addr="127.0.0.1:50102",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
                python_version="py3.11",
            )
            client.register_node(
                node_id="node-py313",
                control_addr="127.0.0.1:50103",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
                python_version="py3.13",
            )

            for node_id in ("node-py310", "node-py311", "node-py313"):
                client.heartbeat_node(
                    node_id=node_id,
                    healthy=True,
                    metrics={"queued": 0, "inflight": 0, "running": 0, "credit": 8},
                    python_version={
                        "node-py310": "py3.10",
                        "node-py311": "py3.11",
                        "node-py313": "py3.13",
                    }[node_id],
                )

            selected_exact = list(
                client.select_task_nodes(
                    healthy_only=True,
                    tags=["compute"],
                    node_count=10,
                    runtime="py3.11",
                )
            )
            assert [node.node_id for node in selected_exact] == ["node-py311"]

            selected_ge = list(
                client.select_task_nodes(
                    healthy_only=True,
                    tags=["compute"],
                    node_count=10,
                    runtime=">=py3.11",
                )
            )
            assert [node.node_id for node in selected_ge] == ["node-py311", "node-py313"]
    finally:
        info_server.stop()


def test_infocenter_client_select_task_nodes_skips_startup_only_nodes():
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=5)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    try:
        with InfoCenterClient(info_target, timeout_sec=5.0) as client:
            client.register_node(
                node_id="node-compute",
                control_addr="127.0.0.1:50101",
                capacity=4,
                queue_capacity=20,
                tags=["compute"],
                accept_service_deploy=True,
            )
            client.register_node(
                node_id="node-startup-only",
                control_addr="127.0.0.1:18080",
                capacity=4,
                queue_capacity=20,
                tags=["startup-service"],
                accept_service_deploy=False,
            )
            for node_id in ("node-compute", "node-startup-only"):
                client.heartbeat_node(
                    node_id=node_id,
                    healthy=True,
                    metrics={"queued": 0, "inflight": 0, "running": 0, "credit": 8},
                    accept_service_deploy=(node_id == "node-compute"),
                )

            selected = list(
                client.select_task_nodes(
                    healthy_only=True,
                    node_count=10,
                    require_credit=False,
                )
            )
            assert [node.node_id for node in selected] == ["node-compute"]
    finally:
        info_server.stop()


def test_node_registrar_close_sends_final_empty_startup_service_snapshot():
    from pycloud_parallel.controlplane.startup_service_node import StartupServiceNode

    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    node_state = StartupServiceNode(
        node_id="startup-close-node",
        service_http_bind="",
        service_http_base_url="http://127.0.0.1:19080",
        enable_internal_executor=False,
        enable_service_session=True,
    )
    node_state.mount_python_module_service(
        service_name="startup-close-service",
        entry_module="math",
        export_methods=("sqrt",),
        policy_id="trusted_internal",
    )
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr=info_server.base_url,
        node_id=node_state.node_id,
        control_addr="",
        state=node_state,
        capacity=1,
        queue_capacity=1,
        tags=["startup-service"],
        fallback_heartbeat_sec=1,
    )
    try:
        assert registrar.sync_now() is True
        routes = info_state.list_service_routes(service_name="startup-close-service", healthy_only=True, limit=10)
        assert len(routes) == 1

        registrar.close()

        routes = info_state.list_service_routes(service_name="startup-close-service", healthy_only=True, limit=10)
        assert routes == []
        assert node_state._closed.is_set() is True  # noqa: SLF001
    finally:
        registrar.close(mark_lost=False)
        node_state.close()
        info_server.stop()


def test_startup_service_registrar_closes_after_fenced_register_response():
    from pycloud_parallel.controlplane.startup_service_node import StartupServiceNode

    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    node_state = StartupServiceNode(
        node_id="startup-fenced-node",
        service_http_bind="",
        service_http_base_url="http://127.0.0.1:19082",
        enable_internal_executor=False,
        enable_service_session=True,
    )
    node_state.close_on_registration_lost = True
    node_state.mount_python_module_service(
        service_name="startup-fenced-service",
        entry_module="math",
        export_methods=("sqrt",),
        policy_id="trusted_internal",
    )
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr=info_server.base_url,
        node_id=node_state.node_id,
        control_addr="",
        state=node_state,
        capacity=1,
        queue_capacity=1,
        tags=["startup-service"],
        fallback_heartbeat_sec=1,
    )
    try:
        assert registrar.sync_now() is True
        info_state.mark_node_lost(registrar.node_instance_id, reason="test startup fence")
        registrar._registered = False  # noqa: SLF001

        assert registrar.sync_now() is False
        assert registrar._stop_event.is_set() is True  # noqa: SLF001
        assert node_state._closed.is_set() is True  # noqa: SLF001
        assert info_state.list_service_routes(
            service_name="startup-fenced-service",
            healthy_only=True,
            limit=10,
        ) == []
    finally:
        registrar.close(mark_lost=False)
        node_state.close()
        info_server.stop()


def test_node_runtime_close_unregisters_startup_service_route():
    from pycloud_parallel.controlplane.startup_service_node import StartupServiceNode

    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    node_state = StartupServiceNode(
        node_id="startup-runtime-close-node",
        service_http_bind="",
        service_http_base_url="http://127.0.0.1:19081",
        enable_internal_executor=False,
        enable_service_session=True,
    )
    try:
        node_state.mount_python_module_service(
            service_name="startup-runtime-close-service",
            entry_module="math",
            export_methods=("sqrt",),
            policy_id="trusted_internal",
        )
        node_state.start_infocenter_registration(
            infocenter_target=info_server.base_url,
            tags=["startup-service"],
            heartbeat_sec=1,
            rpc_timeout_sec=2.0,
        )
        assert _wait_until(
            lambda: len(info_state.list_service_routes(
                service_name="startup-runtime-close-service",
                healthy_only=True,
                limit=10,
            )) == 1,
            timeout_sec=3.0,
        )

        node_state.close()

        assert info_state.list_service_routes(
            service_name="startup-runtime-close-service",
            healthy_only=True,
            limit=10,
        ) == []
    finally:
        node_state.close()
        info_server.stop()


def test_node_registrar_syncs_active_runtimes(tmp_path):
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    node_state = NodeControlState(
        node_id="node-reg-runtime-01",
        queue_capacity=32,
        worker_capacity=1,
        artifact_dir=str(tmp_path / "code_cache_runtime"),
        enable_internal_executor=True,
        enable_service_session=False,
        monitor_interval_sec=1,
        executor_poll_interval_sec=0.02,
    )
    registrar = NodeInfoCenterRegistrar(
        infocenter_addr=info_target,
        node_id="node-reg-runtime-01",
        control_addr="127.0.0.1:50081",
        state=node_state,
        capacity=4,
        queue_capacity=32,
        tags=["compute", "task"],
        version="test-v1",
        fallback_heartbeat_sec=1,
    )

    try:
        blob = (
            b"import time\n"
            b"def run(sleep_ms=0, **_kwargs):\n"
            b"    sleep_ms = int(sleep_ms)\n"
            b"    if sleep_ms > 0:\n"
            b"        time.sleep(sleep_ms / 1000.0)\n"
            b"    return {'ok': True}\n"
        )
        digest = hashlib.sha256(blob).hexdigest()
        pool = node_state.create_task_pool(
            owner_client_id="owner-runtime-reg",
            pool_name="pool-runtime-reg",
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="runtime_registrar_demo",
            entry_callable="run",
            package_format="py",
            worker_count=1,
            heartbeat_timeout_sec=30,
            idle_ttl_sec=0,
            chunks=[blob],
        )
        accepted, rejected = node_state.submit_pool_tasks(
            pool_id=pool.pool_id,
            pool_token=pool.pool_token,
            job_id="job-runtime-reg",
            tasks=[
                pb2.TaskSubmitItem(
                    task_id="runtime-reg-task-1",
                    payload={"sleep_ms": 800},
                    priority=1,
                    runtime_key="runtime-hot-sync",
                )
            ],
        )
        assert [item.task_id for item in accepted] == ["runtime-reg-task-1"]
        assert rejected == []

        registrar.start()

        with InfoCenterClient(info_target, timeout_sec=5.0) as client:
            assert _wait_until(
                lambda: any(
                    node.node_id == "node-reg-runtime-01"
                    and "runtime-hot-sync" in node.active_runtimes
                    and node.python_version == node_state.python_version
                    for node in client.list_nodes(healthy_only=True, tags=["task"], limit=20)
                ),
                timeout_sec=6.0,
            )

            nodes = list(client.list_nodes(healthy_only=True, tags=["task"], limit=20))
            target = next(node for node in nodes if node.node_id == "node-reg-runtime-01")
            assert target.python_version == node_state.python_version
    finally:
        registrar.close()
        node_state.close()
        info_server.stop()
