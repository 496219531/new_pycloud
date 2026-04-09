from __future__ import annotations

"""Integration tests for NodeControl -> InfoCenter registrar sync."""

import hashlib
import time
from urllib.request import Request, urlopen

import pytest

from pycloud_parallel.controlplane.client import InfoCenterClient
from pycloud_parallel.controlplane.infocenter_http import InfoCenterHttpServer
from pycloud_parallel.controlplane.registrar import NodeInfoCenterRegistrar
from pycloud_parallel.controlplane.runtime_spec import matches_python_runtime, normalize_python_runtime_spec
from pycloud_parallel.controlplane.state import InfoCenterState, NodeControlState
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


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
            assert "<th>node_id</th><th>instance_id</th><th>control_addr</th><th>healthy</th><th>schedulable</th><th>drain</th><th>pycloud</th>" in raw
            assert "last_total_ms" in raw
            assert "avg_total_ms" in raw
            assert "last_build_execute_spec_ms" in raw
            assert "last_child_decode_ms" in raw
            assert "avg_child_invoke_ms" in raw

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


def test_node_registrar_syncs_active_runtimes(tmp_path):
    info_state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    info_server = InfoCenterHttpServer(bind="127.0.0.1:0", state=info_state)
    info_server.start()
    info_target = info_server.base_url

    node_state = NodeControlState(
        node_id="node-reg-runtime-01",
        queue_capacity=32,
        worker_capacity=1,
        runtime_slot_capacity=1,
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
        artifact, _ = node_state.put_code(
            sha256=f"sha256:{digest}",
            runtime="py3",
            entry_module="runtime_registrar_demo",
            entry_callable="run",
            package_format="py",
            chunks=[blob],
        )
        node_state.submit_tasks(
            pb2.SubmitTasksRequest(
                client_id="client-runtime-reg",
                code_version=artifact.code_version,
                execution_mode=pb2.EXECUTION_MODE_PERSISTENT,
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
        )

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
