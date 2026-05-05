from __future__ import annotations

"""Integration tests for multi-node V1 service deployment helpers."""

import time

import pytest
from typing import Tuple

from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient
from pycloud_parallel.artifact import Artifact
from pycloud_parallel.execution.service_session import Service
from pycloud_parallel.controlplane.infocenter_http import InfoCenterHttpServer
from pycloud_parallel.controlplane.infocenter_state import InfoCenterState
from pycloud_parallel.controlplane.node_control_http import NodeControlHttpServer
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


def _start_infocenter_server() -> Tuple[InfoCenterHttpServer, str, InfoCenterState]:
    state = InfoCenterState()
    server = InfoCenterHttpServer(bind="127.0.0.1:0", state=state)
    server.start()
    return server, server.base_url, state


def _start_nodecontrol_server(node_id: str, artifact_dir: str) -> Tuple[NodeControlHttpServer, str, NodeControlState]:
    state = NodeControlState(
        node_id=node_id,
        queue_capacity=32,
        worker_capacity=4,
        artifact_dir=artifact_dir,
        enable_internal_executor=False,
        enable_service_session=True,
        service_http_bind="127.0.0.1:0",
        monitor_interval_sec=1,
    )
    server = NodeControlHttpServer(bind="127.0.0.1:0", state=state)
    server.start()
    return server, server.base_url, state


def _sync_node_services(
    info_target: str,
    *,
    node_id: str,
    control_addr: str,
    tags: list[str],
    state: NodeControlState,
) -> None:
    with InfoCenterClient(info_target, timeout_sec=10.0) as infocenter:
        infocenter.register_node(
            node_id=node_id,
            control_addr=control_addr,
            capacity=16,
            queue_capacity=64,
            tags=tags,
            services=state.service_reports(),
        )


def test_multi_node_group_deploy_and_call(tmp_path):
    info_server, info_target, _info_state = _start_infocenter_server()
    n1_server, n1_target, n1_state = _start_nodecontrol_server("node-multi-01", str(tmp_path / "n1_code"))
    n2_server, n2_target, n2_state = _start_nodecontrol_server("node-multi-02", str(tmp_path / "n2_code"))

    try:
        with InfoCenterClient(info_target, timeout_sec=10.0) as infocenter:
            infocenter.register_node(node_id="node-multi-01", control_addr=n1_target, capacity=16, queue_capacity=64, tags=["test"])
            infocenter.register_node(node_id="node-multi-02", control_addr=n2_target, capacity=16, queue_capacity=64, tags=["test"])

        blob = (
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        )

        group = Service._deploy_from_infocenter(
            infocenter_target=info_target,
            owner_client_id="owner-multi-test",
            service_name="svc-multi-test",
            source=blob,
            runtime="py3",
            entry_module="svc_multi_test",
            entry_callable="run",
            worker_count=2,
            heartbeat_timeout_sec=30,
            healthy_only=True,
            tags=["test"],
            min_success_nodes=2,
            allow_partial=False,
            timeout_sec=10.0,
            session_cache_dir=str(tmp_path / "session_cache"),
        )

        try:
            assert len(group.sessions) == 2
            assert set(group.sessions.keys()) == {"node-multi-01", "node-multi-02"}
            assert all(session._hb_thread is not None and session._hb_thread.is_alive() for session in group.sessions.values())

            r1 = group.call_on_node("node-multi-01", "run", {"value": 3}, timeout_sec=8.0)
            r2 = group.call_on_node("node-multi-02", "run", {"value": 5}, timeout_sec=8.0)
            assert r1["ok"] is True and r1["data"]["square"] == 9
            assert r2["ok"] is True and r2["data"]["square"] == 25

            selected_node, r3 = group.call_balanced("run", {"value": 7}, timeout_sec=8.0, strategy="least_inflight")
            assert selected_node in group.sessions
            assert r3["ok"] is True and r3["data"]["square"] == 49

            statuses = group.status_map()
            assert set(statuses.keys()) == {"node-multi-01", "node-multi-02"}
            for info in statuses.values():
                assert info.status == pb2.SERVICE_STATUS_RUNNING

            ended = group.end("test complete")
            assert set(ended.keys()) == {"node-multi-01", "node-multi-02"}
            for resp in ended.values():
                assert resp is not None
                assert resp.ok is True
                assert resp.accepted is True
                assert resp.status == pb2.SERVICE_STATUS_STOPPED
        finally:
            group.close(end_services=False)
    finally:
        info_server.stop()
        n1_server.stop()
        n2_server.stop()
        n1_state.close()
        n2_state.close()


def test_service_deploy_connect_iter_items_accepts_generator_payload_stream(tmp_path):
    info_server, info_target, _info_state = _start_infocenter_server()
    n1_server, n1_target, n1_state = _start_nodecontrol_server("node-stream-01", str(tmp_path / "stream_n1_code"))

    try:
        with InfoCenterClient(info_target, timeout_sec=10.0) as infocenter:
            infocenter.register_node(node_id="node-stream-01", control_addr=n1_target, capacity=16, queue_capacity=64, tags=["stream"])

        blob = (
            b"from pycloud_parallel import export\n\n"
            b"@export\n"
            b"def square(x=0, **_kwargs):\n"
            b"    x = int(x)\n"
            b"    return {'x': x, 'square': x * x}\n"
        )

        group = Service.deploy(
            target=info_target,
            owner_client_id="owner-stream-test",
            service_name="svc-stream-test",
            artifact=Artifact.from_bytes(blob, package_format="py", entry_module="svc_stream_test"),
            runtime="py3",
            worker_count=1,
            heartbeat_timeout_sec=30,
            healthy_only=True,
            tags=["stream"],
            min_success_nodes=1,
            allow_partial=False,
            timeout_sec=10.0,
            session_cache_dir=str(tmp_path / "session_cache"),
        )
        try:
            _sync_node_services(
                info_target,
                node_id="node-stream-01",
                control_addr=n1_target,
                tags=["stream"],
                state=n1_state,
            )

            client = Service.connect(
                target=info_target,
                service_name="svc-stream-test",
                route="discovery",
                timeout_sec=10.0,
            )
            produced = []

            def payload_stream():
                for idx in range(5):
                    produced.append(idx)
                    yield {"x": idx}

            try:
                stream = client.square.iter_items(payload_stream(), max_in_flight=2, timeout_sec=8.0)
                results = sorted((item.index, item.result) for item in stream)
            finally:
                client.close()

            assert produced == [0, 1, 2, 3, 4]
            assert results == [
                (0, {"x": 0, "square": 0}),
                (1, {"x": 1, "square": 1}),
                (2, {"x": 2, "square": 4}),
                (3, {"x": 3, "square": 9}),
                (4, {"x": 4, "square": 16}),
            ]
        finally:
            group.close(end_services=True, reason="stream test done")
    finally:
        info_server.stop()
        n1_server.stop()
        n1_state.close()


def test_service_connect_streams_generator_results_incrementally(tmp_path, capsys):
    info_server, info_target, _info_state = _start_infocenter_server()
    n1_server, n1_target, n1_state = _start_nodecontrol_server("node-stream-out-01", str(tmp_path / "stream_out_n1_code"))

    try:
        with InfoCenterClient(info_target, timeout_sec=10.0) as infocenter:
            infocenter.register_node(node_id="node-stream-out-01", control_addr=n1_target, capacity=16, queue_capacity=64, tags=["stream"])

        blob = (
            b"from pycloud_parallel import export\n"
            b"import time\n\n"
            b"@export\n"
            b"def count(limit=3, delay_sec=0.05, **_kwargs):\n"
            b"    limit = int(limit)\n"
            b"    delay_sec = float(delay_sec)\n"
            b"    for idx in range(1, limit + 1):\n"
            b"        time.sleep(delay_sec)\n"
            b"        yield idx\n"
        )

        group = Service.deploy(
            target=info_target,
            owner_client_id="owner-stream-out-test",
            service_name="svc-stream-out-test",
            artifact=Artifact.from_bytes(blob, package_format="py", entry_module="svc_stream_out_test"),
            runtime="py3",
            worker_count=1,
            heartbeat_timeout_sec=30,
            healthy_only=True,
            tags=["stream"],
            min_success_nodes=1,
            allow_partial=False,
            timeout_sec=10.0,
            session_cache_dir=str(tmp_path / "session_cache"),
        )
        try:
            _sync_node_services(
                info_target,
                node_id="node-stream-out-01",
                control_addr=n1_target,
                tags=["stream"],
                state=n1_state,
            )
            client = Service.connect(
                target=info_target,
                service_name="svc-stream-out-test",
                route="discovery",
                timeout_sec=10.0,
            )
            try:
                received_at = []
                started = time.perf_counter()
                for value in client.count.stream(limit=3, delay_sec=0.05):
                    received_at.append(time.perf_counter() - started)
                    print(value)
            finally:
                client.close()

            assert capsys.readouterr().out.strip().splitlines() == ["1", "2", "3"]
            assert len(received_at) == 3
            assert received_at[0] >= 0.03
            assert received_at[1] - received_at[0] >= 0.03
            assert received_at[2] - received_at[1] >= 0.03
            assert received_at[2] - received_at[0] < 0.5
        finally:
            group.close(end_services=True, reason="stream output test done")
    finally:
        info_server.stop()
        n1_server.stop()
        n1_state.close()


def test_multi_node_group_circuit_breaker_recovery(tmp_path):
    info_server, info_target, _info_state = _start_infocenter_server()
    n1_server, n1_target, n1_state = _start_nodecontrol_server("node-cb-01", str(tmp_path / "cb_n1_code"))
    n2_server, n2_target, n2_state = _start_nodecontrol_server("node-cb-02", str(tmp_path / "cb_n2_code"))

    try:
        with InfoCenterClient(info_target, timeout_sec=10.0) as infocenter:
            infocenter.register_node(node_id="node-cb-01", control_addr=n1_target, capacity=16, queue_capacity=64, tags=["cb"])
            infocenter.register_node(node_id="node-cb-02", control_addr=n2_target, capacity=16, queue_capacity=64, tags=["cb"])

        blob = (
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        )

        group = Service._deploy_from_infocenter(
            infocenter_target=info_target,
            owner_client_id="owner-cb-test",
            service_name="svc-cb-test",
            source=blob,
            runtime="py3",
            entry_module="svc_cb_test",
            entry_callable="run",
            worker_count=2,
            heartbeat_timeout_sec=30,
            healthy_only=True,
            tags=["cb"],
            min_success_nodes=2,
            allow_partial=False,
            timeout_sec=10.0,
            breaker_enabled=True,
            breaker_failure_threshold=1,
            breaker_cooldown_sec=30.0,
            breaker_max_cooldown_sec=30.0,
            session_cache_dir=str(tmp_path / "session_cache"),
        )

        try:
            assert all(session._hb_thread is not None and session._hb_thread.is_alive() for session in group.sessions.values())
            session_n1 = group.sessions["node-cb-01"]
            origin_call = session_n1.call
            fault_once = {"count": 0}

            def flaky_call(method, payload, *, timeout_sec=60.0, token=None, **kwargs):
                if fault_once["count"] == 0:
                    fault_once["count"] += 1
                    raise RuntimeError("synthetic first failure on node-cb-01")
                return origin_call(method, payload, timeout_sec=timeout_sec, token=token, **kwargs)

            session_n1.call = flaky_call  # type: ignore[assignment]

            node_id_first, resp_first = group.call_balanced(
                "run",
                {"value": 4},
                timeout_sec=8.0,
                strategy="round_robin",
                refresh_status=False,
                max_attempts=2,
            )
            assert node_id_first == "node-cb-02"
            assert resp_first["ok"] is True

            snap1 = group.breaker_snapshot()
            assert snap1["node-cb-01"]["state"] == "open"
            assert snap1["node-cb-01"]["consecutive_failures"] >= 1

            node_id_second, resp_second = group.call_balanced(
                "run",
                {"value": 6},
                timeout_sec=8.0,
                strategy="round_robin",
                refresh_status=False,
                max_attempts=2,
            )
            assert node_id_second == "node-cb-02"
            assert resp_second["ok"] is True

            with group._route_lock:
                group._breaker_states["node-cb-01"].disabled_until_monotonic = time.monotonic() - 0.001

            node_id_third, resp_third = group.call_balanced(
                "run",
                {"value": 7},
                timeout_sec=8.0,
                strategy="round_robin",
                refresh_status=False,
                max_attempts=2,
            )
            assert resp_third["ok"] is True
            assert resp_third["data"]["square"] == 49

            recovered_node = node_id_third
            if recovered_node != "node-cb-01":
                node_id_fourth, resp_fourth = group.call_balanced(
                    "run",
                    {"value": 8},
                    timeout_sec=8.0,
                    strategy="round_robin",
                    refresh_status=False,
                    max_attempts=2,
                )
                assert resp_fourth["ok"] is True
                recovered_node = node_id_fourth

            assert recovered_node == "node-cb-01"

            snap2 = group.breaker_snapshot()
            assert snap2["node-cb-01"]["state"] == "closed"
            assert snap2["node-cb-01"]["consecutive_failures"] == 0
        finally:
            group.close(end_services=True, reason="cb test complete")
    finally:
        info_server.stop()
        n1_server.stop()
        n2_server.stop()
        n1_state.close()
        n2_state.close()


def test_service_group_user_error_does_not_failover(tmp_path):
    info_server, info_target, _info_state = _start_infocenter_server()
    n1_server, n1_target, n1_state = _start_nodecontrol_server("node-user-01", str(tmp_path / "user_n1_code"))
    n2_server, n2_target, n2_state = _start_nodecontrol_server("node-user-02", str(tmp_path / "user_n2_code"))

    try:
        with InfoCenterClient(info_target, timeout_sec=10.0) as infocenter:
            infocenter.register_node(node_id="node-user-01", control_addr=n1_target, capacity=16, queue_capacity=64, tags=["user"])
            infocenter.register_node(node_id="node-user-02", control_addr=n2_target, capacity=16, queue_capacity=64, tags=["user"])

        blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value)}\n"
        group = Service._deploy_from_infocenter(
            infocenter_target=info_target,
            owner_client_id="owner-user-test",
            service_name="svc-user-test",
            source=blob,
            runtime="py3",
            entry_module="svc_user_test",
            entry_callable="run",
            worker_count=1,
            heartbeat_timeout_sec=30,
            healthy_only=True,
            tags=["user"],
            min_success_nodes=2,
            allow_partial=False,
            timeout_sec=10.0,
            session_cache_dir=str(tmp_path / "session_cache"),
        )

        try:
            first = group.sessions["node-user-01"]

            def user_error(method, payload, *, timeout_sec=60.0, token=None, **_kwargs):
                raise RuntimeError("UserError: synthetic bad input")

            first.call = user_error  # type: ignore[assignment]

            with pytest.raises(RuntimeError, match="UserError"):
                group.call_balanced(
                    "run",
                    {"value": 4},
                    timeout_sec=8.0,
                    strategy="round_robin",
                    refresh_status=False,
                    max_attempts=2,
                )
        finally:
            group.close(end_services=True, reason="user error test done")
    finally:
        info_server.stop()
        n1_server.stop()
        n2_server.stop()
        n1_state.close()
        n2_state.close()


def test_service_group_infra_error_still_failsover(tmp_path):
    info_server, info_target, _info_state = _start_infocenter_server()
    n1_server, n1_target, n1_state = _start_nodecontrol_server("node-infra-01", str(tmp_path / "infra_n1_code"))
    n2_server, n2_target, n2_state = _start_nodecontrol_server("node-infra-02", str(tmp_path / "infra_n2_code"))

    try:
        with InfoCenterClient(info_target, timeout_sec=10.0) as infocenter:
            infocenter.register_node(node_id="node-infra-01", control_addr=n1_target, capacity=16, queue_capacity=64, tags=["infra"])
            infocenter.register_node(node_id="node-infra-02", control_addr=n2_target, capacity=16, queue_capacity=64, tags=["infra"])

        blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value), 'square': int(value) * int(value)}\n"
        group = Service._deploy_from_infocenter(
            infocenter_target=info_target,
            owner_client_id="owner-infra-test",
            service_name="svc-infra-test",
            source=blob,
            runtime="py3",
            entry_module="svc_infra_test",
            entry_callable="run",
            worker_count=1,
            heartbeat_timeout_sec=30,
            healthy_only=True,
            tags=["infra"],
            min_success_nodes=2,
            allow_partial=False,
            timeout_sec=10.0,
            session_cache_dir=str(tmp_path / "session_cache"),
        )

        try:
            first = group.sessions["node-infra-01"]
            original = first.call
            fault_once = {"count": 0}

            def infra_error(method, payload, *, timeout_sec=60.0, token=None):
                if fault_once["count"] == 0:
                    fault_once["count"] += 1
                    raise RuntimeError("InfraError: upstream unavailable")
                return original(method, payload, timeout_sec=timeout_sec, token=token)

            first.call = infra_error  # type: ignore[assignment]

            node_id, resp = group.call_balanced(
                "run",
                {"value": 5},
                timeout_sec=8.0,
                strategy="round_robin",
                refresh_status=False,
                max_attempts=2,
            )
            assert node_id == "node-infra-02"
            assert resp["ok"] is True
            assert resp["data"]["square"] == 25
        finally:
            group.close(end_services=True, reason="infra error test done")
    finally:
        info_server.stop()
        n1_server.stop()
        n2_server.stop()
        n1_state.close()
        n2_state.close()


def test_service_route_query_and_duplicate_guard(tmp_path):
    info_server, info_target, _info_state = _start_infocenter_server()
    n1_server, n1_target, n1_state = _start_nodecontrol_server("node-route-01", str(tmp_path / "route_n1_code"))
    try:
        with InfoCenterClient(info_target, timeout_sec=10.0) as infocenter:
            infocenter.register_node(node_id="node-route-01", control_addr=n1_target, capacity=16, queue_capacity=64, tags=["route"])

        blob = b"def run(**_kwargs):\n    return {'ok': True}\n"

        existing_group = Service._deploy_from_infocenter(
            infocenter_target=info_target,
            owner_client_id="owner-existing",
            service_name="svc-existing",
            source=blob,
            runtime="py3",
            entry_module="svc_existing",
            entry_callable="run",
            worker_count=2,
            heartbeat_timeout_sec=30,
            healthy_only=True,
            tags=["route"],
            min_success_nodes=1,
            allow_partial=False,
            timeout_sec=5.0,
            session_cache_dir=str(tmp_path / "session_cache"),
        )
        _sync_node_services(
            info_target,
            node_id="node-route-01",
            control_addr=n1_target,
            tags=["route"],
            state=n1_state,
        )

        try:
            with InfoCenterClient(info_target, timeout_sec=10.0) as infocenter:
                routes = infocenter.list_service_routes(service_name="svc-existing", healthy_only=True, limit=20)
                assert len(routes) == 1
                assert routes[0].service_name == "svc-existing"
                assert routes[0].node_id == "node-route-01"
                assert routes[0].status == pb2.SERVICE_STATUS_RUNNING

            Service._deploy_from_infocenter(
                infocenter_target=info_target,
                owner_client_id="owner-dup-check",
                service_name="svc-existing",
                source=blob,
                runtime="py3",
                entry_module="svc_dup_check",
                entry_callable="run",
                worker_count=2,
                heartbeat_timeout_sec=30,
                healthy_only=True,
                tags=["route"],
                min_success_nodes=1,
                allow_partial=True,
                timeout_sec=5.0,
                ensure_unique_service_name=True,
                session_cache_dir=str(tmp_path / "session_cache"),
            )
            assert False, "expected duplicate service name to be rejected"
        except RuntimeError as exc:
            assert "service_name already exists" in str(exc)
        finally:
            existing_group.close(end_services=True, reason="duplicate guard cleanup")
    finally:
        info_server.stop()
        n1_server.stop()
        n1_state.close()


def test_multi_node_group_reuses_existing_same_code(tmp_path):
    info_server, info_target, _info_state = _start_infocenter_server()
    n1_server, n1_target, n1_state = _start_nodecontrol_server("node-reuse-01", str(tmp_path / "reuse_n1_code"))
    n2_server, n2_target, n2_state = _start_nodecontrol_server("node-reuse-02", str(tmp_path / "reuse_n2_code"))
    cache_dir = tmp_path / "session_cache"

    try:
        with InfoCenterClient(info_target, timeout_sec=10.0) as infocenter:
            infocenter.register_node(node_id="node-reuse-01", control_addr=n1_target, capacity=16, queue_capacity=64, tags=["reuse"])
            infocenter.register_node(node_id="node-reuse-02", control_addr=n2_target, capacity=16, queue_capacity=64, tags=["reuse"])

        blob = (
            b"def run(value=0, **_kwargs):\n"
            b"    value = int(value)\n"
            b"    return {'value': value, 'square': value * value}\n"
        )

        group1 = Service._deploy_from_infocenter(
            infocenter_target=info_target,
            owner_client_id="owner-reuse-test",
            service_name="svc-reuse-test",
            source=blob,
            runtime="py3",
            entry_module="svc_reuse_test",
            entry_callable="run",
            worker_count=2,
            heartbeat_timeout_sec=30,
            healthy_only=True,
            tags=["reuse"],
            min_success_nodes=2,
            allow_partial=False,
            timeout_sec=10.0,
            session_cache_dir=str(cache_dir),
        )
        _sync_node_services(info_target, node_id="node-reuse-01", control_addr=n1_target, tags=["reuse"], state=n1_state)
        _sync_node_services(info_target, node_id="node-reuse-02", control_addr=n2_target, tags=["reuse"], state=n2_state)

        try:
            first_ids = {node_id: session.service_id for node_id, session in group1.sessions.items()}
            group1.close(end_services=False)

            group2 = Service._deploy_from_infocenter(
                infocenter_target=info_target,
                owner_client_id="owner-reuse-test",
                service_name="svc-reuse-test",
                source=blob,
                runtime="py3",
                entry_module="svc_reuse_test",
                entry_callable="run",
                worker_count=2,
                heartbeat_timeout_sec=30,
                healthy_only=True,
                tags=["reuse"],
                min_success_nodes=2,
                allow_partial=False,
                timeout_sec=10.0,
                session_cache_dir=str(cache_dir),
            )

            try:
                second_ids = {node_id: session.service_id for node_id, session in group2.sessions.items()}
                assert second_ids == first_ids

                node_id, resp = group2.call_balanced("run", {"value": 9}, timeout_sec=8.0, refresh_status=False)
                assert node_id in group2.sessions
                assert resp["ok"] is True
                assert resp["data"]["square"] == 81
            finally:
                group2.close(end_services=True, reason="reuse test done")
        finally:
            group1.close(end_services=False)

        assert not (cache_dir / "owner-reuse-test" / "svc-reuse-test.json").exists()
    finally:
        info_server.stop()
        n1_server.stop()
        n2_server.stop()
        n1_state.close()
        n2_state.close()


def test_multi_node_group_changed_code_requires_old_service_to_stop_first(tmp_path):
    info_server, info_target, _info_state = _start_infocenter_server()
    n1_server, n1_target, n1_state = _start_nodecontrol_server("node-replace-01", str(tmp_path / "replace_n1_code"))
    n2_server, n2_target, n2_state = _start_nodecontrol_server("node-replace-02", str(tmp_path / "replace_n2_code"))
    cache_dir = tmp_path / "session_cache"

    try:
        with InfoCenterClient(info_target, timeout_sec=10.0) as infocenter:
            infocenter.register_node(node_id="node-replace-01", control_addr=n1_target, capacity=16, queue_capacity=64, tags=["replace"])
            infocenter.register_node(node_id="node-replace-02", control_addr=n2_target, capacity=16, queue_capacity=64, tags=["replace"])

        blob_v1 = b"def run(**_kwargs):\n    return {'version': 1}\n"
        blob_v2 = b"def run(**_kwargs):\n    return {'version': 2}\n"

        group1 = Service._deploy_from_infocenter(
            infocenter_target=info_target,
            owner_client_id="owner-replace-test",
            service_name="svc-replace-test",
            source=blob_v1,
            runtime="py3",
            entry_module="svc_replace_test",
            entry_callable="run",
            worker_count=2,
            heartbeat_timeout_sec=30,
            healthy_only=True,
            tags=["replace"],
            min_success_nodes=2,
            allow_partial=False,
            timeout_sec=10.0,
            session_cache_dir=str(cache_dir),
        )
        _sync_node_services(info_target, node_id="node-replace-01", control_addr=n1_target, tags=["replace"], state=n1_state)
        _sync_node_services(info_target, node_id="node-replace-02", control_addr=n2_target, tags=["replace"], state=n2_state)

        try:
            Service._deploy_from_infocenter(
                infocenter_target=info_target,
                owner_client_id="owner-replace-test",
                service_name="svc-replace-test",
                source=blob_v2,
                runtime="py3",
                entry_module="svc_replace_test",
                entry_callable="run",
                worker_count=2,
                heartbeat_timeout_sec=30,
                healthy_only=True,
                tags=["replace"],
                min_success_nodes=2,
                allow_partial=False,
                timeout_sec=10.0,
                replace_existing_if_code_changed=False,
                session_cache_dir=str(cache_dir),
            )
            assert False, "expected explicit replace disable to reject changed code"
        except RuntimeError as exc:
            assert "different code_version" in str(exc)
            assert "still running" in str(exc)

        try:
            Service._deploy_from_infocenter(
                infocenter_target=info_target,
                owner_client_id="owner-replace-test",
                service_name="svc-replace-test",
                source=blob_v2,
                runtime="py3",
                entry_module="svc_replace_test",
                entry_callable="run",
                worker_count=2,
                heartbeat_timeout_sec=30,
                healthy_only=True,
                tags=["replace"],
                min_success_nodes=2,
                allow_partial=False,
                timeout_sec=10.0,
                session_cache_dir=str(cache_dir),
            )
            assert False, "expected running service with changed code to be rejected"
        except RuntimeError as exc:
            assert "another local deploy process is already active" in str(exc)

        try:
            first_ids = {node_id: session.service_id for node_id, session in group1.sessions.items()}
            group1.close(end_services=True, reason="replace old version first")
            _sync_node_services(info_target, node_id="node-replace-01", control_addr=n1_target, tags=["replace"], state=n1_state)
            _sync_node_services(info_target, node_id="node-replace-02", control_addr=n2_target, tags=["replace"], state=n2_state)

            group2 = Service._deploy_from_infocenter(
                infocenter_target=info_target,
                owner_client_id="owner-replace-test",
                service_name="svc-replace-test",
                source=blob_v2,
                runtime="py3",
                entry_module="svc_replace_test",
                entry_callable="run",
                worker_count=2,
                heartbeat_timeout_sec=30,
                healthy_only=True,
                tags=["replace"],
                min_success_nodes=2,
                allow_partial=False,
                timeout_sec=10.0,
                session_cache_dir=str(cache_dir),
            )
            try:
                second_ids = {node_id: session.service_id for node_id, session in group2.sessions.items()}
                assert second_ids != first_ids

                _, resp = group2.call_balanced("run", {}, timeout_sec=8.0, refresh_status=False)
                assert resp["ok"] is True
                assert resp["data"]["version"] == 2
            finally:
                group2.close(end_services=True, reason="replace test done")
        finally:
            group1.close(end_services=False)

        assert not (cache_dir / "owner-replace-test" / "svc-replace-test.json").exists()
    finally:
        info_server.stop()
        n1_server.stop()
        n2_server.stop()
        n1_state.close()
        n2_state.close()


def test_service_group_deploy_from_infocenter_filters_nodes_by_runtime(tmp_path):
    info_server, info_target, _info_state = _start_infocenter_server()
    n1_server, n1_target, n1_state = _start_nodecontrol_server("node-runtime-310", str(tmp_path / "runtime_n1_code"))
    n2_server, n2_target, n2_state = _start_nodecontrol_server("node-runtime-313", str(tmp_path / "runtime_n2_code"))

    try:
        with InfoCenterClient(info_target, timeout_sec=10.0) as infocenter:
            infocenter.register_node(
                node_id="node-runtime-310",
                control_addr=n1_target,
                capacity=16,
                queue_capacity=64,
                tags=["runtime"],
                python_version="py3.10",
            )
            infocenter.register_node(
                node_id="node-runtime-313",
                control_addr=n2_target,
                capacity=16,
                queue_capacity=64,
                tags=["runtime"],
                python_version="py3.13",
            )

        blob = b"def run(**_kwargs):\n    return {'ok': True}\n"

        group = Service._deploy_from_infocenter(
            infocenter_target=info_target,
            owner_client_id="owner-runtime-test",
            service_name="svc-runtime-test",
            source=blob,
            runtime=">=py3.11",
            entry_module="svc_runtime_test",
            entry_callable="run",
            worker_count=1,
            heartbeat_timeout_sec=30,
            healthy_only=True,
            tags=["runtime"],
            min_success_nodes=1,
            allow_partial=False,
            timeout_sec=10.0,
            session_cache_dir=str(tmp_path / "session_cache"),
        )

        try:
            assert set(group.sessions.keys()) == {"node-runtime-313"}
        finally:
            group.close(end_services=True, reason="runtime filter test done")
    finally:
        info_server.stop()
        n1_server.stop()
        n2_server.stop()
        n1_state.close()
        n2_state.close()
