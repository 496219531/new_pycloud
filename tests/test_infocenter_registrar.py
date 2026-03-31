from __future__ import annotations

"""Integration tests for NodeControl -> InfoCenter registrar sync."""

import hashlib
import time

from pycloud_parallel.controlplane.client import InfoCenterClient
from pycloud_parallel.controlplane.infocenter_http import InfoCenterHttpServer
from pycloud_parallel.controlplane.registrar import NodeInfoCenterRegistrar
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
                b"def run(payload):\n"
                b"    value = int(payload.get('value', 0))\n"
                b"    return {'value': value, 'square': value * value}\n"
            )
            digest = hashlib.sha256(blob).hexdigest()
            session = node_state.create_service(
                owner_client_id="owner-reg",
                service_name="svc-reg-sync",
                filename="svc_reg.py",
                sha256=f"sha256:{digest}",
                runtime="py3.11",
                entry_module="svc_reg",
                entry_callable="run",
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
