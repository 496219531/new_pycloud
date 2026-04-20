from __future__ import annotations

from pycloud_parallel.controlplane.infocenter_client import InfoCenterNode
from pycloud_parallel.controlplane.node_capability import NodeCapability
from pycloud_parallel.execution.service_session import Service
from pycloud_parallel.execution.task_pool import TaskPool


def _node(node_id: str, capability: NodeCapability) -> InfoCenterNode:
    return InfoCenterNode(
        node_instance_id=f"{node_id}-inst",
        node_id=node_id,
        control_addr=f"{node_id}:50051",
        healthy=True,
        capacity=4,
        queue_capacity=16,
        queued=0,
        inflight=0,
        credit=16,
        capability=capability,
    )


def test_service_session_computes_frozen_effective_policy():
    capability = NodeCapability(
        supported_modes=("legacy_v1", "structured_v1"),
        supports_transport_payload_bytes=True,
        supports_http_bytes_transport=True,
        max_grpc_send_bytes=2 * 1024 * 1024,
        max_grpc_recv_bytes=2 * 1024 * 1024,
        max_http_body_bytes=2 * 1024 * 1024,
        max_upload_file_bytes=64 * 1024 * 1024,
        max_upload_total_bytes=128 * 1024 * 1024,
    )
    service = Service(
        owner_client_id="client-a",
        service_name="svc-a",
        sessions={},
        nodes={"node-a-inst": _node("node-a", capability)},
        policy_id="trusted_internal",
    )

    assert service.effective_policy is not None
    assert service.effective_policy.resolved_mode == "structured_v1"
    assert service.serialization_mode == "structured_v1"


def test_task_pool_session_computes_frozen_effective_policy():
    capability = NodeCapability(
        supported_modes=("legacy_v1",),
        supports_transport_payload_bytes=True,
        supports_http_bytes_transport=True,
        max_grpc_send_bytes=1024 * 1024,
        max_grpc_recv_bytes=1024 * 1024,
        max_http_body_bytes=2 * 1024 * 1024,
        max_upload_file_bytes=64 * 1024 * 1024,
        max_upload_total_bytes=128 * 1024 * 1024,
    )
    pool = TaskPool(
        pools={},
        nodes={"node-a-inst": _node("node-a", capability)},
        task_method="run",
        policy_id="trusted_internal",
    )

    assert pool.effective_policy.resolved_mode == "legacy_v1"
    assert pool.serialization_mode == "legacy_v1"
