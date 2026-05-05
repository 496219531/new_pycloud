from __future__ import annotations

from pycloud_parallel.controlplane.infocenter.models import NodeServiceState
from pycloud_parallel.controlplane.infocenter_state import InfoCenterState
from pycloud_parallel.controlplane.node_capability import NodeCapability, detect_local_node_capability
from pycloud_parallel.controlplane.state_time import utc_now
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


def test_detect_local_node_capability_has_transport_support():
    capability = detect_local_node_capability()

    assert "legacy_v1" in capability.supported_modes
    assert "structured_v1" in capability.supported_modes
    assert capability.supports_raw_bytes_payload is True
    assert capability.supports_http_raw_bytes_body is True
    assert capability.max_control_send_bytes > 0
    assert capability.max_http_body_bytes > 0


def test_node_capability_roundtrips_http_control_fields():
    capability = NodeCapability(
        supports_http_control=True,
        control_base_url="http://127.0.0.1:18061",
    )

    restored = NodeCapability.from_dict(capability.to_dict())

    assert restored.supports_http_control is True
    assert restored.control_base_url == "http://127.0.0.1:18061"


def test_infocenter_roundtrips_node_capability():
    state = InfoCenterState(lease_ttl_sec=20, heartbeat_interval_sec=1)
    capability = NodeCapability(
        supported_modes=("legacy_v1", "structured_v1"),
        supports_raw_bytes_payload=True,
        supports_http_raw_bytes_body=False,
        supports_http_control=True,
        control_base_url="http://127.0.0.1:18061",
        max_control_send_bytes=8 * 1024 * 1024,
        max_control_recv_bytes=8 * 1024 * 1024,
        max_http_body_bytes=2 * 1024 * 1024,
        max_upload_file_bytes=32 * 1024 * 1024,
        max_upload_total_bytes=64 * 1024 * 1024,
    )
    state.register_node_record(
        node_id="node-cap-1",
        node_instance_id="node-cap-1-inst",
        control_addr="127.0.0.1:50061",
        capacity=4,
        queue_capacity=16,
        capability=capability,
        services={
            "svc-cap-1": NodeServiceState(
                service_name="svc-cap",
                service_id="svc-cap-1",
                status=int(pb2.SERVICE_STATUS_RUNNING),
                worker_count=1,
                alive_workers=1,
                in_flight=0,
                lease_expire_at=utc_now(),
                http_base_url="http://127.0.0.1:18080/svc/svc-cap-1",
            )
        },
    )

    node = state.list_nodes(healthy_only=True, tags=(), limit=10)[0]
    route = state.list_service_routes(service_name="svc-cap", healthy_only=True, limit=10)[0]

    assert node.capability == capability
    assert route["capability"] == capability.to_dict()
