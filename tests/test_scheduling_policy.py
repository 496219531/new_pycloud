from __future__ import annotations

from pycloud_parallel.controlplane.scheduling_policy import (
    is_call_route,
    is_conflict_scope,
    is_deploy_candidate,
    is_owner_target,
)
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


def test_scheduling_predicates_capture_unhealthy_drain_cordon_semantics():
    assert is_deploy_candidate(
        healthy=True,
        schedulable=True,
        drain=False,
        accept_service_deploy=True,
    )
    assert not is_deploy_candidate(
        healthy=False,
        schedulable=True,
        drain=False,
        accept_service_deploy=True,
    )
    assert not is_deploy_candidate(
        healthy=True,
        schedulable=False,
        drain=False,
        accept_service_deploy=True,
    )
    assert not is_deploy_candidate(
        healthy=True,
        schedulable=True,
        drain=True,
        accept_service_deploy=True,
    )

    assert is_call_route(healthy=True, service_status=pb2.SERVICE_STATUS_RUNNING, node_drain=False)
    assert not is_call_route(healthy=True, service_status=pb2.SERVICE_STATUS_RUNNING, node_drain=True)
    assert not is_call_route(healthy=False, service_status=pb2.SERVICE_STATUS_RUNNING, node_drain=False)

    assert is_owner_target(healthy=True, service_status=pb2.SERVICE_STATUS_RUNNING)
    assert is_owner_target(healthy=True, service_status=pb2.SERVICE_STATUS_DRAINING)
    assert not is_owner_target(healthy=False, service_status=pb2.SERVICE_STATUS_RUNNING)

    assert is_conflict_scope(healthy=True, service_status=pb2.SERVICE_STATUS_STARTING)
    assert is_conflict_scope(healthy=True, service_status=pb2.SERVICE_STATUS_DRAINING)
    assert not is_conflict_scope(healthy=False, service_status=pb2.SERVICE_STATUS_RUNNING)
