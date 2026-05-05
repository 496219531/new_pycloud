from __future__ import annotations

"""Shared scheduling predicates for node and service routing."""

from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


ACTIVE_SERVICE_STATUSES = {
    int(pb2.SERVICE_STATUS_STARTING),
    int(pb2.SERVICE_STATUS_RUNNING),
    int(pb2.SERVICE_STATUS_DRAINING),
}


def is_call_route(*, healthy: bool = True, service_status: int, node_drain: bool = False) -> bool:
    return bool(healthy) and int(service_status) == int(pb2.SERVICE_STATUS_RUNNING) and not bool(node_drain)


def is_owner_target(*, healthy: bool = True, service_status: int) -> bool:
    return bool(healthy) and int(service_status) in ACTIVE_SERVICE_STATUSES


def is_conflict_scope(*, healthy: bool = True, service_status: int) -> bool:
    return bool(healthy) and int(service_status) in ACTIVE_SERVICE_STATUSES


def deploy_candidate_block_reason(
    *,
    healthy: bool,
    schedulable: bool,
    drain: bool,
    accept_service_deploy: bool,
    control_addr: str = "",
    require_control_addr: bool = False,
    credit: int = 0,
    require_credit: bool = False,
) -> str:
    if not bool(healthy):
        return "unhealthy"
    if not bool(schedulable):
        return "cordon"
    if bool(drain):
        return "drain"
    if not bool(accept_service_deploy):
        return "accept_service_deploy=false"
    if require_control_addr and not str(control_addr or "").strip():
        return "missing_control_addr"
    if require_credit and int(credit or 0) <= 0:
        return "no_credit"
    return ""


def node_admission_block_reason(
    node: object,
    *,
    require_control_addr: bool = False,
    require_credit: bool = False,
) -> str:
    """Return why a node cannot accept new centrally scheduled work.

    This is the shared base admission predicate for task and service deploy
    candidates. Runtime compatibility, tags, and scoring stay at call sites.
    Node capability is observability/compat metadata and is not consulted here;
    this helper is not a resource scheduler or complex node manager.
    """

    return deploy_candidate_block_reason(
        healthy=bool(getattr(node, "healthy", True)),
        schedulable=bool(getattr(node, "schedulable", True)),
        drain=bool(getattr(node, "drain", False)),
        accept_service_deploy=bool(getattr(node, "accept_service_deploy", True)),
        control_addr=str(getattr(node, "control_addr", "") or ""),
        require_control_addr=bool(require_control_addr),
        credit=int(getattr(node, "credit", 0) or 0),
        require_credit=bool(require_credit),
    )


def is_admitted_node(
    node: object,
    *,
    require_control_addr: bool = False,
    require_credit: bool = False,
) -> bool:
    return not node_admission_block_reason(
        node,
        require_control_addr=require_control_addr,
        require_credit=require_credit,
    )


def is_deploy_candidate(**kwargs: object) -> bool:
    return not deploy_candidate_block_reason(**kwargs)


def call_routes(*, service_status: int, node_drain: bool) -> bool:
    return is_call_route(service_status=service_status, node_drain=node_drain)


def owner_targets(*, service_status: int) -> bool:
    return is_owner_target(service_status=service_status)


def conflict_scope(*, service_status: int) -> bool:
    return is_conflict_scope(service_status=service_status)


def deploy_candidates(**kwargs: object) -> bool:
    return is_deploy_candidate(**kwargs)


__all__ = [
    "is_call_route",
    "is_owner_target",
    "is_conflict_scope",
    "is_deploy_candidate",
    "is_admitted_node",
    "deploy_candidate_block_reason",
    "node_admission_block_reason",
    "call_routes",
    "owner_targets",
    "conflict_scope",
    "deploy_candidates",
]
