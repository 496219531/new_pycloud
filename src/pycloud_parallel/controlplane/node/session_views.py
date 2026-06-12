from __future__ import annotations

"""Shared session view and warmup helpers for NodeControl service/task-pool state."""

import logging
from datetime import timedelta
from typing import Dict, List, Sequence, Tuple

from pycloud_parallel.controlplane.infocenter.models import NodeTaskPoolInfo
from pycloud_parallel.controlplane.node.models import ServiceSession, TaskPoolState
from pycloud_parallel.controlplane.state_time import dt_to_ts, utc_now
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


def warmup_fanout(worker_count: int) -> int:
    return max(1, int(worker_count or 1))


def normalize_warmup_result(result: object, *, fanout: int) -> Tuple[int, List[int]]:
    if isinstance(result, tuple) and len(result) == 2:
        submitted_count, worker_pids = result
    elif isinstance(result, list):
        submitted_count, worker_pids = fanout, result
    else:
        submitted_count, worker_pids = result, []
    normalized_pids = [int(pid) for pid in (worker_pids or []) if int(pid or 0) > 0]
    return max(0, int(submitted_count or 0)), normalized_pids


def log_warmup_result(
    *,
    logger: logging.Logger,
    scope: str,
    key: str,
    worker_count: int,
    submitted_count: int,
    worker_pids: Sequence[int],
) -> None:
    unique_pids = sorted({int(pid) for pid in worker_pids if int(pid or 0) > 0})
    if int(submitted_count or 0) > 0 and not unique_pids:
        logger.debug(
            "[Warmup] scope=%s key=%s worker_count=%d submitted=%d completion=async pids=pending",
            scope,
            key,
            int(worker_count or 0),
            int(submitted_count or 0),
        )
        return
    logger.debug(
        "[Warmup] scope=%s key=%s worker_count=%d submitted=%d completed_workers=%d pids=%s",
        scope,
        key,
        int(worker_count or 0),
        int(submitted_count or 0),
        len(unique_pids),
        unique_pids,
    )


def execute_warmup(
    executor_host,
    *,
    scope: str,
    key: str,
    worker_count: int,
    execute_spec: Dict[str, object],
) -> Tuple[int, List[int]]:
    fanout = warmup_fanout(worker_count)
    normalized_scope = str(scope or "").strip().lower()
    if normalized_scope == "service":
        raw_result = executor_host.warmup_service(
            service_id=str(key or ""),
            fanout=fanout,
            execute_spec=execute_spec,
        )
    elif normalized_scope == "pool":
        raw_result = executor_host.warmup_pool(
            pool_id=str(key or ""),
            fanout=fanout,
            execute_spec=execute_spec,
        )
    elif normalized_scope == "runtime":
        raw_result = executor_host.warmup_runtime(
            runtime_key=str(key or ""),
            fanout=fanout,
            execute_spec=execute_spec,
        )
    else:
        raise ValueError(f"unsupported warmup scope: {scope!r}")
    return normalize_warmup_result(raw_result, fanout=fanout)


def _service_lease_expire_at(session: ServiceSession):
    if bool(getattr(session, "node_managed", False)) and session.is_running():
        return utc_now() + timedelta(seconds=max(5, int(session.heartbeat_timeout_sec or 5)))
    return session.lease_expire_at


def _service_status_name(status: int) -> str:
    try:
        return pb2.ServiceStatus.Name(int(status or pb2.SERVICE_STATUS_UNSPECIFIED))
    except Exception:
        return str(status or pb2.SERVICE_STATUS_UNSPECIFIED)


def _service_report_status_text(session: ServiceSession) -> str:
    if bool(getattr(session, "degraded", False)):
        return "DEGRADED"
    return _service_status_name(session.status)


def _task_pool_resource_health(pool: TaskPoolState, *, alive_workers: int) -> str:
    if not pool.is_running():
        return "stopped"
    if str(pool.stop_reason or "").strip():
        return "failed"
    if bool(getattr(pool, "degraded", False)) or int(alive_workers or 0) <= 0:
        return "degraded"
    return "running"


def build_service_status_info(session: ServiceSession, *, in_flight: int) -> Dict[str, object]:
    resource = session.resource_snapshot(in_flight=in_flight)
    lease_expire_at = _service_lease_expire_at(session)
    return {
        "service_id": session.service_id,
        "owner_client_id": session.owner_client_id,
        "service_name": session.service_name,
        "policy_id": str(session.policy_id or "").strip().lower() or "default_safe",
        "code_version": session.code_version,
        "status": int(session.status),
        "status_text": _service_report_status_text(session),
        "worker_count": resource.worker_count,
        "alive_workers": resource.alive_workers,
        "in_flight": resource.in_flight,
        "queued": session.queued,
        "received_count": resource.received_count,
        "returned_count": resource.returned_count,
        "created_at": session.created_at,
        "last_heartbeat_at": session.last_heartbeat_at,
        "lease_expire_at": lease_expire_at,
        "stop_reason": str(session.stop_reason or ""),
        "failure_at": getattr(session, "failure_at", None),
        "readiness": str(getattr(session, "readiness", "") or ""),
        "readiness_reason": str(getattr(session, "readiness_reason", "") or ""),
        "create_stage": str(getattr(session, "create_stage", "") or ""),
        "signal_cursor": int(getattr(session, "signal_cursor", 0) or 0),
        "http_base_url": session.http_base_url,
        "methods": sorted(session.methods.keys()),
        "timing_metrics": dict(session.timing_metrics or {}),
        "degraded": bool(getattr(session, "degraded", False)),
    }


def build_service_route_report(session: ServiceSession, *, in_flight: int) -> pb2.ServiceRouteReport:
    resource = session.resource_snapshot(in_flight=in_flight)
    lease_expire_at = _service_lease_expire_at(session)
    return pb2.ServiceRouteReport(
        service_name=session.service_name,
        service_id=session.service_id,
        status=session.status,
        worker_count=resource.worker_count,
        alive_workers=resource.alive_workers,
        in_flight=resource.in_flight,
        lease_expire_at=dt_to_ts(lease_expire_at),
        http_base_url=session.http_base_url,
        policy_id=str(session.policy_id or "").strip().lower() or "default_safe",
    )


def build_service_report_payload(session: ServiceSession, *, in_flight: int) -> Dict[str, object]:
    resource = session.resource_snapshot(in_flight=in_flight)
    metrics = dict(session.timing_metrics or {})
    lease_expire_at = _service_lease_expire_at(session)
    return {
        "service_name": session.service_name,
        "service_id": session.service_id,
        "policy_id": str(session.policy_id or "").strip().lower() or "default_safe",
        "owner_client_id": session.owner_client_id,
        "code_version": session.code_version,
        "entry_module": str(getattr(session, "entry_module", "") or ""),
        "entry_callable": str(getattr(session, "entry_callable", "") or ""),
        "serialization_mode": str(getattr(session, "serialization_mode", "") or ""),
        "status": int(session.status),
        "status_text": _service_report_status_text(session),
        "worker_count": int(resource.worker_count),
        "alive_workers": int(resource.alive_workers),
        "in_flight": int(resource.in_flight),
        "received_count": int(resource.received_count),
        "returned_count": int(resource.returned_count),
        "ema_child_invoke_ms": float(metrics.get("ema_child_invoke_ms", 0.0) or 0.0),
        "ema_samples": int(metrics.get("ema_samples", 0) or 0),
        "lease_expire_at": lease_expire_at.isoformat(),
        "failure_at": session.failure_at.isoformat() if getattr(session, "failure_at", None) is not None else "",
        "http_base_url": session.http_base_url,
        "stop_reason": str(session.stop_reason or ""),
        "degraded": bool(getattr(session, "degraded", False)),
        "readiness": str(getattr(session, "readiness", "") or ""),
        "readiness_reason": str(getattr(session, "readiness_reason", "") or ""),
        "create_stage": str(getattr(session, "create_stage", "") or ""),
        "signal_cursor": int(getattr(session, "signal_cursor", 0) or 0),
    }


def build_task_pool_info(pool: TaskPoolState, *, in_flight: int) -> NodeTaskPoolInfo:
    resource = pool.resource_snapshot(in_flight=in_flight)
    metrics = dict(pool.timing_metrics or {})
    status = "DEGRADED" if pool.is_running() and bool(getattr(pool, "degraded", False)) else str(pool.status)
    stop_reason = str(pool.stop_reason or "")
    return NodeTaskPoolInfo(
        pool_id=pool.pool_id,
        owner_client_id=pool.owner_client_id,
        pool_name=pool.pool_name,
        code_version=pool.code_version,
        status=status,
        resource_health=_task_pool_resource_health(pool, alive_workers=resource.alive_workers),
        degraded=bool(getattr(pool, "degraded", False)),
        worker_count=resource.worker_count,
        alive_workers=resource.alive_workers,
        task_count=pool.task_count,
        inflight=resource.in_flight,
        received_count=resource.received_count,
        returned_count=resource.returned_count,
        ema_child_invoke_ms=float(metrics.get("ema_child_invoke_ms", 0.0) or 0.0),
        ema_samples=int(metrics.get("ema_samples", 0) or 0),
        created_at=pool.created_at,
        last_heartbeat_at=pool.last_heartbeat_at,
        lease_expire_at=pool.lease_expire_at,
        stop_reason=stop_reason,
        failure_reason=stop_reason,
        failure_at=getattr(pool, "failure_at", None),
        readiness=str(getattr(pool, "readiness", "") or ""),
        readiness_reason=str(getattr(pool, "readiness_reason", "") or ""),
        create_stage=str(getattr(pool, "create_stage", "") or ""),
        signal_cursor=int(getattr(pool, "signal_cursor", 0) or 0),
    )


def build_task_pool_status_info(pool: TaskPoolState, *, in_flight: int) -> Dict[str, object]:
    resource = pool.resource_snapshot(in_flight=in_flight)
    status = "DEGRADED" if pool.is_running() and bool(getattr(pool, "degraded", False)) else str(pool.status)
    return {
        "pool_id": pool.pool_id,
        "owner_client_id": pool.owner_client_id,
        "pool_name": pool.pool_name,
        "code_version": pool.code_version,
        "task_method": pool.task_method,
        "worker_count": resource.worker_count,
        "alive_workers": resource.alive_workers,
        "heartbeat_timeout_sec": pool.heartbeat_timeout_sec,
        "status": status,
        "resource_health": _task_pool_resource_health(pool, alive_workers=resource.alive_workers),
        "degraded": bool(getattr(pool, "degraded", False)),
        "task_count": int(resource.received_count),
        "received_count": int(resource.received_count),
        "returned_count": int(resource.returned_count),
        "inflight": int(resource.in_flight),
        "created_at": pool.created_at,
        "last_heartbeat_at": pool.last_heartbeat_at,
        "lease_expire_at": pool.lease_expire_at,
        "timing_metrics": dict(pool.timing_metrics or {}),
        "stop_reason": str(pool.stop_reason or ""),
        "failure_reason": str(pool.stop_reason or ""),
        "failure_at": getattr(pool, "failure_at", None),
        "readiness": str(getattr(pool, "readiness", "") or ""),
        "readiness_reason": str(getattr(pool, "readiness_reason", "") or ""),
        "create_stage": str(getattr(pool, "create_stage", "") or ""),
        "signal_cursor": int(getattr(pool, "signal_cursor", 0) or 0),
    }


__all__ = [
    "build_service_report_payload",
    "build_service_route_report",
    "build_service_status_info",
    "build_task_pool_info",
    "build_task_pool_status_info",
    "execute_warmup",
    "log_warmup_result",
    "normalize_warmup_result",
    "warmup_fanout",
]
