from __future__ import annotations

"""HTTP + JSON server for InfoCenter control-plane and lightweight ops UI."""

import errno
import os
import html
import json
import threading
from datetime import datetime, timezone
from importlib import metadata as importlib_metadata
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse
from urllib.request import Request, urlopen

from pycloud_parallel.controlplane.data_ref import coerce_data_ref
from pycloud_parallel.controlplane.gateway_http import GatewayHttpApp
from pycloud_parallel.controlplane.job_queue import JobQueueManager
from pycloud_parallel.controlplane.netutil import resolve_public_host
from pycloud_parallel.controlplane.node_capability import NodeCapability
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.controlplane.serialization import serialize_arrow_compatible
from pycloud_parallel.controlplane.infocenter.state import (
    DataRegistryEntry,
    InfoCenterState,
    NodeMetricsState,
    NodeServiceState,
    NodeTaskPoolInfo,
)
from pycloud_parallel.controlplane.state_time import utc_now
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


MAX_BODY_BYTES = 64 * 1024 * 1024


def _is_client_disconnect_error(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
        return True
    if isinstance(exc, OSError):
        return getattr(exc, "errno", None) in (errno.EPIPE, errno.ECONNRESET, errno.ECONNABORTED)
    return False


def _split_host_port(bind: str) -> Tuple[str, int]:
    if ":" not in bind:
        raise ValueError("bind must be host:port")
    host, port = bind.rsplit(":", 1)
    return host.strip(), int(port)


def _dt_text(dt) -> str:
    try:
        return dt.isoformat()
    except Exception:
        return str(dt)


def _parse_dt(raw: object) -> datetime:
    if isinstance(raw, datetime):
        return raw.astimezone(timezone.utc) if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(raw), tz=timezone.utc)
        except Exception:
            return utc_now()
    if isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw)
            return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except Exception:
            return utc_now()
    return utc_now()


def _parse_services(payload: object) -> Dict[str, NodeServiceState]:
    out: Dict[str, NodeServiceState] = {}
    for item in payload or []:
        if not isinstance(item, dict):
            continue
        service_id = str(item.get("service_id", "")).strip()
        service_name = str(item.get("service_name", "")).strip()
        if not service_id or not service_name:
            continue
        out[service_id] = NodeServiceState(
            service_name=service_name,
            service_id=service_id,
            status=max(0, int(item.get("status", 0) or 0)),
            policy_id=str(item.get("policy_id", "") or "default_safe"),
            worker_count=max(0, int(item.get("worker_count", 0) or 0)),
            alive_workers=max(0, int(item.get("alive_workers", 0) or 0)),
            in_flight=max(0, int(item.get("in_flight", 0) or 0)),
            received_count=max(0, int(item.get("received_count", 0) or 0)),
            returned_count=max(0, int(item.get("returned_count", 0) or 0)),
            ema_child_invoke_ms=float(item.get("ema_child_invoke_ms", 0.0) or 0.0),
            ema_samples=max(0, int(item.get("ema_samples", 0) or 0)),
            lease_expire_at=_parse_dt(item.get("lease_expire_at") or item.get("lease_expire_at_ts") or utc_now()),
            http_base_url=str(item.get("http_base_url", "") or ""),
        )
    return out


def _parse_task_pools(payload: object) -> Dict[str, NodeTaskPoolInfo]:
    out: Dict[str, NodeTaskPoolInfo] = {}
    for item in payload or []:
        if not isinstance(item, dict):
            continue
        pool_id = str(item.get("pool_id", "")).strip()
        if not pool_id:
            continue
        out[pool_id] = NodeTaskPoolInfo(
            pool_id=pool_id,
            owner_client_id=str(item.get("owner_client_id", "") or ""),
            pool_name=str(item.get("pool_name", "") or ""),
            code_version=str(item.get("code_version", "") or ""),
            status=str(item.get("status", "") or ""),
            worker_count=max(0, int(item.get("worker_count", 0) or 0)),
            alive_workers=max(0, int(item.get("alive_workers", 0) or 0)),
            task_count=max(0, int(item.get("task_count", 0) or 0)),
            inflight=max(0, int(item.get("inflight", 0) or 0)),
            received_count=max(0, int(item.get("received_count", item.get("task_count", 0)) or 0)),
            returned_count=max(0, int(item.get("returned_count", 0) or 0)),
            ema_child_invoke_ms=float(item.get("ema_child_invoke_ms", 0.0) or 0.0),
            ema_samples=max(0, int(item.get("ema_samples", 0) or 0)),
            created_at=_parse_dt(item.get("created_at") or utc_now()),
            last_heartbeat_at=_parse_dt(item.get("last_heartbeat_at") or utc_now()),
            lease_expire_at=_parse_dt(item.get("lease_expire_at") or utc_now()),
        )
    return out


def _service_status_text(status: int) -> str:
    mapping = {
        int(pb2.SERVICE_STATUS_UNSPECIFIED): "UNSPECIFIED",
        int(pb2.SERVICE_STATUS_STARTING): "STARTING",
        int(pb2.SERVICE_STATUS_RUNNING): "RUNNING",
        int(pb2.SERVICE_STATUS_DRAINING): "DRAINING",
        int(pb2.SERVICE_STATUS_STOPPED): "STOPPED",
    }
    return mapping.get(int(status or 0), f"UNKNOWN({int(status or 0)})")


def _effective_service_status_text(*, node_healthy: bool, service_status: int) -> str:
    if not bool(node_healthy):
        return "LOST"
    return _service_status_text(service_status)


def _serialize_service(service: NodeServiceState, *, node_healthy: bool = True) -> Dict[str, object]:
    status_text = _effective_service_status_text(node_healthy=node_healthy, service_status=service.status)
    return {
        "service_name": str(service.service_name),
        "service_id": str(service.service_id),
        "status": int(service.status),
        "status_text": status_text,
        "policy_id": str(service.policy_id or "").strip().lower() or "default_safe",
        "node_healthy": bool(node_healthy),
        "worker_count": int(service.worker_count),
        "alive_workers": int(service.alive_workers if node_healthy else 0),
        "in_flight": int(service.in_flight if node_healthy else 0),
        "lease_expire_at": _dt_text(service.lease_expire_at),
        "http_base_url": str(service.http_base_url or ""),
    }


def _serialize_node(state) -> Dict[str, object]:
    services = [
        _serialize_service(svc, node_healthy=bool(state.healthy))
        for svc in sorted(state.services.values(), key=lambda item: (item.service_name, item.service_id))
    ]
    task_pools = sorted(state.task_pools.values(), key=lambda item: (item.created_at, item.pool_name, item.pool_id), reverse=True)
    active_task_pools = [pool for pool in task_pools if str(pool.status or "").strip().upper() == "RUNNING"]
    loaded_services = sorted({svc["service_name"] for svc in services})
    return {
        "node_instance_id": state.node_instance_id,
        "node_id": state.node_id,
        "control_addr": state.control_addr,
        "healthy": bool(state.healthy),
        "schedulable": bool(state.schedulable),
        "drain": bool(state.drain),
        "reason": str(state.reason or ""),
        "capability": state.capability.to_dict(),
        "capacity": int(state.capacity),
        "queue_capacity": int(state.queue_capacity),
        "queued": int(state.metrics.queued),
        "inflight": int(state.metrics.inflight),
        "running": int(state.metrics.running),
        "credit": int(state.metrics.credit),
        "python_version": str(state.python_version or ""),
        "active_runtimes": list(state.active_runtimes),
        "active_runtime_count": int(state.active_runtime_count()),
        "cpu_percent": float(state.metrics.cpu_percent),
        "mem_percent": float(state.metrics.mem_percent),
        "tags": list(state.tags),
        "version": str(state.version or ""),
        "metadata": dict(state.metadata),
        "last_seen_at": _dt_text(state.last_seen_at),
        "service_worker_capacity": int(state.service_worker_capacity),
        "service_worker_used": int(state.service_worker_used),
        "service_worker_available": int(state.service_worker_available()),
        "task_pool_worker_capacity": int(state.task_pool_worker_capacity),
        "task_pool_worker_used": int(state.task_pool_worker_used),
        "task_pool_worker_available": int(state.task_pool_worker_available()),
        "task_pool_count": int(len(active_task_pools)),
        "task_pool_total_count": int(len(task_pools)),
        "service_count": int(len(services)),
        "loaded_services": loaded_services,
        "services": services,
        "task_pools": [
            {
                "pool_id": str(pool.pool_id),
                "owner_client_id": str(pool.owner_client_id),
                "pool_name": str(pool.pool_name),
                "code_version": str(pool.code_version),
                "status": str(pool.status),
                "worker_count": int(pool.worker_count),
                "alive_workers": int(pool.alive_workers),
                "task_count": int(pool.task_count),
                "inflight": int(pool.inflight),
                "received_count": int(pool.received_count),
                "returned_count": int(pool.returned_count),
                "ema_child_invoke_ms": float(pool.ema_child_invoke_ms),
                "ema_samples": int(pool.ema_samples),
                "created_at": _dt_text(pool.created_at),
                "last_heartbeat_at": _dt_text(pool.last_heartbeat_at),
                "lease_expire_at": _dt_text(pool.lease_expire_at),
            }
            for pool in task_pools
        ],
    }


def _parse_node_capability(payload: object) -> NodeCapability:
    return NodeCapability.from_dict(payload if isinstance(payload, dict) else None)


def _serialize_data_registry_entry(entry: DataRegistryEntry) -> Dict[str, object]:
    return {
        "ref_id": str(entry.ref_id or ""),
        "storage_id": str(entry.storage_id or ""),
        "logical_type": str(entry.logical_type or ""),
        "format": str(entry.format or ""),
        "size_bytes": int(entry.size_bytes or 0),
        "materialize_as": str(entry.materialize_as or ""),
        "locator_kind": str(entry.locator_kind or ""),
        "locator_token": str(entry.locator_token or ""),
        "consume_on_read": bool(entry.consume_on_read),
        "node_id": str(entry.node_id or ""),
        "node_instance_id": str(entry.node_instance_id or ""),
        "control_addr": str(entry.control_addr or ""),
        "replicas": [
            {
                "node_id": str(item.get("node_id", "") or ""),
                "node_instance_id": str(item.get("node_instance_id", "") or ""),
                "control_addr": str(item.get("control_addr", "") or ""),
            }
            for item in (entry.replicas or ())
        ],
        "created_at": _dt_text(entry.created_at),
        "last_at": _dt_text(entry.last_at),
        "ttl_sec": int(entry.ttl_sec or 0),
    }


def _parse_service_timing_metrics(node_metadata: Dict[str, object]) -> Dict[str, Dict[str, object]]:
    raw = str((node_metadata or {}).get("service_timing_metrics", "") or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    out: Dict[str, Dict[str, object]] = {}
    for service_id, item in payload.items():
        if isinstance(item, dict):
            out[str(service_id)] = dict(item)
    return out


def _parse_task_pool_timing_metrics(node_metadata: Dict[str, object]) -> Dict[str, Dict[str, object]]:
    raw = str((node_metadata or {}).get("task_pool_timing_metrics", "") or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    out: Dict[str, Dict[str, object]] = {}
    for pool_id, item in payload.items():
        if isinstance(item, dict):
            out[str(pool_id)] = dict(item)
    return out


def _parse_job_recent_entries(node_metadata: Dict[str, object]) -> List[Dict[str, object]]:
    raw = str((node_metadata or {}).get("job_recent", "") or "").strip()
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    out: List[Dict[str, object]] = []
    for item in payload:
        if isinstance(item, dict):
            out.append(dict(item))
    return out


def _parse_job_waiting_entries(node_metadata: Dict[str, object]) -> List[Dict[str, object]]:
    raw = str((node_metadata or {}).get("job_waiting_list", "") or "").strip()
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    out: List[Dict[str, object]] = []
    for item in payload:
        if isinstance(item, dict):
            out.append(dict(item))
    return out


def _pycloud_version() -> str:
    try:
        import pycloud_parallel

        runtime_version = str(getattr(pycloud_parallel, "__version__", "") or "").strip()
        if runtime_version:
            return runtime_version
    except Exception:
        pass
    try:
        return str(importlib_metadata.version("pycloud-parallel"))
    except Exception:
        return "unknown"


def _job_detail_link(http_base_url: str, job_id: object) -> str:
    base = str(http_base_url or "").strip().rstrip("/")
    normalized_job_id = str(job_id or "").strip()
    if not base or not normalized_job_id:
        return html.escape(normalized_job_id or "-")
    return (
        f"<a href='{html.escape(base)}/jobs/{html.escape(normalized_job_id)}?view=html' target='_blank'>"
        f"{html.escape(normalized_job_id)}</a>"
    )


def _job_queue_service_http_base(state: InfoCenterState, owner: str) -> str:
    normalized = str(owner or "").strip()
    if normalized == "embedded":
        return ""
    for node in state.list_nodes(healthy_only=False, tags=(), limit=10000):
        if str(getattr(node, "node_instance_id", "") or "") != normalized:
            continue
        metadata = dict(getattr(node, "metadata", {}) or {})
        if str(metadata.get("component", "") or "").strip() != "job-orchestrator":
            continue
        for svc in getattr(node, "services", {}).values():
            if str(getattr(svc, "service_name", "") or "") == "job-orchestrator" and str(getattr(svc, "http_base_url", "") or "").strip():
                return str(getattr(svc, "http_base_url", "") or "").strip().rstrip("/")
    return ""


def _reorder_job_via_http(http_base_url: str, job_id: str, *, direction: str) -> Dict[str, object]:
    base = str(http_base_url or "").strip().rstrip("/")
    if not base:
        raise RuntimeError("job orchestrator route has no http_base_url")
    raw = json.dumps({"job_id": str(job_id or "").strip(), "direction": str(direction or "").strip()}).encode("utf-8")
    req = Request(
        f"{base}/call/reorder_job?timeout_sec=5.000",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=raw,
    )
    with urlopen(req, timeout=6.0) as resp:
        payload = json.loads(resp.read().decode("utf-8") or "{}")
    if not isinstance(payload, dict) or not payload.get("ok", False):
        raise RuntimeError(str((payload or {}).get("error", "reorder failed")))
    return payload


def _render_ops_page(state: InfoCenterState, job_queue: Optional[JobQueueManager] = None) -> str:
    nodes = state.list_nodes(healthy_only=False, tags=(), limit=10000)
    node_rows: List[str] = []
    service_rows: List[str] = []
    pool_entries: List[tuple] = []
    job_queue_rows: List[str] = []
    recent_job_rows: List[tuple] = []
    waiting_job_rows: List[tuple] = []
    if job_queue is not None:
        summary = dict(job_queue.summary() or {})
        job_queue_rows.append(
            "<tr>"
            "<td>embedded</td>"
            "<td>-</td>"
            "<td>yes</td>"
            "<td>-</td>"
            f"<td>{html.escape(str(summary.get('current_job_id', '') or '-'))}</td>"
            f"<td>{html.escape(str(summary.get('current_job_status', '') or '-'))}</td>"
            f"<td>{int(summary.get('waiting', 0) or 0)}</td>"
            f"<td>{int(summary.get('running', 0) or 0)}</td>"
            f"<td>{int(summary.get('terminal', 0) or 0)}</td>"
            f"<td>{int(summary.get('job_count', 0) or 0)}</td>"
            "<td>-</td>"
            "</tr>"
        )
        for item in summary.get("recent_jobs") or []:
            if not isinstance(item, dict):
                continue
            recent_job_rows.append(
                (
                    str(item.get("submitted_at", "") or ""),
                    "<tr>"
                    "<td>embedded</td>"
                    "<td>-</td>"
                    f"<td>{html.escape(str(item.get('job_id', '') or '-'))}</td>"
                    f"<td>{html.escape(str(item.get('status', '') or '-'))}</td>"
                    f"<td>{html.escape(str(item.get('submitted_at', '') or '-'))}</td>"
                    f"<td>{html.escape(str(item.get('finished_at', '') or '-'))}</td>"
                    f"<td>{html.escape(str(item.get('final_result_preview', '') or '-'))}</td>"
                    f"<td>{html.escape(str(item.get('error_preview', '') or '-'))}</td>"
                    "</tr>"
                )
            )
        waiting_jobs = list(summary.get("waiting_jobs") or [])
        for idx, item in enumerate(waiting_jobs):
            if not isinstance(item, dict):
                continue
            job_id = str(item.get("job_id", "") or "")
            waiting_job_rows.append(
                (
                    int(item.get("position", idx + 1) or (idx + 1)),
                    "<tr>"
                    "<td>embedded</td>"
                    "<td>-</td>"
                    f"<td>{html.escape(job_id or '-')}</td>"
                    f"<td>{html.escape(str(item.get('priority', 0) or 0))}</td>"
                    f"<td>{html.escape(str(item.get('submitted_at', '') or '-'))}</td>"
                    f"<td>{html.escape(str(item.get('position', idx + 1) or (idx + 1)))}</td>"
                    "<td>"
                    f"<form method='post' action='/ops/job-queues/embedded/jobs/{html.escape(job_id)}/move-up' style='display:inline'><button type='submit'>up</button></form> "
                    f"<form method='post' action='/ops/job-queues/embedded/jobs/{html.escape(job_id)}/move-down' style='display:inline'><button type='submit'>down</button></form>"
                    "</td>"
                    "</tr>"
                )
            )
    for node in nodes:
        services = sorted(node.services.values(), key=lambda item: (item.service_name, item.service_id))
        task_pools = sorted(node.task_pools.values(), key=lambda item: (item.created_at, item.pool_name, item.pool_id), reverse=True)
        node_healthy = bool(node.healthy)
        timing_map = _parse_service_timing_metrics(dict(node.metadata))
        pool_timing_map = _parse_task_pool_timing_metrics(dict(node.metadata))
        loaded = "<br>".join(
            f"{html.escape(svc.service_name)} "
            f"<span class='muted'>[{(svc.alive_workers if node_healthy else 0)}/{svc.worker_count} alive, "
            f"in-flight {(svc.in_flight if node_healthy else 0)}]</span>"
            for svc in services
        ) or "-"
        active_runtimes = ", ".join(node.active_runtimes[:10]) or "-"
        node_rows.append(
            "<tr>"
            f"<td>{html.escape(node.node_id)}</td>"
            f"<td>{html.escape(getattr(node, 'node_instance_id', '-') or '-')}</td>"
            f"<td>{html.escape(node.control_addr)}</td>"
            f"<td>{'yes' if node.healthy else 'no'}</td>"
            f"<td>{'yes' if node.schedulable else 'no'}</td>"
            f"<td>{'yes' if node.drain else 'no'}</td>"
            f"<td>{html.escape(str((node.metadata or {}).get('pycloud_version', '-') or '-'))}</td>"
            f"<td>{html.escape(node.python_version or '-')}</td>"
            f"<td>{html.escape(active_runtimes)}</td>"
            f"<td>{node.service_worker_capacity}</td>"
            f"<td>{node.service_worker_used}</td>"
            f"<td>{node.service_worker_available()}</td>"
            f"<td>{node.task_pool_worker_capacity}</td>"
            f"<td>{node.task_pool_worker_used}</td>"
            f"<td>{node.task_pool_worker_available()}</td>"
            f"<td>{sum(int(getattr(pool, 'inflight', 0) or 0) for pool in task_pools)}</td>"
            f"<td>{len(task_pools)}</td>"
            f"<td>{len(services)}</td>"
            f"<td>{loaded}</td>"
            f"<td>{html.escape(node.reason or '')}</td>"
            "<td>"
            f"<form method='post' action='/ops/nodes/{html.escape(getattr(node, 'node_instance_id', node.node_id))}/cordon' style='display:inline'><button type='submit'>cordon</button></form> "
            f"<form method='post' action='/ops/nodes/{html.escape(getattr(node, 'node_instance_id', node.node_id))}/uncordon' style='display:inline'><button type='submit'>uncordon</button></form> "
            f"<form method='post' action='/ops/nodes/{html.escape(getattr(node, 'node_instance_id', node.node_id))}/drain' style='display:inline'><button type='submit'>drain</button></form> "
            f"<form method='post' action='/ops/nodes/{html.escape(getattr(node, 'node_instance_id', node.node_id))}/undrain' style='display:inline'><button type='submit'>undrain</button></form>"
            "</td>"
            "</tr>"
        )
        for svc in services:
            stale_row = "" if node_healthy else " class='stale-row'"
            timing = timing_map.get(str(svc.service_id), {})
            service_rows.append(
                f"<tr{stale_row}>"
                f"<td>{html.escape(node.node_id)}</td>"
                f"<td>{html.escape(getattr(node, 'node_instance_id', '-') or '-')}</td>"
                f"<td>{html.escape(svc.service_name)}</td>"
                f"<td>{html.escape(svc.service_id)}</td>"
                f"<td>{'yes' if node_healthy else 'no'}</td>"
                f"<td>{html.escape(_effective_service_status_text(node_healthy=node_healthy, service_status=svc.status))}</td>"
                f"<td>{svc.worker_count}</td>"
                f"<td>{svc.alive_workers if node_healthy else 0}</td>"
                f"<td>{svc.in_flight if node_healthy else 0}</td>"
                f"<td>{html.escape(str(timing.get('call_count', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('error_count', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_total_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_child_decode_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_child_invoke_ms', timing.get('avg_invoke_ms', '-'))))}</td>"
                f"<td>{html.escape(str(timing.get('avg_child_encode_ms', '-')))}</td>"
                f"<td>{html.escape(_dt_text(svc.lease_expire_at))}</td>"
                f"<td>{html.escape(svc.http_base_url or '-')}</td>"
                "</tr>"
            )
        for pool in task_pools:
            stale_row = "" if node_healthy else " class='stale-row'"
            timing = pool_timing_map.get(str(pool.pool_id), {})
            pool_entries.append((
                getattr(pool, "created_at", None),
                f"<tr{stale_row}>"
                f"<td>{html.escape(node.node_id)}</td>"
                f"<td>{html.escape(getattr(node, 'node_instance_id', '-') or '-')}</td>"
                f"<td>{html.escape(pool.pool_name)}</td>"
                f"<td>{html.escape(pool.pool_id)}</td>"
                f"<td>{html.escape(pool.owner_client_id)}</td>"
                f"<td>{html.escape(pool.status)}</td>"
                f"<td>{pool.worker_count}</td>"
                f"<td>{pool.task_count}</td>"
                f"<td>{int(getattr(pool, 'inflight', 0) or 0)}</td>"
                f"<td>{html.escape(str(timing.get('call_count', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('error_count', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_total_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_child_decode_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_child_invoke_ms', timing.get('avg_invoke_ms', '-'))))}</td>"
                f"<td>{html.escape(str(timing.get('avg_child_encode_ms', '-')))}</td>"
                f"<td>{html.escape(pool.code_version[:20] + ('...' if len(pool.code_version) > 20 else ''))}</td>"
                f"<td>{html.escape(_dt_text(pool.created_at))}</td>"
                f"<td>{html.escape(_dt_text(pool.last_heartbeat_at))}</td>"
                f"<td>{html.escape(_dt_text(pool.lease_expire_at))}</td>"
                "</tr>"
            ))
        metadata = dict(node.metadata or {})
        component = str(metadata.get("component", "") or "").strip()
        if component == "job-orchestrator":
            job_service = next((svc for svc in services if svc.service_name == "job-orchestrator"), None)
            job_waiting = str(metadata.get("job_waiting", "0") or "0")
            job_running = str(metadata.get("job_running", "0") or "0")
            job_terminal = str(metadata.get("job_terminal", "0") or "0")
            job_queue_rows.append(
                "<tr>"
                f"<td>{html.escape(node.node_id)}</td>"
                f"<td>{html.escape(getattr(node, 'node_instance_id', '-') or '-')}</td>"
                f"<td>{'yes' if node_healthy else 'no'}</td>"
                f"<td>{html.escape(str(metadata.get('pycloud_version', '-') or '-'))}</td>"
                f"<td>{html.escape(str(metadata.get('current_job_id', '') or '-'))}</td>"
                f"<td>{html.escape(str(metadata.get('current_job_status', '') or '-'))}</td>"
                f"<td>{html.escape(job_waiting)}</td>"
                f"<td>{html.escape(job_running)}</td>"
                f"<td>{html.escape(job_terminal)}</td>"
                f"<td>{html.escape(str(sum(int(value or 0) for value in (job_waiting, job_running, job_terminal))))}</td>"
                f"<td>{html.escape((job_service.http_base_url if job_service is not None else '') or '-')}</td>"
                "</tr>"
            )
            for item in _parse_job_recent_entries(metadata):
                job_http_base = job_service.http_base_url if job_service is not None else ""
                recent_job_rows.append(
                    (
                        str(item.get("submitted_at", "") or ""),
                        "<tr>"
                        f"<td>{html.escape(node.node_id)}</td>"
                        f"<td>{html.escape(getattr(node, 'node_instance_id', '-') or '-')}</td>"
                        f"<td>{_job_detail_link(job_http_base, item.get('job_id', ''))}</td>"
                        f"<td>{html.escape(str(item.get('status', '') or '-'))}</td>"
                        f"<td>{html.escape(str(item.get('submitted_at', '') or '-'))}</td>"
                        f"<td>{html.escape(str(item.get('finished_at', '') or '-'))}</td>"
                        f"<td>{html.escape(str(item.get('final_result_preview', '') or '-'))}</td>"
                        f"<td>{html.escape(str(item.get('error_preview', '') or '-'))}</td>"
                        "</tr>"
                    )
                )
            for idx, item in enumerate(_parse_job_waiting_entries(metadata)):
                if not isinstance(item, dict):
                    continue
                job_id = str(item.get("job_id", "") or "")
                waiting_job_rows.append(
                    (
                        int(item.get("position", idx + 1) or (idx + 1)),
                        "<tr>"
                        f"<td>{html.escape(node.node_id)}</td>"
                        f"<td>{html.escape(getattr(node, 'node_instance_id', '-') or '-')}</td>"
                        f"<td>{_job_detail_link(job_service.http_base_url if job_service is not None else '', job_id)}</td>"
                        f"<td>{html.escape(str(item.get('priority', 0) or 0))}</td>"
                        f"<td>{html.escape(str(item.get('submitted_at', '') or '-'))}</td>"
                        f"<td>{html.escape(str(item.get('position', idx + 1) or (idx + 1)))}</td>"
                        "<td>"
                        f"<form method='post' action='/ops/job-queues/{html.escape(getattr(node, 'node_instance_id', '-') or '-')}/jobs/{html.escape(job_id)}/move-up' style='display:inline'><button type='submit'>up</button></form> "
                        f"<form method='post' action='/ops/job-queues/{html.escape(getattr(node, 'node_instance_id', '-') or '-')}/jobs/{html.escape(job_id)}/move-down' style='display:inline'><button type='submit'>down</button></form>"
                        "</td>"
                        "</tr>"
                    )
                )
    node_body = "\n".join(node_rows) or "<tr><td colspan='21'>no nodes</td></tr>"
    service_body = "\n".join(service_rows) or "<tr><td colspan='17'>no services</td></tr>"
    pool_entries.sort(key=lambda item: item[0], reverse=True)
    pool_rows = [row for _created_at, row in pool_entries]
    pool_body = "\n".join(pool_rows) or "<tr><td colspan='18'>no task pools</td></tr>"
    job_queue_body = "\n".join(job_queue_rows) or "<tr><td colspan='11'>no job queues</td></tr>"
    recent_job_rows.sort(key=lambda item: item[0], reverse=True)
    recent_job_body = "\n".join(row for _sort_key, row in recent_job_rows) or "<tr><td colspan='8'>no recent jobs</td></tr>"
    waiting_job_rows.sort(key=lambda item: item[0])
    waiting_job_body = "\n".join(row for _sort_key, row in waiting_job_rows) or "<tr><td colspan='7'>no waiting jobs</td></tr>"
    return (
        "<!doctype html><html><head><meta charset='utf-8'><meta http-equiv='refresh' content='10'><title>InfoCenter Ops</title>"
        "<style>body{font-family:Menlo,monospace;margin:20px;}table{border-collapse:collapse;width:100%;}"
        "th,td{border:1px solid #ccc;padding:6px 8px;font-size:13px;vertical-align:top;word-break:break-word;overflow-wrap:anywhere;white-space:normal;}"
        "th{background:#f5f5f5;text-align:left;}"
        "h2{margin-top:28px;} .muted{color:#666;} .section-note{color:#555;font-size:12px;margin:6px 0 10px;}"
        ".stale-row{background:#fff1f0;color:#8a1f11;}</style>"
        "</head><body>"
        f"<h1>InfoCenter Ops</h1><div class='section-note'>controlplane_version={html.escape(_pycloud_version())}</div>"
        "<div class='section-note'>auto_refresh_sec=10</div>"
        "<div class='section-note'>Node table shows task-mode pressure plus service/task-pool capacity. "
        "Service table below shows each deployed service instance, worker process counts, and reduced timing metrics. "
        "Task pool table shows native temporary pools running on each node. "
        "Timing columns keep only average total latency plus child decode/invoke/encode averages. "
        "Rows for stale nodes are highlighted and rendered as LOST.</div>"
        "<table><thead><tr>"
        "<th>node_id</th><th>instance_id</th><th>control_addr</th><th>healthy</th><th>schedulable</th><th>drain</th><th>pycloud</th>"
        "<th>python</th><th>active runtimes</th><th>svc cap</th><th>svc used</th><th>svc avail</th><th>pool cap</th><th>pool used</th><th>pool avail</th><th>pool inflight</th><th>pool count</th><th>svc count</th><th>services</th><th>reason</th><th>actions</th>"
        "</tr></thead><tbody>"
        f"{node_body}"
        "</tbody></table>"
        "<h2>Job Queue</h2>"
        "<div class='section-note'>Shows embedded controlplane job queue state and any standalone `job-orchestrator` processes registered via InfoCenter metadata.</div>"
        "<table><thead><tr>"
        "<th>owner</th><th>instance_id</th><th>healthy</th><th>pycloud</th><th>current_job_id</th><th>current_status</th><th>waiting</th><th>running</th><th>terminal</th><th>job_count</th><th>http_base_url</th>"
        "</tr></thead><tbody>"
        f"{job_queue_body}"
        "</tbody></table>"
        "<h2>Recent Jobs</h2>"
        "<table><thead><tr>"
        "<th>owner</th><th>instance_id</th><th>job_id</th><th>status</th><th>submitted_at</th><th>finished_at</th><th>final_result</th><th>error</th>"
        "</tr></thead><tbody>"
        f"{recent_job_body}"
        "</tbody></table>"
        "<h2>Waiting Jobs</h2>"
        "<div class='section-note'>Only waiting jobs can be reordered. Running jobs keep their current slot.</div>"
        "<table><thead><tr>"
        "<th>owner</th><th>instance_id</th><th>job_id</th><th>priority</th><th>submitted_at</th><th>position</th><th>actions</th>"
        "</tr></thead><tbody>"
        f"{waiting_job_body}"
        "</tbody></table>"
        "<h2>Service Instances</h2>"
        "<table><thead><tr>"
        "<th>node_id</th><th>instance_id</th><th>service_name</th><th>service_id</th><th>node_healthy</th><th>status</th><th>workers</th><th>alive</th><th>in_flight</th><th>calls</th><th>errors</th><th>avg_total_ms</th><th>avg_child_decode_ms</th><th>avg_child_invoke_ms</th><th>avg_child_encode_ms</th><th>lease_expire_at</th><th>http_base_url</th>"
        "</tr></thead><tbody>"
        f"{service_body}"
        "</tbody></table>"
        "<h2>Task Pools</h2>"
        "<table><thead><tr>"
        "<th>node_id</th><th>instance_id</th><th>pool_name</th><th>pool_id</th><th>owner_client_id</th><th>status</th><th>workers</th><th>tasks</th><th>in_flight</th><th>calls</th><th>errors</th><th>avg_total_ms</th><th>avg_child_decode_ms</th><th>avg_child_invoke_ms</th><th>avg_child_encode_ms</th><th>code_version</th><th>created_at</th><th>last_heartbeat_at</th><th>lease_expire_at</th>"
        "</tr></thead><tbody>"
        f"{pool_body}"
        "</tbody></table></body></html>"
    )


class InfoCenterHttpServer:
    def __init__(
        self,
        *,
        bind: str,
        state: Optional[InfoCenterState] = None,
        gateway_app: Optional[GatewayHttpApp] = None,
        job_queue: Optional[JobQueueManager] = None,
        auth_token: str = "",
    ) -> None:
        self._bind = bind
        self.state = state or InfoCenterState()
        self.gateway_app = gateway_app
        self.job_queue = job_queue
        env_token = str(os.getenv("PYCLOUD_INFOCENTER_TOKEN", "") or "").strip()
        self.auth_token = str(auth_token or env_token or "").strip()
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self.base_url = ""

    def start(self) -> None:
        if self._server is not None:
            return
        host, port = _split_host_port(self._bind)
        state = self.state
        gateway_app = self.gateway_app
        if gateway_app is not None:
            gateway_app.start()

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                parsed = urlparse(self.path)
                parts = [x for x in parsed.path.split("/") if x]
                if self._requires_auth(parsed.path):
                    if not self._check_auth():
                        self._send_json(401, {"ok": False, "error": "unauthorized"})
                        return
                if parsed.path == "/nodes/register":
                    payload = self._read_json()
                    if payload is None:
                        return
                    node = state.register_node_record(
                        node_instance_id=str(payload.get("node_instance_id", "")).strip() or str(payload.get("node_id", "")).strip(),
                        node_id=str(payload.get("node_id", "")).strip(),
                        control_addr=str(payload.get("control_addr", "")).strip(),
                        capacity=max(1, int(payload.get("capacity", 1) or 1)),
                        queue_capacity=max(1, int(payload.get("queue_capacity", 1) or 1)),
                        tags=payload.get("tags") or [],
                        version=str(payload.get("version", "") or ""),
                        python_version=str(payload.get("python_version", "") or ""),
                        metadata=dict(payload.get("metadata") or {}),
                        services=_parse_services(payload.get("services")),
                        task_pools=_parse_task_pools(payload.get("task_pools")),
                        active_runtimes=[str(x).strip() for x in (payload.get("active_runtimes") or []) if str(x).strip()],
                        service_worker_capacity=max(0, int(payload.get("service_worker_capacity", 0) or 0)),
                        service_worker_used=max(0, int(payload.get("service_worker_used", 0) or 0)),
                        task_pool_worker_capacity=max(0, int(payload.get("task_pool_worker_capacity", 0) or 0)),
                        task_pool_worker_used=max(0, int(payload.get("task_pool_worker_used", 0) or 0)),
                        capability=_parse_node_capability(payload.get("capability")),
                    )
                    self._send_json(200, {"ok": True, "heartbeat_interval_sec": state.heartbeat_interval_sec, "node": _serialize_node(node)})
                    return
                if parsed.path == "/nodes/heartbeat":
                    payload = self._read_json()
                    if payload is None:
                        return
                    metrics_raw = payload.get("metrics") or {}
                    node = state.heartbeat_record(
                        node_instance_id=str(payload.get("node_instance_id", "")).strip() or str(payload.get("node_id", "")).strip(),
                        node_id=str(payload.get("node_id", "")).strip(),
                        healthy=bool(payload.get("healthy", True)),
                        metrics=NodeMetricsState(
                            queued=max(0, int(metrics_raw.get("queued", 0) or 0)),
                            inflight=max(0, int(metrics_raw.get("inflight", 0) or 0)),
                            running=max(0, int(metrics_raw.get("running", 0) or 0)),
                            credit=max(0, int(metrics_raw.get("credit", 0) or 0)),
                            cpu_percent=float(metrics_raw.get("cpu_percent", 0.0) or 0.0),
                            mem_percent=float(metrics_raw.get("mem_percent", 0.0) or 0.0),
                        ),
                        metadata=dict(payload.get("metadata") or {}),
                        services=_parse_services(payload.get("services")),
                        task_pools=_parse_task_pools(payload.get("task_pools")),
                        python_version=str(payload.get("python_version", "") or ""),
                        active_runtimes=[str(x).strip() for x in (payload.get("active_runtimes") or []) if str(x).strip()],
                        service_worker_capacity=max(0, int(payload.get("service_worker_capacity", 0) or 0)),
                        service_worker_used=max(0, int(payload.get("service_worker_used", 0) or 0)),
                        task_pool_worker_capacity=max(0, int(payload.get("task_pool_worker_capacity", 0) or 0)),
                        task_pool_worker_used=max(0, int(payload.get("task_pool_worker_used", 0) or 0)),
                        capability=_parse_node_capability(payload.get("capability")),
                    )
                    if node is None:
                        self._send_json(404, {"ok": False, "error": "unknown node"})
                        return
                    self._send_json(200, {"ok": True, "accepted": True, "next_heartbeat_in_sec": state.heartbeat_interval_sec})
                    return
                if parsed.path == "/data/register":
                    payload = self._read_json()
                    if payload is None:
                        return
                    try:
                        ref_payload = payload.get("ref")
                        entry = state.register_data_ref_record(
                            ref=coerce_data_ref(ref_payload),
                            ttl_sec=max(1, int(payload.get("ttl_sec", 3600) or 3600)),
                            node_id=str(payload.get("node_id", "") or "").strip(),
                            node_instance_id=str(payload.get("node_instance_id", "") or "").strip(),
                            control_addr=str(payload.get("control_addr", "") or "").strip(),
                            locator_kind=str(payload.get("locator_kind", "") or "").strip(),
                            locator_token=str(payload.get("locator_token", "") or "").strip(),
                            replicas=list(payload.get("replicas") or ()),
                        )
                    except Exception as exc:
                        self._send_json(400, {"ok": False, "error": str(exc)})
                        return
                    pin_targets = list(entry.replicas or ())
                    if not pin_targets and str(entry.control_addr or "").strip():
                        pin_targets = [{"control_addr": str(entry.control_addr or "").strip()}]
                    for item in pin_targets:
                        control_addr = str(item.get("control_addr", "") or "").strip()
                        if not control_addr:
                            continue
                        try:
                            with NodeControlClient(control_addr, timeout_sec=5.0) as client:
                                client.pin_object(
                                    object_id=str(entry.storage_id or entry.ref_id or ""),
                                    ref_id=str(entry.ref_id or ""),
                                )
                        except Exception:
                            continue
                    self._send_json(200, {"ok": True, "entry": _serialize_data_registry_entry(entry)})
                    return
                if parsed.path == "/data/touch":
                    payload = self._read_json()
                    if payload is None:
                        return
                    try:
                        entry = state.touch_data_ref_record(str(payload.get("ref_id", "") or "").strip())
                    except KeyError:
                        self._send_json(404, {"ok": False, "error": "data ref not found"})
                        return
                    except Exception as exc:
                        self._send_json(400, {"ok": False, "error": str(exc)})
                        return
                    self._send_json(200, {"ok": True, "entry": _serialize_data_registry_entry(entry)})
                    return
                if parsed.path == "/data/release":
                    payload = self._read_json()
                    if payload is None:
                        return
                    ref_id = str(payload.get("ref_id", "") or "").strip()
                    try:
                        entry = state.resolve_data_ref_record(ref_id)
                    except KeyError:
                        self._send_json(404, {"ok": False, "error": "data ref not found"})
                        return
                    released = state.release_data_ref_record(ref_id)
                    if released:
                        release_targets = list(entry.replicas or ())
                        if not release_targets and str(entry.control_addr or "").strip():
                            release_targets = [{"control_addr": str(entry.control_addr or "").strip()}]
                        for item in release_targets:
                            control_addr = str(item.get("control_addr", "") or "").strip()
                            if not control_addr:
                                continue
                            try:
                                with NodeControlClient(control_addr, timeout_sec=5.0) as client:
                                    client.release_object_ref(
                                        object_id=str(entry.storage_id or entry.ref_id or ""),
                                        ref_id=str(entry.ref_id or ""),
                                    )
                            except Exception:
                                continue
                    self._send_json(200, {"ok": True, "released": bool(released)})
                    return
                if parsed.path == "/jobs/submit":
                    if self.server_ref.job_queue is None:
                        self._send_json(503, {"ok": False, "error": "job queue unavailable"})
                        return
                    payload = self._read_json()
                    if payload is None:
                        return
                    job = self.server_ref.job_queue.submit_job(payload)
                    self._send_json(200, {"ok": True, "job": job.as_dict()})
                    return
                if len(parts) == 3 and parts[0] == "jobs" and parts[2] == "cancel":
                    if self.server_ref.job_queue is None:
                        self._send_json(503, {"ok": False, "error": "job queue unavailable"})
                        return
                    state_obj = self.server_ref.job_queue.cancel_job(parts[1])
                    if state_obj is None:
                        self._send_json(404, {"ok": False, "error": "job not found"})
                        return
                    self._send_json(200, {"ok": True, "job": state_obj.as_dict()})
                    return
                if len(parts) == 4 and parts[:2] == ["ops", "nodes"]:
                    node_instance_id = parts[2]
                    action = parts[3]
                    if action == "cordon":
                        state.update_node_schedule_state(node_instance_id, schedulable=False)
                    elif action == "uncordon":
                        state.update_node_schedule_state(node_instance_id, schedulable=True)
                    elif action == "drain":
                        state.update_node_schedule_state(node_instance_id, drain=True)
                    elif action == "undrain":
                        state.update_node_schedule_state(node_instance_id, drain=False)
                    elif action == "mark-lost":
                        state.mark_node_lost(node_instance_id, reason="marked lost via ops")
                    else:
                        self._send_json(404, {"ok": False, "error": "unknown ops action"})
                        return
                    if "text/html" in (self.headers.get("Accept", "")):
                        self.send_response(303)
                        self.send_header("Location", "/ops")
                        self.end_headers()
                        return
                    self._send_json(200, {"ok": True})
                    return
                if len(parts) == 6 and parts[:2] == ["ops", "job-queues"] and parts[3] == "jobs":
                    owner = str(parts[2] or "").strip()
                    job_id = str(parts[4] or "").strip()
                    action = str(parts[5] or "").strip()
                    direction = "up" if action == "move-up" else ("down" if action == "move-down" else "")
                    if not direction:
                        self._send_json(404, {"ok": False, "error": "unknown job queue action"})
                        return
                    try:
                        if owner == "embedded":
                            if self.server_ref.job_queue is None:
                                raise RuntimeError("embedded job queue unavailable")
                            state_obj = self.server_ref.job_queue.reorder_job(job_id, direction=direction)
                            if state_obj is None:
                                self._send_json(404, {"ok": False, "error": "job not found"})
                                return
                            if state_obj.status != "WAITING":
                                self._send_json(409, {"ok": False, "error": "only waiting jobs can be reordered"})
                                return
                        else:
                            http_base_url = _job_queue_service_http_base(state, owner)
                            if not http_base_url:
                                raise RuntimeError("job orchestrator route not found")
                            _reorder_job_via_http(http_base_url, job_id, direction=direction)
                    except Exception as exc:
                        self._send_json(502, {"ok": False, "error": str(exc)})
                        return
                    if "text/html" in (self.headers.get("Accept", "")):
                        self.send_response(303)
                        self.send_header("Location", "/ops")
                        self.end_headers()
                        return
                    self._send_json(200, {"ok": True})
                    return
                if gateway_app is not None:
                    try:
                        length = int(self.headers.get("Content-Length", "0") or 0)
                    except Exception:
                        length = 0
                    handled = gateway_app.handle_post_stream(
                        path=self.path,
                        headers=self.headers,
                        stream=self.rfile,
                        content_length=length,
                    )
                    if handled is not None:
                        code, resp = handled
                        self._send_json(code, resp)
                        return
                    body = self._read_body()
                    if body is None:
                        return
                    handled = gateway_app.handle_post(path=self.path, headers=self.headers, body=body)
                    if handled is not None:
                        code, resp = handled
                        self._send_json(code, resp)
                        return
                self._send_json(404, {"ok": False, "error": "not found"})

            def do_GET(self):  # noqa: N802
                parsed = urlparse(self.path)
                if self._requires_auth(parsed.path):
                    if not self._check_auth():
                        self._send_json(401, {"ok": False, "error": "unauthorized"})
                        return
                if parsed.path == "/nodes":
                    qs = parse_qs(parsed.query)
                    tags = [x for x in ",".join(qs.get("tags", [])).split(",") if x]
                    healthy_only = str((qs.get("healthy_only", ["true"]) or ["true"])[0]).lower() not in ("0", "false", "no")
                    limit = max(1, int((qs.get("limit", ["100"]) or ["100"])[0]))
                    nodes = [_serialize_node(item) for item in state.list_nodes(healthy_only=healthy_only, tags=tags, limit=limit)]
                    self._send_json(200, {"ok": True, "nodes": nodes})
                    return
                if parsed.path == "/services/routes":
                    qs = parse_qs(parsed.query)
                    service_name = str((qs.get("service_name", [""]) or [""])[0])
                    healthy_only = str((qs.get("healthy_only", ["true"]) or ["true"])[0]).lower() not in ("0", "false", "no")
                    limit = max(1, int((qs.get("limit", ["500"]) or ["500"])[0]))
                    routes = state.list_service_routes(service_name=service_name, healthy_only=healthy_only, limit=limit)
                    serialized = []
                    for item in routes:
                        row = dict(item)
                        row["lease_expire_at"] = _dt_text(item["lease_expire_at"])
                        serialized.append(row)
                    self._send_json(200, {"ok": True, "routes": serialized})
                    return
                if parsed.path == "/data/refs":
                    qs = parse_qs(parsed.query)
                    limit = max(1, int((qs.get("limit", ["1000"]) or ["1000"])[0]))
                    node_id = str((qs.get("node_id", [""]) or [""])[0] or "").strip()
                    node_instance_id = str((qs.get("node_instance_id", [""]) or [""])[0] or "").strip()
                    entries = [
                        _serialize_data_registry_entry(item)
                        for item in state.list_data_ref_records(
                            limit=limit,
                            node_id=node_id,
                            node_instance_id=node_instance_id,
                        )
                    ]
                    self._send_json(200, {"ok": True, "refs": entries})
                    return
                if len(parts := [x for x in parsed.path.split("/") if x]) == 3 and parts[0] == "data" and parts[1] == "resolve":
                    try:
                        entry = state.resolve_data_ref_record(unquote(parts[2]))
                    except KeyError:
                        self._send_json(404, {"ok": False, "error": "data ref not found"})
                        return
                    self._send_json(200, {"ok": True, "entry": _serialize_data_registry_entry(entry)})
                    return
                if parsed.path == "/ops":
                    if self._requires_auth(parsed.path):
                        if not self._check_auth():
                            self._send_json(401, {"ok": False, "error": "unauthorized"})
                            return
                    raw = _render_ops_page(state, self.server_ref.job_queue).encode("utf-8")
                    try:
                        self.send_response(200)
                        self.send_header("Content-Type", "text/html; charset=utf-8")
                        self.send_header("Content-Length", str(len(raw)))
                        self.end_headers()
                        self.wfile.write(raw)
                    except Exception as exc:
                        if not _is_client_disconnect_error(exc):
                            raise
                    return
                if len([x for x in parsed.path.split("/") if x]) == 2 and parsed.path.split("/")[1] == "jobs":
                    parts = [x for x in parsed.path.split("/") if x]
                    if self.server_ref.job_queue is None:
                        self._send_json(503, {"ok": False, "error": "job queue unavailable"})
                        return
                    state_obj = self.server_ref.job_queue.get_job(parts[1])
                    if state_obj is None:
                        self._send_json(404, {"ok": False, "error": "job not found"})
                        return
                    self._send_json(200, {"ok": True, "job": state_obj.as_dict()})
                    return
                if gateway_app is not None:
                    handled = gateway_app.handle_get(path=self.path, headers=self.headers)
                    if handled is not None:
                        code, resp = handled
                        self._send_json(code, resp)
                        return
                self._send_json(404, {"ok": False, "error": "not found"})

            def log_message(self, fmt, *args):  # noqa: A003
                return

            @property
            def server_ref(self) -> "InfoCenterHttpServer":
                return self.server.pycloud_owner  # type: ignore[attr-defined]

            def _read_json(self) -> Optional[dict]:
                body = self._read_body()
                if body is None:
                    return None
                try:
                    payload = json.loads(body.decode("utf-8") if body else "{}")
                except Exception:
                    self._send_json(400, {"ok": False, "error": "invalid json body"})
                    return None
                if not isinstance(payload, dict):
                    self._send_json(400, {"ok": False, "error": "json body must be object"})
                    return None
                return payload

            def _read_body(self) -> Optional[bytes]:
                try:
                    length = int(self.headers.get("Content-Length", "0") or 0)
                except Exception:
                    length = 0
                if length > MAX_BODY_BYTES:
                    self._send_json(413, {"ok": False, "error": "payload too large"})
                    return None
                return self.rfile.read(max(0, length))

            def _requires_auth(self, path: str) -> bool:
                if not self.server_ref.auth_token:
                    return False
                return path.startswith("/nodes") or path.startswith("/jobs") or path.startswith("/ops")

            def _check_auth(self) -> bool:
                token = self.server_ref.auth_token
                if not token:
                    return True
                x_token = str(self.headers.get("X-Infocenter-Token", "") or "").strip()
                if x_token:
                    return x_token == token
                auth = str(self.headers.get("Authorization", "") or "").strip()
                low = auth.lower()
                if low.startswith("bearer "):
                    return auth[7:].strip() == token
                return False

            def _send_json(self, status_code: int, data: Dict[str, object]) -> None:
                raw = json.dumps(serialize_arrow_compatible(data), ensure_ascii=False).encode("utf-8")
                try:
                    self.send_response(status_code)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                except Exception as exc:
                    if not _is_client_disconnect_error(exc):
                        raise

        self._server = ThreadingHTTPServer((host, port), _Handler)
        self._server.pycloud_owner = self  # type: ignore[attr-defined]
        self._thread = threading.Thread(target=self._server.serve_forever, name="infocenter-http", daemon=True)
        self._thread.start()
        public_host = resolve_public_host(host)
        actual_port = int(self._server.server_address[1])
        self.base_url = f"http://{public_host}:{actual_port}"
        if self.gateway_app is not None:
            self.gateway_app.controlplane_target = self.base_url
        if self.job_queue is not None:
            self.job_queue.start(controlplane_target=self.base_url)

    def wait_for_termination(self) -> None:
        thread = self._thread
        if thread is not None:
            thread.join()

    def stop(self, grace: int = 0) -> None:
        del grace
        if self.job_queue is not None:
            self.job_queue.close()
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None
        if self.gateway_app is not None:
            self.gateway_app.stop()
