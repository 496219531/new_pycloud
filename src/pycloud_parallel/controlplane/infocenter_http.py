from __future__ import annotations

"""HTTP + JSON server for InfoCenter control-plane and lightweight ops UI."""

import errno
import hashlib
import os
import html
import json
import logging
import re
import threading
from datetime import datetime, timezone
from importlib import metadata as importlib_metadata
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pycloud_parallel.data.ref import DataRef, coerce_data_ref
from pycloud_parallel.controlplane.config import get_infocenter_http_body_limit_bytes
from pycloud_parallel.controlplane.data_registry import ResolvedDataRef
from pycloud_parallel.controlplane.data_plane_http import DataPlaneHttpApp
from pycloud_parallel.controlplane.gateway_http import GatewayHttpApp
from pycloud_parallel.controlplane.http_gateway import StreamingHttpResponse
from pycloud_parallel.controlplane.job_queue import JobQueueManager
from pycloud_parallel.controlplane.netutil import resolve_public_host
from pycloud_parallel.controlplane.node_capability import NodeCapability
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.controlplane.serialization import serialize_arrow_compatible
from pycloud_parallel.controlplane.infocenter.models import (
    DataRegistryEntry,
    NodeMetricsState,
    NodeServiceState,
    NodeTaskPoolInfo,
)
from pycloud_parallel.controlplane.infocenter_state import InfoCenterState
from pycloud_parallel.controlplane.state_time import utc_now
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


MAX_BODY_BYTES = get_infocenter_http_body_limit_bytes()
logger = logging.getLogger(__name__)


def _is_client_disconnect_error(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
        return True
    if isinstance(exc, OSError):
        return getattr(exc, "errno", None) in (errno.EPIPE, errno.ECONNRESET, errno.ECONNABORTED)
    return False


def _coerce_bool(value: object, *, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


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


def _parse_optional_dt(raw: object) -> Optional[datetime]:
    if raw is None:
        return None
    if isinstance(raw, str) and not raw.strip():
        return None
    return _parse_dt(raw)


def _failure_text_with_time(reason: object, failure_at: object = None) -> str:
    text = str(reason or "").strip()
    if not text:
        return "-"
    parsed_at = _parse_optional_dt(failure_at)
    if parsed_at is None:
        return text
    return f"[{_dt_text(parsed_at)}] {text}"


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
            status=max(0, int(item.get("status", 0) or 0)),
            service_name=service_name,
            service_id=service_id,
            policy_id=str(item.get("policy_id", "") or "default_safe"),
            owner_client_id=str(item.get("owner_client_id", "") or ""),
            code_version=str(item.get("code_version", "") or ""),
            entry_module=str(item.get("entry_module", "") or ""),
            entry_callable=str(item.get("entry_callable", "") or ""),
            serialization_mode=str(item.get("serialization_mode", "") or ""),
            status_text=str(item.get("status_text", "") or ""),
            resource_health=str(item.get("resource_health", "") or "") or (
                "degraded"
                if _coerce_bool(item.get("degraded"), default=False)
                else ""
            ),
            degraded=_coerce_bool(item.get("degraded"), default=False),
            worker_count=max(0, int(item.get("worker_count", 0) or 0)),
            alive_workers=max(0, int(item.get("alive_workers", 0) or 0)),
            in_flight=max(0, int(item.get("in_flight", 0) or 0)),
            received_count=max(0, int(item.get("received_count", 0) or 0)),
            returned_count=max(0, int(item.get("returned_count", 0) or 0)),
            ema_child_invoke_ms=float(item.get("ema_child_invoke_ms", 0.0) or 0.0),
            ema_samples=max(0, int(item.get("ema_samples", 0) or 0)),
            lease_expire_at=_parse_dt(item.get("lease_expire_at") or item.get("lease_expire_at_ts") or utc_now()),
            http_base_url=str(item.get("http_base_url", "") or ""),
            stop_reason=str(item.get("stop_reason", item.get("failure_reason", "")) or ""),
            failure_at=_parse_optional_dt(item.get("failure_at") or item.get("failure_at_ts")),
        )
    return out


def _service_endpoint_key(http_base_url: str) -> str:
    text = str(http_base_url or "").strip()
    if not text:
        return ""
    parsed = urlparse(text)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return text


def _merge_services_for_display(services: List[NodeServiceState]) -> List[Dict[str, object]]:
    grouped: Dict[Tuple[str, str], List[NodeServiceState]] = {}
    for svc in services:
        key = (str(svc.service_name or "").strip(), _service_endpoint_key(str(svc.http_base_url or "").strip()))
        grouped.setdefault(key, []).append(svc)

    merged: List[Dict[str, object]] = []
    for (_service_name, _endpoint_key), items in sorted(grouped.items(), key=lambda item: item[0]):
        ordered = sorted(
            items,
            key=lambda svc: (
                getattr(svc, "lease_expire_at", utc_now()),
                int(getattr(svc, "alive_workers", 0) or 0),
                int(getattr(svc, "worker_count", 0) or 0),
                str(getattr(svc, "service_id", "") or ""),
            ),
            reverse=True,
        )
        primary = ordered[0]
        duplicate_count = len(ordered)
        merged.append(
            {
                "primary": primary,
                "service_id": str(primary.service_id or ""),
                "service_name": str(primary.service_name or ""),
                "status": int(primary.status),
                "worker_count": max(int(getattr(item, "worker_count", 0) or 0) for item in ordered),
                "alive_workers": max(int(getattr(item, "alive_workers", 0) or 0) for item in ordered),
                "in_flight": max(int(getattr(item, "in_flight", 0) or 0) for item in ordered),
                "lease_expire_at": max(getattr(item, "lease_expire_at", utc_now()) for item in ordered),
                "http_base_url": str(primary.http_base_url or ""),
                "stop_reason": "; ".join(
                    sorted(
                        {
                            str(getattr(item, "stop_reason", "") or "").strip()
                            for item in ordered
                            if str(getattr(item, "stop_reason", "") or "").strip()
                        }
                    )
                ),
                "failure_at": max(
                    (
                        item.failure_at
                        for item in ordered
                        if getattr(item, "failure_at", None) is not None
                    ),
                    default=None,
                ),
                "duplicate_count": duplicate_count,
                "service_ids": [str(item.service_id or "") for item in ordered],
            }
        )
    return merged


def _merge_nodes_for_display(nodes: List[object]) -> List[object]:
    grouped: Dict[str, List[object]] = {}
    for node in nodes:
        key = str(getattr(node, "control_addr", "") or "").strip()
        if not key:
            key = str(getattr(node, "node_instance_id", "") or getattr(node, "node_id", "") or "").strip()
        grouped.setdefault(key, []).append(node)

    merged: List[object] = []
    for _endpoint, items in sorted(grouped.items(), key=lambda item: item[0]):
        ordered = sorted(
            items,
            key=lambda node: (
                bool(getattr(node, "healthy", False)),
                getattr(node, "last_seen_at", utc_now()),
                str(getattr(node, "node_instance_id", "") or ""),
            ),
            reverse=True,
        )
        primary = ordered[0]
        if len(ordered) == 1:
            merged.append(primary)
            continue

        services: Dict[str, NodeServiceState] = {}
        task_pools: Dict[str, NodeTaskPoolInfo] = {}
        active_runtimes = set()
        node_ids = []
        instance_ids = []
        reasons = []
        tags = set()
        for node in ordered:
            node_id = str(getattr(node, "node_id", "") or "").strip()
            instance_id = str(getattr(node, "node_instance_id", "") or "").strip()
            if node_id and node_id not in node_ids:
                node_ids.append(node_id)
            if instance_id and instance_id not in instance_ids:
                instance_ids.append(instance_id)
            reason = str(getattr(node, "reason", "") or "").strip()
            if reason and reason not in reasons:
                reasons.append(reason)
            tags.update(str(tag) for tag in (getattr(node, "tags", []) or []) if str(tag))
            active_runtimes.update(str(item) for item in (getattr(node, "active_runtimes", []) or []) if str(item))
            services.update(dict(getattr(node, "services", {}) or {}))
            task_pools.update(dict(getattr(node, "task_pools", {}) or {}))

        merged.append(
            SimpleNamespace(
                node_instance_id=", ".join(instance_ids) or str(getattr(primary, "node_instance_id", "") or ""),
                action_node_instance_id=str(getattr(primary, "node_instance_id", "") or ""),
                node_id=", ".join(node_ids) or str(getattr(primary, "node_id", "") or ""),
                control_addr=str(getattr(primary, "control_addr", "") or ""),
                capacity=max(int(getattr(node, "capacity", 0) or 0) for node in ordered),
                queue_capacity=max(int(getattr(node, "queue_capacity", 0) or 0) for node in ordered),
                tags=sorted(tags),
                profile_key=str(getattr(primary, "profile_key", "") or ""),
                managed_tags=sorted({str(tag) for node in ordered for tag in (getattr(node, "managed_tags", []) or []) if str(tag)}),
                capability_tags=sorted({str(tag) for node in ordered for tag in (getattr(node, "capability_tags", []) or []) if str(tag)}),
                legacy_node_tags=sorted({str(tag) for node in ordered for tag in (getattr(node, "legacy_node_tags", []) or []) if str(tag)}),
                profile_enabled=any(bool(getattr(node, "profile_enabled", True)) for node in ordered),
                profile_notes=str(getattr(primary, "profile_notes", "") or ""),
                version=str(getattr(primary, "version", "") or ""),
                python_version=str(getattr(primary, "python_version", "") or ""),
                metadata=dict(getattr(primary, "metadata", {}) or {}),
                healthy=any(bool(getattr(node, "healthy", False)) for node in ordered),
                last_seen_at=max(getattr(node, "last_seen_at", utc_now()) for node in ordered),
                metrics=getattr(primary, "metrics"),
                services=services,
                task_pools=task_pools,
                active_runtimes=sorted(active_runtimes),
                service_worker_capacity=max(int(getattr(node, "service_worker_capacity", 0) or 0) for node in ordered),
                service_worker_used=max(int(getattr(node, "service_worker_used", 0) or 0) for node in ordered),
                task_pool_worker_capacity=max(int(getattr(node, "task_pool_worker_capacity", 0) or 0) for node in ordered),
                task_pool_worker_used=max(int(getattr(node, "task_pool_worker_used", 0) or 0) for node in ordered),
                accept_service_deploy=any(bool(getattr(node, "accept_service_deploy", True)) for node in ordered),
                schedulable=any(bool(getattr(node, "schedulable", False)) for node in ordered),
                drain=all(bool(getattr(node, "drain", False)) for node in ordered),
                reason="; ".join(reasons + [f"merged_nodes={len(ordered)}"]),
                capability=getattr(primary, "capability"),
                service_worker_available=lambda nodes=tuple(ordered): max(
                    0,
                    max(int(getattr(node, "service_worker_available")()) for node in nodes),
                ),
                task_pool_worker_available=lambda nodes=tuple(ordered): max(
                    0,
                    max(int(getattr(node, "task_pool_worker_available")()) for node in nodes),
                ),
                active_runtime_count=lambda count=len(active_runtimes): count,
            )
        )
    return merged


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
            resource_health=str(item.get("resource_health", "") or "") or (
                "degraded"
                if _coerce_bool(item.get("degraded"), default=False)
                else ""
            ),
            degraded=_coerce_bool(item.get("degraded"), default=False),
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
            stop_reason=str(item.get("stop_reason", item.get("failure_reason", "")) or ""),
            failure_reason=str(item.get("failure_reason", item.get("stop_reason", "")) or ""),
            failure_at=_parse_optional_dt(item.get("failure_at") or item.get("failure_at_ts")),
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


def _service_resource_health_text(*, node_healthy: bool, service_status: int, alive_workers: int, stop_reason: object = "") -> str:
    if not bool(node_healthy):
        return "node_lost"
    if int(service_status) == int(pb2.SERVICE_STATUS_STOPPED):
        return "stopped"
    if str(stop_reason or "").strip():
        return "failed"
    if int(alive_workers or 0) <= 0:
        return "degraded"
    return "running"


def _task_pool_resource_health_text(
    *,
    node_healthy: bool,
    status: object,
    alive_workers: int,
    stop_reason: object = "",
    degraded: object = False,
) -> str:
    if not bool(node_healthy):
        return "node_lost"
    normalized = str(status or "").strip().upper()
    if normalized == "STOPPED":
        return "stopped"
    if str(stop_reason or "").strip():
        return "failed"
    if normalized == "DEGRADED" or _coerce_bool(degraded, default=False) or int(alive_workers or 0) <= 0:
        return "degraded"
    return "running"


def _ops_badge(text: object, tone: str = "neutral") -> str:
    return f"<span class='badge badge-{html.escape(str(tone), quote=True)}'>{html.escape(str(text))}</span>"


def _ops_bool_badge(value: object, *, true_text: str = "yes", false_text: str = "no", invert: bool = False) -> str:
    ok = bool(value)
    tone = "bad" if ok and invert else "good" if ok else "neutral" if invert else "bad"
    return _ops_badge(true_text if ok else false_text, tone)


def _ops_status_badge(text: object) -> str:
    normalized = str(text or "-").strip().upper()
    if normalized in {"RUNNING", "SUCCEEDED", "YES"}:
        tone = "good"
    elif normalized in {"STARTING", "DRAINING", "PENDING", "WAITING"}:
        tone = "warn"
    elif normalized in {"STOPPED", "FAILED", "LOST", "NO", "ERROR"}:
        tone = "bad"
    else:
        tone = "neutral"
    return _ops_badge(text, tone)


def _ops_metric_card(label: str, value: object, subtext: str = "") -> str:
    detail = f"<div class='metric-sub'>{html.escape(str(subtext))}</div>" if str(subtext or "").strip() else ""
    return (
        "<div class='metric-card'>"
        "<div class='metric-glow'></div>"
        f"<div class='metric-label'>{html.escape(label)}</div>"
        f"<div class='metric-value'>{html.escape(str(value))}</div>"
        f"{detail}"
        "</div>"
    )


def _ops_table(title: str, note: str, headers: List[str], body_id: str, body: str) -> str:
    note_html = f"<div class='section-note'>{html.escape(note)}</div>" if note else ""
    header_html = "".join(f"<th>{html.escape(item)}</th>" for item in headers)
    table_key = re.sub(r"[^a-z0-9]+", "-", body_id.lower()).strip("-")
    if table_key.startswith("ops-"):
        table_key = table_key[4:]
    if table_key.endswith("-body"):
        table_key = table_key[:-5]
    return (
        "<section class='ops-section'>"
        "<div class='section-head'>"
        f"<h2>{html.escape(title)}</h2>"
        f"<a class='section-anchor' href='#{html.escape(body_id, quote=True)}'>focus</a>"
        "</div>"
        f"{note_html}"
        f"<div class='table-wrap'><table class='ops-table ops-table--{html.escape(table_key, quote=True)}'><thead><tr>"
        f"{header_html}"
        f"</tr></thead><tbody id='{html.escape(body_id, quote=True)}'>"
        f"{body}"
        "</tbody></table></div></section>"
    )


def _serialize_service(service: NodeServiceState, *, node_healthy: bool = True) -> Dict[str, object]:
    status_text = _effective_service_status_text(node_healthy=node_healthy, service_status=service.status)
    alive_workers = int(service.alive_workers if node_healthy else 0)
    return {
        "service_name": str(service.service_name),
        "service_id": str(service.service_id),
        "status": int(service.status),
        "status_text": status_text,
        "resource_health": _service_resource_health_text(
            node_healthy=node_healthy,
            service_status=service.status,
            alive_workers=alive_workers,
            stop_reason=service.stop_reason,
        ),
        "policy_id": str(service.policy_id or "").strip().lower() or "default_safe",
        "owner_client_id": str(getattr(service, "owner_client_id", "") or ""),
        "code_version": str(getattr(service, "code_version", "") or ""),
        "entry_module": str(getattr(service, "entry_module", "") or ""),
        "entry_callable": str(getattr(service, "entry_callable", "") or ""),
        "serialization_mode": str(getattr(service, "serialization_mode", "") or ""),
        "node_healthy": bool(node_healthy),
        "worker_count": int(service.worker_count),
        "alive_workers": alive_workers,
        "in_flight": int(service.in_flight if node_healthy else 0),
        "lease_expire_at": _dt_text(service.lease_expire_at),
        "http_base_url": str(service.http_base_url or ""),
        "stop_reason": str(service.stop_reason or ""),
        "failure_at": _dt_text(service.failure_at) if getattr(service, "failure_at", None) is not None else "",
    }


def _serialize_node(state) -> Dict[str, object]:
    services = [
        _serialize_service(svc, node_healthy=bool(state.healthy))
        for svc in sorted(state.services.values(), key=lambda item: (item.service_name, item.service_id))
    ]
    task_pools = sorted(state.task_pools.values(), key=lambda item: (item.created_at, item.pool_name, item.pool_id), reverse=True)
    active_task_pools = [pool for pool in task_pools if str(pool.status or "").strip().upper() in {"RUNNING", "DEGRADED"}]
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
        "profile_key": str(getattr(state, "profile_key", "") or ""),
        "managed_tags": list(getattr(state, "managed_tags", []) or []),
        "capability_tags": list(getattr(state, "capability_tags", []) or []),
        "legacy_node_tags": list(getattr(state, "legacy_node_tags", []) or []),
        "profile_enabled": bool(getattr(state, "profile_enabled", True)),
        "profile_notes": str(getattr(state, "profile_notes", "") or ""),
        "version": str(state.version or ""),
        "metadata": dict(state.metadata),
        "last_seen_at": _dt_text(state.last_seen_at),
        "service_worker_capacity": int(state.service_worker_capacity),
        "service_worker_used": int(state.service_worker_used),
        "service_worker_available": int(state.service_worker_available()),
        "task_pool_worker_capacity": int(state.task_pool_worker_capacity),
        "task_pool_worker_used": int(state.task_pool_worker_used),
        "task_pool_worker_available": int(state.task_pool_worker_available()),
        "accept_service_deploy": bool(getattr(state, "accept_service_deploy", True)),
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
                "resource_health": str(getattr(pool, "resource_health", "") or "") or _task_pool_resource_health_text(
                    node_healthy=bool(state.healthy),
                    status=pool.status,
                    alive_workers=int(pool.alive_workers if state.healthy else 0),
                    stop_reason=getattr(pool, "stop_reason", "") or getattr(pool, "failure_reason", ""),
                    degraded=getattr(pool, "degraded", False),
                ),
                "degraded": bool(getattr(pool, "degraded", False)),
                "worker_count": int(pool.worker_count),
                "alive_workers": int(pool.alive_workers if state.healthy else 0),
                "task_count": int(pool.task_count),
                "in_flight": int(pool.inflight if state.healthy else 0),
                "inflight": int(pool.inflight if state.healthy else 0),
                "received_count": int(pool.received_count),
                "returned_count": int(pool.returned_count),
                "ema_child_invoke_ms": float(pool.ema_child_invoke_ms),
                "ema_samples": int(pool.ema_samples),
                "created_at": _dt_text(pool.created_at),
                "last_heartbeat_at": _dt_text(pool.last_heartbeat_at),
                "lease_expire_at": _dt_text(pool.lease_expire_at),
                "stop_reason": str(getattr(pool, "stop_reason", "") or getattr(pool, "failure_reason", "") or ""),
                "failure_reason": str(pool.failure_reason or ""),
                "failure_at": _dt_text(pool.failure_at) if getattr(pool, "failure_at", None) is not None else "",
            }
            for pool in task_pools
        ],
    }


def _ops_snapshot_content_key(nodes: List[object], *, job_summary: Optional[Dict[str, object]] = None) -> str:
    payload: Dict[str, object] = {
        "nodes": [],
        "job_queue": job_summary or {},
    }
    for node in sorted(nodes, key=lambda item: (str(getattr(item, "node_id", "") or ""), str(getattr(item, "node_instance_id", "") or ""))):
        metrics = getattr(node, "metrics", None)
        metadata = dict(getattr(node, "metadata", {}) or {})
        node_payload: Dict[str, object] = {
            "node_instance_id": str(getattr(node, "node_instance_id", "") or ""),
            "node_id": str(getattr(node, "node_id", "") or ""),
            "control_addr": str(getattr(node, "control_addr", "") or ""),
            "healthy": bool(getattr(node, "healthy", False)),
            "schedulable": bool(getattr(node, "schedulable", False)),
            "drain": bool(getattr(node, "drain", False)),
            "reason": str(getattr(node, "reason", "") or ""),
            "capacity": int(getattr(node, "capacity", 0) or 0),
            "queue_capacity": int(getattr(node, "queue_capacity", 0) or 0),
            "queued": int(getattr(metrics, "queued", 0) or 0),
            "inflight": int(getattr(metrics, "inflight", 0) or 0),
            "running": int(getattr(metrics, "running", 0) or 0),
            "credit": int(getattr(metrics, "credit", 0) or 0),
            "python_version": str(getattr(node, "python_version", "") or ""),
            "active_runtimes": list(getattr(node, "active_runtimes", []) or []),
            "tags": list(getattr(node, "tags", []) or []),
            "profile_key": str(getattr(node, "profile_key", "") or ""),
            "managed_tags": list(getattr(node, "managed_tags", []) or []),
            "capability_tags": list(getattr(node, "capability_tags", []) or []),
            "legacy_node_tags": list(getattr(node, "legacy_node_tags", []) or []),
            "profile_enabled": bool(getattr(node, "profile_enabled", True)),
            "profile_notes": str(getattr(node, "profile_notes", "") or ""),
            "version": str(getattr(node, "version", "") or ""),
            "metadata": metadata,
            "service_worker_capacity": int(getattr(node, "service_worker_capacity", 0) or 0),
            "service_worker_used": int(getattr(node, "service_worker_used", 0) or 0),
            "task_pool_worker_capacity": int(getattr(node, "task_pool_worker_capacity", 0) or 0),
            "task_pool_worker_used": int(getattr(node, "task_pool_worker_used", 0) or 0),
            "accept_service_deploy": bool(getattr(node, "accept_service_deploy", True)),
            "services": [],
            "task_pools": [],
        }
        for svc in sorted(getattr(node, "services", {}).values(), key=lambda item: (str(getattr(item, "service_name", "") or ""), str(getattr(item, "service_id", "") or ""))):
            node_payload["services"].append(
                {
                    "service_name": str(getattr(svc, "service_name", "") or ""),
                    "service_id": str(getattr(svc, "service_id", "") or ""),
                    "status": int(getattr(svc, "status", 0) or 0),
                    "policy_id": str(getattr(svc, "policy_id", "") or ""),
                    "owner_client_id": str(getattr(svc, "owner_client_id", "") or ""),
                    "code_version": str(getattr(svc, "code_version", "") or ""),
                    "entry_module": str(getattr(svc, "entry_module", "") or ""),
                    "entry_callable": str(getattr(svc, "entry_callable", "") or ""),
                    "serialization_mode": str(getattr(svc, "serialization_mode", "") or ""),
                    "status_text": str(getattr(svc, "status_text", "") or ""),
                    "resource_health": str(getattr(svc, "resource_health", "") or ""),
                    "degraded": bool(getattr(svc, "degraded", False)),
                    "worker_count": int(getattr(svc, "worker_count", 0) or 0),
                    "alive_workers": int(getattr(svc, "alive_workers", 0) or 0),
                    "in_flight": int(getattr(svc, "in_flight", 0) or 0),
                    "received_count": int(getattr(svc, "received_count", 0) or 0),
                    "returned_count": int(getattr(svc, "returned_count", 0) or 0),
                    "ema_child_invoke_ms": float(getattr(svc, "ema_child_invoke_ms", 0.0) or 0.0),
                    "ema_samples": int(getattr(svc, "ema_samples", 0) or 0),
                    "http_base_url": str(getattr(svc, "http_base_url", "") or ""),
                    "stop_reason": str(getattr(svc, "stop_reason", "") or ""),
                    "failure_at": _dt_text(getattr(svc, "failure_at", "")) if getattr(svc, "failure_at", None) is not None else "",
                }
            )
        for pool in sorted(getattr(node, "task_pools", {}).values(), key=lambda item: (str(getattr(item, "pool_name", "") or ""), str(getattr(item, "pool_id", "") or ""))):
            node_payload["task_pools"].append(
                {
                    "pool_id": str(getattr(pool, "pool_id", "") or ""),
                    "owner_client_id": str(getattr(pool, "owner_client_id", "") or ""),
                    "pool_name": str(getattr(pool, "pool_name", "") or ""),
                    "code_version": str(getattr(pool, "code_version", "") or ""),
                    "status": str(getattr(pool, "status", "") or ""),
                    "resource_health": str(getattr(pool, "resource_health", "") or ""),
                    "degraded": bool(getattr(pool, "degraded", False)),
                    "worker_count": int(getattr(pool, "worker_count", 0) or 0),
                    "alive_workers": int(getattr(pool, "alive_workers", 0) or 0),
                    "task_count": int(getattr(pool, "task_count", 0) or 0),
                    "inflight": int(getattr(pool, "inflight", 0) or 0),
                    "received_count": int(getattr(pool, "received_count", 0) or 0),
                    "returned_count": int(getattr(pool, "returned_count", 0) or 0),
                    "ema_child_invoke_ms": float(getattr(pool, "ema_child_invoke_ms", 0.0) or 0.0),
                    "ema_samples": int(getattr(pool, "ema_samples", 0) or 0),
                    "created_at": _dt_text(getattr(pool, "created_at", "")),
                    "stop_reason": str(getattr(pool, "stop_reason", "") or ""),
                    "failure_reason": str(getattr(pool, "failure_reason", "") or ""),
                    "failure_at": _dt_text(getattr(pool, "failure_at", "")) if getattr(pool, "failure_at", None) is not None else "",
                }
            )
        payload["nodes"].append(node_payload)
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _parse_form_body(body: bytes) -> Dict[str, str]:
    raw = body.decode("utf-8") if body else ""
    parsed = parse_qs(raw, keep_blank_values=True)
    return {str(key): str((values or [""])[0]) for key, values in parsed.items()}


def _parse_node_capability(payload: object) -> NodeCapability:
    return NodeCapability.from_dict(payload if isinstance(payload, dict) else None)


def _serialize_data_registry_entry(entry: DataRegistryEntry, *, public: bool = True) -> Dict[str, object]:
    data = {
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
    if public:
        data["control_addr"] = ""
        data["replicas"] = []
        locator_kind = str(data.get("locator_kind", "") or "").strip().lower()
        if locator_kind == "node_control":
            data["locator_kind"] = "controlplane"
            data["locator_token"] = ""
    return data


def _resolve_data_ref_record_for_data_plane(state: InfoCenterState, ref) -> ResolvedDataRef:
    data_ref = coerce_data_ref(ref)
    entry = state.resolve_data_ref_record(data_ref.ref_id)
    replicas = [
        dict(item)
        for item in (entry.replicas or ())
        if isinstance(item, dict) and str(item.get("control_addr", "") or "").strip()
    ]
    if str(entry.control_addr or "").strip():
        replicas.append(
            {
                "control_addr": str(entry.control_addr or "").strip(),
                "node_id": str(entry.node_id or ""),
                "node_instance_id": str(entry.node_instance_id or ""),
            }
        )
    normalized_replicas = []
    seen = set()
    for item in replicas:
        control_addr = str(item.get("control_addr", "") or "").strip()
        if not control_addr or control_addr in seen:
            continue
        seen.add(control_addr)
        node_instance_id = str(item.get("node_instance_id", "") or "").strip()
        if node_instance_id:
            try:
                node_state = state.get_node(node_instance_id)
            except Exception:
                node_state = None
            if node_state is not None and not bool(getattr(node_state, "healthy", True)):
                continue
        normalized_replicas.append(
            {
                "control_addr": control_addr,
                "node_id": str(item.get("node_id", "") or ""),
                "node_instance_id": node_instance_id,
            }
        )
    if not normalized_replicas:
        raise KeyError("data ref has no healthy node replica")
    best = normalized_replicas[0]
    resolved_ref = DataRef(
        ref_id=str(entry.ref_id or ""),
        storage_id=str(entry.storage_id or entry.ref_id or ""),
        logical_type=str(entry.logical_type or ""),
        format=str(entry.format or ""),
        size_bytes=int(entry.size_bytes or 0),
        materialize_as=str(entry.materialize_as or "auto"),
        locator_kind="node_control",
        locator_token=str(best.get("control_addr", "") or ""),
        consume_on_read=bool(entry.consume_on_read),
        node_id=str(best.get("node_id", "") or entry.node_id or ""),
        node_instance_id=str(best.get("node_instance_id", "") or entry.node_instance_id or ""),
        control_addr=str(best.get("control_addr", "") or ""),
    )
    return ResolvedDataRef(
        ref=resolved_ref,
        control_addr=str(best.get("control_addr", "") or ""),
        node_id=str(best.get("node_id", "") or entry.node_id or ""),
        node_instance_id=str(best.get("node_instance_id", "") or entry.node_instance_id or ""),
        locator_kind="node_control",
        locator_token=str(best.get("control_addr", "") or ""),
        via_registry=True,
        replicas=tuple(normalized_replicas),
    )


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


def _reorder_job_via_http(http_base_url: str, job_id: str, *, direction: str, auth_token: str = "") -> Dict[str, object]:
    base = str(http_base_url or "").strip().rstrip("/")
    if not base:
        raise RuntimeError("job orchestrator route has no http_base_url")
    raw = json.dumps({"job_id": str(job_id or "").strip(), "direction": str(direction or "").strip()}).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    normalized_auth_token = str(auth_token or "").strip()
    if normalized_auth_token:
        headers["Authorization"] = f"Bearer {normalized_auth_token}"
    req = Request(
        f"{base}/call/reorder_job?timeout_sec=5.000",
        method="POST",
        headers=headers,
        data=raw,
    )
    try:
        with urlopen(req, timeout=6.0) as resp:
            payload = json.loads(resp.read().decode("utf-8") or "{}")
    except HTTPError as exc:
        try:
            payload = json.loads((exc.read() or b"{}").decode("utf-8") or "{}")
        except Exception:
            payload = {"ok": False, "error": exc.reason}
        raise RuntimeError(str((payload or {}).get("error", exc.reason))) from exc
    if not isinstance(payload, dict) or not payload.get("ok", False):
        raise RuntimeError(str((payload or {}).get("error", "reorder failed")))
    return payload


def _render_ops_page(state: InfoCenterState, job_queue: Optional[JobQueueManager] = None, *, _snapshot_only: bool = False):
    nodes = _merge_nodes_for_display(state.list_nodes(healthy_only=False, tags=(), limit=10000))
    node_rows: List[str] = []
    service_rows: List[str] = []
    service_entries: Dict[Tuple[str, str], List[Dict[str, object]]] = {}
    pool_entries: List[tuple] = []
    job_queue_rows: List[str] = []
    recent_job_rows: List[tuple] = []
    waiting_job_rows: List[tuple] = []
    current_job_timing: Dict[str, object] = {}
    queue_timing: Dict[str, object] = {}
    job_summary: Dict[str, object] = {}
    if job_queue is not None:
        summary = dict(job_queue.summary() or {})
        job_summary = summary
        current_job_timing = dict(summary.get("current_job_timing") or {})
        queue_timing = dict(summary.get("timing") or {})
        job_queue_rows.append(
            "<tr>"
            "<td>embedded</td>"
            "<td>-</td>"
            f"<td>{_ops_bool_badge(True)}</td>"
            "<td>-</td>"
            f"<td>{html.escape(str(summary.get('current_job_id', '') or '-'))}</td>"
            f"<td>{_ops_status_badge(str(summary.get('current_job_status', '') or '-'))}</td>"
            f"<td>{html.escape(str(summary.get('current_job_phase', '') or '-'))}</td>"
            f"<td>{html.escape(str(current_job_timing.get('pool_action', '') or '-'))}</td>"
            f"<td>{html.escape(str(current_job_timing.get('total_ms', '-')))}</td>"
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
                    f"<td>{_ops_status_badge(str(item.get('status', '') or '-'))}</td>"
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
        merged_services = _merge_services_for_display(services)
        task_pools = sorted(node.task_pools.values(), key=lambda item: (item.created_at, item.pool_name, item.pool_id), reverse=True)
        node_healthy = bool(node.healthy)
        timing_map = _parse_service_timing_metrics(dict(node.metadata))
        pool_timing_map = _parse_task_pool_timing_metrics(dict(node.metadata))
        active_runtimes = ", ".join(node.active_runtimes[:10]) or "-"
        effective_tags = ", ".join(getattr(node, "tags", []) or []) or "-"
        managed_tags = ", ".join(getattr(node, "managed_tags", []) or []) or "-"
        capability_tags = ", ".join(getattr(node, "capability_tags", []) or []) or "-"
        legacy_node_tags = ", ".join(getattr(node, "legacy_node_tags", []) or []) or "-"
        profile_key = str(getattr(node, "profile_key", "") or "").strip()
        profile_notes = str(getattr(node, "profile_notes", "") or "")
        action_node_id = html.escape(getattr(node, "action_node_instance_id", getattr(node, "node_instance_id", node.node_id)))
        task_capacity = max(1, int(getattr(node, "capacity", 0) or 0))
        task_running = max(0, int(getattr(getattr(node, "metrics", None), "running", 0) or 0))
        task_queued = max(0, int(getattr(getattr(node, "metrics", None), "queued", 0) or 0))
        task_credit = max(0, int(getattr(getattr(node, "metrics", None), "credit", 0) or 0))
        task_used = min(task_capacity, task_running)
        task_free = max(0, task_capacity - task_used)
        node_quota = (
            f"task {task_used}/{task_capacity}"
            f" <span class='muted'>free {task_free}</span><br>"
            f"queue {task_queued}/{node.queue_capacity}"
            f" <span class='muted'>credit {task_credit}</span>"
        )
        proc_quota = (
            f"svc {node.service_worker_used}/{node.service_worker_capacity}"
            f" <span class='muted'>free {node.service_worker_available()}</span><br>"
            f"pool {node.task_pool_worker_used}/{node.task_pool_worker_capacity}"
            f" <span class='muted'>free {node.task_pool_worker_available()}</span>"
        )
        metadata = dict(node.metadata or {})
        deploy_reason = str(metadata.get("deploy_health_reason", "") or "")
        if not deploy_reason:
            deploy_reason = "accepting service deploy" if bool(getattr(node, "accept_service_deploy", True)) else "service deploy disabled"
        node_rows.append(
            "<tr>"
            f"<td>{html.escape(node.node_id)}</td>"
            f"<td>{html.escape(getattr(node, 'node_instance_id', '-') or '-')}</td>"
            f"<td>{html.escape(node.control_addr)}</td>"
            f"<td>{_ops_bool_badge(node.healthy)}</td>"
            f"<td>{_ops_bool_badge(node.schedulable)}</td>"
            f"<td class='quota-cell'>{node_quota}</td>"
            f"<td class='quota-cell'>{proc_quota}</td>"
            f"<td>{_ops_bool_badge(getattr(node, 'accept_service_deploy', True))}</td>"
            f"<td>{html.escape(deploy_reason or '-')}</td>"
            f"<td>{_ops_bool_badge(node.drain, invert=True)}</td>"
            f"<td>{_ops_bool_badge(getattr(node, 'profile_enabled', True))}</td>"
            f"<td>{html.escape(str((node.metadata or {}).get('pycloud_version', '-') or '-'))}</td>"
            f"<td>{html.escape(node.python_version or '-')}</td>"
            f"<td>{html.escape(active_runtimes)}</td>"
            f"<td>{html.escape(effective_tags)}</td>"
            f"<td>{html.escape(managed_tags)}</td>"
            f"<td>{html.escape(capability_tags)}</td>"
            f"<td>{html.escape(legacy_node_tags)}</td>"
            f"<td>{task_capacity}</td>"
            f"<td>{task_used}</td>"
            f"<td>{task_free}</td>"
            f"<td>{task_queued}</td>"
            f"<td>{task_running}</td>"
            f"<td>{node.service_worker_capacity}</td>"
            f"<td>{node.service_worker_used}</td>"
            f"<td>{node.service_worker_available()}</td>"
            f"<td>{node.task_pool_worker_capacity}</td>"
            f"<td>{node.task_pool_worker_used}</td>"
            f"<td>{node.task_pool_worker_available()}</td>"
            f"<td>{html.escape(node.reason or '')}</td>"
            f"<td>{html.escape(profile_notes or '-')}</td>"
            "<td>"
            f"<form method='post' action='/ops/nodes/{action_node_id}/cordon' style='display:inline'><button type='submit'>cordon</button></form> "
            f"<form method='post' action='/ops/nodes/{action_node_id}/uncordon' style='display:inline'><button type='submit'>uncordon</button></form> "
            f"<form method='post' action='/ops/nodes/{action_node_id}/drain' style='display:inline'><button type='submit'>drain</button></form> "
            f"<form method='post' action='/ops/nodes/{action_node_id}/undrain' style='display:inline'><button type='submit'>undrain</button></form> "
            f"<form method='post' action='/ops/nodes/{action_node_id}/disable' style='display:inline'><button type='submit'>disable</button></form> "
            f"<form method='post' action='/ops/nodes/{action_node_id}/enable' style='display:inline'><button type='submit'>enable</button></form>"
            f"<form method='post' action='/ops/nodes/{action_node_id}/managed-tags' style='margin-top:4px'>"
            f"<input name='tag' placeholder='managed tag' size='14'>"
            "<button type='submit' name='op' value='add'>add</button>"
            "<button type='submit' name='op' value='remove'>remove</button></form>"
            f"<form method='post' action='/ops/nodes/{action_node_id}/managed-tags' style='margin-top:4px'>"
            "<select name='tag'>"
            + "".join(f"<option value='{html.escape(str(tag))}'>{html.escape(str(tag))}</option>" for tag in (getattr(node, "capability_tags", []) or []))
            + "</select><button type='submit' name='op' value='add'>add capability tag</button></form>"
            f"<form method='post' action='/ops/nodes/{action_node_id}/notes' style='margin-top:4px'>"
            f"<input name='notes' value='{html.escape(profile_notes, quote=True)}' placeholder='notes' size='18'>"
            "<button type='submit'>save</button></form>"
            f"<div class='muted'>profile={html.escape(profile_key or '-')}</div>"
            "</td>"
            "</tr>"
        )
        for item in merged_services:
            key = (str(item.get("service_name", "") or ""), _service_endpoint_key(str(item.get("http_base_url", "") or "")))
            service_entries.setdefault(key, []).append(
                {
                    "node": node,
                    "node_healthy": node_healthy,
                    "item": item,
                    "timing": timing_map.get(str(item["service_id"]), {}),
                }
            )
        for pool in task_pools:
            stale_row = "" if node_healthy else " class='stale-row'"
            timing = pool_timing_map.get(str(pool.pool_id), {})
            pool_alive_workers = int(getattr(pool, "alive_workers", 0) if node_healthy else 0)
            pool_stop_reason = str(getattr(pool, "stop_reason", "") or getattr(pool, "failure_reason", "") or "")
            pool_resource_health = str(getattr(pool, "resource_health", "") or "") or _task_pool_resource_health_text(
                node_healthy=node_healthy,
                status=getattr(pool, "status", ""),
                alive_workers=pool_alive_workers,
                stop_reason=pool_stop_reason,
                degraded=getattr(pool, "degraded", False),
            )
            pool_entries.append((
                getattr(pool, "created_at", None),
                f"<tr{stale_row}>"
                f"<td>{html.escape(node.node_id)}</td>"
                f"<td>{html.escape(getattr(node, 'node_instance_id', '-') or '-')}</td>"
                f"<td>{html.escape(pool.pool_name)}</td>"
                f"<td>{html.escape(pool.pool_id)}</td>"
                f"<td>{html.escape(pool.owner_client_id)}</td>"
                f"<td>{_ops_status_badge(pool.status)}</td>"
                f"<td>{_ops_status_badge(pool_resource_health)}</td>"
                f"<td>{pool.worker_count}</td>"
                f"<td>{pool_alive_workers}</td>"
                f"<td>{pool.task_count}</td>"
                f"<td>{int(getattr(pool, 'inflight', 0) or 0)}</td>"
                f"<td>{html.escape(str(timing.get('call_count', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('error_count', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_total_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_child_decode_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_child_invoke_ms', timing.get('avg_invoke_ms', '-'))))}</td>"
                f"<td>{html.escape(str(timing.get('avg_child_encode_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_executor_create_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_warmup_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('executor_rebuild_count', '-')))}</td>"
                f"<td>{html.escape(pool.code_version[:20] + ('...' if len(pool.code_version) > 20 else ''))}</td>"
                f"<td>{html.escape(_dt_text(pool.created_at))}</td>"
                f"<td>{html.escape(_dt_text(pool.last_heartbeat_at))}</td>"
                f"<td>{html.escape(_dt_text(pool.lease_expire_at))}</td>"
                f"<td>{html.escape(_failure_text_with_time(pool_stop_reason, getattr(pool, 'failure_at', None)))}</td>"
                "</tr>"
            ))
        component = str(metadata.get("component", "") or "").strip()
        if component == "job-orchestrator":
            job_service = next((item["primary"] for item in merged_services if str(item["service_name"]) == "job-orchestrator"), None)
            job_waiting = str(metadata.get("job_waiting", "0") or "0")
            job_running = str(metadata.get("job_running", "0") or "0")
            job_terminal = str(metadata.get("job_terminal", "0") or "0")
            job_queue_rows.append(
                "<tr>"
                f"<td>{html.escape(node.node_id)}</td>"
                f"<td>{html.escape(getattr(node, 'node_instance_id', '-') or '-')}</td>"
                f"<td>{_ops_bool_badge(node_healthy)}</td>"
                f"<td>{html.escape(str(metadata.get('pycloud_version', '-') or '-'))}</td>"
                f"<td>{html.escape(str(metadata.get('current_job_id', '') or '-'))}</td>"
                f"<td>{_ops_status_badge(str(metadata.get('current_job_status', '') or '-'))}</td>"
                "<td>-</td>"
                "<td>-</td>"
                "<td>-</td>"
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
                        f"<td>{_ops_status_badge(str(item.get('status', '') or '-'))}</td>"
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
    for (_service_name, _endpoint), entries in sorted(service_entries.items(), key=lambda item: item[0]):
        ordered = sorted(
            entries,
            key=lambda entry: (
                bool(entry["node_healthy"]),
                int(entry["item"].get("status", 0) == pb2.SERVICE_STATUS_RUNNING),
                getattr(entry["item"].get("primary"), "lease_expire_at", utc_now()),
                int(entry["item"].get("alive_workers", 0) or 0),
            ),
            reverse=True,
        )
        primary = ordered[0]
        item = primary["item"]
        timing = dict(primary["timing"] or {})
        any_healthy = any(bool(entry["node_healthy"]) for entry in ordered)
        node_ids = sorted({str(entry["node"].node_id or "") for entry in ordered if str(entry["node"].node_id or "")})
        instance_ids = sorted(
            {
                str(getattr(entry["node"], "node_instance_id", "") or "")
                for entry in ordered
                if str(getattr(entry["node"], "node_instance_id", "") or "")
            }
        )
        service_ids = []
        for entry in ordered:
            service_ids.extend(str(value) for value in (entry["item"].get("service_ids") or ()) if str(value))
        unique_service_ids = sorted(set(service_ids))
        service_id_text = html.escape(str(item["service_id"]) or "-")
        duplicate_count = len(unique_service_ids) or len(ordered)
        if duplicate_count > 1:
            service_id_text = f"{service_id_text} (+{duplicate_count - 1})"
        stop_reasons = [
            _failure_text_with_time(
                entry["item"].get("stop_reason", ""),
                entry["item"].get("failure_at"),
            )
            for entry in ordered
            if str(entry["item"].get("stop_reason", "") or "").strip()
        ]
        stop_reason = "; ".join(sorted({text for text in stop_reasons if text and text != "-"}))
        max_alive_workers = max(int(entry["item"].get("alive_workers", 0) or 0) for entry in ordered) if any_healthy else 0
        resource_health = str(item.get("resource_health", "") or "") or _service_resource_health_text(
            node_healthy=any_healthy,
            service_status=int(item["status"]),
            alive_workers=max_alive_workers,
            stop_reason=stop_reason,
        )
        stale_row = "" if any_healthy else " class='stale-row'"
        service_rows.append(
            f"<tr{stale_row}>"
            f"<td>{html.escape(', '.join(node_ids) or '-')}</td>"
            f"<td>{html.escape(', '.join(instance_ids) or '-')}</td>"
            f"<td>{html.escape(str(item['service_name']))}</td>"
            f"<td>{service_id_text}</td>"
            f"<td>{_ops_bool_badge(any_healthy)}</td>"
            f"<td>{_ops_status_badge(_effective_service_status_text(node_healthy=any_healthy, service_status=int(item['status'])))}</td>"
            f"<td>{_ops_status_badge(resource_health)}</td>"
            f"<td>{max(int(entry['item'].get('worker_count', 0) or 0) for entry in ordered)}</td>"
            f"<td>{max_alive_workers}</td>"
            f"<td>{max(int(entry['item'].get('in_flight', 0) or 0) for entry in ordered) if any_healthy else 0}</td>"
            f"<td>{html.escape(str(timing.get('call_count', '-')))}</td>"
            f"<td>{html.escape(str(timing.get('error_count', '-')))}</td>"
            f"<td>{html.escape(str(timing.get('avg_total_ms', '-')))}</td>"
            f"<td>{html.escape(str(timing.get('avg_child_decode_ms', '-')))}</td>"
            f"<td>{html.escape(str(timing.get('avg_child_invoke_ms', timing.get('avg_invoke_ms', '-'))))}</td>"
            f"<td>{html.escape(str(timing.get('avg_child_encode_ms', '-')))}</td>"
            f"<td>{html.escape(_dt_text(max(entry['item'].get('lease_expire_at', utc_now()) for entry in ordered)))}</td>"
            f"<td>{html.escape(stop_reason or '-')}</td>"
            f"<td>{html.escape(str(item['http_base_url']) or '-')}</td>"
            "</tr>"
        )
    node_body = "\n".join(node_rows) or "<tr><td colspan='31'>no nodes</td></tr>"
    service_body = "\n".join(service_rows) or "<tr><td colspan='19'>no services</td></tr>"
    pool_entries.sort(key=lambda item: item[0], reverse=True)
    pool_rows = [row for _created_at, row in pool_entries]
    pool_body = "\n".join(pool_rows) or "<tr><td colspan='23'>no task pools</td></tr>"
    job_queue_body = "\n".join(job_queue_rows) or "<tr><td colspan='11'>no job queues</td></tr>"
    recent_job_rows.sort(key=lambda item: item[0], reverse=True)
    recent_job_body = "\n".join(row for _sort_key, row in recent_job_rows) or "<tr><td colspan='8'>no recent jobs</td></tr>"
    waiting_job_rows.sort(key=lambda item: item[0])
    waiting_job_body = "\n".join(row for _sort_key, row in waiting_job_rows) or "<tr><td colspan='7'>no waiting jobs</td></tr>"
    job_timing_body = (
        f"<tr><td>embedded-job-orch</td><td>{html.escape(str(queue_timing.get('job_count', '-')))}</td><td>{html.escape(str(queue_timing.get('avg_queue_wait_ms', '-')))}</td><td>{html.escape(str(queue_timing.get('avg_pool_prepare_ms', '-')))}</td><td>{html.escape(str(queue_timing.get('avg_fanout_globals_ms', '-')))}</td><td>{html.escape(str(queue_timing.get('avg_running_tasks_ms', '-')))}</td><td>{html.escape(str(queue_timing.get('avg_finalize_ms', '-')))}</td><td>{html.escape(str(queue_timing.get('avg_terminal_writeback_ms', '-')))}</td><td>{html.escape(str(queue_timing.get('avg_total_ms', '-')))}</td><td>{html.escape(str(queue_timing.get('max_total_ms', '-')))}</td><td>{html.escape(str(queue_timing.get('executor_create_count', '-')))}</td><td>{html.escape(str(queue_timing.get('executor_rebuild_count', '-')))}</td><td>{html.escape(str(queue_timing.get('pool_reuse_count', '-')))}</td><td>{html.escape(str(queue_timing.get('pool_create_count', '-')))}</td><td>{html.escape(str(queue_timing.get('pool_rebuild_count', '-')))}</td><td>{html.escape(str(queue_timing.get('avg_first_result_wait_ms', '-')))}</td><td>{html.escape(str(queue_timing.get('avg_warmup_ms', '-')))}</td></tr>"
        f"<tr><td>current-job</td><td>1</td><td>{html.escape(str(current_job_timing.get('queue_wait_ms', '-')))}</td><td>{html.escape(str(current_job_timing.get('pool_prepare_ms', '-')))}</td><td>{html.escape(str(current_job_timing.get('fanout_globals_ms', '-')))}</td><td>{html.escape(str(current_job_timing.get('running_tasks_ms', '-')))}</td><td>{html.escape(str(current_job_timing.get('finalize_ms', '-')))}</td><td>{html.escape(str(current_job_timing.get('terminal_writeback_ms', '-')))}</td><td>{html.escape(str(current_job_timing.get('total_ms', '-')))}</td><td>{html.escape(str(current_job_timing.get('total_ms', '-')))}</td><td>{html.escape(str(current_job_timing.get('executor_create_count', '-')))}</td><td>{html.escape(str(current_job_timing.get('executor_rebuild_count', '-')))}</td><td>{html.escape(str(current_job_timing.get('pool_reuse_count', '-')))}</td><td>{html.escape(str(1 if current_job_timing.get('pool_action', '') == 'create' else 0))}</td><td>{html.escape(str(1 if current_job_timing.get('pool_action', '') == 'rebuild' else 0))}</td><td>{html.escape(str(current_job_timing.get('first_result_wait_ms', '-')))}</td><td>{html.escape(str(current_job_timing.get('warmup_ms', '-')))}</td></tr>"
    )
    healthy_nodes = sum(1 for node in nodes if bool(getattr(node, "healthy", False)))
    total_nodes = len(nodes)
    total_services = len(service_entries)
    running_services = sum(
        1
        for rows in service_entries.values()
        if any(bool(entry["node_healthy"]) and int(entry["item"].get("status", 0) or 0) == int(pb2.SERVICE_STATUS_RUNNING) for entry in rows)
    )
    total_pools = len(pool_entries)
    pool_inflight = sum(
        int(getattr(pool, "inflight", 0) or 0)
        for node in nodes
        for pool in getattr(node, "task_pools", {}).values()
    )
    total_waiting_jobs = len(waiting_job_rows)
    total_recent_jobs = len(recent_job_rows)
    overview = (
        "<div class='metrics-grid' id='ops-overview'>"
        f"{_ops_metric_card('Nodes', f'{healthy_nodes}/{total_nodes}', 'healthy / total')}"
        f"{_ops_metric_card('Services', f'{running_services}/{total_services}', 'routable / known')}"
        f"{_ops_metric_card('Task Pools', total_pools, f'in-flight {pool_inflight}')}"
        f"{_ops_metric_card('Jobs', total_waiting_jobs, f'waiting, {total_recent_jobs} recent')}"
        "</div>"
    )
    content_key = _ops_snapshot_content_key(nodes, job_summary=job_summary)
    if _snapshot_only:
        return {
            "ok": True,
            "content_key": content_key,
            "fragments": {
                "ops-nodes-body": node_body,
                "ops-job-queue-body": job_queue_body,
                "ops-job-timing-body": job_timing_body,
                "ops-recent-jobs-body": recent_job_body,
                "ops-waiting-jobs-body": waiting_job_body,
                "ops-services-body": service_body,
                "ops-pools-body": pool_body,
            },
            "metrics": {
                "nodes": f"{healthy_nodes}/{total_nodes}",
                "services": f"{running_services}/{total_services}",
                "task_pools": str(total_pools),
                "pool_inflight": str(pool_inflight),
                "jobs": str(total_waiting_jobs),
                "recent_jobs": str(total_recent_jobs),
            },
            "controlplane_version": _pycloud_version(),
            "auto_refresh_sec": 5,
        }
    node_headers = [
        "node_id", "instance_id", "control_addr", "healthy", "schedulable", "node quota", "proc quota", "accept deploy", "deploy reason", "drain", "enabled", "pycloud",
        "python", "active runtimes", "effective tags", "managed tags", "capability tags", "legacy node tags", "task cap",
        "task used", "task free", "queued", "running", "svc cap",
        "svc used", "svc avail", "pool cap", "pool used", "pool avail", "reason", "notes", "actions",
    ]
    job_queue_headers = [
        "owner", "instance_id", "healthy", "pycloud", "current_job_id", "current_status", "current_phase", "pool_action",
        "current_total_ms", "waiting", "running", "terminal", "job_count", "http_base_url",
    ]
    recent_job_headers = ["owner", "instance_id", "job_id", "status", "submitted_at", "finished_at", "final_result", "error"]
    waiting_job_headers = ["owner", "instance_id", "job_id", "priority", "submitted_at", "position", "actions"]
    service_headers = [
        "node_id", "instance_id", "service_name", "service_id", "node_healthy", "status", "resource", "workers", "alive", "in_flight",
        "calls", "errors", "avg_total_ms", "avg_child_decode_ms", "avg_child_invoke_ms", "avg_child_encode_ms",
        "lease_expire_at", "failure_reason", "http_base_url",
    ]
    pool_headers = [
        "node_id", "instance_id", "pool_name", "pool_id", "owner_client_id", "status", "resource", "workers", "alive", "tasks", "in_flight",
        "calls", "errors", "avg_total_ms", "avg_child_decode_ms", "avg_child_invoke_ms", "avg_child_encode_ms",
        "last_executor_create_ms", "avg_warmup_ms", "executor_rebuild_count", "code_version", "created_at",
        "last_heartbeat_at", "lease_expire_at", "failure_reason",
    ]
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<title>InfoCenter Ops</title>"
        "<style>"
        ":root{color-scheme:dark;--bg:#07111f;--bg2:#0b1530;--panel:#101b31;--panel2:#0d1729;--line:#263957;--line-soft:#1e2d47;--text:#e5edf7;--muted:#93a4bd;--head:#16243c;--good:#4ade80;--good-bg:#052e1a;--warn:#fbbf24;--warn-bg:#3b2a05;--bad:#fb7185;--bad-bg:#3b1018;--neutral:#cbd5e1;--neutral-bg:#263244;--accent:#60a5fa;--accent2:#22d3ee;--shadow:0 18px 50px rgba(0,0,0,.32);}"
        "*{box-sizing:border-box;}html{scroll-behavior:smooth;}body{font-family:Inter,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:0;min-height:100vh;background:radial-gradient(circle at 12% -10%,rgba(34,211,238,.22),transparent 30%),radial-gradient(circle at 88% 0,rgba(96,165,250,.18),transparent 28%),linear-gradient(180deg,var(--bg),#050a13 80%);color:var(--text);}"
        "body:before{content:'';position:fixed;inset:0;pointer-events:none;background-image:linear-gradient(rgba(255,255,255,.025) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.02) 1px,transparent 1px);background-size:28px 28px;mask-image:linear-gradient(#000,transparent 75%);}"
        ".ops-shell{max-width:1760px;margin:0 auto;padding:30px 26px 44px;position:relative;} .topbar{display:flex;align-items:flex-start;justify-content:space-between;gap:18px;margin-bottom:18px;padding:20px;border:1px solid rgba(96,165,250,.24);border-radius:22px;background:linear-gradient(135deg,rgba(16,27,49,.92),rgba(13,23,41,.72));box-shadow:var(--shadow);backdrop-filter:blur(10px);}"
        ".eyebrow{color:var(--accent2);font-size:12px;font-weight:800;text-transform:uppercase;letter-spacing:.14em;margin-bottom:8px;}h1{font-size:32px;line-height:1.08;margin:0 0 8px;font-weight:820;letter-spacing:-.03em;}h2{font-size:18px;margin:0;font-weight:760;letter-spacing:-.01em;}"
        ".muted{color:var(--muted);} .section-note{color:var(--muted);font-size:12px;line-height:1.55;margin:8px 0 12px;}.hero-sub{max-width:860px;color:#b6c4d8;font-size:13px;line-height:1.55;}"
        ".top-actions{display:flex;align-items:flex-end;gap:10px;flex-wrap:wrap;justify-content:flex-end;}.nav-pill,.refresh-pill{white-space:nowrap;border:1px solid rgba(148,163,184,.22);background:rgba(15,23,42,.72);border-radius:999px;padding:8px 11px;font-size:12px;color:#cbd5e1;text-decoration:none;box-shadow:0 1px 0 rgba(255,255,255,.05) inset;}.nav-pill:hover{border-color:rgba(96,165,250,.65);color:#fff;background:rgba(37,99,235,.22);}"
        ".metrics-grid{display:grid;grid-template-columns:repeat(4,minmax(170px,1fr));gap:14px;margin:18px 0 22px;}.metric-card{position:relative;overflow:hidden;background:linear-gradient(180deg,rgba(16,27,49,.96),rgba(10,18,32,.96));border:1px solid rgba(148,163,184,.18);border-radius:18px;padding:16px 18px;box-shadow:0 14px 34px rgba(0,0,0,.24);}.metric-glow{position:absolute;right:-34px;top:-34px;width:120px;height:120px;background:radial-gradient(circle,rgba(96,165,250,.24),transparent 70%);}.metric-label{position:relative;font-size:11px;color:var(--muted);font-weight:800;text-transform:uppercase;letter-spacing:.09em;}.metric-value{position:relative;font-size:31px;line-height:1.05;margin-top:9px;font-weight:840;letter-spacing:-.03em;}.metric-sub{position:relative;font-size:12px;color:var(--muted);margin-top:7px;}"
        ".ops-section{margin:20px 0 24px;}.section-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin:0 0 8px;}.section-anchor{color:var(--muted);font-size:12px;text-decoration:none;border:1px solid var(--line);border-radius:999px;padding:4px 9px;background:rgba(15,23,42,.72);}.section-anchor:hover{color:#fff;border-color:var(--accent);}"
        ".table-wrap{overflow:auto;max-height:70vh;background:rgba(16,27,49,.9);border:1px solid rgba(148,163,184,.2);border-radius:16px;box-shadow:0 16px 42px rgba(0,0,0,.26);}table{border-collapse:separate;border-spacing:0;width:100%;min-width:1120px;}th,td{border-right:1px solid var(--line-soft);border-bottom:1px solid var(--line-soft);padding:9px 11px;font-size:12px;line-height:1.42;vertical-align:top;word-break:break-word;overflow-wrap:anywhere;white-space:normal;}th{position:sticky;top:0;z-index:2;background:linear-gradient(180deg,#1a2a46,#142138);text-align:left;color:#c9d7eb;font-weight:780;text-transform:uppercase;letter-spacing:.035em;font-size:11px;}tr:last-child td{border-bottom:0;}td:last-child,th:last-child{border-right:0;}tbody tr:nth-child(even){background:rgba(255,255,255,.018);}tbody tr:hover{background:rgba(96,165,250,.09);}"
        ".quota-cell{white-space:nowrap;line-height:1.45;}"
        "body:not(.show-details) .ops-table--nodes :is(th,td):nth-child(2),body:not(.show-details) .ops-table--nodes :is(th,td):nth-child(3),body:not(.show-details) .ops-table--nodes :is(th,td):nth-child(8),body:not(.show-details) .ops-table--nodes :is(th,td):nth-child(n+11):nth-child(-n+29),body:not(.show-details) .ops-table--nodes :is(th,td):nth-child(31){display:none;}"
        "body:not(.show-details) .ops-table--job-queue :is(th,td):nth-child(2),body:not(.show-details) .ops-table--job-queue :is(th,td):nth-child(4),body:not(.show-details) .ops-table--job-queue :is(th,td):nth-child(14){display:none;}"
        "body:not(.show-details) .ops-table--job-timing :is(th,td):nth-child(4),body:not(.show-details) .ops-table--job-timing :is(th,td):nth-child(5),body:not(.show-details) .ops-table--job-timing :is(th,td):nth-child(7),body:not(.show-details) .ops-table--job-timing :is(th,td):nth-child(8),body:not(.show-details) .ops-table--job-timing :is(th,td):nth-child(n+11):nth-child(-n+15){display:none;}"
        "body:not(.show-details) .ops-table--recent-jobs :is(th,td):nth-child(2),body:not(.show-details) .ops-table--recent-jobs :is(th,td):nth-child(7),body:not(.show-details) .ops-table--recent-jobs :is(th,td):nth-child(8){display:none;}"
        "body:not(.show-details) .ops-table--waiting-jobs :is(th,td):nth-child(2){display:none;}"
        "body:not(.show-details) .ops-table--services :is(th,td):nth-child(2),body:not(.show-details) .ops-table--services :is(th,td):nth-child(4),body:not(.show-details) .ops-table--services :is(th,td):nth-child(n+14):nth-child(-n+17),body:not(.show-details) .ops-table--services :is(th,td):nth-child(19){display:none;}"
        "body:not(.show-details) .ops-table--pools :is(th,td):nth-child(2),body:not(.show-details) .ops-table--pools :is(th,td):nth-child(4),body:not(.show-details) .ops-table--pools :is(th,td):nth-child(5),body:not(.show-details) .ops-table--pools :is(th,td):nth-child(n+15):nth-child(-n+24){display:none;}"
        "body:not(.show-details) .ops-table{min-width:900px;}body.show-details .ops-table{min-width:1120px;}.density-toggle{border-color:rgba(34,211,238,.35);color:#cffafe;background:rgba(8,47,73,.5);}.density-toggle:hover{border-color:rgba(34,211,238,.72);background:rgba(14,116,144,.36);}.density-hint{color:#8fb2d9;font-size:12px;width:100%;text-align:right;}"
        ".badge{display:inline-flex;align-items:center;min-height:21px;border-radius:999px;padding:3px 9px;font-size:11px;font-weight:800;line-height:1.2;border:1px solid transparent;white-space:nowrap;box-shadow:0 1px 0 rgba(255,255,255,.06) inset;}.badge-good{background:var(--good-bg);color:var(--good);border-color:rgba(74,222,128,.2);}.badge-warn{background:var(--warn-bg);color:var(--warn);border-color:rgba(251,191,36,.22);}.badge-bad{background:var(--bad-bg);color:var(--bad);border-color:rgba(251,113,133,.22);}.badge-neutral{background:var(--neutral-bg);color:var(--neutral);border-color:rgba(203,213,225,.14);}"
        ".stale-row{background:rgba(127,29,29,.24)!important;color:#fecaca;}form{margin:0;}td form{display:inline-flex;align-items:center;gap:4px;flex-wrap:wrap;}button{appearance:none;border:1px solid rgba(148,163,184,.28);border-radius:9px;background:rgba(15,23,42,.9);color:#dbeafe;padding:5px 9px;margin:2px;font:inherit;font-size:12px;cursor:pointer;transition:.12s ease;}button:hover{transform:translateY(-1px);border-color:rgba(96,165,250,.72);color:#fff;background:rgba(37,99,235,.3);}input,select{border:1px solid rgba(148,163,184,.28);border-radius:9px;padding:5px 7px;font:inherit;font-size:12px;background:#08111f;color:var(--text);max-width:190px;}input::placeholder{color:#64748b;}"
        "a{color:#93c5fd;}code{color:#67e8f9;}::-webkit-scrollbar{height:12px;width:12px;}::-webkit-scrollbar-track{background:#07111f;}::-webkit-scrollbar-thumb{background:#263957;border-radius:999px;border:3px solid #07111f;}::-webkit-scrollbar-thumb:hover{background:#3b5278;}"
        "@media(max-width:1050px){.topbar{display:block}.top-actions{justify-content:flex-start;margin-top:14px}.metrics-grid{grid-template-columns:repeat(2,minmax(0,1fr));}.metric-value{font-size:25px;}}"
        "@media(max-width:620px){.ops-shell{padding:16px}.topbar{padding:16px;border-radius:16px}.metrics-grid{grid-template-columns:1fr;}h1{font-size:27px;}th,td{font-size:11px;padding:7px 8px;}}"
        "</style>"
        "</head><body>"
        "<div class='ops-shell'>"
        "<div class='topbar'><div>"
        "<div class='eyebrow'>PyCloud Parallel Control Plane</div>"
        "<h1>InfoCenter Ops</h1>"
        "<div class='hero-sub'>Live view of nodes, services, job queues and task pools. "
        "Tables auto-refresh every 5 seconds while preserving the page chrome.</div>"
        f"<div class='section-note'>controlplane_version={html.escape(_pycloud_version())}</div>"
        "</div><div class='top-actions'>"
        "<a class='nav-pill' href='/nodes?healthy_only=false&limit=500'>nodes json</a>"
        "<a class='nav-pill' href='/services/routes?healthy_only=false&limit=500'>services json</a>"
        "<a class='nav-pill' href='/ops/snapshot'>snapshot</a>"
        "<button type='button' class='density-toggle' id='ops-density-toggle'>show details</button>"
        "<div class='refresh-pill' id='ops-refresh-status'>auto_refresh_sec=5 mode=partial</div>"
        "<div class='density-hint' id='ops-density-hint'>compact node view shows node/process quota and hides raw capacity columns</div>"
        "</div></div>"
        f"{overview}"
        "<div class='section-note'>Node table focuses on health, task queue capacity, and service/task-pool process quota. "
        "Service details and task-pool task pressure live in the tables below, so node rows stay compact. "
        "Rows for stale nodes are highlighted and rendered as LOST.</div>"
        f"{_ops_table('Nodes', '', node_headers, 'ops-nodes-body', node_body)}"
        f"{_ops_table('Job Queue', 'Shows embedded controlplane job queue state and any standalone `job-orchestrator` processes registered via InfoCenter metadata.', job_queue_headers, 'ops-job-queue-body', job_queue_body)}"
        "<section class='ops-section'><div class='section-note'>Job-orch timing is reduced timing for queue wait, pool prepare, globals fanout, task running, finalize, writeback and total. Windows-focused fields highlight executor create/rebuild, warmup, and first-result wait.</div>"
        "<div class='table-wrap'><table class='ops-table ops-table--job-timing'><thead><tr>"
        "<th>scope</th><th>job_count</th><th>avg_queue_wait_ms</th><th>avg_pool_prepare_ms</th><th>avg_fanout_globals_ms</th><th>avg_running_tasks_ms</th><th>avg_finalize_ms</th><th>avg_terminal_writeback_ms</th><th>avg_total_ms</th><th>max_total_ms</th><th>executor_create_count</th><th>executor_rebuild_count</th><th>pool_reuse_count</th><th>pool_create_count</th><th>pool_rebuild_count</th><th>avg_first_result_wait_ms</th><th>avg_warmup_ms</th></tr></thead><tbody id='ops-job-timing-body'>"
        f"{job_timing_body}"
        "</tbody></table></div></section>"
        f"{_ops_table('Recent Jobs', '', recent_job_headers, 'ops-recent-jobs-body', recent_job_body)}"
        f"{_ops_table('Waiting Jobs', 'Only waiting jobs can be reordered. Running jobs keep their current slot.', waiting_job_headers, 'ops-waiting-jobs-body', waiting_job_body)}"
        f"{_ops_table('Service Instances', '', service_headers, 'ops-services-body', service_body)}"
        f"{_ops_table('Task Pools', '', pool_headers, 'ops-pools-body', pool_body)}"
        "<script>"
        "(function(){"
        "const densityKey='pycloud.ops.showDetails';"
        "const densityBtn=document.getElementById('ops-density-toggle');"
        "const densityHint=document.getElementById('ops-density-hint');"
        "function getDensity(){try{return localStorage.getItem(densityKey)==='1';}catch(_err){return false;}}"
        "function setDensity(show){try{localStorage.setItem(densityKey,show?'1':'0');}catch(_err){}}"
        "function applyDensity(show){document.body.classList.toggle('show-details',!!show);if(densityBtn){densityBtn.textContent=show?'hide details':'show details';}if(densityHint){densityHint.textContent=show?'detail view shows diagnostic IDs, URLs and quota breakdown columns':'compact node view shows quota/usage and hides service/task details';}}"
        "applyDensity(getDensity());"
        "if(densityBtn){densityBtn.addEventListener('click',function(){const show=!document.body.classList.contains('show-details');setDensity(show);applyDensity(show);});}"
        "const ids=['ops-nodes-body','ops-job-queue-body','ops-job-timing-body','ops-recent-jobs-body','ops-waiting-jobs-body','ops-services-body','ops-pools-body'];"
        f"let lastOpsContentKey={json.dumps(content_key)};"
        "function card(label,value,sub){return '<div class=\"metric-card\"><div class=\"metric-glow\"></div><div class=\"metric-label\">'+label+'</div><div class=\"metric-value\">'+value+'</div><div class=\"metric-sub\">'+sub+'</div></div>';}"
        "function updateOverview(data){const el=document.getElementById('ops-overview');if(!el||!data.metrics){return;}const m=data.metrics;el.innerHTML=card('Nodes',m.nodes||'-','healthy / total')+card('Services',m.services||'-','routable / known')+card('Task Pools',m.task_pools||'-','in-flight '+(m.pool_inflight||0))+card('Jobs',m.jobs||'-','waiting, '+(m.recent_jobs||0)+' recent');}"
        "async function refreshOps(){"
        "const status=document.getElementById('ops-refresh-status');"
        "try{const resp=await fetch('/ops/snapshot',{cache:'no-store',headers:{'Accept':'application/json'}});"
        "if(!resp.ok){throw new Error('http '+resp.status);}"
        "const data=await resp.json();if(!data.ok){throw new Error(data.error||'snapshot failed');}"
        "if(data.content_key&&lastOpsContentKey===data.content_key){if(status){status.textContent='auto_refresh_sec=5 mode=partial heartbeat_ignored='+new Date().toLocaleTimeString();}return;}"
        "if(data.content_key){lastOpsContentKey=data.content_key;}"
        "updateOverview(data);const fragments=data.fragments||{};"
        "ids.forEach(function(id){const el=document.getElementById(id);if(el&&Object.prototype.hasOwnProperty.call(fragments,id)){el.innerHTML=fragments[id];}});"
        "if(status){status.textContent='auto_refresh_sec=5 mode=partial last_update='+new Date().toLocaleTimeString();}"
        "}catch(err){if(status){status.textContent='auto_refresh_sec=5 mode=partial refresh_error='+(err&&err.message?err.message:err);}}"
        "}"
        "window.setInterval(refreshOps,5000);"
        "})();"
        "</script></div></body></html>"
    )


def _render_ops_snapshot(state: InfoCenterState, job_queue: Optional[JobQueueManager] = None) -> Dict[str, object]:
    return _render_ops_page(state, job_queue, _snapshot_only=True)


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
        self.data_plane_app = DataPlaneHttpApp(
            target="",
            resolver=lambda ref: _resolve_data_ref_record_for_data_plane(self.state, ref),
        )
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
        data_plane_app = self.data_plane_app
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
                    node_instance_id = str(payload.get("node_instance_id", "")).strip() or str(payload.get("node_id", "")).strip()
                    if state.is_instance_fenced(node_instance_id):
                        fence_reason = state.fenced_instance_reason(node_instance_id)
                        self._send_json(
                            200,
                            {
                                "ok": False,
                                "accepted": False,
                                "reset_required": True,
                                "new_instance_required": True,
                                "lease_ttl_sec": state.lease_ttl_sec,
                                "reason": fence_reason or "node_instance_id fenced",
                                "error": "node_instance_id fenced",
                            },
                        )
                        return
                    control_addr = str(payload.get("control_addr", "")).strip()
                    conflicts = state.control_addr_conflicting_instances(
                        node_instance_id=node_instance_id,
                        control_addr=control_addr,
                    )
                    if conflicts and control_addr:
                        status_error = ""
                        try:
                            with NodeControlClient(control_addr, timeout_sec=0.35) as client:
                                status = client.node_status()
                        except Exception as exc:
                            status = {}
                            status_error = repr(exc)
                        actual_instance_id = str(status.get("node_instance_id", "") or "").strip()
                        if actual_instance_id and actual_instance_id != node_instance_id:
                            logger.warning(
                                "rejecting node registration because control_addr is served by another instance "
                                "control_addr=%s expected_node_instance_id=%s actual_node_instance_id=%s conflicts=%s",
                                control_addr,
                                node_instance_id,
                                actual_instance_id,
                                [str(getattr(item, "node_instance_id", "") or "") for item in conflicts],
                            )
                            self._send_json(
                                200,
                                {
                                    "ok": False,
                                    "accepted": False,
                                    "reset_required": True,
                                    "new_instance_required": True,
                                    "reason": (
                                        "node control_addr is still served by another node instance; "
                                        f"control_addr={control_addr} "
                                        f"expected_node_instance_id={node_instance_id} "
                                        f"actual_node_instance_id={actual_instance_id} "
                                        f"actual_node_id={str(status.get('node_id', '') or '-')}"
                                    ),
                                    "error": "node control_addr instance mismatch",
                                },
                            )
                            return
                        if not actual_instance_id:
                            logger.warning(
                                "rejecting node registration because control_addr conflicts and status probe "
                                "did not confirm replacement control_addr=%s expected_node_instance_id=%s "
                                "conflicts=%s status_error=%s",
                                control_addr,
                                node_instance_id,
                                [str(getattr(item, "node_instance_id", "") or "") for item in conflicts],
                                status_error or "empty node status",
                            )
                            self._send_json(
                                200,
                                {
                                    "ok": False,
                                    "accepted": False,
                                    "reset_required": False,
                                    "retryable": True,
                                    "lease_ttl_sec": state.lease_ttl_sec,
                                    "reason": (
                                        "node control_addr conflicts with an existing instance and status probe "
                                        "did not confirm this replacement; retry after the old NodeControl exits"
                                    ),
                                    "error": "node control_addr replacement not confirmed",
                                },
                            )
                            return
                    try:
                        node = state.register_node_record(
                            node_instance_id=node_instance_id,
                            node_id=str(payload.get("node_id", "")).strip(),
                            control_addr=control_addr,
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
                            accept_service_deploy=_coerce_bool(
                                payload.get("accept_service_deploy", (payload.get("metadata") or {}).get("accept_service_deploy")),
                                default=True,
                            ),
                            capability=_parse_node_capability(payload.get("capability")),
                        )
                    except ValueError as exc:
                        self._send_json(400, {"ok": False, "error": str(exc)})
                        return
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "accepted": True,
                            "reset_required": False,
                            "heartbeat_interval_sec": state.heartbeat_interval_sec,
                            "lease_ttl_sec": state.lease_ttl_sec,
                            "node": _serialize_node(node),
                        },
                    )
                    return
                if parsed.path == "/nodes/heartbeat":
                    payload = self._read_json()
                    if payload is None:
                        return
                    node_instance_id = str(payload.get("node_instance_id", "")).strip() or str(payload.get("node_id", "")).strip()
                    if state.is_instance_fenced(node_instance_id):
                        fence_reason = state.fenced_instance_reason(node_instance_id)
                        self._send_json(
                            200,
                            {
                                "ok": False,
                                "accepted": False,
                                "reset_required": True,
                                "new_instance_required": True,
                                "lease_ttl_sec": state.lease_ttl_sec,
                                "reason": fence_reason or "node_instance_id fenced",
                                "error": "node_instance_id fenced",
                            },
                        )
                        return
                    metrics_raw = payload.get("metrics") or {}
                    inventory_included = _coerce_bool(payload.get("inventory_included", True), default=True)
                    node = state.heartbeat_record(
                        node_instance_id=node_instance_id,
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
                        services=_parse_services(payload.get("services")) if inventory_included else None,
                        task_pools=_parse_task_pools(payload.get("task_pools")) if inventory_included else None,
                        python_version=str(payload.get("python_version", "") or ""),
                        active_runtimes=(
                            [str(x).strip() for x in (payload.get("active_runtimes") or []) if str(x).strip()]
                            if inventory_included
                            else None
                        ),
                        service_worker_capacity=max(0, int(payload.get("service_worker_capacity", 0) or 0)),
                        service_worker_used=max(0, int(payload.get("service_worker_used", 0) or 0)),
                        task_pool_worker_capacity=max(0, int(payload.get("task_pool_worker_capacity", 0) or 0)),
                        task_pool_worker_used=max(0, int(payload.get("task_pool_worker_used", 0) or 0)),
                        accept_service_deploy=_coerce_bool(
                            payload.get("accept_service_deploy", (payload.get("metadata") or {}).get("accept_service_deploy")),
                            default=True,
                        ),
                        capability=_parse_node_capability(payload.get("capability")),
                    )
                    if node is None:
                        self._send_json(404, {"ok": False, "error": "unknown node"})
                        return
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "accepted": True,
                            "next_heartbeat_in_sec": state.heartbeat_interval_sec,
                            "lease_ttl_sec": state.lease_ttl_sec,
                        },
                    )
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
                            with NodeControlClient(control_addr, timeout_sec=0.5) as client:
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
                                with NodeControlClient(control_addr, timeout_sec=0.5) as client:
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
                    body = self._read_body()
                    if body is None:
                        return
                    form = _parse_form_body(body)
                    try:
                        if action == "cordon":
                            state.update_node_profile_for_instance(node_instance_id, enabled=False)
                        elif action == "uncordon":
                            state.update_node_profile_for_instance(node_instance_id, enabled=True)
                        elif action == "disable":
                            state.update_node_profile_for_instance(node_instance_id, enabled=False)
                        elif action == "enable":
                            state.update_node_profile_for_instance(node_instance_id, enabled=True)
                        elif action == "drain":
                            state.update_node_profile_for_instance(node_instance_id, drain=True)
                        elif action == "undrain":
                            state.update_node_profile_for_instance(node_instance_id, drain=False)
                        elif action == "managed-tags":
                            tag = str(form.get("tag", "") or "").strip()
                            op = str(form.get("op", "add") or "add").strip().lower()
                            if op == "remove":
                                state.update_node_profile_for_instance(node_instance_id, remove_tags=[tag])
                            else:
                                state.update_node_profile_for_instance(node_instance_id, add_tags=[tag])
                        elif action == "notes":
                            state.update_node_profile_for_instance(node_instance_id, notes=str(form.get("notes", "") or ""))
                        elif action == "mark-lost":
                            reason = str(form.get("reason", "") or "").strip()
                            if not reason and body:
                                try:
                                    parsed_body = json.loads(body.decode("utf-8") or "{}")
                                except Exception:
                                    parsed_body = {}
                                if isinstance(parsed_body, dict):
                                    reason = str(parsed_body.get("reason", "") or "").strip()
                            state.mark_node_lost(node_instance_id, reason=reason or "marked lost via ops")
                        else:
                            self._send_json(404, {"ok": False, "error": "unknown ops action"})
                            return
                    except KeyError:
                        self._send_json(404, {"ok": False, "error": "node not found"})
                        return
                    except ValueError as exc:
                        self._send_json(400, {"ok": False, "error": str(exc)})
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
                            _reorder_job_via_http(
                                http_base_url,
                                job_id,
                                direction=direction,
                                auth_token=str(self.server_ref.auth_token or ""),
                            )
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
                        if len(handled) == 1 and isinstance(handled[0], StreamingHttpResponse):
                            self._send_stream(handled[0])
                        elif len(handled) == 4:
                            code, resp, content_type, extra_headers = handled
                            self._send_body(code, resp, content_type=content_type, extra_headers=extra_headers)
                        else:
                            code, resp = handled
                            self._send_json(code, resp)
                        return
                    body = self._read_body()
                    if body is None:
                        return
                    handled = gateway_app.handle_post(path=self.path, headers=self.headers, body=body)
                    if handled is not None:
                        if len(handled) == 1 and isinstance(handled[0], StreamingHttpResponse):
                            self._send_stream(handled[0])
                        elif len(handled) == 4:
                            code, resp, content_type, extra_headers = handled
                            self._send_body(code, resp, content_type=content_type, extra_headers=extra_headers)
                        else:
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
                handled_data = data_plane_app.handle_get(self.path)
                if handled_data is not None:
                    if isinstance(handled_data, StreamingHttpResponse):
                        self._send_stream(handled_data)
                    else:
                        code, headers, raw = handled_data
                        self._send_body(code, raw, content_type=str(headers.get("Content-Type", "application/octet-stream")), extra_headers=headers)
                    return
                if parsed.path == "/nodes":
                    qs = parse_qs(parsed.query)
                    tags = [x for x in ",".join(qs.get("tags", [])).split(",") if x]
                    node_ids = [x for x in ",".join(qs.get("node_ids", [])).split(",") if x]
                    node_instance_ids = [x for x in ",".join(qs.get("node_instance_ids", [])).split(",") if x]
                    healthy_only = str((qs.get("healthy_only", ["true"]) or ["true"])[0]).lower() not in ("0", "false", "no")
                    limit = max(1, int((qs.get("limit", ["100"]) or ["100"])[0]))
                    try:
                        nodes = [
                            _serialize_node(item)
                            for item in state.list_selected_nodes(
                                healthy_only=healthy_only,
                                tags=tags,
                                limit=limit,
                                node_ids=node_ids,
                                node_instance_ids=node_instance_ids,
                            )
                        ]
                    except (ValueError, RuntimeError) as exc:
                        self._send_json(400, {"ok": False, "error": str(exc)})
                        return
                    logger.info(
                        "[InfoCenter] GET /nodes healthy_only=%s tags=%s node_ids=%s node_instance_ids=%s limit=%d count=%d",
                        healthy_only,
                        tags,
                        node_ids,
                        node_instance_ids,
                        limit,
                        len(nodes),
                    )
                    self._send_json(200, {"ok": True, "nodes": nodes})
                    return
                if parsed.path == "/services/routes":
                    qs = parse_qs(parsed.query)
                    service_name = str((qs.get("service_name", [""]) or [""])[0])
                    healthy_only = str((qs.get("healthy_only", ["true"]) or ["true"])[0]).lower() not in ("0", "false", "no")
                    limit = max(1, int((qs.get("limit", ["500"]) or ["500"])[0]))
                    route_scope = str((qs.get("route_scope", ["call"]) or ["call"])[0] or "call")
                    routes = state.list_service_routes(
                        service_name=service_name,
                        healthy_only=healthy_only,
                        limit=limit,
                        route_scope=route_scope,
                    )
                    logger.info(
                        "[InfoCenter] GET /services/routes service_name=%s healthy_only=%s route_scope=%s limit=%d count=%d",
                        service_name,
                        healthy_only,
                        route_scope,
                        limit,
                        len(routes),
                    )
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
                if parsed.path == "/ops/snapshot":
                    if self._requires_auth(parsed.path):
                        if not self._check_auth():
                            self._send_json(401, {"ok": False, "error": "unauthorized"})
                            return
                    self._send_json(200, _render_ops_snapshot(state, self.server_ref.job_queue))
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
                return path.startswith("/nodes") or path.startswith("/jobs") or path.startswith("/ops") or path.startswith("/data")

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

            def _send_stream(self, response: StreamingHttpResponse) -> None:
                try:
                    self.send_response(int(response.status_code or 200))
                    self.send_header("Content-Type", str(response.content_type or "application/x-ndjson; charset=utf-8"))
                    self.send_header("Cache-Control", "no-cache")
                    self.send_header("Connection", "close")
                    for key, value in dict(response.extra_headers or {}).items():
                        if str(key).lower() == "content-type":
                            continue
                        self.send_header(str(key), str(value))
                    if int(response.content_length or 0) > 0:
                        self.send_header("Content-Length", str(int(response.content_length)))
                    self.end_headers()
                    for chunk in response.body_iter:
                        if not chunk:
                            continue
                        raw = chunk if isinstance(chunk, (bytes, bytearray)) else str(chunk).encode("utf-8")
                        self.wfile.write(raw)
                        self.wfile.flush()
                except Exception as exc:
                    if not _is_client_disconnect_error(exc):
                        raise

            def _send_body(
                self,
                status_code: int,
                body: bytes,
                *,
                content_type: str,
                extra_headers: Optional[Dict[str, str]] = None,
            ) -> None:
                raw = bytes(body or b"")
                try:
                    self.send_response(status_code)
                    self.send_header("Content-Type", content_type or "application/octet-stream")
                    for key, value in (extra_headers or {}).items():
                        if str(key).lower() == "content-type":
                            continue
                        self.send_header(str(key), str(value))
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
        self.data_plane_app.target = self.base_url
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
