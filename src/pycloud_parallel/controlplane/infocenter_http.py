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
from urllib.parse import parse_qs, urlparse

from pycloud_parallel.controlplane.gateway_http import GatewayHttpApp
from pycloud_parallel.controlplane.job_queue import JobQueueManager
from pycloud_parallel.controlplane.netutil import resolve_public_host
from pycloud_parallel.controlplane.serialization import serialize_arrow_compatible
from pycloud_parallel.controlplane.state import InfoCenterState, NodeMetricsState, NodeServiceState, NodeTaskPoolInfo, utc_now
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
            worker_count=max(0, int(item.get("worker_count", 0) or 0)),
            alive_workers=max(0, int(item.get("alive_workers", 0) or 0)),
            in_flight=max(0, int(item.get("in_flight", 0) or 0)),
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
            task_count=max(0, int(item.get("task_count", 0) or 0)),
            inflight=max(0, int(item.get("inflight", 0) or 0)),
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
                "task_count": int(pool.task_count),
                "inflight": int(pool.inflight),
                "created_at": _dt_text(pool.created_at),
                "last_heartbeat_at": _dt_text(pool.last_heartbeat_at),
                "lease_expire_at": _dt_text(pool.lease_expire_at),
            }
            for pool in task_pools
        ],
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


def _pycloud_version() -> str:
    try:
        return str(importlib_metadata.version("pycloud-parallel"))
    except Exception:
        return "unknown"


def _render_ops_page(state: InfoCenterState) -> str:
    nodes = state.list_nodes(healthy_only=False, tags=(), limit=10000)
    node_rows: List[str] = []
    service_rows: List[str] = []
    pool_entries: List[tuple] = []
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
                f"<td>{html.escape(str(timing.get('last_total_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_setup_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_build_execute_spec_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_executor_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_finalize_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_child_decode_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_child_invoke_ms', timing.get('last_invoke_ms', '-'))))}</td>"
                f"<td>{html.escape(str(timing.get('last_child_encode_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_total_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_setup_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_build_execute_spec_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_executor_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_finalize_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_child_decode_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_child_invoke_ms', timing.get('avg_invoke_ms', '-'))))}</td>"
                f"<td>{html.escape(str(timing.get('avg_child_encode_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('max_total_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_invoke_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_method', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_error_type', '-')))}</td>"
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
                f"<td>{html.escape(str(timing.get('last_total_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_setup_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_build_execute_spec_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_executor_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_finalize_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_child_decode_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_child_invoke_ms', timing.get('last_invoke_ms', '-'))))}</td>"
                f"<td>{html.escape(str(timing.get('last_child_encode_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_total_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_setup_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_build_execute_spec_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_executor_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_finalize_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_child_decode_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('avg_child_invoke_ms', timing.get('avg_invoke_ms', '-'))))}</td>"
                f"<td>{html.escape(str(timing.get('avg_child_encode_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('max_total_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_invoke_ms', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_method', '-')))}</td>"
                f"<td>{html.escape(str(timing.get('last_error_type', '-')))}</td>"
                f"<td>{html.escape(pool.code_version[:20] + ('...' if len(pool.code_version) > 20 else ''))}</td>"
                f"<td>{html.escape(_dt_text(pool.created_at))}</td>"
                f"<td>{html.escape(_dt_text(pool.last_heartbeat_at))}</td>"
                f"<td>{html.escape(_dt_text(pool.lease_expire_at))}</td>"
                "</tr>"
            ))
    node_body = "\n".join(node_rows) or "<tr><td colspan='21'>no nodes</td></tr>"
    service_body = "\n".join(service_rows) or "<tr><td colspan='33'>no services</td></tr>"
    pool_entries.sort(key=lambda item: item[0], reverse=True)
    pool_rows = [row for _created_at, row in pool_entries]
    pool_body = "\n".join(pool_rows) or "<tr><td colspan='28'>no task pools</td></tr>"
    return (
        "<!doctype html><html><head><meta charset='utf-8'><title>InfoCenter Ops</title>"
        "<style>body{font-family:Menlo,monospace;margin:20px;}table{border-collapse:collapse;width:100%;}"
        "th,td{border:1px solid #ccc;padding:6px 8px;font-size:13px;vertical-align:top;}th{background:#f5f5f5;text-align:left;}"
        "h2{margin-top:28px;} .muted{color:#666;} .section-note{color:#555;font-size:12px;margin:6px 0 10px;}"
        ".stale-row{background:#fff1f0;color:#8a1f11;}</style>"
        "</head><body>"
        f"<h1>InfoCenter Ops</h1><div class='section-note'>controlplane_version={html.escape(_pycloud_version())}</div>"
        "<div class='section-note'>Node table shows task-mode pressure plus service/task-pool capacity. "
        "Service table below shows each deployed service instance, worker process counts, and aggregated timing metrics. "
        "Task pool table shows native temporary pools running on each node. "
        "Timing details are split into setup/build-execute-spec/executor/finalize plus child decode/invoke/encode segments. "
        "Rows for stale nodes are highlighted and rendered as LOST.</div>"
        "<table><thead><tr>"
        "<th>node_id</th><th>instance_id</th><th>control_addr</th><th>healthy</th><th>schedulable</th><th>drain</th><th>pycloud</th>"
        "<th>python</th><th>active runtimes</th><th>svc cap</th><th>svc used</th><th>svc avail</th><th>pool cap</th><th>pool used</th><th>pool avail</th><th>pool inflight</th><th>pool count</th><th>svc count</th><th>services</th><th>reason</th><th>actions</th>"
        "</tr></thead><tbody>"
        f"{node_body}"
        "</tbody></table>"
        "<h2>Service Instances</h2>"
        "<table><thead><tr>"
        "<th>node_id</th><th>instance_id</th><th>service_name</th><th>service_id</th><th>node_healthy</th><th>status</th><th>workers</th><th>alive</th><th>in_flight</th><th>calls</th><th>errors</th><th>last_total_ms</th><th>last_setup_ms</th><th>last_build_execute_spec_ms</th><th>last_executor_ms</th><th>last_finalize_ms</th><th>last_child_decode_ms</th><th>last_child_invoke_ms</th><th>last_child_encode_ms</th><th>avg_total_ms</th><th>avg_setup_ms</th><th>avg_build_execute_spec_ms</th><th>avg_executor_ms</th><th>avg_finalize_ms</th><th>avg_child_decode_ms</th><th>avg_child_invoke_ms</th><th>avg_child_encode_ms</th><th>max_total_ms</th><th>last_invoke_ms</th><th>last_method</th><th>last_error_type</th><th>lease_expire_at</th><th>http_base_url</th>"
        "</tr></thead><tbody>"
        f"{service_body}"
        "</tbody></table>"
        "<h2>Task Pools</h2>"
        "<table><thead><tr>"
        "<th>node_id</th><th>instance_id</th><th>pool_name</th><th>pool_id</th><th>owner_client_id</th><th>status</th><th>workers</th><th>tasks</th><th>in_flight</th><th>calls</th><th>errors</th><th>last_total_ms</th><th>last_setup_ms</th><th>last_build_execute_spec_ms</th><th>last_executor_ms</th><th>last_finalize_ms</th><th>last_child_decode_ms</th><th>last_child_invoke_ms</th><th>last_child_encode_ms</th><th>avg_total_ms</th><th>avg_setup_ms</th><th>avg_build_execute_spec_ms</th><th>avg_executor_ms</th><th>avg_finalize_ms</th><th>avg_child_decode_ms</th><th>avg_child_invoke_ms</th><th>avg_child_encode_ms</th><th>max_total_ms</th><th>last_invoke_ms</th><th>last_method</th><th>last_error_type</th><th>code_version</th><th>created_at</th><th>last_heartbeat_at</th><th>lease_expire_at</th>"
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
                    )
                    if node is None:
                        self._send_json(404, {"ok": False, "error": "unknown node"})
                        return
                    self._send_json(200, {"ok": True, "accepted": True, "next_heartbeat_in_sec": state.heartbeat_interval_sec})
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
                if gateway_app is not None:
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
                if parsed.path == "/ops":
                    if self._requires_auth(parsed.path):
                        if not self._check_auth():
                            self._send_json(401, {"ok": False, "error": "unauthorized"})
                            return
                    raw = _render_ops_page(state).encode("utf-8")
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
