from __future__ import annotations

"""HTTP + JSON server for InfoCenter control-plane and lightweight ops UI."""

import html
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from pycloud_parallel.controlplane.gateway_http import GatewayHttpApp
from pycloud_parallel.controlplane.state import InfoCenterState, NodeMetricsState, NodeServiceState, utc_now
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


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
            lease_expire_at=utc_now(),
            http_base_url=str(item.get("http_base_url", "") or ""),
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


def _serialize_service(service: NodeServiceState) -> Dict[str, object]:
    return {
        "service_name": str(service.service_name),
        "service_id": str(service.service_id),
        "status": int(service.status),
        "status_text": _service_status_text(service.status),
        "worker_count": int(service.worker_count),
        "alive_workers": int(service.alive_workers),
        "in_flight": int(service.in_flight),
        "lease_expire_at": _dt_text(service.lease_expire_at),
        "http_base_url": str(service.http_base_url or ""),
    }


def _serialize_node(state) -> Dict[str, object]:
    services = [_serialize_service(svc) for svc in sorted(state.services.values(), key=lambda item: (item.service_name, item.service_id))]
    loaded_services = sorted({svc["service_name"] for svc in services})
    return {
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
        "service_count": int(len(services)),
        "loaded_services": loaded_services,
        "services": services,
    }


def _render_ops_page(state: InfoCenterState) -> str:
    nodes = state.list_nodes(healthy_only=False, tags=(), limit=10000)
    node_rows: List[str] = []
    service_rows: List[str] = []
    for node in nodes:
        services = sorted(node.services.values(), key=lambda item: (item.service_name, item.service_id))
        loaded = "<br>".join(
            f"{html.escape(svc.service_name)} "
            f"<span class='muted'>[{svc.alive_workers}/{svc.worker_count} alive, in-flight {svc.in_flight}]</span>"
            for svc in services
        ) or "-"
        active_runtimes = ", ".join(node.active_runtimes[:10]) or "-"
        node_rows.append(
            "<tr>"
            f"<td>{html.escape(node.node_id)}</td>"
            f"<td>{html.escape(node.control_addr)}</td>"
            f"<td>{'yes' if node.healthy else 'no'}</td>"
            f"<td>{'yes' if node.schedulable else 'no'}</td>"
            f"<td>{'yes' if node.drain else 'no'}</td>"
            f"<td>{html.escape(node.python_version or '-')}</td>"
            f"<td>{html.escape(active_runtimes)}</td>"
            f"<td>{node.service_worker_capacity}</td>"
            f"<td>{node.service_worker_used}</td>"
            f"<td>{node.service_worker_available()}</td>"
            f"<td>{len(services)}</td>"
            f"<td>{loaded}</td>"
            f"<td>{html.escape(node.reason or '')}</td>"
            "<td>"
            f"<form method='post' action='/ops/nodes/{html.escape(node.node_id)}/cordon' style='display:inline'><button type='submit'>cordon</button></form> "
            f"<form method='post' action='/ops/nodes/{html.escape(node.node_id)}/uncordon' style='display:inline'><button type='submit'>uncordon</button></form> "
            f"<form method='post' action='/ops/nodes/{html.escape(node.node_id)}/drain' style='display:inline'><button type='submit'>drain</button></form> "
            f"<form method='post' action='/ops/nodes/{html.escape(node.node_id)}/undrain' style='display:inline'><button type='submit'>undrain</button></form>"
            "</td>"
            "</tr>"
        )
        for svc in services:
            service_rows.append(
                "<tr>"
                f"<td>{html.escape(node.node_id)}</td>"
                f"<td>{html.escape(svc.service_name)}</td>"
                f"<td>{html.escape(svc.service_id)}</td>"
                f"<td>{html.escape(_service_status_text(svc.status))}</td>"
                f"<td>{svc.worker_count}</td>"
                f"<td>{svc.alive_workers}</td>"
                f"<td>{svc.in_flight}</td>"
                f"<td>{html.escape(_dt_text(svc.lease_expire_at))}</td>"
                f"<td>{html.escape(svc.http_base_url or '-')}</td>"
                "</tr>"
            )
    node_body = "\n".join(node_rows) or "<tr><td colspan='14'>no nodes</td></tr>"
    service_body = "\n".join(service_rows) or "<tr><td colspan='9'>no services</td></tr>"
    return (
        "<!doctype html><html><head><meta charset='utf-8'><title>InfoCenter Ops</title>"
        "<style>body{font-family:Menlo,monospace;margin:20px;}table{border-collapse:collapse;width:100%;}"
        "th,td{border:1px solid #ccc;padding:6px 8px;font-size:13px;vertical-align:top;}th{background:#f5f5f5;text-align:left;}"
        "h2{margin-top:28px;} .muted{color:#666;} .section-note{color:#555;font-size:12px;margin:6px 0 10px;}</style>"
        "</head><body>"
        "<h1>InfoCenter Ops</h1>"
        "<div class='section-note'>Node table shows task-mode pressure and service capacity. Service table below shows each deployed service instance and its worker process counts.</div>"
        "<table><thead><tr>"
        "<th>node_id</th><th>control_addr</th><th>healthy</th><th>schedulable</th><th>drain</th>"
        "<th>python</th><th>active runtimes</th><th>svc cap</th><th>svc used</th><th>svc avail</th><th>svc count</th><th>services</th><th>reason</th><th>actions</th>"
        "</tr></thead><tbody>"
        f"{node_body}"
        "</tbody></table>"
        "<h2>Service Instances</h2>"
        "<table><thead><tr>"
        "<th>node_id</th><th>service_name</th><th>service_id</th><th>status</th><th>workers</th><th>alive</th><th>in_flight</th><th>lease_expire_at</th><th>http_base_url</th>"
        "</tr></thead><tbody>"
        f"{service_body}"
        "</tbody></table></body></html>"
    )


class InfoCenterHttpServer:
    def __init__(
        self,
        *,
        bind: str,
        state: Optional[InfoCenterState] = None,
        gateway_app: Optional[GatewayHttpApp] = None,
    ) -> None:
        self._bind = bind
        self.state = state or InfoCenterState()
        self.gateway_app = gateway_app
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
                if parsed.path == "/nodes/register":
                    payload = self._read_json()
                    if payload is None:
                        return
                    node = state.register_node_record(
                        node_id=str(payload.get("node_id", "")).strip(),
                        control_addr=str(payload.get("control_addr", "")).strip(),
                        capacity=max(1, int(payload.get("capacity", 1) or 1)),
                        queue_capacity=max(1, int(payload.get("queue_capacity", 1) or 1)),
                        tags=payload.get("tags") or [],
                        version=str(payload.get("version", "") or ""),
                        python_version=str(payload.get("python_version", "") or ""),
                        metadata=dict(payload.get("metadata") or {}),
                        services=_parse_services(payload.get("services")),
                        active_runtimes=[str(x).strip() for x in (payload.get("active_runtimes") or []) if str(x).strip()],
                        service_worker_capacity=max(0, int(payload.get("service_worker_capacity", 0) or 0)),
                        service_worker_used=max(0, int(payload.get("service_worker_used", 0) or 0)),
                    )
                    self._send_json(200, {"ok": True, "heartbeat_interval_sec": state.heartbeat_interval_sec, "node": _serialize_node(node)})
                    return
                if parsed.path == "/nodes/heartbeat":
                    payload = self._read_json()
                    if payload is None:
                        return
                    metrics_raw = payload.get("metrics") or {}
                    node = state.heartbeat_record(
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
                        services=_parse_services(payload.get("services")),
                        python_version=str(payload.get("python_version", "") or ""),
                        active_runtimes=[str(x).strip() for x in (payload.get("active_runtimes") or []) if str(x).strip()],
                        service_worker_capacity=max(0, int(payload.get("service_worker_capacity", 0) or 0)),
                        service_worker_used=max(0, int(payload.get("service_worker_used", 0) or 0)),
                    )
                    if node is None:
                        self._send_json(404, {"ok": False, "error": "unknown node"})
                        return
                    self._send_json(200, {"ok": True, "accepted": True, "next_heartbeat_in_sec": state.heartbeat_interval_sec})
                    return
                if len(parts) == 4 and parts[:2] == ["ops", "nodes"]:
                    node_id = parts[2]
                    action = parts[3]
                    if action == "cordon":
                        state.update_node_schedule_state(node_id, schedulable=False)
                    elif action == "uncordon":
                        state.update_node_schedule_state(node_id, schedulable=True)
                    elif action == "drain":
                        state.update_node_schedule_state(node_id, drain=True)
                    elif action == "undrain":
                        state.update_node_schedule_state(node_id, drain=False)
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
                    body = self.rfile.read(int(self.headers.get("Content-Length", "0") or 0))
                    handled = gateway_app.handle_post(path=self.path, headers=self.headers, body=body)
                    if handled is not None:
                        code, resp = handled
                        self._send_json(code, resp)
                        return
                self._send_json(404, {"ok": False, "error": "not found"})

            def do_GET(self):  # noqa: N802
                parsed = urlparse(self.path)
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
                    raw = _render_ops_page(state).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
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

            def _read_json(self) -> Optional[dict]:
                body = self.rfile.read(int(self.headers.get("Content-Length", "0") or 0))
                try:
                    payload = json.loads(body.decode("utf-8") if body else "{}")
                except Exception:
                    self._send_json(400, {"ok": False, "error": "invalid json body"})
                    return None
                if not isinstance(payload, dict):
                    self._send_json(400, {"ok": False, "error": "json body must be object"})
                    return None
                return payload

            def _send_json(self, status_code: int, data: Dict[str, object]) -> None:
                raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self._server = ThreadingHTTPServer((host, port), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, name="infocenter-http", daemon=True)
        self._thread.start()
        public_host = "127.0.0.1" if host in ("0.0.0.0", "", "::") else host
        actual_port = int(self._server.server_address[1])
        self.base_url = f"http://{public_host}:{actual_port}"

    def wait_for_termination(self) -> None:
        thread = self._thread
        if thread is not None:
            thread.join()

    def stop(self, grace: int = 0) -> None:
        del grace
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None
        if self.gateway_app is not None:
            self.gateway_app.stop()
