from __future__ import annotations

"""InfoCenter client and route/node models extracted from controlplane client."""

from dataclasses import dataclass
from datetime import datetime, timezone
import math
from typing import Dict, Optional, Sequence, Tuple


@dataclass(frozen=True)
class InfoCenterNodeService:
    service_name: str
    service_id: str
    status: int
    status_text: str = ""
    worker_count: int = 0
    alive_workers: int = 0
    in_flight: int = 0
    http_base_url: str = ""


@dataclass(frozen=True)
class InfoCenterNodeTaskPool:
    pool_id: str
    owner_client_id: str
    pool_name: str
    code_version: str
    status: str
    worker_count: int = 0
    task_count: int = 0
    inflight: int = 0


@dataclass(frozen=True)
class InfoCenterNode:
    node_instance_id: str
    node_id: str
    control_addr: str
    healthy: bool
    capacity: int
    queue_capacity: int
    queued: int
    inflight: int
    credit: int
    python_version: str = ""
    active_runtimes: Tuple[str, ...] = ()
    tags: Tuple[str, ...] = ()
    service_worker_capacity: int = 0
    service_worker_used: int = 0
    service_worker_available: int = 0
    task_pool_worker_capacity: int = 0
    task_pool_worker_used: int = 0
    task_pool_worker_available: int = 0
    schedulable: bool = True
    drain: bool = False
    reason: str = ""
    loaded_services: Tuple[str, ...] = ()
    services: Tuple[InfoCenterNodeService, ...] = ()
    task_pools: Tuple[InfoCenterNodeTaskPool, ...] = ()


@dataclass(frozen=True)
class InfoCenterServiceRoute:
    service_name: str
    service_id: str
    status: int
    node_instance_id: str
    node_id: str
    control_addr: str
    node_healthy: bool
    worker_count: int
    alive_workers: int
    in_flight: int
    lease_expire_at: datetime
    http_base_url: str
    reported_in_flight: int = 0
    received_count: int = 0
    returned_count: int = 0
    ema_child_invoke_ms: float = 0.0
    ema_samples: int = 0
    predicted_busy: float = 0.0


@dataclass
class NodeCircuitState:
    state: str = "closed"  # closed | open | half_open
    consecutive_failures: int = 0
    open_until_monotonic: float = 0.0
    open_count: int = 0
    probe_in_flight: bool = False
    last_error: str = ""


def _node_instance_key_from_node(node: InfoCenterNode) -> str:
    return str(getattr(node, "node_instance_id", "") or getattr(node, "node_id", "") or getattr(node, "control_addr", "")).strip()


def _node_instance_key_from_route(route: InfoCenterServiceRoute) -> str:
    return str(getattr(route, "node_instance_id", "") or getattr(route, "node_id", "") or getattr(route, "control_addr", "")).strip()


def _route_predicted_busy(route: InfoCenterServiceRoute) -> float:
    value = float(getattr(route, "predicted_busy", 0.0) or 0.0)
    if math.isfinite(value) and value > 0.0:
        return value
    inflight = max(0, int(getattr(route, "in_flight", 0) or 0))
    alive_workers = max(1, int(getattr(route, "alive_workers", 0) or 0))
    return float(inflight) / float(alive_workers)


def _route_sort_key(route: InfoCenterServiceRoute, *, strategy: str) -> Tuple[object, ...]:
    if strategy == "predicted_busy":
        return (
            _route_predicted_busy(route),
            int(getattr(route, "in_flight", 0) or 0),
            -int(getattr(route, "alive_workers", 0) or 0),
            _node_instance_key_from_route(route),
            str(getattr(route, "service_id", "") or ""),
        )
    if strategy == "least_inflight":
        return (
            int(getattr(route, "in_flight", 0) or 0),
            -int(getattr(route, "alive_workers", 0) or 0),
            _node_instance_key_from_route(route),
            str(getattr(route, "service_id", "") or ""),
        )
    raise ValueError("strategy must be one of: predicted_busy, least_inflight, round_robin")


def _build_unique_node_id_map(nodes: Sequence[InfoCenterNode], *, requested_ids: Optional[Sequence[str]] = None) -> Dict[str, InfoCenterNode]:
    out: Dict[str, InfoCenterNode] = {}
    duplicates: set[str] = set()
    for node in nodes:
        node_id = str(getattr(node, "node_id", "") or "").strip()
        if not node_id:
            continue
        if node_id in out:
            duplicates.add(node_id)
            continue
        out[node_id] = node
    relevant_duplicates = duplicates if requested_ids is None else (duplicates & {str(x).strip() for x in requested_ids if str(x).strip()})
    if relevant_duplicates:
        dup_list = sorted(relevant_duplicates)
        raise RuntimeError(
            f"requested node_ids are ambiguous because multiple live node instances share the same node_id: {dup_list}; "
            "please select by node_instance_ids instead"
        )
    return out


class InfoCenterClient:
    """Thin HTTP + JSON client wrapper for InfoCenter service."""

    def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
        from pycloud_parallel.controlplane.client import _target_to_base_url

        self.target = target
        self.base_url = _target_to_base_url(target)
        self.timeout_sec = max(0.1, float(timeout_sec))

    def close(self) -> None:
        return None

    def __enter__(self) -> "InfoCenterClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def register_node(
        self,
        *,
        node_id: str,
        node_instance_id: str = "",
        control_addr: str,
        capacity: int = 32,
        queue_capacity: int = 4000,
        tags: Optional[Sequence[str]] = None,
        version: str = "",
        metadata: Optional[Dict[str, str]] = None,
        services: Optional[Sequence[object]] = None,
        task_pools: Optional[Sequence[Dict[str, object]]] = None,
        active_runtimes: Optional[Sequence[str]] = None,
        service_worker_capacity: int = 0,
        service_worker_used: int = 0,
        task_pool_worker_capacity: int = 0,
        task_pool_worker_used: int = 0,
        python_version: str = "",
    ) -> Dict[str, object]:
        from pycloud_parallel.controlplane.client import _http_json_request

        serialized_services = []
        for item in services or []:
            if isinstance(item, dict):
                serialized_services.append(dict(item))
                continue
            serialized_services.append(
                {
                    "service_name": str(item.service_name),
                    "service_id": str(item.service_id),
                    "status": int(item.status),
                    "worker_count": int(item.worker_count),
                    "alive_workers": int(item.alive_workers),
                    "in_flight": int(item.in_flight),
                    "http_base_url": str(item.http_base_url),
                }
            )
        return _http_json_request(
            base_url=self.base_url,
            path="/nodes/register",
            method="POST",
            timeout_sec=self.timeout_sec,
            payload={
                "node_id": node_id,
                "node_instance_id": str(node_instance_id or "").strip(),
                "control_addr": control_addr,
                "capacity": max(1, int(capacity)),
                "queue_capacity": max(1, int(queue_capacity)),
                "tags": list(tags or []),
                "version": version,
                "metadata": dict(metadata or {}),
                "services": serialized_services,
                "task_pools": list(task_pools or []),
                "python_version": str(python_version or "").strip(),
                "active_runtimes": [str(x).strip() for x in (active_runtimes or []) if str(x).strip()],
                "service_worker_capacity": max(0, int(service_worker_capacity or 0)),
                "service_worker_used": max(0, int(service_worker_used or 0)),
                "task_pool_worker_capacity": max(0, int(task_pool_worker_capacity or 0)),
                "task_pool_worker_used": max(0, int(task_pool_worker_used or 0)),
            },
        )

    def heartbeat_node(
        self,
        *,
        node_id: str,
        node_instance_id: str = "",
        healthy: bool = True,
        metrics: Optional[Dict[str, object]] = None,
        metadata: Optional[Dict[str, str]] = None,
        services: Optional[Sequence[object]] = None,
        task_pools: Optional[Sequence[Dict[str, object]]] = None,
        active_runtimes: Optional[Sequence[str]] = None,
        service_worker_capacity: int = 0,
        service_worker_used: int = 0,
        task_pool_worker_capacity: int = 0,
        task_pool_worker_used: int = 0,
        python_version: str = "",
    ) -> Dict[str, object]:
        from pycloud_parallel.controlplane.client import _http_json_request

        serialized_services = []
        for item in services or []:
            if isinstance(item, dict):
                serialized_services.append(dict(item))
                continue
            serialized_services.append(
                {
                    "service_name": str(item.service_name),
                    "service_id": str(item.service_id),
                    "status": int(item.status),
                    "worker_count": int(item.worker_count),
                    "alive_workers": int(item.alive_workers),
                    "in_flight": int(item.in_flight),
                    "http_base_url": str(item.http_base_url),
                }
            )
        return _http_json_request(
            base_url=self.base_url,
            path="/nodes/heartbeat",
            method="POST",
            timeout_sec=self.timeout_sec,
            payload={
                "node_id": node_id,
                "node_instance_id": str(node_instance_id or "").strip(),
                "healthy": bool(healthy),
                "metrics": dict(metrics or {}),
                "metadata": dict(metadata or {}),
                "services": serialized_services,
                "task_pools": list(task_pools or []),
                "python_version": str(python_version or "").strip(),
                "active_runtimes": [str(x).strip() for x in (active_runtimes or []) if str(x).strip()],
                "service_worker_capacity": max(0, int(service_worker_capacity or 0)),
                "service_worker_used": max(0, int(service_worker_used or 0)),
                "task_pool_worker_capacity": max(0, int(task_pool_worker_capacity or 0)),
                "task_pool_worker_used": max(0, int(task_pool_worker_used or 0)),
            },
        )

    def list_nodes(
        self,
        *,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        limit: int = 100,
    ) -> Sequence[InfoCenterNode]:
        from pycloud_parallel.controlplane.client import _http_json_request

        params = "&".join(
            [
                f"healthy_only={'true' if healthy_only else 'false'}",
                f"tags={','.join([x for x in (tags or []) if x])}",
                f"limit={max(1, int(limit))}",
            ]
        )
        resp = _http_json_request(
            base_url=self.base_url,
            path=f"/nodes?{params}",
            method="GET",
            timeout_sec=self.timeout_sec,
        )
        out = []
        for item in resp.get("nodes", []):
            services = []
            for svc in item.get("services", []) or []:
                services.append(
                    InfoCenterNodeService(
                        service_name=str(svc.get("service_name", "") or ""),
                        service_id=str(svc.get("service_id", "") or ""),
                        status=int(svc.get("status", 0) or 0),
                        status_text=str(svc.get("status_text", "") or ""),
                        worker_count=int(svc.get("worker_count", 0) or 0),
                        alive_workers=int(svc.get("alive_workers", 0) or 0),
                        in_flight=int(svc.get("in_flight", 0) or 0),
                        http_base_url=str(svc.get("http_base_url", "") or ""),
                    )
                )
            task_pools = []
            for pool in item.get("task_pools", []) or []:
                task_pools.append(
                    InfoCenterNodeTaskPool(
                        pool_id=str(pool.get("pool_id", "") or ""),
                        owner_client_id=str(pool.get("owner_client_id", "") or ""),
                        pool_name=str(pool.get("pool_name", "") or ""),
                        code_version=str(pool.get("code_version", "") or ""),
                        status=str(pool.get("status", "") or ""),
                        worker_count=int(pool.get("worker_count", 0) or 0),
                        task_count=int(pool.get("task_count", 0) or 0),
                        inflight=int(pool.get("inflight", 0) or 0),
                    )
                )
            out.append(
                InfoCenterNode(
                    node_instance_id=str(item.get("node_instance_id", "") or item.get("node_id", "") or ""),
                    node_id=str(item.get("node_id", "")),
                    control_addr=str(item.get("control_addr", "")),
                    healthy=bool(item.get("healthy", False)),
                    capacity=int(item.get("capacity", 0) or 0),
                    queue_capacity=int(item.get("queue_capacity", 0) or 0),
                    queued=int(item.get("queued", 0) or 0),
                    inflight=int(item.get("inflight", 0) or 0),
                    credit=int(item.get("credit", 0) or 0),
                    python_version=str(item.get("python_version", "") or ""),
                    active_runtimes=tuple(item.get("active_runtimes") or ()),
                    tags=tuple(item.get("tags") or ()),
                    service_worker_capacity=int(item.get("service_worker_capacity", 0) or 0),
                    service_worker_used=int(item.get("service_worker_used", 0) or 0),
                    service_worker_available=int(item.get("service_worker_available", 0) or 0),
                    task_pool_worker_capacity=int(item.get("task_pool_worker_capacity", 0) or 0),
                    task_pool_worker_used=int(item.get("task_pool_worker_used", 0) or 0),
                    task_pool_worker_available=int(item.get("task_pool_worker_available", 0) or 0),
                    schedulable=bool(item.get("schedulable", True)),
                    drain=bool(item.get("drain", False)),
                    reason=str(item.get("reason", "") or ""),
                    loaded_services=tuple(item.get("loaded_services") or ()),
                    services=tuple(services),
                    task_pools=tuple(task_pools),
                )
            )
        return out

    def register_data_ref(
        self,
        *,
        ref: object,
        ttl_sec: int = 3600,
        node_id: str = "",
        node_instance_id: str = "",
        control_addr: str = "",
        locator_kind: str = "",
        locator_token: str = "",
        replicas: Optional[Sequence[Dict[str, object]]] = None,
    ) -> Dict[str, object]:
        from pycloud_parallel.controlplane.client import _http_json_request
        from pycloud_parallel.controlplane.data_ref import coerce_data_ref, data_ref_to_payload

        data_ref = coerce_data_ref(ref)
        payload = {
            "ref": data_ref_to_payload(data_ref),
            "ttl_sec": max(1, int(ttl_sec or 3600)),
            "node_id": str(node_id or "").strip(),
            "node_instance_id": str(node_instance_id or "").strip(),
            "control_addr": str(control_addr or "").strip(),
            "locator_kind": str(locator_kind or "").strip(),
            "locator_token": str(locator_token or "").strip(),
            "replicas": [dict(item) for item in (replicas or ()) if isinstance(item, dict)],
        }
        return _http_json_request(
            base_url=self.base_url,
            path="/data/register",
            method="POST",
            timeout_sec=self.timeout_sec,
            payload=payload,
        )

    def resolve_data_ref(self, *, ref_id: str) -> Dict[str, object]:
        from pycloud_parallel.controlplane.client import _http_json_request
        from urllib.parse import quote

        normalized_ref_id = str(ref_id or "").strip()
        if not normalized_ref_id:
            raise ValueError("ref_id is required")
        return _http_json_request(
            base_url=self.base_url,
            path=f"/data/resolve/{quote(normalized_ref_id, safe='')}",
            method="GET",
            timeout_sec=self.timeout_sec,
        )

    def touch_data_ref(self, *, ref_id: str) -> Dict[str, object]:
        from pycloud_parallel.controlplane.client import _http_json_request

        normalized_ref_id = str(ref_id or "").strip()
        if not normalized_ref_id:
            raise ValueError("ref_id is required")
        return _http_json_request(
            base_url=self.base_url,
            path="/data/touch",
            method="POST",
            timeout_sec=self.timeout_sec,
            payload={"ref_id": normalized_ref_id},
        )

    def release_data_ref(self, *, ref_id: str) -> Dict[str, object]:
        from pycloud_parallel.controlplane.client import _http_json_request

        normalized_ref_id = str(ref_id or "").strip()
        if not normalized_ref_id:
            raise ValueError("ref_id is required")
        return _http_json_request(
            base_url=self.base_url,
            path="/data/release",
            method="POST",
            timeout_sec=self.timeout_sec,
            payload={"ref_id": normalized_ref_id},
        )

    def list_data_refs(
        self,
        *,
        limit: int = 1000,
        node_id: str = "",
        node_instance_id: str = "",
    ) -> Sequence[Dict[str, object]]:
        from pycloud_parallel.controlplane.client import _http_json_request
        from urllib.parse import urlencode

        params = urlencode(
            {
                "limit": str(max(1, int(limit or 1000))),
                "node_id": str(node_id or "").strip(),
                "node_instance_id": str(node_instance_id or "").strip(),
            }
        )
        resp = _http_json_request(
            base_url=self.base_url,
            path=f"/data/refs?{params}",
            method="GET",
            timeout_sec=self.timeout_sec,
        )
        refs = resp.get("refs", [])
        if not isinstance(refs, list):
            raise RuntimeError("invalid data refs response")
        return [dict(item) for item in refs if isinstance(item, dict)]

    def list_service_routes(
        self,
        *,
        service_name: str = "",
        healthy_only: bool = True,
        limit: int = 500,
    ) -> Sequence[InfoCenterServiceRoute]:
        from pycloud_parallel.controlplane.client import _http_json_request

        params = "&".join(
            [
                f"service_name={service_name}",
                f"healthy_only={'true' if healthy_only else 'false'}",
                f"limit={max(1, int(limit))}",
            ]
        )
        resp = _http_json_request(
            base_url=self.base_url,
            path=f"/services/routes?{params}",
            method="GET",
            timeout_sec=self.timeout_sec,
        )
        out = []
        for item in resp.get("routes", []):
            dt_text = str(item.get("lease_expire_at", "") or "")
            dt = datetime.fromisoformat(dt_text) if dt_text else datetime.now(timezone.utc)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            out.append(
                InfoCenterServiceRoute(
                    service_name=str(item.get("service_name", "")),
                    service_id=str(item.get("service_id", "")),
                    status=int(item.get("status", 0) or 0),
                    node_instance_id=str(item.get("node_instance_id", "") or item.get("node_id", "") or ""),
                    node_id=str(item.get("node_id", "")),
                    control_addr=str(item.get("control_addr", "")),
                    node_healthy=bool(item.get("node_healthy", False)),
                    worker_count=int(item.get("worker_count", 0) or 0),
                    alive_workers=int(item.get("alive_workers", 0) or 0),
                    in_flight=int(item.get("in_flight", 0) or 0),
                    lease_expire_at=dt.astimezone(timezone.utc),
                    http_base_url=str(item.get("http_base_url", "")),
                    reported_in_flight=int(item.get("reported_in_flight", 0) or 0),
                    received_count=int(item.get("received_count", 0) or 0),
                    returned_count=int(item.get("returned_count", 0) or 0),
                    ema_child_invoke_ms=float(item.get("ema_child_invoke_ms", 0.0) or 0.0),
                    ema_samples=int(item.get("ema_samples", 0) or 0),
                    predicted_busy=float(item.get("predicted_busy", 0.0) or 0.0),
                )
            )
        return out

    def select_task_nodes(
        self,
        *,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        limit: int = 100,
        require_credit: bool = True,
        preferred_runtime_key: str = "",
        runtime: str = "",
    ) -> Sequence[InfoCenterNode]:
        from pycloud_parallel.controlplane.client import _filter_nodes_by_runtime
        from pycloud_parallel.controlplane.runtime_spec import matches_python_runtime, normalize_python_runtime_spec

        nodes = list(self.list_nodes(healthy_only=healthy_only, tags=tags, limit=limit))
        requested_node_ids = [str(node_id).strip() for node_id in (node_ids or []) if str(node_id).strip()]
        requested_instance_ids = [str(node_id).strip() for node_id in (node_instance_ids or []) if str(node_id).strip()]
        preferred_runtime = str(preferred_runtime_key or "").strip()
        normalized_runtime = normalize_python_runtime_spec(runtime)
        discovered_instance_map = {_node_instance_key_from_node(node): node for node in nodes}

        def _ensure_control_addrs(selected_nodes: Sequence[InfoCenterNode], *, label: str) -> None:
            missing = [
                _node_instance_key_from_node(node) or node.node_id
                for node in selected_nodes
                if not str(node.control_addr or "").strip()
            ]
            if missing:
                raise RuntimeError(f"{label} do not expose control_addr and cannot host task pools: {missing}")

        if requested_instance_ids:
            missing_instance_ids = [node_id for node_id in requested_instance_ids if node_id not in discovered_instance_map]
            if missing_instance_ids:
                raise RuntimeError(f"requested node_instance_ids not found in current discovery scope: {missing_instance_ids}")
            selected = [discovered_instance_map[node_id] for node_id in requested_instance_ids]
            _ensure_control_addrs(selected, label="requested node_instance_ids")
            if normalized_runtime:
                incompatible = [
                    node.node_instance_id
                    for node in selected
                    if str(node.python_version or "").strip()
                    and not matches_python_runtime(node.python_version, normalized_runtime)
                ]
                if incompatible:
                    raise RuntimeError(
                        f"requested node_instance_ids do not satisfy runtime {normalized_runtime}: {incompatible}"
                    )
        elif requested_node_ids:
            discovered_node_map = _build_unique_node_id_map(nodes, requested_ids=requested_node_ids)
            missing_node_ids = [node_id for node_id in requested_node_ids if node_id not in discovered_node_map]
            if missing_node_ids:
                raise RuntimeError(f"requested node_ids not found in current discovery scope: {missing_node_ids}")
            selected = [discovered_node_map[node_id] for node_id in requested_node_ids]
            _ensure_control_addrs(selected, label="requested node_ids")
            if normalized_runtime:
                incompatible = [
                    node.node_id
                    for node in selected
                    if str(node.python_version or "").strip()
                    and not matches_python_runtime(node.python_version, normalized_runtime)
                ]
                if incompatible:
                    raise RuntimeError(
                        f"requested node_ids do not satisfy runtime {normalized_runtime}: {incompatible}"
                    )
        else:
            candidates = [
                node
                for node in nodes
                if node.healthy
                and node.schedulable
                and not node.drain
                and str(node.control_addr or "").strip()
                and (not require_credit or node.credit > 0)
            ]
            if normalized_runtime:
                candidates = _filter_nodes_by_runtime(candidates, runtime=normalized_runtime)
            if not candidates:
                if normalized_runtime:
                    raise RuntimeError(
                        f"no schedulable task nodes from InfoCenter for runtime {normalized_runtime}"
                    )
                raise RuntimeError("no schedulable task nodes from InfoCenter")
            candidates.sort(
                key=lambda node: (
                    0 if preferred_runtime and preferred_runtime in node.active_runtimes else 1,
                    -int(node.credit),
                    int(node.queued),
                    int(node.inflight),
                    node.node_id,
                )
            )
            requested_count = int(node_count or 0)
            selected = candidates if requested_count <= 0 else candidates[:requested_count]

        if not selected:
            raise RuntimeError("no task nodes selected from InfoCenter")
        return selected


__all__ = [
    "InfoCenterNodeService",
    "InfoCenterNodeTaskPool",
    "InfoCenterNode",
    "InfoCenterServiceRoute",
    "NodeCircuitState",
    "_node_instance_key_from_node",
    "_node_instance_key_from_route",
    "_route_predicted_busy",
    "_route_sort_key",
    "_build_unique_node_id_map",
    "InfoCenterClient",
]
