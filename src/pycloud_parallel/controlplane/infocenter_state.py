from __future__ import annotations

"""InfoCenter state backend."""

import threading
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pycloud_parallel.controlplane.data_ref import DataRef, coerce_data_ref
from pycloud_parallel.controlplane.infocenter.models import (
    DataRegistryEntry,
    NodeMetricsState,
    NodeCapability,
    NodeServiceState,
    NodeState,
    NodeTaskPoolInfo,
)
from pycloud_parallel.controlplane.state_time import ts_to_dt, utc_now
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


class InfoCenterState:
    def __init__(self, *, lease_ttl_sec: int = 90, heartbeat_interval_sec: int = 30) -> None:
        self.lease_ttl_sec = max(1, lease_ttl_sec)
        self.heartbeat_interval_sec = max(1, heartbeat_interval_sec)
        self._lock = threading.Lock()
        self._nodes: Dict[str, NodeState] = {}
        self._data_refs: Dict[str, DataRegistryEntry] = {}

    def _node_is_stale_locked(self, state: NodeState, *, now: Optional[datetime] = None) -> bool:
        current_time = now or utc_now()
        return (current_time - state.last_seen_at).total_seconds() > float(self.lease_ttl_sec)

    def _node_is_healthy_locked(self, state: NodeState, *, now: Optional[datetime] = None) -> bool:
        return bool(state.healthy) and not self._node_is_stale_locked(state, now=now)

    def _data_ref_is_expired_locked(self, entry: DataRegistryEntry, *, now: Optional[datetime] = None) -> bool:
        current_time = now or utc_now()
        ttl_sec = max(1, int(entry.ttl_sec or 0))
        return (current_time - entry.last_at).total_seconds() > float(ttl_sec)

    def _prune_expired_data_refs_locked(self, *, now: Optional[datetime] = None) -> None:
        current_time = now or utc_now()
        expired = [ref_id for ref_id, entry in self._data_refs.items() if self._data_ref_is_expired_locked(entry, now=current_time)]
        for ref_id in expired:
            self._data_refs.pop(ref_id, None)

    def _prune_replaced_stale_nodes_locked(
        self,
        *,
        node_instance_id: str,
        node_id: str,
        control_addr: str,
        now: Optional[datetime] = None,
    ) -> None:
        current_time = now or utc_now()
        normalized_instance_id = str(node_instance_id or "").strip()
        normalized_node_id = str(node_id or "").strip()
        normalized_control_addr = str(control_addr or "").strip()
        if not normalized_instance_id or not normalized_node_id or not normalized_control_addr:
            return
        stale_keys = [
            key
            for key, state in self._nodes.items()
            if key != normalized_instance_id
            and str(state.node_id or "").strip() == normalized_node_id
            and str(state.control_addr or "").strip() == normalized_control_addr
            and self._node_is_stale_locked(state, now=current_time)
        ]
        for key in stale_keys:
            self._nodes.pop(key, None)

    def _effective_service_state_locked(
        self,
        state: NodeState,
        svc: NodeServiceState,
        *,
        now: Optional[datetime] = None,
    ) -> Tuple[int, int, int, datetime, bool, str]:
        current_time = now or utc_now()
        node_healthy = self._node_is_healthy_locked(state, now=current_time)
        if node_healthy:
            return (
                int(svc.status),
                int(svc.alive_workers),
                int(svc.in_flight),
                svc.lease_expire_at,
                False,
                "",
            )
        return (
            int(pb2.SERVICE_STATUS_UNSPECIFIED),
            0,
            0,
            current_time,
            True,
            "LOST",
        )

    @staticmethod
    def _predicted_busy_score(*, inflight: int, ema_child_invoke_ms: float, alive_workers: int) -> float:
        normalized_inflight = max(0, int(inflight or 0))
        normalized_workers = max(1, int(alive_workers or 0))
        normalized_ema = max(0.0, float(ema_child_invoke_ms or 0.0))
        if normalized_ema <= 0.0:
            return float(normalized_inflight) / float(normalized_workers)
        return (float(normalized_inflight) * normalized_ema) / float(normalized_workers)

    def register_node_record(
        self,
        *,
        node_instance_id: str = "",
        node_id: str,
        control_addr: str,
        capacity: int,
        queue_capacity: int,
        tags: Iterable[str] = (),
        version: str = "",
        metadata: Optional[Dict[str, str]] = None,
        services: Optional[Dict[str, NodeServiceState]] = None,
        task_pools: Optional[Dict[str, NodeTaskPoolInfo]] = None,
        active_runtimes: Optional[Sequence[str]] = None,
        service_worker_capacity: int = 0,
        service_worker_used: int = 0,
        task_pool_worker_capacity: int = 0,
        task_pool_worker_used: int = 0,
        python_version: str = "",
        capability: Optional[NodeCapability] = None,
    ) -> NodeState:
        now = utc_now()
        normalized_instance_id = str(node_instance_id or node_id or "").strip()
        if not normalized_instance_id:
            raise ValueError("node_instance_id is required")
        with self._lock:
            self._prune_replaced_stale_nodes_locked(
                node_instance_id=normalized_instance_id,
                node_id=node_id,
                control_addr=control_addr,
                now=now,
            )
            state = self._nodes.get(normalized_instance_id)
            if state is None:
                state = NodeState(
                    node_instance_id=normalized_instance_id,
                    node_id=node_id,
                    control_addr=control_addr,
                    capacity=max(1, capacity),
                    queue_capacity=max(1, queue_capacity),
                    python_version=str(python_version or "").strip(),
                )
                self._nodes[normalized_instance_id] = state
            state.node_instance_id = normalized_instance_id
            state.node_id = str(node_id or state.node_id or "").strip() or normalized_instance_id
            state.control_addr = control_addr
            state.capacity = max(1, capacity)
            state.queue_capacity = max(1, queue_capacity)
            state.tags = list(tags or [])
            state.version = str(version or "")
            state.python_version = str(python_version or state.python_version or "").strip()
            state.metadata = dict(metadata or {})
            state.healthy = True
            state.last_seen_at = now
            state.services = dict(services or {})
            state.task_pools = dict(task_pools or {})
            state.active_runtimes = [str(x).strip() for x in (active_runtimes or []) if str(x).strip()]
            state.service_worker_capacity = max(0, int(service_worker_capacity or 0))
            state.service_worker_used = max(0, min(int(service_worker_used or 0), state.service_worker_capacity or int(service_worker_used or 0)))
            state.task_pool_worker_capacity = max(0, int(task_pool_worker_capacity or 0))
            state.task_pool_worker_used = max(0, min(int(task_pool_worker_used or 0), state.task_pool_worker_capacity or int(task_pool_worker_used or 0)))
            if capability is not None:
                state.capability = capability
            if state.metrics.credit == 0:
                state.metrics.credit = state.queue_capacity
            return state

    def register_data_ref_record(
        self,
        *,
        ref: DataRef | object,
        ttl_sec: int = 3600,
        node_id: str = "",
        node_instance_id: str = "",
        control_addr: str = "",
        locator_kind: str = "",
        locator_token: str = "",
        replicas: Optional[Sequence[Dict[str, object]]] = None,
    ) -> DataRegistryEntry:
        data_ref = ref if isinstance(ref, DataRef) else coerce_data_ref(ref)
        now = utc_now()
        normalized_replicas = tuple(
            {
                "node_id": str(item.get("node_id", "") or "").strip(),
                "node_instance_id": str(item.get("node_instance_id", "") or "").strip(),
                "control_addr": str(item.get("control_addr", "") or "").strip(),
            }
            for item in (replicas or ())
            if isinstance(item, dict) and str(item.get("control_addr", "") or "").strip()
        )
        entry = DataRegistryEntry(
            ref_id=str(data_ref.ref_id or ""),
            storage_id=str(data_ref.storage_id or data_ref.object_id or ""),
            logical_type=str(data_ref.logical_type or ""),
            format=str(data_ref.format or "bin"),
            size_bytes=max(0, int(data_ref.size_bytes or 0)),
            materialize_as=str(data_ref.materialize_as or "auto"),
            locator_kind=str(locator_kind or data_ref.locator_kind or "").strip(),
            locator_token=str(locator_token or data_ref.locator_token or "").strip(),
            consume_on_read=bool(data_ref.consume_on_read),
            node_id=str(node_id or data_ref.node_id or "").strip(),
            node_instance_id=str(node_instance_id or data_ref.node_instance_id or "").strip(),
            control_addr=str(control_addr or data_ref.control_addr or "").strip(),
            replicas=normalized_replicas,
            created_at=now,
            last_at=now,
            ttl_sec=max(1, int(ttl_sec or 3600)),
        )
        with self._lock:
            self._prune_expired_data_refs_locked(now=now)
            existing = self._data_refs.get(entry.ref_id)
            if existing is not None:
                entry = DataRegistryEntry(
                    ref_id=entry.ref_id,
                    storage_id=entry.storage_id,
                    logical_type=entry.logical_type,
                    format=entry.format,
                    size_bytes=entry.size_bytes,
                    materialize_as=entry.materialize_as,
                    locator_kind=entry.locator_kind,
                    locator_token=entry.locator_token,
                    consume_on_read=entry.consume_on_read,
                    node_id=entry.node_id,
                    node_instance_id=entry.node_instance_id,
                    control_addr=entry.control_addr,
                    replicas=entry.replicas,
                    created_at=existing.created_at,
                    last_at=now,
                    ttl_sec=max(1, int(ttl_sec or existing.ttl_sec or 3600)),
                )
            self._data_refs[entry.ref_id] = entry
        return entry

    def resolve_data_ref_record(self, ref_id: str) -> DataRegistryEntry:
        normalized_ref_id = str(ref_id or "").strip()
        if not normalized_ref_id:
            raise KeyError("ref_id is required")
        now = utc_now()
        with self._lock:
            self._prune_expired_data_refs_locked(now=now)
            entry = self._data_refs.get(normalized_ref_id)
            if entry is None:
                raise KeyError("data ref not found")
            return entry

    def touch_data_ref_record(self, ref_id: str) -> DataRegistryEntry:
        entry = self.resolve_data_ref_record(ref_id)
        now = utc_now()
        updated = DataRegistryEntry(
            ref_id=entry.ref_id,
            storage_id=entry.storage_id,
            logical_type=entry.logical_type,
            format=entry.format,
            size_bytes=entry.size_bytes,
            materialize_as=entry.materialize_as,
            locator_kind=entry.locator_kind,
            locator_token=entry.locator_token,
            consume_on_read=entry.consume_on_read,
            node_id=entry.node_id,
            node_instance_id=entry.node_instance_id,
            control_addr=entry.control_addr,
            replicas=entry.replicas,
            created_at=entry.created_at,
            last_at=now,
            ttl_sec=entry.ttl_sec,
        )
        with self._lock:
            self._data_refs[entry.ref_id] = updated
        return updated

    def release_data_ref_record(self, ref_id: str) -> bool:
        normalized_ref_id = str(ref_id or "").strip()
        if not normalized_ref_id:
            return False
        with self._lock:
            return self._data_refs.pop(normalized_ref_id, None) is not None

    def list_data_ref_records(
        self,
        *,
        limit: int = 1000,
        node_id: str = "",
        node_instance_id: str = "",
    ) -> List[DataRegistryEntry]:
        normalized_node_id = str(node_id or "").strip()
        normalized_node_instance_id = str(node_instance_id or "").strip()
        now = utc_now()
        with self._lock:
            self._prune_expired_data_refs_locked(now=now)
            entries = list(self._data_refs.values())
        if normalized_node_instance_id:
            entries = [entry for entry in entries if str(entry.node_instance_id or "").strip() == normalized_node_instance_id]
        elif normalized_node_id:
            entries = [entry for entry in entries if str(entry.node_id or "").strip() == normalized_node_id]
        entries.sort(key=lambda item: (item.last_at, item.created_at, item.ref_id), reverse=True)
        return entries[: max(1, int(limit or 1000))]

    def register_node(self, request: pb2.RegisterNodeRequest) -> NodeState:
        metadata = dict(request.metadata)
        return self.register_node_record(
            node_instance_id=getattr(request, "node_instance_id", "") or request.node_id,
            node_id=request.node_id,
            control_addr=request.control_addr,
            capacity=max(1, request.capacity),
            queue_capacity=max(1, request.queue_capacity),
            tags=request.tags,
            version=request.version,
            metadata=metadata,
            services=self._parse_services(request.services),
            task_pools={},
            active_runtimes=(),
            service_worker_capacity=int(metadata.get("service_worker_capacity", "0") or 0),
            service_worker_used=int(metadata.get("service_worker_used", "0") or 0),
            task_pool_worker_capacity=int(metadata.get("task_pool_worker_capacity", "0") or 0),
            task_pool_worker_used=int(metadata.get("task_pool_worker_used", "0") or 0),
            python_version=metadata.get("python_version", ""),
            capability=NodeCapability.from_dict(getattr(request, "capability", None) and {
                "supported_modes": list(getattr(request.capability, "supported_modes", []) or []),
                "supports_transport_payload_bytes": bool(getattr(request.capability, "supports_transport_payload_bytes", False)),
                "supports_http_bytes_transport": bool(getattr(request.capability, "supports_http_bytes_transport", False)),
                "max_grpc_send_bytes": int(getattr(request.capability, "max_grpc_send_bytes", 0) or 0),
                "max_grpc_recv_bytes": int(getattr(request.capability, "max_grpc_recv_bytes", 0) or 0),
                "max_http_body_bytes": int(getattr(request.capability, "max_http_body_bytes", 0) or 0),
                "max_upload_file_bytes": int(getattr(request.capability, "max_upload_file_bytes", 0) or 0),
                "max_upload_total_bytes": int(getattr(request.capability, "max_upload_total_bytes", 0) or 0),
            }),
        )

    def heartbeat_record(
        self,
        *,
        node_instance_id: str = "",
        node_id: str,
        healthy: bool,
        metrics: Optional[NodeMetricsState] = None,
        metadata: Optional[Dict[str, str]] = None,
        services: Optional[Dict[str, NodeServiceState]] = None,
        task_pools: Optional[Dict[str, NodeTaskPoolInfo]] = None,
        active_runtimes: Optional[Sequence[str]] = None,
        service_worker_capacity: int = 0,
        service_worker_used: int = 0,
        task_pool_worker_capacity: int = 0,
        task_pool_worker_used: int = 0,
        python_version: str = "",
        capability: Optional[NodeCapability] = None,
    ) -> Optional[NodeState]:
        now = utc_now()
        normalized_instance_id = str(node_instance_id or node_id or "").strip()
        if not normalized_instance_id:
            return None
        with self._lock:
            state = self._nodes.get(normalized_instance_id)
            if state is None:
                return None
            state.node_instance_id = normalized_instance_id
            state.node_id = str(node_id or state.node_id or "").strip() or normalized_instance_id
            state.healthy = bool(healthy)
            state.last_seen_at = now
            if metrics is not None:
                state.metrics = metrics
            if metadata is not None:
                state.metadata = dict(metadata or {})
            state.services = dict(services or {})
            state.task_pools = dict(task_pools or {})
            if python_version:
                state.python_version = str(python_version).strip()
            if active_runtimes is not None:
                state.active_runtimes = [str(x).strip() for x in active_runtimes if str(x).strip()]
            if service_worker_capacity > 0:
                state.service_worker_capacity = max(0, int(service_worker_capacity))
            state.service_worker_used = max(
                0,
                min(
                    int(service_worker_used or 0),
                    state.service_worker_capacity or int(service_worker_used or 0),
                ),
            )
            if task_pool_worker_capacity > 0:
                state.task_pool_worker_capacity = max(0, int(task_pool_worker_capacity))
            state.task_pool_worker_used = max(
                0,
                min(
                    int(task_pool_worker_used or 0),
                    state.task_pool_worker_capacity or int(task_pool_worker_used or 0),
                ),
            )
            if capability is not None:
                state.capability = capability
            return state

    def heartbeat(self, request: pb2.HeartbeatNodeRequest) -> Optional[NodeState]:
        return self.heartbeat_record(
            node_instance_id=getattr(request, "node_instance_id", "") or request.node_id,
            node_id=request.node_id,
            healthy=bool(request.healthy),
            metrics=NodeMetricsState(
                queued=max(0, request.metrics.queued),
                inflight=max(0, request.metrics.inflight),
                running=max(0, request.metrics.running),
                credit=request.metrics.credit,
                cpu_percent=float(request.metrics.cpu_percent),
                mem_percent=float(request.metrics.mem_percent),
            ),
            services=self._parse_services(request.services),
            capability=NodeCapability.from_dict(getattr(request, "capability", None) and {
                "supported_modes": list(getattr(request.capability, "supported_modes", []) or []),
                "supports_transport_payload_bytes": bool(getattr(request.capability, "supports_transport_payload_bytes", False)),
                "supports_http_bytes_transport": bool(getattr(request.capability, "supports_http_bytes_transport", False)),
                "max_grpc_send_bytes": int(getattr(request.capability, "max_grpc_send_bytes", 0) or 0),
                "max_grpc_recv_bytes": int(getattr(request.capability, "max_grpc_recv_bytes", 0) or 0),
                "max_http_body_bytes": int(getattr(request.capability, "max_http_body_bytes", 0) or 0),
                "max_upload_file_bytes": int(getattr(request.capability, "max_upload_file_bytes", 0) or 0),
                "max_upload_total_bytes": int(getattr(request.capability, "max_upload_total_bytes", 0) or 0),
            }),
        )

    def _parse_services(self, reports: Iterable[pb2.ServiceRouteReport]) -> Dict[str, NodeServiceState]:
        out: Dict[str, NodeServiceState] = {}
        for item in reports:
            if not item.service_name or not item.service_id:
                continue
            out[item.service_id] = NodeServiceState(
                service_name=item.service_name,
                service_id=item.service_id,
                status=int(item.status),
                policy_id=str(getattr(item, "policy_id", "") or "default_safe"),
                worker_count=max(0, int(item.worker_count)),
                alive_workers=max(0, int(item.alive_workers)),
                in_flight=max(0, int(item.in_flight)),
                lease_expire_at=ts_to_dt(item.lease_expire_at),
                http_base_url=item.http_base_url,
            )
        return out

    def mark_node_lost(self, node_instance_id: str, *, reason: str = "") -> NodeState:
        now = utc_now()
        with self._lock:
            state = self._nodes.get(node_instance_id)
            if state is None:
                raise KeyError("node not found")
            state.healthy = False
            state.schedulable = False
            state.last_seen_at = now - timedelta(seconds=float(self.lease_ttl_sec) + 1.0)
            state.reason = str(reason or state.reason or "node lost")
            degraded: Dict[str, NodeServiceState] = {}
            for service_id, svc in state.services.items():
                degraded[service_id] = NodeServiceState(
                    service_name=svc.service_name,
                    service_id=svc.service_id,
                    status=int(pb2.SERVICE_STATUS_UNSPECIFIED),
                    policy_id=str(svc.policy_id or "").strip().lower() or "default_safe",
                    worker_count=max(0, int(svc.worker_count)),
                    alive_workers=0,
                    in_flight=0,
                    lease_expire_at=now,
                    http_base_url=svc.http_base_url,
                )
            state.services = degraded
            return NodeState(
                node_instance_id=state.node_instance_id,
                node_id=state.node_id,
                control_addr=state.control_addr,
                capacity=state.capacity,
                queue_capacity=state.queue_capacity,
                tags=list(state.tags),
                version=state.version,
                python_version=state.python_version,
                metadata=dict(state.metadata),
                healthy=False,
                last_seen_at=state.last_seen_at,
                metrics=NodeMetricsState(**vars(state.metrics)),
                services={k: NodeServiceState(**vars(v)) for k, v in state.services.items()},
                task_pools={k: NodeTaskPoolInfo(**vars(v)) for k, v in state.task_pools.items()},
                active_runtimes=list(state.active_runtimes),
                service_worker_capacity=state.service_worker_capacity,
                service_worker_used=state.service_worker_used,
                task_pool_worker_capacity=state.task_pool_worker_capacity,
                task_pool_worker_used=state.task_pool_worker_used,
                schedulable=state.schedulable,
                drain=state.drain,
                reason=state.reason,
                capability=NodeCapability.from_dict(state.capability.to_dict()),
            )

    def list_service_routes(self, *, service_name: str, healthy_only: bool, limit: int) -> List[Dict[str, object]]:
        now = utc_now()
        name_filter = service_name.strip()
        with self._lock:
            out: List[Dict[str, object]] = []
            for state in self._nodes.values():
                is_healthy = self._node_is_healthy_locked(state, now=now)
                if healthy_only and not is_healthy:
                    continue
                for svc in state.services.values():
                    if name_filter and svc.service_name != name_filter:
                        continue
                    effective_status, effective_alive, effective_in_flight, effective_lease_expire_at, stale, status_text = (
                        self._effective_service_state_locked(state, svc, now=now)
                    )
                    reported_inflight = max(0, int(svc.in_flight or 0))
                    received_count = max(0, int(svc.received_count or 0))
                    returned_count = max(0, int(svc.returned_count or 0))
                    if received_count > 0 or returned_count > 0:
                        computed_inflight = max(0, received_count - returned_count)
                    else:
                        computed_inflight = max(0, int(effective_in_flight or 0))
                    effective_computed_inflight = computed_inflight if is_healthy else 0
                    ema_samples = max(0, int(svc.ema_samples or 0))
                    raw_ema_child_invoke_ms = max(0.0, float(svc.ema_child_invoke_ms or 0.0))
                    effective_ema_child_invoke_ms = raw_ema_child_invoke_ms if ema_samples >= 10 else 0.0
                    predicted_busy = self._predicted_busy_score(
                        inflight=effective_computed_inflight,
                        ema_child_invoke_ms=effective_ema_child_invoke_ms,
                        alive_workers=effective_alive,
                    )
                    out.append(
                        {
                            "service_name": svc.service_name,
                            "service_id": svc.service_id,
                            "status": effective_status,
                            "status_text": status_text,
                            "policy_id": str(svc.policy_id or "").strip().lower() or "default_safe",
                            "node_instance_id": state.node_instance_id,
                            "node_id": state.node_id,
                            "control_addr": state.control_addr,
                            "node_healthy": is_healthy,
                            "stale": stale,
                            "worker_count": svc.worker_count,
                            "alive_workers": effective_alive,
                            "in_flight": effective_computed_inflight,
                            "reported_in_flight": reported_inflight,
                            "received_count": received_count,
                            "returned_count": returned_count,
                            "ema_child_invoke_ms": raw_ema_child_invoke_ms,
                            "ema_samples": ema_samples,
                            "predicted_busy": predicted_busy,
                            "lease_expire_at": effective_lease_expire_at,
                            "http_base_url": svc.http_base_url,
                            "capability": state.capability.to_dict(),
                        }
                    )
            out.sort(
                key=lambda x: (
                    x["service_name"],
                    not x["node_healthy"],
                    int(x["status"] != pb2.SERVICE_STATUS_RUNNING),
                    float(x.get("predicted_busy", 0.0) or 0.0),
                    int(x["in_flight"]),
                    -int(x.get("alive_workers", 0) or 0),
                    x["node_id"],
                    x["service_id"],
                )
            )
            return out[: max(1, limit)]

    def list_nodes(self, *, healthy_only: bool, tags: Iterable[str], limit: int) -> List[NodeState]:
        now = utc_now()
        filter_tags = set(tags)
        with self._lock:
            out: List[NodeState] = []
            for state in self._nodes.values():
                is_healthy = self._node_is_healthy_locked(state, now=now)
                if healthy_only and not is_healthy:
                    continue
                if filter_tags and not filter_tags.issubset(set(state.tags)):
                    continue
                out.append(
                    NodeState(
                        node_instance_id=state.node_instance_id,
                        node_id=state.node_id,
                        control_addr=state.control_addr,
                        capacity=state.capacity,
                        queue_capacity=state.queue_capacity,
                        tags=list(state.tags),
                        version=state.version,
                        python_version=state.python_version,
                        metadata=dict(state.metadata),
                        healthy=is_healthy,
                        last_seen_at=state.last_seen_at,
                        metrics=NodeMetricsState(**vars(state.metrics)),
                        services={k: NodeServiceState(**vars(v)) for k, v in state.services.items()},
                        task_pools={k: NodeTaskPoolInfo(**vars(v)) for k, v in state.task_pools.items()},
                        active_runtimes=list(state.active_runtimes),
                        service_worker_capacity=state.service_worker_capacity,
                        service_worker_used=state.service_worker_used,
                        task_pool_worker_capacity=state.task_pool_worker_capacity,
                        task_pool_worker_used=state.task_pool_worker_used,
                        schedulable=state.schedulable,
                        drain=state.drain,
                        reason=state.reason,
                        capability=NodeCapability.from_dict(state.capability.to_dict()),
                    )
                )
            out.sort(key=lambda n: (not n.healthy, not n.schedulable, n.drain, -(n.service_worker_available())))
            return out[: max(1, limit)]

    def update_node_schedule_state(
        self,
        node_instance_id: str,
        *,
        schedulable: Optional[bool] = None,
        drain: Optional[bool] = None,
        reason: Optional[str] = None,
    ) -> NodeState:
        with self._lock:
            state = self._nodes.get(node_instance_id)
            if state is None:
                raise KeyError("node not found")
            if schedulable is not None:
                state.schedulable = bool(schedulable)
            if drain is not None:
                state.drain = bool(drain)
            if reason is not None:
                state.reason = str(reason or "")
            return NodeState(
                node_instance_id=state.node_instance_id,
                node_id=state.node_id,
                control_addr=state.control_addr,
                capacity=state.capacity,
                queue_capacity=state.queue_capacity,
                tags=list(state.tags),
                version=state.version,
                python_version=state.python_version,
                metadata=dict(state.metadata),
                healthy=state.healthy,
                last_seen_at=state.last_seen_at,
                metrics=NodeMetricsState(**vars(state.metrics)),
                services={k: NodeServiceState(**vars(v)) for k, v in state.services.items()},
                task_pools={k: NodeTaskPoolInfo(**vars(v)) for k, v in state.task_pools.items()},
                active_runtimes=list(state.active_runtimes),
                service_worker_capacity=state.service_worker_capacity,
                service_worker_used=state.service_worker_used,
                task_pool_worker_capacity=state.task_pool_worker_capacity,
                task_pool_worker_used=state.task_pool_worker_used,
                schedulable=state.schedulable,
                drain=state.drain,
                reason=state.reason,
                capability=NodeCapability.from_dict(state.capability.to_dict()),
            )
