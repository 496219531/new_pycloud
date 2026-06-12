from __future__ import annotations

"""InfoCenter state backend."""

import json
import os
import tempfile
import contextlib
import heapq
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from pycloud_parallel.data.ref import DataRef, coerce_data_ref
from pycloud_parallel.controlplane.infocenter.models import (
    DataRegistryEntry,
    NodeMetricsState,
    NodeCapability,
    NodeProfile,
    NodeServiceState,
    NodeState,
    NodeTaskPoolInfo,
)
from pycloud_parallel.controlplane.scheduling_policy import is_call_route, is_conflict_scope, is_owner_target
from pycloud_parallel.controlplane.state_time import ts_to_dt, utc_now
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


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


def _endpoint_from_url_or_addr(value: str) -> Tuple[str, int]:
    text = str(value or "").strip().rstrip("/")
    if not text:
        return "", 0
    parsed = urlparse(text)
    if parsed.hostname and parsed.port:
        return parsed.hostname.strip().lower(), int(parsed.port)
    if "/" in text:
        text = text.split("/", 1)[0]
    if ":" not in text:
        return "", 0
    host, port = text.rsplit(":", 1)
    try:
        return host.strip("[]").lower(), int(port)
    except ValueError:
        return "", 0


def normalize_node_profile_key(value: str) -> str:
    host, port = _endpoint_from_url_or_addr(value)
    if not host or port <= 0:
        return ""
    return f"{host}:{port}"


def _startup_service_endpoint(state: NodeState, svc: NodeServiceState) -> Tuple[str, int]:
    endpoint = _endpoint_from_url_or_addr(str(svc.http_base_url or ""))
    if endpoint[1] > 0:
        return endpoint
    return _endpoint_from_url_or_addr(str(state.control_addr or ""))


def _endpoint_matches(left: Tuple[str, int], right: Tuple[str, int]) -> bool:
    left_host, left_port = left
    right_host, right_port = right
    if left_port <= 0 or right_port <= 0 or left_port != right_port:
        return False
    wildcard_hosts = {"", "0.0.0.0", "::", "[::]"}
    if left_host in wildcard_hosts or right_host in wildcard_hosts:
        return True
    return left_host == right_host


class InfoCenterState:
    def __init__(
        self,
        *,
        lease_ttl_sec: int = 90,
        heartbeat_interval_sec: int = 30,
        profiles_path: Optional[str | os.PathLike[str]] = None,
    ) -> None:
        self.lease_ttl_sec = max(1, lease_ttl_sec)
        self.heartbeat_interval_sec = max(1, heartbeat_interval_sec)
        self._lock = threading.Lock()
        self._nodes: Dict[str, NodeState] = {}
        self._services_by_name: Dict[str, set[str]] = {}
        self._profiles: Dict[str, NodeProfile] = {}
        self._profiles_path = Path(profiles_path).expanduser().resolve() if profiles_path else None
        self._data_refs: Dict[str, DataRegistryEntry] = {}
        self._load_profiles()

    def _node_is_stale_locked(self, state: NodeState, *, now: Optional[datetime] = None) -> bool:
        current_time = now or utc_now()
        return (current_time - state.last_seen_at).total_seconds() > float(self.lease_ttl_sec)

    def _node_is_healthy_locked(self, state: NodeState, *, now: Optional[datetime] = None) -> bool:
        return bool(state.healthy) and not self._node_is_stale_locked(state, now=now)

    @staticmethod
    def _normalize_tags(tags: Iterable[str]) -> List[str]:
        return sorted({str(tag).strip() for tag in tags if str(tag).strip()})

    @staticmethod
    def _profile_from_payload(profile_key: str, payload: Dict[str, object]) -> NodeProfile:
        return NodeProfile(
            profile_key=profile_key,
            managed_tags=InfoCenterState._normalize_tags(payload.get("managed_tags") or ()),
            enabled=_coerce_bool(payload.get("enabled"), default=True),
            drain=_coerce_bool(payload.get("drain"), default=False),
            notes=str(payload.get("notes", "") or ""),
        )

    def _load_profiles(self) -> None:
        path = self._profiles_path
        if path is None or not path.exists():
            return
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return
        if isinstance(raw, dict) and isinstance(raw.get("profiles"), dict):
            items = raw.get("profiles", {})
        elif isinstance(raw, dict):
            items = raw
        else:
            return
        profiles: Dict[str, NodeProfile] = {}
        for raw_key, item in items.items():
            if not isinstance(item, dict):
                continue
            profile_key = normalize_node_profile_key(str(item.get("profile_key", "") or raw_key))
            if not profile_key:
                continue
            profiles[profile_key] = self._profile_from_payload(profile_key, item)
        self._profiles = profiles

    def _save_profiles_locked(self) -> None:
        path = self._profiles_path
        if path is None:
            return
        payload = {
            "profiles": {
                key: {
                    "profile_key": profile.profile_key,
                    "managed_tags": list(profile.managed_tags),
                    "enabled": bool(profile.enabled),
                    "drain": bool(profile.drain),
                    "notes": str(profile.notes or ""),
                }
                for key, profile in sorted(self._profiles.items())
            }
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, sort_keys=True)
                fh.write("\n")
            os.replace(tmp_name, path)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp_name)

    @staticmethod
    def _capability_tags_for_state(state: NodeState) -> List[str]:
        tags = ["runtime:py3"]
        python_text = str(state.python_version or "").strip()
        major = python_text.split(".", 1)[0] if python_text else ""
        if major.isdigit():
            tags.append(f"python:{major}.x")
        else:
            tags.append("python:3.x")
        metadata = dict(state.metadata or {})
        component = str(metadata.get("component", "") or "").strip()
        if component == "job-orchestrator" or not bool(getattr(state, "accept_service_deploy", True)):
            tags.append("role:job")
        else:
            tags.append("role:compute")
        return InfoCenterState._normalize_tags(tags)

    def _apply_profile_locked(self, state: NodeState, *, registration_tags: Optional[Iterable[str]] = None) -> None:
        # Boundary: managed_tags are the only persisted human inputs; capability
        # and legacy node tags are recomputed/received facts. Keep clients on the
        # merged tags field so scheduling stays simple.
        profile_key = normalize_node_profile_key(str(state.control_addr or ""))
        state.profile_key = profile_key
        if registration_tags is not None:
            state.legacy_node_tags = self._normalize_tags(registration_tags)
        profile = self._profiles.get(profile_key) if profile_key else None
        state.managed_tags = list(profile.managed_tags) if profile is not None else []
        state.profile_enabled = bool(profile.enabled) if profile is not None else True
        state.profile_notes = str(profile.notes or "") if profile is not None else ""
        state.capability_tags = self._capability_tags_for_state(state)
        state.tags = self._normalize_tags([*state.capability_tags, *state.managed_tags, *state.legacy_node_tags])
        if profile is not None:
            state.drain = bool(profile.drain)
            if not bool(profile.enabled):
                state.schedulable = False
                state.reason = str(state.reason or "disabled by managed node profile")
            elif state.reason == "disabled by managed node profile":
                state.schedulable = True
                state.reason = ""

    def _clone_node_locked(self, state: NodeState, *, healthy: Optional[bool] = None) -> NodeState:
        return NodeState(
            node_instance_id=state.node_instance_id,
            node_id=state.node_id,
            control_addr=state.control_addr,
            capacity=state.capacity,
            queue_capacity=state.queue_capacity,
            tags=list(state.tags),
            profile_key=state.profile_key,
            managed_tags=list(state.managed_tags),
            capability_tags=list(state.capability_tags),
            legacy_node_tags=list(state.legacy_node_tags),
            profile_enabled=state.profile_enabled,
            profile_notes=state.profile_notes,
            version=state.version,
            python_version=state.python_version,
            metadata=dict(state.metadata),
            healthy=state.healthy if healthy is None else bool(healthy),
            last_seen_at=state.last_seen_at,
            metrics=NodeMetricsState(**vars(state.metrics)),
            services={k: NodeServiceState(**vars(v)) for k, v in state.services.items()},
            task_pools={k: NodeTaskPoolInfo(**vars(v)) for k, v in state.task_pools.items()},
            active_runtimes=list(state.active_runtimes),
            service_worker_capacity=state.service_worker_capacity,
            service_worker_used=state.service_worker_used,
            task_pool_worker_capacity=state.task_pool_worker_capacity,
            task_pool_worker_used=state.task_pool_worker_used,
            accept_service_deploy=state.accept_service_deploy,
            schedulable=state.schedulable,
            drain=state.drain,
            reason=state.reason,
            capability=NodeCapability.from_dict(state.capability.to_dict()),
        )

    @staticmethod
    def _with_failure_timestamps(
        *,
        incoming_services: Dict[str, NodeServiceState],
        incoming_task_pools: Dict[str, NodeTaskPoolInfo],
        previous_state: Optional[NodeState],
        now: datetime,
    ) -> Tuple[Dict[str, NodeServiceState], Dict[str, NodeTaskPoolInfo]]:
        previous_services = dict(getattr(previous_state, "services", {}) or {}) if previous_state is not None else {}
        previous_pools = dict(getattr(previous_state, "task_pools", {}) or {}) if previous_state is not None else {}

        services: Dict[str, NodeServiceState] = {}
        for service_id, svc in incoming_services.items():
            reason = str(getattr(svc, "stop_reason", "") or "").strip()
            previous = previous_services.get(service_id)
            previous_reason = str(getattr(previous, "stop_reason", "") or "").strip() if previous is not None else ""
            failure_at = getattr(svc, "failure_at", None)
            if reason:
                if failure_at is None and previous_reason == reason:
                    failure_at = getattr(previous, "failure_at", None)
                if failure_at is None:
                    failure_at = now
            else:
                failure_at = None
            svc.failure_at = failure_at
            services[service_id] = svc

        task_pools: Dict[str, NodeTaskPoolInfo] = {}
        for pool_id, pool in incoming_task_pools.items():
            reason = str(getattr(pool, "stop_reason", "") or getattr(pool, "failure_reason", "") or "").strip()
            previous = previous_pools.get(pool_id)
            previous_reason = (
                str(getattr(previous, "stop_reason", "") or getattr(previous, "failure_reason", "") or "").strip()
                if previous is not None
                else ""
            )
            failure_at = getattr(pool, "failure_at", None)
            if reason:
                if failure_at is None and previous_reason == reason:
                    failure_at = getattr(previous, "failure_at", None)
                if failure_at is None:
                    failure_at = now
            else:
                failure_at = None
            pool.stop_reason = reason
            pool.failure_reason = reason
            pool.failure_at = failure_at
            task_pools[pool_id] = pool

        return services, task_pools

    def update_node_profile(
        self,
        profile_key_or_endpoint: str,
        *,
        managed_tags: Optional[Iterable[str]] = None,
        add_tags: Optional[Iterable[str]] = None,
        remove_tags: Optional[Iterable[str]] = None,
        enabled: Optional[bool] = None,
        drain: Optional[bool] = None,
        notes: Optional[str] = None,
    ) -> NodeProfile:
        profile_key = normalize_node_profile_key(profile_key_or_endpoint)
        if not profile_key:
            raise ValueError("profile endpoint is required")
        with self._lock:
            profile = self._profiles.get(profile_key) or NodeProfile(profile_key=profile_key)
            tags = set(profile.managed_tags)
            if managed_tags is not None:
                tags = set(self._normalize_tags(managed_tags))
            tags.update(self._normalize_tags(add_tags or ()))
            tags.difference_update(self._normalize_tags(remove_tags or ()))
            profile.managed_tags = sorted(tags)
            if enabled is not None:
                profile.enabled = bool(enabled)
            if drain is not None:
                profile.drain = bool(drain)
            if notes is not None:
                profile.notes = str(notes or "")
            self._profiles[profile_key] = profile
            for state in self._nodes.values():
                if normalize_node_profile_key(str(state.control_addr or "")) == profile_key:
                    if bool(profile.enabled) and state.reason == "disabled by managed node profile":
                        state.schedulable = True
                        state.reason = ""
                    self._apply_profile_locked(state)
            self._save_profiles_locked()
            return NodeProfile(
                profile_key=profile.profile_key,
                managed_tags=list(profile.managed_tags),
                enabled=profile.enabled,
                drain=profile.drain,
                notes=profile.notes,
            )

    def update_node_profile_for_instance(
        self,
        node_instance_id: str,
        *,
        managed_tags: Optional[Iterable[str]] = None,
        add_tags: Optional[Iterable[str]] = None,
        remove_tags: Optional[Iterable[str]] = None,
        enabled: Optional[bool] = None,
        drain: Optional[bool] = None,
        notes: Optional[str] = None,
    ) -> NodeProfile:
        normalized_instance_id = str(node_instance_id or "").strip()
        if not normalized_instance_id:
            raise ValueError("node_instance_id is required")
        with self._lock:
            state = self._nodes.get(normalized_instance_id)
            if state is None:
                raise KeyError("node not found")
            profile_key = normalize_node_profile_key(str(state.control_addr or ""))
        return self.update_node_profile(
            profile_key,
            managed_tags=managed_tags,
            add_tags=add_tags,
            remove_tags=remove_tags,
            enabled=enabled,
            drain=drain,
            notes=notes,
        )

    def _fence_instance_locked(
        self,
        state: NodeState,
        *,
        now: Optional[datetime] = None,
        reason: str = "",
    ) -> None:
        """Deprecated compatibility hook.

        InfoCenter is registry-only and must not issue NodeControl reset/exit
        directives. Lost/replaced instances are represented by discovery state.
        """
        del state, now, reason
        return

    def _fence_if_stale_locked(self, state: NodeState, *, now: Optional[datetime] = None) -> None:
        current_time = now or utc_now()
        if self._node_is_stale_locked(state, now=current_time):
            state.healthy = False
            state.schedulable = False
            state.reason = str(state.reason or "node heartbeat timeout")

    def _reindex_node_services_locked(self, state: NodeState, *, previous_names: Optional[Iterable[str]] = None) -> None:
        node_key = str(getattr(state, "node_instance_id", "") or "").strip()
        if not node_key:
            return
        self._remove_node_services_index_locked(state, previous_names=previous_names)
        current_names = {
            str(svc.service_name or "").strip()
            for svc in state.services.values()
            if str(svc.service_name or "").strip()
        }
        for normalized_name in current_names:
            self._services_by_name.setdefault(normalized_name, set()).add(node_key)

    def _remove_node_services_index_locked(self, state: NodeState, *, previous_names: Optional[Iterable[str]] = None) -> None:
        node_key = str(getattr(state, "node_instance_id", "") or "").strip()
        if not node_key:
            return
        names = previous_names
        if names is None:
            names = (
                str(svc.service_name or "").strip()
                for svc in state.services.values()
                if str(svc.service_name or "").strip()
            )
        for name in names:
            normalized_name = str(name or "").strip()
            if not normalized_name:
                continue
            members = self._services_by_name.get(normalized_name)
            if members is None:
                continue
            members.discard(node_key)
            if not members:
                self._services_by_name.pop(normalized_name, None)

    def is_instance_fenced(self, node_instance_id: str) -> bool:
        del node_instance_id
        return False

    def fenced_instance_reason(self, node_instance_id: str) -> str:
        del node_instance_id
        return ""

    def _data_ref_is_expired_locked(self, entry: DataRegistryEntry, *, now: Optional[datetime] = None) -> bool:
        current_time = now or utc_now()
        ttl_sec = max(1, int(entry.ttl_sec or 0))
        return (current_time - entry.last_at).total_seconds() > float(ttl_sec)

    def _prune_expired_data_refs_locked(self, *, now: Optional[datetime] = None) -> None:
        current_time = now or utc_now()
        expired = [ref_id for ref_id, entry in self._data_refs.items() if self._data_ref_is_expired_locked(entry, now=current_time)]
        for ref_id in expired:
            self._data_refs.pop(ref_id, None)

    def _prune_replaced_nodes_locked(
        self,
        *,
        node_instance_id: str,
        node_id: str,
        control_addr: str,
        now: Optional[datetime] = None,
    ) -> None:
        del now
        del node_id
        normalized_instance_id = str(node_instance_id or "").strip()
        normalized_control_addr = str(control_addr or "").strip()
        if not normalized_instance_id or not normalized_control_addr:
            return
        incoming_profile_key = normalize_node_profile_key(normalized_control_addr)
        replaced_keys = [
            key
            for key, state in self._nodes.items()
            if key != normalized_instance_id
            and (
                str(state.control_addr or "").strip() == normalized_control_addr
                or (
                    incoming_profile_key
                    and normalize_node_profile_key(str(state.control_addr or "")) == incoming_profile_key
                )
            )
        ]
        for key in replaced_keys:
            old_state = self._nodes.pop(key, None)
            if old_state is not None:
                self._remove_node_services_index_locked(old_state)

    def control_addr_conflicting_instances(
        self,
        *,
        node_instance_id: str,
        control_addr: str,
    ) -> List[NodeState]:
        normalized_instance_id = str(node_instance_id or "").strip()
        normalized_control_addr = str(control_addr or "").strip()
        if not normalized_instance_id or not normalized_control_addr:
            return []
        incoming_profile_key = normalize_node_profile_key(normalized_control_addr)
        with self._lock:
            current = self._nodes.get(normalized_instance_id)
            if current is not None and normalize_node_profile_key(current.control_addr) == incoming_profile_key:
                return []
            out = [
                self._clone_node_locked(state)
                for key, state in self._nodes.items()
                if key != normalized_instance_id
                and (
                    str(state.control_addr or "").strip() == normalized_control_addr
                    or (
                        incoming_profile_key
                        and normalize_node_profile_key(str(state.control_addr or "")) == incoming_profile_key
                    )
                )
            ]
        return out

    def _validate_startup_service_names_locked(
        self,
        *,
        node_instance_id: str,
        control_addr: str,
        metadata: Dict[str, str],
        services: Dict[str, NodeServiceState],
        now: datetime,
    ) -> None:
        if not _coerce_bool((metadata or {}).get("startup_service"), default=False):
            return
        incoming_by_name: Dict[str, NodeServiceState] = {}
        for svc in services.values():
            if not is_conflict_scope(healthy=True, service_status=int(svc.status or 0)):
                continue
            name = str(svc.service_name or "").strip()
            if not name:
                continue
            existing = incoming_by_name.get(name)
            if existing is not None and str(existing.service_id or "").strip() != str(svc.service_id or "").strip():
                raise ValueError(f"startup service_name already exists in node registration: {name}")
            incoming_by_name[name] = svc
        if not incoming_by_name:
            return
        replaced_keys: List[str] = []
        for key, state in self._nodes.items():
            if key == node_instance_id or not self._node_is_healthy_locked(state, now=now):
                continue
            for svc in state.services.values():
                if not is_conflict_scope(healthy=True, service_status=int(svc.status or 0)):
                    continue
                name = str(svc.service_name or "").strip()
                incoming = incoming_by_name.get(name)
                if incoming is None:
                    continue
                if not _coerce_bool(state.metadata.get("startup_service"), default=False):
                    raise ValueError(f"startup service_name already exists: {name}")
                incoming_endpoint = _startup_service_endpoint(
                    NodeState(
                        node_instance_id=node_instance_id,
                        node_id="",
                        control_addr=control_addr,
                        capacity=1,
                        queue_capacity=1,
                        services={str(incoming.service_id or ""): incoming},
                    ),
                    incoming,
                )
                existing_endpoint = _startup_service_endpoint(state, svc)
                if not _endpoint_matches(incoming_endpoint, existing_endpoint):
                    raise ValueError(f"startup service_name already exists: {name}")
                replaced_keys.append(key)
                break
        for key in replaced_keys:
            old_state = self._nodes.pop(key, None)
            if old_state is not None:
                self._remove_node_services_index_locked(old_state)
                self._fence_instance_locked(old_state, reason="startup service endpoint replaced")

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
        accept_service_deploy: bool = True,
        python_version: str = "",
        capability: Optional[NodeCapability] = None,
    ) -> NodeState:
        now = utc_now()
        normalized_instance_id = str(node_instance_id or node_id or "").strip()
        if not normalized_instance_id:
            raise ValueError("node_instance_id is required")
        with self._lock:
            self._prune_replaced_nodes_locked(
                node_instance_id=normalized_instance_id,
                node_id=node_id,
                control_addr=control_addr,
                now=now,
            )
            incoming_metadata = dict(metadata or {})
            incoming_services = dict(services or {})
            self._validate_startup_service_names_locked(
                node_instance_id=normalized_instance_id,
                control_addr=control_addr,
                metadata=incoming_metadata,
                services=incoming_services,
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
            incoming_services, incoming_task_pools = self._with_failure_timestamps(
                incoming_services=incoming_services,
                incoming_task_pools=dict(task_pools or {}),
                previous_state=state,
                now=now,
            )
            state.node_instance_id = normalized_instance_id
            state.node_id = str(node_id or state.node_id or "").strip() or normalized_instance_id
            state.control_addr = control_addr
            state.capacity = max(1, capacity)
            state.queue_capacity = max(1, queue_capacity)
            state.legacy_node_tags = self._normalize_tags(tags or ())
            state.version = str(version or "")
            state.python_version = str(python_version or state.python_version or "").strip()
            state.metadata = incoming_metadata
            state.healthy = True
            state.last_seen_at = now
            previous_service_names = {
                str(svc.service_name or "").strip()
                for svc in state.services.values()
                if str(svc.service_name or "").strip()
            }
            state.services = incoming_services
            state.task_pools = incoming_task_pools
            state.active_runtimes = [str(x).strip() for x in (active_runtimes or []) if str(x).strip()]
            state.service_worker_capacity = max(0, int(service_worker_capacity or 0))
            state.service_worker_used = max(0, min(int(service_worker_used or 0), state.service_worker_capacity or int(service_worker_used or 0)))
            state.task_pool_worker_capacity = max(0, int(task_pool_worker_capacity or 0))
            state.task_pool_worker_used = max(0, min(int(task_pool_worker_used or 0), state.task_pool_worker_capacity or int(task_pool_worker_used or 0)))
            state.accept_service_deploy = bool(accept_service_deploy)
            if capability is not None:
                state.capability = capability
            self._apply_profile_locked(state, registration_tags=tags)
            self._reindex_node_services_locked(state, previous_names=previous_service_names)
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
            instance_ids = [
                str(entry.node_instance_id or "").strip(),
                *[
                    str(item.get("node_instance_id", "") or "").strip()
                    for item in entry.replicas
                    if str(item.get("node_instance_id", "") or "").strip()
                ],
            ]
            del instance_ids
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
            accept_service_deploy=_coerce_bool(metadata.get("accept_service_deploy"), default=True),
            python_version=metadata.get("python_version", ""),
            capability=NodeCapability.from_dict(getattr(request, "capability", None) and {
                "supported_modes": list(getattr(request.capability, "supported_modes", []) or []),
                "supports_raw_bytes_payload": bool(
                    getattr(
                        request.capability,
                        "supports_raw_bytes_payload",
                        getattr(request.capability, "supports_transport_payload_bytes", False),
                    )
                ),
                "supports_http_raw_bytes_body": bool(
                    getattr(
                        request.capability,
                        "supports_http_raw_bytes_body",
                        getattr(request.capability, "supports_http_bytes_transport", False),
                    )
                ),
                "max_control_send_bytes": int(getattr(request.capability, "max_control_send_bytes", 0) or 0),
                "max_control_recv_bytes": int(getattr(request.capability, "max_control_recv_bytes", 0) or 0),
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
        accept_service_deploy: Optional[bool] = None,
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
                state = NodeState(
                    node_instance_id=normalized_instance_id,
                    node_id=node_id,
                    control_addr="",
                    capacity=1,
                    queue_capacity=1,
                    python_version=str(python_version or "").strip(),
                )
                self._nodes[normalized_instance_id] = state
            state.node_instance_id = normalized_instance_id
            state.node_id = str(node_id or state.node_id or "").strip() or normalized_instance_id
            state.healthy = bool(healthy)
            state.last_seen_at = now
            if metrics is not None:
                state.metrics = metrics
            if metadata is not None:
                state.metadata = dict(metadata or {})
            previous_service_names = {
                str(svc.service_name or "").strip()
                for svc in state.services.values()
                if str(svc.service_name or "").strip()
            }
            inventory_updated = services is not None or task_pools is not None
            if inventory_updated:
                incoming_services, incoming_task_pools = self._with_failure_timestamps(
                    incoming_services=dict(state.services if services is None else services),
                    incoming_task_pools=dict(state.task_pools if task_pools is None else task_pools),
                    previous_state=state,
                    now=now,
                )
                if services is not None:
                    state.services = incoming_services
                if task_pools is not None:
                    state.task_pools = incoming_task_pools
            if python_version:
                state.python_version = str(python_version).strip()
            if active_runtimes is not None:
                state.active_runtimes = [str(x).strip() for x in active_runtimes if str(x).strip()]
            if inventory_updated:
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
            if accept_service_deploy is not None:
                state.accept_service_deploy = bool(accept_service_deploy)
            if capability is not None:
                state.capability = capability
            self._apply_profile_locked(state)
            if services is not None:
                self._reindex_node_services_locked(state, previous_names=previous_service_names)
            return state

    def heartbeat(self, request: pb2.HeartbeatNodeRequest) -> Optional[NodeState]:
        # Legacy protobuf heartbeats have no explicit inventory_included flag.
        # Treat an empty services list as a lightweight lease heartbeat so an
        # older/lightweight node heartbeat cannot wipe the last full inventory.
        services = self._parse_services(request.services) if len(request.services) > 0 else None
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
            services=services,
            capability=NodeCapability.from_dict(getattr(request, "capability", None) and {
                "supported_modes": list(getattr(request.capability, "supported_modes", []) or []),
                "supports_raw_bytes_payload": bool(
                    getattr(
                        request.capability,
                        "supports_raw_bytes_payload",
                        getattr(request.capability, "supports_transport_payload_bytes", False),
                    )
                ),
                "supports_http_raw_bytes_body": bool(
                    getattr(
                        request.capability,
                        "supports_http_raw_bytes_body",
                        getattr(request.capability, "supports_http_bytes_transport", False),
                    )
                ),
                "max_control_send_bytes": int(getattr(request.capability, "max_control_send_bytes", 0) or 0),
                "max_control_recv_bytes": int(getattr(request.capability, "max_control_recv_bytes", 0) or 0),
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
                owner_client_id=str(getattr(item, "owner_client_id", "") or ""),
                code_version=str(getattr(item, "code_version", "") or ""),
                entry_module=str(getattr(item, "entry_module", "") or ""),
                entry_callable=str(getattr(item, "entry_callable", "") or ""),
                serialization_mode=str(getattr(item, "serialization_mode", "") or ""),
                worker_count=max(0, int(item.worker_count)),
                alive_workers=max(0, int(item.alive_workers)),
                in_flight=max(0, int(item.in_flight)),
                lease_expire_at=ts_to_dt(item.lease_expire_at),
                http_base_url=item.http_base_url,
                stop_reason=str(getattr(item, "stop_reason", "") or ""),
            )
        return out

    def mark_node_lost(self, node_instance_id: str, *, reason: str = "") -> NodeState:
        now = utc_now()
        with self._lock:
            state = self._nodes.get(node_instance_id)
            if state is None:
                raise KeyError("node not found")
            previous_service_names = {
                str(svc.service_name or "").strip()
                for svc in state.services.values()
                if str(svc.service_name or "").strip()
            }
            self._fence_instance_locked(state, now=now, reason=str(reason or "node lost"))
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
                    owner_client_id=str(getattr(svc, "owner_client_id", "") or ""),
                    code_version=str(getattr(svc, "code_version", "") or ""),
                    entry_module=str(getattr(svc, "entry_module", "") or ""),
                    entry_callable=str(getattr(svc, "entry_callable", "") or ""),
                    serialization_mode=str(getattr(svc, "serialization_mode", "") or ""),
                    worker_count=max(0, int(svc.worker_count)),
                    alive_workers=0,
                    in_flight=0,
                    lease_expire_at=now,
                    http_base_url=svc.http_base_url,
                    stop_reason=str(svc.stop_reason or state.reason or "node lost"),
                    failure_at=getattr(svc, "failure_at", None) or now,
                )
            state.services = degraded
            self._reindex_node_services_locked(state, previous_names=previous_service_names)
            return self._clone_node_locked(state, healthy=False)

    def list_service_routes(
        self,
        *,
        service_name: str,
        healthy_only: bool,
        limit: int,
        route_scope: str = "call",
    ) -> List[Dict[str, object]]:
        now = utc_now()
        name_filter = service_name.strip()
        normalized_scope = str(route_scope or "call").strip().lower()
        effective_limit = max(1, int(limit))
        with self._lock:
            candidate_node_ids = (
                list(self._services_by_name.get(name_filter, ()))
                if name_filter
                else list(self._nodes.keys())
            )
            snapshots: List[tuple[str, str, bool, NodeState, NodeServiceState]] = []
            for node_instance_id in candidate_node_ids:
                state = self._nodes.get(node_instance_id)
                if state is None:
                    continue
                self._fence_if_stale_locked(state, now=now)
                is_healthy = self._node_is_healthy_locked(state, now=now)
                if healthy_only and not is_healthy:
                    continue
                for svc in state.services.values():
                    if name_filter and svc.service_name != name_filter:
                        continue
                    snapshots.append((str(svc.service_name or ""), str(svc.service_id or ""), is_healthy, state, svc))
        out: List[Dict[str, object]] = []
        for _svc_name, _svc_id, is_healthy, state, svc in snapshots:
            effective_status, effective_alive, effective_in_flight, effective_lease_expire_at, stale, status_text = (
                self._effective_service_state_locked(state, svc, now=now)
            )
            if healthy_only:
                if normalized_scope == "call":
                    readiness = str(getattr(svc, "readiness", "") or "").strip().lower()
                    explicit_resource_health = str(getattr(svc, "resource_health", "") or "").strip().lower()
                    explicit_status_text = str(getattr(svc, "status_text", "") or "").strip().upper()
                    explicit_degraded = (
                        explicit_resource_health in {"degraded", "failed", "stopped", "node_lost"}
                        or explicit_status_text == "DEGRADED"
                        or bool(getattr(svc, "degraded", False))
                        or bool(str(getattr(svc, "stop_reason", "") or "").strip())
                    )
                    if (
                        not is_call_route(healthy=is_healthy, service_status=effective_status, node_drain=bool(state.drain))
                        or explicit_degraded
                        or (readiness and readiness != "ready")
                    ):
                        continue
                elif normalized_scope == "owner_command":
                    if not is_owner_target(healthy=is_healthy, service_status=effective_status):
                        continue
                elif normalized_scope == "exclusive_check":
                    if not is_conflict_scope(healthy=is_healthy, service_status=effective_status):
                        continue
                elif not is_call_route(healthy=is_healthy, service_status=effective_status, node_drain=False):
                    continue
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
            resource_health = str(getattr(svc, "resource_health", "") or "")
            if not resource_health:
                if int(effective_status) == int(pb2.SERVICE_STATUS_STOPPED):
                    resource_health = "stopped"
                else:
                    resource_health = "running"
            reported_status_text = str(getattr(svc, "status_text", "") or status_text or "")
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
                    "service_status": effective_status,
                    "status_text": reported_status_text,
                    "resource_health": resource_health,
                    "readiness": str(getattr(svc, "readiness", "") or ""),
                    "readiness_reason": str(getattr(svc, "readiness_reason", "") or ""),
                    "create_stage": str(getattr(svc, "create_stage", "") or ""),
                    "operation_id": str(getattr(svc, "operation_id", "") or ""),
                    "operation_updated_at": getattr(svc, "operation_updated_at", None),
                    "stop_reason": str(getattr(svc, "stop_reason", "") or ""),
                    "failure_at": getattr(svc, "failure_at", None),
                    "policy_id": str(svc.policy_id or "").strip().lower() or "default_safe",
                    "owner_client_id": str(getattr(svc, "owner_client_id", "") or ""),
                    "code_version": str(getattr(svc, "code_version", "") or ""),
                    "entry_module": str(getattr(svc, "entry_module", "") or ""),
                    "entry_callable": str(getattr(svc, "entry_callable", "") or ""),
                    "serialization_mode": str(getattr(svc, "serialization_mode", "") or ""),
                    "node_instance_id": state.node_instance_id,
                    "node_id": state.node_id,
                    "control_addr": state.control_addr,
                    "node_healthy": is_healthy,
                    "node_schedulable": bool(state.schedulable),
                    "node_drain": bool(state.drain),
                    "accept_service_deploy": bool(getattr(state, "accept_service_deploy", True)),
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
        if len(out) <= effective_limit:
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
            return out[:effective_limit]
        return heapq.nsmallest(
            effective_limit,
            out,
            key=lambda x: (
                x["service_name"],
                not x["node_healthy"],
                int(x["status"] != pb2.SERVICE_STATUS_RUNNING),
                float(x.get("predicted_busy", 0.0) or 0.0),
                int(x["in_flight"]),
                -int(x.get("alive_workers", 0) or 0),
                x["node_id"],
                x["service_id"],
            ),
        )

    def list_nodes(self, *, healthy_only: bool, tags: Iterable[str], limit: int) -> List[NodeState]:
        now = utc_now()
        filter_tags = set(tags)
        effective_limit = max(1, int(limit))
        with self._lock:
            ranked: List[tuple[tuple[object, ...], NodeState, bool]] = []
            for state in self._nodes.values():
                self._fence_if_stale_locked(state, now=now)
                is_healthy = self._node_is_healthy_locked(state, now=now)
                if healthy_only and not is_healthy:
                    continue
                if filter_tags and not filter_tags.issubset(set(state.tags)):
                    continue
                ranked.append(
                    (
                        (
                            not is_healthy,
                            not bool(state.schedulable),
                            bool(state.drain),
                            -int(state.service_worker_available()),
                        ),
                        state,
                        is_healthy,
                    )
                )
            selected = (
                ranked
                if len(ranked) <= effective_limit
                else heapq.nsmallest(effective_limit, ranked, key=lambda item: item[0])
            )
            selected.sort(key=lambda item: item[0])
            return [self._clone_node_locked(state, healthy=is_healthy) for _key, state, is_healthy in selected]

    def list_selected_nodes(
        self,
        *,
        healthy_only: bool,
        tags: Iterable[str],
        limit: int,
        node_ids: Iterable[str] = (),
        node_instance_ids: Iterable[str] = (),
    ) -> List[NodeState]:
        now = utc_now()
        filter_tags = set(tags)
        requested_instance_ids = [str(value or "").strip() for value in node_instance_ids if str(value or "").strip()]
        requested_node_ids = [str(value or "").strip() for value in node_ids if str(value or "").strip()]
        if not requested_instance_ids and not requested_node_ids:
            return self.list_nodes(healthy_only=healthy_only, tags=tags, limit=limit)
        with self._lock:
            out: List[NodeState] = []
            if requested_instance_ids:
                for node_instance_id in requested_instance_ids:
                    state = self._nodes.get(node_instance_id)
                    if state is None:
                        continue
                    self._fence_if_stale_locked(state, now=now)
                    is_healthy = self._node_is_healthy_locked(state, now=now)
                    if healthy_only and not is_healthy:
                        continue
                    if filter_tags and not filter_tags.issubset(set(state.tags)):
                        continue
                    out.append(self._clone_node_locked(state, healthy=is_healthy))
                return out[: max(1, limit)]

            node_id_to_instances: Dict[str, List[NodeState]] = {}
            for state in self._nodes.values():
                node_id = str(state.node_id or "").strip()
                if not node_id or node_id not in requested_node_ids:
                    continue
                self._fence_if_stale_locked(state, now=now)
                is_healthy = self._node_is_healthy_locked(state, now=now)
                if healthy_only and not is_healthy:
                    continue
                if filter_tags and not filter_tags.issubset(set(state.tags)):
                    continue
                node_id_to_instances.setdefault(node_id, []).append(self._clone_node_locked(state, healthy=is_healthy))

            duplicates = sorted(node_id for node_id, items in node_id_to_instances.items() if len(items) > 1)
            if duplicates:
                dup_list = sorted(duplicates)
                raise RuntimeError(
                    f"requested node_ids are ambiguous because multiple live node instances share the same node_id: {dup_list}; "
                    "please select by node_instance_ids instead"
                )
            for node_id in requested_node_ids:
                item = node_id_to_instances.get(node_id)
                if item:
                    out.append(item[0])
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
            self._apply_profile_locked(state)
            return self._clone_node_locked(state)
