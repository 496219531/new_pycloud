from __future__ import annotations

"""Discovery route cache extracted from controlplane client."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import threading
import time
from typing import Dict, List, Optional, Sequence, Set, Tuple

from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient, _node_instance_key_from_route, _route_sort_key
from pycloud_parallel.execution.failover import (
    CandidateBreakerState,
    ROUTE_UNAVAILABLE,
    candidate_allowed,
    before_probe,
    mark_candidate_failure,
    mark_candidate_success,
)
from pycloud_parallel.execution.scheduler import (
    SERVICE_DEFAULT,
    resolve_service_strategy,
    SchedulerCandidate,
    SchedulerState,
    select_one_candidate,
)


@dataclass
class _ServiceRouteSnapshot:
    service_name: str
    routes: List[object] = field(default_factory=list)
    refreshed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class _DiscoveryRouteCache:
    def __init__(
        self,
        *,
        infocenter_target: str,
        timeout_sec: float = 10.0,
        refresh_interval_sec: float = 3.0,
        failure_threshold: int = 3,
        open_sec: float = 5.0,
        route_limit: int = 500,
    ) -> None:
        self.infocenter_target = str(infocenter_target or "").strip()
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.refresh_interval_sec = max(0.2, float(refresh_interval_sec))
        self.failure_threshold = max(1, int(failure_threshold))
        self.open_sec = max(0.1, float(open_sec))
        self.route_limit = max(1, int(route_limit))

        self._lock = threading.Lock()
        self._snapshots: Dict[str, _ServiceRouteSnapshot] = {}
        self._local_state: Dict[Tuple[str, str], CandidateBreakerState] = {}
        self._route_index: Dict[str, int] = {}
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._refresh_loop,
                name="discovery-route-cache",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=1.0)
        with self._lock:
            self._thread = None

    def _refresh_loop(self) -> None:
        while not self._stop_event.wait(self.refresh_interval_sec):
            with self._lock:
                service_names = list(self._snapshots.keys())
            for service_name in service_names:
                try:
                    self.refresh(service_name, force=True)
                except Exception:
                    continue

    def refresh(self, service_name: str, *, force: bool = False) -> Sequence[object]:
        name = str(service_name or "").strip()
        if not name:
            raise ValueError("service_name is required")
        with InfoCenterClient(self.infocenter_target, timeout_sec=self.timeout_sec) as client:
            rows = list(
                client.list_service_routes(
                    service_name=name,
                    healthy_only=True,
                    limit=self.route_limit,
                )
            )
        snapshot = _ServiceRouteSnapshot(
            service_name=name,
            routes=rows,
            refreshed_at=datetime.now(timezone.utc),
        )
        with self._lock:
            if force or name not in self._snapshots or rows:
                self._snapshots[name] = snapshot
        return rows

    def get_routes(self, service_name: str) -> Sequence[object]:
        name = str(service_name or "").strip()
        if not name:
            raise ValueError("service_name is required")
        with self._lock:
            snapshot = self._snapshots.get(name)
        if snapshot is None:
            return list(self.refresh(name, force=True))
        return list(snapshot.routes)

    def snapshot_info(self, service_name: str) -> Dict[str, object]:
        routes = list(self.get_routes(service_name))
        with self._lock:
            snapshot = self._snapshots.get(str(service_name or "").strip())
        return {
            "service_name": str(service_name or "").strip(),
            "refreshed_at": snapshot.refreshed_at.isoformat() if snapshot is not None else "",
            "route_count": len(routes),
            "routes": routes,
        }

    def select_route(
        self,
        service_name: str,
        *,
        exclude_service_ids: Optional[Set[str]] = None,
        force_refresh: bool = False,
        strategy: str = "predicted_busy",
    ):
        from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

        name = str(service_name or "").strip()
        routes = list(self.refresh(name, force=True)) if force_refresh else list(self.get_routes(name))
        excluded = exclude_service_ids or set()
        candidates = [
            route
            for route in routes
            if route.node_healthy
            and route.status == pb2.SERVICE_STATUS_RUNNING
            and route.http_base_url
            and route.service_id not in excluded
            and self._route_available(name, route.service_id)
        ]
        if not candidates:
            raise RuntimeError(f"no available route for service_name={name}")
        if strategy == "round_robin":
            candidates.sort(key=lambda route: (_node_instance_key_from_route(route), route.service_id))
            with self._lock:
                idx = self._route_index.get(name, 0)
                self._route_index[name] = idx + 1
            return candidates[idx % len(candidates)]
        normalized_strategy, profile = resolve_service_strategy(strategy)
        if normalized_strategy == "round_robin":
            candidates.sort(key=lambda route: (_node_instance_key_from_route(route), route.service_id))
            with self._lock:
                idx = self._route_index.get(name, 0)
                self._route_index[name] = idx + 1
            return candidates[idx % len(candidates)]
        if normalized_strategy == "least_inflight":
            candidates.sort(key=lambda route: _route_sort_key(route, strategy=normalized_strategy))
            return candidates[0]
        scheduler_candidates: List[SchedulerCandidate] = []
        recent_failures: Dict[str, int] = {}
        for route in candidates:
            local_state = self._local_state.get((name, route.service_id))
            recent_failures[str(route.service_id)] = int(getattr(local_state, "consecutive_failures", 0) or 0)
            scheduler_candidates.append(
                SchedulerCandidate(
                    id=str(route.service_id),
                    kind="service",
                    node_id=str(route.node_id or ""),
                    node_instance_id=_node_instance_key_from_route(route),
                    healthy=bool(route.node_healthy),
                    schedulable=bool(route.http_base_url),
                    drain=False,
                    breaker_state=(local_state.state if local_state is not None else "closed"),
                    predicted_busy=float(getattr(route, "predicted_busy", 0.0) or 0.0),
                    node_inflight=int(getattr(route, "in_flight", 0) or 0),
                    alive_workers=max(1, int(getattr(route, "alive_workers", 0) or 1)),
                    worker_capacity=max(1, int(getattr(route, "worker_count", 0) or 1)),
                    credit=1,
                    recent_failures=recent_failures[str(route.service_id)],
                )
            )
        with self._lock:
            idx = self._route_index.get(name, 0)
            self._route_index[name] = idx + 1
        selected = select_one_candidate(
            scheduler_candidates,
            profile=profile or SERVICE_DEFAULT,
            state=SchedulerState(recent_submit_failures=recent_failures),
            round_robin_counter=idx,
        )
        for route in candidates:
            if str(route.service_id) == str(selected.id):
                if self.before_probe(route):
                    return route
                break
        candidates.sort(key=lambda route: _route_sort_key(route, strategy="predicted_busy"))
        return candidates[0]

    def _route_available(self, service_name: str, service_id: str) -> bool:
        key = (service_name, service_id)
        with self._lock:
            state = self._local_state.get(key)
            if state is None:
                return True
            _state, allowed = candidate_allowed(state)
            return allowed

    def before_probe(self, route) -> bool:
        key = (route.service_name, route.service_id)
        with self._lock:
            state = self._local_state.get(key)
            if state is None:
                state = CandidateBreakerState()
                self._local_state[key] = state
            return before_probe(state)

    def mark_success(self, route) -> None:
        key = (route.service_name, route.service_id)
        with self._lock:
            state = self._local_state.get(key)
            if state is None:
                return
            mark_candidate_success(state)

    def mark_failure(self, route, error: str) -> None:
        key = (route.service_name, route.service_id)
        with self._lock:
            state = self._local_state.get(key)
            if state is None:
                state = CandidateBreakerState()
                self._local_state[key] = state
            mark_candidate_failure(
                state,
                failure_kind=ROUTE_UNAVAILABLE,
                error=RuntimeError(str(error or "")),
                failure_threshold=self.failure_threshold,
                cooldown_sec=self.open_sec,
                max_cooldown_sec=self.open_sec,
            )
