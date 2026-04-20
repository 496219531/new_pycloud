from __future__ import annotations

"""Gateway route cache and lightweight breaker state."""

import threading
import time
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Set, Tuple

from pycloud_parallel.controlplane.infocenter_client import InfoCenterServiceRoute
from pycloud_parallel.controlplane.gateway_source import RouteSource
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
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


@dataclass
class ServiceRouteSnapshot:
    service_name: str
    routes: List[InfoCenterServiceRoute] = field(default_factory=list)
    refreshed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class GatewayRouteCache:
    def __init__(
        self,
        *,
        source: RouteSource,
        refresh_interval_sec: float = 3.0,
        failure_threshold: int = 3,
        open_sec: float = 5.0,
        route_limit: int = 500,
    ) -> None:
        self._source = source
        self.refresh_interval_sec = max(0.2, float(refresh_interval_sec))
        self.failure_threshold = max(1, int(failure_threshold))
        self.open_sec = max(0.1, float(open_sec))
        self.route_limit = max(1, int(route_limit))

        self._lock = threading.Lock()
        self._snapshots: Dict[str, ServiceRouteSnapshot] = {}
        self._local_state: Dict[Tuple[str, str], CandidateBreakerState] = {}
        self._refresh_events: Dict[str, threading.Event] = {}
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._round_robin_counter: int = 0

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._refresh_loop,
                name="gateway-route-cache",
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

    def refresh(self, service_name: str, *, force: bool = False) -> Sequence[InfoCenterServiceRoute]:
        name = str(service_name or "").strip()
        if not name:
            raise ValueError("service_name is required")
        rows: List[InfoCenterServiceRoute] = []
        error: Optional[Exception] = None
        try:
            rows = list(
                self._source.list_service_routes(
                    service_name=name,
                    healthy_only=True,
                    limit=self.route_limit,
                )
            )
        except Exception as exc:
            error = exc
        snapshot = ServiceRouteSnapshot(
            service_name=name,
            routes=rows,
            refreshed_at=datetime.now(timezone.utc),
        )
        with self._lock:
            if error is None and (force or name not in self._snapshots or rows):
                self._snapshots[name] = snapshot
            valid_ids = {route.service_id for route in rows}
            for key in list(self._local_state.keys()):
                svc_name, svc_id = key
                if svc_name != name:
                    continue
                if valid_ids and svc_id not in valid_ids:
                    self._local_state.pop(key, None)
                if not valid_ids:
                    self._local_state.pop(key, None)
            event = self._refresh_events.pop(name, None)
            if event is not None:
                event.set()
        if error is not None:
            raise error
        return rows

    def get_routes(self, service_name: str) -> Sequence[InfoCenterServiceRoute]:
        name = str(service_name or "").strip()
        if not name:
            raise ValueError("service_name is required")
        return list(self._coalesced_refresh(name))

    def _coalesced_refresh(self, service_name: str) -> Sequence[InfoCenterServiceRoute]:
        name = str(service_name or "").strip()
        if not name:
            raise ValueError("service_name is required")
        with self._lock:
            snapshot = self._snapshots.get(name)
            if snapshot is not None and snapshot.routes:
                return list(snapshot.routes)
            event = self._refresh_events.get(name)
            if event is None:
                event = threading.Event()
                self._refresh_events[name] = event
                do_refresh = True
            else:
                do_refresh = False
        if do_refresh:
            return list(self.refresh(name, force=True))
        event.wait(timeout=max(0.1, self.refresh_interval_sec))
        with self._lock:
            snapshot = self._snapshots.get(name)
            if snapshot is not None:
                return list(snapshot.routes)
        return list(self.refresh(name, force=True))

    def snapshot_info(self, service_name: str) -> Dict[str, object]:
        name = str(service_name or "").strip()
        routes = list(self.get_routes(name))
        with self._lock:
            snapshot = self._snapshots.get(name)
            if snapshot is not None:
                routes = list(snapshot.routes)
        return {
            "service_name": name,
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
    ) -> InfoCenterServiceRoute:
        name = str(service_name or "").strip()
        if force_refresh:
            routes = list(self.refresh(name, force=True))
        else:
            routes = list(self.get_routes(name))
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
        normalized_strategy, profile = resolve_service_strategy(strategy)
        if normalized_strategy == "round_robin":
            with self._lock:
                rr = self._round_robin_counter
                self._round_robin_counter += 1
            ordered = sorted(candidates, key=lambda route: (str(route.node_instance_id or route.node_id or route.control_addr or ""), str(route.service_id)))
            return ordered[rr % len(ordered)]
        if normalized_strategy == "least_inflight":
            candidates.sort(key=self._route_sort_key)
            best_key = self._route_sort_key(candidates[0])
            top_tier = [route for route in candidates if self._route_sort_key(route) == best_key]
            with self._lock:
                rr = self._round_robin_counter
                self._round_robin_counter += 1
            return top_tier[rr % len(top_tier)]
        recent_failures: Dict[str, int] = {}
        scheduler_candidates: List[SchedulerCandidate] = []
        for route in candidates:
            local_state = self._local_state.get((name, route.service_id))
            recent_failures[str(route.service_id)] = int(getattr(local_state, "consecutive_failures", 0) or 0)
            scheduler_candidates.append(
                SchedulerCandidate(
                    id=str(route.service_id),
                    kind="service",
                    node_id=str(route.node_id or ""),
                    node_instance_id=str(route.node_instance_id or route.node_id or route.control_addr or ""),
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
            rr = self._round_robin_counter
            self._round_robin_counter += 1
        selected = select_one_candidate(
            scheduler_candidates,
            profile=profile or SERVICE_DEFAULT,
            state=SchedulerState(recent_submit_failures=recent_failures),
            round_robin_counter=rr,
        )
        for route in candidates:
            if str(route.service_id) == str(selected.id):
                if self.before_probe(route):
                    return route
                break
        candidates.sort(key=self._route_sort_key)
        best_key = self._route_sort_key(candidates[0])
        top_tier = [route for route in candidates if self._route_sort_key(route) == best_key]
        return top_tier[rr % len(top_tier)]

    @staticmethod
    def _predicted_busy(route: InfoCenterServiceRoute) -> float:
        value = float(getattr(route, "predicted_busy", 0.0) or 0.0)
        if math.isfinite(value) and value > 0.0:
            return value
        inflight = max(0, int(getattr(route, "in_flight", 0) or 0))
        alive_workers = max(1, int(getattr(route, "alive_workers", 0) or 0))
        return float(inflight) / float(alive_workers)

    @classmethod
    def _route_sort_key(cls, route: InfoCenterServiceRoute) -> Tuple[object, ...]:
        return (
            cls._predicted_busy(route),
            int(getattr(route, "in_flight", 0) or 0),
            -int(getattr(route, "alive_workers", 0) or 0),
            str(getattr(route, "node_instance_id", "") or getattr(route, "node_id", "") or getattr(route, "control_addr", "") or ""),
            str(getattr(route, "service_id", "") or ""),
        )

    def _route_available(self, service_name: str, service_id: str) -> bool:
        key = (service_name, service_id)
        with self._lock:
            state = self._local_state.get(key)
            if state is None:
                return True
            _state, allowed = candidate_allowed(state)
            return allowed

    def before_probe(self, route: InfoCenterServiceRoute) -> bool:
        key = (route.service_name, route.service_id)
        with self._lock:
            state = self._local_state.get(key)
            if state is None:
                state = CandidateBreakerState()
                self._local_state[key] = state
            return before_probe(state)

    def mark_success(self, route: InfoCenterServiceRoute) -> None:
        key = (route.service_name, route.service_id)
        with self._lock:
            state = self._local_state.get(key)
            if state is None:
                return
            mark_candidate_success(state)

    def mark_failure(self, route: InfoCenterServiceRoute, error: str) -> None:
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
