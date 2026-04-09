from __future__ import annotations

"""Gateway route cache and lightweight breaker state."""

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Set, Tuple

from pycloud_parallel.controlplane.client import InfoCenterServiceRoute
from pycloud_parallel.controlplane.gateway_source import RouteSource
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


@dataclass
class RouteLocalState:
    consecutive_failures: int = 0
    open_until_monotonic: float = 0.0
    last_error: str = ""


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
        self._local_state: Dict[Tuple[str, str], RouteLocalState] = {}
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
        # Sort by load first; use round-robin as final tiebreaker to spread
        # traffic when metrics are equal (e.g. all nodes idle with in_flight=0).
        with self._lock:
            rr = self._round_robin_counter
            self._round_robin_counter += 1
        candidates.sort(key=lambda route: (int(route.in_flight), -int(route.alive_workers)))
        # Among equal-load candidates, rotate selection via round-robin.
        min_in_flight = int(candidates[0].in_flight)
        min_alive = int(candidates[0].alive_workers)
        top_tier = [r for r in candidates if int(r.in_flight) == min_in_flight and int(r.alive_workers) == min_alive]
        return top_tier[rr % len(top_tier)]

    def _route_available(self, service_name: str, service_id: str) -> bool:
        key = (service_name, service_id)
        now = time.monotonic()
        with self._lock:
            state = self._local_state.get(key)
            if state is None:
                return True
            return now >= state.open_until_monotonic

    def mark_success(self, route: InfoCenterServiceRoute) -> None:
        key = (route.service_name, route.service_id)
        with self._lock:
            state = self._local_state.get(key)
            if state is None:
                return
            state.consecutive_failures = 0
            state.open_until_monotonic = 0.0
            state.last_error = ""

    def mark_failure(self, route: InfoCenterServiceRoute, error: str) -> None:
        key = (route.service_name, route.service_id)
        with self._lock:
            state = self._local_state.get(key)
            if state is None:
                state = RouteLocalState()
                self._local_state[key] = state
            state.consecutive_failures += 1
            state.last_error = str(error or "")
            if state.consecutive_failures >= self.failure_threshold:
                state.open_until_monotonic = time.monotonic() + self.open_sec
