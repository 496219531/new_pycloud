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
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

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
        rows = list(
            self._source.list_service_routes(
                service_name=name,
                healthy_only=True,
                limit=self.route_limit,
            )
        )
        snapshot = ServiceRouteSnapshot(
            service_name=name,
            routes=rows,
            refreshed_at=datetime.now(timezone.utc),
        )
        with self._lock:
            if force or name not in self._snapshots or rows:
                self._snapshots[name] = snapshot
        return rows

    def get_routes(self, service_name: str) -> Sequence[InfoCenterServiceRoute]:
        name = str(service_name or "").strip()
        if not name:
            raise ValueError("service_name is required")
        with self._lock:
            snapshot = self._snapshots.get(name)
        if snapshot is None:
            return list(self.refresh(name, force=True))
        if not snapshot.routes:
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
        candidates.sort(key=lambda route: (int(route.in_flight), -int(route.alive_workers), route.node_id, route.service_id))
        return candidates[0]

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
