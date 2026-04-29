from __future__ import annotations

"""Discovery route cache extracted from controlplane client."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import threading
import time
from typing import Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlsplit

from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient, _node_instance_key_from_route, _route_sort_key
from pycloud_parallel.controlplane.scheduling_policy import is_call_route
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
    StrategyProfile,
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
        self._local_inflight: Dict[Tuple[str, str], int] = {}
        self._last_call_observation: Dict[str, Dict[str, object]] = {}
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
            valid_ids = {str(getattr(route, "service_id", "") or "") for route in rows}
            for key in list(self._local_state.keys()):
                svc_name, svc_id = key
                if svc_name == name and ((valid_ids and svc_id not in valid_ids) or not valid_ids):
                    self._local_state.pop(key, None)
            for key in list(self._local_inflight.keys()):
                svc_name, svc_id = key
                if svc_name == name and ((valid_ids and svc_id not in valid_ids) or not valid_ids):
                    self._local_inflight.pop(key, None)
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
        name = str(service_name or "").strip()
        routes = list(self.get_routes(name))
        with self._lock:
            snapshot = self._snapshots.get(name)
            route_cache_index = int(self._route_index.get(name, 0) or 0)
        return {
            "service_name": name,
            "refreshed_at": snapshot.refreshed_at.isoformat() if snapshot is not None else "",
            "route_count": len(routes),
            "route_cache_index": route_cache_index,
            "routes": routes,
            "last_call": self._last_call_info(name),
        }

    def record_call_observation(
        self,
        service_name: str,
        *,
        route_attempt_count: int,
        failed_route_count: int,
        last_failed_route_id: str = "",
        selected_route_id: str = "",
    ) -> None:
        name = str(service_name or "").strip()
        if not name:
            return
        with self._lock:
            self._last_call_observation[name] = {
                "route_attempt_count": int(route_attempt_count or 0),
                "failed_route_count": int(failed_route_count or 0),
                "last_failed_route_id": str(last_failed_route_id or ""),
                "selected_route_id": str(selected_route_id or ""),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

    def _last_call_info(self, service_name: str) -> Dict[str, object]:
        with self._lock:
            return dict(self._last_call_observation.get(str(service_name or "").strip(), {}))

    def select_route(
        self,
        service_name: str,
        *,
        exclude_service_ids: Optional[Set[str]] = None,
        force_refresh: bool = False,
        strategy: str = "predicted_busy",
    ):
        name = str(service_name or "").strip()
        routes = list(self.refresh(name, force=True)) if force_refresh else list(self.get_routes(name))
        excluded = exclude_service_ids or set()
        candidates = [
            route
            for route in routes
            if is_call_route(
                healthy=bool(route.node_healthy),
                service_status=int(route.status),
                node_drain=bool(getattr(route, "node_drain", False)),
            )
            and route.http_base_url
            and route.service_id not in excluded
        ]
        conflicts = self._conflicting_http_endpoint_keys(candidates)
        if conflicts:
            candidates = [
                route
                for route in candidates
                if self._http_endpoint_key(str(getattr(route, "http_base_url", "") or "")) not in conflicts
            ]
        normalized_strategy, profile = resolve_service_strategy(strategy)
        with self._lock:
            candidates = [
                route
                for route in candidates
                if self._route_available_locked(name, str(route.service_id))
            ]
            if not candidates:
                if conflicts:
                    details = ", ".join(sorted(conflicts))
                    raise RuntimeError(
                        f"no available route for service_name={name}; conflicting service HTTP endpoints across node instances: {details}"
                    )
                raise RuntimeError(f"no available route for service_name={name}")
            idx = self._route_index.get(name, 0)
            self._route_index[name] = idx + 1

            if normalized_strategy == "round_robin":
                candidates.sort(key=lambda route: (_node_instance_key_from_route(route), route.service_id))
                selected_route = candidates[idx % len(candidates)]
                self._reserve_route_locked(selected_route)
                return selected_route

            if normalized_strategy == "least_inflight":
                candidates.sort(
                    key=lambda route: (
                        self._local_inflight_count_locked(name, str(route.service_id)),
                        _route_sort_key(route, strategy=normalized_strategy),
                    )
                )
                best_local = self._local_inflight_count_locked(name, str(candidates[0].service_id))
                best_key = _route_sort_key(candidates[0], strategy=normalized_strategy)
                top_tier = [
                    route
                    for route in candidates
                    if self._local_inflight_count_locked(name, str(route.service_id)) == best_local
                    and _route_sort_key(route, strategy=normalized_strategy) == best_key
                ]
                selected_route = top_tier[idx % len(top_tier)]
                self._reserve_route_locked(selected_route)
                return selected_route

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
                        drain=bool(getattr(route, "node_drain", False)),
                        breaker_state=(local_state.state if local_state is not None else "closed"),
                        predicted_busy=float(getattr(route, "predicted_busy", 0.0) or 0.0),
                        node_inflight=int(getattr(route, "in_flight", 0) or 0),
                        alive_workers=max(1, int(getattr(route, "alive_workers", 0) or 1)),
                        worker_capacity=max(1, int(getattr(route, "worker_count", 0) or 1)),
                        credit=1,
                        recent_failures=recent_failures[str(route.service_id)],
                    )
                )
            route_profile = StrategyProfile(
                name=f"route_cache_{(profile or SERVICE_DEFAULT).name}",
                weights={"local_inflight": 8.0, **dict((profile or SERVICE_DEFAULT).weights)},
                tie_break=(profile or SERVICE_DEFAULT).tie_break,
                failure_penalty=(profile or SERVICE_DEFAULT).failure_penalty,
            )
            selected = select_one_candidate(
                scheduler_candidates,
                profile=route_profile,
                state=SchedulerState(
                    local_inflight_by_candidate=self._local_inflight_snapshot_locked(name, candidates),
                    recent_submit_failures=recent_failures,
                ),
                round_robin_counter=idx,
            )
            for route in candidates:
                if str(route.service_id) == str(selected.id) and self._before_probe_locked(route):
                    self._reserve_route_locked(route)
                    return route
            candidates.sort(key=lambda route: _route_sort_key(route, strategy="predicted_busy"))
            best_key = _route_sort_key(candidates[0], strategy="predicted_busy")
            top_tier = [route for route in candidates if _route_sort_key(route, strategy="predicted_busy") == best_key]
            selected_route = top_tier[idx % len(top_tier)]
            self._reserve_route_locked(selected_route)
            return selected_route

    @staticmethod
    def _http_endpoint_key(http_base_url: str) -> str:
        parsed = urlsplit(str(http_base_url or "").strip())
        if not parsed.scheme or not parsed.netloc:
            return ""
        return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"

    @classmethod
    def _conflicting_http_endpoint_keys(cls, routes: Sequence[object]) -> Set[str]:
        owners: Dict[str, Set[str]] = {}
        for route in routes:
            key = cls._http_endpoint_key(str(getattr(route, "http_base_url", "") or ""))
            if not key:
                continue
            node_key = _node_instance_key_from_route(route)
            if not node_key:
                continue
            owners.setdefault(key, set()).add(str(node_key))
        return {key for key, node_keys in owners.items() if len(node_keys) > 1}

    def _local_inflight_count_locked(self, service_name: str, service_id: str) -> int:
        return int(self._local_inflight.get((str(service_name or "").strip(), str(service_id or "").strip()), 0) or 0)

    def _local_inflight_count(self, service_name: str, service_id: str) -> int:
        with self._lock:
            return int(self._local_inflight.get((str(service_name or "").strip(), str(service_id or "").strip()), 0) or 0)

    def _local_inflight_snapshot(self, service_name: str, routes: Sequence[object]) -> Dict[str, int]:
        name = str(service_name or "").strip()
        with self._lock:
            return self._local_inflight_snapshot_locked(name, routes)

    def _local_inflight_snapshot_locked(self, service_name: str, routes: Sequence[object]) -> Dict[str, int]:
        name = str(service_name or "").strip()
        return {
            str(getattr(route, "service_id", "") or ""): int(
                self._local_inflight.get((name, str(getattr(route, "service_id", "") or "")), 0) or 0
            )
            for route in routes
        }

    def _reserve_route(self, route) -> None:
        key = (str(getattr(route, "service_name", "") or "").strip(), str(getattr(route, "service_id", "") or "").strip())
        if not key[0] or not key[1]:
            return
        with self._lock:
            self._reserve_route_locked(route)

    def _reserve_route_locked(self, route) -> None:
        key = (str(getattr(route, "service_name", "") or "").strip(), str(getattr(route, "service_id", "") or "").strip())
        if not key[0] or not key[1]:
            return
        self._local_inflight[key] = int(self._local_inflight.get(key, 0) or 0) + 1

    def _release_route(self, route) -> None:
        key = (str(getattr(route, "service_name", "") or "").strip(), str(getattr(route, "service_id", "") or "").strip())
        if not key[0] or not key[1]:
            return
        with self._lock:
            current = int(self._local_inflight.get(key, 0) or 0)
            if current <= 1:
                self._local_inflight.pop(key, None)
            else:
                self._local_inflight[key] = current - 1

    def release_route(self, route) -> None:
        self._release_route(route)

    def _route_available(self, service_name: str, service_id: str) -> bool:
        key = (service_name, service_id)
        with self._lock:
            return self._route_available_locked(str(service_name or "").strip(), str(service_id or "").strip())

    def _route_available_locked(self, service_name: str, service_id: str) -> bool:
        key = (service_name, service_id)
        state = self._local_state.get(key)
        if state is None:
            return True
        _state, allowed = candidate_allowed(state)
        return allowed

    def before_probe(self, route) -> bool:
        with self._lock:
            return self._before_probe_locked(route)

    def _before_probe_locked(self, route) -> bool:
        key = (route.service_name, route.service_id)
        state = self._local_state.get(key)
        if state is None:
            state = CandidateBreakerState()
            self._local_state[key] = state
        return before_probe(state)

    def mark_success(self, route) -> None:
        key = (route.service_name, route.service_id)
        with self._lock:
            state = self._local_state.get(key)
            if state is not None:
                mark_candidate_success(state)
        self._release_route(route)

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
                failure_threshold=1,
                cooldown_sec=self.open_sec,
                max_cooldown_sec=self.open_sec,
            )
        self._release_route(route)
