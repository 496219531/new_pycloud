from __future__ import annotations

"""Route sources for Gateway service discovery."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol, Sequence

from pycloud_parallel.controlplane.client import InfoCenterClient, InfoCenterServiceRoute
from pycloud_parallel.controlplane.state import InfoCenterState


class RouteSource(Protocol):
    def list_service_routes(self, *, service_name: str, healthy_only: bool, limit: int) -> Sequence[InfoCenterServiceRoute]:
        """Return routes for a service name."""


@dataclass
class InProcessInfoCenterSource:
    state: InfoCenterState

    def list_service_routes(self, *, service_name: str, healthy_only: bool, limit: int) -> Sequence[InfoCenterServiceRoute]:
        rows = self.state.list_service_routes(
            service_name=service_name,
            healthy_only=healthy_only,
            limit=limit,
        )
        out = []
        for item in rows:
            dt = item["lease_expire_at"]
            if isinstance(dt, datetime):
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
            else:
                dt = datetime.now(timezone.utc)
            out.append(
                InfoCenterServiceRoute(
                    service_name=str(item.get("service_name", "")),
                    service_id=str(item.get("service_id", "")),
                    status=int(item.get("status", 0) or 0),
                    node_id=str(item.get("node_id", "")),
                    control_addr=str(item.get("control_addr", "")),
                    node_healthy=bool(item.get("node_healthy", False)),
                    worker_count=int(item.get("worker_count", 0) or 0),
                    alive_workers=int(item.get("alive_workers", 0) or 0),
                    in_flight=int(item.get("in_flight", 0) or 0),
                    lease_expire_at=dt,
                    http_base_url=str(item.get("http_base_url", "")),
                )
            )
        return out


@dataclass
class RemoteInfoCenterSource:
    target: str
    timeout_sec: float = 10.0

    def list_service_routes(self, *, service_name: str, healthy_only: bool, limit: int) -> Sequence[InfoCenterServiceRoute]:
        with InfoCenterClient(self.target, timeout_sec=self.timeout_sec) as client:
            return list(
                client.list_service_routes(
                    service_name=service_name,
                    healthy_only=healthy_only,
                    limit=limit,
                )
            )
