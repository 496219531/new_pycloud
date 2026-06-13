from __future__ import annotations

"""Route sources for Gateway service discovery."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import threading
from typing import Optional, Protocol, Sequence

from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient, InfoCenterServiceRoute
from pycloud_parallel.controlplane.infocenter_state import InfoCenterState


class RouteSource(Protocol):
    def list_service_routes(
        self,
        *,
        service_name: str,
        healthy_only: bool,
        limit: int,
        method: str = "",
    ) -> Sequence[InfoCenterServiceRoute]:
        """Return routes for a service name."""


@dataclass
class InProcessInfoCenterSource:
    state: InfoCenterState

    def list_service_routes(
        self,
        *,
        service_name: str,
        healthy_only: bool,
        limit: int,
        method: str = "",
    ) -> Sequence[InfoCenterServiceRoute]:
        rows = self.state.list_service_routes(
            service_name=service_name,
            healthy_only=healthy_only,
            limit=limit,
            method=method,
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
                    node_instance_id=str(item.get("node_instance_id", "") or item.get("node_id", "") or ""),
                    node_id=str(item.get("node_id", "")),
                    control_addr=str(item.get("control_addr", "")),
                    node_healthy=bool(item.get("node_healthy", False)),
                    worker_count=int(item.get("worker_count", 0) or 0),
                    alive_workers=int(item.get("alive_workers", 0) or 0),
                    in_flight=int(item.get("in_flight", 0) or 0),
                    lease_expire_at=dt,
                    http_base_url=str(item.get("http_base_url", "")),
                    reported_in_flight=int(item.get("reported_in_flight", 0) or 0),
                    received_count=int(item.get("received_count", 0) or 0),
                    returned_count=int(item.get("returned_count", 0) or 0),
                    ema_child_invoke_ms=float(item.get("ema_child_invoke_ms", 0.0) or 0.0),
                    ema_samples=int(item.get("ema_samples", 0) or 0),
                    predicted_busy=float(item.get("predicted_busy", 0.0) or 0.0),
                    policy_id=str(item.get("policy_id", "") or "default_safe"),
                    method_failures={
                        str(k): dict(v) if isinstance(v, dict) else {"reason": str(v)}
                        for k, v in dict(item.get("method_failures") or {}).items()
                    },
                )
            )
        return out


@dataclass
class RemoteInfoCenterSource:
    target: str
    timeout_sec: float = 10.0
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _client: Optional[InfoCenterClient] = field(default=None, init=False, repr=False)

    def list_service_routes(
        self,
        *,
        service_name: str,
        healthy_only: bool,
        limit: int,
        method: str = "",
    ) -> Sequence[InfoCenterServiceRoute]:
        with self._lock:
            client = self._client
            if client is None:
                client = InfoCenterClient(self.target, timeout_sec=self.timeout_sec)
                self._client = client
        try:
            return list(
                client.list_service_routes(
                    service_name=service_name,
                    healthy_only=healthy_only,
                    limit=limit,
                    method=method,
                )
            )
        except Exception:
            with self._lock:
                if self._client is not None:
                    try:
                        self._client.close()
                    except Exception:
                        pass
                    self._client = None
            raise
