from __future__ import annotations

"""Shared execution session identity and snapshot models."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Mapping


SessionKind = Literal["service", "task_pool"]


@dataclass(frozen=True)
class SessionIdentity:
    kind: SessionKind
    session_id: str
    session_name: str
    owner_client_id: str
    session_token: str


@dataclass
class SessionLease:
    heartbeat_timeout_sec: int
    idle_ttl_sec: int
    created_at: datetime
    last_heartbeat_at: datetime
    lease_expire_at: datetime

    def is_expired(self, *, at: datetime | None = None) -> bool:
        reference = at or datetime.now(timezone.utc)
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=timezone.utc)
        return reference >= self.lease_expire_at

    def remaining_seconds(self, *, at: datetime | None = None) -> float:
        reference = at or datetime.now(timezone.utc)
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=timezone.utc)
        return max(0.0, (self.lease_expire_at - reference).total_seconds())


@dataclass
class SessionBinding:
    code_version: str
    worker_count: int
    executor_ready: bool = False
    managed_global_names: tuple[str, ...] = ()
    managed_globals_scope_dir: str = ""
    managed_globals_digest: str = ""


@dataclass(frozen=True)
class ExecutionReplicaSnapshot:
    kind: SessionKind
    node_instance_id: str
    node_id: str
    session_id: str
    session_name: str
    code_version: str
    worker_count: int
    alive: bool
    status: str
    lease_expire_at: datetime
    failure: str = ""


@dataclass(frozen=True)
class WorkerResourceSnapshot:
    worker_count: int
    alive_workers: int
    in_flight: int
    received_count: int
    returned_count: int
    requested_workers: int = 0
    busy_workers: int = 0
    queued: int = 0
    worker_pids: tuple[int, ...] = ()
    executor_generation: int = 0


@dataclass(frozen=True)
class ExecutionSessionStatus:
    kind: SessionKind
    replica_count: int
    alive_replica_count: int
    failed_replica_count: int
    alive: bool
    failed: bool
    failures: dict[str, str]
    last_heartbeat_at: datetime | None
    lease_expire_at: datetime | None
    replicas: dict[str, ExecutionReplicaSnapshot]


def build_execution_session_status(
    *,
    kind: SessionKind,
    replicas: Mapping[str, ExecutionReplicaSnapshot],
    failures: Mapping[str, str],
    failed: bool,
    closed: bool,
    leases: Mapping[str, SessionLease],
) -> ExecutionSessionStatus:
    """Build a shared client/control-plane session status view.

    This intentionally models only session identity/lease/replica liveness. It
    does not imply shared runtime call or discovery semantics between services
    and task pools.
    """

    normalized_replicas = dict(replicas)
    alive_replica_count = sum(1 for snapshot in normalized_replicas.values() if bool(snapshot.alive))
    normalized_failures = {
        node_instance_id: str(snapshot.failure or failures.get(node_instance_id, "") or "")
        for node_instance_id, snapshot in normalized_replicas.items()
        if str(snapshot.failure or failures.get(node_instance_id, "") or "").strip()
    }
    active_leases = [
        lease.lease_expire_at
        for node_instance_id, lease in leases.items()
        if bool(normalized_replicas.get(node_instance_id, None) and normalized_replicas[node_instance_id].alive)
    ]
    all_leases = [lease.lease_expire_at for lease in leases.values()]
    last_heartbeat_values = [lease.last_heartbeat_at for lease in leases.values()]
    effective_failed = bool(failed) or (bool(normalized_replicas) and alive_replica_count <= 0)
    alive = (not bool(closed)) and alive_replica_count > 0 and not effective_failed
    return ExecutionSessionStatus(
        kind=kind,
        replica_count=len(normalized_replicas),
        alive_replica_count=alive_replica_count,
        failed_replica_count=len(normalized_failures),
        alive=alive,
        failed=effective_failed,
        failures=normalized_failures,
        last_heartbeat_at=max(last_heartbeat_values) if last_heartbeat_values else None,
        lease_expire_at=min(active_leases or all_leases) if (active_leases or all_leases) else None,
        replicas=normalized_replicas,
    )
