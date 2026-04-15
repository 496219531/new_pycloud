from __future__ import annotations

"""Shared execution session identity and snapshot models."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal


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
