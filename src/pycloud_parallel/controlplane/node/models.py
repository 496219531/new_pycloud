from __future__ import annotations

"""NodeControl-facing state models."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar, Dict, Optional, Tuple

from pycloud_parallel.controlplane.data_store import StoredDataArtifact
from pycloud_parallel.controlplane.session_model import (
    ExecutionReplicaSnapshot,
    SessionBinding,
    SessionIdentity,
    SessionLease,
)
from pycloud_parallel.controlplane.state_time import dt_to_ts, utc_now
from pycloud_parallel.controlplane.serialization import dict_to_struct
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

StoredResultArtifact = StoredDataArtifact


@dataclass
class ManagedGlobalsState:
    scope_kind: str
    scope_key: str
    scope_dir: str
    allowed_names: Tuple[str, ...]
    globals_digest: str


@dataclass
class CodeArtifact:
    code_version: str
    path: str
    runtime: str
    entry_module: str
    entry_callable: str
    package_format: str
    export_mode: str
    export_methods: Tuple[str, ...]
    export_decorator: str
    dependency_policy_mode: str
    dependency_allowlist: Tuple[str, ...]
    dependency_path: str
    size_bytes: int
    created_at: datetime


@dataclass
class ObjectArtifact:
    object_id: str
    path: str
    format: str
    size_bytes: int
    created_at: datetime
    storage_backend: str = "file"
    segment_path: str = ""
    segment_offset: int = 0
    segment_length: int = 0


@dataclass
class TaskState:
    task_id: str
    client_id: str
    job_id: str
    code_version: str
    runtime_key: str
    execution_mode: int
    payload: dict
    timeout_hint_sec: int
    priority: int
    status: int = pb2.TASK_STATUS_QUEUED
    attempt: int = 1
    worker_id: str = ""
    lease_id: str = ""
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    last_heartbeat_at: Optional[datetime] = None
    cancel_requested: bool = False
    result: Optional[Any] = None
    error_type: str = ""
    error_message: str = ""
    dispatch_build_execute_spec_ms: float = 0.0

    def as_result(self) -> pb2.TaskResult:
        return pb2.TaskResult(
            task_id=self.task_id,
            job_id=self.job_id,
            status=self.status,
            attempt=self.attempt,
            started_at=dt_to_ts(self.started_at or utc_now()),
            finished_at=dt_to_ts(self.finished_at or utc_now()),
            result=dict_to_struct(self.result),
            error=pb2.TaskError(type=self.error_type, message=self.error_message),
        )


@dataclass
class ServiceSession:
    kind: ClassVar[str] = "service"
    service_id: str
    owner_client_id: str
    service_name: str
    code_version: str
    worker_count: int
    heartbeat_timeout_sec: int
    idle_ttl_sec: int
    expose_http: bool
    service_token: str
    http_base_url: str
    status: int
    created_at: datetime
    last_heartbeat_at: datetime
    lease_expire_at: datetime
    executor_ready: bool = False
    in_flight: int = 0
    queued: int = 0
    alive_workers: int = 0
    stop_reason: str = ""
    methods: Dict[str, Tuple[str, str]] = field(default_factory=dict)
    managed_global_names: Tuple[str, ...] = ()
    managed_globals_scope_dir: str = ""
    managed_globals_digest: str = ""
    timing_metrics: Dict[str, object] = field(default_factory=dict)
    request_count: int = 0
    returned_count: int = 0

    def identity(self) -> SessionIdentity:
        return SessionIdentity(
            kind="service",
            session_id=str(self.service_id or ""),
            session_name=str(self.service_name or ""),
            owner_client_id=str(self.owner_client_id or ""),
            session_token=str(self.service_token or ""),
        )

    def lease(self) -> SessionLease:
        return SessionLease(
            heartbeat_timeout_sec=max(1, int(self.heartbeat_timeout_sec or 0)),
            idle_ttl_sec=max(0, int(self.idle_ttl_sec or 0)),
            created_at=self.created_at,
            last_heartbeat_at=self.last_heartbeat_at,
            lease_expire_at=self.lease_expire_at,
        )

    def binding(self) -> SessionBinding:
        return SessionBinding(
            code_version=str(self.code_version or ""),
            worker_count=max(0, int(self.worker_count or 0)),
            executor_ready=bool(self.executor_ready),
            managed_global_names=tuple(str(name) for name in (self.managed_global_names or ())),
            managed_globals_scope_dir=str(self.managed_globals_scope_dir or ""),
            managed_globals_digest=str(self.managed_globals_digest or ""),
        )

    def snapshot(
        self,
        *,
        node_instance_id: str = "",
        node_id: str = "",
        failure: str = "",
    ) -> ExecutionReplicaSnapshot:
        status_text = pb2.ServiceStatus.Name(int(self.status or pb2.SERVICE_STATUS_UNSPECIFIED))
        alive = not str(failure or "").strip() and int(self.status or 0) in {
            int(pb2.SERVICE_STATUS_STARTING),
            int(pb2.SERVICE_STATUS_RUNNING),
            int(pb2.SERVICE_STATUS_DRAINING),
        }
        return ExecutionReplicaSnapshot(
            kind="service",
            node_instance_id=str(node_instance_id or ""),
            node_id=str(node_id or ""),
            session_id=str(self.service_id or ""),
            session_name=str(self.service_name or ""),
            code_version=str(self.code_version or ""),
            worker_count=max(0, int(self.worker_count or 0)),
            alive=alive,
            status=status_text,
            lease_expire_at=self.lease_expire_at,
            failure=str(failure or ""),
        )


@dataclass
class TaskPoolState:
    kind: ClassVar[str] = "task_pool"
    pool_id: str
    owner_client_id: str
    pool_name: str
    code_version: str
    task_method: str
    worker_count: int
    heartbeat_timeout_sec: int
    idle_ttl_sec: int
    pool_token: str
    status: str
    created_at: datetime
    last_heartbeat_at: datetime
    lease_expire_at: datetime
    managed_global_names: Tuple[str, ...] = ()
    managed_globals_scope_dir: str = ""
    managed_globals_digest: str = ""
    executor_ready: bool = False
    task_count: int = 0
    timing_metrics: Dict[str, object] = field(default_factory=dict)
    returned_count: int = 0

    def identity(self) -> SessionIdentity:
        return SessionIdentity(
            kind="task_pool",
            session_id=str(self.pool_id or ""),
            session_name=str(self.pool_name or ""),
            owner_client_id=str(self.owner_client_id or ""),
            session_token=str(self.pool_token or ""),
        )

    def lease(self) -> SessionLease:
        return SessionLease(
            heartbeat_timeout_sec=max(1, int(self.heartbeat_timeout_sec or 0)),
            idle_ttl_sec=max(0, int(self.idle_ttl_sec or 0)),
            created_at=self.created_at,
            last_heartbeat_at=self.last_heartbeat_at,
            lease_expire_at=self.lease_expire_at,
        )

    def binding(self) -> SessionBinding:
        return SessionBinding(
            code_version=str(self.code_version or ""),
            worker_count=max(0, int(self.worker_count or 0)),
            executor_ready=bool(self.executor_ready),
            managed_global_names=tuple(str(name) for name in (self.managed_global_names or ())),
            managed_globals_scope_dir=str(self.managed_globals_scope_dir or ""),
            managed_globals_digest=str(self.managed_globals_digest or ""),
        )

    def snapshot(
        self,
        *,
        node_instance_id: str = "",
        node_id: str = "",
        failure: str = "",
    ) -> ExecutionReplicaSnapshot:
        status_text = str(self.status or "")
        alive = not str(failure or "").strip() and status_text.upper() == "RUNNING"
        return ExecutionReplicaSnapshot(
            kind="task_pool",
            node_instance_id=str(node_instance_id or ""),
            node_id=str(node_id or ""),
            session_id=str(self.pool_id or ""),
            session_name=str(self.pool_name or ""),
            code_version=str(self.code_version or ""),
            worker_count=max(0, int(self.worker_count or 0)),
            alive=alive,
            status=status_text,
            lease_expire_at=self.lease_expire_at,
            failure=str(failure or ""),
        )


ServiceReplicaState = ServiceSession
TaskPoolReplicaState = TaskPoolState


__all__ = [
    "CodeArtifact",
    "ManagedGlobalsState",
    "ObjectArtifact",
    "ServiceReplicaState",
    "ServiceSession",
    "StoredResultArtifact",
    "TaskPoolReplicaState",
    "TaskPoolState",
    "TaskState",
]
