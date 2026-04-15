from __future__ import annotations

"""Protocols for replica-scoped execution handles and task backends."""

from typing import Any, Dict, Protocol, runtime_checkable

from pycloud_parallel.controlplane.session_model import (
    ExecutionReplicaSnapshot,
    SessionBinding,
    SessionIdentity,
    SessionLease,
    SessionKind,
)


@runtime_checkable
class ExecutionReplicaHandle(Protocol):
    kind: SessionKind
    node_instance_id: str
    node_id: str
    code_version: str
    worker_count: int
    heartbeat_timeout_sec: int

    @property
    def session_id(self) -> str: ...

    @property
    def session_name(self) -> str: ...

    @property
    def session_token(self) -> str: ...

    def identity(self) -> SessionIdentity: ...

    def lease(self) -> SessionLease: ...

    def binding(self) -> SessionBinding: ...

    def heartbeat(self, *args, **kwargs) -> Any: ...

    def get_status(self) -> Any: ...

    def close(self, *args, **kwargs) -> Any: ...

    def update_globals_prepared(self, prepared_values: Dict[str, object]) -> Any: ...

    def snapshot(
        self,
        *,
        node_instance_id: str = "",
        node_id: str = "",
        failure: str = "",
    ) -> ExecutionReplicaSnapshot: ...


@runtime_checkable
class ServiceReplicaHandle(ExecutionReplicaHandle, Protocol):
    def list_methods(self, *, include_docs: bool = False) -> Any: ...

    def call(self, method: str, payload: Dict[str, object], *, timeout_sec: float = 60.0, token: str | None = None) -> Any: ...


@runtime_checkable
class TaskPoolReplicaHandle(ExecutionReplicaHandle, Protocol):
    def submit_tasks(self, tasks: Any, *, job_id: str = "") -> Any: ...

    def pull_results(self, *, limit: int = 100, wait_ms: int = 0, cursor: str = "") -> Any: ...

    def cancel_job(self, *, job_id: str, reason: str = "") -> Any: ...


@runtime_checkable
class TaskExecutionBackend(Protocol):
    def submit_payloads(self, payloads: Any, **kwargs) -> Any: ...

    def iter_results(self, **kwargs) -> Any: ...

    def wait_for_data(self, **kwargs) -> Any: ...

    def cancel_job(self, **kwargs) -> Any: ...
