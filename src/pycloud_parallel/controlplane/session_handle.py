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
