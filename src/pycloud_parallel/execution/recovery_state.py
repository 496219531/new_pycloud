from __future__ import annotations

"""Small recovery-state primitives shared by execution sessions."""

from dataclasses import dataclass
from enum import Enum


class ReplicaLifecycle(str, Enum):
    ACTIVE = "active"
    FAILED_RETRYABLE = "failed_retryable"
    FAILED_PERMANENT = "failed_permanent"
    TERMINAL = "terminal"


@dataclass(frozen=True)
class ReplicaRecoveryState:
    node_instance_id: str
    lifecycle: ReplicaLifecycle
    error: str = ""

    @property
    def active(self) -> bool:
        return self.lifecycle == ReplicaLifecycle.ACTIVE

    @property
    def retryable(self) -> bool:
        return self.lifecycle == ReplicaLifecycle.FAILED_RETRYABLE

    @property
    def permanent(self) -> bool:
        return self.lifecycle == ReplicaLifecycle.FAILED_PERMANENT

    @property
    def terminal(self) -> bool:
        return self.lifecycle == ReplicaLifecycle.TERMINAL


def build_replica_recovery_state(
    node_instance_id: str,
    *,
    active: bool,
    terminal: bool,
    retryable: bool = True,
    error: object = "",
) -> ReplicaRecoveryState:
    normalized_node_id = str(node_instance_id or "").strip()
    if terminal:
        lifecycle = ReplicaLifecycle.TERMINAL
    elif active:
        lifecycle = ReplicaLifecycle.ACTIVE
    elif retryable:
        lifecycle = ReplicaLifecycle.FAILED_RETRYABLE
    else:
        lifecycle = ReplicaLifecycle.FAILED_PERMANENT
    return ReplicaRecoveryState(
        node_instance_id=normalized_node_id,
        lifecycle=lifecycle,
        error=str(error or ""),
    )


__all__ = [
    "ReplicaLifecycle",
    "ReplicaRecoveryState",
    "build_replica_recovery_state",
]
