from __future__ import annotations

from pycloud_parallel.execution.recovery_state import (
    ReplicaLifecycle,
    build_replica_recovery_state,
)


def test_build_replica_recovery_state_prioritizes_terminal() -> None:
    state = build_replica_recovery_state(
        "node-inst-1",
        active=True,
        terminal=True,
        error="task pool not running",
    )

    assert state.lifecycle == ReplicaLifecycle.TERMINAL
    assert state.terminal is True
    assert state.active is False
    assert state.retryable is False
    assert state.error == "task pool not running"


def test_build_replica_recovery_state_active() -> None:
    state = build_replica_recovery_state("node-inst-1", active=True, terminal=False)

    assert state.lifecycle == ReplicaLifecycle.ACTIVE
    assert state.active is True
    assert state.retryable is False


def test_build_replica_recovery_state_failed_retryable() -> None:
    state = build_replica_recovery_state("node-inst-1", active=False, terminal=False, error=RuntimeError("timeout"))

    assert state.lifecycle == ReplicaLifecycle.FAILED_RETRYABLE
    assert state.retryable is True
    assert state.error == "timeout"


def test_build_replica_recovery_state_failed_permanent() -> None:
    state = build_replica_recovery_state(
        "node-inst-1",
        active=False,
        terminal=False,
        retryable=False,
        error="ModuleNotFoundError",
    )

    assert state.lifecycle == ReplicaLifecycle.FAILED_PERMANENT
    assert state.retryable is False
    assert state.permanent is True
