from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pycloud_parallel import JobQueue
from pycloud_parallel.controlplane.effective_policy import resolve_effective_policy
from pycloud_parallel.controlplane.job_queue import JobQueueManager
from pycloud_parallel.controlplane.policy_profile import get_policy_profile


class _FakePool:
    def __init__(self, *, mode: str = "structured_v1", policy_id: str = "trusted_internal") -> None:
        self._serialization_mode = mode
        self.effective_policy = resolve_effective_policy(
            get_policy_profile(policy_id),
            requested_mode=mode,
            context="taskpool_session",
        )
        self.globals_digests = {"node-1": "sha256:test"}
        self._job_id = ""
        self.closed = 0

    def _pending_result_count(self) -> int:
        return 0

    def close(self) -> None:
        self.closed += 1


def test_shared_pool_soft_switches_mode_without_rebuild():
    manager = JobQueueManager()
    created = []

    def _create_pool(mode: str):
        pool = _FakePool(mode=mode)
        created.append(pool)
        return pool

    with patch("pycloud_parallel.controlplane.job_queue._close_executor_async", side_effect=lambda executor: None):
        first = manager._get_or_create_shared_pool(
            artifact_key="artifact-1",
            requested_mode="structured_v1",
            create_pool=_create_pool,
        )
        second = manager._get_or_create_shared_pool(
            artifact_key="artifact-1",
            requested_mode="pickle_stable_v1",
            create_pool=_create_pool,
        )

    assert first is second
    assert len(created) == 1
    assert second._serialization_mode == "pickle_stable_v1"  # noqa: SLF001
    assert manager._shared_pool is not None
    assert manager._shared_pool.current_mode == "pickle_stable_v1"


def test_shared_pool_reset_pool_rebuilds_pool():
    manager = JobQueueManager()
    created = []

    def _create_pool(mode: str):
        pool = _FakePool(mode=mode)
        created.append(pool)
        return pool

    with patch("pycloud_parallel.controlplane.job_queue._close_executor_async", side_effect=lambda executor: None):
        first = manager._get_or_create_shared_pool(
            artifact_key="artifact-1",
            requested_mode="structured_v1",
            create_pool=_create_pool,
        )
        second = manager._get_or_create_shared_pool(
            artifact_key="artifact-1",
            requested_mode="structured_v1",
            reset_pool=True,
            create_pool=_create_pool,
        )

    assert first is not second
    assert len(created) == 2


def test_shared_pool_soft_switch_failure_rebuilds_once():
    manager = JobQueueManager()
    created = []

    class _BusyPool(_FakePool):
        def _pending_result_count(self) -> int:
            return 1

    def _create_pool(mode: str):
        pool = _BusyPool(mode=mode) if not created else _FakePool(mode=mode)
        created.append(pool)
        return pool

    with patch("pycloud_parallel.controlplane.job_queue._close_executor_async", side_effect=lambda executor: None):
        manager._get_or_create_shared_pool(
            artifact_key="artifact-1",
            requested_mode="structured_v1",
            create_pool=_create_pool,
        )
        pool = manager._get_or_create_shared_pool(
            artifact_key="artifact-1",
            requested_mode="pickle_stable_v1",
            create_pool=_create_pool,
        )

    assert len(created) == 2
    assert pool._serialization_mode == "pickle_stable_v1"  # noqa: SLF001


def test_shared_pool_soft_switch_logs_reason_before_rebuild(caplog):
    manager = JobQueueManager()
    created = []

    class _BusyPool(_FakePool):
        def _pending_result_count(self) -> int:
            return 1

    def _create_pool(mode: str):
        pool = _BusyPool(mode=mode) if not created else _FakePool(mode=mode)
        created.append(pool)
        return pool

    with patch("pycloud_parallel.controlplane.job_queue._close_executor_async", side_effect=lambda executor: None):
        manager._get_or_create_shared_pool(
            artifact_key="artifact-1",
            requested_mode="structured_v1",
            create_pool=_create_pool,
        )
        manager._get_or_create_shared_pool(
            artifact_key="artifact-1",
            requested_mode="pickle_stable_v1",
            create_pool=_create_pool,
        )

    assert "shared pool soft switch failed" in caplog.text


def test_invalid_mode_rejected_against_fixed_policy():
    manager = JobQueueManager(taskpool_policy_id="default_safe")

    with pytest.raises(ValueError, match="requested_mode"):
        manager._resolve_requested_task_mode({"task_serialization_mode": "pickle_stable_v1"})


def test_shared_pool_idle_expiry_detected():
    manager = JobQueueManager(pool_idle_ttl_sec=10)
    manager._shared_pool = type("_Holder", (), {})()  # type: ignore[assignment]
    manager._shared_pool.last_used_at = datetime.now(timezone.utc) - timedelta(seconds=11)  # type: ignore[attr-defined]

    assert manager._shared_pool_idle_expired_locked(now=datetime.now(timezone.utc)) is True


def test_jobqueue_submit_rejects_policy_id():
    client = JobQueue("127.0.0.1:50051", client_id="policy-reject")
    try:
        with pytest.raises(ValueError, match="no longer accepts policy_id"):
            client.submit(
                source=b"def run(**_kwargs):\n    return {}\n\ndef task_generator(**_kwargs):\n    return []\n",
                entry_module="job_demo",
                policy_id="pickle_internal_heavy",
            )
    finally:
        client.close()
