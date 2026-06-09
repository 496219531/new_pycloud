from __future__ import annotations

import threading
import time


def test_dispatch_create_requests_respects_configured_worker_limit(monkeypatch):
    from pycloud_parallel.execution.deployment_create_helper import dispatch_create_requests

    monkeypatch.setenv("PYCLOUD_DEPLOY_CREATE_MAX_WORKERS", "3")
    active = 0
    max_active = 0
    lock = threading.Lock()

    def _create_one(node):
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        try:
            time.sleep(0.02)
            return node
        finally:
            with lock:
                active -= 1

    results = dispatch_create_requests(
        list(range(12)),
        create_one=_create_one,
        thread_name_prefix="test-create-limit",
    )

    assert [item.created for item in results] == list(range(12))
    assert max_active <= 3


def test_should_retry_replica_create_failures_uses_shared_categories():
    from pycloud_parallel.execution.deployment_create_helper import should_retry_replica_create_failures

    assert should_retry_replica_create_failures(
        {"node-old": "RuntimeError('cannot connect to 127.0.0.1:50061')"},
        success=0,
        required=1,
        resource_kind="service",
    )
    assert should_retry_replica_create_failures(
        {"node-old": "expected_node_instance_id mismatch"},
        success=0,
        required=1,
        resource_kind="task_pool",
    )
    assert not should_retry_replica_create_failures(
        {"node-bad": "ModuleNotFoundError: No module named 'missing_pkg'"},
        success=0,
        required=1,
        resource_kind="service",
    )
    assert not should_retry_replica_create_failures(
        {"node-ok": "RuntimeError('cannot connect to 127.0.0.1:50061')"},
        success=1,
        required=1,
        resource_kind="service",
    )


def test_next_replica_create_interval_is_bounded():
    from pycloud_parallel.execution.deployment_create_helper import next_replica_create_interval

    assert next_replica_create_interval(1, deadline_remaining_sec=10.0, base_sec=0.25, max_sec=1.0) == 0.25
    assert next_replica_create_interval(10, deadline_remaining_sec=10.0, base_sec=0.25, max_sec=1.0) == 1.0
    assert next_replica_create_interval(10, deadline_remaining_sec=0.2, base_sec=0.25, max_sec=1.0) == 0.2
    assert next_replica_create_interval(1, deadline_remaining_sec=0.0) == 0.0


def test_run_replica_create_recovery_loop_stops_when_condition_clears(monkeypatch):
    import pycloud_parallel.execution.deployment_create_helper as helper

    now = {"value": 0.0}
    attempts = []

    monkeypatch.setattr(helper.time, "monotonic", lambda: now["value"])

    def _sleep(seconds):
        now["value"] += float(seconds)

    monkeypatch.setattr(helper.time, "sleep", _sleep)

    def _should_continue():
        return len(attempts) < 2

    def _attempt_once(attempt):
        attempts.append(attempt)

    count = helper.run_replica_create_recovery_loop(
        timeout_sec=10.0,
        should_continue=_should_continue,
        attempt_once=_attempt_once,
        base_interval_sec=0.5,
        max_interval_sec=0.5,
    )

    assert count == 2
    assert attempts == [1, 2]
