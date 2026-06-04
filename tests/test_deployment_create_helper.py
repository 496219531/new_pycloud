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
