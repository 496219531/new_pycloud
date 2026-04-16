from __future__ import annotations

import pycloud_parallel.api as api_pkg
import pycloud_parallel.api.pool as api_pool_module
import pycloud_parallel.api.queue as api_queue_module
import pycloud_parallel.api.service as api_service_module
import pycloud_parallel
from pycloud_parallel.api import DataRef as ApiDataRef
from pycloud_parallel.api import JobQueue as ApiJobQueue
from pycloud_parallel.api import Service as ApiService
from pycloud_parallel.api import TaskPool as ApiTaskPool
from pycloud_parallel.api import export as api_export
from pycloud_parallel.local import configure, foreach, parallel_for


def test_top_level_public_api_surface_is_v1_only():
    assert pycloud_parallel.Service is ApiService
    assert pycloud_parallel.TaskPool is ApiTaskPool
    assert pycloud_parallel.JobQueue is ApiJobQueue
    assert pycloud_parallel.DataRef is ApiDataRef
    assert pycloud_parallel.export is api_export
    assert pycloud_parallel.__all__ == ["DataRef", "JobQueue", "Service", "TaskPool", "export"]


def test_top_level_no_longer_exposes_legacy_or_local_runtime_names():
    legacy_names = [
        "Object" + "Ref",
        "Result" + "Ref",
        "Deployed" + "Service",
        "Gateway" + "Connect",
        "Direct" + "Connect",
        "JobQueue" + "Client",
        "TaskPool" + "Session",
        "DedicatedTaskService" + "Session",
        "configure",
        "foreach",
        "parallel_for",
    ]
    for name in legacy_names:
        assert not hasattr(pycloud_parallel, name)


def test_local_parallel_api_moves_under_pycloud_parallel_local():
    assert callable(configure)
    assert callable(foreach)
    assert callable(parallel_for)


def test_api_package_exports_only_v1_semantic_names():
    assert api_pkg.__all__ == ["DataRef", "JobQueue", "Service", "TaskPool", "export"]
    assert dir(api_pkg) == api_pkg.__all__


def test_api_service_module_exposes_only_service():
    assert api_service_module.__all__ == ["Service"]
    assert dir(api_service_module) == ["Service"]
    assert not hasattr(api_service_module, "ServiceGroup")
    assert not hasattr(api_service_module, "deploy_service_from_infocenter")


def test_api_pool_module_exposes_only_task_pool():
    assert api_pool_module.__all__ == ["TaskPool"]
    assert dir(api_pool_module) == ["TaskPool"]
    assert not hasattr(api_pool_module, "TaskPoolSession")
    assert not hasattr(api_pool_module, "create_task_pool_from_infocenter")


def test_api_queue_module_exposes_only_job_queue():
    assert api_queue_module.__all__ == ["JobQueue"]
    assert dir(api_queue_module) == ["JobQueue"]
    assert not hasattr(api_queue_module, "JobQueueClient")
