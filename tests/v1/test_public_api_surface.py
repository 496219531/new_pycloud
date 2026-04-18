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
    assert callable(ApiService.deploy)
    discovery = ApiService.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        transport="discovery",
        validate_on_init=False,
    )
    gateway = ApiService.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        transport="gateway",
        validate_on_init=False,
    )
    try:
        assert discovery.service_name == "svc-demo"
        assert discovery.transport == "discovery"
        assert callable(discovery.status)
        assert isinstance(getattr(type(discovery), "methods", None), property)
        assert gateway.service_name == "svc-demo"
        assert gateway.transport == "gateway"
        assert callable(gateway.status)
        assert isinstance(getattr(type(gateway), "methods", None), property)
    finally:
        discovery.close()
        gateway.close()


def test_api_pool_module_exposes_only_task_pool():
    assert api_pool_module.__all__ == ["TaskPool"]
    assert dir(api_pool_module) == ["TaskPool"]
    assert not hasattr(api_pool_module, "TaskPoolSession")
    assert not hasattr(api_pool_module, "create_task_pool_from_infocenter")
    assert callable(ApiTaskPool.open)


def test_service_deploy_public_api_uses_target_keyword(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        ApiService,
        "deploy_from_infocenter",
        classmethod(lambda cls, **kwargs: captured.update(kwargs) or "service-session"),
    )

    result = ApiService.deploy(target="127.0.0.1:50051", source=b"blob")

    assert result == "service-session"
    assert captured["infocenter_target"] == "127.0.0.1:50051"
    assert captured["source"] == b"blob"


def test_task_pool_open_public_api_uses_target_keyword(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        ApiTaskPool,
        "from_infocenter",
        classmethod(lambda cls, **kwargs: captured.update(kwargs) or "task-pool-session"),
    )

    result = ApiTaskPool.open(target="127.0.0.1:50051", source=b"blob")

    assert result == "task-pool-session"
    assert captured["infocenter_target"] == "127.0.0.1:50051"
    assert captured["source"] == b"blob"


def test_public_api_still_accepts_legacy_infocenter_target_keyword(monkeypatch):
    captured_service = {}
    captured_pool = {}

    monkeypatch.setattr(
        ApiService,
        "deploy_from_infocenter",
        classmethod(lambda cls, **kwargs: captured_service.update(kwargs) or "service-session"),
    )
    monkeypatch.setattr(
        ApiTaskPool,
        "from_infocenter",
        classmethod(lambda cls, **kwargs: captured_pool.update(kwargs) or "task-pool-session"),
    )

    service_result = ApiService.deploy(infocenter_target="127.0.0.1:50051", source=b"svc")
    pool_result = ApiTaskPool.open(infocenter_target="127.0.0.1:50051", source=b"pool")

    assert service_result == "service-session"
    assert pool_result == "task-pool-session"
    assert captured_service["infocenter_target"] == "127.0.0.1:50051"
    assert captured_pool["infocenter_target"] == "127.0.0.1:50051"


def test_api_queue_module_exposes_only_job_queue():
    assert api_queue_module.__all__ == ["JobQueue"]
    assert dir(api_queue_module) == ["JobQueue"]
    assert not hasattr(api_queue_module, "JobQueueClient")
    queue = ApiJobQueue.connect("127.0.0.1:50051", client_id="surface-client")
    try:
        assert isinstance(queue, ApiJobQueue)
    finally:
        queue.close()
