from __future__ import annotations

import inspect

import pytest

import pycloud_parallel.api as api_pkg
import pycloud_parallel.easy as easy_pkg
import pycloud_parallel.api.pool as api_pool_module
import pycloud_parallel.api.queue as api_queue_module
import pycloud_parallel.api.service as api_service_module
import pycloud_parallel
from pycloud_parallel.api import DataRef as ApiDataRef
from pycloud_parallel.api import JobQueue as ApiJobQueue
from pycloud_parallel.api import Service as ApiService
from pycloud_parallel.api import TaskPool as ApiTaskPool
from pycloud_parallel.api import export as api_export


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
        "Startup" + "ServiceNode",
        "configure",
        "foreach",
        "parallel_for",
    ]
    for name in legacy_names:
        assert not hasattr(pycloud_parallel, name)


def test_api_package_exports_only_v1_semantic_names():
    assert api_pkg.__all__ == ["DataRef", "JobQueue", "Service", "TaskPool", "export"]
    assert dir(api_pkg) == api_pkg.__all__


def test_easy_module_exposes_convenience_helpers_without_top_level_pollution():
    expected = {
        "call_service_method",
        "deploy_module_service",
        "run_tasks",
        "serve_controlplane",
        "serve_function",
        "serve_module",
        "serve_node",
        "submit_tasks",
    }

    for name in expected:
        assert callable(getattr(easy_pkg, name))
        assert not hasattr(pycloud_parallel, name)
    assert "run_db_server" not in easy_pkg.__all__


def test_easy_run_module_forever_accepts_legacy_worker_num(monkeypatch):
    captured = {}
    module = object()

    def fake_serve_module(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setattr(easy_pkg, "serve_module", fake_serve_module)

    easy_pkg.run_module_forever(
        info_pub_addr="127.0.0.1:50051",
        module=module,
        service_name="public_data_source",
        worker_num=3,
        policy_id="trusted_internal",
    )

    assert captured["args"] == (module,)
    assert captured["kwargs"] == {
        "target": "127.0.0.1:50051",
        "port": None,
        "service_name": "public_data_source",
        "worker_count": 3,
        "policy_id": "trusted_internal",
    }


def test_easy_legacy_helpers_accept_old_positional_signatures(monkeypatch):
    captured = []

    def fake_serve_controlplane(*args, **kwargs):
        captured.append(("controlplane", args, kwargs))

    def fake_serve_node(*args, **kwargs):
        captured.append(("node", args, kwargs))

    def fake_serve_function(*args, **kwargs):
        captured.append(("function", args, kwargs))

    monkeypatch.setattr(easy_pkg, "serve_controlplane", fake_serve_controlplane)
    monkeypatch.setattr(easy_pkg, "serve_node", fake_serve_node)
    monkeypatch.setattr(easy_pkg, "serve_function", fake_serve_function)

    fn = object()
    easy_pkg.run_info_center(50051, 50052, 8038)
    easy_pkg.run_worker_forever("127.0.0.1:50051", 18061)
    easy_pkg.run_func_server_without_return(fn, "127.0.0.1:50051", 2)

    assert captured == [
        ("controlplane", (50051,), {"host": "0.0.0.0"}),
        ("node", ("127.0.0.1:50051",), {"port": 18061}),
        ("function", (fn,), {"target": "127.0.0.1:50051", "worker_count": 2, "join": True}),
    ]


def test_api_service_module_exposes_only_service():
    assert api_service_module.__all__ == ["Service"]
    assert dir(api_service_module) == ["Service"]
    assert not hasattr(api_service_module, "ServiceGroup")
    assert not hasattr(api_service_module, "deploy_service_from_infocenter")
    assert callable(ApiService.startup)
    assert callable(ApiService.deploy)
    assert callable(ApiService.connect)
    discovery = ApiService.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        route="discovery",
        validate_on_init=False,
    )
    gateway = ApiService.connect(
        target="127.0.0.1:50051",
        service_name="svc-demo",
        route="gateway",
        validate_on_init=False,
    )
    try:
        assert discovery.service_name == "svc-demo"
        assert discovery.route == "discovery"
        assert discovery.protocol == "http"
        assert callable(discovery.status)
        assert isinstance(getattr(type(discovery), "methods", None), property)
        assert not hasattr(discovery, "update_globals")
        assert gateway.service_name == "svc-demo"
        assert gateway.route == "gateway"
        assert gateway.protocol == "http"
        assert callable(gateway.status)
        assert isinstance(getattr(type(gateway), "methods", None), property)
        assert not hasattr(gateway, "update_globals")
    finally:
        discovery.close()
        gateway.close()


def test_api_service_connect_no_longer_exposes_policy_id():
    assert "policy_id" not in inspect.signature(ApiService.connect).parameters
    assert "policy_id" not in inspect.signature(ApiService.deploy).parameters
    assert "nodecontrol_transport" not in inspect.signature(ApiService.deploy).parameters
    assert "replace_existing" in inspect.signature(ApiService.startup).parameters


def test_api_pool_module_exposes_only_task_pool():
    assert api_pool_module.__all__ == ["TaskPool"]
    assert dir(api_pool_module) == ["TaskPool"]
    assert not hasattr(api_pool_module, "TaskPoolSession")
    assert not hasattr(api_pool_module, "create_task_pool_from_infocenter")
    assert callable(ApiTaskPool.open)
    assert not hasattr(ApiTaskPool, "connect")


def test_service_deploy_public_api_uses_target_keyword(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        ApiService,
        "_deploy_from_infocenter",
        classmethod(lambda cls, **kwargs: captured.update(kwargs) or "service-session"),
    )

    result = ApiService.deploy(target="127.0.0.1:50051", source=b"blob")

    assert result == "service-session"
    assert captured["infocenter_target"] == "127.0.0.1:50051"
    assert captured["source"] == b"blob"


def test_service_deploy_public_api_forwards_resource_paths(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        ApiService,
        "_deploy_from_infocenter",
        classmethod(lambda cls, **kwargs: captured.update(kwargs) or "service-session"),
    )

    result = ApiService.deploy(
        target="127.0.0.1:50051",
        source=object(),
        resource_paths=["data/demo.csv"],
    )

    assert result == "service-session"
    assert captured["resource_paths"] == ["data/demo.csv"]


def test_task_pool_open_public_api_uses_target_keyword(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        ApiTaskPool,
        "_from_infocenter",
        classmethod(lambda cls, **kwargs: captured.update(kwargs) or "task-pool-session"),
    )

    result = ApiTaskPool.open(target="127.0.0.1:50051", source=b"blob")

    assert result == "task-pool-session"
    assert captured["infocenter_target"] == "127.0.0.1:50051"
    assert captured["source"] == b"blob"


def test_task_pool_open_public_api_forwards_resource_paths(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        ApiTaskPool,
        "_from_infocenter",
        classmethod(lambda cls, **kwargs: captured.update(kwargs) or "task-pool-session"),
    )

    result = ApiTaskPool.open(
        target="127.0.0.1:50051",
        source=object(),
        resource_paths=["data/demo.csv"],
    )

    assert result == "task-pool-session"
    assert captured["resource_paths"] == ["data/demo.csv"]


def test_low_level_compat_entries_are_not_public_class_surface():
    assert not hasattr(ApiService, "deploy_from_infocenter")
    assert not hasattr(ApiService, "deploy_from_func")
    assert not hasattr(ApiService, "deploy_from_file")
    assert not hasattr(ApiService, "deploy_from_bytes")
    assert not hasattr(ApiTaskPool, "from_infocenter")


def test_public_api_rejects_legacy_infocenter_target_keyword():
    with pytest.raises(TypeError):
        ApiService.deploy(infocenter_target="127.0.0.1:50051", source=b"svc")
    with pytest.raises(TypeError):
        ApiTaskPool.open(infocenter_target="127.0.0.1:50051", source=b"pool")


def test_public_deploy_open_reject_legacy_artifact_keywords():
    legacy_kwargs = {
        "blob": b"def run(): return 1\n",
        "entry_module": "legacy_mod",
        "entry_callable": "run",
        "artifact_path": "legacy.py",
        "dependency_allowlist": ["orjson==3.10.18"],
        "export_mode": "single",
        "export_methods": ["run"],
        "func": lambda: None,
    }

    for name, value in legacy_kwargs.items():
        with pytest.raises(TypeError):
            ApiService.deploy(target="127.0.0.1:50051", source=b"svc", **{name: value})
        with pytest.raises(TypeError):
            ApiTaskPool.open(target="127.0.0.1:50051", source=b"pool", **{name: value})


def test_service_startup_rejects_legacy_dependency_allowlist():
    with pytest.raises(TypeError):
        ApiService.startup(
            source="startup_demo",
            dependency_allowlist=["orjson==3.10.18"],
            start=False,
        )


def test_service_startup_rejects_legacy_infocenter_target_keyword():
    with pytest.raises(TypeError):
        ApiService.startup(
            source="startup_demo",
            infocenter_target="127.0.0.1:50051",
            start=False,
        )


def test_job_queue_public_submit_rejects_legacy_dependency_allowlist():
    queue = ApiJobQueue.connect("127.0.0.1:50051", client_id="surface-client")
    try:
        assert not hasattr(queue, "submit_job_from_bytes")
        assert not hasattr(queue, "submit_job_from_module")
        with pytest.raises(TypeError):
            queue.submit(
                source=b"def run(**_kwargs): return {}\n",
                entry_module="job_demo",
                dependency_allowlist=["orjson==3.10.18"],
            )
    finally:
        queue.close()


def test_api_queue_module_exposes_only_job_queue():
    assert api_queue_module.__all__ == ["JobQueue"]
    assert dir(api_queue_module) == ["JobQueue"]
    assert not hasattr(api_queue_module, "JobQueueClient")
    queue = ApiJobQueue.connect("127.0.0.1:50051", client_id="surface-client")
    try:
        assert isinstance(queue, ApiJobQueue)
    finally:
        queue.close()
