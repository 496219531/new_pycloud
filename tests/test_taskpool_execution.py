from __future__ import annotations

"""Execution-focused tests for the V1 TaskPool implementation."""

import asyncio
import importlib
import io
import sys
import tarfile
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.controlplane.serialization import dict_to_struct


def _build_task_entry_module(tmp_path, monkeypatch, *, with_init: bool = True):
    package_name = "demo_task_pkg_entry"
    sys.modules.pop(package_name, None)
    sys.modules.pop(f"{package_name}.worker", None)
    sys.modules.pop(f"{package_name}.helper", None)
    package_dir = tmp_path / package_name
    package_dir.mkdir()
    if with_init:
        (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "helper.py").write_text(
        "def normalize(value):\n"
        "    return int(value)\n",
        encoding="utf-8",
    )
    (package_dir / "ignored.csv").write_text("value\n1\n", encoding="utf-8")
    (package_dir / "worker.py").write_text(
        "from .helper import normalize\n\n"
        "def run(value=0, **_kwargs):\n"
        "    return {'value': normalize(value)}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    return importlib.import_module(f"{package_name}.worker")


def _build_task_entry_module_with_resource(tmp_path, monkeypatch):
    worker_module = _build_task_entry_module(tmp_path, monkeypatch)
    package_dir = tmp_path / worker_module.__package__
    (package_dir / "data.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    return worker_module


def test_native_task_pool_session_submit_and_wait() -> None:
    from pycloud_parallel import TaskPool

    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(
            ok=True,
            accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
            rejected=[],
        ),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(
            ok=True,
            results=[
                pb2.TaskResult(
                    task_id="pool-task-0001",
                    job_id="job-native",
                    status=pb2.TASK_STATUS_SUCCEEDED,
                    result=dict_to_struct({"value": 1}),
                )
            ],
            next_cursor="",
        ),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )

    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        return_value=fake_pool_client,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native",
            blob=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
            entry_module="task_demo",
            entry_callable="run",
            worker_count=2,
            node_count=1,
        )

    try:
        resp = session.submit_payloads([{"value": 1}])
        assert len(resp.accepted) == 1
        assert session.job_id == "job-native"
        assert session.node_ids == ["node-1"]
        assert session.methods == ["run"]
    finally:
        session.close()


def test_native_task_pool_session_dynamic_default_max_in_flight_uses_effective_workers() -> None:
    from pycloud_parallel import TaskPool

    fake_pool_1 = SimpleNamespace(
        owner_client_id="owner-demo",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        worker_count=2,
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    fake_pool_2 = SimpleNamespace(
        owner_client_id="owner-demo",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        worker_count=3,
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    session = TaskPool(
        pools={"node-1": fake_pool_1, "node-2": fake_pool_2},
        nodes={
            "node-1": SimpleNamespace(node_id="node-1", task_pool_worker_available=2),
            "node-2": SimpleNamespace(node_id="node-2", task_pool_worker_available=3),
        },
        task_method="run",
    )
    try:
        assert session._resolve_max_in_flight(None) == 8  # noqa: SLF001
    finally:
        session.close()


def test_native_task_pool_session_cancel_job_aggregates_pool_responses() -> None:
    from pycloud_parallel import TaskPool

    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True, queued_cancelled=1, running_marked=2, already_done=3, not_found=0),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        return_value=fake_pool_client,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-cancel",
            blob=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
            entry_module="task_demo",
            entry_callable="run",
            worker_count=2,
            node_count=1,
        )
    try:
        resp = session.cancel_job(job_id="job-native-cancel", reason="test cancel")
        assert resp.queued_cancelled == 1
        assert resp.running_marked == 2
        assert resp.already_done == 3
        assert session._pending_task_ids == set()  # noqa: SLF001
    finally:
        session.close()


def test_task_pool_from_infocenter_includes_only_explicit_resource_paths(tmp_path, monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    worker_module = _build_task_entry_module_with_resource(tmp_path, monkeypatch)
    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    create_calls = []
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True, queued_cancelled=0, running_marked=0, already_done=0, not_found=0),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )

    def _fake_create_task_pool(self, **kwargs):
        del self
        create_calls.append(dict(kwargs))
        return fake_pool_client

    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        _fake_create_task_pool,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-resource",
            source=worker_module,
            resource_paths=["data.csv"],
            worker_count=2,
            node_count=1,
        )

    try:
        with tarfile.open(fileobj=io.BytesIO(create_calls[0]["blob"]), mode="r:gz") as tar:
            names = set(tar.getnames())
        assert f"{worker_module.__package__}/data.csv" in names
    finally:
        session.close()


def test_native_task_pool_session_submit_payloads_rejects_unknown_task_method() -> None:
    from pycloud_parallel import TaskPool

    fake_pool = SimpleNamespace(
        owner_client_id="owner-demo",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    session = TaskPool(
        pools={"node-1": fake_pool},
        nodes={},
        task_method="run",
        job_id="job-native-method-check",
    )
    try:
        with pytest.raises(AttributeError, match="has no method 'other'"):
            session.submit_payloads([{"value": 1}], task_method="other")
    finally:
        session.close()


def test_native_task_pool_session_status_map() -> None:
    from pycloud_parallel import TaskPool

    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_status = SimpleNamespace(status="RUNNING", worker_count=2, task_count=0)
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True),
        get_status=lambda: fake_status,
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        return_value=fake_pool_client,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-status",
            blob=b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
            entry_module="task_demo",
            entry_callable="run",
            worker_count=2,
            node_count=1,
        )
    try:
        status_map = session.status_map()
        assert status_map["node-1"].status == "RUNNING"
    finally:
        session.close()


def test_task_pool_session_packages_module_object_entry_module(tmp_path, monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    worker_module = _build_task_entry_module(tmp_path, monkeypatch)
    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    captured = {}

    def _fake_create_task_pool(self, **kwargs):
        captured.update(kwargs)
        return fake_pool_client

    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        _fake_create_task_pool,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-module-entry",
            entry_module=worker_module,
            entry_callable="run",
            worker_count=2,
            node_count=1,
        )

    try:
        assert captured["entry_module"] == worker_module.__name__
        assert captured["package_format"] == "tar.gz"
        with tarfile.open(fileobj=io.BytesIO(captured["blob"]), mode="r:gz") as tar:
            names = set(tar.getnames())
        assert f"{worker_module.__package__}/__init__.py" in names
        assert f"{worker_module.__package__}/worker.py" in names
        assert f"{worker_module.__package__}/helper.py" in names
        assert f"{worker_module.__package__}/ignored.csv" not in names
    finally:
        session.close()


def test_task_pool_session_packages_callable_object_entry_callable(tmp_path, monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    worker_module = _build_task_entry_module(tmp_path, monkeypatch)
    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    captured = {}

    def _fake_create_task_pool(self, **kwargs):
        captured.update(kwargs)
        return fake_pool_client

    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        _fake_create_task_pool,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-callable-entry",
            entry_callable=worker_module.run,
            worker_count=2,
            node_count=1,
        )

    try:
        assert captured["entry_module"] == worker_module.__name__
        assert captured["entry_callable"] == "run"
        assert captured["package_format"] == "tar.gz"
        with tarfile.open(fileobj=io.BytesIO(captured["blob"]), mode="r:gz") as tar:
            names = set(tar.getnames())
        assert f"{worker_module.__package__}/__init__.py" in names
        assert f"{worker_module.__package__}/worker.py" in names
        assert f"{worker_module.__package__}/helper.py" in names
        assert f"{worker_module.__package__}/ignored.csv" not in names
    finally:
        session.close()


def test_task_pool_session_packages_namespace_module_with_synthetic_init(tmp_path, monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    worker_module = _build_task_entry_module(tmp_path, monkeypatch, with_init=False)
    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    captured = {}

    def _fake_create_task_pool(self, **kwargs):
        captured.update(kwargs)
        return fake_pool_client

    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        _fake_create_task_pool,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-namespace-module-entry",
            entry_module=worker_module,
            entry_callable="run",
            worker_count=2,
            node_count=1,
        )

    try:
        with tarfile.open(fileobj=io.BytesIO(captured["blob"]), mode="r:gz") as tar:
            names = set(tar.getnames())
            synthetic_init = tar.extractfile(f"{worker_module.__package__}/__init__.py")
            init_blob = synthetic_init.read() if synthetic_init is not None else None
        assert f"{worker_module.__package__}/__init__.py" in names
        assert init_blob == b""
        assert f"{worker_module.__package__}/worker.py" in names
        assert f"{worker_module.__package__}/helper.py" in names
    finally:
        session.close()


def test_task_pool_session_packages_entry_func_alias(tmp_path, monkeypatch) -> None:
    from pycloud_parallel import TaskPool

    worker_module = _build_task_entry_module(tmp_path, monkeypatch)
    fake_node = SimpleNamespace(node_id="node-1", control_addr="127.0.0.1:50061")
    fake_pool_client = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-1",
        pool_token="token-1",
        code_version="sha256:test",
        worker_count=2,
        heartbeat_timeout_sec=30,
        submit_tasks=lambda tasks, job_id="": pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[]),
        pull_results=lambda limit=100, wait_ms=0, cursor="": pb2.PullResultsResponse(ok=True, results=[], next_cursor=""),
        heartbeat=lambda seq=0: pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=15),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )
    captured = {}

    def _fake_create_task_pool(self, **kwargs):
        captured.update(kwargs)
        return fake_pool_client

    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        _fake_create_task_pool,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-entry-func",
            entry_func=worker_module.run,
            worker_count=2,
            node_count=1,
        )

    try:
        assert captured["entry_module"] == worker_module.__name__
        assert captured["entry_callable"] == "run"
        assert captured["package_format"] == "tar.gz"
    finally:
        session.close()


def test_native_task_pool_session_update_globals_aggregates_digests() -> None:
    from pycloud_parallel import TaskPool

    prepared_values = {}

    def _fake_prepare(clients, values, **_kwargs):
        prepared_values["clients"] = clients
        prepared_values["values"] = values
        return {"cfg": {"k": "v"}}

    pool_a = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-a",
        pool_token="token-a",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        _client=SimpleNamespace(
            update_runtime_globals_prepared=lambda **kwargs: SimpleNamespace(globals_digest="sha256:same"),
        ),
    )
    pool_b = SimpleNamespace(
        owner_client_id="owner-demo",
        pool_id="pool-b",
        pool_token="token-b",
        code_version="sha256:test",
        heartbeat_timeout_sec=30,
        _client=SimpleNamespace(
            update_runtime_globals_prepared=lambda **kwargs: SimpleNamespace(globals_digest="sha256:same"),
        ),
    )
    session = TaskPool(
        pools={"node-a": pool_a, "node-b": pool_b},
        nodes={},
        task_method="run",
        job_id="job-update-globals",
    )
    with patch("pycloud_parallel.execution.task_pool._prepare_managed_globals_values_for_upload", _fake_prepare):
        digest = session.update_globals({"cfg": {"k": "v"}})
    assert digest == "sha256:same"
    assert session.globals_digests == {"node-a": "sha256:same", "node-b": "sha256:same"}
    assert prepared_values["values"] == {"cfg": {"k": "v"}}


def test_native_task_pool_session_submit_values_delegates() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-values",
    )
    captured = {}

    def _fake_submit(payloads, **kwargs):
        captured["payloads"] = payloads
        captured["kwargs"] = kwargs
        return pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[])

    session.submit_payloads = _fake_submit  # type: ignore[method-assign]
    session.submit_values([1, 2, 3], arg_name="x", extra=9)
    assert captured["payloads"] == [{"x": 1, "extra": 9}, {"x": 2, "extra": 9}, {"x": 3, "extra": 9}]


def test_native_task_pool_session_is_alive_tracks_remaining_nodes() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={
            "node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30),
            "node-2": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30),
        },
        nodes={},
        task_method="run",
        job_id="job-alive",
    )
    assert session.is_alive() is True
    session._active_nodes.discard("node-1")  # noqa: SLF001
    assert session.is_alive() is True
    session._active_nodes.clear()  # noqa: SLF001
    session.failed = True
    assert session.is_alive() is False


def test_native_task_pool_session_submit_payloads_avoids_degraded_nodes() -> None:
    from pycloud_parallel import TaskPool

    submitted_to: list[str] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._client = SimpleNamespace()

        def submit_tasks(self, tasks, job_id=""):
            submitted_to.extend([self.node_id] * len(tasks))
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

    session = TaskPool(
        pools={
            "node-good": _Pool("node-good"),
            "node-bad": _Pool("node-bad"),
        },
        nodes={},
        task_method="run",
        job_id="job-active-submit",
    )
    session._active_nodes = {"node-good"}  # noqa: SLF001

    session.submit_payloads([{"value": 1}, {"value": 2}, {"value": 3}])
    assert submitted_to == ["node-good", "node-good", "node-good"]


def test_native_task_pool_session_submit_payloads_fail_when_no_active_nodes() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={
            "node-good": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30),
            "node-bad": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30),
        },
        nodes={},
        task_method="run",
        job_id="job-no-active-submit",
    )
    session._active_nodes = set()  # noqa: SLF001

    with pytest.raises(RuntimeError, match="no node pools|no active"):
        session.submit_payloads([{"value": 1}])


def test_native_task_pool_session_submit_payloads_accepts_throughput_strategy() -> None:
    from pycloud_parallel import TaskPool

    captured = {}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._client = SimpleNamespace()

        def submit_tasks(self, tasks, job_id=""):
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

    session = TaskPool(
        pools={"node-a": _Pool("node-a"), "node-b": _Pool("node-b")},
        nodes={},
        task_method="run",
        job_id="job-strategy-submit",
    )

    def _fake_select(candidates, *, profile, state, round_robin_counter=0):
        captured["profile"] = profile.name
        return candidates[0]

    with patch("pycloud_parallel.execution.task_pool.select_one_candidate", side_effect=_fake_select):
        session.submit_payloads([{"value": 1}], strategy="taskpool_throughput")

    assert captured["profile"] == "taskpool_throughput"


def test_native_task_pool_session_iter_items_batch_uses_imap_unordered_core() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-iter-items-core",
    )

    with patch.object(
        session,
        "imap_unordered",
        return_value=iter([(0, {"value": 1}), (1, None)]),
    ) as mocked:
        items = list(session.iter_items([{"value": 1}, {"value": 2}], timeout_sec=0.1))

    assert [item.index for item in items] == [0, 1]
    assert items[0].ok is True and items[0].result == {"value": 1}
    assert items[1].ok is False and items[1].result is None
    mocked.assert_called_once()


def test_native_task_pool_session_map_forwards_strategy_to_collect_items() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-map-strategy",
    )

    with patch.object(session, "collect_items", return_value=[]) as mocked:
        session.map([1, 2], strategy="taskpool_throughput")

    assert mocked.call_args.kwargs["strategy"] == "taskpool_throughput"


def test_native_task_pool_session_keepalive_degrades_per_node() -> None:
    from pycloud_parallel import TaskPool

    calls: list[tuple[str, int]] = []

    class _Pool:
        def __init__(self, node_id: str, *, should_fail: bool) -> None:
            self.node_id = node_id
            self.owner_client_id = "owner"
            self.code_version = "sha256:test"
            self.heartbeat_timeout_sec = 1
            self._should_fail = should_fail

        def heartbeat(self, *, seq: int = 0):
            calls.append((self.node_id, seq))
            if self._should_fail:
                raise RuntimeError(f"{self.node_id} heartbeat failed")
            return pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=1)

    session = TaskPool(
        pools={
            "node-bad": _Pool("node-bad", should_fail=True),
            "node-good": _Pool("node-good", should_fail=False),
        },
        nodes={},
        task_method="run",
        job_id="job-hb",
    )

    session._start_keepalive(interval_sec=0.05)
    try:
        import time

        deadline = time.time() + 1.0
        while time.time() < deadline and "node-bad" not in session.failures:
            time.sleep(0.05)

        assert "node-bad" in session.failures
        assert "heartbeat failed" in session.failures["node-bad"]
        assert "node-good" in session._active_nodes  # noqa: SLF001
        assert "node-bad" not in session._active_nodes  # noqa: SLF001
        assert session.failed is False
        assert session.is_alive() is True
    finally:
        session.close()


def test_native_task_pool_session_keepalive_fails_when_all_nodes_fail() -> None:
    from pycloud_parallel import TaskPool

    class _FailPool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 1

        def heartbeat(self, *, seq: int = 0):
            raise RuntimeError(f"heartbeat failed seq={seq}")

    session = TaskPool(
        pools={"node-1": _FailPool()},
        nodes={},
        task_method="run",
        job_id="job-fail-all",
    )

    session._start_keepalive(interval_sec=0.05)
    try:
        import time

        deadline = time.time() + 1.0
        while time.time() < deadline and not session.failed:
            time.sleep(0.05)

        assert session.failed is True
        assert session.is_alive() is False
        assert "node-1" in session.failures
    finally:
        session.close()


def test_native_task_pool_session_close_retries_replica_close() -> None:
    from pycloud_parallel import TaskPool

    close_calls = {"count": 0}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        _client = SimpleNamespace(close=lambda: None)

        def close(self, reason=""):
            del reason
            close_calls["count"] += 1
            if close_calls["count"] == 1:
                raise RuntimeError("temporary close failure")
            return pb2.CloseTaskPoolResponse(ok=True, accepted=True)

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-close-retry",
    )

    session.close()

    assert close_calls["count"] >= 2


def test_native_task_pool_session_iter_and_collect_results_consume_incrementally() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"task_id": task_result.task_id})
            self._results = [
                pb2.TaskResult(task_id="task-1", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1})),
                pb2.TaskResult(task_id="task-2", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 2})),
                pb2.TaskResult(task_id="task-3", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 3})),
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    pool = _Pool()
    session = TaskPool(
        pools={"node-1": pool},
        nodes={},
        task_method="run",
        job_id="job-iter",
    )
    session._pending_task_ids = {"task-1", "task-2", "task-3"}  # noqa: SLF001

    first_batch = list(session.iter_results(max_count=2, timeout_sec=0.1))
    assert [item.task_id for item in first_batch] == ["task-1", "task-2"]
    assert session._pending_task_ids == {"task-3"}  # noqa: SLF001

    second_batch = session.collect_results(timeout_sec=0.1)
    assert [item.task_id for item in second_batch] == ["task-3"]
    assert session._pending_task_ids == set()  # noqa: SLF001


def test_native_task_pool_session_iter_data_materializes_per_result() -> None:
    from pycloud_parallel import TaskPool

    fetched: list[str] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(
                fetch_result_data=lambda task_result, target_path="": fetched.append(task_result.task_id) or {"value": task_result.task_id}
            )
            self._results = [
                pb2.TaskResult(task_id="task-a", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1})),
                pb2.TaskResult(task_id="task-b", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 2})),
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-data",
    )
    session._pending_task_ids = {"task-a", "task-b"}  # noqa: SLF001

    items = session.collect_data(timeout_sec=0.1)
    assert items == [("task-a", {"value": "task-a"}), ("task-b", {"value": "task-b"})]
    assert fetched == ["task-a", "task-b"]


def test_native_task_pool_session_collect_results_with_none_waits_pending_results() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._results = [
                pb2.TaskResult(task_id="task-1", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1})),
                pb2.TaskResult(task_id="task-2", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 2})),
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-none",
    )
    session._pending_task_ids = {"task-1", "task-2"}  # noqa: SLF001

    out = session.collect_results(max_count=None, timeout_sec=0.1)
    assert [item.task_id for item in out] == ["task-1", "task-2"]
    assert session._pending_task_ids == set()  # noqa: SLF001


def test_native_task_pool_session_imap_unordered_streams_results() -> None:
    from pycloud_parallel import TaskPool

    submitted: list[str] = []
    materialized: list[str] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(
                fetch_result_data=lambda task_result, target_path="": materialized.append(task_result.task_id) or {"value": task_result.task_id}
            )

        def submit_tasks(self, tasks, job_id=""):
            task_ids = [item.task_id for item in tasks]
            submitted.extend(task_ids)
            for task_id in task_ids:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"value": task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=task_id, status=pb2.TASK_STATUS_QUEUED) for task_id in task_ids],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-stream",
    )

    items = list(
        session.imap_unordered(
            [{"value": 1}, {"value": 2}, {"value": 3}],
            max_in_flight=2,
            receive_batch=1,
            submit_timeout_sec=1.0,
            result_timeout_sec=1.0,
        )
    )

    assert submitted == ["job-stream-task-0001", "job-stream-task-0002", "job-stream-task-0003"]
    assert [index for index, _ in items] == [0, 1, 2]
    assert materialized == submitted
    assert session._pending_task_ids == set()  # noqa: SLF001


def test_native_task_pool_session_submit_payloads_keeps_round_robin_without_polling() -> None:
    from pycloud_parallel import TaskPool

    submissions: dict[str, list[str]] = {"node-1": [], "node-2": []}
    pull_calls: list[str] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._client = SimpleNamespace()

        def submit_tasks(self, tasks, job_id=""):
            submissions[self.node_id].extend(item.task_id for item in tasks)
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            pull_calls.append(self.node_id)
            return pb2.PullResultsResponse(ok=True, results=[], next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool("node-1"), "node-2": _Pool("node-2")},
        nodes={},
        task_method="run",
        job_id="job-submit-rr",
    )

    resp = session.submit_payloads([{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}], job_id="job-submit-override")

    assert len(resp.accepted) == 4
    assert submissions["node-1"] == ["job-submit-rr-task-0001", "job-submit-rr-task-0003"]
    assert submissions["node-2"] == ["job-submit-rr-task-0002", "job-submit-rr-task-0004"]
    assert pull_calls == []
    assert session._pending_task_ids == set(resp_task.task_id for resp_task in resp.accepted)  # noqa: SLF001


def test_native_task_pool_session_imap_unordered_rotates_poll_order() -> None:
    from pycloud_parallel import TaskPool

    pull_calls: list[str] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            pull_calls.append(self.node_id)
            return pb2.PullResultsResponse(ok=True, results=[], next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool("node-1"), "node-2": _Pool("node-2")},
        nodes={},
        task_method="run",
        job_id="job-poll-rotate",
    )

    with pytest.raises(TimeoutError, match="imap_unordered did not receive results"):
        list(
            session.imap_unordered(
                [{"value": 1}],
                max_in_flight=1,
                receive_batch=1,
                result_timeout_sec=0.08,
                wait_ms=1,
            )
        )

    assert pull_calls[:2] == ["node-1", "node-2"]
    assert len(pull_calls) >= 2


def test_native_task_pool_session_imap_unordered_refills_fast_node() -> None:
    from pycloud_parallel import TaskPool

    submitted_by_node: dict[str, list[str]] = {"node-slow": [], "node-fast": []}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str, *, ready_after_pulls: int) -> None:
            self.node_id = node_id
            self._ready_after_pulls = ready_after_pulls
            self._inflight: list[list[object]] = []
            self._client = SimpleNamespace(
                fetch_result_data=lambda task_result, target_path="": {
                    "task_id": task_result.task_id,
                    "node_id": self.node_id,
                }
            )

        def submit_tasks(self, tasks, job_id=""):
            for item in tasks:
                submitted_by_node[self.node_id].append(item.task_id)
                self._inflight.append([item.task_id, self._ready_after_pulls])
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            ready: list[pb2.TaskResult] = []
            kept: list[list[object]] = []
            for task_id, remaining in self._inflight:
                next_remaining = int(remaining) - 1
                if next_remaining <= 0 and len(ready) < limit:
                    ready.append(
                        pb2.TaskResult(
                            task_id=str(task_id),
                            status=pb2.TASK_STATUS_SUCCEEDED,
                            result=dict_to_struct({"node_id": self.node_id}),
                        )
                    )
                else:
                    kept.append([task_id, next_remaining])
            self._inflight = kept
            return pb2.PullResultsResponse(ok=True, results=ready, next_cursor="")

    session = TaskPool(
        pools={
            "node-slow": _Pool("node-slow", ready_after_pulls=3),
            "node-fast": _Pool("node-fast", ready_after_pulls=1),
        },
        nodes={},
        task_method="run",
        job_id="job-fast-refill",
    )

    items = list(
        session.imap_unordered(
            [{"value": idx} for idx in range(6)],
            max_in_flight=4,
            receive_batch=2,
            result_timeout_sec=0.5,
            wait_ms=1,
        )
    )

    assert len(items) == 6
    assert len(submitted_by_node["node-fast"]) == 4
    assert len(submitted_by_node["node-slow"]) == 2
    assert {data["node_id"] for _task_id, data in items} == {"node-fast", "node-slow"}


def test_native_task_pool_session_imap_unordered_uses_full_global_max_in_flight() -> None:
    from pycloud_parallel import TaskPool

    submit_batch_sizes: list[int] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self) -> None:
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(
                fetch_result_data=lambda task_result, target_path="": {
                    "task_id": task_result.task_id,
                }
            )

        def submit_tasks(self, tasks, job_id=""):
            submit_batch_sizes.append(len(tasks))
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"task_id": item.task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-global-window",
    )

    items = list(
        session.imap_unordered(
            [{"value": idx} for idx in range(8)],
            max_in_flight=8,
            receive_batch=8,
            result_timeout_sec=0.5,
            wait_ms=1,
        )
    )

    assert len(items) == 8
    assert submit_batch_sizes[0] == 8


def test_native_task_pool_session_imap_unordered_times_out_when_results_do_not_arrive() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            return pb2.PullResultsResponse(ok=True, results=[], next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-timeout",
    )

    with pytest.raises(TimeoutError, match="imap_unordered did not receive results"):
        list(
            session.imap_unordered(
                [{"value": 1}],
                max_in_flight=1,
                receive_batch=1,
                result_timeout_sec=0.1,
                wait_ms=10,
            )
        )


def test_native_task_pool_session_imap_unordered_cancels_outstanding_on_error() -> None:
    from pycloud_parallel import TaskPool

    cancel_calls: list[tuple[str, str]] = []

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self) -> None:
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            for item in tasks:
                if item.task_id.endswith("0001"):
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_FAILED_USER,
                            error=pb2.TaskError(type="UserError", message="boom"),
                        )
                    )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

        def cancel_job(self, job_id="", reason=""):
            cancel_calls.append((job_id, reason))
            return pb2.CancelJobResponse(ok=True, queued_cancelled=1, running_marked=0, already_done=0, not_found=0)

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-error-cancel",
    )

    with pytest.raises(RuntimeError, match="boom"):
        list(
            session.imap_unordered(
                [{"value": 1}, {"value": 2}],
                max_in_flight=2,
                receive_batch=1,
                result_timeout_sec=0.5,
                wait_ms=1,
            )
        )

    assert cancel_calls == [("job-error-cancel", "imap_unordered task failure")]
    assert session._pending_task_ids == set()  # noqa: SLF001


def test_native_task_pool_proxy_sync_requires_clean_session() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-sync-clean",
    )
    session._pending_task_ids = {"task-old"}  # noqa: SLF001

    with pytest.raises(RuntimeError, match="requires a clean task pool session"):
        session.run.sync(value=7)


def test_native_task_pool_session_imap_unordered_requires_clean_session() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-imap-clean",
    )
    session._pending_task_ids = {"task-old"}  # noqa: SLF001

    with pytest.raises(RuntimeError, match="requires a clean task pool session"):
        list(session.imap_unordered([{"value": 1}]))


def test_native_task_pool_session_exclusive_mode_blocks_concurrent_submit_and_iter() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-exclusive",
    )
    session._exclusive_mode = "imap_unordered"  # noqa: SLF001
    session._exclusive_owner_thread_id = 999999  # noqa: SLF001
    session._exclusive_depth = 1  # noqa: SLF001

    with pytest.raises(RuntimeError, match="exclusively used by imap_unordered"):
        session.submit_payloads([{"value": 1}])

    with pytest.raises(RuntimeError, match="exclusively used by imap_unordered"):
        list(session.iter_data(timeout_sec=0.1))


def test_native_task_pool_session_drops_late_results_for_non_pending_tasks() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._results = [
                pb2.TaskResult(task_id="task-late", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1}))
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-drop-late",
    )
    session._pending_task_ids = set()  # noqa: SLF001

    assert session.collect_results(timeout_sec=0.1) == []


def test_native_task_pool_session_collect_results_calls_iter_results() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-collect",
    )

    with patch.object(
        session,
        "iter_results",
        return_value=iter(
            [
                pb2.TaskResult(task_id="task-1", status=pb2.TASK_STATUS_SUCCEEDED),
                pb2.TaskResult(task_id="task-2", status=pb2.TASK_STATUS_SUCCEEDED),
            ]
        ),
    ) as mocked:
        out = session.collect_results(max_count=2, timeout_sec=1.0)

    assert [item.task_id for item in out] == ["task-1", "task-2"]
    mocked.assert_called_once_with(max_count=2, timeout_sec=1.0, wait_ms=500, limit=100, job_id="")


def test_native_task_pool_session_collect_data_calls_iter_data() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-collect-data",
    )

    with patch.object(
        session,
        "iter_data",
        return_value=iter([("task-1", {"value": 1}), ("task-2", {"value": 2})]),
    ) as mocked:
        out = session.collect_data(max_count=2, timeout_sec=1.0)

    assert out == [("task-1", {"value": 1}), ("task-2", {"value": 2})]
    mocked.assert_called_once_with(max_count=2, timeout_sec=1.0, wait_ms=500, limit=100, job_id="", raise_on_error=False, task_ids=None)


def test_native_task_pool_session_unordered_rejects_legacy_control_params() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-unordered",
    )
    payloads = [{"value": 1}, {"value": 2}]

    with pytest.raises(TypeError):
        list(
            session.unordered(
                payloads,
                max_in_flight=4,
                receive_batch=2,
            )
        )


def test_native_task_pool_session_consume_unordered_calls_handle() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-consume-unordered",
    )
    payloads = [{"value": 1}, {"value": 2}]
    handled: list[tuple[object, object]] = []

    with patch.object(
        session,
        "imap_unordered",
        return_value=iter([("task-1", {"value": 1}), ("task-2", {"value": 2})]),
    ) as mocked:
        processed = session.consume_unordered(
            payloads,
            handle=lambda task_id, result: handled.append((task_id, result)),
            max_in_flight=3,
            receive_batch=1,
            submit_timeout_sec=1.5,
            result_timeout_sec=2.5,
            wait_ms=15,
            raise_on_error=False,
            node_window_factor=1.25,
        )

    assert processed == 2
    assert handled == [(0, {"value": 1}), (1, {"value": 2})]
    mocked.assert_called_once_with(
        payloads,
        task_method="",
        strategy="taskpool_default",
        max_in_flight=3,
        receive_batch=1,
        submit_timeout_sec=1.5,
        result_timeout_sec=2.5,
        wait_ms=15,
        raise_on_error=False,
        node_window_factor=1.25,
    )


def test_native_task_pool_session_iter_items_includes_failures() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._results = [
                pb2.TaskResult(task_id="task-ok", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1})),
                pb2.TaskResult(
                    task_id="task-fail",
                    status=pb2.TASK_STATUS_FAILED_USER,
                    error=pb2.TaskError(type="UserError", message="boom"),
                ),
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-items",
    )
    session._pending_task_ids = {"task-ok", "task-fail"}  # noqa: SLF001

    items = session.collect_items(timeout_sec=0.1)
    assert len(items) == 2
    assert items[0].task_id == "task-ok"
    assert items[0].ok is True
    assert items[0].data == {"value": "task-ok"}
    assert items[1].task_id == "task-fail"
    assert items[1].ok is False
    assert items[1].error_type == "UserError"
    assert items[1].error_message == "boom"


def test_native_task_pool_session_collect_data_returns_none_on_failure_by_default() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._results = [
                pb2.TaskResult(
                    task_id="task-fail",
                    status=pb2.TASK_STATUS_FAILED_INFRA,
                    error=pb2.TaskError(type="InfraError", message="node lost"),
                )
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-fail-data",
    )
    session._pending_task_ids = {"task-fail"}  # noqa: SLF001

    out = session.collect_data(timeout_sec=0.1)
    assert out == [("task-fail", None)]


def test_native_task_pool_session_aiter_items_supports_receiving_existing_results() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._results = [
                pb2.TaskResult(task_id="task-a", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1})),
                pb2.TaskResult(task_id="task-b", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 2})),
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    async def _collect(session):
        items = []
        async for item in session.aiter_items(timeout_sec=0.1):
            items.append(item)
        return items

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-aiter-items",
    )
    session._pending_task_ids = {"task-a", "task-b"}  # noqa: SLF001

    items = asyncio.run(_collect(session))
    assert [item.task_id for item in items] == ["task-a", "task-b"]


def test_native_task_pool_session_acollect_items_supports_receiving_existing_results() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._results = [
                pb2.TaskResult(task_id="task-a", status=pb2.TASK_STATUS_SUCCEEDED, result=dict_to_struct({"value": 1})),
                pb2.TaskResult(task_id="task-b", status=pb2.TASK_STATUS_FAILED_USER, error=pb2.TaskError(type="UserError", message="boom")),
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-acollect-items",
    )
    session._pending_task_ids = {"task-a", "task-b"}  # noqa: SLF001

    items = asyncio.run(session.acollect_items(timeout_sec=0.1))
    assert len(items) == 2
    assert items[0].task_id == "task-a"
    assert items[1].task_id == "task-b"
    assert items[1].ok is False


def test_native_task_pool_session_collect_data_raises_on_failure_when_enabled() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._results = [
                pb2.TaskResult(
                    task_id="task-fail",
                    status=pb2.TASK_STATUS_FAILED_INFRA,
                    error=pb2.TaskError(type="InfraError", message="node lost"),
                )
            ]

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._results[:limit]
            self._results = self._results[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-fail-data-raise",
    )
    session._pending_task_ids = {"task-fail"}  # noqa: SLF001

    with pytest.raises(RuntimeError, match="node lost"):
        session.collect_data(timeout_sec=0.1, raise_on_error=True)


def test_native_task_pool_proxy_submit_returns_task_id() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-submit-id",
    )

    with patch.object(
        session,
        "submit_payloads",
        return_value=pb2.SubmitTasksResponse(
            ok=True,
            accepted=[pb2.TaskAccepted(task_id="task-submit-1", status=pb2.TASK_STATUS_QUEUED)],
            rejected=[],
        ),
    ) as mocked:
        task_id = session.run.submit(value=7)

    assert task_id == "task-submit-1"
    mocked.assert_called_once()


def test_native_task_pool_proxy_call_waits_for_own_task_id() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-own-task",
    )

    with patch.object(
        session,
        "submit_payloads",
        return_value=pb2.SubmitTasksResponse(
            ok=True,
            accepted=[pb2.TaskAccepted(task_id="task-own-1", status=pb2.TASK_STATUS_QUEUED)],
            rejected=[],
        ),
    ), patch.object(
        session,
        "_collect_data_for_task_ids",
        return_value=[("task-own-1", {"value": 49})],
    ) as mocked_collect:
        task_id = session.run(value=7)
        result = session.run.sync(value=7)

    assert task_id == "task-own-1"
    assert result == {"value": 49}
    mocked_collect.assert_called_once_with({"task-own-1"}, timeout_sec=30.0)


def test_native_task_pool_map_returns_none_on_failure() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._ready: list[pb2.TaskResult] = []

        def submit_tasks(self, tasks, job_id=""):
            for item in tasks:
                if "0002" in item.task_id:
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_FAILED_USER,
                            error=pb2.TaskError(type="UserError", message="boom"),
                        )
                    )
                else:
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_SUCCEEDED,
                            result=dict_to_struct({"value": item.task_id}),
                        )
                    )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-map-none",
    )

    out = session.map([1, 2, 3], arg_name="value", timeout_sec=0.1)
    assert out[0] == {"value": "job-map-none-task-0001"}
    assert out[1] is None
    assert out[2] == {"value": "job-map-none-task-0003"}


def test_native_task_pool_unordered_returns_index_and_result_or_none() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._ready: list[pb2.TaskResult] = []

        def submit_tasks(self, tasks, job_id=""):
            for item in tasks:
                if "0002" in item.task_id:
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_FAILED_INFRA,
                            error=pb2.TaskError(type="InfraError", message="node lost"),
                        )
                    )
                else:
                    self._ready.append(
                        pb2.TaskResult(
                            task_id=item.task_id,
                            status=pb2.TASK_STATUS_SUCCEEDED,
                            result=dict_to_struct({"value": item.task_id}),
                        )
                    )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-unordered-batch",
    )

    out = list(session.unordered([{"value": 1}, {"value": 2}, {"value": 3}], timeout_sec=0.1))
    assert sorted(out) == [
        (0, {"value": "job-unordered-batch-task-0001"}),
        (1, None),
        (2, {"value": "job-unordered-batch-task-0003"}),
    ]


def test_native_task_pool_imap_unordered_requeues_after_submit_failure_to_healthy_node() -> None:
    from pycloud_parallel import TaskPool

    submitted_by_node = {"node-bad": [], "node-good": []}

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30
        worker_count = 1

        def __init__(self, node_id: str) -> None:
            self.node_id = node_id
            self._ready: list[pb2.TaskResult] = []
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})

        def submit_tasks(self, tasks, job_id=""):
            submitted_by_node[self.node_id].extend(item.task_id for item in tasks)
            if self.node_id == "node-bad":
                raise RuntimeError("submit failed on bad node")
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"value": item.task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = self._ready[:limit]
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-bad": _Pool("node-bad"), "node-good": _Pool("node-good")},
        nodes={},
        task_method="run",
        job_id="job-requeue-after-submit-fail",
    )

    with patch(
        "pycloud_parallel.execution.task_pool.select_one_candidate",
        side_effect=lambda candidates, *, profile, state, round_robin_counter=0: (
            next(candidate for candidate in candidates if candidate.id == "node-bad")
            if "node-bad" in [candidate.id for candidate in candidates] and "node-bad" not in state.disabled_candidates
            else next(candidate for candidate in candidates if candidate.id == "node-good")
        ),
    ):
        out = list(session.imap_unordered([{"value": 1}, {"value": 2}], timeout_sec=0.1))

    assert [index for index, _ in out] == [0, 1]
    assert submitted_by_node["node-bad"]
    assert len(submitted_by_node["node-good"]) >= 2
    assert session._submit_breaker_states["node-bad"].consecutive_failures >= 1
    assert "node-bad" in session._scheduler_state.disabled_candidates


def test_native_task_pool_collect_items_batch_returns_execution_items_in_input_order() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})
            self._ready: list[pb2.TaskResult] = []

        def submit_tasks(self, tasks, job_id=""):
            for item in tasks:
                self._ready.append(
                    pb2.TaskResult(
                        task_id=item.task_id,
                        status=pb2.TASK_STATUS_SUCCEEDED,
                        result=dict_to_struct({"value": item.task_id}),
                    )
                )
            return pb2.SubmitTasksResponse(
                ok=True,
                accepted=[pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED) for item in tasks],
                rejected=[],
            )

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            batch = list(reversed(self._ready[:limit]))
            self._ready = self._ready[limit:]
            return pb2.PullResultsResponse(ok=True, results=batch, next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-collect-items",
    )

    items = session.collect_items([{"value": 1}, {"value": 2}, {"value": 3}], timeout_sec=0.1)
    assert [item.index for item in items] == [0, 1, 2]
    assert [item.result for item in items] == [
        {"value": "job-collect-items-task-0001"},
        {"value": "job-collect-items-task-0002"},
        {"value": "job-collect-items-task-0003"},
    ]


def test_native_task_pool_session_collect_items_replays_buffered_results_without_deadlock() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def __init__(self) -> None:
            self._client = SimpleNamespace(fetch_result_data=lambda task_result, target_path="": {"value": task_result.task_id})

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            return pb2.PullResultsResponse(ok=True, results=[], next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-buffered-items",
    )
    session._pending_task_ids = {"task-buffered"}  # noqa: SLF001
    session._buffered_result_items.append(  # noqa: SLF001
        (
            "node-1",
            pb2.TaskResult(
                task_id="task-buffered",
                status=pb2.TASK_STATUS_SUCCEEDED,
                result=dict_to_struct({"value": 1}),
            ),
        )
    )

    items = session.collect_items(timeout_sec=0.1)

    assert len(items) == 1
    assert items[0].task_id == "task-buffered"
    assert items[0].ok is True
    assert items[0].result == {"value": "task-buffered"}


def test_native_task_pool_session_wait_for_results_replays_buffered_results_without_deadlock() -> None:
    from pycloud_parallel import TaskPool

    class _Pool:
        owner_client_id = "owner"
        code_version = "sha256:test"
        heartbeat_timeout_sec = 30

        def pull_results(self, limit=100, wait_ms=0, cursor=""):
            return pb2.PullResultsResponse(ok=True, results=[], next_cursor="")

    session = TaskPool(
        pools={"node-1": _Pool()},
        nodes={},
        task_method="run",
        job_id="job-buffered-results",
    )
    session._pending_task_ids = {"task-buffered"}  # noqa: SLF001
    session._buffered_result_items.append(  # noqa: SLF001
        (
            "node-1",
            pb2.TaskResult(
                task_id="task-buffered",
                status=pb2.TASK_STATUS_SUCCEEDED,
                result=dict_to_struct({"value": 1}),
            ),
        )
    )

    results = session.wait_for_results(expected_count=1, timeout_sec=0.1)

    assert len(results) == 1
    assert results[0].task_id == "task-buffered"


def test_native_task_pool_async_batch_helpers_exist() -> None:
    from pycloud_parallel import TaskPool

    session = TaskPool(
        pools={"node-1": SimpleNamespace(owner_client_id="owner", code_version="sha256:test", heartbeat_timeout_sec=30)},
        nodes={},
        task_method="run",
        job_id="job-async-batch",
    )

    with patch.object(session, "map", return_value=[{"value": 1}, None]) as mocked_map, patch.object(
        session,
        "collect_items",
        return_value=[],
    ) as mocked_collect:
        out = asyncio.run(session.amap([1, 2], timeout_sec=0.1))
        collected = asyncio.run(session.acollect_items([{"value": 1}], timeout_sec=0.1))

    assert out == [{"value": 1}, None]
    assert collected == []
    mocked_map.assert_called_once()
    mocked_collect.assert_called_once()
