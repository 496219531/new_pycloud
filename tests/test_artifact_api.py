from __future__ import annotations

import io
import tarfile
from types import SimpleNamespace
from unittest.mock import patch

from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


def test_prepare_artifact_applies_service_and_task_defaults() -> None:
    from pycloud_parallel.controlplane.artifact import Artifact, _normalize_artifact_input, _prepare_artifact

    artifact = Artifact.from_bytes(
        b"def run(**_kwargs):\n    return {'ok': True}\n",
        package_format="py",
        entry_module="demo_artifact",
    )

    prepared_service = _prepare_artifact(
        _normalize_artifact_input(consumer_kind="service", artifact=artifact),
        consumer_kind="service",
    )
    prepared_task = _prepare_artifact(
        _normalize_artifact_input(consumer_kind="task", artifact=artifact),
        consumer_kind="task",
    )

    assert prepared_service.export_mode == "decorator"
    assert prepared_service.export_methods == ()
    assert prepared_service.dependency_policy.mode == "prebuilt"
    assert prepared_service.code_version.startswith("sha256:")

    assert prepared_task.export_mode == "single"
    assert prepared_task.export_methods == ("run",)
    assert prepared_task.dependency_policy.mode == "prebuilt"


def test_service_group_deploy_from_infocenter_accepts_artifact(tmp_path) -> None:
    from pycloud_parallel.controlplane.artifact import Artifact, ArtifactDeps, ArtifactExports
    from pycloud_parallel.execution.service_session import Service

    fake_node = SimpleNamespace(
        node_id="node-1",
        control_addr="127.0.0.1:50061",
        healthy=True,
        schedulable=True,
        drain=False,
        service_worker_available=2,
        capacity=2,
        queued=0,
        python_version="py3.11",
    )
    create_calls = []

    class _FakeNodeControlClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            self.target = target
            self.timeout_sec = timeout_sec

        def create_service_from_bytes(self, **kwargs):
            create_calls.append(dict(kwargs))
            return SimpleNamespace(
                service_id="svc-1",
                service_token="token-1",
                http_base_url="http://127.0.0.1:18081/svc/svc-1",
                heartbeat_timeout_sec=30,
                worker_count=1,
                status=pb2.SERVICE_STATUS_RUNNING,
            )

        def close(self) -> None:
            return None

    artifact = Artifact.from_bytes(
        b"def run(**_kwargs):\n    return {'ok': True}\n",
        package_format="py",
        entry_module="demo_service",
        entry_callable="run",
        exports=ArtifactExports.use_decorator(),
        deps=ArtifactDeps.allow_install(["orjson==3.10.18"]),
        managed_global_names=("CONFIG",),
    )

    with patch(
        "pycloud_parallel.execution.service_session._retry_infocenter_request",
        return_value=((), [fake_node]),
    ), patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient",
        _FakeNodeControlClient,
    ), patch.object(
        Service,
        "_persist_session_cache",
        lambda self: None,
    ), patch.object(
        Service,
        "_start_keepalive",
        lambda self, interval_sec=None: None,
    ):
        group = Service._deploy_from_infocenter(
            infocenter_target="127.0.0.1:50051",
            owner_client_id="owner-demo",
            service_name="demo-artifact-service",
            artifact=artifact,
            session_cache_dir=str(tmp_path),
        )

    try:
        assert len(create_calls) == 1
        create_call = create_calls[0]
        assert create_call["entry_module"] == "demo_service"
        assert create_call["entry_callable"] == "run"
        assert create_call["package_format"] == "py"
        assert create_call["export_mode"] == "decorator"
        assert create_call["export_methods"] == []
        assert create_call["deps"].mode == "allow_install"
        assert create_call["deps"].requirements == ("orjson==3.10.18",)
        assert create_call["managed_global_names"] == ["CONFIG"]
        assert create_call["blob"].startswith(b"def run")
    finally:
        for client in group._clients.values():  # noqa: SLF001
            client.close()


def test_task_pool_session_from_infocenter_accepts_artifact() -> None:
    from pycloud_parallel import TaskPool
    from pycloud_parallel.controlplane.artifact import Artifact, ArtifactDeps

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
        cancel_job=lambda job_id="", reason="": pb2.CancelJobResponse(ok=True),
        close=lambda reason="": None,
        _client=SimpleNamespace(close=lambda: None),
    )

    def _fake_create_task_pool_from_bytes(**kwargs):
        create_calls.append(dict(kwargs))
        return fake_pool_client

    artifact = Artifact.from_bytes(
        b"def run(value=0, **_kwargs):\n    return {'value': value}\n",
        package_format="py",
        entry_module="task_demo",
        entry_callable="run",
        deps=ArtifactDeps.allow_install(["numpy==2.1.0"]),
    )

    with patch("pycloud_parallel.controlplane.infocenter_client.InfoCenterClient") as mocked_infocenter, patch(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.create_task_pool_from_bytes",
        side_effect=_fake_create_task_pool_from_bytes,
    ):
        mocked_infocenter.return_value.__enter__.return_value.select_task_nodes.return_value = [fake_node]
        session = TaskPool._from_infocenter(
            infocenter_target="127.0.0.1:50051",
            job_id="job-native-artifact",
            artifact=artifact,
            worker_count=2,
            node_count=1,
        )

    try:
        assert len(create_calls) == 1
        create_call = create_calls[0]
        assert create_call["entry_module"] == "task_demo"
        assert create_call["entry_callable"] == "run"
        assert create_call["package_format"] == "py"
        assert create_call["deps"].mode == "allow_install"
        assert create_call["deps"].requirements == ("numpy==2.1.0",)
        assert create_call["blob"].startswith(b"def run")
        assert session.methods == ["run"]
    finally:
        session.close()


def test_prepare_artifact_accepts_public_deps_override() -> None:
    from pycloud_parallel.controlplane.artifact import ArtifactDeps, _normalize_artifact_input, _prepare_artifact

    normalized = _normalize_artifact_input(
        consumer_kind="task",
        source=b"def run(**_kwargs):\n    return {'ok': True}\n",
        runtime="py3",
        entry_module="demo_override",
        entry_callable="run",
        package_format="py",
        deps=ArtifactDeps.node_preinstalled(),
    )

    prepared = _prepare_artifact(normalized, consumer_kind="task")

    assert prepared.dependency_policy_mode == "node_preinstalled"
    assert prepared.dependency_allowlist == ()


def test_artifact_from_paths_packages_single_directory(tmp_path) -> None:
    from pycloud_parallel.controlplane.artifact import Artifact, ArtifactExports, _prepare_artifact

    pkg_dir = tmp_path / "demo_pkg"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
    (pkg_dir / "worker.py").write_text(
        "def run(**_kwargs):\n    return {'ok': True}\n",
        encoding="utf-8",
    )
    artifact = Artifact.from_paths(
        pkg_dir,
        entry_module="demo_pkg.worker",
        exports=ArtifactExports.use_decorator(),
    )

    prepared = _prepare_artifact(artifact, consumer_kind="service")

    assert prepared.package_format == "tar.gz"
    with tarfile.open(fileobj=io.BytesIO(prepared.blob), mode="r:gz") as tf:
        names = set(tf.getnames())
    assert "demo_pkg/__init__.py" in names
    assert "demo_pkg/worker.py" in names
