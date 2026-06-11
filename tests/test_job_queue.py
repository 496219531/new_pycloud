from __future__ import annotations

import base64
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import inspect
import json
import socket
import time
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from pycloud_parallel.controlplane.job_queue import JobQueueManager
from pycloud_parallel.controlplane.job_orchestrator import (
    JOB_ORCHESTRATOR_EXPORT_METHODS,
    JOB_ORCHESTRATOR_SERVICE_MODULE,
    JobOrchestratorModule,
    JobOrchestratorServer,
)
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState
from pycloud_parallel.controlplane.startup_service_node import StartupServiceNode
from pycloud_parallel.controlplane.serialization import serialize_arrow_compatible
from pycloud_parallel.data.ref import DataRef, data_ref_to_payload, maybe_data_ref


def test_startup_service_node_rejects_dynamic_service_deploy() -> None:
    node = StartupServiceNode(node_id="startup-only", service_http_bind="")

    with pytest.raises(RuntimeError, match="dynamic service deployment is disabled"):
        node.create_service()


def test_startup_service_node_mounts_python_module_service(tmp_path, monkeypatch) -> None:
    module_path = tmp_path / "startup_calc.py"
    module_path.write_text(
        "def add(x=0, y=0):\n"
        "    return {'value': int(x) + int(y)}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    node = StartupServiceNode(node_id="startup-only", service_http_bind="")
    mount = node.mount_python_module_service(service_name="startup-calc", entry_module="startup_calc")

    code, methods = node._methods_mounted_startup_service(mount.service_id, include_docs=False)  # noqa: SLF001
    assert code == 200
    assert methods["service_id"] == mount.service_id
    assert [item["method"] for item in methods["methods"]] == ["add"]

    code, body = node._invoke_mounted_startup_service(  # noqa: SLF001
        mount.service_id,
        "add",
        {"x": 2, "y": 3},
        "",
        5.0,
    )
    assert code == 200
    assert body["data"] == {"value": 5}


def test_startup_service_node_call_balanced_uses_python_module_mount(tmp_path, monkeypatch) -> None:
    module_path = tmp_path / "startup_calc_call.py"
    module_path.write_text(
        "def add(x=0, y=0):\n"
        "    return {'value': int(x) + int(y)}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    node = StartupServiceNode(node_id="startup-only", service_http_bind="")
    node.mount_python_module_service(service_name="startup-calc", entry_module="startup_calc_call")

    _node_key, body = node.call_balanced("add", {"x": 4, "y": 6}, timeout_sec=5.0)

    assert body["ok"] is True
    assert body["data"] == {"value": 10}


def test_startup_module_mount_accepts_local_ipc_payload_at_invoke(tmp_path, monkeypatch) -> None:
    module_path = tmp_path / "startup_payload_call.py"
    module_path.write_text(
        "def echo(**payload):\n"
        "    return payload\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    from pycloud_parallel.controlplane.local_ipc import _prepare_local_ipc_payload

    node = StartupServiceNode(node_id="startup-only", service_http_bind="")
    node.mount_python_module_service(service_name="startup-payload", entry_module="startup_payload_call")
    payload = _prepare_local_ipc_payload(
        {"job_mode": "hooks", "task_generator_callable": "task_generator"},
        meta={"object_dir": str(tmp_path)},
    )

    _node_key, body = node.call_balanced("echo", payload, timeout_sec=5.0)

    assert body["ok"] is True
    assert body["data"]["job_mode"] == "hooks"
    assert body["data"]["task_generator_callable"] == "task_generator"


def test_local_ipc_payload_prepares_bytes_when_estimated_over_threshold(tmp_path, monkeypatch) -> None:
    from pycloud_parallel.controlplane import local_ipc as local_ipc_mod

    base_policy = local_ipc_mod.get_local_service_payload_policy()
    forced_policy = replace(
        base_policy,
        limits=replace(
            base_policy.limits,
            inline_payload_threshold_bytes=1,
            inline_payload_hard_limit_bytes=10**9,
        ),
    )
    monkeypatch.setattr(local_ipc_mod, "get_local_service_payload_policy", lambda: forced_policy)

    payload = local_ipc_mod._prepare_local_ipc_payload(
        {"items": b"x" * 128},
        meta={"object_dir": str(tmp_path)},
    )

    assert payload != {"items": b"x" * 128}
    assert maybe_data_ref(payload["items"]) is not None


def test_local_ipc_payload_normalizes_file_paths(tmp_path) -> None:
    from pycloud_parallel.controlplane import local_ipc as local_ipc_mod

    source = tmp_path / "demo.txt"
    source.write_text("hello", encoding="utf-8")

    payload = local_ipc_mod._prepare_local_ipc_payload(
        {"file_path": source, "file_name": str(source)},
        meta={"object_dir": str(tmp_path)},
    )

    assert payload["file_path"] == source.resolve()
    assert payload["file_name"] == str(source.resolve())


def test_local_ipc_payload_keeps_large_file_path_inline(tmp_path) -> None:
    from pycloud_parallel.controlplane import local_ipc as local_ipc_mod

    source = tmp_path / "large.bin"
    source.write_bytes(b"x" * (1024 * 1024))

    payload = local_ipc_mod._prepare_local_ipc_payload(
        {"file_path": source},
        meta={"object_dir": str(tmp_path)},
    )

    assert payload["file_path"] == source.resolve()


def test_local_ipc_payload_keeps_small_payload_unwrapped(tmp_path, monkeypatch) -> None:
    from pycloud_parallel.controlplane import local_ipc as local_ipc_mod

    def _fail_pickle_dumps(*_args, **_kwargs):
        raise AssertionError("local IPC small payload should rely on send/recv serialization")

    monkeypatch.setattr(local_ipc_mod.pickle, "dumps", _fail_pickle_dumps)

    payload = local_ipc_mod._prepare_local_ipc_payload(
        {"items": [{"value": i} for i in range(2)]},
        meta={"object_dir": str(tmp_path)},
    )

    assert payload == {"items": [{"value": 0}, {"value": 1}]}


def test_local_put_payload_data_uses_file_commit_without_reading_whole_file(tmp_path, monkeypatch) -> None:
    from pathlib import Path
    from pycloud_parallel.controlplane import local_ipc as local_ipc_mod

    source = tmp_path / "payload.bin"
    source.write_bytes(b"stream-local-file")
    source_size = source.stat().st_size
    original_read_bytes = Path.read_bytes

    def _fail_read_bytes(self):  # noqa: ANN001
        if Path(self) == source:
            raise AssertionError("local file-backed payload must not read_bytes() the whole file")
        return original_read_bytes(self)

    monkeypatch.setattr(Path, "read_bytes", _fail_read_bytes)

    ref = local_ipc_mod._put_local_payload_data(
        source,
        meta={"object_dir": str(tmp_path / "objects")},
    )

    assert ref.object_id.startswith("sha256:")
    assert ref.size_bytes == source_size


def test_startup_module_mount_decodes_transport_envelope_payload_at_invoke(tmp_path, monkeypatch) -> None:
    module_path = tmp_path / "startup_envelope_call.py"
    module_path.write_text(
        "def echo(**payload):\n"
        "    return payload\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    from pycloud_parallel.controlplane.payload_transport import encode_payload_for_transport
    from pycloud_parallel.controlplane.config import get_payload_policy

    node = StartupServiceNode(node_id="startup-only", service_http_bind="")
    node.mount_python_module_service(service_name="startup-envelope", entry_module="startup_envelope_call")
    payload = encode_payload_for_transport(
        {"job_mode": "hooks", "task_generator_callable": "task_generator"},
        policy=get_payload_policy("http_call"),
        context="service_owner",
        mode="legacy_v1",
    )

    _node_key, body = node.call_balanced("echo", payload, timeout_sec=5.0, serialization_mode="legacy_v1")

    assert body["ok"] is True
    assert body["data"]["job_mode"] == "hooks"
    assert body["data"]["task_generator_callable"] == "task_generator"


def test_startup_service_node_updates_python_module_managed_globals(tmp_path, monkeypatch) -> None:
    module_path = tmp_path / "startup_cfg.py"
    module_path.write_text(
        "CFG = {}\n"
        "LAST_CONTEXT = {}\n\n"
        "def apply_managed_globals(values, **context):\n"
        "    global CFG, LAST_CONTEXT\n"
        "    CFG = dict(values.get('cfg') or {})\n"
        "    LAST_CONTEXT = dict(context)\n\n"
        "def read_cfg(_service_id=''):\n"
        "    return {'cfg': CFG, 'service_id': _service_id, 'context_service_id': LAST_CONTEXT.get('service_id')}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    node = StartupServiceNode(node_id="startup-only", service_http_bind="")
    sync_requests = []
    node._infocenter_registrar = SimpleNamespace(request_sync=lambda: sync_requests.append("sync"))  # noqa: SLF001
    mount = node.mount_python_module_service(
        service_name="startup-cfg",
        entry_module="startup_cfg",
        managed_global_names=("cfg",),
    )
    digest = node.update_globals({"cfg": {"mode": "fast"}}, service_id=mount.service_id)

    assert digest.startswith("sha256:")
    assert node.globals_digests == {mount.service_id: digest}
    code, body = node._invoke_mounted_startup_service(  # noqa: SLF001
        mount.service_id,
        "read_cfg",
        {},
        "",
        5.0,
    )
    assert code == 200
    assert body["data"]["cfg"] == {"mode": "fast"}
    assert body["data"]["service_id"] == mount.service_id
    assert body["data"]["context_service_id"] == mount.service_id
    assert sync_requests == ["sync"]


def test_startup_service_node_applies_pending_managed_globals_on_mount(tmp_path, monkeypatch) -> None:
    module_path = tmp_path / "startup_pending_cfg.py"
    module_path.write_text(
        "CFG = {}\n\n"
        "def apply_managed_globals(values, **_context):\n"
        "    global CFG\n"
        "    CFG = dict(values.get('cfg') or {})\n\n"
        "def read_cfg():\n"
        "    return {'cfg': CFG}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    node = StartupServiceNode(node_id="startup-only", service_http_bind="")
    service_id = "startup-pending-service"
    digest = node.update_globals({"cfg": {"mode": "pending"}}, service_id=service_id)
    mount = node.mount_python_module_service(
        service_name="startup-pending",
        entry_module="startup_pending_cfg",
        service_id=service_id,
        managed_global_names=("cfg",),
    )

    assert node.globals_digests[mount.service_id] == digest
    code, body = node._invoke_mounted_startup_service(  # noqa: SLF001
        mount.service_id,
        "read_cfg",
        {},
        "",
        5.0,
    )
    assert code == 200
    assert body["data"]["cfg"] == {"mode": "pending"}


def test_startup_service_node_infocenter_registration_is_background(monkeypatch) -> None:
    events = []

    class _FakeRegistrar:
        def __init__(self, **kwargs):
            events.append(("init", kwargs))

        def sync_now(self):
            events.append(("sync_now", {}))
            raise AssertionError("startup registration should not synchronously sync")

        def start(self):
            events.append(("start", {}))

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.registrar.NodeInfoCenterRegistrar",
        _FakeRegistrar,
    )
    node = StartupServiceNode(node_id="startup-only", service_http_bind="")

    node.start_infocenter_registration(infocenter_target="http://127.0.0.1:9", heartbeat_sec=1)

    assert [name for name, _payload in events] == ["init", "start"]


def test_nodecontrol_and_job_orchestrator_start_uses_service_startup(monkeypatch) -> None:
    server = JobOrchestratorServer(
        bind="127.0.0.1:0",
        infocenter_addr="127.0.0.1:50051",
        node_id="job-orchestrator-test",
        queue_capacity=123,
        version="job-orch-test-version",
    )

    assert issubclass(StartupServiceNode, NodeControlState)
    assert server._service_module_name == JOB_ORCHESTRATOR_SERVICE_MODULE  # noqa: SLF001
    assert server._node is None  # noqa: SLF001
    assert server.module is None

    calls = {}
    globals_updates = []
    sync_requests = []

    fake_node = SimpleNamespace(
        _local_service_id=server.service_id,
        service_http_base_url="http://127.0.0.1:50053",
        update_globals=lambda values, service_id="": globals_updates.append((service_id, values)),
        request_infocenter_sync=lambda: sync_requests.append("sync"),
        close=lambda: None,
    )
    fake_module = SimpleNamespace(
        business_module=lambda service_id="": JobOrchestratorModule(service_name="job-orchestrator"),
        start=lambda **kwargs: calls.setdefault("module_start", kwargs),
        close=lambda service_id="": calls.setdefault("module_close", service_id),
    )
    server._service_module = fake_module  # noqa: SLF001

    def _fake_startup(**kwargs):
        calls["startup"] = kwargs
        return fake_node

    monkeypatch.setattr("pycloud_parallel.execution.service_session.Service.startup", _fake_startup)

    server.start()

    startup_kwargs = calls["startup"]
    assert startup_kwargs["source"] is fake_module
    assert startup_kwargs["service_name"] == "job-orchestrator"
    assert startup_kwargs["export_methods"] == JOB_ORCHESTRATOR_EXPORT_METHODS
    assert startup_kwargs["bind"] == "127.0.0.1:0"
    assert startup_kwargs["target"] == "127.0.0.1:50051"
    assert startup_kwargs["node_id"] == "job-orchestrator-test"
    assert startup_kwargs["service_id"] == server.service_id
    assert startup_kwargs["worker_count"] == 1
    assert startup_kwargs["policy_id"] == server.job_orch_policy_id
    assert startup_kwargs["initial_globals"]["service_id"] == server.service_id
    assert startup_kwargs["initial_globals"]["service_name"] == "job-orchestrator"
    assert startup_kwargs["queue_capacity"] == 123
    assert startup_kwargs["version"] == "job-orch-test-version"
    assert startup_kwargs["start"] is True
    assert globals_updates
    assert globals_updates[0][0] == server.service_id
    assert globals_updates[0][1]["service_name"] == "job-orchestrator"
    assert calls["module_start"]["controlplane_target"] == "127.0.0.1:50051"
    assert calls["module_start"]["base_url"] == "http://127.0.0.1:50053"
    assert sync_requests == ["sync"]


def test_job_orchestrator_instances_have_independent_startup_service_ids() -> None:
    first = JobOrchestratorServer(
        bind="127.0.0.1:0",
        infocenter_addr="127.0.0.1:50051",
        node_id="job-orch-a",
    )
    second = JobOrchestratorServer(
        bind="127.0.0.1:0",
        infocenter_addr="127.0.0.1:50051",
        node_id="job-orch-b",
    )

    assert first.node_id != second.node_id
    assert first.service_id != second.service_id
    assert first._node is None  # noqa: SLF001
    assert second._node is None  # noqa: SLF001


def test_service_startup_preflight_rejects_same_service_on_different_endpoint(monkeypatch) -> None:
    from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2
    import pycloud_parallel.controlplane.job_orchestrator_service as job_orchestrator_service

    route = SimpleNamespace(
        service_name="job-orchestrator",
        status=pb2.SERVICE_STATUS_RUNNING,
        node_healthy=True,
        node_id="existing-job-orch",
        node_instance_id="existing-job-orch-inst",
        http_base_url="http://127.0.0.1:50054/svc/existing",
        control_addr="",
    )

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.infocenter_client.InfoCenterClient.list_service_routes",
        lambda self, *, service_name="", healthy_only=True, limit=100, **kwargs: [route],
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.infocenter_client.InfoCenterClient.list_service_routes_for_exclusive_check",
        lambda self, *, service_name="", limit=100, **kwargs: [route],
        raising=False,
    )

    from pycloud_parallel.execution.service_session import Service

    with pytest.raises(RuntimeError, match="different endpoint"):
        Service.startup(
            source=job_orchestrator_service,
            service_name="job-orchestrator",
            export_methods=JOB_ORCHESTRATOR_EXPORT_METHODS,
            bind="127.0.0.1:50053",
            target="127.0.0.1:50051",
            node_id="job-orch-test",
        )


def test_service_startup_preflight_allows_same_endpoint(tmp_path, monkeypatch) -> None:
    from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2
    import importlib

    module_path = tmp_path / "startup_same_endpoint_demo.py"
    module_path.write_text(
        "def submit_job(**_kwargs):\n"
        "    return {'ok': True}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    startup_module = importlib.import_module("startup_same_endpoint_demo")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    route = SimpleNamespace(
        service_name="job-orchestrator",
        status=pb2.SERVICE_STATUS_RUNNING,
        node_healthy=True,
        node_id="existing-job-orch",
        node_instance_id="existing-job-orch-inst",
        http_base_url=f"http://127.0.0.1:{port}/svc/existing",
        control_addr="",
    )

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.infocenter_client.InfoCenterClient.list_service_routes",
        lambda self, *, service_name="", healthy_only=True, limit=100, **kwargs: [route],
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.infocenter_client.InfoCenterClient.list_service_routes_for_exclusive_check",
        lambda self, *, service_name="", limit=100, **kwargs: [route],
        raising=False,
    )

    from pycloud_parallel.execution.service_session import Service

    node = Service.startup(
        source=startup_module,
        service_name="job-orchestrator",
        export_methods=("submit_job",),
        bind=f"127.0.0.1:{port}",
        target="127.0.0.1:50051",
        node_id="job-orch-test",
    )
    node.close()


def test_job_queue_manager_default_shared_pool_idle_ttl_is_longer(monkeypatch) -> None:
    monkeypatch.delenv("PYCLOUD_JOB_QUEUE_POOL_IDLE_TTL_SEC", raising=False)

    queue = JobQueueManager()

    assert queue._pool_idle_ttl_sec == 300  # noqa: SLF001


def test_job_queue_manager_shared_pool_idle_ttl_can_be_overridden(monkeypatch) -> None:
    monkeypatch.setenv("PYCLOUD_JOB_QUEUE_POOL_IDLE_TTL_SEC", "600")

    from_env = JobQueueManager()
    explicit = JobQueueManager(pool_idle_ttl_sec=120)

    assert from_env._pool_idle_ttl_sec == 600  # noqa: SLF001
    assert explicit._pool_idle_ttl_sec == 120  # noqa: SLF001


def _hook_job_payload(
    *,
    job_id: str,
    client_id: str = "client-a",
    priority: int = 0,
    job_payload: dict | None = None,
    **extra,
) -> dict:
    payload = {
        "job_id": job_id,
        "client_id": client_id,
        "priority": priority,
        "job_mode": "hooks",
        "blob_b64": base64.b64encode(
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': value}\n\n"
            b"def task_generator(items=None, **_kwargs):\n"
            b"    return list(items or [{'value': 1}])\n"
        ).decode("utf-8"),
        "entry_module": "job_hook_demo",
        "task_generator_callable": "task_generator",
        "job_payload": dict(job_payload or {"items": [{"value": 1}]}),
    }
    payload.update(extra)
    return payload


def test_submit_and_cancel_waiting_job() -> None:
    queue = JobQueueManager()
    job = queue.submit_job(_hook_job_payload(job_id="job-waiting-1", priority=2))
    assert job.status == "WAITING"

    cancelled = queue.cancel_job("job-waiting-1")
    assert cancelled is not None
    assert cancelled.status == "CANCELLED"
    assert cancelled.cancel_requested is True


def test_cancel_job_rejects_auth_token_mismatch() -> None:
    queue = JobQueueManager()
    queue.submit_job(
        _hook_job_payload(job_id="job-auth-1"),
        auth_token="token-a",
    )

    with pytest.raises(PermissionError, match="cancel auth failed"):
        queue.cancel_job("job-auth-1", auth_token="token-b")

    cancelled = queue.cancel_job("job-auth-1", auth_token="token-a")
    assert cancelled is not None
    assert cancelled.status == "CANCELLED"


def test_cancel_job_rejects_expired_auth_token() -> None:
    queue = JobQueueManager()
    job = queue.submit_job(
        _hook_job_payload(job_id="job-auth-expired"),
        auth_token="token-a",
    )
    job.owner_token_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)

    with pytest.raises(PermissionError, match="cancel auth expired"):
        queue.cancel_job("job-auth-expired", auth_token="token-a")


def test_submit_job_rejects_unresolvable_object_ref_payloads() -> None:
    queue = JobQueueManager()
    with pytest.raises(ValueError, match="resolvable locator"):
        queue.submit_job(
            _hook_job_payload(
                job_id="job-ref-1",
                job_payload={
                    "blob": data_ref_to_payload(
                        DataRef(
                            ref_id="sha256:" + ("c" * 64),
                            storage_id="sha256:" + ("c" * 64),
                            logical_type="json",
                            format="json",
                            size_bytes=12,
                            materialize_as="json",
                            locator_kind="node_local",
                            locator_token="",
                        )
                    )
                },
            )
        )


def test_job_queue_manager_submit_job_uses_unified_inbound_normalizer(monkeypatch) -> None:
    queue = JobQueueManager()
    captured = {}

    def _fake_normalize(payload, *, object_dir, policy, resolve_object_refs=None):
        del resolve_object_refs
        captured["payload"] = dict(payload)
        captured["object_dir"] = object_dir
        captured["mode"] = policy.mode
        return dict(payload)

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.job_queue.normalize_inbound_payload",
        _fake_normalize,
    )

    job = queue.submit_job(_hook_job_payload(job_id="job-policy-1"))

    assert job.job_id == "job-policy-1"
    assert captured["mode"] == "job_submit"
    assert captured["payload"]["job_payload"] == {"items": [{"value": 1}]}


def test_pick_next_job_prefers_priority_then_submission_order() -> None:
    queue = JobQueueManager()
    low = queue.submit_job(_hook_job_payload(job_id="job-low", priority=1))
    high = queue.submit_job(_hook_job_payload(job_id="job-high", client_id="client-b", priority=9))
    with queue._lock:  # noqa: SLF001
        selected = queue._pick_next_job_locked()  # noqa: SLF001
    assert selected is not None
    assert selected.job_id == high.job_id
    assert selected.job_id != low.job_id


def test_reorder_waiting_job_updates_waiting_order() -> None:
    queue = JobQueueManager()
    queue.submit_job(_hook_job_payload(job_id="job-1", priority=1))
    queue.submit_job(_hook_job_payload(job_id="job-2", priority=1))
    queue.submit_job(_hook_job_payload(job_id="job-3", priority=1))

    moved = queue.reorder_job("job-3", direction="up")
    assert moved is not None
    moved = queue.reorder_job("job-3", direction="up")
    assert moved is not None

    summary = queue.summary()
    assert [item["job_id"] for item in summary["waiting_jobs"]] == ["job-3", "job-1", "job-2"]


def test_job_queue_summary_includes_timing_aggregate() -> None:
    queue = JobQueueManager()
    first = queue.submit_job(_hook_job_payload(job_id="job-timing-1"))
    second = queue.submit_job(_hook_job_payload(job_id="job-timing-2"))
    first.status = "SUCCEEDED"
    first.finished_at = datetime.now(timezone.utc)
    first.timing.update({"queue_wait_ms": 100.0, "running_tasks_ms": 300.0, "total_ms": 500.0, "pool_action": "reuse", "pool_reuse_count": 1})
    second.status = "FAILED"
    second.finished_at = datetime.now(timezone.utc)
    second.timing.update({"queue_wait_ms": 200.0, "running_tasks_ms": 500.0, "total_ms": 900.0, "pool_action": "rebuild", "executor_rebuild_count": 1})

    summary = queue.summary()

    assert "timing" in summary
    assert summary["timing"]["job_count"] == 2
    assert summary["timing"]["avg_queue_wait_ms"] == 150.0
    assert summary["timing"]["avg_running_tasks_ms"] == 400.0
    assert summary["timing"]["max_total_ms"] == 900.0
    assert summary["timing"]["pool_reuse_count"] == 1
    assert summary["timing"]["pool_rebuild_count"] == 1
    assert summary["recent_jobs"][0]["timing"]


def test_job_orchestrator_reorder_job_requires_admin_token() -> None:
    module = JobOrchestratorModule(
        service_name="job-orchestrator",
        admin_token="admin-token",
    )
    queue = module.job_queue
    queue.submit_job(_hook_job_payload(job_id="job-1"))
    queue.submit_job(_hook_job_payload(job_id="job-2"))
    queue.submit_job(_hook_job_payload(job_id="job-3"))
    with queue._cv:  # noqa: SLF001
        queue._waiting_order = ["job-1", "job-2", "job-3"]  # noqa: SLF001

    status, body = module.reorder_job("job-3", direction="up", token="")
    assert status == 403
    assert body["error"] == "admin auth required"

    status, body = module.reorder_job("job-3", direction="up", token="owner-token")
    assert status == 403
    assert body["error"] == "admin auth required"

    status, body = module.reorder_job("job-3", direction="up", token="admin-token")
    assert status == 200
    waiting_ids = [item["job_id"] for item in body["queue"]["waiting_jobs"]]
    assert waiting_ids == ["job-1", "job-3", "job-2"]


def test_job_orchestrator_forwards_owner_api_token_to_taskpool() -> None:
    module = JobOrchestratorModule(
        service_name="job-orchestrator",
        api_token="node-owner-token",
    )

    assert module.job_queue._api_token == "node-owner-token"  # noqa: SLF001


def test_job_queue_manager_passes_owner_api_token_to_created_taskpool() -> None:
    queue = JobQueueManager(api_token="node-owner-token")
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    state = queue.submit_job(_hook_job_payload(job_id="job-api-token-1"))
    state.status = "RUNNING"

    class _FakePool:
        job_id = "job-api-token-1"

        def update_globals(self, values):
            del values

        def imap_unordered(self, payloads, **kwargs):
            del kwargs
            for index, _item in enumerate(payloads):
                yield index, {"ok": True}

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            del kwargs

    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=_FakePool()) as mocked:
        queue._run_job("job-api-token-1")  # noqa: SLF001

    assert mocked.call_args.kwargs["api_token"] == "node-owner-token"


def test_run_job_with_hooks_uses_generator_handler_and_finalize() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    value = int(value)\n"
        b"    return {'value': value, 'square': value * value}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    for i in range(count):\n"
        b"        yield {'value': value + i}\n\n"
        b"def handle_result(index, result, state=None, **_kwargs):\n"
        b"    state.setdefault('squares', []).append(result['square'])\n\n"
        b"def finalize(state=None, **_kwargs):\n"
        b"    return {'sum_square': sum(state.get('squares', []))}\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-1",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "handle_result_callable": "handle_result",
            "finalize_callable": "finalize",
            "job_payload": {"value": 2, "count": 3},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-1"
            self.updated_globals = []

        def imap_unordered(self, payloads, **kwargs):
            assert kwargs["max_in_flight"] >= 1
            assert kwargs["max_infra_retries"] == 1
            assert kwargs["retry_backoff_ms"] == 0
            items = list(payloads)
            assert items == [{"value": 2}, {"value": 3}, {"value": 4}]
            for idx, item in enumerate(items):
                value = int(item["value"])
                yield idx, {"value": value, "square": value * value}

        def update_globals(self, values):
            self.updated_globals.append(dict(values))

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-hooks-1")  # noqa: SLF001

    mocked.assert_called_once()
    job = queue.get_job("job-hooks-1")
    assert job is not None
    assert job.status == "SUCCEEDED"
    assert [item["result"]["square"] for item in job.results] == [4, 9, 16]
    assert job.final_result == {"sum_square": 29}


def test_run_job_with_hooks_accepts_update_globals_callable_name() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value)}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n\n"
        b"def build_globals(value=0, count=1, **_kwargs):\n"
        b"    return {'cfg': {'base': int(value), 'count': int(count)}}\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-globals-name",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_globals_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "job_payload": {"value": 2, "count": 3},
            "update_globals": "build_globals",
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-globals-name"
            self.updated_globals = []

        def imap_unordered(self, payloads, **kwargs):
            del kwargs
            for idx, item in enumerate(list(payloads)):
                yield idx, {"value": int(item["value"])}

        def update_globals(self, values):
            self.updated_globals.append(dict(values))

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-hooks-globals-name")  # noqa: SLF001

    assert fake_pool.updated_globals == [{"cfg": {"base": 2, "count": 3}}]
    assert mocked.call_args.kwargs["managed_global_names"] == ["cfg"]
    job = queue.get_job("job-hooks-globals-name")
    assert job is not None
    assert job.status == "SUCCEEDED"


def test_run_job_with_hooks_uses_module_source_for_taskpool() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value)}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-entryfunc",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_entryfunc_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "job_payload": {"value": 2, "count": 3},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-entryfunc"

        def imap_unordered(self, payloads, **kwargs):
            del kwargs
            for idx, item in enumerate(list(payloads)):
                yield idx, {"value": int(item["value"])}

        def update_globals(self, values):
            return values

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-hooks-entryfunc")  # noqa: SLF001

    call_kwargs = mocked.call_args.kwargs
    assert call_kwargs["source"] == module_blob
    assert call_kwargs["entry_module"] == "job_hooks_entryfunc_demo"
    assert call_kwargs["entry_callable"] == "run"
    assert call_kwargs["package_format"] == "py"
    assert "func" not in call_kwargs
    assert "blob" not in call_kwargs
    assert "entry_func" not in call_kwargs


def test_run_job_with_hooks_forwards_requested_taskpool_mode_and_fixed_policy() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value)}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-requested-policy",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
                "entry_module": "job_hooks_requested_policy_demo",
                "entry_callable": "run",
                "package_format": "py",
                "task_generator_callable": "task_generator",
                "task_serialization_mode": "pickle_stable_v1",
                "job_payload": {"value": 2, "count": 1},
            }
        )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-requested-policy"

        def imap_unordered(self, payloads, **kwargs):
            del kwargs
            for idx, item in enumerate(list(payloads)):
                yield idx, {"value": int(item["value"])}

        def update_globals(self, values):
            return values

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-hooks-requested-policy")  # noqa: SLF001

    call_kwargs = mocked.call_args.kwargs
    assert call_kwargs["serialization_mode"] == "pickle_stable_v1"
    assert call_kwargs["policy_id"] == queue._taskpool_policy_id


def test_run_job_with_hooks_defaults_taskpool_policy_when_submit_omits_execution_policy() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value)}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-default-policy",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_default_policy_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "job_payload": {"value": 2, "count": 1},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-default-policy"

        def imap_unordered(self, payloads, **kwargs):
            del kwargs
            for idx, item in enumerate(list(payloads)):
                yield idx, {"value": int(item["value"])}

        def update_globals(self, values):
            return values

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-hooks-default-policy")  # noqa: SLF001

    call_kwargs = mocked.call_args.kwargs
    assert call_kwargs["serialization_mode"] == ""
    assert call_kwargs["policy_id"] == queue._taskpool_policy_id


def test_run_job_with_hooks_uses_default_taskpool_policy_when_submit_omits_policy_fields() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value)}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-default-policy",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_default_policy_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "job_payload": {"value": 2, "count": 1},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-default-policy"

        def imap_unordered(self, payloads, **kwargs):
            del kwargs
            for idx, item in enumerate(list(payloads)):
                yield idx, {"value": int(item["value"])}

        def update_globals(self, values):
            return values

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-hooks-default-policy")  # noqa: SLF001

    call_kwargs = mocked.call_args.kwargs
    assert call_kwargs["serialization_mode"] == ""
    assert call_kwargs["policy_id"] == queue._taskpool_policy_id


def test_run_job_with_hooks_forwards_task_resource_paths_to_task_pool() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value)}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-task-resources",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_task_resources_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "task_resource_paths": ["worker/data.csv"],
            "task_serialization_mode": "pickle_stable_v1",
            "job_payload": {"value": 2, "count": 1},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-task-resources"

        def imap_unordered(self, payloads, **kwargs):
            del kwargs
            for idx, item in enumerate(list(payloads)):
                yield idx, {"value": int(item["value"])}

        def update_globals(self, values):
            return values

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-hooks-task-resources")  # noqa: SLF001

    call_kwargs = mocked.call_args.kwargs
    assert call_kwargs["source"] == module_blob
    assert call_kwargs["entry_module"] == "job_hooks_task_resources_demo"
    assert call_kwargs["entry_callable"] == "run"
    assert call_kwargs["package_format"] == "py"
    assert call_kwargs["resource_paths"] == ["worker/data.csv"]
    assert call_kwargs["serialization_mode"] == "pickle_stable_v1"
    assert call_kwargs["policy_id"] == queue._taskpool_policy_id
    assert "entry_func" not in call_kwargs


def test_run_job_with_hooks_accepts_direct_payload_list_task_generator() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = b"def run(value=0, **_kwargs):\n    return {'value': int(value)}\n"
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-direct-payloads",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_direct_payloads_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": [{"value": 7}, {"value": 8}],
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-direct-payloads"

        def imap_unordered(self, payloads, **kwargs):
            del kwargs
            items = list(payloads)
            assert items == [{"value": 7}, {"value": 8}]
            for idx, item in enumerate(items):
                yield idx, {"value": int(item["value"])}

        def update_globals(self, values):
            return values

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool):
        queue._run_job("job-hooks-direct-payloads")  # noqa: SLF001

    job = queue.get_job("job-hooks-direct-payloads")
    assert job is not None
    assert job.status == "SUCCEEDED"


def test_run_job_with_hooks_accepts_update_globals_dict() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value)}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-globals-dict",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_globals_dict_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "job_payload": {"value": 1, "count": 2},
            "update_globals": {"cfg": {"mode": "fast"}},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-globals-dict"
            self.updated_globals = []

        def imap_unordered(self, payloads, **kwargs):
            del kwargs
            for idx, item in enumerate(list(payloads)):
                yield idx, {"value": int(item["value"])}

        def update_globals(self, values):
            self.updated_globals.append(dict(values))

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-hooks-globals-dict")  # noqa: SLF001

    assert fake_pool.updated_globals == [{"cfg": {"mode": "fast"}}]
    assert mocked.call_args.kwargs["managed_global_names"] == ["cfg"]
    job = queue.get_job("job-hooks-globals-dict")
    assert job is not None
    assert job.status == "SUCCEEDED"


def test_run_job_with_hooks_accepts_blob_ref_payload() -> None:
    from pycloud_parallel.data.ref import DataRef, data_ref_to_payload

    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    value = int(value)\n"
        b"    return {'value': value, 'square': value * value}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    for i in range(count):\n"
        b"        yield {'value': value + i}\n\n"
        b"def handle_result(index, result, state=None, **_kwargs):\n"
        b"    state.setdefault('items', []).append(result)\n\n"
        b"def finalize(state=None, **_kwargs):\n"
        b"    return {'count': len(state.get('items', []))}\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-ref-1",
            "client_id": "client-a",
            "priority": 5,
            "blob_ref": data_ref_to_payload(
                DataRef(
                    ref_id="sha256:" + ("b" * 64),
                    storage_id="sha256:" + ("b" * 64),
                    logical_type="bytes",
                    format="py",
                    size_bytes=len(module_blob),
                    materialize_as="bytes",
                    locator_kind="node_local",
                    locator_token="",
                )
            ),
            "blob_control_addr": "127.0.0.1:50061",
            "entry_module": "job_hooks_ref_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "handle_result_callable": "handle_result",
            "finalize_callable": "finalize",
            "job_payload": {"value": 2, "count": 3},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-ref-1"

        def imap_unordered(self, payloads, **kwargs):
            del kwargs
            items = list(payloads)
            assert items == [{"value": 2}, {"value": 3}, {"value": 4}]
            for idx, item in enumerate(items):
                value = int(item["value"])
                yield idx, {"value": value, "square": value * value}

        def update_globals(self, values):
            return values

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with (
        patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool),
        patch(
            "pycloud_parallel.controlplane.job_queue.NodeControlClient.download_object_bytes",
            return_value=module_blob,
        ),
    ):
        queue._run_job("job-hooks-ref-1")  # noqa: SLF001

    job = queue.get_job("job-hooks-ref-1")
    assert job is not None
    assert job.status == "SUCCEEDED"
    assert job.final_result == {"count": 3}


def test_job_queue_manager_submit_job_tracks_controlplane_data_ref_in_nested_business_blob_ref() -> None:
    from pycloud_parallel.data.ref import DataRef

    queue = JobQueueManager()

    data_ref = DataRef(
        ref_id="sha256:" + ("b" * 64),
        storage_id="sha256:" + ("b" * 64),
        format="bin",
        size_bytes=16,
        locator_kind="controlplane",
        locator_token="http://127.0.0.1:50051",
    )
    job = queue.submit_job(
        {
            "job_id": "job-invalid-job-payload-ref",
            "client_id": "client-a",
            "entry_module": "job_demo",
            "job_mode": "hooks",
            "blob_b64": base64.b64encode(
                b"def run(**_kwargs):\n    return {}\n\ndef task_generator(**_kwargs):\n    return []\n"
            ).decode("utf-8"),
            "package_format": "py",
            "job_payload": {"blob_ref": data_ref},
        }
    )

    assert job.staged_ref_ids == [data_ref.ref_id]
    assert job.payload_schema_version == 2
    assert job.payload["job_payload"]["blob_ref"] == data_ref


def test_resolve_payload_data_refs_eager_falls_back_across_replicas(monkeypatch, request) -> None:
    from pycloud_parallel.controlplane import config as config_mod
    from pycloud_parallel.data.ref import DataRef
    from pycloud_parallel.controlplane.data_registry import ResolvedDataRef
    from pycloud_parallel.controlplane.job_queue import _resolve_payload_data_refs

    monkeypatch.setenv("PYCLOUD_JOBQUEUE_RESOLVE_REFS", "eager")
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    data_ref = DataRef(
        ref_id="sha256:" + ("c" * 64),
        storage_id="sha256:" + ("c" * 64),
        format="json",
        size_bytes=32,
        logical_type="json",
        materialize_as="json",
        locator_kind="controlplane",
        locator_token="http://127.0.0.1:50051",
    )
    attempts = []

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.job_queue.DataRegistryClient.resolve",
        lambda self, ref: ResolvedDataRef(
            ref=data_ref,
            control_addr="127.0.0.1:50062",
            locator_kind="node_control",
            locator_token="127.0.0.1:50062",
            via_registry=True,
            replicas=(
                {
                    "control_addr": "127.0.0.1:50061",
                    "node_id": "node-1",
                    "node_instance_id": "node-1-inst",
                },
                {
                    "control_addr": "127.0.0.1:50062",
                    "node_id": "node-2",
                    "node_instance_id": "node-2-inst",
                },
            ),
        ),
    )

    class _FakeNodeClient:
        def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
            del timeout_sec
            self.target = target

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def fetch_result_ref_data(self, result_ref):
            del result_ref
            attempts.append(self.target)
            if self.target.endswith(":50061"):
                raise RuntimeError("replica unavailable")
            return {"value": 1}

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.job_queue.NodeControlClient",
        _FakeNodeClient,
    )

    resolved = _resolve_payload_data_refs(
        {"job_payload": {"blob_ref": data_ref}},
        registry_target="http://127.0.0.1:50051",
        timeout_sec=1.0,
    )

    assert resolved["job_payload"]["blob_ref"] == {"value": 1}
    assert attempts == ["127.0.0.1:50061", "127.0.0.1:50062"]


def test_resolve_payload_data_refs_defaults_to_defer_to_worker(monkeypatch, request) -> None:
    from pycloud_parallel.controlplane import config as config_mod
    from pycloud_parallel.data.ref import DataRef
    from pycloud_parallel.controlplane.job_queue import _resolve_payload_data_refs

    monkeypatch.delenv("PYCLOUD_JOBQUEUE_RESOLVE_REFS", raising=False)
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    data_ref = DataRef(
        ref_id="sha256:" + ("f" * 64),
        storage_id="sha256:" + ("f" * 64),
        format="json",
        size_bytes=32,
        logical_type="json",
        materialize_as="json",
        locator_kind="node_control",
        locator_token="127.0.0.1:50061",
        control_addr="127.0.0.1:50061",
    )

    class _ForbiddenNodeClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("defer_to_worker must not materialize DataRef in job-orch")

    monkeypatch.setattr("pycloud_parallel.controlplane.job_queue.NodeControlClient", _ForbiddenNodeClient)
    resolved = _resolve_payload_data_refs(
        {"job_payload": {"blob_ref": data_ref}},
        registry_target="http://127.0.0.1:50051",
        timeout_sec=1.0,
    )

    assert resolved["job_payload"]["blob_ref"] == data_ref


def test_defer_payload_data_refs_still_rejects_unresolvable_ref(monkeypatch, request) -> None:
    from pycloud_parallel.controlplane import config as config_mod
    from pycloud_parallel.data.ref import DataRef
    from pycloud_parallel.controlplane.job_queue import _resolve_payload_data_refs

    monkeypatch.setenv("PYCLOUD_JOBQUEUE_RESOLVE_REFS", "defer_to_worker")
    config_mod.reload_config()
    request.addfinalizer(config_mod.reload_config)

    data_ref = DataRef(
        ref_id="sha256:" + ("1" * 64),
        storage_id="sha256:" + ("1" * 64),
        format="json",
        size_bytes=32,
        logical_type="json",
        materialize_as="json",
        locator_kind="node_local",
        locator_token="",
    )

    with pytest.raises(ValueError, match="resolvable locator"):
        _resolve_payload_data_refs(
            {"job_payload": {"blob_ref": data_ref}},
            registry_target="http://127.0.0.1:50051",
            timeout_sec=1.0,
        )


def test_submit_job_rejects_nested_unresolvable_business_blob_ref() -> None:
    queue = JobQueueManager()
    with pytest.raises(ValueError, match="resolvable locator"):
        queue.submit_job(
            {
                "job_id": "job-nested-ref-1",
                "client_id": "client-a",
                "entry_module": "task_demo",
                "job_mode": "hooks",
                "blob_b64": base64.b64encode(
                    b"def run(**_kwargs):\n    return {}\n\ndef task_generator(**_kwargs):\n    return []\n"
                ).decode("utf-8"),
                "package_format": "py",
                "job_payload": {
                    "blob_ref": data_ref_to_payload(
                        DataRef(
                            ref_id="sha256:" + ("e" * 64),
                            storage_id="sha256:" + ("e" * 64),
                            logical_type="json",
                            format="json",
                            size_bytes=12,
                            materialize_as="json",
                            locator_kind="node_local",
                            locator_token="",
                        )
                    )
                },
            }
        )


def test_job_queue_manager_close_releases_staged_refs(monkeypatch) -> None:
    from pycloud_parallel.data.ref import DataRef

    released = []
    queue = JobQueueManager()
    queue._controlplane_target = "http://127.0.0.1:50051"  # noqa: SLF001
    data_ref = DataRef(
        ref_id="sha256:" + ("d" * 64),
        storage_id="sha256:" + ("d" * 64),
        format="json",
        size_bytes=24,
        logical_type="json",
        materialize_as="json",
        locator_kind="controlplane",
        locator_token="http://127.0.0.1:50051",
    )
    job = queue.submit_job(_hook_job_payload(job_id="job-close-release", job_payload={"cfg": data_ref}))

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.job_queue.DataRegistryClient.release",
        lambda self, ref_id: released.append(ref_id) or {"ok": True},
    )

    queue.close()

    assert released == [data_ref.ref_id]
    current = queue.get_job(job.job_id)
    assert current is not None
    assert current.staged_ref_ids == []


def test_run_job_with_hooks_purges_loaded_modules() -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value)}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-purge-1",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_purge_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "job_payload": {"value": 1, "count": 2},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-purge-1"

        def imap_unordered(self, payloads, **kwargs):
            del kwargs
            for idx, item in enumerate(list(payloads)):
                yield idx, {"value": int(item["value"])}

        def update_globals(self, values):
            return values

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with (
        patch("pycloud_parallel.controlplane.job_queue._purge_loaded_artifact_modules") as mocked,
        patch("pycloud_parallel.controlplane.job_queue.gc.collect") as mocked_gc,
        patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool),
    ):
        queue._run_job("job-hooks-purge-1")  # noqa: SLF001

    job = queue.get_job("job-hooks-purge-1")
    assert job is not None
    assert job.status == "SUCCEEDED"
    mocked.assert_called_once()
    mocked_gc.assert_called_once()
    assert mocked.call_args.kwargs["entry_module"] == "job_hooks_purge_demo"
    assert mocked.call_args.kwargs["package_format"] == "py"


def test_run_job_with_hooks_cleans_extracted_dir_without_scheduler_crash(tmp_path) -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    extract_dir = tmp_path / "job_hooks_extract"
    extract_dir.mkdir()
    (extract_dir / "marker.txt").write_text("ok", encoding="utf-8")

    fake_module = SimpleNamespace(
        __pycloud_temp_extract_dir__=str(extract_dir),
    )

    def _run(value=0, **_kwargs):
        return {"value": int(value)}

    def _task_generator(value=0, count=1, **_kwargs):
        return [{"value": value + idx} for idx in range(int(count))]

    fake_module.run = _run
    fake_module.task_generator = _task_generator

    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-cleanup-1",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(b"placeholder").decode("utf-8"),
            "entry_module": "job_hooks_cleanup_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "job_payload": {"value": 1, "count": 2},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        def __init__(self):
            self.job_id = "job-hooks-cleanup-1"

        def imap_unordered(self, payloads, **kwargs):
            del kwargs
            for idx, item in enumerate(list(payloads)):
                yield idx, {"value": int(item["value"])}

        def update_globals(self, values):
            return values

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    fake_pool = _FakePool()
    with (
        patch("pycloud_parallel.controlplane.job_queue._load_user_module", return_value=fake_module),
        patch("pycloud_parallel.controlplane.job_queue._purge_loaded_artifact_modules"),
        patch("pycloud_parallel.controlplane.job_queue.gc.collect"),
        patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool),
    ):
        queue._run_job("job-hooks-cleanup-1")  # noqa: SLF001

    job = queue.get_job("job-hooks-cleanup-1")
    assert job is not None
    assert job.status == "SUCCEEDED"
    assert not extract_dir.exists()


def test_job_queue_client_submit_source_bytes_uses_minimal_payload() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]
    resp = client.submit(
        source=b"def run(**_kwargs):\n    return {}\n\ndef task_generator(**_kwargs):\n    return []\n",
        entry_module="job_demo",
    )
    assert resp == {"ok": True}
    assert captured["job_mode"] == "hooks"
    assert captured["client_id"] == "client-a"
    assert captured["entry_module"] == "job_demo"
    assert captured["entry_callable"] == "run"
    assert captured["task_generator_callable"] == "task_generator"
    assert "handle_result_callable" not in captured
    assert "finalize_callable" not in captured
    assert "pool_worker_count" not in captured
    assert "priority" not in captured


def test_job_queue_client_submit_uses_source_bytes_as_default_product_path() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]
    resp = client.submit(
        source=b"def run(**_kwargs):\n    return {}\n\ndef task_generator(**_kwargs):\n    return []\n",
        entry_module="job_demo",
    )
    assert resp == {"ok": True}
    assert captured["entry_module"] == "job_demo"
    assert captured["entry_callable"] == "run"
    assert captured["task_generator_callable"] == "task_generator"
    assert captured["package_format"] == "py"


def test_job_queue_client_submit_accepts_advanced_artifact() -> None:
    from pycloud_parallel import JobQueue
    from pycloud_parallel.artifact import Artifact, ArtifactDeps

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]
    resp = client.submit(
        artifact=Artifact.from_bytes(
            b"def run(**_kwargs):\n    return {}\n\ndef task_generator(**_kwargs):\n    return []\n",
            package_format="py",
            entry_module="job_demo",
            deps=ArtifactDeps.allow_install(["orjson==3.10.18"]),
        ),
    )
    assert resp == {"ok": True}
    assert captured["entry_module"] == "job_demo"
    assert captured["dependency_allowlist"] == ["orjson==3.10.18"]


def test_job_queue_client_submit_accepts_module_source_via_unified_artifact_path() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]

    import types

    module = types.ModuleType("job_module_demo")
    exec(
        (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': value}\n\n"
            b"def task_generator(value=0, **_kwargs):\n"
            b"    return [{'value': value}]\n"
        ).decode("utf-8"),
        module.__dict__,
    )

    with patch(
        "pycloud_parallel.controlplane.artifact._prepare_artifact_blob",
        return_value=(b"blob", "job_module_demo.tar.gz"),
    ):
        resp = client.submit(source=module)

    assert resp == {"ok": True}
    assert captured["entry_module"] == "job_module_demo"
    assert captured["task_generator_callable"] == "task_generator"
    assert captured["package_format"] == "tar.gz"


def test_job_queue_submit_interprets_task_serialization_mode_for_future_task_pool() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]
    resp = client.submit(
        source=b"def run(**_kwargs):\n    return {}\n\ndef task_generator(**_kwargs):\n    return []\n",
        entry_module="job_demo",
        task_serialization_mode="pickle_stable_v1",
    )

    assert resp == {"ok": True}
    assert captured["task_serialization_mode"] == "pickle_stable_v1"


def test_job_queue_public_submit_helpers_do_not_expose_policy_id() -> None:
    from pycloud_parallel import JobQueue

    assert "policy_id" not in inspect.signature(JobQueue.submit).parameters
    assert not hasattr(JobQueue, "submit_job_from_bytes")
    assert not hasattr(JobQueue, "submit_job_from_module")


def test_job_queue_submit_job_rejects_policy_override_fields() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    try:
        for field in ("policy_id", "taskpool_policy_id"):
            with pytest.raises(ValueError, match="policy_id/taskpool_policy_id"):
                client.submit_job({"entry_module": "job_demo", "subtasks": [], field: "trusted_internal"})
    finally:
        client.close()


def test_job_queue_manager_submit_rejects_taskpool_policy_id() -> None:
    queue = JobQueueManager()

    with pytest.raises(ValueError, match="policy_id/taskpool_policy_id"):
        queue.submit_job(
            {
                "job_id": "job-policy-rejected",
                "client_id": "client-a",
                "entry_module": "task_demo",
                "subtasks": [{"value": 1}],
                "taskpool_policy_id": "trusted_internal",
            }
        )


def test_job_orchestrator_module_rejects_submit_policy_fields() -> None:
    module = JobOrchestratorModule()

    for field in ("policy_id", "taskpool_policy_id"):
        status, body = module.submit_job(
            {
                "entry_module": "task_demo",
                "subtasks": [{"value": 1}],
                field: "trusted_internal",
            },
            "",
            serialization_mode="structured_v1",
        )
        assert status == 400
        assert "policy_id/taskpool_policy_id" in body["error"]


def test_job_orchestrator_module_rejects_non_structured_submit_mode() -> None:
    module = JobOrchestratorModule()

    status, body = module.submit_job(
        {"entry_module": "task_demo", "subtasks": [{"value": 1}]},
        "",
        serialization_mode="pickle_stable_v1",
    )

    assert status == 400
    assert "structured_v1" in body["error"]


def test_job_queue_client_submit_rejects_callable_source() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")

    def _job_func(**_kwargs):
        return {}

    with pytest.raises(ValueError, match="JobQueue.submit\\(source=callable\\) is not supported"):
        client.submit(source=_job_func)


def test_job_queue_client_submit_source_bytes_auto_binds_update_globals() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]
    resp = client.submit(
        source=(
            b"def run(**_kwargs):\n    return {}\n\n"
            b"def task_generator(**_kwargs):\n    return []\n\n"
            b"def update_globals(**_kwargs):\n    return {'cfg': {'k': 'v'}}\n"
        ),
        entry_module="job_demo",
    )
    assert resp == {"ok": True}
    assert captured["update_globals"] == "update_globals"


def test_job_queue_client_submit_source_bytes_auto_binds_handle_data_alias() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]
    resp = client.submit(
        source=(
            b"def run(**_kwargs):\n    return {}\n\n"
            b"def task_generator(**_kwargs):\n    return []\n\n"
            b"def handle_data(index, result, state=None, **_kwargs):\n    return state\n"
        ),
        entry_module="job_demo",
    )
    assert resp == {"ok": True}
    assert captured["handle_result_callable"] == "handle_data"
    assert "finalize_callable" not in captured


def test_job_queue_client_submit_source_bytes_auto_binds_finalize_when_present() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]
    resp = client.submit(
        source=(
            b"def run(**_kwargs):\n    return {}\n\n"
            b"def task_generator(**_kwargs):\n    return []\n\n"
            b"def finalize(state=None, **_kwargs):\n    return state\n"
        ),
        entry_module="job_demo",
    )
    assert resp == {"ok": True}
    assert captured["finalize_callable"] == "finalize"


def test_prepare_job_blob_submit_fields_uses_object_ref_for_large_blob(monkeypatch) -> None:
    from pycloud_parallel.execution.support import _prepare_job_blob_submit_fields
    from pycloud_parallel.data.ref import DataRef

    monkeypatch.setattr(
        "pycloud_parallel.execution.support._job_blob_requires_object_ref",
        lambda blob: True,
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.infocenter_client.InfoCenterClient.select_task_nodes",
        lambda self, **kwargs: [SimpleNamespace(control_addr="127.0.0.1:50061")],
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.node_control_client.NodeControlClient.upload_object_from_bytes",
        lambda self, **kwargs: DataRef(
            ref_id="sha256:" + ("a" * 64),
            storage_id="sha256:" + ("a" * 64),
            logical_type="archive",
            format="tar.gz",
            size_bytes=4096,
            materialize_as="path",
            locator_kind="node_local",
            locator_token="",
        ),
    )

    fields = _prepare_job_blob_submit_fields(
        target="127.0.0.1:50051",
        blob=b"x" * (3 * 1024 * 1024),
        package_format="tar.gz",
        runtime="py3",
        timeout_sec=10.0,
    )
    assert "blob_b64" not in fields
    assert fields["blob_control_addr"] == "127.0.0.1:50061"
    assert fields["blob_ref"].object_id == "sha256:" + ("a" * 64)


def test_default_job_node_count_retries_transient_infocenter_failure(monkeypatch) -> None:
    from pycloud_parallel.controlplane.job_queue import _default_job_node_count

    calls = {"select": 0}

    def _fake_select(self, **_kwargs):  # noqa: ANN001
        calls["select"] += 1
        if calls["select"] == 1:
            raise ConnectionResetError(10054, "connection reset")
        return [
            SimpleNamespace(node_instance_id="node-inst-1", node_id="node-1", control_addr="127.0.0.1:50061"),
            SimpleNamespace(node_instance_id="node-inst-2", node_id="node-2", control_addr="127.0.0.1:50062"),
        ]

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.infocenter_client.InfoCenterClient.select_task_nodes",
        _fake_select,
    )

    assert _default_job_node_count(
        controlplane_target="127.0.0.1:50051",
        payload={"timeout_sec": 1.0},
    ) == 2
    assert calls["select"] >= 2


def test_prepare_job_submit_payload_for_call_preserves_staged_update_globals(monkeypatch) -> None:
    from pycloud_parallel.data.ref import DataRef
    from pycloud_parallel.execution.support import _prepare_job_submit_payload_for_call

    class _FakeUploadClient:
        def close(self) -> None:
            return None

    staged_ref = DataRef(
        ref_id="sha256:" + ("9" * 64),
        storage_id="sha256:" + ("9" * 64),
        format="json",
        size_bytes=32,
        logical_type="json",
        materialize_as="json",
        locator_kind="controlplane",
        locator_token="http://127.0.0.1:50051",
    )

    monkeypatch.setattr(
        "pycloud_parallel.execution.support._job_submit_upload_clients",
        lambda **kwargs: [_FakeUploadClient()],
    )
    monkeypatch.setattr(
        "pycloud_parallel.execution.support.prepare_outbound_payload",
        lambda payload, **kwargs: {**dict(payload or {}), "artifact_path": "prepared-object-ref"},
    )

    prepared = _prepare_job_submit_payload_for_call(
        target="127.0.0.1:50051",
        payload={
            "entry_module": "job_demo",
            "artifact_path": "demo.py",
            "update_globals": {"cfg": staged_ref},
        },
        timeout_sec=10.0,
    )

    assert prepared["artifact_path"] == "prepared-object-ref"
    assert prepared["update_globals"]["cfg"] == staged_ref


def test_job_queue_client_submit_job_accepts_controlplane_data_ref_in_job_payload(monkeypatch) -> None:
    from pycloud_parallel import DataRef, JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    data_ref = DataRef(
        ref_id="sha256:" + ("a" * 64),
        storage_id="sha256:" + ("a" * 64),
        format="bin",
        size_bytes=12,
        locator_kind="controlplane",
        locator_token="http://127.0.0.1:50051",
    )
    captured = {}

    monkeypatch.setattr(
        "pycloud_parallel.execution.support._job_submit_upload_clients",
        lambda **kwargs: [],
    )

    def _fake_call(*, service_name, method, payload=None, timeout_sec=60.0, service_token=None):
        del service_name, method, timeout_sec, service_token
        captured["payload"] = dict(payload or {})
        return {"ok": True, "job": {"job_id": "job-1", "status": "WAITING"}}

    client._call_job_orchestrator = lambda *, effective_policy, **kwargs: _fake_call(
        **{key: value for key, value in kwargs.items() if key != "serialization_mode"}
    )  # type: ignore[method-assign]
    resp = client.submit_job(
        {
            "entry_module": "job_demo",
            "job_payload": {"cfg": data_ref},
        }
    )

    assert resp["job"]["job_id"] == "job-1"
    assert captured["payload"]["job_payload"]["cfg"] == data_ref


def _submit_oversized_job_payload(monkeypatch):
    from pycloud_parallel import DataRef, JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}
    registered = {}
    replicas = [
        {
            "node_id": "node-1",
            "node_instance_id": "node-1-inst",
            "control_addr": "127.0.0.1:50061",
        },
        {
            "node_id": "node-2",
            "node_instance_id": "node-2-inst",
            "control_addr": "127.0.0.1:50062",
        },
    ]

    class _FakeStageClient:
        def __init__(self, target: str) -> None:
            self.target = target

        def upload_object_from_bytes(self, *, blob, format="", chunk_size=0):
            del blob, chunk_size
            return DataRef(
                ref_id="sha256:" + ("d" * 64),
                storage_id="sha256:" + ("d" * 64),
                logical_type="text",
                format=format or "txt",
                size_bytes=256,
                materialize_as="text",
                locator_kind="node_local",
                locator_token="",
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "pycloud_parallel.execution.support._select_job_staging_clients",
        lambda **kwargs: (
            [_FakeStageClient("127.0.0.1:50061"), _FakeStageClient("127.0.0.1:50062")],
            list(replicas),
        ),
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.data_registry.DataRegistryClient.register",
        lambda self, ref, **kwargs: registered.update({"ref": ref, **kwargs}) or {"ok": True},
    )
    monkeypatch.setattr(
        "pycloud_parallel.execution.support._job_submit_upload_clients",
        lambda **kwargs: [],
    )

    def _fake_call(*, service_name, method, payload=None, timeout_sec=60.0, service_token=None):
        del service_name, method, timeout_sec, service_token
        captured["payload"] = dict(payload or {})
        return {"ok": True, "job": {"job_id": "job-1", "status": "WAITING"}}

    client._call_job_orchestrator = lambda *, effective_policy, **kwargs: _fake_call(
        **{key: value for key, value in kwargs.items() if key != "serialization_mode"}
    )  # type: ignore[method-assign]
    resp = client.submit_job(
        {
            "entry_module": "job_demo",
            "runtime": "py3",
            "job_payload": {"text": "x" * 256},
        }
    )

    return resp, captured, registered, replicas, DataRef


def test_job_queue_client_submit_job_stages_oversized_job_payload(monkeypatch, request) -> None:
    from pycloud_parallel.controlplane import config as config_mod

    monkeypatch.setenv("PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES", "32")
    monkeypatch.delenv("PYCLOUD_DATAREF_UPLOAD_STRATEGY", raising=False)
    config_mod.reload_config()

    def _reset_config() -> None:
        monkeypatch.delenv("PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES", raising=False)
        monkeypatch.delenv("PYCLOUD_DATAREF_UPLOAD_STRATEGY", raising=False)
        config_mod.reload_config()

    request.addfinalizer(_reset_config)

    resp, captured, registered, replicas, data_ref_type = _submit_oversized_job_payload(monkeypatch)

    assert resp["job"]["job_id"] == "job-1"
    assert isinstance(captured["payload"]["job_payload"], data_ref_type)
    assert captured["payload"]["job_payload"].locator_kind == "controlplane"
    assert registered["replicas"] == [replicas[0]]


def test_job_queue_client_submit_job_fanout_registers_all_replicas(monkeypatch, request) -> None:
    from pycloud_parallel.controlplane import config as config_mod

    monkeypatch.setenv("PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES", "32")
    monkeypatch.setenv("PYCLOUD_DATAREF_UPLOAD_STRATEGY", "fanout")
    config_mod.reload_config()

    def _reset_config() -> None:
        monkeypatch.delenv("PYCLOUD_INLINE_PAYLOAD_THRESHOLD_BYTES", raising=False)
        monkeypatch.delenv("PYCLOUD_DATAREF_UPLOAD_STRATEGY", raising=False)
        config_mod.reload_config()

    request.addfinalizer(_reset_config)

    resp, captured, registered, replicas, data_ref_type = _submit_oversized_job_payload(monkeypatch)

    assert resp["job"]["job_id"] == "job-1"
    assert isinstance(captured["payload"]["job_payload"], data_ref_type)
    assert captured["payload"]["job_payload"].locator_kind == "controlplane"
    assert registered["replicas"] == replicas


def test_job_queue_client_submit_job_keeps_directory_root_dir_inline(monkeypatch, tmp_path) -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    def _fail_if_staged(**kwargs):
        raise AssertionError(f"directory payload should not be staged: {kwargs}")

    monkeypatch.setattr(
        "pycloud_parallel.execution.support._select_job_staging_clients",
        _fail_if_staged,
    )
    monkeypatch.setattr(
        "pycloud_parallel.execution.support._job_submit_upload_clients",
        lambda **kwargs: [],
    )

    def _fake_call(*, service_name, method, payload=None, timeout_sec=60.0, service_token=None):
        del service_name, method, timeout_sec, service_token
        captured["payload"] = dict(payload or {})
        return {"ok": True, "job": {"job_id": "job-1", "status": "WAITING"}}

    client._call_job_orchestrator = lambda *, effective_policy, **kwargs: _fake_call(
        **{key: value for key, value in kwargs.items() if key != "serialization_mode"}
    )  # type: ignore[method-assign]
    resp = client.submit_job(
        {
            "entry_module": "job_demo",
            "runtime": "py3",
            "job_payload": {"root_dir": str(tmp_path)},
        }
    )

    assert resp["job"]["job_id"] == "job-1"
    assert captured["payload"]["job_payload"]["root_dir"] == str(tmp_path)


def test_job_queue_client_submit_job_uses_unified_outbound_policy(monkeypatch) -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-a")
    captured = {}

    class _FakeUploadClient:
        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "pycloud_parallel.execution.support._job_submit_upload_clients",
        lambda **kwargs: [_FakeUploadClient()],
    )

    def _fake_prepare(payload, *, put_data, estimate_inline_size, policy, managed_global_policy=None):
        del put_data, estimate_inline_size
        del managed_global_policy
        captured["payload"] = dict(payload or {})
        captured["mode"] = policy.mode
        captured["managed_global_field_names"] = policy.managed_global_field_names
        return dict(payload or {})

    monkeypatch.setattr(
        "pycloud_parallel.execution.support.prepare_outbound_payload",
        _fake_prepare,
    )

    client._call_job_orchestrator = lambda *, effective_policy, **kwargs: {"ok": True, "job": {"job_id": "job-1", "status": "WAITING"}}  # type: ignore[method-assign]
    resp = client.submit_job({"entry_module": "job_demo", "update_globals": {"cfg": {"k": "v"}}})

    assert resp["job"]["job_id"] == "job-1"
    assert captured["mode"] == "job_submit"
    assert captured["managed_global_field_names"] == ("update_globals",)


def test_job_queue_client_restores_cached_session(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("PYCLOUD_JOB_CLIENT_SESSION_DIR", str(tmp_path))

    from pycloud_parallel import JobQueue

    first = JobQueue("127.0.0.1:50051")
    second = JobQueue("127.0.0.1:50051")

    assert first.client_id == second.client_id
    assert first.auth_token == second.auth_token


def test_job_queue_client_rotates_expired_cached_session(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("PYCLOUD_JOB_CLIENT_SESSION_DIR", str(tmp_path))

    from pycloud_parallel import JobQueue
    from pycloud_parallel.execution.support import _job_client_session_cache_file

    first = JobQueue("127.0.0.1:50051", client_id="client-cache")
    cache_path = _job_client_session_cache_file(
        target="127.0.0.1:50051",
        service_name="job-orchestrator",
        client_scope="client-cache",
    )
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    payload["expires_at"] = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    cache_path.write_text(json.dumps(payload), encoding="utf-8")

    second = JobQueue("127.0.0.1:50051", client_id="client-cache")

    assert second.client_id == "client-cache"
    assert second.auth_token != first.auth_token


def test_job_queue_client_recent_job_ids_tracks_and_restores(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("PYCLOUD_JOB_CLIENT_SESSION_DIR", str(tmp_path))

    from pycloud_parallel import JobQueue
    from pycloud_parallel.execution import queue as queue_mod

    monkeypatch.setattr(
        queue_mod,
        "_prepare_job_submit_payload_for_call",
        lambda *, payload, **_kwargs: dict(payload),
    )

    client = JobQueue("127.0.0.1:50051", client_id="client-recent")
    job_ids = iter(["job-1", "job-2", "job-1"])

    def _fake_call(*, service_name, method, payload=None, timeout_sec=60.0, service_token=None):
        del service_name, payload, timeout_sec, service_token
        if method == "submit_job":
            return {"ok": True, "job": {"job_id": next(job_ids)}}
        raise AssertionError(f"unexpected method: {method}")

    client._call_job_orchestrator = lambda *, effective_policy, **kwargs: _fake_call(
        **{key: value for key, value in kwargs.items() if key != "serialization_mode"}
    )  # type: ignore[method-assign]
    client.submit_job({"entry_module": "job_demo"})
    client.submit_job({"entry_module": "job_demo"})
    client.submit_job({"entry_module": "job_demo"})

    assert client.recent_job_ids() == ["job-1", "job-2"]

    restored = JobQueue("127.0.0.1:50051", client_id="client-recent")
    assert restored.recent_job_ids() == ["job-1", "job-2"]


def test_job_queue_client_discovers_job_orchestrator_via_infocenter(monkeypatch) -> None:
    from pycloud_parallel import JobQueue
    from pycloud_parallel.controlplane.infocenter_client import InfoCenterServiceRoute
    from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2

    route = InfoCenterServiceRoute(
        service_name="job-orchestrator",
        service_id="job-orch-1",
        status=pb2.SERVICE_STATUS_RUNNING,
        node_instance_id="job-orch-1-inst",
        node_id="job-orch-1",
        control_addr="",
        node_healthy=True,
        worker_count=1,
        alive_workers=1,
        in_flight=0,
        lease_expire_at=datetime.now(timezone.utc),
        http_base_url="http://127.0.0.1:18080/svc/job-orch-1",
    )
    captured = {}

    def _fake_list_service_routes(self, *, service_name="", healthy_only=True, limit=500):
        captured["service_name"] = service_name
        captured["healthy_only"] = healthy_only
        captured["limit"] = limit
        return [route]

    def _fake_call_route_http(
        route_arg,
        *,
        method,
        payload,
        timeout_sec,
        service_token,
        serialization_mode="",
        effective_policy=None,
    ):
        captured["route"] = route_arg
        captured["method"] = method
        captured["payload"] = dict(payload or {})
        captured["timeout_sec"] = timeout_sec
        captured["service_token"] = service_token
        captured["serialization_mode"] = serialization_mode
        captured["effective_policy"] = effective_policy
        return {"ok": True, "job": {"job_id": "job-1", "status": "WAITING"}}

    monkeypatch.setattr(
        "pycloud_parallel.controlplane.infocenter_client.InfoCenterClient.list_service_routes",
        _fake_list_service_routes,
    )
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.discovery_client.client_mod._call_route_http",
        _fake_call_route_http,
    )

    client = JobQueue("127.0.0.1:50051", client_id="client-discovery", auth_token="token-discovery")
    try:
        resp = client.get_job_status("job-1")
    finally:
        client.close()

    assert resp["job"]["job_id"] == "job-1"
    assert captured["service_name"] == "job-orchestrator"
    assert captured["method"] == "get_job_status"
    assert captured["payload"] == {"job_id": "job-1", "include_details": False}
    assert captured["service_token"] == "token-discovery"
    assert captured["serialization_mode"] == "structured_v1"
    assert captured["route"].http_base_url == "http://127.0.0.1:18080/svc/job-orch-1"


def test_job_queue_local_reuses_service_connect_local_transport(monkeypatch) -> None:
    from pycloud_parallel import JobQueue
    from pycloud_parallel.execution.service_session import Service

    captured = {}

    class _FakeConnectedService:
        route = "local"
        protocol = "http"
        service_name = "job-orchestrator"

        def close(self):
            captured["closed"] = True

        def call_balanced(self, method, payload, **kwargs):
            captured["method"] = method
            captured["payload"] = dict(payload or {})
            captured["call_kwargs"] = dict(kwargs)
            return "local", {"ok": True, "job": {"job_id": "job-local", "status": "WAITING"}}

    def _fake_connect(**kwargs):
        captured["connect_kwargs"] = dict(kwargs)
        return _FakeConnectedService()

    monkeypatch.setattr(Service, "_connect_route", staticmethod(_fake_connect))

    client = JobQueue.connect("local", client_id="client-local", auth_token="token-local")
    try:
        resp = client.get_job_status("job-local")
    finally:
        client.close()

    assert resp["job"]["job_id"] == "job-local"
    assert captured["connect_kwargs"]["target"] == "local"
    assert captured["connect_kwargs"]["service_name"] == "job-orchestrator"
    assert captured["connect_kwargs"]["route"] == "local"
    assert captured["connect_kwargs"]["protocol"] == "http"
    assert captured["connect_kwargs"]["service_token"] == "token-local"
    assert captured["connect_kwargs"]["serialization_mode"] == "pickle_native_v1"
    assert captured["connect_kwargs"]["effective_policy_override"] is None
    assert captured["connect_kwargs"]["prepare_discovery_payload"] is False
    assert captured["method"] == "get_job_status"
    assert captured["payload"] == {
        "job_id": "job-local",
        "include_details": False,
        "_service_token": "token-local",
    }
    assert captured["call_kwargs"]["serialization_mode"] == "pickle_native_v1"


def test_job_queue_local_calls_local_service_ipc_end_to_end(tmp_path, monkeypatch) -> None:
    import importlib

    from pycloud_parallel import JobQueue, Service

    monkeypatch.setenv("PYCLOUD_LOCAL_IPC_DIR", str(tmp_path / "local-ipc"))
    module_path = tmp_path / "local_job_orchestrator_demo.py"
    module_path.write_text(
        "def pycloud_export(fn):\n"
        "    fn.__pycloud_export__ = True\n"
        "    return fn\n\n"
        "def _raw(job):\n"
        "    return {'__pycloud_raw_response__': True, '__pycloud_status_code__': 200, 'ok': True, 'job': job}\n\n"
        "@pycloud_export\n"
        "def submit_job(_service_token='', **payload):\n"
        "    return _raw({'job_id': payload.get('entry_module', 'job-local'), 'status': 'WAITING', 'token': _service_token})\n\n"
        "@pycloud_export\n"
        "def get_job_status(job_id='', include_details=False, **_payload):\n"
        "    return _raw({'job_id': job_id, 'status': 'SUCCEEDED', 'include_details': bool(include_details)})\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    service = Service.deploy(
        target="local",
        service_name="job-orchestrator",
        source=importlib.import_module("local_job_orchestrator_demo"),
        worker_count=1,
    )
    try:
        client = JobQueue.connect("local", client_id="client-local", auth_token="token-local", timeout_sec=5.0)
        try:
            submitted = client.submit_job({"entry_module": "job-local-1"})
            status = client.get_job_status("job-local-1", include_details=True)
        finally:
            client.close()
    finally:
        service.close()

    assert submitted["job"]["job_id"] == "job-local-1"
    assert submitted["job"]["token"] == "token-local"
    assert status["job"] == {"job_id": "job-local-1", "status": "SUCCEEDED", "include_details": True}


def test_job_queue_manager_local_creates_local_taskpool(monkeypatch) -> None:
    import base64

    from pycloud_parallel.controlplane.job_queue import JobQueueManager

    captured: dict[str, object] = {}

    class _FakePool:
        def update_globals(self, values):
            del values

        def unordered(self, items, **kwargs):
            del kwargs
            for item in items:
                yield {"value": item}

        def imap_unordered(self, items, **kwargs):
            del kwargs
            for item in items:
                yield {"value": item}

        def close(self):
            return None

    def _fake_open(**kwargs):
        captured.update(kwargs)
        return _FakePool()

    monkeypatch.setattr("pycloud_parallel.controlplane.job_queue.TaskPool.open", _fake_open)
    queue = JobQueueManager()
    queue.start(controlplane_target="local")
    try:
        payload = {
            "entry_module": "demo_job",
            "task_generator_callable": [{"x": 1}],
            "task_entry_callable": "run",
            "package_format": "py",
            "blob_b64": base64.b64encode(b"def run(**payload):\n    return payload\n").decode("ascii"),
            "pool_name": "local-job-pool",
        }
        state = queue.submit_job(payload, auth_token="token")
        deadline = time.monotonic() + 2.0
        while not captured and time.monotonic() < deadline:
            current = queue.get_job(state.job_id)
            if current is not None and current.status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                break
            time.sleep(0.01)
    finally:
        queue.close()

    assert captured["target"] == "local"
    assert "infocenter_target" not in captured
    assert "entry_module" not in captured
    assert "entry_callable" not in captured
    assert "source" not in captured
    assert captured["artifact"].entry_module == "demo_job"
    assert captured["artifact"].entry_callable == "run"
    assert captured["node_count"] == 0
    assert state.status in {"WAITING", "RUNNING", "SUCCEEDED", "FAILED"}


def test_job_queue_client_local_submit_source_module_uses_import_metadata(tmp_path, monkeypatch) -> None:
    import importlib

    from pycloud_parallel import JobQueue
    from pycloud_parallel.execution.service_session import Service

    module_path = tmp_path / "local_job_module_demo.py"
    module_path.write_text(
        "def run(value=0, **_kwargs):\n"
        "    return {'value': value}\n\n"
        "def task_generator(value=0, **_kwargs):\n"
        "    return [{'value': value}]\n\n"
        "def handle_result(index, result, state=None, **_kwargs):\n"
        "    state.setdefault('items', []).append((index, result))\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    module = importlib.import_module("local_job_module_demo")
    monkeypatch.setattr(Service, "_connect_route", staticmethod(lambda **_kwargs: SimpleNamespace(close=lambda: None)))
    client = JobQueue.connect("local", client_id="client-module")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]

    with patch("pycloud_parallel.execution.queue._prepare_artifact") as mocked_prepare:
        resp = client.submit(source=module, job_payload={"value": 3})

    assert resp == {"ok": True}
    mocked_prepare.assert_not_called()
    assert captured["source_mode"] == "module_import"
    assert captured["entry_module"] == "local_job_module_demo"
    assert captured["entry_callable"] == "run"
    assert captured["task_generator_callable"] == "task_generator"
    assert captured["handle_result_callable"] == "handle_result"
    assert captured["source_root"] == str(tmp_path.resolve())
    assert "blob_b64" not in captured
    assert "blob_ref" not in captured


def test_job_queue_manager_local_module_import_creates_direct_local_taskpool(tmp_path, monkeypatch) -> None:
    import importlib

    from pycloud_parallel.controlplane.job_queue import JobQueueManager

    module_path = tmp_path / "local_job_pool_demo.py"
    module_path.write_text(
        "def run(value=0, **_kwargs):\n"
        "    return {'value': value}\n\n"
        "def task_generator(value=0, **_kwargs):\n"
        "    return [{'value': value}]\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.import_module("local_job_pool_demo")
    captured: dict[str, object] = {}

    class _FakePool:
        def update_globals(self, values):
            del values

        def imap_unordered(self, items, **kwargs):
            del kwargs
            for index, item in enumerate(items):
                yield index, {"value": item["value"]}

        def close(self):
            return None

    def _fake_open(**kwargs):
        captured.update(kwargs)
        return _FakePool()

    monkeypatch.setattr("pycloud_parallel.controlplane.job_queue.TaskPool.open", _fake_open)
    queue = JobQueueManager()
    queue.start(controlplane_target="local")
    try:
        state = queue.submit_job(
            {
                "job_mode": "hooks",
                "source_mode": "module_import",
                "entry_module": "local_job_pool_demo",
                "entry_callable": "run",
                "task_generator_callable": "task_generator",
                "source_root": str(tmp_path),
                "job_payload": {"value": 7},
                "pool_name": "local-job-pool",
            },
            auth_token="token",
        )
        deadline = time.monotonic() + 2.0
        while not captured and time.monotonic() < deadline:
            current = queue.get_job(state.job_id)
            if current is not None and current.status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                break
            time.sleep(0.01)
    finally:
        queue.close()

    assert captured["target"] == "local"
    assert captured["source"] == "local_job_pool_demo"
    assert captured["entry_module"] == "local_job_pool_demo"
    assert captured["entry_callable"] == "run"
    assert "artifact" not in captured
    assert "package_format" not in captured
    assert captured["node_count"] == 0


def test_job_queue_client_submit_source_module_builds_payloads() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-module")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]

    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': value}\n\n"
        b"def task_generator(value=0, **_kwargs):\n"
        b"    return [{'value': value}]\n\n"
        b"def handle_result(index, result, state=None, **_kwargs):\n"
        b"    state.setdefault('items', []).append((index, result))\n"
    )
    module_name = "job_module_demo"

    import types

    module = types.ModuleType(module_name)
    exec(module_blob.decode("utf-8"), module.__dict__)

    with patch("pycloud_parallel.execution.support._prepare_code_blob", return_value=(module_blob, f"{module_name}.tar.gz")):
        resp = client.submit(source=module)
    assert resp == {"ok": True}
    assert captured["client_id"] == "client-module"
    assert captured["entry_module"] == module_name
    assert captured["entry_callable"] == "run"
    assert captured["task_generator_callable"] == "task_generator"
    assert captured["handle_result_callable"] == "handle_result"
    assert "finalize_callable" not in captured
    assert captured["package_format"] == "tar.gz"
    assert captured["blob_b64"]


def test_job_queue_client_submit_source_module_forwards_resource_paths() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-module")

    import types

    module = types.ModuleType("job_module_demo")
    exec(
        (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': value}\n\n"
            b"def task_generator(value=0, **_kwargs):\n"
            b"    return [{'value': value}]\n"
        ).decode("utf-8"),
        module.__dict__,
    )

    with patch.object(client, "submit", return_value={"ok": True}) as mocked_submit:
        resp = client.submit(source=module, resource_paths=["fund_nav_df.csv"])

    assert resp == {"ok": True}
    mocked_submit.assert_called_once()
    assert mocked_submit.call_args.kwargs["source"] is module
    assert mocked_submit.call_args.kwargs["resource_paths"] == ["fund_nav_df.csv"]


def test_job_queue_client_submit_source_module_forwards_task_resource_paths() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-module")

    import types

    module = types.ModuleType("job_module_demo")
    exec(
        (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': value}\n\n"
            b"def task_generator(value=0, **_kwargs):\n"
            b"    return [{'value': value}]\n"
        ).decode("utf-8"),
        module.__dict__,
    )

    with patch.object(client, "submit", return_value={"ok": True}) as mocked_submit:
        resp = client.submit(source=module, task_resource_paths=["worker/data.csv"])

    assert resp == {"ok": True}
    mocked_submit.assert_called_once()
    assert mocked_submit.call_args.kwargs["task_resource_paths"] == ["worker/data.csv"]


def test_job_queue_client_submit_source_module_bundles_task_resources_into_job_blob() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-module")
    captured = {}

    import types

    module = types.ModuleType("job_module_demo")
    exec(
        (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': value}\n\n"
            b"def task_generator(value=0, **_kwargs):\n"
            b"    return [{'value': value}]\n"
        ).decode("utf-8"),
        module.__dict__,
    )

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]

    with patch(
        "pycloud_parallel.execution.queue._prepare_code_blob",
        return_value=(b"blob", "job_module_demo.tar.gz"),
    ) as mocked_blob:
        resp = client.submit(
            source=module,
            task_resource_paths=["worker/data.csv"],
        )

    assert resp == {"ok": True}
    mocked_blob.assert_called_once()
    assert mocked_blob.call_args.kwargs["resource_paths"] == ["worker/data.csv"]


def test_job_queue_client_submit_source_module_auto_binds_update_globals() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-module")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]

    import types

    module = types.ModuleType("job_module_demo_with_globals")
    exec(
        (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': value}\n\n"
            b"def task_generator(value=0, **_kwargs):\n"
            b"    return [{'value': value}]\n\n"
            b"def handle_result(index, result, state=None, **_kwargs):\n"
            b"    state.setdefault('items', []).append((index, result))\n\n"
            b"def update_globals(**_kwargs):\n"
            b"    return {'cfg': {'mode': 'auto'}}\n"
        ).decode("utf-8"),
        module.__dict__,
    )

    with patch(
        "pycloud_parallel.execution.support._prepare_code_blob",
        return_value=(b"blob", "job_module_demo_with_globals.tar.gz"),
    ):
        resp = client.submit(source=module)
    assert resp == {"ok": True}
    assert captured["update_globals"] == "update_globals"


def test_job_queue_client_submit_source_module_auto_binds_handle_data_alias() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-module")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]

    import types

    module = types.ModuleType("job_module_demo_with_handle_data")
    exec(
        (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': value}\n\n"
            b"def task_generator(value=0, **_kwargs):\n"
            b"    return [{'value': value}]\n\n"
            b"def handle_data(index, result, state=None, **_kwargs):\n"
            b"    state.setdefault('items', []).append((index, result))\n"
        ).decode("utf-8"),
        module.__dict__,
    )

    with patch(
        "pycloud_parallel.execution.support._prepare_code_blob",
        return_value=(b"blob", "job_module_demo_with_handle_data.tar.gz"),
    ):
        resp = client.submit(source=module)
    assert resp == {"ok": True}
    assert captured["handle_result_callable"] == "handle_data"
    assert "finalize_callable" not in captured


def test_job_queue_client_submit_source_module_auto_binds_finalize_when_present() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-module")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]

    import types

    module = types.ModuleType("job_module_demo_with_finalize")
    exec(
        (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': value}\n\n"
            b"def task_generator(value=0, **_kwargs):\n"
            b"    return [{'value': value}]\n\n"
            b"def finalize(state=None, **_kwargs):\n"
            b"    return state\n"
        ).decode("utf-8"),
        module.__dict__,
    )

    with patch(
        "pycloud_parallel.execution.support._prepare_code_blob",
        return_value=(b"blob", "job_module_demo_with_finalize.tar.gz"),
    ):
        resp = client.submit(source=module)
    assert resp == {"ok": True}
    assert captured["finalize_callable"] == "finalize"


def test_job_queue_client_submit_source_module_accepts_update_globals_dict() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-module")
    captured = {}

    def _fake_submit(payload):
        captured.update(payload)
        return {"ok": True}

    client.submit_job = _fake_submit  # type: ignore[method-assign]

    import types

    module = types.ModuleType("job_module_demo_with_explicit_globals")
    exec(
        (
            b"def run(value=0, **_kwargs):\n"
            b"    return {'value': value}\n\n"
            b"def task_generator(value=0, **_kwargs):\n"
            b"    return [{'value': value}]\n"
        ).decode("utf-8"),
        module.__dict__,
    )

    with patch(
        "pycloud_parallel.execution.support._prepare_code_blob",
        return_value=(b"blob", "job_module_demo_with_explicit_globals.tar.gz"),
    ):
        resp = client.submit(source=module, update_globals={"cfg": {"mode": "manual"}})
    assert resp == {"ok": True}
    assert captured["update_globals"] == {"cfg": {"mode": "manual"}}


def test_run_job_with_hooks_uses_larger_default_worker_node_and_inflight(monkeypatch) -> None:
    queue = JobQueueManager()
    queue._controlplane_target = "127.0.0.1:50051"  # noqa: SLF001
    module_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    return {'value': int(value)}\n\n"
        b"def task_generator(value=0, count=1, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n"
    )
    state = queue.submit_job(  # noqa: SLF001
        {
            "job_id": "job-hooks-defaults",
            "client_id": "client-a",
            "priority": 5,
            "blob_b64": base64.b64encode(module_blob).decode("utf-8"),
            "entry_module": "job_hooks_defaults_demo",
            "entry_callable": "run",
            "package_format": "py",
            "task_generator_callable": "task_generator",
            "job_payload": {"value": 2, "count": 3},
        }
    )
    state.status = "RUNNING"

    class _FakePool:
        job_id = "job-hooks-defaults"
        worker_count = 10

        def _resolve_max_in_flight(self, requested):
            del requested
            return 15

        def imap_unordered(self, payloads, **kwargs):
            assert kwargs["max_in_flight"] == 15
            assert kwargs["receive_batch"] == 10
            assert kwargs["max_infra_retries"] == 1
            for idx, item in enumerate(list(payloads)):
                yield idx, {"value": int(item["value"])}

        def update_globals(self, values):
            return values

        def close(self):
            return None

        def cancel_job(self, **kwargs):
            return None

    monkeypatch.setattr("pycloud_parallel.controlplane.job_queue.os.cpu_count", lambda: 8)
    monkeypatch.setattr(
        "pycloud_parallel.controlplane.job_queue.InfoCenterClient.select_task_nodes",
        lambda self, **kwargs: [SimpleNamespace(node_id="n1"), SimpleNamespace(node_id="n2")],
    )

    fake_pool = _FakePool()
    with patch("pycloud_parallel.controlplane.job_queue._create_job_task_pool", return_value=fake_pool) as mocked:
        queue._run_job("job-hooks-defaults")  # noqa: SLF001

    assert mocked.call_args.kwargs["worker_count"] == 4
    assert mocked.call_args.kwargs["node_count"] == 2


def test_job_queue_client_wait_for_terminal_polls_until_done() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051")
    states = [
        {"ok": True, "job": {"job_id": "job-1", "status": "WAITING"}},
        {"ok": True, "job": {"job_id": "job-1", "status": "RUNNING"}},
        {"ok": True, "job": {"job_id": "job-1", "status": "SUCCEEDED"}},
        {"ok": True, "job": {"job_id": "job-1", "status": "SUCCEEDED", "final_result": {"count": 1}}},
    ]

    def _fake_status(job_id, *, include_details=False):
        assert job_id == "job-1"
        payload = states.pop(0)
        if include_details:
            assert payload["job"]["status"] == "SUCCEEDED"
        return payload

    client.get_job_status = _fake_status  # type: ignore[method-assign]
    result = client.wait_for_terminal("job-1", timeout_sec=2.0, poll_interval_sec=0.01)
    assert result["job"]["status"] == "SUCCEEDED"
    assert result["job"]["final_result"] == {"count": 1}


def test_job_queue_client_decodes_dataframe_final_result_from_job_response() -> None:
    from pycloud_parallel import JobQueue

    frame = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
    client = JobQueue("127.0.0.1:50051", client_id="client-final-result")

    def _fake_call(*, service_name, method, payload=None, timeout_sec=60.0, service_token=None):
        del service_name, method, payload, timeout_sec, service_token
        return {
            "ok": True,
            "job": {
                "job_id": "job-1",
                "status": "SUCCEEDED",
                "final_result": [serialize_arrow_compatible(frame)],
                "results": [
                    {
                        "index": 0,
                        "result": serialize_arrow_compatible(frame),
                    }
                ],
            },
        }

    client._call_job_orchestrator = lambda *, effective_policy, **kwargs: _fake_call(
        **{key: value for key, value in kwargs.items() if key != "serialization_mode"}
    )  # type: ignore[method-assign]
    try:
        resp = client.get_job_status("job-1", include_details=True)
    finally:
        client.close()

    final_result = resp["job"]["final_result"]
    assert isinstance(final_result, list)
    assert len(final_result) == 1
    assert isinstance(final_result[0], pd.DataFrame)
    assert final_result[0].equals(frame)
    assert isinstance(resp["job"]["results"][0]["result"], pd.DataFrame)


def test_job_queue_client_get_job_status_defaults_to_metadata_only() -> None:
    from pycloud_parallel import JobQueue

    client = JobQueue("127.0.0.1:50051", client_id="client-status")
    captured = {}

    def _fake_call(*, service_name, method, payload=None, timeout_sec=60.0, service_token=None):
        del service_name, method, timeout_sec, service_token
        captured["payload"] = dict(payload or {})
        return {"ok": True, "job": {"job_id": "job-1", "status": "SUCCEEDED"}}

    client._call_job_orchestrator = lambda *, effective_policy, **kwargs: _fake_call(
        **{key: value for key, value in kwargs.items() if key != "serialization_mode"}
    )  # type: ignore[method-assign]
    try:
        resp = client.get_job_status("job-1")
    finally:
        client.close()

    assert resp["job"]["job_id"] == "job-1"
    assert captured["payload"] == {"job_id": "job-1", "include_details": False}
