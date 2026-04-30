from __future__ import annotations

"""Tests for the pycloudctl CLI surface."""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from pycloud_parallel.controlplane import ctl
from pycloud_parallel.controlplane import server as controlplane_server
from pycloud_parallel.data.ref import DataRef, object_id_from_sha256_hex, data_ref_to_payload
from pycloud_parallel.controlplane.node.filesystem import (
    _code_content_dir,
    _code_index_link_path,
    _code_index_meta_path,
    _code_variant_dir,
    _ensure_code_index_entry,
    _managed_globals_scope_dir,
)


def _mock_public_host(host: str, *, remote_hint: str = "") -> str:
    del remote_hint
    text = str(host or "").strip()
    if text in {"", "0.0.0.0", "::", "[::]"}:
        return "10.0.0.9"
    return text.strip("[]")


def test_default_node_worker_capacity_is_positive(monkeypatch):
    monkeypatch.setattr(ctl.os, "cpu_count", lambda: 8)
    assert ctl._default_node_worker_capacity() == 4


def test_default_node_worker_capacity_handles_single_cpu(monkeypatch):
    monkeypatch.setattr(ctl.os, "cpu_count", lambda: 1)
    assert ctl._default_node_worker_capacity() == 1


def test_ctl_parser_accepts_start_command():
    parser = ctl.build_parser()
    args = parser.parse_args(["start"])
    assert args.command == "start"
    assert args.local is False
    assert args.controlplane_port == 50051
    assert args.node_worker_capacity == 0
    assert args.env == []


def test_ctl_parser_accepts_local_flag_after_start_command():
    parser = ctl.build_parser()
    args = parser.parse_args(["start", "--local"])
    assert args.command == "start"
    assert args.local is True


def test_ctl_parser_accepts_local_flag_before_restart_command():
    parser = ctl.build_parser()
    args = parser.parse_args(["--local", "restart"])
    assert args.command == "restart"
    assert args.local is True


def test_ctl_parser_accepts_env_overrides_for_start_commands():
    parser = ctl.build_parser()
    args = parser.parse_args(
        [
            "start-controlplane",
            "--env",
            "PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES=1048576",
            "--env",
            "PYCLOUD_GRPC_MAX_SEND_MESSAGE_LENGTH_BYTES=16777216",
        ]
    )
    assert args.command == "start-controlplane"
    assert args.env == [
        "PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES=1048576",
        "PYCLOUD_GRPC_MAX_SEND_MESSAGE_LENGTH_BYTES=16777216",
    ]


def test_ctl_parser_accepts_debug_flag():
    parser = ctl.build_parser()

    args = parser.parse_args(["start", "--debug"])
    assert args.command == "start"
    assert args.debug is True


def test_ctl_parser_rejects_debug_typo_alias():
    parser = ctl.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["start-node", "--dubug"])


def test_ctl_parser_accepts_stop_node_command():
    parser = ctl.build_parser()
    args = parser.parse_args(["stop-node", "node-1"])
    assert args.command == "stop-node"
    assert args.node_name == "node-1"


def test_ctl_parser_accepts_start_node_command():
    parser = ctl.build_parser()
    args = parser.parse_args(["start-node", "--node-id", "node-a", "--bind", "0.0.0.0:51061"])
    assert args.command == "start-node"
    assert args.node_id == "node-a"
    assert args.bind == "0.0.0.0:51061"


def test_ctl_start_node_help_mentions_canonical_bind_options():
    parser = ctl.build_parser()
    help_text = parser.format_help()
    start_node = parser._subparsers._group_actions[0].choices["start-node"]
    start_node_help = start_node.format_help()
    assert "--bind" in start_node_help
    assert "--service-http-bind" in start_node_help
    assert "--advertise-addr" in start_node_help
    assert "start-node" in help_text


def test_ctl_parser_accepts_start_gateway_command():
    parser = ctl.build_parser()
    args = parser.parse_args(["start-gateway", "--bind", "0.0.0.0:50052", "--infocenter-addr", "127.0.0.1:50051"])
    assert args.command == "start-gateway"
    assert args.bind == "0.0.0.0:50052"
    assert args.infocenter_addr == "127.0.0.1:50051"


def test_ctl_parser_accepts_start_job_orchestrator_command():
    parser = ctl.build_parser()
    args = parser.parse_args(["start-job-orchestrator", "--bind", "0.0.0.0:50053", "--infocenter-addr", "127.0.0.1:50051"])
    assert args.command == "start-job-orchestrator"
    assert args.bind == "0.0.0.0:50053"
    assert args.infocenter_addr == "127.0.0.1:50051"


def test_server_role_prefixes_normalize_to_canonical_names():
    assert controlplane_server._normalize_role("info") == "infocenter"
    assert controlplane_server._normalize_role("gateway") == "gateway"
    assert controlplane_server._normalize_role("job-runner") == "joborchestrator"
    assert controlplane_server._normalize_role("node-local") == "nodecontrol"
    assert controlplane_server._normalize_role("controlplane") == "controlplane"


def test_ctl_role_from_command_normalizes_prefixes():
    assert ctl._role_from_command("python -m pycloud_parallel.controlplane.server --role info") == "infocenter"
    assert ctl._role_from_command("python -m pycloud_parallel.controlplane.server --role gate-http") == "gateway"
    assert ctl._role_from_command("python -m pycloud_parallel.controlplane.server --role job-main") == "joborchestrator"
    assert ctl._role_from_command("python -m pycloud_parallel.controlplane.server --role node-blue") == "nodecontrol"


def test_ctl_parser_accepts_status_target_option():
    parser = ctl.build_parser()
    args = parser.parse_args(["status", "--target", "127.0.0.1:50071"])
    assert args.command == "status"
    assert args.target == "127.0.0.1:50071"


def test_ctl_parser_accepts_doctor_command():
    parser = ctl.build_parser()
    args = parser.parse_args(["doctor", "--ports", "50051,50061"])
    assert args.command == "doctor"
    assert args.ports == "50051,50061"


def test_ctl_parser_accepts_cache_list_command():
    parser = ctl.build_parser()
    args = parser.parse_args(["cache-list", "--match", "demo"])
    assert args.command == "cache-list"
    assert args.match == "demo"


def test_ctl_parser_accepts_gc_command():
    parser = ctl.build_parser()
    args = parser.parse_args(["gc", "--dry-run"])
    assert args.command == "gc"
    assert args.dry_run is True


def test_ctl_parser_accepts_gc_force_flag():
    parser = ctl.build_parser()
    args = parser.parse_args(["gc", "--force"])
    assert args.command == "gc"
    assert args.force is True


def test_assert_bind_available_rejects_in_use_port():
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    try:
        with pytest.raises(RuntimeError, match="already in use|unavailable"):
            ctl._assert_bind_available(f"127.0.0.1:{port}")
    finally:
        sock.close()


def test_wait_ready_with_pid_fails_when_process_exits(monkeypatch):
    monkeypatch.setattr(ctl, "_is_pid_running", lambda _pid: False)
    assert ctl._wait_ready_with_pid(12345, 0.2, lambda: False) is False


def test_cmd_start_uses_env_override_for_node_worker_capacity(tmp_path, monkeypatch):
    parser = ctl.build_parser()
    args = parser.parse_args(
        [
            "--runtime-root",
            str(tmp_path),
            "start",
            "--env",
            "PYCLOUD_NODE_WORKER_CAPACITY=10",
        ]
    )
    started_nodes: list[dict[str, object]] = []

    monkeypatch.setattr(ctl, "detect_local_ip", lambda *, remote_hint="": "10.0.0.9")
    monkeypatch.setattr(ctl, "resolve_public_host", _mock_public_host)
    monkeypatch.setattr(ctl, "_ensure_runtime_dirs", lambda _root: None)
    monkeypatch.setattr(ctl, "_stop_all_managed_processes", lambda _root: None)
    monkeypatch.setattr(ctl.time, "sleep", lambda *_args: None)
    monkeypatch.setattr(ctl, "_start_controlplane", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ctl, "_start_job_orchestrator", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        ctl,
        "_start_node",
        lambda root, name, port, http_port, infocenter_target, worker_capacity, **kwargs: started_nodes.append(
            {
                "root": root,
                "name": name,
                "port": port,
                "http_port": http_port,
                "infocenter_target": infocenter_target,
                "worker_capacity": worker_capacity,
                **kwargs,
            }
        ),
    )

    assert ctl._cmd_start(args) == 0
    assert [item["worker_capacity"] for item in started_nodes] == [10, 10]


def test_cmd_start_propagates_host_overrides(tmp_path, monkeypatch):
    parser = ctl.build_parser()
    args = parser.parse_args(
        [
            "--runtime-root",
            str(tmp_path),
            "--controlplane-host",
            "127.0.0.1",
            "--controlplane-port",
            "51051",
            "--node1-host",
            "0.0.0.0",
            "--node1-port",
            "51061",
            "--node1-http-host",
            "0.0.0.0",
            "--node1-http-port",
            "18181",
            "--node2-host",
            "192.168.1.10",
            "--node2-port",
            "51062",
            "--node2-http-host",
            "127.0.0.1",
            "--node2-http-port",
            "18182",
            "start",
        ]
    )
    controlplane_calls: list[dict[str, object]] = []
    job_orchestrator_calls: list[dict[str, object]] = []
    started_nodes: list[dict[str, object]] = []

    monkeypatch.setattr(ctl, "detect_local_ip", lambda *, remote_hint="": "10.0.0.9")
    monkeypatch.setattr(ctl, "resolve_public_host", _mock_public_host)
    monkeypatch.setattr(ctl, "_ensure_runtime_dirs", lambda _root: None)
    monkeypatch.setattr(ctl, "_stop_all_managed_processes", lambda _root: None)
    monkeypatch.setattr(ctl.time, "sleep", lambda *_args: None)
    monkeypatch.setattr(
        ctl,
        "_start_controlplane",
        lambda root, port, **kwargs: controlplane_calls.append({"root": root, "port": port, **kwargs}),
    )
    monkeypatch.setattr(
        ctl,
        "_start_job_orchestrator",
        lambda root, **kwargs: job_orchestrator_calls.append({"root": root, **kwargs}),
    )
    monkeypatch.setattr(
        ctl,
        "_start_node",
        lambda root, name, port, http_port, infocenter_target, worker_capacity, **kwargs: started_nodes.append(
            {
                "root": root,
                "name": name,
                "port": port,
                "http_port": http_port,
                "infocenter_target": infocenter_target,
                "worker_capacity": worker_capacity,
                **kwargs,
            }
        ),
    )

    assert ctl._cmd_start(args) == 0
    assert controlplane_calls == [
        {
            "root": tmp_path.resolve(),
            "port": 51051,
            "bind_host": "127.0.0.1",
            "remote_hint": "127.0.0.1:51051",
            "extra_env": {},
        }
    ]
    assert job_orchestrator_calls == [
        {
            "root": tmp_path.resolve(),
            "bind": "10.0.0.9:50053",
            "infocenter_addr": "127.0.0.1:51051",
            "extra_env": {},
        }
    ]
    assert started_nodes[0]["bind_host"] == "10.0.0.9"
    assert started_nodes[0]["service_http_host"] == "10.0.0.9"
    assert started_nodes[0]["advertise_host"] == "10.0.0.9"
    assert started_nodes[0]["infocenter_target"] == "127.0.0.1:51051"
    assert started_nodes[1]["bind_host"] == "192.168.1.10"
    assert started_nodes[1]["service_http_host"] == "127.0.0.1"
    assert started_nodes[1]["advertise_host"] == "192.168.1.10"


def test_cmd_start_uses_loopback_defaults_when_local_enabled(tmp_path, monkeypatch):
    parser = ctl.build_parser()
    args = parser.parse_args(
        [
            "--runtime-root",
            str(tmp_path),
            "start",
            "--local",
        ]
    )
    controlplane_calls: list[dict[str, object]] = []
    job_orchestrator_calls: list[dict[str, object]] = []
    started_nodes: list[dict[str, object]] = []

    monkeypatch.setattr(ctl, "detect_local_ip", lambda *, remote_hint="": "10.0.0.9")
    monkeypatch.setattr(ctl, "resolve_public_host", _mock_public_host)
    monkeypatch.setattr(ctl, "_ensure_runtime_dirs", lambda _root: None)
    monkeypatch.setattr(ctl, "_stop_all_managed_processes", lambda _root: None)
    monkeypatch.setattr(ctl.time, "sleep", lambda *_args: None)
    monkeypatch.setattr(
        ctl,
        "_start_controlplane",
        lambda root, port, **kwargs: controlplane_calls.append({"root": root, "port": port, **kwargs}),
    )
    monkeypatch.setattr(
        ctl,
        "_start_job_orchestrator",
        lambda root, **kwargs: job_orchestrator_calls.append({"root": root, **kwargs}),
    )
    monkeypatch.setattr(
        ctl,
        "_start_node",
        lambda root, name, port, http_port, infocenter_target, worker_capacity, **kwargs: started_nodes.append(
            {
                "root": root,
                "name": name,
                "port": port,
                "http_port": http_port,
                "infocenter_target": infocenter_target,
                "worker_capacity": worker_capacity,
                **kwargs,
            }
        ),
    )

    assert ctl._cmd_start(args) == 0
    assert controlplane_calls[0]["bind_host"] == "127.0.0.1"
    assert controlplane_calls[0]["remote_hint"] == "127.0.0.1:50051"
    assert job_orchestrator_calls == [
        {
            "root": tmp_path.resolve(),
            "bind": "127.0.0.1:50053",
            "infocenter_addr": "127.0.0.1:50051",
            "extra_env": {},
        }
    ]
    assert started_nodes[0]["bind_host"] == "127.0.0.1"
    assert started_nodes[0]["service_http_host"] == "127.0.0.1"
    assert started_nodes[0]["advertise_host"] == "127.0.0.1"
    assert started_nodes[1]["bind_host"] == "127.0.0.1"
    assert started_nodes[1]["service_http_host"] == "127.0.0.1"
    assert started_nodes[1]["advertise_host"] == "127.0.0.1"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_gc_objects_keeps_current_globals_refs_and_deletes_stale_others(tmp_path, capsys):
    artifact_dir = tmp_path / "code_cache"
    object_dir = artifact_dir / "objects"
    meta_dir = object_dir / "meta"
    object_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    live_digest = "a" * 64
    stale_digest = "b" * 64
    live_id = object_id_from_sha256_hex(live_digest)
    stale_id = object_id_from_sha256_hex(stale_digest)
    live_path = object_dir / f"{live_digest}.bin"
    stale_path = object_dir / f"{stale_digest}.bin"
    live_path.write_bytes(b"live")
    stale_path.write_bytes(b"stale")

    old_time = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    _write_json(
        meta_dir / f"{live_digest}.json",
        {
            "object_id": live_id,
            "format": "bin",
            "size_bytes": 4,
            "created_at": old_time,
            "last_at": old_time,
        },
    )
    _write_json(
        meta_dir / f"{stale_digest}.json",
        {
            "object_id": stale_id,
            "format": "bin",
            "size_bytes": 5,
            "created_at": old_time,
            "last_at": old_time,
        },
    )

    code_sha = "c" * 64
    scopes_dir = artifact_dir / "codes" / code_sha / "subversions" / "subv-1" / "globals"
    scope_dir = _managed_globals_scope_dir(scopes_dir, scope_kind="service", scope_key="svc-1")
    globals_digest = "d" * 64
    value_digest = "e" * 64
    _write_json(scope_dir / "current.json", {"globals_digest": f"sha256:{globals_digest}"})
    _write_json(
        scope_dir / "manifests" / f"{globals_digest}.json",
        {
            "scope_kind": "service",
            "scope_key": "svc-1",
            "allowed_names": ["MODEL"],
            "values": {"MODEL": {"sha256": f"sha256:{value_digest}"}},
        },
    )
    _write_json(
        scope_dir / "values" / f"{value_digest}.json",
        data_ref_to_payload(
            DataRef(
                ref_id=live_id,
                storage_id=live_id,
                logical_type="bytes",
                format="bin",
                size_bytes=0,
                materialize_as="bytes",
                locator_kind="node_local",
                locator_token="",
            )
        ),
    )

    parser = ctl.build_parser()
    dry_args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "gc",
        "--artifact-dir",
        str(artifact_dir),
        "--older-than-hours",
        "168",
        "--dry-run",
    ])
    assert ctl._cmd_gc(dry_args) == 0
    dry_payload = json.loads(capsys.readouterr().out)
    deleted_ids = {row["object_id"] for row in dry_payload["deleted_objects"]}
    kept_ids = {row["object_id"] for row in dry_payload["kept_objects"]}
    assert stale_id in deleted_ids
    assert live_id in kept_ids
    assert stale_path.exists()

    run_args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "gc",
        "--artifact-dir",
        str(artifact_dir),
        "--older-than-hours",
        "168",
    ])
    assert ctl._cmd_gc(run_args) == 0
    assert not stale_path.exists()
    assert live_path.exists()


def test_gc_objects_keeps_active_data_ref_objects(tmp_path, monkeypatch, capsys):
    artifact_dir = tmp_path / "code_cache"
    object_dir = artifact_dir / "objects"
    meta_dir = object_dir / "meta"
    object_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    kept_digest = "1" * 64
    stale_digest = "2" * 64
    kept_id = object_id_from_sha256_hex(kept_digest)
    stale_id = object_id_from_sha256_hex(stale_digest)
    kept_path = object_dir / f"{kept_digest}.bin"
    stale_path = object_dir / f"{stale_digest}.bin"
    kept_path.write_bytes(b"keep")
    stale_path.write_bytes(b"stale")

    old_time = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    _write_json(
        meta_dir / f"{kept_digest}.json",
        {
            "object_id": kept_id,
            "format": "bin",
            "size_bytes": 4,
            "created_at": old_time,
            "last_at": old_time,
        },
    )
    _write_json(
        meta_dir / f"{stale_digest}.json",
        {
            "object_id": stale_id,
            "format": "bin",
            "size_bytes": 5,
            "created_at": old_time,
            "last_at": old_time,
        },
    )

    monkeypatch.setattr(
        ctl,
        "_collect_active_data_ref_object_ids",
        lambda target: {kept_id} if target == "http://127.0.0.1:50051" else set(),
    )

    parser = ctl.build_parser()
    args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "gc",
        "--artifact-dir",
        str(artifact_dir),
        "--target",
        "http://127.0.0.1:50051",
        "--older-than-hours",
        "168",
    ])
    assert ctl._cmd_gc(args) == 0
    payload = json.loads(capsys.readouterr().out)

    kept_reasons = {row["object_id"]: row["reason"] for row in payload["kept_objects"]}
    deleted_ids = {row["object_id"] for row in payload["deleted_objects"]}
    assert kept_reasons[kept_id] == "referenced_by_active_data_ref"
    assert stale_id in deleted_ids
    assert kept_path.exists()
    assert not stale_path.exists()


def test_gc_segments_compacts_live_segment_and_deletes_orphan_segment(tmp_path, capsys):
    artifact_dir = tmp_path / "code_cache"
    object_dir = artifact_dir / "objects"
    meta_dir = object_dir / "meta"
    segments_dir = object_dir / "segments"
    meta_dir.mkdir(parents=True, exist_ok=True)
    segments_dir.mkdir(parents=True, exist_ok=True)

    stale_id = object_id_from_sha256_hex("3" * 64)
    live_id = object_id_from_sha256_hex("4" * 64)
    orphan_segment = segments_dir / "segment-orphan.bin"
    orphan_segment.write_bytes(b"orphan")
    segment_path = segments_dir / "segment-live.bin"
    stale_blob = b"dead"
    live_blob = b"live-data"
    segment_path.write_bytes(stale_blob + live_blob)
    old_time = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    recent_time = datetime.now(timezone.utc).isoformat()

    _write_json(
        meta_dir / f"{stale_id.replace('sha256:', '')}.json",
        {
            "object_id": stale_id,
            "format": "bin",
            "size_bytes": len(stale_blob),
            "created_at": old_time,
            "last_at": old_time,
            "storage_backend": "segment",
            "segment_relpath": "segments/segment-live.bin",
            "segment_offset": 0,
            "segment_length": len(stale_blob),
        },
    )
    _write_json(
        meta_dir / f"{live_id.replace('sha256:', '')}.json",
        {
            "object_id": live_id,
            "format": "bin",
            "size_bytes": len(live_blob),
            "created_at": old_time,
            "last_at": recent_time,
            "storage_backend": "segment",
            "segment_relpath": "segments/segment-live.bin",
            "segment_offset": len(stale_blob),
            "segment_length": len(live_blob),
        },
    )

    parser = ctl.build_parser()
    args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "gc",
        "--artifact-dir",
        str(artifact_dir),
        "--scope",
        "objects",
        "--older-than-hours",
        "168",
    ])
    assert ctl._cmd_gc(args) == 0
    payload = json.loads(capsys.readouterr().out)

    deleted_ids = {row["object_id"] for row in payload["deleted_objects"]}
    assert stale_id in deleted_ids
    assert orphan_segment.exists() is False
    assert any(row["segment_relpath"] == "segments/segment-orphan.bin" for row in payload["deleted_segments"])
    assert any(row["segment_relpath"] == "segments/segment-live.bin" for row in payload["compacted_segments"])

    meta = json.loads((meta_dir / f"{live_id.replace('sha256:', '')}.json").read_text(encoding="utf-8"))
    assert int(meta["segment_offset"]) == 0
    assert int(meta["segment_length"]) == len(live_blob)
    assert segment_path.read_bytes() == live_blob


def test_gc_codes_deletes_stale_code_dirs(tmp_path, capsys):
    artifact_dir = tmp_path / "code_cache"
    code_version = f"sha256:{'f' * 64}"
    code_dir = _code_content_dir(artifact_dir, code_version=code_version)
    variant_dir = _code_variant_dir(artifact_dir, code_version=code_version)
    variant_dir.mkdir(parents=True, exist_ok=True)
    old_time = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    _write_json(
        variant_dir / "meta.json",
        {
            "code_version": code_version,
            "runtime": "py3",
            "entry_module": "demo.gc_cleanup",
            "entry_callable": "run",
            "package_format": "py",
            "artifact_path": str(code_dir / "pkg" / "artifact.py"),
            "created_at": old_time,
            "last_at": old_time,
        },
    )
    (code_dir / "pkg").mkdir(parents=True, exist_ok=True)
    (code_dir / "pkg" / "artifact.py").write_text("def run():\n    return 1\n", encoding="utf-8")
    assert _ensure_code_index_entry(artifact_dir, code_version=code_version) is True
    index_path = _code_index_link_path(
        artifact_dir,
        code_version=code_version,
        entry_module="demo.gc_cleanup",
        entry_callable="run",
    )
    index_meta_path = _code_index_meta_path(
        artifact_dir,
        code_version=code_version,
        entry_module="demo.gc_cleanup",
        entry_callable="run",
    )
    assert index_meta_path.exists()

    parser = ctl.build_parser()
    args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "gc",
        "--artifact-dir",
        str(artifact_dir),
        "--scope",
        "codes",
        "--older-than-hours",
        "168",
    ])
    assert ctl._cmd_gc(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["deleted_codes"]
    assert not code_dir.exists()
    assert not index_path.exists()
    assert not index_meta_path.exists()


def test_gc_refuses_destructive_run_when_managed_processes_are_running(tmp_path, monkeypatch, capsys):
    parser = ctl.build_parser()
    args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "gc",
        "--artifact-dir",
        str(tmp_path / "code_cache"),
    ])
    monkeypatch.setattr(ctl, "_running_managed_processes", lambda _root: [("node-1", 1234), ("controlplane", 5678)])

    assert ctl._cmd_gc(args) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert "refused" in payload["error"]
    assert payload["running_processes"] == [
        {"name": "node-1", "pid": 1234},
        {"name": "controlplane", "pid": 5678},
    ]


def test_gc_allows_dry_run_when_managed_processes_are_running(tmp_path, monkeypatch, capsys):
    artifact_dir = tmp_path / "code_cache"
    (artifact_dir / "codes").mkdir(parents=True, exist_ok=True)
    parser = ctl.build_parser()
    args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "gc",
        "--artifact-dir",
        str(artifact_dir),
        "--dry-run",
    ])
    monkeypatch.setattr(ctl, "_running_managed_processes", lambda _root: [("node-1", 1234)])

    assert ctl._cmd_gc(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["dry_run"] is True


def test_gc_allows_force_when_managed_processes_are_running(tmp_path, monkeypatch, capsys):
    artifact_dir = tmp_path / "code_cache"
    (artifact_dir / "codes").mkdir(parents=True, exist_ok=True)
    parser = ctl.build_parser()
    args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "gc",
        "--artifact-dir",
        str(artifact_dir),
        "--force",
    ])
    monkeypatch.setattr(ctl, "_running_managed_processes", lambda _root: [("node-1", 1234)])

    assert ctl._cmd_gc(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["dry_run"] is False


def test_cache_list_rebuilds_index_and_returns_readable_paths(tmp_path, capsys):
    artifact_dir = tmp_path / "code_cache"
    code_version = f"sha256:{'1' * 64}.{'2' * 16}"
    code_dir = _code_content_dir(artifact_dir, code_version=code_version)
    variant_dir = _code_variant_dir(artifact_dir, code_version=code_version)
    variant_dir.mkdir(parents=True, exist_ok=True)
    created_at = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    last_at = datetime.now(timezone.utc).isoformat()
    _write_json(
        variant_dir / "meta.json",
        {
            "code_version": code_version,
            "runtime": "py3",
            "entry_module": "calc_asset_ratio.calc_asset_ratio",
            "entry_callable": "run_sync",
            "package_format": "py",
            "artifact_path": str(code_dir / "pkg" / "artifact.py"),
            "dependency_path": "",
            "size_bytes": 128,
            "created_at": created_at,
            "last_at": last_at,
        },
    )
    (code_dir / "pkg").mkdir(parents=True, exist_ok=True)
    (code_dir / "pkg" / "artifact.py").write_text("def run_sync():\n    return 1\n", encoding="utf-8")

    parser = ctl.build_parser()
    args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "cache-list",
        "--artifact-dir",
        str(artifact_dir),
        "--match",
        "calc_asset_ratio",
        "--json",
    ])
    assert ctl._cmd_cache_list(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    item = payload["items"][0]
    assert item["entry_module"] == "calc_asset_ratio.calc_asset_ratio"
    assert item["entry_callable"] == "run_sync"
    assert item["code_digest"] == "1" * 64
    assert item["code_key"] == code_dir.name
    assert item["subversion_key"] == variant_dir.name
    assert item["code_dir"] == str(variant_dir)
    assert item["content_dir"] == str(code_dir)
    assert "calc_asset_ratio.calc_asset_ratio__run_sync__" in item["index_path"]
    index_path = Path(item["index_path"])
    assert index_path.exists() or index_path.is_symlink()


def test_cmd_stop_node_only_stops_requested_node(tmp_path, monkeypatch):
    parser = ctl.build_parser()
    args = parser.parse_args(["--runtime-root", str(tmp_path), "stop-node", "node-blue"])
    stopped: list[tuple[Path, str]] = []
    logs: list[tuple[str, str]] = []
    cleaned: list[tuple[str, str]] = []

    monkeypatch.setattr(ctl, "_stop_named_process", lambda root, name: stopped.append((root, name)))
    monkeypatch.setattr(ctl, "_best_effort_mark_node_lost", lambda target, name: cleaned.append((target, name)) or True)
    monkeypatch.setattr(ctl, "_log", lambda label, message: logs.append((label, message)))

    assert ctl._cmd_stop_node(args) == 0
    assert stopped == [(tmp_path.resolve(), "node-blue")]
    assert cleaned == [("127.0.0.1:50051", "node-blue")]
    assert logs == [("OK", "node-blue stopped")]


def test_cmd_stop_stops_all_pid_backed_processes(tmp_path, monkeypatch):
    pids_dir = tmp_path / "pids"
    pids_dir.mkdir(parents=True, exist_ok=True)
    for name in ("gateway", "node-blue"):
        (pids_dir / f"{name}.pid").write_text("123\n", encoding="utf-8")
    parser = ctl.build_parser()
    args = parser.parse_args(["--runtime-root", str(tmp_path), "stop"])
    stopped: list[tuple[Path, str]] = []
    cleaned: list[tuple[str, str]] = []
    machine_scan_calls: list[tuple[Path, str]] = []

    monkeypatch.setattr(ctl, "_stop_named_process", lambda root, name: stopped.append((root, name)))
    monkeypatch.setattr(ctl, "_best_effort_mark_node_lost", lambda target, name: cleaned.append((target, name)) or True)
    monkeypatch.setattr(ctl, "_kill_machine_pycloud_processes", lambda *, root, target: machine_scan_calls.append((root, target)) or [])
    monkeypatch.setattr(ctl, "_log", lambda *_args: None)

    assert ctl._cmd_stop(args) == 0
    assert [name for _, name in stopped] == ["node-1", "node-2", "node-blue", "job-orchestrator", "gateway", "controlplane"]
    assert cleaned == [
        ("127.0.0.1:50051", "node-1"),
        ("127.0.0.1:50051", "node-2"),
        ("127.0.0.1:50051", "node-blue"),
    ]
    assert machine_scan_calls == [(tmp_path.resolve(), "127.0.0.1:50051")]


def test_kill_machine_pycloud_processes_stops_discovered_server_processes(tmp_path, monkeypatch):
    target = "127.0.0.1:50051"
    killed: list[tuple[int, bool]] = []
    cleaned: list[tuple[str, str]] = []
    removed: list[Path] = []
    logs: list[tuple[str, str]] = []

    monkeypatch.setattr(
        ctl,
        "_inspect_machine_processes",
        lambda: [
            {
                "pid": 301,
                "command": "python -m pycloud_parallel.controlplane.server --role nodecontrol --node-id node-blue",
                "matches_pycloud": True,
                "matches_stoppable_server": True,
                "role": "nodecontrol",
                "node_name": "node-blue",
            },
            {
                "pid": 302,
                "command": "python -m pycloud_parallel.controlplane.server --role gateway",
                "matches_pycloud": True,
                "matches_stoppable_server": True,
                "role": "gateway",
                "node_name": "",
            },
            {
                "pid": 303,
                "command": "python -m pycloud_parallel.controlplane.server --role job-orchestrator",
                "matches_pycloud": True,
                "matches_stoppable_server": True,
                "role": "joborchestrator",
                "node_name": "",
            },
            {
                "pid": 304,
                "command": "python -m something_else",
                "matches_pycloud": False,
                "matches_stoppable_server": False,
                "role": "",
                "node_name": "",
            },
            {
                "pid": 305,
                "command": "python -m pycloud_parallel.controlplane.server --role weird",
                "matches_pycloud": True,
                "matches_stoppable_server": True,
                "role": "weird",
                "node_name": "",
            },
            {
                "pid": 306,
                "command": "python -m pycloudctl",
                "matches_pycloud": True,
                "matches_stoppable_server": False,
                "role": "",
                "node_name": "",
            },
        ],
    )
    monkeypatch.setattr(ctl, "_terminate_pid", lambda pid, *, force=False: killed.append((pid, force)))
    monkeypatch.setattr(ctl, "_is_pid_running", lambda _pid: False)
    monkeypatch.setattr(ctl, "_best_effort_mark_node_lost", lambda target, name: cleaned.append((target, name)) or True)
    monkeypatch.setattr(ctl, "_remove_pid", lambda path: removed.append(path))
    monkeypatch.setattr(ctl, "_log", lambda label, message: logs.append((label, message)))

    rows = ctl._kill_machine_pycloud_processes(root=tmp_path.resolve(), target=target)

    assert [int(item["pid"]) for item in rows] == [301, 303, 302]
    assert killed == [(301, False), (303, False), (302, False)]
    assert cleaned == [(target, "node-blue")]
    assert removed == [
        tmp_path.resolve() / "pids" / "node-blue.pid",
        tmp_path.resolve() / "pids" / "job-orchestrator.pid",
        tmp_path.resolve() / "pids" / "gateway.pid",
    ]
    assert logs == [
        ("INFO", "Stopping discovered process PID 301 (node-blue)..."),
        ("INFO", "Stopping discovered process PID 303 (job-orchestrator)..."),
        ("INFO", "Stopping discovered process PID 302 (gateway)..."),
    ]


def test_inspect_machine_processes_returns_empty_when_windows_shell_missing(monkeypatch):
    monkeypatch.setattr(ctl.os, "name", "nt", raising=False)
    monkeypatch.setattr(ctl, "_run_windows_shell", lambda _command: None)

    assert ctl._inspect_machine_processes() == []


def test_command_for_pid_on_windows_falls_back_to_tasklist_when_shell_missing(monkeypatch):
    monkeypatch.setattr(ctl.os, "name", "nt", raising=False)
    monkeypatch.setattr(ctl, "_run_windows_shell", lambda _command: None)
    monkeypatch.setattr(
        ctl.subprocess,
        "run",
        lambda cmd, capture_output, text, check: SimpleNamespace(stdout='"python.exe","4321","Console","1","12,000 K"\n')
        if cmd[:3] == ["tasklist", "/FO", "CSV"]
        else (_ for _ in ()).throw(AssertionError(f"unexpected command: {cmd!r}")),
    )

    assert '"python.exe","4321","Console","1","12,000 K"' in ctl._command_for_pid(4321)


def test_cmd_stop_scan_ports_invokes_listener_cleanup(tmp_path, monkeypatch):
    parser = ctl.build_parser()
    args = parser.parse_args(["--runtime-root", str(tmp_path), "stop", "--scan-ports", "--ports", "50051,50061"])
    scanned: list[tuple[str, tuple[int, ...]]] = []
    logs: list[tuple[str, str]] = []

    monkeypatch.setattr(ctl, "_managed_process_names", lambda _root: [])
    monkeypatch.setattr(ctl, "_stop_all_managed_processes", lambda _root: None)
    monkeypatch.setattr(ctl, "_kill_machine_pycloud_processes", lambda *, root, target: [])
    monkeypatch.setattr(ctl, "_kill_scanned_port_processes", lambda *, target, ports: scanned.append((target, tuple(ports))) or [{"pid": 11, "port": 50051}])
    monkeypatch.setattr(ctl, "_log", lambda label, message: logs.append((label, message)))

    assert ctl._cmd_stop(args) == 0
    assert scanned == [("127.0.0.1:50051", (50051, 50061))]
    assert logs == [
        ("OK", "Stopped 1 additional scanned listener process(es)"),
        ("OK", "All services stopped"),
    ]


def test_cmd_doctor_uses_default_ports(tmp_path, monkeypatch, capsys):
    parser = ctl.build_parser()
    args = parser.parse_args(["--runtime-root", str(tmp_path), "doctor"])

    monkeypatch.setattr(ctl, "_managed_process_names", lambda _root: ["controlplane"])
    monkeypatch.setattr(ctl, "_read_pid", lambda _path: 123)
    monkeypatch.setattr(ctl, "_is_pid_running", lambda pid: pid == 123)
    monkeypatch.setattr(
        ctl,
        "_inspect_listening_ports",
        lambda ports: [
            {"port": list(ports)[0], "pid": 123, "command": "python -m pycloud_parallel.controlplane.server --role controlplane", "matches_pycloud": True, "role": "controlplane", "node_name": ""},
            {"port": list(ports)[1], "pid": 0, "command": "", "matches_pycloud": False, "role": "", "node_name": ""},
        ],
    )

    assert ctl._cmd_doctor(args) == 0
    out = capsys.readouterr().out
    assert "PyCloud Doctor" in out
    assert "Runtime Root:" in out
    assert "50051 (controlplane): pid=123 pycloud=yes role=controlplane node=-" in out
    assert "50053 (job-orchestrator): no listener" in out


def test_cmd_start_node_requires_explicit_infocenter_target(tmp_path, monkeypatch):
    parser = ctl.build_parser()
    args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "start-node",
        "--node-id",
        "node-blue",
        "--bind",
        "0.0.0.0:51061",
        "--service-http-bind",
        "127.0.0.1:18181",
    ])

    monkeypatch.setattr(ctl, "detect_local_ip", lambda *, remote_hint="": "10.0.0.9")
    monkeypatch.setattr(ctl, "resolve_public_host", _mock_public_host)
    monkeypatch.setattr(ctl, "_ensure_runtime_dirs", lambda _root: None)
    monkeypatch.setattr(ctl, "_stop_named_process", lambda *_args: None)

    with pytest.raises(RuntimeError, match="requires an explicit --infocenter-addr target"):
        ctl._cmd_start_node(args)


def test_cmd_start_node_uses_explicit_infocenter_target_and_local_advertise(tmp_path, monkeypatch):
    parser = ctl.build_parser()
    args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "start-node",
        "--node-id",
        "node-blue",
        "--bind",
        "0.0.0.0:51061",
        "--service-http-bind",
        "127.0.0.1:18181",
        "--infocenter-addr",
        "10.0.0.8:51051",
    ])
    started: list[dict[str, object]] = []

    monkeypatch.setattr(ctl, "detect_local_ip", lambda *, remote_hint="": "10.0.0.9")
    monkeypatch.setattr(ctl, "resolve_public_host", _mock_public_host)
    monkeypatch.setattr(ctl, "_ensure_runtime_dirs", lambda _root: None)
    monkeypatch.setattr(ctl, "_stop_named_process", lambda *_args: None)
    monkeypatch.setattr(ctl, "_default_node_worker_capacity", lambda: 6)
    monkeypatch.setattr(
        ctl,
        "_start_standalone_node",
        lambda root, **kwargs: started.append({"root": root, **kwargs}),
    )

    assert ctl._cmd_start_node(args) == 0
    assert started == [
        {
            "root": tmp_path.resolve(),
            "node_id": "node-blue",
            "bind": "10.0.0.9:51061",
            "service_http_bind": "127.0.0.1:18181",
            "infocenter_addr": "10.0.0.8:51051",
            "advertise_addr": "10.0.0.9:51061",
            "worker_capacity": 6,
            "queue_capacity": 4000,
            "max_workers": 64,
            "service_default_workers": 10,
            "service_heartbeat_timeout_sec": 30,
            "node_tags": "compute",
            "node_version": "v1",
            "extra_env": {},
        }
    ]


@pytest.mark.parametrize(
    ("argv", "expected_bind", "expected_infocenter"),
    [
        (["start-infocenter", "--local"], "127.0.0.1:50051", None),
        (["start-controlplane", "--local"], "127.0.0.1:50051", None),
        (["start-gateway", "--local", "--infocenter-addr", "127.0.0.1:50051"], "127.0.0.1:50052", "127.0.0.1:50051"),
        (["start-job-orchestrator", "--local", "--infocenter-addr", "127.0.0.1:50051"], "127.0.0.1:50053", "127.0.0.1:50051"),
    ],
)
def test_standalone_start_commands_use_loopback_defaults_when_local_enabled(
    tmp_path,
    monkeypatch,
    argv,
    expected_bind,
    expected_infocenter,
):
    parser = ctl.build_parser()
    args = parser.parse_args(["--runtime-root", str(tmp_path), *argv])
    infocenter_calls: list[dict[str, object]] = []
    controlplane_calls: list[dict[str, object]] = []
    gateway_calls: list[dict[str, object]] = []
    job_orchestrator_calls: list[dict[str, object]] = []

    monkeypatch.setattr(ctl, "detect_local_ip", lambda *, remote_hint="": "10.0.0.9")
    monkeypatch.setattr(ctl, "resolve_public_host", _mock_public_host)
    monkeypatch.setattr(ctl, "_ensure_runtime_dirs", lambda _root: None)
    monkeypatch.setattr(ctl, "_stop_named_process", lambda *_args: None)
    monkeypatch.setattr(
        ctl,
        "_start_infocenter",
        lambda root, **kwargs: infocenter_calls.append({"root": root, **kwargs}),
    )
    monkeypatch.setattr(
        ctl,
        "_start_standalone_controlplane",
        lambda root, **kwargs: controlplane_calls.append({"root": root, **kwargs}),
    )
    monkeypatch.setattr(
        ctl,
        "_start_gateway",
        lambda root, **kwargs: gateway_calls.append({"root": root, **kwargs}),
    )
    monkeypatch.setattr(
        ctl,
        "_start_job_orchestrator",
        lambda root, **kwargs: job_orchestrator_calls.append({"root": root, **kwargs}),
    )

    if args.command == "start-infocenter":
        assert ctl._cmd_start_infocenter(args) == 0
        assert infocenter_calls == [{"root": tmp_path.resolve(), "bind": expected_bind, "extra_env": {}}]
        return

    if args.command == "start-controlplane":
        assert ctl._cmd_start_controlplane(args) == 0
        assert controlplane_calls == [
            {
                "root": tmp_path.resolve(),
                "bind": expected_bind,
                "gateway_refresh_interval_sec": 3.0,
                "gateway_failure_threshold": 3,
                "gateway_open_sec": 5.0,
                "extra_env": {},
            }
        ]
        return

    if args.command == "start-job-orchestrator":
        assert ctl._cmd_start_job_orchestrator(args) == 0
        assert job_orchestrator_calls == [
            {
                "root": tmp_path.resolve(),
                "bind": expected_bind,
                "infocenter_addr": expected_infocenter,
                "node_id": "job-orchestrator-01",
                "service_name": "job-orchestrator",
                "queue_capacity": 4000,
                "node_tags": "job",
                "node_version": "v1",
                "extra_env": {},
            }
        ]
        return

    assert ctl._cmd_start_gateway(args) == 0
    assert gateway_calls == [
        {
            "root": tmp_path.resolve(),
            "bind": expected_bind,
            "infocenter_addr": expected_infocenter,
            "gateway_refresh_interval_sec": 3.0,
            "gateway_failure_threshold": 3,
            "gateway_open_sec": 5.0,
            "extra_env": {},
        }
    ]


def test_cmd_start_node_uses_loopback_defaults_when_local_enabled(tmp_path, monkeypatch):
    parser = ctl.build_parser()
    args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "start-node",
        "--local",
        "--node-id",
        "node-blue",
        "--infocenter-addr",
        "127.0.0.1:50051",
    ])
    started: list[dict[str, object]] = []

    monkeypatch.setattr(ctl, "detect_local_ip", lambda *, remote_hint="": "10.0.0.9")
    monkeypatch.setattr(ctl, "resolve_public_host", _mock_public_host)
    monkeypatch.setattr(ctl, "_ensure_runtime_dirs", lambda _root: None)
    monkeypatch.setattr(ctl, "_stop_named_process", lambda *_args: None)
    monkeypatch.setattr(ctl, "_default_node_worker_capacity", lambda: 6)
    monkeypatch.setattr(
        ctl,
        "_start_standalone_node",
        lambda root, **kwargs: started.append({"root": root, **kwargs}),
    )

    assert ctl._cmd_start_node(args) == 0
    assert started == [
        {
            "root": tmp_path.resolve(),
            "node_id": "node-blue",
            "bind": "127.0.0.1:50061",
            "service_http_bind": "127.0.0.1:18081",
            "infocenter_addr": "127.0.0.1:50051",
            "advertise_addr": "127.0.0.1:50061",
            "worker_capacity": 6,
            "queue_capacity": 4000,
            "max_workers": 64,
            "service_default_workers": 10,
            "service_heartbeat_timeout_sec": 30,
            "node_tags": "compute",
            "node_version": "v1",
            "extra_env": {},
        }
    ]


def test_cmd_start_node_derives_default_service_http_port_from_node_bind(tmp_path, monkeypatch):
    parser = ctl.build_parser()
    args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "start-node",
        "--node-id",
        "node-2",
        "--infocenter-addr",
        "10.168.70.123:50051",
        "--bind",
        "10.168.30.154:50062",
    ])
    started: list[dict[str, object]] = []

    monkeypatch.setattr(ctl, "detect_local_ip", lambda *, remote_hint="": "10.168.30.154")
    monkeypatch.setattr(ctl, "resolve_public_host", _mock_public_host)
    monkeypatch.setattr(ctl, "_ensure_runtime_dirs", lambda _root: None)
    monkeypatch.setattr(ctl, "_stop_named_process", lambda *_args: None)
    monkeypatch.setattr(ctl, "_default_node_worker_capacity", lambda: 6)
    monkeypatch.setattr(
        ctl,
        "_start_standalone_node",
        lambda root, **kwargs: started.append({"root": root, **kwargs}),
    )

    assert ctl._cmd_start_node(args) == 0
    assert started[0]["bind"] == "10.168.30.154:50062"
    assert started[0]["service_http_bind"] == "10.168.30.154:18082"


def test_cmd_start_node_can_disable_registration(tmp_path, monkeypatch):
    parser = ctl.build_parser()
    args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "start-node",
        "--node-id",
        "node-standalone",
        "--infocenter-addr",
        "",
    ])
    started: list[dict[str, object]] = []

    monkeypatch.setattr(ctl, "detect_local_ip", lambda *, remote_hint="": "10.0.0.9")
    monkeypatch.setattr(ctl, "resolve_public_host", _mock_public_host)
    monkeypatch.setattr(ctl, "_ensure_runtime_dirs", lambda _root: None)
    monkeypatch.setattr(ctl, "_stop_named_process", lambda *_args: None)
    monkeypatch.setattr(ctl, "_default_node_worker_capacity", lambda: 4)
    monkeypatch.setattr(
        ctl,
        "_start_standalone_node",
        lambda root, **kwargs: started.append({"root": root, **kwargs}),
    )

    assert ctl._cmd_start_node(args) == 0
    assert started[0]["infocenter_addr"] == ""
    assert started[0]["advertise_addr"] == ""


def test_cmd_start_node_canonical_addresses(tmp_path, monkeypatch):
    parser = ctl.build_parser()
    args = parser.parse_args(
        [
            "--runtime-root",
            str(tmp_path),
            "start-node",
            "--node-id",
            "node-green",
            "--bind",
            "0.0.0.0:52061",
            "--service-http-bind",
            "0.0.0.0:19181",
            "--advertise-addr",
            "10.0.0.9:62061",
            "--infocenter-addr",
            "10.0.0.8:51051",
        ]
    )
    started: list[dict[str, object]] = []

    monkeypatch.setattr(ctl, "detect_local_ip", lambda *, remote_hint="": "10.0.0.9")
    monkeypatch.setattr(ctl, "resolve_public_host", _mock_public_host)
    monkeypatch.setattr(ctl, "_ensure_runtime_dirs", lambda _root: None)
    monkeypatch.setattr(ctl, "_stop_named_process", lambda *_args: None)
    monkeypatch.setattr(ctl, "_default_node_worker_capacity", lambda: 3)
    monkeypatch.setattr(
        ctl,
        "_start_standalone_node",
        lambda root, **kwargs: started.append({"root": root, **kwargs}),
    )

    assert ctl._cmd_start_node(args) == 0
    assert started == [
        {
            "root": tmp_path.resolve(),
            "node_id": "node-green",
            "bind": "10.0.0.9:52061",
            "service_http_bind": "10.0.0.9:19181",
            "infocenter_addr": "10.0.0.8:51051",
            "advertise_addr": "10.0.0.9:62061",
            "worker_capacity": 3,
            "queue_capacity": 4000,
            "max_workers": 64,
            "service_default_workers": 10,
            "service_heartbeat_timeout_sec": 30,
            "node_tags": "compute",
            "node_version": "v1",
            "extra_env": {},
        }
    ]


def test_cmd_start_gateway_requires_explicit_infocenter_target(tmp_path, monkeypatch):
    parser = ctl.build_parser()
    args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "start-gateway",
    ])

    monkeypatch.setattr(ctl, "_ensure_runtime_dirs", lambda _root: None)
    monkeypatch.setattr(ctl, "_stop_named_process", lambda *_args: None)

    with pytest.raises(RuntimeError, match="start-gateway requires --infocenter-addr"):
        ctl._cmd_start_gateway(args)


def test_cmd_start_job_orchestrator_requires_explicit_infocenter_target(tmp_path, monkeypatch):
    parser = ctl.build_parser()
    args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "start-job-orchestrator",
    ])

    monkeypatch.setattr(ctl, "_ensure_runtime_dirs", lambda _root: None)
    monkeypatch.setattr(ctl, "_stop_named_process", lambda *_args: None)

    with pytest.raises(RuntimeError, match="start-job-orchestrator requires --infocenter-addr"):
        ctl._cmd_start_job_orchestrator(args)


def test_spawn_server_passes_env_overrides(tmp_path, monkeypatch):
    captured = {}

    class DummyProc:
        pid = 12345

    def fake_popen(cmd, stdout=None, stderr=None, cwd=None, env=None, close_fds=None, creationflags=0):
        captured["cmd"] = cmd
        captured["cwd"] = cwd
        captured["env"] = env
        captured["close_fds"] = close_fds
        captured["stdout"] = stdout
        captured["stderr"] = stderr
        captured["creationflags"] = creationflags
        return DummyProc()

    monkeypatch.setattr(ctl.subprocess, "Popen", fake_popen)
    log_path = tmp_path / "logs" / "service.log"
    pid = ctl._spawn_server(
        tmp_path,
        log_path,
        ["--role", "controlplane"],
        extra_env={"PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES": "1048576"},
    )

    assert pid == 12345
    assert captured["env"]["PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES"] == "1048576"


def test_spawn_server_uses_new_console_on_windows(tmp_path, monkeypatch):
    captured = {}

    class DummyProc:
        pid = 23456

    def fake_popen(cmd, stdout=None, stderr=None, cwd=None, env=None, close_fds=None, creationflags=0):
        captured["stdout"] = stdout
        captured["stderr"] = stderr
        captured["cwd"] = cwd
        captured["env"] = env
        captured["close_fds"] = close_fds
        captured["creationflags"] = creationflags
        return DummyProc()

    monkeypatch.setattr(ctl.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(ctl.os, "name", "nt", raising=False)
    monkeypatch.setattr(ctl.subprocess, "CREATE_NEW_CONSOLE", 16, raising=False)

    pid = ctl._spawn_server(tmp_path, tmp_path / "logs" / "service.log", ["--role", "controlplane"])

    assert pid == 23456
    assert captured["stdout"] is None
    assert captured["stderr"] is None
    assert captured["close_fds"] is False
    assert captured["creationflags"] == 16


def test_spawn_server_debug_inherits_stdio_on_posix(tmp_path, monkeypatch):
    captured = {}

    class DummyProc:
        pid = 34567

    def fake_popen(cmd, stdout=None, stderr=None, cwd=None, env=None, close_fds=None, creationflags=0):
        captured["stdout"] = stdout
        captured["stderr"] = stderr
        captured["cwd"] = cwd
        captured["env"] = env
        captured["close_fds"] = close_fds
        captured["creationflags"] = creationflags
        return DummyProc()

    monkeypatch.setattr(ctl.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(ctl.os, "name", "posix", raising=False)
    monkeypatch.setattr(ctl.sys, "platform", "linux", raising=False)

    pid = ctl._spawn_server(
        tmp_path,
        tmp_path / "logs" / "service.log",
        ["--role", "controlplane"],
        debug=True,
    )

    assert pid == 34567
    assert captured["stdout"] is None
    assert captured["stderr"] is None
    assert captured["close_fds"] is True
    assert captured["creationflags"] == 0


def test_spawn_server_debug_uses_terminal_windows_on_macos(tmp_path, monkeypatch):
    captured = {}

    def fake_spawn(root, log_path, args, *, env):
        captured["root"] = root
        captured["log_path"] = log_path
        captured["args"] = list(args)
        captured["env"] = dict(env)
        return 45678

    monkeypatch.setattr(ctl.os, "name", "posix", raising=False)
    monkeypatch.setattr(ctl.sys, "platform", "darwin", raising=False)
    monkeypatch.setattr(ctl, "_spawn_server_debug_macos_terminal", fake_spawn)

    pid = ctl._spawn_server(
        tmp_path,
        tmp_path / "logs" / "service.log",
        ["--role", "controlplane"],
        extra_env={"PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES": "1048576"},
        debug=True,
    )

    assert pid == 45678
    assert captured["root"] == tmp_path
    assert captured["log_path"] == tmp_path / "logs" / "service.log"
    assert captured["args"] == ["--role", "controlplane"]
    assert captured["env"]["PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES"] == "1048576"
