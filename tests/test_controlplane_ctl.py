from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pycloud_parallel.controlplane import ctl
from pycloud_parallel.controlplane.object_ref import object_id_from_sha256_hex, object_ref_to_payload, ObjectRef
from pycloud_parallel.controlplane.state import _managed_globals_scope_dir


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
    assert args.controlplane_port == 50051
    assert args.node_worker_capacity == 0
    assert args.env == []


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


def test_ctl_parser_accepts_start_gateway_command():
    parser = ctl.build_parser()
    args = parser.parse_args(["start-gateway", "--bind", "0.0.0.0:50052", "--infocenter-addr", "127.0.0.1:50051"])
    assert args.command == "start-gateway"
    assert args.bind == "0.0.0.0:50052"
    assert args.infocenter_addr == "127.0.0.1:50051"


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


def test_ctl_parser_accepts_gc_command():
    parser = ctl.build_parser()
    args = parser.parse_args(["gc", "--dry-run"])
    assert args.command == "gc"
    assert args.dry_run is True


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

    monkeypatch.setattr(ctl, "_ensure_runtime_dirs", lambda _root: None)
    monkeypatch.setattr(ctl, "_stop_all_managed_processes", lambda _root: None)
    monkeypatch.setattr(ctl.time, "sleep", lambda *_args: None)
    monkeypatch.setattr(ctl, "_start_controlplane", lambda *_args, **_kwargs: None)
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
    scopes_dir = artifact_dir / "codes" / code_sha / "scopes"
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
        object_ref_to_payload(ObjectRef(object_id=live_id, format="bin")),
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


def test_gc_codes_deletes_stale_code_dirs(tmp_path, capsys):
    artifact_dir = tmp_path / "code_cache"
    code_dir = artifact_dir / "codes" / ("f" * 64)
    code_dir.mkdir(parents=True, exist_ok=True)
    old_time = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    _write_json(
        code_dir / "meta.json",
        {
            "code_version": f"sha256:{'f'*64}",
            "artifact_path": str(code_dir / "artifact.py"),
            "created_at": old_time,
            "last_at": old_time,
        },
    )
    (code_dir / "artifact.py").write_text("def run():\n    return 1\n", encoding="utf-8")

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

    monkeypatch.setattr(ctl, "_stop_named_process", lambda root, name: stopped.append((root, name)))
    monkeypatch.setattr(ctl, "_best_effort_mark_node_lost", lambda target, name: cleaned.append((target, name)) or True)
    monkeypatch.setattr(ctl, "_log", lambda *_args: None)

    assert ctl._cmd_stop(args) == 0
    assert [name for _, name in stopped] == ["node-1", "node-2", "node-blue", "gateway", "controlplane"]
    assert cleaned == [
        ("127.0.0.1:50051", "node-1"),
        ("127.0.0.1:50051", "node-2"),
        ("127.0.0.1:50051", "node-blue"),
    ]


def test_cmd_stop_scan_ports_invokes_listener_cleanup(tmp_path, monkeypatch):
    parser = ctl.build_parser()
    args = parser.parse_args(["--runtime-root", str(tmp_path), "stop", "--scan-ports", "--ports", "50051,50061"])
    scanned: list[tuple[str, tuple[int, ...]]] = []
    logs: list[tuple[str, str]] = []

    monkeypatch.setattr(ctl, "_managed_process_names", lambda _root: [])
    monkeypatch.setattr(ctl, "_stop_all_managed_processes", lambda _root: None)
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
    assert "50061 (node-1-grpc): no listener" in out


def test_cmd_start_node_defaults_to_controlplane_target_and_local_advertise(tmp_path, monkeypatch):
    parser = ctl.build_parser()
    args = parser.parse_args([
        "--runtime-root",
        str(tmp_path),
        "--controlplane-port",
        "51051",
        "start-node",
        "--node-id",
        "node-blue",
        "--bind",
        "0.0.0.0:51061",
        "--service-http-bind",
        "127.0.0.1:18181",
    ])
    started: list[dict[str, object]] = []

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
            "bind": "0.0.0.0:51061",
            "service_http_bind": "127.0.0.1:18181",
            "infocenter_addr": "127.0.0.1:51051",
            "advertise_addr": "127.0.0.1:51061",
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
