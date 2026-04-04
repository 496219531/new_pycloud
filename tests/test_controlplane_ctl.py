from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

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


def test_ctl_parser_accepts_gc_command():
    parser = ctl.build_parser()
    args = parser.parse_args(["gc", "--dry-run"])
    assert args.command == "gc"
    assert args.dry_run is True


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
