from __future__ import annotations

"""Filesystem helpers for NodeControl code/object layout and managed-globals snapshots."""

import contextlib
import json
import os
import re
import shutil
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple

from pycloud_parallel.controlplane.artifact import _normalize_dependency_policy_mode
from pycloud_parallel.controlplane.code_version import _sha256_text, _stable_json_bytes
from pycloud_parallel.controlplane.node.models import CodeArtifact, ManagedGlobalsState
from pycloud_parallel.controlplane.state_time import utc_now
from pycloud_parallel.data.ref import normalize_object_id


_SEGMENT_WRITER_LOCKS_LOCK = threading.Lock()
_SEGMENT_WRITER_LOCKS: Dict[Tuple[str, int], threading.Lock] = {}


def _managed_globals_scope_dir(base_dir: Path, *, scope_kind: str, scope_key: str) -> Path:
    import hashlib

    digest = hashlib.sha1(f"{scope_kind}:{scope_key}".encode("utf-8")).hexdigest()
    return Path(base_dir) / scope_kind / digest


def _normalize_code_version(code_version: str) -> str:
    digest = str(code_version or "").replace("sha256:", "").strip().lower()
    if not digest:
        raise ValueError("invalid code_version")
    return digest


def _split_code_version(code_version: str) -> Tuple[str, str]:
    normalized = _normalize_code_version(code_version)
    if "." not in normalized:
        return normalized, ""
    code_digest, variant_digest = normalized.split(".", 1)
    return code_digest, variant_digest


def _code_digest_from_code_version(code_version: str) -> str:
    return _normalize_code_version(code_version)


def _code_storage_key(code_version: str) -> str:
    import hashlib

    normalized = _code_digest_from_code_version(code_version)
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def _code_scope_dir(base_dir: Path, *, code_version: str) -> Path:
    return Path(base_dir) / "codes" / _code_storage_key(code_version)


def _code_subversion_key(code_version: str) -> str:
    _code_digest, variant_digest = _split_code_version(code_version)
    return variant_digest or "default"


def _code_content_storage_key(code_version: str) -> str:
    import hashlib

    code_digest, _variant_digest = _split_code_version(code_version)
    return hashlib.sha1(code_digest.encode("utf-8")).hexdigest()[:20]


def _code_content_dir(base_dir: Path, *, code_version: str) -> Path:
    return Path(base_dir) / "codes" / _code_content_storage_key(code_version)


def _code_variant_dir(base_dir: Path, *, code_version: str) -> Path:
    return _code_content_dir(base_dir, code_version=code_version) / "subversions" / _code_subversion_key(code_version)


def _code_pkg_dir(base_dir: Path, *, code_version: str) -> Path:
    return _code_content_dir(base_dir, code_version=code_version) / "pkg"


def _code_globals_dir(base_dir: Path, *, code_version: str) -> Path:
    return _code_variant_dir(base_dir, code_version=code_version) / "globals"


def _code_data_dir(base_dir: Path, *, code_version: str) -> Path:
    return _code_variant_dir(base_dir, code_version=code_version) / "data"


def _code_index_dir(base_dir: Path) -> Path:
    return Path(base_dir) / "code_index"


def _sanitize_code_index_part(value: str, *, fallback: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    normalized = re.sub(r"_+", "_", normalized).strip("._-")
    return (normalized or fallback)[:64]


def _code_index_name(*, code_version: str, entry_module: str, entry_callable: str) -> str:
    digest = _code_digest_from_code_version(code_version)
    code_digest, variant_digest = digest, ""
    if "." in digest:
        code_digest, variant_digest = digest.split(".", 1)
    module_part = _sanitize_code_index_part(entry_module, fallback="module")
    callable_part = _sanitize_code_index_part(entry_callable, fallback="module")
    suffix = code_digest[:12]
    if variant_digest:
        suffix += f"_{variant_digest[:8]}"
    return f"{module_part}__{callable_part}__{suffix}"


def _code_index_link_path(base_dir: Path, *, code_version: str, entry_module: str, entry_callable: str) -> Path:
    return _code_index_dir(base_dir) / _code_index_name(
        code_version=code_version,
        entry_module=entry_module,
        entry_callable=entry_callable,
    )


def _code_index_meta_path(base_dir: Path, *, code_version: str, entry_module: str, entry_callable: str) -> Path:
    link_path = _code_index_link_path(
        base_dir,
        code_version=code_version,
        entry_module=entry_module,
        entry_callable=entry_callable,
    )
    return Path(f"{link_path}.meta.json")


def _code_dependency_dir(base_dir: Path, *, code_version: str) -> Path:
    return _code_variant_dir(base_dir, code_version=code_version) / "deps"


def _legacy_code_meta_path(base_dir: Path, *, code_version: str) -> Path:
    return _code_scope_dir(base_dir, code_version=code_version) / "meta.json"


def _code_meta_path(base_dir: Path, *, code_version: str) -> Path:
    return _code_variant_dir(base_dir, code_version=code_version) / "meta.json"


def _existing_code_meta_path(base_dir: Path, *, code_version: str) -> Path:
    preferred = _code_meta_path(base_dir, code_version=code_version)
    if preferred.exists():
        return preferred
    legacy = _legacy_code_meta_path(base_dir, code_version=code_version)
    if legacy.exists():
        return legacy
    return preferred


def _code_archive_path(base_dir: Path, *, code_version: str, package_format: str) -> Path:
    from pycloud_parallel.controlplane.node.execution import _normalize_package_format

    normalized = _normalize_package_format(package_format)
    code_dir = _code_content_dir(base_dir, code_version=code_version)
    if normalized == "tar.gz":
        return code_dir / "artifact.tar.gz"
    if normalized == "zip":
        return code_dir / "artifact.zip"
    if normalized == "whl":
        return code_dir / "artifact.whl"
    raise ValueError(f"unsupported archive package_format: {package_format}")


def _code_exec_path(base_dir: Path, *, code_version: str, package_format: str) -> Path:
    from pycloud_parallel.controlplane.node.execution import _normalize_package_format

    normalized = _normalize_package_format(package_format)
    code_dir = _code_pkg_dir(base_dir, code_version=code_version)
    if normalized == "py":
        return code_dir / "artifact.py"
    if normalized in ("tar.gz", "zip", "whl"):
        return code_dir
    raise ValueError(f"unsupported package_format for code exec path: {package_format}")


def _objects_meta_dir(object_dir: Path) -> Path:
    return Path(object_dir) / "meta"


def _object_meta_path(object_dir: Path, *, object_id: str) -> Path:
    digest = normalize_object_id(object_id).replace("sha256:", "", 1)
    return _objects_meta_dir(object_dir) / f"{digest}.json"


def _segments_dir(object_dir: Path) -> Path:
    return Path(object_dir) / "segments"


def _materialized_objects_dir(object_dir: Path) -> Path:
    return Path(object_dir) / "materialized"


def _segment_writer_key(object_dir: Path) -> Tuple[str, int]:
    return (str(Path(object_dir).resolve()), os.getpid())


def _segment_writer_lock(object_dir: Path) -> threading.Lock:
    key = _segment_writer_key(object_dir)
    with _SEGMENT_WRITER_LOCKS_LOCK:
        lock = _SEGMENT_WRITER_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _SEGMENT_WRITER_LOCKS[key] = lock
        return lock


def _segment_relpath(object_dir: Path, segment_path: Path) -> str:
    return str(segment_path.resolve().relative_to(Path(object_dir).resolve()))


def _segment_path_from_relpath(object_dir: Path, relpath: str) -> Path:
    return Path(object_dir).resolve() / str(relpath or "").strip()


def _managed_globals_manifest_path(scope_dir: Path, globals_digest: str) -> Path:
    normalized = str(globals_digest or "").replace("sha256:", "").strip().lower()
    if not normalized:
        raise ValueError("globals_digest is required")
    return Path(scope_dir) / "manifests" / f"{normalized}.json"


def _managed_globals_value_path(scope_dir: Path, *, value_digest: str) -> Path:
    normalized = str(value_digest or "").replace("sha256:", "").strip().lower()
    if not normalized:
        raise ValueError("value_digest is required")
    return Path(scope_dir) / "values" / f"{normalized}.json"


def _managed_globals_current_path(scope_dir: Path) -> Path:
    return Path(scope_dir) / "current.json"


def _normalize_managed_global_names(names: Sequence[str]) -> Tuple[str, ...]:
    normalized = []
    seen = set()
    for item in names or ():
        name = str(item or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        normalized.append(name)
    return tuple(sorted(normalized))


def _load_managed_globals_snapshot_serialized(state: ManagedGlobalsState) -> Dict[str, Any]:
    if not state.globals_digest:
        return {}
    scope_dir = Path(state.scope_dir)
    manifest_path = _managed_globals_manifest_path(scope_dir, state.globals_digest)
    if not manifest_path.exists():
        return {}
    manifest = json.loads(manifest_path.read_text(encoding="utf-8") or "{}")
    values_meta = dict(manifest.get("values") or {})
    out: Dict[str, Any] = {}
    for name in state.allowed_names:
        item = values_meta.get(name)
        if not isinstance(item, dict):
            continue
        value_digest = str(item.get("sha256", "") or "").strip()
        if not value_digest:
            continue
        value_path = _managed_globals_value_path(scope_dir, value_digest=value_digest)
        if not value_path.exists():
            continue
        out[name] = json.loads(value_path.read_text(encoding="utf-8") or "null")
    return out


def _write_managed_globals_snapshot(
    state: ManagedGlobalsState,
    *,
    values_serialized: Dict[str, Any],
) -> str:
    scope_dir = Path(state.scope_dir)
    manifests_dir = scope_dir / "manifests"
    values_dir = scope_dir / "values"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    values_dir.mkdir(parents=True, exist_ok=True)

    values_meta: Dict[str, Dict[str, str]] = {}
    for name in state.allowed_names:
        if name not in values_serialized:
            continue
        payload = values_serialized[name]
        value_digest = _sha256_text(payload)
        value_path = _managed_globals_value_path(scope_dir, value_digest=value_digest)
        if not value_path.exists():
            tmp_path = value_path.with_suffix(".tmp")
            tmp_path.write_bytes(_stable_json_bytes(payload))
            os.replace(str(tmp_path), str(value_path))
        values_meta[name] = {"sha256": value_digest}

    manifest = {
        "scope_kind": state.scope_kind,
        "scope_key": state.scope_key,
        "allowed_names": list(state.allowed_names),
        "values": values_meta,
    }
    globals_digest = _sha256_text(manifest)
    manifest_path = _managed_globals_manifest_path(scope_dir, globals_digest)
    if not manifest_path.exists():
        tmp_path = manifest_path.with_suffix(".tmp")
        tmp_path.write_bytes(_stable_json_bytes(manifest))
        os.replace(str(tmp_path), str(manifest_path))
    return globals_digest


def _load_code_meta(base_dir: Path, *, code_version: str) -> Dict[str, Any]:
    meta_path = _existing_code_meta_path(base_dir, code_version=code_version)
    if not meta_path.exists():
        return {}
    return json.loads(meta_path.read_text(encoding="utf-8") or "{}")


def _parse_timestamp_or_now(raw: Any) -> datetime:
    text = str(raw or "").strip()
    if text:
        with contextlib.suppress(Exception):
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
    return utc_now()


def _code_artifact_from_meta(meta: Dict[str, Any]) -> CodeArtifact:
    normalized_dependency_allowlist = tuple(
        str(name or "").strip() for name in list(meta.get("dependency_allowlist") or ()) if str(name or "").strip()
    )
    dependency_path = str(meta.get("dependency_path", "") or "").strip()
    raw_dependency_policy_mode = str(meta.get("dependency_policy_mode", "") or "").strip()
    if not raw_dependency_policy_mode and dependency_path and not normalized_dependency_allowlist:
        raw_dependency_policy_mode = "allow_install"
    return CodeArtifact(
        code_version=str(meta.get("code_version", "") or "").strip(),
        path=str(meta.get("artifact_path", "") or "").strip(),
        runtime=str(meta.get("runtime", "") or "").strip(),
        entry_module=str(meta.get("entry_module", "") or "").strip(),
        entry_callable=str(meta.get("entry_callable", "") or "").strip(),
        package_format=str(meta.get("package_format", "") or "").strip(),
        export_mode=str(meta.get("export_mode", "") or "").strip(),
        export_methods=tuple(str(name or "").strip() for name in list(meta.get("export_methods") or ()) if str(name or "").strip()),
        export_decorator=str(meta.get("export_decorator", "") or "").strip(),
        dependency_policy_mode=_normalize_dependency_policy_mode(
            raw_dependency_policy_mode,
            dependency_allowlist=normalized_dependency_allowlist,
        ),
        dependency_allowlist=normalized_dependency_allowlist,
        dependency_path=dependency_path,
        size_bytes=max(0, int(meta.get("size_bytes", 0) or 0)),
        created_at=_parse_timestamp_or_now(meta.get("created_at")),
    )


def _write_code_index(base_dir: Path, artifact: CodeArtifact, *, created_at: str, last_at: str) -> None:
    index_dir = _code_index_dir(base_dir)
    index_dir.mkdir(parents=True, exist_ok=True)
    code_dir = _code_variant_dir(base_dir, code_version=artifact.code_version)
    pkg_dir = _code_pkg_dir(base_dir, code_version=artifact.code_version)
    variant_dir = _code_variant_dir(base_dir, code_version=artifact.code_version)
    link_path = _code_index_link_path(
        base_dir,
        code_version=artifact.code_version,
        entry_module=artifact.entry_module,
        entry_callable=artifact.entry_callable,
    )
    meta_path = _code_index_meta_path(
        base_dir,
        code_version=artifact.code_version,
        entry_module=artifact.entry_module,
        entry_callable=artifact.entry_callable,
    )
    payload = {
        "code_version": artifact.code_version,
        "entry_module": artifact.entry_module,
        "entry_callable": artifact.entry_callable,
        "package_format": artifact.package_format,
        "runtime": artifact.runtime,
        "dependency_policy_mode": artifact.dependency_policy_mode,
        "artifact_path": artifact.path,
        "dependency_path": artifact.dependency_path,
        "code_dir": str(code_dir),
        "pkg_dir": str(pkg_dir),
        "variant_dir": str(variant_dir),
        "index_path": str(link_path),
        "storage_key": code_dir.name,
        "size_bytes": int(artifact.size_bytes),
        "created_at": created_at,
        "last_at": last_at,
    }
    _atomic_write_json(meta_path, payload, prefix=".code-index-")
    relative_target = os.path.relpath(code_dir, start=index_dir)
    try:
        if link_path.is_symlink():
            current_target = os.readlink(link_path)
            current_resolved = (link_path.parent / current_target).resolve(strict=False)
            expected_resolved = code_dir.resolve(strict=False)
            if current_resolved == expected_resolved:
                return
            link_path.unlink()
        elif link_path.exists():
            if link_path.is_dir():
                shutil.rmtree(link_path, ignore_errors=True)
            else:
                link_path.unlink()
        os.symlink(relative_target, str(link_path), target_is_directory=True)
    except (FileExistsError, NotImplementedError, OSError):
        return


def _ensure_code_index_entry(base_dir: Path, *, code_version: str) -> bool:
    meta = _load_code_meta(base_dir, code_version=code_version)
    if not meta:
        return False
    artifact = _code_artifact_from_meta(meta)
    if not artifact.code_version or not artifact.entry_module:
        return False
    created_at = str(meta.get("created_at", "") or artifact.created_at.astimezone(timezone.utc).isoformat())
    last_at = str(meta.get("last_at", "") or created_at)
    _write_code_index(base_dir, artifact, created_at=created_at, last_at=last_at)
    return True


def _write_code_meta(base_dir: Path, artifact: CodeArtifact, *, last_at: Optional[datetime] = None) -> None:
    meta_path = _code_meta_path(base_dir, code_version=artifact.code_version)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    existing = _load_code_meta(base_dir, code_version=artifact.code_version)
    created_at = artifact.created_at.astimezone(timezone.utc).isoformat()
    if existing.get("created_at"):
        created_at = str(existing.get("created_at"))
    effective_last_at = (
        last_at.astimezone(timezone.utc).isoformat()
        if last_at is not None
        else str(existing.get("last_at", "") or created_at)
    )
    payload = {
        "code_version": artifact.code_version,
        "runtime": artifact.runtime,
        "entry_module": artifact.entry_module,
        "entry_callable": artifact.entry_callable,
        "package_format": artifact.package_format,
        "export_mode": artifact.export_mode,
        "export_methods": list(artifact.export_methods),
        "export_decorator": artifact.export_decorator,
        "dependency_policy_mode": artifact.dependency_policy_mode,
        "dependency_allowlist": list(artifact.dependency_allowlist),
        "artifact_path": artifact.path,
        "dependency_path": artifact.dependency_path,
        "data_path": str(_code_data_dir(base_dir, code_version=artifact.code_version)),
        "size_bytes": int(artifact.size_bytes),
        "created_at": created_at,
        "last_at": effective_last_at,
    }
    tmp_path = meta_path.with_suffix(".tmp")
    tmp_path.write_bytes(_stable_json_bytes(payload))
    os.replace(str(tmp_path), str(meta_path))
    _write_code_index(base_dir, artifact, created_at=created_at, last_at=effective_last_at)


def touch_code_last_at(base_dir: Path, *, code_version: str) -> None:
    meta = _load_code_meta(base_dir, code_version=code_version)
    if not meta:
        return
    meta_path = _existing_code_meta_path(base_dir, code_version=code_version)
    meta["last_at"] = utc_now().astimezone(timezone.utc).isoformat()
    tmp_path = meta_path.with_suffix(".tmp")
    tmp_path.write_bytes(_stable_json_bytes(meta))
    os.replace(str(tmp_path), str(meta_path))


def _atomic_write_json(path: Path, payload: Dict[str, Any], *, prefix: str = ".meta-") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = _stable_json_bytes(payload)
    fd, tmp_name = tempfile.mkstemp(prefix=prefix, suffix=".tmp", dir=str(path.parent))
    try:
        os.write(fd, data)
    finally:
        os.close(fd)
    os.replace(tmp_name, str(path))


def _write_managed_globals_current(scope_dir: Path, *, globals_digest: str) -> None:
    current_path = _managed_globals_current_path(scope_dir)
    current_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "globals_digest": str(globals_digest or "").strip(),
        "updated_at": utc_now().astimezone(timezone.utc).isoformat(),
    }
    tmp_path = current_path.with_suffix(".tmp")
    tmp_path.write_bytes(_stable_json_bytes(payload))
    os.replace(str(tmp_path), str(current_path))


def _code_artifact_exists(artifact: CodeArtifact) -> bool:
    return bool(str(artifact.path or "").strip()) and Path(artifact.path).exists()


__all__ = [
    "_atomic_write_json",
    "_code_archive_path",
    "_code_artifact_exists",
    "_code_artifact_from_meta",
    "_code_content_dir",
    "_code_content_storage_key",
    "_code_data_dir",
    "_code_dependency_dir",
    "_code_exec_path",
    "_code_globals_dir",
    "_code_variant_dir",
    "_materialized_objects_dir",
    "_object_meta_path",
    "_objects_meta_dir",
    "_ensure_code_index_entry",
    "_existing_code_meta_path",
    "_load_code_meta",
    "_load_managed_globals_snapshot_serialized",
    "_managed_globals_scope_dir",
    "_normalize_code_version",
    "_normalize_managed_global_names",
    "_parse_timestamp_or_now",
    "_segment_path_from_relpath",
    "_segment_relpath",
    "_segment_writer_key",
    "_segment_writer_lock",
    "_segments_dir",
    "_write_code_meta",
    "_write_managed_globals_current",
    "_write_managed_globals_snapshot",
    "touch_code_last_at",
]
