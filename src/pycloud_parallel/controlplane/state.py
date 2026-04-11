from __future__ import annotations

"""In-memory state backends for InfoCenter and NodeControl."""

import contextlib
import hashlib
import io
import importlib
import importlib.util
import inspect
import json
import logging
import os
import re
import subprocess
import secrets
import shutil
import sys
import tarfile
import tempfile
import threading
import time
import uuid
import zipfile
from concurrent.futures import TimeoutError as FutureTimeout
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple

from google.protobuf import timestamp_pb2

from pycloud_parallel.controlplane.executor_host import ExecutorHostClient
from pycloud_parallel.controlplane.http_gateway import ServiceHttpGateway
from pycloud_parallel.controlplane.hooks import InMemoryResultHook
from pycloud_parallel.controlplane.object_ref import (
    ObjectRef,
    is_object_ref_payload,
    normalize_materialize_as,
    object_format_suffix,
    normalize_object_format,
    normalize_object_id,
    object_id_from_sha256_hex,
    object_ref_from_payload,
    object_storage_path,
)
from pycloud_parallel.controlplane.result_ref import ResultRef
from pycloud_parallel.controlplane.runtime_spec import (
    matches_python_runtime,
    normalize_python_runtime_spec,
)
from pycloud_parallel.controlplane.config import FILE_HASH_CHUNK_SIZE_BYTES
from pycloud_parallel.controlplane.config import (
    OBJECT_SEGMENT_MAX_BYTES,
    OBJECT_SEGMENT_TARGET_BYTES,
)
from pycloud_parallel.controlplane.serialization import (
    convert_dict_to_arrow,
    dataframe_bundle_parquet_frame,
    dict_to_struct,
    is_arrow_compatible,
    log_payload_flow,
    serialize_arrow_compatible,
    serialize_dataframe_bundle,
    serialize_series_bundle,
    serialize_inline_result,
    summarize_payload_flow_value,
    struct_to_dict,
)
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

_DEFAULT_EXPORT_DECORATOR = "pycloud_export"
service_timing_logger = logging.getLogger("pycloud_parallel.service_timing")


class LargeResultError(ValueError):
    """Raised when a task result is too large for safe inline return."""


@dataclass(frozen=True)
class StoredResultArtifact:
    object_id: str
    format: str
    size_bytes: int
    materialize_as: str
    storage_backend: str = "file"
    segment_relpath: str = ""
    segment_offset: int = 0
    segment_length: int = 0


@dataclass
class ManagedGlobalsState:
    scope_kind: str
    scope_key: str
    scope_dir: str
    allowed_names: Tuple[str, ...]
    globals_digest: str


_MANAGED_GLOBALS_CACHE_LOCK = threading.Lock()
_MANAGED_GLOBALS_CACHE: Dict[str, str] = {}
_MANAGED_GLOBALS_APPLY_LOCKS_LOCK = threading.Lock()
_MANAGED_GLOBALS_APPLY_LOCKS: Dict[str, threading.Lock] = {}
_SEGMENT_WRITER_LOCKS_LOCK = threading.Lock()
_SEGMENT_WRITER_LOCKS: Dict[Tuple[str, int], threading.Lock] = {}
_SEGMENT_WRITER_STATE: Dict[Tuple[str, int], str] = {}


def _stable_json_bytes(data: Any) -> bytes:
    return json.dumps(
        data,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _sha256_text(data: Any) -> str:
    return f"sha256:{hashlib.sha256(_stable_json_bytes(data)).hexdigest()}"


def _code_version_from_digest(
    digest: str,
    *,
    runtime: str,
    entry_module: str,
    entry_callable: str,
    package_format: str,
    export_mode: str,
    export_methods: Sequence[str],
    export_decorator: str,
    dependency_allowlist: Sequence[str],
) -> str:
    normalized_digest = str(digest or "").strip().lower()
    if not normalized_digest:
        raise ValueError("invalid code digest")
    variant_payload = {
        "runtime": str(runtime or "").strip(),
        "entry_module": str(entry_module or "").strip(),
        "entry_callable": str(entry_callable or "").strip(),
        "package_format": str(package_format or "").strip(),
        "export_mode": str(export_mode or "").strip(),
        "export_methods": [str(name) for name in export_methods],
        "export_decorator": str(export_decorator or "").strip(),
        "dependency_allowlist": [str(name) for name in dependency_allowlist],
    }
    variant_digest = hashlib.sha256(_stable_json_bytes(variant_payload)).hexdigest()[:16]
    return f"sha256:{normalized_digest}.{variant_digest}"


def _managed_globals_scope_dir(base_dir: Path, *, scope_kind: str, scope_key: str) -> Path:
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
    normalized = _code_digest_from_code_version(code_version)
    # Keep filesystem paths short and stable across variant-suffixed code versions.
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def _code_scope_dir(base_dir: Path, *, code_version: str) -> Path:
    return Path(base_dir) / "codes" / _code_storage_key(code_version)


def _code_subversion_key(code_version: str) -> str:
    _code_digest, variant_digest = _split_code_version(code_version)
    return variant_digest or "default"


def _code_content_storage_key(code_version: str) -> str:
    code_digest, _variant_digest = _split_code_version(code_version)
    # Windows path length is tight once we add pkg/subversions/data nesting, so
    # keep the shared code directory name short while preserving the full digest
    # in metadata and logical identifiers.
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
    normalized: List[str] = []
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


def _code_artifact_from_meta(meta: Dict[str, Any]) -> "CodeArtifact":
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
        dependency_allowlist=tuple(
            str(name or "").strip() for name in list(meta.get("dependency_allowlist") or ()) if str(name or "").strip()
        ),
        dependency_path=str(meta.get("dependency_path", "") or "").strip(),
        size_bytes=max(0, int(meta.get("size_bytes", 0) or 0)),
        created_at=_parse_timestamp_or_now(meta.get("created_at")),
    )


def _write_code_index(base_dir: Path, artifact: "CodeArtifact", *, created_at: str, last_at: str) -> None:
    code_dir = _code_variant_dir(base_dir, code_version=artifact.code_version)
    pkg_dir = _code_pkg_dir(base_dir, code_version=artifact.code_version)
    variant_dir = _code_variant_dir(base_dir, code_version=artifact.code_version)
    index_dir = _code_index_dir(base_dir)
    index_dir.mkdir(parents=True, exist_ok=True)
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


def _write_code_meta(base_dir: Path, artifact: "CodeArtifact", *, last_at: Optional[datetime] = None) -> None:
    meta_path = _code_meta_path(base_dir, code_version=artifact.code_version)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    existing = _load_code_meta(base_dir, code_version=artifact.code_version)
    created_at = artifact.created_at.astimezone(timezone.utc).isoformat()
    if existing.get("created_at"):
        created_at = str(existing.get("created_at"))
    effective_last_at = last_at.astimezone(timezone.utc).isoformat() if last_at is not None else str(existing.get("last_at", "") or created_at)
    payload = {
        "code_version": artifact.code_version,
        "runtime": artifact.runtime,
        "entry_module": artifact.entry_module,
        "entry_callable": artifact.entry_callable,
        "package_format": artifact.package_format,
        "export_mode": artifact.export_mode,
        "export_methods": list(artifact.export_methods),
        "export_decorator": artifact.export_decorator,
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


def _write_object_meta(
    object_dir: Path,
    *,
    object_id: str,
    fmt: str,
    size_bytes: int,
    created_at: datetime,
    last_at: Optional[datetime] = None,
    storage_backend: str = "file",
    segment_relpath: str = "",
    segment_offset: int = 0,
    segment_length: int = 0,
) -> None:
    meta_dir = _objects_meta_dir(object_dir)
    meta_dir.mkdir(parents=True, exist_ok=True)
    meta_path = _object_meta_path(object_dir, object_id=object_id)
    timestamp = (last_at or created_at).astimezone(timezone.utc).isoformat()
    payload = {
        "object_id": normalize_object_id(object_id),
        "format": normalize_object_format(fmt, default="bin"),
        "size_bytes": max(0, int(size_bytes or 0)),
        "created_at": created_at.astimezone(timezone.utc).isoformat(),
        "last_at": timestamp,
        "storage_backend": str(storage_backend or "file").strip() or "file",
    }
    if payload["storage_backend"] == "segment":
        payload["segment_relpath"] = str(segment_relpath or "").strip()
        payload["segment_offset"] = max(0, int(segment_offset or 0))
        payload["segment_length"] = max(0, int(segment_length or 0))
    _atomic_write_json(meta_path, payload)


def _load_object_meta(object_dir: Path, *, object_id: str) -> Dict[str, Any]:
    meta_path = _object_meta_path(object_dir, object_id=object_id)
    if not meta_path.exists():
        return {}
    return json.loads(meta_path.read_text(encoding="utf-8") or "{}")


def _touch_object_last_at(object_dir: Path, *, object_id: str, fallback_path: Optional[Path] = None) -> None:
    object_root = Path(object_dir)
    meta = _load_object_meta(object_root, object_id=object_id)
    now = utc_now()
    if meta:
        meta["object_id"] = normalize_object_id(object_id)
        meta["format"] = normalize_object_format(str(meta.get("format", "") or "bin"), default="bin")
        meta["size_bytes"] = max(0, int(meta.get("size_bytes", 0) or 0))
        created_at_raw = str(meta.get("created_at", "") or "").strip()
        if not created_at_raw:
            meta["created_at"] = now.astimezone(timezone.utc).isoformat()
        meta["last_at"] = now.astimezone(timezone.utc).isoformat()
        _atomic_write_json(_object_meta_path(object_root, object_id=object_id), meta)
        return
    candidate = Path(fallback_path) if fallback_path is not None else None
    if candidate is None or not candidate.exists():
        return
    _write_object_meta(
        object_root,
        object_id=object_id,
        fmt=normalize_object_format("", source_name=candidate.name, default="bin"),
        size_bytes=candidate.stat().st_size,
        created_at=datetime.fromtimestamp(candidate.stat().st_ctime, tz=timezone.utc),
        last_at=now,
        storage_backend="file",
    )


def touch_object_last_at(object_dir: Path, *, object_id: str, fallback_path: Optional[Path] = None) -> None:
    _touch_object_last_at(object_dir, object_id=object_id, fallback_path=fallback_path)


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


def _stored_result_to_result_ref(result: StoredResultArtifact, *, node_id: str) -> ResultRef:
    return ResultRef(
        object_id=result.object_id,
        node_id=node_id,
        format=result.format,
        size_bytes=result.size_bytes,
        materialize_as=result.materialize_as,
    )


def _materialize_object_bytes(*, blob: bytes, fmt: str, materialize_as: str) -> Any:
    materialized = normalize_materialize_as(materialize_as, default="path")
    normalized_format = normalize_object_format(fmt, default="bin")
    if materialized == "bytes":
        return bytes(blob)
    if materialized == "json":
        return convert_dict_to_arrow(json.loads(blob.decode("utf-8")))
    if materialized == "ndarray":
        try:
            import numpy as np
        except ImportError as exc:
            raise ObjectResolutionError("numpy not available, cannot materialize ndarray") from exc
        return np.load(io.BytesIO(blob), allow_pickle=False)
    if materialized == "dataframe":
        try:
            import pandas as pd
        except ImportError as exc:
            raise ObjectResolutionError("pandas not available, cannot materialize dataframe") from exc
        if normalized_format == "dfbundle":
            import zipfile
            from pycloud_parallel.controlplane.serialization import deserialize_dataframe_bundle

            with zipfile.ZipFile(io.BytesIO(blob)) as zf:
                with zf.open("data.parquet") as fh:
                    frame = pd.read_parquet(fh)
                with zf.open("meta.json") as fh:
                    meta = json.load(fh)
            return deserialize_dataframe_bundle(meta, frame)
        return pd.read_parquet(io.BytesIO(blob))
    if materialized == "series":
        try:
            import pandas as pd
        except ImportError as exc:
            raise ObjectResolutionError("pandas not available, cannot materialize series") from exc
        if normalized_format == "seriesbundle":
            import zipfile
            from pycloud_parallel.controlplane.serialization import deserialize_series_bundle

            with zipfile.ZipFile(io.BytesIO(blob)) as zf:
                with zf.open("data.parquet") as fh:
                    frame = pd.read_parquet(fh)
                with zf.open("meta.json") as fh:
                    meta = json.load(fh)
            return deserialize_series_bundle(meta, frame)
        frame = pd.read_parquet(io.BytesIO(blob))
        if frame.empty:
            return pd.Series(dtype=float)
        return frame.iloc[:, 0]
    raise ObjectResolutionError(f"blob-backed ObjectRef does not support materialize_as={materialized!r}")


def _sha256_file(path: Path, *, chunk_size: int = FILE_HASH_CHUNK_SIZE_BYTES) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(max(1, int(chunk_size)))
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _is_transient_filesystem_permission_error(exc: BaseException) -> bool:
    if isinstance(exc, PermissionError):
        return True
    if not isinstance(exc, OSError):
        return False
    return getattr(exc, "errno", None) in {1, 13, 16, 32, 33}


def _replace_file_with_retry(source_path: Path, final_path: Path, *, max_attempts: int = 8) -> None:
    last_exc: Optional[BaseException] = None
    for attempt in range(max(1, int(max_attempts))):
        try:
            if final_path.exists():
                source_path.unlink(missing_ok=True)
                return
            os.replace(str(source_path), str(final_path))
            return
        except FileNotFoundError:
            if final_path.exists() or not source_path.exists():
                return
            raise
        except Exception as exc:
            if final_path.exists():
                source_path.unlink(missing_ok=True)
                return
            if not _is_transient_filesystem_permission_error(exc):
                raise
            last_exc = exc
            time.sleep(min(0.5, 0.02 * float(attempt + 1)))
    if final_path.exists():
        source_path.unlink(missing_ok=True)
        return
    if last_exc is not None:
        raise last_exc


def _write_object_meta_with_retry(
    object_dir: Path,
    *,
    object_id: str,
    fmt: str,
    size_bytes: int,
    created_at: datetime,
    last_at: Optional[datetime] = None,
    storage_backend: str = "file",
    segment_relpath: str = "",
    segment_offset: int = 0,
    segment_length: int = 0,
    max_attempts: int = 8,
) -> None:
    last_exc: Optional[BaseException] = None
    for attempt in range(max(1, int(max_attempts))):
        try:
            _write_object_meta(
                object_dir,
                object_id=object_id,
                fmt=fmt,
                size_bytes=size_bytes,
                created_at=created_at,
                last_at=last_at,
                storage_backend=storage_backend,
                segment_relpath=segment_relpath,
                segment_offset=segment_offset,
                segment_length=segment_length,
            )
            return
        except Exception as exc:
            if not _is_transient_filesystem_permission_error(exc):
                raise
            last_exc = exc
            time.sleep(min(0.5, 0.02 * float(attempt + 1)))
    if last_exc is not None:
        raise last_exc


def _parse_meta_datetime(value: Any, *, default: Optional[datetime] = None) -> datetime:
    fallback = default or utc_now()
    text = str(value or "").strip()
    if not text:
        return fallback
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return fallback


def _artifact_exists(artifact: "ObjectArtifact") -> bool:
    if artifact.storage_backend == "segment":
        return bool(artifact.segment_path) and Path(artifact.segment_path).exists()
    return bool(artifact.path) and Path(artifact.path).exists()


def _code_artifact_exists(artifact: "CodeArtifact") -> bool:
    return bool(str(artifact.path or "").strip()) and Path(artifact.path).exists()


def _object_artifact_from_meta(object_dir: Path, *, object_id: str, meta: Dict[str, Any]) -> "ObjectArtifact":
    normalized_id = normalize_object_id(object_id)
    normalized_format = normalize_object_format(str(meta.get("format", "") or "bin"), default="bin")
    storage_backend = str(meta.get("storage_backend", "file") or "file").strip() or "file"
    created_at = _parse_meta_datetime(meta.get("created_at"))
    size_bytes = max(0, int(meta.get("size_bytes", 0) or 0))
    if storage_backend == "segment":
        relpath = str(meta.get("segment_relpath", "") or "").strip()
        return ObjectArtifact(
            object_id=normalized_id,
            path="",
            format=normalized_format,
            size_bytes=size_bytes,
            created_at=created_at,
            storage_backend="segment",
            segment_path=str(_segment_path_from_relpath(object_dir, relpath)),
            segment_offset=max(0, int(meta.get("segment_offset", 0) or 0)),
            segment_length=max(0, int(meta.get("segment_length", size_bytes) or size_bytes)),
        )
    return ObjectArtifact(
        object_id=normalized_id,
        path=str(object_storage_path(object_dir, object_id=normalized_id, fmt=normalized_format)),
        format=normalized_format,
        size_bytes=size_bytes,
        created_at=created_at,
        storage_backend="file",
    )


def _read_object_artifact_bytes(artifact: "ObjectArtifact") -> bytes:
    if artifact.storage_backend == "segment":
        with open(artifact.segment_path, "rb") as fp:
            fp.seek(max(0, int(artifact.segment_offset or 0)))
            return fp.read(max(0, int(artifact.segment_length or artifact.size_bytes or 0)))
    return Path(artifact.path).read_bytes()


def _materialized_object_path(root: Path, *, object_id: str, fmt: str) -> Path:
    return object_storage_path(_materialized_objects_dir(root), object_id=object_id, fmt=fmt)


def _materialize_object_artifact(
    artifact: "ObjectArtifact",
    *,
    materialize_as: str,
    root: Path,
) -> Any:
    if artifact.storage_backend == "file":
        candidate = Path(artifact.path)
        if materialize_as == "path":
            return candidate
        if materialize_as == "bytes":
            return candidate.read_bytes()
        if materialize_as == "json":
            return convert_dict_to_arrow(json.loads(candidate.read_text(encoding="utf-8")))
        if materialize_as == "ndarray":
            try:
                import numpy as np
            except ImportError as exc:
                raise ObjectResolutionError("numpy not available on node, cannot materialize ndarray") from exc
            return np.load(candidate, allow_pickle=False)
        if materialize_as == "dataframe":
            try:
                import pandas as pd
            except ImportError as exc:
                raise ObjectResolutionError("pandas not available on node, cannot materialize dataframe") from exc
            if str(artifact.format or "").strip().lower() == "dfbundle":
                import zipfile
                from pycloud_parallel.controlplane.serialization import deserialize_dataframe_bundle

                with zipfile.ZipFile(candidate) as zf:
                    if {"data.parquet", "meta.json"}.issubset(set(zf.namelist())):
                        with zf.open("data.parquet") as fh:
                            frame = pd.read_parquet(fh)
                        with zf.open("meta.json") as fh:
                            meta = json.load(fh)
                        return deserialize_dataframe_bundle(meta, frame)
            return pd.read_parquet(candidate)
        if materialize_as == "series":
            try:
                import pandas as pd
            except ImportError as exc:
                raise ObjectResolutionError("pandas not available on node, cannot materialize series") from exc
            if str(artifact.format or "").strip().lower() == "seriesbundle":
                import zipfile
                from pycloud_parallel.controlplane.serialization import deserialize_series_bundle

                with zipfile.ZipFile(candidate) as zf:
                    if {"data.parquet", "meta.json"}.issubset(set(zf.namelist())):
                        with zf.open("data.parquet") as fh:
                            frame = pd.read_parquet(fh)
                        with zf.open("meta.json") as fh:
                            meta = json.load(fh)
                        if len(frame.columns) != 1:
                            raise ObjectResolutionError("series bundle parquet must contain exactly one column")
                        return deserialize_series_bundle(meta, frame.iloc[:, 0])
            frame = pd.read_parquet(candidate)
            if len(frame.columns) != 1:
                raise ObjectResolutionError("series parquet must contain exactly one column")
            return frame.iloc[:, 0]
        raise ObjectResolutionError(f"unsupported materialize_as: {materialize_as!r}")

    blob = _read_object_artifact_bytes(artifact)
    if materialize_as == "path":
        candidate = _materialized_object_path(root, object_id=artifact.object_id, fmt=artifact.format)
        candidate.parent.mkdir(parents=True, exist_ok=True)
        if not candidate.exists():
            candidate.write_bytes(blob)
        return candidate
    return _materialize_object_bytes(blob=blob, fmt=artifact.format, materialize_as=materialize_as)


def _append_bytes_to_segment(
    object_dir: Path,
    *,
    object_id: str,
    fmt: str,
    blob: bytes,
    materialize_as: str,
    created_at: Optional[datetime] = None,
) -> StoredResultArtifact:
    root = Path(object_dir).resolve()
    segments_root = _segments_dir(root)
    segments_root.mkdir(parents=True, exist_ok=True)
    lock = _segment_writer_lock(root)
    with lock:
        key = _segment_writer_key(root)
        current_segment = Path(_SEGMENT_WRITER_STATE.get(key, "")).resolve() if _SEGMENT_WRITER_STATE.get(key) else None
        if (
            current_segment is None
            or not current_segment.exists()
            or (current_segment.stat().st_size + len(blob)) > max(1, int(OBJECT_SEGMENT_TARGET_BYTES))
        ):
            current_segment = segments_root / f"segment-{os.getpid()}-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}.bin"
            current_segment.touch()
            _SEGMENT_WRITER_STATE[key] = str(current_segment)
        with current_segment.open("ab") as fp:
            offset = fp.tell()
            fp.write(blob)
        relpath = _segment_relpath(root, current_segment)
    current_time = created_at or utc_now()
    _write_object_meta_with_retry(
        root,
        object_id=object_id,
        fmt=fmt,
        size_bytes=len(blob),
        created_at=current_time,
        last_at=current_time,
        storage_backend="segment",
        segment_relpath=relpath,
        segment_offset=offset,
        segment_length=len(blob),
    )
    return StoredResultArtifact(
        object_id=object_id,
        format=normalize_object_format(fmt, default="bin"),
        size_bytes=len(blob),
        materialize_as=normalize_materialize_as(materialize_as, default="path"),
        storage_backend="segment",
        segment_relpath=relpath,
        segment_offset=offset,
        segment_length=len(blob),
    )


def _commit_result_file(source_path: Path, *, object_dir: str, fmt: str, size_bytes: int, materialize_as: str) -> StoredResultArtifact:
    root = Path(str(object_dir or "")).resolve()
    root.mkdir(parents=True, exist_ok=True)
    digest = _sha256_file(source_path)
    object_id = object_id_from_sha256_hex(digest)
    normalized_format = normalize_object_format(fmt, source_name=source_path.name, default="bin")
    final_path = object_storage_path(root, object_id=object_id, fmt=normalized_format)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    _replace_file_with_retry(source_path, final_path)
    created_at = utc_now()
    _write_object_meta_with_retry(
        root,
        object_id=object_id,
        fmt=normalized_format,
        size_bytes=max(0, int(size_bytes or final_path.stat().st_size)),
        created_at=created_at,
        last_at=created_at,
    )
    return StoredResultArtifact(
        object_id=object_id,
        format=normalized_format,
        size_bytes=max(0, int(size_bytes or final_path.stat().st_size)),
        materialize_as=normalize_materialize_as(materialize_as, default="path"),
        storage_backend="file",
    )


def _commit_result_segment(blob: bytes, *, object_dir: str, fmt: str, materialize_as: str) -> StoredResultArtifact:
    digest = hashlib.sha256(blob).hexdigest()
    object_id = object_id_from_sha256_hex(digest)
    return _append_bytes_to_segment(
        Path(object_dir).resolve(),
        object_id=object_id,
        fmt=fmt,
        blob=blob,
        materialize_as=materialize_as,
    )


def _store_result_path(path: Path, *, object_dir: str) -> StoredResultArtifact:
    if not path.exists() or not path.is_file():
        raise LargeResultError(f"returned path is not a readable file: {path}")
    if path.stat().st_size <= max(0, int(OBJECT_SEGMENT_MAX_BYTES)):
        return _commit_result_segment(
            path.read_bytes(),
            object_dir=object_dir,
            fmt=normalize_object_format("", source_name=path.name, default="bin"),
            materialize_as="path",
        )
    suffix = object_format_suffix(normalize_object_format("", source_name=path.name, default="bin")) or ".bin"
    fd, tmp_name = tempfile.mkstemp(prefix="pycloud-result-", suffix=suffix, dir=str(Path(object_dir).resolve()))
    os.close(fd)
    tmp_path = Path(tmp_name)
    shutil.copyfile(str(path), str(tmp_path))
    return _commit_result_file(
        tmp_path,
        object_dir=object_dir,
        fmt=normalize_object_format("", source_name=path.name, default="bin"),
        size_bytes=path.stat().st_size,
        materialize_as="path",
    )


def _store_result_dataframe(frame: Any, *, object_dir: str) -> StoredResultArtifact:
    try:
        import io
        import zipfile

        parquet_buf = io.BytesIO()
        dataframe_bundle_parquet_frame(frame).to_parquet(parquet_buf, index=False)
        meta = serialize_dataframe_bundle(frame)
        bundle = io.BytesIO()
        with zipfile.ZipFile(bundle, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("data.parquet", parquet_buf.getvalue())
            zf.writestr("meta.json", json.dumps(meta, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        blob = bundle.getvalue()
        if len(blob) <= max(0, int(OBJECT_SEGMENT_MAX_BYTES)):
            return _commit_result_segment(blob, object_dir=object_dir, fmt="dfbundle", materialize_as="dataframe")
        fd, tmp_name = tempfile.mkstemp(prefix="pycloud-result-", suffix=".zip", dir=str(Path(object_dir).resolve()))
        os.close(fd)
        tmp_path = Path(tmp_name)
        tmp_path.write_bytes(blob)
        return _commit_result_file(
            tmp_path,
            object_dir=object_dir,
            fmt="dfbundle",
            size_bytes=tmp_path.stat().st_size,
            materialize_as="dataframe",
        )
    except Exception:
        if "tmp_path" in locals():
            tmp_path.unlink(missing_ok=True)
        raise


def _store_result_series(series: Any, *, object_dir: str) -> StoredResultArtifact:
    try:
        import io
        import zipfile

        parquet_buf = io.BytesIO()
        series.to_frame("__pycloud_series_value__").to_parquet(parquet_buf, index=False)
        meta = serialize_series_bundle(series)
        bundle = io.BytesIO()
        with zipfile.ZipFile(bundle, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("data.parquet", parquet_buf.getvalue())
            zf.writestr("meta.json", json.dumps(meta, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        blob = bundle.getvalue()
        if len(blob) <= max(0, int(OBJECT_SEGMENT_MAX_BYTES)):
            return _commit_result_segment(blob, object_dir=object_dir, fmt="seriesbundle", materialize_as="series")
        fd, tmp_name = tempfile.mkstemp(prefix="pycloud-result-", suffix=".zip", dir=str(Path(object_dir).resolve()))
        os.close(fd)
        tmp_path = Path(tmp_name)
        tmp_path.write_bytes(blob)
        return _commit_result_file(
            tmp_path,
            object_dir=object_dir,
            fmt="seriesbundle",
            size_bytes=tmp_path.stat().st_size,
            materialize_as="series",
        )
    except Exception:
        if "tmp_path" in locals():
            tmp_path.unlink(missing_ok=True)
        raise


def _store_result_ndarray(array: Any, *, object_dir: str) -> StoredResultArtifact:
    try:
        import numpy as np

        buf = io.BytesIO()
        np.save(buf, array, allow_pickle=False)
        blob = buf.getvalue()
        if len(blob) <= max(0, int(OBJECT_SEGMENT_MAX_BYTES)):
            return _commit_result_segment(blob, object_dir=object_dir, fmt="npy", materialize_as="ndarray")
        fd, tmp_name = tempfile.mkstemp(prefix="pycloud-result-", suffix=".npy", dir=str(Path(object_dir).resolve()))
        os.close(fd)
        tmp_path = Path(tmp_name)
        tmp_path.write_bytes(blob)
        return _commit_result_file(
            tmp_path,
            object_dir=object_dir,
            fmt="npy",
            size_bytes=tmp_path.stat().st_size,
            materialize_as="ndarray",
        )
    except Exception:
        if "tmp_path" in locals():
            tmp_path.unlink(missing_ok=True)
        raise


def _normalize_result_value(ret: Any, *, object_dir: str) -> Any:
    def _try_inline_result(value: Any) -> Tuple[bool, Any]:
        serialized = serialize_arrow_compatible(value)
        wrapped = serialized if isinstance(serialized, dict) else {"value": serialized}
        try:
            serialize_inline_result(wrapped, context="task result")
        except ValueError:
            return False, None
        log_payload_flow("inline_result_ready", context="task result", summary=summarize_payload_flow_value(value))
        return True, wrapped

    if isinstance(ret, Path):
        log_payload_flow("result_ref_store", path_type="path", summary=summarize_payload_flow_value(ret))
        return _store_result_path(ret, object_dir=object_dir)

    try:
        import pandas as pd

        if isinstance(ret, pd.DataFrame):
            inlined, wrapped = _try_inline_result(ret)
            if inlined:
                return wrapped
            log_payload_flow("result_ref_store", path_type="dataframe", summary=summarize_payload_flow_value(ret))
            return _store_result_dataframe(ret, object_dir=object_dir)
        if isinstance(ret, pd.Series):
            inlined, wrapped = _try_inline_result(ret)
            if inlined:
                return wrapped
            log_payload_flow("result_ref_store", path_type="series", summary=summarize_payload_flow_value(ret))
            return _store_result_series(ret, object_dir=object_dir)
    except ImportError:
        pass

    try:
        import numpy as np

        if isinstance(ret, np.ndarray):
            inlined, wrapped = _try_inline_result(ret)
            if inlined:
                return wrapped
            log_payload_flow("result_ref_store", path_type="ndarray", summary=summarize_payload_flow_value(ret))
            return _store_result_ndarray(ret, object_dir=object_dir)
    except ImportError:
        pass

    inlined, wrapped = _try_inline_result(ret)
    if inlined:
        return wrapped
    raise LargeResultError("task result exceeds inline limit and must be returned as Path/DataFrame/Series/ndarray for ResultRef storage")


def _normalize_user_return(ret: Any, *, object_dir: str) -> Tuple[str, Optional[Any], str, str]:
    def _normalize_status(v: Any) -> str:
        s = str(v or "SUCCEEDED").strip().upper()
        if s in ("SUCCESS", "OK"):
            return "SUCCEEDED"
        if s not in ("SUCCEEDED", "FAILED_USER", "FAILED_INFRA"):
            return "SUCCEEDED"
        return s

    if isinstance(ret, tuple) and len(ret) == 4:
        status_text, result, err_type, err_message = ret
        result = _normalize_result_value(result, object_dir=object_dir) if result is not None else None
        return _normalize_status(status_text), result, str(err_type), str(err_message)

    if isinstance(ret, dict) and "status" in ret:
        status_text = _normalize_status(ret.get("status", "SUCCEEDED"))
        result = ret.get("result")
        err_type = str(ret.get("error_type", ""))
        err_message = str(ret.get("error_message", ""))
        result = _normalize_result_value(result, object_dir=object_dir) if result is not None else None
        return status_text, result, err_type, err_message

    return "SUCCEEDED", _normalize_result_value(ret, object_dir=object_dir), "", ""


def _build_execute_spec(
    artifact: "CodeArtifact",
    *,
    object_dir: Path,
    work_dir: Optional[Path] = None,
    method_name: str,
    payload: dict,
    managed_globals_scope_dir: str = "",
    managed_globals_digest: str = "",
    warmup_only: bool = False,
) -> Dict[str, Any]:
    return {
        "artifact_path": artifact.path,
        "entry_module": artifact.entry_module,
        "package_format": artifact.package_format,
        "dependency_path": artifact.dependency_path,
        "object_dir": str(object_dir),
        "work_dir": str(work_dir or ""),
        "export_mode": artifact.export_mode,
        "export_methods": list(artifact.export_methods),
        "export_decorator": artifact.export_decorator,
        "method_name": method_name,
        "entry_callable": artifact.entry_callable,
        "payload": payload or {},
        "managed_globals_scope_dir": str(managed_globals_scope_dir or ""),
        "managed_globals_digest": str(managed_globals_digest or ""),
        "warmup_only": bool(warmup_only),
    }


def _serialize_result_for_json(obj: Any) -> Any:
    """序列化返回值中的 Arrow 对象和 numpy 类型，使其可被 JSON 序列化。"""
    return serialize_arrow_compatible(obj)


_ROUTER_CACHE_LOCK = threading.Lock()
_ROUTER_CACHE: Dict[str, Tuple[Dict[str, Any], Dict[str, Tuple[str, str]]]] = {}


def _artifact_module_name(artifact_path: str) -> str:
    return f"_pycloud_user_{hashlib.sha1(artifact_path.encode('utf-8')).hexdigest()}"


def _normalize_package_format(package_format: str, artifact_path: str = "") -> str:
    raw = str(package_format or "").strip().lower().replace("_", "").replace(".", "")
    if raw in ("py", "python"):
        return "py"
    if raw in ("targz", "tgz", "tar"):
        return "tar.gz"
    if raw == "zip":
        return "zip"
    if raw == "whl":
        return "whl"

    lower_name = str(artifact_path or "").strip().lower()
    if lower_name.endswith(".tar.gz") or lower_name.endswith(".tgz"):
        return "tar.gz"
    if lower_name.endswith(".zip"):
        return "zip"
    if lower_name.endswith(".whl"):
        return "whl"
    if lower_name.endswith(".py"):
        return "py"
    return "bin"


def _package_suffix(package_format: str) -> str:
    normalized = _normalize_package_format(package_format)
    if normalized == "tar.gz":
        return ".tar.gz"
    if normalized == "zip":
        return ".zip"
    if normalized == "whl":
        return ".whl"
    if normalized == "py":
        return ".py"
    return ".bin"


def _normalize_export_spec(
    *,
    mode: str,
    methods: Sequence[str],
    decorator: str,
    entry_callable: str,
) -> Tuple[str, Tuple[str, ...], str]:
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode not in ("decorator", "explicit", "all", "single"):
        normalized_mode = ""

    normalized_methods = tuple(sorted({x.strip() for x in methods if str(x).strip()}))
    normalized_decorator = _DEFAULT_EXPORT_DECORATOR
    fallback_callable = str(entry_callable or "").strip() or "run"

    if not normalized_mode:
        if normalized_methods:
            normalized_mode = "explicit"
        elif fallback_callable:
            normalized_mode = "single"
        else:
            normalized_mode = "decorator"

    if normalized_mode == "single":
        normalized_methods = (fallback_callable,)
    return normalized_mode, normalized_methods, normalized_decorator


def _validate_python_runtime_or_raise(*, node_python_version: str, runtime: str) -> str:
    normalized_runtime = normalize_python_runtime_spec(runtime)
    if not normalized_runtime:
        return ""
    if not matches_python_runtime(node_python_version, normalized_runtime):
        raise ValueError(
            f"runtime {normalized_runtime} is incompatible with node python_version {node_python_version}"
        )
    return normalized_runtime


def _is_user_artifact_error(exc: BaseException) -> bool:
    user_error_types = (
        SyntaxError,
        ImportError,
        ModuleNotFoundError,
        AttributeError,
        NameError,
        TypeError,
        ValueError,
    )
    runtime_error_markers = (
        "cannot load python module",
        "entry_module is required",
        "not found",
        "not callable",
        "no exported methods found",
        "duplicate exported method",
        "exported method cannot start with",
    )

    seen: set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, user_error_types):
            return True
        if isinstance(current, RuntimeError):
            message = str(current)
            if any(marker in message for marker in runtime_error_markers):
                return True
        current = current.__cause__ or current.__context__
    return False


def _describe_artifact_error(
    exc: BaseException,
    *,
    entry_module: str,
    entry_callable: str,
    package_format: str,
) -> str:
    if isinstance(exc, SyntaxError):
        line = int(exc.lineno or 0)
        filename = str(exc.filename or entry_module or "<artifact>")
        if line > 0:
            detail = f"SyntaxError at {filename}:{line}: {exc.msg}"
        else:
            detail = f"SyntaxError at {filename}: {exc.msg}"
    else:
        message = str(exc) or repr(exc)
        detail = f"{exc.__class__.__name__}: {message}"
    normalized_module = str(entry_module or "").strip() or "<auto>"
    normalized_callable = str(entry_callable or "").strip() or "run"
    normalized_format = _normalize_package_format(package_format, package_format or "artifact.py")
    missing_import = _missing_import_name(exc)
    repair_hint = (
        f" Missing dependency `{missing_import}` detected; retry with dependency_allowlist if node-side install is allowed."
        if missing_import
        else ""
    )
    return (
        "artifact validation failed while loading "
        f"(entry_module={normalized_module}, entry_callable={normalized_callable}, package_format={normalized_format}): "
        f"{detail}{repair_hint}"
    )


def _describe_user_execution_error(exc: BaseException) -> str:
    message = str(exc) or repr(exc)
    detail = f"{exc.__class__.__name__}: {message}"
    missing_import = _missing_import_name(exc)
    repair_hint = (
        f" Missing dependency `{missing_import}` detected during execution; "
        "retry with dependency_allowlist if node-side install is allowed."
        if missing_import
        else ""
    )
    return f"user code execution failed: {detail}{repair_hint}"


def _normalize_dependency_allowlist(requirements: Sequence[str]) -> Tuple[str, ...]:
    normalized: List[str] = []
    seen: set[str] = set()
    for item in requirements or ():
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return tuple(normalized)


def _missing_import_name(exc: BaseException) -> str:
    current: Optional[BaseException] = exc
    while current is not None:
        if isinstance(current, ModuleNotFoundError):
            name = str(getattr(current, "name", "") or "").strip()
            if name:
                return name
        current = current.__cause__ or current.__context__
    return ""


@contextmanager
def _temporary_import_paths(*paths: str):
    inserted: List[str] = []
    for raw in paths:
        path = str(raw or "").strip()
        if not path:
            continue
        sys.path.insert(0, path)
        inserted.append(path)
    try:
        yield
    finally:
        for path in reversed(inserted):
            try:
                sys.path.remove(path)
            except ValueError:
                pass


@contextmanager
def _temporary_working_dir(path: str):
    target = str(path or "").strip()
    if not target:
        yield
        return
    target_path = Path(target)
    target_path.mkdir(parents=True, exist_ok=True)
    previous = Path.cwd()
    os.chdir(target_path)
    try:
        yield
    finally:
        os.chdir(previous)


def _install_dependency_allowlist(requirements: Sequence[str], *, target_dir: Path) -> None:
    normalized = _normalize_dependency_allowlist(requirements)
    if not normalized:
        return
    target_dir = Path(target_dir)
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    staging_dir = target_dir.with_name(f"{target_dir.name}.tmp-{uuid.uuid4().hex}")
    shutil.rmtree(staging_dir, ignore_errors=True)
    staging_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--upgrade",
        "--no-input",
        "--disable-pip-version-check",
        "--target",
        str(staging_dir),
        *normalized,
    ]
    completed = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        shutil.rmtree(staging_dir, ignore_errors=True)
        stderr = str(completed.stderr or "").strip()
        stdout = str(completed.stdout or "").strip()
        detail = stderr or stdout or f"pip exited with code {completed.returncode}"
        raise RuntimeError(f"dependency install failed for {list(normalized)}: {detail}")
    backup_dir = target_dir.with_name(f"{target_dir.name}.bak-{uuid.uuid4().hex}")
    try:
        if target_dir.exists():
            os.replace(str(target_dir), str(backup_dir))
        os.replace(str(staging_dir), str(target_dir))
    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        if backup_dir.exists() and not target_dir.exists():
            os.replace(str(backup_dir), str(target_dir))
        raise
    finally:
        shutil.rmtree(backup_dir, ignore_errors=True)


def _purge_module_tree(module_name: str) -> None:
    if not module_name:
        return
    to_delete = [k for k in list(sys.modules.keys()) if k == module_name or k.startswith(f"{module_name}.")]
    for key in to_delete:
        sys.modules.pop(key, None)


def _load_user_module(
    artifact_path: str,
    *,
    entry_module: str,
    package_format: str,
    dependency_path: str = "",
):
    path = Path(artifact_path)
    format_name = _normalize_package_format(package_format, path.name)

    if format_name == "py" and path.is_file() and path.suffix.lower() == ".py":
        module_name = _artifact_module_name(artifact_path)
        loaded = sys.modules.get(module_name)
        if loaded is not None:
            return loaded
        spec = importlib.util.spec_from_file_location(module_name, artifact_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"cannot load python module from {artifact_path}")
        module = importlib.util.module_from_spec(spec)
        with _temporary_import_paths(dependency_path):
            spec.loader.exec_module(module)
            sys.modules[module_name] = module
        return module

    if not entry_module:
        raise RuntimeError("entry_module is required for package artifacts")

    importlib.invalidate_caches()
    # 清理父包缓存，避免重复部署同名包时命中旧 __path__。
    root_module = entry_module.split(".", 1)[0].strip()
    if root_module:
        _purge_module_tree(root_module)
    _purge_module_tree(entry_module)
    with _temporary_import_paths(dependency_path, artifact_path):
        return importlib.import_module(entry_module)


def _purge_loaded_artifact_modules(
    artifact_path: str,
    *,
    entry_module: str,
    package_format: str,
    dependency_path: str = "",
) -> None:
    format_name = _normalize_package_format(package_format, Path(artifact_path).name)
    if format_name == "py":
        _purge_module_tree(_artifact_module_name(artifact_path))
    else:
        root_module = str(entry_module or "").split(".", 1)[0].strip()
        if root_module:
            _purge_module_tree(root_module)
        _purge_module_tree(str(entry_module or "").strip())

    prefixes = [str(Path(artifact_path).resolve())]
    if dependency_path:
        prefixes.append(str(Path(dependency_path).resolve()))

    for name, module in list(sys.modules.items()):
        module_file = getattr(module, "__file__", None)
        if module_file:
            resolved_file = str(Path(module_file).resolve())
            if any(resolved_file.startswith(prefix) for prefix in prefixes):
                sys.modules.pop(name, None)
                continue
        module_paths = getattr(module, "__path__", None)
        if module_paths:
            resolved_paths = [str(Path(p).resolve()) for p in module_paths]
            if any(any(path.startswith(prefix) for prefix in prefixes) for path in resolved_paths):
                sys.modules.pop(name, None)


def _build_callable_router(
    module,
    *,
    mode: str,
    methods: Sequence[str],
    decorator: str,
    entry_callable: str,
) -> Tuple[Dict[str, Any], Dict[str, Tuple[str, str]]]:
    marker = _DEFAULT_EXPORT_DECORATOR
    marker_candidates = {
        marker,
        f"__{marker}__",
        _DEFAULT_EXPORT_DECORATOR,
        f"__{_DEFAULT_EXPORT_DECORATOR}__",
    }
    exported_declared = set()
    declared = getattr(module, "__pycloud_exports__", None)
    if isinstance(declared, (list, tuple, set)):
        exported_declared = {str(x).strip() for x in declared if str(x).strip()}

    all_callables: Dict[str, Any] = {}
    for name in dir(module):
        if name.startswith("_"):
            continue
        value = getattr(module, name, None)
        if callable(value):
            all_callables[name] = value

    router: Dict[str, Any] = {}
    method_info: Dict[str, Tuple[str, str]] = {}

    def _register(method_name: str, fn: Any) -> None:
        normalized_method = str(method_name or "").strip()
        if not normalized_method:
            return
        if normalized_method.startswith("_"):
            raise RuntimeError(f"exported method cannot start with _: {normalized_method}")
        if normalized_method in router:
            raise RuntimeError(f"duplicate exported method: {normalized_method}")
        router[normalized_method] = fn
        method_info[normalized_method] = (str(getattr(fn, "__qualname__", normalized_method)), inspect.getdoc(fn) or "")

    if mode == "all":
        for name, fn in all_callables.items():
            _register(name, fn)
    elif mode == "explicit":
        for name in methods:
            fn = getattr(module, name, None)
            if fn is None or not callable(fn):
                raise RuntimeError(f"explicit exported method `{name}` not found or not callable")
            _register(name, fn)
    elif mode == "single":
        only = (list(methods)[:1] or [str(entry_callable or "run").strip() or "run"])[0]
        fn = getattr(module, only, None)
        if fn is None or not callable(fn):
            raise RuntimeError(f"callable `{only}` not found in uploaded artifact")
        _register(only, fn)
    else:  # decorator
        for name, fn in all_callables.items():
            if name in exported_declared:
                exported_name = str(getattr(fn, "__pycloud_export_name__", "") or name).strip()
                _register(exported_name, fn)
                continue
            if any(bool(getattr(fn, attr, False)) for attr in marker_candidates):
                exported_name = str(getattr(fn, "__pycloud_export_name__", "") or name).strip()
                _register(exported_name, fn)
        if not router:
            legacy_name = str(entry_callable or "").strip()
            if legacy_name:
                legacy_fn = getattr(module, legacy_name, None)
                if legacy_fn is not None and callable(legacy_fn):
                    _register(legacy_name, legacy_fn)

    if not router:
        raise RuntimeError("no exported methods found; use decorator/explicit export rules")
    return router, method_info


def _load_callable_router(
    artifact_path: str,
    *,
    entry_module: str,
    package_format: str,
    dependency_path: str,
    export_mode: str,
    export_methods: Sequence[str],
    export_decorator: str,
    entry_callable: str,
) -> Tuple[Dict[str, Any], Dict[str, Tuple[str, str]]]:
    mode, methods, decorator = _normalize_export_spec(
        mode=export_mode,
        methods=export_methods,
        decorator=export_decorator,
        entry_callable=entry_callable,
    )
    key = "|".join(
        (
            artifact_path,
            entry_module,
            package_format,
            dependency_path,
            mode,
            ",".join(methods),
            decorator,
            entry_callable or "",
        )
    )
    with _ROUTER_CACHE_LOCK:
        cached = _ROUTER_CACHE.get(key)
        if cached is not None:
            return cached

    module = _load_user_module(
        artifact_path,
        entry_module=entry_module,
        package_format=package_format,
        dependency_path=dependency_path,
    )
    loaded = _build_callable_router(
        module,
        mode=mode,
        methods=methods,
        decorator=decorator,
        entry_callable=entry_callable,
    )
    with _ROUTER_CACHE_LOCK:
        _ROUTER_CACHE[key] = loaded
    return loaded


def _discover_callable_methods(
    artifact_path: str,
    *,
    entry_module: str,
    package_format: str,
    dependency_path: str,
    export_mode: str,
    export_methods: Sequence[str],
    export_decorator: str,
    entry_callable: str,
) -> Tuple[Any, Dict[str, Tuple[str, str]]]:
    mode, methods, decorator = _normalize_export_spec(
        mode=export_mode,
        methods=export_methods,
        decorator=export_decorator,
        entry_callable=entry_callable,
    )
    module = _load_user_module(
        artifact_path,
        entry_module=entry_module,
        package_format=package_format,
        dependency_path=dependency_path,
    )
    try:
        _router, method_info = _build_callable_router(
            module,
            mode=mode,
            methods=methods,
            decorator=decorator,
            entry_callable=entry_callable,
        )
        return module, dict(method_info)
    finally:
        pass


def _discover_callable_methods_or_raise_user_error(
    artifact_path: str,
    *,
    entry_module: str,
    package_format: str,
    dependency_path: str,
    export_mode: str,
    export_methods: Sequence[str],
    export_decorator: str,
    entry_callable: str,
) -> Tuple[Any, Dict[str, Tuple[str, str]]]:
    try:
        return _discover_callable_methods(
            artifact_path,
            entry_module=entry_module,
            package_format=package_format,
            dependency_path=dependency_path,
            export_mode=export_mode,
            export_methods=export_methods,
            export_decorator=export_decorator,
            entry_callable=entry_callable,
        )
    except Exception as exc:
        if _is_user_artifact_error(exc):
            raise ValueError(
                _describe_artifact_error(
                    exc,
                    entry_module=entry_module,
                    entry_callable=entry_callable,
                    package_format=package_format,
                )
            ) from exc
        raise


def _validate_arrow_compatible(obj: Any) -> None:
    """验证对象是否 Arrow 兼容，如果不兼容则抛出错误。

    Args:
        obj: 要验证的对象

    Raises:
        TypeError: 如果对象不 Arrow 兼容
    """
    # 基本类型
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return

    # 容器类型
    if isinstance(obj, (list, tuple)):
        for item in obj:
            _validate_arrow_compatible(item)
        return

    if isinstance(obj, dict):
        for key, value in obj.items():
            _validate_arrow_compatible(key)
            _validate_arrow_compatible(value)
        return

    # Arrow 兼容类型
    if is_arrow_compatible(obj):
        return

    # 不支持的类型
    raise TypeError(
        f"Type {type(obj).__name__} is not supported in PyCloud. "
        f"Supported types: basic types (str, int, float, bool, None), "
        f"list, tuple, dict, pd.DataFrame, pd.Series, np.ndarray (basic dtypes only). "
        f"For complex objects, please convert to JSON or use external storage."
    )


def _invoke_user_callable(fn, payload: dict):
    """调用用户函数，支持多种参数传递方式。

    支持的 payload 格式：
    1. {"args": [...], "kwargs": {...}} - 新格式，支持位置参数和命名参数
    2. {"args": [...]} - 只有位置参数，kwargs 为空
    3. {"kwargs": {...}} - 只有命名参数，args 为空
    4. {"key": value, ...} - HTTP 风格，直接作为 kwargs

    Arrow 兼容类型自动转换：
    - DataFrame → 保留 index/columns 的结构化数据
    - Series → 保留 index 的结构化数据
    - ndarray → list
    """
    try:
        signature = inspect.signature(fn)
        params = list(signature.parameters.values())
    except Exception:
        params = []

    if not params:
        return fn()

    # 检查是否是新的 args/kwargs 格式
    # 判断标准：payload 只包含 args 和 kwargs 键（不包含其他键）
    if isinstance(payload, dict) and ("args" in payload or "kwargs" in payload):
        # 检查是否还有其他键（除了 args 和 kwargs）
        other_keys = set(payload.keys()) - {"args", "kwargs"}
        if not other_keys:
            # 纯净的 args/kwargs 格式
            args = payload.get("args", [])
            kwargs = payload.get("kwargs", {})

            # 验证 Arrow 兼容性
            _validate_arrow_compatible(args)
            _validate_arrow_compatible(kwargs)

            # 反序列化 Arrow 对象
            args = convert_dict_to_arrow(args)
            kwargs = convert_dict_to_arrow(kwargs)

            # 确保 args 是列表类型
            if not isinstance(args, list):
                args = list(args) if args else []
            # 确保 kwargs 是字典类型
            if not isinstance(kwargs, dict):
                kwargs = {}
            log_payload_flow(
                "user_invoke",
                mode="args_kwargs",
                args_summary=summarize_payload_flow_value(args),
                kwargs_summary=summarize_payload_flow_value(kwargs),
            )
            return fn(*args, **kwargs)

    # HTTP 风格：整个 payload 作为 kwargs
    # 这样服务端可以用 def square(**payload) 或 def square(x) 接收
    if isinstance(payload, dict):
        # 反序列化 Arrow 对象
        deserialized = convert_dict_to_arrow(payload)
        log_payload_flow(
            "user_invoke",
            mode="http_kwargs",
            kwargs_summary=summarize_payload_flow_value(deserialized),
        )
        return fn(**deserialized)

    # 其他情况：直接传递 payload
    log_payload_flow(
        "user_invoke",
        mode="direct_payload",
        payload_summary=summarize_payload_flow_value(payload),
    )
    return fn(payload)


class ObjectResolutionError(RuntimeError):
    """Raised when an ObjectRef cannot be materialized on the node."""


def _resolve_object_refs_in_payload(payload: Any, *, object_dir: str) -> Any:
    root = Path(str(object_dir or "")).resolve()

    def _resolve(value: Any) -> Any:
        if isinstance(value, ObjectRef):
            materialized = normalize_materialize_as(value.materialize_as, default="path")
            log_payload_flow(
                "object_ref_resolve",
                materialize_as=materialized,
                summary=summarize_payload_flow_value(value),
            )
            meta = _load_object_meta(root, object_id=value.object_id)
            artifact: Optional[ObjectArtifact] = None
            if meta:
                artifact = _object_artifact_from_meta(root, object_id=value.object_id, meta=meta)
                if not _artifact_exists(artifact):
                    artifact = None
            if artifact is None:
                candidate = object_storage_path(root, object_id=value.object_id, fmt=value.format)
                if candidate.exists():
                    artifact = ObjectArtifact(
                        object_id=normalize_object_id(value.object_id),
                        path=str(candidate),
                        format=normalize_object_format(value.format, source_name=candidate.name, default="bin"),
                        size_bytes=candidate.stat().st_size,
                        created_at=utc_now(),
                        storage_backend="file",
                    )
                else:
                    digest = normalize_object_id(value.object_id).replace("sha256:", "", 1)
                    suffix = object_format_suffix(value.format)
                    legacy_candidate = Path(root) / f"{digest}{suffix}"
                    fallback = []
                    if legacy_candidate.exists():
                        fallback = [legacy_candidate]
                    if not fallback:
                        subdir = Path(root) / digest[:2]
                        fallback = sorted(subdir.glob(f"{digest[2:]}*")) if subdir.exists() else []
                    if not fallback:
                        fallback = sorted(root.glob(f"{digest}*"))
                    if fallback:
                        artifact = ObjectArtifact(
                            object_id=normalize_object_id(value.object_id),
                            path=str(fallback[0]),
                            format=normalize_object_format("", source_name=fallback[0].name, default="bin"),
                            size_bytes=fallback[0].stat().st_size,
                            created_at=utc_now(),
                            storage_backend="file",
                        )
            if artifact is not None:
                fallback_path = Path(artifact.path) if artifact.path else Path(artifact.segment_path)
                _touch_object_last_at(root, object_id=value.object_id, fallback_path=fallback_path)
                resolved = _materialize_object_artifact(
                    artifact,
                    materialize_as=materialized,
                    root=root,
                )
                log_payload_flow(
                    "object_ref_resolved",
                    materialize_as=materialized,
                    summary=summarize_payload_flow_value(resolved),
                )
                return resolved
            raise ObjectResolutionError(f"object not found on node: {value.object_id}")
        if isinstance(value, dict):
            if is_object_ref_payload(value):
                return _resolve(object_ref_from_payload(value))
            return {key: _resolve(item) for key, item in value.items()}
        if isinstance(value, list):
            return [_resolve(item) for item in value]
        if isinstance(value, tuple):
            return tuple(_resolve(item) for item in value)
        return value

    return _resolve(payload)


def _apply_managed_globals_to_router(
    router: Dict[str, Any],
    *,
    scope_dir: str,
    globals_digest: str,
    object_dir: str,
) -> None:
    normalized_scope_dir = str(scope_dir or "").strip()
    normalized_digest = str(globals_digest or "").strip()
    if not normalized_scope_dir or not normalized_digest:
        return

    with _MANAGED_GLOBALS_APPLY_LOCKS_LOCK:
        apply_lock = _MANAGED_GLOBALS_APPLY_LOCKS.get(normalized_scope_dir)
        if apply_lock is None:
            apply_lock = threading.Lock()
            _MANAGED_GLOBALS_APPLY_LOCKS[normalized_scope_dir] = apply_lock

    with apply_lock:
        with _MANAGED_GLOBALS_CACHE_LOCK:
            if _MANAGED_GLOBALS_CACHE.get(normalized_scope_dir) == normalized_digest:
                return

        manifest_path = _managed_globals_manifest_path(Path(normalized_scope_dir), normalized_digest)
        if not manifest_path.exists():
            raise RuntimeError(f"managed globals manifest missing: {manifest_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8") or "{}")
        values_meta = dict(manifest.get("values") or {})

        resolved_values: Dict[str, Any] = {}
        for name, item in values_meta.items():
            if not isinstance(item, dict):
                continue
            value_digest = str(item.get("sha256", "") or "").strip()
            if not value_digest:
                continue
            value_path = _managed_globals_value_path(Path(normalized_scope_dir), value_digest=value_digest)
            if not value_path.exists():
                raise RuntimeError(f"managed globals value missing: {value_path}")
            serialized_value = json.loads(value_path.read_text(encoding="utf-8") or "null")
            resolved_value = convert_dict_to_arrow(serialized_value)
            resolved_values[name] = _resolve_object_refs_in_payload(resolved_value, object_dir=object_dir)

        seen_globals_ids = set()
        for fn in router.values():
            globals_dict = getattr(fn, "__globals__", None)
            if not isinstance(globals_dict, dict):
                continue
            globals_id = id(globals_dict)
            if globals_id in seen_globals_ids:
                continue
            seen_globals_ids.add(globals_id)
            for name, value in resolved_values.items():
                globals_dict[name] = value

        with _MANAGED_GLOBALS_CACHE_LOCK:
            _MANAGED_GLOBALS_CACHE[normalized_scope_dir] = normalized_digest


def _execute_payload_in_subprocess(
    artifact_path: str,
    entry_module: str,
    package_format: str,
    dependency_path: str,
    object_dir: str,
    work_dir: str,
    managed_globals_scope_dir: str,
    managed_globals_digest: str,
    export_mode: str,
    export_methods: Sequence[str],
    export_decorator: str,
    method_name: str,
    entry_callable: str,
    payload: dict,
    warmup_only: bool = False,
) -> Tuple[str, Optional[dict], str, str, Dict[str, float]]:
    """Execute uploaded user code in subprocess.

    Returns:
        (status_text, result, error_type, error_message, timings)
    """
    decode_start = time.perf_counter()
    decode_end = decode_start
    invoke_start = decode_start
    invoke_end = decode_start
    encode_start = decode_start
    encode_end = decode_start

    def _timings() -> Dict[str, float]:
        return {
            "decode_ms": round(max(0.0, decode_end - decode_start) * 1000.0, 3),
            "invoke_ms": round(max(0.0, invoke_end - invoke_start) * 1000.0, 3),
            "encode_ms": round(max(0.0, encode_end - encode_start) * 1000.0, 3),
        }

    try:
        with _temporary_working_dir(work_dir):
            try:
                router, _method_info = _load_callable_router(
                    artifact_path,
                    entry_module=entry_module,
                    package_format=package_format,
                    dependency_path=dependency_path,
                    export_mode=export_mode,
                    export_methods=export_methods,
                    export_decorator=export_decorator,
                    entry_callable=entry_callable,
                )
            except Exception as exc:
                decode_end = time.perf_counter()
                if _is_user_artifact_error(exc):
                    return (
                        "FAILED_USER",
                        None,
                        "ArtifactLoadError",
                        _describe_artifact_error(
                            exc,
                            entry_module=entry_module,
                            entry_callable=entry_callable,
                            package_format=package_format,
                        ),
                        _timings(),
                    )
                return ("FAILED_INFRA", None, exc.__class__.__name__, repr(exc), _timings())
            try:
                with _temporary_import_paths(dependency_path):
                    method = str(method_name or "").strip() or str(entry_callable or "run").strip() or "run"
                    fn = router.get(method)
                    if fn is None:
                        raise RuntimeError(f"method `{method}` not exported")
                    _apply_managed_globals_to_router(
                        router,
                        scope_dir=managed_globals_scope_dir,
                        globals_digest=managed_globals_digest,
                        object_dir=object_dir,
                    )
                    resolved_payload = _resolve_object_refs_in_payload(payload, object_dir=object_dir)
                    decode_end = time.perf_counter()
                    if bool(warmup_only):
                        invoke_start = decode_end
                        invoke_end = decode_end
                        encode_start = decode_end
                        encode_end = decode_end
                        return ("SUCCEEDED", {"warmed": True, "worker_pid": os.getpid()}, "", "", _timings())
                    invoke_start = decode_end
                    ret = _invoke_user_callable(fn, resolved_payload)
                    invoke_end = time.perf_counter()
                    encode_start = invoke_end
                status_text, result, error_type, error_message = _normalize_user_return(ret, object_dir=object_dir)
                encode_end = time.perf_counter()
                return (status_text, result, error_type, error_message, _timings())
            except LargeResultError as exc:
                if decode_end <= decode_start:
                    decode_end = time.perf_counter()
                if invoke_end <= invoke_start and decode_end > decode_start:
                    invoke_end = time.perf_counter()
                    encode_start = invoke_end
                encode_end = time.perf_counter()
                return ("FAILED_USER", None, exc.__class__.__name__, str(exc), _timings())
            except ObjectResolutionError as exc:
                if decode_end <= decode_start:
                    decode_end = time.perf_counter()
                return ("FAILED_INFRA", None, exc.__class__.__name__, str(exc), _timings())
            except Exception as exc:
                now = time.perf_counter()
                if decode_end <= decode_start:
                    decode_end = now
                elif invoke_end <= invoke_start:
                    invoke_end = now
                else:
                    encode_end = now
                if isinstance(exc, (ImportError, ModuleNotFoundError)):
                    return ("FAILED_USER", None, exc.__class__.__name__, _describe_user_execution_error(exc), _timings())
                return ("FAILED_USER", None, exc.__class__.__name__, repr(exc), _timings())
    except Exception as exc:
        decode_end = time.perf_counter()
        return ("FAILED_INFRA", None, exc.__class__.__name__, repr(exc), _timings())


def utc_now() -> datetime:
    """获取当前 UTC 时间。"""
    return datetime.now(timezone.utc)


def dt_to_ts(dt: datetime) -> timestamp_pb2.Timestamp:
    """将 datetime 转换为 protobuf Timestamp。

    Args:
        dt: datetime 对象

    Returns:
        timestamp_pb2.Timestamp: protobuf 时间戳
    """
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(dt)
    return ts


def ts_to_dt(ts: timestamp_pb2.Timestamp) -> datetime:
    if ts is None:
        return utc_now()
    if ts.seconds == 0 and ts.nanos == 0:
        return utc_now()
    try:
        dt = ts.ToDatetime()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return utc_now()




@dataclass
class NodeMetricsState:
    """节点指标状态。

    Attributes:
        queued: 队列中的任务数
        inflight: 执行中的任务数
        running: 运行中的任务数
        credit: 可用配额
        cpu_percent: CPU 使用率
        mem_percent: 内存使用率
    """
    queued: int = 0
    inflight: int = 0
    running: int = 0
    credit: int = 0
    cpu_percent: float = 0.0
    mem_percent: float = 0.0


@dataclass
class NodeServiceState:
    service_name: str
    service_id: str
    status: int
    worker_count: int = 0
    alive_workers: int = 0
    in_flight: int = 0
    received_count: int = 0
    returned_count: int = 0
    ema_child_invoke_ms: float = 0.0
    ema_samples: int = 0
    lease_expire_at: datetime = field(default_factory=utc_now)
    http_base_url: str = ""


@dataclass
class NodeTaskPoolInfo:
    pool_id: str
    owner_client_id: str
    pool_name: str
    code_version: str
    status: str
    worker_count: int = 0
    task_count: int = 0
    inflight: int = 0
    created_at: datetime = field(default_factory=utc_now)
    last_heartbeat_at: datetime = field(default_factory=utc_now)
    lease_expire_at: datetime = field(default_factory=utc_now)


@dataclass
class NodeState:
    """节点状态。

    Attributes:
        node_id: 节点 ID
        control_addr: 控制地址
        capacity: 容量
        queue_capacity: 队列容量
        tags: 标签列表
        version: 版本
        metadata: 元数据
        healthy: 是否健康
        last_seen_at: 最后活跃时间
        metrics: 节点指标
    """
    node_instance_id: str
    node_id: str
    control_addr: str
    capacity: int
    queue_capacity: int
    tags: List[str] = field(default_factory=list)
    version: str = ""
    python_version: str = ""
    metadata: Dict[str, str] = field(default_factory=dict)
    healthy: bool = True
    last_seen_at: datetime = field(default_factory=utc_now)
    metrics: NodeMetricsState = field(default_factory=NodeMetricsState)
    services: Dict[str, NodeServiceState] = field(default_factory=dict)
    task_pools: Dict[str, NodeTaskPoolInfo] = field(default_factory=dict)
    active_runtimes: List[str] = field(default_factory=list)
    service_worker_capacity: int = 0
    service_worker_used: int = 0
    task_pool_worker_capacity: int = 0
    task_pool_worker_used: int = 0
    schedulable: bool = True
    drain: bool = False
    reason: str = ""

    def service_worker_available(self) -> int:
        capacity = max(0, int(self.service_worker_capacity or 0))
        used = max(0, int(self.service_worker_used or 0))
        return max(0, capacity - used)

    def active_runtime_count(self) -> int:
        return len(self.active_runtimes)

    def task_pool_worker_available(self) -> int:
        capacity = max(0, int(self.task_pool_worker_capacity or 0))
        used = max(0, int(self.task_pool_worker_used or 0))
        return max(0, capacity - used)


class InfoCenterState:
    def __init__(self, *, lease_ttl_sec: int = 90, heartbeat_interval_sec: int = 30) -> None:
        self.lease_ttl_sec = max(1, lease_ttl_sec)
        self.heartbeat_interval_sec = max(1, heartbeat_interval_sec)
        self._lock = threading.Lock()
        self._nodes: Dict[str, NodeState] = {}

    def _node_is_stale_locked(self, state: NodeState, *, now: Optional[datetime] = None) -> bool:
        current_time = now or utc_now()
        return (current_time - state.last_seen_at).total_seconds() > float(self.lease_ttl_sec)

    def _node_is_healthy_locked(self, state: NodeState, *, now: Optional[datetime] = None) -> bool:
        return bool(state.healthy) and not self._node_is_stale_locked(state, now=now)

    def _prune_replaced_stale_nodes_locked(
        self,
        *,
        node_instance_id: str,
        node_id: str,
        control_addr: str,
        now: Optional[datetime] = None,
    ) -> None:
        current_time = now or utc_now()
        normalized_instance_id = str(node_instance_id or "").strip()
        normalized_node_id = str(node_id or "").strip()
        normalized_control_addr = str(control_addr or "").strip()
        if not normalized_instance_id or not normalized_node_id or not normalized_control_addr:
            return
        stale_keys = [
            key
            for key, state in self._nodes.items()
            if key != normalized_instance_id
            and str(state.node_id or "").strip() == normalized_node_id
            and str(state.control_addr or "").strip() == normalized_control_addr
            and self._node_is_stale_locked(state, now=current_time)
        ]
        for key in stale_keys:
            self._nodes.pop(key, None)

    def _effective_service_state_locked(
        self,
        state: NodeState,
        svc: NodeServiceState,
        *,
        now: Optional[datetime] = None,
    ) -> Tuple[int, int, int, datetime, bool, str]:
        current_time = now or utc_now()
        node_healthy = self._node_is_healthy_locked(state, now=current_time)
        if node_healthy:
            return (
                int(svc.status),
                int(svc.alive_workers),
                int(svc.in_flight),
                svc.lease_expire_at,
                False,
                "",
            )
        return (
            int(pb2.SERVICE_STATUS_UNSPECIFIED),
            0,
            0,
            current_time,
            True,
            "LOST",
        )

    @staticmethod
    def _predicted_busy_score(*, inflight: int, ema_child_invoke_ms: float, alive_workers: int) -> float:
        normalized_inflight = max(0, int(inflight or 0))
        normalized_workers = max(1, int(alive_workers or 0))
        normalized_ema = max(0.0, float(ema_child_invoke_ms or 0.0))
        if normalized_ema <= 0.0:
            return float(normalized_inflight) / float(normalized_workers)
        return (float(normalized_inflight) * normalized_ema) / float(normalized_workers)

    def register_node_record(
        self,
        *,
        node_instance_id: str = "",
        node_id: str,
        control_addr: str,
        capacity: int,
        queue_capacity: int,
        tags: Iterable[str] = (),
        version: str = "",
        metadata: Optional[Dict[str, str]] = None,
        services: Optional[Dict[str, NodeServiceState]] = None,
        task_pools: Optional[Dict[str, NodeTaskPoolInfo]] = None,
        active_runtimes: Optional[Sequence[str]] = None,
        service_worker_capacity: int = 0,
        service_worker_used: int = 0,
        task_pool_worker_capacity: int = 0,
        task_pool_worker_used: int = 0,
        python_version: str = "",
    ) -> NodeState:
        now = utc_now()
        normalized_instance_id = str(node_instance_id or node_id or "").strip()
        if not normalized_instance_id:
            raise ValueError("node_instance_id is required")
        with self._lock:
            self._prune_replaced_stale_nodes_locked(
                node_instance_id=normalized_instance_id,
                node_id=node_id,
                control_addr=control_addr,
                now=now,
            )
            state = self._nodes.get(normalized_instance_id)
            if state is None:
                state = NodeState(
                    node_instance_id=normalized_instance_id,
                    node_id=node_id,
                    control_addr=control_addr,
                    capacity=max(1, capacity),
                    queue_capacity=max(1, queue_capacity),
                    python_version=str(python_version or "").strip(),
                )
                self._nodes[normalized_instance_id] = state
            state.node_instance_id = normalized_instance_id
            state.node_id = str(node_id or state.node_id or "").strip() or normalized_instance_id
            state.control_addr = control_addr
            state.capacity = max(1, capacity)
            state.queue_capacity = max(1, queue_capacity)
            state.tags = list(tags or [])
            state.version = str(version or "")
            state.python_version = str(python_version or state.python_version or "").strip()
            state.metadata = dict(metadata or {})
            state.healthy = True
            state.last_seen_at = now
            state.services = dict(services or {})
            state.task_pools = dict(task_pools or {})
            state.active_runtimes = [str(x).strip() for x in (active_runtimes or []) if str(x).strip()]
            state.service_worker_capacity = max(0, int(service_worker_capacity or 0))
            state.service_worker_used = max(0, min(int(service_worker_used or 0), state.service_worker_capacity or int(service_worker_used or 0)))
            state.task_pool_worker_capacity = max(0, int(task_pool_worker_capacity or 0))
            state.task_pool_worker_used = max(0, min(int(task_pool_worker_used or 0), state.task_pool_worker_capacity or int(task_pool_worker_used or 0)))
            if state.metrics.credit == 0:
                state.metrics.credit = state.queue_capacity
            return state

    def register_node(self, request: pb2.RegisterNodeRequest) -> NodeState:
        metadata = dict(request.metadata)
        return self.register_node_record(
            node_instance_id=getattr(request, "node_instance_id", "") or request.node_id,
            node_id=request.node_id,
            control_addr=request.control_addr,
            capacity=max(1, request.capacity),
            queue_capacity=max(1, request.queue_capacity),
            tags=request.tags,
            version=request.version,
            metadata=metadata,
            services=self._parse_services(request.services),
            task_pools={},
            active_runtimes=(),
            service_worker_capacity=int(metadata.get("service_worker_capacity", "0") or 0),
            service_worker_used=int(metadata.get("service_worker_used", "0") or 0),
            task_pool_worker_capacity=int(metadata.get("task_pool_worker_capacity", "0") or 0),
            task_pool_worker_used=int(metadata.get("task_pool_worker_used", "0") or 0),
            python_version=metadata.get("python_version", ""),
        )

    def heartbeat_record(
        self,
        *,
        node_instance_id: str = "",
        node_id: str,
        healthy: bool,
        metrics: Optional[NodeMetricsState] = None,
        metadata: Optional[Dict[str, str]] = None,
        services: Optional[Dict[str, NodeServiceState]] = None,
        task_pools: Optional[Dict[str, NodeTaskPoolInfo]] = None,
        active_runtimes: Optional[Sequence[str]] = None,
        service_worker_capacity: int = 0,
        service_worker_used: int = 0,
        task_pool_worker_capacity: int = 0,
        task_pool_worker_used: int = 0,
        python_version: str = "",
    ) -> Optional[NodeState]:
        now = utc_now()
        normalized_instance_id = str(node_instance_id or node_id or "").strip()
        if not normalized_instance_id:
            return None
        with self._lock:
            state = self._nodes.get(normalized_instance_id)
            if state is None:
                return None
            state.node_instance_id = normalized_instance_id
            state.node_id = str(node_id or state.node_id or "").strip() or normalized_instance_id
            state.healthy = bool(healthy)
            state.last_seen_at = now
            if metrics is not None:
                state.metrics = metrics
            if metadata is not None:
                state.metadata = dict(metadata or {})
            state.services = dict(services or {})
            state.task_pools = dict(task_pools or {})
            if python_version:
                state.python_version = str(python_version).strip()
            if active_runtimes is not None:
                state.active_runtimes = [str(x).strip() for x in active_runtimes if str(x).strip()]
            if service_worker_capacity > 0:
                state.service_worker_capacity = max(0, int(service_worker_capacity))
            state.service_worker_used = max(
                0,
                min(
                    int(service_worker_used or 0),
                    state.service_worker_capacity or int(service_worker_used or 0),
                ),
            )
            if task_pool_worker_capacity > 0:
                state.task_pool_worker_capacity = max(0, int(task_pool_worker_capacity))
            state.task_pool_worker_used = max(
                0,
                min(
                    int(task_pool_worker_used or 0),
                    state.task_pool_worker_capacity or int(task_pool_worker_used or 0),
                ),
            )
            return state

    def heartbeat(self, request: pb2.HeartbeatNodeRequest) -> Optional[NodeState]:
        return self.heartbeat_record(
            node_instance_id=getattr(request, "node_instance_id", "") or request.node_id,
            node_id=request.node_id,
            healthy=bool(request.healthy),
            metrics=NodeMetricsState(
                queued=max(0, request.metrics.queued),
                inflight=max(0, request.metrics.inflight),
                running=max(0, request.metrics.running),
                credit=request.metrics.credit,
                cpu_percent=float(request.metrics.cpu_percent),
                mem_percent=float(request.metrics.mem_percent),
            ),
            services=self._parse_services(request.services),
        )

    def _parse_services(self, reports: Iterable[pb2.ServiceRouteReport]) -> Dict[str, NodeServiceState]:
        out: Dict[str, NodeServiceState] = {}
        for item in reports:
            if not item.service_name or not item.service_id:
                continue
            out[item.service_id] = NodeServiceState(
                service_name=item.service_name,
                service_id=item.service_id,
                status=int(item.status),
                worker_count=max(0, int(item.worker_count)),
                alive_workers=max(0, int(item.alive_workers)),
                in_flight=max(0, int(item.in_flight)),
                lease_expire_at=ts_to_dt(item.lease_expire_at),
                http_base_url=item.http_base_url,
            )
        return out

    def mark_node_lost(self, node_instance_id: str, *, reason: str = "") -> NodeState:
        now = utc_now()
        with self._lock:
            state = self._nodes.get(node_instance_id)
            if state is None:
                raise KeyError("node not found")
            state.healthy = False
            state.schedulable = False
            state.last_seen_at = now - timedelta(seconds=float(self.lease_ttl_sec) + 1.0)
            state.reason = str(reason or state.reason or "node lost")
            degraded: Dict[str, NodeServiceState] = {}
            for service_id, svc in state.services.items():
                degraded[service_id] = NodeServiceState(
                    service_name=svc.service_name,
                    service_id=svc.service_id,
                    status=int(pb2.SERVICE_STATUS_UNSPECIFIED),
                    worker_count=max(0, int(svc.worker_count)),
                    alive_workers=0,
                    in_flight=0,
                    lease_expire_at=now,
                    http_base_url=svc.http_base_url,
                )
            state.services = degraded
            return NodeState(
                node_instance_id=state.node_instance_id,
                node_id=state.node_id,
                control_addr=state.control_addr,
                capacity=state.capacity,
                queue_capacity=state.queue_capacity,
                tags=list(state.tags),
                version=state.version,
                python_version=state.python_version,
                metadata=dict(state.metadata),
                healthy=False,
                last_seen_at=state.last_seen_at,
                metrics=NodeMetricsState(**vars(state.metrics)),
                services={k: NodeServiceState(**vars(v)) for k, v in state.services.items()},
                task_pools={k: NodeTaskPoolInfo(**vars(v)) for k, v in state.task_pools.items()},
                active_runtimes=list(state.active_runtimes),
                service_worker_capacity=state.service_worker_capacity,
                service_worker_used=state.service_worker_used,
                task_pool_worker_capacity=state.task_pool_worker_capacity,
                task_pool_worker_used=state.task_pool_worker_used,
                schedulable=state.schedulable,
                drain=state.drain,
                reason=state.reason,
            )

    def list_service_routes(self, *, service_name: str, healthy_only: bool, limit: int) -> List[Dict[str, object]]:
        now = utc_now()
        name_filter = service_name.strip()
        with self._lock:
            out: List[Dict[str, object]] = []
            for state in self._nodes.values():
                is_healthy = self._node_is_healthy_locked(state, now=now)
                if healthy_only and not is_healthy:
                    continue
                for svc in state.services.values():
                    if name_filter and svc.service_name != name_filter:
                        continue
                    effective_status, effective_alive, effective_in_flight, effective_lease_expire_at, stale, status_text = (
                        self._effective_service_state_locked(state, svc, now=now)
                    )
                    reported_inflight = max(0, int(svc.in_flight or 0))
                    received_count = max(0, int(svc.received_count or 0))
                    returned_count = max(0, int(svc.returned_count or 0))
                    if received_count > 0 or returned_count > 0:
                        computed_inflight = max(0, received_count - returned_count)
                    else:
                        computed_inflight = max(0, int(effective_in_flight or 0))
                    effective_computed_inflight = computed_inflight if is_healthy else 0
                    ema_samples = max(0, int(svc.ema_samples or 0))
                    raw_ema_child_invoke_ms = max(0.0, float(svc.ema_child_invoke_ms or 0.0))
                    effective_ema_child_invoke_ms = raw_ema_child_invoke_ms if ema_samples >= 10 else 0.0
                    predicted_busy = self._predicted_busy_score(
                        inflight=effective_computed_inflight,
                        ema_child_invoke_ms=effective_ema_child_invoke_ms,
                        alive_workers=effective_alive,
                    )
                    out.append(
                        {
                            "service_name": svc.service_name,
                            "service_id": svc.service_id,
                            "status": effective_status,
                            "status_text": status_text,
                            "node_instance_id": state.node_instance_id,
                            "node_id": state.node_id,
                            "control_addr": state.control_addr,
                            "node_healthy": is_healthy,
                            "stale": stale,
                            "worker_count": svc.worker_count,
                            "alive_workers": effective_alive,
                            "in_flight": effective_computed_inflight,
                            "reported_in_flight": reported_inflight,
                            "received_count": received_count,
                            "returned_count": returned_count,
                            "ema_child_invoke_ms": raw_ema_child_invoke_ms,
                            "ema_samples": ema_samples,
                            "predicted_busy": predicted_busy,
                            "lease_expire_at": effective_lease_expire_at,
                            "http_base_url": svc.http_base_url,
                        }
                    )
            out.sort(
                key=lambda x: (
                    x["service_name"],
                    not x["node_healthy"],
                    int(x["status"] != pb2.SERVICE_STATUS_RUNNING),
                    float(x.get("predicted_busy", 0.0) or 0.0),
                    int(x["in_flight"]),
                    -int(x.get("alive_workers", 0) or 0),
                    x["node_id"],
                    x["service_id"],
                )
            )
            return out[: max(1, limit)]

    def list_nodes(self, *, healthy_only: bool, tags: Iterable[str], limit: int) -> List[NodeState]:
        now = utc_now()
        filter_tags = set(tags)
        with self._lock:
            out: List[NodeState] = []
            for state in self._nodes.values():
                is_healthy = self._node_is_healthy_locked(state, now=now)
                if healthy_only and not is_healthy:
                    continue
                if filter_tags and not filter_tags.issubset(set(state.tags)):
                    continue
                out.append(
                    NodeState(
                        node_instance_id=state.node_instance_id,
                        node_id=state.node_id,
                        control_addr=state.control_addr,
                        capacity=state.capacity,
                        queue_capacity=state.queue_capacity,
                        tags=list(state.tags),
                        version=state.version,
                        python_version=state.python_version,
                        metadata=dict(state.metadata),
                        healthy=is_healthy,
                        last_seen_at=state.last_seen_at,
                        metrics=NodeMetricsState(**vars(state.metrics)),
                        services={k: NodeServiceState(**vars(v)) for k, v in state.services.items()},
                        task_pools={k: NodeTaskPoolInfo(**vars(v)) for k, v in state.task_pools.items()},
                        active_runtimes=list(state.active_runtimes),
                        service_worker_capacity=state.service_worker_capacity,
                        service_worker_used=state.service_worker_used,
                        task_pool_worker_capacity=state.task_pool_worker_capacity,
                        task_pool_worker_used=state.task_pool_worker_used,
                        schedulable=state.schedulable,
                        drain=state.drain,
                        reason=state.reason,
                    )
                )
            out.sort(key=lambda n: (not n.healthy, not n.schedulable, n.drain, -(n.service_worker_available())))
            return out[: max(1, limit)]

    def update_node_schedule_state(
        self,
        node_instance_id: str,
        *,
        schedulable: Optional[bool] = None,
        drain: Optional[bool] = None,
        reason: Optional[str] = None,
    ) -> NodeState:
        with self._lock:
            state = self._nodes.get(node_instance_id)
            if state is None:
                raise KeyError("node not found")
            if schedulable is not None:
                state.schedulable = bool(schedulable)
            if drain is not None:
                state.drain = bool(drain)
            if reason is not None:
                state.reason = str(reason or "")
            return NodeState(
                node_instance_id=state.node_instance_id,
                node_id=state.node_id,
                control_addr=state.control_addr,
                capacity=state.capacity,
                queue_capacity=state.queue_capacity,
                tags=list(state.tags),
                version=state.version,
                python_version=state.python_version,
                metadata=dict(state.metadata),
                healthy=state.healthy,
                last_seen_at=state.last_seen_at,
                metrics=NodeMetricsState(**vars(state.metrics)),
                services={k: NodeServiceState(**vars(v)) for k, v in state.services.items()},
                task_pools={k: NodeTaskPoolInfo(**vars(v)) for k, v in state.task_pools.items()},
                active_runtimes=list(state.active_runtimes),
                service_worker_capacity=state.service_worker_capacity,
                service_worker_used=state.service_worker_used,
                task_pool_worker_capacity=state.task_pool_worker_capacity,
                task_pool_worker_used=state.task_pool_worker_used,
                schedulable=state.schedulable,
                drain=state.drain,
                reason=state.reason,
            )


@dataclass
class CodeArtifact:
    """代码制品。

    Attributes:
        code_version: 代码版本（SHA256）
        path: 文件路径
        size_bytes: 文件大小
        created_at: 创建时间
    """
    code_version: str
    path: str
    runtime: str
    entry_module: str
    entry_callable: str
    package_format: str
    export_mode: str
    export_methods: Tuple[str, ...]
    export_decorator: str
    dependency_allowlist: Tuple[str, ...]
    dependency_path: str
    size_bytes: int
    created_at: datetime


@dataclass
class ObjectArtifact:
    object_id: str
    path: str
    format: str
    size_bytes: int
    created_at: datetime
    storage_backend: str = "file"
    segment_path: str = ""
    segment_offset: int = 0
    segment_length: int = 0


@dataclass
class TaskState:
    """任务状态。

    Attributes:
        task_id: 任务 ID
        client_id: 客户端 ID
        code_version: 代码版本
        execution_mode: 执行模式
        payload: 载荷数据
        timeout_hint_sec: 超时提示
        priority: 优先级
        status: 状态
        attempt: 尝试次数
        worker_id: 工作进程 ID
        lease_id: 租约 ID
        started_at: 开始时间
        finished_at: 完成时间
        last_heartbeat_at: 最后心跳时间
        cancel_requested: 是否请求取消
        result: 结果
        error_type: 错误类型
        error_message: 错误消息
    """
    task_id: str
    client_id: str
    job_id: str
    code_version: str
    runtime_key: str
    execution_mode: int
    payload: dict
    timeout_hint_sec: int
    priority: int
    status: int = pb2.TASK_STATUS_QUEUED
    attempt: int = 1
    worker_id: str = ""
    lease_id: str = ""
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    last_heartbeat_at: Optional[datetime] = None
    cancel_requested: bool = False
    result: Optional[Any] = None
    error_type: str = ""
    error_message: str = ""
    dispatch_build_execute_spec_ms: float = 0.0

    def as_result(self) -> pb2.TaskResult:
        """转换为 protobuf TaskResult。

        Returns:
            pb2.TaskResult: protobuf 任务结果对象
        """
        item = pb2.TaskResult(
            task_id=self.task_id,
            job_id=self.job_id,
            status=self.status,
            attempt=self.attempt,
            started_at=dt_to_ts(self.started_at or utc_now()),
            finished_at=dt_to_ts(self.finished_at or utc_now()),
            result=dict_to_struct(self.result),
            error=pb2.TaskError(type=self.error_type, message=self.error_message),
        )
        return item


@dataclass
class ServiceSession:
    service_id: str
    owner_client_id: str
    service_name: str
    code_version: str
    worker_count: int
    heartbeat_timeout_sec: int
    idle_ttl_sec: int
    expose_http: bool
    service_token: str
    http_base_url: str
    status: int
    created_at: datetime
    last_heartbeat_at: datetime
    lease_expire_at: datetime
    executor_ready: bool = False
    in_flight: int = 0
    queued: int = 0
    alive_workers: int = 0
    stop_reason: str = ""
    methods: Dict[str, Tuple[str, str]] = field(default_factory=dict)
    managed_global_names: Tuple[str, ...] = ()
    managed_globals_scope_dir: str = ""
    managed_globals_digest: str = ""
    timing_metrics: Dict[str, object] = field(default_factory=dict)
    request_count: int = 0
    returned_count: int = 0


@dataclass
class TaskPoolState:
    pool_id: str
    owner_client_id: str
    pool_name: str
    code_version: str
    task_method: str
    worker_count: int
    heartbeat_timeout_sec: int
    idle_ttl_sec: int
    pool_token: str
    status: str
    created_at: datetime
    last_heartbeat_at: datetime
    lease_expire_at: datetime
    managed_global_names: Tuple[str, ...] = ()
    managed_globals_scope_dir: str = ""
    managed_globals_digest: str = ""
    executor_ready: bool = False
    task_count: int = 0
    timing_metrics: Dict[str, object] = field(default_factory=dict)
    returned_count: int = 0


class NodeControlState:
    """NodeControl 状态管理。

    负责代码上传、任务提交、结果拉取等核心功能。

    Attributes:
        node_id: 节点 ID
        worker_capacity: 工作进程容量
        queue_capacity: 队列容量
        heartbeat_timeout_sec: 心跳超时
        max_retries: 最大重试次数
        monitor_interval_sec: 监控间隔
        artifact_dir: 制品目录
    """
    def __init__(
        self,
        *,
        node_id: str,
        worker_capacity: int = 32,
        queue_capacity: int = 4000,
        heartbeat_timeout_sec: int = 90,
        max_retries: int = 3,
        monitor_interval_sec: int = 10,
        artifact_dir: str = "./code_cache",
        enable_internal_executor: bool = True,
        executor_poll_interval_sec: float = 0.05,
        enable_service_session: bool = True,
        service_default_worker_count: int = 10,
        service_default_heartbeat_timeout_sec: int = 30,
        service_worker_capacity: int = 0,
        task_pool_worker_capacity: int = 0,
        service_http_bind: str = "0.0.0.0:18080",
        service_http_base_url: str = "",
    ) -> None:
        self.node_id = node_id
        self.worker_capacity = max(1, worker_capacity)
        self.queue_capacity = max(1, queue_capacity)
        self.heartbeat_timeout_sec = max(5, heartbeat_timeout_sec)
        self.max_retries = max(0, max_retries)
        self.monitor_interval_sec = max(1, monitor_interval_sec)
        self.enable_internal_executor = bool(enable_internal_executor)
        self.executor_poll_interval_sec = max(0.01, float(executor_poll_interval_sec))
        self.enable_service_session = bool(enable_service_session)
        self.service_default_worker_count = max(1, service_default_worker_count)
        self.service_default_heartbeat_timeout_sec = max(5, service_default_heartbeat_timeout_sec)
        self.service_worker_capacity = max(1, int(service_worker_capacity or worker_capacity))
        default_task_pool_capacity = max(1, int(os.cpu_count() or 1))
        self.task_pool_worker_capacity = max(1, int(task_pool_worker_capacity or default_task_pool_capacity))
        self.service_http_bind = service_http_bind
        self.service_http_base_url = service_http_base_url.strip()
        self.started_at = utc_now()

        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._pending: Deque[str] = deque()
        self._tasks: Dict[str, TaskState] = {}
        self._pool_tasks: Dict[str, TaskState] = {}
        self._codes: Dict[str, CodeArtifact] = {}
        self._objects: Dict[str, ObjectArtifact] = {}
        self._services: Dict[str, ServiceSession] = {}
        self._result_hook = InMemoryResultHook()
        self._pool_result_hook = InMemoryResultHook()
        self._task_pools: Dict[str, TaskPoolState] = {}
        self._service_worker_reserved = 0
        self._task_pool_worker_reserved = 0
        self._code_write_locks: Dict[str, threading.Lock] = {}
        self._object_write_locks: Dict[str, threading.Lock] = {}

        # 检测并保存当前 Python 版本
        self._python_version = f"py{sys.version_info.major}.{sys.version_info.minor}"

        self._artifact_dir = Path(artifact_dir).expanduser().resolve()
        self._artifact_dir.mkdir(parents=True, exist_ok=True)
        self._codes_dir = self._artifact_dir / "codes"
        self._codes_dir.mkdir(parents=True, exist_ok=True)
        self._object_dir = self._artifact_dir / "objects"
        self._object_dir.mkdir(parents=True, exist_ok=True)
        self._runtime_managed_globals: Dict[Tuple[str, str, str], ManagedGlobalsState] = {}
        self._client_code_tokens: Dict[Tuple[str, str], str] = {}
        self._client_code_managed_globals: Dict[Tuple[str, str, str], Tuple[str, ...]] = {}
        self._object_segment_max_bytes = max(0, int(OBJECT_SEGMENT_MAX_BYTES))
        self._object_segment_target_bytes = max(self._object_segment_max_bytes, int(OBJECT_SEGMENT_TARGET_BYTES))

        self._stop_event = threading.Event()
        self._monitor = threading.Thread(target=self._monitor_loop, name="nodecontrol-monitor", daemon=True)
        self._monitor.start()
        self._executor_host = (
            ExecutorHostClient(task_worker_capacity=self.worker_capacity)
            if (self.enable_internal_executor or self.enable_service_session)
            else None
        )
        self._dispatcher: Optional[threading.Thread] = None
        self._service_http_gateway: Optional[ServiceHttpGateway] = None

        if self.enable_internal_executor:
            self._dispatcher = threading.Thread(
                target=self._dispatch_loop,
                name="nodecontrol-dispatcher",
                daemon=True,
            )
            self._dispatcher.start()

        if self.enable_service_session and self.service_http_bind:
            self._service_http_gateway = ServiceHttpGateway(
                bind=self.service_http_bind,
                invoke_handler=self._invoke_service_http,
                status_handler=self._service_status_http,
            )
            self._service_http_gateway.start()
            if not self.service_http_base_url:
                self.service_http_base_url = self._service_http_gateway.base_url

    def _record_service_timing_locked(
        self,
        session: ServiceSession,
        *,
        method: str,
        ok: bool,
        http_status: int,
        setup_ms: float,
        build_execute_spec_ms: float,
        executor_ms: float,
        finalize_ms: float,
        total_ms: float,
        subprocess_timings: Optional[Dict[str, object]] = None,
        error_type: str = "",
        error_message: str = "",
    ) -> None:
        try:
            metrics = dict(session.timing_metrics or {})
            call_count = int(metrics.get("call_count", 0) or 0) + 1
            error_count = int(metrics.get("error_count", 0) or 0) + (0 if ok else 1)
            metrics["call_count"] = call_count
            metrics["error_count"] = error_count
            metrics["last_method"] = str(method or "")
            metrics["last_ok"] = bool(ok)
            metrics["last_http_status"] = int(http_status)
            metrics["last_total_ms"] = round(float(total_ms), 3)
            metrics["last_setup_ms"] = round(float(setup_ms), 3)
            metrics["last_build_execute_spec_ms"] = round(float(build_execute_spec_ms), 3)
            metrics["last_executor_ms"] = round(float(executor_ms), 3)
            metrics["last_finalize_ms"] = round(float(finalize_ms), 3)
            metrics["max_total_ms"] = round(max(float(metrics.get("max_total_ms", 0.0) or 0.0), float(total_ms)), 3)
            metrics["avg_setup_ms"] = round(
                ((float(metrics.get("avg_setup_ms", 0.0) or 0.0) * (call_count - 1)) + float(setup_ms)) / call_count,
                3,
            )
            metrics["avg_build_execute_spec_ms"] = round(
                (
                    (float(metrics.get("avg_build_execute_spec_ms", 0.0) or 0.0) * (call_count - 1))
                    + float(build_execute_spec_ms)
                )
                / call_count,
                3,
            )
            metrics["avg_executor_ms"] = round(
                ((float(metrics.get("avg_executor_ms", 0.0) or 0.0) * (call_count - 1)) + float(executor_ms)) / call_count,
                3,
            )
            metrics["avg_finalize_ms"] = round(
                ((float(metrics.get("avg_finalize_ms", 0.0) or 0.0) * (call_count - 1)) + float(finalize_ms)) / call_count,
                3,
            )
            metrics["avg_total_ms"] = round(
                ((float(metrics.get("avg_total_ms", 0.0) or 0.0) * (call_count - 1)) + float(total_ms)) / call_count,
                3,
            )
            if subprocess_timings:
                decode_ms = float(subprocess_timings.get("decode_ms", 0.0) or 0.0)
                invoke_ms = float(subprocess_timings.get("invoke_ms", 0.0) or 0.0)
                encode_ms = float(subprocess_timings.get("encode_ms", 0.0) or 0.0)
                queue_wait_ms = max(0.0, float(executor_ms) - decode_ms - invoke_ms - encode_ms)
                alpha = 0.2
                prev_ema = float(metrics.get("ema_child_invoke_ms", 0.0) or 0.0)
                sample_count = int(metrics.get("ema_samples", 0) or 0) + 1
                ema = invoke_ms if sample_count <= 1 else ((alpha * invoke_ms) + ((1.0 - alpha) * prev_ema))
                metrics["last_child_decode_ms"] = round(decode_ms, 3)
                metrics["last_invoke_ms"] = round(invoke_ms, 3)
                metrics["last_child_invoke_ms"] = round(invoke_ms, 3)
                metrics["last_child_encode_ms"] = round(encode_ms, 3)
                metrics["last_queue_wait_ms"] = round(queue_wait_ms, 3)
                metrics["ema_child_invoke_ms"] = round(ema, 3)
                metrics["ema_samples"] = sample_count
                metrics["avg_child_decode_ms"] = round(
                    ((float(metrics.get("avg_child_decode_ms", 0.0) or 0.0) * (call_count - 1)) + decode_ms) / call_count,
                    3,
                )
                metrics["avg_invoke_ms"] = round(
                    ((float(metrics.get("avg_invoke_ms", 0.0) or 0.0) * (call_count - 1)) + invoke_ms) / call_count,
                    3,
                )
                metrics["avg_child_invoke_ms"] = metrics["avg_invoke_ms"]
                metrics["avg_child_encode_ms"] = round(
                    ((float(metrics.get("avg_child_encode_ms", 0.0) or 0.0) * (call_count - 1)) + encode_ms) / call_count,
                    3,
                )
            else:
                metrics.setdefault("last_child_decode_ms", 0.0)
                metrics.setdefault("last_invoke_ms", 0.0)
                metrics.setdefault("last_child_invoke_ms", metrics.get("last_invoke_ms", 0.0))
                metrics.setdefault("last_child_encode_ms", 0.0)
                metrics.setdefault("avg_child_decode_ms", 0.0)
                metrics.setdefault("avg_invoke_ms", 0.0)
                metrics.setdefault("avg_child_invoke_ms", metrics.get("avg_invoke_ms", 0.0))
                metrics.setdefault("avg_child_encode_ms", 0.0)
            metrics["last_error_type"] = str(error_type or "")
            metrics["last_error_message"] = str(error_message or "")
            metrics["updated_at"] = utc_now().isoformat()
            session.timing_metrics = metrics

            event = {
                "event": "service_timing",
                "service_id": session.service_id,
                "service_name": session.service_name,
                "method": str(method or ""),
                "ok": bool(ok),
                "http_status": int(http_status),
                "setup_ms": round(float(setup_ms), 3),
                "build_execute_spec_ms": round(float(build_execute_spec_ms), 3),
                "executor_ms": round(float(executor_ms), 3),
                "finalize_ms": round(float(finalize_ms), 3),
                "total_ms": round(float(total_ms), 3),
                "error_type": str(error_type or ""),
                "error_message": str(error_message or ""),
            }
            if subprocess_timings:
                event["subprocess"] = dict(subprocess_timings)
            service_timing_logger.info(json.dumps(event, ensure_ascii=False))
        except Exception as exc:
            logger = logging.getLogger(__name__)
            logger.debug("failed to record service timing: %r", exc)

    def _record_task_pool_timing_locked(
        self,
        pool: TaskPoolState,
        *,
        method: str,
        ok: bool,
        setup_ms: float,
        build_execute_spec_ms: float,
        executor_ms: float,
        finalize_ms: float,
        total_ms: float,
        subprocess_timings: Optional[Dict[str, object]] = None,
        error_type: str = "",
        error_message: str = "",
    ) -> None:
        try:
            metrics = dict(pool.timing_metrics or {})
            call_count = int(metrics.get("call_count", 0) or 0) + 1
            error_count = int(metrics.get("error_count", 0) or 0) + (0 if ok else 1)
            metrics["call_count"] = call_count
            metrics["error_count"] = error_count
            metrics["last_method"] = str(method or "")
            metrics["last_ok"] = bool(ok)
            metrics["last_total_ms"] = round(float(total_ms), 3)
            metrics["last_setup_ms"] = round(float(setup_ms), 3)
            metrics["last_build_execute_spec_ms"] = round(float(build_execute_spec_ms), 3)
            metrics["last_executor_ms"] = round(float(executor_ms), 3)
            metrics["last_finalize_ms"] = round(float(finalize_ms), 3)
            metrics["max_total_ms"] = round(max(float(metrics.get("max_total_ms", 0.0) or 0.0), float(total_ms)), 3)
            metrics["avg_setup_ms"] = round(
                ((float(metrics.get("avg_setup_ms", 0.0) or 0.0) * (call_count - 1)) + float(setup_ms)) / call_count,
                3,
            )
            metrics["avg_build_execute_spec_ms"] = round(
                (
                    (float(metrics.get("avg_build_execute_spec_ms", 0.0) or 0.0) * (call_count - 1))
                    + float(build_execute_spec_ms)
                )
                / call_count,
                3,
            )
            metrics["avg_executor_ms"] = round(
                ((float(metrics.get("avg_executor_ms", 0.0) or 0.0) * (call_count - 1)) + float(executor_ms)) / call_count,
                3,
            )
            metrics["avg_finalize_ms"] = round(
                ((float(metrics.get("avg_finalize_ms", 0.0) or 0.0) * (call_count - 1)) + float(finalize_ms)) / call_count,
                3,
            )
            metrics["avg_total_ms"] = round(
                ((float(metrics.get("avg_total_ms", 0.0) or 0.0) * (call_count - 1)) + float(total_ms)) / call_count,
                3,
            )
            if subprocess_timings:
                decode_ms = float(subprocess_timings.get("decode_ms", 0.0) or 0.0)
                invoke_ms = float(subprocess_timings.get("invoke_ms", 0.0) or 0.0)
                encode_ms = float(subprocess_timings.get("encode_ms", 0.0) or 0.0)
                queue_wait_ms = max(0.0, float(executor_ms) - decode_ms - invoke_ms - encode_ms)
                alpha = 0.2
                prev_ema = float(metrics.get("ema_child_invoke_ms", 0.0) or 0.0)
                sample_count = int(metrics.get("ema_samples", 0) or 0) + 1
                ema = invoke_ms if sample_count <= 1 else ((alpha * invoke_ms) + ((1.0 - alpha) * prev_ema))
                metrics["last_child_decode_ms"] = round(decode_ms, 3)
                metrics["last_invoke_ms"] = round(invoke_ms, 3)
                metrics["last_child_invoke_ms"] = round(invoke_ms, 3)
                metrics["last_child_encode_ms"] = round(encode_ms, 3)
                metrics["last_queue_wait_ms"] = round(queue_wait_ms, 3)
                metrics["ema_child_invoke_ms"] = round(ema, 3)
                metrics["ema_samples"] = sample_count
                metrics["avg_child_decode_ms"] = round(
                    ((float(metrics.get("avg_child_decode_ms", 0.0) or 0.0) * (call_count - 1)) + decode_ms) / call_count,
                    3,
                )
                metrics["avg_invoke_ms"] = round(
                    ((float(metrics.get("avg_invoke_ms", 0.0) or 0.0) * (call_count - 1)) + invoke_ms) / call_count,
                    3,
                )
                metrics["avg_child_invoke_ms"] = metrics["avg_invoke_ms"]
                metrics["avg_child_encode_ms"] = round(
                    ((float(metrics.get("avg_child_encode_ms", 0.0) or 0.0) * (call_count - 1)) + encode_ms) / call_count,
                    3,
                )
                metrics["avg_queue_wait_ms"] = round(
                    ((float(metrics.get("avg_queue_wait_ms", 0.0) or 0.0) * (call_count - 1)) + queue_wait_ms) / call_count,
                    3,
                )
            else:
                metrics.setdefault("last_child_decode_ms", 0.0)
                metrics.setdefault("last_invoke_ms", 0.0)
                metrics.setdefault("last_child_invoke_ms", metrics.get("last_invoke_ms", 0.0))
                metrics.setdefault("last_child_encode_ms", 0.0)
                metrics.setdefault("last_queue_wait_ms", 0.0)
                metrics.setdefault("ema_child_invoke_ms", 0.0)
                metrics.setdefault("ema_samples", 0)
                metrics.setdefault("avg_child_decode_ms", 0.0)
                metrics.setdefault("avg_invoke_ms", 0.0)
                metrics.setdefault("avg_child_invoke_ms", metrics.get("avg_invoke_ms", 0.0))
                metrics.setdefault("avg_child_encode_ms", 0.0)
                metrics.setdefault("avg_queue_wait_ms", 0.0)
            metrics["last_error_type"] = str(error_type or "")
            metrics["last_error_message"] = str(error_message or "")
            metrics["updated_at"] = utc_now().isoformat()
            pool.timing_metrics = metrics

            event = {
                "event": "task_pool_timing",
                "pool_id": pool.pool_id,
                "pool_name": pool.pool_name,
                "method": str(method or ""),
                "ok": bool(ok),
                "setup_ms": round(float(setup_ms), 3),
                "build_execute_spec_ms": round(float(build_execute_spec_ms), 3),
                "executor_ms": round(float(executor_ms), 3),
                "finalize_ms": round(float(finalize_ms), 3),
                "total_ms": round(float(total_ms), 3),
                "error_type": str(error_type or ""),
                "error_message": str(error_message or ""),
            }
            if subprocess_timings:
                event["subprocess"] = dict(subprocess_timings)
                event["queue_wait_ms"] = round(
                    max(
                        0.0,
                        float(executor_ms)
                        - float(subprocess_timings.get("decode_ms", 0.0) or 0.0)
                        - float(subprocess_timings.get("invoke_ms", 0.0) or 0.0)
                        - float(subprocess_timings.get("encode_ms", 0.0) or 0.0),
                    ),
                    3,
                )
            service_timing_logger.info(json.dumps(event, ensure_ascii=False))
        except Exception as exc:
            logger = logging.getLogger(__name__)
            logger.debug("failed to record task pool timing: %r", exc)

    def service_timing_metadata(self) -> Dict[str, str]:
        with self._lock:
            service_payload: Dict[str, object] = {}
            for session in self._services.values():
                if not session.timing_metrics:
                    continue
                service_payload[session.service_id] = {
                    "service_name": session.service_name,
                    **dict(session.timing_metrics),
                }
            pool_payload: Dict[str, object] = {}
            for pool in self._task_pools.values():
                if not pool.timing_metrics:
                    continue
                pool_payload[pool.pool_id] = {
                    "pool_name": pool.pool_name,
                    "task_method": pool.task_method,
                    **dict(pool.timing_metrics),
                }
        out: Dict[str, str] = {}
        if service_payload:
            out["service_timing_metrics"] = json.dumps(service_payload, ensure_ascii=False, separators=(",", ":"))
        if pool_payload:
            out["task_pool_timing_metrics"] = json.dumps(pool_payload, ensure_ascii=False, separators=(",", ":"))
        return out

    def close(self) -> None:
        self._stop_event.set()
        self._monitor.join(timeout=1.0)
        if self._dispatcher is not None:
            self._dispatcher.join(timeout=1.0)
        if self._service_http_gateway is not None:
            self._service_http_gateway.stop()
        self._shutdown_all_services()
        if self._executor_host is not None:
            self._executor_host.close()

    @property
    def artifact_dir(self) -> Path:
        return self._artifact_dir

    @property
    def object_dir(self) -> Path:
        return self._object_dir

    @property
    def codes_dir(self) -> Path:
        return self._codes_dir

    def _new_managed_globals_state(
        self,
        *,
        code_version: str,
        scope_kind: str,
        scope_key: str,
        allowed_names: Sequence[str],
    ) -> ManagedGlobalsState:
        scopes_dir = _code_globals_dir(self._artifact_dir, code_version=code_version)
        state = ManagedGlobalsState(
            scope_kind=str(scope_kind or "").strip(),
            scope_key=str(scope_key or "").strip(),
            scope_dir=str(_managed_globals_scope_dir(scopes_dir, scope_kind=scope_kind, scope_key=scope_key)),
            allowed_names=_normalize_managed_global_names(allowed_names),
            globals_digest="",
        )
        state.globals_digest = _write_managed_globals_snapshot(state, values_serialized={})
        _write_managed_globals_current(Path(state.scope_dir), globals_digest=state.globals_digest)
        return state

    def _ensure_service_managed_globals_state_locked(self, session: ServiceSession) -> Optional[ManagedGlobalsState]:
        allowed_names = _normalize_managed_global_names(session.managed_global_names)
        if not allowed_names:
            session.managed_globals_scope_dir = ""
            session.managed_globals_digest = ""
            return None
        if not session.managed_globals_scope_dir:
            state = self._new_managed_globals_state(
                code_version=session.code_version,
                scope_kind="service",
                scope_key=session.service_id,
                allowed_names=allowed_names,
            )
            session.managed_globals_scope_dir = state.scope_dir
            session.managed_globals_digest = state.globals_digest
            return state
        return ManagedGlobalsState(
            scope_kind="service",
            scope_key=session.service_id,
            scope_dir=session.managed_globals_scope_dir,
            allowed_names=allowed_names,
            globals_digest=session.managed_globals_digest,
        )

    def _ensure_runtime_managed_globals_state_locked(
        self,
        *,
        client_id: str = "",
        code_version: str,
        runtime_key: str,
        allowed_names: Sequence[str],
    ) -> Optional[ManagedGlobalsState]:
        normalized_allowed_names = _normalize_managed_global_names(allowed_names)
        if not normalized_allowed_names:
            return None
        normalized_key = (
            str(client_id or "").strip(),
            str(code_version or "").strip(),
            str(runtime_key or "").strip(),
        )
        state = self._runtime_managed_globals.get(normalized_key)
        if state is None:
            state = self._new_managed_globals_state(
                code_version=code_version,
                scope_kind="runtime",
                scope_key=f"{self.node_id}|{normalized_key[0]}|{normalized_key[1]}|{normalized_key[2]}",
                allowed_names=normalized_allowed_names,
            )
            self._runtime_managed_globals[normalized_key] = state
        return state

    def _update_managed_globals_state(
        self,
        state: ManagedGlobalsState,
        *,
        values: Dict[str, Any],
    ) -> Tuple[str, List[str]]:
        if not values:
            raise ValueError("managed globals values cannot be empty")
        unknown = [name for name in values if name not in set(state.allowed_names)]
        if unknown:
            raise ValueError(f"managed globals not declared in upload metadata: {unknown}")

        current_values = _load_managed_globals_snapshot_serialized(state)
        updated_names: List[str] = []
        for name, value in values.items():
            if inspect.ismodule(value) or inspect.isfunction(value) or inspect.isclass(value) or callable(value):
                raise ValueError(
                    f"managed globals must be data values, not callables/modules/classes: {[name]}"
                )
            prepared_value = self._prepare_managed_globals_value_for_subprocess_locked(value)
            current_values[name] = serialize_arrow_compatible(prepared_value)
            updated_names.append(name)
        state.globals_digest = _write_managed_globals_snapshot(state, values_serialized=current_values)
        _write_managed_globals_current(Path(state.scope_dir), globals_digest=state.globals_digest)
        return state.globals_digest, sorted(updated_names)

    def _register_client_code_token_locked(
        self,
        *,
        client_id: str,
        code_version: str,
        code_token: str,
    ) -> str:
        normalized_client_id = str(client_id or "").strip()
        normalized_code_version = str(code_version or "").strip()
        normalized_code_token = str(code_token or "").strip()
        if not normalized_client_id:
            raise ValueError("client_id is required for code token registration")
        if not normalized_code_version:
            raise ValueError("code_version is required for code token registration")
        if not normalized_code_token:
            normalized_code_token = secrets.token_urlsafe(24)
        self._client_code_tokens[(normalized_client_id, normalized_code_version)] = normalized_code_token
        return normalized_code_token

    def _register_client_code_managed_globals_locked(
        self,
        *,
        client_id: str,
        code_version: str,
        runtime_key: str = "",
        managed_global_names: Sequence[str],
    ) -> Tuple[str, ...]:
        normalized_client_id = str(client_id or "").strip()
        normalized_code_version = str(code_version or "").strip()
        normalized_runtime_key = str(runtime_key or "").strip()
        normalized_names = _normalize_managed_global_names(managed_global_names)
        if normalized_client_id and normalized_code_version:
            self._client_code_managed_globals[(normalized_client_id, normalized_code_version, normalized_runtime_key)] = normalized_names
        return normalized_names

    @staticmethod
    def _warmup_fanout(worker_count: int) -> int:
        return max(1, int(worker_count or 1) * 2)

    @staticmethod
    def _normalize_warmup_result(result: object, *, fanout: int) -> Tuple[int, List[int]]:
        if isinstance(result, tuple) and len(result) == 2:
            submitted_count, worker_pids = result
        elif isinstance(result, list):
            submitted_count, worker_pids = fanout, result
        else:
            submitted_count, worker_pids = result, []
        normalized_pids = [int(pid) for pid in (worker_pids or []) if int(pid or 0) > 0]
        return max(0, int(submitted_count or 0)), normalized_pids

    def _log_warmup_result(self, *, scope: str, key: str, worker_count: int, submitted_count: int, worker_pids: Sequence[int]) -> None:
        unique_pids = sorted({int(pid) for pid in worker_pids if int(pid or 0) > 0})
        logging.getLogger(__name__).info(
            "[Warmup] scope=%s key=%s worker_count=%d submitted=%d warmed_workers=%d pids=%s",
            scope,
            key,
            int(worker_count or 0),
            int(submitted_count or 0),
            len(unique_pids),
            unique_pids,
        )

    def get_client_code_token(self, *, client_id: str, code_version: str) -> str:
        normalized_client_id = str(client_id or "").strip()
        normalized_code_version = str(code_version or "").strip()
        with self._lock:
            return str(self._client_code_tokens.get((normalized_client_id, normalized_code_version), "") or "")

    def get_client_code_managed_globals(
        self,
        *,
        client_id: str,
        code_version: str,
        runtime_key: str = "",
    ) -> Tuple[str, ...]:
        normalized_client_id = str(client_id or "").strip()
        normalized_code_version = str(code_version or "").strip()
        normalized_runtime_key = str(runtime_key or "").strip()
        with self._lock:
            return self._get_client_code_managed_globals_locked(
                client_id=normalized_client_id,
                code_version=normalized_code_version,
                runtime_key=normalized_runtime_key,
            )

    def _get_client_code_managed_globals_locked(
        self,
        *,
        client_id: str,
        code_version: str,
        runtime_key: str = "",
    ) -> Tuple[str, ...]:
        normalized_client_id = str(client_id or "").strip()
        normalized_code_version = str(code_version or "").strip()
        normalized_runtime_key = str(runtime_key or "").strip()
        exact = self._client_code_managed_globals.get(
            (normalized_client_id, normalized_code_version, normalized_runtime_key),
            (),
        )
        if exact:
            return tuple(exact)
        return tuple(
            self._client_code_managed_globals.get(
                (normalized_client_id, normalized_code_version, ""),
                (),
            )
        )

    def _executor_host_required(self) -> bool:
        return bool(self.enable_internal_executor or self.enable_service_session)

    def _executor_host_alive_locked(self) -> bool:
        return self._executor_host is not None and self._executor_host.is_alive()

    def _delete_object_artifact_locked(self, object_id: str) -> None:
        artifact = self._objects.pop(object_id, None)
        if artifact is None:
            return
        if artifact.storage_backend == "segment":
            return
        if artifact.path:
            with contextlib.suppress(FileNotFoundError):
                Path(artifact.path).unlink()
        with contextlib.suppress(FileNotFoundError):
            _object_meta_path(self._object_dir, object_id=object_id).unlink()

    def _ensure_executor_host_alive_locked(self, *, now: Optional[datetime] = None) -> None:
        if not self._executor_host_required():
            return
        if self._executor_host_alive_locked():
            return

        current_time = now or utc_now()
        old_host = self._executor_host
        self._executor_host = ExecutorHostClient(task_worker_capacity=self.worker_capacity)

        for session in self._services.values():
            if session.status != pb2.SERVICE_STATUS_RUNNING or not session.executor_ready:
                continue
            try:
                self._executor_host.create_service(
                    service_id=session.service_id,
                    worker_count=session.worker_count,
                )
                session.alive_workers = session.worker_count
            except Exception:
                session.executor_ready = False
                session.alive_workers = 0
                session.status = pb2.SERVICE_STATUS_STOPPED
                session.stop_reason = "executor host restart failed"
                session.lease_expire_at = current_time

        for task in self._tasks.values():
            if task.status != pb2.TASK_STATUS_RUNNING:
                continue
            self._handle_infra_failure_locked(
                task,
                reason="executor host restarted during task execution",
                now=current_time,
            )

        if old_host is not None:
            try:
                old_host.close()
            except Exception:
                pass

    def get_object_artifact(self, object_id: str) -> ObjectArtifact:
        normalized = normalize_object_id(object_id)
        with self._lock:
            artifact = self._objects.get(normalized)
            if artifact is not None and _artifact_exists(artifact):
                return artifact
        meta = _load_object_meta(self._object_dir, object_id=normalized)
        if meta:
            artifact = _object_artifact_from_meta(self._object_dir, object_id=normalized, meta=meta)
            if _artifact_exists(artifact):
                with self._lock:
                    self._objects[normalized] = artifact
                return artifact
        candidate = object_storage_path(self._object_dir, object_id=normalized, fmt="bin")
        digest = normalized.replace("sha256:", "", 1)
        legacy_candidate = Path(self._object_dir) / f"{digest}.bin"
        fallback = []
        if candidate.exists():
            artifact = ObjectArtifact(
                object_id=normalized,
                path=str(candidate),
                format=normalize_object_format(candidate.suffix, source_name=candidate.name, default="bin"),
                size_bytes=candidate.stat().st_size,
                created_at=utc_now(),
                storage_backend="file",
            )
            with self._lock:
                self._objects[normalized] = artifact
            return artifact
        if legacy_candidate.exists():
            fallback = [legacy_candidate]
        if not fallback:
            subdir = Path(self._object_dir) / digest[:2]
            fallback = sorted(path for path in subdir.glob(f"{digest[2:]}*") if path.is_file()) if subdir.exists() else []
        if not fallback:
            fallback = sorted(path for path in self._object_dir.glob(f"{digest}*") if path.is_file())
        if fallback:
            path = fallback[0]
            artifact = ObjectArtifact(
                object_id=normalized,
                path=str(path),
                format=normalize_object_format("", source_name=path.name, default="bin"),
                size_bytes=path.stat().st_size,
                created_at=utc_now(),
                storage_backend="file",
            )
            with self._lock:
                self._objects[normalized] = artifact
            return artifact
        raise KeyError("object not found")

    def _resolve_memory_object_refs_in_payload_locked(self, payload: Any) -> Any:
        return payload

    def _prepare_managed_globals_value_for_subprocess_locked(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {
                key: self._prepare_managed_globals_value_for_subprocess_locked(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._prepare_managed_globals_value_for_subprocess_locked(item) for item in value]
        if isinstance(value, tuple):
            return tuple(self._prepare_managed_globals_value_for_subprocess_locked(item) for item in value)
        return value

    def _register_stored_result_artifact_locked(self, result: StoredResultArtifact) -> StoredResultArtifact:
        if result.storage_backend == "segment":
            artifact = ObjectArtifact(
                object_id=result.object_id,
                path="",
                format=result.format,
                size_bytes=result.size_bytes,
                created_at=utc_now(),
                storage_backend="segment",
                segment_path=str(_segment_path_from_relpath(self._object_dir, result.segment_relpath)),
                segment_offset=result.segment_offset,
                segment_length=result.segment_length,
            )
        else:
            artifact = ObjectArtifact(
                object_id=result.object_id,
                path=str(object_storage_path(self._object_dir, object_id=result.object_id, fmt=result.format)),
                format=result.format,
                size_bytes=result.size_bytes,
                created_at=utc_now(),
                storage_backend="file",
            )
        self._objects[result.object_id] = artifact
        return result

    def _dependency_dir_for_code_version(self, code_version: str) -> Path:
        return _code_dependency_dir(self._artifact_dir, code_version=code_version)

    def _get_live_code_artifact_locked(self, code_version: str) -> Optional[CodeArtifact]:
        normalized_code_version = str(code_version or "").strip()
        if not normalized_code_version:
            return None
        artifact = self._codes.get(normalized_code_version)
        if artifact is None:
            return None
        if _code_artifact_exists(artifact):
            return artifact
        self._codes.pop(normalized_code_version, None)
        self._client_code_tokens = {
            key: value for key, value in self._client_code_tokens.items() if key[1] != normalized_code_version
        }
        self._client_code_managed_globals = {
            key: value for key, value in self._client_code_managed_globals.items() if key[1] != normalized_code_version
        }
        return None

    def _validate_managed_global_names(self, managed_global_names: Sequence[str], *, module: Any) -> None:
        normalized_names = _normalize_managed_global_names(managed_global_names)
        if not normalized_names:
            return
        missing = [name for name in normalized_names if not hasattr(module, name)]
        if missing:
            raise ValueError(f"managed globals not found in entry module: {missing}")

    def _validate_artifact_methods(
        self,
        artifact: CodeArtifact,
        *,
        dependency_path: str,
        managed_global_names: Sequence[str] = (),
    ) -> Dict[str, Tuple[str, str]]:
        module = None
        try:
            module, methods = _discover_callable_methods(
                artifact.path,
                entry_module=artifact.entry_module,
                package_format=artifact.package_format,
                dependency_path=dependency_path,
                export_mode=artifact.export_mode,
                export_methods=artifact.export_methods,
                export_decorator=artifact.export_decorator,
                entry_callable=artifact.entry_callable,
            )
            self._validate_managed_global_names(managed_global_names, module=module)
            return methods
        finally:
            _purge_loaded_artifact_modules(
                artifact.path,
                entry_module=artifact.entry_module,
                package_format=artifact.package_format,
                dependency_path=dependency_path,
            )

    def _ensure_artifact_ready(
        self,
        artifact: CodeArtifact,
        *,
        dependency_allowlist: Sequence[str],
        managed_global_names: Sequence[str] = (),
    ) -> Dict[str, Tuple[str, str]]:
        normalized_allowlist = _normalize_dependency_allowlist(dependency_allowlist)
        installed_dependency_path = str(artifact.dependency_path or "").strip()
        effective_allowlist = _normalize_dependency_allowlist(
            [*artifact.dependency_allowlist, *normalized_allowlist]
        )

        created_dir = False
        candidate_dependency_path = installed_dependency_path
        if effective_allowlist and (not candidate_dependency_path or effective_allowlist != artifact.dependency_allowlist):
            target_dir = self._dependency_dir_for_code_version(artifact.code_version)
            try:
                _install_dependency_allowlist(effective_allowlist, target_dir=target_dir)
            except Exception as install_exc:
                if _is_user_artifact_error(install_exc):
                    raise ValueError(
                        _describe_artifact_error(
                            install_exc,
                            entry_module=artifact.entry_module,
                            entry_callable=artifact.entry_callable,
                            package_format=artifact.package_format,
                        )
                    ) from install_exc
                raise
            created_dir = True
            candidate_dependency_path = str(target_dir)

        try:
            method_info = self._validate_artifact_methods(
                artifact,
                dependency_path=candidate_dependency_path,
                managed_global_names=managed_global_names,
            )
        except Exception as exc:
            if not effective_allowlist or not _missing_import_name(exc):
                if created_dir:
                    shutil.rmtree(candidate_dependency_path, ignore_errors=True)
                if _is_user_artifact_error(exc):
                    raise ValueError(
                        _describe_artifact_error(
                            exc,
                            entry_module=artifact.entry_module,
                            entry_callable=artifact.entry_callable,
                            package_format=artifact.package_format,
                        )
                    ) from exc
                raise

            target_dir = self._dependency_dir_for_code_version(artifact.code_version)
            try:
                _install_dependency_allowlist(effective_allowlist, target_dir=target_dir)
                method_info = self._validate_artifact_methods(
                    artifact,
                    dependency_path=str(target_dir),
                    managed_global_names=managed_global_names,
                )
            except Exception as repair_exc:
                if created_dir or target_dir.exists():
                    shutil.rmtree(target_dir, ignore_errors=True)
                if _is_user_artifact_error(repair_exc):
                    raise ValueError(
                        _describe_artifact_error(
                            repair_exc,
                            entry_module=artifact.entry_module,
                            entry_callable=artifact.entry_callable,
                            package_format=artifact.package_format,
                        )
                    ) from repair_exc
                raise

            artifact.dependency_allowlist = effective_allowlist
            artifact.dependency_path = str(target_dir)
            _write_code_meta(self._artifact_dir, artifact)
            return method_info

        if effective_allowlist and candidate_dependency_path:
            artifact.dependency_allowlist = effective_allowlist
            artifact.dependency_path = candidate_dependency_path
            _write_code_meta(self._artifact_dir, artifact)
        return method_info

    def service_worker_used(self) -> int:
        with self._lock:
            active = sum(
                max(0, int(session.worker_count))
                for session in self._services.values()
                if session.status in (
                    pb2.SERVICE_STATUS_STARTING,
                    pb2.SERVICE_STATUS_RUNNING,
                    pb2.SERVICE_STATUS_DRAINING,
                )
            )
            return active + max(0, int(self._service_worker_reserved))

    def service_worker_available(self) -> int:
        return max(0, int(self.service_worker_capacity) - int(self.service_worker_used()))

    @staticmethod
    def _service_inflight_locked(session: ServiceSession) -> int:
        return max(0, int(session.request_count or 0) - int(session.returned_count or 0))

    @staticmethod
    def _task_pool_inflight_locked(pool: TaskPoolState) -> int:
        return max(0, int(pool.task_count or 0) - int(pool.returned_count or 0))

    def task_pool_worker_used(self) -> int:
        with self._lock:
            active = sum(
                max(0, int(pool.worker_count))
                for pool in self._task_pools.values()
                if str(pool.status or "").strip().upper() == "RUNNING"
            )
            return active + max(0, int(self._task_pool_worker_reserved))

    def task_pool_worker_available(self) -> int:
        return max(0, int(self.task_pool_worker_capacity) - int(self.task_pool_worker_used()))

    def task_pool_reports(self) -> Dict[str, NodeTaskPoolInfo]:
        with self._lock:
            inflight_by_pool: Dict[str, int] = {}
            for task in self._pool_tasks.values():
                if int(task.status) != int(pb2.TASK_STATUS_RUNNING):
                    continue
                pool_id = str(task.client_id or "").strip()
                if not pool_id:
                    continue
                inflight_by_pool[pool_id] = inflight_by_pool.get(pool_id, 0) + 1
            return {
                pool.pool_id: NodeTaskPoolInfo(
                    pool_id=pool.pool_id,
                    owner_client_id=pool.owner_client_id,
                    pool_name=pool.pool_name,
                    code_version=pool.code_version,
                    status=pool.status,
                    worker_count=pool.worker_count,
                    task_count=pool.task_count,
                    inflight=self._task_pool_inflight_locked(pool),
                    created_at=pool.created_at,
                    last_heartbeat_at=pool.last_heartbeat_at,
                    lease_expire_at=pool.lease_expire_at,
                )
                for pool in self._task_pools.values()
                if str(pool.status or "").strip().upper() == "RUNNING" or bool(pool.timing_metrics)
            }

    def _get_code_write_lock(self, code_version: str) -> threading.Lock:
        key = str(code_version or "").strip()
        with self._lock:
            lock = self._code_write_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._code_write_locks[key] = lock
            return lock

    def _get_code_content_write_lock(self, code_version: str) -> threading.Lock:
        key = f"content:{_code_content_storage_key(code_version)}"
        with self._lock:
            lock = self._code_write_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._code_write_locks[key] = lock
            return lock

    def _get_object_write_lock(self, object_id: str) -> threading.Lock:
        key = str(object_id or "").strip()
        with self._lock:
            lock = self._object_write_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._object_write_locks[key] = lock
            return lock

    def task_pool(self, pool_id: str) -> TaskPoolState:
        normalized = str(pool_id or "").strip()
        with self._lock:
            pool = self._task_pools.get(normalized)
            if pool is None:
                raise KeyError("task pool not found")
            return pool

    def task_pool_status_info(self, pool_id: str) -> Dict[str, object]:
        pool = self.task_pool(pool_id)
        inflight = 0
        with self._lock:
            for task in self._pool_tasks.values():
                if str(task.client_id or "").strip() != pool.pool_id:
                    continue
                if int(task.status) == int(pb2.TASK_STATUS_RUNNING):
                    inflight += 1
        return {
            "pool_id": pool.pool_id,
            "owner_client_id": pool.owner_client_id,
            "pool_name": pool.pool_name,
            "code_version": pool.code_version,
            "task_method": pool.task_method,
            "worker_count": pool.worker_count,
            "heartbeat_timeout_sec": pool.heartbeat_timeout_sec,
            "status": str(pool.status),
            "task_count": int(pool.task_count),
            "inflight": int(inflight),
            "created_at": pool.created_at,
            "last_heartbeat_at": pool.last_heartbeat_at,
            "lease_expire_at": pool.lease_expire_at,
            "timing_metrics": dict(pool.timing_metrics or {}),
        }

    def _extract_archive(self, *, archive_path: Path, package_format: str, out_dir: Path) -> None:
        if out_dir.exists():
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        root = out_dir.resolve()

        def _safe_join(name: str) -> Path:
            candidate = (root / name).resolve()
            if candidate != root and root not in candidate.parents:
                raise ValueError(f"archive path escapes destination: {name}")
            return candidate

        if package_format in ("zip", "whl"):
            with zipfile.ZipFile(archive_path, "r") as zf:
                for info in zf.infolist():
                    _safe_join(info.filename)
                zf.extractall(out_dir)
            return

        if package_format == "tar.gz":
            with tarfile.open(archive_path, "r:gz") as tf:
                for member in tf.getmembers():
                    _safe_join(member.name)
                tf.extractall(out_dir)
            return

        raise ValueError(f"unsupported package format for extraction: {package_format}")

    def put_code_from_uploaded_file(
        self,
        *,
        client_id: str,
        sha256: str,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str = "",
        export_mode: str = "",
        export_methods: Sequence[str] = (),
        export_decorator: str = "",
        dependency_allowlist: Sequence[str] = (),
        managed_global_names: Sequence[str] = (),
        code_token: str = "",
        uploaded_path: str,
        actual_sha256: str,
        size_bytes: int,
        validate_load: bool = False,
    ) -> Tuple[CodeArtifact, bool]:
        expected = str(sha256 or "").replace("sha256:", "").strip().lower()
        digest = str(actual_sha256 or "").strip().lower()
        if not digest:
            raise ValueError("empty uploaded artifact")
        if expected and expected != digest:
            raise ValueError(f"sha256 mismatch: expected={expected}, actual={digest}")

        normalized_format = _normalize_package_format(package_format, uploaded_path)
        normalized_runtime = _validate_python_runtime_or_raise(
            node_python_version=self.python_version,
            runtime=runtime,
        )
        normalized_callable = str(entry_callable or "").strip() or "run"
        normalized_module = str(entry_module or "").strip()
        if not normalized_module and normalized_format == "py":
            normalized_module = "artifact"
        if normalized_format in ("tar.gz", "zip", "whl") and not normalized_module:
            raise ValueError(f"entry_module is required for {normalized_format} artifact")
        if normalized_format == "bin":
            raise ValueError("unsupported package_format; expected py/tar.gz/zip/whl")

        normalized_export_mode, normalized_export_methods, normalized_export_decorator = _normalize_export_spec(
            mode=export_mode,
            methods=export_methods,
            decorator=export_decorator,
            entry_callable=normalized_callable,
        )
        normalized_dependency_allowlist = _normalize_dependency_allowlist(dependency_allowlist)
        normalized_managed_global_names = _normalize_managed_global_names(managed_global_names)
        code_version = _code_version_from_digest(
            digest,
            runtime=normalized_runtime,
            entry_module=normalized_module,
            entry_callable=normalized_callable,
            package_format=normalized_format,
            export_mode=normalized_export_mode,
            export_methods=normalized_export_methods,
            export_decorator=normalized_export_decorator,
            dependency_allowlist=normalized_dependency_allowlist,
        )
        content_lock = self._get_code_content_write_lock(code_version)
        variant_lock = self._get_code_write_lock(code_version)
        with content_lock, variant_lock:
            with self._lock:
                existing = self._codes.get(code_version)
                if existing is not None:
                    if validate_load:
                        self._ensure_artifact_ready(
                            existing,
                            dependency_allowlist=normalized_dependency_allowlist,
                            managed_global_names=normalized_managed_global_names,
                        )
                    if str(client_id or "").strip():
                        self._register_client_code_token_locked(
                            client_id=client_id,
                            code_version=code_version,
                            code_token=code_token,
                        )
                        self._register_client_code_managed_globals_locked(
                            client_id=client_id,
                            code_version=code_version,
                            runtime_key="",
                            managed_global_names=normalized_managed_global_names,
                        )
                    return existing, True

            tmp_path = Path(uploaded_path)
            if not tmp_path.exists():
                raise ValueError(f"uploaded file missing: {uploaded_path}")

            now = utc_now()
            code_dir = _code_content_dir(self._artifact_dir, code_version=code_version)
            variant_dir = _code_variant_dir(self._artifact_dir, code_version=code_version)
            cleanup_paths: List[Path] = [variant_dir]
            code_dir.mkdir(parents=True, exist_ok=True)
            variant_dir.mkdir(parents=True, exist_ok=True)
            _code_data_dir(self._artifact_dir, code_version=code_version).mkdir(parents=True, exist_ok=True)
            if normalized_format == "py":
                final_path = _code_exec_path(self._artifact_dir, code_version=code_version, package_format=normalized_format)
                final_path.parent.mkdir(parents=True, exist_ok=True)
                if final_path.exists():
                    tmp_path.unlink(missing_ok=True)
                else:
                    os.replace(str(tmp_path), str(final_path))
                artifact_exec_path = str(final_path)
            else:
                archive_path = _code_archive_path(self._artifact_dir, code_version=code_version, package_format=normalized_format)
                if archive_path.exists():
                    tmp_path.unlink(missing_ok=True)
                else:
                    os.replace(str(tmp_path), str(archive_path))
                extract_dir = _code_exec_path(self._artifact_dir, code_version=code_version, package_format=normalized_format)
                if not extract_dir.exists():
                    self._extract_archive(archive_path=archive_path, package_format=normalized_format, out_dir=extract_dir)
                artifact_exec_path = str(extract_dir)

            artifact = CodeArtifact(
                code_version=code_version,
                path=artifact_exec_path,
                runtime=normalized_runtime,
                entry_module=normalized_module,
                entry_callable=normalized_callable,
                package_format=normalized_format,
                export_mode=normalized_export_mode,
                export_methods=normalized_export_methods,
                export_decorator=normalized_export_decorator,
                dependency_allowlist=normalized_dependency_allowlist,
                dependency_path="",
                size_bytes=max(0, int(size_bytes)),
                created_at=now,
            )
            if validate_load:
                try:
                    self._ensure_artifact_ready(
                        artifact,
                        dependency_allowlist=normalized_dependency_allowlist,
                        managed_global_names=normalized_managed_global_names,
                    )
                except Exception:
                    for target in cleanup_paths:
                        if target.is_dir():
                            shutil.rmtree(target, ignore_errors=True)
                        else:
                            target.unlink(missing_ok=True)
                    if artifact.dependency_path:
                        shutil.rmtree(artifact.dependency_path, ignore_errors=True)
                    raise
            with self._lock:
                self._codes[code_version] = artifact
                if str(client_id or "").strip():
                    self._register_client_code_token_locked(
                        client_id=client_id,
                        code_version=code_version,
                        code_token=code_token,
                    )
                    self._register_client_code_managed_globals_locked(
                        client_id=client_id,
                        code_version=code_version,
                        runtime_key="",
                        managed_global_names=normalized_managed_global_names,
                    )
            _write_code_meta(self._artifact_dir, artifact)
            return artifact, False

    def put_object_from_uploaded_file(
        self,
        *,
        object_id: str,
        format: str = "",
        uploaded_path: str,
        actual_sha256: str,
        size_bytes: int,
    ) -> Tuple[ObjectArtifact, bool]:
        expected = normalize_object_id(object_id)
        digest = str(actual_sha256 or "").strip().lower()
        if not digest:
            raise ValueError("empty uploaded object")
        actual_object_id = object_id_from_sha256_hex(digest)
        if expected and expected != actual_object_id:
            raise ValueError(f"sha256 mismatch: expected={expected}, actual={actual_object_id}")

        tmp_path = Path(uploaded_path)
        if not tmp_path.exists():
            raise ValueError(f"uploaded object missing: {uploaded_path}")

        normalized_format = normalize_object_format(format, source_name=uploaded_path, default="bin")
        object_lock = self._get_object_write_lock(actual_object_id)
        with object_lock:
            with self._lock:
                existing = self._objects.get(actual_object_id)
                if existing is not None and _artifact_exists(existing):
                    return existing, True
            meta = _load_object_meta(self._object_dir, object_id=actual_object_id)
            if meta:
                artifact = _object_artifact_from_meta(self._object_dir, object_id=actual_object_id, meta=meta)
                if _artifact_exists(artifact):
                    with self._lock:
                        self._objects[actual_object_id] = artifact
                    return artifact, True

            now = utc_now()
            if max(0, int(size_bytes or 0)) <= max(0, int(self._object_segment_max_bytes)):
                result = _append_bytes_to_segment(
                    self._object_dir,
                    object_id=actual_object_id,
                    fmt=normalized_format,
                    blob=tmp_path.read_bytes(),
                    materialize_as="path",
                    created_at=now,
                )
                tmp_path.unlink(missing_ok=True)
                artifact = ObjectArtifact(
                    object_id=actual_object_id,
                    path="",
                    format=normalized_format,
                    size_bytes=result.size_bytes,
                    created_at=now,
                    storage_backend="segment",
                    segment_path=str(_segment_path_from_relpath(self._object_dir, result.segment_relpath)),
                    segment_offset=result.segment_offset,
                    segment_length=result.segment_length,
                )
                with self._lock:
                    self._objects[actual_object_id] = artifact
                return artifact, False

            final_path = object_storage_path(self._object_dir, object_id=actual_object_id, fmt=normalized_format)
            final_path.parent.mkdir(parents=True, exist_ok=True)
            if final_path.exists():
                try:
                    tmp_path.unlink(missing_ok=True)
                except OSError:
                    pass
                _write_object_meta(
                    self._object_dir,
                    object_id=actual_object_id,
                    fmt=normalized_format,
                    size_bytes=max(0, int(size_bytes)),
                    created_at=now,
                    last_at=now,
                )
                artifact = ObjectArtifact(
                    object_id=actual_object_id,
                    path=str(final_path),
                    format=normalized_format,
                    size_bytes=max(0, int(size_bytes)),
                    created_at=now,
                    storage_backend="file",
                )
                with self._lock:
                    self._objects[actual_object_id] = artifact
                return artifact, True

            os.replace(str(tmp_path), str(final_path))
            _write_object_meta(
                self._object_dir,
                object_id=actual_object_id,
                fmt=normalized_format,
                size_bytes=max(0, int(size_bytes)),
                created_at=now,
                last_at=now,
            )
            artifact = ObjectArtifact(
                object_id=actual_object_id,
                path=str(final_path),
                format=normalized_format,
                size_bytes=max(0, int(size_bytes)),
                created_at=now,
                storage_backend="file",
            )
            with self._lock:
                self._objects[actual_object_id] = artifact
            return artifact, False

    def put_code(
        self,
        *,
        client_id: str = "",
        sha256: str,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str = "",
        export_mode: str = "",
        export_methods: Optional[Sequence[str]] = None,
        export_decorator: str = "",
        dependency_allowlist: Sequence[str] = (),
        managed_global_names: Sequence[str] = (),
        code_token: str = "",
        chunks: Iterable[bytes],
        validate_load: bool = False,
    ) -> Tuple[CodeArtifact, bool]:
        h = hashlib.sha256()
        size = 0
        suffix = _package_suffix(package_format)
        fd, tmp_name = tempfile.mkstemp(prefix="pycloud-upload-", suffix=suffix, dir=str(self._artifact_dir))
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            with tmp_path.open("wb") as fp:
                for part in chunks:
                    if not part:
                        continue
                    h.update(part)
                    fp.write(part)
                    size += len(part)
            return self.put_code_from_uploaded_file(
                client_id=client_id,
                sha256=sha256,
                runtime=runtime,
                entry_module=entry_module,
                entry_callable=entry_callable,
                package_format=package_format,
                export_mode=export_mode,
                export_methods=list(export_methods or ()),
                export_decorator=export_decorator,
                dependency_allowlist=dependency_allowlist,
                managed_global_names=managed_global_names,
                code_token=code_token,
                uploaded_path=str(tmp_path),
                actual_sha256=h.hexdigest(),
                size_bytes=size,
                validate_load=validate_load,
            )
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

    def has_code_version(self, code_version: str) -> bool:
        with self._lock:
            return code_version in self._codes

    def create_service(
        self,
        *,
        owner_client_id: str,
        service_name: str,
        sha256: str,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str = "",
        export_mode: str = "",
        export_methods: Sequence[str] = (),
        export_decorator: str = "",
        dependency_allowlist: Sequence[str] = (),
        managed_global_names: Sequence[str] = (),
        worker_count: int,
        heartbeat_timeout_sec: int,
        idle_ttl_sec: int,
        expose_http: bool,
        chunks: Iterable[bytes],
    ) -> ServiceSession:
        if not owner_client_id:
            raise ValueError("owner_client_id is required")
        normalized_managed_global_names = _normalize_managed_global_names(managed_global_names)

        artifact, _cached = self.put_code(
            client_id=owner_client_id,
            sha256=sha256,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            export_mode=export_mode,
            export_methods=export_methods,
            export_decorator=export_decorator,
            dependency_allowlist=dependency_allowlist,
            chunks=chunks,
            validate_load=True,
        )
        method_info = self._ensure_artifact_ready(
            artifact,
            dependency_allowlist=dependency_allowlist,
            managed_global_names=normalized_managed_global_names,
        )

        requested_workers = max(1, worker_count or self.service_default_worker_count)
        actual_hb_timeout = max(5, heartbeat_timeout_sec or self.service_default_heartbeat_timeout_sec)
        actual_idle_ttl = max(0, idle_ttl_sec)
        now = utc_now()
        service_id = uuid.uuid4().hex
        token = secrets.token_urlsafe(24)
        http_base = f"{self.service_http_base_url}/svc/{service_id}" if (expose_http and self.service_http_base_url) else ""

        reserved = 0
        with self._lock:
            active = sum(
                max(0, int(session.worker_count))
                for session in self._services.values()
                if session.status in (
                    pb2.SERVICE_STATUS_STARTING,
                    pb2.SERVICE_STATUS_RUNNING,
                    pb2.SERVICE_STATUS_DRAINING,
                )
            )
            available_workers = max(0, int(self.service_worker_capacity) - int(active + self._service_worker_reserved))
            if available_workers <= 0:
                raise RuntimeError("service worker capacity exhausted")
            actual_workers = min(requested_workers, available_workers)
            self._service_worker_reserved += actual_workers
            reserved = actual_workers
            self._ensure_executor_host_alive_locked(now=now)
            executor_host = self._executor_host
        if executor_host is None:
            with self._lock:
                self._service_worker_reserved = max(0, self._service_worker_reserved - reserved)
            raise RuntimeError("executor host unavailable")
        try:
            executor_host.create_service(service_id=service_id, worker_count=actual_workers)
            session = ServiceSession(
                service_id=service_id,
                owner_client_id=owner_client_id,
                service_name=service_name or f"service-{service_id[:8]}",
                code_version=artifact.code_version,
                worker_count=actual_workers,
                heartbeat_timeout_sec=actual_hb_timeout,
                idle_ttl_sec=actual_idle_ttl,
                expose_http=bool(expose_http),
                service_token=token,
                http_base_url=http_base,
                status=pb2.SERVICE_STATUS_RUNNING,
                created_at=now,
                last_heartbeat_at=now,
                lease_expire_at=now + timedelta(seconds=actual_hb_timeout),
                executor_ready=True,
                alive_workers=actual_workers,
                methods=method_info,
                managed_global_names=normalized_managed_global_names,
            )
            managed_state = self._ensure_service_managed_globals_state_locked(session)
            if managed_state is not None:
                session.managed_globals_scope_dir = managed_state.scope_dir
                session.managed_globals_digest = managed_state.globals_digest
            with self._lock:
                self._services[service_id] = session
                if reserved:
                    self._service_worker_reserved = max(0, self._service_worker_reserved - reserved)
            return session
        except Exception:
            with self._lock:
                if reserved:
                    self._service_worker_reserved = max(0, self._service_worker_reserved - reserved)
            raise

    def create_task_pool(
        self,
        *,
        owner_client_id: str,
        pool_name: str,
        sha256: str,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str = "",
        dependency_allowlist: Sequence[str] = (),
        managed_global_names: Sequence[str] = (),
        worker_count: int,
        heartbeat_timeout_sec: int,
        idle_ttl_sec: int,
        chunks: Iterable[bytes],
    ) -> TaskPoolState:
        if not owner_client_id:
            raise ValueError("owner_client_id is required")
        normalized_managed_global_names = _normalize_managed_global_names(managed_global_names)
        artifact, _cached = self.put_code(
            client_id=owner_client_id,
            sha256=sha256,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            export_mode="single",
            export_methods=[entry_callable],
            dependency_allowlist=dependency_allowlist,
            chunks=chunks,
            validate_load=True,
        )
        self._ensure_artifact_ready(
            artifact,
            dependency_allowlist=dependency_allowlist,
            managed_global_names=normalized_managed_global_names,
        )

        requested_workers = max(1, int(worker_count or self.worker_capacity or 1))
        now = utc_now()
        pool_id = uuid.uuid4().hex
        token = secrets.token_urlsafe(24)
        reserved = 0
        with self._lock:
            active = sum(
                max(0, int(pool.worker_count))
                for pool in self._task_pools.values()
                if str(pool.status or "").strip().upper() == "RUNNING"
            )
            available_workers = max(0, int(self.task_pool_worker_capacity) - int(active + self._task_pool_worker_reserved))
            if available_workers <= 0:
                raise RuntimeError("task pool worker capacity exhausted")
            actual_workers = min(requested_workers, available_workers)
            self._task_pool_worker_reserved += actual_workers
            reserved = actual_workers
            self._ensure_executor_host_alive_locked(now=now)
            executor_host = self._executor_host
        if executor_host is None:
            with self._lock:
                self._task_pool_worker_reserved = max(0, self._task_pool_worker_reserved - reserved)
            raise RuntimeError("executor host unavailable")
        try:
            executor_host.create_task_pool(pool_id=pool_id, worker_count=actual_workers)
        except Exception:
            with self._lock:
                self._task_pool_worker_reserved = max(0, self._task_pool_worker_reserved - reserved)
            raise
        try:
            pool = TaskPoolState(
                pool_id=pool_id,
                owner_client_id=owner_client_id,
                pool_name=str(pool_name or f"task-pool-{pool_id[:8]}"),
                code_version=artifact.code_version,
                task_method=str(entry_callable or "run").strip() or "run",
                worker_count=actual_workers,
                heartbeat_timeout_sec=max(5, int(heartbeat_timeout_sec or 30)),
                idle_ttl_sec=max(0, int(idle_ttl_sec or 0)),
                pool_token=token,
                status="RUNNING",
                created_at=now,
                last_heartbeat_at=now,
                lease_expire_at=now + timedelta(seconds=max(5, int(heartbeat_timeout_sec or 30))),
                managed_global_names=normalized_managed_global_names,
                executor_ready=True,
                task_count=0,
            )
            managed_state = self._ensure_runtime_managed_globals_state_locked(
                client_id=pool.pool_id,
                code_version=pool.code_version,
                runtime_key=pool.pool_id,
                allowed_names=pool.managed_global_names,
            )
            if managed_state is not None:
                pool.managed_globals_scope_dir = managed_state.scope_dir
                pool.managed_globals_digest = managed_state.globals_digest
            with self._lock:
                self._task_pool_worker_reserved = max(0, self._task_pool_worker_reserved - reserved)
                self._task_pools[pool_id] = pool
                self._register_client_code_token_locked(
                    client_id=pool.pool_id,
                    code_version=pool.code_version,
                    code_token=pool.pool_token,
                )
                self._register_client_code_managed_globals_locked(
                    client_id=pool.pool_id,
                    code_version=pool.code_version,
                    runtime_key=pool.pool_id,
                    managed_global_names=pool.managed_global_names,
                )
            return pool
        except Exception:
            with self._lock:
                self._task_pool_worker_reserved = max(0, self._task_pool_worker_reserved - reserved)
            with contextlib.suppress(Exception):
                executor_host.stop_task_pool(pool_id=pool_id)
            raise

    def submit_pool_tasks(
        self,
        *,
        pool_id: str,
        pool_token: str,
        tasks: Sequence[pb2.TaskSubmitItem],
        job_id: str = "",
    ) -> Tuple[List[pb2.TaskAccepted], List[pb2.TaskRejected]]:
        accepted: List[pb2.TaskAccepted] = []
        rejected: List[pb2.TaskRejected] = []
        log_payload_flow(
            "taskpool_submit_state",
            pool_id=str(pool_id or "").strip(),
            task_count=len(tasks),
            job_id=str(job_id or "").strip(),
        )
        now = utc_now()
        with self._cv:
            pool = self._task_pools.get(str(pool_id or "").strip())
            if pool is None:
                raise KeyError("task pool not found")
            if not pool.pool_token or pool.pool_token != str(pool_token or "").strip():
                raise PermissionError("pool_token mismatch")
            if pool.status != "RUNNING":
                raise RuntimeError("task pool not running")
            artifact = self._codes.get(pool.code_version)
            if artifact is None:
                raise RuntimeError("code artifact missing")
            for item in tasks:
                if item.task_id in self._pool_tasks:
                    rejected.append(
                        pb2.TaskRejected(
                            task_id=item.task_id,
                            code=pb2.ERROR_CODE_DUPLICATE_TASK,
                            message="duplicate task_id",
                        )
                    )
                    continue
                record = TaskState(
                    task_id=item.task_id,
                    client_id=pool.pool_id,
                    job_id=str(job_id or "").strip(),
                    code_version=pool.code_version,
                    runtime_key=str(item.runtime_key or "").strip(),
                    execution_mode=pb2.EXECUTION_MODE_PERSISTENT,
                    payload=struct_to_dict(item.payload),
                    timeout_hint_sec=max(0, item.timeout_hint_sec),
                    priority=max(1, item.priority or 1),
                    status=pb2.TASK_STATUS_RUNNING,
                    attempt=1,
                    worker_id=f"task-pool:{pool.pool_id}",
                    lease_id=str(uuid.uuid4()),
                    started_at=now,
                    last_heartbeat_at=now,
                )
                self._pool_tasks[item.task_id] = record
                build_start = time.perf_counter()
                execute_spec = _build_execute_spec(
                    artifact,
                    object_dir=self._object_dir,
                    work_dir=_code_data_dir(self._artifact_dir, code_version=artifact.code_version),
                    method_name=artifact.entry_callable,
                    payload=self._resolve_memory_object_refs_in_payload_locked(record.payload),
                    managed_globals_scope_dir=pool.managed_globals_scope_dir,
                    managed_globals_digest=pool.managed_globals_digest,
                )
                record.dispatch_build_execute_spec_ms = (time.perf_counter() - build_start) * 1000.0
                self._executor_host.submit_pool_task(
                    pool_id=pool.pool_id,
                    task_id=item.task_id,
                    attempt=record.attempt,
                    execute_spec=execute_spec,
                )
                pool.task_count += 1
                accepted.append(pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED))
        log_payload_flow(
            "taskpool_submit_state_result",
            pool_id=str(pool_id or "").strip(),
            accepted=len(accepted),
            rejected=len(rejected),
        )
        return accepted, rejected

    def pull_pool_results(
        self,
        *,
        pool_id: str,
        pool_token: str,
        limit: int = 100,
        wait_ms: int = 0,
        cursor: str = "",
    ) -> Tuple[List[pb2.TaskResult], str]:
        pool = self.task_pool(pool_id)
        if pool.pool_token != str(pool_token or "").strip():
            raise PermissionError("pool_token mismatch")
        results, next_cursor = self._pool_result_hook.pull(
            pool.pool_id,
            limit=max(1, int(limit or 100)),
            wait_ms=max(0, int(wait_ms or 0)),
            cursor=cursor,
        )
        log_payload_flow(
            "taskpool_pull_results_state",
            pool_id=str(pool_id or "").strip(),
            result_count=len(results),
            next_cursor=next_cursor,
        )
        return results, next_cursor

    def close_task_pool(
        self,
        *,
        owner_client_id: str,
        pool_id: str,
        pool_token: str,
        reason: str = "",
    ) -> TaskPoolState:
        del reason
        normalized = str(pool_id or "").strip()
        with self._lock:
            pool = self._task_pools.get(normalized)
            if pool is None:
                raise KeyError("task pool not found")
            if pool.owner_client_id != str(owner_client_id or "").strip():
                raise PermissionError("owner_client_id mismatch")
            if pool.pool_token != str(pool_token or "").strip():
                raise PermissionError("pool_token mismatch")
            if self._executor_host is not None and pool.executor_ready:
                self._executor_host.stop_task_pool(pool_id=pool.pool_id)
            pool.executor_ready = False
            pool.status = "STOPPED"
            pool.lease_expire_at = utc_now()
            return pool

    def cancel_pool_job(
        self,
        *,
        pool_id: str,
        pool_token: str,
        job_id: str,
        reason: str = "",
    ) -> Tuple[int, int, int, int]:
        normalized_pool_id = str(pool_id or "").strip()
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            raise ValueError("job_id is required")
        with self._cv:
            pool = self._task_pools.get(normalized_pool_id)
            if pool is None:
                raise KeyError("task pool not found")
            if pool.pool_token != str(pool_token or "").strip():
                raise PermissionError("pool_token mismatch")
            queued_cancelled = 0
            running_marked = 0
            already_done = 0
            matched = 0
            for task in self._pool_tasks.values():
                if task.client_id != normalized_pool_id:
                    continue
                if task.job_id != normalized_job_id:
                    continue
                matched += 1
                if task.status in (
                    pb2.TASK_STATUS_SUCCEEDED,
                    pb2.TASK_STATUS_FAILED_USER,
                    pb2.TASK_STATUS_FAILED_INFRA,
                    pb2.TASK_STATUS_CANCELLED,
                ):
                    already_done += 1
                    continue
                task.cancel_requested = True
                if task.status == pb2.TASK_STATUS_QUEUED:
                    task.status = pb2.TASK_STATUS_CANCELLED
                    task.finished_at = utc_now()
                    task.error_type = "Cancelled"
                    task.error_message = reason or f"cancelled by pool job_id={normalized_job_id}"
                    pool.returned_count += 1
                    self._pool_result_hook.push(normalized_pool_id, task.as_result())
                    queued_cancelled += 1
                elif task.status == pb2.TASK_STATUS_RUNNING:
                    running_marked += 1
            if queued_cancelled or running_marked:
                self._cv.notify_all()
            not_found = 0 if matched else 1
            return queued_cancelled, running_marked, already_done, not_found

    def heartbeat_task_pool(
        self,
        *,
        owner_client_id: str,
        pool_id: str,
        pool_token: str,
    ) -> TaskPoolState:
        normalized = str(pool_id or "").strip()
        with self._lock:
            pool = self._task_pools.get(normalized)
            if pool is None:
                raise KeyError("task pool not found")
            if pool.owner_client_id != str(owner_client_id or "").strip():
                raise PermissionError("owner_client_id mismatch")
            if pool.pool_token != str(pool_token or "").strip():
                raise PermissionError("pool_token mismatch")
            if pool.status != "RUNNING":
                raise RuntimeError("task pool not running")
            now = utc_now()
            pool.last_heartbeat_at = now
            pool.lease_expire_at = now + timedelta(seconds=pool.heartbeat_timeout_sec)
            return pool

    def heartbeat_service(self, *, owner_client_id: str, service_id: str, service_token: str) -> ServiceSession:
        now = utc_now()
        with self._lock:
            session = self._services.get(service_id)
            if session is None:
                raise KeyError("service not found")
            if session.owner_client_id != owner_client_id:
                raise PermissionError("owner_client_id mismatch")
            if not service_token or session.service_token != service_token:
                raise PermissionError("service_token mismatch")
            if session.status == pb2.SERVICE_STATUS_STOPPED:
                raise RuntimeError("service is stopped")
            session.last_heartbeat_at = now
            session.lease_expire_at = now + timedelta(seconds=session.heartbeat_timeout_sec)
            if session.status == pb2.SERVICE_STATUS_STARTING:
                session.status = pb2.SERVICE_STATUS_RUNNING
            return session

    def end_service(self, *, owner_client_id: str, service_id: str, service_token: str, reason: str) -> ServiceSession:
        with self._lock:
            session = self._services.get(service_id)
            if session is None:
                raise KeyError("service not found")
            if session.owner_client_id != owner_client_id:
                raise PermissionError("owner_client_id mismatch")
            if not service_token or session.service_token != service_token:
                raise PermissionError("service_token mismatch")
            self._stop_service_locked(session, reason=reason or "owner requested")
            return session

    def get_service(self, service_id: str) -> ServiceSession:
        with self._lock:
            session = self._services.get(service_id)
            if session is None:
                raise KeyError("service not found")
            return session

    def _stop_service_locked(self, session: ServiceSession, *, reason: str) -> None:
        if session.status == pb2.SERVICE_STATUS_STOPPED:
            return
        session.status = pb2.SERVICE_STATUS_DRAINING
        session.executor_ready = False
        session.stop_reason = reason
        session.alive_workers = 0
        session.status = pb2.SERVICE_STATUS_STOPPED
        session.lease_expire_at = utc_now()
        if self._executor_host is not None:
            try:
                self._executor_host.stop_service(service_id=session.service_id)
            except Exception:
                pass

    def _shutdown_all_services(self) -> None:
        with self._lock:
            sessions = list(self._services.values())
        for session in sessions:
            with self._lock:
                self._stop_service_locked(session, reason="nodecontrol shutdown")

    def list_service_methods(self, service_id: str) -> List[Dict[str, str]]:
        with self._lock:
            session = self._services.get(service_id)
            if session is None:
                raise KeyError("service not found")
            out = []
            for method in sorted(session.methods.keys()):
                qualified, doc = session.methods.get(method, ("", ""))
                out.append({"method": method, "qualified_name": qualified, "doc": doc})
            return out

    def _invoke_service_call(
        self,
        *,
        service_id: str,
        method: str,
        payload: dict,
        service_token: str,
        timeout_sec: float,
    ) -> Tuple[int, Dict[str, object]]:
        total_start = time.perf_counter()
        requested_method = str(method or "").strip()
        if not requested_method:
            return 400, {"ok": False, "error": "method is required"}
        with self._lock:
            session = self._services.get(service_id)
            if session is None:
                return 404, {"ok": False, "error": "service not found"}
            if session.status != pb2.SERVICE_STATUS_RUNNING:
                return 409, {"ok": False, "error": "service not running", "status": int(session.status)}
            if service_token and service_token != session.service_token:
                return 401, {"ok": False, "error": "invalid service token"}
            if requested_method not in session.methods:
                return 404, {"ok": False, "error": f"method not found: {requested_method}"}
            artifact = self._codes.get(session.code_version)
            if artifact is None:
                return 500, {"ok": False, "error": "artifact missing"}
            touch_code_last_at(self._artifact_dir, code_version=artifact.code_version)
            self._ensure_executor_host_alive_locked()
            if not session.executor_ready or self._executor_host is None:
                return 409, {"ok": False, "error": "service executor stopped"}
            session.request_count += 1
            session.in_flight = self._service_inflight_locked(session)
            prepared_payload = self._resolve_memory_object_refs_in_payload_locked(payload or {})
        setup_end = time.perf_counter()

        try:
            build_execute_spec_ms = 0.0
            executor_start = 0.0
            build_start = time.perf_counter()
            execute_spec = _build_execute_spec(
                artifact,
                object_dir=self._object_dir,
                work_dir=_code_data_dir(self._artifact_dir, code_version=artifact.code_version),
                method_name=requested_method,
                payload=prepared_payload,
                managed_globals_scope_dir=session.managed_globals_scope_dir,
                managed_globals_digest=session.managed_globals_digest,
            )
            build_end = time.perf_counter()
            build_execute_spec_ms = (build_end - build_start) * 1000.0
            executor_start = build_end
            resp = self._executor_host.call_service(
                service_id=service_id,
                timeout_sec=max(0.1, timeout_sec),
                execute_spec=execute_spec,
            )
            executor_end = time.perf_counter()
            if not resp.get("ok", False):
                if resp.get("timeout", False):
                    raise FutureTimeout()
                raise RuntimeError(str(resp.get("error", "service invoke failed")))
            status_text = str(resp.get("status_text", "FAILED_INFRA") or "FAILED_INFRA")
            result = resp.get("result")
            err_type = str(resp.get("err_type", "") or "")
            err_message = str(resp.get("err_message", "") or "")
            subprocess_timings = dict(resp.get("timings") or {})
        except FutureTimeout:
            with self._lock:
                session = self._services.get(service_id)
                if session is not None:
                    session.returned_count += 1
                    session.in_flight = self._service_inflight_locked(session)
                    self._record_service_timing_locked(
                        session,
                        method=requested_method,
                        ok=False,
                        http_status=504,
                        setup_ms=(setup_end - total_start) * 1000.0,
                        build_execute_spec_ms=build_execute_spec_ms,
                        executor_ms=(time.perf_counter() - executor_start) * 1000.0 if executor_start > 0 else 0.0,
                        finalize_ms=0.0,
                        total_ms=(time.perf_counter() - total_start) * 1000.0,
                        subprocess_timings=None,
                        error_type="Timeout",
                        error_message="invoke timeout",
                    )
            return 504, {"ok": False, "error": "invoke timeout"}
        except Exception as exc:
            with self._lock:
                session = self._services.get(service_id)
                if session is not None:
                    session.returned_count += 1
                    session.in_flight = self._service_inflight_locked(session)
                    self._record_service_timing_locked(
                        session,
                        method=requested_method,
                        ok=False,
                        http_status=500,
                        setup_ms=(setup_end - total_start) * 1000.0,
                        build_execute_spec_ms=build_execute_spec_ms,
                        executor_ms=(time.perf_counter() - executor_start) * 1000.0 if executor_start > 0 else 0.0,
                        finalize_ms=0.0,
                        total_ms=(time.perf_counter() - total_start) * 1000.0,
                        subprocess_timings=None,
                        error_type=exc.__class__.__name__,
                        error_message=repr(exc),
                    )
            return 500, {"ok": False, "error": repr(exc)}

        with self._lock:
            session = self._services.get(service_id)
            if session is not None:
                session.returned_count += 1
                session.in_flight = self._service_inflight_locked(session)
        finalize_start = time.perf_counter()

        if status_text == "SUCCEEDED":
            if isinstance(result, StoredResultArtifact):
                with self._lock:
                    self._register_stored_result_artifact_locked(result)
                result = _stored_result_to_result_ref(result, node_id=self.node_id)
            finalize_end = time.perf_counter()
            with self._lock:
                session = self._services.get(service_id)
                if session is not None:
                    self._record_service_timing_locked(
                        session,
                        method=requested_method,
                        ok=True,
                        http_status=200,
                        setup_ms=(setup_end - total_start) * 1000.0,
                        build_execute_spec_ms=build_execute_spec_ms,
                        executor_ms=(executor_end - executor_start) * 1000.0,
                        finalize_ms=(finalize_end - finalize_start) * 1000.0,
                        total_ms=(finalize_end - total_start) * 1000.0,
                        subprocess_timings=subprocess_timings,
                    )
            return 200, {"ok": True, "method": requested_method, "data": result or {}}
        if status_text == "FAILED_USER":
            finalize_end = time.perf_counter()
            with self._lock:
                session = self._services.get(service_id)
                if session is not None:
                    self._record_service_timing_locked(
                        session,
                        method=requested_method,
                        ok=False,
                        http_status=400,
                        setup_ms=(setup_end - total_start) * 1000.0,
                        build_execute_spec_ms=build_execute_spec_ms,
                        executor_ms=(executor_end - executor_start) * 1000.0,
                        finalize_ms=(finalize_end - finalize_start) * 1000.0,
                        total_ms=(finalize_end - total_start) * 1000.0,
                        subprocess_timings=subprocess_timings,
                        error_type=err_type or "UserError",
                        error_message=err_message or "user error",
                    )
            return 400, {
                "ok": False,
                "method": requested_method,
                "error_type": err_type or "UserError",
                "error": err_message or "user error",
            }
        finalize_end = time.perf_counter()
        with self._lock:
            session = self._services.get(service_id)
            if session is not None:
                self._record_service_timing_locked(
                    session,
                    method=requested_method,
                    ok=False,
                    http_status=503,
                    setup_ms=(setup_end - total_start) * 1000.0,
                    build_execute_spec_ms=build_execute_spec_ms,
                    executor_ms=(executor_end - executor_start) * 1000.0,
                    finalize_ms=(finalize_end - finalize_start) * 1000.0,
                    total_ms=(finalize_end - total_start) * 1000.0,
                    subprocess_timings=subprocess_timings,
                    error_type=err_type or "InfraError",
                    error_message=err_message or "infra error",
                )
        return 503, {
            "ok": False,
            "method": requested_method,
            "error_type": err_type or "InfraError",
            "error": err_message or "infra error",
        }

    def _invoke_service_http(
        self,
        service_id: str,
        method: str,
        payload: dict,
        service_token: str,
        timeout_sec: float,
    ) -> Tuple[int, Dict[str, object]]:
        return self._invoke_service_call(
            service_id=service_id,
            method=method,
            payload=payload,
            service_token=service_token,
            timeout_sec=timeout_sec,
        )

    def call_service(
        self,
        *,
        service_id: str,
        method: str,
        payload: dict,
        service_token: str,
        timeout_sec: float,
    ) -> Tuple[int, Dict[str, object]]:
        return self._invoke_service_call(
            service_id=service_id,
            method=method,
            payload=payload,
            service_token=service_token,
            timeout_sec=timeout_sec,
        )

    def update_service_globals(
        self,
        *,
        owner_client_id: str,
        service_id: str,
        service_token: str,
        values: Dict[str, Any],
    ) -> Tuple[str, List[str]]:
        with self._lock:
            session = self._services.get(service_id)
            if session is None:
                raise KeyError("service not found")
            if session.owner_client_id != owner_client_id:
                raise PermissionError("owner_client_id mismatch")
            if not service_token or session.service_token != service_token:
                raise PermissionError("service_token mismatch")
            artifact = self._get_live_code_artifact_locked(session.code_version)
            if artifact is None:
                raise KeyError("code artifact not found")
            state = self._ensure_service_managed_globals_state_locked(session)
            if state is None:
                raise ValueError("service artifact did not declare managed globals")
            globals_digest, updated_names = self._update_managed_globals_state(state, values=values)
            session.managed_globals_scope_dir = state.scope_dir
            session.managed_globals_digest = globals_digest
            executor_host = self._executor_host
            service_id = session.service_id
            worker_count = session.worker_count
        if artifact is None or executor_host is None:
            return globals_digest, updated_names
        fanout = self._warmup_fanout(worker_count)
        warmup_result = executor_host.warmup_service(
            service_id=service_id,
            fanout=fanout,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=self._object_dir,
                work_dir=_code_data_dir(self._artifact_dir, code_version=artifact.code_version),
                method_name=next(iter(session.methods.keys()), artifact.entry_callable),
                payload={},
                managed_globals_scope_dir=state.scope_dir,
                managed_globals_digest=globals_digest,
                warmup_only=True,
            ),
        )
        submitted, worker_pids = self._normalize_warmup_result(warmup_result, fanout=fanout)
        self._log_warmup_result(
            scope="service",
            key=service_id,
            worker_count=worker_count,
            submitted_count=submitted,
            worker_pids=worker_pids,
        )
        return globals_digest, updated_names

    def update_runtime_globals(
        self,
        *,
        client_id: str,
        code_version: str,
        runtime_key: str,
        code_token: str,
        values: Dict[str, Any],
    ) -> Tuple[str, List[str]]:
        normalized_client_id = str(client_id or "").strip()
        normalized_code_version = str(code_version or "").strip()
        normalized_runtime_key = str(runtime_key or normalized_code_version).strip() or normalized_code_version
        with self._lock:
            artifact = self._get_live_code_artifact_locked(normalized_code_version)
            if artifact is None:
                raise KeyError("code artifact not found")
            expected_code_token = self._client_code_tokens.get((normalized_client_id, normalized_code_version), "")
            if not code_token or not expected_code_token or expected_code_token != code_token:
                raise PermissionError("code_token mismatch")
            allowed_names = self._get_client_code_managed_globals_locked(
                client_id=normalized_client_id,
                code_version=normalized_code_version,
                runtime_key=normalized_runtime_key,
            )
            state = self._ensure_runtime_managed_globals_state_locked(
                client_id=normalized_client_id,
                code_version=normalized_code_version,
                runtime_key=normalized_runtime_key,
                allowed_names=allowed_names,
            )
            if state is None:
                raise ValueError("task artifact did not declare managed globals")
            globals_digest, updated_names = self._update_managed_globals_state(state, values=values)
            self._runtime_managed_globals[(normalized_client_id, normalized_code_version, normalized_runtime_key)] = state
            executor_host = self._executor_host
            pool = self._task_pools.get(normalized_client_id)
            worker_count = int(pool.worker_count if pool is not None else self.worker_capacity)
        if artifact is None or executor_host is None:
            return globals_digest, updated_names
        execute_spec = _build_execute_spec(
            artifact,
            object_dir=self._object_dir,
            work_dir=_code_data_dir(self._artifact_dir, code_version=artifact.code_version),
            method_name=artifact.entry_callable,
            payload={},
            managed_globals_scope_dir=state.scope_dir,
            managed_globals_digest=globals_digest,
            warmup_only=True,
        )
        fanout = self._warmup_fanout(worker_count)
        if pool is not None:
            warmup_result = executor_host.warmup_pool(
                pool_id=pool.pool_id,
                fanout=fanout,
                execute_spec=execute_spec,
            )
            submitted, worker_pids = self._normalize_warmup_result(warmup_result, fanout=fanout)
            self._log_warmup_result(
                scope="pool",
                key=pool.pool_id,
                worker_count=worker_count,
                submitted_count=submitted,
                worker_pids=worker_pids,
            )
        else:
            warmup_result = executor_host.warmup_runtime(
                runtime_key=normalized_runtime_key,
                fanout=fanout,
                execute_spec=execute_spec,
            )
            submitted, worker_pids = self._normalize_warmup_result(warmup_result, fanout=fanout)
            self._log_warmup_result(
                scope="runtime",
                key=normalized_runtime_key,
                worker_count=worker_count,
                submitted_count=submitted,
                worker_pids=worker_pids,
            )
        return globals_digest, updated_names

    def _service_status_http(self, service_id: str) -> Tuple[int, Dict[str, object]]:
        try:
            info = self.service_status_info(service_id)
        except KeyError:
            return 404, {"ok": False, "error": "service not found"}
        return 200, {"ok": True, "service": info}

    def service_status_info(self, service_id: str) -> Dict[str, object]:
        with self._lock:
            session = self._services.get(service_id)
            if session is None:
                raise KeyError("service not found")
            return {
                "service_id": session.service_id,
                "owner_client_id": session.owner_client_id,
                "service_name": session.service_name,
                "code_version": session.code_version,
                "status": int(session.status),
                "worker_count": session.worker_count,
                "alive_workers": session.alive_workers,
                "in_flight": self._service_inflight_locked(session),
                "queued": session.queued,
                "created_at": session.created_at,
                "last_heartbeat_at": session.last_heartbeat_at,
                "lease_expire_at": session.lease_expire_at,
                "http_base_url": session.http_base_url,
                "methods": sorted(session.methods.keys()),
                "timing_metrics": dict(session.timing_metrics or {}),
            }

    def submit_tasks(self, request: pb2.SubmitTasksRequest) -> Tuple[List[pb2.TaskAccepted], List[pb2.TaskRejected], int]:
        accepted: List[pb2.TaskAccepted] = []
        rejected: List[pb2.TaskRejected] = []
        with self._cv:
            if request.code_version not in self._codes:
                for item in request.tasks:
                    rejected.append(
                        pb2.TaskRejected(
                            task_id=item.task_id,
                            code=pb2.ERROR_CODE_UNKNOWN_CODE_VERSION,
                            message=f"unknown code_version: {request.code_version}",
                        )
                    )
                return accepted, rejected, self.credit_locked()

            for item in request.tasks:
                runtime_key = str(item.runtime_key or request.code_version).strip() or str(request.code_version)
                if item.task_id in self._tasks:
                    rejected.append(
                        pb2.TaskRejected(
                            task_id=item.task_id,
                            code=pb2.ERROR_CODE_DUPLICATE_TASK,
                            message="duplicate task_id",
                        )
                    )
                    continue

                if self.credit_locked() <= 0:
                    rejected.append(
                        pb2.TaskRejected(
                            task_id=item.task_id,
                            code=pb2.ERROR_CODE_NO_CREDIT,
                            message="node queue/inflight is full",
                        )
                    )
                    continue

                record = TaskState(
                    task_id=item.task_id,
                    client_id=request.client_id,
                    job_id=str(request.job_id or "").strip(),
                    code_version=request.code_version,
                    runtime_key=runtime_key,
                    execution_mode=request.execution_mode,
                    payload=struct_to_dict(item.payload),
                    timeout_hint_sec=max(0, item.timeout_hint_sec),
                    priority=max(1, item.priority or 1),
                )
                self._tasks[item.task_id] = record
                self._pending.append(item.task_id)
                accepted.append(pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED))
            if accepted:
                self._cv.notify_all()
            return accepted, rejected, self.credit_locked()

    def _claim_task_locked(self, worker_id: str) -> Optional[pb2.TaskEnvelope]:
        while self._pending:
            task_id = self._pending.popleft()
            task = self._tasks.get(task_id)
            if task is None:
                continue
            if task.status != pb2.TASK_STATUS_QUEUED:
                continue
            if task.cancel_requested:
                task.status = pb2.TASK_STATUS_CANCELLED
                task.finished_at = utc_now()
                self._publish_result_locked(task)
                continue

            now = utc_now()
            task.status = pb2.TASK_STATUS_RUNNING
            task.worker_id = worker_id
            task.lease_id = str(uuid.uuid4())
            task.started_at = now
            task.last_heartbeat_at = now
            return pb2.TaskEnvelope(
                task_id=task.task_id,
                code_version=task.code_version,
                attempt=task.attempt,
                execution_mode=task.execution_mode,
                payload=dict_to_struct(task.payload),
                lease_id=task.lease_id,
                lease_ttl_sec=self.heartbeat_timeout_sec,
            )
        return None

    def poll_task(self, worker_id: str) -> Optional[pb2.TaskEnvelope]:
        with self._cv:
            return self._claim_task_locked(worker_id)

    def heartbeat_task(self, request: pb2.HeartbeatTaskRequest) -> Tuple[bool, bool]:
        with self._lock:
            task = self._tasks.get(request.task_id)
            if task is None:
                return False, False
            if task.attempt != request.attempt:
                return False, False
            if task.status not in (pb2.TASK_STATUS_RUNNING, pb2.TASK_STATUS_CANCELLED):
                return False, False
            task.last_heartbeat_at = utc_now()
            return True, task.cancel_requested

    def report_result(self, request: pb2.ReportResultRequest) -> bool:
        with self._cv:
            task = self._tasks.get(request.task_id)
            if task is None:
                return False
            if task.attempt != request.attempt:
                return False
            if task.status not in (pb2.TASK_STATUS_RUNNING, pb2.TASK_STATUS_CANCELLED):
                return False

            task.finished_at = utc_now()
            task.last_heartbeat_at = task.finished_at
            should_publish = True
            if request.status == pb2.TASK_STATUS_SUCCEEDED:
                task.status = pb2.TASK_STATUS_SUCCEEDED
                task.result = struct_to_dict(request.result)
                task.error_type = ""
                task.error_message = ""
                log_payload_flow(
                    "task_result_report",
                    task_id=request.task_id,
                    status="SUCCEEDED",
                    result_summary=summarize_payload_flow_value(task.result),
                )
            elif request.status == pb2.TASK_STATUS_FAILED_USER:
                task.status = pb2.TASK_STATUS_FAILED_USER
                task.result = None
                task.error_type = request.error.type
                task.error_message = request.error.message
                log_payload_flow(
                    "task_result_report",
                    task_id=request.task_id,
                    status="FAILED_USER",
                    error_type=request.error.type,
                    error_message=request.error.message,
                )
            else:
                self._handle_infra_failure_locked(
                    task,
                    reason=request.error.message or request.error.type or "infra failure",
                    now=task.finished_at,
                )
                should_publish = task.status == pb2.TASK_STATUS_FAILED_INFRA
                log_payload_flow(
                    "task_result_report",
                    task_id=request.task_id,
                    status="FAILED_INFRA",
                    error_type=request.error.type,
                    error_message=request.error.message,
                )

            if should_publish:
                self._publish_result_locked(task)
            self._cv.notify_all()
            return True

    def pull_results(self, request: pb2.PullResultsRequest) -> Tuple[List[pb2.TaskResult], str]:
        return self._result_hook.pull(
            request.client_id,
            limit=max(1, request.limit or 100),
            wait_ms=max(0, request.wait_ms),
            cursor=request.cursor,
        )

    def cancel_tasks(self, request: pb2.CancelTasksRequest) -> Tuple[List[str], List[str], List[str]]:
        cancelled: List[str] = []
        not_found: List[str] = []
        already_done: List[str] = []
        with self._cv:
            for task_id in request.task_ids:
                task = self._tasks.get(task_id)
                if task is None:
                    not_found.append(task_id)
                    continue

                if task.status in (
                    pb2.TASK_STATUS_SUCCEEDED,
                    pb2.TASK_STATUS_FAILED_USER,
                    pb2.TASK_STATUS_FAILED_INFRA,
                    pb2.TASK_STATUS_CANCELLED,
                ):
                    already_done.append(task_id)
                    continue

                task.cancel_requested = True
                if task.status == pb2.TASK_STATUS_QUEUED:
                    task.status = pb2.TASK_STATUS_CANCELLED
                    task.finished_at = utc_now()
                    task.error_type = "Cancelled"
                    task.error_message = request.reason or "cancelled by client"
                    self._publish_result_locked(task)
                cancelled.append(task_id)
            if cancelled:
                self._cv.notify_all()
        return cancelled, not_found, already_done

    def cancel_job(self, request: pb2.CancelJobRequest) -> Tuple[int, int, int, int]:
        queued_cancelled = 0
        running_marked = 0
        already_done = 0
        matched = 0
        with self._cv:
            for task in self._tasks.values():
                if task.client_id != request.client_id:
                    continue
                if task.job_id != request.job_id:
                    continue
                matched += 1

                if task.status in (
                    pb2.TASK_STATUS_SUCCEEDED,
                    pb2.TASK_STATUS_FAILED_USER,
                    pb2.TASK_STATUS_FAILED_INFRA,
                    pb2.TASK_STATUS_CANCELLED,
                ):
                    already_done += 1
                    continue

                task.cancel_requested = True
                if task.status == pb2.TASK_STATUS_QUEUED:
                    task.status = pb2.TASK_STATUS_CANCELLED
                    task.finished_at = utc_now()
                    task.error_type = "Cancelled"
                    task.error_message = request.reason or f"cancelled by job_id={request.job_id}"
                    self._publish_result_locked(task)
                    queued_cancelled += 1
                elif task.status == pb2.TASK_STATUS_RUNNING:
                    running_marked += 1

            if queued_cancelled or running_marked:
                self._cv.notify_all()

        not_found = 0 if matched else 1
        return queued_cancelled, running_marked, already_done, not_found

    def metrics(self) -> Dict[str, int]:
        with self._lock:
            queued = self._queued_count_locked()
            shared_inflight = self._inflight_count_locked()
            service_inflight = sum(self._service_inflight_locked(session) for session in self._services.values())
            pool_inflight = sum(self._task_pool_inflight_locked(pool) for pool in self._task_pools.values())
            inflight = shared_inflight + service_inflight + pool_inflight
            credit = max(0, self.queue_capacity - (queued + inflight))
            return {
                "queued": queued,
                "inflight": inflight,
                "running": inflight,
                "credit": credit,
                "queue_capacity": self.queue_capacity,
                "worker_capacity": self.worker_capacity,
                "uptime_sec": int((utc_now() - self.started_at).total_seconds()),
            }

    def service_reports(self, *, include_stopped: bool = False) -> List[pb2.ServiceRouteReport]:
        with self._lock:
            out: List[pb2.ServiceRouteReport] = []
            for session in self._services.values():
                if not include_stopped and session.status == pb2.SERVICE_STATUS_STOPPED:
                    continue
                out.append(
                    pb2.ServiceRouteReport(
                        service_name=session.service_name,
                        service_id=session.service_id,
                        status=session.status,
                        worker_count=session.worker_count,
                        alive_workers=session.alive_workers,
                        in_flight=self._service_inflight_locked(session),
                        lease_expire_at=dt_to_ts(session.lease_expire_at),
                        http_base_url=session.http_base_url,
                    )
                )
            return out

    def service_report_payloads(self, *, include_stopped: bool = False) -> List[Dict[str, object]]:
        with self._lock:
            out: List[Dict[str, object]] = []
            for session in self._services.values():
                if not include_stopped and session.status == pb2.SERVICE_STATUS_STOPPED:
                    continue
                metrics = dict(session.timing_metrics or {})
                out.append(
                    {
                        "service_name": session.service_name,
                        "service_id": session.service_id,
                        "status": int(session.status),
                        "worker_count": int(session.worker_count),
                        "alive_workers": int(session.alive_workers),
                        "in_flight": int(self._service_inflight_locked(session)),
                        "received_count": int(session.request_count or 0),
                        "returned_count": int(session.returned_count or 0),
                        "ema_child_invoke_ms": float(metrics.get("ema_child_invoke_ms", 0.0) or 0.0),
                        "ema_samples": int(metrics.get("ema_samples", 0) or 0),
                        "lease_expire_at": session.lease_expire_at.isoformat(),
                        "http_base_url": session.http_base_url,
                    }
                )
            return out

    def active_runtime_keys(self, *, limit: int = 10) -> List[str]:
        with self._lock:
            stats: Dict[str, Tuple[int, int, float]] = {}
            now_ts = utc_now().timestamp()
            for task in self._tasks.values():
                if task.status not in (pb2.TASK_STATUS_QUEUED, pb2.TASK_STATUS_RUNNING):
                    continue
                runtime_key = str(task.runtime_key or task.code_version).strip() or str(task.code_version or "")
                running, queued, last_used = stats.get(runtime_key, (0, 0, 0.0))
                if task.status == pb2.TASK_STATUS_RUNNING:
                    running += 1
                    last_used = max(last_used, (task.last_heartbeat_at or task.started_at or utc_now()).timestamp())
                else:
                    queued += 1
                    last_used = max(last_used, now_ts)
                stats[runtime_key] = (running, queued, last_used)
            rows: List[Tuple[int, int, float, str]] = [
                (running, queued, last_used, runtime_key)
                for runtime_key, (running, queued, last_used) in stats.items()
            ]
            rows.sort(key=lambda item: (-item[0], -item[1], -item[2], item[3]))
            return [runtime_key for _hot, _queued, _last_used, runtime_key in rows[: max(1, int(limit))]]

    @property
    def python_version(self) -> str:
        """获取节点的 Python 版本。

        Returns:
            str: Python 版本，如 "py3.11", "py3.10"
        """
        return self._python_version

    def credit_locked(self) -> int:
        return max(0, self.queue_capacity - (self._queued_count_locked() + self._inflight_count_locked()))

    def _queued_count_locked(self) -> int:
        return sum(1 for task in self._tasks.values() if task.status == pb2.TASK_STATUS_QUEUED)

    def _inflight_count_locked(self) -> int:
        return sum(1 for task in self._tasks.values() if task.status == pb2.TASK_STATUS_RUNNING)

    def _publish_result_locked(self, task: TaskState) -> None:
        result = task.as_result()
        self._result_hook.push(task.client_id, result)

    def _handle_infra_failure_locked(self, task: TaskState, *, reason: str, now: datetime) -> None:
        if task.attempt < self.max_retries:
            task.attempt += 1
            task.status = pb2.TASK_STATUS_QUEUED
            task.worker_id = ""
            task.lease_id = ""
            task.started_at = None
            task.finished_at = None
            task.last_heartbeat_at = None
            task.error_type = ""
            task.error_message = ""
            self._pending.append(task.task_id)
            return

        task.status = pb2.TASK_STATUS_FAILED_INFRA
        task.finished_at = now
        task.error_type = "InfraFailure"
        task.error_message = reason
        self._publish_result_locked(task)

    def _touch_internal_heartbeats_locked(self) -> None:
        now = utc_now()
        for task in self._tasks.values():
            if task.status != pb2.TASK_STATUS_RUNNING:
                continue
            task.last_heartbeat_at = now

    def _dispatch_task_pool_locked(self) -> None:
        if not self.enable_internal_executor:
            return
        while self._inflight_count_locked() < self.worker_capacity and self._pending:
            task_id = self._pending.popleft()
            task = self._tasks.get(task_id)
            if task is None or task.status != pb2.TASK_STATUS_QUEUED:
                continue
            if task.cancel_requested:
                task.status = pb2.TASK_STATUS_CANCELLED
                task.finished_at = utc_now()
                task.error_type = "Cancelled"
                task.error_message = "cancelled by client"
                self._publish_result_locked(task)
                continue

            artifact = self._codes.get(task.code_version)
            if artifact is None:
                now = utc_now()
                self._handle_infra_failure_locked(task, reason="missing code artifact", now=now)
                continue

            now = utc_now()
            task.status = pb2.TASK_STATUS_RUNNING
            task.worker_id = f"runtime-key:{task.runtime_key or task.code_version}"
            task.lease_id = str(uuid.uuid4())
            task.started_at = now
            task.last_heartbeat_at = now
            try:
                if self._executor_host is None:
                    raise RuntimeError("executor host unavailable")
                touch_code_last_at(self._artifact_dir, code_version=artifact.code_version)
                managed_state = self._ensure_runtime_managed_globals_state_locked(
                    client_id=task.client_id,
                    code_version=task.code_version,
                    runtime_key=task.runtime_key,
                    allowed_names=self._get_client_code_managed_globals_locked(
                        client_id=str(task.client_id or "").strip(),
                        code_version=str(task.code_version or "").strip(),
                        runtime_key=str(task.runtime_key or "").strip(),
                    ),
                )
                self._executor_host.submit_runtime_task(
                    runtime_key=task.runtime_key,
                    task_id=task.task_id,
                    attempt=task.attempt,
                    execute_spec=_build_execute_spec(
                        artifact,
                        object_dir=self._object_dir,
                        work_dir=_code_data_dir(self._artifact_dir, code_version=artifact.code_version),
                        method_name=artifact.entry_callable,
                        payload=self._resolve_memory_object_refs_in_payload_locked(task.payload),
                        managed_globals_scope_dir=(managed_state.scope_dir if managed_state is not None else ""),
                        managed_globals_digest=(managed_state.globals_digest if managed_state is not None else ""),
                    ),
                )
            except Exception as exc:
                self._handle_infra_failure_locked(task, reason=repr(exc), now=now)
                continue

    def _drain_executor_events(self) -> None:
        with self._cv:
            self._ensure_executor_host_alive_locked()
            if self._executor_host is None:
                return
            for item in self._executor_host.drain_events():
                if str(item.get("kind", "") or "") == "pool_task_done":
                    pool_id = str(item.get("pool_id", "") or "")
                    task_id = str(item.get("task_id", "") or "")
                    attempt = int(item.get("attempt", 0) or 0)
                    status_text = str(item.get("status_text", "FAILED_INFRA") or "FAILED_INFRA")
                    result = item.get("result")
                    err_type = str(item.get("err_type", "") or "")
                    err_message = str(item.get("err_message", "") or "")
                    subprocess_timings = dict(item.get("timings") or {})
                    now = utc_now()
                    task = self._pool_tasks.get(task_id)
                    if task is None or task.attempt != attempt:
                        continue
                    pool = self._task_pools.get(pool_id)
                    task.finished_at = now
                    task.last_heartbeat_at = now
                    total_ms = max(
                        0.0,
                        (now - (task.started_at or now)).total_seconds() * 1000.0,
                    )
                    build_execute_spec_ms = float(getattr(task, "dispatch_build_execute_spec_ms", 0.0) or 0.0)
                    executor_ms = max(0.0, total_ms - build_execute_spec_ms)
                    if status_text == "FAILED_USER":
                        task.status = pb2.TASK_STATUS_FAILED_USER
                        task.result = None
                        task.error_type = err_type or "UserError"
                        task.error_message = err_message or "user function failed"
                    elif status_text == "FAILED_INFRA":
                        task.status = pb2.TASK_STATUS_FAILED_INFRA
                        task.result = None
                        task.error_type = err_type or "InfraError"
                        task.error_message = err_message or "infra failure"
                    else:
                        task.status = pb2.TASK_STATUS_SUCCEEDED
                        if isinstance(result, StoredResultArtifact):
                            self._register_stored_result_artifact_locked(result)
                            task.result = _stored_result_to_result_ref(result, node_id=self.node_id)
                        else:
                            task.result = result or {}
                        task.error_type = ""
                        task.error_message = ""
                    if pool is not None:
                        pool.returned_count += 1
                        self._record_task_pool_timing_locked(
                            pool,
                            method=pool.task_method,
                            ok=bool(status_text not in {"FAILED_USER", "FAILED_INFRA"}),
                            setup_ms=0.0,
                            build_execute_spec_ms=build_execute_spec_ms,
                            executor_ms=executor_ms,
                            finalize_ms=0.0,
                            total_ms=total_ms,
                            subprocess_timings=subprocess_timings,
                            error_type=task.error_type,
                            error_message=task.error_message,
                        )
                    self._pool_result_hook.push(pool_id, task.as_result())
                    self._cv.notify_all()
                    continue
                if str(item.get("kind", "") or "") != "runtime_task_done":
                    continue
                task_id = str(item.get("task_id", "") or "")
                attempt = int(item.get("attempt", 0) or 0)
                status_text = str(item.get("status_text", "FAILED_INFRA") or "FAILED_INFRA")
                result = item.get("result")
                err_type = str(item.get("err_type", "") or "")
                err_message = str(item.get("err_message", "") or "")
                now = utc_now()
                task = self._tasks.get(task_id)
                if task is None:
                    continue
                if task.attempt != attempt:
                    continue
                if task.status not in (pb2.TASK_STATUS_RUNNING, pb2.TASK_STATUS_CANCELLED):
                    continue
                task.finished_at = now
                task.last_heartbeat_at = now
                if task.cancel_requested:
                    task.status = pb2.TASK_STATUS_CANCELLED
                    task.error_type = "Cancelled"
                    task.error_message = "cancelled by client"
                    task.result = None
                    self._publish_result_locked(task)
                elif status_text == "FAILED_INFRA":
                    self._handle_infra_failure_locked(task, reason=err_message or err_type or "infra failure", now=now)
                elif status_text == "FAILED_USER":
                    task.status = pb2.TASK_STATUS_FAILED_USER
                    task.result = None
                    task.error_type = err_type or "UserError"
                    task.error_message = err_message or "user function failed"
                    self._publish_result_locked(task)
                else:
                    task.status = pb2.TASK_STATUS_SUCCEEDED
                    if isinstance(result, StoredResultArtifact):
                        self._register_stored_result_artifact_locked(result)
                        task.result = _stored_result_to_result_ref(result, node_id=self.node_id)
                    else:
                        task.result = result or {}
                    task.error_type = ""
                    task.error_message = ""
                    self._publish_result_locked(task)
                self._cv.notify_all()

    def _dispatch_loop(self) -> None:
        while not self._stop_event.is_set():
            self._drain_executor_events()
            with self._cv:
                self._ensure_executor_host_alive_locked()
                self._touch_internal_heartbeats_locked()
                self._dispatch_task_pool_locked()
            self._drain_executor_events()
            self._stop_event.wait(self.executor_poll_interval_sec)

    def _monitor_loop(self) -> None:
        while not self._stop_event.wait(self.monitor_interval_sec):
            self._handle_timeouts()
            self._handle_service_timeouts()

    def _handle_timeouts(self) -> None:
        now = utc_now()
        with self._cv:
            mutated = False
            for task in self._tasks.values():
                if task.status != pb2.TASK_STATUS_RUNNING:
                    continue
                if task.last_heartbeat_at is None:
                    continue
                diff = (now - task.last_heartbeat_at).total_seconds()
                if diff <= self.heartbeat_timeout_sec:
                    continue
                if self.enable_internal_executor:
                    task.error_message = task.error_message or "heartbeat timeout"
                self._handle_infra_failure_locked(task, reason="heartbeat timeout", now=now)
                mutated = True

            if mutated:
                self._cv.notify_all()

    def _handle_service_timeouts(self) -> None:
        now = utc_now()
        with self._lock:
            for session in self._services.values():
                if session.status != pb2.SERVICE_STATUS_RUNNING:
                    continue
                if now <= session.lease_expire_at:
                    continue
                self._stop_service_locked(session, reason="owner heartbeat timeout")
            for pool in self._task_pools.values():
                if pool.status != "RUNNING":
                    continue
                if now <= pool.lease_expire_at:
                    continue
                if self._executor_host is not None and pool.executor_ready:
                    self._executor_host.stop_task_pool(pool_id=pool.pool_id)
                pool.executor_ready = False
                pool.status = "STOPPED"
