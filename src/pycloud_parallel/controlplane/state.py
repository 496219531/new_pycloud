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
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, ClassVar, Dict, Iterable, List, Optional, Sequence, Tuple

from google.protobuf import timestamp_pb2

from pycloud_parallel.controlplane.executor_host import ExecutorHostClient
from pycloud_parallel.controlplane.artifact import (
    _dependency_policy_allows_install,
    _normalize_dependency_policy_mode,
)
from pycloud_parallel.controlplane.data_store import DataStore, StoredDataArtifact
from pycloud_parallel.controlplane.data_ref import (
    DataRef,
    coerce_data_ref,
    data_ref_from_payload,
    is_data_ref_payload,
    maybe_data_ref,
    resolve_data_ref_materialize_as,
)
from pycloud_parallel.controlplane.http_gateway import ServiceHttpGateway
from pycloud_parallel.controlplane.hooks import InMemoryResultHook
from pycloud_parallel.controlplane.object_ref import (
    ObjectRef,
    normalize_materialize_as,
    object_format_suffix,
    normalize_object_format,
    normalize_object_id,
    object_id_from_sha256_hex,
    object_storage_path,
)
from pycloud_parallel.controlplane.result_ref import ResultRef
from pycloud_parallel.controlplane.runtime_spec import (
    matches_python_runtime,
    normalize_python_runtime_spec,
)
from pycloud_parallel.controlplane.session_model import (
    ExecutionReplicaSnapshot,
    SessionBinding,
    SessionIdentity,
    SessionLease,
)
from pycloud_parallel.controlplane.config import FILE_HASH_CHUNK_SIZE_BYTES
from pycloud_parallel.controlplane.config import (
    OBJECT_SEGMENT_MAX_BYTES,
    OBJECT_SEGMENT_TARGET_BYTES,
    get_payload_policy,
)
from pycloud_parallel.controlplane.payload_transport import decode_payload_from_transport, normalize_inbound_payload
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


StoredResultArtifact = StoredDataArtifact


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
    dependency_policy_mode: str = "",
    dependency_allowlist: Sequence[str],
) -> str:
    normalized_digest = str(digest or "").strip().lower()
    if not normalized_digest:
        raise ValueError("invalid code digest")
    normalized_dependency_policy_mode = _normalize_dependency_policy_mode(
        dependency_policy_mode,
        dependency_allowlist=dependency_allowlist,
    )
    variant_payload = {
        "runtime": str(runtime or "").strip(),
        "entry_module": str(entry_module or "").strip(),
        "entry_callable": str(entry_callable or "").strip(),
        "package_format": str(package_format or "").strip(),
        "export_mode": str(export_mode or "").strip(),
        "export_methods": [str(name) for name in export_methods],
        "export_decorator": str(export_decorator or "").strip(),
        "dependency_policy_mode": normalized_dependency_policy_mode,
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
    pinned_ref_ids: Sequence[str] = (),
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
        "pinned_ref_ids": [str(item).strip() for item in pinned_ref_ids if str(item).strip()],
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
        meta["pinned_ref_ids"] = [
            str(item).strip()
            for item in list(meta.get("pinned_ref_ids") or ())
            if str(item).strip()
        ]
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
        pinned_ref_ids=(),
    )


def touch_object_last_at(object_dir: Path, *, object_id: str, fallback_path: Optional[Path] = None) -> None:
    _touch_object_last_at(object_dir, object_id=object_id, fallback_path=fallback_path)


def _normalize_pinned_ref_ids(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in values or ():
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _object_meta_pinned_ref_ids(meta: Dict[str, Any]) -> List[str]:
    return _normalize_pinned_ref_ids(list(meta.get("pinned_ref_ids") or ()))


def _pin_object_meta(object_dir: Path, *, object_id: str, ref_id: str, fallback_path: Optional[Path] = None) -> bool:
    normalized_ref_id = str(ref_id or "").strip()
    if not normalized_ref_id:
        raise ValueError("ref_id is required")
    object_root = Path(object_dir)
    meta = _load_object_meta(object_root, object_id=object_id)
    now = utc_now()
    if not meta:
        candidate = Path(fallback_path) if fallback_path is not None else None
        if candidate is None or not candidate.exists():
            return False
        _write_object_meta(
            object_root,
            object_id=object_id,
            fmt=normalize_object_format("", source_name=candidate.name, default="bin"),
            size_bytes=candidate.stat().st_size,
            created_at=datetime.fromtimestamp(candidate.stat().st_ctime, tz=timezone.utc),
            last_at=now,
            storage_backend="file",
            pinned_ref_ids=(normalized_ref_id,),
        )
        return True

    pinned = _object_meta_pinned_ref_ids(meta)
    if normalized_ref_id not in pinned:
        pinned.append(normalized_ref_id)
    meta["pinned_ref_ids"] = pinned
    meta["last_at"] = now.astimezone(timezone.utc).isoformat()
    _atomic_write_json(_object_meta_path(object_root, object_id=object_id), meta)
    return True


def _release_object_meta_pin(object_dir: Path, *, object_id: str, ref_id: str) -> Tuple[bool, bool]:
    normalized_ref_id = str(ref_id or "").strip()
    if not normalized_ref_id:
        raise ValueError("ref_id is required")
    object_root = Path(object_dir)
    meta = _load_object_meta(object_root, object_id=object_id)
    if not meta:
        return False, False
    pinned = _object_meta_pinned_ref_ids(meta)
    if normalized_ref_id in pinned:
        pinned = [item for item in pinned if item != normalized_ref_id]
    meta["pinned_ref_ids"] = pinned
    meta["last_at"] = utc_now().astimezone(timezone.utc).isoformat()
    _atomic_write_json(_object_meta_path(object_root, object_id=object_id), meta)
    return True, bool(pinned)


def _segment_has_live_refs(object_dir: Path, *, segment_relpath: str) -> bool:
    normalized_relpath = str(segment_relpath or "").strip()
    if not normalized_relpath:
        return False
    meta_dir = _objects_meta_dir(object_dir)
    if not meta_dir.exists():
        return False
    for meta_path in meta_dir.glob("*.json"):
        meta = _load_object_meta(object_dir, object_id=f"sha256:{meta_path.stem}")
        if not meta:
            continue
        if str(meta.get("storage_backend", "file") or "file").strip() != "segment":
            continue
        if str(meta.get("segment_relpath", "") or "").strip() == normalized_relpath:
            return True
    return False


def _cleanup_orphan_segment_file(object_dir: Path, *, segment_relpath: str) -> None:
    normalized_relpath = str(segment_relpath or "").strip()
    if not normalized_relpath:
        return
    if _segment_has_live_refs(object_dir, segment_relpath=normalized_relpath):
        return
    segment_path = _segment_path_from_relpath(object_dir, normalized_relpath)
    with contextlib.suppress(FileNotFoundError):
        segment_path.unlink()


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


def _data_store_for_object_dir(
    object_dir: str,
    *,
    node_id: str = "",
    control_addr: str = "",
) -> DataStore:
    normalized_dir = str(object_dir or "").strip()
    return DataStore(
        object_dir=normalized_dir,
        node_id=str(node_id or ""),
        control_addr=str(control_addr or ""),
        store_path_impl=lambda path: _store_result_path(path, object_dir=normalized_dir),
        store_dataframe_impl=lambda frame: _store_result_dataframe(frame, object_dir=normalized_dir),
        store_series_impl=lambda series: _store_result_series(series, object_dir=normalized_dir),
        store_ndarray_impl=lambda array: _store_result_ndarray(array, object_dir=normalized_dir),
        resolve_data_ref_impl=lambda ref: _resolve_single_data_ref(ref, object_dir=normalized_dir),
    )


def _stored_result_to_result_ref(result: StoredResultArtifact, *, node_id: str) -> ResultRef:
    return _data_store_for_object_dir("", node_id=node_id).result_ref_from_stored_artifact(result)


def _materialize_object_bytes(*, blob: bytes, fmt: str, materialize_as: str) -> Any:
    materialized = normalize_materialize_as(materialize_as, default="path")
    normalized_format = normalize_object_format(fmt, default="bin")
    if materialized == "bytes":
        return bytes(blob)
    if materialized == "text":
        return blob.decode("utf-8")
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
            if len(frame.columns) != 1:
                raise ObjectResolutionError("series bundle parquet must contain exactly one column")
            return deserialize_series_bundle(meta, frame.iloc[:, 0])
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
        if materialize_as == "text":
            return candidate.read_text(encoding="utf-8")
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
    data_store = _data_store_for_object_dir(object_dir)

    def _try_inline_result(value: Any) -> Tuple[bool, Any]:
        policy = get_payload_policy("result")
        serialized = serialize_arrow_compatible(value)
        wrapped = serialized if isinstance(serialized, dict) else {"value": serialized}
        try:
            serialize_inline_result(
                wrapped,
                context="task result",
                limit_bytes=policy.inline_result_hard_limit_bytes,
            )
        except ValueError:
            return False, None
        log_payload_flow("inline_result_ready", context="task result", summary=summarize_payload_flow_value(value))
        return True, wrapped

    if isinstance(ret, Path):
        log_payload_flow("result_ref_store", path_type="path", summary=summarize_payload_flow_value(ret))
        return data_store.store_path(ret)

    try:
        import pandas as pd

        if isinstance(ret, pd.DataFrame):
            inlined, wrapped = _try_inline_result(ret)
            if inlined:
                return wrapped
            log_payload_flow("result_ref_store", path_type="dataframe", summary=summarize_payload_flow_value(ret))
            return data_store.store_dataframe(ret)
        if isinstance(ret, pd.Series):
            inlined, wrapped = _try_inline_result(ret)
            if inlined:
                return wrapped
            log_payload_flow("result_ref_store", path_type="series", summary=summarize_payload_flow_value(ret))
            return data_store.store_series(ret)
    except ImportError:
        pass

    try:
        import numpy as np

        if isinstance(ret, np.ndarray):
            inlined, wrapped = _try_inline_result(ret)
            if inlined:
                return wrapped
            log_payload_flow("result_ref_store", path_type="ndarray", summary=summarize_payload_flow_value(ret))
            return data_store.store_ndarray(ret)
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
    payload_mode: str = "task_submit",
    managed_globals_scope_dir: str = "",
    managed_globals_digest: str = "",
    warmup_only: bool = False,
) -> Dict[str, Any]:
    return {
        "artifact_path": artifact.path,
        "entry_module": artifact.entry_module,
        "package_format": artifact.package_format,
        "dependency_path": artifact.dependency_path,
        "dependency_policy_mode": artifact.dependency_policy_mode,
        "object_dir": str(object_dir),
        "work_dir": str(work_dir or ""),
        "export_mode": artifact.export_mode,
        "export_methods": list(artifact.export_methods),
        "export_decorator": artifact.export_decorator,
        "method_name": method_name,
        "entry_callable": artifact.entry_callable,
        "payload": payload or {},
        "payload_mode": str(payload_mode or "task_submit"),
        "managed_globals_scope_dir": str(managed_globals_scope_dir or ""),
        "managed_globals_digest": str(managed_globals_digest or ""),
        "warmup_only": bool(warmup_only),
    }


def _serialize_result_for_json(obj: Any) -> Any:
    """序列化返回值中的 Arrow 对象和 numpy 类型，使其可被 JSON 序列化。"""
    return serialize_arrow_compatible(obj)


_ROUTER_CACHE_LOCK = threading.Lock()
_ROUTER_CACHE: Dict[str, Tuple[Any, Dict[str, Any], Dict[str, Tuple[str, str]]]] = {}


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
    dependency_policy_mode: str = "",
    install_failed: bool = False,
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
    repair_hint = _dependency_policy_missing_import_hint(
        dependency_policy_mode=dependency_policy_mode,
        missing_import=missing_import,
        install_failed=install_failed,
    )
    return (
        "artifact validation failed while loading "
        f"(entry_module={normalized_module}, entry_callable={normalized_callable}, package_format={normalized_format}): "
        f"{detail}{repair_hint}"
    )


def _describe_user_execution_error(exc: BaseException, *, dependency_policy_mode: str = "") -> str:
    message = str(exc) or repr(exc)
    detail = f"{exc.__class__.__name__}: {message}"
    missing_import = _missing_import_name(exc)
    repair_hint = _dependency_policy_missing_import_hint(
        dependency_policy_mode=dependency_policy_mode,
        missing_import=missing_import,
        install_failed=False,
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


def _dependency_policy_missing_import_hint(
    *,
    dependency_policy_mode: str,
    missing_import: str,
    install_failed: bool = False,
) -> str:
    normalized_mode = _normalize_dependency_policy_mode(dependency_policy_mode)
    if not missing_import:
        if install_failed and normalized_mode == "allow_install":
            return (
                " artifact dependency policy is `allow_install`; dependency installation failed. "
                "Pin versions and verify node network/package index availability."
            )
        return ""
    if normalized_mode == "node_preinstalled":
        return (
            f" artifact dependency policy is `node_preinstalled`; node environment is missing `{missing_import}`. "
            "Preinstall it on the node, or switch to a prebuilt artifact."
        )
    if normalized_mode == "allow_install":
        if install_failed:
            return (
                f" artifact dependency policy is `allow_install`; dependency install failed for `{missing_import}`. "
                "Pin the version and verify node network/package index availability."
            )
        return (
            f" artifact dependency policy is `allow_install`; dependency `{missing_import}` is still unavailable "
            "after preparation. Pin the version and verify node network/package index availability."
        )
    return (
        f" artifact dependency policy is `prebuilt`; missing dependency `{missing_import}`. "
        "Rebuild the artifact with bundled dependencies, or switch to `ArtifactDeps.allow_install([...])`."
    )


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
) -> Tuple[Any, Dict[str, Any], Dict[str, Tuple[str, str]]]:
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
    loaded = (module, loaded[0], loaded[1])
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


def _resolve_apply_managed_globals_hook(module: Any) -> Optional[Any]:
    candidate = getattr(module, "apply_managed_globals", None)
    if candidate is None:
        return None
    if not callable(candidate):
        raise ValueError("apply_managed_globals must be callable when defined")
    return candidate


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


def _resolve_single_data_ref(ref: DataRef | object, *, object_dir: str) -> Any:
    data_ref = ref if isinstance(ref, DataRef) else coerce_data_ref(ref)
    root = Path(str(object_dir or "")).resolve()
    materialized = resolve_data_ref_materialize_as(data_ref, default="path")
    log_payload_flow(
        "object_ref_resolve",
        materialize_as=materialized,
        summary=summarize_payload_flow_value(data_ref),
    )
    meta = _load_object_meta(root, object_id=data_ref.object_id)
    artifact: Optional[ObjectArtifact] = None
    if meta:
        artifact = _object_artifact_from_meta(root, object_id=data_ref.object_id, meta=meta)
        if not _artifact_exists(artifact):
            artifact = None
    if artifact is None:
        candidate = object_storage_path(root, object_id=data_ref.object_id, fmt=data_ref.format)
        if candidate.exists():
            artifact = ObjectArtifact(
                object_id=normalize_object_id(data_ref.object_id),
                path=str(candidate),
                format=normalize_object_format(data_ref.format, source_name=candidate.name, default="bin"),
                size_bytes=candidate.stat().st_size,
                created_at=utc_now(),
                storage_backend="file",
            )
        else:
            digest = normalize_object_id(data_ref.object_id).replace("sha256:", "", 1)
            suffix = object_format_suffix(data_ref.format)
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
                    object_id=normalize_object_id(data_ref.object_id),
                    path=str(fallback[0]),
                    format=normalize_object_format("", source_name=fallback[0].name, default="bin"),
                    size_bytes=fallback[0].stat().st_size,
                    created_at=utc_now(),
                    storage_backend="file",
                )
    if artifact is not None:
        fallback_path = Path(artifact.path) if artifact.path else Path(artifact.segment_path)
        _touch_object_last_at(root, object_id=data_ref.object_id, fallback_path=fallback_path)
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
    raise ObjectResolutionError(f"object not found on node: {data_ref.object_id}")


def _resolve_object_refs_in_payload(payload: Any, *, object_dir: str) -> Any:
    data_store = _data_store_for_object_dir(object_dir)

    def _resolve(value: Any) -> Any:
        data_ref = maybe_data_ref(value)
        if data_ref is not None:
            return data_store.resolve_data_ref(data_ref)
        if isinstance(value, dict):
            if is_data_ref_payload(value):
                return _resolve(data_ref_from_payload(value))
            return {key: _resolve(item) for key, item in value.items()}
        if isinstance(value, list):
            return [_resolve(item) for item in value]
        if isinstance(value, tuple):
            return tuple(_resolve(item) for item in value)
        return value

    return _resolve(payload)


def _apply_managed_globals_to_router(
    module: Any,
    router: Dict[str, Any],
    *,
    scope_dir: str,
    globals_digest: str,
    object_dir: str,
    entry_module: str,
    method_name: str,
    session_kind: str,
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

        apply_hook = _resolve_apply_managed_globals_hook(module)
        fallback_assign_values: Optional[Dict[str, Any]] = None
        if apply_hook is None:
            fallback_assign_values = dict(resolved_values)
        else:
            context = {
                "entry_module": str(entry_module or "").strip(),
                "session_kind": str(session_kind or "").strip(),
                "method_name": str(method_name or "").strip(),
                "globals_digest": normalized_digest,
            }
            hook_result = apply_hook(dict(resolved_values), **context)
            if hook_result is None:
                fallback_assign_values = None
            elif isinstance(hook_result, dict):
                fallback_assign_values = dict(hook_result)
            else:
                raise RuntimeError("apply_managed_globals must return None or dict")

        if fallback_assign_values:
            if apply_hook is not None:
                module_globals = getattr(module, "__dict__", None)
                if not isinstance(module_globals, dict):
                    raise RuntimeError("entry module globals are unavailable for apply_managed_globals fallback assign")
                for name, value in fallback_assign_values.items():
                    normalized_name = str(name or "").strip()
                    if not normalized_name:
                        continue
                    module_globals[normalized_name] = value
            else:
                seen_globals_ids = set()
                for fn in router.values():
                    globals_dict = getattr(fn, "__globals__", None)
                    if not isinstance(globals_dict, dict):
                        continue
                    globals_id = id(globals_dict)
                    if globals_id in seen_globals_ids:
                        continue
                    seen_globals_ids.add(globals_id)
                    for name, value in fallback_assign_values.items():
                        normalized_name = str(name or "").strip()
                        if not normalized_name:
                            continue
                        globals_dict[normalized_name] = value

        with _MANAGED_GLOBALS_CACHE_LOCK:
            _MANAGED_GLOBALS_CACHE[normalized_scope_dir] = normalized_digest


def _execute_payload_in_subprocess(
    artifact_path: str,
    entry_module: str,
    package_format: str,
    dependency_path: str,
    dependency_policy_mode: str,
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
    payload_mode: str = "task_submit",
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
                module, router, _method_info = _load_callable_router(
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
                            dependency_policy_mode=dependency_policy_mode,
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
                        module,
                        router,
                        scope_dir=managed_globals_scope_dir,
                        globals_digest=managed_globals_digest,
                        object_dir=object_dir,
                        entry_module=entry_module,
                        method_name=method,
                        session_kind=("service" if str(payload_mode or "task_submit") == "http_call" else "task_pool"),
                    )
                    resolved_payload = normalize_inbound_payload(
                        payload,
                        object_dir=object_dir,
                        policy=get_payload_policy(str(payload_mode or "task_submit")),  # type: ignore[arg-type]
                        resolve_object_refs=lambda value: _resolve_object_refs_in_payload(value, object_dir=object_dir),
                    )
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
                    return (
                        "FAILED_USER",
                        None,
                        exc.__class__.__name__,
                        _describe_user_execution_error(exc, dependency_policy_mode=dependency_policy_mode),
                        _timings(),
                    )
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


@dataclass(frozen=True)
class DataRegistryEntry:
    ref_id: str
    storage_id: str
    logical_type: str
    format: str
    size_bytes: int
    materialize_as: str
    locator_kind: str
    locator_token: str
    consume_on_read: bool
    node_id: str = ""
    node_instance_id: str = ""
    control_addr: str = ""
    replicas: Tuple[Dict[str, object], ...] = ()
    created_at: datetime = field(default_factory=utc_now)
    last_at: datetime = field(default_factory=utc_now)
    ttl_sec: int = 3600



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
    dependency_policy_mode: str
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
    kind: ClassVar[str] = "service"
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

    def identity(self) -> SessionIdentity:
        return SessionIdentity(
            kind="service",
            session_id=str(self.service_id or ""),
            session_name=str(self.service_name or ""),
            owner_client_id=str(self.owner_client_id or ""),
            session_token=str(self.service_token or ""),
        )

    def lease(self) -> SessionLease:
        return SessionLease(
            heartbeat_timeout_sec=max(1, int(self.heartbeat_timeout_sec or 0)),
            idle_ttl_sec=max(0, int(self.idle_ttl_sec or 0)),
            created_at=self.created_at,
            last_heartbeat_at=self.last_heartbeat_at,
            lease_expire_at=self.lease_expire_at,
        )

    def binding(self) -> SessionBinding:
        return SessionBinding(
            code_version=str(self.code_version or ""),
            worker_count=max(0, int(self.worker_count or 0)),
            executor_ready=bool(self.executor_ready),
            managed_global_names=tuple(str(name) for name in (self.managed_global_names or ())),
            managed_globals_scope_dir=str(self.managed_globals_scope_dir or ""),
            managed_globals_digest=str(self.managed_globals_digest or ""),
        )

    def snapshot(
        self,
        *,
        node_instance_id: str = "",
        node_id: str = "",
        failure: str = "",
    ) -> ExecutionReplicaSnapshot:
        status_text = pb2.ServiceStatus.Name(int(self.status or pb2.SERVICE_STATUS_UNSPECIFIED))
        alive = not str(failure or "").strip() and int(self.status or 0) in {
            int(pb2.SERVICE_STATUS_STARTING),
            int(pb2.SERVICE_STATUS_RUNNING),
            int(pb2.SERVICE_STATUS_DRAINING),
        }
        return ExecutionReplicaSnapshot(
            kind="service",
            node_instance_id=str(node_instance_id or ""),
            node_id=str(node_id or ""),
            session_id=str(self.service_id or ""),
            session_name=str(self.service_name or ""),
            code_version=str(self.code_version or ""),
            worker_count=max(0, int(self.worker_count or 0)),
            alive=alive,
            status=status_text,
            lease_expire_at=self.lease_expire_at,
            failure=str(failure or ""),
        )


@dataclass
class TaskPoolState:
    kind: ClassVar[str] = "task_pool"
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

    def identity(self) -> SessionIdentity:
        return SessionIdentity(
            kind="task_pool",
            session_id=str(self.pool_id or ""),
            session_name=str(self.pool_name or ""),
            owner_client_id=str(self.owner_client_id or ""),
            session_token=str(self.pool_token or ""),
        )

    def lease(self) -> SessionLease:
        return SessionLease(
            heartbeat_timeout_sec=max(1, int(self.heartbeat_timeout_sec or 0)),
            idle_ttl_sec=max(0, int(self.idle_ttl_sec or 0)),
            created_at=self.created_at,
            last_heartbeat_at=self.last_heartbeat_at,
            lease_expire_at=self.lease_expire_at,
        )

    def binding(self) -> SessionBinding:
        return SessionBinding(
            code_version=str(self.code_version or ""),
            worker_count=max(0, int(self.worker_count or 0)),
            executor_ready=bool(self.executor_ready),
            managed_global_names=tuple(str(name) for name in (self.managed_global_names or ())),
            managed_globals_scope_dir=str(self.managed_globals_scope_dir or ""),
            managed_globals_digest=str(self.managed_globals_digest or ""),
        )

    def snapshot(
        self,
        *,
        node_instance_id: str = "",
        node_id: str = "",
        failure: str = "",
    ) -> ExecutionReplicaSnapshot:
        status_text = str(self.status or "")
        alive = not str(failure or "").strip() and status_text.upper() == "RUNNING"
        return ExecutionReplicaSnapshot(
            kind="task_pool",
            node_instance_id=str(node_instance_id or ""),
            node_id=str(node_id or ""),
            session_id=str(self.pool_id or ""),
            session_name=str(self.pool_name or ""),
            code_version=str(self.code_version or ""),
            worker_count=max(0, int(self.worker_count or 0)),
            alive=alive,
            status=status_text,
            lease_expire_at=self.lease_expire_at,
            failure=str(failure or ""),
        )


ServiceReplicaState = ServiceSession
TaskPoolReplicaState = TaskPoolState

_SPLIT_STATE_EXPORTS = {
    "InfoCenterState": "pycloud_parallel.controlplane.infocenter_state",
    "NodeControlState": "pycloud_parallel.controlplane.nodecontrol_state",
}


def __getattr__(name: str) -> Any:
    module_name = _SPLIT_STATE_EXPORTS.get(name)
    if not module_name:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = importlib.import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> List[str]:
    return sorted(set(globals().keys()) | set(_SPLIT_STATE_EXPORTS))
