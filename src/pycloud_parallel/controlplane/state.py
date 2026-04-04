from __future__ import annotations

"""In-memory state backends for InfoCenter and NodeControl."""

import hashlib
import importlib
import importlib.util
import inspect
import json
import os
import queue
import subprocess
import secrets
import shutil
import sys
import tarfile
import tempfile
import threading
import uuid
import zipfile
from concurrent.futures import TimeoutError as FutureTimeout
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple

from google.protobuf import struct_pb2
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
from pycloud_parallel.controlplane.serialization import (
    convert_arrow_to_dict,
    convert_dict_to_arrow,
    dict_to_struct,
    is_arrow_compatible,
    serialize_arrow_compatible,
    serialize_inline_result,
    struct_to_dict,
)
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

_DEFAULT_EXPORT_DECORATOR = "pycloud_export"


class LargeResultError(ValueError):
    """Raised when a task result is too large for safe inline return."""


@dataclass(frozen=True)
class StoredResultArtifact:
    object_id: str
    format: str
    size_bytes: int
    materialize_as: str


@dataclass
class ManagedGlobalsState:
    scope_kind: str
    scope_key: str
    scope_dir: str
    allowed_names: Tuple[str, ...]
    globals_digest: str


_EXECUTOR_ACTIVE = object()
_MANAGED_GLOBALS_CACHE_LOCK = threading.Lock()
_MANAGED_GLOBALS_CACHE: Dict[str, str] = {}


def _stable_json_bytes(data: Any) -> bytes:
    return json.dumps(
        data,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _sha256_text(data: Any) -> str:
    return f"sha256:{hashlib.sha256(_stable_json_bytes(data)).hexdigest()}"


def _managed_globals_scope_dir(base_dir: Path, *, scope_kind: str, scope_key: str) -> Path:
    digest = hashlib.sha1(f"{scope_kind}:{scope_key}".encode("utf-8")).hexdigest()
    return Path(base_dir) / scope_kind / digest


def _code_digest_from_code_version(code_version: str) -> str:
    digest = str(code_version or "").replace("sha256:", "").strip().lower()
    if not digest:
        raise ValueError("invalid code_version")
    return digest


def _code_scope_dir(base_dir: Path, *, code_version: str) -> Path:
    return Path(base_dir) / "codes" / _code_digest_from_code_version(code_version)


def _code_dependency_dir(base_dir: Path, *, code_version: str) -> Path:
    return _code_scope_dir(base_dir, code_version=code_version) / "deps"


def _code_meta_path(base_dir: Path, *, code_version: str) -> Path:
    return _code_scope_dir(base_dir, code_version=code_version) / "meta.json"


def _code_archive_path(base_dir: Path, *, code_version: str, package_format: str) -> Path:
    normalized = _normalize_package_format(package_format)
    code_dir = _code_scope_dir(base_dir, code_version=code_version)
    if normalized == "tar.gz":
        return code_dir / "artifact.tar.gz"
    if normalized == "zip":
        return code_dir / "artifact.zip"
    if normalized == "whl":
        return code_dir / "artifact.whl"
    raise ValueError(f"unsupported archive package_format: {package_format}")


def _code_exec_path(base_dir: Path, *, code_version: str, package_format: str) -> Path:
    normalized = _normalize_package_format(package_format)
    code_dir = _code_scope_dir(base_dir, code_version=code_version)
    if normalized == "py":
        return code_dir / "artifact.py"
    if normalized in ("tar.gz", "zip", "whl"):
        return code_dir / "pkg"
    raise ValueError(f"unsupported package_format for code exec path: {package_format}")


def _objects_meta_dir(object_dir: Path) -> Path:
    return Path(object_dir) / "meta"


def _object_meta_path(object_dir: Path, *, object_id: str) -> Path:
    digest = normalize_object_id(object_id).replace("sha256:", "", 1)
    return _objects_meta_dir(object_dir) / f"{digest}.json"


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
    meta_path = _code_meta_path(base_dir, code_version=code_version)
    if not meta_path.exists():
        return {}
    return json.loads(meta_path.read_text(encoding="utf-8") or "{}")


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
        "managed_global_names": list(artifact.managed_global_names),
        "artifact_path": artifact.path,
        "dependency_path": artifact.dependency_path,
        "size_bytes": int(artifact.size_bytes),
        "created_at": created_at,
        "last_at": effective_last_at,
    }
    tmp_path = meta_path.with_suffix(".tmp")
    tmp_path.write_bytes(_stable_json_bytes(payload))
    os.replace(str(tmp_path), str(meta_path))


def touch_code_last_at(base_dir: Path, *, code_version: str) -> None:
    meta = _load_code_meta(base_dir, code_version=code_version)
    if not meta:
        return
    meta_path = _code_meta_path(base_dir, code_version=code_version)
    meta["last_at"] = utc_now().astimezone(timezone.utc).isoformat()
    tmp_path = meta_path.with_suffix(".tmp")
    tmp_path.write_bytes(_stable_json_bytes(meta))
    os.replace(str(tmp_path), str(meta_path))


def _write_object_meta(
    object_dir: Path,
    *,
    object_id: str,
    fmt: str,
    size_bytes: int,
    created_at: datetime,
    last_at: Optional[datetime] = None,
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
    }
    tmp_path = meta_path.with_suffix(".tmp")
    tmp_path.write_bytes(_stable_json_bytes(payload))
    os.replace(str(tmp_path), str(meta_path))


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
        created_at_raw = str(meta.get("created_at", "") or "").strip()
        try:
            created_at = datetime.fromisoformat(created_at_raw)
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
        except Exception:
            created_at = now
        _write_object_meta(
            object_root,
            object_id=object_id,
            fmt=str(meta.get("format", "") or "bin"),
            size_bytes=int(meta.get("size_bytes", 0) or 0),
            created_at=created_at,
            last_at=now,
        )
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


def _sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(max(1, int(chunk_size)))
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _commit_result_file(source_path: Path, *, object_dir: str, fmt: str, size_bytes: int, materialize_as: str) -> StoredResultArtifact:
    root = Path(str(object_dir or "")).resolve()
    root.mkdir(parents=True, exist_ok=True)
    digest = _sha256_file(source_path)
    object_id = object_id_from_sha256_hex(digest)
    normalized_format = normalize_object_format(fmt, source_name=source_path.name, default="bin")
    final_path = object_storage_path(root, object_id=object_id, fmt=normalized_format)
    if not final_path.exists():
        os.replace(str(source_path), str(final_path))
    else:
        source_path.unlink(missing_ok=True)
    created_at = utc_now()
    _write_object_meta(
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
    )


def _store_result_path(path: Path, *, object_dir: str) -> StoredResultArtifact:
    if not path.exists() or not path.is_file():
        raise LargeResultError(f"returned path is not a readable file: {path}")
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
    fd, tmp_name = tempfile.mkstemp(prefix="pycloud-result-", suffix=".parquet", dir=str(Path(object_dir).resolve()))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        frame.to_parquet(tmp_path)
        return _commit_result_file(
            tmp_path,
            object_dir=object_dir,
            fmt="parquet",
            size_bytes=tmp_path.stat().st_size,
            materialize_as="dataframe",
        )
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _store_result_ndarray(array: Any, *, object_dir: str) -> StoredResultArtifact:
    fd, tmp_name = tempfile.mkstemp(prefix="pycloud-result-", suffix=".npy", dir=str(Path(object_dir).resolve()))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        import numpy as np

        with tmp_path.open("wb") as fh:
            np.save(fh, array, allow_pickle=False)
        return _commit_result_file(
            tmp_path,
            object_dir=object_dir,
            fmt="npy",
            size_bytes=tmp_path.stat().st_size,
            materialize_as="ndarray",
        )
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _normalize_result_value(ret: Any, *, object_dir: str) -> Any:
    if isinstance(ret, Path):
        return _store_result_path(ret, object_dir=object_dir)

    try:
        import pandas as pd

        if isinstance(ret, pd.DataFrame):
            return _store_result_dataframe(ret, object_dir=object_dir)
    except ImportError:
        pass

    try:
        import numpy as np

        if isinstance(ret, np.ndarray):
            return _store_result_ndarray(ret, object_dir=object_dir)
    except ImportError:
        pass

    serialized = serialize_arrow_compatible(ret)
    wrapped = serialized if isinstance(serialized, dict) else {"value": serialized}
    serialize_inline_result(wrapped, context="task result")
    return wrapped


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
    method_name: str,
    payload: dict,
    managed_globals_scope_dir: str = "",
    managed_globals_digest: str = "",
) -> Dict[str, Any]:
    return {
        "artifact_path": artifact.path,
        "entry_module": artifact.entry_module,
        "package_format": artifact.package_format,
        "dependency_path": artifact.dependency_path,
        "object_dir": str(object_dir),
        "export_mode": artifact.export_mode,
        "export_methods": list(artifact.export_methods),
        "export_decorator": artifact.export_decorator,
        "method_name": method_name,
        "entry_callable": artifact.entry_callable,
        "payload": payload or {},
        "managed_globals_scope_dir": str(managed_globals_scope_dir or ""),
        "managed_globals_digest": str(managed_globals_digest or ""),
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
) -> Dict[str, Tuple[str, str]]:
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
        return dict(method_info)
    finally:
        _purge_loaded_artifact_modules(
            artifact_path,
            entry_module=entry_module,
            package_format=package_format,
            dependency_path=dependency_path,
        )


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
) -> Dict[str, Tuple[str, str]]:
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
    - DataFrame → dict (JSON records)
    - Series → dict
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
            return fn(*args, **kwargs)

    # HTTP 风格：整个 payload 作为 kwargs
    # 这样服务端可以用 def square(**payload) 或 def square(x) 接收
    if isinstance(payload, dict):
        # 反序列化 Arrow 对象
        deserialized = convert_dict_to_arrow(payload)
        return fn(**deserialized)

    # 其他情况：直接传递 payload
    return fn(payload)


class ObjectResolutionError(RuntimeError):
    """Raised when an ObjectRef cannot be materialized on the node."""


def _resolve_object_refs_in_payload(payload: Any, *, object_dir: str) -> Any:
    root = Path(str(object_dir or "")).resolve()

    def _resolve(value: Any) -> Any:
        if isinstance(value, ObjectRef):
            materialized = normalize_materialize_as(value.materialize_as, default="path")

            def _materialize_path(candidate: Path) -> Any:
                if materialized == "path":
                    return candidate
                if materialized == "bytes":
                    return candidate.read_bytes()
                if materialized == "json":
                    import json

                    return json.loads(candidate.read_text(encoding="utf-8"))
                if materialized == "ndarray":
                    try:
                        import numpy as np
                    except ImportError as exc:
                        raise ObjectResolutionError("numpy not available on node, cannot materialize ndarray") from exc
                    return np.load(candidate, allow_pickle=False)
                if materialized == "dataframe":
                    try:
                        import pandas as pd
                    except ImportError as exc:
                        raise ObjectResolutionError("pandas not available on node, cannot materialize dataframe") from exc
                    return pd.read_parquet(candidate)
                raise ObjectResolutionError(f"unsupported materialize_as: {value.materialize_as}")

            candidate = object_storage_path(root, object_id=value.object_id, fmt=value.format)
            if candidate.exists():
                _touch_object_last_at(root, object_id=value.object_id, fallback_path=candidate)
                return _materialize_path(candidate)
            digest = normalize_object_id(value.object_id).replace("sha256:", "", 1)
            fallback = sorted(root.glob(f"{digest}*"))
            if fallback:
                _touch_object_last_at(root, object_id=value.object_id, fallback_path=fallback[0])
                return _materialize_path(fallback[0])
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
    managed_globals_scope_dir: str,
    managed_globals_digest: str,
    export_mode: str,
    export_methods: Sequence[str],
    export_decorator: str,
    method_name: str,
    entry_callable: str,
    payload: dict,
) -> Tuple[str, Optional[dict], str, str]:
    """Execute uploaded user code in subprocess.

    Returns:
        (status_text, result, error_type, error_message)
    """
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
            )
        return ("FAILED_INFRA", None, exc.__class__.__name__, repr(exc))
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
            ret = _invoke_user_callable(fn, resolved_payload)
        return _normalize_user_return(ret, object_dir=object_dir)
    except LargeResultError as exc:
        return ("FAILED_USER", None, exc.__class__.__name__, str(exc))
    except ObjectResolutionError as exc:
        return ("FAILED_INFRA", None, exc.__class__.__name__, str(exc))
    except Exception as exc:
        if isinstance(exc, (ImportError, ModuleNotFoundError)):
            return ("FAILED_USER", None, exc.__class__.__name__, _describe_user_execution_error(exc))
        return ("FAILED_USER", None, exc.__class__.__name__, repr(exc))


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
    lease_expire_at: datetime = field(default_factory=utc_now)
    http_base_url: str = ""


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
    active_runtimes: List[str] = field(default_factory=list)
    service_worker_capacity: int = 0
    service_worker_used: int = 0
    schedulable: bool = True
    drain: bool = False
    reason: str = ""

    def service_worker_available(self) -> int:
        capacity = max(0, int(self.service_worker_capacity or 0))
        used = max(0, int(self.service_worker_used or 0))
        return max(0, capacity - used)

    def active_runtime_count(self) -> int:
        return len(self.active_runtimes)


class InfoCenterState:
    def __init__(self, *, lease_ttl_sec: int = 90, heartbeat_interval_sec: int = 30) -> None:
        self.lease_ttl_sec = max(1, lease_ttl_sec)
        self.heartbeat_interval_sec = max(1, heartbeat_interval_sec)
        self._lock = threading.Lock()
        self._nodes: Dict[str, NodeState] = {}

    def register_node_record(
        self,
        *,
        node_id: str,
        control_addr: str,
        capacity: int,
        queue_capacity: int,
        tags: Iterable[str] = (),
        version: str = "",
        metadata: Optional[Dict[str, str]] = None,
        services: Optional[Dict[str, NodeServiceState]] = None,
        active_runtimes: Optional[Sequence[str]] = None,
        service_worker_capacity: int = 0,
        service_worker_used: int = 0,
        python_version: str = "",
    ) -> NodeState:
        now = utc_now()
        with self._lock:
            state = self._nodes.get(node_id)
            if state is None:
                state = NodeState(
                    node_id=node_id,
                    control_addr=control_addr,
                    capacity=max(1, capacity),
                    queue_capacity=max(1, queue_capacity),
                    python_version=str(python_version or "").strip(),
                )
                self._nodes[node_id] = state
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
            state.active_runtimes = [str(x).strip() for x in (active_runtimes or []) if str(x).strip()]
            state.service_worker_capacity = max(0, int(service_worker_capacity or 0))
            state.service_worker_used = max(0, min(int(service_worker_used or 0), state.service_worker_capacity or int(service_worker_used or 0)))
            if state.metrics.credit == 0:
                state.metrics.credit = state.queue_capacity
            return state

    def register_node(self, request: pb2.RegisterNodeRequest) -> NodeState:
        metadata = dict(request.metadata)
        return self.register_node_record(
            node_id=request.node_id,
            control_addr=request.control_addr,
            capacity=max(1, request.capacity),
            queue_capacity=max(1, request.queue_capacity),
            tags=request.tags,
            version=request.version,
            metadata=metadata,
            services=self._parse_services(request.services),
            active_runtimes=(),
            service_worker_capacity=int(metadata.get("service_worker_capacity", "0") or 0),
            service_worker_used=int(metadata.get("service_worker_used", "0") or 0),
            python_version=metadata.get("python_version", ""),
        )

    def heartbeat_record(
        self,
        *,
        node_id: str,
        healthy: bool,
        metrics: Optional[NodeMetricsState] = None,
        services: Optional[Dict[str, NodeServiceState]] = None,
        active_runtimes: Optional[Sequence[str]] = None,
        service_worker_capacity: int = 0,
        service_worker_used: int = 0,
        python_version: str = "",
    ) -> Optional[NodeState]:
        now = utc_now()
        with self._lock:
            state = self._nodes.get(node_id)
            if state is None:
                return None
            state.healthy = bool(healthy)
            state.last_seen_at = now
            if metrics is not None:
                state.metrics = metrics
            state.services = dict(services or {})
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
            return state

    def heartbeat(self, request: pb2.HeartbeatNodeRequest) -> Optional[NodeState]:
        return self.heartbeat_record(
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

    def list_service_routes(self, *, service_name: str, healthy_only: bool, limit: int) -> List[Dict[str, object]]:
        now = utc_now()
        name_filter = service_name.strip()
        with self._lock:
            out: List[Dict[str, object]] = []
            for state in self._nodes.values():
                stale = (now - state.last_seen_at).total_seconds() > float(self.lease_ttl_sec)
                is_healthy = state.healthy and not stale
                if healthy_only and not is_healthy:
                    continue
                for svc in state.services.values():
                    if name_filter and svc.service_name != name_filter:
                        continue
                    out.append(
                        {
                            "service_name": svc.service_name,
                            "service_id": svc.service_id,
                            "status": svc.status,
                            "node_id": state.node_id,
                            "control_addr": state.control_addr,
                            "node_healthy": is_healthy,
                            "worker_count": svc.worker_count,
                            "alive_workers": svc.alive_workers,
                            "in_flight": svc.in_flight,
                            "lease_expire_at": svc.lease_expire_at,
                            "http_base_url": svc.http_base_url,
                        }
                    )
            out.sort(
                key=lambda x: (
                    x["service_name"],
                    not x["node_healthy"],
                    int(x["status"] != pb2.SERVICE_STATUS_RUNNING),
                    int(x["in_flight"]),
                    x["node_id"],
                )
            )
            return out[: max(1, limit)]

    def list_nodes(self, *, healthy_only: bool, tags: Iterable[str], limit: int) -> List[NodeState]:
        now = utc_now()
        filter_tags = set(tags)
        with self._lock:
            out: List[NodeState] = []
            for state in self._nodes.values():
                stale = (now - state.last_seen_at).total_seconds() > float(self.lease_ttl_sec)
                is_healthy = state.healthy and not stale
                if healthy_only and not is_healthy:
                    continue
                if filter_tags and not filter_tags.issubset(set(state.tags)):
                    continue
                out.append(
                    NodeState(
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
                        active_runtimes=list(state.active_runtimes),
                        service_worker_capacity=state.service_worker_capacity,
                        service_worker_used=state.service_worker_used,
                        schedulable=state.schedulable,
                        drain=state.drain,
                        reason=state.reason,
                    )
                )
            out.sort(key=lambda n: (not n.healthy, not n.schedulable, n.drain, -(n.service_worker_available())))
            return out[: max(1, limit)]

    def update_node_schedule_state(
        self,
        node_id: str,
        *,
        schedulable: Optional[bool] = None,
        drain: Optional[bool] = None,
        reason: Optional[str] = None,
    ) -> NodeState:
        with self._lock:
            state = self._nodes.get(node_id)
            if state is None:
                raise KeyError("node not found")
            if schedulable is not None:
                state.schedulable = bool(schedulable)
            if drain is not None:
                state.drain = bool(drain)
            if reason is not None:
                state.reason = str(reason or "")
            return NodeState(
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
                active_runtimes=list(state.active_runtimes),
                service_worker_capacity=state.service_worker_capacity,
                service_worker_used=state.service_worker_used,
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
    managed_global_names: Tuple[str, ...]
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
    managed_globals_scope_dir: str = ""
    managed_globals_digest: str = ""


@dataclass
class RuntimeSlotState:
    runtime_key: str
    code_version: str
    task_ids: Deque[str] = field(default_factory=deque)
    executor: Optional[object] = None
    executor_ready: bool = False
    current_task_id: str = ""
    current_attempt: int = 0
    last_used_at: datetime = field(default_factory=utc_now)
    waiting: bool = False


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
        runtime_slot_capacity: int = 0,
        runtime_slot_idle_ttl_sec: int = 30,
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
        service_http_bind: str = "127.0.0.1:18080",
        service_http_base_url: str = "",
    ) -> None:
        self.node_id = node_id
        self.worker_capacity = max(1, worker_capacity)
        self.queue_capacity = max(1, queue_capacity)
        self.runtime_slot_capacity = max(1, int(runtime_slot_capacity or worker_capacity))
        self.runtime_slot_idle_ttl_sec = max(1, int(runtime_slot_idle_ttl_sec))
        self.heartbeat_timeout_sec = max(5, heartbeat_timeout_sec)
        self.max_retries = max(0, max_retries)
        self.monitor_interval_sec = max(1, monitor_interval_sec)
        self.enable_internal_executor = bool(enable_internal_executor)
        self.executor_poll_interval_sec = max(0.01, float(executor_poll_interval_sec))
        self.enable_service_session = bool(enable_service_session)
        self.service_default_worker_count = max(1, service_default_worker_count)
        self.service_default_heartbeat_timeout_sec = max(5, service_default_heartbeat_timeout_sec)
        self.service_worker_capacity = max(1, int(service_worker_capacity or worker_capacity))
        self.service_http_bind = service_http_bind
        self.service_http_base_url = service_http_base_url.strip()
        self.started_at = utc_now()

        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._pending: Deque[str] = deque()
        self._tasks: Dict[str, TaskState] = {}
        self._codes: Dict[str, CodeArtifact] = {}
        self._objects: Dict[str, ObjectArtifact] = {}
        self._runtime_slots: Dict[str, RuntimeSlotState] = {}
        self._runtime_waiting: Deque[str] = deque()
        self._services: Dict[str, ServiceSession] = {}
        self._result_hook = InMemoryResultHook()

        # 检测并保存当前 Python 版本
        self._python_version = f"py{sys.version_info.major}.{sys.version_info.minor}"

        self._artifact_dir = Path(artifact_dir)
        self._artifact_dir.mkdir(parents=True, exist_ok=True)
        self._codes_dir = self._artifact_dir / "codes"
        self._codes_dir.mkdir(parents=True, exist_ok=True)
        self._object_dir = self._artifact_dir / "objects"
        self._object_dir.mkdir(parents=True, exist_ok=True)
        self._runtime_managed_globals: Dict[Tuple[str, str, str], ManagedGlobalsState] = {}
        self._client_code_tokens: Dict[Tuple[str, str], str] = {}

        self._stop_event = threading.Event()
        self._monitor = threading.Thread(target=self._monitor_loop, name="nodecontrol-monitor", daemon=True)
        self._monitor.start()
        self._executor_host = ExecutorHostClient() if (self.enable_internal_executor or self.enable_service_session) else None
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
        code_dir = _code_scope_dir(self._artifact_dir, code_version=code_version)
        scopes_dir = code_dir / "scopes"
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

    def _ensure_service_managed_globals_state_locked(
        self,
        session: ServiceSession,
        *,
        artifact: CodeArtifact,
    ) -> Optional[ManagedGlobalsState]:
        if not artifact.managed_global_names:
            session.managed_globals_scope_dir = ""
            session.managed_globals_digest = ""
            return None
        if not session.managed_globals_scope_dir:
            state = self._new_managed_globals_state(
                code_version=session.code_version,
                scope_kind="service",
                scope_key=session.service_id,
                allowed_names=artifact.managed_global_names,
            )
            session.managed_globals_scope_dir = state.scope_dir
            session.managed_globals_digest = state.globals_digest
            return state
        return ManagedGlobalsState(
            scope_kind="service",
            scope_key=session.service_id,
            scope_dir=session.managed_globals_scope_dir,
            allowed_names=_normalize_managed_global_names(artifact.managed_global_names),
            globals_digest=session.managed_globals_digest,
        )

    def _ensure_runtime_managed_globals_state_locked(
        self,
        *,
        client_id: str = "",
        code_version: str,
        runtime_key: str,
        artifact: CodeArtifact,
    ) -> Optional[ManagedGlobalsState]:
        if not artifact.managed_global_names:
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
                scope_key=f"{normalized_key[0]}|{normalized_key[1]}|{normalized_key[2]}",
                allowed_names=artifact.managed_global_names,
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
            current_values[name] = serialize_arrow_compatible(value)
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

    def get_client_code_token(self, *, client_id: str, code_version: str) -> str:
        normalized_client_id = str(client_id or "").strip()
        normalized_code_version = str(code_version or "").strip()
        with self._lock:
            return str(self._client_code_tokens.get((normalized_client_id, normalized_code_version), "") or "")

    def _executor_host_required(self) -> bool:
        return bool(self.enable_internal_executor or self.enable_service_session)

    def _executor_host_alive_locked(self) -> bool:
        return self._executor_host is not None and self._executor_host.is_alive()

    def _ensure_executor_host_alive_locked(self, *, now: Optional[datetime] = None) -> None:
        if not self._executor_host_required():
            return
        if self._executor_host_alive_locked():
            return

        current_time = now or utc_now()
        old_host = self._executor_host
        self._executor_host = ExecutorHostClient()

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

        for runtime_key, slot in list(self._runtime_slots.items()):
            if slot.current_task_id:
                task = self._tasks.get(slot.current_task_id)
                task_attempt = slot.current_attempt
                self._reset_runtime_slot_locked(runtime_key, now=current_time, ensure_host=False)
                if task is not None and task.attempt == task_attempt and task.status == pb2.TASK_STATUS_RUNNING:
                    self._handle_infra_failure_locked(
                        task,
                        reason="executor host restarted during task execution",
                        now=current_time,
                    )
                continue
            slot.executor = None
            slot.executor_ready = False

        if old_host is not None:
            try:
                old_host.close()
            except Exception:
                pass

    def get_object_artifact(self, object_id: str) -> ObjectArtifact:
        normalized = normalize_object_id(object_id)
        with self._lock:
            artifact = self._objects.get(normalized)
            if artifact is not None and Path(artifact.path).exists():
                return artifact
        candidate = object_storage_path(self._object_dir, object_id=normalized, fmt="bin")
        digest = normalized.replace("sha256:", "", 1)
        fallback = sorted(
            path
            for path in self._object_dir.glob(f"{digest}*")
            if path.is_file()
        )
        if candidate.exists():
            return ObjectArtifact(
                object_id=normalized,
                path=str(candidate),
                format=normalize_object_format(candidate.suffix, source_name=candidate.name, default="bin"),
                size_bytes=candidate.stat().st_size,
                created_at=utc_now(),
            )
        if fallback:
            path = fallback[0]
            return ObjectArtifact(
                object_id=normalized,
                path=str(path),
                format=normalize_object_format("", source_name=path.name, default="bin"),
                size_bytes=path.stat().st_size,
                created_at=utc_now(),
            )
        raise KeyError("object not found")

    def _dependency_dir_for_code_version(self, code_version: str) -> Path:
        return _code_dependency_dir(self._artifact_dir, code_version=code_version)

    def _validate_managed_global_names(
        self,
        artifact: CodeArtifact,
        *,
        dependency_path: str,
    ) -> None:
        if not artifact.managed_global_names:
            return
        module = _load_user_module(
            artifact.path,
            entry_module=artifact.entry_module,
            package_format=artifact.package_format,
            dependency_path=dependency_path,
        )
        try:
            missing = [name for name in artifact.managed_global_names if not hasattr(module, name)]
            if missing:
                raise ValueError(f"managed globals not found in entry module: {missing}")
            invalid = []
            for name in artifact.managed_global_names:
                value = getattr(module, name)
                if inspect.ismodule(value) or inspect.isfunction(value) or inspect.isclass(value) or callable(value):
                    invalid.append(name)
            if invalid:
                raise ValueError(f"managed globals must be data values, not callables/modules/classes: {invalid}")
        finally:
            _purge_loaded_artifact_modules(
                artifact.path,
                entry_module=artifact.entry_module,
                package_format=artifact.package_format,
                dependency_path=dependency_path,
            )

    def _validate_artifact_methods(
        self,
        artifact: CodeArtifact,
        *,
        dependency_path: str,
    ) -> Dict[str, Tuple[str, str]]:
        methods = _discover_callable_methods(
            artifact.path,
            entry_module=artifact.entry_module,
            package_format=artifact.package_format,
            dependency_path=dependency_path,
            export_mode=artifact.export_mode,
            export_methods=artifact.export_methods,
            export_decorator=artifact.export_decorator,
            entry_callable=artifact.entry_callable,
        )
        self._validate_managed_global_names(artifact, dependency_path=dependency_path)
        return methods

    def _ensure_artifact_ready(
        self,
        artifact: CodeArtifact,
        *,
        dependency_allowlist: Sequence[str],
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
            method_info = self._validate_artifact_methods(artifact, dependency_path=candidate_dependency_path)
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
                method_info = self._validate_artifact_methods(artifact, dependency_path=str(target_dir))
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
            return sum(
                max(0, int(session.worker_count))
                for session in self._services.values()
                if session.status in (
                    pb2.SERVICE_STATUS_STARTING,
                    pb2.SERVICE_STATUS_RUNNING,
                    pb2.SERVICE_STATUS_DRAINING,
                )
            )

    def service_worker_available(self) -> int:
        return max(0, int(self.service_worker_capacity) - int(self.service_worker_used()))

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

        code_version = f"sha256:{digest}"
        normalized_dependency_allowlist = _normalize_dependency_allowlist(dependency_allowlist)
        normalized_managed_global_names = _normalize_managed_global_names(managed_global_names)
        with self._lock:
            existing = self._codes.get(code_version)
            if existing is not None:
                if existing.managed_global_names != normalized_managed_global_names and (
                    existing.managed_global_names or normalized_managed_global_names
                ):
                    raise ValueError(
                        "cached code_version exists with different managed_global_names: "
                        f"existing={list(existing.managed_global_names)} incoming={list(normalized_managed_global_names)}"
                    )
                if validate_load:
                    self._ensure_artifact_ready(
                        existing,
                        dependency_allowlist=normalized_dependency_allowlist,
                    )
                if str(client_id or "").strip():
                    self._register_client_code_token_locked(
                        client_id=client_id,
                        code_version=code_version,
                        code_token=code_token,
                    )
                return existing, True

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

        tmp_path = Path(uploaded_path)
        if not tmp_path.exists():
            raise ValueError(f"uploaded file missing: {uploaded_path}")

        now = utc_now()
        code_dir = _code_scope_dir(self._artifact_dir, code_version=code_version)
        cleanup_paths: List[Path] = [code_dir]
        code_dir.mkdir(parents=True, exist_ok=True)
        if normalized_format == "py":
            final_path = _code_exec_path(self._artifact_dir, code_version=code_version, package_format=normalized_format)
            os.replace(str(tmp_path), str(final_path))
            artifact_exec_path = str(final_path)
        else:
            archive_path = _code_archive_path(self._artifact_dir, code_version=code_version, package_format=normalized_format)
            os.replace(str(tmp_path), str(archive_path))
            extract_dir = _code_exec_path(self._artifact_dir, code_version=code_version, package_format=normalized_format)
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
            managed_global_names=normalized_managed_global_names,
            dependency_path="",
            size_bytes=max(0, int(size_bytes)),
            created_at=now,
        )
        if validate_load:
            try:
                self._ensure_artifact_ready(
                    artifact,
                    dependency_allowlist=normalized_dependency_allowlist,
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
        with self._lock:
            existing = self._objects.get(actual_object_id)
            if existing is not None and Path(existing.path).exists():
                return existing, True

        now = utc_now()
        final_path = object_storage_path(self._object_dir, object_id=actual_object_id, fmt=normalized_format)
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
            managed_global_names=managed_global_names,
            chunks=chunks,
            validate_load=True,
        )
        method_info = self._ensure_artifact_ready(
            artifact,
            dependency_allowlist=dependency_allowlist,
        )

        requested_workers = max(1, worker_count or self.service_default_worker_count)
        available_workers = self.service_worker_available()
        if available_workers <= 0:
            raise RuntimeError("service worker capacity exhausted")
        actual_workers = min(requested_workers, available_workers)
        actual_hb_timeout = max(5, heartbeat_timeout_sec or self.service_default_heartbeat_timeout_sec)
        actual_idle_ttl = max(0, idle_ttl_sec)
        now = utc_now()
        service_id = uuid.uuid4().hex
        token = secrets.token_urlsafe(24)
        http_base = f"{self.service_http_base_url}/svc/{service_id}" if (expose_http and self.service_http_base_url) else ""

        self._ensure_executor_host_alive_locked(now=now)
        if self._executor_host is None:
            raise RuntimeError("executor host unavailable")
        self._executor_host.create_service(service_id=service_id, worker_count=actual_workers)
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
        )
        managed_state = self._ensure_service_managed_globals_state_locked(session, artifact=artifact)
        if managed_state is not None:
            session.managed_globals_scope_dir = managed_state.scope_dir
            session.managed_globals_digest = managed_state.globals_digest
        with self._lock:
            self._services[service_id] = session
        return session

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
            session.in_flight += 1

        try:
            resp = self._executor_host.call_service(
                service_id=service_id,
                timeout_sec=max(0.1, timeout_sec),
                execute_spec=_build_execute_spec(
                    artifact,
                    object_dir=self._object_dir,
                    method_name=requested_method,
                    payload=payload or {},
                    managed_globals_scope_dir=session.managed_globals_scope_dir,
                    managed_globals_digest=session.managed_globals_digest,
                ),
            )
            if not resp.get("ok", False):
                if resp.get("timeout", False):
                    raise FutureTimeout()
                raise RuntimeError(str(resp.get("error", "service invoke failed")))
            status_text = str(resp.get("status_text", "FAILED_INFRA") or "FAILED_INFRA")
            result = resp.get("result")
            err_type = str(resp.get("err_type", "") or "")
            err_message = str(resp.get("err_message", "") or "")
        except FutureTimeout:
            with self._lock:
                session = self._services.get(service_id)
                if session is not None:
                    session.in_flight = max(0, session.in_flight - 1)
            return 504, {"ok": False, "error": "invoke timeout"}
        except Exception as exc:
            with self._lock:
                session = self._services.get(service_id)
                if session is not None:
                    session.in_flight = max(0, session.in_flight - 1)
            return 500, {"ok": False, "error": repr(exc)}

        with self._lock:
            session = self._services.get(service_id)
            if session is not None:
                session.in_flight = max(0, session.in_flight - 1)

        if status_text == "SUCCEEDED":
            if isinstance(result, StoredResultArtifact):
                result = _stored_result_to_result_ref(result, node_id=self.node_id)
            return 200, {"ok": True, "method": requested_method, "data": result or {}}
        if status_text == "FAILED_USER":
            return 400, {
                "ok": False,
                "method": requested_method,
                "error_type": err_type or "UserError",
                "error": err_message or "user error",
            }
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
            artifact = self._codes.get(session.code_version)
            if artifact is None:
                raise RuntimeError("artifact missing")
            state = self._ensure_service_managed_globals_state_locked(session, artifact=artifact)
            if state is None:
                raise ValueError("service artifact did not declare managed globals")
            globals_digest, updated_names = self._update_managed_globals_state(state, values=values)
            session.managed_globals_scope_dir = state.scope_dir
            session.managed_globals_digest = globals_digest
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
            artifact = self._codes.get(normalized_code_version)
            if artifact is None:
                raise KeyError("code artifact not found")
            expected_code_token = self._client_code_tokens.get((normalized_client_id, normalized_code_version), "")
            if not code_token or not expected_code_token or expected_code_token != code_token:
                raise PermissionError("code_token mismatch")
            state = self._ensure_runtime_managed_globals_state_locked(
                client_id=normalized_client_id,
                code_version=normalized_code_version,
                runtime_key=normalized_runtime_key,
                artifact=artifact,
            )
            if state is None:
                raise ValueError("task artifact did not declare managed globals")
            globals_digest, updated_names = self._update_managed_globals_state(state, values=values)
            self._runtime_managed_globals[(normalized_client_id, normalized_code_version, normalized_runtime_key)] = state
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
                "in_flight": session.in_flight,
                "queued": session.queued,
                "created_at": session.created_at,
                "last_heartbeat_at": session.last_heartbeat_at,
                "lease_expire_at": session.lease_expire_at,
                "http_base_url": session.http_base_url,
                "methods": sorted(session.methods.keys()),
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

                existing_slot = self._runtime_slots.get(runtime_key)
                if existing_slot is not None and existing_slot.code_version != request.code_version:
                    rejected.append(
                        pb2.TaskRejected(
                            task_id=item.task_id,
                            code=pb2.ERROR_CODE_INVALID_REQUEST,
                            message=f"runtime_key `{runtime_key}` already bound to {existing_slot.code_version}",
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
                if self.enable_internal_executor:
                    slot = self._runtime_slots.get(runtime_key)
                    if slot is None:
                        slot = RuntimeSlotState(runtime_key=runtime_key, code_version=request.code_version)
                        self._runtime_slots[runtime_key] = slot
                    slot.task_ids.append(item.task_id)
                    slot.last_used_at = utc_now()
                    if not slot.executor_ready and not slot.waiting and self._active_runtime_slot_count_locked() >= self.runtime_slot_capacity:
                        slot.waiting = True
                        self._runtime_waiting.append(runtime_key)
                else:
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
            elif request.status == pb2.TASK_STATUS_FAILED_USER:
                task.status = pb2.TASK_STATUS_FAILED_USER
                task.result = None
                task.error_type = request.error.type
                task.error_message = request.error.message
            else:
                self._handle_infra_failure_locked(
                    task,
                    reason=request.error.message or request.error.type or "infra failure",
                    now=task.finished_at,
                )
                should_publish = task.status == pb2.TASK_STATUS_FAILED_INFRA

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
            inflight = self._inflight_count_locked()
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
                        in_flight=session.in_flight,
                        lease_expire_at=dt_to_ts(session.lease_expire_at),
                        http_base_url=session.http_base_url,
                    )
                )
            return out

    def active_runtime_keys(self, *, limit: int = 10) -> List[str]:
        with self._lock:
            rows: List[Tuple[int, int, float, str]] = []
            for runtime_key, slot in self._runtime_slots.items():
                queued = len(slot.task_ids)
                hot = 1 if slot.executor_ready else 0
                last_used = slot.last_used_at.timestamp() if slot.last_used_at else 0.0
                rows.append((hot, queued, last_used, runtime_key))
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

    def _active_runtime_slot_count_locked(self) -> int:
        return sum(1 for slot in self._runtime_slots.values() if slot.executor_ready)

    def _queued_count_locked(self) -> int:
        return sum(1 for task in self._tasks.values() if task.status == pb2.TASK_STATUS_QUEUED)

    def _inflight_count_locked(self) -> int:
        return sum(1 for task in self._tasks.values() if task.status == pb2.TASK_STATUS_RUNNING)

    def _publish_result_locked(self, task: TaskState) -> None:
        result = task.as_result()
        self._result_hook.push(task.client_id, result)

    def _reset_runtime_slot_locked(
        self,
        runtime_key: str,
        *,
        now: datetime,
        drop_queued: bool = False,
        ensure_host: bool = True,
    ) -> None:
        slot = self._runtime_slots.get(runtime_key)
        if slot is None:
            return
        if ensure_host:
            self._ensure_executor_host_alive_locked(now=now)
        if self._executor_host is not None and slot.executor_ready:
            try:
                self._executor_host.stop_runtime_slot(runtime_key=runtime_key)
            except Exception:
                pass
        slot.executor = None
        slot.executor_ready = False
        slot.current_task_id = ""
        slot.current_attempt = 0
        slot.last_used_at = now
        if drop_queued:
            slot.task_ids.clear()

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
            if self.enable_internal_executor:
                slot = self._runtime_slots.get(task.runtime_key)
                if slot is None:
                    slot = RuntimeSlotState(runtime_key=task.runtime_key, code_version=task.code_version)
                    self._runtime_slots[task.runtime_key] = slot
                slot.task_ids.append(task.task_id)
                slot.last_used_at = now
                if not slot.executor_ready and not slot.waiting and self._active_runtime_slot_count_locked() >= self.runtime_slot_capacity:
                    slot.waiting = True
                    self._runtime_waiting.append(task.runtime_key)
            else:
                self._pending.append(task.task_id)
            return

        task.status = pb2.TASK_STATUS_FAILED_INFRA
        task.finished_at = now
        task.error_type = "InfraFailure"
        task.error_message = reason
        self._publish_result_locked(task)

    def _start_runtime_slot_locked(self, slot: RuntimeSlotState) -> None:
        if slot.executor_ready:
            return
        self._ensure_executor_host_alive_locked()
        if self._executor_host is None:
            raise RuntimeError("executor host unavailable")
        self._executor_host.start_runtime_slot(runtime_key=slot.runtime_key)
        slot.executor = _EXECUTOR_ACTIVE
        slot.executor_ready = True
        slot.current_task_id = ""
        slot.current_attempt = 0
        slot.last_used_at = utc_now()
        slot.waiting = False

    def _activate_waiting_runtime_slots_locked(self) -> None:
        while self._active_runtime_slot_count_locked() < self.runtime_slot_capacity and self._runtime_waiting:
            runtime_key = self._runtime_waiting.popleft()
            slot = self._runtime_slots.get(runtime_key)
            if slot is None:
                continue
            slot.waiting = False
            if slot.executor_ready:
                continue
            if not slot.task_ids:
                continue
            self._start_runtime_slot_locked(slot)

    def _reclaim_idle_runtime_slots_locked(self) -> None:
        now = utc_now()
        for runtime_key, slot in list(self._runtime_slots.items()):
            if not slot.executor_ready:
                if not slot.task_ids and not slot.waiting:
                    self._runtime_slots.pop(runtime_key, None)
                continue
            if slot.current_task_id or slot.task_ids:
                continue
            idle_sec = (now - slot.last_used_at).total_seconds()
            if idle_sec < self.runtime_slot_idle_ttl_sec:
                continue
            if self._executor_host is not None:
                try:
                    self._executor_host.stop_runtime_slot(runtime_key=runtime_key)
                except Exception:
                    pass
            slot.executor = None
            slot.executor_ready = False
            slot.current_task_id = ""
            slot.current_attempt = 0
            slot.last_used_at = now

    def _dispatch_runtime_slots_locked(self) -> None:
        if not self.enable_internal_executor:
            return
        self._activate_waiting_runtime_slots_locked()
        if self._active_runtime_slot_count_locked() < self.runtime_slot_capacity:
            for slot in self._runtime_slots.values():
                if self._active_runtime_slot_count_locked() >= self.runtime_slot_capacity:
                    break
                if not slot.executor_ready and slot.task_ids and not slot.waiting:
                    self._start_runtime_slot_locked(slot)

        for slot in self._runtime_slots.values():
            if not slot.executor_ready or slot.current_task_id:
                continue
            while slot.task_ids:
                task_id = slot.task_ids[0]
                task = self._tasks.get(task_id)
                if task is None or task.status != pb2.TASK_STATUS_QUEUED:
                    slot.task_ids.popleft()
                    continue
                if task.cancel_requested:
                    slot.task_ids.popleft()
                    task.status = pb2.TASK_STATUS_CANCELLED
                    task.finished_at = utc_now()
                    task.error_type = "Cancelled"
                    task.error_message = "cancelled by client"
                    self._publish_result_locked(task)
                    continue

                artifact = self._codes.get(task.code_version)
                if artifact is None:
                    slot.task_ids.popleft()
                    now = utc_now()
                    self._handle_infra_failure_locked(task, reason="missing code artifact", now=now)
                    continue

                slot.task_ids.popleft()
                now = utc_now()
                task.status = pb2.TASK_STATUS_RUNNING
                task.worker_id = f"runtime-slot:{slot.runtime_key}"
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
                        artifact=artifact,
                    )
                    self._executor_host.submit_runtime_task(
                        runtime_key=slot.runtime_key,
                        task_id=task.task_id,
                        attempt=task.attempt,
                        execute_spec=_build_execute_spec(
                            artifact,
                            object_dir=self._object_dir,
                            method_name=artifact.entry_callable,
                            payload=task.payload,
                            managed_globals_scope_dir=(managed_state.scope_dir if managed_state is not None else ""),
                            managed_globals_digest=(managed_state.globals_digest if managed_state is not None else ""),
                        ),
                    )
                except Exception as exc:
                    self._handle_infra_failure_locked(task, reason=repr(exc), now=now)
                    slot.current_task_id = ""
                    slot.current_attempt = 0
                    slot.last_used_at = now
                    continue
                slot.current_task_id = task.task_id
                slot.current_attempt = task.attempt
                slot.last_used_at = now
                break

    def _touch_internal_heartbeats_locked(self) -> None:
        now = utc_now()
        for slot in self._runtime_slots.values():
            if not slot.current_task_id:
                continue
            task = self._tasks.get(slot.current_task_id)
            if task is None:
                continue
            if task.attempt != slot.current_attempt:
                continue
            if task.status != pb2.TASK_STATUS_RUNNING:
                continue
            task.last_heartbeat_at = now

    def _drain_executor_events(self) -> None:
        self._ensure_executor_host_alive_locked()
        if self._executor_host is None:
            return
        for item in self._executor_host.drain_events():
            if str(item.get("kind", "") or "") != "runtime_task_done":
                continue
            runtime_key = str(item.get("runtime_key", "") or "")
            task_id = str(item.get("task_id", "") or "")
            attempt = int(item.get("attempt", 0) or 0)
            status_text = str(item.get("status_text", "FAILED_INFRA") or "FAILED_INFRA")
            result = item.get("result")
            err_type = str(item.get("err_type", "") or "")
            err_message = str(item.get("err_message", "") or "")
            now = utc_now()
            with self._cv:
                slot = self._runtime_slots.get(runtime_key)
                if slot is not None and slot.current_task_id == task_id and slot.current_attempt == attempt:
                    slot.current_task_id = ""
                    slot.current_attempt = 0
                    slot.last_used_at = now
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
                self._reclaim_idle_runtime_slots_locked()
                self._dispatch_runtime_slots_locked()
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
                    self._reset_runtime_slot_locked(task.runtime_key, now=now)
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
