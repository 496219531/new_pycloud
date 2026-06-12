from __future__ import annotations

"""Result storage/materialization helpers for NodeControl domain."""

import hashlib
import io
import json
import os
import shutil
import tempfile
import time
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from pycloud_parallel.controlplane.config import (
    FILE_HASH_CHUNK_SIZE_BYTES,
    OBJECT_SEGMENT_MAX_BYTES,
    OBJECT_SEGMENT_TARGET_BYTES,
    get_dataref_resolution,
    get_payload_policy,
    validate_bytes_materialize_size,
    validate_object_size_bytes,
)
from pycloud_parallel.data.ref import DataRef, coerce_data_ref, maybe_data_ref, resolve_data_ref_materialize_as
from pycloud_parallel.controlplane.data_store import DataStore
from pycloud_parallel.controlplane.node.filesystem import (
    _materialized_objects_dir,
    _segment_relpath,
    _segment_writer_key,
    _segment_writer_lock,
    _segments_dir,
)
from pycloud_parallel.controlplane.node.models import ObjectArtifact, StoredResultArtifact
from pycloud_parallel.controlplane.object_file_source import (
    dataframe_bundle_temp_file,
    ndarray_temp_file,
    series_bundle_temp_file,
)
from pycloud_parallel.controlplane.node.object_meta import (
    _load_object_meta,
    touch_object_last_at_throttled,
    _write_object_meta,
)
from pycloud_parallel.controlplane.serialization import (
    encode_transport_payload_bytes,
    convert_dict_to_arrow,
    deserialize_by_mode,
    log_payload_flow,
    serialize_arrow_compatible,
    summarize_payload_flow_value,
    transport_payload_to_inline_carrier,
    serialize_inline_result,
)
from pycloud_parallel.controlplane.payload_transport import estimate_payload_inline_size
from pycloud_parallel.controlplane.state_time import utc_now
from pycloud_parallel.data.ref import (
    normalize_materialize_as,
    normalize_object_format,
    normalize_object_id,
    object_format_suffix,
    object_id_from_sha256_hex,
    object_storage_path,
)


_SEGMENT_WRITER_STATE: Dict[Tuple[str, int], str] = {}


class LargeResultError(ValueError):
    """Raised when a task result is too large for safe inline return."""


class ObjectResolutionError(RuntimeError):
    """Raised when a large-data reference cannot be materialized on the node."""


def _materialize_object_bytes(*, blob: bytes, fmt: str, materialize_as: str) -> Any:
    materialized = normalize_materialize_as(materialize_as, default="path")
    normalized_format = normalize_object_format(fmt, default="bin")
    if normalized_format in {"structured_v1", "pickle_stable_v1", "pickle_native_v1"}:
        return deserialize_by_mode(blob, mode=normalized_format)
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
    raise ObjectResolutionError(f"blob-backed large-data wrapper does not support materialize_as={materialized!r}")


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


def _artifact_exists(artifact: ObjectArtifact) -> bool:
    if artifact.storage_backend == "segment":
        return bool(artifact.segment_path) and Path(artifact.segment_path).exists()
    return bool(artifact.path) and Path(artifact.path).exists()


def _object_artifact_from_meta(object_dir: Path, *, object_id: str, meta: Dict[str, Any]) -> ObjectArtifact:
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
            segment_path=str(Path(object_dir).resolve() / relpath),
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


def _read_object_artifact_bytes(artifact: ObjectArtifact) -> bytes:
    if artifact.storage_backend == "segment":
        with open(artifact.segment_path, "rb") as fp:
            fp.seek(max(0, int(artifact.segment_offset or 0)))
            return fp.read(max(0, int(artifact.segment_length or artifact.size_bytes or 0)))
    return Path(artifact.path).read_bytes()


def _materialized_object_path(root: Path, *, object_id: str, fmt: str) -> Path:
    return object_storage_path(_materialized_objects_dir(root), object_id=object_id, fmt=fmt)


def _data_ref_remote_targets(data_ref: DataRef) -> tuple[str, ...]:
    locator_kind = str(data_ref.locator_kind or "").strip().lower()
    control_addr = str(data_ref.control_addr or "").strip()
    locator_token = str(data_ref.locator_token or "").strip()
    if control_addr:
        return (control_addr,)
    if locator_kind == "node_control" and locator_token:
        return (locator_token,)
    if locator_kind in {"controlplane", "node_local", ""} and locator_token:
        from pycloud_parallel.controlplane.data_registry import DataRegistryClient

        try:
            resolved = DataRegistryClient(locator_token).resolve(data_ref)
        except Exception as exc:
            raise ObjectResolutionError(
                f"data ref registry resolve failed object_id={data_ref.object_id} registry_target={locator_token}: {exc}"
            ) from exc
        candidates = [str(item.get("control_addr", "") or "").strip() for item in resolved.replicas]
        candidates.append(str(resolved.control_addr or "").strip())
        return tuple(dict.fromkeys(item for item in candidates if item))
    return ()


def _cache_remote_data_ref(data_ref: DataRef, *, object_dir: Path, target: str) -> ObjectArtifact:
    started_at = time.perf_counter()
    from pycloud_parallel.controlplane.node_object_http import make_node_object_client

    normalized_id = normalize_object_id(data_ref.object_id)
    expected_size = int(data_ref.size_bytes or 0)
    normalized_format = normalize_object_format(data_ref.format, default="bin")
    final_path = object_storage_path(object_dir, object_id=normalized_id, fmt=normalized_format)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = final_path.with_name(f".{final_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with make_node_object_client(target) as client:
            client.download_object_to_file(object_id=data_ref.object_id, target_path=str(tmp_path))
    except Exception as exc:
        tmp_path.unlink(missing_ok=True)
        raise ObjectResolutionError(
            f"remote fetch failed object_id={data_ref.object_id} control_addr={target} "
            f"error_type={type(exc).__name__}: {exc}"
        ) from exc
    fetch_ms = (time.perf_counter() - started_at) * 1000.0

    actual_id = object_id_from_sha256_hex(_sha256_file(tmp_path))
    if actual_id != normalized_id:
        tmp_path.unlink(missing_ok=True)
        raise ObjectResolutionError(
            f"remote object checksum mismatch: expected {normalized_id}, got {actual_id}"
        )
    actual_size = tmp_path.stat().st_size
    if expected_size > 0 and expected_size != actual_size:
        tmp_path.unlink(missing_ok=True)
        raise ObjectResolutionError(
            f"remote object size mismatch: expected {expected_size}, got {actual_size}"
        )

    cache_started_at = time.perf_counter()
    try:
        _replace_file_with_retry(tmp_path, final_path)
    finally:
        tmp_path.unlink(missing_ok=True)
    created_at = utc_now()
    _write_object_meta_with_retry(
        object_dir,
        object_id=normalized_id,
        fmt=normalized_format,
        size_bytes=actual_size,
        created_at=created_at,
        last_at=created_at,
    )
    cache_ms = (time.perf_counter() - cache_started_at) * 1000.0
    log_payload_flow(
        "dataref_remote_fetch_done",
        object_id=normalized_id,
        target=target,
        worker_dataref_fetch_ms=fetch_ms,
        worker_dataref_cache_write_ms=cache_ms,
        size_bytes=actual_size,
    )
    return ObjectArtifact(
        object_id=normalized_id,
        path=str(final_path),
        format=normalized_format,
        size_bytes=actual_size,
        created_at=created_at,
        storage_backend="file",
    )


def _materialize_object_artifact(
    artifact: ObjectArtifact,
    *,
    materialize_as: str,
    root: Path,
) -> Any:
    if artifact.storage_backend == "file":
        candidate = Path(artifact.path)
        if str(artifact.format or "").strip().lower() in {"structured_v1", "pickle_stable_v1", "pickle_native_v1"}:
            validate_bytes_materialize_size(
                int(artifact.size_bytes or candidate.stat().st_size),
                context=f"object {artifact.object_id}",
            )
            return deserialize_by_mode(candidate.read_bytes(), mode=str(artifact.format or "").strip().lower())
        if materialize_as == "path":
            return candidate
        if materialize_as == "bytes":
            validate_bytes_materialize_size(
                int(artifact.size_bytes or candidate.stat().st_size),
                context=f"object {artifact.object_id}",
            )
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


def _append_file_to_segment(
    object_dir: Path,
    *,
    object_id: str,
    fmt: str,
    source_path: Path,
    size_bytes: int,
    materialize_as: str,
    created_at: Optional[datetime] = None,
) -> StoredResultArtifact:
    root = Path(object_dir).resolve()
    segments_root = _segments_dir(root)
    segments_root.mkdir(parents=True, exist_ok=True)
    normalized_size = max(0, int(size_bytes or source_path.stat().st_size))
    lock = _segment_writer_lock(root)
    with lock:
        key = _segment_writer_key(root)
        current_segment = Path(_SEGMENT_WRITER_STATE.get(key, "")).resolve() if _SEGMENT_WRITER_STATE.get(key) else None
        if (
            current_segment is None
            or not current_segment.exists()
            or (current_segment.stat().st_size + normalized_size) > max(1, int(OBJECT_SEGMENT_TARGET_BYTES))
        ):
            current_segment = segments_root / f"segment-{os.getpid()}-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}.bin"
            current_segment.touch()
            _SEGMENT_WRITER_STATE[key] = str(current_segment)
        with current_segment.open("ab") as out_fp:
            offset = out_fp.tell()
            with source_path.open("rb") as in_fp:
                shutil.copyfileobj(in_fp, out_fp)
        relpath = _segment_relpath(root, current_segment)
    current_time = created_at or utc_now()
    _write_object_meta_with_retry(
        root,
        object_id=object_id,
        fmt=fmt,
        size_bytes=normalized_size,
        created_at=current_time,
        last_at=current_time,
        storage_backend="segment",
        segment_relpath=relpath,
        segment_offset=offset,
        segment_length=normalized_size,
    )
    return StoredResultArtifact(
        object_id=object_id,
        format=normalize_object_format(fmt, default="bin"),
        size_bytes=normalized_size,
        materialize_as=normalize_materialize_as(materialize_as, default="path"),
        storage_backend="segment",
        segment_relpath=relpath,
        segment_offset=offset,
        segment_length=normalized_size,
    )


def _commit_result_file(
    source_path: Path,
    *,
    object_dir: str,
    fmt: str,
    size_bytes: int,
    materialize_as: str,
    copy_source: bool = False,
) -> StoredResultArtifact:
    root = Path(str(object_dir or "")).resolve()
    root.mkdir(parents=True, exist_ok=True)
    validate_object_size_bytes(size_bytes or source_path.stat().st_size, context="result object")
    digest = _sha256_file(source_path)
    object_id = object_id_from_sha256_hex(digest)
    normalized_format = normalize_object_format(fmt, source_name=source_path.name, default="bin")
    final_path = object_storage_path(root, object_id=object_id, fmt=normalized_format)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    if copy_source:
        shutil.copyfile(str(source_path), str(final_path))
    else:
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
    validate_object_size_bytes(len(blob), context="result object")
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
    return _commit_result_file(
        path,
        object_dir=object_dir,
        fmt=normalize_object_format("", source_name=path.name, default="bin"),
        size_bytes=path.stat().st_size,
        materialize_as="path",
        copy_source=True,
    )


def _store_result_dataframe(frame: Any, *, object_dir: str) -> StoredResultArtifact:
    try:
        Path(object_dir).resolve().mkdir(parents=True, exist_ok=True)
        tmp_path = dataframe_bundle_temp_file(frame, dir=str(Path(object_dir).resolve()))
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
        Path(object_dir).resolve().mkdir(parents=True, exist_ok=True)
        tmp_path = series_bundle_temp_file(series, dir=str(Path(object_dir).resolve()))
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
        Path(object_dir).resolve().mkdir(parents=True, exist_ok=True)
        tmp_path, fmt = ndarray_temp_file(array, format="npy", dir=str(Path(object_dir).resolve()))
        return _commit_result_file(
            tmp_path,
            object_dir=object_dir,
            fmt=fmt,
            size_bytes=tmp_path.stat().st_size,
            materialize_as="ndarray",
        )
    except Exception:
        if "tmp_path" in locals():
            tmp_path.unlink(missing_ok=True)
        raise


def _store_result_json(value: Any, *, object_dir: str) -> StoredResultArtifact:
    blob = json.dumps(serialize_arrow_compatible(value), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    validate_object_size_bytes(len(blob), context="result object")
    if len(blob) <= max(0, int(OBJECT_SEGMENT_MAX_BYTES)):
        return _commit_result_segment(blob, object_dir=object_dir, fmt="json", materialize_as="json")
    Path(object_dir).resolve().mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix="pycloud-result-", suffix=".json", dir=str(Path(object_dir).resolve()))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        tmp_path.write_bytes(blob)
        return _commit_result_file(
            tmp_path,
            object_dir=object_dir,
            fmt="json",
            size_bytes=tmp_path.stat().st_size,
            materialize_as="json",
        )
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _normalize_result_value(
    ret: Any,
    *,
    object_dir: str,
    serialization_mode: str = "",
    use_transport_result: Optional[bool] = None,
) -> Any:
    data_store = _data_store_for_object_dir(object_dir)
    result_policy = get_payload_policy("result")
    result_inline_threshold = int(result_policy.inline_result_threshold_bytes)

    def _should_skip_inline_attempt(value: Any) -> bool:
        try:
            return estimate_payload_inline_size(value) > result_inline_threshold
        except Exception:
            return True

    def _try_inline_result(value: Any) -> tuple[bool, Any]:
        try:
            if bool(use_transport_result):
                result_limit = result_policy.inline_result_hard_limit_bytes
                transport = encode_transport_payload_bytes(
                    value,
                    mode=serialization_mode,
                    context="task result",
                    limit_bytes=result_limit,
                )
                value = transport_payload_to_inline_carrier(
                    transport,
                    payload_mode="result",
                    context="service_result",
                    limit_bytes=result_limit,
                )
            else:
                serialize_inline_result(
                    value,
                    context="task result",
                    mode=serialization_mode,
                )
        except ValueError:
            return False, None
        log_payload_flow("inline_result_ready", context="task result", summary=summarize_payload_flow_value(value))
        return True, value

    if isinstance(ret, Path):
        log_payload_flow("result_ref_store", path_type="path", summary=summarize_payload_flow_value(ret))
        return data_store.store_path(ret)

    try:
        import pandas as pd

        if isinstance(ret, pd.DataFrame):
            if not _should_skip_inline_attempt(ret):
                is_inline, inline_value = _try_inline_result(ret)
                if is_inline:
                    return inline_value
            log_payload_flow("result_ref_store", path_type="dataframe", summary=summarize_payload_flow_value(ret))
            return data_store.store_dataframe(ret)
        if isinstance(ret, pd.Series):
            if not _should_skip_inline_attempt(ret):
                is_inline, inline_value = _try_inline_result(ret)
                if is_inline:
                    return inline_value
            log_payload_flow("result_ref_store", path_type="series", summary=summarize_payload_flow_value(ret))
            return data_store.store_series(ret)
    except ImportError:
        pass

    try:
        import numpy as np

        if isinstance(ret, np.ndarray):
            if not _should_skip_inline_attempt(ret):
                is_inline, inline_value = _try_inline_result(ret)
                if is_inline:
                    return inline_value
            log_payload_flow("result_ref_store", path_type="ndarray", summary=summarize_payload_flow_value(ret))
            return data_store.store_ndarray(ret)
    except ImportError:
        pass

    if isinstance(ret, (dict, list)):
        if _should_skip_inline_attempt(ret):
            log_payload_flow("result_ref_store", path_type="json", summary=summarize_payload_flow_value(ret))
            return data_store.store_json(ret)

    is_inline, inline_value = _try_inline_result(ret)
    if is_inline:
        return inline_value
    raise LargeResultError(
        "task result exceeds inline limit and must be returned as "
        "Path/DataFrame/Series/ndarray for DataRef storage"
    )


def _is_streaming_user_return(ret: Any) -> bool:
    return isinstance(ret, Iterator)


def _normalize_stream_item_value(
    ret: Any,
    *,
    object_dir: str,
    serialization_mode: str = "",
    use_transport_result: Optional[bool] = None,
) -> Any:
    del object_dir
    result_policy = get_payload_policy("result")
    result_limit = result_policy.inline_result_hard_limit_bytes
    try:
        if bool(use_transport_result):
            transport = encode_transport_payload_bytes(
                ret,
                mode=serialization_mode,
                context="service stream item",
                limit_bytes=result_limit,
            )
            inline_value = transport_payload_to_inline_carrier(
                transport,
                payload_mode="result",
                context="service_result",
                limit_bytes=result_limit,
            )
        else:
            inline_value, _struct, _size = serialize_inline_result(
                ret,
                context="service stream item",
                mode=serialization_mode,
                limit_bytes=result_limit,
            )
    except ValueError as exc:
        raise LargeResultError(
            f"service stream item exceeds inline result limit: limit_bytes={result_limit}; "
            "stream does not support DataRef or large result items"
        ) from exc
    log_payload_flow("inline_stream_item_ready", context="service stream item", summary=summarize_payload_flow_value(ret))
    return inline_value


def _normalize_user_return(
    ret: Any,
    *,
    object_dir: str,
    serialization_mode: str = "",
    use_transport_result: Optional[bool] = None,
) -> Tuple[str, Optional[Any], str, str]:
    def _normalize_status(v: Any) -> str:
        s = str(v or "SUCCEEDED").strip().upper()
        if s in ("SUCCESS", "OK"):
            return "SUCCEEDED"
        if s not in ("SUCCEEDED", "FAILED_USER", "FAILED_INFRA", "FAILED_DEPENDENCY"):
            return "SUCCEEDED"
        return s

    if isinstance(ret, tuple) and len(ret) == 4:
        status_text, result, err_type, err_message = ret
        result = (
            _normalize_result_value(
                result,
                object_dir=object_dir,
                serialization_mode=serialization_mode,
                use_transport_result=use_transport_result,
            )
            if result is not None
            else None
        )
        return _normalize_status(status_text), result, str(err_type), str(err_message)

    if isinstance(ret, dict) and "status" in ret:
        status_text = _normalize_status(ret.get("status", "SUCCEEDED"))
        result = ret.get("result")
        err_type = str(ret.get("error_type", ""))
        err_message = str(ret.get("error_message", ""))
        result = (
            _normalize_result_value(
                result,
                object_dir=object_dir,
                serialization_mode=serialization_mode,
                use_transport_result=use_transport_result,
            )
            if result is not None
            else None
        )
        return status_text, result, err_type, err_message

    return (
        "SUCCEEDED",
        _normalize_result_value(
            ret,
            object_dir=object_dir,
            serialization_mode=serialization_mode,
            use_transport_result=use_transport_result,
        ),
        "",
        "",
    )


def _data_store_for_object_dir(
    object_dir: str,
    *,
    node_id: str = "",
    node_instance_id: str = "",
    control_addr: str = "",
) -> DataStore:
    normalized_dir = str(object_dir or "").strip()
    return DataStore(
        object_dir=normalized_dir,
        node_id=str(node_id or ""),
        node_instance_id=str(node_instance_id or ""),
        control_addr=str(control_addr or ""),
        store_path_impl=lambda path: _store_result_path(path, object_dir=normalized_dir),
        store_dataframe_impl=lambda frame: _store_result_dataframe(frame, object_dir=normalized_dir),
        store_series_impl=lambda series: _store_result_series(series, object_dir=normalized_dir),
        store_ndarray_impl=lambda array: _store_result_ndarray(array, object_dir=normalized_dir),
        store_json_impl=lambda value: _store_result_json(value, object_dir=normalized_dir),
        resolve_data_ref_impl=lambda ref: _resolve_single_data_ref(ref, object_dir=normalized_dir),
    )


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
    if artifact is not None:
        fallback_path = Path(artifact.path) if artifact.path else Path(artifact.segment_path)
        touch_object_last_at_throttled(root, object_id=data_ref.object_id, fallback_path=fallback_path)
        materialize_started_at = time.perf_counter()
        resolved = _materialize_object_artifact(
            artifact,
            materialize_as=materialized,
            root=root,
        )
        log_payload_flow(
            "object_ref_resolved",
            materialize_as=materialized,
            summary=summarize_payload_flow_value(resolved),
            worker_dataref_materialize_ms=(time.perf_counter() - materialize_started_at) * 1000.0,
        )
        return resolved
    if get_dataref_resolution() == "remote_fetch":
        targets = _data_ref_remote_targets(data_ref)
        failures: list[tuple[str, BaseException]] = []
        for target in targets:
            log_payload_flow(
                "dataref_remote_fetch_start",
                object_id=normalize_object_id(data_ref.object_id),
                target=target,
                locator_kind=str(data_ref.locator_kind or ""),
            )
            try:
                artifact = _cache_remote_data_ref(data_ref, object_dir=root, target=target)
            except Exception as exc:
                failures.append((target, exc))
                log_payload_flow(
                    "dataref_remote_fetch_failed",
                    object_id=normalize_object_id(data_ref.object_id),
                    target=target,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                continue
            materialize_started_at = time.perf_counter()
            resolved = _materialize_object_artifact(
                artifact,
                materialize_as=materialized,
                root=root,
            )
            log_payload_flow(
                "object_ref_resolved",
                materialize_as=materialized,
                summary=summarize_payload_flow_value(resolved),
                worker_dataref_local_hit=False,
                worker_dataref_materialize_ms=(time.perf_counter() - materialize_started_at) * 1000.0,
            )
            return resolved
        if failures:
            detail = "; ".join(f"{target}: {exc}" for target, exc in failures)
            raise ObjectResolutionError(f"remote fetch failed for {data_ref.object_id}: {detail}") from failures[-1][1]
    raise ObjectResolutionError(f"object not found on node: {data_ref.object_id}")


def _resolve_object_refs_in_payload(payload: Any, *, object_dir: str) -> Any:
    data_store = _data_store_for_object_dir(object_dir)

    def _resolve(value: Any) -> Any:
        data_ref = maybe_data_ref(value)
        if data_ref is not None:
            return data_store.resolve_data_ref(data_ref)
        if isinstance(value, dict):
            from pycloud_parallel.data.ref import data_ref_from_payload, is_data_ref_payload

            if is_data_ref_payload(value):
                return _resolve(data_ref_from_payload(value))
            return {key: _resolve(item) for key, item in value.items()}
        if isinstance(value, list):
            return [_resolve(item) for item in value]
        if isinstance(value, tuple):
            return tuple(_resolve(item) for item in value)
        return value

    return _resolve(payload)


__all__ = [
    "LargeResultError",
    "ObjectResolutionError",
    "_append_bytes_to_segment",
    "_artifact_exists",
    "_commit_result_file",
    "_commit_result_segment",
    "_data_store_for_object_dir",
    "_materialize_object_artifact",
    "_materialize_object_bytes",
    "_normalize_result_value",
    "_normalize_stream_item_value",
    "_normalize_user_return",
    "_is_streaming_user_return",
    "_object_artifact_from_meta",
    "_read_object_artifact_bytes",
    "_replace_file_with_retry",
    "_resolve_object_refs_in_payload",
    "_resolve_single_data_ref",
    "_sha256_file",
    "_store_result_dataframe",
    "_store_result_ndarray",
    "_store_result_path",
    "_store_result_json",
    "_store_result_series",
    "_write_object_meta_with_retry",
]
