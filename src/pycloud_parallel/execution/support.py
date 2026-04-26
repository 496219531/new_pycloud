from __future__ import annotations

"""Shared helpers for authoritative V1 execution modules."""

import base64
import contextlib
import errno
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import threading
import time
from dataclasses import replace
from datetime import date, datetime, time as dt_time, timedelta
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, TYPE_CHECKING, Union
from urllib.error import URLError
from urllib.parse import urlparse

from google.protobuf import timestamp_pb2

from pycloud_parallel.controlplane.artifact import (
    _default_entry_module_for_module,
    _packaging_kwargs,
    _resolve_package_format,
)
from pycloud_parallel.controlplane.config import (
    FILE_HASH_CHUNK_SIZE_BYTES,
    GRPC_MAX_SEND_MESSAGE_LENGTH_BYTES,
    INLINE_PAYLOAD_HARD_LIMIT_BYTES,
    INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    JOB_PAYLOAD_MAX_BYTES,
    JOB_STAGED_REF_TTL_SEC,
    JOB_STAGING_REPLICA_COUNT,
    OBJECT_CHUNK_SIZE_BYTES,
)
from pycloud_parallel.controlplane.data_ref import DataRef, maybe_data_ref
from pycloud_parallel.controlplane.effective_policy import (
    EffectivePolicy,
    payload_policy_from_effective_policy,
    should_use_transport_payload_bytes,
)
from pycloud_parallel.controlplane.netutil import detect_local_ip
from pycloud_parallel.data.ref import normalize_materialize_as, normalize_object_format
from pycloud_parallel.controlplane.payload_transport import (
    estimate_payload_inline_size,
    prepare_outbound_payload,
    prepare_outbound_value,
)
from pycloud_parallel.controlplane.runtime_spec import matches_python_runtime, normalize_python_runtime_spec
from pycloud_parallel.controlplane.serialization import (
    dataframe_bundle_parquet_frame,
    encode_transport_payload_bytes,
    log_payload_flow,
    serialize_arrow_compatible,
    serialize_dataframe_bundle,
    serialize_series_bundle,
    serialize_by_mode,
    serialize_inline_payload,
    summarize_payload_flow_value,
)
from pycloud_parallel.controlplane.serialization_mode import resolve_effective_serialization_mode
from pycloud_parallel.runtime.compat import runtime_mismatch_message_for_nodes

if TYPE_CHECKING:
    from pycloud_parallel.controlplane.infocenter_client import InfoCenterNode


_SERVICE_SESSION_SCHEMA_VERSION = 2
_JOB_CLIENT_SESSION_SCHEMA_VERSION = 1
_SERVICE_SESSION_LOCK_GUARD = threading.Lock()
_SERVICE_SESSION_LOCKED_PATHS: set[str] = set()
_JOB_UPDATE_GLOBALS_AUTO = object()
_DEFAULT_EXPORT_DECORATOR = "pycloud_export"


class _RetryableReadyError(RuntimeError):
    """Signals a transient not-ready state that should be retried briefly."""


def _utc_now():
    from datetime import datetime, timezone

    return datetime.now(timezone.utc)


def _timestamp_to_datetime(value: timestamp_pb2.Timestamp | None):
    from datetime import timezone

    if value is None:
        return _utc_now()
    try:
        dt = value.ToDatetime()
    except Exception:
        return _utc_now()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _emit_owner_notice(message: str) -> None:
    text = str(message or "").strip()
    if not text:
        return
    print(f"[Service] {text}", file=sys.stderr, flush=True)


def _summarize_discovered_nodes(nodes: Sequence["InfoCenterNode"], *, limit: int = 8) -> str:
    rows: List[str] = []
    for node in list(nodes)[: max(1, int(limit))]:
        rows.append(
            f"{node.node_id}(healthy={'yes' if node.healthy else 'no'},"
            f"schedulable={'yes' if node.schedulable else 'no'},"
            f"accept_deploy={'yes' if getattr(node, 'accept_service_deploy', True) else 'no'},"
            f"drain={'yes' if node.drain else 'no'},"
            f"svc_avail={int(node.service_worker_available)},"
            f"py={node.python_version or '-'})"
        )
    if len(nodes) > max(1, int(limit)):
        rows.append(f"...+{len(nodes) - max(1, int(limit))} more")
    return ", ".join(rows) if rows else "(none)"


def _get_local_ip() -> str:
    detected = detect_local_ip()
    return detected or "localhost"


def _filter_nodes_by_runtime(
    nodes: Sequence["InfoCenterNode"],
    *,
    runtime: str,
) -> List["InfoCenterNode"]:
    normalized_runtime = normalize_python_runtime_spec(runtime)
    if not normalized_runtime:
        return list(nodes)
    return [
        node
        for node in nodes
        if not str(node.python_version or "").strip()
        or matches_python_runtime(node.python_version, normalized_runtime)
    ]


def _is_transient_infocenter_error(exc: Exception) -> bool:
    candidate: object = exc
    if isinstance(candidate, URLError):
        candidate = candidate.reason
    if isinstance(candidate, TimeoutError):
        return True
    if isinstance(candidate, OSError):
        return getattr(candidate, "errno", None) in {
            errno.ECONNREFUSED,
            errno.ECONNRESET,
            errno.ETIMEDOUT,
            errno.EHOSTUNREACH,
            errno.ENETUNREACH,
        }
    if isinstance(candidate, str):
        lowered = candidate.lower()
        return (
            "connection refused" in lowered
            or "connection reset" in lowered
            or "timed out" in lowered
            or "temporarily unavailable" in lowered
        )
    return False


def _retry_infocenter_request(
    fn: Callable[[], Any],
    *,
    timeout_sec: float,
    target: str,
    action: str,
    retry_interval_sec: float = 0.25,
) -> Any:
    deadline = time.monotonic() + max(0.1, float(timeout_sec))
    last_exc: Optional[Exception] = None
    while True:
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"InfoCenter {target} not ready for {action} after {float(timeout_sec):.1f}s: {last_exc}"
            )
        try:
            return fn()
        except Exception as exc:
            if not isinstance(exc, _RetryableReadyError) and not _is_transient_infocenter_error(exc):
                raise
            last_exc = exc
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"InfoCenter {target} not ready for {action} after {float(timeout_sec):.1f}s: {exc}"
                ) from exc
            time.sleep(min(retry_interval_sec, max(0.05, deadline - time.monotonic())))


def _resolve_public_target_arg(
    *,
    target: str = "",
    kwargs: Optional[Dict[str, Any]] = None,
    action_name: str = "",
) -> str:
    remaining_kwargs = kwargs if kwargs is not None else {}
    normalized_target = str(target or "").strip()
    compatibility_target = str(remaining_kwargs.pop("infocenter_target", "") or "").strip()
    if normalized_target and compatibility_target and normalized_target != compatibility_target:
        label = str(action_name or "public API").strip()
        raise ValueError(
            f"{label} received both target={normalized_target!r} and "
            f"infocenter_target={compatibility_target!r}; please pass only target"
        )
    effective_target = normalized_target or compatibility_target
    if effective_target:
        return effective_target
    label = str(action_name or "public API").strip()
    raise TypeError(f"{label} requires target=...")


def _artifact_code_version(
    blob: bytes,
    *,
    runtime: str,
    entry_module: str,
    entry_callable: str,
    package_format: str,
    export_mode: str,
    export_methods: Optional[Sequence[str]] = None,
    export_decorator: str = _DEFAULT_EXPORT_DECORATOR,
    dependency_policy_mode: str = "",
    dependency_allowlist: Optional[Sequence[str]] = None,
) -> str:
    from pycloud_parallel.controlplane.code_version import _code_version_from_digest

    return _code_version_from_digest(
        hashlib.sha256(blob).hexdigest(),
        runtime=runtime,
        entry_module=entry_module,
        entry_callable=entry_callable,
        package_format=package_format,
        export_mode=export_mode,
        export_methods=list(export_methods or ()),
        export_decorator=export_decorator,
        dependency_policy_mode=dependency_policy_mode,
        dependency_allowlist=list(dependency_allowlist or ()),
    )


def _default_service_session_cache_dir() -> Path:
    custom = str(os.environ.get("PYCLOUD_SERVICE_SESSION_DIR", "")).strip()
    if custom:
        return Path(custom).expanduser()
    return Path.home() / ".pycloud_parallel" / "service_sessions"


def _sanitize_session_cache_part(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    return text.strip("._") or "default"


def _ensure_private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path, 0o700)
    except OSError:
        pass


def _write_private_json(path: Path, payload: Dict[str, object]) -> None:
    _ensure_private_dir(path.parent)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.stem}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=True, indent=2, sort_keys=True)
            fp.write("\n")
            fp.flush()
            os.fsync(fp.fileno())
        try:
            os.chmod(tmp_name, 0o600)
        except OSError:
            pass
        os.replace(tmp_name, path)
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
    finally:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass


def _extract_result_ref(response: Dict[str, object]):
    direct = maybe_data_ref(response.get("data"))
    if direct is not None:
        return direct
    value = response.get("data")
    if isinstance(value, dict):
        return maybe_data_ref(value.get("data"))
    return None


def _resolve_high_level_service_data(group: object, *, node_id: str, response: Dict[str, object]):
    if not isinstance(response, dict) or "data" not in response:
        return response
    result_ref = _extract_result_ref(response)
    if result_ref is None:
        return response.get("data", response)

    sessions = getattr(group, "sessions", None)
    if isinstance(sessions, dict) and node_id in sessions:
        return sessions[node_id].fetch_result_data(response)

    fetcher = getattr(group, "fetch_result_data", None)
    if callable(fetcher):
        return fetcher(response)

    return response.get("data", response)


def _resolve_high_level_service_results(
    group: object,
    *,
    results: Sequence[Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]],
) -> List[Tuple[Optional[str], Optional[object], Optional[Exception]]]:
    resolved: List[Tuple[Optional[str], Optional[object], Optional[Exception]]] = []
    for node_id, response, error in results:
        if error is not None or node_id is None or response is None:
            resolved.append((node_id, response, error))
            continue
        resolved.append(
            (
                node_id,
                _resolve_high_level_service_data(group, node_id=node_id, response=response),
                error,
            )
        )
    return resolved


def _serialize_data_for_object_ref(
    data: Any,
    *,
    format: str = "",
    materialize_as: str = "auto",
    serialization_mode: str = "",
    default_serialization_mode: str = "",
) -> Tuple[str, str, bytes]:
    log_payload_flow(
        "object_ref_upload_prepare",
        format=(format or "auto"),
        materialize_as=materialize_as,
        summary=summarize_payload_flow_value(data),
    )
    if maybe_data_ref(data) is not None:
        raise ValueError("data is already uploaded; no need to serialize again")

    normalized_mode = resolve_effective_serialization_mode(
        request_mode=serialization_mode,
        default_mode=default_serialization_mode,
        context="object_upload",
    )
    if normalized_mode in {"structured_v1", "pickle_stable_v1"}:
        if isinstance(data, os.PathLike):
            path = Path(data).expanduser()
            if not path.exists() or not path.is_file():
                raise FileNotFoundError(f"path not found or not a file: {path}")
            log_payload_flow(
                "object_ref_upload",
                path_type="file",
                format=normalize_object_format(format, source_name=path.name),
            )
            return "path", normalize_object_format(format, source_name=path.name), path.read_bytes()
        if isinstance(data, str):
            path = Path(data).expanduser()
            if path.exists() and path.is_file():
                log_payload_flow(
                    "object_ref_upload",
                    path_type="string-file",
                    format=normalize_object_format(format, source_name=path.name),
                )
                return "path", normalize_object_format(format, source_name=path.name), path.read_bytes()
        blob = serialize_by_mode(data, mode=normalized_mode)
        if not isinstance(blob, (bytes, bytearray, memoryview)):
            blob = json.dumps(blob, ensure_ascii=False).encode("utf-8")
        materialize_kind = "auto"
        try:
            import pandas as pd
            import numpy as np

            if isinstance(data, pd.DataFrame):
                materialize_kind = "dataframe"
            elif isinstance(data, pd.Series):
                materialize_kind = "series"
            elif isinstance(data, np.ndarray):
                materialize_kind = "ndarray"
            elif isinstance(data, (dict, list, tuple)):
                materialize_kind = "json"
            elif isinstance(data, (bytes, bytearray, memoryview)):
                materialize_kind = "bytes"
        except ImportError:
            if isinstance(data, (dict, list, tuple)):
                materialize_kind = "json"
            elif isinstance(data, (bytes, bytearray, memoryview)):
                materialize_kind = "bytes"
        log_payload_flow(
            "object_ref_upload",
            path_type=f"serialization-{normalized_mode}",
            format=normalized_mode,
            summary=summarize_payload_flow_value(data),
        )
        return materialize_kind, normalized_mode, bytes(blob)

    if isinstance(data, os.PathLike):
        path = Path(data).expanduser()
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"path not found or not a file: {path}")
        log_payload_flow(
            "object_ref_upload",
            path_type="file",
            format=normalize_object_format(format, source_name=path.name),
        )
        return "path", normalize_object_format(format, source_name=path.name), path.read_bytes()

    if isinstance(data, str):
        path = Path(data).expanduser()
        if path.exists() and path.is_file():
            log_payload_flow(
                "object_ref_upload",
                path_type="string-file",
                format=normalize_object_format(format, source_name=path.name),
            )
            return "path", normalize_object_format(format, source_name=path.name), path.read_bytes()
        raise TypeError("plain string is not supported by put_data; pass it inline in payload or use an existing file path")

    try:
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            import io
            import zipfile

            parquet_buf = io.BytesIO()
            dataframe_bundle_parquet_frame(data).to_parquet(parquet_buf, index=False)
            meta = serialize_dataframe_bundle(data)
            bundle_buf = io.BytesIO()
            with zipfile.ZipFile(bundle_buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("data.parquet", parquet_buf.getvalue())
                zf.writestr("meta.json", json.dumps(meta, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
            log_payload_flow("object_ref_upload", path_type="dataframe", format="dfbundle", summary=summarize_payload_flow_value(data))
            return "dataframe", "dfbundle", bundle_buf.getvalue()
        if isinstance(data, pd.Series):
            import io
            import zipfile

            parquet_buf = io.BytesIO()
            data.to_frame("__pycloud_series_value__").to_parquet(parquet_buf, index=False)
            meta = serialize_series_bundle(data)
            bundle_buf = io.BytesIO()
            with zipfile.ZipFile(bundle_buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("data.parquet", parquet_buf.getvalue())
                zf.writestr("meta.json", json.dumps(meta, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
            log_payload_flow("object_ref_upload", path_type="series", format="seriesbundle", summary=summarize_payload_flow_value(data))
            return "series", "seriesbundle", bundle_buf.getvalue()
    except ImportError:
        pass

    try:
        import numpy as np

        if isinstance(data, np.ndarray):
            import io

            buf = io.BytesIO()
            np.save(buf, data, allow_pickle=False)
            log_payload_flow("object_ref_upload", path_type="ndarray", format=(format or "npy"), summary=summarize_payload_flow_value(data))
            return "ndarray", normalize_object_format(format or "npy", default="npy"), buf.getvalue()
    except ImportError:
        pass

    if isinstance(data, (dict, list)):
        log_payload_flow("object_ref_upload", path_type="json", format=(format or "json"), summary=summarize_payload_flow_value(data))
        serialized = serialize_arrow_compatible(data)
        return "json", normalize_object_format(format or "json", default="json"), json.dumps(serialized, ensure_ascii=False).encode("utf-8")

    if isinstance(data, (bytes, bytearray, memoryview)):
        log_payload_flow("object_ref_upload", path_type="bytes", format=(format or "bin"), summary=summarize_payload_flow_value(data))
        return "bytes", normalize_object_format(format or "bin", default="bin"), bytes(data)

    raise TypeError(
        f"put_data does not support type {type(data).__name__}; "
        "supported inputs are file paths, pandas.DataFrame, numpy.ndarray, dict/list, bytes, and DataRef-compatible uploads"
    )


def _put_data_via_clients(
    clients: Sequence[Any],
    data: Any,
    *,
    format: str = "",
    chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    serialization_mode: str = "",
    default_serialization_mode: str = "",
) -> DataRef:
    existing = maybe_data_ref(data)
    if existing is not None:
        return existing
    materialize_as, effective_format, blob = _serialize_data_for_object_ref(
        data,
        format=format,
        serialization_mode=serialization_mode,
        default_serialization_mode=default_serialization_mode,
    )
    refs = [
        client.upload_object_from_bytes(
            blob=blob,
            format=effective_format,
            chunk_size=chunk_size,
        )
        for client in clients
    ]
    if not refs:
        raise RuntimeError("no node clients available for object upload")
    object_ids = {ref.object_id for ref in refs}
    formats = {ref.format for ref in refs}
    if len(object_ids) != 1 or len(formats) != 1:
        raise RuntimeError(f"inconsistent object upload across nodes: {refs}")
    first = refs[0]
    return DataRef(
        ref_id=first.object_id,
        storage_id=first.object_id,
        logical_type="",
        format=first.format,
        size_bytes=first.size_bytes,
        materialize_as=normalize_materialize_as(materialize_as, default="path"),
        locator_kind="node_local",
        locator_token="",
        consume_on_read=bool(getattr(first, "consume_on_read", False)),
        node_id=str(getattr(first, "node_id", "") or ""),
        node_instance_id=str(getattr(first, "node_instance_id", "") or ""),
        control_addr=str(getattr(first, "control_addr", "") or ""),
    )


def _estimate_managed_global_inline_size(value: Any) -> int:
    return estimate_payload_inline_size(value)


def _policy_with_soft_limit(policy, object_threshold_bytes: int):
    if int(object_threshold_bytes) == int(policy.inline_payload_soft_limit_bytes):
        return policy
    return replace(
        policy,
        limits=replace(
            policy.limits,
            inline_payload_soft_limit_bytes=max(1, int(object_threshold_bytes)),
        ),
    )


def _payload_policy_for_mode(
    mode: str,
    *,
    effective_policy: Optional[EffectivePolicy] = None,
    object_threshold_bytes: int = 0,
):
    policy = payload_policy_from_effective_policy(mode, effective_policy)
    if int(object_threshold_bytes or 0) > 0:
        threshold = min(
            max(1, int(object_threshold_bytes)),
            max(1, int(policy.inline_payload_soft_limit_bytes)),
        )
        policy = _policy_with_soft_limit(policy, threshold)
    return policy


def _prepare_payload_for_policy(
    clients: Sequence[Any],
    payload: Optional[Dict[str, object]],
    *,
    policy,
    managed_global_policy=None,
    default_serialization_mode: str = "",
) -> Dict[str, object]:
    put_kwargs = {}
    if str(default_serialization_mode or "").strip() and str(default_serialization_mode).strip().lower() != "legacy_v1":
        put_kwargs["default_serialization_mode"] = default_serialization_mode
    prepare_kwargs = {
        "put_data": lambda value, *, format="": _put_data_via_clients(clients, value, format=format, **put_kwargs),
        "estimate_inline_size": _estimate_managed_global_inline_size,
        "policy": policy,
    }
    if managed_global_policy is not None:
        prepare_kwargs["managed_global_policy"] = managed_global_policy
    return prepare_outbound_payload(
        payload,
        **prepare_kwargs,
    )


def _prepare_value_for_policy(
    clients: Sequence[Any],
    value: Any,
    *,
    policy,
    preserve_container: bool = False,
    default_serialization_mode: str = "",
) -> Any:
    put_kwargs = {}
    if str(default_serialization_mode or "").strip() and str(default_serialization_mode).strip().lower() != "legacy_v1":
        put_kwargs["default_serialization_mode"] = default_serialization_mode
    return prepare_outbound_value(
        value,
        put_data=lambda data, *, format="": _put_data_via_clients(clients, data, format=format, **put_kwargs),
        estimate_inline_size=_estimate_managed_global_inline_size,
        policy=policy,
        preserve_container=preserve_container,
    )


def _prepare_payload_value_for_upload(
    clients: Sequence[Any],
    value: Any,
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    preserve_container: bool = False,
    recurse_containers: bool = False,
    upload_pathlike: bool = False,
    upload_string_file: bool = False,
    upload_bytes: bool = False,
    consume_on_read: bool = False,
    serialization_mode: str = "",
    effective_policy: Optional[EffectivePolicy] = None,
) -> Any:
    base_policy = _payload_policy_for_mode(
        "managed_globals",
        effective_policy=effective_policy,
        object_threshold_bytes=object_threshold_bytes,
    )
    policy = replace(
        base_policy,
        objectify_pathlikes=bool(upload_pathlike),
        objectify_strings_as_files=bool(upload_string_file),
        objectify_bytes=bool(upload_bytes),
        recurse_containers=bool(recurse_containers),
        consume_on_read=bool(consume_on_read),
    )
    return _prepare_value_for_policy(
        clients,
        value,
        policy=policy,
        preserve_container=preserve_container,
        default_serialization_mode=serialization_mode,
    )


def _prepare_managed_global_value_for_upload(
    clients: Sequence[Any],
    value: Any,
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    serialization_mode: str = "",
    effective_policy: Optional[EffectivePolicy] = None,
) -> Any:
    policy = _payload_policy_for_mode(
        "managed_globals",
        effective_policy=effective_policy,
        object_threshold_bytes=object_threshold_bytes,
    )
    effective_threshold_bytes = max(1, int(policy.inline_payload_soft_limit_bytes))
    inline_size = _estimate_managed_global_inline_size(value)
    if inline_size <= effective_threshold_bytes:
        log_payload_flow(
            "managed_global_inline",
            threshold_bytes=effective_threshold_bytes,
            size_bytes=inline_size,
            summary=summarize_payload_flow_value(value),
        )
        return value

    try:
        prepared = _prepare_payload_value_for_upload(
            clients,
            value,
            object_threshold_bytes=effective_threshold_bytes,
            upload_pathlike=True,
            upload_string_file=True,
            upload_bytes=True,
            consume_on_read=False,
            serialization_mode=serialization_mode,
            effective_policy=effective_policy,
        )
        log_payload_flow(
            "managed_global_objectref_ready",
            threshold_bytes=effective_threshold_bytes,
            size_bytes=inline_size,
            summary=summarize_payload_flow_value(prepared),
        )
        return prepared
    except Exception as exc:
        log_payload_flow(
            "managed_global_objectref_failed",
            threshold_bytes=effective_threshold_bytes,
            size_bytes=inline_size,
            summary=summarize_payload_flow_value(value),
            error=repr(exc),
        )
        raise ValueError(
            "managed global exceeds inline threshold and large-object upload failed: "
            f"size_bytes={inline_size} threshold_bytes={effective_threshold_bytes}; "
            f"error={exc}"
        ) from exc


def _prepare_managed_globals_values_for_upload(
    clients: Sequence[Any],
    values: Dict[str, object],
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    serialization_mode: str = "",
    effective_policy: Optional[EffectivePolicy] = None,
) -> Dict[str, object]:
    return {
        str(name): _prepare_managed_global_value_for_upload(
            clients,
            value,
            object_threshold_bytes=object_threshold_bytes,
            serialization_mode=serialization_mode,
            effective_policy=effective_policy,
        )
        for name, value in (values or {}).items()
    }


def _managed_globals_effective_inline_limit(
    *,
    effective_policy: Optional[EffectivePolicy] = None,
) -> int:
    policy = _payload_policy_for_mode("managed_globals", effective_policy=effective_policy)
    return max(
        1,
        min(
            int(policy.inline_payload_hard_limit_bytes),
            int(GRPC_MAX_SEND_MESSAGE_LENGTH_BYTES),
        ),
    )


def _encoded_managed_globals_size(
    values: Dict[str, object],
    *,
    serialization_mode: str = "",
    effective_policy: Optional[EffectivePolicy] = None,
    context: str = "taskpool_session",
) -> int:
    effective_mode = resolve_effective_serialization_mode(
        request_mode=serialization_mode,
        context=context,
    )
    if should_use_transport_payload_bytes(mode=effective_mode, effective_policy=effective_policy):
        transport = encode_transport_payload_bytes(
            values,
            mode=effective_mode,
            context=context,
        )
        return len(bytes(transport.payload or b""))
    _serialized, _struct, size_bytes = serialize_inline_payload(
        values,
        context=context,
        limit_bytes=sys.maxsize,
        mode=effective_mode,
    )
    return int(size_bytes)


def _stage_managed_global_value_for_upload(
    clients: Sequence[Any],
    value: Any,
    *,
    serialization_mode: str = "",
) -> Any:
    if maybe_data_ref(value) is not None:
        return value
    return _put_data_via_clients(
        clients,
        value,
        default_serialization_mode=serialization_mode,
    )


def _prepare_managed_globals_batches_for_upload(
    clients: Sequence[Any],
    values: Dict[str, object],
    *,
    serialization_mode: str = "",
    effective_policy: Optional[EffectivePolicy] = None,
    context: str = "taskpool_session",
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    limit_bytes = _managed_globals_effective_inline_limit(effective_policy=effective_policy)
    batches: List[Dict[str, object]] = []
    batch_bytes: List[int] = []
    inline_keys: List[str] = []
    staged_keys: List[str] = []
    current: Dict[str, object] = {}
    current_size = 0
    if not values:
        empty_size = _encoded_managed_globals_size(
            {},
            serialization_mode=serialization_mode,
            effective_policy=effective_policy,
            context=context,
        )
        return [{}], {
            "globals_batch_count": 1,
            "batch_keys": [[]],
            "batch_bytes": [empty_size],
            "staged_keys": [],
            "inline_keys": [],
            "effective_grpc_limit_bytes": limit_bytes,
        }

    for raw_name, raw_value in (values or {}).items():
        name = str(raw_name)
        prepared_value = raw_value
        staged = False
        try:
            single_size = _encoded_managed_globals_size(
                {name: raw_value},
                serialization_mode=serialization_mode,
                effective_policy=effective_policy,
                context=context,
            )
        except Exception:
            single_size = limit_bytes + 1

        if single_size > limit_bytes:
            prepared_value = _stage_managed_global_value_for_upload(
                clients,
                raw_value,
                serialization_mode=serialization_mode,
            )
            staged = True
            single_size = _encoded_managed_globals_size(
                {name: prepared_value},
                serialization_mode=serialization_mode,
                effective_policy=effective_policy,
                context=context,
            )
            if single_size > limit_bytes:
                raise ValueError(
                    "managed global remains above effective gRPC limit after staging: "
                    f"key={name!r} size_bytes={single_size} limit_bytes={limit_bytes}"
                )

        candidate = dict(current)
        candidate[name] = prepared_value
        candidate_size = _encoded_managed_globals_size(
            candidate,
            serialization_mode=serialization_mode,
            effective_policy=effective_policy,
            context=context,
        )
        if current and candidate_size > limit_bytes:
            batches.append(current)
            batch_bytes.append(current_size)
            current = {name: prepared_value}
            current_size = single_size
        else:
            current = candidate
            current_size = candidate_size

        if staged:
            staged_keys.append(name)
        else:
            inline_keys.append(name)

    if current:
        batches.append(current)
        batch_bytes.append(current_size)

    return batches, {
        "globals_batch_count": len(batches),
        "batch_keys": [sorted(str(key) for key in batch.keys()) for batch in batches],
        "batch_bytes": batch_bytes,
        "staged_keys": sorted(staged_keys),
        "inline_keys": sorted(inline_keys),
        "effective_grpc_limit_bytes": limit_bytes,
    }


def _prepare_task_payload_for_submit(
    client: Any,
    payload: Dict[str, object],
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    serialization_mode: str = "",
    effective_policy: Optional[EffectivePolicy] = None,
) -> Any:
    policy = _payload_policy_for_mode(
        "task_submit",
        effective_policy=effective_policy,
        object_threshold_bytes=object_threshold_bytes,
    )
    return _prepare_payload_for_policy(
        [client],
        payload,
        policy=policy,
        managed_global_policy=(
            _payload_policy_for_mode("managed_globals", effective_policy=effective_policy)
            if effective_policy is not None
            else None
        ),
        default_serialization_mode=serialization_mode,
    )


def _prepare_http_payload_for_call(
    clients: Sequence[Any],
    payload: Optional[Dict[str, object]],
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    serialization_mode: str = "",
    effective_policy: Optional[EffectivePolicy] = None,
) -> Dict[str, object]:
    policy = _payload_policy_for_mode(
        "http_call",
        effective_policy=effective_policy,
        object_threshold_bytes=object_threshold_bytes,
    )
    return _prepare_payload_for_policy(
        clients,
        payload,
        policy=policy,
        managed_global_policy=(
            _payload_policy_for_mode("managed_globals", effective_policy=effective_policy)
            if effective_policy is not None
            else None
        ),
        default_serialization_mode=serialization_mode,
    )


def _default_job_update_globals_for_blob(blob: bytes, *, package_format: str) -> Optional[object]:
    if _resolve_package_format(package_format, default="py") != "py":
        return None
    try:
        source = blob.decode("utf-8")
    except Exception:
        return None
    if re.search(r"(?m)^def\s+update_globals\s*\(", source):
        return "update_globals"
    return None


def _default_job_task_generator_for_blob(blob: bytes, *, package_format: str) -> str:
    if _resolve_package_format(package_format, default="py") != "py":
        return "task_generator"
    try:
        source = blob.decode("utf-8")
    except Exception:
        return "task_generator"
    if re.search(r"(?m)^def\s+task_generator\s*\(", source):
        return "task_generator"
    raise ValueError("task_generator callable not found in job blob")


def _default_job_handle_result_for_blob(blob: bytes, *, package_format: str) -> Optional[str]:
    if _resolve_package_format(package_format, default="py") != "py":
        return None
    try:
        source = blob.decode("utf-8")
    except Exception:
        return None
    for name in ("handle_result", "handle_data"):
        if re.search(rf"(?m)^def\s+{re.escape(name)}\s*\(", source):
            return name
    return None


def _default_job_finalize_for_blob(blob: bytes, *, package_format: str) -> Optional[str]:
    if _resolve_package_format(package_format, default="py") != "py":
        return None
    try:
        source = blob.decode("utf-8")
    except Exception:
        return None
    if re.search(r"(?m)^def\s+finalize\s*\(", source):
        return "finalize"
    return None


def _default_job_task_generator_for_module(module: Any) -> str:
    candidate = getattr(module, "task_generator", None)
    if callable(candidate):
        return "task_generator"
    raise ValueError("task_generator callable not found in run module")


def _default_job_handle_result_for_module(module: Any) -> Optional[str]:
    for name in ("handle_result", "handle_data"):
        candidate = getattr(module, name, None)
        if callable(candidate):
            return name
    return None


def _default_job_finalize_for_module(module: Any) -> Optional[str]:
    candidate = getattr(module, "finalize", None)
    if callable(candidate):
        return "finalize"
    return None


def _normalize_job_update_globals_arg(update_globals: Any, *, auto_default: Any = None) -> Optional[object]:
    value = auto_default if update_globals is _JOB_UPDATE_GLOBALS_AUTO else update_globals
    if value is None:
        return None
    if isinstance(value, str):
        normalized = str(value or "").strip()
        return normalized or None
    if callable(value):
        name = str(getattr(value, "__name__", "") or "").strip()
        if not name:
            raise ValueError("update_globals callable must have a __name__")
        return name
    if isinstance(value, dict):
        return dict(value)
    raise TypeError("update_globals must be None, str, callable, or dict")


def _select_job_staging_clients(
    *,
    target: str,
    runtime: str,
    timeout_sec: float,
    replica_count: int,
) -> Tuple[List[Any], List[Dict[str, object]]]:
    from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient

    desired = max(1, int(replica_count or JOB_STAGING_REPLICA_COUNT))
    with InfoCenterClient(target, timeout_sec=timeout_sec) as infocenter:
        nodes = list(
            infocenter.select_task_nodes(
                healthy_only=True,
                node_count=max(desired, 1),
                limit=max(16, desired * 4),
                require_credit=False,
                preferred_runtime_key="",
                runtime=str(runtime or "py3"),
            )
        )
    selected = []
    seen: set[str] = set()
    for node in nodes:
        control_addr = str(getattr(node, "control_addr", "") or "").strip()
        node_instance_id = str(getattr(node, "node_instance_id", "") or "").strip()
        if not control_addr or not node_instance_id or node_instance_id in seen:
            continue
        seen.add(node_instance_id)
        selected.append(node)
        if len(selected) >= desired:
            break
    if not selected:
        raise RuntimeError("no healthy task nodes with control_addr available for job payload staging")

    clients = []
    from pycloud_parallel.controlplane.node_control_client import NodeControlClient

    for node in selected:
        clients.append(NodeControlClient(str(node.control_addr), timeout_sec=timeout_sec))
    replicas = [
        {
            "node_id": str(getattr(node, "node_id", "") or ""),
            "node_instance_id": str(getattr(node, "node_instance_id", "") or ""),
            "control_addr": str(getattr(node, "control_addr", "") or ""),
        }
        for node in selected
    ]
    return clients, replicas


def _upload_text_data_via_clients(clients: Sequence[Any], text: str) -> DataRef:
    blob = str(text or "").encode("utf-8")
    refs = [
        client.upload_object_from_bytes(
            blob=blob,
            format="txt",
            chunk_size=OBJECT_CHUNK_SIZE_BYTES,
        )
        for client in clients
    ]
    if not refs:
        raise RuntimeError("no node clients available for job staging upload")
    object_ids = {ref.object_id for ref in refs}
    if len(object_ids) != 1:
        raise RuntimeError(f"inconsistent text object upload across nodes: {refs}")
    first = refs[0]
    return DataRef(
        ref_id=first.object_id,
        storage_id=first.object_id,
        logical_type="text",
        format=first.format,
        size_bytes=first.size_bytes,
        materialize_as="text",
        locator_kind="node_local",
        locator_token="",
        consume_on_read=False,
        node_id=str(getattr(first, "node_id", "") or ""),
        node_instance_id=str(getattr(first, "node_instance_id", "") or ""),
        control_addr=str(getattr(first, "control_addr", "") or ""),
    )


def _stage_job_value_as_data_ref(
    *,
    target: str,
    value: Any,
    runtime: str,
    timeout_sec: float,
    replica_count: int,
    ttl_sec: int,
    serialization_mode: str = "",
) -> DataRef:
    data_ref = maybe_data_ref(value)
    if data_ref is not None:
        return data_ref
    clients, replicas = _select_job_staging_clients(
        target=target,
        runtime=runtime,
        timeout_sec=timeout_sec,
        replica_count=replica_count,
    )
    try:
        if isinstance(value, str) and not Path(value).expanduser().exists():
            object_ref = _upload_text_data_via_clients(clients, value)
            staged_ref = DataRef(
                ref_id=object_ref.object_id,
                storage_id=object_ref.object_id,
                logical_type="text",
                format="txt",
                size_bytes=object_ref.size_bytes,
                materialize_as="text",
                locator_kind="controlplane",
                locator_token=target,
                consume_on_read=False,
            )
        else:
            upload_value = list(value) if isinstance(value, tuple) else value
            object_ref = _put_data_via_clients(
                clients,
                upload_value,
                default_serialization_mode=serialization_mode,
            )
            staged_ref = DataRef(
                ref_id=object_ref.object_id,
                storage_id=object_ref.object_id,
                logical_type="",
                format=object_ref.format,
                size_bytes=object_ref.size_bytes,
                materialize_as=object_ref.materialize_as if object_ref.materialize_as != "path" else "auto",
                locator_kind="controlplane",
                locator_token=target,
                consume_on_read=False,
            )
        from pycloud_parallel.controlplane.data_registry import DataRegistryClient

        DataRegistryClient(target, timeout_sec=timeout_sec).register(
            staged_ref,
            ttl_sec=max(1, int(ttl_sec or JOB_STAGED_REF_TTL_SEC)),
            replicas=replicas,
            locator_kind="controlplane",
            locator_token=target,
        )
        return staged_ref
    finally:
        for client in clients:
            with contextlib.suppress(Exception):
                client.close()


def _stage_job_submit_value(
    *,
    target: str,
    value: Any,
    runtime: str,
    timeout_sec: float,
    replica_count: int,
    ttl_sec: int,
    serialization_mode: str = "",
) -> Any:
    def _path_is_file(path: Path) -> bool:
        try:
            return path.exists() and path.is_file()
        except OSError:
            return False

    existing_ref = maybe_data_ref(value)
    if existing_ref is not None:
        locator_kind = str(existing_ref.locator_kind or "").strip().lower()
        locator_token = str(existing_ref.locator_token or "").strip()
        control_addr = str(existing_ref.control_addr or "").strip()
        if locator_kind == "node_local" and not locator_token and not control_addr:
            raise ValueError(
                "job_payload/update_globals refs must use controlplane staging or include a node control locator"
            )
        return existing_ref
    if value is None or isinstance(value, (bool, int, float, datetime, date, dt_time, timedelta)):
        return value
    if isinstance(value, str):
        path = Path(value).expanduser()
        if _path_is_file(path):
            return _stage_job_value_as_data_ref(
                target=target,
                value=path,
                runtime=runtime,
                timeout_sec=timeout_sec,
                replica_count=replica_count,
                ttl_sec=ttl_sec,
                serialization_mode=serialization_mode,
            )
        if estimate_payload_inline_size(value) <= INLINE_PAYLOAD_SOFT_LIMIT_BYTES:
            return value
        return _stage_job_value_as_data_ref(
            target=target,
            value=value,
            runtime=runtime,
            timeout_sec=timeout_sec,
            replica_count=replica_count,
            ttl_sec=ttl_sec,
            serialization_mode=serialization_mode,
        )
    if isinstance(value, (bytes, bytearray, memoryview)):
        if len(bytes(value)) <= INLINE_PAYLOAD_SOFT_LIMIT_BYTES:
            return bytes(value)
        return _stage_job_value_as_data_ref(
            target=target,
            value=bytes(value),
            runtime=runtime,
            timeout_sec=timeout_sec,
            replica_count=replica_count,
            ttl_sec=ttl_sec,
            serialization_mode=serialization_mode,
        )
    if isinstance(value, os.PathLike):
        path = Path(value).expanduser()
        size = path.stat().st_size if _path_is_file(path) else estimate_payload_inline_size(str(path))
        if size <= INLINE_PAYLOAD_SOFT_LIMIT_BYTES:
            return value
        return _stage_job_value_as_data_ref(
            target=target,
            value=value,
            runtime=runtime,
            timeout_sec=timeout_sec,
            replica_count=replica_count,
            ttl_sec=ttl_sec,
            serialization_mode=serialization_mode,
        )
    if isinstance(value, dict):
        try:
            inline_size = estimate_payload_inline_size(value)
        except Exception:
            inline_size = INLINE_PAYLOAD_SOFT_LIMIT_BYTES + 1
        if inline_size > INLINE_PAYLOAD_SOFT_LIMIT_BYTES:
            return _stage_job_value_as_data_ref(
                target=target,
                value=value,
                runtime=runtime,
                timeout_sec=timeout_sec,
                replica_count=replica_count,
                ttl_sec=ttl_sec,
                serialization_mode=serialization_mode,
            )
        return {
            str(key): _stage_job_submit_value(
                target=target,
                value=item,
                runtime=runtime,
                timeout_sec=timeout_sec,
                replica_count=replica_count,
                ttl_sec=ttl_sec,
                serialization_mode=serialization_mode,
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        try:
            inline_size = estimate_payload_inline_size(value)
        except Exception:
            inline_size = INLINE_PAYLOAD_SOFT_LIMIT_BYTES + 1
        if inline_size > INLINE_PAYLOAD_SOFT_LIMIT_BYTES:
            return _stage_job_value_as_data_ref(
                target=target,
                value=value,
                runtime=runtime,
                timeout_sec=timeout_sec,
                replica_count=replica_count,
                ttl_sec=ttl_sec,
                serialization_mode=serialization_mode,
            )
        return [
            _stage_job_submit_value(
                target=target,
                value=item,
                runtime=runtime,
                timeout_sec=timeout_sec,
                replica_count=replica_count,
                ttl_sec=ttl_sec,
                serialization_mode=serialization_mode,
            )
            for item in value
        ]
    if isinstance(value, tuple):
        return tuple(
            _stage_job_submit_value(
                target=target,
                value=item,
                runtime=runtime,
                timeout_sec=timeout_sec,
                replica_count=replica_count,
                ttl_sec=ttl_sec,
                serialization_mode=serialization_mode,
            )
            for item in value
        )
    try:
        if estimate_payload_inline_size(value) <= INLINE_PAYLOAD_SOFT_LIMIT_BYTES:
            return value
    except Exception:
        pass
    return _stage_job_value_as_data_ref(
        target=target,
        value=value,
        runtime=runtime,
        timeout_sec=timeout_sec,
        replica_count=replica_count,
        ttl_sec=ttl_sec,
        serialization_mode=serialization_mode,
    )


def _stage_job_submit_payload_for_transport(
    *,
    target: str,
    payload: Dict[str, object],
    timeout_sec: float,
    serialization_mode: str = "",
) -> Dict[str, object]:
    prepared = dict(payload or {})
    runtime = str(prepared.get("runtime", "py3") or "py3")
    replica_count = max(1, int(prepared.get("staging_replica_count", JOB_STAGING_REPLICA_COUNT) or JOB_STAGING_REPLICA_COUNT))
    ttl_sec = max(1, int(prepared.get("staging_ttl_sec", JOB_STAGED_REF_TTL_SEC) or JOB_STAGED_REF_TTL_SEC))
    for field_name in ("job_payload", "update_globals"):
        if field_name not in prepared:
            continue
        value = prepared.get(field_name)
        if field_name == "update_globals" and not isinstance(value, dict):
            continue
        if field_name == "job_payload" and value is not None and not isinstance(value, dict):
            raise ValueError("job_payload must be dict when provided")
        prepared[field_name] = _stage_job_submit_value(
            target=target,
            value=value,
            runtime=runtime,
            timeout_sec=timeout_sec,
            replica_count=replica_count,
            ttl_sec=ttl_sec,
            serialization_mode=serialization_mode,
        )
    return prepared


def _job_blob_requires_object_ref(blob: bytes) -> bool:
    raw_size = max(0, len(blob or b""))
    inline_threshold = max(256 * 1024, int(INLINE_PAYLOAD_HARD_LIMIT_BYTES / 1.5))
    return raw_size > inline_threshold


def _prepare_job_blob_submit_fields(
    *,
    target: str,
    blob: bytes,
    package_format: str,
    runtime: str,
    timeout_sec: float,
) -> Dict[str, object]:
    if not _job_blob_requires_object_ref(blob):
        return {"blob_b64": base64.b64encode(blob).decode("utf-8")}

    from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient
    from pycloud_parallel.controlplane.node_control_client import NodeControlClient

    with InfoCenterClient(target, timeout_sec=timeout_sec) as infocenter:
        selected_nodes = list(
            infocenter.select_task_nodes(
                healthy_only=True,
                node_count=1,
                limit=32,
                require_credit=False,
                preferred_runtime_key="",
                runtime=runtime,
            )
        )
    if not selected_nodes:
        raise RuntimeError("job code blob exceeds inline limit and no task node is available for large-object upload")

    selected_node = selected_nodes[0]
    with NodeControlClient(selected_node.control_addr, timeout_sec=timeout_sec) as node_client:
        ref = node_client.upload_object_from_bytes(
            blob=blob,
            format=normalize_object_format(package_format, default="bin"),
        )
    return {
        "blob_ref": ref,
        "blob_control_addr": str(selected_node.control_addr or "").strip(),
    }


def _job_submit_upload_clients(
    *,
    target: str,
    payload: Dict[str, object],
    timeout_sec: float,
) -> List[Any]:
    from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient
    from pycloud_parallel.controlplane.node_control_client import NodeControlClient

    runtime = str(payload.get("runtime", "py3") or "py3")
    tags = list(payload.get("tags") or ())
    node_ids = list(payload.get("node_ids") or ())
    node_limit = max(1, int(payload.get("node_limit", 32) or 32))
    requested_count = max(0, int(payload.get("pool_node_count", payload.get("node_count", 0)) or 0))
    fetch_limit = requested_count if requested_count > 0 else node_limit
    with InfoCenterClient(target, timeout_sec=timeout_sec) as infocenter:
        selected_nodes = list(
            infocenter.select_task_nodes(
                healthy_only=bool(payload.get("healthy_only", True)),
                tags=tags,
                node_ids=node_ids,
                node_count=fetch_limit,
                limit=node_limit,
                require_credit=False,
                preferred_runtime_key=str(payload.get("preferred_runtime_key", "") or "").strip(),
                runtime=runtime,
            )
        )
    return [
        NodeControlClient(node.control_addr, timeout_sec=timeout_sec)
        for node in selected_nodes
        if str(getattr(node, "control_addr", "") or "").strip()
    ]


def _prepare_job_submit_payload_for_call(
    *,
    target: str,
    payload: Dict[str, object],
    timeout_sec: float,
    serialization_mode: str = "",
    effective_policy: Optional[EffectivePolicy] = None,
) -> Dict[str, object]:
    prepared = dict(payload or {})
    preserved_fields = {
        field_name: prepared.pop(field_name)
        for field_name in ("job_payload", "update_globals")
        if field_name in prepared
    }
    clients: List[Any] = []
    try:
        clients = _job_submit_upload_clients(
            target=target,
            payload=prepared,
            timeout_sec=timeout_sec,
        )
        if not clients:
            prepared.update(preserved_fields)
            return prepared
        put_kwargs = {}
        if str(serialization_mode or "").strip() and str(serialization_mode).strip().lower() != "legacy_v1":
            put_kwargs["default_serialization_mode"] = serialization_mode
        outbound = prepare_outbound_payload(
            prepared,
            put_data=lambda value, *, format="": _put_data_via_clients(clients, value, format=format, **put_kwargs),
            estimate_inline_size=_estimate_managed_global_inline_size,
            policy=_payload_policy_for_mode("job_submit", effective_policy=effective_policy),
            **(
                {"managed_global_policy": _payload_policy_for_mode("managed_globals", effective_policy=effective_policy)}
                if effective_policy is not None
                else {}
            ),
        )
        outbound.update(preserved_fields)
        return outbound
    finally:
        for client in clients:
            with contextlib.suppress(Exception):
                client.close()


def _source_module_from_entry_module_arg(
    entry_module: Any,
    *,
    artifact_path: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]] = "",
    blob: Optional[bytes] = None,
) -> Optional[Any]:
    if blob is not None or artifact_path:
        return None
    if inspect.ismodule(entry_module):
        return entry_module
    return None


def _source_func_from_entry_callable_arg(
    entry_callable: Any,
    *,
    artifact_path: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]] = "",
    blob: Optional[bytes] = None,
) -> Optional[Callable]:
    if blob is not None or artifact_path:
        return None
    if isinstance(entry_callable, str):
        return None
    if callable(entry_callable):
        return entry_callable
    return None


def _auto_package_function(func: Callable) -> bytes:
    from pycloud_parallel.controlplane.dependency import DependencyPackager

    packager = DependencyPackager()
    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        packager.package_function(func, output_file=tmp_path, **_packaging_kwargs())
        with open(tmp_path, "rb") as f:
            return f.read()
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def _package_directory_to_targz(dir_path: Path) -> Path:
    from pycloud_parallel.controlplane.dependency import DependencyPackager

    return Path(DependencyPackager().package_directory(dir_path, **_packaging_kwargs()))


def _package_paths_to_targz(*, root_dir: Path, paths: Sequence[str]) -> Path:
    from pycloud_parallel.controlplane.dependency import DependencyPackager

    return Path(
        DependencyPackager().package_paths(
            root_dir=root_dir,
            paths=paths,
            **_packaging_kwargs(synthesize_missing_package_inits=True),
        )
    )


class _PreparedLocalArtifact:
    def __init__(
        self,
        *,
        source_path: Path,
        upload_path: Path,
        filename: str,
        package_format: str,
        cleanup_path: Optional[Path] = None,
    ) -> None:
        self.source_path = source_path
        self.upload_path = upload_path
        self.filename = filename
        self.package_format = package_format
        self.cleanup_path = cleanup_path

    def cleanup(self) -> None:
        if self.cleanup_path is not None:
            self.cleanup_path.unlink(missing_ok=True)


def _prepare_local_artifact_for_upload(
    artifact_path: Union[str, os.PathLike[str]],
    *,
    package_format: str = "",
) -> _PreparedLocalArtifact:
    path = Path(artifact_path)
    if not path.exists():
        raise FileNotFoundError(f"artifact_path not found: {artifact_path}")
    if path.is_dir():
        tar_path = _package_directory_to_targz(path)
        return _PreparedLocalArtifact(
            source_path=path,
            upload_path=tar_path,
            filename=f"{path.name}.tar.gz",
            package_format="tar.gz",
            cleanup_path=tar_path,
        )
    return _PreparedLocalArtifact(
        source_path=path,
        upload_path=path,
        filename=path.name,
        package_format=_resolve_package_format(package_format, path.name),
    )


def _prepare_code_blob(
    func: Optional[Callable] = None,
    module: Optional[Any] = None,
    artifact_path: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]] = "",
    blob: Optional[bytes] = None,
    resource_paths: Optional[Sequence[Union[str, os.PathLike[str]]]] = None,
) -> Tuple[Optional[bytes], str]:
    from pycloud_parallel.controlplane.dependency import (
        DependencyPackager,
        _TarSourceEntry,
        _normalize_arcname,
        _write_deterministic_targz,
    )

    packager = DependencyPackager()
    if module is not None:
        if not inspect.ismodule(module):
            raise ValueError("module must be a module object")
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            normalized_resource_paths = [str(item) for item in list(resource_paths or ()) if str(item or "").strip()]
            if not normalized_resource_paths:
                packager.package_module(module, output_file=tmp_path, **_packaging_kwargs())
            else:
                loaded_module = module
                deps = packager.analyzer.analyze_module(loaded_module)
                if deps.get("error"):
                    raise RuntimeError(deps["error"])

                module_name = str(getattr(loaded_module, "__name__", "") or deps.get("module_name") or "").strip()
                module_file = str(deps.get("file") or "").strip()
                if not module_file:
                    raise RuntimeError(f"cannot determine source file for module {module_name!r}")
                module_file_path = Path(module_file).resolve()
                module_dir = module_file_path.parent
                module_parts = [part for part in module_name.split(".") if part]
                if module_file_path.name == "__init__.py":
                    base_arc_dir = Path(*module_parts)
                elif len(module_parts) <= 1:
                    base_arc_dir = Path()
                else:
                    base_arc_dir = Path(*module_parts[:-1])

                entries = list(
                    packager._build_module_entries(
                        module_name=module_name,
                        module_file=module_file,
                        deps=deps,
                        include_tests=bool(_packaging_kwargs()["include_tests"]),
                    )
                )
                for raw in normalized_resource_paths:
                    candidate = Path(raw).expanduser()
                    resource_path = candidate.resolve() if candidate.is_absolute() else (module_dir / candidate).resolve()
                    if not resource_path.exists():
                        raise FileNotFoundError(f"resource path not found for module package: {resource_path}")
                    if not resource_path.is_file():
                        raise ValueError(f"resource_paths only accepts files: {resource_path}")
                    try:
                        rel = resource_path.relative_to(module_dir)
                    except ValueError as exc:
                        raise ValueError(
                            f"resource path must stay under the module directory: {resource_path}"
                        ) from exc
                    arcname = _normalize_arcname(base_arc_dir / rel)
                    entries.append(_TarSourceEntry(arcname=arcname, source_path=resource_path))
                _write_deterministic_targz(entries, tmp_path)
            with open(tmp_path, "rb") as f:
                blob = f.read()
            filename = f"{module.__name__}.tar.gz"
            return blob, filename
        finally:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    if func is not None:
        if not callable(func):
            raise ValueError("func must be callable")
        blob = _auto_package_function(func)
        filename = f"{func.__module__}_{func.__name__}.tar.gz"
        return blob, filename

    if blob is not None:
        return blob, ""

    if artifact_path:
        if isinstance(artifact_path, (list, tuple)):
            paths = [Path(str(p)) for p in artifact_path if str(p)]
            if not paths:
                raise ValueError("artifact_path list is empty")

            tar_path: Optional[str] = None
            try:
                tar_path = packager.package_roots(
                    paths,
                    **_packaging_kwargs(synthesize_missing_package_inits=True),
                )
                with open(tar_path, "rb") as f:
                    return f.read(), "artifact_bundle.tar.gz"
            finally:
                try:
                    Path(tar_path).unlink(missing_ok=True)
                except Exception:
                    pass

        prepared = _prepare_local_artifact_for_upload(artifact_path)
        try:
            return prepared.upload_path.read_bytes(), prepared.filename
        finally:
            prepared.cleanup()

    return None, ""


def _default_job_auth_ttl_sec() -> int:
    for key in ("PYCLOUD_JOB_AUTH_TTL_SEC", "PYCLOUD_JOB_CLIENT_AUTH_TTL_SEC"):
        raw = str(os.environ.get(key, "") or "").strip()
        if not raw:
            continue
        try:
            return max(60, int(raw))
        except Exception:
            continue
    return 24 * 60 * 60


def _target_to_base_url(target: str) -> str:
    text = str(target or "").strip()
    if not text:
        raise ValueError("target is required")
    parsed = urlparse(text)
    if parsed.scheme in ("http", "https"):
        return text.rstrip("/")
    return f"http://{text}"


def _default_job_client_session_cache_dir() -> Path:
    custom = str(os.environ.get("PYCLOUD_JOB_CLIENT_SESSION_DIR", "")).strip()
    if custom:
        return Path(custom).expanduser()
    return Path.home() / ".pycloud_parallel" / "job_client_sessions"


def _job_client_session_cache_file(
    *,
    target: str,
    service_name: str,
    client_scope: str = "",
    cache_dir: str = "",
) -> Path:
    base_dir = Path(cache_dir).expanduser() if str(cache_dir).strip() else _default_job_client_session_cache_dir()
    normalized_target = _target_to_base_url(target)
    return (
        base_dir
        / _sanitize_session_cache_part(normalized_target)
        / f"{_sanitize_session_cache_part(service_name)}__{_sanitize_session_cache_part(client_scope or 'default')}.json"
    )


def _parse_cache_datetime(value: object):
    from datetime import datetime, timezone

    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _load_job_client_session_cache(
    *,
    target: str,
    service_name: str,
    client_scope: str = "",
    cache_dir: str = "",
) -> Optional[Dict[str, object]]:
    from datetime import datetime, timezone

    path = _job_client_session_cache_file(
        target=target,
        service_name=service_name,
        client_scope=client_scope,
        cache_dir=cache_dir,
    )
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if int(payload.get("schema_version", 0) or 0) != _JOB_CLIENT_SESSION_SCHEMA_VERSION:
        return None
    if str(payload.get("target", "") or "").strip() != _target_to_base_url(target):
        return None
    if str(payload.get("service_name", "") or "").strip() != str(service_name or "").strip():
        return None
    cached_client_id = str(payload.get("client_id", "") or "").strip()
    cached_auth_token = str(payload.get("auth_token", "") or "").strip()
    expires_at = _parse_cache_datetime(payload.get("expires_at"))
    if client_scope and cached_client_id != str(client_scope or "").strip():
        return None
    if not cached_client_id or not cached_auth_token or expires_at is None:
        return None
    if expires_at <= datetime.now(timezone.utc):
        return None
    return payload


def _write_job_client_session_cache(
    *,
    target: str,
    service_name: str,
    client_scope: str,
    client_id: str,
    auth_token: str,
    cache_dir: str = "",
    ttl_sec: int = 0,
    recent_job_ids: Optional[Sequence[str]] = None,
) -> None:
    from datetime import timedelta

    normalized_client_id = str(client_id or "").strip()
    normalized_auth_token = str(auth_token or "").strip()
    if not normalized_client_id or not normalized_auth_token:
        return
    ttl = max(60, int(ttl_sec or _default_job_auth_ttl_sec()))
    now = _utc_now()
    payload: Dict[str, object] = {
        "schema_version": _JOB_CLIENT_SESSION_SCHEMA_VERSION,
        "target": _target_to_base_url(target),
        "service_name": str(service_name or "").strip(),
        "client_scope": str(client_scope or "").strip(),
        "client_id": normalized_client_id,
        "auth_token": normalized_auth_token,
        "saved_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=ttl)).isoformat(),
        "recent_job_ids": [str(job_id).strip() for job_id in list(recent_job_ids or []) if str(job_id).strip()],
    }
    path = _job_client_session_cache_file(
        target=target,
        service_name=service_name,
        client_scope=client_scope,
        cache_dir=cache_dir,
    )
    _write_private_json(path, payload)


_serialize_arrow_compatible = serialize_arrow_compatible


__all__ = [
    "_DEFAULT_EXPORT_DECORATOR",
    "_JOB_UPDATE_GLOBALS_AUTO",
    "_SERVICE_SESSION_LOCKED_PATHS",
    "_SERVICE_SESSION_LOCK_GUARD",
    "_SERVICE_SESSION_SCHEMA_VERSION",
    "_artifact_code_version",
    "_default_job_auth_ttl_sec",
    "_default_job_finalize_for_blob",
    "_default_job_finalize_for_module",
    "_default_job_handle_result_for_blob",
    "_default_job_handle_result_for_module",
    "_default_job_task_generator_for_blob",
    "_default_job_task_generator_for_module",
    "_default_job_update_globals_for_blob",
    "_default_service_session_cache_dir",
    "_emit_owner_notice",
    "_ensure_private_dir",
    "_filter_nodes_by_runtime",
    "_get_local_ip",
    "_load_job_client_session_cache",
    "_normalize_job_update_globals_arg",
    "_prepare_code_blob",
    "_prepare_http_payload_for_call",
    "_prepare_job_blob_submit_fields",
    "_prepare_job_submit_payload_for_call",
    "_prepare_managed_global_value_for_upload",
    "_prepare_managed_globals_batches_for_upload",
    "_prepare_managed_globals_values_for_upload",
    "_prepare_local_artifact_for_upload",
    "_prepare_task_payload_for_submit",
    "_package_paths_to_targz",
    "_put_data_via_clients",
    "_resolve_high_level_service_data",
    "_resolve_high_level_service_results",
    "_RetryableReadyError",
    "_retry_infocenter_request",
    "_sanitize_session_cache_part",
    "_serialize_arrow_compatible",
    "_source_func_from_entry_callable_arg",
    "_source_module_from_entry_module_arg",
    "_stage_job_submit_payload_for_transport",
    "_summarize_discovered_nodes",
    "_target_to_base_url",
    "_timestamp_to_datetime",
    "_write_job_client_session_cache",
    "_write_private_json",
    "_serialize_data_for_object_ref",
    "_job_blob_requires_object_ref",
    "_job_client_session_cache_file",
]
