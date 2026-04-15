from __future__ import annotations

"""Client helpers for InfoCenter/NodeControl service-session workflow."""

import asyncio
import base64
from collections import deque
import contextlib
import errno
import hashlib
import inspect
import json
import logging
import io
import math
import os
import queue
import re
import socket
import sys
import tempfile
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
import uuid
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple, Union
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen

import grpc
from google.protobuf import timestamp_pb2

from pycloud_parallel.controlplane.config import (
    FILE_HASH_CHUNK_SIZE_BYTES,
    INLINE_PAYLOAD_HARD_LIMIT_BYTES,
    JOB_PAYLOAD_MAX_BYTES,
    JOB_STAGED_REF_TTL_SEC,
    JOB_STAGING_REPLICA_COUNT,
    OBJECT_CHUNK_SIZE_BYTES,
    get_payload_policy,
    grpc_channel_options,
)
from pycloud_parallel.controlplane.artifact import (
    Artifact as _ExtractedArtifact,
    ArtifactDeps as _ExtractedArtifactDeps,
    ArtifactExports as _ExtractedArtifactExports,
    PreparedArtifact as _ExtractedPreparedArtifact,
    _coerce_artifact_deps as _extracted_coerce_artifact_deps,
    _normalize_artifact_input as _extracted_normalize_artifact_input,
    _prepare_artifact as _extracted_prepare_artifact,
)
from pycloud_parallel.controlplane.data_ref import (
    DataRef as _ExtractedDataRef,
    maybe_data_ref as _extracted_maybe_data_ref,
)
from pycloud_parallel.controlplane.client_transport import (
    DiscoveryCallError,
    _call_route_http,
    _decode_http_request_body,
    _decode_http_response_body,
    _encode_http_json_body,
    _is_route_failure,
    _list_route_methods_http,
    _materialize_downloaded_result,
    _normalize_http_response_body,
    _serialize_http_call_payload,
    _serialize_route,
)
from pycloud_parallel.controlplane.task_backend import (
    NativeTaskBackend as _ExtractedNativeTaskBackend,
    TaskPoolItem as _ExtractedTaskPoolItem,
    _ServiceCompatTaskBackend as _ExtractedServiceCompatTaskBackend,
    _TaskPoolCallProxy as _ExtractedTaskPoolCallProxy,
)
from pycloud_parallel.controlplane.discovery_route_cache import (
    _DiscoveryRouteCache as _ExtractedDiscoveryRouteCache,
    _RouteLocalState as _ExtractedRouteLocalState,
    _ServiceRouteSnapshot as _ExtractedServiceRouteSnapshot,
)
from pycloud_parallel.controlplane.infocenter_client import (
    InfoCenterClient as _ExtractedInfoCenterClient,
    InfoCenterNode as _ExtractedInfoCenterNode,
    InfoCenterNodeService as _ExtractedInfoCenterNodeService,
    InfoCenterNodeTaskPool as _ExtractedInfoCenterNodeTaskPool,
    InfoCenterServiceRoute as _ExtractedInfoCenterServiceRoute,
    NodeCircuitState as _ExtractedNodeCircuitState,
    _build_unique_node_id_map as _extracted_build_unique_node_id_map,
    _node_instance_key_from_node as _extracted_node_instance_key_from_node,
    _node_instance_key_from_route as _extracted_node_instance_key_from_route,
    _route_predicted_busy as _extracted_route_predicted_busy,
    _route_sort_key as _extracted_route_sort_key,
)
from pycloud_parallel.controlplane.replica_client import (
    NativeTaskPoolClient as _ExtractedNativeTaskPoolClient,
    ServiceSessionClient as _ExtractedServiceSessionClient,
)
from pycloud_parallel.controlplane.payload_transport import (
    decode_result_from_transport,
    decode_payload_from_transport,
    encode_payload_for_transport,
    estimate_payload_inline_size,
    prepare_outbound_value,
    prepare_outbound_payload,
)
from pycloud_parallel.controlplane.runtime_spec import (
    matches_python_runtime,
    normalize_python_runtime_spec,
)
from pycloud_parallel.controlplane.object_ref import (
    ObjectRef,
    normalize_materialize_as,
    normalize_object_format,
    object_id_from_sha256_hex,
)
from pycloud_parallel.controlplane.result_ref import ResultRef
from pycloud_parallel.controlplane.session_handle import ExecutionReplicaHandle
from pycloud_parallel.controlplane.session_model import (
    ExecutionReplicaSnapshot,
    SessionBinding,
    SessionIdentity,
    SessionLease,
)
from pycloud_parallel.controlplane.serialization import (
    INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    dataframe_bundle_parquet_frame,
    deserialize_dataframe_bundle,
    deserialize_series_bundle,
    dict_to_struct,
    log_payload_flow,
    serialize_arrow_compatible,
    serialize_dataframe_bundle,
    serialize_series_bundle,
    serialize_inline_payload,
    summarize_payload_flow_value,
    struct_to_dict,
    validate_inline_payload_structs,
)
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc

logger = logging.getLogger(__name__)

Artifact = _ExtractedArtifact
ArtifactDeps = _ExtractedArtifactDeps
ArtifactExports = _ExtractedArtifactExports
PreparedArtifact = _ExtractedPreparedArtifact
DataRef = _ExtractedDataRef
_coerce_artifact_deps = _extracted_coerce_artifact_deps
_normalize_artifact_input = _extracted_normalize_artifact_input
_prepare_artifact = _extracted_prepare_artifact
_maybe_data_ref = _extracted_maybe_data_ref

_SERVICE_SESSION_LOCK_GUARD = threading.Lock()
_SERVICE_SESSION_LOCKED_PATHS: Set[str] = set()
_JOB_UPDATE_GLOBALS_AUTO = object()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp_to_datetime(value: timestamp_pb2.Timestamp | None) -> datetime:
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
    print(f"[DeployedService] {text}", file=sys.stderr, flush=True)


def _summarize_discovered_nodes(nodes: Sequence["InfoCenterNode"], *, limit: int = 8) -> str:
    rows: List[str] = []
    for node in list(nodes)[: max(1, int(limit))]:
        rows.append(
            f"{node.node_id}(healthy={'yes' if node.healthy else 'no'},"
            f"schedulable={'yes' if node.schedulable else 'no'},"
            f"drain={'yes' if node.drain else 'no'},"
            f"svc_avail={int(node.service_worker_available)},"
            f"py={node.python_version or '-'})"
        )
    if len(nodes) > max(1, int(limit)):
        rows.append(f"...+{len(nodes) - max(1, int(limit))} more")
    return ", ".join(rows) if rows else "(none)"


def pycloud_export(fn):
    fn.__pycloud_export__ = True
    return fn


_DEFAULT_EXPORT_DECORATOR = "pycloud_export"


def _auto_package_function(func: Callable) -> bytes:
    """自动打包函数及其依赖。

    Args:
        func: 要打包的函数

    Returns:
        bytes: tar.gz 格式的包内容
    """
    from pycloud_parallel.controlplane.dependency import DependencyPackager

    packager = DependencyPackager()

    # 创建临时文件
    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # 打包函数和依赖
        packager.package_function(
            func,
            output_file=tmp_path,
            include_tests=False,
        )

        # 读取包内容
        with open(tmp_path, "rb") as f:
            return f.read()
    finally:
        # 清理临时文件
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def _infer_entry_module_from_source_file(source_file: str) -> str:
    path = Path(str(source_file or "")).resolve()
    if not path.exists() or path.suffix != ".py":
        return ""
    parts = [path.stem]
    parent = path.parent
    while (parent / "__init__.py").exists():
        parts.append(parent.name)
        parent = parent.parent
    return ".".join(reversed(parts))


def _default_entry_module_for_func(func: Callable) -> str:
    module_name = str(getattr(func, "__module__", "") or "").strip()
    try:
        source_file = inspect.getsourcefile(func) or inspect.getfile(func)
    except Exception:
        source_file = ""
    inferred = _infer_entry_module_from_source_file(str(source_file or ""))
    if module_name and module_name != "__main__" and not module_name.startswith("_pycloud_user_"):
        return module_name
    return inferred or module_name or "user_function"


def _default_entry_module_for_module(module: Any) -> str:
    module_name = str(getattr(module, "__name__", "") or "").strip()
    if module_name and module_name != "__main__":
        return module_name
    module_file = str(getattr(module, "__file__", "") or "").strip()
    inferred = _infer_entry_module_from_source_file(module_file)
    return inferred or module_name or "user_module"


def _normalize_entry_module_arg(entry_module: Any) -> str:
    """Normalize entry_module to a dotted module name string.

    Accepts either a module object or a string-like value. Module objects are
    converted to ``module.__name__`` so callers do not have to extract the name
    manually before invoking deploy/upload helpers.
    """
    if inspect.ismodule(entry_module):
        return str(getattr(entry_module, "__name__", "") or "").strip()
    return str(entry_module or "").strip()


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


def _normalize_entry_callable_arg(entry_callable: Any) -> str:
    if not isinstance(entry_callable, str) and callable(entry_callable):
        return str(getattr(entry_callable, "__name__", "") or "").strip()
    return str(entry_callable or "").strip()


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


def _validate_job_payload_control_params(
    value: object,
    *,
    context: str = "job_payload",
    limit_bytes: Optional[int] = None,
) -> Dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{context} must be dict of small control parameters")

    def _validate(item: object, *, path: str) -> None:
        if _maybe_data_ref(item) is not None:
            raise ValueError(f"{path} must not contain ObjectRef/DataRef/ResultRef")
        if item is None or isinstance(item, (str, int, float, bool, datetime, date, dt_time, timedelta)):
            return
        if isinstance(item, (bytes, bytearray, memoryview)):
            raise TypeError(f"{path} must not contain bytes-like values")
        if isinstance(item, os.PathLike):
            raise TypeError(f"{path} must not contain path-like values")
        if inspect.ismodule(item) or inspect.isclass(item) or inspect.isfunction(item) or callable(item):
            raise TypeError(f"{path} must not contain callable/module/class values")
        if isinstance(item, dict):
            for key, nested in item.items():
                if not isinstance(key, str) or not str(key).strip():
                    raise TypeError(f"{path} must use non-empty string keys")
                _validate(nested, path=f"{path}.{str(key).strip()}")
            return
        if isinstance(item, (list, tuple)):
            for idx, nested in enumerate(item):
                _validate(nested, path=f"{path}[{idx}]")
            return
        raise TypeError(f"{path} has unsupported control parameter type {type(item).__name__}")

    normalized = dict(value)
    _validate(normalized, path=context)
    serialized = serialize_arrow_compatible(normalized)
    effective_limit = max(1, int(limit_bytes if limit_bytes is not None else JOB_PAYLOAD_MAX_BYTES))
    size_bytes = len(json.dumps(serialized, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    if size_bytes > effective_limit:
        raise ValueError(
            f"{context} serialized to {size_bytes} bytes, "
            f"which exceeds the job payload limit {effective_limit} bytes"
        )
    return normalized


_JOB_SUBMIT_STAGING_FIELDS = ("job_payload", "update_globals")
_JOB_SUBMIT_STAGING_CODE_FIELDS = {
    "blob_ref",
    "blob_b64",
    "blob_control_addr",
    "driver_blob_ref",
    "driver_blob_b64",
    "driver_blob_control_addr",
}


def _select_job_staging_clients(
    *,
    target: str,
    runtime: str,
    timeout_sec: float,
    replica_count: int,
) -> Tuple[List[Any], List[Dict[str, object]]]:
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
    clients = [NodeControlClient(str(node.control_addr), timeout_sec=timeout_sec) for node in selected]
    replicas = [
        {
            "node_id": str(getattr(node, "node_id", "") or ""),
            "node_instance_id": str(getattr(node, "node_instance_id", "") or ""),
            "control_addr": str(getattr(node, "control_addr", "") or ""),
        }
        for node in selected
    ]
    return clients, replicas


def _upload_text_data_via_clients(
    clients: Sequence["NodeControlClient"],
    text: str,
) -> ObjectRef:
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
    return ObjectRef(
        object_id=first.object_id,
        format=first.format,
        size_bytes=first.size_bytes,
        materialize_as="text",
        consume_on_read=False,
    )


def _stage_job_value_as_data_ref(
    *,
    target: str,
    value: Any,
    runtime: str,
    timeout_sec: float,
    replica_count: int,
    ttl_sec: int,
) -> DataRef:
    data_ref = _maybe_data_ref(value)
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
            object_ref = _put_data_via_clients(clients, upload_value)
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
) -> Any:
    if _maybe_data_ref(value) is not None:
        return value
    if value is None or isinstance(value, (bool, int, float, datetime, date, dt_time, timedelta)):
        return value
    if isinstance(value, str):
        if estimate_payload_inline_size(value) <= INLINE_PAYLOAD_SOFT_LIMIT_BYTES:
            return value
        return _stage_job_value_as_data_ref(
            target=target,
            value=value,
            runtime=runtime,
            timeout_sec=timeout_sec,
            replica_count=replica_count,
            ttl_sec=ttl_sec,
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
        )
    if isinstance(value, os.PathLike):
        path = Path(value).expanduser()
        size = path.stat().st_size if path.exists() and path.is_file() else estimate_payload_inline_size(str(path))
        if size <= INLINE_PAYLOAD_SOFT_LIMIT_BYTES:
            return value
        return _stage_job_value_as_data_ref(
            target=target,
            value=value,
            runtime=runtime,
            timeout_sec=timeout_sec,
            replica_count=replica_count,
            ttl_sec=ttl_sec,
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
            )
        return {
            str(key): _stage_job_submit_value(
                target=target,
                value=item,
                runtime=runtime,
                timeout_sec=timeout_sec,
                replica_count=replica_count,
                ttl_sec=ttl_sec,
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
            )
        return [
            _stage_job_submit_value(
                target=target,
                value=item,
                runtime=runtime,
                timeout_sec=timeout_sec,
                replica_count=replica_count,
                ttl_sec=ttl_sec,
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
            )
            for item in value
        )
    return value


def _stage_job_submit_payload_for_transport(
    *,
    target: str,
    payload: Dict[str, object],
    timeout_sec: float,
) -> Dict[str, object]:
    prepared = dict(payload or {})
    runtime = str(prepared.get("runtime", "py3") or "py3")
    replica_count = max(1, int(prepared.get("staging_replica_count", JOB_STAGING_REPLICA_COUNT) or JOB_STAGING_REPLICA_COUNT))
    ttl_sec = max(1, int(prepared.get("staging_ttl_sec", JOB_STAGED_REF_TTL_SEC) or JOB_STAGED_REF_TTL_SEC))
    for field_name in _JOB_SUBMIT_STAGING_FIELDS:
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
        )
    return prepared


def _module_object_for_func(func: Callable) -> Optional[Any]:
    module = inspect.getmodule(func)
    if module is not None:
        return module
    module_name = str(getattr(func, "__module__", "") or "").strip()
    if module_name:
        return sys.modules.get(module_name)
    return None


def _resolve_job_hook_name_from_module(
    module: Any,
    *,
    candidates: Sequence[str],
    required: bool = False,
    label: str,
) -> str:
    for name in candidates:
        normalized = str(name or "").strip()
        if not normalized:
            continue
        candidate = getattr(module, normalized, None)
        if callable(candidate):
            return normalized
    if required:
        raise ValueError(f"{label} callable not found in run module; checked {list(candidates)}")
    return ""


def _invoke_job_helper_callable_locally(fn: Callable, payload: Optional[Dict[str, object]]) -> Any:
    from pycloud_parallel.controlplane.state import _invoke_user_callable

    return _invoke_user_callable(fn, dict(payload or {}))


def _resolve_job_update_globals_prepared(
    update_globals: Any,
    *,
    module: Any,
    job_payload: Optional[Dict[str, object]],
) -> Tuple[Optional[Dict[str, object]], List[str]]:
    value = getattr(module, "update_globals", None) if update_globals is _JOB_UPDATE_GLOBALS_AUTO else update_globals
    if value is None:
        return None, []

    if isinstance(value, str):
        normalized = str(value or "").strip()
        if not normalized:
            return None, []
        value = getattr(module, normalized, None)
        if value is None:
            raise ValueError(f"update_globals callable not found in run module: {normalized}")

    if callable(value):
        value = _invoke_job_helper_callable_locally(value, job_payload)

    if value is None:
        return None, []
    if not isinstance(value, dict):
        raise TypeError("resolved update_globals must be dict or callable returning dict")

    names = [str(name).strip() for name in value.keys() if str(name).strip()]
    missing = [name for name in names if not hasattr(module, name)]
    if missing:
        raise ValueError(f"managed globals not found in run module: {missing}")
    return dict(value), names


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
        raise RuntimeError("job code blob exceeds inline limit and no task node is available for ObjectRef upload")

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
) -> List["NodeControlClient"]:
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
) -> Dict[str, object]:
    prepared = dict(payload or {})
    clients: List[NodeControlClient] = []
    try:
        clients = _job_submit_upload_clients(
            target=target,
            payload=prepared,
            timeout_sec=timeout_sec,
        )
        if not clients:
            return prepared
        return prepare_outbound_payload(
            prepared,
            put_data=lambda value, *, format="": _put_data_via_clients(clients, value, format=format),
            estimate_inline_size=_estimate_managed_global_inline_size,
            policy=get_payload_policy("job_submit"),
        )
    finally:
        for client in clients:
            with contextlib.suppress(Exception):
                client.close()


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


def _infer_entry_module_from_artifact_path(
    artifact_path: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]] = "",
) -> str:
    if not artifact_path:
        return ""
    if isinstance(artifact_path, (list, tuple)):
        first_path = next((Path(str(p)) for p in artifact_path if str(p)), None)
        if first_path is not None and first_path.suffix == ".py":
            return first_path.stem
        return ""
    path = Path(artifact_path)
    if path.suffix == ".py":
        return path.stem
    return ""


def _prepare_code_blob(
    func: Optional[Callable] = None,
    module: Optional[Any] = None,
    artifact_path: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]] = "",
    blob: Optional[bytes] = None,
) -> Tuple[Optional[bytes], str]:
    """准备代码 blob 和文件名。

    智能处理模块对象、函数对象、文件路径/路径列表、直接 blob 四种情况。

    Args:
        func: 函数对象（自动打包依赖）
        module: 模块对象（自动打包整个模块）
        artifact_path: 文件路径、文件夹路径或路径列表
        blob: 直接提供的 blob

    Returns:
        (blob, filename): blob 内容和文件名
    """
    from pycloud_parallel.controlplane.dependency import DependencyPackager

    packager = DependencyPackager()

    # 优先级 1: 模块对象（自动打包整个模块）
    if module is not None:
        if not inspect.ismodule(module):
            raise ValueError("module must be a module object")

        # 创建临时文件
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # 打包模块和依赖
            packager.package_module(
                module,
                output_file=tmp_path,
                include_tests=False,
            )

            # 读取包内容
            with open(tmp_path, "rb") as f:
                blob = f.read()

            # 确定文件名
            filename = f"{module.__name__}.tar.gz"

            return blob, filename
        finally:
            # 清理临时文件
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    # 优先级 2: 函数对象（自动打包）
    if func is not None:
        if not callable(func):
            raise ValueError("func must be callable")

        # 自动打包函数和依赖
        blob = _auto_package_function(func)

        # 确定文件名
        filename = f"{func.__module__}_{func.__name__}.tar.gz"

        return blob, filename

    # 优先级 3: 直接提供的 blob
    if blob is not None:
        return blob, ""

    # 优先级 4: 文件路径 / 路径列表
    if artifact_path:
        if isinstance(artifact_path, (list, tuple)):
            paths = [Path(str(p)) for p in artifact_path if str(p)]
            if not paths:
                raise ValueError("artifact_path list is empty")

            tar_path: Optional[str] = None
            try:
                tar_path = packager.package_roots(
                    paths,
                    include_tests=False,
                    synthesize_missing_package_inits=True,
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

    # 没有提供任何代码
    return None, ""


def _serialize_arrow_compatible(obj: Any) -> Any:
    """序列化 Arrow 兼容对象为字典。

    用于 Service Session 模式的 HTTP 调用。

    Args:
        obj: 要序列化的对象

    Returns:
        Any: 可 JSON 序列化的对象
    """
    return serialize_arrow_compatible(obj)


def _validate_task_submit_items(tasks: Sequence[pb2.TaskSubmitItem], *, request_context: str) -> None:
    validate_inline_payload_structs(
        [item.payload for item in tasks],
        item_context="task payload",
        request_context=request_context,
    )




def _extract_result_ref(value: object) -> Optional[DataRef]:
    direct = _maybe_data_ref(value)
    if direct is not None:
        return direct
    if isinstance(value, dict):
        return _maybe_data_ref(value.get("data"))
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


def _resolve_task_results_data(batch: Any, results: Sequence[pb2.TaskResult]) -> List[Any]:
    return [batch.fetch_result_data(item) for item in results]


def _inline_task_result_data(task_result: pb2.TaskResult, *, data: Any) -> pb2.TaskResult:
    serialized = serialize_arrow_compatible(data)
    wrapped = serialized if isinstance(serialized, dict) else {"value": serialized}
    resolved = pb2.TaskResult()
    resolved.CopyFrom(task_result)
    resolved.result.Clear()
    resolved.result.update(wrapped)
    return resolved


def _resolve_high_level_task_result(batch: Any, task_result: pb2.TaskResult) -> pb2.TaskResult:
    if int(task_result.status) != int(pb2.TASK_STATUS_SUCCEEDED):
        return task_result
    data = struct_to_dict(task_result.result)
    if not isinstance(data, ResultRef):
        return task_result
    resolved_data = batch.fetch_result_data(task_result)
    try:
        return _inline_task_result_data(task_result, data=resolved_data)
    except TypeError:
        # Bytes/path-like values cannot be represented by protobuf Struct; keep the raw ResultRef envelope.
        return task_result


def _resolve_high_level_task_results(batch: Any, results: Sequence[pb2.TaskResult]) -> List[pb2.TaskResult]:
    return [_resolve_high_level_task_result(batch, item) for item in results]


def _resolve_high_level_pull_results_response(
    batch: Any,
    response: pb2.PullResultsResponse,
) -> pb2.PullResultsResponse:
    resolved = pb2.PullResultsResponse()
    resolved.CopyFrom(response)
    resolved.ClearField("results")
    resolved.results.extend(_resolve_high_level_task_results(batch, response.results))
    return resolved


def _get_local_ip() -> str:
    """获取本机 IP 地址。

    Returns:
        str: 本机 IP 地址，如果获取失败返回 "localhost"
    """
    try:
        # 创建一个 UDP socket，不实际发送数据
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            # 连接到一个外部地址（不实际发送数据）
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            return local_ip
    except Exception:
        return "localhost"


def _now_timestamp() -> timestamp_pb2.Timestamp:
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(datetime.now(timezone.utc))
    return ts


def _err_msg(resp_error: pb2.Error, default_msg: str) -> str:
    if resp_error and resp_error.message:
        return resp_error.message
    return default_msg


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


def _target_to_base_url(target: str) -> str:
    text = str(target or "").strip()
    if not text:
        raise ValueError("target is required")
    parsed = urlparse(text)
    if parsed.scheme in ("http", "https"):
        return text.rstrip("/")
    return f"http://{text}"


def _http_json_request(
    *,
    base_url: str,
    path: str,
    method: str,
    timeout_sec: float,
    payload: Optional[Dict[str, object]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, object]:
    raw = None
    request_headers = dict(headers or {})
    if payload is not None:
        payload = _serialize_arrow_compatible(payload)
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request_headers["Content-Type"] = "application/json; charset=utf-8"

    url = f"{base_url.rstrip('/')}{path}"
    logger.debug(
        "http request method=%s url=%s payload=%s headers=%s",
        method.upper(),
        url,
        payload if payload is not None else None,
        request_headers,
    )

    req = Request(
        url,
        method=method.upper(),
        headers=request_headers,
        data=raw,
    )
    try:
        with urlopen(req, timeout=max(0.1, float(timeout_sec))) as resp:
            data = _normalize_http_response_body(json.loads(resp.read().decode("utf-8") or "{}"))
    except HTTPError as exc:
        try:
            body = _normalize_http_response_body(json.loads((exc.read() or b"{}").decode("utf-8") or "{}"))
        except Exception:
            body = {"ok": False, "error": exc.reason}
        raise RuntimeError(str(body.get("error", exc.reason))) from exc
    if data.get("ok", False) is False:
        raise RuntimeError(str(data.get("error", "request failed")))
    return data


def _is_transient_infocenter_error(exc: Exception) -> bool:
    candidate: object = exc
    if isinstance(candidate, URLError):
        candidate = candidate.reason
    if isinstance(candidate, socket.timeout):
        return True
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
            if not _is_transient_infocenter_error(exc):
                raise
            last_exc = exc
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"InfoCenter {target} not ready for {action} after {float(timeout_sec):.1f}s: {exc}"
                ) from exc
            time.sleep(min(retry_interval_sec, max(0.05, deadline - time.monotonic())))


def _sha256_file(path: Path, *, chunk_size: int = FILE_HASH_CHUNK_SIZE_BYTES) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(max(1, int(chunk_size)))
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _iter_file_chunks(path: Path, *, chunk_size: int = OBJECT_CHUNK_SIZE_BYTES) -> Iterator[bytes]:
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(max(1, int(chunk_size)))
            if not chunk:
                break
            yield chunk


def _package_format_from_filename(filename: str) -> str:
    lower = str(filename or "").lower()
    if lower.endswith(".tar.gz") or lower.endswith(".tgz"):
        return "tar.gz"
    if lower.endswith(".zip"):
        return "zip"
    if lower.endswith(".whl"):
        return "whl"
    if lower.endswith(".py"):
        return "py"
    return "bin"


def _resolve_package_format(package_format: str, filename: str = "", *, default: str = "bin") -> str:
    explicit = str(package_format or "").strip().lower()
    if explicit:
        return explicit
    inferred = _package_format_from_filename(filename)
    if inferred != "bin":
        return inferred
    fallback = str(default or "bin").strip().lower()
    return fallback or "bin"


def _default_artifact_filename(
    *,
    package_format: str,
    entry_module: Any = "",
    fallback_stem: str = "artifact",
) -> str:
    stem = _normalize_entry_module_arg(entry_module).split(".")[-1].strip()
    if not stem:
        stem = str(fallback_stem or "artifact").strip() or "artifact"
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("._") or "artifact"

    normalized_format = _resolve_package_format(package_format, default="py")
    if normalized_format == "tar.gz":
        suffix = ".tar.gz"
    elif normalized_format == "zip":
        suffix = ".zip"
    elif normalized_format == "whl":
        suffix = ".whl"
    elif normalized_format == "py":
        suffix = ".py"
    else:
        suffix = ".bin"
    return f"{stem}{suffix}"


def _default_entry_module_for_package(
    *,
    package_format: str,
    entry_module: Any = "",
    fallback_stem: str = "artifact",
) -> str:
    normalized_module = _normalize_entry_module_arg(entry_module).strip()
    if normalized_module:
        return normalized_module
    if _resolve_package_format(package_format, default="py") != "py":
        return ""
    return Path(
        _default_artifact_filename(
            package_format=package_format,
            entry_module="",
            fallback_stem=fallback_stem,
        )
    ).stem


def _build_export_spec(
    *,
    export_mode: str,
    export_methods: Optional[Sequence[str]],
) -> pb2.ModuleExportSpec:
    return pb2.ModuleExportSpec(
        mode=str(export_mode or "").strip(),
        methods=[x.strip() for x in (export_methods or []) if str(x).strip()],
        decorator=_DEFAULT_EXPORT_DECORATOR,
    )


def _package_directory_to_targz(dir_path: Path) -> Path:
    from pycloud_parallel.controlplane.dependency import DependencyPackager

    return Path(DependencyPackager().package_directory(dir_path, include_tests=False))


def _package_paths_to_targz(*, root_dir: Path, paths: Sequence[str]) -> Path:
    from pycloud_parallel.controlplane.dependency import DependencyPackager

    return Path(
        DependencyPackager().package_paths(
            root_dir=root_dir,
            paths=paths,
            include_tests=False,
        )
    )


@dataclass(frozen=True)
class _PreparedLocalArtifact:
    source_path: Path
    upload_path: Path
    filename: str
    package_format: str
    cleanup_path: Optional[Path] = None

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


def _serialize_data_for_object_ref(
    data: Any,
    *,
    format: str = "",
    materialize_as: str = "auto",
) -> Tuple[str, str, bytes]:
    log_payload_flow(
        "object_ref_upload_prepare",
        format=(format or "auto"),
        materialize_as=materialize_as,
        summary=summarize_payload_flow_value(data),
    )
    if isinstance(data, ObjectRef):
        raise ValueError("ObjectRef is already uploaded; no need to serialize again")

    if isinstance(data, os.PathLike):
        path = Path(data).expanduser()
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"path not found or not a file: {path}")
        log_payload_flow("object_ref_upload", path_type="file", format=normalize_object_format(format, source_name=path.name))
        return "path", normalize_object_format(format, source_name=path.name), path.read_bytes()

    if isinstance(data, str):
        path = Path(data).expanduser()
        if path.exists() and path.is_file():
            log_payload_flow("object_ref_upload", path_type="string-file", format=normalize_object_format(format, source_name=path.name))
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
        "supported inputs are file paths, pandas.DataFrame, numpy.ndarray, dict/list, bytes, and ObjectRef"
    )


def _put_data_via_clients(
    clients: Sequence["NodeControlClient"],
    data: Any,
    *,
    format: str = "",
    chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
) -> ObjectRef:
    if isinstance(data, ObjectRef):
        return data
    materialize_as, effective_format, blob = _serialize_data_for_object_ref(
        data,
        format=format,
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
    return ObjectRef(
        object_id=first.object_id,
        format=first.format,
        size_bytes=first.size_bytes,
        materialize_as=normalize_materialize_as(materialize_as, default="path"),
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


def _prepare_payload_for_policy(
    clients: Sequence["NodeControlClient"],
    payload: Optional[Dict[str, object]],
    *,
    policy,
) -> Dict[str, object]:
    return prepare_outbound_payload(
        payload,
        put_data=lambda value, *, format="": _put_data_via_clients(clients, value, format=format),
        estimate_inline_size=_estimate_managed_global_inline_size,
        policy=policy,
    )


def _prepare_value_for_policy(
    clients: Sequence["NodeControlClient"],
    value: Any,
    *,
    policy,
    preserve_container: bool = False,
) -> Any:
    return prepare_outbound_value(
        value,
        put_data=lambda data, *, format="": _put_data_via_clients(clients, data, format=format),
        estimate_inline_size=_estimate_managed_global_inline_size,
        policy=policy,
        preserve_container=preserve_container,
    )


def _prepare_payload_value_for_upload(
    clients: Sequence["NodeControlClient"],
    value: Any,
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    preserve_container: bool = False,
    recurse_containers: bool = False,
    upload_pathlike: bool = False,
    upload_string_file: bool = False,
    upload_bytes: bool = False,
    consume_on_read: bool = False,
) -> Any:
    policy = _policy_with_soft_limit(
        replace(
            get_payload_policy("managed_globals"),
            objectify_pathlikes=bool(upload_pathlike),
            objectify_strings_as_files=bool(upload_string_file),
            objectify_bytes=bool(upload_bytes),
            recurse_containers=bool(recurse_containers),
            consume_on_read=bool(consume_on_read),
        ),
        object_threshold_bytes,
    )
    return _prepare_value_for_policy(
        clients,
        value,
        policy=policy,
        preserve_container=preserve_container,
    )


def _prepare_managed_global_value_for_upload(
    clients: Sequence["NodeControlClient"],
    value: Any,
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
) -> Any:
    try:
        inline_size = _estimate_managed_global_inline_size(value)
    except Exception as exc:
        log_payload_flow(
            "managed_global_estimate_failed",
            threshold_bytes=max(1, int(object_threshold_bytes)),
            summary=summarize_payload_flow_value(value),
            error=repr(exc),
        )
        return value
    if inline_size <= max(1, int(object_threshold_bytes)):
        log_payload_flow(
            "managed_global_inline",
            threshold_bytes=max(1, int(object_threshold_bytes)),
            size_bytes=inline_size,
            summary=summarize_payload_flow_value(value),
        )
        return value

    try:
        prepared = _prepare_payload_value_for_upload(
            clients,
            value,
            object_threshold_bytes=object_threshold_bytes,
            upload_pathlike=True,
            upload_string_file=True,
            upload_bytes=True,
            consume_on_read=False,
        )
        log_payload_flow(
            "managed_global_objectref_ready",
            threshold_bytes=max(1, int(object_threshold_bytes)),
            size_bytes=inline_size,
            summary=summarize_payload_flow_value(prepared),
        )
        return prepared
    except Exception as exc:
        log_payload_flow(
            "managed_global_objectref_failed",
            threshold_bytes=max(1, int(object_threshold_bytes)),
            size_bytes=inline_size,
            summary=summarize_payload_flow_value(value),
            error=repr(exc),
        )
        raise ValueError(
            "managed global exceeds inline threshold and ObjectRef upload failed: "
            f"size_bytes={inline_size} threshold_bytes={max(1, int(object_threshold_bytes))}; "
            f"error={exc}"
        ) from exc


def _prepare_managed_globals_values_for_upload(
    clients: Sequence["NodeControlClient"],
    values: Dict[str, object],
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
) -> Dict[str, object]:
    return {
        str(name): _prepare_managed_global_value_for_upload(
            clients,
            value,
            object_threshold_bytes=object_threshold_bytes,
        )
        for name, value in (values or {}).items()
    }


def _prepare_task_payload_for_submit(
    client: "NodeControlClient",
    payload: Dict[str, object],
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
) -> Any:
    return _prepare_payload_for_policy(
        [client],
        payload,
        policy=_policy_with_soft_limit(get_payload_policy("task_submit"), object_threshold_bytes),
    )


def _prepare_http_payload_value_for_upload(
    clients: Sequence["NodeControlClient"],
    value: Any,
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    preserve_container: bool = False,
) -> Any:
    return _prepare_payload_value_for_upload(
        clients,
        value,
        object_threshold_bytes=object_threshold_bytes,
        preserve_container=preserve_container,
        recurse_containers=True,
        consume_on_read=True,
    )


def _prepare_http_payload_for_call(
    clients: Sequence["NodeControlClient"],
    payload: Optional[Dict[str, object]],
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
) -> Dict[str, object]:
    return _prepare_payload_for_policy(
        clients,
        payload,
        policy=_policy_with_soft_limit(get_payload_policy("http_call"), object_threshold_bytes),
    )


def _prepare_remote_call_payload(
    clients: Sequence["NodeControlClient"],
    payload: Optional[Dict[str, object]],
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    managed_global_field_names: Sequence[str] = (),
) -> Dict[str, object]:
    policy = _policy_with_soft_limit(get_payload_policy("http_call"), object_threshold_bytes)
    if managed_global_field_names:
        policy = replace(policy, managed_global_field_names=tuple(str(name) for name in managed_global_field_names))
    return _prepare_payload_for_policy(clients, payload, policy=policy)


_SERVICE_SESSION_SCHEMA_VERSION = 2
_JOB_CLIENT_SESSION_SCHEMA_VERSION = 1


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
    from pycloud_parallel.controlplane.state import _code_version_from_digest

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


def _parse_cache_datetime(value: object) -> Optional[datetime]:
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
    normalized_client_id = str(client_id or "").strip()
    normalized_auth_token = str(auth_token or "").strip()
    if not normalized_client_id or not normalized_auth_token:
        return
    ttl = max(60, int(ttl_sec or _default_job_auth_ttl_sec()))
    now = datetime.now(timezone.utc)
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
        try:
            os.chmod(tmp_name, 0o600)
        except OSError:
            pass
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.unlink(tmp_name)
            except OSError:
                pass


InfoCenterNodeService = _ExtractedInfoCenterNodeService
InfoCenterNodeTaskPool = _ExtractedInfoCenterNodeTaskPool
InfoCenterNode = _ExtractedInfoCenterNode
InfoCenterServiceRoute = _ExtractedInfoCenterServiceRoute
NodeCircuitState = _ExtractedNodeCircuitState
_node_instance_key_from_node = _extracted_node_instance_key_from_node
_node_instance_key_from_route = _extracted_node_instance_key_from_route
_route_predicted_busy = _extracted_route_predicted_busy
_route_sort_key = _extracted_route_sort_key
_build_unique_node_id_map = _extracted_build_unique_node_id_map


_RouteLocalState = _ExtractedRouteLocalState
_ServiceRouteSnapshot = _ExtractedServiceRouteSnapshot


InfoCenterClient = _ExtractedInfoCenterClient

from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient as _ExtractedGatewayServiceClient
from pycloud_parallel.controlplane.job_queue_client import (
    JobQueueClient as _ExtractedJobQueueClient,
    _JobOrchestratorDiscoveryClient as _ExtractedJobOrchestratorDiscoveryClient,
)

GatewayServiceClient = _ExtractedGatewayServiceClient
_JobOrchestratorDiscoveryClient = _ExtractedJobOrchestratorDiscoveryClient
JobQueueClient = _ExtractedJobQueueClient


class ExecutionSessionBase:
    """Common logical-session view for service/task execution sessions."""

    kind: str = ""
    nodes: Dict[str, InfoCenterNode]
    failures: Dict[str, str]
    globals_digests: Dict[str, str]

    def _replica_handles(self) -> Dict[str, ExecutionReplicaHandle]:
        raise NotImplementedError

    def _init_execution_session_state(self) -> None:
        self._hb_stop = threading.Event()
        self._hb_thread = None
        self._hb_lock = threading.Lock()
        self._keepalive_seq = 0
        self._keepalive_failure_counts = {}
        self._active_replica_ids = set(self.replicas.keys())
        if hasattr(self, "_active_nodes"):
            self._active_nodes = self._active_replica_ids
        if not hasattr(self, "failed"):
            self.failed = False

    @property
    def replicas(self) -> Dict[str, ExecutionReplicaHandle]:
        return self._replica_handles()

    def snapshot(self) -> Dict[str, ExecutionReplicaSnapshot]:
        snapshots: Dict[str, ExecutionReplicaSnapshot] = {}
        for node_instance_id, replica in self.replicas.items():
            node = self.nodes.get(node_instance_id)
            snapshots[node_instance_id] = replica.snapshot(
                node_instance_id=node_instance_id,
                node_id=str(node.node_id if node is not None else getattr(replica, "node_id", "") or ""),
                failure=str(self.failures.get(node_instance_id, "") or ""),
            )
        return snapshots

    def is_alive(self) -> bool:
        return any(snapshot.alive for snapshot in self.snapshot().values())

    def _default_keepalive_interval_sec(self, interval_sec: Optional[float] = None) -> float:
        if interval_sec is not None:
            return max(0.5, float(interval_sec))
        timeouts = [
            max(1, int(getattr(replica, "heartbeat_timeout_sec", 0) or 1))
            for replica in self.replicas.values()
        ]
        if not timeouts:
            return 1.0
        return max(0.5, min(30.0, min(timeouts) / 2.0))

    def _heartbeat_failure_threshold(self, node_id: str, replica: ExecutionReplicaHandle) -> int:
        del node_id
        return max(1, int(getattr(replica, "heartbeat_failure_threshold", 1) or 1))

    def _heartbeat_replica(self, node_id: str, replica: ExecutionReplicaHandle, *, seq: int) -> Any:
        del node_id
        try:
            return replica.heartbeat(seq=seq)
        except TypeError:
            return replica.heartbeat()

    def _mark_replica_heartbeat_success(self, node_id: str, replica: ExecutionReplicaHandle) -> None:
        self._keepalive_failure_counts.pop(node_id, None)
        if hasattr(replica, "failed"):
            replica.failed = False
        if hasattr(replica, "last_error"):
            replica.last_error = ""
        self.failures.pop(node_id, None)

    def _mark_replica_heartbeat_failure(self, node_id: str, replica: ExecutionReplicaHandle, exc: Exception) -> None:
        message = repr(exc)
        self.failures[node_id] = message
        if hasattr(replica, "failed"):
            replica.failed = True
        if hasattr(replica, "last_error"):
            replica.last_error = message
        if getattr(replica, "kind", "") == "service" and hasattr(replica, "status"):
            replica.status = pb2.SERVICE_STATUS_STOPPED
        self._active_replica_ids.discard(node_id)

    def _keepalive_loop(self, interval_sec: float) -> None:
        next_tick = time.monotonic() + max(0.1, float(interval_sec))
        while not self._hb_stop.is_set():
            now = time.monotonic()
            wait_sec = max(0.0, next_tick - now)
            if self._hb_stop.wait(wait_sec):
                break
            next_tick += max(0.1, float(interval_sec))
            self._keepalive_seq += 1
            replicas = self.replicas
            for node_id in list(self._active_replica_ids):
                replica = replicas.get(node_id)
                if replica is None:
                    self._active_replica_ids.discard(node_id)
                    continue
                try:
                    self._heartbeat_replica(node_id, replica, seq=self._keepalive_seq)
                    self._mark_replica_heartbeat_success(node_id, replica)
                except Exception as exc:
                    count = int(self._keepalive_failure_counts.get(node_id, 0) or 0) + 1
                    self._keepalive_failure_counts[node_id] = count
                    if count >= self._heartbeat_failure_threshold(node_id, replica):
                        self._mark_replica_heartbeat_failure(node_id, replica, exc)
            if not self._active_replica_ids:
                self.failed = True
                self._hb_stop.set()
                break

    def _start_keepalive(self, interval_sec: Optional[float] = None) -> None:
        with self._hb_lock:
            if self._hb_thread is not None and self._hb_thread.is_alive():
                return
            self.failed = False
            self._keepalive_failure_counts = {}
            self._active_replica_ids = set(self.replicas.keys())
            if hasattr(self, "_active_nodes"):
                self._active_nodes = self._active_replica_ids
            for replica in self.replicas.values():
                if hasattr(replica, "failed"):
                    replica.failed = False
                if hasattr(replica, "last_error"):
                    replica.last_error = ""
            self._hb_stop.clear()
            wait_sec = self._default_keepalive_interval_sec(interval_sec)
            self._hb_thread = threading.Thread(
                target=self._keepalive_loop,
                args=(wait_sec,),
                name=f"{self.kind or 'execution'}-hb",
                daemon=True,
            )
            self._hb_thread.start()
            for replica in self.replicas.values():
                setattr(replica, "_hb_thread", self._hb_thread)
                setattr(replica, "_hb_lock", self._hb_lock)

    def _stop_keepalive(self) -> None:
        with self._hb_lock:
            self._hb_stop.set()
            thread = self._hb_thread
        if thread is not None:
            thread.join(timeout=2.0)
        with self._hb_lock:
            self._hb_thread = None
            for replica in self.replicas.values():
                setattr(replica, "_hb_thread", None)
                setattr(replica, "_hb_lock", self._hb_lock)

    def _sync_failures_from_replicas(self) -> None:
        for node_id, replica in self.replicas.items():
            if getattr(replica, "failed", False):
                self.failures[node_id] = str(getattr(replica, "last_error", "") or self.failures.get(node_id, "") or "replica failed")


class ServiceExecutionSession(ExecutionSessionBase):
    kind = "service"


class TaskExecutionSession(ExecutionSessionBase):
    kind = "task_pool"


class ServiceCompatTaskBackend:
    """Compatibility backend for task semantics built on service sessions."""

NativeTaskBackend = _ExtractedNativeTaskBackend
_TaskPoolCallProxy = _ExtractedTaskPoolCallProxy
_ServiceCompatTaskBackend = _ExtractedServiceCompatTaskBackend
TaskPoolItem = _ExtractedTaskPoolItem


from pycloud_parallel.controlplane.task_session import (
    DedicatedTaskServiceSession as _ExtractedDedicatedTaskServiceSession,
    TaskPoolSession as _ExtractedTaskPoolSession,
    _NativePoolResultAdapter as _ExtractedNativePoolResultAdapter,
    _task_pool_session_from_infocenter as _extracted_task_pool_session_from_infocenter,
)

TaskPoolSession = _ExtractedTaskPoolSession
DedicatedTaskServiceSession = _ExtractedDedicatedTaskServiceSession
_task_pool_session_from_infocenter = _extracted_task_pool_session_from_infocenter
_NativePoolResultAdapter = _ExtractedNativePoolResultAdapter
_DiscoveryRouteCache = _ExtractedDiscoveryRouteCache


from pycloud_parallel.controlplane.discovery_client import DiscoveryServiceClient as _ExtractedDiscoveryServiceClient

DiscoveryServiceClient = _ExtractedDiscoveryServiceClient


NativeTaskPoolClient = _ExtractedNativeTaskPoolClient
ServiceSessionClient = _ExtractedServiceSessionClient




from pycloud_parallel.controlplane.node_control_client import NodeControlClient as _ExtractedNodeControlClient

NodeControlClient = _ExtractedNodeControlClient

from pycloud_parallel.controlplane.service_session import (
    ServiceGroup as _ExtractedServiceGroup,
    _ServiceSessionFileLock as _ExtractedServiceSessionFileLock,
    _load_service_session_cache as _extracted_load_service_session_cache,
    _service_session_cache_file as _extracted_service_session_cache_file,
)

_service_session_cache_file = _extracted_service_session_cache_file
_ServiceSessionFileLock = _ExtractedServiceSessionFileLock
_load_service_session_cache = _extracted_load_service_session_cache

ServiceGroup = _ExtractedServiceGroup


from pycloud_parallel.controlplane.caller_facade import (
    _BroadcastProxy as _ExtractedBroadcastProxy,
    _CallProxy as _ExtractedCallProxy,
    _SyncCallProxy as _ExtractedSyncCallProxy,
    DeployedService as _ExtractedDeployedService,
    DirectConnect as _ExtractedDirectConnect,
    GatewayConnect as _ExtractedGatewayConnect,
)

_CallProxy = _ExtractedCallProxy
_SyncCallProxy = _ExtractedSyncCallProxy
_BroadcastProxy = _ExtractedBroadcastProxy
DeployedService = _ExtractedDeployedService
GatewayConnect = _ExtractedGatewayConnect
DirectConnect = _ExtractedDirectConnect
